//! Gmail history-id driven change stream.
//!
//! All error paths route through `account_error::into_account_error`. The
//! history endpoint context maps 404 / 410 / `historyNotFound` /
//! `failedPrecondition` to `SyncState(CursorInvalid)` with the
//! account cursor scope, which the central recovery mapper resolves
//! to `Engine(RestartScope(CursorScope::Account))`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Change, Checkpoint, LabelId, MembershipScope, ObjectChange,
    ObjectChangeKind, ObjectId, PageBoundary, ScopeChange, ScopeChangeKind, SyncEvent,
};
use futures::stream;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::error::{Error, PageRefusal};
use crate::types::{GmailHistoryItem, GmailMessage, GmailProfile};

#[cfg(test)]
use super::cursor::decode_gmail_state;
use super::cursor::{cursor_for_history, decode_gmail_state_for_profile};
use super::error as account_error;

/// Upper bound on `users.history.list` pages in a single walk.
///
/// The walk had no bound at all: it followed `nextPageToken` until the
/// field was absent, so a server that keeps handing one back - a bug, a
/// misbehaving proxy, a token that echoes itself - spins forever issuing
/// quota-bearing requests. The budget is deliberately far above any real
/// mailbox (a history page carries up to 500 records, so this covers five
/// million change records in one walk) because it is a refusal boundary,
/// not a paging policy: it must never fire for a real account.
///
/// Refusal must TERMINATE, never truncate. `changes_stream` checkpoints
/// only on its final page, so stopping early and reporting a normal
/// completion would tell the engine the walk covered ground it never read;
/// a `SyncEvent::Terminated` costs a redone walk and claims nothing.
const MAX_HISTORY_PAGES: usize = 10_000;

#[cfg(test)]
pub(crate) fn changes_stream(
    client: Arc<GmailClient>,
    profile: GmailProfile,
    cursor: bifrost_types::ChangeCursor,
) -> AccountStream<SyncEvent<Change>> {
    changes_stream_cancellable(client, profile, cursor, CancellationToken::new())
}

pub(crate) fn changes_stream_cancellable(
    client: Arc<GmailClient>,
    profile: GmailProfile,
    cursor: bifrost_types::ChangeCursor,
    shutdown: CancellationToken,
) -> AccountStream<SyncEvent<Change>> {
    // One accumulator for the life of this stream. Every request the
    // walk makes - the identity probe as well as each history page -
    // reports into it, and each emitted batch takes and clears it, so
    // consecutive batches partition the traffic instead of each
    // restating a running total.
    let (client, tally) = client.metered();
    let client = Arc::new(client);
    let state = ChangeState {
        client,
        tally,
        profile,
        phase: ChangePhase::Probe(cursor),
        page_token: None,
        walk_history_id: None,
        seen_page_tokens: HashSet::new(),
        pages_walked: 0,
        shutdown,
    };
    stream_from_state(state)
}

fn stream_from_state(state: ChangeState) -> AccountStream<SyncEvent<Change>> {
    Box::pin(stream::unfold(state, |mut state| async move {
        if state.shutdown.is_cancelled() {
            return None;
        }
        let started = Instant::now();
        // Taking the phase makes the probe cursor and validated walking id
        // exclusive; a walking stream cannot lack its start history id.
        let start_history_id = match std::mem::replace(&mut state.phase, ChangePhase::Ended) {
            ChangePhase::Ended => return None,
            ChangePhase::Done => return Some((SyncEvent::Done(None), state)),
            ChangePhase::Walking(start) => start,
            ChangePhase::Probe(cursor) => {
                let decoded = match decode_gmail_state_for_profile(
                    &cursor.server_state,
                    &state.profile.email_address,
                ) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        state.phase = ChangePhase::Ended;
                        let account_error = account_error::into_account_error(
                            error,
                            account_error::GmailErrorContext::changes(),
                        );
                        return Some((SyncEvent::Terminated(account_error), state));
                    }
                };
                // One `users.getProfile` per poll, KEPT DELIBERATELY. The cost
                // objection is correct and has already been answered: it doubles
                // the request count and the failure surface on a 30-second poll
                // path. It is also the only thing that catches a rotated token
                // now pointing at a DIFFERENT Google account before that
                // account's history is mixed into this cursor's slot, and there
                // is no token-source identity binding upstream to lean on. It
                // comes out when that binding exists, not before; do not re-file
                // it as free savings. See "Per-scope inventory / changes /
                // hydration" in `reference/google.md`.
                let profile = tokio::select! {
                    () = state.shutdown.cancelled() => return None,
                    result = state.client.get_profile() => result,
                };
                let verified = profile.and_then(|current| {
                    super::cursor::validate_gmail_profile(&decoded, &current.email_address)
                });
                match verified {
                    Ok(()) => crate::types::HistoryId(decoded.history_id),
                    Err(error) => {
                        state.phase = ChangePhase::Ended;
                        let account_error = account_error::into_account_error(
                            error,
                            account_error::GmailErrorContext::changes(),
                        );
                        return Some((SyncEvent::Terminated(account_error), state));
                    }
                }
            }
        };
        state.phase = ChangePhase::Walking(start_history_id);

        let history = tokio::select! {
            () = state.shutdown.cancelled() => return None,
            result = state.client.get_history(start_history_id, state.page_token.as_deref()) => result,
        };
        match history {
            Ok(response) => {
                let bytes_in = state.tally.take();
                let history_id = response.history_id.0;
                state.pages_walked += 1;
                if let Some(refusal) = walk_refusal(
                    &mut state.seen_page_tokens,
                    state.pages_walked,
                    response.next_page_token.as_deref(),
                ) {
                    // The page just read is discarded on purpose. The walk
                    // has emitted no checkpoint yet (only a final page
                    // does), so the next walk restarts from the same
                    // `startHistoryId` and re-reads it; keeping the page
                    // would buy nothing and risk reading a partial walk as
                    // progress.
                    state.phase = ChangePhase::Ended;
                    let account_error = account_error::into_account_error(
                        Error::from(refusal),
                        account_error::GmailErrorContext::changes(),
                    );
                    return Some((SyncEvent::Terminated(account_error), state));
                }
                let items = changes_from_history(&response.history);
                let is_final = response.next_page_token.is_none();
                let walk_history_id = *state.walk_history_id.get_or_insert(history_id);
                let checkpoint = checkpoint_for_history_page(
                    is_final,
                    walk_history_id,
                    &state.profile.email_address,
                );
                state.page_token = response.next_page_token;
                if is_final {
                    state.phase = ChangePhase::Done;
                }
                Some((
                    SyncEvent::Batch(Batch {
                        items,
                        page_boundary: if is_final {
                            PageBoundary::Final
                        } else {
                            PageBoundary::Page
                        },
                        server_latency: started.elapsed(),
                        bytes_in,
                        checkpoint,
                    }),
                    state,
                ))
            }
            Err(error) => {
                state.phase = ChangePhase::Ended;
                let account_error = account_error::into_account_error(
                    error,
                    account_error::GmailErrorContext::changes(),
                );
                Some((SyncEvent::Terminated(account_error), state))
            }
        }
    }))
}

/// Translate shared pagination admission into this API's diagnostics.
/// Repeat precedence and page-budget policy live in bifrost-net.
fn walk_refusal(
    seen_page_tokens: &mut HashSet<String>,
    pages_walked: usize,
    next_page_token: Option<&str>,
) -> Option<PageRefusal> {
    match bifrost_net::pagination::check_next(
        seen_page_tokens,
        pages_walked,
        MAX_HISTORY_PAGES,
        next_page_token.map(str::to_string),
    ) {
        Ok(()) => None,
        Err(bifrost_net::pagination::PageRefusal::Repeated) => Some(PageRefusal::RepeatedToken(
            "gmail users.history.list repeated a page token".to_string(),
        )),
        Err(_) => Some(PageRefusal::BudgetExceeded(format!(
            "gmail users.history.list exceeded {MAX_HISTORY_PAGES} pages in one walk"
        ))),
    }
}

/// Only the final page of a history walk may advance the durable cursor.
///
/// State the promise precisely, because the weaker reading looks like a bug
/// and is not: this checkpoint means "every history record at or below this id
/// has been delivered at least once". It does NOT mean "caught up at this id".
/// Retaining the FIRST page's `historyId` across the walk is what makes the
/// weaker claim true, and the weaker claim is the one that is safe - it can
/// replay, and replay is what the engine is built to absorb.
///
/// `response.history_id` is the mailbox's current history record at the
/// time each page is requested, not a per-page resume marker. Records can
/// arrive while a multi-page response snapshot is being drained, so the
/// final checkpoint uses the value observed on the first page. That can
/// replay concurrent records on the next walk, but cannot skip them.
/// `GmailChangeState` carries no page token to record how far into the walk
/// we are. Checkpointing an intermediate page would
/// therefore tell the engine "durably at 400" while pages two and three
/// are still unread; a restart or a `pause()` landing on that batch
/// resumes from 400 and those changes are gone for good, because Gmail
/// cannot re-serve them.
///
/// If mid-walk resumability is ever wanted, it needs a schema bump:
/// `GMAIL_SCHEMA_VERSION` to 2, a `page_token` on `GmailChangeState`, and
/// `ChangeCursor::advanced_through` populated from it - the shape
/// `crates/graph` already uses.
fn checkpoint_for_history_page(
    is_final: bool,
    history_id: u64,
    profile_email: &str,
) -> Option<Checkpoint> {
    is_final.then(|| Checkpoint::Change(cursor_for_history(history_id, profile_email)))
}

enum ChangePhase {
    Probe(bifrost_types::ChangeCursor),
    Walking(crate::types::HistoryId),
    Done,
    Ended,
}

struct ChangeState {
    client: Arc<GmailClient>,
    tally: crate::client::ByteTally,
    profile: GmailProfile,
    phase: ChangePhase,
    page_token: Option<String>,
    walk_history_id: Option<u64>,
    seen_page_tokens: HashSet<String>,
    pages_walked: usize,
    shutdown: CancellationToken,
}

fn changes_from_history(history: &[GmailHistoryItem]) -> Vec<Change> {
    let mut changes = Vec::new();
    for item in history {
        for added in &item.messages_added {
            let object_id = ObjectId(added.message.id.clone());
            changes.push(Change::ObjectChange(ObjectChange {
                id: object_id.clone(),
                kind: ObjectChangeKind::Created,
            }));
            for label in label_ids(&added.message) {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: object_id.clone(),
                    membership: MembershipScope::Label(LabelId(label.clone())),
                    kind: ScopeChangeKind::Added,
                }));
            }
        }
        for deleted in &item.messages_deleted {
            changes.push(Change::ObjectChange(ObjectChange {
                id: ObjectId(deleted.message.id.clone()),
                kind: ObjectChangeKind::Destroyed,
            }));
        }
        for labels_added in &item.labels_added {
            let object_id = ObjectId(labels_added.message.id.clone());
            for label in &labels_added.label_ids {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: object_id.clone(),
                    membership: MembershipScope::Label(LabelId(label.clone())),
                    kind: ScopeChangeKind::Added,
                }));
            }
        }
        for labels_removed in &item.labels_removed {
            let object_id = ObjectId(labels_removed.message.id.clone());
            for label in &labels_removed.label_ids {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: object_id.clone(),
                    membership: MembershipScope::Label(LabelId(label.clone())),
                    kind: ScopeChangeKind::Removed,
                }));
            }
        }
    }
    changes
}

fn label_ids(message: &GmailMessage) -> Vec<String> {
    message.label_ids.clone()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bytes::Bytes;
    use futures::StreamExt;
    use reqwest::StatusCode;

    use super::*;
    use serde_json::json;

    #[test]
    fn only_the_final_history_page_advances_the_durable_cursor() {
        assert!(
            checkpoint_for_history_page(false, 400, "person@example.com").is_none(),
            "Gmail page tokens are not represented in the cursor, so an intermediate page cannot be resumed",
        );
        assert!(matches!(
            checkpoint_for_history_page(true, 400, "person@example.com"),
            Some(Checkpoint::Change(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn change_cancellation_stops_an_inflight_request_without_an_extra_call() {
        let script = ScriptedDispatch::new(vec![Canned::Pending, Canned::Pending]);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        let client = Arc::new(GmailClient::with_account_net("https://gmail.test", net));
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: crate::types::HistoryId(100),
        };
        let shutdown = CancellationToken::new();
        let mut events = changes_stream_cancellable(
            client,
            profile,
            cursor_for_history(100, "person@example.com"),
            shutdown.clone(),
        );
        let started = tokio::time::Instant::now();
        let next = tokio::spawn(async move { events.next().await });
        while script.requests().is_empty() {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), next)
                .await
                .expect("cancellation must beat the one-second deadline")
                .expect("change task joins")
                .is_none()
        );
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test]
    async fn multi_page_walk_checkpoints_the_first_pages_history_boundary() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({
                "historyId": "200",
                "history": [{"messagesAdded": [{"message": message("first", &[])}]}],
                "nextPageToken": "page-2"
            })),
            ok_json(json!({
                "historyId": "250",
                "history": [{"messagesAdded": [{"message": message("second", &[])}]}]
            })),
        ]);
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: crate::types::HistoryId(100),
        };

        let events = changes_stream(
            client,
            profile,
            cursor_for_history(100, "person@example.com"),
        )
        .collect::<Vec<_>>()
        .await;

        let SyncEvent::Batch(final_page) = &events[1] else {
            panic!("second history response must be a batch");
        };
        let Some(Checkpoint::Change(cursor)) = &final_page.checkpoint else {
            panic!("final history page must carry a change checkpoint");
        };
        let state = decode_gmail_state(&cursor.server_state).expect("checkpoint decodes");
        assert_eq!(state.history_id, 200);
    }

    /// A server that keeps handing back the same page token makes no
    /// progress, so the walk must stop. It must stop by TERMINATING:
    /// reporting a normal completion would emit a final checkpoint for a
    /// walk that never reached the end of the history, claiming coverage
    /// it did not earn.
    #[tokio::test]
    async fn a_repeated_history_page_token_terminates_the_walk() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({
                "historyId": "200",
                "history": [{"messagesAdded": [{"message": message("first", &[])}]}],
                "nextPageToken": "stuck"
            })),
            ok_json(json!({
                "historyId": "200",
                "history": [{"messagesAdded": [{"message": message("second", &[])}]}],
                "nextPageToken": "stuck"
            })),
        ]);
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: crate::types::HistoryId(100),
        };

        let events = changes_stream(
            client,
            profile,
            cursor_for_history(100, "person@example.com"),
        )
        .collect::<Vec<_>>()
        .await;

        let SyncEvent::Batch(first) = &events[0] else {
            panic!("the first history page is delivered normally");
        };
        assert!(matches!(first.page_boundary, PageBoundary::Page));
        assert!(
            first.checkpoint.is_none(),
            "an intermediate page never checkpoints",
        );
        let SyncEvent::Terminated(error) = &events[1] else {
            panic!("a cycling page token must terminate, not complete");
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, SyncEvent::Done(_))),
            "a refused walk must not report completion",
        );
    }

    /// The budget is the second guard, for a server that cycles through
    /// FRESH tokens forever - repeated-token detection never fires there.
    #[test]
    fn the_page_budget_refuses_a_walk_that_never_ends() {
        let mut seen = HashSet::new();
        assert!(
            walk_refusal(&mut seen, MAX_HISTORY_PAGES - 1, Some("more")).is_none(),
            "the last page inside the budget still follows its token",
        );
        let refusal = walk_refusal(&mut seen, MAX_HISTORY_PAGES, Some("another"))
            .expect("the budget refuses");
        assert!(matches!(refusal, PageRefusal::BudgetExceeded(_)));
        assert!(
            walk_refusal(&mut seen, MAX_HISTORY_PAGES, None).is_none(),
            "a walk that ends on the budget page ends normally",
        );
        // An honestly huge mailbox looks exactly like this, so it is the
        // implementation's own limit, never a provider fault.
        let error = account_error::into_account_error(
            Error::from(refusal),
            account_error::GmailErrorContext::changes(),
        );
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Internal(
                bifrost_types::InternalErrorKind::LimitExceeded
            )
        );
    }

    /// A page that trips BOTH guards is diagnosed as the repeated token,
    /// not as budget exhaustion: the repeated token is the sharper claim,
    /// and this lane must agree with `inventory_walk_refusal`.
    #[test]
    fn a_page_tripping_both_guards_is_diagnosed_as_the_repeated_token() {
        let mut seen = HashSet::new();
        seen.insert("cycling".to_string());
        let refusal = walk_refusal(&mut seen, MAX_HISTORY_PAGES, Some("cycling"))
            .expect("a page tripping both guards must refuse");
        assert!(matches!(refusal, PageRefusal::RepeatedToken(_)));
        let detail = refusal.detail();
        assert!(
            detail.contains("repeated a page token"),
            "repeated-token detection must win over the budget: {detail}",
        );
        assert!(
            !detail.contains("exceeded"),
            "the budget must not claim a page the repeated-token guard owns: {detail}",
        );
    }

    /// A `historyId` that is present but not a number did not parse; it was
    /// not missing. The response arrived whole, so the evidence is
    /// `Acknowledged`.
    #[tokio::test]
    async fn an_unparseable_history_id_is_a_provider_parse_failure() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({ "historyId": "not-a-number", "history": [] })),
        ]);
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: crate::types::HistoryId(100),
        };

        let events = changes_stream(
            client,
            profile,
            cursor_for_history(100, "person@example.com"),
        )
        .collect::<Vec<_>>()
        .await;

        let [SyncEvent::Terminated(error)] = events.as_slice() else {
            panic!("the walk must terminate alone, got {} events", events.len());
        };
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            )
        );
        let attempt = error.chain().iter().find_map(|cause| match cause {
            bifrost_types::Cause::Attempt(attempt) => Some(attempt.transmission_state),
            _ => None,
        });
        assert_eq!(
            attempt,
            Some(bifrost_types::TransmissionState::Acknowledged)
        );
    }

    fn history(value: serde_json::Value) -> Vec<GmailHistoryItem> {
        serde_json::from_value(value).expect("history fixture deserializes")
    }

    fn ok_json(value: serde_json::Value) -> Canned {
        Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&value).expect("fixture serializes")),
        }
    }

    fn scripted_client(steps: Vec<Canned>) -> Arc<GmailClient> {
        let script = ScriptedDispatch::new(steps);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        Arc::new(GmailClient::with_account_net("https://gmail.test", net))
    }

    fn message(id: &str, labels: &[&str]) -> serde_json::Value {
        json!({
            "id": id,
            "threadId": format!("t-{id}"),
            "labelIds": labels,
        })
    }

    #[tokio::test]
    async fn changes_accepts_profile_email_case_drift() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "PERSON@example.com", "historyId": "100" })),
            ok_json(json!({ "historyId": "101", "history": [] })),
        ]);
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: crate::types::HistoryId(100),
        };
        let cursor = cursor_for_history(100, "Person@Example.com");

        let events = changes_stream(client, profile, cursor)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(events.len(), 2);
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("case-only profile drift must not terminate changes");
        };
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert!(matches!(events[1], SyncEvent::Done(None)));
    }

    fn object_changes(changes: &[Change]) -> Vec<(String, ObjectChangeKind)> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::ObjectChange(oc) => Some((oc.id.0.clone(), oc.kind)),
                _ => None,
            })
            .collect()
    }

    fn scope_changes(changes: &[Change]) -> Vec<(String, String, ScopeChangeKind)> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::ScopeChange(sc) => match &sc.membership {
                    MembershipScope::Label(LabelId(label)) => {
                        Some((sc.id.0.clone(), label.clone(), sc.kind))
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    #[test]
    fn empty_history_yields_no_changes() {
        assert!(changes_from_history(&[]).is_empty());
        assert!(changes_from_history(&history(json!([{}]))).is_empty());
    }

    /// A `messagesAdded` record is both an object birth and a
    /// membership announcement: the engine needs the `Created` row to
    /// materialise the object and one `ScopeChange::Added` per label so
    /// the object lands in the right containers without a follow-up
    /// hydrate.
    #[test]
    fn messages_added_emits_created_then_one_scope_row_per_label() {
        let changes = changes_from_history(&history(json!([{
            "messagesAdded": [{ "message": message("m1", &["INBOX", "UNREAD"]) }],
        }])));

        assert_eq!(
            object_changes(&changes),
            vec![("m1".to_owned(), ObjectChangeKind::Created)]
        );
        assert_eq!(
            scope_changes(&changes),
            vec![
                ("m1".to_owned(), "INBOX".to_owned(), ScopeChangeKind::Added),
                ("m1".to_owned(), "UNREAD".to_owned(), ScopeChangeKind::Added),
            ]
        );
        assert!(
            matches!(changes.first(), Some(Change::ObjectChange(_))),
            "the object must be created before its memberships are announced"
        );
    }

    /// Gmail omits `labelIds` on a message with no labels; the row must
    /// still create the object.
    #[test]
    fn messages_added_without_labels_still_creates_the_object() {
        let changes = changes_from_history(&history(json!([{
            "messagesAdded": [{ "message": { "id": "m2", "threadId": "t2" } }],
        }])));
        assert_eq!(
            object_changes(&changes),
            vec![("m2".to_owned(), ObjectChangeKind::Created)]
        );
        assert!(scope_changes(&changes).is_empty());
    }

    #[test]
    fn messages_deleted_emits_a_destroyed_row_only() {
        let changes = changes_from_history(&history(json!([{
            "messagesDeleted": [{ "message": message("m3", &["INBOX"]) }],
        }])));
        assert_eq!(
            object_changes(&changes),
            vec![("m3".to_owned(), ObjectChangeKind::Destroyed)]
        );
        assert!(
            scope_changes(&changes).is_empty(),
            "a destroyed message needs no membership teardown row"
        );
    }

    #[test]
    fn label_add_and_remove_become_scoped_membership_rows() {
        let changes = changes_from_history(&history(json!([{
            "labelsAdded": [{
                "message": message("m4", &["INBOX"]),
                "labelIds": ["Label_1", "STARRED"],
            }],
            "labelsRemoved": [{
                "message": message("m4", &["INBOX"]),
                "labelIds": ["UNREAD"],
            }],
        }])));

        assert!(
            object_changes(&changes).is_empty(),
            "a label flip is not an object birth or death"
        );
        assert_eq!(
            scope_changes(&changes),
            vec![
                (
                    "m4".to_owned(),
                    "Label_1".to_owned(),
                    ScopeChangeKind::Added
                ),
                (
                    "m4".to_owned(),
                    "STARRED".to_owned(),
                    ScopeChangeKind::Added
                ),
                (
                    "m4".to_owned(),
                    "UNREAD".to_owned(),
                    ScopeChangeKind::Removed
                ),
            ]
        );
    }

    /// The label rows carry the *changed* labels from `labelIds`, not
    /// the message's full label set. Pinning this matters because the
    /// wrapper also carries a `message` whose own `labelIds` is the
    /// post-change full set - reading the wrong one would announce
    /// memberships that did not change.
    #[test]
    fn label_rows_use_the_delta_not_the_messages_full_label_set() {
        let changes = changes_from_history(&history(json!([{
            "labelsAdded": [{
                "message": message("m5", &["INBOX", "UNREAD", "Label_9"]),
                "labelIds": ["Label_9"],
            }],
        }])));
        assert_eq!(
            scope_changes(&changes),
            vec![(
                "m5".to_owned(),
                "Label_9".to_owned(),
                ScopeChangeKind::Added
            )]
        );
    }

    /// Multiple history records fold into one flat, order-preserving
    /// change list; the engine relies on history order for correctness
    /// when an object is created and then relabelled in the same page.
    #[test]
    fn multiple_history_records_preserve_wire_order() {
        let changes = changes_from_history(&history(json!([
            { "messagesAdded": [{ "message": message("m6", &["INBOX"]) }] },
            {
                "labelsRemoved": [{
                    "message": message("m6", &["INBOX"]),
                    "labelIds": ["INBOX"],
                }],
            },
            { "messagesDeleted": [{ "message": message("m6", &[]) }] },
        ])));

        assert_eq!(
            object_changes(&changes),
            vec![
                ("m6".to_owned(), ObjectChangeKind::Created),
                ("m6".to_owned(), ObjectChangeKind::Destroyed),
            ]
        );
        assert_eq!(
            scope_changes(&changes),
            vec![
                ("m6".to_owned(), "INBOX".to_owned(), ScopeChangeKind::Added),
                (
                    "m6".to_owned(),
                    "INBOX".to_owned(),
                    ScopeChangeKind::Removed
                ),
            ]
        );
    }

    /// One history record can carry every category at once; all four
    /// buckets must be drained in the fixed added / deleted / labelled
    /// order the mapper documents.
    #[test]
    fn a_single_record_drains_all_four_buckets() {
        let changes = changes_from_history(&history(json!([{
            "messagesAdded": [{ "message": message("a", &[]) }],
            "messagesDeleted": [{ "message": message("b", &[]) }],
            "labelsAdded": [{ "message": message("c", &[]), "labelIds": ["L"] }],
            "labelsRemoved": [{ "message": message("d", &[]), "labelIds": ["L"] }],
        }])));
        assert_eq!(changes.len(), 4);
        assert_eq!(
            object_changes(&changes),
            vec![
                ("a".to_owned(), ObjectChangeKind::Created),
                ("b".to_owned(), ObjectChangeKind::Destroyed),
            ]
        );
    }
}
