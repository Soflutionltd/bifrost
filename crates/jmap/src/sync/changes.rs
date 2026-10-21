use std::num::NonZeroUsize;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Change, ChangeCursor, Checkpoint, CursorScope,
    MailboxId as TypesMailboxId, ObjectChange, ObjectChangeKind, ObjectId, PageBoundary, SyncEvent,
};

use crate::core::changes::{ChangesMethod, ChangesObject, ChangesResponse};
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailChanges};
use crate::mailbox::{Mailbox, MailboxChanges};

use super::capabilities::CoreLimits;
use super::state::{self, JmapScopeRepr};
use super::state_cache::{self, StateMap};
use super::state_walk::StateWalk;

type MailAccount<T> = crate::account::Account<T>;

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream<T: HttpTransport>(
    mail: MailAccount<T>,
    account_id: String,
    limits: CoreLimits,
    cursor: ChangeCursor,
    owner: Option<TypesMailboxId>,
    email_states: StateMap,
    mailbox_states: StateMap,
) -> AccountStream<SyncEvent<Change>> {
    let decoded = state::decode_cursor(&cursor);
    let (scope, state_string) = match decoded {
        Ok(decoded) => decoded,
        Err(err) => {
            // Cursor decode failure. `decode_cursor` reports a plain
            // crate error, so the classified `AccountError` is
            // synthesized at this boundary - and the CLASS matters,
            // because the two failure families want opposite recoveries:
            //
            // - A version this engine has disowned (older envelope) or
            //   cannot read yet (future envelope) is schema drift.
            //   `SyncState(SchemaIncompatible)` derives the account-wide
            //   schema clear, which also drops every backfill checkpoint
            //   so the inventory re-walk re-mints ids - reseeding is the
            //   migration.
            // - A cursor tagged with another PROTOCOL, or whose payload
            //   scope disagrees with the `ChangeCursor` it rode in on,
            //   is a mis-keyed row: a consumer/store bug, not schema
            //   drift. `SyncState(CursorInvalid)` derives
            //   `Engine(RestartScope(scope))` - delete the bogus row and
            //   re-establish THIS scope - instead of paying the
            //   account-wide clear plus full re-hydration for a row the
            //   schema machinery cannot heal anyway.
            let schema_drift = matches!(
                err,
                state::JmapCursorError::SchemaIncompatible
                    | state::JmapCursorError::CursorEnvelopeUnknown
            );
            let (kind, cause, text) = if schema_drift {
                (
                    bifrost_types::AccountErrorKind::SyncState(
                        bifrost_types::SyncStateErrorKind::SchemaIncompatible,
                    ),
                    bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
                    "JMAP change cursor envelope version is not readable by this engine",
                )
            } else {
                (
                    bifrost_types::AccountErrorKind::SyncState(
                        bifrost_types::SyncStateErrorKind::CursorInvalid,
                    ),
                    bifrost_types::Cause::State(bifrost_types::StateCause::CursorInvalid),
                    "JMAP change cursor row is mis-keyed (wrong protocol or scope mismatch)",
                )
            };
            return Box::pin(async_stream::stream! {
                yield super::error::terminated(
                    bifrost_types::AccountErrorBuilder::new(kind, cause)
                        .protocol(bifrost_types::Protocol::Jmap)
                        .operation(bifrost_types::AccountOperation::SyncChanges)
                        .scope(bifrost_types::ErrorScope::Cursor(cursor.scope.clone()))
                        .text(bifrost_types::DiagnosticText::support_only(text))
                        .try_build()
                        .expect("valid account error classification"),
                );
            });
        }
    };

    match scope {
        JmapScopeRepr::Email => changes_walk::<T, EmailChanges, Email>(
            mail,
            account_id,
            limits,
            cursor.scope.clone(),
            state_string,
            None,
            email_states,
        ),
        JmapScopeRepr::Mailbox => changes_walk::<T, MailboxChanges, Mailbox>(
            mail,
            account_id,
            limits,
            cursor.scope.clone(),
            state_string,
            // A Mailbox scope is never foreign: shared-account mailboxes
            // are reached through the account-level `Folder` scope below.
            None,
            mailbox_states,
        ),
        // Thread cursor inventory is derived from Email inventory and is
        // deliberately not exposed. Old opaque Thread cursors must fail
        // explicitly rather than reviving an unseeded sync path.
        JmapScopeRepr::Thread => unsupported_scope(cursor.scope.clone(), "Thread"),
        // Query definitions are absent from the v1 Account contract, so a
        // query id cannot be turned into a valid filter/sort request. Do
        // not send an unfiltered Email/queryChanges and mislabel its ids.
        JmapScopeRepr::Query(_) => unsupported_scope(cursor.scope.clone(), "Query"),
        // A foreign (shared/delegate) account: its emails sync against
        // that account's `Email/changes` state. The owner tag drives
        // revocation isolation. `Email/changes` is account-wide and
        // cannot be filtered by mailbox, which is exactly why the seeded
        // topology is ONE account-level `Folder` scope per share: the
        // change set streams once, and per-mailbox membership is learned
        // at hydration via the foreign-qualified `mailboxIds` - the same
        // model the primary `Type(Email)` scope uses (no `ScopeChange`
        // needed). A legacy per-mailbox `Folder` cursor still lands here
        // and still advances correctly; it just is not seeded or
        // discovered anymore.
        JmapScopeRepr::Folder { .. } => changes_walk::<T, EmailChanges, Email>(
            mail,
            account_id,
            limits,
            cursor.scope.clone(),
            state_string,
            owner,
            email_states,
        ),
    }
}

/// The paginated `*/changes` walk, shared by every state-based change
/// scope this crate drives.
///
/// `Email/changes` and `Mailbox/changes` ran as two hand-copied loops
/// whose only real differences are the three this signature takes:
///
/// - the METHOD (`M`), which also supplies the diagnostic name
///   `StateWalkFault` reports via `M::NAME`;
/// - `owner`, `Some(accountId)` only for a foreign (shared/delegate)
///   scope. It does two things at once, and both are why the parameter
///   exists rather than a flag: it qualifies every emitted change id into
///   the owning account's namespace (matching what the foreign inventory
///   mints - otherwise hydrating a changed foreign email routes through
///   the primary account), and it turns a permission denial into a
///   quarantine of this scope alone instead of a terminal account error.
///   With `owner: None` the error lane is exactly `into_account_error`,
///   which is what the primary walks always did;
/// - the per-`accountId` state cache (`states`) the scope advances -
///   Email and Mailbox states are separate JMAP `(accountId, type)`
///   positions and must not share a map.
///
/// `StateWalk` owns the shared progress guard and state-aware error context.
/// Its policy keeps the different recovery obligations explicit: the engine
/// restarts a durable change scope, lifecycle reconciles a fresh name snapshot,
/// and anchored inventory allows queryState churn but restarts on a lost
/// anchor. The callers retain their distinct event and coverage behavior.
fn changes_walk<T, M, O>(
    mail: MailAccount<T>,
    account_id: String,
    limits: CoreLimits,
    scope: CursorScope,
    mut since_state: String,
    owner: Option<TypesMailboxId>,
    states: StateMap,
) -> AccountStream<SyncEvent<Change>>
where
    T: HttpTransport,
    O: ChangesObject + Send + 'static,
    O::Id: ToString + Send,
    O::ChangesResponse: Send,
    // `Sync` is not implied by `JmapMethod`: `Request::send_single` holds
    // a `&CallHandle<M>` across its await, so the call future is only
    // `Send` when `M` is `Sync`. Every generated `*/changes` struct is.
    M: ChangesMethod<Response = ChangesResponse<O>> + Sync + 'static,
{
    Box::pin(async_stream::stream! {
        // One accumulator for the whole paged walk; each emitted page
        // takes and clears it, so consecutive pages partition the
        // traffic instead of each restating a running total.
        let (mail, tally) = mail.metered();
        let max_changes = nonzero(limits.max_objects_in_get);
        let mut walk = StateWalk::changes(scope.clone(), &since_state);
        loop {
            let started = Instant::now();
            let response = mail
                .call(M::since(since_state.clone(), max_changes))
                .await;

            let response = match response {
                Ok(response) => response,
                Err(err) => {
                    // Foreign-scope permission denial quarantines just
                    // this scope; primary-scope denial stays terminal.
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        owner.as_ref(),
                        walk.error_context(bifrost_types::AccountOperation::SyncChanges),
                    ));
                    break;
                }
            };

            let new_state = response.new_state().to_string();
            if let Some(fault) = walk.observe_changes_page(
                &since_state,
                &new_state,
                response.has_more_changes(),
            ) {
                yield super::error::terminated_contract_violation(
                    bifrost_types::AccountOperation::SyncChanges,
                    Some(bifrost_types::ErrorScope::Cursor(scope.clone())),
                    fault.describe(M::NAME),
                );
                break;
            }
            let qualify = owner.as_ref().map(|owner| owner.0.clone());
            let changes = object_changes::<O>(response.created(), ObjectChangeKind::Created, qualify.as_deref())
                .into_iter()
                .chain(object_changes::<O>(response.updated(), ObjectChangeKind::Updated, qualify.as_deref()))
                .chain(object_changes::<O>(
                    response.destroyed(),
                    ObjectChangeKind::Destroyed,
                    qualify.as_deref(),
                ))
                .collect::<Vec<_>>();
            let checkpoint = checkpoint_for(scope.clone(), new_state.clone());
            // This cache is a hint for later mutation `ifInState` calls, not
            // the durable engine cursor. Advance it before yield so a
            // concurrent mutation can use the newest state this account has
            // already observed; if the consumer drops the stream before
            // persisting the checkpoint, its older cursor still replays this
            // page. A stale cached state only causes a server-side
            // `stateMismatch` and refresh, never a skipped change page.
            state_cache::advance(&states, &account_id, Some(&since_state), new_state.clone()).await;

            yield SyncEvent::Batch(Batch {
                items: changes,
                page_boundary: PageBoundary::Page,
                server_latency: started.elapsed(),
                bytes_in: tally.take(),
                checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
            });

            since_state = new_state;
            if !response.has_more_changes() {
                yield SyncEvent::Done(Some(Checkpoint::Change(checkpoint)));
                break;
            }
        }
    })
}

fn unsupported_scope(scope: CursorScope, name: &'static str) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        yield super::error::terminated(super::error::unsupported_error(
            bifrost_types::AccountOperation::SyncChanges,
            Some(bifrost_types::ErrorScope::Cursor(scope)),
            format!("JMAP {name} cursor changes are not supported by the v1 Account contract"),
        ));
    })
}

/// Project a changes id list onto `ObjectChange`s. `owner_account`
/// (`Some(accountId)` only for a foreign/shared scope) qualifies each id
/// into the owning account's namespace so downstream hydration and blob
/// reads route to that account; a primary scope leaves ids bare.
fn object_changes<O: ChangesObject>(
    ids: &[O::Id],
    kind: ObjectChangeKind,
    owner_account: Option<&str>,
) -> Vec<Change>
where
    O::Id: ToString,
{
    ids.iter()
        .map(|id| {
            let native = id.to_string();
            let id = match owner_account {
                Some(account) => super::foreign::encode_object(account, &native),
                None => native,
            };
            Change::ObjectChange(ObjectChange {
                id: ObjectId(id),
                kind,
            })
        })
        .collect()
}

fn checkpoint_for(scope: CursorScope, state_string: String) -> ChangeCursor {
    state::cursor_for_scope(scope, state_string)
        .expect("JMAP change loops only checkpoint supported cursor scopes")
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value.max(1)).expect("value.max(1) is non-zero")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::email::EmailId;
    use futures::StreamExt;

    fn object_ids(changes: &[Change]) -> Vec<String> {
        changes
            .iter()
            .map(|change| match change {
                Change::ObjectChange(object) => object.id.0.clone(),
                other => panic!("expected an ObjectChange, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn primary_change_ids_stay_bare() {
        let ids = vec![EmailId::new("M1"), EmailId::new("M2")];
        let changes = object_changes::<Email>(&ids, ObjectChangeKind::Created, None);

        assert_eq!(object_ids(&changes), vec!["M1", "M2"]);
        for change in &changes {
            match change {
                Change::ObjectChange(object) => {
                    assert_eq!(object.kind, ObjectChangeKind::Created);
                }
                other => panic!("expected an ObjectChange, got {other:?}"),
            }
        }
    }

    #[test]
    fn foreign_change_ids_carry_their_owning_account() {
        // A foreign scope's change ids must land in the SAME namespace the
        // foreign inventory mints, or hydrating a changed shared-account
        // message routes through the primary account's `Email/get`.
        let ids = vec![EmailId::new("M1")];
        let changes = object_changes::<Email>(&ids, ObjectChangeKind::Destroyed, Some("acct-9"));

        assert_eq!(
            object_ids(&changes),
            vec![super::super::foreign::encode_object("acct-9", "M1")]
        );
        // And the encoding is reversible back to the wire id.
        assert_eq!(
            super::super::foreign::native_object(&object_ids(&changes)[0]),
            "M1"
        );
    }

    #[test]
    fn an_empty_change_list_yields_no_changes() {
        let ids: Vec<EmailId> = Vec::new();
        assert!(object_changes::<Email>(&ids, ObjectChangeKind::Updated, None).is_empty());
        assert!(
            object_changes::<Email>(&ids, ObjectChangeKind::Updated, Some("acct-9")).is_empty()
        );
    }

    #[test]
    fn every_change_stream_scope_can_build_its_checkpoint_cursor() {
        // `checkpoint_for` unwraps, so any scope reachable from a change
        // loop must encode. The foreign `Folder` shape is the one that
        // round-trips through a codec rather than a fixed tag.
        for scope in [
            CursorScope::Type(bifrost_types::ObjectType::Email),
            CursorScope::Type(bifrost_types::ObjectType::Mailbox),
            // The seeded foreign shape (account-level) and the legacy
            // per-mailbox shape both reach this loop; both must encode.
            CursorScope::Folder(super::super::foreign::encode_foreign_account("acct-9")),
            CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-1")),
        ] {
            let cursor = checkpoint_for(scope.clone(), "state-1".to_string());
            assert_eq!(cursor.scope, scope);
            assert_eq!(
                cursor.envelope_version,
                state::OUTER_CURSOR_ENVELOPE_VERSION
            );
            let (_, state_string) =
                state::decode_cursor(&cursor).expect("a checkpoint must decode again");
            assert_eq!(state_string, "state-1");
        }
    }

    /// A JMAP HTTP boundary that records every request and answers any
    /// `*/changes` call with an empty, terminal change page echoing the
    /// method name and call id it was asked for. Enough to observe WHICH
    /// method a cursor scope routed to without pinning any payload.
    struct RecordingTransport {
        requests: RequestLog,
        /// When set, answer every `*/changes` call with `newState ==
        /// sinceState` and `hasMoreChanges: true` - a stuck server that
        /// would drive an unguarded loop forever.
        stuck: bool,
        /// When set, alternate `newState` between two values, always with
        /// `hasMoreChanges: true`. Every single step "moves" the state, so
        /// the single-step guard sees progress; only a cycle guard can end
        /// the walk.
        oscillating: bool,
    }

    /// Shared handle on the recorded requests. The transport is moved
    /// into the client, so the log has to be held separately.
    #[derive(Clone)]
    struct RequestLog(std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>);

    impl RequestLog {
        fn new() -> Self {
            Self(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())))
        }

        fn methods(&self) -> Vec<String> {
            self.0
                .lock()
                .expect("recorded requests")
                .iter()
                .flat_map(|request| {
                    request["methodCalls"]
                        .as_array()
                        .expect("methodCalls array")
                        .iter()
                        .map(|call| call[0].as_str().expect("method name").to_string())
                        .collect::<Vec<_>>()
                })
                .collect()
        }

        fn account_ids(&self) -> Vec<String> {
            self.0
                .lock()
                .expect("recorded requests")
                .iter()
                .flat_map(|request| {
                    request["methodCalls"]
                        .as_array()
                        .expect("methodCalls array")
                        .iter()
                        .map(|call| call[1]["accountId"].as_str().unwrap_or("").to_string())
                        .collect::<Vec<_>>()
                })
                .collect()
        }

        fn push(&self, request: serde_json::Value) {
            self.0.lock().expect("recorded requests").push(request);
        }
    }

    impl RecordingTransport {
        fn reply(
            &self,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).map_err(|error| {
                crate::core::transport::TransportError::with_source(
                    "JMAP client emitted a non-JSON API request",
                    error,
                )
            })?;
            let call = request["methodCalls"][0].clone();
            self.requests.push(request);
            let name = call[0]
                .as_str()
                .unwrap_or_else(|| panic!("request has no method name: {call}"))
                .to_string();
            let call_id = call[2]
                .as_str()
                .unwrap_or_else(|| panic!("request has no call id: {call}"))
                .to_string();
            let account_id = call[1]["accountId"].clone();
            let since = call[1]["sinceState"].clone();
            let new_state = if self.stuck {
                since.clone()
            } else if self.oscillating {
                if since == serde_json::json!("state-2") {
                    serde_json::json!("state-1")
                } else {
                    serde_json::json!("state-2")
                }
            } else {
                serde_json::json!("state-2")
            };
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[
                    name,
                    {
                        "accountId": account_id,
                        "oldState": since,
                        "newState": new_state,
                        "hasMoreChanges": self.stuck || self.oscillating,
                        "created": [],
                        "updated": [],
                        "destroyed": []
                    },
                    call_id
                ]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }
    }

    impl HttpTransport for RecordingTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            self.reply(body)
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no upload reply",
            ))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no download reply",
            ))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no session reply",
            ))
        }
    }

    fn test_session() -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    fn limits() -> CoreLimits {
        CoreLimits {
            max_objects_in_get: 256,
            max_objects_in_set: 256,
        }
    }

    fn empty_states() -> StateMap {
        std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()))
    }

    /// Drive `stream` to exhaustion against a recording transport and
    /// return the events plus the request log.
    async fn drive(
        account_id: &str,
        cursor: ChangeCursor,
        owner: Option<TypesMailboxId>,
    ) -> (Vec<SyncEvent<Change>>, RequestLog) {
        let log = RequestLog::new();
        let client = crate::client::Client::with_transport(
            RecordingTransport {
                requests: log.clone(),
                stuck: false,
                oscillating: false,
            },
            test_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client, account_id);
        let events = stream(
            mail,
            account_id.to_string(),
            limits(),
            cursor,
            owner,
            empty_states(),
            empty_states(),
        )
        .collect::<Vec<_>>()
        .await;
        (events, log)
    }

    /// The scope-to-method dispatch table. A cursor scope selects the
    /// JMAP method the change loop polls, and getting this wrong is
    /// silent: a Mailbox scope answered by `Email/changes` would emit
    /// message ids as container changes. The foreign `Folder` scope is
    /// the one that is NOT self-evident - it is a mailbox-shaped scope
    /// that must poll `Email/changes` against the OWNING account.
    #[tokio::test]
    async fn each_cursor_scope_polls_its_own_jmap_method() {
        for (account_id, scope, method, owner) in [
            (
                "primary",
                CursorScope::Type(bifrost_types::ObjectType::Email),
                "Email/changes",
                None,
            ),
            (
                "primary",
                CursorScope::Type(bifrost_types::ObjectType::Mailbox),
                "Mailbox/changes",
                None,
            ),
            (
                "shared",
                CursorScope::Folder(super::super::foreign::encode_foreign_account("shared")),
                "Email/changes",
                Some(TypesMailboxId("shared".to_string())),
            ),
            (
                "shared",
                CursorScope::Folder(super::super::foreign::encode_foreign("shared", "mbx-1")),
                "Email/changes",
                Some(TypesMailboxId("shared".to_string())),
            ),
        ] {
            let cursor = state::cursor_for_scope(scope.clone(), "state-1").expect("scope encodes");
            let (events, log) = drive(account_id, cursor, owner).await;

            assert_eq!(log.methods(), vec![method.to_string()], "{scope:?}");
            assert_eq!(
                log.account_ids(),
                vec![account_id.to_string()],
                "{scope:?}: the poll must address the scope's own account"
            );
            // One page, then a terminal Done carrying the advanced cursor.
            assert!(
                matches!(events.first(), Some(SyncEvent::Batch(_))),
                "{scope:?}: expected a Batch, got {:?}",
                events.first()
            );
            match events.last() {
                Some(SyncEvent::Done(Some(Checkpoint::Change(cursor)))) => {
                    assert_eq!(cursor.scope, scope, "checkpoint keeps its scope");
                    let (_, state_string) =
                        state::decode_cursor(cursor).expect("checkpoint decodes");
                    assert_eq!(state_string, "state-2", "checkpoint advances");
                }
                other => panic!("{scope:?}: expected a terminal Done, got {other:?}"),
            }
        }
    }

    /// A server answering `hasMoreChanges: true` with `newState ==
    /// sinceState` would drive an unguarded loop into an unbounded run of
    /// wire requests, each emitting a checkpoint-bearing batch at full
    /// speed. The forward-progress guard must terminate the stream as a
    /// contract violation after exactly one request - the state did not
    /// move, so nothing is lost.
    #[tokio::test]
    async fn a_stuck_changes_state_terminates_instead_of_looping() {
        for (scope, method_count) in [
            (CursorScope::Type(bifrost_types::ObjectType::Email), 1),
            (CursorScope::Type(bifrost_types::ObjectType::Mailbox), 1),
        ] {
            let log = RequestLog::new();
            let client = crate::client::Client::with_transport(
                RecordingTransport {
                    requests: log.clone(),
                    stuck: true,
                    oscillating: false,
                },
                test_session(),
                "https://example.test/.well-known/jmap",
            )
            .expect("client builds");
            let mail = crate::account::Account::new(client, "primary");
            let cursor = state::cursor_for_scope(scope.clone(), "state-1").expect("scope encodes");
            let events = stream(
                mail,
                "primary".to_string(),
                limits(),
                cursor,
                None,
                empty_states(),
                empty_states(),
            )
            .collect::<Vec<_>>()
            .await;

            assert_eq!(
                log.methods().len(),
                method_count,
                "{scope:?}: the guard must fire after one request"
            );
            let terminated = events.iter().find_map(|event| match event {
                SyncEvent::Terminated(error) => Some(error),
                _ => None,
            });
            let error = terminated
                .unwrap_or_else(|| panic!("{scope:?}: expected Terminated, got {events:?}"));
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                ),
                "{scope:?}"
            );
        }
    }

    /// A server alternating between two states with `hasMoreChanges: true`
    /// moves the state at every single step, so the single-step guard sees
    /// progress at each one and the walk paginates forever. The cycle guard
    /// must end it the moment a state this walk already served comes back.
    #[tokio::test]
    async fn an_oscillating_changes_state_terminates_instead_of_looping() {
        for scope in [
            CursorScope::Type(bifrost_types::ObjectType::Email),
            CursorScope::Type(bifrost_types::ObjectType::Mailbox),
        ] {
            let log = RequestLog::new();
            let client = crate::client::Client::with_transport(
                RecordingTransport {
                    requests: log.clone(),
                    stuck: false,
                    oscillating: true,
                },
                test_session(),
                "https://example.test/.well-known/jmap",
            )
            .expect("client builds");
            let mail = crate::account::Account::new(client, "primary");
            let cursor = state::cursor_for_scope(scope.clone(), "state-1").expect("scope encodes");
            let events = stream(
                mail,
                "primary".to_string(),
                limits(),
                cursor,
                None,
                empty_states(),
                empty_states(),
            )
            .collect::<Vec<_>>()
            .await;

            // Request one moves state-1 -> state-2 and is served; request
            // two comes back to state-1, which this walk has already
            // served, and is refused.
            assert_eq!(
                log.methods().len(),
                2,
                "{scope:?}: the cycle guard must fire on the second request"
            );
            let error = events
                .iter()
                .find_map(|event| match event {
                    SyncEvent::Terminated(error) => Some(error),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{scope:?}: expected Terminated, got {events:?}"));
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                ),
                "{scope:?}"
            );
        }
    }

    /// The two scopes the v1 contract decodes but does not drive.
    /// Both must terminate before a request is built - an unfiltered
    /// `Email/queryChanges` would return the whole account mislabelled
    /// as one query's result set.
    #[tokio::test]
    async fn undriven_scopes_terminate_without_touching_the_wire() {
        for scope in [
            CursorScope::Type(bifrost_types::ObjectType::Thread),
            CursorScope::Query(bifrost_types::QueryId("unread-in-inbox".to_string())),
        ] {
            let cursor = state::cursor_for_scope(scope.clone(), "state-1").expect("scope encodes");
            let (events, log) = drive("primary", cursor, None).await;

            assert!(
                log.methods().is_empty(),
                "{scope:?}: no request may be sent"
            );
            match events.as_slice() {
                [SyncEvent::Terminated(err)] => {
                    assert_eq!(
                        err.kind(),
                        &bifrost_types::AccountErrorKind::Unsupported(
                            bifrost_types::AccountOperation::SyncChanges
                        ),
                        "{scope:?}"
                    );
                    assert_eq!(
                        err.scope(),
                        Some(&bifrost_types::ErrorScope::Cursor(scope.clone()))
                    );
                }
                other => panic!("{scope:?}: expected one Terminated, got {other:?}"),
            }
        }
    }

    /// A durable cursor row this build cannot read is schema drift: the
    /// engine clears schema-versioned state account-wide and reseeds
    /// through inventory. Both directions count - an envelope from an
    /// older build (whose foreign thread ids were minted bare) and one
    /// from a newer build.
    #[tokio::test]
    async fn an_unreadable_cursor_envelope_clears_the_schema() {
        for version in [1, state::PAYLOAD_ENVELOPE_VERSION + 1] {
            let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
            let mut cursor =
                state::cursor_for_scope(scope.clone(), "state-1").expect("scope encodes");
            cursor.server_state.envelope_version = version;

            let (events, log) = drive("primary", cursor, None).await;
            assert!(log.methods().is_empty(), "envelope {version}");
            match events.as_slice() {
                [SyncEvent::Terminated(err)] => {
                    assert_eq!(
                        err.kind(),
                        &bifrost_types::AccountErrorKind::SyncState(
                            bifrost_types::SyncStateErrorKind::SchemaIncompatible
                        ),
                        "envelope {version}"
                    );
                    assert_eq!(
                        err.recovery(),
                        &bifrost_types::RecoveryClass::Engine(
                            bifrost_types::EngineDirective::SchemaIncompatible
                        ),
                        "envelope {version}"
                    );
                }
                other => panic!("envelope {version}: expected one Terminated, got {other:?}"),
            }
        }
    }

    /// A row that is readable but MIS-KEYED - tagged with another
    /// protocol, or whose payload scope disagrees with the scope it rode
    /// in on - is a store bug, not schema drift. Paying the account-wide
    /// schema clear for it would re-hydrate everything to heal one bad
    /// row, so it restarts just that scope instead.
    #[tokio::test]
    async fn a_mis_keyed_cursor_row_restarts_only_its_own_scope() {
        let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
        let good = state::cursor_for_scope(scope.clone(), "state-1").expect("scope encodes");

        let foreign_protocol = ChangeCursor {
            scope: scope.clone(),
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Gmail,
                envelope_version: state::PAYLOAD_ENVELOPE_VERSION,
                bytes: good.server_state.bytes.clone(),
            },
            advanced_through: None,
            envelope_version: state::OUTER_CURSOR_ENVELOPE_VERSION,
        };
        // Same payload, but the row is filed under a different scope.
        let crossed = ChangeCursor {
            scope: CursorScope::Type(bifrost_types::ObjectType::Mailbox),
            server_state: good.server_state.clone(),
            advanced_through: None,
            envelope_version: state::OUTER_CURSOR_ENVELOPE_VERSION,
        };

        for (label, cursor) in [
            ("wrong protocol", foreign_protocol),
            ("scope mismatch", crossed),
        ] {
            let expected_scope = cursor.scope.clone();
            let (events, log) = drive("primary", cursor, None).await;
            assert!(log.methods().is_empty(), "{label}");
            match events.as_slice() {
                [SyncEvent::Terminated(err)] => {
                    assert_eq!(
                        err.kind(),
                        &bifrost_types::AccountErrorKind::SyncState(
                            bifrost_types::SyncStateErrorKind::CursorInvalid
                        ),
                        "{label}"
                    );
                    assert_eq!(
                        err.recovery(),
                        &bifrost_types::RecoveryClass::Engine(
                            bifrost_types::EngineDirective::RestartScope(expected_scope)
                        ),
                        "{label}"
                    );
                }
                other => panic!("{label}: expected one Terminated, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn query_changes_fail_before_an_unfiltered_request_can_be_sent() {
        let scope = CursorScope::Query(bifrost_types::QueryId("unread-in-inbox".to_string()));
        let mut stream = unsupported_scope(scope.clone(), "Query");

        match stream.next().await {
            Some(SyncEvent::Terminated(err)) => {
                assert_eq!(
                    err.kind(),
                    &bifrost_types::AccountErrorKind::Unsupported(
                        bifrost_types::AccountOperation::SyncChanges
                    )
                );
                assert_eq!(err.scope(), Some(&bifrost_types::ErrorScope::Cursor(scope)));
            }
            other => panic!("expected unsupported query termination, got {other:?}"),
        }
        assert!(stream.next().await.is_none());
    }
}
