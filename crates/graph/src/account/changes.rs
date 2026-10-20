use bifrost_types::{
    AccountOperation, AccountStream, Change, ChangeCursor, Checkpoint, ErrorScope, ObjectChange,
    ObjectChangeKind, PageBoundary, ScopeChange, ScopeChangeKind, SyncEvent,
};
use serde_json::Value;

use super::GraphAccount;
use super::cursor::{CursorError, decode_cursor, encode_cursor};
use super::graph_error::{GraphErrorContext, cursor_error_to_account_error};
use super::inventory::{
    batch, fetch_delta_page, graph_etag, is_removed, membership_from_value, page_marker,
    removed_id, scope_matches_payload,
};

/// Refresh a calendarView delta link before its fixed horizon gets close.
/// Keeping a ninety-day lead preserves the original 365-day forward view
/// while avoiding a cursor that silently stops seeing future events.
const CALENDAR_WINDOW_RESEED_LEAD_SECS: i64 = 90 * 24 * 60 * 60;

fn calendar_window_needs_reseed(payload: &super::cursor::GraphCursorPayload, now: i64) -> bool {
    matches!(&payload.kind, super::cursor::GraphCursorKind::Events { .. })
        && payload
            .calendar_window_end
            .is_none_or(|end| now >= end.saturating_sub(CALENDAR_WINDOW_RESEED_LEAD_SECS))
}

pub(crate) fn changes_stream(
    account: GraphAccount,
    cursor: ChangeCursor,
) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        if !super::inventory::sync_scope_readable(&cursor.scope) {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(CursorError::Unsupported, ctx));
            return;
        }
        let mut payload = match decode_cursor(&cursor) {
            Ok(payload) => payload,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
                yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };
        if !scope_matches_payload(&cursor.scope, &payload) {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                CursorError::SchemaIncompatible,
                ctx,
            ));
            yield SyncEvent::Done(None);
            return;
        }
        // A v4 in-progress inventory cursor carries a next link, not a
        // delta link. `inventory_resume_stream` is the only correct way to
        // consume one, and both engine establishment paths route it there;
        // reaching here means something handed a page position to the
        // changes walk, which would resume from an empty URL. Refuse
        // instead, so the engine restarts the scope rather than silently
        // syncing nothing.
        if payload.inventory_in_progress {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                CursorError::InventoryInProgress,
                ctx,
            ));
            yield SyncEvent::Done(None);
            return;
        }
        if calendar_window_needs_reseed(&payload, jiff::Timestamp::now().as_second()) {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                CursorError::CalendarWindowExpired,
                ctx,
            ));
            yield SyncEvent::Done(None);
            return;
        }

        // The resume URL is an absolute `@odata.deltaLink` Graph itself
        // minted, so it already names the right mailbox and this walk would
        // keep SUCCEEDING for a shared mailbox the account no longer
        // configures. That is worse than a wrong-namespace request: the
        // scope stays live and emits changes whose object ids then fail
        // hydration and every mutation terminally, because those paths do
        // consult the shared-client map. Select and retain the scope's
        // client here, so the engine disables an unconfigured scope and a
        // configured foreign scope keeps its own transport for the complete
        // delta-link walk.
        let client = match account.client_for_scope(&cursor.scope) {
            Ok(client) => client.clone(),
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
                yield SyncEvent::Terminated(cursor_error_to_account_error(
                    super::cursor::routing_error(error),
                    ctx,
                ));
                yield SyncEvent::Done(None);
                return;
            }
        };

        // One accumulator for the delta walk; each emitted page takes
        // and clears it, so consecutive pages partition the traffic.
        let (client, tally) = client.metered();

        let scope = cursor.scope.clone();
        // The persisted link is the consumer's durable state, re-admitted
        // here because serde cannot know the client's origin. A link that
        // does not admit would otherwise be fetched with the account bearer;
        // refusing it as an unreadable cursor reseeds the scope.
        let mut current_url = match super::cursor::admit_resume_url(&client, payload.resume_url()) {
            Ok(url) => url,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };
        let mut walk = crate::paging::PageWalk::new("changes delta");

        loop {
            if let Err(error) = walk.enter(&current_url) {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                let owner = account.owner_of_scope(&scope);
                yield SyncEvent::Terminated(super::graph_error::graph_shared_scope_error(
                    error, &scope, owner.as_ref(), ctx,
                ));
                yield SyncEvent::Done(None);
                return;
            }
            let page = match fetch_delta_page(&client, &current_url).await {
                Ok(page) => page,
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                        .with_scope(ErrorScope::Cursor(scope.clone()));
                    // Foreign-scope permission denial quarantines just
                    // this scope; primary-scope denial stays terminal.
                    let owner = account.owner_of_scope(&scope);
                    yield SyncEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error,
                        &scope,
                        owner.as_ref(),
                        ctx,
                    ));
                    yield SyncEvent::Done(None);
                    return;
                }
            };
            let mut changes = Vec::new();
            // Diagnostic page marker only: the last value's id after the
            // same scope projection used for emitted changes. Resume never
            // reads it - `next_link` alone drives resumption - but the
            // persisted field uses one id encoding across inventory and
            // changes page markers.
            let mut last_seen_id = None;
            let mut etags = Vec::new();
            let mut removed_etag_ids = Vec::new();
            let mut idless_value = false;
            for value in page.value {
                if let Some(id) = value.get("id").and_then(Value::as_str) {
                    last_seen_id = Some(super::inventory::sync_object_id(&scope, id).0);
                }
                if is_removed(&value) {
                    if let Some(id) = removed_id(&value) {
                        // Encode the removed id the same way the Added /
                        // Updated ids are encoded, so a foreign item's
                        // remove carries the same bytes its add did
                        // (ratatoskr equality-joins on this id).
                        let id = super::inventory::sync_object_id(&scope, &id.0);
                        if super::inventory::is_mail_scope(&scope) {
                            removed_etag_ids.push(id.0.clone());
                        }
                        changes.push(Change::ScopeChange(ScopeChange {
                            id,
                            membership: membership_from_value(&scope, &value),
                            kind: ScopeChangeKind::Removed,
                        }));
                    } else {
                        idless_value = true;
                    }
                    continue;
                }
                if value.get("id").and_then(Value::as_str).is_none() {
                    idless_value = true;
                }
                if let Some(id) = value.get("id").and_then(Value::as_str) {
                    let object_id = super::inventory::sync_object_id(&scope, id);
                    // Only mail mutations consume this bounded cache.
                    if super::inventory::is_mail_scope(&scope)
                        && let Some(etag) = graph_etag(&value) {
                        etags.push((object_id.0.clone(), etag));
                    }
                    // Graph delta has no created/updated discriminator, and
                    // this account keeps no complete per-scope id set to diff
                    // against. Report a refresh as Updated; Added below names
                    // current membership, not object creation. The engine's
                    // live changes driver forwards these kinds unchanged, so
                    // treating Updated for an unknown id as insertion is a
                    // consumer contract; changing this to Created would also
                    // misclassify ordinary edits.
                    changes.push(Change::ObjectChange(ObjectChange {
                        id: object_id.clone(),
                        kind: ObjectChangeKind::Updated,
                    }));
                    changes.push(Change::ScopeChange(ScopeChange {
                        id: object_id,
                        membership: membership_from_value(&scope, &value),
                        kind: ScopeChangeKind::Added,
                    }));
                }
            }

            if !etags.is_empty() || !removed_etag_ids.is_empty() {
                let mut cache = account.etag_index.write().await;
                for (id, etag) in etags {
                    cache.insert(id, etag);
                }
                for id in removed_etag_ids {
                    cache.remove(&id);
                }
            }

            if idless_value {
                // An id-less delta value is a checkpoint barrier - the same
                // rule the inventory walk enforces with a Region obligation.
                // The changes lane has no obligation vocabulary in
                // `SyncEvent<Change>`, so the honest fallback is the
                // neither-link arm's: surface what was decoded, then
                // terminate WITHOUT emitting the page's checkpoint. Crossing
                // the page would advance the cursor past a value this stream
                // could not represent, converting a malformed page into
                // silent, permanent, unreported loss for the scope.
                if !changes.is_empty() {
                    yield batch(changes, PageBoundary::Page, None, tally.take());
                }
                yield SyncEvent::Terminated(super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::ContractViolation,
                    AccountOperation::SyncChanges,
                    Some(ErrorScope::Cursor(scope.clone())),
                    "Graph delta page carried a value with no usable id",
                ));
                yield SyncEvent::Done(None);
                return;
            }

            // Both links are admitted at receipt, before either is followed
            // or persisted into the checkpoint below.
            let links = client
                .admit_next(page.next_link.as_ref())
                .and_then(|next| Ok((next, client.admit_next(page.delta_link.as_ref())?)));
            let (next_link, delta_link) = match links {
                Ok(links) => links,
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                        .with_scope(ErrorScope::Cursor(scope.clone()));
                    let owner = account.owner_of_scope(&scope);
                    if !changes.is_empty() {
                        yield batch(changes, PageBoundary::Page, None, tally.take());
                    }
                    yield SyncEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error, &scope, owner.as_ref(), ctx,
                    ));
                    yield SyncEvent::Done(None);
                    return;
                }
            };
            if let Some(next_link) = next_link {
                payload.advanced_through = Some(page_marker(&next_link, last_seen_id));
                let checkpoint_cursor =
                    match encode_cursor(scope.clone(), payload.clone()) {
                        Ok(cursor) => cursor,
                        Err(error) => {
                            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                                .with_scope(ErrorScope::Cursor(scope.clone()));
                            yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                            yield SyncEvent::Done(None);
                            return;
                        }
                    };
                yield batch(changes, PageBoundary::Page, Some(checkpoint_cursor), tally.take());
                current_url = next_link;
            } else if let Some(delta_link) = delta_link {
                payload.delta_link = delta_link.as_str().to_string();
                payload.advanced_through = None;
                let checkpoint_cursor =
                    match encode_cursor(scope.clone(), payload.clone()) {
                        Ok(cursor) => cursor,
                        Err(error) => {
                            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                                .with_scope(ErrorScope::Cursor(scope.clone()));
                            yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                            yield SyncEvent::Done(None);
                            return;
                        }
                    };
                let checkpoint = Checkpoint::Change(checkpoint_cursor.clone());
                yield batch(changes, PageBoundary::Final, Some(checkpoint_cursor), tally.take());
                yield SyncEvent::Done(Some(checkpoint));
                return;
            } else {
                // A Graph delta page MUST carry either an `@odata.nextLink`
                // (more pages) or an `@odata.deltaLink` (end of the walk).
                // Neither present is a Graph contract violation. Emitting
                // `Final` + `Done(None)` here would drop the cursor advance,
                // so the engine would re-issue the same final page on every
                // poll forever. Terminate with a contract violation instead
                // so the failure is visible rather than a silent live-lock.
                if !changes.is_empty() {
                    yield batch(changes, PageBoundary::Page, None, tally.take());
                }
                yield SyncEvent::Terminated(super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::ContractViolation,
                    AccountOperation::SyncChanges,
                    Some(ErrorScope::Cursor(scope.clone())),
                    "Graph delta page carried neither @odata.nextLink nor @odata.deltaLink",
                ));
                yield SyncEvent::Done(None);
                return;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bifrost_types::{
        AccountErrorKind, CursorScope, FolderId, ObjectType, OpaqueChangeState, ProtocolKind,
        SyncStateErrorKind,
    };
    use futures::StreamExt;
    use serde_json::json;

    use super::super::PushMode;
    use super::super::cursor::{
        CHANGE_CURSOR_ENVELOPE_VERSION, GRAPH_CURSOR_ENVELOPE_VERSION, GraphCursorPayload,
        encode_cursor, kind_for_scope,
    };
    use super::*;
    use crate::client::{GraphClient, ScriptedRestResponse};

    fn account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
    }

    fn email_scope(folder: &str) -> CursorScope {
        CursorScope::FolderType {
            folder: FolderId(folder.to_string()),
            ty: ObjectType::Email,
        }
    }

    #[test]
    fn calendar_window_reseeds_within_the_refresh_lead() {
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("event scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        )
        .with_calendar_window_end_option(Some(1_000));
        assert!(calendar_window_needs_reseed(
            &payload,
            1_000 - CALENDAR_WINDOW_RESEED_LEAD_SECS
        ));
        assert!(!calendar_window_needs_reseed(
            &payload,
            1_000 - CALENDAR_WINDOW_RESEED_LEAD_SECS - 1
        ));
    }

    #[tokio::test]
    async fn a_calendar_cursor_at_its_horizon_restarts_scope_before_the_wire() {
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let cursor = encode_cursor(
            scope,
            GraphCursorPayload::new(
                kind_for_scope(&CursorScope::FolderType {
                    folder: FolderId("calendar".to_string()),
                    ty: ObjectType::Event,
                })
                .expect("event scope maps"),
                &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
                None,
            )
            .with_calendar_window_end_option(Some(0)),
        )
        .expect("cursor encodes");
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    /// A v4 mid-inventory cursor holds a next link and an EMPTY delta link.
    /// Both engine establishment paths route it to
    /// `inventory_resume_stream`, but the changes walk must refuse it on
    /// its own rather than resuming from an empty URL - a scope that
    /// syncs nothing forever is the worst possible failure here.
    #[tokio::test]
    async fn an_in_progress_inventory_cursor_is_not_a_changes_cursor() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let cursor = encode_cursor(
            scope.clone(),
            GraphCursorPayload::inventory_page(
                kind_for_scope(&scope).expect("email scope maps"),
                super::super::inventory::page_marker(
                    &crate::origin::AdmittedUrl::for_tests(
                        "https://graph.microsoft.com/v1.0/next?page=2",
                    ),
                    None,
                ),
                None,
            ),
        )
        .expect("cursor encodes");
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    /// Drain a `changes_stream` that is expected to reject its cursor
    /// before touching the wire, returning the terminal error.
    async fn terminal_kind(cursor: ChangeCursor) -> bifrost_types::AccountError {
        let mut stream = changes_stream(account(), cursor);
        let error = match stream.next().await.expect("an event") {
            SyncEvent::Terminated(error) => error,
            _ => panic!("expected SyncEvent::Terminated"),
        };
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
        assert!(stream.next().await.is_none());
        error
    }

    #[tokio::test]
    async fn a_cursor_minted_by_another_protocol_terminates_schema_incompatible() {
        // The engine persists cursors opaquely; handing a Gmail-tagged
        // cursor to Graph must be rejected at the door, not walked.
        let cursor = ChangeCursor {
            scope: email_scope("inbox"),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Gmail,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[tokio::test]
    async fn a_future_envelope_version_terminates_schema_incompatible() {
        let cursor = ChangeCursor {
            scope: email_scope("inbox"),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION + 1,
                bytes: b"{}".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[tokio::test]
    async fn a_v1_cursor_terminates_on_a_recovery_path_that_reseeds_through_inventory() {
        // v1 is the encoding that minted BARE thread ids for shared
        // mailboxes. Its payload still deserializes - the shape never
        // changed - so nothing but the version rejects it, and resuming it
        // would keep emitting thread ids that parse as primary and route
        // thread hydration and thread-targeted writes at `/me`.
        //
        // The rejection must not be terminal: `SchemaIncompatible` derives
        // `Engine(SchemaIncompatible)`, which is the directive that makes
        // the engine drop every durable cursor and re-establish each scope
        // through a full inventory pass (the pass that re-mints the ids).
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = ChangeCursor {
            scope,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: 1,
                bytes: serde_json::to_vec(&payload).expect("serialize"),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
        assert!(matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::Engine(
                bifrost_types::EngineDirective::SchemaIncompatible
            )
        ));
    }

    /// Cursor bytes that do not decode are the caller's durable state, refused
    /// before any request: never a provider fault. They re-establish like
    /// every other unreadable cursor. (This used to terminate as
    /// `Protocol(ContractViolation)`, blaming Graph for bytes it never sent.)
    #[tokio::test]
    async fn a_garbage_payload_terminates_as_schema_incompatible() {
        let cursor = ChangeCursor {
            scope: email_scope("inbox"),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: b"{not-json".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
        assert!(matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::Engine(
                bifrost_types::EngineDirective::SchemaIncompatible
            )
        ));
    }

    #[tokio::test]
    async fn a_payload_naming_another_collection_terminates_before_any_fetch() {
        // The hostile / stale case that matters: a cursor whose scope says
        // "inbox messages" but whose delta link belongs to the contacts
        // collection. Walking it would file contact rows as messages, so
        // `scope_matches_payload` must terminate the stream.
        let contact_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Contact,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&contact_scope).expect("contact scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        // `encode_cursor` does not cross-check, so the mismatch is
        // constructible exactly as a corrupted persisted cursor would be.
        let cursor = encode_cursor(email_scope("inbox"), payload).expect("encode");
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[tokio::test]
    async fn a_payload_for_a_different_folder_terminates() {
        let payload = GraphCursorPayload::new(
            kind_for_scope(&email_scope("archive")).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = encode_cursor(email_scope("inbox"), payload).expect("encode");
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    /// A persisted foreign scope whose shared mailbox left the
    /// configuration must be disabled, not resumed. The `@odata.deltaLink`
    /// it carries was minted by Graph against `/users/{mailbox}`, so the
    /// walk itself would keep working - and would keep emitting object ids
    /// that hydration and every mutation now refuse locally, because those
    /// paths consult the shared-client map. Inventory already reported
    /// `ScopeRevoked` for the same scope; the two cursor doors have to
    /// agree, or which one the engine calls first decides whether the scope
    /// lives.
    #[tokio::test]
    async fn a_stale_foreign_scope_is_revoked_before_the_delta_link_is_resumed() {
        let stale_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("gone@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&stale_scope).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests(
                "https://graph.microsoft.com/v1.0/users/gone@contoso.com/delta",
            ),
            None,
        );
        let cursor = encode_cursor(stale_scope.clone(), payload).expect("encode");
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        ));
        // Scope-bearing, so the engine disables just this cursor instead of
        // restarting the whole account.
        assert_eq!(error.scope(), Some(&ErrorScope::Cursor(stale_scope)));
        assert!(matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::DisableScope(_))
        ));
    }

    /// A foreign delta-link resume holds the owning mailbox's client for
    /// the WHOLE walk, continuation pages included.
    ///
    /// Both URLs here are absolute links Graph minted, so they are the same
    /// bytes whichever client sends them - the only observable difference
    /// is which client's script answers. The primary is armed with an empty
    /// script so a resume that falls back to it hits the seam's exhaustion
    /// panic instead of quietly borrowing the shared client's queue.
    #[tokio::test]
    async fn every_page_of_a_foreign_delta_resume_rides_the_owner_client() {
        let primary = GraphClient::new("primary-token");
        // Armed, empty: the primary must not be asked for anything.
        primary.script_rest([]);
        let shared = GraphClient::new("shared-token").for_shared_mailbox("shared@contoso.com");
        shared.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{ "id": "m1", "changeKey": "ck1" }],
                    "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta?$skiptoken=p2"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [],
                    "@odata.deltaLink": "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta?$deltatoken=d1"
                }),
            ),
        ]);
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary.clone(),
            PushMode::GraphSubscriptions,
            HashMap::from([("shared@contoso.com".to_string(), shared.clone())]),
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests(
                "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta",
            ),
            None,
        );
        let cursor = encode_cursor(scope, payload).expect("cursor encodes");

        let mut stream = changes_stream(account, cursor);
        assert!(matches!(stream.next().await, Some(SyncEvent::Batch(_))));
        assert!(matches!(stream.next().await, Some(SyncEvent::Batch(_))));
        assert!(matches!(
            stream.next().await,
            Some(SyncEvent::Done(Some(_)))
        ));

        let requests = shared.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url,
            "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta"
        );
        assert_eq!(
            requests[1].url,
            "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta?$skiptoken=p2"
        );
        assert!(
            primary.take_rest_requests().is_empty(),
            "the primary client must not see a foreign-scope request"
        );
    }

    #[tokio::test]
    async fn a_new_delta_value_uses_updated_when_graph_does_not_identify_creation() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "new-to-this-scope" }],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta?token=next"
            }),
        )]);
        let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
        let scope = email_scope("inbox");
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests(
                "https://graph.microsoft.com/v1.0/delta?token=old",
            ),
            None,
        );
        let cursor = encode_cursor(scope, payload).expect("cursor encodes");

        let mut stream = changes_stream(account, cursor);
        let Some(SyncEvent::Batch(batch)) = stream.next().await else {
            panic!("the delta value is emitted");
        };
        assert!(matches!(
            batch.items.as_slice(),
            [
                Change::ObjectChange(ObjectChange {
                    id,
                    kind: ObjectChangeKind::Updated
                }),
                Change::ScopeChange(ScopeChange {
                    id: membership_id,
                    kind: ScopeChangeKind::Added,
                    ..
                })
            ] if id.0 == "new-to-this-scope" && membership_id == id
        ));
    }

    #[tokio::test]
    async fn foreign_delta_page_markers_use_the_projected_object_id() {
        let mailbox = "shared@contoso.com";
        let primary = GraphClient::new("primary-token");
        primary.script_rest([]);
        let shared = GraphClient::new("shared-token").for_shared_mailbox(mailbox);
        shared.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "m1" }],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta?$skiptoken=p2"
            }),
        )]);
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary,
            PushMode::GraphSubscriptions,
            HashMap::from([(mailbox.to_string(), shared)]),
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign(mailbox, "AAMk"),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests(
                "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta",
            ),
            None,
        );
        let cursor = encode_cursor(scope, payload).expect("cursor encodes");

        let mut stream = changes_stream(account, cursor);
        let Some(SyncEvent::Batch(batch)) = stream.next().await else {
            panic!("the delta page is emitted");
        };
        let Some(Checkpoint::Change(cursor)) = batch.checkpoint else {
            panic!("the next page checkpoint is present");
        };
        let marker = decode_cursor(&cursor)
            .expect("page cursor decodes")
            .advanced_through
            .expect("page marker is present");
        assert_eq!(
            marker.last_seen_id.as_deref(),
            Some("shared@contoso.com\u{1f}m1")
        );
    }

    /// A delta server echoing the walk's own resume URL back as its
    /// `nextLink` would spin the changes walk forever - `PageWalk` must
    /// refuse the repeat and project the refusal as `Terminated` rather
    /// than fetching the same page again.
    #[tokio::test]
    async fn a_repeated_next_link_refuses_the_changes_delta_walk() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "m1", "changeKey": "ck1" }],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/delta"
            }),
        )]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let payload = GraphCursorPayload::new(
            kind_for_scope(&email_scope("inbox")).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = encode_cursor(email_scope("inbox"), payload).expect("cursor encodes");
        let mut stream = changes_stream(account, cursor);
        assert!(
            matches!(stream.next().await, Some(SyncEvent::Batch(_))),
            "the first page still delivers its changes"
        );
        let Some(SyncEvent::Terminated(error)) = stream.next().await else {
            panic!("expected SyncEvent::Terminated on the echoed link");
        };
        // A repeated link is a semantic breach of the paging contract by a
        // response that parsed fine, not a parse failure.
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ContractViolation)
        ));
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
        assert!(stream.next().await.is_none());
        assert_eq!(
            client.take_rest_requests().len(),
            1,
            "the echoed link is refused before a second fetch"
        );
    }

    /// An id-less delta value is a checkpoint barrier (the rule the
    /// inventory walk enforces with a Region obligation). The changes lane
    /// must not cross the page: emitting the delta-link checkpoint would
    /// advance the cursor past a value the stream could not represent -
    /// silent, permanent, unreported loss. Decoded siblings still ride a
    /// checkpoint-less batch, and the stream terminates as a contract
    /// violation.
    #[tokio::test]
    async fn an_idless_delta_value_terminates_without_crossing_the_page() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [
                    { "id": "m1", "changeKey": "ck1" },
                    { "subject": "no id at all" }
                ],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta?token=next"
            }),
        )]);
        let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
        let payload = GraphCursorPayload::new(
            kind_for_scope(&email_scope("inbox")).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = encode_cursor(email_scope("inbox"), payload).expect("cursor encodes");

        let mut stream = changes_stream(account, cursor);
        let Some(SyncEvent::Batch(batch)) = stream.next().await else {
            panic!("the decoded sibling still rides a batch");
        };
        assert!(
            batch.checkpoint.is_none(),
            "the malformed page's checkpoint must not be emitted"
        );
        let Some(SyncEvent::Terminated(error)) = stream.next().await else {
            panic!("expected SyncEvent::Terminated on the id-less value");
        };
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ContractViolation)
        ));
        assert!(
            matches!(stream.next().await, Some(SyncEvent::Done(None))),
            "no terminal checkpoint may cross the page"
        );
        assert!(stream.next().await.is_none());
    }

    /// A persisted delta link is the consumer's durable state; one that does
    /// not admit onto the client's origin (a forged or corrupted cursor, or
    /// one minted against another api-base) was resumed verbatim with the
    /// account bearer. It is refused before any request as an unreadable
    /// cursor, so the engine reseeds the scope.
    #[tokio::test]
    async fn a_persisted_link_off_the_api_origin_is_refused_before_any_request() {
        for link in [
            "https://elsewhere.example/v1.0/delta?$deltatoken=d",
            "https://graph.microsoft.com@elsewhere.example/v1.0/delta",
            "http://graph.microsoft.com/v1.0/delta",
        ] {
            let client = GraphClient::new("token");
            client.script_rest([]);
            let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
            let mut payload = GraphCursorPayload::new(
                kind_for_scope(&email_scope("inbox")).expect("email scope maps"),
                &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
                None,
            );
            payload.delta_link = link.to_string();
            let cursor = encode_cursor(email_scope("inbox"), payload).expect("cursor encodes");

            let mut stream = changes_stream(account, cursor);
            let Some(SyncEvent::Terminated(error)) = stream.next().await else {
                panic!("{link}: expected a terminal refusal");
            };
            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
                ),
                "{link}: {:?}",
                error.kind()
            );
            assert_eq!(client.wire_attempts(), 0, "{link} must not be requested");
        }
    }

    /// A delta link a Graph page hands back on another origin is refused at
    /// receipt: never persisted into the checkpoint, never followed.
    #[tokio::test]
    async fn a_foreign_delta_link_is_refused_at_receipt() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "m1", "changeKey": "ck1" }],
                "@odata.deltaLink": "https://elsewhere.example/v1.0/delta?token=next"
            }),
        )]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let payload = GraphCursorPayload::new(
            kind_for_scope(&email_scope("inbox")).expect("email scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = encode_cursor(email_scope("inbox"), payload).expect("cursor encodes");

        let events: Vec<_> = changes_stream(account, cursor).collect().await;
        for event in &events {
            if let SyncEvent::Batch(batch) = event {
                assert!(
                    batch.checkpoint.is_none(),
                    "no checkpoint may carry the link"
                );
            }
        }
        let Some(SyncEvent::Terminated(error)) = events
            .iter()
            .find(|event| matches!(event, SyncEvent::Terminated(_)))
        else {
            panic!("expected a terminal refusal: {events:?}");
        };
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ContractViolation)
        ));
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
        assert_eq!(
            client.wire_attempts(),
            1,
            "the foreign link is not followed"
        );
    }

    #[tokio::test]
    async fn the_event_calendar_event_alias_is_accepted_at_the_changes_door() {
        // A cursor minted from a `CalendarEvent` scope must not be rejected
        // when the engine hands it back tagged `Event` (the Events cursor
        // structurally cannot record which of the two it was). The stream
        // gets past the guard and fails on the wire instead - an unattached
        // client - which is the observable proof the guard let it through.
        let payload = GraphCursorPayload::new(
            kind_for_scope(&CursorScope::FolderType {
                folder: FolderId("calendar".to_string()),
                ty: ObjectType::CalendarEvent,
            })
            .expect("calendar event scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = encode_cursor(
            CursorScope::FolderType {
                folder: FolderId("calendar".to_string()),
                ty: ObjectType::Event,
            },
            payload,
        )
        .expect("encode");
        let error = terminal_kind(cursor).await;
        assert!(
            !matches!(
                error.kind(),
                AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
            ),
            "alias must not be rejected as a schema mismatch: {:?}",
            error.kind()
        );
    }
}
