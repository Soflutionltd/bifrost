//! One REST delta walk for inventory and changes. Public-folder EWS polling
//! has its own protocol and does not pass through this driver.

use bifrost_types::{
    AccountError, AccountOperation, AccountStream, Change, ChangeCursor, Checkpoint, CursorScope,
    ErrorScope, InventoryEntry, InventoryObligation, ObjectChange, ObjectChangeKind, ObjectId,
    PageBoundary, ProtocolErrorKind, ScopeChange, ScopeChangeKind,
};

use super::GraphAccount;
use super::cursor::{GraphCursorPayload, encode_cursor};
use super::graph_error::{GraphErrorContext, cursor_error_to_account_error};
use super::inventory::{coverage_of, graph_etag, inventory_entry_from_parsed, page_marker};
use super::scope::GraphScope;
use super::wire_item::{DeltaState, GraphItem};
use crate::client::Resource;
use crate::origin::AdmittedUrl;
use crate::types::ODataCollection;
use serde_json::Value;

pub(super) enum DeltaLane {
    Inventory { calendar_window_end: Option<i64> },
    Changes(Box<GraphCursorPayload>),
}

pub(super) struct DeltaPage {
    pub(super) entries: Vec<InventoryEntry>,
    pub(super) removed: Vec<ObjectId>,
    pub(super) changes: Vec<Change>,
    pub(super) boundary: PageBoundary,
    pub(super) cursor: Option<ChangeCursor>,
    pub(super) coverage: bifrost_types::InventoryCoverageReport,
    pub(super) bytes_in: u64,
}

pub(super) enum DeltaEvent {
    Page(DeltaPage),
    Terminated(AccountError),
    Done(Checkpoint, bifrost_types::InventoryCoverageReport),
}

pub(super) fn walk(
    account: GraphAccount,
    scope: CursorScope,
    parsed: GraphScope,
    client: crate::client::GraphClient,
    mut current_url: AdmittedUrl,
    mut lane: DeltaLane,
) -> AccountStream<DeltaEvent> {
    Box::pin(async_stream::stream! {
        let operation = match &lane {
            DeltaLane::Inventory { .. } => AccountOperation::SyncInventory,
            DeltaLane::Changes(_) => AccountOperation::SyncChanges,
        };
        let ctx = GraphErrorContext::graph(operation)
            .with_scope(ErrorScope::Cursor(scope.clone()));
        let owner = parsed.owner().map(|owner| bifrost_types::PrincipalId(owner.to_string()));
        let kind = match parsed.kind() {
            Ok(kind) => kind,
            Err(error) => {
                yield DeltaEvent::Terminated(cursor_error_to_account_error(error, ctx));
                return;
            }
        };
        let (client, tally) = client.metered();
        let mut walk = crate::paging::PageWalk::new("Graph delta");
        loop {
            if let Err(error) = walk.enter(&current_url) {
                yield DeltaEvent::Terminated(super::graph_error::graph_shared_scope_error(
                    error, &scope, owner.as_ref(), ctx.clone(),
                ));
                return;
            }
            let page = match fetch_delta_page(&client, &current_url, &parsed).await {
                Ok(page) => page,
                Err(error) => {
                    yield DeltaEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error, &scope, owner.as_ref(), ctx.clone(),
                    ));
                    return;
                }
            };
            // Admit both links before projecting values or mutating the cache.
            // A corrupt provider link must never become a durable checkpoint.
            let links = client.admit_next(page.next_link.as_ref())
                .and_then(|next| Ok((next, client.admit_next(page.delta_link.as_ref())?)));
            let (next_link, delta_link) = match links {
                Ok(links) => links,
                Err(error) => {
                    yield DeltaEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error, &scope, owner.as_ref(), ctx.clone(),
                    ));
                    return;
                }
            };
            let mut entries = Vec::new();
            let mut removed = Vec::new();
            let mut changes = Vec::new();
            let mut obligations = Vec::new();
            let mut last_seen_id = None;
            let mut etag_observations = Vec::new();
            for value in page.value {
                let item = GraphItem::decode(&value);
                // Decide identity once for both lanes, live values and tombstones.
                // An Outlook user expects valid siblings, but no cursor advance
                // past an object we cannot name. Empty ids are equally unusable.
                let Some(id) = item.id else {
                    if obligations.is_empty() {
                        obligations.push(InventoryObligation::Region {
                            key: bifrost_types::ObligationKey(
                                format!("graph:unidentifiable-value:{}", current_url.as_str()).into_bytes(),
                            ),
                            failure_label: format!("{scope:?}:unidentifiable-value"),
                            error: super::graph_error::protocol_violation(
                                ProtocolErrorKind::ContractViolation, operation,
                                Some(ErrorScope::Cursor(scope.clone())),
                                "Graph delta page carried a value with no usable id",
                            ),
                            // A page URL dies with this delta session; it is
                            // not a durable repair capability for lost objects.
                            recovery: bifrost_types::RegionRecovery::CheckpointBarrier {
                                transient_replay: Some(bifrost_types::TransientReplayHint {
                                    token: current_url.as_str().as_bytes().to_vec(),
                                    note: "delta-session page link; dies with the enumeration".into(),
                                }),
                            },
                        });
                    }
                    continue;
                };
                let object_id = parsed.mint_object_id(id);
                last_seen_id = Some(object_id.0.clone());
                if matches!(item.state, DeltaState::Removed) {
                    if parsed.etag_cached() {
                        etag_observations.push((object_id.0.clone(), super::EtagObservation::Invalidated));
                    }
                    match &lane {
                        DeltaLane::Inventory { .. } => removed.push(object_id),
                        DeltaLane::Changes(_) => changes.push(Change::ScopeChange(ScopeChange {
                            id: object_id,
                            membership: parsed.membership(item.parent_folder),
                            kind: ScopeChangeKind::Removed,
                        })),
                    }
                    continue;
                }
                if parsed.etag_cached() && let Some(etag) = graph_etag(&value) {
                    etag_observations.push((object_id.0.clone(), super::EtagObservation::Read(etag)));
                }
                match &lane {
                    DeltaLane::Inventory { .. } => {
                        let mut entry = inventory_entry_from_parsed(&parsed, &value)
                            .expect("delta identity already admitted");
                        if let Some(owner) = &owner {
                            entry.memberships.push(bifrost_types::MembershipScope::Owner(owner.clone()));
                        }
                        entries.push(entry);
                    }
                    DeltaLane::Changes(_) => {
                        // Graph has no creation discriminator. Updated refreshes
                        // an unknown object too; Added describes membership only.
                        changes.push(Change::ObjectChange(ObjectChange {
                            id: object_id.clone(), kind: ObjectChangeKind::Updated,
                        }));
                        changes.push(Change::ScopeChange(ScopeChange {
                            id: object_id, membership: parsed.membership(item.parent_folder),
                            kind: ScopeChangeKind::Added,
                        }));
                    }
                }
            }
            if !etag_observations.is_empty() {
                let mut cache = account.etag_index.write().await;
                // Preserve wire order: a re-added message must retain its new validator.
                for (id, observation) in etag_observations { cache.observe(&id, observation); }
            }
            let coverage = coverage_of(&scope, &obligations);
            let failure = if !obligations.is_empty() {
                Some("Graph delta page carried a value with no usable id")
            } else if next_link.is_none() && delta_link.is_none() {
                Some("Graph delta page carried neither @odata.nextLink nor @odata.deltaLink")
            } else {
                None
            };
            if next_link.is_none() && delta_link.is_none() {
                yield DeltaEvent::Page(DeltaPage {
                    entries, removed, changes, boundary: PageBoundary::Page,
                    cursor: None, coverage, bytes_in: tally.take(),
                });
                yield DeltaEvent::Terminated(super::graph_error::protocol_violation(
                    ProtocolErrorKind::ContractViolation, operation,
                    Some(ErrorScope::Cursor(scope.clone())), failure.expect("missing links"),
                ));
                return;
            }
            let final_page = next_link.is_none();
            let payload = match &mut lane {
                DeltaLane::Inventory { calendar_window_end } => {
                    if let Some(next) = &next_link {
                        GraphCursorPayload::inventory_page(
                            kind.clone(), page_marker(next, last_seen_id), *calendar_window_end,
                        )
                    } else {
                        GraphCursorPayload::new(kind.clone(), delta_link.as_ref().expect("final delta link"), None)
                            .with_calendar_window_end_option(*calendar_window_end)
                    }
                }
                DeltaLane::Changes(payload) => {
                    if let Some(next) = &next_link {
                        payload.set_marker(Some(page_marker(next, last_seen_id))).expect("delta marker");
                    } else {
                        payload.set_delta_link(delta_link.as_ref().expect("final delta link").as_str().to_string());
                    }
                    payload.as_ref().clone()
                }
            };
            let cursor = match encode_cursor(scope.clone(), payload) {
                Ok(cursor) => cursor,
                Err(error) => {
                    yield DeltaEvent::Terminated(cursor_error_to_account_error(error, ctx.clone()));
                    return;
                }
            };
            let checkpoint = Checkpoint::Change(cursor.clone());
            yield DeltaEvent::Page(DeltaPage {
                entries, removed, changes,
                boundary: if final_page && failure.is_none() { PageBoundary::Final } else { PageBoundary::Page },
                cursor: Some(cursor), coverage: coverage.clone(), bytes_in: tally.take(),
            });
            if let Some(message) = failure {
                // Inventory retains the candidate behind PageCheckpoint::Withheld
                // so an operator's explicit waiver can still accept this page.
                // Changes has no obligation vocabulary and suppresses that
                // candidate in its adapter. Both lanes terminate after siblings.
                yield DeltaEvent::Terminated(super::graph_error::protocol_violation(
                    ProtocolErrorKind::ContractViolation, operation,
                    Some(ErrorScope::Cursor(scope.clone())), message,
                ));
                return;
            }
            if final_page {
                yield DeltaEvent::Done(checkpoint, coverage);
                return;
            }
            current_url = next_link.expect("continuation link");
        }
    })
}

async fn fetch_delta_page(
    client: &crate::client::GraphClient,
    url: &AdmittedUrl,
    scope: &GraphScope,
) -> Result<ODataCollection<Value>, crate::error::GraphError> {
    match scope.delta_prefer() {
        Some(prefer) => client.get_prefer(Resource::Outlook, url, prefer).await,
        None => client.get(Resource::Outlook, url).await,
    }
}

#[cfg(test)]
mod tests {
    use bifrost_types::{CursorScope, FolderId, InventoryEvent, ObjectType, SyncEvent};
    use futures::StreamExt;
    use serde_json::json;

    use super::super::PushMode;
    use super::super::changes::changes_stream;
    use super::super::cursor::kind_for_scope;
    use super::super::inventory::inventory_stream;
    use super::*;
    use crate::client::{GraphClient, ScriptedRestResponse};

    fn scope() -> CursorScope {
        CursorScope::FolderType {
            folder: FolderId("inbox".into()),
            ty: ObjectType::Email,
        }
    }

    fn account(page: Value) -> (GraphAccount, GraphClient) {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(reqwest::StatusCode::OK, page)]);
        (
            GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions),
            client,
        )
    }

    fn cursor() -> ChangeCursor {
        encode_cursor(
            scope(),
            GraphCursorPayload::new(
                kind_for_scope(&scope()).expect("mail kind"),
                &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
                None,
            ),
        )
        .expect("cursor")
    }

    #[tokio::test]
    async fn malformed_identities_stop_both_lanes_before_the_next_page() {
        for malformed in [
            json!({}),
            json!({"id": null}),
            json!({"id": 1}),
            json!({"id": ""}),
            json!({"@removed": null}),
            json!({"id": "", "@removed": {}}),
        ] {
            for link in ["@odata.nextLink", "@odata.deltaLink"] {
                let mut page = json!({"value": [
                    {"id": "valid", "changeKey": "ck"},
                    {"id": "gone", "@removed": {}},
                    malformed.clone(),
                ]});
                page[link] = json!("https://graph.microsoft.com/v1.0/next");
                let (inventory, inventory_client) = account(page.clone());
                let (changes, changes_client) = account(page);
                let inventory_events: Vec<_> = inventory_stream(inventory, scope()).collect().await;
                let changes_events: Vec<_> = changes_stream(changes, cursor()).collect().await;
                let [
                    InventoryEvent::Batch(batch),
                    InventoryEvent::Terminated(error),
                ] = inventory_events.as_slice()
                else {
                    panic!("inventory must deliver siblings then terminate: {inventory_events:?}");
                };
                assert_eq!(batch.items.len(), 1);
                assert_eq!(batch.items[0].id.0, "valid");
                assert_eq!(batch.removed, vec![ObjectId("gone".into())]);
                assert!(batch.coverage.has_barrier());
                assert!(batch.checkpoint.accepted(false).is_none());
                assert!(
                    batch.checkpoint.accepted(true).is_some(),
                    "retain the explicit waiver capability"
                );
                assert_eq!(error.operation(), Some(AccountOperation::SyncInventory));
                let [SyncEvent::Batch(batch), SyncEvent::Terminated(error)] =
                    changes_events.as_slice()
                else {
                    panic!("changes must deliver siblings then terminate: {changes_events:?}");
                };
                assert_eq!(batch.items.len(), 3);
                assert!(batch.checkpoint.is_none());
                assert_eq!(error.operation(), Some(AccountOperation::SyncChanges));
                assert_eq!(inventory_client.wire_attempts(), 1);
                assert_eq!(changes_client.wire_attempts(), 1);
            }
        }
    }

    #[tokio::test]
    async fn invalid_links_refuse_both_lanes_before_projection_or_etag_updates() {
        for link in ["@odata.nextLink", "@odata.deltaLink"] {
            let mut page = json!({"value": [{"id": "valid", "changeKey": "ck"}]});
            page[link] = json!("https://elsewhere.example/delta");
            let (inventory, _) = account(page.clone());
            let (changes, _) = account(page);
            let inventory_events: Vec<_> =
                inventory_stream(inventory.clone(), scope()).collect().await;
            let changes_events: Vec<_> = changes_stream(changes.clone(), cursor()).collect().await;
            assert!(matches!(
                inventory_events.as_slice(),
                [InventoryEvent::Terminated(_)]
            ));
            assert!(matches!(
                changes_events.as_slice(),
                [SyncEvent::Terminated(_)]
            ));
            assert!(inventory.etag_index.write().await.get("valid").is_none());
            assert!(changes.etag_index.write().await.get("valid").is_none());
        }
    }

    #[tokio::test]
    async fn both_lanes_finish_only_after_a_final_checkpoint() {
        let page =
            json!({"value": [], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta"});
        let (inventory, _) = account(page.clone());
        let (changes, _) = account(page);
        let inventory_events: Vec<_> = inventory_stream(inventory, scope()).collect().await;
        let changes_events: Vec<_> = changes_stream(changes, cursor()).collect().await;
        assert!(matches!(
            inventory_events.as_slice(),
            [InventoryEvent::Batch(_), InventoryEvent::Done(_)]
        ));
        assert!(matches!(
            changes_events.as_slice(),
            [SyncEvent::Batch(_), SyncEvent::Done(Some(_))]
        ));
    }
}
