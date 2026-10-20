use std::collections::HashSet;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchSuccess, Checkpoint,
    CursorScope, ErrorScope, FolderId, HydratedObject, HydratedObjectKind, ItemOutcome,
    MembershipScope, ObjectId, ObjectType, PageBoundary, Projection, SyncEvent,
};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde_json::Value;

use crate::error::GraphResponseError;
use crate::types::{BatchRequest, BatchRequestItem, BatchResponse, MESSAGE_SELECT};

use super::GraphAccount;
use super::blob::blob_handle_from_graph_attachment;
use super::graph_error::{GraphErrorContext, into_account_error, response_to_account_error_pub};
use super::inventory::{graph_etag, inventory_entry_from_value};

pub(crate) fn get_stream(
    account: GraphAccount,
    mut ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    Box::pin(async_stream::stream! {
        let max_items = account.capabilities.batching_policy.max_items.max(1);
        let mut chunk = Vec::with_capacity(max_items);
        // The input is a stream, so "is this the last chunk" is only
        // known after the input ends. Hold a filled chunk back by one
        // step: once the stream is exhausted, the held chunk (if any) is
        // the final batch and carries `PageBoundary::Final`, matching the
        // inventory/changes streams (a hydration that fired chunks tagged
        // only `Page` never told the engine where the stream terminated).
        let mut pending: Option<Vec<ObjectId>> = None;
        while let Some(id) = ids.next().await {
            chunk.push(id);
            if chunk.len() >= max_items {
                if let Some(ready) = pending.take() {
                    match fetch_batch(&account, &ready, projection, false).await {
                        Ok(batch_events) => for event in batch_events { yield event; },
                        Err(error) => {
                            let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                                .with_scope(ErrorScope::Account);
                            yield SyncEvent::Terminated(into_account_error(error, ctx));
                            yield SyncEvent::Done(None);
                            return;
                        }
                    }
                }
                pending = Some(std::mem::replace(&mut chunk, Vec::with_capacity(max_items)));
            }
        }

        // Flush the held-back full chunk (non-final iff a trailing
        // partial chunk follows) then the trailing partial chunk.
        let trailing_nonempty = !chunk.is_empty();
        if let Some(ready) = pending.take() {
            match fetch_batch(&account, &ready, projection, !trailing_nonempty).await {
                Ok(batch_events) => for event in batch_events { yield event; },
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                        .with_scope(ErrorScope::Account);
                    yield SyncEvent::Terminated(into_account_error(error, ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        if trailing_nonempty {
            match fetch_batch(&account, &chunk, projection, true).await {
                Ok(batch_events) => for event in batch_events { yield event; },
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                        .with_scope(ErrorScope::Account);
                    yield SyncEvent::Terminated(into_account_error(error, ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

/// Split a hydration chunk into the ids that must be read over EWS
/// `GetItem` and the ids that go through the Graph REST `$batch`.
///
/// A public-folder item is a raw EWS `ItemId` carried inside a
/// folder-qualified `ObjectId`; Graph REST has no route that can address it
/// (`/me/messages/{ItemId}` 404s), so before this split every public-folder
/// item hydrated as `Failed`. Membership in the routing map is the
/// discriminator - a folder-qualified id whose folder was never discovered
/// falls back to the REST arm, which reports the real miss rather than a
/// fabricated local error.
async fn partition_ews_ids(
    account: &GraphAccount,
    ids: &[ObjectId],
) -> (Vec<(ObjectId, FolderId)>, Vec<ObjectId>) {
    let mut ews = Vec::new();
    let mut rest = Vec::new();
    for id in ids {
        match super::pim::ews_read_folder(id) {
            Some(folder) => {
                if account.public_folder_routing(&folder).await.is_some() {
                    ews.push((id.clone(), folder));
                } else {
                    rest.push(id.clone());
                }
            }
            None => rest.push(id.clone()),
        }
    }
    (ews, rest)
}

/// Hydrate the public-folder ids of a chunk through EWS `GetItem`, one
/// request per item (EWS `GetItem` takes an id list, but each id can sit in
/// a different public folder with a different routing header pair, so the
/// per-folder routing is what forces the fan-out).
///
/// ACCEPTED for now, with a known fix shape: ids that share a public folder
/// share a routing header pair, so they could be grouped and sent as a single
/// `<m:ItemIds>` list, cutting a 20-id chunk drawn from one folder to one
/// round trip. It is not done because it buys nothing at the sizes this lane
/// currently sees - public folders are opt-in and pinned explicitly, so a
/// chunk is typically a handful of items - and grouping costs a second
/// per-item reconciliation (EWS answers a multi-id `GetItem` with a response
/// message per id, which then has to be mapped back onto the submitted order
/// to keep the one-outcome-per-id contract this function already satisfies
/// trivially). Do it when a consumer pins a genuinely large public folder and
/// the per-item round trips show up as hydration latency.
async fn fetch_ews_outcomes(
    account: &GraphAccount,
    ids: &[(ObjectId, FolderId)],
    projection: Projection,
) -> Vec<ItemOutcome<HydratedObject>> {
    let mut outcomes = Vec::new();
    for (id, folder) in ids {
        let batch_id = BatchItemId(id.0.clone());
        let Some(routing) = account.public_folder_routing(folder).await else {
            // The routing map starts empty on every reopen and is filled by
            // public-folder discovery, which can skip a folder after a
            // per-folder failure, so a legitimately minted id can miss here.
            // Refused per item, before any EWS request, rather than poisoning
            // the rest of the chunk - and neither the caller's fault nor the
            // provider's: local state that no longer matches the id.
            outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                batch_id,
                super::graph_error::stale_local_state_error(
                    AccountOperation::Hydrate,
                    ErrorScope::Message {
                        id: (id.0.clone()).into(),
                    },
                    format!("public folder {} has no routing entry", folder.0),
                ),
            )));
            continue;
        };
        let ews = match super::public_folder::ews_client(account) {
            Ok(ews) => ews,
            Err(error) => {
                outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                    batch_id,
                    super::graph_error::ews_error_to_account_error(
                        error,
                        GraphErrorContext::ews(AccountOperation::Hydrate).with_scope(
                            ErrorScope::Message {
                                id: (id.0.clone()).into(),
                            },
                        ),
                    ),
                )));
                continue;
            }
        };
        let native = super::foreign::parse_message_id(id).native_id().to_string();
        // Class-conditional property shape: a contact / calendar item in a
        // mixed public folder answers a message-shaped GetItem with
        // `ErrorInvalidPropertyRequest`.
        let shape = account.public_item_shape(id, folder).await;
        match ews.get_item(&native, shape, &routing.headers()).await {
            Ok(item) if !item.is_mail() => outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                batch_id,
                super::graph_error::unsupported_item_error(
                    AccountOperation::Hydrate,
                    ErrorScope::Message {
                        id: id.0.clone().into(),
                    },
                ),
            ))),
            Ok(item) => outcomes.push(ItemOutcome::Succeeded(BatchSuccess::new(
                batch_id,
                hydrated_from_ews_item(
                    id.clone(),
                    &item,
                    folder,
                    &routing.anchor_mailbox,
                    projection,
                ),
            ))),
            Err(error) => outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                batch_id,
                super::graph_error::ews_error_to_account_error(
                    error,
                    GraphErrorContext::ews(AccountOperation::Hydrate).with_scope(
                        ErrorScope::Message {
                            id: (id.0.clone()).into(),
                        },
                    ),
                ),
            ))),
        }
    }
    outcomes
}

/// Project an EWS `GetItem` result into the SAME `HydratedObject` shape the
/// Graph REST arm returns.
///
/// `Metadata` reuses the public-folder inventory projection, so a hydrated
/// item is byte-identical to its inventory entry (same id, same memberships,
/// same change-key fingerprint). The body-bearing projections degrade to
/// `Metadata` for the same reason the REST arm does: `HydratedObjectKind` can
/// only carry assembled RFC822 in `RawMime`, and an EWS `GetItem` returns a
/// parsed HTML body, not MIME octets - minting it as `RawMime` would violate
/// that contract. Attachment DESCRIPTORS still ride out as blob handles, so
/// the consumer can pull the bytes through `open_blob`.
///
/// Pure over the already-fetched item, so the projection is unit-pinnable
/// without a live EWS server.
pub(crate) fn hydrated_from_ews_item(
    id: ObjectId,
    item: &crate::ews::EwsItem,
    folder: &FolderId,
    content_mailbox: &str,
    projection: Projection,
) -> HydratedObject {
    let kind = match projection {
        Projection::FlagsOnly => HydratedObjectKind::FlagsOnly(ews_flags(item)),
        _ => HydratedObjectKind::Metadata(super::public_folder::item_to_inventory_entry(
            item,
            folder,
            content_mailbox,
        )),
    };
    let blobs = item
        .attachments
        .iter()
        .map(|attachment| super::blob::blob_handle_from_ews_attachment(&id, attachment))
        .collect();
    HydratedObject { id, kind, blobs }
}

/// The canonical flag set an EWS item carries. EWS surfaces only the
/// read/unread bit on this shape (no categories, no flag status), so
/// `\seen` is the whole vocabulary.
fn ews_flags(item: &crate::ews::EwsItem) -> HashSet<String> {
    let mut flags = HashSet::new();
    if item.is_read {
        flags.insert("\\seen".to_string());
    }
    flags
}

/// Project a hydration chunk into `ItemOutcome` envelopes.
///
/// Public-folder ids are served by EWS `GetItem`, everything else by the
/// Graph `/$batch`; both arms land in ONE `Batch` so the consumer still sees
/// exactly one outcome per pulled id. Per-item 2xx hydrate; per-item 4xx/5xx
/// (or an EWS SOAP fault) emit `ItemOutcome::Failed` with a structured
/// `AccountError` (Protocol::Graph / Protocol::Ews, AttemptCause
/// Acknowledged, classified `RecoveryClass`). Locally-invalid items (no body
/// where one is required) also emit `Failed` rather than poisoning the rest
/// of the batch.
async fn fetch_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    projection: Projection,
    is_final: bool,
) -> Result<Vec<SyncEvent<ItemOutcome<HydratedObject>>>, crate::error::GraphError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    // One hydration chunk is one logical batch. Both its Graph REST and
    // buffered EWS responses enroll in this accumulator.
    let (metered, tally) = account.metered();
    let account = &metered;
    // Public-folder ids read over EWS; everything else over Graph REST.
    let (ews_ids, rest_ids) = partition_ews_ids(account, ids).await;
    // The EWS-hydrated items ride in the same batch as the REST ones: the
    // consumer sees one outcome per pulled id regardless of which arm served
    // it - or of whether its arm could build a request at all.
    let mut outcomes = fetch_ews_outcomes(account, &ews_ids, projection).await;

    let select = select_for_projection(projection);
    // Decode the (possibly foreign-encoded) id: a shared-mailbox item routes
    // to `/users/{owner}/messages/{native}`, a primary item to
    // `/me/messages/{id}`. The owning mailbox rides in the path prefix, the
    // native id in the `/messages/{id}` segment.
    //
    // An id whose shared mailbox is no longer configured has no endpoint at
    // all. That is ONE id's failure: propagating it would discard the valid
    // REST siblings of the same chunk AND the EWS outcomes already fetched
    // above, so it is filed on the failed lane and the chunk goes on.
    let (routable, rejected) = super::batch_routing::partition_routable(&rest_ids, |id| {
        hydrate_url_for_id(account, id, select)
    });
    for (id, error) in rejected {
        let ctx =
            GraphErrorContext::graph(AccountOperation::Hydrate).with_scope(ErrorScope::Message {
                id: (id.0.clone()).into(),
            });
        outcomes.push(ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            into_account_error(error, ctx),
        )));
    }
    let (rest_ids, urls): (Vec<ObjectId>, Vec<String>) = routable.into_iter().unzip();
    if rest_ids.is_empty() {
        // Nothing left to ask Graph. An empty `requests` array is a 400,
        // and every id already holds an outcome.
        return Ok(vec![hydration_batch(outcomes, is_final, tally.take())]);
    }
    // The subrequest index is assigned AFTER the routing split, so it
    // indexes `rest_ids` and `reconcile_hydration_responses` can project
    // responses back onto exactly the ids that were sent.
    let requests = urls
        .into_iter()
        .enumerate()
        .map(|(index, url)| BatchRequestItem {
            id: index.to_string(),
            method: "GET".to_string(),
            url,
            body: None,
            headers: None,
        })
        .collect();
    let request = BatchRequest { requests };
    let response: BatchResponse = account
        .client
        .post_batch(account.batch_resource(&rest_ids)?, &request)
        .await?;
    let etags = reconcile_hydration_responses(
        &rest_ids,
        response.responses,
        projection,
        &mut outcomes,
        &|id| account.budget_for_object(id),
    );

    if !etags.is_empty() {
        let mut cache = account.etag_index.write().await;
        for (id, etag) in etags {
            cache.insert(id, etag);
        }
    }

    Ok(vec![hydration_batch(outcomes, is_final, tally.take())])
}

/// Wrap one chunk's accumulated outcomes in the `Batch` envelope, tagging
/// the boundary the streaming loop computed.
fn hydration_batch(
    items: Vec<ItemOutcome<HydratedObject>>,
    is_final: bool,
    bytes_in: u64,
) -> SyncEvent<ItemOutcome<HydratedObject>> {
    SyncEvent::Batch(Batch {
        items,
        page_boundary: if is_final {
            PageBoundary::Final
        } else {
            PageBoundary::Page
        },
        server_latency: std::time::Duration::default(),
        bytes_in,
        checkpoint: None::<Checkpoint>,
    })
}

/// Project the REST arm's `$batch` responses onto the submitted ids,
/// appending one `ItemOutcome` per id to `outcomes` and returning the
/// `(id, etag)` pairs harvested from successful bodies.
///
/// Pure over the decoded response, so the accounting rules below are
/// unit-pinnable without a live `$batch` endpoint. Three of them are
/// non-obvious:
///
/// - A response id that does not parse as a submitted index, or parses out
///   of range, is DISCARDED rather than turned into an outcome. Minting an
///   `ObjectId` from the response id would inject an id the caller never
///   asked for while the real one stays unanswered.
/// - A duplicate response id is discarded for the same reason: the first
///   answer already claimed that lane slot, and a second would break the
///   one-outcome-per-id contract.
/// - Any submitted id left unanswered gets `Protocol(PartialResponse)`,
///   not a terminal contract violation. Hydration is idempotent, so the
///   consumer re-reads; a terminal class would drop the id permanently for
///   a condition the next GET usually clears.
fn reconcile_hydration_responses(
    ids: &[ObjectId],
    responses: Vec<crate::types::BatchResponseItem>,
    projection: Projection,
    outcomes: &mut Vec<ItemOutcome<HydratedObject>>,
    // The mailbox budget each id's sub-request was charged to
    // (`GraphAccount::budget_for_object`), stamped on a throttled item. The
    // outer `$batch` mixes mailboxes; each sub-request does not.
    budget_for: &dyn Fn(&ObjectId) -> Option<bifrost_types::ThrottleBudget>,
) -> Vec<(String, String)> {
    let mut etags = Vec::new();
    let mut seen_indices = HashSet::new();

    for item in responses {
        let Some(index) = item.id.parse::<usize>().ok().filter(|idx| *idx < ids.len()) else {
            tracing::warn!(
                target: "bifrost_graph::batch",
                response_id = %item.id,
                "ignoring Graph $batch hydration response with an invalid request id"
            );
            continue;
        };
        if !seen_indices.insert(index) {
            tracing::warn!(
                target: "bifrost_graph::batch",
                response_id = %item.id,
                "ignoring duplicate Graph $batch hydration response"
            );
            continue;
        }
        let id = ids[index].clone();
        let batch_id = BatchItemId(id.0.clone());
        if !(200..=299).contains(&item.status) {
            // Per-item failure: build a structured AccountError via
            // the same response classifier the top-level boundary
            // uses. Headers and body flow through so retry-after,
            // throttle scope, and inner error envelope are preserved.
            let headers = item
                .headers
                .as_ref()
                .map(reconstruct_headers)
                .unwrap_or_default();
            let body = item
                .body
                .as_ref()
                .map(|v| Bytes::from(serde_json::to_vec(v).unwrap_or_default()))
                .unwrap_or_default();
            let response = GraphResponseError::from_response(
                StatusCode::from_u16(item.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                headers,
                body,
            );
            let mut ctx = GraphErrorContext::graph(AccountOperation::Hydrate).with_scope(
                ErrorScope::Message {
                    id: (id.0.clone()).into(),
                },
            );
            ctx.budget = budget_for(&id);
            let error = response_to_account_error_pub(response, &ctx);
            outcomes.push(ItemOutcome::Failed(BatchFailure::new(batch_id, error)));
            continue;
        }
        let Some(body) = item.body else {
            // 2xx with no body where a hydration was expected: the
            // provider promised a payload it did not ship. Surface
            // as a per-item failure so the consumer can decide
            // whether to retry or drop the id.
            let error = super::graph_error::protocol_violation(
                bifrost_types::ProtocolErrorKind::MissingField,
                AccountOperation::Hydrate,
                Some(ErrorScope::Message {
                    id: (id.0.clone()).into(),
                }),
                format!("Graph $batch GET for {} returned 2xx with no body", id.0),
            );
            outcomes.push(ItemOutcome::Failed(BatchFailure::new(batch_id, error)));
            continue;
        };
        if let Some(etag) = graph_etag(&body) {
            etags.push((id.0.clone(), etag));
        }
        let hydrated = hydrated_from_value(id, &body, projection);
        outcomes.push(ItemOutcome::Succeeded(BatchSuccess::new(
            batch_id, hydrated,
        )));
    }

    for (index, id) in ids.iter().enumerate() {
        if seen_indices.contains(&index) {
            continue;
        }
        outcomes.push(ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::graph_error::batch_response_missing(
                AccountOperation::Hydrate,
                Some(ErrorScope::Message {
                    id: (id.0.clone()).into(),
                }),
                format!("Graph $batch returned no response for {}", id.0),
            ),
        )));
    }

    etags
}

fn reconstruct_headers(h: &std::collections::HashMap<String, String>) -> HeaderMap {
    let mut hm = HeaderMap::new();
    for (k, v) in h {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(v),
        ) {
            hm.insert(name, val);
        }
    }
    hm
}

fn hydrated_from_value(id: ObjectId, value: &Value, projection: Projection) -> HydratedObject {
    let kind = match projection {
        Projection::FlagsOnly => HydratedObjectKind::FlagsOnly(flags_from_value(value)),
        // Graph's JSON message resource is not assembled RFC822, so the
        // body-bearing projections cannot honestly produce `RawMime`
        // here (the prior code minted a JSON blob, violating the
        // RawMime = RFC822-bytes contract). A3 has no per-item `$value`
        // fetch in the hydration path (that N+1 is A1's call), so these
        // projections degrade to `Metadata` (falling back to `FlagsOnly`
        // when the inventory entry cannot be built). The assembled bytes
        // come exclusively through the dedicated `open_raw_rfc822` read.
        Projection::Metadata
        | Projection::Headers
        | Projection::Preview(_)
        | Projection::TextOnly
        | Projection::Full
        | Projection::FullWithBlobs => metadata_or_flags(&id, value),
        _ => HydratedObjectKind::FlagsOnly(flags_from_value(value)),
    };
    let blobs = value
        .get("attachments")
        .and_then(Value::as_array)
        .map(|attachments| {
            attachments
                .iter()
                .filter_map(|attachment| blob_handle_from_graph_attachment(&id, attachment))
                .collect()
        })
        .unwrap_or_default();

    HydratedObject { id, kind, blobs }
}

/// Build a `Metadata` kind from the Graph message JSON, falling back to
/// `FlagsOnly` when an inventory entry cannot be constructed. Shared by
/// the `Metadata` projection and the body-bearing projections that A3
/// degrades to metadata (the real body path is `open_raw_rfc822`).
fn metadata_or_flags(id: &ObjectId, value: &Value) -> HydratedObjectKind {
    let folder = value
        .get("parentFolderId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let parsed_id = super::foreign::parse_message_id(id);
    let scope = CursorScope::FolderType {
        folder: match parsed_id.owner() {
            Some(owner) => super::foreign::encode_foreign(owner, folder),
            None => FolderId(folder.to_string()),
        },
        ty: ObjectType::Email,
    };
    inventory_entry_from_value(&scope, value).map_or_else(
        || HydratedObjectKind::FlagsOnly(flags_from_value(value)),
        |mut entry| {
            if let Some(owner) = parsed_id.owner() {
                entry
                    .memberships
                    .push(MembershipScope::Mailbox(bifrost_types::MailboxId(
                        owner.to_string(),
                    )));
            }
            HydratedObjectKind::Metadata(entry)
        },
    )
}

fn flags_from_value(value: &Value) -> HashSet<String> {
    let mut flags = HashSet::new();
    if value
        .get("isRead")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        flags.insert("\\seen".to_string());
    }
    if value
        .get("flag")
        .and_then(|flag| flag.get("flagStatus"))
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("flagged"))
    {
        flags.insert("\\flagged".to_string());
    }
    if let Some(categories) = value.get("categories").and_then(Value::as_array) {
        for category in categories {
            if let Some(category) = category.as_str() {
                flags.insert(format!("category:{category}"));
            }
        }
    }
    flags
}

/// `$select` list for a hydration projection. Message fields only - see
/// `hydrate_url_for_id` for why this lane is mail-shaped by contract.
fn select_for_projection(projection: Projection) -> &'static str {
    match projection {
        Projection::FlagsOnly => "id,isRead,categories,flag,changeKey,parentFolderId",
        Projection::Metadata | Projection::Headers | Projection::Preview(_) => MESSAGE_SELECT,
        Projection::TextOnly | Projection::Full | Projection::FullWithBlobs => {
            "id,body,uniqueBody,attachments,isRead,categories,flag,changeKey,parentFolderId"
        }
        _ => "id,isRead,categories,flag,changeKey,parentFolderId",
    }
}

pub(crate) fn folder_destination(destination: MembershipScope) -> Option<FolderId> {
    match destination {
        MembershipScope::Folder(folder) => Some(folder),
        _ => None,
    }
}

/// Build the per-id Graph `/$batch` GET URL for hydration, decoding the
/// (possibly foreign-encoded) id to route to `/users/{owner}` vs `/me`.
/// Extracted so the routing is unit-testable without a live `$batch`.
///
/// Deliberately message-shaped for every id, even though this account also
/// establishes `ObjectType::Event` and `ObjectType::Contact` cursor scopes.
/// `get_stream` is the MAIL hydration lane across this workspace: the
/// standalone calendar and contact account crates answer it with
/// `unsupported_stream(AccountOperation::Hydrate)`, the sync engine's only
/// internal callers are the mutation read-back paths (which hydrate ids the
/// caller just put through the mail mutation API at `FlagsOnly` /
/// `Metadata`), and calendar events and contacts are read through their own
/// typed surfaces instead - `calendar::get` by `EventId`, the contacts API
/// by contact id - which address `/events/{id}` and `/contacts/{id}`
/// themselves. An `ObjectId` carries no object-type marker, so this function
/// could not route by kind even if it wanted to; the contract is that
/// non-mail ids do not arrive here.
fn hydrate_url_for_id(
    account: &GraphAccount,
    id: &ObjectId,
    select: &str,
) -> Result<String, crate::error::GraphError> {
    let parsed = super::foreign::parse_message_id(id);
    let prefix = account.client_for_owner(parsed.owner())?.api_path_prefix();
    let enc_id = bifrost_net::url::encode_path_component(parsed.native_id());
    Ok(format!("{prefix}/messages/{enc_id}?$select={select}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::client::GraphClient;
    use crate::types::BatchResponseItem;
    use serde_json::json;

    fn ok_response(id: &str, message_id: &str) -> BatchResponseItem {
        BatchResponseItem {
            id: id.to_string(),
            status: 200,
            headers: None,
            body: Some(json!({ "id": message_id, "parentFolderId": "inbox" })),
        }
    }

    fn outcome_id(outcome: &ItemOutcome<HydratedObject>) -> String {
        match outcome {
            ItemOutcome::Succeeded(success) => success.item.0.clone(),
            ItemOutcome::Failed(failure) => failure.item.0.clone(),
            ItemOutcome::Uncertain(uncertain) => uncertain.item.0.clone(),
        }
    }

    /// Graph is documented to return FEWER `$batch` responses than
    /// requests under partial failure or throttling. The unanswered ids
    /// must still reach the consumer on exactly one lane, and must arrive
    /// as a retryable `Protocol(PartialResponse)`: a GET is idempotent and
    /// the omission says nothing about the resource, so a terminal
    /// contract violation would drop the id for good.
    #[test]
    fn unanswered_hydration_ids_are_retryable_partial_responses() {
        let ids = vec![
            ObjectId("m0".to_string()),
            ObjectId("m1".to_string()),
            ObjectId("m2".to_string()),
        ];
        let mut outcomes = Vec::new();
        let etags = reconcile_hydration_responses(
            &ids,
            vec![ok_response("1", "m1")],
            Projection::Metadata,
            &mut outcomes,
            &|_| None,
        );

        assert!(etags.is_empty());
        assert_eq!(outcomes.len(), 3);
        let mut reported: Vec<String> = outcomes.iter().map(outcome_id).collect();
        reported.sort();
        assert_eq!(reported, vec!["m0", "m1", "m2"]);

        for outcome in &outcomes {
            match outcome {
                ItemOutcome::Succeeded(success) => assert_eq!(success.item.0, "m1"),
                ItemOutcome::Failed(failure) => {
                    assert!(matches!(
                        failure.error.kind(),
                        bifrost_types::AccountErrorKind::Protocol(
                            bifrost_types::ProtocolErrorKind::PartialResponse
                        )
                    ));
                    assert!(failure.error.recovery().is_retryable());
                }
                ItemOutcome::Uncertain(_) => panic!("hydration has no uncertain lane"),
            }
        }
    }

    /// A response id that is not a submitted index is DISCARDED, not
    /// turned into an `ObjectId`. Minting one would hand the consumer an
    /// id it never asked for while the id it did ask for stays silently
    /// unanswered.
    #[test]
    fn out_of_range_and_unparsable_hydration_response_ids_are_discarded() {
        let ids = vec![ObjectId("m0".to_string())];
        let mut outcomes = Vec::new();
        reconcile_hydration_responses(
            &ids,
            vec![
                ok_response("7", "stranger"),
                ok_response("not-a-number", "stranger"),
            ],
            Projection::Metadata,
            &mut outcomes,
            &|_| None,
        );

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcome_id(&outcomes[0]), "m0");
        assert!(matches!(outcomes[0], ItemOutcome::Failed(_)));
    }

    /// A repeated response id answers a lane slot that is already
    /// claimed. Accepting it would emit two outcomes for one submitted id
    /// and break the one-outcome-per-id contract the batch model rests on.
    #[test]
    fn duplicate_hydration_response_ids_yield_one_outcome() {
        let ids = vec![ObjectId("m0".to_string())];
        let mut outcomes = Vec::new();
        reconcile_hydration_responses(
            &ids,
            vec![ok_response("0", "m0"), ok_response("0", "m0")],
            Projection::Metadata,
            &mut outcomes,
            &|_| None,
        );

        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0], ItemOutcome::Succeeded(_)));
    }

    #[test]
    fn hydrate_routes_foreign_id_to_owner_and_primary_to_me() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let foreign_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        // A foreign-scope mint encodes the owner into the message id; the
        // request site decodes it back to `/users/{owner}/messages/{native}`.
        let foreign_id = super::super::foreign::encode_message_id(&foreign_scope, "AAMkmsg");
        assert_eq!(
            hydrate_url_for_id(&account, &foreign_id, "id").expect("configured"),
            "/users/shared%40contoso.com/messages/AAMkmsg?$select=id"
        );
        // A primary id stays bare and routes through `/me`.
        let primary_id = ObjectId("AAMkmsg".to_string());
        assert_eq!(
            hydrate_url_for_id(&account, &primary_id, "id").expect("primary"),
            "/me/messages/AAMkmsg?$select=id"
        );
    }

    /// A stale shared-mailbox id costs ITSELF a hydration outcome and
    /// nothing else. `fetch_batch` used to `?` the URL-construction error
    /// out of the chunk builder, which discarded the valid REST siblings
    /// AND the EWS outcomes already fetched for the same chunk - a
    /// per-request answer on a per-item surface, the mistake the `$batch`
    /// reconcilers had to unlearn once already.
    ///
    /// The routing split is what is pinned here; the round trip the
    /// routable half then makes is pinned separately through the
    /// `GraphClient` REST seam.
    #[test]
    fn a_stale_foreign_id_is_split_out_and_its_siblings_still_route() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let live_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let stale_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("gone@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let ids = [
            ObjectId("AAMkprimary".to_string()),
            super::super::foreign::encode_message_id(&stale_scope, "AAMkstale"),
            super::super::foreign::encode_message_id(&live_scope, "AAMkshared"),
        ];
        let (routable, rejected) = super::super::batch_routing::partition_routable(&ids, |id| {
            hydrate_url_for_id(&account, id, "id")
        });
        let urls: Vec<&str> = routable.iter().map(|(_, url)| url.as_str()).collect();
        assert_eq!(
            urls,
            [
                "/me/messages/AAMkprimary?$select=id",
                "/users/shared%40contoso.com/messages/AAMkshared?$select=id"
            ]
        );
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].0, ids[1]);

        // And the outcome the rejected id gets names the message, so the
        // consumer can tell which id it must stop pulling.
        let (id, error) = rejected.into_iter().next().expect("one rejection");
        let account_error = into_account_error(
            error,
            GraphErrorContext::graph(AccountOperation::Hydrate).with_scope(ErrorScope::Message {
                id: (id.0.clone()).into(),
            }),
        );
        assert!(matches!(
            account_error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(
            account_error.scope(),
            Some(&ErrorScope::Message {
                id: (id.0.clone()).into()
            })
        );
    }

    fn ews_item() -> crate::ews::EwsItem {
        crate::ews::EwsItem {
            wire_kind: "Message".to_string(),
            item_id: "AAMkItem=".to_string(),
            change_key: Some("CK1".to_string()),
            subject: Some("Company Policy".to_string()),
            sender_email: Some("hr@contoso.com".to_string()),
            sender_name: Some("HR".to_string()),
            received_at: Some("2026-03-01T10:30:00Z".to_string()),
            body_preview: None,
            body_html: Some("<p>Read this</p>".to_string()),
            is_read: true,
            flag_status: None,
            categories: Vec::new(),
            item_class: "IPM.Note".to_string(),
            to_recipients: Vec::new(),
            cc_recipients: Vec::new(),
            attachments: vec![
                crate::ews::EwsAttachment {
                    attachment_id: "AAMkAtt1=".to_string(),
                    name: Some("policy.pdf".to_string()),
                    content_type: Some("application/pdf".to_string()),
                    size: Some(2048),
                    is_inline: false,
                    is_item: false,
                },
                crate::ews::EwsAttachment {
                    attachment_id: "AAMkAtt2=".to_string(),
                    name: Some("Forwarded".to_string()),
                    content_type: None,
                    size: None,
                    is_inline: false,
                    is_item: true,
                },
            ],
        }
    }

    /// The EWS arm projects into the SAME `HydratedObject` shape the Graph
    /// REST arm does: `Metadata` for the body-bearing projections (never
    /// `RawMime`, which must be assembled RFC822), `FlagsOnly` for
    /// `FlagsOnly`, and one blob handle per attachment descriptor.
    #[test]
    fn ews_get_item_projects_into_the_graph_hydrated_shape() {
        let folder = FolderId("AAMkPF=".to_string());
        let id = super::super::foreign::encode_public_item_id(&folder, "AAMkItem=");
        let item = ews_item();

        for projection in [Projection::Metadata, Projection::Headers, Projection::Full] {
            let hydrated = hydrated_from_ews_item(
                id.clone(),
                &item,
                &folder,
                "content@contoso.com",
                projection,
            );
            // The hydrated id is the folder-qualified id the caller asked
            // for, identical to the inventory entry's.
            assert_eq!(hydrated.id, id);
            match &hydrated.kind {
                HydratedObjectKind::Metadata(entry) => {
                    assert_eq!(entry.id, id);
                    assert!(
                        entry
                            .memberships
                            .contains(&MembershipScope::Folder(folder.clone()))
                    );
                    assert!(entry.memberships.contains(&MembershipScope::Mailbox(
                        bifrost_types::MailboxId("content@contoso.com".to_string())
                    )));
                }
                other => panic!("expected Metadata for {projection:?}, got {other:?}"),
            }
            // Attachment metadata rides out as blob handles; the bytes come
            // through `open_blob` (EWS GetAttachment).
            assert_eq!(hydrated.blobs.len(), 2);
            // EWS has no byte-range attachment form.
            assert!(
                hydrated
                    .blobs
                    .iter()
                    .all(|b| !b.capabilities.supports_range)
            );
            assert_eq!(hydrated.blobs[0].size, Some(2048));
            assert_eq!(
                hydrated.blobs[0].content_type.as_deref(),
                Some("application/pdf")
            );
        }

        let flags = hydrated_from_ews_item(
            id.clone(),
            &item,
            &folder,
            "content@contoso.com",
            Projection::FlagsOnly,
        );
        match flags.kind {
            HydratedObjectKind::FlagsOnly(flags) => {
                assert!(flags.contains("\\seen"));
                assert_eq!(flags.len(), 1);
            }
            other => panic!("expected FlagsOnly, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn public_folder_ids_partition_onto_the_ews_arm() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let folder = FolderId("AAMkPF=".to_string());
        account
            .seed_public_folder_for_tests(
                folder.clone(),
                crate::account::cursor::PublicFolderRouting {
                    anchor_mailbox: "content@contoso.com".to_string(),
                    public_folder_mailbox: Some("pf@contoso.com".to_string()),
                },
            )
            .await;

        let public = super::super::foreign::encode_public_item_id(&folder, "AAMkItem=");
        let primary = ObjectId("AAMkmsg".to_string());
        // A folder-qualified id whose folder was never discovered has no
        // routing, so it stays on the REST arm rather than being dropped.
        let unknown = super::super::foreign::encode_public_item_id(
            &FolderId("AAMkOther=".to_string()),
            "AAMkItem=",
        );

        let (ews, rest) = partition_ews_ids(
            &account,
            &[public.clone(), primary.clone(), unknown.clone()],
        )
        .await;
        assert_eq!(ews, vec![(public, folder)]);
        assert_eq!(rest, vec![primary, unknown]);
    }

    /// A3: Graph hydration body projections no longer mint a JSON
    /// `RawMime` (the prior contract violation). They degrade to
    /// `Metadata` (or `FlagsOnly` on the fallback path), never
    /// `RawMime`. Asserting the exact degraded variant records the
    /// stopgap as a deliberate contract, not an accident. The assembled
    /// bytes come exclusively through `open_raw_rfc822`.
    #[test]
    fn hydration_body_projection_degrades_to_metadata() {
        let value = json!({
            "id": "AAMkmessage",
            "parentFolderId": "inbox",
            "isRead": true,
            "changeKey": "CK1"
        });

        for projection in [Projection::Headers, Projection::Full] {
            let hydrated =
                hydrated_from_value(ObjectId("AAMkmessage".to_string()), &value, projection);
            match hydrated.kind {
                HydratedObjectKind::Metadata(_) => {}
                other => panic!("expected Metadata for {projection:?}, got {other:?}"),
            }
        }

        // Fallback path: a value with no `id` cannot build an inventory
        // entry, so it degrades to `FlagsOnly` - still never `RawMime`.
        let no_id = json!({ "parentFolderId": "inbox", "isRead": false });
        let hydrated =
            hydrated_from_value(ObjectId("missing".to_string()), &no_id, Projection::Headers);
        assert!(matches!(hydrated.kind, HydratedObjectKind::FlagsOnly(_)));
    }

    #[test]
    fn metadata_projection_preserves_the_shared_mailbox_owner() {
        let foreign_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let outer = super::super::foreign::encode_message_id(&foreign_scope, "AAMkmsg");
        let value = json!({
            "id": "AAMkmsg",
            "parentFolderId": "inbox",
            "changeKey": "CK1"
        });

        let hydrated = hydrated_from_value(outer.clone(), &value, Projection::Metadata);
        assert_eq!(hydrated.id, outer);
        match hydrated.kind {
            HydratedObjectKind::Metadata(entry) => {
                assert_eq!(entry.id, outer);
                assert_eq!(
                    entry.memberships,
                    vec![
                        MembershipScope::Folder(super::super::foreign::encode_foreign(
                            "shared@contoso.com",
                            "inbox"
                        )),
                        MembershipScope::Mailbox(bifrost_types::MailboxId(
                            "shared@contoso.com".to_string()
                        )),
                    ]
                );
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[test]
    fn flags_only_maps_graph_state_onto_the_canonical_vocabulary() {
        let value = json!({
            "isRead": true,
            "flag": { "flagStatus": "flagged" },
            "categories": ["Work", "Urgent"]
        });
        let flags = flags_from_value(&value);
        assert!(flags.contains("\\seen"));
        assert!(flags.contains("\\flagged"));
        assert!(flags.contains("category:Work"));
        assert!(flags.contains("category:Urgent"));
        assert_eq!(flags.len(), 4);
    }

    #[test]
    fn only_the_flagged_status_becomes_the_starred_flag() {
        // Graph's `flagStatus` is a three-value enum; `complete` means the
        // follow-up was finished, not that the message is starred.
        for status in ["notFlagged", "complete"] {
            let value = json!({ "flag": { "flagStatus": status } });
            assert!(
                !flags_from_value(&value).contains("\\flagged"),
                "flagStatus {status} must not map to \\flagged"
            );
        }
        // The read side is case-insensitive on the token.
        let value = json!({ "flag": { "flagStatus": "Flagged" } });
        assert!(flags_from_value(&value).contains("\\flagged"));
    }

    #[test]
    fn absent_fields_produce_no_flags() {
        assert!(flags_from_value(&json!({ "id": "m1" })).is_empty());
        assert!(flags_from_value(&json!({ "isRead": false })).is_empty());
    }

    #[test]
    fn every_projection_selects_the_parent_folder() {
        // `metadata_or_flags` rebuilds the membership scope from
        // `parentFolderId`; a projection that omitted it would file every
        // hydrated message under `FolderId("")`.
        for projection in [
            Projection::FlagsOnly,
            Projection::Metadata,
            Projection::Headers,
            Projection::Full,
            Projection::FullWithBlobs,
            Projection::TextOnly,
        ] {
            assert!(
                select_for_projection(projection).contains("parentFolderId"),
                "{projection:?} select lacks parentFolderId"
            );
        }
    }

    #[test]
    fn metadata_select_explicitly_requests_the_change_key() {
        // `changeKey` is the documented message concurrency token. Keep it
        // explicit rather than making the whole If-Match chain depend on an
        // OData annotation Graph happens to include.
        assert!(MESSAGE_SELECT.contains("changeKey"));
        assert!(select_for_projection(Projection::FlagsOnly).contains("changeKey"));
        assert!(select_for_projection(Projection::Full).contains("changeKey"));

        assert_eq!(
            graph_etag(&json!({ "@odata.etag": "W/\"CK1\"" })).as_deref(),
            Some("W/\"CK1\"")
        );
    }

    #[test]
    fn folder_destination_accepts_only_folder_memberships() {
        assert_eq!(
            folder_destination(MembershipScope::Folder(FolderId("inbox".to_string()))),
            Some(FolderId("inbox".to_string()))
        );
        assert_eq!(
            folder_destination(MembershipScope::Mailbox(bifrost_types::MailboxId(
                "shared@contoso.com".to_string()
            ))),
            None
        );
    }

    /// The hydration lane is mail-shaped for EVERY projection and every id
    /// shape, and that is the contract rather than an oversight: `get_stream`
    /// is the mail hydration lane (caldav/carddav answer it
    /// `unsupported`, the engine drives it only from mutation read-back on
    /// mail ids), and calendar/contact objects are read through their own
    /// typed surfaces. An `ObjectId` carries no type marker, so nothing here
    /// could route by scope kind; this pins the answer so a future reader
    /// does not have to re-derive it, and fails loudly if someone starts
    /// mixing event or contact fields into a lane that cannot address them.
    #[test]
    fn every_hydration_projection_selects_message_fields_on_a_messages_url() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        for projection in [
            Projection::FlagsOnly,
            Projection::Metadata,
            Projection::Headers,
            Projection::Preview(1024),
            Projection::TextOnly,
            Projection::Full,
            Projection::FullWithBlobs,
        ] {
            let select = select_for_projection(projection);
            assert!(
                select.starts_with("id,") || select == "id",
                "{projection:?} selects {select}"
            );
            for event_or_contact_only in
                ["start", "end", "attendees", "givenName", "emailAddresses"]
            {
                assert!(
                    !select.contains(event_or_contact_only),
                    "{projection:?} selects the non-mail field {event_or_contact_only}: {select}"
                );
            }
            let url = hydrate_url_for_id(&account, &ObjectId("AAMk".to_string()), select)
                .expect("primary id routes");
            assert!(
                url.starts_with("/me/messages/"),
                "{projection:?} hydrates from {url}"
            );
        }
    }

    #[test]
    fn hydration_url_percent_encodes_the_native_id_and_keeps_the_select() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let url = hydrate_url_for_id(&account, &ObjectId("AAMk/GI2=".to_string()), "id,changeKey")
            .expect("primary");
        assert!(!url.contains("AAMk/GI2="), "unencoded id in {url}");
        assert!(url.ends_with("?$select=id,changeKey"), "{url}");
    }

    /// A public-folder id whose folder is NOT in the routing map falls back
    /// to the REST arm (pinned by `public_folder_ids_partition_onto_the_ews_arm`).
    /// This pins what that fallback then asks for: `/me/messages/{itemId}`
    /// with the RS separator percent-encoded, which Graph answers with
    /// `ErrorItemNotFound`. That is the deliberate "report the real miss"
    /// behavior, not a silent drop.
    #[test]
    fn an_unrouted_public_folder_id_hydrates_through_the_rest_arm_url() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let id = super::super::foreign::encode_public_item_id(
            &FolderId("AAMkPF=".to_string()),
            "AAMkItem=",
        );
        let url = hydrate_url_for_id(&account, &id, "id").expect("primary");
        assert!(url.starts_with("/me/messages/"), "{url}");
        assert!(!url.contains('\u{1e}'), "{url}");
    }

    // ── Class-conditional EWS hydration ─────────────────────

    /// A `GetItem` SOAP answer for `AAMkItem=` of the given class. The
    /// element name matters: the parser switches item-collection state on
    /// it.
    fn get_item_soap(element: &str, item_class: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:{element}>
              <t:ItemId Id="AAMkItem=" ChangeKey="CK1"/>
              <t:ItemClass>{item_class}</t:ItemClass>
            </t:{element}>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#
        )
    }

    fn ews_item_of_class(item_id: &str, item_class: &str) -> crate::ews::EwsItem {
        let mut item = ews_item();
        item.item_id = item_id.to_string();
        item.item_class = item_class.to_string();
        item
    }

    /// Drive the real batch hydration door against a scripted EWS transport
    /// and return the SOAP body that reached the wire, plus whether the
    /// outcome succeeded.
    async fn ews_hydrate_request_body(
        folder_class: Option<&str>,
        known_item: Option<crate::ews::EwsItem>,
        response: &str,
    ) -> (String, bool) {
        use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_account};
        use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource, TokenSource};
        use std::sync::Arc;

        let script = ScriptedDispatch::new((0..2).map(|_| Canned::Response {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::from(response.to_string()),
        }));
        let token_source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
        let net = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::clone(&token_source),
            RetryPolicy::disabled(),
        );
        let client =
            GraphClient::with_account_net(net, "https://graph.contoso.test/v1.0", token_source);
        let account = GraphAccount::new_for_tests(client, PushMode::EwsStreaming);

        let folder = FolderId("AAMkPF=".to_string());
        account
            .seed_public_folder_meta_for_tests(
                folder.clone(),
                crate::account::cursor::PublicFolderRouting {
                    anchor_mailbox: "content@contoso.com".to_string(),
                    public_folder_mailbox: Some("pf@contoso.com".to_string()),
                },
                crate::account::public_folder::PublicFolderMeta {
                    display_name: "Notices".to_string(),
                    folder_class: folder_class.map(str::to_string),
                    parent: None,
                    effective_rights: crate::ews::EwsEffectiveRights::default(),
                },
            )
            .await;
        if let Some(item) = known_item {
            // Exactly what the inventory / poll walk records.
            account
                .record_public_item_classes(&folder, std::slice::from_ref(&item))
                .await;
        }

        let id = super::super::foreign::encode_public_item_id(&folder, "AAMkItem=");
        let mut outcomes = Vec::new();
        for projection in [Projection::Metadata, Projection::FlagsOnly] {
            outcomes.extend(
                fetch_ews_outcomes(&account, &[(id.clone(), folder.clone())], projection).await,
            );
        }
        for outcome in &outcomes {
            if let ItemOutcome::Failed(failure) = outcome {
                assert!(matches!(
                    failure.error.kind(),
                    bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Hydrate)
                ));
            }
        }
        assert_eq!(
            outcomes.len(),
            2,
            "one outcome per pulled id and projection"
        );
        let succeeded = outcomes
            .iter()
            .all(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)));

        let requests = script.requests();
        assert_eq!(requests.len(), 2, "one GetItem per item and projection");
        let body = requests[0].body.clone().expect("GetItem carries a body");
        (
            String::from_utf8(body.to_vec()).expect("utf8 SOAP"),
            succeeded,
        )
    }

    #[tokio::test]
    async fn nc2a_flags_only_refuses_returned_contact_after_mail_cache_hit() {
        let (_, succeeded) = ews_hydrate_request_body(
            Some("IPF.Note"),
            Some(ews_item_of_class("AAMkItem=", "IPM.Note")),
            &get_item_soap("Contact", "IPM.Contact"),
        )
        .await;
        assert!(!succeeded);
    }

    /// The defect this closes: a `Contact` in a MIXED public folder (the
    /// folder itself is `IPF.Note`, so the container tier says "message")
    /// was asked for `message:ToRecipients` and answered
    /// `ErrorInvalidPropertyRequest`. The per-item class the inventory pass
    /// already parsed is what makes the request class-safe.
    #[tokio::test]
    async fn a_known_contact_in_a_mail_public_folder_is_asked_for_no_message_properties() {
        let (body, succeeded) = ews_hydrate_request_body(
            Some("IPF.Note"),
            Some(ews_item_of_class("AAMkItem=", "IPM.Contact")),
            &get_item_soap("Contact", "IPM.Contact"),
        )
        .await;
        assert!(
            !body.contains("message:"),
            "a contact was asked for message properties: {body}"
        );
        assert!(!succeeded, "non-mail must be a per-id refusal");
    }

    /// The mail path is untouched: same folder, same door, the historical
    /// message-shaped body.
    #[tokio::test]
    async fn a_mail_item_still_gets_the_message_shaped_body() {
        let (body, succeeded) = ews_hydrate_request_body(
            Some("IPF.Note"),
            Some(ews_item_of_class("AAMkItem=", "IPM.Note")),
            &get_item_soap("Message", "IPM.Note"),
        )
        .await;
        assert!(
            body.contains(r#"FieldURI="message:ToRecipients""#),
            "{body}"
        );
        assert!(
            body.contains(r#"FieldURI="message:CcRecipients""#),
            "{body}"
        );
        assert!(succeeded);
    }

    /// An unknown item uses common properties even in a mail folder.
    #[tokio::test]
    async fn nc2a_unknown_item_uses_neutral_shape_in_mail_folder() {
        let (body, succeeded) = ews_hydrate_request_body(
            Some("IPF.Note"),
            None,
            &get_item_soap("Contact", "IPM.Contact"),
        )
        .await;
        assert!(!body.contains("message:"), "{body}");
        assert!(!succeeded);
    }

    /// Without any class hint the request remains class-neutral.
    #[tokio::test]
    async fn an_entirely_unknown_class_uses_a_neutral_request() {
        let (body, _) =
            ews_hydrate_request_body(None, None, &get_item_soap("Message", "IPM.Note")).await;
        assert!(!body.contains("message:"), "{body}");
    }
}
