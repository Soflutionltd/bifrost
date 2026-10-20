use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchUncertain, Checkpoint,
    ErrorScope, FlagOp, IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, ObjectId,
    PageBoundary, ProtocolErrorKind, SyncEvent,
};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::client::Resource;
use crate::types::{BatchRequest, BatchRequestItem};

use super::GraphAccount;
use super::get::folder_destination;
use super::graph_error::{
    GraphErrorContext, into_account_error, mutation_item_outcome, protocol_violation,
    unsupported_account_error,
};
use super::inventory::graph_etag;

enum MutationKind {
    SetFlags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

pub(crate) fn bulk_set_flags_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    if let Err(error) = op.validate_for_account(bifrost_types::Protocol::Graph) {
        return Box::pin(futures::stream::once(async move {
            SyncEvent::Terminated(error)
        }));
    }
    bulk_mutation_stream(account, targets, MutationKind::SetFlags(op))
}

pub(crate) fn bulk_move_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_mutation_stream(account, targets, MutationKind::Move(destination))
}

pub(crate) fn bulk_destroy_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_mutation_stream(account, targets, MutationKind::Destroy)
}

/// Chunk the target stream and submit one `$batch` per chunk.
///
/// Every batch is tagged `PageBoundary::Page`, including the last one, and
/// the stream ends with `Done(None)`. That asymmetry with the hydration and
/// inventory lanes - which had to be reworked to tag their terminal chunk
/// `Final` - is deliberate, and it is a fact about the engine rather than a
/// preference: the mutation consumer in `bifrost-sync` loops on
/// `stream.next()` and matches only `Batch` and `Terminated`, treating the
/// END OF THE STREAM as the terminus. It never reads `batch.page_boundary`
/// on this lane, so there is no boundary to hold a final chunk back for.
///
/// The hydration lane is different because a cursor-advancing / checkpointed
/// reader needs to know which chunk closes the stream before the stream
/// itself ends. A mutation batch carries `checkpoint: None` by construction
/// and advances nothing. Tagging the last chunk `Final` here would mean
/// buffering a chunk to discover it is last, which buys the consumer
/// nothing.
fn bulk_mutation_stream(
    account: GraphAccount,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    Box::pin(async_stream::stream! {
        let max_items = account.capabilities.batching_policy.max_items.max(1);
        let mut chunk = Vec::with_capacity(max_items);
        while let Some(id) = targets.next().await {
            chunk.push(id);
            if chunk.len() >= max_items {
                match submit_batch(&account, &chunk, &kind).await {
                    Ok(batch_events) => {
                        for event in batch_events {
                            yield event;
                        }
                    }
                    Err(error) => {
                        let ctx = GraphErrorContext::graph(operation_for_kind(&kind));
                        yield SyncEvent::Terminated(into_account_error(error, ctx));
                        yield SyncEvent::Done(None);
                        return;
                    }
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            match submit_batch(&account, &chunk, &kind).await {
                Ok(batch_events) => {
                    for event in batch_events {
                        yield event;
                    }
                }
                Err(error) => {
                    let ctx = GraphErrorContext::graph(operation_for_kind(&kind));
                    yield SyncEvent::Terminated(into_account_error(error, ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

fn operation_for_kind(kind: &MutationKind) -> AccountOperation {
    match kind {
        MutationKind::SetFlags(_) => AccountOperation::UpdateFlags,
        MutationKind::Move(_) => AccountOperation::BulkMove,
        MutationKind::Destroy => AccountOperation::BulkDestroy,
    }
}

async fn submit_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    kind: &MutationKind,
) -> Result<Vec<SyncEvent<ItemOutcome<MutationSuccess>>>, crate::error::GraphError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    // Graph only accepts a full replacement for `categories`; its PATCH
    // surface has no add/remove member operation. Sending the partial flag
    // patch below would omit category tokens entirely, and a category-only
    // request would become `{}` and falsely report Applied on a 2xx. A
    // read-modify-write implementation can replace this rejection later,
    // but it must never silently claim the incremental category change.
    //
    // The same reasoning covers every flag Graph does not model at all
    // (`\answered`, `\draft`, arbitrary keywords): the built body is empty,
    // Graph answers 200 to it, and the caller would be told a flag change
    // landed that was never expressed on the wire. So the test is on the
    // body, not on the token namespace.
    if let MutationKind::SetFlags(op) = kind
        && let Some(error) = invalid_flag_op_error(op)
    {
        return Ok(vec![SyncEvent::Batch(Batch {
            items: ids
                .iter()
                .map(|id| {
                    ItemOutcome::Failed(BatchFailure::new(BatchItemId(id.0.clone()), error.clone()))
                })
                .collect(),
            page_boundary: PageBoundary::Page,
            server_latency: Duration::default(),
            // Rejected locally, before the chunk reached the wire: the
            // flag op is not expressible against Graph's PATCH surface,
            // so no request was made and nothing was read.
            bytes_in: 0,
            checkpoint: None::<Checkpoint>,
        })]);
    }

    // One mutation chunk is the `$batch` submission plus whatever the
    // etag preflight had to re-read. `refresh_missing_etags` issues one
    // GET per id whose etag was not cached, so leaving it out would
    // under-report a cold chunk by most of its traffic.
    let (metered, tally) = account.metered();
    let account = &metered;
    let mut etags = HashMap::new();
    {
        let mut cache = account.etag_index.write().await;
        for id in ids {
            if let Some(etag) = cache.get(&id.0) {
                etags.insert(id.0.clone(), etag);
            }
        }
    }
    let mut preflight_outcomes = refresh_missing_etags(account, ids, kind, &mut etags).await;
    let mut requests = Vec::new();
    let mut request_ids = Vec::new();
    for id in ids {
        if requires_etag(kind) && !etags.contains_key(&id.0) {
            // Etag fetch failed; the failed outcome was recorded in
            // `preflight_outcomes` by `refresh_missing_etags`.
            continue;
        }
        let built = match request_for_mutation(account, id, kind, &etags) {
            Ok(built) => built,
            Err(error) => {
                // The id has no endpoint: its shared mailbox is gone from
                // configuration. `SetFlags` / `Move` never reach here (the
                // etag preflight above already filed them), but `Destroy`
                // needs no etag, so this is the only place its stale ids
                // are caught - and propagating would leave every valid
                // sibling of the chunk with no outcome at all, which the
                // engine's mutation pipeline cannot sweep for read-back
                // either, since the terminal `Request(Malformed)` it would
                // raise names the whole request rather than the one id.
                preflight_outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                    BatchItemId(id.0.clone()),
                    into_account_error(
                        error,
                        GraphErrorContext::graph(operation_for_kind(kind)).with_scope(
                            ErrorScope::Message {
                                id: (id.0.clone()).into(),
                            },
                        ),
                    ),
                )));
                continue;
            }
        };
        let Some(request) = built else {
            // `request_for_mutation` returns `Ok(None)` when the
            // mutation cannot be built. For `Move` kinds the etag is
            // preflight-checked above, so this fires either because the
            // destination isn't a folder, or because the destination
            // folder belongs to a different mailbox than the source (a
            // cross-mailbox move is not expressible against one
            // `/users/{owner}` endpoint). Both are caller-side malformed
            // requests. Classify as `Request(Malformed)` so recovery
            // routes to `ClientBug` rather than the misleading
            // `Unsupported(BulkMove)` shape that suggests the protocol
            // doesn't support moves at all. (graph-F4 / F1b)
            preflight_outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                BatchItemId(id.0.clone()),
                super::graph_error::invalid_item_error(
                    operation_for_kind(kind),
                    bifrost_types::ErrorScope::Message {
                        id: (id.0.clone()).into(),
                    },
                    format!(
                        "Graph Move request for {} did not resolve to a same-mailbox folder destination",
                        id.0
                    ),
                ),
            )));
            continue;
        };
        request_ids.push(id.clone());
        requests.push(request);
    }

    let mut item_outcomes: Vec<ItemOutcome<MutationSuccess>> = preflight_outcomes;

    if !requests.is_empty() {
        assign_batch_ids(&mut requests);
        let response = match account.batch_resource(&request_ids) {
            Ok(resource) => {
                account
                    .client
                    .post_batch(resource, &BatchRequest { requests })
                    .await
            }
            Err(error) => Err(error),
        };
        match response {
            Ok(response) => {
                // Cache and outcome lanes must consume the same validated responses.
                let resolved = resolve_batch_responses(&request_ids, response.responses);
                update_etag_cache_from_responses(account, &request_ids, &resolved, kind).await;
                reconcile_mutation_responses(
                    &request_ids,
                    resolved,
                    kind,
                    &mut item_outcomes,
                    &|id| account.budget_for_object(id),
                );
            }
            Err(error) => append_batch_failure(&request_ids, kind, error, &mut item_outcomes),
        }
    }

    let events = vec![SyncEvent::Batch(Batch {
        items: item_outcomes,
        page_boundary: PageBoundary::Page,
        server_latency: Duration::default(),
        bytes_in: tally.take(),
        checkpoint: None::<Checkpoint>,
    })];
    Ok(events)
}

/// A failed envelope must not erase local decisions or abandon later chunks.
/// In-flight failures leave every submitted write unconfirmed. A successful
/// envelope that cannot be decoded proves no individual write's outcome either.
/// A rejected envelope or an unsent request instead fails each submitted id.
/// Account-wide errors retain their classification on every item; consuming the
/// remaining targets ensures even those ids receive an explicit outcome.
fn append_batch_failure(
    ids: &[ObjectId],
    kind: &MutationKind,
    error: crate::error::GraphError,
    outcomes: &mut Vec<ItemOutcome<MutationSuccess>>,
) {
    let unreadable_success = matches!(&error, crate::error::GraphError::Json { .. });
    let error = into_account_error(error, GraphErrorContext::graph(operation_for_kind(kind)));
    let uncertain = unreadable_success
        || error.chain().iter().any(|cause| {
            matches!(cause, bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::InFlight)
        });
    for id in ids {
        let scoped = error
            .clone()
            .into_builder()
            .scope(ErrorScope::Message {
                id: id.0.clone().into(),
            })
            .try_build()
            .expect("valid account error classification");
        let item = BatchItemId(id.0.clone());
        outcomes.push(if uncertain {
            ItemOutcome::Uncertain(BatchUncertain::new(item, scoped))
        } else {
            ItemOutcome::Failed(BatchFailure::new(item, scoped))
        });
    }
}

/// Validate `$batch` subresponses against the submitted request ids.
///
/// A response whose id does not parse as a submitted index, parses out of
/// range, or repeats an index already answered is DISCARDED; the first
/// response for an index wins. Both consumers of the wire data - the etag
/// cache and the outcome projection - read this one resolved list, because
/// they used to walk the raw subresponses independently and disagreed on
/// malformed input: a `412` followed by a duplicate `200` for the same
/// request left an etag installed while the accepted outcome was a
/// concurrency conflict, so the retry re-sent against a version the caller
/// had already been told was stale.
fn resolve_batch_responses(
    request_ids: &[ObjectId],
    responses: Vec<crate::types::BatchResponseItem>,
) -> Vec<(usize, crate::types::BatchResponseItem)> {
    let mut seen_indices: HashSet<usize> = HashSet::new();
    let mut resolved = Vec::with_capacity(responses.len());
    for item in responses {
        let Some(index) = item
            .id
            .parse::<usize>()
            .ok()
            .filter(|idx| *idx < request_ids.len())
        else {
            tracing::warn!(
                target: "bifrost_graph::batch",
                response_id = %item.id,
                "ignoring Graph $batch mutation response with an invalid request id"
            );
            continue;
        };
        if !seen_indices.insert(index) {
            tracing::warn!(
                target: "bifrost_graph::batch",
                response_id = %item.id,
                "ignoring duplicate Graph $batch mutation response"
            );
            continue;
        }
        resolved.push((index, item));
    }
    resolved
}

async fn update_etag_cache_from_responses(
    account: &GraphAccount,
    request_ids: &[ObjectId],
    responses: &[(usize, crate::types::BatchResponseItem)],
    kind: &MutationKind,
) {
    let mut cache = account.etag_index.write().await;
    for (index, response) in responses {
        let Some(id) = request_ids.get(*index) else {
            continue;
        };
        // A 404 means there is no object version left to condition a future
        // write on. Destroy maps that response to Skipped, but it still has
        // to evict the cached etag for the same id.
        if response.status == 412
            || response.status == 404
            || ((200..300).contains(&response.status)
                && matches!(kind, MutationKind::Move(_) | MutationKind::Destroy))
        {
            cache.remove(&id.0);
            continue;
        }
        if (200..300).contains(&response.status) {
            let etag = response
                .headers
                .as_ref()
                .and_then(|headers| {
                    headers
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
                        .map(|(_, value)| value.clone())
                })
                .or_else(|| response.body.as_ref().and_then(graph_etag));
            match etag {
                Some(etag) => cache.insert(id.0.clone(), etag),
                None => cache.remove(&id.0),
            }
        }
    }
}

/// Project the `$batch` responses onto the submitted request ids,
/// appending exactly one `ItemOutcome` per id to `item_outcomes`.
///
/// Pure over the RESOLVED response list produced by
/// `resolve_batch_responses`, which is where invalid and duplicate response
/// ids are discarded - fabricating an id the caller never submitted would
/// put a stranger on the lane while the real id stays unanswered. Taking
/// the resolved list rather than the raw one is what keeps the etag cache
/// and the outcome lanes reading the same subresponse for an id.
///
/// An id Graph never answered lands on the UNCERTAIN lane. The `$batch`
/// envelope decoded, which is evidence about the envelope and nothing
/// else: a `move` or `permanentDelete` that committed and then lost its
/// subresponse is byte-identical to one that never ran. The uncertain lane is exactly
/// the lane for "this write may have landed, read it back rather than
/// replay it". Reporting `Failed(ContractViolation)` instead classified
/// terminal, so the engine recorded `failed_terminal` with no read-back
/// and permanently mis-stated an applied mutation as a lost one.
fn reconcile_mutation_responses(
    request_ids: &[ObjectId],
    responses: Vec<(usize, crate::types::BatchResponseItem)>,
    kind: &MutationKind,
    item_outcomes: &mut Vec<ItemOutcome<MutationSuccess>>,
    // The mailbox budget each id's sub-request was charged to
    // (`GraphAccount::budget_for_object`), stamped on a throttled item.
    budget_for: &dyn Fn(&ObjectId) -> Option<bifrost_types::ThrottleBudget>,
) {
    let mut seen_indices: HashSet<usize> = HashSet::new();
    for (index, item) in responses {
        seen_indices.insert(index);
        let id = request_ids[index].clone();
        let scope = ErrorScope::Message {
            id: (id.0.clone()).into(),
        };
        let headers = item
            .headers
            .map(|h| {
                let mut hm = reqwest::header::HeaderMap::new();
                for (k, v) in h {
                    if let (Ok(name), Ok(val)) = (
                        reqwest::header::HeaderName::from_bytes(k.as_bytes()),
                        reqwest::header::HeaderValue::from_str(&v),
                    ) {
                        hm.insert(name, val);
                    }
                }
                hm
            })
            .unwrap_or_default();
        let body = item
            .body
            .as_ref()
            .map(|v| bytes::Bytes::from(serde_json::to_vec(v).unwrap_or_default()))
            .unwrap_or_default();
        item_outcomes.push(mutation_item_outcome(
            item.status,
            headers,
            body,
            matches!(kind, MutationKind::Destroy),
            BatchItemId(id.0.clone()),
            operation_for_kind(kind),
            scope,
            budget_for(&id),
        ));
    }

    for (index, id) in request_ids.iter().enumerate() {
        if seen_indices.contains(&index) {
            continue;
        }
        item_outcomes.push(ItemOutcome::Uncertain(BatchUncertain::new(
            BatchItemId(id.0.clone()),
            super::graph_error::batch_response_missing(
                operation_for_kind(kind),
                Some(ErrorScope::Message {
                    id: (id.0.clone()).into(),
                }),
                format!(
                    "Graph $batch returned no response for {}; the item's fate is unknown",
                    id.0
                ),
            ),
        )));
    }
}

/// Fetch etags for ids that require one but don't have a cached value.
/// Returns per-item `ItemOutcome::Failed` for ids where the fetch fails.
async fn refresh_missing_etags(
    account: &GraphAccount,
    ids: &[ObjectId],
    kind: &MutationKind,
    etags: &mut HashMap<String, String>,
) -> Vec<ItemOutcome<MutationSuccess>> {
    if !matches!(kind, MutationKind::SetFlags(_) | MutationKind::Move(_)) {
        return Vec::new();
    }

    let missing = ids
        .iter()
        .filter(|id| !etags.contains_key(&id.0))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Vec::new();
    }

    let mut refreshed = Vec::new();
    let mut failed = Vec::new();
    for id in missing {
        // Decode the (possibly foreign-encoded) id so the etag refresh
        // hits the owning mailbox: a shared-mailbox message lives under
        // `/users/{owner}/messages/{native}`, not `/me`. The cache key
        // stays the encoded `id.0` (F1's etag_index keying), only the
        // request URL uses the native id + owner prefix.
        let parsed = super::foreign::parse_message_id(&id);
        let client = match account.client_for_owner(parsed.owner()) {
            Ok(client) => client,
            Err(error) => {
                failed.push(ItemOutcome::Failed(BatchFailure::new(
                    BatchItemId(id.0.clone()),
                    into_account_error(
                        error,
                        GraphErrorContext::graph(operation_for_kind(kind)).with_scope(
                            ErrorScope::Message {
                                id: (id.0.clone()).into(),
                            },
                        ),
                    ),
                )));
                continue;
            }
        };
        let prefix = client.api_path_prefix();
        let enc_id = bifrost_net::url::encode_path_component(parsed.native_id());
        let path = format!("{prefix}/messages/{enc_id}?$select=id");
        match client.get_json::<Value>(Resource::Outlook, &path).await {
            Ok(value) => {
                if let Some(etag) = graph_etag(&value) {
                    etags.insert(id.0.clone(), etag.clone());
                    refreshed.push((id.0.clone(), etag));
                } else {
                    // Missing etag after a refresh is a Graph
                    // contract violation (`Protocol(MissingField)`),
                    // not an `Unsupported` op. Surface it on the
                    // failed lane so the consumer sees structured
                    // evidence rather than a misclassified terminal.
                    failed.push(ItemOutcome::Failed(BatchFailure::new(
                        BatchItemId(id.0.clone()),
                        protocol_violation(
                            ProtocolErrorKind::MissingField,
                            operation_for_kind(kind),
                            Some(ErrorScope::Message {
                                id: (id.0.clone()).into(),
                            }),
                            format!(
                                "Graph message {} did not expose an etag after refresh",
                                id.0
                            ),
                        ),
                    )));
                }
            }
            Err(error) => {
                let ctx = GraphErrorContext::graph(operation_for_kind(kind)).with_scope(
                    ErrorScope::Message {
                        id: (id.0.clone()).into(),
                    },
                );
                failed.push(ItemOutcome::Failed(BatchFailure::new(
                    BatchItemId(id.0.clone()),
                    into_account_error(error, ctx),
                )));
            }
        }
    }

    if !refreshed.is_empty() {
        let mut cache = account.etag_index.write().await;
        for (id, etag) in refreshed {
            cache.insert(id, etag);
        }
    }
    failed
}

fn requires_etag(kind: &MutationKind) -> bool {
    matches!(kind, MutationKind::SetFlags(_) | MutationKind::Move(_))
}

fn request_for_mutation(
    account: &GraphAccount,
    id: &ObjectId,
    kind: &MutationKind,
    etags: &HashMap<String, String>,
) -> Result<Option<BatchRequestItem>, crate::error::GraphError> {
    // Decode the (possibly foreign-encoded) id and route to the owning
    // mailbox: a shared-mailbox item builds `/users/{owner}/messages/{native}`,
    // a primary item `/me/messages/{id}`. The etag cache is keyed by the
    // encoded `id.0` (F1's etag_index), so the lookups below use `id.0`;
    // only the URL is built from the decoded native id + owner prefix.
    let parsed = super::foreign::parse_message_id(id);
    let prefix = account.client_for_owner(parsed.owner())?.api_path_prefix();
    let enc_id = bifrost_net::url::encode_path_component(parsed.native_id());
    let mut headers = HashMap::new();
    match kind {
        MutationKind::SetFlags(op) => {
            let Some(etag) = etags.get(&id.0) else {
                return Ok(None);
            };
            headers.insert("If-Match".to_string(), etag.clone());
            Ok(Some(BatchRequestItem {
                id: "0".to_string(),
                method: "PATCH".to_string(),
                url: format!("{prefix}/messages/{enc_id}"),
                body: Some(patch_for_flags(op)),
                headers: Some(headers),
            }))
        }
        MutationKind::Move(destination) => {
            let Some(etag) = etags.get(&id.0) else {
                return Ok(None);
            };
            let Some(folder) = folder_destination(destination.clone()) else {
                return Ok(None);
            };
            // The destination folder id is itself a (possibly
            // foreign-encoded) FolderId. Decode it: `destinationId` must
            // carry the native folder id (a `\u{1f}`-bearing id is not a
            // valid Graph folder id), and the destination must belong to
            // the same mailbox as the source. A cross-mailbox move is not
            // expressible against one `/users/{owner}` (or `/me`) endpoint,
            // so reject it as a malformed request rather than emit a wrong
            // one - returning `None` routes through the caller's
            // `Request(Malformed)` lane.
            let dest = super::foreign::parse_folder(&folder);
            if dest.foreign().map(|f| f.mailbox.as_str()) != parsed.owner() {
                return Ok(None);
            }
            headers.insert("If-Match".to_string(), etag.clone());
            Ok(Some(BatchRequestItem {
                id: "0".to_string(),
                method: "POST".to_string(),
                url: format!("{prefix}/messages/{enc_id}/move"),
                body: Some(json!({ "destinationId": dest.native_id() })),
                headers: Some(headers),
            }))
        }
        MutationKind::Destroy => {
            if let Some(etag) = etags.get(&id.0) {
                headers.insert("If-Match".to_string(), etag.clone());
            }
            Ok(Some(BatchRequestItem {
                id: "0".to_string(),
                // DELETE moves mail to Deleted Items (or recoverable storage).
                // permanentDelete removes it from the normal mailbox inventory;
                // retention can still keep it in Purges on the server.
                method: "POST".to_string(),
                url: format!("{prefix}/messages/{enc_id}/permanentDelete"),
                body: None,
                headers: (!headers.is_empty()).then_some(headers),
            }))
        }
    }
}

pub(crate) fn assign_batch_ids(requests: &mut [BatchRequestItem]) {
    for (index, request) in requests.iter_mut().enumerate() {
        request.id = index.to_string();
    }
}

fn patch_for_flags(op: &FlagOp) -> Value {
    let mut body = serde_json::Map::new();
    match op {
        FlagOp::Add(flags) => apply_flag_adds(&mut body, flags),
        FlagOp::Remove(flags) => apply_flag_removes(&mut body, flags),
        FlagOp::Set(flags) => {
            // `Set` is full-replace across the flags Graph owns: any flag
            // absent from the set must be cleared, not left untouched.
            // `apply_flag_adds` alone only ever writes `isRead:true` /
            // `flag:flagged` for present flags, so `Set({})` or
            // `Set({category:x})` would leave a previously-read/flagged
            // message read/flagged. Emit the authoritative value for each
            // owned field.
            body.insert(
                "isRead".to_string(),
                json!(
                    flags
                        .iter()
                        .any(|flag| graph_flag_field(flag) == Some(GraphFlagField::Read))
                ),
            );
            let flagged = flags
                .iter()
                .any(|flag| graph_flag_field(flag) == Some(GraphFlagField::Flagged));
            body.insert(
                "flag".to_string(),
                json!({ "flagStatus": if flagged { "flagged" } else { "notFlagged" } }),
            );
            body.insert(
                "categories".to_string(),
                json!(categories_from_flags(flags)),
            );
        }
        FlagOp::Patch { add, remove } => {
            apply_flag_adds(&mut body, add);
            apply_flag_removes(&mut body, remove);
        }
        _ => {}
    }
    Value::Object(body)
}

fn apply_flag_adds(body: &mut serde_json::Map<String, Value>, flags: &HashSet<String>) {
    if flags
        .iter()
        .any(|flag| graph_flag_field(flag) == Some(GraphFlagField::Read))
    {
        body.insert("isRead".to_string(), json!(true));
    }
    if flags
        .iter()
        .any(|flag| graph_flag_field(flag) == Some(GraphFlagField::Flagged))
    {
        body.insert("flag".to_string(), json!({ "flagStatus": "flagged" }));
    }
}

fn apply_flag_removes(body: &mut serde_json::Map<String, Value>, flags: &HashSet<String>) {
    if flags
        .iter()
        .any(|flag| graph_flag_field(flag) == Some(GraphFlagField::Read))
    {
        body.insert("isRead".to_string(), json!(false));
    }
    if flags
        .iter()
        .any(|flag| graph_flag_field(flag) == Some(GraphFlagField::Flagged))
    {
        body.insert("flag".to_string(), json!({ "flagStatus": "notFlagged" }));
    }
}

fn categories_from_flags(flags: &HashSet<String>) -> Vec<String> {
    let mut categories: Vec<String> = flags
        .iter()
        .filter_map(|flag| flag.strip_prefix("category:").map(str::to_string))
        .collect();
    categories.sort();
    categories
}

/// A flag op that Graph cannot express as a PATCH: either it needs the
/// category read-modify-write Graph has no member operation for, or it
/// names only flags Graph does not model, leaving an empty body that
/// would 200 and be filed `Applied`.
fn flag_op_is_unexpressible(op: &FlagOp) -> bool {
    if flag_op_requires_category_rmw(op) {
        return true;
    }
    patch_for_flags(op)
        .as_object()
        .is_none_or(serde_json::Map::is_empty)
}

/// Reject a Patch that asks for contradictory state before it reaches the
/// provider. The same token is case-insensitive in every flag operation, and
/// Graph's two owned aliases (`\\Seen`/`read`, `\\Flagged`/`starred`) name
/// one field too, so those aliases are contradictions as well.
fn invalid_flag_op_error(op: &FlagOp) -> Option<bifrost_types::AccountError> {
    let FlagOp::Patch { add, remove } = op else {
        return flag_op_is_unexpressible(op)
            .then(|| unsupported_account_error(AccountOperation::UpdateFlags));
    };
    let overlaps = add.iter().any(|added| {
        remove
            .iter()
            .any(|removed| canonical_flag_name(added) == canonical_flag_name(removed))
    });
    if overlaps {
        Some(super::graph_error::invalid_account_error(
            AccountOperation::UpdateFlags,
            "FlagOp::Patch cannot add and remove the same flag",
        ))
    } else {
        flag_op_is_unexpressible(op)
            .then(|| unsupported_account_error(AccountOperation::UpdateFlags))
    }
}

fn canonical_flag_name(flag: &str) -> String {
    let normalized = flag.to_ascii_lowercase();
    match graph_flag_field(&normalized) {
        Some(GraphFlagField::Read) => "\\seen".to_string(),
        Some(GraphFlagField::Flagged) => "\\flagged".to_string(),
        None => normalized,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphFlagField {
    Read,
    Flagged,
}

/// The single alias table for both PATCH projection and contradiction checks.
fn graph_flag_field(flag: &str) -> Option<GraphFlagField> {
    if flag.eq_ignore_ascii_case("\\seen") || flag.eq_ignore_ascii_case("read") {
        Some(GraphFlagField::Read)
    } else if flag.eq_ignore_ascii_case("\\flagged")
        || flag.eq_ignore_ascii_case("flagged")
        || flag.eq_ignore_ascii_case("starred")
    {
        Some(GraphFlagField::Flagged)
    } else {
        None
    }
}

fn flag_op_requires_category_rmw(op: &FlagOp) -> bool {
    let has_categories =
        |flags: &HashSet<String>| flags.iter().any(|flag| flag.starts_with("category:"));
    match op {
        FlagOp::Add(flags) | FlagOp::Remove(flags) => has_categories(flags),
        FlagOp::Patch { add, remove } => has_categories(add) || has_categories(remove),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::account::foreign::{encode_foreign, encode_message_id};
    use crate::client::GraphClient;
    use bifrost_types::{CursorScope, FolderId, ObjectType};

    #[tokio::test]
    async fn delta_and_refresh_produce_identical_if_match_bytes() {
        use crate::client::ScriptedRestResponse;
        use futures::StreamExt;
        use serde_json::json;
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{"id": "m1", "changeKey": "CK1"}],
                    "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({"id": "m1", "@odata.etag": "W/\"CK1\""}),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".into()),
            ty: ObjectType::Email,
        };
        let payload = super::super::cursor::GraphCursorPayload::new(
            super::super::cursor::kind_for_scope(&scope).expect("kind"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = super::super::cursor::encode_cursor(scope, payload).expect("cursor");
        let events: Vec<_> = super::super::changes::changes_stream(account.clone(), cursor)
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, bifrost_types::SyncEvent::Batch(_)))
        );
        let id = ObjectId("m1".into());
        let cached = account
            .etag_index
            .write()
            .await
            .get(&id.0)
            .expect("delta etag");
        let kind = MutationKind::SetFlags(FlagOp::Set(HashSet::new()));
        let before = request_for_mutation(
            &account,
            &id,
            &kind,
            &HashMap::from([(id.0.clone(), cached)]),
        )
        .expect("route")
        .expect("request");
        account.etag_index.write().await.remove(&id.0);
        let mut refreshed = HashMap::new();
        assert!(
            refresh_missing_etags(&account, std::slice::from_ref(&id), &kind, &mut refreshed)
                .await
                .is_empty()
        );
        let after = request_for_mutation(&account, &id, &kind, &refreshed)
            .expect("route")
            .expect("request");
        let before = before.headers.expect("headers");
        let after = after.headers.expect("headers");
        assert_eq!(before["If-Match"].as_bytes(), b"W/\"CK1\"");
        assert_eq!(before["If-Match"].as_bytes(), after["If-Match"].as_bytes());
    }

    fn shared_account() -> GraphAccount {
        GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        )
    }

    fn foreign_message_id(mailbox: &str, folder: &str, native: &str) -> ObjectId {
        let scope = CursorScope::FolderType {
            folder: encode_foreign(mailbox, folder),
            ty: ObjectType::Email,
        };
        encode_message_id(&scope, native)
    }

    fn etag_map(id: &ObjectId) -> HashMap<String, String> {
        let mut etags = HashMap::new();
        etags.insert(id.0.clone(), "W/\"CK1\"".to_string());
        etags
    }

    fn ok_response(id: &str) -> crate::types::BatchResponseItem {
        crate::types::BatchResponseItem {
            id: id.to_string(),
            status: 200,
            headers: None,
            body: None,
        }
    }

    fn outcome_item(outcome: &ItemOutcome<MutationSuccess>) -> String {
        match outcome {
            ItemOutcome::Succeeded(success) => success.item.0.clone(),
            ItemOutcome::Failed(failure) => failure.item.0.clone(),
            ItemOutcome::Uncertain(uncertain) => uncertain.item.0.clone(),
        }
    }

    /// A `$batch` response that omits a `move` subresponse is NOT proof
    /// the move was skipped - a move that committed and then lost its
    /// subresponse is byte-identical on the wire. The id belongs on the
    /// uncertain lane so the engine reads it back; the terminal
    /// `Failed(ContractViolation)` this replaced made the engine record
    /// `failed_terminal` with no read-back at all, so a landed move was
    /// reported to the consumer as permanently lost.
    #[test]
    fn an_unanswered_move_lands_on_the_uncertain_lane_for_readback() {
        let request_ids = vec![ObjectId("m0".to_string()), ObjectId("m1".to_string())];
        let mut outcomes = Vec::new();
        reconcile_mutation_responses(
            &request_ids,
            resolve_batch_responses(&request_ids, vec![ok_response("0")]),
            &MutationKind::Move(MembershipScope::Folder(FolderId("archive".to_string()))),
            &mut outcomes,
            &|_| None,
        );

        assert_eq!(outcomes.len(), 2);
        let ItemOutcome::Uncertain(uncertain) = &outcomes[1] else {
            panic!("expected uncertain, got {:?}", outcomes[1]);
        };
        assert_eq!(uncertain.item.0, "m1");
        assert!(matches!(
            uncertain.error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        ));
        // Non-idempotent: probe the target rather than replaying the move.
        assert!(uncertain.error.recovery().requires_reconciliation());
        assert!(!uncertain.error.recovery().is_terminal());
    }

    /// An absolute-state flag write is idempotent, so the same ambiguity
    /// classifies retryable - but it still rides the uncertain lane,
    /// because the engine's read-back guard is what turns "may have
    /// landed" into a fact for either shape.
    #[test]
    fn an_unanswered_flag_write_is_uncertain_and_retryable() {
        let request_ids = vec![ObjectId("m0".to_string())];
        let mut outcomes = Vec::new();
        reconcile_mutation_responses(
            &request_ids,
            Vec::new(),
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &mut outcomes,
            &|_| None,
        );

        let ItemOutcome::Uncertain(uncertain) = &outcomes[0] else {
            panic!("expected uncertain, got {:?}", outcomes[0]);
        };
        assert!(uncertain.error.recovery().is_retryable());
    }

    /// A response id outside the submitted range, unparsable, or repeated
    /// is discarded rather than answered. Accepting one would put an id
    /// the caller never submitted on a lane (or two outcomes on one id)
    /// while the real id stays unanswered.
    #[test]
    fn invalid_and_duplicate_mutation_response_ids_are_discarded() {
        let request_ids = vec![ObjectId("m0".to_string())];
        let mut outcomes = Vec::new();
        reconcile_mutation_responses(
            &request_ids,
            resolve_batch_responses(
                &request_ids,
                vec![
                    ok_response("0"),
                    ok_response("0"),
                    ok_response("9"),
                    ok_response("nope"),
                ],
            ),
            &MutationKind::Destroy,
            &mut outcomes,
            &|_| None,
        );

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcome_item(&outcomes[0]), "m0");
        assert!(matches!(outcomes[0], ItemOutcome::Succeeded(_)));
    }

    #[tokio::test]
    async fn mutation_responses_refresh_or_evict_consumed_etags() {
        let account = shared_account();
        account
            .etag_index
            .write()
            .await
            .insert("m0".to_string(), "old".to_string());
        let responses = vec![crate::types::BatchResponseItem {
            id: "0".to_string(),
            status: 200,
            headers: Some(HashMap::from([("ETag".to_string(), "new".to_string())])),
            body: None,
        }];
        let ids = [ObjectId("m0".to_string())];
        update_etag_cache_from_responses(
            &account,
            &ids,
            &resolve_batch_responses(&ids, responses),
            &MutationKind::SetFlags(FlagOp::Set(HashSet::new())),
        )
        .await;
        assert_eq!(
            account.etag_index.write().await.get("m0").as_deref(),
            Some("new")
        );

        let conflict = vec![crate::types::BatchResponseItem {
            id: "0".to_string(),
            status: 412,
            headers: None,
            body: None,
        }];
        update_etag_cache_from_responses(
            &account,
            &ids,
            &resolve_batch_responses(&ids, conflict),
            &MutationKind::SetFlags(FlagOp::Set(HashSet::new())),
        )
        .await;
        assert_eq!(account.etag_index.write().await.get("m0"), None);
    }

    #[tokio::test]
    async fn a_destroy_404_evicts_the_cached_etag_for_the_skipped_item() {
        let account = shared_account();
        account
            .etag_index
            .write()
            .await
            .insert("m0".to_string(), "old".to_string());
        let ids = [ObjectId("m0".to_string())];
        let responses = vec![crate::types::BatchResponseItem {
            id: "0".to_string(),
            status: 404,
            headers: None,
            body: None,
        }];

        update_etag_cache_from_responses(
            &account,
            &ids,
            &resolve_batch_responses(&ids, responses),
            &MutationKind::Destroy,
        )
        .await;

        assert_eq!(account.etag_index.write().await.get("m0"), None);
    }

    /// A malformed `$batch` answering one request twice - a `412`
    /// concurrency conflict followed by a duplicate `200` - must not leave
    /// the etag cache and the outcome lanes disagreeing. The outcome
    /// projection has always taken the FIRST response, so the cache must
    /// too: installing the second response's etag would hand the retry a
    /// version the caller was simultaneously told was stale.
    #[tokio::test]
    async fn a_duplicate_success_after_a_conflict_cannot_reinstall_an_etag() {
        let account = shared_account();
        account
            .etag_index
            .write()
            .await
            .insert("m0".to_string(), "old".to_string());
        let ids = [ObjectId("m0".to_string())];
        let resolved = resolve_batch_responses(
            &ids,
            vec![
                crate::types::BatchResponseItem {
                    id: "0".to_string(),
                    status: 412,
                    headers: None,
                    body: None,
                },
                crate::types::BatchResponseItem {
                    id: "0".to_string(),
                    status: 200,
                    headers: Some(HashMap::from([("ETag".to_string(), "new".to_string())])),
                    body: None,
                },
            ],
        );
        let kind = MutationKind::SetFlags(FlagOp::Set(HashSet::new()));
        update_etag_cache_from_responses(&account, &ids, &resolved, &kind).await;
        let mut outcomes = Vec::new();
        reconcile_mutation_responses(&ids, resolved, &kind, &mut outcomes, &|_| None);

        assert_eq!(
            account.etag_index.write().await.get("m0"),
            None,
            "the conflict evicted the etag and the duplicate did not reinstall it"
        );
        assert_eq!(outcomes.len(), 1, "one outcome per submitted id");
        assert!(
            matches!(outcomes[0], ItemOutcome::Failed(_)),
            "the accepted outcome is the 412 conflict, got {:?}",
            outcomes[0]
        );
    }

    #[test]
    fn foreign_flag_mutation_routes_to_owner_with_native_id() {
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let etags = etag_map(&id);
        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &etags,
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(request.url, "/users/shared%40contoso.com/messages/AAMkmsg");
        assert!(!request.url.contains('\u{1f}'), "{}", request.url);
    }

    #[test]
    fn primary_flag_mutation_stays_on_me() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let etags = etag_map(&id);
        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &etags,
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(request.url, "/me/messages/AAMkmsg");
    }

    #[test]
    fn foreign_destroy_routes_to_owner() {
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let request = request_for_mutation(&account, &id, &MutationKind::Destroy, &HashMap::new())
            .expect("builds")
            .expect("some request");
        assert_eq!(request.method, "POST");
        assert_eq!(
            request.url,
            "/users/shared%40contoso.com/messages/AAMkmsg/permanentDelete"
        );
        assert!(request.body.is_none());
    }

    #[test]
    fn bulk_move_routes_to_owner_with_native_destination_folder() {
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let etags = etag_map(&id);
        // The destination folder belongs to the same shared mailbox.
        let destination = MembershipScope::Folder(encode_foreign("shared@contoso.com", "AAMkdest"));
        let request = request_for_mutation(&account, &id, &MutationKind::Move(destination), &etags)
            .expect("builds")
            .expect("some request");
        assert_eq!(
            request.url,
            "/users/shared%40contoso.com/messages/AAMkmsg/move"
        );
        // The move body's destinationId is the native folder id, not the
        // `\u{1f}`-encoded FolderId.
        assert_eq!(request.body, Some(json!({ "destinationId": "AAMkdest" })));
    }

    #[test]
    fn cross_mailbox_move_fails_cleanly() {
        let account = shared_account();
        // Source message in the shared mailbox, destination in the primary
        // mailbox: not expressible against one endpoint -> None (the caller
        // turns this into a Request(Malformed) failure).
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let etags = etag_map(&id);
        let primary_dest = MembershipScope::Folder(FolderId("inbox".to_string()));
        assert!(
            request_for_mutation(&account, &id, &MutationKind::Move(primary_dest), &etags)
                .expect("builds")
                .is_none()
        );

        // The mirror: a primary source moving into a shared-mailbox folder
        // is equally inexpressible.
        let primary_id = ObjectId("AAMkmsg".to_string());
        let primary_etags = etag_map(&primary_id);
        let foreign_dest =
            MembershipScope::Folder(encode_foreign("shared@contoso.com", "AAMkdest"));
        assert!(
            request_for_mutation(
                &account,
                &primary_id,
                &MutationKind::Move(foreign_dest),
                &primary_etags,
            )
            .expect("builds")
            .is_none()
        );
    }

    /// A stale shared-mailbox id has no endpoint, and `Destroy` is the kind
    /// that reaches `request_for_mutation` with it: `SetFlags` / `Move` are
    /// filtered out earlier by the etag preflight, which already files them
    /// per item, while `Destroy` needs no etag at all. Propagating the
    /// routing error - what this path used to do - left every valid sibling
    /// of the chunk with no outcome in any lane, and the terminal
    /// `Request(Malformed)` it raised named the request rather than the id,
    /// so the engine's mutation pipeline could not sweep the unseen ids for
    /// read-back either.
    ///
    /// Wholly unroutable, hence hermetic: no chunk reaches `$batch`.
    #[tokio::test]
    async fn a_stale_destroy_chunk_answers_every_id_instead_of_aborting() {
        let account = shared_account();
        let ids = [
            foreign_message_id("gone@contoso.com", "AAMkfolder", "AAMkone"),
            foreign_message_id("gone@contoso.com", "AAMkfolder", "AAMktwo"),
        ];
        let events = submit_batch(&account, &ids, &MutationKind::Destroy)
            .await
            .expect("a stale id is a per-item failure, not a chunk failure");
        let [SyncEvent::Batch(batch)] = events.as_slice() else {
            panic!("expected exactly one Batch event, got {events:?}");
        };
        assert_eq!(batch.items.len(), 2);
        for (item, id) in batch.items.iter().zip(ids.iter()) {
            let ItemOutcome::Failed(failure) = item else {
                panic!("expected a failed outcome, got {item:?}");
            };
            assert_eq!(failure.item.0, id.0);
            assert!(matches!(
                failure.error.kind(),
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            ));
            assert_eq!(
                failure.error.scope(),
                Some(&ErrorScope::Message {
                    id: (id.0.clone()).into()
                })
            );
        }
    }

    /// The other half of the same rule: the routing decision is per id, so
    /// a live sibling of a stale id still builds its request. The `$batch`
    /// round trip that request then makes is exercised through the
    /// `GraphClient` REST seam by the wire-level batch tests.
    #[test]
    fn a_stale_id_rejects_while_its_live_sibling_still_builds() {
        let account = shared_account();
        let stale = foreign_message_id("gone@contoso.com", "AAMkfolder", "AAMkmsg");
        let live = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        assert!(
            request_for_mutation(&account, &stale, &MutationKind::Destroy, &HashMap::new())
                .is_err()
        );
        let request =
            request_for_mutation(&account, &live, &MutationKind::Destroy, &HashMap::new())
                .expect("the configured mailbox still routes")
                .expect("some request");
        assert_eq!(
            request.url,
            "/users/shared%40contoso.com/messages/AAMkmsg/permanentDelete"
        );
    }

    #[test]
    fn batch_ids_are_assigned_in_order() {
        let mut requests = vec![
            BatchRequestItem {
                id: String::new(),
                method: "DELETE".to_string(),
                url: "/me/messages/a".to_string(),
                body: None,
                headers: None,
            },
            BatchRequestItem {
                id: String::new(),
                method: "DELETE".to_string(),
                url: "/me/messages/b".to_string(),
                body: None,
                headers: None,
            },
        ];
        assign_batch_ids(&mut requests);
        assert_eq!(requests[0].id, "0");
        assert_eq!(requests[1].id, "1");
    }

    fn flag_set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn set_clears_unflagged_owned_fields() {
        // `Set` with only a category present must still authoritatively
        // clear isRead and the flag: a previously-read/flagged message
        // becomes unread/unflagged. (Full-replace semantics.)
        let body = patch_for_flags(&FlagOp::Set(flag_set(&["category:Work"])));
        assert_eq!(body.get("isRead"), Some(&json!(false)));
        assert_eq!(
            body.get("flag"),
            Some(&json!({ "flagStatus": "notFlagged" }))
        );
        assert_eq!(body.get("categories"), Some(&json!(["Work"])));
    }

    #[test]
    fn set_empty_clears_all_owned_fields() {
        let body = patch_for_flags(&FlagOp::Set(HashSet::new()));
        assert_eq!(body.get("isRead"), Some(&json!(false)));
        assert_eq!(
            body.get("flag"),
            Some(&json!({ "flagStatus": "notFlagged" }))
        );
        assert_eq!(body.get("categories"), Some(&json!([] as [&str; 0])));
    }

    #[test]
    fn set_with_read_and_flagged_writes_true() {
        let body = patch_for_flags(&FlagOp::Set(flag_set(&["\\Seen", "\\Flagged"])));
        assert_eq!(body.get("isRead"), Some(&json!(true)));
        assert_eq!(body.get("flag"), Some(&json!({ "flagStatus": "flagged" })));
    }

    #[test]
    fn add_does_not_clear_absent_fields() {
        // `Add` is incremental: only the present flag is written, others
        // are untouched.
        let body = patch_for_flags(&FlagOp::Add(flag_set(&["\\Seen"])));
        assert_eq!(body.get("isRead"), Some(&json!(true)));
        assert!(body.get("flag").is_none());
        assert!(body.get("categories").is_none());
    }

    #[test]
    fn remove_clears_the_named_fields() {
        let body = patch_for_flags(&FlagOp::Remove(flag_set(&["\\Seen", "starred"])));
        assert_eq!(body.get("isRead"), Some(&json!(false)));
        assert_eq!(
            body.get("flag"),
            Some(&json!({ "flagStatus": "notFlagged" }))
        );
    }

    #[test]
    fn patch_rejects_a_contested_flag_before_building_a_body() {
        let op = FlagOp::Patch {
            add: flag_set(&["\\Seen", "\\Flagged"]),
            remove: flag_set(&["\\Seen"]),
        };
        let error = invalid_flag_op_error(&op).expect("contested Patch is invalid");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(
            invalid_flag_op_error(&FlagOp::Patch {
                add: flag_set(&["read"]),
                remove: flag_set(&["\\Seen"]),
            })
            .is_some()
        );
    }

    #[test]
    fn every_graph_field_alias_is_a_patch_contradiction() {
        for aliases in [
            &["\\Seen", "read"][..],
            &["\\Flagged", "flagged", "starred"][..],
        ] {
            for added in aliases {
                for removed in aliases {
                    assert!(
                        invalid_flag_op_error(&FlagOp::Patch {
                            add: flag_set(&[added]),
                            remove: flag_set(&[removed]),
                        })
                        .is_some(),
                        "{added} and {removed} project to one Graph field"
                    );
                }
            }
        }
    }

    #[test]
    fn set_sorts_categories_for_a_deterministic_body() {
        let body = patch_for_flags(&FlagOp::Set(flag_set(&[
            "category:Zeta",
            "category:alpha",
            "category:Mid",
        ])));
        // Byte order, not locale order - the point is only that the same
        // input set always produces the same JSON.
        assert_eq!(
            body.get("categories"),
            Some(&json!(["Mid", "Zeta", "alpha"]))
        );
    }

    #[test]
    fn flag_names_are_matched_case_insensitively() {
        for token in ["\\seen", "\\SEEN", "Read", "read"] {
            let body = patch_for_flags(&FlagOp::Add(flag_set(&[token])));
            assert_eq!(
                body.get("isRead"),
                Some(&json!(true)),
                "token {token} did not set isRead"
            );
        }
        for token in ["\\flagged", "\\FLAGGED", "Flagged", "Starred"] {
            let body = patch_for_flags(&FlagOp::Add(flag_set(&[token])));
            assert_eq!(
                body.get("flag"),
                Some(&json!({ "flagStatus": "flagged" })),
                "token {token} did not set the flag"
            );
        }
    }

    /// Incremental category changes need a category-array read-modify-write.
    /// Until that exists, every shape containing a category is rejected as a
    /// unit so a mixed flag/category request cannot partially apply either.
    #[test]
    fn category_incremental_ops_are_detected_before_building_a_patch() {
        assert!(flag_op_requires_category_rmw(&FlagOp::Add(flag_set(&[
            "category:Work"
        ]))));
        assert!(flag_op_requires_category_rmw(&FlagOp::Remove(flag_set(&[
            "category:Work"
        ]))));
        assert!(flag_op_requires_category_rmw(&FlagOp::Patch {
            add: flag_set(&["category:Work"]),
            remove: flag_set(&["category:Old"]),
        }));
        assert!(!flag_op_requires_category_rmw(&FlagOp::Set(flag_set(&[
            "category:Work"
        ]))));
        assert!(!flag_op_requires_category_rmw(&FlagOp::Add(flag_set(&[
            "\\Seen"
        ]))));
    }

    #[tokio::test]
    async fn category_incremental_ops_fail_without_emitting_an_empty_patch() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let events = submit_batch(
            &account,
            std::slice::from_ref(&id),
            &MutationKind::SetFlags(FlagOp::Add(flag_set(&["category:Work"]))),
        )
        .await
        .expect("preflight category rejection is local");
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("expected a batch outcome");
        };
        let ItemOutcome::Failed(failure) = &batch.items[0] else {
            panic!("category operation must not be applied");
        };
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::UpdateFlags)
        ));
    }

    /// Graph models only `isRead` and `flag.flagStatus`. An incremental op
    /// naming any other flag builds `{}`, which Graph answers 200 to, so it
    /// has to be refused before the wire rather than filed `Applied`.
    #[test]
    fn flags_graph_does_not_model_are_unexpressible() {
        for token in ["\\Answered", "\\Draft", "$Junk", "arbitrary-keyword"] {
            assert!(
                flag_op_is_unexpressible(&FlagOp::Add(flag_set(&[token]))),
                "add {token} built a non-empty patch"
            );
            assert!(
                flag_op_is_unexpressible(&FlagOp::Remove(flag_set(&[token]))),
                "remove {token} built a non-empty patch"
            );
        }
        assert!(flag_op_is_unexpressible(&FlagOp::Patch {
            add: flag_set(&["\\Answered"]),
            remove: flag_set(&["\\Draft"]),
        }));
        // A mixed op still carries an expressible half, so it proceeds.
        assert!(!flag_op_is_unexpressible(&FlagOp::Add(flag_set(&[
            "\\Answered",
            "\\Seen"
        ]))));
        // `Set` is full-replace and always writes all three owned fields.
        assert!(!flag_op_is_unexpressible(&FlagOp::Set(HashSet::new())));
    }

    #[tokio::test]
    async fn unmodelled_flag_ops_fail_rather_than_report_applied() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let events = submit_batch(
            &account,
            std::slice::from_ref(&id),
            &MutationKind::SetFlags(FlagOp::Add(flag_set(&["\\Answered"]))),
        )
        .await
        .expect("preflight rejection is local");
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("expected a batch outcome");
        };
        let ItemOutcome::Failed(failure) = &batch.items[0] else {
            panic!("an empty patch must not be reported as applied");
        };
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::UpdateFlags)
        ));
    }

    #[test]
    fn set_flags_without_an_etag_refuses_to_build_a_request() {
        // `If-Match` is mandatory for SetFlags/Move; without a cached or
        // refreshed etag the item must fail rather than write unconditioned.
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
                &HashMap::new(),
            )
            .expect("builds")
            .is_none()
        );
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::Move(MembershipScope::Folder(FolderId("archive".to_string()))),
                &HashMap::new(),
            )
            .expect("builds")
            .is_none()
        );
    }

    #[test]
    fn destroy_attaches_if_match_opportunistically() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());

        let unconditioned =
            request_for_mutation(&account, &id, &MutationKind::Destroy, &HashMap::new())
                .expect("builds")
                .expect("destroy needs no etag");
        assert!(unconditioned.headers.is_none());
        assert_eq!(unconditioned.method, "POST");
        assert_eq!(unconditioned.url, "/me/messages/AAMkmsg/permanentDelete");
        assert!(unconditioned.body.is_none());

        let conditioned =
            request_for_mutation(&account, &id, &MutationKind::Destroy, &etag_map(&id))
                .expect("builds")
                .expect("some request");
        assert_eq!(
            conditioned
                .headers
                .as_ref()
                .and_then(|h| h.get("If-Match"))
                .map(String::as_str),
            Some("W/\"CK1\"")
        );
        assert!(conditioned.body.is_none());
    }

    #[test]
    fn move_to_a_non_folder_destination_is_rejected() {
        // Only `MembershipScope::Folder` names a Graph move target; a
        // mailbox-scoped destination is a caller error, surfaced by the
        // caller as `Request(Malformed)`.
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let etags = etag_map(&id);
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::Move(MembershipScope::Mailbox(bifrost_types::MailboxId(
                    "shared@contoso.com".to_string()
                ))),
                &etags,
            )
            .expect("builds")
            .is_none()
        );
    }

    #[test]
    fn primary_move_uses_the_bare_destination_folder_id() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let etags = etag_map(&id);
        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::Move(MembershipScope::Folder(FolderId("archive".to_string()))),
            &etags,
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(request.method, "POST");
        assert_eq!(request.url, "/me/messages/AAMkmsg/move");
        assert_eq!(request.body, Some(json!({ "destinationId": "archive" })));
    }

    #[test]
    fn set_flags_conditions_on_the_cached_etag_keyed_by_the_encoded_id() {
        // The etag cache is keyed by the CALLER-facing (foreign-encoded) id
        // even though the URL carries the native one; a lookup by native id
        // would miss and drop the mutation.
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let mut native_keyed = HashMap::new();
        native_keyed.insert("AAMkmsg".to_string(), "W/\"CK1\"".to_string());
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
                &native_keyed,
            )
            .expect("builds")
            .is_none(),
            "a native-keyed etag must not satisfy an encoded-id lookup"
        );

        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &etag_map(&id),
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(
            request
                .headers
                .as_ref()
                .and_then(|h| h.get("If-Match"))
                .map(String::as_str),
            Some("W/\"CK1\"")
        );
    }

    #[test]
    fn operation_labels_match_the_mutation_kind() {
        assert_eq!(
            operation_for_kind(&MutationKind::SetFlags(FlagOp::Add(HashSet::new()))),
            AccountOperation::UpdateFlags
        );
        assert_eq!(
            operation_for_kind(&MutationKind::Move(MembershipScope::Folder(FolderId(
                "archive".to_string()
            )))),
            AccountOperation::BulkMove
        );
        assert_eq!(
            operation_for_kind(&MutationKind::Destroy),
            AccountOperation::BulkDestroy
        );
    }

    #[test]
    fn only_flag_bearing_kinds_require_an_etag() {
        assert!(requires_etag(&MutationKind::SetFlags(FlagOp::Add(
            HashSet::new()
        ))));
        assert!(requires_etag(&MutationKind::Move(MembershipScope::Folder(
            FolderId("archive".to_string())
        ))));
        assert!(!requires_etag(&MutationKind::Destroy));
    }

    fn idempotency_key() -> IdempotencyKey {
        IdempotencyKey {
            run_id: bifrost_types::RunId("run-1".to_string()),
            sequence: 0,
            protocol_salt: bifrost_types::ProtocolSalt::Graph("salt".to_string()),
        }
    }

    /// The mutation lane's terminus is the end of the stream, not a
    /// `PageBoundary::Final` tag. `bifrost-sync`'s mutation consumer loops
    /// on `stream.next()` and matches only `Batch` / `Terminated`; it never
    /// reads `page_boundary` here, and a mutation batch carries no
    /// checkpoint to close. Pinned so the asymmetry with the hydration lane
    /// (which WAS reworked to tag its terminal chunk `Final`) is a recorded
    /// decision rather than a suspicious-looking gap - and so that a change
    /// to either half is made deliberately.
    #[tokio::test]
    async fn mutation_batches_are_paged_and_the_stream_end_is_the_terminus() {
        let account = shared_account();
        account
            .client
            .script_rest([crate::client::ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({ "responses": [{ "id": "0", "status": 204 }] }),
            )]);
        let targets: AccountStream<ObjectId> =
            Box::pin(futures::stream::iter([ObjectId("AAMk1".to_string())]));
        let mut stream = bulk_destroy_stream(account, targets, idempotency_key());

        let mut boundaries = Vec::new();
        let mut saw_done = false;
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => boundaries.push(batch.page_boundary),
                SyncEvent::Done(checkpoint) => {
                    assert!(checkpoint.is_none(), "a mutation lane advances no cursor");
                    saw_done = true;
                }
                SyncEvent::Terminated(error) => panic!("unexpected termination: {error:?}"),
                _ => {}
            }
        }

        assert!(!boundaries.is_empty(), "the chunk must produce a batch");
        assert!(
            boundaries
                .iter()
                .all(|boundary| matches!(boundary, PageBoundary::Page)),
            "mutation batches are Page, never Final: {boundaries:?}"
        );
        assert!(saw_done, "the stream closes with Done(None)");
    }

    #[tokio::test]
    async fn failed_batch_envelopes_preserve_local_outcomes_and_drain_later_chunks() {
        use bifrost_net::test_support::Canned;
        use bifrost_types::TransmissionState;
        for (failure, uncertain) in [
            (
                Canned::Error(bifrost_net::Error::Network {
                    message: "reset after send".to_string(),
                    transmission_state: TransmissionState::InFlight,
                    source: None,
                }),
                true,
            ),
            (
                Canned::Error(bifrost_net::Error::Network {
                    message: "connect failed".to_string(),
                    transmission_state: TransmissionState::Unsent,
                    source: None,
                }),
                false,
            ),
            (
                Canned::Response {
                    status: reqwest::StatusCode::BAD_REQUEST,
                    headers: Default::default(),
                    body: bytes::Bytes::from_static(b"{}"),
                },
                false,
            ),
            (
                Canned::Response {
                    status: reqwest::StatusCode::FORBIDDEN,
                    headers: Default::default(),
                    body: bytes::Bytes::from_static(b"{}"),
                },
                false,
            ),
            (
                Canned::Response {
                    status: reqwest::StatusCode::OK,
                    headers: Default::default(),
                    body: bytes::Bytes::from_static(b"not JSON"),
                },
                true,
            ),
        ] {
            let mut account = shared_account();
            account.capabilities.batching_policy.max_items = 2;
            let client = account.client.clone();
            client.script_wire(
                [
                    failure,
                    Canned::Response {
                        status: reqwest::StatusCode::OK,
                        headers: Default::default(),
                        body: bytes::Bytes::from_static(
                            b"{\"responses\":[{\"id\":\"0\",\"status\":204}]}",
                        ),
                    },
                ],
                bifrost_net::RetryPolicy::disabled(),
            );
            let stale = foreign_message_id("gone@contoso.com", "folder", "stale");
            let ids = [
                stale.clone(),
                ObjectId("submitted".to_string()),
                ObjectId("later".to_string()),
            ];
            let targets = Box::pin(futures::stream::iter(ids.clone()));
            let events: Vec<_> = bulk_destroy_stream(account, targets, idempotency_key())
                .collect()
                .await;
            let mut outcomes = Vec::new();
            let mut done = 0;
            for event in events {
                match event {
                    SyncEvent::Batch(batch) => outcomes.extend(batch.items),
                    SyncEvent::Done(None) => done += 1,
                    SyncEvent::Terminated(error) => {
                        panic!("chunk failure terminated targets: {error:?}")
                    }
                    other => panic!("unexpected event: {other:?}"),
                }
            }
            assert_eq!(done, 1);
            assert_eq!(outcomes.len(), 3);
            assert_eq!(
                outcomes.iter().map(outcome_item).collect::<Vec<_>>(),
                ids.iter().map(|id| id.0.clone()).collect::<Vec<_>>()
            );
            assert!(matches!(&outcomes[0], ItemOutcome::Failed(_)));
            assert_eq!(matches!(&outcomes[1], ItemOutcome::Uncertain(_)), uncertain);
            assert_eq!(matches!(&outcomes[1], ItemOutcome::Failed(_)), !uncertain);
            let error = match &outcomes[1] {
                ItemOutcome::Failed(failure) => &failure.error,
                ItemOutcome::Uncertain(uncertain) => &uncertain.error,
                ItemOutcome::Succeeded(_) => panic!("failed envelope cannot confirm a write"),
            };
            assert_eq!(
                error.scope(),
                Some(&ErrorScope::Message {
                    id: "submitted".into()
                })
            );
            assert_eq!(error.operation(), Some(AccountOperation::BulkDestroy));
            assert!(matches!(&outcomes[2], ItemOutcome::Succeeded(success)
                if success.output == MutationSuccess::Applied));
            let requests = client.take_rest_requests();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0].body.as_ref().unwrap()["requests"][0]["method"],
                "POST"
            );
            assert_eq!(
                requests[0].body.as_ref().unwrap()["requests"][0]["url"],
                "/me/messages/submitted/permanentDelete"
            );
        }
    }

    #[tokio::test]
    async fn rejected_batch_keeps_a_failed_etag_preflight() {
        let account = shared_account();
        account
            .etag_index
            .write()
            .await
            .insert("cached".to_string(), "etag".to_string());
        account.client.script_rest([
            crate::client::ScriptedRestResponse::empty(reqwest::StatusCode::NOT_FOUND),
            crate::client::ScriptedRestResponse::empty(reqwest::StatusCode::BAD_REQUEST),
        ]);
        let events = submit_batch(
            &account,
            &[
                ObjectId("missing".to_string()),
                ObjectId("cached".to_string()),
            ],
            &MutationKind::SetFlags(FlagOp::Set(HashSet::new())),
        )
        .await
        .expect("envelope failures are per item");
        let [SyncEvent::Batch(batch)] = events.as_slice() else {
            panic!("expected batch")
        };
        assert_eq!(batch.items.len(), 2);
        assert_eq!(outcome_item(&batch.items[0]), "missing");
        assert_eq!(outcome_item(&batch.items[1]), "cached");
        assert!(
            batch
                .items
                .iter()
                .all(|item| matches!(item, ItemOutcome::Failed(_)))
        );
        let requests = account.client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].body.as_ref().unwrap()["requests"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn rejected_batch_keeps_a_cross_mailbox_move_refusal() {
        let account = shared_account();
        let foreign = foreign_message_id("shared@contoso.com", "folder", "foreign");
        let local = ObjectId("local".to_string());
        for id in [&foreign, &local] {
            account
                .etag_index
                .write()
                .await
                .insert(id.0.clone(), "etag".to_string());
        }
        account
            .client
            .script_rest([crate::client::ScriptedRestResponse::empty(
                reqwest::StatusCode::BAD_REQUEST,
            )]);
        let events = submit_batch(
            &account,
            &[foreign.clone(), local.clone()],
            &MutationKind::Move(MembershipScope::Folder(FolderId("archive".to_string()))),
        )
        .await
        .expect("envelope failures are per item");
        let [SyncEvent::Batch(batch)] = events.as_slice() else {
            panic!("expected batch")
        };
        assert_eq!(batch.items.len(), 2);
        assert_eq!(outcome_item(&batch.items[0]), foreign.0);
        assert_eq!(outcome_item(&batch.items[1]), local.0);
        assert!(
            batch
                .items
                .iter()
                .all(|item| matches!(item, ItemOutcome::Failed(_)))
        );
        let requests = account.client.take_rest_requests();
        assert_eq!(requests.len(), 1);
        let submitted = requests[0].body.as_ref().unwrap()["requests"]
            .as_array()
            .unwrap();
        assert_eq!(submitted.len(), 1);
        assert_eq!(submitted[0]["url"], "/me/messages/local/move");
    }

    #[tokio::test]
    async fn an_empty_target_stream_yields_only_done() {
        // No ids means no `$batch`; the stream must not emit an empty
        // `Batch` (which the engine would read as "0 of N applied").
        let account = shared_account();
        let targets: AccountStream<ObjectId> = Box::pin(futures::stream::empty());
        let mut stream = bulk_destroy_stream(account, targets, idempotency_key());
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
        assert!(stream.next().await.is_none());
    }
}
