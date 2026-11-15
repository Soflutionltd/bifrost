//! Per-message writes (flags, categories, extended properties,
//! importance) and the shared `$batch` write pipeline that resolves
//! targets, submits the batch, and reconciles per-item outcomes.

use crate::account::GraphAccount;
use crate::account::GraphClient;
use crate::account::graph_error::{
    GraphErrorContext, batch_response_missing, into_account_error, mutation_item_outcome,
    unsupported_account_error,
};
use crate::account::inventory::graph_etag;
use crate::client::Resource;
use crate::etag::GraphETag;
use crate::types::{BatchRequest, BatchRequestItem, BatchResponse, ODataCollection};
use bifrost_types::{
    AccountError, AccountOperation, ContainerId, ErrorScope, Importance, MutationTarget, ObjectId,
};
use bifrost_types::{
    Application, PartialCommitBuilder, PrimaryLeg, StepEffect, StepReplay, StepTargets, TargetRef,
    TargetRole,
};
use serde_json::{Value, json};

use super::common::*;
use super::threads::*;

#[cfg(test)]
pub(super) use crate::account::flags::STARRED_CATEGORY;
pub(super) const PR_LAST_VERB_EXECUTED_ALIAS: &str = "PR_LAST_VERB_EXECUTED";
pub(super) const PR_LAST_VERB_EXECUTED_GRAPH_ID: &str = "Integer 0x1081";

// Returned by send_message / draft_create when the request carries an
// AttachmentHandle minted by attachment_upload. Graph does not
// implement upload-session attachment_upload (capability flag false),
pub(crate) async fn add_to_container(
    account: GraphAccount,
    target: MutationTarget,
    container: ContainerId,
) -> Result<(), AccountError> {
    // Refused before thread expansion, which is itself a Graph REST read.
    if crate::account::mutate::public_write_destination(
        &account,
        &bifrost_types::FolderId(container.0.clone()),
    )
    .await
    {
        return Err(crate::account::graph_error::unsupported_account_error(
            AccountOperation::AddToContainer,
        ));
    }
    let ids = resolve_target_ids(&account, target, AccountOperation::AddToContainer).await?;
    move_messages(
        &account,
        &ids,
        &container.0,
        AccountOperation::AddToContainer,
    )
    .await
}

pub(crate) async fn set_category(
    account: GraphAccount,
    target: MutationTarget,
    category: String,
    value: bool,
) -> Result<(), AccountError> {
    let values = resolve_target_values(
        &account,
        target,
        "id,categories,flag,changeKey",
        AccountOperation::SetCategory,
    )
    .await?;
    let mut patches = Vec::new();
    for ResolvedMessage {
        routing_id,
        value: message,
    } in values
    {
        let id = routing_id;
        let etag = graph_etag(&message).ok_or_else(|| {
            pim_protocol_error(
                AccountOperation::SetCategory,
                Some(ErrorScope::Message {
                    id: id.0.clone().into(),
                }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        let body = if is_starred_category(&category) {
            json!({
                "flag": {
                    "flagStatus": if value { "flagged" } else { "notFlagged" }
                }
            })
        } else {
            let mut categories = categories_from_value(&message);
            if value {
                if !categories.iter().any(|existing| existing == &category) {
                    categories.push(category.clone());
                }
            } else {
                categories.retain(|existing| existing != &category);
            }
            categories.sort();
            json!({ "categories": categories })
        };
        patches.push(MessagePatch { id, body, etag });
    }
    patch_messages(&account, patches, AccountOperation::SetCategory).await
}

pub(crate) async fn set_extended_property(
    account: GraphAccount,
    target: MutationTarget,
    property_id: String,
    value: Option<String>,
) -> Result<(), AccountError> {
    let property_id = graph_extended_property_id(&property_id);
    match value {
        Some(value) => {
            let values = resolve_target_values(
                &account,
                target,
                "id,changeKey",
                AccountOperation::SetExtendedProperty,
            )
            .await?;
            let mut patches = Vec::new();
            for ResolvedMessage {
                routing_id,
                value: message,
            } in values
            {
                let id = routing_id;
                let etag = graph_etag(&message).ok_or_else(|| {
                    pim_protocol_error(
                        AccountOperation::SetExtendedProperty,
                        Some(ErrorScope::Message {
                            id: id.0.clone().into(),
                        }),
                        format!("Graph message {} did not expose an etag", id.0),
                    )
                })?;
                patches.push(MessagePatch {
                    id,
                    body: json!({
                        "singleValueExtendedProperties": [{
                            "id": property_id,
                            "value": value
                        }]
                    }),
                    etag,
                });
            }
            patch_messages(&account, patches, AccountOperation::SetExtendedProperty).await
        }
        None => {
            // Clear: DELETE the property's navigation entry on each
            // resolved message. Graph answers 204 No Content on
            // success and 404 if the property never existed; the
            // batch helper tolerates 404 for the clear path so a
            // partial / never-set state still resolves to Ok.
            let ids =
                resolve_target_ids(&account, target, AccountOperation::SetExtendedProperty).await?;
            delete_extended_property(
                &account,
                &ids,
                &property_id,
                AccountOperation::SetExtendedProperty,
            )
            .await
        }
    }
}

pub(super) async fn delete_extended_property(
    account: &GraphAccount,
    ids: &[ObjectId],
    property_id: &str,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let mut requests = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        // `ids` are the caller-supplied (possibly foreign-encoded) routing
        // ids; route the clear DELETE to the owning mailbox.
        let suffix = format!(
            "/singleValueExtendedProperties/{}",
            bifrost_net::url::encode_path_component(property_id)
        );
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "DELETE".to_string(),
            url: message_batch_url(account, id, &suffix, operation)?,
            body: None,
            headers: None,
        });
    }
    // Absence is successful for an already cleared property. A successful
    // clear can change the parent message's validator, so retain its target
    // for cache eviction as well as error correlation.
    let resource = account
        .batch_resource(ids)
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))?;
    let progress = submit_write_batch_resource(
        account,
        resource,
        requests,
        ids,
        crate::account::batch::AbsentMeans::Success,
        operation,
    )
    .await?;
    batch_progress_error(progress, ids, StepEffect::Updated, StepReplay::Repeatable)
}

pub(crate) async fn set_is_read(
    account: GraphAccount,
    target: MutationTarget,
    is_read: bool,
) -> Result<(), AccountError> {
    let values = resolve_target_values(
        &account,
        target,
        "id,changeKey",
        AccountOperation::SetIsRead,
    )
    .await?;
    let mut patches = Vec::new();
    for ResolvedMessage {
        routing_id,
        value: message,
    } in values
    {
        let id = routing_id;
        let etag = graph_etag(&message).ok_or_else(|| {
            pim_protocol_error(
                AccountOperation::SetIsRead,
                Some(ErrorScope::Message {
                    id: id.0.clone().into(),
                }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        patches.push(MessagePatch {
            id,
            body: json!({ "isRead": is_read }),
            etag,
        });
    }
    patch_messages(&account, patches, AccountOperation::SetIsRead).await
}

/// Exclusive importance overwrite: one `If-Match`-conditioned PATCH of
/// `{ importance }` per resolved message. Graph's `importance` is a
/// single-valued field, so this is a strict single overwrite with no
/// read-modify-write - the consumer never expands one change into two.
pub(crate) async fn set_importance(
    account: GraphAccount,
    target: MutationTarget,
    level: Importance,
) -> Result<(), AccountError> {
    let values = resolve_target_values(
        &account,
        target,
        "id,changeKey",
        AccountOperation::SetImportance,
    )
    .await?;
    let body = graph_importance_body(level);
    let mut patches = Vec::new();
    for ResolvedMessage {
        routing_id,
        value: message,
    } in values
    {
        let id = routing_id;
        let etag = graph_etag(&message).ok_or_else(|| {
            pim_protocol_error(
                AccountOperation::SetImportance,
                Some(ErrorScope::Message {
                    id: id.0.clone().into(),
                }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        patches.push(MessagePatch {
            id,
            body: body.clone(),
            etag,
        });
    }
    patch_messages(&account, patches, AccountOperation::SetImportance).await
}

/// The single exclusive PATCH body for `set_importance`. Maps the
/// uniform level onto Graph's single-valued `importance` field - one
/// overwrite, never a clear-then-set pair.
pub(super) fn graph_importance_body(level: Importance) -> Value {
    let wire = match level {
        Importance::Low => "low",
        Importance::High => "high",
        // `Normal` and any future variant collapse to Graph's `normal`.
        _ => "normal",
    };
    json!({ "importance": wire })
}

pub(super) fn is_starred_category(category: &str) -> bool {
    crate::account::flags::GraphFlag::is_starred_category(category)
}

pub(super) fn categories_from_value(value: &Value) -> Vec<String> {
    value
        .get("categories")
        .and_then(Value::as_array)
        .map(|categories| {
            categories
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(super) struct MessagePatch {
    pub(super) id: ObjectId,
    pub(super) body: Value,
    pub(super) etag: GraphETag,
}

pub(super) async fn resolve_target_ids(
    account: &GraphAccount,
    target: MutationTarget,
    operation: AccountOperation,
) -> Result<Vec<ObjectId>, AccountError> {
    match target {
        MutationTarget::Message(id) => {
            refuse_public_write(&id, operation)?;
            Ok(vec![id])
        }
        MutationTarget::Thread(thread) => {
            let values = message_values_for_thread(account, &thread, "id").await?;
            let owner = crate::account::foreign::parse_thread_id(&thread)
                .owner()
                .map(str::to_string);
            values
                .iter()
                .map(|value| object_id_from_value(value, operation, owner.as_deref()))
                .collect()
        }
        _ => Err(unsupported_account_error(operation)),
    }
}

/// A resolved message for a write: the routing id (the caller-supplied,
/// possibly foreign-encoded `ObjectId` that the conditioned write must
/// route by) paired with its fetched value (carrying the etag/body). For
/// a `Thread` target the routing id is qualified with the thread's owner, so
/// its subsequent `$batch` subrequest stays in that shared mailbox.
pub(super) struct ResolvedMessage {
    pub(super) routing_id: ObjectId,
    pub(super) value: Value,
}

/// Refuse a write whose target is a public-folder item. Its id wraps a raw
/// EWS `ItemId` that no Graph REST route can address, and this crate has no
/// EWS write arm: sending it to `/me/messages/{itemId}` answers
/// `ErrorItemNotFound`, which a write would report as the item being gone
/// (or, for a tolerated-404 clear, as success). `Unsupported(operation)` is
/// the true answer.
pub(crate) fn refuse_public_write(
    id: &ObjectId,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    crate::account::mutate::refuse_message_write(id, operation)
}

pub(super) async fn resolve_target_values(
    account: &GraphAccount,
    target: MutationTarget,
    select: &str,
    operation: AccountOperation,
) -> Result<Vec<ResolvedMessage>, AccountError> {
    match target {
        MutationTarget::Message(id) => {
            refuse_public_write(&id, operation)?;
            let value = fetch_message_value(account, &id, select).await?;
            Ok(vec![ResolvedMessage {
                routing_id: id,
                value,
            }])
        }
        MutationTarget::Thread(thread) => {
            let values = message_values_for_thread(account, &thread, select).await?;
            let owner = crate::account::foreign::parse_thread_id(&thread)
                .owner()
                .map(str::to_string);
            values
                .into_iter()
                .map(|value| {
                    let routing_id = object_id_from_value(&value, operation, owner.as_deref())?;
                    Ok(ResolvedMessage { routing_id, value })
                })
                .collect()
        }
        _ => Err(unsupported_account_error(operation)),
    }
}

pub(super) async fn fetch_message_value(
    account: &GraphAccount,
    id: &ObjectId,
    select: &str,
) -> Result<Value, AccountError> {
    crate::account::mutate::fetch_message_observation(
        account,
        id,
        select,
        AccountOperation::Hydrate,
    )
    .await
}

pub(super) async fn fetch_paged_values(
    client: &GraphClient,
    first_url: String,
) -> Result<Vec<Value>, AccountError> {
    let ctx = GraphErrorContext::graph(AccountOperation::Hydrate);
    let mut values = Vec::new();
    let mut walk = crate::paging::PageWalk::new("hydration values");
    let mut next_url = Some(
        client
            .api_url(&first_url)
            .map_err(|e| into_account_error(e, ctx.clone()))?,
    );
    while let Some(url) = next_url {
        walk.enter(&url)
            .map_err(|e| into_account_error(e, ctx.clone()))?;
        let page: ODataCollection<Value> = client
            .get(Resource::Outlook, &url)
            .await
            .map_err(|e| into_account_error(e, ctx.clone()))?;
        values.extend(page.value);
        next_url = client
            .admit_next(page.next_link.as_ref())
            .map_err(|e| into_account_error(e, ctx.clone()))?;
    }
    Ok(values)
}

/// Build a per-message `$batch` request URL, decoding the (possibly
/// foreign-encoded) id and routing to the owning mailbox: a shared-mailbox
/// message yields `/users/{owner}/messages/{native}{suffix}`, a primary
/// (bare) id `/me/messages/{id}{suffix}`. `suffix` is the trailing path
/// segment after the message id (`""`, `"/move"`, or a
/// `/singleValueExtendedProperties/...` clear). The `$batch` envelope is
/// always posted on the primary client; routing rides entirely in the
/// per-item URL prefix, mirroring the read paths in `get.rs`.
pub(crate) fn message_batch_url(
    account: &GraphAccount,
    id: &ObjectId,
    suffix: &str,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    // Every caller is a Graph REST `$batch` surface; a public-folder id has
    // no URL here (see `refuse_public_write`).
    refuse_public_write(id, operation)?;
    let parsed = crate::account::foreign::parse_message_id(id);
    // A stale foreign owner is rejected per MESSAGE: this helper is the
    // per-item URL builder for four `$batch` surfaces, and the id it could
    // not route is the only thing that makes the failure actionable.
    let prefix = account
        .client_for_owner(parsed.owner())
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(operation).with_scope(ErrorScope::Message {
                    id: id.0.clone().into(),
                }),
            )
        })?
        .api_path_prefix();
    let enc_id = bifrost_net::url::encode_path_component(parsed.native_id());
    Ok(format!("{prefix}/messages/{enc_id}{suffix}"))
}

pub(super) async fn patch_messages(
    account: &GraphAccount,
    patches: Vec<MessagePatch>,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let targets: Vec<_> = patches.iter().map(|patch| patch.id.clone()).collect();
    let mut requests = Vec::new();
    for patch in patches {
        refuse_public_write(&patch.id, operation)?;
        requests.push(planned_message_write(
            account,
            &patch.id,
            crate::account::mutate::MessageWrite::Patch(patch.body),
            &patch.etag,
            operation,
        )?);
    }
    let progress = submit_message_write_chunks(
        account,
        requests,
        &targets,
        operation,
        StepEffect::Updated,
        StepReplay::Repeatable,
    )
    .await?;
    // Repeatable: each public call refetches the etag it patches under.
    batch_progress_error(
        progress,
        &targets,
        StepEffect::Updated,
        StepReplay::Repeatable,
    )
}

pub(super) async fn move_messages(
    account: &GraphAccount,
    ids: &[ObjectId],
    destination: &str,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    // The destination may itself be foreign-encoded (a shared-mailbox
    // folder); the `move` body's `destinationId` must carry the native
    // folder id, never a `\u{1f}`-bearing one. Decode once up front.
    let dest_folder = bifrost_types::FolderId(destination.to_string());
    // A discovered public folder is a bare EWS `FolderId`, which
    // `parse_folder` reads as primary; Graph REST cannot move into it.
    // Keep admission here too: this door is also called without thread
    // expansion. The earlier check protects expansion reads, this one protects
    // direct move callers; neither can replace the other.
    if crate::account::mutate::public_write_destination(account, &dest_folder).await {
        return Err(crate::account::graph_error::unsupported_account_error(
            operation,
        ));
    }
    for id in ids {
        refuse_public_write(id, operation)?;
    }
    // Refuse from routing identities before an etag GET can contact the
    // wrong mailbox. Both write doors use the planner's owner rule.
    for id in ids {
        if !crate::account::mutate::same_mailbox_destination(id, &dest_folder) {
            return Err(crate::account::graph_error::invalid_item_error(
                operation,
                ErrorScope::Message { id: id.clone() },
                "Graph move requires a same-mailbox folder destination",
            ));
        }
    }
    let values = message_values_for_ids(account, ids, "id,changeKey").await?;
    let mut requests = Vec::new();
    let mut targets = Vec::new();
    // `ids[i]` is the caller-supplied (possibly foreign-encoded) routing
    // id; `values[i]` is its fetched value (etag). Route the request by
    // the routing id so a shared-mailbox message moves under
    // `/users/{owner}`.
    for (id, value) in ids.iter().zip(values.iter()) {
        let etag = graph_etag(value).ok_or_else(|| {
            pim_protocol_error(
                operation,
                Some(ErrorScope::Message {
                    id: id.0.clone().into(),
                }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        requests.push(planned_message_write(
            account,
            id,
            crate::account::mutate::MessageWrite::Move(dest_folder.clone()),
            &etag,
            operation,
        )?);
        targets.push(id.clone());
    }
    let progress = submit_message_write_chunks(
        account,
        requests,
        &targets,
        operation,
        StepEffect::Moved,
        StepReplay::Blocking,
    )
    .await?;
    // Blocking: Graph assigns a moved message a new id, so a replay addresses
    // a source id that no longer exists.
    batch_progress_error(progress, &targets, StepEffect::Moved, StepReplay::Blocking)
}

fn planned_message_write(
    account: &GraphAccount,
    id: &ObjectId,
    effect: crate::account::mutate::MessageWrite,
    etag: &GraphETag,
    operation: AccountOperation,
) -> Result<BatchRequestItem, AccountError> {
    crate::account::mutate::plan_message_write(account, id, effect, Some(etag))
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(operation)
                    .with_scope(ErrorScope::Message { id: id.clone() }),
            )
        })?
        .ok_or_else(|| {
            crate::account::graph_error::invalid_item_error(
                operation,
                ErrorScope::Message { id: id.clone() },
                "Graph write requires a validator and a same-mailbox destination",
            )
        })
}

/// Chunk at the operation owner, where earlier commits can be retained if a
/// later envelope fails. The transport must never hide those commits.
async fn submit_message_write_chunks(
    account: &GraphAccount,
    requests: Vec<BatchRequestItem>,
    targets: &[ObjectId],
    operation: AccountOperation,
    effect: StepEffect,
    replay: StepReplay,
) -> Result<BatchProgress, AccountError> {
    let mut progress = BatchProgress::default();
    for (chunk_index, chunk) in requests.chunks(crate::account::batch::LIMIT).enumerate() {
        let offset = chunk_index * crate::account::batch::LIMIT;
        let chunk_targets = &targets[offset..offset + chunk.len()];
        let mut chunk = chunk.to_vec();
        crate::account::mutate::assign_batch_ids(&mut chunk);
        match submit_write_batch_with_targets(
            account,
            chunk,
            chunk_targets,
            crate::account::batch::AbsentMeans::Failure,
            operation,
        )
        .await
        {
            Ok(next) => {
                let failed = !next.failed.is_empty();
                progress.append(next, offset);
                if failed {
                    break;
                }
            }
            Err(error) => {
                return Err(batch_call_error(
                    progress,
                    targets,
                    chunk_targets,
                    error,
                    effect,
                    replay,
                ));
            }
        }
    }
    Ok(progress)
}

pub(super) async fn message_values_for_ids(
    account: &GraphAccount,
    ids: &[ObjectId],
    select: &str,
) -> Result<Vec<Value>, AccountError> {
    let mut values = Vec::new();
    for id in ids {
        values.push(fetch_message_value(account, id, select).await?);
    }
    Ok(values)
}

/// Submit one `$batch` of writes and return typed progress, never a record:
/// the public operation that owns the replay judgment attaches it through
/// `batch_progress_error`. `targets[i]` is the `ObjectId` of the request whose
/// `BatchRequestItem::id == i.to_string()`, so per-item failures carry
/// `ErrorScope::Message { id }` rather than the coarser `ErrorScope::Account`.
pub(super) async fn submit_write_batch_with_targets(
    account: &GraphAccount,
    requests: Vec<BatchRequestItem>,
    targets: &[ObjectId],
    absent: crate::account::batch::AbsentMeans,
    operation: AccountOperation,
) -> Result<BatchProgress, AccountError> {
    let resource = account
        .batch_resource(targets)
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))?;
    submit_write_batch_resource(account, resource, requests, targets, absent, operation).await
}

async fn submit_write_batch_resource(
    account: &GraphAccount,
    resource: Resource,
    requests: Vec<BatchRequestItem>,
    targets: &[ObjectId],
    absent: crate::account::batch::AbsentMeans,
    operation: AccountOperation,
) -> Result<BatchProgress, AccountError> {
    if requests.is_empty() {
        return Ok(BatchProgress::default());
    }
    let ctx = GraphErrorContext::graph(operation);
    // Track every submitted request id (`BatchRequestItem::id`, assigned
    // `index.to_string()` 0..N-1 by the builders) so we can reconcile the
    // returned responses against them: Graph occasionally returns fewer
    // `$batch` responses than requests, and a missing id is a sub-request
    // whose fate is unknown. Silently treating it as done would violate
    // the batch accounting contract (every id accounted for exactly once),
    // so every unaccounted id becomes a failed, `Uncertain` entry.
    // Kept as an ordered `Vec`, not a set, so the sweep visits missing ids
    // in submission order and the reported failures are stable between
    // runs of a reproducible failure.
    let replayable: std::collections::HashMap<_, _> = requests
        .iter()
        .map(|r| (r.id.clone(), super::super::batch::subrequest_replayable(r)))
        .collect();
    let expected_ids: Vec<String> = requests.iter().map(|r| r.id.clone()).collect();
    let mut seen_ids = std::collections::HashSet::new();
    let mut progress = BatchProgress::default();
    let response: BatchResponse =
        super::super::batch::submit(&account.client, resource, &BatchRequest { requests })
            .await
            .map_err(|e| into_account_error(e, ctx.clone()))?;
    // The loop DRAINS every subresponse rather than stopping at the first bad
    // item. `$batch` response order is Graph's, not the caller's: stopping
    // early once left the etag of a message that demonstrably WAS destroyed
    // in the cache whenever its subresponse sorted after a failing sibling's,
    // and it dropped the successes the partial-commit record must report.
    for (index, mut item) in super::super::batch_routing::resolve_answers(
        // Ruling: a non-numeric submitted id is a caller bug, asserted in
        // debug builds. In production it fails soft: the id is never
        // correlated, so the missing-answer sweep reports it unanswered
        // instead of a panic aborting the user's whole send of the batch.
        expected_ids.iter().filter_map(|id| {
            let parsed = id.parse::<usize>().ok();
            debug_assert!(parsed.is_some(), "submitted batch id {id} is not numeric");
            parsed
        }),
        response.responses,
    ) {
        // Normalize numeric aliases so the missing sweep and replay lookup use
        // the same identity as correlation, not the provider's raw spelling.
        item.id = index.to_string();
        seen_ids.insert(item.id.clone());
        // Reuse the per-item outcome projector so 4xx/5xx items
        // build structured `AccountError`s with `Protocol::Graph`,
        // `AttemptCause(Acknowledged)`, `WireCause::Graph(signal)`
        // (when an envelope is present), and `Retry-After` -> throttle
        // scope translation. Per-item 2xx and destroy-404 short-
        // circuit as `Succeeded`; everything else surfaces as a failed
        // entry carrying its own classified error.
        let (headers, body) = super::super::batch::response_parts(&item);
        // Per-item failures carry `ErrorScope::Message { id }` when
        // the caller provided a parallel `targets` slice; otherwise
        // we fall back to `ErrorScope::Account` (graph-F3 done for
        // patch/move/destroy paths; remaining callers pass empty).
        let target = item
            .id
            .parse::<usize>()
            .ok()
            .and_then(|idx| targets.get(idx))
            .cloned();
        let scope = target
            .as_ref()
            .map(|id| ErrorScope::Message {
                id: (id.0.clone()).into(),
            })
            .unwrap_or(ErrorScope::Account);
        // A conflict invalidates our observation; absence has no validator.
        // Match bulk writes so a later retry cannot reuse a known stale etag.
        if matches!(item.status, 404 | 412)
            && let Some(id) = &target
        {
            account
                .etag_index
                .write()
                .await
                .observe(&id.0, super::super::EtagObservation::Invalidated);
        }
        let outcome = mutation_item_outcome(
            item.status,
            headers,
            body,
            absent,
            bifrost_types::BatchItemId(item.id.clone()),
            operation,
            scope,
            target.as_ref().and_then(|id| account.budget_for_object(id)),
        );
        match outcome {
            bifrost_types::ItemOutcome::Succeeded(_) => {
                if let Ok(index) = item.id.parse::<usize>() {
                    let new_id = item
                        .body
                        .as_ref()
                        .and_then(|v| v.get("id"))
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .map(|id| {
                            let owner = target.as_ref().and_then(|t| {
                                crate::account::foreign::parse_message_id(t)
                                    .owner()
                                    .map(str::to_owned)
                            });
                            ObjectId(crate::account::foreign::qualify_with_owner(
                                owner.as_deref(),
                                id,
                            ))
                        });
                    progress.committed.push((index, new_id));
                }
                // A successful write consumes the observed validator, including
                // PATCHes and moves whose response omits the new etag.
                if let Some(id) = target {
                    account
                        .etag_index
                        .write()
                        .await
                        .observe(&id.0, super::super::EtagObservation::Written(None));
                }
            }
            bifrost_types::ItemOutcome::Failed(failure) => {
                if let Ok(index) = item.id.parse() {
                    progress.failed.push((
                        index,
                        Application::from_leaf(&failure.error),
                        failure.error,
                    ));
                }
            }
            bifrost_types::ItemOutcome::Uncertain(uncertain) => {
                if let Ok(index) = item.id.parse() {
                    progress
                        .failed
                        .push((index, Application::Uncertain, uncertain.error));
                }
            }
        }
    }
    for missing in expected_ids.iter().filter(|id| !seen_ids.contains(*id)) {
        let scope = missing
            .parse::<usize>()
            .ok()
            .and_then(|idx| targets.get(idx))
            .map(|id| ErrorScope::Message {
                id: (id.0.clone()).into(),
            });
        let error = batch_response_missing(
            operation,
            scope,
            format!(
                "Graph $batch returned no response for request {missing}; the item's fate is unknown"
            ),
            replayable.get(missing).copied().unwrap_or(false),
        );
        progress.failed.push((
            missing.parse().expect("submitted numeric id"),
            Application::Uncertain,
            error,
        ));
    }
    progress.failed.sort_by_key(|(index, _, _)| *index);
    progress.committed.sort_by_key(|(index, _)| *index);
    Ok(progress)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::PushMode;

    /// A move whose destination is in a different mailbox than the message
    /// is refused from the ids alone, before the etag GET, as the caller's
    /// malformed request - the same classification the bulk-move path gives
    /// the same condition. It used to be decided after the GET, as a
    /// provider contract violation.
    #[tokio::test]
    async fn a_cross_mailbox_move_is_refused_before_any_request() {
        let client = GraphClient::new("token");
        client.script_rest([]);
        let account = GraphAccount::new_for_tests_with_shared(
            client.clone(),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let destination = crate::account::foreign::encode_foreign("shared@contoso.com", "archive");

        let error = move_messages(
            &account,
            &[ObjectId("AAMkprimary".to_string())],
            &destination.0,
            AccountOperation::AddToContainer,
        )
        .await
        .expect_err("a cross-mailbox move is not expressible");
        assert!(
            matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            ),
            "got {:?}",
            error.kind()
        );
        assert!(
            client.take_rest_requests().is_empty(),
            "the refusal must come before the etag GET"
        );
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use bifrost_net::test_support::canned;
    use bifrost_types::{AccountErrorKind, ProtocolErrorKind, ReconcileAction, RecoveryClass};

    async fn patch_with_replies(
        replies: Vec<bifrost_net::test_support::Canned>,
    ) -> (AccountError, GraphClient) {
        let client = GraphClient::new("token");
        let mut policy = bifrost_net::RetryPolicy::default();
        policy.max_attempts = 2;
        policy.initial_backoff = std::time::Duration::ZERO;
        policy.max_backoff = std::time::Duration::ZERO;
        client.script_wire(replies, policy);
        let account = GraphAccount::new_for_tests_with_shared(
            client.clone(),
            crate::account::PushMode::GraphSubscriptions,
            &[],
        );
        let error = patch_messages(
            &account,
            vec![MessagePatch {
                id: ObjectId("m1".into()),
                etag: GraphETag::from_odata_annotation("v1"),
                body: json!({"categories":["blue"]}),
            }],
            AccountOperation::SetCategory,
        )
        .await
        .unwrap_err();
        let requests = client.rest_requests();
        let body: Value = serde_json::from_slice(requests[0].body.as_ref().unwrap()).unwrap();
        assert_eq!(body["requests"][0]["headers"]["If-Match"], "v1");
        (error, client)
    }
    #[tokio::test]
    async fn typed_writes_chunk_twenty_legs_and_keep_prior_commits() {
        for moving in [false, true] {
            for fail_later in [false, true] {
                let client = GraphClient::new("token");
                let ids: Vec<_> = (0..21).map(|i| ObjectId(format!("m{i}"))).collect();
                let mut replies = Vec::new();
                if moving {
                    for id in &ids {
                        let body = serde_json::to_vec(&json!({
                            "id": id.0, "@odata.etag": "v1"
                        }))
                        .unwrap();
                        replies.push(bifrost_net::test_support::Canned::Response {
                            status: reqwest::StatusCode::OK,
                            headers: reqwest::header::HeaderMap::new(),
                            body: bytes::Bytes::from(body),
                        });
                    }
                }
                let body = serde_json::to_vec(&json!({"responses":
                    (0..20).map(|i| json!({
                        "id": i.to_string(), "status": 200,
                        "body": {"id": format!("new{i}")}
                    })).collect::<Vec<_>>()
                }))
                .unwrap();
                replies.push(bifrost_net::test_support::Canned::Response {
                    status: reqwest::StatusCode::OK,
                    headers: reqwest::header::HeaderMap::new(),
                    body: bytes::Bytes::from(body),
                });
                if fail_later {
                    replies.push(canned(
                        reqwest::StatusCode::FORBIDDEN,
                        br#"{"error":{"code":"ErrorAccessDenied","message":"denied"}}"#,
                    ));
                } else {
                    replies.push(canned(
                        reqwest::StatusCode::OK,
                        br#"{"responses":[{"id":"0","status":200,"body":{"id":"new20"}}]}"#,
                    ));
                }
                client.script_wire(replies, bifrost_net::RetryPolicy::default());
                let account = GraphAccount::new_for_tests_with_shared(
                    client.clone(),
                    crate::account::PushMode::GraphSubscriptions,
                    &[],
                );
                let result = if moving {
                    move_messages(&account, &ids, "archive", AccountOperation::AddToContainer).await
                } else {
                    patch_messages(
                        &account,
                        ids.iter()
                            .map(|id| MessagePatch {
                                id: id.clone(),
                                etag: GraphETag::from_odata_annotation("v1"),
                                body: json!({"isRead": true}),
                            })
                            .collect(),
                        AccountOperation::SetIsRead,
                    )
                    .await
                };
                if fail_later {
                    let error = result.unwrap_err();
                    let record = error.partial_commit().expect("earlier envelope committed");
                    assert_eq!(record.steps().len(), 20);
                    if moving {
                        assert_eq!(
                            record.steps()[0].targets().refs[1].scope,
                            ErrorScope::Message {
                                id: ObjectId("new0".into())
                            }
                        );
                    }
                } else {
                    result.unwrap();
                }
                let requests = client.rest_requests();
                let envelopes: Vec<Value> = requests
                    .iter()
                    .filter_map(|request| request.body.as_ref())
                    .map(|body| serde_json::from_slice(body).unwrap())
                    .collect();
                assert_eq!(envelopes.len(), 2);
                assert_eq!(envelopes[0]["requests"].as_array().unwrap().len(), 20);
                assert_eq!(envelopes[1]["requests"].as_array().unwrap().len(), 1);
                assert_eq!(envelopes[1]["requests"][0]["id"], "0");
                assert!(
                    envelopes[1]["requests"][0]["url"]
                        .as_str()
                        .unwrap()
                        .contains("m20")
                );
                assert_eq!(envelopes[1]["requests"][0]["headers"]["If-Match"], "v1");
            }
        }
    }

    #[tokio::test]
    async fn write_answers_ignore_unsubmitted_ids_and_invalidate_stale_versions() {
        for status in [204, 404, 412] {
            let client = GraphClient::new("token");
            let body = serde_json::to_vec(&json!({"responses": [
                {"id": "99", "status": 204},
                {"id": "00", "status": status},
                {"id": "0", "status": 204}
            ]}))
            .unwrap();
            client.script_wire(
                [bifrost_net::test_support::Canned::Response {
                    status: reqwest::StatusCode::OK,
                    headers: reqwest::header::HeaderMap::new(),
                    body: bytes::Bytes::from(body),
                }],
                bifrost_net::RetryPolicy::default(),
            );
            let account = GraphAccount::new_for_tests_with_shared(
                client,
                crate::account::PushMode::GraphSubscriptions,
                &[],
            );
            account
                .etag_index
                .write()
                .await
                .insert("m1".into(), GraphETag::from_odata_annotation("old"));
            let progress = submit_write_batch_resource(
                &account,
                Resource::Outlook,
                vec![BatchRequestItem {
                    id: "0".into(),
                    method: "PATCH".into(),
                    url: "/me/messages/m1".into(),
                    headers: None,
                    body: Some(json!({"isRead": true})),
                }],
                &[ObjectId("m1".into())],
                crate::account::batch::AbsentMeans::Failure,
                AccountOperation::SetCategory,
            )
            .await
            .expect("batch envelope");
            assert_eq!(progress.committed.len(), usize::from(status == 204));
            assert_eq!(progress.failed.len(), usize::from(status != 204));
            assert!(progress.committed.iter().all(|(index, _)| *index == 0));
            assert!(progress.failed.iter().all(|(index, _, _)| *index == 0));
            assert_eq!(account.etag_index.write().await.get("m1"), None);
        }
    }

    #[tokio::test]
    async fn a_503_before_a_batch_412_reconciles_without_replay() {
        let conflict = || {
            canned(reqwest::StatusCode::OK, br#"{"responses":[{"id":"0","status":412,"body":{"error":{"code":"ErrorIrresolvableConflict","message":"stale etag"}}}]}"#)
        };
        let (error, client) = patch_with_replies(vec![
            canned(reqwest::StatusCode::SERVICE_UNAVAILABLE, b"ambiguous"),
            conflict(),
        ])
        .await;
        assert_eq!(client.rest_requests().len(), 1);
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
        );
        assert!(
            matches!(error.recovery(), RecoveryClass::Reconcile(a) if a.guidance.actions == [ReconcileAction::CheckTarget])
        );
        let (error, client) = patch_with_replies(vec![conflict()]).await;
        assert_eq!(client.rest_requests().len(), 1);
        assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
    }
    #[tokio::test]
    async fn a_missing_conditional_subresponse_reconciles() {
        let (error, _) = patch_with_replies(vec![canned(
            reqwest::StatusCode::OK,
            br#"{"responses":[]}"#,
        )])
        .await;
        assert!(
            matches!(error.recovery(), RecoveryClass::Reconcile(a) if a.guidance.actions == [ReconcileAction::CheckTarget])
        );
    }
}

/// Typed progress of one or more `$batch` submissions, indexed by request
/// position. `committed` holds every 2xx (and destroy-404) sub-request with
/// the new id a successful move returned, owner-qualified for a shared
/// mailbox; `failed` holds every refused, uncertain or missing sub-request
/// with its own leaf and application. Unsent requests appear in neither.
#[derive(Default)]
pub(super) struct BatchProgress {
    pub committed: Vec<(usize, Option<ObjectId>)>,
    pub failed: Vec<(usize, Application, AccountError)>,
}
impl BatchProgress {
    /// Fold a later chunk's progress in, shifting its indexes by the chunk's
    /// start so they address the operation's full target list.
    pub fn append(&mut self, other: Self, offset: usize) {
        self.committed
            .extend(other.committed.into_iter().map(|(i, id)| (i + offset, id)));
        self.failed.extend(
            other
                .failed
                .into_iter()
                .map(|(i, app, e)| (i + offset, app, e)),
        );
    }
}
/// Attach the public operation's record over batch progress, with the effect
/// and replay judgment the caller declares for its own operation. The primary
/// is the first failure in submission order; every other failure is a sibling
/// and every success a committed step. A lone refusal with nothing committed
/// is returned as the plain leaf. A move success without a usable new id is
/// recorded with incomplete targets, which gates.
pub(super) fn batch_progress_error(
    mut progress: BatchProgress,
    targets: &[ObjectId],
    effect: StepEffect,
    replay: StepReplay,
) -> Result<(), AccountError> {
    if progress.failed.is_empty() {
        return Ok(());
    }
    progress.failed.sort_by_key(|(index, _, _)| *index);
    let (primary, application, error) = progress.failed.remove(0);
    if progress.committed.is_empty()
        && progress.failed.is_empty()
        && application != Application::Uncertain
    {
        return Err(error);
    }
    let subject = |index: usize| {
        targets.get(index).map_or_else(StepTargets::unknown, |id| {
            StepTargets::complete(vec![TargetRef::new(
                TargetRole::Subject,
                ErrorScope::Message { id: id.clone() },
            )])
        })
    };
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        effect,
        subject(primary),
        replay,
        application,
    ));
    for (index, new_id) in progress.committed {
        let targets = if effect == StepEffect::Moved {
            let refs = targets
                .get(index)
                .map(|id| {
                    TargetRef::new(TargetRole::Source, ErrorScope::Message { id: id.clone() })
                })
                .into_iter()
                .chain(new_id.as_ref().map(|id| {
                    TargetRef::new(
                        TargetRole::Destination,
                        ErrorScope::Message { id: id.clone() },
                    )
                }))
                .collect();
            if new_id.is_some() {
                StepTargets::complete(refs)
            } else {
                StepTargets::at_least(refs)
            }
        } else {
            subject(index)
        };
        record.committed(effect, targets, replay);
    }
    for (index, application, error) in progress.failed {
        record.failed_sibling(Some(effect), subject(index), replay, application, error);
    }
    Err(finish_partial(error, record))
}

/// A whole `$batch` call failed after earlier chunks committed: the call's
/// leaf is the primary over every id of the failed chunk, and every earlier
/// committed id is a step. With nothing committed the leaf is returned plain.
pub(super) fn batch_call_error(
    progress: BatchProgress,
    all_targets: &[ObjectId],
    chunk_targets: &[ObjectId],
    error: AccountError,
    effect: StepEffect,
    replay: StepReplay,
) -> AccountError {
    if progress.committed.is_empty() {
        return error;
    }
    let subject =
        |id: &ObjectId| TargetRef::new(TargetRole::Subject, ErrorScope::Message { id: id.clone() });
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        effect,
        StepTargets::complete(chunk_targets.iter().map(subject).collect()),
        replay,
        Application::from_leaf(&error),
    ));
    for (index, new_id) in progress.committed {
        let targets = if effect == StepEffect::Moved {
            let refs = all_targets
                .get(index)
                .map(|id| {
                    TargetRef::new(TargetRole::Source, ErrorScope::Message { id: id.clone() })
                })
                .into_iter()
                .chain(new_id.as_ref().map(|id| {
                    TargetRef::new(
                        TargetRole::Destination,
                        ErrorScope::Message { id: id.clone() },
                    )
                }))
                .collect();
            if new_id.is_some() {
                StepTargets::complete(refs)
            } else {
                StepTargets::at_least(refs)
            }
        } else {
            all_targets
                .get(index)
                .map_or_else(StepTargets::unknown, |id| {
                    StepTargets::complete(vec![subject(id)])
                })
        };
        record.committed(effect, targets, replay);
    }
    finish_partial(error, record)
}
