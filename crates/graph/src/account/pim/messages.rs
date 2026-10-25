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
use crate::types::{BatchRequest, BatchRequestItem, BatchResponse, ODataCollection};
use bifrost_types::{
    AccountError, AccountOperation, ContainerId, ErrorScope, Importance, MutationTarget, ObjectId,
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

use super::common::*;
use super::hydrate::*;
use super::threads::*;

pub(super) const STARRED_CATEGORY: &str = "$flagged";
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
    submit_write_batch(
        account,
        account
            .batch_resource(ids)
            .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))?,
        requests,
        true,
        operation,
    )
    .await
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
    category.eq_ignore_ascii_case(STARRED_CATEGORY)
        || category.eq_ignore_ascii_case("\\flagged")
        || category.eq_ignore_ascii_case("flagged")
        || category.eq_ignore_ascii_case("starred")
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
    pub(super) etag: String,
}

pub(super) async fn resolve_target_ids(
    account: &GraphAccount,
    target: MutationTarget,
    operation: AccountOperation,
) -> Result<Vec<ObjectId>, AccountError> {
    match target {
        MutationTarget::Message(id) => Ok(vec![id]),
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

pub(super) async fn resolve_target_values(
    account: &GraphAccount,
    target: MutationTarget,
    select: &str,
    operation: AccountOperation,
) -> Result<Vec<ResolvedMessage>, AccountError> {
    match target {
        MutationTarget::Message(id) => {
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
    // Decode the (possibly foreign-encoded) id so a shared-mailbox
    // message reads from `/users/{owner}/messages/{native}`; a primary
    // (bare) id stays on `/me`. This makes the typed `message_hydrate`
    // and every pim read-modify-write that starts from `fetch_message_value`
    // mailbox-correct.
    let parsed = crate::account::foreign::parse_message_id(id);
    let client = account.client_for_owner(parsed.owner()).map_err(|error| {
        into_account_error(
            error,
            GraphErrorContext::graph(AccountOperation::Hydrate).with_scope(ErrorScope::Message {
                id: id.0.clone().into(),
            }),
        )
    })?;
    let path = format!(
        "{}/messages/{}?{}",
        client.api_path_prefix(),
        bifrost_net::url::encode_path_component(parsed.native_id()),
        select_query(select)
    );
    let value = client
        .get_json(Resource::Outlook, &path)
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::Hydrate)))?;
    // Cache the etag under the encoded id (the key the mutation paths
    // look up), preserving the owner so the conditioned write routes
    // back to the same mailbox.
    cache_etag_for(account, id, &value).await;
    Ok(value)
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
    let mut requests = Vec::new();
    let mut targets = Vec::new();
    for (index, patch) in patches.iter().enumerate() {
        let mut headers = HashMap::new();
        headers.insert("If-Match".to_string(), patch.etag.clone());
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "PATCH".to_string(),
            url: message_batch_url(account, &patch.id, "", operation)?,
            body: Some(patch.body.clone()),
            headers: Some(headers),
        });
        targets.push(patch.id.clone());
    }
    submit_write_batch_with_targets(account, requests, &targets, false, operation).await
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
    let dest =
        crate::account::foreign::parse_folder(&bifrost_types::FolderId(destination.to_string()));
    // A cross-mailbox move is not expressible against one endpoint. Decided
    // from the ids alone, so it is refused BEFORE the etag GET - a local
    // refusal of the caller's request, `Request(Malformed)`, exactly as the
    // bulk-move path in `mutate.rs` refuses the same condition.
    for id in ids {
        let source_owner = crate::account::foreign::parse_message_id(id)
            .owner()
            .map(str::to_string);
        if dest.foreign().map(|f| f.mailbox.as_str()) != source_owner.as_deref() {
            return Err(crate::account::graph_error::invalid_item_error(
                operation,
                ErrorScope::Message {
                    id: id.0.clone().into(),
                },
                format!(
                    "Graph move for {} targets a folder in a different mailbox than the message",
                    id.0
                ),
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
    for (index, (id, value)) in ids.iter().zip(values.iter()).enumerate() {
        let etag = graph_etag(value).ok_or_else(|| {
            pim_protocol_error(
                operation,
                Some(ErrorScope::Message {
                    id: id.0.clone().into(),
                }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        let mut headers = HashMap::new();
        headers.insert("If-Match".to_string(), etag);
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "POST".to_string(),
            url: message_batch_url(account, id, "/move", operation)?,
            body: Some(json!({ "destinationId": dest.native_id() })),
            headers: Some(headers),
        });
        targets.push(id.clone());
    }
    submit_write_batch_with_targets(account, requests, &targets, false, operation).await
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

pub(super) async fn submit_write_batch(
    account: &GraphAccount,
    resource: Resource,
    requests: Vec<BatchRequestItem>,
    destroy: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    submit_write_batch_resource(account, resource, requests, &[], destroy, operation).await
}

/// Variant of `submit_write_batch` that threads per-request target
/// message ids so per-item failures carry `ErrorScope::Message { id }`
/// rather than the coarser `ErrorScope::Account`. `targets[i]` is the
/// `ObjectId` of the request whose `BatchRequestItem::id == i.to_string()`;
/// callers without per-request ids use `submit_write_batch` with explicit
/// ownership instead.
pub(super) async fn submit_write_batch_with_targets(
    account: &GraphAccount,
    requests: Vec<BatchRequestItem>,
    targets: &[ObjectId],
    destroy: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let resource = account
        .batch_resource(targets)
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))?;
    submit_write_batch_resource(account, resource, requests, targets, destroy, operation).await
}

async fn submit_write_batch_resource(
    account: &GraphAccount,
    resource: Resource,
    requests: Vec<BatchRequestItem>,
    targets: &[ObjectId],
    destroy: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if requests.is_empty() {
        return Ok(());
    }
    let ctx = GraphErrorContext::graph(operation);
    // Track every submitted request id (`BatchRequestItem::id`, assigned
    // `index.to_string()` 0..N-1 by the builders) so we can reconcile the
    // returned responses against them: Graph occasionally returns fewer
    // `$batch` responses than requests, and a missing id means that
    // message was never patched. Silently returning `Ok(())` would
    // violate the batch accounting contract (every id accounted for
    // exactly once), so an unaccounted id surfaces as an error.
    // Kept as an ordered `Vec`, not a set: the sweep below reports ONE
    // missing id and stamps its message on the error scope, so drawing it
    // from a `HashSet` made the message an operator saw for a reproducible
    // failure differ between runs. Submission order is the only stable
    // choice, and a linear scan over at most 20 ids is free.
    let replayable: std::collections::HashMap<_, _> = requests
        .iter()
        .map(|r| {
            (
                r.id.clone(),
                super::super::graph_error::subrequest_replayable(r),
            )
        })
        .collect();
    let expected_ids: Vec<String> = requests.iter().map(|r| r.id.clone()).collect();
    let mut seen_ids: HashSet<String> = HashSet::new();
    let mut first_error: Option<AccountError> = None;
    let response: BatchResponse = account
        .client
        .post_batch(
            resource,
            &BatchRequest { requests },
            bifrost_net::ReplayPolicy::Never,
        )
        .await
        .map_err(|e| into_account_error(e, ctx.clone()))?;
    for item in response.responses {
        seen_ids.insert(item.id.clone());
        // Reuse the per-item outcome projector so 4xx/5xx items
        // build structured `AccountError`s with `Protocol::Graph`,
        // `AttemptCause(Acknowledged)`, `WireCause::Graph(signal)`
        // (when an envelope is present), and `Retry-After` -> throttle
        // scope translation. Per-item 2xx and destroy-404 short-
        // circuit as `Succeeded`; everything else surfaces as
        // `Failed(_)` carrying the classified error which we then
        // unwrap into the function's single-error return contract.
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
        let outcome = mutation_item_outcome(
            item.status,
            headers,
            body,
            destroy,
            bifrost_types::BatchItemId(item.id.clone()),
            operation,
            scope,
            target.as_ref().and_then(|id| account.budget_for_object(id)),
        );
        match outcome {
            bifrost_types::ItemOutcome::Succeeded(_) => {
                if destroy && let Some(id) = target {
                    account.etag_index.write().await.remove(&id.0);
                }
            }
            bifrost_types::ItemOutcome::Failed(failure) => {
                if first_error.is_none() {
                    first_error = Some(failure.error);
                }
            }
            bifrost_types::ItemOutcome::Uncertain(uncertain) => {
                if first_error.is_none() {
                    first_error = Some(uncertain.error);
                }
            }
        }
    }
    // The loop DRAINS rather than returning on the first bad item, even
    // though this surface answers once. `$batch` response order is Graph's,
    // not the caller's, so returning early left the etag of a message that
    // demonstrably WAS destroyed in the cache whenever its subresponse
    // happened to sort after a failing sibling's - and a later conditioned
    // write on that id then sent an `If-Match` for a message that no longer
    // exists. Draining costs one pass over an already-decoded response and
    // changes nothing the caller sees: the error returned is still the
    // first failure in response order.
    if let Some(error) = first_error {
        return Err(error);
    }
    // Any submitted id with no corresponding response is ambiguous. The
    // outer request was acknowledged (a 200 for the `$batch` envelope),
    // which is no evidence either way about the omitted subrequest, so
    // this must not report a clean `Ok(())` and must not assert the item
    // was left alone. `Protocol(PartialResponse)` carries that ambiguity:
    // idempotent operations retry, non-idempotent ones reconcile against
    // the target. These direct write methods bypass the engine's bulk
    // mutation funnel, so this is the only classification their callers
    // see - the same rule the bulk path applies via its uncertain lane.
    if let Some(missing) = expected_ids.iter().find(|id| !seen_ids.contains(*id)) {
        let scope = missing
            .parse::<usize>()
            .ok()
            .and_then(|idx| targets.get(idx))
            .map(|id| ErrorScope::Message {
                id: (id.0.clone()).into(),
            });
        return Err(batch_response_missing(
            operation,
            scope,
            format!(
                "Graph $batch returned no response for request {missing}; the item's fate is unknown"
            ),
            replayable.get(missing).copied().unwrap_or(false),
        ));
    }
    Ok(())
}

/// Cache the message etag under the caller-supplied (possibly
/// foreign-encoded) id - the same key the mutation paths look up - so a
/// conditioned write on a shared-mailbox message finds its etag and
/// routes back to the owning mailbox. Keying by the native id from the
/// response body instead would lose the owner and orphan the cache entry.
pub(super) async fn cache_etag_for(account: &GraphAccount, id: &ObjectId, value: &Value) {
    let Some(etag) = graph_etag(value) else {
        return;
    };
    let mut cache = account.etag_index.write().await;
    cache.insert(id.0.clone(), etag);
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
                etag: "v1".into(),
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
