//! Thread doors: hydration, move, and delete, each routed to the
//! mailbox that owns the thread.

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::account::inventory::graph_etag;
use crate::types::BatchRequestItem;
use bifrost_types::{
    AccountError, AccountOperation, ContainerId, ErrorScope, HydrationProjection, MutationTarget,
    ThreadHydration, ThreadId,
};
use serde_json::Value;
use std::collections::HashMap;

use super::containers::*;
use super::hydrate::*;
use super::messages::*;
use super::search::*;

pub(crate) async fn thread_hydrate(
    account: GraphAccount,
    thread: ThreadId,
) -> Result<ThreadHydration, AccountError> {
    let values = message_values_for_thread(&account, &thread, hydrate_select(true)).await?;
    let owner = crate::account::foreign::parse_thread_id(&thread)
        .owner()
        .map(str::to_string);
    let mut messages = Vec::new();
    for value in values {
        messages.push(message_from_value(
            &value,
            HydrationProjection::Full,
            owner.as_deref(),
        )?);
    }
    messages.sort_by_key(|message| message.date);
    Ok(ThreadHydration {
        id: thread,
        messages,
    })
}

pub(crate) async fn move_thread(
    account: GraphAccount,
    thread: ThreadId,
    target: ContainerId,
) -> Result<(), AccountError> {
    add_to_container(account, MutationTarget::Thread(thread), target).await
}

/// Delete a thread: move its members to Trash, or destroy only the members
/// actually in Trash when the caller says the thread is already there.
///
/// The WHOLE operation is owner-scoped, not just the member lookup.
/// `resolve_target_values` keeps each member's routing id owner-qualified, and
/// `move_messages` refuses a destination whose owner differs from the
/// message's, so resolving Trash against the primary mailbox would have
/// failed the move outright for a shared thread - and before that guard
/// existed it named a folder id from a mailbox the messages do not live
/// in. The `already_in_trash` short-circuit is compared in the same
/// namespace for the same reason.
pub(crate) async fn delete_thread(
    account: GraphAccount,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> Result<(), AccountError> {
    let owner = crate::account::foreign::parse_thread_id(&thread)
        .owner()
        .map(str::to_string);
    let scope = ErrorScope::Thread {
        id: (thread.0.clone()).into(),
    };
    let members = resolve_target_values(
        &account,
        MutationTarget::Thread(thread),
        "id,parentFolderId,changeKey",
        AccountOperation::BulkMove,
    )
    .await?;
    let trash = trash_container_id(&account, owner.as_deref(), scope.clone()).await?;
    let already_in_trash = current
        .as_ref()
        .is_some_and(|id| container_is_trash(id, &trash, owner.as_deref()));
    if already_in_trash {
        // `current` describes the container the caller used to find the
        // thread, not every message's folder. A conversation can include
        // Sent or Inbox copies, so permanent deletion must be narrowed to
        // members whose own parentFolderId is this mailbox's resolved Trash.
        // Compare the concrete ids exactly: the well-known-name fallback in
        // `container_is_trash` is appropriate for caller input, not for
        // deciding whether a server-returned message is safe to destroy.
        let mut destroy = Vec::new();
        for member in members {
            let parent = member
                .value
                .get("parentFolderId")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    super::common::pim_protocol_error(
                        AccountOperation::BulkDestroy,
                        Some(scope.clone()),
                        "Graph conversation message did not include parentFolderId",
                    )
                })?;
            if crate::account::foreign::qualify_with_owner(owner.as_deref(), parent) == trash.0 {
                let etag = graph_etag(&member.value).ok_or_else(|| {
                    super::common::pim_protocol_error(
                        AccountOperation::BulkDestroy,
                        Some(scope.clone()),
                        "Graph conversation message did not include an entity tag",
                    )
                })?;
                destroy.push((member.routing_id, etag));
            }
        }

        if destroy.is_empty() {
            return Ok(());
        }
        // Bind permanentDelete to the same parentFolderId/etag snapshot used
        // to choose these members. A second ETag read could otherwise authorize
        // deletion after a member moved out of Trash.
        let batch_size = account.capabilities.batching_policy.max_items.max(1);
        for chunk in destroy.chunks(batch_size) {
            let mut requests = Vec::with_capacity(chunk.len());
            let mut targets = Vec::with_capacity(chunk.len());
            for (index, (id, etag)) in chunk.iter().enumerate() {
                requests.push(BatchRequestItem {
                    id: index.to_string(),
                    method: "POST".to_string(),
                    url: message_batch_url(
                        &account,
                        id,
                        "/permanentDelete",
                        AccountOperation::BulkDestroy,
                    )?,
                    body: None,
                    headers: Some(HashMap::from([("If-Match".to_string(), etag.clone())])),
                });
                targets.push(id.clone());
            }
            submit_write_batch_with_targets(
                &account,
                requests,
                &targets,
                true,
                AccountOperation::BulkDestroy,
            )
            .await?;
        }
        Ok(())
    } else {
        let ids = members
            .into_iter()
            .map(|member| member.routing_id)
            .collect::<Vec<_>>();
        move_messages(&account, &ids, &trash.0, AccountOperation::BulkMove).await
    }
}

pub(super) async fn message_values_for_thread(
    account: &GraphAccount,
    thread: &ThreadId,
    select: &str,
) -> Result<Vec<Value>, AccountError> {
    let parsed = crate::account::foreign::parse_thread_id(thread);
    let client = account.client_for_owner(parsed.owner()).map_err(|error| {
        into_account_error(
            error,
            GraphErrorContext::graph(AccountOperation::Hydrate).with_scope(ErrorScope::Thread {
                id: (thread.0.clone()).into(),
            }),
        )
    })?;
    let filter = format!("conversationId eq {}", odata_quoted(parsed.native_id()));
    let path = format!(
        "{}/messages?{}&$filter={}&$top=50",
        client.api_path_prefix(),
        select_query(select),
        bifrost_net::url::encode_query_value(&filter)
    );
    fetch_paged_values(client, path).await
}
