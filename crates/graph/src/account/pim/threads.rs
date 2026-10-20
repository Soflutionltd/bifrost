//! Thread doors: hydration, move, and delete, each routed to the
//! mailbox that owns the thread.

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use bifrost_types::{
    AccountError, AccountOperation, ContainerId, ErrorScope, HydrationProjection, MutationTarget,
    ThreadHydration, ThreadId,
};
use serde_json::Value;

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

/// Delete a thread: move its members to Trash, or destroy them outright
/// when they are already there.
///
/// The WHOLE operation is owner-scoped, not just the member lookup.
/// `resolve_target_ids` returns owner-qualified message ids, and
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
    let ids = resolve_target_ids(
        &account,
        MutationTarget::Thread(thread),
        AccountOperation::BulkMove,
    )
    .await?;
    let trash = trash_container_id(&account, owner.as_deref(), scope).await?;
    let already_in_trash = current
        .as_ref()
        .is_some_and(|id| container_is_trash(id, &trash, owner.as_deref()));
    if already_in_trash {
        // Use the permanent-delete helper here too: changing only bulk_destroy
        // leaves this separate PIM batch path performing a soft delete.
        destroy_messages(&account, &ids, AccountOperation::BulkDestroy).await
    } else {
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
