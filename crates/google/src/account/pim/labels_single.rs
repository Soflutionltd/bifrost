//! Gmail labels single operations.

use super::super::flags::{LABEL_TRASH, LABEL_UNREAD, is_archive_id};
use super::super::{error, flags};
use super::{account_error_for, unsupported};
use crate::client::GmailClient;
use bifrost_types::{AccountError, AccountFuture, ContainerId, MutationTarget, ThreadId};
use std::sync::Arc;

pub(crate) fn add_to_container(
    client: Arc<GmailClient>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = add_container_patch(&container);
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::AddToContainer,
        )
        .await
    })
}

pub(crate) fn remove_from_container(
    client: Arc<GmailClient>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = remove_container_patch(&container);
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::RemoveFromContainer,
        )
        .await
    })
}

pub(crate) fn set_label_membership(
    client: Arc<GmailClient>,
    target: MutationTarget,
    label: ContainerId,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = if value {
            add_container_patch(&label)
        } else {
            remove_container_patch(&label)
        };
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::SetLabelMembership,
        )
        .await
    })
}

pub(crate) fn set_is_read(
    client: Arc<GmailClient>,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let add = if is_read {
            Vec::new()
        } else {
            vec![LABEL_UNREAD.to_string()]
        };
        let remove = if is_read {
            vec![LABEL_UNREAD.to_string()]
        } else {
            Vec::new()
        };
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::SetIsRead,
        )
        .await
    })
}

pub(crate) fn move_thread(
    client: Arc<GmailClient>,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // One `threads.modify`: the add and every removal (the exclusive
        // display containers plus the caller's source label) ride the
        // same request, so there is no window where the thread sits in
        // both containers.
        let (add, remove) = move_container_patch(&target, source.as_ref());
        modify_target(
            &client,
            MutationTarget::Thread(thread),
            add,
            remove,
            bifrost_types::AccountOperation::MoveThread,
        )
        .await
    })
}

pub(crate) fn delete_thread(
    client: Arc<GmailClient>,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if current
            .as_ref()
            .is_some_and(|id| id.0.eq_ignore_ascii_case(LABEL_TRASH))
        {
            let operation = bifrost_types::AccountOperation::DeleteThread;
            return match client.delete_thread(&thread.0).await {
                Ok(()) => Ok(()),
                Err(error) => {
                    let error = account_error_for(
                        error,
                        mutation_context(&MutationTarget::Thread(thread.clone()), operation),
                    );
                    // DELETE has no label operand: a missing thread is the subject.
                    // Outlook users expect it gone without claiming a new write.
                    if operation.skips_absent_target()
                        && matches!(
                            error.kind(),
                            bifrost_types::AccountErrorKind::NotFound(
                                bifrost_types::ResourceKind::Thread
                            )
                        )
                    {
                        Ok(())
                    } else {
                        Err(error)
                    }
                }
            };
        }
        let (add, remove) = move_container_patch(&ContainerId(LABEL_TRASH.to_string()), None);
        modify_target(
            &client,
            MutationTarget::Thread(thread),
            add,
            remove,
            bifrost_types::AccountOperation::DeleteThread,
        )
        .await
    })
}

fn mutation_context(
    target: &MutationTarget,
    operation: bifrost_types::AccountOperation,
) -> error::GmailErrorContext {
    let mut ctx = match target {
        MutationTarget::Message(id) => error::GmailErrorContext::hydrate_message(id.0.clone()),
        MutationTarget::Thread(id) => error::GmailErrorContext::hydrate_thread(id.0.clone()),
        _ => error::GmailErrorContext::base(operation),
    };
    ctx.operation = operation;
    ctx
}

async fn modify_target(
    client: &GmailClient,
    target: MutationTarget,
    add_labels: Vec<String>,
    remove_labels: Vec<String>,
    operation: bifrost_types::AccountOperation,
) -> Result<(), AccountError> {
    // MutationTarget currently exposes only Message and Thread, so no caller
    // can reproduce an unsupported-kind no-op today. Keep admission ahead of
    // the empty patch for future variants: users expect unsupported targets
    // to fail even when removing the synthetic Archive membership does no work.
    if !matches!(
        &target,
        MutationTarget::Message(_) | MutationTarget::Thread(_)
    ) {
        return Err(unsupported(operation));
    }
    if add_labels.is_empty() && remove_labels.is_empty() {
        return Ok(());
    }
    // `skips_absent_target` requires confirmed subject absence. A modify 404
    // can name a label operand instead, even for one target. Keep the error:
    // Outlook users must see a missing destination rather than a false success.
    let ctx = mutation_context(&target, operation);
    match target {
        MutationTarget::Message(id) => {
            client
                .modify_message(&id.0, &add_labels, &remove_labels)
                .await
                .map_err(|e| account_error_for(e, ctx))?;
        }
        MutationTarget::Thread(id) => {
            client
                .modify_thread(&id.0, &add_labels, &remove_labels)
                .await
                .map_err(|e| account_error_for(e, ctx))?;
        }
        _ => return Err(unsupported(operation)),
    }
    Ok(())
}

fn add_container_patch(container: &ContainerId) -> (Vec<String>, Vec<String>) {
    if is_archive_id(&container.0) {
        // Archive is not a label - it is the absence of every exclusive
        // display container - so "add to archive" is only expressible
        // as a relocation.
        return move_container_patch(container, None);
    }
    (vec![container.0.clone()], Vec::new())
}

/// Single-object relocation, sharing
/// [`flags::move_placement_patch`] with the bulk `batchModify` driver
/// so a consumer gets the same wire semantics whichever entry point it
/// reached: the exclusive display containers the message is leaving are
/// stripped, and the synthetic `archive` id never reaches
/// `addLabelIds`.
///
/// `source` adds the user label being filed out of, in the SAME modify
/// call - the destination alone cannot imply it.
fn move_container_patch(
    target: &ContainerId,
    source: Option<&ContainerId>,
) -> (Vec<String>, Vec<String>) {
    let patch = flags::move_placement_patch(&target.0, source.map(|id| id.0.as_str()));
    (patch.add_label_ids, patch.remove_label_ids)
}

fn remove_container_patch(container: &ContainerId) -> (Vec<String>, Vec<String>) {
    if is_archive_id(&container.0) {
        (Vec::new(), Vec::new())
    } else {
        (Vec::new(), vec![container.0.clone()])
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::flags::{ARCHIVE_ID, LABEL_INBOX, LABEL_SPAM};
    use super::*;

    #[test]
    fn mutation_context_preserves_target_and_operation() {
        for operation in [
            bifrost_types::AccountOperation::MoveThread,
            bifrost_types::AccountOperation::DeleteThread,
        ] {
            let ctx = super::mutation_context(
                &bifrost_types::MutationTarget::Thread(bifrost_types::ThreadId("thread".into())),
                operation,
            );
            assert_eq!(ctx.operation, operation);
            assert_eq!(
                ctx.scope,
                Some(bifrost_types::ErrorScope::Thread {
                    id: "thread".into()
                })
            );
            assert!(matches!(
                ctx.resource,
                Some(super::error::GmailResource::Thread)
            ));
        }
        let ctx = super::mutation_context(
            &bifrost_types::MutationTarget::Message(bifrost_types::ObjectId("message".into())),
            bifrost_types::AccountOperation::AddToContainer,
        );
        assert_eq!(
            ctx.scope,
            Some(bifrost_types::ErrorScope::Message {
                id: "message".into()
            })
        );
    }

    #[test]
    fn adding_archive_is_a_relocation_not_a_label() {
        let (add, remove) = add_container_patch(&ContainerId(ARCHIVE_ID.to_string()));
        assert!(add.is_empty());
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ]
        );
    }

    #[test]
    fn adding_a_user_label_does_not_relocate() {
        let (add, remove) = add_container_patch(&ContainerId("Label_42".to_string()));
        assert_eq!(add, vec!["Label_42".to_string()]);
        assert!(
            remove.is_empty(),
            "add_to_container is additive; only a move strips the inbox"
        );
    }

    #[test]
    fn single_object_move_matches_the_bulk_rule() {
        let (add, remove) = move_container_patch(&ContainerId("Label_42".to_string()), None);
        assert_eq!(add, vec!["Label_42".to_string()]);
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ],
            "the single-object and bulk builders must not disagree"
        );
    }

    #[test]
    fn single_object_move_folds_the_source_into_one_request() {
        let (_, remove) = move_container_patch(
            &ContainerId("Label_42".to_string()),
            Some(&ContainerId("Label_7".to_string())),
        );
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string(),
                "Label_7".to_string()
            ]
        );
    }

    #[test]
    fn single_object_move_to_archive_adds_no_label() {
        let (add, remove) = move_container_patch(&ContainerId(ARCHIVE_ID.to_string()), None);
        assert!(add.is_empty());
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ]
        );
    }
}
