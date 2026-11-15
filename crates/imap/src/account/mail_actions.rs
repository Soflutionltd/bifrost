//! Direct mail mutations and composed thread actions over the shared mutation engine.

use super::containers::folder_from_container;
use super::folder_registry::folder_role;
use super::message_targets::{
    ThreadScope, group_by_folder, is_native_thread, resolve_thread_members, resolved_targets,
};
use super::{DecodedObjectId, ImapAccount, encode_object_id};
use crate::types::{Flag, MailboxName};
use bifrost_types::container::{ContainerId, FolderRole, MutationTarget};
use bifrost_types::hydration::Importance;
use bifrost_types::ids::ThreadId;
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, Application, ErrorScope, PartialCommitBuilder,
    PrimaryLeg, StepEffect, StepReplay, StepTargets, TargetRef, TargetRole,
};

/// One per-folder effect a direct PIM write committed.
pub(super) struct FolderEffect {
    folder: MailboxName,
    uids: super::UidSet,
    uidvalidity: crate::types::UidValidity,
    effect: StepEffect,
    destination: Option<MailboxName>,
}

/// Typed progress of the per-folder helpers (`copy_messages`,
/// `delete_messages`, `set_flag`) and the inner forms of the composed
/// operations. Never a record: each public operation attaches exactly once,
/// through `folder_progress_error`. `stage` is the write leg in flight when
/// `error` happened (`None` for a preflight or local refusal, which is an
/// effectless primary); folders never attempted appear nowhere.
pub(super) struct FolderProgress {
    done: Vec<FolderEffect>,
    error: AccountError,
    stage: Option<StepEffect>,
    targets: StepTargets,
    siblings: Vec<(AccountError, StepTargets, Option<StepEffect>)>,
}

impl From<AccountError> for FolderProgress {
    fn from(error: AccountError) -> Self {
        Self {
            done: Vec::new(),
            error,
            stage: None,
            targets: StepTargets::unknown(),
            siblings: Vec::new(),
        }
    }
}

fn folder_effect_targets(effect: &FolderEffect) -> StepTargets {
    let mut refs: Vec<_> = effect
        .uids
        .identifiers()
        .map(|uid| {
            TargetRef::new(
                if effect.destination.is_some() {
                    TargetRole::Source
                } else {
                    TargetRole::Subject
                },
                ErrorScope::Message {
                    id: encode_object_id(&effect.folder, effect.uidvalidity, uid),
                },
            )
        })
        .collect();
    if let Some(destination) = &effect.destination {
        refs.push(TargetRef::new(
            TargetRole::Destination,
            ErrorScope::Cursor(super::folder_scope(destination)),
        ));
        StepTargets::at_least(refs)
    } else {
        StepTargets::complete(refs)
    }
}

pub(super) fn folder_progress_error(progress: FolderProgress) -> AccountError {
    let FolderProgress {
        done,
        error,
        stage,
        targets,
        siblings,
    } = progress;
    // Nothing committed: only an uncertain write leg is worth a record (it
    // may have applied); a refusal or an effectless preflight failure is the
    // plain leaf.
    if done.is_empty()
        && siblings.iter().all(|(error, _, stage)| {
            stage.is_none() || Application::from_leaf(error) != Application::Uncertain
        })
        && (stage.is_none() || Application::from_leaf(&error) != Application::Uncertain)
    {
        return error;
    }
    let primary = stage.map_or_else(
        || PrimaryLeg::effectless(targets.clone()),
        |effect| {
            PrimaryLeg::new(
                effect,
                targets.clone(),
                folder_replay(effect),
                Application::from_leaf(&error),
            )
        },
    );
    let mut record = PartialCommitBuilder::new(primary);
    for (error, targets, effect) in siblings {
        record.failed_sibling(
            effect,
            targets,
            effect.map_or(StepReplay::Repeatable, folder_replay),
            if effect.is_some() {
                Application::from_leaf(&error)
            } else {
                Application::NotApplicable
            },
            error,
        );
    }
    for effect in done {
        record.committed(
            effect.effect,
            folder_effect_targets(&effect),
            folder_replay(effect.effect),
        );
    }
    super::mutate::finish_partial(error, record)
}

/// Replay judgment for a direct PIM folder effect: a replayed COPY makes a
/// second copy at the destination (`Blocking`); STORE on an expunged UID,
/// UID EXPUNGE of absent UIDs and re-applied flags are no-ops (`Repeatable`).
fn folder_replay(effect: StepEffect) -> StepReplay {
    if effect == StepEffect::Copied {
        StepReplay::Blocking
    } else {
        StepReplay::Repeatable
    }
}

pub(crate) fn add_to_container(
    account: ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        add_to_container_inner(&account, target, container)
            .await
            .map(|_| ())
            .map_err(folder_progress_error)
    })
}

async fn add_to_container_inner(
    account: &ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    if !account.capabilities.pim_methods.add_to_container {
        return Err(super::error::unsupported(AccountOperation::AddToContainer).into());
    }
    let destination = folder_from_container(
        &account.dav_scopes,
        &container,
        AccountOperation::AddToContainer,
    )?;
    // Members already in the destination are not copied into it again.
    let ids = resolved_targets(
        account,
        &target,
        ThreadScope::Except(&destination),
        AccountOperation::AddToContainer,
    )
    .await?;
    if ids.is_empty() && is_native_thread(&target) {
        return Ok(Vec::new());
    }
    copy_messages(account, ids, &destination, AccountOperation::AddToContainer).await
}

pub(crate) fn remove_from_container(
    account: ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        remove_from_container_inner(&account, target, container)
            .await
            .map(|_| ())
            .map_err(folder_progress_error)
    })
}

async fn remove_from_container_inner(
    account: &ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    if !account.capabilities.pim_methods.remove_from_container {
        return Err(super::error::unsupported(AccountOperation::RemoveFromContainer).into());
    }
    let source = folder_from_container(
        &account.dav_scopes,
        &container,
        AccountOperation::RemoveFromContainer,
    )?;
    let ids = resolved_targets(
        account,
        &target,
        ThreadScope::Only(&source),
        AccountOperation::RemoveFromContainer,
    )
    .await?
    .into_iter()
    .filter(|id| id.folder == source)
    .collect::<Vec<_>>();
    if ids.is_empty() {
        // A conversation with no current member in this folder is already
        // out of it, which is the result an enterprise user asked removal to
        // achieve. A snapshot handle naming another folder stays a refusal.
        if is_native_thread(&target) {
            return Ok(Vec::new());
        }
        return Err(super::error::unsupported(AccountOperation::RemoveFromContainer).into());
    }
    delete_messages(account, ids, AccountOperation::RemoveFromContainer).await
}

pub(crate) fn set_keyword(
    account: ImapAccount,
    target: MutationTarget,
    keyword: bifrost_types::Flag,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.set_keyword {
            return Err(super::error::unsupported(AccountOperation::SetKeyword));
        }
        let flag = imap_flag_for_keyword(keyword.as_str());
        let ids = resolved_targets(
            &account,
            &target,
            ThreadScope::Account,
            AccountOperation::SetKeyword,
        )
        .await?;
        set_flag(&account, ids, flag, value, AccountOperation::SetKeyword)
            .await
            .map(|_| ())
            .map_err(folder_progress_error)
    })
}

pub(crate) fn set_is_read(
    account: ImapAccount,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.set_is_read {
            return Err(super::error::unsupported(AccountOperation::SetIsRead));
        }
        let ids = resolved_targets(
            &account,
            &target,
            ThreadScope::Account,
            AccountOperation::SetIsRead,
        )
        .await?;
        set_flag(
            &account,
            ids,
            Flag::Seen,
            is_read,
            AccountOperation::SetIsRead,
        )
        .await
        .map(|_| ())
        .map_err(folder_progress_error)
    })
}

pub(crate) fn set_importance(
    account: ImapAccount,
    target: MutationTarget,
    level: Importance,
) -> AccountFuture<Result<(), AccountError>> {
    // IMAP has no native importance field; map onto the `$important`
    // keyword. Exclusive: `High` sets it, `Normal`/`Low` clear it - one
    // STORE op, never an expand-into-two.
    Box::pin(async move {
        if !account.capabilities.pim_methods.set_importance {
            return Err(super::error::unsupported(AccountOperation::SetImportance));
        }
        let important = importance_sets_important_keyword(level);
        let ids = resolved_targets(
            &account,
            &target,
            ThreadScope::Account,
            AccountOperation::SetImportance,
        )
        .await?;
        set_flag(
            &account,
            ids,
            imap_flag_for_keyword(IMPORTANCE_KEYWORD),
            important,
            AccountOperation::SetImportance,
        )
        .await
        .map(|_| ())
        .map_err(folder_progress_error)
    })
}

pub(super) const IMPORTANCE_KEYWORD: &str = "$important";

/// IMAP's two-valued importance mapping: `High` sets the importance
/// keyword, `Normal`/`Low` clear it.
fn importance_sets_important_keyword(level: Importance) -> bool {
    matches!(level, Importance::High)
}

pub(crate) fn move_thread(
    account: ImapAccount,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        move_thread_inner(&account, thread, target, source)
            .await
            .map(|_| ())
            .map_err(folder_progress_error)
    })
}

async fn move_thread_inner(
    account: &ImapAccount,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    if is_native_thread(&MutationTarget::Thread(thread.clone())) {
        return move_native_thread(account, &thread, target, source).await;
    }
    let mut done =
        add_to_container_inner(account, MutationTarget::Thread(thread.clone()), target).await?;
    if let Some(source) = source {
        match remove_from_container_inner(account, MutationTarget::Thread(thread), source).await {
            Ok(next) => done.extend(next),
            Err(mut failure) => {
                done.append(&mut failure.done);
                failure.done = done;
                return Err(failure);
            }
        }
    }
    Ok(done)
}

/// Move an account-scoped native conversation. The removal set (members in
/// `source`) is resolved BEFORE the copy set (members anywhere but `target`),
/// and both before any write: a message present in the source at the first
/// search is still visible to the second, so nothing is removed from the
/// source that was not also copied, even if a reply arrives mid-move.
async fn move_native_thread(
    account: &ImapAccount,
    thread: &ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    if !account.capabilities.pim_methods.add_to_container {
        return Err(super::error::unsupported(AccountOperation::AddToContainer).into());
    }
    let destination = folder_from_container(
        &account.dav_scopes,
        &target,
        AccountOperation::AddToContainer,
    )?;
    let removal = match source {
        Some(source) => {
            if !account.capabilities.pim_methods.remove_from_container {
                return Err(
                    super::error::unsupported(AccountOperation::RemoveFromContainer).into(),
                );
            }
            let source = folder_from_container(
                &account.dav_scopes,
                &source,
                AccountOperation::RemoveFromContainer,
            )?;
            resolve_thread_members(
                account,
                thread,
                ThreadScope::Only(&source),
                AccountOperation::RemoveFromContainer,
            )
            .await?
        }
        None => Vec::new(),
    };
    let copies = resolve_thread_members(
        account,
        thread,
        ThreadScope::Except(&destination),
        AccountOperation::AddToContainer,
    )
    .await?;
    let mut done = if copies.is_empty() {
        Vec::new()
    } else {
        copy_messages(
            account,
            copies,
            &destination,
            AccountOperation::AddToContainer,
        )
        .await?
    };
    if !removal.is_empty() {
        match delete_messages(account, removal, AccountOperation::RemoveFromContainer).await {
            Ok(next) => done.extend(next),
            Err(mut failure) => {
                done.append(&mut failure.done);
                failure.done = done;
                return Err(failure);
            }
        }
    }
    Ok(done)
}

pub(crate) fn delete_thread(
    account: ImapAccount,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if let Some(current) = current {
            let folder =
                folder_from_container(&account.dav_scopes, &current, AccountOperation::BulkMove)?;
            // Ruling: a folder whose OWN role is Trash deletes in place, not
            // only the account's resolved Trash. An Outlook user emptying an
            // item from anything shown as Deleted Items expects it gone: a
            // second Trash-like personal folder, and a shared mailbox's own
            // Trash, which must delete in that mailbox and never be moved
            // across into the user's personal Trash. The single resolver still
            // picks the move TARGET below; this test only decides in-place.
            let entry = account.folders.get(&folder);
            let attributes = entry
                .as_deref()
                .map(super::folder_registry::FolderEntry::attributes)
                .unwrap_or_default();
            let own_role = folder_role(
                &attributes,
                folder.as_str(),
                entry
                    .as_deref()
                    .and_then(super::folder_registry::FolderEntry::delimiter),
            );
            if own_role == Some(FolderRole::Trash)
                || account.folders.role_folder(FolderRole::Trash).as_ref() == Some(&folder)
            {
                return remove_from_container_inner(
                    &account,
                    MutationTarget::Thread(thread),
                    current,
                )
                .await
                .map(|_| ())
                .map_err(folder_progress_error);
            }
            let trash = account
                .folders
                .role_folder(FolderRole::Trash)
                .ok_or_else(|| super::error::unsupported(AccountOperation::BulkMove))?;
            return move_thread_inner(
                &account,
                thread,
                ContainerId(trash.account_id()),
                Some(current),
            )
            .await
            .map(|_| ())
            .map_err(folder_progress_error);
        }
        let trash = account
            .folders
            .role_folder(FolderRole::Trash)
            .ok_or_else(|| super::error::unsupported(AccountOperation::BulkMove))?;
        move_thread_inner(&account, thread, ContainerId(trash.account_id()), None)
            .await
            .map(|_| ())
            .map_err(folder_progress_error)
    })
}

async fn copy_messages(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    destination: &MailboxName,
    op: AccountOperation,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    result_mutation(
        account,
        ids,
        super::mutate::MutationKind::Copy(destination.clone()),
        StepEffect::Copied,
        Some(destination.clone()),
        op,
    )
    .await
}

pub(super) async fn delete_messages(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    op: AccountOperation,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    result_mutation(
        account,
        ids,
        super::mutate::MutationKind::Destroy,
        StepEffect::Deleted,
        None,
        op,
    )
    .await
}

async fn set_flag(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    flag: Flag,
    value: bool,
    op: AccountOperation,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    let flags = bifrost_types::FlagSet::from([flag.to_string()]);
    let flags = if value {
        bifrost_types::FlagOp::Add(flags)
    } else {
        bifrost_types::FlagOp::Remove(flags)
    };
    result_mutation(
        account,
        ids,
        super::mutate::MutationKind::Flags(flags),
        StepEffect::Updated,
        None,
        op,
    )
    .await
}

/// The engine owns admission and wire truth; this adapter only projects its
/// per-item answers into the PIM Result and the composed operation's record.
/// Outlook users expect a partial change to remain visible even when another
/// member conflicts. Keep every settled answer in the failing window, then
/// stop before attempting another window or folder.
async fn result_mutation(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    kind: super::mutate::MutationKind,
    effect: StepEffect,
    destination: Option<MailboxName>,
    op: AccountOperation,
) -> Result<Vec<FolderEffect>, FolderProgress> {
    use bifrost_types::{ItemOutcome, TransmissionState};

    let mut done = Vec::new();
    for (folder, ids) in group_by_folder(ids) {
        for window in ids.chunks(account.config.mutation_batch_size.max(1)) {
            let (outcomes, marked_deleted) = match super::mutate::result_folder_mutation(
                account,
                &folder,
                window.to_vec(),
                &kind,
                op,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    let mut failure = FolderProgress::from(error);
                    failure.done = done;
                    failure.targets = super::mutate::folder_targets(&folder, TargetRole::Subject);
                    return Err(failure);
                }
            };
            // The destroy engine supplies exactly the subset whose STORE
            // landed but EXPUNGE failed. Never infer it from the error kind.
            for (epoch, uids) in &marked_deleted {
                done.push(FolderEffect {
                    folder: folder.clone(),
                    uids: super::UidSet::from_numbers(uids.iter().copied()),
                    uidvalidity: *epoch,
                    effect: StepEffect::MarkedDeleted,
                    destination: None,
                });
            }
            let mut failure: Option<FolderProgress> = None;
            let mut succeeded = Vec::new();
            for outcome in outcomes {
                let (item, error) = match outcome {
                    ItemOutcome::Succeeded(success) => {
                        succeeded.push(success.item);
                        continue;
                    }
                    ItemOutcome::Failed(failed) => (failed.item, failed.error),
                    ItemOutcome::Uncertain(uncertain) => (uncertain.item, uncertain.error),
                };
                let id = window
                    .iter()
                    .find(|id| id.id.0 == item.0)
                    .expect("engine answers under submitted identity");
                let expunge_failed = marked_deleted
                    .iter()
                    .any(|(epoch, uids)| *epoch == id.uidvalidity && uids.contains(&id.uid.get()));
                let stage = if expunge_failed {
                    Some(StepEffect::Deleted)
                } else if matches!(
                    error.chain().iter().find_map(|cause| match cause {
                        bifrost_types::Cause::Attempt(attempt) => Some(attempt.transmission_state),
                        _ => None,
                    }),
                    Some(TransmissionState::Acknowledged | TransmissionState::InFlight)
                ) || Application::from_leaf(&error) == Application::Uncertain
                {
                    Some(if effect == StepEffect::Deleted {
                        StepEffect::MarkedDeleted
                    } else {
                        effect
                    })
                } else {
                    None
                };
                let targets = folder_effect_targets(&FolderEffect {
                    folder: folder.clone(),
                    uids: super::UidSet::from_uids([id.uid]),
                    uidvalidity: id.uidvalidity,
                    effect,
                    destination: destination.clone(),
                });
                if let Some(failure) = &mut failure {
                    failure.siblings.push((error, targets, stage));
                } else {
                    failure = Some(FolderProgress {
                        done: Vec::new(),
                        error,
                        stage,
                        targets,
                        siblings: Vec::new(),
                    });
                }
            }
            // Preserve the selected epoch and aggregate one committed effect
            // per epoch/window, including successes beside a MODIFIED refusal.
            let mut epochs: std::collections::BTreeMap<crate::types::UidValidity, Vec<u32>> =
                Default::default();
            for item in succeeded {
                let id = window
                    .iter()
                    .find(|id| id.id.0 == item.0)
                    .expect("engine answers under submitted identity");
                epochs.entry(id.uidvalidity).or_default().push(id.uid.get());
            }
            for (uidvalidity, uids) in epochs {
                done.push(FolderEffect {
                    folder: folder.clone(),
                    uids: super::UidSet::from_numbers(uids),
                    uidvalidity,
                    effect,
                    destination: destination.clone(),
                });
            }
            if let Some(mut failure) = failure {
                failure.done = done;
                return Err(failure);
            }
        }
    }
    Ok(done)
}

fn imap_flag_for_keyword(keyword: &str) -> Flag {
    Flag::from_neutral_keyword(keyword)
}

#[cfg(test)]
mod tests;
