//! Single-target and thread mail mutations with partial progress.

use super::containers::role_mailbox;
use super::{MailAccount, to_acct_err};
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailGet, EmailId, EmailSet, Property as EmailProperty};
use crate::mailbox::MailboxId;
use crate::sync::error::finish_partial;
use crate::thread::{ThreadGet, ThreadId as JmapThreadId};
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, Application, ContainerId, ErrorScope,
    FolderRole, Importance, MutationTarget, ObjectId, PartialCommitBuilder, PrimaryLeg, StepEffect,
    StepReplay, StepTargets, TargetRef, TargetRole, ThreadId,
};
use std::collections::HashSet;

const SEEN_KEYWORD: &str = "$seen";
const IMPORTANT_KEYWORD: &str = "$important";
pub(crate) fn add_to_container<T: HttpTransport>(
    mail: MailAccount<T>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            target,
            container,
            true,
            AccountOperation::AddToContainer,
        )
        .await
    })
}

pub(crate) fn remove_from_container<T: HttpTransport>(
    mail: MailAccount<T>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            target,
            container,
            false,
            AccountOperation::RemoveFromContainer,
        )
        .await
    })
}

pub(crate) fn set_keyword<T: HttpTransport>(
    mail: MailAccount<T>,
    target: MutationTarget,
    keyword: bifrost_types::Flag,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let keyword = crate::email::keyword::Keyword::parse(keyword.as_str())
            .map_err(to_acct_err(AccountOperation::SetKeyword))?;
        // IDs resolved on the primary fallback remain literal. Qualifying
        // them with a departed or primary declaration would duplicate the
        // prefix and give an Outlook user an unroutable reconciliation target.
        let owner = mail.owner().shared().map(str::to_owned);
        let ids = resolve_target(&mail, target, AccountOperation::SetKeyword).await?;
        let batch_size = crate::sync::capabilities::set_batch_size(mail.limits.max_objects_in_set);
        let mut progress = SetProgress::default().with_owner(owner);
        for chunk in ids.chunks(batch_size) {
            let keyword_for_set = keyword.clone();
            let ids_for_set = chunk.to_vec();
            let mut response =
                match send_email_set(&mail, AccountOperation::SetKeyword, move || {
                    let mut set = EmailSet::new();
                    for id in &ids_for_set {
                        set.update(id.clone()).keyword(&keyword_for_set, value);
                    }
                    set
                })
                .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        progress.call_failure(chunk, error);
                        break;
                    }
                };
            for id in chunk {
                progress.item(id, response.updated(id), AccountOperation::SetKeyword);
            }
            if progress.is_failed() {
                break;
            }
        }
        progress.finish(StepEffect::Updated)
    })
}

pub(crate) fn set_is_read<T: HttpTransport>(
    mail: MailAccount<T>,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    set_keyword(mail, target, SEEN_KEYWORD.into(), is_read)
}

/// Exclusive importance overwrite via the `$important` keyword. JMAP's
/// model is two-valued: `High` sets `$important`, `Normal`/`Low` clear
/// it. One `Email/set` keyword update, no expand-into-two.
pub(crate) fn set_importance<T: HttpTransport>(
    mail: MailAccount<T>,
    target: MutationTarget,
    level: Importance,
) -> AccountFuture<Result<(), AccountError>> {
    set_keyword(
        mail,
        target,
        IMPORTANT_KEYWORD.into(),
        importance_sets_important_keyword(level),
    )
}

/// JMAP's two-valued importance mapping: `High` sets `$important`,
/// `Normal`/`Low` clear it.
pub(super) fn importance_sets_important_keyword(level: Importance) -> bool {
    matches!(level, Importance::High)
}

#[derive(Default)]
pub(super) struct SetProgress {
    pub(super) owner: Option<String>,
    pub(super) committed: Vec<EmailId>,
    failed: Vec<FailedSet>,
    pub(super) after_chunk: Option<AccountError>,
}
struct FailedSet {
    pub(super) ids: Vec<EmailId>,
    pub(super) application: Application,
    pub(super) error: AccountError,
}
impl SetProgress {
    pub(super) fn with_owner(mut self, owner: Option<String>) -> Self {
        self.owner = owner;
        self
    }
    pub(super) fn is_failed(&self) -> bool {
        !self.failed.is_empty() || self.after_chunk.is_some()
    }
    pub(super) fn append(&mut self, other: Self) {
        self.committed.extend(other.committed);
        self.failed.extend(other.failed);
        self.after_chunk = other.after_chunk;
    }
    pub(super) fn call_failure(&mut self, ids: &[EmailId], error: AccountError) {
        self.failed.push(FailedSet {
            ids: ids.to_vec(),
            application: Application::from_leaf(&error),
            error,
        });
    }
    /// Record one id's `Email/set` answer with its request-stamped guard.
    /// An unanswered id is `Uncertain` explicitly: for an
    /// idempotent unguarded op its PartialResponse leaf advises a same-request
    /// retry, which `from_leaf` would read as `NotApplied`.
    pub(super) fn item(
        &mut self,
        id: &EmailId,
        result: crate::Result<impl Sized>,
        op: AccountOperation,
    ) {
        let missing = matches!(&result, Err(crate::Error::IdUnanswered { .. }));
        let outcome = crate::sync::mail_set::classify_set_response(
            result.map(|_| ()),
            ObjectId(self.owner.as_deref().map_or_else(
                || id.as_str().to_owned(),
                |owner| crate::sync::foreign::encode_object(owner, id.as_str()),
            )),
            op,
        );
        match outcome {
            bifrost_types::ItemOutcome::Succeeded(success) => {
                if matches!(success.output, bifrost_types::MutationSuccess::Applied) {
                    self.committed.push(id.clone());
                }
            }
            bifrost_types::ItemOutcome::Failed(failure) => {
                let application = if missing {
                    Application::Uncertain
                } else {
                    Application::from_leaf(&failure.error)
                };
                self.failed.push(FailedSet {
                    ids: vec![id.clone()],
                    application,
                    error: failure.error,
                });
            }
            // The mail-set classifier `mail_set::classify_set_response` never produces
            // this lane today. Should they start to, the honest record is
            // an uncertain application, never a panic in a user's write.
            bifrost_types::ItemOutcome::Uncertain(uncertain) => {
                self.failed.push(FailedSet {
                    ids: vec![id.clone()],
                    application: Application::Uncertain,
                    error: uncertain.error,
                });
            }
        }
    }

    pub(super) fn finish(mut self, effect: StepEffect) -> Result<(), AccountError> {
        // Progress is collected in wire ids, but an Outlook shared-mailbox user
        // can correlate only the owner-qualified identities returned by reads.
        let owner = self.owner.as_deref();
        let targets = |ids: &[EmailId]| {
            StepTargets::complete(
                ids.iter()
                    .map(|id| {
                        TargetRef::new(
                            TargetRole::Subject,
                            ErrorScope::Message {
                                id: ObjectId(owner.map_or_else(
                                    || id.as_str().to_owned(),
                                    |owner| crate::sync::foreign::encode_object(owner, id.as_str()),
                                )),
                            },
                        )
                    })
                    .collect(),
            )
        };
        let (error, primary) = if let Some(error) = self.after_chunk {
            (error, PrimaryLeg::effectless(StepTargets::unknown()))
        } else if self.failed.is_empty() {
            return Ok(());
        } else {
            let failure = self.failed.remove(0);
            if self.failed.is_empty()
                && self.committed.is_empty()
                && failure.application != Application::Uncertain
            {
                return Err(failure.error);
            }
            let primary = PrimaryLeg::new(
                effect,
                targets(&failure.ids),
                StepReplay::Repeatable,
                failure.application,
            );
            (failure.error, primary)
        };
        let mut record = PartialCommitBuilder::new(primary);
        for id in self.committed {
            record.committed(effect, targets(&[id]), StepReplay::Repeatable);
        }
        for failure in self.failed {
            record.failed_sibling(
                Some(effect),
                targets(&failure.ids),
                StepReplay::Repeatable,
                failure.application,
                failure.error,
            );
        }
        Err(finish_partial(error, record))
    }
}

pub(crate) fn move_thread<T: HttpTransport>(
    mail: MailAccount<T>,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mutation_target = MutationTarget::Thread(thread);
        cross_account_container(&mutation_target, &target, AccountOperation::BulkMove)?;
        if let Some(source) = source.as_ref() {
            cross_account_container(
                &mutation_target,
                source,
                AccountOperation::RemoveFromContainer,
            )?;
        }
        // Resolved ONCE, before either leg. Re-resolving between the legs
        // let a message delivered to the thread in between be removed from
        // the source having never been added to the target.
        let ids = resolve_target(&mail, mutation_target, AccountOperation::BulkMove).await?;
        let mut progress =
            patch_mailbox_membership_of(&mail, &ids, target, true, AccountOperation::BulkMove)
                .await?;
        if progress.is_failed() {
            return progress.finish(StepEffect::Updated);
        }
        if let Some(source) = source {
            progress.append(
                patch_mailbox_membership_of(
                    &mail,
                    &ids,
                    source,
                    false,
                    AccountOperation::RemoveFromContainer,
                )
                .await?,
            );
        }
        progress.finish(StepEffect::Updated)
    })
}

pub(crate) fn delete_thread<T: HttpTransport>(
    mail: MailAccount<T>,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let owner = mail.owner().shared().map(str::to_owned);
        let trash_mailbox =
            role_mailbox(&mail, FolderRole::Trash, AccountOperation::BulkDestroy).await?;
        // `role_mailbox` ran against the account the thread id routed to,
        // so it answered a NATIVE mailbox id in that account. Every other
        // container id crossing this boundary - the caller's `current`, the
        // ids `containers_list` mints - is owner-qualified for a foreign
        // account, and `cross_account_container` compares the two owners
        // before anything is sent. Mint the trash id in the thread's own
        // namespace so the comparison sees one account, not a bare id that
        // reads as primary.
        let trash = match crate::sync::foreign::JmapRef::parse(&thread.0)
            .owner
            .shared()
        {
            Some(owner) => {
                ContainerId(crate::sync::foreign::encode_foreign(owner, trash_mailbox.as_str()).0)
            }
            None => ContainerId(trash_mailbox.as_str().to_string()),
        };
        // Mailbox ids are opaque and case-sensitive. An Outlook user deleting
        // from another folder expects a move to Trash, never an expunge merely
        // because that folder's id differs from Trash only in case.
        let already_in_trash = current.as_ref().is_some_and(|id| id == &trash);
        if already_in_trash {
            let ids = resolve_target(
                &mail,
                MutationTarget::Thread(thread),
                AccountOperation::BulkDestroy,
            )
            .await?;
            // `current` identifies the container through which the caller
            // found this thread; it does not prove that every Email in the
            // conversation belongs to Trash. Read the member-level
            // mailboxIds and expunge only the emails actually present in
            // this account's resolved Trash mailbox. The destroy is also
            // guarded by that read's Email state, so a concurrent membership
            // change cannot turn the selected ids stale before the set. Email
            // state is account-wide, so an unrelated Email change may also
            // refuse the set; a retry re-reads membership before destroying.
            let (ids, read_state) = resolve_thread_emails_in_mailbox(
                &mail,
                &ids,
                &trash_mailbox,
                AccountOperation::BulkDestroy,
            )
            .await?;
            destroy_thread_emails_if_unchanged(
                &mail,
                owner,
                ids,
                read_state,
                AccountOperation::BulkDestroy,
            )
            .await?
            .finish(StepEffect::Deleted)
        } else {
            let mutation_target = MutationTarget::Thread(thread);
            cross_account_container(&mutation_target, &trash, AccountOperation::BulkMove)?;
            if let Some(source) = current.as_ref() {
                cross_account_container(
                    &mutation_target,
                    source,
                    AccountOperation::RemoveFromContainer,
                )?;
            }
            // See `move_thread`: resolved once, both legs over that set.
            let ids = resolve_target(&mail, mutation_target, AccountOperation::BulkMove).await?;
            let mut progress =
                patch_mailbox_membership_of(&mail, &ids, trash, true, AccountOperation::BulkMove)
                    .await?;
            if progress.is_failed() {
                return progress.finish(StepEffect::Updated);
            }
            if let Some(source) = current {
                progress.append(
                    patch_mailbox_membership_of(
                        &mail,
                        &ids,
                        source,
                        false,
                        AccountOperation::RemoveFromContainer,
                    )
                    .await?,
                );
            }
            progress.finish(StepEffect::Updated)
        }
    })
}

async fn resolve_thread_emails_in_mailbox<T: HttpTransport>(
    mail: &MailAccount<T>,
    ids: &[EmailId],
    mailbox: &MailboxId,
    op: AccountOperation,
) -> Result<(Vec<EmailId>, Option<crate::core::id::ObjectState<Email>>), AccountError> {
    if ids.is_empty() {
        return Ok((Vec::new(), None));
    }

    // A conversation can exceed maxObjectsInGet; keep every Email/get
    // within the account's advertised per-call bound.
    let batch_size = mail.limits.max_objects_in_get.max(1);
    let mut in_mailbox = Vec::new();
    let mut selected = HashSet::new();
    let mut read_state = None;
    for chunk in ids.chunks(batch_size) {
        let response = mail
            .call(
                EmailGet::new()
                    .ids(chunk.iter().cloned())
                    .properties([EmailProperty::Id, EmailProperty::MailboxIds]),
            )
            .await
            .map_err(to_acct_err(op))?;
        if read_state.is_none() {
            read_state = Some(response.state().clone());
        }

        let requested = chunk.iter().cloned().collect::<HashSet<_>>();
        for email in response.list() {
            let Some(id) = email.id() else {
                continue;
            };
            if !requested.contains(id) {
                continue;
            }
            if email
                .mailbox_ids()
                .iter()
                .any(|member| member.as_str() == mailbox.as_str())
                && selected.insert(id.clone())
            {
                in_mailbox.push(id.clone());
            }
        }

        // A notFound answer means the thread member disappeared between the
        // expansion and this read, so there is no extant Email to destroy. An
        // id in neither response lane makes membership unknowable; fail closed
        // before sending any permanent-delete request.
        for id in chunk {
            let answer = response.answer(id, |email| email.id() == Some(id));
            match answer {
                crate::core::answer::IdAnswer::Ok(_)
                | crate::core::answer::IdAnswer::NotFound(_) => {}
                other => {
                    other
                        .into_result(id.as_str(), response.answer_context)
                        .map_err(crate::sync::error::answer_error(op))?;
                }
            }
        }
    }

    Ok((in_mailbox, read_state))
}

pub(super) async fn destroy_thread_emails_if_unchanged<T: HttpTransport>(
    mail: &MailAccount<T>,
    owner: Option<String>,
    ids: Vec<EmailId>,
    read_state: Option<crate::core::id::ObjectState<Email>>,
    op: AccountOperation,
) -> Result<SetProgress, AccountError> {
    if ids.is_empty() {
        return Ok(SetProgress::default().with_owner(owner));
    }
    let read_state = read_state
        .ok_or_else(|| to_acct_err(op)(crate::Error::MissingResponseField("Email/get.state")))?;
    let batch_size = crate::sync::capabilities::set_batch_size(mail.limits.max_objects_in_set);
    let mut if_in_state = read_state;
    // The owner must be known before the first item is classified: each
    // failed id's own error scope is minted in `item`, not at `finish`.
    let mut progress = SetProgress::default().with_owner(owner);
    for (index, chunk) in ids.chunks(batch_size).enumerate() {
        let set = EmailSet::new().destroy(chunk.iter().cloned());
        let mut set = Some(set);
        let mut response = match crate::sync::mail_set::execute(
            mail,
            crate::sync::mail_set::Guard::Exact(&if_in_state),
            || Ok(set.take().expect("exact guarded set is sent once")),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                progress.call_failure(chunk, to_acct_err(op)(error));
                break;
            }
        };
        let new_state = response
            .new_state()
            .filter(|state| !state.is_empty())
            .cloned();
        for id in chunk {
            progress.item(id, response.destroyed(id), op);
        }
        if progress.is_failed() {
            break;
        }
        if index + 1 < ids.len().div_ceil(batch_size) {
            let Some(new_state) = new_state else {
                let error =
                    to_acct_err(op)(crate::Error::MissingResponseField("Email/set.newState"));
                progress.after_chunk = Some(error);
                break;
            };
            if_in_state = new_state;
        }
    }
    Ok(progress)
}

async fn patch_mailbox_membership<T: HttpTransport>(
    mail: &MailAccount<T>,
    target: MutationTarget,
    container: ContainerId,
    value: bool,
    op: AccountOperation,
) -> Result<(), AccountError> {
    cross_account_container(&target, &container, op)?;
    let ids = resolve_target(mail, target, op).await?;
    patch_mailbox_membership_of(mail, &ids, container, value, op)
        .await?
        .finish(StepEffect::Updated)
}

/// The membership patch over an ALREADY RESOLVED set of message ids.
///
/// A thread move is two legs (add to the target, remove from the source)
/// and each leg used to run its own `Thread/get`. A message delivered to
/// the thread between the two resolves was therefore removed from the
/// source without ever having been added to the target: it lost the
/// source membership and gained nothing. The two-leg non-atomicity is the
/// documented cross-provider shape and is not fixable here, but both legs
/// operate on one FIXED id set, resolved only once. Each leg then splits that
/// set at the server's `maxObjectsInSet` limit.
///
/// The caller performs the cross-account container check for each
/// container it passes; the ids themselves are already in this account's
/// namespace.
pub(super) async fn patch_mailbox_membership_of<T: HttpTransport>(
    mail: &MailAccount<T>,
    ids: &[EmailId],
    container: ContainerId,
    value: bool,
    op: AccountOperation,
) -> Result<SetProgress, AccountError> {
    if ids.is_empty() {
        return Ok(SetProgress::default());
    }
    // The owner check has established that the container names the
    // same account as the target, so the only work left is stripping the
    // qualification the selected account does not use.
    let mailbox = MailboxId::new(mail.wire(&container.0).to_owned());
    let batch_size = crate::sync::capabilities::set_batch_size(mail.limits.max_objects_in_set);
    let mut progress = SetProgress::default().with_owner(mail.owner().shared().map(str::to_owned));
    for chunk in ids.chunks(batch_size) {
        let ids_for_set = chunk.to_vec();
        let mailbox_for_set = mailbox.clone();
        let mut response = match send_email_set(mail, op, move || {
            let mut set = EmailSet::new();
            for id in &ids_for_set {
                set.update(id.clone()).mailbox_id(&mailbox_for_set, value);
            }
            set
        })
        .await
        {
            Ok(response) => response,
            Err(error) => {
                progress.call_failure(chunk, error);
                break;
            }
        };
        for id in chunk {
            progress.item(id, response.updated(id), op);
        }
        if progress.is_failed() {
            break;
        }
    }
    Ok(progress)
}

async fn resolve_target<T: HttpTransport>(
    mail: &MailAccount<T>,
    target: MutationTarget,
    op: AccountOperation,
) -> Result<Vec<EmailId>, AccountError> {
    match target {
        MutationTarget::Message(id) => Ok(vec![EmailId::new(mail.wire(&id.0).to_owned())]),
        MutationTarget::Thread(thread) => {
            // Same rule as the `Message` arm above: the account layer has
            // already selected `mail` from the thread id's owner, so strip
            // the qualification only when it names this exact account. An
            // unreachable foreign thread id stays literal and the server
            // reports the honest miss instead of a same-id primary thread.
            let mut response = mail
                .call(
                    ThreadGet::new()
                        .ids([JmapThreadId::new(mail.wire(&thread.0).to_owned())])
                        .properties([crate::thread::Property::EmailIds]),
                )
                .await
                .map_err(to_acct_err(op))?;
            let thread = response.pop().ok_or_else(|| {
                // The operation is the MUTATION being performed, not
                // `HydrateThread`: the thread lookup here only expands a
                // mutation target, and mislabelling it sends the caller a
                // hydration failure for an operation it never issued.
                crate::sync::error::into_account_error(
                    crate::Error::IdNotFound(thread.0.clone()),
                    crate::sync::error::JmapErrorContext::new(op).with_scope(
                        bifrost_types::ErrorScope::Thread {
                            id: (thread.0.clone()).into(),
                        },
                    ),
                )
            })?;
            thread
                .email_ids()
                .ok_or_else(|| {
                    to_acct_err(op)(crate::Error::MissingResponseField("Thread/get.emailIds"))
                })
                .map(ToOwned::to_owned)
        }
        _ => Err(crate::sync::error::unsupported_error(
            op,
            None,
            "JMAP does not support this mutation target",
        )),
    }
}

/// Refuse a container membership patch whose object and whose container
/// belong to different JMAP accounts.
///
/// The account layer routes the `Email/set` by the TARGET's owner but takes
/// the container id as given, and the server resolves both operands inside
/// that one `accountId`. So a bare (primary) container id addressed to a
/// foreign account does not fail: it names whatever mailbox that account
/// holds under the same id, and the message is filed somewhere the caller
/// never asked for with no error reported. Compare owners before anything is
/// sent. A thread target declares its owner the same way a message does -
/// a foreign thread id is owner-qualified in the object namespace, a bare
/// one is primary - so the two arms are symmetric; other target shapes keep
/// `resolve_target`'s `Unsupported` classification and are left alone here.
///
/// `Request(Malformed)` matches bifrost-graph's cross-mailbox `bulk_move`
/// rejection: the caller asked for something one endpoint cannot express.
pub(super) fn cross_account_container(
    target: &MutationTarget,
    container: &ContainerId,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let (target_id, target_owner) = match target {
        MutationTarget::Message(id) => (
            id.0.as_str(),
            crate::sync::foreign::JmapRef::parse(&id.0).owner.shared(),
        ),
        MutationTarget::Thread(thread) => (
            thread.0.as_str(),
            crate::sync::foreign::JmapRef::parse(&thread.0)
                .owner
                .shared(),
        ),
        _ => return Ok(()),
    };
    let container_owner = crate::sync::foreign::JmapRef::parse(&container.0)
        .owner
        .shared();
    if crate::sync::foreign::JmapRef::parse(target_id)
        .same_owner(crate::sync::foreign::JmapRef::parse(&container.0))
    {
        return Ok(());
    }
    Err(crate::sync::error::cross_account_destination(
        op,
        target_id,
        target_owner,
        &container.0,
        container_owner,
    ))
}

pub(super) async fn destroy_emails<T: HttpTransport>(
    mail: &MailAccount<T>,
    ids: impl IntoIterator<Item = EmailId> + Clone + Send + 'static,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let ids_vec = ids.into_iter().collect::<Vec<_>>();
    if ids_vec.is_empty() {
        return Ok(());
    }
    let ids_for_set = ids_vec.clone();
    let mut response = send_email_set(mail, op, move || {
        EmailSet::new().destroy(ids_for_set.clone())
    })
    .await?;
    for id in &ids_vec {
        match crate::sync::mail_set::classify_set_response(
            response.destroyed(id),
            ObjectId(id.as_str().to_owned()),
            op,
        ) {
            bifrost_types::ItemOutcome::Succeeded(_) => {}
            bifrost_types::ItemOutcome::Failed(failure) => return Err(failure.error),
            // Not produced today; never report an uncertain destroy as done.
            bifrost_types::ItemOutcome::Uncertain(uncertain) => return Err(uncertain.error),
        }
    }
    Ok(())
}

pub(super) async fn send_email_set<T: HttpTransport, F>(
    mail: &MailAccount<T>,
    op: AccountOperation,
    make_set: F,
) -> Result<crate::core::set::SetResponse<Email>, AccountError>
where
    F: FnOnce() -> EmailSet,
{
    let mut set = Some(make_set());
    crate::sync::mail_set::execute(mail, crate::sync::mail_set::Guard::Unconditional, || {
        Ok(set.take().expect("unconditional set is sent once"))
    })
    .await
    .map_err(to_acct_err(op))
}
