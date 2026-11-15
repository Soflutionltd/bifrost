//! Stage 1 PIM actions for the IMAP account.
//!
//! Audit boundary: the 2026-09-04 bug hunt read the driver, wire framing,
//! pool, push/IDLE, auth, change strategies, mutations, inventory,
//! hydration, blob, cursor envelope and folder registry line by line. It did
//! NOT line-audit this module's long tail beyond the container-membership,
//! keyword and search entry points, so an absence of findings here is an
//! absence of reading, not evidence of correctness. Noted so a later auditor
//! knows where coverage stops.

use super::error::{
    expunged_message, or_folder_restriction_error, pim_malformed, restamp, search_cursor_voided,
    stamp_imap_protocol,
};

use bifrost_types::{
    Application, ErrorScope, PartialCommitBuilder, PrimaryLeg, StepEffect, StepReplay, StepTargets,
    TargetRef, TargetRole,
};
use std::collections::HashMap;
use std::time::SystemTime;

use bifrost_types::cloud::HostedAttachment;
use bifrost_types::compose::{
    Address, AttachmentHandle, DraftDiscardFailure, DraftHandle, DraftPatch, IdentityId,
    RecipientNotAccepted, SendReceipt, SendRequest, SentCopy, SentCopyNotFiled,
};
use bifrost_types::container::{
    Container, ContainerId, ContainerKind, ContainerList, ContainerNamespace, ContainerRights,
    FolderRole, MutationTarget, Provenance,
};
use bifrost_types::hydration::{
    AttachmentSource, HydrationProjection, Importance, Message, MessageAttachment, ThreadHydration,
};
use bifrost_types::ids::{ObjectId, ThreadId};
use bifrost_types::mime::{Defect, MimeLimits, SelectOptions, parse_message, select_body_with};
use bifrost_types::page::Page;
use bifrost_types::search::{SearchFilter, SearchRequest};
use bifrost_types::settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, DiagnosticText, LabelId, Protocol, ProtocolKind,
};
#[cfg(test)]
use bifrost_types::{AccountErrorKind, Cause, RequestCause, RequestErrorKind};
use jiff::Timestamp;
use jiff::tz::Offset;

use crate::types::{
    FetchAttr, Flag, MailboxAttribute, MailboxName, MailboxRights, SearchCriteria, StatusItem,
    ThreadNode,
};

use super::targets::UidOperand;
use super::{
    DecodedObjectId, ImapAccount, account_error_with, decode_object_id, decode_thread_id,
    encode_object_id, encode_thread_id, factory, selected_uidvalidity,
};
use crate::connection::ImapConnection;
use crate::error::Error;

/// Build a `map_err` closure that stamps every internal IMAP `Error` with
/// the operation of the calling public method. Threading operation
/// per call site is what lets the central recovery mapping distinguish
/// `Reconcile` vs `Retry::SameRequest` on non-idempotent operations.
fn op_err(op: AccountOperation) -> impl Fn(Error) -> AccountError + Copy {
    move |e| account_error_with(e, super::error::ImapErrorContext::operation(op))
}

fn folder_err<'a>(
    account: &'a ImapAccount,
    folder: &'a MailboxName,
    op: AccountOperation,
) -> impl Fn(Error) -> AccountError + Copy + 'a {
    move |error| account.folder_error(error, folder, super::error::ImapErrorContext::operation(op))
}

/// Folder-aware classification for a failure in the PREFLIGHT of a per-folder step - the
/// checkout, the SELECT/EXAMINE, the UIDVALIDITY read - restamped `Unsent`
/// relative to the operation, the rule `connection::append`'s
/// `unsent_preflight` states for APPEND. The preflight command carries its own
/// transmission evidence (a tagged `NO` to SELECT is `Acknowledged`), but that
/// is about the SELECT, not about the COPY / STORE / FETCH the operation is
/// made of, which was never sent. Publishing the SELECT's evidence would make
/// recovery reconcile a non-idempotent mutation that never reached the server.
///
/// This speaks only for the folder being prepared. Earlier folders' committed
/// effects remain in FolderProgress and are attached by the public operation;
/// an unsent preparation does not erase them.
fn folder_preflight_err<'a>(
    account: &'a ImapAccount,
    folder: &'a MailboxName,
    op: AccountOperation,
) -> impl Fn(Error) -> AccountError + Copy + 'a {
    move |error| {
        folder_err(account, folder, op)(
            error.with_attempt(bifrost_types::TransmissionState::Unsent),
        )
    }
}

/// Memory budget for the one-shot full-message `BODY[]` fetch in
/// `draft_send`. A saved draft is the account's own outgoing message, so
/// this need only bound a single message body, not a folder sweep. 64 MiB
/// comfortably covers any realistic draft (large inline attachments
/// included) while keeping the `FetchLimit` guard engaged, so a corrupt or
/// adversarial server cannot make us buffer an unbounded literal.
const DRAFT_FETCH_BUDGET: usize = 64 * 1024 * 1024;

/// A preview fetch needs enough source to reach past MIME framing in ordinary mail.
pub(crate) const PREVIEW_FETCH_BYTES: u64 = 64 * 1024;

/// One per-folder effect a direct PIM write committed.
struct FolderEffect {
    folder: MailboxName,
    uids: Vec<u32>,
    uidvalidity: u32,
    effect: StepEffect,
    destination: Option<MailboxName>,
}
/// Typed progress of the per-folder helpers (`copy_messages`,
/// `delete_messages`, `set_flag`) and the inner forms of the composed
/// operations. Never a record: each public operation attaches exactly once,
/// through `folder_progress_error`. `stage` is the write leg in flight when
/// `error` happened (`None` for a preflight or local refusal, which is an
/// effectless primary); folders never attempted appear nowhere.
struct FolderProgress {
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
        .iter()
        .map(|uid| {
            TargetRef::new(
                if effect.destination.is_some() {
                    TargetRole::Source
                } else {
                    TargetRole::Subject
                },
                ErrorScope::Message {
                    id: encode_object_id(&effect.folder, effect.uidvalidity, *uid),
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
fn folder_progress_error(progress: FolderProgress) -> AccountError {
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

const IMPORTANCE_KEYWORD: &str = "$important";

/// IMAP's two-valued importance mapping: `High` sets the importance
/// keyword, `Normal`/`Low` clear it.
fn importance_sets_important_keyword(level: Importance) -> bool {
    matches!(level, Importance::High)
}

/// IMAP/SMTP does not do Graph-style shared-mailbox send routing; a
/// `send_as` request is rejected `Unsupported(Send)` rather than
/// silently sent from the authenticated user's own mailbox.
///
/// A shared-mailbox send over SMTP already works without `send_as`: the
/// consumer sets `request.from` to the shared address and the relay's Send-As
/// policy authorizes it. `Unsupported` is therefore a deliberate answer, not a
/// gap. Translating `send_as` into a `from` override is the change to consider
/// if a consumer wants the uniform surface to carry shared-mailbox send for
/// IMAP-shaped accounts too.
fn send_as_guard(request: &SendRequest) -> Option<AccountError> {
    request
        .send_as
        .is_some()
        .then(|| super::error::unsupported(AccountOperation::Send))
}

pub(crate) fn unsupported_unit(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_object(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_attachment(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_hosted(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<HostedAttachment, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn draft_create(
    account: ImapAccount,
    patch: DraftPatch,
) -> AccountFuture<Result<DraftHandle, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.draft_create {
            return Err(super::error::unsupported(AccountOperation::DraftCreate));
        }
        let folder = role_folder(&account, FolderRole::Drafts)
            .ok_or_else(|| super::error::unsupported(AccountOperation::DraftCreate))?;
        let raw = draft_patch_to_rfc5322(&patch)?;
        let err = op_err(AccountOperation::DraftCreate);
        let conn = account.pool.checkout_any().await.map_err(err)?;
        // The rendered draft is owned: `append_message` carries it to the
        // socket without the copy `append(&[u8])` would make.
        let mut message = crate::types::AppendMessage::new(raw);
        message.flags = vec![Flag::Draft];
        let appended = conn
            .connection()
            .append_message(&folder, message, account.command_timeout())
            .await
            .map_err(err)?;
        let Some((uidvalidity, uid)) = appended else {
            return Err(super::error::unsupported(AccountOperation::DraftCreate));
        };
        Ok(DraftHandle(encode_object_id(&folder, uidvalidity, uid).0))
    })
}

pub(crate) fn send_message(
    account: ImapAccount,
    request: SendRequest,
) -> AccountFuture<Result<SendReceipt, AccountError>> {
    send_message_with_render(account, request, bifrost_types::send_request_to_rfc5322)
}

/// `send_message` with the renderer injected. The shared renderer mints the
/// `Date` and `Message-ID` from the wall clock, so a scripted SMTP peer that
/// matches DATA byte for byte cannot know the body in advance; a test passes
/// a closure that calls the real renderer and records what it produced. The
/// closure runs at the render site, after the gates, exactly as production
/// renders.
pub(super) fn send_message_with_render<R>(
    account: ImapAccount,
    request: SendRequest,
    render: R,
) -> AccountFuture<Result<SendReceipt, AccountError>>
where
    R: FnOnce(
            &SendRequest,
            &bifrost_types::Address,
        ) -> Result<bifrost_types::RenderedMessage, AccountError>
        + Send
        + 'static,
{
    Box::pin(async move {
        let _send_gate = account.send_gate.read().await;
        if account.shutdown.is_cancelled() {
            return Err(bifrost_types::AccountError::account_closed(
                AccountOperation::Send,
                bifrost_types::Protocol::Imap,
            ));
        }
        // Graph-style shared-mailbox send routing is not modeled over
        // SMTP (a shared-mailbox send is `request.from` + relay
        // authorization). Reject a `send_as` before the submission check
        // so the contract stays uniform with the other providers.
        if let Some(err) = send_as_guard(&request) {
            return Err(err);
        }
        let Some(submission) = account.submission.clone() else {
            return Err(super::error::unsupported(AccountOperation::Send));
        };

        // The shared assembler is provider-neutral and leaves the protocol
        // unknown; attribute it to IMAP here so a "no recipient" /
        // "uploaded attachments" rejection carries the right protocol.
        let rendered = render(&request, submission.default_from()).map_err(stamp_imap_protocol)?;
        // Scheduled send rides on SMTP FUTURERELEASE (HOLDUNTIL). The
        // relay's EHLO at send time is authoritative: an unsupporting
        // relay surfaces an `Unsupported(Send)` AccountError from the
        // smtp boundary (kind set there, not here). Re-stamp only the
        // operation/protocol; `restamp` cannot change the kind.
        let not_accepted = submission
            .send_rfc5322(&rendered.envelope, &rendered.raw, request.scheduled)
            .await
            .map_err(|err| restamp(err, AccountOperation::Send))?;
        let not_accepted = restamp_recipients(not_accepted, AccountOperation::Send);

        // The message is committed. Optionally append to Sent; a failed
        // APPEND is non-fatal (never resend) but is not swallowed: the
        // receipt's `sent_copy` reports it, and the returned id falls back
        // to the generated Message-ID.
        let save = request
            .save_to_sent
            .unwrap_or_else(|| submission.save_to_sent_default());
        // The Sent copy retains the `Bcc:` header (the sender's record of
        // who was blind copied); only the transmitted body strips it. The
        // assembler hands back a distinct Bcc-bearing body when a Bcc is
        // present, else the two are identical and we reuse `raw`.
        let sent_body = rendered.sent_copy.unwrap_or(rendered.raw);
        Ok(
            append_to_sent_or_fallback(&account, sent_body.into(), save, &rendered.message_id)
                .await
                .with_recipients_not_accepted(not_accepted),
        )
    })
}

pub(crate) fn send_raw_message(
    account: ImapAccount,
    raw: bytes::Bytes,
    save_to_sent: Option<bool>,
) -> AccountFuture<Result<SendReceipt, AccountError>> {
    Box::pin(async move {
        let _send_gate = account.send_gate.read().await;
        if account.shutdown.is_cancelled() {
            return Err(bifrost_types::AccountError::account_closed(
                AccountOperation::Send,
                bifrost_types::Protocol::Imap,
            ));
        }
        let Some(submission) = account.submission.clone() else {
            return Err(super::error::unsupported(AccountOperation::Send));
        };

        // The caller pre-assembled the RFC 5322 / RFC 8098 octets. Parse
        // the envelope (From/Sender drive MAIL FROM; To/Cc/Bcc drive RCPT
        // TO) out of the MIME headers and strip the Bcc header from the
        // transmitted body, exactly as draft_send does for a saved draft.
        let parsed =
            parse_draft_for_submission(&raw, submission.default_from(), AccountOperation::Send)?;
        let not_accepted = submission
            .send_rfc5322(&parsed.envelope, &parsed.body, None)
            .await
            .map_err(|err| restamp(err, AccountOperation::Send))?;
        let not_accepted = restamp_recipients(not_accepted, AccountOperation::Send);

        // Committed. Optionally append to Sent. The Sent copy keeps the
        // verbatim caller bytes (Bcc header retained, the sender's record);
        // only `parsed.body` strips Bcc for the wire.
        let save = save_to_sent.unwrap_or_else(|| submission.save_to_sent_default());
        Ok(
            append_to_sent_or_fallback(&account, raw, save, &parsed.message_id)
                .await
                .with_recipients_not_accepted(not_accepted),
        )
    })
}

pub(crate) fn draft_send(
    account: ImapAccount,
    draft: DraftHandle,
) -> AccountFuture<Result<SendReceipt, AccountError>> {
    draft_send_with_clock(account, draft, jiff::Timestamp::now)
}

/// `draft_send` with the send-time clock injected, so a scripted test can
/// match the restamped `Date` byte for byte. The clock is a callable, not a
/// timestamp, so it is still read at the restamp site after the draft FETCH,
/// exactly as production reads it.
pub(super) fn draft_send_with_clock(
    account: ImapAccount,
    draft: DraftHandle,
    now: fn() -> jiff::Timestamp,
) -> AccountFuture<Result<SendReceipt, AccountError>> {
    Box::pin(async move {
        let _send_gate = account.send_gate.read().await;
        if account.shutdown.is_cancelled() {
            return Err(bifrost_types::AccountError::account_closed(
                AccountOperation::DraftSend,
                bifrost_types::Protocol::Imap,
            ));
        }
        let Some(submission) = account.submission.clone() else {
            return Err(super::error::unsupported(AccountOperation::DraftSend));
        };

        let decoded = decode_object_id(&ObjectId(draft.0.clone()), AccountOperation::DraftSend)?;
        let raw = fetch_full_message(&account, &decoded).await?;
        // Finalize: the saved `Date` is the save time; the message is ready
        // for delivery now. Both the transmitted body and the Sent copy
        // below derive from these restamped bytes, so they agree.
        let raw: bytes::Bytes =
            restamp_draft_date(&raw, now(), AccountOperation::DraftSend)?.into();

        // Parse the draft headers to build the envelope: To/Cc/Bcc drive
        // RCPT TO, From/Sender drive MAIL FROM. The Bcc header is folded
        // into recipients and stripped from the transmitted body so blind
        // recipients are delivered but never disclosed.
        let parsed = parse_draft_for_submission(
            &raw,
            submission.default_from(),
            AccountOperation::DraftSend,
        )?;
        let not_accepted = submission
            .send_rfc5322(&parsed.envelope, &parsed.body, None)
            .await
            .map_err(|err| restamp(err, AccountOperation::DraftSend))?;
        let not_accepted = restamp_recipients(not_accepted, AccountOperation::DraftSend);

        // Sent; discard the draft from Drafts and optionally append to
        // Sent. The Sent copy is the saved draft `raw`, which retains the
        // `Bcc:` header (the sender's record of who was blind copied) -
        // only `parsed.body` (the transmitted bytes) strips it.
        let receipt = append_to_sent_or_fallback(
            &account,
            raw,
            submission.save_to_sent_default(),
            &parsed.message_id,
        )
        .await
        .with_recipients_not_accepted(not_accepted);

        // Discard the original draft. The draft is discarded even when the
        // Sent copy above was not filed: a submitted message left in Drafts,
        // whose ordinary action is Send, invites a duplicate from any client.
        // A failed discard is not an `Err` (the message was sent); it rides
        // the receipt as a `DraftDiscard` error, attributing the failure to
        // the cleanup rather than the send, with its partial-commit record
        // (marked deleted, expunge failed). Its recovery class follows the
        // actual error; the follow-up is `draft_discard`, never a re-driven
        // send.
        //
        // Deliberately not gated on `pim_methods.draft_discard`: this is the
        // same `delete_messages` removal `draft_discard` runs, and that flag
        // is advertised on every server for the reason given in
        // `build_capabilities`. Without UIDPLUS the removal falls back to plain
        // EXPUNGE only when `UID SEARCH DELETED` finds no `\Deleted` message
        // but this draft, and otherwise refuses with the draft left flagged;
        // it never knowingly expunges another message. The non-atomic
        // SEARCH-to-EXPUNGE window is the one `expunge_uids_or_fall_back`
        // documents for every removal path. Skipping the discard instead would
        // leave a submitted message live in Drafts, inviting a duplicate send.
        match delete_messages(&account, vec![decoded], AccountOperation::DraftDiscard).await {
            Ok(_) => Ok(receipt),
            Err(progress) => {
                let error = folder_progress_error(progress);
                tracing::warn!(
                    target: "bifrost_imap::draft_send",
                    error = %error,
                    "draft sent but discard from Drafts failed; the draft may linger"
                );
                Ok(receipt.with_draft_discard(DraftDiscardFailure::new(draft, error)))
            }
        }
    })
}

/// Append the raw message to the Sent folder when requested and a Sent
/// role folder resolves. Returns the real APPENDUID-derived `ObjectId`
/// when the server surfaces one under UIDPLUS, else the supplied
/// fallback `Message-ID` (controlled domain). A failed APPEND after a
/// committed send is non-fatal: the send already succeeded, so we never
/// re-drive SMTP. The receipt's `sent_copy` says what happened: an APPEND
/// failure whose transmission evidence leaves its application unknown is
/// `Uncertain` (the copy may be filed; appending again may duplicate it),
/// otherwise `NotFiled`.
///
/// Takes the message by value: `append_message` carries that allocation to the
/// socket as it is, where `append(&[u8])` would copy the whole message once
/// more just to cross the driver channel.
pub(super) async fn append_to_sent_or_fallback(
    account: &ImapAccount,
    raw: bytes::Bytes,
    save: bool,
    fallback_message_id: &str,
) -> SendReceipt {
    let fallback = || ObjectId(format!("imapmsgid1:{fallback_message_id}"));
    let not_filed =
        |reason, detail| SendReceipt::new(fallback(), SentCopy::NotFiled { reason, detail });
    if !save {
        return SendReceipt::new(fallback(), SentCopy::NotRequested);
    }
    let Some(sent) = role_folder(account, FolderRole::Sent) else {
        tracing::warn!(
            target: "bifrost_imap::send",
            "save_to_sent requested but no Sent folder resolved; returning generated Message-ID"
        );
        return not_filed(SentCopyNotFiled::NoSentFolder, None);
    };
    let conn = match account.pool.checkout_any().await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(
                target: "bifrost_imap::send",
                error = %err,
                "message sent but Sent-folder APPEND could not get a connection; \
                 no Sent copy was filed"
            );
            return not_filed(
                SentCopyNotFiled::NoConnection,
                Some(sent_copy_warning(
                    &err,
                    "no connection for the Sent-folder APPEND",
                )),
            );
        }
    };
    let mut message = crate::types::AppendMessage::new(raw);
    message.flags = vec![Flag::Seen];
    match conn
        .connection()
        .append_message(&sent, message, account.command_timeout())
        .await
    {
        Ok(Some((uidvalidity, uid))) => {
            let id = encode_object_id(&sent, uidvalidity, uid);
            SendReceipt::new(id.clone(), SentCopy::Filed { id: Some(id) })
        }
        // Filed, but the server gave no UIDPLUS code to identify the copy.
        Ok(None) => SendReceipt::new(fallback(), SentCopy::Filed { id: None }),
        Err(err) => {
            tracing::warn!(
                target: "bifrost_imap::send",
                error = %err,
                "message sent but Sent-folder APPEND failed; \
                 check the Sent folder before filing it again"
            );
            // Classified through the same leaf rule as every partial-commit
            // producer, so transmission evidence decides, not error text.
            let leaf = op_err(AccountOperation::Send)(err);
            let uncertain = Application::from_leaf(&leaf) == Application::Uncertain;
            let detail = sent_copy_warning(&leaf, "the Sent-folder APPEND failed");
            if uncertain {
                SendReceipt::new(fallback(), SentCopy::Uncertain { detail })
            } else {
                not_filed(SentCopyNotFiled::AppendFailed, Some(detail))
            }
        }
    }
}

/// The advisory detail of a Sent copy this call could not confirm. A
/// `Warning`, not an `AccountError`: an error stamped `Send` on a success
/// value would carry recovery advice for re-sending, and there is no public
/// Sent-append operation for it to advise instead.
fn sent_copy_warning(err: &dyn std::fmt::Display, what: &str) -> bifrost_types::Warning {
    bifrost_types::Warning::user_safe(
        bifrost_types::WarningKind::OperatorAttentionNeeded,
        format!("Message sent, but {what}; the Sent folder may lack a copy."),
    )
    .with_protocol_detail(DiagnosticText::support_only(err.to_string()))
}

/// One-shot raw `BODY[]` full-message fetch. Hydration returns a parsed
/// projection, not verbatim octets, so `draft_send` needs this dedicated
/// fetch to recover the exact RFC 5322 bytes of a saved draft.
async fn fetch_full_message(
    account: &ImapAccount,
    decoded: &DecodedObjectId,
) -> Result<bytes::Bytes, AccountError> {
    let err = folder_err(account, &decoded.folder, AccountOperation::DraftSend);
    let mut conn = account
        .checkout_for_folder(&decoded.folder)
        .await
        .map_err(err)?;
    account
        .select_folder(&mut conn, &decoded.folder, super::SelectPurpose::Read)
        .await
        .map_err(err)?;
    let operand = pim_operand([decoded.uid], AccountOperation::DraftSend)?;
    let uids = operand
        .uid_set()
        .ok_or_else(|| pim_malformed(AccountOperation::DraftSend, "draft object id has no UID"))?;
    let responses = conn
        .connection()
        .uid_fetch_full_messages(uids, DRAFT_FETCH_BUDGET, account.command_timeout())
        .await
        .map_err(err)?;
    responses
        .into_iter()
        .filter(|fetch| fetch.uid == Some(decoded.uid))
        .reduce(|mut held, later| {
            super::get::merge_fetch_response(&mut held, later);
            held
        })
        .and_then(|mut fetch| super::hydration::decode_body(&mut fetch))
        .ok_or_else(|| {
            pim_malformed(
                AccountOperation::DraftSend,
                "draft message body not returned by FETCH",
            )
        })
}

#[derive(Debug)]
struct ParsedDraft {
    envelope: bifrost_types::SubmissionEnvelope,
    body: Vec<u8>,
    message_id: String,
}

/// Parse a saved draft's RFC 5322 headers into a submission envelope and
/// strip the `Bcc:` header from the transmitted body. To/Cc/Bcc become
/// the recipient set; shared header resolution chooses the reverse path. The
/// draft's own `Message-ID` is the fallback id.
fn parse_draft_for_submission(
    raw: &[u8],
    default_from: &bifrost_types::Address,
    op: AccountOperation,
) -> Result<ParsedDraft, AccountError> {
    // Operate on bytes end-to-end: the transmitted body must be the
    // verbatim draft octets. Decoding through `from_utf8_lossy` would
    // turn any 8-bit / binary octet into U+FFFD on the wire while the
    // Sent-folder APPEND keeps the original bytes - a silent divergence
    // between what was sent and what is recorded. Header parsing only
    // needs the names and the address-list / message-id values, which
    // are ASCII-structured; we decode those lossily for parsing but
    // never feed the decoded form back into the transmitted bytes.
    let (header_end, body_start) = split_header_body(raw);
    let header_block = &raw[..header_end];

    let headers = bifrost_types::mime::HeaderMap::parse(header_block);

    let (from, recipients) = headers
        .submission_envelope()
        .map_err(|detail| pim_malformed(op, detail))?;
    let from = from.unwrap_or_else(|| default_from.clone());

    let message_id = headers
        .message_ids("message-id")
        .into_iter()
        .next()
        .unwrap_or_else(|| bifrost_types::mime::generate_message_id(&from));

    let body = strip_bcc_header(header_block, &raw[body_start..]);

    Ok(ParsedDraft {
        envelope: bifrost_types::SubmissionEnvelope { from, recipients },
        body,
        message_id,
    })
}

/// Locate the header/body split of a raw RFC 5322 message, tolerating
/// mixed CRLF / bare-LF line endings and a header-only draft (no blank
/// line). Returns `(header_end, body_start)` byte offsets: the header
/// block is `raw[..header_end]` and the body is `raw[body_start..]`. The
/// separator blank line itself sits between the two and is dropped. For
/// a header-only draft both offsets are `raw.len()` (empty body).
fn split_header_body(raw: &[u8]) -> (usize, usize) {
    // Find the empty line that ends the header block. We scan for the
    // first line terminator (`\r\n` or bare `\n`) that is immediately
    // followed by another line terminator. `header_end` is the offset
    // just *after* the first terminator (so the last header line keeps
    // its own ending); `body_start` is just *after* the second
    // terminator (so the blank separator line is dropped). Mixed CRLF /
    // bare-LF endings are tolerated on both sides.
    let mut i = 0;
    while i < raw.len() {
        // Length of a terminator starting at `j`, if any.
        let term_len = |j: usize| -> Option<usize> {
            match raw.get(j) {
                Some(b'\r') if raw.get(j + 1) == Some(&b'\n') => Some(2),
                Some(b'\n') => Some(1),
                _ => None,
            }
        };
        if let Some(first) = term_len(i) {
            let header_end = i + first;
            if let Some(second) = term_len(header_end) {
                return (header_end, header_end + second);
            }
        }
        i += 1;
    }
    (raw.len(), raw.len())
}

/// Reassemble the message bytes with every `Bcc:` header line removed so
/// blind recipients are not disclosed in the transmitted message.
///
/// Byte-exact: header and body octets are copied verbatim (no UTF-8
/// round-trip), so an 8-bit / binary body survives unchanged. Only the
/// dropped `Bcc:` lines and the synthesized header/body separator touch
/// line endings; the separator matches the header block's own ending
/// (CRLF unless the draft is bare-LF) so a uniform-LF draft is not
/// emitted with a mixed CRLF separator.
fn strip_bcc_header(header_block: &[u8], body: &[u8]) -> Vec<u8> {
    rewrite_header_block(header_block, body, b"bcc", None)
}

/// Reassemble a message with every field named `drop` (case-insensitive,
/// continuation lines included) removed from the header block and, when
/// `prepend` is set, that complete field line inserted as the first
/// header. Same byte-exact and separator policy as [`strip_bcc_header`].
fn rewrite_header_block(
    header_block: &[u8],
    body: &[u8],
    drop: &[u8],
    prepend: Option<&str>,
) -> Vec<u8> {
    let uses_crlf = header_block.windows(2).any(|w| w == b"\r\n") || !header_block.contains(&b'\n');
    let sep: &[u8] = if uses_crlf { b"\r\n" } else { b"\n" };

    let mut kept = Vec::with_capacity(header_block.len() + body.len() + 4);
    if let Some(field) = prepend {
        kept.extend_from_slice(field.as_bytes());
        kept.extend_from_slice(sep);
    }
    let mut skipping = false;
    for line in split_inclusive_lf(header_block) {
        let trimmed = trim_leading_crlf(line);
        let is_continuation = line.first() == Some(&b' ') || line.first() == Some(&b'\t');
        if !is_continuation {
            skipping = header_name_is(trimmed, drop);
        }
        if !skipping {
            kept.extend_from_slice(line);
        }
    }
    // Ensure the header block ends with a line terminator, then add the
    // blank-line separator, then the verbatim body.
    if !kept.ends_with(b"\n") {
        kept.extend_from_slice(sep);
    }
    kept.extend_from_slice(sep);
    kept.extend_from_slice(body);
    kept
}

/// Split on `\n`, keeping the terminator on each line (the byte analogue
/// of `str::split_inclusive('\n')`), and skipping a trailing empty slice.
fn split_inclusive_lf(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    std::iter::from_fn(move || {
        if start >= bytes.len() {
            return None;
        }
        let rest = &bytes[start..];
        match rest.iter().position(|&b| b == b'\n') {
            Some(idx) => {
                let line = &rest[..=idx];
                start += idx + 1;
                Some(line)
            }
            None => {
                let line = rest;
                start = bytes.len();
                Some(line)
            }
        }
    })
}

fn trim_leading_crlf(line: &[u8]) -> &[u8] {
    let mut s = line;
    while let [first, rest @ ..] = s
        && (*first == b'\r' || *first == b'\n')
    {
        s = rest;
    }
    s
}

/// True when a header line's field name (before the first `:`) is `name`,
/// case-insensitive, ignoring surrounding whitespace.
fn header_name_is(line: &[u8], name: &[u8]) -> bool {
    match line.iter().position(|&b| b == b':') {
        Some(idx) => line[..idx].trim_ascii().eq_ignore_ascii_case(name),
        None => false,
    }
}

/// Replace a saved draft's `Date:` with the send time, at finalization.
///
/// A draft's `Date` is its save time (or absent, for a draft another
/// client wrote); RFC 5322 section 3.6.1 makes `Date` the moment the
/// message is ready for delivery, which is now. Every `Date` field is
/// dropped and one fresh field is inserted first, so the transmitted body
/// and the Sent copy built from the result agree.
///
/// Returns the draft unchanged when it carries a `DKIM-Signature`: the
/// signer committed to those header bytes, and replacing or inserting a
/// `Date` would invalidate the signature, so a signed draft keeps its own
/// `Date` (stale or missing). Raw sends never come through here.
///
/// # Errors
///
/// `Request(Malformed)` when the header block opens with a continuation
/// line: inserting a field in front would absorb it into the new `Date`.
fn restamp_draft_date(
    raw: &[u8],
    now: jiff::Timestamp,
    op: AccountOperation,
) -> Result<Vec<u8>, AccountError> {
    let (header_end, body_start) = split_header_body(raw);
    let header_block = &raw[..header_end];
    if split_inclusive_lf(header_block).any(|line| {
        !matches!(line.first(), Some(b' ' | b'\t'))
            && header_name_is(trim_leading_crlf(line), b"dkim-signature")
    }) {
        return Ok(raw.to_vec());
    }
    if matches!(header_block.first(), Some(b' ' | b'\t')) {
        return Err(pim_malformed(
            op,
            "draft header block opens with a continuation line",
        ));
    }
    let date = bifrost_types::format_rfc5322_date(now).map_err(stamp_imap_protocol)?;
    Ok(rewrite_header_block(
        header_block,
        &raw[body_start..],
        b"date",
        Some(&format!("Date: {date}")),
    ))
}

/// Stamp each not-accepted recipient's error with the send call's operation.
fn restamp_recipients(
    recipients: Vec<RecipientNotAccepted>,
    op: AccountOperation,
) -> Vec<RecipientNotAccepted> {
    recipients
        .into_iter()
        .map(|recipient| {
            RecipientNotAccepted::new(
                recipient.address,
                recipient.outcome,
                restamp(recipient.error, op),
            )
        })
        .collect()
}

pub(crate) fn draft_discard(
    account: ImapAccount,
    draft: DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.draft_discard {
            return Err(super::error::unsupported(AccountOperation::DraftDiscard));
        }
        let id = decode_object_id(&ObjectId(draft.0), AccountOperation::DraftDiscard)?;
        delete_messages(&account, vec![id], AccountOperation::DraftDiscard)
            .await
            .map(|_| ())
            .map_err(folder_progress_error)
    })
}

pub(crate) fn search(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.search {
            return Err(super::error::unsupported(AccountOperation::Search));
        }
        let plan = search_plan(&account.dav_scopes, &request, AccountOperation::Search)?;
        let mut pager = SearchPager::new(&request, plan.folder.as_ref(), AccountOperation::Search)?;
        for folder in pager.folders(search_folders(&account, plan.folder.as_ref())) {
            if pager.is_full() {
                break;
            }
            let err = folder_err(&account, &folder, AccountOperation::Search);
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
                .await
                .map_err(err)?;
            let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
            pager.check_epoch(&folder, uidvalidity)?;
            let roots = conn
                .connection()
                .uid_thread(
                    "REFERENCES",
                    "UTF-8",
                    &plan.criteria,
                    account.command_timeout(),
                )
                .await
                .map_err(err)?;
            // A thread's position is its lowest member UID: member sets are
            // disjoint, so the minima order the folder's threads totally,
            // and an expunge elsewhere cannot move an untouched thread.
            let mut threads = Vec::new();
            for root in roots {
                let mut uids = Vec::new();
                flatten_thread(&root, &mut uids);
                if let Some(&key) = uids.iter().min() {
                    threads.push((key, uids));
                }
            }
            pager.accept(&folder, uidvalidity, threads, |uids| {
                encode_thread_id(&folder, uidvalidity, &uids)
            })?;
        }
        Ok(pager.finish())
    })
}

pub(crate) fn search_messages(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.search_messages {
            return Err(super::error::unsupported(AccountOperation::SearchMessages));
        }
        let plan = search_plan(
            &account.dav_scopes,
            &request,
            AccountOperation::SearchMessages,
        )?;
        let mut pager = SearchPager::new(
            &request,
            plan.folder.as_ref(),
            AccountOperation::SearchMessages,
        )?;
        for folder in pager.folders(search_folders(&account, plan.folder.as_ref())) {
            if pager.is_full() {
                break;
            }
            let err = folder_err(&account, &folder, AccountOperation::SearchMessages);
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
                .await
                .map_err(err)?;
            let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
            pager.check_epoch(&folder, uidvalidity)?;
            let result = conn
                .connection()
                .uid_search(&plan.criteria, account.command_timeout())
                .await
                .map_err(err)?;
            let uids = result.ids.into_iter().map(|uid| (uid, uid)).collect();
            pager.accept(&folder, uidvalidity, uids, |uid| {
                encode_object_id(&folder, uidvalidity, uid)
            })?;
        }
        Ok(pager.finish())
    })
}

pub(crate) fn containers_list(
    account: ImapAccount,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    Box::pin(async move {
        Ok(ContainerList {
            containers: containers_snapshot(&account),
            skipped_scopes: account.discovery_skips.clone(),
        })
    })
}

fn container_refresh_error(
    error: AccountError,
    effect: StepEffect,
    source: &MailboxName,
    destination: Option<&MailboxName>,
) -> AccountError {
    let mut refs = vec![TargetRef::new(
        if destination.is_some() {
            TargetRole::Source
        } else {
            TargetRole::Subject
        },
        ErrorScope::Cursor(super::folder_scope(source)),
    )];
    if let Some(destination) = destination {
        refs.push(TargetRef::new(
            TargetRole::Destination,
            ErrorScope::Cursor(super::folder_scope(destination)),
        ));
    }
    let targets = StepTargets::complete(refs);
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(targets.clone()));
    record.committed(effect, targets, StepReplay::Blocking);
    super::mutate::finish_partial(error, record)
}

pub(crate) fn container_create(
    account: ImapAccount,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    // IMAP mailboxes carry no container color; accepted for trait
    // parity with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(super::error::unsupported(AccountOperation::ContainerCreate));
        }
        let full_name = child_name(
            &account,
            parent.as_ref(),
            &name,
            AccountOperation::ContainerCreate,
        )?;
        let authority = parent
            .as_ref()
            .map(|parent| {
                folder_from_container(
                    &account.dav_scopes,
                    parent,
                    AccountOperation::ContainerCreate,
                )
            })
            .transpose()?;
        let err = |error| {
            account.folder_error(
                error,
                authority.as_ref().unwrap_or(&full_name),
                super::error::ImapErrorContext::operation(AccountOperation::ContainerCreate),
            )
        };
        let conn = account.pool.checkout_any().await.map_err(err)?;
        conn.connection()
            .create(&full_name, account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(
            &account,
            conn.connection(),
            AccountOperation::ContainerCreate,
        )
        .await
        .map_err(|error| container_refresh_error(error, StepEffect::Created, &full_name, None))?;
        Ok(ContainerId(full_name.account_id()))
    })
}

pub(crate) fn container_rename(
    account: ImapAccount,
    container: ContainerId,
    name: String,
    // IMAP has no mailbox recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(
            &account.dav_scopes,
            &container,
            AccountOperation::ContainerRename,
        )?;
        let new_name =
            renamed_sibling(&account, &folder, &name, AccountOperation::ContainerRename)?;
        let err = folder_err(&account, &folder, AccountOperation::ContainerRename);
        let mut conn = account.pool.checkout_any().await.map_err(err)?;
        conn.deselect_target(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        conn.connection()
            .rename(&folder, &new_name, account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(
            &account,
            conn.connection(),
            AccountOperation::ContainerRename,
        )
        .await
        .map_err(|error| {
            container_refresh_error(error, StepEffect::Moved, &folder, Some(&new_name))
        })
    })
}

pub(crate) fn container_move(
    account: ImapAccount,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(
            &account.dav_scopes,
            &container,
            AccountOperation::ContainerMove,
        )?;
        let leaf = leaf_name(&account, &folder);
        let new_name = child_name(
            &account,
            new_parent.as_ref(),
            &leaf,
            AccountOperation::ContainerMove,
        )?;
        let destination = new_parent
            .as_ref()
            .map(|parent| {
                folder_from_container(&account.dav_scopes, parent, AccountOperation::ContainerMove)
            })
            .transpose()?;
        let err = folder_err(&account, &folder, AccountOperation::ContainerMove);
        let mut conn = account.pool.checkout_any().await.map_err(err)?;
        conn.deselect_target(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        conn.connection()
            .rename(&folder, &new_name, account.command_timeout())
            .await
            .map_err(|error| {
                account.folder_pair_error(
                    error,
                    &folder,
                    destination.as_ref(),
                    super::error::ImapErrorContext::operation(AccountOperation::ContainerMove),
                )
            })?;
        refresh_folders(&account, conn.connection(), AccountOperation::ContainerMove)
            .await
            .map_err(|error| {
                container_refresh_error(error, StepEffect::Moved, &folder, Some(&new_name))
            })
    })
}

pub(crate) fn container_delete(
    account: ImapAccount,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(
            &account.dav_scopes,
            &container,
            AccountOperation::ContainerDelete,
        )?;
        let err = folder_err(&account, &folder, AccountOperation::ContainerDelete);
        let mut conn = account.pool.checkout_any().await.map_err(err)?;
        conn.deselect_target(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        let status = conn
            .connection()
            .status(&folder, "MESSAGES", account.command_timeout())
            .await
            .map_err(err)?;
        let non_empty = status
            .items
            .iter()
            .any(|item| matches!(item, StatusItem::Messages(count) if *count > 0));
        if non_empty {
            return Err(pim_malformed(
                AccountOperation::ContainerDelete,
                "refusing to delete non-empty IMAP mailbox",
            ));
        }
        conn.connection()
            .delete(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(
            &account,
            conn.connection(),
            AccountOperation::ContainerDelete,
        )
        .await
        .map_err(|error| container_refresh_error(error, StepEffect::Deleted, &folder, None))
    })
}

pub(crate) fn identities_list() -> AccountFuture<Result<Vec<Identity>, AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::IdentitiesList)) })
}

pub(crate) fn identity_update(
    _identity: IdentityId,
    _patch: IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::IdentityUpdate)) })
}

pub(crate) fn vacation_get() -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::VacationGet)) })
}

pub(crate) fn vacation_set(_config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::VacationSet)) })
}

pub(crate) fn quota_get(
    account: ImapAccount,
) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.quota_get {
            return Err(super::error::unsupported(AccountOperation::QuotaGet));
        }
        let Some(folder) = quota_probe_folder(&account) else {
            return Ok(None);
        };
        let err = op_err(AccountOperation::QuotaGet);
        let conn = account.pool.checkout_any().await.map_err(err)?;
        let quota = conn
            .connection()
            .get_quota_root(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        for (_root, resources) in quota.resources {
            for resource in resources {
                if resource.name.eq_ignore_ascii_case("STORAGE") {
                    return Ok(Some(QuotaInfo {
                        used_bytes: resource.usage.saturating_mul(1024),
                        total_bytes: Some(resource.limit.saturating_mul(1024)),
                    }));
                }
            }
        }
        Ok(None)
    })
}

pub(crate) fn thread_hydrate(
    account: ImapAccount,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.thread_hydrate {
            return Err(super::error::unsupported(AccountOperation::HydrateThread));
        }
        let members = resolve_thread_members(
            &account,
            &thread,
            ThreadScope::Account,
            AccountOperation::HydrateThread,
        )
        .await?;
        let messages = hydrate_decoded(
            &account,
            members,
            HydrationProjection::Full,
            AccountOperation::HydrateThread,
        )
        .await?;
        Ok(ThreadHydration {
            id: thread,
            messages,
        })
    })
}

pub(crate) fn message_hydrate(
    account: ImapAccount,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.message_hydrate {
            return Err(super::error::unsupported(AccountOperation::HydrateMessage));
        }
        let decoded = decode_object_id(&message, AccountOperation::HydrateMessage)?;
        // The canonical spelling of the requested id, compared against the
        // ids `fetch_to_message` mints. `hydrate_decoded` already drops
        // FETCH responses for UIDs nobody asked about; picking by id rather
        // than by position keeps the answer right independently of that.
        // The answer then carries the id as submitted (a legacy `imap1` or
        // non-canonical spelling included), so the caller gets back the key
        // it asked under, matching the batch hydration lane.
        let wanted = encode_object_id(&decoded.folder, decoded.uidvalidity, decoded.uid);
        let messages = hydrate_decoded(
            &account,
            vec![decoded],
            projection,
            AccountOperation::HydrateMessage,
        )
        .await?;
        let mut found = messages
            .into_iter()
            .find(|hydrated| hydrated.id == wanted)
            .ok_or_else(|| expunged_message(message.clone()))?;
        found.id = message;
        Ok(found)
    })
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
                || role_folder(&account, FolderRole::Trash).as_ref() == Some(&folder)
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
            let trash = role_folder(&account, FolderRole::Trash)
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
        let trash = role_folder(&account, FolderRole::Trash)
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

async fn delete_messages(
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
                    uids: uids.clone(),
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
                    .any(|(epoch, uids)| *epoch == id.uidvalidity && uids.contains(&id.uid));
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
                    uids: vec![id.uid],
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
            let mut epochs: std::collections::BTreeMap<u32, Vec<u32>> = Default::default();
            for item in succeeded {
                let id = window
                    .iter()
                    .find(|id| id.id.0 == item.0)
                    .expect("engine answers under submitted identity");
                epochs.entry(id.uidvalidity).or_default().push(id.uid);
            }
            for (uidvalidity, uids) in epochs {
                done.push(FolderEffect {
                    folder: folder.clone(),
                    uids,
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

async fn hydrate_decoded(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    projection: HydrationProjection,
    op: AccountOperation,
) -> Result<Vec<Message>, AccountError> {
    let mut messages = Vec::new();
    for (folder, ids) in group_by_folder(ids) {
        let err = folder_err(account, &folder, op);
        let preflight = folder_preflight_err(account, &folder, op);
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(preflight)?;
        let selected = account
            .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
            .await
            .map_err(preflight)?;
        let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(preflight)?;
        let operand = pim_operand(valid_uids(ids, uidvalidity, op)?, op)?;
        let Some(uid_set) = operand.uid_set() else {
            continue;
        };
        let mut attrs = attrs_for_hydration(projection);
        super::hydration::push_native_thread_attrs(&mut attrs, &conn.connection().server_profile());
        let fetches = conn
            .connection()
            .uid_fetch(uid_set.as_sequence_set(), &attrs, account.command_timeout())
            .await
            .map_err(err)?;
        let merged = super::get::merge_requested_fetches(fetches, operand.uids(), |_, _| {
            Ok::<(), crate::Error>(())
        })
        .map_err(err)?;
        messages.extend(
            merged
                .into_values()
                .filter_map(|fetch| fetch_to_message(&folder, uidvalidity, fetch, projection)),
        );
    }
    messages.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    Ok(messages)
}

/// Which folders an account-scoped native thread is resolved in for one
/// action. Snapshot handles ignore it: their members are fixed.
#[derive(Debug, Clone, Copy)]
enum ThreadScope<'a> {
    /// Every folder that can hold a member (hydration, flag changes).
    Account,
    /// One folder only (removal from that folder).
    Only(&'a MailboxName),
    /// Every member folder except this one (copy into it).
    Except(&'a MailboxName),
}

fn is_native_thread(target: &MutationTarget) -> bool {
    matches!(target, MutationTarget::Thread(thread) if thread.0.starts_with("imapnative2:"))
}

/// The folders a native thread's members are searched in. Gmail labels are
/// views of one message store, so X-GM-THRID is searched once in the `\All`
/// folder when the server lists one (searching every label would return the
/// same message once per label); OBJECTID servers hold distinct messages per
/// folder, so THREADID is searched in every selectable folder. The order is
/// the deterministic account order the search pager uses.
fn native_thread_folders(
    entries: Vec<std::sync::Arc<super::folder_registry::FolderEntry>>,
    kind: super::envelope::NativeThreadKind,
    scope: ThreadScope<'_>,
) -> Vec<MailboxName> {
    if let ThreadScope::Only(folder) = scope {
        return vec![folder.clone()];
    }
    let selectable: Vec<_> = entries
        .into_iter()
        .filter(|entry| entry.selectable())
        .collect();
    let all_mail = (kind == super::envelope::NativeThreadKind::Gmail)
        .then(|| {
            selectable.iter().find(|entry| {
                entry
                    .attributes()
                    .iter()
                    .any(|attr| matches!(attr, MailboxAttribute::All))
            })
        })
        .flatten();
    let mut folders: Vec<MailboxName> = match all_mail {
        Some(entry) => vec![entry.name.clone()],
        None => selectable.iter().map(|entry| entry.name.clone()).collect(),
    };
    if let ThreadScope::Except(excluded) = scope {
        folders.retain(|folder| folder != excluded);
    }
    folders.sort_by(MailboxName::cmp_account_order);
    folders
}

/// Resolve a thread handle to its current member ids.
///
/// A snapshot handle decodes to its recorded UIDs. An account-scoped native
/// handle runs one UID SEARCH per folder chosen by `native_thread_folders`
/// under the epoch SELECT reports now; the handle records no UIDVALIDITY, so
/// there is no stale epoch to refuse here. An epoch change between this
/// resolution and the write receives the shared mutation engine's stale-epoch
/// answer. Hydration separately validates its read epoch through `valid_uids`.
async fn resolve_thread_members(
    account: &ImapAccount,
    thread: &ThreadId,
    scope: ThreadScope<'_>,
    op: AccountOperation,
) -> Result<Vec<DecodedObjectId>, AccountError> {
    let Some(native) = super::envelope::decode_server_thread_id(thread, op)? else {
        let decoded = decode_thread_id(thread, op)?;
        return Ok(decoded
            .uids
            .into_iter()
            .map(|uid| DecodedObjectId::canonical(decoded.folder.clone(), decoded.uidvalidity, uid))
            .collect());
    };
    let mut members = Vec::new();
    for folder in native_thread_folders(account.folders.entries(), native.kind, scope) {
        let preflight = folder_preflight_err(account, &folder, op);
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(preflight)?;
        let selected = account
            .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
            .await
            .map_err(preflight)?;
        let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(preflight)?;
        let uids = conn
            .connection()
            .uid_search(&native.criteria, account.command_timeout())
            .await
            .map_err(preflight)?
            .ids;
        members.extend(
            uids.into_iter()
                .map(|uid| DecodedObjectId::canonical(folder.clone(), uidvalidity, uid)),
        );
    }
    Ok(members)
}

async fn resolved_targets(
    account: &ImapAccount,
    target: &MutationTarget,
    scope: ThreadScope<'_>,
    op: AccountOperation,
) -> Result<Vec<DecodedObjectId>, AccountError> {
    if let MutationTarget::Thread(thread) = target {
        return resolve_thread_members(account, thread, scope, op).await;
    }
    decoded_targets(target, op)
}

fn decoded_targets(
    target: &MutationTarget,
    op: AccountOperation,
) -> Result<Vec<DecodedObjectId>, AccountError> {
    match target {
        MutationTarget::Message(id) => Ok(vec![decode_object_id(id, op)?]),
        MutationTarget::Thread(thread) => {
            let decoded = decode_thread_id(thread, op)?;
            Ok(decoded
                .uids
                .into_iter()
                .map(|uid| {
                    DecodedObjectId::canonical(decoded.folder.clone(), decoded.uidvalidity, uid)
                })
                .collect())
        }
        _ => Err(pim_malformed(
            op,
            "unsupported MutationTarget variant for IMAP per-message operation",
        )),
    }
}

fn group_by_folder(ids: Vec<DecodedObjectId>) -> Vec<(MailboxName, Vec<DecodedObjectId>)> {
    let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
    for id in ids {
        grouped
            .entry(id.folder.account_id())
            .or_insert_with(|| (id.folder.clone(), Vec::new()))
            .1
            .push(id);
    }
    grouped.into_values().collect()
}

/// Build the wire operand for a PIM primitive, from the same source the
/// batch lanes use.
///
/// These lanes answer `Result<(), AccountError>` for the whole request:
/// there is no per-id outcome lane, so a UID the operand cannot carry has
/// nowhere to be reported. Refuse the command instead of sending an operand
/// narrower than the caller asked for, which would report success for a
/// message the server never saw.
fn pim_operand<I: IntoIterator<Item = u32>>(
    uids: I,
    op: AccountOperation,
) -> Result<UidOperand, AccountError> {
    let operand = UidOperand::build(uids);
    if !operand.excluded().is_empty() {
        return Err(pim_malformed(
            op,
            "message id could not be placed in an IMAP UID operand",
        ));
    }
    Ok(operand)
}

fn valid_uids(
    ids: Vec<DecodedObjectId>,
    uidvalidity: u32,
    op: AccountOperation,
) -> Result<Vec<u32>, AccountError> {
    let mut uids = Vec::new();
    for id in ids {
        if id.uidvalidity != uidvalidity {
            // This is the hydration read guard. Writes use the shared
            // mutation engine's per-target stale-epoch classification.
            return Err(pim_malformed(
                op,
                "UIDVALIDITY changed before IMAP operation",
            ));
        }
        uids.push(id.uid);
    }
    Ok(uids)
}

fn folder_from_container(
    scopes: &super::DavScopeIndex,

    container: &ContainerId,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    scopes
        .imap_container(container, op)
        .map(super::ImapFolder::into_mailbox)
}

fn imap_flag_for_keyword(keyword: &str) -> Flag {
    Flag::from_neutral_keyword(keyword)
}

#[derive(Debug)]
struct SearchPlan {
    criteria: SearchCriteria,
    folder: Option<MailboxName>,
}

fn search_plan(
    scopes: &super::DavScopeIndex,
    request: &SearchRequest,
    op: AccountOperation,
) -> Result<SearchPlan, AccountError> {
    let mut plan = match &request.filter {
        Some(filter) => criteria_from_filter(scopes, filter, op)?,
        None => CriteriaPart {
            criteria: SearchCriteria::new().all(),
            folder: None,
        },
    };
    if let Some(raw) = request
        .provider_query
        .as_deref()
        .filter(|raw| !raw.trim().is_empty())
    {
        validate_provider_query(raw, op)?;
        if plan.criteria == "ALL" {
            plan.criteria = SearchCriteria::raw(raw.trim().to_owned(), Vec::new());
        } else {
            plan.criteria = plan
                .criteria
                .and(SearchCriteria::raw(raw.trim().to_owned(), Vec::new()));
        }
    }
    if plan.criteria.trim().is_empty() {
        plan.criteria = SearchCriteria::new().all();
    }
    Ok(SearchPlan {
        criteria: plan.criteria,
        folder: plan.folder,
    })
}

fn validate_provider_query(raw: &str, op: AccountOperation) -> Result<(), AccountError> {
    let bytes = raw.as_bytes();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut pos = 0usize;

    while pos < bytes.len() {
        let byte = bytes[pos];
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            pos += 1;
            continue;
        }

        match byte {
            b'"' => quoted = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| pim_malformed(op, "provider query has an unmatched ')'"))?;
            }
            b'{' => {
                let mut end = pos + 1;
                let digits_start = end;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end > digits_start {
                    if bytes.get(end) == Some(&b'+') {
                        end += 1;
                    }
                    if bytes.get(end) == Some(&b'}') {
                        return Err(pim_malformed(
                            op,
                            "provider query must not contain an IMAP literal",
                        ));
                    }
                }
            }
            _ => {}
        }
        pos += 1;
    }

    if quoted {
        return Err(pim_malformed(
            op,
            "provider query has an unterminated quoted string",
        ));
    }
    if depth != 0 {
        return Err(pim_malformed(op, "provider query has an unmatched '('"));
    }
    Ok(())
}

#[derive(Debug)]
struct CriteriaPart {
    criteria: SearchCriteria,
    folder: Option<MailboxName>,
}

fn criteria_from_filter(
    scopes: &super::DavScopeIndex,

    filter: &SearchFilter,
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    match filter {
        SearchFilter::From(value) => leaf(SearchCriteria::new().from(value), op),
        SearchFilter::To(value) => leaf(SearchCriteria::new().to(value), op),
        SearchFilter::Subject(value) => leaf(SearchCriteria::new().subject(value), op),
        // Has currently preserves the existing BODY approximation, which can
        // match prose and miss encoded attachment filenames. It does not meet
        // the attachment filename/type contract: enterprise users expect actual
        // attachments. Correct support needs attachment-aware evaluation (also
        // through AND/OR/NOT and pagination), not another SEARCH string alias.
        // Do not replace this with Unsupported without owner authorization:
        // that would remove the existing search path rather than repair it.
        SearchFilter::Body(value) | SearchFilter::Has(value) => {
            leaf(SearchCriteria::new().body(value), op)
        }
        SearchFilter::In(container) => Ok(CriteriaPart {
            criteria: SearchCriteria::new().all(),
            folder: Some(folder_from_container(scopes, container, op)?),
        }),
        SearchFilter::Labeled(LabelId(label)) => leaf(SearchCriteria::new().keyword(label), op),
        SearchFilter::DateRange { after, before } => {
            let mut criteria = SearchCriteria::new();
            if let Some(after) = after {
                criteria = criteria
                    .sent_since(&imap_date(*after))
                    .map_err(|e| pim_malformed(op, e.to_string()))?;
            }
            if let Some(before) = before {
                criteria = criteria
                    .sent_before(&imap_date(*before))
                    .map_err(|e| pim_malformed(op, e.to_string()))?;
            }
            Ok(CriteriaPart {
                criteria: empty_to_all(criteria),
                folder: None,
            })
        }
        SearchFilter::And(filters) => combine_and(scopes, filters, op),
        SearchFilter::Or(filters) => combine_or(scopes, filters, op),
        SearchFilter::Not(filter) => {
            let inner = criteria_from_filter(scopes, filter, op)?;
            Ok(CriteriaPart {
                criteria: SearchCriteria::new().not(&SearchCriteria::new().group(inner.criteria)),
                folder: inner.folder,
            })
        }
        _ => Err(super::error::unsupported(op)),
    }
}

fn leaf(
    result: Result<SearchCriteria, crate::Error>,
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    let criteria = result.map_err(|e| pim_malformed(op, e.to_string()))?;
    Ok(CriteriaPart {
        criteria: empty_to_all(criteria),
        folder: None,
    })
}

fn combine_and(
    scopes: &super::DavScopeIndex,

    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    let mut criteria = Vec::new();
    let mut folder = None;
    for filter in filters {
        let part = criteria_from_filter(scopes, filter, op)?;
        folder = merge_folder(folder, part.folder, op)?;
        if part.criteria != "ALL" {
            criteria.push(part.criteria);
        }
    }
    Ok(CriteriaPart {
        criteria: if criteria.is_empty() {
            SearchCriteria::new().all()
        } else {
            criteria
                .into_iter()
                .fold(SearchCriteria::new(), SearchCriteria::and)
        },
        folder,
    })
}

fn combine_or(
    scopes: &super::DavScopeIndex,

    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    if filters.is_empty() {
        return Ok(CriteriaPart {
            criteria: SearchCriteria::new().all(),
            folder: None,
        });
    }
    let mut parts = Vec::new();
    for filter in filters {
        let part = criteria_from_filter(scopes, filter, op)?;
        // A folder restriction (`In`) nested inside an `Or` cannot be
        // expressed in a single-mailbox IMAP SEARCH: SEARCH runs against
        // the currently-selected mailbox, so hoisting the `In` folder to
        // the whole OR group would silently restrict the *other* OR
        // branches to that folder too, changing the query's meaning. The
        // AND case is defensible (the folder narrows the whole
        // conjunction); the OR case is not, so reject it explicitly
        // rather than leak the restriction.
        if part.folder.is_some() {
            return Err(or_folder_restriction_error(op));
        }
        parts.push(part.criteria);
    }
    let folder = None;
    let mut iter = parts.into_iter();
    let mut criteria = iter.next().unwrap_or_else(|| SearchCriteria::new().all());
    for next in iter {
        criteria = SearchCriteria::new().or(
            &SearchCriteria::new().group(criteria),
            &SearchCriteria::new().group(next),
        );
    }
    Ok(CriteriaPart { criteria, folder })
}

#[allow(clippy::unwrap_in_result)]
fn merge_folder(
    current: Option<MailboxName>,
    next: Option<MailboxName>,
    op: AccountOperation,
) -> Result<Option<MailboxName>, bifrost_types::AccountError> {
    match (current, next) {
        (Some(a), Some(b)) if a != b => Err(super::error::search_folders_conflict(&a, &b, op)),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

fn empty_to_all(criteria: SearchCriteria) -> SearchCriteria {
    if criteria.trim().is_empty() {
        SearchCriteria::new().all()
    } else {
        criteria
    }
}

fn imap_date(time: SystemTime) -> String {
    let datetime = Offset::UTC
        .to_datetime(Timestamp::try_from(time).unwrap_or(Timestamp::UNIX_EPOCH))
        .date();
    let month = match datetime.month() {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        _ => "Dec",
    };
    format!("{}-{month}-{}", datetime.day(), datetime.year())
}

fn search_folders(account: &ImapAccount, restriction: Option<&MailboxName>) -> Vec<MailboxName> {
    if let Some(folder) = restriction {
        return vec![folder.clone()];
    }
    // `entries()` iterates a `HashMap`, so its order is not stable
    // run-to-run. The search page cursor is a position in a walk ordered
    // by folder and then UID, and `SearchPager` resumes by skipping every
    // folder ordered before the cursor's; a nondeterministic folder order
    // would skip folders the walk never visited. Sort by display name and
    // wire identity, the same total order the pager compares by.
    sorted_search_folders(&account.folders)
}

fn sorted_search_folders(registry: &super::folder_registry::FolderRegistry) -> Vec<MailboxName> {
    let mut folders: Vec<MailboxName> = registry
        .entries()
        .into_iter()
        .filter(|entry| entry.selectable())
        .map(|entry| entry.name.clone())
        .collect();
    folders.sort_by(MailboxName::cmp_account_order);
    folders
}

/// Prefix of the search page cursor. The payload after it is the canonical
/// `imap2` object id of the walk position, so the folder rides in the same
/// length-prefixed wire-identity encoding object ids use.
const SEARCH_CURSOR_PREFIX: &str = "imapsearch1:";

/// Where a search walk stands: the folder, its UIDVALIDITY, and the key UID
/// of the last item handed back (the message UID, or a thread's lowest
/// member UID).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchPosition {
    folder: MailboxName,
    uidvalidity: u32,
    key: u32,
}

impl SearchPosition {
    fn encode(&self) -> Vec<u8> {
        let id = encode_object_id(&self.folder, self.uidvalidity, self.key);
        format!("{SEARCH_CURSOR_PREFIX}{}", id.0).into_bytes()
    }

    fn decode(bytes: &[u8], op: AccountOperation) -> Result<Self, AccountError> {
        let invalid = || search_page_cursor_error(op, "invalid IMAP page cursor");
        let payload = std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.strip_prefix(SEARCH_CURSOR_PREFIX))
            .filter(|payload| payload.starts_with("imap2:"))
            .ok_or_else(invalid)?;
        let decoded = decode_object_id(&ObjectId(payload.to_owned()), op).map_err(|_| invalid())?;
        Ok(Self {
            folder: decoded.folder,
            uidvalidity: decoded.uidvalidity,
            key: decoded.uid,
        })
    }
}

/// Pages a multi-folder search by position, not by offset.
///
/// IMAP has no server-side result state a page cursor could pin, so every
/// page call re-runs the search. An offset into that fresh result is not
/// stable: expunging an item on an earlier page shifts every later item down
/// one place and the item at the boundary is never returned. The cursor is
/// instead the position of the last item handed back, in a walk ordered by
/// folder (`MailboxName::cmp_account_order`) and then key UID ascending. UIDs
/// within one UIDVALIDITY never move, so "key greater than the cursor's" names
/// the same survivors whatever was expunged meanwhile, and folders ordered
/// before the cursor's are not searched again.
///
/// What a position walk does not promise: an item moved into, or a folder
/// created or renamed into, a position the walk already passed is not
/// returned; a thread whose membership changed between pages can come back
/// under a new id or be passed. A changed UIDVALIDITY on the cursor's folder
/// voids every UID in the cursor, so the call fails `ConcurrencyConflict` and
/// the caller repeats the walk.
struct SearchPager<T> {
    resume: Option<SearchPosition>,
    limit: usize,
    collected: Vec<(SearchPosition, T)>,
    op: AccountOperation,
}

impl<T> SearchPager<T> {
    fn new(
        request: &SearchRequest,
        restriction: Option<&MailboxName>,
        op: AccountOperation,
    ) -> Result<Self, AccountError> {
        let resume = request
            .page_cursor
            .as_deref()
            .map(|bytes| SearchPosition::decode(bytes, op))
            .transpose()?;
        if let (Some(resume), Some(restriction)) = (&resume, restriction)
            && resume.folder.cmp_account_order(restriction) != std::cmp::Ordering::Equal
        {
            return Err(search_page_cursor_error(
                op,
                "IMAP page cursor names a folder outside the search restriction",
            ));
        }
        // A zero-item page has no last item to mint a position from: reusing
        // the incoming cursor would loop, and advancing would skip.
        let limit = request.limit.unwrap_or(500);
        if limit == 0 {
            return Err(pim_malformed(op, "IMAP search page limit must be nonzero"));
        }
        Ok(Self {
            resume,
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
            collected: Vec::new(),
            op,
        })
    }

    /// The folders still ahead of the cursor, in walk order. `all` must
    /// already be sorted by `cmp_account_order`.
    fn folders(&self, all: Vec<MailboxName>) -> Vec<MailboxName> {
        match &self.resume {
            None => all,
            Some(resume) => all
                .into_iter()
                .filter(|folder| folder.cmp_account_order(&resume.folder).is_ge())
                .collect(),
        }
    }

    /// One item past the page is known, so the page is settled and later
    /// folders need not be searched.
    fn is_full(&self) -> bool {
        self.collected.len() > self.limit
    }

    /// The UID floor this folder resumes above, or a `ConcurrencyConflict`
    /// when it is the cursor's folder under a different UIDVALIDITY. Call it
    /// straight after SELECT, so a changed epoch is reported as itself rather
    /// than as whatever the following search command did.
    fn check_epoch(
        &self,
        folder: &MailboxName,
        uidvalidity: u32,
    ) -> Result<Option<u32>, AccountError> {
        match &self.resume {
            Some(resume) if folder.cmp_account_order(&resume.folder).is_eq() => {
                if resume.uidvalidity != uidvalidity {
                    return Err(search_cursor_voided(self.op, folder));
                }
                Ok(Some(resume.key))
            }
            _ => Ok(None),
        }
    }

    /// Take one folder's results, `(key, item)` in any order.
    fn accept<K>(
        &mut self,
        folder: &MailboxName,
        uidvalidity: u32,
        mut entries: Vec<(u32, K)>,
        mint: impl Fn(K) -> T,
    ) -> Result<(), AccountError> {
        let floor = self.check_epoch(folder, uidvalidity)?;
        entries.sort_by_key(|(key, _)| *key);
        entries.dedup_by_key(|(key, _)| *key);
        for (key, entry) in entries {
            if self.is_full() {
                break;
            }
            if floor.is_some_and(|floor| key <= floor) {
                continue;
            }
            let position = SearchPosition {
                folder: folder.clone(),
                uidvalidity,
                key,
            };
            self.collected.push((position, mint(entry)));
        }
        Ok(())
    }

    fn finish(mut self) -> Page<T> {
        let (next_cursor, estimated_total) = if self.collected.len() > self.limit {
            // The lookahead item stays ahead of the cursor for the next call.
            self.collected.truncate(self.limit);
            let last = self
                .collected
                .last()
                .map(|(position, _)| position.encode().into());
            (last, None)
        } else if self.resume.is_none() {
            // This call enumerated the whole result from the start.
            (
                None,
                Some(u64::try_from(self.collected.len()).unwrap_or(u64::MAX)),
            )
        } else {
            (None, None)
        };
        Page {
            truncated: false,
            items: self.collected.into_iter().map(|(_, item)| item).collect(),
            next_cursor,
            estimated_total,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        }
    }
}

/// A page cursor this account cannot use (undecodable, or naming a folder
/// outside the search restriction) requires restarting the walk, not
/// retrying the same page. The shared page_cursor field is the signal JMAP
/// uses too, so consumers restart paging the same way across providers.
fn search_page_cursor_error(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    AccountError::invalid_page_cursor(op, Protocol::Imap, detail)
}

fn flatten_thread(node: &ThreadNode, out: &mut Vec<u32>) {
    if let Some(uid) = node.id {
        out.push(uid);
    }
    for child in &node.children {
        flatten_thread(child, out);
    }
}

fn containers_snapshot(account: &ImapAccount) -> Vec<Container> {
    account
        .folders
        .entries()
        .into_iter()
        .map(|entry| {
            let display_name = leaf_name(account, &entry.name);
            container_from_folder_entry(&entry, display_name)
        })
        .collect()
}

/// Project one registry entry onto a `Container`. Pure over the entry plus
/// the already-resolved display name (the only piece that needs the
/// account, for INBOX aliasing), so the namespace / owner / rights
/// projection is unit-pinnable without a live connection.
///
/// A shared/other-user folder is namespaced by its owning mailbox. IMAP has
/// no separate per-owner id space - the full mailbox path IS the native id
/// in every namespace - so `owner_local_id` is that same path, and
/// `native_id` (which the cursor scope keys on) needs no re-encoding. That
/// keeps `native_id` byte-identical to the `CursorScope::Folder` string
/// `discover_cursor_scopes` emits for the same folder.
fn container_from_folder_entry(
    entry: &super::folder_registry::FolderEntry,
    display_name: String,
) -> Container {
    let native = entry.name.account_id();
    let namespace = if entry.shared_owner.is_some() {
        ContainerNamespace::Shared
    } else {
        ContainerNamespace::Personal
    };
    Container::new(
        ContainerId(native.clone()),
        ContainerKind::Folder,
        entry
            .shared_owner
            .is_none()
            .then(|| folder_role(&entry.attributes(), entry.name.as_str(), entry.delimiter()))
            .flatten(),
        Provenance {
            provider: ProtocolKind::Imap,
            kind: ContainerKind::Folder,
            native: native.clone(),
        },
        display_name,
        parent_id(entry.delimiter(), &native),
    )
    // IMAP mailboxes carry no container color, and IMAP is folder-shaped
    // (special-use maps into `role`), so `style` and `system` keep their
    // `Container::new` defaults.
    .with_namespace(namespace)
    .with_owner(entry.shared_owner.clone())
    .with_owner_local_id(entry.shared_owner.as_ref().map(|_| native.clone()))
    // RFC 4314 MYRIGHTS, captured at shared-folder discovery. `None` for a
    // personal folder or a server without ACL.
    .with_rights(entry.rights.as_ref().map(rights_from_myrights))
}

/// Project an RFC 4314 MYRIGHTS set onto the unified [`ContainerRights`].
///
/// Every member is `Some(_)`: once the server answered MYRIGHTS, the
/// absence of a letter is a definite "no", not "unreported" (the
/// unreported case is `FolderEntry::rights == None`, which never reaches
/// here). The mapping follows RFC 4314 Section 4:
///
/// The typed rights set owns the projection. RFC 4314 Section 2.1.1 says
/// virtual c/d in MYRIGHTS responses must be ignored, not expanded; granting
/// x from d could offer deletion of a shared folder without server authority.
fn rights_from_myrights(rights: &MailboxRights) -> ContainerRights {
    rights.container_rights()
}

use super::folder_registry::folder_role;
#[cfg(test)]
use super::folder_registry::role_folder_from_entries;

fn parent_id(delimiter: Option<char>, native: &str) -> Option<ContainerId> {
    let delimiter = delimiter?;
    let parent = MailboxName::from_account_id(native)
        .ok()?
        .parent(delimiter)
        .ok()??;
    Some(ContainerId(parent.account_id()))
}

fn role_folder(account: &ImapAccount, role: FolderRole) -> Option<MailboxName> {
    account.folders.role_folder(role)
}

fn quota_probe_folder(account: &ImapAccount) -> Option<MailboxName> {
    role_folder(account, FolderRole::Inbox)
        .or_else(|| first_selectable_folder(account.folders.entries()))
}

fn first_selectable_folder(
    entries: Vec<std::sync::Arc<super::folder_registry::FolderEntry>>,
) -> Option<MailboxName> {
    entries
        .into_iter()
        // Account quota must not silently report a colleague's shared mailbox
        // usage when the user's personal namespace has no INBOX.
        .filter(|entry| entry.selectable() && entry.shared_owner.is_none())
        // `entries()` comes from a HashMap, and distinct mailbox identities
        // can share a display name. Keep the fallback stable across rebuilds.
        .min_by(|left, right| left.name.cmp_account_order(&right.name))
        .map(|entry| entry.name.clone())
}

fn child_name(
    account: &ImapAccount,
    parent: Option<&ContainerId>,
    leaf: &str,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    let Some(parent) = parent else {
        return MailboxName::new(leaf.to_owned()).map_err(|e| pim_malformed(op, e.to_string()));
    };
    let parent_folder = folder_from_container(&account.dav_scopes, parent, op)?;
    let delimiter = account
        .folders
        .get(&parent_folder)
        .and_then(|entry| entry.delimiter())
        .ok_or_else(|| super::error::unsupported(op))?;
    parent_folder
        .child(delimiter, leaf)
        .map_err(|e| pim_malformed(op, e.to_string()))
}

fn renamed_sibling(
    account: &ImapAccount,
    folder: &MailboxName,
    new_leaf: &str,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    let delimiter = account
        .folders
        .get(folder)
        .and_then(|entry| entry.delimiter());
    let Some(delimiter) = delimiter else {
        return MailboxName::new(new_leaf.to_owned()).map_err(|e| pim_malformed(op, e.to_string()));
    };
    if let Some(parent) = folder
        .parent(delimiter)
        .map_err(|e| pim_malformed(op, e.to_string()))?
    {
        parent
            .child(delimiter, new_leaf)
            .map_err(|e| pim_malformed(op, e.to_string()))
    } else {
        MailboxName::new(new_leaf.to_owned()).map_err(|e| pim_malformed(op, e.to_string()))
    }
}

fn leaf_name(account: &ImapAccount, folder: &MailboxName) -> String {
    let delimiter = account
        .folders
        .get(folder)
        .and_then(|entry| entry.delimiter());
    crate::types::mailbox::SpecialUse::leaf_name(folder.as_str(), delimiter).to_owned()
}

/// Re-LIST the personal namespace over a connection the caller already holds.
///
/// The connection is a parameter rather than a fresh checkout on purpose: every
/// caller is mid-mutation and still holds a pooled checkout, so acquiring a
/// second one here would deadlock whenever the in-flight mutations exhaust the
/// pool cap. LIST is not mailbox-relative, so reusing the mutation's own connection
/// is safe regardless of what it has selected.
async fn refresh_folders(
    account: &ImapAccount,
    conn: &ImapConnection,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    let profile = conn.server_profile();
    let folders = factory::list_folders(conn, &account.config, &profile)
        .await
        .map_err(err)?;
    // Personal-root re-LIST only. `replace_all` would clear the shared /
    // other-user entries NAMESPACE discovery installed at open, taking their
    // owner tags and MYRIGHTS with them, so every container mutation would
    // blank the shared half of `containers_list` until the next reopen.
    account.folders.replace_personal(folders);
    Ok(())
}

fn body_for_hydration(projection: HydrationProjection) -> super::hydration::BodySelection {
    use super::hydration::BodySelection;
    match projection {
        HydrationProjection::Headers => BodySelection::None,
        HydrationProjection::Preview(limit) => BodySelection::Preview(limit),
        HydrationProjection::Full | HydrationProjection::FullWithBlobs => BodySelection::Whole,
        // Not dead code: `HydrationProjection` is `#[non_exhaustive]` and that
        // attribute is scoped per crate, so a match in bifrost-imap on an enum
        // defined in bifrost-types needs a wildcard even when the named arms
        // are exhaustive in practice.
        _ => BodySelection::None,
    }
}

fn attrs_for_hydration(projection: HydrationProjection) -> Vec<FetchAttr> {
    super::hydration::FetchSelection {
        flags: true,
        envelope: true,
        size: true,
        modseq: false,
        body: body_for_hydration(projection),
    }
    .attributes()
}

fn fetch_to_message(
    folder: &MailboxName,
    uidvalidity: u32,
    mut fetch: crate::types::FetchResponse,
    projection: HydrationProjection,
) -> Option<Message> {
    let uid = fetch.uid?;
    let envelope = fetch.envelope.clone();
    let raw_body =
        super::hydration::decode_selected_body(&mut fetch, body_for_hydration(projection));
    let parsed = raw_body.as_deref().map(parse_message);
    let decoded = parsed.as_ref().map(|message| {
        select_body_with(
            message,
            SelectOptions {
                include_attachment_bytes: matches!(projection, HydrationProjection::FullWithBlobs),
                max_body_bytes: match projection {
                    HydrationProjection::Preview(limit) => Some(limit),
                    _ => None,
                },
                limits: MimeLimits::default(),
            },
        )
    });
    let incomplete = parsed
        .as_ref()
        .is_some_and(|message| message.defects.iter().any(is_incomplete_defect))
        || decoded
            .as_ref()
            .is_some_and(|body| body.defects.iter().any(is_incomplete_defect));
    // Unlike inventory and hydration, a PIM read is a one-off display answer
    // that writes no cached flag state, so a reply missing FLAGS still shows
    // the message (with no flags) rather than hiding it: an enterprise user
    // asking for a message expects to see it. The sync lanes, whose output
    // would overwrite cached read/flagged state, are the strict ones.
    let flags = super::inventory::flags_set(fetch.flags.as_deref().unwrap_or(&[]));
    let importance = if flags.contains(IMPORTANCE_KEYWORD) {
        Importance::High
    } else {
        Importance::Normal
    };
    Some(Message {
        id: encode_object_id(folder, uidvalidity, uid),
        thread_id: super::envelope::encode_server_thread_id(
            fetch.thread_id.as_deref(),
            fetch.gmail_thread_id,
        ),
        from: envelope
            .as_ref()
            .map(|env| addresses(&env.from))
            .unwrap_or_default(),
        to: envelope
            .as_ref()
            .map(|env| addresses(&env.to))
            .unwrap_or_default(),
        cc: envelope
            .as_ref()
            .map(|env| addresses(&env.cc))
            .unwrap_or_default(),
        bcc: envelope
            .as_ref()
            .map(|env| addresses(&env.bcc))
            .unwrap_or_default(),
        reply_to: envelope
            .as_ref()
            .map(|env| addresses(&env.reply_to))
            .unwrap_or_default(),
        subject: envelope.as_ref().and_then(|env| env.subject.clone()),
        date: parsed
            .as_ref()
            .and_then(|message| message.headers.date("Date")),
        containers: vec![ContainerId(folder.account_id())],
        importance,
        flags,
        body_text: decoded.as_ref().and_then(|body| body.text.clone()),
        body_html: decoded.as_ref().and_then(|body| body.html.clone()),
        attachments: match projection {
            HydrationProjection::Full | HydrationProjection::FullWithBlobs => decoded
                .as_ref()
                .map(|body| {
                    body.attachments
                        .iter()
                        .map(|attachment| MessageAttachment {
                            filename: attachment.filename.clone(),
                            content_type: Some(attachment.content_type.clone()),
                            content_id: attachment.content_id.clone(),
                            inline: attachment.inline,
                            size: Some(attachment.size),
                            source: attachment
                                .data
                                .clone()
                                .map(AttachmentSource::Inline)
                                .unwrap_or(AttachmentSource::None),
                            truncated: attachment.truncated,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            // `Headers` and `Preview` carry no attachment list, and the
            // wildcard is required because the enum is `#[non_exhaustive]`
            // outside its defining crate.
            _ => Vec::new(),
        },
        size_bytes: fetch.rfc822_size,
        in_reply_to: envelope
            .as_ref()
            .and_then(|env| env.first_in_reply_to().map(str::to_owned)),
        references: parsed
            .as_ref()
            .map(|message| message.headers.message_ids("References"))
            .unwrap_or_default(),
        incomplete,
    })
}

fn is_incomplete_defect(defect: &Defect) -> bool {
    matches!(
        defect,
        Defect::Truncated
            | Defect::TextTruncated
            | Defect::DepthExceeded
            | Defect::PartCountExceeded
            | Defect::HeaderBlockTooLarge
    )
}

fn addresses(addresses: &[crate::types::EnvelopeAddress]) -> Vec<Address> {
    addresses
        .iter()
        .filter_map(|addr| {
            addr.email().map(|address| Address {
                name: addr.name.clone(),
                address,
            })
        })
        .collect()
}

/// Serialize a `DraftPatch` into RFC 5322 octets via the shared
/// `bifrost-types` MIME assembler - the same path the send surface uses,
/// so IMAP has one composition path, not two. Inline attachments are now
/// supported on drafts; uploaded-attachment handles remain rejected (A6).
/// The `Bcc:` header is preserved in the saved draft body (unlike the
/// send path, which strips it).
fn draft_patch_to_rfc5322(patch: &DraftPatch) -> Result<Vec<u8>, AccountError> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(super::error::unsupported(
            AccountOperation::AttachmentUpload,
        ));
    }

    let from = patch.from.as_ref().and_then(Option::as_ref);
    let empty_addresses: Vec<Address> = Vec::new();
    let empty_strings: Vec<String> = Vec::new();
    let empty_attachments: Vec<bifrost_types::AttachmentInline> = Vec::new();

    let composed = bifrost_types::ComposedMessage {
        // Provisional save-time stamp so a stored draft is a conforming
        // message; `draft_send` replaces it with the send time.
        date: Some(jiff::Timestamp::now()),
        from,
        to: patch.to.as_deref().unwrap_or(&empty_addresses),
        cc: patch.cc.as_deref().unwrap_or(&empty_addresses),
        bcc: patch.bcc.as_deref().unwrap_or(&empty_addresses),
        reply_to: patch.reply_to.as_deref().unwrap_or(&empty_addresses),
        subject: patch.subject.as_ref().and_then(Option::as_deref),
        body_text: patch.body_text.as_ref().and_then(Option::as_deref),
        body_html: patch.body_html.as_ref().and_then(Option::as_deref),
        attachments_inline: patch
            .attachments_inline
            .as_deref()
            .unwrap_or(&empty_attachments),
        in_reply_to: patch.in_reply_to.as_ref().and_then(Option::as_deref),
        references: patch.references.as_deref().unwrap_or(&empty_strings),
        message_id: None,
        include_bcc_header: true,
        // The draft path carries no read-receipt request (that field lives
        // on SendRequest, not DraftPatch).
        disposition_notification_to: None,
    };
    bifrost_types::render_rfc5322(&composed).map_err(stamp_imap_protocol)
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::folder_registry::{FolderRegistry, SharedFolderEntry};
    use crate::types::MailboxInfo;

    #[test]
    fn search_folder_offsets_survive_registry_rebuild_with_display_ties() {
        let names = [
            MailboxName::new("a").unwrap(),
            MailboxName::from_wire("a".into(), b"&AGE-", false),
            MailboxName::from_wire("a".into(), b"&AAGE-", false),
        ];
        let expected = vec![
            names[2].account_id(),
            names[1].account_id(),
            names[0].account_id(),
        ];
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
            let registry = FolderRegistry::from_lists(
                order
                    .into_iter()
                    .map(|i| MailboxInfo {
                        name: names[i].clone(),
                        ..Default::default()
                    })
                    .collect(),
                vec![],
            );
            let folders = sorted_search_folders(&registry);
            assert_eq!(
                folders
                    .iter()
                    .map(MailboxName::account_id)
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(folders[1], names[1]);
        }
    }

    fn registry_with_shared(rights: Option<&str>) -> FolderRegistry {
        let personal = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            delimiter: Some('/'),
            ..Default::default()
        };
        let shared = MailboxInfo {
            name: MailboxName::new("Shared/alice/Reports").expect("valid mailbox"),
            delimiter: Some('/'),
            ..Default::default()
        };
        FolderRegistry::from_lists(
            vec![personal],
            vec![SharedFolderEntry {
                info: shared,
                owner: bifrost_types::MailboxId("alice".to_owned()),
                rights: rights.map(MailboxRights::parse),
                namespace_prefix: "Shared/".to_owned(),
            }],
        )
    }

    // imap-F2: the locally-malformed helpers used to build an
    // operation-less `Request(Malformed)`, so every PIM lane's client-bug
    // errors arrived indistinguishable in telemetry. Each helper now takes
    // the operation its caller is servicing. The recovery class was already
    // `ClientBug` and is unchanged; what this pins is that the operation
    // rides along, and that it is the *caller's* operation rather than a
    // hardcoded one.
    #[test]
    fn pim_containers_and_nested_search_refuse_dav_ownership() {
        let container = ContainerId("https://example.test/contacts/".into());
        let (scopes, _) = super::super::DavScopeIndex::build(
            vec![bifrost_types::FolderId(container.0.clone())],
            Vec::new(),
            &std::collections::HashSet::new(),
        );
        let err = folder_from_container(&scopes, &container, AccountOperation::AddToContainer)
            .expect_err("a DAV destination must not reach IMAP");
        assert_eq!(err.operation(), Some(AccountOperation::AddToContainer));
        let filter = SearchFilter::And(vec![SearchFilter::In(container)]);
        assert!(criteria_from_filter(&scopes, &filter, AccountOperation::Search).is_err());
        assert!(
            folder_from_container(
                &scopes,
                &ContainerId("INBOX".into()),
                AccountOperation::AddToContainer,
            )
            .is_ok()
        );
    }

    #[test]
    fn locally_malformed_pim_errors_carry_the_calling_operation() {
        // Id / cursor / mailbox-name decode.
        let err = folder_from_container(
            &super::super::DavScopeIndex::default(),
            &ContainerId("INBOX\r\nNOOP".to_owned()),
            AccountOperation::BulkMove,
        )
        .expect_err("a CRLF-bearing mailbox name is not valid");
        assert_eq!(err.operation(), Some(AccountOperation::BulkMove));

        let err = decoded_targets(
            &MutationTarget::Message(ObjectId("not-an-imap-id".to_owned())),
            AccountOperation::SetIsRead,
        )
        .expect_err("a foreign object id must not decode");
        assert_eq!(err.operation(), Some(AccountOperation::SetIsRead));

        let Err(err) = pim_operand([0u32], AccountOperation::UpdateFlags) else {
            panic!("uid 0 cannot ride an operand");
        };
        assert_eq!(err.operation(), Some(AccountOperation::UpdateFlags));

        let err = valid_uids(
            vec![DecodedObjectId::canonical(
                MailboxName::new("INBOX").expect("valid mailbox"),
                2,
                5,
            )],
            3,
            AccountOperation::BulkDestroy,
        )
        .expect_err("a UIDVALIDITY change is refused");
        assert_eq!(err.operation(), Some(AccountOperation::BulkDestroy));

        // The search lane is the one that had two operations to confuse:
        // `search` and `search_messages` share every helper below, and the
        // criteria/folder-merge builders used to stamp `SearchMessages`
        // unconditionally. Drive each through `Search` so a reverted
        // threading shows up as the wrong op, not merely as `None`.
        let err = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::provider("(UNSEEN"),
            AccountOperation::Search,
        )
        .expect_err("unbalanced provider query");
        assert_eq!(err.operation(), Some(AccountOperation::Search));

        let err = combine_and(
            &super::super::DavScopeIndex::default(),
            &[
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::In(ContainerId("Sent".to_owned())),
            ],
            AccountOperation::Search,
        )
        .expect_err("a two-folder conjunction is refused");
        assert_eq!(err.operation(), Some(AccountOperation::Search));

        let err = combine_or(
            &super::super::DavScopeIndex::default(),
            &[
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::From("a@b.test".to_owned()),
            ],
            AccountOperation::Search,
        )
        .expect_err("a folder restriction inside an Or is refused");
        assert_eq!(err.operation(), Some(AccountOperation::Search));

        let Err(err) = SearchPager::<u32>::new(
            &search_request(None, Some("not-a-number")),
            None,
            AccountOperation::Search,
        ) else {
            panic!("a page cursor this crate did not mint");
        };
        assert_eq!(err.operation(), Some(AccountOperation::Search));
    }

    // The PIM primitives answer for the whole request, so they have nowhere
    // to report a per-id exclusion. Sending the narrowed operand anyway
    // would return `Ok(())` for messages the server was never asked about.
    #[test]
    fn a_pim_operand_refuses_a_uid_it_cannot_carry_instead_of_narrowing() {
        let Err(refused) = super::pim_operand([4u32, 0, 6], AccountOperation::SearchMessages)
        else {
            panic!("a dropped UID must be refused, not silently narrowed");
        };
        assert!(matches!(
            refused.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));

        let operand = super::pim_operand([4u32, 6], AccountOperation::SearchMessages)
            .expect("carryable UIDs");
        assert_eq!(operand.uids(), &[4, 6]);
        assert_eq!(
            operand
                .uid_set()
                .expect("non-empty operand")
                .as_sequence_set()
                .as_str(),
            "4,6"
        );

        // An empty group is not an exclusion: nothing was asked for, so
        // there is no command to send and nothing to refuse.
        let empty = super::pim_operand([], AccountOperation::SearchMessages)
            .expect("an empty request is not malformed");
        assert!(empty.uid_set().is_none());
    }

    #[test]
    fn container_projection_namespaces_shared_folder_and_leaves_personal_alone() {
        let registry = registry_with_shared(Some("lr"));

        let shared = registry
            .get(&MailboxName::new("Shared/alice/Reports").expect("valid mailbox"))
            .expect("shared entry");
        let container = container_from_folder_entry(&shared, "Reports".to_owned());
        assert_eq!(container.namespace, ContainerNamespace::Shared);
        assert_eq!(
            container.owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        // IMAP's owner-local id is the full path: it is already the native
        // id in the owner's namespace, and it matches the cursor-scope
        // string byte for byte.
        assert_eq!(
            container.owner_local_id.as_deref(),
            Some("Shared/alice/Reports")
        );
        assert_eq!(container.native_id(), "Shared/alice/Reports");
        // No provider types IMAP folders, so `content_class` stays None.
        assert!(container.content_class.is_none());

        let personal = registry
            .get(&MailboxName::new("INBOX").expect("valid mailbox"))
            .expect("personal entry");
        let container = container_from_folder_entry(&personal, "INBOX".to_owned());
        assert_eq!(container.namespace, ContainerNamespace::Personal);
        assert!(container.owner.is_none());
        assert!(container.owner_local_id.is_none());
        // A personal folder was never MYRIGHTS-probed, so rights are
        // unreported rather than "no rights".
        assert!(container.rights.is_none());
    }

    // The rights projection has to survive the case where the personal
    // `LIST "" "*"` ALSO returned the shared path (RFC 2342 permits it and
    // several servers do it). Before the precedence fix the overlapping entry
    // stayed personal, so `Container::rights` came back `None` for a folder
    // whose MYRIGHTS had been parsed and then discarded.
    #[test]
    fn shared_container_keeps_its_rights_when_the_personal_list_overlaps() {
        let path = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: path.clone(),
                delimiter: Some('/'),
                ..Default::default()
            }],
            vec![SharedFolderEntry {
                info: MailboxInfo {
                    name: path.clone(),
                    delimiter: Some('/'),
                    ..Default::default()
                },
                owner: bifrost_types::MailboxId("alice".to_owned()),
                rights: Some(MailboxRights::parse("lr")),
                namespace_prefix: "Shared/".to_owned(),
            }],
        );

        let entry = registry.get(&path).expect("entry present");
        let container = container_from_folder_entry(&entry, "Reports".to_owned());
        assert_eq!(container.namespace, ContainerNamespace::Shared);
        let rights = container.rights.expect("MYRIGHTS reaches the container");
        // `lr` is a read-only share: readable, but no insert / delete / flag.
        assert_eq!(rights.may_read_items, Some(true));
        assert_eq!(rights.may_add_items, Some(false));
        assert_eq!(rights.may_remove_items, Some(false));
    }

    /// The container's `native_id` must be byte-identical to the
    /// `CursorScope::Folder` string discovery emits for the same folder -
    /// that identity is what lets the consumer join a container to its
    /// sync scope.
    #[test]
    fn container_native_id_matches_discovered_cursor_scope() {
        let registry = registry_with_shared(Some("lr"));
        for entry in registry.entries() {
            let scope = super::super::folder_scope(&entry.name);
            let container = container_from_folder_entry(&entry, "n/a".to_owned());
            assert_eq!(
                scope,
                bifrost_types::CursorScope::Folder(bifrost_types::FolderId(
                    container.native_id().to_string()
                )),
                "container native_id must equal the emitted cursor scope string",
            );
        }
    }

    #[test]
    fn myrights_projection_distinguishes_read_only_from_writable() {
        // Read-only share: lookup + read only.
        let read_only = rights_from_myrights(&MailboxRights::parse("lr"));
        assert_eq!(read_only.may_read_items, Some(true));
        assert_eq!(read_only.may_add_items, Some(false));
        assert_eq!(read_only.may_remove_items, Some(false));
        assert_eq!(read_only.may_set_seen, Some(false));
        assert_eq!(read_only.may_set_keywords, Some(false));
        assert_eq!(read_only.may_create_child, Some(false));
        assert_eq!(read_only.may_rename, Some(false));
        assert_eq!(read_only.may_delete, Some(false));
        assert_eq!(read_only.may_submit, Some(false));

        // Full share.
        let full = rights_from_myrights(&MailboxRights::parse("lrswipkxtea"));
        assert_eq!(full.may_read_items, Some(true));
        assert_eq!(full.may_add_items, Some(true));
        assert_eq!(full.may_remove_items, Some(true));
        assert_eq!(full.may_set_seen, Some(true));
        assert_eq!(full.may_set_keywords, Some(true));
        assert_eq!(full.may_create_child, Some(true));
        assert_eq!(full.may_rename, Some(true));
        assert_eq!(full.may_delete, Some(true));
        assert_eq!(full.may_submit, Some(true));

        // Virtual response rights do not grant the modern member rights.
        let legacy = rights_from_myrights(&MailboxRights::parse("lrcd"));
        assert_eq!(legacy.may_create_child, Some(false));
        assert_eq!(legacy.may_delete, Some(false));
    }

    #[test]
    fn importance_high_sets_keyword_others_clear() {
        // High -> set `$important`; Normal/Low -> clear it. One STORE op,
        // never an expand-into-two.
        assert!(importance_sets_important_keyword(Importance::High));
        assert!(!importance_sets_important_keyword(Importance::Normal));
        assert!(!importance_sets_important_keyword(Importance::Low));
    }

    #[test]
    fn send_as_rejected_unsupported() {
        let mut request = SendRequest::default();
        request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::MailboxId(
            "shared@contoso.com".to_string(),
        )));
        let err = send_as_guard(&request).expect("send_as request must be rejected");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::Send));

        // A personal send passes the guard.
        assert!(send_as_guard(&SendRequest::default()).is_none());
    }

    #[tokio::test]
    async fn host_attachment_unsupported() {
        // IMAP has no cloud-drive hosting; the leg returns
        // `Unsupported(HostAttachment)`.
        let err = unsupported_hosted(AccountOperation::HostAttachment)
            .await
            .expect_err("imap host_attachment is unsupported");
        assert_eq!(
            err.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[test]
    fn scheduled_send_restamp_preserves_unsupported_kind() {
        // The smtp boundary already produces `Unsupported(Send)` for an
        // unsupporting relay (brick 4.6a). The IMAP send path re-stamps
        // only the operation/protocol via `restamp`; it must not change
        // the kind. Pin that the kind survives unchanged.
        let smtp_derived = crate::account::error::test_support::smtp_unsupported();

        let restamped = restamp(smtp_derived, AccountOperation::Send);
        assert_eq!(
            restamped.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        );
        assert_eq!(restamped.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn draft_send_round_trips_escaped_and_encoded_display_names() {
        for name in ["a\", b", "a\\, b", "Janne \u{e4}"] {
            let address = bifrost_types::Address::named(name, "x@y");
            let wire_address = bifrost_types::mime::format_address(&address);
            let raw = format!(
                "From: {wire_address}\r\nTo: {wire_address}\r\nBcc: {wire_address}\r\nMessage-ID: <id@test>\r\n\r\nbody"
            );
            let parsed = parse_draft_for_submission(
                raw.as_bytes(),
                &bifrost_types::Address::bare("fallback@test"),
                AccountOperation::DraftSend,
            )
            .expect("rendered addresses parse");
            assert_eq!(parsed.envelope.from.name.as_deref(), Some(name));
            assert_eq!(parsed.envelope.from.address, "x@y");
            assert_eq!(parsed.envelope.recipients.len(), 2);
            for recipient in &parsed.envelope.recipients {
                assert_eq!(recipient.address, "x@y");
                assert_eq!(recipient.name.as_deref(), Some(name));
            }
            assert_eq!(parsed.message_id, "id@test");
            let body = String::from_utf8(parsed.body).expect("UTF-8 draft");
            assert!(!body.contains("Bcc:"));
            assert!(body.ends_with("\r\n\r\nbody"));
        }
    }

    #[test]
    fn draft_send_refuses_rejected_envelope_addresses() {
        for field in ["From", "Sender", "To", "Cc", "Bcc"] {
            let raw = format!(
                "From: sender@example.test\r\nTo: recipient@example.test\r\n{field}: accepted@example.test, rejected-item\r\n\r\nbody"
            );
            let error = parse_draft_for_submission(
                raw.as_bytes(),
                &bifrost_types::Address::bare("fallback@example.test"),
                AccountOperation::DraftSend,
            )
            .expect_err("a rejected envelope address must stop draft submission");

            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                ),
                "field {field}"
            );
            assert_eq!(
                error.operation(),
                Some(AccountOperation::DraftSend),
                "field {field}"
            );
        }
    }

    #[test]
    fn draft_send_strips_bcc_into_envelope() {
        let raw = b"From: Me <me@sender.test>\r\n\
To: Ann <ann@to.test>\r\n\
Cc: cc@cc.test\r\n\
Bcc: blind1@bcc.test, Blind Two <blind2@bcc.test>\r\n\
Subject: Hi\r\n\
Message-ID: <draft-123@sender.test>\r\n\
\r\n\
body text\r\n";
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
            .expect("parses");

        // Bcc addresses are folded into the envelope recipients.
        let recipients: Vec<&str> = parsed
            .envelope
            .recipients
            .iter()
            .map(|a| a.address.as_str())
            .collect();
        assert_eq!(
            recipients,
            [
                "ann@to.test",
                "cc@cc.test",
                "blind1@bcc.test",
                "blind2@bcc.test"
            ]
        );
        assert_eq!(parsed.envelope.from.address, "me@sender.test");
        assert_eq!(parsed.message_id, "draft-123@sender.test");

        // Bcc header is stripped from the transmitted body; To/Cc remain.
        let body = String::from_utf8(parsed.body).expect("utf8");
        assert!(!body.to_ascii_lowercase().contains("bcc:"));
        assert!(body.contains("To: Ann <ann@to.test>"));
        assert!(body.contains("Cc: cc@cc.test"));
        assert!(body.contains("body text"));
    }

    #[test]
    fn raw_submissions_without_message_id_get_distinct_correlation_ids() {
        let raw = b"From: shared@example.test\r\nSender: delegate@example.test\r\nTo: target@example.test\r\n\r\nbody";
        let default_from = Address::bare("default@example.test");
        let first = parse_draft_for_submission(raw, &default_from, AccountOperation::Send)
            .expect("first submission");
        let second = parse_draft_for_submission(raw, &default_from, AccountOperation::Send)
            .expect("second submission");
        assert_eq!(first.envelope.from.address, "shared@example.test");
        assert_ne!(first.message_id, second.message_id);
        assert!(first.message_id.ends_with("@example.test"));
    }

    #[test]
    fn draft_send_falls_back_to_default_from_and_requires_recipient() {
        let default_from = bifrost_types::Address::bare("fallback@sender.test");

        // No From header: reverse path falls back to default_from.
        let raw = b"To: ann@to.test\r\n\r\nhi\r\n";
        let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
            .expect("parses");
        assert_eq!(parsed.envelope.from.address, "fallback@sender.test");

        // No recipients at all: malformed.
        let raw = b"From: me@sender.test\r\n\r\nhi\r\n";
        let err = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
            .expect_err("no recipients");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn draft_send_preserves_non_utf8_body_bytes_verbatim() {
        // An 8-bit / binary body octet (here 0xFF, invalid UTF-8) must
        // survive byte-exact in the transmitted body. The previous
        // `from_utf8_lossy` path turned it into U+FFFD (0xEF 0xBF 0xBD).
        let mut raw = b"From: me@sender.test\r\nTo: ann@to.test\r\n\r\n".to_vec();
        raw.extend_from_slice(&[0xFF, 0x00, 0xFE, b'\r', b'\n']);
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(&raw, &default_from, AccountOperation::DraftSend)
            .expect("parses");

        // Body retains the verbatim 8-bit octets; no U+FFFD substitution.
        assert!(parsed.body.ends_with(&[0xFF, 0x00, 0xFE, b'\r', b'\n']));
        assert!(!parsed.body.windows(3).any(|w| w == [0xEF, 0xBF, 0xBD]));
    }

    #[test]
    fn draft_send_bare_lf_split_and_strip_keeps_lf_endings() {
        // A bare-LF draft (no CRLF) must split on `\n\n` and the rebuilt
        // body must not gain a stray CRLF separator (no mixed endings).
        let raw = b"From: me@sender.test\nTo: ann@to.test\nBcc: blind@bcc.test\n\nbody line\n";
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
            .expect("parses");

        let body = parsed.body;
        // Bcc folded into recipients.
        assert!(
            parsed
                .envelope
                .recipients
                .iter()
                .any(|a| a.address == "blind@bcc.test")
        );
        // No CRLF anywhere: the bare-LF draft stays bare-LF.
        assert!(!body.windows(2).any(|w| w == b"\r\n"));
        // Bcc header line is gone; To survives.
        let text = String::from_utf8(body).expect("ascii");
        assert!(!text.to_ascii_lowercase().contains("bcc:"));
        assert!(text.contains("To: ann@to.test"));
        assert!(text.contains("body line"));
    }

    #[test]
    fn draft_send_header_only_draft_has_empty_body() {
        // A header-only draft (no blank line, no body) is degenerate but
        // recoverable: the split yields an empty body and the envelope is
        // still built from the headers.
        let raw = b"From: me@sender.test\r\nTo: ann@to.test\r\n";
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
            .expect("parses");
        assert_eq!(parsed.envelope.recipients[0].address, "ann@to.test");
        // Header block ends with CRLF + a synthesized blank line, then no body.
        let text = String::from_utf8(parsed.body).expect("ascii");
        assert!(text.contains("To: ann@to.test"));
        assert!(text.ends_with("\r\n\r\n"));
    }

    #[test]
    fn split_header_body_handles_mixed_and_header_only() {
        // CRLF blank line: header_end after `A: 1\r\nB: 2\r\n` (12),
        // body_start after the second `\r\n` (14).
        assert_eq!(split_header_body(b"A: 1\r\nB: 2\r\n\r\nbody"), (12, 14));
        // Bare-LF blank line: header_end after `A: 1\nB: 2\n` (10),
        // body_start after the second `\n` (11).
        assert_eq!(split_header_body(b"A: 1\nB: 2\n\nbody"), (10, 11));
        // No blank line: header-only.
        let raw = b"A: 1\r\n";
        assert_eq!(split_header_body(raw), (raw.len(), raw.len()));
    }

    #[test]
    fn or_with_nested_in_folder_is_rejected() {
        // An `In(folder)` nested inside an `Or` cannot be expressed as a
        // single-mailbox IMAP SEARCH; reject rather than silently leak
        // the folder restriction to the whole OR group.
        use bifrost_types::search::SearchFilter;
        let filter = SearchFilter::Or(vec![
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::From("a@b.test".to_owned()),
        ]);
        let err = match criteria_from_filter(
            &super::super::DavScopeIndex::default(),
            &filter,
            AccountOperation::SearchMessages,
        ) {
            Err(err) => err,
            Ok(_) => panic!("OR with In must be rejected"),
        };
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );

        // An `In` inside an `And` is still accepted (the folder narrows
        // the whole conjunction - defensible).
        let filter = SearchFilter::And(vec![
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::From("a@b.test".to_owned()),
        ]);
        let part = match criteria_from_filter(
            &super::super::DavScopeIndex::default(),
            &filter,
            AccountOperation::SearchMessages,
        ) {
            Ok(part) => part,
            Err(_) => panic!("AND with In is accepted"),
        };
        assert_eq!(
            part.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );
    }

    #[test]
    fn folder_role_prefers_special_use_over_the_folder_name() {
        // A localized mailbox is only recognizable by its SPECIAL-USE
        // attribute, and a mislabelled name must not beat the attribute.
        assert_eq!(
            folder_role(&[MailboxAttribute::Sent], "Gesendete Objekte", None),
            Some(FolderRole::Sent)
        );
        assert_eq!(
            folder_role(&[MailboxAttribute::Junk], "Archive", None),
            Some(FolderRole::Spam),
            "the attribute wins over the name",
        );
        assert_eq!(
            folder_role(
                &[MailboxAttribute::from_imap_str("\\INBOX")],
                "Posteingang",
                None,
            ),
            None,
            "an Inbox attribute cannot make another name the protocol INBOX",
        );
    }

    #[test]
    fn folder_role_falls_back_to_the_slash_delimited_leaf_name() {
        assert_eq!(
            folder_role(&[], "INBOX", Some('/')),
            Some(FolderRole::Inbox)
        );
        assert_eq!(
            folder_role(&[], "inbox", Some('/')),
            Some(FolderRole::Inbox)
        );
        assert_eq!(
            folder_role(&[], "[Gmail]/Sent Mail", Some('/')),
            Some(FolderRole::Sent)
        );
        assert_eq!(
            folder_role(&[], "Deleted Messages", Some('/')),
            Some(FolderRole::Trash)
        );
        assert_eq!(folder_role(&[], "Projects/Q3", Some('/')), None);
        assert_eq!(
            folder_role(&[MailboxAttribute::HasChildren], "Whatever", Some('/')),
            None,
            "a non-special-use attribute contributes no role",
        );
    }

    #[test]
    fn folder_role_uses_the_servers_hierarchy_delimiter() {
        assert_eq!(
            folder_role(&[], "INBOX.Sent", Some('.')),
            Some(FolderRole::Sent)
        );
        assert_eq!(
            folder_role(&[], "INBOX.Trash", Some('.')),
            Some(FolderRole::Trash)
        );
        assert_eq!(
            folder_role(&[], "INBOX/Sent", Some('/')),
            Some(FolderRole::Sent)
        );
        assert_eq!(
            folder_role(&[], "INBOX/Sent", None),
            None,
            "a NIL delimiter is a flat namespace: no guessed slash hierarchy",
        );
        assert_eq!(
            folder_role(&[], "Sent", None),
            Some(FolderRole::Sent),
            "a flat name that is itself a role name still resolves",
        );
    }

    fn role_entry(
        name: &str,
        delimiter: Option<char>,
        attributes: Vec<MailboxAttribute>,
    ) -> std::sync::Arc<crate::account::folder_registry::FolderEntry> {
        role_entry_with_name(
            MailboxName::new(name).expect("valid mailbox"),
            delimiter,
            attributes,
        )
    }

    fn role_entry_with_name(
        name: MailboxName,
        delimiter: Option<char>,
        attributes: Vec<MailboxAttribute>,
    ) -> std::sync::Arc<crate::account::folder_registry::FolderEntry> {
        std::sync::Arc::new(crate::account::folder_registry::FolderEntry::from_mailbox(
            MailboxInfo {
                name,
                delimiter,
                attributes,
                ..Default::default()
            },
        ))
    }

    #[test]
    fn role_and_quota_folder_choices_are_deterministic() {
        let entries = vec![
            role_entry("Z/Sent", Some('/'), Vec::new()),
            role_entry("A/Sent", Some('/'), Vec::new()),
            role_entry("Special", Some('/'), vec![MailboxAttribute::Sent]),
        ];
        assert_eq!(
            role_folder_from_entries(entries, FolderRole::Sent)
                .as_ref()
                .map(MailboxName::as_str),
            Some("Special"),
            "SPECIAL-USE wins over lexical name fallback",
        );

        let entries = vec![
            role_entry("Zeta", Some('/'), Vec::new()),
            role_entry("Hidden", Some('/'), vec![MailboxAttribute::NoSelect]),
            role_entry("Archive", Some('/'), Vec::new()),
        ];
        assert_eq!(
            first_selectable_folder(entries)
                .as_ref()
                .map(MailboxName::as_str),
            Some("Archive"),
        );
    }

    #[test]
    fn quota_fallback_uses_only_personal_selectable_folders() {
        let mut shared = role_entry("A-shared", Some('/'), Vec::new());
        std::sync::Arc::get_mut(&mut shared).unwrap().shared_owner =
            Some(bifrost_types::MailboxId("colleague".into()));
        assert!(first_selectable_folder(vec![std::sync::Arc::clone(&shared)]).is_none());
        assert_eq!(
            first_selectable_folder(vec![
                shared,
                role_entry("Z-personal", Some('/'), Vec::new()),
            ])
            .unwrap()
            .as_str(),
            "Z-personal",
        );
    }

    #[test]
    fn role_folder_ties_use_wire_octets_and_mode_after_display_name() {
        let canonical = MailboxName::new("a").expect("valid mailbox");
        let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
        for names in [
            [canonical.clone(), noncanonical.clone()],
            [noncanonical.clone(), canonical.clone()],
        ] {
            let selected = role_folder_from_entries(
                names
                    .into_iter()
                    .map(|name| role_entry_with_name(name, Some('/'), vec![MailboxAttribute::Sent]))
                    .collect(),
                FolderRole::Sent,
            )
            .expect("a Sent folder");
            assert_eq!(selected, noncanonical);
        }

        // The same malformed octets can decode to the same replacement
        // character in either mode. The mode is the final identity tie-break.
        let legacy = MailboxName::from_wire("\u{fffd}".into(), b"\xff", false);
        let utf8 = MailboxName::from_wire("\u{fffd}".into(), b"\xff", true);
        for names in [
            [utf8.clone(), legacy.clone()],
            [legacy.clone(), utf8.clone()],
        ] {
            let selected = role_folder_from_entries(
                names
                    .into_iter()
                    .map(|name| role_entry_with_name(name, Some('/'), vec![MailboxAttribute::Sent]))
                    .collect(),
                FolderRole::Sent,
            )
            .expect("a Sent folder");
            assert_eq!(selected, legacy);
        }
    }

    #[test]
    fn quota_fallback_ties_use_wire_identity_after_display_name() {
        let canonical = MailboxName::new("a").expect("valid mailbox");
        let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
        for names in [
            [canonical.clone(), noncanonical.clone()],
            [noncanonical.clone(), canonical.clone()],
        ] {
            let selected = first_selectable_folder(
                names
                    .into_iter()
                    .map(|name| role_entry_with_name(name, Some('/'), Vec::new()))
                    .collect(),
            )
            .expect("a selectable folder");
            assert_eq!(selected, noncanonical);
        }
    }

    #[test]
    fn parent_id_needs_the_servers_delimiter() {
        assert_eq!(
            parent_id(Some('/'), "Projects/Q3"),
            Some(ContainerId("Projects".to_owned()))
        );
        assert_eq!(
            parent_id(Some('.'), "INBOX.Projects.Q3"),
            Some(ContainerId("INBOX.Projects".to_owned())),
            "the delimiter is taken from LIST, not assumed",
        );
        assert_eq!(parent_id(Some('/'), "INBOX"), None, "a root has no parent");
        assert_eq!(
            parent_id(None, "Projects/Q3"),
            None,
            "a flat server has no hierarchy at all",
        );
    }

    #[test]
    fn convenience_keywords_map_onto_system_flags() {
        assert_eq!(imap_flag_for_keyword("$flagged"), Flag::Flagged);
        assert_eq!(imap_flag_for_keyword("$FLAGGED"), Flag::Flagged);
        assert_eq!(imap_flag_for_keyword("\\Flagged"), Flag::Flagged);
        assert_eq!(imap_flag_for_keyword("$answered"), Flag::Answered);
        assert_eq!(imap_flag_for_keyword("$seen"), Flag::Seen);
        // Everything else stays a keyword, verbatim: `$forwarded` and
        // `$MDNSent` have no system-flag equivalent.
        assert_eq!(
            imap_flag_for_keyword("$MDNSent"),
            Flag::from_imap_str("$MDNSent")
        );
        assert_eq!(
            imap_flag_for_keyword(IMPORTANCE_KEYWORD),
            Flag::from_imap_str("$important")
        );
    }

    fn search_request(limit: Option<u32>, cursor: Option<&str>) -> SearchRequest {
        let mut request = SearchRequest::default();
        request.limit = limit;
        request.page_cursor = cursor.map(|c| c.as_bytes().to_vec().into());
        request
    }

    /// One page call over `folders` (each `(name, uidvalidity, uids)`, in
    /// walk order), driven the way `search_messages` drives the pager.
    fn search_page(
        folders: &[(&str, u32, &[u32])],
        restriction: Option<&str>,
        limit: u32,
        cursor: Option<bifrost_types::PageCursor>,
    ) -> Result<Page<(String, u32)>, AccountError> {
        let mut request = search_request(Some(limit), None);
        request.page_cursor = cursor;
        let restriction = restriction.map(|name| MailboxName::new(name).unwrap());
        let mut pager = SearchPager::new(
            &request,
            restriction.as_ref(),
            AccountOperation::SearchMessages,
        )?;
        let names = folders
            .iter()
            .map(|(name, _, _)| MailboxName::new(*name).unwrap())
            .collect();
        for folder in pager.folders(names) {
            if pager.is_full() {
                break;
            }
            let (_, uidvalidity, uids) = folders
                .iter()
                .find(|(name, _, _)| *name == folder.as_str())
                .unwrap();
            let entries = uids.iter().map(|uid| (*uid, *uid)).collect();
            pager.accept(&folder, *uidvalidity, entries, |uid| {
                (folder.as_str().to_owned(), uid)
            })?;
        }
        Ok(pager.finish())
    }

    fn uids_of(page: &Page<(String, u32)>) -> Vec<u32> {
        page.items.iter().map(|(_, uid)| *uid).collect()
    }

    #[test]
    fn a_search_walk_does_not_skip_a_survivor_when_an_earlier_result_is_expunged() {
        let first =
            search_page(&[("INBOX", 7, &[101, 102, 103, 104])], None, 2, None).expect("first page");
        assert_eq!(uids_of(&first), vec![101, 102]);
        assert_eq!(first.estimated_total, None, "the walk was not enumerated");

        // 101 is expunged before the next page is asked for. An offset of 2
        // into the fresh [102, 103, 104] would hand back 104 and lose 103.
        let second = search_page(
            &[("INBOX", 7, &[102, 103, 104])],
            None,
            2,
            first.next_cursor,
        )
        .expect("second page");
        assert_eq!(uids_of(&second), vec![103, 104]);
        assert_eq!(second.next_cursor, None, "the final page ends the walk");
    }

    #[test]
    fn a_search_walk_crosses_folders_in_order_and_skips_folders_it_passed() {
        let folders: &[(&str, u32, &[u32])] =
            &[("A", 1, &[3, 1]), ("B", 2, &[5]), ("C", 3, &[9, 8])];
        let first = search_page(folders, None, 3, None).expect("first page");
        assert_eq!(
            first.items,
            vec![
                ("A".to_owned(), 1),
                ("A".to_owned(), 3),
                ("B".to_owned(), 5)
            ],
            "a folder's results are walked in UID order"
        );
        // The cursor now stands in B, so folder A changing UIDVALIDITY is
        // not observed: passed folders are not searched again.
        let passed: &[(&str, u32, &[u32])] = &[("A", 99, &[1]), ("B", 2, &[5]), ("C", 3, &[9, 8])];
        let second = search_page(passed, None, 1, first.next_cursor).expect("second page");
        assert_eq!(second.items, vec![("C".to_owned(), 8)]);
        let third = search_page(passed, None, 1, second.next_cursor).expect("third page");
        assert_eq!(third.items, vec![("C".to_owned(), 9)]);
        assert_eq!(third.next_cursor, None);
        assert_eq!(third.estimated_total, None, "a resumed call saw a suffix");

        let whole = search_page(folders, None, 10, None).expect("one page");
        assert_eq!(whole.next_cursor, None);
        assert_eq!(whole.estimated_total, Some(5));
    }

    #[test]
    fn a_search_cursor_whose_folder_vanished_resumes_at_the_next_folder() {
        let first =
            search_page(&[("A", 1, &[1, 2]), ("C", 3, &[4])], None, 1, None).expect("first page");
        assert_eq!(uids_of(&first), vec![1]);
        let second =
            search_page(&[("C", 3, &[4])], None, 5, first.next_cursor).expect("second page");
        assert_eq!(uids_of(&second), vec![4]);
    }

    #[test]
    fn a_changed_uidvalidity_in_the_cursor_folder_voids_the_walk() {
        let first = search_page(&[("INBOX", 7, &[1, 2])], None, 1, None).expect("first page");
        let err = search_page(&[("INBOX", 8, &[1, 2])], None, 1, first.next_cursor)
            .expect_err("the cursor's UIDs name nothing under a new UIDVALIDITY");
        assert_eq!(*err.kind(), AccountErrorKind::ConcurrencyConflict);
        assert_eq!(err.operation(), Some(AccountOperation::SearchMessages));
    }

    #[test]
    fn a_search_page_refuses_cursors_and_limits_it_cannot_honour() {
        let malformed = AccountErrorKind::Request(RequestErrorKind::Malformed);
        let is_page_cursor_fault = |err: &AccountError| {
            matches!(
                err.chain().outermost(),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("page_cursor"),
                    ..
                })
            )
        };
        // The retired offset spelling, garbage, and a legacy imap1 payload.
        for cursor in ["2", "not-a-number", "imapsearch1:imap1:5:INBOX:7:9"] {
            let err = search_page(&[], None, 2, Some(cursor.as_bytes().to_vec().into()))
                .expect_err("a cursor this crate did not mint");
            assert_eq!(*err.kind(), malformed, "{cursor}");
            assert!(is_page_cursor_fault(&err), "{cursor}");
        }
        let err = search_page(&[], None, 0, None).expect_err("a zero limit");
        assert_eq!(*err.kind(), malformed);
        assert!(
            !is_page_cursor_fault(&err),
            "a zero limit is not a cursor fault"
        );

        let first = search_page(&[("A", 1, &[1, 2])], Some("A"), 1, None).expect("first page");
        let err = search_page(&[("B", 1, &[1, 2])], Some("B"), 1, first.next_cursor)
            .expect_err("a cursor from another folder restriction");
        assert_eq!(*err.kind(), malformed);
        assert!(is_page_cursor_fault(&err));
    }

    #[test]
    fn the_search_cursor_encoding_is_frozen() {
        let position = SearchPosition {
            folder: MailboxName::new("INBOX").unwrap(),
            uidvalidity: 7,
            key: 9,
        };
        let bytes = position.encode();
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            format!("imapsearch1:{}", encode_object_id(&position.folder, 7, 9).0)
        );
        assert_eq!(
            SearchPosition::decode(&bytes, AccountOperation::Search).unwrap(),
            position
        );
    }

    #[test]
    fn imap_date_uses_the_rfc3501_date_text_production() {
        // date-text = date-day "-" date-month "-" date-year, and
        // date-day is 1*2DIGIT, so a single-digit day is not zero-padded.
        assert_eq!(imap_date(SystemTime::UNIX_EPOCH), "1-Jan-1970");
        assert_eq!(
            imap_date(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(60 * 60 * 24 * 364)),
            "31-Dec-1970"
        );
    }

    #[test]
    fn search_plan_defaults_to_all_and_carries_the_folder_restriction() {
        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::default(),
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(plan.criteria, "ALL");
        assert!(plan.folder.is_none());

        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::filter(SearchFilter::In(ContainerId("Archive".to_owned()))),
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(plan.criteria, "ALL", "In contributes no criteria");
        assert_eq!(
            plan.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );
    }

    #[test]
    fn search_plan_quotes_filter_operands() {
        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::filter(SearchFilter::Subject("quarterly \"report\"".to_owned())),
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(plan.criteria, "SUBJECT \"quarterly \\\"report\\\"\"");
    }

    #[test]
    fn search_plan_substitutes_or_conjoins_the_provider_query() {
        // With no structured filter the raw query replaces the ALL.
        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::provider("  UNSEEN  "),
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(plan.criteria, "UNSEEN", "the raw query is trimmed");

        // With a structured filter the two are juxtaposed, which is
        // IMAP's implicit AND (RFC 3501 Section 6.4.4).
        let mut request = SearchRequest::filter(SearchFilter::From("a@b.test".to_owned()));
        request.provider_query = Some("UNSEEN".to_owned());
        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &request,
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(plan.criteria, "FROM \"a@b.test\" UNSEEN");

        // A whitespace-only raw query is ignored entirely.
        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::provider("   "),
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(plan.criteria, "ALL");
    }

    #[test]
    fn search_plan_rejects_structurally_unsafe_provider_queries() {
        for raw in [")", "(UNSEEN", "SUBJECT \"unterminated", "BODY {4}"] {
            let err = search_plan(
                &super::super::DavScopeIndex::default(),
                &SearchRequest::provider(raw),
                AccountOperation::SearchMessages,
            )
            .expect_err("malformed query");
            assert_eq!(
                *err.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            );
        }
        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::provider("OR (FROM \"a\\\\\\\"b\") (SUBJECT \"{4}\")"),
            AccountOperation::SearchMessages,
        )
        .expect("balanced query");
        assert_eq!(plan.criteria, "OR (FROM \"a\\\\\\\"b\") (SUBJECT \"{4}\")");
        let plan = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::provider("FROM"),
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(
            plan.criteria, "FROM",
            "the structural guard does not attempt operand-arity parsing"
        );
    }

    #[test]
    fn and_and_or_combinators_produce_the_expected_criteria_shapes() {
        let and = combine_and(
            &super::super::DavScopeIndex::default(),
            &[
                SearchFilter::From("a@b.test".to_owned()),
                SearchFilter::Subject("hi".to_owned()),
            ],
            AccountOperation::SearchMessages,
        )
        .expect("and");
        assert_eq!(and.criteria, "FROM \"a@b.test\" SUBJECT \"hi\"");

        // An `In`-only conjunction degenerates to ALL plus the folder.
        let folder_only = combine_and(
            &super::super::DavScopeIndex::default(),
            &[SearchFilter::In(ContainerId("Archive".to_owned()))],
            AccountOperation::SearchMessages,
        )
        .expect("and");
        assert_eq!(folder_only.criteria, "ALL");
        assert_eq!(
            folder_only.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );

        // RFC 3501 OR takes exactly two search-keys, so a three-way OR
        // has to nest.
        let or = combine_or(
            &super::super::DavScopeIndex::default(),
            &[
                SearchFilter::From("a@b.test".to_owned()),
                SearchFilter::From("c@d.test".to_owned()),
                SearchFilter::From("e@f.test".to_owned()),
            ],
            AccountOperation::SearchMessages,
        )
        .expect("or");
        assert_eq!(
            or.criteria,
            "OR (OR (FROM \"a@b.test\") (FROM \"c@d.test\")) (FROM \"e@f.test\")"
        );

        assert_eq!(
            combine_or(
                &super::super::DavScopeIndex::default(),
                &[],
                AccountOperation::SearchMessages
            )
            .expect("empty or")
            .criteria,
            "ALL"
        );
    }

    #[test]
    fn and_across_two_folders_is_rejected_rather_than_silently_narrowed() {
        // IMAP SEARCH runs against one selected mailbox, so a filter
        // naming two folders has no faithful single-command encoding.
        let err = combine_and(
            &super::super::DavScopeIndex::default(),
            &[
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::In(ContainerId("Sent".to_owned())),
            ],
            AccountOperation::SearchMessages,
        )
        .expect_err("two folders");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
        // The same folder twice is not a conflict.
        assert!(
            combine_and(
                &super::super::DavScopeIndex::default(),
                &[
                    SearchFilter::In(ContainerId("Archive".to_owned())),
                    SearchFilter::In(ContainerId("Archive".to_owned())),
                ],
                AccountOperation::SearchMessages,
            )
            .is_ok()
        );
    }

    #[test]
    fn not_wraps_its_inner_criteria_and_keeps_the_folder() {
        let part = criteria_from_filter(
            &super::super::DavScopeIndex::default(),
            &SearchFilter::Not(Box::new(SearchFilter::And(vec![
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::From("a@b.test".to_owned()),
            ]))),
            AccountOperation::SearchMessages,
        )
        .expect("not");
        assert_eq!(part.criteria, "NOT (FROM \"a@b.test\")");
        assert_eq!(
            part.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );
    }

    #[test]
    fn header_name_matching_is_trimmed_and_case_insensitive() {
        assert!(header_name_is(b"Bcc: x@y", b"bcc"));
        assert!(header_name_is(b"BCC:x@y", b"bcc"));
        assert!(header_name_is(b" bcc : x@y", b"bcc"));
        assert!(!header_name_is(b"Bcc-Original: x@y", b"bcc"));
        assert!(!header_name_is(b"X-Original-Bcc: x@y", b"bcc"));
        assert!(!header_name_is(b"Bcc x@y", b"bcc"), "no colon, no header");
    }

    fn fixed_now() -> jiff::Timestamp {
        "2026-11-05T12:00:00Z".parse().expect("timestamp")
    }

    #[test]
    fn restamp_replaces_every_date_field_and_keeps_lookalikes() {
        let raw = b"Date: Mon, 2 Nov 2026 09:00:00 +0000\r\nTo: a@b.test\r\nDATE: Tue,\r\n 3 Nov 2026 09:00:00 +0000\r\nResent-Date: kept\r\nX-Date: kept\r\n\r\nDate: in the body\r\n";
        let out =
            restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("restamped");
        let text = String::from_utf8(out).expect("ascii");
        assert_eq!(
            text,
            "Date: Thu, 5 Nov 2026 12:00:00 +0000\r\nTo: a@b.test\r\nResent-Date: kept\r\nX-Date: kept\r\n\r\nDate: in the body\r\n"
        );
    }

    #[test]
    fn restamp_inserts_a_date_into_a_draft_without_one() {
        let raw = b"To: a@b.test\nSubject: hi\n\nbody\n";
        let out =
            restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("restamped");
        assert_eq!(
            out,
            b"Date: Thu, 5 Nov 2026 12:00:00 +0000\nTo: a@b.test\nSubject: hi\n\nbody\n"
        );
    }

    #[test]
    fn restamp_handles_an_unterminated_header_only_draft() {
        let raw = b"To: a@b.test\r\nDate: old";
        let out =
            restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("restamped");
        assert_eq!(
            out,
            b"Date: Thu, 5 Nov 2026 12:00:00 +0000\r\nTo: a@b.test\r\n\r\n"
        );
    }

    #[test]
    fn restamp_leaves_a_dkim_signed_draft_verbatim() {
        let raw = b"DKIM-Signature: v=1; h=date:from\r\nDate: old\r\nTo: a@b.test\r\n\r\nbody";
        let out =
            restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("verbatim");
        assert_eq!(out, raw);
    }

    #[test]
    fn restamp_rejects_a_leading_continuation_line() {
        let raw = b" bcc: secret@example.test\r\nTo: a@b.test\r\n\r\nbody";
        let err = restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend)
            .expect_err("orphan continuation");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn strip_bcc_drops_the_folded_continuation_lines_too() {
        // A folded `Bcc:` whose continuation lines were kept would leak
        // the blind recipients as a dangling header fragment.
        let header = b"To: a@b.test\r\nBcc: x@y.test,\r\n\tz@w.test\r\nSubject: hi\r\n";
        let out = strip_bcc_header(header, b"body");
        let text = String::from_utf8(out).expect("ascii");
        assert!(!text.to_ascii_lowercase().contains("bcc"));
        assert!(
            !text.contains("z@w.test"),
            "the fold must go with its header"
        );
        assert!(text.contains("To: a@b.test"));
        assert!(text.contains("Subject: hi"));
        assert!(text.ends_with("\r\n\r\nbody"));
    }

    #[test]
    fn draft_send_collects_folded_repeated_headers_in_order() {
        let raw = b"To: a@b.test,\r\n  c@d.test\r\nTo: e@f.test\r\n\r\nbody";
        let parsed = parse_draft_for_submission(
            raw,
            &bifrost_types::Address::bare("sender@test"),
            AccountOperation::DraftSend,
        )
        .expect("parses");
        let addresses: Vec<_> = parsed
            .envelope
            .recipients
            .iter()
            .map(|address| address.address.as_str())
            .collect();
        assert_eq!(addresses, ["a@b.test", "c@d.test", "e@f.test"]);
    }

    #[test]
    fn hydration_attrs_track_the_projection() {
        for projection in [
            HydrationProjection::Headers,
            HydrationProjection::Preview(64),
            HydrationProjection::Full,
            HydrationProjection::FullWithBlobs,
        ] {
            let attrs = attrs_for_hydration(projection);
            assert!(attrs.iter().any(|attr| matches!(attr, FetchAttr::Envelope)));
            assert!(attrs.iter().any(|attr| matches!(attr, FetchAttr::Uid)));
        }

        assert!(
            attrs_for_hydration(HydrationProjection::Headers)
                .iter()
                .all(|attr| !matches!(attr, FetchAttr::BodySection { .. })),
            "a headers-only hydration must not pull a body",
        );

        let preview = attrs_for_hydration(HydrationProjection::Preview(64));
        assert!(preview.iter().any(|attr| matches!(
            attr,
            FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: Some((0, PREVIEW_FETCH_BYTES)),
            }
        )));

        let full = attrs_for_hydration(HydrationProjection::Full);
        assert!(full.iter().any(|attr| matches!(
            attr,
            FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            }
        )));
    }

    #[test]
    fn hydrated_message_maps_importance_from_the_important_keyword() {
        let with = crate::types::FetchResponse {
            uid: Some(2),
            flags: Some(vec![Flag::from_imap_str("$Important")]),
            ..Default::default()
        };
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let message =
            fetch_to_message(&folder, 9, with, HydrationProjection::Headers).expect("uid present");
        assert_eq!(message.importance, Importance::High);
        assert_eq!(message.containers, vec![ContainerId("INBOX".to_owned())]);
        assert!(
            message.body_text.is_none(),
            "a headers projection carries no body"
        );

        let without = crate::types::FetchResponse {
            uid: Some(2),
            ..Default::default()
        };
        let message = fetch_to_message(&folder, 9, without, HydrationProjection::Headers)
            .expect("uid present");
        assert_eq!(message.importance, Importance::Normal);
    }

    #[test]
    fn full_hydration_decodes_multipart_into_text_html_and_attachments() {
        let fetch = crate::types::FetchResponse {
            uid: Some(2),
            body_sections: vec![crate::types::fetch::BodySection {
                section: String::new(),
                origin: None,
                data: Some(b"Subject: hi\r\nDate: Fri, 21 Nov 1997 09:55:06 -0600\r\nReferences: <a@test> <b@test>\r\nContent-Type: multipart/mixed; boundary=mix\r\n\r\n--mix\r\nContent-Type: multipart/alternative; boundary=alt\r\n\r\n--alt\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\ncaf=C3=A9\r\n--alt\r\nContent-Type: text/html\r\nContent-Transfer-Encoding: base64\r\n\r\nPGI+aHRtbDwvYj4=\r\n--alt--\r\n--mix\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename*=utf-8''report%20%C3%A6.pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0=\r\n--mix--\r\n".to_vec().into()),
            }],
            ..Default::default()
        };
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let message = fetch_to_message(&folder, 9, fetch, HydrationProjection::FullWithBlobs)
            .expect("uid present");
        assert_eq!(message.body_text.as_deref(), Some("café"));
        assert_eq!(message.body_html.as_deref(), Some("<b>html</b>"));
        assert_eq!(message.references, ["a@test", "b@test"]);
        assert!(message.date.is_some());
        assert_eq!(message.attachments.len(), 1);
        let attachment = &message.attachments[0];
        assert_eq!(attachment.filename.as_deref(), Some("report æ.pdf"));
        assert_eq!(attachment.content_type.as_deref(), Some("application/pdf"));
        assert!(
            matches!(&attachment.source, AttachmentSource::Inline(bytes) if bytes.as_ref() == b"%PDF-")
        );
    }

    #[test]
    fn preview_projection_extracts_text_from_a_multipart_prefix() {
        let fetch = crate::types::FetchResponse { uid: Some(2), body_sections: vec![crate::types::fetch::BodySection { section: String::new(), origin: Some(0), data: Some(b"Content-Type: multipart/alternative; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\npreview prose\r\n--x--\r\n".to_vec().into()) }], ..Default::default() };
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let message = fetch_to_message(&folder, 9, fetch, HydrationProjection::Preview(200))
            .expect("uid present");
        assert_eq!(message.body_text.as_deref(), Some("preview prose"));
        assert!(
            message
                .body_text
                .as_deref()
                .is_none_or(|body| !body.contains("--x"))
        );
        assert!(message.attachments.is_empty());
    }

    #[test]
    fn preview_projection_requests_at_least_the_caller_limit() {
        assert!(
            attrs_for_hydration(HydrationProjection::Preview(256 * 1024))
                .iter()
                .any(|attr| matches!(
                    attr,
                    FetchAttr::BodySection {
                        partial: Some((0, 262_144)),
                        ..
                    }
                ))
        );
    }

    #[test]
    fn truncated_fetch_marks_the_message_incomplete() {
        let fetch = crate::types::FetchResponse { uid: Some(2), body_sections: vec![crate::types::fetch::BodySection { section: String::new(), origin: None, data: Some(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: application/octet-stream\r\n\r\nprefix".to_vec().into()) }], ..Default::default() };
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let message = fetch_to_message(&folder, 9, fetch, HydrationProjection::FullWithBlobs)
            .expect("uid present");
        assert!(message.incomplete);
        assert_eq!(message.attachments.len(), 1);
        assert!(message.attachments[0].truncated);
    }

    #[test]
    fn parse_address_list_respects_quoted_commas() {
        // A display name this crate's own assembler quotes (`"Last, First"`)
        // must not split into a phantom recipient.
        let parsed =
            bifrost_types::Address::parse_list("\"Doe, Jane\" <jane@to.test>, bob@to.test");
        let addrs: Vec<&str> = parsed.iter().map(|a| a.address.as_str()).collect();
        assert_eq!(addrs, ["jane@to.test", "bob@to.test"]);
        assert_eq!(parsed[0].name.as_deref(), Some("Doe, Jane"));
    }
}

#[cfg(test)]
mod partial_commit_tests {
    use super::super::folder_registry::FolderRegistry;
    use super::super::test_support::scripted_account_parts;
    use super::*;
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, tag_of,
    };
    use crate::types::MailboxInfo;
    #[tokio::test]
    async fn fetched_server_thread_handles_resolve_current_members() {
        for (objectid, gmail, expected) in [
            (
                Some("T-objectid"),
                Some(1234),
                "UID SEARCH THREADID T-objectid",
            ),
            (None, Some(1234), "UID SEARCH X-GM-THRID 1234"),
        ] {
            let (conn, mut server) =
                driver_pair(&preauth_greeting("IMAP4rev1 OBJECTID X-GM-EXT-1")).await;
            let account = ImapAccount::new(scripted_account_parts(conn, 1));
            let folder = MailboxName::new("INBOX").unwrap();
            account
                .folders
                .replace_personal(vec![crate::types::MailboxInfo {
                    name: folder.clone(),
                    ..Default::default()
                }]);
            let fetch = crate::types::FetchResponse {
                uid: Some(1),
                flags: Some(Vec::new()),
                thread_id: objectid.map(str::to_owned),
                gmail_thread_id: gmail,
                ..Default::default()
            };
            let inventory =
                super::super::inventory::fetch_to_inventory(&folder, 7, fetch.clone(), None)
                    .unwrap();
            // The same conversation seen in another folder and epoch carries
            // the same account-scoped id, so a consumer can group on it.
            let elsewhere = super::super::inventory::fetch_to_inventory(
                &MailboxName::new("Sent").unwrap(),
                42,
                fetch.clone(),
                None,
            )
            .unwrap();
            let message =
                fetch_to_message(&folder, 7, fetch, HydrationProjection::Headers).unwrap();
            assert_eq!(inventory.thread_id, message.thread_id);
            assert_eq!(elsewhere.thread_id, message.thread_id);
            let thread = message.thread_id.unwrap();
            let script = tokio::spawn(async move {
                let select = read_line(&mut server).await;
                respond(&mut server, &format!(
                    "* 2 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",
                    tag_of(&select),
                )).await;
                let search = read_line(&mut server).await;
                assert!(search.contains(expected), "unexpected command: {search}");
                respond(
                    &mut server,
                    &format!("* SEARCH 1 9\r\n{} OK searched\r\n", tag_of(&search),),
                )
                .await;
                server
            });
            let ids = resolved_targets(
                &account,
                &MutationTarget::Thread(thread),
                ThreadScope::Account,
                AccountOperation::SetIsRead,
            )
            .await
            .unwrap();
            assert_eq!(ids.iter().map(|id| id.uid).collect::<Vec<_>>(), [1, 9]);
            assert!(
                ids.iter()
                    .all(|id| id.folder == folder && id.uidvalidity == 7)
            );
            let _server = script.await.unwrap();
        }
    }

    #[test]
    fn native_thread_folders_cover_the_conversation_once() {
        use super::super::envelope::NativeThreadKind;
        use crate::types::MailboxAttribute;
        let entry = |name: &str, attributes: Vec<MailboxAttribute>| {
            std::sync::Arc::new(super::super::folder_registry::FolderEntry::from_mailbox(
                crate::types::MailboxInfo {
                    name: MailboxName::new(name).unwrap(),
                    attributes,
                    ..Default::default()
                },
            ))
        };
        let entries = || {
            vec![
                entry("Sent", Vec::new()),
                entry("INBOX", Vec::new()),
                entry("[Gmail]/All Mail", vec![MailboxAttribute::All]),
                entry("[Gmail]", vec![MailboxAttribute::NoSelect]),
            ]
        };
        let names = |folders: Vec<MailboxName>| {
            folders
                .iter()
                .map(|f| f.as_str().to_owned())
                .collect::<Vec<_>>()
        };
        // Gmail: one search in \All, not one per label.
        assert_eq!(
            names(native_thread_folders(
                entries(),
                NativeThreadKind::Gmail,
                ThreadScope::Account
            )),
            ["[Gmail]/All Mail"]
        );
        // OBJECTID: every selectable folder, never a \Noselect parent.
        let all = names(native_thread_folders(
            entries(),
            NativeThreadKind::ObjectId,
            ThreadScope::Account,
        ));
        assert_eq!(all.len(), 3);
        assert!(!all.iter().any(|name| name == "[Gmail]"));
        let inbox = MailboxName::new("INBOX").unwrap();
        let except = names(native_thread_folders(
            entries(),
            NativeThreadKind::ObjectId,
            ThreadScope::Except(&inbox),
        ));
        assert!(!except.iter().any(|name| name == "INBOX"));
        assert_eq!(except.len(), 2);
        assert_eq!(
            names(native_thread_folders(
                entries(),
                NativeThreadKind::Gmail,
                ThreadScope::Only(&inbox)
            )),
            ["INBOX"]
        );
        // Gmail without a listed \All falls back to every selectable folder.
        let no_all = vec![entry("INBOX", Vec::new()), entry("Sent", Vec::new())];
        assert_eq!(
            native_thread_folders(no_all, NativeThreadKind::Gmail, ThreadScope::Account).len(),
            2
        );
    }

    // These transcripts pin the Result adapter at the actual engine boundary:
    // a direct unguarded STORE would fail the command assertion, and expunging
    // the conflicting UID would fail the delete transcript.
    #[tokio::test]
    async fn result_mutations_share_modseq_conflicts_and_committed_subsets() {
        for delete in [false, true] {
            let (conn, mut server) =
                driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS CONDSTORE")).await;
            let mut parts = scripted_account_parts(conn, 1);
            let folder = MailboxName::new("INBOX").unwrap();
            parts.folders = std::sync::Arc::new(FolderRegistry::from_lists(
                vec![MailboxInfo {
                    name: folder.clone(),
                    delimiter: Some('/'),
                    ..Default::default()
                }],
                Vec::new(),
            ));
            for uid in [1, 2] {
                parts.folders.record_modseq(&folder, 7, uid, 99).unwrap();
            }
            let account = ImapAccount::new(parts);
            let script = tokio::spawn(async move {
                let select = read_line(&mut server).await;
                respond(&mut server, &format!(
                    "* 2 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen \\Deleted)\r\n* OK [UIDVALIDITY 7] epoch\r\n* OK [HIGHESTMODSEQ 100] modseq\r\n{} OK selected\r\n",
                    tag_of(&select),
                )).await;
                let store = read_line(&mut server).await;
                assert!(store.contains("UID STORE"), "{store}");
                assert!(store.contains("UNCHANGEDSINCE 99"), "{store}");
                respond(
                    &mut server,
                    &format!("{} OK [MODIFIED 2] partial\r\n", tag_of(&store),),
                )
                .await;
                if delete {
                    let expunge = read_line(&mut server).await;
                    assert!(expunge.contains("UID EXPUNGE 1\r\n"), "{expunge}");
                    respond(&mut server, &format!("{} OK gone\r\n", tag_of(&expunge))).await;
                }
                server
            });
            let ids = [1, 2]
                .into_iter()
                .map(|uid| DecodedObjectId::canonical(folder.clone(), 7, uid))
                .collect();
            let op = if delete {
                AccountOperation::RemoveFromContainer
            } else {
                AccountOperation::SetIsRead
            };
            let result = if delete {
                delete_messages(&account, ids, op).await
            } else {
                set_flag(&account, ids, Flag::Seen, true, op).await
            };
            let failure = match result {
                Err(failure) => failure,
                Ok(_) => panic!("MODIFIED member must fail the Result"),
            };
            let error = folder_progress_error(failure);
            assert_eq!(error.operation(), Some(op));
            assert!(matches!(
                error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ));
            let steps = error.partial_commit().unwrap().steps();
            assert_eq!(steps.len(), 1);
            assert_eq!(
                steps[0].effect(),
                Some(if delete {
                    StepEffect::Deleted
                } else {
                    StepEffect::Updated
                })
            );
            assert_eq!(steps[0].targets().refs.len(), 1);
            assert_eq!(account.folders.modseq(&folder, 7, 1), None);
            assert_eq!(account.folders.modseq(&folder, 7, 2), Some(99));
            let _server = script.await.unwrap();
        }
    }

    #[tokio::test]
    async fn result_copy_preserves_live_epoch_commit_beside_stale_target() {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
        let account = ImapAccount::new(scripted_account_parts(conn, 1));
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            respond(&mut server, &format!(
                "* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",
                tag_of(&select),
            )).await;
            let copy = read_line(&mut server).await;
            assert!(copy.contains("UID COPY 2 "), "{copy}");
            respond(&mut server, &format!("{} OK copied\r\n", tag_of(&copy))).await;
            server
        });
        let folder = MailboxName::new("INBOX").unwrap();
        let result = copy_messages(
            &account,
            vec![
                DecodedObjectId::canonical(folder.clone(), 6, 1),
                DecodedObjectId::canonical(folder.clone(), 6, 3),
                DecodedObjectId::canonical(folder, 7, 2),
            ],
            &MailboxName::new("Archive").unwrap(),
            AccountOperation::AddToContainer,
        )
        .await;
        let failure = match result {
            Err(failure) => failure,
            Ok(_) => panic!("stale target must fail the Result"),
        };
        let error = folder_progress_error(failure);
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        let record = error.partial_commit().unwrap();
        assert_eq!(record.primary().effect, None);
        assert_eq!(record.steps().len(), 2);
        assert!(
            record
                .steps()
                .iter()
                .any(|step| step.effect() == Some(StepEffect::Copied))
        );
        assert!(record.steps().iter().any(|step| step.effect().is_none()));
        assert!(error.recovery().requires_reconciliation());
        let _server = script.await.unwrap();
    }

    #[tokio::test]
    async fn partial_commit_copy_messages_second_folder_failure_keeps_first() {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
        let account = ImapAccount::new(scripted_account_parts(conn, 1));
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&select))).await;
            let copy = read_line(&mut server).await;
            assert!(copy.contains("UID COPY"));
            respond(&mut server, &format!("{} OK copied\r\n", tag_of(&copy))).await;
            let select = read_line(&mut server).await;
            assert!(select.contains("SELECT"));
            respond(
                &mut server,
                &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&select)),
            )
            .await;
            server
        });
        let ids = ["A", "B"]
            .into_iter()
            .map(|name| DecodedObjectId::canonical(MailboxName::new(name).unwrap(), 7, 1))
            .collect();
        let failure = match copy_messages(
            &account,
            ids,
            &MailboxName::new("Archive").unwrap(),
            AccountOperation::AddToContainer,
        )
        .await
        {
            Err(f) => f,
            Ok(_) => panic!("second folder refused"),
        };
        let error = folder_progress_error(failure);
        assert_eq!(error.partial_commit().unwrap().steps().len(), 1);
        assert_eq!(error.partial_commit().unwrap().primary().effect, None);
        assert!(error.recovery().requires_reconciliation());
        let _server = script.await.unwrap();
    }
    async fn composed_failure(delete: bool) -> AccountError {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
        let mut parts = scripted_account_parts(conn, 1);
        parts.capabilities.pim_methods.add_to_container = true;
        parts.capabilities.pim_methods.remove_from_container = true;
        parts.folders =
            std::sync::Arc::new(super::super::folder_registry::FolderRegistry::from_lists(
                ["INBOX", "Trash"]
                    .into_iter()
                    .map(|name| crate::types::MailboxInfo {
                        name: MailboxName::new(name).unwrap(),
                        ..Default::default()
                    })
                    .collect(),
                vec![],
            ));
        let account = ImapAccount::new(parts);
        let script = tokio::spawn(async move {
            loop {
                let line = read_line(&mut server).await;
                if line.contains("SELECT") {
                    respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&line))).await;
                } else if line.contains("UID COPY") || line.contains("NOOP") {
                    respond(&mut server, &format!("{} OK copied\r\n", tag_of(&line))).await;
                } else {
                    assert!(line.contains("UID STORE"), "{line}");
                    respond(
                        &mut server,
                        &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&line)),
                    )
                    .await;
                    break;
                }
            }
            server
        });
        let thread = encode_thread_id(&MailboxName::new("INBOX").unwrap(), 7, &[1]);
        let result = if delete {
            delete_thread(account, thread, Some(ContainerId("INBOX".into()))).await
        } else {
            move_thread(
                account,
                thread,
                ContainerId("Archive".into()),
                Some(ContainerId("INBOX".into())),
            )
            .await
        };
        let error = result.unwrap_err();
        let _server = script.await.unwrap();
        error
    }
    #[tokio::test]
    async fn partial_commit_move_thread_attaches_once_across_legs() {
        let error = composed_failure(false).await;
        assert_eq!(
            error.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::Copied)
        );
    }
    #[tokio::test]
    async fn partial_commit_delete_thread_attaches_once() {
        let error = composed_failure(true).await;
        assert_eq!(error.partial_commit().unwrap().steps().len(), 1);
    }
    /// A bare server (no UIDPLUS, no IMAP4rev2) advertises removal, so
    /// `move_thread` reaches the guarded plain-EXPUNGE fallback. A foreign
    /// `\Deleted` message found by `UID SEARCH DELETED` stops it before any
    /// EXPUNGE, and the failure keeps both the COPY and the MarkedDeleted step.
    #[tokio::test]
    async fn bare_server_move_thread_refuses_expunge_over_foreign_deleted() {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        let mut parts = scripted_account_parts(conn, 1);
        let profile = crate::types::ServerProfile::new(Vec::new(), Vec::new());
        parts.capabilities = super::super::capabilities::build_capabilities(
            &profile,
            &[],
            false,
            None,
            None,
            false,
            false,
            1,
        );
        assert!(parts.capabilities.pim_methods.remove_from_container);
        let account = ImapAccount::new(parts);
        let script = tokio::spawn(async move {
            loop {
                let line = read_line(&mut server).await;
                if line.contains("SELECT") {
                    respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Deleted)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&line))).await;
                } else if line.contains("UID COPY") || line.contains("NOOP") {
                    respond(&mut server, &format!("{} OK copied\r\n", tag_of(&line))).await;
                } else if line.contains("UID STORE") {
                    respond(&mut server, &format!("{} OK stored\r\n", tag_of(&line))).await;
                } else {
                    assert!(line.contains("UID SEARCH DELETED"), "{line}");
                    respond(
                        &mut server,
                        &format!("* SEARCH 1 9\r\n{} OK searched\r\n", tag_of(&line)),
                    )
                    .await;
                    break;
                }
            }
            server
        });
        let thread = encode_thread_id(&MailboxName::new("INBOX").unwrap(), 7, &[1]);
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            move_thread(
                account,
                thread,
                ContainerId("Archive".into()),
                Some(ContainerId("INBOX".into())),
            ),
        )
        .await
        .expect("no EXPUNGE may follow a foreign \\Deleted")
        .unwrap_err();
        let effects: Vec<_> = error
            .partial_commit()
            .unwrap()
            .steps()
            .iter()
            .map(bifrost_types::Step::effect)
            .collect();
        assert!(effects.contains(&Some(StepEffect::Copied)), "{effects:?}");
        assert!(
            effects.contains(&Some(StepEffect::MarkedDeleted)),
            "{effects:?}"
        );
        let _server = script.await.unwrap();
    }
    #[tokio::test]
    async fn partial_commit_draft_discard_expunge_failure_marks_deleted() {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
        let mut parts = scripted_account_parts(conn, 1);
        parts.capabilities.pim_methods.draft_discard = true;
        let account = ImapAccount::new(parts);
        let script = tokio::spawn(async move {
            let line = read_line(&mut server).await;
            respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Deleted)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&line))).await;
            let line = read_line(&mut server).await;
            assert!(line.contains("UID STORE"));
            respond(&mut server, &format!("{} OK stored\r\n", tag_of(&line))).await;
            let line = read_line(&mut server).await;
            assert!(line.contains("UID EXPUNGE"));
            respond(
                &mut server,
                &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&line)),
            )
            .await;
            server
        });
        let id = encode_object_id(&MailboxName::new("Drafts").unwrap(), 7, 1);
        let error = draft_discard(account, DraftHandle(id.0)).await.unwrap_err();
        assert_eq!(
            error.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::MarkedDeleted)
        );
        let _server = script.await.unwrap();
    }
    /// With nothing committed, an uncertain COPY is recorded (it may have
    /// made a copy); a refused COPY and a preparation failure stay plain.
    #[test]
    fn partial_commit_lone_uncertain_write_leg_is_recorded() {
        let folder = MailboxName::new("INBOX").unwrap();
        let progress = |error: AccountError, stage| FolderProgress {
            done: Vec::new(),
            error,
            stage,
            targets: super::super::mutate::folder_targets(&folder, TargetRole::Subject),
            siblings: Vec::new(),
        };
        let leaf = |attempt| {
            super::super::account_error_with(
                crate::Error::timeout().with_attempt(attempt),
                super::super::error::ImapErrorContext::operation(AccountOperation::AddToContainer)
                    .with_folder_scope(&folder),
            )
        };
        let dropped = leaf(bifrost_types::TransmissionState::InFlight);
        assert_eq!(Application::from_leaf(&dropped), Application::Uncertain);
        let error = folder_progress_error(progress(dropped.clone(), Some(StepEffect::Copied)));
        let record = error
            .partial_commit()
            .expect("an uncertain copy is recorded");
        assert_eq!(record.primary().application, Application::Uncertain);
        assert_eq!(record.plan().gating.len(), 1);
        assert!(
            folder_progress_error(progress(dropped, None))
                .partial_commit()
                .is_none()
        );
        let unsent = leaf(bifrost_types::TransmissionState::Unsent);
        assert!(
            folder_progress_error(progress(unsent, Some(StepEffect::Copied)))
                .partial_commit()
                .is_none()
        );
    }
    #[test]
    fn partial_commit_container_create_refresh_failure_is_protected() {
        let leaf = crate::account::error::test_support::unavailable_without_context();
        let error = container_refresh_error(
            leaf,
            StepEffect::Created,
            &MailboxName::new("new").unwrap(),
            None,
        );
        assert_eq!(
            error.partial_commit().unwrap().primary().application,
            Application::NotApplicable
        );
        assert!(
            matches!(error.recovery(),bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions.contains(&bifrost_types::ReconcileAction::ObserveOnly))
        );
    }
}
