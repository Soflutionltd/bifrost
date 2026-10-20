use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, AttachmentSource,
    BlobCapabilities, BlobEncoding, BlobHandle, BlobId, Container, ContainerId, ContainerKind,
    ContainerList, ContainerNamespace, ContainerRights, FolderRole, HydrationProjection,
    Importance, LabelId, Message, MessageAttachment, MutationTarget, ObjectId, Page, Provenance,
    QuotaInfo, SearchFilter, SearchRequest, SkippedScope, ThreadHydration, ThreadId,
    VacationConfig,
};
/// Convert a crate-internal error to `AccountError` with the correct
/// `AccountOperation` for this call site. Every call site in this
/// module passes the operation that produced the error; `Discover` is
/// no longer used as a default.
#[inline]
fn to_acct_err(op: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |err| super::error::into_account_error(err, super::error::JmapErrorContext::new(op))
}

/// Local helper for search-cursor decode failures: the page cursor
/// JMAP stores is opaque, and a malformed value is a schema mismatch
/// from the caller's point of view. Maps to `SyncState(SchemaIncompatible)`.
fn schema_incompatible_search_cursor(op: AccountOperation) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::SchemaIncompatible,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(op)
    .text(bifrost_types::DiagnosticText::support_only(
        "search page cursor was malformed or from an older cursor version",
    ))
    .try_build()
    .expect("valid account error classification")
}
use bytes::Bytes;
use futures::StreamExt;

use super::state_cache::{self, StateMap};
use crate::core::SetCreate;
use crate::core::query;
use crate::core::transport::HttpTransport;
use crate::email::{
    BodyProperty, DRAFT_KEYWORD, Email, EmailAddress as JmapEmailAddress, EmailBodyPart,
    EmailBodyValue, EmailGet, EmailId, EmailPatch, EmailSet, Property as EmailProperty,
};
use crate::email_submission::{Address as SubmissionAddress, EmailSubmissionSet, UndoStatus};
use crate::identity::{IdentityGet, IdentityId as JmapIdentityId, IdentitySet};
use crate::mailbox::{
    Mailbox, MailboxGet, MailboxId, MailboxRights, MailboxSet, Property as MailboxProperty, Role,
};
use crate::quota::{Property as QuotaProperty, QuotaGet};
use crate::thread::{ThreadGet, ThreadId as JmapThreadId};
use crate::vacation_response::{VacationResponseGet, VacationResponseId, VacationResponseSet};

type MailAccount<T> = crate::account::Account<T>;

const SEEN_KEYWORD: &str = "$seen";
const IMPORTANT_KEYWORD: &str = "$important";
const SUBMISSION_CREATE_ID: &str = "submit0";
const ATTACHMENT_HANDLE_PREFIX: &str = "jmap:";

/// Foreign (shared/delegate) submission context selected by the account
/// layer. `mail` already targets the foreign JMAP account.
pub(crate) struct ForeignSubmission {
    pub(crate) mode: bifrost_types::SendAs,
    pub(crate) self_address: Option<bifrost_types::Address>,
}

pub(crate) fn add_to_container<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            &email_states,
            &account_id,
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
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            &email_states,
            &account_id,
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
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    keyword: String,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ids = resolve_target(&mail, target, AccountOperation::SetKeyword).await?;
        let keyword_for_set = keyword.clone();
        let ids_for_set = ids.clone();
        let mut response = send_email_set(
            &mail,
            &email_states,
            &account_id,
            AccountOperation::SetKeyword,
            move || {
                let mut set = EmailSet::new();
                for id in &ids_for_set {
                    set.update(id.clone()).keyword(&keyword_for_set, value);
                }
                set
            },
        )
        .await?;
        for id in &ids {
            response
                .updated(id)
                .map_err(to_acct_err(AccountOperation::SetKeyword))?;
        }
        Ok(())
    })
}

pub(crate) fn set_is_read<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    set_keyword(
        mail,
        email_states,
        account_id,
        target,
        SEEN_KEYWORD.to_string(),
        is_read,
    )
}

/// Exclusive importance overwrite via the `$important` keyword. JMAP's
/// model is two-valued: `High` sets `$important`, `Normal`/`Low` clear
/// it. One `Email/set` keyword update, no expand-into-two.
pub(crate) fn set_importance<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    level: Importance,
) -> AccountFuture<Result<(), AccountError>> {
    set_keyword(
        mail,
        email_states,
        account_id,
        target,
        IMPORTANT_KEYWORD.to_string(),
        importance_sets_important_keyword(level),
    )
}

/// JMAP's two-valued importance mapping: `High` sets `$important`,
/// `Normal`/`Low` clear it.
fn importance_sets_important_keyword(level: Importance) -> bool {
    matches!(level, Importance::High)
}

/// Resolve From/Sender for a foreign submission. `OnBehalfOf` keeps an
/// explicit consumer From for compatibility with Graph; otherwise the
/// foreign identity supplies it. The envelope deliberately follows From.
fn resolve_foreign_headers(
    mode: &bifrost_types::SendAs,
    ident: &bifrost_types::Address,
    consumer_from: Option<bifrost_types::Address>,
    self_address: Option<bifrost_types::Address>,
) -> (bifrost_types::Address, Option<bifrost_types::Address>) {
    match mode {
        bifrost_types::SendAs::As(_) => (ident.clone(), None),
        bifrost_types::SendAs::OnBehalfOf(_) => {
            (consumer_from.unwrap_or_else(|| ident.clone()), self_address)
        }
        _ => (ident.clone(), None),
    }
}

/// A successful exchange can still leave a multi-step write incomplete or
/// its final effect unknown. Replaying the whole operation is unsafe.
fn incomplete_submission(op: AccountOperation, error: AccountError, detail: &str) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Protocol(
            bifrost_types::ProtocolErrorKind::PartialResponse,
        ),
        bifrost_types::Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol: bifrost_types::Protocol::Jmap,
            detail: Some(bifrost_types::DiagnosticText::support_only(detail)),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(op)
    .push_cause(bifrost_types::Cause::Attempt(
        bifrost_types::AttemptCause::new(bifrost_types::TransmissionState::Acknowledged),
    ))
    .text(bifrost_types::DiagnosticText::user_safe(detail))
    .text(bifrost_types::DiagnosticText::support_only(format!(
        "underlying error: {error:?}"
    )))
    .try_build()
    .expect("valid partial submission classification")
}

fn submission_create_error(error: crate::Error) -> (AccountError, bool) {
    // These errors came from the submission handle, so Method names that
    // call and Set came from its notCreated entry. Missing/invalid replies
    // cannot establish rejection, even when the HTTP exchange succeeded.
    let rejected = matches!(&error, crate::Error::Method(_) | crate::Error::Set(_));
    let error = to_acct_err(AccountOperation::Send)(error);
    if rejected {
        (error, true)
    } else {
        (
            incomplete_submission(
                AccountOperation::Send,
                error,
                "Submission outcome is unknown; the source Email was retained. Reconcile before sending again.",
            ),
            false,
        )
    }
}

pub(crate) fn send_message<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    max_delayed_send: usize,
    foreign: Option<ForeignSubmission>,
    mut request: bifrost_types::SendRequest,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let personal_identity = request.identity.clone();
        let identity = if let Some(foreign) = foreign.as_ref() {
            let selected = foreign_sending_identity(&mail, request.identity.as_ref()).await?;
            let ident_address = bifrost_types::Address {
                name: selected.name,
                address: selected.email,
            };
            let (from, sender) = resolve_foreign_headers(
                &foreign.mode,
                &ident_address,
                request.from.take(),
                foreign.self_address.clone(),
            );
            request.from = Some(from);
            Some((JmapIdentityId::new(selected.id), sender))
        } else {
            None
        };
        let scheduled = request.scheduled;
        if let Some(at) = scheduled {
            // A scheduled request on a relay with no delay window
            // (`maxDelayedSend == 0`, the `scheduled_send` flag false) is
            // unsupported, not malformed - mirror the cross-provider
            // contract (gmail/imap surface `Unsupported(Send)` too).
            if max_delayed_send == 0 {
                return Err(super::error::unsupported_error(
                    AccountOperation::Send,
                    None,
                    "JMAP relay advertises no scheduled-send window",
                ));
            }
            // The window source is the server's maxDelayedSend seconds.
            let window = std::time::Duration::from_secs(max_delayed_send as u64);
            bifrost_types::validate_scheduled(at, Some(window))?;
        }
        let envelope_recipients = request
            .to
            .iter()
            .chain(&request.cc)
            .chain(&request.bcc)
            .cloned()
            .collect::<Vec<_>>();
        if scheduled.is_some() && envelope_recipients.is_empty() {
            return Err(scheduled_requires_envelope());
        }
        if scheduled.is_some() && request.from.is_none() && identity.is_none() {
            if let Some(identity_id) = personal_identity.as_ref() {
                let selected = primary_sending_identity(&mail, identity_id).await?;
                request.from = Some(bifrost_types::Address {
                    name: selected.name,
                    address: selected.email,
                });
            } else {
                return Err(scheduled_requires_envelope());
            }
        }
        let envelope_from = request.from.clone();
        if scheduled.is_some() && envelope_from.is_none() {
            // JMAP has no default Identity: only an explicit identity id can
            // supply From when the caller omits it. Do not choose an
            // arbitrary Identity/get row, whose order is server-defined.
            return Err(scheduled_requires_envelope());
        }
        // One batched Mailbox/get for both roles: `fetch_mailboxes` is an
        // uncached full listing, so resolving Drafts and Sent separately
        // would double the wire cost of the hottest write path.
        let mut roles = role_mailboxes(
            &mail,
            &[FolderRole::Drafts, FolderRole::Sent],
            AccountOperation::Send,
        )
        .await?;
        let draft_mailbox = roles
            .remove(&FolderRole::Drafts)
            .ok_or_else(|| missing_role_mailbox(AccountOperation::Send))?;
        let sent_mailbox = if request.save_to_sent == Some(false) {
            None
        } else {
            Some(
                roles
                    .remove(&FolderRole::Sent)
                    .ok_or_else(|| missing_role_mailbox(AccountOperation::Send))?,
            )
        };

        let mut create =
            build_email_create_from_send(&mail, request, draft_mailbox.clone()).await?;
        if let Some((_, Some(sender))) = identity.as_ref() {
            create.sender([address_to_jmap(sender.clone())]);
        }
        let mut email_set = EmailSet::new();
        let email_create_id = email_set.create_item(create);

        let mut submission_set = EmailSubmissionSet::new();
        {
            let submit = submission_set.create_with_id(SUBMISSION_CREATE_ID);
            if let Some((identity, _)) = identity.as_ref() {
                submit.identity_id(identity.clone());
            } else if let Some(identity) = personal_identity.as_ref() {
                submit.identity_id(JmapIdentityId::new(identity.0.clone()));
            }
            if let Some(from) = envelope_from.as_ref()
                && !envelope_recipients.is_empty()
            {
                // RFC 8621 carries SMTP FUTURERELEASE params on the
                // envelope mailFrom address. `holduntil` is the
                // absolute-time form matching our `SystemTime`.
                let mut mail_from = submission_address_from_compose(from);
                if let Some(at) = scheduled {
                    mail_from = mail_from.with_parameter("holduntil", Some(rfc3339(at)));
                }
                submit.envelope(
                    mail_from,
                    envelope_recipients
                        .iter()
                        .map(submission_address_from_compose),
                );
            }
        }

        if let Some(sent) = sent_mailbox.as_ref() {
            submission_set
                .on_success_update_email(SUBMISSION_CREATE_ID)
                .submitted_to_sent(sent, Some(&draft_mailbox));
        } else {
            submission_set = submission_set.on_success_destroy_email(SUBMISSION_CREATE_ID);
        }

        // Capture the per-account email state observed before the set so
        // the post-call advance is compare-and-swap, not an
        // unconditional overwrite. A slow `Email/set` response carrying
        // an older `newState` must not clobber a newer state the
        // `Email/changes` loop has since CAS-set.
        let prior_state = state_cache::get(&email_states, &account_id).await;
        let mut batch = mail.build();
        let email_handle = batch
            .call(email_set)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_ref = email_handle.result_reference(format!("/created/{email_create_id}/id"));
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id_ref(email_ref);
        let submission_handle = batch
            .call(submission_set)
            .map_err(to_acct_err(AccountOperation::Send))?;

        let mut response = batch
            .send()
            .await
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut email_response = response
            .get(&email_handle)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut email = email_response
            .created(&email_create_id)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_id = email.take_id();

        // An absent `newState` is a non-conforming server (RFC 8620 s5.3
        // makes it mandatory) and an empty one names no state; neither is
        // a value to cache, and caching either would hand a later
        // `*/changes` a `sinceState` no server ever issued.
        if let Some(new_state) = email_response.new_state().filter(|state| !state.is_empty()) {
            state_cache::advance(
                &email_states,
                &account_id,
                prior_state.as_deref(),
                new_state.to_string(),
            )
            .await;
        }

        let mut submission = match response
            .get(&submission_handle)
            .and_then(|mut result| result.created(SUBMISSION_CREATE_ID))
            .map_err(submission_create_error)
        {
            Ok(submission) => submission,
            Err((error, rejected)) => {
                if rejected {
                    discard_created_send_email(&mail, &email_states, &account_id, &email_id).await;
                }
                return Err(error);
            }
        };

        // A scheduled send returns its EmailSubmission id for
        // cancel/reschedule. An immediate send keeps returning the email id.
        if scheduled.is_some() {
            Ok(ObjectId(submission.take_id().into_string()))
        } else {
            Ok(ObjectId(email_id.into_string()))
        }
    })
}

pub(crate) fn send_raw_message<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    raw: Bytes,
    save_to_sent: Option<bool>,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        // JMAP has no raw-bytes send. The pre-assembled RFC 5322 / RFC 8098
        // octets are uploaded as a blob, imported into Drafts via
        // `Email/import`, then submitted via `EmailSubmission/set`. The
        // submission omits an explicit envelope, so the server derives
        // MAIL FROM / RCPT TO from the message's own header fields
        // (RFC 8621 §7) - no MIME parse needed on our side.
        // One batched Mailbox/get for both roles (see send_message).
        let mut roles = role_mailboxes(
            &mail,
            &[FolderRole::Drafts, FolderRole::Sent],
            AccountOperation::Send,
        )
        .await?;
        let draft_mailbox = roles
            .remove(&FolderRole::Drafts)
            .ok_or_else(|| missing_role_mailbox(AccountOperation::Send))?;
        let sent_mailbox = if save_to_sent == Some(false) {
            None
        } else {
            Some(
                roles
                    .remove(&FolderRole::Sent)
                    .ok_or_else(|| missing_role_mailbox(AccountOperation::Send))?,
            )
        };

        let blob = mail
            .upload(raw.to_vec(), Some("message/rfc822"))
            .await
            .map_err(to_acct_err(AccountOperation::Send))?;

        let mut import_req = crate::email::import::EmailImportRequest::new();
        let import_create_id = {
            let entry = import_req.email(blob.blob_id);
            entry.mailbox_ids([draft_mailbox.clone()]);
            entry.keywords([DRAFT_KEYWORD]);
            entry.create_id()
        };

        let mut submission_set = EmailSubmissionSet::new();
        submission_set.create_with_id(SUBMISSION_CREATE_ID);
        if let Some(sent) = sent_mailbox.as_ref() {
            submission_set
                .on_success_update_email(SUBMISSION_CREATE_ID)
                .submitted_to_sent(sent, Some(&draft_mailbox));
        } else {
            submission_set = submission_set.on_success_destroy_email(SUBMISSION_CREATE_ID);
        }

        let mut batch = mail.build();
        let import_handle = batch
            .call(import_req)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_ref = import_handle.result_reference(format!("/created/{import_create_id}/id"));
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id_ref(email_ref);
        let submission_handle = batch
            .call(submission_set)
            .map_err(to_acct_err(AccountOperation::Send))?;

        let prior_state = state_cache::get(&email_states, &account_id).await;
        let mut response = batch
            .send()
            .await
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut import_response = response
            .get(&import_handle)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut email = import_response
            .created(&import_create_id)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_id = email.take_id();
        let imported_state = import_response.new_state().to_string();
        if !imported_state.is_empty() {
            state_cache::advance(
                &email_states,
                &account_id,
                prior_state.as_deref(),
                imported_state,
            )
            .await;
        }

        // Surface a submission failure rather than reporting the import id
        // as if the send committed.
        match response
            .get(&submission_handle)
            .and_then(|mut result| result.created(SUBMISSION_CREATE_ID))
            .map_err(submission_create_error)
        {
            Ok(_) => {}
            Err((error, rejected)) => {
                if rejected {
                    discard_created_send_email(&mail, &email_states, &account_id, &email_id).await;
                }
                return Err(error);
            }
        }
        Ok(ObjectId(email_id.into_string()))
    })
}

async fn discard_created_send_email<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    email_id: &EmailId,
) {
    if email_id.as_str().is_empty() {
        return;
    }
    let _ = destroy_emails(
        mail,
        email_states,
        account_id,
        [email_id.clone()],
        AccountOperation::Send,
    )
    .await;
}

pub(crate) fn attachment_upload<T: HttpTransport>(
    mail: MailAccount<T>,
    mut bytes: AccountStream<Result<Bytes, AccountError>>,
    mime: String,
) -> AccountFuture<Result<bifrost_types::AttachmentHandle, AccountError>> {
    Box::pin(async move {
        let mut data = Vec::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk?;
            data.extend_from_slice(&chunk);
        }
        let blob = mail
            .upload(data, Some(&mime))
            .await
            .map_err(to_acct_err(AccountOperation::AttachmentUpload))?;
        Ok(bifrost_types::AttachmentHandle(encode_attachment_handle(
            blob.blob_id.as_str(),
            Some(&mime),
        )))
    })
}

pub(crate) fn draft_create<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    patch: bifrost_types::DraftPatch,
) -> AccountFuture<Result<bifrost_types::DraftHandle, AccountError>> {
    Box::pin(async move {
        let draft_mailbox =
            role_mailbox(&mail, FolderRole::Drafts, AccountOperation::DraftCreate).await?;
        let create = build_email_create_from_draft(
            &mail,
            patch,
            draft_mailbox,
            AccountOperation::DraftCreate,
        )
        .await?;
        let mut set = EmailSet::new();
        let create_id = set.create_item(create);
        // CAS the post-create state against the state seen before the
        // call (see `send_message`) so a slow response cannot clobber a
        // newer state the changes loop already advanced to.
        let prior_state = state_cache::get(&email_states, &account_id).await;
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::DraftCreate))?;
        if let Some(new_state) = response.new_state().filter(|state| !state.is_empty()) {
            state_cache::advance(
                &email_states,
                &account_id,
                prior_state.as_deref(),
                new_state.to_string(),
            )
            .await;
        }
        let mut email = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::DraftCreate))?;
        Ok(bifrost_types::DraftHandle(email.take_id().into_string()))
    })
}

pub(crate) fn draft_update<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    draft: bifrost_types::DraftHandle,
    patch: bifrost_types::DraftPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let email_id = EmailId::new(draft.0);
        let mut email_patch = EmailPatch::default();
        apply_draft_patch_to_email_patch(
            &mail,
            &mut email_patch,
            patch,
            AccountOperation::DraftUpdate,
        )
        .await?;
        let email_id_for_set = email_id.clone();
        let mut response = send_email_set(
            &mail,
            &email_states,
            &account_id,
            AccountOperation::DraftUpdate,
            move || {
                let mut set = EmailSet::new();
                set.update_item(email_id_for_set.clone(), email_patch.clone());
                set
            },
        )
        .await?;
        response
            .updated(&email_id)
            .map_err(to_acct_err(AccountOperation::DraftUpdate))?;
        Ok(())
    })
}

pub(crate) fn draft_discard<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        destroy_emails(
            &mail,
            &email_states,
            &account_id,
            [EmailId::new(draft.0)],
            AccountOperation::DraftDiscard,
        )
        .await
    })
}

pub(crate) fn draft_send<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let draft_id = EmailId::new(draft.0.clone());
        // Sent and Drafts in one `Mailbox/get`: the submission patch
        // needs the Drafts id to un-file the draft by path rather than
        // by replacing `mailboxIds` wholesale.
        let mut roles = role_mailboxes(
            &mail,
            &[FolderRole::Sent, FolderRole::Drafts],
            AccountOperation::DraftSend,
        )
        .await?;
        let sent_mailbox = roles
            .remove(&FolderRole::Sent)
            .ok_or_else(|| missing_role_mailbox(AccountOperation::DraftSend))?;
        let draft_mailbox = roles.remove(&FolderRole::Drafts);
        let mut submission_set = EmailSubmissionSet::new();
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id(draft_id.clone());
        submission_set
            .on_success_update_email(SUBMISSION_CREATE_ID)
            .submitted_to_sent(&sent_mailbox, draft_mailbox.as_ref());
        let prior_state = state_cache::get(&email_states, &account_id).await;
        let mut response = mail
            .call(submission_set)
            .await
            .map_err(to_acct_err(AccountOperation::DraftSend))?;
        response
            .created(SUBMISSION_CREATE_ID)
            .map_err(to_acct_err(AccountOperation::DraftSend))?;
        // The submission is already committed. A failed cache refresh must
        // not turn that success into an error that invites a duplicate send;
        // the stale state will be reconciled by the next conditional mutation.
        if let Ok(fresh) = super::mutation::probe_email_state(&mail).await {
            state_cache::advance(&email_states, &account_id, prior_state.as_deref(), fresh).await;
        }
        Ok(ObjectId(draft_id.into_string()))
    })
}

/// The JMAP account a search runs against, derived from the `In`
/// containers in its filter.
///
/// `Email/query` addresses exactly one `accountId`, and a foreign
/// container's `ContainerId` is the owner-qualified form `containers_list`
/// mints (`encode_foreign`), so the owner has to be decided BEFORE the
/// query is built: the qualified id is not a mailbox id in any account's
/// namespace. `None` is the primary account - no `In` at all, or an `In`
/// naming a primary container.
///
/// Two `In` filters naming different owners are rejected rather than
/// unioned. One request cannot carry two `accountId`s, and an implicit
/// cross-account union is a deliberate non-goal.
fn search_owner(
    filter: Option<&SearchFilter>,
    op: AccountOperation,
) -> Result<Option<String>, AccountError> {
    fn walk<'a>(
        filter: &'a SearchFilter,
        found: &mut Option<Option<&'a str>>,
        op: AccountOperation,
    ) -> Result<(), AccountError> {
        match filter {
            SearchFilter::In(container) => {
                let owner = super::foreign::owner_of(&container.0);
                match found {
                    Some(seen) if *seen != owner => {
                        return Err(super::error::search_cross_account_filter(op, *seen, owner));
                    }
                    Some(_) => {}
                    None => *found = Some(owner),
                }
            }
            SearchFilter::And(filters) | SearchFilter::Or(filters) => {
                for inner in filters {
                    walk(inner, found, op)?;
                }
            }
            SearchFilter::Not(inner) => walk(inner, found, op)?,
            _ => {}
        }
        Ok(())
    }

    let Some(filter) = filter else {
        return Ok(None);
    };
    let mut found = None;
    walk(filter, &mut found, op)?;
    Ok(found.flatten().map(ToString::to_string))
}

/// Select the `Mail` handle a search routes to.
///
/// An `In` naming a share this session does not hold fails honestly. Unlike
/// hydration, there is no stay-literal fallback available here: the id is a
/// filter operand, so the primary account would answer "no such mailbox"
/// as an empty page rather than a miss.
fn route_search<'a, T: HttpTransport>(
    primary: &'a MailAccount<T>,
    foreign_mail: &'a HashMap<String, MailAccount<T>>,
    filter: Option<&SearchFilter>,
    op: AccountOperation,
) -> Result<(&'a MailAccount<T>, Option<String>), AccountError> {
    let owner = search_owner(filter, op)?;
    match owner {
        None => Ok((primary, None)),
        Some(account_id) => match foreign_mail.get(&account_id) {
            Some(handle) => Ok((handle, Some(account_id))),
            None => Err(super::error::search_unknown_account(
                op,
                &qualified_container_label(filter, &account_id),
                &account_id,
            )),
        },
    }
}

/// The qualified container id that selected an unreachable owner, for the
/// error text. Falls back to the account id when the filter shape changed
/// under us.
fn qualified_container_label(filter: Option<&SearchFilter>, account_id: &str) -> String {
    fn find(filter: &SearchFilter, account_id: &str) -> Option<String> {
        match filter {
            SearchFilter::In(container) => (super::foreign::owner_of(&container.0)
                == Some(account_id))
            .then(|| container.0.clone()),
            SearchFilter::And(filters) | SearchFilter::Or(filters) => {
                filters.iter().find_map(|inner| find(inner, account_id))
            }
            SearchFilter::Not(inner) => find(inner, account_id),
            _ => None,
        }
    }
    filter
        .and_then(|filter| find(filter, account_id))
        .unwrap_or_else(|| account_id.to_string())
}

/// Re-qualify a native id from `owner`'s namespace back into the
/// consumer-facing one. A primary result stays bare - one wire form per
/// logical object.
fn qualify(owner: Option<&str>, native: String) -> String {
    match owner {
        Some(account_id) => super::foreign::encode_object(account_id, &native),
        None => native,
    }
}

pub(crate) fn search<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        let op = AccountOperation::Search;
        let (routed, owner) = route_search(&mail, &foreign_mail, request.filter.as_ref(), op)?;
        let owner = owner.as_deref();
        let page = search_email_ids(routed, request, true, owner, op).await?;
        if page.items.is_empty() {
            return Ok(Page {
                truncated: false,
                items: Vec::new(),
                next_cursor: page.next_cursor,
                estimated_total: page.estimated_total,
                failed_ids: Vec::new(),
                skipped_scopes: Vec::new(),
            });
        }
        let requested = page
            .items
            .iter()
            .cloned()
            .map(EmailId::into_string)
            .collect::<Vec<_>>();
        let response = routed
            .call(
                EmailGet::new()
                    .ids(page.items)
                    .properties([EmailProperty::Id, EmailProperty::ThreadId]),
            )
            .await
            .map_err(to_acct_err(AccountOperation::Search))?;
        let not_found = response.not_found().to_vec();
        let (items, failed_ids) =
            reconcile_search_threads(requested, &not_found, response.into_list());
        // Thread ids ride the object namespace, exactly as foreign inventory
        // and hydration mint them: a bare foreign thread id asserts primary
        // ownership and every thread-keyed door would then act on an
        // unrelated primary thread.
        let items = items
            .into_iter()
            .map(|thread| ThreadId(qualify(owner, thread.0)))
            .collect();
        let failed_ids = failed_ids
            .into_iter()
            .map(|id| qualify(owner, id))
            .collect();
        Ok(Page {
            truncated: false,
            items,
            next_cursor: page.next_cursor,
            estimated_total: page.estimated_total,
            failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

/// Project a thread-id search answer, accounting for every submitted
/// email id.
///
/// A thread search is a query for emails followed by a projection onto
/// their threads, so an email the projection cannot place is a hit the
/// caller asked for and does not receive. Three ways that happens - the
/// id named in `notFound`, the id answered in neither list, and an email
/// returned WITHOUT a `threadId` (which RFC 8621 makes mandatory) - are
/// all "the provider fetched it but could not materialize it", which is
/// what `Page::failed_ids` means. Dropping them left the page silently
/// short of its own result count.
///
/// Thread ids are deduplicated: a query that hits several messages of one
/// thread describes one thread, and `collapseThreads` is not guaranteed
/// to have collapsed them.
fn reconcile_search_threads(
    requested: Vec<String>,
    not_found: &[EmailId],
    list: Vec<Email>,
) -> (Vec<ThreadId>, Vec<String>) {
    let mut answered: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut failed_ids: Vec<String> = Vec::new();
    for missing in not_found {
        let missing = missing.clone().into_string();
        if answered.insert(missing.clone()) {
            failed_ids.push(missing);
        }
    }

    let mut seen_threads: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut items = Vec::new();
    for email in list {
        let Some(id) = email.id().map(ToString::to_string) else {
            continue;
        };
        if id.is_empty() {
            continue;
        }
        match email.thread_id() {
            Some(thread) => {
                answered.insert(id);
                if seen_threads.insert(thread.to_string()) {
                    items.push(ThreadId(thread.to_string()));
                }
            }
            None => {
                if answered.insert(id.clone()) {
                    failed_ids.push(id);
                }
            }
        }
    }

    for id in requested {
        if answered.insert(id.clone()) {
            failed_ids.push(id);
        }
    }
    (items, failed_ids)
}

pub(crate) fn search_messages<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        let op = AccountOperation::SearchMessages;
        let (routed, owner) = route_search(&mail, &foreign_mail, request.filter.as_ref(), op)?;
        let owner = owner.as_deref();
        let page = search_email_ids(routed, request, false, owner, op).await?;
        Ok(Page {
            truncated: false,
            items: page
                .items
                .into_iter()
                .map(|id| ObjectId(qualify(owner, id.into_string())))
                .collect(),
            next_cursor: page.next_cursor,
            estimated_total: page.estimated_total,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

/// Every container the account exposes: the primary account's mailboxes
/// plus each foreign (shared/delegate) account's, namespaced by owner.
///
/// A per-share enumeration failure degrades to a `SkippedScope` on the
/// returned `ContainerList` plus the remaining containers: one
/// unreachable share must not blank the whole sidebar, and it must not
/// vanish from it silently either - the skip entry names the foreign
/// account and carries the classified error, so the consumer can tell
/// "share degraded" from "share deleted". A primary enumeration
/// failure still fails the call.
pub(crate) fn containers_list<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    Box::pin(async move {
        let mut containers = fetch_containers(&mail, AccountOperation::ContainersList).await?;
        // Owner emails are resolved once per foreign account per call
        // (never per container) and are best-effort metadata: any
        // resolution failure yields `None` for that account and MUST NOT
        // propagate out of `containers_list` - the consumer treats a
        // container-listing error as an attach failure, and a missing
        // cosmetic email must never cause an account outage.
        let owner_emails = resolve_owner_emails(&mail, &foreign_mail).await;
        let (foreign, skipped_scopes) = fetch_foreign_containers(
            &foreign_mail,
            &owner_emails,
            AccountOperation::ContainersList,
        )
        .await;
        containers.extend(foreign);
        Ok(ContainerList {
            containers,
            skipped_scopes,
        })
    })
}

pub(crate) fn container_create<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    // JMAP mailboxes carry no container color; accepted for trait
    // parity with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(super::error::unsupported_error(
                AccountOperation::ContainerCreate,
                None,
                "JMAP only supports folder containers",
            ));
        }
        let mut set = MailboxSet::new();
        let mut create = crate::mailbox::MailboxCreate::new(None);
        create.name(name);
        create.parent_id(parent.map(|id| MailboxId::new(id.0)));
        let create_id = set.create_item(create);
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerCreate))?;
        if let Some(new_state) = response.new_state().filter(|state| !state.is_empty()) {
            state_cache::set(&mailbox_states, &account_id, new_state.to_string()).await;
        }
        let mut mailbox = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::ContainerCreate))?;
        Ok(ContainerId(mailbox.take_id().into_string()))
    })
}

pub(crate) fn container_rename<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    container: ContainerId,
    name: String,
    // JMAP has no mailbox recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut set = MailboxSet::new();
        set.update(mailbox).name(name);
        let response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerRename))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::ContainerRename))?;
        if let Some(new_state) = response.new_state().filter(|state| !state.is_empty()) {
            state_cache::set(&mailbox_states, &account_id, new_state.to_string()).await;
        }
        Ok(())
    })
}

pub(crate) fn container_move<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut set = MailboxSet::new();
        set.update(mailbox)
            .parent_id(new_parent.map(|id| MailboxId::new(id.0)));
        let response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerMove))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::ContainerMove))?;
        if let Some(new_state) = response.new_state().filter(|state| !state.is_empty()) {
            state_cache::set(&mailbox_states, &account_id, new_state.to_string()).await;
        }
        Ok(())
    })
}

pub(crate) fn container_delete<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut response = mail
            .call(
                MailboxSet::new()
                    .destroy([mailbox.clone()])
                    .on_destroy_remove_emails(false),
            )
            .await
            .map_err(to_acct_err(AccountOperation::ContainerDelete))?;
        response
            .destroyed(&mailbox)
            .map_err(to_acct_err(AccountOperation::ContainerDelete))?;
        if let Some(new_state) = response.new_state().filter(|state| !state.is_empty()) {
            state_cache::set(&mailbox_states, &account_id, new_state.to_string()).await;
        }
        Ok(())
    })
}

pub(crate) fn identities_list<T: HttpTransport>(
    submission: Option<MailAccount<T>>,
) -> AccountFuture<Result<Vec<bifrost_types::Identity>, AccountError>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(super::error::unsupported_error(
                AccountOperation::IdentitiesList,
                None,
                "JMAP submission capability not available",
            ));
        };
        let response = account
            .call(IdentityGet::new().properties([
                crate::identity::Property::Id,
                crate::identity::Property::Name,
                crate::identity::Property::Email,
                crate::identity::Property::ReplyTo,
                crate::identity::Property::TextSignature,
                crate::identity::Property::HtmlSignature,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::IdentitiesList))?;
        let mut identities = Vec::new();
        for mut identity in response.into_list() {
            let id = identity.take_id().into_string();
            let reply_to = identity
                .reply_to()
                .and_then(|values| values.first())
                .map(address_from_jmap);
            identities.push(bifrost_types::Identity {
                id: bifrost_types::IdentityId(id),
                name: identity.name().unwrap_or("").to_string(),
                address: identity.email().unwrap_or("").to_string(),
                signature_text: identity.text_signature().map(str::to_string),
                signature_html: identity.html_signature().map(str::to_string),
                reply_to,
                // JMAP has no default-identity concept (identity_update
                // refuses is_default for the same reason) and Identity/get
                // order is server-arbitrary, so no row is reported default.
                is_default: false,
            });
        }
        Ok(identities)
    })
}

#[cfg_attr(test, derive(Debug))]
struct SendingIdentity {
    id: String,
    name: Option<String>,
    email: String,
}

/// Select a concrete foreign identity. JMAP does not designate a default
/// identity, so the first concrete address returned by the server is the
/// documented best-effort convention. Wildcard identities cannot be used in
/// an RFC 5322 From header.
async fn foreign_sending_identity<T: HttpTransport>(
    mail: &MailAccount<T>,
    requested: Option<&bifrost_types::IdentityId>,
) -> Result<SendingIdentity, AccountError> {
    let get = IdentityGet::new().properties([
        crate::identity::Property::Id,
        crate::identity::Property::Name,
        crate::identity::Property::Email,
    ]);
    let get = match requested {
        Some(requested) => get.ids([JmapIdentityId::new(requested.0.clone())]),
        None => get,
    };
    let response = mail
        .call(get)
        .await
        .map_err(to_acct_err(AccountOperation::Send))?;
    select_concrete_identity(
        response.into_list(),
        if requested.is_some() {
            "requested foreign sending identity is absent or has no concrete email"
        } else {
            "foreign account advertises no sending identity"
        },
    )
}

/// Resolve only a caller-selected primary identity for scheduled envelope
/// fallback. JMAP has no default identity, and Identity/get order is not a
/// selection contract.
async fn primary_sending_identity<T: HttpTransport>(
    mail: &MailAccount<T>,
    requested: &bifrost_types::IdentityId,
) -> Result<SendingIdentity, AccountError> {
    let response = mail
        .call(
            IdentityGet::new()
                .ids([JmapIdentityId::new(requested.0.clone())])
                .properties([
                    crate::identity::Property::Id,
                    crate::identity::Property::Name,
                    crate::identity::Property::Email,
                ]),
        )
        .await
        .map_err(to_acct_err(AccountOperation::Send))?;
    select_concrete_identity(
        response.into_list(),
        "requested sending identity is absent or has no concrete email",
    )
}

/// Pure identity selection: first row with a concrete (non-empty,
/// non-wildcard) email. Callers provide the detail for an empty or
/// wildcard-only result, since a by-id read and a foreign account listing
/// describe different failures.
fn select_concrete_identity(
    rows: Vec<crate::identity::Identity>,
    missing_detail: &str,
) -> Result<SendingIdentity, AccountError> {
    let mut concrete = rows.into_iter().filter_map(|mut identity| {
        let id = identity.take_id().into_string();
        let email = identity.email()?.to_string();
        (!id.is_empty() && !email.is_empty() && !email.contains('*')).then(|| SendingIdentity {
            id,
            name: identity.name().map(str::to_string),
            email,
        })
    });
    if let Some(identity) = concrete.next() {
        return Ok(identity);
    }
    Err(super::error::unsupported_error(
        AccountOperation::Send,
        None,
        missing_detail,
    ))
}

pub(crate) fn identity_update<T: HttpTransport>(
    submission: Option<MailAccount<T>>,
    identity: bifrost_types::IdentityId,
    patch: bifrost_types::IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(super::error::unsupported_error(
                AccountOperation::IdentityUpdate,
                None,
                "JMAP submission capability not available",
            ));
        };
        if patch.is_default.is_some() {
            return Err(super::error::unsupported_error(
                AccountOperation::IdentityUpdate,
                None,
                "JMAP does not support setting default identity",
            ));
        }
        let mut set = IdentitySet::new();
        let item = set.update(JmapIdentityId::new(identity.0));
        if let Some(name) = patch.name {
            item.name(name);
        }
        if let Some(signature) = patch.signature_text {
            if let Some(value) = signature {
                item.text_signature(value);
            } else {
                item.text_signature("");
            }
        }
        if let Some(signature) = patch.signature_html {
            if let Some(value) = signature {
                item.html_signature(value);
            } else {
                item.html_signature("");
            }
        }
        if let Some(reply_to) = patch.reply_to {
            let values = reply_to.into_iter().map(address_to_jmap);
            item.reply_to(Some(values));
        }
        let response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::IdentityUpdate))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::IdentityUpdate))
    })
}

pub(crate) fn vacation_get<T: HttpTransport>(
    vacation: Option<MailAccount<T>>,
) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(super::error::unsupported_error(
                AccountOperation::VacationGet,
                None,
                "JMAP vacation capability not available",
            ));
        };
        let mut response = account
            .call(VacationResponseGet::new().ids([VacationResponseId::new("singleton")]))
            .await
            .map_err(to_acct_err(AccountOperation::VacationGet))?;
        let Some(vacation) = response.pop() else {
            return Ok(None);
        };
        Ok(Some(VacationConfig {
            is_enabled: vacation.is_enabled(),
            subject: vacation.subject().map(str::to_string),
            body_text: vacation.text_body().map(str::to_string),
            body_html: vacation.html_body().map(str::to_string),
            starts_at: vacation.from_date().and_then(unix_to_system_time),
            ends_at: vacation.to_date().and_then(unix_to_system_time),
        }))
    })
}

pub(crate) fn vacation_set<T: HttpTransport>(
    vacation: Option<MailAccount<T>>,
    config: VacationConfig,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(super::error::unsupported_error(
                AccountOperation::VacationSet,
                None,
                "JMAP vacation capability not available",
            ));
        };
        let mut set = VacationResponseSet::new();
        let patch = set.update(VacationResponseId::new("singleton"));
        patch.is_enabled(config.is_enabled);
        patch.subject(config.subject);
        patch.text_body(config.body_text);
        patch.html_body(config.body_html);
        patch.from_date(config.starts_at.map(system_time_to_unix));
        patch.to_date(config.ends_at.map(system_time_to_unix));
        let response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::VacationSet))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::VacationSet))
    })
}

pub(crate) fn quota_get<T: HttpTransport>(
    quota: Option<MailAccount<T>>,
) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async move {
        let Some(account) = quota else {
            return Err(super::error::unsupported_error(
                AccountOperation::QuotaGet,
                None,
                "JMAP quota capability not available",
            ));
        };
        let response = account
            .call(QuotaGet::new().properties([
                QuotaProperty::ResourceType,
                QuotaProperty::Used,
                QuotaProperty::HardLimit,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::QuotaGet))?;
        let mut fallback = None;
        for quota in response.into_list() {
            let info = QuotaInfo {
                used_bytes: quota.used().unwrap_or(0),
                total_bytes: quota.hard_limit(),
            };
            if matches!(quota.resource_type(), Some("octets" | "storage")) {
                return Ok(Some(info));
            }
            fallback.get_or_insert(info);
        }
        Ok(fallback)
    })
}

/// Route a thread id to the account that owns it.
///
/// `Thread/get` is accountId-scoped and the thread id is the ONLY operand,
/// so an owner-qualified foreign thread id has to select its own account's
/// handle before the qualification is stripped. An id naming an account
/// this session has no handle for stays on the primary route with its
/// LITERAL (still-qualified) form - the same rule `hydrate::route_for_id`
/// plus `wire_id_for_mail` apply to message ids: stripped, the bare native
/// id could resolve an unrelated same-id primary thread; literal, it can
/// name no real primary thread (`\u{1f}` never occurs in an RFC 8620 id)
/// and the server reports the honest `notFound`.
/// Pure over the registration predicate, so the selection is unit-pinnable
/// without a live session - the same shape as `hydrate::route_for_id`.
pub(crate) fn thread_owner<F>(thread: &ThreadId, is_registered: F) -> Option<String>
where
    F: Fn(&str) -> bool,
{
    super::foreign::parse_object(&thread.0)
        .filter(|(account, _)| is_registered(account))
        .map(|(account, _)| account.to_string())
}

pub(crate) fn thread_hydrate<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        let owner = thread_owner(&thread, |account| foreign_mail.contains_key(account));
        let mail = match owner
            .as_deref()
            .and_then(|account| foreign_mail.get(account))
        {
            Some(handle) => handle.clone(),
            None => mail,
        };
        let jmap_thread_id = JmapThreadId::new(wire_id_for_mail(&thread.0, mail.id_str()));
        let mut thread_response = mail
            .call(ThreadGet::new().ids([jmap_thread_id]).properties([
                crate::thread::Property::Id,
                crate::thread::Property::EmailIds,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::HydrateThread))?;
        let thread_object = thread_response.pop().ok_or_else(|| {
            super::error::into_account_error(
                crate::Error::IdNotFound(thread.0.clone()),
                super::error::JmapErrorContext::new(AccountOperation::HydrateThread).with_scope(
                    bifrost_types::ErrorScope::Thread {
                        id: (thread.0.clone()).into(),
                    },
                ),
            )
        })?;
        let order = thread_object
            .email_ids()
            .ok_or_else(|| {
                to_acct_err(AccountOperation::HydrateThread)(crate::Error::NotParsable(
                    "Thread/get response omitted requested emailIds".to_string(),
                ))
            })?
            .to_vec();
        if order.is_empty() {
            return Ok(ThreadHydration {
                id: thread,
                messages: Vec::new(),
            });
        }
        let response = mail
            .call(
                EmailGet::new()
                    .ids(order.clone())
                    .properties(message_properties(HydrationProjection::Full))
                    .fetch_text_body_values(true)
                    .fetch_html_body_values(true)
                    .body_properties(body_properties()),
            )
            .await
            .map_err(to_acct_err(AccountOperation::HydrateThread))?;
        let mut by_id = response
            .into_list()
            .into_iter()
            .filter_map(|email| email.id().map(ToString::to_string).map(|id| (id, email)))
            .collect::<HashMap<_, _>>();
        let mut messages = Vec::new();
        for id in order {
            if let Some(email) = by_id.remove(id.as_str()) {
                let mut message = email_to_message(email, HydrationProjection::Full);
                // The member ids came off the wire in the foreign
                // account's own namespace. Handed back bare they would
                // route every follow-on request (blob download, container
                // join, a per-message mutation) through the PRIMARY
                // account, so re-qualify them into the same namespace the
                // foreign inventory and `message_hydrate` mint.
                if let Some(account) = owner.as_deref() {
                    qualify_foreign_message_ids(
                        &mut message.id,
                        message.thread_id.as_mut(),
                        &mut message.containers,
                        &mut message.attachments,
                        account,
                    );
                }
                messages.push(message);
            }
        }
        // The hydration's own id is the id the CALLER submitted, verbatim,
        // so a round-trip through this door is byte-stable.
        Ok(ThreadHydration {
            id: thread,
            messages,
        })
    })
}

/// Re-qualify a foreign account's `Message` ids with their owning account.
///
/// `email_to_message` reads the bare native ids off the wire, but every id a
/// consumer hands back to this crate (`open_blob`, `get_stream`, a container
/// join against `containers_list`) has to be self-routing. Without this the
/// hydrated message's own id, its container ids, and its attachment blob ids
/// all come back in the PRIMARY namespace, so the follow-up blob download
/// 404s against the primary account and the container ids join nothing.
///
/// The `thread_id` is qualified for the same reason, and it is the
/// sharpest one: the consumer hands it back as `thread_hydrate(thread)`
/// and as `MutationTarget::Thread`, each of which expands it through an
/// accountId-scoped `Thread/get`. A bare foreign thread id asserts primary
/// ownership, so on an id collision the write lands on an unrelated
/// primary thread's messages.
///
/// Pure over the id slices so the qualification is unit-pinnable
/// without constructing a whole `Message`.
fn qualify_foreign_message_ids(
    id: &mut ObjectId,
    thread_id: Option<&mut ThreadId>,
    containers: &mut [ContainerId],
    attachments: &mut [MessageAttachment],
    account: &str,
) {
    id.0 = super::foreign::encode_object(account, &id.0);
    if let Some(thread) = thread_id {
        thread.0 = super::foreign::encode_object(account, &thread.0);
    }
    for container in containers {
        // A container id is a MAILBOX id, so it carries the same namespace
        // `containers_list` mints for a foreign mailbox - not the object
        // namespace - and the two must not be confused.
        *container = ContainerId(super::foreign::encode_foreign(account, &container.0).0);
    }
    for attachment in attachments {
        if let AttachmentSource::Blob(blob) = &mut attachment.source {
            blob.id = BlobId(super::foreign::encode_object(account, &blob.id.0));
        }
    }
}

pub(crate) fn message_hydrate<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        // `Email/get` is accountId-scoped. A foreign-qualified id therefore
        // has to run against its OWNING account's handle: on the primary
        // handle it either 404s (reporting the encoded id verbatim, which is
        // how this surfaced downstream) or resolves an unrelated primary
        // object that happens to share the native id. `get_stream` already
        // routed per id; this one-id door did not, and it is the door the
        // engine's `message_hydrate` passthrough funnels into.
        let owner = match super::hydrate::route_for_id(&message, |account| {
            foreign_mail.contains_key(account)
        }) {
            super::hydrate::HydrationRoute::Foreign(account) => Some(account),
            super::hydrate::HydrationRoute::Primary => None,
        };
        let mail = match owner
            .as_deref()
            .and_then(|account| foreign_mail.get(account))
        {
            Some(handle) => handle.clone(),
            None => mail,
        };
        // The wire call takes the native id only when the call runs against
        // the id's own foreign account. An id for an UNREGISTERED account
        // rides the primary route with its literal (still-qualified) form,
        // matching `wire_id_for_mail` and the bulk pipeline: stripped, the
        // bare native id could resolve an unrelated same-id primary message;
        // literal, the primary `Email/get` reports the honest `notFound`.
        let native = super::hydrate::wire_object_id(&message, owner.as_deref()).to_string();
        let mut get = EmailGet::new()
            .ids([EmailId::new(native)])
            .properties(message_properties(projection));
        if matches!(
            projection,
            HydrationProjection::Preview(_)
                | HydrationProjection::Full
                | HydrationProjection::FullWithBlobs
        ) {
            get = get
                .fetch_text_body_values(true)
                .fetch_html_body_values(true)
                .body_properties(body_properties());
        }
        if let HydrationProjection::Preview(max) = projection {
            get = get.max_body_value_bytes(max);
        }
        let mut response = mail
            .call(get)
            .await
            .map_err(to_acct_err(AccountOperation::HydrateMessage))?;
        let email = response.pop().ok_or_else(|| {
            super::error::into_account_error(
                crate::Error::IdNotFound(message.0.clone()),
                super::error::JmapErrorContext::new(AccountOperation::HydrateMessage).with_scope(
                    bifrost_types::ErrorScope::Message {
                        id: (message.0.clone()).into(),
                    },
                ),
            )
        })?;
        let mut hydrated = email_to_message(email, projection);
        if let Some(account) = owner.as_deref() {
            qualify_foreign_message_ids(
                &mut hydrated.id,
                hydrated.thread_id.as_mut(),
                &mut hydrated.containers,
                &mut hydrated.attachments,
                account,
            );
        }
        Ok(hydrated)
    })
}

pub(crate) fn move_thread<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
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
        patch_mailbox_membership_of(
            &mail,
            &email_states,
            &account_id,
            &ids,
            target,
            true,
            AccountOperation::BulkMove,
        )
        .await?;
        if let Some(source) = source {
            patch_mailbox_membership_of(
                &mail,
                &email_states,
                &account_id,
                &ids,
                source,
                false,
                AccountOperation::RemoveFromContainer,
            )
            .await?;
        }
        Ok(())
    })
}

pub(crate) fn delete_thread<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
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
        let trash = match super::foreign::owner_of(&thread.0) {
            Some(owner) => {
                ContainerId(super::foreign::encode_foreign(owner, trash_mailbox.as_str()).0)
            }
            None => ContainerId(trash_mailbox.as_str().to_string()),
        };
        // JMAP mailbox ids are server-issued opaque strings and RFC
        // 8620 leaves case-sensitivity to the server, but two
        // mailboxes that differ only in case is a pathological shape
        // we are happy to misclassify in favor of defensiveness:
        // case-insensitive compare here means a caller who normalized
        // the id elsewhere in their stack still resolves a "thread is
        // already in Trash" branch correctly.
        let already_in_trash = current
            .as_ref()
            .is_some_and(|id| id.0.eq_ignore_ascii_case(&trash.0));
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
                &email_states,
                &account_id,
                ids,
                read_state,
                AccountOperation::BulkDestroy,
            )
            .await
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
            patch_mailbox_membership_of(
                &mail,
                &email_states,
                &account_id,
                &ids,
                trash,
                true,
                AccountOperation::BulkMove,
            )
            .await?;
            if let Some(source) = current {
                patch_mailbox_membership_of(
                    &mail,
                    &email_states,
                    &account_id,
                    &ids,
                    source,
                    false,
                    AccountOperation::RemoveFromContainer,
                )
                .await?;
            }
            Ok(())
        }
    })
}

async fn resolve_thread_emails_in_mailbox<T: HttpTransport>(
    mail: &MailAccount<T>,
    ids: &[EmailId],
    mailbox: &MailboxId,
    op: AccountOperation,
) -> Result<(Vec<EmailId>, Option<String>), AccountError> {
    if ids.is_empty() {
        return Ok((Vec::new(), None));
    }

    // A conversation can exceed maxObjectsInGet; keep every Email/get
    // within the account's advertised per-call bound.
    let batch_size = super::factory::max_objects_in_get(&mail.client().session());
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
            read_state = Some(response.state().to_string());
        }

        let requested = chunk.iter().cloned().collect::<HashSet<_>>();
        let mut answered = HashSet::new();
        for email in response.list() {
            let Some(id) = email.id() else {
                continue;
            };
            if !requested.contains(id) {
                continue;
            }
            answered.insert(id.clone());
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
            if !answered.contains(id) && !response.not_found().contains(id) {
                return Err(super::error::get_id_unanswered(
                    id.as_str(),
                    super::error::JmapErrorContext::new(op),
                ));
            }
        }
    }

    Ok((in_mailbox, read_state))
}

async fn destroy_thread_emails_if_unchanged<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    ids: Vec<EmailId>,
    read_state: Option<String>,
    op: AccountOperation,
) -> Result<(), AccountError> {
    if ids.is_empty() {
        return Ok(());
    }
    let read_state = read_state.ok_or_else(|| {
        to_acct_err(op)(crate::Error::NotParsable(
            "Trash membership read returned no Email state".to_string(),
        ))
    })?;
    let ids_for_set = ids.clone();
    let prior_state = state_cache::get(email_states, account_id).await;
    let mut response = mail
        .call(EmailSet::new().if_in_state(read_state).destroy(ids_for_set))
        .await
        .map_err(to_acct_err(op))?;
    if let Some(new_state) = response.new_state().filter(|state| !state.is_empty()) {
        state_cache::advance(
            email_states,
            account_id,
            prior_state.as_deref(),
            new_state.to_string(),
        )
        .await;
    }
    for id in &ids {
        response.destroyed(id).map_err(to_acct_err(op))?;
    }
    Ok(())
}

async fn patch_mailbox_membership<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    target: MutationTarget,
    container: ContainerId,
    value: bool,
    op: AccountOperation,
) -> Result<(), AccountError> {
    cross_account_container(&target, &container, op)?;
    let ids = resolve_target(mail, target, op).await?;
    patch_mailbox_membership_of(mail, email_states, account_id, &ids, container, value, op).await
}

/// The membership patch over an ALREADY RESOLVED set of message ids.
///
/// A thread move is two legs (add to the target, remove from the source)
/// and each leg used to run its own `Thread/get`. A message delivered to
/// the thread between the two resolves was therefore removed from the
/// source without ever having been added to the target: it lost the
/// source membership and gained nothing. The two-leg non-atomicity is the
/// documented cross-provider shape and is not fixable here, but the
/// window only has to be one `Email/set` pair over a FIXED id set, so the
/// thread is resolved once and both legs run over that set.
///
/// The caller performs the cross-account container check for each
/// container it passes; the ids themselves are already in this account's
/// namespace.
async fn patch_mailbox_membership_of<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    ids: &[EmailId],
    container: ContainerId,
    value: bool,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let ids = ids.to_vec();
    // The owner check has established that the container names the
    // same account as the target, so the only work left is stripping the
    // qualification the selected account does not use.
    let mailbox = MailboxId::new(wire_id_for_mail(&container.0, mail.id_str()));
    let ids_for_set = ids.clone();
    let mut response = send_email_set(mail, email_states, account_id, op, move || {
        let mut set = EmailSet::new();
        for id in &ids_for_set {
            set.update(id.clone()).mailbox_id(&mailbox, value);
        }
        set
    })
    .await?;
    for id in &ids {
        response.updated(id).map_err(to_acct_err(op))?;
    }
    Ok(())
}

async fn resolve_target<T: HttpTransport>(
    mail: &MailAccount<T>,
    target: MutationTarget,
    op: AccountOperation,
) -> Result<Vec<EmailId>, AccountError> {
    match target {
        MutationTarget::Message(id) => {
            Ok(vec![EmailId::new(wire_id_for_mail(&id.0, mail.id_str()))])
        }
        MutationTarget::Thread(thread) => {
            // Same rule as the `Message` arm above: the account layer has
            // already selected `mail` from the thread id's owner, so strip
            // the qualification only when it names this exact account. An
            // unreachable foreign thread id stays literal and the server
            // reports the honest miss instead of a same-id primary thread.
            let mut response = mail
                .call(
                    ThreadGet::new()
                        .ids([JmapThreadId::new(wire_id_for_mail(
                            &thread.0,
                            mail.id_str(),
                        ))])
                        .properties([crate::thread::Property::EmailIds]),
                )
                .await
                .map_err(to_acct_err(op))?;
            let thread = response.pop().ok_or_else(|| {
                // The operation is the MUTATION being performed, not
                // `HydrateThread`: the thread lookup here only expands a
                // mutation target, and mislabelling it sends the caller a
                // hydration failure for an operation it never issued.
                super::error::into_account_error(
                    crate::Error::IdNotFound(thread.0.clone()),
                    super::error::JmapErrorContext::new(op).with_scope(
                        bifrost_types::ErrorScope::Thread {
                            id: (thread.0.clone()).into(),
                        },
                    ),
                )
            })?;
            thread
                .email_ids()
                .ok_or_else(|| {
                    to_acct_err(op)(crate::Error::NotParsable(
                        "Thread/get response omitted requested emailIds".to_string(),
                    ))
                })
                .map(ToOwned::to_owned)
        }
        _ => Err(super::error::unsupported_error(
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
fn cross_account_container(
    target: &MutationTarget,
    container: &ContainerId,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let (target_id, target_owner) = match target {
        MutationTarget::Message(id) => (id.0.as_str(), super::foreign::owner_of(&id.0)),
        MutationTarget::Thread(thread) => (thread.0.as_str(), super::foreign::owner_of(&thread.0)),
        _ => return Ok(()),
    };
    let container_owner = super::foreign::owner_of(&container.0);
    if target_owner == container_owner {
        return Ok(());
    }
    Err(super::error::cross_account_destination(
        op,
        target_id,
        target_owner,
        &container.0,
        container_owner,
    ))
}

/// The account layer has already selected `mail` from a registered foreign
/// object id. Strip that routing prefix only when it names this exact account;
/// an unreachable foreign id remains literal on the primary route so the
/// server produces the normal not-found response.
fn wire_id_for_mail(id: &str, account_id: &str) -> String {
    match super::foreign::parse_object(id) {
        Some((owner, native)) if owner == account_id => native.to_string(),
        _ => id.to_string(),
    }
}

async fn destroy_emails<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    ids: impl IntoIterator<Item = EmailId> + Clone + Send + 'static,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let ids_vec = ids.into_iter().collect::<Vec<_>>();
    if ids_vec.is_empty() {
        return Ok(());
    }
    let ids_for_set = ids_vec.clone();
    let mut response = send_email_set(mail, email_states, account_id, op, move || {
        EmailSet::new().destroy(ids_for_set.clone())
    })
    .await?;
    for id in &ids_vec {
        response.destroyed(id).map_err(to_acct_err(op))?;
    }
    Ok(())
}

async fn send_email_set<T: HttpTransport, F>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    op: AccountOperation,
    make_set: F,
) -> Result<crate::core::set::SetResponse<Email>, AccountError>
where
    F: FnOnce() -> EmailSet,
{
    // These writes are narrow field/path patches or destroys by explicit id.
    // An account-wide `ifInState` rejects them after an unrelated Email
    // change, while a patch preserves fields it does not name. Keep the cache
    // advance compare-and-swap guarded without probing before the write.
    let prior_state = state_cache::get(email_states, account_id).await;
    let response = mail.call(make_set()).await.map_err(to_acct_err(op))?;
    if let Some(new_state) = response.new_state().filter(|state| !state.is_empty()) {
        state_cache::advance(
            email_states,
            account_id,
            prior_state.as_deref(),
            new_state.to_string(),
        )
        .await;
    }
    Ok(response)
}

async fn role_mailbox<T: HttpTransport>(
    mail: &MailAccount<T>,
    role: FolderRole,
    op: AccountOperation,
) -> Result<MailboxId, AccountError> {
    role_mailboxes(mail, &[role], op)
        .await?
        .remove(&role)
        .ok_or_else(|| missing_role_mailbox(op))
}

/// Resolve several role mailboxes from ONE `Mailbox/get`. Roles the
/// account does not expose are simply absent from the map, so the
/// caller decides which of them are required. Batching matters because
/// the submission paths need Drafts and Sent together and
/// `fetch_mailboxes` is an uncached round trip.
async fn role_mailboxes<T: HttpTransport>(
    mail: &MailAccount<T>,
    roles: &[FolderRole],
    op: AccountOperation,
) -> Result<HashMap<FolderRole, MailboxId>, AccountError> {
    let mut found = HashMap::new();
    for mut mailbox in fetch_mailboxes(mail, op).await? {
        let Some(role) = map_role(mailbox.role()) else {
            continue;
        };
        if !roles.contains(&role) {
            continue;
        }
        let id = mailbox.take_id();
        if id.as_str().is_empty() {
            continue;
        }
        found.entry(role).or_insert(id);
    }
    Ok(found)
}

fn missing_role_mailbox(op: AccountOperation) -> AccountError {
    super::error::unsupported_error(op, None, "JMAP required mailbox role not found")
}

async fn fetch_containers<T: HttpTransport>(
    mail: &MailAccount<T>,
    op: AccountOperation,
) -> Result<Vec<Container>, AccountError> {
    Ok(fetch_mailboxes(mail, op)
        .await?
        .into_iter()
        .filter_map(|mailbox| container_from_mailbox(mailbox, None, None))
        .collect())
}

/// The JMAP principals capabilities (RFC 9670) gating owner-email
/// resolution.
const PRINCIPALS_CAPABILITY: &str = "urn:ietf:params:jmap:principals";
const PRINCIPALS_OWNER_CAPABILITY: &str = "urn:ietf:params:jmap:principals:owner";

/// How one foreign account's owner email resolves. Pure decision core of
/// `resolve_owner_emails`, split out so the gate is unit-pinnable.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnerEmailPlan {
    /// No email for this account. Also the fail-soft landing for every
    /// error path.
    Skip,
    /// The account advertises `urn:ietf:params:jmap:principals:owner`
    /// with a principal id: resolve via `Principal/get`, and if that
    /// yields nothing, degrade to `name_fallback` when the session name
    /// is itself an address.
    FromPrincipal {
        principal_id: String,
        name_fallback: Option<String>,
    },
    /// No owner principal, but the account's session NAME is an address:
    /// use it directly. Only reachable when the SESSION advertises the
    /// principals capability - the caller gates on that first.
    FromName(String),
}

/// Decide how a foreign account's owner email resolves, given the
/// account-level owner principal id (if advertised) and the account's
/// session name.
///
/// A principal id is always preferred: `Principal/get` is authoritative
/// and the session name is only a label. A principal the server answers
/// for and does not know is the case the fallback exists for, but the
/// fallback applies ONLY when the lookup actually completed - see
/// `PrincipalEmail`. A lookup that failed teaches nothing, and guessing
/// over it would overwrite real ownership with metadata.
fn owner_email_plan(owner_principal_id: Option<&str>, account_name: &str) -> OwnerEmailPlan {
    let name_email = account_name_as_address(account_name);
    if let Some(principal_id) = owner_principal_id {
        return OwnerEmailPlan::FromPrincipal {
            principal_id: principal_id.to_string(),
            name_fallback: name_email,
        };
    }
    match name_email {
        Some(email) => OwnerEmailPlan::FromName(email),
        None => OwnerEmailPlan::Skip,
    }
}

/// Interpret an `Account.name` as an owner address by PARSING it, not
/// by sniffing for an `@`.
///
/// RFC 8620 defines `Account.name` as a user-facing label ("e.g. the
/// email address"); an address is one possible shape, not a contract.
/// A containment test accepted anything with an `@` anywhere in it and
/// stored the whole string, so `Support <support@example.com>` became
/// an owner "address" verbatim.
///
/// Only a bare addr-spec is accepted. Pulling the address out of a
/// `Name <addr>` form is deliberately NOT done: that guesses which
/// address inside a free-form label identifies the owner, and a wrong
/// owner is worse than no owner. Requiring a dotted domain rejects
/// local-only labels that could never route.
fn account_name_as_address(name: &str) -> Option<String> {
    let candidate = name.trim();
    if candidate.is_empty() || candidate.chars().any(char::is_whitespace) {
        return None;
    }
    if candidate.contains(['<', '>', ',', ';', '"', '\\']) {
        return None;
    }
    let (local, domain) = candidate.split_once('@')?;
    if local.is_empty() || domain.is_empty() || domain.contains('@') {
        return None;
    }
    if !domain.contains('.') || domain.starts_with('.') || domain.ends_with('.') {
        return None;
    }
    Some(candidate.to_string())
}

/// Plan owner-email resolution for every foreign account in the session.
///
/// Level ONE of the two-level gate lives here: a session that does not
/// advertise `urn:ietf:params:jmap:principals` yields no plans at all -
/// not even the name fallback. Resolving from the account name on a
/// session with no principals capability would populate owner emails
/// where the legacy behavior left them NULL.
fn owner_email_plans(
    session: &crate::core::session::Session,
    foreign_account_ids: &[&String],
) -> Vec<(String, OwnerEmailPlan)> {
    if !session.has_capability(PRINCIPALS_CAPABILITY) {
        return Vec::new();
    }
    foreign_account_ids
        .iter()
        .filter_map(|account_id| {
            let account = session.account(account_id)?;
            // Level TWO: the individual account's owner capability.
            let owner_principal_id =
                account
                    .capability(PRINCIPALS_OWNER_CAPABILITY)
                    .and_then(|capability| match capability {
                        crate::core::session::Capabilities::PrincipalsOwner(owner) => {
                            owner.principal_id().map(|id| id.as_str().to_string())
                        }
                        _ => None,
                    });
            Some((
                (*account_id).clone(),
                owner_email_plan(owner_principal_id.as_deref(), account.name()),
            ))
        })
        .collect()
}

/// Resolve owner emails for the foreign accounts, once per account.
/// Returns only the resolved entries; every failure lands as an absent
/// key (fail-soft, mandatory - see `containers_list`).
async fn resolve_owner_emails<T: HttpTransport>(
    mail: &MailAccount<T>,
    foreign_mail: &HashMap<String, MailAccount<T>>,
) -> HashMap<String, String> {
    let mut resolved = HashMap::new();
    if foreign_mail.is_empty() {
        return resolved;
    }
    let session = mail.client().session();
    // Deterministic order so the resolution (and its logging, if any is
    // ever added) is stable across calls.
    let mut foreign_ids: Vec<&String> = foreign_mail.keys().collect();
    foreign_ids.sort();
    let principals_account_id = session
        .principals_capabilities()
        .and_then(|capabilities| capabilities.account_id_for_principal())
        .map(|id| id.as_str().to_string());
    for (account_id, plan) in owner_email_plans(&session, &foreign_ids) {
        match plan {
            OwnerEmailPlan::Skip => {}
            OwnerEmailPlan::FromName(email) => {
                resolved.insert(account_id, email);
            }
            OwnerEmailPlan::FromPrincipal {
                principal_id,
                name_fallback,
            } => {
                let outcome =
                    fetch_principal_email(mail, principals_account_id.as_deref(), &principal_id)
                        .await;
                if let Some(email) = owner_email_from_lookup(outcome, name_fallback) {
                    resolved.insert(account_id, email);
                }
            }
        }
    }
    resolved
}

/// Outcome of one owner-principal lookup.
///
/// The distinction that matters is between "the server answered and
/// there is no email" and "we never got an answer". Collapsing both to
/// `None` meant a transport blip or an unimplemented `Principal/get`
/// was indistinguishable from genuine absence, and the name fallback
/// fired on all of them - so a transient failure could overwrite real
/// ownership with a guess derived from a display label.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PrincipalEmail {
    /// Authoritative address from `Principal/get`.
    Resolved(String),
    /// The call succeeded and yielded no address: either the principal
    /// is not one the server knows, or it carries no email property.
    /// There is nothing better to be had, so a parsed account name may
    /// stand in.
    Absent,
    /// The call did not complete. This is not evidence of anything;
    /// never fall back on it, or a blip rewrites ownership.
    Unavailable,
}

/// Combine a principal lookup with the plan's parsed name fallback.
///
/// The whole point of classifying the lookup: only a COMPLETED one
/// licenses the fallback. `Absent` means the server answered and there
/// is no address, so the account name is the best remaining evidence.
/// `Unavailable` means we never got an answer, and substituting a guess
/// there would overwrite authoritative ownership with metadata on every
/// transient failure.
fn owner_email_from_lookup(
    outcome: PrincipalEmail,
    name_fallback: Option<String>,
) -> Option<String> {
    match outcome {
        PrincipalEmail::Resolved(email) => Some(email),
        PrincipalEmail::Absent => name_fallback,
        PrincipalEmail::Unavailable => None,
    }
}

/// `Principal/get` for one owner principal, against the session's
/// principals account when it names one (falling back to the mail
/// account's id, matching the legacy default-account behavior).
///
/// Still fail-soft - no error escapes - but the failure is now
/// classified rather than flattened.
async fn fetch_principal_email<T: HttpTransport>(
    mail: &MailAccount<T>,
    principals_account_id: Option<&str>,
    principal_id: &str,
) -> PrincipalEmail {
    let account = match principals_account_id {
        Some(id) => crate::account::Account::new(mail.client().clone(), id),
        None => mail.clone(),
    };
    let get = crate::principal::PrincipalGet::new()
        .ids([crate::principal::PrincipalId::new(principal_id)])
        .properties([
            crate::principal::Property::Email,
            crate::principal::Property::Name,
        ]);
    match account.call(get).await {
        Ok(response) => response
            .into_list()
            .into_iter()
            .next()
            .and_then(|principal| principal.email().map(String::from))
            .map_or(PrincipalEmail::Absent, PrincipalEmail::Resolved),
        // Transport failure, method error, unimplemented method: we
        // learned nothing about who owns this account.
        Err(_) => PrincipalEmail::Unavailable,
    }
}

/// Enumerate every foreign (shared/delegate) account's mailboxes as
/// `Shared`-namespace containers.
///
/// Each foreign account is independent, so a per-account `Mailbox/get`
/// failure degrades to a `SkippedScope` (naming the foreign accountId,
/// carrying the classified error) plus the remaining containers -
/// matching the shape open-time seeding uses for an unreachable share.
/// One unreachable share must not blank the whole sidebar.
async fn fetch_foreign_containers<T: HttpTransport>(
    foreign_mail: &HashMap<String, MailAccount<T>>,
    owner_emails: &HashMap<String, String>,
    op: AccountOperation,
) -> (Vec<Container>, Vec<SkippedScope>) {
    let mut containers = Vec::new();
    let mut skipped = Vec::new();
    // Deterministic order so the projection is stable across calls.
    let mut account_ids: Vec<&String> = foreign_mail.keys().collect();
    account_ids.sort();
    for account_id in account_ids {
        let mail = &foreign_mail[account_id];
        let owner_email = owner_emails.get(account_id).map(String::as_str);
        match fetch_mailboxes(mail, op).await {
            Ok(mailboxes) => {
                for mailbox in mailboxes {
                    // Read before projection: `container_from_mailbox`
                    // maps the absence onto `rights: None`, which is
                    // also what a protocol that never reports rights
                    // produces, so the omission is unrecoverable after.
                    let rights_missing = mailbox.my_rights().is_none();
                    let Some(container) =
                        container_from_mailbox(mailbox, Some(account_id), owner_email)
                    else {
                        continue;
                    };
                    if rights_missing {
                        skipped.push(SkippedScope {
                            scope: bifrost_types::ErrorScope::Mailbox {
                                id: container.id.0.clone().into(),
                            },
                            error: missing_shared_rights(&container.id.0, op),
                        });
                    }
                    containers.push(container);
                }
            }
            Err(error) => skipped.push(SkippedScope {
                scope: bifrost_types::ErrorScope::Mailbox {
                    id: (account_id.clone()).into(),
                },
                error,
            }),
        }
    }
    (containers, skipped)
}

/// A shared mailbox whose `Mailbox/get` answered without `myRights`.
///
/// RFC 8621 s2 makes `myRights` a server-set Mailbox property, and
/// `fetch_mailboxes` asks for it by name, so an answer that omits it is
/// a missing field rather than a protocol that has no rights notion. It
/// matters only on a SHARE: `Container::rights` gates submit and the
/// read-only-share distinction, and `None` there is indistinguishable
/// from a personal mailbox where rights are not consulted. Reported as
/// a degradation, not a failure - the container is still listed, since
/// a share with unknown rights is far more useful than a share missing
/// from the sidebar.
fn missing_shared_rights(container_id: &str, op: AccountOperation) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField),
        bifrost_types::Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol: bifrost_types::Protocol::Jmap,
            detail: Some(bifrost_types::DiagnosticText::support_only(format!(
                "shared Mailbox/get answered without myRights: {container_id}"
            ))),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

async fn fetch_mailboxes<T: HttpTransport>(
    mail: &MailAccount<T>,
    op: AccountOperation,
) -> Result<Vec<Mailbox>, AccountError> {
    Ok(mail
        .call(MailboxGet::new().properties([
            MailboxProperty::Id,
            MailboxProperty::Name,
            MailboxProperty::ParentId,
            MailboxProperty::Role,
            MailboxProperty::MyRights,
            MailboxProperty::IsSubscribed,
        ]))
        .await
        .map_err(to_acct_err(op))?
        .into_list())
}

/// Project one `Mailbox` onto a `Container`.
///
/// `owner_account` is `Some(accountId)` for a foreign (shared/delegate)
/// account's mailbox. In that case the container's `native_id` is
/// `encode_foreign(accountId, mailboxId)` - byte-identical to the
/// `MembershipScope::Folder` qualification the foreign inventory and
/// hydration stamp on that account's messages, which is what lets the
/// consumer join a message's membership to its container. (The account's
/// SYNC scope is coarser: one account-level `Folder` scope per share,
/// since `Email/changes` cannot be filtered by mailbox.) `owner_local_id`
/// keeps the bare mailbox id for calls made against the owner's own
/// account. The parent is re-encoded in the same namespace so a foreign
/// child never points at a primary mailbox that happens to share the
/// parent's id.
fn container_from_mailbox(
    mut mailbox: Mailbox,
    owner_account: Option<&str>,
    owner_email: Option<&str>,
) -> Option<Container> {
    let id = mailbox.take_id();
    if id.as_str().is_empty() {
        return None;
    }
    let local = id.into_string();
    let native = match owner_account {
        Some(account) => super::foreign::encode_foreign(account, &local).0,
        None => local.clone(),
    };
    let parent = mailbox.parent_id().map(|parent| {
        let parent = parent.to_string();
        ContainerId(match owner_account {
            Some(account) => super::foreign::encode_foreign(account, &parent).0,
            None => parent,
        })
    });
    let role = map_role(mailbox.role());
    Some(
        Container::new(
            ContainerId(native.clone()),
            ContainerKind::Folder,
            role,
            Provenance {
                provider: bifrost_types::ProtocolKind::Jmap,
                kind: ContainerKind::Folder,
                native,
            },
            mailbox.name().unwrap_or("").to_string(),
            parent,
        )
        // JMAP mailboxes carry no container color, so `style` keeps its
        // `Container::new` default.
        //
        // A role-bearing mailbox is JMAP's native system notion; for
        // folder-shaped JMAP that is exactly what `role` already captures,
        // so there is no hidden split to surface.
        .with_system(role.is_some())
        // JMAP's `Mailbox.myRights` / `Mailbox.isSubscribed` carry the
        // per-folder ACL and subscription state the shared-mailbox sidebar
        // gates submit on.
        .with_rights(mailbox.my_rights().map(rights_from_mailbox))
        .with_subscription(mailbox.is_subscribed())
        .with_namespace(match owner_account {
            Some(_) => ContainerNamespace::Shared,
            None => ContainerNamespace::Personal,
        })
        .with_owner(owner_account.map(|account| bifrost_types::MailboxId(account.to_string())))
        // Best-effort metadata resolved once per foreign account by
        // `resolve_owner_emails`; `None` for a personal mailbox and for
        // every unresolvable owner (fail-soft).
        .with_owner_email(owner_email.map(str::to_string))
        .with_owner_local_id(owner_account.map(|_| local)),
    )
}

/// Map the JMAP `Mailbox/myRights` object onto the unified
/// [`ContainerRights`]. JMAP always emits every member as a concrete
/// boolean when it emits the object at all, so each maps to `Some(_)`.
fn rights_from_mailbox(rights: &MailboxRights) -> ContainerRights {
    ContainerRights {
        may_read_items: Some(rights.may_read_items()),
        may_add_items: Some(rights.may_add_items()),
        may_remove_items: Some(rights.may_remove_items()),
        may_set_seen: Some(rights.may_set_seen()),
        may_set_keywords: Some(rights.may_set_keywords()),
        may_create_child: Some(rights.may_create_child()),
        may_rename: Some(rights.may_rename()),
        may_delete: Some(rights.may_delete()),
        may_submit: Some(rights.may_submit()),
    }
}

fn map_role(role: Option<&Role>) -> Option<FolderRole> {
    match role {
        Some(Role::Inbox) => Some(FolderRole::Inbox),
        Some(Role::Sent) => Some(FolderRole::Sent),
        Some(Role::Drafts) => Some(FolderRole::Drafts),
        Some(Role::Archive) => Some(FolderRole::Archive),
        Some(Role::Trash) => Some(FolderRole::Trash),
        Some(Role::Junk) => Some(FolderRole::Spam),
        _ => None,
    }
}

async fn search_email_ids<T: HttpTransport>(
    mail: &MailAccount<T>,
    request: SearchRequest,
    collapse_threads: bool,
    owner: Option<&str>,
    op: AccountOperation,
) -> Result<Page<EmailId>, AccountError> {
    let cursor = decode_position(request.page_cursor.as_deref(), owner, op)?;
    let limit = request.limit.unwrap_or(50).max(1);
    let query_request = build_search_query(request, collapse_threads, cursor.position, limit)?;
    let response = mail.call(query_request).await.map_err(to_acct_err(op))?;

    // The result set must not have moved between the page that minted the
    // cursor and this one. `Email/query` positions index an order the server
    // recomputes per call, so a `queryState` change means page 2 is being
    // taken from a DIFFERENT list than page 1: at a single delivery the
    // window slides by one and the caller silently loses a hit or sees a
    // duplicate. One delivered message is enough to move it, so this is not
    // terminal - it is a concurrency conflict: repeat the search.
    if let Some(pinned) = cursor.query_state.as_deref()
        && pinned != response.query_state()
    {
        return Err(super::error::search_result_set_superseded(op));
    }

    // A `total` that does not fit `u64` is reported, not dropped: silently
    // converting it to `None` would switch the walk's termination mode
    // without saying so.
    let total = response
        .total()
        .map(|total| {
            u64::try_from(total).map_err(|error| {
                super::error::unsupported_error(
                    op,
                    None,
                    format!("JMAP search total does not fit u64: {error}"),
                )
            })
        })
        .transpose()?;
    let served_from = response.position();
    let query_state = response.query_state().to_string();
    let ids = response.into_ids();
    let next_position = validate_search_page(cursor.position, served_from, ids.len(), total, op)?;

    // "The page came back full" is not evidence that more remains, and a
    // short page is not evidence that nothing does. `total` (which this query
    // always requests via `calculateTotal`) is the authoritative answer; with
    // no total the walk continues until an EMPTY page, the same rule the
    // anchored contact and calendar walks use. See `validate_search_page` for
    // why the envelope is checked before this test rather than after, and for
    // why the "server ignores position" non-termination residual this comment
    // used to accept is in fact detectable.
    let more = if ids.is_empty() {
        false
    } else if let Some(total) = total {
        // `next_position` is non-negative (`validate_search_page` refused a
        // negative echo), so `unsigned_abs` is an exact widening and the
        // comparison happens in a domain that holds both sides. The previous
        // `i64::try_from(total).unwrap_or(i64::MAX)` substituted a sentinel
        // for an unrepresentable total instead of comparing it.
        u64::from(next_position.unsigned_abs()) < total
    } else {
        true
    };
    let next_cursor = more.then(|| encode_position(next_position, &query_state, owner));

    Ok(Page {
        truncated: false,
        items: ids,
        next_cursor,
        estimated_total: total,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

/// Check a search page's envelope against the invariants RFC 8620 s5.5 gives
/// it, and return the position a successor cursor would carry.
///
/// Four rules, each of which a conforming server satisfies for free. All are
/// checked BEFORE the termination test, so a broken envelope can never
/// present as a completed search - which is what the old
/// `next_position >= total` arm did on its own.
///
/// - The echoed `position` is an UnsignedInt in the response schema; a
///   negative one indexes nothing.
/// - On a NONEMPTY page `position` is the index of the first returned id and
///   `total` is the length of the whole result list, so
///   `position + served <= total`. A response claiming to have served past
///   the end of the list it just measured is CONTRADICTORY - it is not
///   evidence that the walk finished, which is exactly how `>= total` alone
///   read it.
/// - On a NONEMPTY continuation (a positive requested position, which only a
///   decoded cursor produces) the page must BEGIN where it was asked to. The
///   pinned `queryState` was verified equal just above, so the result list is
///   the same finite ordered list the previous page came from and there is
///   nothing for a conforming server to clamp against; an echo that
///   disagrees with the request means the window moved while the state did
///   not. Trusting the echo instead is what let a server silently re-serve
///   its own idea of the page.
/// - The successor must advance past the position this page resumed from.
///   Given the echo rule this is implied on a continuation and trivial on a
///   first page (`served >= 1` from position 0), so it does not bite at this
///   seam today; it is stated because it is the property TERMINATION rests
///   on, and it is the rule a relaxation of the echo check would otherwise
///   take away silently.
///
/// The last two are why the non-termination residual is detectable. It was
/// documented as undetectable - "a server that omits `total` and ignores
/// `position` is indistinguishable from an unbounded result set" - and that
/// is false. The query state is PINNED, so the result list is stable and
/// finite for the length of the walk; a walk that does not advance under an
/// unmoved state is a server contract violation, not a long list.
///
/// A count or sum that does not fit the cursor's `i32` position is
/// `SyncState(SchemaIncompatible)`: it is the CURSOR representation that
/// cannot carry the value, and the caller's answer is to restart the search
/// rather than to conclude anything about the server.
fn validate_search_page(
    requested: i32,
    served_from: i32,
    served: usize,
    total: Option<u64>,
    op: AccountOperation,
) -> Result<i32, AccountError> {
    if served_from < 0 {
        return Err(super::error::contract_violation(
            op,
            None,
            format!("Email/query answered with a negative position ({served_from})"),
        ));
    }
    if served > 0 && requested > 0 && served_from != requested {
        return Err(super::error::contract_violation(
            op,
            None,
            format!(
                "Email/query served page from position {served_from} for a continuation \
                 requested at {requested}, under an unmoved queryState"
            ),
        ));
    }
    let served_i32 = i32::try_from(served).map_err(|_| schema_incompatible_search_cursor(op))?;
    let next_position = served_from
        .checked_add(served_i32)
        .ok_or_else(|| schema_incompatible_search_cursor(op))?;
    if served > 0 {
        if let Some(total) = total
            && u64::from(next_position.unsigned_abs()) > total
        {
            return Err(super::error::contract_violation(
                op,
                None,
                format!(
                    "Email/query served {served} ids from position {served_from} of a result \
                     list it reports as {total} long"
                ),
            ));
        }
        if next_position <= requested {
            return Err(super::error::contract_violation(
                op,
                None,
                format!(
                    "Email/query did not advance past position {requested}: the successor \
                     would resume at {next_position}, under an unmoved queryState"
                ),
            ));
        }
    }
    Ok(next_position)
}

/// A decoded search page cursor: where in the result order the next page
/// starts, plus the `queryState` that order belonged to.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchCursor {
    position: i32,
    /// `None` only for the synthetic first page (no cursor supplied).
    query_state: Option<String>,
}

/// Build the `Email/query` a search page issues.
///
/// Split out from `search_email_ids` so the request shape is pinnable without
/// a live session; the sort in particular is load-bearing. RFC 8621 leaves the
/// order of an unsorted `Email/query` server-defined and guarantees nothing
/// about its stability across calls, while the page cursor here is a bare
/// integer position into that order. With no explicit comparator, paging a
/// search can duplicate and skip results on any server whose default order is
/// not stable, and returns a different order per server. Pin the same
/// `receivedAt desc` every inventory query in the crate pins.
fn build_search_query(
    request: SearchRequest,
    collapse_threads: bool,
    position: i32,
    limit: u32,
) -> Result<crate::email::EmailQuery, AccountError> {
    let mut query_request = crate::email::EmailQuery::new()
        .collapse_threads(collapse_threads)
        .sort([query::Comparator::new(crate::email::query::Comparator::ReceivedAt).descending()])
        .position(position)
        .limit(usize::try_from(limit).unwrap_or(usize::MAX))
        .calculate_total(true);
    if let Some(filter) = build_search_filter(request.filter, request.provider_query)? {
        query_request = query_request.filter(filter);
    }
    Ok(query_request)
}

/// Version tag of the current search-page-cursor payload. v1 was a bare
/// integer position with no `queryState` pin; v2 adds the pin. The two are
/// distinguishable without ambiguity - a v1 payload is entirely digits (with
/// an optional sign) and so can never contain the `SEARCH_CURSOR_SEP`.
const SEARCH_CURSOR_V2: &str = "2";

/// Separator inside the search cursor payload. `:` is safe as a payload
/// delimiter because only the first two fields are split on it: the
/// `queryState` is the whole remaining tail and is never re-split, so an
/// opaque state string containing `:` round-trips verbatim.
const SEARCH_CURSOR_SEP: char = ':';

/// Encode a search page cursor: `2:<position>:<queryState>` for the primary
/// account, and the owner-qualified form (the same object-id codec every
/// other foreign id uses) for a share.
///
/// The account has to ride in the cursor because the position means
/// nothing outside the account whose result order produced it. Without it,
/// a page-2 request whose filter routes elsewhere - a caller that reused
/// the cursor with a different `In`, or dropped the `In` entirely - pages
/// one account by another's offsets and silently duplicates and skips.
///
/// The `queryState` rides for the same class of reason one layer in: the
/// position means nothing outside the result ORDER that produced it either,
/// and that order is not stable across calls. Pinning it lets the next page
/// refuse honestly instead of paging a reordered list.
fn encode_position(position: i32, query_state: &str, owner: Option<&str>) -> Vec<u8> {
    qualify(
        owner,
        format!("{SEARCH_CURSOR_V2}{SEARCH_CURSOR_SEP}{position}{SEARCH_CURSOR_SEP}{query_state}"),
    )
    .into_bytes()
}

/// Decode a search page cursor and check it belongs to the account this
/// request routes to. A primary cursor is bare, so the absent-separator
/// case is "primary", not "unknown".
///
/// A v1 (bare-integer) cursor is REFUSED rather than upgraded in place. It
/// carries no `queryState`, so continuing it is exactly the unpinned paging
/// this version exists to stop, and silently treating it as pinned-to-
/// whatever-comes-back would launder the defect into the new shape. That is
/// the crate's standing cursor policy: an older payload version is
/// `SyncState(SchemaIncompatible)` and the caller restarts the walk.
fn decode_position(
    cursor: Option<&[u8]>,
    owner: Option<&str>,
    op: AccountOperation,
) -> Result<SearchCursor, AccountError> {
    let Some(bytes) = cursor else {
        return Ok(SearchCursor {
            position: 0,
            query_state: None,
        });
    };
    let text = std::str::from_utf8(bytes).map_err(|_| schema_incompatible_search_cursor(op))?;
    let (cursor_owner, payload) = match super::foreign::parse_object(text) {
        Some((account_id, payload)) => (Some(account_id), payload),
        None => (None, text),
    };
    if cursor_owner != owner {
        return Err(super::error::search_cursor_account_mismatch(
            op,
            cursor_owner,
            owner,
        ));
    }
    let mut parts = payload.splitn(3, SEARCH_CURSOR_SEP);
    let (Some(version), Some(position), Some(query_state)) =
        (parts.next(), parts.next(), parts.next())
    else {
        // Either a v1 bare integer or an unparseable payload; both are a
        // schema refusal, and neither can be paged from safely.
        return Err(schema_incompatible_search_cursor(op));
    };
    if version != SEARCH_CURSOR_V2 {
        return Err(schema_incompatible_search_cursor(op));
    }
    Ok(SearchCursor {
        position: position
            .parse::<i32>()
            .map_err(|_| schema_incompatible_search_cursor(op))?,
        query_state: Some(query_state.to_string()),
    })
}

fn build_search_filter(
    filter: Option<SearchFilter>,
    provider_query: Option<String>,
) -> Result<Option<query::Filter<crate::email::query::Filter>>, AccountError> {
    let mut filters = Vec::new();
    if let Some(filter) = filter {
        filters.push(search_filter_to_jmap(filter)?);
    }
    if let Some(provider_query) = provider_query
        && !provider_query.is_empty()
    {
        filters.push(crate::email::query::Filter::text(provider_query).into());
    }
    Ok(match filters.len() {
        0 => None,
        1 => filters.pop(),
        _ => Some(query::Filter::and(filters)),
    })
}

fn search_filter_to_jmap(
    filter: SearchFilter,
) -> Result<query::Filter<crate::email::query::Filter>, AccountError> {
    Ok(match filter {
        SearchFilter::From(value) => crate::email::query::Filter::from(value).into(),
        SearchFilter::To(value) => query::Filter::or([
            crate::email::query::Filter::to(value.clone()),
            crate::email::query::Filter::cc(value.clone()),
            crate::email::query::Filter::bcc(value),
        ]),
        SearchFilter::Subject(value) => crate::email::query::Filter::subject(value).into(),
        SearchFilter::Body(value) => crate::email::query::Filter::body(value).into(),
        SearchFilter::Has(value) if value.is_empty() => {
            crate::email::query::Filter::has_attachment(true).into()
        }
        SearchFilter::Has(value) => {
            let filters: Vec<query::Filter<crate::email::query::Filter>> = vec![
                crate::email::query::Filter::has_attachment(true).into(),
                crate::email::query::Filter::text(value).into(),
            ];
            query::Filter::and(filters)
        }
        // The owner qualifier is stripped here, never sent: `route_search`
        // already selected the owning account handle and refused a filter
        // that names two, so the native part is a mailbox id in exactly the
        // account this query addresses. Sending the qualified form is the
        // old defect - it matched nothing in the primary account and the
        // consumer got an empty page with no error.
        SearchFilter::In(container) => crate::email::query::Filter::in_mailbox(MailboxId::new(
            super::foreign::native_object(&container.0).to_string(),
        ))
        .into(),
        SearchFilter::Labeled(LabelId(label)) => {
            crate::email::query::Filter::has_keyword(label).into()
        }
        SearchFilter::DateRange { after, before } => {
            let mut filters: Vec<query::Filter<crate::email::query::Filter>> = Vec::new();
            if let Some(after) = after {
                filters.push(
                    crate::email::query::Filter::sent_after(system_time_to_unix(after)).into(),
                );
            }
            if let Some(before) = before {
                filters.push(
                    crate::email::query::Filter::sent_before(system_time_to_unix(before)).into(),
                );
            }
            query::Filter::and(filters)
        }
        SearchFilter::And(filters) => query::Filter::and(
            filters
                .into_iter()
                .map(search_filter_to_jmap)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Or(filters) => query::Filter::or(
            filters
                .into_iter()
                .map(search_filter_to_jmap)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Not(filter) => query::Filter::not([search_filter_to_jmap(*filter)?]),
        _ => {
            return Err(super::error::unsupported_error(
                AccountOperation::Search,
                None,
                "JMAP does not support this SearchFilter variant",
            ));
        }
    })
}

async fn build_email_create_from_send<T: HttpTransport>(
    mail: &MailAccount<T>,
    request: bifrost_types::SendRequest,
    mailbox: MailboxId,
) -> Result<crate::email::EmailCreate, AccountError> {
    // RFC 8098 read receipt: targets the resolved sender. JMAP carries
    // arbitrary headers as structured Email properties, so this becomes a
    // `Disposition-Notification-To` address header on the create. Captured
    // before `request` is consumed field-by-field below.
    let read_receipt_to = request
        .request_read_receipt
        .then(|| request.from.clone())
        .flatten();
    let mut patch = bifrost_types::DraftPatch::default();
    patch.identity = request.identity;
    patch.from = Some(request.from);
    patch.to = Some(request.to);
    patch.cc = Some(request.cc);
    patch.bcc = Some(request.bcc);
    patch.reply_to = Some(request.reply_to);
    patch.subject = Some(request.subject);
    patch.body_text = Some(request.body_text);
    patch.body_html = Some(request.body_html);
    patch.attachments_inline = Some(request.attachments_inline);
    patch.attachments_uploaded = Some(request.attachments_uploaded);
    patch.in_reply_to = Some(request.in_reply_to);
    patch.references = Some(request.references);
    let mut create =
        build_email_create_from_draft(mail, patch, mailbox, AccountOperation::Send).await?;
    if let Some(addr) = read_receipt_to {
        create.header(
            crate::email::Header {
                name: "Disposition-Notification-To".to_string(),
                form: crate::email::HeaderForm::Addresses,
                all: false,
            },
            crate::email::HeaderValue::AsAddresses(vec![address_to_jmap(addr)]),
        );
    }
    Ok(create)
}

async fn build_email_create_from_draft<T: HttpTransport>(
    mail: &MailAccount<T>,
    patch: bifrost_types::DraftPatch,
    mailbox: MailboxId,
    op: AccountOperation,
) -> Result<crate::email::EmailCreate, AccountError> {
    let mut create = crate::email::EmailCreate::new(None);
    let body_patch = patch.clone();
    create.mailbox_ids([mailbox]);
    create.keywords([DRAFT_KEYWORD]);
    if let Some(from) = patch.from.flatten() {
        create.from([address_to_jmap(from)]);
    }
    if let Some(to) = patch.to {
        create.to(to.into_iter().map(address_to_jmap));
    }
    if let Some(cc) = patch.cc {
        create.cc(cc.into_iter().map(address_to_jmap));
    }
    if let Some(bcc) = patch.bcc {
        create.bcc(bcc.into_iter().map(address_to_jmap));
    }
    if let Some(reply_to) = patch.reply_to {
        create.reply_to(reply_to.into_iter().map(address_to_jmap));
    }
    if let Some(Some(subject)) = patch.subject {
        create.subject(subject);
    }
    if let Some(Some(in_reply_to)) = patch.in_reply_to {
        create.in_reply_to([in_reply_to]);
    }
    if let Some(references) = patch.references {
        create.references(references);
    }
    apply_body_to_create(mail, &mut create, body_patch, op).await?;
    Ok(create)
}

async fn apply_body_to_create<T: HttpTransport>(
    mail: &MailAccount<T>,
    create: &mut crate::email::EmailCreate,
    patch: bifrost_types::DraftPatch,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let body = build_body(
        mail,
        patch.body_text.flatten(),
        patch.body_html.flatten(),
        patch.attachments_inline.unwrap_or_default(),
        patch.attachments_uploaded.unwrap_or_default(),
        op,
    )
    .await?;
    if let Some(structure) = body.body_structure {
        create.body_structure(structure);
    }
    for (id, value) in body.body_values {
        create.body_value(id, value);
    }
    for part in body.text_body {
        create.text_body(part);
    }
    for part in body.html_body {
        create.html_body(part);
    }
    for part in body.attachments {
        create.attachment(part);
    }
    Ok(())
}

async fn apply_draft_patch_to_email_patch<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_patch: &mut EmailPatch,
    patch: bifrost_types::DraftPatch,
    op: AccountOperation,
) -> Result<(), AccountError> {
    if let Some(from) = patch.from {
        match from {
            Some(value) => {
                email_patch
                    .raw_property("from", &vec![address_to_jmap(value)])
                    .map_err(crate::Error::from)
                    .map_err(to_acct_err(op))?;
            }
            None => {
                email_patch.null_property("from");
            }
        }
    }
    if let Some(to) = patch.to {
        let values = to.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("to", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(cc) = patch.cc {
        let values = cc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("cc", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(bcc) = patch.bcc {
        let values = bcc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("bcc", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(reply_to) = patch.reply_to {
        let values = reply_to
            .into_iter()
            .map(address_to_jmap)
            .collect::<Vec<_>>();
        email_patch
            .raw_property("replyTo", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(subject) = patch.subject {
        if let Some(subject) = subject {
            email_patch.subject(subject);
        } else {
            email_patch.null_property("subject");
        }
    }
    if let Some(in_reply_to) = patch.in_reply_to {
        if let Some(in_reply_to) = in_reply_to {
            email_patch
                .raw_property("inReplyTo", &vec![in_reply_to])
                .map_err(crate::Error::from)
                .map_err(to_acct_err(op))?;
        } else {
            email_patch.null_property("inReplyTo");
        }
    }
    if let Some(references) = patch.references {
        email_patch
            .raw_property("references", &references)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if patch.body_text.is_some()
        || patch.body_html.is_some()
        || patch.attachments_inline.is_some()
        || patch.attachments_uploaded.is_some()
    {
        let body = build_body(
            mail,
            patch.body_text.flatten(),
            patch.body_html.flatten(),
            patch.attachments_inline.unwrap_or_default(),
            patch.attachments_uploaded.unwrap_or_default(),
            op,
        )
        .await?;
        if let Some(structure) = body.body_structure {
            email_patch
                .raw_property("bodyStructure", &structure)
                .map_err(crate::Error::from)
                .map_err(to_acct_err(op))?;
        }
        email_patch
            .raw_property("bodyValues", &body.body_values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("textBody", &body.text_body)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("htmlBody", &body.html_body)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("attachments", &body.attachments)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    Ok(())
}

struct BuiltBody {
    body_structure: Option<EmailBodyPart>,
    body_values: HashMap<String, EmailBodyValue>,
    text_body: Vec<EmailBodyPart>,
    html_body: Vec<EmailBodyPart>,
    attachments: Vec<EmailBodyPart>,
}

async fn build_body<T: HttpTransport>(
    mail: &MailAccount<T>,
    text: Option<String>,
    html: Option<String>,
    inline: Vec<bifrost_types::AttachmentInline>,
    uploaded: Vec<bifrost_types::AttachmentHandle>,
    op: AccountOperation,
) -> Result<BuiltBody, AccountError> {
    let mut body_values = HashMap::new();
    let mut text_body = Vec::new();
    let mut html_body = Vec::new();
    let mut body_parts = Vec::new();

    if let Some(text) = text {
        let part = EmailBodyPart::new()
            .with_part_id("text")
            .with_content_type("text/plain");
        body_values.insert("text".to_string(), EmailBodyValue::from(text));
        text_body.push(part.clone());
        body_parts.push(part);
    }
    if let Some(html) = html {
        let part = EmailBodyPart::new()
            .with_part_id("html")
            .with_content_type("text/html");
        body_values.insert("html".to_string(), EmailBodyValue::from(html));
        html_body.push(part.clone());
        body_parts.push(part);
    }
    if body_parts.is_empty() {
        let part = EmailBodyPart::new()
            .with_part_id("text")
            .with_content_type("text/plain");
        body_values.insert("text".to_string(), EmailBodyValue::from(""));
        text_body.push(part.clone());
        body_parts.push(part);
    }

    let mut attachments = Vec::new();
    for attachment in inline {
        let blob = mail
            .upload(attachment.data.to_vec(), Some(&attachment.mime))
            .await
            .map_err(to_acct_err(op))?;
        let mut part = EmailBodyPart::new()
            .with_blob_id(blob.blob_id)
            .with_name(attachment.filename)
            .with_content_type(attachment.mime);
        // Carry the Content-ID so a `cid:` reference in the HTML body
        // resolves to this part. JMAP's `cid` property is the bare token
        // (no angle brackets); strip any the caller supplied.
        if let Some(cid) = attachment.content_id.as_deref().map(|cid| {
            cid.trim()
                .trim_matches(|c| c == '<' || c == '>')
                .to_string()
        }) && !cid.is_empty()
        {
            part = part.with_content_id(cid);
        }
        attachments.push(part);
    }
    for handle in uploaded {
        let (blob_id, mime) = decode_attachment_handle(&handle.0);
        let mut part = EmailBodyPart::new().with_blob_id(crate::core::id::BlobId::new(blob_id));
        if let Some(mime) = mime {
            part = part.with_content_type(mime);
        }
        attachments.push(part);
    }

    let content = if body_parts.len() == 1 {
        body_parts.remove(0)
    } else {
        body_parts.into_iter().fold(
            EmailBodyPart::new().with_content_type("multipart/alternative"),
            EmailBodyPart::with_sub_part,
        )
    };
    let body_structure = if attachments.is_empty() {
        Some(content)
    } else {
        let mut mixed = EmailBodyPart::new()
            .with_content_type("multipart/mixed")
            .with_sub_part(content);
        for attachment in &attachments {
            mixed = mixed.with_sub_part(attachment.clone());
        }
        Some(mixed)
    };

    Ok(BuiltBody {
        body_structure,
        body_values,
        text_body,
        html_body,
        attachments,
    })
}

fn encode_attachment_handle(blob_id: &str, mime: Option<&str>) -> String {
    match mime {
        Some(mime) => format!("{ATTACHMENT_HANDLE_PREFIX}{blob_id}\n{mime}"),
        None => format!("{ATTACHMENT_HANDLE_PREFIX}{blob_id}"),
    }
}

fn decode_attachment_handle(handle: &str) -> (String, Option<String>) {
    let raw = handle
        .strip_prefix(ATTACHMENT_HANDLE_PREFIX)
        .unwrap_or(handle);
    let mut parts = raw.splitn(2, '\n');
    let blob = parts.next().unwrap_or("").to_string();
    let mime = parts.next().filter(|v| !v.is_empty()).map(str::to_string);
    (blob, mime)
}

fn address_to_jmap(address: bifrost_types::Address) -> JmapEmailAddress {
    let mut out = JmapEmailAddress::new(address.address);
    if let Some(name) = address.name {
        out = out.with_name(name);
    }
    out
}

fn address_from_jmap(address: &JmapEmailAddress) -> bifrost_types::Address {
    bifrost_types::Address {
        name: address.name().map(str::to_string),
        address: address.email().to_string(),
    }
}

fn submission_address_from_compose(address: &bifrost_types::Address) -> SubmissionAddress {
    SubmissionAddress::new(address.address.clone())
}

/// Format an absolute instant as RFC 3339 / ISO 8601 UTC for the SMTP
/// FUTURERELEASE `holduntil` envelope parameter.
fn rfc3339(at: SystemTime) -> String {
    jiff::Timestamp::try_from(at)
        .map(|ts| ts.to_string())
        .unwrap_or_default()
}

/// A scheduled JMAP send needs a concrete mailFrom and recipients to carry
/// the hold parameter. A caller-selected Identity can supply mailFrom when
/// `from` is absent; without either, JMAP defines no default identity to use.
/// Maps to `Request(Malformed)`.
fn scheduled_requires_envelope() -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        bifrost_types::Cause::Request(bifrost_types::RequestCause::Malformed {
            detail: bifrost_types::DiagnosticText::user_safe(
                "Scheduled send requires a from address or selected identity and at least one recipient.",
            ),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(AccountOperation::Send)
    .try_build()
    .expect("valid account error classification")
}

/// Cancel a scheduled JMAP submission by id: `EmailSubmission/set`
/// update setting `undoStatus: canceled`.
pub(crate) fn cancel_scheduled_send<T: HttpTransport>(
    submission_account: MailAccount<T>,
    handle: ObjectId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let submission_id = crate::email_submission::EmailSubmissionId::from(handle.0.as_str());
        let mut set = EmailSubmissionSet::new();
        set.update(submission_id.clone())
            .undo_status(UndoStatus::Canceled);
        let mut batch = submission_account.build();
        let handle_ref = batch
            .call(set)
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        let mut response = batch
            .send()
            .await
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        let mut set_response = response
            .get(&handle_ref)
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        set_response
            .updated(&submission_id)
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        Ok(())
    })
}

/// Reschedule a scheduled JMAP submission. JMAP has no in-place
/// reschedule: cancel the existing submission and create a new one
/// referencing the same `emailId` with the new `holduntil`. Returns the
/// new submission id.
pub(crate) fn reschedule_send<T: HttpTransport>(
    submission_account: MailAccount<T>,
    max_delayed_send: usize,
    handle: ObjectId,
    scheduled: SystemTime,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let window = std::time::Duration::from_secs(max_delayed_send as u64);
        bifrost_types::validate_scheduled(scheduled, Some(window))?;

        let submission_id = crate::email_submission::EmailSubmissionId::new(handle.0.as_str());

        // Fetch the existing submission to recover its emailId and
        // envelope (the mailFrom we must restamp with the new hold).
        let mut existing = submission_account
            .call(crate::email_submission::EmailSubmissionGet::new().ids([submission_id.clone()]))
            .await
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        let existing = existing
            .pop()
            .ok_or_else(|| reschedule_not_found(&handle.0))?;
        let email_id = existing
            .email_id()
            .cloned()
            .ok_or_else(|| reschedule_missing_field("the scheduled submission has no emailId"))?;

        // A save_to_sent=false schedule may have destroyed its Email when
        // the submission was created. Confirm the source still exists before
        // canceling anything, so a replacement cannot fail on a missing
        // email after the original schedule has been canceled.
        let email_get = EmailGet::new()
            .ids([email_id.clone()])
            .properties([EmailProperty::Id]);
        let email_response = submission_account
            .call(email_get)
            .await
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        if !email_response
            .list()
            .iter()
            .any(|email| email.id() == Some(&email_id))
        {
            if email_response.not_found().contains(&email_id) {
                return Err(reschedule_not_found(email_id.as_str()));
            }
            return Err(super::error::get_id_unanswered(
                email_id.as_str(),
                super::error::JmapErrorContext::new(AccountOperation::RescheduleSend),
            ));
        }

        let mail_from_email = existing
            .mail_from()
            .ok_or_else(|| reschedule_missing_field("the scheduled submission has no envelope"))?
            .email()
            .to_string();
        let rcpt_to: Vec<String> = existing
            .rcpt_to()
            .unwrap_or(&[])
            .iter()
            .map(|a| a.email().to_string())
            .collect();

        // RFC 8620 processes creates before updates within one set call. Keep
        // the cancellation in its own call and verify it before creating the
        // replacement, or both submissions could remain live.
        let mut cancel = EmailSubmissionSet::new();
        cancel
            .update(submission_id.clone())
            .undo_status(UndoStatus::Canceled);
        let mut cancel_response = submission_account
            .call(cancel)
            .await
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        cancel_response
            .updated(&submission_id)
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;

        let mut set = EmailSubmissionSet::new();
        {
            let submit = set.create_with_id(SUBMISSION_CREATE_ID);
            submit.email_id(email_id);
            let mail_from = SubmissionAddress::new(mail_from_email)
                .with_parameter("holduntil", Some(rfc3339(scheduled)));
            submit.envelope(mail_from, rcpt_to.into_iter().map(SubmissionAddress::new));
        }
        let mut set_response = submission_account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::RescheduleSend))
            .map_err(|error| incomplete_submission(
                AccountOperation::RescheduleSend,
                error,
                "The old schedule was canceled; replacement scheduling failed or is unconfirmed. Reconcile the submission before scheduling again.",
            ))?;
        let mut new_submission = set_response
            .created(SUBMISSION_CREATE_ID)
            .map_err(to_acct_err(AccountOperation::RescheduleSend))
            .map_err(|error| incomplete_submission(
                AccountOperation::RescheduleSend,
                error,
                "The old schedule was canceled; replacement scheduling failed or is unconfirmed. Reconcile the submission before scheduling again.",
            ))?;
        Ok(ObjectId(new_submission.take_id().into_string()))
    })
}

fn reschedule_missing_field(detail: &'static str) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField),
        bifrost_types::Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol: bifrost_types::Protocol::Jmap,
            detail: Some(bifrost_types::DiagnosticText::support_only(detail)),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(AccountOperation::RescheduleSend)
    .try_build()
    .expect("valid account error classification")
}

fn reschedule_not_found(id: &str) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message),
        bifrost_types::Cause::Request(bifrost_types::RequestCause::NotFound {
            what: bifrost_types::ResourceKind::Message,
            id: Some(id.to_string()),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(AccountOperation::RescheduleSend)
    .try_build()
    .expect("valid account error classification")
}

fn message_properties(projection: HydrationProjection) -> Vec<EmailProperty> {
    let mut properties = vec![
        EmailProperty::Id,
        EmailProperty::ThreadId,
        EmailProperty::MailboxIds,
        EmailProperty::Keywords,
        EmailProperty::From,
        EmailProperty::To,
        EmailProperty::Cc,
        EmailProperty::Bcc,
        EmailProperty::ReplyTo,
        EmailProperty::Subject,
        EmailProperty::SentAt,
        EmailProperty::ReceivedAt,
        EmailProperty::Size,
        EmailProperty::InReplyTo,
        EmailProperty::References,
        EmailProperty::BlobId,
    ];
    if matches!(
        projection,
        HydrationProjection::Preview(_)
            | HydrationProjection::Full
            | HydrationProjection::FullWithBlobs
    ) {
        properties.extend([
            EmailProperty::Preview,
            EmailProperty::TextBody,
            EmailProperty::HtmlBody,
            EmailProperty::BodyValues,
            EmailProperty::Attachments,
        ]);
    }
    properties
}

fn body_properties() -> Vec<BodyProperty> {
    vec![
        BodyProperty::PartId,
        BodyProperty::BlobId,
        BodyProperty::Size,
        BodyProperty::Name,
        BodyProperty::Type,
        BodyProperty::Cid,
        BodyProperty::Disposition,
    ]
}

fn email_to_message(email: Email, projection: HydrationProjection) -> Message {
    let id = ObjectId(email.id().map(ToString::to_string).unwrap_or_default());
    let thread_id = email.thread_id().map(|id| ThreadId(id.to_string()));
    let containers = email
        .mailbox_ids()
        .into_iter()
        .map(|id| ContainerId(id.to_string()))
        .collect();
    let flags = email
        .keywords()
        .into_iter()
        .map(str::to_string)
        .collect::<HashSet<_>>();
    let body_text = if matches!(
        projection,
        HydrationProjection::Preview(_)
            | HydrationProjection::Full
            | HydrationProjection::FullWithBlobs
    ) {
        collect_body_values(email.text_body(), &email)
            .or_else(|| email.preview().map(str::to_string))
    } else {
        None
    };
    let body_html = if matches!(
        projection,
        HydrationProjection::Full | HydrationProjection::FullWithBlobs
    ) {
        collect_body_values(email.html_body(), &email)
    } else {
        None
    };

    Message {
        id,
        thread_id,
        from: email
            .from()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        to: email
            .to()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        cc: email
            .cc()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        bcc: email
            .bcc()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        reply_to: email
            .reply_to()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        subject: email.subject().map(str::to_string),
        date: email
            .sent_at()
            .or_else(|| email.received_at())
            .and_then(unix_to_system_time),
        containers,
        importance: if flags.contains("$important") {
            Importance::High
        } else {
            Importance::Normal
        },
        flags,
        body_text,
        body_html,
        attachments: email
            .attachments()
            .unwrap_or_default()
            .iter()
            .filter_map(attachment_from_part)
            .collect(),
        size_bytes: u64::try_from(email.size()).ok(),
        in_reply_to: email.in_reply_to().and_then(|ids| ids.first().cloned()),
        references: email.references().map(<[_]>::to_vec).unwrap_or_default(),
        incomplete: false,
    }
}

fn collect_body_values(parts: Option<&[EmailBodyPart]>, email: &Email) -> Option<String> {
    let mut values = Vec::new();
    for part in parts.unwrap_or_default() {
        if let Some(part_id) = part.part_id()
            && let Some(value) = email.body_value(part_id)
        {
            values.push(value.value().to_string());
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values.join("\n"))
    }
}

fn attachment_from_part(part: &EmailBodyPart) -> Option<MessageAttachment> {
    let id = part.blob_id()?.to_string();
    let size = u64::try_from(part.size()).ok();
    let content_type = part.content_type().map(str::to_string);
    Some(MessageAttachment {
        filename: part.name().map(str::to_string),
        content_type: content_type.clone(),
        content_id: part.content_id().map(str::to_string),
        inline: part.content_disposition() == Some("inline"),
        size,
        source: AttachmentSource::Blob(BlobHandle {
            id: BlobId(id),
            size,
            content_type,
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Raw8Bit,
            },
        }),
        truncated: false,
    })
}

fn system_time_to_unix(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(err) => -i64::try_from(err.duration().as_secs()).unwrap_or(i64::MAX),
    }
}

fn unix_to_system_time(timestamp: i64) -> Option<SystemTime> {
    if timestamp >= 0 {
        Some(UNIX_EPOCH + std::time::Duration::from_secs(u64::try_from(timestamp).ok()?))
    } else {
        UNIX_EPOCH.checked_sub(std::time::Duration::from_secs(timestamp.unsigned_abs()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mailbox_json(id: &str, parent: Option<&str>, rights: serde_json::Value) -> Mailbox {
        let mut value = serde_json::json!({
            "id": id,
            "name": "Reports",
            "myRights": rights,
            "isSubscribed": true,
        });
        if let Some(parent) = parent {
            value["parentId"] = serde_json::Value::String(parent.to_string());
        }
        serde_json::from_value(value).expect("mailbox deserializes")
    }

    /// Answers one `Mailbox/get` with two shared mailboxes: one that
    /// reports `myRights`, one that omits it.
    #[derive(Clone)]
    struct RightsTransport;

    impl HttpTransport for RightsTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value =
                serde_json::from_slice(&body).expect("the client emits JSON");
            let call = &request["methodCalls"][0];
            assert_eq!(call[0], "Mailbox/get");
            let call_id = call[2].clone();
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[
                    "Mailbox/get",
                    {
                        "accountId": "shared",
                        "state": "s1",
                        "list": [
                            {
                                "id": "mbx-rights",
                                "name": "Reports",
                                "isSubscribed": true,
                                "myRights": {
                                    "mayReadItems": true, "mayAddItems": false,
                                    "mayRemoveItems": false, "maySetSeen": true,
                                    "maySetKeywords": false, "mayCreateChild": false,
                                    "mayRename": false, "mayDelete": false,
                                    "maySubmit": false
                                }
                            },
                            {
                                "id": "mbx-bare",
                                "name": "Silent",
                                "isSubscribed": true
                            }
                        ],
                        "notFound": []
                    },
                    call_id
                ]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    /// A share whose `Mailbox/get` answers without `myRights` projected
    /// as `rights: None` - byte-identical to a protocol that has no
    /// rights notion at all - so the sidebar could not tell "this share
    /// is read-only" from "rights were never reported". The container
    /// still lists (a share with unknown rights beats a missing share),
    /// but the omission is now a `SkippedScope` naming that container.
    #[tokio::test]
    async fn a_share_omitting_my_rights_is_reported_as_a_degradation() {
        let client = crate::client::Client::with_transport(
            RightsTransport,
            search_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mut foreign = HashMap::new();
        foreign.insert(
            "shared".to_string(),
            crate::account::Account::new(client, "shared"),
        );

        let (containers, skipped) = super::fetch_foreign_containers(
            &foreign,
            &HashMap::new(),
            AccountOperation::ContainersList,
        )
        .await;

        // Both shares are still listed.
        assert_eq!(containers.len(), 2);
        let bare = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-bare").0);
        assert!(
            containers
                .iter()
                .find(|c| c.id == bare)
                .expect("the bare share is listed")
                .rights
                .is_none()
        );

        // Exactly the rights-less one is classified.
        assert_eq!(skipped.len(), 1);
        assert_eq!(
            skipped[0].scope,
            bifrost_types::ErrorScope::Mailbox {
                id: bare.0.clone().into()
            }
        );
        assert_eq!(
            *skipped[0].error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::MissingField
            )
        );
    }

    /// A thread search is a query for emails projected onto their
    /// threads, so an email the projection cannot place is a hit the
    /// caller asked for and never receives. All three ways that happens -
    /// declared `notFound`, answered in neither list, and returned
    /// without the mandatory `threadId` - belong in `failed_ids`, and one
    /// thread hit by two messages is still one thread.
    #[test]
    fn search_emails_without_a_thread_ride_failed_ids() {
        let email = |value: serde_json::Value| -> Email {
            serde_json::from_value(value).expect("email deserializes")
        };
        let (items, failed_ids) = reconcile_search_threads(
            vec![
                "M0".to_string(),
                "M1".to_string(),
                "M2".to_string(),
                "M3".to_string(),
                "M4".to_string(),
            ],
            &[EmailId::new("M0")],
            vec![
                email(serde_json::json!({"id": "M1", "threadId": "T1"})),
                // Same thread as M1: one thread, one item.
                email(serde_json::json!({"id": "M2", "threadId": "T1"})),
                // Returned, but unplaceable - `threadId` is mandatory.
                email(serde_json::json!({"id": "M3"})),
            ],
        );

        assert_eq!(items, vec![ThreadId("T1".to_string())]);
        let mut failed_ids = failed_ids;
        failed_ids.sort();
        assert_eq!(
            failed_ids,
            vec!["M0".to_string(), "M3".to_string(), "M4".to_string()]
        );
    }

    /// A JMAP boundary that expands one thread to a growing message set:
    /// the first `Thread/get` answers one message, every later one answers
    /// two. A `move_thread` that resolved the thread per leg would add
    /// only M1 to the target and then remove BOTH M1 and M2 from the
    /// source, stranding M2 with no membership at all.
    struct GrowingThreadTransport {
        thread_gets: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        sets: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl crate::core::transport::HttpTransport for GrowingThreadTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let arguments = match name.as_str() {
                "Thread/get" => {
                    let seen = self
                        .thread_gets
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let email_ids = if seen == 0 {
                        serde_json::json!(["M1"])
                    } else {
                        serde_json::json!(["M1", "M2"])
                    };
                    serde_json::json!({
                        "accountId": "primary",
                        "state": "t1",
                        "list": [{"id": "T1", "emailIds": email_ids}],
                        "notFound": []
                    })
                }
                // The email-state probe the mutation door makes before its
                // conditional `Email/set`.
                "Email/get" => serde_json::json!({
                    "accountId": "primary",
                    "state": "s1",
                    "list": [],
                    "notFound": []
                }),
                "Email/set" => {
                    self.sets.lock().expect("sets").push(call[1].clone());
                    let updated: serde_json::Map<String, serde_json::Value> = call[1]["update"]
                        .as_object()
                        .expect("update map")
                        .keys()
                        .map(|id| (id.clone(), serde_json::Value::Null))
                        .collect();
                    serde_json::json!({
                        "accountId": "primary",
                        "oldState": "s1",
                        "newState": "s2",
                        "updated": updated
                    })
                }
                other => panic!("unexpected method {other}"),
            };
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    /// The two legs of a thread move must run over ONE resolved id set.
    /// Re-resolving between them let a message that arrived in the middle
    /// be removed from the source without ever being added to the target.
    #[tokio::test]
    async fn a_thread_move_resolves_the_thread_once_for_both_legs() {
        let thread_gets = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sets = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let session: crate::core::session::Session = serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses");
        let client = crate::client::Client::with_transport(
            GrowingThreadTransport {
                thread_gets: std::sync::Arc::clone(&thread_gets),
                sets: std::sync::Arc::clone(&sets),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client, "primary");
        let states: StateMap =
            std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

        move_thread(
            mail,
            states,
            "primary".to_string(),
            ThreadId("T1".to_string()),
            ContainerId("archive".to_string()),
            Some(ContainerId("inbox".to_string())),
        )
        .await
        .expect("move succeeds");

        assert_eq!(
            thread_gets.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the thread must be resolved once for both legs"
        );
        let sets = sets.lock().expect("sets");
        assert_eq!(sets.len(), 2, "two legs, one Email/set each");
        let ids_of = |call: &serde_json::Value| {
            let mut ids: Vec<String> = call["update"]
                .as_object()
                .expect("update map")
                .keys()
                .cloned()
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(
            ids_of(&sets[0]),
            ids_of(&sets[1]),
            "both legs must address the same message set"
        );
    }

    /// The search page cursor is a bare integer position, so the query it
    /// pages MUST carry an explicit comparator: RFC 8621 gives an unsorted
    /// `Email/query` a server-defined order with no stability guarantee, and
    /// paging an unstable order duplicates and skips results.
    #[test]
    fn a_search_query_pins_its_sort_order() {
        let mut request = SearchRequest::default();
        request.limit = Some(10);
        let query = build_search_query(request, true, 20, 10).expect("filter is supported");
        let value = serde_json::to_value(&query).expect("query serializes");

        assert_eq!(
            value.get("sort"),
            Some(&serde_json::json!([
                {"isAscending": false, "property": "receivedAt"}
            ]))
        );
        assert_eq!(value.get("position"), Some(&serde_json::json!(20)));
        assert_eq!(value.get("limit"), Some(&serde_json::json!(10)));
        assert_eq!(value.get("collapseThreads"), Some(&serde_json::json!(true)));
    }

    /// Scripted `Email/query` + `Email/get` server for the search routing
    /// tests. Records every request so the account the query addressed and
    /// the ids it carried can be asserted.
    #[derive(Clone)]
    struct SearchTransport {
        requests: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl SearchTransport {
        fn new() -> Self {
            Self {
                requests: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn calls(&self, method: &str) -> Vec<serde_json::Value> {
            self.requests
                .lock()
                .expect("requests")
                .iter()
                .filter(|call| call[0] == serde_json::json!(method))
                .map(|call| call[1].clone())
                .collect()
        }
    }

    impl crate::core::transport::HttpTransport for SearchTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            self.requests.lock().expect("requests").push(call.clone());
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let account_id = call[1]["accountId"].clone();
            let arguments = match name.as_str() {
                "Email/query" => serde_json::json!({
                    "accountId": account_id,
                    "queryState": "q-1",
                    "canCalculateChanges": false,
                    "position": call[1]["position"].clone(),
                    // Three hits, one served per page: a page is short of
                    // `total`, so the walk continues on `total`, not on the
                    // page having come back full.
                    "total": 3,
                    "ids": ["M1"]
                }),
                "Email/get" => serde_json::json!({
                    "accountId": account_id,
                    "state": "s-1",
                    "list": [{"id": "M1", "threadId": "T1"}],
                    "notFound": []
                }),
                other => panic!("unexpected method {other}"),
            };
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": [[name, arguments, call_id]]
                })
                .to_string(),
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    fn search_session() -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses")
    }

    /// Primary handle, foreign routing table, and the scripted transport
    /// backing both.
    type SearchHandles = (
        MailAccount<SearchTransport>,
        Arc<HashMap<String, MailAccount<SearchTransport>>>,
        SearchTransport,
    );

    /// Primary handle plus one registered share (`shared`), over one
    /// scripted transport.
    fn search_handles() -> SearchHandles {
        let transport = SearchTransport::new();
        let client = crate::client::Client::with_transport(
            transport.clone(),
            search_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let primary = crate::account::Account::new(client.clone(), "primary");
        let mut foreign = HashMap::new();
        foreign.insert(
            "shared".to_string(),
            crate::account::Account::new(client, "shared"),
        );
        (primary, Arc::new(foreign), transport)
    }

    fn in_container(id: ContainerId) -> SearchRequest {
        let mut request = SearchRequest::default();
        request.filter = Some(SearchFilter::In(id));
        request.limit = Some(1);
        request
    }

    /// The defect: a `SearchFilter::In` naming a shared mailbox sent the
    /// owner-QUALIFIED id as `inMailbox` to the PRIMARY account, which
    /// matches nothing, so the consumer got an empty page and no error.
    /// The query must address the owner with the native mailbox id, and
    /// the hits must come back in the foreign object namespace.
    #[tokio::test]
    async fn a_foreign_container_search_routes_to_its_owner_account() {
        let (primary, foreign, transport) = search_handles();
        let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);

        let page = search_messages(primary, foreign, in_container(container))
            .await
            .expect("search succeeds");

        let queries = transport.calls("Email/query");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0]["accountId"], serde_json::json!("shared"));
        assert_eq!(
            queries[0]["filter"]["inMailbox"],
            serde_json::json!("mbx-1"),
            "the owner qualifier must be stripped from the wire filter"
        );
        assert_eq!(
            page.items,
            vec![ObjectId(super::super::foreign::encode_object(
                "shared", "M1"
            ))]
        );
    }

    /// The thread door projects query hits through `Email/get`. Both legs
    /// must run against the owner, and the thread ids must be qualified:
    /// a bare foreign thread id asserts primary ownership, and every
    /// thread-keyed door would then act on an unrelated primary thread.
    #[tokio::test]
    async fn a_foreign_thread_search_qualifies_its_thread_ids() {
        let (primary, foreign, transport) = search_handles();
        let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);

        let page = search(primary, foreign, in_container(container))
            .await
            .expect("search succeeds");

        let gets = transport.calls("Email/get");
        assert_eq!(gets.len(), 1);
        assert_eq!(gets[0]["accountId"], serde_json::json!("shared"));
        assert_eq!(
            gets[0]["ids"],
            serde_json::json!(["M1"]),
            "the follow-up get sends native ids to the owner"
        );
        assert_eq!(
            page.items,
            vec![ThreadId(super::super::foreign::encode_object(
                "shared", "T1"
            ))]
        );
    }

    /// An `In` naming a share this session cannot reach must fail
    /// honestly, before the wire. Falling back to the primary account is
    /// the silent-empty-result defect in a different costume.
    #[tokio::test]
    async fn an_unregistered_foreign_container_search_is_refused() {
        let (primary, foreign, transport) = search_handles();
        let container = ContainerId(super::super::foreign::encode_foreign("ghost", "mbx-1").0);

        let error = search_messages(primary, foreign, in_container(container))
            .await
            .expect_err("an unreachable share is an error");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(
            transport.calls("Email/query").is_empty(),
            "nothing may reach the wire"
        );
    }

    /// One `Email/query` carries one `accountId`. Two `In` filters naming
    /// different owners is an error, not an implicit union and not a
    /// silent pick of one of them.
    #[tokio::test]
    async fn a_mixed_owner_search_filter_is_refused() {
        let (primary, foreign, transport) = search_handles();
        let mut request = SearchRequest::default();
        request.filter = Some(SearchFilter::And(vec![
            SearchFilter::In(ContainerId(
                super::super::foreign::encode_foreign("shared", "mbx-1").0,
            )),
            SearchFilter::In(ContainerId("mbx-1".to_string())),
        ]));

        let error = search_messages(primary, foreign, request)
            .await
            .expect_err("a cross-account filter is an error");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(transport.calls("Email/query").is_empty());
    }

    /// A search page cursor is a bare position into ONE account's result
    /// order. It must name that account so a page 2 cannot be paged
    /// against another one - which would duplicate and skip silently.
    #[tokio::test]
    async fn a_foreign_search_cursor_resumes_on_the_same_account() {
        let (primary, foreign, transport) = search_handles();
        let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);

        let page = search_messages(
            primary.clone(),
            Arc::clone(&foreign),
            in_container(container),
        )
        .await
        .expect("search succeeds");
        let cursor = page.next_cursor.expect("a full page has a next cursor");
        assert_eq!(
            String::from_utf8(cursor.clone()).expect("utf8"),
            super::super::foreign::encode_object("shared", "2:1:q-1"),
            "the cursor names the account it was minted against, \
             the position it resumes at, and the queryState it belongs to"
        );

        // Page 2 with the same filter resumes on the same account at the
        // decoded position.
        let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);
        let mut resume = in_container(container);
        resume.page_cursor = Some(cursor.clone());
        search_messages(primary.clone(), Arc::clone(&foreign), resume)
            .await
            .expect("resume succeeds");
        let queries = transport.calls("Email/query");
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[1]["accountId"], serde_json::json!("shared"));
        assert_eq!(queries[1]["position"], serde_json::json!(1));

        // The same cursor replayed with a primary-routing request is
        // refused rather than paging the primary account by the share's
        // offsets.
        let mut crossed = SearchRequest::default();
        crossed.limit = Some(1);
        crossed.page_cursor = Some(cursor);
        let error = search_messages(primary, foreign, crossed)
            .await
            .expect_err("a cursor may not cross accounts");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(
            transport.calls("Email/query").len(),
            2,
            "the refused resume never reached the wire"
        );
    }

    /// The primary path is unchanged: no `In`, or an `In` naming a primary
    /// container, addresses the primary account with a bare mailbox id,
    /// bare result ids, and a bare cursor.
    #[tokio::test]
    async fn a_primary_container_search_stays_primary_and_bare() {
        let (primary, foreign, transport) = search_handles();

        let page = search_messages(
            primary,
            foreign,
            in_container(ContainerId("mbx-1".to_string())),
        )
        .await
        .expect("search succeeds");

        let queries = transport.calls("Email/query");
        assert_eq!(queries[0]["accountId"], serde_json::json!("primary"));
        assert_eq!(
            queries[0]["filter"]["inMailbox"],
            serde_json::json!("mbx-1")
        );
        assert_eq!(page.items, vec![ObjectId("M1".to_string())]);
        assert_eq!(
            page.next_cursor
                .map(|c| String::from_utf8(c).expect("utf8")),
            Some("2:1:q-1".to_string())
        );
    }

    /// A scripted `Email/query` server whose answers are given per call:
    /// `(ids, total, queryState)`. Deliberately independent of
    /// `SearchTransport`, whose single canned answer cannot express a
    /// short page, an absent `total`, or a moving result set.
    #[derive(Clone)]
    struct PagingTransport {
        pages: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<serde_json::Value>>>,
        positions: std::sync::Arc<std::sync::Mutex<Vec<i64>>>,
        /// When set, every response echoes this `position` instead of the
        /// one the request asked for - the "server ignores `position`" shape.
        forced_position: Option<i64>,
    }

    impl PagingTransport {
        fn new(pages: Vec<(Vec<&'static str>, Option<usize>, &'static str)>) -> Self {
            let pages = pages
                .into_iter()
                .map(|(ids, total, query_state)| {
                    serde_json::json!({
                        "ids": ids,
                        "total": total,
                        "queryState": query_state,
                    })
                })
                .collect();
            Self {
                pages: std::sync::Arc::new(std::sync::Mutex::new(pages)),
                positions: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                forced_position: None,
            }
        }

        fn echoing(mut self, position: i64) -> Self {
            self.forced_position = Some(position);
            self
        }

        fn positions(&self) -> Vec<i64> {
            self.positions.lock().expect("positions").clone()
        }
    }

    impl crate::core::transport::HttpTransport for PagingTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            let name = call[0].as_str().expect("method name").to_string();
            assert_eq!(name, "Email/query", "these tests only page the query");
            let call_id = call[2].as_str().expect("call id").to_string();
            let position = call[1]["position"].as_i64().unwrap_or(0);
            self.positions.lock().expect("positions").push(position);
            let page = self
                .pages
                .lock()
                .expect("pages")
                .pop_front()
                .expect("an unscripted page was requested");
            let mut arguments = serde_json::json!({
                "accountId": call[1]["accountId"].clone(),
                "queryState": page["queryState"].clone(),
                "canCalculateChanges": false,
                "position": self.forced_position.unwrap_or(position),
                "ids": page["ids"].clone(),
            });
            if !page["total"].is_null() {
                arguments["total"] = page["total"].clone();
            }
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": [[name, arguments, call_id]]
                })
                .to_string(),
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    type PagingHandles = (
        MailAccount<PagingTransport>,
        Arc<HashMap<String, MailAccount<PagingTransport>>>,
        PagingTransport,
    );

    fn paging_handles(
        pages: Vec<(Vec<&'static str>, Option<usize>, &'static str)>,
    ) -> PagingHandles {
        handles_for(PagingTransport::new(pages))
    }

    fn handles_for(transport: PagingTransport) -> PagingHandles {
        let client = crate::client::Client::with_transport(
            transport.clone(),
            search_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let primary = crate::account::Account::new(client, "primary");
        (primary, Arc::new(HashMap::new()), transport)
    }

    fn search_with_limit(limit: u32) -> SearchRequest {
        let mut request = SearchRequest::default();
        request.limit = Some(limit);
        request
    }

    /// The defect: `next_cursor` was derived from "the page came back full",
    /// so a page short of its `limit` ended the walk in Done-shaped silence -
    /// the consumer sees a complete search missing most of its hits. `total`
    /// is the authority on whether more remains.
    ///
    /// Note the fixture is a NON-CONFORMING server, deliberately. RFC 8620
    /// s5.5 does not permit an arbitrary short non-final page (`ids` runs to
    /// the end of the results or to the effective limit), which is what a
    /// wrong justification in three comments and the reference used to
    /// claim. The rule survives the correction: fullness is still not a
    /// completion test, because it would rest on a `limit` echo a server may
    /// omit, and `total` costs nothing to prefer.
    #[tokio::test]
    async fn a_short_non_final_page_still_yields_a_next_cursor() {
        let (primary, foreign, _transport) =
            paging_handles(vec![(vec!["M1", "M2"], Some(9), "q-1")]);

        let page = search_messages(primary, foreign, search_with_limit(5))
            .await
            .expect("search succeeds");

        assert_eq!(page.items.len(), 2, "the server served a short page");
        assert_eq!(page.estimated_total, Some(9));
        assert_eq!(
            page.next_cursor
                .map(|c| String::from_utf8(c).expect("utf8")),
            Some("2:2:q-1".to_string()),
            "seven hits remain, so the walk must continue"
        );
    }

    /// The mirror case: a page that came back FULL but exhausted `total`
    /// is final, and must not hand back a cursor that would page past the
    /// end of the result set.
    #[tokio::test]
    async fn a_full_final_page_ends_the_walk() {
        let (primary, foreign, _transport) =
            paging_handles(vec![(vec!["M1", "M2"], Some(2), "q-1")]);

        let page = search_messages(primary, foreign, search_with_limit(2))
            .await
            .expect("search succeeds");

        assert!(page.next_cursor.is_none(), "position + served == total");
    }

    /// With no `total` the walk continues until an EMPTY page. Page fullness
    /// used to be the fallback here and it truncated: the FIRST short page
    /// ended the search in Done-shaped silence - the same defect
    /// `a_short_non_final_page_still_yields_a_next_cursor` pins for the
    /// total-bearing case, surviving on the other branch. The honest argument
    /// for continue-until-empty is that an absent `total` is not evidence of
    /// completion while a conforming empty page is conclusive; it is NOT
    /// "the RFC permits an arbitrary short non-final page", which is false.
    ///
    /// The rule transfers from the anchored PIM walks to this positional
    /// cursor because the successor position is the server's own echo plus
    /// what it served, and `validate_search_page` refuses an echo that
    /// disagrees with the request, so it advances by at least one per
    /// nonempty page.
    ///
    /// Bite check: with the fullness fallback restored, the second assertion
    /// fails - page two is short, so the walk would stop with M4 unread.
    #[tokio::test]
    async fn a_server_that_omits_total_pages_until_an_empty_page() {
        let (primary, foreign, transport) = paging_handles(vec![
            (vec!["M1", "M2"], None, "q-1"),
            (vec!["M3"], None, "q-1"),
            (vec!["M4"], None, "q-1"),
            (vec![], None, "q-1"),
        ]);

        let mut seen: Vec<String> = Vec::new();
        let mut cursor = None;
        for _ in 0..4 {
            let mut request = search_with_limit(2);
            request.page_cursor = cursor;
            let page = search_messages(primary.clone(), Arc::clone(&foreign), request)
                .await
                .expect("search succeeds");
            seen.extend(page.items.iter().map(|id| id.0.clone()));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(seen, vec!["M1", "M2", "M3", "M4"]);
        assert!(cursor.is_none(), "the empty page is what ends it");
        assert_eq!(transport.positions(), vec![0, 2, 3, 4]);
    }

    /// The defect: page 2 was taken from whatever order the server had at
    /// the time, with no check that it was the order page 1's position
    /// indexed. One delivered message reorders `receivedAt desc` and the
    /// window slides, silently duplicating one hit and dropping another.
    /// A moved `queryState` must be refused, as a concurrency conflict the
    /// caller resolves by searching again - never as terminal.
    #[tokio::test]
    async fn a_moved_query_state_refuses_the_next_page() {
        let (primary, foreign, _transport) = paging_handles(vec![
            (vec!["M1", "M2"], Some(9), "q-1"),
            (vec!["M2", "M3"], Some(10), "q-2"),
        ]);

        let page = search_messages(primary.clone(), Arc::clone(&foreign), search_with_limit(2))
            .await
            .expect("search succeeds");
        let mut resume = search_with_limit(2);
        resume.page_cursor = Some(page.next_cursor.expect("more remains"));

        let error = search_messages(primary, foreign, resume)
            .await
            .expect_err("a reordered result set must not be paged");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
        assert!(
            !error.recovery().is_terminal(),
            "ordinary mail delivery must not permanently kill search"
        );
    }

    /// An unmoved `queryState` is the normal case and must page through
    /// without complaint, at the position the previous page ended on.
    #[tokio::test]
    async fn an_unmoved_query_state_pages_on() {
        let (primary, foreign, transport) = paging_handles(vec![
            (vec!["M1", "M2"], Some(4), "q-1"),
            (vec!["M3", "M4"], Some(4), "q-1"),
        ]);

        let page = search_messages(primary.clone(), Arc::clone(&foreign), search_with_limit(2))
            .await
            .expect("search succeeds");
        let mut resume = search_with_limit(2);
        resume.page_cursor = Some(page.next_cursor.expect("more remains"));
        let second = search_messages(primary, foreign, resume)
            .await
            .expect("resume succeeds");

        assert_eq!(transport.positions(), vec![0, 2]);
        assert_eq!(
            second.items,
            vec![ObjectId("M3".to_string()), ObjectId("M4".to_string())]
        );
        assert!(second.next_cursor.is_none(), "total is exhausted");
    }

    /// BITES, at the envelope level. All four refusals in
    /// `validate_search_page`, each stated as the smallest input that
    /// separates it from the old behaviour.
    ///
    /// Bite check per row, against the code this replaced (`served_from`
    /// trusted verbatim, `next_position >= total` the only test):
    ///
    /// - negative echo: `-4 + 2 = -2`, which compares below every total, so
    ///   the walk minted `2:-2:q-1` and paged on from a nonsense base.
    /// - `2 + 2 > 3`: `4 >= 3` ended the walk and reported it complete.
    /// - echo 0 for a page requested at 2: accepted silently, and since the
    ///   successor is `0 + 2 = 2` the walk re-requests position 2 forever.
    ///
    /// The advance rule is a real backstop rather than a restatement: on the
    /// "echo that ignores the request" row it catches the same envelope on
    /// its own (`0 + 2 <= 2`), so removing EITHER rule leaves that row
    /// refused. The echo rule is the strictly stronger of the two - the
    /// "moved the window" row advances to 7 and only the echo rule sees it -
    /// and it runs first, which is why the advance rule has no row of its own
    /// that reaches it through `search_email_ids`. It is exercised here at
    /// the function, and this doc says that rather than implying a coverage
    /// it does not have.
    #[test]
    fn a_search_page_envelope_is_validated_before_it_can_end_the_walk() {
        let op = AccountOperation::Search;
        for (case, requested, served_from, served, total) in [
            ("a negative position echo", 0, -4, 2, Some(50_u64)),
            ("a negative position echo, no total", 0, -4, 2, None),
            ("served past the reported total", 0, 2, 2, Some(3)),
            ("served past a zero total", 0, 0, 1, Some(0)),
            ("an echo that ignores the request", 2, 0, 2, None),
            ("an echo that moved the window", 2, 5, 2, Some(50)),
        ] {
            let error =
                validate_search_page(requested, served_from, served, total, op).expect_err(case);
            assert!(
                matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Protocol(
                        bifrost_types::ProtocolErrorKind::ContractViolation
                    )
                ),
                "{case} must be a ProviderContractViolation, got {:?}",
                error.kind()
            );
        }

        // An EMPTY page is exempt from all three content rules: it carries no
        // first id for `position` to index and it terminates the walk anyway.
        // Refusing it here would turn every ordinary end-of-list into an
        // error.
        assert_eq!(
            validate_search_page(4, 4, 0, None, op).expect("an empty page is a legitimate ending"),
            4
        );
        assert_eq!(
            validate_search_page(4, 99, 0, Some(4), op).expect("so is an empty page past the end"),
            99
        );

        // The controls: conforming envelopes still produce a successor.
        assert_eq!(
            validate_search_page(0, 0, 2, Some(9), op).expect("valid first page"),
            2
        );
        assert_eq!(
            validate_search_page(2, 2, 2, Some(4), op).expect("valid continuation"),
            4
        );
        assert_eq!(
            validate_search_page(2, 2, 1, None, op).expect("valid, total-less continuation"),
            3
        );
    }

    /// BITES end to end, and this is the residual `reference/jmap.md` used to
    /// record as undetectable: a server that omits `total` and IGNORES
    /// `position` re-serves its first page forever, and the walk never
    /// terminates.
    ///
    /// It is detectable because the `queryState` is pinned. This server
    /// answers `q-1` on both pages, so by its own account the result list is
    /// one stable finite list - and a stable finite list cannot answer a
    /// request for position 2 with the rows that live at position 0.
    ///
    /// Bite check: with the echo rule removed the advance rule still refuses
    /// this page (`0 + 2` does not pass 2); with BOTH removed, page two
    /// mints `2:2:q-1` again and the caller loops on the same two ids
    /// forever, which is precisely the behaviour the reference used to
    /// document as unavoidable.
    #[tokio::test]
    async fn a_server_that_ignores_position_cannot_page_forever() {
        let (primary, foreign, transport) = handles_for(
            PagingTransport::new(vec![
                (vec!["M1", "M2"], None, "q-1"),
                (vec!["M1", "M2"], None, "q-1"),
            ])
            .echoing(0),
        );

        let page = search_messages(primary.clone(), Arc::clone(&foreign), search_with_limit(2))
            .await
            .expect("first page succeeds");
        let mut resume = search_with_limit(2);
        resume.page_cursor = Some(page.next_cursor.expect("more may remain"));

        let error = search_messages(primary, foreign, resume)
            .await
            .expect_err("a re-served page must not extend the walk");
        assert!(
            matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                )
            ),
            "got {:?}",
            error.kind()
        );
        assert_eq!(
            transport.positions(),
            vec![0, 2],
            "the walk did ask for the second page"
        );
    }

    /// The cursor codec: version, position and an opaque `queryState`
    /// round-trip, on the primary account and on a share alike. The state
    /// is the payload TAIL, so a state containing the field separator
    /// survives verbatim.
    #[test]
    fn the_search_cursor_round_trips_position_state_and_owner() {
        for owner in [None, Some("shared")] {
            for state in ["q-1", "a:b:c", ""] {
                let encoded = encode_position(42, state, owner);
                let decoded = decode_position(Some(&encoded), owner, AccountOperation::Search)
                    .expect("round-trips");
                assert_eq!(decoded.position, 42);
                assert_eq!(decoded.query_state.as_deref(), Some(state));
            }
        }
    }

    /// A v1 cursor (a bare integer position, no `queryState`) is REFUSED,
    /// not silently upgraded: continuing it is exactly the unpinned paging
    /// the version bump exists to stop.
    #[test]
    fn a_v1_search_cursor_is_refused_as_schema_incompatible() {
        let error = decode_position(Some(b"7"), None, AccountOperation::Search)
            .expect_err("a v1 cursor must not resume");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::SchemaIncompatible
            )
        );

        let foreign = super::super::foreign::encode_object("shared", "7").into_bytes();
        let error = decode_position(Some(&foreign), Some("shared"), AccountOperation::Search)
            .expect_err("a foreign v1 cursor must not resume either");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::SchemaIncompatible
            )
        );
    }

    /// The owner qualification that landed with the account-mismatch fix
    /// still binds: a v2 cursor minted on a share is refused against the
    /// primary account, before the wire.
    #[test]
    fn a_v2_cursor_still_refuses_to_cross_accounts() {
        let cursor = encode_position(3, "q-1", Some("shared"));
        let error = decode_position(Some(&cursor), None, AccountOperation::Search)
            .expect_err("a cursor may not cross accounts");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
    }

    // A foreign-account hydration must come back in the SAME id namespace
    // the foreign inventory minted, or the consumer's follow-up blob read
    // and container join both address the primary account.
    #[test]
    fn rebase_foreign_requalifies_message_container_and_blob_ids() {
        let mut id = ObjectId("M1".to_string());
        let mut thread_id = ThreadId("T1".to_string());
        let mut containers = vec![ContainerId("inbox".to_string())];
        let mut attachments = vec![
            MessageAttachment {
                filename: None,
                content_type: None,
                content_id: None,
                inline: false,
                size: None,
                source: AttachmentSource::Blob(BlobHandle {
                    id: BlobId("B1".to_string()),
                    size: None,
                    content_type: None,
                    digest: None,
                    capabilities: BlobCapabilities {
                        supports_range: false,
                        supports_parallel: false,
                        digest_available_pre_download: false,
                        encoding: BlobEncoding::Raw8Bit,
                    },
                }),
                truncated: false,
            },
            MessageAttachment {
                filename: None,
                content_type: None,
                content_id: None,
                inline: false,
                size: Some(6),
                source: AttachmentSource::Inline(Bytes::from_static(b"inline")),
                truncated: false,
            },
        ];

        qualify_foreign_message_ids(
            &mut id,
            Some(&mut thread_id),
            &mut containers,
            &mut attachments,
            "acct-9",
        );

        // Object ids (message, blob) ride the OBJECT namespace, which
        // `open_blob` / `get_stream` decode.
        assert_eq!(id.0, super::super::foreign::encode_object("acct-9", "M1"));
        // The thread id rides the same namespace: it is what the consumer
        // hands to `thread_hydrate` and `MutationTarget::Thread`, and both
        // expand it through an accountId-scoped `Thread/get`.
        assert_eq!(
            thread_id.0,
            super::super::foreign::encode_object("acct-9", "T1")
        );
        assert_eq!(
            match &attachments[0].source {
                AttachmentSource::Blob(blob) => blob.id.0.as_str(),
                _ => panic!("blob attachment must stay blob-backed"),
            },
            super::super::foreign::encode_object("acct-9", "B1")
        );
        assert!(matches!(
            &attachments[1].source,
            AttachmentSource::Inline(bytes) if bytes == &Bytes::from_static(b"inline")
        ));
        // A container id is a mailbox id, so it rides the FOLDER namespace
        // `containers_list` and the qualified memberships key on -
        // byte-identical, or the join fails.
        assert_eq!(
            containers[0].0,
            super::super::foreign::encode_foreign("acct-9", "inbox").0
        );
    }

    #[test]
    fn attachment_from_part_fills_cid_and_disposition() {
        let part: EmailBodyPart = serde_json::from_value(serde_json::json!({
            "blobId": "blob-1",
            "size": 42,
            "name": "logo.png",
            "type": "image/png",
            "cid": "logo@example.test",
            "disposition": "inline"
        }))
        .expect("attachment body part deserializes");

        let attachment = attachment_from_part(&part).expect("blob-backed attachment");
        assert_eq!(attachment.filename.as_deref(), Some("logo.png"));
        assert_eq!(attachment.content_type.as_deref(), Some("image/png"));
        assert_eq!(attachment.content_id.as_deref(), Some("logo@example.test"));
        assert!(attachment.inline);
        assert_eq!(attachment.size, Some(42));
        assert!(!attachment.truncated);
        assert!(matches!(
            attachment.source,
            AttachmentSource::Blob(BlobHandle { id, size: Some(42), content_type: Some(content_type), .. })
                if id.0 == "blob-1" && content_type == "image/png"
        ));

        let requested = body_properties();
        assert!(requested.contains(&BodyProperty::Cid));
        assert!(requested.contains(&BodyProperty::Disposition));
    }

    /// The routing decision behind every thread-keyed door.
    ///
    /// A bare id is PRIMARY by construction (only foreign ids are ever
    /// encoded), a qualified id names its share, and a qualified id for a
    /// share that has gone away deliberately does NOT resolve to a
    /// registered account - it rides the primary route with its literal
    /// form so the server reports the miss, rather than being stripped
    /// into a bare native id that could collide with a real primary
    /// thread.
    #[test]
    fn a_thread_id_declares_the_account_that_must_expand_it() {
        let registered = |account: &str| account == "shared";

        let foreign = ThreadId(super::super::foreign::encode_object("shared", "T9"));
        assert_eq!(
            thread_owner(&foreign, registered),
            Some("shared".to_string())
        );

        assert_eq!(thread_owner(&ThreadId("T9".to_string()), registered), None);

        let departed = ThreadId(super::super::foreign::encode_object("revoked", "T9"));
        assert_eq!(thread_owner(&departed, registered), None);
        // ...and the primary route keeps it literal, so the wire id can
        // name no real primary thread.
        assert_eq!(wire_id_for_mail(&departed.0, "primary"), departed.0);
    }

    /// The qualification round-trip a thread-keyed door performs: qualify
    /// at the projection site, decode at the door, and the wire id is the
    /// native id the owning account issued.
    #[test]
    fn a_qualified_thread_id_round_trips_to_its_native_wire_form() {
        let qualified = super::super::foreign::encode_object("shared", "T9");
        assert_eq!(wire_id_for_mail(&qualified, "shared"), "T9");
        // Byte-stable: re-qualifying the decoded native id reproduces it.
        assert_eq!(
            super::super::foreign::encode_object("shared", &wire_id_for_mail(&qualified, "shared")),
            qualified
        );
        // A bare (primary) thread id is untouched on the primary route.
        assert_eq!(wire_id_for_mail("T9", "primary"), "T9");
    }

    /// A thread target answers the cross-account question the same way a
    /// message target does. Before this, threads asserted primary
    /// ownership unconditionally, so a foreign thread paired with a bare
    /// primary container passed the guard.
    #[test]
    fn a_thread_target_and_its_container_must_name_the_same_account() {
        let foreign_thread = MutationTarget::Thread(ThreadId(
            super::super::foreign::encode_object("shared", "T9"),
        ));
        let foreign_container =
            ContainerId(super::super::foreign::encode_foreign("shared", "inbox").0);
        let primary_container = ContainerId("inbox".to_string());

        assert!(
            cross_account_container(
                &foreign_thread,
                &foreign_container,
                AccountOperation::BulkMove
            )
            .is_ok()
        );
        let err = cross_account_container(
            &foreign_thread,
            &primary_container,
            AccountOperation::BulkMove,
        )
        .expect_err("a foreign thread cannot be filed into a primary mailbox id");
        assert_eq!(
            err.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );

        // The mirror case: a primary thread and a foreign container.
        let primary_thread = MutationTarget::Thread(ThreadId("T9".to_string()));
        assert!(
            cross_account_container(
                &primary_thread,
                &primary_container,
                AccountOperation::BulkMove
            )
            .is_ok()
        );
        assert!(
            cross_account_container(
                &primary_thread,
                &foreign_container,
                AccountOperation::BulkMove
            )
            .is_err()
        );
    }

    // A primary (bare) id is left alone: one logical object, one wire form.
    #[test]
    fn primary_hydration_ids_are_not_qualified() {
        let route = super::super::hydrate::route_for_id(&ObjectId("M1".to_string()), |_| true);
        assert_eq!(route, super::super::hydrate::HydrationRoute::Primary);
        assert_eq!(super::super::foreign::native_object("M1"), "M1");
    }

    fn full_rights() -> serde_json::Value {
        serde_json::json!({
            "mayReadItems": true,
            "mayAddItems": true,
            "mayRemoveItems": true,
            "maySetSeen": true,
            "maySetKeywords": true,
            "mayCreateChild": true,
            "mayRename": true,
            "mayDelete": true,
            "maySubmit": true,
        })
    }

    #[test]
    fn foreign_mailbox_projects_as_shared_namespaced_container() {
        let container = container_from_mailbox(
            mailbox_json("mbx-12", Some("mbx-1"), full_rights()),
            Some("acct-9"),
            Some("owner@example.test"),
        )
        .expect("container");

        assert_eq!(container.namespace, ContainerNamespace::Shared);
        assert_eq!(container.owner_email.as_deref(), Some("owner@example.test"));
        assert_eq!(
            container.owner,
            Some(bifrost_types::MailboxId("acct-9".to_string()))
        );
        // `native_id` is the per-account namespaced form (what the cursor
        // scope keys on); `owner_local_id` is the bare mailbox id.
        assert_eq!(
            container.native_id,
            super::super::foreign::encode_foreign("acct-9", "mbx-12").0
        );
        assert_eq!(container.owner_local_id.as_deref(), Some("mbx-12"));
        // The parent is re-encoded in the same namespace, so a foreign child
        // never points at a same-id primary mailbox.
        assert_eq!(
            container.parent,
            Some(ContainerId(
                super::super::foreign::encode_foreign("acct-9", "mbx-1").0
            ))
        );
        // JMAP rights still project (they already did) and JMAP folders
        // carry no content class.
        assert_eq!(
            container.rights.as_ref().and_then(|r| r.may_submit),
            Some(true)
        );
        assert!(container.content_class.is_none());
    }

    /// The container's `native_id` must be byte-identical to the
    /// `MembershipScope::Folder` qualification the foreign inventory and
    /// hydration stamp on the same mailbox - that identity is the join
    /// key between a message's membership and its container. (The
    /// share's SYNC scope is coarser: one account-level `Folder` scope
    /// per account, pinned in `factory.rs`.)
    #[test]
    fn foreign_container_native_id_matches_qualified_membership() {
        let container = container_from_mailbox(
            mailbox_json("mbx-12", None, full_rights()),
            Some("acct-9"),
            None,
        )
        .expect("container");
        let mut memberships = vec![bifrost_types::MembershipScope::Mailbox(
            bifrost_types::MailboxId("mbx-12".to_string()),
        )];
        super::super::inventory::qualify_foreign_memberships(
            &mut memberships,
            &bifrost_types::MailboxId("acct-9".to_string()),
        );
        assert!(
            memberships.contains(&bifrost_types::MembershipScope::Folder(
                bifrost_types::FolderId(container.native_id.clone())
            )),
            "container {:?} does not join the qualified memberships {memberships:?}",
            container.native_id
        );
    }

    #[test]
    fn primary_mailbox_stays_personal_and_unqualified() {
        let container = container_from_mailbox(
            mailbox_json("mbx-12", Some("mbx-1"), full_rights()),
            None,
            None,
        )
        .expect("container");
        assert_eq!(container.namespace, ContainerNamespace::Personal);
        assert!(container.owner.is_none());
        assert!(container.owner_email.is_none());
        assert!(container.owner_local_id.is_none());
        assert_eq!(container.native_id, "mbx-12");
        assert_eq!(container.parent, Some(ContainerId("mbx-1".to_string())));
    }

    // -- owner-email resolution (the two-level RFC 9670 gate) ------------

    #[test]
    fn owner_email_plan_prefers_principal_over_name() {
        // A present principal id always routes through `Principal/get`
        // first, even when the account name looks like an address.
        assert_eq!(
            owner_email_plan(Some("p-1"), "shared@example.test"),
            OwnerEmailPlan::FromPrincipal {
                principal_id: "p-1".to_string(),
                name_fallback: Some("shared@example.test".to_string()),
            }
        );
        assert_eq!(
            owner_email_plan(None, "shared@example.test"),
            OwnerEmailPlan::FromName("shared@example.test".to_string())
        );
        assert_eq!(
            owner_email_plan(None, "Shared Mailbox"),
            OwnerEmailPlan::Skip
        );
    }

    #[test]
    fn unresolvable_principal_degrades_to_the_name_instead_of_blanking() {
        // A principal the server answers for and does not know leaves
        // nothing authoritative, so the plan carries the parsed session
        // name to degrade to. (Only `PrincipalEmail::Absent` actually
        // consumes it; a failed lookup does not.)
        let plan = owner_email_plan(Some("p-unreadable"), "shared@example.test");
        let OwnerEmailPlan::FromPrincipal { name_fallback, .. } = plan else {
            panic!("a present principal id must plan a principal lookup");
        };
        assert_eq!(name_fallback.as_deref(), Some("shared@example.test"));
    }

    #[test]
    fn account_name_must_parse_as_an_address_not_merely_contain_an_at() {
        // RFC 8620 Account.name is a user-facing label. Sniffing for
        // '@' stored display-name forms wholesale as owner addresses.
        assert_eq!(
            account_name_as_address("shared@example.test").as_deref(),
            Some("shared@example.test")
        );
        assert_eq!(
            account_name_as_address("  shared@example.test  ").as_deref(),
            Some("shared@example.test"),
            "surrounding whitespace is trimmed, not a rejection"
        );

        for rejected in [
            "Support <support@example.com>",
            "Shared Mailbox",
            "Ada Lovelace ada@example.test",
            "@example.test",
            "ada@",
            "ada@@example.test",
            "ada@localhost",
            "ada@.example.test",
            "ada@example.",
            "\"quoted\"@example.test",
            "a@b.test, c@d.test",
            "",
            "   ",
        ] {
            assert_eq!(
                account_name_as_address(rejected),
                None,
                "{rejected:?} must not be treated as an owner address"
            );
        }
    }

    #[test]
    fn a_display_name_account_never_becomes_a_fallback() {
        // The plan for an account whose name is a display string must
        // carry no fallback at all, so a completed-but-empty lookup
        // resolves to nothing rather than to a label.
        assert_eq!(
            owner_email_plan(Some("p-1"), "Support <support@example.com>"),
            OwnerEmailPlan::FromPrincipal {
                principal_id: "p-1".to_string(),
                name_fallback: None,
            }
        );
        assert_eq!(
            owner_email_plan(None, "Support <support@example.com>"),
            OwnerEmailPlan::Skip
        );
    }

    #[test]
    fn only_a_completed_lookup_permits_the_name_fallback() {
        // The rule the resolver applies, stated directly: `Absent`
        // (the server answered, no address) may degrade to the parsed
        // name; `Unavailable` (no answer) must not, or a transient
        // failure overwrites real ownership with a guess.
        let fallback = || Some("shared@example.test".to_string());
        assert_eq!(
            owner_email_from_lookup(
                PrincipalEmail::Resolved("owner@example.test".into()),
                fallback()
            )
            .as_deref(),
            Some("owner@example.test"),
            "an authoritative answer always wins over the name"
        );
        assert_eq!(
            owner_email_from_lookup(PrincipalEmail::Absent, fallback()).as_deref(),
            Some("shared@example.test"),
            "a completed lookup with no address may degrade to the name"
        );
        assert_eq!(
            owner_email_from_lookup(PrincipalEmail::Unavailable, fallback()),
            None,
            "a failed lookup must not be papered over with the account name"
        );
        assert_eq!(
            owner_email_from_lookup(PrincipalEmail::Absent, None),
            None,
            "and with no parseable name there is nothing to degrade to"
        );
    }

    #[test]
    fn unresolvable_principal_without_an_address_name_has_nothing_to_fall_back_to() {
        // A non-address name is not an email and must never be guessed
        // into one; the fallback stays empty and the account resolves to
        // no owner email at all.
        assert_eq!(
            owner_email_plan(Some("p-unreadable"), "Shared Mailbox"),
            OwnerEmailPlan::FromPrincipal {
                principal_id: "p-unreadable".to_string(),
                name_fallback: None,
            }
        );
    }

    fn gate_session(with_principals_capability: bool) -> crate::core::session::Session {
        let mut capabilities = serde_json::json!({
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 50000000,
                "maxConcurrentUpload": 4,
                "maxSizeRequest": 10000000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 16,
                "maxObjectsInGet": 500,
                "maxObjectsInSet": 500,
                "collationAlgorithms": []
            },
        });
        if with_principals_capability {
            capabilities["urn:ietf:params:jmap:principals"] = serde_json::json!({});
        }
        serde_json::from_value(serde_json::json!({
            "capabilities": capabilities,
            "accounts": {
                "acct-owner": {
                    "name": "Shared Mailbox",
                    "isPersonal": false,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:principals:owner": {
                            "accountIdForPrincipal": "acct-principals",
                            "principalId": "p-owner"
                        }
                    }
                },
                "acct-name": {
                    "name": "shared@example.test",
                    "isPersonal": false,
                    "isReadOnly": false,
                    "accountCapabilities": {}
                },
                "acct-plain": {
                    "name": "Shared Mailbox",
                    "isPersonal": false,
                    "isReadOnly": false,
                    "accountCapabilities": {}
                }
            },
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/",
            "downloadUrl": "https://example.test/jmap/download/{accountId}/{blobId}/{name}",
            "uploadUrl": "https://example.test/jmap/upload/{accountId}/",
            "eventSourceUrl": "https://example.test/jmap/es/",
            "state": "s1"
        }))
        .expect("gate session deserializes")
    }

    #[test]
    fn session_without_principals_capability_plans_nothing() {
        // Level ONE of the gate: no session principals capability means no
        // resolution at all - INCLUDING the name fallback. Resolving from
        // the account name here would populate emails where the legacy
        // behavior left them NULL.
        let session = gate_session(false);
        let acct_owner = "acct-owner".to_string();
        let acct_name = "acct-name".to_string();
        let ids = vec![&acct_owner, &acct_name];
        assert!(owner_email_plans(&session, &ids).is_empty());
    }

    #[test]
    fn session_with_principals_capability_plans_per_account() {
        let session = gate_session(true);
        let acct_owner = "acct-owner".to_string();
        let acct_name = "acct-name".to_string();
        let acct_plain = "acct-plain".to_string();
        let acct_unknown = "acct-unknown".to_string();
        let ids = vec![&acct_owner, &acct_name, &acct_plain, &acct_unknown];
        let plans = owner_email_plans(&session, &ids);
        assert_eq!(
            plans,
            vec![
                (
                    "acct-owner".to_string(),
                    // "Shared Mailbox" is not address-shaped, so this
                    // account has no name to degrade to.
                    OwnerEmailPlan::FromPrincipal {
                        principal_id: "p-owner".to_string(),
                        name_fallback: None,
                    }
                ),
                (
                    "acct-name".to_string(),
                    OwnerEmailPlan::FromName("shared@example.test".to_string())
                ),
                ("acct-plain".to_string(), OwnerEmailPlan::Skip),
                // `acct-unknown` is absent from the session and yields no
                // plan at all (fail-soft: absent, never an error).
            ]
        );
    }

    #[test]
    fn importance_high_sets_keyword_others_clear() {
        // High -> set `$important`; Normal/Low -> clear it. One keyword op,
        // never an expand-into-two.
        assert!(importance_sets_important_keyword(Importance::High));
        assert!(!importance_sets_important_keyword(Importance::Normal));
        assert!(!importance_sets_important_keyword(Importance::Low));
    }

    #[test]
    fn resolve_foreign_headers_as_overrides_consumer_from() {
        let identity = bifrost_types::Address::bare("shared@example.test");
        let (from, sender) = resolve_foreign_headers(
            &bifrost_types::SendAs::As(bifrost_types::MailboxId("foreign".to_string())),
            &identity,
            Some(bifrost_types::Address::bare("consumer@example.test")),
            Some(bifrost_types::Address::bare("user@example.test")),
        );
        assert_eq!(from.address, "shared@example.test");
        assert!(sender.is_none());
    }

    #[test]
    fn resolve_foreign_headers_on_behalf_of_honors_consumer_from() {
        let identity = bifrost_types::Address::bare("shared@example.test");
        let (from, sender) = resolve_foreign_headers(
            &bifrost_types::SendAs::OnBehalfOf(bifrost_types::MailboxId("foreign".to_string())),
            &identity,
            Some(bifrost_types::Address::bare("author@example.test")),
            Some(bifrost_types::Address::bare("user@example.test")),
        );
        assert_eq!(from.address, "author@example.test");
        assert_eq!(
            sender.expect("known self address").address,
            "user@example.test"
        );
    }

    #[test]
    fn resolve_foreign_headers_on_behalf_of_omits_unknown_sender() {
        let identity = bifrost_types::Address::bare("shared@example.test");
        let (from, sender) = resolve_foreign_headers(
            &bifrost_types::SendAs::OnBehalfOf(bifrost_types::MailboxId("foreign".to_string())),
            &identity,
            None,
            None,
        );
        assert_eq!(from.address, "shared@example.test");
        assert!(sender.is_none());
    }

    fn identity_rows(json: serde_json::Value) -> Vec<crate::identity::Identity> {
        serde_json::from_value(json).expect("identity rows deserialize")
    }

    #[test]
    fn select_concrete_identity_takes_first_concrete_email() {
        let rows = identity_rows(serde_json::json!([
            {"id": "i0", "name": "Shared", "email": "shared@example.test"},
            {"id": "i1", "email": "other@example.test"},
        ]));
        let picked = select_concrete_identity(rows, "no foreign sending identity")
            .expect("first concrete identity");
        assert_eq!(picked.id, "i0");
        assert_eq!(picked.email, "shared@example.test");
        assert_eq!(picked.name.as_deref(), Some("Shared"));
    }

    #[test]
    fn select_concrete_identity_skips_wildcard_email() {
        let rows = identity_rows(serde_json::json!([
            {"id": "i0", "email": "*"},
            {"id": "i1", "email": "*@example.test"},
            {"id": "i2", "email": "concrete@example.test"},
        ]));
        let picked = select_concrete_identity(rows, "no foreign sending identity")
            .expect("concrete over wildcard");
        assert_eq!(picked.id, "i2");
    }

    #[test]
    fn select_concrete_identity_empty_list_rejects_unsupported() {
        let err = select_concrete_identity(Vec::new(), "no foreign sending identity")
            .expect_err("no identity to send as");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    #[test]
    fn select_concrete_identity_requested_absent_rejects() {
        // A by-id get that returns nothing usable is a distinct rejection
        // detail from the empty-account case, but still `Unsupported(Send)`.
        let rows = identity_rows(serde_json::json!([{"id": "i0", "email": "*"}]));
        let err = select_concrete_identity(rows, "requested identity has no concrete email")
            .expect_err("requested identity has no concrete email");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    /// In-process JMAP server for the send commit paths. It records each
    /// method call and can refuse a submission, refuse an unsend, omit the
    /// linked Email, or fail the post-send state probe.
    #[derive(Clone)]
    struct SendTransport {
        requests: Arc<std::sync::Mutex<Vec<Vec<serde_json::Value>>>>,
        submission_seen: Arc<std::sync::atomic::AtomicBool>,
        submission_reply: Option<&'static str>,
        fail_submission: bool,
        fail_cancel: bool,
        fail_probe_after_submission: bool,
        missing_email: bool,
    }

    impl SendTransport {
        fn new(
            fail_submission: bool,
            fail_cancel: bool,
            fail_probe_after_submission: bool,
            missing_email: bool,
        ) -> Self {
            Self {
                requests: Arc::new(std::sync::Mutex::new(Vec::new())),
                submission_seen: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                submission_reply: None,
                fail_submission,
                fail_cancel,
                fail_probe_after_submission,
                missing_email,
            }
        }

        fn calls(&self, method: &str) -> Vec<serde_json::Value> {
            self.requests
                .lock()
                .expect("recorded requests")
                .iter()
                .flat_map(|request| request.iter())
                .filter(|call| call[0] == serde_json::json!(method))
                .map(|call| call[1].clone())
                .collect()
        }

        fn method_sequence(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("recorded requests")
                .iter()
                .flat_map(|request| request.iter())
                .map(|call| call[0].as_str().expect("method name").to_string())
                .collect()
        }

        fn keyed(key: &str, value: serde_json::Value) -> serde_json::Value {
            let mut object = serde_json::Map::new();
            object.insert(key.to_string(), value);
            serde_json::Value::Object(object)
        }
    }

    impl HttpTransport for SendTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value =
                serde_json::from_slice(&body).expect("the client emits JSON");
            let calls = request["methodCalls"]
                .as_array()
                .expect("method calls array")
                .clone();
            self.requests
                .lock()
                .expect("recorded requests")
                .push(calls.clone());

            let mut responses = Vec::with_capacity(calls.len());
            for call in calls {
                let method = call[0].as_str().expect("method name");
                let args = &call[1];
                let call_id = call[2].as_str().expect("call id").to_string();
                if method == "EmailSubmission/set" {
                    self.submission_seen
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                if method == "Email/get"
                    && args["ids"].as_array().is_some_and(Vec::is_empty)
                    && self.fail_probe_after_submission
                    && self
                        .submission_seen
                        .load(std::sync::atomic::Ordering::Relaxed)
                {
                    return Err(crate::core::transport::TransportError::new(
                        "scripted post-submission probe failure",
                    ));
                }

                let account_id = args["accountId"].clone();
                let result = match method {
                    "Mailbox/get" => serde_json::json!({
                        "accountId": account_id,
                        "state": "mailbox-state",
                        "list": [
                            {"id": "drafts", "name": "Drafts", "role": "drafts"},
                            {"id": "sent", "name": "Sent", "role": "sent"}
                        ],
                        "notFound": []
                    }),
                    "EmailSubmission/get" => serde_json::json!({
                        "accountId": account_id,
                        "state": "submission-state",
                        "list": [{
                            "id": "old-submission",
                            "emailId": "email-old",
                            "envelope": {
                                "mailFrom": {"email": "from@example.test", "parameters": null},
                                "rcptTo": [{"email": "to@example.test", "parameters": null}]
                            }
                        }],
                        "notFound": []
                    }),
                    "Email/get" => {
                        let ids = args["ids"].as_array().expect("get ids");
                        if ids.is_empty() {
                            serde_json::json!({
                                "accountId": account_id,
                                "state": "email-probe-state",
                                "list": [],
                                "notFound": []
                            })
                        } else if self.missing_email {
                            serde_json::json!({
                                "accountId": account_id,
                                "state": "email-state",
                                "list": [],
                                "notFound": ids
                            })
                        } else {
                            serde_json::json!({
                                "accountId": account_id,
                                "state": "email-state",
                                "list": ids.iter().map(|id| serde_json::json!({"id": id})).collect::<Vec<_>>(),
                                "notFound": []
                            })
                        }
                    }
                    "Email/set" => {
                        if let Some(destroyed) = args["destroy"].as_array() {
                            serde_json::json!({
                                "accountId": account_id,
                                "oldState": args["ifInState"],
                                "newState": "email-deleted-state",
                                "destroyed": destroyed
                            })
                        } else if let Some(updated) = args["update"].as_object() {
                            let updated = updated
                                .keys()
                                .map(|id| (id.clone(), serde_json::Value::Null))
                                .collect::<serde_json::Map<_, _>>();
                            serde_json::json!({
                                "accountId": account_id,
                                "oldState": args["ifInState"],
                                "newState": "email-updated-state",
                                "updated": updated
                            })
                        } else {
                            let create_id = args["create"]
                                .as_object()
                                .expect("email create map")
                                .keys()
                                .next()
                                .expect("email create id");
                            serde_json::json!({
                                "accountId": account_id,
                                "oldState": "email-state-before-create",
                                "newState": "email-created-state",
                                "created": Self::keyed(create_id, serde_json::json!({"id": "email-created"}))
                            })
                        }
                    }
                    "Identity/get" => {
                        let identity_id = args["ids"][0].as_str().unwrap_or("identity-primary");
                        let email = if identity_id == "identity-selected" {
                            "selected@example.test"
                        } else {
                            "identity@example.test"
                        };
                        serde_json::json!({
                            "accountId": account_id,
                            "state": "identity-state",
                            "list": [{
                                "id": identity_id,
                                "name": "Sender",
                                "email": email
                            }],
                            "notFound": []
                        })
                    }
                    "Email/import" => serde_json::json!({
                        "accountId": account_id,
                        "oldState": "email-state-before-import",
                        "newState": "email-imported-state",
                        "created": {"i0": {"id": "email-created"}}
                    }),
                    "EmailSubmission/set" => {
                        let mut result = serde_json::json!({
                            "accountId": account_id,
                            "oldState": "submission-state-before",
                            "newState": "submission-state-after"
                        });
                        if let Some(update) = args["update"].as_object() {
                            let id = update.keys().next().expect("submission update id");
                            if self.fail_cancel {
                                result["notUpdated"] = Self::keyed(
                                    id,
                                    serde_json::json!({
                                        "type": "cannotUnsend",
                                        "description": "already released"
                                    }),
                                );
                            } else {
                                result["updated"] = Self::keyed(id, serde_json::Value::Null);
                            }
                        }
                        if let Some(create) = args["create"].as_object() {
                            let id = create.keys().next().expect("submission create id");
                            if self.fail_submission {
                                result["notCreated"] = Self::keyed(
                                    id,
                                    serde_json::json!({
                                        "type": "invalidRecipients",
                                        "description": "recipient refused"
                                    }),
                                );
                            } else {
                                result["created"] =
                                    Self::keyed(id, serde_json::json!({"id": "new-submission"}));
                            }
                        }
                        result
                    }
                    other => panic!("unexpected method {other}"),
                };
                if method == "EmailSubmission/set" && args["create"].is_object() {
                    match self.submission_reply {
                        Some("transport") => {
                            return Err(crate::core::transport::TransportError::new(
                                "scripted replacement transport failure",
                            ));
                        }
                        Some("missing") => continue,
                        Some("decode") => {
                            responses
                                .push(serde_json::json!([method, {"created": false}, call_id]));
                            continue;
                        }
                        Some("empty") => {
                            responses.push(serde_json::json!([method, {
                                "accountId": account_id,
                                "newState": "submission-state-after"
                            }, call_id]));
                            continue;
                        }
                        Some("method") => {
                            responses
                                .push(serde_json::json!(["error", {"type": "forbidden"}, call_id]));
                            continue;
                        }
                        _ => {}
                    }
                }
                responses.push(serde_json::json!([method, result, call_id]));
            }

            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": responses
                })
                .to_string(),
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "accountId": "primary",
                    "blobId": "blob-1",
                    "type": "message/rfc822",
                    "size": body.len()
                })
                .to_string(),
            ))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    fn send_account(transport: SendTransport) -> MailAccount<SendTransport> {
        let mut session = serde_json::to_value(search_session()).expect("session serializes");
        session["capabilities"]["urn:ietf:params:jmap:submission"] = serde_json::json!({});
        session["accounts"]["primary"]["accountCapabilities"]["urn:ietf:params:jmap:submission"] =
            serde_json::json!({"maxDelayedSend": 3600});
        session["primaryAccounts"]["urn:ietf:params:jmap:submission"] =
            serde_json::json!("primary");
        let client = crate::client::Client::with_transport(
            transport,
            serde_json::from_value(session).expect("submission session parses"),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        crate::account::Account::new(client, "primary")
    }

    fn send_state_map() -> StateMap {
        Arc::new(tokio::sync::Mutex::new(HashMap::new()))
    }

    fn basic_send_request() -> bifrost_types::SendRequest {
        let mut request = bifrost_types::SendRequest::default();
        request.from = Some(bifrost_types::Address::bare("from@example.test"));
        request.to = vec![bifrost_types::Address::bare("to@example.test")];
        request.body_text = Some("hello".to_string());
        request
    }

    #[tokio::test]
    async fn keyword_patch_avoids_an_account_wide_state_probe_and_precondition() {
        let transport = SendTransport::new(false, false, false, false);
        let states = send_state_map();
        state_cache::set(&states, "primary", "cached-state".to_string()).await;

        set_keyword(
            send_account(transport.clone()),
            Arc::clone(&states),
            "primary".to_string(),
            MutationTarget::Message(ObjectId("email-old".to_string())),
            "$seen".to_string(),
            true,
        )
        .await
        .expect("commutative keyword patch succeeds");

        assert_eq!(transport.method_sequence(), vec!["Email/set"]);
        let sets = transport.calls("Email/set");
        assert_eq!(sets.len(), 1);
        assert!(sets[0].get("ifInState").is_none());
        assert_eq!(sets[0]["update"]["email-old"]["keywords/$seen"], true);
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("email-updated-state")
        );
    }

    #[tokio::test]
    async fn draft_send_keeps_success_when_the_post_send_state_probe_fails() {
        let transport = SendTransport::new(false, false, true, false);
        let states = send_state_map();
        state_cache::set(&states, "primary", "email-before-send".to_string()).await;
        let result = draft_send(
            send_account(transport.clone()),
            Arc::clone(&states),
            "primary".to_string(),
            bifrost_types::DraftHandle("email-old".to_string()),
        )
        .await;

        assert_eq!(
            result.expect("send was committed"),
            ObjectId("email-old".to_string())
        );
        assert_eq!(
            transport.method_sequence(),
            vec!["Mailbox/get", "EmailSubmission/set", "Email/get"]
        );
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("email-before-send")
        );
        let sets = transport.calls("EmailSubmission/set");
        assert_eq!(sets.len(), 1);
        assert!(
            sets[0]["create"][SUBMISSION_CREATE_ID]
                .get("undoStatus")
                .is_none()
        );
    }

    #[tokio::test]
    async fn ambiguous_send_retains_email_and_requires_reconciliation() {
        for reply in ["missing", "decode", "empty", "method"] {
            for raw in [false, true] {
                let mut transport = SendTransport::new(false, false, false, false);
                transport.submission_reply = Some(reply);
                let states = send_state_map();
                let error = if raw {
                    send_raw_message(
                        send_account(transport.clone()),
                        states,
                        "primary".to_string(),
                        Bytes::from_static(b"raw message"),
                        None,
                    )
                    .await
                } else {
                    send_message(
                        send_account(transport.clone()),
                        states,
                        "primary".to_string(),
                        0,
                        None,
                        basic_send_request(),
                    )
                    .await
                }
                .expect_err("scripted submission result must fail");
                let destroys = transport
                    .calls("Email/set")
                    .into_iter()
                    .filter(|args| args["destroy"].is_array())
                    .count();
                if reply == "method" {
                    assert_eq!(destroys, 1, "confirmed method rejection permits cleanup");
                } else {
                    assert_eq!(destroys, 0, "ambiguous {reply} must retain the Email");
                    assert!(matches!(
                        error.recovery(),
                        bifrost_types::RecoveryClass::Reconcile(_)
                    ));
                    assert!(error.chain().iter().any(|cause| matches!(cause,
                        bifrost_types::Cause::Attempt(attempt)
                        if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
                    )));
                }
            }
        }
    }

    #[tokio::test]
    async fn replacement_failure_preserves_confirmed_cancellation() {
        for reply in [
            None,
            Some("missing"),
            Some("decode"),
            Some("empty"),
            Some("method"),
            Some("transport"),
        ] {
            let mut transport = SendTransport::new(true, false, false, false);
            transport.submission_reply = reply;
            let error = reschedule_send(
                send_account(transport.clone()),
                3600,
                ObjectId("old-submission".to_string()),
                SystemTime::now() + std::time::Duration::from_secs(60),
            )
            .await
            .expect_err("replacement must fail");
            let sets = transport.calls("EmailSubmission/set");
            assert_eq!(sets.len(), 2);
            assert_eq!(
                sets[0]["update"]["old-submission"]["undoStatus"],
                "canceled"
            );
            assert!(sets[1]["create"].is_object());
            assert_eq!(error.operation(), Some(AccountOperation::RescheduleSend));
            assert!(matches!(
                error.recovery(),
                bifrost_types::RecoveryClass::Reconcile(_)
            ));
            assert!(
                error
                    .user_safe_text()
                    .any(|text| text.contains("old schedule was canceled"))
            );
            assert!(error.chain().iter().any(|cause| matches!(cause,
                bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
            )));
        }
    }

    #[tokio::test]
    async fn failed_send_message_submission_best_effort_destroys_created_email() {
        let transport = SendTransport::new(true, false, false, false);
        let states = send_state_map();
        let result = send_message(
            send_account(transport.clone()),
            Arc::clone(&states),
            "primary".to_string(),
            0,
            None,
            basic_send_request(),
        )
        .await;

        assert!(
            result.is_err(),
            "the rejected submission must remain an error"
        );
        let sets = transport.calls("Email/set");
        assert_eq!(sets.len(), 2, "create, followed by best-effort destroy");
        assert!(sets[0]["create"].is_object());
        assert_eq!(sets[1]["destroy"], serde_json::json!(["email-created"]));
        assert!(sets[1].get("ifInState").is_none());
        let submissions = transport.calls("EmailSubmission/set");
        assert_eq!(submissions.len(), 1);
        assert!(
            submissions[0]["create"][SUBMISSION_CREATE_ID]
                .get("undoStatus")
                .is_none()
        );
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("email-deleted-state")
        );
    }

    #[tokio::test]
    async fn failed_raw_send_submission_best_effort_destroys_imported_email() {
        let transport = SendTransport::new(true, false, false, false);
        let states = send_state_map();
        let result = send_raw_message(
            send_account(transport.clone()),
            Arc::clone(&states),
            "primary".to_string(),
            Bytes::from_static(b"raw message"),
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "the rejected submission must remain an error"
        );
        let imports = transport.calls("Email/import");
        assert_eq!(imports.len(), 1);
        let destroys = transport.calls("Email/set");
        assert_eq!(destroys.len(), 1, "the imported email is destroyed");
        assert_eq!(destroys[0]["destroy"], serde_json::json!(["email-created"]));
        assert!(destroys[0].get("ifInState").is_none());
        let submissions = transport.calls("EmailSubmission/set");
        assert_eq!(submissions.len(), 1);
        assert!(
            submissions[0]["create"][SUBMISSION_CREATE_ID]
                .get("undoStatus")
                .is_none()
        );
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("email-deleted-state")
        );
    }

    #[tokio::test]
    async fn scheduled_send_omits_server_owned_undo_status() {
        let transport = SendTransport::new(false, false, false, false);
        let mut request = basic_send_request();
        request.save_to_sent = Some(false);
        request.scheduled =
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800));
        let result = send_message(
            send_account(transport.clone()),
            send_state_map(),
            "primary".to_string(),
            usize::MAX,
            None,
            request,
        )
        .await;

        assert_eq!(
            result.expect("scheduled send was committed"),
            ObjectId("new-submission".to_string())
        );
        let submissions = transport.calls("EmailSubmission/set");
        assert_eq!(submissions.len(), 1);
        let create = &submissions[0]["create"][SUBMISSION_CREATE_ID];
        assert!(create.get("undoStatus").is_none());
        assert_eq!(
            submissions[0]["onSuccessDestroyEmail"],
            serde_json::json!([format!("#{SUBMISSION_CREATE_ID}")])
        );
        assert!(create["envelope"]["mailFrom"]["parameters"]["holduntil"].is_string());
    }

    #[tokio::test]
    async fn scheduled_send_uses_the_selected_identity_when_from_is_absent() {
        let transport = SendTransport::new(false, false, false, false);
        let mut request = basic_send_request();
        request.from = None;
        request.identity = Some(bifrost_types::IdentityId("identity-selected".to_string()));
        request.scheduled =
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800));

        let result = send_message(
            send_account(transport.clone()),
            send_state_map(),
            "primary".to_string(),
            usize::MAX,
            None,
            request,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(
            transport.method_sequence(),
            vec![
                "Identity/get",
                "Mailbox/get",
                "Email/set",
                "EmailSubmission/set"
            ]
        );
        let identities = transport.calls("Identity/get");
        assert_eq!(
            identities[0]["ids"],
            serde_json::json!(["identity-selected"])
        );
        let emails = transport.calls("Email/set");
        let email_create = emails[0]["create"]
            .as_object()
            .and_then(|creates| creates.values().next())
            .expect("Email/create entry");
        assert_eq!(email_create["from"][0]["email"], "selected@example.test");
        let submissions = transport.calls("EmailSubmission/set");
        let create = &submissions[0]["create"][SUBMISSION_CREATE_ID];
        assert_eq!(
            create["envelope"]["mailFrom"]["email"],
            "selected@example.test"
        );
        assert!(create["envelope"]["mailFrom"]["parameters"]["holduntil"].is_string());
        assert_eq!(create["identityId"], "identity-selected");
    }

    #[tokio::test]
    async fn scheduled_send_does_not_guess_an_identity_when_none_is_selected() {
        let transport = SendTransport::new(false, false, false, false);
        let mut request = basic_send_request();
        request.from = None;
        request.identity = None;
        request.scheduled =
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800));

        let error = send_message(
            send_account(transport.clone()),
            send_state_map(),
            "primary".to_string(),
            usize::MAX,
            None,
            request,
        )
        .await
        .expect_err("JMAP does not identify a default Identity");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(transport.method_sequence().is_empty());
    }

    #[tokio::test]
    async fn reschedule_cancels_before_creating_and_preserves_the_email() {
        let transport = SendTransport::new(false, false, false, false);
        let result = reschedule_send(
            send_account(transport.clone()),
            usize::MAX,
            ObjectId("old-submission".to_string()),
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800),
        )
        .await;

        assert_eq!(
            result.expect("replacement submission was committed"),
            ObjectId("new-submission".to_string())
        );
        assert_eq!(
            transport.method_sequence(),
            vec![
                "EmailSubmission/get",
                "Email/get",
                "EmailSubmission/set",
                "EmailSubmission/set"
            ]
        );
        let sets = transport.calls("EmailSubmission/set");
        assert_eq!(sets.len(), 2);
        assert!(sets[0]["update"]["old-submission"].is_object());
        assert!(sets[0].get("create").is_none());
        assert!(
            sets[1]["create"][SUBMISSION_CREATE_ID]
                .get("undoStatus")
                .is_none()
        );
        assert!(sets[1].get("update").is_none());
    }

    #[tokio::test]
    async fn reschedule_does_not_create_when_cancellation_is_refused() {
        let transport = SendTransport::new(false, true, false, false);
        let result = reschedule_send(
            send_account(transport.clone()),
            usize::MAX,
            ObjectId("old-submission".to_string()),
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800),
        )
        .await;

        assert!(result.is_err());
        let sets = transport.calls("EmailSubmission/set");
        assert_eq!(sets.len(), 1, "the replacement is never created");
        assert!(sets[0].get("create").is_none());
    }

    #[tokio::test]
    async fn reschedule_checks_the_email_before_canceling_legacy_destroyed_schedules() {
        let transport = SendTransport::new(false, false, false, true);
        let result = reschedule_send(
            send_account(transport.clone()),
            usize::MAX,
            ObjectId("old-submission".to_string()),
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            transport.method_sequence(),
            vec!["EmailSubmission/get", "Email/get"]
        );
        assert!(transport.calls("EmailSubmission/set").is_empty());
    }

    #[derive(Clone)]
    struct DeleteThreadTransport {
        calls: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl DeleteThreadTransport {
        fn new() -> Self {
            Self {
                calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<serde_json::Value> {
            self.calls.lock().expect("calls").clone()
        }
    }

    impl crate::core::transport::HttpTransport for DeleteThreadTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            self.calls.lock().expect("calls").push(call.clone());
            let method = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].clone();
            let account_id = call[1]["accountId"].clone();
            let arguments = match method.as_str() {
                "Mailbox/get" => serde_json::json!({
                    "accountId": account_id,
                    "state": "mailbox-state",
                    "list": [{ "id": "trash-id", "name": "Trash", "role": "trash" }],
                    "notFound": []
                }),
                "Thread/get" => serde_json::json!({
                    "accountId": account_id,
                    "state": "thread-state",
                    "list": [{
                        "id": "thread-1",
                        "emailIds": ["sent-copy", "inbox-copy", "trash-copy"]
                    }],
                    "notFound": []
                }),
                "Email/get" => {
                    let requested = call[1]["ids"]
                        .as_array()
                        .expect("Email/get ids")
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .collect::<HashSet<_>>();
                    let list = vec![
                        serde_json::json!({
                            "id": "trash-copy",
                            "mailboxIds": { "trash-id": true, "sent-id": false }
                        }),
                        serde_json::json!({
                            "id": "sent-copy",
                            "mailboxIds": { "sent-id": true }
                        }),
                        serde_json::json!({
                            "id": "inbox-copy",
                            "mailboxIds": { "inbox-id": true }
                        }),
                    ]
                    .into_iter()
                    .filter(|email| {
                        email["id"]
                            .as_str()
                            .is_some_and(|id| requested.contains(id))
                    })
                    .collect::<Vec<_>>();
                    serde_json::json!({
                        "accountId": account_id,
                        "state": "email-state",
                        "list": list,
                        "notFound": []
                    })
                }
                "Email/set" => serde_json::json!({
                    "accountId": account_id,
                    "oldState": "email-state",
                    "newState": "email-next",
                    "destroyed": call[1]["destroy"],
                    "notDestroyed": {}
                }),
                other => panic!("unexpected method {other}"),
            };
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[method, arguments, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    #[tokio::test]
    async fn deleting_a_thread_from_trash_destroys_only_members_in_trash() {
        let transport = DeleteThreadTransport::new();
        let mut session = serde_json::to_value(search_session()).expect("session serializes");
        session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInGet"] =
            serde_json::json!(2);
        let client = crate::client::Client::with_transport(
            transport.clone(),
            serde_json::from_value(session).expect("session parses"),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client, "primary");
        let states: StateMap = std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        delete_thread(
            mail,
            states,
            "primary".to_string(),
            ThreadId("thread-1".to_string()),
            Some(ContainerId("trash-id".to_string())),
        )
        .await
        .expect("only the Trash copy is destroyed");

        let calls = transport.calls();
        assert_eq!(
            calls
                .iter()
                .map(|call| call[0].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "Mailbox/get",
                "Thread/get",
                "Email/get",
                "Email/get",
                "Email/set"
            ]
        );
        assert_eq!(calls[2][1]["ids"].as_array().unwrap().len(), 2);
        assert_eq!(calls[3][1]["ids"].as_array().unwrap().len(), 1);
        for get in [&calls[2], &calls[3]] {
            assert_eq!(
                get[1]["properties"],
                serde_json::json!(["id", "mailboxIds"])
            );
        }
        assert_eq!(calls[4][1]["destroy"], serde_json::json!(["trash-copy"]));
        assert_eq!(calls[4][1]["ifInState"], "email-state");
        assert!(calls[4][1].get("update").is_none());
    }
}
