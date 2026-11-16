//! Immediate and scheduled submission workflows and commit evidence.

use super::body::{address_to_jmap, build_email_create_from_send};
use super::containers::{missing_role_mailbox, role_mailboxes};
use super::mail_mutation::destroy_emails;
use super::settings::{foreign_sending_identity, primary_sending_identity};
use super::{MailAccount, SubmissionAccount, checked_timestamp, to_acct_err};
use crate::core::transport::HttpTransport;
use crate::email::{DRAFT_KEYWORD, Email, EmailGet, EmailId, EmailSet, Property as EmailProperty};
use crate::email_submission::{Address as SubmissionAddress, EmailSubmissionSet, UndoStatus};
use crate::identity::IdentityId as JmapIdentityId;
use crate::sync::error::{
    answer_error, finish_partial, reschedule_missing_field, reschedule_not_found,
    scheduled_requires_envelope,
};
use crate::sync::state_cache;
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, Application, CompensationOutcome, ErrorScope,
    FolderRole, ObjectId, PartialCommitBuilder, PrimaryLeg, StepEffect, StepReplay, StepTargets,
    TargetRef, TargetRole,
};
use bytes::Bytes;
use std::time::SystemTime;

pub(super) const SUBMISSION_CREATE_ID: &str = "submit0";
/// Resolve From/Sender for a foreign submission. `OnBehalfOf` keeps an
/// explicit consumer From for compatibility with Graph; otherwise the
/// foreign identity supplies it. The envelope deliberately follows From.
pub(super) fn resolve_foreign_headers(
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

pub(super) fn email_targets(id: Option<&EmailId>) -> StepTargets {
    id.map_or_else(StepTargets::unknown, |id| {
        StepTargets::complete(vec![TargetRef::new(
            TargetRole::Subject,
            ErrorScope::Message {
                id: ObjectId(id.as_str().to_owned()),
            },
        )])
    })
}
fn submission_targets(id: Option<&str>) -> StepTargets {
    id.map_or_else(StepTargets::unknown, |id| {
        StepTargets::complete(vec![TargetRef::new(
            TargetRole::Subject,
            ErrorScope::Submission {
                id: ObjectId(id.to_owned()),
            },
        )])
    })
}
fn submission_create_error(
    error: crate::Error,
    operation: AccountOperation,
) -> (AccountError, bool) {
    let rejected = match &error {
        crate::Error::Set(_) => true,
        crate::Error::Method(e) => !matches!(
            e.error_type(),
            crate::core::error::MethodErrorType::ServerPartialFail
        ),
        _ => false,
    };
    // An omitted submission create is not a refusal: it may have landed.
    (answer_error(operation)(error), rejected)
}
/// A send whose submission did not succeed. `rejected` is a proven refusal
/// (`notCreated`, or any method error except `serverPartialFail`): the
/// primary takes `from_leaf`. Anything else - an unreadable envelope, a
/// missing or malformed submission result, `serverPartialFail` - proves
/// nothing about the submission and is raised to `Uncertain`; such a leaf is
/// built from a received response and carries no `Unsent` evidence. `email`
/// is the Email this call created, recorded as a committed `Created` step;
/// `cleanup` is the destroy compensation, which only a proven refusal runs.
fn failed_submission(
    error: AccountError,
    rejected: bool,
    email: Option<&EmailId>,
    cleanup: Option<CompensationOutcome>,
) -> AccountError {
    if rejected && email.is_none() {
        return error;
    }
    let application = if rejected {
        Application::from_leaf(&error)
    } else {
        Application::Uncertain
    };
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Submitted,
        StepTargets::at_least(email_targets(email).refs),
        StepReplay::Blocking,
        application,
    ));
    if let Some(email) = email {
        let targets = email_targets(Some(email));
        let create = record.committed(StepEffect::Created, targets.clone(), StepReplay::Repeatable);
        if let Some(outcome) = cleanup {
            record.compensation(StepEffect::Deleted, targets, vec![create], outcome);
        }
    }
    finish_partial(error, record)
}
/// A lone `Uncertain` submission over `subject` (the caller's draft for
/// `draft_send`, unknown when the envelope was unreadable). Same raise as
/// `failed_submission`, with no committed step.
fn uncertain_submission(error: AccountError, subject: Option<&EmailId>) -> AccountError {
    let record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Submitted,
        StepTargets::at_least(email_targets(subject).refs),
        StepReplay::Blocking,
        Application::Uncertain,
    ));
    finish_partial(error, record)
}
/// The submission was acknowledged but a result the call needs had no usable
/// id. Both effects certainly happened; an Email without a known id is
/// `Blocking` because a replay would send again while it is unidentified, and
/// an unidentified committed `Blocking` step gates, so the result stays
/// check-before-resend rather than the extraction leaf's terminal barrier.
fn acknowledged_send_error(
    error: AccountError,
    email: Option<&EmailId>,
    submission: Option<&str>,
) -> AccountError {
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.committed(
        StepEffect::Created,
        email_targets(email),
        if email.is_some() {
            StepReplay::Repeatable
        } else {
            StepReplay::Blocking
        },
    );
    record.committed(
        StepEffect::Submitted,
        submission_targets(submission),
        StepReplay::Blocking,
    );
    finish_partial(error, record)
}
/// Map a whole-request failure at a send boundary. An evidence-less custom
/// transport failure is stamped `InFlight` (a single-leg leaf that already
/// reconciles); an unreadable envelope is stamped `Acknowledged` and recorded
/// as an `Uncertain` submission over `subject`.
fn send_boundary_error(
    op: AccountOperation,
    subject: Option<EmailId>,
) -> impl FnOnce(crate::Error) -> AccountError {
    move |error| {
        let unreadable = matches!(
            error,
            crate::Error::EnvelopeDecode(_) | crate::Error::ResultDecode { .. }
        );
        let ctx = crate::sync::error::JmapErrorContext::send(op);
        let error = crate::sync::error::into_account_error(error, ctx);
        if unreadable {
            uncertain_submission(error, subject.as_ref())
        } else {
            error
        }
    }
}

pub(super) fn require_created_email_id(
    email: &Email,
    op: AccountOperation,
) -> Result<EmailId, AccountError> {
    crate::sync::error::require_created_id(
        email.id().map(EmailId::as_str),
        op,
        "Email/set or Email/import",
    )
    .map(EmailId::new)
}

fn require_created_submission_id(
    submission: &crate::email_submission::EmailSubmission,
    op: AccountOperation,
) -> Result<crate::email_submission::EmailSubmissionId, AccountError> {
    crate::sync::error::require_created_id(
        submission
            .id()
            .map(crate::email_submission::EmailSubmissionId::as_str),
        op,
        "EmailSubmission/set",
    )
    .map(crate::email_submission::EmailSubmissionId::new)
}

/// What a send asked the server to do to its Email once the submission
/// succeeded (RFC 8621 s7.5 `onSuccessUpdateEmail` / `onSuccessDestroyEmail`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnSuccess {
    /// Move it into Sent and drop Drafts and `$draft`.
    File,
    /// Destroy it (`save_to_sent == Some(false)`).
    Destroy,
}

/// How the implicit `Email/set` answered for the send's Email.
#[derive(Debug)]
enum ImplicitOutcome {
    Applied,
    /// Proven not applied: the id is in `notUpdated` / `notDestroyed`, or the
    /// implicit call failed with a method-level error other than
    /// `serverPartialFail`, which RFC 8620 s3.6.2 guarantees made no change.
    Refused(crate::Error),
    /// Nothing proves either way: the implicit response is absent (RFC 8621
    /// s7.5 requires it after a successful submission carrying onSuccess
    /// arguments), undecodable, under another method name, a
    /// `serverPartialFail`, or silent about the id.
    Unknown(crate::Error),
}

/// An `Email/set` oldState -> newState transition.
type EmailStateTransition = (
    crate::core::id::ObjectState<Email>,
    crate::core::id::ObjectState<Email>,
);

/// Read the implicit `Email/set` RFC 8621 s7.5 appends under the submission
/// call id, after the explicit submission response has been consumed (so the
/// first remaining response under that id is the implicit one). Returns its
/// oldState -> newState transition and the outcome for `email`, the native
/// id the server keys its answer by, never a foreign-encoded id.
fn take_implicit_email_set(
    response: &mut crate::core::response::Response,
    handle: &crate::core::request::CallHandle<EmailSubmissionSet>,
    email: &EmailId,
    on_success: OnSuccess,
) -> (Option<EmailStateTransition>, ImplicitOutcome) {
    let mut result = match response.get_method::<EmailSet>(handle) {
        Ok(result) => result,
        Err(error) => {
            let refused = matches!(
                &error,
                crate::Error::Method(e) if !matches!(
                    e.error_type(),
                    crate::core::error::MethodErrorType::ServerPartialFail
                )
            );
            let outcome = if refused {
                ImplicitOutcome::Refused(error)
            } else {
                ImplicitOutcome::Unknown(error)
            };
            return (None, outcome);
        }
    };
    let state = state_cache::set_transition(result.old_state(), result.new_state());
    let answer = match on_success {
        OnSuccess::File => result.updated(email).map(drop),
        OnSuccess::Destroy => result.destroyed(email),
    };
    let outcome = match answer {
        Ok(()) => ImplicitOutcome::Applied,
        Err(error @ crate::Error::IdUnanswered { .. }) => ImplicitOutcome::Unknown(error),
        Err(error) => ImplicitOutcome::Refused(error),
    };
    (state, outcome)
}

/// The `SentCopy` a JMAP send reports from its implicit `Email/set`.
/// `public_id` is the email id as the consumer addresses it (foreign-encoded
/// for a send-as through a foreign account). A destroy that did not confirm
/// leaves `NotRequested` standing - no call ever targeted Sent - and is only
/// logged: no receipt field carries a routable handle for the staging Email.
fn implicit_sent_copy(
    outcome: ImplicitOutcome,
    on_success: OnSuccess,
    public_id: ObjectId,
) -> bifrost_types::SentCopy {
    use bifrost_types::{SentCopy, SentCopyNotFiled};
    let warning = |error: &crate::Error, what: &str| {
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            format!("Message sent, but {what}; it may remain in Drafts."),
        )
        .with_protocol_detail(bifrost_types::DiagnosticText::support_only(
            error.to_string(),
        ))
    };
    match (on_success, outcome) {
        (OnSuccess::File, ImplicitOutcome::Applied) => SentCopy::Filed {
            id: Some(public_id),
        },
        (OnSuccess::File, ImplicitOutcome::Refused(error)) => SentCopy::NotFiled {
            reason: SentCopyNotFiled::MoveRefused,
            detail: Some(warning(&error, "the server refused to move it to Sent")),
        },
        (OnSuccess::File, ImplicitOutcome::Unknown(error)) => SentCopy::Uncertain {
            detail: warning(&error, "the server did not confirm moving it to Sent"),
        },
        (OnSuccess::Destroy, ImplicitOutcome::Applied) => SentCopy::NotRequested,
        (OnSuccess::Destroy, ImplicitOutcome::Refused(error) | ImplicitOutcome::Unknown(error)) => {
            tracing::warn!(
                email = %public_id.0,
                %error,
                "JMAP send: the staging Email for save_to_sent = false was not confirmed destroyed"
            );
            SentCopy::NotRequested
        }
    }
}

pub(crate) fn send_message<T: HttpTransport>(
    mail: SubmissionAccount<T>,
    max_delayed_send: usize,
    foreign: Option<ForeignSubmission>,
    mut request: bifrost_types::SendRequest,
) -> AccountFuture<Result<bifrost_types::SendReceipt, AccountError>> {
    let email_states = mail.email_states.clone();
    let account_id = mail.id_str().to_string();

    Box::pin(async move {
        let mail = mail
            .view::<crate::core::capability::Mail>()
            .map_err(to_acct_err(AccountOperation::Send))?;

        // Every id this call hands back - the receipt id, the Sent copy, and
        // the partial-commit targets - is in the public form: a send-as
        // through a foreign account owner-qualifies the native id, because
        // id-only readers (`get_stream`, `open_blob`, mutations) would route
        // a bare one to the primary account. The wire (implicit Email/set
        // lookup, cleanup destroy) keeps the native id.
        let is_foreign = foreign.is_some();
        let public = |native: &str| {
            if is_foreign {
                crate::sync::foreign::encode_object(&account_id, native)
            } else {
                native.to_owned()
            }
        };
        request.validate_recipients(bifrost_types::Protocol::Jmap)?;
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
                return Err(crate::sync::error::unsupported_error(
                    AccountOperation::Send,
                    None,
                    bifrost_types::UnsupportedReason::ScheduledSendUnavailable,
                    "JMAP relay advertises no scheduled-send window",
                ));
            }
            // The window source is the server's maxDelayedSend seconds.
            let window = std::time::Duration::from_secs(max_delayed_send as u64);
            bifrost_types::validate_scheduled(at, Some(window))?;
        }
        let hold_until = scheduled
            .map(|at| checked_timestamp(at, AccountOperation::Send).map(|at| at.to_string()))
            .transpose()?;
        let envelope_recipients = request
            .to
            .iter()
            .chain(&request.cc)
            .chain(&request.bcc)
            .cloned()
            .collect::<Vec<_>>();
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
                if let Some(at) = hold_until {
                    mail_from = mail_from.with_parameter("holduntil", Some(at));
                }
                submit.envelope(
                    mail_from,
                    envelope_recipients
                        .iter()
                        .map(submission_address_from_compose),
                );
            }
        }

        let on_success = if let Some(sent) = sent_mailbox.as_ref() {
            submission_set
                .on_success_update_email(SUBMISSION_CREATE_ID)
                .submitted_to_sent(sent, Some(&draft_mailbox));
            OnSuccess::File
        } else {
            submission_set = submission_set.on_success_destroy_email(SUBMISSION_CREATE_ID);
            OnSuccess::Destroy
        };

        // Each Email response is observed from its own oldState, so a
        // concurrent edit cannot be blessed by an unrelated send.
        let mut batch = mail.build();
        let email_handle = batch
            .call(email_set)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_ref = email_handle.result_reference(format!("/created/{email_create_id}/id"));
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id_ref(email_ref);
        let submission_handle = batch
            .call_as(submission_set)
            .map_err(to_acct_err(AccountOperation::Send))?;

        let mut response = batch
            .send()
            .await
            .map_err(send_boundary_error(AccountOperation::Send, None))?;

        // Inspect the submission outcome before extracting the preceding
        // Email create. A malformed Email result must not hide that the
        // submission create was acknowledged and may already be live.
        let submission_result = response
            .get(&submission_handle)
            .and_then(|mut result| result.created(SUBMISSION_CREATE_ID))
            .map_err(|error| submission_create_error(error, AccountOperation::Send));
        let email_result = response
            .get(&email_handle)
            .map_err(to_acct_err(AccountOperation::Send))
            .map(|result| {
                let new_state = state_cache::set_transition(result.old_state(), result.new_state());
                (result, new_state)
            });

        if let Ok((_, transition)) = email_result.as_ref() {
            email_states.observe_set(transition.clone()).await;
        }
        let submission = match submission_result {
            Ok(submission) => submission,
            Err((error, rejected)) => {
                let email = email_result
                    .ok()
                    .and_then(|(mut result, _)| result.created(&email_create_id).ok())
                    .and_then(|email| {
                        require_created_email_id(&email, AccountOperation::Send).ok()
                    });
                // The Email is destroyed only on a proven refusal, and only
                // when this call created it.
                let cleanup = match (&email, rejected) {
                    (Some(email), true) => Some(discard_created_send_email(&mail, email).await),
                    _ => None,
                };
                let public_email = email.map(|email| EmailId::new(public(email.as_str())));
                return Err(failed_submission(
                    error,
                    rejected,
                    public_email.as_ref(),
                    cleanup,
                ));
            }
        };

        let (email_result, _created_state) = match email_result {
            Ok((mut result, state)) => (
                result
                    .created(&email_create_id)
                    .map_err(answer_error(AccountOperation::Send))
                    .and_then(|email| require_created_email_id(&email, AccountOperation::Send)),
                state,
            ),
            Err(error) => (Err(error), None),
        };
        // RFC 8621 appends the implicit Email/set result under the
        // submission call id. Its state follows the initial Email create,
        // and is observed as a separate oldState -> newState transition.
        // A concurrent change between the calls must remain unobserved.
        // Its per-item answer for the created Email decides the Sent copy.
        let (implicit_email_state, implicit) = match email_result.as_ref() {
            Ok(email_id) => {
                let (state, outcome) = take_implicit_email_set(
                    &mut response,
                    &submission_handle,
                    email_id,
                    on_success,
                );
                (state, Some(outcome))
            }
            Err(_) => (
                response
                    .get_method::<EmailSet>(&submission_handle)
                    .ok()
                    .and_then(|result| {
                        state_cache::set_transition(result.old_state(), result.new_state())
                    }),
                None,
            ),
        };
        email_states.observe_set(implicit_email_state).await;

        let email_id = email_result.map_err(|error| {
            let public_submission = submission.id().map(|id| public(id.as_str()));
            acknowledged_send_error(error, None, public_submission.as_deref())
        })?;
        let public_email_id = EmailId::new(public(email_id.as_str()));
        let sent_copy = implicit_sent_copy(
            implicit.expect("the implicit outcome is read whenever the email id is"),
            on_success,
            ObjectId(public_email_id.as_str().to_owned()),
        );

        // A scheduled send returns its EmailSubmission id for
        // cancel/reschedule (scheduled send-as is refused upstream, so this
        // arm is primary-only). An immediate send returns the Email id.
        let id = if scheduled.is_some() {
            let submission_id = require_created_submission_id(&submission, AccountOperation::Send)
                .map_err(|error| acknowledged_send_error(error, Some(&public_email_id), None))?;
            ObjectId(public(submission_id.as_str()))
        } else {
            ObjectId(public_email_id.as_str().to_owned())
        };
        // deliveryStatus is asynchronous delivery evidence, not the SMTP
        // acceptance transcript for this submission. Even delivered=no can
        // describe a later bounce after acceptance. An Outlook user must not
        // be invited to resend an accepted message from that evidence; leave
        // recipients_not_accepted empty rather than reinterpret delivery as
        // submission refusal. This applies to scheduled submissions too.
        Ok(bifrost_types::SendReceipt::new(id, sent_copy))
    })
}

pub(crate) fn send_raw_message<T: HttpTransport>(
    mail: SubmissionAccount<T>,
    raw: Bytes,
    save_to_sent: Option<bool>,
) -> AccountFuture<Result<bifrost_types::SendReceipt, AccountError>> {
    let email_states = mail.email_states.clone();

    Box::pin(async move {
        let mail = mail
            .view::<crate::core::capability::Mail>()
            .map_err(to_acct_err(AccountOperation::Send))?;

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
            entry.keywords([crate::email::keyword::Keyword::parse(DRAFT_KEYWORD)
                .expect("valid standard keyword")]);
            entry.create_id()
        };

        let mut submission_set = EmailSubmissionSet::new();
        submission_set.create_with_id(SUBMISSION_CREATE_ID);
        let on_success = if let Some(sent) = sent_mailbox.as_ref() {
            submission_set
                .on_success_update_email(SUBMISSION_CREATE_ID)
                .submitted_to_sent(sent, Some(&draft_mailbox));
            OnSuccess::File
        } else {
            submission_set = submission_set.on_success_destroy_email(SUBMISSION_CREATE_ID);
            OnSuccess::Destroy
        };

        let mut batch = mail.build();
        let import_handle = batch
            .call(import_req)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_ref = import_handle.result_reference(format!("/created/{import_create_id}/id"));
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id_ref(email_ref);
        let submission_handle = batch
            .call_as(submission_set)
            .map_err(to_acct_err(AccountOperation::Send))?;

        let mut response = batch
            .send()
            .await
            .map_err(send_boundary_error(AccountOperation::Send, None))?;
        let submission_result = response
            .get(&submission_handle)
            .and_then(|mut result| result.created(SUBMISSION_CREATE_ID))
            .map_err(|error| submission_create_error(error, AccountOperation::Send));
        let email_result = response
            .get(&import_handle)
            .map_err(to_acct_err(AccountOperation::Send))
            .map(|result| {
                let imported_state =
                    state_cache::set_transition(result.old_state(), Some(result.new_state()));
                (result, imported_state)
            });

        if let Ok((_, transition)) = email_result.as_ref() {
            email_states.observe_set(transition.clone()).await;
        }
        let submission = match submission_result {
            Ok(submission) => submission,
            Err((error, rejected)) => {
                let email = email_result
                    .ok()
                    .and_then(|(mut result, _)| result.created(&import_create_id).ok())
                    .and_then(|email| {
                        require_created_email_id(&email, AccountOperation::Send).ok()
                    });
                // The Email is destroyed only on a proven refusal, and only
                // when this call created it.
                let cleanup = match (&email, rejected) {
                    (Some(email), true) => Some(discard_created_send_email(&mail, email).await),
                    _ => None,
                };
                return Err(failed_submission(error, rejected, email.as_ref(), cleanup));
            }
        };
        let (email_result, _imported_state) = match email_result {
            Ok((mut result, state)) => (
                result
                    .created(&import_create_id)
                    .map_err(answer_error(AccountOperation::Send))
                    .and_then(|email| require_created_email_id(&email, AccountOperation::Send)),
                state,
            ),
            Err(error) => (Err(error), None),
        };
        // The implicit Email/set's state follows the import; its per-item
        // answer for the imported Email decides the Sent copy.
        let (implicit_email_state, implicit) = match email_result.as_ref() {
            Ok(email_id) => {
                let (state, outcome) = take_implicit_email_set(
                    &mut response,
                    &submission_handle,
                    email_id,
                    on_success,
                );
                (state, Some(outcome))
            }
            Err(_) => (
                response
                    .get_method::<EmailSet>(&submission_handle)
                    .ok()
                    .and_then(|result| {
                        state_cache::set_transition(result.old_state(), result.new_state())
                    }),
                None,
            ),
        };
        email_states.observe_set(implicit_email_state).await;
        let email_id = email_result.map_err(|error| {
            acknowledged_send_error(
                error,
                None,
                submission
                    .id()
                    .map(crate::email_submission::EmailSubmissionId::as_str),
            )
        })?;
        let id = ObjectId(email_id.into_string());
        let sent_copy = implicit_sent_copy(
            implicit.expect("the implicit outcome is read whenever the email id is"),
            on_success,
            id.clone(),
        );
        // deliveryStatus is asynchronous delivery evidence, not the SMTP
        // acceptance transcript for this submission. Even delivered=no can
        // describe a later bounce after acceptance. An Outlook user must not
        // be invited to resend an accepted message from that evidence; leave
        // recipients_not_accepted empty rather than reinterpret delivery as
        // submission refusal. This applies to scheduled submissions too.
        Ok(bifrost_types::SendReceipt::new(id, sent_copy))
    })
}

/// Destroy the Email a refused send created, and report how that cleanup
/// ended. The outcome comes from `CompensationOutcome::from_error`, with one
/// stated override: a decoded response missing the `destroyed(id)` result
/// (mapped to `Protocol(PartialResponse)`, which `from_error` would call
/// `Failed` for its acknowledged evidence) is `Unconfirmed` - the destroy may
/// have applied.
async fn discard_created_send_email<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_id: &EmailId,
) -> CompensationOutcome {
    match destroy_emails(mail, [email_id.clone()], AccountOperation::Send).await {
        Ok(()) => CompensationOutcome::Succeeded,
        Err(error)
            if matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::PartialResponse
                )
            ) =>
        {
            CompensationOutcome::Unconfirmed(error)
        }
        Err(error) => CompensationOutcome::from_error(error),
    }
}

pub(crate) fn draft_send<T: HttpTransport>(
    mail: SubmissionAccount<T>,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<bifrost_types::SendReceipt, AccountError>> {
    let email_states = mail.email_states.clone();

    Box::pin(async move {
        let mail = mail
            .view::<crate::core::capability::Mail>()
            .map_err(to_acct_err(AccountOperation::DraftSend))?;

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
        let prior_state = email_states.guard().await;
        let mut request = mail.build();
        let submission_handle = request
            .call_as(submission_set)
            .map_err(to_acct_err(AccountOperation::DraftSend))?;
        let mut response = request.send().await.map_err(send_boundary_error(
            AccountOperation::DraftSend,
            Some(draft_id.clone()),
        ))?;
        // A proven refusal is the plain leaf (a lone `NotApplied` primary);
        // anything else is an uncertain submission of the caller's draft,
        // which `draft_send` never compensates.
        response
            .get(&submission_handle)
            .and_then(|mut result| result.created(SUBMISSION_CREATE_ID))
            .map_err(|error| {
                let (error, rejected) = submission_create_error(error, AccountOperation::DraftSend);
                if rejected {
                    error
                } else {
                    uncertain_submission(error, Some(&draft_id))
                }
            })?;
        // The submission is already committed. A failed cache refresh must
        // not turn that success into an error that invites a duplicate send;
        // the stale state will be reconciled by the next conditional mutation.
        let (implicit_state, implicit) = take_implicit_email_set(
            &mut response,
            &submission_handle,
            &draft_id,
            OnSuccess::File,
        );
        if implicit_state.is_some() {
            email_states.observe_set(implicit_state).await;
        } else if let Ok(fresh) = crate::sync::mail_set::probe_email_state(&mail).await {
            email_states
                .observe_probe(prior_state.as_ref(), fresh)
                .await;
        }
        let id = ObjectId(draft_id.into_string());
        let sent_copy = implicit_sent_copy(implicit, OnSuccess::File, id.clone());
        // deliveryStatus is asynchronous delivery evidence, not the SMTP
        // acceptance transcript for this submission. Even delivered=no can
        // describe a later bounce after acceptance. An Outlook user must not
        // be invited to resend an accepted message from that evidence; leave
        // recipients_not_accepted empty rather than reinterpret delivery as
        // submission refusal. This applies to scheduled submissions too.
        Ok(bifrost_types::SendReceipt::new(id, sent_copy))
    })
}

fn submission_address_from_compose(address: &bifrost_types::Address) -> SubmissionAddress {
    SubmissionAddress::new(address.address.clone())
}

/// Cancel a scheduled JMAP submission by id: `EmailSubmission/set`
/// update setting `undoStatus: canceled`.
pub(crate) fn cancel_scheduled_send<T: HttpTransport>(
    submission_account: SubmissionAccount<T>,
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
            .map_err(crate::sync::error::answer_error(
                AccountOperation::CancelScheduledSend,
            ))?;
        Ok(())
    })
}

/// Reschedule a scheduled JMAP submission. JMAP has no in-place
/// reschedule: cancel the existing submission and create a new one
/// referencing the same `emailId` with the new `holduntil`. Returns the
/// new submission id.
pub(crate) fn reschedule_send<T: HttpTransport>(
    submission_account: SubmissionAccount<T>,
    max_delayed_send: usize,
    handle: ObjectId,
    scheduled: SystemTime,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let window = std::time::Duration::from_secs(max_delayed_send as u64);
        bifrost_types::validate_scheduled(scheduled, Some(window))?;
        // Refuse before cancelling the existing scheduled send.
        let hold_until =
            checked_timestamp(scheduled, AccountOperation::RescheduleSend)?.to_string();

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
        let mail_account = submission_account
            .view::<crate::core::capability::Mail>()
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        let email_response = mail_account
            .call(email_get)
            .await
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        match email_response.answer(&email_id, |email| email.id() == Some(&email_id)) {
            crate::core::answer::IdAnswer::NotFound(_) => {
                return Err(reschedule_not_found(email_id.as_str()));
            }
            answer => {
                answer
                    .into_result(email_id.as_str(), email_response.answer_context)
                    .map_err(crate::sync::error::answer_error(
                        AccountOperation::RescheduleSend,
                    ))?;
            }
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
        // An unanswered cancel may have landed and nothing follows it, so it
        // is a plain reconcilable PartialResponse leaf with no record.
        cancel_response
            .updated(&submission_id)
            .map_err(crate::sync::error::answer_error(
                AccountOperation::RescheduleSend,
            ))?;

        let mut set = EmailSubmissionSet::new();
        {
            let submit = set.create_with_id(SUBMISSION_CREATE_ID);
            submit.email_id(email_id);
            let mail_from = SubmissionAddress::new(mail_from_email)
                .with_parameter("holduntil", Some(hold_until));
            submit.envelope(mail_from, rcpt_to.into_iter().map(SubmissionAddress::new));
        }
        // The replacement create's application: `from_leaf` (a `notCreated`
        // refusal is `NotApplied`; a transport drop or `serverPartialFail`
        // is already `Uncertain`), raised to `Uncertain` only for an
        // undecodable or missing result, or an evidence-less transport
        // failure, and never over explicit `Unsent` evidence: a refused
        // connection on the create must not be declared uncertain.
        let replacement_error = |error: crate::Error| {
            let raisable = matches!(
                &error,
                crate::Error::EnvelopeDecode(_)
                    | crate::Error::ResultDecode { .. }
                    | crate::Error::IdUnanswered { .. }
                    | crate::Error::CallNotFound(_)
            ) || matches!(&error, crate::Error::Transport(transport) if transport.local.is_none());
            let error = answer_error(AccountOperation::RescheduleSend)(error);
            let unsent = error.chain().iter().any(|cause| {
                matches!(
                    cause,
                    bifrost_types::Cause::Attempt(attempt)
                        if attempt.transmission_state == bifrost_types::TransmissionState::Unsent
                )
            });
            let application = if raisable && !unsent {
                Application::Uncertain
            } else {
                Application::from_leaf(&error)
            };
            let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
                StepEffect::Submitted,
                submission_targets(None),
                StepReplay::Blocking,
                application,
            ));
            record.committed(
                StepEffect::Cancelled,
                submission_targets(Some(submission_id.as_str())),
                StepReplay::Blocking,
            );
            finish_partial(error, record)
        };
        let mut set_response = submission_account
            .call(set)
            .await
            .map_err(replacement_error)?;
        let new_submission = set_response
            .created(SUBMISSION_CREATE_ID)
            .map_err(replacement_error)?;
        let new_id =
            require_created_submission_id(&new_submission, AccountOperation::RescheduleSend)
                .map_err(|error| {
                    let mut record =
                        PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
                    record.committed(
                        StepEffect::Cancelled,
                        submission_targets(Some(submission_id.as_str())),
                        StepReplay::Blocking,
                    );
                    record.committed(
                        StepEffect::Submitted,
                        submission_targets(None),
                        StepReplay::Blocking,
                    );
                    finish_partial(error, record)
                })?;
        Ok(ObjectId(new_id.into_string()))
    })
}

/// Foreign (shared/delegate) submission context selected by the account
/// layer. `mail` already targets the foreign JMAP account.
pub(crate) struct ForeignSubmission {
    pub(crate) mode: bifrost_types::SendAs,
    pub(crate) self_address: Option<bifrost_types::Address>,
}
