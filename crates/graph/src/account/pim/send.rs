//! Send and scheduled send: the submit doors, send-as stamping, and
//! the deferred-send-time property with its opaque handle codec.

use crate::account::GraphAccount;
use crate::account::GraphClient;
use crate::account::graph_error::{
    GraphErrorContext, into_account_error, invalid_argument_account_error,
    unsupported_account_error,
};
use crate::client::Resource;
use base64::Engine;
use bifrost_types::{AccountError, AccountOperation, DraftHandle, MailboxId, ObjectId, SendAs};
use bifrost_types::{
    Application, ErrorScope, PartialCommitBuilder, PrimaryLeg, StepEffect, StepReplay, StepTargets,
    TargetRef, TargetRole,
};
use serde_json::{Value, json};

use super::common::*;
use super::drafts::*;

pub(crate) async fn send_message(
    account: GraphAccount,
    request: bifrost_types::SendRequest,
) -> Result<ObjectId, AccountError> {
    if !request.attachments_uploaded.is_empty() {
        return Err(unsupported_account_error(AccountOperation::Send));
    }
    if let Some(at) = request.scheduled {
        // Graph has no documented hard cap on deferred-send time;
        // rely on server rejection for an unreasonable window. Only
        // the past-instant rule is enforced client-side.
        bifrost_types::validate_scheduled(at, None)?;
    }
    let scheduled = request.scheduled;
    // Resolve the API-path client up front: a personal send uses the
    // primary `/me` client; a send-as routes the entire
    // draft-create-stamp-send cycle through the shared mailbox's
    // `/users/{id}` client so the draft is owned by the shared mailbox.
    let client = match &request.send_as {
        None => &account.client,
        // The two answers mean different things and the capability flag picks
        // between them (`SendRequest::send_as` is the governing statement).
        // An EMPTY map is `pim_methods.send_as == false`: the feature is
        // absent on this account, so the honest answer is `Unsupported`, and
        // telling the caller to correct an argument would send them to fix a
        // request that no argument could have satisfied. A non-empty map is
        // the flag being true, so an id that is not in it is a bad argument.
        Some(_) if account.shared_clients.is_empty() => {
            return Err(crate::account::graph_error::unsupported_account_error(
                AccountOperation::Send,
            ));
        }
        Some(send_as) => account
            .shared_clients
            .get(&send_as.mailbox().0)
            .ok_or_else(|| send_as_unknown_mailbox(send_as.mailbox()))?,
    };
    let mut message = message_from_send_request(&request)?;
    if let Some(send_as) = &request.send_as {
        apply_send_as(&mut message, send_as, account.user_email.as_deref());
    }
    // The draft is the send's first leg, so its failure is stamped `Send`.
    let created = post_draft_message(client, &message, AccountOperation::Send).await?;
    let draft = send_created_draft(&created, "Graph draft create did not return an id")?;
    if let Some(at) = scheduled {
        // Stamp PidTagDeferredSendTime on the draft before send so
        // Graph queues it for deferred delivery.
        stamp_deferred_send_time(client, &draft, at, AccountOperation::Send)
            .await
            .map_err(|e| send_progress_error(e, &draft, false, false))?;
    }
    send_draft_message(client, &draft)
        .await
        .map_err(|e| send_progress_error(e, &draft, scheduled.is_some(), true))?;
    // For a scheduled send-as, the draft lives in the shared mailbox, not
    // `/me`; the cancel/reschedule handle must carry the owning mailbox so
    // those ops route to the same `/users/{id}` client the draft was
    // created on (a bare draft id would 404 against `/me`). A non-scheduled
    // send has nothing to cancel, so the discriminator is moot there.
    let owning_mailbox = scheduled
        .and(request.send_as.as_ref())
        .map(|send_as| send_as.mailbox().0.clone());
    Ok(ObjectId(encode_scheduled_send_handle(
        owning_mailbox.as_deref(),
        &draft.0,
    )))
}

pub(crate) async fn send_raw_message(
    account: GraphAccount,
    raw: bytes::Bytes,
    _save_to_sent: Option<bool>,
) -> Result<ObjectId, AccountError> {
    // Graph has no raw-MIME sendMail. It imports raw MIME by POSTing the
    // base64 of the octets to `/messages` with `Content-Type: text/plain`,
    // which creates a draft; we then send that draft. Graph always files
    // the sent copy in Sent, so `save_to_sent` has no Graph-side toggle.
    let client = &account.client;
    let base64_mime = base64::engine::general_purpose::STANDARD.encode(&raw);
    let path = format!("{}/messages", client.api_path_prefix());
    let created: Value = client
        .post_mime(
            Resource::Outlook,
            &path,
            bytes::Bytes::from(base64_mime),
            bifrost_net::ReplayPolicy::Never,
        )
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::Send)))?;
    let draft = send_created_draft(&created, "Graph MIME import did not return an id")?;
    send_draft_message(client, &draft)
        .await
        .map_err(|e| send_progress_error(e, &draft, false, true))?;
    Ok(ObjectId(draft.0))
}

/// Reserved separator for the scheduled-send cancel/reschedule handle.
/// A handle of the form `"<mailbox>\u{1f}<draft id>"` is a send-as
/// scheduled draft owned by a shared mailbox; a plain id is a primary
/// (`/me`) draft. `\u{1f}` (US) cannot appear in a Graph message id or an
/// SMTP address / user id, so it is an unambiguous delimiter (mirrors the
/// foreign-folder codec in `foreign.rs`).
pub(super) const SCHEDULED_SEND_HANDLE_SEP: char = '\u{1f}';

/// Encode the owning-mailbox discriminator into a scheduled-send handle.
/// `None` mailbox yields a bare draft id (primary `/me` draft).
pub(super) fn encode_scheduled_send_handle(mailbox: Option<&str>, draft_id: &str) -> String {
    match mailbox {
        Some(mailbox) => format!("{mailbox}{SCHEDULED_SEND_HANDLE_SEP}{draft_id}"),
        None => draft_id.to_string(),
    }
}

/// Split a scheduled-send handle back into `(owning mailbox, draft id)`.
/// A handle with no separator is a primary draft (`None` mailbox).
pub(super) fn decode_scheduled_send_handle(handle: &str) -> (Option<&str>, &str) {
    match handle.split_once(SCHEDULED_SEND_HANDLE_SEP) {
        Some((mailbox, draft_id)) => (Some(mailbox), draft_id),
        None => (None, handle),
    }
}

/// Resolve the `GraphClient` a scheduled-send handle was created on: the
/// shared-mailbox client when the handle carries an owning mailbox, else
/// the primary client.
pub(super) fn client_for_scheduled_send_handle<'a>(
    account: &'a GraphAccount,
    mailbox: Option<&str>,
) -> Result<&'a GraphClient, AccountError> {
    match mailbox {
        None => Ok(&account.client),
        Some(mailbox) => account
            .shared_clients
            .get(mailbox)
            .ok_or_else(|| send_as_unknown_mailbox(&MailboxId(mailbox.to_string()))),
    }
}

/// Stamp Graph's `from` / `sender` fields from a `SendAs` directive.
/// `from` is the author header; `sender` is the on-behalf-of
/// discriminator. The two arms differ deliberately on `from`:
/// `As` overrides any consumer-set `from` (`insert`), because its
/// contract is author == sender == mailbox; `OnBehalfOf` honors a
/// consumer-set `from` (`entry(..).or_insert_with`), filling it only
/// when absent, because an explicit author is a legitimate divergence
/// there.
pub(super) fn apply_send_as(message: &mut Value, send_as: &SendAs, user_email: Option<&str>) {
    let mailbox = send_as.mailbox();
    let mailbox_recipient = json!({ "emailAddress": { "address": mailbox.0 } });
    // `message_from_draft_patch` always returns `Value::Object`.
    let obj = message.as_object_mut().expect("message is an object");
    match send_as {
        SendAs::As(_) => {
            obj.insert("from".to_string(), mailbox_recipient.clone());
            obj.insert("sender".to_string(), mailbox_recipient);
        }
        SendAs::OnBehalfOf(_) => {
            obj.entry("from")
                .or_insert_with(|| mailbox_recipient.clone());
            // When the config carries the authenticated user's own
            // address, stamp it as `sender`; otherwise omit `sender`
            // and let Graph populate it from the authenticated context.
            if let Some(me) = user_email {
                obj.insert(
                    "sender".to_string(),
                    json!({ "emailAddress": { "address": me } }),
                );
            }
        }
        // `SendAs` is `#[non_exhaustive]`; a future mode falls back to
        // the most conservative stamping (author == sender == mailbox),
        // so a new variant can never leak the authenticated user's own
        // mailbox as the visible sender.
        _ => {
            obj.insert("from".to_string(), mailbox_recipient.clone());
            obj.insert("sender".to_string(), mailbox_recipient);
        }
    }
}

/// A `send_as` request targeting a mailbox not registered on this
/// account (`shared_clients` is seeded at construction, and the account
/// advertises `pim_methods.send_as` only when it is non-empty). The
/// provider supports send-as; this specific mailbox is just not
/// configured, so it is a caller error (`Request(Malformed)`), not
/// `Unsupported`.
///
/// `InvalidArgument` rather than the plain `Malformed` detail, and with
/// the same `send_as.mailbox` field pointer JMAP uses on this rejection:
/// the two backends that advertise the flag are the two a consumer can
/// get this answer from, and a UI that highlights the offending input
/// should not have to know which one it is talking to.
#[must_use]
pub(super) fn send_as_unknown_mailbox(mailbox: &MailboxId) -> AccountError {
    invalid_argument_account_error(
        AccountOperation::Send,
        "send_as.mailbox",
        format!(
            "shared mailbox not configured on this account: {}",
            mailbox.0
        ),
    )
}

/// MAPI proptag form for `PidTagDeferredSendTime` (`PT_SYSTIME 0x3FEF`),
/// the single-valued extended property Graph reads to defer a send.
pub(super) const DEFERRED_SEND_TIME_PROPERTY_ID: &str = "SystemTime 0x3FEF";

/// PATCH a draft's `PidTagDeferredSendTime` extended property to `at`,
/// serialized as ISO-8601 UTC. Used both by the scheduled send path and
/// by `reschedule_send` (Graph reschedule is an in-place PATCH).
pub(super) async fn stamp_deferred_send_time(
    client: &GraphClient,
    draft: &DraftHandle,
    at: std::time::SystemTime,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/messages/{}",
        client.api_path_prefix(),
        bifrost_net::url::encode_path_component(&draft.0)
    );
    let body = deferred_send_time_body(at);
    client
        .patch(
            Resource::Outlook,
            &path,
            &body,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))
}

/// The `MessagePatch` body that stamps `PidTagDeferredSendTime` on a
/// Graph draft.
pub(super) fn deferred_send_time_body(at: std::time::SystemTime) -> Value {
    json!({
        "singleValueExtendedProperties": [{
            "id": DEFERRED_SEND_TIME_PROPERTY_ID,
            "value": graph_iso8601_utc(at)
        }]
    })
}

pub(crate) async fn cancel_scheduled_send(
    account: GraphAccount,
    handle: ObjectId,
) -> Result<(), AccountError> {
    // A Graph deferred message sits in the mailbox until its send time;
    // deleting it cancels the send. The handle carries the owning mailbox
    // for a send-as draft so the DELETE routes to the same client the
    // draft was created on (else a shared-mailbox draft 404s against /me).
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle.0);
    let client = client_for_scheduled_send_handle(&account, mailbox)?;
    let path = format!(
        "{}/messages/{}",
        client.api_path_prefix(),
        bifrost_net::url::encode_path_component(draft_id)
    );
    client
        .delete(Resource::Outlook, &path, bifrost_net::ReplayPolicy::Never)
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::CancelScheduledSend),
            )
        })
}

pub(crate) async fn reschedule_send(
    account: GraphAccount,
    handle: ObjectId,
    scheduled: std::time::SystemTime,
) -> Result<ObjectId, AccountError> {
    bifrost_types::validate_scheduled(scheduled, None)?;
    // Graph reschedule is an in-place PATCH of the deferred-send time.
    // Route through the client the draft was created on (the handle's
    // owning-mailbox discriminator) and patch the native draft id.
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle.0);
    let client = client_for_scheduled_send_handle(&account, mailbox)?;
    let draft = DraftHandle(draft_id.to_string());
    stamp_deferred_send_time(client, &draft, scheduled, AccountOperation::RescheduleSend).await?;
    Ok(handle)
}

fn send_created_draft(created: &Value, detail: &'static str) -> Result<DraftHandle, AccountError> {
    if let Some(id) = created
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    {
        return Ok(DraftHandle(id.to_owned()));
    }
    let error = pim_protocol_error(AccountOperation::Send, None, detail);
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    Err(finish_partial(error, record))
}
fn send_progress_error(
    error: AccountError,
    draft: &DraftHandle,
    stamped: bool,
    submission: bool,
) -> AccountError {
    let targets = StepTargets::complete(vec![TargetRef::new(
        TargetRole::Subject,
        ErrorScope::Message {
            id: ObjectId(draft.0.clone()),
        },
    )]);
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        if submission {
            StepEffect::Submitted
        } else {
            StepEffect::Updated
        },
        targets.clone(),
        if submission {
            StepReplay::Blocking
        } else {
            StepReplay::Repeatable
        },
        Application::from_leaf(&error),
    ));
    record.committed(StepEffect::Created, targets.clone(), StepReplay::Repeatable);
    if stamped {
        record.committed(StepEffect::Updated, targets, StepReplay::Repeatable);
    }
    finish_partial(error, record)
}

#[cfg(test)]
mod partial_commit_tests {
    use super::*;
    #[test]
    fn partial_commit_send_refusal_reports_the_orphan_draft() {
        let leaf =
            AccountError::unsupported(AccountOperation::Send, bifrost_types::Protocol::Graph);
        let error = send_progress_error(leaf.clone(), &DraftHandle("draft".into()), false, true);
        assert_eq!(error.recovery(), leaf.recovery());
        assert_eq!(
            error.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::Created)
        );
    }
    #[test]
    fn partial_commit_send_drop_reconciles() {
        let leaf = into_account_error(
            crate::error::GraphError::Net(bifrost_net::Error::Network {
                message: "lost".into(),
                transmission_state: bifrost_types::TransmissionState::InFlight,
                source: None,
            }),
            GraphErrorContext::graph(AccountOperation::Send),
        );
        let error = send_progress_error(leaf, &DraftHandle("draft".into()), false, true);
        assert_eq!(
            error.partial_commit().unwrap().plan().replay,
            bifrost_types::ReplaySafety::AfterObservation
        );
        assert!(
            matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget,bifrost_types::ReconcileAction::DedupeByClientId])
        );
    }
    #[test]
    fn partial_commit_draft_create_without_id_in_send_names_the_orphan() {
        let error = send_created_draft(&json!({}), "missing id").unwrap_err();
        assert_eq!(error.operation(), Some(AccountOperation::Send));
        let record = error.partial_commit().unwrap();
        assert!(!record.steps()[0].targets().complete);
        assert_eq!(record.primary().effect, None);
    }
}
