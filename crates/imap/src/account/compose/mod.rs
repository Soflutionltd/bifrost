//! SMTP submission, saved drafts and Sent-copy policy.

use super::error::{folder_err, op_err, pim_malformed, restamp, stamp_imap_protocol};
use super::mail_actions::{delete_messages, folder_progress_error};
use super::message_targets::pim_operand;
use super::{DecodedObjectId, ImapAccount, decode_object_id, encode_object_id};
use crate::types::Flag;
use bifrost_types::compose::{
    Address, DraftDiscardFailure, DraftHandle, DraftPatch, RecipientNotAccepted, SendReceipt,
    SendRequest, SentCopy, SentCopyNotFiled,
};
use bifrost_types::container::FolderRole;
use bifrost_types::ids::ObjectId;
use bifrost_types::{AccountError, AccountFuture, AccountOperation, Application, DiagnosticText};
use headers::{parse_draft_for_submission, restamp_draft_date};
mod headers;

/// Memory budget for the one-shot full-message `BODY[]` fetch in
/// `draft_send`. A saved draft is the account's own outgoing message, so
/// this need only bound a single message body, not a folder sweep. 64 MiB
/// comfortably covers any realistic draft (large inline attachments
/// included) while keeping the `FetchLimit` guard engaged, so a corrupt or
/// adversarial server cannot make us buffer an unbounded literal.
const DRAFT_FETCH_BUDGET: usize = 64 * 1024 * 1024;

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

pub(crate) fn draft_create(
    account: ImapAccount,
    patch: DraftPatch,
) -> AccountFuture<Result<DraftHandle, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.draft_create {
            return Err(super::error::unsupported(AccountOperation::DraftCreate));
        }
        let folder = account
            .folders
            .role_folder(FolderRole::Drafts)
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
        let Some(crate::types::AppendUid {
            validity: uidvalidity,
            uid,
        }) = appended
        else {
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
    let Some(sent) = account.folders.role_folder(FolderRole::Sent) else {
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
        Ok(Some(crate::types::AppendUid {
            validity: uidvalidity,
            uid,
        })) => {
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
    let operand = pim_operand([decoded.uid.get()], AccountOperation::DraftSend)?;
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
        .filter(|fetch| fetch.uid == Some(decoded.uid.get()))
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
mod tests;
