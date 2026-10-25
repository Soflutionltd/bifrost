//! Stage 1 PIM actions for the IMAP account.
//!
//! Audit boundary: the 2026-09-04 bug hunt read the driver, wire framing,
//! pool, push/IDLE, auth, change strategies, mutations, inventory,
//! hydration, blob, cursor envelope and folder registry line by line. It did
//! NOT line-audit this module's long tail beyond the container-membership,
//! keyword and search entry points, so an absence of findings here is an
//! absence of reading, not evidence of correctness. Noted so a later auditor
//! knows where coverage stops.

use std::collections::HashMap;
use std::time::SystemTime;

use bifrost_types::cloud::HostedAttachment;
use bifrost_types::compose::{
    Address, AttachmentHandle, DraftHandle, DraftPatch, IdentityId, SendRequest,
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
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFuture, AccountOperation, Cause,
    DiagnosticText, LabelId, Protocol, ProtocolKind, RequestCause, RequestErrorKind,
};
use jiff::Timestamp;
use jiff::tz::Offset;

use crate::types::{
    AclRight, FetchAttr, Flag, MailboxAttribute, MailboxName, MailboxRights, SearchCriteria,
    StatusItem, StoreOperation, ThreadNode,
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

/// `op_err` for a failure in the PREFLIGHT of a per-folder step - the
/// checkout, the SELECT/EXAMINE, the UIDVALIDITY read - restamped `Unsent`
/// relative to the operation, the rule `connection::append`'s
/// `unsent_preflight` states for APPEND. The preflight command carries its own
/// transmission evidence (a tagged `NO` to SELECT is `Acknowledged`), but that
/// is about the SELECT, not about the COPY / STORE / FETCH the operation is
/// made of, which was never sent. Publishing the SELECT's evidence would make
/// recovery reconcile a non-idempotent mutation that never reached the server.
///
/// Scope: this speaks for the folder being prepared. Earlier folders of the
/// same multi-folder call may already have been mutated, in which case
/// `Unsent` understates what the call did; these `Result<(), _>` lanes cannot
/// express partial success at all, a known gap this does not close.
fn preflight_err(op: AccountOperation) -> impl Fn(Error) -> AccountError + Copy {
    move |e| {
        account_error_with(
            e.with_attempt(bifrost_types::TransmissionState::Unsent),
            super::error::ImapErrorContext::operation(op),
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

pub(crate) fn add_to_container(
    account: ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.add_to_container {
            return Err(super::error::unsupported(AccountOperation::AddToContainer));
        }
        let destination = folder_from_container(&container, AccountOperation::AddToContainer)?;
        let ids = decoded_targets(&target, AccountOperation::AddToContainer)?;
        copy_messages(
            &account,
            ids,
            &destination,
            AccountOperation::AddToContainer,
        )
        .await
    })
}

pub(crate) fn remove_from_container(
    account: ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.remove_from_container {
            return Err(super::error::unsupported(
                AccountOperation::RemoveFromContainer,
            ));
        }
        let source = folder_from_container(&container, AccountOperation::RemoveFromContainer)?;
        let ids = decoded_targets(&target, AccountOperation::RemoveFromContainer)?
            .into_iter()
            .filter(|id| id.folder == source)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Err(super::error::unsupported(
                AccountOperation::RemoveFromContainer,
            ));
        }
        delete_messages(&account, ids, AccountOperation::RemoveFromContainer).await
    })
}

pub(crate) fn set_keyword(
    account: ImapAccount,
    target: MutationTarget,
    keyword: String,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.set_keyword {
            return Err(super::error::unsupported(AccountOperation::SetKeyword));
        }
        let flag = imap_flag_for_keyword(&keyword);
        let ids = decoded_targets(&target, AccountOperation::SetKeyword)?;
        set_flag(&account, ids, flag, value, AccountOperation::SetKeyword).await
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
        let ids = decoded_targets(&target, AccountOperation::SetIsRead)?;
        set_flag(
            &account,
            ids,
            Flag::Seen,
            is_read,
            AccountOperation::SetIsRead,
        )
        .await
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
        let ids = decoded_targets(&target, AccountOperation::SetImportance)?;
        set_flag(
            &account,
            ids,
            imap_flag_for_keyword("$important"),
            important,
            AccountOperation::SetImportance,
        )
        .await
    })
}

/// IMAP's two-valued importance mapping: `High` sets the `$important`
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
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let _send_gate = account.send_gate.read().await;
        if account.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(op_err(AccountOperation::Send)(Error::closed()));
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
        let rendered = bifrost_types::send_request_to_rfc5322(&request, submission.default_from())
            .map_err(stamp_imap_protocol)?;
        // Scheduled send rides on SMTP FUTURERELEASE (HOLDUNTIL). The
        // relay's EHLO at send time is authoritative: an unsupporting
        // relay surfaces an `Unsupported(Send)` AccountError from the
        // smtp boundary (kind set there, not here). Re-stamp only the
        // operation/protocol; `restamp` cannot change the kind.
        submission
            .send_rfc5322(&rendered.envelope, &rendered.raw, request.scheduled)
            .await
            .map_err(|err| restamp(err, AccountOperation::Send))?;

        // The message is committed. Optionally append to Sent; a failed
        // APPEND is non-fatal (never resend) but is not swallowed: it
        // surfaces the uncertain Sent state and falls back to the
        // generated Message-ID for the returned id.
        let save = request
            .save_to_sent
            .unwrap_or_else(|| submission.save_to_sent_default());
        // The Sent copy retains the `Bcc:` header (the sender's record of
        // who was blind copied); only the transmitted body strips it. The
        // assembler hands back a distinct Bcc-bearing body when a Bcc is
        // present, else the two are identical and we reuse `raw`.
        let sent_body = rendered.sent_copy.unwrap_or(rendered.raw);
        let object_id =
            append_to_sent_or_fallback(&account, sent_body.into(), save, &rendered.message_id)
                .await;
        Ok(object_id)
    })
}

pub(crate) fn send_raw_message(
    account: ImapAccount,
    raw: bytes::Bytes,
    save_to_sent: Option<bool>,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let _send_gate = account.send_gate.read().await;
        if account.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(op_err(AccountOperation::Send)(Error::closed()));
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
        submission
            .send_rfc5322(&parsed.envelope, &parsed.body, None)
            .await
            .map_err(|err| restamp(err, AccountOperation::Send))?;

        // Committed. Optionally append to Sent. The Sent copy keeps the
        // verbatim caller bytes (Bcc header retained, the sender's record);
        // only `parsed.body` strips Bcc for the wire.
        let save = save_to_sent.unwrap_or_else(|| submission.save_to_sent_default());
        let object_id = append_to_sent_or_fallback(&account, raw, save, &parsed.message_id).await;
        Ok(object_id)
    })
}

pub(crate) fn draft_send(
    account: ImapAccount,
    draft: DraftHandle,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let _send_gate = account.send_gate.read().await;
        if account.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(op_err(AccountOperation::DraftSend)(Error::closed()));
        }
        let Some(submission) = account.submission.clone() else {
            return Err(super::error::unsupported(AccountOperation::DraftSend));
        };

        let decoded = decode_object_id(&ObjectId(draft.0.clone()), AccountOperation::DraftSend)?;
        let raw = fetch_full_message(&account, &decoded).await?;

        // Parse the draft headers to build the envelope: To/Cc/Bcc drive
        // RCPT TO, From/Sender drive MAIL FROM. The Bcc header is folded
        // into recipients and stripped from the transmitted body so blind
        // recipients are delivered but never disclosed.
        let parsed = parse_draft_for_submission(
            &raw,
            submission.default_from(),
            AccountOperation::DraftSend,
        )?;
        submission
            .send_rfc5322(&parsed.envelope, &parsed.body, None)
            .await
            .map_err(|err| restamp(err, AccountOperation::DraftSend))?;

        // Sent; discard the draft from Drafts and optionally append to
        // Sent. The Sent copy is the saved draft `raw`, which retains the
        // `Bcc:` header (the sender's record of who was blind copied) -
        // only `parsed.body` (the transmitted bytes) strips it.
        let object_id = append_to_sent_or_fallback(
            &account,
            raw,
            submission.save_to_sent_default(),
            &parsed.message_id,
        )
        .await;

        // Discard the original draft. A failed discard is non-fatal: the
        // message was sent. Surface it as a warning rather than failing.
        if let Err(err) =
            delete_messages(&account, vec![decoded], AccountOperation::DraftSend).await
        {
            tracing::warn!(
                target: "bifrost_imap::draft_send",
                error = %err,
                "draft sent but discard from Drafts failed; the draft may linger"
            );
        }

        Ok(object_id)
    })
}

/// Append the raw message to the Sent folder when requested and a Sent
/// role folder resolves. Returns the real APPENDUID-derived `ObjectId`
/// when the server surfaces one under UIDPLUS, else the supplied
/// fallback `Message-ID` (controlled domain). A failed APPEND after a
/// committed send is non-fatal: the send already succeeded, so we never
/// re-drive SMTP; the uncertain Sent state is logged for reconcile.
///
/// Takes the message by value: `append_message` carries that allocation to the
/// socket as it is, where `append(&[u8])` would copy the whole message once
/// more just to cross the driver channel.
async fn append_to_sent_or_fallback(
    account: &ImapAccount,
    raw: bytes::Bytes,
    save: bool,
    fallback_message_id: &str,
) -> ObjectId {
    let fallback = || ObjectId(format!("imapmsgid1:{fallback_message_id}"));
    if !save {
        return fallback();
    }
    let Some(sent) = role_folder(account, FolderRole::Sent) else {
        tracing::warn!(
            target: "bifrost_imap::send",
            "save_to_sent requested but no Sent folder resolved; returning generated Message-ID"
        );
        return fallback();
    };
    let conn = match account.pool.checkout_any().await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(
                target: "bifrost_imap::send",
                error = %err,
                "message sent but Sent-folder APPEND could not get a connection; \
                 local Sent view is uncertain (reconcile by checking the Sent folder)"
            );
            return fallback();
        }
    };
    let mut message = crate::types::AppendMessage::new(raw);
    message.flags = vec![Flag::Seen];
    match conn
        .connection()
        .append_message(&sent, message, account.command_timeout())
        .await
    {
        Ok(Some((uidvalidity, uid))) => encode_object_id(&sent, uidvalidity, uid),
        Ok(None) => {
            // Sent succeeded but the server gave no UIDPLUS code.
            fallback()
        }
        Err(err) => {
            tracing::warn!(
                target: "bifrost_imap::send",
                error = %err,
                "message sent but Sent-folder APPEND failed; \
                 local Sent view is uncertain (reconcile by checking the Sent folder)"
            );
            fallback()
        }
    }
}

/// One-shot raw `BODY[]` full-message fetch. Hydration returns a parsed
/// projection, not verbatim octets, so `draft_send` needs this dedicated
/// fetch to recover the exact RFC 5322 bytes of a saved draft.
async fn fetch_full_message(
    account: &ImapAccount,
    decoded: &DecodedObjectId,
) -> Result<bytes::Bytes, AccountError> {
    let err = op_err(AccountOperation::DraftSend);
    let mut conn = account
        .checkout_for_folder(&decoded.folder)
        .await
        .map_err(err)?;
    account
        .select_folder(&mut conn, &decoded.folder, None, true)
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
        .find(|fetch| fetch.uid == Some(decoded.uid))
        .and_then(|fetch| {
            fetch
                .body_sections
                .into_iter()
                .find(|section| section.section.is_empty())
                .and_then(|section| section.data)
        })
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
/// the recipient set; From (or `default_from`) is the reverse path. The
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

    let from_addresses = checked_draft_addresses(&headers, "from", op)?;
    let sender_addresses = checked_draft_addresses(&headers, "sender", op)?;
    let to = checked_draft_addresses(&headers, "to", op)?;
    let cc = checked_draft_addresses(&headers, "cc", op)?;
    let bcc = checked_draft_addresses(&headers, "bcc", op)?;

    let from = from_addresses
        .into_iter()
        .next()
        .or_else(|| sender_addresses.into_iter().next())
        .unwrap_or_else(|| default_from.clone());

    let mut recipients = Vec::new();
    recipients.extend(to);
    recipients.extend(cc);
    recipients.extend(bcc);
    if recipients.is_empty() {
        return Err(pim_malformed(op, "draft has no recipients"));
    }

    let message_id = headers
        .message_ids("message-id")
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            let domain = from
                .address
                .rsplit_once('@')
                .map_or("localhost", |(_, domain)| domain);
            format!("bifrost.draft@{domain}")
        });

    let body = strip_bcc_header(header_block, &raw[body_start..]);

    Ok(ParsedDraft {
        envelope: bifrost_types::SubmissionEnvelope { from, recipients },
        body,
        message_id,
    })
}

fn checked_draft_addresses(
    headers: &bifrost_types::mime::HeaderMap,
    name: &str,
    op: AccountOperation,
) -> Result<Vec<Address>, AccountError> {
    headers.addresses_checked(name).map_err(|rejected| {
        pim_malformed(
            op,
            format!(
                "draft {name} header contains {} unsupported address item(s)",
                rejected.len()
            ),
        )
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
    let uses_crlf = header_block.windows(2).any(|w| w == b"\r\n") || !header_block.contains(&b'\n');
    let sep: &[u8] = if uses_crlf { b"\r\n" } else { b"\n" };

    let mut kept = Vec::with_capacity(header_block.len() + body.len() + 4);
    let mut skipping = false;
    for line in split_inclusive_lf(header_block) {
        let trimmed = trim_leading_crlf(line);
        let is_continuation = line.first() == Some(&b' ') || line.first() == Some(&b'\t');
        if !is_continuation {
            skipping = header_name_is_bcc(trimmed);
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

/// True when a header line's field name (before the first `:`) is `Bcc`,
/// case-insensitive, ignoring surrounding whitespace.
fn header_name_is_bcc(line: &[u8]) -> bool {
    match line.iter().position(|&b| b == b':') {
        Some(idx) => line[..idx].trim_ascii().eq_ignore_ascii_case(b"bcc"),
        None => false,
    }
}

/// Collapse RFC 5322 folded header lines into `(lowercase-name, value)`
/// pairs, preserving order.
/// Re-stamp an `AccountError` from the submission boundary with the IMAP
/// operation that drove it, so recovery/telemetry attribute it correctly.
///
/// Coupling caveat: `into_builder` does NOT preserve builder-only
/// overrides (`idempotency_override`, `throttle_scope`) per
/// `reference/error-model.md`. After the round-trip, `derive` recomputes
/// idempotency from the *operation* via `AccountOperation::is_idempotent`.
/// This is correct today only because both operations `restamp` is called
/// with - `Send` and `DraftSend` - are non-idempotent in the central
/// table, matching what an SMTP `idempotency_override(false)` would have
/// said. If a future caller passes an idempotent operation here, or if
/// SMTP ever stamps an `idempotency_override` that disagrees with the
/// operation's default, this re-stamp would silently drop it. The fix
/// would be to reapply the override after `into_builder` (it is readable
/// from the original error's recovery only indirectly), which requires
/// either an accessor in `bifrost-types` or threading the override
/// through - out of this crate's scope.
fn restamp(err: AccountError, op: AccountOperation) -> AccountError {
    err.into_builder()
        .operation(op)
        .try_build()
        .expect("valid account error classification")
}

/// Attribute a provider-neutral assembler error to IMAP. The shared
/// `bifrost-types` MIME assembler leaves the protocol unset (it is shared
/// across providers); the IMAP send path stamps `Protocol::Imap` so
/// telemetry and recovery attribute the failure to this account.
fn stamp_imap_protocol(err: AccountError) -> AccountError {
    err.into_builder()
        .protocol(Protocol::Imap)
        .try_build()
        .expect("valid account error classification")
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
        delete_messages(&account, vec![id], AccountOperation::DraftDiscard).await
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
        let err = op_err(AccountOperation::Search);
        let plan = search_plan(&request, AccountOperation::Search)?;
        let mut threads = Vec::new();
        for folder in search_folders(&account, plan.folder.as_ref()) {
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, None, true)
                .await
                .map_err(err)?;
            let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
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
            for root in roots {
                let mut uids = Vec::new();
                flatten_thread(&root, &mut uids);
                if !uids.is_empty() {
                    threads.push(encode_thread_id(&folder, uidvalidity, &uids));
                }
            }
        }
        page_from_items(threads, &request, AccountOperation::Search)
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
        let err = op_err(AccountOperation::SearchMessages);
        let plan = search_plan(&request, AccountOperation::SearchMessages)?;
        let mut messages = Vec::new();
        for folder in search_folders(&account, plan.folder.as_ref()) {
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, None, true)
                .await
                .map_err(err)?;
            let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
            let result = conn
                .connection()
                .uid_search(&plan.criteria, account.command_timeout())
                .await
                .map_err(err)?;
            messages.extend(
                result
                    .ids
                    .into_iter()
                    .map(|uid| encode_object_id(&folder, uidvalidity, uid)),
            );
        }
        page_from_items(messages, &request, AccountOperation::SearchMessages)
    })
}

pub(crate) fn containers_list(
    account: ImapAccount,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    // The snapshot already folds in shared/other-user folders discovered
    // at open; per-prefix degradations were handled (and skipped) during
    // that discovery, so the enumeration itself has nothing to skip.
    Box::pin(async move { Ok(ContainerList::complete(containers_snapshot(&account))) })
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
        let err = op_err(AccountOperation::ContainerCreate);
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
        .await?;
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
        let folder = folder_from_container(&container, AccountOperation::ContainerRename)?;
        let new_name =
            renamed_sibling(&account, &folder, &name, AccountOperation::ContainerRename)?;
        let err = op_err(AccountOperation::ContainerRename);
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
    })
}

pub(crate) fn container_move(
    account: ImapAccount,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(&container, AccountOperation::ContainerMove)?;
        let leaf = leaf_name(&account, &folder);
        let new_name = child_name(
            &account,
            new_parent.as_ref(),
            &leaf,
            AccountOperation::ContainerMove,
        )?;
        let err = op_err(AccountOperation::ContainerMove);
        let mut conn = account.pool.checkout_any().await.map_err(err)?;
        conn.deselect_target(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        conn.connection()
            .rename(&folder, &new_name, account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(&account, conn.connection(), AccountOperation::ContainerMove).await
    })
}

pub(crate) fn container_delete(
    account: ImapAccount,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(&container, AccountOperation::ContainerDelete)?;
        let err = op_err(AccountOperation::ContainerDelete);
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
        let decoded = decode_thread_id(&thread, AccountOperation::HydrateThread)?;
        let messages = hydrate_decoded(
            &account,
            decoded
                .uids
                .iter()
                .map(|uid| DecodedObjectId {
                    folder: decoded.folder.clone(),
                    uidvalidity: decoded.uidvalidity,
                    uid: *uid,
                })
                .collect(),
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
        let wanted = encode_object_id(&decoded.folder, decoded.uidvalidity, decoded.uid);
        let messages = hydrate_decoded(
            &account,
            vec![decoded],
            projection,
            AccountOperation::HydrateMessage,
        )
        .await?;
        messages
            .into_iter()
            .find(|hydrated| hydrated.id == wanted)
            .ok_or_else(|| expunged_message(wanted))
    })
}

pub(crate) fn move_thread(
    account: ImapAccount,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        add_to_container(
            account.clone(),
            MutationTarget::Thread(thread.clone()),
            target,
        )
        .await?;
        if let Some(source) = source {
            remove_from_container(account, MutationTarget::Thread(thread), source).await?;
        }
        Ok(())
    })
}

pub(crate) fn delete_thread(
    account: ImapAccount,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if let Some(current) = current {
            let folder = folder_from_container(&current, AccountOperation::BulkMove)?;
            let entry = account.folders.get(&folder);
            let attributes = entry
                .as_deref()
                .map(super::folder_registry::FolderEntry::attributes)
                .unwrap_or_default();
            if folder_role(
                &attributes,
                folder.as_str(),
                entry
                    .as_deref()
                    .and_then(super::folder_registry::FolderEntry::delimiter),
            ) == Some(FolderRole::Trash)
            {
                return remove_from_container(account, MutationTarget::Thread(thread), current)
                    .await;
            }
            let trash = role_folder(&account, FolderRole::Trash)
                .ok_or_else(|| super::error::unsupported(AccountOperation::BulkMove))?;
            return move_thread(
                account,
                thread,
                ContainerId(trash.account_id()),
                Some(current),
            )
            .await;
        }
        let trash = role_folder(&account, FolderRole::Trash)
            .ok_or_else(|| super::error::unsupported(AccountOperation::BulkMove))?;
        move_thread(account, thread, ContainerId(trash.account_id()), None).await
    })
}

async fn copy_messages(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    destination: &MailboxName,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    let preflight = preflight_err(op);
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(preflight)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(preflight)?;
        let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(preflight)?;
        let operand = pim_operand(valid_uids(ids, uidvalidity, op)?, op)?;
        let Some(uid_set) = operand.uid_set() else {
            continue;
        };
        conn.connection()
            .uid_copy(
                uid_set.as_sequence_set(),
                &destination,
                account.command_timeout(),
            )
            .await
            .map_err(err)?;
    }
    Ok(())
}

async fn delete_messages(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    let preflight = preflight_err(op);
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(preflight)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(preflight)?;
        let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(preflight)?;
        let operand = pim_operand(valid_uids(ids, uidvalidity, op)?, op)?;
        let Some(uid_set) = operand.uid_set() else {
            continue;
        };
        conn.connection()
            .uid_store(
                uid_set.as_sequence_set(),
                StoreOperation::AddSilent,
                &[Flag::Deleted],
                None,
                account.command_timeout(),
            )
            .await
            .map_err(err)?;
        super::mutate::expunge_uids_or_fall_back(
            account,
            &conn,
            uid_set.as_sequence_set(),
            operand.uids(),
        )
        .await
        .map_err(err)?;
        account
            .folders
            .clear_modseqs(&folder, uidvalidity, operand.uids());
    }
    Ok(())
}

async fn set_flag(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    flag: Flag,
    value: bool,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    let preflight = preflight_err(op);
    let operation = if value {
        StoreOperation::AddSilent
    } else {
        StoreOperation::RemoveSilent
    };
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(preflight)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(preflight)?;
        let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(preflight)?;
        let operand = pim_operand(valid_uids(ids, uidvalidity, op)?, op)?;
        let Some(uid_set) = operand.uid_set() else {
            continue;
        };
        conn.connection()
            .uid_store(
                uid_set.as_sequence_set(),
                operation,
                std::slice::from_ref(&flag),
                None,
                account.command_timeout(),
            )
            .await
            .map_err(err)?;
        account
            .folders
            .clear_modseqs(&folder, uidvalidity, operand.uids());
    }
    Ok(())
}

async fn hydrate_decoded(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    projection: HydrationProjection,
    op: AccountOperation,
) -> Result<Vec<Message>, AccountError> {
    let err = op_err(op);
    let preflight = preflight_err(op);
    let mut messages = Vec::new();
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(preflight)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, true)
            .await
            .map_err(preflight)?;
        let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(preflight)?;
        let operand = pim_operand(valid_uids(ids, uidvalidity, op)?, op)?;
        let Some(uid_set) = operand.uid_set() else {
            continue;
        };
        let fetches = conn
            .connection()
            .uid_fetch(
                uid_set.as_sequence_set(),
                &attrs_for_hydration(projection),
                account.command_timeout(),
            )
            .await
            .map_err(err)?;
        let merged = merge_requested_fetches(fetches, operand.uids());
        messages.extend(
            merged
                .into_values()
                .filter_map(|fetch| fetch_to_message(&folder, uidvalidity, fetch, projection)),
        );
    }
    messages.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    Ok(messages)
}

/// Fold a UID FETCH's collected responses into one response per REQUESTED
/// UID.
///
/// The connection hands back every FETCH seen during the command, and two
/// kinds are not the answer: an unsolicited FETCH for a UID nobody asked
/// about (another client's flag change), which would otherwise hydrate as a
/// message the caller never requested, and a second response for a requested
/// UID - the server may split data items across responses, or append a
/// FLAGS-only update - which would otherwise hydrate as a duplicate, possibly
/// bodiless, copy. Responses without a UID cannot be attributed and are
/// dropped. The fold is `get`'s `merge_fetch_response`: later data fills gaps
/// and never blanks what an earlier response carried.
fn merge_requested_fetches(
    fetches: Vec<crate::types::FetchResponse>,
    requested: &[u32],
) -> HashMap<u32, crate::types::FetchResponse> {
    let requested: std::collections::HashSet<u32> = requested.iter().copied().collect();
    let mut by_uid: HashMap<u32, crate::types::FetchResponse> = HashMap::new();
    for fetch in fetches {
        let Some(uid) = fetch.uid else {
            continue;
        };
        if !requested.contains(&uid) {
            continue;
        }
        match by_uid.entry(uid) {
            std::collections::hash_map::Entry::Occupied(mut held) => {
                super::get::merge_fetch_response(held.get_mut(), fetch);
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(fetch);
            }
        }
    }
    by_uid
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
                .map(|uid| DecodedObjectId {
                    folder: decoded.folder.clone(),
                    uidvalidity: decoded.uidvalidity,
                    uid,
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
    container: &ContainerId,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    MailboxName::from_account_id(&container.0).map_err(|e| pim_malformed(op, e.to_string()))
}

fn imap_flag_for_keyword(keyword: &str) -> Flag {
    if keyword.eq_ignore_ascii_case("$flagged") || keyword.eq_ignore_ascii_case("\\flagged") {
        Flag::Flagged
    } else if keyword.eq_ignore_ascii_case("$answered")
        || keyword.eq_ignore_ascii_case("\\answered")
    {
        Flag::Answered
    } else if keyword.eq_ignore_ascii_case("$seen") || keyword.eq_ignore_ascii_case("\\seen") {
        Flag::Seen
    } else {
        Flag::from(keyword)
    }
}

#[derive(Debug)]
struct SearchPlan {
    criteria: String,
    folder: Option<MailboxName>,
}

fn search_plan(request: &SearchRequest, op: AccountOperation) -> Result<SearchPlan, AccountError> {
    let mut plan = match &request.filter {
        Some(filter) => criteria_from_filter(filter, op)?,
        None => CriteriaPart {
            criteria: "ALL".to_owned(),
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
            plan.criteria = raw.trim().to_owned();
        } else {
            plan.criteria.push(' ');
            plan.criteria.push_str(raw.trim());
        }
    }
    if plan.criteria.trim().is_empty() {
        plan.criteria = "ALL".to_owned();
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
    criteria: String,
    folder: Option<MailboxName>,
}

fn criteria_from_filter(
    filter: &SearchFilter,
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    match filter {
        SearchFilter::From(value) => leaf(SearchCriteria::new().from(value), op),
        SearchFilter::To(value) => leaf(SearchCriteria::new().to(value), op),
        SearchFilter::Subject(value) => leaf(SearchCriteria::new().subject(value), op),
        SearchFilter::Body(value) | SearchFilter::Has(value) => {
            leaf(SearchCriteria::new().body(value), op)
        }
        SearchFilter::In(container) => Ok(CriteriaPart {
            criteria: "ALL".to_owned(),
            folder: Some(folder_from_container(container, op)?),
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
                criteria: empty_to_all(criteria.as_str()),
                folder: None,
            })
        }
        SearchFilter::And(filters) => combine_and(filters, op),
        SearchFilter::Or(filters) => combine_or(filters, op),
        SearchFilter::Not(filter) => {
            let inner = criteria_from_filter(filter, op)?;
            Ok(CriteriaPart {
                criteria: format!("NOT ({})", inner.criteria),
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
        criteria: empty_to_all(criteria.as_str()),
        folder: None,
    })
}

fn combine_and(
    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    let mut criteria = Vec::new();
    let mut folder = None;
    for filter in filters {
        let part = criteria_from_filter(filter, op)?;
        folder = merge_folder(folder, part.folder, op)?;
        if part.criteria != "ALL" {
            criteria.push(part.criteria);
        }
    }
    Ok(CriteriaPart {
        criteria: if criteria.is_empty() {
            "ALL".to_owned()
        } else {
            criteria.join(" ")
        },
        folder,
    })
}

fn combine_or(
    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    if filters.is_empty() {
        return Ok(CriteriaPart {
            criteria: "ALL".to_owned(),
            folder: None,
        });
    }
    let mut parts = Vec::new();
    for filter in filters {
        let part = criteria_from_filter(filter, op)?;
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
    let mut criteria = iter.next().unwrap_or_else(|| "ALL".to_owned());
    for next in iter {
        criteria = format!("OR ({criteria}) ({next})");
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
        (Some(a), Some(b)) if a != b => {
            use bifrost_types::{
                AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, Protocol,
                RequestCause, RequestErrorKind,
            };
            Err(AccountErrorBuilder::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("folder"),
                    message: Some(DiagnosticText::support_only(format!(
                        "search filter spans two folders ({} and {}); \
                         IMAP SEARCH cannot span multiple mailboxes in one command",
                        a.as_str(),
                        b.as_str()
                    ))),
                }),
            )
            .protocol(Protocol::Imap)
            .operation(op)
            .try_build()
            .expect("valid account error classification"))
        }
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

/// A folder restriction (`SearchFilter::In`) nested inside an `Or` has no
/// faithful single-mailbox IMAP SEARCH encoding (see `combine_or`).
fn or_folder_restriction_error(op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("folder"),
            message: Some(DiagnosticText::support_only(
                "a folder restriction (In) inside an Or cannot be expressed as a \
                 single-mailbox IMAP SEARCH; restructure the query so folder scoping \
                 is not OR-ed with other criteria",
            )),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

fn empty_to_all(criteria: &str) -> String {
    let trimmed = criteria.trim();
    if trimmed.is_empty() {
        "ALL".to_owned()
    } else {
        trimmed.to_owned()
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
    // run-to-run. The multi-folder search cursor is a plain offset into
    // the concatenated per-folder result list, so a nondeterministic
    // folder order would make the same `page_cursor` resolve to a
    // different slice on the next page request (dropped / duplicated
    // results across pages). Sort by display name and wire identity so
    // the concatenation order - and therefore the offset cursor - is stable.
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

fn page_from_items<T: Clone>(
    items: Vec<T>,
    request: &SearchRequest,
    op: AccountOperation,
) -> Result<Page<T>, AccountError> {
    let offset = match &request.page_cursor {
        Some(cursor) => std::str::from_utf8(cursor)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| pim_malformed(op, "invalid IMAP page cursor"))?,
        None => 0,
    };
    let limit = usize::try_from(request.limit.unwrap_or(500)).unwrap_or(usize::MAX);
    let end = offset.saturating_add(limit).min(items.len());
    let page_items = items.get(offset..end).unwrap_or(&[]).to_vec();
    let next_cursor = (end < items.len()).then(|| end.to_string().into_bytes());
    Ok(Page {
        truncated: false,
        items: page_items,
        next_cursor,
        estimated_total: Some(u64::try_from(items.len()).unwrap_or(u64::MAX)),
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
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
        folder_role(&entry.attributes(), entry.name.as_str(), entry.delimiter()),
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
/// - `l`+`r` -> read (`l` is the visibility gate, `r` the access gate)
/// - `i` -> add items (APPEND / COPY into)
/// - `t` -> remove items (set `\Deleted`)
/// - `s` -> persist `\Seen`
/// - `w` -> set other flags/keywords
/// - `k` (or the pre-4314 `c`) -> create a child mailbox
/// - `x` (or the pre-4314 `d`) -> rename / delete the mailbox itself
/// - `p` -> post to the mailbox's submission address
fn rights_from_myrights(rights: &MailboxRights) -> ContainerRights {
    let mailbox_admin =
        rights.contains(AclRight::DeleteMailbox) || rights.contains(AclRight::DeleteLegacy);
    ContainerRights {
        may_read_items: Some(rights.can_read()),
        may_add_items: Some(rights.contains(AclRight::Insert)),
        may_remove_items: Some(rights.contains(AclRight::DeleteMessages)),
        may_set_seen: Some(rights.contains(AclRight::Seen)),
        may_set_keywords: Some(rights.contains(AclRight::Write)),
        may_create_child: Some(
            rights.contains(AclRight::CreateMailbox) || rights.contains(AclRight::CreateLegacy),
        ),
        may_rename: Some(mailbox_admin),
        may_delete: Some(mailbox_admin),
        may_submit: Some(rights.contains(AclRight::Post)),
    }
}

fn folder_role_from_attributes(attributes: &[MailboxAttribute]) -> Option<FolderRole> {
    for attr in attributes {
        match attr {
            MailboxAttribute::Sent => return Some(FolderRole::Sent),
            MailboxAttribute::Drafts => return Some(FolderRole::Drafts),
            MailboxAttribute::Archive => return Some(FolderRole::Archive),
            MailboxAttribute::Trash => return Some(FolderRole::Trash),
            MailboxAttribute::Junk => return Some(FolderRole::Spam),
            MailboxAttribute::Custom(value) if value.eq_ignore_ascii_case("\\Inbox") => {
                return Some(FolderRole::Inbox);
            }
            _ => {}
        }
    }
    None
}

fn folder_role(
    attributes: &[MailboxAttribute],
    name: &str,
    delimiter: Option<char>,
) -> Option<FolderRole> {
    if let Some(role) = folder_role_from_attributes(attributes) {
        return Some(role);
    }
    // SPECIAL-USE is authoritative; this is the name fallback for servers
    // that publish none. The leaf split is shared with the `draft_create`
    // Drafts probe - see `folder_registry::leaf_name` for why it is one
    // function and what a `None` delimiter means.
    let lower = super::folder_registry::leaf_name(name, delimiter).to_ascii_lowercase();
    match lower.as_str() {
        "inbox" => Some(FolderRole::Inbox),
        "sent" | "sent mail" | "sent messages" => Some(FolderRole::Sent),
        "draft" | "drafts" => Some(FolderRole::Drafts),
        "archive" | "archives" => Some(FolderRole::Archive),
        "trash" | "deleted" | "deleted messages" => Some(FolderRole::Trash),
        "junk" | "spam" | "junk mail" => Some(FolderRole::Spam),
        _ => None,
    }
}

fn parent_id(delimiter: Option<char>, native: &str) -> Option<ContainerId> {
    let delimiter = delimiter?;
    let parent = MailboxName::from_account_id(native)
        .ok()?
        .parent(delimiter)
        .ok()??;
    Some(ContainerId(parent.account_id()))
}

fn role_folder(account: &ImapAccount, role: FolderRole) -> Option<MailboxName> {
    role_folder_from_entries(account.folders.entries(), role)
}

fn role_folder_from_entries(
    entries: Vec<std::sync::Arc<super::folder_registry::FolderEntry>>,
    role: FolderRole,
) -> Option<MailboxName> {
    entries
        .into_iter()
        .filter(|entry| {
            folder_role(&entry.attributes(), entry.name.as_str(), entry.delimiter()) == Some(role)
        })
        .min_by(|left, right| {
            let left_rank = u8::from(folder_role_from_attributes(&left.attributes()) != Some(role));
            let right_rank =
                u8::from(folder_role_from_attributes(&right.attributes()) != Some(role));
            // Distinct wire addresses can decode to the same display name.
            // Use the stable mailbox order last so registry iteration cannot
            // decide which address represents the role.
            left_rank
                .cmp(&right_rank)
                .then_with(|| left.name.cmp_account_order(&right.name))
        })
        .map(|entry| entry.name.clone())
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
        .filter(|entry| entry.selectable())
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
    let parent_folder = folder_from_container(parent, op)?;
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
    delimiter
        .and_then(|delimiter| folder.as_str().rsplit_once(delimiter).map(|(_, leaf)| leaf))
        .unwrap_or_else(|| folder.as_str())
        .to_owned()
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

fn attrs_for_hydration(projection: HydrationProjection) -> Vec<FetchAttr> {
    use super::hydration::{BodySelection, FetchSelection};
    let body = match projection {
        HydrationProjection::Headers => BodySelection::None,
        HydrationProjection::Preview(limit) => BodySelection::Preview(limit),
        HydrationProjection::Full | HydrationProjection::FullWithBlobs => BodySelection::Whole,
        // Not dead code: `HydrationProjection` is `#[non_exhaustive]` and that
        // attribute is scoped per crate, so a match in bifrost-imap on an enum
        // defined in bifrost-types needs a wildcard even when the named arms
        // are exhaustive in practice.
        _ => BodySelection::None,
    };
    FetchSelection {
        flags: true,
        envelope: true,
        size: true,
        modseq: false,
        body,
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
    let raw_body = super::hydration::decode_body(&mut fetch);
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
    let flags = super::inventory::flags_set(fetch.flags.as_deref().unwrap_or(&[]));
    let importance = if flags.contains("$important") {
        Importance::High
    } else {
        Importance::Normal
    };
    Some(Message {
        id: encode_object_id(folder, uidvalidity, uid),
        thread_id: fetch
            .thread_id
            .map(ThreadId)
            .or_else(|| fetch.gmail_thread_id.map(|id| ThreadId(id.to_string()))),
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
    Ok(bifrost_types::render_rfc5322(&composed))
}

/// The message a completed `UID FETCH` did not answer for.
///
/// `hydrate_decoded` only reaches the FETCH once `valid_uids` has confirmed the
/// id's UIDVALIDITY matches the selected mailbox, so the UID is a live name in
/// the current epoch and the id was well-formed. A tagged OK with no FETCH for
/// that UID is how RFC 3501 6.4.8 / RFC 9051 6.4.9 report a UID that no longer
/// exists: the message was expunged between the caller learning the id and
/// this FETCH. That is `NotFound(Message)`, not the caller's malformed request.
/// The server answered, so the attempt is `Acknowledged`.
fn expunged_message(id: ObjectId) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message),
        Cause::Request(RequestCause::NotFound {
            what: bifrost_types::ResourceKind::Message,
            id: Some(id.0.clone()),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::HydrateMessage)
    .scope(bifrost_types::ErrorScope::Message { id })
    .push_cause(Cause::Attempt(bifrost_types::AttemptCause::new(
        bifrost_types::TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Build a `Request(Malformed)` `AccountError` for local PIM failures
/// where the caller provided invalid or internally inconsistent state.
///
/// The operation is threaded in from the caller. Helpers reachable from
/// more than one PIM surface (id decode, mailbox-name validation,
/// search-criteria assembly, page-cursor parsing) take the op as a
/// parameter rather than guessing, so a locally-malformed request is
/// attributable to the PIM call that produced it. The recovery class is
/// `ClientBug` for all of them; the operation is the telemetry.
fn pim_malformed(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.into()),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
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
    fn locally_malformed_pim_errors_carry_the_calling_operation() {
        // Id / cursor / mailbox-name decode.
        let err = folder_from_container(
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
            vec![DecodedObjectId {
                folder: MailboxName::new("INBOX").expect("valid mailbox"),
                uidvalidity: 2,
                uid: 5,
            }],
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
            &SearchRequest::provider("(UNSEEN"),
            AccountOperation::Search,
        )
        .expect_err("unbalanced provider query");
        assert_eq!(err.operation(), Some(AccountOperation::Search));

        let err = combine_and(
            &[
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::In(ContainerId("Sent".to_owned())),
            ],
            AccountOperation::Search,
        )
        .expect_err("a two-folder conjunction is refused");
        assert_eq!(err.operation(), Some(AccountOperation::Search));

        let err = combine_or(
            &[
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::From("a@b.test".to_owned()),
            ],
            AccountOperation::Search,
        )
        .expect_err("a folder restriction inside an Or is refused");
        assert_eq!(err.operation(), Some(AccountOperation::Search));

        let err = page_from_items(
            vec![1u32],
            &search_request(None, Some("not-a-number")),
            AccountOperation::Search,
        )
        .expect_err("a page cursor this crate did not mint");
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
        assert_eq!(container.native_id, "Shared/alice/Reports");
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
                    container.native_id.clone()
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

        // The pre-4314 virtual rights map onto the same members.
        let legacy = rights_from_myrights(&MailboxRights::parse("lrcd"));
        assert_eq!(legacy.may_create_child, Some(true));
        assert_eq!(legacy.may_delete, Some(true));
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
        let smtp_derived = bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send),
            bifrost_types::Cause::Request(bifrost_types::RequestCause::Unsupported {
                operation: AccountOperation::Send,
            }),
        )
        .protocol(Protocol::Smtp)
        .operation(AccountOperation::Send)
        .try_build()
        .expect("valid account error classification");

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
        let err = match criteria_from_filter(&filter, AccountOperation::SearchMessages) {
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
        let part = match criteria_from_filter(&filter, AccountOperation::SearchMessages) {
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
                &[MailboxAttribute::Custom("\\INBOX".to_owned())],
                "Posteingang",
                None,
            ),
            Some(FolderRole::Inbox),
            "the \\Inbox custom attribute is matched case-insensitively",
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
            Some(FolderRole::Sent),
            "unknown delimiter preserves the slash fallback",
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
            Flag::Custom("$MDNSent".to_owned())
        );
        assert_eq!(
            imap_flag_for_keyword("$important"),
            Flag::Custom("$important".to_owned())
        );
    }

    fn search_request(limit: Option<u32>, cursor: Option<&str>) -> SearchRequest {
        let mut request = SearchRequest::default();
        request.limit = limit;
        request.page_cursor = cursor.map(|c| c.as_bytes().to_vec());
        request
    }

    #[test]
    fn page_from_items_walks_an_offset_cursor_to_exhaustion() {
        let items: Vec<u32> = (0..5).collect();

        let first = page_from_items(
            items.clone(),
            &search_request(Some(2), None),
            AccountOperation::SearchMessages,
        )
        .expect("page");
        assert_eq!(first.items, vec![0, 1]);
        assert_eq!(first.next_cursor.as_deref(), Some(b"2".as_slice()));
        assert_eq!(first.estimated_total, Some(5));

        let last = page_from_items(
            items.clone(),
            &search_request(Some(2), Some("4")),
            AccountOperation::SearchMessages,
        )
        .expect("page");
        assert_eq!(last.items, vec![4]);
        assert_eq!(last.next_cursor, None, "the final page ends the walk");

        // A cursor at or past the end yields an empty final page rather
        // than an error or a panic on the slice.
        let past = page_from_items(
            items,
            &search_request(Some(2), Some("99")),
            AccountOperation::SearchMessages,
        )
        .expect("page");
        assert!(past.items.is_empty());
        assert_eq!(past.next_cursor, None);
    }

    #[test]
    fn page_from_items_rejects_a_cursor_it_did_not_mint() {
        let err = page_from_items(
            vec![1u32],
            &search_request(None, Some("not-a-number")),
            AccountOperation::SearchMessages,
        )
        .expect_err("garbage cursor");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
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
        let plan =
            search_plan(&SearchRequest::default(), AccountOperation::SearchMessages).expect("plan");
        assert_eq!(plan.criteria, "ALL");
        assert!(plan.folder.is_none());

        let plan = search_plan(
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
            &SearchRequest::provider("  UNSEEN  "),
            AccountOperation::SearchMessages,
        )
        .expect("plan");
        assert_eq!(plan.criteria, "UNSEEN", "the raw query is trimmed");

        // With a structured filter the two are juxtaposed, which is
        // IMAP's implicit AND (RFC 3501 Section 6.4.4).
        let mut request = SearchRequest::filter(SearchFilter::From("a@b.test".to_owned()));
        request.provider_query = Some("UNSEEN".to_owned());
        let plan = search_plan(&request, AccountOperation::SearchMessages).expect("plan");
        assert_eq!(plan.criteria, "FROM \"a@b.test\" UNSEEN");

        // A whitespace-only raw query is ignored entirely.
        let plan = search_plan(
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
            &SearchRequest::provider("OR (FROM \"a\\\\\\\"b\") (SUBJECT \"{4}\")"),
            AccountOperation::SearchMessages,
        )
        .expect("balanced query");
        assert_eq!(plan.criteria, "OR (FROM \"a\\\\\\\"b\") (SUBJECT \"{4}\")");
        let plan = search_plan(
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
            combine_or(&[], AccountOperation::SearchMessages)
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
        assert!(header_name_is_bcc(b"Bcc: x@y"));
        assert!(header_name_is_bcc(b"BCC:x@y"));
        assert!(header_name_is_bcc(b" bcc : x@y"));
        assert!(!header_name_is_bcc(b"Bcc-Original: x@y"));
        assert!(!header_name_is_bcc(b"X-Original-Bcc: x@y"));
        assert!(!header_name_is_bcc(b"Bcc x@y"), "no colon, no header");
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
            flags: Some(vec![Flag::Custom("$Important".to_owned())]),
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
        let fetch = crate::types::FetchResponse { uid: Some(2), body_sections: vec![crate::types::fetch::BodySection { section: String::new(), origin: None, data: Some(b"Content-Type: multipart/alternative; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\npreview prose\r\n--x--\r\n".to_vec().into()) }], ..Default::default() };
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
