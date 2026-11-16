//! Gmail compose operations.

use super::super::blobs::{content_disposition_inline, content_id_value};
use super::super::error;
use super::{account_error_for, unsupported};
use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::headers::references_from_header;
use crate::types::{GmailHeader, GmailMessage, GmailPayload};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bifrost_types::{
    AccountError, AccountFuture, AccountStream, Address, AttachmentHandle, AttachmentInline,
    DraftHandle, DraftPatch, IdentityId, ObjectId, SendRequest,
};
use bytes::Bytes;
use std::sync::Arc;

use super::hydrate::{body_parts, header_from, message_headers};

pub(crate) fn send_message(
    client: Arc<GmailClient>,
    default_address: String,
    request: SendRequest,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        if !request.attachments_uploaded.is_empty() {
            return Err(unsupported(
                bifrost_types::AccountOperation::AttachmentUpload,
            ));
        }
        if let Some(err) = scheduled_send_guard(&request) {
            return Err(err);
        }
        if let Some(err) = send_as_guard(&request) {
            return Err(err);
        }
        let doc = MailDocument::from_send(request, &default_address);
        let raw = render_message(&doc, &default_address, true)?;
        let message = client
            .send_message(&raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::send()))?;
        Ok(ObjectId(message.id))
    })
}

pub(crate) fn send_raw_message(
    client: Arc<GmailClient>,
    raw: Bytes,
    _save_to_sent: Option<bool>,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        // Gmail `messages.send` takes the verbatim RFC 5322 octets as a
        // base64url (no-pad) string and learns recipients + Bcc from the
        // message headers. It always files the result in Sent, so
        // `save_to_sent` has no Gmail-side toggle (matches send_message).
        let headers = bifrost_types::mime::parse_message(&raw).headers;
        validate_header_recipients(&headers, bifrost_types::AccountOperation::Send)?;
        let encoded = URL_SAFE_NO_PAD.encode(&raw);
        let message = client
            .send_message(&encoded, None)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::send()))?;
        Ok(ObjectId(message.id))
    })
}

pub(crate) fn attachment_upload(
    _bytes: AccountStream<Result<Bytes, AccountError>>,
    _mime: String,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    Box::pin(async {
        Err(unsupported(
            bifrost_types::AccountOperation::AttachmentUpload,
        ))
    })
}

pub(crate) fn draft_create(
    client: Arc<GmailClient>,
    default_address: String,
    patch: DraftPatch,
) -> AccountFuture<Result<DraftHandle, AccountError>> {
    Box::pin(async move {
        let doc = MailDocument::from_draft_patch(patch, &default_address);
        let raw = render_message(&doc, &default_address, false)?;
        let draft = client
            .create_draft(&raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| {
                account_error_for(
                    e,
                    error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftCreate),
                )
            })?;
        Ok(DraftHandle(draft.id))
    })
}

pub(crate) fn draft_update(
    client: Arc<GmailClient>,
    default_address: String,
    draft: DraftHandle,
    patch: DraftPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ctx = error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftUpdate);
        let existing = client
            .get_draft(&draft.0, "full")
            .await
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        let mut doc = document_from_message(&client, &existing.message, true).await?;
        doc.apply_patch(patch);
        let raw = render_message(&doc, &default_address, false)?;
        client
            .update_draft(&draft.0, &raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| account_error_for(e, ctx))?;
        Ok(())
    })
}

pub(crate) fn draft_discard(
    client: Arc<GmailClient>,
    draft: DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        client.delete_draft(&draft.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftDiscard),
            )
        })
    })
}

pub(crate) fn draft_send(
    client: Arc<GmailClient>,
    draft: DraftHandle,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let operation = bifrost_types::AccountOperation::DraftSend;
        let existing = client
            .get_draft(&draft.0, "full")
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::draft(operation)))?;
        let headers = message_headers(&existing.message);
        let to = checked_address_list(headers, "To", operation)?;
        let cc = checked_address_list(headers, "Cc", operation)?;
        let bcc = checked_address_list(headers, "Bcc", operation)?;
        bifrost_types::compose::validate_send_recipients(
            &to,
            &cc,
            &bcc,
            bifrost_types::Protocol::Gmail,
        )?;
        // Validate the stored recipient snapshot without rewriting the draft.
        // Users expect empty or unrepresentable recipients to fail before send.
        // Gmail has no conditional drafts.send: a concurrent editor can still
        // replace the draft between this read and submission.
        let message = client.send_draft(&draft.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftSend),
            )
        })?;
        Ok(ObjectId(message.id))
    })
}

async fn document_from_message(
    client: &GmailClient,
    message: &GmailMessage,
    include_attachments: bool,
) -> Result<MailDocument, AccountError> {
    let headers = message_headers(message);
    let from = checked_draft_address_list(headers, "From")?
        .into_iter()
        .next();
    let to = checked_draft_address_list(headers, "To")?;
    let cc = checked_draft_address_list(headers, "Cc")?;
    let bcc = checked_draft_address_list(headers, "Bcc")?;
    let reply_to = checked_draft_address_list(headers, "Reply-To")?;
    let (body_text, body_html) = body_parts(message.payload.as_ref());
    let attachments_inline = if include_attachments {
        attachment_inlines(client, message).await?
    } else {
        Vec::new()
    };
    Ok(MailDocument {
        identity: None,
        from,
        to,
        cc,
        bcc,
        reply_to,
        subject: header_from(headers, "Subject"),
        body_text,
        body_html,
        attachments_inline,
        attachments_uploaded: Vec::new(),
        in_reply_to: header_from(headers, "In-Reply-To"),
        references: references_from_header(header_from(headers, "References").as_deref()),
        thread_id: Some(message.thread_id.clone()),
        request_read_receipt: false,
    })
}

fn checked_draft_address_list(
    headers: &[GmailHeader],
    name: &str,
) -> Result<Vec<Address>, AccountError> {
    checked_address_list(headers, name, bifrost_types::AccountOperation::DraftUpdate)
}

fn checked_address_list(
    headers: &[GmailHeader],
    name: &str,
    operation: bifrost_types::AccountOperation,
) -> Result<Vec<Address>, AccountError> {
    let mut addresses = Vec::new();
    for header in headers
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case(name))
    {
        let parsed = Address::parse_list_checked(&header.value).map_err(|rejected| {
            account_error_for(
                crate::Error::invalid_request(
                    operation,
                    format!(
                        "draft {name} header contains {} unsupported address item(s)",
                        rejected.len()
                    ),
                ),
                error::GmailErrorContext::draft(operation),
            )
        })?;
        addresses.extend(parsed);
    }
    Ok(addresses)
}

// Gmail derives delivery recipients from these headers, including Bcc.
// Reject lossy parsing rather than silently submitting to only some recipients.
fn validate_header_recipients(
    headers: &bifrost_types::mime::HeaderMap,
    operation: bifrost_types::AccountOperation,
) -> Result<(), AccountError> {
    let read = |name: &str| {
        headers.addresses_checked(name).map_err(|_| {
            account_error_for(
                crate::Error::invalid_request(operation, "unsupported recipient address"),
                error::GmailErrorContext::base(operation),
            )
        })
    };
    bifrost_types::compose::validate_send_recipients(
        &read("To")?,
        &read("Cc")?,
        &read("Bcc")?,
        bifrost_types::Protocol::Gmail,
    )
}

async fn attachment_inlines(
    client: &GmailClient,
    message: &GmailMessage,
) -> Result<Vec<AttachmentInline>, AccountError> {
    let mut refs = Vec::new();
    if let Some(payload) = &message.payload {
        collect_attachment_refs(payload, &mut refs);
    }
    let mut attachments = Vec::with_capacity(refs.len());
    // attachment_inlines is used during draft hydration (draft_update);
    // the error context is DraftUpdate.
    let ctx = error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftUpdate);
    for item in refs {
        let attachment = client
            .get_attachment(&message.id, &item.attachment_id)
            .await
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        let data = decode_base64url_nopad(&attachment.data)
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        attachments.push(AttachmentInline {
            filename: item.filename,
            mime: item.mime,
            data: Bytes::from(data),
            inline: item.inline,
            content_id: item.content_id,
        });
    }
    Ok(attachments)
}

struct AttachmentRef {
    attachment_id: String,
    filename: String,
    mime: String,
    inline: bool,
    content_id: Option<String>,
}

fn collect_attachment_refs(part: &GmailPayload, refs: &mut Vec<AttachmentRef>) {
    if let Some(body) = &part.body
        && let Some(attachment_id) = &body.attachment_id
    {
        refs.push(AttachmentRef {
            attachment_id: attachment_id.clone(),
            filename: part.filename.clone(),
            mime: part.mime_type.clone(),
            inline: content_disposition_inline(&part.headers),
            content_id: content_id_value(&part.headers),
        });
    }
    for child in &part.parts {
        collect_attachment_refs(child, refs);
    }
}

#[derive(Debug, Clone, Default)]
struct MailDocument {
    identity: Option<IdentityId>,
    from: Option<Address>,
    to: Vec<Address>,
    cc: Vec<Address>,
    bcc: Vec<Address>,
    reply_to: Vec<Address>,
    subject: Option<String>,
    body_text: Option<String>,
    body_html: Option<String>,
    attachments_inline: Vec<AttachmentInline>,
    attachments_uploaded: Vec<AttachmentHandle>,
    in_reply_to: Option<String>,
    references: Vec<String>,
    thread_id: Option<String>,
    /// RFC 8098 read-receipt request, carried from `SendRequest`. Drafts
    /// hydrated from an existing message default to `false`.
    request_read_receipt: bool,
}

impl MailDocument {
    fn from_send(request: SendRequest, default_address: &str) -> Self {
        let mut doc = Self {
            identity: request.identity,
            from: request.from,
            to: request.to,
            cc: request.cc,
            bcc: request.bcc,
            reply_to: request.reply_to,
            subject: request.subject,
            body_text: request.body_text,
            body_html: request.body_html,
            attachments_inline: request.attachments_inline,
            attachments_uploaded: request.attachments_uploaded,
            in_reply_to: request.in_reply_to,
            references: request.references,
            thread_id: None,
            request_read_receipt: request.request_read_receipt,
        };
        doc.ensure_from(default_address);
        doc
    }

    fn from_draft_patch(patch: DraftPatch, default_address: &str) -> Self {
        let mut doc = Self {
            identity: patch.identity,
            from: patch.from.flatten(),
            to: patch.to.unwrap_or_default(),
            cc: patch.cc.unwrap_or_default(),
            bcc: patch.bcc.unwrap_or_default(),
            reply_to: patch.reply_to.unwrap_or_default(),
            subject: patch.subject.flatten(),
            body_text: patch.body_text.flatten(),
            body_html: patch.body_html.flatten(),
            attachments_inline: patch.attachments_inline.unwrap_or_default(),
            attachments_uploaded: patch.attachments_uploaded.unwrap_or_default(),
            in_reply_to: patch.in_reply_to.flatten(),
            references: patch.references.unwrap_or_default(),
            thread_id: None,
            // DraftPatch carries no read-receipt request; that field lives
            // on SendRequest only.
            request_read_receipt: false,
        };
        doc.ensure_from(default_address);
        doc
    }

    fn apply_patch(&mut self, patch: DraftPatch) {
        if let Some(identity) = patch.identity {
            self.identity = Some(identity);
        }
        if let Some(from) = patch.from {
            self.from = from;
        }
        if let Some(to) = patch.to {
            self.to = to;
        }
        if let Some(cc) = patch.cc {
            self.cc = cc;
        }
        if let Some(bcc) = patch.bcc {
            self.bcc = bcc;
        }
        if let Some(reply_to) = patch.reply_to {
            self.reply_to = reply_to;
        }
        if let Some(subject) = patch.subject {
            self.subject = subject;
        }
        if let Some(body_text) = patch.body_text {
            self.body_text = body_text;
        }
        if let Some(body_html) = patch.body_html {
            self.body_html = body_html;
        }
        if let Some(attachments_inline) = patch.attachments_inline {
            self.attachments_inline = attachments_inline;
        }
        if let Some(attachments_uploaded) = patch.attachments_uploaded {
            self.attachments_uploaded = attachments_uploaded;
        }
        if let Some(in_reply_to) = patch.in_reply_to {
            self.in_reply_to = in_reply_to;
        }
        if let Some(references) = patch.references {
            self.references = references;
        }
    }

    // Gmail identities are sendAsEmail addresses by API contract. They may be
    // aliases rather than the owning getProfile mailbox: users selecting a
    // shared or delegated sender expect that sender on the message. Do not
    // replace this identity with the mailbox coordination key.
    fn ensure_from(&mut self, default_address: &str) {
        if self.from.is_none() {
            let address = self
                .identity
                .as_ref()
                .map_or_else(|| default_address.to_string(), |id| id.0.clone());
            self.from = Some(Address::bare(address));
        }
    }
}

fn render_message(
    doc: &MailDocument,
    default_address: &str,
    require_recipient: bool,
) -> Result<String, AccountError> {
    if require_recipient {
        bifrost_types::compose::validate_send_recipients(
            &doc.to,
            &doc.cc,
            &doc.bcc,
            bifrost_types::Protocol::Gmail,
        )?;
    }
    if !doc.attachments_uploaded.is_empty() {
        return Err(unsupported(
            bifrost_types::AccountOperation::AttachmentUpload,
        ));
    }

    let from = doc
        .from
        .clone()
        .unwrap_or_else(|| Address::bare(default_address.to_string()));
    // Gmail learns the Bcc recipients from the message headers (it strips
    // the Bcc header itself before delivery), so the Google path emits it.
    // Gmail mints its own Message-ID, so none is requested here.
    //
    // A send stamps a fresh `Date`. A draft carries none: Gmail's
    // drafts.send transmits the stored draft, and whether Gmail refreshes a
    // supplied `Date` at that point is unverified, so a save-time stamp
    // could go out stale. Leaving it absent keeps what Gmail received
    // before.
    let composed = bifrost_types::ComposedMessage {
        date: require_recipient.then(jiff::Timestamp::now),
        from: Some(&from),
        to: &doc.to,
        cc: &doc.cc,
        bcc: &doc.bcc,
        reply_to: &doc.reply_to,
        subject: doc.subject.as_deref(),
        body_text: doc.body_text.as_deref(),
        body_html: doc.body_html.as_deref(),
        attachments_inline: &doc.attachments_inline,
        in_reply_to: doc.in_reply_to.as_deref(),
        references: &doc.references,
        message_id: None,
        include_bcc_header: true,
        // RFC 8098 read receipt targets the resolved sender. Gmail honors
        // the header in the sent MIME. `None` when unrequested.
        disposition_notification_to: doc.request_read_receipt.then_some(&from),
    };
    let raw = bifrost_types::render_rfc5322(&composed)?;
    Ok(URL_SAFE_NO_PAD.encode(&raw))
}

fn refusal(
    operation: bifrost_types::AccountOperation,
    reason: bifrost_types::UnsupportedReason,
) -> AccountError {
    unsupported(operation)
        .into_builder()
        .unsupported_reason(reason)
        .try_build()
        .expect("valid reason on unsupported refusal")
}

/// Gmail's REST API has no scheduled-send lever; a scheduled request is
/// rejected `Unsupported(Send)` rather than silently sent now.
fn scheduled_send_guard(request: &SendRequest) -> Option<AccountError> {
    request.scheduled.is_some().then(|| {
        refusal(
            bifrost_types::AccountOperation::Send,
            bifrost_types::UnsupportedReason::ScheduledSendUnavailable,
        )
    })
}

pub(crate) fn cancel_scheduled_send_unsupported() -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async {
        Err(refusal(
            bifrost_types::AccountOperation::CancelScheduledSend,
            bifrost_types::UnsupportedReason::ScheduledSendUnavailable,
        ))
    })
}

pub(crate) fn reschedule_send_unsupported() -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async {
        Err(refusal(
            bifrost_types::AccountOperation::RescheduleSend,
            bifrost_types::UnsupportedReason::ScheduledSendUnavailable,
        ))
    })
}

/// Gmail has no delegate-send REST surface in scope; a `send_as`
/// request is rejected `Unsupported(Send)` rather than silently sent
/// from the authenticated user's own mailbox.
fn send_as_guard(request: &SendRequest) -> Option<AccountError> {
    request.send_as.is_some().then(|| {
        refusal(
            bifrost_types::AccountOperation::Send,
            bifrost_types::UnsupportedReason::SendAsUnavailable,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::super::test_support::gmail_message;
    use super::super::test_support::ok_json;
    use super::super::test_support::scripted_client;
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn raw_send_rejects_empty_or_lossy_recipients_before_dispatch() {
        for raw in [
            "From: sender@example.test\r\n\r\nbody",
            "To: kept@example.test, rejected-item\r\n\r\nbody",
        ] {
            let (client, script) = scripted_client(Vec::new());
            let error = send_raw_message(client, Bytes::from(raw), None)
                .await
                .expect_err("invalid recipients must not be submitted");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed,
                )
            );
            assert!(script.requests().is_empty());
        }
    }

    #[tokio::test]
    async fn raw_send_accepts_cc_or_bcc_and_preserves_octets() {
        for field in ["Cc", "Bcc"] {
            let (client, script) =
                scripted_client(vec![ok_json(br#"{"id":"sent","threadId":"thread"}"#)]);
            let raw = Bytes::from(format!("{field}: recipient@example.test\r\n\r\nbody"));
            let sent = send_raw_message(client, raw.clone(), None).await.unwrap();
            assert_eq!(sent.0, "sent");
            let requests = script.requests();
            assert_eq!(requests.len(), 1);
            let body: serde_json::Value =
                serde_json::from_slice(requests[0].body.as_deref().unwrap()).unwrap();
            assert_eq!(body["raw"], URL_SAFE_NO_PAD.encode(&raw));
        }
    }

    #[tokio::test]
    async fn stored_draft_send_rejects_empty_or_lossy_recipients_before_post() {
        for body in [
            br#"{"id":"draft","message":{"id":"message","threadId":"thread","payload":{"mimeType":"text/plain","headers":[]}}}"#.as_slice(),
            br#"{"id":"draft","message":{"id":"message","threadId":"thread","payload":{"mimeType":"text/plain","headers":[{"name":"To","value":"kept@example.test, rejected-item"}]}}}"#.as_slice(),
        ] {
            let (client, script) = scripted_client(vec![ok_json(body)]);
            let error = draft_send(client, DraftHandle("draft".into())).await.unwrap_err();
            assert_eq!(error.kind(), &bifrost_types::AccountErrorKind::Request(
                bifrost_types::RequestErrorKind::Malformed,
            ));
            let requests = script.requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method, reqwest::Method::GET);
        }
    }

    #[tokio::test]
    async fn stored_draft_send_accepts_bcc_without_rewriting() {
        let (client, script) = scripted_client(vec![
            ok_json(br#"{"id":"draft","message":{"id":"message","threadId":"thread","payload":{"mimeType":"text/plain","headers":[{"name":"Bcc","value":"recipient@example.test"}]}}}"#),
            ok_json(br#"{"id":"sent","threadId":"thread"}"#),
        ]);
        assert_eq!(
            draft_send(client, DraftHandle("draft".into()))
                .await
                .unwrap()
                .0,
            "sent"
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, reqwest::Method::GET);
        assert_eq!(requests[1].method, reqwest::Method::POST);
        let body: serde_json::Value =
            serde_json::from_slice(requests[1].body.as_deref().unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"id": "draft"}));
    }

    #[test]
    fn draft_attachment_disposition_uses_only_the_type() {
        let part: crate::types::GmailPayload = serde_json::from_value(serde_json::json!({
            "mimeType": "image/png", "filename": "inline.png",
            "headers": [{"name": "Content-Disposition", "value": "attachment; filename=inline.png"}],
            "body": {"attachmentId": "attachment", "size": 1}
        })).unwrap();
        let mut refs = Vec::new();
        super::collect_attachment_refs(&part, &mut refs);
        assert_eq!(refs.len(), 1);
        assert!(!refs[0].inline);
    }

    #[tokio::test]
    async fn draft_document_decodes_names_before_rendering_again() {
        let (client, script) = scripted_client(Vec::new());
        let message = gmail_message(serde_json::json!({
            "id": "draft-message", "threadId": "thread", "snippet": "",
            "payload": {"mimeType": "text/plain", "headers": [
                {"name": "To", "value": "\"a\\\", b\" <x@y>, =?UTF-8?Q?Doe=2C_Jane?= <jane@test>"}
            ]}
        }));
        let doc = document_from_message(&client, &message, false)
            .await
            .expect("draft document");
        assert_eq!(doc.to.len(), 2);
        assert_eq!(doc.to[0].name.as_deref(), Some("a\", b"));
        assert_eq!(doc.to[0].address, "x@y");
        assert_eq!(doc.to[1].name.as_deref(), Some("Doe, Jane"));
        let encoded = render_message(&doc, "sender@test", true).expect("rendered draft");
        let raw = URL_SAFE_NO_PAD.decode(encoded).expect("base64url draft");
        let parsed = bifrost_types::mime::parse_message(&raw)
            .headers
            .addresses("To");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].name, doc.to[0].name);
        assert_eq!(parsed[1].name, doc.to[1].name);
        assert!(script.requests().is_empty());
    }

    #[tokio::test]
    async fn draft_update_refuses_rejected_addresses_before_put() {
        let (client, script) = scripted_client(vec![ok_json(
            br#"{"id":"draft-1","message":{"id":"message-1","threadId":"thread-1","payload":{"mimeType":"text/plain","headers":[{"name":"To","value":"kept@example.test"},{"name":"To","value":"also@example.test, rejected-item"}]}}}"#,
        )]);

        let error = draft_update(
            client,
            "sender@example.test".to_string(),
            DraftHandle("draft-1".to_string()),
            DraftPatch::default(),
        )
        .await
        .expect_err("a lossy draft address list must not be written back");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert_eq!(
            error.operation(),
            Some(bifrost_types::AccountOperation::DraftUpdate)
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, reqwest::Method::GET);
    }

    #[test]
    fn address_list_respects_quoted_commas_and_escapes() {
        let parsed = Address::parse_list(
            "\"Doe, John\" <jd@example.com>, plain@example.com, \
             \"Escaped \\\"Nick\\\", Esq.\" <nick@example.com>",
        );

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].name.as_deref(), Some("Doe, John"));
        assert_eq!(parsed[0].address, "jd@example.com");
        assert_eq!(parsed[1].name, None);
        assert_eq!(parsed[1].address, "plain@example.com");
        assert_eq!(parsed[2].name.as_deref(), Some("Escaped \"Nick\", Esq."));
        assert_eq!(parsed[2].address, "nick@example.com");
    }

    #[test]
    fn scheduled_send_request_is_unsupported() {
        let mut request = SendRequest::default();
        request.scheduled = Some(std::time::SystemTime::now() + Duration::from_secs(600));
        let err = scheduled_send_guard(&request).expect("scheduled request must be rejected");
        assert_eq!(
            err.unsupported_reason(),
            Some(bifrost_types::UnsupportedReason::ScheduledSendUnavailable)
        );
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(bifrost_types::AccountOperation::Send)
        ));

        // An immediate send passes the guard.
        assert!(scheduled_send_guard(&SendRequest::default()).is_none());
    }

    #[test]
    fn send_as_rejected_unsupported() {
        let mut request = SendRequest::default();
        request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::PrincipalId(
            "shared@contoso.com".to_string(),
        )));
        let err = send_as_guard(&request).expect("send_as request must be rejected");
        assert_eq!(
            err.unsupported_reason(),
            Some(bifrost_types::UnsupportedReason::SendAsUnavailable)
        );
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(bifrost_types::AccountOperation::Send)
        ));
        assert_eq!(err.operation(), Some(bifrost_types::AccountOperation::Send));

        // A personal send passes the guard.
        assert!(send_as_guard(&SendRequest::default()).is_none());
    }

    #[test]
    fn scheduled_send_capability_is_false() {
        let caps = crate::account::capabilities::gmail_capabilities();
        assert!(!caps.pim_methods.scheduled_send);
    }
}
