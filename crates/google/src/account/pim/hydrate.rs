//! Gmail hydrate operations.

use super::super::blobs::content_id_value;
use super::super::scopes::{ScopeCache, labels_for_flags};
use super::super::{blobs, error, flags};
use super::{account_error_for, non_negative_i64};
use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::headers::{find_header_value_case_insensitive, references_from_header};
use crate::types::{GmailHeader, GmailLabel, GmailMessage, GmailPayload};
use bifrost_types::{
    AccountError, AccountFuture, Address, ContainerId, HydrationProjection, Importance, Message,
    ObjectId, ThreadHydration, ThreadId,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn thread_hydrate(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        let labels = labels_for_flags(&client, &cache).await.map_err(|error| {
            account_error_for(
                error,
                error::GmailErrorContext::hydrate_thread(thread.0.clone()),
            )
        })?;
        let gmail_thread = client.get_thread(&thread.0, "full").await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::hydrate_thread(thread.0.clone()),
            )
        })?;
        let mut messages = Vec::with_capacity(gmail_thread.messages.len());
        for message in &gmail_thread.messages {
            messages.push(message_from_gmail(&labels, message, HydrationProjection::Full).await?);
        }
        Ok(ThreadHydration {
            id: ThreadId(gmail_thread.id),
            messages,
        })
    })
}

pub(crate) fn message_hydrate(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        let labels = labels_for_flags(&client, &cache).await.map_err(|error| {
            account_error_for(
                error,
                error::GmailErrorContext::hydrate_message(message.0.clone()),
            )
        })?;
        let format = match projection {
            HydrationProjection::Headers | HydrationProjection::Preview(_) => "metadata",
            HydrationProjection::Full | HydrationProjection::FullWithBlobs => "full",
            _ => "full",
        };
        let gmail_message = client.get_message(&message.0, format).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::hydrate_message(message.0.clone()),
            )
        })?;
        message_from_gmail(&labels, &gmail_message, projection).await
    })
}

async fn message_from_gmail(
    labels: &[GmailLabel],
    message: &GmailMessage,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    let headers = message_headers(message);
    let (body_text, body_html, mut incomplete) = match projection {
        HydrationProjection::Headers => (None, None, false),
        HydrationProjection::Preview(limit) => (
            Some(message.snippet.chars().take(limit).collect::<String>()),
            None,
            false,
        ),
        HydrationProjection::Full | HydrationProjection::FullWithBlobs => {
            selected_body_parts(message.payload.as_ref())
        }
        _ => selected_body_parts(message.payload.as_ref()),
    };
    let attachments = if matches!(
        projection,
        HydrationProjection::Full | HydrationProjection::FullWithBlobs
    ) {
        blobs::attachments_for_message(message)
    } else {
        Vec::new()
    };
    let flags = flags::flag_set(&message.label_ids, labels);

    Ok(Message {
        id: ObjectId(message.id.clone()),
        thread_id: Some(ThreadId(message.thread_id.clone())),
        from: hydrated_address_list(headers, "From", &mut incomplete),
        to: hydrated_address_list(headers, "To", &mut incomplete),
        cc: hydrated_address_list(headers, "Cc", &mut incomplete),
        bcc: hydrated_address_list(headers, "Bcc", &mut incomplete),
        reply_to: hydrated_address_list(headers, "Reply-To", &mut incomplete),
        subject: header_from(headers, "Subject"),
        date: internal_date(&message.internal_date),
        containers: message
            .label_ids
            .iter()
            .map(|id| ContainerId(id.clone()))
            .collect(),
        flags,
        // Gmail has no importance field; always Normal.
        importance: Importance::Normal,
        body_text,
        body_html,
        attachments,
        incomplete,
        size_bytes: message.size_estimate.and_then(non_negative_i64),
        in_reply_to: header_from(headers, "In-Reply-To"),
        references: references_from_header(header_from(headers, "References").as_deref()),
    })
}

// Users need readable mail even when one mailbox cannot be represented, but
// must see that the recipient list is incomplete before relying on reply-all.
// Preserve every repeated field in wire order, including accepted siblings.
fn hydrated_address_list(
    headers: &[GmailHeader],
    name: &str,
    incomplete: &mut bool,
) -> Vec<Address> {
    let mut addresses = Vec::new();
    for header in headers
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case(name))
    {
        match Address::parse_list_checked(&header.value) {
            Ok(parsed) => addresses.extend(parsed),
            Err(_) => {
                *incomplete = true;
                // The checked API does not expose accepted siblings on failure.
                addresses.extend(Address::parse_list(&header.value));
            }
        }
    }
    addresses
}

pub(super) fn body_parts(payload: Option<&GmailPayload>) -> (Option<String>, Option<String>) {
    let (text, html, _) = selected_body_parts(payload);
    (text, html)
}

// Gmail adapts payload topology only; the shared selector owns body policy.
fn selected_body_parts(payload: Option<&GmailPayload>) -> (Option<String>, Option<String>, bool) {
    use bifrost_types::mime::{ParsedMessage, select_body};

    let Some(payload) = payload else {
        return (None, None, true);
    };
    let mut incomplete = false;
    let root = gmail_mime_part(payload, &mut incomplete);
    let selected = select_body(&ParsedMessage {
        headers: root.headers.clone(),
        root,
        defects: Vec::new(),
    });
    incomplete |= !selected.defects.is_empty();
    (selected.text, selected.html, incomplete)
}

/// Gmail has already decoded Content-Transfer-Encoding. Preserve MIME metadata
/// and topology, but feed the selector those decoded bytes without decoding twice.
fn gmail_mime_part(part: &GmailPayload, incomplete: &mut bool) -> bifrost_types::mime::MimePart {
    use bifrost_types::mime::{
        HeaderMap, MimePart, PartBody, TransferEncoding, parse_content_type,
    };

    let header_block = part
        .headers
        .iter()
        .map(|header| format!("{}: {}\r\n", header.name, header.value))
        .collect::<String>();
    let headers = HeaderMap::parse(header_block.as_bytes());
    let content_type = parse_content_type(headers.get("Content-Type").unwrap_or(&part.mime_type));
    let disposition = headers
        .get("Content-Disposition")
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let filename = (!part.filename.is_empty()).then(|| part.filename.clone());
    let body = if content_type.ty == "multipart" {
        PartBody::Multipart(
            part.parts
                .iter()
                .map(|child| gmail_mime_part(child, incomplete))
                .collect(),
        )
    } else {
        let data = part.body.as_ref().and_then(|body| body.data.as_deref());
        let bytes = match data.map(decode_base64url_nopad).transpose() {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                // An explicitly empty body is complete: users should not see
                // a download warning on an intentionally blank message. Missing
                // size or an external body still supplies no evidence of bytes.
                let known_empty = part
                    .body
                    .as_ref()
                    .is_some_and(|body| body.size == Some(0) && body.attachment_id.is_none());
                if !known_empty
                    && content_type.ty == "text"
                    && disposition != "attachment"
                    && filename.is_none()
                {
                    *incomplete = true;
                }
                Vec::new()
            }
            Err(_) => {
                *incomplete = true;
                Vec::new()
            }
        };
        PartBody::Leaf(bytes)
    };
    MimePart {
        content_id: content_id_value(&part.headers),
        headers,
        content_type,
        encoding: TransferEncoding::Binary,
        disposition,
        filename,
        description: None,
        body,
    }
}

pub(super) fn message_headers(message: &GmailMessage) -> &[GmailHeader] {
    message
        .payload
        .as_ref()
        .map_or(&[] as &[GmailHeader], |payload| payload.headers.as_slice())
}

pub(super) fn header_from(headers: &[GmailHeader], name: &str) -> Option<String> {
    find_header_value_case_insensitive(
        headers,
        name,
        |header| header.name.as_str(),
        |header| header.value.as_str(),
    )
}

fn internal_date(value: &Option<String>) -> Option<SystemTime> {
    value
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::gmail_message;
    use super::*;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

    #[tokio::test]
    async fn hydrate_repeated_address_headers_preserves_siblings_and_marks_loss() {
        let message = gmail_message(serde_json::json!({
            "id": "m1", "threadId": "t1",
            "payload": {"mimeType": "text/plain", "headers": [
                {"name": "To", "value": "first@example.com"},
                {"name": "tO", "value": "second@example.com, invalid"},
                {"name": "Cc", "value": "cc@example.com"},
                {"name": "Cc", "value": "other@example.com"}
            ]}
        }));
        let hydrated = message_from_gmail(&[], &message, HydrationProjection::Headers)
            .await
            .unwrap();
        assert_eq!(hydrated.to.len(), 2);
        assert_eq!(hydrated.to[0].address, "first@example.com");
        assert_eq!(hydrated.to[1].address, "second@example.com");
        assert_eq!(hydrated.cc.len(), 2);
        assert!(hydrated.incomplete);
    }

    #[tokio::test]
    async fn hydrate_decodes_declared_charset_without_transfer_decoding_twice() {
        let message = gmail_message(serde_json::json!({
            "id": "m1", "threadId": "t1",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "Content-Type", "value": "text/plain; charset=iso-8859-1"},
                    {"name": "Content-Transfer-Encoding", "value": "quoted-printable"}
                ],
                "body": {"data": URL_SAFE_NO_PAD.encode(b"caf\xe9 =E9"), "size": 8}
            }
        }));
        let hydrated = message_from_gmail(&[], &message, HydrationProjection::Full)
            .await
            .unwrap();
        assert_eq!(hydrated.body_text.as_deref(), Some("caf\u{e9} =E9"));
        assert!(!hydrated.incomplete);
    }

    #[tokio::test]
    async fn hydrate_explicitly_empty_body_is_complete() {
        let message = gmail_message(serde_json::json!({
            "id": "m1", "threadId": "t1",
            "payload": {"mimeType": "text/plain", "body": {"size": 0}}
        }));
        let hydrated = message_from_gmail(&[], &message, HydrationProjection::Full)
            .await
            .unwrap();
        assert!(!hydrated.incomplete);
    }

    #[tokio::test]
    async fn hydrate_missing_or_invalid_body_is_incomplete() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({"size": 42}),
            serde_json::json!({"attachmentId": "external-body", "size": 0}),
            serde_json::json!({"attachmentId": "external-body", "size": 42}),
            serde_json::json!({"data": "!!!", "size": 42}),
        ] {
            let message = gmail_message(serde_json::json!({
                "id": "m1", "threadId": "t1",
                "payload": {"mimeType": "text/plain", "body": body}
            }));
            let hydrated = message_from_gmail(&[], &message, HydrationProjection::Full)
                .await
                .unwrap();
            assert!(hydrated.incomplete);
        }
    }

    #[tokio::test]
    async fn hydrate_selects_last_alternative_and_skips_text_attachments() {
        let message = gmail_message(serde_json::json!({
            "id": "m1", "threadId": "t1",
            "payload": {"mimeType": "multipart/alternative", "parts": [
                {"mimeType": "text/plain", "filename": "a.txt",
                 "body": {"data": URL_SAFE_NO_PAD.encode(b"attachment")}},
                {"mimeType": "text/plain", "body": {"data": URL_SAFE_NO_PAD.encode(b"old")}},
                {"mimeType": "text/plain", "body": {"data": URL_SAFE_NO_PAD.encode(b"new")}}
            ]}
        }));
        let hydrated = message_from_gmail(&[], &message, HydrationProjection::Full)
            .await
            .unwrap();
        assert_eq!(hydrated.body_text.as_deref(), Some("new"));
    }

    #[tokio::test]
    async fn hydrate_full_reports_attachment_metadata_without_bytes() {
        let message = gmail_message(serde_json::json!({
            "id": "m1",
            "threadId": "t1",
            "payload": {
                "mimeType": "multipart/mixed",
                "headers": [],
                "parts": [{
                    "mimeType": "image/png",
                    "filename": "chart.png",
                    "headers": [
                        { "name": "Content-ID", "value": "<chart-image>" },
                        { "name": "Content-Disposition", "value": "inline; filename=chart.png" }
                    ],
                    "body": { "attachmentId": "att-1", "size": 42 }
                }]
            }
        }));

        let hydrated = message_from_gmail(&[], &message, HydrationProjection::Full)
            .await
            .expect("full hydration maps the parsed Gmail payload");

        assert_eq!(hydrated.attachments.len(), 1);
        let attachment = &hydrated.attachments[0];
        assert_eq!(attachment.filename.as_deref(), Some("chart.png"));
        assert_eq!(attachment.content_type.as_deref(), Some("image/png"));
        assert_eq!(attachment.content_id.as_deref(), Some("chart-image"));
        assert!(attachment.inline);
        assert_eq!(attachment.size, Some(42));
        assert!(!attachment.truncated);
        assert!(matches!(
            &attachment.source,
            bifrost_types::AttachmentSource::Blob(_)
        ));
    }

    #[tokio::test]
    async fn message_hydration_remains_lenient_for_rejected_addresses() {
        let message = gmail_message(serde_json::json!({
            "id": "message-1", "threadId": "thread-1",
            "payload": {"mimeType": "text/plain", "headers": [
                {"name": "To", "value": "kept@example.test, rejected-item"}
            ]}
        }));

        let hydrated = message_from_gmail(&[], &message, HydrationProjection::Headers)
            .await
            .expect("ordinary message hydration keeps its tolerant behavior");

        assert_eq!(hydrated.to.len(), 1);
        assert_eq!(hydrated.to[0].address, "kept@example.test");
    }
}
