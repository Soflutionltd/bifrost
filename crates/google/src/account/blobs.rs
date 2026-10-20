//! Gmail blob open paths.
//!
//! Gmail attachments are base64url-encoded inside a JSON envelope and
//! do not support HTTP byte ranges. `open_blob_range` thus always
//! returns `Unsupported(OpenBlobRange)` per capabilities.
//!
//! Errors funnel through `error::into_account_error` and
//! terminate streams with `SyncEvent::Terminated(AccountError)`.

use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountError, AccountOperation, AccountStream, AttachmentSource, Batch, BlobCapabilities,
    BlobEncoding, BlobHandle, BlobId, ByteRange, MessageAttachment, ObjectId, PageBoundary,
    SyncEvent,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::error::GmailLocalError;
use crate::types::{GmailMessage, GmailPayload};

use super::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GmailBlobKey {
    message_id: String,
    attachment_id: String,
}

pub(crate) fn open_blob(
    client: Arc<GmailClient>,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<Bytes>> {
    Box::pin(
        stream::once(async move {
            let started = Instant::now();
            match download_blob(&client, &handle).await {
                Ok((bytes, transferred_size)) => SyncEvent::Batch(Batch {
                    items: vec![bytes],
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: transferred_size,
                    checkpoint: None,
                }),
                Err(error) => terminate_blob(translate(error, &handle.id)),
            }
        })
        .flat_map(finish_blob_event),
    )
}

pub(crate) fn open_blob_range(
    _client: Arc<GmailClient>,
    handle: BlobHandle,
    _range: ByteRange,
) -> AccountStream<SyncEvent<Bytes>> {
    // Gmail attachments never support byte ranges; the convergence
    // contract requires `Unsupported(OpenBlobRange)` rather than a
    // range-not-supported terminal error.
    //
    // This answers `Unsupported` for EVERY input, including a forged handle
    // whose `supports_range` claims otherwise, and that is a decision rather
    // than a stub: Gmail returns an attachment base64url-encoded inside a
    // JSON envelope with no HTTP Range surface, so there is no transport a
    // range could ride. `capabilities.rs` advertises
    // `BlobRangeSupport::No` for the same reason, and this function is what
    // enforces it. It would change only if Gmail published a byte-range
    // attachment endpoint.
    let error = error::into_account_error(
        crate::error::Error::Local(GmailLocalError::BlobRangeUnsupported {
            blob_id: handle.id.0.clone(),
        }),
        error::GmailErrorContext::open_blob_range(),
    );
    Box::pin(stream::iter([terminate_blob(error)]))
}

/// Open a message's assembled RFC822 octets via Gmail `format=raw`.
///
/// Gmail returns the whole message base64url-encoded inside a JSON
/// envelope; `raw_bytes` decodes it to verbatim MIME octets. Emits one
/// `Final` batch then `Done`.
pub(crate) fn open_raw_rfc822(
    client: Arc<GmailClient>,
    message: ObjectId,
) -> AccountStream<SyncEvent<Bytes>> {
    Box::pin(
        stream::once(async move {
            let started = Instant::now();
            // Measure the response envelope before `raw_bytes` decodes its
            // base64url MIME field. The request-local tally belongs to this
            // stream, so another account request cannot inflate this batch.
            let (metered_client, tally) = client.metered();
            match download_raw(&metered_client, &message.0).await {
                Ok(bytes) => SyncEvent::Batch(Batch {
                    items: vec![bytes],
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: tally.take(),
                    checkpoint: None,
                }),
                Err(error) => terminate_blob(error::into_account_error(
                    error,
                    error::GmailErrorContext::open_raw_rfc822(message.0.clone()),
                )),
            }
        })
        .flat_map(finish_blob_event),
    )
}

async fn download_raw(client: &GmailClient, message_id: &str) -> crate::Result<Bytes> {
    let message = client.get_message(message_id, "raw").await?;
    super::inventory::raw_bytes(&message)
}

fn finish_blob_event(event: SyncEvent<Bytes>) -> impl futures::Stream<Item = SyncEvent<Bytes>> {
    let add_done = matches!(event, SyncEvent::Batch(_));
    let mut events = vec![event];
    if add_done {
        events.push(SyncEvent::Done(None));
    }
    stream::iter(events)
}

pub(crate) fn attachments_for_message(message: &GmailMessage) -> Vec<MessageAttachment> {
    let mut attachments = Vec::new();
    if let Some(payload) = &message.payload {
        collect_attachments(&message.id, payload, &mut attachments);
    }
    attachments
}

/// The engine's `HydratedObject` still carries its binary blob shape.
/// User-facing `Message` hydration uses `attachments_for_message` instead.
pub(crate) fn blob_handles_for_message(message: &GmailMessage) -> Vec<BlobHandle> {
    attachments_for_message(message)
        .into_iter()
        .filter_map(|attachment| match attachment.source {
            AttachmentSource::Blob(handle) => Some(handle),
            _ => None,
        })
        .collect()
}

fn collect_attachments(
    message_id: &str,
    part: &GmailPayload,
    attachments: &mut Vec<MessageAttachment>,
) {
    if let Some(body) = &part.body
        && let Some(attachment_id) = &body.attachment_id
    {
        let size = u64::try_from(body.size).ok();
        let handle = BlobHandle {
            id: encode_blob_id(message_id, attachment_id),
            size,
            content_type: Some(part.mime_type.clone()),
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Base64Url,
            },
        };
        attachments.push(MessageAttachment {
            filename: (!part.filename.is_empty()).then(|| part.filename.clone()),
            content_type: Some(part.mime_type.to_ascii_lowercase()),
            content_id: content_id_value(&part.headers),
            inline: content_disposition_inline(&part.headers),
            size,
            source: AttachmentSource::Blob(handle),
            truncated: false,
        });
    }
    for child in &part.parts {
        collect_attachments(message_id, child, attachments);
    }
}

fn content_id_value(headers: &[crate::types::GmailHeader]) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case("Content-ID"))
        .map(|header| {
            header
                .value
                .trim()
                .trim_matches(|character| character == '<' || character == '>')
                .to_string()
        })
}

fn content_disposition_inline(headers: &[crate::types::GmailHeader]) -> bool {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case("Content-Disposition"))
        .is_some_and(|header| header.value.to_ascii_lowercase().contains("inline"))
}

fn encode_blob_id(message_id: &str, attachment_id: &str) -> BlobId {
    let key = GmailBlobKey {
        message_id: message_id.to_string(),
        attachment_id: attachment_id.to_string(),
    };
    BlobId(serde_json::to_string(&key).unwrap_or_default())
}

enum BlobError {
    InvalidId(String),
    Gmail(crate::Error),
}

async fn download_blob(
    client: &GmailClient,
    handle: &BlobHandle,
) -> Result<(Bytes, u64), BlobError> {
    let key = decode_blob_id(&handle.id)?;
    let (attachment, transferred_size) = client
        .get_attachment_with_transferred_size(&key.message_id, &key.attachment_id)
        .await
        .map_err(BlobError::Gmail)?;
    let decoded = decode_base64url_nopad(&attachment.data).map_err(BlobError::Gmail)?;
    Ok((Bytes::from(decoded), transferred_size))
}

fn decode_blob_id(id: &BlobId) -> Result<GmailBlobKey, BlobError> {
    serde_json::from_str(&id.0).map_err(|err| BlobError::InvalidId(err.to_string()))
}

fn translate(error: BlobError, blob_id: &BlobId) -> AccountError {
    match error {
        BlobError::InvalidId(detail) => error::into_account_error(
            crate::error::Error::invalid_request(
                AccountOperation::OpenBlob,
                format!("invalid gmail blob id: {detail}"),
            ),
            error::GmailErrorContext::open_blob(blob_id.0.clone()),
        ),
        BlobError::Gmail(error) => error::into_account_error(
            error,
            error::GmailErrorContext::open_blob(blob_id.0.clone()),
        ),
    }
}

fn terminate_blob(error: AccountError) -> SyncEvent<Bytes> {
    SyncEvent::Terminated(error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use futures::StreamExt;
    use reqwest::StatusCode;

    use super::*;
    use serde_json::json;

    fn message(value: serde_json::Value) -> GmailMessage {
        serde_json::from_value(value).expect("message fixture deserializes")
    }

    fn scripted_client(body: Vec<u8>) -> Arc<GmailClient> {
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(body),
        }]);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        Arc::new(GmailClient::with_account_net("https://gmail.test", net))
    }

    #[test]
    fn a_message_without_a_payload_has_no_attachments() {
        let msg = message(json!({ "id": "m1", "threadId": "t1" }));
        assert!(attachments_for_message(&msg).is_empty());
    }

    /// Only parts carrying an `attachmentId` become attachments.
    /// Inline bodies (Gmail returns their bytes in `body.data`) are
    /// deliberately not surfaced as attachments.
    #[test]
    fn inline_bodies_without_an_attachment_id_are_not_attachments() {
        let msg = message(json!({
            "id": "m2",
            "threadId": "t2",
            "payload": {
                "mimeType": "text/plain",
                "body": { "size": 12, "data": "aGVsbG8" },
            },
        }));
        assert!(attachments_for_message(&msg).is_empty());
    }

    #[test]
    fn attachments_for_message_preserves_metadata_and_gmail_blob_capabilities() {
        let msg = message(json!({
            "id": "m3",
            "threadId": "t3",
            "payload": {
                "mimeType": "multipart/mixed",
                "parts": [
                    {
                        "mimeType": "text/plain",
                        "body": { "size": 4, "data": "aGk" },
                    },
                    {
                        "mimeType": "application/pdf",
                        "filename": "report.pdf",
                        "headers": [
                            { "name": "Content-ID", "value": "<report-1>" },
                            { "name": "Content-Disposition", "value": "inline; filename=report.pdf" }
                        ],
                        "body": { "attachmentId": "att-1", "size": 2048 },
                    },
                ],
            },
        }));

        let attachments = attachments_for_message(&msg);
        assert_eq!(attachments.len(), 1);
        let attachment = &attachments[0];
        assert_eq!(attachment.filename.as_deref(), Some("report.pdf"));
        assert_eq!(attachment.content_type.as_deref(), Some("application/pdf"));
        assert_eq!(attachment.content_id.as_deref(), Some("report-1"));
        assert!(attachment.inline);
        assert_eq!(attachment.size, Some(2048));
        assert!(!attachment.truncated);
        let AttachmentSource::Blob(handle) = &attachment.source else {
            panic!("Gmail attachments must retain an openable blob handle");
        };
        assert_eq!(handle.size, Some(2048));
        assert_eq!(handle.content_type.as_deref(), Some("application/pdf"));
        assert!(
            handle.digest.is_none(),
            "Gmail exposes no pre-download digest for attachments"
        );
        assert!(
            !handle.capabilities.supports_range,
            "the range gate in open_blob_range depends on this staying false"
        );
        assert!(!handle.capabilities.supports_parallel);
        assert!(!handle.capabilities.digest_available_pre_download);
        assert!(matches!(
            handle.capabilities.encoding,
            BlobEncoding::Base64Url
        ));
    }

    /// The MIME tree is walked depth-first, so an attachment nested
    /// inside a `multipart/related` under a `multipart/mixed` is still
    /// found. A shallow walk would silently drop inline-image parts.
    #[test]
    fn nested_multipart_trees_are_walked_recursively() {
        let msg = message(json!({
            "id": "m4",
            "threadId": "t4",
            "payload": {
                "mimeType": "multipart/mixed",
                "parts": [{
                    "mimeType": "multipart/related",
                    "parts": [{
                        "mimeType": "image/png",
                        "body": { "attachmentId": "deep-att", "size": 99 },
                    }],
                }],
            },
        }));
        let attachments = attachments_for_message(&msg);
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].size, Some(99));
    }

    /// A negative `size` (Gmail should never send one, but the DTO is
    /// `i64`) yields `None` rather than a wrapped huge `u64`.
    #[test]
    fn a_negative_part_size_becomes_an_unknown_size() {
        let msg = message(json!({
            "id": "m5",
            "threadId": "t5",
            "payload": {
                "mimeType": "application/octet-stream",
                "body": { "attachmentId": "att-neg", "size": -1 },
            },
        }));
        assert_eq!(attachments_for_message(&msg)[0].size, None);
    }

    /// The blob id is an opaque JSON envelope pairing the message with
    /// the attachment; `open_blob` decodes it back. Both halves must
    /// survive, because Gmail's attachment endpoint is message-scoped.
    #[test]
    fn blob_ids_round_trip_the_message_and_attachment_pair() {
        let encoded = encode_blob_id("msg-1", "att-1");
        let decoded = match decode_blob_id(&encoded) {
            Ok(key) => key,
            Err(_) => panic!("a freshly encoded blob id must decode"),
        };
        assert_eq!(decoded.message_id, "msg-1");
        assert_eq!(decoded.attachment_id, "att-1");
    }

    #[test]
    fn a_malformed_blob_id_is_rejected_rather_than_guessed_at() {
        for raw in ["", "not json", "{}", r#"{"message_id":"m"}"#] {
            assert!(
                matches!(
                    decode_blob_id(&BlobId(raw.to_owned())),
                    Err(BlobError::InvalidId(_))
                ),
                "{raw:?} must not decode as a blob id"
            );
        }
    }

    /// Ids containing JSON metacharacters survive the envelope, so an
    /// exotic attachment id cannot corrupt the handle.
    #[test]
    fn blob_ids_survive_json_metacharacters() {
        let encoded = encode_blob_id(r#"m"1"#, "a\\b");
        let decoded = match decode_blob_id(&encoded) {
            Ok(key) => key,
            Err(_) => panic!("escaped ids must round-trip"),
        };
        assert_eq!(decoded.message_id, r#"m"1"#);
        assert_eq!(decoded.attachment_id, "a\\b");
    }

    #[tokio::test]
    async fn open_blob_reports_the_transferred_json_size() {
        let body = br#"{ "data": "aGVsbG8" }"#.to_vec();
        let transferred = body.len() as u64;
        let handle = BlobHandle {
            id: encode_blob_id("m1", "a1"),
            size: Some(5),
            content_type: None,
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Base64Url,
            },
        };

        let events = open_blob(scripted_client(body), handle)
            .collect::<Vec<_>>()
            .await;
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("blob open must emit a batch");
        };
        assert_eq!(batch.items[0].as_ref(), b"hello");
        assert_eq!(batch.bytes_in, transferred);
        assert_ne!(batch.bytes_in, 5, "decoded size is not transferred size");
    }

    #[tokio::test]
    async fn open_raw_rfc822_reports_the_transferred_envelope_size() {
        let response = br#"{"id":"m1","threadId":"t1","raw":"RnJvbTogYUBiDQoNCmhp"}"#;
        let transferred = response.len() as u64;
        let events = open_raw_rfc822(
            scripted_client(response.to_vec()),
            ObjectId("m1".to_string()),
        )
        .collect::<Vec<_>>()
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("raw RFC822 open must emit a batch");
        };
        assert_eq!(batch.items[0].as_ref(), b"From: a@b\r\n\r\nhi");
        assert_eq!(batch.bytes_in, transferred);
        assert_ne!(batch.bytes_in, batch.items[0].len() as u64);
    }

    #[tokio::test]
    async fn range_claim_on_a_forged_handle_still_returns_unsupported() {
        let handle = BlobHandle {
            id: encode_blob_id("m1", "a1"),
            size: Some(5),
            content_type: None,
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: true,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Base64Url,
            },
        };
        let client = scripted_client(br#"{"data":"aGVsbG8"}"#.to_vec());
        let events = open_blob_range(
            client,
            handle,
            ByteRange {
                start: 0,
                length: Some(1),
            },
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(events.len(), 1);
        let SyncEvent::Terminated(error) = &events[0] else {
            panic!("range open must terminate with Unsupported");
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::OpenBlobRange)
        ));
    }
}
