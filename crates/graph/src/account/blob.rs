use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream, Batch,
    BlobCapabilities, BlobEncoding, BlobHandle, BlobId, ByteRange, Cause, Checkpoint,
    DiagnosticText, ErrorScope, ObjectId, PageBoundary, Protocol, ProtocolErrorKind, Provider,
    RequestCause, SyncEvent, WireCause,
};

use super::GraphAccount;
use super::error::warning_blob_not_byte_stream;
use super::graph_error::{GraphErrorContext, into_account_error};
use crate::client::Resource;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum GraphBlobKind {
    File,
    Item,
    Reference,
    /// An EWS `FileAttachment` on a public-folder item. Fetched via SOAP
    /// `GetAttachment` (base64-inline), not via a Graph REST `$value` byte
    /// stream - EWS has no such endpoint.
    Ews,
    /// An EWS `ItemAttachment`: an embedded Exchange item whose
    /// `GetAttachment` payload is nested XML, not octets. Surfaced as a
    /// non-byte-stream blob, like a Graph reference attachment.
    EwsItem,
    Unknown,
}

// Graph blob ids need message id, attachment id, and attachment kind inside bifrost_types::BlobId.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GraphBlobLocator {
    message_id: String,
    attachment_id: String,
    kind: GraphBlobKind,
}

/// One streamed byte chunk as a `Batch`.
///
/// `bytes_in` carries the chunk's real length: the engine meters download
/// volume off it, and reporting 0 here made every REST byte stream invisible
/// to metering while the EWS attachment path reported the truth.
fn byte_chunk_batch(bytes: Bytes, page_boundary: PageBoundary) -> Batch<Bytes> {
    let bytes_in = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    Batch {
        items: vec![bytes],
        page_boundary,
        server_latency: std::time::Duration::default(),
        bytes_in,
        checkpoint: None::<Checkpoint>,
    }
}

pub(crate) fn open_blob_stream(
    account: GraphAccount,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<Bytes>> {
    open_blob_inner_stream(account, handle, None)
}

pub(crate) fn open_blob_range_stream(
    account: GraphAccount,
    handle: BlobHandle,
    range: ByteRange,
) -> AccountStream<SyncEvent<Bytes>> {
    open_blob_inner_stream(account, handle, Some(range))
}

pub(crate) fn blob_handle_from_graph_attachment(
    message_id: &ObjectId,
    attachment: &Value,
) -> Option<BlobHandle> {
    let attachment_id = attachment.get("id")?.as_str()?.to_string();
    let odata_type = attachment
        .get("@odata.type")
        .and_then(Value::as_str)
        .unwrap_or("#microsoft.graph.fileAttachment");
    let kind = match odata_type {
        "#microsoft.graph.fileAttachment" => GraphBlobKind::File,
        "#microsoft.graph.itemAttachment" => GraphBlobKind::Item,
        "#microsoft.graph.referenceAttachment" => GraphBlobKind::Reference,
        _ => GraphBlobKind::Unknown,
    };
    let supports_range = kind == GraphBlobKind::File;
    let locator = GraphBlobLocator {
        message_id: message_id.0.clone(),
        attachment_id,
        kind,
    };
    let id = serde_json::to_string(&locator)
        .unwrap_or_else(|_| format!("{}:{}", locator.message_id, locator.attachment_id));
    let size = attachment
        .get("size")
        .and_then(Value::as_i64)
        .and_then(|size| u64::try_from(size).ok());

    Some(BlobHandle {
        id: BlobId(id),
        size,
        content_type: attachment
            .get("contentType")
            .and_then(Value::as_str)
            .map(str::to_string),
        digest: None,
        capabilities: BlobCapabilities {
            supports_range,
            supports_parallel: supports_range,
            digest_available_pre_download: false,
            encoding: BlobEncoding::Raw8Bit,
        },
    })
}

/// Mint a blob handle for one EWS attachment descriptor on a public-folder
/// item.
///
/// `message_id` is the folder-qualified public item id, so the locator
/// carries the folder whose `routing_map` entry supplies the
/// `X-AnchorMailbox` / `X-PublicFolderMailbox` pair `GetAttachment` needs.
/// Range is never advertised: `GetAttachment` returns the whole attachment
/// base64-inline in a SOAP body, so there is nothing to range over.
pub(crate) fn blob_handle_from_ews_attachment(
    message_id: &ObjectId,
    attachment: &crate::ews::EwsAttachment,
) -> BlobHandle {
    let locator = GraphBlobLocator {
        message_id: message_id.0.clone(),
        attachment_id: attachment.attachment_id.clone(),
        kind: if attachment.is_item {
            GraphBlobKind::EwsItem
        } else {
            GraphBlobKind::Ews
        },
    };
    let id = serde_json::to_string(&locator)
        .unwrap_or_else(|_| format!("{}:{}", locator.message_id, locator.attachment_id));
    BlobHandle {
        id: BlobId(id),
        size: attachment.size,
        content_type: attachment.content_type.clone(),
        digest: None,
        capabilities: BlobCapabilities {
            supports_range: false,
            supports_parallel: false,
            digest_available_pre_download: false,
            encoding: BlobEncoding::Raw8Bit,
        },
    }
}

/// Fetch an EWS attachment's bytes through SOAP `GetAttachment`, routed by
/// the enclosing public folder's `routing_map` headers.
async fn fetch_ews_attachment(
    account: &GraphAccount,
    locator: &GraphBlobLocator,
) -> Result<Bytes, EwsAttachmentError> {
    let parsed = super::foreign::parse_message_id(&ObjectId(locator.message_id.clone()));
    let Some(folder) = parsed.public_folder() else {
        return Err(EwsAttachmentError::NotPublic);
    };
    let folder = bifrost_types::FolderId(folder.to_string());
    let Some(routing) = account.public_folder_routing(&folder).await else {
        return Err(EwsAttachmentError::NotPublic);
    };
    let ews = super::public_folder::ews_client(account)
        .map_err(|error| EwsAttachmentError::NotAttached(Box::new(error)))?;
    let content = ews
        .get_attachment(&locator.attachment_id, &routing.headers())
        .await
        .map_err(|error| EwsAttachmentError::Ews(Box::new(error)))?;
    Ok(Bytes::from(content.content))
}

enum EwsAttachmentError {
    /// The locator does not name a public folder we route (a stale handle,
    /// or discovery has not seeded the folder).
    NotPublic,
    /// No attached transport: the client's own invariant (see
    /// `public_folder::ews_client`), scoped to the account.
    NotAttached(Box<crate::ews::EwsError>),
    Ews(Box<crate::ews::EwsError>),
}

fn open_ews_blob_stream(
    account: GraphAccount,
    locator: GraphBlobLocator,
) -> AccountStream<SyncEvent<Bytes>> {
    Box::pin(async_stream::stream! {
        match fetch_ews_attachment(&account, &locator).await {
            Ok(bytes) => {
                // GetAttachment returns the whole attachment in one SOAP
                // body, so this is always a single final page.
                yield SyncEvent::Batch(byte_chunk_batch(bytes, PageBoundary::Final));
            }
            Err(EwsAttachmentError::NotPublic) => {
                yield SyncEvent::Warning(warning_blob_not_byte_stream(
                    &ObjectId(locator.message_id.clone()),
                ));
            }
            Err(EwsAttachmentError::NotAttached(error)) => {
                let ctx = GraphErrorContext::ews(AccountOperation::OpenBlob)
                    .with_scope(ErrorScope::Account);
                yield SyncEvent::Terminated(super::graph_error::ews_error_to_account_error(
                    *error, ctx,
                ));
            }
            Err(EwsAttachmentError::Ews(error)) => {
                let ctx = GraphErrorContext::ews(AccountOperation::OpenBlob)
                    .with_scope(ErrorScope::Message { id: (locator.message_id.clone() ).into()});
                yield SyncEvent::Terminated(super::graph_error::ews_error_to_account_error(
                    *error, ctx,
                ));
            }
        }
        yield SyncEvent::Done(None);
    })
}

fn open_blob_inner_stream(
    account: GraphAccount,
    handle: BlobHandle,
    range: Option<ByteRange>,
) -> AccountStream<SyncEvent<Bytes>> {
    Box::pin(async_stream::stream! {
        let locator = match decode_locator(&handle) {
            Ok(locator) => locator,
            Err(error) => {
                // A handle this crate did not mint (or one damaged in the
                // caller's storage). Refused before any request, so it is the
                // caller's input, never a provider fault - `Request(Malformed)`,
                // matching Google's `BlobError::InvalidId`.
                let account_error = crate::account::graph_error::invalid_account_error(
                    AccountOperation::OpenBlob,
                    error,
                );
                yield SyncEvent::Terminated(account_error);
                yield SyncEvent::Done(None);
                return;
            }
        };
        if locator.kind == GraphBlobKind::Reference || locator.kind == GraphBlobKind::EwsItem {
            yield SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id)));
            yield SyncEvent::Done(None);
            return;
        }
        // A public-folder attachment is an EWS SOAP read, not a Graph REST
        // `$value` byte stream: Graph cannot address an EWS AttachmentId at
        // all. Ranged reads are refused below (the handle never advertises
        // range support), so only the whole-attachment path reaches here.
        if locator.kind == GraphBlobKind::Ews {
            if range.is_some() {
                yield SyncEvent::Terminated(unsupported_range_error());
                yield SyncEvent::Done(None);
                return;
            }
            let mut stream = open_ews_blob_stream(account, locator);
            while let Some(event) = stream.next().await {
                yield event;
            }
            return;
        }
        let op = if range.is_some() {
            AccountOperation::OpenBlobRange
        } else {
            AccountOperation::OpenBlob
        };
        if range.is_some() && !handle.capabilities.supports_range {
            yield SyncEvent::Terminated(unsupported_range_error());
            yield SyncEvent::Done(None);
            return;
        }

        let mut stream = match fetch_blob_stream(&account, &locator, range).await {
            Ok(stream) => stream,
            Err(BlobFetchError::MethodNotAllowed) => {
                yield SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id)));
                yield SyncEvent::Done(None);
                return;
            }
            Err(BlobFetchError::Failed(error)) => {
                let ctx = GraphErrorContext::graph(op).with_scope(ErrorScope::Account);
                yield SyncEvent::Terminated(into_account_error(*error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };

        // One chunk of lookahead so the last chunk of a complete stream can be
        // `Final`. A stream that errors mid-way has no final page: its pending
        // chunk flushes as `Page` before the terminal error.
        let mut pending: Option<Bytes> = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if let Some(prev) = pending.replace(bytes) {
                        yield SyncEvent::Batch(byte_chunk_batch(prev, PageBoundary::Page));
                    }
                }
                Err(error) => {
                    if let Some(prev) = pending.take() {
                        yield SyncEvent::Batch(byte_chunk_batch(prev, PageBoundary::Page));
                    }
                    let account_error = AccountErrorBuilder::new(
                        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
                        Cause::Wire(WireCause::MalformedResponse {
                            protocol: Protocol::Graph,
                            detail: Some(DiagnosticText::support_only(format!(
                                "Graph blob stream error: {error}"
                            ))),
                        }),
                    )
                    .operation(op)
                    .provider(Provider::Microsoft)
                    .protocol(Protocol::Graph)
                    .scope(ErrorScope::Account)
                    .try_build()
                .expect("valid account error classification");
                    yield SyncEvent::Terminated(account_error);
                    break;
                }
            }
        }
        if let Some(last) = pending {
            yield SyncEvent::Batch(byte_chunk_batch(last, PageBoundary::Final));
        }
        yield SyncEvent::Done(None);
    })
}

/// Open a message's assembled RFC822 octets via `GET /messages/{id}/$value`.
///
/// Graph returns the verbatim assembled MIME for the message. Streams
/// chunks as `SyncEvent::Batch`, terminating through the shared
/// `into_account_error` boundary tagged `OpenRawRfc822`.
pub(crate) fn open_raw_rfc822(
    account: GraphAccount,
    message: ObjectId,
) -> AccountStream<SyncEvent<Bytes>> {
    Box::pin(async_stream::stream! {
        let op = AccountOperation::OpenRawRfc822;
        let mut stream = match fetch_raw_stream(&account, &message).await {
            Ok(stream) => stream,
            Err(error) => {
                let ctx = GraphErrorContext::graph(op)
                    .with_scope(ErrorScope::Message { id: (message.0.clone() ).into()});
                yield SyncEvent::Terminated(into_account_error(*error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };

        // Same one-chunk lookahead as `open_blob_inner_stream`.
        let mut pending: Option<Bytes> = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if let Some(prev) = pending.replace(bytes) {
                        yield SyncEvent::Batch(byte_chunk_batch(prev, PageBoundary::Page));
                    }
                }
                Err(error) => {
                    if let Some(prev) = pending.take() {
                        yield SyncEvent::Batch(byte_chunk_batch(prev, PageBoundary::Page));
                    }
                    let account_error = AccountErrorBuilder::new(
                        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
                        Cause::Wire(WireCause::MalformedResponse {
                            protocol: Protocol::Graph,
                            detail: Some(DiagnosticText::support_only(format!(
                                "Graph raw message stream error: {error}"
                            ))),
                        }),
                    )
                    .operation(op)
                    .provider(Provider::Microsoft)
                    .protocol(Protocol::Graph)
                    .scope(ErrorScope::Message { id: (message.0.clone() ).into()})
                    .try_build()
                    .expect("valid account error classification");
                    yield SyncEvent::Terminated(account_error);
                    break;
                }
            }
        }
        if let Some(last) = pending {
            yield SyncEvent::Batch(byte_chunk_batch(last, PageBoundary::Final));
        }
        yield SyncEvent::Done(None);
    })
}

async fn fetch_raw_stream(
    account: &GraphAccount,
    message: &ObjectId,
) -> Result<bifrost_net::ByteStream, Box<crate::error::GraphError>> {
    // Decode the (possibly foreign-encoded) id so a shared-mailbox raw
    // fetch routes to `/users/{owner}/messages/{native}/$value`; a primary
    // id keeps `/me`.
    let parsed = super::foreign::parse_message_id(message);
    let client = account.client_for_owner(parsed.owner()).map_err(Box::new)?;
    let prefix = client.api_path_prefix();
    let enc_message_id = bifrost_net::url::encode_path_component(parsed.native_id());
    let url = client
        .api_url(&format!("{prefix}/messages/{enc_message_id}/$value"))
        .map_err(Box::new)?;
    client
        .download_stream(Resource::Outlook, &url, None)
        .await
        .map_err(Box::new)
}

async fn fetch_blob_stream(
    account: &GraphAccount,
    locator: &GraphBlobLocator,
    range: Option<ByteRange>,
) -> Result<bifrost_net::ByteStream, BlobFetchError> {
    // The locator's `message_id` is whatever id minted the blob handle: a
    // shared-mailbox attachment inherits the foreign-encoded message id, so
    // decode and route to `/users/{owner}`; a primary attachment keeps
    // `/me`. The attachment id is always native (it has no mailbox of its
    // own - it rides on the message).
    let parsed = super::foreign::parse_message_id(&ObjectId(locator.message_id.clone()));
    let client = account
        .client_for_owner(parsed.owner())
        .map_err(|error| BlobFetchError::Failed(Box::new(error)))?;
    let prefix = client.api_path_prefix();
    let enc_message_id = bifrost_net::url::encode_path_component(parsed.native_id());
    let enc_attachment_id = bifrost_net::url::encode_path_component(&locator.attachment_id);
    let url = client
        .api_url(&format!(
            "{prefix}/messages/{enc_message_id}/attachments/{enc_attachment_id}/$value"
        ))
        .map_err(|error| BlobFetchError::Failed(Box::new(error)))?;
    client
        .download_stream(Resource::Outlook, &url, range)
        .await
        .map_err(BlobFetchError::from)
}

enum BlobFetchError {
    MethodNotAllowed,
    Failed(Box<crate::error::GraphError>),
}

impl From<crate::error::GraphError> for BlobFetchError {
    fn from(error: crate::error::GraphError) -> Self {
        match error {
            crate::error::GraphError::Net(bifrost_net::Error::Status { code, .. })
                if code == reqwest::StatusCode::METHOD_NOT_ALLOWED =>
            {
                Self::MethodNotAllowed
            }
            // Everything else, a client-side failure included, flows
            // through the typed GraphError boundary.
            other => Self::Failed(Box::new(other)),
        }
    }
}

/// `Unsupported(OpenBlobRange)` for a handle that cannot be ranged (a Graph
/// item/reference attachment, or any EWS attachment - `GetAttachment` has no
/// byte-range form).
fn unsupported_range_error() -> bifrost_types::AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::OpenBlobRange),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::OpenBlobRange,
        }),
    )
    .operation(AccountOperation::OpenBlobRange)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(ErrorScope::Account)
    .try_build()
    .expect("valid account error classification")
}

fn decode_locator(handle: &BlobHandle) -> Result<GraphBlobLocator, String> {
    serde_json::from_str(&handle.id.0)
        .map_err(|error| format!("Graph blob handle is not an account blob locator: {error}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::account::{GraphAccount, PushMode};
    use crate::client::GraphClient;
    use bifrost_types::CursorScope;

    /// A blob handle minted from a foreign-encoded message id inherits the
    /// owning mailbox: `blob_handle_from_graph_attachment(&encoded_id, ..)`
    /// stores the encoded id in the locator, so `open_blob` routes the
    /// `/attachments/{aid}/$value` fetch to `/users/{owner}`.
    #[test]
    fn blob_handle_inherits_foreign_message_owner() {
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let encoded = super::super::foreign::encode_message_id(&scope, "AAMkmsg");
        let handle = blob_handle_from_graph_attachment(
            &encoded,
            &json!({
                "id": "att1",
                "@odata.type": "#microsoft.graph.fileAttachment",
                "size": 7
            }),
        )
        .expect("handle expected");
        let locator = decode_locator(&handle).expect("locator");
        // The locator's message id is the encoded id; it decodes back to
        // the owner + native message id.
        let parsed = super::super::foreign::parse_message_id(&ObjectId(locator.message_id));
        assert_eq!(parsed.owner(), Some("shared@contoso.com"));
        assert_eq!(parsed.native_id(), "AAMkmsg");
    }

    #[test]
    fn blob_and_raw_route_foreign_message_to_owner() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let foreign = super::super::foreign::encode_message_id(&scope, "AAMkmsg");
        let parsed = super::super::foreign::parse_message_id(&foreign);
        // Both fetch_raw_stream and fetch_blob_stream build their URL from
        // exactly these two pieces: the owner's prefix + the native id.
        assert_eq!(
            account
                .client_for_owner(parsed.owner())
                .expect("configured")
                .api_path_prefix(),
            "/users/shared%40contoso.com"
        );
        assert_eq!(parsed.native_id(), "AAMkmsg");

        // A primary message id stays on `/me`.
        let primary = ObjectId("AAMkmsg".to_string());
        let parsed_primary = super::super::foreign::parse_message_id(&primary);
        assert_eq!(
            account
                .client_for_owner(parsed_primary.owner())
                .expect("primary")
                .api_path_prefix(),
            "/me"
        );
    }

    #[test]
    fn file_attachment_handle_supports_range() {
        let handle = blob_handle_from_graph_attachment(
            &ObjectId("m1".to_string()),
            &json!({
                "id": "a1",
                "@odata.type": "#microsoft.graph.fileAttachment",
                "size": 42,
                "contentType": "text/plain"
            }),
        )
        .expect("handle expected");
        assert!(handle.capabilities.supports_range);
        assert_eq!(handle.size, Some(42));
    }

    /// Collect a blob/raw stream to completion. Every one of these streams
    /// ends with exactly one `Done`, so a test that never sees it is
    /// looking at a stream that hung or ended early.
    async fn drain(mut stream: AccountStream<SyncEvent<Bytes>>) -> Vec<SyncEvent<Bytes>> {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event);
        }
        events
    }

    /// A handle this crate did not mint is the caller's input, refused before
    /// any request: `Request(Malformed)`, never a provider contract violation,
    /// and nothing reaches the transport.
    #[tokio::test]
    async fn a_malformed_blob_handle_is_a_client_bug_and_sends_nothing() {
        let client = GraphClient::new("token");
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let mut handle = file_handle(&ObjectId("AAMkmsg".to_string()), "att1");
        handle.id = bifrost_types::BlobId("not a locator".to_string());

        let events = drain(open_blob_inner_stream(account, handle, None)).await;
        let Some(SyncEvent::Terminated(error)) = events.first() else {
            panic!("expected a terminal error first, got {events:?}");
        };
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
            ),
            "got {:?}",
            error.kind()
        );
        assert!(client.take_download_requests().is_empty());
    }

    fn file_handle(message: &ObjectId, attachment: &str) -> BlobHandle {
        blob_handle_from_graph_attachment(
            message,
            &json!({
                "id": attachment,
                "@odata.type": "#microsoft.graph.fileAttachment",
                "size": 5
            }),
        )
        .expect("file attachment handle")
    }

    /// The byte-stream leg, end to end through the download seam: the
    /// requested window reaches the transport as the caller's `ByteRange`
    /// (not re-derived, not dropped), the `$value` URL is the attachment's,
    /// and every chunk the transport frames is forwarded as its own
    /// `Batch` rather than being coalesced.
    #[tokio::test]
    async fn a_ranged_blob_read_forwards_every_transport_chunk() {
        let client = GraphClient::new("token");
        // A 206 whose `Content-Range` matches the window requested below.
        // bifrost-net checks the two against each other, so this also
        // pins that the account sent `bytes=2-6` rather than merely
        // recording a range it never put on the wire.
        client.script_downloads([crate::client::ScriptedDownload::PartialChunks {
            content_range: "bytes 2-6/11",
            chunks: vec![Bytes::from_static(b"abc"), Bytes::from_static(b"de")],
        }]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let message = ObjectId("AAMkmsg".to_string());
        let handle = file_handle(&message, "att1");
        let range = ByteRange {
            start: 2,
            length: Some(5),
        };

        let events = drain(open_blob_range_stream(account, handle, range)).await;
        let chunks: Vec<Bytes> = events
            .iter()
            .filter_map(|event| match event {
                SyncEvent::Batch(batch) => Some(batch.items[0].clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            chunks,
            vec![Bytes::from_static(b"abc"), Bytes::from_static(b"de")]
        );
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));

        let requests = client.take_download_requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .url
                .ends_with("/me/messages/AAMkmsg/attachments/att1/$value"),
            "{}",
            requests[0].url
        );
        assert_eq!(requests[0].range, Some(range));
    }

    /// Graph answers `405 Method Not Allowed` for an attachment that has no
    /// `$value` byte stream at all. That is not a failure of the account -
    /// it is "this blob is not a byte stream" - so the stream warns and
    /// completes instead of terminating, which is what keeps a whole
    /// hydration from dying on one unstreamable attachment.
    #[tokio::test]
    async fn a_405_on_the_value_endpoint_warns_instead_of_terminating() {
        let client = GraphClient::new("token");
        // The 405 is what the server answers; the typed `Error::Status`
        // the account then classifies is produced by the transport, not
        // hand-built here.
        client.script_downloads([crate::client::ScriptedDownload::FailedStatus(
            reqwest::StatusCode::METHOD_NOT_ALLOWED,
        )]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let message = ObjectId("AAMkmsg".to_string());
        let handle = file_handle(&message, "att1");

        let events = drain(open_blob_stream(account, handle)).await;
        assert!(
            matches!(events.first(), Some(SyncEvent::Warning(_))),
            "{events:?}"
        );
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
        assert_eq!(client.take_download_requests().len(), 1);
        // A "not a byte stream" answer is never an account failure.
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, SyncEvent::Terminated(_)))
        );
    }

    /// A transport failure that arrives AFTER the stream opened is the one
    /// blob failure mode the status check cannot pre-empt. The chunks that
    /// did arrive stay delivered, and the tail is classified
    /// `Protocol(PartialResponse)` - the caller must not mistake a
    /// truncated blob for a complete one.
    #[tokio::test]
    async fn a_mid_stream_failure_keeps_delivered_chunks_and_terminates_partial() {
        let client = GraphClient::new("token");
        client.script_downloads([crate::client::ScriptedDownload::ChunksThenError(
            vec![Bytes::from_static(b"head")],
            "connection reset mid-body".to_string(),
        )]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let handle = file_handle(&ObjectId("AAMkmsg".to_string()), "att1");

        let events = drain(open_blob_stream(account, handle)).await;
        assert!(matches!(&events[0], SyncEvent::Batch(batch) if batch.items[0] == "head"));
        match &events[1] {
            SyncEvent::Terminated(error) => assert_eq!(
                *error.kind(),
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
            ),
            other => panic!("expected a partial-response termination, got {other:?}"),
        }
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
    }

    /// `open_raw_rfc822` for a foreign-encoded message must open its
    /// `$value` against the OWNER's mailbox. The primary is armed with an
    /// EMPTY download script, so a fetch that fell back to `/me` panics on
    /// exhaustion instead of passing quietly - the URL alone would not
    /// prove routing here, since both clients share nothing else.
    #[tokio::test]
    async fn raw_rfc822_of_a_foreign_message_streams_from_the_owner_mailbox() {
        let primary = GraphClient::new("token");
        primary.script_downloads(std::iter::empty::<crate::client::ScriptedDownload>());
        let shared_root = GraphClient::new("token");
        shared_root.script_downloads([crate::client::ScriptedDownload::Chunks(vec![
            Bytes::from_static(b"From: a@b\r\n"),
        ])]);
        let mailbox = "shared@contoso.com";
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary.clone(),
            PushMode::GraphSubscriptions,
            std::collections::HashMap::from([(
                mailbox.to_string(),
                shared_root.for_shared_mailbox(mailbox),
            )]),
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign(mailbox, "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let foreign = super::super::foreign::encode_message_id(&scope, "AAMkmsg");

        let events = drain(open_raw_rfc822(account, foreign)).await;
        assert!(matches!(&events[0], SyncEvent::Batch(batch) if batch.items[0] == "From: a@b\r\n"));
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
        assert!(primary.take_download_requests().is_empty());
        let requests = shared_root.take_download_requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .url
                .ends_with("/users/shared%40contoso.com/messages/AAMkmsg/$value"),
            "{}",
            requests[0].url
        );
        assert_eq!(requests[0].range, None);
    }

    #[test]
    fn reference_attachment_handle_is_not_byte_stream() {
        let handle = blob_handle_from_graph_attachment(
            &ObjectId("m1".to_string()),
            &json!({
                "id": "a1",
                "@odata.type": "#microsoft.graph.referenceAttachment"
            }),
        )
        .expect("handle expected");
        let locator = decode_locator(&handle).expect("locator expected");
        assert_eq!(locator.kind, GraphBlobKind::Reference);
        assert!(!handle.capabilities.supports_range);
    }
}
