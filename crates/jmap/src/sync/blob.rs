use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt as _;

use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream, Batch, BlobHandle,
    ByteRange, Cause, ObjectId, PageBoundary, Protocol, RequestCause, ResourceKind, SyncEvent,
};

use crate::blob::BlobRef;
use crate::blob::download::{BlobStream, BlobTransport, DEFAULT_MAX_BLOB_SIZE};
use crate::client::{ByteTally, Client};
use crate::core::id::{AccountId, BlobId};
use crate::email::{EmailGet, EmailId, Property};

type MailAccount<T> = crate::account::Account<T>;

/// Resolve the JMAP `accountId` a foreign-qualified id belongs to.
///
/// Blob download is accountId-scoped (`/download/{accountId}/{blobId}`), so
/// a foreign (shared/delegate) blob must be requested under its OWNING
/// account - the primary account's download URL would 404 on a blob it does
/// not own. `Some((accountId, native))` when the id was qualified by the
/// foreign inventory projection AND that account is still reachable;
/// `None` for a primary id (or an id naming an account that vanished, which
/// falls back to the primary account so the miss surfaces as a real 404
/// rather than a fabricated local error). Pure so the selection is
/// unit-pinnable without a live session.
pub(crate) fn foreign_split<F>(id: &str, is_registered: F) -> Option<(&str, &str)>
where
    F: Fn(&str) -> bool,
{
    super::foreign::parse_object(id).filter(|(account, _)| is_registered(account))
}

pub(crate) fn open<T: BlobTransport>(
    client: Client<T>,
    account_id: AccountId,
    foreign_accounts: Arc<HashMap<String, MailAccount<T>>>,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        // A foreign-qualified blob id names its owning account; download
        // under that account, with the native blobId on the wire.
        let (account_id, native_blob) =
            match foreign_split(&handle.id.0, |account| foreign_accounts.contains_key(account)) {
                Some((account, native)) => (AccountId::new(account), native.to_string()),
                None => (account_id, handle.id.0.clone()),
            };
        let mut blob = BlobRef::new(account_id, BlobId::new(native_blob));
        if let Some(content_type) = handle.content_type {
            blob = blob.with_content_type(content_type);
        }

        let (client, tally) = client.metered();
        let body = match client.download_stream(&blob, DEFAULT_MAX_BLOB_SIZE).await {
            Ok(body) => body,
            Err(err) => {
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::new(AccountOperation::OpenBlob),
                );
                return;
            }
        };

        let mut events = download_events(
            body,
            tally,
            started,
            super::error::JmapErrorContext::new(AccountOperation::OpenBlob),
        );
        while let Some(event) = events.next().await {
            yield event;
        }
    })
}

/// Open a message's assembled RFC822 octets.
///
/// One `Email/get` for the whole-message `blobId`, then a streamed download
/// of that blob. The `hydrate.rs` raw-projection fatal is intentionally
/// untouched: hydration's raw projection is A1's concern; this is the
/// dedicated raw read.
pub(crate) fn open_raw_rfc822<T: BlobTransport>(
    client: Client<T>,
    account_id: AccountId,
    mail: MailAccount<T>,
    foreign_accounts: Arc<HashMap<String, MailAccount<T>>>,
    message: ObjectId,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        // A foreign-qualified message id routes BOTH legs (the `Email/get`
        // for the whole-message blobId and the blob download) to the owning
        // account. Routing only one of them would fetch a blobId from one
        // account and download it from another.
        let (account_id, mail, native_message) =
            match foreign_split(&message.0, |account| foreign_accounts.contains_key(account)) {
                Some((account, native)) => (
                    AccountId::new(account),
                    foreign_accounts
                        .get(account)
                        .cloned()
                        .expect("foreign_split only returns a registered account"),
                    native.to_string(),
                ),
                None => (account_id, mail, message.0.clone()),
            };
        // Both legs report into one accumulator: the `Email/get` that
        // resolves the whole-message blobId and the streamed download are
        // traffic this read caused. The API response count is attached to
        // the first emitted chunk batch (or the empty final batch), then each
        // download delta follows its own chunk.
        let tally = crate::client::ByteTally::default();
        let client = client.metered_into(&tally);
        let mail = mail.metered_into(&tally);
        let response = match mail
            .call(
                EmailGet::new()
                    .ids([EmailId::new(native_message.clone())])
                    // `Id` is requested explicitly so the correlation below
                    // has something to match on.
                    .properties([Property::Id, Property::BlobId]),
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                // Email absent: the transport surfaces `IdNotFound`,
                // which `message(op, id)` classifies as
                // `NotFound(ResourceKind::Message)` via the scoped
                // `ErrorScope::Message`.
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::message(
                        AccountOperation::OpenRawRfc822,
                        message.0.clone(),
                    ),
                );
                return;
            }
        };

        // Correlate on the id we submitted rather than taking the head of the
        // echoed list. One id was requested, so positional trust happens to be
        // safe today, but it is the pattern `reconcile_hydration` was written
        // to eliminate: a server echoing an unrelated object would have ITS
        // blobId downloaded and returned as this caller's message body. An
        // uncorrelated response falls through to the NotFound below.
        let blob_id = response
            .into_list()
            .into_iter()
            .find(|email| email.id().is_some_and(|id| id.as_str() == native_message))
            .and_then(|email| email.blob_id().cloned());
        let Some(blob_id) = blob_id else {
            // Email present but `blobId` is `None`: no transport error to
            // convert, so build the NotFound directly. `terminated_unsupported`
            // would be wrong - the operation is supported; the
            // whole-message blob is missing.
            let err = AccountErrorBuilder::new(
                AccountErrorKind::NotFound(ResourceKind::Message),
                Cause::Request(RequestCause::NotFound {
                    what: ResourceKind::Message,
                    id: Some(message.0.clone()),
                }),
            )
            .operation(AccountOperation::OpenRawRfc822)
            .protocol(Protocol::Jmap)
            .try_build()
            .expect("valid account error classification");
            yield super::error::terminated(err);
            return;
        };

        let blob = BlobRef::new(account_id, blob_id);
        let body = match client
            .download_stream(&blob, DEFAULT_MAX_BLOB_SIZE)
            .await
        {
            Ok(body) => body,
            Err(err) => {
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::message(
                        AccountOperation::OpenRawRfc822,
                        message.0.clone(),
                    ),
                );
                return;
            }
        };

        let mut events = download_events(
            body,
            tally,
            started,
            super::error::JmapErrorContext::message(
                AccountOperation::OpenRawRfc822,
                message.0.clone(),
            ),
        );
        while let Some(event) = events.next().await {
            yield event;
        }
    })
}

/// Turn a streamed blob response into engine batches without retaining the
/// whole response. One prior transport chunk is held while polling its
/// successor so the final successful chunk can carry `PageBoundary::Final`;
/// the inbound count taken after reading each chunk stays attached to that
/// chunk even while the next one is read ahead.
fn download_events(
    body: BlobStream,
    tally: ByteTally,
    started: Instant,
    context: super::error::JmapErrorContext,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let mut body = body;
        let mut pending: Option<(bytes::Bytes, u64)> = None;
        let mut batch_started = started;

        loop {
            let next = body.next().await;
            // `Client::download_stream` updates this tally as it polls the
            // transport. Taking after every poll partitions request-local
            // bytes across the emitted batches instead of repeating a running
            // total on each chunk.
            let bytes_in = tally.take();
            match next {
                Some(Ok(chunk)) => {
                    if let Some((previous, previous_bytes_in)) =
                        pending.replace((chunk, bytes_in))
                    {
                        yield SyncEvent::Batch(Batch {
                            items: vec![previous],
                            page_boundary: PageBoundary::Partial,
                            server_latency: batch_started.elapsed(),
                            bytes_in: previous_bytes_in,
                            checkpoint: None,
                        });
                        batch_started = Instant::now();
                    }
                }
                Some(Err(error)) => {
                    if let Some((partial, partial_bytes_in)) = pending.take() {
                        yield SyncEvent::Batch(Batch {
                            items: vec![partial],
                            page_boundary: PageBoundary::Partial,
                            server_latency: batch_started.elapsed(),
                            bytes_in: partial_bytes_in,
                            checkpoint: None,
                        });
                    }
                    // Earlier chunks are incomplete when the body fails. The
                    // terminal event tells the consumer to discard them.
                    yield super::error::terminated_from_jmap(error.into(), context);
                    return;
                }
                None => {
                    // Preserve the old successful empty-blob shape and carry
                    // API/get bytes for an empty raw-message body.
                    let (last, last_bytes_in) =
                        pending.take().unwrap_or_else(|| (bytes::Bytes::new(), bytes_in));
                    yield SyncEvent::Batch(Batch {
                        items: vec![last],
                        page_boundary: PageBoundary::Final,
                        server_latency: batch_started.elapsed(),
                        bytes_in: last_bytes_in,
                        checkpoint: None,
                    });
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }
    })
}

pub(crate) fn open_range(
    handle: BlobHandle,
    range: ByteRange,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        if let (Some(total), start) = (handle.size, range.start)
            && start >= total
        {
            // A start past the known size is a caller argument fault, not a
            // capability gap: `Request(Malformed)` derives to `ClientBug`,
            // where `Unsupported` would tell the engine the protocol has no
            // ranged read at all and suppress the operation wholesale.
            let err = AccountErrorBuilder::new(
                AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("range.start"),
                    message: Some(bifrost_types::DiagnosticText::support_only(format!(
                        "JMAP blob range starts past the known blob size (start {start}, total {total})"
                    ))),
                }),
            )
            .operation(AccountOperation::OpenBlobRange)
            .protocol(Protocol::Jmap)
            .try_build()
            .expect("valid account error classification");
            yield super::error::terminated(err);
            return;
        }

        if !handle.capabilities.supports_range {
            yield super::error::terminated_unsupported(
                AccountOperation::OpenBlobRange,
                None,
                "JMAP blob handle does not support range fetches",
            );
            return;
        }

        yield super::error::terminated_unsupported(
            AccountOperation::OpenBlobRange,
            None,
            "JMAP ranged blob download needs a Range-capable transport hook",
        );
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use super::foreign_split;
    use bifrost_types::{PageBoundary, SyncEvent};
    use futures::StreamExt as _;

    /// A boundary that answers one `Email/get` and then streams its blob.
    struct MeteredBlobTransport;

    /// What the API leg reports as its request-local inbound total.
    const API_BYTES_IN: u64 = 4_096;
    const RAW: &[u8] = b"From: a@example.test\r\n\r\nbody";

    impl crate::core::transport::HttpTransport for MeteredBlobTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value =
                serde_json::from_slice(&body).expect("the client emits JSON");
            let call = &request["methodCalls"][0];
            assert_eq!(call[0], "Email/get");
            let call_id = call[2].clone();
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[
                    "Email/get",
                    {
                        "accountId": "primary",
                        "state": "s1",
                        "list": [{"id": "M1", "blobId": "B1"}],
                        "notFound": []
                    },
                    call_id
                ]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn api_request_measured(
            &self,
            url: &str,
            body: Vec<u8>,
        ) -> Result<(bytes::Bytes, u64), crate::core::transport::TransportError> {
            Ok((self.api_request(url, body).await?, API_BYTES_IN))
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
            Err(crate::core::transport::TransportError::new(
                "the sync blob facade must use the streaming door",
            ))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    impl crate::blob::download::BlobTransport for MeteredBlobTransport {
        async fn download_stream(
            &self,
            _url: &str,
            max_bytes: u64,
        ) -> Result<crate::blob::download::BlobDownload, crate::core::transport::TransportError>
        {
            assert!(RAW.len() as u64 <= max_bytes);
            let chunks = RAW
                .chunks(5)
                .map(bytes::Bytes::copy_from_slice)
                .collect::<Vec<_>>();
            let body = futures::stream::iter(
                chunks
                    .into_iter()
                    .map(Ok::<_, crate::core::transport::TransportError>),
            );
            Ok(crate::blob::download::BlobDownload {
                body: Box::pin(body),
                bytes_in: bifrost_net::RequestByteCounter::new(),
            })
        }
    }

    fn metered_client() -> crate::client::Client<MeteredBlobTransport> {
        let session: crate::core::session::Session = serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 2,
                    "maxObjectsInSet": 2,
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
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses");
        crate::client::Client::with_transport(
            MeteredBlobTransport,
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds")
    }

    fn batches(events: &[SyncEvent<bytes::Bytes>]) -> Vec<&bifrost_types::Batch<bytes::Bytes>> {
        events
            .iter()
            .filter_map(|event| match event {
                SyncEvent::Batch(batch) => Some(batch),
                _ => None,
            })
            .collect()
    }

    /// The facade uses the streaming door, emits transport chunks before it
    /// has read the entire body, and marks only the last chunk final.
    #[tokio::test]
    async fn a_blob_read_emits_incremental_chunk_batches() {
        let handle = bifrost_types::BlobHandle {
            id: bifrost_types::BlobId("B1".to_string()),
            size: None,
            content_type: None,
            digest: None,
            capabilities: bifrost_types::BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: bifrost_types::BlobEncoding::Raw8Bit,
            },
        };
        let events = super::open(
            metered_client(),
            crate::core::id::AccountId::new("primary"),
            std::sync::Arc::new(std::collections::HashMap::new()),
            handle,
        )
        .collect::<Vec<_>>()
        .await;

        let batches = batches(&events);
        assert_eq!(batches.len(), RAW.chunks(5).count());
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.items.iter())
                .flat_map(|chunk| chunk.iter().copied())
                .collect::<Vec<_>>(),
            RAW,
        );
        assert!(
            batches[..batches.len() - 1]
                .iter()
                .all(|batch| batch.page_boundary == PageBoundary::Partial)
        );
        assert_eq!(
            batches.last().map(|batch| batch.page_boundary),
            Some(PageBoundary::Final),
        );
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
    }

    /// A raw RFC822 read is two requests. The Email/get bytes are attributed
    /// once, to the first streamed chunk batch, and are not repeated on later
    /// batches.
    #[tokio::test]
    async fn a_raw_read_accounts_for_the_api_leg_once_across_chunk_batches() {
        let client = metered_client();
        let mail = crate::account::Account::new(client.clone(), "primary");
        let events = super::open_raw_rfc822(
            client,
            crate::core::id::AccountId::new("primary"),
            mail,
            std::sync::Arc::new(std::collections::HashMap::new()),
            bifrost_types::ObjectId("M1".to_string()),
        )
        .collect::<Vec<_>>()
        .await;

        let batches = batches(&events);
        assert_eq!(batches.len(), RAW.chunks(5).count());
        assert_eq!(batches[0].bytes_in, API_BYTES_IN);
        assert!(batches[1..].iter().all(|batch| batch.bytes_in == 0));
        assert_eq!(
            batches.iter().map(|batch| batch.bytes_in).sum::<u64>(),
            API_BYTES_IN,
        );
    }

    #[tokio::test]
    async fn an_empty_stream_preserves_the_api_count_in_its_final_batch() {
        let (metered, tally) = metered_client().metered();
        metered.record_bytes_in(API_BYTES_IN);
        let body = futures::stream::empty::<
            Result<bytes::Bytes, crate::core::transport::TransportError>,
        >();
        let events = super::download_events(
            Box::pin(body),
            tally,
            Instant::now(),
            super::super::error::JmapErrorContext::new(bifrost_types::AccountOperation::OpenBlob),
        )
        .collect::<Vec<_>>()
        .await;

        let [SyncEvent::Batch(batch), SyncEvent::Done(None)] = events.as_slice() else {
            panic!("expected an empty final batch and Done, got {events:?}");
        };
        assert_eq!(batch.items, vec![bytes::Bytes::new()]);
        assert_eq!(batch.bytes_in, API_BYTES_IN);
        assert_eq!(batch.page_boundary, PageBoundary::Final);
    }

    /// The stream helper takes a fresh tally delta for each chunk. Its
    /// one-chunk lookahead still yields before it drains the full body.
    #[tokio::test]
    async fn streamed_batches_partition_byte_counts_per_chunk() {
        let (metered, tally) = metered_client().metered();
        let polled = Arc::new(AtomicUsize::new(0));
        let source_polled = Arc::clone(&polled);
        let chunks = VecDeque::from([
            (bytes::Bytes::from_static(b"first"), 11),
            (bytes::Bytes::from_static(b"second"), 13),
            (bytes::Bytes::from_static(b"third"), 17),
        ]);
        let body = futures::stream::unfold(
            (chunks, metered, source_polled),
            |(mut chunks, client, polled)| async move {
                let (chunk, bytes_in) = chunks.pop_front()?;
                polled.fetch_add(1, Ordering::Relaxed);
                client.record_bytes_in(bytes_in);
                Some((
                    Ok::<_, crate::core::transport::TransportError>(chunk),
                    (chunks, client, polled),
                ))
            },
        );
        let mut events = super::download_events(
            Box::pin(body),
            tally,
            Instant::now(),
            super::super::error::JmapErrorContext::new(bifrost_types::AccountOperation::OpenBlob),
        );

        let first = events.next().await.expect("first batch");
        let SyncEvent::Batch(first) = first else {
            panic!("expected a batch, got {first:?}");
        };
        assert_eq!(first.items, vec![bytes::Bytes::from_static(b"first")]);
        assert_eq!(first.bytes_in, 11);
        assert_eq!(first.page_boundary, PageBoundary::Partial);
        assert_eq!(polled.load(Ordering::Relaxed), 2);

        let rest = events.collect::<Vec<_>>().await;
        let rest_batches = rest
            .iter()
            .filter_map(|event| match event {
                SyncEvent::Batch(batch) => Some(batch),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(rest_batches.len(), 2);
        assert_eq!(
            rest_batches[0].items,
            vec![bytes::Bytes::from_static(b"second")]
        );
        assert_eq!(rest_batches[0].bytes_in, 13);
        assert_eq!(rest_batches[0].page_boundary, PageBoundary::Partial);
        assert_eq!(
            rest_batches[1].items,
            vec![bytes::Bytes::from_static(b"third")]
        );
        assert_eq!(rest_batches[1].bytes_in, 17);
        assert_eq!(rest_batches[1].page_boundary, PageBoundary::Final);
        assert!(matches!(rest.last(), Some(SyncEvent::Done(None))));
    }

    /// A body failure after a delivered prefix must remain terminal, so the
    /// consumer knows the streamed octets do not form a complete blob.
    #[tokio::test]
    async fn a_stream_error_terminates_after_marking_the_prefix_partial() {
        let (metered, tally) = metered_client().metered();
        let body = futures::stream::iter([
            Ok(bytes::Bytes::from_static(b"prefix")),
            Err(crate::core::transport::TransportError::limit_exceeded(
                "test response limit",
            )),
        ])
        .map(move |chunk| {
            metered.record_bytes_in(7);
            chunk
        });
        let events = super::download_events(
            Box::pin(body),
            tally,
            Instant::now(),
            super::super::error::JmapErrorContext::new(bifrost_types::AccountOperation::OpenBlob),
        )
        .collect::<Vec<_>>()
        .await;

        assert!(matches!(
            events.as_slice(),
            [
                SyncEvent::Batch(bifrost_types::Batch {
                    page_boundary: PageBoundary::Partial,
                    ..
                }),
                SyncEvent::Terminated(_)
            ]
        ));
    }

    #[test]
    fn foreign_blob_id_selects_the_owning_account() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |account: &str| registered.contains(account);

        let foreign = super::super::foreign::encode_object("acct-9", "B1");
        // The download runs under the FOREIGN accountId with the native
        // blobId on the wire.
        assert_eq!(
            foreign_split(&foreign, is_registered),
            Some(("acct-9", "B1"))
        );

        // A bare (primary) id keeps the primary account.
        assert_eq!(foreign_split("B1", is_registered), None);
        // An id naming an unreachable account falls back to primary, so the
        // miss surfaces as a real download 404.
        let gone = super::super::foreign::encode_object("acct-gone", "B1");
        assert_eq!(foreign_split(&gone, is_registered), None);
    }
}
