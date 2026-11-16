use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt as _;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BlobHandle, ByteRange, ObjectId, PageBoundary,
    SyncEvent,
};

use crate::blob::BlobRef;
use crate::blob::download::{BlobStream, BlobTransport, DEFAULT_MAX_BLOB_SIZE};
use crate::client::ByteTally;
use crate::core::id::BlobId;
use crate::email::{EmailGet, EmailId, Property};

pub(crate) fn open<T: BlobTransport>(
    foreign_accounts: Arc<super::registry::Registry<T>>,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        // A foreign-qualified blob id names its owning account; download
        // under that account, with the native blobId on the wire.
        let slot = foreign_accounts.object(&handle.id.0);
        let client = slot.client().clone();
        let mut blob = BlobRef::new(slot.id().clone(), BlobId::new(slot.wire(&handle.id.0)));
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
    foreign_accounts: Arc<super::registry::Registry<T>>,
    message: ObjectId,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        // A foreign-qualified message id routes BOTH legs (the `Email/get`
        // for the whole-message blobId and the blob download) to the owning
        // account. Routing only one of them would fetch a blobId from one
        // account and download it from another.
        let mail = foreign_accounts.object(&message.0).clone();
        let account_id = mail.id().clone();
        let client = mail.client().clone();
        let native_message = mail.wire(&message.0).to_owned();
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

        let blob_id = match raw_message_blob(&response, &native_message, &message.0) {
            Ok(blob_id) => blob_id,
            Err(error) => {
                yield super::error::terminated(error);
                return;
            }
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

// An Outlook user must not see an unrelated server object reported as a
// vanished message, or receive that object's body. Only an explicit miss or
// the requested Email without its whole-message blob takes the missing lane.
fn raw_message_blob(
    response: &crate::core::get::GetResponse<crate::email::Email>,
    native_message: &str,
    consumer_message: &str,
) -> Result<BlobId, bifrost_types::AccountError> {
    use crate::core::answer::IdAnswer;

    match response.answer(&EmailId::new(native_message), |email| {
        email.id().is_some_and(|id| id.as_str() == native_message)
    }) {
        IdAnswer::Ok(email) => email
            .blob_id()
            .cloned()
            .ok_or_else(|| super::error::raw_message_not_found(consumer_message)),
        IdAnswer::NotFound(_) => Err(super::error::raw_message_not_found(consumer_message)),
        IdAnswer::Unanswered | IdAnswer::Refused(_) => Err(super::error::contract_violation(
            AccountOperation::OpenRawRfc822,
            Some(bifrost_types::ErrorScope::Message {
                id: consumer_message.to_owned().into(),
            }),
            "Email/get did not answer the requested raw message",
        )),
    }
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
    _handle: BlobHandle,
    _range: ByteRange,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        // There is no Range-capable transport hook. Refuse the operation
        // independently of handle metadata or caller arguments, matching the
        // account's BlobRangeSupport::No promise.
        yield super::error::terminated_unsupported(
            AccountOperation::OpenBlobRange,
            None,
            bifrost_types::UnsupportedReason::RangeTransport,
            "JMAP ranged blob download needs a Range-capable transport hook",
        );
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use bifrost_types::{PageBoundary, SyncEvent};
    use futures::StreamExt as _;

    #[tokio::test]
    async fn ranged_reads_are_unsupported_regardless_of_metadata_or_start() {
        for supports_range in [false, true] {
            for size in [None, Some(0), Some(10)] {
                for start in [0, 9, 10, 11, u64::MAX] {
                    let events = super::open_range(
                        bifrost_types::BlobHandle {
                            id: bifrost_types::BlobId("B1".into()),
                            size,
                            content_type: None,
                            digest: None,
                            capabilities: bifrost_types::BlobCapabilities {
                                supports_range,
                                supports_parallel: false,
                                digest_available_pre_download: false,
                                encoding: bifrost_types::BlobEncoding::Raw8Bit,
                            },
                        },
                        bifrost_types::ByteRange {
                            start,
                            length: Some(1),
                        },
                    )
                    .collect::<Vec<_>>()
                    .await;
                    let [SyncEvent::Terminated(error)] = events.as_slice() else {
                        panic!("expected one terminal refusal, got {events:?}");
                    };
                    assert_eq!(
                        error.kind(),
                        &bifrost_types::AccountErrorKind::Unsupported(
                            bifrost_types::AccountOperation::OpenBlobRange,
                        )
                    );
                }
            }
        }
    }

    #[test]
    fn raw_message_reads_distinguish_missing_content_from_unrelated_answers() {
        use bifrost_types::{AccountErrorKind, ProtocolErrorKind, ResourceKind};

        for (list, not_found, expected) in [
            (
                serde_json::json!([{"id": "M1"}]),
                serde_json::json!([]),
                AccountErrorKind::NotFound(ResourceKind::Message),
            ),
            (
                serde_json::json!([]),
                serde_json::json!(["M1"]),
                AccountErrorKind::NotFound(ResourceKind::Message),
            ),
            (
                serde_json::json!([{"id": "other", "blobId": "B2"}]),
                serde_json::json!([]),
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            ),
            (
                serde_json::json!([]),
                serde_json::json!([]),
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            ),
        ] {
            let response = serde_json::from_value(serde_json::json!({
                "accountId": "primary", "state": "s1", "list": list, "notFound": not_found
            }))
            .unwrap();
            let error = super::raw_message_blob(&response, "M1", "M1").unwrap_err();
            assert_eq!(error.kind(), &expected);
        }
    }

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
            _replay: bifrost_net::ReplayPolicy,
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
            replay: bifrost_net::ReplayPolicy,
        ) -> Result<(bytes::Bytes, u64), crate::core::transport::TransportError> {
            Ok((self.api_request(url, body, replay).await?, API_BYTES_IN))
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
            std::sync::Arc::new(crate::sync::registry::Registry::from_test(
                crate::sync::registry::Slot::new(
                    metered_client(),
                    crate::core::id::AccountId::new("primary"),
                ),
                std::sync::Arc::new(std::collections::HashMap::<
                    crate::core::id::AccountId,
                    crate::sync::registry::Slot<_>,
                >::new()),
            )),
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
        let mail = crate::sync::registry::Slot::new(client.clone(), "primary");
        let events = super::open_raw_rfc822(
            std::sync::Arc::new(crate::sync::registry::Registry::from_test(
                mail,
                std::sync::Arc::new(std::collections::HashMap::<
                    crate::core::id::AccountId,
                    crate::sync::registry::Slot<_>,
                >::new()),
            )),
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
}
