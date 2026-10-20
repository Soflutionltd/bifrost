use crate::{
    blob::BlobRef,
    client::Client,
    core::{
        session::{URLPart, encode_template_value_at},
        transport::HttpTransport,
    },
};

/// Blob buffering has its own bound, independent of small JSON responses.
/// Streaming callers choose their own ceiling without allocating that ceiling.
pub(crate) const DEFAULT_MAX_BLOB_SIZE: u64 = 512 * 1024 * 1024;

pub(crate) type BlobStream = std::pin::Pin<
    Box<
        dyn futures::Stream<Item = Result<bytes::Bytes, crate::core::transport::TransportError>>
            + Send,
    >,
>;

pub(crate) struct BlobDownload {
    pub(crate) body: BlobStream,
    pub(crate) bytes_in: bifrost_net::RequestByteCounter,
}

/// Kept separate from HttpTransport so buffered custom transports retain
/// their existing contract without pretending to support incremental reads.
pub(crate) trait BlobTransport: HttpTransport {
    fn download_stream(
        &self,
        url: &str,
        max_bytes: u64,
    ) -> impl std::future::Future<
        Output = Result<BlobDownload, crate::core::transport::TransportError>,
    > + Send;
}

pub(crate) fn bounded_stream(mut source: bifrost_net::ByteStream, max_bytes: u64) -> BlobStream {
    Box::pin(async_stream::stream! {
        use futures::StreamExt;
        let mut remaining = max_bytes;
        while let Some(chunk) = source.next().await {
            match chunk {
                Ok(chunk) => {
                    let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
                    if len > remaining {
                        yield Err(crate::core::transport::TransportError::limit_exceeded(
                            format!("blob exceeds the {max_bytes} byte download ceiling"),
                        ));
                        break;
                    }
                    remaining -= len;
                    yield Ok(chunk);
                }
                Err(error) => {
                    yield Err(crate::core::transport::TransportError::from_net(error));
                    break;
                }
            }
        }
    })
}

impl<Tr: BlobTransport> Client<Tr> {
    /// Download incrementally, rejecting a chunk that would exceed the caller's
    /// ceiling. A failure after earlier chunks leaves an incomplete blob; the
    /// caller must discard it. Dropping the stream cancels the body read.
    pub(crate) async fn download_stream(
        &self,
        blob: &BlobRef,
        max_bytes: u64,
    ) -> crate::Result<BlobStream> {
        let download = self
            .transport()
            .download_stream(&self.blob_download_url(blob), max_bytes)
            .await
            .map_err(crate::Error::from)?;
        let client = self.clone();
        Ok(Box::pin(async_stream::stream! {
            use futures::StreamExt;
            let mut body = download.body;
            let mut recorded = 0;
            loop {
                let chunk = body.next().await;
                let total = download.bytes_in.bytes_in();
                client.record_bytes_in(total.saturating_sub(recorded));
                recorded = total;
                match chunk {
                    Some(chunk) => yield chunk,
                    None => break,
                }
            }
        }))
    }
}

impl<Tr: HttpTransport> Client<Tr> {
    /// Download a blob.
    ///
    /// The account ID, blob ID, and (when the server's download URL
    /// template references them) `name` and `type` come from the
    /// `BlobRef`. RFC 8620 §6 specifies `accountId` as required, and
    /// `name` / `type` as advisory hints used to populate
    /// `Content-Disposition` and `Content-Type` on the response. When
    /// the ref leaves them as `None` and the template needs a value,
    /// neutral defaults (`"download"`, `"application/octet-stream"`)
    /// are substituted.
    ///
    /// Every substituted value is percent-encoded per RFC 6570 §3.2.2
    /// simple string expansion, so a content type like `"image/png"` or
    /// `"application/ld+json"` reaches the server intact rather than
    /// splitting a path segment or decoding `+` as a space.
    pub(crate) async fn download(&self, blob: &BlobRef) -> crate::Result<bytes::Bytes> {
        let download_url = self.blob_download_url(blob);
        let (bytes, bytes_in) = self
            .transport()
            .download_measured(&download_url)
            .await
            .map_err(crate::Error::from)?;
        self.record_bytes_in(bytes_in);
        Ok(bytes)
    }

    fn blob_download_url(&self, blob: &BlobRef) -> String {
        let state = self.session_state();
        let mut download_url = String::with_capacity(state.session().download_url().len() + 64);

        for part in state.download_url() {
            match part {
                URLPart::Value(value) => download_url.push_str(value),
                URLPart::Parameter(param) => match param {
                    super::URLParameter::AccountId => {
                        download_url.push_str(&encode_template_value_at(
                            blob.account_id.as_str(),
                            &download_url,
                        ));
                    }
                    super::URLParameter::BlobId => {
                        download_url.push_str(&encode_template_value_at(
                            blob.blob_id.as_str(),
                            &download_url,
                        ));
                    }
                    super::URLParameter::Name => {
                        let name = blob.name.as_deref().unwrap_or("download");
                        download_url.push_str(&encode_template_value_at(name, &download_url));
                    }
                    super::URLParameter::Type => {
                        let ctype = blob
                            .content_type
                            .as_deref()
                            .unwrap_or("application/octet-stream");
                        download_url.push_str(&encode_template_value_at(ctype, &download_url));
                    }
                },
            }
        }

        download_url
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use serde_json::json;

    use crate::{
        blob::BlobRef,
        client::Client,
        core::{
            id::{AccountId, BlobId},
            session::Session,
            transport::{HttpTransport, TransportError},
        },
    };

    #[tokio::test]
    async fn blob_ceiling_accepts_exact_size_and_stops_after_overflow() {
        use futures::StreamExt;
        let source = || {
            Box::pin(futures::stream::iter([
                Ok(Bytes::from_static(b"ab")),
                Ok(Bytes::from_static(b"cd")),
                Ok(Bytes::from_static(b"e")),
                Ok(Bytes::from_static(b"must not escape")),
            ])) as bifrost_net::ByteStream
        };
        let mut stream = super::bounded_stream(source(), 4);
        assert_eq!(stream.next().await.unwrap().unwrap(), b"ab"[..]);
        assert_eq!(stream.next().await.unwrap().unwrap(), b"cd"[..]);
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
        let mut zero = super::bounded_stream(source(), 0);
        assert!(zero.next().await.unwrap().is_err());
        assert!(zero.next().await.is_none());
    }

    #[tokio::test]
    async fn blob_stream_ends_after_a_body_error() {
        use futures::StreamExt;
        let source = Box::pin(futures::stream::iter([
            Err(bifrost_net::Error::Status {
                code: reqwest::StatusCode::BAD_REQUEST,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            }),
            Ok(Bytes::from_static(b"must not escape")),
        ]));
        let mut stream = super::bounded_stream(source, 100);
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
    }

    /// Captures the URL the client asked to download from.
    struct UrlCapture(Arc<Mutex<Option<String>>>);

    impl HttpTransport for UrlCapture {
        async fn api_request(&self, _url: &str, _body: Vec<u8>) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not call the API"))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not upload"))
        }

        async fn download(&self, url: &str) -> Result<Bytes, TransportError> {
            *self.0.lock().expect("capture lock") = Some(url.to_string());
            Ok(Bytes::from_static(b"blob"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport has no session route"))
        }
    }

    fn session() -> Session {
        serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}?name={name}&type={type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    /// RFC 6570 §3.2.2: everything outside the unreserved set is
    /// percent-encoded. A deny list that stops at `&` and `=` leaves
    /// `+`, `;`, `@`, `!`, `,` and friends to be reinterpreted by the
    /// server - `application/ld+json` would arrive as
    /// `application/ld json`.
    #[tokio::test]
    async fn template_values_are_rfc6570_level_one_encoded() {
        let captured = Arc::new(Mutex::new(None));
        let client = Client::with_transport(
            UrlCapture(Arc::clone(&captured)),
            session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        let blob = BlobRef::new(AccountId::new("a c!count"), BlobId::new("b;lob@1"))
            .with_name("re,port+final.txt")
            .with_content_type("application/ld+json");

        client.download(&blob).await.expect("download runs");

        let url = captured.lock().expect("capture lock").clone().expect("url");
        assert_eq!(
            url,
            "https://example.test/dl/a%20c%21count/b%3Blob%401\
             ?name=re%2Cport%2Bfinal.txt&type=application%2Fld%2Bjson"
        );
    }
}
