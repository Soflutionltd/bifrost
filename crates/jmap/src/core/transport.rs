use std::future::Future;

use bytes::Bytes;

/// Transport-level error. Wraps the underlying HTTP client error
/// without leaking it into the crate's public error model.
// types: crate-owned so custom transports can still carry response bodies for ProblemDetails.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) struct TransportError {
    pub(crate) message: String,
    /// HTTP response body, if available (for parsing ProblemDetails).
    pub(crate) body: Option<Bytes>,
    /// Original net error, when the default reqwest-backed transport
    /// produced this `TransportError`. Custom transports leave this
    /// `None`; the conversion boundary falls back to a generic
    /// `Transport(Network)` classification when absent.
    pub(crate) net: Option<bifrost_net::Error>,
    /// Set only when this crate failed locally rather than on the wire: an
    /// invariant broken before anything was sent, or a client-side safety
    /// limit tripped while reading a response. Authoritative over the
    /// no-`net` fallback: without it, a local failure fell through to
    /// `Transport(Network)` and was retried as a network fault forever.
    /// Only the dedicated constructors set it, so a passed-through 3xx
    /// (`with_body`) or a test stub's missing reply keeps the fallback.
    pub(crate) local: Option<LocalFailure>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// What failed locally in a [`TransportError`]. Semantic rather than a flag,
/// so each variant maps to its own `Internal` kind and no classification
/// ever keys on the message text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalFailure {
    /// A state the transport believes impossible by construction.
    InvariantViolated,
    /// A bound this crate imposes on a response the server is free to make
    /// arbitrarily large, where the protocol itself sets no limit (the
    /// EventSource block cap). The server broke no rule, so this is not a
    /// provider fault, and the same stream replayed trips the same bound, so
    /// it is not a network fault to retry either: `Internal(LimitExceeded)`.
    LimitExceeded,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl TransportError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            body: None,
            net: None,
            local: None,
            source: None,
        }
    }

    pub(crate) fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            body: None,
            net: None,
            local: None,
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn with_body(message: impl Into<String>, body: impl Into<Bytes>) -> Self {
        Self {
            message: message.into(),
            body: Some(body.into()),
            net: None,
            local: None,
            source: None,
        }
    }

    /// A local state this crate believes impossible by construction, raised
    /// before anything was sent: `Internal(InvariantViolated)`, never a
    /// network fault.
    pub(crate) fn invariant(message: impl Into<String>) -> Self {
        Self {
            local: Some(LocalFailure::InvariantViolated),
            ..Self::new(message)
        }
    }

    /// As [`TransportError::invariant`], keeping the underlying error.
    pub(crate) fn invariant_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            local: Some(LocalFailure::InvariantViolated),
            ..Self::with_source(message, source)
        }
    }

    /// A client-side safety limit on a response the protocol leaves
    /// unbounded: `Internal(LimitExceeded)`, never a network fault.
    pub(crate) fn limit_exceeded(message: impl Into<String>) -> Self {
        Self {
            local: Some(LocalFailure::LimitExceeded),
            ..Self::new(message)
        }
    }

    /// Construct a `TransportError` from a `bifrost_net::Error`,
    /// preserving the original net error so the JMAP conversion
    /// boundary can delegate to `bifrost_net::into_account_error`
    /// for pure transport failures.
    pub(crate) fn from_net(error: bifrost_net::Error) -> Self {
        let message = error.to_string();
        match error {
            bifrost_net::Error::Status {
                code,
                body,
                headers,
            } => Self {
                message: format!("HTTP {code}"),
                body: Some(body.clone()),
                net: Some(bifrost_net::Error::Status {
                    code,
                    body,
                    headers,
                }),
                local: None,
                source: None,
            },
            other => Self {
                message,
                body: preserved_response_body(&other),
                net: Some(other),
                local: None,
                source: None,
            },
        }
    }
}

/// The response body a non-`Status` net error still carries.
///
/// `Error::Status` is the only variant bifrost-net produces for a
/// *terminal* HTTP status, and a 429 or 5xx never takes that path: the
/// retry loop consumes those and surfaces `RateLimited` /
/// `RetryBudgetExhausted` with the final attempt's response preserved
/// on the error instead. Those are exactly the statuses a JMAP server
/// answers with an RFC 7807 problem document (RFC 8620 s3.6.1), so the
/// body has to be lifted back out of the evidence or the JMAP boundary
/// never sees the provider's own explanation of the failure.
///
/// `AuthLost` is deliberately not lifted. Its 401 classification and
/// its `transmission_state` evidence are bifrost-net's to interpret,
/// and routing it through the problem-details branch would replace a
/// typed auth failure with a status-derived guess.
fn preserved_response_body(error: &bifrost_net::Error) -> Option<Bytes> {
    match error {
        bifrost_net::Error::UnexpectedResponse { response } => Some(response.body.clone()),
        bifrost_net::Error::RateLimited { final_response, .. } => Some(final_response.body.clone()),
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            ..
        } => Some(final_response.body.clone()),
        _ => None,
    }
}

/// HTTP transport abstraction.
///
/// Implement this trait to use a custom HTTP client. The default
/// implementation uses `reqwest`.
pub(crate) trait HttpTransport: Send + Sync + 'static {
    /// Send a JMAP API request (POST with JSON body).
    fn api_request(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> impl Future<Output = Result<Bytes, TransportError>> + Send;

    /// Send a JMAP API request, also reporting the inbound payload
    /// bytes the transport read to produce the answer.
    ///
    /// Defaulted so no implementor is forced to grow a byte counter it
    /// has no way to produce: the default reports the DECODED body
    /// length, which is what a transport that hands back only `Bytes`
    /// can honestly know. The production `reqwest` transport overrides
    /// it with `bifrost_net`'s request-local counter, which is the real
    /// inbound total across retries, redirects and 401 recovery - a
    /// figure the decoded length cannot see.
    fn api_request_measured(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> impl Future<Output = Result<(Bytes, u64), TransportError>> + Send {
        async move {
            let bytes = self.api_request(url, body).await?;
            let measured = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            Ok((bytes, measured))
        }
    }

    /// Upload a blob (POST with binary body).
    fn upload(
        &self,
        url: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> impl Future<Output = Result<Bytes, TransportError>> + Send;

    /// Download a blob (GET, returns raw bytes).
    fn download(&self, url: &str) -> impl Future<Output = Result<Bytes, TransportError>> + Send;

    /// Download a blob, also reporting the inbound payload bytes the
    /// transport read to produce the answer.
    ///
    /// Defaulted on the same terms as [`Self::api_request_measured`]:
    /// the default reports the DECODED body length, and the production
    /// `reqwest` transport overrides it with `bifrost_net`'s
    /// request-local counter. Blob bytes are the bulk of the traffic a
    /// raw-message or blob read causes, so a batch emitted by one of
    /// those paths cannot report an honest count without this door.
    fn download_measured(
        &self,
        url: &str,
    ) -> impl Future<Output = Result<(Bytes, u64), TransportError>> + Send {
        async move {
            let bytes = self.download(url).await?;
            let measured = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            Ok((bytes, measured))
        }
    }

    /// Fetch the session resource (GET, returns JSON).
    fn get_session(&self, url: &str) -> impl Future<Output = Result<Bytes, TransportError>> + Send;

    /// Bound the API requests this transport has in flight at once to the
    /// session's `maxConcurrentRequests`, as `Session::api_request_concurrency`
    /// reads it. Called with every session the client installs.
    ///
    /// RFC 8620 limits the API endpoint only - uploads and downloads have
    /// their own limits - so only `api_request` traffic is gated. Defaulted
    /// to a no-op for transports with nothing to bound.
    ///
    /// The setter is synchronous. During a refresh it is called while the
    /// client holds its session lock; at initial install it runs before the
    /// client is returned. An implementation must not read the client's
    /// session from here.
    fn set_api_concurrency(&self, _max_concurrent_requests: usize) {}

    /// Bound upload requests to the session's `maxConcurrentUpload`, as
    /// `Session::upload_concurrency` reads it. Called with every session the
    /// client installs. Defaulted to a no-op for transports with nothing to
    /// bound.
    ///
    /// The setter is synchronous. During a refresh it is called while the
    /// client holds its session lock; at initial install it runs before the
    /// client is returned. An implementation must not read the client's
    /// session from here.
    fn set_upload_concurrency(&self, _max_concurrent_upload: usize) {}
}

/// Streaming transport for Server-Sent Events (EventSource).
///
/// Implement this to provide EventSource support with a custom HTTP client.
/// The default implementation uses reqwest's byte streaming.
pub(crate) trait SseTransport: Send + Sync + 'static {
    /// The byte stream type returned by the SSE connection.
    type ByteStream: futures::Stream<Item = Result<Vec<u8>, TransportError>> + Send + Unpin;

    /// Open an SSE connection to the given URL.
    ///
    /// If `last_event_id` is provided, the transport should send it as the
    /// `Last-Event-ID` HTTP header, allowing the server to replay missed events.
    fn open_sse(
        &self,
        url: &str,
        last_event_id: Option<&str>,
    ) -> impl Future<Output = Result<Self::ByteStream, TransportError>> + Send;
}
