//! Supported in-process test seam for crates that ride `bifrost-net`.
//!
//! Account crates cannot construct a [`crate::Response`] directly - it
//! is `#[non_exhaustive]` by design, so the buffered response shape can
//! grow without breaking consumers. That left every net-riding crate
//! building its own private double, and each one had to re-derive
//! bifrost-net's status contract from scratch. The contract is easy to
//! get wrong in a way that hides live defects: the retry loop returns
//! `Err` for every 4xx/5xx, so a hand-rolled double that hands back a
//! 404 as `Ok(Response)` pins a branch production can never reach.
//!
//! This module removes the need to re-derive it. A canned response is
//! staged at the *wire* boundary, below retry, redirect, rate limiting,
//! and bandwidth metering, so a scripted test drives the real pipeline
//! and observes whatever the real pipeline would have produced.
//!
//! The dispatch trait itself stays crate-private: its signature is in
//! terms of `reqwest` types, and keeping `reqwest` out of the public
//! API is the reason this crate's request wrapper exists at all. What
//! is public is the scripted double and the `Net` constructor that
//! installs it.
//!
//! Gated behind the `test-support` feature. Consumers enable it as a
//! dev-dependency:
//!
//! ```toml
//! [dev-dependencies]
//! bifrost-net = { path = "../net", features = ["test-support"] }
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bifrost_types::{AccountFuture, AccountId};
use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;

use crate::auth::TokenSource;
use crate::config::NetConfig;
use crate::error::Error;
use crate::net::{AccountNet, AccountSpec, Net};
use crate::rate::RateLimit;
use crate::request::{Dispatch, send_error_to_error};
use crate::retry::RetryPolicy;

/// One scripted wire outcome, consumed by the next dispatch.
///
/// Note what a `Response` means here: it is what the socket produced,
/// *not* what `send()` returns. bifrost-net's retry loop converts every
/// 4xx and 5xx into `Err`, so scripting a `Response` with a 404 exercises
/// the error path, not a success path. A 2xx and a passed-through 3xx are
/// the only statuses that surface to a caller as `Ok`.
pub enum Canned {
    /// A response the server produced, with the given status, headers,
    /// and body.
    Response {
        /// Status line.
        status: StatusCode,
        /// Response headers.
        headers: HeaderMap,
        /// Body bytes.
        body: Bytes,
    },
    /// A 200 whose body arrives as the given chunks, in order, each one
    /// framed as its own chunk on the wire.
    ///
    /// For the streaming download path, where what matters is how the
    /// body is FRAMED rather than what it totals. A `Response` with the
    /// concatenated bytes would let the transport frame them however it
    /// liked, so a test could not tell a stream that forwards its chunks
    /// from one that coalesces or re-splits them.
    Stream {
        /// Status line. Streaming callers care about 200 vs 206.
        status: StatusCode,
        /// Response headers - `Content-Range` for a ranged read.
        headers: HeaderMap,
        /// Body chunks, delivered in order and framed individually.
        chunks: Vec<Bytes>,
    },
    /// A response whose body fails partway through, after delivering the
    /// given chunks.
    ///
    /// The one failure a status check cannot pre-empt: the status and
    /// headers are already good, so the caller has a stream in hand
    /// before anything goes wrong. Surfaces to the caller as
    /// `Error::Network`, which is what a real socket failure mid-body
    /// produces - the specific error given here is what the transport
    /// reports having seen, not what the caller receives verbatim.
    StreamThenError {
        /// Status line.
        status: StatusCode,
        /// Response headers.
        headers: HeaderMap,
        /// Chunks delivered before the failure.
        chunks: Vec<Bytes>,
        /// Message carried into the resulting `Error::Network`.
        message: String,
    },
    /// A response whose status and headers arrive, and whose body then
    /// stalls forever after the given chunks.
    ///
    /// Distinct from [`Canned::Pending`], which never produces a
    /// response at all: here the caller already holds a status it can
    /// act on, so anything that drains the body without a read timeout
    /// blocks indefinitely. That is what a server sending 4xx headers
    /// and then hanging looks like, and it is not reachable through
    /// `StreamThenError`, whose stream does terminate.
    StreamThenStall {
        /// Status line.
        status: StatusCode,
        /// Response headers.
        headers: HeaderMap,
        /// Chunks delivered before the stall.
        chunks: Vec<Bytes>,
    },
    /// A transport-level failure, before any response exists. Drives
    /// the network-retry lane rather than the status lane.
    Error(Error),
    /// A dispatch that never completes. Drives timeout and cancellation
    /// paths.
    Pending,
}

/// What a scripted dispatch actually received on the wire, captured
/// before the canned outcome is produced.
#[derive(Clone)]
pub struct RequestSnapshot {
    /// HTTP method as sent.
    pub method: reqwest::Method,
    /// Fully resolved request URL, after any redirect hop.
    pub url: reqwest::Url,
    /// Headers as sent, including the transport-injected
    /// `Authorization` and `traceparent`.
    pub headers: HeaderMap,
    /// Request body, if the request carried one in memory.
    pub body: Option<Bytes>,
    /// Exact body size hint before any frames were polled.
    pub body_size_hint: Option<u64>,
    /// Bytes emitted by the body, counted independently of its size hint.
    pub emitted_body_bytes: u64,
    /// Total request deadline as sent. `None` unless the caller set
    /// one: the pipeline supplies no deadline of its own, bounding a
    /// stalled request with an account request timeout instead. Exposed
    /// so a test can assert what actually reached the wire rather than
    /// trusting the builder.
    pub timeout: Option<std::time::Duration>,
}

/// A wire dispatcher that answers from a fixed script and records
/// every request it saw.
///
/// An exhausted script panics rather than falling through to the
/// network. A test that under-scripts is a test whose remaining
/// assertions were about to be meaningless, and a silent fallthrough
/// would let it reach a real socket.
pub struct ScriptedDispatch {
    steps: Arc<Mutex<VecDeque<Canned>>>,
    requests: Arc<Mutex<Vec<RequestSnapshot>>>,
    in_flight: Arc<AtomicUsize>,
    peak_in_flight: Arc<AtomicUsize>,
    yield_before_answering: bool,
}

impl ScriptedDispatch {
    /// Build a dispatcher that answers the given outcomes in order.
    #[must_use]
    pub fn new(steps: impl IntoIterator<Item = Canned>) -> Arc<Self> {
        Arc::new(Self {
            steps: Arc::new(Mutex::new(steps.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak_in_flight: Arc::new(AtomicUsize::new(0)),
            yield_before_answering: false,
        })
    }

    /// A dispatcher that yields once before answering each request, so a
    /// caller's concurrent legs are genuinely in flight together and
    /// [`Self::peak_in_flight`] measures the caller's fan-out policy rather
    /// than scheduling luck.
    ///
    /// Separate from [`Self::new`] because the yield is observable: it changes
    /// completion interleaving for every test that uses it, and most tests want
    /// the deterministic immediate answer.
    #[must_use]
    pub fn yielding(steps: impl IntoIterator<Item = Canned>) -> Arc<Self> {
        Arc::new(Self {
            steps: Arc::new(Mutex::new(steps.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak_in_flight: Arc::new(AtomicUsize::new(0)),
            yield_before_answering: true,
        })
    }

    /// The high-water mark of simultaneously in-flight dispatches.
    ///
    /// Meaningful only on a [`Self::yielding`] dispatcher: without the yield an
    /// answer is produced before the next leg is polled, so the mark is 1
    /// whatever the caller's fan-out.
    #[must_use]
    pub fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(Ordering::SeqCst)
    }

    /// Append further outcomes to the end of the script.
    ///
    /// For consumers whose scripting API is called more than once per
    /// test (one call per wire surface, say) and which need all of them
    /// answered by the single dispatcher their client is attached to.
    /// Appending keeps one total order, which is what the wire has.
    pub fn extend(&self, steps: impl IntoIterator<Item = Canned>) {
        self.steps
            .lock()
            .expect("scripted step lock poisoned")
            .extend(steps);
    }

    /// Every request the dispatcher has seen, in order.
    #[must_use]
    pub fn requests(&self) -> Vec<RequestSnapshot> {
        self.requests
            .lock()
            .expect("scripted request lock poisoned")
            .clone()
    }

    /// How many scripted outcomes remain unconsumed. A test that
    /// wants to prove the pipeline stopped early asserts on this.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.steps
            .lock()
            .expect("scripted step lock poisoned")
            .len()
    }
}

fn headers_content_length(request: &reqwest::Request) -> Option<u64> {
    request
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .expect("ASCII Content-Length")
                .parse()
                .expect("numeric Content-Length")
        })
}

impl Dispatch for ScriptedDispatch {
    fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> AccountFuture<Result<reqwest::Response, Error>> {
        let steps = Arc::clone(&self.steps);
        let requests = Arc::clone(&self.requests);
        let in_flight = Arc::clone(&self.in_flight);
        let peak_in_flight = Arc::clone(&self.peak_in_flight);
        let yield_before_answering = self.yield_before_answering;
        Box::pin(async move {
            let mut request = request.build().map_err(send_error_to_error)?;
            // Consume the real Body so admission and framing are exercised
            // below the retry loop, just as they are by hyper.
            let mut body = request.body_mut().take();
            let mut headers = request.headers().clone();
            use http_body::Body as _;
            let body_size_hint = body.as_ref().and_then(|body| body.size_hint().exact());
            if let Some(body) = &body {
                use http_body::Body as _;
                if let Some(len) = body.size_hint().exact() {
                    headers
                        .entry(reqwest::header::CONTENT_LENGTH)
                        .or_insert_with(|| len.into());
                }
            }
            let index = {
                let mut requests = requests.lock().expect("scripted request lock poisoned");
                let index = requests.len();
                requests.push(RequestSnapshot {
                    method: request.method().clone(),
                    url: request.url().clone(),
                    headers,
                    body: None,
                    body_size_hint,
                    emitted_body_bytes: 0,
                    timeout: request.timeout().copied(),
                });
                index
            };
            let _in_flight = InFlightGuard::enter(&in_flight, &peak_in_flight);
            if let Some(ref mut body) = body {
                use http_body::Body as _;
                let mut bytes = Vec::new();
                while let Some(frame) =
                    futures::future::poll_fn(|cx| std::pin::Pin::new(&mut *body).poll_frame(cx))
                        .await
                {
                    let frame = frame.map_err(send_error_to_error)?;
                    if let Ok(data) = frame.into_data() {
                        bytes.extend_from_slice(&data);
                    }
                }
                let emitted_body_bytes = bytes.len() as u64;
                if let Some(expected) = body_size_hint {
                    assert_eq!(
                        expected, emitted_body_bytes,
                        "body size hint disagrees with frames"
                    );
                }
                if let Some(length) = headers_content_length(&request) {
                    assert_eq!(
                        length, emitted_body_bytes,
                        "Content-Length disagrees with frames"
                    );
                }
                let mut requests = requests.lock().expect("scripted request lock poisoned");
                requests[index].emitted_body_bytes = emitted_body_bytes;
                requests[index].body = Some(Bytes::from(bytes));
            }
            if yield_before_answering {
                tokio::task::yield_now().await;
            }
            let step = steps
                .lock()
                .expect("scripted step lock poisoned")
                .pop_front()
                .expect("scripted dispatch exhausted");
            match step {
                Canned::Response {
                    status,
                    headers,
                    body,
                } => Ok(canned_response(status, &headers, body)),
                Canned::Stream {
                    status,
                    headers,
                    chunks,
                } => {
                    let body = reqwest::Body::wrap_stream(futures::stream::iter(
                        chunks.into_iter().map(Ok::<Bytes, std::io::Error>),
                    ));
                    Ok(canned_response(status, &headers, body))
                }
                Canned::StreamThenError {
                    status,
                    headers,
                    chunks,
                    message,
                } => {
                    let failure = std::io::Error::other(message);
                    let body = reqwest::Body::wrap_stream(futures::stream::iter(
                        chunks
                            .into_iter()
                            .map(Ok)
                            .chain(std::iter::once(Err(failure))),
                    ));
                    Ok(canned_response(status, &headers, body))
                }
                Canned::StreamThenStall {
                    status,
                    headers,
                    chunks,
                } => {
                    use futures::StreamExt as _;

                    let stall = futures::stream::once(futures::future::pending::<
                        Result<Bytes, std::io::Error>,
                    >());
                    let body = reqwest::Body::wrap_stream(
                        futures::stream::iter(chunks.into_iter().map(Ok::<Bytes, std::io::Error>))
                            .chain(stall),
                    );
                    Ok(canned_response(status, &headers, body))
                }
                Canned::Error(error) => Err(error),
                Canned::Pending => futures::future::pending().await,
            }
        })
    }
}

/// Raises the in-flight count for as long as one dispatch is outstanding, and
/// records the high-water mark.
///
/// A guard rather than a bare increment/decrement pair because a dispatch can
/// leave by any of several paths - a scripted `Error`, a cancelled future - and
/// a leaked increment silently inflates every later reading.
struct InFlightGuard(Arc<AtomicUsize>);

impl InFlightGuard {
    fn enter(in_flight: &Arc<AtomicUsize>, peak: &Arc<AtomicUsize>) -> Self {
        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(now, Ordering::SeqCst);
        Self(Arc::clone(in_flight))
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Assemble a `reqwest::Response` from a canned status, headers, and
/// any body reqwest accepts (buffered `Bytes` or a wrapped stream).
fn canned_response<B: Into<reqwest::Body>>(
    status: StatusCode,
    headers: &HeaderMap,
    body: B,
) -> reqwest::Response {
    let mut response = http::Response::builder().status(status);
    for (name, value) in headers {
        response = response.header(name, value);
    }
    response
        .body(body.into())
        .expect("valid canned response")
        .into()
}

/// A canned response with the given status and body and no headers.
#[must_use]
pub fn canned(status: StatusCode, body: &'static [u8]) -> Canned {
    Canned::Response {
        status,
        headers: HeaderMap::new(),
        body: Bytes::from_static(body),
    }
}

/// A canned response carrying headers - `Retry-After`, `Location`,
/// `WWW-Authenticate`, and friends.
#[must_use]
pub fn canned_with_headers(status: StatusCode, headers: HeaderMap, body: &'static [u8]) -> Canned {
    Canned::Response {
        status,
        headers,
        body: Bytes::from_static(body),
    }
}

/// Build a `Net` whose wire dispatch answers from `script`.
///
/// Everything above the wire - retry budget, `Retry-After` honor,
/// per-host rate limiting, the redirect walk, bandwidth metering,
/// token refresh - is the production pipeline.
///
/// # Panics
/// Panics if the supplied `NetConfig` cannot build a reqwest client.
/// The client is still constructed (the scripted dispatcher replaces
/// only the send call), so a config that fails here would fail in
/// production too.
#[must_use]
pub fn scripted_net(script: &Arc<ScriptedDispatch>, config: NetConfig) -> Net {
    Net::new_with_dispatch(config, Arc::clone(script) as Arc<dyn Dispatch>)
        .expect("scripted net builds")
}

/// Attach one account to a scripted `Net` and return its `AccountNet`.
///
/// The convenience path for the common case: one account, one script.
/// Tests needing several accounts on a shared governor should call
/// [`scripted_net`] and attach each themselves.
///
/// # Panics
/// Panics if the supplied `NetConfig` cannot build a reqwest client.
#[must_use]
pub fn scripted_account(
    script: &Arc<ScriptedDispatch>,
    config: NetConfig,
    hosts: Vec<RateLimit>,
    token_source: Arc<dyn TokenSource>,
    default_retry: RetryPolicy,
) -> AccountNet {
    scripted_net(script, config).attach_account(
        AccountId("scripted".to_string()),
        AccountSpec {
            hosts,
            default_retry,
            ..AccountSpec::new(Some(token_source))
        },
    )
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[test]
    fn unpolled_dispatch_future_consumes_neither_request_nor_step() {
        let script = ScriptedDispatch::new([canned(StatusCode::OK, b"body")]);
        let client = reqwest::Client::new();
        let future = Dispatch::send(&*script, client.get("https://cancel.test/"));
        drop(future);
        assert_eq!(script.remaining(), 1);
        assert!(script.requests().is_empty());
    }
}
