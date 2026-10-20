//! Per-request builder, finished response shapes, and the streaming
//! body adapter.
//!
//! Protocol crates never see a `reqwest::RequestBuilder` directly.
//! Everything routes through this wrapper so the underlying HTTP
//! stack can be swapped without touching call sites.

use std::error::Error as StdError;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bifrost_types::{AccountFuture, TransmissionState};
use bytes::Bytes;
use futures::Stream;
use reqwest::{
    StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER},
};
use serde::Serialize;

use crate::auth::AccessToken;
use crate::concurrency::{ConcurrencyLimit, ConcurrencyPermit};
use crate::error::{
    Error, FinalResponse, STATUS_BODY_CAP, STATUS_BODY_CONNECTION_MARKER,
    STATUS_BODY_TIMEOUT_MARKER,
};
use crate::net::{AccountNet, into_byte_stream, wrap_delivery, wrap_metered};
use crate::rate::{RateGeneration, RateLimitGovernor};
use crate::redirect::{FollowRedirects, RedirectAction, RedirectPolicy, classify_redirect};
use crate::retry::RetryPolicy;

#[derive(Clone, Copy)]
pub(crate) struct RequestDeadline(Option<tokio::time::Instant>);

impl RequestDeadline {
    fn from_timeout(timeout: Option<Duration>) -> Self {
        Self(timeout.map(|timeout| tokio::time::Instant::now() + timeout))
    }

    /// Deadline check for the response-body path.
    ///
    /// Expiry here is not the same event as expiry before dispatch:
    /// response headers already arrived, so the server acted on the
    /// request and the evidence is `Acknowledged`. That routes to
    /// `Protocol(PartialResponse)` rather than a transport class, which
    /// is what a truncated body is. The point is that a body cut short
    /// by the deadline must never surface as a clean end-of-stream -
    /// `send()` would otherwise hand back a prefix of the payload as if
    /// it were the whole response.
    pub(crate) fn check_body(self) -> Result<(), Error> {
        match self.0 {
            Some(at) if tokio::time::Instant::now() >= at => Err(Error::Timeout {
                transmission_state: TransmissionState::Acknowledged,
            }),
            _ => Ok(()),
        }
    }

    /// Bound a wait on the response-body path by the total deadline.
    /// Shared bandwidth admission waits are the reason this exists: they are the one class of wait the
    /// retry loop never sees, and with a low cap they can outlast the
    /// caller's total timeout by minutes.
    pub(crate) async fn bound_body<F: std::future::Future>(
        self,
        future: F,
    ) -> Result<F::Output, Error> {
        match self.0 {
            Some(at) => tokio::time::timeout_at(at, future)
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::Acknowledged,
                }),
            None => Ok(future.await),
        }
    }

    fn remaining(self) -> Result<Option<Duration>, Error> {
        self.0
            .map(|deadline| {
                deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|remaining| !remaining.is_zero())
                    .ok_or(Error::Timeout {
                        transmission_state: TransmissionState::Unsent,
                    })
            })
            .transpose()
    }

    /// Bound a pre-acknowledgement wait by the total deadline.
    ///
    /// Everything this covers - rate-limit admission, token lookup, the
    /// forced refresh after a 401 - happens before the replacement
    /// attempt puts anything on the wire, so expiry is `Unsent`. The
    /// post-header counterpart is `bound_body`, which is `Acknowledged`;
    /// keeping the two apart is the whole point of routing every wait
    /// through this type rather than reaching for the instant directly.
    async fn bound<F: std::future::Future>(self, future: F) -> Result<F::Output, Error> {
        match self.0 {
            Some(at) => tokio::time::timeout_at(at, future)
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::Unsent,
                }),
            None => Ok(future.await),
        }
    }

    /// Bound dispatch, including upload admission, by the absolute deadline.
    /// Once dispatch starts, expiry carries in-flight transmission evidence.
    async fn bound_dispatch<F: std::future::Future>(self, future: F) -> Result<F::Output, Error> {
        match self.0 {
            Some(at) => tokio::time::timeout_at(at, future)
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::InFlight,
                }),
            None => Ok(future.await),
        }
    }

    async fn sleep(self, delay: Duration) -> Result<(), Error> {
        match self.0 {
            Some(deadline) => tokio::time::timeout_at(deadline, tokio::time::sleep(delay))
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::Unsent,
                }),
            None => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
        }
    }
}

struct AttemptBudget {
    used: u32,
    limit: u32,
}

impl AttemptBudget {
    fn new(limit: u32) -> Self {
        Self { used: 0, limit }
    }

    fn begin(&mut self) -> u32 {
        self.used = self.used.saturating_add(1);
        self.used
    }

    fn can_retry(&self) -> bool {
        self.used < self.limit
    }

    fn discard_current(&mut self) {
        self.used = self.used.saturating_sub(1);
    }
}

struct AuthBudget(bool);

impl AuthBudget {
    fn take(&mut self) -> bool {
        if self.0 {
            false
        } else {
            self.0 = true;
            true
        }
    }
}

struct RedirectBudget {
    used: u16,
    limit: u8,
}

impl RedirectBudget {
    fn follow(&mut self) -> Result<(), Error> {
        self.used = self.used.saturating_add(1);
        if self.used > u16::from(self.limit) {
            Err(Error::RedirectLoop { hops: self.used })
        } else {
            Ok(())
        }
    }
}

/// Erased byte-chunk stream, as returned by `AccountNet::download_stream`.
/// One element per chunk reqwest yields off the underlying socket;
/// bandwidth metering wraps every chunk.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + 'static>>;

pub(crate) trait Dispatch: Send + Sync + 'static {
    fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> AccountFuture<Result<reqwest::Response, Error>>;
}

pub(crate) struct ReqwestDispatch;

impl Dispatch for ReqwestDispatch {
    fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> AccountFuture<Result<reqwest::Response, Error>> {
        Box::pin(async move { request.send().await.map_err(send_error_to_error) })
    }
}

struct RateDebit {
    governor: RateLimitGovernor,
    host: String,
    quota_scope: String,
    cost: u32,
    generation: Option<RateGeneration>,
    armed: bool,
}

impl RateDebit {
    fn new(
        governor: RateLimitGovernor,
        host: String,
        quota_scope: String,
        cost: u32,
        generation: Option<RateGeneration>,
    ) -> Self {
        Self {
            governor,
            host,
            quota_scope,
            cost,
            generation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn refund_acknowledged(&self) {
        if let Some(generation) = self.generation {
            self.governor
                .refund_scoped(&self.host, &self.quota_scope, self.cost, generation);
        }
    }
}

impl Drop for RateDebit {
    fn drop(&mut self) {
        if self.armed
            && let Some(generation) = self.generation
        {
            self.governor
                .refund_scoped(&self.host, &self.quota_scope, self.cost, generation);
        }
    }
}

/// Fluent request builder. Consumes `self` on every setter so the
/// final `send` call is a single move.
pub struct RequestBuilder {
    /// Method + URL + headers + body, captured opaquely. Replaced
    /// piecewise as the caller chains setters.
    inner: RequestBuilderInner,
}

type ErrorObserver = Arc<dyn Fn(&FinalResponse) + Send + Sync>;
type RedirectGuard = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// Backing state for the builder. Kept private so the request
/// pipeline can replace the representation without touching call
/// sites.
struct RequestBuilderInner {
    /// Account-scoped transport handle. Carries the reqwest client,
    /// token source, default retry, governor, and bandwidth meter.
    account: AccountNet,
    /// HTTP method.
    method: reqwest::Method,
    /// Target URL.
    url: String,
    /// Caller-provided headers. Merged with transport-injected
    /// `Authorization` and `traceparent` at send time.
    headers: HeaderMap,
    /// Body bytes, if any.
    body: Option<Bytes>,
    /// Optional per-request cost override; otherwise the host's
    /// default cost is used.
    cost: Option<u32>,
    /// Optional per-request quota-scope override. `None` selects the
    /// scope the account declared for the request host. An account may
    /// declare several `(host, quota_scope)` buckets on one host, and
    /// the declaration order alone cannot say which one a given request
    /// belongs in; this is how the caller says.
    quota_scope: Option<String>,
    /// Optional per-request retry policy override.
    retry: Option<RetryPolicy>,
    /// Optional per-request timeout override.
    timeout: Option<Option<Duration>>,
    /// Optional per-request redirect policy override. `None` uses the
    /// account's `AccountSpec::follow_redirects`.
    follow_redirects: Option<FollowRedirects>,
    /// Caller's override of whether replaying this request can change
    /// server state. `None` derives it from the HTTP method.
    idempotent: Option<bool>,
    /// Whether the transport should inject `Authorization: Bearer`.
    /// Some protocol flows use a pre-authenticated upload URL or an
    /// explicit Basic authorization header, but still need the shared
    /// retry, rate-limit, and metering pipeline.
    bearer_auth: bool,
    /// Deferred error captured at a fluent-setter call site (e.g.
    /// `json()` could not serialize the body). The fluent API does
    /// not return `Result` on every setter, so the error is stashed
    /// here and surfaced from the subsequent `send` / `send_streaming`
    /// before any network call. Only the first error is retained; if
    /// a later setter fails too, we keep the earliest one because
    /// that is what callers tend to debug first.
    pending_error: Option<Error>,
    /// Caller-supplied counter to record this request's inbound body
    /// bytes into. Present when the caller needs the number even if the
    /// request ultimately returns `Err` - `Response::bytes_in` exists
    /// only on the success path, but the bytes came off the wire either
    /// way.
    bytes_in: Option<RequestByteCounter>,
    /// Additional concurrency claims, composed with the account claim.
    concurrency: Vec<(ConcurrencyLimit, usize)>,
    hold_gate: Option<String>,
    error_observer: Option<ErrorObserver>,
    redirect_guard: Option<RedirectGuard>,
}

impl RequestBuilder {
    /// Reject a followed redirect unless the guard accepts its source and
    /// resolved target URL. Runs before draining the redirect or admitting
    /// the target hop; pass-through responses do not invoke it.
    #[must_use]
    pub fn redirect_guard(
        mut self,
        guard: impl Fn(&str, &str) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.inner.redirect_guard = Some(Arc::new(guard));
        self
    }

    /// Refuse each wire attempt while this key is held on the parent Net.
    #[must_use]
    pub fn hold_gate(mut self, key: impl Into<String>) -> Self {
        self.inner.hold_gate = Some(key.into());
        self
    }

    /// Observe every status >= 400 before authentication or retry decisions.
    /// The body is the existing capped diagnostic prefix, including any
    /// truncation/read-failure marker. Redirects and successes are not observed.
    #[must_use]
    pub fn observe_errors(
        mut self,
        observer: impl Fn(&FinalResponse) + Send + Sync + 'static,
    ) -> Self {
        self.inner.error_observer = Some(Arc::new(observer));
        self
    }

    /// Construct a builder from an account-scoped transport, method,
    /// and URL. The account-scoped wrappers (`AccountNet::get` etc.)
    /// call this.
    pub(crate) fn new(account: AccountNet, method: reqwest::Method, url: &str) -> Self {
        Self {
            inner: RequestBuilderInner {
                account,
                method,
                url: url.to_owned(),
                headers: HeaderMap::new(),
                body: None,
                cost: None,
                quota_scope: None,
                retry: None,
                timeout: None,
                follow_redirects: None,
                idempotent: None,
                bearer_auth: true,
                pending_error: None,
                bytes_in: None,
                concurrency: Vec::new(),
                hold_gate: None,
                error_observer: None,
                redirect_guard: None,
            },
        }
    }

    /// Add a weight-1 claim on `limit`, in addition to the account claim
    /// and every other request claim. Repeated limits have their weights summed.
    ///
    /// The slot is taken before the first wire attempt and held across
    /// retries and redirect hops until the response body ends. For
    /// `send_streaming` that is when the body stream ends, fails or is
    /// dropped; see [`ConcurrencyLimit`] for the deadlock that follows from
    /// holding a streamed body open while awaiting another request on the
    /// same limit.
    #[must_use]
    pub fn concurrency_limit(self, limit: ConcurrencyLimit) -> Self {
        self.concurrency_claim(limit, 1)
    }

    /// Add weighted demand on `limit`, composed with all other claims.
    /// Zero weight is a caller bug: debug builds assert, otherwise it is one.
    /// Claims on the same limit are summed before acquisition; overflow is a
    /// local `InvalidRequest` error. Distinct limits are acquired in id order.
    #[must_use]
    pub fn concurrency_claim(mut self, limit: ConcurrencyLimit, weight: usize) -> Self {
        debug_assert!(weight != 0, "zero concurrency demand");
        self.inner.concurrency.push((limit, weight.max(1)));
        self
    }

    /// Record this request's inbound body bytes into a caller-owned
    /// counter, in addition to the count reported by `Response`.
    ///
    /// `Response::bytes_in` is final but only exists on the success
    /// path. `bifrost-net` drains, meters and throttles the bodies of
    /// non-2xx responses, exhausted retries and repeated 401s before
    /// converting them to `Error`, so those bytes are real traffic that
    /// a caller reporting per-batch totals must still be able to
    /// collect. Passing a counter here is the only way to read the
    /// number back after an `Err`.
    ///
    /// The counter is shared with the request, so read it only after
    /// `send` / `send_streaming` has returned. For `send` the value is
    /// then final; for `send_streaming` it advances as the body is
    /// drained, exactly as `StreamingResponse::bytes_in` does.
    #[must_use]
    pub fn count_bytes_into(mut self, counter: RequestByteCounter) -> Self {
        self.inner.bytes_in = Some(counter);
        self
    }

    /// Set a header. Multiple calls with the same key append rather
    /// than overwriting; this matches `reqwest::RequestBuilder::header`
    /// semantics and is what callers expect for `Cookie` and
    /// `Set-Cookie`-style multi-valued headers.
    #[must_use]
    pub fn header(mut self, key: &str, value: &str) -> Self {
        match HeaderName::from_bytes(key.as_bytes()) {
            Ok(name) => match HeaderValue::from_str(value) {
                Ok(val) => {
                    self.inner.headers.append(name, val);
                }
                Err(e) => {
                    if self.inner.pending_error.is_none() {
                        self.inner.pending_error = Some(Error::InvalidHeader {
                            message: format!("invalid value for header {key:?}: {e}"),
                            source: Some(Box::new(e)),
                        });
                    }
                }
            },
            Err(e) => {
                if self.inner.pending_error.is_none() {
                    self.inner.pending_error = Some(Error::InvalidHeader {
                        message: format!("invalid header name {key:?}: {e}"),
                        source: Some(Box::new(e)),
                    });
                }
            }
        }
        self
    }

    /// Set the request body to a JSON-serialized value. Encoded with
    /// `serde_json` and sets `Content-Type: application/json`.
    ///
    /// A serialization failure (custom `Serialize` impl returning an
    /// error) is captured on the builder and surfaced from the next
    /// `send` / `send_streaming` call as `Error::EncodeBody`. The
    /// fluent setter does not return `Result` because the protocol
    /// crates compose dozens of these chains; threading a `Result`
    /// through every setter would force a `?` after each call and
    /// hurt readability without catching anything callers cannot
    /// already learn about at send time.
    #[must_use]
    pub fn json<B: Serialize + ?Sized>(mut self, body: &B) -> Self {
        match serde_json::to_vec(body) {
            Ok(v) => {
                self.inner.body = Some(Bytes::from(v));
                let ct = HeaderName::from_static("content-type");
                let val = HeaderValue::from_static("application/json");
                self.inner.headers.insert(ct, val);
            }
            Err(e) => {
                // Keep the first deferred error if one is already
                // present; later failures often mask the root cause.
                if self.inner.pending_error.is_none() {
                    self.inner.pending_error = Some(Error::EncodeBody {
                        message: format!("serde_json::to_vec failed: {e}"),
                        source: Some(Box::new(e)),
                    });
                }
            }
        }
        self
    }

    /// Set the request body to raw bytes.
    #[must_use]
    pub fn body(mut self, body: Bytes) -> Self {
        self.inner.body = Some(body);
        self
    }

    /// Override the per-request cost for the rate-limit governor.
    #[must_use]
    pub fn cost(mut self, cost: u32) -> Self {
        self.inner.cost = Some(cost);
        self
    }

    /// Select which `(host, quota_scope)` bucket this request debits.
    ///
    /// Without it the request uses the scope the account declared for
    /// the request host. Callers that declared more than one bucket on
    /// a single host MUST use this, because host alone cannot name the
    /// bucket and the account-level default resolves to only one of
    /// them. An empty string selects the host-only compatibility
    /// bucket. The scope is re-evaluated per redirect hop, since a hop
    /// may land on a different host.
    #[must_use]
    pub fn quota_scope(mut self, scope: impl Into<String>) -> Self {
        self.inner.quota_scope = Some(scope.into());
        self
    }

    /// Override the per-request retry policy.
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.inner.retry = Some(policy);
        self
    }

    /// Override the per-request timeout.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner.timeout = Some(Some(timeout));
        self
    }

    /// Override the account's redirect policy for this request only.
    ///
    /// `FollowRedirects::Disabled` hands every 3xx back to the caller as a
    /// terminal response (status, headers and body), exactly as an account
    /// attached with redirects disabled does; nothing is sent to the
    /// `Location`. Meant for a request whose URL was admitted by a rule the
    /// transport's redirect walk cannot apply to a hop - a pre-authenticated
    /// upload URL is the standing case - where following a hop would send
    /// the body somewhere the caller never admitted. `Enabled(policy)`
    /// substitutes `policy` for the account's for this request's walk.
    #[must_use]
    pub fn follow_redirects(mut self, policy: FollowRedirects) -> Self {
        self.inner.follow_redirects = Some(policy);
        self
    }

    /// Disable the account's default total request deadline for this request.
    /// Header and body inactivity timeouts still apply.
    #[must_use]
    pub fn without_timeout(mut self) -> Self {
        self.inner.timeout = Some(None);
        self
    }

    /// Declare whether replaying this request can change server state,
    /// overriding the method-derived default.
    ///
    /// The default is `reqwest::Method::is_idempotent()`, so POST,
    /// PATCH, and extension methods are treated as unsafe to replay and
    /// the transport will not retry them once bytes have gone out. Two
    /// shapes want the override:
    ///
    /// - `idempotent(true)` for a read carried over POST. JMAP's single
    ///   `/jmap/api` endpoint is the standing example: a request whose
    ///   method calls are all `*/get` or `*/query` is safe to replay,
    ///   and saying so restores transport-level retry for it.
    /// - `idempotent(false)` for a GET or PUT the caller knows the
    ///   provider treats as an action rather than a read or an
    ///   absolute-state write.
    ///
    /// This does not affect retries driven by a status RESPONSE (5xx,
    /// 429). A complete response is `Acknowledged` evidence that the
    /// server is reporting it did not do the work, which the shared
    /// error model already treats as replayable regardless of
    /// idempotency. Only the transport-failure path consults this.
    #[must_use]
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.inner.idempotent = Some(idempotent);
        self
    }

    /// Disable automatic bearer-token injection for this request.
    /// Existing caller-provided headers are still sent unchanged.
    #[must_use]
    pub fn without_bearer_auth(mut self) -> Self {
        self.inner.bearer_auth = false;
        self
    }

    /// Drive the request to completion with the configured retry
    /// budget, returning the buffered response.
    pub async fn send(self) -> Result<Response, Error> {
        let limit = self.inner.account.max_buffered_response();
        let internal = send_streaming_inner(self).await?;
        // Held until the body is drained: this function returning, by any
        // path, is the end of the request.
        let _permit = internal.permit;
        let mut evidence = Some(internal.evidence);
        // Drain the body into a single `Bytes`. The retry loop has
        // already validated status; everything from here is a
        // straight body read. Apply the bandwidth meter to the read
        // so buffered receives feed the same counters and cap throttle
        // as streaming.
        let mut body_stream = wrap_metered(
            internal.body,
            internal.account,
            internal.deadline,
            internal.bytes_in.clone(),
        );
        let mut accum: Vec<u8> = Vec::new();
        use futures::StreamExt;
        while let Some(chunk) = body_stream.next().await {
            let chunk = chunk.map_err(|error| evidence.take().unwrap_or_default().finish(error))?;
            // Checked before the extend, so the ceiling bounds what is
            // actually held rather than being noticed one chunk after
            // the allocation that mattered. The stream drops here, so a
            // provider streaming gigabytes stops being read.
            if let Some(limit) = limit
                && accum.len() + chunk.len() > limit
            {
                return Err(evidence
                    .take()
                    .unwrap_or_default()
                    .finish(Error::ResponseTooLarge { limit }));
            }
            accum.extend_from_slice(&chunk);
        }
        Ok(Response {
            status: internal.status,
            headers: internal.headers,
            body: Bytes::from(accum),
            bytes_in: internal.bytes_in.bytes_in(),
            bytes_out: internal.bytes_out,
        })
    }

    /// Drive the request to completion but return the response body
    /// as a `ByteStream` rather than buffering. Used for blob
    /// download endpoints.
    pub async fn send_streaming(self) -> Result<StreamingResponse, Error> {
        let internal = send_streaming_inner(self).await?;
        // Caller-facing streaming response wraps the body in the
        // bandwidth meter + cap adapter.
        let metered = wrap_metered(
            internal.body,
            internal.account,
            internal.deadline,
            internal.bytes_in.clone(),
        );
        Ok(StreamingResponse {
            status: internal.status,
            headers: internal.headers,
            body: hold_permit(internal.evidence.wrap_stream(metered), internal.permit),
            bytes_in: internal.bytes_in,
            bytes_out: internal.bytes_out,
        })
    }
}

/// Buffered HTTP response.
#[non_exhaustive]
pub struct Response {
    /// HTTP status code of the final attempt.
    pub status: StatusCode,
    /// Response headers as received.
    pub headers: HeaderMap,
    /// Buffered response body.
    pub body: Bytes,
    /// Response-body bytes read for this request.
    pub bytes_in: u64,
    /// Payload bytes submitted across this request's wire attempts.
    pub bytes_out: u64,
}

impl Response {
    /// HTTP status code of the final attempt.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Response headers as received.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    #[must_use]
    pub fn bytes_in(&self) -> u64 {
        self.bytes_in
    }

    #[must_use]
    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }
}

/// Shared request-local byte counter for a streaming response.
#[derive(Clone, Default)]
pub struct RequestByteCounter(Arc<AtomicU64>);

impl RequestByteCounter {
    /// A fresh zeroed counter, for passing to
    /// `RequestBuilder::count_bytes_into`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn record(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    #[must_use]
    pub fn bytes_in(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Streaming HTTP response. The body is a `ByteStream` so the caller
/// can apply backpressure and avoid buffering large attachments.
///
/// This counter is deliberately kept OUT of consumers' per-batch byte tallies,
/// long-lived EWS streaming included. A stream's count is only ever as complete
/// as the caller's draining, and a partial number published as a batch total is
/// worse than no number: it looks authoritative and is systematically low.
/// Reporting nothing here is the considered choice, disclosed in
/// `reference/net.md`.
#[non_exhaustive]
pub struct StreamingResponse {
    /// HTTP status code of the final attempt.
    pub status: StatusCode,
    /// Response headers as received.
    pub headers: HeaderMap,
    /// Response body as an erased byte stream. Increments the
    /// bandwidth meter on every chunk.
    pub body: ByteStream,
    /// Shared counter whose value advances as `body` is drained.
    pub bytes_in: RequestByteCounter,
    /// Payload bytes submitted before response headers surfaced.
    pub bytes_out: u64,
}

impl StreamingResponse {
    /// HTTP status code of the final attempt.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Response headers as received.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    #[must_use]
    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.bytes_in()
    }

    #[must_use]
    pub fn byte_counter(&self) -> RequestByteCounter {
        self.bytes_in.clone()
    }

    #[must_use]
    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }
}

/// Carried through admission, redirects, retries, and final body consumption.
/// One completion boundary owns attaching it to every error exit.
#[derive(Default)]
pub(crate) struct AttemptEvidence {
    attempts: Vec<Error>,
    may_have_committed: bool,
}

impl AttemptEvidence {
    pub(crate) fn finish(self, source: Error) -> Error {
        if self.attempts.is_empty() {
            return source;
        }
        Error::AttemptHistory {
            source: Box::new(source),
            attempts: self.attempts,
            may_have_committed: self.may_have_committed,
        }
    }

    pub(crate) fn wrap_stream(self, body: ByteStream) -> ByteStream {
        use futures::StreamExt;
        let mut evidence = Some(self);
        Box::pin(body.map(move |chunk| {
            chunk.map_err(|error| evidence.take().unwrap_or_default().finish(error))
        }))
    }
}

/// Internal streaming response carrying the originating `AccountNet`
/// so `send()` and `send_streaming()` can share one underlying call
/// site without re-binding the body. `body` is unmetered here; the
/// outer wrappers attach the meter at the point of public exposure.
pub(crate) struct InternalStreaming {
    pub(crate) evidence: AttemptEvidence,
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: ByteStream,
    pub(crate) account: AccountNet,
    /// The total request deadline the retry loop computed. Carried out
    /// so the body drain is bounded by the same deadline the dispatch
    /// and retry waits were.
    pub(crate) deadline: RequestDeadline,
    pub(crate) bytes_out: u64,
    pub(crate) bytes_in: RequestByteCounter,
    /// The request's owned concurrency claims. Whoever exposes the body
    /// owns releasing all of them at the body's end.
    pub(crate) permit: Vec<ConcurrencyPermit>,
}

/// Tie concurrency claims to a body stream: released when the stream ends or
/// yields an error, or when it is dropped unfinished. Releasing at the end
/// rather than only on drop matters because a caller may keep a drained
/// stream alive.
pub(crate) fn hold_permit(body: ByteStream, permit: Vec<ConcurrencyPermit>) -> ByteStream {
    if permit.is_empty() {
        return body;
    }
    Box::pin(PermitStream { body, permit })
}

struct PermitStream {
    body: ByteStream,
    permit: Vec<ConcurrencyPermit>,
}

impl futures::Stream for PermitStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polled = self.body.as_mut().poll_next(cx);
        if matches!(polled, std::task::Poll::Ready(None | Some(Err(_)))) {
            self.permit.clear();
        }
        polled
    }
}

#[allow(clippy::result_large_err)]
fn normalize_claims(
    account: Option<&ConcurrencyLimit>,
    mut claims: Vec<(ConcurrencyLimit, usize)>,
) -> Result<Vec<(ConcurrencyLimit, usize)>, Error> {
    if let Some(limit) = account {
        claims.push((limit.clone(), 1));
    }
    claims.sort_unstable_by_key(|(limit, _)| limit.id());
    let mut normalized: Vec<(ConcurrencyLimit, usize)> = Vec::new();
    for (limit, demand) in claims {
        if let Some((last, total)) = normalized.last_mut()
            && last.id() == limit.id()
        {
            *total = total
                .checked_add(demand)
                .ok_or_else(|| Error::InvalidRequest {
                    field: "concurrency_claim",
                    detail: "summed concurrency demand exceeds usize".to_owned(),
                })?;
        } else {
            normalized.push((limit, demand));
        }
    }
    Ok(normalized)
}

async fn acquire_claims(claims: Vec<(ConcurrencyLimit, usize)>) -> Vec<ConcurrencyPermit> {
    let mut permits = Vec::with_capacity(claims.len());
    for (limit, demand) in claims {
        permits.push(limit.acquire_weighted(demand).await);
    }
    permits
}

/// Drive a request to a streamable response, running the retry loop
/// against the configured policy. The body is exposed unmetered;
/// outer wrappers add metering. Used by `RequestBuilder::send` (which
/// drains the body afterwards) and `RequestBuilder::send_streaming`
/// (which exposes the stream).
///
/// The function also walks 3xx redirect chains under the configured
/// `FollowRedirects` policy. Redirect classification calls
/// `redirect::classify_redirect` which enforces RFC 7231 section 6.4 method
/// rewriting, the trusted-host allowlist, and `Authorization` stripping
/// on cross-host hops; this function then rebuilds the request from
/// the resulting `RedirectStep` and re-enters the retry loop with the
/// rewritten state. Each redirect step counts as zero retries (it is
/// a fresh logical request) but counts as one hop against
/// `RedirectPolicy::max_hops`.
///
/// Ordering note (accepted): retries run before `AccountError`
/// classification, so intermediate attempts surface only as raw
/// `Error` values and the classified shape reflects the final attempt.
/// The final classification is correct even if the intermediate shape
/// is not pretty - do not re-raise.
pub(crate) async fn send_streaming_inner(
    builder: RequestBuilder,
) -> Result<InternalStreaming, Error> {
    let RequestBuilderInner {
        account,
        method,
        url,
        mut headers,
        body,
        mut cost,
        quota_scope: quota_scope_override,
        retry,
        timeout,
        follow_redirects: redirect_override,
        idempotent,
        bearer_auth,
        pending_error,
        bytes_in: caller_bytes_in,
        concurrency,
        hold_gate,
        error_observer,
        redirect_guard,
    } = builder.inner;

    // Surface any deferred error from a fluent setter (e.g. `json()`
    // failing to serialize) before touching the network. We do this
    // *before* acquiring a rate-limit slot so a malformed request
    // does not burn anyone else's quota.
    if let Some(err) = pending_error {
        return Err(err);
    }

    let policy = retry.unwrap_or_else(|| account.default_retry().clone());
    let retry_attempt_limit = policy.max_attempts;
    let redirect_policy = redirect_override.unwrap_or_else(|| account.follow_redirects().clone());
    let deadline =
        RequestDeadline::from_timeout(timeout.unwrap_or_else(|| account.request_timeout()));
    // Claims for the whole logical request, taken before the first attempt
    // and bounded by the same total deadline as every other wait, so an
    // expiry here is `Timeout { Unsent }`. Held across retries and redirect
    // hops: see `ConcurrencyLimit`.
    let claims = normalize_claims(account.concurrency_limit(), concurrency)?;
    let permit = deadline.bound(acquire_claims(claims)).await?;
    if !headers.contains_key(reqwest::header::USER_AGENT) {
        let value = reqwest::header::HeaderValue::from_str(account.user_agent()).map_err(|e| {
            Error::InvalidRequest {
                field: "user_agent",
                detail: format!("invalid User-Agent header: {e}"),
            }
        })?;
        headers.insert(reqwest::header::USER_AGENT, value);
    }
    // Per-request method, URL, body, headers, host, cost. These
    // change across redirect hops: 301/302/303 rewrite to GET and
    // drop the body, 307/308 preserve, and a cross-host hop changes
    // the host bucket the rate-limit governor uses.
    let mut method = method;
    let mut url = url;
    let mut body = body;
    let mut headers = headers;
    let mut auth_for_next_hop = bearer_auth;
    // Whether the request as written presented a credential at all -
    // either the token-source bearer or a caller-built `Authorization`
    // header (JMAP Basic auth takes the latter route).
    let mut carrying_auth = bearer_auth || headers.contains_key(AUTHORIZATION);
    // Set once the redirect walker itself removes the credential on a
    // cross-origin hop. A later 401 then proves nothing about the
    // credential, only that an unauthenticated request was refused, so
    // it must not be reported as credential loss.
    let mut auth_self_stripped = false;
    let mut host = host_from_url(&url);
    let mut quota_scope =
        resolve_quota_scope(&account, host.as_deref(), quota_scope_override.as_deref());
    let mut cost_units = recompute_cost_units(
        account.net().governor(),
        host.as_deref(),
        &quota_scope,
        cost,
    );

    let mut retry_after_history: Vec<Duration> = Vec::new();
    let mut bytes_out = 0_u64;
    // One counter for the whole request, so error drains, 401 recovery,
    // redirects and retries all contribute. When the caller supplied one
    // it IS that counter, which is what lets an `Err` return still carry
    // the bytes that came off the wire.
    let bytes_in = caller_bytes_in.unwrap_or_else(RequestByteCounter::new);
    // Network retry budget is independent from the 401-recovery
    // budget. A 401 forces a token refresh + retry that must not
    // burn the network budget (otherwise a single stale cache hit
    // halves the retries left for transient 5xx). Cap 401 retries at
    // 1; the second 401 returns `Error::AuthLost`.
    let mut attempts = AttemptBudget::new(policy.max_attempts);
    let mut auth_budget = AuthBudget(false);
    // Redirect hop count. Each redirect hop is a fresh logical
    // request - retry-budget zero, auth-budget zero, but one tick
    // against the configured `RedirectPolicy::max_hops`.
    let redirect_limit = match &redirect_policy {
        FollowRedirects::Disabled => 0,
        FollowRedirects::Enabled(policy) => policy.max_hops,
    };
    let mut redirects = RedirectBudget {
        used: 0,
        limit: redirect_limit,
    };

    let mut prior_attempts = Vec::new();
    // HTTP acknowledgement does not prove a mutation was rejected. In
    // particular a POST's 303 can follow a successful commit. Keep this
    // across method rewrites so a held replacement GET cannot authorize
    // replaying the original operation.
    let mut may_have_committed = false;
    let mut pending_response = None;
    let outcome = async {
        'outer: loop {
            deadline.remaining()?;
            let attempt = attempts.begin();
            // Resolved per attempt rather than once, because a redirect hop
            // can rewrite the method: RFC 7231 section 6.4 turns a 301/302/303 on a
            // POST into a GET, and the GET that results is replayable even
            // though the request the caller wrote was not.
            let replayable = idempotent.unwrap_or_else(|| method.is_idempotent());
            // Acquire a rate-limit slot. No-op if no host is configured.
            // Surfaces `Error::CostExceedsBurst` if the caller asked for
            // more units than the host's bucket can ever hold; that is a
            // configuration bug, not a transient condition, so we do not
            // burn retry budget on it.
            let rate_generation = if let Some(ref h) = host {
                deadline
                    .bound(account.net().governor().acquire_generation_scoped(
                        h,
                        &quota_scope,
                        cost_units,
                    ))
                    .await??
            } else {
                None
            };
            // Until the server acknowledges the request, cancellation or
            // any early return must restore the debited slot. Once a
            // response arrives the guard is disarmed because the server
            // has consumed the attempt. Explicit retry and redirect paths
            // below retain their existing refund rules.
            let mut rate_debit = host.as_ref().map(|h| {
                RateDebit::new(
                    account.net().governor().clone(),
                    h.clone(),
                    quota_scope.clone(),
                    cost_units,
                    rate_generation,
                )
            });

            // Mint a fresh `Authorization` from the token source. The
            // token source itself handles single-flight refresh.
            //
            // Error classification: `auth.rs::arc_err_to_error` already
            // sorts the underlying failure into the right typed variant -
            // `AuthLost` for terminal auth failures (revoked refresh
            // token, 401/403 from the token endpoint), `RefreshFailed`
            // wrapping a transient `Network`/`Timeout` for everything
            // else. We pass the variant through unchanged so callers can
            // pattern-match for retry-vs-give-up decisions; collapsing
            // every variant into `AuthLost` would discard that signal.
            let token = if auth_for_next_hop {
                let source = account
                    .token_source()
                    .ok_or_else(|| Error::InvalidRequest {
                        field: "bearer_auth",
                        detail: "bearer authentication requires an AccountSpec token source"
                            .to_owned(),
                    })?;
                match deadline.bound(source.current()).await? {
                    Ok(t) => Some(t),
                    Err(e) => return Err(e),
                }
            } else {
                None
            };

            if let Some(key) = hold_gate.as_ref()
                && let Some(held) = account.shared_hold(key).current()
            {
                return Err(Error::LocalHold {
                    key: key.clone(),
                    held,
                    attempts: std::mem::take(&mut prior_attempts),
                    may_have_committed,
                });
            }

            // Record outbound body bytes against the per-account meter
            // before we issue the request. Each retry is another wire
            // transmission, so we count the body once per attempt. We do
            // this even on 401-recovery retries because the failed
            // attempt did put bytes on the wire. Header bytes are
            // intentionally excluded - the meter is a payload sizing
            // tool, not a TCP byte counter.
            if let Some(ref b) = body {
                let out_n = u64::try_from(b.len()).unwrap_or(u64::MAX);
                account.meter().record_bytes_out(out_n);
                bytes_out = bytes_out.saturating_add(out_n);
            }

            let header_clock = crate::upload::HeaderClock::new();
            let request = build_reqwest(
                account.net().client(),
                &method,
                &url,
                &headers,
                &body,
                token.as_ref(),
                deadline.remaining()?,
                &account,
                &header_clock,
            );

            let dispatched = account.net().dispatch().send(request);
            let dispatched = async {
                header_clock
                    .bound(dispatched, account.response_headers_timeout())
                    .await?
            };
            let dispatched = deadline
                .bound_dispatch(dispatched)
                .await
                .and_then(std::convert::identity);
            let response = match dispatched {
                Ok(r) => {
                    may_have_committed |= !replayable;
                    pending_response = Some(FinalResponse {
                        status: r.status(),
                        headers: r.headers().clone(),
                        body: Bytes::new(),
                    });
                    if let Some(debit) = rate_debit.as_mut() {
                        debit.disarm();
                    }
                    r
                }
                Err(e) => {
                    if policy.network_errors
                        && attempts.can_retry()
                        && transport_error_is_replayable(&e, replayable)
                    {
                        let delay = backoff_for(&policy, attempt);
                        prior_attempts.push(e);
                        deadline.sleep(delay).await?;
                        continue;
                    }
                    return Err(e);
                }
            };

            let status = response.status();

            if status.as_u16() >= 400 {
                let evidence = final_response_from_response(
                    response,
                    &account,
                    deadline,
                    &bytes_in,
                    error_observer.as_ref(),
                )
                .await?;
                if auth_for_next_hop && status == StatusCode::UNAUTHORIZED {
                    if !auth_budget.take() {
                        pending_response = None;
                        return Err(Error::AuthLost {
                            transmission_state: Some(TransmissionState::Acknowledged),
                            final_response: Some(evidence),
                        });
                    }
                    prior_attempts.push(status_evidence(&evidence));
                    pending_response = None;
                    if let Some(debit) = rate_debit.as_ref() {
                        debit.refund_acknowledged();
                    }
                    let rejected_token = token.expect("authenticated request minted a token");
                    let refresh = account
                        .token_source()
                        .expect("bearer-authenticated request validated its token source")
                        .refresh_if_unchanged(rejected_token);
                    deadline.bound(refresh).await??;
                    attempts.discard_current();
                    continue 'outer;
                }
                if status == StatusCode::UNAUTHORIZED && auth_self_stripped && !auth_for_next_hop {
                    pending_response = None;
                    return Err(Error::UnauthenticatedRedirectHop {
                        message: format!(
                            "redirect hop to {url} answered 401 after the redirect walker stripped \
                         Authorization on a cross-origin hop; the account credential was never \
                         presented to this hop"
                        ),
                        final_response: evidence,
                    });
                }
                if policy.statuses.contains(&status) || status.is_server_error() {
                    let retry_after = parse_retry_after(evidence.headers.get(RETRY_AFTER))
                        .map(|d| d.min(policy.honor_retry_after_cap));
                    if let Some(ra) = retry_after {
                        retry_after_history.push(ra);
                    }
                    if attempt >= policy.max_attempts {
                        pending_response = None;
                        if status == StatusCode::TOO_MANY_REQUESTS {
                            return Err(Error::RateLimited {
                                retry_after: retry_after_history.last().copied(),
                                final_response: evidence,
                            });
                        }
                        return Err(Error::RetryBudgetExhausted {
                            final_response: Some(evidence),
                            retry_after_history,
                        });
                    }
                    let wait = retry_after.unwrap_or_else(|| backoff_for(&policy, attempt));
                    prior_attempts.push(status_evidence(&evidence));
                    pending_response = None;
                    if let Some(debit) = rate_debit.as_ref() {
                        debit.refund_acknowledged();
                    }
                    deadline.sleep(wait).await?;
                    continue;
                }
                pending_response = None;
                return Err(status_evidence(&evidence));
            }

            // 2xx: return directly. 3xx: classify against the redirect
            // policy. The classifier yields `PassThrough` for non-followed
            // 3xx (304 Not Modified, 305, 306) so conditional-request
            // flows (`If-None-Match` -> 304) and protocol-specific
            // surfaces (Graph's 304 on `$delta`) keep working.
            if status.is_success() {
                let headers_out = response.headers().clone();
                let stream = into_byte_stream(response, account.read_timeout());
                return Ok(InternalStreaming {
                    evidence: AttemptEvidence::default(),
                    status,
                    headers: headers_out,
                    body: stream,
                    account,
                    deadline,
                    bytes_out,
                    bytes_in,
                    permit,
                });
            }

            if status.is_redirection() {
                let resp_headers = response.headers().clone();
                // Do not refund the rate-limit slot at this point: a 3xx
                // is a real server response. The refund happens only on
                // the `RedirectAction::Follow` path below, where the
                // next hop will issue a fresh request that should debit
                // anew.
                let active_policy: Option<&RedirectPolicy> = match &redirect_policy {
                    FollowRedirects::Disabled => None,
                    FollowRedirects::Enabled(p) => Some(p),
                };
                match active_policy {
                    None => {
                        // Pass 3xx through to the caller exactly as the
                        // original implementation did. Caller code
                        // (conditional GET, 304 handling) reads the
                        // status + headers.
                        let stream = into_byte_stream(response, account.read_timeout());
                        return Ok(InternalStreaming {
                            evidence: AttemptEvidence::default(),
                            status,
                            headers: resp_headers,
                            body: stream,
                            account,
                            deadline,
                            bytes_out,
                            bytes_in,
                            permit,
                        });
                    }
                    Some(policy) => {
                        let response = response;
                        let parsed_url =
                            reqwest::Url::parse(&url).map_err(|e| Error::InvalidRequest {
                                field: "url",
                                detail: format!("could not re-parse request URL for redirect: {e}"),
                            })?;
                        match classify_redirect(
                            policy,
                            &method,
                            &parsed_url,
                            status,
                            &resp_headers,
                        )? {
                            RedirectAction::PassThrough => {
                                // Hand the body up exactly as the
                                // redirects-disabled arm above does. A
                                // passed-through 3xx is a terminal response
                                // from the caller's point of view, and the
                                // caller - not this loop - decides whether
                                // its body is interesting. 304/305/306
                                // ordinarily carry nothing, and Drive's
                                // header-only `308 Resume Incomplete` yields
                                // an empty stream on its own; but a followed
                                // status with no `Location` also lands here,
                                // and that shape can carry a real explanatory
                                // body. Discarding it made those bytes
                                // invisible to the caller AND to the
                                // request-local byte counter, which counts
                                // only what a body reader actually reads.
                                let stream = into_byte_stream(response, account.read_timeout());
                                return Ok(InternalStreaming {
                                    evidence: AttemptEvidence::default(),
                                    status,
                                    headers: resp_headers,
                                    body: stream,
                                    account,
                                    deadline,
                                    bytes_out,
                                    bytes_in,
                                    permit,
                                });
                            }
                            RedirectAction::Follow(step) => {
                                if redirect_guard
                                    .as_ref()
                                    .is_some_and(|guard| !guard(&url, &step.next_url))
                                {
                                    return Err(Error::RedirectRejected {
                                        message: format!(
                                            "request redirect guard rejected {url} -> {}",
                                            step.next_url
                                        ),
                                    });
                                }
                                let evidence = final_response_from_response(
                                    response, &account, deadline, &bytes_in, None,
                                )
                                .await?;
                                prior_attempts.push(status_evidence(&evidence));
                                pending_response = None;
                                redirects.follow()?;
                                // The next hop will issue a fresh request
                                // and debit anew, so refund the slot the
                                // 3xx debited. Otherwise a 10-hop chain
                                // would burn 10 units instead of one.
                                if let Some(debit) = rate_debit.as_ref() {
                                    debit.refund_acknowledged();
                                }
                                method = step.next_method;
                                url = step.next_url;
                                if !step.preserve_body {
                                    body = None;
                                    // RFC 7231 section 6.4 also drops content-
                                    // describing headers when the body is
                                    // dropped, otherwise the next hop
                                    // carries a content-type for a body
                                    // that no longer exists.
                                    strip_body_headers(&mut headers);
                                }
                                auth_for_next_hop = step.keep_auth && auth_for_next_hop;
                                // Strip caller-set `Authorization` headers
                                // too on cross-host hops, not just the
                                // token-source-derived bearer. Otherwise
                                // JMAP Basic-auth (or any caller that
                                // builds its own Authorization header)
                                // leaks credentials across a host
                                // boundary even when the bearer-injection
                                // path is suppressed.
                                if !step.keep_auth {
                                    headers.remove(AUTHORIZATION);
                                    // Remember that the credential left the
                                    // request by our own hand, not the
                                    // caller's. A 401 further down the
                                    // chain - including on a hop back to
                                    // the original origin, which arrives
                                    // unauthenticated because `keep_auth`
                                    // ANDs monotonically - is then reported
                                    // as a redirect fault rather than as a
                                    // dead credential.
                                    auth_self_stripped |= carrying_auth;
                                    carrying_auth = false;
                                }
                                let prior_host = host.clone();
                                host = host_from_url(&url);
                                // A per-request `.cost(n)` is chosen in the
                                // ORIGINAL host's quota units. Another
                                // host's bucket meters something else
                                // entirely (Graph counts requests, Gmail
                                // counts quota units), so carrying the
                                // number verbatim onto a cross-host hop
                                // debits a bucket in units the caller never
                                // spoke. Drop the override on a host change
                                // and let the hop re-resolve the new host's
                                // registered default, which is what the
                                // no-override path already did per hop.
                                cost = cost_override_for_hop(
                                    prior_host.as_deref(),
                                    host.as_deref(),
                                    cost,
                                );
                                quota_scope = resolve_quota_scope(
                                    &account,
                                    host.as_deref(),
                                    quota_scope_override.as_deref(),
                                );
                                cost_units = recompute_cost_units(
                                    account.net().governor(),
                                    host.as_deref(),
                                    &quota_scope,
                                    cost,
                                );
                                // Reset retry counter for the next hop:
                                // a redirect is a fresh logical request,
                                // its retries should not eat into the
                                // budget of the prior hop.
                                attempts = AttemptBudget::new(retry_attempt_limit);
                                auth_budget = AuthBudget(false);
                                continue 'outer;
                            }
                        }
                    }
                }
            }

            // Any remaining informational status is terminal at this layer.
            let headers_out = response.headers().clone();
            let body = read_capped_response_body(response, &account, deadline, &bytes_in).await?;
            pending_response = None;
            return Err(Error::Status {
                code: status,
                body,
                headers: headers_out,
            });
        }
    }
    .await;
    if let Some(response) = pending_response {
        prior_attempts.push(status_evidence(&response));
    }
    let evidence = AttemptEvidence {
        attempts: prior_attempts,
        may_have_committed,
    };
    match outcome {
        Ok(mut response) => {
            response.evidence = evidence;
            Ok(response)
        }
        Err(error) => Err(evidence.finish(error)),
    }
}

/// Build the underlying `reqwest::RequestBuilder` from the captured
/// pieces. Injects `Authorization`, `traceparent`, and the per-request
/// timeout. Body is cloned from `Bytes` so each retry attempt sends
/// the same payload.
#[allow(clippy::too_many_arguments)]
fn build_reqwest(
    client: &reqwest::Client,
    method: &reqwest::Method,
    url: &str,
    headers: &HeaderMap,
    body: &Option<Bytes>,
    token: Option<&AccessToken>,
    timeout: Option<Duration>,
    account: &AccountNet,
    clock: &Arc<crate::upload::HeaderClock>,
) -> reqwest::RequestBuilder {
    let mut req = client.request(method.clone(), url);
    for (k, v) in headers {
        req = req.header(k.clone(), v.clone());
    }
    // Authorization: Bearer <token>. The token may be empty if the
    // protocol crate is on no-auth mode; we still emit the header
    // so call sites don't see undocumented gaps.
    if let Some(token) = token
        && let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", token.as_str()))
    {
        req = req.header(AUTHORIZATION, val);
    }
    // W3C traceparent.
    let tp = crate::trace::current_traceparent();
    if let Ok(val) = HeaderValue::from_str(&tp) {
        req = req.header(HeaderName::from_static("traceparent"), val);
    }
    if let Some(t) = timeout {
        req = req.timeout(t);
    }
    if let Some(b) = body {
        req = req.body(reqwest::Body::wrap(crate::upload::UploadBody::new(
            b.clone(),
            account.bandwidth_budget().clone(),
            Arc::clone(clock),
        )));
    }
    req
}

/// Whether a transport failure may be replayed on the wire.
///
/// Two conditions make a replay safe, and either suffices: nothing was
/// transmitted, or transmitting it again cannot change server state.
/// Past `Unsent` on a non-idempotent request the side effect may
/// already have landed - `send_error_to_error` classifies other
/// non-connect, non-timeout reqwest failures as
/// `Network { InFlight }`, which is precisely the "may have landed"
/// evidence - and a blind replay is how one Gmail `messages/send`
/// becomes two delivered messages, or one JMAP `Email/set` create
/// becomes two drafts.
///
/// Refusing the replay is not the end of the retry story. The error
/// surfaces to `into_account_error`, which has the caller's real
/// `AccountOperation` and routes `InFlight` + non-idempotent to
/// `Reconcile(TransportDropAfterSend)` and `InFlight` + idempotent to
/// `Retry(SameRequest)`. The decision moves up to the layer that knows
/// what the request meant, instead of being taken three times by a
/// layer that only knows it is holding bytes.
///
/// A variant carrying no transmission evidence is not replayable here:
/// `InvalidRequest` and the rest are local failures a retry cannot fix.
fn transport_error_is_replayable(error: &Error, idempotent: bool) -> bool {
    let state = match error {
        Error::Network {
            transmission_state, ..
        }
        | Error::Tls {
            transmission_state, ..
        }
        | Error::Timeout { transmission_state } => transmission_state,
        _ => return false,
    };
    idempotent || *state == TransmissionState::Unsent
}

pub(crate) fn send_error_to_error(e: reqwest::Error) -> Error {
    let message = format!("{e}");
    if e.is_builder() {
        // Builder failures become `InvalidRequest`; the retry loop's
        // typed retry guard excludes that variant.
        return Error::InvalidRequest {
            field: "request",
            detail: message,
        };
    }
    if native_tls_error_in_source_chain(&e) {
        return Error::Tls {
            message,
            transmission_state: TransmissionState::Unsent,
        };
    }
    if e.is_timeout() && e.is_connect() {
        return Error::Timeout {
            transmission_state: TransmissionState::Unsent,
        };
    }
    if e.is_timeout() {
        return Error::Timeout {
            transmission_state: TransmissionState::InFlight,
        };
    }
    if e.is_connect() || refused_stream_in_source_chain(&e) {
        return Error::Network {
            message,
            transmission_state: TransmissionState::Unsent,
            source: Some(Box::new(e)),
        };
    }
    Error::Network {
        message,
        transmission_state: TransmissionState::InFlight,
        source: Some(Box::new(e)),
    }
}

// RFC 9113 section 8.7: REFUSED_STREAM guarantees no processing. Wrapped
// upload bodies cannot be cloned by reqwest, so our retry loop rebuilds them.
fn refused_stream_in_source_chain(e: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(e);
    while let Some(err) = current {
        if let Some(h2) = err.downcast_ref::<h2::Error>()
            && h2.is_reset()
            && h2.is_remote()
            && h2.reason() == Some(h2::Reason::REFUSED_STREAM)
        {
            return true;
        }
        current = err.source();
    }
    false
}

fn native_tls_error_in_source_chain(e: &reqwest::Error) -> bool {
    let mut current: Option<&(dyn StdError + 'static)> = Some(e);
    while let Some(err) = current {
        if err.downcast_ref::<native_tls::Error>().is_some() {
            return true;
        }
        current = err.source();
    }
    false
}

fn status_evidence(response: &FinalResponse) -> Error {
    Error::Status {
        code: response.status,
        headers: response.headers.clone(),
        body: response.body.clone(),
    }
}

async fn final_response_from_response(
    response: reqwest::Response,
    account: &AccountNet,
    deadline: RequestDeadline,
    request_bytes: &RequestByteCounter,
    observer: Option<&ErrorObserver>,
) -> Result<FinalResponse, Error> {
    let status = response.status();
    let headers = response.headers().clone();
    let body =
        read_capped_response_body_observed(response, account, deadline, request_bytes, observer)
            .await?;
    Ok(FinalResponse {
        status,
        headers,
        body,
    })
}

/// Drain a terminal-status body up to `STATUS_BODY_CAP`, so the error the
/// caller reconstitutes can carry the server's explanation.
///
/// Accepted limit: the read timeout is applied PER CHUNK, not to the whole
/// drain, so a server trickling one byte per interval can stretch this to
/// roughly `STATUS_BODY_CAP` intervals. The cap is 4 KB, which bounds the
/// worst case to something a consumer can wait out, and the body is
/// diagnostic text rather than payload; a second, whole-drain deadline
/// would be another timer on a path whose only job is to say why a request
/// failed. Revisit if the cap ever grows to payload size.
async fn read_capped_response_body(
    response: reqwest::Response,
    account: &AccountNet,
    deadline: RequestDeadline,
    request_bytes: &RequestByteCounter,
) -> Result<Bytes, Error> {
    read_capped_response_body_observed(response, account, deadline, request_bytes, None).await
}

async fn read_capped_response_body_observed(
    response: reqwest::Response,
    account: &AccountNet,
    deadline: RequestDeadline,
    request_bytes: &RequestByteCounter,
    observer: Option<&ErrorObserver>,
) -> Result<Bytes, Error> {
    use futures::StreamExt;

    let status = response.status();
    let headers = response.headers().clone();
    let observe = |body: &Bytes| {
        if let Some(observer) = observer {
            observer(&FinalResponse {
                status,
                headers: headers.clone(),
                body: body.clone(),
            });
        }
    };

    let mut stream = wrap_delivery(
        into_byte_stream(response, account.read_timeout()),
        account.clone(),
        deadline,
        request_bytes.clone(),
        Some(STATUS_BODY_CAP + 1),
    );
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                let total_error = deadline.check_body().err();
                let marker = if matches!(error, Error::Timeout { .. }) {
                    STATUS_BODY_TIMEOUT_MARKER
                } else {
                    STATUS_BODY_CONNECTION_MARKER
                };
                append_status_marker(&mut buf, marker);
                if let Some(error) = total_error {
                    observe(&crate::error::cap_status_body(Bytes::from(buf)));
                    return Err(error);
                }
                break;
            }
        };
        if buf.len() <= STATUS_BODY_CAP {
            let remaining = STATUS_BODY_CAP + 1 - buf.len();
            if chunk.len() > remaining {
                buf.extend_from_slice(&chunk[..remaining]);
                break;
            }
            buf.extend_from_slice(&chunk);
        } else {
            break;
        }
    }
    let body = crate::error::cap_status_body(Bytes::from(buf));
    observe(&body);
    Ok(body)
}

fn append_status_marker(buf: &mut Vec<u8>, marker: &[u8]) {
    buf.truncate(STATUS_BODY_CAP.saturating_sub(marker.len()));
    buf.extend_from_slice(marker);
}

/// Extract host string from a URL. Returns `None` if the URL is not
/// parseable; the rate-limit governor then no-ops for this request.
fn host_from_url(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned))
}

/// Resolve the rate-limit cost units for `host`, honoring the
/// per-request override `cost_override` when present and falling
/// back to the governor's registered host default. Used at request
/// start and recomputed on every redirect hop because a cross-host
/// hop changes which host bucket the request debits against.
/// Decide which quota scope a wire attempt debits.
///
/// An explicit `RequestBuilder::quota_scope` wins for every hop of the
/// request. Otherwise the account's declared scope for this host
/// applies; an account that declared no scope for the host falls back
/// to the empty host-only bucket, which is also the only bucket an
/// unregistered host could ever match.
fn resolve_quota_scope(
    account: &AccountNet,
    host: Option<&str>,
    override_: Option<&str>,
) -> String {
    match override_ {
        Some(scope) => scope.to_owned(),
        None => host
            .map_or("", |host| account.rate_scope_for(host))
            .to_owned(),
    }
}

/// Decide whether a per-request `.cost(n)` override still applies after
/// a redirect hop.
///
/// A cost is denominated in the quota units of the host the caller
/// addressed: Graph counts requests, Gmail counts quota units, and a
/// host the caller never named counts whatever it counts. Carrying the
/// number verbatim across an origin boundary debits a foreign bucket in
/// units nobody agreed on - and an override larger than the new host's
/// burst is rejected outright as `CostExceedsBurst`, failing a request
/// the caller never over-costed. On a host change the override is
/// dropped so the hop re-resolves that host's registered default, which
/// is what the no-override path already did per hop. A same-host hop
/// keeps it: the units mean the same thing on the same bucket.
///
/// Host comparison is case-insensitive (RFC 3986 section 3.2.2), matching the
/// redirect classifier's own same-origin test.
fn cost_override_for_hop(
    prior_host: Option<&str>,
    next_host: Option<&str>,
    cost: Option<u32>,
) -> Option<u32> {
    let same_host = match (prior_host, next_host) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        (None, None) => true,
        _ => false,
    };
    if same_host { cost } else { None }
}

fn recompute_cost_units(
    governor: &crate::rate::RateLimitGovernor,
    host: Option<&str>,
    quota_scope: &str,
    cost_override: Option<u32>,
) -> u32 {
    match cost_override {
        Some(n) => n,
        None => host
            .and_then(|h| governor.cost_default_for_scoped(h, quota_scope))
            .unwrap_or(1),
    }
}

/// Strip body-describing headers after a redirect demands the body
/// be dropped (RFC 7231 section 6.4 method rewrite to GET). Without this,
/// the next hop carries `Content-Type` / `Content-Length` /
/// `Content-Encoding` for a body that no longer exists.
fn strip_body_headers(headers: &mut HeaderMap) {
    headers.remove(reqwest::header::CONTENT_TYPE);
    headers.remove(reqwest::header::CONTENT_LENGTH);
    headers.remove(reqwest::header::CONTENT_ENCODING);
}

/// Exponential backoff with jitter. The base doubles per attempt,
/// capped at `policy.max_backoff`. Decorrelated jitter (half-to-full
/// of the base) prevents thundering-herd retries from one shared
/// outage.
///
/// Jitter is **proportional** to the capped backoff rather than a
/// fixed 0..1 s window. With a short `initial_backoff` (e.g. 10 ms),
/// a fixed-ms jitter would dominate by two orders of magnitude and
/// make the backoff effectively a random-1-second sleep. By taking
/// jitter from `0..capped`, the wait stays in the spirit of the
/// policy: half-base plus up-to-full-base.
fn backoff_for(policy: &RetryPolicy, attempt: u32) -> Duration {
    let base = policy.initial_backoff;
    let exp = attempt.saturating_sub(1).min(16);
    let scaled = base.saturating_mul(2u32.saturating_pow(exp));
    let capped = scaled.min(policy.max_backoff);
    // Use the workspace's UUID RNG as a cheap per-attempt jitter
    // source. This is not cryptographic policy; it just avoids
    // lockstep modulus walks between clients that started together.
    let nanos = uuid::Uuid::new_v4().as_u128();
    let capped_ns = u128::from(u64::try_from(capped.as_nanos()).unwrap_or(u64::MAX)).max(1);
    let jitter_ns = u64::try_from(nanos % capped_ns).unwrap_or(0);
    let half = capped / 2;
    let jitter = Duration::from_nanos(jitter_ns);
    half.saturating_add(jitter).min(policy.max_backoff)
}

/// Parse a `Retry-After` header. Either delta-seconds (an integer) or
/// an HTTP-date per RFC 9110 section 10.2.3.
///
/// Public so protocol crates that read `Retry-After` on responses they
/// handle themselves (without going through `bifrost-net::Error::Status`)
/// share a single parser. The companion shape on `AccountError` is the
/// `retry_after` field on `ServerCause::{Unavailable, RateLimited,
/// QuotaExhausted}` and the `not_before` field on `RetryAdvice`.
pub fn parse_retry_after(value: Option<&HeaderValue>) -> Option<Duration> {
    let v = value?.to_str().ok()?.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let when = httpdate::parse_http_date(v).ok()?;
    let now = std::time::SystemTime::now();
    when.duration_since(now).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::StaticTokenSource;
    use crate::config::NetConfig;
    use crate::rate::{RateLimit, RateLimitGovernor};
    use crate::redirect::FollowRedirects;
    use crate::test_support::{Canned, ScriptedDispatch, canned, canned_with_headers};
    use reqwest::header::LOCATION;
    use std::sync::Arc;

    // The scripted double these tests drive is the published one in
    // `crate::test_support`, not a private copy - so a downstream crate
    // scripting a status gets the same wire contract this crate pins.
    fn scripted_account(
        script: &Arc<ScriptedDispatch>,
        config: NetConfig,
        hosts: Vec<RateLimit>,
        retry: RetryPolicy,
    ) -> crate::net::AccountNet {
        crate::test_support::scripted_account(
            script,
            config,
            hosts,
            Arc::new(StaticTokenSource::new("token", None)),
            retry,
        )
    }

    fn hv(value: &str) -> HeaderValue {
        HeaderValue::from_str(value).expect("test header value")
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_retry_refunds_quota_and_preserves_final_response() {
        let mut retry_after = HeaderMap::new();
        retry_after.insert(RETRY_AFTER, HeaderValue::from_static("0"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, retry_after, b"retry"),
            canned(StatusCode::OK, b"ok"),
        ]);
        let retry = RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        };
        let account = scripted_account(
            &script,
            NetConfig::default(),
            vec![RateLimit {
                host: "retry.test".to_string(),
                quota_scope: String::new(),
                quota_per_second: 0.0001,
                cost_default: 1,
                burst: 1,
            }],
            retry,
        );
        let started = tokio::time::Instant::now();

        let response = account
            .post("https://retry.test/resource")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
            .expect("second scripted attempt succeeds");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body, Bytes::from_static(b"ok"));
        assert_eq!(script.requests().len(), 2);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the retried 503 must refund the only quota token"
        );
        assert_eq!(account.meter().bytes_out(), 14);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_network_failure_retries_through_the_same_loop() {
        let script = ScriptedDispatch::new([
            Canned::Error(Error::Network {
                message: "synthetic reset".to_string(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            canned(StatusCode::OK, b"recovered"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                ..RetryPolicy::default()
            },
        );

        let response = account
            .get("https://network.test/resource")
            .without_bearer_auth()
            .send()
            .await
            .expect("network retry succeeds");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    fn in_flight_reset() -> Canned {
        Canned::Error(Error::Network {
            message: "connection reset mid-body".to_string(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        })
    }

    fn two_attempts() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        }
    }

    /// The duplicate-send bug. `send_error_to_error` classifies every
    /// non-connect, non-timeout reqwest failure as
    /// `Network { InFlight }` - the side effect may already have landed
    /// - and the loop used to replay it anyway, three times, before
    /// `into_account_error` ever ran. Against Gmail's
    /// `/messages/send` that is a duplicate delivered message; against
    /// JMAP's `Email/set` it is a duplicate create.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_in_flight_failure_on_a_post_is_not_replayed() {
        let script = ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"second")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let Err(error) = account
            .post("https://send.test/messages/send")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
        else {
            panic!("a POST that may have landed must not be replayed");
        };

        assert!(matches!(
            error,
            Error::Network {
                transmission_state: TransmissionState::InFlight,
                ..
            }
        ));
        assert_eq!(
            script.requests().len(),
            1,
            "exactly one wire attempt; the reconcile decision belongs to the caller"
        );
    }

    /// `Unsent` is the other half of the rule: nothing reached the
    /// server, so replaying a POST cannot duplicate anything.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_unsent_failure_on_a_post_still_retries() {
        let script = ScriptedDispatch::new([
            Canned::Error(Error::Network {
                message: "connect refused".to_string(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            canned(StatusCode::OK, b"recovered"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .post("https://send.test/messages/send")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
            .expect("nothing was transmitted, so the replay is safe");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    /// An idempotent method is replayable at any transmission state:
    /// sending the same GET twice cannot change server state.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_in_flight_failure_on_a_get_still_retries() {
        let script =
            ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"recovered")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .get("https://read.test/resource")
            .send()
            .await
            .expect("an idempotent request is replayable in any state");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    /// The escape hatch for a read carried over POST, which is JMAP's
    /// whole API shape.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_explicitly_idempotent_post_retries_in_flight() {
        let script =
            ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"recovered")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .post("https://jmap.test/jmap/api")
            .idempotent(true)
            .body(Bytes::from_static(b"{\"methodCalls\":[]}"))
            .send()
            .await
            .expect("a caller that knows the payload is read-only opts back in");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    /// And the inverse: a caller that knows a GET is really an action.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_explicitly_non_idempotent_get_is_not_replayed() {
        let script = ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"second")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let Err(error) = account
            .get("https://legacy.test/trigger-action")
            .idempotent(false)
            .send()
            .await
        else {
            panic!("the override wins over the method default");
        };

        assert!(matches!(error, Error::Network { .. }));
        assert_eq!(script.requests().len(), 1);
    }

    /// A status RESPONSE is `Acknowledged` evidence that the server is
    /// reporting it did not do the work, which the shared error model
    /// treats as replayable regardless of idempotency. Idempotency must
    /// not leak into that branch and turn every 503 on a POST into an
    /// immediate failure.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_503_on_a_post_still_retries() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::SERVICE_UNAVAILABLE, b"try again"),
            canned(StatusCode::OK, b"recovered"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .post("https://send.test/messages/send")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
            .expect("status-driven retry is unchanged by the idempotency rule");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
        assert_eq!(
            account.meter().bytes_in(),
            18,
            "the discarded 503 body and successful retry body are both metered"
        );
    }

    /// `send` buffers the whole body, and every JSON API call in
    /// google / graph / jmap takes that path. Without a ceiling a
    /// provider outage page, a mis-routed blob URL, or a hostile
    /// response OOMs the process. The error path already had a 4 KB
    /// cap; this is the success path's.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_buffered_body_past_the_ceiling_is_refused() {
        let script = ScriptedDispatch::new([canned(StatusCode::OK, &[b'x'; 4096])]);
        let mut spec =
            crate::AccountSpec::new(Some(Arc::new(StaticTokenSource::new("test", None))));
        spec.max_buffered_response = Some(1024);
        spec.default_retry = RetryPolicy::disabled();
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("buffer-limit".to_owned()), spec);

        let Err(error) = account.get("https://big.test/resource").send().await else {
            panic!("a 4 KiB body must not pass a 1 KiB ceiling");
        };

        assert!(
            matches!(error.terminal(), Error::ResponseTooLarge { limit: 1024 }),
            "unexpected error: {error:?}"
        );
    }

    /// The ceiling must not fire on ordinary traffic, and `None`
    /// disables it entirely.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_body_within_the_ceiling_is_returned_intact() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::OK, b"small"),
            canned(StatusCode::OK, &[b'x'; 4096]),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        let response = account
            .get("https://ok.test/resource")
            .send()
            .await
            .expect("a small body is well under the default ceiling");
        assert_eq!(response.body, Bytes::from_static(b"small"));

        let mut spec =
            crate::AccountSpec::new(Some(Arc::new(StaticTokenSource::new("test", None))));
        spec.max_buffered_response = None;
        spec.default_retry = RetryPolicy::disabled();
        let unbounded = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("unbounded".to_owned()), spec);
        let response = unbounded
            .get("https://ok.test/resource")
            .send()
            .await
            .expect("None disables the check");
        assert_eq!(response.body.len(), 4096);
    }

    /// The pipeline supplies no total deadline of its own - the only
    /// per-request timeout on the wire is one the caller asked for.
    ///
    /// Account defaults and explicit builder timeouts are the only
    /// request deadlines in this layer.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_only_request_deadline_is_one_the_caller_set() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::OK, b"one"),
            canned(StatusCode::OK, b"two"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        account
            .get("https://deadline.test/a")
            .send()
            .await
            .expect("first request succeeds");
        account
            .get("https://deadline.test/b")
            .timeout(Duration::from_secs(90))
            .send()
            .await
            .expect("second request succeeds");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].timeout, None,
            "the pipeline must not invent a total deadline"
        );
        assert_eq!(
            requests[1].timeout,
            Some(Duration::from_secs(90)),
            "a caller-set timeout reaches the wire"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn response_headers_timeout_bounds_a_dispatch_that_never_answers() {
        let script = ScriptedDispatch::new([Canned::Pending]);
        let mut spec =
            crate::AccountSpec::new(Some(Arc::new(StaticTokenSource::new("test", None))));
        spec.connect_timeout = None;
        spec.response_headers_timeout = Some(Duration::from_secs(7));
        spec.default_retry = RetryPolicy::disabled();
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("connect-timeout".to_owned()), spec);

        let error = match account.get("https://pending.test/resource").send().await {
            Err(error) => error,
            Ok(_) => panic!("the account timeout must end a pending dispatch"),
        };

        assert!(matches!(
            error.terminal(),
            Error::Timeout {
                transmission_state: TransmissionState::InFlight
            }
        ));
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn total_deadline_bounds_retry_backoff() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("30"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, headers, b"retry"),
            canned(StatusCode::OK, b"too late"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
        let started = tokio::time::Instant::now();

        let result = account
            .get("https://deadline.test/retry")
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        let Err(error) = result else {
            panic!("the total deadline must interrupt retry backoff");
        };

        assert!(matches!(error.terminal(), Error::Timeout { .. }));
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn total_deadline_during_retry_body_drain_stays_acknowledged() {
        let script = ScriptedDispatch::new([
            Canned::StreamThenStall {
                status: StatusCode::SERVICE_UNAVAILABLE,
                headers: HeaderMap::new(),
                chunks: vec![Bytes::from_static(b"partial")],
            },
            canned(StatusCode::OK, b"must not retry"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let error = account
            .post("https://deadline.test/mutate")
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .err()
            .expect("the overall deadline interrupts the retry-body drain");

        assert!(matches!(
            error.terminal(),
            Error::Timeout {
                transmission_state: TransmissionState::Acknowledged
            }
        ));
        assert_eq!(script.requests().len(), 1);
        assert_eq!(account.meter().bytes_in(), 7);
    }

    // The bandwidth-cap throttle sleeps inside the metering wrapper,
    // which the retry loop never sees. A 4 KiB body under a 256 B/s cap
    // owes sixteen seconds of throttle; the three-second total deadline
    // must cut it off, and it must surface as a truncation error rather
    // than as a short but successful body.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn total_deadline_bounds_the_bandwidth_throttle() {
        const BODY: &[u8] = &[b'x'; 4096];
        let script = ScriptedDispatch::new([canned(StatusCode::OK, BODY)]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(256));
        let started = tokio::time::Instant::now();

        let result = account
            .get("https://throttle.test/blob")
            .timeout(Duration::from_secs(3))
            .send()
            .await;
        let Err(error) = result else {
            panic!("a throttled body must not outlive the total deadline");
        };

        assert!(
            matches!(
                error.terminal(),
                Error::Timeout {
                    transmission_state: TransmissionState::Acknowledged
                }
            ),
            "mid-body deadline expiry is acknowledged truncation, got {error:?}"
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }

    // A byte counted against the account must also be paid for. The
    // error-path drains meter their chunks, so they owe the same
    // bandwidth cap the success body pays: otherwise a 429/5xx storm
    // or a long redirect chain reads inbound bytes the per-account cap
    // never sees. Four 256-byte chunks under a 256 B/s cap: the first
    // rides the full bucket, the remaining three owe a second each.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn terminal_status_drain_pays_the_bandwidth_cap() {
        let script = ScriptedDispatch::new([Canned::Stream {
            status: StatusCode::NOT_FOUND,
            headers: HeaderMap::new(),
            chunks: vec![Bytes::from_static(&[b'x'; 256]); 4],
        }]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(256));
        let started = tokio::time::Instant::now();

        let error = account
            .get("https://capped.test/missing")
            .send()
            .await
            .err()
            .expect("a 404 is terminal");

        assert!(
            matches!(error, Error::Status { code, .. } if code == StatusCode::NOT_FOUND),
            "the drain still yields the terminal status, got {error:?}"
        );
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(3),
            "the error-path drain must pay the per-account bandwidth cap"
        );
        assert_eq!(account.meter().bytes_in(), 1024);
    }

    // Adding a throttle to an error path must not convert an
    // accounting bug into a hang: the wait is bounded by the overall
    // deadline exactly as the success body's is.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_throttled_error_drain_is_bounded_by_the_total_deadline() {
        let script = ScriptedDispatch::new([Canned::Stream {
            status: StatusCode::NOT_FOUND,
            headers: HeaderMap::new(),
            chunks: vec![Bytes::from_static(&[b'x'; 256]); 4],
        }]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(256));
        let started = tokio::time::Instant::now();

        let error = account
            .get("https://capped.test/missing")
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .err()
            .expect("the deadline interrupts the throttled drain");

        assert!(
            matches!(
                error.terminal(),
                Error::Timeout {
                    transmission_state: TransmissionState::Acknowledged
                }
            ),
            "a drain cut short mid-body is acknowledged truncation, got {error:?}"
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn total_deadline_survives_a_redirect_budget_reset() {
        let mut retry_headers = HeaderMap::new();
        retry_headers.insert(RETRY_AFTER, HeaderValue::from_static("4"));
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(LOCATION, HeaderValue::from_static("/next"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, retry_headers, b"retry"),
            canned_with_headers(StatusCode::TEMPORARY_REDIRECT, redirect_headers, b""),
            Canned::Pending,
        ]);
        let mut spec =
            crate::AccountSpec::new(Some(Arc::new(StaticTokenSource::new("test", None))));
        spec.response_headers_timeout = Some(Duration::from_secs(30));
        spec.default_retry = two_attempts();
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("redirect-deadline".to_owned()), spec);
        let started = tokio::time::Instant::now();

        let result = account
            .get("https://deadline.test/start")
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        let Err(error) = result else {
            panic!("a redirect must not reset the total deadline");
        };

        assert!(matches!(error.terminal(), Error::Timeout { .. }));
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        assert_eq!(script.requests().len(), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_401_refresh_has_a_separate_attempt_budget() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::UNAUTHORIZED, b"stale"),
            canned(StatusCode::OK, b"fresh"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            },
        );

        let response = account
            .get("https://auth.test/resource")
            .send()
            .await
            .expect("401 refresh retry does not consume max_attempts");

        assert_eq!(response.body, Bytes::from_static(b"fresh"));
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_second_401_preserves_auth_lost_response() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer"),
        );
        let script = ScriptedDispatch::new([
            canned(StatusCode::UNAUTHORIZED, b"first"),
            canned_with_headers(StatusCode::UNAUTHORIZED, headers, b"second"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let error = match account.get("https://auth.test/resource").send().await {
            Err(error) => error,
            Ok(_) => panic!("a second 401 must be terminal auth loss"),
        };

        let Error::AuthLost {
            transmission_state: Some(TransmissionState::Acknowledged),
            final_response: Some(final_response),
        } = error.terminal()
        else {
            panic!("expected acknowledged AuthLost");
        };
        assert_eq!(final_response.status, StatusCode::UNAUTHORIZED);
        assert_eq!(final_response.body, Bytes::from_static(b"second"));
        assert!(
            final_response
                .headers
                .contains_key(reqwest::header::WWW_AUTHENTICATE)
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_redirect_rewrites_post_and_strips_cross_host_auth() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(
            LOCATION,
            HeaderValue::from_static("https://target.test/final"),
        );
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::FOUND, redirect_headers, b""),
            canned(StatusCode::OK, b"done"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let response = account
            .post("https://origin.test/start")
            .header(AUTHORIZATION.as_str(), "Basic caller-secret")
            .body(Bytes::from_static(b"body"))
            .send()
            .await
            .expect("redirect target succeeds");

        assert_eq!(response.body, Bytes::from_static(b"done"));
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(requests[0].body, Some(Bytes::from_static(b"body")));
        assert!(requests[0].headers.contains_key(AUTHORIZATION));
        assert_eq!(requests[1].method, reqwest::Method::GET);
        assert_eq!(requests[1].url.as_str(), "https://target.test/final");
        assert_eq!(requests[1].body, None);
        assert!(!requests[1].headers.contains_key(AUTHORIZATION));
    }

    /// `follow_redirects(Disabled)` hands a 307 back to that one request
    /// as a terminal response and sends nothing to its `Location`, while the
    /// next request on the same account still walks the account's own
    /// (enabled) policy. Without the override the first PUT would follow the
    /// hop, replaying its body to the cleartext target.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_per_request_redirect_override_stops_only_that_request() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(
            LOCATION,
            HeaderValue::from_static("http://elsewhere.test/landing"),
        );
        let script = ScriptedDispatch::new([
            canned_with_headers(
                StatusCode::TEMPORARY_REDIRECT,
                redirect_headers.clone(),
                b"moved",
            ),
            canned_with_headers(StatusCode::TEMPORARY_REDIRECT, redirect_headers, b""),
            canned(StatusCode::OK, b"followed"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let stopped = account
            .put("https://upload.test/session")
            .without_bearer_auth()
            .follow_redirects(FollowRedirects::Disabled)
            .body(Bytes::from_static(b"chunk"))
            .send()
            .await
            .expect("a 3xx the request declined to follow is its response");
        assert_eq!(stopped.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(stopped.body, Bytes::from_static(b"moved"));
        assert_eq!(script.requests().len(), 1, "nothing went to the Location");

        let followed = account
            .put("https://upload.test/session")
            .without_bearer_auth()
            .body(Bytes::from_static(b"chunk"))
            .send()
            .await
            .expect("the account policy still follows");
        assert_eq!(followed.body, Bytes::from_static(b"followed"));
        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].url.as_str(), "http://elsewhere.test/landing");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn max_255_redirect_policy_terminates_on_hop_256() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(LOCATION, HeaderValue::from_static("/next"));
        let steps = (0..=255).map(|_| {
            canned_with_headers(
                StatusCode::TEMPORARY_REDIRECT,
                redirect_headers.clone(),
                b"",
            )
        });
        let script = ScriptedDispatch::new(steps);
        let mut spec =
            crate::AccountSpec::new(Some(Arc::new(StaticTokenSource::new("test", None))));
        spec.follow_redirects = FollowRedirects::Enabled(RedirectPolicy::with_hops(255));
        spec.default_retry = RetryPolicy::disabled();
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("redirect-limit".to_owned()), spec);

        let error = match account
            .get("https://redirect.test/start")
            .without_bearer_auth()
            .send()
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("the widened counter must terminate after the configured maximum"),
        };

        assert!(matches!(
            error.terminal(),
            Error::RedirectLoop { hops: 256 }
        ));
        assert_eq!(script.requests().len(), 256);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn exhausted_status_retry_keeps_the_final_hint_and_body() {
        let mut first_headers = HeaderMap::new();
        first_headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
        let mut final_headers = HeaderMap::new();
        final_headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, first_headers, b"first"),
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, final_headers, b"final"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy {
                max_attempts: 2,
                honor_retry_after_cap: Duration::from_secs(10),
                ..RetryPolicy::default()
            },
        );

        let error = match account
            .get("https://retry.test/resource")
            .without_bearer_auth()
            .send()
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("retry budget must be exhausted"),
        };

        let Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            retry_after_history,
        } = error.terminal()
        else {
            panic!("expected retry budget evidence");
        };
        assert_eq!(final_response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(final_response.body, Bytes::from_static(b"final"));
        assert_eq!(
            *retry_after_history,
            vec![Duration::from_secs(1), Duration::from_secs(2)]
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_download_accepts_a_legal_shortened_closed_range() {
        use futures::TryStreamExt;

        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-49/50"),
        );
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::PARTIAL_CONTENT,
            headers,
            body: Bytes::from(vec![7; 50]),
        }]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let chunks = account
            .download_stream(
                "https://range.test/blob",
                Some(bifrost_types::ByteRange {
                    start: 0,
                    length: Some(100),
                }),
            )
            .await
            .expect("the server returned the complete shorter resource tail")
            .try_collect::<Vec<_>>()
            .await
            .expect("body stream succeeds");

        assert_eq!(chunks.concat(), vec![7; 50]);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn dropping_an_armed_rate_debit_refunds_the_slot() {
        let governor = RateLimitGovernor::new();
        governor.register(RateLimit {
            host: "cancel.test".to_string(),
            quota_scope: String::new(),
            quota_per_second: 0.0001,
            cost_default: 1,
            burst: 1,
        });
        let generation = governor
            .acquire_generation("cancel.test", 1)
            .await
            .expect("initial debit");
        drop(RateDebit::new(
            governor.clone(),
            "cancel.test".to_string(),
            String::new(),
            1,
            generation,
        ));

        governor
            .acquire("cancel.test", 1)
            .await
            .expect("guard drop restored the slot");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn cancelling_an_in_flight_request_refunds_its_rate_debit() {
        let script = ScriptedDispatch::new([Canned::Pending]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            vec![RateLimit {
                host: "cancel.test".to_string(),
                quota_scope: String::new(),
                quota_per_second: 0.0001,
                cost_default: 1,
                burst: 1,
            }],
            RetryPolicy::disabled(),
        );
        let request_account = account.clone();
        let request = tokio::spawn(async move {
            request_account
                .get("https://cancel.test/resource")
                .without_bearer_auth()
                .send()
                .await
        });
        for _ in 0..100 {
            if !script.requests().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !script.requests().is_empty(),
            "request never reached the scripted dispatcher"
        );

        request.abort();
        let _ = request.await;

        account
            .net()
            .governor()
            .acquire("cancel.test", 1)
            .await
            .expect("cancelling before a response restores the only token");
    }

    // ---- backoff_for -------------------------------------------------

    /// The documented shape is `half-of-capped` plus proportional
    /// jitter in `0..capped`. Pin both ends of the interval across
    /// enough draws that a broken jitter source shows up.
    #[test]
    fn backoff_first_attempt_stays_inside_the_proportional_window() {
        let policy = RetryPolicy::default();
        for _ in 0..64 {
            let wait = backoff_for(&policy, 1);
            assert!(
                wait >= Duration::from_millis(500),
                "backoff must never dip below half the base, got {wait:?}"
            );
            assert!(
                wait < Duration::from_millis(1500),
                "backoff must stay under half-base plus one full base, got {wait:?}"
            );
        }
    }

    /// The base doubles per attempt until `max_backoff` clamps it.
    /// Attempt 7 with a 1 s base scales to 64 s, which the default
    /// 60 s ceiling caps, so the window becomes `[30 s, 60 s]`.
    #[test]
    fn backoff_growth_is_clamped_by_max_backoff() {
        let policy = RetryPolicy::default();
        for _ in 0..32 {
            let wait = backoff_for(&policy, 7);
            assert!(wait >= Duration::from_secs(30), "got {wait:?}");
            assert!(wait <= policy.max_backoff, "got {wait:?}");
        }
    }

    /// `exp` is clamped at 16 and the result is `min`-ed against
    /// `max_backoff`, so an absurd attempt count cannot produce an
    /// unbounded (or overflowing) sleep.
    #[test]
    fn backoff_saturates_rather_than_overflowing_on_huge_attempt_counts() {
        let policy = RetryPolicy::default();
        for attempt in [17u32, 64, 1_000, u32::MAX] {
            let wait = backoff_for(&policy, attempt);
            assert!(
                wait <= policy.max_backoff,
                "attempt {attempt} produced {wait:?}, above max_backoff"
            );
        }
    }

    /// A degenerate all-zero policy must return `Duration::ZERO`
    /// rather than panic inside the modulus (the `.max(1)` guard on
    /// `capped_ns` is what makes this safe).
    #[test]
    fn backoff_with_zero_durations_is_zero_and_does_not_panic() {
        let policy = RetryPolicy {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        };
        assert_eq!(backoff_for(&policy, 1), Duration::ZERO);
        assert_eq!(backoff_for(&policy, 9), Duration::ZERO);
    }

    /// `Duration::MAX` inputs exercise the `u64::try_from(as_nanos)`
    /// fallback and the `saturating_add`. The value is nonsense as a
    /// policy but must not panic; it saturates at `max_backoff`.
    #[test]
    fn backoff_with_saturated_durations_does_not_panic() {
        let policy = RetryPolicy {
            initial_backoff: Duration::MAX,
            max_backoff: Duration::MAX,
            ..RetryPolicy::default()
        };
        let wait = backoff_for(&policy, 3);
        assert!(
            wait >= Duration::from_secs(1),
            "a saturated policy still yields a positive wait, got {wait:?}"
        );
    }

    /// A `max_backoff` shorter than `initial_backoff` clamps on the
    /// very first attempt; the window is then `[max/2, max]`.
    #[test]
    fn backoff_honors_a_max_below_the_initial_base() {
        let policy = RetryPolicy {
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_millis(200),
            ..RetryPolicy::default()
        };
        for _ in 0..32 {
            let wait = backoff_for(&policy, 1);
            assert!(wait >= Duration::from_millis(100), "got {wait:?}");
            assert!(wait <= Duration::from_millis(200), "got {wait:?}");
        }
    }

    // ---- parse_retry_after -------------------------------------------

    #[test]
    fn retry_after_absent_header_is_none() {
        assert_eq!(parse_retry_after(None), None);
    }

    #[test]
    fn retry_after_parses_delta_seconds() {
        assert_eq!(
            parse_retry_after(Some(&hv("30"))),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after(Some(&hv("0"))),
            Some(Duration::ZERO),
            "a zero hint is a real instruction, not an absent one"
        );
    }

    #[test]
    fn retry_after_trims_surrounding_whitespace() {
        assert_eq!(
            parse_retry_after(Some(&hv("  45  "))),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn retry_after_rejects_negative_and_garbage() {
        assert_eq!(parse_retry_after(Some(&hv("-5"))), None);
        assert_eq!(parse_retry_after(Some(&hv("soon"))), None);
        assert_eq!(parse_retry_after(Some(&hv(""))), None);
        assert_eq!(parse_retry_after(Some(&hv("1.5"))), None);
    }

    /// An HTTP-date already in the past yields `None` (the header
    /// carries no useful wait), and a future date yields the remaining
    /// delta.
    #[test]
    fn retry_after_parses_http_dates_relative_to_now() {
        assert_eq!(
            parse_retry_after(Some(&hv("Wed, 21 Oct 2015 07:28:00 GMT"))),
            None,
            "a date in the past is not a wait"
        );
        let future =
            httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(3600));
        let parsed = parse_retry_after(Some(&hv(&future))).expect("future date parses");
        assert!(parsed <= Duration::from_secs(3600));
        assert!(parsed > Duration::from_secs(3000));
    }

    /// The parser deliberately does NOT cap: an absurd server hint is
    /// returned verbatim and the retry loop applies
    /// `policy.honor_retry_after_cap` as the single capping site. This
    /// pins that the cap lives with the caller, not here.
    #[test]
    fn retry_after_does_not_cap_absurd_server_hints() {
        assert_eq!(
            parse_retry_after(Some(&hv("31536000"))),
            Some(Duration::from_secs(31_536_000)),
            "a one-year hint survives the parser; capping is the policy's job"
        );
        let capped = parse_retry_after(Some(&hv("31536000")))
            .map(|d| d.min(RetryPolicy::default().honor_retry_after_cap));
        assert_eq!(capped, Some(Duration::from_secs(60)));
    }

    // ---- cost resolution ---------------------------------------------

    #[test]
    fn cost_precedence_is_override_then_host_default_then_one() {
        let governor = RateLimitGovernor::new();
        governor.register(RateLimit {
            host: "cost.test".to_owned(),
            quota_scope: String::new(),
            quota_per_second: 10.0,
            cost_default: 5,
            burst: 50,
        });

        assert_eq!(
            recompute_cost_units(&governor, Some("cost.test"), "", Some(9)),
            9,
            "an explicit .cost(n) wins over the host default"
        );
        assert_eq!(
            recompute_cost_units(&governor, Some("cost.test"), "", None),
            5,
            "without an override the host's registered default applies"
        );
        assert_eq!(
            recompute_cost_units(&governor, Some("unknown.test"), "", None),
            1,
            "an unregistered host falls back to one unit"
        );
        assert_eq!(
            recompute_cost_units(&governor, None, "", None),
            1,
            "an unparseable URL has no host bucket and costs one unit"
        );
    }

    // ---- URL / header helpers ----------------------------------------

    #[test]
    fn host_extraction_handles_ports_userinfo_and_junk() {
        assert_eq!(
            host_from_url("https://a.example/path?q=1"),
            Some("a.example".to_owned())
        );
        assert_eq!(
            host_from_url("https://user:pw@b.example:8443/p"),
            Some("b.example".to_owned()),
            "the bucket key is the host alone, without userinfo or port"
        );
        assert_eq!(host_from_url("not a url"), None);
        assert_eq!(
            host_from_url("https://A.EXAMPLE/"),
            Some("a.example".to_owned()),
            "reqwest lowercases the host, so bucket keys are case-normalised"
        );
    }

    #[test]
    fn body_headers_are_stripped_but_others_survive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(
            reqwest::header::CONTENT_LENGTH,
            HeaderValue::from_static("42"),
        );
        headers.insert(
            reqwest::header::CONTENT_ENCODING,
            HeaderValue::from_static("gzip"),
        );
        headers.insert(
            HeaderName::from_static("x-keep"),
            HeaderValue::from_static("yes"),
        );
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer t"));

        strip_body_headers(&mut headers);

        assert!(!headers.contains_key(reqwest::header::CONTENT_TYPE));
        assert!(!headers.contains_key(reqwest::header::CONTENT_LENGTH));
        assert!(!headers.contains_key(reqwest::header::CONTENT_ENCODING));
        assert!(
            headers.contains_key(HeaderName::from_static("x-keep")),
            "only body-describing headers are dropped on a method rewrite"
        );
        assert!(
            headers.contains_key(AUTHORIZATION),
            "Authorization is the redirect loop's business, not the body strip's"
        );
    }

    // ---- deferred builder errors -------------------------------------

    /// `RequestBuilder::header` defers a rejected header name or value
    /// to send time rather than returning `Result` from the setter.
    /// Pin the underlying reqwest rejections the deferred-error arms
    /// key off, so a future header crate that starts accepting these
    /// silently does not turn the deferral into dead code.
    #[test]
    fn header_rejections_the_deferred_error_arms_rely_on() {
        assert!(
            HeaderName::from_bytes(b"bad header").is_err(),
            "a space in a header name must be rejected"
        );
        assert!(
            HeaderValue::from_str("bad\nvalue").is_err(),
            "a newline in a header value must be rejected"
        );
        assert!(
            HeaderValue::from_str("fine").is_ok(),
            "an ordinary value must still be accepted"
        );
    }

    // ---- self-stripped auth on a redirect chain ------------------------

    /// A chain that leaves its origin and comes back (`A -> B -> A`, the
    /// CDN-bounce shape) arrives home unauthenticated, because
    /// `keep_auth` ANDs monotonically and never restores the credential.
    /// The 401 that follows must NOT read as credential death: the
    /// account's token was never presented to that hop, and reporting
    /// `AuthLost` would drive a re-authorization prompt for a perfectly
    /// good account.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_401_after_the_walker_stripped_auth_is_not_auth_lost() {
        let mut away = HeaderMap::new();
        away.insert(LOCATION, HeaderValue::from_static("https://cdn.test/hop"));
        let mut home = HeaderMap::new();
        home.insert(
            LOCATION,
            HeaderValue::from_static("https://origin.test/final"),
        );
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::TEMPORARY_REDIRECT, away, b""),
            canned_with_headers(StatusCode::TEMPORARY_REDIRECT, home, b""),
            canned(StatusCode::UNAUTHORIZED, b"no credential"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let error = match account.get("https://origin.test/start").send().await {
            Err(error) => error,
            Ok(_) => panic!("the unauthenticated final hop must fail the request"),
        };

        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].headers.contains_key(AUTHORIZATION));
        assert!(
            !requests[2].headers.contains_key(AUTHORIZATION),
            "the hop home arrives unauthenticated - the walker stripped it leaving the origin"
        );
        let Error::UnauthenticatedRedirectHop { final_response, .. } = error.terminal() else {
            panic!("expected UnauthenticatedRedirectHop, got {error:?}");
        };
        assert_eq!(final_response.status, StatusCode::UNAUTHORIZED);
        assert_eq!(final_response.body, Bytes::from_static(b"no credential"));

        let classified = crate::account_error::into_account_error(
            error,
            crate::account_error::NetErrorContext {
                provider: None,
                protocol: bifrost_types::Protocol::Jmap,
                operation: bifrost_types::AccountOperation::Hydrate,
                scope: None,
            },
        );
        assert_eq!(
            *classified.recovery(),
            bifrost_types::RecoveryClass::ProviderContractViolation,
            "a self-stripped 401 is a provider contract fault, never terminal AuthLost"
        );
    }

    /// The ordinary case is untouched: a 401 on a request that really did
    /// present its credential still surfaces as `AuthLost` (after the one
    /// forced refresh + retry the auth budget allows).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_401_on_an_authenticated_request_still_reports_auth_lost() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::UNAUTHORIZED, b"first"),
            canned(StatusCode::UNAUTHORIZED, b"second"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let error = match account.get("https://origin.test/start").send().await {
            Err(error) => error,
            Ok(_) => panic!("a genuine 401 must fail the request"),
        };

        assert!(
            matches!(error.terminal(), Error::AuthLost { .. }),
            "expected AuthLost, got {error:?}"
        );
    }

    // ---- per-request cost across a cross-host hop ----------------------

    /// A `.cost(n)` is denominated in the ORIGINAL host's quota units.
    /// Carrying it verbatim onto another host debits that host's bucket
    /// in units the caller never spoke - here a 20-unit override against
    /// a 5-unit burst, which the governor rejects outright as
    /// `CostExceedsBurst`. The hop must re-resolve the new host's own
    /// registered default instead.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_cost_override_does_not_follow_a_cross_host_hop() {
        let mut headers = HeaderMap::new();
        headers.insert(
            LOCATION,
            HeaderValue::from_static("https://small.test/final"),
        );
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::TEMPORARY_REDIRECT, headers, b""),
            canned(StatusCode::OK, b"done"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            vec![
                RateLimit {
                    host: "big.test".to_owned(),
                    quota_scope: String::new(),
                    quota_per_second: 1000.0,
                    cost_default: 1,
                    burst: 100,
                },
                RateLimit {
                    host: "small.test".to_owned(),
                    quota_scope: String::new(),
                    quota_per_second: 1000.0,
                    cost_default: 1,
                    burst: 5,
                },
            ],
            RetryPolicy::disabled(),
        );

        let response = account
            .get("https://big.test/start")
            .cost(20)
            .send()
            .await
            .expect("the cross-host hop re-resolves cost against small.test's own bucket");

        assert_eq!(response.body, Bytes::from_static(b"done"));
        assert_eq!(script.requests().len(), 2);
    }

    /// The hop-by-hop rule itself: dropped when the host changes, kept
    /// when it does not (including a mixed-case spelling of the same
    /// host, which is the same origin and the same bucket), so existing
    /// same-host callers see no behavior change.
    #[test]
    fn cost_override_is_kept_per_host_and_dropped_across_hosts() {
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("a.test"), Some(20)),
            Some(20),
            "a same-host hop keeps the caller's units"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("A.Test"), Some(20)),
            Some(20),
            "hosts are case-insensitive; this is the same bucket"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("b.test"), Some(20)),
            None,
            "a cross-host hop must re-resolve the new host's own default"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), None, Some(20)),
            None,
            "a hop with no resolvable host has no bucket the units apply to"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("b.test"), None),
            None,
            "no override stays no override"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_streams_share_one_inbound_schedule() {
        let script = ScriptedDispatch::new((0..4).map(|_| canned(StatusCode::OK, b"0123456789")));
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(10));
        let started = tokio::time::Instant::now();
        use futures::StreamExt as _;
        let results = futures::future::join_all((0..4).map(|_| {
            let account = account.clone();
            async move {
                let mut response = account
                    .get("https://shared.test/body")
                    .send_streaming()
                    .await
                    .expect("shared download headers");
                let mut len = 0;
                while let Some(chunk) = response.body.next().await {
                    len += chunk.expect("shared download slice").len();
                }
                len
            }
        }))
        .await;
        assert_eq!(results, vec![10; 4]);
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(account.meter().bytes_in(), 40);
    }

    #[tokio::test]
    #[should_panic(expected = "Content-Length disagrees with frames")]
    async fn scripted_upload_rejects_wrong_content_length() {
        let script = ScriptedDispatch::new([canned(StatusCode::OK, b"")]);
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(
                crate::AccountId("wrong-length".into()),
                crate::AccountSpec::new(None),
            );
        account
            .post("https://upload.test/body")
            .without_bearer_auth()
            .header("Content-Length", "99")
            .body(Bytes::from_static(b"abc"))
            .send()
            .await
            .expect("script response");
    }

    #[tokio::test]
    async fn refused_stream_is_unsent_and_post_replayable() {
        use http_body::Body as _;
        for (reason, expected) in [
            (h2::Reason::REFUSED_STREAM, TransmissionState::Unsent),
            (h2::Reason::CANCEL, TransmissionState::InFlight),
            (h2::Reason::INTERNAL_ERROR, TransmissionState::InFlight),
        ] {
            let (client_io, server_io) = tokio::io::duplex(4096);
            let server = tokio::spawn(async move {
                let mut connection = h2::server::handshake(server_io)
                    .await
                    .expect("server handshake");
                let (_, mut response) = connection
                    .accept()
                    .await
                    .expect("request")
                    .expect("valid request");
                response.send_reset(reason);
                let _ = connection.accept().await;
            });
            let (mut client, connection) = h2::client::handshake(client_io)
                .await
                .expect("client handshake");
            let driver = tokio::spawn(connection);
            let request = http::Request::builder()
                .method("POST")
                .uri("https://upload.test/body")
                .body(())
                .expect("request");
            let (response, _) = client.send_request(request, true).expect("send request");
            let reset = response.await.expect_err("remote reset");
            assert!(reset.is_reset() && reset.is_remote());
            server.abort();
            driver.abort();
            let mut body =
                reqwest::Body::wrap_stream(futures::stream::once(
                    async move { Err::<Bytes, _>(reset) },
                ));
            let frame = futures::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                .await
                .expect("error frame");
            let error = send_error_to_error(frame.expect_err("transport error"));
            assert!(
                matches!(&error, Error::Network { transmission_state, .. } if *transmission_state == expected)
            );
            assert_eq!(
                transport_error_is_replayable(&error, false),
                expected == TransmissionState::Unsent
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn refused_stream_post_replays_full_body_and_pays_admission_again() {
        let script = ScriptedDispatch::new([
            Canned::Error(Error::Network {
                message: "remote REFUSED_STREAM".into(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            canned(StatusCode::OK, b""),
        ]);
        let mut policy = two_attempts();
        policy.initial_backoff = Duration::ZERO;
        policy.max_backoff = Duration::ZERO;
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), policy);
        account.set_bandwidth_cap(Some(10));
        let payload = Bytes::from_static(b"01234567890123456789");
        let started = tokio::time::Instant::now();
        account
            .post("https://upload.test/body")
            .body(payload.clone())
            .send()
            .await
            .expect("unsent POST retries");
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert_eq!(request.method, reqwest::Method::POST);
            assert_eq!(request.body.as_ref(), Some(&payload));
            assert_eq!(request.emitted_body_bytes, 20);
        }
        assert_eq!(script.remaining(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn scripted_outcomes_follow_arrival_when_shorter_upload_finishes_first() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::OK, b"first"),
            canned(StatusCode::OK, b"second"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(10));
        let mut first = Box::pin(
            account
                .post("https://upload.test/first")
                .body(Bytes::from_static(b"012345678901234567890123456789"))
                .send(),
        );
        use futures::FutureExt as _;
        while script.requests().is_empty() {
            assert!(first.as_mut().now_or_never().is_none());
            tokio::task::yield_now().await;
        }
        assert_eq!(script.requests().len(), 1);
        assert_eq!(script.remaining(), 1);
        let second = account
            .post("https://upload.test/second")
            .body(Bytes::from_static(b"x"))
            .send();
        let (first, second) = futures::future::join(first, async {
            let response = second.await.expect("short upload");
            assert_eq!(script.requests()[0].emitted_body_bytes, 0);
            response
        })
        .await;
        assert_eq!(
            first.expect("long upload").body,
            Bytes::from_static(b"first")
        );
        assert_eq!(second.body, Bytes::from_static(b"second"));
        let requests = script.requests();
        assert_eq!(requests[0].emitted_body_bytes, 30);
        assert_eq!(requests[1].emitted_body_bytes, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn capped_upload_preserves_body_length_and_excludes_admission_from_headers_timeout() {
        let script = ScriptedDispatch::new([canned(StatusCode::OK, b"")]);
        let mut spec = crate::AccountSpec::new(None);
        spec.response_headers_timeout = Some(Duration::from_millis(100));
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("upload".into()), spec);
        account.set_bandwidth_cap(Some(10));
        let started = tokio::time::Instant::now();
        let payload = Bytes::from_static(b"0123456789012345678901234567890123456789");
        account
            .post("https://upload.test/body")
            .without_bearer_auth()
            .body(payload.clone())
            .send()
            .await
            .expect("admission does not consume header timeout");
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        let requests = script.requests();
        // Body::wrap preserves UploadBody's exact hint for hyper framing;
        // Body::wrap_stream would lose this body-length contract.
        assert_eq!(
            requests[0].body_size_hint,
            Some(requests[0].emitted_body_bytes)
        );
        assert_eq!(requests[0].body_size_hint, Some(40));
        assert_eq!(requests[0].emitted_body_bytes, 40);
        assert_eq!(requests[0].body.as_ref(), Some(&payload));
    }

    #[tokio::test(start_paused = true)]
    async fn header_timeout_still_runs_after_upload_admission() {
        let script = ScriptedDispatch::new([Canned::Pending]);
        let mut spec = crate::AccountSpec::new(None);
        spec.response_headers_timeout = Some(Duration::from_millis(100));
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("upload-stall".into()), spec);
        account.set_bandwidth_cap(Some(10));
        let started = tokio::time::Instant::now();
        let error = account
            .post("https://upload.test/stall")
            .without_bearer_auth()
            .body(Bytes::from_static(b"01234567890123456789"))
            .send()
            .await
            .err()
            .expect("headers stall");
        assert!(matches!(
            error.terminal(),
            Error::Timeout {
                transmission_state: TransmissionState::InFlight
            }
        ));
        assert_eq!(started.elapsed(), Duration::from_millis(1100));
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_pays_upload_admission_again() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::SERVICE_UNAVAILABLE, b""),
            canned(StatusCode::OK, b""),
        ]);
        let mut policy = two_attempts();
        policy.initial_backoff = Duration::ZERO;
        policy.max_backoff = Duration::ZERO;
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), policy);
        account.set_bandwidth_cap(Some(10));
        let started = tokio::time::Instant::now();
        account
            .put("https://retry-upload.test/body")
            .body(Bytes::from_static(b"01234567890123456789"))
            .send()
            .await
            .expect("retry upload");
        assert_eq!(script.requests().len(), 2);
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(account.meter().bytes_out(), 40);
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_preserving_redirect_pays_upload_admission_again() {
        let mut headers = HeaderMap::new();
        headers.insert(LOCATION, HeaderValue::from_static("/next"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::TEMPORARY_REDIRECT, headers, b""),
            canned(StatusCode::OK, b""),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(10));
        let started = tokio::time::Instant::now();
        account
            .put("https://redirect-upload.test/body")
            .body(Bytes::from_static(b"01234567890123456789"))
            .send()
            .await
            .expect("redirect upload");
        assert_eq!(script.requests().len(), 2);
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn diagnostics_pay_only_the_delivered_prefix_of_a_large_hyper_chunk() {
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::NOT_FOUND,
            headers: HeaderMap::new(),
            body: Bytes::from(vec![b'x'; 512 * 1024]),
        }]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(1024));
        let started = tokio::time::Instant::now();
        let error = account
            .get("https://diagnostic.test/body")
            .send()
            .await
            .err()
            .expect("terminal status");
        assert!(matches!(error, Error::Status { .. }));
        assert!(started.elapsed() < Duration::from_secs(4));
        assert!(started.elapsed() >= Duration::from_secs(3));
        assert_eq!(account.meter().bytes_in(), 512 * 1024);
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_slice_rechecks_deadline_when_the_consumer_resumes() {
        use futures::StreamExt as _;
        let script = ScriptedDispatch::new([canned(StatusCode::OK, b"0123456789abcdefghij")]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(Some(10));
        let mut response = account
            .get("https://resume.test/body")
            .timeout(Duration::from_secs(2))
            .send_streaming()
            .await
            .expect("response headers");
        assert_eq!(
            response
                .body
                .next()
                .await
                .expect("first slice")
                .expect("data")
                .len(),
            10
        );
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(matches!(
            response
                .body
                .next()
                .await
                .expect("deadline error")
                .unwrap_err()
                .terminal(),
            Error::Timeout {
                transmission_state: TransmissionState::Acknowledged
            }
        ));
        assert!(response.body.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn body_inactivity_timer_does_not_run_during_admission() {
        let script = ScriptedDispatch::new([canned(StatusCode::OK, b"0123456789abcdefghij")]);
        let mut spec = crate::AccountSpec::new(None);
        spec.read_timeout = Some(Duration::from_millis(100));
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("body-admission".into()), spec);
        account.set_bandwidth_cap(Some(10));
        let started = tokio::time::Instant::now();
        let response = account
            .get("https://body-admission.test/body")
            .without_bearer_auth()
            .send()
            .await
            .expect("throttle is not body inactivity");
        assert_eq!(response.body.len(), 20);
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    }
}
