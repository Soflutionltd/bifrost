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
use crate::exchange::{Hop, RequestDeadline};
use crate::net::{AccountNet, into_byte_stream, wrap_delivery, wrap_metered};
use crate::rate::{RateGeneration, RateLimitGovernor};
use crate::redirect::{
    FollowRedirects, RedirectAction, RedirectPolicy, classify_redirect, classify_redirect_with_mode,
};
use crate::retry::RetryPolicy;

/// Whether the transport may send a request again after bytes of an earlier
/// attempt may have reached the server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReplayPolicy {
    /// A replay can neither change server state nor change the answer the
    /// caller reads. Holds whatever headers the request carries: an explicit
    /// `Idempotent` wins over a precondition.
    Idempotent,
    /// Never replay once an attempt may have left the process.
    Never,
    /// `Idempotent`, unless the request carries a precondition on a non-safe
    /// method, in which case `Never`.
    IdempotentUnlessConditional,
}

impl ReplayPolicy {
    /// The default for a request that declares nothing: safe methods (GET,
    /// HEAD, OPTIONS, TRACE) are `Idempotent`, PUT and DELETE are
    /// `IdempotentUnlessConditional`, everything else is `Never`.
    #[must_use]
    pub fn for_method(method: &reqwest::Method) -> Self {
        if method.is_safe() {
            Self::Idempotent
        } else if matches!(*method, reqwest::Method::PUT | reqwest::Method::DELETE) {
            Self::IdempotentUnlessConditional
        } else {
            Self::Never
        }
    }

    /// Resolve the policy against the request as it will be sent.
    #[must_use]
    pub fn permits_replay(self, method: &reqwest::Method, headers: &HeaderMap) -> bool {
        match self {
            Self::Idempotent => true,
            Self::Never => false,
            Self::IdempotentUnlessConditional => method.is_safe() || !carries_precondition(headers),
        }
    }
}

/// Whether the request carries a precondition that a replay after its own
/// committed first attempt would answer as a failure (412).
///
/// Deliberately excluded, so nobody adds them by analogy: `If-Modified-Since`
/// and `If-Range` (defined only for GET/HEAD and range requests, which are
/// safe), and WebDAV `Overwrite: F` (applies only to MOVE and COPY, which
/// default to `Never` anyway). `HeaderMap` lookups are case-insensitive.
fn carries_precondition(headers: &HeaderMap) -> bool {
    [
        "if-match",
        "if-none-match",
        "if-unmodified-since",
        "if",
        "if-schedule-tag-match",
    ]
    .iter()
    .any(|name| headers.contains_key(*name))
}

/// The one replay rule, shared by net's retry loop and callers that walk
/// their own hops (dav-core): the declared policy, else
/// [`ReplayPolicy::for_method`], resolved against `method` and `headers`.
#[must_use]
pub fn request_is_replayable(
    declared: Option<ReplayPolicy>,
    method: &reqwest::Method,
    headers: &HeaderMap,
) -> bool {
    declared
        .unwrap_or_else(|| ReplayPolicy::for_method(method))
        .permits_replay(method, headers)
}

enum TimeoutOverride {
    Inherit,
    Unbounded,
    Bounded(Duration),
    Deadline(RequestDeadline),
}

impl TimeoutOverride {
    fn resolve(self, inherited: Option<Duration>) -> RequestDeadline {
        match self {
            Self::Inherit => RequestDeadline::from_timeout(inherited),
            Self::Unbounded => RequestDeadline::from_timeout(None),
            Self::Bounded(duration) => RequestDeadline::from_timeout(Some(duration)),
            Self::Deadline(deadline) => deadline,
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
    quota_scope: crate::QuotaScope,
    cost: u32,
    generation: Option<RateGeneration>,
    armed: bool,
}

impl RateDebit {
    fn new(
        governor: RateLimitGovernor,
        host: String,
        quota_scope: crate::QuotaScope,
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
type RedirectGuard = Arc<dyn Fn(&reqwest::Url, &reqwest::Url) -> bool + Send + Sync>;

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
    url: Result<reqwest::Url, Error>,
    /// Preserve lexical userinfo rejection before URL parsing discards empty `@`.
    credential_origin_error: Option<Error>,
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
    quota_scope: Option<crate::QuotaScope>,
    /// Optional per-request retry policy override.
    retry: Option<RetryPolicy>,
    /// Optional per-request timeout override.
    timeout: TimeoutOverride,
    /// Optional per-request redirect policy override. `None` uses the
    /// account's `AccountSpec::follow_redirects`.
    follow_redirects: Option<FollowRedirects>,
    /// Caller's declared replay policy. `None` uses
    /// `ReplayPolicy::for_method`. Reset to `None` when a redirect rewrites
    /// the method, because the declaration describes the request as written.
    replay: Option<ReplayPolicy>,
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
    hold_gate: Option<crate::HoldKey>,
    error_observer: Option<ErrorObserver>,
    redirect_guard: Option<RedirectGuard>,
    hop_policy: Option<Arc<dyn crate::HopPolicy>>,
    /// The request URL is itself a credential: every error this request
    /// returns passes through [`Error::redact_request_text`].
    credential_url: bool,
    // Session URL admission covers exactly one destination. Shared dispatch
    // funnels may customize accounting, but cannot widen credential authority.
    session_policy: bool,
    statuses_as_data: Vec<StatusCode>,
}

impl RequestBuilder {
    pub(crate) fn lock_session(mut self) -> Self {
        self.inner.session_policy = true;
        self
    }

    /// Declare that the request URL is itself a credential (a
    /// pre-authenticated upload session URL). Every error `send` returns,
    /// and every error `send_streaming` returns or yields from its body,
    /// then passes through [`Error::redact_request_text`], so the HTTP
    /// stack's error text, which names the URL, never reaches the error
    /// and from there support diagnostics. Classification is unchanged.
    ///
    /// The guarantee covers transport-generated request text only.
    /// Server-supplied response bodies and headers are kept as evidence;
    /// a server that echoes the URL back is not scrubbed.
    #[must_use]
    pub fn credential_url(mut self) -> Self {
        self.inner.credential_url = true;
        self
    }

    /// Reject a followed redirect unless the guard accepts its source and
    /// resolved target URL. Runs before draining the redirect or admitting
    /// the target hop; pass-through responses do not invoke it.
    #[must_use]
    pub fn redirect_guard(
        mut self,
        guard: impl Fn(&reqwest::Url, &reqwest::Url) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.inner.redirect_guard = Some(Arc::new(guard));
        self
    }

    /// Supply protocol admission, credential and header policy to the shared walk.
    /// A policy replaces automatic account bearer injection and caller Authorization.
    ///
    /// On a converted session builder, records a deferred [`Error::InvalidRequest`]
    /// with field `hop_policy`; `send` / `send_streaming` refuse before dispatch.
    #[must_use]
    pub fn hop_policy(mut self, policy: Arc<dyn crate::HopPolicy>) -> Self {
        if self.inner.session_policy {
            // A dispatch funnel must learn that its policy was refused, while
            // the upload credential stays on the provider-admitted destination.
            if self.inner.pending_error.is_none() {
                self.inner.pending_error = Some(Error::InvalidRequest {
                    field: "hop_policy",
                    detail: "session request hop policy is locked".to_owned(),
                });
            }
        } else {
            self.inner.hop_policy = Some(policy);
        }
        self
    }

    /// Refuse each wire attempt while this key is held on the parent Net.
    #[must_use]
    pub fn hold_gate(mut self, key: impl Into<crate::HoldKey>) -> Self {
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

    /// Return these statuses with their full body and exchange evidence before
    /// status retries, authentication recovery or redirect following. The caller
    /// owns their interpretation, including an explicitly selected 401 or 3xx.
    /// Other statuses retain the normal transport policy. Error observers run
    /// only for statuses that remain transport errors.
    #[must_use]
    pub fn statuses_as_data(mut self, statuses: impl IntoIterator<Item = StatusCode>) -> Self {
        self.inner.statuses_as_data = statuses.into_iter().collect();
        self
    }

    /// Share an absolute deadline across several protocol requests. Body reads
    /// and every retry use the same deadline; later timeout setters replace it.
    #[must_use]
    pub fn deadline(mut self, deadline: RequestDeadline) -> Self {
        self.inner.timeout = TimeoutOverride::Deadline(deadline);
        self
    }

    /// Construct a builder from an account-scoped transport, method,
    /// and URL. The account-scoped wrappers (`AccountNet::get` etc.)
    /// call this.
    pub(crate) fn new(
        account: AccountNet,
        method: reqwest::Method,
        url: impl reqwest::IntoUrl,
    ) -> Self {
        // Admission remains conditional on authentication at send time. Retain
        // lexical refusals here because the parser erases empty userinfo.
        let has_userinfo = crate::url::has_userinfo(url.as_str());
        let url = url.into_url().map_err(|error| Error::InvalidRequest {
            field: "url",
            detail: error.to_string(),
        });
        let credential_origin_error = account.inner.trusted_origins.as_ref().and_then(|origins| {
            if has_userinfo {
                Some(Error::InvalidRequest {
                    field: "url",
                    detail: "credential destination must not contain userinfo".into(),
                })
            } else {
                url.as_ref()
                    .ok()
                    .and_then(|url| origins.admit_url(url).err())
            }
        });
        Self {
            inner: RequestBuilderInner {
                account,
                method,
                url,
                credential_origin_error,
                headers: HeaderMap::new(),
                body: None,
                cost: None,
                quota_scope: None,
                retry: None,
                timeout: TimeoutOverride::Inherit,
                follow_redirects: None,
                replay: None,
                bearer_auth: true,
                pending_error: None,
                bytes_in: None,
                concurrency: Vec::new(),
                hold_gate: None,
                error_observer: None,
                redirect_guard: None,
                hop_policy: None,
                credential_url: false,
                session_policy: false,
                statuses_as_data: Vec::new(),
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
    pub fn quota_scope(mut self, scope: impl Into<crate::QuotaScope>) -> Self {
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
        self.inner.timeout = TimeoutOverride::Bounded(timeout);
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
    ///
    /// On a converted session builder, records a deferred [`Error::InvalidRequest`]
    /// with field `follow_redirects`, even for `Disabled`; `send` /
    /// `send_streaming` refuse before dispatch. The session policy stays locked.
    #[must_use]
    pub fn follow_redirects(mut self, policy: FollowRedirects) -> Self {
        if self.inner.session_policy {
            // Refuse explicitly even when the supplied policy also disables
            // redirects: a funnel must not mistake a locked setter for success.
            if self.inner.pending_error.is_none() {
                self.inner.pending_error = Some(Error::InvalidRequest {
                    field: "follow_redirects",
                    detail: "session request redirect policy is locked".to_owned(),
                });
            }
        } else {
            self.inner.follow_redirects = Some(policy);
        }
        self
    }

    /// Disable the account's default total request deadline for this request.
    /// Header and body inactivity timeouts still apply.
    #[must_use]
    pub fn without_timeout(mut self) -> Self {
        self.inner.timeout = TimeoutOverride::Unbounded;
        self
    }

    /// Declare this request's replay policy, overriding
    /// [`ReplayPolicy::for_method`]. It describes one wire request, not the
    /// enclosing account operation.
    ///
    /// The resolved policy gates transport failures and retryable HTTP statuses.
    /// A 401 or 429 refusal may be retried even when the policy forbids replay.
    /// An ambiguous terminal outcome retains commit evidence for read-back
    /// instead of authorizing another write: a non-replayable request that
    /// received any non-401 answer, or dropped past `Unsent`, carries
    /// a possible-commit ledger verdict, and `into_account_error` then narrows its
    /// classification to non-idempotent whatever the operation table says.
    ///
    /// The declaration is dropped when a redirect rewrites the method (a
    /// 301/302/303 POST becoming a GET); the new method's default applies.
    #[must_use]
    pub fn replay(mut self, policy: ReplayPolicy) -> Self {
        self.inner.replay = Some(policy);
        self
    }

    /// Compatibility spelling for [`Self::replay`]: `true` declares
    /// [`ReplayPolicy::Idempotent`], `false` declares [`ReplayPolicy::Never`].
    #[must_use]
    pub fn idempotent(self, idempotent: bool) -> Self {
        self.replay(if idempotent {
            ReplayPolicy::Idempotent
        } else {
            ReplayPolicy::Never
        })
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
        // Wraps the whole request, body buffering included, so no `?` on
        // any path returns an unredacted error.
        if self.inner.credential_url {
            self.send_buffered()
                .await
                .map_err(Error::redact_request_text)
        } else {
            self.send_buffered().await
        }
    }

    async fn send_buffered(self) -> Result<Response, Error> {
        let credential_url = self.inner.credential_url;
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
            evidence: {
                let evidence = evidence
                    .expect("body completed without error")
                    .into_evidence();
                if credential_url {
                    evidence.redact_request_text()
                } else {
                    evidence
                }
            },
        })
    }

    /// Drive the request to completion but return the response body
    /// as a `ByteStream` rather than buffering. Used for blob
    /// download endpoints.
    pub async fn send_streaming(self) -> Result<StreamingResponse, Error> {
        let credential_url = self.inner.credential_url;
        let internal = send_streaming_inner(self).await.map_err(|error| {
            if credential_url {
                error.redact_request_text()
            } else {
                error
            }
        })?;
        // Caller-facing streaming response wraps the body in the
        // bandwidth meter + cap adapter.
        let metered = wrap_metered(
            internal.body,
            internal.account,
            internal.deadline,
            internal.bytes_in.clone(),
        );
        // Redacted after `wrap_stream`, which attaches the earlier attempts'
        // evidence to a body error, so that history is redacted too.
        let ledger = internal.evidence.ledger.clone();
        let mut body = internal.evidence.wrap_stream(metered);
        if credential_url {
            use futures::TryStreamExt;
            body = Box::pin(body.map_err(Error::redact_request_text));
        }
        Ok(StreamingResponse {
            status: internal.status,
            headers: internal.headers,
            body: hold_permit(body, internal.permit),
            bytes_in: internal.bytes_in,
            bytes_out: internal.bytes_out,
            ledger,
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
    /// Ordered wire evidence, including the final response status.
    evidence: crate::RequestEvidence,
}

impl Response {
    /// Exchange evidence in wire order, including the terminal response.
    #[must_use]
    pub fn evidence(&self) -> &crate::RequestEvidence {
        &self.evidence
    }

    /// Classify this response through the ordinary status-error funnel without
    /// losing earlier attempts. The body is capped only at this conversion.
    /// Feed the result to the shared account-error translator and its status
    /// policy; provider body refinements can still inspect the terminal status.
    #[must_use]
    pub fn into_status_error(self) -> Error {
        let (mut attempts, ledger) = self.evidence.into_parts();
        // The final response's metadata was retained for body-read failures.
        // Replace that marker with the complete, capped terminal response.
        attempts.pop();
        let source = Error::Status {
            code: self.status,
            headers: self.headers,
            body: crate::error::cap_status_body(self.body),
        };
        // A gateway's server error can follow a committed write. Selecting
        // that response as data must not authorize blind engine replay if
        // the caller later rejects it through the shared classification.
        if self.status.is_server_error()
            && ledger.verdict() == crate::CommitEvidence::MayHaveCommitted
        {
            return Error::AttemptHistory {
                source: Box::new(source),
                evidence: crate::RequestEvidence::from_ledger(attempts, ledger),
            };
        }
        AttemptEvidence { attempts, ledger }.finish(source)
    }

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
    /// Ordered wire evidence, including the final response status.
    pub ledger: crate::AttemptLedger,
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
    ledger: crate::AttemptLedger,
}

impl AttemptEvidence {
    fn into_evidence(self) -> crate::RequestEvidence {
        crate::RequestEvidence::from_ledger(self.attempts, self.ledger)
    }

    pub(crate) fn finish(self, source: Error) -> Error {
        // Dispatch records each wire outcome once. The terminal stage remains
        // separate evidence and must not be counted as another wire attempt.
        let commit = self.ledger.verdict();
        // A direct terminal status is a complete answer, not an incomplete
        // exchange. Only retry-candidate stops and body/transport failures
        // need reconciliation in the absence of an earlier attempt.
        let complete_answer = matches!(
            source,
            Error::Status { .. }
                | Error::AuthLost { .. }
                | Error::RateLimited { .. }
                | Error::LocalHold { .. }
        );
        if self.attempts.is_empty()
            && (commit != crate::CommitEvidence::MayHaveCommitted || complete_answer)
        {
            return source;
        }
        Error::AttemptHistory {
            source: Box::new(source),
            evidence: crate::RequestEvidence::from_ledger(self.attempts, self.ledger),
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

/// The pass-through boundary is independent of body metering and admission.
struct ExchangeBody {
    status: StatusCode,
    headers: HeaderMap,
    response: reqwest::Response,
}

impl ExchangeBody {
    fn new(response: reqwest::Response) -> Self {
        Self {
            status: response.status(),
            headers: response.headers().clone(),
            response,
        }
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
        mut credential_origin_error,
        mut headers,
        body,
        cost,
        quota_scope: quota_scope_override,
        retry,
        timeout,
        follow_redirects: redirect_override,
        replay,
        bearer_auth,
        pending_error,
        bytes_in: caller_bytes_in,
        concurrency,
        hold_gate,
        error_observer,
        redirect_guard,
        hop_policy,
        // Applied by `send` and `send_streaming` around this function.
        credential_url: _,
        session_policy: _,
        statuses_as_data,
    } = builder.inner;

    // Surface any deferred error from a fluent setter (e.g. `json()`
    // failing to serialize) before touching the network. We do this
    // *before* acquiring a rate-limit slot so a malformed request
    // does not burn anyone else's quota.
    if let Some(err) = pending_error {
        return Err(err);
    }
    let url = url?;

    let policy = retry.unwrap_or_else(|| account.default_retry().clone());
    let retry_attempt_limit = policy.max_attempts;
    let redirect_policy = redirect_override.unwrap_or_else(|| account.follow_redirects().clone());
    let deadline = timeout.resolve(account.request_timeout());
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
    let mut hop = Hop {
        method,
        url,
        body,
        carrying_auth: bearer_auth || headers.contains_key(AUTHORIZATION),
        headers,
        auth_for_next_hop: bearer_auth,
        auth_self_stripped: false,
        prior_url: None,
        replay,
        host: None,
        cost,
        quota_scope_override,
        quota_scope: crate::QuotaScope::HostOnly,
        cost_units: 1,
    };
    hop.resolve_rate_key(&account);

    let mut retry_after_history: Vec<bifrost_types::RetryHint> = Vec::new();
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
    let mut ledger = crate::AttemptLedger::default();
    let mut pending_response = None;
    let mut refreshed_token = None;
    let outcome = async {
        'outer: loop {
            deadline.remaining()?;
            let hop_credential = hop.credential(hop_policy.as_deref())?;
            // Per-leg HopPolicy admission above is authoritative for its own
            // source, including Outlook and DAV on composed account handles.
            // Account bearers and caller Authorization headers must pass account
            // admission before lookup or dispatch. HopPolicy owns its credentials.
            if hop_policy.is_none()
                && (hop.headers.contains_key(AUTHORIZATION)
                    || (hop.auth_for_next_hop && account.token_source().is_some()))
                && let Some(origins) = &account.inner.trusted_origins
            {
                if let Some(error) = credential_origin_error.take() {
                    return Err(error);
                }
                let _admitted = origins.admit_url(&hop.url)?;
            }
            let hop_source = match &hop_credential {
                Some(crate::HopCredential::Bearer(source)) => Some(Arc::clone(source.as_source())),
                Some(_) => None,
                None if hop.auth_for_next_hop => account.token_source().cloned(),
                None => None,
            };
            if let Some(crate::HopCredential::Header(value)) = &hop_credential {
                hop.headers.insert(AUTHORIZATION, value.clone());
            }
            let attempt = attempts.begin();
            // Resolved per attempt rather than once, because a redirect hop
            // can rewrite the method: RFC 7231 section 6.4 turns a 301/302/303 on a
            // POST into a GET, and the GET that results is replayable even
            // though the request the caller wrote was not.
            let replayable = request_is_replayable(hop.replay, &hop.method, &hop.headers);
            // Acquire a rate-limit slot. No-op if no host is configured.
            // Surfaces `Error::CostExceedsBurst` if the caller asked for
            // more units than the host's bucket can ever hold; that is a
            // configuration bug, not a transient condition, so we do not
            // burn retry budget on it.
            let rate_generation = if let Some(ref h) = hop.host {
                deadline
                    .bound(account.net().governor().acquire_generation_scoped(
                        h,
                        &hop.quota_scope,
                        hop.cost_units,
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
            let mut rate_debit = hop.host.as_ref().map(|h| {
                RateDebit::new(
                    account.net().governor().clone(),
                    h.clone(),
                    hop.quota_scope.clone(),
                    hop.cost_units,
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
            let token = if let Some(source) = &hop_source {
                // Use the value returned by refresh even if a coordinating
                // source exposes a stale current() view. Bind it to the exact
                // source selected for this origin, and clear it on relocation.
                match refreshed_token.take() {
                    Some((refreshed_source, token)) if Arc::ptr_eq(source, &refreshed_source) => {
                        Some(token)
                    }
                    _ => Some(deadline.bound(source.current()).await??),
                }
            } else {
                if hop_policy.is_none() && hop.auth_for_next_hop {
                    return Err(Error::InvalidRequest {
                        field: "bearer_auth",
                        detail: "bearer authentication requires an AccountSpec token source".into(),
                    });
                }
                None
            };

            if let Some(policy) = &hop_policy {
                // Dialect exclusions can change while admission or token
                // lookup waits. Check the final targets immediately before
                // dispatch, including on retries of the same hop.
                policy.check_target(&hop.method, &hop.url, &hop.headers)?;
            }

            if let Some(key) = hold_gate.as_ref()
                && let Some(held) = account.shared_hold(key).current()
            {
                return Err(Error::LocalHold {
                    key: key.as_str().to_owned(),
                    held,
                    evidence: crate::RequestEvidence::from_ledger(
                        std::mem::take(&mut prior_attempts),
                        ledger.clone(),
                    ),
                });
            }

            // Record outbound body bytes against the per-account meter
            // before we issue the request. Each retry is another wire
            // transmission, so we count the body once per attempt. We do
            // this even on 401-recovery retries because the failed
            // attempt did put bytes on the wire. Header bytes are
            // intentionally excluded - the meter is a payload sizing
            // tool, not a TCP byte counter.
            if let Some(ref b) = hop.body {
                let out_n = u64::try_from(b.len()).unwrap_or(u64::MAX);
                account.meter().record_bytes_out(out_n);
                bytes_out = bytes_out.saturating_add(out_n);
            }

            let header_clock = crate::upload::HeaderClock::new();
            let request = build_reqwest(
                account.net().client(),
                &hop.method,
                &hop.url,
                &hop.headers,
                &hop.body,
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
                    // A 401 explicitly rejects the request before
                    // authentication, so it cannot be evidence that this
                    // non-idempotent attempt committed. Keep any earlier
                    // commit evidence sticky across redirects and retries.
                    ledger.record(crate::AttemptOutcome::Response(r.status()), replayable);
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
                    ledger.record_error(&e, replayable);
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

            if status.as_u16() >= 400 && !statuses_as_data.contains(&status) {
                let evidence = final_response_from_response(
                    response,
                    &account,
                    deadline,
                    &bytes_in,
                    error_observer.as_ref(),
                )
                .await?;
                if status == StatusCode::UNAUTHORIZED
                    && let Some(source) = hop_source.as_ref()
                {
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
                    let refresh = source.refresh_if_unchanged(rejected_token);
                    let replacement = deadline.bound(refresh).await??;
                    refreshed_token = Some((Arc::clone(source), replacement));
                    attempts.discard_current();
                    continue 'outer;
                }
                if status == StatusCode::UNAUTHORIZED
                    && hop_policy.is_none()
                    && hop.auth_self_stripped
                    && !hop.auth_for_next_hop
                {
                    pending_response = None;
                    return Err(Error::UnauthenticatedRedirectHop {
                        message: format!(
                            "redirect hop to {} answered 401 after the redirect walker stripped \
                         Authorization on a cross-origin hop; the account credential was never \
                         presented to this hop",
                            hop.url
                        ),
                        final_response: evidence,
                    });
                }
                if policy.statuses.contains(&status) || status.is_server_error() {
                    // Resolve HTTP-date hints only when choosing the actual
                    // wait. An elapsed date means no server-imposed wait;
                    // dropping it as an absent duration would select local
                    // exponential backoff instead.
                    let retry_after = parse_retry_after_hint(evidence.headers.get(RETRY_AFTER));
                    if let Some(ra) = retry_after {
                        retry_after_history.push(ra);
                    }
                    if attempt >= policy.max_attempts
                        || !status_error_is_replayable(status, replayable)
                    {
                        pending_response = None;
                        if status == StatusCode::TOO_MANY_REQUESTS {
                            return Err(Error::RateLimited {
                                retry_after,
                                final_response: evidence,
                            });
                        }
                        return Err(Error::RetryBudgetExhausted {
                            final_response: Some(evidence),
                            retry_after_history,
                            stop_reason: if !status_error_is_replayable(status, replayable) {
                                crate::RetryStopReason::ReplayForbidden
                            } else {
                                crate::RetryStopReason::AttemptBudget
                            },
                        });
                    }
                    let wait = retry_after
                        .map(|hint| {
                            hint.min_delay(std::time::SystemTime::now())
                                .min(policy.honor_retry_after_cap)
                        })
                        .unwrap_or_else(|| backoff_for(&policy, attempt));
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

            // Successful and selected statuses share one body
            // path. A selected redirect bypasses following just like a selected
            // 401 bypasses refresh: the caller owns that complete response.
            // Enterprise consumers need complete precondition/fallback bodies,
            // with the same limits and evidence as any other response.
            let pass_through = status.is_success() || statuses_as_data.contains(&status);
            if pass_through {
                return Ok(ExchangeBody::new(response));
            }

            if status.is_redirection() {
                let resp_headers = response.headers().clone();
                let active_policy: Option<&RedirectPolicy> = match &redirect_policy {
                    FollowRedirects::Disabled => None,
                    FollowRedirects::Enabled(p) => Some(p),
                };
                match active_policy {
                    None => return Ok(ExchangeBody::new(response)),
                    Some(policy) => {
                        let response = response;
                        let action = if let Some(hop_policy) = &hop_policy {
                            classify_redirect_with_mode(
                                policy,
                                &hop.method,
                                &hop.url,
                                status,
                                &resp_headers,
                                hop_policy.method_rewrite(),
                            )?
                        } else {
                            classify_redirect(policy, &hop.method, &hop.url, status, &resp_headers)?
                        };
                        match action {
                            RedirectAction::PassThrough => {
                                return Ok(ExchangeBody::new(response));
                            }
                            RedirectAction::Follow(step) => {
                                if redirect_guard
                                    .as_ref()
                                    .is_some_and(|guard| !guard(&hop.url, &step.next_url))
                                {
                                    return Err(Error::RedirectRejected {
                                        message: format!(
                                            "request redirect guard rejected {} -> {}",
                                            hop.url, step.next_url
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
                                hop.follow(step, hop_policy.as_deref(), &account)?;
                                refreshed_token = None;
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
        ledger,
    };
    match outcome {
        Ok(response) => Ok(InternalStreaming {
            evidence,
            status: response.status,
            headers: response.headers,
            body: into_byte_stream(response.response, account.read_timeout()),
            account,
            deadline,
            bytes_out,
            bytes_in,
            permit,
        }),
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
    url: &reqwest::Url,
    headers: &HeaderMap,
    body: &Option<Bytes>,
    token: Option<&AccessToken>,
    timeout: Option<Duration>,
    account: &AccountNet,
    clock: &Arc<crate::upload::HeaderClock>,
) -> reqwest::RequestBuilder {
    let mut req = client.request(method.clone(), url.clone());
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

/// Whether a retry-candidate status may be followed by another attempt.
///
/// A selected status is only a retry candidate. Authentication and rate-limit
/// refusals establish that the attempted work was refused; a generic 5xx does not,
/// because a gateway 502/503/504 can follow a commit.
fn status_error_is_replayable(status: StatusCode, replayable: bool) -> bool {
    replayable || crate::is_proven_refusal(status)
}

/// Whether a transport failure may be replayed on the wire.
///
/// Two conditions make a replay safe, and either suffices: nothing was
/// transmitted, or the resolved replay policy says transmitting it again
/// cannot change server state or the answer. A terminal unreplayable drop
/// past `Unsent` retains commit evidence (`AttemptHistory` with
/// a possible-commit ledger verdict), so classification requires read-back even under
/// an idempotent logical operation.
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

#[cfg(test)]
fn retry_after_delay(
    value: Option<&HeaderValue>,
    now: std::time::SystemTime,
    cap: Duration,
) -> Option<Duration> {
    parse_retry_after_hint(value).map(|hint| hint.min_delay(now).min(cap))
}

/// Parse a server hint without resolving an HTTP-date against the local clock.
/// Delta-seconds become `RetryHint::After`; HTTP-dates remain `RetryHint::At`,
/// including elapsed dates. Invalid or absent values return `None`.
/// The `retry_hint` fields on server causes and retry advice carry this value.
/// Retry policy caps belong to the transport wait, not the server evidence.
pub fn parse_retry_after_hint(value: Option<&HeaderValue>) -> Option<bifrost_types::RetryHint> {
    let raw = value?.to_str().ok()?.trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(bifrost_types::RetryHint::After(Duration::from_secs(
            seconds,
        )));
    }
    httpdate::parse_http_date(raw)
        .ok()
        .map(bifrost_types::RetryHint::At)
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

    #[tokio::test(start_paused = true)]
    async fn exchange_core_selected_statuses_keep_full_bodies_without_transport_recovery() {
        for code in [401, 404, 412, 429, 501, 503, 507, 308] {
            let status = StatusCode::from_u16(code).unwrap();
            let payload = Bytes::from(vec![b'x'; STATUS_BODY_CAP + 73]);
            let mut headers = HeaderMap::new();
            headers.insert(LOCATION, hv("https://elsewhere.test/next"));
            headers.insert(RETRY_AFTER, hv("600"));
            let script = ScriptedDispatch::new([
                Canned::Response {
                    status,
                    headers,
                    body: payload.clone(),
                },
                canned(StatusCode::OK, b"must remain unused"),
            ]);
            let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
            let observed = Arc::new(AtomicU64::new(0));
            let observer = Arc::clone(&observed);
            let response = account
                .get("https://exchange.test/data")
                .statuses_as_data([status])
                .observe_errors(move |_| {
                    observer.fetch_add(1, Ordering::Relaxed);
                })
                .send()
                .await
                .expect("selected response is data");
            assert_eq!(response.status, status);
            assert_eq!(response.body, payload);
            assert_eq!(response.bytes_in(), payload.len() as u64);
            assert_eq!(response.headers[RETRY_AFTER], "600");
            assert_eq!(observed.load(Ordering::Relaxed), 0);
            assert_eq!(script.requests().len(), 1);
            assert_eq!(script.remaining(), 1);
            assert!(matches!(response.evidence().ledger().outcomes(),
                [crate::AttemptOutcome::Response(got)] if *got == status));
            assert_eq!(account.meter().bytes_in(), payload.len() as u64);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_core_unselected_statuses_keep_the_existing_policy() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::NOT_IMPLEMENTED, b"retry"),
            canned(StatusCode::OK, b"recovered"),
            canned(StatusCode::NOT_FOUND, b"missing"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
        let response = account
            .get("https://exchange.test/data")
            .statuses_as_data([StatusCode::NOT_FOUND])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(script.requests().len(), 2);
        let error = account
            .get("https://exchange.test/data")
            .statuses_as_data([StatusCode::NOT_IMPLEMENTED])
            .send()
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error.terminal(),
            Error::Status {
                code: StatusCode::NOT_FOUND,
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_core_status_conversion_keeps_replay_safety_and_shared_classification() {
        use bifrost_types::{AccountErrorKind, AccountOperation, RecoveryClass};
        for (code, expected_partial) in [(503, true), (412, false), (501, false)] {
            let script = ScriptedDispatch::new([canned(
                StatusCode::from_u16(code).unwrap(),
                b"provider body",
            )]);
            let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
            let response = account
                .post("https://exchange.test/write")
                .statuses_as_data([StatusCode::from_u16(code).unwrap()])
                .replay(ReplayPolicy::Never)
                .send()
                .await
                .unwrap();
            let error =
                classify_replay_error(response.into_status_error(), AccountOperation::SetIsRead);
            assert_eq!(
                matches!(error.recovery(), RecoveryClass::Reconcile(_)),
                expected_partial
            );
            assert_eq!(error.telemetry_fields().status, Some(code));
            if code == 412 {
                assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
            }
        }
        let mut headers = HeaderMap::new();
        headers.insert(LOCATION, hv("/after-write"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::SEE_OTHER, headers, b"committed"),
            canned(StatusCode::PRECONDITION_FAILED, b"terminal"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
        let response = account
            .post("https://exchange.test/write")
            .statuses_as_data([StatusCode::PRECONDITION_FAILED])
            .send()
            .await
            .unwrap();
        assert_eq!(response.evidence().ledger().outcomes().len(), 2);
        assert_eq!(
            response.evidence().verdict(),
            crate::CommitEvidence::MayHaveCommitted
        );
        let error = response.into_status_error();
        assert!(matches!(&error, Error::AttemptHistory { evidence, .. }
            if evidence.ledger().outcomes().len() == 2 && evidence.attempts().len() == 1));
        let error = classify_replay_error(error, AccountOperation::SetIsRead);
        assert!(matches!(error.recovery(), RecoveryClass::Reconcile(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_core_selected_stream_preserves_failures_and_releases_claims() {
        use futures::StreamExt;
        let mut headers = HeaderMap::new();
        headers.insert(LOCATION, hv("/redirected"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::TEMPORARY_REDIRECT, headers, b"hop"),
            Canned::StreamThenError {
                status: StatusCode::NOT_IMPLEMENTED,
                headers: HeaderMap::new(),
                chunks: vec![Bytes::from_static(b"prefix")],
                message: "broken response".into(),
            },
        ]);
        let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
        let limit = ConcurrencyLimit::new(1);
        let mut response = account
            .get("https://exchange.test/data")
            .statuses_as_data([StatusCode::NOT_IMPLEMENTED])
            .concurrency_limit(limit.clone())
            .send_streaming()
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(response.ledger.outcomes().len(), 2);
        assert_eq!(limit.in_flight(), 1);
        assert_eq!(response.body.next().await.unwrap().unwrap(), b"prefix"[..]);
        let error = response.body.next().await.unwrap().unwrap_err();
        assert!(matches!(&error, Error::AttemptHistory { evidence, .. }
            if evidence.ledger().outcomes().len() == 2 && evidence.attempts().len() == 2));
        assert_eq!(limit.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_core_selected_body_still_obeys_the_absolute_deadline() {
        let script = ScriptedDispatch::new([Canned::StreamThenStall {
            status: StatusCode::NOT_IMPLEMENTED,
            headers: HeaderMap::new(),
            chunks: vec![Bytes::from_static(b"prefix")],
        }]);
        let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
        let started = tokio::time::Instant::now();
        let deadline = RequestDeadline::from_timeout(Some(Duration::from_secs(2)));
        let error = account
            .get("https://exchange.test/data")
            .statuses_as_data([StatusCode::NOT_IMPLEMENTED])
            .deadline(deadline)
            .send()
            .await
            .err()
            .unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(2));
        assert!(matches!(
            error.terminal(),
            Error::Timeout {
                transmission_state: TransmissionState::Acknowledged,
            }
        ));
        assert!(matches!(error, Error::AttemptHistory { evidence, .. }
            if matches!(evidence.ledger().outcomes(),
                [crate::AttemptOutcome::Response(StatusCode::NOT_IMPLEMENTED)])));
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_core_selected_body_obeys_buffer_limits() {
        let script =
            ScriptedDispatch::new([canned(StatusCode::PRECONDITION_FAILED, &[b'x'; 4096])]);
        let mut spec = crate::AccountSpec::new(None);
        spec.max_buffered_response = Some(1024);
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("selected-limit".into()), spec);
        let error = account
            .get("https://exchange.test/data")
            .without_bearer_auth()
            .statuses_as_data([StatusCode::PRECONDITION_FAILED])
            .send()
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error.terminal(),
            Error::ResponseTooLarge { limit: 1024 }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_core_absolute_deadline_is_shared_across_protocol_legs() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::OK, b"first"),
            canned(StatusCode::OK, b"must remain unused"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
        let deadline = RequestDeadline::from_timeout(Some(Duration::from_secs(2)));
        account
            .get("https://exchange.test/first")
            .deadline(deadline)
            .send()
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(3)).await;
        let error = account
            .get("https://exchange.test/second")
            .deadline(deadline)
            .send()
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            Error::Timeout {
                transmission_state: TransmissionState::Unsent
            }
        ));
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_core_credential_response_history_stays_redacted() {
        let script = ScriptedDispatch::new([
            Canned::Error(Error::Network {
                message: "failed https://exchange.test/secret-token".into(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            canned(StatusCode::NOT_FOUND, b"missing"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), vec![], two_attempts());
        let response = account
            .get("https://exchange.test/secret-token")
            .credential_url()
            .statuses_as_data([StatusCode::NOT_FOUND])
            .send()
            .await
            .unwrap();
        assert_eq!(response.evidence().ledger().outcomes().len(), 2);
        assert!(!format!("{:?}", response.evidence()).contains("secret-token"));
        let error = response.into_status_error();
        assert!(!format!("{error:?}").contains("secret-token"));
        assert!(matches!(
            error.terminal(),
            Error::Status {
                code: StatusCode::NOT_FOUND,
                ..
            }
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn converted_session_builder_keeps_redirects_disabled() {
        let mut headers = HeaderMap::new();
        headers.insert(
            LOCATION,
            HeaderValue::from_static("https://other.test/upload"),
        );
        let script = ScriptedDispatch::new([canned_with_headers(
            StatusCode::TEMPORARY_REDIRECT,
            headers,
            b"redirect",
        )]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
        let builder: RequestBuilder = account
            .session_request(
                reqwest::Method::PUT,
                &crate::SessionUrl::new("https://session.test/upload"),
            )
            .body(Bytes::from_static(b"payload"))
            .into();
        assert!(builder.inner.session_policy);
        assert!(!builder.inner.bearer_auth);
        let response = builder.send().await.expect("unfollowed redirect");
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(script.requests().len(), 1);

        // Enabling redirects after conversion is refused, not ignored: the
        // locked session policy must never be silently weakened or dropped.
        let refused_script = ScriptedDispatch::new([canned(StatusCode::OK, b"unreached")]);
        let account = scripted_account(
            &refused_script,
            NetConfig::default(),
            Vec::new(),
            two_attempts(),
        );
        let builder: RequestBuilder = account
            .session_request(
                reqwest::Method::PUT,
                &crate::SessionUrl::new("https://session.test/upload"),
            )
            .into();
        let error = builder
            .follow_redirects(FollowRedirects::Enabled(Default::default()))
            .send()
            .await
            .err()
            .expect("deferred invalid request");
        assert!(matches!(
            error.terminal(),
            Error::InvalidRequest {
                field: "follow_redirects",
                ..
            }
        ));
        assert!(refused_script.requests().is_empty());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn final_response_without_retry_after_does_not_reuse_an_earlier_hint() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
            let script = ScriptedDispatch::new([
                canned_with_headers(status, headers, b"first"),
                canned(status, b"last"),
            ]);
            let account =
                scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
            let error = account
                .get("https://retry.test/resource")
                .send()
                .await
                .err()
                .expect("retry budget must exhaust");
            match error.terminal() {
                Error::RateLimited { retry_after, .. } => assert!(retry_after.is_none()),
                Error::RetryBudgetExhausted {
                    final_response: Some(response),
                    retry_after_history,
                    ..
                } => {
                    assert_eq!(retry_after_history.len(), 1);
                    assert!(parse_retry_after_hint(response.headers.get(RETRY_AFTER)).is_none());
                }
                other => panic!("unexpected retry result: {other:?}"),
            }
            assert_eq!(script.requests().len(), 2);
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn trusted_origins_gate_checks_explicit_and_automatic_credentials() {
        for explicit in [false, true] {
            let script = ScriptedDispatch::new([canned(StatusCode::OK, b"ok")]);
            let source = if explicit {
                None
            } else {
                Some(Arc::new(StaticTokenSource::new("test", None)) as Arc<dyn crate::TokenSource>)
            };
            let mut spec = crate::AccountSpec::new(source);
            let mut origins = crate::TrustedOrigins::default();
            origins.trust("https://allowed.test").unwrap();
            spec.trusted_origins = Some(origins);
            let account = crate::test_support::scripted_net(&script, NetConfig::default())
                .attach_account(crate::AccountId("origin-gate".to_owned()), spec);
            let mut request = account.get("https://denied.test/resource");
            if explicit {
                request = request
                    .without_bearer_auth()
                    .header("Authorization", "Basic secret");
            }
            assert!(request.send().await.is_err());
            assert!(script.requests().is_empty());
            // URL parsing erases empty userinfo. Retaining the lexical refusal
            // is necessary when the walker now receives a parsed target.
            let mut request = account.get("https://@allowed.test/resource");
            if explicit {
                request = request
                    .without_bearer_auth()
                    .header("Authorization", "Basic secret");
            }
            assert!(request.send().await.is_err());
            assert!(script.requests().is_empty());
            let mut request = account.get("https://allowed.test/resource");
            if explicit {
                request = request
                    .without_bearer_auth()
                    .header("Authorization", "Basic secret");
            }
            assert!(request.send().await.is_ok());
            assert_eq!(script.requests().len(), 1);
        }
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
                quota_scope: crate::QuotaScope::HostOnly,
                quota_per_second: 0.0001,
                cost_default: 1,
                burst: 1,
            }],
            retry,
        );
        let started = tokio::time::Instant::now();

        let response = account
            .post("https://retry.test/resource")
            .replay(ReplayPolicy::Idempotent)
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

    /// A request declared `credential_url` returns no error text naming its
    /// URL, on either send path and through the earlier attempts it carries,
    /// while the error still classifies as the transport failure it was. An
    /// undeclared request keeps the transport's text, URL included.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_credential_url_request_withholds_the_url_from_its_errors() {
        const URL: &str = "https://upload.test/session/abc?token=preauth";
        let failure = || {
            Canned::Error(Error::Network {
                message: format!("error sending request for url ({URL})"),
                transmission_state: TransmissionState::Unsent,
                source: None,
            })
        };
        for (credential, streaming) in [(true, false), (true, true), (false, false), (false, true)]
        {
            let script = ScriptedDispatch::new([failure(), failure()]);
            let account =
                scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
            let mut request = account
                .put(URL)
                .without_bearer_auth()
                .replay(ReplayPolicy::Idempotent)
                .body(Bytes::from_static(b"chunk"));
            if credential {
                request = request.credential_url();
            }
            let error = if streaming {
                request.send_streaming().await.map(|_| ()).unwrap_err()
            } else {
                request.send().await.map(|_| ()).unwrap_err()
            };
            assert_eq!(script.requests().len(), 2, "both attempts went out");
            assert!(
                matches!(error.terminal(), Error::Network { .. }),
                "{error:?}"
            );
            let rendered = format!("{error:?} {error}");
            assert_eq!(
                rendered.contains("upload.test"),
                !credential,
                "credential {credential}, streaming {streaming}: {rendered}"
            );
        }
    }

    /// A body the server starts and the connection then breaks fails after
    /// the response was admitted: for `send_streaming` the error comes out
    /// of the returned stream, past the point the request's own error was
    /// redacted; for `send` it comes out of body buffering. Under
    /// `credential_url` both are redacted, earlier-attempt history
    /// included. The marker rides the I/O error the scripted body fails
    /// with: `into_byte_stream` keeps the reqwest error, and through it that
    /// I/O error, as the `Network` source, which the debug rendering quotes
    /// and redaction must drop. reqwest attaches no URL to a body read
    /// error and the body `Network` message is fixed text here, so this
    /// pins the dropped source and the redacted history; message
    /// replacement itself is pinned by `redact_request_text`'s own test.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_credential_url_request_withholds_the_url_from_its_body_error() {
        use futures::StreamExt;
        const URL: &str = "https://upload.test/session/abc?token=preauth";
        const BODY_URL: &str = "https://body-read.test/session/abc?token=preauth";
        for (credential, streaming) in [(true, false), (true, true), (false, false), (false, true)]
        {
            let script = ScriptedDispatch::new([
                Canned::Error(Error::Network {
                    message: format!("error sending request for url ({URL})"),
                    transmission_state: TransmissionState::Unsent,
                    source: None,
                }),
                Canned::StreamThenError {
                    status: StatusCode::OK,
                    headers: HeaderMap::new(),
                    chunks: vec![Bytes::from_static(b"partial")],
                    message: format!("connection reset reading {BODY_URL}"),
                },
            ]);
            let account =
                scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
            let mut request = account
                .get(URL)
                .without_bearer_auth()
                .replay(ReplayPolicy::Idempotent);
            if credential {
                request = request.credential_url();
            }
            let error = if streaming {
                let mut response = request
                    .send_streaming()
                    .await
                    .expect("second attempt streams");
                let first = response.body.next().await.expect("a chunk");
                assert_eq!(first.expect("chunk ok"), Bytes::from_static(b"partial"));
                response
                    .body
                    .next()
                    .await
                    .expect("the body fails")
                    .unwrap_err()
            } else {
                request.send().await.map(|_| ()).unwrap_err()
            };
            assert_eq!(script.requests().len(), 2, "both attempts went out");
            assert!(
                matches!(error.terminal(), Error::Network { .. }),
                "{error:?}"
            );
            assert!(
                matches!(error, Error::AttemptHistory { .. }),
                "the earlier attempt rides the body error: {error:?}"
            );
            // History Debug retains local messages, but the body URL lives in
            // a foreign source that Debug deliberately omits. Inspect that source
            // separately so both credential redaction paths still have bite.
            let rendered = format!("{error:?} {error}");
            assert_eq!(
                rendered.contains("upload.test"),
                !credential,
                "credential {credential}, streaming {streaming}: {rendered}"
            );
            let Error::Network {
                message, source, ..
            } = error.terminal()
            else {
                unreachable!()
            };
            if credential {
                assert!(!rendered.contains("body-read.test"));
                assert!(message.contains("detail withheld"));
                assert!(source.is_none());
            } else {
                assert!(format!("{source:?}").contains("body-read.test"));
            }
        }
    }

    /// The duplicate-send bug. `send_error_to_error` classifies every
    /// non-connect, non-timeout reqwest failure as
    /// `Network { InFlight }` - the side effect may already have landed
    /// - and the loop used to replay it anyway, three times, before
    /// `into_account_error_with_throttle_scope` ever ran. Against Gmail's
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
            error.terminal(),
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
        assert_commit_envelope(&error);
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

    /// One logical update can contain either an absolute-state PATCH or a
    /// conditional create-PUT leg. The operation table cannot distinguish them.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn request_safety_can_narrow_an_idempotent_operation() {
        use bifrost_types::AccountOperation;

        for operation in [
            AccountOperation::EventUpdate,
            AccountOperation::ContactUpdate,
        ] {
            assert!(operation.is_idempotent());
            for replayable in [true, false] {
                let script = ScriptedDispatch::new([
                    in_flight_reset(),
                    canned(StatusCode::OK, b"recovered"),
                ]);
                let account =
                    scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
                let result = account
                    .put("https://dav.test/destination")
                    .header("If-None-Match", "*")
                    .idempotent(replayable)
                    .send()
                    .await;
                assert_eq!(result.is_ok(), replayable);
                assert_eq!(script.requests().len(), if replayable { 2 } else { 1 });
            }
        }
    }

    /// PATCH is conservative by default; explicit payload-level proof can
    /// widen it. An operation label is not present in this transport API.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn patch_replay_requires_explicit_request_safety() {
        for replayable in [false, true] {
            let script =
                ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"recovered")]);
            let account =
                scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
            let request = account.patch("https://graph.test/me/events/one");
            let request = if replayable {
                request.idempotent(true)
            } else {
                request
            };
            let result = request.send().await;
            assert_eq!(result.is_ok(), replayable);
            assert_eq!(script.requests().len(), if replayable { 2 } else { 1 });
        }
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

        assert!(matches!(error.terminal(), Error::Network { .. }));
        assert_eq!(script.requests().len(), 1);
        assert_commit_envelope(&error);
    }

    /// A 503 can follow a commit, so it is only a retry candidate: the
    /// resolved replay policy decides. A POST the caller declared
    /// `Idempotent` (a read carried over POST) keeps its status retry.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_503_on_an_explicitly_idempotent_post_still_retries() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::SERVICE_UNAVAILABLE, b"try again"),
            canned(StatusCode::OK, b"recovered"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .post("https://send.test/messages/send")
            .replay(ReplayPolicy::Idempotent)
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
            .expect("an explicitly replayable POST can retry an ambiguous status");

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
        spec.response_headers_timeout =
            crate::ResponseHeadersTimeout::Bounded(Duration::from_secs(7));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(256));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(256));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(256));
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
        spec.response_headers_timeout =
            crate::ResponseHeadersTimeout::Bounded(Duration::from_secs(30));
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

    /// Cookie and Proxy-Authorization are origin-bound credentials the walk
    /// never re-mints, so a cross-origin hop strips them exactly as it strips
    /// Authorization, with or without a HopPolicy. A same-origin hop keeps them.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn cross_origin_redirect_strips_cookie_and_proxy_authorization() {
        struct OpenPolicy;
        impl crate::HopPolicy for OpenPolicy {
            fn admit(
                &self,
                _prior: Option<&reqwest::Url>,
                _next: &reqwest::Url,
            ) -> Result<crate::HopCredential, Error> {
                Ok(crate::HopCredential::None)
            }
        }
        for with_policy in [false, true] {
            let redirect = |location: &'static str| {
                let mut headers = HeaderMap::new();
                headers.insert(LOCATION, HeaderValue::from_static(location));
                canned_with_headers(StatusCode::TEMPORARY_REDIRECT, headers, b"")
            };
            let script = ScriptedDispatch::new([
                redirect("https://origin.test/same"),
                redirect("https://target.test/final"),
                canned(StatusCode::OK, b"done"),
            ]);
            let account = scripted_account(
                &script,
                NetConfig::default(),
                Vec::new(),
                RetryPolicy::disabled(),
            );
            let mut builder = account
                .get("https://origin.test/start")
                .header(reqwest::header::COOKIE.as_str(), "session=origin-only")
                .header(
                    reqwest::header::PROXY_AUTHORIZATION.as_str(),
                    "Basic proxy-secret",
                );
            if with_policy {
                builder = builder.hop_policy(Arc::new(OpenPolicy));
            }
            builder.send().await.expect("redirect target succeeds");
            let requests = script.requests();
            assert_eq!(requests.len(), 3);
            for same_origin in &requests[..2] {
                assert!(same_origin.headers.contains_key(reqwest::header::COOKIE));
                assert!(
                    same_origin
                        .headers
                        .contains_key(reqwest::header::PROXY_AUTHORIZATION)
                );
            }
            assert_eq!(requests[2].url.as_str(), "https://target.test/final");
            assert!(
                !requests[2].headers.contains_key(reqwest::header::COOKIE),
                "policy={with_policy}: cookie crossed origins"
            );
            assert!(
                !requests[2]
                    .headers
                    .contains_key(reqwest::header::PROXY_AUTHORIZATION),
                "policy={with_policy}: proxy credential crossed origins"
            );
        }
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
            ..
        } = error.terminal()
        else {
            panic!("expected retry budget evidence");
        };
        assert_eq!(final_response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(final_response.body, Bytes::from_static(b"final"));
        assert_eq!(
            *retry_after_history,
            vec![
                bifrost_types::RetryHint::After(Duration::from_secs(1)),
                bifrost_types::RetryHint::After(Duration::from_secs(2))
            ]
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
            quota_scope: crate::QuotaScope::HostOnly,
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
            crate::QuotaScope::HostOnly,
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
                quota_scope: crate::QuotaScope::HostOnly,
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

    #[test]
    fn retry_hint_preserves_syntax_and_elapsed_dates() {
        use bifrost_types::RetryHint;
        let date = "Sun, 06 Nov 1994 08:49:37 GMT";
        let at = httpdate::parse_http_date(date).expect("fixed HTTP-date");
        assert_eq!(
            parse_retry_after_hint(Some(&hv(date))),
            Some(RetryHint::At(at))
        );
        assert_eq!(
            parse_retry_after_hint(Some(&hv(" 30 "))),
            Some(RetryHint::After(Duration::from_secs(30)))
        );
        assert_eq!(
            RetryHint::At(at).min_delay(at + Duration::from_secs(60)),
            Duration::ZERO
        );
        assert_eq!(
            retry_after_delay(
                Some(&hv(date)),
                at + Duration::from_secs(60),
                Duration::from_secs(10),
            ),
            Some(Duration::ZERO),
            "the retry loop treats an elapsed server date as an immediate retry"
        );
        assert_eq!(
            retry_after_delay(Some(&hv("30")), at, Duration::from_secs(10),),
            Some(Duration::from_secs(10)),
            "the policy cap applies only when resolving the actual wait"
        );
        for invalid in ["", "-5", "soon", "1.5"] {
            assert_eq!(parse_retry_after_hint(Some(&hv(invalid))), None);
        }
        assert_eq!(parse_retry_after_hint(None), None);
    }

    #[test]
    fn retry_after_hint_absent_header_is_none() {
        assert_eq!(parse_retry_after_hint(None), None);
    }

    #[test]
    fn retry_after_hint_parses_delta_seconds() {
        use bifrost_types::RetryHint;

        assert_eq!(
            parse_retry_after_hint(Some(&hv("30"))),
            Some(RetryHint::After(Duration::from_secs(30)))
        );
        assert_eq!(
            parse_retry_after_hint(Some(&hv("0"))),
            Some(RetryHint::After(Duration::ZERO)),
            "a zero hint is a real instruction, not an absent one"
        );
    }

    #[test]
    fn retry_after_hint_trims_surrounding_whitespace() {
        use bifrost_types::RetryHint;

        assert_eq!(
            parse_retry_after_hint(Some(&hv("  45  "))),
            Some(RetryHint::After(Duration::from_secs(45)))
        );
    }

    #[test]
    fn retry_after_hint_rejects_negative_and_garbage() {
        assert_eq!(parse_retry_after_hint(Some(&hv("-5"))), None);
        assert_eq!(parse_retry_after_hint(Some(&hv("soon"))), None);
        assert_eq!(parse_retry_after_hint(Some(&hv(""))), None);
        assert_eq!(parse_retry_after_hint(Some(&hv("1.5"))), None);
    }

    #[test]
    fn retry_after_hint_preserves_http_dates_without_resolving_them() {
        use bifrost_types::RetryHint;

        let date = "Fri, 01 Jan 2100 00:00:00 GMT";
        let at = httpdate::parse_http_date(date).expect("fixed HTTP-date");
        assert_eq!(
            parse_retry_after_hint(Some(&hv(date))),
            Some(RetryHint::At(at)),
            "the parser preserves the server deadline without resolving it against local time"
        );
    }

    #[test]
    fn retry_after_wait_caps_absurd_server_hints() {
        assert_eq!(
            parse_retry_after_hint(Some(&hv("31536000"))),
            Some(bifrost_types::RetryHint::After(Duration::from_secs(
                31_536_000
            ))),
            "the parsed evidence keeps the server's full hint"
        );
        assert_eq!(
            retry_after_delay(
                Some(&hv("31536000")),
                std::time::SystemTime::UNIX_EPOCH,
                RetryPolicy::default().honor_retry_after_cap,
            ),
            Some(Duration::from_secs(60)),
            "the retry policy caps the wait without rewriting the evidence"
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

        let classified = crate::account_error::into_account_error_with_throttle_scope(
            error,
            crate::account_error::NetErrorContext {
                provider: None,
                protocol: bifrost_types::Protocol::Jmap,
                operation: bifrost_types::AccountOperation::Hydrate,
                scope: None,
            },
            None,
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
                    quota_scope: crate::QuotaScope::HostOnly,
                    quota_per_second: 1000.0,
                    cost_default: 1,
                    burst: 100,
                },
                RateLimit {
                    host: "small.test".to_owned(),
                    quota_scope: crate::QuotaScope::HostOnly,
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

    #[tokio::test(start_paused = true)]
    async fn concurrent_streams_share_one_inbound_schedule() {
        let script = ScriptedDispatch::new((0..4).map(|_| canned(StatusCode::OK, b"0123456789")));
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        spec.response_headers_timeout =
            crate::ResponseHeadersTimeout::Bounded(Duration::from_millis(100));
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("upload".into()), spec);
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        spec.response_headers_timeout =
            crate::ResponseHeadersTimeout::Bounded(Duration::from_millis(100));
        let account = crate::test_support::scripted_net(&script, NetConfig::default())
            .attach_account(crate::AccountId("upload-stall".into()), spec);
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(1024));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(10));
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
    #[test]
    fn replay_policy_resolves_methods_preconditions_and_explicit_declarations() {
        use reqwest::Method;
        for method in [
            Method::GET,
            Method::HEAD,
            Method::OPTIONS,
            Method::TRACE,
            Method::PUT,
            Method::DELETE,
            Method::POST,
            Method::PATCH,
            Method::from_bytes(b"MOVE").unwrap(),
        ] {
            for header in [
                None,
                Some("If-Match"),
                Some("If-None-Match"),
                Some("If-Unmodified-Since"),
                Some("If"),
                Some("If-Schedule-Tag-Match"),
                Some("If-Modified-Since"),
                Some("If-Range"),
                Some("Overwrite"),
            ] {
                let mut headers = HeaderMap::new();
                if let Some(header) = header {
                    headers.insert(
                        HeaderName::from_bytes(header.as_bytes()).unwrap(),
                        HeaderValue::from_static("value"),
                    );
                }
                let conditional = matches!(
                    header,
                    Some(
                        "If-Match"
                            | "If-None-Match"
                            | "If-Unmodified-Since"
                            | "If"
                            | "If-Schedule-Tag-Match"
                    )
                );
                assert_eq!(
                    request_is_replayable(None, &method, &headers),
                    method.is_safe()
                        || ((method == Method::PUT || method == Method::DELETE) && !conditional)
                );
                assert!(request_is_replayable(
                    Some(ReplayPolicy::Idempotent),
                    &method,
                    &headers
                ));
                assert!(!request_is_replayable(
                    Some(ReplayPolicy::Never),
                    &method,
                    &headers
                ));
                assert_eq!(
                    request_is_replayable(
                        Some(ReplayPolicy::IdempotentUnlessConditional),
                        &method,
                        &headers
                    ),
                    method.is_safe() || !conditional
                );
            }
        }
    }

    fn assert_commit_envelope(error: &Error) {
        assert!(
            matches!(error, Error::AttemptHistory { evidence, .. } if evidence.verdict() == crate::CommitEvidence::MayHaveCommitted && evidence.attempts().is_empty()),
            "{error:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_conditional_put_is_not_replayed_after_an_in_flight_drop() {
        for conditional in [false, true] {
            let script = ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"ok")]);
            let account =
                scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
            let mut request = account.put("https://replay.test/resource");
            if conditional {
                request = request.header("If-Match", "v1");
            }
            let result = request.send().await;
            assert_eq!(script.requests().len(), if conditional { 1 } else { 2 });
            if conditional {
                let error = result.err().unwrap();
                assert_commit_envelope(&error);
                assert!(matches!(
                    error.terminal(),
                    Error::Network {
                        transmission_state: TransmissionState::InFlight,
                        ..
                    }
                ));
            } else {
                assert!(result.is_ok());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_method_rewriting_redirect_restores_the_method_default() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_static("/result"),
        );
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: StatusCode::SEE_OTHER,
                headers,
                body: Bytes::new(),
            },
            in_flight_reset(),
            canned(StatusCode::OK, b"ok"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
        account
            .post("https://replay.test/action")
            .replay(ReplayPolicy::Never)
            .send()
            .await
            .unwrap();
        assert_eq!(script.requests().len(), 3);
        assert_eq!(script.requests()[1].method, reqwest::Method::GET);
    }

    #[tokio::test]
    async fn hop_policy_replaces_credentials_per_origin_and_retains_commit_evidence() {
        struct Policy;
        impl crate::HopPolicy for Policy {
            fn method_rewrite(&self) -> crate::MethodRewrite {
                crate::MethodRewrite::PreserveAndRefuse303
            }

            fn admit(
                &self,
                _prior: Option<&reqwest::Url>,
                next: &reqwest::Url,
            ) -> Result<crate::HopCredential, Error> {
                let value = match next.host_str() {
                    Some("first.test") => "Basic first-only",
                    Some("second.test") => "Basic second-only",
                    _ => {
                        return Err(Error::RedirectRejected {
                            message: "unadmitted origin".into(),
                        });
                    }
                };
                Ok(crate::HopCredential::Header(HeaderValue::from_static(
                    value,
                )))
            }
        }
        let redirect = |location: &'static str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                reqwest::header::LOCATION,
                HeaderValue::from_static(location),
            );
            Canned::Response {
                status: StatusCode::FOUND,
                headers,
                body: Bytes::new(),
            }
        };
        let script = ScriptedDispatch::new([
            redirect("https://second.test/new"),
            redirect("https://evil.test/steal"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
        let error = account
            .post("https://first.test/start")
            .hop_policy(Arc::new(Policy))
            .body(Bytes::from_static(b"write"))
            .replay(ReplayPolicy::Never)
            .send()
            .await
            .err()
            .expect("third origin refused");
        // `assert_commit_envelope` requires an EMPTY attempt list; this walk
        // answered two hops, so check the envelope directly below.
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].headers[AUTHORIZATION], "Basic first-only");
        assert_eq!(requests[1].headers[AUTHORIZATION], "Basic second-only");
        assert_eq!(requests[1].method, reqwest::Method::POST);
        assert_eq!(requests[1].body.as_deref(), Some(b"write".as_slice()));
        assert!(matches!(
            error,
            Error::AttemptHistory { evidence, .. } if evidence.verdict() == crate::CommitEvidence::MayHaveCommitted && evidence.attempts().len() == 2
        ));
    }

    /// A target refusal raised at the TOP of the next hop's loop iteration,
    /// before `attempts.begin()`, still leaves through the one completion
    /// boundary: the answered first hop of an unreplayable write is retained
    /// as possibly committed, never reported as a clean local refusal.
    #[tokio::test]
    async fn a_loop_top_hop_refusal_keeps_the_answered_hops_commit_evidence() {
        struct Policy;
        impl crate::HopPolicy for Policy {
            fn method_rewrite(&self) -> crate::MethodRewrite {
                crate::MethodRewrite::PreserveAndRefuse303
            }

            fn admit(
                &self,
                _prior: Option<&reqwest::Url>,
                _next: &reqwest::Url,
            ) -> Result<crate::HopCredential, Error> {
                Ok(crate::HopCredential::None)
            }

            fn check_target(
                &self,
                _method: &reqwest::Method,
                url: &reqwest::Url,
                _headers: &HeaderMap,
            ) -> Result<(), Error> {
                if url.path() == "/excluded" {
                    return Err(Error::RedirectRejected {
                        message: "excluded target".into(),
                    });
                }
                Ok(())
            }
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_static("/excluded"),
        );
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::TEMPORARY_REDIRECT,
            headers,
            body: Bytes::new(),
        }]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
        let error = account
            .post("https://first.test/start")
            .hop_policy(Arc::new(Policy))
            .body(Bytes::from_static(b"write"))
            .replay(ReplayPolicy::Never)
            .send()
            .await
            .err()
            .expect("the excluded hop is refused");
        assert_eq!(script.requests().len(), 1, "the refused hop never goes out");
        assert!(matches!(
            &error,
            Error::AttemptHistory { evidence, .. } if evidence.verdict() == crate::CommitEvidence::MayHaveCommitted && evidence.attempts().len() == 1
        ));
    }

    #[test]
    fn status_replay_obeys_policy_except_proven_refusals() {
        for code in [401, 405, 429, 501, 409, 500, 502, 503, 504, 599] {
            let status = StatusCode::from_u16(code).unwrap();
            assert!(status_error_is_replayable(status, true));
            assert_eq!(
                status_error_is_replayable(status, false),
                matches!(code, 401 | 429 | 405 | 501)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_exhausted_status_on_an_unreplayable_request_is_wrapped() {
        for (status, replay) in [
            (StatusCode::SERVICE_UNAVAILABLE, None),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some(ReplayPolicy::Idempotent),
            ),
            (StatusCode::TOO_MANY_REQUESTS, None),
        ] {
            let script = ScriptedDispatch::new([canned(status, b"refused")]);
            let account = scripted_account(
                &script,
                NetConfig::default(),
                Vec::new(),
                RetryPolicy::disabled(),
            );
            let mut request = account.post("https://replay.test/action");
            if let Some(policy) = replay {
                request = request.replay(policy);
            }
            let error = request.send().await.err().unwrap();
            if status == StatusCode::TOO_MANY_REQUESTS {
                assert!(matches!(error, Error::RateLimited { .. }));
            } else if replay.is_some() {
                assert!(matches!(error, Error::RetryBudgetExhausted { .. }));
            } else {
                assert_commit_envelope(&error);
                assert!(
                    matches!(error.terminal(), Error::RetryBudgetExhausted { final_response: Some(r), .. } if r.status == status),
                    "{error:?}"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_status_retries_obey_the_resolved_request_policy() {
        for code in [500, 502, 503, 504, 599, 409] {
            for (method, declared, conditional) in [
                (reqwest::Method::POST, Some(ReplayPolicy::Never), false),
                (reqwest::Method::POST, None, false),
                (reqwest::Method::PUT, None, true),
                (reqwest::Method::DELETE, None, true),
                (
                    reqwest::Method::PUT,
                    Some(ReplayPolicy::IdempotentUnlessConditional),
                    true,
                ),
                (
                    reqwest::Method::DELETE,
                    Some(ReplayPolicy::IdempotentUnlessConditional),
                    true,
                ),
                (reqwest::Method::GET, Some(ReplayPolicy::Never), false),
            ] {
                for control in [false, true] {
                    let status = StatusCode::from_u16(code).unwrap();
                    let mut headers = HeaderMap::new();
                    headers.insert("request-id", HeaderValue::from_static("replay-test"));
                    headers.insert(
                        RETRY_AFTER,
                        HeaderValue::from_static(if control { "0" } else { "60" }),
                    );
                    let script = ScriptedDispatch::new([
                        Canned::Response {
                            status,
                            headers,
                            body: Bytes::from_static(b"ambiguous"),
                        },
                        canned(StatusCode::OK, b"ok"),
                    ]);
                    let mut policy = two_attempts();
                    policy.statuses.push(status);
                    let account =
                        scripted_account(&script, NetConfig::default(), Vec::new(), policy);
                    let mut request = account
                        .request(method.clone(), "https://replay.test/resource")
                        .timeout(Duration::from_secs(1));
                    if let Some(policy) = declared {
                        request = request.replay(policy);
                    }
                    if conditional {
                        request = request.header("If-Match", "v1");
                    }
                    if control {
                        request = request.replay(ReplayPolicy::Idempotent);
                    }
                    let started = tokio::time::Instant::now();
                    let result = request.send().await;
                    if control {
                        assert!(result.is_ok());
                        assert_eq!(script.requests().len(), 2);
                    } else {
                        assert_eq!(started.elapsed(), Duration::ZERO);
                        let error = result.err().unwrap();
                        assert_commit_envelope(&error);
                        assert!(
                            matches!(error.terminal(), Error::RetryBudgetExhausted { final_response: Some(r), retry_after_history,
            ..
        } if r.status == status && r.body.as_ref() == b"ambiguous" && r.headers["request-id"] == "replay-test" && retry_after_history == &[bifrost_types::RetryHint::After(Duration::from_secs(60))])
                        );
                        assert_eq!(script.requests().len(), 1);
                        assert_eq!(script.remaining(), 1);
                    }
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_never_request_still_retries_a_429_refusal() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("0"));
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: StatusCode::TOO_MANY_REQUESTS,
                headers,
                body: Bytes::new(),
            },
            canned(StatusCode::OK, b"ok"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
        account
            .post("https://replay.test/action")
            .replay(ReplayPolicy::Never)
            .send()
            .await
            .unwrap();
        assert_eq!(script.requests().len(), 2);
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("60"));
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers,
            body: Bytes::from_static(b"rate refusal"),
        }]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        let error = account
            .post("https://replay.test/action")
            .replay(ReplayPolicy::Never)
            .send()
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, Error::RateLimited { retry_after: Some(delay), final_response } if delay == bifrost_types::RetryHint::After(Duration::from_secs(60)) && final_response.body.as_ref() == b"rate refusal")
        );
    }

    fn classify_replay_error(
        error: Error,
        operation: bifrost_types::AccountOperation,
    ) -> bifrost_types::AccountError {
        crate::into_account_error_with_throttle_scope(
            error,
            crate::NetErrorContext {
                provider: None,
                protocol: bifrost_types::error::Protocol::Graph,
                operation,
                scope: None,
            },
            None,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreplayable_drop_reconciles_under_an_idempotent_operation() {
        for control in [false, true] {
            let script = ScriptedDispatch::new([in_flight_reset(), in_flight_reset()]);
            let account =
                scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
            let mut request = account
                .put("https://replay.test/resource")
                .header("If-Match", "v1");
            if control {
                request = request.replay(ReplayPolicy::Idempotent);
            }
            let error = classify_replay_error(
                request.send().await.err().unwrap(),
                bifrost_types::AccountOperation::EventUpdate,
            );
            if control {
                assert!(matches!(
                    error.recovery(),
                    bifrost_types::RecoveryClass::Retry(_)
                ));
            } else {
                assert!(matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Transport(_)
                ));
                assert!(
                    matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.reason == bifrost_types::ReconcileReason::TransportDropAfterSend && a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget])
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_exhausted_status_reconciles_under_an_idempotent_operation() {
        let script = ScriptedDispatch::new([canned(StatusCode::SERVICE_UNAVAILABLE, b"ambiguous")]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        let error = classify_replay_error(
            account
                .post("https://replay.test/action")
                .send()
                .await
                .err()
                .unwrap(),
            bifrost_types::AccountOperation::SetIsRead,
        );
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        ));
        assert!(
            matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_policy_stopped_status_reconciles_before_a_duplicate_effect() {
        for operation in [
            bifrost_types::AccountOperation::EventCreate,
            bifrost_types::AccountOperation::Send,
            bifrost_types::AccountOperation::SetIsRead,
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("request-id", HeaderValue::from_static("commit"));
            let script = ScriptedDispatch::new([
                Canned::Response {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    headers,
                    body: Bytes::from_static(b"commit unknown"),
                },
                canned(StatusCode::OK, b"duplicate"),
            ]);
            let account =
                scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
            let error = classify_replay_error(
                account
                    .post("https://replay.test/action")
                    .replay(ReplayPolicy::Never)
                    .send()
                    .await
                    .err()
                    .unwrap(),
                operation,
            );
            let bifrost_types::RecoveryClass::Reconcile(a) = error.recovery() else {
                panic!("{error:?}")
            };
            let expected = if operation == bifrost_types::AccountOperation::SetIsRead {
                vec![bifrost_types::ReconcileAction::CheckTarget]
            } else {
                vec![
                    bifrost_types::ReconcileAction::CheckTarget,
                    bifrost_types::ReconcileAction::DedupeByClientId,
                ]
            };
            assert_eq!(a.guidance.actions, expected);
            assert_eq!(script.requests().len(), 1);
            assert_eq!(script.remaining(), 1);
            assert!(
                !error
                    .support_consented()
                    .support_text
                    .iter()
                    .any(|text| text.contains("commit unknown"))
            );
            assert_eq!(error.telemetry_fields().request_id, Some("commit"));
            assert_eq!(error.telemetry_fields().status, Some(503));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_post_that_may_have_committed_reconciles_under_an_idempotent_operation() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::UNAUTHORIZED, b"refresh"),
            canned(StatusCode::SERVICE_UNAVAILABLE, b"ambiguous"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());
        let error = account
            .post("https://replay.test/action")
            .replay(ReplayPolicy::Never)
            .send()
            .await
            .err()
            .unwrap();
        assert!(
            matches!(&error, Error::AttemptHistory { evidence, .. } if evidence.verdict() == crate::CommitEvidence::MayHaveCommitted && matches!(evidence.attempts(), [Error::Status { code: StatusCode::UNAUTHORIZED, .. }]))
        );
        assert!(
            matches!(error.terminal(), Error::RetryBudgetExhausted { final_response: Some(r), .. } if r.status == StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(script.requests().len(), 2);
        let error = classify_replay_error(error, bifrost_types::AccountOperation::SetIsRead);
        assert!(
            matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.reason == bifrost_types::ReconcileReason::PartialCompletionSignal && a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget])
        );
    }
}
