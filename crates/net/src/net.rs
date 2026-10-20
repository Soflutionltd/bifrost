//! The top-level `Net` handle and the per-account `AccountNet` view.
//!
//! Protocol crates hold an `AccountNet` and never see the underlying
//! `reqwest::Client`. `Net` itself is process-wide; one instance is
//! shared across every account so connection pooling, rate limiting,
//! and bandwidth metering can coordinate across accounts that share a
//! host.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use bifrost_types::TransmissionState;
use futures::StreamExt;
use reqwest::header::RANGE;

use crate::auth::{DEFAULT_TOKEN_MAX_AGE, OAuthRefresher, TokenSource};
use crate::bandwidth::{AccountMeter, BandwidthMeter};
use crate::budget::{BandwidthBudget, BandwidthDirection, BandwidthRegistry};
use crate::concurrency::{ConcurrencyLimit, WeakLimit};
use crate::config::NetConfig;
use crate::error::{Error, RangeFailureKind};
use crate::rate::{RateKey, RateLimit, RateLimitGovernor};
use crate::request::{
    ByteStream, Dispatch, InternalStreaming, RequestBuilder, ReqwestDispatch, send_streaming_inner,
};
use crate::retry::RetryPolicy;
use crate::{AccountId, ByteRange, Priority};
use crate::{DEFAULT_MAX_BUFFERED_RESPONSE, FollowRedirects};

type SharedNetResources = (
    Arc<BandwidthRegistry>,
    RateLimitGovernor,
    Arc<BandwidthMeter>,
    Arc<crate::hold::HoldRegistry>,
);

/// Process-wide HTTP transport. Holds the shared reqwest client, the
/// per-host rate-limit governor, and the bandwidth meter. Cheap to
/// clone via the inner `Arc`.
#[derive(Clone)]
pub struct Net {
    inner: Arc<NetInner>,
}

/// Backing state for `Net`. Kept private so the Phase 2
/// implementation can rearrange without touching call sites.
pub(crate) struct NetInner {
    /// Caller-supplied configuration. Snapshotted at construction;
    /// runtime mutations go through the per-account handles.
    pub(crate) config: NetConfig,
    /// Shared reqwest client built from `config` at `Net::new`.
    pub(crate) client: reqwest::Client,
    /// Crate-private wire dispatch seam. Production delegates to
    /// reqwest; unit tests install a scripted in-process dispatcher.
    pub(crate) dispatch: Arc<dyn Dispatch>,
    /// Per-host rate-limit governor. Shared across every account so
    /// multi-account quota sharing on Gmail / Graph works out of the
    /// box.
    pub(crate) governor: RateLimitGovernor,
    /// Bandwidth meter, partitioned per account.
    pub(crate) meter: Arc<BandwidthMeter>,
    bandwidth_registry: Arc<BandwidthRegistry>,
    /// Successfully registered hosts, indexed first by `AccountId`
    /// and then by the exact attachment token.
    pub(crate) account_hosts: Mutex<HashMap<AccountId, HashMap<u64, Vec<RateKey>>>>,
    /// Monotone identity for each `attach_account` registration.
    /// AccountNet carries this token so teardown targets the exact
    /// attachment even when the same AccountId is reopened before an
    /// older handle drops.
    pub(crate) next_registration_id: AtomicU64,
    /// Keyed concurrency scopes shared only by accounts on this Net.
    concurrency_limits: Mutex<HashMap<String, WeakLimit>>,
    holds: Arc<crate::hold::HoldRegistry>,
}

impl Net {
    /// Open a provider hold shared by every account on this transport.
    #[must_use]
    pub fn shared_hold(&self, key: impl Into<String>) -> crate::SharedHold {
        crate::SharedHold::new(Arc::clone(&self.inner.holds), key.into())
    }

    /// Construct a `Net` from a `NetConfig`. Builds the shared
    /// `reqwest::Client` with the requested pool, HTTP/2 keepalive,
    /// TCP keepalive and trust-store settings. Request timeouts,
    /// User-Agent, and redirect behavior are account-scoped.
    /// Native-tls only.
    ///
    /// # Errors
    /// Returns `Error::InvalidRequest` if `native_tls` rejects a
    /// supplied root certificate, if `reqwest::Certificate::from_der`
    /// rejects the DER re-encoding, or if `reqwest::ClientBuilder::build`
    /// itself fails. The same `NetConfig` would fail again, so these
    /// are local configuration failures rather than retryable
    /// transport setup failures.
    // `Error` is a wide enum (boxed `dyn Error`, `Bytes`, `HeaderMap`)
    // and a one-shot constructor returning it trips
    // `result_large_err`. Boxing the `Err` here would force the rest
    // of the public surface to box too, which is the wrong tradeoff
    // for a process-wide constructor that runs once at startup.
    #[allow(clippy::result_large_err)]
    pub fn new(config: NetConfig) -> Result<Self, Error> {
        Self::new_with_dispatch(config, Arc::new(ReqwestDispatch))
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn new_with_dispatch(
        config: NetConfig,
        dispatch: Arc<dyn Dispatch>,
    ) -> Result<Self, Error> {
        Self::build(config, dispatch, Arc::new(BandwidthRegistry::new()))
    }

    /// Build with an injected registry shared with the engine and stream
    /// factories. Matching account ids coordinate only within one registry.
    pub fn with_bandwidth_registry(
        config: NetConfig,
        registry: Arc<BandwidthRegistry>,
    ) -> Result<Self, Error> {
        Self::build(config, Arc::new(ReqwestDispatch), registry)
    }

    /// Registry used by all accounts attached to this transport.
    #[must_use]
    pub fn bandwidth_registry(&self) -> &Arc<BandwidthRegistry> {
        &self.inner.bandwidth_registry
    }

    fn build(
        config: NetConfig,
        dispatch: Arc<dyn Dispatch>,
        bandwidth_registry: Arc<BandwidthRegistry>,
    ) -> Result<Self, Error> {
        let mut builder = reqwest::ClientBuilder::new()
            .pool_idle_timeout(config.pool_idle_timeout)
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .http2_keep_alive_interval(config.http2_keep_alive_interval)
            .http2_keep_alive_timeout(config.http2_keep_alive_timeout)
            .tcp_keepalive(Some(config.tcp_keepalive))
            .danger_accept_invalid_certs(config.dangerous_accept_invalid_certs);
        if let Some(connect_timeout) = config.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }

        // bifrost-net owns the redirect loop unconditionally. Reqwest's
        // default policy follows 3xx but without RFC 7231 method
        // rewriting or a trusted-host allowlist; our loop in
        // `redirect.rs` plus `request.rs::send_streaming_inner` enforces
        // both. Installing `Policy::none()` here is the only handoff
        // point - everything else is bifrost-net code.
        builder = builder.redirect(reqwest::redirect::Policy::none());

        for cert in &config.root_certs {
            // `native_tls::Certificate` -> `reqwest::Certificate` via
            // DER round-trip; both backends share the same DER format
            // so this is lossless under normal conditions. A failure
            // here means the caller handed us a corrupt cert.
            let der = cert.to_der().map_err(|e| Error::InvalidRequest {
                field: "root_certs",
                detail: format!("native_tls certificate failed to encode as DER: {e}"),
            })?;
            let reqwest_cert =
                reqwest::Certificate::from_der(&der).map_err(|e| Error::InvalidRequest {
                    field: "root_certs",
                    detail: format!("reqwest rejected DER certificate: {e}"),
                })?;
            builder = builder.add_root_certificate(reqwest_cert);
        }

        let client = builder.build().map_err(|e| Error::InvalidRequest {
            field: "client_config",
            detail: format!("reqwest client build failed: {e}"),
        })?;

        Ok(Self {
            inner: Arc::new(NetInner {
                config,
                client,
                dispatch,
                governor: RateLimitGovernor::new(),
                meter: Arc::new(BandwidthMeter::new()),
                bandwidth_registry,
                account_hosts: Mutex::new(HashMap::new()),
                next_registration_id: AtomicU64::new(1),
                concurrency_limits: Mutex::new(HashMap::new()),
                holds: Arc::default(),
            }),
        })
    }

    // TLS trust requires separate clients, but does not partition process quota,
    // provider holds, bandwidth admission or observation.
    fn shared_build(config: NetConfig) -> Self {
        static RESOURCES: OnceLock<SharedNetResources> = OnceLock::new();
        let (registry, governor, meter, holds) = RESOURCES.get_or_init(|| {
            (
                Arc::new(BandwidthRegistry::new()),
                RateLimitGovernor::new(),
                Arc::new(BandwidthMeter::new()),
                Arc::default(),
            )
        });
        let mut net = Self::with_bandwidth_registry(config, Arc::clone(registry))
            .expect("default NetConfig should build");
        let inner = Arc::get_mut(&mut net.inner).expect("new Net is uniquely owned");
        inner.governor = governor.clone();
        inner.meter = Arc::clone(meter);
        inner.holds = Arc::clone(holds);
        net
    }

    /// Shared default transport for simple constructors. Applications
    /// with explicit transport ownership can still call `Net::new`
    /// and pass account handles into protocol clients directly.
    #[must_use]
    pub fn shared_default() -> Self {
        static DEFAULT: OnceLock<Net> = OnceLock::new();
        DEFAULT
            .get_or_init(|| Net::shared_build(NetConfig::default()))
            .clone()
    }

    /// Shared transport that accepts invalid TLS certificates.
    ///
    /// TLS trust is a property of the reqwest client, so it cannot be
    /// per-account on a shared client. Rather than silently discarding
    /// a caller's explicit request to accept invalid certificates -
    /// which would leave a self-signed deployment failing on every
    /// HTTP request while the caller believes it opted in - accounts
    /// that ask for it share a second process-wide client. Sharing is
    /// preserved within each trust class: at most two clients exist,
    /// not one per account.
    ///
    /// Prefer [`Net::shared_default`]. This exists for deployments
    /// against self-signed endpoints and is as dangerous as its name
    /// suggests.
    #[must_use]
    pub fn shared_accepting_invalid_certs() -> Self {
        static SHARED: OnceLock<Net> = OnceLock::new();
        SHARED
            .get_or_init(|| {
                let config = NetConfig {
                    dangerous_accept_invalid_certs: true,
                    ..NetConfig::default()
                };
                Net::shared_build(config)
            })
            .clone()
    }

    /// Pick the shared transport matching a caller's TLS trust
    /// requirement. See [`Net::shared_accepting_invalid_certs`] for
    /// why this is a choice between two shared clients rather than a
    /// per-account setting.
    #[must_use]
    pub fn shared_for_tls(accept_invalid_certs: bool) -> Self {
        if accept_invalid_certs {
            Self::shared_accepting_invalid_certs()
        } else {
            Self::shared_default()
        }
    }

    /// Register an account with the transport. Returns an
    /// `AccountNet` carrying the per-account token source, rate
    /// limits, and default retry policy.
    pub fn attach_account(&self, id: AccountId, spec: AccountSpec) -> AccountNet {
        // Register the meter so the per-account counters exist before
        // any request lands. Rate-limit registration walks the host
        // list the caller supplied.
        self.inner.meter.register_account(id.clone());
        let account_meter = self.inner.meter.account(id.clone());
        let mut registered_hosts = Vec::with_capacity(spec.hosts.len());
        for limit in &spec.hosts {
            if self.inner.governor.register(limit.clone()) {
                registered_hosts.push(RateKey::new(&limit.host, &limit.quota_scope));
            }
        }
        // Default scope selection for requests that do not name one.
        // Built from the keys that actually registered, so a rejected
        // declaration cannot point requests at a bucket that does not
        // exist. Host alone cannot name a bucket once an account
        // declares two scopes on one host, and silently letting the
        // last declaration win would leave the other bucket registered,
        // refcounted, and permanently unreachable. First declaration
        // wins and the ambiguity is announced; reaching the others is
        // what `RequestBuilder::quota_scope` is for.
        let mut rate_scopes: HashMap<String, String> = HashMap::new();
        for key in &registered_hosts {
            match rate_scopes.get(&key.host) {
                Some(existing) if existing == &key.quota_scope => {}
                Some(existing) => {
                    tracing::warn!(
                        host = %key.host,
                        default_scope = %existing,
                        also_declared = %key.quota_scope,
                        "account declares multiple quota scopes for one host; \
                         requests that do not call RequestBuilder::quota_scope \
                         will debit the first-declared scope",
                    );
                }
                None => {
                    rate_scopes.insert(key.host.clone(), key.quota_scope.clone());
                }
            }
        }
        let registration_id = self
            .inner
            .next_registration_id
            .fetch_add(1, Ordering::Relaxed);
        {
            let mut map = self
                .inner
                .account_hosts
                .lock()
                .expect("net account_hosts lock poisoned");
            map.entry(id.clone())
                .or_default()
                .insert(registration_id, registered_hosts);
        }
        AccountNet {
            inner: Arc::new(AccountNetInner {
                net: self.clone(),
                bandwidth_budget: self.inner.bandwidth_registry.budget(id.clone()),
                account: id,
                registration_id,
                meter: account_meter,
                token_source: spec.token_source.map(|source| {
                    if source.is_coordinating() {
                        source
                    } else {
                        Arc::new(OAuthRefresher::new(source).with_max_age(spec.token_max_age))
                            as Arc<dyn TokenSource>
                    }
                }),
                rate_scopes,
                default_retry: spec.default_retry,
                request_timeout: spec.request_timeout,
                response_headers_timeout: spec.response_headers_timeout.or(spec.connect_timeout),
                read_timeout: spec.read_timeout,
                max_buffered_response: spec.max_buffered_response,
                concurrency_limit: spec.concurrency_limit,
                user_agent: spec.user_agent,
                follow_redirects: spec.follow_redirects,
                priority: AtomicU8::new(Priority::Foreground as u8),
            }),
        }
    }

    /// Drop one registration for an account id.
    ///
    /// Prefer `AccountNet::detach()` when the handle is available: it
    /// targets an exact attachment token and is the only form that is
    /// correct when two live handles share an `AccountId`. This
    /// compatibility path has no handle to key on, so it removes the
    /// *oldest* registration under `id` - registration tokens are
    /// monotone, so "oldest" is well defined and this cannot become an
    /// arbitrary choice driven by `HashMap` iteration order.
    pub fn detach_account(&self, id: &AccountId) {
        self.detach_registration(id, None);
    }

    fn detach_registration(&self, id: &AccountId, registration_id: Option<u64>) {
        let hosts = {
            let mut map = self
                .inner
                .account_hosts
                .lock()
                .expect("net account_hosts lock poisoned");
            let Some(registrations) = map.get_mut(id) else {
                return;
            };
            // No token supplied means the id-only compatibility path;
            // take the oldest registration rather than whatever the
            // map happens to yield first.
            let token = registration_id.or_else(|| registrations.keys().copied().min());
            let Some(token) = token else {
                return;
            };
            let Some(hosts) = registrations.remove(&token) else {
                return;
            };
            if registrations.is_empty() {
                map.remove(id);
            }
            hosts
        };
        self.inner.meter.forget_account(id);
        for key in hosts {
            self.inner
                .governor
                .unregister_scoped(&key.host, &key.quota_scope);
        }
    }

    /// Process-wide bandwidth meter handle. Per-account readings go
    /// through `AccountNet::meter`.
    #[must_use]
    pub fn meter(&self) -> &BandwidthMeter {
        &self.inner.meter
    }

    /// Per-host rate-limit governor.
    #[must_use]
    pub fn governor(&self) -> &RateLimitGovernor {
        &self.inner.governor
    }

    /// Configuration snapshot.
    #[must_use]
    pub fn config(&self) -> &NetConfig {
        &self.inner.config
    }

    /// Crate-internal accessor for the underlying reqwest client.
    /// Used by `RequestBuilder::send` to dispatch the actual HTTP
    /// request; intentionally not exposed publicly so callers cannot
    /// bypass retry, rate-limit, and metering.
    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.inner.client
    }

    pub(crate) fn dispatch(&self) -> &Arc<dyn Dispatch> {
        &self.inner.dispatch
    }
}

/// Account-scoped view onto `Net`. Carries the account identity used
/// for metering, the token source for OAuth, and the rate-limit plus
/// retry defaults the protocol crate supplied at registration.
///
/// `Clone` is one `Arc` refcount bump. Protocol crates typically clone
/// this once per spawned task so each stream owns its own handle.
#[derive(Clone)]
pub struct AccountNet {
    pub(crate) inner: Arc<AccountNetInner>,
}

pub(crate) struct AccountNetInner {
    /// Shared underlying transport.
    pub(crate) net: Net,
    /// Account identity for metering and tracing.
    pub(crate) account: AccountId,
    /// Exact attach registration owned by this handle.
    pub(crate) registration_id: u64,
    /// Cached meter handle. Constructed once at attach/retag time so
    /// the request hot path does not clone the account id or lock the
    /// meter map per attempt.
    pub(crate) meter: AccountMeter,
    /// Provider of OAuth bearer tokens for this account.
    pub(crate) token_source: Option<Arc<dyn TokenSource>>,
    /// Quota discriminator selected for each registered request host.
    pub(crate) rate_scopes: HashMap<String, String>,
    /// Default retry policy applied to every request unless the
    /// caller overrides via `RequestBuilder::retry`.
    pub(crate) default_retry: RetryPolicy,
    /// Default total request timeout. Individual builders may override it.
    pub(crate) request_timeout: Option<Duration>,
    /// Deadline for dispatch to produce response headers.
    pub(crate) response_headers_timeout: Option<Duration>,
    /// Inactivity deadline between response body chunks.
    pub(crate) read_timeout: Option<Duration>,
    /// Buffered response ceiling for this account.
    pub(crate) max_buffered_response: Option<usize>,
    /// Weight-1 claim composed with every request concurrency claim.
    pub(crate) concurrency_limit: Option<ConcurrencyLimit>,
    /// User-Agent header inserted unless the request supplied one.
    pub(crate) user_agent: String,
    /// Account-scoped redirect policy and trusted-host allowlist.
    pub(crate) follow_redirects: FollowRedirects,
    /// Engine-controlled priority hint. Atomic so the hot per-request
    /// read does not take a lock. Stored as the `Priority` enum's
    /// discriminant byte; `priority()` converts back.
    pub(crate) priority: AtomicU8,
    /// Shared per-id payload admission, independent of handle registration.
    pub(crate) bandwidth_budget: BandwidthBudget,
}

/// Release the registration when the last clone of the handle goes.
///
/// Google and Graph call `detach()` explicitly; the JMAP reqwest
/// transport attaches and never does, so `NetInner::account_hosts` and
/// the meter map grew by one entry per JMAP account open for the
/// process lifetime. Making the release automatic closes the leak for
/// every caller instead of relying on each one to remember, and leaves
/// the explicit `detach()` as the way to release early.
///
/// The teardown is keyed on this handle's exact `registration_id`,
/// which is what makes the reference doc's "a stale Drop cannot detach
/// the replacement" true rather than aspirational. `detach_registration`
/// returns before touching the meter or the governor when that token is
/// no longer present, so a Drop after an explicit `detach()`, or after
/// `retag()` moved the token to another id, is a complete no-op.
impl Drop for AccountNetInner {
    fn drop(&mut self) {
        self.net
            .detach_registration(&self.account, Some(self.registration_id));
    }
}

impl AccountNet {
    /// Read the same coordinating bearer cache used by HTTP requests.
    /// Non-HTTP wire paths must use this lifecycle, including its backoff.
    pub async fn bearer(&self) -> Result<Option<crate::AccessToken>, Error> {
        match &self.inner.token_source {
            Some(source) => source.current().await.map(Some),
            None => Ok(None),
        }
    }

    /// Open a provider hold in the parent transport's registry.
    #[must_use]
    pub fn shared_hold(&self, key: impl Into<String>) -> crate::SharedHold {
        self.net().shared_hold(key)
    }

    pub(crate) fn rate_scope_for(&self, host: &str) -> &str {
        self.inner.rate_scopes.get(host).map_or("", String::as_str)
    }

    /// True when two account handles use the same process-wide client,
    /// governor, and meter.
    #[must_use]
    pub fn shares_transport_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner.net.inner, &other.inner.net.inner)
    }

    /// Start a `GET` request. Not async because the builder itself is
    /// pure construction; the network round-trip happens in
    /// `RequestBuilder::send`.
    #[must_use]
    pub fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::GET, url)
    }

    /// Start a request with any standard or extension HTTP method.
    /// The public method type comes from `http`, so reqwest remains
    /// absent from this crate's API.
    #[must_use]
    pub fn request(&self, method: http::Method, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), method, url)
    }

    /// Start a `POST` request.
    #[must_use]
    pub fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::POST, url)
    }

    /// Start a `PUT` request.
    #[must_use]
    pub fn put(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::PUT, url)
    }

    /// Start a `PATCH` request.
    #[must_use]
    pub fn patch(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::PATCH, url)
    }

    /// Start a `DELETE` request.
    #[must_use]
    pub fn delete(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::DELETE, url)
    }

    /// Stream a download. Issues a GET with optional `Range` header,
    /// applies retry + rate-limit + bandwidth metering, and wraps the
    /// response body in a `ByteStream` that increments the meter on
    /// every chunk.
    ///
    /// When a `Range` was requested, the response must be `206 Partial
    /// Content` with a `Content-Range` that matches the requested
    /// window. A `200 OK` to a ranged request means the server
    /// collapsed the range and would return the full resource; that
    /// would silently corrupt any caller that asked for a specific
    /// byte window, so we reject it with `Error::RangeNotHonored`
    /// before yielding any chunk. When no `Range` was requested we
    /// accept `200 OK` as before.
    ///
    /// A `range` with `length = Some(0)` is degenerate: the caller
    /// asked for zero bytes. Rather than emit `bytes=N-N` (one byte)
    /// or `bytes=N-(N-1)` (RFC-invalid), we short-circuit before
    /// touching the network and return an empty `ByteStream`. The
    /// bandwidth meter and rate-limit governor are not touched for a
    /// zero-byte read.
    pub async fn download_stream(
        &self,
        url: &str,
        range: Option<ByteRange>,
    ) -> Result<ByteStream, Error> {
        // Short-circuit zero-length: no network round-trip, empty
        // stream. Avoids the degenerate `bytes=N-N` encoding (one
        // byte) and the RFC-9110-invalid `bytes=N-(N-1)` form.
        if matches!(
            range,
            Some(ByteRange {
                length: Some(0),
                ..
            })
        ) {
            let empty = futures::stream::empty::<Result<bytes::Bytes, Error>>();
            return Ok(Box::pin(empty));
        }
        // Build the request: GET with optional Range header, going
        // through the full retry/rate-limit/traceparent stack.
        let want_range = match range {
            Some(r) => Some(encode_range(r)?),
            None => None,
        };
        let mut builder = self.get(url);
        if let Some(ref r) = want_range {
            builder = builder.header(RANGE.as_str(), r);
        }

        let InternalStreaming {
            evidence,
            status,
            headers,
            body,
            account: _,
            deadline,
            bytes_out: _,
            bytes_in,
            permit,
        } = send_streaming_inner(builder).await?;

        // Ranged-download safety: if the caller asked for a specific
        // window we must see a `206 Partial Content` with a matching
        // `Content-Range`. Anything else means the server gave us
        // bytes that do not match the requested offset; assembling
        // them into the caller's blob would corrupt it silently.
        let validated = (|| {
            if let Some(want) = want_range.as_deref() {
                if status != reqwest::StatusCode::PARTIAL_CONTENT {
                    return Err(Error::RangeNotHonored {
                        kind: RangeFailureKind::ResponseNotPartial,
                        message: format!(
                            "expected 206 Partial Content for Range request {want}, got {status}"
                        ),
                    });
                }
                let Some(got_hv) = headers.get(reqwest::header::CONTENT_RANGE) else {
                    return Err(Error::RangeNotHonored {
                        kind: RangeFailureKind::MissingContentRange,
                        message: format!(
                            "206 response for Range request {want} omitted Content-Range header"
                        ),
                    });
                };
                let got = got_hv.to_str().unwrap_or("");
                if !content_range_matches(want, got) {
                    return Err(Error::RangeNotHonored {
                        kind: RangeFailureKind::ContentRangeMismatch,
                        message: format!("content-range mismatch: requested {want}, got {got}"),
                    });
                }
            }

            Ok(())
        })();
        if let Err(error) = validated {
            return Err(evidence.finish(error));
        }
        // Wrap the body in a metering + bandwidth-cap adapter.
        let metered = wrap_metered(body, self.clone(), deadline, bytes_in);
        Ok(crate::request::hold_permit(
            evidence.wrap_stream(metered),
            permit,
        ))
    }

    /// Per-account meter handle.
    #[must_use]
    pub fn meter(&self) -> AccountMeter {
        self.inner.meter.clone()
    }

    /// Set the shared account payload smoothing cap. Zero becomes one B/s.
    /// See `BandwidthBudget` for the allowance bound and plaintext scope.
    pub fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.inner.bandwidth_budget.set_cap(bps);
    }

    /// Current shared cap; None means unlimited.
    #[must_use]
    pub fn bandwidth_cap(&self) -> Option<u64> {
        self.inner.bandwidth_budget.cap()
    }

    /// Shared account admission budget, also usable by stream adapters.
    #[must_use]
    pub fn bandwidth_budget(&self) -> &BandwidthBudget {
        &self.inner.bandwidth_budget
    }

    /// Set the engine-controlled priority hint. Single atomic store.
    pub fn set_priority(&self, p: Priority) {
        self.inner.priority.store(p as u8, Ordering::Relaxed);
    }

    /// Current priority hint.
    #[must_use]
    pub fn priority(&self) -> Priority {
        match self.inner.priority.load(Ordering::Relaxed) {
            x if x == Priority::Foreground as u8 => Priority::Foreground,
            x if x == Priority::Normal as u8 => Priority::Normal,
            x if x == Priority::Background as u8 => Priority::Background,
            x if x == Priority::Bulk as u8 => Priority::Bulk,
            // Unreachable in practice: only set_priority writes to
            // this atomic and the enum is non-exhaustive only at the
            // public API boundary, not on the wire.
            _ => Priority::Normal,
        }
    }

    /// The account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.inner.account
    }

    /// Detach this exact account registration. Idempotent across
    /// cloned handles.
    pub fn detach(&self) {
        self.inner
            .net
            .detach_registration(&self.inner.account, Some(self.inner.registration_id));
    }

    /// Default retry policy applied to requests on this account.
    #[must_use]
    pub fn default_retry(&self) -> &RetryPolicy {
        &self.inner.default_retry
    }

    pub(crate) fn request_timeout(&self) -> Option<Duration> {
        self.inner.request_timeout
    }

    pub(crate) fn response_headers_timeout(&self) -> Option<Duration> {
        self.inner.response_headers_timeout
    }

    pub(crate) fn read_timeout(&self) -> Option<Duration> {
        self.inner.read_timeout
    }

    pub(crate) fn max_buffered_response(&self) -> Option<usize> {
        self.inner.max_buffered_response
    }

    /// Return the live limit for `key` on this account's owning Net.
    /// Existing limits keep their capacity even if `limit` differs; resize
    /// explicitly with `ConcurrencyLimit::set_limit`. Entries are weak, so a
    /// key gets a fresh limit after all its handles and claims are dropped.
    #[must_use]
    pub fn shared_concurrency_limit(
        &self,
        key: impl Into<String>,
        limit: usize,
    ) -> ConcurrencyLimit {
        let key = key.into();
        let mut registry = self
            .inner
            .net
            .inner
            .concurrency_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = registry.get(&key).and_then(WeakLimit::upgrade) {
            let current = existing.limit();
            if current != limit {
                tracing::warn!(key = %key, existing_limit = current, requested_limit = limit,
                    "shared concurrency limit already exists; use set_limit to resize");
            }
            return existing;
        }
        registry.retain(|_, entry| entry.upgrade().is_some());
        let handle = ConcurrencyLimit::new(limit);
        registry.insert(key, handle.downgrade());
        handle
    }

    pub(crate) fn concurrency_limit(&self) -> Option<&ConcurrencyLimit> {
        self.inner.concurrency_limit.as_ref()
    }

    pub(crate) fn user_agent(&self) -> &str {
        &self.inner.user_agent
    }

    pub(crate) fn follow_redirects(&self) -> &FollowRedirects {
        &self.inner.follow_redirects
    }

    /// Underlying token source. Exposed so the OAuth refresher in
    /// `auth.rs` can share the trait object across requests.
    #[must_use]
    pub fn token_source(&self) -> Option<&Arc<dyn TokenSource>> {
        self.inner.token_source.as_ref()
    }

    /// Underlying shared `Net` handle. Crate-internal: used by the
    /// request pipeline to reach the governor and the reqwest client.
    pub(crate) fn net(&self) -> &Net {
        &self.inner.net
    }

    /// Re-mint this `AccountNet` under a new engine `AccountId`.
    ///
    /// The returned handle shares the same parent `Net`, token
    /// source, default retry policy and priority hint. Future admissions use
    /// the destination id budget; outstanding source grants stay with it.
    /// Used by protocol-crate factories that need to honor
    /// `AccountFactory::open(account_id)` when the caller built the
    /// `AccountNet` via `Net::attach_account` ahead of time
    /// (`*Client::with_account_net`) and the engine-minted id arrives
    /// only at open time. Idempotent: `retag(self.account())` is a
    /// cheap rebuild with no registration-count change.
    ///
    /// Bookkeeping:
    ///
    /// - The bandwidth meter transfers one attachment count from the
    ///   old id to the new id.
    /// - The exact token's host registrations move from the old id to
    ///   the new id. Governor refcounts are unchanged by the rename.
    /// - The old `AccountNet` clone the caller holds keeps working
    ///   for in-flight requests but no longer owns the host
    ///   registrations; calling `detach()` on the old handle is a
    ///   no-op afterwards. The new handle is the
    ///   one engine code should propagate forward.
    #[must_use]
    pub fn retag(&self, new_id: AccountId) -> AccountNet {
        // Same-id retag returns THIS handle, not a second
        // `AccountNetInner` carrying the same `registration_id` under
        // the same account. Two inners sharing one token is ambiguous
        // ownership: whichever dropped first would release a
        // registration the other still depends on. Cross-id retag is
        // unambiguous because the token moves - the old inner's Drop
        // finds nothing under the old id and no-ops - but the same-id
        // path moves nothing, so the only safe rebuild is no rebuild.
        // The documented contract is unchanged: "a cheap rebuild with
        // no registration-count change".
        if new_id == self.inner.account {
            return self.clone();
        }
        let net_inner = &self.inner.net.inner;
        let mut moved = false;
        {
            let mut map = net_inner
                .account_hosts
                .lock()
                .expect("net account_hosts lock poisoned");
            let hosts = map
                .get_mut(&self.inner.account)
                .and_then(|registrations| registrations.remove(&self.inner.registration_id));
            if map.get(&self.inner.account).is_some_and(HashMap::is_empty) {
                map.remove(&self.inner.account);
            }
            if let Some(hosts) = hosts {
                map.entry(new_id.clone())
                    .or_default()
                    .insert(self.inner.registration_id, hosts);
                moved = true;
            }
        }
        if moved {
            net_inner
                .meter
                .retag_account(&self.inner.account, new_id.clone());
        }
        let account_meter = if moved {
            net_inner.meter.account(new_id.clone())
        } else {
            BandwidthMeter::inert_account(new_id.clone())
        };
        AccountNet {
            inner: Arc::new(AccountNetInner {
                net: self.inner.net.clone(),
                bandwidth_budget: net_inner.bandwidth_registry.budget(new_id.clone()),
                account: new_id,
                registration_id: self.inner.registration_id,
                meter: account_meter,
                token_source: self.inner.token_source.clone(),
                rate_scopes: self.inner.rate_scopes.clone(),
                default_retry: self.inner.default_retry.clone(),
                request_timeout: self.inner.request_timeout,
                response_headers_timeout: self.inner.response_headers_timeout,
                read_timeout: self.inner.read_timeout,
                max_buffered_response: self.inner.max_buffered_response,
                concurrency_limit: self.inner.concurrency_limit.clone(),
                user_agent: self.inner.user_agent.clone(),
                follow_redirects: self.inner.follow_redirects.clone(),
                priority: AtomicU8::new(self.inner.priority.load(Ordering::Relaxed)),
            }),
        }
    }
}

/// Caller-supplied spec describing how `Net::attach_account` should
/// register an account: which hosts have rate limits, where bearer
/// tokens come from, and what retry budget to apply by default.
#[non_exhaustive]
pub struct AccountSpec {
    /// Per-host rate-limit declarations. Empty means "no governor
    /// enforcement for this account", which is the default for JMAP.
    pub hosts: Vec<RateLimit>,
    /// Optional OAuth token provider. Coordinating sources are retained as-is;
    /// raw providers are wrapped in an `OAuthRefresher` using `token_max_age`.
    /// A retained source owns its proactive refresh and backoff policy.
    /// `None` expresses an account that does not use bearer auth.
    pub token_source: Option<Arc<dyn TokenSource>>,
    /// Default retry policy.
    pub default_retry: RetryPolicy,
    /// Default total timeout applied to requests from this account.
    /// `None` leaves requests unbounded unless a builder sets one.
    pub request_timeout: Option<Duration>,
    /// Legacy name for `response_headers_timeout`. Retained for source
    /// compatibility; new code should set `response_headers_timeout`.
    pub connect_timeout: Option<Duration>,
    /// Per-attempt deadline for receiving response headers.
    pub response_headers_timeout: Option<Duration>,
    /// Inactivity deadline between response body chunks.
    pub read_timeout: Option<Duration>,
    /// Ceiling for buffered response bodies. `None` disables it.
    pub max_buffered_response: Option<usize>,
    /// Bound on this account's simultaneously in-flight requests. `None`
    /// leaves them unbounded. Pass the same handle to several accounts to
    /// bound them together. Request claims add to this weight-1 claim,
    /// including when they name the same limit.
    pub concurrency_limit: Option<ConcurrencyLimit>,
    /// User-Agent header used for requests from this account.
    pub user_agent: String,
    /// Method-aware redirect policy, including the account's trusted hosts.
    /// A single request can override it with
    /// `RequestBuilder::follow_redirects`.
    pub follow_redirects: FollowRedirects,
    /// Refresh max-age for opaque bearer tokens without an expiry hint.
    pub token_max_age: Duration,
}

impl AccountSpec {
    /// Common account defaults used by protocol clients.
    #[must_use]
    pub fn new(token_source: Option<Arc<dyn TokenSource>>) -> Self {
        Self {
            hosts: Vec::new(),
            token_source,
            default_retry: RetryPolicy::default(),
            request_timeout: None,
            connect_timeout: Some(Duration::from_secs(10)),
            response_headers_timeout: None,
            read_timeout: Some(Duration::from_secs(30)),
            max_buffered_response: Some(DEFAULT_MAX_BUFFERED_RESPONSE),
            concurrency_limit: None,
            user_agent: format!("bifrost-net/{}", env!("CARGO_PKG_VERSION")),
            follow_redirects: FollowRedirects::default(),
            token_max_age: DEFAULT_TOKEN_MAX_AGE,
        }
    }
}

/// Encode a `ByteRange` as an HTTP `Range` header value.
///
/// `length = None` means "from start to the end of the resource",
/// which is the RFC 9110 `bytes=N-` open-ended form. `length = Some(n)`
/// means `bytes=start-(start+n-1)` inclusive.
///
/// A `length = Some(0)` request is rejected upstream by
/// `AccountNet::download_stream`; the encoder treats `Some(0)` as a
/// local invalid range and returns `Error::RangeNotHonored` with
/// `RangeFailureKind::LocalInvalid`.
///
/// `start + length` is checked for `u64` overflow. Previously the
/// saturating math would silently emit `bytes=start-u64::MAX`, which
/// servers either reject or fulfil with the full tail of the resource
/// (silent semantics drift). We surface
/// `Error::RangeNotHonored { kind: LocalInvalid, .. }` instead so
/// callers see the configuration bug immediately.
// `Error` is intentionally wide (Bytes + HeaderMap), so a Result-wrapping
// constructor trips `result_large_err`. The alternative (boxing) would
// force a `Box<Error>` through every caller's error chain; ranges are an
// uncommon path and the wide Err is a workspace-consistent tradeoff.
#[allow(clippy::result_large_err)]
pub(crate) fn encode_range(range: ByteRange) -> Result<String, Error> {
    match range.length {
        None => Ok(format!("bytes={}-", range.start)),
        Some(0) => Err(Error::RangeNotHonored {
            kind: RangeFailureKind::LocalInvalid,
            message: "zero-length range".to_owned(),
        }),
        Some(n) => {
            let Some(end_plus_one) = range.start.checked_add(n) else {
                return Err(Error::RangeNotHonored {
                    kind: RangeFailureKind::LocalInvalid,
                    message: format!(
                        "range overflow: start {} + length {n} exceeds u64::MAX",
                        range.start,
                    ),
                });
            };
            // `end_plus_one >= 1` because `n > 0` here.
            let end = end_plus_one - 1;
            Ok(format!("bytes={}-{}", range.start, end))
        }
    }
}

/// Compare a `Range` request header against the corresponding
/// `Content-Range` response header. Server format is
/// `bytes <first>-<last>/<total>` or `bytes <first>-<last>/*`.
/// Returns `true` if:
///
/// - Closed request (`bytes=N-M`): the response's first matches
///   exactly. The last either matches exactly or, when the
///   resource is shorter than the requested window, equals the
///   known resource tail.
/// - Open-ended request (`bytes=N-`): the response's first matches
///   the request's first AND, when the total is known
///   (`/<total>`), the response's last equals `total - 1` (i.e.
///   covers bytes N through the end of the resource). When the
///   server returns `/*` (total unknown) we accept any last >= N
///   because we cannot verify the tail.
///
/// The previous open-ended check accepted any response end >= start,
/// which permitted truncated bodies (e.g. server returns `bytes
/// 0-9/100` to `bytes=0-` and the caller assembled a 10-byte slice
/// believing it had the full resource).
fn content_range_matches(req_range: &str, resp_range: &str) -> bool {
    // Parse `bytes=START-END?` from the request side.
    let Some(req_rest) = req_range.strip_prefix("bytes=") else {
        return false;
    };
    let (req_start, req_end) = match req_rest.split_once('-') {
        Some((s, e)) => (s.trim(), e.trim()),
        None => return false,
    };
    let Ok(req_start_n) = req_start.parse::<u64>() else {
        return false;
    };
    let req_end_n: Option<u64> = if req_end.is_empty() {
        None
    } else {
        req_end.parse().ok()
    };

    // Parse `bytes START-END/TOTAL` from the response side.
    let Some(resp_rest) = resp_range.strip_prefix("bytes ") else {
        return false;
    };
    let Some((range_part, total_part)) = resp_rest.split_once('/') else {
        return false;
    };
    let Some((resp_start, resp_end)) = range_part.split_once('-') else {
        return false;
    };
    let Ok(resp_start_n) = resp_start.trim().parse::<u64>() else {
        return false;
    };
    let Ok(resp_end_n) = resp_end.trim().parse::<u64>() else {
        return false;
    };

    if resp_start_n != req_start_n || resp_end_n < resp_start_n {
        return false;
    }
    let total_trim = total_part.trim();
    let total_known: Option<u64> = if total_trim == "*" {
        None
    } else {
        match total_trim.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => return false,
        }
    };
    if let Some(total) = total_known
        && resp_end_n >= total
    {
        return false;
    }
    match req_end_n {
        Some(end) => {
            resp_end_n == end
                || total_known
                    .is_some_and(|total| total > 0 && resp_end_n == total - 1 && resp_end_n < end)
        }
        // Open-ended request: must cover the full tail of the
        // resource when the total is known. With total unknown
        // (`*`), we cannot verify and accept any end >= start.
        None => match total_known {
            Some(total) => total > 0 && resp_end_n == total - 1,
            None => resp_end_n >= req_start_n,
        },
    }
}

/// Deliver bounded payload slices through the shared inbound budget.
pub(crate) fn wrap_metered(
    body: ByteStream,
    account: AccountNet,
    deadline: crate::request::RequestDeadline,
    request_bytes: crate::request::RequestByteCounter,
) -> ByteStream {
    wrap_delivery(body, account, deadline, request_bytes, None)
}

/// Diagnostic drains limit delivery before admission. Bytes buffered in the
/// current hyper chunk but abandoned after the limit remain unpaid.
pub(crate) fn wrap_delivery(
    body: ByteStream,
    account: AccountNet,
    deadline: crate::request::RequestDeadline,
    request_bytes: crate::request::RequestByteCounter,
    limit: Option<usize>,
) -> ByteStream {
    let stream = futures::stream::unfold(
        (
            body,
            bytes::Bytes::new(),
            limit.unwrap_or(usize::MAX),
            false,
        ),
        move |(mut body, mut chunk, mut remaining, done)| {
            let account = account.clone();
            let request_bytes = request_bytes.clone();
            async move {
                if done || remaining == 0 {
                    return None;
                }
                let result = async {
                    deadline.check_body()?;
                    while chunk.is_empty() {
                        let Some(next) = deadline.bound_body(body.next()).await? else {
                            return Ok(None);
                        };
                        chunk = next?;
                        let n = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
                        account.meter().record_bytes_in(n);
                        request_bytes.record(n);
                        deadline.check_body()?;
                    }
                    let grant = loop {
                        let mut grant = deadline
                            .bound_body(
                                account
                                    .bandwidth_budget()
                                    .ready(BandwidthDirection::Inbound, chunk.len().min(remaining)),
                            )
                            .await?;
                        deadline.check_body()?;
                        if grant.offer() {
                            break grant;
                        }
                    };
                    let len = grant.granted();
                    let slice = chunk.split_to(len);
                    grant.finish(len);
                    remaining -= len;
                    Ok(Some(slice))
                }
                .await;
                match result {
                    Ok(Some(slice)) => Some((Ok(slice), (body, chunk, remaining, false))),
                    Ok(None) => None,
                    Err(error) => Some((Err(error), (body, chunk, remaining, true))),
                }
            }
        },
    );
    Box::pin(stream)
}

/// Public re-export helper so `request.rs` can build a response body
/// adapter without pulling the `Net` types in.
pub(crate) fn into_byte_stream(
    response: reqwest::Response,
    read_timeout: Option<Duration>,
) -> ByteStream {
    use futures::TryStreamExt;
    let stream = response.bytes_stream().map_err(|e| {
        if e.is_timeout() {
            Error::Timeout {
                transmission_state: TransmissionState::Acknowledged,
            }
        } else {
            Error::Network {
                message: format!("response body chunk error: {e}"),
                transmission_state: TransmissionState::Acknowledged,
                source: Some(Box::new(e)),
            }
        }
    });
    let Some(read_timeout) = read_timeout else {
        return Box::pin(stream);
    };
    use futures::StreamExt;
    let timed = futures::stream::unfold((stream, false), move |(mut stream, done)| async move {
        if done {
            return None;
        }
        match tokio::time::timeout(read_timeout, stream.next()).await {
            Ok(Some(item)) => Some((item, (stream, false))),
            Ok(None) => None,
            Err(_) => Some((
                Err(Error::Timeout {
                    transmission_state: TransmissionState::Acknowledged,
                }),
                (stream, true),
            )),
        }
    });
    Box::pin(timed)
}

#[cfg(test)]
mod tests {
    use super::content_range_matches;
    use super::{AccountId, AccountSpec, ByteRange, FollowRedirects, Net, encode_range};
    use crate::StaticTokenSource;
    use crate::config::NetConfig;
    use crate::rate::{RateKey, RateLimit};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn content_range_rejects_inverted_response_range() {
        assert!(!content_range_matches("bytes=100-", "bytes 100-99/100"));
    }

    // ---- AccountSpec defaults -----------------------------------------
    //
    // These three defaults used to live on `NetConfig` and were pinned
    // by tests there. The process-wide/per-account split moved the
    // fields onto `AccountSpec` and the old tests went with the fields
    // they described, which left the defaults themselves unpinned.
    // They are re-pinned here, on their new home.

    #[test]
    fn tls_trust_selects_between_two_shared_clients() {
        // TLS trust is a client property, so it cannot be per-account
        // on a shared client. The split must not resolve that by
        // silently discarding the caller's choice: an account that
        // asked to accept invalid certificates gets a different shared
        // client, not the default one.
        let secure = Net::shared_for_tls(false);
        let also_secure = Net::shared_for_tls(false);
        let insecure = Net::shared_for_tls(true);
        let also_insecure = Net::shared_for_tls(true);

        assert!(
            Arc::ptr_eq(&secure.inner, &also_secure.inner),
            "accounts with the same trust requirement share one client"
        );
        assert!(
            Arc::ptr_eq(&insecure.inner, &also_insecure.inner),
            "sharing is preserved WITHIN the insecure class too - the \
             point of the split is one client per trust class, not one \
             per account"
        );
        assert!(
            !Arc::ptr_eq(&secure.inner, &insecure.inner),
            "a caller that opted into invalid certificates must not be \
             handed the strict client, which would fail every request \
             while appearing to have opted in"
        );
    }

    #[test]
    fn default_redirect_policy_is_enabled_with_ten_hops() {
        let spec = AccountSpec::new(None);
        let FollowRedirects::Enabled(policy) = spec.follow_redirects else {
            panic!("redirect following is on by default");
        };
        assert_eq!(
            policy.max_hops, 10,
            "ten hops is the cap reqwest classically used, and callers \
             rely on the default rather than setting it"
        );
        assert!(
            policy.trusted_hosts.is_empty(),
            "the allowlist is seeded per account from its own base host, \
             so the default must start empty rather than trusting anything"
        );
    }

    #[test]
    fn token_max_age_leaves_a_margin_under_a_typical_one_hour_ttl() {
        let spec = AccountSpec::new(None);
        assert!(
            spec.token_max_age < Duration::from_secs(60 * 60),
            "an opaque token with no expiry hint must be refreshed before \
             a typical one-hour TTL lapses, not exactly at it"
        );
    }

    #[test]
    fn a_bare_account_spec_carries_a_read_timeout() {
        let spec = AccountSpec::new(None);
        assert!(
            spec.read_timeout.is_some(),
            "the client-level read timeout moved per-account in the \
             NetConfig split; if the default is None, a server that \
             stalls mid-body blocks a caller with no total deadline \
             forever"
        );
    }

    #[test]
    fn content_range_rejects_end_at_or_after_total() {
        assert!(!content_range_matches("bytes=0-99", "bytes 0-99/99"));
        assert!(!content_range_matches("bytes=0-", "bytes 0-100/100"));
    }

    #[test]
    fn content_range_accepts_valid_open_ended_tail() {
        assert!(content_range_matches("bytes=10-", "bytes 10-99/100"));
    }

    // ---- encode_range -------------------------------------------------

    #[test]
    fn encode_range_emits_inclusive_closed_ranges() {
        let encoded = encode_range(ByteRange {
            start: 0,
            length: Some(100),
        })
        .expect("closed range encodes");
        assert_eq!(
            encoded, "bytes=0-99",
            "HTTP ranges are inclusive, so a 100-byte read ends at 99"
        );

        let encoded = encode_range(ByteRange {
            start: 500,
            length: Some(1),
        })
        .expect("single-byte range encodes");
        assert_eq!(encoded, "bytes=500-500");
    }

    #[test]
    fn encode_range_emits_the_open_ended_form_for_no_length() {
        let encoded = encode_range(ByteRange {
            start: 42,
            length: None,
        })
        .expect("open-ended range encodes");
        assert_eq!(encoded, "bytes=42-");
    }

    /// `Some(0)` is degenerate: `bytes=N-N` would be one byte and
    /// `bytes=N-(N-1)` is RFC-invalid. `download_stream` short-circuits
    /// it before the encoder is reached; the encoder rejects it so a
    /// hand-rolled call site still sees the error path.
    #[test]
    fn encode_range_rejects_a_zero_length_window() {
        let err = encode_range(ByteRange {
            start: 7,
            length: Some(0),
        })
        .expect_err("zero-length range is a local input error");
        assert!(matches!(
            err,
            crate::error::Error::RangeNotHonored {
                kind: crate::error::RangeFailureKind::LocalInvalid,
                ..
            }
        ));
    }

    /// `start + length` overflow used to saturate into
    /// `bytes=N-u64::MAX`, which servers either reject or answer with
    /// the whole tail. Surfacing the local error keeps the caller's bug
    /// visible.
    #[test]
    fn encode_range_rejects_start_plus_length_overflow() {
        let err = encode_range(ByteRange {
            start: u64::MAX - 1,
            length: Some(10),
        })
        .expect_err("overflowing range is a local input error");
        match err {
            crate::error::Error::RangeNotHonored { kind, message } => {
                assert_eq!(kind, crate::error::RangeFailureKind::LocalInvalid);
                assert!(message.contains("overflow"), "got {message}");
            }
            other => panic!("expected RangeNotHonored, got {other:?}"),
        }
    }

    #[test]
    fn encode_range_accepts_the_largest_representable_window() {
        let encoded = encode_range(ByteRange {
            start: 0,
            length: Some(u64::MAX),
        })
        .expect("start 0 plus u64::MAX does not overflow");
        assert_eq!(encoded, format!("bytes=0-{}", u64::MAX - 1));
    }

    // ---- content_range_matches ---------------------------------------

    #[test]
    fn content_range_matches_an_exact_closed_window() {
        assert!(content_range_matches("bytes=0-99", "bytes 0-99/1000"));
        assert!(content_range_matches("bytes=0-99", "bytes 0-99/*"));
    }

    #[test]
    fn content_range_accepts_a_legally_shortened_closed_window() {
        assert!(content_range_matches("bytes=0-99", "bytes 0-49/50"));
    }

    #[test]
    fn content_range_rejects_a_short_window_before_the_resource_tail() {
        assert!(!content_range_matches("bytes=0-99", "bytes 0-49/1000"));
    }

    #[test]
    fn content_range_rejects_a_shifted_start() {
        assert!(!content_range_matches("bytes=10-19", "bytes 11-19/100"));
        assert!(!content_range_matches("bytes=10-", "bytes 11-99/100"));
    }

    #[test]
    fn content_range_rejects_malformed_syntax_on_either_side() {
        assert!(!content_range_matches("0-99", "bytes 0-99/100"));
        assert!(!content_range_matches("bytes=0-99", "bytes=0-99/100"));
        assert!(!content_range_matches("bytes=0-99", "bytes 0-99"));
        assert!(!content_range_matches("bytes=0-99", "bytes 0/100"));
        assert!(!content_range_matches("bytes=abc-99", "bytes 0-99/100"));
        assert!(!content_range_matches("bytes=0-99", "bytes 0-xyz/100"));
        assert!(!content_range_matches("bytes=0-99", "bytes 0-99/xyz"));
        assert!(!content_range_matches("bytes=0-99", ""));
    }

    /// An open-ended request against an unknown total cannot be
    /// verified, so any end at or past the start is accepted.
    #[test]
    fn content_range_open_ended_with_unknown_total_accepts_any_tail() {
        assert!(content_range_matches("bytes=10-", "bytes 10-10/*"));
        assert!(content_range_matches("bytes=10-", "bytes 10-999999/*"));
        assert!(!content_range_matches("bytes=10-", "bytes 10-9/*"));
    }

    #[test]
    fn content_range_rejects_a_zero_total_resource() {
        assert!(!content_range_matches("bytes=0-", "bytes 0-0/0"));
    }

    fn build_net() -> Net {
        Net::new(NetConfig::default()).expect("default NetConfig should build")
    }

    fn build_spec(host: &str) -> AccountSpec {
        let token_source = Arc::new(StaticTokenSource::new("test-token", None));
        AccountSpec {
            hosts: vec![RateLimit {
                host: host.to_string(),
                quota_scope: String::new(),
                quota_per_second: 1.0,
                cost_default: 1,
                burst: 1,
            }],
            ..AccountSpec::new(Some(token_source))
        }
    }

    fn account_hosts_snapshot(net: &Net, id: &AccountId) -> Option<Vec<RateKey>> {
        let map = net
            .inner
            .account_hosts
            .lock()
            .expect("net account_hosts lock poisoned");
        map.get(id)
            .and_then(|registrations| registrations.values().next().cloned())
    }

    /// The leak: the JMAP reqwest transport attaches and never calls
    /// `detach()`, so `account_hosts` and the meter map grew by one
    /// entry per account open for the process lifetime. Release is now
    /// automatic on the last clone.
    #[test]
    fn dropping_the_last_handle_releases_the_registration() {
        let net = build_net();
        let id = AccountId("dropped".to_string());
        let account = net.attach_account(id.clone(), build_spec("drop.example"));
        let clone = account.clone();
        assert!(account_hosts_snapshot(&net, &id).is_some());

        drop(account);
        assert!(
            account_hosts_snapshot(&net, &id).is_some(),
            "a surviving clone still owns the registration"
        );

        drop(clone);
        assert!(
            account_hosts_snapshot(&net, &id).is_none(),
            "the last clone going away must release the registration"
        );
    }

    /// Drop is keyed on the exact attachment token, which is what makes
    /// "a stale Drop cannot detach the replacement" true rather than
    /// aspirational. Two live handles under one `AccountId` must not
    /// interfere.
    #[test]
    fn a_dropped_handle_does_not_detach_a_sibling_under_the_same_id() {
        let net = build_net();
        let id = AccountId("duplicate".to_string());
        let first = net.attach_account(id.clone(), build_spec("dup.example"));
        let second = net.attach_account(id.clone(), build_spec("dup.example"));

        drop(first);
        assert!(
            account_hosts_snapshot(&net, &id).is_some(),
            "the second attachment's registration must survive"
        );

        drop(second);
        assert!(account_hosts_snapshot(&net, &id).is_none());
    }

    /// An explicit `detach()` followed by the handle going out of scope
    /// must not double-release: `detach_registration` returns before
    /// touching the meter or the governor once the token is gone.
    #[test]
    fn an_explicit_detach_then_drop_is_not_a_double_release() {
        let net = build_net();
        let id = AccountId("explicit".to_string());
        let survivor = net.attach_account(id.clone(), build_spec("shared.example"));
        let early = net.attach_account(id.clone(), build_spec("shared.example"));

        early.detach();
        drop(early);

        assert!(
            account_hosts_snapshot(&net, &id).is_some(),
            "the double release would have taken the survivor's registration too"
        );
        drop(survivor);
        assert!(account_hosts_snapshot(&net, &id).is_none());
    }

    /// A cross-id retag moves the token, so the old handle's Drop finds
    /// nothing under the old id and must leave the renamed registration
    /// alone.
    #[test]
    fn dropping_a_retagged_handles_predecessor_does_not_release_the_new_id() {
        let net = build_net();
        let old = AccountId("before".to_string());
        let new = AccountId("after".to_string());
        let original = net.attach_account(old.clone(), build_spec("retagdrop.example"));
        let renamed = original.retag(new.clone());

        drop(original);
        assert!(
            account_hosts_snapshot(&net, &new).is_some(),
            "the moved registration belongs to the renamed handle"
        );

        drop(renamed);
        assert!(account_hosts_snapshot(&net, &new).is_none());
    }

    /// Same-id retag returns the same handle rather than a second inner
    /// sharing one `registration_id`. Two inners on one token is
    /// ambiguous ownership - whichever dropped first would release a
    /// registration the other still depends on.
    #[test]
    fn a_same_id_retag_does_not_mint_a_second_owner_of_one_token() {
        let net = build_net();
        let id = AccountId("stable".to_string());
        let original = net.attach_account(id.clone(), build_spec("same.example"));
        let rebuilt = original.retag(id.clone());
        assert_eq!(rebuilt.account(), &id);

        drop(original);
        assert!(
            account_hosts_snapshot(&net, &id).is_some(),
            "the rebuilt handle must still own a live registration"
        );

        drop(rebuilt);
        assert!(account_hosts_snapshot(&net, &id).is_none());
    }

    #[test]
    fn retag_moves_host_registrations_and_registers_meter() {
        let net = build_net();
        let old = AccountId("old".to_string());
        let new = AccountId("new".to_string());
        let original = net.attach_account(old.clone(), build_spec("retag.example"));
        assert_eq!(original.account(), &old);
        let hosts_before = account_hosts_snapshot(&net, &old).expect("old id has hosts");
        assert_eq!(hosts_before, vec![RateKey::new("retag.example", "")]);

        let retagged = original.retag(new.clone());

        assert_eq!(retagged.account(), &new);
        assert!(
            account_hosts_snapshot(&net, &old).is_none(),
            "old id should no longer hold host registrations after retag",
        );
        let hosts_after = account_hosts_snapshot(&net, &new).expect("new id has hosts");
        assert_eq!(
            hosts_after, hosts_before,
            "retag moves the same host list under the new id",
        );
        // Meter has counters under the new id: `account` builds an
        // AccountMeter that reads the registered AccountCounters, so a
        // never-recorded counter returns 0 bps without panicking.
        assert_eq!(net.meter().account(new).observed_bps(), 0);
    }

    #[test]
    fn retag_with_same_id_is_idempotent() {
        let net = build_net();
        let id = AccountId("stable".to_string());
        let original = net.attach_account(id.clone(), build_spec("retag.example"));
        let _retagged = original.retag(id.clone());
        let hosts = account_hosts_snapshot(&net, &id).expect("id still has hosts");
        assert_eq!(
            hosts,
            vec![RateKey::new("retag.example", "")],
            "retag with same id must not drop the host registration",
        );
    }

    /// The id-only compatibility path has no token to key on. It must
    /// still be deterministic: registration tokens are monotone, so it
    /// takes the oldest attachment rather than whatever `HashMap`
    /// iteration order yields.
    #[test]
    fn id_only_detach_removes_the_oldest_registration() {
        let net = build_net();
        let id = AccountId("compat".to_string());
        let _first = net.attach_account(id.clone(), build_spec("first.example"));
        let _second = net.attach_account(id.clone(), build_spec("second.example"));

        net.detach_account(&id);

        assert_eq!(
            net.governor().cost_default_for("first.example"),
            None,
            "the oldest attachment is the one that went away"
        );
        assert_eq!(net.governor().cost_default_for("second.example"), Some(1));

        net.detach_account(&id);
        assert_eq!(net.governor().cost_default_for("second.example"), None);
        net.detach_account(&id);
    }

    /// `retag` on a handle whose registration has already been torn
    /// down has nothing to move. The replacement handle must be inert
    /// rather than silently adopting another attachment's counters.
    #[test]
    fn retag_of_a_detached_registration_yields_an_inert_meter() {
        let net = build_net();
        let id = AccountId("gone".to_string());
        let account = net.attach_account(id.clone(), build_spec("gone.example"));
        account.meter().record_bytes_in(7);
        account.detach();

        let retagged = account.retag(AccountId("renamed".to_string()));

        retagged.meter().record_bytes_in(11);
        assert_eq!(
            retagged.meter().bytes_in(),
            0,
            "a retag with no live registration must not record or report bytes"
        );
        assert!(
            account_hosts_snapshot(&net, &AccountId("renamed".to_string())).is_none(),
            "a dead registration must not resurrect host bookkeeping under the new id"
        );
        assert_eq!(net.governor().cost_default_for("gone.example"), None);
    }

    #[test]
    fn duplicate_account_ids_detach_by_exact_registration() {
        let net = build_net();
        let id = AccountId("reopened".to_string());
        let first = net.attach_account(id.clone(), build_spec("old.example"));
        first.meter().record_bytes_in(10);
        let second = net.attach_account(id.clone(), build_spec("new.example"));
        assert_eq!(second.meter().bytes_in(), 10);

        first.detach();

        assert_eq!(net.governor().cost_default_for("old.example"), None);
        assert_eq!(net.governor().cost_default_for("new.example"), Some(1));
        assert_eq!(
            net.meter().account(id.clone()).bytes_in(),
            10,
            "the live attachment keeps the shared meter registered"
        );
        first.detach();
        assert_eq!(
            net.governor().cost_default_for("new.example"),
            Some(1),
            "repeated stale teardown is idempotent"
        );

        second.meter().record_bytes_in(5);
        second.detach();
        assert_eq!(net.governor().cost_default_for("new.example"), None);
        assert_eq!(net.meter().account(id).bytes_in(), 0);
        assert_eq!(
            second.meter().bytes_in(),
            15,
            "the detached cached handle retains its frozen snapshot"
        );
    }

    #[test]
    fn rejected_registration_cannot_unregister_a_later_valid_account() {
        let net = build_net();
        let id = AccountId("invalid-first".to_string());
        let token_source = Arc::new(StaticTokenSource::new("test-token", None));
        let invalid = net.attach_account(
            id.clone(),
            AccountSpec {
                hosts: vec![RateLimit {
                    host: "shared.example".to_string(),
                    quota_scope: String::new(),
                    quota_per_second: f64::NAN,
                    cost_default: 1,
                    burst: 1,
                }],
                ..AccountSpec::new(Some(token_source))
            },
        );
        let valid = net.attach_account(id, build_spec("shared.example"));

        invalid.detach();
        assert_eq!(net.governor().cost_default_for("shared.example"), Some(1));
        valid.detach();
        assert_eq!(net.governor().cost_default_for("shared.example"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn retag_uses_destination_budget_and_keeps_source_grants_attributed() {
        let net = build_net();
        let source = net.attach_account(AccountId("source-budget".into()), AccountSpec::new(None));
        let destination = net.attach_account(
            AccountId("destination-budget".into()),
            AccountSpec::new(None),
        );
        source.set_bandwidth_cap(Some(10));
        destination.set_bandwidth_cap(Some(1));
        let mut outstanding = source
            .bandwidth_budget()
            .reserve(crate::BandwidthDirection::Inbound, 10);
        assert!(outstanding.offer());
        let retagged = source.retag(destination.account().clone());
        assert_eq!(retagged.bandwidth_cap(), Some(1));
        retagged.set_bandwidth_cap(Some(2));
        assert_eq!(destination.bandwidth_cap(), Some(2));
        assert_eq!(source.bandwidth_cap(), Some(10));
        outstanding.finish(10);
        let now = tokio::time::Instant::now();
        assert_eq!(
            source
                .bandwidth_budget()
                .reserve(crate::BandwidthDirection::Inbound, 10)
                .ready_at(),
            now + Duration::from_secs(1)
        );
        assert_eq!(
            retagged
                .bandwidth_budget()
                .reserve(crate::BandwidthDirection::Inbound, 2)
                .ready_at(),
            now
        );
    }

    #[test]
    fn injected_registry_coordinates_distinct_nets_and_default_registries_are_private() {
        let registry = Arc::new(crate::BandwidthRegistry::new());
        let first = Net::with_bandwidth_registry(NetConfig::default(), Arc::clone(&registry))
            .expect("first Net");
        let second = Net::with_bandwidth_registry(NetConfig::default(), Arc::clone(&registry))
            .expect("second Net");
        assert!(Arc::ptr_eq(first.bandwidth_registry(), &registry));
        let id = AccountId("shared-budget".into());
        let a = first.attach_account(id.clone(), AccountSpec::new(None));
        let b = second.attach_account(id.clone(), AccountSpec::new(None));
        a.set_bandwidth_cap(Some(7));
        assert_eq!(b.bandwidth_cap(), Some(7));
        let private = build_net().attach_account(id, AccountSpec::new(None));
        assert_eq!(private.bandwidth_cap(), None);
    }
}

#[cfg(test)]
mod coordinating_resource_tests {
    use super::*;
    use crate::{AccessToken, StaticTokenSource};

    #[tokio::test]
    async fn coordinating_sources_survive_multiple_attachments() {
        let source: Arc<dyn TokenSource> = Arc::new(OAuthRefresher::new(Arc::new(
            StaticTokenSource::new("shared", None),
        )));
        let net = Net::new(NetConfig::default()).unwrap();
        let mut spec = AccountSpec::new(Some(Arc::clone(&source)));
        spec.token_max_age = Duration::ZERO;
        let first = net.attach_account(AccountId("coordinated-first".into()), spec);
        let mut spec = AccountSpec::new(Some(Arc::clone(&source)));
        spec.token_max_age = Duration::ZERO;
        let second = net.attach_account(AccountId("coordinated-second".into()), spec);
        assert!(Arc::ptr_eq(
            first.inner.token_source.as_ref().unwrap(),
            &source
        ));
        assert!(Arc::ptr_eq(
            second.inner.token_source.as_ref().unwrap(),
            &source
        ));
        assert_eq!(first.bearer().await.unwrap().unwrap().as_str(), "shared");
        assert_eq!(second.bearer().await.unwrap().unwrap().as_str(), "shared");
    }

    #[tokio::test]
    async fn bearer_reads_the_http_refresh_cache_for_plain_sources() {
        struct Source;
        impl TokenSource for Source {
            fn current(&self) -> bifrost_types::AccountFuture<Result<AccessToken, Error>> {
                Box::pin(async { Ok(AccessToken::new("stale", None)) })
            }
            fn refresh(&self) -> bifrost_types::AccountFuture<Result<AccessToken, Error>> {
                Box::pin(async { Ok(AccessToken::new("fresh", None)) })
            }
        }
        let net = Net::new(NetConfig::default()).unwrap();
        let account = net.attach_account(
            AccountId("plain-bearer".into()),
            AccountSpec::new(Some(Arc::new(Source))),
        );
        account
            .inner
            .token_source
            .as_ref()
            .unwrap()
            .refresh()
            .await
            .unwrap();
        assert_eq!(account.bearer().await.unwrap().unwrap().as_str(), "fresh");
    }

    #[test]
    fn shared_tls_clients_share_process_resources() {
        let trusted = Net::shared_default();
        let permissive = Net::shared_accepting_invalid_certs();
        assert!(!Arc::ptr_eq(&trusted.inner, &permissive.inner));
        assert!(Arc::ptr_eq(
            trusted.bandwidth_registry(),
            permissive.bandwidth_registry()
        ));
        assert!(Arc::ptr_eq(&trusted.inner.holds, &permissive.inner.holds));
        assert!(Arc::ptr_eq(&trusted.inner.meter, &permissive.inner.meter));
        let id = AccountId("shared-tls-resource-test".into());
        let first = trusted.attach_account(id.clone(), AccountSpec::new(None));
        let second = permissive.attach_account(id, AccountSpec::new(None));
        first.set_bandwidth_cap(Some(17));
        assert_eq!(second.bandwidth_cap(), Some(17));
    }
}
