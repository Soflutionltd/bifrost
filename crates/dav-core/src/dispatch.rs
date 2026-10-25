//! The shared DAV request dispatcher: credentials, the origin trust gate, the
//! redirect walk, and the generic WebDAV verbs.
//!
//! This is the security-sensitive half of the duplication. Both crates carried
//! `send_raw_request`, `auth_headers`, `admit_discovered_urls`, `is_trusted_url`
//! and `resolve_url` byte for byte - the credential gate, the downgrade refusal
//! and the redirect walk included. A divergence here is a credential leak or a
//! silent downgrade, which is the worst possible place to keep two copies.
//!
//! Wire traffic goes through `bifrost-net`, so DAV legs share the retry budget,
//! per-host rate limiting, bandwidth metering and observability with every other
//! HTTP protocol crate. Two deliberate departures from the net defaults:
//!
//! - Redirects are DISABLED on the account spec and walked here instead. The
//!   net redirect loop strips `Authorization` on any cross-origin hop and cannot
//!   restore it, and its allowlist compares hosts where this gate compares
//!   scheme, host and effective port. Walking here means every hop - same-origin
//!   or not - is re-credentialed for the origin it actually addresses, and an
//!   origin discovery never admitted is refused locally.
//! - Bearer injection is opted out with `without_bearer_auth`. The credential
//!   has to pass the origin gate before it is minted, which is a decision this
//!   crate makes and the transport cannot. A 401 is refreshed once here with
//!   the exact rejected token, under the same total deadline as every hop.
//!
//! Keeping this walk local also keeps the dialect target guard and MOVE
//! Destination rebasing together. A transport credential callback alone would
//! not replace those policies; adding one is unnecessary for these contracts.

use crate::error::net_account_error as into_account_error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use bifrost_net::{
    AccessToken, AccountNet, AccountSpec, Error as NetError, FollowRedirects,
    MalformedRedirectKind, Net, NetErrorContext, TokenSource,
};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountId, AccountOperation, AttemptCause,
    Cause, DiagnosticText, ProtocolErrorKind, TransmissionState, WireCause,
};
use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use tokio::time::Instant;

/// The WebDAV `Destination` header, lowercased so it is both a valid
/// `HeaderName::from_static` argument and the key `HeaderMap` stores it under.
const DESTINATION: &str = "destination";

use crate::error::{DavProtocol, local_error, response_read_error, status_error, transport_error};
use crate::transport::{
    DAV_CLIENT_TIMEOUT, DavBody, DavRequest, DavResponse, origin_is_secure, settle_body, url_origin,
};

/// Credentials as the dispatcher needs them.
///
/// The published `CalDavCredentials` and `CardDavCredentials` stay exactly where
/// they are and convert into this; it exists so one dispatcher can serve both
/// without either public enum moving.
#[derive(Clone)]
pub enum DavCredentials {
    Basic { username: String, password: String },
    Bearer { token_source: Arc<dyn TokenSource> },
}

/// The account spec every DAV account attaches under.
///
/// `token_source` stays `None` and every request opts out of bearer injection:
/// the origin gate decides whether a credential may be minted at all, and the
/// transport cannot make that call. Redirects are disabled because the walk
/// below owns them.
fn dav_account_spec() -> AccountSpec {
    let mut spec = AccountSpec::new(None);
    spec.request_timeout = Some(DAV_CLIENT_TIMEOUT);
    spec.follow_redirects = FollowRedirects::Disabled;
    spec
}

/// Dialect policy checked before every wire hop, for its target and any
/// Destination header after redirect rebasing. Origin trust remains separate.
pub trait DavRequestGuard: Send + Sync {
    fn check_target(
        &self,
        method: &Method,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError>;
}

/// Inbound response-body bytes attributed to one DAV account stream.
///
/// A metered dispatcher records request-local net counters here, including
/// error drains, retries and redirect hops. Separate streams own separate
/// tallies even when they share an account transport.
#[derive(Clone, Default)]
pub struct DavByteTally(Arc<AtomicU64>);

impl DavByteTally {
    fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Take the bytes recorded since the preceding batch boundary.
    #[must_use]
    pub fn take(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}

/// A DAV client's transport half: the account handle, credentials, and the set
/// of origins those credentials may reach.
#[derive(Clone)]
pub struct DavDispatch {
    net: AccountNet,
    base_url: String,
    credentials: DavCredentials,
    trusted_origins: Vec<String>,
    protocol: DavProtocol,
    request_guard: Option<Arc<dyn DavRequestGuard>>,
    byte_tally: Option<DavByteTally>,
}

impl DavDispatch {
    /// Attach a DAV account to the process-wide shared transport.
    ///
    /// The shared `Net` is the same one the other HTTP protocol crates use, so a
    /// DAV leg joins the existing per-host rate-limit buckets and connection
    /// pool rather than opening a private client beside them.
    #[must_use]
    pub fn new(
        account_id: AccountId,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        let net = Net::shared_default().attach_account(account_id, dav_account_spec());
        Self::around(net, base_url, credentials, protocol)
    }

    /// Attach through a supplied transport, resolving its budget before discovery.
    #[must_use]
    pub fn on_net(
        net: &Net,
        account_id: AccountId,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        Self::around(
            net.attach_account(account_id, dav_account_spec()),
            base_url,
            credentials,
            protocol,
        )
    }

    /// Build over a caller-supplied `AccountNet`.
    ///
    /// Two callers: an account composed into an IMAP-shaped account, which hands
    /// down the handle carrying that account's bandwidth meter and priority so
    /// the DAV legs are metered and capped with the rest of it; and tests, which
    /// hand down a handle attached to a scripted wire dispatcher.
    #[must_use]
    pub fn with_account_net(
        net: AccountNet,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        Self::around(net, base_url, credentials, protocol)
    }

    fn around(
        net: AccountNet,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        let trusted_origins = url_origin(&base_url).into_iter().collect();
        Self {
            net,
            base_url,
            credentials,
            trusted_origins,
            protocol,
            request_guard: None,
            byte_tally: None,
        }
    }

    /// Clone this dispatcher with a fresh stream-local byte tally.
    /// The transport, credentials and request guard retain their identity.
    #[must_use]
    pub fn metered(&self) -> (Self, DavByteTally) {
        let tally = DavByteTally::default();
        let mut dispatch = self.clone();
        dispatch.byte_tally = Some(tally.clone());
        (dispatch, tally)
    }

    /// Install a dialect policy before sharing the account. The policy may
    /// maintain monotone exclusions shared by cloned dispatchers.
    pub fn set_request_guard(&mut self, guard: Arc<dyn DavRequestGuard>) {
        self.request_guard = Some(guard);
    }

    fn check_request_targets(
        &self,
        request: &DavRequest,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        if let Some(guard) = &self.request_guard {
            guard.check_target(&request.method, &request.url, operation)?;
            if let Some(destination) = request
                .headers
                .get(DESTINATION)
                .and_then(|v| v.to_str().ok())
            {
                guard.check_target(&request.method, destination, operation)?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn protocol(&self) -> DavProtocol {
        self.protocol
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The account handle, for the priority and bandwidth-cap doors the
    /// `Account` impls forward onto it.
    #[must_use]
    pub fn net(&self) -> &AccountNet {
        &self.net
    }

    /// Start a request the caller will finish with its own headers and body.
    #[must_use]
    pub fn request(&self, method: Method, url: &str) -> DavRequest {
        DavRequest::new(method, url)
    }

    /// Turn a native id into an absolute request URL.
    ///
    /// An absolute href is returned verbatim. A root-relative href (`/cal/x`)
    /// replaces the base's path, which is what RFC 3986 says and what a server
    /// naming such an href in a Multi-Status means. A RELATIVE href lands under
    /// the configured base path.
    ///
    /// That last case is why the base is given its trailing slash back before
    /// joining. `around()` trims it, and `Url::join` reads a base whose path has
    /// no trailing slash as naming a RESOURCE, so a relative href replaces the
    /// base's last segment instead of landing beneath it: an account configured
    /// at `https://host/dav` resolved `cal/one.ics` to `https://host/cal/one.ics`,
    /// silently relocating the id out of the collection the account lives in.
    /// Appending is also what the unparseable-base fallback below has always
    /// done, so the two branches now agree on the shape they share.
    #[must_use]
    pub fn resolve_url(&self, href: &str) -> String {
        if href.starts_with("http://") || href.starts_with("https://") {
            return href.to_string();
        }
        if let Some(base) = self.join_base()
            && let Ok(resolved) = base.join(href)
        {
            return resolved.to_string();
        }
        // `around()` trims the trailing slash from base_url, so only the
        // href side decides whether a separator is needed.
        if href.starts_with('/') {
            format!("{}{href}", self.base_url)
        } else {
            format!("{}/{href}", self.base_url)
        }
    }

    /// The base URL as a directory, so relative hrefs resolve beneath it.
    fn join_base(&self) -> Option<Url> {
        let mut base = Url::parse(&self.base_url).ok()?;
        if !base.cannot_be_a_base() && !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        Some(base)
    }

    /// Admit successfully discovered DAV origins to the credential gate.
    ///
    /// Discovery is server-steered: the collection home (and, for CalDAV, the
    /// scheduling outbox) comes out of the principal's own PROPFIND response,
    /// so a compromised or hostile server picks these origins. Two rules bound
    /// what it can pick. A discovered origin must parse, and it must never
    /// weaken the transport guarantee the configured base URL already
    /// established - an HTTPS-configured account never trusts a plaintext
    /// discovered home, because that would turn discovery into a downgrade
    /// channel for the account credential. A cross-origin HTTPS home is still
    /// admitted; that is a real deployment shape, where the principal and the
    /// home live on different hosts of the same service. Admission happens only
    /// after the complete authenticated discovery result is available, before
    /// the account is shared or a home request can be in flight.
    pub fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        for url in urls {
            let Some(origin) = url_origin(&url) else {
                continue;
            };
            if origin_is_secure(&self.base_url) && !origin_is_secure(&url) {
                continue;
            }
            if !self.trusted_origins.contains(&origin) {
                self.trusted_origins.push(origin);
            }
        }
    }

    #[must_use]
    pub fn is_trusted_url(&self, url: &str) -> bool {
        url_origin(url).is_some_and(|origin| self.trusted_origins.contains(&origin))
    }

    /// Build the per-request auth headers.
    ///
    /// Standalone header minting is bounded by the DAV request timeout. Request
    /// wrappers leave minting to `send_raw_request` so it shares their deadline.
    /// The bearer token is read from the shared source on every call, so a
    /// token rotated mid-sync is honored on the next DAV request without
    /// reopening the account.
    ///
    /// The origin allowlist is a deliberate trade, and it is consumer-visible.
    /// A server that names hrefs on a THIRD origin - neither the configured
    /// base nor an origin admitted by `admit_discovered_urls` - now gets a hard
    /// LOCAL error here, with no request going out at all, where the
    /// pre-gate behaviour was to send the account credential to whatever origin
    /// the server named. Failing a legitimate-but-unusual deployment locally is
    /// accepted as the cheaper failure than leaking a credential to an origin
    /// the account never authenticated against. The way to widen it is to admit
    /// the origin through discovery, not to relax this check.
    pub async fn auth_headers(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<HeaderMap, AccountError> {
        tokio::time::timeout(DAV_CLIENT_TIMEOUT, self.mint_auth(url, operation))
            .await
            .unwrap_or_else(|_| {
                Err(self.redirect_error(
                    NetError::Timeout {
                        transmission_state: bifrost_types::TransmissionState::Unsent,
                    },
                    operation,
                ))
            })
            .map(|(headers, _)| headers)
    }

    async fn mint_auth(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(HeaderMap, Option<AccessToken>), AccountError> {
        if !self.is_trusted_url(url) {
            return Err(local_error(
                operation,
                format!("refusing to send DAV credentials to untrusted URL: {url}"),
                self.protocol,
            ));
        }
        let mut headers = HeaderMap::new();
        let mut used = None;
        // A header-invalid byte in a credential (e.g. a stray `\n` from a
        // sloppy config file) must fail loudly here. Swallowing it sent the
        // request UNAUTHENTICATED and surfaced as a mystifying 401 instead
        // of a local "credential is malformed" error. (base64 output is
        // always header-safe, so the Basic arm can only trip on this via a
        // future refactor; the Bearer token is user-supplied input.)
        match &self.credentials {
            DavCredentials::Basic { username, password } => {
                let credentials = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                let value =
                    HeaderValue::from_str(&format!("Basic {credentials}")).map_err(|_| {
                        local_error(
                            operation,
                            "Basic credential contains header-invalid bytes",
                            self.protocol,
                        )
                    })?;
                headers.insert(AUTHORIZATION, value);
            }
            DavCredentials::Bearer { token_source } => {
                let token = token_source
                    .current()
                    .await
                    .map_err(|error| self.token_read_error(error, operation))?;
                let value =
                    HeaderValue::from_str(&format!("Bearer {}", token.as_str())).map_err(|_| {
                        local_error(
                            operation,
                            "bearer token contains header-invalid bytes",
                            self.protocol,
                        )
                    })?;
                headers.insert(AUTHORIZATION, value);
                used = Some(token);
            }
        }
        Ok((headers, used))
    }

    async fn mint_auth_until(
        &self,
        url: &str,
        operation: AccountOperation,
        deadline: Instant,
    ) -> Result<(HeaderMap, Option<AccessToken>), AccountError> {
        tokio::time::timeout_at(deadline, self.mint_auth(url, operation))
            .await
            .unwrap_or_else(|_| {
                Err(self.redirect_error(
                    NetError::Timeout {
                        transmission_state: TransmissionState::Unsent,
                    },
                    operation,
                ))
            })
    }

    /// Classify a `TokenSource::current()` failure.
    ///
    /// Folding every one of these into `transport_error` was survivable while
    /// that constructor claimed `InFlight` - the result reconciled and stopped.
    /// It now claims `Unsent`, which derives `Retry(SameRequest)`, and a
    /// PERMANENT credential failure retried as a transport blip is a retry storm
    /// that never emits the reauthorization signal the consumer needs. So the
    /// two auth-bearing net errors go through `net_account_error`, which passes
    /// DAV's explicit account throttle scope to
    /// `bifrost_net::into_account_error_with_throttle_scope`. It maps `AuthLost`
    /// to `Auth(ReauthorizationRequired)` and `RefreshFailed` to
    /// `Auth(RefreshTransient)`. Only the remainder keeps the `Unsent` transport
    /// classification, which is correct for it: no request left this process and
    /// there is nothing at the target to reconcile against.
    fn token_read_error(&self, error: NetError, operation: AccountOperation) -> AccountError {
        if matches!(
            error,
            NetError::AuthLost { .. } | NetError::RefreshFailed { .. }
        ) {
            return into_account_error(
                error,
                NetErrorContext {
                    provider: None,
                    protocol: self.protocol.protocol(),
                    operation,
                    scope: None,
                },
            );
        }
        transport_error(
            operation,
            format!("failed to read OAuth access token: {error}"),
            self.protocol,
        )
    }

    /// Classify a redirect this walk will not follow, through the transport's
    /// own funnel.
    ///
    /// Redirects are disabled in `bifrost-net` and walked here, so the
    /// transport never sees these failures - but they are the same failures its
    /// own redirect loop reports, and they must classify the same way:
    /// `Protocol(ContractViolation)` with an ACKNOWLEDGED attempt. Every one is
    /// raised after the server answered a hop, and every one is about what the
    /// server answered (a loop past the hop cap, a `Location` that will not
    /// resolve or will not decode). They used to be `local_error`, which is
    /// `Request(Malformed)` - `ClientBug`, blaming the caller for the provider's
    /// redirect, with no transmission evidence at all.
    ///
    /// The refused redirect to an unadmitted origin is deliberately NOT here: it
    /// is this client's own credential-gate decision, raised by the walk before
    /// the next hop goes out. It stays a local `Request(Malformed)`, carrying
    /// the `Acknowledged` attempt the answered previous hop earned.
    fn redirect_error(&self, error: NetError, operation: AccountOperation) -> AccountError {
        into_account_error(
            error,
            NetErrorContext {
                provider: None,
                protocol: self.protocol.protocol(),
                operation,
                scope: None,
            },
        )
    }

    /// One wire attempt through `bifrost-net`, with its status-bearing failures
    /// turned back into responses.
    ///
    /// DAV reads statuses as data, not only as failure: `sync-collection` reads
    /// 403 and 410 as cursor invalidation, `MOVE` reads 405 and 501 as "no MOVE
    /// here, fall back to copy-then-delete", and a conditional PUT reads 412.
    /// The net retry loop turns every 4xx and 5xx into `Err` before a response
    /// surfaces, so a terminal `Error::Status` is reconstituted here rather than
    /// classified, as is exhausted 501 for MOVE fallback. Exhausted retry and
    /// rate-limit budgets and transport failures keep the shared funnel, which
    /// preserves their retry history and transmission evidence. The DAV status
    /// ladder receives headers for statuses interpreted outside that funnel.
    ///
    /// One narrowing worth knowing: a reconstituted body is capped at
    /// `bifrost_net::STATUS_BODY_CAP`, where the previous DAV transport buffered
    /// error bodies to the full 64 MiB ceiling. Every status DAV reads as data
    /// carries a short precondition document, so the cap does not bite; a
    /// diagnostic body longer than the cap now arrives truncated.
    async fn dispatch_once(
        &self,
        request: &DavRequest,
        operation: AccountOperation,
        deadline: Instant,
    ) -> Result<DavResponse, AccountError> {
        // A header the record could not carry, and a header value the transport
        // cannot express as text, are the same failure seen one layer apart:
        // the request would go out MISSING something the caller asked for. Both
        // refuse locally instead, so the caller sees `Request(Malformed)`
        // rather than a server answering a request it was never sent.
        if let Some(name) = request.invalid_header() {
            return Err(local_error(
                operation,
                format!("DAV request header {name} is not a valid HTTP header"),
                self.protocol,
            ));
        }
        let mut builder = self
            .net
            .request(request.method.clone(), &request.url)
            .without_bearer_auth()
            .timeout(deadline.saturating_duration_since(Instant::now()));
        for (name, value) in &request.headers {
            let Ok(value) = value.to_str() else {
                return Err(local_error(
                    operation,
                    format!("DAV request header {name} carries a non-ASCII value"),
                    self.protocol,
                ));
            };
            builder = builder.header(name.as_str(), value);
        }
        // Keep request-level safety distinct from the operation default:
        // EventUpdate/ContactUpdate also enclose MOVE and create-PUT legs.
        // Using operation.is_idempotent() here would replay those legs, while
        // removing the matching error override would authorize engine replay.
        // Net resolves the forwarded declaration with the same
        // `request_is_replayable` rule `DavRequest::replayable` uses, against
        // the same headers plus the minted `Authorization`, which is not a
        // precondition, so both sides agree by construction.
        if let Some(replay) = request.replay {
            builder = builder.replay(replay);
        }
        if let Some(body) = &request.body {
            builder = builder.body(body.clone());
        }
        // A request that resolves unreplayable on the wire must classify
        // that way too, whichever failure it hits. Net already narrows its own
        // ambiguous outcomes; the stamp below also covers failures minted here,
        // such as the oversized-response error. The `AccountOperation` table
        // is the default source of idempotency and it is keyed on the logical
        // operation, not on the request: the copy leg of the MOVE fallback is a
        // create-PUT (`If-None-Match: *`) issued under
        // `EventUpdate`/`ContactUpdate`, which the table calls idempotent.
        // Every `Err` arm below is routed by that flag - the transport arm
        // through `derive`'s transmission-state branch, the oversized-response
        // arm through `derive_protocol` - so the decoration wraps the whole
        // result rather than one arm. Left on one arm it let an oversized
        // response on that copy leg derive `Retry(SameRequest)`, whose replay
        // answers 412 at the destination the first attempt already occupied,
        // forever.
        let counter = bifrost_net::RequestByteCounter::new();
        let outcome = builder.count_bytes_into(counter.clone()).send().await;
        if let Some(tally) = &self.byte_tally {
            tally.add(counter.bytes_in());
        }
        self.wire_outcome(request, operation, outcome)
            .map_err(|error| {
                let error = if matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Server(
                        bifrost_types::ServerErrorKind::RateLimited
                            | bifrost_types::ServerErrorKind::QuotaExhausted
                    )
                ) {
                    error
                        .into_builder()
                        .throttle_scope(bifrost_types::ThrottleScope::Account)
                        .try_build()
                        .expect("valid DAV throttle scope")
                } else {
                    error
                };
                if !request.replayable() {
                    return error
                        .into_builder()
                        .idempotency_override(false)
                        .try_build()
                        .expect("declaring a DAV failure unreplayable cannot invalidate it");
                }
                error
            })
    }

    /// Turn one `bifrost-net` outcome into a `DavResponse` or a classified
    /// `AccountError`. Split out of [`Self::dispatch_once`] so the unreplayable
    /// declaration can decorate every failing arm at one point.
    fn wire_outcome(
        &self,
        request: &DavRequest,
        operation: AccountOperation,
        outcome: Result<bifrost_net::Response, NetError>,
    ) -> Result<DavResponse, AccountError> {
        match outcome {
            Err(error @ NetError::AttemptHistory { .. }) => {
                // The terminal status of an unreplayable request is data only
                // when its earlier attempts cannot have produced it. A replay
                // after the request's own committed first attempt can answer
                // 412 (conditional PUT/DELETE, create-PUT) or 404 (versioned
                // DELETE, MOVE whose source already moved), so those go to the
                // shared classifier, whose `may_have_committed` path
                // reconciles. A 405/501 denies the method itself, which no
                // earlier commit can cause, and is the one status DAV needs as
                // data after a refusal (the MOVE fallback).
                //
                // The all-401 clause is defensive and cannot fire today: DAV
                // builds every net request without bearer auth and refreshes in
                // `send_raw_until` across separate dispatches, so net never
                // walks a 401 refresh for DAV. It keeps the rule right if DAV
                // ever routes auth through net. It needs at least one attempt:
                // an empty-attempt envelope is net's own ambiguity record.
                let refused_only = match &error {
                    NetError::AttemptHistory { attempts, .. } => {
                        let status_is = |attempt: &NetError, codes: &[u16]| {
                            matches!(attempt, NetError::Status { code, .. }
                                if codes.contains(&code.as_u16()))
                        };
                        let all_auth = !attempts.is_empty()
                            && attempts.iter().all(|attempt| status_is(attempt, &[401]));
                        let method_refused = match error.terminal() {
                            NetError::Status { code, .. } => matches!(code.as_u16(), 405 | 501),
                            NetError::RetryBudgetExhausted {
                                final_response: Some(r),
                                ..
                            } => matches!(r.status.as_u16(), 405 | 501),
                            _ => false,
                        };
                        let earlier_refused = attempts
                            .iter()
                            .all(|attempt| status_is(attempt, &[401, 429, 503, 501]));
                        all_auth || (method_refused && earlier_refused)
                    }
                    _ => false,
                };
                if request.replayable() || refused_only {
                    let response = match error.terminal() {
                        NetError::Status {
                            code,
                            headers,
                            body,
                        } => Some((*code, headers, body)),
                        NetError::RetryBudgetExhausted {
                            final_response: Some(response),
                            ..
                        }
                        | NetError::RateLimited {
                            final_response: response,
                            ..
                        } if response.status == StatusCode::NOT_IMPLEMENTED => {
                            Some((response.status, &response.headers, &response.body))
                        }
                        _ => None,
                    };
                    if let Some((status, headers, body)) = response {
                        return Ok(DavResponse {
                            status,
                            headers: headers.clone(),
                            body: decode_body(body),
                            url: request.url.clone(),
                        });
                    }
                    if let NetError::ResponseTooLarge { limit } = error.terminal() {
                        let limit = *limit;
                        return Err(response_read_error_with_history(
                            error,
                            limit,
                            operation,
                            self.protocol,
                        ));
                    }
                }
                Err(into_account_error(
                    error,
                    NetErrorContext {
                        provider: None,
                        protocol: self.protocol.protocol(),
                        operation,
                        scope: None,
                    },
                ))
            }
            Ok(response) => Ok(DavResponse {
                status: response.status,
                headers: response.headers,
                body: decode_body(&response.body),
                url: request.url.clone(),
            }),
            Err(NetError::Status {
                code,
                body,
                headers,
            }) => Ok(DavResponse {
                status: code,
                headers,
                body: decode_body(&body),
                url: request.url.clone(),
            }),
            // MOVE consumes 501 as data even after net exhausts its 5xx
            // retry budget. Other exhausted statuses keep their complete retry
            // history in the shared funnel instead of being rebuilt as a bare
            // response and losing Retry-After evidence.
            Err(NetError::RetryBudgetExhausted {
                final_response: Some(response),
                ..
            }) if response.status == StatusCode::NOT_IMPLEMENTED => Ok(DavResponse {
                status: response.status,
                headers: response.headers,
                body: decode_body(&response.body),
                url: request.url.clone(),
            }),
            // A body past the buffered ceiling is acknowledged but unreadable:
            // the request reached the server and may have taken effect, so a
            // non-idempotent mutation must reconcile rather than replay.
            Err(NetError::ResponseTooLarge { limit }) => Err(response_read_error(
                operation,
                format!("DAV response body exceeded the buffered ceiling ({limit} bytes)"),
                self.protocol,
            )),
            Err(error) => Err(into_account_error(
                error,
                NetErrorContext {
                    provider: None,
                    protocol: self.protocol.protocol(),
                    operation,
                    scope: None,
                },
            )),
        }
    }

    /// Send a request, following every redirect hop by hand.
    ///
    /// Redirects are disabled in the transport, so both same-origin and
    /// cross-origin hops arrive here. Each hop re-mints `auth_headers` for the
    /// origin it is about to address, which refuses locally when that origin was
    /// never admitted by discovery. Doing this for same-origin hops too costs
    /// nothing - the credential is the same one - and removes the previous
    /// split, where reqwest followed same-origin hops internally and only
    /// cross-origin hops were walked.
    pub async fn send_raw_request(
        &self,
        request: DavRequest,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let unreplayable = !request.replayable();
        self.send_raw_until(request, operation, Instant::now() + DAV_CLIENT_TIMEOUT)
            .await
            .map_err(|error| {
                if unreplayable {
                    error
                        .into_builder()
                        .idempotency_override(false)
                        .try_build()
                        .expect("valid unreplayable DAV request classification")
                } else {
                    error
                }
            })
    }

    async fn send_raw_until(
        &self,
        request: DavRequest,
        operation: AccountOperation,
        deadline: Instant,
    ) -> Result<DavResponse, AccountError> {
        // The named shared constant, not `RedirectPolicy::default().max_hops`:
        // DAV has no per-account redirect config, and laundering the cap
        // through a type Default hid the coupling (changing the default
        // silently changed DAV behavior).
        let max_hops = usize::from(bifrost_net::DEFAULT_MAX_HOPS);
        // The first hop is gated here, before anything is sent, exactly as
        // every later hop is gated below. Wrappers build unauthenticated
        // requests so credential minting happens only here, inside the total
        // deadline. `DavRequest::new` and this method are public, so callers
        // can also hand over a URL nobody checked. Checking only the final answer
        // protected nothing - the request had already gone out - and as a
        // debug assertion it panicked a downstream debug build while
        // returning the response silently in release.
        if !self.is_trusted_url(&request.url) {
            return Err(local_error(
                operation,
                format!(
                    "refusing to send a DAV request to an untrusted URL: {}",
                    request.url
                ),
                self.protocol,
            ));
        }
        let unreplayable = !request.replayable();
        let mut history = RedirectHistory::default();
        let mut request = request;
        let mut hops = 0usize;
        let (auth, mut used) = self
            .mint_auth_until(&request.url, operation, deadline)
            .await
            .map_err(|error| history.attach(error, unreplayable, operation, self.protocol))?;
        request.headers.extend(auth);
        let mut refreshed = false;
        loop {
            // Check both the source and the current Destination on EVERY hop.
            // Redirect rebasing below has already produced this hop's final
            // destination, so dialect policy cannot be bypassed by a redirect.
            self.check_request_targets(&request, operation)
                .map_err(|error| history.attach(error, unreplayable, operation, self.protocol))?;
            let response = self
                .dispatch_once(&request, operation, deadline)
                .await
                .map_err(|error| history.attach(error, unreplayable, operation, self.protocol))?;
            history.record_response(response.status, unreplayable);
            if response.status == StatusCode::UNAUTHORIZED
                && !refreshed
                && let (DavCredentials::Bearer { token_source }, Some(token)) =
                    (&self.credentials, used.take())
            {
                // The 401 is a refusal, so replaying the owned byte body is
                // safe even for MOVE/conditional create. Use the token actually
                // rejected, not a second current() read that could race rotation.
                let refreshed_token = match tokio::time::timeout_at(
                    deadline,
                    token_source.refresh_if_unchanged(token),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(NetError::Timeout {
                        transmission_state: TransmissionState::Unsent,
                    }),
                };
                let token = refreshed_token
                    .map_err(|error| self.token_read_error(error, operation))
                    .map_err(|error| {
                        history.attach(error, unreplayable, operation, self.protocol)
                    })?;
                let value =
                    HeaderValue::from_str(&format!("Bearer {}", token.as_str())).map_err(|_| {
                        history.attach(
                            local_error(
                                operation,
                                "bearer token contains header-invalid bytes",
                                self.protocol,
                            ),
                            unreplayable,
                            operation,
                            self.protocol,
                        )
                    })?;
                request.headers.insert(AUTHORIZATION, value);
                used = Some(token);
                refreshed = true;
                continue;
            }
            // Preserve DAV methods on endpoint relocation: GET cannot stand
            // in for a PROPFIND/REPORT XML query or a conditional mutation.
            // A 303 asks for a different operation, so surface it to the caller
            // instead of silently accepting a GET representation as DAV data.
            let redirect = matches!(
                response.status,
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            );
            let location = match response.headers.get(reqwest::header::LOCATION) {
                Some(value) if redirect => Some(value.to_str().map_err(|error| {
                    history.attach(
                        self.redirect_error(
                            NetError::MalformedRedirect {
                                kind: MalformedRedirectKind::InvalidLocationEncoding,
                                message: format!("Location header was not valid UTF-8: {error}"),
                            },
                            operation,
                        ),
                        unreplayable,
                        operation,
                        self.protocol,
                    )
                })?),
                _ => None,
            };
            let Some(location) = location else {
                return Ok(response);
            };
            let next = Url::parse(&response.url)
                .and_then(|base| base.join(location))
                .map_err(|error| {
                    history.attach(
                        self.redirect_error(
                            NetError::MalformedRedirect {
                                kind: MalformedRedirectKind::UnresolvableLocation,
                                message: format!(
                                    "Location {location:?} could not be resolved against base: {error}"
                                ),
                            },
                            operation,
                        ),
                        unreplayable,
                        operation,
                        self.protocol,
                    )
                })?;
            hops += 1;
            if hops > max_hops {
                return Err(history.attach(
                    self.redirect_error(
                        NetError::RedirectLoop {
                            hops: u16::try_from(hops).unwrap_or(u16::MAX),
                        },
                        operation,
                    ),
                    unreplayable,
                    operation,
                    self.protocol,
                ));
            }
            // Refused locally when the origin was never admitted by discovery.
            // The gate is checked here rather than left to `auth_headers`,
            // whose own refusal is right for a first hop but not for this one:
            // by now the server has answered the previous hop, and that
            // response is already in `history` below.
            if !self.is_trusted_url(next.as_str()) {
                return Err(history.attach(
                    local_error(
                        operation,
                        format!("refusing to follow a DAV redirect to an untrusted URL: {next}"),
                        self.protocol,
                    ),
                    unreplayable,
                    operation,
                    self.protocol,
                ));
            }
            // Fresh credentials for the target origin.
            let (auth, token) = self
                .mint_auth_until(next.as_str(), operation, deadline)
                .await
                .map_err(|error| history.attach(error, unreplayable, operation, self.protocol))?;
            used = token;
            refreshed = false;
            let mut headers = request.headers.clone();
            headers.remove(AUTHORIZATION);
            for (name, value) in &auth {
                headers.insert(name, value.clone());
            }
            // A redirect of the SOURCE relocates the collection namespace, and a
            // `Destination` still naming the pre-redirect one asks the server to
            // write where it has just said the source no longer lives: a MOVE of
            // `/cal/one.ics` redirected to `/dav/cal/one.ics` kept a destination
            // under `/cal/`. Rebase it on the same hop the source took - express
            // it relative to the pre-hop request URL, resolve that against the
            // post-hop one - so a cross-collection move stays cross-collection
            // inside the new namespace.
            //
            // This is inside the credential gate rather than beside it. The
            // destination is a URL this client asks the server to write to, so
            // it carries the same admitted-origin requirement `move_resource`
            // enforced on the caller's value. The rebase happens only after
            // the gate has admitted `next`, and the rebased URL is
            // `next`-relative, so the requirement holds by construction; the
            // check below is the guard that keeps it holding if either half
            // moves. A destination on a different origin than the source is left
            // verbatim - the source's hop says nothing about it, and it passed
            // the gate on the way in.
            let rebased = headers
                .get(DESTINATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|destination| rebase_destination(&request.url, &next, destination));
            if let Some(rebased) = rebased {
                if !self.is_trusted_url(&rebased) {
                    return Err(history.attach(
                        local_error(
                            operation,
                            format!(
                                "refusing to name an untrusted DAV move destination after a redirect: {rebased}"
                            ),
                            self.protocol,
                        ),
                        unreplayable,
                        operation,
                        self.protocol,
                    ));
                }
                let value = HeaderValue::from_str(&rebased).map_err(|error| {
                    history.attach(
                        local_error(operation, error.to_string(), self.protocol),
                        unreplayable,
                        operation,
                        self.protocol,
                    )
                })?;
                headers.insert(HeaderName::from_static(DESTINATION), value);
            }
            request = DavRequest {
                method: request.method,
                url: next.to_string(),
                headers,
                body: request.body,
                replay: request.replay,
                // Unreachable: the first dispatch already refused a request
                // carrying one. Carried rather than reset so a future path that
                // reaches here does not launder the rejection away.
                invalid_header: request.invalid_header,
            };
        }
    }

    pub async fn send_body_request(
        &self,
        request: DavRequest,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        settle_body(response, operation, self.protocol)
    }

    pub async fn send_status_request(
        &self,
        request: DavRequest,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() {
            Ok(())
        } else {
            Err(status_error(
                &response.headers,
                operation,
                response.status,
                response.body,
                self.protocol,
            ))
        }
    }

    pub async fn propfind_raw(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let method = Method::from_bytes(b"PROPFIND")
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let request = DavRequest::new(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            // A read: replaying after a dropped connection cannot change state.
            .replay(bifrost_net::ReplayPolicy::Idempotent)
            .body(body.to_string());
        self.send_body_request(request, operation).await
    }

    /// A `REPORT` returning the raw response, so a caller that must inspect a
    /// non-2xx status before it becomes an error can do so. Two kinds of caller
    /// do: CalDAV's `sync-collection`, which reads 403 `valid-sync-token` and
    /// 410 as cursor invalidation rather than failure; and both crates'
    /// `href_query_leg`, which reads a refused filter (`filter_unsupported`)
    /// as "run the query locally instead" rather than as a failed listing.
    pub async fn report_raw_response(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let method = Method::from_bytes(b"REPORT")
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let request = DavRequest::new(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .replay(bifrost_net::ReplayPolicy::Idempotent)
            .body(body.to_string());
        self.send_raw_request(request, operation).await
    }

    pub async fn report_raw(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let response = self
            .report_raw_response(url, depth, body, operation)
            .await?;
        settle_body(response, operation, self.protocol)
    }

    /// `DELETE` one resource. A target that is already gone is a success.
    ///
    /// RULED 2026-09-07 (dav-D1). A DELETE is REPLAYABLE, as HTTP says, and
    /// a 404 or 410 answer is absorbed here rather than classified: a delete
    /// whose target does not exist has reached its intended end state. That is
    /// what makes the replay safe. A DELETE dropped mid-flight before the
    /// server saw it replays and succeeds; one dropped after it landed replays,
    /// answers 404, and succeeds too - with no consumer probe either way. The
    /// alternative, declaring the DELETE unreplayable and sending every drop to
    /// `Reconcile(CheckTarget)`, cost a probe on every drop to keep one signal:
    /// a "resource never existed" for a consumer deleting by a stale id. That
    /// signal is now absorbed, deliberately; a consumer that needs it reads the
    /// resource first. CalDAV's `delete_event` and CardDAV's `delete_vcard`
    /// carry the same rule and must move with this one.
    pub async fn delete_resource(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let request = DavRequest::new(Method::DELETE, url)
            .replay(bifrost_net::ReplayPolicy::IdempotentUnlessConditional);
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() || delete_target_already_gone(response.status) {
            Ok(())
        } else {
            Err(status_error(
                &response.headers,
                operation,
                response.status,
                response.body,
                self.protocol,
            ))
        }
    }

    /// WebDAV `MOVE` of one resource into another collection.
    ///
    /// `Overwrite: F` so a name collision at the destination is refused rather
    /// than silently destroying whatever already sits there. `Destination` must
    /// be an absolute URI (RFC 4918 s10.3).
    ///
    /// The destination is gated by the same admitted-origin set as the source:
    /// it is a URL this client asks the server to write to, and a
    /// consumer-supplied collection id must not be able to steer it anywhere
    /// the credential gate would refuse.
    ///
    /// `Ok(false)` means the server does not implement MOVE, so the caller can
    /// fall back to copy-then-delete. Every other non-2xx is a real error - in
    /// particular 412 (the destination is occupied) and 502 (the server refuses
    /// the destination) are failures, not fallback triggers.
    pub async fn move_resource(
        &self,
        from: &str,
        to: &str,
        operation: AccountOperation,
    ) -> Result<bool, AccountError> {
        if !self.is_trusted_url(to) {
            return Err(local_error(
                operation,
                format!("refusing to name an untrusted DAV move destination: {to}"),
                self.protocol,
            ));
        }
        let method = Method::from_bytes(b"MOVE")
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let destination = HeaderValue::from_str(to)
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let request = DavRequest::new(method, from)
            .header(DESTINATION, destination)
            .header("Overwrite", "F")
            // `MOVE` is an extension method, so `bifrost-net` never replays it
            // on the wire - but the CLASSIFICATION of a mid-flight drop is
            // decided by the `AccountOperation` table, and a relocate runs
            // under `EventUpdate`/`ContactUpdate`, which that table calls
            // idempotent. Without this declaration the drop derives
            // `Retry(SameRequest)`, the engine re-issues the update, and the
            // re-issue GETs a source URL the committed MOVE already emptied:
            // 404 -> `NotFound` -> `ProviderRefused`, a move that succeeded
            // reported as a permanent refusal. Declaring it unreplayable
            // routes the drop to `Reconcile(CheckTarget)` instead.
            .replay(bifrost_net::ReplayPolicy::Never);
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() {
            return Ok(true);
        }
        if matches!(
            response.status,
            StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED
        ) {
            return Ok(false);
        }
        Err(status_error(
            &response.headers,
            operation,
            response.status,
            response.body,
            self.protocol,
        ))
    }
}

/// Evidence from responses received before the final DAV hop.
///
/// Net aggregates its own retries, but the DAV redirect walk starts a new
/// `AccountNet` request for each hop. Keep acknowledged responses here so a
/// later failure cannot erase an earlier non-idempotent commit possibility.
#[derive(Default)]
struct RedirectHistory {
    causes: Vec<Cause>,
    may_have_committed: bool,
}

impl RedirectHistory {
    fn record_response(&mut self, status: StatusCode, unreplayable: bool) {
        self.causes.push(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )));
        // 401 refuses this attempt before authentication. Other responses,
        // including redirects, retain net's conservative commit evidence.
        self.may_have_committed |= unreplayable && status != StatusCode::UNAUTHORIZED;
    }

    fn attach(
        &self,
        error: AccountError,
        unreplayable: bool,
        operation: AccountOperation,
        protocol: DavProtocol,
    ) -> AccountError {
        if self.causes.is_empty() {
            return error;
        }
        let terminal_state = error.chain().iter().find_map(|cause| match cause {
            Cause::Attempt(attempt) => Some(attempt.transmission_state),
            _ => None,
        });
        if unreplayable
            && self.may_have_committed
            && matches!(terminal_state, None | Some(TransmissionState::Unsent))
        {
            // A later hop can fail before dispatch (including while waiting
            // for admission). Credential errors can omit attempt evidence
            // entirely. The earlier response still makes the logical
            // mutation ambiguous, so use the existing partial-response
            // reconciliation lane and retain the terminal error as evidence.
            // History appended through into_builder is diagnostic only, so
            // declare the combined outcome on a fresh builder.
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
                Cause::Wire(WireCause::MalformedResponse {
                    protocol: protocol.protocol(),
                    detail: Some(DiagnosticText::support_only(
                        "DAV redirect chain failed after an unreplayable request may have committed",
                    )),
                }),
            )
            .push_cause(Cause::Attempt(AttemptCause::new(
                TransmissionState::InFlight,
            )))
            .protocol(protocol.protocol())
            .operation(operation)
            .idempotency_override(false);
            for cause in &self.causes {
                builder = builder.push_cause(cause.clone());
            }
            for cause in error.chain().iter() {
                builder = builder.push_cause(cause.clone());
            }
            return builder
                .try_build()
                .expect("valid DAV redirect-chain partial response");
        }
        let mut builder = error.into_builder();
        for cause in &self.causes {
            builder = builder.push_cause(cause.clone());
        }
        builder
            .try_build()
            .expect("valid DAV redirect-chain transmission evidence")
    }
}

/// Re-express `destination` against the URL the source was redirected to.
///
/// The hop is applied as a relative resolution rather than as a string rewrite:
/// the destination is made relative to the pre-hop request URL, then joined onto
/// the post-hop one. That is what keeps a cross-collection move
/// cross-collection, where prefix arithmetic on the two paths would not:
/// `/cal/a/one.ics` to `/cal/b/one.ics`, redirected to `/dav/cal/a/one.ics`,
/// yields `/dav/cal/b/one.ics` rather than losing the `b`.
///
/// `None` when the hop does not apply: an unparseable URL on either side, or a
/// destination on a different origin than the source, which the source's own
/// relocation says nothing about. The caller then keeps the destination it had.
fn rebase_destination(previous: &str, next: &Url, destination: &str) -> Option<String> {
    let previous = Url::parse(previous).ok()?;
    let destination = Url::parse(destination).ok()?;
    let relative = previous.make_relative(&destination)?;
    Some(next.join(&relative).ok()?.to_string())
}

/// Decode a DAV response body.
///
/// RFC 4918 bodies are XML, whose declared default encoding is UTF-8. Lossy
/// rather than strict keeps a malformed byte behaving as it always has - a
/// replacement character, not a failed request.
fn decode_body(body: &Bytes) -> String {
    String::from_utf8_lossy(body).into_owned()
}

impl std::fmt::Debug for DavDispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavDispatch")
            .field("base_url", &self.base_url)
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}

/// Whether a DELETE's answer says the target is already gone, which is the end
/// state the DELETE was asking for. 404 and 410 both say so; nothing else does.
#[must_use]
pub fn delete_target_already_gone(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND || status == StatusCode::GONE
}

fn response_read_error_with_history(
    error: NetError,
    limit: usize,
    operation: AccountOperation,
    protocol: DavProtocol,
) -> AccountError {
    let mut builder = response_read_error(
        operation,
        format!("DAV response body exceeded the buffered ceiling ({limit} bytes)"),
        protocol,
    )
    .into_builder();
    let evidence = into_account_error(
        error,
        NetErrorContext {
            provider: None,
            protocol: protocol.protocol(),
            operation,
            scope: None,
        },
    );
    for cause in evidence.chain().iter() {
        builder = builder.push_cause(cause.clone());
    }
    builder
        .try_build()
        .expect("valid DAV body failure evidence")
}

#[cfg(test)]
mod tests {
    use bifrost_net::{NetConfig, RetryPolicy, test_support::Canned};
    use bifrost_types::{
        AccountErrorKind, AuthErrorKind, ProtocolErrorKind, ReconcileAction, RecoveryClass,
        TransmissionState,
    };
    use reqwest::header::HeaderMap;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::test_support::{
        DavTranscript, dav_dropped_after_send, dav_redirect, dav_script, dav_script_empty,
        scripted_dav_net, scripted_dav_net_capped, transcripts,
    };

    const BASE: &str = "https://dav.example.test";

    struct DispatchReachedGuard(std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>);

    impl DavRequestGuard for DispatchReachedGuard {
        fn check_target(
            &self,
            _method: &Method,
            _url: &str,
            _operation: AccountOperation,
        ) -> Result<(), AccountError> {
            if let Some(entered) = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                let _ = entered.send(());
            }
            Ok(())
        }
    }

    fn dispatch_at(net: AccountNet, base_url: &str) -> DavDispatch {
        DavDispatch::with_account_net(
            net,
            base_url,
            DavCredentials::Basic {
                username: "user".to_string(),
                password: "pass".to_string(),
            },
            DavProtocol::CalDav,
        )
    }

    fn scripted_dispatch(net: AccountNet) -> DavDispatch {
        dispatch_at(net, BASE)
    }

    fn reconciles(error: &AccountError) -> bool {
        matches!(
            error.recovery(),
            RecoveryClass::Reconcile(advice)
                if advice.guidance.actions.contains(&ReconcileAction::CheckTarget)
        )
    }

    #[tokio::test(start_paused = true)]
    async fn batch_tally_counts_raw_bodies_across_retries_and_redirects() {
        let response = |status, body: &'static [u8]| Canned::Response {
            status,
            headers: HeaderMap::new(),
            body: Bytes::from_static(body),
        };
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(reqwest::header::LOCATION, "/next".parse().unwrap());
        let script = dav_script([
            response(StatusCode::SERVICE_UNAVAILABLE, b"retry"),
            Canned::Response {
                status: StatusCode::TEMPORARY_REDIRECT,
                headers: redirect_headers,
                body: Bytes::from_static(b"hop"),
            },
            response(StatusCode::OK, b"\xff\xfe"),
            response(StatusCode::OK, b"unrelated"),
            response(StatusCode::OK, b"second"),
        ]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));
        let (first, first_tally) = dispatch.metered();
        let (second, second_tally) = dispatch.metered();
        let operation = AccountOperation::SyncInventory;
        let result = first
            .send_raw_request(first.request(Method::GET, BASE), operation)
            .await
            .unwrap();
        assert_eq!(result.body.len(), 6, "lossy UTF-8 expands the two bytes");
        dispatch
            .send_raw_request(dispatch.request(Method::GET, BASE), operation)
            .await
            .unwrap();
        second
            .send_raw_request(second.request(Method::GET, BASE), operation)
            .await
            .unwrap();
        assert_eq!(
            first_tally.take(),
            10,
            "retry + redirect + raw success body"
        );
        assert_eq!(second_tally.take(), 6, "other streams cannot contribute");
        assert_eq!(first_tally.take(), 0, "a batch consumes its tally once");
        assert_eq!(transcripts(&script).len(), 5);
    }

    /// A `MOVE` dropped mid-flight must reconcile, not replay.
    ///
    /// `MOVE` is an extension method, so `bifrost-net` never replays it on the
    /// wire - but classification is a separate question, decided by the
    /// `AccountOperation` table, and a relocate runs under `EventUpdate` /
    /// `ContactUpdate`, which that table calls idempotent. Without the
    /// request-level declaration the drop derives `Retry(SameRequest)`; the
    /// engine re-issues the update, which GETs the source URL the committed
    /// MOVE already emptied, and a move that SUCCEEDED is reported as
    /// `NotFound` -> `ProviderRefused`.
    ///
    /// Exactly one drop is scripted, so a replay would exhaust the script and
    /// panic rather than pass quietly.
    #[tokio::test]
    async fn a_dropped_move_reconciles_rather_than_replaying() {
        let script = dav_script([dav_dropped_after_send()]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));

        let error = dispatch
            .move_resource(
                &format!("{BASE}/cal/a/one.ics"),
                &format!("{BASE}/cal/b/one.ics"),
                AccountOperation::EventUpdate,
            )
            .await
            .expect_err("a dropped MOVE must fail");

        assert_eq!(
            transcripts(&script).len(),
            1,
            "the dropped MOVE must not be replayed"
        );
        assert!(
            reconciles(&error),
            "a dropped MOVE must reconcile against the destination: {:?}",
            error.recovery()
        );
    }

    /// A relative native id lands UNDER the configured base path, and the two
    /// resolution branches agree on that.
    ///
    /// `around()` trims the base's trailing slash, and `Url::join` reads a
    /// slashless path as naming a resource, so the parsed branch used to replace
    /// the base's last segment: an account configured at `/dav` resolved
    /// `cal/one.ics` to `/cal/one.ics`, outside the collection it lives in. The
    /// string fallback for an unparseable base appended instead, so the same id
    /// resolved two different ways depending on whether the base parsed.
    ///
    /// The root-relative and absolute cases are pinned alongside because they
    /// are deliberately NOT append: an href the server names with a leading `/`
    /// is root-relative per RFC 3986, and replacing the base's whole path is the
    /// right answer for it.
    #[test]
    fn a_relative_id_resolves_under_the_base_path_on_both_branches() {
        let dispatch = dispatch_at(
            scripted_dav_net(&dav_script_empty()),
            "https://dav.example.test/dav/",
        );
        assert_eq!(
            dispatch.resolve_url("cal/one.ics"),
            "https://dav.example.test/dav/cal/one.ics",
            "a relative id belongs under the configured base path"
        );
        assert_eq!(
            dispatch.resolve_url("/cal/one.ics"),
            "https://dav.example.test/cal/one.ics",
            "a root-relative href replaces the base path"
        );
        assert_eq!(
            dispatch.resolve_url("https://other.example.test/cal/one.ics"),
            "https://other.example.test/cal/one.ics",
            "an absolute href is returned verbatim"
        );

        // The fallback branch, reached only when the base does not parse, must
        // give a relative id the same shape.
        let unparseable = dispatch_at(scripted_dav_net(&dav_script_empty()), "not a url/dav");
        assert_eq!(
            unparseable.resolve_url("cal/one.ics"),
            "not a url/dav/cal/one.ics"
        );
    }

    /// A first hop to an origin discovery never admitted is refused before
    /// anything is sent. `DavRequest::new` is public, so an external caller
    /// can skip `auth_headers`; the empty script panics if the walk sends.
    #[tokio::test]
    async fn an_untrusted_first_hop_is_refused_before_sending() {
        let script = dav_script_empty();
        let dispatch = scripted_dispatch(scripted_dav_net(&script));

        let error = dispatch
            .send_raw_request(
                DavRequest::new(Method::GET, "https://elsewhere.example.test/cal/"),
                AccountOperation::EventGet,
            )
            .await
            .expect_err("an untrusted first hop must be refused");

        assert!(transcripts(&script).is_empty(), "nothing may be sent");
        assert!(
            matches!(error.kind(), AccountErrorKind::Request(_)),
            "a local refusal, not a wire failure: {:?}",
            error.kind()
        );
    }

    /// A redirect of the SOURCE rebases the `Destination` on the same hop.
    ///
    /// The walk rebuilds each hop from the previous request's headers, so a MOVE
    /// redirected into a new collection namespace kept a `Destination` naming
    /// the pre-redirect one - the server was asked to write where it had just
    /// said the source no longer lives. The move here is cross-collection, which
    /// is the shape prefix arithmetic gets wrong: `b` must survive the rebase.
    #[tokio::test]
    async fn a_redirected_move_rebases_its_destination_on_the_same_hop() {
        let script = dav_script([
            dav_redirect(StatusCode::MOVED_PERMANENTLY, "/dav/cal/a/one.ics"),
            Canned::Response {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));

        let moved = dispatch
            .move_resource(
                &format!("{BASE}/cal/a/one.ics"),
                &format!("{BASE}/cal/b/one.ics"),
                AccountOperation::EventUpdate,
            )
            .await
            .expect("the followed MOVE succeeds");
        assert!(moved);

        let sent = transcripts(&script);
        assert_eq!(sent.len(), 2, "one hop, then the redirected MOVE");
        let destination = |hop: &DavTranscript| {
            hop.headers
                .get(DESTINATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        assert_eq!(
            destination(&sent[0]),
            format!("{BASE}/cal/b/one.ics"),
            "the first attempt names the destination the caller gave"
        );
        assert_eq!(sent[1].url, format!("{BASE}/dav/cal/a/one.ics"));
        assert_eq!(
            destination(&sent[1]),
            format!("{BASE}/dav/cal/b/one.ics"),
            "the destination must follow the source into the new namespace"
        );
    }

    /// A DELETE replays after a mid-flight drop, and a target already gone is
    /// a success - the two halves of the dav-D1 ruling, which only hold
    /// together: the replay is safe BECAUSE the 404 a replayed landed DELETE
    /// answers is absorbed. Ablation: with the 404 arm removed the second case
    /// reports a successful delete as `NotFound`; with the request declared
    /// unreplayable the first case starves on the script.
    #[tokio::test]
    async fn a_delete_replays_and_reads_a_gone_target_as_success() {
        let gone = |status: StatusCode| Canned::Response {
            status,
            headers: HeaderMap::new(),
            body: bytes::Bytes::new(),
        };
        for (script, requests, label) in [
            (
                dav_script([dav_dropped_after_send(), gone(StatusCode::NO_CONTENT)]),
                2,
                "a drop before the server saw the DELETE replays and lands",
            ),
            (
                dav_script([dav_dropped_after_send(), gone(StatusCode::NOT_FOUND)]),
                2,
                "a drop after the DELETE landed replays, answers 404, and succeeds",
            ),
            (
                dav_script([gone(StatusCode::GONE)]),
                1,
                "a target already gone is the delete's end state",
            ),
        ] {
            let dispatch = scripted_dispatch(scripted_dav_net(&script));
            dispatch
                .delete_resource(
                    &format!("{BASE}/cal/one.ics"),
                    AccountOperation::EventDelete,
                )
                .await
                .unwrap_or_else(|error| panic!("{label}: {error:?}"));
            assert_eq!(transcripts(&script).len(), requests, "{label}");
        }
    }

    fn replay_dav_response(status: StatusCode, body: &str) -> Canned {
        Canned::Response {
            status,
            headers: HeaderMap::new(),
            body: Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    #[tokio::test]
    async fn an_undeclared_conditional_put_drop_reconciles() {
        let script = dav_script([
            dav_dropped_after_send(),
            replay_dav_response(StatusCode::NO_CONTENT, ""),
        ]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));
        let error = dispatch
            .send_raw_request(
                DavRequest::new(Method::PUT, format!("{BASE}/one.ics")).header("If-Match", "v1"),
                AccountOperation::EventUpdate,
            )
            .await
            .unwrap_err();
        assert!(reconciles(&error));
        assert_eq!(transcripts(&script).len(), 1);
    }

    #[test]
    fn a_status_after_a_possible_commit_is_not_read_as_data() {
        let dispatch = scripted_dispatch(scripted_dav_net(&dav_script_empty()));
        for (method, code, operation) in [
            (Method::PUT, 412, AccountOperation::EventUpdate),
            (Method::DELETE, 404, AccountOperation::EventDelete),
            (
                Method::from_bytes(b"MOVE").unwrap(),
                404,
                AccountOperation::EventUpdate,
            ),
            (Method::PUT, 405, AccountOperation::EventUpdate),
        ] {
            let status = |code| NetError::Status {
                code: StatusCode::from_u16(code).unwrap(),
                headers: HeaderMap::new(),
                body: Bytes::new(),
            };
            let request =
                DavRequest::new(method, format!("{BASE}/one.ics")).header("If-Match", "v1");
            let result = dispatch.wire_outcome(
                &request,
                operation,
                Err(NetError::AttemptHistory {
                    source: Box::new(status(code)),
                    attempts: vec![status(503)],
                    may_have_committed: true,
                }),
            );
            if code == 405 {
                assert_eq!(result.unwrap().status, StatusCode::METHOD_NOT_ALLOWED);
            } else {
                assert!(reconciles(&result.unwrap_err()));
            }
        }
        let request = DavRequest::new(
            Method::from_bytes(b"MOVE").unwrap(),
            format!("{BASE}/one.ics"),
        );
        let result = dispatch.wire_outcome(
            &request,
            AccountOperation::EventUpdate,
            Err(NetError::AttemptHistory {
                source: Box::new(NetError::RetryBudgetExhausted {
                    final_response: Some(bifrost_net::error::FinalResponse {
                        status: StatusCode::NOT_IMPLEMENTED,
                        headers: HeaderMap::new(),
                        body: Bytes::new(),
                    }),
                    retry_after_history: vec![],
                }),
                attempts: vec![],
                may_have_committed: true,
            }),
        );
        assert_eq!(result.unwrap().status, StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn an_unreplayable_dav_status_stops_before_the_next_reply() {
        for (method, terminal) in [
            (Method::PUT, StatusCode::PRECONDITION_FAILED),
            (Method::DELETE, StatusCode::NOT_FOUND),
            (
                Method::from_bytes(b"MOVE").unwrap(),
                StatusCode::METHOD_NOT_ALLOWED,
            ),
        ] {
            let script = dav_script([
                replay_dav_response(StatusCode::SERVICE_UNAVAILABLE, "unknown"),
                replay_dav_response(terminal, "unused"),
            ]);
            let dispatch = scripted_dispatch(scripted_dav_net(&script));
            // PUT and DELETE are unreplayable because they are conditional;
            // MOVE is unreplayable by its method default.
            let mut request = DavRequest::new(method.clone(), format!("{BASE}/one.ics"));
            if method != Method::from_bytes(b"MOVE").unwrap() {
                request = request.header("If-Match", "v1");
            }
            let error = dispatch
                .send_raw_request(request, AccountOperation::EventUpdate)
                .await
                .unwrap_err();
            assert!(reconciles(&error));
            assert_eq!(transcripts(&script).len(), 1);
            assert_eq!(script.remaining(), 1);
        }
        for status in [StatusCode::METHOD_NOT_ALLOWED, StatusCode::NOT_IMPLEMENTED] {
            let script = dav_script([replay_dav_response(status, "unsupported")]);
            let dispatch = scripted_dispatch(scripted_dav_net(&script));
            assert!(
                !dispatch
                    .move_resource(
                        &format!("{BASE}/one.ics"),
                        &format!("{BASE}/two.ics"),
                        AccountOperation::EventUpdate
                    )
                    .await
                    .unwrap()
            );
        }
        let script = dav_script([replay_dav_response(
            StatusCode::PRECONDITION_FAILED,
            "conflict",
        )]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));
        let error = dispatch
            .send_status_request(
                DavRequest::new(Method::PUT, format!("{BASE}/one.ics")).header("If-Match", "v1"),
                AccountOperation::EventUpdate,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
        let script = dav_script([
            answer(StatusCode::UNAUTHORIZED),
            answer(StatusCode::PRECONDITION_FAILED),
        ]);
        let refreshes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dispatch = DavDispatch::with_account_net(
            scripted_dav_net(&script),
            BASE,
            DavCredentials::Bearer {
                token_source: Arc::new(RotatingTokenSource {
                    refreshes: Arc::clone(&refreshes),
                }),
            },
            DavProtocol::CalDav,
        );
        let error = dispatch
            .send_status_request(
                DavRequest::new(Method::PUT, format!("{BASE}/one.ics")).header("If-Match", "v1"),
                AccountOperation::EventUpdate,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
        assert_eq!(refreshes.load(Ordering::Relaxed), 1);
        assert_eq!(transcripts(&script).len(), 2);
    }

    /// An oversized response on an unreplayable request reconciles too.
    ///
    /// `response_read_error` mints `Protocol(PartialResponse)` with an
    /// ACKNOWLEDGED attempt, and `derive_protocol` routes that purely by
    /// idempotency - so this arm needs the same declaration the transport arm
    /// does. The case that bites is the copy leg of the MOVE fallback: a
    /// create-PUT under `EventUpdate`, which the operation table calls
    /// idempotent, deriving `Retry(SameRequest)` whose replay answers 412 at
    /// the destination the first attempt already occupied - forever.
    #[tokio::test]
    async fn an_oversized_response_on_an_unreplayable_request_reconciles() {
        let oversized = || Canned::Response {
            status: StatusCode::CREATED,
            headers: HeaderMap::new(),
            body: Bytes::from(vec![b'x'; 64]),
        };
        for idempotent in [true, false] {
            let script = dav_script([oversized()]);
            let dispatch = scripted_dispatch(scripted_dav_net_capped(&script, Some(8)));
            let url = format!("{BASE}/cal/b/one.ics");
            let request = DavRequest::new(Method::PUT, &url)
                .header("If-None-Match", "*")
                .headers(
                    dispatch
                        .auth_headers(&url, AccountOperation::EventUpdate)
                        .await
                        .expect("credentials mint for the base origin"),
                )
                .idempotent(idempotent)
                .body("BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n".to_string());

            let error = dispatch
                .send_raw_request(request, AccountOperation::EventUpdate)
                .await
                .expect_err("an oversized body must fail");

            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
                ),
                "idempotent={idempotent}: {:?}",
                error.kind()
            );
            if idempotent {
                // The control: an ordinary replayable request keeps the retry
                // classification, so the assertion below is about the
                // declaration rather than about the arm as a whole.
                assert!(
                    matches!(error.recovery(), RecoveryClass::Retry(_)),
                    "a replayable request may retry: {:?}",
                    error.recovery()
                );
            } else {
                assert!(
                    reconciles(&error),
                    "an unreplayable request must reconcile: {:?}",
                    error.recovery()
                );
            }
        }
    }

    fn acknowledged(error: &AccountError) -> bool {
        error.chain().iter().any(|cause| {
            matches!(
                cause,
                bifrost_types::Cause::Attempt(attempt)
                    if attempt.transmission_state == TransmissionState::Acknowledged
            )
        })
    }

    /// A redirect the walk will not follow is the PROVIDER's contract fault,
    /// classified exactly as `bifrost-net` classifies its own redirect loop:
    /// `Protocol(ContractViolation)` with an acknowledged attempt.
    ///
    /// Against the code before the reclassification every case fails the kind
    /// assertion: the loop and the unresolvable `Location` were
    /// `local_error` (`Request(Malformed)` -> `ClientBug`, no attempt), and a
    /// `Location` that would not decode was not an error at all - the 3xx was
    /// handed back as a response and classified by the status ladder as a
    /// server refusal of status 302.
    #[tokio::test]
    async fn a_redirect_the_walk_will_not_follow_is_the_providers_contract_fault() {
        let max_hops = usize::from(bifrost_net::DEFAULT_MAX_HOPS);
        let looping = (0..=max_hops)
            .map(|_| dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/next"))
            .collect::<Vec<_>>();
        let mut undecodable = HeaderMap::new();
        undecodable.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_bytes(b"/caf\xe9").expect("obs-text is a legal header value"),
        );
        for (steps, requests, label) in [
            (looping, max_hops + 1, "a chain past the hop cap"),
            (
                vec![dav_redirect(StatusCode::FOUND, "https://")],
                1,
                "a Location with no host will not resolve",
            ),
            (
                vec![Canned::Response {
                    status: StatusCode::FOUND,
                    headers: undecodable,
                    body: Bytes::new(),
                }],
                1,
                "a Location that is not valid UTF-8",
            ),
        ] {
            let script = dav_script(steps);
            let dispatch = scripted_dispatch(scripted_dav_net(&script));

            let error = dispatch
                .propfind_raw(
                    &format!("{BASE}/start"),
                    "0",
                    "<propfind/>",
                    AccountOperation::Discover,
                )
                .await
                .expect_err(label);

            assert_eq!(transcripts(&script).len(), requests, "{label}");
            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
                ),
                "{label}: {:?}",
                error.kind()
            );
            assert!(acknowledged(&error), "{label}: the server answered a hop");
            assert_eq!(
                error.recovery(),
                &RecoveryClass::ProviderContractViolation,
                "{label}"
            );
        }
    }

    /// A redirect to an origin the credential gate never admitted is refused
    /// locally - the next hop never goes out - but the refusal carries the
    /// ACKNOWLEDGED attempt the answered first hop earned.
    ///
    /// Against the code before the stamp the evidence assertion fails: the
    /// refusal was a bare `local_error` with no attempt cause, which read as
    /// `Unsent` though the server had answered. The kind stays
    /// `Request(Malformed)`, because the well-known fallback predicate reads a
    /// refused redirect by it; the first-hop gate in `auth_headers`, where
    /// nothing has been sent, stays unstamped.
    #[tokio::test]
    async fn a_refused_redirect_says_the_first_hop_was_answered() {
        let script = dav_script([dav_redirect(StatusCode::FOUND, "https://evil.test/dav/")]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));

        let error = dispatch
            .propfind_raw(
                &format!("{BASE}/start"),
                "0",
                "<propfind/>",
                AccountOperation::Discover,
            )
            .await
            .expect_err("a redirect to an unadmitted origin is refused");

        assert_eq!(
            transcripts(&script).len(),
            1,
            "the refused hop must not go out"
        );
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert!(
            acknowledged(&error),
            "the server answered the first hop: {error:?}"
        );
        assert!(crate::should_fallback_discovery(
            &error,
            DavProtocol::CalDav
        ));

        let first_hop = dispatch
            .auth_headers("https://evil.test/dav/", AccountOperation::Discover)
            .await
            .expect_err("the gate refuses an unadmitted first hop");
        assert!(
            !acknowledged(&first_hop),
            "nothing was sent before a first-hop refusal: {first_hop:?}"
        );
    }

    struct RotatingTokenSource {
        refreshes: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl TokenSource for RotatingTokenSource {
        fn current(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            // Deliberately remains stale: the dispatcher must use the refresh
            // return value, not assume a simple source stores it in current().
            Box::pin(async { Ok(AccessToken::new("old", None)) })
        }

        fn refresh(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            self.refreshes.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(AccessToken::new("fresh", None)) })
        }

        fn refresh_if_unchanged(
            &self,
            used: AccessToken,
        ) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            assert_eq!(
                used.as_str(),
                "old",
                "refresh uses the token sent on the wire"
            );
            self.refresh()
        }
    }

    fn answer(status: StatusCode) -> Canned {
        Canned::Response {
            status,
            headers: HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    #[tokio::test]
    async fn bearer_401_refreshes_once_and_replays_the_exact_body() {
        for terminal in [StatusCode::CREATED, StatusCode::UNAUTHORIZED] {
            let script = dav_script([answer(StatusCode::UNAUTHORIZED), answer(terminal)]);
            let refreshes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let dispatch = DavDispatch::with_account_net(
                scripted_dav_net(&script),
                BASE,
                DavCredentials::Bearer {
                    token_source: Arc::new(RotatingTokenSource {
                        refreshes: Arc::clone(&refreshes),
                    }),
                },
                DavProtocol::CalDav,
            );
            let result = dispatch
                .send_status_request(
                    DavRequest::new(Method::PUT, format!("{BASE}/one.ics"))
                        .replay(bifrost_net::ReplayPolicy::Never)
                        .body("calendar bytes"),
                    AccountOperation::EventCreate,
                )
                .await;
            if terminal.is_success() {
                result.expect("refresh recovers the request");
            } else {
                assert_eq!(
                    result.expect_err("second 401 is terminal").recovery(),
                    &RecoveryClass::AuthLost
                );
            }
            assert_eq!(refreshes.load(Ordering::Relaxed), 1);
            let sent = transcripts(&script);
            assert_eq!(sent.len(), 2);
            assert_eq!(sent[0].headers[AUTHORIZATION], "Bearer old");
            assert_eq!(sent[1].headers[AUTHORIZATION], "Bearer fresh");
            assert_eq!(sent[0].body, "calendar bytes");
            assert_eq!(sent[1].body, sent[0].body);
        }
    }

    #[tokio::test]
    async fn a_429_refusal_preserves_move_fallback_but_create_ambiguity_reconciles() {
        let script = dav_script([
            answer(StatusCode::TOO_MANY_REQUESTS),
            answer(StatusCode::METHOD_NOT_ALLOWED),
        ]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));
        assert!(
            !dispatch
                .move_resource(
                    &format!("{BASE}/a.ics"),
                    &format!("{BASE}/b.ics"),
                    AccountOperation::EventUpdate
                )
                .await
                .unwrap()
        );
        assert_eq!(transcripts(&script).len(), 2);
        let script = dav_script([
            answer(StatusCode::TOO_MANY_REQUESTS),
            answer(StatusCode::PRECONDITION_FAILED),
        ]);
        let dispatch = scripted_dispatch(scripted_dav_net(&script));
        let error = dispatch
            .send_status_request(
                DavRequest::new(Method::PUT, format!("{BASE}/new.ics"))
                    .replay(bifrost_net::ReplayPolicy::Never)
                    .header("If-None-Match", "*")
                    .body("calendar"),
                AccountOperation::EventCreate,
            )
            .await
            .unwrap_err();
        assert!(reconciles(&error));
        assert_eq!(transcripts(&script).len(), 2);
    }

    #[test]
    fn an_ambiguous_prior_attempt_cannot_be_hidden_by_a_terminal_status() {
        let dispatch = scripted_dispatch(scripted_dav_net(&dav_script_empty()));
        let request = DavRequest::new(Method::PUT, format!("{BASE}/new.ics"))
            .replay(bifrost_net::ReplayPolicy::Never);
        let error = dispatch
            .wire_outcome(
                &request,
                AccountOperation::EventCreate,
                Err(NetError::AttemptHistory {
                    source: Box::new(NetError::Status {
                        code: StatusCode::PRECONDITION_FAILED,
                        headers: HeaderMap::new(),
                        body: Bytes::new(),
                    }),
                    attempts: vec![NetError::Network {
                        message: "lost response".into(),
                        transmission_state: TransmissionState::InFlight,
                        source: None,
                    }],
                    may_have_committed: true,
                }),
            )
            .expect_err("an ambiguous create must reconcile");
        assert!(reconciles(&error));
    }

    #[tokio::test]
    async fn an_acknowledged_unreplayable_redirect_is_kept_on_a_later_unsent_failure() {
        use bifrost_net::test_support::scripted_net;

        let script = dav_script([
            dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/next"),
            Canned::Error(NetError::Timeout {
                transmission_state: TransmissionState::Unsent,
            }),
        ]);
        let mut spec = AccountSpec::new(None);
        spec.follow_redirects = FollowRedirects::Disabled;
        let mut retry = RetryPolicy::default();
        retry.max_attempts = 1;
        retry.initial_backoff = std::time::Duration::ZERO;
        retry.max_backoff = std::time::Duration::ZERO;
        spec.default_retry = retry;
        let account_net = scripted_net(&script, NetConfig::default())
            .attach_account(AccountId("dav-redirect-history".to_string()), spec);
        let dispatch = dispatch_at(account_net, BASE);
        let error = dispatch
            .send_raw_request(
                DavRequest::new(Method::PUT, format!("{BASE}/start"))
                    .replay(bifrost_net::ReplayPolicy::Never),
                AccountOperation::EventUpdate,
            )
            .await
            .expect_err("the second hop times out before admission");

        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
            ),
            "the first hop may have committed: {error:?}"
        );
        assert!(
            reconciles(&error),
            "the redirect chain is ambiguous: {error:?}"
        );
        let states: Vec<_> = error
            .chain()
            .iter()
            .filter_map(|cause| match cause {
                Cause::Attempt(attempt) => Some(attempt.transmission_state),
                _ => None,
            })
            .collect();
        assert!(
            states.contains(&TransmissionState::Acknowledged),
            "{states:?}"
        );
        assert!(states.contains(&TransmissionState::InFlight), "{states:?}");
        assert!(states.contains(&TransmissionState::Unsent), "{states:?}");
        assert_eq!(script.requests().len(), 2);
    }

    struct RedirectFailingTokenSource {
        reads: std::sync::atomic::AtomicUsize,
        fail_refresh: bool,
    }

    impl TokenSource for RedirectFailingTokenSource {
        fn current(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            let read = self.reads.fetch_add(1, Ordering::Relaxed);
            let succeeds = read == 0 || self.fail_refresh;
            Box::pin(async move {
                if succeeds {
                    Ok(AccessToken::new("token", None))
                } else {
                    Err(redirect_refresh_failure())
                }
            })
        }

        fn refresh(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            Box::pin(async { Err(redirect_refresh_failure()) })
        }
    }

    fn redirect_refresh_failure() -> NetError {
        NetError::RefreshFailed {
            retry_after: None,
            source: Arc::new(NetError::Timeout {
                transmission_state: TransmissionState::Unsent,
            }),
        }
    }

    #[tokio::test]
    async fn credential_failure_after_an_unreplayable_redirect_requires_reconciliation() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            for fail_refresh in [false, true] {
                let mut responses = vec![dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/next")];
                if fail_refresh {
                    responses.push(answer(StatusCode::UNAUTHORIZED));
                }
                let script = dav_script(responses);
                let dispatch = DavDispatch::with_account_net(
                    scripted_dav_net(&script),
                    BASE,
                    DavCredentials::Bearer {
                        token_source: Arc::new(RedirectFailingTokenSource {
                            reads: std::sync::atomic::AtomicUsize::new(0),
                            fail_refresh,
                        }),
                    },
                    protocol,
                );
                let error = dispatch
                    .send_raw_request(
                        DavRequest::new(Method::PUT, format!("{BASE}/start"))
                            .replay(bifrost_net::ReplayPolicy::Never),
                        AccountOperation::EventUpdate,
                    )
                    .await
                    .expect_err("credential failure must preserve the earlier possible commit");

                assert!(reconciles(&error), "{error:?}");
                assert!(acknowledged(&error), "{error:?}");
                assert!(
                    error
                        .chain()
                        .iter()
                        .any(|cause| matches!(cause, Cause::Auth(_)))
                );
                assert_eq!(script.requests().len(), if fail_refresh { 2 } else { 1 });
            }
        }
    }

    struct AdvancingTokenSource;

    impl TokenSource for AdvancingTokenSource {
        fn current(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            Box::pin(async {
                tokio::time::advance(std::time::Duration::from_secs(2)).await;
                Ok(AccessToken::new("token", None))
            })
        }

        fn refresh(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            self.current()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn redirect_hops_use_the_remaining_logical_request_deadline() {
        let script = dav_script([
            dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/next"),
            answer(StatusCode::OK),
        ]);
        let dispatch = DavDispatch::with_account_net(
            scripted_dav_net(&script),
            BASE,
            DavCredentials::Bearer {
                token_source: Arc::new(AdvancingTokenSource),
            },
            DavProtocol::CalDav,
        );
        // Model twenty seconds already spent reading credentials or prior hops.
        let remaining = std::time::Duration::from_secs(10);
        dispatch
            .send_raw_until(
                DavRequest::new(Method::GET, format!("{BASE}/start")),
                AccountOperation::EventGet,
                Instant::now() + remaining,
            )
            .await
            .expect("both hops complete inside the remaining budget");
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].timeout, Some(std::time::Duration::from_secs(8)));
        assert_eq!(requests[1].timeout, Some(std::time::Duration::from_secs(6)));
    }

    #[test]
    fn exhausted_retry_history_keeps_a_hint_absent_from_the_final_response() {
        let dispatch = scripted_dispatch(scripted_dav_net(&dav_script_empty()));
        let request = DavRequest::new(Method::GET, BASE);
        let error = dispatch
            .wire_outcome(
                &request,
                AccountOperation::EventGet,
                Err(NetError::RetryBudgetExhausted {
                    final_response: Some(bifrost_net::FinalResponse {
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        headers: HeaderMap::new(),
                        body: Bytes::new(),
                    }),
                    retry_after_history: vec![std::time::Duration::from_secs(300)],
                }),
            )
            .expect_err("exhaustion keeps its retry history");
        assert!(error.chain().iter().any(|cause| matches!(cause,
            bifrost_types::Cause::Server(bifrost_types::ServerCause::Unavailable {
                retry_hint: Some(bifrost_types::RetryHint::After(delay)),
            }) if *delay == std::time::Duration::from_secs(300))));
    }

    struct PendingTokenSource;

    impl TokenSource for PendingTokenSource {
        fn current(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            Box::pin(std::future::pending())
        }
        fn refresh(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            self.current()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn credential_wait_is_inside_the_logical_deadline() {
        let script = dav_script_empty();
        let dispatch = DavDispatch::with_account_net(
            scripted_dav_net(&script),
            BASE,
            DavCredentials::Bearer {
                token_source: Arc::new(PendingTokenSource),
            },
            DavProtocol::CalDav,
        );
        let start = Instant::now();
        let error = dispatch
            .send_raw_request(
                DavRequest::new(Method::GET, BASE),
                AccountOperation::EventGet,
            )
            .await
            .expect_err("stalled credential read times out without wire traffic");
        assert_eq!(start.elapsed(), DAV_CLIENT_TIMEOUT);
        assert!(script.requests().is_empty());
        assert!(!acknowledged(&error));
        assert!(!reconciles(&error));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_waiting_for_net_admission_is_unsent() {
        use bifrost_net::test_support::scripted_net;

        let script = dav_script([Canned::Pending]);
        let mut spec = AccountSpec::new(None);
        spec.request_timeout = Some(DAV_CLIENT_TIMEOUT);
        spec.connect_timeout = None;
        spec.response_headers_timeout = None;
        spec.follow_redirects = FollowRedirects::Disabled;
        spec.concurrency_limit = Some(bifrost_net::ConcurrencyLimit::new(1));
        let account_net = scripted_net(&script, NetConfig::default())
            .attach_account(AccountId("dav-admission-timeout".to_string()), spec);
        let holding_net = account_net.clone();
        let holding_request = tokio::spawn(async move {
            holding_net
                .get(BASE)
                .without_bearer_auth()
                .timeout(std::time::Duration::from_secs(60))
                .send()
                .await
        });
        for _ in 0..100 {
            if script.requests().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(script.requests().len(), 1, "the permit holder is in flight");

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let mut dispatch = dispatch_at(account_net, BASE);
        dispatch.set_request_guard(Arc::new(DispatchReachedGuard(std::sync::Mutex::new(Some(
            entered_tx,
        )))));
        let waiting_request = tokio::spawn(async move {
            dispatch
                .send_raw_request(
                    DavRequest::new(Method::PUT, format!("{BASE}/waiting"))
                        .replay(bifrost_net::ReplayPolicy::Never),
                    AccountOperation::EventUpdate,
                )
                .await
        });
        entered_rx
            .await
            .expect("DAV request reached its guard before admission");
        assert_eq!(
            script.requests().len(),
            1,
            "the DAV request is still waiting for concurrency admission"
        );

        tokio::time::advance(DAV_CLIENT_TIMEOUT + std::time::Duration::from_millis(1)).await;
        let waiting_error = waiting_request
            .await
            .expect("waiting task completes")
            .expect_err("admission wait times out");
        assert!(
            matches!(waiting_error.recovery(), RecoveryClass::Retry(_)),
            "the PUT that never passed admission is safe to retry: {waiting_error:?}"
        );
        assert!(
            waiting_error.chain().iter().any(|cause| matches!(
                cause,
                Cause::Attempt(attempt)
                    if attempt.transmission_state == TransmissionState::Unsent
            )),
            "the expired admission wait is unsent: {waiting_error:?}"
        );
        assert_eq!(
            script.requests().len(),
            1,
            "the waiting PUT never dispatched"
        );
        holding_request.abort();
        let _ = holding_request.await;
    }

    #[tokio::test(start_paused = true)]
    async fn public_wrappers_bound_pending_credentials_before_dispatch() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            for wrapper in 0..5 {
                let script = dav_script_empty();
                let dispatch = DavDispatch::with_account_net(
                    scripted_dav_net(&script),
                    BASE,
                    DavCredentials::Bearer {
                        token_source: Arc::new(PendingTokenSource),
                    },
                    protocol,
                );
                let start = Instant::now();
                let operation = AccountOperation::Discover;
                let result = match wrapper {
                    0 => dispatch
                        .propfind_raw(BASE, "0", "", operation)
                        .await
                        .map(|_| ()),
                    1 => dispatch
                        .report_raw_response(BASE, "1", "", operation)
                        .await
                        .map(|_| ()),
                    2 => dispatch.delete_resource(BASE, operation).await,
                    3 => dispatch
                        .move_resource(BASE, &format!("{BASE}/target"), operation)
                        .await
                        .map(|_| ()),
                    _ => dispatch.auth_headers(BASE, operation).await.map(|_| ()),
                };
                let error = result.expect_err("pending credentials must time out");
                assert_eq!(start.elapsed(), DAV_CLIENT_TIMEOUT);
                assert!(script.requests().is_empty());
                assert!(!acknowledged(&error));
                assert!(!reconciles(&error));
                assert!(matches!(error.kind(), AccountErrorKind::Transport(_)));
            }
        }
    }

    /// A token source that always fails, with the error the case under test
    /// needs.
    struct FailingTokenSource(fn() -> NetError);

    impl TokenSource for FailingTokenSource {
        fn current(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            let make = self.0;
            Box::pin(async move { Err(make()) })
        }

        fn refresh(&self) -> bifrost_types::AccountFuture<Result<AccessToken, NetError>> {
            self.current()
        }
    }

    fn bearer_dispatch(make: fn() -> NetError) -> DavDispatch {
        DavDispatch::with_account_net(
            scripted_dav_net(&dav_script_empty()),
            BASE,
            DavCredentials::Bearer {
                token_source: Arc::new(FailingTokenSource(make)),
            },
            DavProtocol::CalDav,
        )
    }

    /// A credential the token source cannot mint is classified by WHY.
    ///
    /// `transport_error` now stamps `Unsent`, which derives
    /// `Retry(SameRequest)`. That is right for a transient read failure and
    /// wrong for a permanently revoked token: folding every `TokenSource`
    /// failure into it turns a dead credential into a retry storm that never
    /// raises a reauthorization signal. So the two auth-bearing net errors go
    /// through `net_account_error`, which passes DAV's explicit account
    /// throttle scope to `bifrost_net::into_account_error_with_throttle_scope`,
    /// and only the remainder keeps the `Unsent` transport classification.
    ///
    /// Nothing is scripted, so a case that reaches the wire panics on an
    /// exhausted script rather than passing.
    #[tokio::test]
    async fn a_token_source_failure_is_classified_by_why_it_failed() {
        let revoked = bearer_dispatch(|| NetError::AuthLost {
            transmission_state: None,
            final_response: None,
        })
        .auth_headers(&format!("{BASE}/cal/"), AccountOperation::EventUpdate)
        .await
        .expect_err("a revoked token cannot mint a credential");
        assert!(
            matches!(
                revoked.kind(),
                AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
            ),
            "a revoked bearer token must ask for reauthorization: {:?}",
            revoked.kind()
        );
        assert!(
            !matches!(revoked.recovery(), RecoveryClass::Retry(_)),
            "a revoked bearer token must not retry: {:?}",
            revoked.recovery()
        );

        let refresh = bearer_dispatch(|| NetError::RefreshFailed {
            retry_after: None,
            source: Arc::new(NetError::Timeout {
                transmission_state: TransmissionState::Unsent,
            }),
        })
        .auth_headers(&format!("{BASE}/cal/"), AccountOperation::EventUpdate)
        .await
        .expect_err("a failed refresh cannot mint a credential");
        assert!(
            matches!(
                refresh.kind(),
                AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient)
            ),
            "a failed refresh is transient auth, not transport: {:?}",
            refresh.kind()
        );

        let other = bearer_dispatch(|| NetError::Cancelled)
            .auth_headers(&format!("{BASE}/cal/"), AccountOperation::EventUpdate)
            .await
            .expect_err("a cancelled token read cannot mint a credential");
        assert!(
            matches!(other.kind(), AccountErrorKind::Transport(_)),
            "every other token failure stays a transport error: {:?}",
            other.kind()
        );
        assert!(
            matches!(other.recovery(), RecoveryClass::Retry(_)),
            "nothing was sent, so the same request may be retried: {:?}",
            other.recovery()
        );
    }
}
