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
//!   crate makes and the transport cannot.

use std::sync::Arc;

use base64::Engine as _;
use bifrost_net::{
    AccountNet, AccountSpec, Error as NetError, FollowRedirects, MalformedRedirectKind, Net,
    NetErrorContext, TokenSource, into_account_error,
};
use bifrost_types::{AccountError, AccountId, AccountOperation};
use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};

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
        }
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
        if !self.is_trusted_url(url) {
            return Err(local_error(
                operation,
                format!("refusing to send DAV credentials to untrusted URL: {url}"),
                self.protocol,
            ));
        }
        let mut headers = HeaderMap::new();
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
            }
        }
        Ok(headers)
    }

    /// Classify a `TokenSource::current()` failure.
    ///
    /// Folding every one of these into `transport_error` was survivable while
    /// that constructor claimed `InFlight` - the result reconciled and stopped.
    /// It now claims `Unsent`, which derives `Retry(SameRequest)`, and a
    /// PERMANENT credential failure retried as a transport blip is a retry storm
    /// that never emits the reauthorization signal the consumer needs. So the
    /// two auth-bearing net errors are handed to the same funnel the wire path
    /// uses, `bifrost_net::into_account_error`, which maps `AuthLost` to
    /// `Auth(ReauthorizationRequired)` and `RefreshFailed` to
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
    /// classified. Everything else - an exhausted retry budget, a rate limit
    /// past its budget, a transport failure - is left to
    /// `bifrost_net::into_account_error`, which classifies with the retry hints
    /// and transmission evidence the DAV status ladder does not carry.
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
            .without_bearer_auth();
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
        if let Some(idempotent) = request.idempotent {
            builder = builder.idempotent(idempotent);
        }
        if let Some(body) = &request.body {
            builder = builder.body(body.clone());
        }
        // A request that declared itself unreplayable on the wire must classify
        // that way too, whichever failure it hits. The `AccountOperation` table
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
        self.wire_outcome(request, operation, builder.send().await)
            .map_err(|error| {
                if request.idempotent == Some(false) {
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
                // A replayable DAV request can use its terminal response as
                // data (notably DELETE -> 404 means done). Unreplayable
                // histories must reach the shared reconciliation classifier.
                if request
                    .idempotent
                    .unwrap_or_else(|| request.method.is_idempotent())
                {
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
                        } => Some((response.status, &response.headers, &response.body)),
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
            // A retry budget spent against a status-bearing response, or a 429
            // past its budget, still ends in a response the server sent. The
            // DAV ladder classifies it exactly as it did before this crate had
            // any retry at all - the difference is only that the server got
            // asked more than once first, which is the capability dav-B9 was
            // about. A budget exhausted with no final response (every attempt
            // failed below the status line) has nothing to classify and falls
            // through to the transport mapping.
            Err(
                NetError::RetryBudgetExhausted {
                    final_response: Some(final_response),
                    ..
                }
                | NetError::RateLimited { final_response, .. },
            ) => Ok(DavResponse {
                status: final_response.status,
                headers: final_response.headers,
                body: decode_body(&final_response.body),
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
        // The named shared constant, not `RedirectPolicy::default().max_hops`:
        // DAV has no per-account redirect config, and laundering the cap
        // through a type Default hid the coupling (changing the default
        // silently changed DAV behavior).
        let max_hops = usize::from(bifrost_net::DEFAULT_MAX_HOPS);
        // The first hop is gated here, before anything is sent, exactly as
        // every later hop is gated below. Every in-workspace caller already
        // credentials its URL through `auth_headers`, the same gate, but
        // `DavRequest::new` and this method are public, so an external caller
        // can hand over a URL nobody checked. Checking only the final answer
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
        let mut request = request;
        let mut hops = 0usize;
        loop {
            // Check both the source and the current Destination on EVERY hop.
            // Redirect rebasing below has already produced this hop's final
            // destination, so dialect policy cannot be bypassed by a redirect.
            self.check_request_targets(&request, operation)
                .map_err(|error| {
                    if hops > 0 {
                        refused_after_answered_hop(error)
                    } else {
                        error
                    }
                })?;
            let response = self.dispatch_once(&request, operation).await?;
            let redirect = matches!(
                response.status,
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            );
            let location = match response.headers.get(reqwest::header::LOCATION) {
                Some(value) if redirect => Some(value.to_str().map_err(|error| {
                    self.redirect_error(
                        NetError::MalformedRedirect {
                            kind: MalformedRedirectKind::InvalidLocationEncoding,
                            message: format!("Location header was not valid UTF-8: {error}"),
                        },
                        operation,
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
                    self.redirect_error(
                        NetError::MalformedRedirect {
                            kind: MalformedRedirectKind::UnresolvableLocation,
                            message: format!(
                                "Location {location:?} could not be resolved against base: {error}"
                            ),
                        },
                        operation,
                    )
                })?;
            hops += 1;
            if hops > max_hops {
                return Err(self.redirect_error(
                    NetError::RedirectLoop {
                        hops: u16::try_from(hops).unwrap_or(u16::MAX),
                    },
                    operation,
                ));
            }
            // Refused locally when the origin was never admitted by discovery.
            // The gate is checked here rather than left to `auth_headers`,
            // whose own refusal is right for a first hop but not for this one:
            // by now the server has answered the previous hop, and the error
            // must say so (see `refused_after_answered_hop`).
            if !self.is_trusted_url(next.as_str()) {
                return Err(refused_after_answered_hop(local_error(
                    operation,
                    format!("refusing to follow a DAV redirect to an untrusted URL: {next}"),
                    self.protocol,
                )));
            }
            // Fresh credentials for the target origin.
            let auth = self.auth_headers(next.as_str(), operation).await?;
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
                    return Err(refused_after_answered_hop(local_error(
                        operation,
                        format!(
                            "refusing to name an untrusted DAV move destination after a redirect: {rebased}"
                        ),
                        self.protocol,
                    )));
                }
                let value = HeaderValue::from_str(&rebased).map_err(|error| {
                    refused_after_answered_hop(local_error(
                        operation,
                        error.to_string(),
                        self.protocol,
                    ))
                })?;
                headers.insert(HeaderName::from_static(DESTINATION), value);
            }
            request = DavRequest {
                method: request.method,
                url: next.to_string(),
                headers,
                body: request.body,
                idempotent: request.idempotent,
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
            .headers(self.auth_headers(url, operation).await?)
            // A read: replaying after a dropped connection cannot change state.
            .idempotent(true)
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
            .headers(self.auth_headers(url, operation).await?)
            .idempotent(true)
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
        let request =
            DavRequest::new(Method::DELETE, url).headers(self.auth_headers(url, operation).await?);
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() || delete_target_already_gone(response.status) {
            Ok(())
        } else {
            Err(status_error(
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
            .headers(self.auth_headers(from, operation).await?)
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
            .idempotent(false);
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
            operation,
            response.status,
            response.body,
            self.protocol,
        ))
    }
}

/// Stamp a local refusal raised partway through the redirect walk with the
/// transmission evidence it is owed.
///
/// The classification stays this client's decision: `Request(Malformed)` for
/// origin refusal (which discovery's fallback predicate reads), or Unsupported
/// for a dialect policy refusal. The previous hop was sent and answered, so
/// this is not an UNSENT exchange. `Acknowledged` records that evidence without
/// changing either refusal's recovery class. The refused next hop is not sent.
fn refused_after_answered_hop(error: AccountError) -> AccountError {
    error
        .into_builder()
        .push_cause(bifrost_types::Cause::Attempt(
            bifrost_types::AttemptCause::new(bifrost_types::TransmissionState::Acknowledged),
        ))
        .try_build()
        .expect("an acknowledged attempt is valid on a local refusal")
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
    use bifrost_net::AccessToken;
    use bifrost_net::test_support::Canned;
    use bifrost_types::{
        AccountErrorKind, AuthErrorKind, ProtocolErrorKind, ReconcileAction, RecoveryClass,
        TransmissionState,
    };
    use reqwest::header::HeaderMap;

    use super::*;
    use crate::test_support::{
        DavTranscript, dav_dropped_after_send, dav_redirect, dav_script, dav_script_empty,
        scripted_dav_net, scripted_dav_net_capped, transcripts,
    };

    const BASE: &str = "https://dav.example.test";

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
    /// through `bifrost_net::into_account_error`, and only the remainder keeps
    /// the `Unsent` transport classification.
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
