//! OAuth bearer-token plumbing with single-flight refresh.
//!
//! The token source is a trait so different OAuth flows (refresh
//! token, device code, service-account JWT) can share one bearer
//! pipeline. `OAuthRefresher` coordinates concurrent refresh
//! attempts behind one `tokio::sync::Mutex` so N in-flight requests
//! do not trigger N independent refreshes.

use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bifrost_types::{AccountFuture, RetryHint};
use reqwest::header::RETRY_AFTER;
use tokio::sync::{Mutex, oneshot};
use zeroize::Zeroizing;

use crate::error::{Error, FinalResponse};
use crate::request::parse_retry_after_hint;

/// Default age at which a token without an `expires_at` is treated as
/// stale enough to refresh. Typical OAuth access tokens have a 60-min
/// TTL; refreshing at 55 min leaves a 5-min safety margin without
/// burning the issuer's quota on every request. Wired through
/// `OAuthRefresher::with_max_age` so callers can lengthen or shorten
/// it.
pub const DEFAULT_TOKEN_MAX_AGE: Duration = Duration::from_secs(55 * 60);
const PROACTIVE_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(30);
/// First quiet interval after a refresh failure that left no usable
/// cached token. Short on purpose: the common case is a blip, and the
/// engine's recovery latency is bounded by how soon the next attempt
/// may run.
const FAILED_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(1);
/// Ceiling for the doubling applied to consecutive transient failures.
/// An outage therefore costs at most one issuer call per minute per
/// account instead of one per second for its whole duration.
const MAX_FAILED_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(60);
/// Quiet interval after a failure the issuer answered authoritatively:
/// a 401/403 from the token endpoint, or an `AuthLost` the source
/// raised itself. Retrying at the transient cadence cannot help - the
/// credential needs human repair - and can get the account throttled
/// or blocked at the IdP, so these skip the escalation and start at
/// the ceiling.
const TERMINAL_REFRESH_RETRY_DELAY: Duration = MAX_FAILED_REFRESH_RETRY_DELAY;

/// Quiet interval to impose after a refresh failure.
///
/// `consecutive` counts this failure, so the first is 1. Transient
/// failures double from `FAILED_REFRESH_RETRY_DELAY` and clamp at
/// `MAX_FAILED_REFRESH_RETRY_DELAY`; any success resets the count.
fn failed_refresh_delay(consecutive: u32, terminal: bool) -> Duration {
    if terminal {
        return TERMINAL_REFRESH_RETRY_DELAY;
    }
    let shift = consecutive.saturating_sub(1).min(u32::BITS - 1);
    FAILED_REFRESH_RETRY_DELAY
        .saturating_mul(1_u32.checked_shl(shift).unwrap_or(u32::MAX))
        .min(MAX_FAILED_REFRESH_RETRY_DELAY)
}

/// Result of inspecting a token cache without refreshing or waiting for a lock.
#[derive(Debug)]
#[non_exhaustive]
pub enum TokenSnapshot {
    /// Cached token usable under this source's freshness policy.
    Usable(AccessToken),
    /// Async resolution is required, or this source has no snapshot support.
    RefreshRequired,
    /// The cache state is temporarily locked by another caller.
    Contended,
}

/// Source of OAuth bearer tokens. Implementations are responsible for
/// holding the refresh token (or whatever provider-specific material
/// is needed) and exchanging it for a fresh access token.
///
/// `current()` returns the cached token; `refresh()` forces a network
/// round-trip and returns the freshly minted token.
///
/// Traffic an implementation performs against the OAuth issuer is neither
/// METERED nor CAPPED by this crate, and that is a disclosed exclusion rather
/// than a hole. A `TokenSource` is shared by the HTTP, IMAP and SMTP paths and
/// owns arbitrary provider-specific exchange machinery, so bifrost-net cannot
/// count its wire bytes without replacing this abstraction with an
/// HTTP-specific request model - which changes a published consumer-implemented
/// contract. `reference/net.md` states the exclusion; do not re-file it as
/// missing coverage.
pub trait TokenSource: Send + Sync + 'static {
    /// Whether this source owns its token-cache semantics and must be kept as
    /// the shared source of truth. Attachments preserve coordinating sources;
    /// raw provider adapters receive an OAuth refresher.
    fn is_coordinating(&self) -> bool {
        false
    }

    /// Inspect the cache synchronously without starting refresh work, polling
    /// futures, or waiting for locks. Known source failures remain typed errors.
    /// The safe default requires async resolution: even a single poll of an
    /// arbitrary `current()` future may spawn work and require a runtime.
    fn snapshot(&self) -> Result<TokenSnapshot, Error> {
        Ok(TokenSnapshot::RefreshRequired)
    }

    /// Return the currently cached access token. Implementations must
    /// be cheap; a hot path through `RequestBuilder::send` may call
    /// this once per request.
    fn current(&self) -> AccountFuture<Result<AccessToken, Error>>;

    /// Force a network round-trip to mint a new access token. Called
    /// by `OAuthRefresher` under its single-flight lock or by the
    /// 401-retry path. Implementations must not coalesce internally;
    /// the refresher handles single-flighting.
    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>>;

    /// Refresh only if `used` is still the token this source exposes.
    ///
    /// The default preserves the behavior of simple token sources. A
    /// coordinating cache overrides this to collapse concurrent 401s:
    /// once one request has replaced the rejected token, later requests
    /// reuse that replacement instead of contacting the issuer again.
    fn refresh_if_unchanged(
        &self,
        _used: AccessToken,
    ) -> AccountFuture<Result<AccessToken, Error>> {
        self.refresh()
    }
}

/// An OAuth access token. The bytes are held in a `Zeroizing<String>`
/// so the buffer is wiped on drop. The `Debug` impl redacts the bytes;
/// only the expiry hint and a fingerprint length are surfaced.
pub struct AccessToken {
    /// Opaque bearer-token bytes. Wrapped in `Zeroizing` to scrub the
    /// allocation on drop. Access goes through `as_str`.
    secret: Zeroizing<String>,
    /// Absolute deadline at which the server is expected to reject
    /// the token. `None` for opaque tokens without a TTL hint, in
    /// which case the refresher falls back to the 401 path.
    expires_at: Option<Instant>,
}

/// Mutable in-memory token source for clients whose caller supplies
/// already-minted access tokens. `refresh()` returns the current token
/// because there is no refresh material inside bifrost-net for this
/// shape.
#[derive(Debug, Clone)]
pub struct StaticTokenSource {
    token: Arc<RwLock<AccessToken>>,
}

impl StaticTokenSource {
    /// Build a static source from raw bearer bytes and an optional
    /// expiry deadline.
    #[must_use]
    pub fn new(secret: impl Into<String>, expires_at: Option<Instant>) -> Self {
        Self::from_token(AccessToken::new(secret, expires_at))
    }

    /// Build a static source from an existing access-token wrapper.
    #[must_use]
    pub fn from_token(token: AccessToken) -> Self {
        Self {
            token: Arc::new(RwLock::new(token)),
        }
    }

    /// Replace the currently exposed token.
    pub fn set(&self, token: AccessToken) {
        *self.token.write().expect("static token lock poisoned") = token;
    }

    /// Snapshot the current token.
    pub fn token(&self) -> AccessToken {
        self.token
            .read()
            .expect("static token lock poisoned")
            .clone()
    }
}

impl TokenSource for StaticTokenSource {
    fn is_coordinating(&self) -> bool {
        // This RwLock is the live token cache. Wrapping it would snapshot a
        // token that an external auth service can later replace with `set`.
        true
    }

    fn snapshot(&self) -> Result<TokenSnapshot, Error> {
        match self.token.try_read() {
            Ok(token)
                if token_expiry(&token)
                    .is_some_and(|expiry| tokio::time::Instant::now() >= expiry) =>
            {
                Ok(TokenSnapshot::RefreshRequired)
            }
            Ok(token) => Ok(TokenSnapshot::Usable(token.clone())),
            Err(std::sync::TryLockError::WouldBlock) => Ok(TokenSnapshot::Contended),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(Error::Network {
                message: "static token lock poisoned".to_owned(),
                transmission_state: bifrost_types::TransmissionState::Unsent,
                source: None,
            }),
        }
    }

    fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
        let token = Arc::clone(&self.token);
        Box::pin(async move { Ok(token.read().expect("static token lock poisoned").clone()) })
    }

    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
        self.current()
    }
}

impl AccessToken {
    /// Build an `AccessToken` from raw bytes and an optional absolute
    /// expiry deadline.
    #[must_use]
    pub fn new(secret: impl Into<String>, expires_at: Option<Instant>) -> Self {
        Self {
            secret: Zeroizing::new(secret.into()),
            expires_at,
        }
    }

    /// Build an `AccessToken` by moving an existing zeroizing allocation.
    #[must_use]
    pub fn from_zeroizing(secret: Zeroizing<String>, expires_at: Option<Instant>) -> Self {
        Self { secret, expires_at }
    }

    /// Borrow the bearer token as a string slice. Callers must not
    /// log or persist this value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.secret
    }

    /// Absolute expiry deadline, if the token issuer supplied a TTL.
    #[must_use]
    pub fn expires_at(&self) -> Option<Instant> {
        self.expires_at
    }
}

impl Clone for AccessToken {
    fn clone(&self) -> Self {
        Self {
            secret: Zeroizing::new((*self.secret).clone()),
            expires_at: self.expires_at,
        }
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redact the bearer bytes. Surface only metadata that does
        // not enable replay if the log sink is compromised.
        f.debug_struct("AccessToken")
            .field("secret", &"<redacted>")
            .field("len", &self.secret.len())
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Single-flight access-token cache built on top of a `TokenSource`.
///
/// The state machine: `Fresh { token, refreshed_at }` is the steady
/// state; on either a proactive expiry check or a forced refresh, the
/// state transitions to `Refreshing { waiters }` and exactly one
/// spawned task drives the network call. Concurrent callers register a
/// oneshot receiver in `waiters` and await. The spawned driver is
/// deliberate: if the first caller is cancelled mid-refresh, the state
/// still resolves to `Fresh` or `Empty` instead of stranding future
/// callers behind a stale `Refreshing` entry.
pub struct OAuthRefresher {
    /// Underlying provider hooked up at construction.
    source: Arc<dyn TokenSource>,
    /// Shared state behind the single-flight lock.
    state: Arc<Mutex<RefreshState>>,
    /// Maximum age for a cached token without an `expires_at` hint.
    /// Past this age the refresher proactively refreshes instead of
    /// waiting for a server 401. Defaults to `DEFAULT_TOKEN_MAX_AGE`
    /// (55 min) and is tunable via `with_max_age`.
    max_age: Duration,
    /// Consecutive no-fallback refresh failures, shared by every handle
    /// onto this refresher. Drives the escalating quiet interval; reset
    /// to zero by any successful refresh so a recovered issuer is back
    /// to the one-second floor immediately.
    consecutive_failures: Arc<AtomicU32>,
}

impl OAuthRefresher {
    /// Build a refresher around the given token source. The initial
    /// state is `Empty` - no cached token, no in-flight refresh. The
    /// first `token()` call transitions to `Refreshing` and drives
    /// the network round-trip itself; concurrent callers join the
    /// `waiters` list. The prior draft initialized to
    /// `Refreshing { waiters: [] }` which left no way to distinguish
    /// "refresh in progress" from "first caller hasn't claimed the
    /// refresher role yet" - a naive `token()` impl would park
    /// callers as waiters forever.
    #[must_use]
    pub fn new(source: Arc<dyn TokenSource>) -> Self {
        Self {
            source,
            state: Arc::new(Mutex::new(RefreshState::Empty)),
            max_age: DEFAULT_TOKEN_MAX_AGE,
            consecutive_failures: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Override the proactive-refresh max-age for tokens without an
    /// `expires_at` hint. Defaults to `DEFAULT_TOKEN_MAX_AGE`.
    #[must_use]
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = max_age;
        self
    }

    /// Current proactive-refresh max-age for tokens without an
    /// `expires_at` hint.
    #[must_use]
    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    /// Return the current access token, refreshing if the cached copy
    /// is within the proactive-refresh window or absent.
    ///
    /// Single-flights concurrent refresh attempts. The first caller
    /// to find the state empty or stale transitions the state to
    /// `Refreshing`, registers itself as a waiter, and spawns the
    /// refresh driver. Concurrent callers register a oneshot receiver
    /// and await.
    pub async fn token(&self) -> Result<AccessToken, Error> {
        self.token_inner(false, None).await
    }

    /// Force a fresh token, bypassing the proactive-refresh window.
    /// Called by the 401-retry path. Itself single-flighted on the
    /// same lock: a forced refresh while another refresh is in
    /// flight registers as a waiter on the existing refresh rather
    /// than launching a second network round-trip.
    pub async fn force_refresh(&self) -> Result<AccessToken, Error> {
        self.token_inner(true, None).await
    }

    /// Refresh after a target 401 only while the rejected token is still
    /// current. If another request already replaced it, return that newer
    /// token without another issuer call.
    pub async fn force_refresh_if_unchanged(
        &self,
        used: &AccessToken,
    ) -> Result<AccessToken, Error> {
        self.token_inner(true, Some(used)).await
    }

    /// Shared engine driving both `token()` and `force_refresh()`.
    /// The only difference between the two flows is whether a `Fresh`
    /// token within its proactive-refresh window short-circuits;
    /// everything else is identical.
    async fn token_inner(
        &self,
        force: bool,
        rejected: Option<&AccessToken>,
    ) -> Result<AccessToken, Error> {
        // Lifetime block: snapshot the state, decide the path, drop
        // the guard before any `.await`.
        let role = {
            let mut state = self.state.lock().await;
            match &mut *state {
                RefreshState::Fresh {
                    token,
                    refreshed_at,
                    refresh_not_before,
                } if !force
                    && (!needs_refresh(token, *refreshed_at, self.max_age)
                        || refresh_is_deferred(token, *refresh_not_before)) =>
                {
                    // Steady state: cached token is fresh enough.
                    return Ok(token.clone());
                }
                RefreshState::Fresh {
                    token,
                    refreshed_at,
                    ..
                } => {
                    if rejected.is_some_and(|rejected| rejected.secret != token.secret) {
                        return Ok(token.clone());
                    }
                    // This caller is responsible for spawning the
                    // refresh driver. Register it as the first waiter
                    // before dropping the lock so cancellation of this
                    // future cannot leave the state stuck in
                    // `Refreshing`.
                    let (tx, rx) = oneshot::channel();
                    let fallback = if force {
                        None
                    } else {
                        Some((token.clone(), *refreshed_at))
                    };
                    *state = RefreshState::Refreshing {
                        waiters: vec![tx],
                        fallback,
                    };
                    DriverRole::Driver(rx)
                }
                RefreshState::Empty => {
                    let (tx, rx) = oneshot::channel();
                    *state = RefreshState::Refreshing {
                        waiters: vec![tx],
                        fallback: None,
                    };
                    DriverRole::Driver(rx)
                }
                RefreshState::Backoff { error, retry_at }
                    if tokio::time::Instant::now() < *retry_at =>
                {
                    return Err(arc_err_to_error(Arc::clone(error)));
                }
                RefreshState::Backoff { .. } => {
                    let (tx, rx) = oneshot::channel();
                    *state = RefreshState::Refreshing {
                        waiters: vec![tx],
                        fallback: None,
                    };
                    DriverRole::Driver(rx)
                }
                RefreshState::Refreshing { waiters, .. } => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    DriverRole::Waiter(rx)
                }
            }
        };

        match role {
            DriverRole::Driver(rx) => {
                let driver = self.clone_handle();
                tokio::spawn(async move {
                    let _ = driver.drive_refresh().await;
                });
                wait_for_refresh(rx).await
            }
            DriverRole::Waiter(rx) => wait_for_refresh(rx).await,
        }
    }

    /// Run the refresh future and fan the result out. Re-acquires
    /// the state lock once the network call completes to install the
    /// new `Fresh` entry and drain any waiters that joined while we
    /// were off the lock.
    async fn drive_refresh(&self) -> Result<AccessToken, Error> {
        let result = self.source.refresh().await;
        let mut state = self.state.lock().await;
        // We were the driver. Whatever state we left behind in
        // `Refreshing` must be replaced; pull the waiters out before
        // installing the new `Fresh` or rolling back to `Empty`.
        let (waiters, fallback) = match std::mem::replace(&mut *state, RefreshState::Empty) {
            RefreshState::Refreshing { waiters, fallback } => (waiters, fallback),
            // The only way this can happen is if a second driver
            // claimed the role concurrently, which the single-flight
            // protocol forbids. Defensive.
            _ => (Vec::new(), None),
        };

        match result {
            Ok(token) => {
                self.consecutive_failures.store(0, Ordering::Relaxed);
                *state = RefreshState::Fresh {
                    token: token.clone(),
                    refreshed_at: tokio::time::Instant::now(),
                    refresh_not_before: None,
                };
                drop(state);
                for waiter in waiters {
                    let _ = waiter.send(Ok(token.clone()));
                }
                Ok(token)
            }
            Err(err) => {
                let terminal = is_terminal_auth_error(&err);
                let can_fallback = !terminal
                    && fallback
                        .as_ref()
                        .and_then(|(token, _)| token_expiry(token))
                        .is_some_and(|expires_at| tokio::time::Instant::now() < expires_at);
                if can_fallback {
                    let (token, refreshed_at) = fallback.expect("fallback was checked as present");
                    *state = RefreshState::Fresh {
                        token: token.clone(),
                        refreshed_at,
                        refresh_not_before: Some(
                            tokio::time::Instant::now() + PROACTIVE_REFRESH_RETRY_DELAY,
                        ),
                    };
                    drop(state);
                    for waiter in waiters {
                        let _ = waiter.send(Ok(token.clone()));
                    }
                    return Ok(token);
                }
                let consecutive = self
                    .consecutive_failures
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                let shared = Arc::new(err);
                *state = RefreshState::Backoff {
                    error: Arc::clone(&shared),
                    retry_at: tokio::time::Instant::now()
                        + failed_refresh_delay(consecutive, terminal),
                };
                drop(state);
                for waiter in waiters {
                    let _ = waiter.send(Err(Arc::clone(&shared)));
                }
                Err(arc_err_to_error(shared))
            }
        }
    }

    /// Underlying token source. Exposed so call sites that already
    /// hold a refresher can fall through to the source for paths
    /// where single-flighting is not appropriate.
    #[must_use]
    pub fn source(&self) -> &Arc<dyn TokenSource> {
        &self.source
    }

    fn clone_handle(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            max_age: self.max_age,
            consecutive_failures: Arc::clone(&self.consecutive_failures),
        }
    }

    /// Internal state handle. Crate-public so `Net` and `RequestBuilder`
    /// can drive the state machine when Phase 2 wires the refresh
    /// path. Not yet wired - `token()` / `force_refresh()` are the
    /// only paths that read the state.
    #[allow(dead_code)]
    pub(crate) fn state(&self) -> &Mutex<RefreshState> {
        &self.state
    }
}

// `OAuthRefresher` is itself a `TokenSource` so call sites can
// freely substitute a refresher for a raw token source. `current()`
// returns the cached token (refreshing only if stale); `refresh()`
// forces a network round-trip.
//
// Attachments recognize this coordinator and preserve its cache and policy.
// Manually nesting refreshers still forces the inner proactive refresh through
// refresh(), bypassing its fresh-cache check and causing an issuer exchange.
impl TokenSource for OAuthRefresher {
    fn is_coordinating(&self) -> bool {
        true
    }

    fn snapshot(&self) -> Result<TokenSnapshot, Error> {
        // Never drive token_inner here: an empty or stale cache would spawn a
        // refresh task, panicking on a blocking caller without a runtime.
        let Ok(state) = self.state.try_lock() else {
            return Ok(TokenSnapshot::Contended);
        };
        match &*state {
            RefreshState::Fresh {
                token,
                refreshed_at,
                refresh_not_before,
            } if !needs_refresh(token, *refreshed_at, self.max_age)
                || refresh_is_deferred(token, *refresh_not_before) =>
            {
                Ok(TokenSnapshot::Usable(token.clone()))
            }
            RefreshState::Backoff { error, retry_at }
                if tokio::time::Instant::now() < *retry_at =>
            {
                Err(arc_err_to_error(Arc::clone(error)))
            }
            _ => Ok(TokenSnapshot::RefreshRequired),
        }
    }

    fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
        // Clone the source-bearing fields so the returned future is
        // `'static`. The trait return is `Pin<Box<...>>` for dyn
        // safety; we don't capture `&self`.
        let me = OAuthRefresher {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            max_age: self.max_age,
            consecutive_failures: Arc::clone(&self.consecutive_failures),
        };
        Box::pin(async move { me.token_inner(false, None).await })
    }

    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
        let me = OAuthRefresher {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            max_age: self.max_age,
            consecutive_failures: Arc::clone(&self.consecutive_failures),
        };
        Box::pin(async move { me.force_refresh().await })
    }

    fn refresh_if_unchanged(&self, used: AccessToken) -> AccountFuture<Result<AccessToken, Error>> {
        let me = self.clone_handle();
        Box::pin(async move { me.force_refresh_if_unchanged(&used).await })
    }
}

/// Outcome of the lock-held inspection inside `token_inner`. Captures
/// whether this caller is the refresh driver or one of the parked
/// waiters; carried out of the `Mutex` guard so we can `await` outside
/// the lock.
enum DriverRole {
    /// This caller transitioned the state to `Refreshing` and must
    /// spawn the refresh driver, then await the same waiter path as
    /// every other caller.
    Driver(oneshot::Receiver<Result<AccessToken, Arc<Error>>>),
    /// Another task is already driving the refresh; this caller is
    /// parked on a oneshot until the driver finishes.
    Waiter(oneshot::Receiver<Result<AccessToken, Arc<Error>>>),
}

async fn wait_for_refresh(
    rx: oneshot::Receiver<Result<AccessToken, Arc<Error>>>,
) -> Result<AccessToken, Error> {
    match rx.await {
        Ok(Ok(t)) => Ok(t),
        Ok(Err(e)) => Err(arc_err_to_error(e)),
        // The driver dropped its sender without answering: its task was
        // cancelled, it panicked, or the runtime is shutting down.
        //
        // This says nothing about the credential. Reporting `AuthLost`
        // here - which `account_error.rs::auth_lost` maps
        // unconditionally to `Authentication(ReauthorizationRequired)`
        // and thence to the terminal `RecoveryClass::AuthLost` - told
        // the engine the user must re-authorize because a task went
        // away. `RefreshFailed` carries it as
        // `Authentication(RefreshTransient)` into
        // `Retry(AfterAuthRefresh)`, which is what a lost driver
        // actually warrants: ask again.
        Err(_) => Err(Error::RefreshFailed {
            retry_after: None,
            source: Arc::new(Error::Cancelled),
        }),
    }
}

/// Has the cached token aged past the proactive-refresh threshold?
///
/// Two branches:
///
/// 1. Issuer supplied an `expires_at`: refresh 60 s before that
///    instant. Standard proactive-refresh window.
/// 2. No `expires_at` (opaque tokens without a TTL hint): refresh
///    when the cached token is older than `max_age`. The previous
///    behaviour treated such tokens as fresh indefinitely and waited
///    for a server 401, which leaked latency into every request that
///    happened to coincide with the server-side expiry.
fn needs_refresh(
    token: &AccessToken,
    refreshed_at: tokio::time::Instant,
    max_age: Duration,
) -> bool {
    let now = tokio::time::Instant::now();
    if let Some(expires_at) = token_expiry(token) {
        let window = Duration::from_secs(60);
        let deadline = expires_at.checked_sub(window).unwrap_or(expires_at);
        return now >= deadline;
    }
    // Opaque token: fall back to the configured max-age.
    now.saturating_duration_since(refreshed_at) >= max_age
}

fn refresh_is_deferred(token: &AccessToken, not_before: Option<tokio::time::Instant>) -> bool {
    let now = tokio::time::Instant::now();
    not_before.is_some_and(|not_before| now < not_before)
        && token_expiry(token).is_some_and(|expires_at| now < expires_at)
}

/// A token's issuer-supplied expiry on the tokio clock.
///
/// `AccessToken::expires_at` is published as a `std::time::Instant` and
/// stays that way - callers mint tokens from whatever clock they have.
/// Every comparison the refresher makes against it happens on the tokio
/// clock, so that `tokio::time::pause` / `advance` moves the
/// proactive-refresh and fallback windows in tests instead of leaving
/// them pinned to real time. The two clocks share a timeline
/// (`tokio::time::Instant` wraps a `std::time::Instant`), so the
/// conversion is exact and production behavior is unchanged.
fn token_expiry(token: &AccessToken) -> Option<tokio::time::Instant> {
    token.expires_at().map(tokio::time::Instant::from_std)
}

/// Convert an `Arc<Error>` (the wrapper that lets us fan one refresh
/// failure out to N waiters) back into a fresh owned `Error`. We
/// cannot move out of the `Arc` because waiters may still hold
/// references; instead we project by variant.
///
/// True auth failures (`AuthLost`) map straight through; the caller's
/// downstream classification stays the same. A `Status` from a token
/// endpoint with a 401 or 403 indicates the refresh token itself was
/// rejected and is also an auth-terminal condition.
///
/// Every other failure (`Network`, `Timeout`, `Tls`, etc.) is
/// transient from the consumer's point of view; collapsing them into
/// `AuthLost` would discard the retry-vs-give-up distinction the
/// engine needs. We preserve them inside
/// `RefreshFailed { retry_after, source }` so the caller can either
/// pattern-match on the inner variant for a retry decision or treat
/// the wrapper as a single "refresh failed" class.
fn arc_err_to_error(err: Arc<Error>) -> Error {
    if let Error::AuthLost { final_response, .. } = err.as_ref() {
        return Error::AuthLost {
            transmission_state: None,
            final_response: final_response.clone(),
        };
    }
    if let Error::Status {
        code,
        body,
        headers,
    } = err.as_ref()
        && (*code == reqwest::StatusCode::UNAUTHORIZED || *code == reqwest::StatusCode::FORBIDDEN)
    {
        return Error::AuthLost {
            transmission_state: None,
            final_response: Some(FinalResponse {
                status: *code,
                headers: headers.clone(),
                body: body.clone(),
            }),
        };
    }
    Error::RefreshFailed {
        retry_after: retry_after_deadline(err.as_ref()),
        source: err,
    }
}

fn is_terminal_auth_error(err: &Error) -> bool {
    matches!(err, Error::AuthLost { .. })
        || matches!(
            err,
            Error::Status { code, .. }
                if *code == reqwest::StatusCode::UNAUTHORIZED
                    || *code == reqwest::StatusCode::FORBIDDEN
        )
}

fn retry_after_deadline(err: &Error) -> Option<SystemTime> {
    let hint = match err {
        Error::AttemptHistory {
            source, attempts, ..
        } => {
            return retry_after_deadline(source)
                .or_else(|| attempts.iter().rev().find_map(retry_after_deadline));
        }
        Error::LocalHold { attempts, .. } => {
            return attempts.iter().rev().find_map(retry_after_deadline);
        }
        Error::RateLimited {
            retry_after,
            final_response,
        } => parse_retry_after_hint(final_response.headers.get(RETRY_AFTER))
            .or_else(|| retry_after.map(RetryHint::After)),
        Error::Status { code, headers, .. }
            if *code == reqwest::StatusCode::TOO_MANY_REQUESTS
                || *code == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
        {
            parse_retry_after_hint(headers.get(RETRY_AFTER))
        }
        Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            retry_after_history,
        } if final_response.status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || final_response.status == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
        {
            parse_retry_after_hint(final_response.headers.get(RETRY_AFTER))
                .or_else(|| retry_after_history.last().copied().map(RetryHint::After))
        }
        Error::RefreshFailed {
            retry_after,
            source,
        } => return (*retry_after).or_else(|| retry_after_deadline(source)),
        _ => None,
    }?;
    match hint {
        RetryHint::After(delay) => SystemTime::now().checked_add(delay),
        RetryHint::At(at) => Some(at),
        _ => None,
    }
}

/// Internal state of `OAuthRefresher`. Wrapped in a `Mutex` so the
/// `Empty -> Refreshing`, `Fresh -> Refreshing`, and
/// `Refreshing -> Fresh` transitions are atomic.
#[non_exhaustive]
pub enum RefreshState {
    /// Initial state: no cached token, no in-flight refresh. The
    /// first `token()` call transitions to `Refreshing` with the
    /// refreshing role implicitly claimed (`waiters` empty); it
    /// drives the network round-trip itself.
    Empty,
    /// A refresh failed without a usable fallback. Calls fail fast
    /// with the shared error until one short retry interval elapses,
    /// after which exactly one caller becomes the next driver.
    Backoff {
        /// Failure returned to callers during the quiet interval.
        error: Arc<Error>,
        /// Earliest instant at which another issuer call may start.
        retry_at: tokio::time::Instant,
    },
    /// Steady state. The cached token is valid and outside the
    /// proactive-refresh window.
    Fresh {
        /// Currently cached token.
        token: AccessToken,
        /// Instant at which the cached token was minted, on the tokio
        /// clock so a paused-time test can drive the max-age window the
        /// same way it drives `Backoff::retry_at`.
        refreshed_at: tokio::time::Instant,
        /// Earliest next proactive refresh attempt after a transient
        /// failure. Forced refreshes ignore this deadline. Tokio clock,
        /// as above.
        refresh_not_before: Option<tokio::time::Instant>,
    },
    /// A refresh is in flight. The task that transitioned the state
    /// to `Refreshing` is driving the network call; concurrent
    /// callers append a oneshot sender to `waiters` and await. On
    /// completion the driving task fans the result out to every
    /// waiter.
    ///
    /// Errors fan out via `Arc<Error>` because `crate::error::Error`
    /// is not `Clone` (it carries `Box<dyn std::error::Error>` and
    /// `Bytes` and a `HeaderMap`). Each waiter receives the same
    /// `Arc<Error>`; cloning the `Arc` is one refcount bump.
    Refreshing {
        /// Pending oneshot senders, one per waiting caller. The
        /// driving task is NOT in this list - it owns the refresh
        /// future directly.
        waiters: Vec<oneshot::Sender<Result<AccessToken, Arc<Error>>>>,
        /// Still-valid cached token displaced by a proactive refresh.
        /// A transient refresh failure restores this entry so the
        /// caller can use the remaining token lifetime. Forced
        /// refreshes never carry a fallback because a target 401 is
        /// evidence that the cached credential is unusable.
        fallback: Option<(AccessToken, tokio::time::Instant)>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use reqwest::header::{HeaderMap, HeaderValue, WWW_AUTHENTICATE};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[test]
    fn synchronous_refresher_snapshot_never_starts_work_without_a_runtime() {
        let refresher = OAuthRefresher::new(Arc::new(StaticTokenSource::new("issuer", None)));
        assert!(matches!(
            refresher.snapshot().unwrap(),
            TokenSnapshot::RefreshRequired
        ));
        let mut state = refresher.state.try_lock().unwrap();
        *state = RefreshState::Fresh {
            token: AccessToken::new("cached", None),
            refreshed_at: tokio::time::Instant::now(),
            refresh_not_before: None,
        };
        assert!(matches!(
            refresher.snapshot().unwrap(),
            TokenSnapshot::Contended
        ));
        drop(state);
        let TokenSnapshot::Usable(token) = refresher.snapshot().unwrap() else {
            panic!("fresh cache must resolve");
        };
        assert_eq!(token.as_str(), "cached");
        let mut state = refresher.state.try_lock().unwrap();
        *state = RefreshState::Fresh {
            token: AccessToken::new("expired", Some(Instant::now())),
            refreshed_at: tokio::time::Instant::now(),
            refresh_not_before: None,
        };
        drop(state);
        assert!(matches!(
            refresher.snapshot().unwrap(),
            TokenSnapshot::RefreshRequired
        ));
        assert!(matches!(
            *refresher.state.try_lock().unwrap(),
            RefreshState::Fresh { .. }
        ));
    }

    #[test]
    fn synchronous_snapshot_preserves_backoff_auth_loss() {
        let refresher = OAuthRefresher::new(Arc::new(StaticTokenSource::new("issuer", None)));
        *refresher.state.try_lock().unwrap() = RefreshState::Backoff {
            error: Arc::new(Error::AuthLost {
                transmission_state: None,
                final_response: None,
            }),
            retry_at: tokio::time::Instant::now() + Duration::from_secs(60),
        };
        assert!(matches!(refresher.snapshot(), Err(Error::AuthLost { .. })));
    }

    #[test]
    fn static_snapshot_distinguishes_contention_rotation_and_expiry() {
        let source = StaticTokenSource::new("first", None);
        let guard = source.token.write().unwrap();
        assert!(matches!(
            source.snapshot().unwrap(),
            TokenSnapshot::Contended
        ));
        drop(guard);
        source.set(AccessToken::new("second", None));
        let TokenSnapshot::Usable(token) = source.snapshot().unwrap() else {
            panic!("rotated token must resolve");
        };
        assert_eq!(token.as_str(), "second");
        source.set(AccessToken::new("expired", Some(Instant::now())));
        assert!(matches!(
            source.snapshot().unwrap(),
            TokenSnapshot::RefreshRequired
        ));
    }

    struct ControlledRefreshSource {
        calls: AtomicUsize,
        release_replacement: Arc<Notify>,
    }

    impl TokenSource for ControlledRefreshSource {
        fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.refresh()
        }

        fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let release = Arc::clone(&self.release_replacement);
            Box::pin(async move {
                if call == 2 {
                    release.notified().await;
                }
                Ok(AccessToken::new(format!("token-{call}"), None))
            })
        }
    }

    #[tokio::test]
    async fn rejected_token_refresh_is_single_flight_and_cancellation_safe() {
        let source = Arc::new(ControlledRefreshSource {
            calls: AtomicUsize::new(0),
            release_replacement: Arc::new(Notify::new()),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>);
        let rejected = refresher.token().await.expect("initial token");
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);

        let first = refresher.clone_handle();
        let first_rejected = rejected.clone();
        let driver =
            tokio::spawn(async move { first.force_refresh_if_unchanged(&first_rejected).await });
        tokio::task::yield_now().await;

        let second = refresher.clone_handle();
        let second_rejected = rejected.clone();
        let waiter =
            tokio::spawn(async move { second.force_refresh_if_unchanged(&second_rejected).await });
        tokio::task::yield_now().await;
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);

        driver.abort();
        source.release_replacement.notify_one();
        let replacement = waiter
            .await
            .expect("waiter task")
            .expect("shared refresh succeeds");
        assert_eq!(replacement.as_str(), "token-2");

        let reused = refresher
            .force_refresh_if_unchanged(&rejected)
            .await
            .expect("stale 401 reuses replacement");
        assert_eq!(reused.as_str(), "token-2");
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            2,
            "a stale rejected token must not start another issuer call"
        );
    }

    struct FailingRefreshSource {
        calls: AtomicUsize,
    }

    struct RecoveringRefreshSource {
        calls: AtomicUsize,
        healthy: AtomicBool,
    }

    impl TokenSource for RecoveringRefreshSource {
        fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.refresh()
        }

        fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let healthy = self.healthy.load(Ordering::SeqCst);
            Box::pin(async move {
                if healthy {
                    Ok(AccessToken::new("recovered", None))
                } else {
                    Err(Error::Network {
                        message: "issuer unavailable".to_owned(),
                        transmission_state: bifrost_types::TransmissionState::Unsent,
                        source: None,
                    })
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn no_fallback_failure_is_throttled_and_retries_at_the_short_boundary() {
        let source = Arc::new(RecoveringRefreshSource {
            calls: AtomicUsize::new(0),
            healthy: AtomicBool::new(false),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>);

        assert!(refresher.token().await.is_err());
        assert!(refresher.token().await.is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);

        source.healthy.store(true, Ordering::SeqCst);
        tokio::time::advance(FAILED_REFRESH_RETRY_DELAY - Duration::from_millis(1)).await;
        assert!(refresher.token().await.is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(refresher.token().await.unwrap().as_str(), "recovered");
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    }

    struct TerminalRefreshSource {
        calls: AtomicUsize,
    }

    impl TokenSource for TerminalRefreshSource {
        fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.refresh()
        }

        fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(Error::Status {
                    code: reqwest::StatusCode::UNAUTHORIZED,
                    body: Bytes::from_static(br#"{"error":"invalid_grant"}"#),
                    headers: HeaderMap::new(),
                })
            })
        }
    }

    /// Boundary table for the quiet interval. The first failure keeps
    /// the one-second floor so a blip costs almost no recovery latency;
    /// consecutive failures double so a long outage cannot be answered
    /// with one issuer call per second forever; the clamp is exact at
    /// the transition, not merely "eventually 60s".
    #[test]
    fn the_quiet_interval_escalates_from_one_second_and_clamps_at_a_minute() {
        assert_eq!(failed_refresh_delay(1, false), Duration::from_secs(1));
        assert_eq!(failed_refresh_delay(2, false), Duration::from_secs(2));
        assert_eq!(failed_refresh_delay(6, false), Duration::from_secs(32));
        assert_eq!(failed_refresh_delay(7, false), Duration::from_secs(60));
        assert_eq!(failed_refresh_delay(8, false), Duration::from_secs(60));
        // No overflow panic at the far end of the shift domain.
        assert_eq!(
            failed_refresh_delay(u32::MAX, false),
            Duration::from_secs(60)
        );
        // A refusal the issuer is authoritative about never gets the
        // one-second treatment, not even on its first occurrence.
        assert_eq!(failed_refresh_delay(1, true), Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn consecutive_transient_failures_escalate_and_a_success_resets_the_floor() {
        let source = Arc::new(RecoveringRefreshSource {
            calls: AtomicUsize::new(0),
            healthy: AtomicBool::new(false),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>);

        // First failure: one second.
        assert!(refresher.token().await.is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(refresher.token().await.is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);

        // Second failure: two seconds, so one second is not enough.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(refresher.token().await.is_err());
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            2,
            "the second failure must not retry at the one-second floor"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        source.healthy.store(true, Ordering::SeqCst);
        assert_eq!(refresher.token().await.unwrap().as_str(), "recovered");
        assert_eq!(source.calls.load(Ordering::SeqCst), 3);

        // The success reset the count, so the next failure is back to
        // one second rather than resuming the escalation.
        source.healthy.store(false, Ordering::SeqCst);
        assert!(refresher.force_refresh().await.is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 4);
        tokio::time::advance(Duration::from_secs(1)).await;
        source.healthy.store(true, Ordering::SeqCst);
        assert_eq!(refresher.token().await.unwrap().as_str(), "recovered");
        assert_eq!(source.calls.load(Ordering::SeqCst), 5);
    }

    /// A token endpoint answering 401 `invalid_grant` is refusing
    /// authoritatively. Calling it once a second for the life of the
    /// outage helps nobody and is how an account gets throttled or
    /// blocked at the IdP.
    #[tokio::test(start_paused = true)]
    async fn a_hard_issuer_refusal_does_not_retry_at_the_transient_cadence() {
        let source = Arc::new(TerminalRefreshSource {
            calls: AtomicUsize::new(0),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>);

        assert!(refresher.token().await.is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(TERMINAL_REFRESH_RETRY_DELAY - Duration::from_millis(1)).await;
        assert!(refresher.token().await.is_err());
        assert!(refresher.force_refresh().await.is_err());
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            1,
            "a hard refusal must not be re-asked before the long interval"
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(refresher.token().await.is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    }

    impl TokenSource for FailingRefreshSource {
        fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.refresh()
        }

        fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(Error::Network {
                    message: "transient refresh failure".to_string(),
                    transmission_state: bifrost_types::TransmissionState::Unsent,
                    source: None,
                })
            })
        }
    }

    /// A waiter whose driver went away - cancelled task, panic, runtime
    /// shutdown - learns nothing about the credential. Reporting
    /// `AuthLost` turned that into
    /// `Authentication(ReauthorizationRequired)` and a terminal
    /// `RecoveryClass::AuthLost`, so a shutdown race told the engine the
    /// user had to re-authorize.
    #[tokio::test]
    async fn a_dropped_refresh_driver_is_transient_not_terminal_auth_loss() {
        let (tx, rx) = oneshot::channel::<Result<AccessToken, Arc<Error>>>();
        drop(tx);

        let Err(error) = wait_for_refresh(rx).await else {
            panic!("a driver that never answered is a failure");
        };

        match error {
            Error::RefreshFailed { source, .. } => {
                assert!(
                    matches!(*source, Error::Cancelled),
                    "the preserved source names why the driver went away"
                );
            }
            other => panic!("expected a transient RefreshFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn proactive_refresh_failure_restores_a_still_valid_token() {
        let source = Arc::new(FailingRefreshSource {
            calls: AtomicUsize::new(0),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>);
        let token = AccessToken::new(
            "still-valid",
            Some(Instant::now() + Duration::from_secs(30)),
        );
        *refresher.state.lock().await = RefreshState::Fresh {
            token,
            refreshed_at: tokio::time::Instant::now(),
            refresh_not_before: None,
        };

        let returned = refresher
            .token()
            .await
            .expect("transient proactive refresh failure should use cached token");

        assert_eq!(returned.as_str(), "still-valid");
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            refresher
                .token()
                .await
                .expect("retry suppression reuses the fallback token")
                .as_str(),
            "still-valid"
        );
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            1,
            "the next request must not immediately retry the failed refresh"
        );
        let state = refresher.state.lock().await;
        let RefreshState::Fresh { token, .. } = &*state else {
            panic!("still-valid token was not restored");
        };
        assert_eq!(token.as_str(), "still-valid");
    }

    /// The proactive-refresh window is on the tokio clock, so paused
    /// time drives it. With the refresher's instants on
    /// `std::time::Instant` this test could not exist: `advance()` moves
    /// only the tokio clock, real time barely moves inside a test, and
    /// the 60 s pre-expiry window would never open.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn paused_time_drives_the_proactive_refresh_window() {
        let source = Arc::new(RecoveringRefreshSource {
            calls: AtomicUsize::new(0),
            healthy: AtomicBool::new(true),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>);
        // Expires in 10 minutes; the window opens 60 s before that.
        let token = AccessToken::new(
            "minted",
            Some((tokio::time::Instant::now() + Duration::from_secs(600)).into_std()),
        );
        *refresher.state.lock().await = RefreshState::Fresh {
            token,
            refreshed_at: tokio::time::Instant::now(),
            refresh_not_before: None,
        };

        assert_eq!(
            refresher.token().await.expect("cached token").as_str(),
            "minted",
            "well outside the window, the cached token is served"
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);

        // One second short of the window: still cached.
        tokio::time::advance(Duration::from_secs(539)).await;
        assert_eq!(
            refresher.token().await.expect("cached token").as_str(),
            "minted"
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);

        // Crossing into the last 60 s refreshes proactively.
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            refresher.token().await.expect("refreshed token").as_str(),
            "recovered",
            "inside the 60 s window the refresher mints a replacement"
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    }

    /// The max-age branch (opaque tokens with no issuer TTL) is on the
    /// same clock and is likewise drivable.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn paused_time_drives_the_max_age_window_for_opaque_tokens() {
        let source = Arc::new(RecoveringRefreshSource {
            calls: AtomicUsize::new(0),
            healthy: AtomicBool::new(true),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>)
            .with_max_age(Duration::from_secs(300));
        *refresher.state.lock().await = RefreshState::Fresh {
            token: AccessToken::new("opaque", None),
            refreshed_at: tokio::time::Instant::now(),
            refresh_not_before: None,
        };

        tokio::time::advance(Duration::from_secs(299)).await;
        assert_eq!(
            refresher.token().await.expect("cached token").as_str(),
            "opaque"
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            refresher.token().await.expect("refreshed token").as_str(),
            "recovered"
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retry_after_deadline_parses_status_header() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("30"));
        let before = SystemTime::now();
        let deadline = retry_after_deadline(&Error::Status {
            code: reqwest::StatusCode::TOO_MANY_REQUESTS,
            body: Bytes::new(),
            headers,
        })
        .expect("retry-after deadline should parse");
        let after = SystemTime::now();

        let lower = before
            .checked_add(Duration::from_secs(30))
            .expect("test lower bound in range");
        let upper = after
            .checked_add(Duration::from_secs(30))
            .expect("test upper bound in range");
        assert!(deadline >= lower);
        assert!(deadline <= upper);
    }

    #[test]
    fn retry_after_deadline_preserves_an_elapsed_http_date() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_234);
        let date = httpdate::fmt_http_date(at);
        let mut headers = HeaderMap::new();
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_str(&date).expect("formatted HTTP-date is a valid header"),
        );

        assert_eq!(
            retry_after_deadline(&Error::Status {
                code: reqwest::StatusCode::TOO_MANY_REQUESTS,
                body: Bytes::new(),
                headers,
            }),
            Some(at),
            "an elapsed date is still a retry deadline, not an absent hint"
        );
    }

    #[test]
    fn retry_after_deadline_finds_a_hint_in_prior_attempt_history() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_234);
        let date = httpdate::fmt_http_date(at);
        let mut headers = HeaderMap::new();
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_str(&date).expect("formatted HTTP-date is a valid header"),
        );

        assert_eq!(
            retry_after_deadline(&Error::AttemptHistory {
                source: Box::new(Error::Timeout {
                    transmission_state: bifrost_types::TransmissionState::Unsent,
                }),
                attempts: vec![Error::Status {
                    code: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    body: Bytes::new(),
                    headers,
                }],
                may_have_committed: false,
            }),
            Some(at),
            "a later local failure must retain the last response's retry date"
        );
    }

    #[test]
    fn token_endpoint_unauthorized_preserves_response_evidence() {
        let mut headers = HeaderMap::new();
        headers.insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static(r#"Bearer error="invalid_grant""#),
        );
        let projected = arc_err_to_error(Arc::new(Error::Status {
            code: reqwest::StatusCode::UNAUTHORIZED,
            body: Bytes::from_static(br#"{"error":"invalid_grant"}"#),
            headers,
        }));

        let Error::AuthLost {
            transmission_state,
            final_response: Some(final_response),
        } = projected
        else {
            panic!("expected auth-lost with final response");
        };
        assert_eq!(transmission_state, None);
        assert_eq!(final_response.status, reqwest::StatusCode::UNAUTHORIZED);
        assert!(final_response.headers.contains_key(WWW_AUTHENTICATE));
        assert_eq!(
            final_response.body,
            Bytes::from_static(br#"{"error":"invalid_grant"}"#)
        );
    }
}
