use std::sync::{Arc, OnceLock};

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, RateLimit, RequestBuilder, Response,
    StaticTokenSource, TokenSource,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) use crate::gmail::endpoint::GmailEndpoint;
use crate::{Error, Result};

/// Inbound payload bytes attributed to one engine batch.
///
/// An engine batch routinely covers several concurrent HTTP requests -
/// a list page plus a fan-out of hydration GETs - so per-response
/// totals have to be summed somewhere. They are summed HERE, on an
/// accumulator owned by one stream, rather than sampled off the
/// account-cumulative bandwidth meter: that meter is shared by every
/// concurrent request on the account, so a delta across it would
/// attribute another scope's traffic to this batch.
///
/// `take` reads and resets, so consecutive batches from one stream
/// partition the bytes rather than each reporting a running total.
#[derive(Clone, Default)]
pub(crate) struct ByteTally(Arc<AtomicU64>);

impl ByteTally {
    fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Bytes recorded since the previous `take`.
    pub(crate) fn take(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}

pub(crate) const GMAIL_API_BASE: &str = "https://www.googleapis.com/gmail/v1/users/me";
// Local data-request policy, equal to inventory's HYDRATE_BATCH_SIZE, not a
// discovered Gmail ceiling. Gmail's unpublished per-user concurrency limit is
// shared with other clients, so this cannot guarantee staying below it.
// It prevents concurrent walks and scopes from multiplying the per-walk width.
pub(crate) const GMAIL_DATA_CONCURRENCY: usize = 32;
const PEOPLE_API_BASE: &str = "https://people.googleapis.com/v1";
const CALENDAR_API_BASE: &str = "https://www.googleapis.com/calendar/v3";
// Gmail meters per-user traffic in QUOTA UNITS, not requests: the
// published per-user limit is 250 units/second, and each method has its
// own unit cost (see `GmailEndpoint::quota_cost`). We register a deliberately
// conservative 100 units/second so a full backfill leaves headroom for
// the interactive traffic sharing the same per-user budget, rather than
// spending the whole allowance on hydration and driving the 429 backoff
// that a flat one-unit-per-request model produced here.
const GOOGLE_API_QUOTA_PER_SECOND: f64 = 100.0;
pub(crate) const GOOGLE_API_BURST: u32 = 100;
const PEOPLE_API_QUOTA_PER_SECOND: f64 = 1.5;
const PEOPLE_API_BURST: u32 = 30;

#[derive(Clone)]
pub(crate) struct GmailApi {
    inner: Arc<ClientInner>,
    /// Where this handle reports request-local inbound bytes, if
    /// anywhere. `None` on every ordinary client; a stream that needs
    /// per-batch accounting takes a metered handle via `metered()`.
    /// Keeping it OUTSIDE `ClientInner` is the point: a metered handle
    /// is one `Arc` bump over the same connection state, so installing
    /// accounting never forks the client's configuration.
    tally: Option<ByteTally>,
    quota_project: Option<bifrost_types::ThrottleBudget>,
    wall_clock: Arc<dyn Fn() -> std::time::SystemTime + Send + Sync>,
}

struct ClientInner {
    gmail_limit: OnceLock<bifrost_net::ConcurrencyLimit>,
    net: Option<AccountNet>,
    parent_net: Net,
    api_base: String,
    // People/contacts API base. A third, independent Google surface:
    // Gmail mail lives on www.googleapis.com and Calendar on
    // www.googleapis.com/calendar, but People contacts + directory live
    // on people.googleapis.com, so this base is threaded separately from
    // `api_base` (the Gmail mail base) rather than reusing it. Defaults to
    // the production People base; a harness redirects it independently.
    people_base: String,
    // Calendar API base, threaded exactly like `people_base`. Calendar lives
    // under www.googleapis.com/calendar/v3 in production - the same HOST as
    // the Gmail mail base but a different path root - so it cannot be derived
    // from `api_base` and gets its own field.
    //
    // Resolved ONCE, at client construction (`default_calendar_base`), not per
    // call. It used to be a `std::env::var` read on every single request: a
    // per-request `getenv` on a hot path, process-global state read from
    // inside a library, and no way to point two accounts at two endpoints in
    // one process. The environment variable still works as a fallback for
    // existing harnesses - see `default_calendar_base` - but an explicit
    // `GoogleAccountFactory::with_calendar_api_base` now takes precedence over
    // it and is the supported mechanism.
    calendar_base: String,
    drive_base: String,
    token_source: Arc<dyn TokenSource>,
}

/// Production Calendar base, or the legacy environment override.
///
/// LEGACY, kept working deliberately: `RATATOSKR_TEST_GCAL_ENDPOINT` is read by
/// existing downstream harnesses, and dropping it would not fail their builds -
/// it would silently stop redirecting and send their test traffic to the real
/// Google Calendar API. So it stays until those consumers have migrated to
/// `GoogleAccountFactory::with_calendar_api_base`, which overrides it.
///
/// It is read once per client construction rather than per request. A harness
/// that sets the variable after building a client no longer affects that
/// client; harnesses set process environment before startup, so this is the
/// intended trade for taking the read off the request path.
///
/// A bifrost crate should not be naming its downstream consumer in an
/// identifier at all, which is the other reason this is the legacy path and not
/// the supported one.
fn default_calendar_base() -> String {
    std::env::var("RATATOSKR_TEST_GCAL_ENDPOINT").map_or_else(
        |_| CALENDAR_API_BASE.to_string(),
        |endpoint| format!("{}/calendar/v3", endpoint.trim_end_matches('/')),
    )
}

/// Host component of a base URL, for rate-limit registration.
///
/// Falls back to the production host when the base does not parse, so a
/// malformed override degrades to metering the real host rather than silently
/// registering no limit at all.
fn host_of(base: &str, fallback: &str) -> String {
    reqwest::Url::parse(base)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
        .unwrap_or_else(|| fallback.to_string())
}

impl GmailApi {
    #[cfg(test)]
    pub(crate) fn with_fixed_clock(&self, now: std::time::SystemTime) -> Self {
        let mut client = self.clone();
        client.wall_clock = Arc::new(move || now);
        client
    }

    #[cfg(test)]
    pub(crate) fn with_test_clock(
        &self,
        clock: Arc<dyn Fn() -> std::time::SystemTime + Send + Sync>,
    ) -> Self {
        let mut client = self.clone();
        client.wall_clock = clock;
        client
    }

    pub(crate) fn with_quota_project(&self, id: String) -> Self {
        let mut client = self.clone();
        client.quota_project = Some(crate::quota::project_budget(&id));
        client
    }

    pub(crate) fn wall_now(&self) -> std::time::SystemTime {
        (self.wall_clock)()
    }

    pub(crate) fn quota_project(&self) -> Option<&bifrost_types::ThrottleBudget> {
        self.quota_project.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn with_account_net(api_base: impl Into<String>, net: AccountNet) -> Self {
        let token_source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
        Self {
            tally: None,
            quota_project: None,
            wall_clock: Arc::new(std::time::SystemTime::now),
            inner: Arc::new(ClientInner {
                gmail_limit: OnceLock::new(),
                net: Some(net),
                parent_net: Net::shared_default(),
                api_base: api_base.into().trim_end_matches('/').to_string(),
                people_base: PEOPLE_API_BASE.to_string(),
                calendar_base: default_calendar_base(),
                drive_base: "https://www.googleapis.com".to_string(),
                token_source,
            }),
        }
    }

    pub(crate) fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_base(GMAIL_API_BASE, access_token)
    }

    // pub(crate): the factory's `from_access_token_with_api_base` test seam
    // builds a bearer-token client against a redirected Gmail base.
    pub(crate) fn with_api_base(
        api_base: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
        let token_source: Arc<dyn TokenSource> =
            Arc::new(StaticTokenSource::new(access_token, None));
        Self::with_api_base_and_source(api_base, token_source)
    }

    // Source-accepting constructor. ratatoskr hands in a shared
    // `Arc<dyn TokenSource>` so a refreshed-and-persisted token is read
    // live at every wire authentication without reopening the client.
    pub(crate) fn with_source(source: Arc<dyn TokenSource>) -> Self {
        Self::with_api_base_and_source(GMAIL_API_BASE, source)
    }

    // pub(crate): the factory's `from_token_source_with_api_base` test seam
    // routes a refresher-backed client at a redirected Gmail base, mirroring
    // bifrost-graph's `with_source`.
    pub(crate) fn with_api_base_and_source(
        api_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        Self::with_api_base_source_and_net(api_base, token_source, Net::shared_default())
    }

    // The public factory can thread an application-owned Net through here.
    // Keep attachment deferred until for_account supplies the engine id.
    pub(crate) fn with_api_base_source_and_net(
        api_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
        parent_net: Net,
    ) -> Self {
        Self {
            tally: None,
            quota_project: None,
            wall_clock: Arc::new(std::time::SystemTime::now),
            inner: Arc::new(ClientInner {
                gmail_limit: OnceLock::new(),
                net: None,
                parent_net,
                api_base: api_base.into().trim_end_matches('/').to_string(),
                people_base: PEOPLE_API_BASE.to_string(),
                calendar_base: default_calendar_base(),
                drive_base: "https://www.googleapis.com".to_string(),
                token_source,
            }),
        }
    }

    // pub(crate): the factory's `with_people_api_base` test seam points the
    // People/contacts base at a mock endpoint instead of
    // people.googleapis.com, independently of the Gmail mail base. Returns a
    // fresh client sharing the same net/token source with only the People
    // base swapped.
    pub(crate) fn with_people_base(&self, people_base: impl Into<String>) -> Self {
        Self {
            tally: None,
            quota_project: self.quota_project.clone(),
            wall_clock: Arc::clone(&self.wall_clock),
            inner: Arc::new(ClientInner {
                gmail_limit: OnceLock::new(),
                net: self.inner.net.clone(),
                parent_net: self.inner.parent_net.clone(),
                api_base: self.inner.api_base.clone(),
                people_base: people_base.into().trim_end_matches('/').to_string(),
                calendar_base: self.inner.calendar_base.clone(),
                drive_base: self.inner.drive_base.clone(),
                token_source: Arc::clone(&self.inner.token_source),
            }),
        }
    }

    // pub(crate): the factory's `with_calendar_api_base` seam points the
    // Calendar base at a mock endpoint, independently of the Gmail mail and
    // People bases. Takes precedence over the legacy
    // `RATATOSKR_TEST_GCAL_ENDPOINT` environment variable, and unlike it works
    // per client, so two accounts in one process can use two endpoints.
    pub(crate) fn with_calendar_base(&self, calendar_base: impl Into<String>) -> Self {
        Self {
            tally: None,
            quota_project: self.quota_project.clone(),
            wall_clock: Arc::clone(&self.wall_clock),
            inner: Arc::new(ClientInner {
                gmail_limit: OnceLock::new(),
                net: self.inner.net.clone(),
                parent_net: self.inner.parent_net.clone(),
                api_base: self.inner.api_base.clone(),
                people_base: self.inner.people_base.clone(),
                calendar_base: calendar_base.into().trim_end_matches('/').to_string(),
                drive_base: self.inner.drive_base.clone(),
                token_source: Arc::clone(&self.inner.token_source),
            }),
        }
    }

    pub(crate) fn with_drive_base(&self, base: impl Into<String>) -> Self {
        let mut client = self.with_calendar_base(self.calendar_base());
        Arc::get_mut(&mut client.inner)
            .expect("new client configuration")
            .drive_base = base.into().trim_end_matches('/').to_string();
        client
    }

    pub(crate) fn drive_base(&self) -> &str {
        &self.inner.drive_base
    }

    pub(crate) fn calendar(&self) -> crate::calendar::CalendarApi<'_> {
        crate::calendar::CalendarApi(self)
    }

    pub(crate) fn people(&self) -> crate::people::PeopleApi<'_> {
        crate::people::PeopleApi(self)
    }

    pub(crate) fn drive(&self) -> crate::drive::DriveApi<'_> {
        crate::drive::DriveApi(self)
    }

    pub(crate) fn for_account(&self, account_id: AccountId) -> Self {
        // Rate limits are registered against the hosts this client will
        // actually talk to, derived from its configured bases. Registering
        // literal production hostnames left a redirected base completely
        // unmetered, which was already mildly wrong for People (whose base has
        // long been configurable) and would have become wrong for Calendar the
        // moment its override stopped being an environment variable.
        let net = default_account_net(
            &self.inner.parent_net,
            account_id,
            &self.inner.api_base,
            &self.inner.people_base,
            &self.inner.calendar_base,
            &self.inner.drive_base,
            Arc::clone(&self.inner.token_source),
        );
        Self {
            tally: None,
            quota_project: self.quota_project.clone(),
            wall_clock: Arc::clone(&self.wall_clock),
            inner: Arc::new(ClientInner {
                gmail_limit: OnceLock::new(),
                net: Some(net),
                parent_net: self.inner.parent_net.clone(),
                api_base: self.inner.api_base.clone(),
                people_base: self.inner.people_base.clone(),
                calendar_base: self.inner.calendar_base.clone(),
                drive_base: self.inner.drive_base.clone(),
                token_source: Arc::clone(&self.inner.token_source),
            }),
        }
    }

    pub(crate) fn account_net(&self) -> &AccountNet {
        self.inner
            .net
            .as_ref()
            .expect("GmailApi must be scoped with for_account before issuing requests")
    }

    pub(crate) fn detach_account(&self) {
        if let Some(account_net) = &self.inner.net {
            account_net.detach();
        }
    }

    pub(crate) fn api_base(&self) -> &str {
        &self.inner.api_base
    }

    pub(crate) fn people_base(&self) -> &str {
        &self.inner.people_base
    }

    pub(crate) fn calendar_base(&self) -> &str {
        &self.inner.calendar_base
    }

    #[cfg(test)]
    pub(crate) async fn access_token(&self) -> String {
        self.inner
            .token_source
            .current()
            .await
            .expect("static token source is infallible")
            .as_str()
            .to_string()
    }

    pub(crate) async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.api_url(path);
        self.request::<T, ()>(
            &url,
            reqwest::Method::GET,
            None,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, reqwest::Method::POST, Some(body), replay)
            .await
    }

    pub(crate) async fn put<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, reqwest::Method::PUT, Some(body), replay)
            .await
    }

    pub(crate) async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, reqwest::Method::PATCH, Some(body), replay)
            .await
    }

    pub(crate) async fn delete(&self, path: &str, replay: bifrost_net::ReplayPolicy) -> Result<()> {
        let url = self.api_url(path);
        self.delete_absolute(&url, crate::quota::ApiSurface::Gmail, replay)
            .await
    }

    async fn delete_absolute(
        &self,
        url: &str,
        service: crate::quota::ApiSurface,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<()> {
        let response = self
            .execute(url, reqwest::Method::DELETE, None::<&()>, replay)
            .await?;
        check_response_status(response, service).await
    }

    pub(crate) async fn post_no_content<B: Serialize>(
        &self,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<()> {
        let url = self.api_url(path);
        let response = self
            .execute(&url, reqwest::Method::POST, Some(body), replay)
            .await?;
        check_response_status(response, crate::quota::ApiSurface::Gmail).await
    }

    pub(crate) fn api_url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else if path.starts_with('/') {
            format!("{}{}", self.inner.api_base, path)
        } else {
            format!("{}/{}", self.inner.api_base, path)
        }
    }

    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T> {
        let response = self.execute(url, method, body, replay).await?;
        parse_json_response(response, crate::quota::ApiSurface::Gmail).await
    }

    pub(crate) async fn execute<B: Serialize>(
        &self,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<Response> {
        self.execute_if_match(url, method, body, None, replay).await
    }

    pub(crate) async fn execute_if_match<B: Serialize>(
        &self,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        expected: Option<&str>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<Response> {
        self.execute_on(
            crate::quota::ApiSurface::Gmail,
            url,
            method,
            body,
            expected,
            replay,
        )
        .await
    }

    pub(crate) async fn execute_on<B: Serialize>(
        &self,
        surface: crate::quota::ApiSurface,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        expected: Option<&str>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<Response> {
        // The shared transport has no AccountOperation, and
        // needs none: the caller knows the request and its payload and
        // declares its replay policy (applied below). POST covers reads,
        // actions and creates alike, so no verb-level default would do.
        let mut builder = self.account_net().request(method.clone(), url);
        if let Some(endpoint) = self
            .gmail_endpoint(url, &method)
            .filter(|_| surface == crate::quota::ApiSurface::Gmail)
        {
            builder = builder.cost(endpoint.quota_cost());
            if endpoint.uses_data_concurrency() {
                builder = builder.concurrency_limit(self.gmail_data_limit());
            }
        }

        // `json` sets `Content-Type: application/json` itself, so the
        // header is not set here. Setting it unconditionally announced a
        // JSON body on bodyless GET and DELETE requests, which is a
        // claim about a payload that does not exist.
        if let Some(b) = body {
            builder = builder.json(b);
        }

        if let Some(expected) = expected {
            builder = builder.header("If-Match", expected);
        }
        self.send_recorded(builder.replay(replay), surface).await
    }

    fn gmail_data_limit(&self) -> bifrost_net::ConcurrencyLimit {
        self.inner
            .gmail_limit
            .get_or_init(|| {
                let origin = reqwest::Url::parse(self.api_base()).map_or_else(
                    |_| self.api_base().to_string(),
                    |url| url.origin().ascii_serialization(),
                );
                let key = bifrost_net::LimitKey::namespaced(
                    "google.gmail",
                    &[&self.account_net().account().0, &origin],
                );
                self.account_net()
                    .shared_concurrency_limit(key, GMAIL_DATA_CONCURRENCY)
            })
            .clone()
    }

    /// Quota units this URL costs, when it resolves against the Gmail
    /// base. `None` for anything else (Calendar, Drive, People), which
    /// keeps that surface's `cost_default`. Raw Gmail builders must apply
    /// this cost explicitly; production Gmail mutations use the typed funnel.
    #[cfg(test)]
    pub(crate) fn gmail_quota_cost(&self, url: &str, method: reqwest::Method) -> Option<u32> {
        if self.surface_for_url(url) != crate::quota::ApiSurface::Gmail {
            return None;
        }
        self.gmail_endpoint(url, &method)
            .map(GmailEndpoint::quota_cost)
    }

    fn gmail_endpoint(&self, url: &str, method: &reqwest::Method) -> Option<GmailEndpoint> {
        let path = url.strip_prefix(self.api_base())?;
        Some(GmailEndpoint::from_request(path, method))
    }

    pub(crate) async fn execute_builder(
        &self,
        builder: RequestBuilder,
        service: crate::quota::ApiSurface,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<Response> {
        // Net errors retain their original shape alongside observation metadata so
        // the account-side translation boundary can inspect transmission
        // state, retry-after, and other forensic evidence. Service
        // attribution also identifies the raw Gmail data mutation path.
        let builder = if service == crate::quota::ApiSurface::Gmail {
            builder.concurrency_limit(self.gmail_data_limit())
        } else {
            builder
        };
        let surface = service;
        self.send_recorded(builder.replay(replay), surface).await
    }

    fn surface_for_url(&self, url: &str) -> crate::quota::ApiSurface {
        use crate::quota::ApiSurface;
        // Redirected test bases can share an origin or nest under another
        // base. The most specific path root owns the request.
        //
        // A URL under no configured base is Drive, not a refusal. The
        // production instance is a server-issued Drive resumable-upload
        // session URL, which the Drive bucket on `www.googleapis.com`
        // meters. It goes unmetered only when the host has no registration
        // (redirected test bases, or a session URL on another host). That
        // is bounded by Google's own 429s, which net still honours. Refusing
        // it would fail an Outlook user's attachment upload to protect a
        // client-side budget the server enforces anyway.
        [
            (self.api_base(), ApiSurface::Gmail),
            (self.calendar_base(), ApiSurface::Calendar),
            (self.people_base(), ApiSurface::People),
            (self.drive_base(), ApiSurface::Drive),
        ]
        .into_iter()
        .filter(|(base, _)| {
            url.strip_prefix(base).is_some_and(|tail| {
                tail.is_empty() || tail.starts_with('/') || tail.starts_with('?')
            })
        })
        .max_by_key(|(base, _)| base.len())
        .map_or(ApiSurface::Drive, |(_, surface)| surface)
    }

    /// The single point every buffered Gmail/People/Calendar request
    /// leaves through, and therefore the single point request-local
    /// inbound bytes are recorded. Putting the record here rather than
    /// at each typed wrapper is what keeps `delete`, `post_no_content`
    /// and the caller-built `execute_builder` requests counted: those
    /// discard or hand-decode the body and would otherwise be free.
    ///
    /// Recorded from the request-local counter rather than from
    /// `Response`, so a FAILED request contributes too. `bifrost-net`
    /// drains, meters and throttles the bodies of non-2xx responses,
    /// exhausted retries and repeated 401s before converting them to an
    /// error, and the mutation lane turns such an error into per-item
    /// failures while still emitting a batch - so recording only on
    /// success would report zero for exactly the batches that spent the
    /// most quota. The Gmail `batchDelete` permission fallback is the
    /// sharpest case: its refused primary call is pure error-path
    /// traffic.
    async fn send_recorded(
        &self,
        mut builder: RequestBuilder,
        surface: crate::quota::ApiSurface,
    ) -> Result<Response> {
        // The observer and gate describe one API surface. Preserve that
        // attribution throughout net's redirect walk, including without a
        // configured project. Same-surface redirects still use net's policy.
        let redirect_client = self.clone();
        builder = builder
            .quota_scope(surface_quota_scope(self.account_net().account(), surface))
            .redirect_guard(move |_, target| {
                redirect_client.surface_for_url(target.as_str()) == surface
            });
        let project = self
            .quota_project
            .as_ref()
            .filter(|_| surface == crate::quota::ApiSurface::Gmail)
            .cloned();
        let gate = project.as_ref().map(|budget| {
            self.account_net()
                .shared_hold(crate::quota::project_hold_key(budget))
        });
        if let Some(gate) = gate.as_ref() {
            builder = builder.hold_gate(gate.key());
        }
        let observed = Arc::new(std::sync::Mutex::new(None));
        let sink = Arc::clone(&observed);
        let wall_clock = Arc::clone(&self.wall_clock);
        builder = builder.observe_errors(move |response| {
            let wall_now = wall_clock();
            let now = tokio::time::Instant::now();
            let mut headers =
                crate::error::GmailResponseHeaders::from_headers_at(&response.headers, wall_now);
            if let (Some(budget), Some(gate), Some(kind)) = (
                project.as_ref(),
                gate.as_ref(),
                crate::quota::project_kind(response.status.as_u16(), &response.body),
            ) {
                let deadline = crate::quota::resolve_deadline(kind, &headers, wall_now, now);
                gate.record(deadline, kind);
                headers.project = Some(crate::quota::ProjectObservation {
                    budget: budget.clone(),
                    deadline,
                });
            }
            *sink.lock().expect("response observation poisoned") = Some(headers);
        });
        let counter = bifrost_net::RequestByteCounter::new();
        let sent = builder.count_bytes_into(counter.clone()).send().await;
        if let Some(tally) = self.tally.as_ref() {
            tally.add(counter.bytes_in());
        }
        sent.map_err(|error| {
            let headers = observed
                .lock()
                .expect("response observation poisoned")
                .take()
                .unwrap_or_default();
            Error::ObservedNet(Box::new(crate::error::ObservedNetError {
                surface,
                error,
                headers,
            }))
        })
    }

    /// A handle over the same client that reports every buffered
    /// response's inbound bytes into a fresh accumulator.
    ///
    /// One accumulator per stream, not per client: the returned handle
    /// is the only one recording into it, so concurrent work on other
    /// scopes cannot contaminate the total.
    pub(crate) fn metered(&self) -> (Self, ByteTally) {
        let tally = ByteTally::default();
        (
            Self {
                inner: Arc::clone(&self.inner),
                tally: Some(tally.clone()),
                quota_project: self.quota_project.clone(),
                wall_clock: Arc::clone(&self.wall_clock),
            },
            tally,
        )
    }
}

pub(crate) async fn parse_json_response<T: DeserializeOwned>(
    response: Response,
    service: crate::quota::ApiSurface,
) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        let headers = crate::error::GmailResponseHeaders::from_headers(response.headers());
        return Err(Error::response_from_parts(
            service,
            status.as_u16(),
            headers,
            response.body,
        ));
    }
    serde_json::from_slice(response.body.as_ref())
        .map_err(|source| Error::JsonDecode { service, source })
}

pub(crate) async fn check_response_status(
    response: Response,
    service: crate::quota::ApiSurface,
) -> Result<()> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let headers = crate::error::GmailResponseHeaders::from_headers(response.headers());
    Err(Error::response_from_parts(
        service,
        status.as_u16(),
        headers,
        response.body,
    ))
}

/// Stable account-and-surface identity shared by registration and dispatch.
fn surface_quota_scope(
    account: &AccountId,
    surface: crate::quota::ApiSurface,
) -> bifrost_net::QuotaScope {
    let surface = match surface {
        crate::quota::ApiSurface::Gmail => "gmail",
        crate::quota::ApiSurface::People => "people",
        crate::quota::ApiSurface::Calendar => "calendar",
        crate::quota::ApiSurface::Drive => "drive",
    };
    bifrost_net::QuotaScope::namespaced("google.quota", &[&account.0, surface])
}

fn default_account_net(
    net: &Net,
    account: AccountId,
    gmail_base: &str,
    people_base: &str,
    calendar_base: &str,
    drive_base: &str,
    token_source: Arc<dyn TokenSource>,
) -> AccountNet {
    // Surface identities keep calendar reads independent of mail hydration,
    // as users expect even when Google serves both APIs on one host.
    use crate::quota::ApiSurface;
    let hosts = [
        (
            host_of(gmail_base, "www.googleapis.com"),
            ApiSurface::Gmail,
            GOOGLE_API_QUOTA_PER_SECOND,
            GOOGLE_API_BURST,
        ),
        (
            host_of(people_base, "people.googleapis.com"),
            ApiSurface::People,
            PEOPLE_API_QUOTA_PER_SECOND,
            PEOPLE_API_BURST,
        ),
        (
            host_of(calendar_base, "www.googleapis.com"),
            ApiSurface::Calendar,
            GOOGLE_API_QUOTA_PER_SECOND,
            GOOGLE_API_BURST,
        ),
        (
            host_of(drive_base, "www.googleapis.com"),
            ApiSurface::Drive,
            GOOGLE_API_QUOTA_PER_SECOND,
            GOOGLE_API_BURST,
        ),
    ]
    .into_iter()
    .map(|(host, surface, rate, burst)| {
        RateLimit::new(host, rate, 1, burst)
            .with_quota_scope(surface_quota_scope(&account, surface))
    })
    .collect();

    let mut spec = AccountSpec::new(Some(token_source));
    spec.hosts = hosts;
    net.attach_account(account, spec)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bytes::Bytes;
    use reqwest::StatusCode;

    use super::*;

    fn concurrency_client(script: &Arc<ScriptedDispatch>) -> GmailApi {
        let net = bifrost_net::test_support::scripted_account(
            script,
            bifrost_net::NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            bifrost_net::RetryPolicy::disabled(),
        );
        GmailApi::with_account_net("https://gmail.test", net)
    }

    // A reserved concurrency claim is not yet a recorded wire request:
    // admission, rate and token futures precede polling ScriptedDispatch.
    // Drive those stages with a bounded poll/yield loop before inspecting
    // counts. The operation must stay pending on a claim or canned response.
    async fn drive_pending<F: std::future::Future>(mut request: std::pin::Pin<&mut F>) {
        for _ in 0..64 {
            assert!(futures::FutureExt::now_or_never(request.as_mut()).is_none());
            tokio::task::yield_now().await;
        }
    }

    async fn saturate_gmail(
        client: &GmailApi,
    ) -> Vec<futures::future::BoxFuture<'static, Result<Response>>> {
        let mut requests = Vec::new();
        for _ in 0..GMAIL_DATA_CONCURRENCY {
            let client = client.clone();
            let mut request: futures::future::BoxFuture<'static, Result<Response>> =
                Box::pin(async move {
                    client
                        .execute(
                            "https://gmail.test/messages/m",
                            reqwest::Method::GET,
                            None::<&()>,
                            bifrost_net::ReplayPolicy::Idempotent,
                        )
                        .await
                });
            drive_pending(std::pin::Pin::new(&mut request)).await;
            requests.push(request);
        }
        requests
    }

    #[tokio::test]
    async fn the_33rd_gmail_data_request_waits_including_raw_mutations() {
        let script = ScriptedDispatch::new((0..34).map(|_| Canned::Pending));
        let client = concurrency_client(&script);
        let mut held = saturate_gmail(&client).await;
        assert_eq!(script.requests().len(), 32);
        let mut typed = Box::pin(client.execute(
            "https://gmail.test/profile",
            reqwest::Method::GET,
            None::<&()>,
            bifrost_net::ReplayPolicy::Idempotent,
        ));
        drive_pending(std::pin::Pin::new(&mut typed)).await;
        let mut raw = Box::pin(
            client.execute_builder(
                client
                    .account_net()
                    .post("https://gmail.test/messages/batchModify"),
                crate::quota::ApiSurface::Gmail,
                bifrost_net::ReplayPolicy::Never,
            ),
        );
        drive_pending(std::pin::Pin::new(&mut raw)).await;
        assert_eq!(script.requests().len(), 32, "both data funnels must queue");
        held.pop();
        drive_pending(std::pin::Pin::new(&mut typed)).await;
        assert_eq!(
            script.requests().len(),
            33,
            "release admits the next request"
        );
        drop(typed);
        drive_pending(std::pin::Pin::new(&mut raw)).await;
        assert_eq!(script.requests().len(), 34, "raw mutations also resume");
    }

    #[tokio::test]
    async fn other_google_services_bypass_a_saturated_gmail_queue() {
        let script = ScriptedDispatch::new((0..36).map(|_| Canned::Pending));
        let client = concurrency_client(&script);
        let _held = saturate_gmail(&client).await;
        let mut data = Box::pin(client.get::<serde_json::Value>("/profile"));
        drive_pending(std::pin::Pin::new(&mut data)).await;
        assert_eq!(
            script.requests().len(),
            32,
            "Gmail must actually be saturated"
        );
        let calendar_url = format!("{}/users/me/calendarList", client.calendar_base());
        let people_url = format!("{}/people/me/connections", client.people_base());
        let calendar_api = client.calendar();
        let people_api = client.people();
        let drive_api = client.drive();
        let mut calendar = Box::pin(calendar_api.get::<serde_json::Value>(&calendar_url));
        let mut people = Box::pin(people_api.get::<serde_json::Value>(&people_url));
        let mut drive = Box::pin(
            drive_api.get::<serde_json::Value>("https://www.googleapis.com/drive/v3/files"),
        );
        drive_pending(std::pin::Pin::new(&mut calendar)).await;
        drive_pending(std::pin::Pin::new(&mut people)).await;
        drive_pending(std::pin::Pin::new(&mut drive)).await;
        assert_eq!(
            script.requests().len(),
            35,
            "other services must reach transport"
        );
    }

    #[tokio::test]
    async fn watch_and_stop_bypass_a_saturated_gmail_queue() {
        let script = ScriptedDispatch::new((0..35).map(|_| Canned::Pending));
        let client = concurrency_client(&script);
        let _held = saturate_gmail(&client).await;
        let mut data = Box::pin(client.get::<serde_json::Value>("/profile"));
        drive_pending(std::pin::Pin::new(&mut data)).await;
        assert_eq!(
            script.requests().len(),
            32,
            "Gmail must actually be saturated"
        );
        let body = serde_json::json!({"topicName": "projects/test/topics/mail"});
        let mut watch = Box::pin(client.post::<serde_json::Value, _>(
            "/watch",
            &body,
            bifrost_net::ReplayPolicy::Never,
        ));
        let mut stop =
            Box::pin(client.post_no_content("/stop", &(), bifrost_net::ReplayPolicy::Never));
        drive_pending(std::pin::Pin::new(&mut watch)).await;
        drive_pending(std::pin::Pin::new(&mut stop)).await;
        assert_eq!(
            script.requests().len(),
            34,
            "control requests must reach transport"
        );
    }

    /// No single method may cost more than the bucket can ever hold, or
    /// that method could never be admitted at all. `messages.send` and
    /// `users.watch` sit exactly at the burst ceiling, so the two
    /// constants have to move together.
    ///
    /// A method we have not looked up must not ride free: it is billed
    /// like a message fetch until someone checks the published table.
    #[tokio::test(start_paused = true)]
    async fn gmail_requests_debit_their_method_quota_cost() {
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            bifrost_net::NetConfig::default(),
            // Gmail requests meter under the account-and-surface scope, so the
            // registration must carry the same identity dispatch uses.
            vec![
                RateLimit::new("gmail.test", 100.0, 1, 100).with_quota_scope(surface_quota_scope(
                    &AccountId("scripted".to_string()),
                    crate::quota::ApiSurface::Gmail,
                )),
            ],
            Arc::new(StaticTokenSource::new("token", None)),
            bifrost_net::RetryPolicy::disabled(),
        );
        let client = GmailApi::with_account_net("https://gmail.test", net);

        client
            .execute(
                "https://gmail.test/messages/send",
                reqwest::Method::POST,
                Some(&serde_json::json!({})),
                bifrost_net::ReplayPolicy::Never,
            )
            .await
            .expect("send dispatches");
        let second_client = client.clone();
        let second = tokio::spawn(async move {
            second_client
                .execute(
                    "https://gmail.test/profile",
                    reqwest::Method::GET,
                    None::<&()>,
                    bifrost_net::ReplayPolicy::Idempotent,
                )
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(
            script.requests().len(),
            1,
            "the 100-unit send drained the burst"
        );

        tokio::time::advance(Duration::from_millis(20)).await;
        second
            .await
            .expect("task joins")
            .expect("profile dispatches");
        assert_eq!(script.requests().len(), 2);
    }

    /// The mutation lane converts a refused `batchModify` / `batchDelete`
    /// into per-item failures and STILL emits a batch, so the bytes of
    /// the refusal must reach the accumulator. `bifrost-net` drains,
    /// meters and throttles that body before turning it into an error,
    /// so recording only on the success path would report zero for the
    /// batch that actually spent the quota.
    #[tokio::test]
    async fn a_failed_request_still_reports_its_inbound_bytes() {
        let body = "{\"error\":{\"code\":403}}";
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::FORBIDDEN,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(body),
        }]);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            bifrost_net::NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            bifrost_net::RetryPolicy::disabled(),
        );
        let client = GmailApi::with_account_net("https://gmail.test", net);
        let (client, tally) = client.metered();

        let result = client
            .execute(
                "https://gmail.test/messages/batchModify",
                reqwest::Method::POST,
                Some(&serde_json::json!({})),
                bifrost_net::ReplayPolicy::Never,
            )
            .await;
        assert!(result.is_err(), "a 403 must surface as an error");
        assert_eq!(script.requests().len(), 1, "the request was dispatched");
        assert_eq!(
            tally.take(),
            u64::try_from(body.len()).unwrap(),
            "the drained error body counts toward the batch total"
        );
    }

    #[tokio::test]
    async fn trims_api_base() {
        let client = GmailApi::with_api_base("https://example.test/base/", "token");
        assert_eq!(client.api_base(), "https://example.test/base");
        assert!(
            client.inner.net.is_none(),
            "constructing a factory client must not attach a throwaway account"
        );
    }

    #[tokio::test]
    async fn people_base_defaults_and_overrides_independently() {
        let client = GmailApi::with_api_base("https://example.test/gmail", "token");
        // People base defaults to production, independent of the Gmail base.
        assert_eq!(client.people_base(), PEOPLE_API_BASE);

        // Overriding People leaves the Gmail base untouched and trims the
        // trailing slash like the Gmail base does.
        let redirected = client.with_people_base("https://people.mock.test/v1/");
        assert_eq!(redirected.people_base(), "https://people.mock.test/v1");
        assert_eq!(redirected.api_base(), "https://example.test/gmail");
    }

    /// The Calendar base is independent of the other two and overridable per
    /// client.
    ///
    /// It used to be a `std::env::var` read on every request, which made it
    /// process-global: two accounts in one process could not use two Calendar
    /// endpoints, and the read sat on a hot path. The environment variable
    /// still works as a documented legacy fallback (downstream harnesses set
    /// it, and removing it would silently send their traffic to production
    /// rather than fail their build), but an explicit override wins over it.
    #[tokio::test]
    async fn calendar_base_defaults_and_overrides_independently() {
        let client = GmailApi::with_api_base("https://example.test/gmail", "token");
        // Defaults to production unless the legacy variable is set. Asserting
        // the accessor rather than the constant would pass either way, so this
        // reads whichever `default_calendar_base` resolved - the point of the
        // assertion is that the Calendar base is not derived from the Gmail
        // base, which was redirected above.
        assert_eq!(client.calendar_base(), default_calendar_base());
        assert_ne!(client.calendar_base(), client.api_base());

        let redirected = client.with_calendar_base("https://gcal.mock.test/calendar/v3/");
        assert_eq!(
            redirected.calendar_base(),
            "https://gcal.mock.test/calendar/v3",
            "an explicit base wins over the legacy environment variable, and trims like the others"
        );
        assert_eq!(
            redirected.api_base(),
            "https://example.test/gmail",
            "redirecting Calendar must not disturb the Gmail base"
        );
        assert_eq!(
            redirected.people_base(),
            PEOPLE_API_BASE,
            "redirecting Calendar must not disturb the People base"
        );
    }

    /// Overrides retain host metering; shared production hosts have distinct
    /// account-and-surface quota identities.
    #[test]
    fn rate_limit_hosts_follow_bases_and_surface_scopes_are_distinct() {
        assert_eq!(
            host_of("https://gcal.mock.test/calendar/v3", "www.googleapis.com"),
            "gcal.mock.test"
        );
        // A base that does not parse falls back to metering the real host
        // rather than registering nothing at all.
        assert_eq!(
            host_of("not a url", "www.googleapis.com"),
            "www.googleapis.com"
        );
        // Production: Gmail and Calendar are the same host, People differs.
        assert_eq!(
            host_of(GMAIL_API_BASE, "x"),
            host_of(CALENDAR_API_BASE, "y"),
            "the production Gmail and Calendar bases share a host"
        );
        assert_ne!(host_of(PEOPLE_API_BASE, "x"), host_of(GMAIL_API_BASE, "y"));
        let account = AccountId("quota-test".into());
        let scopes: std::collections::HashSet<_> = [
            crate::quota::ApiSurface::Gmail,
            crate::quota::ApiSurface::Calendar,
            crate::quota::ApiSurface::People,
            crate::quota::ApiSurface::Drive,
        ]
        .into_iter()
        .map(|surface| surface_quota_scope(&account, surface))
        .collect();
        assert_eq!(scopes.len(), 4);
    }

    #[tokio::test]
    async fn rotated_token_source_is_read() {
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let client = GmailApi::with_source(Arc::new(source.clone()));
        assert_eq!(client.access_token().await, "old-token");
        source.set(AccessToken::new("new-token", None));
        assert_eq!(client.access_token().await, "new-token");
    }

    #[test]
    fn account_scope_is_attached_only_by_for_account() {
        let client = GmailApi::new("token");
        let opened = client.for_account(AccountId("client-test".to_string()));
        assert!(opened.inner.net.is_some());
        opened.detach_account();
    }
}

#[cfg(test)]
mod injected_net_tests {
    use super::*;

    #[test]
    fn injected_transport_survives_account_attachment() {
        let net = Net::new(bifrost_net::NetConfig::default()).unwrap();
        let witness = net.attach_account(
            AccountId("google-net-witness".into()),
            AccountSpec::new(None),
        );
        let client = GmailApi::with_api_base_source_and_net(
            GMAIL_API_BASE,
            Arc::new(StaticTokenSource::new("token", None)),
            net,
        )
        .for_account(AccountId("google-injected-net".into()));
        assert!(client.account_net().shares_transport_with(&witness));
    }
}
