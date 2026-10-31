use std::sync::{Arc, RwLock};

#[cfg(test)]
use bifrost_net::RetryPolicy;
use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, RateLimit, StaticTokenSource, TokenSource,
};
#[cfg(test)]
use bifrost_types::TransmissionState;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use crate::error::{GraphError, GraphResponseError};
use crate::origin::{AdmittedUrl, Base, ProviderLink, Refusal, UploadUrl};

pub(crate) const GRAPH_API_BASE: &str = "https://graph.microsoft.com/v1.0";

/// Production origin for the two non-Graph Microsoft surfaces this crate
/// talks to: Exchange Autodiscover (`/autodiscover/autodiscover.xml`,
/// `/autodiscover/autodiscover.svc`) and EWS (`/EWS/Exchange.asmx`). Both
/// live on `outlook.office365.com`, NOT on the Graph host, so the Graph
/// api-base override alone never redirected them.
pub(crate) const OUTLOOK_BASE: &str = "https://outlook.office365.com";

/// Production Graph host. Used only to decide whether an api-base is the
/// real Graph endpoint or a harness redirect.
const GRAPH_HOST: &str = "graph.microsoft.com";

/// In-flight Graph REST requests allowed per client (shared with every
/// client derived from it, so a shared-mailbox client does not multiply the
/// budget). This is a LOCAL limiter, and deliberately so only for as long as
/// there is no shared one: bifrost-net governs rate and retry, not
/// concurrency, and bifrost-sync's budget is per-scope work, not per-account
/// requests. If a per-account concurrency limiter ever lands in either of
/// them, this semaphore and its permit acquisition in `execute_wire` should
/// be deleted rather than stacked on top of it - two independent limiters on
/// one request path make the effective ceiling a function of both and neither
/// one tunable.
const CONCURRENCY_LIMIT: usize = 3;

// pub: GraphAccountFactory consumers need a constructible Graph client handle.
#[derive(Clone)]
pub struct GraphClient {
    inner: Arc<ClientInner>,
    /// Where this handle reports request-local inbound bytes, if
    /// anywhere. `None` on every ordinary client; a stream that needs
    /// per-batch accounting takes a metered handle via `metered()`.
    /// Kept OUTSIDE `ClientInner` so installing accounting is one `Arc`
    /// bump over the same transport, semaphore, and script rather than a
    /// fork of the client's configuration.
    tally: Option<ByteTally>,
}

struct ClientInner {
    net: Option<Net>,
    account_net: RwLock<Option<AccountNet>>,
    /// The Graph REST base, and the only origin a REST or blob request may
    /// carry the bearer to (see `crate::origin`).
    api_base: Base,
    /// Origin for the Autodiscover + EWS surfaces. Derived from `api_base`
    /// (see [`derive_outlook_base`]) unless a consumer overrode it with
    /// [`GraphClient::with_outlook_base`].
    outlook_base: Base,
    rate_limit_host: String,
    token_source: Arc<dyn TokenSource>,
    mailbox_id: Option<String>,
    semaphore: Arc<Semaphore>,
    /// Shared with every client derived from this one
    /// (`for_shared_mailbox`, `with_outlook_base`), the way the semaphore
    /// and the `AccountNet` already are: in production those derivatives
    /// issue their requests down the same transport, so a seam they did not
    /// share would leave every foreign-mailbox path unscriptable and
    /// silently outside the recorded request list.
    #[cfg(test)]
    scripted: Arc<std::sync::Mutex<ScriptedRest>>,
}

/// Inbound payload bytes attributed to one engine batch.
///
/// An engine batch here routinely covers several requests - a `$delta`
/// page plus its follow-ups, or a `$batch` submission plus the
/// per-item retries it spawned - so per-response totals have to be
/// summed somewhere. They are summed HERE, on an accumulator owned by
/// one stream, rather than sampled off the account-cumulative
/// bandwidth meter: that meter is shared by every concurrent request on
/// the account, so a delta across it would attribute another scope's
/// traffic to this batch.
///
/// `take` reads and resets, so consecutive batches from one stream
/// partition the bytes rather than each reporting a running total.
#[derive(Clone, Default)]
pub(crate) struct ByteTally(Arc<std::sync::atomic::AtomicU64>);

impl ByteTally {
    pub(crate) fn add(&self, n: u64) {
        self.0.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Bytes recorded since the previous `take`.
    pub(crate) fn take(&self) -> u64 {
        self.0.swap(0, std::sync::atomic::Ordering::Relaxed)
    }
}

/// Graph-owned response shape at the one REST funnel. Adapts the
/// production `bifrost_net::Response` (which is `#[non_exhaustive]`) into
/// a shape this crate owns, so the helpers above the funnel destructure
/// it freely.
///
/// It carries no byte count: the funnel attributes inbound bytes from
/// the request-local counter instead, because that number also exists
/// when the request ends in an error, and a response-carried count does
/// not.
pub(crate) struct RestResponse {
    pub(crate) status: reqwest::StatusCode,
    pub(crate) headers: reqwest::header::HeaderMap,
    pub(crate) body: Bytes,
}

impl From<bifrost_net::Response> for RestResponse {
    fn from(response: bifrost_net::Response) -> Self {
        Self {
            status: response.status(),
            headers: response.headers,
            body: response.body,
        }
    }
}

/// Request body plus the content type it travels under, at the one wire
/// funnel. JSON callers serialize up front instead of handing
/// `RequestBuilder::json` a `Serialize` value, which keeps the funnel
/// non-generic so the raw-MIME caller (`post_mime`) can share it - and
/// therefore share the test seam - rather than opening a second,
/// unscriptable path to `AccountNet`.
struct WireBody {
    content_type: &'static str,
    bytes: Bytes,
}

/// Content type every JSON call sends, and the one a bodiless call still
/// sends (unchanged from before the funnel merge).
const JSON_CONTENT_TYPE: &str = "application/json";

/// Graph's import-from-MIME content type: the body is the base64 of the
/// RFC 5322 octets, not JSON.
const MIME_CONTENT_TYPE: &str = "text/plain";

impl WireBody {
    fn json<B: Serialize + ?Sized>(body: &B) -> Result<Self, GraphError> {
        // Mirrors `RequestBuilder::json`'s failure shape so a body whose
        // `Serialize` impl fails still surfaces as `EncodeBody`.
        let bytes = serde_json::to_vec(body).map_err(|error| {
            GraphError::Net(bifrost_net::Error::EncodeBody {
                message: format!("serde_json::to_vec failed: {error}"),
                source: Some(Box::new(error)),
            })
        })?;
        Ok(Self {
            content_type: JSON_CONTENT_TYPE,
            bytes: Bytes::from(bytes),
        })
    }

    fn mime(bytes: Bytes) -> Self {
        Self {
            content_type: MIME_CONTENT_TYPE,
            bytes,
        }
    }
}

/// Where an auxiliary request goes, and therefore whether it carries the
/// account bearer: only an admitted URL may, so the choice is made by type,
/// not by a flag a caller could set next to any string.
pub(crate) enum AuxTarget<'a> {
    /// Carries the bearer; admitted onto a configured origin.
    Bearer(&'a AdmittedUrl),
    /// Carries no bearer: a URL that is its own credential (the OneDrive
    /// pre-authenticated upload session), admitted under the upload rule.
    Anonymous(&'a UploadUrl),
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct RestRequest {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) if_match: Option<String>,
    pub(crate) prefer: Option<String>,
    pub(crate) content_type: String,
    /// The JSON body as sent, when the call sent one. `None` for a
    /// bodiless call and for the raw-MIME call, whose bytes land in
    /// `raw_body` instead.
    pub(crate) body: Option<serde_json::Value>,
    /// The verbatim bytes of a non-JSON body (Graph's base64 MIME import).
    pub(crate) raw_body: Option<Bytes>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct ScriptedRestResponse {
    status: reqwest::StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Bytes,
}

#[cfg(test)]
impl ScriptedRestResponse {
    pub(crate) fn json(status: reqwest::StatusCode, body: serde_json::Value) -> Self {
        Self {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).expect("JSON response serializes")),
        }
    }

    /// Hand this response to bifrost-net's scripted wire dispatch as the
    /// bytes a server produced. What the caller then sees - `Ok`, a typed
    /// `Error::Status`, `AuthLost`, `RateLimited`, a retry - is decided by
    /// the production retry loop, not restated here.
    fn into_canned(self) -> bifrost_net::test_support::Canned {
        bifrost_net::test_support::Canned::Response {
            status: self.status,
            headers: self.headers,
            body: self.body,
        }
    }

    /// A non-JSON body: the Autodiscover surfaces answer XML, not JSON.
    pub(crate) fn text(status: reqwest::StatusCode, body: &str) -> Self {
        Self {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(body.to_string()),
        }
    }

    pub(crate) fn empty(status: reqwest::StatusCode) -> Self {
        Self {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    pub(crate) fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).expect("valid test header"),
            reqwest::header::HeaderValue::from_str(value).expect("valid test header value"),
        );
        self
    }
}

/// Test-only scripting state, shared by every client derived from a
/// scripted one (`for_shared_mailbox`, `with_outlook_base`) so a
/// derivative issues its requests down the same scripted transport its
/// parent was given - which is what production does with the real one.
///
/// Responses are NOT queued here. The REST and aux surfaces answer from
/// a `bifrost_net` scripted wire dispatch installed as `wire_net`, so a
/// scripted status travels the production retry loop and this crate does
/// not restate what bifrost-net does with it. Only the recorded request
/// shapes stay local, because they are richer than the transport's
/// (parsed JSON bodies, lifted `If-Match` / `Prefer`).
#[cfg(test)]
#[derive(Default)]
struct ScriptedRest {
    /// Installed by the first `script_rest` / `script_aux` call. Its
    /// presence is what makes the client a closed system: once set, every
    /// REST and aux request answers from the script, and an exhausted
    /// script panics rather than reaching the network.
    wire: Option<Arc<bifrost_net::test_support::ScriptedDispatch>>,
    /// The `AccountNet` bound to `wire`. Preferred over the client's own
    /// `account_net` slot while scripting is active, so a client derived
    /// AFTER the script was installed still answers from it.
    wire_net: Option<AccountNet>,
    requests: Vec<RestRequest>,
    aux_requests: Vec<AuxRequest>,
    download_requests: Vec<DownloadRequest>,
}

/// A request recorded at the auxiliary wire funnel: the pre-authed OneDrive
/// chunk PUT and the Autodiscover POST. Both are outside `execute_wire`
/// because neither is a Graph REST JSON call - one drops the bearer and
/// carries `Content-Range`, the other posts XML to the Autodiscover origin -
/// so they get their own recorded shape rather than being forced into
/// `RestRequest`'s JSON-shaped one.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct AuxRequest {
    pub(crate) method: String,
    pub(crate) url: String,
    /// Every header the caller set, in the order it set them.
    pub(crate) headers: Vec<(String, String)>,
    /// `false` for the OneDrive chunk PUT: the session URL is
    /// pre-authenticated and sending the Graph bearer to it is a leak.
    pub(crate) bearer: bool,
    pub(crate) body: Bytes,
}

#[cfg(test)]
impl AuxRequest {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct DownloadRequest {
    pub(crate) url: String,
    pub(crate) range: Option<bifrost_types::ByteRange>,
}

/// A scripted answer to `GraphClient::download_stream`, in terms of what
/// the SERVER produced. What the caller then sees is bifrost-net's to
/// decide, the same way it is for the REST surface.
///
/// `bifrost-net` resolves the status before handing back a stream (a 4xx
/// is an `Err`, never a body), so a failing status is a failure at open;
/// only a body that dies partway through reaches the caller as a stream
/// that then errors.
#[cfg(test)]
pub(crate) enum ScriptedDownload {
    /// A 200 whose body arrives as these chunks, framed individually so a
    /// test can pin that the blob stream forwards them rather than
    /// re-framing.
    Chunks(Vec<Bytes>),
    /// A `206 Partial Content` answering a RANGED read, carrying the
    /// given `Content-Range`.
    ///
    /// bifrost-net refuses a ranged read that does not come back 206 with
    /// a `Content-Range` matching the requested window - assembling
    /// mismatched bytes into a blob would corrupt it silently. So the
    /// header stated here is checked against the `Range` the account
    /// actually sent, which makes a ranged test prove the request as well
    /// as the response.
    PartialChunks {
        /// The `Content-Range` the server answers with, e.g.
        /// `bytes 2-6/11`.
        content_range: &'static str,
        /// Body chunks, framed individually.
        chunks: Vec<Bytes>,
    },
    /// Chunks that arrive before the body fails - the one failure mode
    /// that is NOT resolved at open. Surfaces as `Error::Network`, which
    /// is what a real socket failure mid-body produces.
    ChunksThenError(Vec<Bytes>, String),
    /// A failing status at open. The typed error the caller sees is
    /// produced by the transport from this status, not stated here.
    FailedStatus(reqwest::StatusCode),
}

#[cfg(test)]
impl ScriptedDownload {
    fn into_canned(self) -> bifrost_net::test_support::Canned {
        use bifrost_net::test_support::Canned;
        use reqwest::header::HeaderMap;

        match self {
            Self::Chunks(chunks) => Canned::Stream {
                status: reqwest::StatusCode::OK,
                headers: HeaderMap::new(),
                chunks,
            },
            Self::PartialChunks {
                content_range,
                chunks,
            } => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    reqwest::header::CONTENT_RANGE,
                    reqwest::header::HeaderValue::from_static(content_range),
                );
                Canned::Stream {
                    status: reqwest::StatusCode::PARTIAL_CONTENT,
                    headers,
                    chunks,
                }
            }
            Self::ChunksThenError(chunks, message) => Canned::StreamThenError {
                status: reqwest::StatusCode::OK,
                headers: HeaderMap::new(),
                chunks,
                message,
            },
            Self::FailedStatus(status) => Canned::Response {
                status,
                headers: HeaderMap::new(),
                body: Bytes::new(),
            },
        }
    }
}

impl GraphClient {
    // pub: ergonomic constructor for the default Microsoft Graph endpoint.
    pub fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_base(GRAPH_API_BASE, access_token)
    }

    // pub: consumers may need sovereign-cloud or test Graph API endpoints before registration.
    pub fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        // Bases are trimmed once in `with_bases_and_source`.
        let token_source: Arc<dyn TokenSource> =
            Arc::new(StaticTokenSource::new(access_token, None));
        Self::with_source(api_base, token_source)
    }

    // pub: source-accepting constructor. ratatoskr hands in a shared
    // `Arc<dyn TokenSource>` (typically an `OAuthRefresher` over its own
    // refresh-token store) so a token it refreshes and persists is read
    // live at every wire authentication without reopening the client.
    pub fn with_source(api_base: impl Into<String>, source: Arc<dyn TokenSource>) -> Self {
        let api_base = Base::parse(api_base);
        let rate_limit_host = host_from_api_base(&api_base);
        let outlook_base = derive_outlook_base(&api_base);
        Self {
            tally: None,
            inner: Arc::new(ClientInner {
                net: Some(Net::shared_default()),
                account_net: RwLock::new(None),
                api_base,
                outlook_base,
                rate_limit_host,
                token_source: source,
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
                #[cfg(test)]
                scripted: Arc::new(std::sync::Mutex::new(ScriptedRest::default())),
            }),
        }
    }

    // pub: custom Net injection lets consumers opt out of Net::shared_default host buckets.
    pub fn with_account_net(
        net: AccountNet,
        api_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        let api_base = Base::parse(api_base);
        let outlook_base = derive_outlook_base(&api_base);
        // Derived from the supplied base like every other constructor,
        // not hardcoded: an injected net against a redirected base must
        // meter under its own host bucket, not the production Graph
        // host's.
        let rate_limit_host = host_from_api_base(&api_base);
        Self {
            tally: None,
            inner: Arc::new(ClientInner {
                net: None,
                account_net: RwLock::new(Some(net)),
                api_base,
                outlook_base,
                rate_limit_host,
                token_source,
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
                #[cfg(test)]
                scripted: Arc::new(std::sync::Mutex::new(ScriptedRest::default())),
            }),
        }
    }

    /// Bind this client to the engine's account id so metering, priority,
    /// caps and tracing use the real key.
    ///
    /// Audit boundary: the 2026-07 google+net bug sweep line-audited this
    /// crate only along the reattach path through here; everything else in
    /// `bifrost-graph` was covered by its own tests and the later 2026-09-04
    /// hunt, not by that sweep. Listed so a future auditor knows where that
    /// sweep's coverage stopped.
    pub(crate) fn attach_account(&self, account_id: AccountId) {
        if let Some(net) = self.inner.net.as_ref() {
            let token_source = Arc::clone(&self.inner.token_source);
            let mut spec = AccountSpec::new(Some(token_source));
            spec.hosts = vec![
                RateLimit::new(self.inner.rate_limit_host.clone(), 10.0, 1, 10)
                    .with_quota_scope(account_id.0.clone()),
            ];
            let account_net = net.attach_account(account_id, spec);
            // Install the replacement first, then tear down whatever
            // registration it displaced. `Net::attach_account` mints a
            // fresh token on every call and no longer unregisters a
            // previous attachment for the same id, so a reopen that
            // dropped its old handle without detaching would leak one
            // meter attachment and one governor attach count per
            // cycle - the host bucket would never be reclaimed.
            // Detaching after the install keeps the shared counts from
            // dipping to zero, so in-flight requests on the old handle
            // keep metering against the same counters.
            let displaced = match self.inner.account_net.write() {
                Ok(mut slot) => slot.replace(account_net),
                Err(_) => {
                    // A poisoned lock means a panic happened while a writer
                    // held it. Skipping the detach here leaks one meter
                    // attachment (the very leak the ordering above exists to
                    // prevent), so it must not be silent.
                    tracing::warn!("account_net lock poisoned; displaced attachment not detached");
                    None
                }
            };
            if let Some(displaced) = displaced {
                displaced.detach();
            }
            return;
        }
        // No parent `Net`: the consumer constructed us via
        // `with_account_net`. Retag the existing `AccountNet` so
        // per-account metering and host bookkeeping move under the
        // engine id; in-flight requests on the old handle continue.
        let existing = self
            .inner
            .account_net
            .read()
            .ok()
            .and_then(|slot| slot.clone());
        let Some(existing) = existing else {
            return;
        };
        if existing.account() == &account_id {
            return;
        }
        let retagged = existing.retag(account_id);
        if let Ok(mut slot) = self.inner.account_net.write() {
            *slot = Some(retagged);
        }
    }

    pub(crate) fn account_net(&self) -> Option<AccountNet> {
        self.inner
            .account_net
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    #[cfg(test)]
    pub(crate) fn api_base(&self) -> &str {
        self.inner.api_base.as_str()
    }

    /// Origin for the Autodiscover + EWS surfaces.
    #[cfg(test)]
    pub(crate) fn outlook_base(&self) -> &str {
        self.inner.outlook_base.as_str()
    }

    /// A crate-built Graph path, admitted onto the api-base. The only way
    /// it fails is an unusable configured base, which is the consumer's
    /// configuration, so it surfaces as `GraphError::Configuration`.
    pub(crate) fn api_url(&self, path: &str) -> Result<AdmittedUrl, GraphError> {
        self.inner
            .api_base
            .admit(path)
            .map_err(configuration_refusal)
    }

    /// Admit a link a Graph response supplied. Called at RECEIPT - before
    /// the link is followed, minted into a caller cursor, or persisted - so
    /// a refusal is attributed to the response that carried it.
    pub(crate) fn admit_link(&self, link: &ProviderLink) -> Result<AdmittedUrl, GraphError> {
        self.inner
            .api_base
            .admit_link(link)
            .map_err(|refusal| match refusal {
                Refusal::InvalidBase(_) => configuration_refusal(refusal),
                Refusal::Target(reason) => GraphError::ProviderLinkRefused { reason },
            })
    }

    /// Admit a page's optional `@odata.nextLink`, the step every walk takes
    /// before following it.
    pub(crate) fn admit_next(
        &self,
        link: Option<&ProviderLink>,
    ) -> Result<Option<AdmittedUrl>, GraphError> {
        link.map(|link| self.admit_link(link)).transpose()
    }

    /// Admit a target whose provenance the CALLER knows (a caller page
    /// cursor, a persisted sync cursor) and will classify itself.
    pub(crate) fn admit_target(&self, target: &str) -> Result<AdmittedUrl, Refusal> {
        self.inner.api_base.admit(target)
    }

    /// Admit a OneDrive upload session URL under the upload rule (see
    /// `Base::admit_upload`). The caller classifies the refusal.
    pub(crate) fn admit_upload(&self, raw: &str) -> Result<UploadUrl, Refusal> {
        self.inner.api_base.admit_upload(raw)
    }

    /// A crate-built Autodiscover / EWS path, admitted onto the Outlook
    /// origin.
    pub(crate) fn outlook_url(&self, path: &str) -> Result<AdmittedUrl, GraphError> {
        self.inner
            .outlook_base
            .admit(path)
            .map_err(configuration_refusal)
    }

    /// The EWS SOAP endpoint on the Outlook origin, or why there is none.
    pub(crate) fn ews_url(&self) -> Result<AdmittedUrl, String> {
        self.inner
            .outlook_base
            .admit(crate::ews::EWS_PATH)
            .map_err(|refusal| refusal.to_string())
    }

    /// Admit an endpoint an Autodiscover response named, onto the Outlook
    /// origin. The caller classifies the refusal.
    pub(crate) fn admit_outlook_target(&self, target: &str) -> Result<AdmittedUrl, Refusal> {
        self.inner.outlook_base.admit(target)
    }

    /// Override the Autodiscover / EWS origin independently of the Graph
    /// api-base. Test seam mirroring `bifrost-google`'s
    /// `with_people_api_base`: those two surfaces live on a different host
    /// from Graph in production, so a harness that only redirects the Graph
    /// base would still send Autodiscover and EWS traffic to
    /// `outlook.office365.com`. Redirecting the Graph api-base to a
    /// non-Graph host already derives this (so the common harness case needs
    /// no extra call); this exists for a sovereign cloud, where the Graph and
    /// Outlook hosts differ but neither is the public one.
    // pub: harness / sovereign-cloud consumers redirect Autodiscover + EWS before registration.
    #[must_use]
    pub fn with_outlook_base(&self, outlook_base: impl Into<String>) -> Self {
        Self {
            tally: self.tally.clone(),
            inner: Arc::new(ClientInner {
                net: self.inner.net.clone(),
                account_net: RwLock::new(self.account_net()),
                api_base: self.inner.api_base.clone(),
                outlook_base: Base::parse(outlook_base),
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                mailbox_id: self.inner.mailbox_id.clone(),
                semaphore: Arc::clone(&self.inner.semaphore),
                #[cfg(test)]
                scripted: Arc::clone(&self.inner.scripted),
            }),
        }
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

    pub(crate) fn api_path_prefix(&self) -> String {
        match &self.inner.mailbox_id {
            Some(id) => format!("/users/{}", bifrost_net::url::encode_path_component(id)),
            None => "/me".to_string(),
        }
    }

    // pub: shared-mailbox consumers derive a scoped client before building the factory.
    pub fn for_shared_mailbox(&self, mailbox_id: impl Into<String>) -> Self {
        Self {
            tally: self.tally.clone(),
            inner: Arc::new(ClientInner {
                net: self.inner.net.clone(),
                account_net: RwLock::new(self.account_net()),
                api_base: self.inner.api_base.clone(),
                outlook_base: self.inner.outlook_base.clone(),
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                mailbox_id: Some(mailbox_id.into()),
                semaphore: Arc::clone(&self.inner.semaphore),
                #[cfg(test)]
                scripted: Arc::clone(&self.inner.scripted),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_shared_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_some()
    }

    pub(crate) fn uses_default_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_none()
    }

    #[cfg(test)]
    pub(crate) fn mailbox_id(&self) -> Option<&str> {
        self.inner.mailbox_id.as_deref()
    }

    pub(crate) async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.get(&url).await
    }

    pub(crate) async fn get_json_prefer<T: DeserializeOwned>(
        &self,
        path: &str,
        prefer: &str,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.get_prefer(&url, prefer).await
    }

    /// GET an admitted URL: a crate-built path (`api_url`) or an admitted
    /// cursor / server link. There is no string-taking twin on purpose.
    pub(crate) async fn get<T: DeserializeOwned>(
        &self,
        url: &AdmittedUrl,
    ) -> Result<T, GraphError> {
        self.request::<T, ()>(url, "GET", None).await
    }

    pub(crate) async fn get_prefer<T: DeserializeOwned>(
        &self,
        url: &AdmittedUrl,
        prefer: &str,
    ) -> Result<T, GraphError> {
        self.request_prefer::<T, ()>(url, "GET", prefer, None).await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.request(&url, "POST", Some(body)).await
    }

    pub(crate) async fn post_empty(&self, path: &str) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self.execute(&url, "POST", None::<&()>).await?;
        check_response_status(response)
    }

    pub(crate) async fn post_no_response<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self.execute(&url, "POST", Some(body)).await?;
        check_response_status(response)
    }

    /// POST a base64-encoded RFC 5322 message to create a draft from raw
    /// MIME (Graph's import-from-MIME path: `Content-Type: text/plain`,
    /// body = the base64 of the MIME octets). Returns the created message
    /// resource. Distinct from `post` because that path hardcodes a JSON
    /// content type and serializes its body; raw MIME needs neither.
    pub(crate) async fn post_mime<T: DeserializeOwned>(
        &self,
        path: &str,
        base64_mime: Bytes,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute_wire(&url, "POST", None, None, Some(WireBody::mime(base64_mime)))
            .await?;
        parse_json_response(response)
    }

    pub(crate) async fn patch<B: Serialize>(&self, path: &str, body: &B) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self.execute(&url, "PATCH", Some(body)).await?;
        check_response_status(response)
    }

    pub(crate) async fn patch_if_match<B: Serialize>(
        &self,
        path: &str,
        etag: &str,
        body: &B,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute_if_match(&url, "PATCH", etag, Some(body))
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn patch_json<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.request(&url, "PATCH", Some(body)).await
    }

    pub(crate) async fn delete(&self, path: &str) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self.execute(&url, "DELETE", None::<&()>).await?;
        check_response_status(response)
    }

    pub(crate) async fn delete_if_match(&self, path: &str, etag: &str) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute_if_match(&url, "DELETE", etag, None::<&()>)
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn post_batch(
        &self,
        batch: &crate::types::BatchRequest,
    ) -> Result<crate::types::BatchResponse, GraphError> {
        // Graph rejects a subrequest whose body does not declare its type
        // ("Write request with id '0' does not contain a Content-Type
        // header"), so every body-carrying item gets one here.
        let mut batch = batch.clone();
        for item in batch.requests.iter_mut().filter(|item| item.body.is_some()) {
            item.headers
                .get_or_insert_with(Default::default)
                .entry("Content-Type".to_string())
                .or_insert_with(|| "application/json".to_string());
        }
        self.post("/$batch", &batch).await
    }

    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &AdmittedUrl,
        method: &str,
        body: Option<&B>,
    ) -> Result<T, GraphError> {
        let response = self.execute(url, method, body).await?;
        parse_json_response(response)
    }

    async fn request_prefer<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &AdmittedUrl,
        method: &str,
        prefer: &str,
        body: Option<&B>,
    ) -> Result<T, GraphError> {
        let response = self
            .execute_request(url, method, None, Some(prefer), body)
            .await?;
        parse_json_response(response)
    }

    async fn execute<B: Serialize>(
        &self,
        url: &AdmittedUrl,
        method: &str,
        body: Option<&B>,
    ) -> Result<RestResponse, GraphError> {
        self.execute_request(url, method, None, None, body).await
    }

    async fn execute_if_match<B: Serialize>(
        &self,
        url: &AdmittedUrl,
        method: &str,
        etag: &str,
        body: Option<&B>,
    ) -> Result<RestResponse, GraphError> {
        self.execute_request(url, method, Some(etag), None, body)
            .await
    }

    async fn execute_request<B: Serialize>(
        &self,
        url: &AdmittedUrl,
        method: &str,
        if_match: Option<&str>,
        prefer: Option<&str>,
        body: Option<&B>,
    ) -> Result<RestResponse, GraphError> {
        let body = match body {
            Some(body) => Some(WireBody::json(body)?),
            None => None,
        };
        self.execute_wire(url, method, if_match, prefer, body).await
    }

    /// The one place a Graph REST request leaves this crate. Every helper
    /// above funnels through here, raw MIME included, so the scripted seam
    /// covers the whole surface and no path can quietly bypass it.
    ///
    /// It takes an `AdmittedUrl` and nothing else: bifrost-net attaches the
    /// bearer to the first request whatever its host, so the origin decision
    /// has to be made before this point, and the type proves it was.
    async fn execute_wire(
        &self,
        url: &AdmittedUrl,
        method: &str,
        if_match: Option<&str>,
        prefer: Option<&str>,
        body: Option<WireBody>,
    ) -> Result<RestResponse, GraphError> {
        let url = url.as_str();
        // Records the request in this crate's richer shape and does not
        // answer it: the response comes back from the scripted transport
        // below, through the production retry loop.
        #[cfg(test)]
        self.record_wire(method, url, if_match, prefer, body.as_ref());
        // Nothing closes this semaphore, so a closed one is the client's own
        // invariant failing - not a network fault a retry could clear.
        let _permit = self
            .inner
            .semaphore
            .acquire()
            .await
            .map_err(|_| GraphError::Internal {
                message: "Graph request semaphore closed".to_string(),
            })?;
        let account_net = self.attached_net()?;

        let mut builder = match method {
            "GET" => account_net.get(url),
            "POST" => account_net.post(url),
            "PATCH" => account_net.patch(url),
            "DELETE" => account_net.delete(url),
            // pub(crate) callers route through `get_json` / `post` /
            // `patch` / `delete` / `post_empty` only; any other token
            // is a programmer bug rather than a runtime path.
            other => {
                unreachable!("unsupported HTTP method passed to GraphClient::execute: {other}")
            }
        };

        builder = builder.header(
            "Content-Type",
            body.as_ref()
                .map_or(JSON_CONTENT_TYPE, |body| body.content_type),
        );
        if let Some(etag) = if_match {
            builder = builder.header("If-Match", etag);
        }
        if let Some(prefer) = prefer {
            builder = builder.header("Prefer", prefer);
        }

        if let Some(body) = body {
            builder = builder.body(body.bytes);
        }

        let counter = bifrost_net::RequestByteCounter::new();
        let sent = builder.count_bytes_into(counter.clone()).send().await;
        self.record_bytes(&counter);
        let response = sent.map_err(GraphError::Net)?;
        Ok(RestResponse::from(response))
    }

    /// The one place the two NON-REST wire paths leave this crate: the
    /// pre-authenticated OneDrive upload session (chunk PUTs and the cancel
    /// DELETE, bearer suppressed, because the session URL carries its own
    /// credential and forwarding the Graph token to it would leak it) and
    /// the Autodiscover POST (XML to the Autodiscover origin, which is not
    /// the Graph host).
    ///
    /// Deliberately NOT folded into `execute_wire`: that funnel takes the
    /// client's concurrency permit and always sends a bearer, and a chunked
    /// upload holding a Graph permit per chunk is a different production
    /// behavior than the one this path has always had. Keeping it separate
    /// makes the seam an extraction of the existing code rather than a
    /// change to it.
    ///
    /// `timeout`, when given, is the request's TOTAL deadline in bifrost-net
    /// (admission, every attempt, retry backoff, redirects, body drain), so a
    /// caller spending one budget across several requests passes what is
    /// left of it and bifrost-net classifies an expiry by the stage it hit.
    /// `None` keeps the account default.
    pub(crate) async fn execute_aux(
        &self,
        method: &str,
        target: AuxTarget<'_>,
        headers: &[(&str, &str)],
        body: Bytes,
        timeout: Option<std::time::Duration>,
    ) -> Result<RestResponse, GraphError> {
        let (url, bearer) = match target {
            AuxTarget::Bearer(url) => (url.as_str(), true),
            AuxTarget::Anonymous(url) => (url.as_str(), false),
        };
        #[cfg(test)]
        self.record_aux(method, url, headers, bearer, &body);
        let account_net = self.attached_net()?;
        let mut builder = match method {
            "POST" => account_net.post(url),
            "PUT" => account_net.put(url),
            // The OneDrive upload session cancel.
            "DELETE" => account_net.delete(url),
            other => {
                unreachable!("unsupported HTTP method passed to GraphClient::execute_aux: {other}")
            }
        };
        if !bearer {
            // An anonymous target was admitted under the upload rule, which
            // the transport's redirect walk cannot apply to a hop: following
            // one would send the chunk (and a URL that is itself the
            // credential's destination) somewhere never admitted, over plain
            // http included. Graph's upload flow does not redirect, so a 3xx
            // comes back to the caller as the response it is.
            builder = builder
                .without_bearer_auth()
                .follow_redirects(bifrost_net::FollowRedirects::Disabled);
        }
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        let counter = bifrost_net::RequestByteCounter::new();
        let sent = builder
            .body(body)
            .count_bytes_into(counter.clone())
            .send()
            .await;
        self.record_bytes(&counter);
        let response = sent.map_err(GraphError::Net)?;
        Ok(RestResponse::from(response))
    }

    /// Attribute one request's inbound bytes to this handle's batch
    /// accumulator, if it has one. Applied at BOTH wire funnels: the
    /// pre-authenticated chunk PUT and the Autodiscover POST are real
    /// inbound traffic on the account and would otherwise be free.
    ///
    /// Takes the request-local counter rather than the `Response`, so
    /// the bytes are attributed whether the request succeeded or
    /// failed. `bifrost-net` drains non-2xx bodies, exhausted retries
    /// and repeated 401s before converting them to an error, and the
    /// mutation and hydration lanes turn such an error into a per-item
    /// failure while still emitting a batch - so recording only on
    /// success would under-report exactly the batches that hit trouble.
    fn record_bytes(&self, counter: &bifrost_net::RequestByteCounter) {
        if let Some(tally) = self.tally.as_ref() {
            tally.add(counter.bytes_in());
        }
    }

    /// A handle over the same client that reports every buffered
    /// response's inbound bytes into a fresh accumulator.
    ///
    /// One accumulator per stream, not per client: the returned handle
    /// is the only one recording into it, so concurrent work on other
    /// scopes cannot contaminate the total. Clients DERIVED from this
    /// handle (`for_shared_mailbox`, `with_outlook_base`) inherit it,
    /// because in production those derivatives issue their requests
    /// down the same transport on behalf of the same batch.
    pub(crate) fn metered(&self) -> (Self, ByteTally) {
        let tally = ByteTally::default();
        (self.with_tally(tally.clone()), tally)
    }

    /// The same handle reporting into an EXISTING accumulator. Used to
    /// enroll the shared-mailbox clients of one account into the batch
    /// accumulator the primary client already carries, so a
    /// foreign-mailbox request is attributed to the batch that made it.
    pub(crate) fn with_tally(&self, tally: ByteTally) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            tally: Some(tally),
        }
    }

    /// The batch accumulator carried by this handle, when the caller is
    /// operating on a metered account view. EWS composes `AccountNet`
    /// directly, so its buffered funnel enrolls in the same accumulator
    /// through this accessor rather than passing through a REST funnel.
    pub(crate) fn batch_tally(&self) -> Option<ByteTally> {
        self.tally.clone()
    }

    /// The one place a Graph blob byte stream is opened. Production is the
    /// `AccountNet` download (retry, rate limit, bandwidth metering, the
    /// `Range` / `206` contract); tests script the chunk sequence or the
    /// open failure.
    pub(crate) async fn download_stream(
        &self,
        url: &AdmittedUrl,
        range: Option<bifrost_types::ByteRange>,
    ) -> Result<bifrost_net::ByteStream, GraphError> {
        let url = url.as_str();
        #[cfg(test)]
        self.record_download(url, range);
        let account_net = self.attached_net()?;
        account_net
            .download_stream(url, range)
            .await
            .map_err(GraphError::Net)
    }

    /// The transport a wire call should use: the scripted one under test
    /// when a script is installed, otherwise this client's own attached
    /// `AccountNet`. In production this is always the latter.
    ///
    /// An unattached client is unreachable: `GraphAccountFactory::open`
    /// attaches before any `GraphAccount` exists, `with_account_net` fills
    /// the slot at construction, derived clients copy a filled slot, and
    /// nothing empties it. So an empty or poisoned slot is the client's own
    /// invariant failing (`GraphError::Internal`), and it must not be
    /// dressed as a network fault: a retry of the same request never
    /// re-attaches, so a retryable classification could only loop.
    fn attached_net(&self) -> Result<AccountNet, GraphError> {
        #[cfg(test)]
        if let Some(scripted) = self.scripted_net() {
            return Ok(scripted);
        }
        match self.inner.account_net.read() {
            Ok(slot) => slot.clone().ok_or_else(|| GraphError::Internal {
                message: "Graph client is not attached to an account".to_string(),
            }),
            Err(_) => Err(GraphError::Internal {
                message: "Graph client account_net lock poisoned".to_string(),
            }),
        }
    }

    #[cfg(test)]
    fn record_aux(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        bearer: bool,
        body: &Bytes,
    ) {
        let mut scripted = self.inner.scripted.lock().expect("REST script lock");
        if scripted.wire.is_none() {
            return;
        }
        scripted.aux_requests.push(AuxRequest {
            method: method.to_string(),
            url: url.to_string(),
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect(),
            bearer,
            body: body.clone(),
        });
    }

    #[cfg(test)]
    fn record_download(&self, url: &str, range: Option<bifrost_types::ByteRange>) {
        let mut scripted = self.inner.scripted.lock().expect("REST script lock");
        if scripted.wire.is_none() {
            return;
        }
        scripted.download_requests.push(DownloadRequest {
            url: url.to_string(),
            range,
        });
    }

    #[cfg(test)]
    pub(crate) fn script_aux(&self, responses: impl IntoIterator<Item = ScriptedRestResponse>) {
        self.script_wire(
            responses.into_iter().map(ScriptedRestResponse::into_canned),
            RetryPolicy::disabled(),
        );
    }

    /// Script aux responses that never arrive: the request is dispatched and
    /// then waits forever, for a test that needs a deadline or a shutdown to
    /// land while a request is outstanding.
    #[cfg(test)]
    pub(crate) fn script_aux_pending(&self, count: usize) {
        self.script_wire(
            std::iter::repeat_with(|| bifrost_net::test_support::Canned::Pending).take(count),
            RetryPolicy::disabled(),
        );
    }

    #[cfg(test)]
    pub(crate) fn take_aux_requests(&self) -> Vec<AuxRequest> {
        std::mem::take(
            &mut self
                .inner
                .scripted
                .lock()
                .expect("REST script lock")
                .aux_requests,
        )
    }

    #[cfg(test)]
    pub(crate) fn script_downloads(&self, downloads: impl IntoIterator<Item = ScriptedDownload>) {
        self.script_wire(
            downloads.into_iter().map(ScriptedDownload::into_canned),
            RetryPolicy::disabled(),
        );
    }

    #[cfg(test)]
    pub(crate) fn take_download_requests(&self) -> Vec<DownloadRequest> {
        std::mem::take(
            &mut self
                .inner
                .scripted
                .lock()
                .expect("REST script lock")
                .download_requests,
        )
    }

    /// Record a REST request in this crate's shape, which is richer than
    /// the transport's `RequestSnapshot`: the JSON body arrives parsed,
    /// and `If-Match` / `Prefer` are lifted out of the header bag.
    ///
    /// This does not answer the request. The response comes from the
    /// scripted transport, so an exhausted script panics inside
    /// bifrost-net rather than reaching the network - the same
    /// hermeticity guarantee the local queue used to provide, now
    /// enforced one layer down for every net-riding crate.
    #[cfg(test)]
    fn record_wire(
        &self,
        method: &str,
        url: &str,
        if_match: Option<&str>,
        prefer: Option<&str>,
        body: Option<&WireBody>,
    ) {
        let mut scripted = self.inner.scripted.lock().expect("REST script lock");
        if scripted.wire.is_none() {
            return;
        }
        let is_json = body.is_none_or(|body| body.content_type == JSON_CONTENT_TYPE);
        scripted.requests.push(RestRequest {
            method: method.to_string(),
            url: url.to_string(),
            if_match: if_match.map(str::to_string),
            prefer: prefer.map(str::to_string),
            content_type: body
                .map_or(JSON_CONTENT_TYPE, |body| body.content_type)
                .to_string(),
            body: body.filter(|_| is_json).map(|body| {
                serde_json::from_slice(body.bytes.as_ref()).expect("Graph JSON body round-trips")
            }),
            raw_body: body.filter(|_| !is_json).map(|body| body.bytes.clone()),
        });
    }

    /// Install (or extend) the scripted wire transport backing this
    /// client's REST and aux surfaces.
    ///
    /// Both surfaces share one dispatcher because they share one wire:
    /// appending keeps a single total order, which is the order the
    /// server would see. A test scripting both must therefore list its
    /// responses in the order the requests actually go out.
    #[cfg(test)]
    fn script_wire(
        &self,
        canned: impl IntoIterator<Item = bifrost_net::test_support::Canned>,
        retry: RetryPolicy,
    ) {
        use bifrost_net::test_support::ScriptedDispatch;

        let mut scripted = self.inner.scripted.lock().expect("REST script lock");
        if let Some(wire) = scripted.wire.as_ref() {
            wire.extend(canned);
            return;
        }
        let wire = ScriptedDispatch::new(canned);
        let account_net = bifrost_net::test_support::scripted_account(
            &wire,
            bifrost_net::NetConfig::default(),
            Vec::new(),
            Arc::clone(&self.inner.token_source),
            retry,
        );
        scripted.wire = Some(wire);
        scripted.wire_net = Some(account_net);
    }

    /// Script with the retry policy production installs, for a test that
    /// wants to pin the retry loop itself - how many attempts a status
    /// costs, and whether a transient failure is recovered from without
    /// the caller ever seeing it.
    ///
    /// The default (`script_rest` / `script_aux`) disables retries so that
    /// one scripted response answers one request, which is what a test
    /// pinning WHICH outcome a status produces wants. Retrying by default
    /// would silently consume a later leg's response.
    #[cfg(test)]
    pub(crate) fn script_rest_with_retries(
        &self,
        responses: impl IntoIterator<Item = ScriptedRestResponse>,
    ) {
        self.script_wire(
            responses.into_iter().map(ScriptedRestResponse::into_canned),
            RetryPolicy::default(),
        );
    }

    /// How many requests the scripted transport has actually seen on the
    /// wire, across every surface.
    ///
    /// This is the transport's own count, not the funnel's: a retried
    /// attempt is a wire request but not a new funnel call, so this is the
    /// only place the retry loop's behavior is observable from this crate.
    #[cfg(test)]
    pub(crate) fn wire_attempts(&self) -> usize {
        self.inner
            .scripted
            .lock()
            .expect("REST script lock")
            .wire
            .as_ref()
            .map_or(0, |wire| wire.requests().len())
    }

    /// The scripted transport, when one is installed. Resolved through the
    /// SHARED scripting state rather than this client's own `account_net`
    /// slot, so a client derived after the script was installed still
    /// answers from it.
    #[cfg(test)]
    fn scripted_net(&self) -> Option<AccountNet> {
        self.inner
            .scripted
            .lock()
            .expect("REST script lock")
            .wire_net
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn script_rest(&self, responses: impl IntoIterator<Item = ScriptedRestResponse>) {
        self.script_wire(
            responses.into_iter().map(ScriptedRestResponse::into_canned),
            RetryPolicy::disabled(),
        );
    }

    #[cfg(test)]
    pub(crate) fn take_rest_requests(&self) -> Vec<RestRequest> {
        std::mem::take(
            &mut self
                .inner
                .scripted
                .lock()
                .expect("REST script lock")
                .requests,
        )
    }
}

/// Where a request that could not name its own target goes: an unusable
/// configured base is the consumer's configuration, so it is a malformed
/// request, not a transport or provider condition.
fn configuration_refusal(refusal: Refusal) -> GraphError {
    GraphError::Configuration {
        message: refusal.to_string(),
    }
}

/// The rate-limit bucket host. An unusable base falls back to the production
/// Graph host only for bookkeeping: no request can be admitted against it.
fn host_from_api_base(api_base: &Base) -> String {
    api_base
        .url()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| GRAPH_HOST.to_string())
}

/// Derive the Autodiscover / EWS origin from the Graph api-base.
///
/// A base on the production Graph host means production Microsoft, so
/// Autodiscover and EWS keep their own production origin
/// (`outlook.office365.com`) - they are a different host from Graph and
/// must not be rewritten to it. Any OTHER host means the api-base was
/// redirected (a harness mock, a sovereign cloud), and the same origin is
/// the only sane target for the sibling surfaces: a harness that redirects
/// Graph but leaves Autodiscover/EWS pointing at the real
/// `outlook.office365.com` cannot exercise the public-folder or EWS-streaming
/// legs at all.
///
/// An UNUSABLE api-base derives an unusable Outlook origin rather than
/// falling back to production: a consumer that misconfigured Graph must not
/// be left with Autodiscover and EWS sending its bearer to the real
/// `outlook.office365.com`. `with_outlook_base` can still name one
/// explicitly.
fn derive_outlook_base(api_base: &Base) -> Base {
    let Some(url) = api_base.url() else {
        return Base::invalid(
            api_base.as_str(),
            "Outlook origin derived from an unusable Graph api-base",
        );
    };
    if url.host_str() == Some(GRAPH_HOST) {
        return Base::parse(OUTLOOK_BASE);
    }
    // `validate` guarantees a host on a usable base.
    Base::parse(url.origin().ascii_serialization())
}

/// Parse a Graph JSON success response. On a non-success status,
/// constructs a `GraphResponseError` from the raw response so the
/// account boundary receives structured evidence, not a formatted
/// string.
///
/// The reachable non-success statuses here are the 3xx `bifrost-net`
/// passes through (304 on a conditional read, and a redirect no one can
/// follow); 4xx and 5xx never arrive as a response at all, because the
/// transport converts them into `bifrost_net::Error` first. Those are
/// re-decoded against Graph's error envelope at the account boundary
/// (`graph_error::into_account_error`), not here.
fn parse_json_response<T: DeserializeOwned>(response: RestResponse) -> Result<T, GraphError> {
    let status = response.status;
    let RestResponse { headers, body, .. } = response;
    if !status.is_success() {
        let err = GraphResponseError::from_response(status, headers, body);
        return Err(GraphError::Response(err));
    }

    // `GraphError::Json` is shared with the Autodiscover XML and OneDrive
    // upload readers, and its diagnostic label names no format, so the
    // message says which body failed.
    serde_json::from_slice(body.as_ref()).map_err(|e| GraphError::Json {
        message: format!("Graph REST JSON body: {e}"),
        body: if body.is_empty() { None } else { Some(body) },
    })
}

/// Check a Graph response for success status. On failure, constructs
/// a `GraphResponseError` from the raw response.
fn check_response_status(response: RestResponse) -> Result<(), GraphError> {
    let status = response.status;
    if status.is_success() {
        return Ok(());
    }
    let body = Bytes::copy_from_slice(response.body.as_ref());
    let err = GraphResponseError::from_response(status, response.headers, body);
    Err(GraphError::Response(err))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `Json` diagnostic label is format-neutral because Autodiscover
    /// XML and OneDrive upload failures share the variant, so a Graph REST
    /// body that does not decode must say so itself. It used to carry only
    /// serde's bare message, naming neither Graph nor JSON.
    #[test]
    fn a_graph_rest_parse_failure_names_graph_json() {
        let response = RestResponse {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(b"{not json"),
        };
        match parse_json_response::<serde_json::Value>(response) {
            Err(GraphError::Json { message, body }) => {
                assert!(message.starts_with("Graph REST JSON body: "), "{message}");
                assert_eq!(body.as_deref(), Some(&b"{not json"[..]));
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn trims_api_base() {
        let client = GraphClient::with_api_base("https://example.test/v1.0/", "token");
        assert_eq!(client.api_base(), "https://example.test/v1.0");
        assert_eq!(client.access_token().await, "token");
    }

    #[test]
    fn harness_api_base_redirects_autodiscover_and_ews() {
        // A harness pointing the Graph api-base at its mock must get the
        // Autodiscover + EWS surfaces redirected too: in production they
        // live on `outlook.office365.com`, a different host from Graph, so
        // an api-base-only override left them hitting the real service.
        let harness = GraphClient::with_api_base("http://127.0.0.1:8181/v1.0", "token");
        assert_eq!(harness.outlook_base(), "http://127.0.0.1:8181");
        assert_eq!(
            crate::ews::ews_url(harness.outlook_base()),
            "http://127.0.0.1:8181/EWS/Exchange.asmx"
        );
        assert_eq!(
            crate::account::autodiscover_soap_url(harness.outlook_base()),
            "http://127.0.0.1:8181/autodiscover/autodiscover.svc"
        );
        assert_eq!(
            crate::account::autodiscover_xml_url(harness.outlook_base()),
            "http://127.0.0.1:8181/autodiscover/autodiscover.xml"
        );

        // The production Graph base keeps the production Outlook origin -
        // Autodiscover/EWS are NOT on the Graph host.
        let production = GraphClient::new("token");
        assert_eq!(production.outlook_base(), OUTLOOK_BASE);
        assert_eq!(
            crate::ews::ews_url(production.outlook_base()),
            "https://outlook.office365.com/EWS/Exchange.asmx"
        );

        // A shared-mailbox client inherits the override.
        assert_eq!(
            harness
                .for_shared_mailbox("shared@contoso.com")
                .outlook_base(),
            "http://127.0.0.1:8181"
        );

        // An explicit override wins over the derivation.
        assert_eq!(
            production
                .with_outlook_base("https://ews.test/")
                .outlook_base(),
            "https://ews.test"
        );
    }

    #[tokio::test]
    async fn rotated_token_source_is_read() {
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let client = GraphClient::with_source(GRAPH_API_BASE, Arc::new(source.clone()));
        assert_eq!(client.access_token().await, "old-token");
        source.set(AccessToken::new("new-token", None));
        assert_eq!(client.access_token().await, "new-token");
    }

    #[test]
    fn api_path_prefix_returns_me_for_primary_mailbox() {
        let client = GraphClient::new("token");
        assert_eq!(client.api_path_prefix(), "/me");
        assert!(!client.is_shared_mailbox());
    }

    #[test]
    fn for_shared_mailbox_creates_scoped_client() {
        let client = GraphClient::new("token");
        let scoped = client.for_shared_mailbox("shared@example.com");
        assert_eq!(scoped.api_path_prefix(), "/users/shared%40example.com");
        assert_eq!(scoped.mailbox_id(), Some("shared@example.com"));
        assert!(scoped.is_shared_mailbox());
    }

    #[test]
    fn non_versioned_api_base_keeps_its_path() {
        let client = GraphClient::with_api_base("http://127.0.0.1:8181/graph", "token");
        assert_eq!(client.api_base(), "http://127.0.0.1:8181/graph");
        // The Outlook origin, by contrast, correctly follows the redirect.
        assert_eq!(client.outlook_base(), "http://127.0.0.1:8181");
    }

    /// A client with no attached transport is the client's own invariant
    /// failing, on every send path: it used to be minted as a
    /// `Network(Unsent)` error, which the engine retried as a network fault
    /// forever, though a retry of the same request never attaches anything.
    #[tokio::test]
    async fn an_unattached_client_is_an_internal_failure_on_every_send_path() {
        let client = GraphClient::new("token");
        let url = client.api_url("/me").expect("api url");
        let is_internal = |error: GraphError| matches!(error, GraphError::Internal { .. });

        assert!(is_internal(
            client
                .get::<serde_json::Value>(&url)
                .await
                .expect_err("rest")
        ));
        assert!(is_internal(
            client
                .execute_aux("POST", AuxTarget::Bearer(&url), &[], Bytes::new(), None)
                .await
                .err()
                .expect("aux")
        ));
        assert!(is_internal(
            client
                .download_stream(&url, None)
                .await
                .err()
                .expect("download")
        ));
    }

    /// Nothing closes the request semaphore, so a closed one is the client's
    /// invariant too, not a network fault.
    #[tokio::test]
    async fn a_closed_request_semaphore_is_an_internal_failure() {
        let client = GraphClient::new("token");
        client.script_rest([]);
        client.inner.semaphore.close();
        let url = client.api_url("/me").expect("api url");
        assert!(matches!(
            client.get::<serde_json::Value>(&url).await,
            Err(GraphError::Internal { .. })
        ));
        assert_eq!(client.wire_attempts(), 0);
    }

    #[test]
    fn api_url_joins_paths_and_admits_only_the_api_origin() {
        // The `@odata.nextLink` / `@odata.deltaLink` walk feeds absolute
        // URLs back in; rewriting them onto the api-base would break
        // pagination, so a same-origin absolute URL passes through. One on
        // any other origin is refused: the request would carry the bearer.
        let client = GraphClient::with_api_base("https://x/v1.0", "token");
        assert_eq!(
            client.api_url("/me/messages").expect("path").as_str(),
            "https://x/v1.0/me/messages"
        );
        assert_eq!(
            client.api_url("me/messages").expect("bare path").as_str(),
            "https://x/v1.0/me/messages"
        );
        assert_eq!(
            client
                .api_url("https://x/v1.0/next?$skiptoken=a")
                .expect("same origin")
                .as_str(),
            "https://x/v1.0/next?$skiptoken=a"
        );
        assert!(matches!(
            client.api_url("https://graph.example/next?$skiptoken=a"),
            Err(GraphError::Configuration { .. })
        ));
        assert!(matches!(
            client.admit_link(&ProviderLink::for_tests("http://x/v1.0/next")),
            Err(GraphError::ProviderLinkRefused { .. })
        ));
    }

    #[test]
    fn rate_limit_host_tracks_the_api_base_host() {
        // The per-host token bucket must follow a redirected base, or a
        // harness run would meter against `graph.microsoft.com`.
        let host = |base: &str| host_from_api_base(&Base::parse(base));
        assert_eq!(host("https://graph.microsoft.com/v1.0"), GRAPH_HOST);
        assert_eq!(host("http://127.0.0.1:8181/v1.0"), "127.0.0.1");
        // An unparseable base falls back to production rather than panicking.
        assert_eq!(host("not a url"), GRAPH_HOST);
    }

    /// nc-9's shape: `with_account_net` was the one constructor that
    /// hardcoded `rate_limit_host` to the production Graph host, so an
    /// injected net against a redirected base metered under the wrong
    /// bucket. It must derive from the supplied base like the others.
    #[test]
    fn with_account_net_derives_the_rate_limit_host_from_its_base() {
        let donor = GraphClient::new("token");
        donor.attach_account(AccountId("with-account-net-host-probe".to_string()));
        let net = donor.account_net().expect("account net attached");
        let client = GraphClient::with_account_net(
            net,
            "http://127.0.0.1:8181/v1.0",
            Arc::new(StaticTokenSource::new("token", None)),
        );
        assert_eq!(client.inner.rate_limit_host, "127.0.0.1");
    }

    #[test]
    fn outlook_base_keeps_the_scheme_and_port_of_a_redirected_api_base() {
        let derive = |base: &str| derive_outlook_base(&Base::parse(base));
        assert_eq!(
            derive("https://graph.contoso-cloud.test/v1.0").as_str(),
            "https://graph.contoso-cloud.test"
        );
        assert_eq!(
            derive("http://127.0.0.1:8181/v1.0").as_str(),
            "http://127.0.0.1:8181"
        );
        // An unusable Graph base derives an unusable Outlook origin rather
        // than falling back to production: the fallback would have left
        // Autodiscover and EWS sending the bearer to the real service.
        assert!(derive("nonsense").url().is_none());
        assert!(
            GraphClient::with_api_base("nonsense", "token")
                .ews_url()
                .is_err()
        );
        // An explicit Outlook origin still repairs it.
        assert!(
            GraphClient::with_api_base("nonsense", "token")
                .with_outlook_base("https://ews.test")
                .ews_url()
                .is_ok()
        );
    }

    #[test]
    fn shared_mailbox_prefix_percent_encodes_the_routing_key() {
        // The routing key is an SMTP address; `@` and any `+` tag must not
        // leak into the path unencoded.
        let client = GraphClient::new("token").for_shared_mailbox("a+tag@contoso.com");
        let prefix = client.api_path_prefix();
        assert!(!prefix.contains('@'), "{prefix}");
        assert!(!prefix.contains('+'), "{prefix}");
        assert!(prefix.starts_with("/users/"), "{prefix}");
        // The stored key stays verbatim - it is the map key every routing
        // site (`client_for_owner`, `encode_foreign`) looks up.
        assert_eq!(client.mailbox_id(), Some("a+tag@contoso.com"));
        assert!(!client.uses_default_mailbox());
    }

    #[test]
    fn attach_account_uses_engine_account_id() {
        let client = GraphClient::new("token");
        client.attach_account(AccountId("engine-account".to_string()));
        let account_net = client.account_net().expect("account net attached");
        assert_eq!(
            account_net.account(),
            &AccountId("engine-account".to_string())
        );
    }

    /// Every reopen calls `attach_account` again with the same engine
    /// id. `Net` mints a fresh registration token per attach and no
    /// longer unregisters a previous one, so the displaced handle has
    /// to be detached explicitly or the host bucket leaks an attach
    /// count per cycle and is never reclaimed.
    /// `GraphClient::new` rides the process-wide `Net`, so this keys
    /// on a meter entry for an id no other test touches rather than on
    /// the shared `GRAPH_HOST` bucket.
    #[test]
    fn reattaching_the_same_engine_id_does_not_leak_registrations() {
        let client = GraphClient::new("token");
        let id = AccountId("graph-reattach-leak-probe".to_string());
        let net = client.inner.net.clone().expect("default client owns a Net");

        for _ in 0..3 {
            client.attach_account(id.clone());
        }
        bifrost_net::MeterSink::record_bytes_in(net.meter(), &id, 5);
        assert_eq!(net.meter().account(id.clone()).bytes_in(), 5);

        client.account_net().expect("account net attached").detach();

        assert_eq!(
            net.meter().account(id).bytes_in(),
            0,
            "three attaches must leave exactly one live registration to detach"
        );
    }

    #[tokio::test]
    async fn scripted_rest_records_the_graph_owned_request_and_returns_its_response() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"displayName":"Ada"}),
        )]);

        let profile = client.get_profile().await.expect("scripted profile");
        assert_eq!(profile.display_name.as_deref(), Some("Ada"));
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert!(
            requests[0]
                .url
                .ends_with("/me?$select=displayName,mail,userPrincipalName")
        );
        assert_eq!(requests[0].if_match, None);
        assert_eq!(requests[0].prefer, None);
        assert_eq!(requests[0].body, None);

        let response = ScriptedRestResponse::empty(reqwest::StatusCode::TOO_MANY_REQUESTS)
            .with_header("Retry-After", "5");
        assert_eq!(response.headers["retry-after"], "5");
    }

    /// Issue one GET against a scripted status and return what the
    /// caller actually sees. Every response here travels the production
    /// retry loop, so these outcomes are the transport's, not a local
    /// restatement of them.
    async fn outcome_for(
        responses: impl IntoIterator<Item = ScriptedRestResponse>,
    ) -> Result<RestResponse, GraphError> {
        let client = GraphClient::new("token");
        client.script_rest(responses);
        let url = client.api_url("/me").expect("api url");
        client.execute_wire(&url, "GET", None, None, None).await
    }

    /// `bifrost-net` returns `Ok(Response)` for 2xx and a passed-through
    /// 3xx ONLY; every other status leaves its retry loop as a typed
    /// `Error`. This used to assert against a local reimplementation of
    /// that rule, which could agree with itself while disagreeing with
    /// the transport. It now drives the transport.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_scripted_status_takes_the_shape_bifrost_net_produces() {
        use reqwest::StatusCode;

        assert!(
            outcome_for([ScriptedRestResponse::empty(StatusCode::OK)])
                .await
                .is_ok()
        );
        // A 304 on a conditional read is passed through as a response.
        assert!(
            outcome_for([ScriptedRestResponse::empty(StatusCode::NOT_MODIFIED)])
                .await
                .is_ok()
        );

        match outcome_for([ScriptedRestResponse::empty(StatusCode::NOT_FOUND)]).await {
            Err(GraphError::Net(bifrost_net::Error::Status { code, .. })) => {
                assert_eq!(code, StatusCode::NOT_FOUND);
            }
            _ => panic!("a terminal 4xx is Error::Status"),
        }

        match outcome_for([ScriptedRestResponse::empty(StatusCode::TOO_MANY_REQUESTS)
            .with_header("Retry-After", "5")])
        .await
        {
            Err(GraphError::Net(bifrost_net::Error::RateLimited {
                retry_after,
                final_response,
            })) => {
                assert_eq!(retry_after, Some(std::time::Duration::from_secs(5)));
                assert_eq!(final_response.status, StatusCode::TOO_MANY_REQUESTS);
            }
            _ => panic!("429 past the budget is Error::RateLimited"),
        }

        match outcome_for([ScriptedRestResponse::empty(
            StatusCode::INTERNAL_SERVER_ERROR,
        )])
        .await
        {
            Err(GraphError::Net(bifrost_net::Error::RetryBudgetExhausted {
                final_response: Some(final_response),
                ..
            })) => assert_eq!(final_response.status, StatusCode::INTERNAL_SERVER_ERROR),
            _ => panic!("5xx past the budget is Error::RetryBudgetExhausted"),
        }
    }

    /// A fact the local simulation could not have told anyone, because it
    /// answered every request from a single scripted entry: a 401 does not
    /// become `AuthLost` on its own. bifrost-net forces a token refresh and
    /// reissues the request on a budget SEPARATE from `max_attempts`, so
    /// the caller sees `AuthLost` only when the second attempt is also
    /// rejected - and a Graph path that meets a transient 401 recovers
    /// without ever surfacing an error.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_401_is_retried_after_a_forced_refresh_before_it_becomes_auth_lost() {
        use reqwest::StatusCode;

        let recovered = outcome_for([
            ScriptedRestResponse::empty(StatusCode::UNAUTHORIZED),
            ScriptedRestResponse::json(StatusCode::OK, serde_json::json!({"ok": true})),
        ])
        .await
        .expect("the refreshed retry succeeds");
        assert_eq!(recovered.status, StatusCode::OK);

        match outcome_for([
            ScriptedRestResponse::empty(StatusCode::UNAUTHORIZED),
            ScriptedRestResponse::empty(StatusCode::UNAUTHORIZED),
        ])
        .await
        {
            Err(GraphError::Net(bifrost_net::Error::AuthLost {
                transmission_state,
                final_response: Some(_),
            })) => assert_eq!(transmission_state, Some(TransmissionState::Acknowledged)),
            _ => panic!("a 401 surviving the forced refresh is Error::AuthLost"),
        }
    }

    /// What no Graph-local seam could reach, and what graph-T1 was filed
    /// for: the retry loop runs BELOW this crate's funnel, so a seam that
    /// answered at the funnel could pin which outcome a status produced
    /// but never how many attempts it cost. Scripting at the wire makes
    /// the attempt count observable - one funnel call, two wire requests,
    /// and a transient 503 the caller never sees.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_transient_5xx_is_retried_below_the_graph_funnel() {
        use reqwest::StatusCode;

        let client = GraphClient::new("token");
        client.script_rest_with_retries([
            ScriptedRestResponse::empty(StatusCode::SERVICE_UNAVAILABLE)
                .with_header("Retry-After", "0"),
            ScriptedRestResponse::json(StatusCode::OK, serde_json::json!({"ok": true})),
        ]);

        let url = client.api_url("/me").expect("api url");
        let response = client
            .execute_wire(&url, "GET", None, None, None)
            .await
            .expect("the retried 503 succeeds on the second attempt");

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            client.wire_attempts(),
            2,
            "the transport made two wire attempts"
        );
        assert_eq!(
            client.take_rest_requests().len(),
            1,
            "the retry is invisible at the Graph funnel - it issued one request"
        );
    }

    /// A script that runs out must fail the test at the request that
    /// exceeded it. Falling through would put a real socket behind an
    /// unexpected request and leave it out of the recorded list, so a test
    /// asserting "exactly N requests" could not see the N+1th.
    ///
    /// The panic now comes from bifrost-net's dispatcher rather than a
    /// Graph-local queue, which is the point of the consolidation: the
    /// guarantee is enforced once, for every crate riding the transport.
    #[tokio::test]
    #[should_panic(expected = "scripted dispatch exhausted")]
    async fn an_exhausted_script_fails_loudly_instead_of_reaching_the_network() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({}),
        )]);
        let _ = client.get_profile().await;
        let _ = client.get_profile().await;
    }
}
