use std::{
    collections::HashSet,
    net::IpAddr,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bifrost_net::{AccountId as NetAccountId, StaticTokenSource, TokenSource};
use reqwest::header;

use crate::{
    blob,
    core::{
        request::{self, Request},
        response,
        session::{Session, URLPart},
        transport::HttpTransport,
    },
    transport_reqwest::ReqwestTransport,
};

const DEFAULT_TIMEOUT_MS: u64 = 10 * 1000;

#[non_exhaustive]
pub(crate) enum Credentials {
    Basic(String),
    Bearer(Arc<dyn TokenSource>),
}

#[derive(Clone)]
pub(crate) enum Authorization {
    Basic(String),
    Bearer(Arc<dyn TokenSource>),
}

impl Authorization {
    fn from_credentials(credentials: Credentials) -> Self {
        match credentials {
            Credentials::Basic(value) => Self::Basic(value),
            Credentials::Bearer(token) => {
                // HTTP and WebSocket retain this exact coordinating handle.
                // Net preserves it rather than wrapping a second cache.
                let source = if token.is_coordinating() {
                    token
                } else {
                    Arc::new(bifrost_net::OAuthRefresher::new(token)) as Arc<dyn TokenSource>
                };
                Self::Bearer(source)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn from_credentials_for_test(credentials: Credentials) -> Self {
        Self::from_credentials(credentials)
    }

    /// Build the `Authorization` header value. For the bearer case this
    /// reads the coordinating `source.current()` so a token rotated on the shared source
    /// is presented live; only the WebSocket handshake (which builds its
    /// own header) and the Basic path take this route - the HTTP request
    /// pipeline injects the bearer through `AccountNet` instead.
    pub(crate) async fn header_value(&self) -> Result<String, bifrost_net::Error> {
        match self {
            Self::Basic(value) => Ok(format!("Basic {value}")),
            Self::Bearer(source) => {
                let token = source.current().await?;
                Ok(format!("Bearer {}", token.as_str()))
            }
        }
    }

    /// Return a concrete source handle for callers that require one. Basic
    /// transport setup must use the optional accessor below so it attaches
    /// no bearer source.
    pub(crate) fn account_token_source(&self) -> Arc<dyn TokenSource> {
        match self {
            // This inert handle is not attached to Basic transport accounts.
            Self::Basic(_) => Arc::new(StaticTokenSource::new("", None)),
            Self::Bearer(source) => Arc::clone(source),
        }
    }

    pub(crate) fn optional_account_token_source(&self) -> Option<Arc<dyn TokenSource>> {
        match self {
            // Basic requests supply their own Authorization header and
            // disable bearer injection, so they have no token source.
            Self::Basic(_) => None,
            Self::Bearer(_) => Some(self.account_token_source()),
        }
    }

    pub(crate) fn uses_bearer_pipeline(&self) -> bool {
        matches!(self, Self::Bearer(_))
    }
}

/// Everything a client derives from a [`Session`], held together so a
/// refresh swaps all of it at once.
///
/// RFC 8620 §2 lets any Session property change between fetches -
/// `apiUrl`, the upload / download / EventSource templates and
/// `primaryAccounts` included. Caching those next to, rather than with,
/// the `Session` let a refreshed session be observable through
/// `session()` while requests kept going to the endpoints and account of
/// the session it replaced. One `Arc<SessionState>` behind one lock makes
/// the swap atomic: a reader either sees the whole old session or the
/// whole new one.
pub(crate) struct SessionState {
    session: Arc<Session>,
    api_url: String,
    upload_url: Vec<URLPart<blob::URLParameter>>,
    download_url: Vec<URLPart<blob::URLParameter>>,
    /// Parsed with the session, but a template that fails is stored as its
    /// error detail and only raised when EventSource is actually used.
    /// Nothing in the sync `Account` uses EventSource, so refusing the
    /// whole session over it made a server whose EventSource template this
    /// crate cannot parse impossible to open at all. The blob templates stay
    /// eager because the account uses them throughout.
    event_source_url: Result<Vec<URLPart<crate::event_source::URLParameter>>, String>,
    default_account_id: crate::core::id::AccountId,
}

/// Parse one of the session's URI templates. Every template a session
/// carries is the server's, so one that does not parse is the provider's
/// malformed session, named by the property that carried it.
///
/// A template that parses but is not an absolute `http` / `https` URL is
/// refused here too. RFC 8620 §2 defines each of these properties as a URL
/// (template) the client fetches, and a relative one such as
/// `not-a-url/{accountId}` otherwise passed and only failed later, inside
/// the transport, as a network-shaped error that blamed neither the server
/// nor the property.
fn session_template<P: crate::core::session::URLParser>(
    property: &'static str,
    url: &str,
) -> crate::Result<Vec<URLPart<P>>> {
    parse_session_template(url)
        .map_err(|detail| crate::Error::MalformedSessionUrl { property, detail })
}

/// [`session_template`] without the property attached: the bare detail,
/// for a template whose failure is stored and raised later.
fn parse_session_template<P: crate::core::session::URLParser>(
    url: &str,
) -> Result<Vec<URLPart<P>>, String> {
    URLPart::parse(url).and_then(|parts| require_absolute_http(url, &parts).map(|()| parts))
}

/// Check that a parsed template expands to an absolute `http` / `https` URL
/// with a host.
///
/// The check runs on a probe expansion rather than on the raw template, so
/// it cannot refuse a legitimate RFC 6570 level-1 template: every variable
/// `URLPart` accepts (`accountId`, `blobId`, `name`, `type`, `types`,
/// `closeafter`, `ping`) is replaced by one unreserved character, which is
/// valid in a host label, a path segment and a query alike. JMAP servers put
/// variables in the path and query, but a template whose host label is a
/// variable (`https://{accountId}.example/`) is still absolute after
/// expansion and still passes. Parsing uses the same WHATWG parser the
/// transport (reqwest) hands URLs to, so what passes here is what the
/// transport can address.
fn require_absolute_http<P: crate::core::session::URLParser>(
    template: &str,
    parts: &[URLPart<P>],
) -> Result<(), String> {
    let mut probe = String::with_capacity(template.len());
    for part in parts {
        match part {
            URLPart::Value(value) => probe.push_str(value),
            URLPart::Parameter(_) => probe.push('x'),
        }
    }
    require_absolute_http_url(template, &probe)
}

/// Check that `url` is an absolute `http` / `https` URL with a host.
/// `shown` is what the error quotes: the template a probe was expanded
/// from, or the URL itself.
fn require_absolute_http_url(shown: &str, url: &str) -> Result<(), String> {
    let parsed =
        reqwest::Url::parse(url).map_err(|e| format!("not an absolute URL ({e}): {shown}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!(
            "scheme '{}' is not http or https: {shown}",
            parsed.scheme()
        ));
    }
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(format!("no host: {shown}"));
    }
    Ok(())
}

impl SessionState {
    fn derive(session: Session) -> crate::Result<Self> {
        let default_account_id = session
            .default_account_id()
            .map(crate::core::id::AccountId::new)
            .unwrap_or_else(|| crate::core::id::AccountId::new(""));

        // `apiUrl` is a plain URL, not a template, but the same rule holds:
        // it is the server's, and a relative or non-http one otherwise
        // failed later inside the transport, blaming nobody.
        let api_url = session.api_url();
        require_absolute_http_url(api_url, api_url).map_err(|detail| {
            crate::Error::MalformedSessionUrl {
                property: "apiUrl",
                detail,
            }
        })?;

        Ok(Self {
            api_url: api_url.to_string(),
            upload_url: session_template("uploadUrl", session.upload_url())?,
            download_url: session_template("downloadUrl", session.download_url())?,
            event_source_url: parse_session_template(session.event_source_url()),
            default_account_id,
            session: Arc::new(session),
        })
    }

    pub(crate) fn session(&self) -> &Arc<Session> {
        &self.session
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.api_url
    }

    pub(crate) fn upload_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.upload_url
    }

    pub(crate) fn download_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.download_url
    }

    /// The parsed EventSource template, or the session's malformed
    /// `eventSourceUrl` - raised here, at use, rather than when the session
    /// was derived.
    pub(crate) fn event_source_url(
        &self,
    ) -> crate::Result<&[URLPart<crate::event_source::URLParameter>]> {
        self.event_source_url
            .as_deref()
            .map_err(|detail| crate::Error::MalformedSessionUrl {
                property: "eventSourceUrl",
                detail: detail.clone(),
            })
    }

    pub(crate) fn default_account_id(&self) -> &crate::core::id::AccountId {
        &self.default_account_id
    }
}

/// Internal shared state of a [`Client`]. Stored behind an `Arc` so the
/// client itself is cheap to clone and pass around.
pub(crate) struct ClientInner<T: HttpTransport = ReqwestTransport> {
    transport: T,
    state: std::sync::Mutex<Arc<SessionState>>,
    /// Session fetch and install are one serialized operation. Without this
    /// gate, an earlier GET that completes last can overwrite a newer
    /// session fetched by an overlapping refresh.
    refresh_gate: futures::lock::Mutex<()>,
    session_url: String,
    session_updated: AtomicBool,
    /// Wakes the account layer's scope-lifecycle poller when a response
    /// reports a moved `sessionState`. Behind `tokio` because only that
    /// async layer listens, and `tokio` is an optional dependency of this
    /// crate; without it the divergence is still recorded in
    /// `session_updated`.
    #[cfg(feature = "tokio")]
    session_changes: tokio::sync::watch::Sender<u64>,

    pub(crate) timeout: Duration,
    #[cfg(feature = "websockets")]
    pub(crate) accept_invalid_certs: bool,

    #[cfg(feature = "websockets")]
    pub(crate) authorization: Authorization,
    #[cfg(feature = "websockets")]
    pub(crate) ws: tokio::sync::Mutex<Option<crate::client_ws::WsStream>>,
    /// RFC 8887 `requestId` counter for WebSocket requests.
    ///
    /// It lives on the CLIENT, not on the `WsStream`, so it survives a
    /// reconnect. Per-connection it restarted at 0, which meant a response
    /// arriving late from a previous connection carried an id the new
    /// connection was about to reuse - the one case where correlation by
    /// id is actively worse than none.
    #[cfg(feature = "websockets")]
    pub(crate) ws_request_id: std::sync::atomic::AtomicU64,
    /// Callers awaiting a WebSocket response, keyed by that `requestId`.
    ///
    /// On the CLIENT for the same reason the counter is: the map has to
    /// outlive any one connection so a reconnect can fail the waiters
    /// the dropped connection will never answer, instead of leaving them
    /// parked forever on a socket that is gone.
    #[cfg(feature = "websockets")]
    pub(crate) ws_pending: Arc<crate::client_ws::PendingRequests>,
}

/// A JMAP client. Cheap to clone - wraps an `Arc<ClientInner>` internally.
///
/// Cloning a `Client` shares the underlying transport, session state, and
/// (when enabled) WebSocket connection. There is no public lifetime
/// parameter, so `Client` can be stored in long-lived structs and moved
/// across tasks freely.
pub(crate) struct Client<T: HttpTransport = ReqwestTransport> {
    inner: Arc<ClientInner<T>>,
    /// Where this handle reports request-local inbound bytes, if
    /// anywhere. `None` on every ordinary client; a sync stream that
    /// needs per-batch accounting takes a metered handle via
    /// `metered()`. Kept OUTSIDE `ClientInner` deliberately: forking
    /// the inner would fork the session-state mutex, and two clients
    /// with independent session state is a correctness bug, not an
    /// accounting detail.
    tally: Option<ByteTally>,
}

/// Inbound payload bytes attributed to one engine batch.
///
/// An engine batch here is one `/jmap/api` POST carrying several
/// method calls, and a paged walk issues one per page, so the bytes
/// are summed on an accumulator owned by one stream rather than
/// sampled off the account-cumulative bandwidth meter - which is
/// shared by every concurrent request on the account and would
/// attribute another scope's traffic to this batch.
///
/// `take` reads and resets, so consecutive batches from one stream
/// partition the bytes rather than each reporting a running total.
#[derive(Clone, Default)]
pub(crate) struct ByteTally(Arc<std::sync::atomic::AtomicU64>);

impl ByteTally {
    fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Bytes recorded since the previous `take`.
    pub(crate) fn take(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}

impl<T: HttpTransport> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            tally: self.tally.clone(),
        }
    }
}

impl<T: HttpTransport> Deref for Client<T> {
    type Target = ClientInner<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(crate) struct ClientBuilder {
    credentials: Option<Credentials>,
    net_account_id: NetAccountId,
    net: Option<bifrost_net::Net>,
    trusted_hosts: HashSet<String>,
    forwarded_for: Option<String>,
    accept_invalid_certs: bool,
    timeout: Duration,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    pub(crate) fn new() -> Self {
        Self {
            credentials: None,
            net_account_id: NetAccountId("jmap".to_string()),
            net: None,
            trusted_hosts: HashSet::new(),
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            forwarded_for: None,
            accept_invalid_certs: false,
        }
    }

    pub(crate) fn credentials(mut self, credentials: impl Into<Credentials>) -> Self {
        self.credentials = Some(credentials.into());
        self
    }

    pub(crate) fn net_account_id(mut self, account_id: NetAccountId) -> Self {
        self.net_account_id = account_id;
        self
    }

    pub(crate) fn net(mut self, net: bifrost_net::Net) -> Self {
        self.net = Some(net);
        self
    }

    pub(crate) fn accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    pub(crate) fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub(crate) fn follow_redirects(
        mut self,
        trusted_hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.trusted_hosts
            .extend(trusted_hosts.into_iter().map(std::convert::Into::into));
        self
    }

    pub(crate) fn forwarded_for(mut self, ip: IpAddr) -> Self {
        self.forwarded_for = Some(match ip {
            IpAddr::V4(addr) => format!("for={addr}"),
            IpAddr::V6(addr) => format!("for=\"{addr}\""),
        });
        self
    }

    pub(crate) async fn connect(self, url: &str) -> crate::Result<Client> {
        // Both failures are impossible by construction - the only
        // production caller (the sync factory) always sets credentials, and
        // the forwarded-for value is formatted from an `IpAddr` - so they are
        // the client's own invariant, not a network fault to retry.
        let credentials = self.credentials.ok_or_else(|| {
            crate::core::transport::TransportError::invariant(
                "Missing credentials - call .credentials() before .connect()",
            )
        })?;
        let authorization = Authorization::from_credentials(credentials);
        let mut headers = header::HeaderMap::new();
        if let Some(forwarded_for) = self.forwarded_for {
            headers.insert(
                header::FORWARDED,
                header::HeaderValue::from_str(&forwarded_for).map_err(|e| {
                    crate::core::transport::TransportError::invariant_with_source(
                        "Invalid forwarded-for header",
                        e,
                    )
                })?,
            );
        }

        let trusted_hosts = Arc::new(self.trusted_hosts);

        let transport = ReqwestTransport::new_with_net(
            headers.clone(),
            authorization.clone(),
            self.net_account_id,
            self.timeout,
            self.accept_invalid_certs,
            Arc::clone(&trusted_hosts),
            self.net,
        )
        .map_err(crate::Error::from)?;

        let (session_url, session_bytes) = fetch_session(&transport, url)
            .await
            .map_err(crate::Error::from)?;
        let session: Session = serde_json::from_slice(&session_bytes)?;
        // Every installed session hands its limit to the transport, so the
        // server's bound holds across all of this client's concurrent API
        // work, not only within one call site's fan-out.
        transport.set_api_concurrency(session.api_request_concurrency());

        Ok(Client {
            tally: None,
            inner: Arc::new(ClientInner {
                state: std::sync::Mutex::new(Arc::new(SessionState::derive(session)?)),
                refresh_gate: futures::lock::Mutex::new(()),
                session_url,
                session_updated: true.into(),
                #[cfg(feature = "tokio")]
                session_changes: tokio::sync::watch::channel(0).0,
                timeout: self.timeout,
                transport,
                #[cfg(feature = "websockets")]
                accept_invalid_certs: self.accept_invalid_certs,
                #[cfg(feature = "websockets")]
                authorization,
                #[cfg(feature = "websockets")]
                ws: None.into(),
                #[cfg(feature = "websockets")]
                ws_request_id: std::sync::atomic::AtomicU64::new(0),
                #[cfg(feature = "websockets")]
                ws_pending: Arc::new(crate::client_ws::PendingRequests::new()),
            }),
        })
    }
}

/// Probe the RFC 8620 origin-rooted session URL first. Some path-mounted
/// deployments also expose discovery below the configured base path, so
/// retain that form only as a 404 fallback when the base has a path.
fn well_known_session_urls(url: &str) -> Vec<String> {
    let Ok(base) = reqwest::Url::parse(url) else {
        return vec![format!("{}/.well-known/jmap", url.trim_end_matches('/'))];
    };
    if base.cannot_be_a_base() {
        return vec![format!("{}/.well-known/jmap", url.trim_end_matches('/'))];
    }

    let configured_path = base.path().trim_end_matches('/').to_string();
    let mut origin = base.clone();
    origin.set_path("/.well-known/jmap");
    origin.set_query(None);
    origin.set_fragment(None);

    let mut urls = vec![origin.to_string()];
    if !configured_path.is_empty() {
        let mut path_fallback = base;
        path_fallback.set_path(&format!("{configured_path}/.well-known/jmap"));
        path_fallback.set_query(None);
        path_fallback.set_fragment(None);
        urls.push(path_fallback.to_string());
    }
    urls
}

fn is_not_found(error: &crate::core::transport::TransportError) -> bool {
    matches!(
        error.net.as_ref(),
        Some(bifrost_net::Error::Status { code, .. })
            if *code == reqwest::StatusCode::NOT_FOUND
    )
}

async fn fetch_session<T: HttpTransport>(
    transport: &T,
    configured_base: &str,
) -> Result<(String, bytes::Bytes), crate::core::transport::TransportError> {
    let mut urls = well_known_session_urls(configured_base).into_iter();
    let origin_url = urls
        .next()
        .expect("session discovery always has an origin URL");
    match transport.get_session(&origin_url).await {
        Ok(bytes) => Ok((origin_url, bytes)),
        Err(error) if is_not_found(&error) => match urls.next() {
            Some(path_url) => transport
                .get_session(&path_url)
                .await
                .map(|bytes| (path_url, bytes)),
            None => Err(error),
        },
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod session_state_tests {
    use std::collections::VecDeque;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use serde_json::json;
    use tokio::sync::{Notify, Semaphore};

    use super::{Client, Session, fetch_session, well_known_session_urls};
    use crate::core::transport::{HttpTransport, TransportError};

    fn session_json(tag: &str, account: &str, state: &str) -> String {
        json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {"urn:ietf:params:jmap:mail": account},
            "username": "user@example.test",
            "apiUrl": format!("https://{tag}.example.test/api"),
            "downloadUrl": format!("https://{tag}.example.test/dl/{{accountId}}/{{blobId}}/{{name}}/{{type}}"),
            "uploadUrl": format!("https://{tag}.example.test/upload/{{accountId}}"),
            "eventSourceUrl": format!("https://{tag}.example.test/es"),
            "state": state
        })
        .to_string()
    }

    /// RFC 8887 request ids must not repeat across WebSocket
    /// reconnects: a late response from the dropped connection would
    /// otherwise carry an id the new connection is about to reuse, which
    /// is worse than having no correlation at all. The counter therefore
    /// belongs to the CLIENT and not to the per-connection `WsStream`,
    /// which is reconstructed on every `connect_ws`.
    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_request_ids_do_not_restart() {
        let session: Session = serde_json::from_str(&session_json("old", "A1", "session-1"))
            .expect("session fixture parses");
        let client = Client::with_transport(
            RefreshingTransport {
                api_urls: Arc::new(Mutex::new(Vec::new())),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        let ids: Vec<String> = (0..3).map(|_| client.next_ws_request_id()).collect();
        assert_eq!(ids, vec!["0", "1", "2"]);
        // A reconnect replaces the `WsStream`; the counter is not in it,
        // so the sequence continues rather than restarting at 0.
        assert_eq!(client.next_ws_request_id(), "3");
    }

    /// A session advertising no `primaryAccounts` leaves the derived
    /// default account id empty. `build()` used to bake that into every
    /// generic request and the method structs serialized
    /// `"accountId": ""` - a malformed request answered with an opaque
    /// server error, for a fault that is entirely local. It must fail
    /// here instead, naming the capability whose account is missing.
    #[cfg(feature = "mail")]
    #[test]
    fn a_session_without_a_primary_account_refuses_to_build_a_request() {
        let session: Session = serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/api",
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/es",
            "state": "session-1"
        }))
        .expect("session parses");
        let client = Client::with_transport(
            RefreshingTransport {
                api_urls: Arc::new(Mutex::new(Vec::new())),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        assert_eq!(client.default_account_id().as_str(), "");

        let mut request = client.build();
        let error = request
            .call(crate::mailbox::MailboxGet::new())
            .err()
            .expect("an empty account id must not reach the wire");
        assert!(
            matches!(
                error,
                crate::Error::NoPrimaryAccount {
                    capability: "urn:ietf:params:jmap:mail"
                }
            ),
            "unexpected error: {error:?}"
        );
        // Nothing was appended, so the request cannot serialize the
        // malformed call either.
        let body = serde_json::to_string(&request).expect("request serializes");
        assert!(
            !body.contains("\"accountId\":\"\""),
            "empty account id must not be serialized: {body}"
        );
    }

    /// The upload / download templates and the API URL come from the
    /// server's session object, so one that is unusable is the provider's
    /// malformed session, reported under the property that carried it. It
    /// used to surface as the caller's `InvalidUrl`, which the account
    /// boundary maps to `Request(Malformed)` - "fix your request" for a
    /// request nobody made. (`eventSourceUrl` is deferred to use; see
    /// `a_malformed_event_source_template_fails_only_at_use`.)
    #[test]
    fn a_malformed_session_template_names_the_servers_property() {
        for (property, bad) in [
            ("uploadUrl", "https://example.test/upload/{accountId"),
            ("downloadUrl", "https://example.test/dl/{notAVariable}"),
            ("apiUrl", "jmap/api"),
            ("apiUrl", "ftp://example.test/api"),
        ] {
            let mut session =
                serde_json::from_str::<serde_json::Value>(&session_json("bad", "A1", "session-1"))
                    .expect("fixture is json");
            // The session property IS the JSON key the template came from.
            session[property] = json!(bad);
            let session: Session = serde_json::from_value(session).expect("session parses");

            let error = Client::with_transport(
                RefreshingTransport {
                    api_urls: Arc::new(Mutex::new(Vec::new())),
                },
                session,
                "https://example.test/.well-known/jmap",
            )
            .err()
            .expect("a malformed template must not build a client");
            match error {
                crate::Error::MalformedSessionUrl {
                    property: reported,
                    detail,
                } => {
                    assert_eq!(reported, property);
                    assert!(detail.contains(bad), "{property}: {detail}");
                }
                other => panic!("{property}: expected MalformedSessionUrl, got {other:?}"),
            }
        }
    }

    /// Nothing in the sync `Account` uses EventSource, so a session whose
    /// `eventSourceUrl` this crate cannot use must still open. It used to be
    /// parsed eagerly and refused the whole client. The failure is raised
    /// at use instead, still as the server's malformed `eventSourceUrl`.
    #[test]
    fn a_malformed_event_source_template_fails_only_at_use() {
        for bad in [
            "https://example.test/es?types={types",
            "https://example.test/es{?types,closeafter,ping}",
            "es/?types={types}",
        ] {
            let mut session =
                serde_json::from_str::<serde_json::Value>(&session_json("es", "A1", "session-1"))
                    .expect("fixture is json");
            session["eventSourceUrl"] = json!(bad);
            let session: Session = serde_json::from_value(session).expect("session parses");

            let client = Client::with_transport(
                RefreshingTransport {
                    api_urls: Arc::new(Mutex::new(Vec::new())),
                },
                session,
                "https://example.test/.well-known/jmap",
            )
            .unwrap_or_else(|e| panic!("{bad}: the client must still build, got {e:?}"));

            match client.session_state().event_source_url() {
                Err(crate::Error::MalformedSessionUrl { property, detail }) => {
                    assert_eq!(property, "eventSourceUrl");
                    assert!(detail.contains(bad), "{bad}: {detail}");
                }
                Err(other) => panic!("{bad}: expected MalformedSessionUrl, got {other:?}"),
                Ok(_) => panic!("{bad}: a malformed template must fail at use"),
            }
        }
    }

    /// A template that parses but is not an absolute http(s) URL used to
    /// pass `session_template` and fail later inside the transport, where
    /// nothing named the server or the property. It is the provider's
    /// malformed session and must be refused as one.
    #[test]
    fn a_relative_or_non_http_session_template_is_malformed() {
        for bad in [
            "not-a-url/{accountId}",
            "/upload/{accountId}",
            "{accountId}/upload",
            "ftp://example.test/upload/{accountId}",
            "file:///srv/upload/{accountId}",
        ] {
            match super::session_template::<crate::blob::URLParameter>("uploadUrl", bad) {
                Err(crate::Error::MalformedSessionUrl { property, detail }) => {
                    assert_eq!(property, "uploadUrl");
                    assert!(detail.contains(bad), "{bad}: {detail}");
                }
                Err(other) => panic!("{bad}: expected MalformedSessionUrl, got {other:?}"),
                Ok(_) => panic!("{bad}: a non-absolute template must be refused"),
            }
        }
        for bad in ["es?types={types}", "wss://example.test/es?ping={ping}"] {
            assert!(
                matches!(
                    super::session_template::<crate::event_source::URLParameter>(
                        "eventSourceUrl",
                        bad
                    ),
                    Err(crate::Error::MalformedSessionUrl {
                        property: "eventSourceUrl",
                        ..
                    })
                ),
                "{bad}: a non-absolute template must be refused"
            );
        }

        // Through the client door too: a session whose downloadUrl is
        // relative does not build a client.
        let mut session =
            serde_json::from_str::<serde_json::Value>(&session_json("rel", "A1", "session-1"))
                .expect("fixture is json");
        session["downloadUrl"] = json!("dl/{accountId}/{blobId}/{name}/{type}");
        let session: Session = serde_json::from_value(session).expect("session parses");
        let error = Client::with_transport(
            RefreshingTransport {
                api_urls: Arc::new(Mutex::new(Vec::new())),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .err()
        .expect("a relative downloadUrl must not build a client");
        assert!(
            matches!(
                error,
                crate::Error::MalformedSessionUrl {
                    property: "downloadUrl",
                    ..
                }
            ),
            "unexpected error: {error:?}"
        );
    }

    /// The absoluteness check must not refuse a legitimate RFC 6570 level-1
    /// template: variables in the path, in the query, as a whole path
    /// segment, glued to a literal, and even as a host label all expand to
    /// an absolute http(s) URL.
    #[test]
    fn absolute_session_templates_with_variables_anywhere_are_accepted() {
        for good in [
            "https://example.test/dl/{accountId}/{blobId}/{name}?accept={type}",
            "https://example.test/dl/{accountId}/{blobId}?name={name}&type={type}",
            "https://example.test/upload?account={accountId}",
            "https://example.test/upload/{accountId}/",
            "http://127.0.0.1:8080/jmap/upload/{accountId}",
            "https://[::1]:8443/dl/{accountId}{blobId}",
            "HTTPS://Example.TEST/dl/{accountId}/{blobId}",
            "https://{accountId}.example.test/dl/{blobId}",
            "https://example.test{accountId}/{blobId}",
        ] {
            assert!(
                super::session_template::<crate::blob::URLParameter>("downloadUrl", good).is_ok(),
                "{good}: a legitimate template was refused"
            );
        }
        for good in [
            "https://example.test/es?types={types}&closeafter={closeafter}&ping={ping}",
            "https://example.test/es/{types}/{closeafter}/{ping}",
            "https://example.test/es",
        ] {
            assert!(
                super::session_template::<crate::event_source::URLParameter>(
                    "eventSourceUrl",
                    good
                )
                .is_ok(),
                "{good}: a legitimate template was refused"
            );
        }
    }

    fn leading_value<P: crate::core::session::URLParser>(parts: &[super::URLPart<P>]) -> &str {
        match parts.first() {
            Some(super::URLPart::Value(value)) => value,
            _ => panic!("a parsed template starts with a literal"),
        }
    }

    /// Serves a second, entirely different session on refresh, and
    /// records which `apiUrl` requests actually went to.
    struct RefreshingTransport {
        api_urls: Arc<Mutex<Vec<String>>>,
    }

    impl HttpTransport for RefreshingTransport {
        async fn api_request(&self, url: &str, _body: Vec<u8>) -> Result<Bytes, TransportError> {
            self.api_urls
                .lock()
                .expect("api url lock")
                .push(url.to_string());
            Err(TransportError::new("stub transport returns no response"))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not upload"))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Ok(Bytes::from(session_json("new", "B2", "session-2")))
        }
    }

    struct RequestSnapshotTransport {
        requests: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
        refreshed_session: String,
    }

    impl HttpTransport for RequestSnapshotTransport {
        async fn api_request(&self, url: &str, body: Vec<u8>) -> Result<Bytes, TransportError> {
            let request = serde_json::from_slice(&body).expect("request body is JSON");
            self.requests
                .lock()
                .expect("request log")
                .push((url.to_string(), request));
            Err(TransportError::new(
                "snapshot test does not need a response",
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("snapshot test does not upload"))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("snapshot test does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Ok(Bytes::from(self.refreshed_session.clone()))
        }
    }

    struct GatedRefreshTransport {
        calls: Arc<AtomicUsize>,
        first_started: Arc<Notify>,
        release_first: Arc<Semaphore>,
    }

    impl HttpTransport for GatedRefreshTransport {
        async fn api_request(&self, _url: &str, _body: Vec<u8>) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "refresh test does not send API requests",
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("refresh test does not upload"))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("refresh test does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.first_started.notify_one();
                self.release_first
                    .acquire()
                    .await
                    .expect("refresh release semaphore remains open")
                    .forget();
                Ok(Bytes::from(session_json("middle", "B2", "session-2")))
            } else {
                Ok(Bytes::from(session_json("latest", "C3", "session-3")))
            }
        }
    }

    fn session_with_call_limit(tag: &str, account: &str, state: &str, max_calls: usize) -> String {
        let mut session: serde_json::Value =
            serde_json::from_str(&session_json(tag, account, state)).expect("fixture parses");
        session["capabilities"]["urn:ietf:params:jmap:core"] = json!({
            "maxSizeUpload": 1000,
            "maxConcurrentUpload": 2,
            "maxSizeRequest": 100_000,
            "maxConcurrentRequests": 3,
            "maxCallsInRequest": max_calls,
            "maxObjectsInGet": 256,
            "maxObjectsInSet": 256,
            "collationAlgorithms": []
        });
        session.to_string()
    }

    /// A Request must keep one session view for its default account, call
    /// limit, and API URL. Refreshing after construction used to splice the
    /// old call limit and account together with the new endpoint.
    #[cfg(feature = "mail")]
    #[tokio::test]
    async fn a_request_keeps_its_session_snapshot_through_refresh_and_send() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let old_session = session_with_call_limit("old", "A1", "session-1", 1);
        let new_session = session_with_call_limit("new", "B2", "session-2", 2);
        let client = Client::with_transport(
            RequestSnapshotTransport {
                requests: Arc::clone(&requests),
                refreshed_session: new_session,
            },
            serde_json::from_str(&old_session).expect("session parses"),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        let mut request = client.build();
        request
            .call(crate::mailbox::MailboxGet::new())
            .expect("first method fits the original limit");
        client.refresh_session().await.expect("refresh succeeds");

        assert!(matches!(
            request.call(crate::mailbox::MailboxGet::new()),
            Err(crate::Error::RequestCallLimit { max: 1 })
        ));
        let encoded: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&request).expect("request encodes"))
                .expect("request is JSON");
        assert_eq!(
            encoded["methodCalls"]
                .as_array()
                .expect("methodCalls is an array")
                .len(),
            1
        );
        assert_eq!(encoded["methodCalls"][0][1]["accountId"], "A1");

        let _ = client.send_request(&request).await;
        let requests = requests.lock().expect("request log");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "https://old.example.test/api");
        assert_eq!(requests[0].1["methodCalls"][0][1]["accountId"], "A1");
    }

    /// An earlier session fetch can finish after a later one unless refreshes
    /// serialize the fetch as well as the install. The second refresh must
    /// wait, then observe and install the later server session.
    #[tokio::test]
    async fn overlapping_refreshes_do_not_install_an_older_session_last() {
        let calls = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Semaphore::new(0));
        let client = Client::with_transport(
            GatedRefreshTransport {
                calls: Arc::clone(&calls),
                first_started: Arc::clone(&first_started),
                release_first: Arc::clone(&release_first),
            },
            serde_json::from_str(&session_json("old", "A1", "session-1")).expect("session parses"),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        let started = first_started.notified();
        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.refresh_session().await });
        started.await;

        let second_client = client.clone();
        let second = tokio::spawn(async move { second_client.refresh_session().await });
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        release_first.add_permits(1);
        first
            .await
            .expect("first refresh task joins")
            .expect("first refresh");
        second
            .await
            .expect("second refresh task joins")
            .expect("second refresh");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(client.session().state(), "session-3");
    }

    /// Records every API concurrency bound the client hands it, and serves a
    /// refreshed session advertising a different one.
    struct LimitRecordingTransport {
        limits: Arc<Mutex<Vec<usize>>>,
    }

    fn session_with_limit(limit: usize, state: &str) -> String {
        let mut session: serde_json::Value =
            serde_json::from_str(&session_json("limit", "A1", state)).expect("fixture parses");
        session["capabilities"]["urn:ietf:params:jmap:core"] = json!({
            "maxSizeUpload": 1000,
            "maxConcurrentUpload": 2,
            "maxSizeRequest": 100_000,
            "maxConcurrentRequests": limit,
            "maxCallsInRequest": 8,
            "maxObjectsInGet": 256,
            "maxObjectsInSet": 256,
            "collationAlgorithms": []
        });
        session.to_string()
    }

    impl HttpTransport for LimitRecordingTransport {
        async fn api_request(&self, _url: &str, _body: Vec<u8>) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport returns no response"))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not upload"))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Ok(Bytes::from(session_with_limit(5, "session-2")))
        }

        fn set_api_concurrency(&self, max_concurrent_requests: usize) {
            self.limits
                .lock()
                .expect("limit lock")
                .push(max_concurrent_requests);
        }
    }

    /// Every session the client installs hands its `maxConcurrentRequests`
    /// to the transport: the one it is built with, and each refresh, since
    /// RFC 8620 lets any session property change.
    #[tokio::test]
    async fn every_installed_session_bounds_the_transports_api_concurrency() {
        let limits = Arc::new(Mutex::new(Vec::new()));
        let session: Session =
            serde_json::from_str(&session_with_limit(3, "session-1")).expect("session parses");
        let client = Client::with_transport(
            LimitRecordingTransport {
                limits: Arc::clone(&limits),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        assert_eq!(*limits.lock().expect("limit lock"), vec![3]);

        client.refresh_session().await.expect("refresh succeeds");
        assert_eq!(*limits.lock().expect("limit lock"), vec![3, 5]);
    }

    /// A session that omits `maxConcurrentRequests` gates the transport at
    /// one, the same reading the fan-outs take. The transport used to read
    /// an omission as ungated while every fan-out read it as serial.
    #[test]
    fn an_unadvertised_limit_gates_the_transport_at_one() {
        let limits = Arc::new(Mutex::new(Vec::new()));
        let mut session: serde_json::Value =
            serde_json::from_str(&session_with_limit(3, "session-1")).expect("fixture parses");
        session["capabilities"]["urn:ietf:params:jmap:core"]
            .as_object_mut()
            .expect("core capability object")
            .remove("maxConcurrentRequests");
        let session: Session = serde_json::from_value(session).expect("session parses");
        assert_eq!(session.api_request_concurrency(), 1);
        Client::with_transport(
            LimitRecordingTransport {
                limits: Arc::clone(&limits),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        assert_eq!(*limits.lock().expect("limit lock"), vec![1]);
    }

    /// RFC 8620 §2 lets any Session property change. A refresh that only
    /// replaced the `Session` left `apiUrl`, the blob / EventSource
    /// templates and the default account frozen at the values of the
    /// session it replaced, so `session()` reported the new server while
    /// every request still went to the old one.
    #[tokio::test]
    async fn refresh_replaces_the_session_and_everything_derived_from_it() {
        let api_urls = Arc::new(Mutex::new(Vec::new()));
        let old: Session = serde_json::from_str(&session_json("old", "A1", "session-1"))
            .expect("session fixture parses");
        let client = Client::with_transport(
            RefreshingTransport {
                api_urls: Arc::clone(&api_urls),
            },
            old,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        assert_eq!(client.default_account_id().as_str(), "A1");
        let _ = client.send_request(&client.build()).await;

        client.refresh_session().await.expect("refresh succeeds");

        assert_eq!(client.session().state(), "session-2");
        assert_eq!(client.default_account_id().as_str(), "B2");
        let state = client.session_state();
        assert_eq!(state.api_url(), "https://new.example.test/api");
        assert_eq!(
            leading_value(state.download_url()),
            "https://new.example.test/dl/"
        );
        assert_eq!(
            leading_value(state.upload_url()),
            "https://new.example.test/upload/"
        );
        assert_eq!(
            leading_value(
                state
                    .event_source_url()
                    .expect("the refreshed eventSourceUrl parses")
            ),
            "https://new.example.test/es"
        );

        let _ = client.send_request(&client.build()).await;
        let urls = api_urls.lock().expect("api url lock").clone();
        assert_eq!(
            urls,
            vec![
                "https://old.example.test/api".to_string(),
                "https://new.example.test/api".to_string()
            ]
        );
    }

    #[test]
    fn well_known_session_urls_are_origin_rooted_with_a_path_fallback() {
        assert_eq!(
            well_known_session_urls("https://example.test/jmap"),
            vec![
                "https://example.test/.well-known/jmap",
                "https://example.test/jmap/.well-known/jmap"
            ]
        );
        assert_eq!(
            well_known_session_urls("https://example.test/jmap/"),
            vec![
                "https://example.test/.well-known/jmap",
                "https://example.test/jmap/.well-known/jmap"
            ]
        );
        assert_eq!(
            well_known_session_urls("https://example.test/?configured=1#fragment"),
            vec!["https://example.test/.well-known/jmap"]
        );
    }

    struct DiscoveryTransport {
        urls: Arc<Mutex<Vec<String>>>,
        replies: Mutex<VecDeque<Result<Bytes, TransportError>>>,
    }

    impl DiscoveryTransport {
        fn new(replies: impl IntoIterator<Item = Result<Bytes, TransportError>>) -> Self {
            Self {
                urls: Arc::new(Mutex::new(Vec::new())),
                replies: Mutex::new(replies.into_iter().collect()),
            }
        }

        fn status(code: reqwest::StatusCode) -> TransportError {
            TransportError::from_net(bifrost_net::Error::Status {
                code,
                body: Bytes::new(),
                headers: reqwest::header::HeaderMap::new(),
            })
        }
    }

    impl HttpTransport for DiscoveryTransport {
        async fn api_request(&self, _: &str, _: Vec<u8>) -> Result<Bytes, TransportError> {
            unreachable!("session discovery does not make API requests")
        }

        async fn upload(
            &self,
            _: &str,
            _: Vec<u8>,
            _: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            unreachable!("session discovery does not upload")
        }

        async fn download(&self, _: &str) -> Result<Bytes, TransportError> {
            unreachable!("session discovery does not download")
        }

        async fn get_session(&self, url: &str) -> Result<Bytes, TransportError> {
            self.urls
                .lock()
                .expect("discovery URL lock")
                .push(url.to_string());
            self.replies
                .lock()
                .expect("discovery reply lock")
                .pop_front()
                .expect("a scripted discovery reply")
        }
    }

    #[tokio::test]
    async fn session_discovery_uses_the_path_only_after_an_origin_404() {
        let transport = DiscoveryTransport::new([
            Err(DiscoveryTransport::status(reqwest::StatusCode::NOT_FOUND)),
            Ok(Bytes::from_static(b"session")),
        ]);

        let (url, bytes) = fetch_session(&transport, "https://example.test/jmap/")
            .await
            .expect("the path-mounted session follows an origin 404");

        assert_eq!(url, "https://example.test/jmap/.well-known/jmap");
        assert_eq!(bytes, Bytes::from_static(b"session"));
        assert_eq!(
            *transport.urls.lock().expect("discovery URL lock"),
            vec![
                "https://example.test/.well-known/jmap",
                "https://example.test/jmap/.well-known/jmap"
            ]
        );
    }

    #[tokio::test]
    async fn session_discovery_does_not_fallback_on_other_errors_or_root_bases() {
        let transport = DiscoveryTransport::new([Err(DiscoveryTransport::status(
            reqwest::StatusCode::FORBIDDEN,
        ))]);
        assert!(
            fetch_session(&transport, "https://example.test/jmap")
                .await
                .is_err()
        );
        assert_eq!(
            *transport.urls.lock().expect("discovery URL lock"),
            vec!["https://example.test/.well-known/jmap"]
        );

        let root_transport = DiscoveryTransport::new([Err(DiscoveryTransport::status(
            reqwest::StatusCode::NOT_FOUND,
        ))]);
        assert!(
            fetch_session(&root_transport, "https://example.test/")
                .await
                .is_err()
        );
        assert_eq!(
            *root_transport.urls.lock().expect("discovery URL lock"),
            vec!["https://example.test/.well-known/jmap"]
        );
    }
}

/// Default client using reqwest. Use `Client::new()` / `ClientBuilder` to construct.
pub(crate) type DefaultClient = Client<ReqwestTransport>;

impl Client {
    /// Create a new client builder (uses reqwest transport by default).
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new() -> ClientBuilder {
        ClientBuilder::new()
    }
}

impl<T: HttpTransport> Client<T> {
    /// Create a client with a custom transport and pre-fetched session.
    pub(crate) fn with_transport(
        transport: T,
        session: Session,
        session_url: impl Into<String>,
    ) -> crate::Result<Self> {
        let session_url = session_url.into();
        if session_url.is_empty() {
            return Err(crate::Error::InvalidUrl(
                "a custom transport client needs a session URL".to_string(),
            ));
        }
        transport.set_api_concurrency(session.api_request_concurrency());
        Ok(Client {
            tally: None,
            inner: Arc::new(ClientInner {
                state: std::sync::Mutex::new(Arc::new(SessionState::derive(session)?)),
                refresh_gate: futures::lock::Mutex::new(()),
                session_url,
                session_updated: true.into(),
                #[cfg(feature = "tokio")]
                session_changes: tokio::sync::watch::channel(0).0,
                timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
                transport,
                #[cfg(feature = "websockets")]
                accept_invalid_certs: false,
                #[cfg(feature = "websockets")]
                authorization: Authorization::Basic(String::new()),
                #[cfg(feature = "websockets")]
                ws: None.into(),
                #[cfg(feature = "websockets")]
                ws_request_id: std::sync::atomic::AtomicU64::new(0),
                #[cfg(feature = "websockets")]
                ws_pending: Arc::new(crate::client_ws::PendingRequests::new()),
            }),
        })
    }

    pub(crate) fn build(&self) -> Request<'_, T> {
        Request::new(self)
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    /// The session and everything derived from it, as one consistent
    /// snapshot. Hold the returned `Arc` for the duration of a URL build
    /// so a concurrent [`Client::refresh_session`] cannot splice two
    /// sessions into one request.
    pub(crate) fn session_state(&self) -> Arc<SessionState> {
        self.inner
            .state
            .lock()
            .expect("session mutex poisoned")
            .clone()
    }

    pub(crate) fn session(&self) -> Arc<Session> {
        Arc::clone(self.session_state().session())
    }

    pub(crate) fn session_url(&self) -> &str {
        &self.inner.session_url
    }

    pub(crate) fn default_account_id(&self) -> crate::core::id::AccountId {
        self.session_state().default_account_id().clone()
    }

    /// Send a JMAP request and get a typed Response.
    pub(crate) async fn send_request(
        &self,
        request: &request::Request<'_, T>,
    ) -> crate::Result<response::Response> {
        let body = serde_json::to_vec(request).map_err(crate::Error::RequestEncode)?;
        let (bytes, bytes_in) = self
            .inner
            .transport
            .api_request_measured(request.session_state().api_url(), body)
            .await
            .map_err(crate::Error::from)?;
        self.record_bytes_in(bytes_in);
        let response: response::Response = serde_json::from_slice(&bytes)?;
        self.note_session_state(response.session_state());
        Ok(response)
    }

    /// The next RFC 8887 `requestId` for a WebSocket request. Monotonic
    /// for the life of the CLIENT, so ids are not reused across reconnects.
    #[cfg(feature = "websockets")]
    pub(crate) fn next_ws_request_id(&self) -> String {
        self.inner
            .ws_request_id
            .fetch_add(1, Ordering::Relaxed)
            .to_string()
    }

    /// Compare a server-reported `sessionState` against the one this client
    /// is running on, and publish a divergence.
    ///
    /// Every response carries `sessionState` (RFC 8620 s3.4), and the whole
    /// scope-lifecycle `CapabilityChanged` story rests on noticing when it
    /// moves. Both response doors must therefore run this: the HTTP door
    /// always did, while the WebSocket door skipped the comparison and
    /// handed every response frame up without ever reading its
    /// `sessionState`, so staleness went undetected on exactly the
    /// connection that stays open longest.
    pub(crate) fn note_session_state(&self, session_state: &str) {
        if session_state != self.session_state().session().state() {
            self.inner.session_updated.store(false, Ordering::Release);
            #[cfg(feature = "tokio")]
            self.inner.session_changes.send_modify(|generation| {
                *generation = generation.wrapping_add(1);
            });
        }
    }

    /// Re-fetch the session and republish everything derived from it.
    ///
    /// The derived endpoints and default account are replaced together
    /// with the `Session` itself; a partial swap would leave requests
    /// routed by a session no caller can observe.
    ///
    /// Currently unwired: session divergence is handled by the lifecycle path,
    /// which wakes on a watch channel rather than polling. Kept and disclosed
    /// in `reference/jmap.md` rather than deleted - an audit finding it
    /// uncalled has found a recorded state, not dead code.
    pub(crate) async fn refresh_session(&self) -> crate::Result<()> {
        let _refresh = self.inner.refresh_gate.lock().await;
        let bytes = self
            .inner
            .transport
            .get_session(&self.inner.session_url)
            .await
            .map_err(crate::Error::from)?;
        let session: Session = serde_json::from_slice(&bytes)?;
        let limit = session.api_request_concurrency();
        let state = Arc::new(SessionState::derive(session)?);
        // The limit is applied under the same lock as the state it came from,
        // so two overlapping refreshes cannot leave one session's state beside
        // the other's limit. `set_api_concurrency` is synchronous.
        let mut installed = self.inner.state.lock().expect("session mutex poisoned");
        *installed = state;
        self.inner.transport.set_api_concurrency(limit);
        self.inner.session_updated.store(true, Ordering::Release);
        drop(installed);
        Ok(())
    }

    pub(crate) fn is_session_updated(&self) -> bool {
        self.inner.session_updated.load(Ordering::Acquire)
    }

    /// Subscribe to session-state divergence detected at the response boundary.
    #[cfg(feature = "tokio")]
    pub(crate) fn session_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.session_changes.subscribe()
    }

    /// A handle over the same client - same transport, same session
    /// state - that reports every `/jmap/api` response's inbound bytes
    /// into a fresh accumulator.
    ///
    /// One accumulator per stream, not per client: the returned handle
    /// is the only one recording into it, so concurrent work on other
    /// scopes cannot contaminate the total.
    pub(crate) fn metered(&self) -> (Self, ByteTally) {
        let tally = ByteTally::default();
        (self.metered_into(&tally), tally)
    }

    /// A metered handle reporting into an EXISTING accumulator.
    ///
    /// One batch is sometimes produced by two differently scoped
    /// handles - the raw-RFC822 read runs an `Email/get` through an
    /// account view and the blob download through the client - and both
    /// legs are traffic that batch caused, so they share one tally
    /// rather than each opening their own.
    pub(crate) fn metered_into(&self, tally: &ByteTally) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            tally: Some(tally.clone()),
        }
    }

    /// Report inbound payload bytes to this handle's accumulator, if it
    /// has one. Every transport door that reads a body calls this.
    pub(crate) fn record_bytes_in(&self, bytes_in: u64) {
        if let Some(tally) = self.tally.as_ref() {
            tally.add(bytes_in);
        }
    }

    /// Access the underlying transport.
    pub(crate) fn transport(&self) -> &T {
        &self.inner.transport
    }

    /// Returns the `Authorization` header value used by this client.
    /// Async because the bearer case reads the current token from the
    /// shared source so a rotated token is presented on reconnect.
    #[cfg(feature = "websockets")]
    pub(crate) async fn authorization(&self) -> crate::Result<String> {
        self.inner
            .authorization
            .header_value()
            .await
            .map_err(|e| crate::Error::from(crate::core::transport::TransportError::from_net(e)))
    }
}

impl Credentials {
    pub(crate) fn basic(username: &str, password: &str) -> Self {
        use base64::{Engine, engine::general_purpose::STANDARD};
        Credentials::Basic(STANDARD.encode(format!("{username}:{password}")))
    }

    pub(crate) fn bearer(token: impl Into<String>) -> Self {
        Credentials::Bearer(Arc::new(StaticTokenSource::new(token, None)))
    }

    pub(crate) fn bearer_source(source: Arc<dyn TokenSource>) -> Self {
        Credentials::Bearer(source)
    }
}

impl From<(&str, &str)> for Credentials {
    fn from(credentials: (&str, &str)) -> Self {
        Credentials::basic(credentials.0, credentials.1)
    }
}

impl From<(String, String)> for Credentials {
    fn from(credentials: (String, String)) -> Self {
        Credentials::basic(&credentials.0, &credentials.1)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::response::Response;

    #[test]
    fn test_deserialize() {
        let _r: Response = serde_json::from_slice(
            br#"{"sessionState": "123", "methodResponses": [[ "Email/query", {
                "accountId": "A1",
                "queryState": "abcdefg",
                "canCalculateChanges": true,
                "position": 0,
                "total": 101,
                "ids": [ "msg1023", "msg223", "msg110", "msg93", "msg91",
                    "msg38", "msg36", "msg33", "msg11", "msg1" ]
            }, "t0" ],
            [ "Email/get", {
                "accountId": "A1",
                "state": "123456",
                "list": [{
                    "id": "msg1023",
                    "threadId": "trd194"
                }, {
                    "id": "msg223",
                    "threadId": "trd114"
                }
                ],
                "notFound": []
            }, "t1" ],
            [ "Thread/get", {
                "accountId": "A1",
                "state": "123456",
                "list": [{
                    "id": "trd194",
                    "emailIds": [ "msg1020", "msg1021", "msg1023" ]
                }, {
                    "id": "trd114",
                    "emailIds": [ "msg201", "msg223" ]
                }
                ],
                "notFound": []
            }, "t2" ]]}"#,
        )
        .unwrap();
    }
}

#[cfg(test)]
mod bearer_lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn websocket_authentication_reads_the_http_coordinator() {
        struct Source;
        impl TokenSource for Source {
            fn current(
                &self,
            ) -> bifrost_net::AccountFuture<Result<bifrost_net::AccessToken, bifrost_net::Error>>
            {
                Box::pin(async { Ok(bifrost_net::AccessToken::new("old", None)) })
            }
            fn refresh(
                &self,
            ) -> bifrost_net::AccountFuture<Result<bifrost_net::AccessToken, bifrost_net::Error>>
            {
                Box::pin(async { Ok(bifrost_net::AccessToken::new("new", None)) })
            }
        }
        let auth = Authorization::from_credentials(Credentials::Bearer(Arc::new(Source)));
        let source = auth.account_token_source();
        assert!(source.is_coordinating());
        let net = bifrost_net::Net::new(bifrost_net::NetConfig::default()).unwrap();
        let account = net.attach_account(
            NetAccountId("jmap-auth-test".into()),
            bifrost_net::AccountSpec::new(Some(source)),
        );
        assert_eq!(account.bearer().await.unwrap().unwrap().as_str(), "new");
        assert_eq!(auth.header_value().await.unwrap(), "Bearer new");
    }

    #[test]
    fn basic_authorization_has_no_bearer_token_source() {
        let auth = Authorization::from_credentials(Credentials::basic("user", "password"));
        assert!(auth.optional_account_token_source().is_none());
    }
}
