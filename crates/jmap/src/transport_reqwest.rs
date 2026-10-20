use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, ConcurrencyLimit, FollowRedirects, Net, Priority,
    RedirectPolicy,
};
use bytes::Bytes;
use reqwest::header;

use crate::client::Authorization;
use crate::core::transport::{HttpTransport, SseTransport, TransportError};
use futures::Stream;

/// Default HTTP transport implementation using `bifrost-net`.
///
/// Routes through the shared HTTP pipeline for retry, metering, and
/// token handling.
pub(crate) struct ReqwestTransport {
    net: AccountNet,
    headers: header::HeaderMap,
    authorization: Authorization,
    timeout: Duration,
    /// The session's `maxConcurrentRequests`, applied to API requests
    /// only. `None` until the first session is installed; the session GET
    /// that precedes it is not an API request.
    api_limit: std::sync::Mutex<Option<ConcurrencyLimit>>,
}

impl ReqwestTransport {
    pub(crate) fn new(
        headers: header::HeaderMap,
        authorization: Authorization,
        account_id: AccountId,
        timeout: Duration,
        accept_invalid_certs: bool,
        trusted_hosts: Arc<HashSet<String>>,
    ) -> Result<Self, TransportError> {
        let redirect_policy = redirect_policy_from_trusted_hosts(&trusted_hosts);
        let token_source = authorization.account_token_source();
        let mut spec = AccountSpec::new(Some(token_source));
        spec.request_timeout = Some(timeout);
        spec.user_agent = concat!("bifrost-jmap/", env!("CARGO_PKG_VERSION")).to_string();
        spec.follow_redirects = FollowRedirects::Enabled(redirect_policy);
        // TLS trust lives on the reqwest client, so it selects WHICH
        // shared client this account attaches to rather than being a
        // per-account setting. Accounts opting into invalid
        // certificates share one client among themselves; everything
        // else shares the default. Discarding the flag here would
        // leave a self-signed deployment failing every HTTP request
        // while its WebSocket - which builds its own TLS connector -
        // succeeded, so the option would appear to work and not.
        let net = Net::shared_for_tls(accept_invalid_certs).attach_account(account_id, spec);

        Ok(Self {
            net,
            headers,
            authorization,
            timeout,
            api_limit: std::sync::Mutex::new(None),
        })
    }

    fn api_limit(&self) -> Option<ConcurrencyLimit> {
        self.api_limit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_priority(&self, priority: Priority) {
        self.net.set_priority(priority);
    }

    #[cfg(feature = "websockets")]
    pub(crate) fn bandwidth_budget(&self) -> bifrost_net::BandwidthBudget {
        self.net.bandwidth_budget().clone()
    }

    pub(crate) fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.net.set_bandwidth_cap(bps);
    }

    async fn send(
        &self,
        method: &str,
        url: &str,
        body: Option<Bytes>,
        content_type: Option<&str>,
        total_timeout: Option<Duration>,
    ) -> Result<Bytes, TransportError> {
        Ok(self
            .send_measured(method, url, body, content_type, total_timeout, None)
            .await?
            .0)
    }

    /// `send`, also reporting bifrost-net's request-local inbound byte
    /// count. That count is the real inbound total for the whole
    /// request - retry drains, 401 recovery bodies and followed
    /// redirects included - which the decoded body length cannot see.
    async fn send_measured(
        &self,
        method: &str,
        url: &str,
        body: Option<Bytes>,
        content_type: Option<&str>,
        total_timeout: Option<Duration>,
        limit: Option<ConcurrencyLimit>,
    ) -> Result<(Bytes, u64), TransportError> {
        let response = self
            .send_once(method, url, body, content_type, total_timeout, limit)
            .await?;
        let bytes_in = response.bytes_in();
        Ok((
            Self::handle_response(response.status, response.body)?,
            bytes_in,
        ))
    }

    async fn build_request(
        &self,
        method: &str,
        url: &str,
        body: Option<Bytes>,
        content_type: Option<&str>,
        total_timeout: Option<Duration>,
        limit: Option<ConcurrencyLimit>,
    ) -> Result<bifrost_net::RequestBuilder, TransportError> {
        let mut request = match method {
            "GET" => self.net.get(url),
            "POST" => self.net.post(url),
            // Every caller in this file passes GET or POST, and the only
            // default header is `Forwarded`, formatted from an `IpAddr`: both
            // failures are impossible by construction, so they are the
            // client's own invariant, not a network fault to retry.
            _ => {
                return Err(TransportError::invariant(format!(
                    "unsupported HTTP method: {method}"
                )));
            }
        };
        // Bulk blob doors pass `None`: they may be deliberately paced by a
        // bandwidth cap, so net's response-header and body-inactivity bounds
        // are the useful limits while an absolute transfer deadline is not.
        request = match total_timeout {
            Some(timeout) => request.timeout(timeout),
            None => request.without_timeout(),
        };
        for (name, value) in &self.headers {
            let value = value.to_str().map_err(|e| {
                TransportError::invariant_with_source(
                    format!("Invalid default header value for {name}"),
                    e,
                )
            })?;
            request = request.header(name.as_str(), value);
        }
        if self.authorization.uses_bearer_pipeline() {
            // AccountNet injects the bearer token from the shared
            // token source on every request attempt.
        } else {
            let value = self
                .authorization
                .header_value()
                .await
                .map_err(TransportError::from_net)?;
            request = request
                .without_bearer_auth()
                .header(header::AUTHORIZATION.as_str(), &value);
        }
        if let Some(ct) = content_type {
            request = request.header(header::CONTENT_TYPE.as_str(), ct);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        if let Some(limit) = limit {
            request = request.concurrency_limit(limit);
        }
        Ok(request)
    }

    async fn send_once(
        &self,
        method: &str,
        url: &str,
        body: Option<Bytes>,
        content_type: Option<&str>,
        total_timeout: Option<Duration>,
        limit: Option<ConcurrencyLimit>,
    ) -> Result<bifrost_net::Response, TransportError> {
        self.build_request(method, url, body, content_type, total_timeout, limit)
            .await?
            .send()
            .await
            .map_err(transport_error_from_net)
    }

    fn handle_response(
        status: reqwest::StatusCode,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, TransportError> {
        if status.is_success() {
            Ok(body)
        } else {
            // Only a passed-through 3xx reaches here: bifrost-net turns
            // every 4xx/5xx into a typed `Error` before `send` returns,
            // so `Ok(response)` with a non-2xx status means a 304 / 305 /
            // 306 or a `Location`-less redirect. Such a body is normally
            // empty, but the redirect loop hands the real body up rather
            // than discarding it, so a `Location`-less redirect carrying
            // an explanatory document arrives here intact and is
            // preserved on the error.
            Err(TransportError::with_body(format!("HTTP {status}"), body))
        }
    }
}

impl HttpTransport for ReqwestTransport {
    async fn api_request(&self, url: &str, body: Vec<u8>) -> Result<bytes::Bytes, TransportError> {
        Ok(self.api_request_measured(url, body).await?.0)
    }

    async fn api_request_measured(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> Result<(bytes::Bytes, u64), TransportError> {
        self.send_measured(
            "POST",
            url,
            Some(Bytes::from(body)),
            Some("application/json"),
            Some(self.timeout),
            self.api_limit(),
        )
        .await
    }

    fn set_api_concurrency(&self, max_concurrent_requests: usize) {
        let mut slot = self
            .api_limit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match slot.as_ref() {
            // Resized in place, so requests already holding a slot keep
            // counting against the new bound.
            Some(existing) => existing.set_limit(max_concurrent_requests),
            None => *slot = Some(ConcurrencyLimit::new(max_concurrent_requests)),
        }
    }

    async fn upload(
        &self,
        url: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bytes::Bytes, TransportError> {
        self.send("POST", url, Some(Bytes::from(body)), content_type, None)
            .await
    }

    async fn download(&self, url: &str) -> Result<bytes::Bytes, TransportError> {
        Ok(self.download_measured(url).await?.0)
    }

    async fn download_measured(&self, url: &str) -> Result<(bytes::Bytes, u64), TransportError> {
        use crate::blob::download::BlobTransport;
        use futures::StreamExt;
        let download = self
            .download_stream(url, crate::blob::download::DEFAULT_MAX_BLOB_SIZE)
            .await?;
        let mut stream = download.body;
        let mut body = bytes::BytesMut::new();
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk?);
        }
        Ok((body.freeze(), download.bytes_in.bytes_in()))
    }

    async fn get_session(&self, url: &str) -> Result<bytes::Bytes, TransportError> {
        self.send("GET", url, None, None, Some(self.timeout)).await
    }
}

impl crate::blob::download::BlobTransport for ReqwestTransport {
    async fn download_stream(
        &self,
        url: &str,
        max_bytes: u64,
    ) -> Result<crate::blob::download::BlobDownload, TransportError> {
        // Use the streaming request door rather than AccountNet::download_stream:
        // blobs need the JMAP default headers, Basic auth support, and no total
        // deadline. Both doors use net's retry, metering and inactivity bounds.
        let response = self
            .build_request("GET", url, None, None, None, None)
            .await?
            .send_streaming()
            .await
            .map_err(transport_error_from_net)?;
        if !response.status().is_success() {
            use futures::StreamExt;
            let mut stream = response.body;
            let mut body = Vec::new();
            while let Some(chunk) = stream.next().await {
                let Ok(chunk) = chunk else { break };
                let remaining = PASSTHROUGH_BODY_CAP.saturating_sub(body.len());
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if body.len() == PASSTHROUGH_BODY_CAP {
                    break;
                }
            }
            return Err(TransportError::from_net(bifrost_net::Error::Status {
                code: response.status,
                headers: response.headers,
                body: Bytes::from(body),
            }));
        }
        Ok(crate::blob::download::BlobDownload {
            bytes_in: response.bytes_in,
            body: crate::blob::download::bounded_stream(response.body, max_bytes),
        })
    }
}

/// Ceiling on the passed-through 3xx body preserved by `open_sse`'s
/// error path. Mirrors bifrost-net's own 4 KB status-body cap: enough
/// for a provider's explanatory document, bounded against a hostile
/// stream.
const PASSTHROUGH_BODY_CAP: usize = 4096;

impl SseTransport for ReqwestTransport {
    type ByteStream = ReqwestByteStream;

    async fn open_sse(
        &self,
        url: &str,
        last_event_id: Option<&str>,
    ) -> Result<Self::ByteStream, TransportError> {
        let mut request = self
            .net
            .get(url)
            .header(header::ACCEPT.as_str(), "text/event-stream")
            .without_timeout();
        for (name, value) in &self.headers {
            let value = value.to_str().map_err(|e| {
                TransportError::invariant_with_source(
                    format!("Invalid default header value for {name}"),
                    e,
                )
            })?;
            request = request.header(name.as_str(), value);
        }
        if self.authorization.uses_bearer_pipeline() {
        } else {
            let value = self
                .authorization
                .header_value()
                .await
                .map_err(TransportError::from_net)?;
            request = request
                .without_bearer_auth()
                .header(header::AUTHORIZATION.as_str(), &value);
        }
        if let Some(id) = last_event_id {
            request = request.header("Last-Event-ID", id);
        }
        let response = request
            .send_streaming()
            .await
            .map_err(transport_error_from_net)?;

        if response.status().is_success() {
            return Ok(ReqwestByteStream {
                inner: response.body,
            });
        }

        // `send_streaming` already turns every 4xx/5xx into a typed
        // bifrost-net error, so the only non-2xx response it can hand
        // us is a passed-through redirect (304/305/306, or a
        // `Location`-less 3xx). Preserve its status and headers through
        // the same typed error boundary instead of discarding them
        // behind a generic network error. The redirect loop hands the
        // real body up rather than substituting an empty stream - a
        // `Location`-less 3xx can carry an explanatory document - so
        // drain a bounded prefix of it into the error the same way the
        // buffered path preserves it. The read is bounded by the
        // account's body inactivity timeout and by the cap below, so a
        // hostile or stalled body cannot hang or bloat the failure
        // path.
        let mut body_stream = response.body;
        let mut body: Vec<u8> = Vec::new();
        use futures::StreamExt;
        while let Some(chunk) = body_stream.next().await {
            let Ok(chunk) = chunk else { break };
            let remaining = PASSTHROUGH_BODY_CAP.saturating_sub(body.len());
            if chunk.len() >= remaining {
                body.extend_from_slice(&chunk[..remaining]);
                break;
            }
            body.extend_from_slice(&chunk);
        }
        Err(TransportError::from_net(bifrost_net::Error::Status {
            code: response.status,
            body: Bytes::from(body),
            headers: response.headers,
        }))
    }
}

/// Adapter that converts reqwest's `Bytes` stream into `Vec<u8>` chunks.
///
/// Constructed only by the `SseTransport::open_sse` impl, which the
/// Account impl does not currently drive; kept so the trait surface
/// stays complete.
#[allow(dead_code)]
pub(crate) struct ReqwestByteStream {
    inner: bifrost_net::ByteStream,
}

impl Stream for ReqwestByteStream {
    type Item = Result<Vec<u8>, TransportError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx).map(|opt| {
            opt.map(|result| {
                result
                    .map(|bytes| bytes.to_vec())
                    .map_err(transport_error_from_net)
            })
        })
    }
}

impl Unpin for ReqwestByteStream {}

fn transport_error_from_net(error: bifrost_net::Error) -> TransportError {
    // Preserve the original net error so the JMAP conversion boundary
    // can delegate to bifrost_net::into_account_error for pure
    // transport-level signals. `TransportError::from_net` also lifts the
    // response body out of whichever variant carries it (`Status`
    // directly, `RateLimited` / `RetryBudgetExhausted` from the
    // preserved final response) so ProblemDetails parsing still works
    // for the statuses the retry loop consumed.
    TransportError::from_net(error)
}

fn redirect_policy_from_trusted_hosts(trusted_hosts: &HashSet<String>) -> RedirectPolicy {
    let mut policy = RedirectPolicy::with_hops(5);
    if trusted_hosts.is_empty() {
        policy = policy.trust_host("__bifrost_jmap_no_cross_host_redirects__");
    } else {
        for host in trusted_hosts {
            policy = policy.trust_host(host);
        }
    }
    policy
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Credentials;
    use bifrost_net::StaticTokenSource;
    use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_net};

    #[tokio::test]
    async fn blobs_bypass_json_buffer_limit_and_keep_streaming_and_buffered_doors() {
        use crate::blob::download::BlobTransport;
        use futures::StreamExt;
        let script = ScriptedDispatch::new((0..3).map(|_| Canned::Stream {
            status: reqwest::StatusCode::OK,
            headers: header::HeaderMap::new(),
            chunks: vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
        }));
        let mut transport = scripted_transport(&script);
        let mut spec = AccountSpec::new(None);
        spec.max_buffered_response = Some(1);
        transport.net = scripted_net(&script, bifrost_net::NetConfig::default())
            .attach_account(AccountId("small-json-limit".to_owned()), spec);

        let download = transport
            .download_stream("https://jmap.test/blob", 4)
            .await
            .unwrap();
        let mut stream = download.body;
        assert_eq!(stream.next().await.unwrap().unwrap(), b"ab"[..]);
        assert_eq!(stream.next().await.unwrap().unwrap(), b"cd"[..]);
        assert!(stream.next().await.is_none());
        assert_eq!(download.bytes_in.bytes_in(), 4);
        assert_eq!(
            transport.download("https://jmap.test/blob").await.unwrap(),
            b"abcd"[..]
        );
        let (body, count) = transport
            .download_measured("https://jmap.test/blob")
            .await
            .unwrap();
        assert_eq!(body, b"abcd"[..]);
        assert_eq!(count, 4);
        assert!(
            script
                .requests()
                .iter()
                .all(|request| request.timeout.is_none())
        );
    }

    #[test]
    fn independently_built_transports_share_the_process_net() {
        let first = ReqwestTransport::new(
            header::HeaderMap::new(),
            Authorization::from_credentials_for_test(Credentials::basic("one", "secret")),
            AccountId("first".to_owned()),
            Duration::from_secs(30),
            false,
            Arc::new(HashSet::new()),
        )
        .expect("first transport builds");
        let second = ReqwestTransport::new(
            header::HeaderMap::new(),
            Authorization::from_credentials_for_test(Credentials::basic("two", "secret")),
            AccountId("second".to_owned()),
            Duration::from_secs(30),
            false,
            Arc::new(HashSet::new()),
        )
        .expect("second transport builds");

        assert!(first.net.shares_transport_with(&second.net));
    }

    fn scripted_transport(script: &Arc<ScriptedDispatch>) -> ReqwestTransport {
        let mut spec = AccountSpec::new(Some(Arc::new(StaticTokenSource::new("token", None))));
        spec.request_timeout = Some(Duration::from_secs(45));
        let account = scripted_net(script, bifrost_net::NetConfig::default())
            .attach_account(AccountId("api-limit".to_owned()), spec);
        ReqwestTransport {
            net: account,
            headers: header::HeaderMap::new(),
            authorization: Authorization::from_credentials_for_test(Credentials::basic(
                "user", "secret",
            )),
            timeout: Duration::from_secs(30),
            api_limit: std::sync::Mutex::new(None),
        }
    }

    /// The session's `maxConcurrentRequests` bounds API requests across
    /// every caller of the transport, and only API requests: RFC 8620 gives
    /// uploads and downloads limits of their own. The dispatcher yields
    /// before answering, so an ungated burst is genuinely concurrent.
    #[tokio::test]
    async fn the_session_limit_gates_api_requests_and_not_downloads() {
        let ok = || bifrost_net::test_support::canned(reqwest::StatusCode::OK, b"{}");

        let api_script = ScriptedDispatch::yielding((0..4).map(|_| ok()));
        let api = scripted_transport(&api_script);
        api.set_api_concurrency(2);
        let calls = (0..4).map(|_| api.api_request("https://jmap.test/api", b"{}".to_vec()));
        for result in futures::future::join_all(calls).await {
            result.expect("api request");
        }
        assert_eq!(api_script.peak_in_flight(), 2);

        let blob_script = ScriptedDispatch::yielding((0..4).map(|_| ok()));
        let blobs = scripted_transport(&blob_script);
        blobs.set_api_concurrency(2);
        let downloads = (0..4).map(|_| blobs.download("https://jmap.test/blob"));
        for result in futures::future::join_all(downloads).await {
            result.expect("download");
        }
        assert_eq!(
            blob_script.peak_in_flight(),
            4,
            "downloads are not API requests and are not gated by the API limit"
        );
    }

    #[tokio::test]
    async fn blob_requests_disable_the_total_deadline() {
        let script = ScriptedDispatch::new(
            (0..5).map(|_| bifrost_net::test_support::canned(reqwest::StatusCode::OK, b"{}")),
        );
        let transport = scripted_transport(&script);

        transport
            .api_request("https://jmap.test/api", b"{}".to_vec())
            .await
            .expect("API request");
        transport
            .upload(
                "https://jmap.test/upload",
                b"blob".to_vec(),
                Some("text/plain"),
            )
            .await
            .expect("blob upload");
        transport
            .download("https://jmap.test/blob")
            .await
            .expect("blob download");
        transport
            .download_measured("https://jmap.test/measured-blob")
            .await
            .expect("measured blob download");
        transport
            .get_session("https://jmap.test/session")
            .await
            .expect("session request");

        let timeouts = script
            .requests()
            .iter()
            .map(|request| request.timeout)
            .collect::<Vec<_>>();
        assert_eq!(timeouts.len(), 5);
        let is_client_deadline = |timeout: Option<Duration>| {
            timeout.is_some_and(|remaining| {
                remaining > Duration::ZERO && remaining <= Duration::from_secs(30)
            })
        };
        assert!(
            is_client_deadline(timeouts[0]),
            "API request should keep the client's 30s deadline, got {:?}",
            timeouts[0]
        );
        assert_eq!(
            &timeouts[1..4],
            &[None, None, None],
            "blob upload and download doors should disable the total deadline"
        );
        assert!(
            is_client_deadline(timeouts[4]),
            "session request should keep the client's 30s deadline, got {:?}",
            timeouts[4]
        );
    }

    #[tokio::test]
    async fn event_source_suppresses_the_ordinary_request_deadline() {
        let script = ScriptedDispatch::new([Canned::Stream {
            status: reqwest::StatusCode::OK,
            headers: header::HeaderMap::new(),
            chunks: Vec::new(),
        }]);
        let mut spec = AccountSpec::new(Some(Arc::new(StaticTokenSource::new("token", None))));
        spec.request_timeout = Some(Duration::from_secs(30));
        let account = scripted_net(&script, bifrost_net::NetConfig::default())
            .attach_account(AccountId("sse-timeout".to_owned()), spec);
        let transport = ReqwestTransport {
            net: account,
            headers: header::HeaderMap::new(),
            authorization: Authorization::from_credentials_for_test(Credentials::basic(
                "user", "secret",
            )),
            timeout: Duration::from_secs(30),
            api_limit: std::sync::Mutex::new(None),
        };

        transport
            .open_sse("https://push.test/events", None)
            .await
            .expect("SSE headers succeed");

        assert_eq!(script.requests()[0].timeout, None);
    }

    /// The redirect loop hands a passed-through 3xx body up rather than
    /// substituting an empty stream, and `open_sse`'s error path must
    /// preserve it: a `Location`-less 3xx can carry the provider's
    /// explanation of why the stream did not open.
    #[tokio::test]
    async fn sse_open_preserves_a_passed_through_3xx_body() {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        let script = ScriptedDispatch::new([Canned::Stream {
            status: reqwest::StatusCode::PERMANENT_REDIRECT,
            headers,
            chunks: vec![bytes::Bytes::from_static(b"{\"why\":\"gone\"}")],
        }]);
        let spec = AccountSpec::new(Some(Arc::new(StaticTokenSource::new("token", None))));
        let account = scripted_net(&script, bifrost_net::NetConfig::default())
            .attach_account(AccountId("sse-3xx-body".to_owned()), spec);
        let transport = ReqwestTransport {
            net: account,
            headers: header::HeaderMap::new(),
            authorization: Authorization::from_credentials_for_test(Credentials::basic(
                "user", "secret",
            )),
            timeout: Duration::from_secs(30),
            api_limit: std::sync::Mutex::new(None),
        };

        let err = match transport.open_sse("https://push.test/events", None).await {
            Err(err) => err,
            Ok(_) => panic!("a Location-less 308 cannot open a stream"),
        };
        assert_eq!(
            err.body.as_deref(),
            Some(&b"{\"why\":\"gone\"}"[..]),
            "the passed-through body must reach the error"
        );
    }
}
