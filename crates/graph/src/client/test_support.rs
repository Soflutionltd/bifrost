//! In-process wire scripts and request recording shared by derived clients.

use super::{GraphClient, JSON_CONTENT_TYPE, WireBody};
use bifrost_net::{AccountNet, RetryPolicy};
use bytes::Bytes;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct RestRequest {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) if_match: Option<String>,
    pub(crate) prefer: Option<String>,
    /// The sent Content-Type value, or empty when the request has no body.
    pub(crate) content_type: String,
    /// The JSON body as sent, when the call sent one. `None` for a
    /// bodiless call and for the raw-MIME call, whose bytes land in
    /// `raw_body` instead.
    pub(crate) body: Option<serde_json::Value>,
    /// The verbatim bytes of a non-JSON body (Graph's base64 MIME import).
    pub(crate) raw_body: Option<Bytes>,
}

#[derive(Debug, Clone)]
pub(crate) struct ScriptedRestResponse {
    status: reqwest::StatusCode,
    pub(super) headers: reqwest::header::HeaderMap,
    body: Bytes,
}

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
#[derive(Default)]
pub(super) struct ScriptedRest {
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

impl AuxRequest {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

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
    pub(super) fn record_aux(
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

    pub(super) fn record_download(&self, url: &str, range: Option<bifrost_types::ByteRange>) {
        let mut scripted = self.inner.scripted.lock().expect("REST script lock");
        if scripted.wire.is_none() {
            return;
        }
        scripted.download_requests.push(DownloadRequest {
            url: url.to_string(),
            range,
        });
    }

    pub(crate) fn script_aux(&self, responses: impl IntoIterator<Item = ScriptedRestResponse>) {
        self.script_wire(
            responses.into_iter().map(ScriptedRestResponse::into_canned),
            RetryPolicy::disabled(),
        );
    }

    /// Script aux responses that never arrive: the request is dispatched and
    /// then waits forever, for a test that needs a deadline or a shutdown to
    /// land while a request is outstanding.
    pub(crate) fn script_aux_pending(&self, count: usize) {
        self.script_wire(
            std::iter::repeat_with(|| bifrost_net::test_support::Canned::Pending).take(count),
            RetryPolicy::disabled(),
        );
    }

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

    pub(crate) fn script_downloads(&self, downloads: impl IntoIterator<Item = ScriptedDownload>) {
        self.script_wire(
            downloads.into_iter().map(ScriptedDownload::into_canned),
            RetryPolicy::disabled(),
        );
    }

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
    pub(super) fn record_wire(
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
            content_type: body.map_or("", |body| body.content_type).to_string(),
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
    pub(crate) fn script_wire(
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
    pub(crate) fn wire_attempts(&self) -> usize {
        self.inner
            .scripted
            .lock()
            .expect("REST script lock")
            .wire
            .as_ref()
            .map_or(0, |wire| wire.requests().len())
    }

    /// Actual transport requests, including emitted headers.
    pub(crate) fn rest_requests(&self) -> Vec<bifrost_net::test_support::RequestSnapshot> {
        self.inner
            .scripted
            .lock()
            .expect("REST script lock")
            .wire
            .as_ref()
            .expect("script installed")
            .requests()
    }

    /// The scripted transport, when one is installed. Resolved through the
    /// SHARED scripting state rather than this client's own `account_net`
    /// slot, so a client derived after the script was installed still
    /// answers from it.
    pub(super) fn scripted_net(&self) -> Option<AccountNet> {
        self.inner
            .scripted
            .lock()
            .expect("REST script lock")
            .wire_net
            .clone()
    }

    pub(crate) fn script_rest(&self, responses: impl IntoIterator<Item = ScriptedRestResponse>) {
        self.script_wire(
            responses.into_iter().map(ScriptedRestResponse::into_canned),
            RetryPolicy::disabled(),
        );
    }

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
