use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, Stream, StreamExt, stream::SplitSink};
use http::{HeaderValue, Uri, header};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio_websockets::{ClientBuilder, Connector, MaybeTlsStream, Message, WebSocketStream};

use crate::{
    DataType, PushObject,
    client::Client,
    core::{
        error::{ProblemDetails, ProblemType},
        request::Request,
        response::Response,
    },
};

const JMAP_WS_SUBPROTOCOL: &str = "jmap";

#[derive(Debug, Serialize)]
struct WebSocketRequest {
    #[serde(rename = "@type")]
    pub(crate) _type: WebSocketRequestType,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,

    using: Vec<&'static str>,

    #[serde(rename = "methodCalls")]
    method_calls: serde_json::Value,

    #[serde(rename = "createdIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    created_ids: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WebSocketResponse {
    /// The `id` of the request this frame answers (RFC 8887 s4.3.4),
    /// echoed by the server when the request carried one. Dropping it made
    /// two in-flight WebSocket requests indistinguishable on the read
    /// stream: `send_ws` assigned an id that nothing downstream could
    /// ever see again.
    #[serde(rename = "requestId", default)]
    request_id: Option<String>,

    /// The `methodResponses` array, kept as the raw JSON slice the frame
    /// parse already saw. It used to be a `Vec<serde_json::Value>`, which
    /// was then rebuilt into a `json!` envelope and deserialized a second
    /// time - a full `Value` tree plus a re-walk for every method response
    /// on every frame. Handing the raw slice to
    /// `Response::from_frame_parts` decodes each call result exactly once,
    /// while still leaving `sessionState` readable when the array does not
    /// decode.
    #[serde(rename = "methodResponses")]
    method_responses: Box<serde_json::value::RawValue>,

    #[serde(rename = "createdIds")]
    created_ids: Option<HashMap<String, String>>,

    #[serde(rename = "sessionState")]
    session_state: String,
}

#[derive(Debug, Serialize)]
struct WebSocketPushEnable {
    #[serde(rename = "@type")]
    _type: WebSocketPushEnableType,

    #[serde(rename = "dataTypes")]
    data_types: Option<Vec<DataType>>,

    #[serde(rename = "pushState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    push_state: Option<String>,
}

#[derive(Debug, Serialize)]
struct WebSocketPushDisable {
    #[serde(rename = "@type")]
    _type: WebSocketPushDisableType,
}

#[derive(Debug, Serialize)]
enum WebSocketRequestType {
    Request,
}

#[derive(Debug, Serialize)]
enum WebSocketPushEnableType {
    WebSocketPushEnable,
}

#[derive(Debug, Serialize)]
enum WebSocketPushDisableType {
    WebSocketPushDisable,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WebSocketError {
    #[serde(rename = "requestId")]
    pub(crate) request_id: Option<String>,

    #[serde(rename = "type")]
    p_type: ProblemType,
    status: Option<u32>,
    title: Option<String>,
    detail: Option<String>,
    limit: Option<String>,
}

// RFC 8887 frames carry exactly one `@type` discriminator, and the
// payload fields sit beside it rather than nested under it. A nested
// `#[serde(tag = "@type")]` type (e.g. `PushObject`) would look for a
// second `@type` that the wire never sends and fail with
// `protocol.parse-failed`, so each variant's payload struct inlines the
// post-tag fields and the match arms below rebuild a `PushObject` for
// the downstream stream output.
//
// The dispatch is hand-written rather than `#[serde(tag = "@type")]`
// because an internally-tagged enum buffers the whole frame into
// serde's private `Content` tree before it picks a variant, and a
// `RawValue` field cannot survive that buffer (it comes back out as the
// newtype token, not as JSON). Keeping `methodResponses` raw is the
// whole point of this path, so the tag is read by itself first - a skim
// that allocates only the tag string and builds no value tree - and the
// chosen variant is then deserialized straight from the same bytes.
#[derive(Debug)]
enum WebSocketMessage_ {
    Response(WebSocketResponse),
    StateChange(WebSocketStateChange),
    #[cfg(feature = "calendars")]
    CalendarAlert(crate::CalendarAlert),
    RequestError(WebSocketError),
    /// A known push message whose fields could not be decoded. The
    /// connection stays usable, but consumers must reconcile broadly because
    /// the malformed frame may have contained a state change.
    MalformedPush,
    /// A message type this build does not understand. RFC 8887 allows a
    /// client to ignore messages outside the defined response/push shapes.
    Ignored,
}

#[derive(Debug, Deserialize)]
struct FrameType {
    #[serde(rename = "@type")]
    ty: String,
}

#[derive(Debug, Deserialize)]
struct WebSocketStateChange {
    changed: HashMap<String, HashMap<DataType, String>>,
    #[serde(rename = "pushState", default)]
    push_state: Option<String>,
}

/// Decode one RFC 8887 frame: read the `@type` discriminator, then
/// deserialize the frame's own bytes into the variant it names. Unknown
/// types are ignored for forward compatibility. A malformed push payload is
/// preserved as a soft frame error so the push lane can issue a broad
/// invalidation without killing the connection.
fn decode_frame(payload: &[u8]) -> Result<WebSocketMessage_, serde_json::Error> {
    let FrameType { ty } = serde_json::from_slice(payload)?;

    Ok(match ty.as_str() {
        "Response" => WebSocketMessage_::Response(serde_json::from_slice(payload)?),
        "StateChange" => match serde_json::from_slice(payload) {
            Ok(message) => WebSocketMessage_::StateChange(message),
            Err(_) => WebSocketMessage_::MalformedPush,
        },
        #[cfg(feature = "calendars")]
        "CalendarAlert" => match serde_json::from_slice(payload) {
            Ok(message) => WebSocketMessage_::CalendarAlert(message),
            Err(_) => WebSocketMessage_::MalformedPush,
        },
        "RequestError" => WebSocketMessage_::RequestError(serde_json::from_slice(payload)?),
        _ => WebSocketMessage_::Ignored,
    })
}

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum WebSocketMessage {
    Response {
        /// The `requestId` the server echoed, when it sent one. This is
        /// what lets a reader match a response frame to the request
        /// `send_ws` put on the wire; without it, two in-flight requests
        /// are indistinguishable.
        request_id: Option<String>,
        response: Response,
    },
    PushNotification(PushObject),
    /// An unknown message type was ignored. This lets the push reader count
    /// peer traffic without surfacing an event to account consumers.
    IgnoredFrame,
    /// A known push frame could not be decoded. The reader turns this into
    /// an unknown invalidation so no state change is lost silently.
    UnknownFrame,
    /// A control-frame pong. Surfaced rather than swallowed because it is
    /// the only evidence the push reader can get that a silent connection
    /// is still alive: JMAP defines no application-level keepalive, so a
    /// half-open socket and a quiet mailbox look identical on this stream
    /// until something answers a ping.
    Pong,
}

pub(crate) struct WsStream {
    tx: SplitSink<WebSocketStream<bifrost_net::MeteredStream<MaybeTlsStream<TcpStream>>>, Message>,
}

/// Callers waiting for a WebSocket response, keyed by the RFC 8887
/// `requestId` their request carried.
///
/// The map is the await side of the WebSocket request door: `send_ws`
/// alone puts a frame on the wire and returns an id, which correlates
/// nothing on its own because the read stream is consumed by the push
/// reader. Registering here before the frame is written, and routing
/// matching response frames out of `frame_stream`, is what lets a caller
/// send over WebSocket and get its own answer back.
///
/// Entries are stamped with a CONNECTION GENERATION. A waiter can only
/// ever be answered by the connection it was registered on, so a
/// reconnect fails every older-generation waiter with a retryable error
/// rather than leaving it parked on a socket that no longer exists - and,
/// symmetrically, an old read stream that is drained to EOF *after* the
/// reconnect fails only its own generation and cannot reap a waiter
/// belonging to the live connection.
pub(crate) struct PendingRequests {
    inner: std::sync::Mutex<PendingInner>,
}

struct PendingInner {
    generation: u64,
    waiters: HashMap<String, Waiter>,
}

struct Waiter {
    generation: u64,
    tx: tokio::sync::oneshot::Sender<crate::Result<Response>>,
}

impl PendingRequests {
    pub(crate) fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(PendingInner {
                generation: 0,
                waiters: HashMap::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PendingInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Open a new connection generation: every waiter still registered
    /// belongs to a connection that is being replaced and can never be
    /// answered, so each is failed with `error()`. Returns the generation
    /// requests sent on the new connection must register under.
    pub(crate) fn begin_connection(&self, error: impl Fn() -> crate::Error) -> u64 {
        let mut inner = self.lock();
        inner.generation = inner.generation.wrapping_add(1);
        let generation = inner.generation;
        for (_, waiter) in inner.waiters.drain() {
            let _ = waiter.tx.send(Err(error()));
        }
        generation
    }

    /// Register `id` under `generation`. The returned handle deregisters
    /// on drop, so a caller that goes away before its response arrives
    /// leaves nothing behind for the reader to route to.
    pub(crate) fn register(self: &Arc<Self>, id: String, generation: u64) -> PendingResponse {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.lock()
            .waiters
            .insert(id.clone(), Waiter { generation, tx });
        PendingResponse {
            pending: Arc::clone(self),
            id,
            rx,
        }
    }

    /// Hand `outcome` to the waiter registered for `id`.
    ///
    /// Returns the outcome back when there is no such waiter - an id
    /// nobody is waiting on, or one whose caller was dropped between the
    /// lookup and the send. The reader then yields it on the stream
    /// exactly as it did before this map existed, so an unknown id
    /// wedges nothing.
    fn resolve(
        &self,
        id: &str,
        outcome: crate::Result<Response>,
    ) -> Result<(), crate::Result<Response>> {
        let Some(waiter) = self.lock().waiters.remove(id) else {
            return Err(outcome);
        };
        waiter.tx.send(outcome)
    }

    /// Fail every waiter belonging to `generation`. Used when a read
    /// stream ends: whatever that connection had not answered by then it
    /// never will.
    fn fail_generation(&self, generation: u64, error: impl Fn() -> crate::Error) {
        let mut inner = self.lock();
        let ids: Vec<String> = inner
            .waiters
            .iter()
            .filter(|(_, waiter)| waiter.generation == generation)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if let Some(waiter) = inner.waiters.remove(&id) {
                let _ = waiter.tx.send(Err(error()));
            }
        }
    }

    /// The generation new registrations belong to. Read under the
    /// client's WebSocket sink lock, which is what makes it the
    /// generation of the connection the frame is about to be written to.
    pub(crate) fn generation(&self) -> u64 {
        self.lock().generation
    }

    fn remove(&self, id: &str) {
        self.lock().waiters.remove(id);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().waiters.len()
    }
}

/// A registered wait for one WebSocket response.
///
/// Dropping it deregisters the id. That is the whole teardown story for
/// the caller side: a waiter that goes away before its frame arrives
/// leaves no entry behind, so the map cannot grow without bound on
/// cancelled or timed-out calls, and the late frame falls through to the
/// stream as an unrouted response.
pub(crate) struct PendingResponse {
    pending: Arc<PendingRequests>,
    id: String,
    rx: tokio::sync::oneshot::Receiver<crate::Result<Response>>,
}

impl PendingResponse {
    pub(crate) fn request_id(&self) -> &str {
        &self.id
    }

    /// Await the correlated response.
    ///
    /// A dropped sender means the registration was torn down without an
    /// answer (the connection went away by a route that did not fail it
    /// explicitly); report it as the same retryable close the reconnect
    /// path reports rather than hanging.
    pub(crate) async fn response(mut self) -> crate::Result<Response> {
        match (&mut self.rx).await {
            Ok(outcome) => outcome,
            Err(_) => Err(crate::Error::WebSocketClosed),
        }
    }
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        self.pending.remove(&self.id);
    }
}

impl Client {
    pub(crate) async fn connect_ws(
        &self,
    ) -> crate::Result<Pin<Box<impl Stream<Item = crate::Result<WebSocketMessage>> + use<>>>> {
        tokio::time::timeout(self.timeout, async {
            let mut retried = false;
            loop {
                let (authorization, used) = match &self.authorization {
                    crate::client::Authorization::Bearer(source) => {
                        let token = source.current().await.map_err(|error| {
                            crate::Error::from(crate::core::transport::TransportError::from_net(
                                error,
                            ))
                        })?;
                        (format!("Bearer {}", token.as_str()), Some(token))
                    }
                    crate::client::Authorization::Basic(_) => (self.authorization().await?, None),
                };
                let result = self.connect_ws_inner(&authorization).await;
                if !retried
                    && matches!(
                        &result,
                        Err(crate::Error::WebSocketHandshake(
                            tokio_websockets::Error::Upgrade(
                                tokio_websockets::upgrade::Error::DidNotSwitchProtocols(401)
                            )
                        ))
                    )
                    && let (crate::client::Authorization::Bearer(source), Some(used)) =
                        (&self.authorization, used)
                {
                    source.refresh_if_unchanged(used).await.map_err(|error| {
                        crate::Error::from(crate::core::transport::TransportError::from_net(error))
                    })?;
                    retried = true;
                    continue;
                }
                return result;
            }
        })
        .await
        .map_err(|_| {
            crate::Error::from(crate::core::transport::TransportError::from_net(
                bifrost_net::Error::Timeout {
                    transmission_state: bifrost_net::TransmissionState::Unsent,
                },
            ))
        })?
    }

    async fn connect_ws_inner(
        &self,
        authorization: &str,
    ) -> crate::Result<Pin<Box<impl Stream<Item = crate::Result<WebSocketMessage>> + use<>>>> {
        let session = self.session();
        // Absent and malformed part company here. No websocket block is
        // "this server does not do websockets" - `WebSocketNotConnected`,
        // which the account layer maps to `Unsupported`. A block that is
        // present and unparseable is the server contradicting its own
        // advertisement, and reporting that as "unsupported" hides a
        // server bug behind a feature flag.
        let capabilities = match session.websocket_capability_state() {
            crate::core::session::CapabilityState::Present(capabilities) => capabilities,
            crate::core::session::CapabilityState::Absent => {
                return Err(crate::Error::WebSocketNotConnected);
            }
            crate::core::session::CapabilityState::Malformed => {
                return Err(crate::Error::MalformedCapability {
                    capability: <crate::core::capability::WebSocket as crate::core::capability::Capability>::URI,
                });
            }
        };

        // The URL is the server's, from its own capability object, so an
        // unusable one is the provider's malformed session, not the caller's.
        const URL_PROPERTY: &str = "urn:ietf:params:jmap:websocket url";
        let url = capabilities.url().to_string();
        let uri: Uri =
            url.parse().map_err(
                |e: http::uri::InvalidUri| crate::Error::MalformedSessionUrl {
                    property: URL_PROPERTY,
                    detail: format!("{e}: {url}"),
                },
            )?;

        let auth_value =
            HeaderValue::from_str(authorization).map_err(crate::Error::from_invalid_header)?;

        // Match the parsed scheme, not a string prefix: `WSS://` must still
        // get TLS, and a non-websocket scheme in the capability object is a
        // malformed session, not something to hand the connector.
        let scheme = uri.scheme_str().map(str::to_ascii_lowercase);
        let connector = match scheme.as_deref() {
            Some("wss") => {
                let native = native_tls::TlsConnector::builder()
                    .danger_accept_invalid_certs(self.accept_invalid_certs)
                    .build()
                    .map_err(crate::Error::from_tls)?;
                Some(Connector::NativeTls(tokio_native_tls::TlsConnector::from(
                    native,
                )))
            }
            Some("ws") => None,
            _ => {
                return Err(crate::Error::MalformedSessionUrl {
                    property: URL_PROPERTY,
                    detail: format!("non-websocket scheme: {url}"),
                });
            }
        };

        // This connector does not follow redirects: an upgrade target cannot
        // carry credentials onward to a Location supplied by a different host.
        // It uses a direct TCP connection; reqwest proxy configuration applies
        // only to HTTP; Net exposes no proxy resolution or tunnel connector.
        // Net's trace context builder is private to its HTTP pipeline, so this
        // path does not invent a second traceparent generation policy.
        let builder = ClientBuilder::from_uri(uri.clone())
            .add_header(
                header::USER_AGENT,
                HeaderValue::from_static(concat!("bifrost-jmap/", env!("CARGO_PKG_VERSION"))),
            )
            .map_err(crate::Error::WebSocketHandshake)?
            .add_header(header::AUTHORIZATION, auth_value)
            .map_err(crate::Error::WebSocketHandshake)?
            .add_header(
                header::SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static("jmap"),
            )
            .map_err(crate::Error::WebSocketHandshake)?;
        let host = uri
            .host()
            .ok_or_else(|| crate::Error::MalformedSessionUrl {
                property: URL_PROPERTY,
                detail: format!("missing host: {url}"),
            })?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let port = uri
            .port_u16()
            .unwrap_or(if scheme.as_deref() == Some("wss") {
                443
            } else {
                80
            });
        let tcp = TcpStream::connect((host, port))
            .await
            .map_err(|e| crate::Error::WebSocketHandshake(e.into()))?;
        let stream = connector
            .unwrap_or(Connector::Plain)
            .wrap(host, tcp)
            .await
            .map_err(crate::Error::WebSocketHandshake)?;
        // Admission covers plaintext upgrade bytes and frame headers as well
        // as message payloads, before the framing library buffers them.
        let stream = bifrost_net::MeteredStream::new(stream, self.transport().bandwidth_budget())
            .with_meter_sink(Some(self.transport().websocket_meter()));
        let (stream, response) = builder
            .connect_on(stream)
            .await
            .map_err(crate::Error::WebSocketHandshake)?;
        validate_ws_subprotocol(&response)?;
        let (tx, rx) = stream.split();

        // Install the new sink and open the new pending-request
        // generation under the SAME lock. `send_ws` registers its waiter
        // while holding this lock too, so registration and connection
        // replacement are serialized: a waiter is only ever registered
        // against an installed connection, and every waiter of the
        // connection being replaced is failed here with a retryable
        // error instead of parking forever on a socket that is gone.
        let mut sink = self.ws.lock().await;
        let generation = self
            .ws_pending
            .begin_connection(|| crate::Error::WebSocketClosed);
        *sink = WsStream { tx }.into();
        drop(sink);

        // The read half must run the same session-divergence check the
        // HTTP door runs. A `Client` clone is an `Arc` bump and the stream
        // outlives this call, so the closure owns one.
        let client = self.clone();
        let pending = Arc::clone(&self.ws_pending);
        Ok(Box::pin(frame_stream(
            Box::pin(rx),
            pending,
            generation,
            move |session_state| {
                client.note_session_state(session_state);
            },
        )))
    }

    /// Put a request frame on the WebSocket and return the `requestId`
    /// it was assigned. Fire-and-forget: nothing waits for the answer.
    /// Use [`Client::send_ws_awaiting`] to get the correlated response.
    pub(crate) async fn send_ws(&self, request: Request<'_>) -> crate::Result<String> {
        let (frame, request_id) = self.encode_ws_request(request)?;
        let mut sink = self.ws.lock().await;
        sink.as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::text(frame))
            .await
            .map_err(crate::Error::WebSocketSend)?;

        Ok(request_id)
    }

    /// Send a request over the WebSocket and hand back a handle that
    /// resolves to the response frame carrying the same `requestId`.
    ///
    /// The waiter is registered BEFORE the frame is written and while the
    /// sink lock is held, so a server that answers immediately cannot
    /// beat the registration, and a reconnect cannot slip between the two
    /// and leave a waiter attached to a connection that never carried the
    /// request. If the write fails, the returned handle is dropped on the
    /// error path and the registration goes with it.
    /// This request/response lane is part of RFC 8887 even though the sync
    /// account currently uses the WebSocket connection only for push.
    pub(crate) async fn send_ws_awaiting(
        &self,
        request: Request<'_>,
    ) -> crate::Result<PendingResponse> {
        let (frame, request_id) = self.encode_ws_request(request)?;
        let mut sink = self.ws.lock().await;
        let ws = sink
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?;
        let pending = self
            .ws_pending
            .register(request_id, self.ws_pending.generation());
        ws.tx
            .send(Message::text(frame))
            .await
            .map_err(crate::Error::WebSocketSend)?;

        Ok(pending)
    }

    /// Encode one RFC 8887 `Request` frame, assigning its `requestId`
    /// and enforcing the session's `maxSizeRequest`.
    ///
    /// The size guard is on the encoded FRAME, not on the HTTP body the
    /// same calls would have produced: `maxSizeRequest` bounds the
    /// request the server receives, and over WebSocket that request is
    /// the frame, envelope and `requestId` included.
    fn encode_ws_request(&self, request: Request<'_>) -> crate::Result<(String, String)> {
        // Assign a request id. The counter is the CLIENT's, so ids do not
        // repeat across reconnects: a per-connection counter restarted at
        // 0, and a late response from the old connection then carried an
        // id the new one was about to reuse.
        let max_size_request = request
            .session_state()
            .session()
            .core_capabilities()
            .and_then(crate::core::session::CoreCapabilities::max_size_request);
        let request_id = self.next_ws_request_id();

        let method_calls =
            serde_json::to_value(&request.method_calls).map_err(crate::Error::RequestEncode)?;
        let frame = serde_json::to_string(&WebSocketRequest {
            _type: WebSocketRequestType::Request,
            id: request_id.clone().into(),
            using: request.using,
            method_calls,
            created_ids: request.created_ids,
        })
        .map_err(crate::Error::RequestEncode)?;

        // Only an advertised, non-zero limit is enforced, for the same
        // reason `CallLimit` refuses to enforce its two unusable states:
        // an absent or zero `maxSizeRequest` is a session-validation
        // matter, and enforcing it here would turn it into a client bug
        // on a request that could never fit anything.
        // An OMITTED `maxSizeRequest` and an advertised `0` are both
        // unusable here for the same reason `CallLimit` declines both of
        // its unusable states, but they are distinct on the wire and the
        // session validator classifies them differently.
        if let Some(max) = max_size_request
            && max > 0
            && frame.len() > max
        {
            return Err(crate::Error::RequestSizeLimit {
                max,
                size: frame.len(),
            });
        }

        Ok((frame, request_id))
    }

    pub(crate) async fn enable_push_ws(
        &self,
        data_types: Option<impl IntoIterator<Item = DataType>>,
        push_state: Option<impl Into<String>>,
    ) -> crate::Result<()> {
        let frame = serde_json::to_string(&WebSocketPushEnable {
            _type: WebSocketPushEnableType::WebSocketPushEnable,
            data_types: data_types.map(|it| it.into_iter().collect()),
            push_state: push_state.map(std::convert::Into::into),
        })
        .map_err(crate::Error::RequestEncode)?;
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::text(frame))
            .await
            .map_err(crate::Error::WebSocketSend)
    }

    pub(crate) async fn disable_push_ws(&self) -> crate::Result<()> {
        let frame = serde_json::to_string(&WebSocketPushDisable {
            _type: WebSocketPushDisableType::WebSocketPushDisable,
        })
        .map_err(crate::Error::RequestEncode)?;
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::text(frame))
            .await
            .map_err(crate::Error::WebSocketSend)
    }

    pub(crate) async fn ws_ping(&self) -> crate::Result<()> {
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::ping(Bytes::new()))
            .await
            .map_err(crate::Error::WebSocketSend)
    }
}

/// Frame-level decoding of an established JMAP WebSocket read half.
///
/// Split out from `connect_ws` so the close / error / binary arms can be
/// driven from an in-memory stream of `Message`s without a socket: the
/// transport half is the only part of `connect_ws` that needs a real
/// connection.
/// `on_session_state` receives every response frame's `sessionState`, so
/// the WebSocket door detects session divergence exactly as the HTTP door
/// does. It is a callback rather than a `Client` handle so this function
/// stays drivable from an in-memory frame transcript.
///
/// `pending` is the correlation map: a `Response` or `RequestError` frame
/// whose `requestId` a caller is waiting on is routed to that caller and
/// NOT yielded, since the outcome belongs to whoever asked for it. Every
/// other frame - all push traffic, pongs, and any response for an id
/// nobody is waiting on - is yielded exactly as before, so the push
/// reader is unaffected. When the stream ends, waiters of `generation`
/// are failed: this connection will answer nothing further.
fn frame_stream<S, F>(
    mut rx: S,
    pending: Arc<PendingRequests>,
    generation: u64,
    mut on_session_state: F,
) -> impl Stream<Item = crate::Result<WebSocketMessage>> + use<S, F>
where
    S: Stream<Item = Result<Message, tokio_websockets::Error>> + Unpin,
    F: FnMut(&str),
{
    async_stream::stream! {
        let mut saw_close = false;
        let mut invalid_data = false;

        while let Some(message) = rx.next().await {
            match message {
                Ok(message) if message.is_text() => {
                    let payload = message.into_payload();
                    match decode_frame(payload.as_ref()) {
                        Ok(message) => match message {
                            WebSocketMessage_::Response(response) => {
                                // Session divergence is checked on the frame's own
                                // `sessionState`, before the method responses are
                                // touched: a frame whose
                                // method responses fail to decode still carried a
                                // truthful session state, and dropping that would
                                // leave the client running on a session the server
                                // has already replaced.
                                on_session_state(&response.session_state);
                                let request_id = response.request_id;
                                // Decode the method responses in place, from
                                // the raw slice the frame parse already
                                // captured: one pass, no intermediate
                                // `serde_json::Value` envelope.
                                let outcome = Response::from_frame_parts(
                                    &response.method_responses,
                                    response.created_ids,
                                    response.session_state,
                                )
                                .map_err(crate::Error::ResponseDecode);
                                // Route to the caller waiting on this id, if
                                // any. A decode failure goes to the waiter too:
                                // the frame WAS its answer, and yielding the
                                // error onto the push stream instead would leave
                                // that caller parked until the connection died.
                                // If nobody owns a malformed response, closing
                                // the connection below makes it a transient
                                // reconnect instead of a terminal push error.
                                let unrouted = match request_id.as_deref() {
                                    Some(id) => pending.resolve(id, outcome).err(),
                                    None => Some(outcome),
                                };
                                if let Some(outcome) = unrouted {
                                    match outcome {
                                        Ok(response) => yield Ok(WebSocketMessage::Response {
                                            request_id,
                                            response,
                                        }),
                                        Err(crate::Error::ResponseDecode(_)) => {
                                            invalid_data = true;
                                            break;
                                        }
                                        Err(e) => yield Err(e),
                                    }
                                }
                            }
                            WebSocketMessage_::StateChange(WebSocketStateChange { changed, push_state }) => {
                                yield Ok(WebSocketMessage::PushNotification(PushObject::StateChange { changed, push_state }))
                            }
                            #[cfg(feature = "calendars")]
                            WebSocketMessage_::CalendarAlert(alert) => {
                                yield Ok(WebSocketMessage::PushNotification(PushObject::CalendarAlert(alert)))
                            }
                            WebSocketMessage_::RequestError(err) => {
                                // A request-level error naming a requestId is
                                // that request's answer, so it fails the waiter
                                // rather than riding the push stream. An error
                                // with no id - notably the asynchronous
                                // rejection of a push-enable frame, which
                                // carries no id - is yielded exactly as before,
                                // which is what the push reader's reconnect
                                // logic reads.
                                let request_id = err.request_id.clone();
                                let error: crate::Error = ProblemDetails::from(err).into();
                                let unrouted = match request_id.as_deref() {
                                    Some(id) => pending.resolve(id, Err(error)).err(),
                                    None => Some(Err(error)),
                                };
                                if let Some(Err(error)) = unrouted {
                                    yield Err(error);
                                }
                            }
                            WebSocketMessage_::MalformedPush => {
                                yield Ok(WebSocketMessage::UnknownFrame);
                            }
                            WebSocketMessage_::Ignored => {
                                yield Ok(WebSocketMessage::IgnoredFrame);
                            }
                        },
                        Err(_) => {
                            invalid_data = true;
                            break;
                        }
                    }
                }
                Ok(message) if message.is_binary() => {
                    invalid_data = true;
                    break;
                }
                Ok(message) if message.is_close() => {
                    saw_close = true;
                }
                Ok(message) if message.is_pong() => {
                    yield Ok(WebSocketMessage::Pong);
                }
                Ok(_) => (),
                // Post-handshake runtime drop. Classification is
                // `Protocol(PartialResponse) + Attempt(Acknowledged)`.
                // The stream yields the error and keeps reading rather than
                // ending: recoverability is the consumer's call. The sync
                // push reader breaks on first error; a future consumer that
                // does not must, or a persistently erroring socket that
                // never terminates becomes a spin.
                Err(err) => yield Err(crate::Error::WebSocketRuntime(err)),
            }
        }

        // The connection is finished, so nothing it was asked will ever
        // be answered. Fail this generation's waiters with the retryable
        // close rather than leaving them parked. Only this generation:
        // an old stream drained to EOF after a reconnect must not reap a
        // waiter belonging to the live connection.
        pending.fail_generation(generation, || crate::Error::WebSocketClosed);

        if saw_close || invalid_data {
            yield Err(crate::Error::WebSocketClosed);
        }
    }
}

fn validate_ws_subprotocol(response: &http::Response<()>) -> crate::Result<()> {
    let Some(protocol) = response.headers().get(header::SEC_WEBSOCKET_PROTOCOL) else {
        return Err(crate::Error::from_subprotocol(
            "server did not accept the jmap WebSocket subprotocol",
        ));
    };
    let protocol = protocol.to_str().map_err(|e| {
        crate::Error::from_subprotocol(format!("invalid Sec-WebSocket-Protocol header: {e}"))
    })?;

    if protocol.trim() == JMAP_WS_SUBPROTOCOL {
        return Ok(());
    }

    Err(crate::Error::from_subprotocol(format!(
        "server accepted WebSocket subprotocol {protocol:?}, expected {JMAP_WS_SUBPROTOCOL:?}"
    )))
}

impl From<WebSocketError> for ProblemDetails {
    fn from(problem: WebSocketError) -> Self {
        ProblemDetails::new(
            problem.p_type,
            problem.status,
            problem.title,
            problem.detail,
            problem.limit,
            problem.request_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(protocol: Option<HeaderValue>) -> http::Response<()> {
        let mut response = http::Response::new(());
        if let Some(protocol) = protocol {
            response
                .headers_mut()
                .insert(header::SEC_WEBSOCKET_PROTOCOL, protocol);
        }
        response
    }

    #[test]
    fn accepts_jmap_subprotocol() {
        let response = response(Some(HeaderValue::from_static("jmap")));

        validate_ws_subprotocol(&response).unwrap();
    }

    #[test]
    fn rejects_missing_subprotocol() {
        let err = validate_ws_subprotocol(&response(None)).unwrap_err();

        assert!(matches!(
            err,
            crate::Error::WebSocketSetup(crate::WebSocketSetupError::Subprotocol(_))
        ));
    }

    #[test]
    fn rejects_wrong_subprotocol() {
        let response = response(Some(HeaderValue::from_static("other")));
        let err = validate_ws_subprotocol(&response).unwrap_err();

        assert!(matches!(
            err,
            crate::Error::WebSocketSetup(crate::WebSocketSetupError::Subprotocol(_))
        ));
    }

    // A real RFC 8887 StateChange frame carries exactly one `@type`.
    // The frame dispatch consumes it, so the variant's payload struct
    // must inline `changed` directly; a nested `#[serde(tag = "@type")]`
    // payload would demand a second `@type` the wire never sends.
    #[test]
    fn deserializes_single_type_state_change_frame() {
        let frame = r#"{"@type":"StateChange","changed":{"u1138":{"Mailbox":"f9a8d3"}},"pushState":"ps-9"}"#;

        let message = decode_frame(frame.as_bytes()).unwrap();

        let WebSocketMessage_::StateChange(WebSocketStateChange {
            changed,
            push_state,
        }) = message
        else {
            panic!("expected StateChange variant, got {message:?}");
        };

        let by_type = changed.get("u1138").expect("account entry present");
        assert_eq!(by_type.len(), 1);
        assert_eq!(by_type.values().next().map(String::as_str), Some("f9a8d3"));
        assert_eq!(push_state.as_deref(), Some("ps-9"));
    }

    /// Stub read half of a JMAP WebSocket. Same shape as the stub
    /// `HttpTransport` / `SseTransport` doubles the blob and EventSource
    /// tests use: a canned transcript replayed in order, no socket.
    struct StubWsTransport {
        frames: Vec<Result<Message, tokio_websockets::Error>>,
    }

    impl StubWsTransport {
        fn new(frames: Vec<Result<Message, tokio_websockets::Error>>) -> Self {
            Self { frames }
        }

        fn open_ws(self) -> impl Stream<Item = Result<Message, tokio_websockets::Error>> + Unpin {
            futures::stream::iter(self.frames)
        }
    }

    fn io_error() -> tokio_websockets::Error {
        tokio_websockets::Error::Io(std::io::Error::other("peer reset"))
    }

    async fn decode(
        frames: Vec<Result<Message, tokio_websockets::Error>>,
    ) -> Vec<crate::Result<WebSocketMessage>> {
        decode_observing(frames, |_| {}).await
    }

    /// `decode`, with the session states the frames reported handed to
    /// `observe` in arrival order.
    async fn decode_observing(
        frames: Vec<Result<Message, tokio_websockets::Error>>,
        observe: impl FnMut(&str),
    ) -> Vec<crate::Result<WebSocketMessage>> {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        frame_stream(
            StubWsTransport::new(frames).open_ws(),
            pending,
            generation,
            observe,
        )
        .collect::<Vec<_>>()
        .await
    }

    /// `decode`, against a caller-supplied pending map so a test can
    /// register waiters and see which frames the reader routes to them.
    fn decode_with_pending(
        frames: Vec<Result<Message, tokio_websockets::Error>>,
        pending: &Arc<PendingRequests>,
        generation: u64,
    ) -> impl std::future::Future<Output = Vec<crate::Result<WebSocketMessage>>> + use<> {
        let stream = frame_stream(
            StubWsTransport::new(frames).open_ws(),
            Arc::clone(pending),
            generation,
            |_| {},
        );
        async move { stream.collect::<Vec<_>>().await }
    }

    fn response_frame(id: Option<&str>, session_state: &str) -> Message {
        let id = id.map_or(String::new(), |id| format!(r#""requestId":"{id}","#));
        Message::text(format!(
            r#"{{"@type":"Response",{id}"methodResponses":[["Core/echo",{{}},"c0"]],"sessionState":"{session_state}"}}"#
        ))
    }

    #[tokio::test]
    async fn unknown_frame_type_is_ignored_and_later_push_is_decoded() {
        let out = decode(vec![
            Ok(Message::text(
                r#"{"@type":"FutureExtension","value":1}"#.to_string(),
            )),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Ok(WebSocketMessage::IgnoredFrame)));
        assert!(matches!(
            out[1],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
    }

    #[cfg(not(feature = "calendars"))]
    #[tokio::test]
    async fn calendar_alert_frame_is_ignored_when_calendars_are_disabled() {
        let out = decode(vec![
            Ok(Message::text(r#"{"@type":"CalendarAlert"}"#.to_string())),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Ok(WebSocketMessage::IgnoredFrame)));
        assert!(matches!(
            out[1],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn malformed_push_frame_requests_reconcile_without_ending_the_stream() {
        let out = decode(vec![
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":"invalid"}"#.to_string(),
            )),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Ok(WebSocketMessage::UnknownFrame)));
        assert!(matches!(
            out[1],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn binary_frame_ends_the_connection_as_a_retryable_close() {
        let out = decode(vec![
            Ok(Message::binary(Bytes::from_static(b"\x00\x01"))),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Err(crate::Error::WebSocketClosed)));
    }

    #[tokio::test]
    async fn close_frame_yields_closed_only_after_the_stream_ends() {
        let out = decode(vec![
            Ok(Message::close(None, "")),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        // The close arm only records; frames queued behind it are still
        // decoded, and `WebSocketClosed` is emitted last.
        assert_eq!(out.len(), 2);
        assert!(matches!(
            out[0],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
        assert!(matches!(out[1], Err(crate::Error::WebSocketClosed)));
    }

    #[tokio::test]
    async fn stream_end_without_close_frame_is_silent() {
        let out = decode(vec![Ok(Message::ping(Bytes::new()))]).await;

        // Ping / pong fall through the `Ok(_)` arm, and an EOF that was
        // not preceded by a close frame yields nothing at all.
        assert!(out.is_empty(), "expected no items, got {out:?}");
    }

    #[tokio::test]
    async fn runtime_error_is_yielded_and_the_stream_keeps_reading() {
        let out = decode(vec![
            Err(io_error()),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Err(crate::Error::WebSocketRuntime(_))));
        assert!(matches!(
            out[1],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn malformed_text_frame_ends_the_connection_as_a_retryable_close() {
        let out = decode(vec![Ok(Message::text("not json".to_string()))]).await;

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Err(crate::Error::WebSocketClosed)));
    }

    #[tokio::test]
    async fn request_error_frame_becomes_problem_details() {
        let frame = r#"{"@type":"RequestError","requestId":"7","type":"urn:ietf:params:jmap:error:limit","status":400,"limit":"maxSizeRequest"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        let Err(crate::Error::Problem { details, transport }) = &out[0] else {
            panic!("expected Problem, got {:?}", out[0]);
        };
        assert!(transport.is_none());
        assert_eq!(details.status(), Some(400));
        assert_eq!(details.limit(), Some("maxSizeRequest"));
        assert_eq!(details.request_id(), Some("7"));
    }

    /// A `Response` frame decodes into a `Response` in one pass: the
    /// envelope fields it carries beside `@type`/`requestId` are read
    /// straight off the frame, and the `methodResponses` array goes to
    /// `Response::from_frame_parts` as the raw slice the frame parse
    /// already captured.
    #[tokio::test]
    async fn response_frame_decodes_into_a_response() {
        let frame = r#"{"@type":"Response","methodResponses":[["Core/echo",{"hello":true},"c0"]],"createdIds":{"k":"v"},"sessionState":"s-1"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        let Ok(WebSocketMessage::Response { response, .. }) = &out[0] else {
            panic!("expected Response, got {:?}", out[0]);
        };
        assert_eq!(response.session_state(), "s-1");
        assert_eq!(
            response.created_ids().and_then(|ids| ids.get("k")),
            Some(&"v".to_string())
        );
    }

    /// RFC 8887 s4.3.4 echoes `requestId` so a client can match a
    /// `Response` frame to the request it answers. Dropping it made two
    /// in-flight WebSocket requests indistinguishable on the read stream,
    /// while `send_ws` went on assigning ids nothing could ever use.
    #[tokio::test]
    async fn a_response_frame_carries_the_request_id_it_answers() {
        let frame = |id: &str| {
            Ok(Message::text(format!(
                r#"{{"@type":"Response","requestId":"{id}","methodResponses":[["Core/echo",{{}},"c0"]],"sessionState":"s-1"}}"#
            )))
        };
        let out = decode(vec![
            frame("7"),
            frame("8"),
            // A server that echoes no id is still decodable; the absence
            // is reported as absence, not as some other request's id.
            Ok(Message::text(
                r#"{"@type":"Response","methodResponses":[["Core/echo",{},"c0"]],"sessionState":"s-1"}"#
                    .to_string(),
            )),
        ])
        .await;

        let ids = out
            .iter()
            .map(|message| match message {
                Ok(WebSocketMessage::Response { request_id, .. }) => request_id.clone(),
                other => panic!("expected Response, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![Some("7".to_string()), Some("8".to_string()), None]
        );
    }

    /// `Client::send_request` compares every response's `sessionState`
    /// against the session it is running on - the mechanism the whole
    /// scope-lifecycle `CapabilityChanged` story rests on. The WebSocket
    /// door skipped that comparison entirely, handing every response frame
    /// up without ever reading its `sessionState`, so staleness went
    /// undetected on the connection that stays open longest - the one
    /// most likely to outlive a session change. It must observe the
    /// state on every response frame,
    /// including one whose method responses do not decode: that frame's
    /// session state was still truthful.
    #[tokio::test]
    async fn every_response_frame_reports_its_session_state() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = std::sync::Arc::clone(&seen);
        let out = decode_observing(
            vec![
                Ok(Message::text(
                    r#"{"@type":"Response","methodResponses":[["Core/echo",{},"c0"]],"sessionState":"s-1"}"#.to_string(),
                )),
                // Undecodable method responses, truthful session state.
                Ok(Message::text(
                    r#"{"@type":"Response","methodResponses":[[1,2]],"sessionState":"s-2"}"#
                        .to_string(),
                )),
                // The malformed unclaimed response closes the connection, so
                // this later push frame is not read.
                Ok(Message::text(
                    r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
                )),
            ],
            move |state| recorder.lock().expect("seen").push(state.to_string()),
        )
        .await;

        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Ok(WebSocketMessage::Response { .. })));
        assert!(matches!(out[1], Err(crate::Error::WebSocketClosed)));
        assert_eq!(
            *seen.lock().expect("seen"),
            vec!["s-1".to_string(), "s-2".to_string()]
        );
    }

    #[tokio::test]
    async fn unclaimed_response_decode_failure_reconnects_the_connection() {
        // The outer frame parses (methodResponses is a JSON array), but
        // its entries are not call triples. With no matching requestId to
        // deliver the error to, the reader closes this connection so push
        // reconnects rather than treating the bad response as terminal.
        let frame = r#"{"@type":"Response","methodResponses":[[1,2]],"sessionState":"s-1"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Err(crate::Error::WebSocketClosed)));
    }

    /// The point of the correlation map: a caller can send over the
    /// WebSocket and get back ITS answer. Two waiters must be told apart
    /// on one read stream, whatever order the server answers in, and a
    /// routed frame must not also ride the push stream - the outcome
    /// belongs to the caller that asked for it.
    #[tokio::test]
    async fn two_in_flight_requests_are_told_apart_on_the_read_stream() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let seven = pending.register("7".to_string(), generation);
        let eight = pending.register("8".to_string(), generation);

        // Answered out of order, which is exactly the case an id-less
        // reader cannot survive.
        let out = decode_with_pending(
            vec![
                Ok(response_frame(Some("8"), "s-8")),
                Ok(response_frame(Some("7"), "s-7")),
            ],
            &pending,
            generation,
        )
        .await;

        assert!(out.is_empty(), "routed frames must not be yielded: {out:?}");
        assert_eq!(
            seven
                .response()
                .await
                .expect("waiter 7 answered")
                .session_state(),
            "s-7"
        );
        assert_eq!(
            eight
                .response()
                .await
                .expect("waiter 8 answered")
                .session_state(),
            "s-8"
        );
        assert_eq!(pending.len(), 0, "answered waiters must be deregistered");
    }

    /// A response for an id nobody is waiting on - a late frame from a
    /// cancelled call, or a server echoing something we never sent -
    /// must not wedge the reader. It falls through to the stream exactly
    /// as it did before the map existed.
    #[tokio::test]
    async fn a_response_for_an_unknown_id_still_reaches_the_stream() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let waiter = pending.register("7".to_string(), generation);

        let out = decode_with_pending(
            vec![
                Ok(response_frame(Some("999"), "s-unknown")),
                Ok(response_frame(Some("7"), "s-7")),
            ],
            &pending,
            generation,
        )
        .await;

        assert_eq!(out.len(), 1, "only the unrouted frame is yielded: {out:?}");
        let Ok(WebSocketMessage::Response {
            request_id,
            response,
        }) = &out[0]
        else {
            panic!("expected Response, got {:?}", out[0]);
        };
        assert_eq!(request_id.as_deref(), Some("999"));
        assert_eq!(response.session_state(), "s-unknown");
        // The frame the reader could not route did not stop it routing
        // the one it could.
        assert_eq!(
            waiter
                .response()
                .await
                .expect("waiter answered")
                .session_state(),
            "s-7"
        );
    }

    /// Teardown on the caller side: a waiter dropped before its response
    /// must leave no entry behind, or the map grows without bound on
    /// every cancelled or timed-out call. The late frame then falls
    /// through to the stream like any unknown id.
    #[tokio::test]
    async fn a_dropped_waiter_leaves_no_registration_behind() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let waiter = pending.register("7".to_string(), generation);
        assert_eq!(pending.len(), 1);
        drop(waiter);
        assert_eq!(pending.len(), 0, "a dropped waiter must deregister");

        let out = decode_with_pending(
            vec![Ok(response_frame(Some("7"), "s-7"))],
            &pending,
            generation,
        )
        .await;

        assert_eq!(out.len(), 1, "the orphaned frame is yielded: {out:?}");
        assert!(matches!(out[0], Ok(WebSocketMessage::Response { .. })));
    }

    /// A reconnect must fail every waiter of the connection it replaces,
    /// with a retryable error, rather than leaving them parked on a
    /// socket that is gone. `WebSocketClosed` classifies as
    /// `Protocol(PartialResponse)` -> `Retry(SameRequest)`.
    #[tokio::test]
    async fn a_reconnect_fails_every_pending_waiter() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let seven = pending.register("7".to_string(), generation);
        let eight = pending.register("8".to_string(), generation);

        let next = pending.begin_connection(|| crate::Error::WebSocketClosed);

        assert_ne!(next, generation, "a reconnect opens a new generation");
        assert_eq!(pending.len(), 0);
        for waiter in [seven, eight] {
            let id = waiter.request_id().to_string();
            let err = waiter
                .response()
                .await
                .expect_err("a reconnect must fail the waiter");
            assert!(
                matches!(err, crate::Error::WebSocketClosed),
                "waiter {id} got {err:?}"
            );
        }
    }

    /// The other half of the reconnect race: an OLD read stream drained
    /// to EOF after the reconnect must fail only its own generation.
    /// Failing indiscriminately would reap the waiter belonging to the
    /// live connection - the same leak moved one layer over.
    #[tokio::test]
    async fn an_ended_stream_fails_only_its_own_generation() {
        let pending = Arc::new(PendingRequests::new());
        let old_generation = pending.generation();
        let stale = pending.register("7".to_string(), old_generation);

        // Reconnect: the stale waiter is failed here, and a fresh
        // request is sent on the new connection.
        let new_generation = pending.begin_connection(|| crate::Error::WebSocketClosed);
        let live = pending.register("8".to_string(), new_generation);

        // The old stream is only now drained to its end.
        let out = decode_with_pending(vec![], &pending, old_generation).await;
        assert!(out.is_empty());

        assert!(matches!(
            stale.response().await.expect_err("stale waiter failed"),
            crate::Error::WebSocketClosed
        ));
        assert_eq!(
            pending.len(),
            1,
            "the live connection's waiter must survive the old stream ending"
        );

        // And the live connection still answers it.
        let out = decode_with_pending(
            vec![Ok(response_frame(Some("8"), "s-8"))],
            &pending,
            new_generation,
        )
        .await;
        assert!(out.is_empty(), "{out:?}");
        assert_eq!(
            live.response()
                .await
                .expect("live waiter answered")
                .session_state(),
            "s-8"
        );
    }

    /// A stream that ends while a waiter of its own generation is still
    /// registered fails it: that connection will answer nothing further,
    /// and hanging is the one outcome with no recovery.
    #[tokio::test]
    async fn an_ended_stream_fails_its_own_pending_waiters() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let waiter = pending.register("7".to_string(), generation);

        let out =
            decode_with_pending(vec![Ok(Message::close(None, ""))], &pending, generation).await;

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Err(crate::Error::WebSocketClosed)));
        assert!(matches!(
            waiter.response().await.expect_err("waiter failed"),
            crate::Error::WebSocketClosed
        ));
    }

    /// A `RequestError` naming a `requestId` IS that request's answer.
    /// It must fail the waiter rather than ride the push stream, where
    /// the caller would never see it. One carrying no id - notably the
    /// asynchronous rejection of a push-enable frame, which has no id -
    /// still reaches the stream, which is what the push reader's
    /// reconnect logic reads.
    #[tokio::test]
    async fn a_request_error_fails_the_waiter_it_names() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let waiter = pending.register("7".to_string(), generation);

        let out = decode_with_pending(
            vec![
                Ok(Message::text(
                    r#"{"@type":"RequestError","requestId":"7","type":"urn:ietf:params:jmap:error:limit","status":400,"limit":"maxSizeRequest"}"#.to_string(),
                )),
                Ok(Message::text(
                    r#"{"@type":"RequestError","type":"urn:ietf:params:jmap:error:unknownCapability","status":400}"#.to_string(),
                )),
            ],
            &pending,
            generation,
        )
        .await;

        assert_eq!(out.len(), 1, "only the id-less error is yielded: {out:?}");
        let Err(crate::Error::Problem { details, .. }) = &out[0] else {
            panic!("expected Problem, got {:?}", out[0]);
        };
        assert_eq!(details.request_id(), None);

        let Err(crate::Error::Problem { details, .. }) = waiter.response().await else {
            panic!("the named waiter must receive its own error");
        };
        assert_eq!(details.limit(), Some("maxSizeRequest"));
    }

    /// A response frame whose method responses do not decode was still
    /// that request's answer. The error goes to the waiter; yielding it
    /// on the push stream instead would leave the caller parked until
    /// the connection died.
    #[tokio::test]
    async fn an_undecodable_response_frame_fails_its_waiter() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let waiter = pending.register("7".to_string(), generation);

        let out = decode_with_pending(
            vec![Ok(Message::text(
                r#"{"@type":"Response","requestId":"7","methodResponses":[[1,2]],"sessionState":"s-1"}"#
                    .to_string(),
            ))],
            &pending,
            generation,
        )
        .await;

        assert!(
            out.is_empty(),
            "routed to the waiter, not the stream: {out:?}"
        );
        assert!(matches!(
            waiter.response().await.expect_err("waiter failed"),
            crate::Error::ResponseDecode(_)
        ));
    }

    /// Push traffic is untouched by the correlation map: a `StateChange`
    /// frame arriving while a request is in flight is still yielded, and
    /// the waiter is still waiting afterwards.
    #[tokio::test]
    async fn push_frames_are_unaffected_by_a_pending_request() {
        let pending = Arc::new(PendingRequests::new());
        let generation = pending.generation();
        let waiter = pending.register("7".to_string(), generation);

        let out = decode_with_pending(
            vec![
                Ok(Message::text(
                    r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
                )),
                Ok(response_frame(Some("7"), "s-7")),
                Ok(Message::ping(Bytes::new())),
            ],
            &pending,
            generation,
        )
        .await;

        assert_eq!(out.len(), 1, "{out:?}");
        assert!(matches!(
            out[0],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
        assert_eq!(
            waiter
                .response()
                .await
                .expect("waiter answered")
                .session_state(),
            "s-7"
        );
    }

    /// `maxSizeRequest` (RFC 8620 s2) is a hard limit the server rejects
    /// the whole request over. The HTTP door refuses to send an oversized
    /// batch; the WebSocket door had no guard at all, so an oversized
    /// frame went out and came back as an opaque request-level problem.
    /// Enforced on the encoded FRAME, since over WebSocket the frame is
    /// the request.
    #[cfg(feature = "mail")]
    #[test]
    fn an_oversized_websocket_frame_is_refused_before_the_wire() {
        let client = size_limited_client(400);
        let mut request = client.build();
        request
            .call(crate::mailbox::MailboxGet::new())
            .expect("call is added");
        let big = client.build_oversized_request();

        // A small request still encodes.
        let (frame, _) = client
            .encode_ws_request(request)
            .expect("a request within the limit encodes");
        assert!(frame.len() <= 400, "fixture must fit: {}", frame.len());

        let err = client
            .encode_ws_request(big)
            .expect_err("an oversized frame must not reach the wire");
        let crate::Error::RequestSizeLimit { max, size } = err else {
            panic!("expected RequestSizeLimit, got {err:?}");
        };
        assert_eq!(max, 400);
        assert!(size > 400, "reported size {size} must exceed the limit");
    }

    /// An unadvertised or zero `maxSizeRequest` is not enforced, for the
    /// same reason `CallLimit` refuses to enforce its two unusable
    /// states: session validation is the gate for both, and enforcing a
    /// zero here would fail every request as a client bug.
    #[cfg(feature = "mail")]
    #[test]
    fn a_zero_max_size_request_is_not_enforced() {
        let client = size_limited_client(0);
        let big = client.build_oversized_request();

        client
            .encode_ws_request(big)
            .expect("a zero limit is not a bound this door enforces");
    }

    #[cfg(feature = "mail")]
    #[test]
    fn websocket_encoding_uses_the_requests_session_limit() {
        let original = size_limited_client(10000);
        let encoder = size_limited_client(1);
        let request = original.build_oversized_request();
        encoder
            .encode_ws_request(request)
            .expect("the encoder must retain the request's original limit");

        let original = size_limited_client(1);
        let encoder = size_limited_client(10000);
        let request = original.build_oversized_request();
        assert!(matches!(
            encoder.encode_ws_request(request),
            Err(crate::Error::RequestSizeLimit { max: 1, .. })
        ));
    }

    #[cfg(feature = "mail")]
    fn size_limited_client(max_size_request: usize) -> Client {
        let session: crate::core::session::Session = serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": max_size_request,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 64,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 100,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {},
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "A1"},
            "username": "user@example.test",
            "apiUrl": "https://jmap.invalid/api",
            "downloadUrl": "https://jmap.invalid/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://jmap.invalid/upload/{accountId}",
            "eventSourceUrl": "https://jmap.invalid/es",
            "state": "session-1"
        }))
        .expect("session fixture parses");
        let transport = crate::transport_reqwest::ReqwestTransport::new(
            reqwest::header::HeaderMap::new(),
            crate::client::Authorization::Basic(String::new()),
            bifrost_net::AccountId("jmap-ws-size".to_string()),
            std::time::Duration::from_secs(5),
            false,
            Arc::new(std::collections::HashSet::new()),
        )
        .expect("transport builds");
        Client::with_transport(transport, session, "https://jmap.invalid/session")
            .expect("client builds")
    }

    #[cfg(feature = "mail")]
    impl Client {
        /// A request whose encoded frame comfortably exceeds a 400-byte
        /// limit, built from ordinary method calls.
        fn build_oversized_request(&self) -> Request<'_> {
            let mut request = self.build();
            for _ in 0..16 {
                request
                    .call(
                        crate::mailbox::MailboxGet::new()
                            .ids(["a-fairly-long-mailbox-id-value".to_string()]),
                    )
                    .expect("call is added");
            }
            request
        }
    }

    /// A client whose session advertises the WebSocket capability at `url`.
    /// Built on the real reqwest transport, which opens no connection until
    /// asked to.
    fn client_with_websocket_url(url: &str) -> Client {
        let session: crate::core::session::Session = serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:websocket": {"url": url, "supportsPush": true}
            },
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://jmap.invalid/api",
            "downloadUrl": "https://jmap.invalid/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://jmap.invalid/upload/{accountId}",
            "eventSourceUrl": "https://jmap.invalid/es",
            "state": "session-1"
        }))
        .expect("session fixture parses");
        let transport = crate::transport_reqwest::ReqwestTransport::new(
            reqwest::header::HeaderMap::new(),
            crate::client::Authorization::Basic(String::new()),
            bifrost_net::AccountId("jmap-ws-url".to_string()),
            std::time::Duration::from_secs(5),
            false,
            Arc::new(std::collections::HashSet::new()),
        )
        .expect("transport builds");
        Client::with_transport(transport, session, "https://jmap.invalid/session")
            .expect("client builds")
    }

    /// The WebSocket URL comes from the server's own capability object, so
    /// an unparseable one or one with a non-websocket scheme is the
    /// provider's malformed session. Both used to surface as the caller's
    /// `InvalidUrl` (`Request(Malformed)`, "fix your request"). A scheme with
    /// no host (`wss:/push`) never reaches the connector: `http::Uri` refuses
    /// a scheme without an authority, so it is the unparseable case. All are
    /// refused before any connection is attempted, so this is hermetic.
    #[tokio::test]
    async fn an_unusable_websocket_capability_url_is_the_servers_malformed_session() {
        for (url, shape) in [
            ("https://jmap.invalid/ws", "non-websocket scheme"),
            ("wss://bad host/ws", "unparseable URI"),
            ("wss:/push", "scheme with no host"),
        ] {
            let error = client_with_websocket_url(url)
                .connect_ws()
                .await
                .err()
                .expect("the URL is refused before any connection");
            let crate::Error::MalformedSessionUrl { property, detail } = error else {
                panic!("{shape}: expected MalformedSessionUrl, got {error:?}");
            };
            assert!(property.contains("websocket"), "{shape}: {property}");
            assert!(detail.contains(url), "{shape}: {detail}");
        }
    }

    #[cfg(feature = "calendars")]
    #[tokio::test]
    async fn calendar_alert_frame_becomes_a_push_notification() {
        let frame = r#"{"@type":"CalendarAlert","accountId":"a1","calendarEventId":"e1","uid":"u1","alertId":"al1"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        let Ok(WebSocketMessage::PushNotification(PushObject::CalendarAlert(alert))) = &out[0]
        else {
            panic!("expected CalendarAlert, got {:?}", out[0]);
        };
        assert_eq!(alert.alert_id, "al1");
        assert_eq!(alert.recurrence_id, None);
    }
}

#[cfg(test)]
mod bandwidth_tests {
    use super::*;
    use bifrost_net::{BandwidthBudget, BandwidthDirection, MeteredStream};
    use futures::FutureExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
    use tokio::time::{Duration, Instant};

    #[tokio::test(start_paused = true)]
    async fn large_websocket_send_reaches_peer_in_paced_slices() {
        let budget = BandwidthBudget::new(Some(16));
        let (stream, mut peer) = duplex(1024);
        let meter = bifrost_net::BandwidthMeter::new();
        let id = bifrost_net::AccountId("websocket".to_owned());
        meter.register_account(id.clone());
        let observed = meter.account(id);
        let mut ws = ClientBuilder::new().take_over(
            MeteredStream::new(stream, budget).with_meter_sink(Some(observed.sink_handle())),
        );
        let start = Instant::now();
        let mut send = Box::pin(ws.send(Message::text("x".repeat(64))));
        assert!(
            (&mut send).now_or_never().is_none(),
            "a 70-byte frame at 16 B/s cannot go out in one burst"
        );
        send.await.unwrap();
        // A masked 64-byte client frame has six header bytes. However the
        // library splits header and payload into writes, seventy bytes at
        // 16 B/s with one second of burst credit finish at 70/16 - 1 s.
        assert_eq!(start.elapsed(), Duration::from_millis(3375));
        let mut buf = [0; 128];
        let mut received = 0;
        while received < 70 {
            received += peer.read(&mut buf[received..]).await.unwrap();
        }
        assert_eq!(received, 70);
        assert_eq!(observed.bytes_out(), 70);
    }

    #[tokio::test(start_paused = true)]
    async fn inbound_frames_are_admitted_once_including_headers() {
        let payload = br#"{"@type":"StateChange","changed":{}}"#;
        let mut frame = vec![0x81, u8::try_from(payload.len()).unwrap()];
        frame.extend_from_slice(payload);
        let len = frame.len();
        let budget = BandwidthBudget::new(Some(u64::try_from(len).unwrap()));
        let (stream, mut peer) = duplex(1024);
        peer.write_all(&frame).await.unwrap();
        peer.write_all(&frame).await.unwrap();
        let ws = ClientBuilder::new().take_over(MeteredStream::new(stream, budget.clone()));
        let pending = Arc::new(PendingRequests::new());
        let mut decoded = Box::pin(frame_stream(ws, pending, 0, |_| {}));
        let start = Instant::now();
        for _ in 0..2 {
            assert!(matches!(
                decoded.next().await.unwrap().unwrap(),
                WebSocketMessage::PushNotification(_)
            ));
        }
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        // Exactly two full frames were charged, with no message-level debit.
        let next = budget.reserve(BandwidthDirection::Inbound, len);
        assert_eq!(
            next.ready_at().duration_since(start),
            Duration::from_secs(2)
        );
    }
}
