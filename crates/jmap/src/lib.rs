#![forbid(unsafe_code)]
#![doc = "JMAP Account implementation for bifrost."]
// The crate hosts the full JMAP RFC type surface (mail, calendars,
// contacts, principal, sieve, sharing, push) so the Account impl can
// draw on whichever pieces a given stage needs. The Account impl
// today wires a subset; the rest stays built and ready. `wrong_self_convention`
// is allowed because JMAP RFC field names (`fromDate`, `isEnabled`,
// `toDate`) become builder methods that mutate `self`.
#![allow(dead_code)]
#![allow(clippy::wrong_self_convention)]
// JMAP RFC vocabulary uses bare acronyms (JMAP, ACL, DKIM) and
// enum families with shared prefixes (AddressType::As*); both
// shapes are RFC-imposed and won't be renamed.
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::enum_variant_names)]
// Boxed Send futures over deep reqwest/hyper type stacks exceed rustc's
// default auto-trait recursion depth (rust-lang/rust#159228, a
// future-incompat hard error). Raising the limit is the sanctioned fix.
#![recursion_limit = "256"]
// The crate-internal `Error` aggregates JMAP method errors, set errors,
// problem details, and WebSocket failures. These get translated to
// `AccountError` (8 bytes, `Arc<Inner>`-backed) at the protocol boundary,
// so the lint's concern (large Err pessimizing the happy path) only
// applies briefly. Boxing each variant individually would add allocation
// churn for the much more common decode/method paths.
#![allow(clippy::result_large_err)]

pub(crate) mod account;
#[cfg(feature = "contacts")]
pub(crate) mod address_book;
pub(crate) mod blob;
#[cfg(feature = "calendars")]
pub(crate) mod calendar;
#[cfg(feature = "calendars")]
pub(crate) mod calendar_event;
#[cfg(feature = "calendars")]
pub(crate) mod calendar_event_notification;
pub(crate) mod client;
#[cfg(feature = "contacts")]
pub(crate) mod contact_card;
pub(crate) mod core;
#[cfg(feature = "mail")]
pub(crate) mod email;
#[cfg(feature = "mail")]
pub(crate) mod email_submission;
pub(crate) mod event_source;
#[cfg(feature = "mail")]
pub(crate) mod identity;
#[cfg(feature = "mail")]
pub(crate) mod mailbox;
#[cfg(feature = "calendars")]
pub(crate) mod participant_identity;
pub(crate) mod principal;
pub(crate) mod push_subscription;
#[cfg(feature = "quota")]
pub(crate) mod quota;
pub(crate) mod share_notification;
#[cfg(feature = "mail")]
pub(crate) mod sieve;
#[cfg(feature = "sync")]
pub mod sync;
#[cfg(feature = "mail")]
pub(crate) mod thread;
pub(crate) mod transport_reqwest;
#[cfg(feature = "mail")]
pub(crate) mod vacation_response;

use crate::core::error::MethodError;
use crate::core::error::ProblemDetails;
use crate::core::set::SetError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

#[cfg(feature = "websockets")]
pub(crate) mod client_ws;

pub(crate) use crate::core::method::{
    define_changes_method, define_get_method, define_query_changes_method, define_query_method,
    define_set_method,
};
// The remaining macros are gated to exactly the features whose object modules
// invoke them (see their definitions), so the re-exports follow the same gates.
#[cfg(any(feature = "mail", feature = "contacts", feature = "calendars"))]
pub(crate) use crate::core::method::define_copy_method;
#[cfg(any(feature = "contacts", feature = "calendars"))]
pub(crate) use crate::core::{
    __json_object_serde, json_object_struct,
    method::{define_open_property_enum, define_parse_method},
};

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
#[non_exhaustive]
pub(crate) enum DataType {
    // Core (always available)
    Core,
    PushSubscription,
    Principal,
    ShareNotification,

    // Mail
    #[cfg(feature = "mail")]
    Email,
    #[cfg(feature = "mail")]
    EmailDelivery,
    #[cfg(feature = "mail")]
    EmailSubmission,
    #[cfg(feature = "mail")]
    Mailbox,
    #[cfg(feature = "mail")]
    Thread,
    #[cfg(feature = "mail")]
    Identity,
    #[cfg(feature = "mail")]
    SearchSnippet,
    #[cfg(feature = "mail")]
    VacationResponse,
    #[cfg(feature = "mail")]
    Mdn,
    #[cfg(feature = "mail")]
    SieveScript,

    // Calendars
    #[cfg(feature = "calendars")]
    Calendar,
    #[cfg(feature = "calendars")]
    CalendarEvent,
    #[cfg(feature = "calendars")]
    CalendarEventNotification,
    #[cfg(feature = "calendars")]
    ParticipantIdentity,
    #[cfg(feature = "calendars")]
    CalendarAlert,

    // Contacts
    #[cfg(feature = "contacts")]
    AddressBook,
    #[cfg(feature = "contacts")]
    ContactCard,

    // Quota
    #[cfg(feature = "quota")]
    Quota,

    FileNode,

    /// Unknown or feature-gated data type, preserved for wire round-trips.
    Other(String),
}

impl DataType {
    fn parse(value: &str) -> Self {
        match value {
            "Core" => Self::Core,
            "PushSubscription" => Self::PushSubscription,
            "Principal" => Self::Principal,
            "ShareNotification" => Self::ShareNotification,
            #[cfg(feature = "mail")]
            "Email" => Self::Email,
            #[cfg(feature = "mail")]
            "EmailDelivery" => Self::EmailDelivery,
            #[cfg(feature = "mail")]
            "EmailSubmission" => Self::EmailSubmission,
            #[cfg(feature = "mail")]
            "Mailbox" => Self::Mailbox,
            #[cfg(feature = "mail")]
            "Thread" => Self::Thread,
            #[cfg(feature = "mail")]
            "Identity" => Self::Identity,
            #[cfg(feature = "mail")]
            "SearchSnippet" => Self::SearchSnippet,
            #[cfg(feature = "mail")]
            "VacationResponse" => Self::VacationResponse,
            #[cfg(feature = "mail")]
            "MDN" => Self::Mdn,
            #[cfg(feature = "mail")]
            "SieveScript" => Self::SieveScript,
            #[cfg(feature = "calendars")]
            "Calendar" => Self::Calendar,
            #[cfg(feature = "calendars")]
            "CalendarEvent" => Self::CalendarEvent,
            #[cfg(feature = "calendars")]
            "CalendarEventNotification" => Self::CalendarEventNotification,
            #[cfg(feature = "calendars")]
            "ParticipantIdentity" => Self::ParticipantIdentity,
            #[cfg(feature = "calendars")]
            "CalendarAlert" => Self::CalendarAlert,
            #[cfg(feature = "contacts")]
            "AddressBook" => Self::AddressBook,
            #[cfg(feature = "contacts")]
            "ContactCard" => Self::ContactCard,
            #[cfg(feature = "quota")]
            "Quota" => Self::Quota,
            "FileNode" => Self::FileNode,
            other => Self::Other(other.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|value| Self::parse(&value))
    }
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "@type")]
#[non_exhaustive]
pub(crate) enum PushObject {
    StateChange {
        changed: HashMap<String, HashMap<DataType, String>>,
        #[serde(rename = "pushState", default)]
        push_state: Option<String>,
    },
    #[cfg(feature = "mail")]
    EmailPush {
        #[serde(rename = "accountId")]
        account_id: String,
        email: serde_json::Value,
    },
    #[cfg(feature = "calendars")]
    CalendarAlert(CalendarAlert),
    Group {
        entries: Vec<PushObject>,
    },
}

#[cfg(feature = "calendars")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct CalendarAlert {
    #[serde(rename = "accountId")]
    pub(crate) account_id: String,
    #[serde(rename = "calendarEventId")]
    pub(crate) calendar_event_id: String,
    pub(crate) uid: String,
    #[serde(rename = "recurrenceId")]
    pub(crate) recurrence_id: Option<String>,
    #[serde(rename = "alertId")]
    pub(crate) alert_id: String,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "websockets")]
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum WebSocketSetupError {
    /// The client could not construct a valid WebSocket request header.
    InvalidHeader(String),
    /// The TLS connector could not be built before opening the socket.
    Tls(String),
    /// The server did not negotiate the JMAP WebSocket subprotocol.
    Subprotocol(String),
}

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Error {
    /// Transport-level failure (network, TLS, timeout).
    Transport(core::transport::TransportError),
    /// Outbound JSON request serialization failure. Produced when the
    /// crate fails to encode a `Request` or per-method body before the
    /// request crosses the side-effect boundary. Maps to
    /// `Request(Malformed)` / `ClientBug` in the conversion boundary.
    RequestEncode(serde_json::Error),
    /// A request would exceed the session's advertised
    /// `maxCallsInRequest`. Raised before the extra method is added, so
    /// the oversized batch cannot reach the wire.
    RequestCallLimit { max: usize },
    /// An encoded request frame would exceed the session's advertised
    /// `maxSizeRequest` (RFC 8620 s2). Raised before the frame is
    /// written, so the oversized request cannot reach the wire.
    ///
    /// The HTTP door answers the same limit by refusing to send
    /// (`Request::send_methods_within` returns `Ok(None)` against a
    /// caller-supplied bound); the WebSocket door has no such
    /// caller-side contract - `send_ws` either writes the frame or it
    /// does not - so it reports the refusal as an error instead of
    /// letting the server reject the whole frame with an opaque
    /// request-level `limit` problem.
    RequestSizeLimit { max: usize, size: usize },
    /// Inbound JSON response decoding failure. Produced when the JMAP
    /// server's reply or session document cannot be parsed as the
    /// expected shape. Maps to `Protocol(ParseFailed)` /
    /// `ProviderContractViolation`.
    ResponseDecode(serde_json::Error),
    /// Server returned an RFC 7807 problem details response. Carries
    /// the parsed `ProblemDetails` plus, when the failure flowed
    /// through the default reqwest transport, the originating
    /// `TransportError` so the conversion can pull status, headers,
    /// retry hints, and trace IDs straight off `bifrost_net::Error`.
    Problem {
        details: Box<ProblemDetails>,
        transport: Option<core::transport::TransportError>,
    },
    /// A JMAP method call returned an error response.
    Method(MethodError),
    /// A JMAP set operation returned per-object errors.
    Set(SetError<String>),
    /// Requested call ID not found in the response.
    CallNotFound(String),
    /// Server returned a successful response whose method name did not match
    /// the method registered for the call ID.
    UnexpectedMethodResponse {
        call_id: String,
        expected: &'static str,
        actual: String,
    },
    /// Requested object ID not found in set/copy/parse response.
    IdNotFound(String),
    /// Not parsable as the expected format.
    NotParsable(String),
    /// A URL or URL-component value this crate's own caller supplied is
    /// unusable (an empty custom-transport session URL or EventSource type
    /// list, for example). The caller's fault. A URL the SERVER supplied is
    /// [`Error::MalformedSessionUrl`] instead.
    InvalidUrl(String),
    /// A URL or URL template the server published in its session object
    /// (`uploadUrl`, `downloadUrl`, `eventSourceUrl`, the WebSocket
    /// capability's `url`) is not usable: an unbalanced or unknown template
    /// variable, an unparseable URI, a non-websocket scheme. The provider
    /// described itself wrongly, so this is its malformed response, never a
    /// fault in the caller's request.
    MalformedSessionUrl {
        /// The session property that carried the URL.
        property: &'static str,
        /// What was wrong with it, including the offending value.
        detail: String,
    },
    /// The session advertises the named capability but its object does
    /// not parse into the shape the RFC defines for it. Distinct from
    /// "not advertised": the server claimed the capability and then
    /// described it wrongly, which is a contract violation rather than a
    /// missing feature.
    MalformedCapability {
        /// The capability URI whose object failed to parse (e.g.
        /// `urn:ietf:params:jmap:websocket`).
        capability: &'static str,
    },
    /// The session lists no primary account for the requested capability.
    NoPrimaryAccount {
        /// The capability URI that was looked up (e.g.
        /// `urn:ietf:params:jmap:mail`).
        capability: &'static str,
    },
    #[cfg(feature = "websockets")]
    /// WebSocket handshake failure raised by `tokio_websockets` before
    /// the connection is established. Classifies as
    /// `Transport(Network)` + `Attempt(Unsent)` - no bytes have crossed
    /// the side-effect boundary at this point. Distinct from
    /// [`Error::WebSocketRuntime`] which models post-handshake
    /// stream failures.
    WebSocketHandshake(tokio_websockets::Error),
    #[cfg(feature = "websockets")]
    /// WebSocket READ-side failure raised after the handshake completed:
    /// the stream the push reader consumes yielded an error. Classifies as
    /// `Protocol(PartialResponse)` + `Attempt(Acknowledged)`. A failed
    /// WRITE is [`Error::WebSocketSend`], never this.
    WebSocketRuntime(tokio_websockets::Error),
    #[cfg(feature = "websockets")]
    /// Writing a frame to an established connection's sink failed.
    ///
    /// Distinct from [`Error::WebSocketRuntime`] because nothing was
    /// acknowledged: the frame either never left (the library refused it)
    /// or its fate is unknown. `tokio_websockets` refuses with
    /// `AlreadyClosed` from `start_send`, before the frame is queued, once
    /// the stream has left its active state - which the read half does as
    /// soon as it sees the peer's close, a read I/O error, or a protocol
    /// error, and a previous failed write does too. Any other error is an
    /// I/O failure while flushing, when some of the frame may have reached
    /// the wire. Classifies as `Transport(Network)` with `Attempt(Unsent)`
    /// for `AlreadyClosed` and `Attempt(InFlight)` otherwise.
    ///
    /// Every write failure leaves the stream closed for good (the library
    /// moves it to `CloseAcknowledged`), so after one, this connection
    /// will carry no further frame in either direction.
    WebSocketSend(tokio_websockets::Error),
    #[cfg(feature = "websockets")]
    /// WebSocket peer closed the connection.
    WebSocketClosed,
    #[cfg(feature = "websockets")]
    /// WebSocket handshake or TLS setup failed before the connection
    /// was established.
    WebSocketSetup(WebSocketSetupError),
    #[cfg(feature = "websockets")]
    /// WebSocket connection not established.
    WebSocketNotConnected,
}

impl std::error::Error for Error {}

impl From<core::transport::TransportError> for Error {
    fn from(e: core::transport::TransportError) -> Self {
        if let Some(ref body) = e.body
            && let Ok(problem) = serde_json::from_slice::<ProblemDetails>(body)
        {
            return Error::Problem {
                details: Box::new(problem),
                transport: Some(e),
            };
        }
        Error::Transport(e)
    }
}

// Default JSON-error conversion treats failures as response decoding.
// Outbound request encoding sites must use Error::RequestEncode
// explicitly rather than relying on `?`.
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::ResponseDecode(e)
    }
}

impl From<MethodError> for Error {
    fn from(e: MethodError) -> Self {
        Error::Method(e)
    }
}

impl From<ProblemDetails> for Error {
    fn from(e: ProblemDetails) -> Self {
        Error::Problem {
            details: Box::new(e),
            transport: None,
        }
    }
}

impl From<SetError<String>> for Error {
    fn from(e: SetError<String>) -> Self {
        Error::Set(e)
    }
}

// No blanket `From<tokio_websockets::Error> for Error` impl: call sites
// must distinguish pre-handshake (`WebSocketHandshake`) from post-
// handshake (`WebSocketRuntime`) failures so the conversion boundary
// can attach the correct `TransportCause` / `AttemptCause` pair. The
// blanket impl previously mis-routed every websocket error through
// `Protocol(PartialResponse) + Attempt(Acknowledged)`, hiding handshake-
// time TCP/TLS drops behind the wrong recovery class.

#[cfg(feature = "websockets")]
impl Error {
    pub(crate) fn from_invalid_header(e: http::header::InvalidHeaderValue) -> Self {
        Error::WebSocketSetup(WebSocketSetupError::InvalidHeader(e.to_string()))
    }

    pub(crate) fn from_tls(e: native_tls::Error) -> Self {
        Error::WebSocketSetup(WebSocketSetupError::Tls(e.to_string()))
    }

    pub(crate) fn from_subprotocol(message: impl Into<String>) -> Self {
        Error::WebSocketSetup(WebSocketSetupError::Subprotocol(message.into()))
    }
}

#[cfg(feature = "websockets")]
impl Display for WebSocketSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebSocketSetupError::InvalidHeader(msg) => {
                write!(f, "invalid WebSocket header value: {msg}")
            }
            WebSocketSetupError::Tls(msg) => write!(f, "TLS connector build failed: {msg}"),
            WebSocketSetupError::Subprotocol(msg) => write!(f, "{msg}"),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Transport(e) => write!(f, "Transport error: {e}"),
            Error::RequestEncode(e) => write!(f, "Request encode error: {e}"),
            Error::RequestCallLimit { max } => {
                write!(f, "Request exceeds maxCallsInRequest ({max})")
            }
            Error::RequestSizeLimit { max, size } => {
                write!(f, "Request of {size} bytes exceeds maxSizeRequest ({max})")
            }
            Error::ResponseDecode(e) => write!(f, "Response decode error: {e}"),
            Error::Problem { details, .. } => write!(f, "Problem: {details}"),
            Error::Method(e) => write!(f, "Method error: {e}"),
            Error::Set(e) => write!(f, "Set error: {e}"),
            Error::CallNotFound(id) => write!(f, "Call {id} not found in response"),
            Error::UnexpectedMethodResponse {
                call_id,
                expected,
                actual,
            } => write!(f, "Call {call_id} returned {actual}, expected {expected}"),
            Error::IdNotFound(id) => write!(f, "Id {id} not found"),
            Error::NotParsable(id) => write!(f, "{id} is not parsable"),
            Error::InvalidUrl(msg) => write!(f, "Invalid URL: {msg}"),
            Error::MalformedSessionUrl { property, detail } => {
                write!(
                    f,
                    "Session property {property} is not a usable URL: {detail}"
                )
            }
            Error::MalformedCapability { capability } => write!(
                f,
                "Session advertises capability {capability} with an unparseable object"
            ),
            Error::NoPrimaryAccount { capability } => write!(
                f,
                "Session lists no primary account for capability {capability}"
            ),
            #[cfg(feature = "websockets")]
            Error::WebSocketHandshake(e) => write!(f, "WebSocket handshake error: {e}"),
            #[cfg(feature = "websockets")]
            Error::WebSocketRuntime(e) => write!(f, "WebSocket runtime error: {e}"),
            #[cfg(feature = "websockets")]
            Error::WebSocketSend(e) => write!(f, "WebSocket frame write failed: {e}"),
            #[cfg(feature = "websockets")]
            Error::WebSocketClosed => write!(f, "WebSocket connection closed"),
            #[cfg(feature = "websockets")]
            Error::WebSocketSetup(msg) => write!(f, "WebSocket setup failed: {msg}"),
            #[cfg(feature = "websockets")]
            Error::WebSocketNotConnected => write!(f, "WebSocket connection not established"),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Core => write!(f, "Core"),
            DataType::PushSubscription => write!(f, "PushSubscription"),
            DataType::Principal => write!(f, "Principal"),
            DataType::ShareNotification => write!(f, "ShareNotification"),
            DataType::FileNode => write!(f, "FileNode"),
            #[cfg(feature = "mail")]
            DataType::Email => write!(f, "Email"),
            #[cfg(feature = "mail")]
            DataType::EmailDelivery => write!(f, "EmailDelivery"),
            #[cfg(feature = "mail")]
            DataType::EmailSubmission => write!(f, "EmailSubmission"),
            #[cfg(feature = "mail")]
            DataType::Mailbox => write!(f, "Mailbox"),
            #[cfg(feature = "mail")]
            DataType::Thread => write!(f, "Thread"),
            #[cfg(feature = "mail")]
            DataType::Identity => write!(f, "Identity"),
            #[cfg(feature = "mail")]
            DataType::SearchSnippet => write!(f, "SearchSnippet"),
            #[cfg(feature = "mail")]
            DataType::VacationResponse => write!(f, "VacationResponse"),
            #[cfg(feature = "mail")]
            DataType::Mdn => write!(f, "MDN"),
            #[cfg(feature = "mail")]
            DataType::SieveScript => write!(f, "SieveScript"),
            #[cfg(feature = "calendars")]
            DataType::Calendar => write!(f, "Calendar"),
            #[cfg(feature = "calendars")]
            DataType::CalendarEvent => write!(f, "CalendarEvent"),
            #[cfg(feature = "calendars")]
            DataType::CalendarEventNotification => write!(f, "CalendarEventNotification"),
            #[cfg(feature = "calendars")]
            DataType::ParticipantIdentity => write!(f, "ParticipantIdentity"),
            #[cfg(feature = "calendars")]
            DataType::CalendarAlert => write!(f, "CalendarAlert"),
            #[cfg(feature = "contacts")]
            DataType::AddressBook => write!(f, "AddressBook"),
            #[cfg(feature = "contacts")]
            DataType::ContactCard => write!(f, "ContactCard"),
            #[cfg(feature = "quota")]
            DataType::Quota => write!(f, "Quota"),
            DataType::Other(value) => write!(f, "{value}"),
        }
    }
}

#[cfg(test)]
mod tests;
