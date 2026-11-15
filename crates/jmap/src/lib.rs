#![forbid(unsafe_code)]
#![doc = "JMAP Account implementation for bifrost."]
// Dead-code diagnostics stay enabled outside the explicitly retained protocol
// modules. Those modules keep builders and methods the Account facade does not
// yet use; a blanket allowance here also hid drift in the live client and core.
// Without `sync` the crate has no public items at all, so every crate-private
// item is unreachable by construction; the lint is only meaningful with `sync`.
#![cfg_attr(not(feature = "sync"), allow(dead_code))]
// JMAP RFC fields (`fromDate`, `isEnabled`, `toDate`) become mutating builders.
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
// Principal admin ACL fields also use the mailbox vocabulary without mail enabled.
#[path = "mailbox/rights.rs"]
mod mailbox_rights;
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

// Keep wire identity and capability ownership together. Feature-disabled
// types still resolve their capability for Blob/lookup's `using` list.
macro_rules! data_types {
    ($( $(#[$attr:meta])* $kind:ident => $wire:literal, $cap:ident; )*) => {
        #[derive(Debug, Eq, PartialEq, Hash, Clone)]
        #[non_exhaustive]
        pub(crate) enum DataType {
            $($(#[$attr])* $kind,)*
            /// Unknown or feature-gated data type, preserved for wire round-trips.
            Other(String),
        }

        impl DataType {
            fn parse(value: &str) -> Self {
                match value {
                    $($(#[$attr])* $wire => Self::$kind,)*
                    other => Self::Other(other.to_string()),
                }
            }

            /// Known types resolve even when feature-disabled. Unknown extension
            /// types require an explicit additional request capability.
            pub(crate) fn capability_uri(&self) -> Option<&'static str> {
                match self.to_string().as_str() {
                    $($wire => data_types!(@uri $cap),)*
                    _ => None,
                }
            }
        }

        impl Display for DataType {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $($(#[$attr])* Self::$kind => f.write_str($wire),)*
                    Self::Other(value) => f.write_str(value),
                }
            }
        }

        // Object metadata selects the same capability owner as wire lookup.
        mod data_type_metadata {
            $(data_types!(@metadata $(#[$attr])* $kind, $wire, $cap);)*
        }
    };
    (@uri None) => { None };
    (@uri $cap:ident) => {
        Some(<$crate::core::capability::$cap as $crate::core::capability::Capability>::URI)
    };
    (@metadata $(#[$attr:meta])* $kind:ident, $wire:literal, None) => {};
    (@metadata $(#[$attr:meta])* $kind:ident, $wire:literal, $cap:ident) => {
        $(#[$attr])*
        // Some push/lookup types have no Object implementation or methods.
        #[allow(dead_code)]
        pub(crate) type $kind = crate::core::capability::$cap;
        $(#[$attr])*
        #[allow(dead_code, non_upper_case_globals)]
        pub(crate) const $kind: crate::core::method::ObjectMethods =
            crate::core::method::object_methods!($wire);
    };
}

data_types! {
    Core => "Core", Core;
    PushSubscription => "PushSubscription", Core;
    Principal => "Principal", Principals;
    ShareNotification => "ShareNotification", Principals;
    #[cfg(feature = "mail")]
    Email => "Email", Mail;
    #[cfg(feature = "mail")]
    EmailDelivery => "EmailDelivery", Mail;
    #[cfg(feature = "mail")]
    EmailSubmission => "EmailSubmission", Submission;
    #[cfg(feature = "mail")]
    Mailbox => "Mailbox", Mail;
    #[cfg(feature = "mail")]
    Thread => "Thread", Mail;
    #[cfg(feature = "mail")]
    Identity => "Identity", Submission;
    #[cfg(feature = "mail")]
    SearchSnippet => "SearchSnippet", Mail;
    #[cfg(feature = "mail")]
    VacationResponse => "VacationResponse", VacationResponseCap;
    #[cfg(feature = "mail")]
    Mdn => "MDN", None;
    #[cfg(feature = "mail")]
    SieveScript => "SieveScript", Sieve;
    #[cfg(feature = "calendars")]
    Calendar => "Calendar", Calendars;
    #[cfg(feature = "calendars")]
    CalendarEvent => "CalendarEvent", Calendars;
    #[cfg(feature = "calendars")]
    CalendarEventNotification => "CalendarEventNotification", Calendars;
    #[cfg(feature = "calendars")]
    ParticipantIdentity => "ParticipantIdentity", Calendars;
    #[cfg(feature = "calendars")]
    CalendarAlert => "CalendarAlert", Calendars;
    #[cfg(feature = "contacts")]
    AddressBook => "AddressBook", Contacts;
    #[cfg(feature = "contacts")]
    ContactCard => "ContactCard", Contacts;
    #[cfg(feature = "quota")]
    Quota => "Quota", Quota;
    FileNode => "FileNode", None;
}

impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        Self::parse(value)
    }
}

impl From<String> for DataType {
    fn from(value: String) -> Self {
        Self::parse(&value)
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
        // Object states are heterogeneous here: DataType is their runtime
        // discriminator. They require explicit decoding before typed requests.
        changed: HashMap<crate::core::id::AccountId, HashMap<DataType, String>>,
        #[serde(rename = "pushState", default)]
        push_state: Option<crate::core::id::PushState>,
    },
    #[cfg(feature = "mail")]
    EmailPush {
        #[serde(rename = "accountId")]
        account_id: crate::core::id::AccountId,
        email: serde_json::Value,
    },
    #[cfg(feature = "calendars")]
    CalendarAlert(CalendarAlert),
    Group {
        entries: Vec<PushObject>,
    },
}

/// The untagged state payload shared by WebSocket decoding and push blocks.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PushStateChange {
    pub(crate) changed: HashMap<crate::core::id::AccountId, HashMap<DataType, String>>,
    #[serde(rename = "pushState", default)]
    pub(crate) push_state: Option<crate::core::id::PushState>,
}

/// One wire block: token-free notifications precede its merged state.
#[derive(Debug)]
pub(crate) struct PushBlock {
    pub(crate) notifications: Vec<PushObject>,
    pub(crate) state: Option<PushStateChange>,
}

impl PushObject {
    pub(crate) fn decompose(self) -> PushBlock {
        let mut notifications = Vec::new();
        let mut state: Option<PushStateChange> = None;
        let mut pending = vec![self];
        while let Some(object) = pending.pop() {
            match object {
                PushObject::StateChange {
                    changed,
                    push_state,
                } => {
                    let merged = state.get_or_insert_with(|| PushStateChange {
                        changed: HashMap::new(),
                        push_state: None,
                    });
                    for (account, types) in changed {
                        merged.changed.entry(account).or_default().extend(types);
                    }
                    if push_state.is_some() {
                        merged.push_state = push_state;
                    }
                }
                PushObject::Group { entries } => pending.extend(entries.into_iter().rev()),
                // Unreachable when neither `mail` nor `calendars` is enabled:
                // the enum then has only the two variants matched above.
                #[allow(unreachable_patterns)]
                notification => notifications.push(notification),
            }
        }
        // An enterprise user must not lose an alert when processing stops at
        // a checkpoint. Deliver token-free objects before committing state;
        // replaying an alert after interruption is preferable to losing it.
        PushBlock {
            notifications,
            state,
        }
    }
}

#[cfg(feature = "calendars")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct CalendarAlert {
    #[serde(rename = "accountId")]
    pub(crate) account_id: crate::core::id::AccountId,
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
    /// An upload would exceed the session's advertised `maxSizeUpload`.
    /// Raised before sending the file body to the upload endpoint.
    UploadSizeLimit { max: usize, size: usize },
    /// Inbound JSON response decoding failure. Produced when the JMAP
    /// server's reply or session document cannot be parsed as the
    /// expected shape. Maps to `Protocol(ParseFailed)` /
    /// `ProviderContractViolation`.
    EnvelopeDecode(serde_json::Error),
    /// A successful method result failed typed extraction.
    ResultDecode {
        call_id: String,
        source: serde_json::Error,
    },
    /// The provider omitted a required response property.
    MissingResponseField(&'static str),
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
    /// Requested object ID not found in a parse or blob-copy response, or in
    /// a fetched object list.
    IdNotFound(String),
    /// No answer is available for a submitted id. Request facts survive through
    /// extraction so account callers never guess the method or conditionality.
    IdUnanswered {
        id: String,
        context: core::answer::AnswerContext,
    },
    /// Not parsable as the expected format.
    ///
    /// Missing provider properties use MissingResponseField instead.
    NotParsable(String),
    /// A local operation this adapter cannot represent.
    UnsupportedOperation(&'static str),
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
        // A passed-through response is not a JMAP request failure document.
        // Preserve the shared protocol classification even if its body happens
        // to deserialize as Problem Details.
        if matches!(
            e.net.as_ref().map(bifrost_net::Error::terminal),
            Some(bifrost_net::Error::UnexpectedResponse { .. })
        ) {
            return Error::Transport(e);
        }
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
            Error::UploadSizeLimit { max, size } => {
                write!(f, "Upload of {size} bytes exceeds maxSizeUpload ({max})")
            }
            Error::EnvelopeDecode(e) => write!(f, "Envelope decode error: {e}"),
            Error::ResultDecode { call_id, source } => {
                write!(f, "Call {call_id} result decode error: {source}")
            }
            Error::MissingResponseField(field) => write!(f, "Response omitted {field}"),
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
            Error::IdUnanswered { id, context } => {
                write!(f, "Id {id} unanswered in {} response", context.method)
            }
            Error::UnsupportedOperation(reason) => f.write_str(reason),
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

#[cfg(test)]
mod tests;
