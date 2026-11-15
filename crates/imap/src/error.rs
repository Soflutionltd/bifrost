//! Internal IMAP failures grouped by the owner of their recovery evidence.
//!
//! Tagged rejections acknowledge the command by construction. Transport and
//! session termination carry exchange evidence; local refusals never retire
//! a connection. The account boundary projects these categories into AccountError.

use crate::types::{AuthMechanism, Capability, ResponseCode};
use bifrost_types::TransmissionState;
use std::sync::Arc;

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Error {
    OAuthToken(OAuthFailure),

    MoveFallbackIncomplete {
        copy: Box<crate::types::CopyResult>,
        stored_deleted: bool,

        source: Box<Error>,
    },
    /// A helper failed before the enclosing operation was submitted. Keep its
    /// wire evidence intact: a refused SELECT acknowledges SELECT, not MOVE.
    OperationNotSent(Box<Error>),

    ServerStatus(ServerStatus),
    /// BYE terminates the session, but does not acknowledge an active command.
    Bye {
        text: String,
        code: Option<ResponseCode>,
        attempt: Option<TransmissionState>,
    },

    Transport {
        kind: TransportFailure,
        attempt: Option<TransmissionState>,
    },

    LocalRefusal(LocalRefusal),

    Provider(ProviderFailure),
    /// Some evidence means the local failure occurred inside an exchange.
    /// Panics always retire the driver, including those observed before send.
    Internal {
        kind: InternalFailure,
        mid_exchange: Option<TransmissionState>,
    },

    ResultLimit(ResultLimit),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OAuthToken(error) => write!(f, "OAuth token lookup failed: {error}"),
            Self::MoveFallbackIncomplete { source, .. } => {
                write!(f, "MOVE fallback incomplete after COPY: {source}")
            }
            Self::OperationNotSent(source) => std::fmt::Display::fmt(source, f),
            Self::ServerStatus(status) => std::fmt::Display::fmt(status, f),
            Self::Bye { text, .. } => write!(f, "server closing connection: {text}"),
            Self::Transport { kind, .. } => std::fmt::Display::fmt(kind, f),
            Self::LocalRefusal(kind) => std::fmt::Display::fmt(kind, f),
            Self::Provider(kind) => std::fmt::Display::fmt(kind, f),
            Self::Internal {
                kind: InternalFailure::Invariant(message),
                mid_exchange: Some(_),
            } => write!(f, "internal error mid-exchange: {message}"),
            Self::Internal { kind, .. } => std::fmt::Display::fmt(kind, f),
            Self::ResultLimit(kind) => std::fmt::Display::fmt(kind, f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OAuthToken(error) => Some(error.as_ref()),
            Self::MoveFallbackIncomplete { source, .. } | Self::OperationNotSent(source) => {
                Some(source.as_ref())
            }
            Self::Transport {
                kind: TransportFailure::Io(IoFailure(source)),
                ..
            } => Some(source.as_ref()),
            Self::ServerStatus(_)
            | Self::Bye { .. }
            | Self::Transport { .. }
            | Self::LocalRefusal(_)
            | Self::Provider(_)
            | Self::Internal { .. }
            | Self::ResultLimit(_) => None,
        }
    }
}

/// Preclassified token failures compare by retained identity, as AccountError
/// intentionally has no structural equality over diagnostic context.
#[derive(Debug, Clone)]
pub(crate) struct OAuthFailure(pub(crate) Arc<bifrost_types::AccountError>);
impl PartialEq for OAuthFailure {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for OAuthFailure {}
impl AsRef<bifrost_types::AccountError> for OAuthFailure {
    fn as_ref(&self) -> &bifrost_types::AccountError {
        self.0.as_ref()
    }
}
impl std::ops::Deref for OAuthFailure {
    type Target = bifrost_types::AccountError;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
impl std::fmt::Display for OAuthFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for OAuthFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// A server refusal, including a SCRAM server-final refusal. A failed SASL
/// continuation is drained to its tagged completion before this reaches callers.
/// An enterprise user expects an explicit refusal to retain acknowledgement,
/// even when an outer command loop tries to stamp a different exchange phase.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ServerStatus {
    #[error("server rejected command: {text}")]
    No {
        text: String,
        code: Option<ResponseCode>,
    },
    #[error("server reported bad command: {text}")]
    Bad {
        text: String,
        code: Option<ResponseCode>,
    },
    #[error("authentication failed: {text}")]
    Auth {
        text: String,
        code: Option<ResponseCode>,
        mechanism: Option<AuthMechanism>,
        scram: Option<bifrost_sasl::ScramServerError>,
    },
}

impl ServerStatus {
    fn response_code(&self) -> Option<&ResponseCode> {
        match self {
            Self::No { code, .. } | Self::Bad { code, .. } | Self::Auth { code, .. } => {
                code.as_ref()
            }
        }
    }
}

/// I/O equality is intentionally by ErrorKind, rather than diagnostic text.
#[derive(Debug, Clone)]
pub(crate) struct IoFailure(pub(crate) Arc<std::io::Error>);
impl PartialEq for IoFailure {
    fn eq(&self, other: &Self) -> bool {
        self.0.kind() == other.0.kind()
    }
}
impl Eq for IoFailure {}
impl std::fmt::Display for IoFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for IoFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TransportFailure {
    #[error("I/O error: {0}")]
    Io(#[source] IoFailure),
    #[error("operation timed out")]
    Timeout,
    #[error("connection closed")]
    Closed,
    #[error("driver task gone")]
    DriverGone,
}

/// Refused locally before transmission. These failures never blame the peer
/// or retire its usable session. Capability failures remain distinct from
/// unsupported client operations because QRESYNC recovery reads the former.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LocalRefusal {
    #[error("invalid input: {0}")]
    InvalidInput(LocalFailure),
    #[error("command not valid in the current connection state: {0}")]
    InvalidState(LocalFailure),
    #[error("session state changed before the command was sent: {0}")]
    StateChangedBeforeSend(LocalFailure),
    #[error("operation not supported by this client: {0}")]
    UnsupportedOperation(String),
    #[error("STARTTLS unavailable in current session")]
    StartTlsUnavailable,
    #[error("authentication policy rejected authentication: {0}")]
    AuthPolicy(AuthPolicyFailure),
    #[error("missing required capability: {0}")]
    MissingCapability(CapabilityFailure),
    #[error("message size {size} exceeds server APPENDLIMIT of {limit}")]
    AppendLimit { size: u64, limit: u64 },
    #[error("invalid APPEND date-time: {0}")]
    InvalidAppendDate(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProviderFailure {
    /// Malformed or out-of-sequence server data. Never a local refusal.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// A required field missing AFTER completion: framing remains reusable.
    /// An omission noticed during an open exchange is Protocol instead.
    #[error("protocol error: {0}")]
    Missing(String),
    #[error("parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InternalFailure {
    #[error("internal error: {0}")]
    Invariant(String),
    #[error("local runtime failure: {0}")]
    Runtime(String),
    #[error("driver task panicked: {0}")]
    Panicked(String),
}

/// Limits on already observed results, distinct from pre-send refusals.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ResultLimit {
    #[error("estimated FETCH response size {estimated} exceeds caller limit of {limit}")]
    Fetch {
        estimated: usize,
        limit: usize,
        seq: Option<crate::types::Seq>,
        uid: Option<crate::types::Uid>,
    },
    /// Unread literal octets make this failure connection-fatal.
    #[error("FETCH literal bytes {estimated} exceed caller limit of {limit}")]
    FetchLiteral { estimated: usize, limit: usize },
    #[error(
        "UID SEARCH result cannot be represented completely after {returned} IDs; omitted {omitted:?}"
    )]
    SearchTruncated {
        returned: usize,
        omitted: Option<u64>,
    },
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::io(source)
    }
}
impl From<crate::types::ValidationError> for Error {
    fn from(error: crate::types::ValidationError) -> Self {
        Self::LocalRefusal(LocalRefusal::InvalidInput(LocalFailure {
            message: error.to_string(),
            validation: Some(error),
            state: None,
        }))
    }
}

impl Error {
    pub(crate) fn is_connection_fatal(&self) -> bool {
        match self {
            Self::MoveFallbackIncomplete { source, .. } | Self::OperationNotSent(source) => {
                source.is_connection_fatal()
            }
            Self::Transport { kind, .. } => !matches!(kind, TransportFailure::Timeout),
            Self::Bye { .. } => true,
            Self::Provider(kind) => !matches!(kind, ProviderFailure::Missing(_)),
            Self::Internal { kind, mid_exchange } => {
                mid_exchange.is_some() || matches!(kind, InternalFailure::Panicked(_))
            }
            Self::ResultLimit(kind) => matches!(kind, ResultLimit::FetchLiteral { .. }),
            Self::OAuthToken(_) | Self::ServerStatus(_) | Self::LocalRefusal(_) => false,
        }
    }

    pub(crate) fn internal_mid_exchange(
        message: impl Into<String>,
        state: TransmissionState,
    ) -> Self {
        Self::Internal {
            kind: InternalFailure::Invariant(message.into()),
            mid_exchange: Some(state),
        }
    }
    pub(crate) fn io(source: std::io::Error) -> Self {
        Self::Transport {
            kind: TransportFailure::Io(IoFailure(Arc::new(source))),
            attempt: None,
        }
    }
    pub(crate) const fn timeout() -> Self {
        Self::Transport {
            kind: TransportFailure::Timeout,
            attempt: None,
        }
    }
    pub(crate) const fn timeout_inflight() -> Self {
        Self::Transport {
            kind: TransportFailure::Timeout,
            attempt: Some(TransmissionState::InFlight),
        }
    }
    pub(crate) const fn closed() -> Self {
        Self::Transport {
            kind: TransportFailure::Closed,
            attempt: None,
        }
    }
    #[cfg(test)]
    pub(crate) const fn driver_gone() -> Self {
        Self::Transport {
            kind: TransportFailure::DriverGone,
            attempt: None,
        }
    }
    pub(crate) const fn driver_gone_at(state: TransmissionState) -> Self {
        Self::Transport {
            kind: TransportFailure::DriverGone,
            attempt: Some(state),
        }
    }

    /// Stamp only failures whose phase is supplied by the surrounding exchange.
    /// Server refusals already prove acknowledgement; local validation and
    /// provider failures retain their own category rather than invented evidence.
    #[must_use]
    pub(crate) fn with_attempt(self, state: TransmissionState) -> Self {
        match self {
            Self::MoveFallbackIncomplete {
                copy,
                stored_deleted,
                source,
            } => Self::MoveFallbackIncomplete {
                copy,
                stored_deleted,
                source: Box::new(source.with_attempt(state)),
            },
            Self::Transport { kind, .. } => Self::Transport {
                kind,
                attempt: Some(state),
            },
            Self::Bye { text, code, .. } => Self::Bye {
                text,
                code,
                attempt: Some(state),
            },
            Self::Internal { kind, mid_exchange }
                if mid_exchange.is_some() || matches!(kind, InternalFailure::Panicked(_)) =>
            {
                Self::Internal {
                    kind,
                    mid_exchange: Some(state),
                }
            }
            other => other,
        }
    }

    /// Evidence relative to an operation whose prerequisite failed. The
    /// helper's acknowledgement remains inspectable on the source error.
    pub(crate) fn for_unsent_operation(self) -> Self {
        match self {
            // Token failures already carry their own account classification.
            Self::OAuthToken(_) | Self::OperationNotSent(_) => self,
            other => Self::OperationNotSent(Box::new(other)),
        }
    }

    pub(crate) fn attempt(&self) -> Option<TransmissionState> {
        match self {
            Self::MoveFallbackIncomplete { source, .. } => source.attempt(),
            Self::ServerStatus(_) => Some(TransmissionState::Acknowledged),
            Self::OperationNotSent(_) => Some(TransmissionState::Unsent),
            Self::Transport { attempt, .. } | Self::Bye { attempt, .. } => *attempt,
            Self::Internal { mid_exchange, .. } => *mid_exchange,
            Self::LocalRefusal(LocalRefusal::StateChangedBeforeSend(_)) => {
                Some(TransmissionState::Unsent)
            }
            Self::OAuthToken(_)
            | Self::LocalRefusal(_)
            | Self::Provider(_)
            | Self::ResultLimit(_) => None,
        }
    }

    pub(crate) fn no_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::ServerStatus(ServerStatus::No { text, code })
    }
    pub(crate) fn bad_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::ServerStatus(ServerStatus::Bad { text, code })
    }
    pub(crate) fn auth_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::ServerStatus(ServerStatus::Auth {
            text,
            code,
            mechanism: None,
            scram: None,
        })
    }
    pub(crate) fn auth_with_mechanism(
        text: String,
        code: Option<ResponseCode>,
        mechanism: AuthMechanism,
    ) -> Self {
        Self::ServerStatus(ServerStatus::Auth {
            text,
            code,
            mechanism: Some(mechanism),
            scram: None,
        })
    }
    pub(crate) fn bye_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Bye {
            text,
            code,
            attempt: None,
        }
    }

    pub(crate) fn response_code(&self) -> Option<&ResponseCode> {
        match self {
            Self::MoveFallbackIncomplete { source, .. } | Self::OperationNotSent(source) => {
                source.response_code()
            }
            Self::ServerStatus(status) => status.response_code(),
            Self::Bye { code, .. } => code.as_ref(),
            _ => None,
        }
    }
}

/// Structured reason automatic authentication could not select a mechanism.
///
/// `offered` is a SNAPSHOT of the server's advertised mechanisms taken before
/// the ladder ran, not a live reading at the moment of failure. Under
/// capability skew (a post-STARTTLS CAPABILITY refetch racing the ladder) the
/// two can disagree, so `offered` may name a mechanism the server no longer
/// advertises. The `rejected` list is the authoritative per-rung record: a
/// rung that vanished mid-ladder appears there as
/// `UnavailableOnLiveSnapshot`, which is what lets an operator tell "the
/// server never offered it" from "the server stopped offering it". Re-reading
/// the profile at failure time would not fix this - it would only move the
/// skew window - so the discrepancy is recorded rather than papered over.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthPolicyFailure {
    pub(crate) offered: Vec<OfferedAuthentication>,
    pub(crate) rejected: Vec<AuthMechanismRejection>,
}

impl AuthPolicyFailure {
    pub(crate) fn new(offered: Vec<String>, rejected: Vec<AuthMechanismRejection>) -> Self {
        Self {
            offered: offered
                .into_iter()
                .map(OfferedAuthentication::from)
                .collect(),
            rejected,
        }
    }
}

impl std::fmt::Display for AuthPolicyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.offered.is_empty() {
            f.write_str("server offered no compatible authentication mechanisms")
        } else if self.rejected.is_empty() {
            write!(
                f,
                "no permitted authentication mechanism among offered mechanisms: {}",
                self.offered
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            write!(
                f,
                "no permitted authentication mechanism among offered mechanisms: {}; rejected: ",
                self.offered
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            for (idx, rejection) in self.rejected.iter().enumerate() {
                if idx > 0 {
                    f.write_str(", ")?;
                }
                // Reuse offer spelling so SASL LOGIN and the LOGIN command
                // remain distinguishable in support diagnostics.
                let mechanism = if rejection.mechanism == AuthMechanism::ImapLogin {
                    OfferedAuthentication::LoginCommand
                } else {
                    OfferedAuthentication::Sasl(rejection.mechanism)
                };
                write!(f, "{} ({})", mechanism, rejection.reason)?;
            }
            Ok(())
        }
    }
}

/// Offered authentication mechanism that was not attempted to completion.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthMechanismRejection {
    pub(crate) mechanism: AuthMechanism,
    pub(crate) reason: AuthMechanismRejectionReason,
}

impl AuthMechanismRejection {
    pub(crate) const fn new(
        mechanism: AuthMechanism,
        reason: AuthMechanismRejectionReason,
    ) -> Self {
        Self { mechanism, reason }
    }
}

/// Reason an offered authentication mechanism was not attempted to completion.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AuthMechanismRejectionReason {
    DisabledByPolicy,
    CleartextWithoutTls,
    ChannelBindingUnavailable,
    /// The ladder snapshot advertised the mechanism but the live capability
    /// snapshot no longer did, so the attempt came back
    /// `MissingCapability` and the ladder moved on. Not a local policy
    /// decision: the server's advertisement changed underneath the ladder.
    UnavailableOnLiveSnapshot,
}

impl std::fmt::Display for AuthMechanismRejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DisabledByPolicy => f.write_str("disabled by policy"),
            Self::CleartextWithoutTls => f.write_str("requires TLS by policy"),
            Self::ChannelBindingUnavailable => f.write_str("channel binding unavailable"),
            Self::UnavailableOnLiveSnapshot => {
                f.write_str("no longer advertised on the live capability snapshot")
            }
        }
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;

/// Structured prerequisite evidence, with the original diagnostic retained for
/// account translation and support logs. Consumers should inspect `reason`
/// rather than infer session negotiation or protocol removal from prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapabilityFailure {
    pub(crate) capability: Capability,
    pub(crate) reason: CapabilityFailureReason,
    pub(crate) message: String,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CapabilityFailureReason {
    NotAdvertised,
    NotEnabled,
    ProhibitedByServer,
    RemovedInRev2,
    LiteralTooLarge {
        size: usize,
        limit: usize,
    },
    /// Any one of these advertisements would satisfy the operation.
    Alternatives(Vec<Capability>),
}

impl CapabilityFailure {
    pub(crate) fn new(
        capability: Capability,
        reason: CapabilityFailureReason,
        message: impl Into<String>,
    ) -> Self {
        Self {
            capability,
            reason,
            message: message.into(),
        }
    }
}

// Converting a bare wire capability is useful for simple advertisement gates.
// Contextual refusals must use `new` at the gate, where the reason is known.
impl From<String> for CapabilityFailure {
    fn from(message: String) -> Self {
        Self::new(
            Capability::from(message.as_str()),
            CapabilityFailureReason::NotAdvertised,
            message,
        )
    }
}

impl From<&str> for CapabilityFailure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}

impl From<CapabilityFailure> for String {
    fn from(value: CapabilityFailure) -> Self {
        value.message
    }
}

impl std::ops::Deref for CapabilityFailure {
    type Target = str;
    fn deref(&self) -> &str {
        &self.message
    }
}

impl PartialEq<str> for CapabilityFailure {
    fn eq(&self, other: &str) -> bool {
        self.message == other
    }
}

impl PartialEq<&str> for CapabilityFailure {
    fn eq(&self, other: &&str) -> bool {
        self.message == *other
    }
}

impl std::fmt::Display for CapabilityFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Local failure evidence. Free-form local input checks retain their message;
/// validated types and session checks additionally retain their typed cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalFailure {
    pub(crate) message: String,
    pub(crate) validation: Option<crate::types::ValidationError>,
    pub(crate) state: Option<StateRequirement>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StateRequirement {
    LegalStates {
        actual: crate::connection::SessionState,
        allowed: Vec<crate::connection::SessionState>,
    },
    BeforeFirstSelection {
        command: crate::types::CommandKind,
    },
    Enabled(Capability),
    Unencrypted,
    Capability(CapabilityFailure),
}

impl LocalFailure {
    pub(crate) fn state(requirement: StateRequirement, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            validation: None,
            state: Some(requirement),
        }
    }
}

impl From<String> for LocalFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            validation: None,
            state: None,
        }
    }
}

impl From<&str> for LocalFailure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}

impl From<LocalFailure> for String {
    fn from(value: LocalFailure) -> Self {
        value.message
    }
}

impl std::ops::Deref for LocalFailure {
    type Target = str;
    fn deref(&self) -> &str {
        &self.message
    }
}

impl PartialEq<str> for LocalFailure {
    fn eq(&self, other: &str) -> bool {
        self.message == other
    }
}

impl PartialEq<&str> for LocalFailure {
    fn eq(&self, other: &&str) -> bool {
        self.message == *other
    }
}

impl std::fmt::Display for LocalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Unknown SASL offers remain available for diagnostics and future mechanisms.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OfferedAuthentication {
    Sasl(AuthMechanism),
    OtherSasl(String),
    LoginCommand,
}

impl From<String> for OfferedAuthentication {
    fn from(value: String) -> Self {
        if value.eq_ignore_ascii_case("LOGIN") {
            return Self::LoginCommand;
        }
        let name = value.strip_prefix("AUTH=").unwrap_or(&value);
        if let Some(mechanism) = AuthMechanism::from_name(name) {
            return Self::Sasl(mechanism);
        }
        Self::OtherSasl(name.to_owned())
    }
}

impl std::fmt::Display for OfferedAuthentication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sasl(mechanism) => write!(f, "AUTH={}", mechanism.name()),
            Self::OtherSasl(name) => write!(f, "AUTH={name}"),
            Self::LoginCommand => f.write_str("LOGIN"),
        }
    }
}

#[cfg(test)]
mod typed_failure_tests {
    use super::*;

    #[test]
    fn validation_conversion_preserves_the_rule() {
        let validation = crate::types::SequenceSet::new_known("*").unwrap_err();
        let error = Error::from(validation.clone());
        let Error::LocalRefusal(LocalRefusal::InvalidInput(detail)) = error else {
            panic!("expected local validation")
        };
        assert_eq!(detail.validation, Some(validation));
        assert!(detail.state.is_none());
    }

    #[test]
    fn capability_failure_preserves_negotiation_reason_and_message() {
        let detail = CapabilityFailure::new(
            Capability::QResync,
            CapabilityFailureReason::NotEnabled,
            "QRESYNC (not ENABLEd)",
        );
        assert_eq!(detail.capability, Capability::QResync);
        assert_eq!(detail.reason, CapabilityFailureReason::NotEnabled);
        assert_eq!(detail.to_string(), "QRESYNC (not ENABLEd)");
    }
}
