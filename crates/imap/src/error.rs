//! Internal IMAP error type.
//!
//! `Error` is the crate-private failure representation produced by the
//! driver, parser, encoder, and high-level command surface. It carries
//! enough wire-level evidence (response codes, attempt state) for the
//! account-boundary translation in `account/error.rs` to build a
//! faithful `bifrost_types::AccountError`.
//!
//! Server status responses (OK, NO, BAD, BYE) are defined in RFC 3501
//! Section 7.1 and RFC 9051 Section 7.1.

use std::sync::Arc;

use bifrost_types::TransmissionState;

use crate::types::{AuthMechanism, Capability, ResponseCode};

/// Crate-internal wire-level transmission evidence attached to
/// transport-shaped failures.
///
/// The driver populates this when it has direct knowledge of whether a
/// command's bytes ever crossed the side-effect boundary; the account
/// boundary then projects it into `bifrost_types::AttemptCause`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ImapAttempt {
    pub(crate) transmission_state: TransmissionState,
}

impl ImapAttempt {
    pub(crate) const fn new(transmission_state: TransmissionState) -> Self {
        Self { transmission_state }
    }
}

/// Error type for IMAP client operations.
//
// Several variants carry `attempt: Option<ImapAttempt>` so the account
// boundary can distinguish `Unsent` / `InFlight` / `Acknowledged`
// transmissions when building `bifrost_types::AccountError`. The
// driver fills this in at the point of failure; pre-driver call sites
// (encoding, validation, builder preflight) leave it `None`.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum Error {
    /// Token lookup failed locally; preserve net recovery and endpoint evidence.
    #[error("OAuth token lookup failed: {0}")]
    OAuthToken(#[source] Arc<bifrost_types::AccountError>),
    #[error("MOVE fallback incomplete after COPY: {source}")]
    MoveFallbackIncomplete {
        copy: Box<crate::types::CopyResult>,
        stored_deleted: bool,
        #[source]
        source: Box<Error>,
    },
    /// Underlying I/O error, including TLS transport errors (RFC 3501 Section 2.1).
    #[error("I/O error: {source}")]
    Io {
        #[source]
        source: Arc<std::io::Error>,
        attempt: Option<ImapAttempt>,
    },

    /// Authentication was rejected by the server (RFC 3501 Section 6.2.2).
    ///
    /// `mechanism` is the wire name of the mechanism that was being driven
    /// when the server refused (`AuthMechanism::name`), when the producer
    /// knows it. The AUTHENTICATE consumers do; a `SaslError` mapped at a
    /// context-free `From` boundary does not, hence the `Option`. The name
    /// is the one piece of "which rung failed" evidence the server-rejection
    /// path can carry, and the account boundary folds it into the
    /// support-only diagnostic text - `AuthPolicyFailure` covers the same
    /// question on the LOCAL-policy path only.
    ///
    /// Transmission evidence is stamped by the driver after the SASL exchange.
    #[error("authentication failed: {text}")]
    Auth {
        text: String,
        code: Option<ResponseCode>,
        mechanism: Option<AuthMechanism>,
        /// SCRAM refusal recovery comes from bifrost-sasl, not an IMAP code.
        scram: Option<bifrost_sasl::ScramServerError>,
        attempt: Option<ImapAttempt>,
    },

    /// Server returned a NO response to a command (RFC 3501 Section 7.1.2).
    ///
    /// A tagged `NO` is by definition acknowledged by the server; the
    /// default `attempt` at construction is `Some(Acknowledged)`. The
    /// optional shape is kept so a future caller that synthesizes a
    /// `No` from a non-wire path (test fixtures, internal probes) can
    /// opt out, but every production constructor stamps `Acknowledged`.
    #[error("server rejected command: {text}")]
    No {
        text: String,
        code: Option<ResponseCode>,
        attempt: Option<ImapAttempt>,
    },

    /// Server returned a BAD response (RFC 3501 Section 7.1.3).
    ///
    /// As with `No`, a tagged `BAD` is server-acknowledged; constructors
    /// default to `Some(Acknowledged)`.
    #[error("server reported bad command: {text}")]
    Bad {
        text: String,
        code: Option<ResponseCode>,
        attempt: Option<ImapAttempt>,
    },

    /// Server sent BYE (RFC 3501 Section 7.1.5).
    #[error("server closing connection: {text}")]
    Bye {
        text: String,
        code: Option<ResponseCode>,
        attempt: Option<ImapAttempt>,
    },

    /// IMAP protocol violation by the server: it sent something malformed or
    /// out of sequence, or omitted something mandatory.
    ///
    /// Never a LOCAL refusal. A request refused before any byte is sent is
    /// never a provider fault (`reference/error-model.md`, "Local refusals"):
    /// caller input that cannot be expressed is `InvalidInput`, a capability
    /// the server lacks is `MissingCapability`, an operation this crate does
    /// not implement is `UnsupportedOperation`, a command the connection's
    /// local state does not permit is `InvalidState`, and state that moved
    /// while the command was queued is `StateChangedBeforeSend`. This variant
    /// maps to a terminal provider contract violation and is connection-fatal,
    /// so a local refusal filed here both blames the provider and retires a
    /// healthy connection. A LOCAL invariant failure that must retire the
    /// connection is `InternalMidExchange`, not this.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The narrower `Protocol` case where the evidence is an OMISSION: the
    /// server completed a command without a response or field the protocol
    /// makes mandatory (SELECT without UIDVALIDITY, a tagged OK without the
    /// untagged STATUS / QUOTA / ACL / SEARCH it owes, a STATUS reply
    /// without the item asked for).
    ///
    /// Same display text and same `ProviderContractViolation` recovery as
    /// `Protocol`; the account kind is finer, `Protocol(MissingField)`
    /// instead of `Protocol(ContractViolation)`, so diagnostics can tell "the
    /// server left something out" from "the server sent something wrong".
    ///
    /// Unlike `Protocol` it is NOT connection-fatal. It is only ever raised
    /// once the exchange has completed (from `finalize`, on the command's own
    /// tagged OK, or by the account layer afterwards), so the framing is
    /// intact and the connection is reusable. It must not be used for an
    /// omission noticed while the exchange is still open.
    #[error("protocol error: {0}")]
    ProtocolMissing(String),

    /// Failed to parse a server response.
    #[error("parse error: {0}")]
    Parse(String),

    /// Local request was rejected before transmission (invalid input,
    /// mailbox name, validator failure). Distinct from `Protocol`,
    /// which means the *server* violated the wire contract.
    #[error("invalid input: {0}")]
    InvalidInput(LocalFailure),

    /// The connection's local protocol state does not permit the command:
    /// the wrong session state for it (LOGIN once authenticated, CLOSE with
    /// nothing selected), or a mode the server requires that the caller has
    /// not established (UTF8=ONLY before `ENABLE UTF8=ACCEPT`). Refused on the
    /// handle before submission, so nothing was sent and the connection stays
    /// usable. Caller sequencing, hence a client bug rather than a retry: a
    /// state refresh does not make LOGIN-while-authenticated valid.
    ///
    /// A session already in Logout is `Closed` instead - the connection is
    /// gone, which is a transport condition, not a sequencing mistake.
    #[error("command not valid in the current connection state: {0}")]
    InvalidState(LocalFailure),

    /// The session state changed while the command waited in the driver's
    /// queue, so the check made against live state at the head of the queue
    /// refused it. Nothing was sent and the connection stays usable; the
    /// caller may re-issue after refreshing state, which is why this maps to a
    /// transient conflict rather than a client bug.
    #[error("session state changed before the command was sent: {0}")]
    StateChangedBeforeSend(LocalFailure),

    /// The operation is valid but this version of the crate does not
    /// implement it - a variant of a published `#[non_exhaustive]` request
    /// type added upstream that this crate has not learned. Distinct from
    /// `MissingCapability`, which is evidence about the SERVER and which retry
    /// ladders read as such.
    #[error("operation not supported by this client: {0}")]
    UnsupportedOperation(String),

    /// Operation exceeded the caller-supplied timeout.
    #[error("operation timed out")]
    Timeout { attempt: Option<ImapAttempt> },

    /// The TCP connection has been closed (RFC 3501 Section 2.1).
    #[error("connection closed")]
    Closed { attempt: Option<ImapAttempt> },

    /// STARTTLS was requested, but this session cannot be upgraded.
    ///
    /// This includes a cleartext PREAUTH greeting even when STARTTLS is
    /// advertised: STARTTLS is legal only before authentication, so the
    /// current session cannot satisfy a policy that requires TLS. Keep the
    /// wording reason-neutral; capability absence and PREAUTH share this
    /// outcome.
    #[error("STARTTLS unavailable in current session")]
    StartTlsUnavailable,

    /// Authentication policy rejected every mechanism the server offered.
    #[error("authentication policy rejected authentication: {0}")]
    AuthPolicy(AuthPolicyFailure),

    /// A server prerequisite is unavailable; the payload identifies the
    /// capability and distinguishes advertisement, negotiation, and policy.
    #[error("missing required capability: {0}")]
    MissingCapability(CapabilityFailure),

    /// Message exceeds the server's advertised APPENDLIMIT (RFC 7889 Section 3).
    #[error("message size {size} exceeds server APPENDLIMIT of {limit}")]
    AppendLimit { size: u64, limit: u64 },

    /// A FETCH exceeded the caller's configured retained-byte budget after
    /// its response was parsed.
    #[error("estimated FETCH response size {estimated} exceeds caller limit of {limit}")]
    FetchLimit {
        estimated: usize,
        limit: usize,
        seq: Option<u32>,
        uid: Option<u32>,
    },

    /// A FETCH literal declaration exceeds the caller's byte budget before
    /// its payload can be read. The connection is retired because unread
    /// literal octets prevent the next response from being framed safely.
    #[error("FETCH literal bytes {estimated} exceed caller limit of {limit}")]
    FetchLiteralLimit { estimated: usize, limit: usize },

    /// ESEARCH UID ranges could not be expanded without dropping results.
    ///
    /// `omitted` is `None` when the server used the `*` sentinel, whose
    /// concrete upper bound is unknown from the response alone.
    #[error(
        "UID SEARCH result cannot be represented completely after {returned} IDs; omitted {omitted:?}"
    )]
    SearchResultTruncated {
        returned: usize,
        omitted: Option<u64>,
    },

    /// Invalid APPEND date-time (RFC 3501 Section 9 `date-time`).
    #[error("invalid APPEND date-time: {0}")]
    InvalidAppendDate(String),

    /// Internal driver invariant violation, raised where the command framing
    /// is intact (before the first byte, or after the tagged completion), so
    /// the connection stays reusable. Maps to `Internal(InvariantViolated)`.
    #[error("internal error: {0}")]
    Internal(String),

    /// A local invariant failure raised after bytes of the same exchange are
    /// on the wire: mid-SASL, mid-literal, or after a tagged OK to
    /// STARTTLS/COMPRESS once the stream has been swapped out. The client's
    /// fault, not the server's (`Internal(InvariantViolated)`), but the
    /// driver's view of the exchange can no longer be trusted, so it is
    /// connection-fatal. The attempt state is REQUIRED: by construction some
    /// of the exchange was sent, and a missing state would read as `Unsent`.
    #[error("internal error mid-exchange: {message}")]
    InternalMidExchange {
        message: String,
        attempt: ImapAttempt,
    },

    /// A local facility the client depends on failed at runtime (the system
    /// entropy source for a SCRAM nonce, say): not a logic error, and neither
    /// the server's fault nor the caller's. Maps to `Internal(RuntimeFailure)`.
    #[error("local runtime failure: {0}")]
    LocalRuntime(String),

    /// The driver task panicked.
    #[error("driver task panicked: {message}")]
    DriverPanicked {
        message: String,
        attempt: Option<ImapAttempt>,
    },

    /// The driver task exited and the command channel is closed.
    #[error("driver task gone")]
    DriverGone { attempt: Option<ImapAttempt> },
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io {
            source: Arc::new(e),
            attempt: None,
        }
    }
}

impl From<crate::types::ValidationError> for Error {
    fn from(e: crate::types::ValidationError) -> Self {
        Self::InvalidInput(LocalFailure {
            message: e.to_string(),
            validation: Some(e),
            state: None,
        })
    }
}

/// Equality across variants. `Io` compares by `std::io::ErrorKind` only
/// (the underlying error is not `PartialEq`).
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::OAuthToken(a), Self::OAuthToken(b)) => Arc::ptr_eq(a, b),
            (
                Self::MoveFallbackIncomplete {
                    copy: a,
                    stored_deleted: ad,
                    source: asrc,
                },
                Self::MoveFallbackIncomplete {
                    copy: b,
                    stored_deleted: bd,
                    source: bsrc,
                },
            ) => a == b && ad == bd && asrc == bsrc,
            (
                Self::Io {
                    source: a,
                    attempt: a_att,
                },
                Self::Io {
                    source: b,
                    attempt: b_att,
                },
            ) => a.kind() == b.kind() && a_att == b_att,
            (
                Self::Auth {
                    text: t1,
                    code: c1,
                    mechanism: m1,
                    scram: s1,
                    attempt: a1,
                },
                Self::Auth {
                    text: t2,
                    code: c2,
                    mechanism: m2,
                    scram: s2,
                    attempt: a2,
                },
            ) => t1 == t2 && c1 == c2 && m1 == m2 && s1 == s2 && a1 == a2,
            (
                Self::No {
                    text: t1,
                    code: c1,
                    attempt: a1,
                },
                Self::No {
                    text: t2,
                    code: c2,
                    attempt: a2,
                },
            )
            | (
                Self::Bad {
                    text: t1,
                    code: c1,
                    attempt: a1,
                },
                Self::Bad {
                    text: t2,
                    code: c2,
                    attempt: a2,
                },
            ) => t1 == t2 && c1 == c2 && a1 == a2,
            (
                Self::Bye {
                    text: t1,
                    code: c1,
                    attempt: a1,
                },
                Self::Bye {
                    text: t2,
                    code: c2,
                    attempt: a2,
                },
            ) => t1 == t2 && c1 == c2 && a1 == a2,
            (Self::Protocol(a), Self::Protocol(b))
            | (Self::ProtocolMissing(a), Self::ProtocolMissing(b))
            | (Self::Parse(a), Self::Parse(b))
            | (Self::UnsupportedOperation(a), Self::UnsupportedOperation(b))
            | (Self::InvalidAppendDate(a), Self::InvalidAppendDate(b))
            | (Self::Internal(a), Self::Internal(b))
            | (Self::LocalRuntime(a), Self::LocalRuntime(b)) => a == b,
            (
                Self::InternalMidExchange {
                    message: m1,
                    attempt: a1,
                },
                Self::InternalMidExchange {
                    message: m2,
                    attempt: a2,
                },
            ) => m1 == m2 && a1 == a2,
            (Self::MissingCapability(a), Self::MissingCapability(b)) => a == b,
            (Self::InvalidInput(a), Self::InvalidInput(b))
            | (Self::InvalidState(a), Self::InvalidState(b))
            | (Self::StateChangedBeforeSend(a), Self::StateChangedBeforeSend(b)) => a == b,
            (Self::AuthPolicy(a), Self::AuthPolicy(b)) => a == b,
            (Self::Timeout { attempt: a }, Self::Timeout { attempt: b })
            | (Self::Closed { attempt: a }, Self::Closed { attempt: b })
            | (Self::DriverGone { attempt: a }, Self::DriverGone { attempt: b }) => a == b,
            (Self::StartTlsUnavailable, Self::StartTlsUnavailable) => true,
            (
                Self::DriverPanicked {
                    message: m1,
                    attempt: a1,
                },
                Self::DriverPanicked {
                    message: m2,
                    attempt: a2,
                },
            ) => m1 == m2 && a1 == a2,
            (
                Self::AppendLimit {
                    size: s1,
                    limit: l1,
                },
                Self::AppendLimit {
                    size: s2,
                    limit: l2,
                },
            ) => s1 == s2 && l1 == l2,
            (
                Self::FetchLimit {
                    estimated: e1,
                    limit: l1,
                    seq: s1,
                    uid: u1,
                },
                Self::FetchLimit {
                    estimated: e2,
                    limit: l2,
                    seq: s2,
                    uid: u2,
                },
            ) => e1 == e2 && l1 == l2 && s1 == s2 && u1 == u2,
            (
                Self::FetchLiteralLimit {
                    estimated: e1,
                    limit: l1,
                },
                Self::FetchLiteralLimit {
                    estimated: e2,
                    limit: l2,
                },
            ) => e1 == e2 && l1 == l2,
            (
                Self::SearchResultTruncated {
                    returned: r1,
                    omitted: o1,
                },
                Self::SearchResultTruncated {
                    returned: r2,
                    omitted: o2,
                },
            ) => r1 == r2 && o1 == o2,
            _ => false,
        }
    }
}

impl Eq for Error {}

impl Error {
    /// Whether a failure means the client can no longer trust the session.
    /// The driver retires on ReadLoop's typed framing, on explicit
    /// unusable-session signals, and on this classification for any exchange
    /// that may have put bytes on the wire: a tagged completion synchronizes
    /// the framing but does not prove the client understood the answer.
    /// Continuation consumers also use it to decide whether cancellation is
    /// safe.
    ///
    /// Server rejections and local validation failures leave command
    /// framing intact. Transport loss, BYE, parse failure, and protocol
    /// desynchronization do not.
    ///
    /// `ProtocolMissing` is deliberately absent: every producer raises it
    /// from a consumer's `finalize`, after the command's own tagged OK was
    /// read, or from the account layer after the command returned. An
    /// omission is not a desynchronization - the server finished the
    /// exchange and the next byte on the wire begins a fresh response - so
    /// the connection stays reusable. A producer that could raise it with
    /// the exchange unfinished must use `Protocol` instead.
    pub(crate) fn is_connection_fatal(&self) -> bool {
        if let Self::MoveFallbackIncomplete { source, .. } = self {
            return source.is_connection_fatal();
        }
        matches!(
            self,
            Self::Io { .. }
                | Self::Bye { .. }
                | Self::Protocol(_)
                | Self::Parse(_)
                | Self::FetchLiteralLimit { .. }
                | Self::Closed { .. }
                | Self::DriverPanicked { .. }
                | Self::DriverGone { .. }
                | Self::InternalMidExchange { .. }
        )
    }

    /// A local invariant failure after bytes of the exchange are on the
    /// wire. See [`Error::InternalMidExchange`].
    pub(crate) fn internal_mid_exchange(
        message: impl Into<String>,
        transmission_state: TransmissionState,
    ) -> Self {
        Self::InternalMidExchange {
            message: message.into(),
            attempt: ImapAttempt::new(transmission_state),
        }
    }

    /// Construct a transport-flavored I/O error with no attempt-state evidence.
    pub(crate) fn io(source: std::io::Error) -> Self {
        Self::Io {
            source: Arc::new(source),
            attempt: None,
        }
    }

    /// Construct a `Timeout` with no attempt-state evidence.
    pub(crate) const fn timeout() -> Self {
        Self::Timeout { attempt: None }
    }

    /// Construct a `Timeout` with `InFlight` attempt evidence.
    ///
    /// The driver had claimed the command before the deadline expired. It may
    /// have written bytes; withdrawal can no longer prove it unsent.
    pub(crate) fn timeout_inflight() -> Self {
        Self::Timeout {
            attempt: Some(ImapAttempt::new(TransmissionState::InFlight)),
        }
    }

    /// Construct a `Closed` with no attempt-state evidence.
    pub(crate) const fn closed() -> Self {
        Self::Closed { attempt: None }
    }

    /// Construct a `DriverGone` with no attempt-state evidence.
    ///
    /// Test-only. A driver death always has a phase relative to the caller
    /// (`Unsent` when it never claimed a command, `InFlight` when it had claimed
    /// one), and a missing phase reads as `Unsent`, so production code mints
    /// through [`Error::driver_gone_at`] or `observe_driver_panic`.
    #[cfg(test)]
    pub(crate) const fn driver_gone() -> Self {
        Self::DriverGone { attempt: None }
    }

    /// Construct a `DriverGone` stamped with how far the caller's command
    /// got: `Unsent` when the driver never claimed it, `InFlight` when the
    /// driver had claimed it and died before answering.
    pub(crate) const fn driver_gone_at(transmission_state: TransmissionState) -> Self {
        Self::DriverGone {
            attempt: Some(ImapAttempt::new(transmission_state)),
        }
    }

    /// Attach (or override) the attempt evidence on a transport-shaped or
    /// server-acknowledged error.
    ///
    /// No-op for variants that genuinely cannot carry an attempt state
    /// (pure parse errors before any wire activity, local validation,
    /// capability gating). For `No` / `Bad` this patches the (typically
    /// already `Acknowledged`) attempt - callers may use it to override
    /// in tests, but the constructors default to `Acknowledged`.
    #[must_use]
    pub(crate) fn with_attempt(self, state: TransmissionState) -> Self {
        let attempt = Some(ImapAttempt::new(state));
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
            Self::Auth {
                text,
                code,
                mechanism,
                scram,
                ..
            } => Self::Auth {
                text,
                code,
                mechanism,
                scram,
                attempt,
            },
            Self::Io { source, .. } => Self::Io { source, attempt },
            Self::Timeout { .. } => Self::Timeout { attempt },
            Self::Closed { .. } => Self::Closed { attempt },
            Self::Bye { text, code, .. } => Self::Bye {
                text,
                code,
                attempt,
            },
            Self::No { text, code, .. } => Self::No {
                text,
                code,
                attempt,
            },
            Self::Bad { text, code, .. } => Self::Bad {
                text,
                code,
                attempt,
            },
            Self::DriverGone { .. } => Self::DriverGone { attempt },
            Self::DriverPanicked { message, .. } => Self::DriverPanicked { message, attempt },
            Self::InternalMidExchange { message, .. } => Self::InternalMidExchange {
                message,
                attempt: ImapAttempt::new(state),
            },
            other => other,
        }
    }

    /// Read out attempt evidence, if any.
    pub(crate) fn attempt(&self) -> Option<TransmissionState> {
        match self {
            Self::MoveFallbackIncomplete { source, .. } => source.attempt(),
            Self::Auth { attempt, .. }
            | Self::Io { attempt, .. }
            | Self::Timeout { attempt }
            | Self::Closed { attempt }
            | Self::Bye { attempt, .. }
            | Self::No { attempt, .. }
            | Self::Bad { attempt, .. }
            | Self::DriverPanicked { attempt, .. }
            | Self::DriverGone { attempt } => attempt.map(|a| a.transmission_state),
            Self::InternalMidExchange { attempt, .. } => Some(attempt.transmission_state),
            // Refused at the head of the queue, before the first byte: the
            // variant IS the evidence, so it needs no field to carry it.
            Self::StateChangedBeforeSend(_) => Some(TransmissionState::Unsent),
            _ => None,
        }
    }

    /// Construct an [`Error::No`] with an optional response code. Sets
    /// `attempt = Some(Acknowledged)`: a tagged `NO` is by definition
    /// server-acknowledged.
    pub(crate) fn no_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::No {
            text,
            code,
            attempt: Some(ImapAttempt::new(TransmissionState::Acknowledged)),
        }
    }

    /// Construct an [`Error::Bad`] with an optional response code. Sets
    /// `attempt = Some(Acknowledged)`: a tagged `BAD` is by definition
    /// server-acknowledged.
    pub(crate) fn bad_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Bad {
            text,
            code,
            attempt: Some(ImapAttempt::new(TransmissionState::Acknowledged)),
        }
    }

    /// Construct an [`Error::Auth`] with an optional response code and no
    /// mechanism attribution. Used where the producer genuinely does not
    /// know which rung was in flight.
    pub(crate) fn auth_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Auth {
            text,
            code,
            mechanism: None,
            scram: None,
            attempt: None,
        }
    }

    /// Construct an [`Error::Auth`] naming the mechanism that was refused.
    ///
    /// `mechanism` is a wire mechanism name (`AuthMechanism::name`, or the
    /// SCRAM token `bifrost_sasl` emits). It is not a secret: it is the same
    /// token that already travels in clear on the AUTHENTICATE command line
    /// and in the server's CAPABILITY advertisement.
    pub(crate) fn auth_with_mechanism(
        text: String,
        code: Option<ResponseCode>,
        mechanism: AuthMechanism,
    ) -> Self {
        Self::Auth {
            text,
            code,
            mechanism: Some(mechanism),
            scram: None,
            attempt: None,
        }
    }

    /// Construct an [`Error::Bye`] with an optional response code.
    pub(crate) fn bye_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Bye {
            text,
            code,
            attempt: None,
        }
    }

    /// Response code carried by a server status error, if any.
    pub(crate) fn response_code(&self) -> Option<&ResponseCode> {
        match self {
            Self::MoveFallbackIncomplete { source, .. } => source.response_code(),
            Self::Auth { code, .. }
            | Self::No { code, .. }
            | Self::Bad { code, .. }
            | Self::Bye { code, .. } => code.as_ref(),
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
                write!(f, "{} ({})", rejection.mechanism.name(), rejection.reason)?;
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
        for mechanism in [
            AuthMechanism::OAuthBearer,
            AuthMechanism::XOAuth2,
            AuthMechanism::ScramSha256Plus,
            AuthMechanism::ScramSha1Plus,
            AuthMechanism::ScramSha256,
            AuthMechanism::ScramSha1,
            AuthMechanism::Plain,
            AuthMechanism::CramMd5,
        ] {
            if name.eq_ignore_ascii_case(mechanism.name()) {
                return Self::Sasl(mechanism);
            }
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
        let Error::InvalidInput(detail) = error else {
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
