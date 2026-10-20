//! Error and result type for SMTP clients

use std::{error::Error as StdError, fmt, io, sync::Arc};

use bifrost_types::error::AccountError;

use crate::{
    BoxError,
    transport::smtp::response::{Code, EnhancedStatusCode, Response, Severity},
};

// Inspired by https://github.com/seanmonstar/reqwest/blob/a8566383168c0ef06c21f38cbc9213af6ff6db31/src/error.rs

/// The Errors that may occur when sending an email over SMTP
// pub: SMTP keeps full Response-carrying errors instead of Account recovery errors.
pub struct Error {
    inner: Box<Inner>,
}

impl Clone for Error {
    fn clone(&self) -> Self {
        // BoxError is not Clone. Preserve diagnostic text as a plain string source so
        // batch pipelines that clone an error mid-flight retain the diagnostic message.
        let source: Option<BoxError> = self
            .inner
            .source
            .as_ref()
            .map(|e| -> BoxError { Box::new(StringError(e.to_string())) });
        Self {
            inner: Box::new(Inner {
                kind: self.inner.kind.clone(),
                source,
                attempt: self.inner.attempt,
                phase: self.inner.phase,
                preclassified_account_error: self.inner.preclassified_account_error.clone(),
            }),
        }
    }
}

struct Inner {
    kind: ErrorKind,
    source: Option<BoxError>,
    attempt: Option<SmtpAttempt>,
    /// Coarse command phase at the moment this error was constructed.
    ///
    /// Lives on the value (not on `SmtpErrorContext`) so a missed `with_phase`
    /// decoration cannot silently degrade the classifier's signal. Mapping
    /// from low-level wire failures to `AccountErrorKind` reads only
    /// `phase()`; the context has no second phase that could disagree.
    phase: Option<SmtpCommandPhase>,
    /// A lower-level dependency already projected this failure into the
    /// shared account-error model. Keep the typed result through the SMTP
    /// wrapper so its protocol boundary does not flatten it into `InvalidInput`.
    preclassified_account_error: Option<Arc<AccountError>>,
}

/// Wire-level transmission state for the mail-send side effect.
///
/// `Unsent` means no recipient command has been written.
/// `InFlight` means a write or read failed mid-command and the server's view is
/// ambiguous. `Acknowledged` means the server returned a definitive negative
/// reply for the command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SmtpTransmissionState {
    Unsent,
    InFlight,
    Acknowledged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SmtpAttempt {
    pub(crate) transmission_state: SmtpTransmissionState,
}

/// Coarse command phase at the point an SMTP transport error was constructed.
///
/// Carried on `Error::Inner` directly: a missed `with_phase` call cannot
/// silently degrade the classifier because the phase is part of the value,
/// not a side-channel decoration on `SmtpErrorContext`.
///
/// The 16 variants cover every wire-side command boundary the bifrost SMTP
/// driver actually crosses. `DataCommand` distinguishes the `DATA` command-
/// write/read from the body upload (`DataBody`) and from the final reply
/// after the dot terminator (`DataFinal`), so the LMTP DATA-command negative
/// reply does not get misclassified as a body-side transport drop.
///
/// Some variants (`Connect`, `Greeting`, `Hello`, `StartTls`, `BdatBody`,
/// `Noop`, `Vrfy`, `Expn`, `Rset`) describe phases the driver crosses but
/// does not yet decorate with `with_phase` because the corresponding call
/// sites currently propagate the raw `Error` upward without funneling
/// through `into_account_error`. They are present so that future per-phase
/// classifier refinements (e.g. tagging connect-time transport drops as
/// `Connect`) do not require widening the enum and breaking the exhaustive
/// match in the classifier.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SmtpCommandPhase {
    Connect,
    Greeting,
    Hello,
    StartTls,
    /// The AUTH exchange. `mechanism` is the SASL rung the exchange was
    /// running when it failed, as it appears on the wire (`PLAIN`,
    /// `SCRAM-SHA-256-PLUS`, ...), or `None` for auth-phase faults raised
    /// before a rung was chosen (local policy refusing plaintext AUTH, no
    /// compatible mechanism, an OAuth token source that cannot resolve).
    ///
    /// It is a payload on the variant rather than a separate field on
    /// `Error` so that stamping the AUTH phase forces a decision about the
    /// mechanism at every call site, the same reason the phase itself lives
    /// on the value rather than on `SmtpErrorContext`.
    Auth {
        mechanism: Option<&'static str>,
    },
    MailFrom,
    RcptTo,
    DataCommand,
    DataBody,
    DataFinal,
    BdatBody,
    LmtpFinalStatus,
    Noop,
    Vrfy,
    Expn,
    Rset,
}

impl Error {
    pub(crate) fn new<E>(kind: ErrorKind, source: Option<E>) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Box::new(Inner {
                kind,
                source: source.map(Into::into),
                attempt: None,
                phase: None,
                preclassified_account_error: None,
            }),
        }
    }

    fn without_source(kind: ErrorKind) -> Error {
        Error {
            inner: Box::new(Inner {
                kind,
                source: None,
                attempt: None,
                phase: None,
                preclassified_account_error: None,
            }),
        }
    }

    /// Attach transmission state to a transport-layer error. Returns a builder-
    /// style updated `Error`. Crate-internal: send pipelines decorate errors at
    /// the point they cross a command boundary so the account-error mapper can
    /// emit the right `Attempt` cause without re-deriving wire context.
    pub(crate) fn with_attempt(mut self, state: SmtpTransmissionState) -> Self {
        self.inner.attempt = Some(SmtpAttempt {
            transmission_state: state,
        });
        self
    }

    pub(crate) fn attempt(&self) -> Option<SmtpTransmissionState> {
        self.inner.attempt.map(|a| a.transmission_state)
    }

    /// Attach a command phase to this error so the classifier can refine
    /// per-phase routing without depending on the caller setting it on a
    /// separate `SmtpErrorContext`.
    pub(crate) fn with_phase(mut self, phase: SmtpCommandPhase) -> Self {
        self.inner.phase = Some(phase);
        self
    }

    /// Attach a command phase only when none is stamped yet. Callers that
    /// wrap a multi-phase exchange use this so an inner, more specific tag
    /// (e.g. `DataBody` on a body-upload write) survives the outer wrapper's
    /// coarser tag.
    ///
    /// One exception to set-if-absent: an AUTH stamp that carries no
    /// mechanism accepts the mechanism from an outer AUTH stamp that has
    /// one. Inner constructors such as `From<SaslError>` know they are on
    /// the auth lane but not which rung is running; the wrapper in the
    /// connection driver knows the rung. Without this the SCRAM server-error
    /// path would keep the unnamed stamp and lose the name the driver holds.
    pub(crate) fn or_phase(self, phase: SmtpCommandPhase) -> Self {
        match (self.inner.phase, phase) {
            (None, _) => self.with_phase(phase),
            (
                Some(SmtpCommandPhase::Auth { mechanism: None }),
                SmtpCommandPhase::Auth { mechanism: Some(_) },
            ) => self.with_phase(phase),
            _ => self,
        }
    }

    /// Preserve an account error already classified by a dependency such as
    /// bifrost-net. The SMTP account boundary will add its call-site context.
    pub(crate) fn with_preclassified_account_error(mut self, error: AccountError) -> Self {
        self.inner.preclassified_account_error = Some(Arc::new(error));
        self
    }

    pub(crate) fn preclassified_account_error(&self) -> Option<AccountError> {
        self.inner.preclassified_account_error.as_deref().cloned()
    }

    pub(crate) fn phase(&self) -> Option<SmtpCommandPhase> {
        self.inner.phase
    }

    /// Support-safe diagnostic string for the account-error mapper. AUTH paths
    /// already redact credentials before they reach here; this just exposes the
    /// boxed source or the kind discriminant for support text.
    pub(crate) fn diagnostic_text(&self) -> Option<String> {
        self.inner
            .source
            .as_ref()
            .map(std::string::ToString::to_string)
    }

    /// Returns the classification for this SMTP error.
    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }

    /// Returns the SMTP reply that caused this error.
    pub fn smtp_response(&self) -> Option<&Response> {
        match &self.inner.kind {
            ErrorKind::Transient(response) | ErrorKind::Permanent(response) => Some(response),
            _ => None,
        }
    }

    /// Returns the SMTP status code for reply errors.
    pub fn status(&self) -> Option<Code> {
        self.smtp_response().map(Response::code)
    }

    /// Returns the first enhanced status code from the SMTP reply text.
    pub fn enhanced_status_code(&self) -> Option<EnhancedStatusCode> {
        self.smtp_response()
            .and_then(Response::enhanced_status_code)
    }

    /// Returns true if the error is a transient or permanent SMTP reply.
    pub fn is_smtp_reply(&self) -> bool {
        self.smtp_response().is_some()
    }

    /// Returns true if the client failed while parsing an SMTP response.
    ///
    /// This does not include SMTP 4xx or 5xx replies. For those, use
    /// [`Error::is_transient`], [`Error::is_permanent`], or
    /// [`Error::smtp_response`].
    pub fn is_parse(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Parse)
    }

    /// Returns true if the error is from invalid caller input or local SMTP state.
    pub fn is_invalid_input(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::InvalidInput)
    }

    /// Returns true if the error is from a violated client invariant.
    pub fn is_internal(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Internal)
    }

    /// Returns true if the error is a client-side policy refusal.
    ///
    /// This includes refusing to transmit credentials over an unencrypted
    /// SMTP connection.
    pub fn is_policy(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Policy)
    }

    /// Returns true if the error is a transient SMTP 4xx reply.
    ///
    /// This is not the inverse of [`Error::is_permanent`]: parse,
    /// invalid-input, connection, network, TLS, timeout, and shutdown errors
    /// are neither transient nor permanent SMTP replies.
    pub fn is_transient(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Transient(_))
    }

    /// Returns true if the error is a permanent SMTP 5xx reply.
    ///
    /// This is not the inverse of [`Error::is_transient`]: parse,
    /// invalid-input, connection, network, TLS, timeout, and shutdown errors
    /// are neither transient nor permanent SMTP replies.
    pub fn is_permanent(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Permanent(_))
    }

    /// Returns true if the error happened while opening or maintaining the SMTP connection.
    pub fn is_connection(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Connection)
    }

    /// Returns true if the error came from the underlying network I/O layer.
    pub fn is_network(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Network)
    }

    /// Returns true if the error is caused by a timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Timeout)
    }

    /// Returns true if the error is from TLS.
    pub fn is_tls(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Tls)
    }

    /// Returns true if the error is because the transport was shut down.
    pub fn is_transport_shutdown(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::TransportShutdown)
    }
}

/// Public classification for [`Error`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
// pub: callers classify SMTP reply, transport, TLS, and policy failures.
pub enum ErrorKind {
    /// Transient SMTP reply, 4xx status.
    Transient(Response),
    /// Permanent SMTP reply, 5xx status.
    Permanent(Response),
    /// Error parsing an SMTP response.
    Parse,
    /// Invalid caller input, unsupported local configuration, or local protocol state.
    InvalidInput,
    /// A requested SMTP extension feature is not advertised by the
    /// server (e.g. FUTURERELEASE on a relay that did not announce it).
    /// Distinct from `InvalidInput` so the account-error mapping can
    /// surface `Unsupported(Send)` rather than a generic malformed
    /// request.
    FeatureUnsupported,
    /// A requested parameter exceeds a server-advertised limit (e.g.
    /// HOLDFOR over the FUTURERELEASE max interval). Maps to a malformed
    /// request - the value is out of the allowed window.
    ParameterOverLimit,
    /// Internal client invariant failure.
    Internal,
    /// Client-side policy refusal.
    Policy,
    /// Connection error.
    Connection,
    /// Underlying network I/O error.
    Network,
    /// Timeout while connecting, reading, writing, or upgrading TLS.
    Timeout,
    /// TLS error.
    Tls,
    /// Transport shutdown error.
    TransportShutdown,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("bifrost_smtp::transport::smtp::Error");

        builder.field("kind", &self.inner.kind);

        if let Some(source) = &self.inner.source {
            builder.field("source", source);
        }

        builder.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner.kind {
            ErrorKind::Parse => f.write_str("SMTP response parse error")?,
            ErrorKind::InvalidInput => f.write_str("invalid SMTP input")?,
            ErrorKind::FeatureUnsupported => {
                f.write_str("requested SMTP feature not supported by server")?;
            }
            ErrorKind::ParameterOverLimit => {
                f.write_str("SMTP parameter exceeds server-advertised limit")?;
            }
            ErrorKind::Internal => f.write_str("internal SMTP error")?,
            ErrorKind::Policy => f.write_str("SMTP policy error")?,
            ErrorKind::Network => f.write_str("SMTP network error")?,
            ErrorKind::Connection => f.write_str("SMTP connection error")?,
            ErrorKind::Timeout => f.write_str("SMTP operation timed out")?,
            ErrorKind::Tls => f.write_str("SMTP TLS error")?,
            ErrorKind::TransportShutdown => f.write_str("SMTP transport has been shut down")?,
            ErrorKind::Transient(response) => {
                write!(f, "transient SMTP reply ({})", response.code())?;
                write_response_message(f, response)?;
            }
            ErrorKind::Permanent(response) => {
                write!(f, "permanent SMTP reply ({})", response.code())?;
                write_response_message(f, response)?;
            }
        }

        if let Some(e) = &self.inner.source {
            write!(f, ": {e}")?;
        }

        Ok(())
    }
}

fn write_response_message(f: &mut fmt::Formatter<'_>, response: &Response) -> fmt::Result {
    let mut message = response.message();
    if let Some(first) = message.next() {
        write!(f, ": {first}")?;
        for line in message {
            write!(f, " / {line}")?;
        }
    }
    Ok(())
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner.source.as_ref().map(|e| {
            let r: &(dyn std::error::Error + 'static) = &**e;
            r
        })
    }
}

pub(crate) fn status(response: Response) -> Error {
    match response.code().severity {
        Severity::TransientNegativeCompletion => {
            Error::without_source(ErrorKind::Transient(response))
        }
        Severity::PermanentNegativeCompletion => {
            Error::without_source(ErrorKind::Permanent(response))
        }
        _ => internal(format!(
            "unexpected positive SMTP reply {}",
            response.code()
        )),
    }
}

/// Helper for `Clone` on `Error`: wraps a diagnostic string as a `BoxError`
/// source so the cloned error retains its human-readable message while discarding
/// the original non-Clone source.
#[derive(Debug)]
pub(crate) struct StringError(pub(crate) String);

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for StringError {}

pub(crate) fn parse<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Parse, Some(e))
}

pub(crate) fn invalid_input<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::InvalidInput, Some(e))
}

pub(crate) fn feature_unsupported<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::FeatureUnsupported, Some(e))
}

pub(crate) fn parameter_over_limit<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::ParameterOverLimit, Some(e))
}

pub(crate) fn internal<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Internal, Some(e))
}

pub(crate) fn policy<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Policy, Some(e))
}

fn io_error(default_kind: ErrorKind, error: io::Error) -> Error {
    let kind = match error.kind() {
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => ErrorKind::Timeout,
        _ => default_kind,
    };
    Error::new(kind, Some(error))
}

pub(crate) fn network(e: io::Error) -> Error {
    io_error(ErrorKind::Network, e)
}

pub(crate) fn connection_io(e: io::Error) -> Error {
    io_error(ErrorKind::Connection, e)
}

// Use `connection_io` for connection-layer I/O errors so timeout
// classification is preserved at construction.
pub(crate) fn connection<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Connection, Some(e))
}

// NOT gated on the `tokio` feature: the BLOCKING reader calls this too, for the
// spent-deadline case where a zero `SO_RCVTIMEO` would mean "block forever"
// rather than "already expired". It carried a `#[cfg(feature = "tokio")]` from
// the per-reply-deadline work until 2026-09-15, which made the blocking half of
// a deliberately async-independent transport fail to compile with the feature
// off. Nothing caught it because nothing built this crate without `tokio` until
// the per-feature check sweeps existed.
pub(crate) fn timeout(message: &'static str) -> Error {
    Error::new(
        ErrorKind::Timeout,
        Some(std::io::Error::new(std::io::ErrorKind::TimedOut, message)),
    )
}

pub(crate) fn tls<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Tls, Some(e))
}

/// A blocking TLS handshake failure. A handshake that ran into the socket's
/// `SO_RCVTIMEO` / `SO_SNDTIMEO` is a `Timeout`, matching the async half's
/// "TLS handshake timed out"; anything else is `Tls`. On a blocking socket a
/// `WouldBlock` can only mean the armed timeout expired.
pub(crate) fn tls_handshake<S>(e: native_tls::HandshakeError<S>) -> Error {
    match e {
        native_tls::HandshakeError::WouldBlock(_) => timeout("TLS handshake timed out"),
        native_tls::HandshakeError::Failure(e) => {
            if chain_has_io_timeout(&e) {
                timeout("TLS handshake timed out")
            } else {
                tls(e)
            }
        }
    }
}

/// True when any error in the source chain is an `io::Error` of a timeout kind.
fn chain_has_io_timeout(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(e);
    while let Some(err) = current {
        if let Some(io) = err.downcast_ref::<io::Error>()
            && matches!(
                io.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            )
        {
            return true;
        }
        current = err.source();
    }
    false
}

pub(crate) fn transport_shutdown() -> Error {
    Error::without_source(ErrorKind::TransportShutdown)
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::{
        ErrorKind, SmtpCommandPhase, connection, connection_io, internal, invalid_input, network,
        policy, status,
    };
    use crate::transport::smtp::response::{Category, Code, Detail, Response, Severity};

    #[test]
    fn an_unnamed_auth_stamp_takes_the_rung_from_the_driver_wrapper() {
        // `From<SaslError>` knows it is on the auth lane but not which rung
        // is running; the connection driver knows. Plain set-if-absent would
        // keep the unnamed stamp and throw the name away, which is how the
        // SCRAM server-error path would silently lose it.
        let named = SmtpCommandPhase::Auth {
            mechanism: Some("SCRAM-SHA-256"),
        };
        let inner =
            invalid_input("invalid-proof").with_phase(SmtpCommandPhase::Auth { mechanism: None });
        assert_eq!(inner.or_phase(named).phase(), Some(named));

        // An already-named stamp is not overwritten, and a non-auth stamp
        // keeps set-if-absent: the auth upgrade is the only exception.
        let inner = invalid_input("invalid-proof").with_phase(SmtpCommandPhase::Auth {
            mechanism: Some("PLAIN"),
        });
        assert_eq!(
            inner.or_phase(named).phase(),
            Some(SmtpCommandPhase::Auth {
                mechanism: Some("PLAIN")
            })
        );
        let inner = invalid_input("body").with_phase(SmtpCommandPhase::DataBody);
        assert_eq!(
            inner.or_phase(SmtpCommandPhase::DataCommand).phase(),
            Some(SmtpCommandPhase::DataBody)
        );
    }

    #[test]
    fn exposes_connection_kind() {
        let error = connection("server closed the connection");

        assert_eq!(error.kind(), &ErrorKind::Connection);
        assert!(error.is_connection());
        assert!(!error.is_network());
        assert!(!error.is_transient());
        assert!(!error.is_permanent());
    }

    #[test]
    fn exposes_network_kind() {
        let error = network(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "socket write failed",
        ));

        assert_eq!(error.kind(), &ErrorKind::Network);
        assert!(error.is_network());
        assert!(!error.is_connection());
    }

    #[test]
    fn exposes_timeout_kind_for_io_timeouts() {
        let network_timeout = network(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
        let connection_timeout = connection_io(io::Error::new(
            io::ErrorKind::WouldBlock,
            "connect timed out",
        ));

        assert_eq!(network_timeout.kind(), &ErrorKind::Timeout);
        assert!(network_timeout.is_timeout());
        assert_eq!(connection_timeout.kind(), &ErrorKind::Timeout);
        assert!(connection_timeout.is_timeout());
    }

    #[test]
    fn a_tls_failure_caused_by_an_io_timeout_is_found_in_the_source_chain() {
        #[derive(Debug)]
        struct Wrapper(io::Error);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("handshake failed")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        for kind in [io::ErrorKind::TimedOut, io::ErrorKind::WouldBlock] {
            assert!(super::chain_has_io_timeout(&Wrapper(io::Error::new(
                kind, "slow"
            ))));
        }
        assert!(!super::chain_has_io_timeout(&Wrapper(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "reset"
        ))));
    }

    #[test]
    fn exposes_internal_kind() {
        let error = internal("recipient status invariant failed");

        assert_eq!(error.kind(), &ErrorKind::Internal);
        assert!(error.is_internal());
        assert!(!error.is_invalid_input());
    }

    #[test]
    fn exposes_smtp_reply_kind_and_status() {
        let code = Code::new(
            Severity::TransientNegativeCompletion,
            Category::Connections,
            Detail::Zero,
        );
        let response = Response::new(code, vec!["4.2.0 mailbox busy".to_owned()]);
        let error = status(response.clone());

        assert_eq!(error.kind(), &ErrorKind::Transient(response));
        assert_eq!(error.status(), Some(code));
        assert_eq!(error.enhanced_status_code().unwrap().to_string(), "4.2.0");
        assert!(error.is_transient());
        assert!(!error.is_permanent());
        assert!(error.is_smtp_reply());
    }

    #[test]
    fn exposes_policy_kind() {
        let error = policy("refusing to authenticate over plaintext");

        assert_eq!(error.kind(), &ErrorKind::Policy);
        assert!(error.is_policy());
        assert!(!error.is_invalid_input());
    }
}
