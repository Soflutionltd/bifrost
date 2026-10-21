//! Error type for the shared SASL/SCRAM computation layer.

/// A SASL/SCRAM computation failure.
///
/// The crate is computation-only: these variants describe malformed protocol
/// messages or a failed SCRAM exchange, not transport or I/O errors. Each
/// protocol crate maps `SaslError` back into its own error enum at the call
/// boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SaslError {
    /// A malformed or unexpected SASL/SCRAM protocol message - the PEER's
    /// fault, or a peer-controlled input such as its certificate.
    #[error("SASL protocol error: {0}")]
    Protocol(String),
    /// A credential the CALLER supplied cannot be used: it fails RFC 4013
    /// SASLprep (a prohibited or unassigned code point, a bidi violation).
    /// Raised while preparing the exchange, before any byte of it is sent,
    /// so a protocol crate maps it to its local invalid-input lane - never
    /// to a provider fault, and never as a reason to retire the connection.
    #[error("SASL credential rejected: {0}")]
    InvalidCredential(String),
    /// The server reported an error in its SCRAM server-final message
    /// (the `e=` field). A server-signature mismatch is a `Protocol` error.
    #[error("SCRAM authentication failed: {0}")]
    AuthFailed(String),
}

impl SaslError {
    /// Return the SCRAM server-final `e=` token, when this error carries one.
    /// Protocol consumers can classify it without parsing display text.
    pub fn scram_server_error(&self) -> Option<&str> {
        match self {
            Self::AuthFailed(message) => message.strip_prefix("SCRAM server error: "),
            _ => None,
        }
    }
}
