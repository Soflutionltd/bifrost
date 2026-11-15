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
    /// The server's typed SCRAM server-final `e=` refusal.
    #[error("SCRAM server error: {0}")]
    ServerRejected(ScramServerError),
    /// A local cryptographic facility failed, independently of peer input.
    #[error("SASL local runtime failure: {0}")]
    LocalRuntime(String),
}

/// Recovery intent for a SCRAM refusal. Only Credential asks for a new password.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RefusalClass {
    Credential,
    Protocol,
    Policy,
    Unavailable,
}

/// RFC 5802 server-error vocabulary, preserving extension tokens losslessly.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScramServerError {
    InvalidEncoding,
    ExtensionsNotSupported,
    InvalidProof,
    ChannelBindingsDontMatch,
    ServerDoesSupportChannelBinding,
    ChannelBindingNotSupported,
    UnsupportedChannelBindingType,
    UnknownUser,
    InvalidUsernameEncoding,
    NoResources,
    OtherError,
    Other(String),
}

impl ScramServerError {
    pub fn from_token(token: &str) -> Self {
        match token {
            "invalid-encoding" => Self::InvalidEncoding,
            "extensions-not-supported" => Self::ExtensionsNotSupported,
            "invalid-proof" => Self::InvalidProof,
            "channel-bindings-dont-match" => Self::ChannelBindingsDontMatch,
            "server-does-support-channel-binding" => Self::ServerDoesSupportChannelBinding,
            "channel-binding-not-supported" => Self::ChannelBindingNotSupported,
            "unsupported-channel-binding-type" => Self::UnsupportedChannelBindingType,
            "unknown-user" => Self::UnknownUser,
            "invalid-username-encoding" => Self::InvalidUsernameEncoding,
            "no-resources" => Self::NoResources,
            "other-error" => Self::OtherError,
            other => Self::Other(other.to_owned()),
        }
    }

    pub fn token(&self) -> &str {
        match self {
            Self::InvalidEncoding => "invalid-encoding",
            Self::ExtensionsNotSupported => "extensions-not-supported",
            Self::InvalidProof => "invalid-proof",
            Self::ChannelBindingsDontMatch => "channel-bindings-dont-match",
            Self::ServerDoesSupportChannelBinding => "server-does-support-channel-binding",
            Self::ChannelBindingNotSupported => "channel-binding-not-supported",
            Self::UnsupportedChannelBindingType => "unsupported-channel-binding-type",
            Self::UnknownUser => "unknown-user",
            Self::InvalidUsernameEncoding => "invalid-username-encoding",
            Self::NoResources => "no-resources",
            Self::OtherError => "other-error",
            Self::Other(token) => token,
        }
    }

    /// Enterprise users should repair credentials only on evidence they are
    /// wrong. A server outage or an unknown extension is not that evidence.
    /// Binding failures require policy repair, never a weaker mechanism retry.
    pub fn refusal_class(&self) -> RefusalClass {
        match self {
            Self::InvalidProof | Self::UnknownUser => RefusalClass::Credential,
            Self::InvalidEncoding | Self::InvalidUsernameEncoding => RefusalClass::Protocol,
            Self::ExtensionsNotSupported
            | Self::ChannelBindingsDontMatch
            | Self::ServerDoesSupportChannelBinding
            | Self::ChannelBindingNotSupported
            | Self::UnsupportedChannelBindingType => RefusalClass::Policy,
            Self::NoResources | Self::OtherError | Self::Other(_) => RefusalClass::Unavailable,
        }
    }
}

impl std::fmt::Display for ScramServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.token())
    }
}

impl SaslError {
    /// Return the typed server refusal without parsing display text.
    pub fn scram_server_error(&self) -> Option<&ScramServerError> {
        match self {
            Self::ServerRejected(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RefusalClass, ScramServerError};

    #[test]
    fn server_refusal_table_preserves_tokens_and_password_repair_intent() {
        use RefusalClass::{Credential, Policy, Protocol, Unavailable};
        for (token, expected) in [
            ("invalid-encoding", Protocol),
            ("extensions-not-supported", Policy),
            ("invalid-proof", Credential),
            ("channel-bindings-dont-match", Policy),
            ("server-does-support-channel-binding", Policy),
            ("channel-binding-not-supported", Policy),
            ("unsupported-channel-binding-type", Policy),
            ("unknown-user", Credential),
            ("invalid-username-encoding", Protocol),
            ("no-resources", Unavailable),
            ("other-error", Unavailable),
            ("future-extension", Unavailable),
        ] {
            let refusal = ScramServerError::from_token(token);
            assert_eq!(refusal.token(), token);
            assert_eq!(refusal.refusal_class(), expected, "{token}");
        }
    }
}
