//! Shared credential storage. Net owns live token sources; SASL stays transport-free.
use std::{fmt, sync::Arc};

use zeroize::Zeroizing;

use crate::{AccessToken, StaticTokenSource, TokenSource};
pub use bifrost_sasl::Secret;

/// Password or live OAuth credentials shared by protocol clients.
///
/// Stream protocols use a String authorization identity. HTTP uses `()`:
/// its bearer header has no SASL identity, so none is invented or discarded.
//
// `Credentials` no longer derives `PartialEq`/`Eq` or the `serde`
// round-trip it used to: the OAuth variant now carries a live
// `Arc<dyn TokenSource>` instead of a frozen token string, and a token
// source is neither comparable nor serializable. Rotation material is
// the consumer's to persist; serializing a credential would freeze a
// token that is meant to rotate. Password credentials keep `Clone`.
#[non_exhaustive]
#[derive(Clone)]
// pub: protocol clients configure password or live OAuth credentials.
pub enum Credentials<I = String> {
    /// Username and password credentials.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: Secret,
    },
    /// OAuth 2.0 bearer-token credentials.
    OAuth2 {
        /// Authorization identity, usually the email address being accessed.
        identity: I,
        /// Live source of the OAuth 2.0 access token. Read at each wire
        /// authentication so a token rotated on the shared source is
        /// presented without reconstructing the credential.
        token_source: Arc<dyn TokenSource>,
    },
}

/// Converts owned secret material into a zeroizing string.
// pub: Credentials constructors expose this bound for secret inputs.
pub trait IntoSecretString {
    /// Move the secret into zeroizing storage.
    fn into_secret_string(self) -> Zeroizing<String>;
}

impl IntoSecretString for String {
    fn into_secret_string(self) -> Zeroizing<String> {
        Zeroizing::new(self)
    }
}

impl IntoSecretString for &str {
    fn into_secret_string(self) -> Zeroizing<String> {
        Zeroizing::new(self.to_owned())
    }
}

impl IntoSecretString for Zeroizing<String> {
    fn into_secret_string(self) -> Zeroizing<String> {
        self
    }
}

impl IntoSecretString for Secret {
    fn into_secret_string(self) -> Zeroizing<String> {
        self.into_zeroizing()
    }
}

impl Credentials {
    /// Create username and password credentials.
    pub fn password<U, P>(username: U, password: P) -> Credentials
    where
        U: Into<String>,
        P: IntoSecretString,
    {
        Credentials::Password {
            username: username.into(),
            password: Secret::from(password.into_secret_string()),
        }
    }

    /// Create OAuth 2.0 bearer-token credentials from a raw token string.
    /// Convenience wrapper: the token is held in a `StaticTokenSource` so
    /// existing call sites keep their string ergonomics.
    pub fn oauth2<I, T>(identity: I, access_token: T) -> Credentials
    where
        I: Into<String>,
        T: IntoSecretString,
    {
        let token = access_token.into_secret_string();
        Credentials::oauth2_source(
            identity,
            Arc::new(StaticTokenSource::from_token(AccessToken::from_zeroizing(
                token, None,
            ))),
        )
    }

    /// Create OAuth 2.0 bearer-token credentials from a shared token
    /// source. ratatoskr supplies one `Arc<dyn TokenSource>` it also
    /// drives rotation on, so a refreshed token is read live at every
    /// authentication without reconstructing the credential.
    pub fn oauth2_source<I>(identity: I, token_source: Arc<dyn TokenSource>) -> Credentials
    where
        I: Into<String>,
    {
        Credentials::OAuth2 {
            identity: identity.into(),
            token_source,
        }
    }
}

impl<I> fmt::Debug for Credentials<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Password { .. } => f.write_str("Credentials::Password(<redacted>)"),
            Self::OAuth2 { .. } => f.write_str("Credentials::OAuth2(<redacted>)"),
        }
    }
}

/// HTTP credentials use the same secret storage without a SASL identity.
/// The unit identity preserves that distinction without inventing an address.
pub type HttpCredentials = Credentials<()>;

impl Credentials<()> {
    /// Construct HTTP Basic credentials without a plaintext intermediate copy.
    pub fn basic(username: impl Into<String>, password: impl IntoSecretString) -> Self {
        Self::Password {
            username: username.into(),
            password: Secret::from(password.into_secret_string()),
        }
    }

    /// Bearer credentials from a raw token string. Convenience wrapper:
    /// the token is held in a `StaticTokenSource` so existing call sites
    /// keep their string ergonomics.
    #[must_use]
    pub fn bearer(access_token: impl IntoSecretString) -> Self {
        let token = AccessToken::from_zeroizing(access_token.into_secret_string(), None);
        Self::bearer_source(Arc::new(StaticTokenSource::from_token(token)))
    }

    /// Bearer credentials from a shared token source. ratatoskr supplies
    /// one `Arc<dyn TokenSource>` it drives rotation on; the token is
    /// read per request so a rotated token is honored mid-sync without
    /// reopen.
    #[must_use]
    pub fn bearer_source(source: Arc<dyn TokenSource>) -> Self {
        Self::OAuth2 {
            identity: (),
            token_source: source,
        }
    }
}

/// Encryption policy shared by stream protocols.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlsMode {
    /// TLS from connection establishment.
    Implicit,
    /// Select an in-protocol STARTTLS upgrade.
    StartTls,
    /// Unencrypted transport, subject to the protocol's authentication policy.
    None,
}

/// Service whose default port is being selected.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlsService {
    /// IMAP mailbox access.
    Imap,
    /// Client message submission, including plaintext submission.
    Submission,
    /// SMTP relay, whose plaintext port differs from submission.
    Smtp,
    /// LMTP's deployed port convention.
    Lmtp,
    /// ManageSieve uses the same port for all encryption modes.
    ManageSieve,
}

impl TlsMode {
    /// Whether TLS starts before the protocol greeting.
    pub const fn uses_implicit_tls(self) -> bool {
        matches!(self, Self::Implicit)
    }
    /// Whether the connection requires an in-protocol upgrade.
    pub const fn uses_starttls(self) -> bool {
        matches!(self, Self::StartTls)
    }
    /// The service default; explicit configured ports always take precedence.
    pub const fn port_for(self, service: TlsService) -> u16 {
        match (service, self) {
            (TlsService::Imap, Self::Implicit) => 993,
            (TlsService::Imap, _) => 143,
            (TlsService::Submission | TlsService::Smtp, Self::Implicit) => 465,
            (TlsService::Smtp, Self::None) => 25,
            (TlsService::Submission | TlsService::Smtp, _) => 587,
            (TlsService::Lmtp, _) => 24,
            (TlsService::ManageSieve, _) => 4190,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_debug_never_formats_secret_or_token_source() {
        for credentials in [
            Credentials::password("identity", "private-password"),
            Credentials::oauth2("identity", "private-token"),
        ] {
            let debug = format!("{credentials:?}");
            assert!(debug.contains("<redacted>"));
            assert!(!debug.contains("private-"));
        }
        let password = HttpCredentials::basic("identity", "private-password");
        let bearer = HttpCredentials::bearer("private-token");
        assert!(!format!("{password:?} {bearer:?}").contains("private-"));
        if let HttpCredentials::Password { password, .. } = password {
            assert_eq!(format!("{password:?}"), "<redacted>");
        }
    }

    #[test]
    fn service_ports_preserve_protocol_defaults() {
        use TlsService::*;
        for (service, ports) in [
            (Imap, [993, 143, 143]),
            (Submission, [465, 587, 587]),
            (Smtp, [465, 587, 25]),
            (Lmtp, [24, 24, 24]),
            (ManageSieve, [4190, 4190, 4190]),
        ] {
            for (mode, expected) in [TlsMode::Implicit, TlsMode::StartTls, TlsMode::None]
                .into_iter()
                .zip(ports)
            {
                assert_eq!(mode.port_for(service), expected);
            }
        }
    }
}
