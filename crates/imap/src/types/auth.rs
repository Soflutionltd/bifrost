//! Authentication policy and credential types.

use std::fmt;
use std::sync::Arc;

use bifrost_net::{StaticTokenSource, TokenSource};

use super::SecretString;

/// Credentials accepted by `ImapAccountFactory`.
///
/// This models the common single-identity flows. Delegated access with a
/// distinct SASL authorization identity is not represented here yet.
//
// `Credentials` deliberately drops the `PartialEq`/`Eq` it once derived:
// the OAuth variant now holds a live `Arc<dyn TokenSource>` instead of a
// frozen token string, and a token source is not `Eq`. Nothing in the
// crate compares credentials for equality. Unlike SMTP's `Credentials`,
// the IMAP type was never serde-derived, so this is the whole of the
// derive fallout here.
#[non_exhaustive]
#[derive(Clone)]
pub struct Credentials {
    kind: CredentialsKind,
}

#[derive(Clone)]
pub(crate) enum CredentialsKind {
    /// Username and password credentials.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: SecretString,
    },
    /// OAuth 2.0 bearer token credentials.
    OAuth2 {
        /// Authorization identity, usually the email address being accessed.
        identity: String,
        /// Live source of the OAuth 2.0 access token. Read at each
        /// connect (and every reconnect) so a token rotated on the
        /// shared source is presented without rebuilding the account -
        /// closing the stale-token-on-reconnect path.
        token_source: Arc<dyn TokenSource>,
    },
}

impl Credentials {
    /// Create username and password credentials.
    pub fn password(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            kind: CredentialsKind::Password {
                username: username.into(),
                password: SecretString::from(password.into()),
            },
        }
    }

    /// Create OAuth 2.0 bearer-token credentials from a raw token string.
    /// Convenience wrapper: the token is held in a `StaticTokenSource` so
    /// existing call sites keep their string ergonomics.
    pub fn oauth2(identity: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self::oauth2_source(
            identity,
            Arc::new(StaticTokenSource::new(access_token.into(), None)),
        )
    }

    /// Create OAuth 2.0 bearer-token credentials from a shared token
    /// source. ratatoskr supplies one `Arc<dyn TokenSource>` it also
    /// drives rotation on; the token is read fresh at every connect and
    /// reconnect, so a refreshed token is presented without reopening.
    pub fn oauth2_source(identity: impl Into<String>, source: Arc<dyn TokenSource>) -> Self {
        Self {
            kind: CredentialsKind::OAuth2 {
                identity: identity.into(),
                token_source: source,
            },
        }
    }

    pub(crate) fn kind(&self) -> &CredentialsKind {
        &self.kind
    }
}

impl CredentialsKind {
    /// Read the OAuth access token the auth path would present right now.
    /// This is the exact per-connect read point reused on every
    /// reconnect, so a value swapped on the shared source between two
    /// reads proves a rotated token is re-read rather than frozen.
    #[cfg(test)]
    pub(crate) async fn current_token(&self) -> Option<String> {
        match self {
            CredentialsKind::OAuth2 { token_source, .. } => {
                Some(token_source.current().await.ok()?.as_str().to_owned())
            }
            CredentialsKind::Password { .. } => None,
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            CredentialsKind::Password { username, .. } => f
                .debug_struct("Credentials::Password")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            CredentialsKind::OAuth2 { identity, .. } => f
                .debug_struct("Credentials::OAuth2")
                .field("identity", identity)
                .field("token_source", &"<token-source>")
                .finish(),
        }
    }
}

/// Shared vocabulary distinguishes IMAP LOGIN from SASL LOGIN.
pub(crate) use bifrost_sasl::Mechanism as AuthMechanism;

/// Policy for automatic authentication mechanism selection.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthPolicy {
    /// Allow the legacy LOGIN command.
    ///
    /// Default: `false`. LOGIN is not SASL and is deprecated for IMAP4rev2.
    pub allow_login: bool,
    /// Allow credential-bearing mechanisms on a connection that is not encrypted.
    ///
    /// This gates every mechanism, including SCRAM: authentication proofs do
    /// not protect subsequent mail traffic on an unauthenticated channel.
    ///
    /// Default: `false`.
    pub allow_cleartext_without_tls: bool,
    /// Allow CRAM-MD5 as a legacy fallback.
    ///
    /// Default: `false`. CRAM-MD5 is vulnerable to offline password
    /// guessing when the challenge is attacker-controlled, so automatic
    /// selection only uses it when callers opt in.
    pub allow_cram_md5: bool,
}

impl AuthPolicy {
    /// Permit the legacy LOGIN command as a last resort.
    pub fn with_login(mut self) -> Self {
        self.allow_login = true;
        self
    }

    /// Permit cleartext credential mechanisms without TLS.
    pub fn allow_cleartext_without_tls(mut self) -> Self {
        self.allow_cleartext_without_tls = true;
        self
    }

    /// Permit CRAM-MD5 as a legacy fallback.
    pub fn with_cram_md5(mut self) -> Self {
        self.allow_cram_md5 = true;
        self
    }

    /// Disable CRAM-MD5 fallback.
    pub fn without_cram_md5(mut self) -> Self {
        self.allow_cram_md5 = false;
        self
    }
}

/// Result of automatic authentication.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct AuthOutcome {
    /// Mechanism used for the successful authentication exchange.
    pub(crate) mechanism: AuthMechanism,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_net::AccessToken;

    #[tokio::test]
    async fn oauth2_credentials_reread_rotated_token() {
        let source = StaticTokenSource::new("old-token", None);
        let credentials = Credentials::oauth2_source("user@example.com", Arc::new(source.clone()));

        // First read: the token the auth path would present on the
        // initial connect.
        assert_eq!(
            credentials.kind().current_token().await.as_deref(),
            Some("old-token")
        );

        // Rotate the token on the shared source, as ratatoskr would after
        // refreshing and persisting a new access token.
        source.set(AccessToken::new("new-token", None));

        // Second read re-enters the exact per-connect read point a
        // reconnect would use, and observes the rotated token - proving
        // the stale-token-on-reconnect path is closed.
        assert_eq!(
            credentials.kind().current_token().await.as_deref(),
            Some("new-token")
        );

        // Password credentials have no token source.
        let password = Credentials::password("user", "pw");
        assert!(password.kind().current_token().await.is_none());
    }
}
