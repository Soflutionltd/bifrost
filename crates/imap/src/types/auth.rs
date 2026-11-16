//! Authentication policy and shared credential storage.

pub use bifrost_net::Credentials;
pub(crate) use bifrost_net::Credentials as CredentialsKind;

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
    use bifrost_net::{AccessToken, StaticTokenSource};
    use std::sync::Arc;
    async fn current_token(credentials: &Credentials) -> Option<String> {
        match credentials {
            Credentials::OAuth2 { token_source, .. } => {
                Some(token_source.current().await.ok()?.as_str().to_owned())
            }
            _ => None,
        }
    }

    #[tokio::test]
    async fn oauth2_credentials_reread_rotated_token() {
        let source = StaticTokenSource::new("old-token", None);
        let credentials = Credentials::oauth2_source("user@example.com", Arc::new(source.clone()));

        // First read: the token the auth path would present on the
        // initial connect.
        assert_eq!(
            current_token(&credentials).await.as_deref(),
            Some("old-token")
        );

        // Rotate the token on the shared source, as ratatoskr would after
        // refreshing and persisting a new access token.
        source.set(AccessToken::new("new-token", None));

        // Second read re-enters the exact per-connect read point a
        // reconnect would use, and observes the rotated token - proving
        // the stale-token-on-reconnect path is closed.
        assert_eq!(
            current_token(&credentials).await.as_deref(),
            Some("new-token")
        );

        // Password credentials have no token source.
        let password = Credentials::password("user", "pw");
        assert!(current_token(&password).await.is_none());
    }
}
