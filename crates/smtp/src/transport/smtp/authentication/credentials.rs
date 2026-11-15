//! Credential storage and live OAuth token resolution.

use super::{Mechanism, OAUTH2_MECHANISMS, PASSWORD_MECHANISMS, Zeroizing};
use crate::transport::smtp::error::{self, Error, SmtpCommandPhase};
use bifrost_net::auth::TokenSnapshot;
use bifrost_net::{AccessToken, NetErrorContext, StaticTokenSource, TokenSource};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Contains user credentials
//
// `Credentials` no longer derives `PartialEq`/`Eq` or the `serde`
// round-trip it used to: the OAuth variant now carries a live
// `Arc<dyn TokenSource>` instead of a frozen token string, and a token
// source is neither comparable nor serializable. Rotation material is
// the consumer's to persist; serializing a credential would freeze a
// token that is meant to rotate. Password credentials keep `Clone`.
#[derive(Clone)]
// pub: users configure password or OAuth bearer SASL credentials.
pub enum Credentials {
    /// Username and password credentials.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: Zeroizing<String>,
    },
    /// OAuth 2.0 bearer-token credentials.
    OAuth2 {
        /// Authorization identity, usually the email address being accessed.
        identity: String,
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

impl Credentials {
    /// Create username and password credentials.
    pub fn password<U, P>(username: U, password: P) -> Credentials
    where
        U: Into<String>,
        P: IntoSecretString,
    {
        Credentials::Password {
            username: username.into(),
            password: password.into_secret_string(),
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
    /// SMTP authentication without reconstructing the credential.
    pub fn oauth2_source<I>(identity: I, token_source: Arc<dyn TokenSource>) -> Credentials
    where
        I: Into<String>,
    {
        Credentials::OAuth2 {
            identity: identity.into(),
            token_source,
        }
    }

    pub(crate) fn preferred_mechanisms(&self) -> &'static [Mechanism] {
        match self {
            Credentials::Password { .. } => PASSWORD_MECHANISMS,
            Credentials::OAuth2 { .. } => OAUTH2_MECHANISMS,
        }
    }

    pub(crate) fn password_parts(&self) -> Result<(&str, &str), Error> {
        match self {
            Credentials::Password { username, password } => Ok((username, password.as_str())),
            Credentials::OAuth2 { .. } => Err(error::invalid_input(
                "OAuth2 credentials cannot be used with password authentication mechanisms",
            )),
        }
    }

    /// Pair the credential's OAuth identity with a token already resolved
    /// by the connection driver. Rejects password credentials (wrong
    /// mechanism) and a missing token (driver bug: an OAuth mechanism was
    /// driven without resolving the token first).
    pub(super) fn oauth_identity_and_token<'a>(
        &'a self,
        oauth_token: Option<&'a str>,
    ) -> Result<(&'a str, &'a str), Error> {
        match self {
            Credentials::OAuth2 { identity, .. } => {
                let token = oauth_token.ok_or_else(|| {
                    error::invalid_input("OAuth mechanism driven without a resolved access token")
                })?;
                Ok((identity, token))
            }
            Credentials::Password { .. } => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }

    /// Resolve the OAuth identity and a freshly read access token from
    /// the shared source. Used by the async transport, which awaits a
    /// possible refresh.
    ///
    /// Its only production caller lives in the Tokio-gated async connection,
    /// so a build without the `tokio` feature compiles this with no in-crate
    /// caller. The method stays present in every build rather than being
    /// gated away, because the blocking sibling `oauth2_token_blocking` is
    /// ungated and the pair is read as one API.
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
    pub(crate) async fn oauth2_token(&self) -> Result<(String, Zeroizing<String>), Error> {
        match self {
            Credentials::OAuth2 {
                identity,
                token_source,
            } => {
                let token = token_source.current().await.map_err(oauth_token_error)?;
                Ok((identity.clone(), Zeroizing::new(token.as_str().to_owned())))
            }
            Credentials::Password { .. } => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }

    /// Resolve a cached OAuth token without an executor or refresh side effects.
    /// Sources without synchronous snapshot support require the async transport.
    pub(crate) fn oauth2_token_blocking(&self) -> Result<(String, Zeroizing<String>), Error> {
        match self {
            Credentials::OAuth2 {
                identity,
                token_source,
            } => {
                // Polling current() even once can spawn a refresh without a
                // runtime. Only the explicit non-refreshing snapshot is safe.
                match token_source.snapshot().map_err(oauth_token_error)? {
                    TokenSnapshot::Usable(token) => {
                        Ok((identity.clone(), Zeroizing::new(token.as_str().to_owned())))
                    }
                    TokenSnapshot::Contended => Err(oauth_token_error(
                        bifrost_net::Error::RefreshFailed {
                            retry_after: None,
                            source: Arc::new(bifrost_net::Error::Network {
                                message: "OAuth token cache is busy; retry when ready".to_owned(),
                                transmission_state: bifrost_types::TransmissionState::Unsent,
                                source: None,
                            }),
                        },
                    )),
                    _ => Err(error::invalid_input(
                        "OAuth token source requires refresh or async resolution; use the async SMTP transport",
                    )
                    .with_phase(SmtpCommandPhase::Auth { mechanism: None })),
                }
            }
            Credentials::Password { .. } => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }
}

/// Preserve bifrost-net's typed projection for token-source failures. In
/// particular, `OAuthRefresher` distinguishes terminal credential loss from a
/// transient refresh failure; flattening either one to `InvalidInput` would
/// turn it into a terminal policy block at the SMTP account boundary.
pub(super) fn oauth_token_error(error: bifrost_net::Error) -> Error {
    let message = format!("failed to read OAuth access token: {error}");
    // A token endpoint throttle has no SMTP transaction scope to name.
    let account_error = bifrost_net::into_account_error_with_throttle_scope(
        error,
        NetErrorContext {
            provider: None,
            protocol: bifrost_types::error::Protocol::Smtp,
            operation: bifrost_types::error::AccountOperation::Send,
            scope: None,
        },
        None,
    );
    error::invalid_input(message)
        .with_preclassified_account_error(account_error)
        .with_phase(SmtpCommandPhase::Auth { mechanism: None })
}

impl Debug for Credentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Credentials::Password { .. } => f.debug_struct("Credentials::Password").finish(),
            Credentials::OAuth2 { .. } => f.debug_struct("Credentials::OAuth2").finish(),
        }
    }
}
