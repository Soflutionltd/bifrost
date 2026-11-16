//! Credential storage and live OAuth token resolution.

use super::{Mechanism, OAUTH2_MECHANISMS, PASSWORD_MECHANISMS, Zeroizing};
use crate::transport::smtp::error::{self, Error, SmtpCommandPhase};
use bifrost_net::auth::TokenSnapshot;

pub use bifrost_net::{Credentials, IntoSecretString};

pub(crate) trait SmtpCredentialsExt {
    fn preferred_mechanisms(&self) -> &'static [Mechanism];
    fn password_parts(&self) -> Result<(&str, &str), Error>;
    #[cfg(feature = "tokio")]
    async fn oauth2_token(&self) -> Result<(String, Zeroizing<String>), Error>;
    fn oauth2_token_blocking(&self) -> Result<(String, Zeroizing<String>), Error>;
}

impl SmtpCredentialsExt for Credentials {
    fn preferred_mechanisms(&self) -> &'static [Mechanism] {
        match self {
            Credentials::Password { .. } => PASSWORD_MECHANISMS,
            Credentials::OAuth2 { .. } => OAUTH2_MECHANISMS,
            _ => &[],
        }
    }

    fn password_parts(&self) -> Result<(&str, &str), Error> {
        match self {
            Credentials::Password { username, password } => Ok((username, password.as_str())),
            _ => Err(error::invalid_input(
                "OAuth2 credentials cannot be used with password authentication mechanisms",
            )),
        }
    }

    /// Resolve the OAuth identity and a freshly read access token from
    /// the shared source. Used by the async transport, which awaits a
    /// possible refresh.
    #[cfg(feature = "tokio")]
    async fn oauth2_token(&self) -> Result<(String, Zeroizing<String>), Error> {
        match self {
            Credentials::OAuth2 {
                identity,
                token_source,
            } => {
                let token = token_source.current().await.map_err(oauth_token_error)?;
                Ok((identity.clone(), token.into_zeroizing()))
            }
            _ => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }

    /// Resolve a cached OAuth token without an executor or refresh side effects.
    /// Sources without synchronous snapshot support require the async transport.
    fn oauth2_token_blocking(&self) -> Result<(String, Zeroizing<String>), Error> {
        match self {
            Credentials::OAuth2 {
                identity,
                token_source,
            } => {
                // Polling current() even once can spawn a refresh without a
                // runtime. Only the explicit non-refreshing snapshot is safe.
                match token_source.snapshot().map_err(oauth_token_error)? {
                    TokenSnapshot::Usable(token) => {
                        Ok((identity.clone(), token.into_zeroizing()))
                    }
                    snapshot @ TokenSnapshot::Contended => {
                        let error = error::connection("OAuth token cache is busy; retry when ready");
                        #[cfg(feature = "account-error")]
                        let error = error.with_preclassified_account_error(
                            snapshot.contention_account_error(
                                bifrost_types::Protocol::Smtp,
                                bifrost_types::AccountOperation::Send,
                            ).expect("contended snapshot"),
                        );
                        #[cfg(not(feature = "account-error"))]
                        let _ = snapshot;
                        Err(error.with_phase(SmtpCommandPhase::Auth { mechanism: None }))
                    }
                    _ => Err(error::invalid_input(
                        "OAuth token source requires refresh or async resolution; use the async SMTP transport",
                    )
                    .with_phase(SmtpCommandPhase::Auth { mechanism: None })),
                }
            }
            _ => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }
}

/// Preserve bifrost-net's typed projection for token-source failures. In
/// particular, `OAuthRefresher` distinguishes terminal credential loss from a
/// transient refresh failure. Without account integration the original net
/// error remains available through the low-level error source chain.
/// Both builds use the direct `Connection` class: an enterprise Outlook user
/// expects a token endpoint outage to retry, not to blame caller input or ask
/// for a replacement password. The typed projection still distinguishes actual
/// credential loss from a transient outage for account-aware recovery.
pub(super) fn oauth_token_error(error: bifrost_net::Error) -> Error {
    #[cfg(feature = "account-error")]
    let message = format!("failed to read OAuth access token: {error}");
    // A token endpoint throttle has no SMTP transaction scope to name.
    #[cfg(feature = "account-error")]
    let account_error = bifrost_net::token_source_account_error(
        error,
        bifrost_types::error::Protocol::Smtp,
        bifrost_types::error::AccountOperation::Send,
    );
    #[cfg(feature = "account-error")]
    let error = error::connection(message).with_preclassified_account_error(account_error);
    #[cfg(not(feature = "account-error"))]
    let error = error::connection(error);
    error.with_phase(SmtpCommandPhase::Auth { mechanism: None })
}

#[cfg(test)]
mod tests {
    #[test]
    fn contended_snapshot_has_transient_authentication_without_wire_evidence() {
        let error = bifrost_net::auth::TokenSnapshot::Contended
            .contention_account_error(
                bifrost_types::Protocol::Smtp,
                bifrost_types::AccountOperation::Send,
            )
            .unwrap();
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::RefreshTransient,
            )
        );
        assert!(error.chain().iter().all(|cause| !matches!(
            cause,
            bifrost_types::Cause::Transport(_) | bifrost_types::Cause::Attempt(_)
        )));
    }

    #[test]
    fn token_source_failure_has_a_connection_class_in_every_feature_build() {
        let error = super::oauth_token_error(bifrost_net::Error::RefreshFailed {
            retry_after: None,
            source: std::sync::Arc::new(bifrost_net::Error::Cancelled),
        });

        assert_eq!(
            error.kind(),
            &crate::transport::smtp::error::ErrorKind::Connection
        );
        assert_eq!(
            error.phase(),
            Some(super::SmtpCommandPhase::Auth { mechanism: None })
        );
    }
}
