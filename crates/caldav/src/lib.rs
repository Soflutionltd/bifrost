#![forbid(unsafe_code)]
#![doc = "CalDAV Account implementation for bifrost."]

mod account;
mod capabilities;
#[cfg(test)]
mod capability_contract_tests;
mod client;
mod dialect;
mod ical;
mod parse;

use std::fmt;
use std::sync::Arc;

use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, OpenedAccount,
};

/// Shared HTTP credentials with zeroizing password storage.
pub use bifrost_net::credentials::HttpCredentials as CalDavCredentials;

/// Configuration for a standalone CalDAV account.
#[derive(Clone)]
pub struct CalDavConfig {
    pub base_url: String,
    pub credentials: CalDavCredentials,
}

// Support needs the origin; resource URLs can contain passwords or bearer tokens.
impl fmt::Debug for CalDavConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CalDavConfig")
            .field(
                "base_url",
                &bifrost_dav_core::url::diagnostic_origin(&self.base_url),
            )
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl CalDavConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>, credentials: CalDavCredentials) -> Self {
        Self {
            base_url: base_url.into(),
            credentials,
        }
    }
}

/// Factory for opening standalone CalDAV accounts through the shared
/// `Account` API.
#[derive(Clone)]
pub struct CalDavAccountFactory {
    config: CalDavConfig,
    net: Option<Arc<bifrost_net::Net>>,
}

impl CalDavAccountFactory {
    #[must_use]
    pub fn new(config: CalDavConfig) -> Self {
        Self { config, net: None }
    }

    /// Attach through this transport before discovery, sharing its registry.
    #[must_use]
    pub fn with_net(mut self, net: Arc<bifrost_net::Net>) -> Self {
        self.net = Some(net);
        self
    }

    #[must_use]
    pub fn config(&self) -> &CalDavConfig {
        &self.config
    }
}

impl std::fmt::Debug for CalDavAccountFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CalDavAccountFactory")
            .field("config", &self.config)
            .field("net", &self.net.as_ref().map(|_| "<injected>"))
            .finish()
    }
}

impl AccountFactory for CalDavAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let config = self.config.clone();
        let net = self.net.clone();
        Box::pin(async move {
            let account = account::CalDavAccount::open(account_id, config, net).await?;
            // Every discovered calendar has its own cursor scope. Enterprise
            // users expect all collections to sync, so a successful open has
            // no collection-limit gaps to report.
            Ok(OpenedAccount {
                account: Arc::new(account) as Arc<dyn Account>,
                skipped_scopes: Vec::new(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_net::{AccessToken, StaticTokenSource};

    async fn current_bearer(credentials: &CalDavCredentials) -> Option<String> {
        match credentials {
            CalDavCredentials::OAuth2 {
                identity: (),
                token_source,
            } => Some(token_source.current().await.ok()?.as_str().to_owned()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn bearer_source_reads_current_token() {
        let source = StaticTokenSource::new("old-token", None);
        let credentials = CalDavCredentials::bearer_source(Arc::new(source.clone()));
        assert_eq!(
            current_bearer(&credentials).await.as_deref(),
            Some("old-token")
        );

        // A token rotated on the shared source is read live on the next
        // request, not frozen at open.
        source.set(AccessToken::new("new-token", None));
        assert_eq!(
            current_bearer(&credentials).await.as_deref(),
            Some("new-token")
        );

        assert!(
            current_bearer(&CalDavCredentials::Password {
                username: "u".to_string(),
                password: "p".into(),
            })
            .await
            .is_none()
        );
    }
}

#[cfg(test)]
mod config_debug_tests {
    use super::*;

    #[test]
    fn config_and_factory_debug_omit_url_credentials() {
        for url in [
            "https://secret-user:secret-password@dav.example/secret-path?secret-query#secret-fragment",
            "invalid-secret-url",
        ] {
            let config = CalDavConfig::new(
                url,
                CalDavCredentials::Password {
                    username: "user".into(),
                    password: "password".into(),
                },
            );
            let config_debug = format!("{config:?}");
            let factory_debug = format!("{:?}", CalDavAccountFactory::new(config));
            for debug in [config_debug, factory_debug] {
                assert!(!debug.contains("secret"), "{debug}");
                assert!(debug.contains("dav.example") || debug.contains("<invalid URL>"));
            }
        }
    }
}
