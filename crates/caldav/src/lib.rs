#![forbid(unsafe_code)]
#![doc = "CalDAV Account implementation for bifrost."]

mod account;
mod capabilities;
#[cfg(test)]
mod capability_contract_tests;
mod client;
mod ical;
mod parse;

use std::fmt;
use std::sync::Arc;

use bifrost_net::{StaticTokenSource, TokenSource};
use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, OpenedAccount,
};

/// Authentication mode for a CalDAV account.
//
// `PartialEq`/`Eq` are dropped: the bearer variant now holds a live
// `Arc<dyn TokenSource>`, which is not comparable. Nothing compares
// credentials for equality. `Debug` is hand-written to redact the
// source.
#[derive(Clone)]
#[non_exhaustive]
pub enum CalDavCredentials {
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// OAuth2 bearer authentication, read live from a shared token
    /// source at every DAV request.
    Bearer { token_source: Arc<dyn TokenSource> },
}

impl CalDavCredentials {
    /// Bearer credentials from a raw token string. Convenience wrapper:
    /// the token is held in a `StaticTokenSource` so existing call sites
    /// keep their string ergonomics.
    #[must_use]
    pub fn bearer(access_token: impl Into<String>) -> Self {
        Self::bearer_source(Arc::new(StaticTokenSource::new(access_token.into(), None)))
    }

    /// Bearer credentials from a shared token source. ratatoskr supplies
    /// one `Arc<dyn TokenSource>` it drives rotation on; the token is
    /// read per request so a rotated token is honored mid-sync without
    /// reopen.
    #[must_use]
    pub fn bearer_source(source: Arc<dyn TokenSource>) -> Self {
        Self::Bearer {
            token_source: source,
        }
    }

    /// Project onto the shared dispatcher's credential type.
    ///
    /// The published enum stays exactly as it is; `bifrost-dav-core` needs one
    /// shape it can serve both DAV crates with, and this is the one-way
    /// conversion into it. Bearer clones the `Arc`, so the token is still read
    /// live from the shared source at every request.
    pub(crate) fn to_shared(&self) -> bifrost_dav_core::DavCredentials {
        match self {
            Self::Basic { username, password } => bifrost_dav_core::DavCredentials::Basic {
                username: username.clone(),
                password: password.clone(),
            },
            Self::Bearer { token_source } => bifrost_dav_core::DavCredentials::Bearer {
                token_source: Arc::clone(token_source),
            },
        }
    }

    /// Read the bearer token the DAV request builder would present right
    /// now. This is the same per-request read point, so a value swapped
    /// on the shared source between two reads proves the token is read
    /// live rather than frozen at open.
    #[cfg(test)]
    pub(crate) async fn current_bearer(&self) -> Option<String> {
        match self {
            Self::Bearer { token_source } => {
                Some(token_source.current().await.ok()?.as_str().to_owned())
            }
            Self::Basic { .. } => None,
        }
    }
}

impl fmt::Debug for CalDavCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Bearer { .. } => f
                .debug_struct("Bearer")
                .field("token_source", &"<token-source>")
                .finish(),
        }
    }
}

/// Configuration for a standalone CalDAV account.
#[derive(Debug, Clone)]
pub struct CalDavConfig {
    pub base_url: String,
    pub credentials: CalDavCredentials,
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
            // Empty by construction: discovery mints one folder cursor scope
            // per calendar collection, so there is no collection the sync
            // lanes leave behind and nothing for the skip lane to report.
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

    #[tokio::test]
    async fn bearer_source_reads_current_token() {
        let source = StaticTokenSource::new("old-token", None);
        let credentials = CalDavCredentials::bearer_source(Arc::new(source.clone()));
        assert_eq!(
            credentials.current_bearer().await.as_deref(),
            Some("old-token")
        );

        // A token rotated on the shared source is read live on the next
        // request, not frozen at open.
        source.set(AccessToken::new("new-token", None));
        assert_eq!(
            credentials.current_bearer().await.as_deref(),
            Some("new-token")
        );

        assert!(
            CalDavCredentials::Basic {
                username: "u".to_string(),
                password: "p".to_string(),
            }
            .current_bearer()
            .await
            .is_none()
        );
    }
}
