//! Account routing projections over the Exchange Autodiscover client.

use bifrost_types::{AccountError, AccountOperation};

use super::GraphAccount;
use super::cursor::PublicFolderRouting;
use super::graph_error::GraphErrorContext;
use crate::autodiscover::{SharedMailbox, missing_setting};
#[cfg(test)]
pub(crate) use crate::autodiscover::{autodiscover_soap_url, autodiscover_xml_url};

/// Construct a synthetic SMTP address from a `PR_REPLICA_LIST` GUID and
/// domain, the address Autodiscover resolves to the real content mailbox.
pub(crate) fn construct_replica_smtp(guid: &str, domain: &str) -> String {
    format!("{guid}@{domain}")
}

/// Reduce the `GetUserSettings` name/value pairs into a
/// `PublicFolderRouting`: `PublicFolderInformation` is the hierarchy
/// (anchor) mailbox, `InternalRpcClientServer` is the public-folder
/// mailbox server. Errors (returns `None`) when `PublicFolderInformation`
/// is absent - there is no usable routing without it.
pub(crate) fn public_folder_routing_from_settings(
    settings: &[(String, String)],
) -> Option<PublicFolderRouting> {
    let mut anchor_mailbox: Option<String> = None;
    let mut public_folder_mailbox: Option<String> = None;
    for (name, value) in settings {
        match name.as_str() {
            "PublicFolderInformation" => anchor_mailbox = Some(value.clone()),
            "InternalRpcClientServer" => public_folder_mailbox = Some(value.clone()),
            _ => {}
        }
    }
    Some(PublicFolderRouting {
        anchor_mailbox: anchor_mailbox?,
        public_folder_mailbox,
    })
}

/// Merge config-supplied shared-mailbox routing keys with the
/// Autodiscover-enumerated delegates into a single deduplicated list.
/// Config entries come first and win ordering; every entry in the merged
/// set (config first, discovered appended) is empty-dropped and deduped
/// by exact string uniformly - a blank config entry seeds no client, and
/// a delegate named both in config and by discovery (or discovered
/// twice) is not seeded twice. Every discovered alternative mailbox is
/// kept regardless of `mailbox_type` - the routing layer keys on the
/// address alone.
pub(crate) fn merge_shared_mailboxes(
    config: &[String],
    discovered: &[SharedMailbox],
) -> Vec<String> {
    let candidates = config.iter().cloned().chain(
        discovered
            .iter()
            .map(|mailbox| mailbox.smtp_address.clone()),
    );
    let mut merged: Vec<String> = Vec::new();
    for candidate in candidates {
        if !candidate.is_empty()
            && !merged
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(&candidate))
        {
            merged.push(candidate);
        }
    }
    merged
}

impl GraphAccount {
    /// `GetUserSettings` SOAP for `PublicFolderInformation` +
    /// `InternalRpcClientServer` -> the hierarchy routing.
    pub(crate) async fn discover_public_folder_routing(
        &self,
        user_email: &str,
    ) -> Result<PublicFolderRouting, AccountError> {
        let parsed = self
            .client
            .soap_get_user_settings(
                user_email,
                &["PublicFolderInformation", "InternalRpcClientServer"],
            )
            .await?;
        public_folder_routing_from_settings(&parsed.settings).ok_or_else(|| {
            missing_setting(
                &parsed,
                "PublicFolderInformation",
                &GraphErrorContext::graph(AccountOperation::Discover),
            )
        })
    }

    /// `GetUserSettings` for `AutoDiscoverSMTPAddress` -> the real
    /// content mailbox SMTP for a replica GUID's synthetic address.
    pub(crate) async fn discover_content_mailbox(
        &self,
        replica_smtp: &str,
    ) -> Result<String, AccountError> {
        let parsed = self
            .client
            .soap_get_user_settings(replica_smtp, &["AutoDiscoverSMTPAddress"])
            .await?;
        parsed
            .settings
            .iter()
            .find_map(|(name, value)| (name == "AutoDiscoverSMTPAddress").then(|| value.clone()))
            .ok_or_else(|| {
                missing_setting(
                    &parsed,
                    "AutoDiscoverSMTPAddress",
                    &GraphErrorContext::graph(AccountOperation::Discover),
                )
            })
    }

    pub(crate) async fn discover_shared_mailboxes(
        &self,
        user_email: &str,
    ) -> Result<Vec<SharedMailbox>, AccountError> {
        self.client.discover_shared_mailboxes(user_email).await
    }
}
