//! Account settings, quota and unsupported primitive results.

use super::ImapAccount;
use super::error::op_err;
use crate::types::MailboxName;
use bifrost_types::cloud::HostedAttachment;
use bifrost_types::compose::{AttachmentHandle, IdentityId};
use bifrost_types::container::FolderRole;
use bifrost_types::ids::ObjectId;
use bifrost_types::settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};
use bifrost_types::{AccountError, AccountFuture, AccountOperation};

pub(crate) fn unsupported_unit(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_object(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_attachment(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_hosted(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<HostedAttachment, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn identities_list() -> AccountFuture<Result<Vec<Identity>, AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::IdentitiesList)) })
}

pub(crate) fn identity_update(
    _identity: IdentityId,
    _patch: IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::IdentityUpdate)) })
}

pub(crate) fn vacation_get() -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::VacationGet)) })
}

pub(crate) fn vacation_set(_config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::VacationSet)) })
}

pub(crate) fn quota_get(
    account: ImapAccount,
) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.quota_get {
            return Err(super::error::unsupported(AccountOperation::QuotaGet));
        }
        let Some(folder) = quota_probe_folder(&account) else {
            return Ok(None);
        };
        let err = op_err(AccountOperation::QuotaGet);
        let conn = account.pool.checkout_any().await.map_err(err)?;
        let quota = conn
            .connection()
            .get_quota_root(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        for (_root, resources) in quota.resources {
            for resource in resources {
                if resource.name.eq_ignore_ascii_case("STORAGE") {
                    return Ok(Some(QuotaInfo {
                        used_bytes: resource.usage.saturating_mul(1024),
                        total_bytes: Some(resource.limit.saturating_mul(1024)),
                    }));
                }
            }
        }
        Ok(None)
    })
}

fn quota_probe_folder(account: &ImapAccount) -> Option<MailboxName> {
    account
        .folders
        .role_folder(FolderRole::Inbox)
        .or_else(|| first_selectable_folder(account.folders.entries()))
}

fn first_selectable_folder(
    entries: Vec<std::sync::Arc<super::folder_registry::FolderEntry>>,
) -> Option<MailboxName> {
    entries
        .into_iter()
        // Account quota must not silently report a colleague's shared mailbox
        // usage when the user's personal namespace has no INBOX.
        .filter(|entry| entry.selectable() && entry.shared_owner.is_none())
        // `entries()` comes from a HashMap, and distinct mailbox identities
        // can share a display name. Keep the fallback stable across rebuilds.
        .min_by(|left, right| left.name.cmp_account_order(&right.name))
        .map(|entry| entry.name.clone())
}

#[cfg(test)]
mod tests;
