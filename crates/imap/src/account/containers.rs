//! Mailbox container projection and hierarchy edits.

use super::error::{folder_err, op_err, pim_malformed};
use super::folder_registry::folder_role;
use super::{ImapAccount, factory};
use crate::connection::ImapConnection;
use crate::types::{MailboxName, MailboxRights, StatusItem};
use bifrost_types::container::{
    Container, ContainerId, ContainerKind, ContainerList, ContainerNamespace, ContainerRights,
    Provenance,
};
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, ErrorScope, PartialCommitBuilder, PrimaryLeg,
    ProtocolKind, StepEffect, StepReplay, StepTargets, TargetRef, TargetRole,
};

pub(crate) fn containers_list(
    account: ImapAccount,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    Box::pin(async move {
        Ok(ContainerList {
            containers: containers_snapshot(&account),
            skipped_scopes: account.discovery_skips.clone(),
        })
    })
}

fn container_refresh_error(
    error: AccountError,
    effect: StepEffect,
    source: &MailboxName,
    destination: Option<&MailboxName>,
) -> AccountError {
    let mut refs = vec![TargetRef::new(
        if destination.is_some() {
            TargetRole::Source
        } else {
            TargetRole::Subject
        },
        ErrorScope::Cursor(super::folder_scope(source)),
    )];
    if let Some(destination) = destination {
        refs.push(TargetRef::new(
            TargetRole::Destination,
            ErrorScope::Cursor(super::folder_scope(destination)),
        ));
    }
    let targets = StepTargets::complete(refs);
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(targets.clone()));
    record.committed(effect, targets, StepReplay::Blocking);
    super::mutate::finish_partial(error, record)
}

pub(crate) fn container_create(
    account: ImapAccount,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    // IMAP mailboxes carry no container color; accepted for trait
    // parity with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(super::error::unsupported(AccountOperation::ContainerCreate));
        }
        let full_name = child_name(
            &account,
            parent.as_ref(),
            &name,
            AccountOperation::ContainerCreate,
        )?;
        let authority = parent
            .as_ref()
            .map(|parent| {
                folder_from_container(
                    &account.dav_scopes,
                    parent,
                    AccountOperation::ContainerCreate,
                )
            })
            .transpose()?;
        let err = |error| {
            account.folder_error(
                error,
                authority.as_ref().unwrap_or(&full_name),
                super::error::ImapErrorContext::operation(AccountOperation::ContainerCreate),
            )
        };
        let conn = account.pool.checkout_any().await.map_err(err)?;
        conn.connection()
            .create(&full_name, account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(
            &account,
            conn.connection(),
            AccountOperation::ContainerCreate,
        )
        .await
        .map_err(|error| container_refresh_error(error, StepEffect::Created, &full_name, None))?;
        Ok(ContainerId(full_name.account_id()))
    })
}

pub(crate) fn container_rename(
    account: ImapAccount,
    container: ContainerId,
    name: String,
    // IMAP has no mailbox recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(
            &account.dav_scopes,
            &container,
            AccountOperation::ContainerRename,
        )?;
        let new_name =
            renamed_sibling(&account, &folder, &name, AccountOperation::ContainerRename)?;
        let err = folder_err(&account, &folder, AccountOperation::ContainerRename);
        let mut conn = account.pool.checkout_any().await.map_err(err)?;
        conn.deselect_target(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        conn.connection()
            .rename(&folder, &new_name, account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(
            &account,
            conn.connection(),
            AccountOperation::ContainerRename,
        )
        .await
        .map_err(|error| {
            container_refresh_error(error, StepEffect::Moved, &folder, Some(&new_name))
        })
    })
}

pub(crate) fn container_move(
    account: ImapAccount,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(
            &account.dav_scopes,
            &container,
            AccountOperation::ContainerMove,
        )?;
        let leaf = leaf_name(&account, &folder);
        let new_name = child_name(
            &account,
            new_parent.as_ref(),
            &leaf,
            AccountOperation::ContainerMove,
        )?;
        let destination = new_parent
            .as_ref()
            .map(|parent| {
                folder_from_container(&account.dav_scopes, parent, AccountOperation::ContainerMove)
            })
            .transpose()?;
        let err = folder_err(&account, &folder, AccountOperation::ContainerMove);
        let mut conn = account.pool.checkout_any().await.map_err(err)?;
        conn.deselect_target(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        conn.connection()
            .rename(&folder, &new_name, account.command_timeout())
            .await
            .map_err(|error| {
                account.folder_pair_error(
                    error,
                    &folder,
                    destination.as_ref(),
                    super::error::ImapErrorContext::operation(AccountOperation::ContainerMove),
                )
            })?;
        refresh_folders(&account, conn.connection(), AccountOperation::ContainerMove)
            .await
            .map_err(|error| {
                container_refresh_error(error, StepEffect::Moved, &folder, Some(&new_name))
            })
    })
}

pub(crate) fn container_delete(
    account: ImapAccount,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(
            &account.dav_scopes,
            &container,
            AccountOperation::ContainerDelete,
        )?;
        let err = folder_err(&account, &folder, AccountOperation::ContainerDelete);
        let mut conn = account.pool.checkout_any().await.map_err(err)?;
        conn.deselect_target(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        let status = conn
            .connection()
            .status(&folder, "MESSAGES", account.command_timeout())
            .await
            .map_err(err)?;
        let non_empty = status
            .items
            .iter()
            .any(|item| matches!(item, StatusItem::Messages(count) if *count > 0));
        if non_empty {
            return Err(pim_malformed(
                AccountOperation::ContainerDelete,
                "refusing to delete non-empty IMAP mailbox",
            ));
        }
        conn.connection()
            .delete(&folder, account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(
            &account,
            conn.connection(),
            AccountOperation::ContainerDelete,
        )
        .await
        .map_err(|error| container_refresh_error(error, StepEffect::Deleted, &folder, None))
    })
}

pub(super) fn folder_from_container(
    scopes: &super::DavScopeIndex,

    container: &ContainerId,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    scopes
        .imap_container(container, op)
        .map(super::ImapFolder::into_mailbox)
}

fn containers_snapshot(account: &ImapAccount) -> Vec<Container> {
    account
        .folders
        .entries()
        .into_iter()
        .map(|entry| {
            let display_name = leaf_name(account, &entry.name);
            container_from_folder_entry(&entry, display_name)
        })
        .collect()
}

/// Project one registry entry onto a `Container`. Pure over the entry plus
/// the already-resolved display name (the only piece that needs the
/// account, for INBOX aliasing), so the namespace / owner / rights
/// projection is unit-pinnable without a live connection.
///
/// A shared/other-user folder is namespaced by its owning mailbox. IMAP has
/// no separate per-owner id space - the full mailbox path IS the native id
/// in every namespace - so `owner_local_id` is that same path, and
/// `native_id` (which the cursor scope keys on) needs no re-encoding. That
/// keeps `native_id` byte-identical to the `CursorScope::Folder` string
/// `discover_cursor_scopes` emits for the same folder.
fn container_from_folder_entry(
    entry: &super::folder_registry::FolderEntry,
    display_name: String,
) -> Container {
    let native = entry.name.account_id();
    let namespace = if entry.shared_owner.is_some() {
        ContainerNamespace::Shared
    } else {
        ContainerNamespace::Personal
    };
    Container::new(
        ContainerId(native.clone()),
        ContainerKind::Folder,
        entry
            .shared_owner
            .is_none()
            .then(|| folder_role(&entry.attributes(), entry.name.as_str(), entry.delimiter()))
            .flatten(),
        Provenance {
            provider: ProtocolKind::Imap,
            kind: ContainerKind::Folder,
            native: native.clone(),
        },
        display_name,
        parent_id(entry.delimiter(), &native),
    )
    // IMAP mailboxes carry no container color, and IMAP is folder-shaped
    // (special-use maps into `role`), so `style` and `system` keep their
    // `Container::new` defaults.
    .with_namespace(namespace)
    .with_owner(entry.shared_owner.clone())
    .with_owner_local_id(entry.shared_owner.as_ref().map(|_| native.clone()))
    // RFC 4314 MYRIGHTS, captured at shared-folder discovery. `None` for a
    // personal folder or a server without ACL.
    .with_rights(entry.rights.as_ref().map(rights_from_myrights))
}

/// Project an RFC 4314 MYRIGHTS set onto the unified [`ContainerRights`].
///
/// Every member is `Some(_)`: once the server answered MYRIGHTS, the
/// absence of a letter is a definite "no", not "unreported" (the
/// unreported case is `FolderEntry::rights == None`, which never reaches
/// here). The mapping follows RFC 4314 Section 4:
///
/// The typed rights set owns the projection. RFC 4314 Section 2.1.1 says
/// virtual c/d in MYRIGHTS responses must be ignored, not expanded; granting
/// x from d could offer deletion of a shared folder without server authority.
fn rights_from_myrights(rights: &MailboxRights) -> ContainerRights {
    rights.container_rights()
}

fn parent_id(delimiter: Option<char>, native: &str) -> Option<ContainerId> {
    let delimiter = delimiter?;
    let parent = MailboxName::from_account_id(native)
        .ok()?
        .parent(delimiter)
        .ok()??;
    Some(ContainerId(parent.account_id()))
}

fn child_name(
    account: &ImapAccount,
    parent: Option<&ContainerId>,
    leaf: &str,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    let Some(parent) = parent else {
        return MailboxName::new(leaf.to_owned()).map_err(|e| pim_malformed(op, e.to_string()));
    };
    let parent_folder = folder_from_container(&account.dav_scopes, parent, op)?;
    let delimiter = account
        .folders
        .get(&parent_folder)
        .and_then(|entry| entry.delimiter())
        .ok_or_else(|| super::error::unsupported(op))?;
    parent_folder
        .child(delimiter, leaf)
        .map_err(|e| pim_malformed(op, e.to_string()))
}

fn renamed_sibling(
    account: &ImapAccount,
    folder: &MailboxName,
    new_leaf: &str,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    let delimiter = account
        .folders
        .get(folder)
        .and_then(|entry| entry.delimiter());
    let Some(delimiter) = delimiter else {
        return MailboxName::new(new_leaf.to_owned()).map_err(|e| pim_malformed(op, e.to_string()));
    };
    if let Some(parent) = folder
        .parent(delimiter)
        .map_err(|e| pim_malformed(op, e.to_string()))?
    {
        parent
            .child(delimiter, new_leaf)
            .map_err(|e| pim_malformed(op, e.to_string()))
    } else {
        MailboxName::new(new_leaf.to_owned()).map_err(|e| pim_malformed(op, e.to_string()))
    }
}

fn leaf_name(account: &ImapAccount, folder: &MailboxName) -> String {
    let delimiter = account
        .folders
        .get(folder)
        .and_then(|entry| entry.delimiter());
    crate::types::mailbox::SpecialUse::leaf_name(folder.as_str(), delimiter).to_owned()
}

/// Re-LIST the personal namespace over a connection the caller already holds.
///
/// The connection is a parameter rather than a fresh checkout on purpose: every
/// caller is mid-mutation and still holds a pooled checkout, so acquiring a
/// second one here would deadlock whenever the in-flight mutations exhaust the
/// pool cap. LIST is not mailbox-relative, so reusing the mutation's own connection
/// is safe regardless of what it has selected.
async fn refresh_folders(
    account: &ImapAccount,
    conn: &ImapConnection,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    let profile = conn.server_profile();
    let folders = factory::list_folders(conn, &account.config, &profile)
        .await
        .map_err(err)?;
    // Personal-root re-LIST only. `replace_all` would clear the shared /
    // other-user entries NAMESPACE discovery installed at open, taking their
    // owner tags and MYRIGHTS with them, so every container mutation would
    // blank the shared half of `containers_list` until the next reopen.
    account.folders.replace_personal(folders);
    Ok(())
}

#[cfg(test)]
mod tests;
