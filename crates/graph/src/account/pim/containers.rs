//! Container listing and CRUD across the primary, shared and public
//! namespaces, well-known folder roles, and the trash resolution
//! (with its per-mailbox cache) that thread deletes route through.

use crate::account::GraphAccount;
use crate::account::GraphClient;
use crate::account::graph_error::{
    GraphErrorContext, into_account_error, unsupported_account_error,
};
use crate::client::Resource;
use crate::types::GraphMailFolder;
use bifrost_types::{
    AccountError, AccountOperation, Container, ContainerContentClass, ContainerId, ContainerKind,
    ContainerList, ContainerNamespace, ContainerRights, ErrorScope, FolderRole, MailboxId,
    ProtocolKind, Provenance, SkippedScope,
};
use serde_json::{Value, json};
use std::collections::HashMap;

use super::common::*;

pub(super) const DELETED_ITEMS: &str = "deletedItems";
pub(super) const MSG_FOLDER_ROOT: &str = "msgfolderroot";

pub(crate) async fn containers_list(account: GraphAccount) -> Result<ContainerList, AccountError> {
    let folders = account
        .client
        .list_mail_folders_recursive()
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::DiscoverMemberships),
            )
        })?;
    account.folder_tree.write().await.replace_mail_folders(
        folders
            .iter()
            .map(|folder| (folder.id.clone(), folder.parent_folder_id.clone())),
    );
    let roles = well_known_folder_roles(&account.client).await;
    let mut containers: Vec<Container> = folders
        .into_iter()
        .map(|folder| container_from_folder(folder, &roles, None))
        .collect();

    // Shared (delegate) mailboxes, then public folders. Both are additive:
    // a failure in either leg leaves the primary mailbox's containers intact.
    // A per-mailbox degradation rides the `ContainerList::skipped_scopes`
    // lane (with its classified error) and is logged for telemetry.
    let (shared, skipped_scopes) = shared_containers(&account).await;
    containers.extend(shared);
    for skip in &skipped_scopes {
        tracing::warn!(
            "[Graph] containers_list skipped {:?}: {}",
            skip.scope,
            skip.error.message_key()
        );
    }
    containers.extend(public_folder_containers(&account).await);
    Ok(ContainerList {
        containers,
        skipped_scopes,
    })
}

/// Enumerate each configured shared (delegate) mailbox's folders as
/// `Shared`-namespace containers.
///
/// Each container's `native_id` is the foreign-encoded
/// `encode_foreign(mailbox, folderId)` - byte-identical to the
/// `CursorScope::FolderType` string `discover_cursor_scopes` emits for the
/// same folder, which is what lets the consumer join a container to its sync
/// scope - while `owner_local_id` keeps the bare Graph folder id for requests
/// made against the owner's own mailbox.
///
/// A per-mailbox enumeration failure degrades to a `SkippedScope` (naming
/// the shared mailbox, carrying the classified error) plus the remaining
/// containers: one revoked share must not blank the whole sidebar, and it
/// must not vanish from it silently either.
///
/// Each shared mailbox's well-known folder roles are resolved on ITS OWN
/// client (nc-4, ruled 2026-09-07). A shared mailbox's `inbox` / `sentItems` /
/// `deletedItems` ids are its own, so the primary's role map cannot apply,
/// and without a per-mailbox lookup the role fell back to
/// `role_from_well_known_name`, which matches the well-known NAME against the
/// folder id - a match that only ever fires when the id happens to be the
/// well-known name itself. Shared mailboxes therefore carried no `FolderRole`
/// at all, and `FolderRole` is what the consumer routes destructive actions
/// on: with none, Delete does not know which folder is Trash, Send does not
/// know where to file the copy, Save-draft does not know Drafts. The cost is
/// six `$select=id` GETs per shared mailbox at `containers_list`, not per
/// operation, and that was judged worth correct routing.
pub(super) async fn shared_containers(
    account: &GraphAccount,
) -> (Vec<Container>, Vec<SkippedScope>) {
    let mut containers = Vec::new();
    let mut skipped = Vec::new();
    // Deterministic order so the projection is stable across calls.
    let mut mailboxes: Vec<&String> = account.shared_clients.keys().collect();
    mailboxes.sort();
    for mailbox in mailboxes {
        let client = &account.shared_clients[mailbox];
        match client.list_mail_folders_recursive().await {
            Ok(folders) => {
                let roles = well_known_folder_roles(client).await;
                containers.extend(
                    folders
                        .into_iter()
                        .map(|folder| container_from_folder(folder, &roles, Some(mailbox))),
                );
            }
            Err(error) => skipped.push(SkippedScope {
                scope: ErrorScope::Mailbox {
                    id: mailbox.clone().into(),
                },
                error: into_account_error(
                    error,
                    GraphErrorContext::graph(AccountOperation::ContainersList),
                ),
            }),
        }
    }
    (containers, skipped)
}

/// Project every discovered public folder as a `Public`-namespace container.
///
/// Reads the `routing_map` / `public_folder_meta` pair that
/// `discover_public_folder_scopes` seeds, so this is purely local - no EWS
/// round-trip. `SyncEngine::attach` drives scope discovery synchronously
/// before any `containers_list` call, which is what makes the maps populated
/// by the time this runs.
///
/// The full readable hierarchy projects here, including folders that are NOT
/// pinned for sync: the consumer has to see a folder before it can decide to
/// pin it.
pub(super) async fn public_folder_containers(account: &GraphAccount) -> Vec<Container> {
    // Snapshot the routing keys and release that guard before taking the
    // metadata one: the two maps are never held simultaneously.
    let mut native_ids: Vec<String> = {
        let routing = account.routing_map.read().await;
        routing.keys().map(|folder| folder.0.clone()).collect()
    };
    // Deterministic order so the projection is stable across calls.
    native_ids.sort();
    let meta = account.public_folder_meta.read().await;
    native_ids
        .into_iter()
        .map(|native| {
            let meta = meta.get(&bifrost_types::FolderId(native.clone()));
            let name = meta
                .map(|meta| meta.display_name.clone())
                .unwrap_or_else(|| native.clone());
            Container::new(
                ContainerId(native.clone()),
                ContainerKind::Folder,
                // A public folder plays no ratatoskr mailbox role: it is not
                // anyone's Inbox / Sent / Trash.
                None,
                Provenance {
                    provider: ProtocolKind::Graph,
                    kind: ContainerKind::Folder,
                    native: native.clone(),
                },
                name,
                meta.and_then(|meta| meta.parent.clone())
                    .map(|parent| ContainerId(parent.0)),
            )
            .with_namespace(ContainerNamespace::Public)
            // A public folder is owned by the organization, not by a
            // principal, so there is no owner mailbox to name and no
            // owner-local id space to translate into.
            .with_content_class(
                meta.and_then(|meta| content_class_from_folder_class(meta.folder_class.as_deref())),
            )
            .with_rights(meta.map(|meta| rights_from_effective_rights(&meta.effective_rights)))
        })
        .collect()
}

/// Map an EWS `FolderClass` onto the unified [`ContainerContentClass`].
///
/// A public folder is typed at the folder level and a mail client must not
/// present an `IPF.Appointment` folder as a mail folder. `None` when the
/// server reported no class at all (distinct from
/// `Some(ContainerContentClass::Other)`, which is "typed, but as something
/// this surface does not model").
pub(super) fn content_class_from_folder_class(
    folder_class: Option<&str>,
) -> Option<ContainerContentClass> {
    let class = folder_class?;
    // The wire form is `IPF.Note`, `IPF.Note.Something`, `IPF.Appointment`,
    // ...; match on the leading segment so a subtype does not fall to Other.
    let normalized = class.trim().to_ascii_lowercase();
    Some(if normalized.starts_with("ipf.note") {
        ContainerContentClass::Mail
    } else if normalized.starts_with("ipf.appointment") {
        ContainerContentClass::Calendar
    } else if normalized.starts_with("ipf.contact") {
        ContainerContentClass::Contacts
    } else {
        ContainerContentClass::Other
    })
}

/// Project EWS folder `EffectiveRights` onto the unified
/// [`ContainerRights`].
///
/// Every member the EWS shape speaks to is `Some(_)`: the server answered,
/// so an absent right is a definite "no". `may_submit` stays `None` - EWS
/// folder rights say nothing about submission (a public folder has no
/// submission address), and reporting `Some(false)` would claim knowledge the
/// wire never provided.
pub(super) fn rights_from_effective_rights(
    rights: &crate::ews::EwsEffectiveRights,
) -> ContainerRights {
    ContainerRights {
        may_read_items: Some(rights.read),
        may_add_items: Some(rights.create_contents),
        may_remove_items: Some(rights.delete),
        // EWS has no per-flag right; `Modify` is the whole item-mutation
        // gate, so both keyword members map to it.
        may_set_seen: Some(rights.modify),
        may_set_keywords: Some(rights.modify),
        may_create_child: Some(rights.create_hierarchy),
        may_rename: Some(rights.modify),
        may_delete: Some(rights.delete),
        may_submit: None,
    }
}

pub(crate) async fn container_create(
    account: GraphAccount,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    // Graph mail folders carry no container color (Graph categories
    // are message flags, not containers); accepted for trait parity
    // with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> Result<ContainerId, AccountError> {
    if kind != ContainerKind::Folder {
        return Err(unsupported_account_error(AccountOperation::ContainerCreate));
    }
    let prefix = account.client.api_path_prefix();
    let path = match parent {
        Some(parent) => format!(
            "{prefix}/mailFolders/{}/childFolders",
            bifrost_net::url::encode_path_component(&parent.0)
        ),
        None => format!("{prefix}/mailFolders"),
    };
    let folder: GraphMailFolder = account
        .client
        .post(Resource::Outlook, &path, &json!({ "displayName": name }))
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::ContainerCreate),
            )
        })?;
    Ok(ContainerId(folder.id))
}

pub(crate) async fn container_rename(
    account: GraphAccount,
    container: ContainerId,
    name: String,
    // Graph has no mail-folder recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/mailFolders/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_path_component(&container.0)
    );
    account
        .client
        .patch(Resource::Outlook, &path, &json!({ "displayName": name }))
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::ContainerRename),
            )
        })
}

pub(crate) async fn container_move(
    account: GraphAccount,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> Result<(), AccountError> {
    let destination_id = new_parent.map_or_else(|| MSG_FOLDER_ROOT.to_string(), |id| id.0);
    let path = format!(
        "{}/mailFolders/{}/move",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_path_component(&container.0)
    );
    let _: GraphMailFolder = account
        .client
        .post(
            Resource::Outlook,
            &path,
            &json!({ "destinationId": destination_id }),
        )
        .await
        .map_err(|e| {
            into_account_error(e, GraphErrorContext::graph(AccountOperation::ContainerMove))
        })?;
    Ok(())
}

pub(crate) async fn container_delete(
    account: GraphAccount,
    container: ContainerId,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/mailFolders/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_path_component(&container.0)
    );
    account
        .client
        .delete(Resource::Outlook, &path)
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::ContainerDelete),
            )
        })
}

/// Resolve the well-known folder ids of ONE mailbox. Takes the client
/// rather than the account: a shared mailbox's well-known folders are its
/// own, so the caller picks the client the lookup must run on.
pub(super) async fn well_known_folder_roles(client: &GraphClient) -> HashMap<String, FolderRole> {
    let mut roles = HashMap::new();
    for (name, role) in WELL_KNOWN_FOLDERS {
        let path = format!(
            "{}/mailFolders/{}?$select=id",
            client.api_path_prefix(),
            bifrost_net::url::encode_path_component(name)
        );
        if let Ok(value) = client.get_json::<Value>(Resource::Outlook, &path).await
            && let Some(id) = value.get("id").and_then(Value::as_str)
        {
            roles.insert(id.to_string(), *role);
        }
    }
    roles
}

pub(super) const WELL_KNOWN_FOLDERS: &[(&str, FolderRole)] = &[
    ("inbox", FolderRole::Inbox),
    ("sentItems", FolderRole::Sent),
    ("drafts", FolderRole::Drafts),
    (DELETED_ITEMS, FolderRole::Trash),
    ("junkEmail", FolderRole::Spam),
    ("archive", FolderRole::Archive),
];

/// Project one Graph mail folder onto a `Container`.
///
/// `owner` is `Some(mailbox)` for a shared (delegate) mailbox's folder,
/// which namespaces the ids: `native_id` becomes the foreign-encoded form
/// (byte-identical to the `CursorScope::FolderType` string discovery emits
/// for the same folder) while `owner_local_id` keeps the bare Graph folder
/// id. The parent is encoded in the same namespace, so a shared child never
/// points at a same-id primary folder.
pub(super) fn container_from_folder(
    folder: GraphMailFolder,
    roles: &HashMap<String, FolderRole>,
    owner: Option<&str>,
) -> Container {
    let role = roles
        .get(&folder.id)
        .copied()
        .or_else(|| role_from_well_known_name(&folder.id));
    let native = match owner {
        Some(mailbox) => crate::account::foreign::encode_foreign(mailbox, &folder.id).0,
        None => folder.id.clone(),
    };
    let parent = folder.parent_folder_id.map(|parent| {
        ContainerId(match owner {
            Some(mailbox) => crate::account::foreign::encode_foreign(mailbox, &parent).0,
            None => parent,
        })
    });
    Container::new(
        ContainerId(native.clone()),
        ContainerKind::Folder,
        role,
        Provenance {
            provider: ProtocolKind::Graph,
            kind: ContainerKind::Folder,
            native: native.clone(),
        },
        folder.display_name.unwrap_or_else(|| native.clone()),
        parent,
    )
    // Graph mail folders carry no container color (categories are message
    // flags, not containers), and Graph is folder-shaped (well-known folders
    // already map into `role`), so `style` and `system` keep their
    // `Container::new` defaults. Graph REST exposes no per-folder ACL or
    // subscription state on mail folders either; the EWS `EffectiveRights`
    // that DO exist are a public-folder-only surface.
    .with_namespace(match owner {
        Some(_) => ContainerNamespace::Shared,
        None => ContainerNamespace::Personal,
    })
    .with_owner(owner.map(|mailbox| MailboxId(mailbox.to_string())))
    // The `/users/{id}` routing key doubles as the owner email exactly when
    // it is addressable (a UPN/SMTP address the account already holds);
    // an object-id-shaped key carries no email and projects `None`.
    .with_owner_email(
        owner
            .filter(|mailbox| mailbox.contains('@'))
            .map(str::to_string),
    )
    .with_owner_local_id(owner.map(|_| folder.id))
}

pub(super) fn role_from_well_known_name(name: &str) -> Option<FolderRole> {
    WELL_KNOWN_FOLDERS
        .iter()
        .find_map(|(known, role)| name.eq_ignore_ascii_case(known).then_some(*role))
}

/// Resolve the Trash container of the mailbox `owner` names (the primary
/// mailbox for `None`), returned OWNER-QUALIFIED so it is byte-identical to
/// the id `containers_list` emits for the same folder and so
/// `move_messages`'s same-mailbox destination guard accepts it.
///
/// The well-known lookup itself must run on the owner's client: shared and
/// primary mailboxes do not share well-known folder ids, so the primary's
/// `deletedItems` id names nothing in a shared mailbox.
///
/// A failed lookup PROPAGATES rather than degrading to the well-known NAME
/// `deletedItems`. That literal is a valid move destination, so the move
/// half of `delete_thread` would still appear to work - but it is not the
/// concrete folder id `container_is_trash` compares against, so a thread
/// already sitting in Trash would be "moved" there a second time and report
/// success instead of being destroyed. Only a resolved id is cached, for
/// the same reason: one transient failure must not outlive itself.
pub(super) async fn trash_container_id(
    account: &GraphAccount,
    owner: Option<&str>,
    scope: ErrorScope,
) -> Result<ContainerId, AccountError> {
    let cache_key = owner.unwrap_or_default();
    if let Some(native) = account
        .trash_folder_ids
        .read()
        .await
        .get(cache_key)
        .cloned()
    {
        return Ok(ContainerId(crate::account::foreign::qualify_with_owner(
            owner, &native,
        )));
    }
    let client = account.client_for_owner(owner).map_err(|error| {
        into_account_error(
            error,
            GraphErrorContext::graph(AccountOperation::BulkMove).with_scope(scope.clone()),
        )
    })?;
    let path = format!(
        "{}/mailFolders/{DELETED_ITEMS}?$select=id",
        client.api_path_prefix()
    );
    let folder: Value = client
        .get_json(Resource::Outlook, &path)
        .await
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(AccountOperation::BulkMove).with_scope(scope.clone()),
            )
        })?;
    let native = folder
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            pim_protocol_error(
                AccountOperation::BulkMove,
                Some(scope),
                "Graph deletedItems lookup returned no folder id",
            )
        })?
        .to_string();
    account
        .trash_folder_ids
        .write()
        .await
        .insert(cache_key.to_string(), native.clone());
    Ok(ContainerId(crate::account::foreign::qualify_with_owner(
        owner, &native,
    )))
}

/// Does `current` already name the Trash of the mailbox `owner` names?
///
/// Both sides are owner-qualified ids. The `deletedItems` well-known-NAME
/// fallback is matched on the NATIVE half so a shared mailbox's
/// `"{owner}\u{1f}deletedItems"` is recognized, while the primary's bare
/// `"deletedItems"` is not accepted as a shared thread's Trash (destroying
/// on that mistake is unrecoverable, where a redundant move is not).
pub(super) fn container_is_trash(
    current: &ContainerId,
    trash: &ContainerId,
    owner: Option<&str>,
) -> bool {
    if current.0 == trash.0 {
        return true;
    }
    let folder = bifrost_types::FolderId(current.0.clone());
    crate::account::foreign::folder_owner(&folder).as_deref() == owner
        && crate::account::foreign::parse_folder(&folder)
            .native_id()
            .eq_ignore_ascii_case(DELETED_ITEMS)
}
