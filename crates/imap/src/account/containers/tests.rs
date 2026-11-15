use super::*;
use crate::account::folder_registry::{FolderRegistry, SharedFolderEntry};
use crate::types::{MailboxAttribute, MailboxInfo};
use bifrost_types::{Application, FolderRole};

fn registry_with_shared(rights: Option<&str>) -> FolderRegistry {
    let personal = MailboxInfo {
        name: MailboxName::new("INBOX").expect("valid mailbox"),
        delimiter: Some('/'),
        ..Default::default()
    };
    let shared = MailboxInfo {
        name: MailboxName::new("Shared/alice/Reports").expect("valid mailbox"),
        delimiter: Some('/'),
        ..Default::default()
    };
    FolderRegistry::from_lists(
        vec![personal],
        vec![SharedFolderEntry {
            info: shared,
            owner: bifrost_types::PrincipalId("alice".to_owned()),
            rights: rights.map(MailboxRights::parse),
            namespace_prefix: "Shared/".to_owned(),
        }],
    )
}

#[test]
fn container_projection_namespaces_shared_folder_and_leaves_personal_alone() {
    let registry = registry_with_shared(Some("lr"));

    let shared = registry
        .get(&MailboxName::new("Shared/alice/Reports").expect("valid mailbox"))
        .expect("shared entry");
    let container = container_from_folder_entry(&shared, "Reports".to_owned());
    assert_eq!(container.namespace, ContainerNamespace::Shared);
    assert_eq!(
        container.owner,
        Some(bifrost_types::PrincipalId("alice".to_owned()))
    );
    // IMAP's owner-local id is the full path: it is already the native
    // id in the owner's namespace, and it matches the cursor-scope
    // string byte for byte.
    assert_eq!(
        container.owner_local_id.as_deref(),
        Some("Shared/alice/Reports")
    );
    assert_eq!(container.native_id(), "Shared/alice/Reports");
    // No provider types IMAP folders, so `content_class` stays None.
    assert!(container.content_class.is_none());

    let personal = registry
        .get(&MailboxName::new("INBOX").expect("valid mailbox"))
        .expect("personal entry");
    let container = container_from_folder_entry(&personal, "INBOX".to_owned());
    assert_eq!(container.namespace, ContainerNamespace::Personal);
    assert!(container.owner.is_none());
    assert!(container.owner_local_id.is_none());
    // A personal folder was never MYRIGHTS-probed, so rights are
    // unreported rather than "no rights".
    assert!(container.rights.is_none());
}

// The rights projection has to survive the case where the personal
// `LIST "" "*"` ALSO returned the shared path (RFC 2342 permits it and
// several servers do it). Before the precedence fix the overlapping entry
// stayed personal, so `Container::rights` came back `None` for a folder
// whose MYRIGHTS had been parsed and then discarded.
#[test]
fn shared_container_keeps_its_rights_when_the_personal_list_overlaps() {
    let path = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
    let registry = FolderRegistry::from_lists(
        vec![MailboxInfo {
            name: path.clone(),
            delimiter: Some('/'),
            ..Default::default()
        }],
        vec![SharedFolderEntry {
            info: MailboxInfo {
                name: path.clone(),
                delimiter: Some('/'),
                ..Default::default()
            },
            owner: bifrost_types::PrincipalId("alice".to_owned()),
            rights: Some(MailboxRights::parse("lr")),
            namespace_prefix: "Shared/".to_owned(),
        }],
    );

    let entry = registry.get(&path).expect("entry present");
    let container = container_from_folder_entry(&entry, "Reports".to_owned());
    assert_eq!(container.namespace, ContainerNamespace::Shared);
    let rights = container.rights.expect("MYRIGHTS reaches the container");
    // `lr` is a read-only share: readable, but no insert / delete / flag.
    assert_eq!(rights.may_read_items, Some(true));
    assert_eq!(rights.may_add_items, Some(false));
    assert_eq!(rights.may_remove_items, Some(false));
}

/// The container's `native_id` must be byte-identical to the
/// `CursorScope::Folder` string discovery emits for the same folder -
/// that identity is what lets the consumer join a container to its
/// sync scope.
#[test]
fn container_native_id_matches_discovered_cursor_scope() {
    let registry = registry_with_shared(Some("lr"));
    for entry in registry.entries() {
        let scope = super::super::folder_scope(&entry.name);
        let container = container_from_folder_entry(&entry, "n/a".to_owned());
        assert_eq!(
            scope,
            bifrost_types::CursorScope::Folder(bifrost_types::FolderId(
                container.native_id().to_string()
            )),
            "container native_id must equal the emitted cursor scope string",
        );
    }
}

#[test]
fn myrights_projection_distinguishes_read_only_from_writable() {
    // Read-only share: lookup + read only.
    let read_only = rights_from_myrights(&MailboxRights::parse("lr"));
    assert_eq!(read_only.may_read_items, Some(true));
    assert_eq!(read_only.may_add_items, Some(false));
    assert_eq!(read_only.may_remove_items, Some(false));
    assert_eq!(read_only.may_set_seen, Some(false));
    assert_eq!(read_only.may_set_keywords, Some(false));
    assert_eq!(read_only.may_create_child, Some(false));
    assert_eq!(read_only.may_rename, Some(false));
    assert_eq!(read_only.may_delete, Some(false));
    assert_eq!(read_only.may_submit, Some(false));

    // Full share.
    let full = rights_from_myrights(&MailboxRights::parse("lrswipkxtea"));
    assert_eq!(full.may_read_items, Some(true));
    assert_eq!(full.may_add_items, Some(true));
    assert_eq!(full.may_remove_items, Some(true));
    assert_eq!(full.may_set_seen, Some(true));
    assert_eq!(full.may_set_keywords, Some(true));
    assert_eq!(full.may_create_child, Some(true));
    assert_eq!(full.may_rename, Some(true));
    assert_eq!(full.may_delete, Some(true));
    assert_eq!(full.may_submit, Some(true));

    // Virtual response rights do not grant the modern member rights.
    let legacy = rights_from_myrights(&MailboxRights::parse("lrcd"));
    assert_eq!(legacy.may_create_child, Some(false));
    assert_eq!(legacy.may_delete, Some(false));
}

#[test]
fn folder_role_prefers_special_use_over_the_folder_name() {
    // A localized mailbox is only recognizable by its SPECIAL-USE
    // attribute, and a mislabelled name must not beat the attribute.
    assert_eq!(
        folder_role(&[MailboxAttribute::Sent], "Gesendete Objekte", None),
        Some(FolderRole::Sent)
    );
    assert_eq!(
        folder_role(&[MailboxAttribute::Junk], "Archive", None),
        Some(FolderRole::Spam),
        "the attribute wins over the name",
    );
    assert_eq!(
        folder_role(
            &[MailboxAttribute::from_imap_str("\\INBOX")],
            "Posteingang",
            None,
        ),
        None,
        "an Inbox attribute cannot make another name the protocol INBOX",
    );
}

#[test]
fn folder_role_falls_back_to_the_slash_delimited_leaf_name() {
    assert_eq!(
        folder_role(&[], "INBOX", Some('/')),
        Some(FolderRole::Inbox)
    );
    assert_eq!(
        folder_role(&[], "inbox", Some('/')),
        Some(FolderRole::Inbox)
    );
    assert_eq!(
        folder_role(&[], "[Gmail]/Sent Mail", Some('/')),
        Some(FolderRole::Sent)
    );
    assert_eq!(
        folder_role(&[], "Deleted Messages", Some('/')),
        Some(FolderRole::Trash)
    );
    assert_eq!(folder_role(&[], "Projects/Q3", Some('/')), None);
    assert_eq!(
        folder_role(&[MailboxAttribute::HasChildren], "Whatever", Some('/')),
        None,
        "a non-special-use attribute contributes no role",
    );
}

#[test]
fn folder_role_uses_the_servers_hierarchy_delimiter() {
    assert_eq!(
        folder_role(&[], "INBOX.Sent", Some('.')),
        Some(FolderRole::Sent)
    );
    assert_eq!(
        folder_role(&[], "INBOX.Trash", Some('.')),
        Some(FolderRole::Trash)
    );
    assert_eq!(
        folder_role(&[], "INBOX/Sent", Some('/')),
        Some(FolderRole::Sent)
    );
    assert_eq!(
        folder_role(&[], "INBOX/Sent", None),
        None,
        "a NIL delimiter is a flat namespace: no guessed slash hierarchy",
    );
    assert_eq!(
        folder_role(&[], "Sent", None),
        Some(FolderRole::Sent),
        "a flat name that is itself a role name still resolves",
    );
}

#[test]
fn parent_id_needs_the_servers_delimiter() {
    assert_eq!(
        parent_id(Some('/'), "Projects/Q3"),
        Some(ContainerId("Projects".to_owned()))
    );
    assert_eq!(
        parent_id(Some('.'), "INBOX.Projects.Q3"),
        Some(ContainerId("INBOX.Projects".to_owned())),
        "the delimiter is taken from LIST, not assumed",
    );
    assert_eq!(parent_id(Some('/'), "INBOX"), None, "a root has no parent");
    assert_eq!(
        parent_id(None, "Projects/Q3"),
        None,
        "a flat server has no hierarchy at all",
    );
}

#[test]
fn partial_commit_container_create_refresh_failure_is_protected() {
    let leaf = crate::account::error::test_support::unavailable_without_context();
    let error = container_refresh_error(
        leaf,
        StepEffect::Created,
        &MailboxName::new("new").unwrap(),
        None,
    );
    assert_eq!(
        error.partial_commit().unwrap().primary().application,
        Application::NotApplicable
    );
    assert!(
        matches!(error.recovery(),bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions.contains(&bifrost_types::ReconcileAction::ObserveOnly))
    );
}
