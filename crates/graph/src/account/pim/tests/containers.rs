use crate::account::foreign::encode_foreign;
use crate::account::pim::containers::{
    container_from_folder, content_class_from_folder_class, public_folder_containers,
    role_from_well_known_name, shared_containers,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};
use crate::types::GraphMailFolder;
use bifrost_types::{
    ContainerContentClass, ContainerId, ContainerNamespace, CursorScope, FolderRole, ObjectType,
    PrincipalId,
};
use serde_json::json;
use std::collections::HashMap;

fn mail_folder(id: &str, parent: Option<&str>) -> GraphMailFolder {
    serde_json::from_value(json!({
        "id": id,
        "displayName": "Reports",
        "parentFolderId": parent,
    }))
    .expect("mail folder deserializes")
}

#[test]
fn shared_mailbox_folder_projects_as_shared_namespaced_container() {
    let container = container_from_folder(
        mail_folder("AAMkChild", Some("AAMkParent")),
        &HashMap::new(),
        Some("shared@contoso.com"),
    );
    assert_eq!(container.namespace, ContainerNamespace::Shared);
    assert_eq!(
        container.owner,
        Some(PrincipalId("shared@contoso.com".to_string()))
    );
    // `native_id` is the foreign-encoded form; `owner_local_id` is the
    // bare Graph folder id, never the encoded one.
    assert_eq!(
        container.native_id(),
        encode_foreign("shared@contoso.com", "AAMkChild").0
    );
    assert_eq!(container.owner_local_id.as_deref(), Some("AAMkChild"));
    assert_ne!(
        container.owner_local_id.as_deref(),
        Some(container.native_id())
    );
    // The parent is encoded in the same namespace.
    assert_eq!(
        container.parent,
        Some(ContainerId(
            encode_foreign("shared@contoso.com", "AAMkParent").0
        ))
    );
    // Graph REST mail folders expose no ACL; only public folders do.
    assert!(container.rights.is_none());
    assert!(container.content_class.is_none());
    // An addressable routing key IS the owner email.
    assert_eq!(container.owner_email.as_deref(), Some("shared@contoso.com"));

    // An object-id-shaped routing key carries no email.
    let opaque = container_from_folder(
        mail_folder("AAMkChild", Some("AAMkParent")),
        &HashMap::new(),
        Some("48d31887-5fad-4d73-a9f5-3c356e68a038"),
    );
    assert_eq!(opaque.namespace, ContainerNamespace::Shared);
    assert!(opaque.owner_email.is_none());

    // A primary folder stays personal and unqualified.
    let primary = container_from_folder(
        mail_folder("AAMkChild", Some("AAMkParent")),
        &HashMap::new(),
        None,
    );
    assert_eq!(primary.namespace, ContainerNamespace::Personal);
    assert!(primary.owner.is_none());
    assert!(primary.owner_local_id.is_none());
    assert!(primary.owner_email.is_none());
    assert_eq!(primary.native_id(), "AAMkChild");
}

/// A shared mailbox's well-known folder roles are resolved on its own client,
/// so a non-English shared Inbox carries `FolderRole::Inbox` (nc-4). The
/// primary is armed with an EMPTY script, so a lookup that fell back to `/me`
/// hits the seam's exhaustion panic; the shared script answers the listing,
/// then the six well-known lookups, of which only `inbox` resolves. Ablation:
/// with the primary's empty role map passed instead, "Posteingang" carries no
/// role and the six lookups never reach the wire.
#[tokio::test]
async fn shared_mailbox_roles_are_resolved_on_the_shared_client() {
    use reqwest::StatusCode;

    let primary = GraphClient::new("token");
    primary.script_rest([]);
    let shared = GraphClient::new("token");
    shared.script_rest([
        ScriptedRestResponse::json(
            StatusCode::OK,
            json!({ "value": [
                { "id": "AAMkInbox", "displayName": "Posteingang", "childFolderCount": 0 },
                { "id": "AAMkReports", "displayName": "Berichte", "childFolderCount": 0 },
            ]}),
        ),
        ScriptedRestResponse::json(StatusCode::OK, json!({ "id": "AAMkInbox" })),
        ScriptedRestResponse::empty(StatusCode::NOT_FOUND),
        ScriptedRestResponse::empty(StatusCode::NOT_FOUND),
        ScriptedRestResponse::empty(StatusCode::NOT_FOUND),
        ScriptedRestResponse::empty(StatusCode::NOT_FOUND),
        ScriptedRestResponse::empty(StatusCode::NOT_FOUND),
    ]);
    let mut shared_clients = HashMap::new();
    shared_clients.insert(
        "shared@contoso.com".to_string(),
        shared.for_shared_mailbox("shared@contoso.com"),
    );
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary,
        PushMode::GraphSubscriptions,
        shared_clients,
    );

    let (containers, skipped) = shared_containers(&account).await;
    assert!(skipped.is_empty(), "{skipped:?}");
    let inbox = containers
        .iter()
        .find(|container| container.owner_local_id.as_deref() == Some("AAMkInbox"))
        .expect("the shared inbox is projected");
    assert_eq!(
        inbox.role,
        Some(FolderRole::Inbox),
        "a shared Inbox is routable by role, whatever its display name"
    );
    let reports = containers
        .iter()
        .find(|container| container.owner_local_id.as_deref() == Some("AAMkReports"))
        .expect("the other folder is projected");
    assert_eq!(reports.role, None);

    let lookups: Vec<String> = shared
        .take_rest_requests()
        .into_iter()
        .map(|request| request.url)
        .filter(|url| url.contains("$select=id") && !url.contains("displayName"))
        .collect();
    assert_eq!(
        lookups.len(),
        6,
        "one lookup per well-known folder: {lookups:?}"
    );
    assert!(
        lookups
            .iter()
            .all(|url| url.contains("/users/shared%40contoso.com/mailFolders/")),
        "every lookup runs on the shared mailbox's own client: {lookups:?}"
    );
}

/// A shared container's `native_id` must be byte-identical to the
/// `CursorScope::FolderType` string `discover_cursor_scopes` emits for
/// the same folder - that identity is the join key between a container
/// and its sync scope.
#[test]
fn shared_container_native_id_matches_discovered_cursor_scope() {
    let container = container_from_folder(
        mail_folder("AAMkChild", None),
        &HashMap::new(),
        Some("shared@contoso.com"),
    );
    // The exact expression `discover_cursor_scopes_inner` uses for a
    // shared mailbox's folder.
    let scope = CursorScope::FolderType {
        folder: encode_foreign("shared@contoso.com", "AAMkChild"),
        ty: ObjectType::Email,
    };
    assert_eq!(
        scope,
        CursorScope::FolderType {
            folder: bifrost_types::FolderId(container.native_id().to_string()),
            ty: ObjectType::Email,
        },
    );
}

#[tokio::test]
async fn public_folders_project_with_content_class_and_rights() {
    let account = GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
    let routing = crate::account::cursor::PublicFolderRouting {
        anchor_mailbox: "content@contoso.com".to_string(),
        public_folder_mailbox: Some("pf@contoso.com".to_string()),
    };
    account
        .seed_public_folder_meta_for_tests(
            bifrost_types::FolderId("AAMkPF=".to_string()),
            routing.clone(),
            crate::account::public_folder::PublicFolderMeta {
                display_name: "Company Announcements".to_string(),
                folder_class: Some("IPF.Note".to_string()),
                parent: None,
                effective_rights: crate::ews::EwsEffectiveRights {
                    create_associated: false,
                    create_contents: false,
                    create_hierarchy: false,
                    delete: false,
                    modify: false,
                    read: true,
                },
            },
        )
        .await;
    account
        .seed_public_folder_meta_for_tests(
            bifrost_types::FolderId("AAMkCal=".to_string()),
            routing,
            crate::account::public_folder::PublicFolderMeta {
                display_name: "Team Calendar".to_string(),
                folder_class: Some("IPF.Appointment".to_string()),
                parent: Some(bifrost_types::FolderId("AAMkPF=".to_string())),
                effective_rights: crate::ews::EwsEffectiveRights {
                    create_associated: true,
                    create_contents: true,
                    create_hierarchy: true,
                    delete: true,
                    modify: true,
                    read: true,
                },
            },
        )
        .await;

    let containers = public_folder_containers(&account).await;
    assert_eq!(containers.len(), 2);
    let calendar = containers
        .iter()
        .find(|c| c.native_id() == "AAMkCal=")
        .expect("calendar public folder");
    assert_eq!(calendar.namespace, ContainerNamespace::Public);
    // A public folder has no owning principal.
    assert!(calendar.owner.is_none());
    assert!(calendar.owner_local_id.is_none());
    assert_eq!(calendar.name, "Team Calendar");
    assert_eq!(calendar.parent, Some(ContainerId("AAMkPF=".to_string())));
    assert_eq!(
        calendar.content_class,
        Some(ContainerContentClass::Calendar)
    );

    // A read-only public folder is distinguishable from a writable one.
    let mail = containers
        .iter()
        .find(|c| c.native_id() == "AAMkPF=")
        .expect("mail public folder");
    assert_eq!(mail.content_class, Some(ContainerContentClass::Mail));
    let read_only = mail.rights.as_ref().expect("rights projected");
    assert_eq!(read_only.may_read_items, Some(true));
    assert_eq!(read_only.may_add_items, Some(false));
    assert_eq!(read_only.may_remove_items, Some(false));
    assert_eq!(read_only.may_set_keywords, Some(false));
    // EWS folder rights say nothing about submission.
    assert_eq!(read_only.may_submit, None);
    let writable = calendar.rights.as_ref().expect("rights projected");
    assert_eq!(writable.may_add_items, Some(true));
    assert_eq!(writable.may_create_child, Some(true));
    assert_eq!(writable.may_delete, Some(true));
}

#[test]
fn folder_class_maps_onto_content_class() {
    assert_eq!(
        content_class_from_folder_class(Some("IPF.Note")),
        Some(ContainerContentClass::Mail)
    );
    // A subtype must not fall through to Other.
    assert_eq!(
        content_class_from_folder_class(Some("IPF.Note.Microsoft.Oof.Log")),
        Some(ContainerContentClass::Mail)
    );
    assert_eq!(
        content_class_from_folder_class(Some("IPF.Appointment")),
        Some(ContainerContentClass::Calendar)
    );
    assert_eq!(
        content_class_from_folder_class(Some("IPF.Contact")),
        Some(ContainerContentClass::Contacts)
    );
    assert_eq!(
        content_class_from_folder_class(Some("IPF.Task")),
        Some(ContainerContentClass::Other)
    );
    // Unreported is distinct from "typed as something we don't model".
    assert_eq!(content_class_from_folder_class(None), None);
}

#[test]
fn maps_well_known_folder_roles() {
    assert_eq!(role_from_well_known_name("inbox"), Some(FolderRole::Inbox));
    assert_eq!(
        role_from_well_known_name("sentItems"),
        Some(FolderRole::Sent)
    );
    assert_eq!(
        role_from_well_known_name("deletedItems"),
        Some(FolderRole::Trash)
    );
}
