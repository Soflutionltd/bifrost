//! The PIM suite. Kept as one module across the `pim` split so every
//! test stays exactly as written.

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::json;

use crate::account::GraphAccount;
use crate::types::{BatchRequestItem, GraphMailFolder};
use base64::Engine;
use bifrost_types::{
    AccessErrorKind, AccountOperation, AttachmentInline, AttachmentSource, ContainerContentClass,
    ContainerId, ContainerNamespace, DraftPatch, ErrorScope, FolderId, FolderRole,
    HydrationProjection, Importance, MailboxId, MutationTarget, ObjectId, ProtocolErrorKind,
    SearchFilter, SearchRequest, SendAs, ThreadId,
};
use serde_json::Value;
use std::time::SystemTime;

use super::common::*;
use super::containers::*;
use super::drafts::*;
use super::hydrate::*;
use super::messages::*;
use super::search::*;
use super::send::*;
use super::threads::*;
use crate::account::PushMode;
use crate::account::foreign::{encode_foreign, encode_message_id, encode_public_item_id};
use crate::client::{GraphClient, ScriptedRestResponse};
use bifrost_types::{
    AccountErrorKind, CursorScope, ObjectType, RecoveryClass, RemediationAction, RequestErrorKind,
};

// The single-id hydration door must reach the EWS arm for a public
// item. Before this, `message_hydrate` fell through to
// `/me/messages/{native}` and the server answered
// `ErrorItemNotFound` naming the BARE item id, even though the stored
// id was folder-qualified all along.
#[test]
fn public_item_id_selects_the_ews_read_arm() {
    let folder = FolderId("AAMkPF=".to_string());
    let public = encode_public_item_id(&folder, "notice-1");
    assert_eq!(ews_read_folder(&public), Some(folder));
}

/// The single-id door builds the SAME class-conditional `GetItem` body the
/// batch door builds. Pinned separately because the two doors have diverged
/// before (the batch arm routed public ids to EWS while `message_hydrate`
/// still sent them to `/me/messages/{native}`), and a message-shaped request
/// for a contact is answered `ErrorInvalidPropertyRequest`.
#[tokio::test]
async fn nc2a_single_id_hydration_refuses_returned_contact_after_mail_cache_hit() {
    use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_account};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource, TokenSource};
    use std::sync::Arc;

    let response = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:Contact>
              <t:ItemId Id="notice-1" ChangeKey="CK1"/>
              <t:ItemClass>IPM.Contact</t:ItemClass>
            </t:Contact>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
    let script = ScriptedDispatch::new([Canned::Response {
        status: reqwest::StatusCode::OK,
        headers: reqwest::header::HeaderMap::new(),
        body: Bytes::from(response),
    }]);
    let token_source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
    let net = scripted_account(
        &script,
        NetConfig::default(),
        Vec::new(),
        Arc::clone(&token_source),
        RetryPolicy::disabled(),
    );
    let client =
        GraphClient::with_account_net(net, "https://graph.contoso.test/v1.0", token_source);
    let account = GraphAccount::new_for_tests(client, PushMode::EwsStreaming);
    let folder = FolderId("AAMkPF=".to_string());
    account
        .seed_public_folder_meta_for_tests(
            folder.clone(),
            crate::account::cursor::PublicFolderRouting {
                anchor_mailbox: "content@contoso.com".to_string(),
                public_folder_mailbox: Some("pf@contoso.com".to_string()),
            },
            crate::account::public_folder::PublicFolderMeta {
                // A MAIL folder, so only the per-item class recorded by the
                // inventory walk can make this request class-safe.
                display_name: "Notices".to_string(),
                folder_class: Some("IPF.Note".to_string()),
                parent: None,
                effective_rights: crate::ews::EwsEffectiveRights::default(),
            },
        )
        .await;
    let mut item = ews_item();
    item.item_class = "IPM.Note".to_string();
    account.record_public_item_classes(&folder, &[item]).await;

    let id = encode_public_item_id(&folder, "notice-1");
    let error = message_hydrate(account, id.clone(), HydrationProjection::Full)
        .await
        .expect_err("non-mail is refused");
    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Unsupported(AccountOperation::HydrateMessage)
    ));

    let requests = script.requests();
    assert_eq!(requests.len(), 1);
    let body = String::from_utf8(
        requests[0]
            .body
            .clone()
            .expect("GetItem carries a body")
            .to_vec(),
    )
    .expect("utf8 SOAP");
    assert!(
        body.contains("message:"),
        "cached mail shape was used: {body}"
    );
}

#[test]
fn primary_and_foreign_ids_stay_on_the_rest_read_arm() {
    assert_eq!(ews_read_folder(&ObjectId("notice-1".to_string())), None);
    let foreign = encode_message_id(
        &CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        },
        "AAMkmsg",
    );
    assert_eq!(ews_read_folder(&foreign), None);
}

/// Graph's raw-MIME import is the one call whose body is not JSON, so
/// it used to build its own request straight on `AccountNet` and sat
/// outside the seam entirely. It now shares the single wire funnel:
/// the base64 octets go out verbatim under `text/plain`, and the draft
/// the import returns is what gets sent.
#[tokio::test]
async fn raw_mime_import_posts_the_base64_octets_as_text_plain() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            json!({"id": "draft-1", "changeKey": "ck"}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::ACCEPTED),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let raw = bytes::Bytes::from_static(b"Subject: hi\r\n\r\nbody");

    let sent = send_raw_message(account, raw.clone(), None)
        .await
        .expect("MIME import returns the draft id");
    assert_eq!(sent.0, "draft-1");

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].content_type, "text/plain");
    assert_eq!(requests[0].body, None);
    assert_eq!(
        requests[0].raw_body.as_deref(),
        Some(
            base64::engine::general_purpose::STANDARD
                .encode(&raw)
                .as_bytes()
        )
    );
    assert!(requests[0].url.ends_with("/me/messages"));
    assert!(requests[1].url.ends_with("/me/messages/draft-1/send"));
    assert!(requests[1].content_type.is_empty());
    assert_eq!(requests[1].body, None);
    assert_eq!(requests[1].raw_body, None);
}

/// Clearing an extended property rides the destroy flag for its 404
/// tolerance, but it is not a message destroy: the message's cached etag
/// must survive, while a refusal still names the message in the record.
#[tokio::test]
async fn an_extended_property_clear_keeps_the_message_etag() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({
            "responses": [
                {"id":"0", "status":204},
                {"id":"1", "status":403, "body":{"error":{"code":"ErrorAccessDenied"}}}
            ]
        }),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let cleared = ObjectId("cleared".to_string());
    let refused = ObjectId("refused".to_string());
    account
        .etag_index
        .write()
        .await
        .insert(cleared.0.clone(), "tag".to_string());
    let error = delete_extended_property(
        &account,
        &[cleared.clone(), refused.clone()],
        "String {00020329-0000-0000-C000-000000000046} Name x",
        AccountOperation::SetExtendedProperty,
    )
    .await
    .unwrap_err();
    assert_eq!(
        account.etag_index.write().await.get(&cleared.0),
        Some("tag".to_string())
    );
    let record = error.partial_commit().expect("one clear committed");
    assert_eq!(
        record.primary().targets.refs[0].scope,
        ErrorScope::Message { id: refused }
    );
    assert_eq!(
        record.steps()[0].targets().refs[0].scope,
        ErrorScope::Message { id: cleared }
    );
}

#[tokio::test]
async fn write_batch_drains_past_a_failure_to_evict_a_later_destroy_etag() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({
            "responses": [
                {"id":"0", "status":400, "body":{"error":{"code":"BadRequest"}}},
                {"id":"1", "status":204}
            ]
        }),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let failed = ObjectId("failed".to_string());
    let destroyed = ObjectId("destroyed".to_string());
    account
        .etag_index
        .write()
        .await
        .insert(destroyed.0.clone(), "stale".to_string());
    let requests = vec![
        BatchRequestItem {
            id: "0".to_string(),
            method: "PATCH".to_string(),
            url: "/me/messages/failed".to_string(),
            body: None,
            headers: None,
        },
        BatchRequestItem {
            id: "1".to_string(),
            method: "DELETE".to_string(),
            url: "/me/messages/destroyed".to_string(),
            body: None,
            headers: None,
        },
    ];
    let progress = submit_write_batch_with_targets(
        &account,
        requests,
        &[failed, destroyed.clone()],
        true,
        AccountOperation::BulkDestroy,
    )
    .await
    .unwrap();
    assert_eq!(progress.failed.len(), 1);
    assert_eq!(progress.committed.len(), 1);
    assert_eq!(account.etag_index.write().await.get(&destroyed.0), None);
    assert_eq!(client.take_rest_requests().len(), 1);
}

fn ews_item() -> crate::ews::EwsItem {
    crate::ews::EwsItem {
        wire_kind: "Message".to_string(),
        item_id: "notice-1".to_string(),
        change_key: Some("CK1".to_string()),
        subject: Some("Notice".to_string()),
        sender_email: Some("poster@contoso.com".to_string()),
        sender_name: Some("Poster".to_string()),
        received_at: Some("2026-01-02T03:04:05Z".to_string()),
        body_preview: Some("preview text".to_string()),
        body_html: Some("<p>preview text</p>".to_string()),
        is_read: true,
        flag_status: None,
        categories: Vec::new(),
        item_class: "IPM.Note".to_string(),
        to_recipients: vec![crate::ews::EwsRecipient {
            email: "reader@contoso.com".to_string(),
            name: Some("Reader".to_string()),
        }],
        cc_recipients: Vec::new(),
        attachments: Vec::new(),
    }
}

// The EWS projection keeps the folder-qualified id the consumer
// stored, so a hydrated public item still round-trips back through
// the EWS arm on the next read.
#[test]
fn ews_item_projects_to_message_keyed_by_the_qualified_id() {
    let folder = FolderId("AAMkPF=".to_string());
    let id = encode_public_item_id(&folder, "notice-1");
    let message =
        message_from_ews_item(id.clone(), &ews_item(), &folder, HydrationProjection::Full);
    assert_eq!(message.id, id);
    assert_eq!(ews_read_folder(&message.id), Some(folder.clone()));
    assert_eq!(message.subject.as_deref(), Some("Notice"));
    assert_eq!(message.from.len(), 1);
    assert_eq!(message.from[0].address, "poster@contoso.com");
    assert_eq!(message.to[0].address, "reader@contoso.com");
    assert_eq!(message.body_html.as_deref(), Some("<p>preview text</p>"));
    assert_eq!(message.body_text.as_deref(), Some("preview text"));
    assert_eq!(message.containers, vec![ContainerId(folder.0)]);
    assert!(message.flags.contains("\\seen"));
    assert!(message.date.is_some());
}

#[test]
fn ews_headers_projection_drops_the_body() {
    let folder = FolderId("AAMkPF=".to_string());
    let message = message_from_ews_item(
        encode_public_item_id(&folder, "notice-1"),
        &ews_item(),
        &folder,
        HydrationProjection::Headers,
    );
    assert!(message.body_text.is_none());
    assert!(message.body_html.is_none());
}

#[test]
fn ews_full_hydration_reports_attachment_metadata_with_blob_source() {
    let folder = FolderId("AAMkPF=".to_string());
    let id = encode_public_item_id(&folder, "notice-1");
    let mut item = ews_item();
    item.attachments.push(crate::ews::EwsAttachment {
        attachment_id: "att-1".to_string(),
        name: Some("notice.pdf".to_string()),
        content_type: Some("application/pdf".to_string()),
        size: Some(2048),
        is_inline: true,
        is_item: false,
    });

    let message = message_from_ews_item(id, &item, &folder, HydrationProjection::Full);
    assert_eq!(message.attachments.len(), 1);
    let attachment = &message.attachments[0];
    assert_eq!(attachment.filename.as_deref(), Some("notice.pdf"));
    assert_eq!(attachment.content_type.as_deref(), Some("application/pdf"));
    assert_eq!(attachment.content_id, None);
    assert!(attachment.inline);
    assert_eq!(attachment.size, Some(2048));
    assert!(matches!(&attachment.source, AttachmentSource::Blob(_)));
    assert!(!attachment.truncated);
}

#[test]
fn graph_full_hydration_reports_attachment_metadata_with_blob_source() {
    let value = json!({
        "id": "message-1",
        "attachments": [{
            "id": "attachment-1",
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "logo.png",
            "contentType": "image/png",
            "contentId": "logo@contoso.com",
            "isInline": true,
            "size": 512
        }]
    });

    for projection in [
        HydrationProjection::Full,
        HydrationProjection::FullWithBlobs,
    ] {
        let message = message_from_value(&value, projection, None).expect("message projects");
        assert_eq!(message.attachments.len(), 1);
        let attachment = &message.attachments[0];
        assert_eq!(attachment.filename.as_deref(), Some("logo.png"));
        assert_eq!(attachment.content_type.as_deref(), Some("image/png"));
        assert_eq!(attachment.content_id.as_deref(), Some("logo@contoso.com"));
        assert!(attachment.inline);
        assert_eq!(attachment.size, Some(512));
        assert!(matches!(&attachment.source, AttachmentSource::Blob(_)));
        assert!(!attachment.truncated);
    }

    let headers =
        message_from_value(&value, HydrationProjection::Headers, None).expect("headers project");
    assert!(headers.attachments.is_empty());
}

fn shared_account() -> GraphAccount {
    GraphAccount::new_for_tests_with_shared(
        GraphClient::new("token"),
        PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    )
}

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
        Some(MailboxId("shared@contoso.com".to_string()))
    );
    // `native_id` is the foreign-encoded form; `owner_local_id` is the
    // bare Graph folder id, never the encoded one.
    assert_eq!(
        container.native_id,
        encode_foreign("shared@contoso.com", "AAMkChild").0
    );
    assert_eq!(container.owner_local_id.as_deref(), Some("AAMkChild"));
    assert_ne!(
        container.owner_local_id.as_deref(),
        Some(container.native_id.as_str())
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
    assert_eq!(primary.native_id, "AAMkChild");
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
            folder: bifrost_types::FolderId(container.native_id.clone()),
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
        .find(|c| c.native_id == "AAMkCal=")
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
        .find(|c| c.native_id == "AAMkPF=")
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

fn foreign_message_id(mailbox: &str, folder: &str, native: &str) -> ObjectId {
    let scope = CursorScope::FolderType {
        folder: encode_foreign(mailbox, folder),
        ty: ObjectType::Email,
    };
    encode_message_id(&scope, native)
}

#[tokio::test]
async fn foreign_thread_members_are_queried_and_requalified_on_the_owner_client() {
    let primary = GraphClient::new("token");
    primary.script_rest(std::iter::empty::<ScriptedRestResponse>());
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{
                "id": "AAMkmsg",
                "conversationId": "conversation-1",
                "parentFolderId": "AAMkfolder",
            }] }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{ "id": "AAMkmsg" }] }),
        ),
    ]);
    let mailbox = "shared@contoso.com";
    let shared = shared_root.for_shared_mailbox(mailbox);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared)]),
    );
    let thread = ThreadId(format!("{mailbox}\u{1f}conversation-1"));

    let hydrated = thread_hydrate(account.clone(), thread.clone())
        .await
        .expect("shared thread hydrates");
    assert_eq!(hydrated.messages.len(), 1);
    assert_eq!(hydrated.messages[0].id.0, format!("{mailbox}\u{1f}AAMkmsg"));
    assert_eq!(hydrated.messages[0].thread_id, Some(thread.clone()));
    // `parentFolderId` comes back bare from `/users/{owner}`; it has to
    // be re-qualified or it neither joins the `Shared`-namespace
    // containers `containers_list` emits nor stays distinguishable from
    // a primary folder that happens to share the id.
    assert_eq!(
        hydrated.messages[0].containers,
        vec![ContainerId(format!("{mailbox}\u{1f}AAMkfolder"))]
    );

    let ids = resolve_target_ids(
        &account,
        MutationTarget::Thread(thread),
        AccountOperation::SetKeyword,
    )
    .await
    .expect("shared thread members resolve");

    assert_eq!(ids, vec![ObjectId(format!("{mailbox}\u{1f}AAMkmsg"))]);
    assert!(primary.take_rest_requests().is_empty());
    let requests = shared_root.take_rest_requests();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(
            request
                .url
                .contains("/users/shared%40contoso.com/messages?")
        );
        assert!(
            request
                .url
                .contains("conversationId%20eq%20%27conversation-1%27")
        );
    }
}

#[tokio::test]
async fn search_walks_shared_mailboxes_and_qualifies_their_ids() {
    let primary = GraphClient::new("token");
    primary.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "primary-message", "conversationId": "primary-thread" }],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/messages?$skiptoken=primary"
            }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{ "id": "primary-message-2", "conversationId": "primary-thread-2" }] }),
        ),
    ]);
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "shared-message", "conversationId": "shared-thread" }] }),
    )]);
    let mailbox = "shared@contoso.com";
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared_root.for_shared_mailbox(mailbox))]),
    );

    let first = search_message_rows(&account, SearchRequest::default())
        .await
        .expect("primary page succeeds");
    assert_eq!(first.items[0].id, ObjectId("primary-message".to_string()));
    assert_eq!(
        first.items[0].thread_id,
        Some(ThreadId("primary-thread".to_string()))
    );
    let mut continuation = SearchRequest::default();
    continuation.page_cursor = first.next_cursor;
    let primary_continuation = search_message_rows(&account, continuation)
        .await
        .expect("primary continuation succeeds");
    assert_eq!(
        primary_continuation.items[0].id,
        ObjectId("primary-message-2".to_string())
    );
    let mut continuation = SearchRequest::default();
    continuation.page_cursor = primary_continuation.next_cursor;
    let second = search_message_rows(&account, continuation)
        .await
        .expect("shared page succeeds");
    assert_eq!(
        second.items[0].id,
        ObjectId(format!("{mailbox}\u{1f}shared-message"))
    );
    assert_eq!(
        second.items[0].thread_id,
        Some(ThreadId(format!("{mailbox}\u{1f}shared-thread")))
    );
    assert!(second.next_cursor.is_none());

    let primary_requests = primary.take_rest_requests();
    assert_eq!(primary_requests.len(), 2);
    assert!(primary_requests[0].url.contains("/me/messages?"));
    assert!(primary_requests[1].url.contains("$skiptoken=primary"));
    let shared_requests = shared_root.take_rest_requests();
    assert_eq!(shared_requests.len(), 1);
    assert!(
        shared_requests[0]
            .url
            .contains("/users/shared%40contoso.com/messages?")
    );
}

/// A full walk across THREE shared mailboxes where every one of them
/// returns results, and where the first shared mailbox pages twice on
/// its own.
///
/// Two things only this shape can catch. A shared mailbox's own
/// `@odata.nextLink` must be followed against THAT mailbox's client -
/// the cursor carries the owner beside the link, and a continuation
/// that forgot the owner would issue the absolute link on the primary
/// (which is armed empty here, so it panics rather than passing).
/// And the hand-off must be `owner -> the NEXT mailbox after it`, not
/// `owner -> the first`: with only two mailboxes an off-by-one that
/// restarted the tail is indistinguishable from a correct walk.
#[tokio::test]
async fn search_pages_within_and_across_three_shared_mailboxes() {
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "p1", "conversationId": "pt" }] }),
    )]);
    let a_root = GraphClient::new("token");
    a_root.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "a1", "conversationId": "at" }],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/a%40contoso.com/messages?$skiptoken=a2"
            }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{ "id": "a2", "conversationId": "at2" }] }),
        ),
    ]);
    let b_root = GraphClient::new("token");
    b_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "b1", "conversationId": "bt" }] }),
    )]);
    let c_root = GraphClient::new("token");
    c_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "c1", "conversationId": "ct" }] }),
    )]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([
            (
                "a@contoso.com".to_string(),
                a_root.for_shared_mailbox("a@contoso.com"),
            ),
            (
                "b@contoso.com".to_string(),
                b_root.for_shared_mailbox("b@contoso.com"),
            ),
            (
                "c@contoso.com".to_string(),
                c_root.for_shared_mailbox("c@contoso.com"),
            ),
        ]),
    );

    // Walk the whole surface, one page at a time, exactly as a consumer
    // would: feed each page's cursor back until there is none.
    let mut ids = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let mut request = SearchRequest::default();
        request.page_cursor = cursor;
        let page = search_message_rows(&account, request)
            .await
            .expect("every page of the walk succeeds");
        ids.extend(page.items.iter().map(|row| row.id.0.clone()));
        assert!(page.skipped_scopes.is_empty());
        pages += 1;
        assert!(pages <= 8, "the walk must terminate");
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert_eq!(pages, 5, "primary, a@ twice, b@, c@");
    assert_eq!(
        ids,
        vec![
            "p1".to_string(),
            "a@contoso.com\u{1f}a1".to_string(),
            "a@contoso.com\u{1f}a2".to_string(),
            "b@contoso.com\u{1f}b1".to_string(),
            "c@contoso.com\u{1f}c1".to_string(),
        ]
    );

    assert_eq!(primary.take_rest_requests().len(), 1);
    let a_requests = a_root.take_rest_requests();
    assert_eq!(a_requests.len(), 2);
    assert!(
        a_requests[0]
            .url
            .contains("/users/a%40contoso.com/messages?"),
        "{}",
        a_requests[0].url
    );
    assert!(
        a_requests[1].url.ends_with("$skiptoken=a2"),
        "the shared mailbox's own continuation is issued verbatim: {}",
        a_requests[1].url
    );
    assert_eq!(b_root.take_rest_requests().len(), 1);
    assert_eq!(c_root.take_rest_requests().len(), 1);
}

/// O-28: search was the one shared-mailbox door where a revoked share
/// escalated account-wide. The dead mailbox stays configured, so the
/// old `Err(NoPermission)` did not just fail one call - every retry
/// and every restarted search died at the same walk position, taking
/// the mailboxes behind it down too. The walk must quarantine the
/// revoked share like its sibling doors do, continue into the next
/// mailbox, and REPORT the skip on the page so "no matches in b@" is
/// distinguishable from "b@ was never searched".
#[tokio::test]
async fn a_revoked_shared_mailbox_is_skipped_and_the_walk_continues() {
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "primary-message", "conversationId": "primary-thread" }] }),
    )]);
    let dead = GraphClient::new("token");
    dead.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        json!({ "error": { "code": "AccessDenied", "message": "delegate access revoked" } }),
    )]);
    let alive = GraphClient::new("token");
    alive.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "alive-message", "conversationId": "alive-thread" }] }),
    )]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([
            (
                "b@contoso.com".to_string(),
                dead.for_shared_mailbox("b@contoso.com"),
            ),
            (
                "c@contoso.com".to_string(),
                alive.for_shared_mailbox("c@contoso.com"),
            ),
        ]),
    );

    let first = search_message_rows(&account, SearchRequest::default())
        .await
        .expect("primary page succeeds");
    assert!(first.skipped_scopes.is_empty());
    let mut continuation = SearchRequest::default();
    continuation.page_cursor = first.next_cursor;
    let resumed = search_message_rows(&account, continuation)
        .await
        .expect("the walk continues past the revoked mailbox");

    // The page holds the LIVE mailbox's results, owner-qualified.
    assert_eq!(
        resumed.items[0].id,
        ObjectId("c@contoso.com\u{1f}alive-message".to_string())
    );
    assert!(resumed.next_cursor.is_none());
    // The quarantined mailbox is reported, carrying the terminal
    // classification the old code raised account-wide.
    assert_eq!(resumed.skipped_scopes.len(), 1);
    let skip = &resumed.skipped_scopes[0];
    assert_eq!(
        skip.scope,
        ErrorScope::Mailbox {
            id: ("b@contoso.com".to_string()).into()
        }
    );
    assert!(matches!(
        skip.error.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    assert!(matches!(
        skip.error.recovery(),
        RecoveryClass::NoPermission { .. }
    ));
    assert_eq!(skip.error.scope(), Some(&skip.scope));

    // One request per mailbox: the revoked one was asked exactly once,
    // and the walk resumed at the next mailbox in the same call.
    assert_eq!(primary.take_rest_requests().len(), 1);
    assert_eq!(dead.take_rest_requests().len(), 1);
    let alive_requests = alive.take_rest_requests();
    assert_eq!(alive_requests.len(), 1);
    assert!(
        alive_requests[0]
            .url
            .contains("/users/c%40contoso.com/messages?")
    );
}

/// The revoked mailbox at the END of the walk must not turn "search
/// complete, minus one quarantined scope" into an error: the call
/// answers a terminal empty page that still reports the skip.
#[tokio::test]
async fn a_walk_ending_in_a_revoked_mailbox_completes_and_reports_the_skip() {
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "primary-message", "conversationId": "primary-thread" }] }),
    )]);
    let dead = GraphClient::new("token");
    dead.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        json!({ "error": { "code": "AccessDenied", "message": "delegate access revoked" } }),
    )]);
    let mailbox = "shared@contoso.com";
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary,
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), dead.for_shared_mailbox(mailbox))]),
    );

    let mut continuation = SearchRequest::default();
    continuation.page_cursor = search_message_rows(&account, SearchRequest::default())
        .await
        .expect("primary page succeeds")
        .next_cursor;
    let last = search_message_rows(&account, continuation)
        .await
        .expect("the walk completes despite the revoked tail mailbox");
    assert!(last.items.is_empty());
    assert!(last.next_cursor.is_none());
    assert_eq!(last.skipped_scopes.len(), 1);
    assert_eq!(
        last.skipped_scopes[0].scope,
        ErrorScope::Mailbox {
            id: (mailbox.to_string()).into()
        }
    );
}

/// The quarantine is exactly as narrow as its sibling doors': only a
/// permission denial on a FOREIGN mailbox skips. A transient failure
/// there still fails the call - the caller retries the same cursor and
/// can succeed - and a primary-mailbox 403 is a genuine account-level
/// signal, not something a walk may step around.
#[tokio::test]
async fn only_a_foreign_permission_denial_is_skipped() {
    // Transient failure on the shared mailbox: propagates retryable.
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [] }),
    )]);
    let flaky = GraphClient::new("token");
    flaky.script_rest([ScriptedRestResponse::empty(
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
    )]);
    let mailbox = "shared@contoso.com";
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary,
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), flaky.for_shared_mailbox(mailbox))]),
    );
    let mut continuation = SearchRequest::default();
    continuation.page_cursor = search_message_rows(&account, SearchRequest::default())
        .await
        .expect("primary page succeeds")
        .next_cursor;
    let transient = search_message_rows(&account, continuation)
        .await
        .expect_err("a transient shared-mailbox failure fails the call");
    assert!(transient.recovery().is_retryable());

    // Permission denial on the PRIMARY mailbox: propagates terminal.
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        json!({ "error": { "code": "AccessDenied", "message": "nope" } }),
    )]);
    let account = GraphAccount::new_for_tests(primary, PushMode::GraphSubscriptions);
    let denied = search_message_rows(&account, SearchRequest::default())
        .await
        .expect_err("a primary permission denial fails the call");
    assert!(matches!(
        denied.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    assert!(denied.recovery().is_terminal());
}

/// A search cursor names a position in a WALK over several mailboxes,
/// so it is only meaningful against the mailbox list that minted it.
/// Both resuming accounts are armed EMPTY: a cursor that decoded into a
/// plausible-looking position would issue its first request and hit the
/// seam's exhaustion panic rather than quietly returning a page set
/// that silently omits a whole mailbox.
#[tokio::test]
async fn a_search_cursor_from_another_shared_mailbox_set_is_rejected() {
    let minting_primary = GraphClient::new("token");
    minting_primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [] }),
    )]);
    let minting = GraphAccount::new_for_tests_with_shared_clients(
        minting_primary,
        PushMode::GraphSubscriptions,
        shared_clients_for_tests(&["b@contoso.com", "c@contoso.com"]),
    );
    let cursor = search_message_rows(&minting, SearchRequest::default())
        .await
        .expect("primary page succeeds")
        .next_cursor
        .expect("the walk continues into the shared mailboxes");

    // Both sets still configure the cursor's own mailbox, so nothing but
    // the pinned set can reject them: `a@` sorts BEFORE the position and
    // would never be searched, and dropping `c@` would end the walk a
    // mailbox early. Either way the caller gets fewer results than it
    // asked for and no signal that it did.
    for mailboxes in [
        vec!["a@contoso.com", "b@contoso.com", "c@contoso.com"],
        vec!["b@contoso.com"],
    ] {
        let primary = GraphClient::new("token");
        primary.script_rest([]);
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary,
            PushMode::GraphSubscriptions,
            shared_clients_for_tests(&mailboxes),
        );
        let mut request = SearchRequest::default();
        request.page_cursor = Some(cursor.clone());
        let error = search_message_rows(&account, request)
            .await
            .expect_err("a cursor minted against another mailbox set is rejected");
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ),
            "{mailboxes:?}: {:?}",
            error.kind()
        );
    }
}

/// Cursor bytes are request INPUT the caller handed back, so a corrupt
/// or stale one is the caller's to fix - not evidence that Graph broke
/// its contract. `Protocol(ContractViolation)` would derive
/// `ProviderContractViolation` / `ContactProviderSupport` and point an
/// operator at Microsoft for damaged client state.
#[tokio::test]
async fn a_corrupt_search_cursor_blames_the_request_not_the_provider() {
    for cursor in [
        // truncated payload under the current version prefix
        format!("{SEARCH_CURSOR_PREFIX}{{\"mailboxes\":[]").into_bytes(),
        // a bare Graph nextLink - what this crate emitted before the
        // walk existed, and what a truncated cursor degrades to. It must
        // be refused, never re-issued as a URL on the caller's word.
        b"https://graph.microsoft.com/v1.0/me/messages?$skiptoken=x".to_vec(),
        // not UTF-8 at all
        vec![0xff, 0xfe],
    ] {
        let primary = GraphClient::new("token");
        primary.script_rest([]);
        let account = GraphAccount::new_for_tests(primary, PushMode::GraphSubscriptions);
        let mut request = SearchRequest::default();
        request.page_cursor = Some(cursor.clone());
        let error = search_message_rows(&account, request)
            .await
            .expect_err("an undecodable cursor is refused");
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ),
            "{}: {:?}",
            String::from_utf8_lossy(&cursor),
            error.kind()
        );
        assert_eq!(error.recovery(), &RecoveryClass::ClientBug);
        assert_eq!(
            error.suggested_remediation(),
            Some(&RemediationAction::FixClientRequest)
        );
        assert_eq!(error.operation(), Some(AccountOperation::Search));
    }
}

fn shared_clients_for_tests(mailboxes: &[&str]) -> HashMap<String, GraphClient> {
    mailboxes
        .iter()
        .map(|mailbox| {
            let client = GraphClient::new("token");
            client.script_rest([]);
            ((*mailbox).to_string(), client.for_shared_mailbox(*mailbox))
        })
        .collect()
}

/// The complete `delete_thread` operation for a shared thread, not just
/// its id resolution.
///
/// Every read leg - the conversation query, the Trash lookup, and the
/// etag preflight - must run on the
/// owner's client, and the Trash id must come back owner-qualified. The
/// primary is armed with EXACTLY the one response the operation
/// legitimately needs (the `/$batch` POST, which is account-wide and
/// carries its routing in the subrequest URLs), so any leg that fell
/// back to `/me` consumes that response and the next primary request
/// hits the seam's exhaustion panic.
///
/// Resolving Trash on the primary is not a silent wrong-mailbox write
/// either way: `move_messages` refuses a destination whose owner
/// differs from the message's, so the bug surfaced as a bare primary
/// folder id being rejected against foreign-owned messages.
#[tokio::test]
async fn deleting_a_shared_thread_resolves_trash_in_the_owner_mailbox() {
    let mailbox = "shared@contoso.com";
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "responses": [{ "id": "0", "status": 200 }] }),
    )]);
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([
        // 1. the conversation members
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{
                    "id": "AAMkmsg",
                    "parentFolderId": "InboxId",
                    "changeKey": "CK0"
                }]
            }),
        ),
        // 2. The shared mailbox's own Trash id. Deliberately different
        // from any primary folder id: the primary's would name nothing.
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "SharedTrash" })),
        // 3. the move's etag preflight
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "id": "AAMkmsg", "changeKey": "CK1" }),
        ),
    ]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared_root.for_shared_mailbox(mailbox))]),
    );

    delete_thread(
        account,
        ThreadId(format!("{mailbox}\u{1f}conversation-1")),
        None,
    )
    .await
    .expect("shared thread deletes");

    let shared_requests = shared_root.take_rest_requests();
    assert_eq!(shared_requests.len(), 3);
    for request in &shared_requests {
        assert!(
            request.url.contains("/users/shared%40contoso.com/"),
            "every read leg routes through the owner: {}",
            request.url
        );
    }
    assert!(
        shared_requests[1]
            .url
            .ends_with("/users/shared%40contoso.com/mailFolders/deletedItems?$select=id"),
        "Trash is resolved against the owner's own well-known folders: {}",
        shared_requests[1].url
    );

    // The primary carried the `/$batch` envelope and nothing else. The
    // destination is the SHARED mailbox's Trash, sent native (the
    // `destinationId` body may never carry a `\u{1f}` id).
    let primary_requests = primary.take_rest_requests();
    assert_eq!(primary_requests.len(), 1);
    assert!(primary_requests[0].url.ends_with("/$batch"));
    let body = primary_requests[0].body.as_ref().expect("batch body");
    let sub = &body["requests"][0];
    assert_eq!(
        sub["url"],
        json!("/users/shared%40contoso.com/messages/AAMkmsg/move")
    );
    assert_eq!(sub["body"]["destinationId"], json!("SharedTrash"));
}

/// The cache is keyed PER MAILBOX, and each key resolves against its
/// own client. A single shared slot would hand one mailbox's Trash id to
/// another, which is a folder id that names nothing there - so the two
/// mailboxes are armed with different ids on independent scripts, and
/// each is asserted to have been asked exactly once.
#[tokio::test]
async fn trash_lookup_is_cached_per_mailbox() {
    let mailbox = "shared@contoso.com";
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "id": "PrimaryTrash" }),
    )]);
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "id": "SharedTrash" }),
    )]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared_root.for_shared_mailbox(mailbox))]),
    );

    for round in 0..2 {
        assert_eq!(
            trash_container_id(&account, None, ErrorScope::Account)
                .await
                .expect("primary Trash resolves"),
            ContainerId("PrimaryTrash".to_string()),
            "round {round}"
        );
        assert_eq!(
            trash_container_id(&account, Some(mailbox), ErrorScope::Account)
                .await
                .expect("shared Trash resolves"),
            ContainerId(format!("{mailbox}\u{1f}SharedTrash")),
            "round {round}"
        );
    }

    assert_eq!(primary.take_rest_requests().len(), 1);
    let shared_requests = shared_root.take_rest_requests();
    assert_eq!(shared_requests.len(), 1);
    assert!(
        shared_requests[0]
            .url
            .ends_with("/users/shared%40contoso.com/mailFolders/deletedItems?$select=id")
    );
}

/// Only a RESOLVED folder id may enter the cache, and a lookup that did
/// not resolve must fail rather than answer with the well-known NAME.
///
/// `deletedItems` is a valid move destination, so a degraded answer
/// keeps `delete_thread` looking healthy - but it is not the concrete id
/// `container_is_trash` compares against, so a thread ALREADY in Trash
/// gets moved there again and reports success instead of being
/// destroyed. Caching it made one transient 503 do that for the rest of
/// the account's life.
#[tokio::test]
async fn a_failed_trash_lookup_neither_answers_nor_caches() {
    let client = GraphClient::new("token");
    client.script_rest([
        // 1. transient failure
        ScriptedRestResponse::empty(reqwest::StatusCode::SERVICE_UNAVAILABLE),
        // 2. a 200 that carries no folder id
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({})),
        // 3. the real answer, which is the only one worth keeping
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "PrimaryTrash" })),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);

    let transient = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect_err("a transient lookup failure propagates");
    assert!(transient.recovery().is_retryable());
    let no_id = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect_err("a 200 without an id is a contract violation");
    assert!(matches!(
        no_id.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
    assert_eq!(no_id.scope(), Some(&ErrorScope::Account));

    let resolved = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect("the lookup resolves once Graph answers");
    assert_eq!(resolved, ContainerId("PrimaryTrash".to_string()));
    let cached = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect("the resolved id is cached");
    assert_eq!(cached, resolved);
    // Three lookups, not four: only the resolved one was retained.
    assert_eq!(client.take_rest_requests().len(), 3);
}

#[test]
fn container_is_trash_compares_in_the_thread_owners_namespace() {
    let mailbox = "shared@contoso.com";
    let shared_trash = ContainerId(format!("{mailbox}\u{1f}SharedTrash"));

    assert!(container_is_trash(
        &shared_trash,
        &shared_trash,
        Some(mailbox)
    ));
    // The well-known-NAME fallback still applies, but only qualified.
    assert!(container_is_trash(
        &ContainerId(format!("{mailbox}\u{1f}deletedItems")),
        &shared_trash,
        Some(mailbox)
    ));
    // A BARE `deletedItems` names the PRIMARY mailbox's Trash. Accepting
    // it for a shared thread would take the destroy branch and delete
    // messages that were never in their own Trash - unrecoverable, where
    // a redundant move is not.
    assert!(!container_is_trash(
        &ContainerId(DELETED_ITEMS.to_string()),
        &shared_trash,
        Some(mailbox)
    ));
    // Another mailbox's Trash is not this thread's Trash either.
    assert!(!container_is_trash(
        &ContainerId("other@contoso.com\u{1f}deletedItems".to_string()),
        &shared_trash,
        Some(mailbox)
    ));
    // The primary keeps its bare form on both sides.
    assert!(container_is_trash(
        &ContainerId(DELETED_ITEMS.to_string()),
        &ContainerId("PrimaryTrash".to_string()),
        None
    ));
}

/// A shared-mailbox client is derived inside `GraphAccount::new`, so a
/// test never holds it. It shares the primary's script - the way it
/// already shares the `AccountNet` a request actually
/// goes down - or every foreign-mailbox request would sit outside both
/// the script and the recorded list.
#[tokio::test]
async fn a_foreign_message_write_rides_the_primary_clients_script() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"id": "AAMkmsg", "changeKey": "ck"}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"responses": [{"id": "0", "status": 200}]}),
        ),
    ]);
    let account = GraphAccount::new_for_tests_with_shared(
        client.clone(),
        PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    );
    let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");

    set_is_read(account, MutationTarget::Message(id), true)
        .await
        .expect("foreign write routes to the owning mailbox");

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .url
            .contains("/users/shared%40contoso.com/messages/AAMkmsg"),
        "{}",
        requests[0].url
    );
    assert!(requests[1].url.ends_with("/$batch"));
}

#[test]
fn message_batch_url_routes_foreign_id_to_owner_with_native_id() {
    let account = shared_account();
    let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
    // Per-message PATCH / DELETE (suffix "") and the move suffix both
    // route to `/users/{owner}/messages/{native}` with no `\u{1f}`.
    assert_eq!(
        message_batch_url(&account, &id, "", AccountOperation::Hydrate).expect("configured"),
        "/users/shared%40contoso.com/messages/AAMkmsg"
    );
    assert_eq!(
        message_batch_url(&account, &id, "/move", AccountOperation::Hydrate).expect("configured"),
        "/users/shared%40contoso.com/messages/AAMkmsg/move"
    );
    assert!(
        !message_batch_url(&account, &id, "", AccountOperation::Hydrate)
            .expect("configured")
            .contains('\u{1f}')
    );
}

#[test]
fn message_batch_url_keeps_primary_id_on_me() {
    let account = shared_account();
    let id = ObjectId("AAMkmsg".to_string());
    assert_eq!(
        message_batch_url(&account, &id, "", AccountOperation::Hydrate).expect("primary"),
        "/me/messages/AAMkmsg"
    );
}

#[test]
fn message_batch_url_rejects_an_unconfigured_foreign_owner() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let id = foreign_message_id("other@contoso.com", "AAMkfolder", "AAMkmsg");
    let error = message_batch_url(&account, &id, "", AccountOperation::Hydrate)
        .expect_err("stale foreign owner must not route through /me");
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
    // This helper is the per-item URL builder for four `$batch`
    // surfaces, so the id it refused is what makes the failure
    // actionable on the lane it lands in.
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Message {
            id: (id.0.clone()).into()
        })
    );
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

#[test]
fn scheduled_send_deferred_body_shape() {
    // 1970-01-01T00:00:00Z plus one day -> a stable ISO-8601 UTC value.
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400);
    let body = deferred_send_time_body(at);
    let props = body
        .get("singleValueExtendedProperties")
        .and_then(serde_json::Value::as_array)
        .expect("singleValueExtendedProperties array");
    let prop = &props[0];
    assert_eq!(
        prop.get("id").and_then(serde_json::Value::as_str),
        Some("SystemTime 0x3FEF")
    );
    assert_eq!(
        prop.get("value").and_then(serde_json::Value::as_str),
        Some("1970-01-02T00:00:00Z")
    );
}

#[test]
fn scheduled_send_capability_is_true() {
    let caps = crate::account::capabilities::build_capabilities(
        crate::account::PushMode::GraphSubscriptions,
        true,
    );
    assert!(caps.pim_methods.scheduled_send);
}

/// `send_as` is advertised from the account's routing table, so the check
/// that matters runs through `capabilities()` on a real account rather than
/// over a hand-built snapshot.
///
/// An account with no shared mailbox can only answer a `Some(send_as)`
/// request with the `send_as_unknown_mailbox` rejection, so advertising the
/// flag there hands a consumer a path that fails every time.
#[test]
fn send_as_capability_follows_configured_shared_mailboxes() {
    use bifrost_types::Account as _;

    let client = GraphClient::new("token");
    client.script_rest([]);
    let bare = GraphAccount::new_for_tests(client, crate::account::PushMode::GraphSubscriptions);
    assert!(!bare.capabilities().pim_methods.send_as);

    let client = GraphClient::new("token");
    client.script_rest([]);
    let shared = GraphAccount::new_for_tests_with_shared(
        client,
        crate::account::PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    );
    assert!(shared.capabilities().pim_methods.send_as);
}

fn mailbox_address(value: &Value) -> Option<&str> {
    value
        .get("emailAddress")
        .and_then(|e| e.get("address"))
        .and_then(Value::as_str)
}

#[test]
fn apply_send_as_as_sets_from_and_sender() {
    let mut message = json!({});
    let send_as = SendAs::As(MailboxId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("shared@contoso.com")
    );
}

#[test]
fn apply_send_as_on_behalf_sets_sender_to_user() {
    let mut message = json!({});
    let send_as = SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("user@contoso.com")
    );
}

#[test]
fn apply_send_as_on_behalf_omits_sender_without_user_email() {
    let mut message = json!({});
    let send_as = SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, None);
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert!(message.get("sender").is_none());
}

#[test]
fn apply_send_as_on_behalf_honors_explicit_from() {
    let mut message = json!({
        "from": { "emailAddress": { "address": "author@contoso.com" } }
    });
    let send_as = SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    // Explicit author survives; only sender is stamped.
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("author@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("user@contoso.com")
    );
}

#[test]
fn apply_send_as_as_overrides_explicit_from() {
    let mut message = json!({
        "from": { "emailAddress": { "address": "author@contoso.com" } }
    });
    let send_as = SendAs::As(MailboxId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    // `As` forces author == sender == mailbox.
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("shared@contoso.com")
    );
}

#[test]
fn send_as_unknown_mailbox_is_malformed() {
    let err = send_as_unknown_mailbox(&MailboxId("nobody@contoso.com".to_string()));
    assert!(matches!(
        err.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert_eq!(err.operation(), Some(AccountOperation::Send));
    assert!(err.recovery().is_terminal());
    // The field pointer is the whole reason this is `InvalidArgument` and
    // not a prose `Malformed`: a consumer UI highlights the input it names,
    // and gets the same name from the only other backend that advertises
    // the capability.
    let names_the_field = err.chain().iter().any(|cause| {
        matches!(
            cause,
            bifrost_types::Cause::Request(bifrost_types::RequestCause::InvalidArgument {
                field: Some("send_as.mailbox"),
                ..
            })
        )
    });
    assert!(names_the_field, "the offending field must be named");
}

#[test]
fn scheduled_send_handle_round_trips_owning_mailbox() {
    let handle = encode_scheduled_send_handle(Some("shared@contoso.com"), "AAMkAGI2");
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle);
    assert_eq!(mailbox, Some("shared@contoso.com"));
    assert_eq!(draft_id, "AAMkAGI2");
}

#[test]
fn scheduled_send_handle_without_mailbox_is_bare_draft_id() {
    let handle = encode_scheduled_send_handle(None, "AAMkAGI2");
    assert_eq!(handle, "AAMkAGI2");
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle);
    assert_eq!(mailbox, None);
    assert_eq!(draft_id, "AAMkAGI2");
}

#[test]
fn maps_last_verb_property_alias() {
    assert_eq!(
        graph_extended_property_id("PR_LAST_VERB_EXECUTED"),
        "Integer 0x1081"
    );
    assert_eq!(graph_extended_property_id("String 0x4001"), "String 0x4001");
}

#[test]
fn search_filter_escapes_odata_strings() {
    let filter =
        odata_filter(&SearchFilter::Subject("Bob's plan".to_string())).expect("filter builds");
    assert_eq!(filter, "contains(subject,'Bob''s plan')");
}

// The query string carries the KQL phrase percent-encoded; compare
// against the same encoder the production path uses rather than a
// hand-maintained decode table.
fn search_param(url: &str) -> String {
    url.split("$search=")
        .nth(1)
        .expect("url has a $search param")
        .to_string()
}

#[test]
fn from_to_filters_route_through_search_kql_not_filter() {
    // A From substring must hit `$search` (KQL), never the `$filter`
    // `contains()` Graph rejects.
    let req = SearchRequest::filter(SearchFilter::From("alice".to_string()));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$search="), "{url}");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"from:\"alice\"\"")
    );

    let to = SearchRequest::filter(SearchFilter::To("bob@x".to_string()));
    let to_url = search_url("/me", &to).expect("url builds");
    assert_eq!(
        search_param(&to_url),
        bifrost_net::url::encode_query_value("\"(to:\"bob@x\" OR cc:\"bob@x\")\"")
    );
}

#[test]
fn non_sender_filter_still_uses_odata_filter() {
    // Subject-only search has a clean `$filter` shape; it must not be
    // forced onto `$search`.
    let req = SearchRequest::filter(SearchFilter::Subject("invoice".to_string()));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$filter="), "{url}");
    assert!(!url.contains("$search="), "{url}");
}

#[test]
fn mixed_from_and_date_range_collapses_to_one_kql_search() {
    // From substring AND a date range: the whole thing goes to KQL,
    // never a mixed `$search`+`$filter` request.
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::From("alice".to_string()),
        SearchFilter::DateRange {
            after: Some(SystemTime::UNIX_EPOCH),
            before: None,
        },
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$search="), "{url}");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"(from:\"alice\") AND (received>=1970-01-01)\"")
    );
}

#[test]
fn from_filter_and_provider_query_combine_in_kql() {
    // A structured From plus a raw provider query AND together into one
    // `$search`; the request must not emit both `$filter` and `$search`.
    let mut req = SearchRequest::filter(SearchFilter::From("alice".to_string()));
    req.provider_query = Some("importance:high".to_string());
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$search="), "{url}");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"(from:\"alice\") AND (importance:high)\"")
    );
}

#[test]
fn from_combined_with_folder_restriction_is_malformed() {
    // `In` has no KQL property; combining it with a From substring is an
    // inexpressible query and must fail cleanly rather than ship an
    // invalid request or silently search every folder.
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::From("alice".to_string()),
        SearchFilter::In(ContainerId("inbox".to_string())),
    ]));
    let err = search_url("/me", &req).expect_err("inexpressible combination");
    assert!(matches!(
        err.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert_eq!(err.operation(), Some(AccountOperation::Search));
}

#[test]
fn kql_quoting_escapes_embedded_double_quotes() {
    assert_eq!(kql_quoted(r#"a"b"#), r#""a\"b""#);
}

#[test]
fn inline_attachment_is_base64_encoded() {
    let attachment = AttachmentInline {
        filename: "note.txt".to_string(),
        mime: "text/plain".to_string(),
        data: Bytes::from_static(b"hello"),
        inline: false,
        content_id: None,
    };
    let value = graph_attachment_from_inline(&attachment).expect("attachment builds");
    assert_eq!(value["contentBytes"], json!("aGVsbG8="));
    // No content_id -> no contentId key emitted.
    assert!(value.get("contentId").is_none());
}

#[test]
fn inline_attachment_emits_content_id_when_set() {
    let attachment = AttachmentInline {
        filename: "logo.png".to_string(),
        mime: "image/png".to_string(),
        data: Bytes::from_static(b"img"),
        inline: true,
        content_id: Some("<logo@x>".to_string()),
    };
    let value = graph_attachment_from_inline(&attachment).expect("attachment builds");
    // Angle brackets are stripped to the bare cid token.
    assert_eq!(value["contentId"], json!("logo@x"));
}

#[test]
fn importance_read_maps_graph_wire_field() {
    assert_eq!(
        importance_from_graph(&json!({ "importance": "high" })),
        Importance::High
    );
    assert_eq!(
        importance_from_graph(&json!({ "importance": "low" })),
        Importance::Low
    );
    assert_eq!(
        importance_from_graph(&json!({ "importance": "normal" })),
        Importance::Normal
    );
    // Absent or unrecognized -> Normal.
    assert_eq!(importance_from_graph(&json!({})), Importance::Normal);
    assert_eq!(
        importance_from_graph(&json!({ "importance": "URGENT" })),
        Importance::Normal
    );
}

#[test]
fn set_importance_produces_one_exclusive_patch_body() {
    // Exactly one wire op: a single `{ "importance": "high" }` body,
    // no clear-then-set pair. The `If-Match` etag is attached by
    // `patch_messages` from `MessagePatch::etag`, not the body.
    let body = graph_importance_body(Importance::High);
    assert_eq!(body, json!({ "importance": "high" }));
    assert_eq!(body.as_object().expect("object").len(), 1);
    assert_eq!(
        graph_importance_body(Importance::Low),
        json!({ "importance": "low" })
    );
    assert_eq!(
        graph_importance_body(Importance::Normal),
        json!({ "importance": "normal" })
    );
}

/// The `bifrost-types` convenience layer routes `set_starred` through
/// `set_category` with the reserved `"$flagged"` sentinel (Graph
/// advertises `StarredFlagShape::Category`). Pin the sentinel Graph
/// honors, plus the historical aliases, so a rename on either side is a
/// test failure rather than a silently-created literal category named
/// `$flagged` on every starred message.
#[test]
fn the_reserved_starred_sentinel_and_its_aliases_are_recognized() {
    assert_eq!(STARRED_CATEGORY, "$flagged");
    for token in [
        "$flagged",
        "$FLAGGED",
        "\\flagged",
        "\\Flagged",
        "flagged",
        "Flagged",
        "starred",
        "STARRED",
    ] {
        assert!(is_starred_category(token), "{token} must map to the flag");
    }
}

#[test]
fn ordinary_category_names_are_not_mistaken_for_the_starred_sentinel() {
    for token in ["Work", "flag", "$flagged-ish", "un-flagged", "$starred", ""] {
        assert!(
            !is_starred_category(token),
            "{token} must stay an ordinary category"
        );
    }
}

#[test]
fn setting_an_ordinary_category_is_a_read_modify_write_over_the_existing_array() {
    // `set_category` reads `categories`, edits, sorts, and writes the
    // whole array back; a naive `["new"]` write would drop the others.
    let existing = json!({ "categories": ["Zeta", "Alpha"] });
    let mut categories = categories_from_value(&existing);
    assert_eq!(categories, vec!["Zeta".to_string(), "Alpha".to_string()]);
    categories.push("Mid".to_string());
    categories.sort();
    assert_eq!(categories, vec!["Alpha", "Mid", "Zeta"]);

    // Absent / non-array `categories` reads as empty rather than
    // panicking, so a sparse `$select` cannot poison the write.
    assert!(categories_from_value(&json!({})).is_empty());
    assert!(categories_from_value(&json!({ "categories": "Work" })).is_empty());
    // Non-string members are skipped.
    assert_eq!(
        categories_from_value(&json!({ "categories": ["Work", 7, null] })),
        vec!["Work".to_string()]
    );
}

#[test]
fn last_verb_executed_aliases_map_onto_the_graph_proptag() {
    // `replied_via_extended_property` / `forwarded_via_extended_property`
    // are advertised true, and the convenience layer passes the MAPI
    // alias; Graph only accepts the proptag form.
    for alias in [
        "PR_LAST_VERB_EXECUTED",
        "pr_last_verb_executed",
        "PidTagLastVerbExecuted",
        "pidtaglastverbexecuted",
    ] {
        assert_eq!(
            graph_extended_property_id(alias),
            PR_LAST_VERB_EXECUTED_GRAPH_ID
        );
    }
    // Anything else passes through verbatim - the caller owns the id.
    assert_eq!(
        graph_extended_property_id("SystemTime 0x3FEF"),
        "SystemTime 0x3FEF"
    );
}

#[test]
fn the_deferred_send_property_is_the_pidtag_deferred_send_time_proptag() {
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
    let body = deferred_send_time_body(at);
    let props = body
        .get("singleValueExtendedProperties")
        .and_then(Value::as_array)
        .expect("array");
    assert_eq!(props.len(), 1);
    assert_eq!(props[0]["id"], json!(DEFERRED_SEND_TIME_PROPERTY_ID));
    // ISO-8601 UTC with a `Z`, second precision, no fraction.
    let value = props[0]["value"].as_str().expect("string value");
    assert!(value.ends_with('Z'), "{value}");
    assert!(!value.contains('.'), "{value}");
    assert_eq!(value, graph_iso8601_utc(at));
}

#[test]
fn send_as_on_behalf_of_keeps_an_explicit_author_but_stamps_the_sender() {
    let mut message = json!({ "from": { "emailAddress": { "address": "author@contoso.com" } } });
    apply_send_as(
        &mut message,
        &SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string())),
        Some("me@contoso.com"),
    );
    assert_eq!(
        message["from"]["emailAddress"]["address"],
        json!("author@contoso.com")
    );
    assert_eq!(
        message["sender"]["emailAddress"]["address"],
        json!("me@contoso.com")
    );
}

#[test]
fn send_as_on_behalf_of_fills_a_missing_author_with_the_mailbox() {
    let mut message = json!({});
    apply_send_as(
        &mut message,
        &SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string())),
        None,
    );
    assert_eq!(
        message["from"]["emailAddress"]["address"],
        json!("shared@contoso.com")
    );
    // Without a known user email `sender` is omitted so Graph fills it
    // from the authenticated context rather than being told wrongly.
    assert!(message.get("sender").is_none());
}

#[test]
fn send_as_overrides_any_consumer_supplied_author() {
    // `As` means author == sender == mailbox; a consumer-set `from`
    // must not survive, or the message would claim an identity the
    // send-as grant does not cover.
    let mut message = json!({ "from": { "emailAddress": { "address": "author@contoso.com" } } });
    apply_send_as(
        &mut message,
        &SendAs::As(MailboxId("shared@contoso.com".to_string())),
        Some("me@contoso.com"),
    );
    assert_eq!(
        message["from"]["emailAddress"]["address"],
        json!("shared@contoso.com")
    );
    assert_eq!(
        message["sender"]["emailAddress"]["address"],
        json!("shared@contoso.com")
    );
}

/// `DraftPatch` is `#[non_exhaustive]`, so functional-update syntax is
/// unavailable outside `bifrost-types`; build through a closure so the
/// tests below read as one expression each.
fn draft_patch(fill: impl FnOnce(&mut DraftPatch)) -> DraftPatch {
    let mut patch = DraftPatch::default();
    fill(&mut patch);
    patch
}

#[test]
fn a_draft_patch_of_one_field_touches_only_that_field() {
    // The partial-update seam: `draft_update` passes
    // `include_empty = false`, so a subject-only rename must not emit
    // `null` / `[]` for the untouched recipient and body buckets (Graph
    // reads those as clears).
    let patch = draft_patch(|patch| {
        patch.subject = Some(Some("Renamed".to_string()));
    });
    let message = message_from_draft_patch(&patch, false).expect("patch builds");
    let object = message.as_object().expect("object");
    assert_eq!(object.get("subject"), Some(&json!("Renamed")));
    for untouched in [
        "toRecipients",
        "ccRecipients",
        "bccRecipients",
        "replyTo",
        "from",
        "body",
        "attachments",
    ] {
        assert!(
            !object.contains_key(untouched),
            "{untouched} must not appear in a sparse draft patch: {message}"
        );
    }
    assert_eq!(object.len(), 1);
}

#[test]
fn an_explicitly_cleared_draft_field_still_emits_its_clear() {
    // The flip side: `Some(None)` is a deliberate clear and MUST reach
    // the wire, so the sparse rule cannot just drop every empty value.
    let patch = draft_patch(|patch| {
        patch.subject = Some(None);
        patch.from = Some(None);
        patch.cc = Some(Vec::new());
    });
    let message = message_from_draft_patch(&patch, false).expect("patch builds");
    assert_eq!(message.get("subject"), Some(&json!("")));
    assert_eq!(message.get("from"), Some(&Value::Null));
    assert_eq!(message.get("cc"), None);
    assert_eq!(message.get("ccRecipients"), Some(&json!([])));
}

#[tokio::test]
async fn deleting_a_thread_from_trash_permanently_deletes_in_its_owner_mailbox() {
    for shared in [false, true] {
        let mailbox = "shared@contoso.com";
        let primary = GraphClient::new("token");
        let owner_root = if shared {
            GraphClient::new("token")
        } else {
            primary.clone()
        };
        owner_root.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{
                        "id": "message",
                        "parentFolderId": "TrashId",
                        "changeKey": "version"
                    }]
                }),
            ),
            ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "TrashId" })),
        ]);
        primary.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "responses": [{ "id": "0", "status": 204 }] }),
        )]);
        let clients = if shared {
            HashMap::from([(mailbox.to_string(), owner_root.for_shared_mailbox(mailbox))])
        } else {
            HashMap::new()
        };
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary.clone(),
            PushMode::GraphSubscriptions,
            clients,
        );
        let (thread, current, prefix) = if shared {
            (
                ThreadId(format!("{mailbox}\u{1f}conversation")),
                ContainerId(encode_foreign(mailbox, "TrashId").0),
                "/users/shared%40contoso.com",
            )
        } else {
            (
                ThreadId("conversation".to_string()),
                ContainerId("TrashId".to_string()),
                "/me",
            )
        };
        delete_thread(account, thread, Some(current))
            .await
            .expect("thread expunged");
        let reads = owner_root.take_rest_requests();
        assert_eq!(reads.len(), if shared { 2 } else { 3 });
        for request in &reads[..2] {
            assert!(
                request.url.contains(&format!("{prefix}/")),
                "{}",
                request.url
            );
        }
        let envelopes = if shared {
            primary.take_rest_requests()
        } else {
            reads[2..].to_vec()
        };
        assert_eq!(envelopes.len(), 1);
        let sub = &envelopes[0].body.as_ref().expect("batch body")["requests"][0];
        assert_eq!(sub["method"], "POST");
        assert_eq!(
            sub["url"],
            format!("{prefix}/messages/message/permanentDelete")
        );
        assert_eq!(sub["headers"]["If-Match"], "W/\"version\"");
        assert!(sub.get("body").is_none_or(Value::is_null));
    }
}

#[tokio::test]
async fn deleting_a_thread_from_trash_only_destroys_members_in_trash() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [
                    {
                        "id": "sent-copy",
                        "parentFolderId": "SentId",
                        "changeKey": "sent-etag"
                    },
                    {
                        "id": "inbox-copy",
                        "parentFolderId": "InboxId",
                        "changeKey": "inbox-etag"
                    },
                    {
                        "id": "trash-copy",
                        "parentFolderId": "TrashId",
                        "changeKey": "trash-etag"
                    }
                ]
            }),
        ),
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "TrashId" })),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "responses": [{ "id": "0", "status": 204 }] }),
        ),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);

    delete_thread(
        account,
        ThreadId("conversation".to_string()),
        Some(ContainerId("TrashId".to_string())),
    )
    .await
    .expect("only the Trash copy is permanently deleted");

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].url.contains("/me/messages?"));
    assert!(requests[0].url.contains("parentFolderId"));
    assert!(requests[0].url.contains("changeKey"));
    let batch = requests[2].body.as_ref().expect("batch body");
    let items = batch["requests"].as_array().expect("batch items");
    assert_eq!(items.len(), 1, "Sent and Inbox copies stay untouched");
    assert_eq!(items[0]["headers"]["If-Match"], "W/\"trash-etag\"");
    assert_eq!(
        items[0]["url"],
        json!("/me/messages/trash-copy/permanentDelete")
    );
}

#[tokio::test]
async fn partial_commit_batch_keeps_success_refusal_and_missing_siblings() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({"responses":[
        {"id":"1", "status":403, "body":{"error":{"code":"ErrorAccessDenied","message":"denied"}}},
        {"id":"0", "status":200, "body":{"id":"new-a"}}
    ]}))]);
    let account =
        GraphAccount::new_for_tests_with_shared(client, PushMode::GraphSubscriptions, &[]);
    let ids: Vec<ObjectId> = ["a", "b", "c"]
        .into_iter()
        .map(|id| ObjectId(id.into()))
        .collect();
    let requests = ids
        .iter()
        .enumerate()
        .map(|(index, id)| BatchRequestItem {
            id: index.to_string(),
            method: "POST".into(),
            url: format!("/me/messages/{}/move", id.0),
            body: Some(json!({"destinationId":"dest"})),
            headers: None,
        })
        .collect();
    let progress = submit_write_batch_with_targets(
        &account,
        requests,
        &ids,
        false,
        AccountOperation::AddToContainer,
    )
    .await
    .unwrap();
    let error = batch_progress_error(
        progress,
        &ids,
        bifrost_types::StepEffect::Moved,
        bifrost_types::StepReplay::Blocking,
    )
    .unwrap_err();
    assert!(matches!(
        error.recovery(),
        RecoveryClass::NoPermission { .. }
    ));
    let record = error.partial_commit().unwrap();
    assert_eq!(record.steps().len(), 2);
    assert_eq!(
        record.steps()[0].targets().refs[1].scope,
        ErrorScope::Message {
            id: ObjectId("new-a".into())
        }
    );
    assert_eq!(record.plan().gating.len(), 1);
}
#[test]
fn partial_commit_batch_move_success_without_id_gates() {
    let leaf = bifrost_types::AccountError::unsupported(
        AccountOperation::AddToContainer,
        bifrost_types::Protocol::Graph,
    );
    let progress = BatchProgress {
        committed: vec![(0, None)],
        failed: vec![(1, bifrost_types::Application::from_leaf(&leaf), leaf)],
    };
    let error = batch_progress_error(
        progress,
        &[ObjectId("a".into()), ObjectId("b".into())],
        bifrost_types::StepEffect::Moved,
        bifrost_types::StepReplay::Blocking,
    )
    .unwrap_err();
    assert!(
        !error.partial_commit().unwrap().steps()[0]
            .targets()
            .complete
    );
    assert_eq!(error.partial_commit().unwrap().plan().gating.len(), 1);
    assert!(
        matches!(error.recovery(), RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget,bifrost_types::ReconcileAction::ObserveOnly])
    );
}

#[test]
fn partial_commit_batch_reconciling_subresponse_is_uncertain() {
    let leaf = crate::account::graph_error::batch_response_missing(
        AccountOperation::AddToContainer,
        Some(ErrorScope::Message {
            id: ObjectId("a".into()),
        }),
        "missing answer",
        false,
    );
    let progress = BatchProgress {
        committed: vec![],
        failed: vec![(0, bifrost_types::Application::from_leaf(&leaf), leaf)],
    };
    let error = batch_progress_error(
        progress,
        &[ObjectId("a".into())],
        bifrost_types::StepEffect::Moved,
        bifrost_types::StepReplay::Blocking,
    )
    .unwrap_err();
    assert_eq!(
        error.partial_commit().unwrap().primary().application,
        bifrost_types::Application::Uncertain
    );
}
#[tokio::test]
async fn partial_commit_delete_thread_keeps_earlier_chunks() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"value":[{"id":"a","parentFolderId":"TrashId","changeKey":"a-tag"},{"id":"b","parentFolderId":"TrashId","changeKey":"b-tag"}]})),
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"id":"TrashId"})),
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"responses":[{"id":"0","status":204}]})),
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"responses":[{"id":"0","status":403,"body":{"error":{"code":"ErrorAccessDenied"}}}]})),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.capabilities.batching_policy.max_items = 1;
    let error = delete_thread(
        account,
        ThreadId("conversation".into()),
        Some(ContainerId("TrashId".into())),
    )
    .await
    .unwrap_err();
    let record = error.partial_commit().unwrap();
    assert_eq!(record.steps().len(), 1);
    assert_eq!(
        record.steps()[0].effect(),
        Some(bifrost_types::StepEffect::Deleted)
    );
    assert_eq!(
        record.steps()[0].targets().refs[0].scope,
        ErrorScope::Message {
            id: ObjectId("a".into())
        }
    );
    assert_eq!(client.take_rest_requests().len(), 4);
}

/// A chunk refused locally, before its `$batch` is sent, still reports the
/// chunks already committed. The refusal comes from `message_batch_url`: the
/// server named a member id holding the shared-mailbox separator, so it
/// parses as a message of a mailbox this account does not have.
#[tokio::test]
async fn partial_commit_delete_thread_keeps_earlier_chunks_on_a_local_refusal() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"value":[
                {"id":"a","parentFolderId":"TrashId","changeKey":"a-tag"},
                {"id":"rogue\u{1f}b","parentFolderId":"TrashId","changeKey":"b-tag"}
            ]}),
        ),
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({"id":"TrashId"})),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"responses":[{"id":"0","status":204}]}),
        ),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.capabilities.batching_policy.max_items = 1;
    let error = delete_thread(
        account,
        ThreadId("conversation".into()),
        Some(ContainerId("TrashId".into())),
    )
    .await
    .unwrap_err();
    assert_eq!(
        client.take_rest_requests().len(),
        3,
        "the refused chunk is never sent"
    );
    let record = error
        .partial_commit()
        .expect("the first chunk's destroy committed");
    assert_eq!(
        record.primary().application,
        bifrost_types::Application::NotApplied
    );
    assert_eq!(
        record.primary().targets.refs[0].scope,
        ErrorScope::Message {
            id: ObjectId("rogue\u{1f}b".into())
        }
    );
    assert_eq!(record.steps().len(), 1);
    assert_eq!(
        record.steps()[0].effect(),
        Some(bifrost_types::StepEffect::Deleted)
    );
    assert_eq!(
        record.steps()[0].targets().refs[0].scope,
        ErrorScope::Message {
            id: ObjectId("a".into())
        }
    );
}
