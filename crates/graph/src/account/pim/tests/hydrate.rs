use crate::account::foreign::{encode_foreign, encode_message_id, encode_public_item_id};
use crate::account::pim::hydrate::{
    ews_read_folder, importance_from_graph, message_from_ews_item, message_from_value,
    message_hydrate,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::GraphClient;
use bifrost_types::{
    AccountOperation, AttachmentSource, CursorScope, FolderId, HydrationProjection, Importance,
    MembershipScope, ObjectId, ObjectType,
};
use bytes::Bytes;
use serde_json::json;

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
    account
        .public_folders
        .record_public_item_classes(&folder, &[item])
        .await;

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

#[test]
fn typed_hydration_preserves_inventory_folder_memberships() {
    let value = json!({ "id": "m1", "parentFolderId": "inbox" });
    for owner in [None, Some("shared@contoso.com")] {
        let folder = owner.map_or_else(
            || FolderId("inbox".into()),
            |owner| encode_foreign(owner, "inbox"),
        );
        let scope = CursorScope::FolderType {
            folder: folder.clone(),
            ty: ObjectType::Email,
        };
        let inventory = crate::account::inventory::inventory_entry_from_value(&scope, &value)
            .expect("mail inventory entry");
        let hydrated = message_from_value(&value, HydrationProjection::Headers, owner).unwrap();
        assert_eq!(hydrated.memberships, inventory.memberships);
        assert_eq!(hydrated.memberships, vec![MembershipScope::Folder(folder)]);
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
    assert_eq!(message.memberships, vec![MembershipScope::Folder(folder)]);
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
