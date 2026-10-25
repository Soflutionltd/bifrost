//! Draft lifecycle (create, update, discard, send) and the
//! `DraftPatch` / `SendRequest` to Graph message-body projections.

use crate::account::GraphAccount;
use crate::account::GraphClient;
use crate::account::graph_error::{
    GraphErrorContext, into_account_error, unsupported_account_error,
};
use crate::client::Resource;
use base64::Engine;
use bifrost_types::{
    AccountError, AccountOperation, Address, AttachmentInline, DraftHandle, DraftPatch, ObjectId,
};
use serde_json::{Map, Value, json};

use super::common::*;

pub(crate) async fn draft_create(
    account: GraphAccount,
    patch: DraftPatch,
) -> Result<DraftHandle, AccountError> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(unsupported_account_error(AccountOperation::DraftCreate));
    }
    let message = message_from_draft_patch(&patch, true)?;
    create_draft_message(&account.client, message).await
}

pub(crate) async fn draft_update(
    account: GraphAccount,
    draft: DraftHandle,
    patch: DraftPatch,
) -> Result<(), AccountError> {
    if patch.attachments_inline.is_some() || patch.attachments_uploaded.is_some() {
        return Err(unsupported_account_error(AccountOperation::DraftUpdate));
    }
    let message = message_from_draft_patch(&patch, false)?;
    let path = format!(
        "{}/messages/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_path_component(&draft.0)
    );
    account
        .client
        .patch(
            Resource::Outlook,
            &path,
            &message,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::DraftUpdate)))
}

pub(crate) async fn draft_discard(
    account: GraphAccount,
    draft: DraftHandle,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/messages/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_path_component(&draft.0)
    );
    account
        .client
        .delete(Resource::Outlook, &path, bifrost_net::ReplayPolicy::Never)
        .await
        .map_err(|e| {
            into_account_error(e, GraphErrorContext::graph(AccountOperation::DraftDiscard))
        })
}

pub(crate) async fn draft_send(
    account: GraphAccount,
    draft: DraftHandle,
) -> Result<ObjectId, AccountError> {
    send_draft_message(&account.client, &draft).await?;
    Ok(ObjectId(draft.0))
}

pub(super) fn message_from_send_request(
    request: &bifrost_types::SendRequest,
) -> Result<Value, AccountError> {
    let mut patch = DraftPatch::default();
    patch.identity.clone_from(&request.identity);
    patch.from = request.from.clone().map(Some);
    patch.to = Some(request.to.clone());
    patch.cc = Some(request.cc.clone());
    patch.bcc = Some(request.bcc.clone());
    patch.reply_to = Some(request.reply_to.clone());
    patch.subject = Some(request.subject.clone());
    patch.body_text = Some(request.body_text.clone());
    patch.body_html = Some(request.body_html.clone());
    patch.attachments_inline = Some(request.attachments_inline.clone());
    patch.attachments_uploaded = Some(request.attachments_uploaded.clone());
    patch.in_reply_to = request.in_reply_to.clone().map(Some);
    patch.references = Some(request.references.clone());
    let mut message = message_from_draft_patch(&patch, true)?;
    // RFC 8098 read receipt: Graph models this as the message-level
    // `isReadReceiptRequested` bit rather than a MIME header. Set it only
    // when requested so existing sends are unaffected.
    if request.request_read_receipt
        && let Some(map) = message.as_object_mut()
    {
        map.insert("isReadReceiptRequested".to_string(), Value::Bool(true));
    }
    Ok(message)
}

pub(super) fn message_from_draft_patch(
    patch: &DraftPatch,
    include_empty: bool,
) -> Result<Value, AccountError> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(unsupported_account_error(AccountOperation::DraftCreate));
    }
    let mut message = Map::new();
    if let Some(from) = &patch.from {
        insert_nullable_address(&mut message, "from", from);
    }
    insert_recipient_list(
        &mut message,
        "toRecipients",
        patch.to.as_ref(),
        include_empty,
    );
    insert_recipient_list(
        &mut message,
        "ccRecipients",
        patch.cc.as_ref(),
        include_empty,
    );
    insert_recipient_list(
        &mut message,
        "bccRecipients",
        patch.bcc.as_ref(),
        include_empty,
    );
    insert_recipient_list(
        &mut message,
        "replyTo",
        patch.reply_to.as_ref(),
        include_empty,
    );
    if let Some(subject) = &patch.subject {
        message.insert(
            "subject".to_string(),
            Value::String(subject.clone().unwrap_or_default()),
        );
    } else if include_empty {
        message.insert("subject".to_string(), Value::String(String::new()));
    }
    insert_body(&mut message, patch, include_empty);
    if let Some(attachments) = &patch.attachments_inline {
        message.insert(
            "attachments".to_string(),
            Value::Array(
                attachments
                    .iter()
                    .map(graph_attachment_from_inline)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        );
    }
    Ok(Value::Object(message))
}

pub(super) fn insert_nullable_address(
    map: &mut Map<String, Value>,
    key: &str,
    address: &Option<Address>,
) {
    let value = address.as_ref().map(graph_recipient).unwrap_or(Value::Null);
    map.insert(key.to_string(), value);
}

pub(super) fn insert_recipient_list(
    map: &mut Map<String, Value>,
    key: &str,
    addresses: Option<&Vec<Address>>,
    include_empty: bool,
) {
    if let Some(addresses) = addresses {
        map.insert(
            key.to_string(),
            Value::Array(addresses.iter().map(graph_recipient).collect()),
        );
    } else if include_empty {
        map.insert(key.to_string(), Value::Array(Vec::new()));
    }
}

pub(super) fn insert_body(map: &mut Map<String, Value>, patch: &DraftPatch, include_empty: bool) {
    let body = match (&patch.body_html, &patch.body_text) {
        (Some(Some(html)), _) => Some(("HTML", html.clone())),
        (Some(None), _) => Some(("HTML", String::new())),
        (_, Some(Some(text))) => Some(("Text", text.clone())),
        (_, Some(None)) => Some(("Text", String::new())),
        _ if include_empty => Some(("Text", String::new())),
        _ => None,
    };
    if let Some((content_type, content)) = body {
        map.insert(
            "body".to_string(),
            json!({ "contentType": content_type, "content": content }),
        );
    }
}

pub(super) fn graph_recipient(address: &Address) -> Value {
    let mut email = Map::new();
    email.insert(
        "address".to_string(),
        Value::String(address.address.clone()),
    );
    if let Some(name) = &address.name {
        email.insert("name".to_string(), Value::String(name.clone()));
    }
    json!({ "emailAddress": email })
}

pub(super) fn graph_attachment_from_inline(
    attachment: &AttachmentInline,
) -> Result<Value, AccountError> {
    let content_bytes = base64::engine::general_purpose::STANDARD.encode(&attachment.data);
    let mut obj = json!({
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": attachment.filename.clone(),
        "contentType": attachment.mime.clone(),
        "isInline": attachment.inline,
        "contentBytes": content_bytes
    });
    // Stamp the contentId so a `cid:` reference in the HTML body resolves
    // to this fileAttachment (Graph's inline-image linkage). Bare value,
    // angle brackets stripped, matching the structured-send contract.
    let content_id = attachment
        .content_id
        .as_deref()
        .map(|cid| {
            cid.trim()
                .trim_matches(|c| c == '<' || c == '>')
                .to_string()
        })
        .filter(|cid| !cid.is_empty());
    if let Some(content_id) = content_id
        && let Some(map) = obj.as_object_mut()
    {
        map.insert("contentId".to_string(), Value::String(content_id));
    }
    Ok(obj)
}

pub(super) async fn create_draft_message(
    client: &GraphClient,
    message: Value,
) -> Result<DraftHandle, AccountError> {
    let path = format!("{}/messages", client.api_path_prefix());
    let created: Value = client
        .post(
            Resource::Outlook,
            &path,
            &message,
            bifrost_net::ReplayPolicy::Never,
        )
        .await
        .map_err(|e| {
            into_account_error(e, GraphErrorContext::graph(AccountOperation::DraftCreate))
        })?;
    let id = created.get("id").and_then(Value::as_str).ok_or_else(|| {
        pim_protocol_error(
            AccountOperation::DraftCreate,
            None,
            "Graph draft create did not return an id",
        )
    })?;
    Ok(DraftHandle(id.to_string()))
}

pub(super) async fn send_draft_message(
    client: &GraphClient,
    draft: &DraftHandle,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/messages/{}/send",
        client.api_path_prefix(),
        bifrost_net::url::encode_path_component(&draft.0)
    );
    client
        .post_empty(Resource::Outlook, &path, bifrost_net::ReplayPolicy::Never)
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::Send)))
}
