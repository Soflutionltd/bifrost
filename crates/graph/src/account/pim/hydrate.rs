//! Typed hydration: the message read doors and the projections from
//! Graph JSON and EWS items onto `Message`.

use crate::account::GraphAccount;
use crate::account::blob::blob_handle_from_graph_attachment;
use crate::account::graph_error::GraphErrorContext;
use bifrost_types::{
    AccountError, AccountOperation, Address, AttachmentSource, ContainerId, ErrorScope, FolderId,
    HydrationProjection, Importance, Message, MessageAttachment, ObjectId, ThreadId,
};
use serde_json::Value;
use std::collections::HashSet;
use std::time::SystemTime;

use super::common::*;
use super::messages::*;

pub(crate) async fn message_hydrate(
    account: GraphAccount,
    message: ObjectId,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    // The single-id hydration door has to make the SAME transport decision
    // the batch door (`get.rs::partition_ews_ids`) makes: a public-folder id
    // wraps a raw EWS `ItemId`, and no Graph REST route can address one.
    // Sending it down `/me/messages/{native}` 404s naming the bare item id,
    // which is precisely what a consumer reaching hydration through the
    // engine's passthrough saw.
    if let Some(folder) = ews_read_folder(&message) {
        return public_message_hydrate(&account, &message, &folder, projection).await;
    }
    let value = fetch_message_value(
        &account,
        &message,
        hydrate_select(includes_attachment_metadata(projection)),
    )
    .await?;
    let owner = crate::account::foreign::parse_message_id(&message)
        .owner()
        .map(str::to_string);
    message_from_value(&value, projection, owner.as_deref())
}

/// The public folder a read of `id` must route through EWS `GetItem`, or
/// `None` when the id reads over Graph REST (primary or shared-mailbox).
///
/// The transport decision lives in the id itself: `PUBLIC_SEP` is the
/// discriminator, and the folder it carries is the `routing_map` key holding
/// the item's `X-AnchorMailbox` / `X-PublicFolderMailbox` pair. Extracted so
/// every door (batch hydrate, single hydrate, blob read) decides identically
/// and the decision is unit-pinnable without a live server.
pub(crate) fn ews_read_folder(id: &ObjectId) -> Option<FolderId> {
    crate::account::foreign::parse_message_id(id)
        .public_folder()
        .map(|folder| FolderId(folder.to_string()))
}

/// Hydrate one public-folder item over EWS `GetItem`, routed by its folder's
/// `routing_map` headers - the single-id peer of `get.rs::fetch_ews_outcomes`.
pub(super) async fn public_message_hydrate(
    account: &GraphAccount,
    id: &ObjectId,
    folder: &FolderId,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    // The routing map starts empty on every reopen and is filled by
    // public-folder discovery, which can skip a folder after a per-folder
    // failure, so a legitimately minted, persisted id can miss here. Refused
    // before any EWS request, and neither the caller's fault nor the
    // provider's: local state that no longer matches the id.
    let Some(routing) = account.public_folder_routing(folder).await else {
        return Err(crate::account::graph_error::stale_local_state_error(
            AccountOperation::HydrateMessage,
            ErrorScope::Message {
                id: id.0.clone().into(),
            },
            format!("public folder {} has no routing entry", folder.0),
        ));
    };
    let ews = crate::account::public_folder::ews_client(account).map_err(|error| {
        crate::account::graph_error::ews_error_to_account_error(
            error,
            GraphErrorContext::ews(AccountOperation::HydrateMessage).with_scope(
                ErrorScope::Message {
                    id: id.0.clone().into(),
                },
            ),
        )
    })?;
    let native = crate::account::foreign::parse_message_id(id)
        .native_id()
        .to_string();
    // Same class-conditional property shape the batch door uses, so both
    // doors ask a non-mail public-folder item for a set it accepts.
    let shape = account.public_item_shape(id, folder).await;
    let item = ews
        .get_item(&native, shape, &routing.headers())
        .await
        .map_err(|error| {
            crate::account::graph_error::ews_error_to_account_error(
                error,
                GraphErrorContext::ews(AccountOperation::HydrateMessage).with_scope(
                    ErrorScope::Message {
                        id: id.0.clone().into(),
                    },
                ),
            )
        })?;
    Ok(message_from_ews_item(id.clone(), &item, folder, projection))
}

/// Project an EWS `GetItem` result into the user-facing `Message` shape.
///
/// EWS returns a parsed body, not MIME octets, so `body_html` carries the
/// HTML part and `body_text` the `BodyPreview` text. Attachments ride out as
/// metadata plus EWS blob handles (`GetAttachment` fetches the bytes), and
/// the containing public folder is the item's one membership. Pure over the
/// already-fetched item so the projection is unit-pinnable without a live EWS
/// server.
pub(super) fn message_from_ews_item(
    id: ObjectId,
    item: &crate::ews::EwsItem,
    folder: &FolderId,
    projection: HydrationProjection,
) -> Message {
    let (body_text, body_html) = match projection {
        HydrationProjection::Headers => (None, None),
        HydrationProjection::Preview(limit) => (
            item.body_preview
                .as_ref()
                .map(|preview| preview.chars().take(limit).collect()),
            None,
        ),
        _ => (item.body_preview.clone(), item.body_html.clone()),
    };
    let attachments = if includes_attachment_metadata(projection) {
        item.attachments
            .iter()
            .map(|attachment| message_attachment_from_ews_attachment(&id, attachment))
            .collect()
    } else {
        Vec::new()
    };
    let mut flags = HashSet::new();
    if item.is_read {
        flags.insert("\\seen".to_string());
    }
    Message {
        id,
        // EWS `GetItem` on the message shape returns no conversation id,
        // and a public item has no Graph conversation to join.
        thread_id: None,
        from: item
            .sender_email
            .iter()
            .map(|address| Address {
                name: item.sender_name.clone(),
                address: address.clone(),
            })
            .collect(),
        to: item.to_recipients.iter().map(ews_address).collect(),
        cc: item.cc_recipients.iter().map(ews_address).collect(),
        bcc: Vec::new(),
        reply_to: Vec::new(),
        subject: item.subject.clone(),
        date: item.received_at.as_deref().and_then(parse_graph_datetime),
        containers: vec![ContainerId(folder.0.clone())],
        flags,
        // EWS surfaces only the read bit on this shape; no importance field
        // is requested, so every public item is Normal.
        importance: Importance::Normal,
        body_text,
        body_html,
        attachments,
        incomplete: false,
        size_bytes: None,
        in_reply_to: None,
        references: Vec::new(),
    }
}

pub(super) fn ews_address(recipient: &crate::ews::EwsRecipient) -> Address {
    Address {
        name: recipient.name.clone(),
        address: recipient.email.clone(),
    }
}

pub(super) fn select_query(select: &str) -> String {
    if select.contains("$expand") {
        select.to_string()
    } else {
        format!("$select={select}")
    }
}

pub(super) fn hydrate_select(include_attachments: bool) -> &'static str {
    if include_attachments {
        "$select=id,conversationId,subject,bodyPreview,body,uniqueBody,from,toRecipients,ccRecipients,bccRecipients,replyTo,receivedDateTime,sentDateTime,parentFolderId,isRead,importance,categories,flag,internetMessageHeaders,internetMessageId,hasAttachments,changeKey&$expand=attachments,singleValueExtendedProperties($filter=id%20eq%20'SystemTime%200x0F01')"
    } else {
        "$select=id,conversationId,subject,bodyPreview,body,uniqueBody,from,toRecipients,ccRecipients,bccRecipients,replyTo,receivedDateTime,sentDateTime,parentFolderId,isRead,importance,categories,flag,internetMessageHeaders,internetMessageId,hasAttachments,changeKey&$expand=singleValueExtendedProperties($filter=id%20eq%20'SystemTime%200x0F01')"
    }
}

pub(super) fn includes_attachment_metadata(projection: HydrationProjection) -> bool {
    matches!(
        projection,
        HydrationProjection::Full | HydrationProjection::FullWithBlobs
    )
}

pub(super) fn message_from_value(
    value: &Value,
    projection: HydrationProjection,
    owner: Option<&str>,
) -> Result<Message, AccountError> {
    let id = object_id_from_value(value, AccountOperation::HydrateMessage, owner)?;
    let body = value.get("body");
    let (mut body_text, mut body_html) = match body {
        Some(body) => body_parts_from_graph_body(body),
        None => (None, None),
    };
    if matches!(projection, HydrationProjection::Headers) {
        body_text = None;
        body_html = None;
    }
    if let HydrationProjection::Preview(limit) = projection {
        body_text = value
            .get("bodyPreview")
            .and_then(Value::as_str)
            .map(|preview| preview.chars().take(limit).collect());
        body_html = None;
    }
    if matches!(
        projection,
        HydrationProjection::FullWithBlobs | HydrationProjection::Full
    ) {
        body_text = body_text.or_else(|| {
            value
                .get("bodyPreview")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    }
    let attachments = if includes_attachment_metadata(projection) {
        value
            .get("attachments")
            .and_then(Value::as_array)
            .map(|attachments| {
                attachments
                    .iter()
                    .filter_map(|attachment| {
                        message_attachment_from_graph_attachment(&id, attachment)
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    Ok(Message {
        id,
        thread_id: value
            .get("conversationId")
            .and_then(Value::as_str)
            .map(|id| ThreadId(crate::account::foreign::qualify_with_owner(owner, id))),
        from: value
            .get("from")
            .and_then(address_from_recipient)
            .into_iter()
            .collect(),
        to: addresses_from_array(value.get("toRecipients")),
        cc: addresses_from_array(value.get("ccRecipients")),
        bcc: addresses_from_array(value.get("bccRecipients")),
        reply_to: addresses_from_array(value.get("replyTo")),
        subject: value
            .get("subject")
            .and_then(Value::as_str)
            .map(str::to_string),
        date: graph_message_date(value),
        // `parentFolderId` arrives bare from Graph even when the request
        // went to `/users/{owner}`, so it must be re-qualified here.
        // `containers_list` emits a shared mailbox's folders as
        // `encode_foreign(mailbox, folderId)`; a bare id could neither join
        // that list nor be told apart from a primary folder carrying the
        // same bytes.
        containers: value
            .get("parentFolderId")
            .and_then(Value::as_str)
            .map(|id| {
                vec![ContainerId(crate::account::foreign::qualify_with_owner(
                    owner, id,
                ))]
            })
            .unwrap_or_default(),
        flags: flags_from_message(value),
        importance: importance_from_graph(value),
        body_text,
        body_html,
        attachments,
        incomplete: false,
        size_bytes: value.get("size").and_then(Value::as_u64),
        in_reply_to: internet_header(value, "In-Reply-To"),
        references: internet_header(value, "References")
            .map(|header| header.split_whitespace().map(str::to_string).collect())
            .unwrap_or_default(),
    })
}

/// Map Graph's attachment descriptor onto the user-facing attachment shape.
///
/// Graph exposes attachment bytes through `open_blob`, so both full hydration
/// projections retain the handle instead of inlining potentially large files.
/// Metadata is carried separately because `BlobHandle` deliberately describes
/// only the byte-fetch capability, not a rendered attachment.
pub(super) fn message_attachment_from_graph_attachment(
    message_id: &ObjectId,
    attachment: &Value,
) -> Option<MessageAttachment> {
    let handle = blob_handle_from_graph_attachment(message_id, attachment)?;
    Some(MessageAttachment {
        filename: attachment
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string),
        content_type: attachment
            .get("contentType")
            .and_then(Value::as_str)
            .map(str::to_string),
        content_id: attachment
            .get("contentId")
            .and_then(Value::as_str)
            .map(str::to_string),
        inline: attachment
            .get("isInline")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        size: attachment.get("size").and_then(Value::as_u64),
        source: AttachmentSource::Blob(handle),
        truncated: false,
    })
}

/// Map an EWS public-folder attachment descriptor onto the shared attachment
/// shape. EWS exposes the same filename, media type, inline flag, and size as
/// Graph REST, but its `GetItem` descriptor has no Content-ID field.
pub(super) fn message_attachment_from_ews_attachment(
    message_id: &ObjectId,
    attachment: &crate::ews::EwsAttachment,
) -> MessageAttachment {
    MessageAttachment {
        filename: attachment.name.clone(),
        content_type: attachment.content_type.clone(),
        content_id: None,
        inline: attachment.is_inline,
        size: attachment.size,
        source: AttachmentSource::Blob(crate::account::blob::blob_handle_from_ews_attachment(
            message_id, attachment,
        )),
        truncated: false,
    }
}

/// Map Graph's single-valued `importance` wire field
/// (`low|normal|high`) onto the uniform `Importance` enum. Absent or
/// unrecognized -> `Normal`.
pub(super) fn importance_from_graph(value: &Value) -> Importance {
    match value.get("importance").and_then(Value::as_str) {
        Some(level) if level.eq_ignore_ascii_case("low") => Importance::Low,
        Some(level) if level.eq_ignore_ascii_case("high") => Importance::High,
        _ => Importance::Normal,
    }
}

pub(super) fn body_parts_from_graph_body(body: &Value) -> (Option<String>, Option<String>) {
    let content = body
        .get("content")
        .and_then(Value::as_str)
        .map(str::to_string);
    let content_type = body
        .get("contentType")
        .and_then(Value::as_str)
        .unwrap_or("text");
    if content_type.eq_ignore_ascii_case("html") {
        (None, content)
    } else {
        (content, None)
    }
}

pub(super) fn address_from_recipient(value: &Value) -> Option<Address> {
    let email = value.get("emailAddress")?;
    let address = email.get("address")?.as_str()?.to_string();
    let name = email
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    Some(Address { name, address })
}

pub(super) fn addresses_from_array(value: Option<&Value>) -> Vec<Address> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(address_from_recipient).collect())
        .unwrap_or_default()
}

pub(super) fn graph_message_date(value: &Value) -> Option<SystemTime> {
    value
        .get("receivedDateTime")
        .and_then(Value::as_str)
        .or_else(|| value.get("sentDateTime").and_then(Value::as_str))
        .and_then(parse_graph_datetime)
}

pub(super) fn flags_from_message(value: &Value) -> HashSet<String> {
    let mut flags = HashSet::new();
    if value
        .get("isRead")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        flags.insert("\\seen".to_string());
    }
    if value
        .get("flag")
        .and_then(|flag| flag.get("flagStatus"))
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("flagged"))
    {
        flags.insert("\\flagged".to_string());
    }
    if crate::account::pin::is_pinned(value) {
        flags.insert(crate::account::pin::PINNED_FLAG.to_string());
    }
    for category in categories_from_value(value) {
        flags.insert(format!("category:{category}"));
    }
    flags
}

pub(super) fn internet_header(value: &Value, name: &str) -> Option<String> {
    value
        .get("internetMessageHeaders")?
        .as_array()?
        .iter()
        .find_map(|header| {
            let header_name = header.get("name")?.as_str()?;
            header_name.eq_ignore_ascii_case(name).then(|| {
                header
                    .get("value")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            })
        })
}
