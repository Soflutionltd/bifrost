//! EWS read-operation result types and quick-xml response parsers.
//!
//! Copied and adapted from ratatoskr's `ews/parsers.rs` + `ews/mod.rs`:
//! the only reshaping is the error boundary. Where ratatoskr returned a
//! bare `String`, these return `EwsError` so the account layer routes
//! the parse miss through `ews_error_to_account_error` as
//! `Protocol(ParseFailed)` (a `MalformedXml` carrier), identical in
//! shape to `check_soap_fault`. Every type is `pub(crate)`: the cursor
//! payload and routing stay private to the graph crate.
//!
//! Some result-struct fields and the `GetItem` parser are part of this
//! foundation but only consumed by the hydration/discovery paths that
//! wire on top of the read ops; they are parsed in full now so the data
//! is ready when those callers land.
#![allow(dead_code)]

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bifrost_types::DiagnosticText;
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;

use super::EwsError;
use super::xml_helpers::{extract_attribute, strip_ns, try_push_general_ref};

// ── Result types ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct EwsFolder {
    pub(crate) folder_id: String,
    pub(crate) display_name: String,
    pub(crate) folder_class: Option<String>,
    pub(crate) total_count: u32,
    pub(crate) unread_count: u32,
    pub(crate) child_folder_count: u32,
    pub(crate) effective_rights: EwsEffectiveRights,
    pub(crate) replica_list: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct EwsEffectiveRights {
    pub(crate) create_associated: bool,
    pub(crate) create_contents: bool,
    pub(crate) create_hierarchy: bool,
    pub(crate) delete: bool,
    pub(crate) modify: bool,
    pub(crate) read: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct EwsItem {
    pub(crate) wire_kind: String,
    pub(crate) item_id: String,
    pub(crate) change_key: Option<String>,
    pub(crate) subject: Option<String>,
    pub(crate) sender_email: Option<String>,
    pub(crate) sender_name: Option<String>,
    /// ISO-8601 `DateTimeReceived` as the server reported it.
    pub(crate) received_at: Option<String>,
    pub(crate) body_preview: Option<String>,
    pub(crate) body_html: Option<String>,
    pub(crate) is_read: bool,
    pub(crate) flag_status: Option<String>,
    pub(crate) categories: Vec<String>,
    pub(crate) item_class: String,
    pub(crate) to_recipients: Vec<EwsRecipient>,
    pub(crate) cc_recipients: Vec<EwsRecipient>,
    /// Attachment METADATA only (`GetItem` returns descriptors, never
    /// bytes). The bytes come from a separate `GetAttachment` call.
    pub(crate) attachments: Vec<EwsAttachment>,
}

#[derive(Debug, Clone)]
pub(crate) struct EwsRecipient {
    pub(crate) email: String,
    pub(crate) name: Option<String>,
}

/// One attachment descriptor from a `GetItem` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsAttachment {
    pub(crate) attachment_id: String,
    pub(crate) name: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) size: Option<u64>,
    pub(crate) is_inline: bool,
    /// `true` for `<t:ItemAttachment>`: an embedded Exchange item, not a
    /// byte stream. `GetAttachment` returns it as XML, so it is surfaced as
    /// a non-byte-stream blob handle rather than a downloadable one.
    pub(crate) is_item: bool,
}

/// The bytes of one attachment, from a `GetAttachment` response.
#[derive(Debug, Clone)]
pub(crate) struct EwsAttachmentContent {
    pub(crate) attachment_id: String,
    pub(crate) name: Option<String>,
    pub(crate) content_type: Option<String>,
    /// Decoded `<t:Content>` octets. Empty for an `ItemAttachment` (whose
    /// payload is a nested XML item, not base64 content).
    pub(crate) content: Vec<u8>,
}

pub(crate) struct FindItemsResult {
    pub(crate) items: Vec<EwsItem>,
    pub(crate) total_count: u32,
    pub(crate) includes_last: bool,
    /// `RootFolder/@IndexedPagingOffset`: the server's own next-page
    /// offset (in wire rows, counting every class), or `None` when the
    /// server omits it (typically on the last page). Paging must advance
    /// off this, never off `items.len()` - the parser drops unhandled
    /// classes, so a parsed-item count under-advances a mixed page.
    pub(crate) next_offset: Option<u32>,
    /// Item-class element local-names seen in this page that the parser
    /// does not collect (e.g. `Task`, `MeetingRequest`, `DistributionList`,
    /// `PostItem`). Deduped, source order. The caller surfaces these as a
    /// scoped `Warning` so the silent omission is visible.
    pub(crate) unhandled_classes: Vec<String>,
}

fn malformed(detail: impl Into<String>) -> EwsError {
    EwsError::MalformedXml(DiagnosticText::support_only(detail.into()))
}

// ── PR_REPLICA_LIST decoding ────────────────────────────────

/// Decode `PR_REPLICA_LIST` (0x6698) binary data into GUID strings.
///
/// The binary format is a sequence of null-terminated ASCII hex GUID
/// strings, each `{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}\0`.
pub(crate) fn decode_replica_list(base64_data: &str) -> Result<Vec<String>, EwsError> {
    let bytes = BASE64
        .decode(base64_data)
        .map_err(|e| malformed(format!("Failed to decode base64 replica list: {e}")))?;
    Ok(decode_replica_bytes(&bytes))
}

/// Parse already-decoded `PR_REPLICA_LIST` bytes into GUID strings. The
/// `GetFolder` parser already base64-decoded the `<t:Value>` into raw
/// bytes, so callers holding the bytes use this directly rather than
/// re-encoding to base64 only to have `decode_replica_list` decode it
/// straight back.
pub(crate) fn decode_replica_bytes(bytes: &[u8]) -> Vec<String> {
    let mut guids = Vec::new();
    let mut start = 0;

    for (i, &b) in bytes.iter().enumerate() {
        if b == 0 {
            if i > start
                && let Ok(s) = std::str::from_utf8(&bytes[start..i])
            {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    guids.push(trimmed.to_string());
                }
            }
            start = i + 1;
        }
    }

    guids
}

// ── Response parsers ────────────────────────────────────────

pub(crate) fn parse_find_folder_response(xml: &str) -> Result<Vec<EwsFolder>, EwsError> {
    let mut reader = Reader::from_str(xml);
    let mut folders = Vec::new();

    let mut in_folder = false;
    let mut in_effective_rights = false;
    let mut current_tag = String::new();
    let mut buf = String::new();

    let mut folder_id = String::new();
    let mut display_name = String::new();
    let mut folder_class: Option<String> = None;
    let mut total_count: u32 = 0;
    let mut unread_count: u32 = 0;
    let mut child_folder_count: u32 = 0;
    let mut rights = EwsEffectiveRights::default();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);

                if is_folder_tag(local) {
                    in_folder = true;
                    folder_id.clear();
                    display_name.clear();
                    folder_class = None;
                    total_count = 0;
                    unread_count = 0;
                    child_folder_count = 0;
                    rights = EwsEffectiveRights::default();
                }
                if in_folder && local == "EffectiveRights" {
                    in_effective_rights = true;
                }
                current_tag = local.to_string();
                buf.clear();

                if in_folder && local == "FolderId" {
                    folder_id = attribute(e, "Id")?;
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                if in_folder && local == "FolderId" {
                    folder_id = attribute(e, "Id")?;
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => push_ref(e, &mut buf)?,
            Ok(Event::End(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if in_effective_rights {
                    apply_effective_right(&mut rights, &current_tag, trimmed);
                    if local == "EffectiveRights" {
                        in_effective_rights = false;
                    }
                } else if in_folder {
                    match current_tag.as_str() {
                        "DisplayName" => display_name = trimmed.to_string(),
                        "FolderClass" => folder_class = Some(trimmed.to_string()),
                        "TotalCount" => total_count = trimmed.parse().unwrap_or(0),
                        "UnreadCount" => unread_count = trimmed.parse().unwrap_or(0),
                        "ChildFolderCount" => child_folder_count = trimmed.parse().unwrap_or(0),
                        _ => {}
                    }
                }

                if is_folder_tag(local) && in_folder {
                    if !folder_id.is_empty() {
                        folders.push(EwsFolder {
                            folder_id: folder_id.clone(),
                            display_name: display_name.clone(),
                            folder_class: folder_class.clone(),
                            total_count,
                            unread_count,
                            child_folder_count,
                            effective_rights: rights.clone(),
                            replica_list: None,
                        });
                    }
                    in_folder = false;
                }

                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("FindFolder parse failed: {e}"))),
            _ => {}
        }
    }

    Ok(folders)
}

pub(crate) fn parse_get_folder_response(xml: &str) -> Result<EwsFolder, EwsError> {
    let mut reader = Reader::from_str(xml);

    let mut in_folder = false;
    let mut in_effective_rights = false;
    let mut in_extended_property = false;
    // PropertyTag of the current ExtendedProperty's ExtendedFieldURI.
    // The Value is only treated as the replica list when this is the
    // PR_REPLICA_LIST tag (0x6698), so any other ExtendedProperty whose
    // Value happens to be base64-decodable cannot be mis-assigned.
    let mut current_property_tag = String::new();
    let mut current_tag = String::new();
    let mut buf = String::new();

    let mut folder_id = String::new();
    let mut display_name = String::new();
    let mut folder_class: Option<String> = None;
    let mut total_count: u32 = 0;
    let mut unread_count: u32 = 0;
    let mut child_folder_count: u32 = 0;
    let mut rights = EwsEffectiveRights::default();
    let mut replica_list: Option<Vec<u8>> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);

                if is_folder_tag(local) {
                    in_folder = true;
                }
                if in_folder && local == "EffectiveRights" {
                    in_effective_rights = true;
                }
                if in_folder && local == "ExtendedProperty" {
                    in_extended_property = true;
                    current_property_tag.clear();
                }
                if in_extended_property && local == "ExtendedFieldURI" {
                    current_property_tag = attribute(e, "PropertyTag")?;
                }
                current_tag = local.to_string();
                buf.clear();

                if in_folder && local == "FolderId" {
                    folder_id = attribute(e, "Id")?;
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                if in_folder && local == "FolderId" {
                    folder_id = attribute(e, "Id")?;
                }
                if in_extended_property && local == "ExtendedFieldURI" {
                    current_property_tag = attribute(e, "PropertyTag")?;
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => push_ref(e, &mut buf)?,
            Ok(Event::End(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if in_effective_rights {
                    apply_effective_right(&mut rights, &current_tag, trimmed);
                    if local == "EffectiveRights" {
                        in_effective_rights = false;
                    }
                } else if in_extended_property {
                    if current_tag == "Value"
                        && is_replica_list_tag(&current_property_tag)
                        && !trimmed.is_empty()
                        && let Ok(bytes) = BASE64.decode(trimmed)
                    {
                        replica_list = Some(bytes);
                    }
                    if local == "ExtendedProperty" {
                        in_extended_property = false;
                    }
                } else if in_folder {
                    match current_tag.as_str() {
                        "DisplayName" => display_name = trimmed.to_string(),
                        "FolderClass" => folder_class = Some(trimmed.to_string()),
                        "TotalCount" => total_count = trimmed.parse().unwrap_or(0),
                        "UnreadCount" => unread_count = trimmed.parse().unwrap_or(0),
                        "ChildFolderCount" => child_folder_count = trimmed.parse().unwrap_or(0),
                        _ => {}
                    }
                }

                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("GetFolder parse failed: {e}"))),
            _ => {}
        }
    }

    if folder_id.is_empty() {
        return Err(malformed("No folder found in GetFolder response"));
    }

    Ok(EwsFolder {
        folder_id,
        display_name,
        folder_class,
        total_count,
        unread_count,
        child_folder_count,
        effective_rights: rights,
        replica_list,
    })
}

pub(crate) fn parse_find_items_response(xml: &str) -> Result<FindItemsResult, EwsError> {
    let mut reader = Reader::from_str(xml);
    // Item-collection flips on any class in `is_item_tag` (Message /
    // CalendarItem / Contact), so mail, calendar, and contact public
    // folders all sync at the identity level. `total_count`
    // (`TotalItemsInView`) counts EVERY class, so `items.len()` can still
    // be < `total_count` when a folder holds classes this parser does not
    // yet collect (e.g. `Task` from an `IPF.Task` folder). That stays
    // consistent for the public-folder cursor diff: the inventory
    // establish pass and the incremental poll run this same parser, so the
    // live-id baseline and the poll observe the identical set of tracked
    // classes.
    let mut items = Vec::new();

    let mut total_count: u32 = 0;
    let mut includes_last = false;
    let mut next_offset: Option<u32> = None;
    let mut unhandled_classes: Vec<String> = Vec::new();

    let mut in_item = false;
    let mut in_from = false;
    let mut in_mailbox = false;
    let mut current_tag = String::new();
    let mut buf = String::new();

    // Element nesting depth (open Start elements not yet closed). Used to
    // pin `ItemId`/change-key capture to a DIRECT child of the top-level
    // item element: an EWS `Mailbox` (organizer / attendee / sender) can
    // legally carry a nested `ItemId`, and capturing it would overwrite
    // the item's true identity. `item_depth` is the level of the currently
    // open item element; `items_depth` is the level of the enclosing
    // `<Items>` container (its direct children are the per-item elements).
    let mut depth: i32 = 0;
    let mut item_depth: i32 = 0;
    let mut items_depth: Option<i32> = None;

    let mut wire_kind = String::new();
    let mut item_id = String::new();
    let mut change_key: Option<String> = None;
    let mut subject: Option<String> = None;
    let mut sender_email: Option<String> = None;
    let mut sender_name: Option<String> = None;
    let mut received_at: Option<String> = None;
    let mut body_preview: Option<String> = None;
    let mut is_read = false;
    let mut flag_status: Option<String> = None;
    let mut categories: Vec<String> = Vec::new();
    let mut in_categories = false;
    let mut item_class = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                depth += 1;

                if local == "Items" {
                    items_depth = Some(depth);
                }
                // A direct child of `<Items>` that is not a class we
                // collect is an unhandled item class (Task, PostItem,
                // MeetingRequest, DistributionList, ...). Record it once so
                // the caller can surface the omission.
                if let Some(id) = items_depth
                    && depth == id + 1
                    && !is_item_tag(local)
                {
                    let owned = local.to_string();
                    if !unhandled_classes.contains(&owned) {
                        unhandled_classes.push(owned);
                    }
                }

                if is_item_tag(local) && !in_item {
                    in_item = true;
                    item_depth = depth;
                    wire_kind = local.to_string();
                    item_id.clear();
                    change_key = None;
                    subject = None;
                    sender_email = None;
                    sender_name = None;
                    received_at = None;
                    body_preview = None;
                    is_read = false;
                    flag_status = None;
                    categories.clear();
                    item_class.clear();
                }
                if in_item && local == "From" {
                    in_from = true;
                }
                if in_item && local == "Categories" {
                    in_categories = true;
                }
                if in_from && local == "Mailbox" {
                    in_mailbox = true;
                }
                if local == "RootFolder" {
                    total_count = attribute(e, "TotalItemsInView")?.parse().unwrap_or(0);
                    includes_last = attribute(e, "IncludesLastItemInRange")? == "true";
                    next_offset = attribute(e, "IndexedPagingOffset")?.parse().ok();
                }

                current_tag = local.to_string();
                buf.clear();

                // Direct-child `ItemId` only (a Start `ItemId` sits one
                // level below the item element).
                if in_item && local == "ItemId" && depth == item_depth + 1 {
                    item_id = attribute(e, "Id")?;
                    change_key = Some(attribute(e, "ChangeKey")?).filter(|s| !s.is_empty());
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                // An Empty `ItemId` is a direct child of the currently-open
                // element (at `depth`); accept it only when that element is
                // the item itself.
                if in_item && local == "ItemId" && depth == item_depth {
                    item_id = attribute(e, "Id")?;
                    change_key = Some(attribute(e, "ChangeKey")?).filter(|s| !s.is_empty());
                }
                if local == "RootFolder" {
                    total_count = attribute(e, "TotalItemsInView")?.parse().unwrap_or(0);
                    includes_last = attribute(e, "IncludesLastItemInRange")? == "true";
                    next_offset = attribute(e, "IndexedPagingOffset")?.parse().ok();
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => push_ref(e, &mut buf)?,
            Ok(Event::End(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if in_mailbox && in_from {
                    match current_tag.as_str() {
                        "EmailAddress" => sender_email = Some(trimmed.to_string()),
                        "Name" => sender_name = Some(trimmed.to_string()),
                        _ => {}
                    }
                    if local == "Mailbox" {
                        in_mailbox = false;
                    }
                } else if in_item {
                    match current_tag.as_str() {
                        "Subject" => subject = Some(trimmed.to_string()),
                        "DateTimeReceived" => received_at = Some(trimmed.to_string()),
                        "Preview" => body_preview = Some(trimmed.to_string()),
                        "IsRead" => is_read = trimmed == "true",
                        "FlagStatus" => flag_status = Some(trimmed.to_string()),
                        "String" if in_categories => categories.push(trimmed.to_string()),
                        "ItemClass" => item_class = trimmed.to_string(),
                        _ => {}
                    }
                }

                if local == "From" {
                    in_from = false;
                }
                if local == "Categories" {
                    in_categories = false;
                }
                if is_item_tag(local) && in_item && depth == item_depth {
                    if !item_id.is_empty() {
                        items.push(EwsItem {
                            wire_kind: wire_kind.clone(),
                            item_id: item_id.clone(),
                            change_key: change_key.clone(),
                            subject: subject.clone(),
                            sender_email: sender_email.clone(),
                            sender_name: sender_name.clone(),
                            received_at: received_at.clone(),
                            body_preview: body_preview.clone(),
                            body_html: None,
                            is_read,
                            flag_status: flag_status.clone(),
                            categories: categories.clone(),
                            item_class: default_item_class(&item_class),
                            to_recipients: Vec::new(),
                            cc_recipients: Vec::new(),
                            // FindItem is IdOnly + a few scalars; the
                            // attachment descriptors come from GetItem.
                            attachments: Vec::new(),
                        });
                    }
                    in_item = false;
                }

                buf.clear();
                current_tag.clear();
                depth -= 1;
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("FindItem parse failed: {e}"))),
            _ => {}
        }
    }

    Ok(FindItemsResult {
        items,
        total_count,
        includes_last,
        next_offset,
        unhandled_classes,
    })
}

pub(crate) fn parse_get_item_response(xml: &str) -> Result<EwsItem, EwsError> {
    let mut reader = Reader::from_str(xml);

    let mut in_item = false;
    let mut in_from = false;
    let mut in_to = false;
    let mut in_cc = false;
    let mut in_mailbox = false;
    let mut current_tag = String::new();
    let mut buf = String::new();

    // Nesting depth, so `ItemId`/change-key capture pins to a direct child
    // of the top-level item element - a recipient/organizer `Mailbox` can
    // carry a nested `ItemId` that must not overwrite the item's identity.
    let mut depth: i32 = 0;
    let mut item_depth: i32 = 0;

    let mut wire_kind = String::new();
    let mut item_id = String::new();
    let mut change_key: Option<String> = None;
    let mut subject: Option<String> = None;
    let mut sender_email: Option<String> = None;
    let mut sender_name: Option<String> = None;
    let mut received_at: Option<String> = None;
    let mut body_html: Option<String> = None;
    let mut is_read = false;
    let mut flag_status: Option<String> = None;
    let mut categories: Vec<String> = Vec::new();
    let mut in_categories = false;
    let mut item_class = String::new();
    let mut to_recipients: Vec<EwsRecipient> = Vec::new();
    let mut cc_recipients: Vec<EwsRecipient> = Vec::new();
    let mut attachments: Vec<EwsAttachment> = Vec::new();

    let mut recip_email = String::new();
    let mut recip_name: Option<String> = None;

    // `<t:Attachments>` is a sub-tree with its own `Name` / `ContentType` /
    // (for an ItemAttachment) nested `Subject`, `Body`, and `ItemId`
    // elements. Everything inside it belongs to the attachment, never to
    // the enclosing item, so field capture is suppressed while
    // `in_attachments` is set.
    let mut in_attachments = false;
    let mut current_attachment: Option<EwsAttachment> = None;
    // Depth of the open attachment element. Descriptor fields are captured
    // only from its DIRECT children: an ItemAttachment embeds a whole item,
    // whose organizer/recipient `Mailbox` carries its own `<t:Name>` that
    // would otherwise overwrite the attachment's file name.
    let mut attachment_depth: i32 = 0;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                depth += 1;

                if is_item_tag(local) && !in_item {
                    in_item = true;
                    item_depth = depth;
                    wire_kind = local.to_string();
                }
                if in_item && local == "Attachments" {
                    in_attachments = true;
                }
                if in_attachments && is_attachment_tag(local) && current_attachment.is_none() {
                    attachment_depth = depth;
                    current_attachment = Some(EwsAttachment {
                        attachment_id: String::new(),
                        name: None,
                        content_type: None,
                        size: None,
                        is_inline: false,
                        is_item: local == "ItemAttachment",
                    });
                }
                if in_item && !in_attachments {
                    match local {
                        "From" => in_from = true,
                        "ToRecipients" => in_to = true,
                        "CcRecipients" => in_cc = true,
                        "Categories" => in_categories = true,
                        _ => {}
                    }
                }
                if (in_from || in_to || in_cc) && local == "Mailbox" {
                    in_mailbox = true;
                    recip_email.clear();
                    recip_name = None;
                }

                current_tag = local.to_string();
                buf.clear();

                if let Some(attachment) = current_attachment.as_mut()
                    && local == "AttachmentId"
                    && depth == attachment_depth + 1
                {
                    attachment.attachment_id = attribute(e, "Id")?;
                }
                // Direct-child `ItemId` only (Start form: one level below),
                // and never one nested inside an attachment.
                if in_item && !in_attachments && local == "ItemId" && depth == item_depth + 1 {
                    item_id = attribute(e, "Id")?;
                    change_key = Some(attribute(e, "ChangeKey")?).filter(|s| !s.is_empty());
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                // An Empty `AttachmentId` is a child of the currently-open
                // element, so it counts when that element IS the attachment.
                if let Some(attachment) = current_attachment.as_mut()
                    && local == "AttachmentId"
                    && depth == attachment_depth
                {
                    attachment.attachment_id = attribute(e, "Id")?;
                }
                // Direct-child `ItemId` only (Empty form: child of the
                // currently-open element at `depth`).
                if in_item && !in_attachments && local == "ItemId" && depth == item_depth {
                    item_id = attribute(e, "Id")?;
                    change_key = Some(attribute(e, "ChangeKey")?).filter(|s| !s.is_empty());
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => push_ref(e, &mut buf)?,
            Ok(Event::End(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if current_attachment.is_some() {
                    if let Some(attachment) = current_attachment.as_mut()
                        && depth == attachment_depth + 1
                    {
                        match current_tag.as_str() {
                            "Name" => attachment.name = Some(trimmed.to_string()),
                            "ContentType" => attachment.content_type = Some(trimmed.to_string()),
                            "Size" => attachment.size = trimmed.parse().ok(),
                            "IsInline" => attachment.is_inline = trimmed == "true",
                            _ => {}
                        }
                    }
                    if is_attachment_tag(local)
                        && depth == attachment_depth
                        // Drop a descriptor with no id: it cannot be fetched,
                        // so surfacing it as a blob handle would mint a handle
                        // that can only ever fail.
                        && let Some(done) = current_attachment
                            .take()
                            .filter(|a| !a.attachment_id.is_empty())
                    {
                        attachments.push(done);
                    }
                } else if in_mailbox {
                    match current_tag.as_str() {
                        "EmailAddress" => recip_email = trimmed.to_string(),
                        "Name" => recip_name = Some(trimmed.to_string()),
                        _ => {}
                    }
                    if local == "Mailbox" {
                        in_mailbox = false;
                        if !recip_email.is_empty() {
                            let recipient = EwsRecipient {
                                email: recip_email.clone(),
                                name: recip_name.clone(),
                            };
                            if in_from {
                                sender_email = Some(recip_email.clone());
                                sender_name = recip_name.clone();
                            } else if in_to {
                                to_recipients.push(recipient);
                            } else if in_cc {
                                cc_recipients.push(recipient);
                            }
                        }
                    }
                } else if in_item && !in_attachments {
                    match current_tag.as_str() {
                        "Subject" => subject = Some(trimmed.to_string()),
                        "DateTimeReceived" => received_at = Some(trimmed.to_string()),
                        "Body" => body_html = Some(trimmed.to_string()),
                        "IsRead" => is_read = trimmed == "true",
                        "FlagStatus" => flag_status = Some(trimmed.to_string()),
                        "String" if in_categories => categories.push(trimmed.to_string()),
                        "ItemClass" => item_class = trimmed.to_string(),
                        _ => {}
                    }
                }

                match local {
                    "From" => in_from = false,
                    "ToRecipients" => in_to = false,
                    "CcRecipients" => in_cc = false,
                    "Categories" => in_categories = false,
                    "Attachments" => in_attachments = false,
                    _ => {}
                }

                // Close the item at its own depth, symmetric with the
                // FindItem parser. `get_item` requests a single id and the
                // result is built once after the loop, so this is not
                // multi-item support: it prevents a field element appearing
                // as a sibling AFTER the item element closes from bleeding
                // into the already-captured item's accumulators (without the
                // close, `in_item` stays true to EOF).
                if is_item_tag(local) && in_item && !in_attachments && depth == item_depth {
                    in_item = false;
                }

                buf.clear();
                current_tag.clear();
                depth -= 1;
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("GetItem parse failed: {e}"))),
            _ => {}
        }
    }

    if item_id.is_empty() {
        return Err(malformed("No item found in GetItem response"));
    }

    Ok(EwsItem {
        wire_kind,
        item_id,
        change_key,
        subject,
        sender_email,
        sender_name,
        received_at,
        body_preview: None,
        body_html,
        is_read,
        flag_status,
        categories,
        item_class: default_item_class(&item_class),
        to_recipients,
        cc_recipients,
        attachments,
    })
}

/// Parse a `GetAttachment` response into the attachment's decoded bytes.
///
/// `<t:Content>` is base64 in the SOAP body (EWS has no byte-stream
/// attachment endpoint - unlike Graph REST's `/attachments/{id}/$value` -
/// so the whole attachment arrives inline and is decoded here). An
/// `ItemAttachment` carries a nested XML item instead of `<t:Content>`; it
/// parses to empty content rather than an error, and the caller surfaces it
/// as a non-byte-stream blob.
pub(crate) fn parse_get_attachment_response(xml: &str) -> Result<EwsAttachmentContent, EwsError> {
    let mut reader = Reader::from_str(xml);

    let mut attachment_id = String::new();
    let mut name: Option<String> = None;
    let mut content_type: Option<String> = None;
    let mut content: Vec<u8> = Vec::new();

    let mut current_tag = String::new();
    let mut buf = String::new();
    // Anything below a nested item inside an ItemAttachment belongs to that
    // item, not to the attachment descriptor.
    let mut in_nested_item = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name_bytes = e.name().as_ref().to_owned();
                let local = strip_ns(&name_bytes);
                if is_item_tag(local) {
                    in_nested_item = true;
                }
                if local == "AttachmentId" {
                    attachment_id = attribute(e, "Id")?;
                }
                current_tag = local.to_string();
                buf.clear();
            }
            Ok(Event::Empty(ref e)) => {
                let name_bytes = e.name().as_ref().to_owned();
                if strip_ns(&name_bytes) == "AttachmentId" {
                    attachment_id = attribute(e, "Id")?;
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => push_ref(e, &mut buf)?,
            Ok(Event::End(ref e)) => {
                let name_bytes = e.name().as_ref().to_owned();
                let local = strip_ns(&name_bytes);
                let trimmed = buf.trim();
                if !in_nested_item {
                    match current_tag.as_str() {
                        "Name" => name = Some(trimmed.to_string()),
                        "ContentType" => content_type = Some(trimmed.to_string()),
                        "Content" if !trimmed.is_empty() => {
                            content = BASE64.decode(trimmed).map_err(|e| {
                                malformed(format!("GetAttachment content is not base64: {e}"))
                            })?;
                        }
                        _ => {}
                    }
                }
                if is_item_tag(local) {
                    in_nested_item = false;
                }
                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("GetAttachment parse failed: {e}"))),
            _ => {}
        }
    }

    if attachment_id.is_empty() {
        return Err(malformed("No attachment found in GetAttachment response"));
    }

    Ok(EwsAttachmentContent {
        attachment_id,
        name,
        content_type,
        content,
    })
}

// ── Shared helpers ──────────────────────────────────────────

/// Attachment container elements a `GetItem` / `GetAttachment` response can
/// carry (EWS `AttachmentType` subtypes).
fn is_attachment_tag(local: &str) -> bool {
    matches!(local, "FileAttachment" | "ItemAttachment")
}

/// Whether an `ExtendedFieldURI` PropertyTag names PR_REPLICA_LIST
/// (`0x6698`). EWS emits the hex form; accept either case and the bare
/// `6698`, plus the decimal equivalent for completeness.
fn is_replica_list_tag(tag: &str) -> bool {
    let normalized = tag
        .trim()
        .strip_prefix("0x")
        .or_else(|| tag.trim().strip_prefix("0X"))
        .unwrap_or_else(|| tag.trim());
    normalized.eq_ignore_ascii_case("6698") || tag.trim() == "26264"
}

fn is_folder_tag(local: &str) -> bool {
    matches!(
        local,
        "Folder" | "ContactsFolder" | "CalendarFolder" | "TasksFolder"
    )
}

/// Item-class elements the `FindItem` / `GetItem` parsers collect into an
/// `EwsItem`. Mail (`Message`), calendar (`CalendarItem`), and contact
/// (`Contact`) public folders all surface as `CursorScope::Folder`
/// scopes, so all three must flip item-collection state or the folder
/// discovers as a scope yet syncs zero items. Collection here is
/// identity-level (id / change-key / `IsRead`); richer class-specific
/// body projection (a contact has no `Subject`/`From`, an appointment
/// sorts by start not received-time) is a deliberate follow-on. `Task`
/// (from an `IPF.Task` folder, classified by `is_folder_tag`) is not
/// collected yet - a named follow-on.
fn is_item_tag(local: &str) -> bool {
    matches!(
        local,
        "Item"
            | "Message"
            | "CalendarItem"
            | "Contact"
            | "MeetingRequest"
            | "MeetingResponse"
            | "MeetingCancellation"
            | "PostItem"
    )
}

fn apply_effective_right(rights: &mut EwsEffectiveRights, tag: &str, value: &str) {
    let on = value == "true";
    match tag {
        "CreateAssociated" => rights.create_associated = on,
        "CreateContents" => rights.create_contents = on,
        "CreateHierarchy" => rights.create_hierarchy = on,
        "Delete" => rights.delete = on,
        "Modify" => rights.modify = on,
        "Read" => rights.read = on,
        _ => {}
    }
}

fn default_item_class(item_class: &str) -> String {
    item_class.to_string()
}

impl EwsItem {
    pub(crate) fn is_mail(&self) -> bool {
        super::EwsItemKind::from_item_class(&self.item_class) == super::EwsItemKind::Mail
            && matches!(
                self.wire_kind.as_str(),
                "Message"
                    | "MeetingRequest"
                    | "MeetingResponse"
                    | "MeetingCancellation"
                    | "PostItem"
            )
    }
}

/// A text run that does not unescape is a malformed response, not an empty
/// run: dropping it would splice the text on either side into a different
/// value (a subject, an address, an id).
fn push_text(e: &quick_xml::events::BytesText<'_>, buf: &mut String) -> Result<(), EwsError> {
    let text = unescape(e.as_ref()).map_err(|error| malformed(format!("EWS text: {error}")))?;
    buf.push_str(&text);
    Ok(())
}

/// A named attribute, unescaped (empty when absent); one that does not
/// parse or unescape is a malformed response.
fn attribute(e: &quick_xml::events::BytesStart<'_>, name: &str) -> Result<String, EwsError> {
    extract_attribute(e, name).map_err(|reason| malformed(format!("EWS {reason}")))
}

/// quick-xml emits every entity and character reference as its own event,
/// so each parser folds them back into the text run. An unknown entity or an
/// invalid character reference is a malformed response: it used to vanish,
/// leaving a value the server never sent.
fn push_ref(e: &quick_xml::events::BytesRef<'_>, buf: &mut String) -> Result<(), EwsError> {
    try_push_general_ref(e, buf).map_err(|reason| malformed(format!("EWS text: {reason}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn nc2a_meeting_wire_kinds_survive_both_parsers() {
        for (tag, class) in [
            ("MeetingRequest", "IPM.Schedule.Meeting.Request"),
            ("MeetingResponse", "IPM.Schedule.Meeting.Resp.Pos"),
            ("MeetingCancellation", "IPM.Schedule.Meeting.Canceled"),
            ("PostItem", "IPM.Post"),
        ] {
            let item = format!(
                r#"<t:{tag}><t:ItemId Id="meeting"/><t:ItemClass>{class}</t:ItemClass></t:{tag}>"#
            );
            let get = parse_get_item_response(&format!("<m:Items>{item}</m:Items>"))
                .expect("GetItem parses");
            assert_eq!(get.wire_kind, tag);
            assert!(get.is_mail());
            let find = parse_find_items_response(&format!(
                r#"<m:RootFolder TotalItemsInView="1" IncludesLastItemInRange="true"><t:Items>{item}</t:Items></m:RootFolder>"#))
                .expect("FindItem parses");
            assert_eq!(find.items.len(), 1);
            assert_eq!(find.items[0].wire_kind, tag);
            assert!(find.items[0].is_mail());
        }
    }

    #[test]
    fn parse_find_folder_response_two_folders() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                          xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="2" IncludesLastItemInRange="true">
            <t:Folders>
              <t:Folder>
                <t:FolderId Id="AAMkAGFk=" ChangeKey="AQAAAB"/>
                <t:DisplayName>Company Announcements</t:DisplayName>
                <t:TotalCount>42</t:TotalCount>
                <t:ChildFolderCount>3</t:ChildFolderCount>
                <t:UnreadCount>5</t:UnreadCount>
                <t:FolderClass>IPF.Note</t:FolderClass>
                <t:EffectiveRights>
                  <t:CreateAssociated>false</t:CreateAssociated>
                  <t:CreateContents>true</t:CreateContents>
                  <t:CreateHierarchy>false</t:CreateHierarchy>
                  <t:Delete>false</t:Delete>
                  <t:Modify>false</t:Modify>
                  <t:Read>true</t:Read>
                </t:EffectiveRights>
              </t:Folder>
              <t:Folder>
                <t:FolderId Id="BBNkAHJk=" ChangeKey="BQAAAC"/>
                <t:DisplayName>IT Helpdesk</t:DisplayName>
                <t:TotalCount>128</t:TotalCount>
                <t:ChildFolderCount>0</t:ChildFolderCount>
                <t:UnreadCount>12</t:UnreadCount>
                <t:FolderClass>IPF.Note</t:FolderClass>
                <t:EffectiveRights>
                  <t:CreateAssociated>true</t:CreateAssociated>
                  <t:CreateContents>true</t:CreateContents>
                  <t:CreateHierarchy>true</t:CreateHierarchy>
                  <t:Delete>true</t:Delete>
                  <t:Modify>true</t:Modify>
                  <t:Read>true</t:Read>
                </t:EffectiveRights>
              </t:Folder>
            </t:Folders>
          </m:RootFolder>
        </m:FindFolderResponseMessage>
      </m:ResponseMessages>
    </m:FindFolderResponse>
  </s:Body>
</s:Envelope>"#;

        let folders = parse_find_folder_response(xml).expect("parse should succeed");
        assert_eq!(folders.len(), 2);
        assert_eq!(folders[0].folder_id, "AAMkAGFk=");
        assert_eq!(folders[0].display_name, "Company Announcements");
        assert_eq!(folders[0].total_count, 42);
        assert_eq!(folders[0].unread_count, 5);
        assert_eq!(folders[0].child_folder_count, 3);
        assert_eq!(folders[0].folder_class.as_deref(), Some("IPF.Note"));
        assert!(folders[0].effective_rights.read);
        assert!(folders[0].effective_rights.create_contents);
        assert!(!folders[0].effective_rights.delete);
        assert!(!folders[0].effective_rights.modify);
        assert_eq!(folders[1].folder_id, "BBNkAHJk=");
        assert_eq!(folders[1].display_name, "IT Helpdesk");
        assert_eq!(folders[1].total_count, 128);
        assert!(folders[1].effective_rights.delete);
        assert!(folders[1].effective_rights.modify);
    }

    #[test]
    fn parse_find_folder_response_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                          xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="0" IncludesLastItemInRange="true">
            <t:Folders/>
          </m:RootFolder>
        </m:FindFolderResponseMessage>
      </m:ResponseMessages>
    </m:FindFolderResponse>
  </s:Body>
</s:Envelope>"#;
        let folders = parse_find_folder_response(xml).expect("parse should succeed");
        assert!(folders.is_empty());
    }

    #[test]
    fn parse_find_folder_response_effective_rights_parsing() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                          xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="1" IncludesLastItemInRange="true">
            <t:Folders>
              <t:Folder>
                <t:FolderId Id="AARead="/>
                <t:DisplayName>ReadOnly Folder</t:DisplayName>
                <t:TotalCount>10</t:TotalCount>
                <t:ChildFolderCount>0</t:ChildFolderCount>
                <t:UnreadCount>0</t:UnreadCount>
                <t:EffectiveRights>
                  <t:CreateAssociated>false</t:CreateAssociated>
                  <t:CreateContents>false</t:CreateContents>
                  <t:CreateHierarchy>false</t:CreateHierarchy>
                  <t:Delete>false</t:Delete>
                  <t:Modify>false</t:Modify>
                  <t:Read>true</t:Read>
                </t:EffectiveRights>
              </t:Folder>
            </t:Folders>
          </m:RootFolder>
        </m:FindFolderResponseMessage>
      </m:ResponseMessages>
    </m:FindFolderResponse>
  </s:Body>
</s:Envelope>"#;
        let folders = parse_find_folder_response(xml).expect("parse should succeed");
        assert_eq!(folders.len(), 1);
        let rights = &folders[0].effective_rights;
        assert!(!rights.create_associated);
        assert!(!rights.create_contents);
        assert!(!rights.create_hierarchy);
        assert!(!rights.delete);
        assert!(!rights.modify);
        assert!(rights.read);
    }

    #[test]
    fn parse_find_items_response_two_items() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                        xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="150" IncludesLastItemInRange="false">
            <t:Items>
              <t:Message>
                <t:ItemId Id="AAMkItem1=" ChangeKey="CK1"/>
                <t:Subject>Q1 Results</t:Subject>
                <t:DateTimeReceived>2026-03-01T10:30:00Z</t:DateTimeReceived>
                <t:From>
                  <t:Mailbox>
                    <t:Name>Jane Doe</t:Name>
                    <t:EmailAddress>jane@contoso.com</t:EmailAddress>
                  </t:Mailbox>
                </t:From>
                <t:IsRead>true</t:IsRead>
                <t:Preview>Here are the Q1 financial results...</t:Preview>
                <t:ItemClass>IPM.Note</t:ItemClass>
              </t:Message>
              <t:Message>
                <t:ItemId Id="AAMkItem2=" ChangeKey="CK2"/>
                <t:Subject>Office Move Update</t:Subject>
                <t:DateTimeReceived>2026-02-28T14:15:00Z</t:DateTimeReceived>
                <t:From>
                  <t:Mailbox>
                    <t:Name>Facilities</t:Name>
                    <t:EmailAddress>facilities@contoso.com</t:EmailAddress>
                  </t:Mailbox>
                </t:From>
                <t:IsRead>false</t:IsRead>
                <t:Preview>The office move has been rescheduled...</t:Preview>
                <t:ItemClass>IPM.Note</t:ItemClass>
              </t:Message>
            </t:Items>
          </m:RootFolder>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_find_items_response(xml).expect("parse should succeed");
        assert_eq!(result.total_count, 150);
        assert!(!result.includes_last);
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.items[0].item_id, "AAMkItem1=");
        assert_eq!(result.items[0].change_key.as_deref(), Some("CK1"));
        assert_eq!(result.items[0].subject.as_deref(), Some("Q1 Results"));
        assert_eq!(
            result.items[0].sender_email.as_deref(),
            Some("jane@contoso.com")
        );
        assert_eq!(result.items[0].sender_name.as_deref(), Some("Jane Doe"));
        assert!(result.items[0].is_read);
        assert_eq!(result.items[1].item_id, "AAMkItem2=");
        assert!(!result.items[1].is_read);
        assert_eq!(
            result.items[1].received_at.as_deref(),
            Some("2026-02-28T14:15:00Z")
        );
    }

    #[test]
    fn parse_find_items_response_calendar_and_contact() {
        // A mixed-class public folder: one Message, one CalendarItem, one
        // Contact - all collectable - plus one Task the parser does not
        // yet handle. `TotalItemsInView` counts every class (4), so
        // `items.len()` stays at the 3 tracked classes.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                        xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="4" IncludesLastItemInRange="true">
            <t:Items>
              <t:Message>
                <t:ItemId Id="AAMkMsg=" ChangeKey="CKM"/>
                <t:Subject>Notice</t:Subject>
                <t:ItemClass>IPM.Note</t:ItemClass>
              </t:Message>
              <t:CalendarItem>
                <t:ItemId Id="AAMkCal=" ChangeKey="CKC"/>
                <t:Subject>Team Standup</t:Subject>
                <t:ItemClass>IPM.Appointment</t:ItemClass>
              </t:CalendarItem>
              <t:Contact>
                <t:ItemId Id="AAMkCon=" ChangeKey="CKN"/>
                <t:ItemClass>IPM.Contact</t:ItemClass>
              </t:Contact>
              <t:Task>
                <t:ItemId Id="AAMkTask=" ChangeKey="CKT"/>
                <t:ItemClass>IPM.Task</t:ItemClass>
              </t:Task>
            </t:Items>
          </m:RootFolder>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_find_items_response(xml).expect("parse should succeed");
        // TotalItemsInView counts all four classes.
        assert_eq!(result.total_count, 4);
        // The Task is not collected: three tracked-class items only.
        assert_eq!(result.items.len(), 3);
        assert_eq!(result.items[0].item_id, "AAMkMsg=");
        assert_eq!(result.items[0].item_class, "IPM.Note");
        assert_eq!(result.items[1].item_id, "AAMkCal=");
        assert_eq!(result.items[1].change_key.as_deref(), Some("CKC"));
        assert_eq!(result.items[1].item_class, "IPM.Appointment");
        assert_eq!(result.items[2].item_id, "AAMkCon=");
        assert_eq!(result.items[2].change_key.as_deref(), Some("CKN"));
        assert_eq!(result.items[2].item_class, "IPM.Contact");
        // A CalendarItem/Contact without DateTimeReceived carries no
        // watermark contribution.
        assert!(result.items[1].received_at.is_none());
        assert!(result.items[2].received_at.is_none());
        assert!(
            result.items.iter().all(|i| i.item_id != "AAMkTask="),
            "Task must not be collected"
        );
        // The dropped Task class is surfaced for a scoped warning.
        assert_eq!(result.unhandled_classes, vec!["Task".to_string()]);
        // Last page (IncludesLastItemInRange=true), no paging offset.
        assert!(result.includes_last);
        assert_eq!(result.next_offset, None);
    }

    #[test]
    fn parse_find_items_response_paging_offset() {
        // A non-final page: the server reports its own next-page offset in
        // wire rows via IndexedPagingOffset. Paging must advance off this,
        // not off the two parsed Messages.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                        xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="500" IncludesLastItemInRange="false" IndexedPagingOffset="100">
            <t:Items>
              <t:Message>
                <t:ItemId Id="AAMk1=" ChangeKey="CK1"/>
              </t:Message>
              <t:Task>
                <t:ItemId Id="AAMkT=" ChangeKey="CKT"/>
              </t:Task>
              <t:Message>
                <t:ItemId Id="AAMk2=" ChangeKey="CK2"/>
              </t:Message>
            </t:Items>
          </m:RootFolder>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_find_items_response(xml).expect("parse should succeed");
        assert!(!result.includes_last);
        // Server offset (100), NOT the two collected Messages.
        assert_eq!(result.next_offset, Some(100));
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.unhandled_classes, vec!["Task".to_string()]);
    }

    #[test]
    fn parse_find_items_response_nested_mailbox_item_id_ignored() {
        // A CalendarItem whose Organizer and attendee Mailbox each carry a
        // nested ItemId. Those must NOT overwrite the appointment's own
        // identity - only the direct-child ItemId counts.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                        xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="1" IncludesLastItemInRange="true">
            <t:Items>
              <t:CalendarItem>
                <t:ItemId Id="ROOT=" ChangeKey="ROOTCK"/>
                <t:Subject>Planning</t:Subject>
                <t:Organizer>
                  <t:Mailbox>
                    <t:Name>Alice</t:Name>
                    <t:EmailAddress>alice@contoso.com</t:EmailAddress>
                    <t:ItemId Id="ORGANIZER=" ChangeKey="ORGCK"/>
                  </t:Mailbox>
                </t:Organizer>
                <t:RequiredAttendees>
                  <t:Attendee>
                    <t:Mailbox>
                      <t:Name>Bob</t:Name>
                      <t:EmailAddress>bob@contoso.com</t:EmailAddress>
                      <t:ItemId Id="ATTENDEE=" ChangeKey="ATTCK"/>
                    </t:Mailbox>
                  </t:Attendee>
                </t:RequiredAttendees>
                <t:ItemClass>IPM.Appointment</t:ItemClass>
              </t:CalendarItem>
            </t:Items>
          </m:RootFolder>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_find_items_response(xml).expect("parse should succeed");
        assert_eq!(result.items.len(), 1);
        // Identity is the appointment's own ItemId, never a nested Mailbox one.
        assert_eq!(result.items[0].item_id, "ROOT=");
        assert_eq!(result.items[0].change_key.as_deref(), Some("ROOTCK"));
        assert_eq!(result.items[0].item_class, "IPM.Appointment");
    }

    #[test]
    fn parse_get_item_response_nested_mailbox_item_id_ignored() {
        // A CalendarItem GetItem whose Organizer Mailbox carries a nested
        // ItemId - identity must stay the appointment's, not the organizer's.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:CalendarItem>
              <t:ItemId Id="APPT=" ChangeKey="APPTCK"/>
              <t:Subject>Review</t:Subject>
              <t:Organizer>
                <t:Mailbox>
                  <t:Name>Carol</t:Name>
                  <t:EmailAddress>carol@contoso.com</t:EmailAddress>
                  <t:ItemId Id="ORG=" ChangeKey="ORGCK"/>
                </t:Mailbox>
              </t:Organizer>
              <t:ItemClass>IPM.Appointment</t:ItemClass>
            </t:CalendarItem>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let item = parse_get_item_response(xml).expect("parse should succeed");
        assert_eq!(item.item_id, "APPT=");
        assert_eq!(item.change_key.as_deref(), Some("APPTCK"));
    }

    #[test]
    fn parse_get_item_response_start_form_item_id() {
        // The item's own ItemId as a Start+End pair (non-self-closing),
        // exercising the `depth == item_depth + 1` Start-form guard, while
        // a nested organizer ItemId in the SAME start form must not steal
        // identity.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:CalendarItem>
              <t:ItemId Id="APPT=" ChangeKey="APPTCK"></t:ItemId>
              <t:Subject>Review</t:Subject>
              <t:Organizer>
                <t:Mailbox>
                  <t:Name>Carol</t:Name>
                  <t:EmailAddress>carol@contoso.com</t:EmailAddress>
                  <t:ItemId Id="ORG=" ChangeKey="ORGCK"></t:ItemId>
                </t:Mailbox>
              </t:Organizer>
              <t:ItemClass>IPM.Appointment</t:ItemClass>
            </t:CalendarItem>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let item = parse_get_item_response(xml).expect("parse should succeed");
        assert_eq!(item.item_id, "APPT=");
        assert_eq!(item.change_key.as_deref(), Some("APPTCK"));
    }

    #[test]
    fn parse_get_item_response_stray_field_after_item_close_ignored() {
        // With `in_item` closed at the item's depth, a field element that
        // appears as a sibling AFTER the item element closes does not bleed
        // into the (already-captured) item's accumulators. Without the
        // close, `in_item` would stay true and the stray `<t:Subject>`
        // would overwrite the real subject.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:Message>
              <t:ItemId Id="REAL=" ChangeKey="CK1"/>
              <t:Subject>Real Subject</t:Subject>
              <t:ItemClass>IPM.Note</t:ItemClass>
            </t:Message>
            <t:Subject>Stray Sibling Subject</t:Subject>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let item = parse_get_item_response(xml).expect("parse should succeed");
        assert_eq!(item.item_id, "REAL=");
        // The subject stays the item's own, not the stray sibling's.
        assert_eq!(item.subject.as_deref(), Some("Real Subject"));
    }

    #[test]
    fn parse_get_item_response_contact() {
        // The GetItem parser flips item-collection state on `<t:Contact>`,
        // so the answer to a contact-shaped hydration request parses to
        // identity rather than erroring. The request side is now
        // class-conditional (`EwsItemShape`, see `ops::get_item`), so this
        // is the real response shape for a non-mail public-folder item, not
        // just parser tolerance.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:Contact>
              <t:ItemId Id="AAMkContact=" ChangeKey="CKContact"/>
              <t:ItemClass>IPM.Contact</t:ItemClass>
              <t:DisplayName>Jane Doe</t:DisplayName>
            </t:Contact>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let item = parse_get_item_response(xml).expect("parse should succeed");
        assert_eq!(item.item_id, "AAMkContact=");
        assert_eq!(item.change_key.as_deref(), Some("CKContact"));
        assert_eq!(item.item_class, "IPM.Contact");
        assert!(item.to_recipients.is_empty());
        assert!(item.cc_recipients.is_empty());
    }

    #[test]
    fn parse_get_item_response_collects_body_recipients_and_attachments() {
        // The public-folder hydration read: a Message with body, To/Cc
        // recipients, and two attachment descriptors. The nested
        // ItemAttachment's own Subject / ItemId must NOT overwrite the
        // enclosing item's, and the id-less descriptor is dropped (it could
        // only ever fail to fetch).
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:Message>
              <t:ItemId Id="AAMkItem=" ChangeKey="CK1"/>
              <t:Subject>Company Policy</t:Subject>
              <t:DateTimeReceived>2026-03-01T10:30:00Z</t:DateTimeReceived>
              <t:Body BodyType="HTML">&lt;p&gt;Read this&lt;/p&gt;</t:Body>
              <t:IsRead>true</t:IsRead>
              <t:ItemClass>IPM.Note</t:ItemClass>
              <t:From>
                <t:Mailbox>
                  <t:Name>HR</t:Name>
                  <t:EmailAddress>hr@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:From>
              <t:ToRecipients>
                <t:Mailbox>
                  <t:Name>All Staff</t:Name>
                  <t:EmailAddress>staff@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:ToRecipients>
              <t:CcRecipients>
                <t:Mailbox>
                  <t:EmailAddress>legal@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:CcRecipients>
              <t:Attachments>
                <t:FileAttachment>
                  <t:AttachmentId Id="AAMkAtt1="/>
                  <t:Name>policy.pdf</t:Name>
                  <t:ContentType>application/pdf</t:ContentType>
                  <t:Size>2048</t:Size>
                  <t:IsInline>false</t:IsInline>
                </t:FileAttachment>
                <t:ItemAttachment>
                  <t:AttachmentId Id="AAMkAtt2="/>
                  <t:Name>Forwarded Note</t:Name>
                  <t:Message>
                    <t:ItemId Id="NESTED=" ChangeKey="NCK"/>
                    <t:Subject>Nested Subject</t:Subject>
                  </t:Message>
                </t:ItemAttachment>
                <t:FileAttachment>
                  <t:Name>no-id.bin</t:Name>
                </t:FileAttachment>
              </t:Attachments>
            </t:Message>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let item = parse_get_item_response(xml).expect("parse should succeed");
        // Identity and body survive the attachment sub-tree.
        assert_eq!(item.item_id, "AAMkItem=");
        assert_eq!(item.change_key.as_deref(), Some("CK1"));
        assert_eq!(item.subject.as_deref(), Some("Company Policy"));
        assert_eq!(item.body_html.as_deref(), Some("<p>Read this</p>"));
        assert!(item.is_read);
        assert_eq!(item.sender_email.as_deref(), Some("hr@contoso.com"));
        assert_eq!(item.to_recipients.len(), 1);
        assert_eq!(item.to_recipients[0].email, "staff@contoso.com");
        assert_eq!(item.cc_recipients.len(), 1);
        assert_eq!(item.cc_recipients[0].email, "legal@contoso.com");

        // Two id-bearing descriptors; the id-less one is dropped.
        assert_eq!(item.attachments.len(), 2);
        assert_eq!(
            item.attachments[0],
            EwsAttachment {
                attachment_id: "AAMkAtt1=".to_string(),
                name: Some("policy.pdf".to_string()),
                content_type: Some("application/pdf".to_string()),
                size: Some(2048),
                is_inline: false,
                is_item: false,
            }
        );
        assert_eq!(item.attachments[1].attachment_id, "AAMkAtt2=");
        assert!(item.attachments[1].is_item);
    }

    #[test]
    fn parse_get_attachment_response_decodes_content() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetAttachmentResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                             xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetAttachmentResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Attachments>
            <t:FileAttachment>
              <t:AttachmentId Id="AAMkAtt1="/>
              <t:Name>hello.txt</t:Name>
              <t:ContentType>text/plain</t:ContentType>
              <t:Content>aGVsbG8=</t:Content>
            </t:FileAttachment>
          </m:Attachments>
        </m:GetAttachmentResponseMessage>
      </m:ResponseMessages>
    </m:GetAttachmentResponse>
  </s:Body>
</s:Envelope>"#;
        let attachment = parse_get_attachment_response(xml).expect("parse should succeed");
        assert_eq!(attachment.attachment_id, "AAMkAtt1=");
        assert_eq!(attachment.name.as_deref(), Some("hello.txt"));
        assert_eq!(attachment.content_type.as_deref(), Some("text/plain"));
        // EWS ships attachment bytes base64-inline, not as a byte stream.
        assert_eq!(attachment.content, b"hello");
    }

    #[test]
    fn parse_find_items_response_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                        xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="0" IncludesLastItemInRange="true">
            <t:Items/>
          </m:RootFolder>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_find_items_response(xml).expect("parse should succeed");
        assert_eq!(result.total_count, 0);
        assert!(result.includes_last);
        assert!(result.items.is_empty());
    }

    #[test]
    fn parse_get_item_response_full() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:Message>
              <t:ItemId Id="AAMkFull=" ChangeKey="CKFull"/>
              <t:Subject>Quarterly Review</t:Subject>
              <t:DateTimeReceived>2026-03-10T09:00:00Z</t:DateTimeReceived>
              <t:Body BodyType="HTML">&lt;html&gt;&lt;body&gt;Meeting notes here&lt;/body&gt;&lt;/html&gt;</t:Body>
              <t:IsRead>true</t:IsRead>
              <t:ItemClass>IPM.Note</t:ItemClass>
              <t:From>
                <t:Mailbox>
                  <t:Name>Alice Smith</t:Name>
                  <t:EmailAddress>alice@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:From>
              <t:ToRecipients>
                <t:Mailbox>
                  <t:Name>Bob Jones</t:Name>
                  <t:EmailAddress>bob@contoso.com</t:EmailAddress>
                </t:Mailbox>
                <t:Mailbox>
                  <t:Name>Carol White</t:Name>
                  <t:EmailAddress>carol@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:ToRecipients>
              <t:CcRecipients>
                <t:Mailbox>
                  <t:Name>Dave Brown</t:Name>
                  <t:EmailAddress>dave@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:CcRecipients>
            </t:Message>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let item = parse_get_item_response(xml).expect("parse should succeed");
        assert_eq!(item.item_id, "AAMkFull=");
        assert_eq!(item.change_key.as_deref(), Some("CKFull"));
        assert_eq!(item.subject.as_deref(), Some("Quarterly Review"));
        assert_eq!(item.sender_email.as_deref(), Some("alice@contoso.com"));
        assert_eq!(item.sender_name.as_deref(), Some("Alice Smith"));
        assert!(item.is_read);
        assert_eq!(item.item_class, "IPM.Note");
        assert!(item.body_html.is_some());
        assert!(item.body_html.as_deref().unwrap_or("").contains("<html>"));
        assert_eq!(item.to_recipients.len(), 2);
        assert_eq!(item.to_recipients[0].email, "bob@contoso.com");
        assert_eq!(item.to_recipients[0].name.as_deref(), Some("Bob Jones"));
        assert_eq!(item.to_recipients[1].email, "carol@contoso.com");
        assert_eq!(item.cc_recipients.len(), 1);
        assert_eq!(item.cc_recipients[0].email, "dave@contoso.com");
    }

    #[test]
    fn parse_get_item_response_empty_is_err() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items/>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_get_item_response(xml);
        assert!(matches!(result, Err(EwsError::MalformedXml(_))));
    }

    #[test]
    fn parse_get_folder_response_with_replica_list() {
        let guid = "{ABCD1234-EF56-7890-AB12-CDEF34567890}";
        let mut raw = Vec::new();
        raw.extend_from_slice(guid.as_bytes());
        raw.push(0);
        let b64 = BASE64.encode(&raw);

        let xml = format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                         xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Folders>
            <t:Folder>
              <t:FolderId Id="AAMkPF=" ChangeKey="CK1"/>
              <t:DisplayName>Public Docs</t:DisplayName>
              <t:TotalCount>55</t:TotalCount>
              <t:ChildFolderCount>2</t:ChildFolderCount>
              <t:UnreadCount>3</t:UnreadCount>
              <t:FolderClass>IPF.Note</t:FolderClass>
              <t:EffectiveRights>
                <t:CreateAssociated>false</t:CreateAssociated>
                <t:CreateContents>true</t:CreateContents>
                <t:CreateHierarchy>false</t:CreateHierarchy>
                <t:Delete>false</t:Delete>
                <t:Modify>false</t:Modify>
                <t:Read>true</t:Read>
              </t:EffectiveRights>
              <t:ExtendedProperty>
                <t:ExtendedFieldURI PropertyTag="0x6698" PropertyType="Binary"/>
                <t:Value>{b64}</t:Value>
              </t:ExtendedProperty>
            </t:Folder>
          </m:Folders>
        </m:GetFolderResponseMessage>
      </m:ResponseMessages>
    </m:GetFolderResponse>
  </s:Body>
</s:Envelope>"#
        );

        let folder = parse_get_folder_response(&xml).expect("parse should succeed");
        assert_eq!(folder.folder_id, "AAMkPF=");
        assert_eq!(folder.display_name, "Public Docs");
        assert_eq!(folder.total_count, 55);
        assert!(folder.replica_list.is_some());

        let replica_bytes = folder.replica_list.as_ref().expect("replica list present");
        let b64_round = BASE64.encode(replica_bytes);
        let guids = decode_replica_list(&b64_round).expect("decode should succeed");
        assert_eq!(guids.len(), 1);
        assert_eq!(guids[0], guid);
    }

    #[test]
    fn get_folder_ignores_non_replica_extended_property() {
        // A base64-decodable Value under a DIFFERENT PropertyTag must not
        // be mis-assigned as the replica list.
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                         xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Folders>
            <t:Folder>
              <t:FolderId Id="AAMkPF=" ChangeKey="CK1"/>
              <t:DisplayName>No Replica</t:DisplayName>
              <t:ExtendedProperty>
                <t:ExtendedFieldURI PropertyTag="0x1234" PropertyType="Binary"/>
                <t:Value>QUJDRA==</t:Value>
              </t:ExtendedProperty>
            </t:Folder>
          </m:Folders>
        </m:GetFolderResponseMessage>
      </m:ResponseMessages>
    </m:GetFolderResponse>
  </s:Body>
</s:Envelope>"#;
        let folder = parse_get_folder_response(xml).expect("parse should succeed");
        assert!(folder.replica_list.is_none());
    }

    #[test]
    fn decode_replica_list_round_trip() {
        let guid1 = "{1A2B3C4D-5E6F-7A8B-9C0D-1E2F3A4B5C6D}";
        let guid2 = "{AAAABBBB-CCCC-DDDD-EEEE-FFFF00001111}";
        let mut raw = Vec::new();
        raw.extend_from_slice(guid1.as_bytes());
        raw.push(0);
        raw.extend_from_slice(guid2.as_bytes());
        raw.push(0);

        let encoded = BASE64.encode(&raw);
        let guids = decode_replica_list(&encoded).expect("decode should succeed");
        assert_eq!(guids.len(), 2);
        assert_eq!(guids[0], guid1);
        assert_eq!(guids[1], guid2);

        let empty = decode_replica_list(&BASE64.encode(b"")).expect("decode empty");
        assert!(empty.is_empty());
    }

    /// Every operation parser, as a function from one text value embedded in
    /// an otherwise well-formed response to the field that value lands in.
    fn text_value_parsers() -> Vec<(&'static str, TextValueParser)> {
        fn folder(tag: &str, value: &str) -> String {
            format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:{tag}Response><m:ResponseMessages><m:{tag}ResponseMessage ResponseClass="Success">
<m:ResponseCode>NoError</m:ResponseCode><m:RootFolder><t:Folders><t:Folder>
<t:FolderId Id="F1"/><t:DisplayName>{value}</t:DisplayName>
</t:Folder></t:Folders></m:RootFolder></m:{tag}ResponseMessage></m:ResponseMessages>
</m:{tag}Response></s:Body></s:Envelope>"#
            )
        }
        fn item(tag: &str, value: &str) -> String {
            format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:{tag}Response><m:ResponseMessages><m:{tag}ResponseMessage ResponseClass="Success">
<m:ResponseCode>NoError</m:ResponseCode><m:RootFolder><t:Items><t:Message>
<t:ItemId Id="I1"/><t:Subject>{value}</t:Subject>
</t:Message></t:Items></m:RootFolder></m:{tag}ResponseMessage></m:ResponseMessages>
</m:{tag}Response></s:Body></s:Envelope>"#
            )
        }
        fn find_folder(value: &str) -> Result<String, EwsError> {
            let folders = parse_find_folder_response(&folder("FindFolder", value))?;
            Ok(folders[0].display_name.clone())
        }
        fn get_folder(value: &str) -> Result<String, EwsError> {
            Ok(parse_get_folder_response(&folder("GetFolder", value))?.display_name)
        }
        fn find_item(value: &str) -> Result<String, EwsError> {
            let result = parse_find_items_response(&item("FindItem", value))?;
            Ok(result.items[0].subject.clone().unwrap_or_default())
        }
        fn get_item(value: &str) -> Result<String, EwsError> {
            Ok(parse_get_item_response(&item("GetItem", value))?
                .subject
                .unwrap_or_default())
        }
        fn get_attachment(value: &str) -> Result<String, EwsError> {
            let xml = format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:GetAttachmentResponse><m:ResponseMessages>
<m:GetAttachmentResponseMessage ResponseClass="Success"><m:Attachments>
<t:FileAttachment><t:AttachmentId Id="A1"/><t:Name>{value}</t:Name></t:FileAttachment>
</m:Attachments></m:GetAttachmentResponseMessage></m:ResponseMessages>
</m:GetAttachmentResponse></s:Body></s:Envelope>"#
            );
            Ok(parse_get_attachment_response(&xml)?
                .name
                .unwrap_or_default())
        }
        vec![
            ("FindFolder", find_folder as TextValueParser),
            ("GetFolder", get_folder as TextValueParser),
            ("FindItem", find_item as TextValueParser),
            ("GetItem", get_item as TextValueParser),
            ("GetAttachment", get_attachment as TextValueParser),
        ]
    }

    type TextValueParser = fn(&str) -> Result<String, EwsError>;

    /// Every operation parser, as a function from one `Id` attribute value
    /// embedded in an otherwise well-formed response to the id it parses.
    fn id_attribute_parsers() -> Vec<(&'static str, TextValueParser)> {
        fn folder(tag: &str, id: &str) -> String {
            format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:{tag}Response><m:ResponseMessages><m:{tag}ResponseMessage ResponseClass="Success">
<m:ResponseCode>NoError</m:ResponseCode><m:RootFolder><t:Folders><t:Folder>
<t:FolderId Id="{id}"/><t:DisplayName>Docs</t:DisplayName>
</t:Folder></t:Folders></m:RootFolder></m:{tag}ResponseMessage></m:ResponseMessages>
</m:{tag}Response></s:Body></s:Envelope>"#
            )
        }
        fn item(tag: &str, id: &str) -> String {
            format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:{tag}Response><m:ResponseMessages><m:{tag}ResponseMessage ResponseClass="Success">
<m:ResponseCode>NoError</m:ResponseCode><m:RootFolder><t:Items><t:Message>
<t:ItemId Id="{id}" ChangeKey="CK"/><t:Subject>Hi</t:Subject>
</t:Message></t:Items></m:RootFolder></m:{tag}ResponseMessage></m:ResponseMessages>
</m:{tag}Response></s:Body></s:Envelope>"#
            )
        }
        fn find_folder(id: &str) -> Result<String, EwsError> {
            let folders = parse_find_folder_response(&folder("FindFolder", id))?;
            Ok(folders[0].folder_id.clone())
        }
        fn get_folder(id: &str) -> Result<String, EwsError> {
            Ok(parse_get_folder_response(&folder("GetFolder", id))?.folder_id)
        }
        fn find_item(id: &str) -> Result<String, EwsError> {
            let result = parse_find_items_response(&item("FindItem", id))?;
            Ok(result.items[0].item_id.clone())
        }
        fn get_item(id: &str) -> Result<String, EwsError> {
            Ok(parse_get_item_response(&item("GetItem", id))?.item_id)
        }
        fn get_attachment(id: &str) -> Result<String, EwsError> {
            let xml = format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:GetAttachmentResponse><m:ResponseMessages>
<m:GetAttachmentResponseMessage ResponseClass="Success"><m:Attachments>
<t:FileAttachment><t:AttachmentId Id="{id}"/><t:Name>a.txt</t:Name></t:FileAttachment>
</m:Attachments></m:GetAttachmentResponseMessage></m:ResponseMessages>
</m:GetAttachmentResponse></s:Body></s:Envelope>"#
            );
            Ok(parse_get_attachment_response(&xml)?.attachment_id)
        }
        vec![
            ("FindFolder", find_folder as TextValueParser),
            ("GetFolder", get_folder as TextValueParser),
            ("FindItem", find_item as TextValueParser),
            ("GetItem", get_item as TextValueParser),
            ("GetAttachment", get_attachment as TextValueParser),
        ]
    }

    /// Attribute values were returned raw, so an id the server escaped kept
    /// its `&amp;` (a different id from the one the server meant, which the
    /// next request would then name), and an unresolvable reference passed
    /// through as if it were text. Ids now unescape, strictly.
    #[test]
    fn an_id_attribute_unescapes_strictly_in_every_parser() {
        for (parser, parse) in id_attribute_parsers() {
            assert_eq!(
                parse("a&amp;b&#x43;").expect("well-formed id parses"),
                "a&bC",
                "{parser}"
            );
            for bad in ["a&bogus;b", "a&#xD800;b"] {
                match parse(bad) {
                    Err(EwsError::MalformedXml(_)) => {}
                    other => panic!("{parser} parsed id {bad:?} as {:?}", other.map(|_| ())),
                }
            }
        }
    }

    /// The lenient reference folding dropped an unknown entity or an invalid
    /// character reference and parsed on, so `R&bogus;D` came back as `RD`:
    /// a folder name, subject, or attachment name the server never sent.
    /// Every parser now reports the body as malformed; the predefined
    /// entities and valid character references still decode.
    #[test]
    fn an_unresolvable_reference_is_a_malformed_response_in_every_parser() {
        for (parser, parse) in text_value_parsers() {
            assert_eq!(
                parse("R&amp;D &#x41;&#66;").expect("well-formed value parses"),
                "R&D AB",
                "{parser}"
            );
            for bad in ["R&bogus;D", "R&#xD800;D", "R&#xZZ;D"] {
                match parse(bad) {
                    Err(EwsError::MalformedXml(_)) => {}
                    other => panic!("{parser} parsed {bad:?} as {:?}", other.map(|_| ())),
                }
            }
        }
    }
}
