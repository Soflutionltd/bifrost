//! Gmail PIM primitives for the shared `Account` trait.
//!
//! Audit boundary: the 2026-07 google bug sweep did NOT line-audit this
//! module - specifically the MIME rendering, draft patching, search
//! translation, and identity and vacation mapping in it. That is a statement
//! about where a past sweep's coverage stops, not a claim that anything here
//! is wrong; a later auditor should treat this file as unswept by that pass
//! and read it fresh rather than assuming it was already walked.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bifrost_types::{
    AccountError, AccountFuture, AccountStream, Address, AttachmentHandle, AttachmentInline,
    Container, ContainerId, ContainerKind, ContainerList, ContainerStyle, DraftHandle, DraftPatch,
    FolderRole, HydrationProjection, Identity, IdentityId, IdentityPatch, Importance, Message,
    MutationTarget, ObjectId, Page, ProtocolKind, Provenance, QuotaInfo, SearchFilter,
    SearchRequest, SendRequest, ThreadHydration, ThreadId, VacationConfig,
};
use bytes::Bytes;
use jiff::Timestamp;
use jiff::tz::Offset;
use serde_json::json;

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::headers::find_header_value_case_insensitive;
use crate::types::{
    GmailHeader, GmailLabel, GmailLabelColor, GmailMessage, GmailPayload, GmailVacationSettings,
};

use super::blobs;
use super::error;
use super::flags;
use super::flags::{ARCHIVE_ID, LABEL_INBOX, LABEL_SPAM, LABEL_TRASH, is_archive_id};
use super::scopes::{ScopeCache, labels_for_flags, refresh_scope_snapshot};

const LABEL_SENT: &str = "SENT";
const LABEL_DRAFT: &str = "DRAFT";
const LABEL_UNREAD: &str = "UNREAD";
const MAX_GMAIL_PAGE_SIZE: u32 = 500;

pub(crate) fn add_to_container(
    client: Arc<GmailClient>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = add_container_patch(&container);
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::AddToContainer,
        )
        .await
    })
}

pub(crate) fn remove_from_container(
    client: Arc<GmailClient>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = remove_container_patch(&container);
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::RemoveFromContainer,
        )
        .await
    })
}

pub(crate) fn set_label_membership(
    client: Arc<GmailClient>,
    target: MutationTarget,
    label: ContainerId,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = if value {
            add_container_patch(&label)
        } else {
            remove_container_patch(&label)
        };
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::SetLabelMembership,
        )
        .await
    })
}

pub(crate) fn set_is_read(
    client: Arc<GmailClient>,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let add = if is_read {
            Vec::new()
        } else {
            vec![LABEL_UNREAD.to_string()]
        };
        let remove = if is_read {
            vec![LABEL_UNREAD.to_string()]
        } else {
            Vec::new()
        };
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::SetIsRead,
        )
        .await
    })
}

pub(crate) fn send_message(
    client: Arc<GmailClient>,
    default_address: String,
    request: SendRequest,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        if !request.attachments_uploaded.is_empty() {
            return Err(unsupported(
                bifrost_types::AccountOperation::AttachmentUpload,
            ));
        }
        if let Some(err) = scheduled_send_guard(&request) {
            return Err(err);
        }
        if let Some(err) = send_as_guard(&request) {
            return Err(err);
        }
        let doc = MailDocument::from_send(request, &default_address);
        let raw = render_message(&doc, &default_address, true)?;
        let message = client
            .send_message(&raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::send()))?;
        Ok(ObjectId(message.id))
    })
}

pub(crate) fn send_raw_message(
    client: Arc<GmailClient>,
    raw: Bytes,
    _save_to_sent: Option<bool>,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        // Gmail `messages.send` takes the verbatim RFC 5322 octets as a
        // base64url (no-pad) string and learns recipients + Bcc from the
        // message headers. It always files the result in Sent, so
        // `save_to_sent` has no Gmail-side toggle (matches send_message).
        let encoded = URL_SAFE_NO_PAD.encode(&raw);
        let message = client
            .send_message(&encoded, None)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::send()))?;
        Ok(ObjectId(message.id))
    })
}

pub(crate) fn attachment_upload(
    _bytes: AccountStream<Result<Bytes, AccountError>>,
    _mime: String,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    Box::pin(async {
        Err(unsupported(
            bifrost_types::AccountOperation::AttachmentUpload,
        ))
    })
}

pub(crate) fn draft_create(
    client: Arc<GmailClient>,
    default_address: String,
    patch: DraftPatch,
) -> AccountFuture<Result<DraftHandle, AccountError>> {
    Box::pin(async move {
        let doc = MailDocument::from_draft_patch(patch, &default_address);
        let raw = render_message(&doc, &default_address, false)?;
        let draft = client
            .create_draft(&raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| {
                account_error_for(
                    e,
                    error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftCreate),
                )
            })?;
        Ok(DraftHandle(draft.id))
    })
}

pub(crate) fn draft_update(
    client: Arc<GmailClient>,
    default_address: String,
    draft: DraftHandle,
    patch: DraftPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ctx = error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftUpdate);
        let existing = client
            .get_draft(&draft.0, "full")
            .await
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        let mut doc = document_from_message(&client, &existing.message, true).await?;
        doc.apply_patch(patch);
        let raw = render_message(&doc, &default_address, false)?;
        client
            .update_draft(&draft.0, &raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| account_error_for(e, ctx))?;
        Ok(())
    })
}

pub(crate) fn draft_discard(
    client: Arc<GmailClient>,
    draft: DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        client.delete_draft(&draft.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftDiscard),
            )
        })
    })
}

pub(crate) fn draft_send(
    client: Arc<GmailClient>,
    draft: DraftHandle,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let message = client.send_draft(&draft.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftSend),
            )
        })?;
        Ok(ObjectId(message.id))
    })
}

pub(crate) fn search(
    client: Arc<GmailClient>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        let query = gmail_query(&request)?;
        let page_token = page_token(&request)?;
        let max = request
            .limit
            .unwrap_or(MAX_GMAIL_PAGE_SIZE)
            .min(MAX_GMAIL_PAGE_SIZE);
        // Gmail treats maxResults=0 as "use the default page size", so a
        // zero limit must short-circuit locally to mean "return nothing".
        if max == 0 {
            return Ok(Page {
                truncated: false,
                items: Vec::new(),
                next_cursor: None,
                estimated_total: None,
                failed_ids: Vec::new(),
                skipped_scopes: Vec::new(),
            });
        }
        let (threads, next) = client
            .list_threads(query.as_deref(), Some(max), page_token.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::search()))?;
        Ok(Page {
            truncated: false,
            items: threads
                .into_iter()
                .map(|thread| ThreadId(thread.id))
                .collect(),
            next_cursor: next.map(String::into_bytes),
            estimated_total: None,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

pub(crate) fn search_messages(
    client: Arc<GmailClient>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        let query = gmail_query(&request)?;
        let page_token = page_token(&request)?;
        let max = request
            .limit
            .unwrap_or(MAX_GMAIL_PAGE_SIZE)
            .min(MAX_GMAIL_PAGE_SIZE);
        // Gmail treats maxResults=0 as "use the default page size", so a
        // zero limit must short-circuit locally to mean "return nothing".
        if max == 0 {
            return Ok(Page {
                truncated: false,
                items: Vec::new(),
                next_cursor: None,
                estimated_total: None,
                failed_ids: Vec::new(),
                skipped_scopes: Vec::new(),
            });
        }
        let (messages, next, estimate) = client
            .list_messages(query.as_deref(), Some(max), page_token.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::search_messages()))?;
        Ok(Page {
            truncated: false,
            items: messages
                .into_iter()
                .map(|message| ObjectId(message.id))
                .collect(),
            next_cursor: next.map(String::into_bytes),
            estimated_total: estimate.and_then(non_negative_i64),
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

pub(crate) fn containers_list(
    client: Arc<GmailClient>,
    cache: ScopeCache,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    Box::pin(async move {
        let snapshot = refresh_scope_snapshot(&client, &cache)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::containers_list()))?;
        let mut containers = Vec::with_capacity(snapshot.labels.len() + 1);
        containers.push(archive_container());
        containers.extend(snapshot.labels.iter().map(container_from_label));
        // Single-namespace enumeration: one `labels.list` answers for
        // the whole account, so there is never a skipped namespace.
        Ok(ContainerList::complete(containers))
    })
}

pub(crate) fn container_create(
    client: Arc<GmailClient>,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    style: Option<ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if parent.is_some() || !matches!(kind, ContainerKind::Label) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerCreate,
            ));
        }
        // Gmail's create takes (text_color, background_color).
        let color = style
            .as_ref()
            .map(|s| (s.color_fg.as_str(), s.color_bg.as_str()));
        let label = client.create_label(&name, color).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::container(
                    bifrost_types::AccountOperation::ContainerCreate,
                ),
            )
        })?;
        Ok(ContainerId(label.id))
    })
}

pub(crate) fn container_rename(
    client: Arc<GmailClient>,
    container: ContainerId,
    name: String,
    style: Option<ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if is_archive_id(&container.0) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerRename,
            ));
        }
        // `update_label`'s color is `Option<Option<(text, bg)>>`: outer
        // `None` leaves the color untouched, `Some(Some(..))` recolors.
        // A recolor with no name change still rides this rename path.
        let color = style
            .as_ref()
            .map(|s| Some((s.color_fg.as_str(), s.color_bg.as_str())));
        client
            .update_label(&container.0, Some(&name), color)
            .await
            .map_err(|e| {
                account_error_for(
                    e,
                    error::GmailErrorContext::container(
                        bifrost_types::AccountOperation::ContainerRename,
                    ),
                )
            })?;
        Ok(())
    })
}

pub(crate) fn container_move(
    _container: ContainerId,
    _new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainerMove)) })
}

pub(crate) fn container_delete(
    client: Arc<GmailClient>,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if is_archive_id(&container.0) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerDelete,
            ));
        }
        client.delete_label(&container.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::container(
                    bifrost_types::AccountOperation::ContainerDelete,
                ),
            )
        })
    })
}

pub(crate) fn identities_list(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
    Box::pin(async move {
        let identities = client
            .list_send_as()
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::identities_list()))?
            .into_iter()
            .map(|send_as| Identity {
                id: IdentityId(send_as.send_as_email.clone()),
                name: send_as.display_name.unwrap_or_default(),
                address: send_as.send_as_email,
                signature_text: None,
                signature_html: send_as.signature,
                reply_to: send_as.reply_to_address.map(Address::bare),
                is_default: send_as.is_default.unwrap_or(false),
            })
            .collect();
        Ok(identities)
    })
}

pub(crate) fn identity_update(
    client: Arc<GmailClient>,
    identity: IdentityId,
    patch: IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let body = identity_update_body(patch);
        if body.as_object().is_none_or(serde_json::Map::is_empty) {
            return Ok(());
        }
        client
            .patch_send_as(&identity.0, &body)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::identity_update()))?;
        Ok(())
    })
}

fn identity_update_body(patch: IdentityPatch) -> serde_json::Value {
    let mut body = json!({});
    if let Some(name) = patch.name {
        body["displayName"] = json!(name);
    }
    if let Some(signature) = patch.signature_html {
        body["signature"] = signature.map_or(serde_json::Value::Null, serde_json::Value::String);
    } else if let Some(signature) = patch.signature_text {
        // Gmail stores this field as HTML. Preserve the shared patch's
        // plain-text semantics by escaping content and converting line breaks.
        body["signature"] = signature.map_or(serde_json::Value::Null, |text| {
            serde_json::Value::String(signature_text_as_html(&text))
        });
    }
    if let Some(reply_to) = patch.reply_to {
        body["replyToAddress"] =
            reply_to.map_or(serde_json::Value::Null, |addr| json!(addr.address));
    }
    if let Some(is_default) = patch.is_default {
        body["isDefault"] = json!(is_default);
    }
    body
}

fn signature_text_as_html(text: &str) -> String {
    let mut html = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '&' => html.push_str("&amp;"),
            '<' => html.push_str("&lt;"),
            '>' => html.push_str("&gt;"),
            '"' => html.push_str("&quot;"),
            '\'' => html.push_str("&#39;"),
            '\r' => {
                if characters.peek() == Some(&'\n') {
                    characters.next();
                }
                html.push_str("<br>");
            }
            '\n' => html.push_str("<br>"),
            other => html.push(other),
        }
    }
    html
}

pub(crate) fn vacation_get(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async move {
        let settings = client
            .get_vacation()
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::vacation_get()))?;
        Ok(Some(VacationConfig {
            is_enabled: settings.enable_auto_reply.unwrap_or(false),
            subject: settings.response_subject,
            body_text: settings.response_body_plain_text,
            body_html: settings.response_body_html,
            starts_at: settings
                .start_time
                .as_deref()
                .and_then(system_time_from_millis),
            ends_at: settings
                .end_time
                .as_deref()
                .and_then(system_time_from_millis),
        }))
    })
}

pub(crate) fn vacation_set(
    client: Arc<GmailClient>,
    config: VacationConfig,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let settings = GmailVacationSettings {
            enable_auto_reply: Some(config.is_enabled),
            response_subject: config.subject,
            response_body_plain_text: config.body_text,
            response_body_html: config.body_html,
            start_time: config.starts_at.map(system_time_to_millis_string),
            end_time: config.ends_at.map(system_time_to_millis_string),
        };
        client
            .update_vacation(&settings)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::vacation_set()))?;
        Ok(())
    })
}

pub(crate) fn quota_get() -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::QuotaGet)) })
}

pub(crate) fn thread_hydrate(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        let labels = labels_for_flags(&client, &cache).await.map_err(|error| {
            account_error_for(
                error,
                error::GmailErrorContext::hydrate_thread(thread.0.clone()),
            )
        })?;
        let gmail_thread = client.get_thread(&thread.0, "full").await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::hydrate_thread(thread.0.clone()),
            )
        })?;
        let mut messages = Vec::with_capacity(gmail_thread.messages.len());
        for message in &gmail_thread.messages {
            messages.push(message_from_gmail(&labels, message, HydrationProjection::Full).await?);
        }
        Ok(ThreadHydration {
            id: ThreadId(gmail_thread.id),
            messages,
        })
    })
}

pub(crate) fn message_hydrate(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        let labels = labels_for_flags(&client, &cache).await.map_err(|error| {
            account_error_for(
                error,
                error::GmailErrorContext::hydrate_message(message.0.clone()),
            )
        })?;
        let format = match projection {
            HydrationProjection::Headers | HydrationProjection::Preview(_) => "metadata",
            HydrationProjection::Full | HydrationProjection::FullWithBlobs => "full",
            _ => "full",
        };
        let gmail_message = client.get_message(&message.0, format).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::hydrate_message(message.0.clone()),
            )
        })?;
        message_from_gmail(&labels, &gmail_message, projection).await
    })
}

pub(crate) fn move_thread(
    client: Arc<GmailClient>,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // One `threads.modify`: the add and every removal (the exclusive
        // display containers plus the caller's source label) ride the
        // same request, so there is no window where the thread sits in
        // both containers.
        let (add, remove) = move_container_patch(&target, source.as_ref());
        modify_target(
            &client,
            MutationTarget::Thread(thread),
            add,
            remove,
            bifrost_types::AccountOperation::BulkMove,
        )
        .await
    })
}

pub(crate) fn delete_thread(
    client: Arc<GmailClient>,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if current
            .as_ref()
            .is_some_and(|id| id.0.eq_ignore_ascii_case(LABEL_TRASH))
        {
            return client.delete_thread(&thread.0).await.map_err(|e| {
                account_error_for(
                    e,
                    error::GmailErrorContext::mutation(
                        bifrost_types::AccountOperation::BulkDestroy,
                    ),
                )
            });
        }
        let (add, remove) = move_container_patch(&ContainerId(LABEL_TRASH.to_string()), None);
        modify_target(
            &client,
            MutationTarget::Thread(thread),
            add,
            remove,
            bifrost_types::AccountOperation::BulkMove,
        )
        .await
    })
}

async fn modify_target(
    client: &GmailClient,
    target: MutationTarget,
    add_labels: Vec<String>,
    remove_labels: Vec<String>,
    operation: bifrost_types::AccountOperation,
) -> Result<(), AccountError> {
    if add_labels.is_empty() && remove_labels.is_empty() {
        return Ok(());
    }
    match target {
        MutationTarget::Message(id) => {
            client
                .modify_message(&id.0, &add_labels, &remove_labels)
                .await
                .map_err(|e| account_error_for(e, error::GmailErrorContext::base(operation)))?;
        }
        MutationTarget::Thread(id) => {
            client
                .modify_thread(&id.0, &add_labels, &remove_labels)
                .await
                .map_err(|e| account_error_for(e, error::GmailErrorContext::base(operation)))?;
        }
        _ => return Err(unsupported(operation)),
    }
    Ok(())
}

fn add_container_patch(container: &ContainerId) -> (Vec<String>, Vec<String>) {
    if is_archive_id(&container.0) {
        // Archive is not a label - it is the absence of every exclusive
        // display container - so "add to archive" is only expressible
        // as a relocation.
        return move_container_patch(container, None);
    }
    (vec![container.0.clone()], Vec::new())
}

/// Single-object relocation, sharing
/// [`flags::move_placement_patch`] with the bulk `batchModify` driver
/// so a consumer gets the same wire semantics whichever entry point it
/// reached: the exclusive display containers the message is leaving are
/// stripped, and the synthetic `archive` id never reaches
/// `addLabelIds`.
///
/// `source` adds the user label being filed out of, in the SAME modify
/// call - the destination alone cannot imply it.
fn move_container_patch(
    target: &ContainerId,
    source: Option<&ContainerId>,
) -> (Vec<String>, Vec<String>) {
    let mut patch = flags::move_placement_patch(&target.0);
    if let Some(source) = source {
        let redundant = is_archive_id(&source.0)
            || source.0.eq_ignore_ascii_case(&target.0)
            || patch
                .remove_label_ids
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(&source.0));
        if !redundant {
            patch.remove_label_ids.push(source.0.clone());
        }
    }
    (patch.add_label_ids, patch.remove_label_ids)
}

fn remove_container_patch(container: &ContainerId) -> (Vec<String>, Vec<String>) {
    if is_archive_id(&container.0) {
        (Vec::new(), Vec::new())
    } else {
        (Vec::new(), vec![container.0.clone()])
    }
}

fn gmail_query(request: &SearchRequest) -> Result<Option<String>, AccountError> {
    let mut parts = Vec::new();
    if let Some(filter) = &request.filter {
        parts.push(filter_query(filter)?);
    }
    if let Some(provider) = &request.provider_query
        && !provider.trim().is_empty()
    {
        parts.push(provider.trim().to_string());
    }
    Ok((!parts.is_empty()).then(|| parts.join(" ")))
}

fn filter_query(filter: &SearchFilter) -> Result<String, AccountError> {
    match filter {
        SearchFilter::From(value) => Ok(format!("from:{}", query_term(value))),
        SearchFilter::To(value) => Ok(format!("to:{}", query_term(value))),
        SearchFilter::Subject(value) => Ok(format!("subject:{}", query_term(value))),
        SearchFilter::Body(value) => Ok(query_term(value)),
        SearchFilter::Has(value) => {
            if value.eq_ignore_ascii_case("attachment") {
                Ok("has:attachment".to_string())
            } else {
                Ok(format!("filename:{}", query_term(value)))
            }
        }
        SearchFilter::In(container) => Ok(container_query(&container.0)),
        SearchFilter::Labeled(label) => Ok(format!("label:{}", query_term(&label.0))),
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("after:{}", gmail_date(*after)));
            }
            if let Some(before) = before {
                parts.push(format!("before:{}", gmail_date(*before)));
            }
            Ok(parts.join(" "))
        }
        SearchFilter::And(filters) => filters
            .iter()
            .map(filter_query)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| items.join(" ")),
        SearchFilter::Or(filters) => filters
            .iter()
            .map(filter_query)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| format!("({})", items.join(" OR "))),
        SearchFilter::Not(filter) => Ok(format!("-({})", filter_query(filter)?)),
        _ => Err(unsupported(bifrost_types::AccountOperation::Search)),
    }
}

fn page_token(request: &SearchRequest) -> Result<Option<String>, AccountError> {
    request
        .page_cursor
        .as_ref()
        .map(|cursor| {
            String::from_utf8(cursor.clone()).map_err(|error| {
                other_error(
                    bifrost_types::AccountOperation::Search,
                    format!("invalid gmail page cursor: {error}"),
                )
            })
        })
        .transpose()
}

fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], " ").trim().to_string()
}

fn query_term(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if sanitized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '@' | '/' | ':'))
    {
        sanitized
    } else {
        format!(
            "\"{}\"",
            sanitized.replace('\\', "\\\\").replace('"', "\\\"")
        )
    }
}

fn container_query(id: &str) -> String {
    match id {
        LABEL_INBOX => "in:inbox".to_string(),
        LABEL_SENT => "in:sent".to_string(),
        LABEL_DRAFT => "in:drafts".to_string(),
        LABEL_TRASH => "in:trash".to_string(),
        LABEL_SPAM => "in:spam".to_string(),
        _ if is_archive_id(id) => "-in:inbox -in:sent -in:drafts -in:trash -in:spam".to_string(),
        _ => format!("label:{}", query_term(id)),
    }
}

fn gmail_date(time: SystemTime) -> String {
    let dt = Offset::UTC
        .to_datetime(Timestamp::try_from(time).unwrap_or(Timestamp::UNIX_EPOCH))
        .date();
    format!("{:04}/{:02}/{:02}", dt.year(), dt.month(), dt.day())
}

fn archive_container() -> Container {
    // Synthetic bifrost container (Gmail models archive as the absence of
    // INBOX, not a real label), so it takes every `Container::new`
    // default: no Gmail color, not a Gmail-native system label, no ACL /
    // subscription / namespace metadata (Gmail is personal-only).
    Container::new(
        ContainerId(ARCHIVE_ID.to_string()),
        ContainerKind::Label,
        Some(FolderRole::Archive),
        Provenance {
            provider: ProtocolKind::Gmail,
            kind: ContainerKind::Label,
            native: ARCHIVE_ID.to_string(),
        },
        "Archive".to_string(),
        None,
    )
}

fn container_from_label(label: &GmailLabel) -> Container {
    let role = match label.id.as_str() {
        LABEL_INBOX => Some(FolderRole::Inbox),
        LABEL_SENT => Some(FolderRole::Sent),
        LABEL_DRAFT => Some(FolderRole::Drafts),
        LABEL_TRASH => Some(FolderRole::Trash),
        LABEL_SPAM => Some(FolderRole::Spam),
        _ => None,
    };
    // Gmail is personal-only and has no per-folder ACL / subscription
    // model, so namespace / owner / rights / subscription all stay at
    // their `Container::new` defaults.
    Container::new(
        ContainerId(label.id.clone()),
        ContainerKind::Label,
        role,
        Provenance {
            provider: ProtocolKind::Gmail,
            kind: ContainerKind::Label,
            native: label.id.clone(),
        },
        label.name.clone(),
        None,
    )
    .with_style(style_from_label_color(label.color.as_ref()))
    // Gmail tags far more labels as system (CATEGORY_*, IMPORTANT, CHAT,
    // ...) than ever receive a `role`, so carry the native
    // `type == "system"` bit through so the consumer can reproduce
    // Gmail's system-label-as-folder split, which `role` alone
    // (INBOX/SENT/DRAFT/TRASH/SPAM only) cannot.
    .with_system(label_is_system(label))
}

/// Map a Gmail label `color` object into a `ContainerStyle`. Yields
/// `None` when the label has no color or the pair is incomplete; Gmail
/// always emits both members when it emits the object at all, but we
/// stay defensive against a partial payload.
fn style_from_label_color(color: Option<&GmailLabelColor>) -> Option<ContainerStyle> {
    let color = color?;
    let bg = color.background_color.clone()?;
    let fg = color.text_color.clone()?;
    Some(ContainerStyle {
        color_bg: bg,
        color_fg: fg,
    })
}

/// True iff Gmail marks the label `type == "system"`.
fn label_is_system(label: &GmailLabel) -> bool {
    label
        .label_type
        .as_deref()
        .is_some_and(|label_type| label_type == "system")
}

async fn message_from_gmail(
    labels: &[GmailLabel],
    message: &GmailMessage,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    let headers = message_headers(message);
    let (body_text, body_html) = match projection {
        HydrationProjection::Headers => (None, None),
        HydrationProjection::Preview(limit) => (
            Some(message.snippet.chars().take(limit).collect::<String>()),
            None,
        ),
        HydrationProjection::Full | HydrationProjection::FullWithBlobs => {
            body_parts(message.payload.as_ref())
        }
        _ => body_parts(message.payload.as_ref()),
    };
    let attachments = if matches!(
        projection,
        HydrationProjection::Full | HydrationProjection::FullWithBlobs
    ) {
        blobs::attachments_for_message(message)
    } else {
        Vec::new()
    };
    let flags = flags::flag_set(&message.label_ids, labels);

    Ok(Message {
        id: ObjectId(message.id.clone()),
        thread_id: Some(ThreadId(message.thread_id.clone())),
        from: parse_address_list(header_from(headers, "From").as_deref()),
        to: parse_address_list(header_from(headers, "To").as_deref()),
        cc: parse_address_list(header_from(headers, "Cc").as_deref()),
        bcc: parse_address_list(header_from(headers, "Bcc").as_deref()),
        reply_to: parse_address_list(header_from(headers, "Reply-To").as_deref()),
        subject: header_from(headers, "Subject"),
        date: internal_date(&message.internal_date),
        containers: message
            .label_ids
            .iter()
            .map(|id| ContainerId(id.clone()))
            .collect(),
        flags,
        // Gmail has no importance field; always Normal.
        importance: Importance::Normal,
        body_text,
        body_html,
        attachments,
        incomplete: false,
        size_bytes: message.size_estimate.and_then(non_negative_i64),
        in_reply_to: header_from(headers, "In-Reply-To"),
        references: references_from_header(header_from(headers, "References").as_deref()),
    })
}

async fn document_from_message(
    client: &GmailClient,
    message: &GmailMessage,
    include_attachments: bool,
) -> Result<MailDocument, AccountError> {
    let headers = message_headers(message);
    let (body_text, body_html) = body_parts(message.payload.as_ref());
    let attachments_inline = if include_attachments {
        attachment_inlines(client, message).await?
    } else {
        Vec::new()
    };
    Ok(MailDocument {
        identity: None,
        from: parse_address_list(header_from(headers, "From").as_deref())
            .into_iter()
            .next(),
        to: parse_address_list(header_from(headers, "To").as_deref()),
        cc: parse_address_list(header_from(headers, "Cc").as_deref()),
        bcc: parse_address_list(header_from(headers, "Bcc").as_deref()),
        reply_to: parse_address_list(header_from(headers, "Reply-To").as_deref()),
        subject: header_from(headers, "Subject"),
        body_text,
        body_html,
        attachments_inline,
        attachments_uploaded: Vec::new(),
        in_reply_to: header_from(headers, "In-Reply-To"),
        references: references_from_header(header_from(headers, "References").as_deref()),
        thread_id: Some(message.thread_id.clone()),
        request_read_receipt: false,
    })
}

async fn attachment_inlines(
    client: &GmailClient,
    message: &GmailMessage,
) -> Result<Vec<AttachmentInline>, AccountError> {
    let mut refs = Vec::new();
    if let Some(payload) = &message.payload {
        collect_attachment_refs(payload, &mut refs);
    }
    let mut attachments = Vec::with_capacity(refs.len());
    // attachment_inlines is used during draft hydration (draft_update);
    // the error context is DraftUpdate.
    let ctx = error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftUpdate);
    for item in refs {
        let attachment = client
            .get_attachment(&message.id, &item.attachment_id)
            .await
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        let data = decode_base64url_nopad(&attachment.data)
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        attachments.push(AttachmentInline {
            filename: item.filename,
            mime: item.mime,
            data: Bytes::from(data),
            inline: item.inline,
            content_id: item.content_id,
        });
    }
    Ok(attachments)
}

struct AttachmentRef {
    attachment_id: String,
    filename: String,
    mime: String,
    inline: bool,
    content_id: Option<String>,
}

fn collect_attachment_refs(part: &GmailPayload, refs: &mut Vec<AttachmentRef>) {
    if let Some(body) = &part.body
        && let Some(attachment_id) = &body.attachment_id
    {
        refs.push(AttachmentRef {
            attachment_id: attachment_id.clone(),
            filename: part.filename.clone(),
            mime: part.mime_type.clone(),
            inline: content_disposition_inline(&part.headers),
            content_id: content_id_value(&part.headers),
        });
    }
    for child in &part.parts {
        collect_attachment_refs(child, refs);
    }
}

/// Extract the bare `Content-ID` value (angle brackets stripped) from a
/// part's headers, so a hydrated draft re-render preserves the `cid:`
/// linkage of an inline image.
fn content_id_value(headers: &[GmailHeader]) -> Option<String> {
    header_from(headers, "Content-ID").map(|value| {
        value
            .trim()
            .trim_matches(|c| c == '<' || c == '>')
            .to_string()
    })
}

fn content_disposition_inline(headers: &[GmailHeader]) -> bool {
    header_from(headers, "Content-Disposition")
        .as_deref()
        .is_some_and(|value| value.to_ascii_lowercase().contains("inline"))
}

fn body_parts(payload: Option<&GmailPayload>) -> (Option<String>, Option<String>) {
    let mut text = None;
    let mut html = None;
    if let Some(payload) = payload {
        collect_body_parts(payload, &mut text, &mut html);
    }
    (text, html)
}

fn collect_body_parts(part: &GmailPayload, text: &mut Option<String>, html: &mut Option<String>) {
    if let Some(data) = part.body.as_ref().and_then(|body| body.data.as_deref())
        && (text.is_none() || html.is_none())
        && let Ok(bytes) = decode_base64url_nopad(data)
        && let Ok(decoded) = String::from_utf8(bytes)
    {
        if part.mime_type.eq_ignore_ascii_case("text/plain") && text.is_none() {
            *text = Some(decoded);
        } else if part.mime_type.eq_ignore_ascii_case("text/html") && html.is_none() {
            *html = Some(decoded);
        }
    }
    for child in &part.parts {
        collect_body_parts(child, text, html);
    }
}

fn message_headers(message: &GmailMessage) -> &[GmailHeader] {
    message
        .payload
        .as_ref()
        .map_or(&[] as &[GmailHeader], |payload| payload.headers.as_slice())
}

fn header_from(headers: &[GmailHeader], name: &str) -> Option<String> {
    find_header_value_case_insensitive(
        headers,
        name,
        |header| header.name.as_str(),
        |header| header.value.as_str(),
    )
}

fn internal_date(value: &Option<String>) -> Option<SystemTime> {
    value
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
}

fn references_from_header(value: Option<&str>) -> Vec<String> {
    value.map_or_else(Vec::new, |value| {
        value
            .split_whitespace()
            .map(|item| item.trim_matches(|ch| ch == '<' || ch == '>').to_string())
            .filter(|item| !item.is_empty())
            .collect()
    })
}

/// Minimal RFC 5322 address-list parser (recipient addr-specs and an
/// optional display name), mirroring the imap crate's.
///
/// Commas inside a quoted display name or inside the angle-bracketed
/// addr-spec do not split addresses. This sits on a WRITE path:
/// `draft_update` re-renders the parsed headers back into the stored
/// draft, so a naive comma split would silently corrupt any draft
/// addressed to `"Last, First" <a@b>` on the next field-level patch.
fn parse_address_list(value: Option<&str>) -> Vec<Address> {
    value.map_or_else(Vec::new, |value| {
        split_address_list(value)
            .into_iter()
            .filter_map(|item| parse_address(&item))
            .collect()
    })
}

fn parse_address(value: &str) -> Option<Address> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Some((name, rest)) = value.split_once('<')
        && let Some((address, _)) = rest.split_once('>')
    {
        let name = name.trim().trim_matches('"').trim();
        // A quoted display name escapes embedded quotes and backslashes;
        // resolve them so the model (and the re-render, which re-quotes)
        // sees the literal name rather than the wire escapes.
        let name = name.replace("\\\"", "\"").replace("\\\\", "\\");
        return Some(Address {
            name: (!name.is_empty()).then_some(name),
            address: address.trim().to_string(),
        });
    }
    Some(Address::bare(value.to_string()))
}

/// Split an address-list header value on the top-level commas only:
/// commas inside a `"..."` quoted display name or inside a `<...>`
/// addr-spec are not separators.
fn split_address_list(value: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut in_angle = false;
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            escaped = false;
            current.push(ch);
            continue;
        }
        match ch {
            '\\' if in_quotes => {
                escaped = true;
                current.push(ch);
            }
            '"' if !in_angle => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            '<' if !in_quotes => {
                in_angle = true;
                current.push(ch);
            }
            '>' if !in_quotes => {
                in_angle = false;
                current.push(ch);
            }
            ',' if !in_quotes && !in_angle => {
                items.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    items.push(current);
    items
}

#[derive(Debug, Clone, Default)]
struct MailDocument {
    identity: Option<IdentityId>,
    from: Option<Address>,
    to: Vec<Address>,
    cc: Vec<Address>,
    bcc: Vec<Address>,
    reply_to: Vec<Address>,
    subject: Option<String>,
    body_text: Option<String>,
    body_html: Option<String>,
    attachments_inline: Vec<AttachmentInline>,
    attachments_uploaded: Vec<AttachmentHandle>,
    in_reply_to: Option<String>,
    references: Vec<String>,
    thread_id: Option<String>,
    /// RFC 8098 read-receipt request, carried from `SendRequest`. Drafts
    /// hydrated from an existing message default to `false`.
    request_read_receipt: bool,
}

impl MailDocument {
    fn from_send(request: SendRequest, default_address: &str) -> Self {
        let mut doc = Self {
            identity: request.identity,
            from: request.from,
            to: request.to,
            cc: request.cc,
            bcc: request.bcc,
            reply_to: request.reply_to,
            subject: request.subject,
            body_text: request.body_text,
            body_html: request.body_html,
            attachments_inline: request.attachments_inline,
            attachments_uploaded: request.attachments_uploaded,
            in_reply_to: request.in_reply_to,
            references: request.references,
            thread_id: None,
            request_read_receipt: request.request_read_receipt,
        };
        doc.ensure_from(default_address);
        doc
    }

    fn from_draft_patch(patch: DraftPatch, default_address: &str) -> Self {
        let mut doc = Self {
            identity: patch.identity,
            from: patch.from.flatten(),
            to: patch.to.unwrap_or_default(),
            cc: patch.cc.unwrap_or_default(),
            bcc: patch.bcc.unwrap_or_default(),
            reply_to: patch.reply_to.unwrap_or_default(),
            subject: patch.subject.flatten(),
            body_text: patch.body_text.flatten(),
            body_html: patch.body_html.flatten(),
            attachments_inline: patch.attachments_inline.unwrap_or_default(),
            attachments_uploaded: patch.attachments_uploaded.unwrap_or_default(),
            in_reply_to: patch.in_reply_to.flatten(),
            references: patch.references.unwrap_or_default(),
            thread_id: None,
            // DraftPatch carries no read-receipt request; that field lives
            // on SendRequest only.
            request_read_receipt: false,
        };
        doc.ensure_from(default_address);
        doc
    }

    fn apply_patch(&mut self, patch: DraftPatch) {
        if let Some(identity) = patch.identity {
            self.identity = Some(identity);
        }
        if let Some(from) = patch.from {
            self.from = from;
        }
        if let Some(to) = patch.to {
            self.to = to;
        }
        if let Some(cc) = patch.cc {
            self.cc = cc;
        }
        if let Some(bcc) = patch.bcc {
            self.bcc = bcc;
        }
        if let Some(reply_to) = patch.reply_to {
            self.reply_to = reply_to;
        }
        if let Some(subject) = patch.subject {
            self.subject = subject;
        }
        if let Some(body_text) = patch.body_text {
            self.body_text = body_text;
        }
        if let Some(body_html) = patch.body_html {
            self.body_html = body_html;
        }
        if let Some(attachments_inline) = patch.attachments_inline {
            self.attachments_inline = attachments_inline;
        }
        if let Some(attachments_uploaded) = patch.attachments_uploaded {
            self.attachments_uploaded = attachments_uploaded;
        }
        if let Some(in_reply_to) = patch.in_reply_to {
            self.in_reply_to = in_reply_to;
        }
        if let Some(references) = patch.references {
            self.references = references;
        }
    }

    fn ensure_from(&mut self, default_address: &str) {
        if self.from.is_none() {
            let address = self
                .identity
                .as_ref()
                .map_or_else(|| default_address.to_string(), |id| id.0.clone());
            self.from = Some(Address::bare(address));
        }
    }
}

fn render_message(
    doc: &MailDocument,
    default_address: &str,
    require_recipient: bool,
) -> Result<String, AccountError> {
    if require_recipient && doc.to.is_empty() && doc.cc.is_empty() && doc.bcc.is_empty() {
        return Err(other_error(
            bifrost_types::AccountOperation::Send,
            "gmail send requires at least one recipient",
        ));
    }
    if !doc.attachments_uploaded.is_empty() {
        return Err(unsupported(
            bifrost_types::AccountOperation::AttachmentUpload,
        ));
    }

    let from = doc
        .from
        .clone()
        .unwrap_or_else(|| Address::bare(default_address.to_string()));
    // Gmail learns the Bcc recipients from the message headers (it strips
    // the Bcc header itself before delivery), so the Google path emits it.
    // Gmail mints its own Message-ID, so none is requested here.
    let composed = bifrost_types::ComposedMessage {
        from: Some(&from),
        to: &doc.to,
        cc: &doc.cc,
        bcc: &doc.bcc,
        reply_to: &doc.reply_to,
        subject: doc.subject.as_deref(),
        body_text: doc.body_text.as_deref(),
        body_html: doc.body_html.as_deref(),
        attachments_inline: &doc.attachments_inline,
        in_reply_to: doc.in_reply_to.as_deref(),
        references: &doc.references,
        message_id: None,
        include_bcc_header: true,
        // RFC 8098 read receipt targets the resolved sender. Gmail honors
        // the header in the sent MIME. `None` when unrequested.
        disposition_notification_to: doc.request_read_receipt.then_some(&from),
    };
    let raw = bifrost_types::render_rfc5322(&composed);
    Ok(URL_SAFE_NO_PAD.encode(&raw))
}

fn system_time_to_millis_string(time: SystemTime) -> String {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    millis.to_string()
}

fn system_time_from_millis(value: &str) -> Option<SystemTime> {
    value
        .parse::<u64>()
        .ok()
        .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
}

fn non_negative_i64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

/// Gmail's REST API has no scheduled-send lever; a scheduled request is
/// rejected `Unsupported(Send)` rather than silently sent now.
fn scheduled_send_guard(request: &SendRequest) -> Option<AccountError> {
    request
        .scheduled
        .is_some()
        .then(|| unsupported(bifrost_types::AccountOperation::Send))
}

pub(crate) fn cancel_scheduled_send_unsupported() -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async {
        Err(unsupported(
            bifrost_types::AccountOperation::CancelScheduledSend,
        ))
    })
}

pub(crate) fn reschedule_send_unsupported() -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::RescheduleSend)) })
}

/// Gmail has no delegate-send REST surface in scope; a `send_as`
/// request is rejected `Unsupported(Send)` rather than silently sent
/// from the authenticated user's own mailbox.
fn send_as_guard(request: &SendRequest) -> Option<AccountError> {
    request
        .send_as
        .is_some()
        .then(|| unsupported(bifrost_types::AccountOperation::Send))
}

fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    error::into_account_error(
        crate::error::Error::unsupported(op),
        error::GmailErrorContext::base(op),
    )
}

fn other_error(op: bifrost_types::AccountOperation, detail: impl Into<String>) -> AccountError {
    error::into_account_error(
        crate::error::Error::invalid_request(op, detail),
        error::GmailErrorContext::base(op),
    )
}

/// Translate a `crate::Error` into `AccountError` with the given
/// per-call-site context. Every PIM call site uses this so the
/// operation, scope, and resource are correct for the classification.
fn account_error_for(error: crate::Error, ctx: error::GmailErrorContext) -> AccountError {
    error::into_account_error(error, ctx)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn gmail_message(value: serde_json::Value) -> GmailMessage {
        serde_json::from_value(value).expect("message fixture deserializes")
    }

    #[test]
    fn plain_text_identity_signatures_are_escaped_and_keep_line_breaks() {
        assert_eq!(
            signature_text_as_html("A <B> & \"C\" 'D'\r\nnext\n"),
            "A &lt;B&gt; &amp; &quot;C&quot; &#39;D&#39;<br>next<br>"
        );

        let mut patch = IdentityPatch::default();
        patch.signature_text = Some(Some("A <B>\n& C".to_string()));
        let body = identity_update_body(patch);
        assert_eq!(body["signature"].as_str(), Some("A &lt;B&gt;<br>&amp; C"));
    }

    #[test]
    fn an_explicit_html_identity_signature_takes_precedence() {
        let mut patch = IdentityPatch::default();
        patch.signature_html = Some(Some("<b>trusted HTML</b>".to_string()));
        patch.signature_text = Some(Some("plain text".to_string()));

        let body = identity_update_body(patch);
        assert_eq!(body["signature"].as_str(), Some("<b>trusted HTML</b>"));
    }

    #[tokio::test]
    async fn hydrate_full_reports_attachment_metadata_without_bytes() {
        let message = gmail_message(serde_json::json!({
            "id": "m1",
            "threadId": "t1",
            "payload": {
                "mimeType": "multipart/mixed",
                "headers": [],
                "parts": [{
                    "mimeType": "image/png",
                    "filename": "chart.png",
                    "headers": [
                        { "name": "Content-ID", "value": "<chart-image>" },
                        { "name": "Content-Disposition", "value": "inline; filename=chart.png" }
                    ],
                    "body": { "attachmentId": "att-1", "size": 42 }
                }]
            }
        }));

        let hydrated = message_from_gmail(&[], &message, HydrationProjection::Full)
            .await
            .expect("full hydration maps the parsed Gmail payload");

        assert_eq!(hydrated.attachments.len(), 1);
        let attachment = &hydrated.attachments[0];
        assert_eq!(attachment.filename.as_deref(), Some("chart.png"));
        assert_eq!(attachment.content_type.as_deref(), Some("image/png"));
        assert_eq!(attachment.content_id.as_deref(), Some("chart-image"));
        assert!(attachment.inline);
        assert_eq!(attachment.size, Some(42));
        assert!(!attachment.truncated);
        assert!(matches!(
            &attachment.source,
            bifrost_types::AttachmentSource::Blob(_)
        ));
    }

    /// A quoted display name legally contains commas (and escaped quotes).
    /// The comma split must not fire inside quotes or angle brackets: this
    /// parser feeds `draft_update`'s re-render, so a naive split writes the
    /// corrupted recipients back into the stored draft.
    #[test]
    fn address_list_respects_quoted_commas_and_escapes() {
        let parsed = parse_address_list(Some(
            "\"Doe, John\" <jd@example.com>, plain@example.com, \
             \"Escaped \\\"Nick\\\", Esq.\" <nick@example.com>",
        ));

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].name.as_deref(), Some("Doe, John"));
        assert_eq!(parsed[0].address, "jd@example.com");
        assert_eq!(parsed[1].name, None);
        assert_eq!(parsed[1].address, "plain@example.com");
        assert_eq!(parsed[2].name.as_deref(), Some("Escaped \"Nick\", Esq."));
        assert_eq!(parsed[2].address, "nick@example.com");
    }

    #[test]
    fn scheduled_send_request_is_unsupported() {
        let mut request = SendRequest::default();
        request.scheduled = Some(std::time::SystemTime::now() + Duration::from_secs(600));
        let err = scheduled_send_guard(&request).expect("scheduled request must be rejected");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(bifrost_types::AccountOperation::Send)
        ));

        // An immediate send passes the guard.
        assert!(scheduled_send_guard(&SendRequest::default()).is_none());
    }

    #[test]
    fn send_as_rejected_unsupported() {
        let mut request = SendRequest::default();
        request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::MailboxId(
            "shared@contoso.com".to_string(),
        )));
        let err = send_as_guard(&request).expect("send_as request must be rejected");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(bifrost_types::AccountOperation::Send)
        ));
        assert_eq!(err.operation(), Some(bifrost_types::AccountOperation::Send));

        // A personal send passes the guard.
        assert!(send_as_guard(&SendRequest::default()).is_none());
    }

    #[test]
    fn scheduled_send_capability_is_false() {
        let caps = crate::account::capabilities::gmail_capabilities();
        assert!(!caps.pim_methods.scheduled_send);
    }

    #[test]
    fn container_roles_map_gmail_system_labels() {
        let label = GmailLabel {
            id: LABEL_INBOX.to_string(),
            name: "Inbox".to_string(),
            label_type: Some("system".to_string()),
            color: None,
        };
        let container = container_from_label(&label);
        assert_eq!(container.role, Some(FolderRole::Inbox));
        assert_eq!(container.native_id, LABEL_INBOX);
        assert!(matches!(container.kind, ContainerKind::Label));
    }

    #[test]
    fn user_label_color_round_trips_into_container_style() {
        // Representative Gmail user-label payload carrying a color.
        let raw = r##"{
            "id": "Label_42",
            "name": "Work",
            "type": "user",
            "color": { "backgroundColor": "#fb4c2f", "textColor": "#ffffff" }
        }"##;
        let label: GmailLabel = serde_json::from_str(raw).expect("parse label");

        let container = container_from_label(&label);
        let style = container.style.expect("user label color carries a style");
        assert_eq!(style.color_bg, "#fb4c2f");
        assert_eq!(style.color_fg, "#ffffff");
        // A user label is not a Gmail system label.
        assert!(!container.system);
    }

    #[test]
    fn system_label_flagged_system_and_user_label_not() {
        let system = GmailLabel {
            id: "CATEGORY_PROMOTIONS".to_string(),
            name: "Promotions".to_string(),
            label_type: Some("system".to_string()),
            color: None,
        };
        let user = GmailLabel {
            id: "Label_42".to_string(),
            name: "Work".to_string(),
            label_type: Some("user".to_string()),
            color: None,
        };
        // A CATEGORY_* label has no `role` yet is a Gmail system label:
        // exactly the split that `system` carries and `role` cannot.
        let system_container = container_from_label(&system);
        assert!(system_container.system);
        assert_eq!(system_container.role, None);
        assert!(!container_from_label(&user).system);
        // An uncolored label yields no style.
        assert!(container_from_label(&user).style.is_none());
    }

    #[test]
    fn archive_container_is_explicit() {
        let container = archive_container();
        assert_eq!(container.id.0, ARCHIVE_ID);
        assert_eq!(container.role, Some(FolderRole::Archive));
        assert!(container.style.is_none());
        assert!(!container.system);
    }

    #[test]
    fn adding_archive_is_a_relocation_not_a_label() {
        let (add, remove) = add_container_patch(&ContainerId(ARCHIVE_ID.to_string()));
        assert!(add.is_empty());
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ]
        );
    }

    #[test]
    fn adding_a_user_label_does_not_relocate() {
        let (add, remove) = add_container_patch(&ContainerId("Label_42".to_string()));
        assert_eq!(add, vec!["Label_42".to_string()]);
        assert!(
            remove.is_empty(),
            "add_to_container is additive; only a move strips the inbox"
        );
    }

    #[test]
    fn single_object_move_matches_the_bulk_rule() {
        let (add, remove) = move_container_patch(&ContainerId("Label_42".to_string()), None);
        assert_eq!(add, vec!["Label_42".to_string()]);
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ],
            "the single-object and bulk builders must not disagree"
        );
    }

    #[test]
    fn single_object_move_folds_the_source_into_one_request() {
        let (_, remove) = move_container_patch(
            &ContainerId("Label_42".to_string()),
            Some(&ContainerId("Label_7".to_string())),
        );
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string(),
                "Label_7".to_string()
            ]
        );
    }

    #[test]
    fn single_object_move_to_archive_adds_no_label() {
        let (add, remove) = move_container_patch(&ContainerId(ARCHIVE_ID.to_string()), None);
        assert!(add.is_empty());
        assert_eq!(
            remove,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ]
        );
    }

    #[test]
    fn search_query_combines_structured_and_provider_terms() {
        let mut request = SearchRequest::default();
        request.filter = Some(SearchFilter::From("ada@example.com".to_string()));
        request.provider_query = Some("larger:5M".to_string());
        let query = gmail_query(&request).expect("query").expect("some query");
        assert_eq!(query, "from:ada@example.com larger:5M");
    }

    #[test]
    fn date_query_uses_gmail_slash_date() {
        let time = UNIX_EPOCH + Duration::from_secs(86_400);
        assert_eq!(gmail_date(time), "1970/01/02");
    }
}
