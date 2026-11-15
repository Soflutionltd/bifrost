//! Message and thread hydration and shared message projection.

use super::body::address_from_jmap;
use super::{to_acct_err, unix_to_system_time};
use crate::core::transport::HttpTransport;
use crate::email::{
    BodyProperty, Email, EmailBodyPart, EmailGet, EmailId, Property as EmailProperty,
};
use crate::thread::{ThreadGet, ThreadId as JmapThreadId};
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AttachmentSource, BlobCapabilities,
    BlobEncoding, BlobHandle, BlobId, ContainerId, HydrationProjection, Importance, Message,
    MessageAttachment, ObjectId, ThreadHydration, ThreadId,
};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn thread_hydrate<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        let mail = foreign_mail.object(&thread.0).clone();
        let owner = mail.owner().shared().map(str::to_owned);
        let jmap_thread_id = JmapThreadId::new(mail.wire(&thread.0).to_owned());
        let mut thread_response = mail
            .call(ThreadGet::new().ids([jmap_thread_id]).properties([
                crate::thread::Property::Id,
                crate::thread::Property::EmailIds,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::HydrateThread))?;
        let thread_object = thread_response.pop().ok_or_else(|| {
            crate::sync::error::into_account_error(
                crate::Error::IdNotFound(thread.0.clone()),
                crate::sync::error::JmapErrorContext::new(AccountOperation::HydrateThread)
                    .with_scope(bifrost_types::ErrorScope::Thread {
                        id: (thread.0.clone()).into(),
                    }),
            )
        })?;
        let order = thread_object
            .email_ids()
            .ok_or_else(|| {
                to_acct_err(AccountOperation::HydrateThread)(crate::Error::MissingResponseField(
                    "Thread/get.emailIds",
                ))
            })?
            .to_vec();
        if order.is_empty() {
            return Ok(ThreadHydration {
                id: thread,
                messages: Vec::new(),
            });
        }
        let response = mail
            .call(
                EmailGet::new()
                    .ids(order.clone())
                    .properties(message_properties(HydrationProjection::Full))
                    .fetch_text_body_values(true)
                    .fetch_html_body_values(true)
                    .body_properties(body_properties()),
            )
            .await
            .map_err(to_acct_err(AccountOperation::HydrateThread))?;
        let mut by_id = response
            .into_list()
            .into_iter()
            .filter_map(|email| email.id().map(ToString::to_string).map(|id| (id, email)))
            .collect::<HashMap<_, _>>();
        let mut messages = Vec::new();
        for id in order {
            if let Some(email) = by_id.remove(id.as_str()) {
                let mut message = email_to_message(email, HydrationProjection::Full);
                // The member ids came off the wire in the foreign
                // account's own namespace. Handed back bare they would
                // route every follow-on request (blob download, container
                // join, a per-message mutation) through the PRIMARY
                // account, so re-qualify them into the same namespace the
                // foreign inventory and `message_hydrate` mint.
                if let Some(account) = owner.as_deref() {
                    qualify_foreign_message_ids(
                        &mut message.id,
                        message.thread_id.as_mut(),
                        &mut message.containers,
                        &mut message.attachments,
                        account,
                    );
                }
                messages.push(message);
            }
        }
        // The hydration's own id is the id the CALLER submitted, verbatim,
        // so a round-trip through this door is byte-stable.
        Ok(ThreadHydration {
            id: thread,
            messages,
        })
    })
}

/// Re-qualify a foreign account's `Message` ids with their owning account.
///
/// `email_to_message` reads the bare native ids off the wire, but every id a
/// consumer hands back to this crate (`open_blob`, `get_stream`, a container
/// join against `containers_list`) has to be self-routing. Without this the
/// hydrated message's own id, its container ids, and its attachment blob ids
/// all come back in the PRIMARY namespace, so the follow-up blob download
/// 404s against the primary account and the container ids join nothing.
///
/// The `thread_id` is qualified for the same reason, and it is the
/// sharpest one: the consumer hands it back as `thread_hydrate(thread)`
/// and as `MutationTarget::Thread`, each of which expands it through an
/// accountId-scoped `Thread/get`. A bare foreign thread id asserts primary
/// ownership, so on an id collision the write lands on an unrelated
/// primary thread's messages.
///
/// Pure over the id slices so the qualification is unit-pinnable
/// without constructing a whole `Message`.
pub(super) fn qualify_foreign_message_ids(
    id: &mut ObjectId,
    thread_id: Option<&mut ThreadId>,
    containers: &mut [ContainerId],
    attachments: &mut [MessageAttachment],
    account: &str,
) {
    id.0 = crate::sync::foreign::encode_object(account, &id.0);
    if let Some(thread) = thread_id {
        thread.0 = crate::sync::foreign::encode_object(account, &thread.0);
    }
    for container in containers {
        // A container id is a MAILBOX id, so it carries the same namespace
        // `containers_list` mints for a foreign mailbox - not the object
        // namespace - and the two must not be confused.
        *container = ContainerId(crate::sync::foreign::encode_foreign(account, &container.0).0);
    }
    for attachment in attachments {
        if let AttachmentSource::Blob(blob) = &mut attachment.source {
            blob.id = BlobId(crate::sync::foreign::encode_object(account, &blob.id.0));
        }
    }
}

pub(crate) fn message_hydrate<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        // `Email/get` is accountId-scoped. A foreign-qualified id therefore
        // has to run against its OWNING account's handle: on the primary
        // handle it either 404s (reporting the encoded id verbatim, which is
        // how this surfaced downstream) or resolves an unrelated primary
        // object that happens to share the native id. `get_stream` already
        // routed per id; this one-id door did not, and it is the door the
        // engine's `message_hydrate` passthrough funnels into.
        let mail = foreign_mail.object(&message.0).clone();
        let owner = mail.owner().shared().map(str::to_owned);
        let native = mail.wire(&message.0).to_owned();
        let mut get = EmailGet::new()
            .ids([EmailId::new(native)])
            .properties(message_properties(projection));
        if matches!(
            projection,
            HydrationProjection::Preview(_)
                | HydrationProjection::Full
                | HydrationProjection::FullWithBlobs
        ) {
            get = get
                .fetch_text_body_values(true)
                .fetch_html_body_values(true)
                .body_properties(body_properties());
        }
        if let HydrationProjection::Preview(max) = projection {
            get = get.max_body_value_bytes(max);
        }
        let mut response = mail
            .call(get)
            .await
            .map_err(to_acct_err(AccountOperation::HydrateMessage))?;
        let email = response.pop().ok_or_else(|| {
            crate::sync::error::into_account_error(
                crate::Error::IdNotFound(message.0.clone()),
                crate::sync::error::JmapErrorContext::new(AccountOperation::HydrateMessage)
                    .with_scope(bifrost_types::ErrorScope::Message {
                        id: (message.0.clone()).into(),
                    }),
            )
        })?;
        let mut hydrated = email_to_message(email, projection);
        if let Some(account) = owner.as_deref() {
            qualify_foreign_message_ids(
                &mut hydrated.id,
                hydrated.thread_id.as_mut(),
                &mut hydrated.containers,
                &mut hydrated.attachments,
                account,
            );
        }
        Ok(hydrated)
    })
}

fn message_properties(projection: HydrationProjection) -> Vec<EmailProperty> {
    let mut properties = vec![
        EmailProperty::Id,
        EmailProperty::ThreadId,
        EmailProperty::MailboxIds,
        EmailProperty::Keywords,
        EmailProperty::From,
        EmailProperty::To,
        EmailProperty::Cc,
        EmailProperty::Bcc,
        EmailProperty::ReplyTo,
        EmailProperty::Subject,
        EmailProperty::SentAt,
        EmailProperty::ReceivedAt,
        EmailProperty::Size,
        EmailProperty::InReplyTo,
        EmailProperty::References,
        EmailProperty::BlobId,
    ];
    if matches!(
        projection,
        HydrationProjection::Preview(_)
            | HydrationProjection::Full
            | HydrationProjection::FullWithBlobs
    ) {
        properties.extend([
            EmailProperty::Preview,
            EmailProperty::TextBody,
            EmailProperty::HtmlBody,
            EmailProperty::BodyValues,
            EmailProperty::Attachments,
        ]);
    }
    properties
}

pub(super) fn body_properties() -> Vec<BodyProperty> {
    vec![
        BodyProperty::PartId,
        BodyProperty::BlobId,
        BodyProperty::Size,
        BodyProperty::Name,
        BodyProperty::Type,
        BodyProperty::Cid,
        BodyProperty::Disposition,
    ]
}

fn email_to_message(email: Email, projection: HydrationProjection) -> Message {
    let id = ObjectId(email.id().map(ToString::to_string).unwrap_or_default());
    let thread_id = email.thread_id().map(|id| ThreadId(id.to_string()));
    let containers = email
        .mailbox_ids()
        .into_iter()
        .map(|id| ContainerId(id.to_string()))
        .collect();
    let flags = email
        .keywords()
        .into_iter()
        .map(str::to_string)
        .collect::<bifrost_types::FlagSet>();
    let body_text = if matches!(
        projection,
        HydrationProjection::Preview(_)
            | HydrationProjection::Full
            | HydrationProjection::FullWithBlobs
    ) {
        collect_body_values(email.text_body(), &email)
            .or_else(|| email.preview().map(str::to_string))
    } else {
        None
    };
    let body_html = if matches!(
        projection,
        HydrationProjection::Full | HydrationProjection::FullWithBlobs
    ) {
        collect_body_values(email.html_body(), &email)
    } else {
        None
    };

    Message {
        id,
        thread_id,
        from: email
            .from()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        to: email
            .to()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        cc: email
            .cc()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        bcc: email
            .bcc()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        reply_to: email
            .reply_to()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        subject: email.subject().map(str::to_string),
        date: email
            .sent_at()
            .or_else(|| email.received_at())
            .and_then(unix_to_system_time),
        containers,
        importance: if flags.contains("$important") {
            Importance::High
        } else {
            Importance::Normal
        },
        flags,
        body_text,
        body_html,
        attachments: email
            .attachments()
            .unwrap_or_default()
            .iter()
            .filter_map(attachment_from_part)
            .collect(),
        size_bytes: u64::try_from(email.size()).ok(),
        in_reply_to: email.in_reply_to().and_then(|ids| ids.first().cloned()),
        references: email.references().map(<[_]>::to_vec).unwrap_or_default(),
        incomplete: false,
    }
}

fn collect_body_values(parts: Option<&[EmailBodyPart]>, email: &Email) -> Option<String> {
    let mut values = Vec::new();
    for part in parts.unwrap_or_default() {
        if let Some(part_id) = part.part_id()
            && let Some(value) = email.body_value(part_id)
        {
            values.push(value.value().to_string());
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values.join("\n"))
    }
}

pub(super) fn attachment_from_part(part: &EmailBodyPart) -> Option<MessageAttachment> {
    let id = part.blob_id()?.to_string();
    let size = u64::try_from(part.size()).ok();
    let content_type = part.content_type().map(ToString::to_string);
    Some(MessageAttachment {
        filename: part.name().map(str::to_string),
        content_type: content_type.clone(),
        content_id: part.content_id().map(str::to_string),
        inline: part.content_disposition() == Some("inline"),
        size,
        source: AttachmentSource::Blob(BlobHandle {
            id: BlobId(id),
            size,
            content_type,
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Raw8Bit,
            },
        }),
        truncated: false,
    })
}
