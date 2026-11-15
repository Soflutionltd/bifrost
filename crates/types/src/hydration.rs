//! Thread and message hydration shapes.
//!
//! `thread_hydrate` and `message_hydrate` are the typed read-side
//! primitives consumers call when they already know which messages
//! they want and want the parsed shape rather than raw MIME bytes.
//! Distinct from `get_stream` (which is the streaming, projection-
//! driven engine read path) - the hydration primitives are
//! one-shot, request-response, and shaped for a UI consumer.

use std::time::SystemTime;

use bytes::Bytes;

use crate::blob::BlobHandle;
use crate::compose::Address;
use crate::container::ContainerId;
use crate::ids::{ObjectId, ThreadId};

/// Message importance, the uniform representation of the
/// single-valued/exclusive priority bit that Graph (`importance`),
/// JMAP (`$important`-adjacent keywords), and IMAP (no native field)
/// each express differently. Exclusive by construction: a message has
/// exactly one importance at a time, which is the whole point - it is
/// why a consumer must never expand one importance change into two
/// intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Importance {
    Low,
    Normal,
    High,
}

/// Per-message projection selector for `message_hydrate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HydrationProjection {
    /// Headers + memberships only. Cheapest projection.
    Headers,
    /// Headers + N bytes of preview text.
    Preview(usize),
    /// All parts decoded, no attachment bytes.
    Full,
    /// `Full` plus inline-attachment bytes.
    FullWithBlobs,
}

/// Where a hydrated attachment's content is available.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AttachmentSource {
    /// A provider-owned object that can be fetched through `Account::open_blob`.
    Blob(BlobHandle),
    /// Decoded bytes already carried in the hydration response.
    Inline(Bytes),
    /// Metadata only; this provider cannot offer an openable blob.
    None,
}

/// One user-facing inbound message attachment.
#[derive(Debug, Clone)]
pub struct MessageAttachment {
    pub filename: Option<String>,
    // Preserve the declared wire media type. parse_content_type is a tolerant
    // inbound parser: malformed input becomes text/plain, which would silently
    // change an outbound declaration or hide inbound evidence.
    pub content_type: Option<String>,
    pub content_id: Option<String>,
    pub inline: bool,
    pub size: Option<u64>,
    pub source: AttachmentSource,
    pub truncated: bool,
}

/// One hydrated message returned by `message_hydrate` or sitting
/// inside a `ThreadHydration`.
///
/// Distinct from `mutation::HydratedObject` (which is the engine-side
/// read-back shape carrying a `Projection` discriminant). `Message`
/// is the user-facing parsed shape; `HydratedObject` is the
/// engine-internal binary one.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Message` directly.
#[derive(Debug, Clone)]
pub struct Message {
    pub id: ObjectId,
    pub thread_id: Option<ThreadId>,
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub reply_to: Vec<Address>,
    pub subject: Option<String>,
    pub date: Option<SystemTime>,
    /// Containers (folders, labels, mailboxes) the message is in.
    pub containers: Vec<ContainerId>,
    /// Keywords / flags currently set on the message.
    pub flags: crate::FlagSet,
    /// Message importance. The uniform, exclusive importance bit:
    /// Graph maps its `low|normal|high` wire field; JMAP/IMAP map the
    /// `$important` keyword (present -> `High`, absent -> `Normal`);
    /// Gmail has no importance field and is always `Normal`.
    pub importance: Importance,
    /// Plain-text body. `None` when not requested by the projection
    /// or when the message has no text/plain part.
    pub body_text: Option<String>,
    /// HTML body. `None` when not requested or when the message has
    /// no text/html part.
    pub body_html: Option<String>,
    /// Decoded attachment metadata and, where available, content source.
    pub attachments: Vec<MessageAttachment>,
    /// Total size in bytes when the protocol exposes it.
    pub size_bytes: Option<u64>,
    /// In-Reply-To header value.
    pub in_reply_to: Option<String>,
    /// References header values.
    pub references: Vec<String>,
    /// Body or attachment content is known to be incomplete.
    pub incomplete: bool,
}

/// Full thread hydration result. Lists every message in the thread
/// in delivery order plus the thread-level identifier the consumer
/// can pass back to `move_thread` / `delete_thread`.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `ThreadHydration` directly.
#[derive(Debug, Clone)]
pub struct ThreadHydration {
    pub id: ThreadId,
    pub messages: Vec<Message>,
}
