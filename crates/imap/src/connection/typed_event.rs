//! Typed event enum for asynchronous server notifications.
//!
//! `TypedEvent` models every piece of asynchronous server data that
//! the driver task forwards to the caller. `Priority` classifies
//! events for back-pressure decisions in
//! [`DriverEventSink`](super::driver::event_sink::DriverEventSink).
//!
//! RFC 3501 Section 7 defines the untagged responses that can arrive
//! asynchronously. RFC 5465 adds NOTIFY-specific events.

use crate::types::fetch::FetchResponse;
use crate::types::mailbox::{MailboxInfo, StatusItem};
use crate::types::response::{Capability, ResponseCode, UidRange, UntaggedResponse};
use crate::types::validated::MailboxName;

/// Asynchronously delivered server data.
///
/// Each variant models a distinct class of server notification. The
/// driver task converts raw [`UntaggedResponse`]s into `TypedEvent`s
/// and publishes them via
/// [`DriverEventSink`](super::driver::event_sink::DriverEventSink).
#[derive(Debug, Clone)]
pub enum TypedEvent {
    /// RFC 3501 Section7.1 \[ALERT\]  -  informational and high priority;
    /// loss under backpressure is reported by `QueueOverflow`.
    Alert(String),
    /// RFC 3501 Section7.1.5 BYE  -  connection ending.
    Bye {
        code: Option<ResponseCode>,
        text: String,
    },
    /// RFC 5465 Section5.8 NOTIFICATIONOVERFLOW  -  NOTIFY registration cleared.
    NotificationOverflow { code: Option<String>, text: String },
    /// Internal overflow marker. Emitted when the event queue drops events.
    QueueOverflow {
        dropped_count: usize,
        since: std::time::Instant,
    },

    /// RFC 3501 Section7.3.1 EXISTS  -  mailbox size change.
    Exists(u32),
    /// RFC 3501 Section7.3.2 RECENT.
    Recent(u32),
    /// RFC 3501 Section7.4.1 EXPUNGE  -  message removed (sequence form).
    Expunge(u32),
    /// RFC 7162 VANISHED  -  message removed (UID form).
    Vanished { earlier: bool, uids: Vec<UidRange> },
    /// RFC 3501 Section7.4.2 FETCH  -  flags or other update.
    FetchUpdate(Box<FetchResponse>),
    /// RFC 5465 Section5 `MailboxName` event.
    MailboxEvent(MailboxInfo),
    /// RFC 5465 Section4 STATUS indicator event for a non-selected mailbox.
    MailboxStatus {
        /// The mailbox whose STATUS data changed.
        mailbox: MailboxName,
        /// STATUS items delivered by the server.
        items: Vec<StatusItem>,
    },
    /// RFC 5465 Section5 `MailboxMetadataChange`.
    MetadataChange {},
    /// RFC 5465 Section5 `ServerMetadataChange`.
    ServerMetadataChange {},
    /// RFC 3501 Section7.2.1 CAPABILITY change.
    CapabilityChange(Vec<Capability>),
    /// Extension or future event.
    Extension(Box<UntaggedResponse>),
}

/// Event priority classification for back-pressure handling.
///
/// The [`DriverEventSink`](super::driver::event_sink::DriverEventSink)
/// uses this to decide which events to buffer vs. drop under queue
/// pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::connection) enum Priority {
    /// Buffered before lower priorities; any loss under backpressure is
    /// reported by `QueueOverflow`.
    Critical,
    /// Dropped first under queue pressure  -  capability changes can be
    /// re-queried with a single CAPABILITY command.
    Requeryable,
    /// Dropped next  -  mailbox state changes that require a full resync
    /// (EXISTS, EXPUNGE, FETCH updates, NOTIFY events, extensions).
    Resyncable,
}

impl From<UntaggedResponse> for TypedEvent {
    /// Convert a raw untagged response into a typed event.
    ///
    /// Used by the driver task when routing unsolicited responses to the
    /// event sink. Handles the well-known response variants directly;
    /// everything else is wrapped in [`TypedEvent::Extension`].
    fn from(resp: UntaggedResponse) -> Self {
        match resp {
            UntaggedResponse::Status {
                status: crate::types::response::UntaggedStatus::Bye,
                code,
                text,
            } => Self::Bye { code, text },
            // NOTE: Alert and NotificationOverflow are NOT handled here.
            // They are extracted and emitted by the driver task directly
            // from the SideEffectDigest, ensuring they reach
            // the event queue regardless of classification  -  including
            // when they appear in tagged responses or in untagged
            // responses classified as solicited. The From conversion
            // only fires for the event_sink.emit(resp.into()) path,
            // which would miss tagged and solicited cases.
            UntaggedResponse::Status {
                code: Some(ResponseCode::Capability(caps)),
                ..
            }
            | UntaggedResponse::Capability(caps) => Self::CapabilityChange(caps),
            UntaggedResponse::Exists(n) => Self::Exists(n),
            UntaggedResponse::Recent(n) => Self::Recent(n),
            UntaggedResponse::Expunge(n) => Self::Expunge(n),
            UntaggedResponse::Fetch(f) => Self::FetchUpdate(f),
            UntaggedResponse::List(info) | UntaggedResponse::Lsub(info) => Self::MailboxEvent(info),
            UntaggedResponse::MailboxStatus { mailbox, items } => {
                Self::MailboxStatus { mailbox, items }
            }
            UntaggedResponse::Vanished { earlier, uids } => Self::Vanished { earlier, uids },
            // Everything else  -  plain `* OK/NO/BAD` with no special response
            // code, SEARCH, ESEARCH, ACL, QUOTA, METADATA, THREAD, SORT, etc.
            //  -  is either stateless or typically consumed by a command consumer,
            // not the event queue. Wrap as Extension for forward compatibility
            //
            other => Self::Extension(Box::new(other)),
        }
    }
}

impl TypedEvent {
    /// Returns the back-pressure priority for this event.
    pub(in crate::connection) fn priority(&self) -> Priority {
        match self {
            Self::Alert(_)
            | Self::Bye { .. }
            | Self::NotificationOverflow { .. }
            | Self::QueueOverflow { .. } => Priority::Critical,
            Self::CapabilityChange(_) => Priority::Requeryable,
            Self::Exists(_)
            | Self::Recent(_)
            | Self::Expunge(_)
            | Self::Vanished { .. }
            | Self::FetchUpdate(_)
            | Self::MailboxEvent(_)
            | Self::MailboxStatus { .. }
            | Self::MetadataChange { .. }
            | Self::ServerMetadataChange { .. }
            | Self::Extension(_) => Priority::Resyncable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mailbox_status_response_becomes_typed_notify_event() {
        let event = TypedEvent::from(UntaggedResponse::MailboxStatus {
            mailbox: MailboxName::new("Archive").unwrap(),
            items: vec![StatusItem::Messages(42)],
        });

        match event {
            TypedEvent::MailboxStatus { mailbox, items } => {
                assert_eq!(mailbox.as_str(), "Archive");
                assert_eq!(items, vec![StatusItem::Messages(42)]);
            }
            other => panic!("expected MailboxStatus event, got {other:?}"),
        }
    }

    #[test]
    fn mailbox_status_event_is_resyncable() {
        let event = TypedEvent::MailboxStatus {
            mailbox: MailboxName::new("Archive").unwrap(),
            items: vec![StatusItem::Messages(42)],
        };

        assert_eq!(event.priority(), Priority::Resyncable);
    }
}
