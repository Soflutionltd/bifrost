#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::connection::typed_event::TypedEvent;
use crate::types::StatusItem;
use crate::types::mailbox::MailboxInfo;
use crate::types::response::{
    Capability, MetadataEntry, ResponseCode, UntaggedResponse, UntaggedStatus,
};
use crate::types::validated::MailboxName;

// ---------------------------------------------------------------------------
// typed_event_to_idle_event
// ---------------------------------------------------------------------------

#[test]
fn capability_change_empty_returns_none() {
    let ev = TypedEvent::CapabilityChange(vec![]);
    assert!(typed_event_to_idle_event(ev).is_none());
}

#[test]
fn capability_change_with_caps_returns_none() {
    let ev = TypedEvent::CapabilityChange(vec![Capability::Imap4Rev1]);
    assert!(typed_event_to_idle_event(ev).is_none());
}

#[test]
fn exists_returns_some() {
    let ev = TypedEvent::Exists(42);
    assert_eq!(typed_event_to_idle_event(ev), Some(IdleEvent::Exists(42)));
}

#[test]
fn expunge_returns_some() {
    let ev = TypedEvent::Expunge(3);
    assert_eq!(typed_event_to_idle_event(ev), Some(IdleEvent::Expunge(3)));
}

#[test]
fn recent_returns_some() {
    let ev = TypedEvent::Recent(5);
    assert_eq!(typed_event_to_idle_event(ev), Some(IdleEvent::Recent(5)));
}

#[test]
fn alert_returns_some() {
    let ev = TypedEvent::Alert("urgent".into());
    assert_eq!(
        typed_event_to_idle_event(ev),
        Some(IdleEvent::Alert("urgent".into()))
    );
}

#[test]
fn fetch_update_returns_some() {
    let ev = TypedEvent::FetchUpdate(Box::default());
    let result = typed_event_to_idle_event(ev);
    assert!(result.is_some());
    assert!(matches!(result, Some(IdleEvent::Fetch(_))));
}

#[test]
fn mailbox_event_returns_some() {
    let ev = TypedEvent::MailboxEvent(MailboxInfo::default());
    let result = typed_event_to_idle_event(ev);
    assert!(result.is_some());
    assert!(matches!(result, Some(IdleEvent::MailboxEvent(_))));
}

#[test]
fn mailbox_status_event_returns_some() {
    let ev = TypedEvent::MailboxStatus {
        mailbox: MailboxName::default(),
        items: vec![StatusItem::Messages(42)],
    };
    let result = typed_event_to_idle_event(ev);
    assert!(matches!(result, Some(IdleEvent::MailboxStatus { .. })));
}

#[test]
fn bye_returns_some() {
    let ev = TypedEvent::Bye {
        code: None,
        text: "goodbye".into(),
    };
    assert_eq!(
        typed_event_to_idle_event(ev),
        Some(IdleEvent::Bye {
            code: None,
            text: "goodbye".into(),
        })
    );
}

#[test]
fn metadata_change_returns_some_extension() {
    let ev = TypedEvent::MetadataChange {};
    let result = typed_event_to_idle_event(ev);
    assert_eq!(
        result,
        Some(IdleEvent::ExtensionEvent(
            super::super::IdleExtension::MetadataChange { server: false }
        ))
    );
}

#[test]
fn server_metadata_change_returns_some_extension() {
    let ev = TypedEvent::ServerMetadataChange {};
    let result = typed_event_to_idle_event(ev);
    assert_eq!(
        result,
        Some(IdleEvent::ExtensionEvent(
            super::super::IdleExtension::MetadataChange { server: true }
        ))
    );
}

#[test]
fn extension_ok_keepalive_returns_none() {
    // `* OK Still here` with no response code is a keepalive  -
    // should be filtered by untagged_to_idle_event and propagated
    // as None through typed_event_to_idle_event.
    let ev = TypedEvent::Extension(Box::new(UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: None,
        text: "Still here".into(),
    }));
    assert!(typed_event_to_idle_event(ev).is_none());
}

// ---------------------------------------------------------------------------
// untagged_to_idle_event
// ---------------------------------------------------------------------------

#[test]
fn untagged_ok_keepalive_returns_none() {
    let resp = UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: None,
        text: "Still here".into(),
    };
    assert!(untagged_to_idle_event(resp).is_none());
}

#[test]
fn untagged_ok_keepalive_empty_text_returns_none() {
    let resp = UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: None,
        text: String::new(),
    };
    assert!(untagged_to_idle_event(resp).is_none());
}

#[test]
fn untagged_ok_with_code_returns_status_update() {
    let resp = UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: Some(ResponseCode::UidValidity(123)),
        text: "valid".into(),
    };
    let result = untagged_to_idle_event(resp);
    assert_eq!(
        result,
        Some(IdleEvent::StatusUpdate {
            status: UntaggedStatus::Ok,
            code: ResponseCode::UidValidity(123),
            text: "valid".into(),
        })
    );
}

#[test]
fn untagged_no_without_code_not_filtered() {
    // `* NO text` is a warning  -  meaningful, not a keepalive.
    let resp = UntaggedResponse::Status {
        status: UntaggedStatus::No,
        code: None,
        text: "warning".into(),
    };
    let result = untagged_to_idle_event(resp);
    assert!(result.is_some(), "* NO should not be filtered");
}

#[test]
fn untagged_mailbox_status_returns_some() {
    let resp = UntaggedResponse::MailboxStatus {
        mailbox: MailboxName::default(),
        items: vec![StatusItem::Messages(42)],
    };
    let result = untagged_to_idle_event(resp);
    assert!(matches!(result, Some(IdleEvent::MailboxStatus { .. })));
}

#[test]
fn untagged_metadata_returns_some() {
    let resp = UntaggedResponse::Metadata {
        mailbox: MailboxName::default(),
        entries: vec![MetadataEntry::default()],
    };
    let result = untagged_to_idle_event(resp);
    assert!(matches!(result, Some(IdleEvent::MetadataChange { .. })));
}

#[test]
fn queue_loss_retains_typed_count_and_start() {
    let since = std::time::Instant::now();
    let event = typed_event_to_idle_event(TypedEvent::QueueOverflow {
        dropped_count: 7,
        since,
    });
    assert_eq!(
        event,
        Some(IdleEvent::ExtensionEvent(
            super::super::IdleExtension::QueueOverflow {
                dropped_count: 7,
                since
            },
        ))
    );
}
