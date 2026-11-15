#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::{Capability, MetadataEntry, ResponseCode, UntaggedResponse, UntaggedStatus};

#[test]
fn bookkeeping_does_not_interrupt_idle() {
    assert!(
        typed_event_to_idle_event(TypedEvent::CapabilityChange(vec![Capability::Idle])).is_none()
    );
    for text in ["", "Still here"] {
        assert!(
            typed_event_to_idle_event(TypedEvent::from(UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: None,
                text: text.into(),
            }))
            .is_none()
        );
    }
}

#[test]
fn idle_retains_server_and_local_event_payloads() {
    let since = std::time::Instant::now();
    let events = vec![
        TypedEvent::Exists(42),
        TypedEvent::Recent(5),
        TypedEvent::Expunge(3),
        TypedEvent::Alert("urgent".into()),
        TypedEvent::Bye {
            code: Some(ResponseCode::Unavailable),
            text: "bye".into(),
        },
        TypedEvent::FetchUpdate(Box::default()),
        TypedEvent::MailboxEvent(Default::default()),
        TypedEvent::MailboxStatus {
            mailbox: Default::default(),
            items: vec![crate::types::StatusItem::Messages(42)],
        },
        TypedEvent::Vanished {
            earlier: true,
            uids: vec![crate::types::UidRange::single(7)],
        },
        TypedEvent::QueueOverflow {
            dropped_count: 7,
            since,
        },
        TypedEvent::NotificationOverflow {
            code: Some("extension".into()),
            text: "overflow".into(),
        },
        TypedEvent::from(UntaggedResponse::Metadata {
            mailbox: Default::default(),
            entries: vec![MetadataEntry::default()],
        }),
        // Legacy SEARCH must retain MODSEQ and its original response type.
        TypedEvent::from(UntaggedResponse::Search {
            numbers: vec![7],
            mod_seq: Some(crate::types::ModSeq::new(99).expect("valid protocol test value")),
        }),
        TypedEvent::from(UntaggedResponse::Esearch(Default::default())),
        TypedEvent::from(UntaggedResponse::Status {
            status: UntaggedStatus::No,
            code: None,
            text: "warning".into(),
        }),
        TypedEvent::from(UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code: Some(ResponseCode::UidValidity(123)),
            text: "valid".into(),
        }),
    ];
    for event in events {
        assert_eq!(
            typed_event_to_idle_event(event.clone()),
            Some(IdleEvent::Event(event))
        );
    }
}
