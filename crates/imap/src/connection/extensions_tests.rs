#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use super::*;
use crate::connection::test_support::detached;
use crate::connection::{ImapConnection, SessionState};
use crate::error::Error;
use crate::types::notify::NotifyEventGroup;
use crate::types::{Capability, MailboxFilter, NotifyEvent, NotifySetParams};

fn group(filter: MailboxFilter, events: Vec<NotifyEvent>) -> NotifyEventGroup {
    NotifyEventGroup::new(filter, events)
}

fn params(groups: Vec<NotifyEventGroup>) -> NotifySetParams {
    NotifySetParams::new(groups, false)
}

fn message_new() -> NotifyEvent {
    NotifyEvent::MessageNew {
        fetch_attrs: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// compute_notify_flags (RFC 5465 Sections 5.1-5.9)
// ---------------------------------------------------------------------------

#[test]
fn notify_flags_empty_registration_is_all_false() {
    assert_eq!(
        compute_notify_flags(&params(Vec::new())),
        (false, false, false)
    );
}

#[test]
fn notify_flags_selected_message_events_produce_nothing_buffered() {
    // RFC 5465 Section 6.1: on the selected mailbox these arrive as
    // EXISTS/EXPUNGE/FETCH, not LIST/STATUS/METADATA.
    let p = params(vec![group(
        MailboxFilter::Selected,
        vec![message_new(), NotifyEvent::MessageExpunge],
    )]);
    assert_eq!(compute_notify_flags(&p), (false, false, false));

    let delayed = params(vec![group(
        MailboxFilter::SelectedDelayed,
        vec![NotifyEvent::FlagChange],
    )]);
    assert_eq!(compute_notify_flags(&delayed), (false, false, false));
}

#[test]
fn notify_flags_non_selected_message_events_set_list_and_status() {
    // RFC 5465 Sections 5.1-5.3 deliver these as STATUS; Section 5.9 can
    // additionally deliver LIST \NoAccess for any monitored mailbox.
    let p = params(vec![group(
        MailboxFilter::Personal,
        vec![message_new(), NotifyEvent::MessageExpunge],
    )]);
    assert_eq!(compute_notify_flags(&p), (true, true, false));
}

#[test]
fn notify_flags_mailbox_name_event_sets_list_only() {
    let p = params(vec![group(
        MailboxFilter::Personal,
        vec![NotifyEvent::MailboxName],
    )]);
    assert_eq!(compute_notify_flags(&p), (true, false, false));
}

#[test]
fn notify_flags_metadata_events_set_metadata_from_any_filter() {
    let mailbox_meta = params(vec![group(
        MailboxFilter::Personal,
        vec![NotifyEvent::MailboxMetadataChange],
    )]);
    assert_eq!(compute_notify_flags(&mailbox_meta), (true, false, true));

    // ServerMetadataChange is server-scoped, so it sets `metadata` even
    // under a selected filter (which contributes no `list`).
    let server_meta = params(vec![group(
        MailboxFilter::Selected,
        vec![NotifyEvent::ServerMetadataChange],
    )]);
    assert_eq!(compute_notify_flags(&server_meta), (false, false, true));
}

#[test]
fn notify_flags_empty_event_list_does_not_set_list() {
    // RFC 5465 Section 8: an empty event list is `NONE` for that filter.
    let p = params(vec![group(MailboxFilter::Personal, Vec::new())]);
    assert_eq!(compute_notify_flags(&p), (false, false, false));
}

#[test]
fn notify_flags_unknown_extension_event_enables_everything() {
    let p = params(vec![group(
        MailboxFilter::Subscribed,
        vec![NotifyEvent::Other("XVENDOR-THING".to_owned())],
    )]);
    assert_eq!(compute_notify_flags(&p), (true, true, true));
}

#[test]
fn notify_flags_union_across_groups() {
    let p = params(vec![
        group(MailboxFilter::Selected, vec![NotifyEvent::FlagChange]),
        group(
            MailboxFilter::Subtree(vec!["Archive".into()]),
            vec![NotifyEvent::MailboxMetadataChange],
        ),
    ]);
    assert_eq!(compute_notify_flags(&p), (true, false, true));
}

// ---------------------------------------------------------------------------
// Capability gates on the extension command surface
// ---------------------------------------------------------------------------

fn conn(caps: Vec<Capability>) -> ImapConnection {
    detached(SessionState::Authenticated, caps, &[])
}

#[tokio::test]
async fn extension_commands_reject_missing_capabilities_without_touching_the_wire() {
    let bare = conn(vec![Capability::Imap4Rev1]);
    let t = Duration::from_millis(50);

    assert!(matches!(
        bare.compress(t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.namespace(t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.id(&[("name", Some("bifrost"))], t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.get_quota("", t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.set_quota("", &[("STORAGE", 1)], t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.get_acl("INBOX", t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.my_rights("INBOX", t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.get_metadata("INBOX", &["/shared/comment"], None, None, t)
            .await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.notify_none(t).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        bare.enable(&["QRESYNC"], t).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn enable_is_authenticated_state_only() {
    // RFC 5161 Section 2: ENABLE is invalid once a mailbox is selected.
    let selected = detached(
        SessionState::Selected,
        vec![Capability::Enable, Capability::Imap4Rev1],
        &[],
    );
    assert!(matches!(
        selected
            .enable(&["QRESYNC"], Duration::from_millis(50))
            .await,
        Err(Error::InvalidState(_))
    ));
}

/// RFC 5161 Section 3.1: ENABLE is valid only "before any mailbox is
/// selected", and a CLOSE that returns the session to Authenticated does not
/// undo the selection. The handle refuses it from session history, before a
/// byte is written: the next line on the wire is the NOOP.
///
/// Removing the history check from `require_state` fails this: the session
/// is Authenticated, so the state table alone admits ENABLE, and its line
/// reaches the wire ahead of the NOOP.
#[tokio::test]
async fn enable_is_refused_after_a_mailbox_was_selected_and_closed() {
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, select_inbox, tag_of,
    };

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 ENABLE QRESYNC")).await;
    select_inbox(&conn, &mut server).await;

    let close = conn.close(Duration::from_secs(5));
    let script = async {
        let line = read_line(&mut server).await;
        assert!(line.contains("CLOSE"), "not CLOSE: {line:?}");
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("{tag} OK closed\r\n")).await;
    };
    let (closed, ()) = tokio::join!(close, script);
    closed.unwrap();
    assert_eq!(
        conn.state_rx.borrow().session_state,
        SessionState::Authenticated
    );

    let err = conn
        .enable(&["QRESYNC"], Duration::from_secs(5))
        .await
        .expect_err("ENABLE after a selection is illegal");
    assert!(matches!(err, Error::InvalidState(_)), "got {err:?}");

    let noop = conn.noop(Duration::from_secs(5));
    let script = async {
        let line = read_line(&mut server).await;
        assert!(
            line.contains("NOOP"),
            "the refused ENABLE reached the wire: {line:?}"
        );
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
    };
    let (nooped, ()) = tokio::join!(noop, script);
    nooped.unwrap();
}

#[tokio::test]
async fn compress_is_rejected_before_authentication() {
    let not_auth = detached(
        SessionState::NotAuthenticated,
        vec![Capability::CompressDeflate],
        &[],
    );
    assert!(matches!(
        not_auth.compress(Duration::from_millis(50)).await,
        Err(Error::InvalidState(_))
    ));
}
