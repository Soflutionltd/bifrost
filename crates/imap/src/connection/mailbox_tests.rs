#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use crate::connection::test_support::detached;
use crate::connection::{ImapConnection, SessionState};
use crate::error::Error;
use crate::types::{Capability, MailboxAttribute, QresyncParams, SelectOptions};

const T: Duration = Duration::from_millis(50);

fn authed(caps: Vec<Capability>) -> ImapConnection {
    detached(SessionState::Authenticated, caps, &[])
}

fn selected(caps: Vec<Capability>, enabled: &[&str]) -> ImapConnection {
    detached(SessionState::Selected, caps, enabled)
}

// ---------------------------------------------------------------------------
// validate_qresync_params (RFC 7162 Section 3.2.5.2)
// ---------------------------------------------------------------------------

#[test]
fn qresync_params_require_enable() {
    let params = QresyncParams::from_raw(1, 2, None, None).unwrap();
    let without = authed(vec![Capability::QResync]);
    assert!(matches!(
        without.validate_qresync_params(&params),
        Err(Error::InvalidState(_))
    ));

    let with = detached(
        SessionState::Authenticated,
        vec![Capability::QResync],
        &["QRESYNC"],
    );
    assert!(with.validate_qresync_params(&params).is_ok());
}

#[test]
fn qresync_seq_match_data_requires_known_uids() {
    let conn = detached(
        SessionState::Authenticated,
        vec![Capability::QResync],
        &["QRESYNC"],
    );
    assert!(
        QresyncParams::from_raw(1, 2, None, Some(("1:10".to_owned(), "100:110".to_owned())))
            .is_err()
    );

    let good = QresyncParams::from_raw(
        1,
        2,
        Some("1:10".to_owned()),
        Some(("1:10".to_owned(), "100:110".to_owned())),
    )
    .unwrap();
    assert!(conn.validate_qresync_params(&good).is_ok());
}

// ---------------------------------------------------------------------------
// Mailbox-command gates. None of these may reach the wire, so a detached
// connection (whose driver channel is closed) is enough.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_condstore_requires_the_capability() {
    let conn = authed(vec![Capability::Imap4Rev1]);
    let opts = SelectOptions::condstore();
    assert!(matches!(
        conn.select_with("INBOX", &opts, T).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn select_rejects_not_authenticated() {
    let conn = detached(SessionState::NotAuthenticated, vec![], &[]);
    assert!(matches!(
        conn.select("INBOX", T).await,
        Err(Error::InvalidState(_))
    ));
}

/// A session already in Logout is a connection that is gone, not a caller
/// sequencing mistake: the refusal is `Closed` with `Unsent` evidence, a
/// transport condition a reconnect answers.
#[tokio::test]
async fn a_command_on_a_logged_out_session_is_closed_not_invalid_state() {
    let conn = detached(SessionState::Logout, vec![], &[]);
    let err = conn.select("INBOX", T).await.expect_err("session is over");
    assert!(matches!(err, Error::Closed { .. }), "got {err:?}");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
}

#[tokio::test]
async fn close_and_unselect_require_a_selected_mailbox() {
    let conn = authed(vec![Capability::Unselect]);
    assert!(matches!(conn.close(T).await, Err(Error::InvalidState(_))));
    assert!(matches!(
        conn.unselect(T).await,
        Err(Error::InvalidState(_))
    ));
}

#[tokio::test]
async fn unselect_requires_the_capability_or_rev2() {
    let without = selected(vec![Capability::Imap4Rev1], &[]);
    assert!(matches!(
        without.unselect(T).await,
        Err(Error::MissingCapability(_))
    ));
    // A capable connection gets past the gate and fails only on the wire.
    let with = selected(vec![Capability::Imap4Rev2], &[]);
    assert!(matches!(
        with.unselect(T).await,
        Err(Error::DriverGone { .. })
    ));
}

#[tokio::test]
async fn lsub_is_rejected_on_rev2() {
    // RFC 9051 Appendix F item 19.
    let rev2 = authed(vec![Capability::Imap4Rev2]);
    assert!(matches!(
        rev2.lsub("", "*", T).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn create_special_use_requires_the_capability_and_valid_attributes() {
    let without = authed(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        without
            .create_special_use("Archive", &[MailboxAttribute::Archive], T)
            .await,
        Err(Error::MissingCapability(_))
    ));

    // RFC 6154 Section 3: USE MUST only contain use-attr values.
    let with = authed(vec![Capability::CreateSpecialUse]);
    assert!(matches!(
        with.create_special_use("Archive", &[MailboxAttribute::NoSelect], T)
            .await,
        Err(Error::InvalidInput(_))
    ));
}

#[tokio::test]
async fn list_status_requires_both_list_status_and_list_extended_on_rev1() {
    let only_status = authed(vec![Capability::Imap4Rev1, Capability::ListStatus]);
    assert!(matches!(
        only_status.list_status("", "*", "MESSAGES", T).await,
        Err(Error::MissingCapability(_))
    ));
    let neither = authed(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        neither.list_status("", "*", "MESSAGES", T).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn status_validates_items_before_dispatch() {
    let conn = authed(vec![Capability::Imap4Rev2]);
    // RECENT was removed in IMAP4rev2.
    assert!(matches!(
        conn.status("INBOX", "RECENT", T).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.status("INBOX", "()", T).await,
        Err(Error::InvalidInput(_))
    ));
}

#[tokio::test]
async fn list_extended_with_a_single_plain_pattern_falls_back_to_list() {
    // No selection or return options and exactly one pattern means the
    // LIST-EXTENDED capability is not required: the call degrades to a
    // plain LIST and therefore reaches the (dead) driver.
    let conn = authed(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        conn.list_extended("", &["*"], &[], &[], T).await,
        Err(Error::DriverGone { .. })
    ));
}

#[tokio::test]
async fn mailbox_names_reject_crlf_injection() {
    // RFC 3501 Section 2.2: commands are CRLF-delimited, so a name
    // carrying CR/LF must be refused before it reaches the encoder.
    let conn = authed(vec![Capability::Imap4Rev2]);
    assert!(conn.select("INBOX\r\nA1 LOGOUT", T).await.is_err());
    assert!(conn.create("bad\nname", T).await.is_err());
    assert!(conn.delete("bad\rname", T).await.is_err());
    assert!(conn.rename("INBOX", "bad\nname", T).await.is_err());
    assert!(conn.subscribe("bad\nname", T).await.is_err());
    assert!(conn.status("bad\nname", "MESSAGES", T).await.is_err());
}
