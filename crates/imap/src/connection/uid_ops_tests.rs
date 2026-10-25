#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use crate::connection::test_support::detached;
use crate::connection::{ImapConnection, SessionState, filter_store_flags};
use crate::error::Error;
use crate::types::{Capability, FetchAttr, Flag, SequenceSet, StoreOperation};

const T: Duration = Duration::from_millis(50);

fn selected(caps: Vec<Capability>) -> ImapConnection {
    detached(SessionState::Selected, caps, &[])
}

fn set(spec: &str) -> SequenceSet {
    SequenceSet::new(spec).unwrap()
}

// ---------------------------------------------------------------------------
// filter_store_flags (RFC 3501 Section 2.3.2)
// ---------------------------------------------------------------------------

#[test]
fn store_flags_drop_recent_and_wildcard() {
    let filtered = filter_store_flags(&[
        Flag::Seen,
        Flag::Recent,
        Flag::Wildcard,
        Flag::Custom("$Important".to_owned()),
    ]);
    assert_eq!(
        filtered,
        vec![Flag::Seen, Flag::Custom("$Important".to_owned())]
    );
}

#[test]
fn store_flags_pass_through_when_nothing_is_filtered() {
    let flags = vec![Flag::Deleted, Flag::Draft];
    assert_eq!(filter_store_flags(&flags), flags);
    assert!(filter_store_flags(&[]).is_empty());
}

// ---------------------------------------------------------------------------
// Capability and state gates on the message command surface.
//
// A detached connection has a closed driver channel, so anything that
// clears every gate surfaces as `DriverGone` - which is exactly how these
// tests tell "rejected locally" from "would have been sent".
// ---------------------------------------------------------------------------

#[tokio::test]
async fn uid_expunge_requires_uidplus_or_rev2() {
    let without = selected(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        without.uid_expunge(&set("1:5"), T).await,
        Err(Error::MissingCapability(_))
    ));
    // RFC 9051 Appendix E item 3: rev2 folds UIDPLUS into the base set.
    let rev2 = selected(vec![Capability::Imap4Rev2]);
    assert!(matches!(
        rev2.uid_expunge(&set("1:5"), T).await,
        Err(Error::DriverGone { .. })
    ));
}

#[tokio::test]
async fn uid_move_without_move_or_uidplus_is_refused() {
    // Falling back to a plain EXPUNGE would delete unrelated \Deleted
    // messages, so the fallback is refused outright rather than guessed.
    let conn = selected(vec![Capability::Imap4Rev1]);
    match conn.uid_move_messages(&set("1:5"), "Archive", T).await {
        Err(Error::MissingCapability(msg)) => {
            assert!(msg.contains("MOVE"), "unexpected message: {msg}");
            assert!(msg.contains("UIDPLUS"), "unexpected message: {msg}");
        }
        other => panic!("expected MissingCapability, got {other:?}"),
    }
}

#[tokio::test]
async fn move_by_sequence_requires_move_or_rev2() {
    // Unlike UID MOVE there is no COPY+EXPUNGE fallback here.
    let conn = selected(vec![Capability::Imap4Rev1, Capability::UidPlus]);
    assert!(matches!(
        conn.move_messages(&set("1:5"), "Archive", T).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn uid_fetch_vanished_requires_qresync_enabled() {
    // RFC 7162 Section 3.2.6: advertised is not enough, it must be ENABLEd.
    let conn = selected(vec![Capability::QResync]);
    assert!(matches!(
        conn.uid_fetch_vanished(&set("1:5"), &[FetchAttr::Uid], 10, T)
            .await,
        Err(Error::MissingCapability(_))
    ));
    let enabled = detached(
        SessionState::Selected,
        vec![Capability::QResync],
        &["QRESYNC"],
    );
    assert!(matches!(
        enabled
            .uid_fetch_vanished(&set("1:5"), &[FetchAttr::Uid], 10, T)
            .await,
        Err(Error::DriverGone { .. })
    ));
    // The ENABLED echo is kept as the server spelled it, and RFC 9051
    // Section 9 makes the extension name an atom. A server echoing
    // `* ENABLED Qresync` has enabled QRESYNC: matching it byte-exactly here
    // would refuse every VANISHED-bearing command for the whole session,
    // while `ServerProfile::enabled` went on reporting it as enabled.
    let odd_case = detached(
        SessionState::Selected,
        vec![Capability::QResync],
        &["Qresync"],
    );
    assert!(matches!(
        odd_case
            .uid_fetch_vanished(&set("1:5"), &[FetchAttr::Uid], 10, T)
            .await,
        Err(Error::DriverGone { .. })
    ));
}

#[tokio::test]
async fn saved_search_reference_requires_searchres() {
    // RFC 5182 Section 2: `$` in a sequence set needs SEARCHRES.
    let conn = selected(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        conn.uid_fetch(&set("$"), &[FetchAttr::Uid], T).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.uid_copy(&set("$"), "Archive", T).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.uid_store(&set("$"), StoreOperation::Add, &[Flag::Seen], None, T)
            .await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.uid_expunge(&set("$"), T).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn changed_since_requires_condstore() {
    let conn = selected(vec![Capability::Imap4Rev2]);
    assert!(matches!(
        conn.uid_fetch_changed_since(&set("1:5"), &[FetchAttr::Uid], 5, T)
            .await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.uid_store(&set("1:5"), StoreOperation::Add, &[Flag::Seen], Some(5), T)
            .await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.fetch_changed_since(&set("1:5"), &[FetchAttr::Uid], 5, T)
            .await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn message_commands_require_a_selected_mailbox() {
    let conn = detached(
        SessionState::Authenticated,
        vec![Capability::Imap4Rev2],
        &[],
    );
    assert!(matches!(
        conn.uid_fetch(&set("1:5"), &[FetchAttr::Uid], T).await,
        Err(Error::InvalidState(_))
    ));
    assert!(matches!(conn.expunge(T).await, Err(Error::InvalidState(_))));
    assert!(matches!(
        conn.uid_search("ALL", T).await,
        Err(Error::InvalidState(_))
    ));
    assert!(matches!(
        conn.uid_copy(&set("1:5"), "Archive", T).await,
        Err(Error::InvalidState(_))
    ));
}

#[tokio::test]
async fn esearch_requires_the_capability_or_rev2() {
    let rev1 = selected(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        rev1.uid_search_esearch("ALL", &["COUNT"], T).await,
        Err(Error::MissingCapability(_))
    ));
    let rev2 = selected(vec![Capability::Imap4Rev2]);
    assert!(matches!(
        rev2.uid_search_esearch("ALL", &["COUNT"], T).await,
        Err(Error::DriverGone { .. })
    ));
}

#[tokio::test]
async fn esearch_save_return_option_requires_searchres() {
    // SEARCHRES rides on the return options, not the criteria.
    let conn = selected(vec![Capability::Imap4Rev1, Capability::Esearch]);
    assert!(matches!(
        conn.uid_search_esearch("ALL", &["SAVE"], T).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn search_save_requires_searchres() {
    let conn = selected(vec![Capability::Imap4Rev1, Capability::Esearch]);
    assert!(matches!(
        conn.search_save("ALL", T).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.uid_search_save("ALL", T).await,
        Err(Error::MissingCapability(_))
    ));
}

// ---------------------------------------------------------------------------
// SORT / THREAD gates (RFC 5256)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sort_requires_the_sort_capability() {
    // SORT is not part of the IMAP4rev2 baseline.
    let conn = selected(vec![Capability::Imap4Rev2]);
    assert!(matches!(
        conn.sort("DATE", "UTF-8", "ALL", T).await,
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn.uid_sort("DATE", "UTF-8", "ALL", T).await,
        Err(Error::MissingCapability(_))
    ));
}

#[tokio::test]
async fn thread_requires_the_matching_algorithm_capability() {
    let conn = selected(vec![
        Capability::Imap4Rev2,
        Capability::Thread("REFERENCES".to_owned()),
    ]);
    // Algorithm names are upper-cased before the capability lookup.
    assert!(matches!(
        conn.thread("references", "UTF-8", "ALL", T).await,
        Err(Error::DriverGone { .. })
    ));
    match conn.uid_thread("ORDEREDSUBJECT", "UTF-8", "ALL", T).await {
        Err(Error::MissingCapability(msg)) => assert_eq!(msg, "THREAD=ORDEREDSUBJECT"),
        other => panic!("expected MissingCapability, got {other:?}"),
    }
}

#[test]
fn sort_and_thread_inherit_the_search_criteria_gates() {
    use crate::codec::encode::{EncodeOptions, LiteralMode, encode_command};
    use crate::types::Command;

    let opts = EncodeOptions {
        utf8_mode: false,
        literal_mode: LiteralMode::Synchronizing,
        capabilities: vec![
            Capability::Imap4Rev1,
            Capability::Sort,
            Capability::Thread("ORDEREDSUBJECT".to_owned()),
        ],
        enabled: Vec::new(),
    };
    // The criteria gate runs at encoding, where queued commands see the
    // capability snapshot that is current when they reach the wire.
    for command in [
        Command::Sort {
            sort_criteria: "DATE".to_owned(),
            charset: "UTF-8".to_owned(),
            criteria: "MODSEQ 5".to_owned(),
        },
        Command::Thread {
            algorithm: "ORDEREDSUBJECT".to_owned(),
            charset: "UTF-8".to_owned(),
            criteria: "MODSEQ 5".to_owned(),
        },
    ] {
        assert!(matches!(
            encode_command("A001", &command, &opts),
            Err(Error::MissingCapability(_))
        ));
    }
}

// ---------------------------------------------------------------------------
// UID MOVE fallback transcript (RFC 6851 Section 3.3)
// ---------------------------------------------------------------------------

/// Without MOVE, UID MOVE falls back to UID COPY + UID STORE + UID EXPUNGE.
/// The final UID EXPUNGE's tagged code (HIGHESTMODSEQ, as a QRESYNC server
/// sends) is the counterpart of the native MOVE's tagged code and lands in
/// `MoveResult::code`; the COPY's COPYUID is not lost, it is in `copy_uid`.
///
/// Against the old fallback `code` was the COPY's COPYUID and the
/// EXPUNGE's HIGHESTMODSEQ was discarded.
#[tokio::test]
async fn uid_move_fallback_carries_the_expunge_tagged_code_and_the_copyuid() {
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, tag_of,
    };
    use crate::types::response::{Removal, ResponseCode};
    use crate::types::{ExpungeResult, UidRange};

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"), "unexpected command: {select}");
        let tag = tag_of(&select).to_owned();
        respond(
            &mut server,
            &format!(
                "* 3 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen \\Deleted)\r\n\
                 * OK [UIDVALIDITY 7] ok\r\n{tag} OK [READ-WRITE] done\r\n"
            ),
        )
        .await;

        let copy = read_line(&mut server).await;
        assert!(copy.contains("UID COPY"), "unexpected command: {copy}");
        let tag = tag_of(&copy).to_owned();
        respond(
            &mut server,
            &format!("{tag} OK [COPYUID 7 4 20] copied\r\n"),
        )
        .await;

        let store = read_line(&mut server).await;
        assert!(store.contains("UID STORE"), "unexpected command: {store}");
        let tag = tag_of(&store).to_owned();
        respond(&mut server, &format!("{tag} OK stored\r\n")).await;

        let expunge = read_line(&mut server).await;
        assert!(
            expunge.contains("UID EXPUNGE"),
            "unexpected command: {expunge}"
        );
        let tag = tag_of(&expunge).to_owned();
        respond(
            &mut server,
            &format!("* 1 EXPUNGE\r\n{tag} OK [HIGHESTMODSEQ 99] expunged\r\n"),
        )
        .await;
        server
    });

    let timeout = Duration::from_secs(5);
    conn.select("INBOX", timeout).await.unwrap();
    let result = conn
        .uid_move_messages(&set("4"), "Archive", timeout)
        .await
        .expect("the fallback move succeeds");
    let _server = script.await.unwrap();

    assert_eq!(result.code, Some(ResponseCode::HighestModSeq(99)));
    assert_eq!(
        result.copy_uid,
        Some(ResponseCode::CopyUid {
            uid_validity: 7,
            source_uids: vec![UidRange::single(4)],
            dest_uids: vec![UidRange::single(20)],
        })
    );
    assert_eq!(
        result.expunged,
        ExpungeResult {
            removals: vec![Removal::Seq(1)],
        }
    );
}
