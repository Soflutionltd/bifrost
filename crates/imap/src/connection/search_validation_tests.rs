#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::connection::test_support::detached;
use crate::connection::{ImapConnection, SessionState};
use crate::error::Error;
use crate::types::Capability;

fn has(criteria: &str, atom: &str) -> bool {
    ImapConnection::search_criteria_contains_atom(criteria, atom)
}

fn conn(caps: Vec<Capability>) -> ImapConnection {
    detached(SessionState::Selected, caps, &[])
}

// ---------------------------------------------------------------------------
// search_criteria_contains_atom - positive detection
// ---------------------------------------------------------------------------

#[test]
fn atom_found_as_a_bare_key() {
    assert!(has("MODSEQ 123", "MODSEQ"));
    assert!(has("SEEN MODSEQ 123", "MODSEQ"));
    assert!(has("modseq 123", "MODSEQ"), "keys are case-insensitive");
}

#[test]
fn atom_found_inside_a_parenthesized_group() {
    assert!(has("(SEEN (MODSEQ 5))", "MODSEQ"));
    assert!(has("OR (SEEN) (OLDER 60)", "OLDER"));
}

#[test]
fn atom_found_through_not_and_or_operands() {
    assert!(has("NOT YOUNGER 60", "YOUNGER"));
    assert!(has("NOT NOT SAVEDSINCE 1-Jan-2026", "SAVEDSINCE"));
    assert!(has("OR SEEN EMAILID M1", "EMAILID"));
    assert!(has("OR EMAILID M1 SEEN", "EMAILID"));
}

#[test]
fn atom_found_after_a_charset_prefix() {
    // RFC 3501 Section 6.4.4: optional `CHARSET <astring>` prefix.
    assert!(has("CHARSET UTF-8 MODSEQ 7", "MODSEQ"));
    assert!(has("CHARSET \"UTF-8\" MODSEQ 7", "MODSEQ"));
}

#[test]
fn atom_found_after_multi_operand_keys() {
    assert!(has("HEADER Subject hi MODSEQ 3", "MODSEQ"));
    assert!(has("BEFORE 1-Jan-2026 THREADID T1", "THREADID"));
    assert!(has("LARGER 1000 SAVEDON 1-Jan-2026", "SAVEDON"));
}

#[test]
fn saved_search_marker_is_detected_as_a_bare_key() {
    // RFC 5182 Section 2.1: `$` as a sequence-set search key.
    assert!(has("$", "$"));
    assert!(has("SEEN $", "$"));
}

// ---------------------------------------------------------------------------
// search_criteria_contains_atom - operand skipping (the whole point)
// ---------------------------------------------------------------------------

#[test]
fn atom_not_matched_inside_a_quoted_operand() {
    assert!(!has("HEADER Subject \"MODSEQ\"", "MODSEQ"));
    assert!(!has("SUBJECT \"OLDER than dirt\"", "OLDER"));
    assert!(!has("FROM \"a\\\"MODSEQ\"", "MODSEQ"), "escaped quote");
}

#[test]
fn atom_not_matched_inside_a_literal_operand() {
    assert!(!has("BODY {6}\r\nMODSEQ", "MODSEQ"));
    assert!(!has("BODY {7+}\r\nMODSEQ!", "MODSEQ"));
}

#[test]
fn atom_not_matched_as_a_bare_operand() {
    assert!(!has("BODY MODSEQ", "MODSEQ"));
    assert!(!has("SUBJECT OLDER", "OLDER"));
    assert!(!has("KEYWORD EMAILID", "EMAILID"));
    assert!(
        !has("HEADER MODSEQ MODSEQ", "MODSEQ"),
        "both HEADER operands"
    );
}

#[test]
fn modseq_variable_operands_are_skipped() {
    // RFC 7162 Section 3.1.5: MODSEQ takes either a bare valzer, or
    // entry-name + entry-type-req + valzer. The three-operand form's
    // `entry-type-req` must be consumed as an operand, not inspected as
    // a key.
    assert!(
        !has("MODSEQ \"/flags/\\\\Draft\" all 5", "ALL"),
        "entry-type-req is an operand, not a search key"
    );
    // Both forms leave the scanner at the next real key.
    assert!(has("MODSEQ 5 OLDER 60", "OLDER"));
    assert!(has("MODSEQ \"/flags/\\\\Draft\" all 5 OLDER 60", "OLDER"));
}

#[test]
fn unknown_keys_are_treated_as_zero_operand() {
    // A sequence set or an unrecognized extension key must not swallow
    // the next token.
    assert!(has("1:100 MODSEQ 5", "MODSEQ"));
    assert!(has("UNKNOWNKEY MODSEQ 5", "MODSEQ"));
}

#[test]
fn saved_search_marker_is_detected_after_uid_key() {
    assert!(has("UID $", "$"));
    assert!(matches!(
        conn(vec![Capability::Imap4Rev1]).validate_search_criteria_capabilities("UID $"),
        Err(Error::MissingCapability(capability)) if capability == "SEARCHRES"
    ));
}

// ---------------------------------------------------------------------------
// Termination on odd input
// ---------------------------------------------------------------------------

#[test]
fn scanner_terminates_on_empty_and_whitespace() {
    assert!(!has("", "MODSEQ"));
    assert!(!has("   ", "MODSEQ"));
    assert!(!has("\r\n", "MODSEQ"));
}

#[test]
fn scanner_terminates_on_unterminated_quote_and_literal() {
    assert!(!has("SUBJECT \"unterminated", "MODSEQ"));
    assert!(!has("BODY {99}\r\nshort", "MODSEQ"));
    assert!(!has("BODY {", "MODSEQ"));
    assert!(!has("BODY {12", "MODSEQ"));
}

#[test]
fn scanner_terminates_on_unclosed_group() {
    assert!(!has("(SEEN", "MODSEQ"));
    assert!(has("(SEEN MODSEQ 5", "MODSEQ"));
}

#[test]
fn scanner_terminates_on_unmatched_closing_parentheses() {
    assert!(!has(")", "MODSEQ"));
    assert!(!has("FROM alice)", "MODSEQ"));
    assert!(has("FROM alice) MODSEQ 5", "MODSEQ"));
    assert!(!has("(UNSEEN))", "MODSEQ"));
}

#[test]
fn scanner_handles_non_ascii_operands() {
    // Byte-index slicing must land on char boundaries.
    assert!(!has("SUBJECT MODSEQ\u{e5}", "MODSEQ"));
    assert!(has("SUBJECT \u{e5}\u{e6}\u{f8} MODSEQ 5", "MODSEQ"));
}

// ---------------------------------------------------------------------------
// validate_search_criteria_capabilities - the gates the scanner feeds
// ---------------------------------------------------------------------------

#[test]
fn modseq_criterion_requires_condstore() {
    assert!(matches!(
        conn(vec![]).validate_search_criteria_capabilities("MODSEQ 5"),
        Err(Error::MissingCapability(_))
    ));
    assert!(
        conn(vec![Capability::Condstore])
            .validate_search_criteria_capabilities("MODSEQ 5")
            .is_ok()
    );
}

#[test]
fn within_criteria_require_the_within_capability() {
    // RFC 5032: OLDER / YOUNGER are WITHIN, and rev2 does not fold them in.
    for criteria in ["OLDER 60", "YOUNGER 60"] {
        assert!(matches!(
            conn(vec![Capability::Imap4Rev2]).validate_search_criteria_capabilities(criteria),
            Err(Error::MissingCapability(_))
        ));
        assert!(
            conn(vec![Capability::Within])
                .validate_search_criteria_capabilities(criteria)
                .is_ok()
        );
    }
}

/// RFC 8514 SEARCH keys need the advertised SAVEDATE token on either
/// revision: RFC 9051 folds in nothing from SAVEDATE.
#[test]
fn savedate_criteria_are_not_implied_by_rev2() {
    assert!(matches!(
        conn(vec![Capability::Imap4Rev1])
            .validate_search_criteria_capabilities("SAVEDSINCE 1-Jan-2026"),
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn(vec![Capability::Imap4Rev2])
            .validate_search_criteria_capabilities("SAVEDBEFORE 1-Jan-2026"),
        Err(Error::MissingCapability(_))
    ));
    assert!(
        conn(vec![Capability::Imap4Rev2, Capability::SaveDate])
            .validate_search_criteria_capabilities("SAVEDBEFORE 1-Jan-2026")
            .is_ok()
    );
    assert!(
        conn(vec![Capability::SaveDate])
            .validate_search_criteria_capabilities("SAVEDATESUPPORTED")
            .is_ok()
    );
}

/// RFC 8474 SEARCH keys need the advertised OBJECTID token on either
/// revision: RFC 9051 folds in nothing from OBJECTID.
#[test]
fn objectid_criteria_are_not_implied_by_rev2() {
    assert!(matches!(
        conn(vec![Capability::Imap4Rev1]).validate_search_criteria_capabilities("EMAILID M1"),
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        conn(vec![Capability::Imap4Rev2]).validate_search_criteria_capabilities("THREADID T1"),
        Err(Error::MissingCapability(_))
    ));
    assert!(
        conn(vec![Capability::Imap4Rev2, Capability::ObjectId])
            .validate_search_criteria_capabilities("THREADID T1")
            .is_ok()
    );
}

#[test]
fn saved_result_marker_requires_searchres() {
    assert!(matches!(
        conn(vec![Capability::Imap4Rev1]).validate_search_criteria_capabilities("$"),
        Err(Error::MissingCapability(_))
    ));
    assert!(
        conn(vec![Capability::SearchRes])
            .validate_search_criteria_capabilities("$")
            .is_ok()
    );
}

#[test]
fn plain_criteria_need_no_capability() {
    assert!(
        conn(vec![])
            .validate_search_criteria_capabilities("ALL")
            .is_ok()
    );
    assert!(
        conn(vec![])
            .validate_search_criteria_capabilities("UNSEEN SINCE 1-Jan-2026")
            .is_ok()
    );
    // Payloads that merely look like gated keys stay ungated.
    assert!(
        conn(vec![])
            .validate_search_criteria_capabilities("HEADER Subject \"MODSEQ\"")
            .is_ok()
    );
}

#[tokio::test]
async fn queued_search_uses_capabilities_received_by_preceding_command() {
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, select_inbox, tag_of,
    };
    use std::time::Duration;
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    select_inbox(&conn, &mut server).await;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
    let script = tokio::spawn(async move {
        let noop = read_line(&mut server).await;
        let noop_tag = tag_of(&noop).to_owned();
        ready_tx.send(()).unwrap();
        queued_rx.await.unwrap();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 WITHIN\r\n{noop_tag} OK done\r\n"),
        )
        .await;
        let search = read_line(&mut server).await;
        assert!(search.contains("UID SEARCH OLDER 1"));
        let search_tag = tag_of(&search).to_owned();
        respond(
            &mut server,
            &format!("* SEARCH 1\r\n{search_tag} OK done\r\n"),
        )
        .await;
    });
    let search = async {
        ready_rx.await.unwrap();
        queued_tx.send(()).unwrap();
        conn.uid_search("OLDER 1", Duration::from_secs(5)).await
    };
    let (noop, search) = tokio::join!(conn.noop(Duration::from_secs(5)), search);
    if search.is_err() {
        script.abort();
    }
    noop.unwrap();
    search.unwrap();
    script.await.unwrap();
}

#[tokio::test]
async fn literal_operand_uses_shared_framing_without_becoming_a_capability_key() {
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, select_inbox, tag_of,
    };
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+")).await;
    select_inbox(&conn, &mut server).await;
    let script = async {
        let command = read_line(&mut server).await;
        assert!(command.ends_with("BODY {6+}\r\n"));
        let tag = tag_of(&command).to_owned();
        let mut payload = [0; 6];
        server.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"MODSEQ");
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(&mut server, &format!("* SEARCH 1\r\n{tag} OK done\r\n")).await;
    };
    let (search, ()) = tokio::join!(
        conn.uid_search("BODY {0006+}\r\nMODSEQ", Duration::from_secs(5)),
        script,
    );
    search.unwrap();
}
