use super::*;

#[test]
fn encode_uid_fetch() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(UID FLAGS ENVELOPE)".into(),
        changed_since: None,
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A003", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A003 UID FETCH 1:* (UID FLAGS ENVELOPE)\r\n");
}

#[test]
fn encode_store_add_flags() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen, crate::types::Flag::Flagged],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A004", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A004 UID STORE 1:3 +FLAGS (\\Seen \\Flagged)\r\n"
    );
}

#[test]
fn encode_store_with_condstore() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("5").unwrap(),
        operation: crate::types::StoreOperation::Remove,
        flags: vec![crate::types::Flag::Deleted],
        unchanged_since: Some(
            crate::types::ModSeqValzer::new(12345).expect("valid protocol test value"),
        ),
    };
    encode_command_to_buf(&mut buf, "A005", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A005 UID STORE 5 (UNCHANGEDSINCE 12345) -FLAGS (\\Deleted)\r\n"
    );
}

#[test]
fn encode_expunge() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Expunge, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 EXPUNGE\r\n");
}

#[test]
fn encode_uid_move() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidMove {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("Trash").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID MOVE 1:5 \"Trash\"\r\n");
}

#[test]
fn encode_uid_copy() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidCopy {
        sequence_set: SequenceSet::new("10:20").unwrap(),
        mailbox: MailboxName::new("Archive").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID COPY 10:20 \"Archive\"\r\n");
}

#[test]
fn encode_uid_expunge() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidExpunge {
        sequence_set: SequenceSet::new("1:3").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID EXPUNGE 1:3\r\n");
}

#[test]
fn encode_store_replace_flags() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("42").unwrap(),
        operation: crate::types::StoreOperation::Replace,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID STORE 42 FLAGS (\\Seen)\r\n");
}

/// STORE +FLAGS.SILENT suppresses implicit FETCH (RFC 3501 Section 6.4.6).
#[test]
fn encode_uid_store_add_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::AddSilent,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID STORE 1:3 +FLAGS.SILENT (\\Seen)\r\n");
}

/// STORE -FLAGS.SILENT (RFC 3501 Section 6.4.6).
#[test]
fn encode_uid_store_remove_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("5").unwrap(),
        operation: crate::types::StoreOperation::RemoveSilent,
        flags: vec![crate::types::Flag::Deleted],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID STORE 5 -FLAGS.SILENT (\\Deleted)\r\n");
}

/// STORE FLAGS.SILENT (RFC 3501 Section 6.4.6).
#[test]
fn encode_store_replace_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("10").unwrap(),
        operation: crate::types::StoreOperation::ReplaceSilent,
        flags: vec![crate::types::Flag::Flagged],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STORE 10 FLAGS.SILENT (\\Flagged)\r\n");
}

/// RFC 6851 Section 3.3: The MOVE fallback emulation sequence uses
/// `UID STORE +FLAGS.SILENT (\Deleted)`  -  the `.SILENT` modifier is
/// required to suppress implicit FETCH responses (RFC 3501 Section 6.4.6).
/// This test documents the wire format that the MOVE fallback path in
/// `ImapConnection::uid_move_messages` must produce.
#[test]
fn encode_uid_store_move_fallback_uses_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::AddSilent,
        flags: vec![crate::types::Flag::Deleted],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID STORE 1:5 +FLAGS.SILENT (\\Deleted)\r\n",
        "MOVE fallback must use +FLAGS.SILENT per RFC 6851 Section 3.3"
    );
}

#[test]
fn encode_fetch() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 FETCH 1:* (FLAGS)\r\n");
}

#[test]
fn encode_store() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STORE 1:3 +FLAGS (\\Seen)\r\n");
}

#[test]
fn encode_copy() {
    let mut buf = BytesMut::new();
    let cmd = Command::Copy {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("Archive").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 COPY 1:5 \"Archive\"\r\n");
}

// ===== non-UID MOVE command (RFC 6851 Section 3) =====
// RFC 6851 Section 5: move = "MOVE" SP sequence-set SP mailbox
// Both UID MOVE and non-UID MOVE must be supported.

/// Non-UID MOVE command encoding (RFC 6851 Section 3).
#[test]
fn encode_move() {
    let mut buf = BytesMut::new();
    let cmd = Command::Move {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("Trash").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 MOVE 1:5 \"Trash\"\r\n");
}

// L14: FETCH CHANGEDSINCE modifier (RFC 7162 Section 3.1.4.1)

#[test]
fn encode_fetch_changedsince() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(crate::types::ModSeq::new(12345).expect("valid protocol test value")),
    };
    encode_command_to_buf(&mut buf, "A011", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A011 FETCH 1:* (FLAGS) (CHANGEDSINCE 12345)\r\n");
}

#[test]
fn encode_fetch_no_changedsince() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:10").unwrap(),
        items: "(UID)".into(),
        changed_since: None,
    };
    encode_command_to_buf(&mut buf, "A012", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A012 FETCH 1:10 (UID)\r\n");
}

#[test]
fn encode_uid_fetch_changedsince() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:500").unwrap(),
        items: "(FLAGS ENVELOPE)".into(),
        changed_since: Some(crate::types::ModSeq::new(67890).expect("valid protocol test value")),
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A013", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A013 UID FETCH 1:500 (FLAGS ENVELOPE) (CHANGEDSINCE 67890)\r\n"
    );
}

// RFC 7162 Section 3.2.6: UID FETCH CHANGEDSINCE VANISHED modifier

#[test]
fn encode_uid_fetch_changedsince_vanished() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(crate::types::ModSeq::new(12345).expect("valid protocol test value")),
        vanished: true,
    };
    encode_command_to_buf(&mut buf, "A014", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A014 UID FETCH 1:* (FLAGS) (CHANGEDSINCE 12345 VANISHED)\r\n"
    );
}

#[test]
fn encode_uid_fetch_vanished_without_changedsince_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: None,
        vanished: true,
    };
    let result = encode_command_to_buf(&mut buf, "A015", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "VANISHED without CHANGEDSINCE must be rejected per RFC 7162 Section 3.2.6"
    );
}

#[test]
fn encode_uid_fetch_changedsince_without_vanished_unchanged() {
    // Existing behavior: CHANGEDSINCE alone, vanished=false
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:500").unwrap(),
        items: "(FLAGS ENVELOPE)".into(),
        changed_since: Some(crate::types::ModSeq::new(67890).expect("valid protocol test value")),
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A016", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A016 UID FETCH 1:500 (FLAGS ENVELOPE) (CHANGEDSINCE 67890)\r\n"
    );
}

/// L2: \\Recent in STORE command.
///
/// RFC 3501 Section 9: the `flag` production excludes `\Recent`.
/// STORE uses `flag` (not `flag-fetch`), so `\Recent` is not permitted.
/// The encoder silently skips `\Recent` (and `\*`) in STORE and APPEND.
#[test]
fn spec_audit_l2_recent_in_store() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen, crate::types::Flag::Recent],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    // RFC 3501 Section 9: \Recent must NOT appear in STORE output.
    assert!(
        !output.contains("\\Recent"),
        "encoder must skip \\Recent in STORE per RFC 3501 Section 9, \
         but output contains it: {output}"
    );
    // \Seen should still be present.
    assert!(
        output.contains("\\Seen"),
        "\\Seen should still be present in output: {output}"
    );
}

/// L8: STORE with all flags filtered must return an error.
///
/// RFC 9051 Section 6.4.6 tightens the ABNF to require at least one flag:
/// `flag-list = "(" flag *(SP flag) ")"`  -  the content is no longer optional.
#[test]
fn audit_l8_store_all_flags_filtered() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Recent, crate::types::Flag::Wildcard],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE with all flags filtered out must return an error \
         per RFC 9051 Section 6.4.6"
    );
}

/// STORE with all flags filtered must return an error.
///
/// RFC 9051 Section 6.4.6 tightens the ABNF to require at least one flag:
/// `flag-list = "(" flag *(SP flag) ")"`  -  the content is no longer optional.
#[test]
fn store_all_flags_filtered_returns_error_recent_only() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Recent],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE with only \\Recent (filtered out) must return an error \
         per RFC 9051 Section 6.4.6"
    );
}

// --- Flag validation and filtering tests (STORE / APPEND) ---
// These lock down the behavior of the shared flag-filter + validate logic
// in encode_store_flags and encode_append before refactoring.

/// RFC 3501 Section 9: `\*` (Wildcard) is not valid in the `flag`
/// production used by STORE; it must be silently filtered.
#[test]
fn store_filters_wildcard_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::Seen,
            crate::types::Flag::Wildcard,
            crate::types::Flag::Flagged,
        ],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 1 +FLAGS (\\Seen \\Flagged)\r\n",
        "Wildcard must be filtered from STORE flags"
    );
}

/// RFC 3501 Section 9: `\Recent` is not valid in the `flag` production
/// used by STORE; it must be silently filtered.
#[test]
fn store_filters_recent_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("2").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::Seen,
            crate::types::Flag::Recent,
            crate::types::Flag::Flagged,
        ],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 2 +FLAGS (\\Seen \\Flagged)\r\n",
        "Recent must be filtered from STORE flags"
    );
}

/// STORE accepts valid custom flag keywords (RFC 3501 Section 9: `flag-keyword`).
#[test]
fn store_accepts_valid_custom_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::Seen,
            crate::types::Flag::from_imap_str("$Important"),
        ],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STORE 1 +FLAGS (\\Seen $Important)\r\n");
}

/// STORE rejects custom flags containing characters outside ATOM-CHAR
/// (RFC 3501 Section 9).
#[test]
fn store_rejects_invalid_custom_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::from_imap_str("bad flag")],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "custom flag with space must be rejected"
    );
}

/// STORE with only filtered-out flags (e.g., `\Recent`) must return an error
/// rather than emitting an empty flag-list `FLAGS ()`.
///
/// RFC 9051 Section 6.4.6 tightens the ABNF to require at least one flag:
/// `flag-list = "(" flag *(SP flag) ")"`  -  the content is no longer optional.
/// Even under RFC 3501, sending an empty flag-list is semantically useless
/// and likely to confuse servers.
#[test]
fn store_all_flags_filtered_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Recent],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE with all flags filtered out must return an error \
         per RFC 9051 Section 6.4.6 (flag-list requires at least one flag)"
    );
}

/// Regression IMAP-003: STORE Replace with empty flags must produce `FLAGS ()`.
///
/// RFC 3501 Section 9 / RFC 9051 Section 9: `flag-list = "(" [flag *(SP flag)] ")"`.
/// `FLAGS ()` is a legitimate command to clear all flags on a message. The encoder
/// must allow empty flag lists for `Replace` and `ReplaceSilent` operations.
#[test]
fn imap_003_store_replace_empty_flags_produces_flags_empty() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Replace,
        flags: vec![],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 1:5 FLAGS ()\r\n",
        "STORE Replace with empty flags must produce FLAGS () to clear all flags"
    );
}

/// Regression IMAP-003: STORE `ReplaceSilent` with empty flags must produce `FLAGS.SILENT ()`.
#[test]
fn imap_003_store_replace_silent_empty_flags_produces_flags_silent_empty() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("42").unwrap(),
        operation: crate::types::StoreOperation::ReplaceSilent,
        flags: vec![],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID STORE 42 FLAGS.SILENT ()\r\n",
        "STORE ReplaceSilent with empty flags must produce FLAGS.SILENT () to clear all flags"
    );
}

/// Regression IMAP-003: STORE Add with empty flags must still return an error.
///
/// Adding zero flags is semantically useless and likely indicates a caller bug.
#[test]
fn imap_003_store_add_empty_flags_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE +FLAGS with empty flags must return an error"
    );
}

/// Regression IMAP-003: STORE Remove with empty flags must still return an error.
#[test]
fn imap_003_store_remove_empty_flags_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Remove,
        flags: vec![],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE -FLAGS with empty flags must return an error"
    );
}

// --- CHANGEDSINCE modifier boundary tests ---

/// RFC 7162 Section 7: minimum valid mod-sequence-value is 1.
#[test]
fn encode_fetch_changedsince_minimum_valid() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(crate::types::ModSeq::new(1).expect("valid protocol test value")),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 FETCH 1:* (FLAGS) (CHANGEDSINCE 1)\r\n");
}

/// RFC 7162 Section 7: minimum valid mod-sequence-value is 1 (UID FETCH variant).
#[test]
fn encode_uid_fetch_changedsince_minimum_valid() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(crate::types::ModSeq::new(1).expect("valid protocol test value")),
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID FETCH 1:* (FLAGS) (CHANGEDSINCE 1)\r\n");
}

// -----------------------------------------------------------------------
// RFC 7162 Section 7: mod-sequence-value / mod-sequence-valzer validation
// -----------------------------------------------------------------------

/// RFC 7162 Section 7: CHANGEDSINCE uses mod-sequence-value (>= 1).
#[test]
fn encode_fetch_changedsince_rejects_zero() {
    assert!(crate::types::ModSeq::new(0).is_none());
}

/// RFC 7162 Section 7: mod-sequence-value must be <= 2^63-1.
#[test]
fn encode_fetch_changedsince_rejects_overflow() {
    assert!(crate::types::ModSeq::new(u64::MAX).is_none());
}

/// RFC 7162 Section 7: UNCHANGEDSINCE uses mod-sequence-valzer (allows 0).
#[test]
fn encode_store_unchangedsince_allows_zero() {
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: Some(crate::types::ModSeqValzer::new(0).unwrap()),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "UNCHANGEDSINCE 0 must be allowed per RFC 7162 Section 7"
    );
}

/// RFC 7162 Section 7: mod-sequence-valzer must be <= 2^63-1.
#[test]
fn encode_store_unchangedsince_rejects_overflow() {
    assert!(crate::types::ModSeqValzer::new(u64::MAX).is_none());
}

/// RFC 7162 Section 7: UID FETCH CHANGEDSINCE also uses mod-sequence-value.
#[test]
fn encode_uid_fetch_changedsince_rejects_zero() {
    assert!(crate::types::ModSeq::new(0).is_none());
}

/// RFC 7162 Section 7: UID STORE UNCHANGEDSINCE must reject > `i64::MAX`.
#[test]
fn encode_uid_store_unchangedsince_rejects_overflow() {
    assert!(crate::types::ModSeqValzer::new(u64::MAX).is_none());
}

// ---------------------------------------------------------------
// Pre-refactor coverage: STORE with CONDSTORE (typical value)
// RFC 7162 Section 3.1.3: UNCHANGEDSINCE mod-sequence-valzer
// Existing tests cover edge cases (0, u64::MAX); this covers a
// representative mid-range value.
// ---------------------------------------------------------------

/// RFC 7162 Section 3.1.3: non-UID STORE with CONDSTORE modifier at a
/// typical mod-sequence-valzer value (12345). The existing test covers
/// UID STORE; this covers the non-UID variant.
#[test]
fn encode_non_uid_store_with_condstore_typical_value() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::Remove,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: Some(
            crate::types::ModSeqValzer::new(12345).expect("valid protocol test value"),
        ),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 1:3 (UNCHANGEDSINCE 12345) -FLAGS (\\Seen)\r\n"
    );
}

/// FETCH items containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_fetch_items_rejected() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS\r\nA002 DELETE INBOX".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "FETCH items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// UID FETCH items containing CRLF must be rejected.
#[test]
fn crlf_injection_uid_fetch_items_rejected() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS\r\nA002 DELETE INBOX".into(),
        changed_since: None,
        vanished: false,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "UID FETCH items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// Normal FETCH items without CRLF must succeed.
#[test]
fn crlf_injection_normal_fetch_succeeds() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS ENVELOPE)".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal FETCH items must succeed: {result:?}"
    );
}

/// RFC 3501 Section 6.4.5: FETCH requires at least one message data item
/// or macro.
#[test]
fn fetch_rejects_empty_items() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "()".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty FETCH item list must be rejected (RFC 3501 Section 6.4.5)"
    );
}

/// RFC 3501 Section 6.4.5: UID FETCH uses the same fetch-att grammar as
/// FETCH and therefore also requires at least one item.
#[test]
fn uid_fetch_rejects_empty_items() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "   ".into(),
        changed_since: None,
        vanished: false,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty UID FETCH item list must be rejected (RFC 3501 Section 6.4.5)"
    );
}

/// RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5 require `fetch-att`
/// syntax to be structurally well-formed. Unbalanced inner delimiters in
/// BODY sections must be rejected before the command is written.
#[test]
fn fetch_rejects_unbalanced_inner_delimiters() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "BODY[HEADER.FIELDS (SUBJECT)".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "malformed inner FETCH delimiters must be rejected (RFC 3501 Section 6.4.5)"
    );
}

/// UID FETCH uses the same `fetch-att` grammar and must reject malformed
/// inner delimiters for the same reason.
#[test]
fn uid_fetch_rejects_unbalanced_inner_delimiters() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "BODY.PEEK[HEADER.FIELDS (SUBJECT)".into(),
        changed_since: None,
        vanished: false,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "malformed inner UID FETCH delimiters must be rejected \
         (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
    );
}

#[test]
fn typed_fetch_preserves_header_operands_during_admission() {
    let mut options = default_opts();
    std::sync::Arc::make_mut(&mut options.session).capabilities = vec![Capability::Imap4Rev1];
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: vec![crate::types::FetchAttr::BodySection {
            peek: true,
            section: Some("HEADER.FIELDS (MODSEQ PREVIEW)".to_owned()),
            partial: None,
        }]
        .into(),
        changed_since: None,
        vanished: false,
    };
    assert!(admit_and_encode("A001", &cmd, &options).is_ok());
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: vec![crate::types::FetchAttr::PreviewLazy].into(),
        changed_since: None,
        vanished: false,
    };
    assert!(matches!(
        admit_and_encode("A001", &cmd, &options),
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
}

/// Valid custom flag keywords must still be accepted.
#[test]
fn regression_valid_custom_flags_accepted() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::from_imap_str("$Important"),
            crate::types::Flag::from_imap_str("$Junk"),
            crate::types::Flag::from_imap_str("NonJunk"),
        ],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "valid custom flags must be accepted; got: {result:?}"
    );
}

/// empty custom flag keyword must be rejected.
#[test]
fn regression_empty_custom_flag_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::from_imap_str("")],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty custom flag must be rejected (RFC 3501 Section 9: atom = 1*ATOM-CHAR)"
    );
}

/// custom flag keywords containing parentheses must be rejected.
#[test]
fn regression_custom_flag_with_parens_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::from_imap_str("bad(flag")],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "custom flag with parenthesis must be rejected (RFC 3501 Section 9: ATOM-CHAR)"
    );
}

/// custom flag keywords containing atom-special characters must
/// be rejected by the encoder (RFC 3501 Section 9: flag-keyword = atom).
#[test]
fn regression_custom_flag_with_spaces_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::from_imap_str("has space")],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "custom flag with space must be rejected (RFC 3501 Section 9: ATOM-CHAR)"
    );
}
