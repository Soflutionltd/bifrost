use super::*;

#[test]
fn encode_options_treats_list_status_as_imap4rev2_base_capability() {
    let opts = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::ModifiedUtf7,
        literal_mode: LiteralMode::Synchronizing,
        session: crate::types::profile::SessionCapabilities::new(
            vec![Capability::Imap4Rev2],
            Vec::new(),
        ),
    };

    assert!(opts.has_capability(&Capability::ListStatus));
}

#[test]
fn encode_select() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A002", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A002 SELECT \"INBOX\"\r\n");
}

#[test]
fn encode_select_qresync() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams::from_raw(67890, 12345, None, None).unwrap()),
    };
    encode_command_to_buf(&mut buf, "A005", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A005 SELECT \"INBOX\" (QRESYNC (67890 12345))\r\n"
    );
}

#[test]
fn encode_select_qresync_with_known_uids() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams::from_raw(67890, 12345, Some("1:500".into()), None).unwrap()),
    };
    encode_command_to_buf(&mut buf, "A006", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A006 SELECT \"INBOX\" (QRESYNC (67890 12345 1:500))\r\n"
    );
}

#[test]
fn encode_examine_qresync() {
    let mut buf = BytesMut::new();
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams::from_raw(100, 50, None, None).unwrap()),
    };
    encode_command_to_buf(&mut buf, "A007", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A007 EXAMINE \"INBOX\" (QRESYNC (100 50))\r\n");
}

/// RFC 3501 Section 9: uidvalidity = nz-number, so 0 is invalid.
/// RFC 7162 Section 3.2.5.2: QRESYNC takes uidvalidity.
#[test]
fn encode_select_qresync_rejects_zero_uid_validity() {
    assert!(crate::types::QresyncParams::from_raw(0, 1, None, None).is_err());
}

/// RFC 3501 Section 9: uidvalidity = nz-number, so 0 is invalid.
/// RFC 7162 Section 3.2.5.2: QRESYNC takes uidvalidity.
#[test]
fn encode_examine_qresync_rejects_zero_uid_validity() {
    assert!(crate::types::QresyncParams::from_raw(0, 1, None, None).is_err());
}

#[test]
fn encode_examine() {
    let mut buf = BytesMut::new();
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 EXAMINE \"INBOX\"\r\n");
}

#[test]
fn encode_create() {
    let mut buf = BytesMut::new();
    let cmd = Command::Create {
        mailbox: MailboxName::new("Archive").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"Archive\"\r\n");
}

#[test]
fn encode_create_special_use_single() {
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("Sent").unwrap(),
        special_use: vec![crate::types::MailboxAttribute::Sent],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"Sent\" (USE (\\Sent))\r\n");
}

#[test]
fn encode_create_special_use_multiple() {
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("Important Sent").unwrap(),
        special_use: vec![
            crate::types::MailboxAttribute::Sent,
            crate::types::MailboxAttribute::Important,
        ],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 CREATE \"Important Sent\" (USE (\\Sent \\Important))\r\n"
    );
}

#[test]
fn encode_create_special_use_special_char_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("My Drafts").unwrap(),
        special_use: vec![crate::types::MailboxAttribute::Drafts],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"My Drafts\" (USE (\\Drafts))\r\n");
}

#[test]
fn encode_create_special_use_empty() {
    // RFC 6154 Section 6 ABNF: `"USE" SP "(" [use-attr *(SP use-attr)] ")"`
    // Brackets mean the use-attr list is optional.
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("Test").unwrap(),
        special_use: vec![],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"Test\" (USE ())\r\n");
}

#[test]
fn encode_delete() {
    let mut buf = BytesMut::new();
    let cmd = Command::Delete {
        mailbox: MailboxName::new("OldMail").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 DELETE \"OldMail\"\r\n");
}

#[test]
fn encode_rename() {
    let mut buf = BytesMut::new();
    let cmd = Command::Rename {
        mailbox: MailboxName::new("OldName").unwrap(),
        new_name: MailboxName::new("NewName").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 RENAME \"OldName\" \"NewName\"\r\n");
}

#[test]
fn encode_list() {
    let mut buf = BytesMut::new();
    let cmd = Command::List {
        reference: String::new().into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LIST \"\" \"*\"\r\n");
}

#[test]
fn encode_list_extended_multiple_patterns_with_return_options() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec!["SUBSCRIBED".into(), "RECURSIVEMATCH".into()],
        reference: String::new().into(),
        patterns: vec!["*".into(), "%".into()],
        return_options: vec!["CHILDREN".into(), "STATUS (MESSAGES UNSEEN)".into()],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST (SUBSCRIBED RECURSIVEMATCH) \"\" (\"*\" \"%\") RETURN (CHILDREN STATUS (MESSAGES UNSEEN))\r\n"
    );
}

#[test]
fn encode_list_extended_rejects_empty_pattern_list() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: Vec::new(),
        reference: String::new().into(),
        patterns: Vec::new(),
        return_options: Vec::new(),
    };

    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        matches!(result, Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(ref msg))) if msg.contains("RFC 5258 Section 3")),
        "empty LIST-EXTENDED patterns must be rejected per RFC 5258 Section 3 / RFC 9051 Section 6.3.9: {result:?}"
    );
}

/// RFC 5258 Section 3 / RFC 9051 Section 6.3.9: option tokens are
/// separated by protocol spaces in the command grammar, so leading and
/// trailing caller whitespace must not leak into the wire encoding.
#[test]
fn encode_list_extended_trims_outer_option_whitespace() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec![" SUBSCRIBED ".into(), "\tRECURSIVEMATCH\t".into()],
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec![" CHILDREN ".into(), " STATUS (MESSAGES) ".into()],
    };

    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST (SUBSCRIBED RECURSIVEMATCH) \"\" \"*\" RETURN (CHILDREN STATUS (MESSAGES))\r\n"
    );
}

/// RFC 5258 Section 3 / RFC 9051 Section 6.3.9: `RECURSIVEMATCH` is a
/// modifying selection option and MUST NOT be the only selection option
/// (or only with `REMOTE`).
#[test]
fn encode_list_extended_rejects_recursivematch_without_base_option() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec!["RECURSIVEMATCH".into()],
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: Vec::new(),
    };

    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        matches!(result, Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(ref msg))) if msg.contains("RECURSIVEMATCH")),
        "LIST-EXTENDED must reject bare RECURSIVEMATCH per RFC 5258 Section 3 / RFC 9051 Section 6.3.9: {result:?}"
    );
}

/// RFC 5819 Section 4 / RFC 9051 Section 7: the reserved LIST-STATUS
/// return option is `STATUS (<items>)`, not a bare `STATUS` atom.
#[test]
fn encode_list_extended_rejects_bare_status_return_option() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec!["SUBSCRIBED".into()],
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec!["STATUS".into()],
    };

    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        matches!(result, Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(ref msg))) if msg.contains("STATUS (") || msg.contains("STATUS (<items>)")),
        "LIST-EXTENDED must reject bare STATUS return option per RFC 5819 Section 4 / RFC 9051 Section 7: {result:?}"
    );
}

#[test]
fn encode_status() {
    let mut buf = BytesMut::new();
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "(MESSAGES UNSEEN)".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STATUS \"INBOX\" (MESSAGES UNSEEN)\r\n");
}

#[test]
fn encode_mailbox_with_special_chars() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new(r#"folder"name"#).unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SELECT \"folder\\\"name\"\r\n");
}

#[test]
fn encode_subscribe() {
    let mut buf = BytesMut::new();
    let cmd = Command::Subscribe {
        mailbox: MailboxName::new("INBOX.Sent").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SUBSCRIBE \"INBOX.Sent\"\r\n");
}

#[test]
fn encode_unsubscribe() {
    let mut buf = BytesMut::new();
    let cmd = Command::Unsubscribe {
        mailbox: MailboxName::new("INBOX.Old").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UNSUBSCRIBE \"INBOX.Old\"\r\n");
}

#[test]
fn encode_lsub() {
    let mut buf = BytesMut::new();
    let cmd = Command::Lsub {
        reference: String::new().into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LSUB \"\" \"*\"\r\n");
}

#[test]
fn encode_namespace() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Namespace, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 NAMESPACE\r\n");
}

#[test]
fn encode_list_status() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: "MESSAGES UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST \"\" \"*\" RETURN (STATUS (MESSAGES UNSEEN))\r\n"
    );
}

#[test]
fn encode_list_status_with_reference() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListStatus {
        reference: "INBOX".into(),
        pattern: "%".into(),
        status_items: "MESSAGES RECENT UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST \"INBOX\" \"%\" RETURN (STATUS (MESSAGES RECENT UNSEEN))\r\n"
    );
}

/// Mailbox name with spaces gets properly quoted in SELECT.
#[test]
fn encode_select_mailbox_with_spaces() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("my folder").unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SELECT \"my folder\"\r\n");
}

// M11: QRESYNC seq-match-data encoding (RFC 7162 Section 3.2.5.2)
#[test]
fn encode_select_qresync_seq_match_data() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(
            QresyncParams::from_raw(
                67890,
                12345,
                Some("1:500".into()),
                Some(("1:3".into(), "100:102".into())),
            )
            .unwrap(),
        ),
    };
    encode_command_to_buf(&mut buf, "A008", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A008 SELECT \"INBOX\" (QRESYNC (67890 12345 1:500 (1:3 100:102)))\r\n"
    );
}

// M13: CONDSTORE SELECT parameter (RFC 7162 Section 3.1.1)
#[test]
fn encode_select_condstore() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: true,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A009", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A009 SELECT \"INBOX\" (CONDSTORE)\r\n");
}

#[test]
fn encode_examine_condstore() {
    let mut buf = BytesMut::new();
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: true,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A010", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A010 EXAMINE \"INBOX\" (CONDSTORE)\r\n");
}

/// `$` must be valid as a component in comma-separated
/// sequence sets (RFC 5182 Section 5).
///
/// RFC 5182 Section 5 extends the grammar: `sequence-set =/ seq-last-command`
/// where `seq-last-command = "$"`. Since the original `sequence-set` includes
/// `*("," sequence-set)` recursion, `$` is valid in comma-separated lists.
#[test]
fn regression_sequence_set_dollar_in_comma_list() {
    // RFC 5182 Section 5: $ as component in comma-separated lists
    assert!(
        SequenceSet::<crate::types::Uid>::new("1:5,$").is_ok(),
        "\"1:5,$\" must be valid per RFC 5182 Section 5"
    );
    assert!(
        SequenceSet::<crate::types::Uid>::new("$,1:*").is_ok(),
        "\"$,1:*\" must be valid per RFC 5182 Section 5"
    );
    assert!(
        SequenceSet::<crate::types::Uid>::new("42,$").is_ok(),
        "\"42,$\" must be valid per RFC 5182 Section 5"
    );
    // Bare $ must still work
    assert!(
        SequenceSet::<crate::types::Uid>::new("$").is_ok(),
        "bare \"$\" must still be valid per RFC 5182 Section 2"
    );
    // $ cannot be part of a range (it's not a seq-number)
    assert!(
        SequenceSet::<crate::types::Uid>::new("$:5").is_err(),
        "\"$:5\" must be rejected ($ is not a seq-number, cannot form a range)"
    );
}

/// RFC 7162 Section 3.2.5.2: QRESYNC `mod_seq` is mod-sequence-value (>= 1).
#[test]
fn encode_select_qresync_rejects_zero_modseq() {
    assert!(crate::types::QresyncParams::from_raw(1, 0, None, None).is_err());
}

// ── Pre-refactor lock-down: LIST/LSUB/RENAME with non-ASCII names ──

#[test]
fn encode_list_non_ascii_reference() {
    // RFC 3501 Section 5.1.3: non-ASCII mailbox names are MUTF-7 encoded
    // when utf8 is false. "café" -> "caf&AOk-" (all ASCII, fits in quoted).
    let mut buf = BytesMut::new();
    let cmd = Command::List {
        reference: "café".into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LIST \"caf&AOk-\" \"*\"\r\n");
}

#[test]
fn encode_lsub_non_ascii_pattern() {
    // RFC 3501 Section 5.1.3: non-ASCII mailbox names are MUTF-7 encoded
    // when utf8 is false. "日本語" -> "&ZeVnLIqe-" (all ASCII, fits in quoted).
    let mut buf = BytesMut::new();
    let cmd = Command::Lsub {
        reference: String::new().into(),
        pattern: "日本語".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LSUB \"\" \"&ZeVnLIqe-\"\r\n");
}

#[test]
fn encode_rename_non_ascii_both_args() {
    // RFC 3501 Section 6.3.5 / RFC 6855: both old and new names non-ASCII.
    // encode_mailbox_name converts to modified UTF-7 when utf8=false.
    let mut buf = BytesMut::new();
    let cmd = Command::Rename {
        mailbox: MailboxName::new("Ünread").unwrap(),
        new_name: MailboxName::new("Gelöscht").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // "Ünread" -> "&ANw-nread", "Gelöscht" -> "Gel&APY-scht" in modified UTF-7
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.starts_with("A001 RENAME "),
        "command prefix mismatch: {output}"
    );
    // Both names should be ASCII after UTF-7 encoding -> quoted strings
    assert!(
        !output.contains('{'),
        "modified UTF-7 encoded names should not need literals: {output}"
    );
}

#[test]
fn encode_list_empty_reference_percent_pattern() {
    // RFC 3501 Section 6.3.8: % matches one level of hierarchy.
    let mut buf = BytesMut::new();
    let cmd = Command::List {
        reference: String::new().into(),
        pattern: "%".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LIST \"\" \"%\"\r\n");
}

#[test]
fn encode_lsub_with_reference_and_pattern() {
    // RFC 3501 Section 6.3.9: LSUB with non-empty reference.
    let mut buf = BytesMut::new();
    let cmd = Command::Lsub {
        reference: "INBOX.".into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LSUB \"INBOX.\" \"*\"\r\n");
}

// ---------------------------------------------------------------
// Pre-refactor coverage: COPY/MOVE with special mailbox names
// RFC 3501 Section 9: quoted strings that contain characters
// requiring escaping, or non-ASCII bytes that force literal form.
// ---------------------------------------------------------------

/// RFC 3501 Section 6.4.7 / Section 9: COPY with a mailbox name
/// containing a double-quote, which must be escaped in the quoted
/// string form (backslash-quote per quoted-specials).
#[test]
fn encode_copy_with_special_char_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::Copy {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("folder\"name").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // Double-quote is ASCII and quotable, so it gets backslash-escaped
    // in quoted form per RFC 3501 Section 9 (quoted-specials).
    assert_eq!(
        &buf[..],
        b"A001 COPY 1:5 \"folder\\\"name\"\r\n",
        "mailbox with double-quote should use quoted form with escaping"
    );
}

/// RFC 6851 / RFC 3501 Section 9: UID MOVE with a non-ASCII mailbox
/// name forces literal encoding (bytes > 0x7F are not TEXT-CHAR per
/// RFC 3501 Section 9, so they cannot appear in a quoted string).
#[test]
fn encode_uid_move_with_non_ascii_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidMove {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("caf\u{00E9}").unwrap(), // "café"  -  5 bytes in UTF-8
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // encode_mailbox_name converts to modified UTF-7 when utf8=false:
    // "café" -> "caf&AOk-" (ASCII), which then gets quoted.
    assert_eq!(
        &buf[..],
        b"A001 UID MOVE 1:5 \"caf&AOk-\"\r\n",
        "non-ASCII mailbox must be modified UTF-7 encoded on the wire"
    );
}

// --- UTF-8 mode: RFC 6855 Section 3 / RFC 9051 Section 9 ---

/// When UTF8=ACCEPT is enabled (RFC 6855 Section 3), non-ASCII UTF-8
/// mailbox names must be encoded as quoted strings rather than literals.
/// RFC 6855 Section 3: "The server MUST accept UTF-8 in quoted strings."
/// RFC 9051 Section 9: extends CHAR to include UTF8-2/UTF8-3/UTF8-4.
#[test]
fn encode_select_utf8_mailbox_quoted_when_utf8_enabled() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("日本語").unwrap(),
        condstore: false,
        qresync: None,
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::Synchronizing, true)).unwrap();
    let wire = encoded.to_vec();
    let wire_str = std::str::from_utf8(&wire).unwrap();

    // With utf8=true, encode_mailbox_name passes the name as raw UTF-8.
    // The per-command encoder then quotes it since it's valid UTF-8.
    assert_eq!(
        wire_str, "A001 SELECT \"日本語\"\r\n",
        "Non-ASCII mailbox should be quoted when UTF8=ACCEPT is active \
         (RFC 6855 Section 3); got literal instead"
    );
}

/// When utf8 mode is false, non-ASCII mailbox names must fall back to
/// literal encoding (RFC 3501 Section 9: CHAR = %x01-7F).
#[test]
fn encode_select_utf8_mailbox_literal_when_utf8_disabled() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("日本語").unwrap(),
        condstore: false,
        qresync: None,
    };
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    let wire = encoded.to_vec();
    let wire_str = std::str::from_utf8(&wire).unwrap();

    // Without utf8 mode, encode_mailbox_name converts to modified UTF-7:
    // "日本語" -> "&ZeVnLIqe-" (ASCII), which then gets quoted.
    assert!(
        !wire_str.contains('{'),
        "modified UTF-7 encoded mailbox should be quoted, not literal: {wire_str}"
    );
    assert!(
        wire_str.starts_with("A001 SELECT \""),
        "expected quoted mailbox name: {wire_str}"
    );
}

/// CREATE, DELETE, SUBSCRIBE, UNSUBSCRIBE with UTF-8 mailbox names.
#[test]
fn encode_mailbox_cmds_utf8_quoted() {
    for (variant, keyword) in [
        ("create", "CREATE"),
        ("delete", "DELETE"),
        ("subscribe", "SUBSCRIBE"),
        ("unsubscribe", "UNSUBSCRIBE"),
    ] {
        let cmd = match variant {
            "create" => Command::Create {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            "delete" => Command::Delete {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            "subscribe" => Command::Subscribe {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            "unsubscribe" => Command::Unsubscribe {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            _ => unreachable!(),
        };
        let encoded = encode_command("T1", &cmd, &opts(LiteralMode::Synchronizing, true)).unwrap();
        let buf = encoded.to_vec();
        let wire = std::str::from_utf8(&buf).unwrap();
        assert_eq!(
            wire,
            format!("T1 {keyword} \"Ångström\"\r\n"),
            "{keyword} with UTF-8 mailbox should produce quoted string \
             when UTF8=ACCEPT is active (RFC 6855 Section 3)"
        );
    }
}

/// STATUS items containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_status_items_rejected() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "(MESSAGES)\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "STATUS items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// LIST-STATUS `status_items` containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_list_status_items_rejected() {
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: "MESSAGES\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "LIST-STATUS items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// Normal STATUS items without CRLF must succeed.
#[test]
fn crlf_injection_normal_status_succeeds() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "(MESSAGES UNSEEN)".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal STATUS items must succeed: {result:?}"
    );
}

/// RFC 3501 Section 6.3.10: STATUS takes a parenthesized status-att list.
/// The encoder should accept raw item text from callers and normalize it
/// to the required on-wire `(<items>)` form.
#[test]
fn status_raw_items_are_parenthesized() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "MESSAGES UNSEEN".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "raw STATUS items should be normalized to a valid status-att-list: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 STATUS \"INBOX\" (MESSAGES UNSEEN)\r\n");
}

/// RFC 3501 Section 6.3.10: STATUS requires at least one status-att in
/// the parenthesized list.
#[test]
fn status_rejects_empty_items_list() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "()".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty STATUS item list must be rejected (RFC 3501 Section 6.3.10)"
    );
}

/// RFC 5819 Section 2 reuses STATUS data items and therefore also requires
/// at least one item inside `STATUS (<items>)`.
#[test]
fn list_status_rejects_empty_status_items() {
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: String::new().into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "LIST-STATUS must reject an empty status data item list (RFC 5819 Section 2)"
    );
}

/// Normal LIST-STATUS items without CRLF must succeed.
#[test]
fn crlf_injection_normal_list_status_succeeds() {
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: "MESSAGES UNSEEN".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal LIST-STATUS items must succeed: {result:?}"
    );
}

/// A one-character STATUS item list is valid in a LIST-EXTENDED return option.
#[test]
fn list_status_return_option_accepts_one_character_item_list() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: Vec::new(),
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec!["STATUS (X)".into()],
    };

    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_ok(),
        "the single item `X` must not be truncated before validation"
    );
    assert_eq!(&buf[..], b"A001 LIST \"\" \"*\" RETURN (STATUS (X))\r\n");
}

/// Nested parentheses are not valid in a LIST-EXTENDED STATUS item list.
#[test]
fn list_status_return_option_rejects_unbalanced_leading_paren() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: Vec::new(),
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec!["STATUS ((MESSAGES)".into()],
    };

    assert!(
        matches!(
            encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()),
            Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(_)
            ))
        ),
        "an unbalanced STATUS list must not be emitted"
    );
}

#[test]
fn listed_mailbox_wire_octets_survive_select_encoding() {
    use crate::codec::decode::parse_response_utf8;
    use crate::types::response::{Response, UntaggedResponse};

    let cases: &[(&[u8], bool)] = &[
        (b"caf\xc3\xa9", false),
        (b"raw\xff", false),
        (b"&AGE-", false),
        (b"&not!base64-", false),
        (b"&unterminated", false),
        (b"&2AA-", false),
        (b"control\x01\x7f", false),
        (b"&AAoALQ-", false),
        (b"caf\xc3\xa9 &AGE-", true),
        (b"invalid\xff", true),
        (b"line\r\nbreak", true),
    ];
    for &(raw, utf8) in cases {
        let mut response = format!("* LIST () \"/\" {{{}}}\r\n", raw.len()).into_bytes();
        response.extend_from_slice(raw);
        response.extend_from_slice(b"\r\n");
        let (rest, response) = parse_response_utf8(&response, (utf8).into()).unwrap();
        assert!(rest.is_empty());
        let Response::Untagged(response) = response else {
            panic!("expected LIST")
        };
        let UntaggedResponse::List(info) = *response else {
            panic!("expected LIST")
        };
        let cmd = Command::Select {
            mailbox: info.name,
            condstore: false,
            qresync: None,
        };
        let mut encoded = BytesMut::new();
        encode_command_to_buf(
            &mut encoded,
            "A1",
            &cmd,
            &opts(LiteralMode::LiteralPlus, utf8),
        )
        .unwrap();
        // These samples contain no quote or backslash: the only legal framing
        // choices here are a quoted string or a counted non-sync literal.
        let mut quoted = b"A1 SELECT \"".to_vec();
        quoted.extend_from_slice(raw);
        quoted.extend_from_slice(b"\"\r\n");
        let mut literal = format!("A1 SELECT {{{}+}}\r\n", raw.len()).into_bytes();
        literal.extend_from_slice(raw);
        literal.extend_from_slice(b"\r\n");
        assert!(
            encoded[..] == quoted || encoded[..] == literal,
            "{raw:?}: {encoded:?}"
        );
    }
}

#[test]
fn mailbox_identity_distinguishes_equal_display_names() {
    use crate::codec::decode::parse_response_utf8;
    use crate::types::response::{Response, UntaggedResponse};
    let parse = |raw: &[u8], utf8: bool| {
        let mut response = format!("* LIST () NIL {{{}}}\r\n", raw.len()).into_bytes();
        response.extend_from_slice(raw);
        response.extend_from_slice(b"\r\n");
        let (_, Response::Untagged(response)) =
            parse_response_utf8(&response, (utf8).into()).unwrap()
        else {
            panic!("expected LIST")
        };
        let UntaggedResponse::List(info) = *response else {
            panic!("expected LIST")
        };
        info.name
    };
    for (a, b, utf8) in [
        (b"&AGE-".as_slice(), b"a".as_slice(), false),
        (b"\xff".as_slice(), b"\xfe".as_slice(), true),
    ] {
        let a = parse(a, utf8);
        let b = parse(b, utf8);
        assert_eq!(a.as_str(), b.as_str());
        assert_ne!(a, b);
        let set = std::collections::HashSet::from([a, b]);
        assert_eq!(set.len(), 2);
    }
    assert_eq!(parse(b"caf&AOk-", false), MailboxName::new("café").unwrap());
    assert_eq!(
        parse("café".as_bytes(), true),
        MailboxName::new("café").unwrap()
    );
}

#[test]
fn mailbox_wire_nul_is_refused_instead_of_addressing_a_different_name() {
    let mailbox = MailboxName::from_wire("a\0b".into(), b"a\0b", true);
    let cmd = Command::Select {
        mailbox,
        condstore: false,
        qresync: None,
    };
    let mut buf = BytesMut::new();
    assert!(matches!(
        encode_command_to_buf(&mut buf, "A1", &cmd, &opts(LiteralMode::LiteralPlus, true)),
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
    assert!(buf.is_empty());
}

#[test]
fn mailbox_encoding_preserves_inbox_children_and_correlation() {
    use crate::codec::classification::{ClassificationContext, SolicitationRule, classify};
    use crate::types::notify::NotifyFlags;
    use crate::types::{CommandKind, UntaggedResponse};

    for utf8 in [false, true] {
        for spelling in ["inbox/Sub", "inbox.Sub", "inbox-Sub", "inbox_Sub", "inbox"] {
            let target = MailboxName::new(spelling).unwrap();
            let bytes = commands::encode_mailbox_name(&target, (utf8).into()).unwrap();
            let expected = if spelling == "inbox" {
                "INBOX"
            } else {
                spelling
            };
            assert_eq!(bytes, expected.as_bytes());
            let response = UntaggedResponse::MailboxStatus {
                mailbox: MailboxName::from_wire(expected.into(), &bytes, utf8),
                items: vec![],
            };
            let context = ClassificationContext {
                notify: NotifyFlags::default(),
                command_target: Some(&target),
            };
            assert!(matches!(
                classify(CommandKind::Status, &response, &context),
                SolicitationRule::OnlySolicited
            ));
        }
    }
}

#[test]
fn a_mode_change_refuses_noncanonical_addresses_before_writing() {
    let mailbox = MailboxName::from_wire("a".into(), b"&AGE-", false);
    let mut bytes = BytesMut::new();
    let result = encode_command_to_buf(
        &mut bytes,
        "A1",
        &Command::Select {
            mailbox,
            condstore: false,
            qresync: None,
        },
        &opts(LiteralMode::LiteralPlus, true),
    );
    assert!(matches!(
        result,
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
    assert!(bytes.is_empty());
}

/// QRESYNC seq-match-data known-uid-set must reject `*`.
#[test]
fn regression_qresync_seq_match_data_rejects_wildcard_in_uid_set() {
    assert!(
        crate::types::QresyncParams::from_raw(
            67890,
            12345,
            Some("1:500".into()),
            Some(("1:100".into(), "1:*".into()))
        )
        .is_err()
    );
}

/// QRESYNC seq-match-data known-sequence-set must reject `*`.
#[test]
fn regression_qresync_seq_match_data_rejects_wildcard_in_seq_set() {
    assert!(
        crate::types::QresyncParams::from_raw(
            67890,
            12345,
            Some("1:500".into()),
            Some(("1:*".into(), "1:100".into()))
        )
        .is_err()
    );
}

/// QRESYNC known-uids must reject `*` (RFC 7162 Section 3.2.5.2).
///
/// RFC 7162 Section 3.2.5.2 states that `*` is not allowed in
/// known-uids, known-sequence-set, and known-uid-set. The encoder
/// must return an error rather than silently producing invalid output.
#[test]
fn regression_qresync_known_uids_rejects_wildcard() {
    assert!(crate::types::QresyncParams::from_raw(67890, 12345, Some("1:*".into()), None).is_err());
}

/// QRESYNC seq-match-data without known-uids must return Err,
/// not silently produce malformed output (RFC 7162 Section 3.2.5.2).
///
/// The ABNF nests `[SP seq-match-data]` inside `[SP known-uids]`, meaning
/// seq-match-data can only appear when known-uids is present. The encoder
/// must return a proper error rather than relying on `debug_assert`.
#[test]
fn regression_qresync_seq_match_without_known_uids_returns_err() {
    assert!(
        crate::types::QresyncParams::from_raw(
            67890,
            12345,
            None,
            Some(("1:100".into(), "1:100".into()))
        )
        .is_err()
    );
}

/// QRESYNC with `known_uids` but no `seq_match_data` encodes correctly.
#[test]
fn audit_finding3_qresync_known_uids_without_seq_match() {
    use crate::types::QresyncParams;

    let params = QresyncParams::from_raw(67890, 12345, Some("1:500".into()), None).unwrap();
    let mut buf = BytesMut::new();
    push_select_or_examine(
        &mut buf,
        "A001",
        "SELECT",
        "INBOX",
        false,
        Some(&params),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 SELECT \"INBOX\" (QRESYNC (67890 12345 1:500))\r\n",
        "QRESYNC with known_uids but no seq_match_data"
    );
}

/// QRESYNC with valid `known_uids` + `seq_match_data` encodes correctly.
#[test]
fn audit_finding3_qresync_valid_known_uids_with_seq_match() {
    use crate::types::QresyncParams;

    let params = QresyncParams::from_raw(
        67890,
        12345,
        Some("1:500".into()),
        Some(("1:100".into(), "1:100".into())),
    )
    .unwrap();
    let mut buf = BytesMut::new();
    push_select_or_examine(
        &mut buf,
        "A001",
        "SELECT",
        "INBOX",
        false,
        Some(&params),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();

    // Valid case: known_uids is present, so seq_match_data is legal.
    assert!(
        output.contains("1:500"),
        "known_uids should appear in output: {output}"
    );
    assert!(
        output.contains("(1:100 1:100)"),
        "seq_match_data should appear in output: {output}"
    );
    assert!(
        !output.contains("1:*"),
        "should NOT contain fabricated 1:* when known_uids is provided: {output}"
    );
}

// ===== Audit finding #3: QRESYNC encoder fabricates known_uids =====

/// RFC 7162 Section 3.2.5.2 ABNF:
/// `"QRESYNC" SP "(" uidvalidity SP mod-sequence-value
///   [SP known-uids] [SP seq-match-data] ")"`
///
/// `seq-match-data` is only valid after `known-uids`. The encoder must
/// NOT silently inject `1:*` as a placeholder when `seq_match_data` is
/// provided without `known_uids`  -  it must return an error.
#[test]
fn audit_finding3_qresync_rejects_seq_match_without_known_uids() {
    assert!(
        crate::types::QresyncParams::from_raw(
            67890,
            12345,
            None,
            Some(("1:100".into(), "1:100".into()))
        )
        .is_err()
    );
}
