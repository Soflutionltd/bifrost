use super::*;

// --- MULTIAPPEND (RFC 3502) encoder tests ---

#[test]
fn encode_multi_append_first_message_no_flags() {
    // First message with no flags and no date (RFC 3502 Section 3).
    let msgs = [append_msg(&[], None, 100)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" {100}\r\n<100>\r\n"
    );
}

#[test]
fn encode_multi_append_first_message_with_flags() {
    // First message with flags (RFC 3502 Section 3 / RFC 3501 Section 6.3.11).
    let msgs = [append_msg(
        &[crate::types::Flag::Seen, crate::types::Flag::Flagged],
        None,
        200,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" (\\Seen \\Flagged) {200}\r\n<200>\r\n"
    );
}

#[test]
fn encode_multi_append_first_message_with_date() {
    // First message with internal date (RFC 3502 Section 3 / RFC 3501 Section 6.3.11).
    let msgs = [append_msg(&[], Some("17-Jul-1996 02:44:25 -0700"), 50)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" \"17-Jul-1996 02:44:25 -0700\" {50}\r\n<50>\r\n"
    );
}

#[test]
fn encode_multi_append_first_message_with_flags_and_date() {
    // First message with both flags and date (RFC 3502 Section 3).
    let msgs = [append_msg(
        &[crate::types::Flag::Seen],
        Some(" 1-Jan-2024 00:00:00 +0000"),
        300,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" (\\Seen) \" 1-Jan-2024 00:00:00 +0000\" {300}\r\n<300>\r\n"
    );
}

#[test]
fn encode_multi_append_subsequent_message() {
    // Subsequent message  -  no tag/APPEND/mailbox prefix, and it follows the
    // previous body directly (RFC 3502 Section 3).
    let msgs = [
        append_msg(&[], None, 10),
        append_msg(&[crate::types::Flag::Draft], None, 75),
    ];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" {10}\r\n<10> (\\Draft) {75}\r\n<75>\r\n"
    );
}

#[test]
fn encode_multi_append_with_literal_plus() {
    // LITERAL+ (RFC 7888)  -  non-synchronizing literal.
    let msgs = [append_msg(&[], None, 42)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::LiteralPlus, false)
        ),
        "A001 APPEND \"INBOX\" {42+}\r\n<42>\r\n"
    );
}

#[test]
fn encode_multi_append_subsequent_with_literal_plus() {
    // Subsequent message with LITERAL+ (RFC 7888 / RFC 3502 Section 3).
    let msgs = [append_msg(&[], None, 1), append_msg(&[], None, 99)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::LiteralPlus, false)
        ),
        "A001 APPEND \"INBOX\" {1+}\r\n<1> {99+}\r\n<99>\r\n"
    );
}

#[test]
fn encode_multi_append_two_messages_mixed_flags() {
    // 2-message MULTIAPPEND: first with flags and date, second with neither
    // (RFC 3502). Synchronizing, so each body is preceded by a boundary.
    let msgs = [
        append_msg(
            &[crate::types::Flag::Seen, crate::types::Flag::Answered],
            Some("15-Mar-2026 10:00:00 +0000"),
            50,
        ),
        append_msg(&[], None, 30),
    ];
    let cmd = append_command(
        "A010",
        "INBOX",
        &msgs,
        true,
        &append_opts(LiteralMode::Synchronizing, false),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A010 APPEND \"INBOX\" (\\Seen \\Answered) \"15-Mar-2026 10:00:00 +0000\" {50}\r\n"
                .to_owned(),
            "<50> {30}\r\n".to_owned(),
            "<30>\r\n".to_owned(),
        ],
        "N synchronizing literals must yield N+1 segments"
    );
}

#[test]
fn encode_multi_append_three_messages_literal_plus() {
    // 3-message MULTIAPPEND with LITERAL+ (RFC 3502 / RFC 7888 Section 4).
    let msgs = [
        append_msg(&[crate::types::Flag::Seen], None, 100),
        append_msg(&[], Some(" 1-Jan-2025 00:00:00 +0000"), 200),
        append_msg(&[crate::types::Flag::Flagged], None, 300),
    ];
    let cmd = append_command(
        "A020",
        "Archive",
        &msgs,
        true,
        &append_opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A020 APPEND \"Archive\" (\\Seen) {100+}\r\n<100> \" 1-Jan-2025 00:00:00 +0000\" \
             {200+}\r\n<200> (\\Flagged) {300+}\r\n<300>\r\n"
                .to_owned()
        ],
        "with LITERAL+ nothing waits, so the whole command is one segment"
    );
}

#[test]
fn encode_multi_append_subsequent_no_flags_with_date() {
    // Subsequent message with date but no flags (RFC 3502 Section 3).
    let msgs = [
        append_msg(&[], None, 10),
        append_msg(&[], Some("25-Dec-2025 12:00:00 +0000"), 500),
    ];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" {10}\r\n<10> \"25-Dec-2025 12:00:00 +0000\" {500}\r\n<500>\r\n"
    );
}

#[test]
fn encode_multi_append_special_mailbox() {
    // Mailbox name with special characters uses quoting (RFC 3501 Section 9).
    let msgs = [append_msg(&[], None, 10)];
    assert_eq!(
        append_wire(
            "A001",
            r#"folder"name"#,
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"folder\\\"name\" {10}\r\n<10>\r\n"
    );
}

/// The encoder must not copy a body: the chunk it emits is the caller's own
/// allocation, and both a synchronizing and a non-synchronizing literal keep
/// that property.
#[test]
fn encode_append_bodies_are_carried_by_reference() {
    let msgs = [append_msg(&[], None, 8192), append_msg(&[], None, 4)];
    for mode in [LiteralMode::Synchronizing, LiteralMode::LiteralPlus] {
        let cmd = append_command("A001", "INBOX", &msgs, true, &append_opts(mode, false)).unwrap();
        let carried: Vec<&bytes::Bytes> = cmd
            .segments()
            .iter()
            .flatten()
            .filter(|chunk| is_body_chunk(chunk, &msgs))
            .collect();
        assert_eq!(
            carried.len(),
            2,
            "both bodies present, once each ({mode:?})"
        );
        for (chunk, msg) in carried.iter().zip(&msgs) {
            assert_eq!(chunk.as_ptr(), msg.data.as_ptr(), "no copy ({mode:?})");
        }
    }
}

/// A mailbox that cannot be a quoted string (here, one carrying TAB; CR, LF and
/// NUL are refused outright by mailbox validation) becomes a
/// literal of its own. Under synchronizing literals that is a boundary in the
/// middle of the header, so the message body is the THIRD segment.
///
/// Only in UTF-8 mode: outside it the mailbox is modified UTF-7, which encodes
/// CR and LF into ASCII, so no mailbox name ever needs a literal there. The
/// second half pins that too.
#[test]
fn encode_append_splits_at_a_synchronizing_mailbox_literal() {
    let msgs = [append_msg(&[], None, 6)];
    let cmd = append_command(
        "A001",
        "a\tb",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, true),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A001 APPEND {3}\r\n".to_owned(),
            "a\tb UTF8 (~{6}\r\n".to_owned(),
            "<6>)\r\n".to_owned(),
        ]
    );

    let cmd = append_command(
        "A001",
        "a\tb",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A001 APPEND \"a&AAk-b\" {6}\r\n".to_owned(),
            "<6>\r\n".to_owned(),
        ],
        "modified UTF-7 turns TAB into ASCII, so the mailbox stays quoted"
    );
}

/// Non-empty segments: an empty message body is a legal zero-length literal and
/// still gets its own continuation under synchronizing literals.
#[test]
fn encode_append_zero_length_body_is_still_a_segment() {
    let msgs = [crate::types::AppendMessage::new(bytes::Bytes::new())];
    let cmd = append_command(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    )
    .unwrap();
    assert_eq!(cmd.segments().len(), 2);
    assert!(cmd.segments().iter().all(|segment| !segment.is_empty()));
}

// ===== Audit finding #1: APPEND lacks RFC 6855 UTF8 data extension =====

/// RFC 6855 Section 4: when UTF8=ACCEPT has been enabled, APPEND of a
/// message with UTF-8 headers MUST use the `UTF8` APPEND data extension:
/// `APPEND <mailbox> UTF8 (~{size[+]}\r\n<message>)`.
#[test]
fn audit_finding1_append_header_with_utf8_extension() {
    let msgs = [append_msg(&[], None, 100)];
    // UTF8=ACCEPT enabled, no literal extension.
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, true),
    );
    assert_eq!(
        output, "A001 APPEND \"INBOX\" UTF8 (~{100}\r\n<100>)\r\n",
        "RFC 6855 Section 4: UTF8 APPEND data extension, closed after the body"
    );
}

/// Without UTF8 mode, the classic form is used.
#[test]
fn audit_finding1_append_header_classic_without_utf8() {
    let msgs = [append_msg(&[], None, 100)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    );
    assert_eq!(output, "A001 APPEND \"INBOX\" {100}\r\n<100>\r\n");
    assert!(!output.contains("UTF8"));
}

/// RFC 6855 Section 4 in MULTIAPPEND: each message is its own `UTF8 (...)`
/// group, closed before the next message's syntax, with one final CRLF.
#[test]
fn append_utf8_extension_wraps_every_message_of_a_multiappend() {
    let msgs = [
        append_msg(&[crate::types::Flag::Seen], None, 5),
        append_msg(&[], None, 7),
    ];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, true)
        ),
        "A001 APPEND \"INBOX\" (\\Seen) UTF8 (~{5}\r\n<5>) UTF8 (~{7}\r\n<7>)\r\n"
    );
}

/// RFC 3516 Section 4.4 / RFC 9051 Section 9: APPEND data containing NUL
/// octets must use the `literal8` marker `~{size}\r\n`, not classic
/// `literal` syntax. It needs BINARY; here no literal extension is in play.
#[test]
fn audit_finding6_append_header_uses_literal8_for_binary_append() {
    let msgs = [append_nul_msg(12)];
    let mut o = append_opts(LiteralMode::Synchronizing, false);
    std::sync::Arc::make_mut(&mut o.session)
        .capabilities
        .push(Capability::Binary);
    assert_eq!(
        append_wire("A001", "INBOX", &msgs, false, &o),
        "A001 APPEND \"INBOX\" ~{12}\r\n<12>\r\n"
    );
}

/// RFC 3516 Section 4.4: a NUL-bearing body without BINARY is refused before
/// any bytes exist, as `MissingCapability`: the body is expressible, the
/// server lacks the capability that carries it, and a local refusal is never
/// a provider protocol fault.
#[test]
fn append_nul_body_without_binary_is_refused() {
    let msgs = [append_nul_msg(3)];
    let err = append_command(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::LiteralPlus, false),
    )
    .expect_err("literal8 needs BINARY");
    assert!(
        matches!(err, crate::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(ref m)) if m.contains("requires BINARY")),
        "got {err:?}"
    );
}

/// The refusal is per message: a clean first message does not excuse a NUL in
/// the second.
#[test]
fn multiappend_nul_body_without_binary_is_refused_in_any_position() {
    let msgs = [append_msg(&[], None, 3), append_nul_msg(3)];
    assert!(
        append_command(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, false),
        )
        .is_err()
    );
}

/// RFC 7888 Section 6 matrix for literal8: the `+` marker needs BINARY AND a
/// literal extension, with the LITERAL- size cap, and is never used on pure
/// `IMAP4rev2` (RFC 9051 Section 9).
#[test]
fn append_literal8_plus_marker_matrix() {
    let binary = |mode: LiteralMode| {
        let mut o = append_opts(mode, false);
        std::sync::Arc::make_mut(&mut o.session)
            .capabilities
            .push(Capability::Binary);
        o
    };
    let small = [append_nul_msg(100)];
    let large = [append_nul_msg(5000)];

    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &small,
            false,
            &binary(LiteralMode::LiteralPlus)
        ),
        "A APPEND \"INBOX\" ~{100+}\r\n<100>\r\n",
        "BINARY + LITERAL+: non-synchronizing literal8"
    );
    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &small,
            false,
            &binary(LiteralMode::LiteralMinus)
        ),
        "A APPEND \"INBOX\" ~{100+}\r\n<100>\r\n",
        "BINARY + LITERAL- within 4096 octets"
    );
    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &large,
            false,
            &binary(LiteralMode::LiteralMinus)
        ),
        "A APPEND \"INBOX\" ~{5000}\r\n<5000>\r\n",
        "BINARY + LITERAL- over 4096 octets stays synchronizing"
    );
    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &small,
            false,
            &binary(LiteralMode::Synchronizing)
        ),
        "A APPEND \"INBOX\" ~{100}\r\n<100>\r\n",
        "BINARY without a literal extension"
    );

    // Pure rev2 with BINARY advertised: literal8 is always synchronizing.
    let rev2_binary = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::Utf8,
        literal_mode: LiteralMode::LiteralMinus,
        session: crate::types::profile::SessionCapabilities::new(
            vec![
                Capability::Imap4Rev2,
                Capability::MultiAppend,
                Capability::Binary,
            ],
            Vec::new(),
        ),
    };
    assert_eq!(
        append_wire("A", "INBOX", &small, false, &rev2_binary),
        "A APPEND \"INBOX\" ~{100}\r\n<100>\r\n",
        "rev2 literal8 has no `+` modifier"
    );
}

/// RFC 9051 Appendix B: IMAP4rev2 folds in the BINARY FETCH items but not
/// RFC 3516's APPEND extension, so a pure rev2 server that does not advertise
/// BINARY is owed no `literal8` APPEND body. A NUL-bearing message is refused
/// before any byte exists, exactly as on rev1.
#[test]
fn rev2_does_not_imply_binary_for_append() {
    let rev2 = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::Utf8,
        literal_mode: LiteralMode::LiteralMinus,
        session: crate::types::profile::SessionCapabilities::new(
            vec![Capability::Imap4Rev2],
            Vec::new(),
        ),
    };
    let err = append_command("A", "INBOX", &[append_nul_msg(100)], false, &rev2)
        .expect_err("pure rev2 without BINARY cannot carry a NUL body");
    assert!(
        matches!(err, crate::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(ref m)) if m.contains("requires BINARY")),
        "got {err:?}"
    );
}

// =====================================================================
// Audit tests (2026-03-16)
// =====================================================================

/// H3: Single-message APPEND must filter `\Recent` and `\*`.
/// RFC 3501 Section 9: `flag` excludes `\Recent` and `\*`.
#[test]
fn audit_h3_single_append_filters_recent_and_wildcard() {
    let flags = vec![
        crate::types::Flag::Seen,
        crate::types::Flag::Recent,
        crate::types::Flag::Wildcard,
        crate::types::Flag::Flagged,
    ];
    let msgs = [append_msg(&flags, None, 10)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    );
    assert!(
        !output.contains("\\Recent"),
        "must filter \\Recent: {output}"
    );
    assert!(!output.contains("\\*"), "must filter \\*: {output}");
    assert!(output.contains("\\Seen"), "must keep \\Seen");
    assert!(output.contains("\\Flagged"), "must keep \\Flagged");
}

/// RFC 3501 Section 9: APPEND must filter `\Recent` and `\*` from flags.
#[test]
fn append_filters_recent_and_wildcard_flags() {
    let msgs = [append_msg(
        &[
            crate::types::Flag::Seen,
            crate::types::Flag::Recent,
            crate::types::Flag::Wildcard,
            crate::types::Flag::Flagged,
        ],
        None,
        42,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" (\\Seen \\Flagged) {42}\r\n<42>\r\n",
        "Recent and Wildcard must be filtered from APPEND flags"
    );
}

/// APPEND accepts valid custom flag keywords (RFC 3501 Section 9).
#[test]
fn append_accepts_valid_custom_flag() {
    let msgs = [append_msg(
        &[crate::types::Flag::from_imap_str("$MailFlagBit0")],
        None,
        10,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" ($MailFlagBit0) {10}\r\n<10>\r\n"
    );
}

/// APPEND rejects custom flags containing invalid characters (RFC 3501 Section 9).
#[test]
fn append_rejects_invalid_custom_flag() {
    let msgs = [append_msg(
        &[crate::types::Flag::from_imap_str("bad{flag}")],
        None,
        10,
    )];
    let result = append_command(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    );
    assert!(result.is_err(), "custom flag with braces must be rejected");
}

/// MULTIAPPEND needs the capability (RFC 3502 Section 3) and at least one
/// message; a single APPEND carries exactly one.
#[test]
fn append_shape_and_capability_validation() {
    let one = [append_msg(&[], None, 1)];
    let two = [append_msg(&[], None, 1), append_msg(&[], None, 1)];
    let with_cap = append_opts(LiteralMode::Synchronizing, false);
    let without_cap = opts(LiteralMode::Synchronizing, false);

    assert!(matches!(
        append_command("A", "INBOX", &one, true, &without_cap),
        Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(ref m))) if m.capability == Capability::MultiAppend
    ));
    // A plain APPEND never needs it.
    assert!(append_command("A", "INBOX", &one, false, &without_cap).is_ok());
    assert!(matches!(
        append_command("A", "INBOX", &[], true, &with_cap),
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
    assert!(matches!(
        append_command("A", "INBOX", &two, false, &with_cap),
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
}

/// Under active `IMAP4rev2` the mailbox goes out as raw UTF-8 like every other
/// command's mailbox argument, not as modified UTF-7 (RFC 9051 Section 5.1);
/// `UTF8=ACCEPT` is what selects the `UTF8 (` wrapper, and rev2 alone does not.
#[test]
fn append_rev2_uses_utf8_mailbox_without_the_utf8_wrapper() {
    let rev2 = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::Utf8,
        literal_mode: LiteralMode::Synchronizing,
        session: crate::types::profile::SessionCapabilities::new(
            vec![Capability::Imap4Rev2],
            Vec::new(),
        ),
    };
    let msgs = [append_msg(&[], None, 3)];
    assert_eq!(
        append_wire("A", "café", &msgs, false, &rev2),
        "A APPEND \"café\" {3}\r\n<3>\r\n"
    );
    // The same mailbox on rev1 without UTF8=ACCEPT is modified UTF-7.
    assert_eq!(
        append_wire(
            "A",
            "café",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A APPEND \"caf&AOk-\" {3}\r\n<3>\r\n"
    );
}

// --- APPEND date-time validation (RFC 3501 Section 9) ---

/// RFC 3501 Section 9: `date-day-fixed = (SP DIGIT) / 2DIGIT`.
/// The `2DIGIT` alternative matches any two ASCII digits, so zero-padded
/// single-digit days like `07` are syntactically valid and semantically
/// correct (day 7 of the month). The validator must accept them.
#[test]
fn test_append_accepts_zero_padded_day() {
    let result = append_with_date("07-Jul-1996 02:44:25 -0700");
    assert!(
        result.is_ok(),
        "zero-padded day '07' is valid per RFC 3501 Section 9 \
         (date-day-fixed = (SP DIGIT) / 2DIGIT): {result:?}"
    );
}

/// Zero-padded days 01 through 09 must all be accepted per RFC 3501 Section 9.
#[test]
fn test_append_accepts_all_zero_padded_days() {
    for day in 1..=9u8 {
        let date = format!("0{day}-Jan-2024 12:00:00 +0000");
        let result = validate_append_datetime(&date);
        assert!(
            result.is_ok(),
            "zero-padded day '0{day}' must be accepted per RFC 3501 Section 9 \
             date-day-fixed 2DIGIT: {result:?}"
        );
    }
}

/// Day `00` must be rejected  -  it matches `2DIGIT` syntactically but is
/// not a valid calendar day.
#[test]
fn test_append_rejects_day_zero() {
    let result = validate_append_datetime("00-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day '00' is not a valid calendar day and must be rejected"
    );
}

/// invalid month name must be rejected (RFC 3501 Section 9).
#[test]
fn test_append_rejects_invalid_month() {
    let result = append_with_date("31-Foo-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "invalid month name should be rejected per RFC 3501 Section 9 date-month"
    );
}

/// completely malformed date string must be rejected, with the
/// `InvalidAppendDate` variant the account layer maps: the encoder returns
/// the crate's own error, so the variant survives to the driver unchanged.
#[test]
fn test_append_rejects_garbage_date() {
    let result = append_with_date("not-a-date");
    assert!(
        matches!(
            result,
            Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidAppendDate(_)
            ))
        ),
        "garbage date string should be rejected per RFC 3501 Section 9: {result:?}"
    );
}

/// RFC 3501 Section 9 paragraph (1): "all alphabetic characters are
/// case-insensitive ... Implementations MUST accept these strings in a
/// case-insensitive fashion." Month names like "JAN", "jan", "jAn" are
/// valid per RFC 5234 Section 2.3 (ABNF strings are case-insensitive).
#[test]
fn test_append_accepts_case_insensitive_month() {
    // Uppercase month
    let result = append_with_date(" 7-JAN-2024 12:00:00 +0000");
    assert!(
        result.is_ok(),
        "uppercase month must be accepted per RFC 3501 Section 9 paragraph (1): {result:?}"
    );

    // Lowercase month
    let result2 = append_with_date("15-jul-2024 12:00:00 +0000");
    assert!(
        result2.is_ok(),
        "lowercase month must be accepted per RFC 3501 Section 9 paragraph (1): {result2:?}"
    );

    // Mixed case month
    let result3 = append_with_date("20-sEp-2024 12:00:00 +0000");
    assert!(
        result3.is_ok(),
        "mixed-case month must be accepted per RFC 3501 Section 9 paragraph (1): {result3:?}"
    );
}

/// Valid date-time strings per RFC 3501 Section 9 must be accepted.
#[test]
fn test_append_accepts_valid_datetime() {
    // Space-padded single-digit day.
    let result = append_with_date(" 7-Jul-1996 02:44:25 -0700");
    assert!(
        result.is_ok(),
        "valid space-padded day should be accepted: {result:?}"
    );

    // Two-digit day.
    let result2 = append_with_date("17-Jul-1996 02:44:25 -0700");
    assert!(
        result2.is_ok(),
        "valid two-digit day should be accepted: {result2:?}"
    );
}

/// RFC 6855 Section 3: verify `encode_append` uses quoted form
/// for a UTF-8 mailbox name when UTF-8 mode is on.
#[test]
fn spec_audit_multi_append_utf8_mailbox_quoted() {
    let msgs = [append_msg(&[], None, 42)];
    let output = append_wire(
        "A001",
        "日本語",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, true),
    );
    let output = output.as_str();
    // The mailbox should be quoted, not a literal.
    assert!(
        output.contains("APPEND \""),
        "UTF-8 mailbox in MULTIAPPEND must be quoted when UTF8=ACCEPT is active \
         per RFC 6855 Section 3, got: '{output}'"
    );
}

/// RFC 7888 Section 5: `encode_append` must respect the
/// LITERAL- size limit for the message literal.
#[test]
fn literal_minus_multi_append_large_message_synchronizing() {
    // Message size 5000 > 4096, must use synchronizing form in LITERAL- mode.
    let msgs = [append_msg(&[], None, 5000)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        true,
        &append_opts(LiteralMode::LiteralMinus, false),
    );
    let output = output.as_str();
    assert!(
        output.contains("{5000}\r\n"),
        "LITERAL- with message > 4096 bytes must use synchronizing literal \
         in MULTIAPPEND (RFC 7888 Section 5); got: {output}"
    );
    assert!(
        !output.contains("{5000+}"),
        "LITERAL- with message > 4096 bytes must NOT use non-synchronizing \
         literal in MULTIAPPEND (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: `encode_append` uses non-synchronizing
/// form for small messages in LITERAL- mode.
#[test]
fn literal_minus_multi_append_small_message_non_synchronizing() {
    // Message size 100 <= 4096, must use non-synchronizing form.
    let msgs = [append_msg(&[], None, 100)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        true,
        &append_opts(LiteralMode::LiteralMinus, false),
    );
    let output = output.as_str();
    assert!(
        output.contains("{100+}\r\n"),
        "LITERAL- with message <= 4096 bytes must use non-synchronizing \
         literal in MULTIAPPEND (RFC 7888 Section 5); got: {output}"
    );
}

#[test]
fn append_list_patterns_and_notify_keep_retained_wire_addresses() {
    use crate::types::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};
    let name = MailboxName::from_wire("a".into(), b"&AGE-", false);
    let commands = [
        Command::Append {
            mailbox: (&name).into(),
            messages: vec![crate::types::AppendMessage::new(bytes::Bytes::from_static(
                b"x",
            ))],
            multi: false,
        },
        Command::List {
            reference: "".into(),
            pattern: (&name).into(),
        },
        Command::ListExtended {
            selection_options: Vec::new(),
            reference: "".into(),
            patterns: vec![(&name).into(), "*".into()],
            return_options: Vec::new(),
        },
        Command::ListStatus {
            reference: "".into(),
            pattern: (&name).into(),
            status_items: "MESSAGES".into(),
        },
        Command::NotifySet(NotifySetParams::new(
            vec![NotifyEventGroup::new(
                MailboxFilter::Mailboxes(vec![(&name).into()]),
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: Vec::new(),
                    },
                    NotifyEvent::MessageExpunge,
                ],
            )],
            false,
        )),
    ];
    for command in commands {
        let mut bytes = BytesMut::new();
        encode_command_to_buf(
            &mut bytes,
            "A1",
            &command,
            &opts(LiteralMode::LiteralPlus, false),
        )
        .unwrap();
        assert!(
            bytes.windows(5).any(|window| window == b"&AGE-"),
            "{command:?}: {bytes:?}"
        );
    }
}

/// RFC 3501 Section 9: `zone = ("+" / "-") 4DIGIT` has no semantic
/// constraint on the digit values.  The validation must allow any
/// hour 0-14 with any minute 0-59.  In particular, +1430 and +1445
/// must be accepted: although no current IANA timezone uses these
/// exact offsets at hour 14, the RFC grammar imposes no such
/// restriction, and future timezone changes could introduce them.
/// Real-world sub-hour offsets like +1245 (Chatham Islands standard)
/// and +1345 (Chatham Islands DST) demonstrate that non-zero minutes
/// with high hours are legitimate.
#[test]
fn validate_datetime_timezone_hour_14_with_nonzero_minutes() {
    // +1430  -  must be accepted per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1430").is_ok(),
        "+1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
    // +1445  -  must be accepted per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1445").is_ok(),
        "+1445 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
    // -1430  -  negative symmetric boundary must also be accepted.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 -1430").is_ok(),
        "-1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
}

/// RFC 3501 Section 9: `zone = ("+" / "-") 4DIGIT`  -  any hour 0-14
/// with any minute 0-59 is valid.  +1400 (Line Islands, Kiribati)
/// and offsets with non-zero minutes at hour 14 must all be accepted.
#[test]
fn validate_datetime_timezone_hour_14_allows_any_minute() {
    // +1400  -  valid (Line Islands, Kiribati).
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1400").is_ok(),
        "+1400 must be accepted  -  it is the maximum real UTC offset"
    );
    // +1430  -  valid per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1430").is_ok(),
        "+1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
    // -1400  -  valid symmetric boundary.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 -1400").is_ok(),
        "-1400 must be accepted  -  symmetric with +1400"
    );
    // -1430  -  valid per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 -1430").is_ok(),
        "-1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
}

/// Boundary: timezone minute 59 is the maximum valid value and must be accepted.
#[test]
fn validate_datetime_timezone_minute_59_is_valid() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +0059");
    assert!(
        result.is_ok(),
        "timezone minute 59 must be accepted  -  it is the maximum valid value: {result:?}"
    );
}

/// RFC 3501 Section 9: timezone minute > 59 must be rejected.
#[test]
fn validate_datetime_timezone_minute_exceeds_59() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +0060");
    assert!(
        result.is_err(),
        "timezone minute 60 must be rejected  -  maximum is 59 (RFC 3501 Section 9)"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("zone minute"),
        "error should mention zone minute, got: {err_msg}"
    );
}

/// Boundary: timezone hour 14 is the maximum valid value and must be accepted.
#[test]
fn validate_datetime_timezone_hour_14_is_valid() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +1400");
    assert!(
        result.is_ok(),
        "timezone hour 14 must be accepted  -  it is the maximum valid offset: {result:?}"
    );
}

/// RFC 3501 Section 9 / RFC 5322 Section 4.3: timezone hour > 14 must be
/// rejected (maximum real UTC offset is +14:00).
#[test]
fn validate_datetime_timezone_hour_exceeds_14() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +1500");
    assert!(
        result.is_err(),
        "timezone hour 15 must be rejected  -  maximum is 14 (RFC 3501 Section 9)"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("zone hour"),
        "error should mention zone hour, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: zone with non-digit HHMM must be rejected.
#[test]
fn validate_datetime_invalid_timezone_non_digit() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +ABCD");
    assert!(
        result.is_err(),
        "zone with non-digit HHMM must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9: zone must be ("+" / "-") 4DIGIT.
#[test]
fn validate_datetime_invalid_timezone_format() {
    // Zone without +/- prefix.
    let result = validate_append_datetime("01-Jan-2024 12:00:00 X0000");
    assert!(
        result.is_err(),
        "zone without +/- prefix must be rejected per RFC 3501 Section 9 zone format"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("zone"),
        "error should mention zone, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: there must be a SP at position 20 (between time and zone).
#[test]
fn validate_datetime_bad_separator_at_position_20() {
    // Replace SP at position 20 with 'X'.
    let result = validate_append_datetime("01-Jan-2024 12:00:00X+0000");
    assert!(
        result.is_err(),
        "non SP at position 20 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 20"),
        "error should mention position 20, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: time with non-digit characters must be rejected.
#[test]
fn validate_datetime_invalid_time_non_digit() {
    let result = validate_append_datetime("01-Jan-2024 AB:CD:EF +0000");
    assert!(
        result.is_err(),
        "non-digit time must be rejected per RFC 3501 Section 9 time = 2DIGIT \":\" 2DIGIT \":\" 2DIGIT"
    );
}

/// RFC 3501 Section 9: time must be 2DIGIT ":" 2DIGIT ":" 2DIGIT.
#[test]
fn validate_datetime_invalid_time_format() {
    // Replace colons in time with dashes.
    let result = validate_append_datetime("01-Jan-2024 12-00-00 +0000");
    assert!(
        result.is_err(),
        "time 'HH-MM-SS' must be rejected  -  colons are required per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("time"),
        "error should mention time, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: there must be a SP at position 11 (between date and time).
#[test]
fn validate_datetime_bad_separator_at_position_11() {
    // Replace SP at position 11 with 'X'.
    let result = validate_append_datetime("01-Jan-2024X12:00:00 +0000");
    assert!(
        result.is_err(),
        "non SP at position 11 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 11"),
        "error should mention position 11, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: date-year must be 4DIGIT.
#[test]
fn validate_datetime_invalid_year() {
    // Replace year digits with non-digits.
    let result = validate_append_datetime("01-Jan-ABCD 12:00:00 +0000");
    assert!(
        result.is_err(),
        "non-digit year 'ABCD' must be rejected per RFC 3501 Section 9 date-year = 4DIGIT"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("year"),
        "error should mention year, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: the second separator at position 6 must be '-'.
#[test]
fn validate_datetime_bad_separator_at_position_6() {
    // Replace '-' at position 6 with '/'.
    let result = validate_append_datetime("01-Jan/2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "non '-' at position 6 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 6"),
        "error should mention position 6, got: {err_msg}"
    );
}

// -----------------------------------------------------------------------
// Date separator and field error paths (RFC 3501 Section 9)
// -----------------------------------------------------------------------

/// RFC 3501 Section 9: the first separator at position 2 must be '-'.
#[test]
fn validate_datetime_bad_separator_at_position_2() {
    // Replace '-' at position 2 with '/'.
    let result = validate_append_datetime("01/Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "non '-' at position 2 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 2"),
        "error should mention position 2, got: {err_msg}"
    );
}

/// Day starting with '9' also hits the fallthrough branch.
#[test]
fn validate_datetime_rejects_day_starting_with_nine() {
    let result = validate_append_datetime("91-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day '91' must be rejected  -  first byte '9' falls through \
         per RFC 3501 Section 9 date-day-fixed"
    );
}

/// RFC 3501 Section 9: date-day-fixed rejects a first byte that is a
/// non-digit, non-space character (e.g., 'X').
#[test]
fn validate_datetime_rejects_non_digit_day() {
    let result = validate_append_datetime("X1-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day 'X1' must be rejected  -  first byte 'X' is not valid \
         per RFC 3501 Section 9 date-day-fixed"
    );
}

// -----------------------------------------------------------------------
// date-day-fixed fallthrough (RFC 3501 Section 9)
// -----------------------------------------------------------------------

/// RFC 3501 Section 9: date-day-fixed = (SP DIGIT) / 2DIGIT.
/// A day whose first byte is outside ' ', '0'-'3' must be rejected
/// (the `_ => false` fallthrough in the day validator).
#[test]
fn validate_datetime_rejects_invalid_day_high_digit() {
    // Day "40"  -  first byte '4' falls into the _ => false branch.
    let result = validate_append_datetime("40-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day '40' must be rejected  -  first byte '4' is not in SP/'0'-'3' \
         per RFC 3501 Section 9 date-day-fixed"
    );
}

/// RFC 3501 Section 9: the validator must reject impossible calendar dates
/// where the day exceeds the maximum for the given month.  February is
/// capped at 29 (we accept Feb 29 universally since a full leap-year check
/// would require parsing the 4-digit year; this is a practical compromise).
/// 30-day months (Apr, Jun, Sep, Nov) must reject day 31.
#[test]
fn spec_audit_day_month_cross_check() {
    // Feb 31  -  impossible, must be rejected.
    assert!(
        validate_append_datetime("31-Feb-2024 00:00:00 +0000").is_err(),
        "31-Feb must be rejected  -  February has at most 29 days (RFC 3501 Section 9)"
    );

    // Feb 30  -  impossible, must be rejected.
    assert!(
        validate_append_datetime("30-Feb-2024 00:00:00 +0000").is_err(),
        "30-Feb must be rejected  -  February has at most 29 days (RFC 3501 Section 9)"
    );

    // Feb 29 in a leap year  -  must be accepted.
    assert!(
        validate_append_datetime("29-Feb-2024 00:00:00 +0000").is_ok(),
        "29-Feb-2024 must be accepted  -  2024 is a leap year (RFC 3501 Section 9)"
    );

    // Feb 29 in a non-leap year  -  must be rejected (Postel's law: be
    // conservative in what you send).
    assert!(
        validate_append_datetime("29-Feb-2023 00:00:00 +0000").is_err(),
        "29-Feb-2023 must be rejected  -  2023 is not a leap year (RFC 3501 Section 9)"
    );

    // Feb 29 in a century year divisible by 400  -  leap year.
    assert!(
        validate_append_datetime("29-Feb-2000 00:00:00 +0000").is_ok(),
        "29-Feb-2000 must be accepted  -  2000 is a leap year (RFC 3501 Section 9)"
    );

    // Feb 29 in a century year NOT divisible by 400  -  not a leap year.
    assert!(
        validate_append_datetime("29-Feb-1900 00:00:00 +0000").is_err(),
        "29-Feb-1900 must be rejected  -  1900 is not a leap year (RFC 3501 Section 9)"
    );

    // Apr 31  -  impossible, must be rejected (April has 30 days).
    assert!(
        validate_append_datetime("31-Apr-2024 00:00:00 +0000").is_err(),
        "31-Apr must be rejected  -  April has at most 30 days (RFC 3501 Section 9)"
    );

    // Jun 31  -  impossible, must be rejected (June has 30 days).
    assert!(
        validate_append_datetime("31-Jun-2024 00:00:00 +0000").is_err(),
        "31-Jun must be rejected  -  June has at most 30 days (RFC 3501 Section 9)"
    );

    // Apr 30  -  valid, must be accepted.
    assert!(
        validate_append_datetime("30-Apr-2024 00:00:00 +0000").is_ok(),
        "30-Apr must be accepted  -  April has 30 days (RFC 3501 Section 9)"
    );

    // Jan 31  -  valid, must be accepted.
    assert!(
        validate_append_datetime("31-Jan-2024 00:00:00 +0000").is_ok(),
        "31-Jan must be accepted  -  January has 31 days (RFC 3501 Section 9)"
    );
}

/// RFC 3501 Section 9: the validator must reject semantically invalid time
/// components (hours > 23, minutes > 59, seconds > 60) and accept valid
/// boundary values including leap seconds (60) per RFC 5322 Section 3.3.
#[test]
fn spec_audit_time_range_validation() {
    // Invalid hour: 25 exceeds 00-23 range.
    assert!(
        validate_append_datetime("01-Jan-2024 25:00:00 +0000").is_err(),
        "hour 25 must be rejected  -  valid range is 00-23 (RFC 3501 Section 9)"
    );

    // Invalid minute: 61 exceeds 00-59 range.
    assert!(
        validate_append_datetime("01-Jan-2024 12:61:00 +0000").is_err(),
        "minute 61 must be rejected  -  valid range is 00-59 (RFC 3501 Section 9)"
    );

    // Invalid second: 61 exceeds 00-60 range.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:61 +0000").is_err(),
        "second 61 must be rejected  -  valid range is 00-60 (RFC 3501 Section 9)"
    );

    // Valid leap second: 60 is permitted per RFC 5322 Section 3.3.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:60 +0000").is_ok(),
        "second 60 (leap second) must be accepted per RFC 5322 Section 3.3"
    );

    // Maximum valid time: 23:59:59.
    assert!(
        validate_append_datetime("01-Jan-2024 23:59:59 +0000").is_ok(),
        "23:59:59 is the maximum valid non-leap-second time"
    );

    // Minimum valid time: 00:00:00.
    assert!(
        validate_append_datetime("01-Jan-2024 00:00:00 +0000").is_ok(),
        "00:00:00 is the minimum valid time"
    );
}
