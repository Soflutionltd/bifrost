use super::*;

// --- GETMETADATA / SETMETADATA (RFC 5464) encoder tests ---

#[test]
fn encode_getmetadata_single_entry() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into()],
        max_size: None,
        depth: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 GETMETADATA \"INBOX\" \"/private/comment\"\r\n"
    );
}

#[test]
fn encode_getmetadata_multiple_entries() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into(), "/shared/vendor/foo".into()],
        max_size: None,
        depth: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 GETMETADATA \"INBOX\" (\"/private/comment\" \"/shared/vendor/foo\")\r\n"
    );
}

#[test]
fn encode_setmetadata_with_values() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/comment".into(), Some(b"my comment".to_vec()))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" \"my comment\")\r\n"
    );
}

#[test]
fn encode_setmetadata_with_nil_delete() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/comment".into(), None)],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" NIL)\r\n"
    );
}

#[test]
fn encode_setmetadata_multiple_entries() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![
            ("/private/comment".into(), Some(b"hello".to_vec())),
            ("/shared/vendor/x".into(), None),
        ],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" \"hello\" \"/shared/vendor/x\" NIL)\r\n"
    );
}

/// RFC 5464 Section 3.2: entry names MUST NOT contain `*`, `%`,
/// consecutive slashes, a trailing slash, non-ASCII, or octets in the
/// range 0x00..=0x19. Invalid names should be rejected client-side.
#[test]
fn encode_setmetadata_invalid_entry_names_return_error() {
    for entry in [
        "comment",
        "/public/comment",
        "/shared/%bad",
        "/shared/*bad",
        "/shared//bad",
        "/shared/bad/",
        "/shared/r\u{00E9}sum\u{00E9}",
        "/shared/\u{0019}control",
    ] {
        let mut buf = BytesMut::new();
        let cmd = Command::SetMetadata {
            mailbox: MailboxName::new("INBOX").unwrap(),
            entries: vec![(entry.into(), Some(b"value".to_vec()))],
        };
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "SETMETADATA entry name {entry:?} must be rejected per RFC 5464 Section 3.2"
        );
    }
}

/// SETMETADATA values containing NUL bytes must be preserved,
/// and binary values must use literal8 syntax (`~{N}\r\n<data>`), not
/// standard literal (`{N}\r\n<data>`).
///
/// RFC 5464 Section 5: `value = nstring / literal8`
/// RFC 3516: `literal8 = "~{" number "}" CRLF *OCTET`  -  *OCTET includes NUL (%x00).
#[test]
fn encode_setmetadata_literal8_preserves_nul_bytes() {
    let mut buf = BytesMut::new();
    // Binary value with NUL bytes  -  must not be stripped.
    let value = b"\x00\x01\x02\x03".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // NUL bytes must be preserved in the output (not stripped).
    assert!(
        output.windows(4).any(|w| w == b"\x00\x01\x02\x03"),
        "NUL bytes must be preserved in SETMETADATA values per RFC 3516 literal8 *OCTET"
    );

    // Must use literal8 syntax ~{N}\r\n, not standard literal {N}\r\n.
    assert!(
        output.windows(5).any(|w| w == b"~{4}\r"),
        "Binary SETMETADATA value must use literal8 ~{{N}} syntax per RFC 5464 Section 5 / RFC 3516, got: {:?}",
        String::from_utf8_lossy(output)
    );
}

/// SETMETADATA values that are ASCII-safe should still use
/// quoted string form (nstring), not literal8.
///
/// RFC 5464 Section 5: `value = nstring / literal8`  -  nstring is preferred
/// when the data is quotable.
#[test]
fn encode_setmetadata_ascii_value_uses_quoted_form() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/comment".into(), Some(b"hello".to_vec()))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // ASCII-safe value should still be quoted, not literal8.
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" \"hello\")\r\n"
    );
}

/// SETMETADATA values with high bytes (>0x7F) must use classic IMAP
/// literal syntax, not literal8.
///
/// RFC 5464 Section 5: `value = nstring / literal8`
/// RFC 3501 Section 9 / RFC 9051 Section 9: `nstring = string / nil`
/// and `string` includes `literal = "{" number "}" CRLF *CHAR8`,
/// with `CHAR8 = %x01-ff`. High bytes are therefore valid in ordinary
/// literals; only NUL (`%x00`) requires literal8.
#[test]
fn encode_setmetadata_high_bytes_use_standard_literal() {
    let mut buf = BytesMut::new();
    let value = b"\x80\x81\xff".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // Must use classic literal syntax, not literal8.
    assert!(
        output.windows(4).any(|w| w == b"{3}\r"),
        "High-byte SETMETADATA value must use classic literal {{N}} syntax per RFC 3501 Section 9 / RFC 9051 Section 9, got: {:?}",
        String::from_utf8_lossy(output)
    );
    assert!(
        !output.windows(5).any(|w| w == b"~{3}\r"),
        "High-byte SETMETADATA value must not use literal8 when no NUL octet is present, got: {:?}",
        String::from_utf8_lossy(output)
    );

    // High bytes must be preserved.
    assert!(
        output.windows(3).any(|w| w == b"\x80\x81\xff"),
        "High bytes must be preserved in SETMETADATA literal values"
    );
}

// --- encode_metadata_value direct tests ---

/// ASCII data with backslash and double-quote must be escaped in quoted form.
/// RFC 3501 Section 9: quoted-specials = `"` / `\` are escaped with `\`.
#[test]
fn metadata_value_escapes_backslash_and_quote() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"a\\b\"c", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"a\\\\b\\\"c\"");
}

/// ASCII data without special chars uses quoted form.
#[test]
fn metadata_value_ascii_uses_quoted() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"hello", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"hello\"");
}

/// Data with CR/LF uses classic literal syntax because IMAP literals
/// allow all CHAR8 octets except NUL.
#[test]
fn metadata_value_crlf_uses_standard_literal() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"line1\r\nline2", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"{12}\r\nline1\r\nline2");
}

/// Data with NUL byte uses literal8 (NUL is preserved, not stripped).
/// RFC 3516: literal8 uses `*OCTET` (%x00-FF).
#[test]
fn metadata_value_nul_uses_literal8() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"\x00data", LiteralMode::Synchronizing);
    // NUL preserved; literal8 form.
    assert_eq!(&buf[..], b"~{5}\r\n\x00data");
}

/// Empty data uses empty quoted string.
#[test]
fn metadata_value_empty() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"\"");
}

/// Data with high bytes (>0x7F) uses classic literal syntax because
/// `literal` permits `*CHAR8` and `CHAR8 = %x01-ff`.
#[test]
fn metadata_value_high_bytes_uses_standard_literal() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"\x80\xff", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"{2}\r\n\x80\xff");
}

/// RFC 9051 Section 9: DEL (0x7F) is excluded from quoted CHAR, so it
/// must fall back to literal form. Classic literals still allow DEL via
/// `CHAR8 = %x01-ff`, so literal8 is not required unless a NUL is present.
#[test]
fn metadata_value_del_byte_uses_standard_literal() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"hello\x7Fworld", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"{"),
        "DEL byte (0x7F) must trigger classic literal encoding per RFC 3501/9051 literal CHAR8 rules, got: {:?}",
        std::str::from_utf8(&buf)
    );
}

/// ASCII control characters (0x01-0x1F) must trigger classic literal
/// encoding, not quoted form. `literal8` is still reserved for NUL.
#[test]
fn metadata_value_control_char_uses_standard_literal() {
    // TAB (0x09) is a control character that must NOT be quoted.
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"hello\tworld", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"{"),
        "TAB (0x09) must trigger classic literal encoding, got: {:?}",
        std::str::from_utf8(&buf)
    );

    // Printable ASCII (0x20-0x7E) should use quoted form.
    buf.clear();
    push_metadata_value(&mut buf, b"hello world", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"\""),
        "Printable ASCII should use quoted encoding, got: {:?}",
        std::str::from_utf8(&buf)
    );

    // NUL (0x00) must trigger literal8.
    buf.clear();
    push_metadata_value(&mut buf, b"\x00", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"~{"),
        "NUL (0x00) must trigger literal8 encoding, got: {:?}",
        std::str::from_utf8(&buf)
    );
}

/// Non-NUL METADATA literals should honor LITERAL+ / LITERAL- when the
/// caller requests non-synchronizing classic literals.
#[test]
fn encode_setmetadata_high_bytes_use_literal_plus_when_available() {
    let mut buf = BytesMut::new();
    let value = b"\x80\x81\xff".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/binary\" {3+}\r\n\x80\x81\xff)\r\n"
    );
}

// ===== Audit finding #7: METADATA encoding lacks MAXSIZE/DEPTH options =====

/// RFC 5464 Section 5 allows GETMETADATA with options:
/// `GETMETADATA [options] <mailbox> <entries>`
/// where options include `MAXSIZE n` and `DEPTH ("0"|"1"|"infinity")`.
///
/// Regression coverage for option encoding and wire ordering.
/// GETMETADATA without options remains backward compatible.
#[test]
fn audit_finding7_getmetadata_without_options() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        None,
        None,
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// GETMETADATA with MAXSIZE and DEPTH options (RFC 5464 Section 4.2.2).
#[test]
fn audit_finding7_getmetadata_with_maxsize_and_depth() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(1024),
        Some("infinity"),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA (MAXSIZE 1024 DEPTH infinity) \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// GETMETADATA with only MAXSIZE option.
#[test]
fn audit_finding7_getmetadata_with_maxsize_only() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(2048),
        None,
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA (MAXSIZE 2048) \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// GETMETADATA with only DEPTH option.
#[test]
fn audit_finding7_getmetadata_with_depth_only() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        None,
        Some("1"),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA (DEPTH 1) \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// RFC 5464 Section 5 plus verified errata 2785/2786 require
/// GETMETADATA options to precede the mailbox name.
#[test]
fn audit_getmetadata_options_before_mailbox_per_verified_errata() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(1024),
        Some("infinity"),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 GETMETADATA (MAXSIZE 1024 DEPTH infinity) \"INBOX\" \"/private/comment\"\r\n",
        "RFC 5464 Section 5 / errata 2785 and 2786: GETMETADATA options must precede the mailbox",
    );
}

/// SETMETADATA with empty entries must return an error (RFC 5464 Section 5).
///
/// RFC 5464 Section 5 ABNF: `entry-values = "(" entry *(SP entry) ")"`  -  at
/// least one entry is REQUIRED. An empty list produces the invalid `SETMETADATA "INBOX" ()\r\n`.
#[test]
fn encode_setmetadata_empty_entries_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![],
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SETMETADATA with empty entries must fail (RFC 5464 Section 5)"
    );
}

/// GETMETADATA with empty entries must return an error (RFC 5464 Section 4.2).
///
/// RFC 5464 Section 4.2 ABNF: `entries = entry / "(" entry *(SP entry) ")"`  -  at
/// least one entry is REQUIRED. An empty list produces the invalid `GETMETADATA "INBOX" ()\r\n`.
#[test]
fn encode_getmetadata_empty_entries_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![],
        max_size: None,
        depth: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "GETMETADATA with empty entries must fail (RFC 5464 Section 4.2)"
    );
}

/// GETMETADATA with invalid DEPTH must return an error (RFC 5464 Section 4.2.2).
///
/// RFC 5464 Section 4.2.2: `scope-opt = "DEPTH" SP ("0" / "1" / "infinity")`
/// Only "0", "1", and "infinity" are valid depth values.
#[test]
fn encode_getmetadata_invalid_depth_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into()],
        max_size: None,
        depth: Some("2".into()),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "GETMETADATA with invalid DEPTH \"2\" must fail (RFC 5464 Section 4.2.2)"
    );
}

/// RFC 5464 Section 3.2 entry-name rules also apply to GETMETADATA.
#[test]
fn encode_getmetadata_invalid_entry_names_return_error() {
    for entry in [
        "comment",
        "/public/comment",
        "/private/%bad",
        "/private/*bad",
        "/private//bad",
        "/private/bad/",
        "/private/r\u{00E9}sum\u{00E9}",
        "/private/\u{0019}control",
    ] {
        let mut buf = BytesMut::new();
        let cmd = Command::GetMetadata {
            mailbox: MailboxName::new("INBOX").unwrap(),
            entries: vec![entry.into()],
            max_size: None,
            depth: None,
        };
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "GETMETADATA entry name {entry:?} must be rejected per RFC 5464 Section 3.2"
        );
    }
}

/// GETMETADATA with valid DEPTH values should succeed.
#[test]
fn encode_getmetadata_valid_depth_values() {
    for depth in &["0", "1", "infinity"] {
        let mut buf = BytesMut::new();
        let cmd = Command::GetMetadata {
            mailbox: MailboxName::new("INBOX").unwrap(),
            entries: vec!["/private/comment".into()],
            max_size: None,
            depth: Some((*depth).to_string()),
        };
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_ok(),
            "GETMETADATA with valid DEPTH \"{depth}\" must succeed"
        );
    }
}

/// GETMETADATA with MAXSIZE option must place the option BEFORE
/// the mailbox per RFC 5464 Section 5 and verified errata 2785.
#[test]
fn regression_getmetadata_options_before_mailbox() {
    // Verified errata 2785 corrects the example to:
    //   C: a GETMETADATA (MAXSIZE 1024) "INBOX" (/shared/comment /private/comment)
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/shared/comment".into(), "/private/comment".into()],
        max_size: Some(1024),
        depth: None,
    };
    encode_command_to_buf(&mut buf, "a", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "a GETMETADATA (MAXSIZE 1024) \"INBOX\" (\"/shared/comment\" \"/private/comment\")\r\n",
        "GETMETADATA options must come BEFORE the mailbox (RFC 5464 Section 5 / errata 2785)"
    );
}

/// GETMETADATA with DEPTH option must place the option BEFORE
/// the mailbox per RFC 5464 Section 5 and verified errata 2786.
#[test]
fn regression_getmetadata_depth_before_mailbox() {
    // Verified errata 2786 corrects the example to:
    //   C: a GETMETADATA (DEPTH 1) "INBOX" (/private/filters/values)
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/filters/values".into()],
        max_size: None,
        depth: Some("1".into()),
    };
    encode_command_to_buf(&mut buf, "a", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "a GETMETADATA (DEPTH 1) \"INBOX\" \"/private/filters/values\"\r\n",
        "GETMETADATA DEPTH must come BEFORE the mailbox (RFC 5464 Section 5 / errata 2786)"
    );
}

/// GETMETADATA with both MAXSIZE and DEPTH options
/// must place them BEFORE the mailbox (RFC 5464 Section 5).
#[test]
fn regression_getmetadata_both_options_before_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into()],
        max_size: Some(2048),
        depth: Some("infinity".into()),
    };
    encode_command_to_buf(&mut buf, "a", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "a GETMETADATA (MAXSIZE 2048 DEPTH infinity) \"INBOX\" \"/private/comment\"\r\n",
        "GETMETADATA MAXSIZE + DEPTH must come BEFORE the mailbox (RFC 5464 Section 5 / errata 2785/2786)"
    );
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: MAXSIZE must fit in `number` (u32).
/// A `max_size` of 0 is valid (edge case but syntactically legal).
#[test]
fn getmetadata_maxsize_zero_accepted() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(0),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(result.is_ok(), "max_size 0 should be accepted");
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: `u32::MAX` is the largest valid `number`.
#[test]
fn getmetadata_maxsize_u32_max_accepted() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(u64::from(u32::MAX)),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(result.is_ok(), "max_size u32::MAX should be accepted");
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("MAXSIZE 4294967295"),
        "should contain u32::MAX value, got: {output}"
    );
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: `u32::MAX` + 1 exceeds `number` range.
#[test]
fn getmetadata_maxsize_u32_max_plus_one_rejected() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(u64::from(u32::MAX) + 1),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(
        result.is_err(),
        "max_size u32::MAX + 1 must be rejected per RFC 5464 Section 5 / RFC 3501 Section 9"
    );
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: `u64::MAX` far exceeds `number` range.
#[test]
fn getmetadata_maxsize_u64_max_rejected() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(u64::MAX),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(
        result.is_err(),
        "max_size u64::MAX must be rejected per RFC 5464 Section 5 / RFC 3501 Section 9"
    );
}

/// SETMETADATA with a binary value produces literal8 (`~{N}\r\n`). Without a
/// literal extension it is synchronizing, so the command must split at the
/// literal8 marker and the sender wait for `+` before the body.
///
/// RFC 3516 Section 4 / RFC 9051 Section 9:
///   `literal8 = "~{" number64 "}" CRLF *OCTET`
///
/// Regression: a send-side scanner once skipped `~{N}\r\n` markers, sending
/// the whole command as one segment without waiting for the continuation.
#[test]
fn regression_setmetadata_literal8_produces_two_segments_without_literal_plus() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(b"\x00\x01\x02".to_vec()))],
    };
    // literal_plus=false: literal8 must be synchronizing.
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        2,
        "SETMETADATA with literal8 and literal_plus=false must produce 2 segments \
         (RFC 3516 Section 4 / RFC 9051 Section 9: literal8 is synchronizing); \
         got {} segment(s): {:?}",
        segments.len(),
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );

    // Segment 0: command prefix ending with ~{3}\r\n
    let seg0 = &segments[0];
    assert!(
        seg0.ends_with(b"~{3}\r\n"),
        "first segment should end with literal8 marker ~{{3}}\\r\\n; \
         got: {:?}",
        String::from_utf8_lossy(seg0)
    );

    // Segment 1: literal body (3 bytes) + closing paren + CRLF
    assert!(
        segments[1].starts_with(b"\x00\x01\x02"),
        "second segment should start with the literal8 body"
    );
}
