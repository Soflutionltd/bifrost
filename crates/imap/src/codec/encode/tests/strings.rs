use super::*;

#[test]
fn encode_quoted_string() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"hello world", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"hello world\"");
}

#[test]
fn encode_literal_for_binary() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"line1\r\nline2", LiteralMode::Synchronizing);
    // Should use literal form because of CRLF.
    assert_eq!(&buf[..], b"{12}\r\nline1\r\nline2");
}

#[test]
fn encode_quoted_with_nul_is_refused() {
    assert_string_nul_rejected(b"has\0nul");
}

#[test]
fn encode_non_ascii_falls_back_to_literal() {
    // Per RFC 3501 Section 9, quoted-string characters must be in %x01-7F
    // (all ASCII) minus CR, LF, and NUL. Non-ASCII bytes (>0x7F) must use literal form.
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, "café".as_bytes(), LiteralMode::Synchronizing);
    // "café" is 5 bytes in UTF-8: 63 61 66 c3 a9
    assert_eq!(&buf[..], b"{5}\r\ncaf\xc3\xa9");
}

// ===== Negative / edge-case encoding tests =====

/// NUL byte in data is refused per RFC 3501 Section 9 (CHAR8 = %x01-ff).
#[test]
fn encode_data_with_nul_byte_is_refused() {
    assert_string_nul_rejected(b"hello\x00world");
}

/// Non-ASCII bytes force literal encoding.
#[test]
fn encode_data_with_non_ascii_uses_literal() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, "café".as_bytes(), LiteralMode::Synchronizing);
    // Non-ASCII (0x80+) is not quotable
    assert!(buf.starts_with(b"{5}\r\n"));
}

/// Empty data encodes as empty quoted string.
#[test]
fn encode_empty_data_quoted() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"\"");
}

/// A LOGIN credential containing CR/LF cannot escape its literal framing.
///
/// `password` is an `astring`, and RFC 9051 Section 4.3 lets a literal carry
/// any CHAR8. The injection this would have to become is a credential whose
/// bytes leave the literal early and are read as a command line; that is
/// impossible because the emitted octet count is taken from exactly the bytes
/// written, so the CRLF is payload the server consumes inside the literal.
#[test]
fn encode_login_crlf_credential_stays_inside_its_literal() {
    let pass = "pass\r\nword A002 DELETE INBOX\r\n";
    let mut buf = BytesMut::new();
    push_login(
        &mut buf,
        "A001",
        "user",
        pass,
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();

    let marker = format!("{{{}}}\r\n", pass.len());
    let output = String::from_utf8(buf.to_vec()).unwrap();
    assert_eq!(
        output,
        format!("A001 LOGIN \"user\" {marker}{pass}\r\n"),
        "the credential is framed as a counted literal, verbatim"
    );

    // The count is what makes this safe: everything after the marker up to
    // `pass.len()` octets is literal payload, so the embedded `A002 DELETE`
    // line is never at a command boundary.
    let body_start = output.find(&marker).unwrap() + marker.len();
    assert_eq!(&output[body_start..body_start + pass.len()], pass);
    assert_eq!(&output[body_start + pass.len()..], "\r\n");
}

// ===== DEL (0x7F) must trigger literal encoding =====
// RFC 9051 Section 9: CHAR = %x01-7E  -  DEL is excluded in IMAP4rev2.
// Using a literal for DEL is safe for both rev1 and rev2.

#[test]
fn del_byte_triggers_literal_encoding() {
    // RFC 9051 Section 9: CHAR = %x01-7E  -  DEL (0x7F) is excluded.
    // Strings containing DEL must use literal encoding, not quoted strings,
    // to be compatible with both IMAP4rev1 and IMAP4rev2.
    let mut buf = BytesMut::new();
    let data_with_del = b"hello\x7Fworld";
    push_quoted_or_literal(&mut buf, data_with_del, LiteralMode::Synchronizing);
    let result = std::str::from_utf8(&buf).unwrap();
    // Must be a literal (starts with {), not a quoted string (starts with ")
    assert!(
        result.starts_with('{'),
        "DEL byte (0x7F) must trigger literal encoding per RFC 9051 Section 9, got: {result}"
    );
}

// ===== Control chars (%x01-1F except CR/LF) must trigger literal =====
// While RFC 3501 Section 9 technically allows these as CHAR (%x01-7F),
// many servers reject control characters in quoted strings. Using literal
// form is the conservative ("be strict in what you send") approach and is
// safe for both IMAP4rev1 and IMAP4rev2.

#[test]
fn control_char_triggers_literal_encoding() {
    // Control characters (TAB, BEL, ESC, etc.) in data should produce
    // a literal rather than a quoted string for interoperability.
    for &byte in &[0x01u8, 0x07, 0x09, 0x1B, 0x1F] {
        let mut buf = BytesMut::new();
        let data = [
            b'h', b'e', b'l', b'l', b'o', byte, b'w', b'o', b'r', b'l', b'd',
        ];
        push_quoted_or_literal(&mut buf, &data, LiteralMode::Synchronizing);
        let result = std::str::from_utf8(&buf).unwrap();
        assert!(
            result.starts_with('{'),
            "Control char 0x{byte:02X} must trigger literal encoding for interoperability, got: {result}"
        );
    }
}

// ===== Spec audit: failing tests for known deviations =====

/// RFC 3501 Section 9 defines CHAR8 = %x01-ff, explicitly excluding NUL (%x00).
/// Rule (3): "The ASCII NUL... MUST NOT be used at any time."
/// The encoder refuses the command rather than rewriting its string.
#[test]
fn spec_audit_nul_bytes_are_refused() {
    assert_string_nul_rejected(b"has\0nul");
}

// --- RFC 6855 UTF8=ACCEPT quoted string encoding tests ---

/// RFC 6855 Section 3: ASCII-only input should produce quoted form regardless of `utf8_mode`.
#[test]
fn spec_audit_utf8_mode_ascii_produces_quoted() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, b"Brouillons", true, LiteralMode::Synchronizing);
    assert_eq!(
        &buf[..],
        b"\"Brouillons\"",
        "ASCII mailbox must be quoted in UTF-8 mode per RFC 6855 Section 3"
    );
}

/// RFC 6855 Section 3 / RFC 9051 Section 9: when UTF8=ACCEPT is active,
/// valid UTF-8 non-ASCII data must use quoted form, not literal.
#[test]
fn spec_audit_utf8_mode_non_ascii_produces_quoted() {
    let mut buf = BytesMut::new();
    // "日本語" = 9 bytes in UTF-8: e6 97 a5 e6 9c ac e8 aa 9e
    push_quoted_or_literal_utf8(
        &mut buf,
        "日本語".as_bytes(),
        true,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"\""),
        "UTF-8 mailbox name must be quoted when UTF8=ACCEPT is active \
         per RFC 6855 Section 3, got literal form instead"
    );
    // Verify the full output: quoted "日本語"
    let expected_bytes = b"\"\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e\"";
    assert_eq!(
        &buf[..],
        &expected_bytes[..],
        "UTF-8 mailbox name must be quoted per RFC 6855 Section 3"
    );
}

/// RFC 3501 Section 9: without UTF8=ACCEPT, non-ASCII bytes must use literal form.
#[test]
fn spec_audit_no_utf8_mode_non_ascii_produces_literal() {
    let mut buf = BytesMut::new();
    // "日本語" = 9 bytes in UTF-8
    push_quoted_or_literal_utf8(
        &mut buf,
        "日本語".as_bytes(),
        false,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"{"),
        "Non-ASCII mailbox name must use literal form when UTF8=ACCEPT is not active \
         per RFC 3501 Section 9, got quoted form instead"
    );
    assert_eq!(
        &buf[..],
        b"{9}\r\n\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e",
        "Non-ASCII mailbox name must use literal form per RFC 3501 Section 9"
    );
}

/// RFC 9051 Section 9: CR and LF are never quotable, even in UTF-8 mode.
/// TEXT-CHAR excludes CR (%x0D) and LF (%x0A).
#[test]
fn spec_audit_utf8_mode_crlf_produces_literal() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        b"line1\r\nline2",
        true,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"{"),
        "Data with CR/LF must use literal form even when UTF8=ACCEPT is active \
         per RFC 9051 Section 9 TEXT-CHAR definition"
    );
    assert_eq!(
        &buf[..],
        b"{12}\r\nline1\r\nline2",
        "CR/LF data must produce literal form per RFC 9051 Section 9"
    );
}

/// RFC 6855 Section 3: invalid UTF-8 sequences must fall back to literal even
/// when `utf8_mode` is true, since they cannot be represented in quoted strings.
#[test]
fn spec_audit_utf8_mode_invalid_utf8_produces_literal() {
    let mut buf = BytesMut::new();
    // Invalid UTF-8: lone continuation byte
    let invalid = &[0x80, 0x81, 0x82];
    push_quoted_or_literal_utf8(&mut buf, invalid, true, LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"{"),
        "Invalid UTF-8 must use literal form even when UTF8=ACCEPT is active \
         per RFC 6855 Section 3 (only valid UTF-8 is allowed in quoted strings)"
    );
}

/// RFC 9051 Section 9: CHAR = %x01-7E / UTF8-2 / UTF8-3 / UTF8-4.
/// DEL (0x7F) is a single-byte ASCII character NOT in %x01-7E, so it must
/// NOT appear in quoted strings even when UTF8=ACCEPT is active.
#[test]
fn spec_audit_utf8_mode_del_byte_produces_literal() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        b"hello\x7Fworld",
        true,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"{"),
        "DEL byte (0x7F) must trigger literal encoding in UTF-8 mode \
         per RFC 9051 Section 9 (CHAR = %x01-7E / UTF8-2/3/4), got: {:?}",
        std::str::from_utf8(&buf)
    );
}

// --- CommandWriter::string: backslash and double-quote escaping ---

/// RFC 3501 Section 9: quoted-specials = DQUOTE / "\"
/// Both backslash and double-quote must be escaped in quoted strings.
#[test]
fn encode_quoted_escapes_backslash_and_dquote() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"a\\b\"c", LiteralMode::Synchronizing);
    // Expected: "a\\b\"c"
    assert_eq!(&buf[..], b"\"a\\\\b\\\"c\"");
}

// --- CommandWriter::string: invalid NUL ---

/// RFC 3501 Section 9 forbids NUL in IMAP strings. Refusing the whole command
/// preserves the caller's mailbox or metadata identity.
#[test]
fn encode_string_rejects_nul_without_rewriting_the_command() {
    let mut w = CommandWriter::new(&opts(LiteralMode::Synchronizing, true));
    w.raw(b"A001 X ");
    w.string(b"hello\x00world", crate::types::wire::WireMode::Utf8);
    w.raw(b"\r\n");

    assert!(matches!(
        w.finish(),
        Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(message))) if message.contains("must not contain NUL")
    ));
}

/// RFC 3501 Section 9 / RFC 6855 Section 3: backslash and double-quote
/// must be escaped in quoted strings even in UTF-8 mode.
#[test]
fn encode_utf8_mode_escapes_backslash_and_dquote() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, b"a\\b\"c", true, LiteralMode::Synchronizing);
    assert_eq!(
        &buf[..],
        b"\"a\\\\b\\\"c\"",
        "backslash and double-quote must be escaped in UTF-8 mode"
    );
}

// ── RFC 9051 Section 9: literal8 must not use non-synchronizing `+` suffix ──

/// RFC 9051 Section 9: `literal8 = "~{" number64 "}" CRLF *OCTET`  -  no `["+"]`.
/// When UTF8=ACCEPT is active on a server with LITERAL+ but WITHOUT BINARY,
/// the encoder must produce `~{N}\r\n`, never `~{N+}\r\n` (RFC 7888 Section 6
/// makes the `+` form conditional on BINARY). The decoder already rejects
/// `~{N+}`  -  the encoder must match.
#[test]
fn literal8_must_not_use_non_sync_plus() {
    let msgs = [append_msg(&[], None, 100)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::LiteralPlus, true), // UTF8=ACCEPT, LITERAL+, no BINARY
    );
    let output = output.as_str();
    // Must NOT contain `+}`  -  literal8 doesn't allow non-synchronizing form.
    assert!(
        !output.contains("+}"),
        "literal8 must not contain non-synchronizing '+' suffix per RFC 9051 Section 9; got: {output}"
    );
    // Must contain the synchronizing literal8 form `~{100}\r\n`.
    assert!(
        output.contains("~{100}\r\n"),
        "literal8 must use synchronizing form ~{{100}} per RFC 9051 Section 9; got: {output}"
    );
}

/// RFC 7888 Section 6: BINARY together with LITERAL+ makes the `UTF8 (~{N+}`
/// form legal on `IMAP4rev1`, and the encoder uses it (the pre-refactor
/// handle-side path did too, by patching the header afterwards).
#[test]
fn utf8_wrapped_literal8_gets_plus_only_with_binary() {
    let msgs = [append_msg(&[], None, 100)];
    let mut o = append_opts(LiteralMode::LiteralPlus, true);
    std::sync::Arc::make_mut(&mut o.session)
        .capabilities
        .push(Capability::Binary);
    assert_eq!(
        append_wire("A001", "INBOX", &msgs, false, &o),
        "A001 APPEND \"INBOX\" UTF8 (~{100+}\r\n<100>)\r\n"
    );
}

/// RFC 9051 Section 9: when UTF8=ACCEPT is NOT active, LITERAL+ `{N+}` is fine.
/// This test ensures we don't regress the normal LITERAL+ path.
#[test]
fn regular_literal_plus_still_works_without_utf8() {
    let msgs = [append_msg(&[], None, 200)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::LiteralPlus, false),
    );
    let output = output.as_str();
    // Regular literal with LITERAL+ must contain `{200+}\r\n`.
    assert!(
        output.contains("{200+}\r\n"),
        "regular literal with LITERAL+ must use non-synchronizing form {{200+}}; got: {output}"
    );
}

/// RFC 9051 Section 9: when neither LITERAL+ nor UTF8=ACCEPT, produce `{N}\r\n`.
#[test]
fn synchronizing_literal_without_utf8() {
    let msgs = [append_msg(&[], None, 300)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false), // no literal extension
    );
    let output = output.as_str();
    // Must contain synchronizing literal `{300}\r\n`, no `+` or `~`.
    assert!(
        output.contains("{300}\r\n"),
        "synchronizing literal must use {{300}}; got: {output}"
    );
    assert!(
        !output.contains('~'),
        "non-UTF8 literal must not contain literal8 prefix '~'; got: {output}"
    );
}

// -----------------------------------------------------------------------
// Non-UTF8 output formatting via from_utf8_lossy
// -----------------------------------------------------------------------

/// Verify that `String::from_utf8_lossy` correctly handles encoded output
/// containing non-UTF8 bytes (e.g., high bytes in literal8 values).
/// This exercises the lossy conversion path used in debug/display formatting.
///
/// RFC 5464 Section 5 / RFC 3516: literal8 values can contain arbitrary
/// octets that are not valid UTF-8.
#[test]
fn from_utf8_lossy_handles_non_utf8_encoded_output() {
    let mut buf = BytesMut::new();
    // Encode a SETMETADATA command with high-byte (non-UTF8) value.
    let value = b"\x80\x81\xfe\xff".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // The output contains non-UTF8 bytes; from_utf8_lossy must produce
    // a valid string with U+FFFD replacement characters.
    let lossy = String::from_utf8_lossy(output);
    assert!(
        lossy.contains('\u{FFFD}'),
        "from_utf8_lossy must replace non-UTF8 bytes with U+FFFD; got: {lossy}"
    );
    // The lossy string must still contain the recognizable ASCII parts.
    assert!(
        lossy.contains("SETMETADATA"),
        "lossy output must preserve ASCII command keyword; got: {lossy}"
    );
    assert!(
        lossy.contains("INBOX"),
        "lossy output must preserve ASCII mailbox name; got: {lossy}"
    );
}

/// Verify `from_utf8_lossy` with NUL bytes in encoded output.
/// NUL bytes (0x00) are valid UTF-8 but exercise the binary path in
/// `encode_metadata_value` (literal8 syntax).
///
/// RFC 3516: literal8 = "~{" number "}" CRLF *OCTET
#[test]
fn from_utf8_lossy_with_nul_bytes_in_output() {
    let mut buf = BytesMut::new();
    let value = b"\x00\x00".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // NUL bytes are valid UTF-8, so from_utf8_lossy should not replace them.
    let lossy = String::from_utf8_lossy(output);
    assert!(
        lossy.contains("SETMETADATA"),
        "lossy output must preserve command keyword even with NUL bytes; got: {lossy}"
    );
    // NUL bytes are preserved (they are valid single-byte UTF-8 codepoints).
    let nul_count = output
        .iter()
        .fold(0usize, |acc, &b| acc + usize::from(b == 0x00));
    assert_eq!(
        nul_count, 2,
        "both NUL bytes must be preserved in the encoded output"
    );
}

// -----------------------------------------------------------------------
// LITERAL+ (RFC 7888) propagation through encode_command
// -----------------------------------------------------------------------

/// RFC 7888 Section 4: when the server advertises LITERAL+, the client
/// SHOULD use the non-synchronizing literal form `{N+}\r\n` instead of
/// the synchronizing `{N}\r\n`. A LOGIN password containing CRLF is a
/// legal `astring` that cannot be quoted, so it forces the literal form.
#[test]
fn encode_command_login_literal_plus() {
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "pass\r\nword".into(),
    };
    let mut buf = BytesMut::new();
    encode_command_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{10+}\r\n"),
        "with literal_plus=true, literal must use non-synchronizing form {{N+}}; got: {output}"
    );
}

/// When `literal_plus` is false, the encoder must use synchronizing
/// literals `{N}\r\n` (RFC 3501 Section 4.3).
#[test]
fn encode_command_login_synchronizing_literal() {
    let mut buf = BytesMut::new();
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "pass\r\nword".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{10}\r\n"),
        "with literal_plus=false, literal must use synchronizing form {{N}}; got: {output}"
    );
    assert!(
        !output.contains("{10+}"),
        "with literal_plus=false, literal must NOT use non-synchronizing form; got: {output}"
    );
}

// -----------------------------------------------------------------------
// Synchronizing literal segmentation (RFC 3501 Section 4.3)
// -----------------------------------------------------------------------

/// RFC 3501 Section 4.3 splits a synchronizing literal without relying on an
/// invalid LOGIN credential as the literal source.
#[test]
fn synchronizing_literal_splits_into_segments() {
    let encoded = x_command(LiteralMode::Synchronizing, &[b"pass\r\nword"]);
    let segments = segment_bytes(&encoded);

    // Must have exactly 2 segments: header+marker, then literal body+CRLF.
    assert_eq!(
        segments.len(),
        2,
        "a synchronizing literal must produce 2 segments \
         (RFC 3501 Section 4.3); got {} segment(s): {:?}",
        segments.len(),
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );

    // Segment 0: command prefix + literal marker `{10}\r\n`.
    let seg0 = std::str::from_utf8(&segments[0]).unwrap();
    assert!(
        seg0.starts_with("A001 X {10}\r\n"),
        "first segment should be the command prefix ending with {{10}}\\r\\n; \
         got: {seg0:?}"
    );

    // Segment 1: literal body (10 bytes = "pass\r\nword") + command CRLF.
    assert_eq!(
        &segments[1][..],
        b"pass\r\nword\r\n",
        "second segment should be the literal body followed by command CRLF"
    );
}

/// Non-synchronizing markers stay in one segment because they require no `+`
/// continuation pause (RFC 7888 Section 4).
#[test]
fn literal_plus_marker_stays_in_one_segment() {
    let encoded = x_command(LiteralMode::LiteralPlus, &[b"pass\r\nword"]);
    assert_eq!(encoded.to_vec(), b"A001 X {10+}\r\npass\r\nword\r\n");
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        1,
        "a non-synchronizing literal must be a single segment \
         (RFC 7888 Section 4); got {} segments",
        segments.len()
    );
}

/// RFC 3501 Section 4.3: each synchronizing literal requires a separate
/// continuation exchange.
#[test]
fn two_synchronizing_literals_produce_three_segments() {
    let encoded = x_command(LiteralMode::Synchronizing, &[b"us\r\ner", b"pass\r\nword"]);
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        3,
        "two synchronizing literals must produce 3 segments \
         (RFC 3501 Section 4.3); got {} segment(s): {:?}",
        segments.len(),
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );

    // Segment 0: command prefix + first literal marker.
    let seg0 = std::str::from_utf8(&segments[0]).unwrap();
    assert!(
        seg0.ends_with("{6}\r\n"),
        "first segment should end with the user literal marker {{6}}\\r\\n; \
         got: {seg0:?}"
    );

    // Segment 1: first literal body + space + second literal marker.
    let seg1_bytes = &segments[1][..];
    // First 6 bytes: literal body "us\r\ner"
    assert_eq!(
        &seg1_bytes[..6],
        b"us\r\ner",
        "second segment should start with the user literal body"
    );
    // Then " {10}\r\n"
    let seg1_rest = std::str::from_utf8(&seg1_bytes[6..]).unwrap();
    assert_eq!(
        seg1_rest, " {10}\r\n",
        "second segment should end with space + password literal marker"
    );

    // Segment 2: second literal body + command CRLF.
    assert_eq!(
        &segments[2][..],
        b"pass\r\nword\r\n",
        "third segment should be the password literal body + command CRLF"
    );
}

/// Commands without literals produce a single segment regardless of
/// `literal_plus` setting (RFC 3501 Section 4.3).
#[test]
fn encode_command_no_literal_single_segment() {
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "secret".into(),
    };
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        encoded.segments().len(),
        1,
        "command without literals should be a single segment"
    );
    assert_eq!(
        &encoded.to_vec()[..],
        b"A001 LOGIN \"alice\" \"secret\"\r\n"
    );
}

/// RFC 7888 Section 4: LITERAL+ makes every classic literal non-synchronizing.
#[test]
fn writer_literal_plus_marks_every_classic_literal() {
    let encoded = x_command(LiteralMode::LiteralPlus, &[b"alice\r\n", b"bob\r\n"]);
    assert_eq!(encoded.segments().len(), 1);
    assert_eq!(
        encoded.to_vec(),
        b"A001 X {7+}\r\nalice\r\n {5+}\r\nbob\r\n\r\n"
    );
}

/// RFC 7888 Section 5: LITERAL- is non-synchronizing up to 4096 octets and
/// synchronizing above.
#[test]
fn writer_literal_minus_boundary_is_4096_octets() {
    let at = vec![b'x'; 4096];
    let encoded = x_command(LiteralMode::LiteralMinus, &[&at]);
    assert_eq!(encoded.segments().len(), 1);
    assert!(encoded.to_vec().starts_with(b"A001 X {4096+}\r\n"));

    let over = vec![b'x'; 4097];
    let segments = segment_bytes(&x_command(LiteralMode::LiteralMinus, &[&over]));
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0], b"A001 X {4097}\r\n");
}

/// RFC 7888 Section 6 / RFC 9051 Section 9: a `~{n}` literal8 may take `+`
/// only with an advertised BINARY on a non-rev2 connection, and only where the
/// literal mode would allow it for a classic literal.
#[test]
fn writer_literal8_non_synchronizing_needs_binary_and_rev1() {
    let literal8 = |opts: &EncodeOptions| {
        written_segments(opts, |w| {
            w.raw(b"A001 X ");
            w.literal(b"abc", LiteralForm::Literal8);
            w.raw(b"\r\n");
        })
    };

    // LITERAL+ without BINARY: synchronizing, split at the marker.
    let no_binary = opts(LiteralMode::LiteralPlus, false);
    assert!(!no_binary.capabilities.contains(&Capability::Binary));
    assert_eq!(
        literal8(&no_binary),
        vec![b"A001 X ~{3}\r\n".to_vec(), b"abc\r\n".to_vec()]
    );

    // LITERAL+ with BINARY on rev1: non-synchronizing.
    let mut binary = opts(LiteralMode::LiteralPlus, false);
    std::sync::Arc::make_mut(&mut binary.session)
        .capabilities
        .push(Capability::Binary);
    assert_eq!(literal8(&binary), vec![b"A001 X ~{3+}\r\nabc\r\n".to_vec()]);

    // LITERAL- with BINARY: small literal8 is non-synchronizing too.
    let mut minus_binary = opts(LiteralMode::LiteralMinus, false);
    std::sync::Arc::make_mut(&mut minus_binary.session)
        .capabilities
        .push(Capability::Binary);
    assert_eq!(
        literal8(&minus_binary),
        vec![b"A001 X ~{3+}\r\nabc\r\n".to_vec()]
    );

    // BINARY on an active rev2 connection: rev2 literal8 has no `+` form.
    let rev2 = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::Utf8,
        literal_mode: LiteralMode::LiteralPlus,
        session: crate::types::profile::SessionCapabilities::new(
            vec![Capability::Imap4Rev2, Capability::Binary],
            Vec::new(),
        ),
    };
    assert_eq!(
        literal8(&rev2),
        vec![b"A001 X ~{3}\r\n".to_vec(), b"abc\r\n".to_vec()]
    );
}

/// A zero-length synchronizing literal still ends its segment on the marker.
#[test]
fn writer_zero_length_literal_is_its_own_boundary() {
    let segments = segment_bytes(&x_command(LiteralMode::Synchronizing, &[b""]));
    assert_eq!(segments, vec![b"A001 X {0}\r\n".to_vec(), b"\r\n".to_vec()]);
}

/// A payload handed over as `Bytes` is carried by reference: the chunk in the
/// encoded command is the caller's own allocation.
#[test]
fn writer_literal_bytes_carries_the_payload_by_reference() {
    let body = bytes::Bytes::from(vec![b'm'; 64]);
    let mut w = CommandWriter::new(&opts(LiteralMode::LiteralPlus, false));
    w.raw(b"A001 X ");
    w.literal_bytes(body.clone(), LiteralForm::Classic);
    w.raw(b"\r\n");
    let encoded = w.finish().expect("valid command");
    assert!(
        encoded
            .segments()
            .iter()
            .flatten()
            .any(|chunk| chunk.as_ptr() == body.as_ptr() && chunk.len() == body.len()),
        "the body must be the caller's allocation, not a copy"
    );
}

#[test]
fn out_of_range_literal_count_is_not_a_marker_for_the_shared_parser() {
    use crate::codec::literals::{LiteralMarker, literal_marker_at};

    assert!(matches!(
        literal_marker_at(b"{9223372036854775807}\r\n", 0),
        LiteralMarker::Counted {
            size: 9_223_372_036_854_775_807,
            ..
        }
    ));
    assert!(matches!(
        literal_marker_at(b"{9223372036854775808}\r\n", 0),
        LiteralMarker::CountOutOfRange {
            size: 9_223_372_036_854_775_808,
            ..
        }
    ));
    assert!(matches!(
        literal_marker_at(b"{18446744073709551616}\r\n", 0),
        LiteralMarker::NotAMarker
    ));
}

/// RFC 6855 Section 5: UTF8=ACCEPT does not extend LOGIN.
/// Non-ASCII credentials must be rejected; clients must use AUTHENTICATE.
#[test]
fn encode_login_rejects_utf8_credentials_even_when_utf8_enabled() {
    let cmd = Command::Login {
        user: "ユーザー".into(),
        pass: "пароль".into(),
    };
    let result = encode_command("A001", &cmd, &opts(LiteralMode::Synchronizing, true));

    assert!(
        matches!(result, Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(ref msg))) if msg.contains("RFC 6855 Section 5")),
        "LOGIN with non-ASCII credentials must be rejected even when UTF8=ACCEPT is active \
         (RFC 6855 Section 5); got: {result:?}"
    );
}

// -----------------------------------------------------------------------
// LITERAL- (RFC 7888 Section 5): size-limited non-synchronizing literals
// -----------------------------------------------------------------------

/// RFC 7888 Section 5: in LITERAL- mode, literals > 4096 bytes MUST use
/// synchronizing form `{N}\r\n`. Previously, the encoder had no concept
/// of LITERAL-  -  it could only produce `{N+}` for all literals or `{N}`
/// for all literals, with the connection layer post-processing the buffer.
/// This test verifies the encoder natively enforces the 4096-byte limit.
#[test]
fn literal_minus_large_literal_uses_synchronizing_form() {
    let large_data = format!("data\r\n{}", "x".repeat(4100));
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        large_data.as_bytes(),
        false,
        LiteralMode::LiteralMinus,
    );
    let output = std::str::from_utf8(&buf).unwrap();
    let expected_size = large_data.len();
    // RFC 7888 Section 5: literal > 4096 bytes MUST use synchronizing form.
    assert!(
        output.contains(&format!("{{{expected_size}}}\r\n")),
        "LITERAL- with literal > 4096 bytes must use synchronizing form \
         {{N}}\\r\\n (RFC 7888 Section 5); got: {output}"
    );
    assert!(
        !output.contains(&format!("{{{expected_size}+}}")),
        "LITERAL- with literal > 4096 bytes must NOT use non-synchronizing \
         form {{N+}} (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: in LITERAL- mode, literals <= 4096 bytes use
/// non-synchronizing form `{N+}\r\n`.
#[test]
fn literal_minus_small_literal_uses_non_synchronizing_form() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, b"pass\r\nword", false, LiteralMode::LiteralMinus);
    let output = std::str::from_utf8(&buf).unwrap();
    // RFC 7888 Section 5: literal <= 4096 bytes uses non-synchronizing form.
    assert!(
        output.contains("{10+}\r\n"),
        "LITERAL- with literal <= 4096 bytes must use non-synchronizing form \
         {{N+}}\\r\\n (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: boundary test  -  a literal of exactly 4096 bytes
/// should use non-synchronizing form in LITERAL- mode.
#[test]
fn literal_minus_boundary_4096_uses_non_synchronizing() {
    let data = format!("x\r\n{}", "a".repeat(4093));
    assert_eq!(data.len(), 4096);
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, data.as_bytes(), false, LiteralMode::LiteralMinus);
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{4096+}\r\n"),
        "LITERAL- with literal of exactly 4096 bytes must use non-synchronizing \
         form (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: boundary test  -  a literal of 4097 bytes MUST use
/// synchronizing form in LITERAL- mode.
#[test]
fn literal_minus_boundary_4097_uses_synchronizing() {
    let data = format!("x\r\n{}", "a".repeat(4094));
    assert_eq!(data.len(), 4097);
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, data.as_bytes(), false, LiteralMode::LiteralMinus);
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{4097}\r\n"),
        "LITERAL- with literal of 4097 bytes must use synchronizing form \
         (RFC 7888 Section 5); got: {output}"
    );
    assert!(
        !output.contains("{4097+}"),
        "LITERAL- with literal of 4097 bytes must NOT use non-synchronizing \
         form (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 4: LITERAL+ mode uses non-synchronizing `{N+}\r\n`
/// regardless of literal size  -  even for very large literals.
#[test]
fn literal_plus_large_literal_uses_non_synchronizing() {
    let large_data = format!("data\r\n{}", "x".repeat(10_000));
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        large_data.as_bytes(),
        false,
        LiteralMode::LiteralPlus,
    );
    let output = std::str::from_utf8(&buf).unwrap();
    let expected_size = large_data.len();
    assert!(
        output.contains(&format!("{{{expected_size}+}}\r\n")),
        "LITERAL+ must use non-synchronizing form {{N+}}\\r\\n for all sizes \
         (RFC 7888 Section 4); got: {output}"
    );
}

/// RFC 7888 Section 5: LITERAL- segment splitting  -  large literals must
/// produce synchronizing boundaries that split into multiple segments.
#[test]
fn literal_minus_large_literal_produces_segments() {
    let data = format!("data\r\n{}", "x".repeat(5000));
    let encoded = x_command(LiteralMode::LiteralMinus, &[data.as_bytes()]);
    let segments = segment_bytes(&encoded);
    // The large literal must produce a synchronizing boundary, resulting
    // in multiple segments (RFC 3501 Section 4.3).
    assert!(
        segments.len() > 1,
        "LITERAL- with literal > 4096 bytes must produce multiple segments \
         for synchronizing literal handling; got {} segment(s)",
        segments.len()
    );
}

/// RFC 7888 Section 5: LITERAL- with small literal produces a single
/// segment (no synchronizing boundary needed).
#[test]
fn literal_minus_small_literal_produces_single_segment() {
    let encoded = x_command(LiteralMode::LiteralMinus, &[b"pass\r\nword"]);
    assert_eq!(encoded.to_vec(), b"A001 X {10+}\r\npass\r\nword\r\n");
    let segments = segment_bytes(&encoded);
    assert_eq!(
        segments.len(),
        1,
        "LITERAL- with literal <= 4096 bytes must produce a single segment \
         (non-synchronizing, RFC 7888 Section 5); got {} segments",
        segments.len()
    );
}

// ========================================================================
// Adversarial / hostile-payload encoding
//
// These pin how the encoder behaves when a *command payload* happens to
// contain bytes that look like IMAP framing.
// ========================================================================

/// A LITERAL+ payload is opaque framing data. A sequence that resembles a
/// synchronizing marker inside the payload must not create another segment.
#[test]
fn literal_plus_body_containing_sync_marker_stays_in_one_segment() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        // 11 opaque octets. `{5}\r\n` is ordinary data here, not framing.
        entries: vec![("/private/x".into(), Some(b"a{5}\r\nbbbbb".to_vec()))],
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::LiteralPlus, false)).unwrap();
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        1,
        "the `{{5}}\\r\\n` inside the LITERAL+ body is data, not a literal \
         marker (RFC 7888 Section 4). Got: {:?}",
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        &segments[0][..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/x\" {11+}\r\na{5}\r\nbbbbb)\r\n"
    );
}

/// A literal-looking payload with an unrepresentable octet count is still
/// opaque LITERAL+ data. It must not be scanned, overflowed, or split.
#[test]
fn literal_plus_body_containing_overflow_marker_stays_in_one_segment() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![(
            "/private/x".into(),
            Some(b"{18446744073709551592}\r\n".to_vec()),
        )],
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::LiteralPlus, false)).unwrap();

    assert_eq!(encoded.segments().len(), 1);
}

/// A *synchronizing* literal payload is skipped correctly, so the same
/// hostile bytes are harmless under `LiteralMode::Synchronizing`. This is the
/// control case for the test above: it isolates the defect to the
/// non-synchronizing marker form rather than to literal payloads generally.
#[test]
fn sync_literal_body_containing_sync_marker_is_skipped() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/x".into(), Some(b"a{5}\r\nbbbbb".to_vec()))],
    };
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        2,
        "exactly one split, at the real `{{11}}\\r\\n` marker (RFC 3501 \
         Section 4.3); got: {:?}",
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );
    assert!(
        segments[0].ends_with(b"{11}\r\n"),
        "the split must be at the real marker; got: {:?}",
        String::from_utf8_lossy(&segments[0])
    );
    assert_eq!(
        &segments[1][..],
        b"a{5}\r\nbbbbb)\r\n",
        "the whole payload plus the command tail is one segment: the writer \
         records only the boundary of the literal it wrote"
    );
}

/// A `{digits}` group inside a payload that is *not* followed by CRLF is
/// payload, and the command stays one segment. The encoder never scans its
/// own output, so this pins that a value shaped like a marker cannot create a
/// boundary.
#[test]
fn brace_digits_not_abutting_crlf_is_not_a_literal_marker() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        // `{5}` appears, but is followed by `x`, not CRLF.
        entries: vec![("/private/x".into(), Some(b"a{5}x\r\nbb".to_vec()))],
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::LiteralPlus, false)).unwrap();
    assert_eq!(
        encoded.segments().len(),
        1,
        "`{{5}}x` is not a literal marker: `}}` does not abut the CRLF \
         (RFC 3501 Section 4.3)"
    );
}

/// `WireCommand`'s Debug prints the shape only: a command can carry a message
/// body or a credential literal, and the driver traces commands.
#[test]
fn wire_command_debug_never_prints_payload() {
    let encoded = x_command(LiteralMode::LiteralPlus, &[b"hunter2"]);
    let debug = format!("{encoded:?}");
    assert!(!debug.contains("hunter2"), "{debug}");
}

// -----------------------------------------------------------------------
// CommandWriter: the one place literal framing is decided
//
// Ported from the tests of the deleted send-path patchers
// (`patch_literals_to_plus_with_binary`, `patch_small_literals_to_plus_with_binary`,
// `find_literal_boundary`): the same rules, now pinned where they live.
// -----------------------------------------------------------------------

/// Marker-shaped bytes inside a literal payload are payload. The writer
/// records the boundary of the literal it writes and never scans anything, so
/// a synchronizing payload holding `{3}\r\n` still yields exactly one split.
#[test]
fn writer_payload_holding_a_marker_is_not_framing() {
    let segments = segment_bytes(&x_command(LiteralMode::Synchronizing, &[b"abc{3}\r\ndef"]));
    assert_eq!(
        segments,
        vec![b"A001 X {11}\r\n".to_vec(), b"abc{3}\r\ndef\r\n".to_vec()]
    );
}
