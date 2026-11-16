use super::*;
use crate::types::Addressing;

#[test]
fn encode_uid_search() {
    let mut buf = BytesMut::new();
    let cmd = Command::Search {
        addressing: Addressing::Uids,
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID SEARCH ALL\r\n");
}

#[test]
fn encode_search() {
    let mut buf = BytesMut::new();
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: "UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH UNSEEN\r\n");
}

/// RFC 3501 Sections 4.3 and 6.4.4: SEARCH keys such as BODY and TEXT
/// take `astring`, and `astring` may be a literal. The encoder must
/// therefore allow a literal-bearing criteria string.
#[test]
fn encode_search_allows_literal_criteria() {
    let mut buf = BytesMut::new();
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: "BODY {12}\r\nhello MODSEQ".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "SEARCH criteria literals must be accepted per RFC 3501 Sections 4.3 and 6.4.4: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 SEARCH BODY {12}\r\nhello MODSEQ\r\n");
}

#[test]
fn encode_search_save() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchSave {
        addressing: Addressing::Sequences,
        criteria: "UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN (SAVE) UNSEEN\r\n");
}

#[test]
fn encode_uid_search_save() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchSave {
        addressing: Addressing::Uids,
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID SEARCH RETURN (SAVE) ALL\r\n");
}

#[test]
fn encode_search_return_min_max_count() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        addressing: Addressing::Sequences,
        criteria: "UNSEEN".into(),
        return_opts: vec!["MIN".into(), "MAX".into(), "COUNT".into()],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN (MIN MAX COUNT) UNSEEN\r\n");
}

#[test]
fn encode_uid_search_return_all() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        addressing: Addressing::Uids,
        criteria: "ALL".into(),
        return_opts: vec!["ALL".into(), "COUNT".into()],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID SEARCH RETURN (ALL COUNT) ALL\r\n");
}

#[test]
fn encode_search_return_empty_opts() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        addressing: Addressing::Sequences,
        criteria: "FLAGGED".into(),
        return_opts: vec![],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN () FLAGGED\r\n");
}

/// RFC 4731 Section 3.2: RETURN options are space-delimited command
/// tokens, so the encoder must not preserve caller-supplied outer
/// whitespace around each option atom.
#[test]
fn encode_search_return_trims_outer_option_whitespace() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        addressing: Addressing::Sequences,
        criteria: "UNSEEN".into(),
        return_opts: vec![" MIN ".into(), "\tCOUNT\t".into()],
    };

    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN (MIN COUNT) UNSEEN\r\n");
}

/// RFC 4731 Section 3.2 defines each RETURN element as a
/// `search-return-opt` token. An element containing embedded
/// whitespace would expand into multiple wire options and must be
/// rejected instead of being serialized verbatim.
#[test]
fn encode_search_return_rejects_embedded_whitespace_in_option() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        addressing: Addressing::Sequences,
        criteria: "UNSEEN".into(),
        return_opts: vec!["MIN COUNT".into()],
    };

    let err = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect_err("SEARCH RETURN option with embedded whitespace must be rejected");

    assert!(
        matches!(err, crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(ref message)) if message.contains("SEARCH RETURN option")),
        "expected Protocol error mentioning SEARCH RETURN option, got {err:?}"
    );
}

// --- THREAD (RFC 5256) encoder tests ---

#[test]
fn encode_thread_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        addressing: Addressing::Sequences,
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 THREAD REFERENCES UTF-8 ALL\r\n");
}

/// RFC 5256 Sections 3 and 5: THREAD reuses IMAP SEARCH criteria, so its
/// criteria argument must permit IMAP literals too.
#[test]
fn encode_thread_allows_literal_criteria() {
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        addressing: Addressing::Sequences,
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "BODY {12}\r\nhello MODSEQ".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "THREAD criteria literals must be accepted per RFC 3501 Section 4.3 and RFC 5256 Section 3: {result:?}"
    );
    assert_eq!(
        &buf[..],
        b"A001 THREAD REFERENCES UTF-8 BODY {12}\r\nhello MODSEQ\r\n"
    );
}

#[test]
fn encode_uid_thread_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        addressing: Addressing::Uids,
        algorithm: "ORDEREDSUBJECT".into(),
        charset: "US-ASCII".into(),
        criteria: "SINCE 1-Jan-2025".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID THREAD ORDEREDSUBJECT US-ASCII SINCE 1-Jan-2025\r\n"
    );
}

// --- SORT (RFC 5256) encoder tests ---

#[test]
fn encode_sort_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        addressing: Addressing::Sequences,
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SORT (DATE) UTF-8 ALL\r\n");
}

#[test]
fn encode_uid_sort_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        addressing: Addressing::Uids,
        sort_criteria: "SUBJECT".into(),
        charset: "US-ASCII".into(),
        criteria: "SINCE 1-Jan-2025".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID SORT (SUBJECT) US-ASCII SINCE 1-Jan-2025\r\n"
    );
}

/// RFC 5256 Section 5: `charset = atom / quoted`.
/// Quoted charsets are legal for THREAD and must not be rejected.
#[test]
fn encode_thread_allows_quoted_charset() {
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        addressing: Addressing::Sequences,
        algorithm: "REFERENCES".into(),
        charset: "\"UTF-8\"".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect("THREAD must accept quoted charset per RFC 5256 Section 5");
    assert_eq!(&buf[..], b"A001 THREAD REFERENCES \"UTF-8\" ALL\r\n");
}

/// RFC 5256 Section 5: `charset = atom / quoted`.
/// Quoted charsets are legal for SORT and must not be rejected.
#[test]
fn encode_sort_allows_quoted_charset() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        addressing: Addressing::Sequences,
        sort_criteria: "DATE".into(),
        charset: "\"UTF-8\"".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect("SORT must accept quoted charset per RFC 5256 Section 5");
    assert_eq!(&buf[..], b"A001 SORT (DATE) \"UTF-8\" ALL\r\n");
}

#[test]
fn sort_command_uses_sort_criteria_field() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        addressing: Addressing::Sequences,
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A1", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "A1 SORT (DATE) UTF-8 ALL\r\n",
        "SORT must encode sort_criteria in parentheses (RFC 5256 Section 2)"
    );
}

// --- THREAD/SORT atom validation (RFC 5256 Sections 2-3, RFC 3501 Section 9) ---

#[test]
fn encode_thread_rejects_invalid_algorithm() {
    // RFC 5256 Section 3: thread-alg is an atom (RFC 3501 Section 9).
    // An algorithm containing spaces is not a valid atom and must be rejected.
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        addressing: Addressing::Sequences,
        algorithm: "ORDEREDSUBJECT INVALID".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "THREAD algorithm with spaces must be rejected (RFC 3501 Section 9: atom)"
    );
}

#[test]
fn encode_sort_rejects_invalid_charset() {
    // RFC 5256 Section 5: charset = atom / quoted. Parentheses are
    // atom-specials, so an unquoted charset containing them must be rejected.
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        addressing: Addressing::Sequences,
        sort_criteria: "DATE".into(),
        charset: "UTF(8)".into(),
        criteria: "ALL".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SORT charset with parens must be rejected (RFC 3501 Section 9: atom)"
    );
}

// ===== Audit finding #8: SORT/SETQUOTA commands encode correctly =====

/// The SORT command encoder exists internally even though the public
/// API does not expose it. Verify encoding is correct so that when
/// finding #8 is fixed (public API exposure), the wire format is ready.
///
/// Note: the encoder wraps the `sort_criteria` field in parentheses, so
/// the `sort_criteria` field should contain just the sort keys (e.g. `"DATE"`),
/// not the parens themselves.
#[test]
fn audit_finding8_sort_command_encodes_correctly() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        addressing: Addressing::Sequences,
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 SORT (DATE) UTF-8 ALL\r\n",
        "SORT command should encode correctly (RFC 5256 Section 2)"
    );
}

/// UID SORT command encoding.
#[test]
fn audit_finding8_uid_sort_command_encodes_correctly() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        addressing: Addressing::Uids,
        sort_criteria: "REVERSE DATE".into(),
        charset: "UTF-8".into(),
        criteria: "SINCE 1-Jan-2024".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 UID SORT (REVERSE DATE) UTF-8 SINCE 1-Jan-2024\r\n",
        "UID SORT command should encode correctly (RFC 5256 Section 2)"
    );
}

/// A caller's synchronizing literal is re-emitted through the writer: under
/// LITERAL+ it takes the `+` form and needs no continuation, as the patcher
/// used to make it.
#[test]
fn criteria_literal_takes_the_live_marker_form() {
    let encoded = uid_search(
        "SUBJECT {5}\r\nhello",
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(encoded.segments().len(), 1);
    assert_eq!(
        encoded.to_vec(),
        b"A001 UID SEARCH SUBJECT {5+}\r\nhello\r\n"
    );
}

/// Without a literal extension the caller's literal is a real segment
/// boundary: the sender waits for `+` before its payload.
#[test]
fn criteria_literal_is_a_segment_boundary_when_synchronizing() {
    let encoded = uid_search("SUBJECT {5}\r\nhello", &default_opts()).unwrap();
    assert_eq!(
        segment_bytes(&encoded),
        vec![
            b"A001 UID SEARCH SUBJECT {5}\r\n".to_vec(),
            b"hello\r\n".to_vec()
        ]
    );
}

/// Marker-shaped bytes inside a caller literal's payload stay payload: the
/// walk skips the payload by its declared length.
#[test]
fn criteria_literal_payload_holding_a_marker_is_not_framing() {
    let encoded = uid_search("SUBJECT {7}\r\n{9}\r\nab", &default_opts()).unwrap();
    assert_eq!(
        segment_bytes(&encoded),
        vec![
            b"A001 UID SEARCH SUBJECT {7}\r\n".to_vec(),
            b"{9}\r\nab\r\n".to_vec()
        ]
    );
}

/// A caller literal8 is re-emitted as literal8, so the writer applies the
/// literal8 rule: `~{1+}` the connection may not send non-synchronizing
/// loses its `+` and becomes a real boundary, rather than being written with
/// no grant.
#[test]
fn criteria_literal8_follows_the_literal8_rule() {
    let no_binary = opts(LiteralMode::LiteralPlus, false);
    let encoded = uid_search("BODY ~{1+}\r\nx", &no_binary).unwrap();
    assert_eq!(
        segment_bytes(&encoded),
        vec![b"A001 UID SEARCH BODY ~{1}\r\n".to_vec(), b"x\r\n".to_vec()]
    );

    let mut binary = opts(LiteralMode::LiteralPlus, false);
    std::sync::Arc::make_mut(&mut binary.session)
        .capabilities
        .push(Capability::Binary);
    let encoded = uid_search("BODY ~{1}\r\nx", &binary).unwrap();
    assert_eq!(encoded.to_vec(), b"A001 UID SEARCH BODY ~{1+}\r\nx\r\n");
}

/// A count written with leading zeros is re-emitted canonically.
#[test]
fn criteria_literal_count_is_written_canonically() {
    let encoded = uid_search(
        "SUBJECT {0001}\r\nx",
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(encoded.to_vec(), b"A001 UID SEARCH SUBJECT {1+}\r\nx\r\n");
}

/// The validator judges the caller's ORIGINAL marker: a `{n+}` the connection
/// cannot take is refused, never silently rewritten to `{n}`.
#[test]
fn criteria_non_synchronizing_literal_without_an_extension_is_refused() {
    let err = uid_search("SUBJECT {5+}\r\nhello", &default_opts()).unwrap_err();
    assert!(
        matches!(
            err,
            crate::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(_))
        ),
        "{err:?}"
    );
}

// -----------------------------------------------------------------------
// CRLF injection prevention (RFC 3501 Section 2.2)
//
// Embedding CR/LF in unquoted parameters would terminate the current
// command prematurely and allow injection of arbitrary IMAP commands.
// These tests verify that every raw-string parameter that is directly
// embedded into a command is validated for CRLF.
// -----------------------------------------------------------------------

/// SEARCH criteria containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_search_criteria_rejected() {
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SEARCH criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("CR or LF"),
        "error message should mention CR or LF: {err_msg}"
    );
}

/// UID SEARCH criteria containing CRLF must be rejected.
#[test]
fn crlf_injection_uid_search_criteria_rejected() {
    let cmd = Command::Search {
        addressing: Addressing::Uids,
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "UID SEARCH criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// SEARCH RETURN options containing CRLF must be rejected.
#[test]
fn crlf_injection_search_return_opts_rejected() {
    let cmd = Command::SearchReturn {
        addressing: Addressing::Sequences,
        criteria: "ALL".into(),
        return_opts: vec!["MIN\r\nA002 DELETE INBOX".into()],
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SEARCH RETURN option with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// THREAD criteria containing CRLF must be rejected.
#[test]
fn crlf_injection_thread_criteria_rejected() {
    let cmd = Command::Thread {
        addressing: Addressing::Sequences,
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "THREAD criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// SORT criteria containing CRLF must be rejected.
#[test]
fn crlf_injection_sort_criteria_rejected() {
    let cmd = Command::Sort {
        addressing: Addressing::Sequences,
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SORT criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// Normal SEARCH criteria without CRLF must succeed.
#[test]
fn crlf_injection_normal_search_succeeds() {
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: "ALL".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal SEARCH criteria must succeed: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 SEARCH ALL\r\n");
}

/// RFC 7888 Section 3: a `+` literal marker is only valid when the
/// server advertised LITERAL+ / LITERAL- (or `IMAP4rev2` semantics selected
/// `LiteralMinus`). SEARCH criteria must therefore reject `{N+}` in plain
/// synchronizing-literal mode.
#[test]
fn search_rejects_non_synchronizing_literal_without_extension() {
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: "TEXT {3+}\r\nfoo".into(),
    };
    let mut buf = BytesMut::new();
    let result = admit_and_encode_to_buf(&mut buf, "A001", &cmd, &default_opts());
    let err = result.expect_err(
        "SEARCH must reject non-synchronizing literals without negotiated \
         LITERAL+/LITERAL- support (RFC 7888 Section 3)",
    );
    // The refusal must arrive as a capability gap, not be flattened into
    // malformed input.
    assert!(
        matches!(
            err,
            crate::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(_))
        ),
        "got {err:?}"
    );
}

/// RFC 7888 Section 3: once LITERAL+ is negotiated, SEARCH criteria may
/// include non-synchronizing literals.
#[test]
fn search_allows_non_synchronizing_literal_with_literal_plus() {
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: "TEXT {3+}\r\nfoo".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralPlus, false),
    );
    assert!(
        result.is_ok(),
        "SEARCH should allow non-synchronizing literals once LITERAL+ is active: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 SEARCH TEXT {3+}\r\nfoo\r\n");
}

/// RFC 3501 Section 6.4.4: SEARCH requires one or more search keys.
#[test]
fn search_rejects_empty_criteria() {
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: String::new().into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty SEARCH criteria must be rejected (RFC 3501 Section 6.4.4)"
    );
}

/// RFC 5256 Section 3 / formal syntax Section 6: THREAD requires one or
/// more search criteria after the charset.
#[test]
fn thread_rejects_empty_criteria() {
    let cmd = Command::Thread {
        addressing: Addressing::Sequences,
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "   ".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty THREAD criteria must be rejected (RFC 5256 Section 6)"
    );
}

/// RFC 5256 Section 2 / formal syntax Section 6: SORT requires one or
/// more search criteria after the charset.
#[test]
fn sort_rejects_empty_criteria() {
    let cmd = Command::Sort {
        addressing: Addressing::Sequences,
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: String::new().into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty SORT criteria must be rejected (RFC 5256 Section 6)"
    );
}

/// RFC 7888 Section 5 / RFC 9051 Section 4.3: in LITERAL- style mode,
/// non-synchronizing literals must be 4096 octets or smaller. THREAD and
/// SORT reuse SEARCH criteria, so the same size cap applies there too.
#[test]
fn thread_rejects_oversized_non_synchronizing_literal_in_literal_minus_mode() {
    let oversized = "a".repeat(4097);
    let cmd = Command::Thread {
        addressing: Addressing::Sequences,
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: format!("TEXT {{4097+}}\r\n{oversized}").into(),
    };
    let mut buf = BytesMut::new();
    let result = admit_and_encode_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralMinus, false),
    );
    let err = result.expect_err(
        "THREAD must reject non-synchronizing literals larger than 4096 octets \
         in LITERAL- mode (RFC 7888 Section 5 / RFC 9051 Section 4.3)",
    );
    assert!(
        matches!(
            err,
            crate::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(_))
        ),
        "LITERAL- without LITERAL+ is a capability gap; got {err:?}"
    );
}

/// RFC 6855 Section 3: once UTF8=ACCEPT is active, clients MUST NOT send
/// SEARCH with a leading CHARSET specification.
#[test]
fn encode_search_rejects_charset_when_utf8_enabled() {
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria: "CHARSET UTF-8 ALL".into(),
    };
    let result = encode_command("A001", &cmd, &opts(LiteralMode::Synchronizing, true));

    assert!(
        matches!(result, Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(ref msg))) if msg.contains("RFC 6855 Section 3")),
        "SEARCH with CHARSET must be rejected when UTF8=ACCEPT is active \
         (RFC 6855 Section 3); got: {result:?}"
    );
}

#[test]
fn typed_search_string_uses_live_mode_and_structural_literals() {
    let criteria = crate::types::SearchCriteria::new()
        .subject("caf\u{e9}")
        .unwrap();
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria,
    };
    let ascii = encode_command("A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        ascii.to_vec(),
        b"A001 SEARCH SUBJECT {5}\r\ncaf\xc3\xa9\r\n"
    );
    let utf8 = encode_command("A001", &cmd, &opts(LiteralMode::Synchronizing, true)).unwrap();
    assert_eq!(utf8.to_vec(), b"A001 SEARCH SUBJECT \"caf\xc3\xa9\"\r\n");
}

#[test]
fn typed_search_control_and_marker_bytes_remain_operand_data() {
    let criteria = crate::types::SearchCriteria::new()
        .body("x\r\n{3}\r\nabc")
        .unwrap();
    let cmd = Command::Search {
        addressing: Addressing::Sequences,
        criteria,
    };
    let wire = encode_command("A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        wire.to_vec(),
        b"A001 SEARCH BODY {11}\r\nx\r\n{3}\r\nabc\r\n"
    );
    assert_eq!(wire.segments().len(), 2);
}

#[test]
fn typed_search_capability_requirements_follow_keys_and_raw_declarations() {
    let mut options = default_opts();
    std::sync::Arc::make_mut(&mut options.session).capabilities = vec![Capability::Imap4Rev1];
    let body = crate::types::SearchCriteria::new()
        .body("MODSEQ OLDER $")
        .unwrap();
    let criteria = crate::types::SearchCriteria::new().not(&body);
    assert!(
        admit_and_encode(
            "A001",
            &Command::Search {
                addressing: Addressing::Sequences,
                criteria
            },
            &options
        )
        .is_ok()
    );
    let criteria = crate::types::SearchCriteria::new().or(
        &body,
        &crate::types::SearchCriteria::new().mod_seq(4).unwrap(),
    );
    assert!(matches!(
        admit_and_encode(
            "A001",
            &Command::Search {
                addressing: Addressing::Sequences,
                criteria
            },
            &options
        ),
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
    let criteria =
        crate::types::SearchCriteria::raw("X-LOCAL value".to_owned(), vec![Capability::Within]);
    assert!(matches!(
        admit_and_encode(
            "A001",
            &Command::Search {
                addressing: Addressing::Sequences,
                criteria
            },
            &options
        ),
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
}
