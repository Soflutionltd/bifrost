use super::*;

// ===== Primitive parser tests =====

#[test]
fn atom_valid() {
    let (rest, val) = atom(b"INBOX rest").unwrap();
    assert_eq!(val, b"INBOX");
    assert_eq!(rest, b" rest");
}

#[test]
fn atom_rejects_specials() {
    assert!(atom(b"(bad").is_err());
    assert!(atom(b")bad").is_err());
    assert!(atom(b"{bad").is_err());
    assert!(atom(b"\"bad").is_err());
}

#[test]
fn atom_empty_fails() {
    assert!(atom(b" oops").is_err());
}

#[test]
fn quoted_string_simple() {
    let (rest, val) = quoted_string(b"\"hello\" rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
}

#[test]
fn quoted_string_escapes() {
    let (_, val) = quoted_string(b"\"a\\\"b\\\\c\"").unwrap();
    assert_eq!(val, b"a\"b\\c");
}

#[test]
fn quoted_string_non_ascii() {
    let (_, val) = quoted_string(b"\"caf\xc3\xa9\"").unwrap();
    assert_eq!(val, b"caf\xc3\xa9");
}

#[test]
fn quoted_string_empty() {
    let (_, val) = quoted_string(b"\"\"").unwrap();
    assert!(val.is_empty());
}

#[test]
fn literal_simple() {
    let (rest, val) = literal(b"{5}\r\nhello rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
}

#[test]
fn literal_plus() {
    let (_, val) = literal(b"{3+}\r\nabc").unwrap();
    assert_eq!(val, b"abc");
}

#[test]
fn literal_zero_length() {
    let (_, val) = literal(b"{0}\r\n").unwrap();
    assert!(val.is_empty());
}

#[test]
fn nstring_nil() {
    let (_, val) = nstring(b"NIL").unwrap();
    assert!(val.is_none());
}

#[test]
fn nstring_nil_case_insensitive() {
    let (_, val) = nstring(b"nil").unwrap();
    assert!(val.is_none());
    let (_, val) = nstring(b"Nil").unwrap();
    assert!(val.is_none());
}

#[test]
fn nstring_string() {
    let (_, val) = nstring(b"\"hello\"").unwrap();
    assert_eq!(val.unwrap(), b"hello");
}

/// Regression: `nstring` must verify a token boundary follows "NIL"
/// so that atoms starting with "NIL" (e.g. "NILX") are not partially
/// consumed as the NIL token (RFC 3501 Section 9).
#[test]
fn nstring_nil_requires_token_boundary() {
    // "NILX" is an atom, not the NIL token followed by "X".
    // Without boundary checking, `tag_no_case(&b"NIL"[..])` greedily
    // matches "NIL" and leaves "X" as unparsed residue.
    // With the fix, the parser correctly rejects "NILX" as not
    // matching either NIL or a quoted/literal string.
    assert!(
        nstring(b"NILX").is_err(),
        "nstring must not partially match 'NIL' from 'NILX' \
             (RFC 3501 Section 9: atoms are delimited tokens)"
    );
}

/// `nstring` NIL followed by a valid delimiter must still parse.
#[test]
fn nstring_nil_with_delimiter() {
    let (rest, val) = nstring(b"NIL rest").unwrap();
    assert!(val.is_none());
    assert_eq!(rest, b" rest");

    let (rest, val) = nstring(b"NIL)").unwrap();
    assert!(val.is_none());
    assert_eq!(rest, b")");

    let (rest, val) = nstring(b"NIL\r\n").unwrap();
    assert!(val.is_none());
    assert_eq!(rest, b"\r\n");
}

/// Regression: `nil_token` must recognise all RFC 3501 Section 9 atom-specials
/// as valid token boundaries, not just SP/`)`/CR/`]`/EOF.
///
/// RFC 3501 Section 9 ABNF:
///   atom-specials = "(" / ")" / "{" / SP / CTL / list-wildcards /
///                   quoted-specials / resp-specials
///
/// `(` and `{` are atom-specials, so `NIL(` and `NIL{5}` are the NIL
/// token followed by another syntactic element.  Without these delimiters
/// in the peek set, `nil_token` rejects the input and the outer `alt`
/// (e.g., `nstring`) fails entirely.
#[test]
fn nil_token_open_paren_boundary() {
    // NIL immediately followed by `(`  -  the `(` is an atom-special
    // per RFC 3501 Section 9, so NIL is a complete token here.
    let (rest, val) = nstring(b"NIL(").unwrap();
    assert!(val.is_none(), "NIL( should parse NIL as None");
    assert_eq!(rest, b"(");
}

#[test]
fn nil_token_open_brace_boundary() {
    // NIL immediately followed by `{`  -  another atom-special.
    let (rest, val) = nstring(b"NIL{5}\r\nhello").unwrap();
    assert!(val.is_none(), "NIL{{ should parse NIL as None");
    assert_eq!(rest, b"{5}\r\nhello");
}

/// Regression: `nil_token` must recognise all CTL bytes (0x00-0x1F, 0x7F)
/// as valid token boundaries, not just CR and LF.
///
/// RFC 3501 Section 9:
///   atom-specials = "(" / ")" / "{" / SP / CTL / ...
///   CTL           = %x00-1F / %x7F
///
/// A TAB (0x09) after NIL is a CTL byte and therefore an atom-special,
/// so `NIL\t` must parse as the NIL token.
#[test]
fn nil_token_tab_boundary() {
    let (rest, val) = nstring(b"NIL\t").unwrap();
    assert!(val.is_none(), "NIL followed by TAB should parse as None");
    assert_eq!(rest, b"\t");
}

/// NUL (0x00) is a CTL byte and must terminate the NIL token.
#[test]
fn nil_token_nul_boundary() {
    let (rest, val) = nstring(b"NIL\x00").unwrap();
    assert!(val.is_none(), "NIL followed by NUL should parse as None");
    assert_eq!(rest, b"\x00");
}

/// DEL (0x7F) is a CTL byte and must terminate the NIL token.
#[test]
fn nil_token_del_boundary() {
    let (rest, val) = nstring(b"NIL\x7F").unwrap();
    assert!(val.is_none(), "NIL followed by DEL should parse as None");
    assert_eq!(rest, b"\x7F");
}

#[test]
fn number_valid() {
    let (_, val) = number(b"12345").unwrap();
    assert_eq!(val, 12345);
}

#[test]
fn number_overflow() {
    assert!(number(b"99999999999").is_err());
}

#[test]
fn number64_valid() {
    // RFC 9051 Section 4: number64 is 0..=i64::MAX (2^63-1)
    let (_, val) = number64(b"9223372036854775807").unwrap();
    assert_eq!(val, i64::MAX as u64);
}

// ===== RFC 2047 tests =====

#[test]
fn rfc2047_base64() {
    let result = decode_rfc2047(b"=?UTF-8?B?SGVsbG8gV29ybGQ=?=");
    assert_eq!(result, "Hello World");
}

#[test]
fn rfc2047_quoted_printable() {
    let result = decode_rfc2047(b"=?UTF-8?Q?Hello_World?=");
    assert_eq!(result, "Hello World");
}

#[test]
fn rfc2047_iso8859() {
    // "café" in ISO-8859-1 Q-encoding
    let result = decode_rfc2047(b"=?ISO-8859-1?Q?caf=E9?=");
    assert_eq!(result, "caf\u{e9}");
}

#[test]
fn rfc2047_consecutive() {
    // Whitespace between consecutive encoded words should be removed
    let result = decode_rfc2047(b"=?UTF-8?Q?Hello?= =?UTF-8?Q?_World?=");
    assert_eq!(result, "Hello World");
}

#[test]
fn rfc2047_mixed() {
    let result = decode_rfc2047(b"Re: =?UTF-8?B?SGVsbG8=?= there");
    assert_eq!(result, "Re: Hello there");
}

#[test]
fn rfc2047_plain_text() {
    let result = decode_rfc2047(b"no encoding here");
    assert_eq!(result, "no encoding here");
}

#[test]
fn rfc2047_garbage() {
    // Malformed  -  should pass through literally
    let result = decode_rfc2047(b"=?broken");
    assert_eq!(result, "=?broken");
}

/// RFC 2231 Section 5: encoded-word charset may include `*language` suffix.
#[test]
fn spec_audit_rfc2047_with_rfc2231_language_tag() {
    // =?charset*language?encoding?text?= per RFC 2231 Section 5
    let input = b"=?UTF-8*EN?Q?Hello_World?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Hello World",
        "RFC 2231 language tag in charset must be stripped"
    );
}

/// RFC 2231 Section 5: language tag with non-UTF-8 charset.
#[test]
fn spec_audit_rfc2047_with_rfc2231_language_tag_iso8859() {
    // ISO-8859-1 with language tag
    let input = b"=?ISO-8859-1*DE?Q?Gr=FC=DFe?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Grüße",
        "RFC 2231 language tag must work with non-UTF-8 charsets"
    );
}

/// RFC 2231 Section 5: language tag with Base64 encoding.
#[test]
fn spec_audit_rfc2047_with_rfc2231_language_tag_b_encoding() {
    // UTF-8*EN with B encoding
    let input = b"=?UTF-8*EN?B?SGVsbG8=?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Hello",
        "RFC 2231 language tag must work with B encoding"
    );
}

/// `decode_with_bom_removal` incorrectly strips a leading BOM
/// from RFC 2047 encoded-word payloads. Encoded words are header text
/// fragments (RFC 2047 Section 2), not standalone documents, so a leading
/// U+FEFF should be preserved as content (ZERO WIDTH NO-BREAK SPACE).
///
/// Test vector: UTF-16BE payload [0xFE, 0xFF, 0x00, 0x68, 0x00, 0x65,
/// 0x00, 0x6C, 0x00, 0x6C, 0x00, 0x6F] which is BOM + "hello".
/// `decode_with_bom_removal` strips the BOM producing "hello";
/// `decode_without_bom_handling` preserves it as "\u{FEFF}hello".
#[test]
fn regression_rfc2047_bom_not_stripped() {
    // Base64 of [0xFE, 0xFF, 0x00, 0x68, 0x00, 0x65, 0x00, 0x6C, 0x00, 0x6C, 0x00, 0x6F]
    let input = b"=?UTF-16BE?B?/v8AaABlAGwAbABv?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "\u{FEFF}hello",
        "RFC 2047 Section 2: encoded words are header fragments, not standalone \
             documents  -  a leading U+FEFF must be preserved as content, not stripped as BOM"
    );
}

#[test]
fn spec_audit_rfc2047_unknown_encoding_preserved_verbatim() {
    // RFC 2047 Section 6.3: If an encoded word is not recognized,
    // it MUST be displayed as-is.
    // Bug: parse_encoded_word advanced the remaining pointer before
    // returning None on unknown encoding 'X', dropping characters.
    let input = b"=?UTF-8?X?hello?= world";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "=?UTF-8?X?hello?= world",
        "Unknown encoding 'X' must preserve entire encoded word verbatim (RFC 2047 Section 6.3)"
    );
}

#[test]
fn spec_audit_rfc2047_invalid_base64_preserved_verbatim() {
    // RFC 2047 Section 6.3: If decoding fails, the encoded word
    // MUST be displayed as-is.
    // Bug: parse_encoded_word advanced past charset/encoding/text
    // before base64 decode failed, losing the consumed characters.
    let input = b"=?UTF-8?B?invalid base64!!!?= tail";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "=?UTF-8?B?invalid base64!!!?= tail",
        "Failed base64 decode must preserve entire encoded word verbatim (RFC 2047 Section 6.3)"
    );
}

#[test]
fn rfc2047_whitespace_preserved_before_invalid_encoded_word() {
    // RFC 2047 Section 6.2: whitespace between adjacent encoded words is
    // ignored, but ONLY when both tokens are valid encoded words.
    // RFC 2047 Section 6.3: unrecognized encoded words must be displayed
    // as ordinary text, so whitespace before them must be preserved.
    let input = b"=?UTF-8?Q?Hello?= =?UTF-8?X?fail?= end";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Hello =?UTF-8?X?fail?= end",
        "Whitespace before an invalid encoded word must be preserved (RFC 2047 Section 6.3)"
    );
}

#[test]
fn rfc2047_requires_lwsp_boundaries_in_text() {
    // RFC 2047 Section 5: an encoded-word in `*text` must be separated
    // from adjacent text by linear whitespace. A glued token is not a
    // valid encoded-word occurrence and must remain literal.
    let input = b"Prefix=?UTF-8?Q?caf=C3=A9?=Suffix";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Prefix=?UTF-8?Q?caf=C3=A9?=Suffix",
        "RFC 2047 Section 5: glued encoded-words must not be decoded"
    );
}

// ===== Negative / malformed input tests =====

#[test]
fn quoted_string_rejects_nul() {
    assert!(quoted_string(b"\"abc\x00def\"").is_err());
}

#[test]
fn quoted_string_rejects_bare_cr() {
    assert!(quoted_string(b"\"abc\rdef\"").is_err());
}

#[test]
fn quoted_string_rejects_bare_lf() {
    assert!(quoted_string(b"\"abc\ndef\"").is_err());
}

/// RFC 3501 Section 9: QUOTED-CHAR = any TEXT-CHAR except quoted-specials,
/// and TEXT-CHAR excludes CR and LF.  A backslash followed by CR is
/// malformed  -  it must not silently include the CR in the result or skip
/// past the CRLF response terminator.
#[test]
fn quoted_string_rejects_escaped_cr() {
    // \<CR> is not a valid QUOTED-CHAR  -  must error.
    assert!(quoted_string(b"\"test\\\rmore\"").is_err());
}

/// Same as above but for LF.
#[test]
fn quoted_string_rejects_escaped_lf() {
    // \<LF> is not a valid QUOTED-CHAR  -  must error.
    assert!(quoted_string(b"\"test\\\nmore\"").is_err());
}

/// RFC 3501 Section 9: "The ASCII NUL character, %x00, MUST NOT be used
/// at any time."  A backslash followed by NUL must be rejected  -  it must
/// not silently push NUL into the output.
#[test]
fn quoted_string_rejects_nul_after_backslash() {
    // \<NUL> is not a valid QUOTED-CHAR  -  must error.
    assert!(quoted_string(b"\"hello\\\x00world\"").is_err());
}

#[test]
fn quoted_string_unterminated() {
    // Complete-mode: unterminated quote must return Error (not Incomplete)
    // so that alt() can fall through to alternative parsers.
    let result = quoted_string(b"\"hello");
    assert!(matches!(result, Err(nom::Err::Error(_))));
}

#[test]
fn quoted_string_unterminated_escape() {
    // Complete-mode: truncated escape must return Error (not Incomplete).
    let result = quoted_string(b"\"hello\\");
    assert!(matches!(result, Err(nom::Err::Error(_))));
}

#[test]
fn literal_truncated_data() {
    // Literal says 10 bytes, but only 5 provided
    let result = literal(b"{10}\r\nhello");
    assert!(result.is_err());
}

#[test]
fn literal_missing_crlf() {
    let result = literal(b"{5}hello");
    assert!(result.is_err());
}

#[test]
fn literal_overflow_count() {
    // Absurdly large count should fail gracefully
    let result = literal(b"{99999999999999999999}\r\n");
    assert!(result.is_err());
}

/// RFC 9051 Section 9: literal count is `number64` (0..2^63-1).
/// Counts exceeding `i64::MAX` must be rejected regardless of platform
/// pointer width. The parser must use u64 for the initial parse, not
/// usize, to avoid truncation on 32-bit platforms.
#[test]
fn literal_count_exceeds_i64_max() {
    // i64::MAX + 1 = 9223372036854775808
    let result = literal(b"{9223372036854775808}\r\n");
    assert!(result.is_err(), "count > i64::MAX must be rejected");
}

/// Literal count at exactly `i64::MAX` should not be rejected by the
/// range check (though it will fail due to insufficient input data).
#[test]
fn literal_count_at_i64_max() {
    // i64::MAX = 9223372036854775807  -  valid per RFC 9051, but
    // take(count) will fail since we don't have that many bytes.
    let result = literal(b"{9223372036854775807}\r\n");
    assert!(
        result.is_err(),
        "should fail from insufficient data, not range check"
    );
}

#[test]
fn number_rejects_non_digit() {
    assert!(number(b"abc").is_err());
}

#[test]
fn number_rejects_empty() {
    assert!(number(b"").is_err());
}

#[test]
fn number_rejects_negative() {
    assert!(number(b"-1").is_err());
}

#[test]
fn nstring_garbage() {
    // Not NIL and not a string
    assert!(nstring(b"GARBAGE").is_err());
}

#[test]
fn high_byte_in_atom_accepted() {
    // Bytes 0x80-0xFF are valid in atoms (not CTL, not specials)
    let (_, val) = atom(b"\xC0\xC1\xC2 rest").unwrap();
    assert_eq!(val, b"\xC0\xC1\xC2");
}

/// Regression: unknown STATUS attribute with an atom value starting with
/// "NIL" (e.g. "NILSIMSA") must be skipped completely. The old code used
/// `tag_no_case(&b"NIL"[..])` without a token boundary check, so it consumed
/// only "NIL" and left the residue ("SIMSA") in the stream, corrupting
/// subsequent attribute parsing (RFC 9051 Section 7.1).
#[test]
fn status_unknown_attr_nil_prefix_atom() {
    let input = b"STATUS \"INBOX\" (MESSAGES 10 XHASH NILSIMSA UNSEEN 3)\r\n";
    let (_, resp) =
        parse_untagged_status_mailbox(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(
            items.contains(&StatusItem::Messages(10)),
            "MESSAGES 10 missing: {items:?}"
        );
        assert!(
            items.contains(&StatusItem::Unseen(3)),
            "UNSEEN 3 missing: {items:?}"
        );
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

/// Regression: `skip_paren_group` must not let a backslash escape
/// consume CR/LF bytes. CR/LF are not valid QUOTED-CHAR (RFC 3501
/// Section 9), so the escape must stop and the outer loop must
/// resume. Without this guard, a `\"` followed by `\r\n` would
/// skip the CR, corrupting the parse.
#[test]
fn skip_paren_group_backslash_crlf_in_quoted_string() {
    // ("val\<CR><LF>...)  -  the backslash must NOT skip the CR.
    // After breaking out of the quoted string, the outer loop
    // sees CR and (in a real response) would stop. For
    // skip_paren_group in isolation, the unclosed paren causes
    // an error, which is the correct outcome for malformed input.
    let input = b"(\"val\\\r\n\")";
    let result = skip_paren_group(input);
    assert!(result.is_err());
}

#[test]
fn paren_skippers_do_not_scan_past_raw_crlf_in_a_quoted_value() {
    // RFC 3501 Section 9: quoted = DQUOTE *QUOTED-CHAR DQUOTE, and QUOTED-CHAR
    // excludes CR and LF - no backslash required. An unterminated quote must
    // not let a scanner adopt the *next* response as part of this value: here
    // the closing quote and paren only appear after `* 2 EXISTS\r\n`.
    let group = b"(\"value\r\n* 2 EXISTS\r\n\")";
    assert!(skip_paren_group(group).is_err());
    assert!(skip_parenthesized_block(group).is_err());

    let section = b"HEADER.FIELDS (\"value\r\n* 2 EXISTS\r\n\") ]";
    assert!(scan_section_spec(section).is_err());
}

/// RFC 8474 Section 7: objectid with characters outside the restricted set
/// should still parse via atom fallback (Postel's law).
#[test]
fn objectid_non_compliant_falls_back_to_atom() {
    // Dot '.' is not in the RFC 8474 objectid charset but is a valid atom char.
    let (rest, val) = objectid(b"F2212ea87.6097 rest").unwrap();
    assert_eq!(val, b"F2212ea87.6097");
    assert_eq!(rest, b" rest");
}

// ===== Large literal tests =====

/// Literal with a large byte count parses correctly.
#[test]
fn literal_large_payload() {
    let size = 100_000;
    let mut input = format!("{{{size}}}\r\n").into_bytes();
    input.extend(vec![b'X'; size]);
    let (rest, val) = literal(&input).unwrap();
    assert_eq!(val.len(), size);
    assert!(rest.is_empty());
}

/// Literal count at `u32::MAX` boundary is accepted by the size parser but
/// rejected because there isn't enough trailing data (Incomplete/Error).
#[test]
fn literal_u32_max_count_insufficient_data() {
    let input = b"{4294967295}\r\nshort";
    assert!(literal(input).is_err());
}

/// Literal count exceeding u32 range fails to parse.
#[test]
fn literal_count_overflow() {
    let input = b"{99999999999}\r\ndata";
    assert!(literal(input).is_err());
}

/// `body_language` with a single string (RFC 3501 Section 7.4.2).
#[test]
fn body_language_single_string() {
    let (_, lang) = body_language(b"\"en\"").unwrap();
    let langs = lang.expect("should not be None");
    assert_eq!(langs, vec!["en"]);
}

/// Number overflow beyond `u32` returns parse error.
#[test]
fn number_overflow_u32() {
    // 4294967296 = u32::MAX + 1
    assert!(number(b"4294967296").is_err());
}

/// Number overflow beyond `u64` returns parse error.
#[test]
fn number_overflow_u64() {
    // 18446744073709551616 = u64::MAX + 1
    assert!(number64(b"18446744073709551616").is_err());
}

/// Non-ASCII bytes in quoted strings are handled gracefully.
#[test]
fn quoted_string_non_ascii_bytes() {
    // Subject with raw non-ASCII in quoted string (some servers do this)
    let input =
        b"* 1 FETCH (UID 1 ENVELOPE (NIL \"caf\xc3\xa9\" NIL NIL NIL NIL NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(
            fr.envelope.as_ref().and_then(|e| e.subject.as_deref()),
            Some("café")
        );
        return;
    }
    panic!("expected Fetch with non-ASCII envelope subject");
}

// ===== RFC 6855 literal8 tests =====

/// literal8: `~{n}\r\n<data>` accepts the tilde prefix (RFC 6855 Section 4).
#[test]
fn literal8_simple() {
    let (rest, val) = literal(b"~{5}\r\nhello rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
}

/// Per Postel's law, tolerate `~{n+}` (literal8 combined with LITERAL+) from
/// non-conformant servers. RFC 9051 Section 9 does not define `+` for literal8,
/// and RFC 7888 Section 4 defines LITERAL+ for regular literals only, but the
/// `+` has no semantic impact on server-to-client data so we accept it.
#[test]
fn literal8_plus_tolerated() {
    let (rest, val) = literal(b"~{3+}\r\nabc rest").unwrap();
    assert_eq!(val, b"abc");
    assert_eq!(rest, b" rest");
}

/// literal8 with zero length.
#[test]
fn literal8_zero_length() {
    let (_, val) = literal(b"~{0}\r\n").unwrap();
    assert!(val.is_empty());
}

/// literal8 with UTF-8 content (RFC 6855 Section 4).
#[test]
fn literal8_utf8_content() {
    let data = "日本語";
    let header = format!("~{{{}}}\r\n{}", data.len(), data);
    let (_, val) = literal(header.as_bytes()).unwrap();
    assert_eq!(val, data.as_bytes());
}

/// RFC 9051 Section 9: literal8 (`~{n}`) does not define the `+` suffix,
/// but per Postel's law we tolerate `~{n+}` from non-conformant servers.
#[test]
fn spec_audit_literal8_tolerates_non_sync() {
    // Regular literal with '+' (LITERAL+) should be accepted
    assert!(
        literal(b"{5+}\r\nhello").is_ok(),
        "LITERAL+ must be accepted"
    );
    // literal8 without '+' should be accepted
    assert!(
        literal(b"~{5}\r\nhello").is_ok(),
        "literal8 must be accepted"
    );
    // literal8 with '+'  -  tolerated per Postel's law (RFC 9051 Section 9
    // does not define it, but the `+` has no semantic impact on
    // server-to-client data).
    assert!(
        literal(b"~{5+}\r\nhello").is_ok(),
        "literal8 with + must be tolerated per Postel's law"
    );
}

/// literal8 in an nstring context (via string -> literal).
#[test]
fn nstring_literal8() {
    let (_, val) = nstring(b"~{4}\r\ntest").unwrap();
    assert_eq!(val, Some(b"test".to_vec()));
}

/// `parse_response_utf8` with `utf8_mode=false` behaves like `parse_response`.
#[test]
fn parse_response_utf8_false_decodes_rfc2047() {
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            NIL NIL NIL NIL \"<id@x.com>\"))\r\n";
    let (_, resp) = parse_response_utf8(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = *u {
            let env = fr.envelope.unwrap();
            assert_eq!(env.subject.as_deref(), Some("Alice"));
            assert_eq!(env.from[0].name.as_deref(), Some("Alice"));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== atom parser allows `[` (RFC 3501 Section 9) =====

#[test]
fn atom_accepts_open_bracket() {
    // '[' is a valid ATOM-CHAR per RFC 3501 Section 9
    let (rest, val) = atom(b"BODY[TEXT] rest").unwrap();
    assert_eq!(val, b"BODY[TEXT");
    assert_eq!(rest, b"] rest");
}

#[test]
fn atom_still_stops_at_close_bracket() {
    let (rest, val) = atom(b"FOO]bar").unwrap();
    assert_eq!(val, b"FOO");
    assert_eq!(rest, b"]bar");
}

// L2: Quoted string accepts any \X escape (deliberate leniency).
// RFC 3501 Section 9 says only \" and \\ are valid, but servers
// in the wild send other escapes. We accept them as literal X.
#[test]
fn quoted_string_lenient_escapes() {
    // RFC 3501 Section 9: only \" and \\ are valid quoted-specials.
    // A \n is malformed  -  the backslash is preserved as literal data
    // per Postel's law (the sender intended to include it).
    let input = b"\"hello\\nworld\"\r\n";
    let (_, val) = crate::codec::decode::quoted_string(input).unwrap();
    assert_eq!(val, b"hello\\nworld");
}

// L4: number64 is now capped at 2^63-1 per RFC 9051 Section 4.
// Values above i64::MAX are rejected. The malformed MODSEQ is skipped
// gracefully; the FETCH response is preserved with mod_seq = None
// (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn number64_rejects_above_i64_max() {
    // u64::MAX exceeds number64 range (RFC 9051 Section 4).
    // The MODSEQ attribute is skipped, but the FETCH response is preserved.
    let input = b"* 1 FETCH (MODSEQ (18446744073709551615))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = &*boxed {
            assert_eq!(
                fr.mod_seq.map(crate::types::ModSeqValzer::get),
                None,
                "u64::MAX MODSEQ should be skipped (None), not stored"
            );
        } else {
            panic!("expected Fetch with mod_seq=None, got {boxed:?}");
        }
    } else {
        panic!("Expected Untagged, got {resp:?}");
    }
}

/// literal count exceeding `i64::MAX` (number64 range per
/// RFC 9051 Section 9) must be rejected by the parser.
#[test]
fn regression_literal_count_exceeds_number64_range() {
    // i64::MAX + 1 = 9223372036854775808, which exceeds the number64 range.
    // Test the literal parser directly via a string context.
    // A quoted-or-literal string `{9223372036854775808}\r\n` should fail.
    let input = b"{9223372036854775808}\r\n";
    let result = literal(input);
    assert!(
        result.is_err(),
        "literal count exceeding i64::MAX must be rejected \
             (RFC 9051 Section 9: number64 = 0..2^63-1); got: {result:?}"
    );
}

/// L4: RFC 9051 Section 4 limits number64 to 2^63-1 (non-negative signed 63-bit).
/// The parser currently uses u64 and happily accepts values above `i64::MAX`.
#[test]
fn spec_audit_l4_number64_exceeds_63bit() {
    // 9999999999999999999 is > i64::MAX (9223372036854775807) but < u64::MAX
    let result = number64(b"9999999999999999999 rest");
    assert!(
        result.is_err(),
        "number64 should reject values > 2^63-1 per RFC 9051 Section 4, \
             but it accepted the value"
    );
}

/// L6: Atoms with bytes > 0x7F are accepted for compatibility with
/// non-conformant servers. RFC 3501 CHAR = %x01-7f but many servers
/// send non-ASCII bytes in atoms.
#[test]
fn spec_audit_l6_atom_with_high_bytes() {
    // An atom containing a byte > 0x7F (e.g. 0xC0, 0xE9)
    let input = b"\xc0\xe9abc rest";
    let (rest, val) = atom(input).unwrap();
    assert_eq!(rest, b" rest");
    assert_eq!(val, b"\xc0\xe9abc");
}

/// M3: `skip_paren_group` must handle literals.
/// RFC 3501 Section 9: body-extension may contain nstring with literals.
#[test]
fn audit_m3_skip_paren_group_handles_literal() {
    let input = b"({5}\r\nhel)o) rest";
    let (rest, _) = skip_paren_group(input).unwrap();
    assert_eq!(rest, b" rest");
}

/// L5: `nz-number` must reject leading zeros.
/// RFC 3501 Section 9: `nz-number = digit-nz *DIGIT`.
#[test]
fn audit_l5_nz_number_rejects_leading_zeros() {
    assert!(nz_number(b"007 ").is_err());
}

/// RFC 9051 Section 9: nz-number64 = digit-nz *DIGIT  -  leading zeros are invalid.
#[test]
fn spec_audit_nz_number64_rejects_leading_zeros() {
    // "01" starts with '0', violating digit-nz requirement
    assert!(
        nz_number64(b"01").is_err(),
        "nz-number64 must reject leading zeros per RFC 9051 Section 9"
    );
    assert!(
        nz_number64(b"007").is_err(),
        "nz-number64 must reject leading zeros per RFC 9051 Section 9"
    );
    // "0" alone should also fail (it's zero)
    assert!(nz_number64(b"0").is_err(), "nz-number64 must reject zero");
    // Valid inputs should still work
    assert_eq!(nz_number64(b"1").unwrap().1, 1);
    assert_eq!(nz_number64(b"42").unwrap().1, 42);
}

// ===== parse_untagged_unknown must skip literals (RFC 9051 Section2.2.2) =====

/// RFC 9051 Section 2.2.2: unknown response containing a literal `{N}\r\n<data>`
/// must be fully consumed. The naive `take_while` scanner stops at the literal's
/// CRLF, leaving data in the buffer (RFC 3501 Section 9: literal = "{" number "}" CRLF *CHAR8).
#[test]
fn unknown_with_literal() {
    let input = b"* XFOO {5}\r\nhello\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with literal");
    assert!(
        rem.is_empty(),
        "remaining bytes should be empty, got {:?}",
        String::from_utf8_lossy(rem)
    );
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(
                    text.starts_with("XFOO"),
                    "Unknown text should start with XFOO, got: {text}"
                );
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// Baseline: unknown response with a quoted string (no literal) should still work.
#[test]
fn unknown_with_quoted_string() {
    let input = b"* XBAR \"quoted\"\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with quoted string");
    assert!(rem.is_empty());
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XBAR"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// RFC 3516 / RFC 6855 Section 4: literal8 `~{N}\r\n<data>` in unknown response.
#[test]
fn unknown_with_literal8() {
    let input = b"* XBAZ ~{3}\r\nabc\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with literal8");
    assert!(
        rem.is_empty(),
        "remaining bytes should be empty, got {:?}",
        String::from_utf8_lossy(rem)
    );
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XBAZ"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// Regression: a quoted string ending with a backslash right before the
/// response-terminating CRLF in `scan_unknown_response` causes the `pos += 2`
/// to skip over `\r`, swallowing the response terminator. The outer loop then
/// cannot find a proper CRLF, causing a parse failure on well-formed input.
///
/// The fix adds a bounds check before advancing (mirroring `skip_balanced_parens`
/// at line ~162 and `quoted_string` at line ~318): if the next byte is `\r`, `\n`,
/// or past end-of-input, the escape is truncated and the scanner must break out
/// of the quoted-string loop.
///
/// RFC 3501 Section 9: quoted = DQUOTE *QUOTED-CHAR DQUOTE;
/// QUOTED-CHAR excludes CR and LF.
#[test]
fn unknown_with_truncated_quoted_backslash_no_panic() {
    // The backslash at the end of the quoted string would `pos += 2`, skipping
    // the `\r` of the CRLF terminator. Without the fix, `parse_response` fails
    // because `crlf` cannot find `\r\n`  -  only `\n` remains.
    let input = b"* XFOO \"trail\\\r\n";
    let result = parse_response(input);
    match result {
        Ok((rem, Response::Untagged(boxed))) => {
            assert!(
                rem.is_empty(),
                "remaining bytes should be empty, got {:?}",
                String::from_utf8_lossy(rem)
            );
            match *boxed {
                UntaggedResponse::Unknown(ref text) => {
                    assert!(
                        text.starts_with("XFOO"),
                        "Unknown text should start with XFOO, got: {text}"
                    );
                }
                other => panic!("Expected Unknown, got {other:?}"),
            }
        }
        Ok((_, other)) => panic!("Expected Untagged(Unknown), got {other:?}"),
        Err(e) => panic!(
            "BUG: parse_response failed on truncated quoted escape: {e:?}  -  \
                 the backslash swallowed the CRLF terminator"
        ),
    }

    // Also verify: backslash as the very last byte of the input (no CRLF at all).
    // The scanner must not panic; it should return an error (no CRLF found).
    let direct_input = b"XFOO \"trail\\";
    let result2 = scan_unknown_response(direct_input);
    assert!(
        result2.is_err(),
        "scan_unknown_response on truncated input with trailing backslash \
             should return Err (no CRLF), got {result2:?}"
    );
}

/// RFC 7888: LITERAL+ `{N+}\r\n<data>` in unknown response.
#[test]
fn unknown_with_literal_plus() {
    let input = b"* XQUX {3+}\r\nabc\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with LITERAL+");
    assert!(
        rem.is_empty(),
        "remaining bytes should be empty, got {:?}",
        String::from_utf8_lossy(rem)
    );
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XQUX"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

// ===== integer overflow in literal count arithmetic (RFC 3501 Section 9) =====

/// RFC 3501 Section 9: literal count near `usize::MAX` must not wrap around
/// in `try_skip_literal`. The function must return `None` instead.
#[test]
fn try_skip_literal_overflow_returns_none() {
    // Construct a literal header with count = usize::MAX.
    // On 64-bit: {18446744073709551615}\r\n
    let count_str = format!("{{{}}}\r\n", usize::MAX);
    let input = count_str.as_bytes();
    // pos after parsing header will be small, so pos + usize::MAX would wrap.
    // The function must return None (not enough data / overflow).
    assert_eq!(
        try_skip_literal(input),
        None,
        "try_skip_literal must return None when literal count overflows usize"
    );
}

/// RFC 3501 Section 9: literal count near `usize::MAX` must not wrap around
/// in `skip_balanced_parens`. The function must skip the malformed literal
/// rather than advancing to a wrapped-around position.
#[test]
fn skip_balanced_parens_literal_overflow_no_wrap() {
    // Build: {<usize::MAX>}\r\n)
    // The literal count is huge; the function must not wrap and must
    // return early to avoid misinterpreting literal body bytes as structure.
    let literal = format!("{{{}}}\r\n", usize::MAX);
    let mut input = literal.into_bytes();
    input.push(b')'); // closing paren after the literal
    let result = skip_balanced_parens(&input);
    // The function should succeed  -  returning early at the overflow point
    // rather than consuming body bytes as parenthesized structure.
    assert!(
        result.is_ok(),
        "skip_balanced_parens must handle overflow gracefully"
    );
    let (rest, ()) = result.unwrap();
    // With the early-return fix, rest includes the literal digits/marker
    // and the trailing ')'. The key invariant is that the function did not
    // wrap around or panic.
    assert!(
        rest.ends_with(b")"),
        "skip_balanced_parens must not consume past overflow point; rest = {:?}",
        String::from_utf8_lossy(rest)
    );
}

/// RFC 3501 Section 9: literal count near `usize::MAX` must not wrap around
/// in `skip_paren_group`. The function must skip the malformed literal
/// rather than advancing to a wrapped-around position.
#[test]
fn skip_paren_group_literal_overflow_no_wrap() {
    // Build: ({<usize::MAX>}\r\n)
    let literal = format!("({{{}}}\r\n)", usize::MAX);
    let input = literal.as_bytes();
    let result = skip_paren_group(input);
    // The function should return Err  -  the literal count overflows, so it
    // cannot find the matching ')' without misinterpreting body bytes.
    assert!(
        result.is_err(),
        "skip_paren_group must return Err on literal overflow \
             (RFC 3501 Section 9 / RFC 9051 Section 9)"
    );
}

#[test]
fn scan_section_spec_literal_overflow_does_not_panic() {
    // scan_section_spec did not bounds-check after advancing
    // `pos` by the literal byte count. A literal count exceeding the
    // remaining input (e.g. {999}) must not cause an out-of-bounds access
    // or integer overflow  -  the parser should return an error or fall
    // through gracefully (RFC 3501 Section 7.4.2, Section 9).

    // Case 1: Literal count (999) exceeds remaining input. Before the fix
    // this would read past the buffer; now the literal skip is silently
    // ignored and the parser continues without panic.
    let input = b"* 1 FETCH (BODY[HEADER.FIELDS ({999}\r\nXX)] \"test\")\r\n";
    let result = parse_response(input);
    // Any non-panicking result is acceptable  -  the key invariant is that
    // the parser does not access out-of-bounds memory.
    assert!(
        result.is_ok() || result.is_err(),
        "scan_section_spec must not panic on oversized literal count"
    );

    // Case 2: Literal count that would overflow usize when added to the
    // current position (RFC 3501 Section 4.3).
    let input2 = format!(
        "* 1 FETCH (BODY[HEADER.FIELDS ({{{}}}\r\nAB)] \"x\")\r\n",
        usize::MAX
    );
    let result2 = parse_response(input2.as_bytes());
    assert!(
        result2.is_ok() || result2.is_err(),
        "scan_section_spec must not panic on usize::MAX literal count"
    );
}

/// RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
/// Inside `scan_section_spec`, a backslash before CR/LF in a quoted string
/// must NOT cause the scanner to skip past the CRLF response terminator  -
/// same bug class as `skip_paren_group` (fc4afd9).
#[test]
fn scan_section_spec_backslash_crlf_in_quoted_string() {
    // Input to scan_section_spec: the bytes after `[` up to the unquoted `]`.
    // Here the quoted string contains \<CR><LF> which is malformed.
    // The scanner must NOT skip the CR via the backslash escape.
    let input = b"HEADER.FIELDS (\"val\\\r\nmore\")]rest";
    let result = scan_section_spec(input);
    if let Ok((rest, section)) = result {
        // If it parses, the section content must NOT include bytes past
        // the CRLF  -  the scanner must break out of the quoted string
        // at the CR/LF rather than consuming past it.
        let section_str = String::from_utf8_lossy(section);
        assert!(
            !section_str.contains("more"),
            "scan_section_spec consumed past CRLF: section = {section_str:?}",
        );
        // The `]` and `rest` must remain unconsumed or be just past `]`.
        assert!(
            rest == b"rest" || rest.starts_with(b"]") || rest.contains(&b'\n'),
            "unexpected rest after scan_section_spec: {:?}",
            String::from_utf8_lossy(rest)
        );
    }
    // Err is also acceptable  -  malformed input.
}

#[test]
fn regression_status_unknown_attr_quoted_string_value() {
    // Unknown STATUS attribute with a quoted string value containing spaces.
    // The quoted string's internal spaces must not prematurely terminate
    // the skip, corrupting parsing of subsequent known attributes.
    let input = b"* STATUS \"INBOX\" (MESSAGES 10 XFOO \"some value\" UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            let messages = items.iter().find_map(|i| match i {
                StatusItem::Messages(n) => Some(*n),
                _ => None,
            });
            let unseen = items.iter().find_map(|i| match i {
                StatusItem::Unseen(n) => Some(*n),
                _ => None,
            });
            assert_eq!(
                messages,
                Some(10),
                "MESSAGES must be parsed before unknown quoted-string attr"
            );
            assert_eq!(
                unseen,
                Some(3),
                "UNSEEN must be parsed after skipping unknown quoted-string value"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn regression_status_unknown_attr_literal_value() {
    // Unknown STATUS attribute with a literal value containing spaces.
    // The literal's {N}\r\n<N bytes> must be consumed as a unit, even
    // when the payload contains spaces or parens.
    let input = b"* STATUS \"INBOX\" (MESSAGES 10 XBLOB {5}\r\na b ) UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            let messages = items.iter().find_map(|i| match i {
                StatusItem::Messages(n) => Some(*n),
                _ => None,
            });
            let unseen = items.iter().find_map(|i| match i {
                StatusItem::Unseen(n) => Some(*n),
                _ => None,
            });
            assert_eq!(
                messages,
                Some(10),
                "MESSAGES must be parsed before unknown literal attr"
            );
            assert_eq!(
                unseen,
                Some(3),
                "UNSEEN must be parsed after skipping unknown literal value"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== Coverage gap tests =====
//
// Tests below target specific uncovered lines identified by coverage analysis.

// ── 1. Escaped chars in string parsing (L163, L173-175) ──

/// RFC 3501 Section 9 / RFC 9051 Section 9: `skip_balanced_parens` must handle
/// escaped characters inside quoted strings so that `\"` does not prematurely
/// end the string and throw off depth tracking.
#[test]
fn skip_balanced_parens_escaped_chars_in_quoted_string() {
    // Quoted string with escaped quote and escaped backslash inside parens.
    // The `\"` and `\\` sequences must be correctly skipped.
    let input = b"(\"a\\\"b\\\\c\") tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

/// RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
/// A backslash before CR/LF in a quoted string inside a parenthesized
/// block must NOT cause the parser to skip past the CRLF response
/// terminator  -  same bug class as `skip_paren_group` (fc4afd9).
#[test]
fn skip_balanced_parens_backslash_crlf_in_quoted_string() {
    // ("val\<CR><LF>...)  -  the backslash must NOT skip the CR.
    let input = b"(\"val\\\r\n\")";
    let result = skip_parenthesized_block(input);
    // If it parses, the CRLF + trailing `")` must NOT have been consumed
    // as part of the quoted string escape.
    if let Ok((rest, ())) = result {
        assert!(
            !rest.is_empty(),
            "skip_balanced_parens consumed past CRLF boundary"
        );
    }
    // Err is also acceptable  -  malformed input.
}

/// RFC 6855 Section 4: `~{n}\r\n<data>` literal8 prefix in `skip_balanced_parens`.
/// The `~` prefix must be consumed so the `{` on the next iteration handles
/// the literal byte count correctly.
#[test]
fn skip_balanced_parens_literal8_prefix() {
    // ~{5}\r\nhello inside a parenthesized block
    let input = b"(~{5}\r\nhello) tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

/// RFC 6855 Section 4: literal8 `~{n}\r\n<data>` with nested parentheses
/// in the literal data must not affect depth tracking.
#[test]
fn skip_balanced_parens_literal8_with_parens_in_data() {
    // literal8 containing bytes that look like parens  -  must be skipped by count
    let input = b"(~{3}\r\n(x)) tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

// ── 2. Literal+ / literal counting edge cases (L190, L207-211) ──

/// RFC 7888 Section 4: `{n+}` non-synchronizing literal in `skip_balanced_parens`.
/// The `+` after the count must be correctly handled.
#[test]
fn skip_balanced_parens_literal_plus() {
    let input = b"({5+}\r\nhello) tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

/// RFC 3501 Section 9: literal `{n}\r\n<data>` in nested parens.
/// The skip function must handle both literal counting AND nested depth.
#[test]
fn skip_balanced_parens_literal_in_nested_parens() {
    let input = b"((\"key\" {3}\r\nval)) rest";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" rest");
}

/// Edge case: literal with invalid (non-digit) count inside `skip_balanced_parens`.
/// Per Postel's law, the parser should not panic  -  it should skip past the `{`
/// and continue scanning.
#[test]
fn skip_balanced_parens_literal_bad_count() {
    // `{abc}` is not a valid literal  -  parser skips past `{` and continues
    let input = b"({abc}) rest";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" rest");
}

// ── 3. Number parsing error paths (L402-403, L418-419, L467-468, L490-493, L505-506, L537-540) ──

/// RFC 3501 Section 9: `number` rejects non-digit input.
#[test]
fn number_rejects_alpha_string() {
    assert!(number(b"abc").is_err());
}

/// RFC 3501 Section 9: `number` rejects u32 overflow values.
#[test]
fn number_rejects_u32_overflow() {
    // 4294967296 = u32::MAX + 1
    assert!(number(b"4294967296").is_err());
}

/// RFC 9051 Section 4: `number64` rejects values exceeding `i64::MAX` (63-bit limit).
#[test]
fn number64_rejects_above_i64_max_boundary() {
    // i64::MAX + 1 = 9223372036854775808
    assert!(number64(b"9223372036854775808").is_err());
}

/// RFC 9051 Section 4: `number64` rejects complete u64 overflow.
#[test]
fn number64_rejects_u64_overflow_parse_error() {
    // 18446744073709551616 = u64::MAX + 1
    assert!(number64(b"18446744073709551616").is_err());
}

/// RFC 3501 Section 9: `nz_number` rejects zero.
#[test]
fn nz_number_rejects_zero() {
    assert!(nz_number(b"0").is_err());
}

/// RFC 3501 Section 9: `nz_number` rejects leading zero (e.g. "01").
/// `digit-nz = %x31-39`  -  leading zero is never valid.
#[test]
fn nz_number_rejects_leading_zero() {
    assert!(nz_number(b"01").is_err());
}

/// RFC 3501 Section 9: `nz_number` accepts valid non-zero values.
#[test]
fn nz_number_accepts_valid() {
    let (_, val) = nz_number(b"42").unwrap();
    assert_eq!(val, 42);
}

/// RFC 9051 Section 9: `nz_number64` rejects zero.
#[test]
fn nz_number64_rejects_zero() {
    assert!(nz_number64(b"0").is_err());
}

/// RFC 9051 Section 9: `nz_number64` rejects leading zero.
#[test]
fn nz_number64_rejects_leading_zero() {
    assert!(nz_number64(b"01").is_err());
}

/// RFC 9051 Section 9: `nz_number64` accepts valid non-zero values.
#[test]
fn nz_number64_accepts_valid() {
    let (_, val) = nz_number64(b"12345678901234").unwrap();
    assert_eq!(val, 12345678901234);
}

/// RFC 3501 Section 9: literal `from_utf8` error path (L401-403).
/// `digit1` always returns ASCII digits, so this path is unreachable
/// in normal use, but we exercise the number path with an overflow
/// that goes through `parse::<u64>` failure (L406-408).
#[test]
fn literal_count_u64_overflow() {
    // A number so large it overflows u64::MAX
    let result = literal(b"{99999999999999999999999}\r\n");
    assert!(result.is_err());
}

/// RFC 9051 Section 9: literal8 does not define `+` suffix, but per
/// Postel's law we tolerate `~{n+}` since `+` has no semantic impact.
#[test]
fn literal8_tolerates_plus_suffix() {
    let result = literal(b"~{5+}\r\nhello");
    assert!(
        result.is_ok(),
        "literal8 ~{{n+}} must be tolerated per Postel's law"
    );
    let (rest, val) = result.unwrap();
    assert_eq!(val, b"hello");
    assert!(rest.is_empty());
}

/// RFC 6855 Section 4: literal8 `~{n}\r\n<data>` must parse correctly.
#[test]
fn literal8_basic() {
    let (rest, val) = literal(b"~{5}\r\nhello rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
}

/// RFC 3501 Section 5.1.3: `&-` (escaped ampersand) must decode to
/// literal `&`. An empty Base64 section between `&` and `-` is the
/// canonical encoding for the ampersand character.
#[test]
fn edge_utf7_escaped_ampersand() {
    use crate::types::wire::{decode_utf7, encode_utf7};

    // &- must decode to &
    assert_eq!(
        decode_utf7(b"&-"),
        "&",
        "RFC 3501 Section 5.1.3: &- must decode to literal &"
    );

    // & must encode to &-
    assert_eq!(
        encode_utf7("&"),
        "&-",
        "RFC 3501 Section 5.1.3: & must encode as &-"
    );

    // Multiple &- in sequence
    assert_eq!(
        decode_utf7(b"&-&-"),
        "&&",
        "RFC 3501 Section 5.1.3: consecutive &- must each decode to &"
    );

    // &- mixed with other text
    assert_eq!(
        decode_utf7(b"AT&-T"),
        "AT&T",
        "RFC 3501 Section 5.1.3: &- in the middle of text"
    );
}

/// RFC 9051 Section 9: literal8 syntax is `~{number64}`  -  the `+`
/// (non-synchronizing) modifier is not defined. Per Postel's law, we
/// tolerate `~{N+}` since `+` has no semantic impact on server-to-client data.
#[test]
fn literal8_tolerates_non_synchronizing_modifier() {
    // ~{100+}\r\n followed by 100 bytes of data.
    let mut input = b"~{100+}\r\n".to_vec();
    input.extend(std::iter::repeat_n(b'x', 100));
    let result = literal(&input);
    assert!(
        result.is_ok(),
        "literal8 with '+' modifier must be tolerated per Postel's law; got {result:?}"
    );
    let (rest, val) = result.unwrap();
    assert_eq!(val.len(), 100);
    assert!(rest.is_empty());
}

/// RFC 9051 Section 9: valid literal8 syntax `~{N}` must be accepted.
#[test]
fn literal8_accepts_valid_syntax() {
    let mut input = b"~{5}\r\n".to_vec();
    input.extend(b"HELLO");
    let result = literal(&input);
    assert!(
        result.is_ok(),
        "valid literal8 ~{{5}} must be accepted (RFC 9051 Section 9); got {result:?}"
    );
    let (remaining, data) = result.unwrap();
    assert_eq!(data, b"HELLO");
    assert!(remaining.is_empty());
}

// ===== skip_balanced_parens literal overflow tests =====

/// RFC 3501 Section 9: when a literal count exceeds the available input,
/// `skip_balanced_parens` must break out of the loop rather than treating
/// the literal body bytes as parenthesized structure. If it continues,
/// parentheses inside the literal body would corrupt the depth tracking.
#[test]
fn skip_balanced_parens_literal_overflow_must_not_process_body_bytes() {
    // Scenario: we are inside a parenthesized group at depth 1, and the
    // content has a literal `{100}` whose body exceeds available data.
    // The literal body contains a `)` which, if treated as structure,
    // would decrement depth to 0 and cause premature return  -  breaking
    // out before the actual closing `)` of the group.
    //
    // Input layout (depth 1 context  -  the outer `(` was consumed by caller):
    //   "key {100}\r\n)garbage real_close)"
    //
    // Without fix: digits/}\r\n consumed byte-by-byte, then `)` at depth 0
    //   causes immediate return with rest = "garbage real_close)".
    // With fix: break on overflow, return remaining from overflow point.
    let input = b"key {100}\r\n)garbage real_close)after";
    let (rest, ()) = skip_balanced_parens(input).unwrap();
    // Without the fix, the function treats the `)` in the literal body as
    // a structural close-paren (depth was 0), returning ")garbage real_close)after".
    // With the fix, the function returns early at the overflow point. The
    // returned rest should include the literal body bytes (not consumed),
    // and crucially the `)` in the literal body should NOT have been
    // treated as a structural close-paren.
    assert!(
        rest.len() > b")garbage real_close)after".len(),
        "skip_balanced_parens must return early on literal overflow, not \
             consume body bytes as structure (RFC 3501 Section 9); \
             rest = {:?}",
        String::from_utf8_lossy(rest)
    );
}

/// RFC 3501 Section 9 / RFC 9051 Section 9: when `checked_add` overflows
/// (crafted literal count near `usize::MAX`), `skip_balanced_parens` must
/// break rather than silently continuing.
#[test]
fn skip_balanced_parens_literal_checked_add_overflow() {
    // A literal count of 18446744073709551615 (u64::MAX) would overflow
    // usize::MAX on checked_add. The function must not panic.
    let input = b"ext {18446744073709551615}\r\ndata) rest";
    let result = skip_balanced_parens(input);
    assert!(
        result.is_ok(),
        "skip_balanced_parens must not panic on overflow: {result:?}"
    );
}

/// RFC 3501 Section 9: quoted-string with trailing escaped backslash
/// `"hello\\"` must parse as the five bytes `hello\`.
/// The `\\` is a valid `quoted-specials` escape per the ABNF:
/// `quoted-specials = DQUOTE / "\"`
#[test]
fn edge_quoted_string_trailing_escaped_backslash() {
    let (rest, val) = quoted_string(b"\"hello\\\\\" rest").unwrap();
    assert_eq!(
        val, b"hello\\",
        "\\\\  at end of quoted string must parse as single backslash \
             (RFC 3501 Section 9: quoted-specials)"
    );
    assert_eq!(rest, b" rest", "rest must follow the closing quote");
}

/// RFC 3501 Section 9: literal strings can contain arbitrary bytes,
/// including CRLF sequences. A literal `{10}\r\nhello\r\nbye` must
/// preserve the embedded `\r\n` in the data.
/// "hello\r\nbye" = 10 bytes: h(1)e(2)l(3)l(4)o(5)\r(6)\n(7)b(8)y(9)e(10).
#[test]
fn edge_literal_containing_crlf() {
    let input = b"{10}\r\nhello\r\nbye rest";
    let (rest, val) = literal(input).unwrap();
    assert_eq!(
        val, b"hello\r\nbye",
        "literal must preserve embedded CRLF (RFC 3501 Section 9)"
    );
    assert_eq!(rest, b" rest");
}

#[test]
fn skip_tagged_ext_simple_literal_form_across_terminator_sets() {
    // The literal arm is terminator-blind by construction: the octet count
    // decides where the value ends (RFC 9051 Section 4).
    assert_skip_ext(b"{3}\r\nabc rest", Some(" rest"), Some(" rest"));
    // Terminator bytes inside the literal body are data, not delimiters.
    assert_skip_ext(b"{5}\r\na b)c rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"{2}\r\n\r\n)", Some(")"), Some(")"));
    // literal8 (RFC 6855 Section 4) and LITERAL+ (RFC 7888) prefixes.
    assert_skip_ext(b"~{2}\r\nxy)", Some(")"), Some(")"));
    assert_skip_ext(b"{2+}\r\nxy)", Some(")"), Some(")"));
    // A literal whose body is shorter than its count is not a literal at all;
    // the atom fallback takes over and stops at the CR for both callers.
    assert_skip_ext(b"{9}\r\nab)", Some("\r\nab)"), Some("\r\nab)"));
}

#[test]
fn skip_tagged_ext_simple_quoted_form_across_terminator_sets() {
    // The quoted arm is tried before the atom fallback, so terminator bytes
    // inside the quotes stay inside the value for both callers.
    assert_skip_ext(b"\"a b)c\" rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"\"\" )", Some(" )"), Some(" )"));
    // RFC 9051 Section 4: `\"` and `\\` are the quoted-specials.
    assert_skip_ext(b"\"a\\\"b\" )", Some(" )"), Some(" )"));
    // An unterminated quote is not a quoted string; the atom fallback runs and
    // the two terminator sets diverge on where it stops.
    assert_skip_ext(b"\"abc)", Some(""), Some(")"));
    assert_skip_ext(b"\"abc rest", Some(" rest"), Some(" rest"));
}

#[test]
fn skip_tagged_ext_simple_atom_form_across_terminator_sets() {
    assert_skip_ext(b"123 rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"1:*,5 )", Some(" )"), Some(" )"));
    // Numbers and sequence sets butted against the caller's own closer.
    assert_skip_ext(b"42)", Some(""), Some(")"));
    assert_skip_ext(b"42\r\n", Some("\r\n"), Some("\r\n"));

    // Nothing to consume: `take_while1` needs at least one non-terminator
    // byte, so a value position that opens with a terminator - or with the
    // CR/LF that ends the response line - is an error for both callers.
    assert_skip_ext(b"", None, None);
    assert_skip_ext(b" x", None, None);
    assert_skip_ext(b")x", Some(""), None);
    assert_skip_ext(b"\r\n", None, None);
}

/// The atom fallback must not scan past the response-terminating CRLF, whatever
/// the caller's terminator set says. The STATUS set (SP/`)`) does not name CR,
/// so before the CR/LF guard in `skip_tagged_ext_simple` this input consumed the
/// CRLF and the following response's `* ` - a parse failure rather than a
/// mis-parse, since STATUS then never found its `)`, but a scan across a
/// response boundary all the same. Every sibling helper (`skip_balanced_parens`,
/// `skip_paren_group`, `skip_fetch_value`) guards this way.
#[test]
fn skip_tagged_ext_simple_atom_stops_at_the_response_boundary() {
    let input = b"badvalue\r\n* OK next\r\n";
    for (label, rest) in [
        ("STATUS", skip_tagged_ext_simple(status_terminator)(input)),
        ("ESEARCH", skip_tagged_ext_simple(esearch_terminator)(input)),
    ] {
        let (rest, ()) = rest.unwrap();
        assert_eq!(
            rest, b"\r\n* OK next\r\n",
            "{label} terminator set left the CRLF for the response parser"
        );
    }
}

/// RFC 3501 Section 7.4.2: `skip_balanced_parens` must still return Ok
/// when input is exhausted at depth == 0 (no parens at all).
#[test]
fn skip_balanced_parens_empty_at_zero_depth_returns_ok() {
    let input = b"";
    let result = skip_balanced_parens(input);
    assert!(
        result.is_ok(),
        "skip_balanced_parens must return Ok when input is empty at depth 0"
    );
}

/// RFC 3501 Section 7.4.2: `skip_balanced_parens` must return an error
/// when input is exhausted with unbalanced parentheses (depth > 0),
/// rather than silently returning Ok  -  which would consume bytes from
/// subsequent responses and corrupt downstream parsing.
#[test]
fn skip_balanced_parens_unbalanced_depth_returns_error() {
    // Open two parens, close only one  -  then input ends.
    let input = b"(\"nested\" (\"inner\")";
    let result = skip_balanced_parens(input);
    assert!(
        result.is_err(),
        "skip_balanced_parens must return Err when input is exhausted with \
             depth > 0 (unbalanced parens), got Ok with rest = {:?}",
        result
            .ok()
            .map(|(r, ())| String::from_utf8_lossy(r).to_string())
    );
}

/// RFC 8474 Section 7 / Postel's law: an objectid > 255 characters should
/// be accepted in full (via atom fallback) rather than truncated. Truncation
/// poisons the input stream  -  the leftover atom bytes cause downstream
/// parsers (e.g., the closing `)` in EMAILID) to fail.
#[test]
fn objectid_oversized_accepted_via_postel() {
    let mut input: Vec<u8> = std::iter::repeat_n(b'A', 300).collect();
    input.push(b')');
    let (rest, val) = objectid(&input).unwrap();
    // Must accept the full 300-char value, not truncate to 255
    assert_eq!(
        val.len(),
        300,
        "oversized objectid must be accepted in full"
    );
    assert_eq!(
        rest, b")",
        "rest must be the delimiter, not leftover atom bytes"
    );
}

/// RFC 8474 Section 7 / Postel's law: objectid > 255 characters must be
/// accepted in full via atom fallback. Truncating to 255 would leave
/// leftover bytes in the input stream, poisoning downstream parsers.
#[test]
fn objectid_256_chars_accepted_in_full() {
    let mut input: Vec<u8> = std::iter::repeat_n(b'A', 256).collect();
    input.push(b')');
    let (rest, val) = objectid(&input).unwrap();
    // Must accept the full 256-char atom, not truncate to 255
    assert_eq!(val.len(), 256);
    assert_eq!(rest, b")");
}

/// RFC 8474 Section 7: objectid has a 255-character maximum.
/// Exactly 255 characters must be accepted.
#[test]
fn objectid_exactly_255_chars() {
    let mut input: Vec<u8> = std::iter::repeat_n(b'A', 255).collect();
    input.push(b')'); // delimiter after objectid
    let (rest, val) = objectid(&input).unwrap();
    assert_eq!(val.len(), 255);
    assert_eq!(rest, b")");
}

/// RFC 8474 Section 7: objectid must reject empty input.
#[test]
fn objectid_empty_fails() {
    assert!(objectid(b" rest").is_err());
}

// ===== OBJECTID strict parser (RFC 8474 Section 7) tests =====

/// RFC 8474 Section 7: `objectid = 1*255(ALPHA / DIGIT / "_" / "-")`
/// The objectid parser must accept valid characters.
#[test]
fn objectid_valid_chars() {
    let (rest, val) = objectid(b"Abc_123-xyz rest").unwrap();
    assert_eq!(val, b"Abc_123-xyz");
    assert_eq!(rest, b" rest");
}

/// Regression IMAP-001: Q-encoded overlong word must also decode.
#[test]
fn imap_001_overlong_q_encoded_word_decoded() {
    // 64 A's in Q-encoding: =?UTF-8?Q?<64 A's>?= = 2+5+1+1+1+64+2 = 76 chars
    let input = b"=?UTF-8?Q?AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA?=";
    assert!(input.len() > 75, "test setup: must exceed 75 chars");
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "Postel's law: overlong Q-encoded words must be decoded"
    );
}

/// Regression IMAP-001: overlong encoded words must be decoded per Postel's law.
///
/// RFC 2047 Section 2 says encoded words "MUST NOT be more than 75 characters
/// long", but that is an ENCODER constraint. Per Postel's law (be liberal in
/// what you accept), the decoder must accept structurally valid encoded words
/// regardless of length. Many real-world servers produce overlong encoded words.
#[test]
fn imap_001_overlong_encoded_word_decoded_per_postels_law() {
    use base64::Engine;
    // Build a base64 encoded word well over 75 characters.
    // "Hello World! This is a long subject line for testing overlong encoded words in IMAP"
    // base64 of this is ~116 chars, total encoded word is ~130 chars.
    let long_text =
        "Hello World! This is a long subject line for testing overlong encoded words in IMAP";
    let b64 = base64::engine::general_purpose::STANDARD.encode(long_text.as_bytes());
    let encoded_word = format!("=?UTF-8?B?{b64}?=");
    // Verify the encoded word exceeds 75 characters
    assert!(
        encoded_word.len() > 75,
        "test setup: encoded word must exceed 75 chars, got {}",
        encoded_word.len()
    );
    let result = decode_rfc2047(encoded_word.as_bytes());
    assert_eq!(
        result, long_text,
        "Postel's law: overlong encoded words must be decoded, not preserved verbatim"
    );
}
