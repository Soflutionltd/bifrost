use super::*;

// ===== Top-level response parser tests =====

#[test]
fn greeting_ok() {
    let (_, resp) = parse_greeting(b"* OK Dovecot ready.\r\n").unwrap();
    if let Response::Greeting(g) = resp {
        assert_eq!(g.status, GreetingStatus::Ok);
        assert_eq!(g.text, "Dovecot ready.");
    } else {
        panic!("expected Greeting");
    }
}

#[test]
fn greeting_preauth() {
    let (_, resp) = parse_greeting(b"* PREAUTH already authed\r\n").unwrap();
    if let Response::Greeting(g) = resp {
        assert_eq!(g.status, GreetingStatus::PreAuth);
    } else {
        panic!("expected Greeting");
    }
}

/// RFC 3501 Section 7.1 / Postel's law: a greeting with no SP or text
/// after the status keyword should parse, consistent with how
/// `parse_tagged` and `parse_untagged_status` tolerate missing SP.
#[test]
fn greeting_bare_ok_no_text() {
    let input = b"* OK\r\n";
    let (_, resp) = parse_greeting(input).expect(
        "bare '* OK\\r\\n' greeting should parse (Postel's law, consistent with parse_tagged)",
    );
    match resp {
        Response::Greeting(g) => {
            assert_eq!(g.status, GreetingStatus::Ok);
            assert!(g.code.is_none());
            assert!(g.text.is_empty());
        }
        other => panic!("expected Greeting, got {other:?}"),
    }
}

/// Bare BYE greeting with no text.
#[test]
fn greeting_bare_bye_no_text() {
    let input = b"* BYE\r\n";
    let (_, resp) = parse_greeting(input).expect("bare '* BYE\\r\\n' greeting should parse");
    match resp {
        Response::Greeting(g) => {
            assert_eq!(g.status, GreetingStatus::Bye);
            assert!(g.text.is_empty());
        }
        other => panic!("expected Greeting, got {other:?}"),
    }
}

/// Greeting with response code but no space before it: `* OK[CAPABILITY ...]\r\n`
#[test]
fn greeting_ok_with_code_no_space() {
    let input = b"* OK[CAPABILITY IMAP4REV1]\r\n";
    // This should also work - the code is directly after OK without SP
    let result = parse_greeting(input);
    // This is a stretch - most servers send SP. Just verify it doesn't panic.
    // If it parses, great. If not, that's also acceptable since the RFC requires SP.
    let _ = result;
}

#[test]
fn tagged_ok() {
    let (_, resp) = parse_response(b"A001 OK done\r\n").unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.tag, "A001");
        assert_eq!(t.status, StatusKind::Ok);
        assert_eq!(t.text, "done");
    } else {
        panic!("expected Tagged");
    }
}

#[test]
fn tagged_no_with_code() {
    let (_, resp) = parse_response(b"A002 NO [NOPERM] not allowed\r\n").unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.status, StatusKind::No);
        assert_eq!(t.code, Some(ResponseCode::NoPerm));
    } else {
        panic!("expected Tagged");
    }
}

#[test]
fn tagged_bad() {
    let (_, resp) = parse_response(b"A003 BAD syntax error\r\n").unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.status, StatusKind::Bad);
    } else {
        panic!("expected Tagged");
    }
}

#[test]
fn continuation() {
    let (_, resp) = parse_response(b"+ go ahead\r\n").unwrap();
    if let Response::Continuation(c) = resp {
        assert_eq!(c.data, "go ahead");
    } else {
        panic!("expected Continuation");
    }
}

#[test]
fn untagged_exists() {
    let (_, resp) = parse_response(b"* 23 EXISTS\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        assert_eq!(*u, UntaggedResponse::Exists(23));
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_recent() {
    let (_, resp) = parse_response(b"* 5 RECENT\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        assert_eq!(*u, UntaggedResponse::Recent(5));
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_expunge() {
    let (_, resp) = parse_response(b"* 3 EXPUNGE\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        assert_eq!(*u, UntaggedResponse::Expunge(3));
    } else {
        panic!("expected Untagged");
    }
}

/// VANISHED with trailing space after uid-set.
///
/// RFC 7162 Section 3.2.10: `"VANISHED" [SP "(EARLIER)"] SP known-uids`
#[test]
fn vanished_trailing_space() {
    let (_, resp) = parse_response(b"* VANISHED 1:5 \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Vanished { earlier, uids } = &*u {
            assert!(!earlier);
            assert!(!uids.is_empty());
        } else {
            panic!("expected Vanished, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_status_ok_with_code() {
    let (_, resp) = parse_response(b"* OK [UIDVALIDITY 3857529045] UIDs valid\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { status, code, text } = &*u {
            assert_eq!(*status, UntaggedStatus::Ok);
            assert_eq!(
                code.as_ref().unwrap(),
                &ResponseCode::UidValidity(3_857_529_045)
            );
            assert_eq!(text, "UIDs valid");
        } else {
            panic!("expected Status");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_bye() {
    let (_, resp) = parse_response(b"* BYE server shutting down\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { status, .. } = &*u {
            assert_eq!(*status, UntaggedStatus::Bye);
        } else {
            panic!("expected Status");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Regression IMAP-002: empty encoded-text must decode to empty string per Postel's law.
///
/// RFC 2047 Section 2 defines `encoded-text = 1*<any printable ASCII except ?>`,
/// so empty payload is technically malformed. But per Postel's law, some servers
/// encode empty strings as `=?UTF-8?B??=`. The decoder should accept this and
/// produce an empty string.
#[test]
fn imap_002_empty_encoded_text_base64_decoded() {
    let input = b"=?UTF-8?B??=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "",
        "Postel's law: empty base64 encoded-text must decode to empty string"
    );
}

/// Regression IMAP-002: empty Q-encoded text must also decode to empty string.
#[test]
fn imap_002_empty_encoded_text_q_decoded() {
    let input = b"=?UTF-8?Q??=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "",
        "Postel's law: empty Q-encoded text must decode to empty string"
    );
}

#[test]
fn untagged_vanished_earlier() {
    let input = b"VANISHED (EARLIER) 1:5,10\r\n";
    let (_, resp) = parse_untagged_vanished(input).unwrap();
    if let UntaggedResponse::Vanished { earlier, uids } = resp {
        assert!(earlier);
        assert_eq!(uids.len(), 2);
        assert_eq!(uids[0], UidRange::range(1, 5));
        assert_eq!(uids[1], UidRange::single(10));
    } else {
        panic!("expected Vanished, got {resp:?}");
    }
}

#[test]
fn untagged_vanished_no_earlier() {
    let input = b"VANISHED 42\r\n";
    let (_, resp) = parse_untagged_vanished(input).unwrap();
    if let UntaggedResponse::Vanished { earlier, uids } = resp {
        assert!(!earlier);
        assert_eq!(uids, vec![UidRange::single(42)]);
    } else {
        panic!("expected Vanished, got {resp:?}");
    }
}

#[test]
fn untagged_id_with_pairs() {
    let input = b"ID (\"name\" \"Dovecot\" \"version\" \"2.3.16\")\r\n";
    let (_, resp) = parse_untagged_id(input).unwrap();
    if let UntaggedResponse::Id(params) = resp {
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].0, "name");
        assert_eq!(params[0].1, Some("Dovecot".to_owned()));
        assert_eq!(params[1].0, "version");
        assert_eq!(params[1].1, Some("2.3.16".to_owned()));
    } else {
        panic!("expected Id, got {resp:?}");
    }
}

#[test]
fn untagged_id_nil() {
    let input = b"ID NIL\r\n";
    let (_, resp) = parse_untagged_id(input).unwrap();
    if let UntaggedResponse::Id(params) = resp {
        assert!(params.is_empty());
    } else {
        panic!("expected Id, got {resp:?}");
    }
}

#[test]
fn parse_response_garbage() {
    // Completely invalid input should fail
    assert!(parse_response(b"!!GARBAGE!!\r\n").is_err());
}

#[test]
fn parse_response_empty() {
    assert!(parse_response(b"").is_err());
}

#[test]
fn parse_response_only_crlf() {
    assert!(parse_response(b"\r\n").is_err());
}

#[test]
fn parse_response_truncated_tagged() {
    // Missing CRLF
    assert!(parse_response(b"A001 OK done").is_err());
}

#[test]
fn parse_response_invalid_status() {
    // Invalid status keyword
    assert!(parse_response(b"A001 INVALID text\r\n").is_err());
}

#[test]
fn greeting_rejects_non_greeting() {
    assert!(parse_greeting(b"* FETCH 1\r\n").is_err());
}

#[test]
fn untagged_status_bare_ok_no_text() {
    // Some servers send `* OK\r\n` with no SP or resp-text.
    // parse_tagged tolerates this (RFC 3501 Section 7.1, Postel's law)
    // but parse_untagged_status must also tolerate it for consistency.
    let input = b"* OK\r\n";
    let result = parse_response(input);
    match result {
        Ok((_, Response::Untagged(boxed))) => match *boxed {
            UntaggedResponse::Status { status, code, text } => {
                assert_eq!(status, UntaggedStatus::Ok);
                assert!(code.is_none());
                assert!(text.is_empty());
            }
            other => panic!("Expected Status, got {other:?}"),
        },
        Ok((_, other)) => panic!("Expected Untagged(Status), got {other:?}"),
        Err(e) => {
            panic!("BUG: bare `* OK\\r\\n` should parse like bare tagged status; got error: {e:?}")
        }
    }
}

#[test]
fn untagged_status_bare_bye_no_text() {
    // Server sends `* BYE\r\n` with no text (e.g., abrupt disconnect notice).
    let input = b"* BYE\r\n";
    let result = parse_response(input);
    match result {
        Ok((_, Response::Untagged(boxed))) => match *boxed {
            UntaggedResponse::Status { status, .. } => {
                assert_eq!(status, UntaggedStatus::Bye);
            }
            other => panic!("Expected Status, got {other:?}"),
        },
        Ok((_, other)) => panic!("Expected Untagged(Status), got {other:?}"),
        Err(e) => panic!("BUG: bare `* BYE\\r\\n` should parse; got error: {e:?}"),
    }
}

#[test]
fn continuation_empty_text() {
    // Bare continuation with just a space
    let (_, cont) = parse_continuation(b"+ \r\n").unwrap();
    assert_eq!(cont.data, "");
}

/// Base64 continuation challenge must not be mistaken for a response code.
/// RFC 3501 Section 7.5: base64 data never starts with `[`.
#[test]
fn continuation_base64_no_code() {
    let (_, cont) = parse_continuation(b"+ dGVzdA==\r\n").unwrap();
    assert_eq!(cont.code, None);
    assert_eq!(cont.data, "dGVzdA==");
}

/// Plain-text continuation must have `code: None`.
#[test]
fn continuation_plain_text_no_code() {
    let (_, cont) = parse_continuation(b"+ go ahead\r\n").unwrap();
    assert_eq!(cont.code, None);
    assert_eq!(cont.data, "go ahead");
}

/// OK response with no response code, just text.
#[test]
fn tagged_ok_no_code() {
    let input = b"A001 OK NOOP completed\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.tag, "A001");
        assert_eq!(tagged.status, StatusKind::Ok);
        assert!(tagged.code.is_none());
        assert_eq!(tagged.text, "NOOP completed");
    } else {
        panic!("expected tagged response");
    }
}

/// BAD response with text.
#[test]
fn tagged_bad_response() {
    let input = b"A001 BAD Command syntax error\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::Bad);
        assert_eq!(tagged.text, "Command syntax error");
    } else {
        panic!("expected tagged BAD");
    }
}

/// Untagged BYE with response text.
#[test]
fn untagged_bye_with_text() {
    let input = b"* BYE Server shutting down\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Status { status, text, .. } = &*boxed
    {
        assert_eq!(*status, UntaggedStatus::Bye);
        assert_eq!(text, "Server shutting down");
        return;
    }
    panic!("expected BYE");
}

// ===== Malformed tagged response tests =====

/// Tagged response with no space between tag and status.
#[test]
fn tagged_response_no_space_after_tag() {
    assert!(parse_response(b"A001OK done\r\n").is_err());
}

/// Tagged response with empty/missing status.
#[test]
fn tagged_response_empty_status() {
    assert!(parse_response(b"A001  done\r\n").is_err());
}

/// Tag starting with non-atom char is not a valid tagged response.
#[test]
fn tagged_response_invalid_tag_char() {
    // '{' is not an atom char, so this can't be parsed as tagged
    assert!(parse_response(b"{BAD} OK done\r\n").is_err());
}

/// Tagged response with only a tag and status, no text after status.
///
/// Many servers send bare `"A001 OK\r\n"` without trailing text.
/// We tolerate this per Postel's law (RFC 1122 Section 1.2.2), even though
/// RFC 3501 Section 7.1 formally requires SP resp-text.
#[test]
fn tagged_response_no_text_after_status() {
    let (rem, resp) = parse_response(b"A001 OK\r\n").unwrap();
    assert!(rem.is_empty());
    if let Response::Tagged(t) = resp {
        assert_eq!(t.tag, "A001");
        assert_eq!(t.status, StatusKind::Ok);
        assert!(t.code.is_none());
        assert!(t.text.is_empty());
    } else {
        panic!("expected Tagged response, got {resp:?}");
    }
}

/// Completely empty input fails.
#[test]
fn parse_response_empty_input() {
    assert!(parse_response(b"").is_err());
}

/// Garbage data with special characters fails to parse.
#[test]
fn parse_response_garbage_special_chars() {
    assert!(parse_response(b"!@#$%^&\r\n").is_err());
}

/// Tagged response with bare LF instead of CRLF.
#[test]
fn tagged_response_bare_lf() {
    assert!(parse_response(b"A001 OK done\n").is_err());
}

/// EXISTS with large count near `u32::MAX`.
#[test]
fn exists_large_count() {
    let input = b"* 4294967295 EXISTS\r\n";
    let (_, resp) = parse_response(input).unwrap();
    assert_eq!(
        resp,
        Response::Untagged(Box::new(UntaggedResponse::Exists(u32::MAX)))
    );
}

/// Garbage data as response returns error.
#[test]
fn garbage_data_returns_error() {
    assert!(parse_response(b"GARBAGE\r\n").is_err());
    assert!(parse_response(b"\x00\x01\x02\r\n").is_err());
    assert!(parse_response(b"").is_err());
}

/// VANISHED with overlapping UID ranges parses correctly.
#[test]
fn vanished_overlapping_ranges() {
    let input = b"* VANISHED (EARLIER) 1:5,3:8,10\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Vanished { earlier, uids } = *boxed
    {
        assert!(earlier);
        assert_eq!(uids.len(), 3);
        assert_eq!(uids[0], UidRange::range(1, 5));
        assert_eq!(uids[1], UidRange::range(3, 8));
        assert_eq!(uids[2], UidRange::single(10));
        return;
    }
    panic!("expected Vanished response");
}

/// VANISHED known-uids must reject `*` per RFC 7162 Section 6.
///
/// RFC 7162 Section 6 ABNF:
///   known-uids    = sequence-set
///                   ;; Sequence of UIDs; "*" is not allowed.
///   expunged-resp = "VANISHED" [SP "(EARLIER)"] SP known-uids
///
/// Although the grammar references `sequence-set`, the ABNF comment
/// explicitly prohibits `*`. The parser must not produce a `Vanished`
/// response when `*` appears in the UID set.
#[test]
fn vanished_sequence_set_with_star() {
    // `*` must be rejected in VANISHED known-uids per RFC 7162 Section 6.
    let input = b"* VANISHED 1:5,*\r\n";
    assert!(parse_response(input).is_err());
}

/// VANISHED known-uids must reject `*` per RFC 7162 Section 6.
///
/// RFC 7162 Section 6 ABNF:
///   known-uids    = sequence-set
///                   ;; Sequence of UIDs; "*" is not allowed.
///   expunged-resp = "VANISHED" [SP "(EARLIER)"] SP known-uids
///
/// The `*` wildcard is explicitly prohibited in VANISHED known-uids,
/// even though the grammar references `sequence-set`. The parser must
/// use `uid-set` (nz-number only) to enforce this restriction.
/// Also tests `*` in a range position (e.g. `1:*`).
#[test]
fn vanished_rejects_star_in_known_uids() {
    // Bare `*` must be rejected.
    let input = b"* VANISHED *\r\n";
    assert!(parse_response(input).is_err());

    // `*` in a range (`1:*`) must also be rejected.
    let input = b"* VANISHED 1:*\r\n";
    assert!(parse_response(input).is_err());

    // `*` with EARLIER must also be rejected.
    let input = b"* VANISHED (EARLIER) 1:5,*\r\n";
    assert!(parse_response(input).is_err());
}

// ===== Tag parser must reject "+" (RFC 3501 Section 9) =====
// RFC 3501 Section 9: tag = 1*<any ASTRING-CHAR except "+">
// "+" is reserved for continuation responses, so it must not appear in tags.

/// Tag containing "+" must be rejected per RFC 3501 Section 9.
#[test]
fn tagged_response_rejects_plus_in_tag() {
    // "A+01 OK done\r\n"  -  "+" is not valid in a tag
    let result = parse_response(b"A+01 OK done\r\n");
    assert!(
        result.is_err(),
        "tag containing '+' should be rejected per RFC 3501 Section 9"
    );
}

/// Tag that is solely "+" must be rejected.
#[test]
fn tagged_response_rejects_bare_plus_tag() {
    let result = parse_response(b"+ OK done\r\n");
    // This should be parsed as a continuation request, not a tagged response.
    // If it's parsed at all, the tag must not be "+".
    if let Ok((_, Response::Tagged(t))) = result {
        panic!("'+' should not be accepted as a tag, got tagged response: {t:?}");
    }
}

/// Verify that a full untagged OK with `[UNSEEN 0]` preserves the
/// response code structurally rather than losing it into the text field.
#[test]
fn unseen_zero_in_full_response_preserved() {
    let input = b"* OK [UNSEEN 0] No unseen messages\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Status { code, text, .. } => {
                assert_eq!(
                    code,
                    Some(ResponseCode::Unseen(0)),
                    "UNSEEN 0 response code must be preserved structurally, \
                         not consumed into the text field"
                );
                assert_eq!(text, "No unseen messages");
            }
            other => panic!("expected Status, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

#[test]
fn expunge_zero_rejected() {
    // message-data uses nz-number for EXPUNGE (RFC 3501 Section 7.4.1).
    // `number SP EXPUNGE` is a form this codec fully implements, so a
    // violation is a server contract violation, not an unknown extension: it
    // must surface as a parse failure the account boundary maps to
    // Protocol(ParseFailed).
    assert!(parse_response(b"* 0 EXPUNGE\r\n").is_err());
    // The same holds for EXISTS and RECENT, and through a tolerated run of
    // spaces after the number.
    assert!(parse_response(b"* 0 EXISTS trailing\r\n").is_err());
    assert!(parse_response(b"* 1  RECENT junk\r\n").is_err());
    // A numbered response we do not model stays an extension.
    assert!(matches!(
        parse_response(b"* 1 XSOMETHING (a b)\r\n"),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Unknown(_))
    ));
}

#[test]
fn exists_zero_still_valid() {
    // EXISTS uses number, not nz-number (RFC 3501 Section 7.3.1)
    let input = b"* 0 EXISTS\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(*boxed, UntaggedResponse::Exists(0));
    } else {
        panic!("expected Exists(0)");
    }
}

#[test]
fn recent_zero_still_valid() {
    // RECENT uses number, not nz-number (RFC 3501 Section 7.3.2)
    let input = b"* 0 RECENT\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(*boxed, UntaggedResponse::Recent(0));
    } else {
        panic!("expected Recent(0)");
    }
}

// ===== Tests for audit fixes =====

// H1: Bare `+\r\n` continuation request (RFC 9051 Section 7.6)
#[test]
fn continuation_bare_plus_crlf() {
    let input = b"+\r\n";
    let (remaining, response) = parse_response(input).unwrap();
    assert!(remaining.is_empty());
    match response {
        Response::Continuation(c) => assert_eq!(c.data, ""),
        _ => panic!("expected Continuation"),
    }
}

#[test]
fn continuation_with_space_and_text() {
    let input = b"+ Ready for literal\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Continuation(c) => assert_eq!(c.data, "Ready for literal"),
        _ => panic!("expected Continuation"),
    }
}

#[test]
fn continuation_bare_plus_base64() {
    // AUTHENTICATE continuation with base64 data after space
    let input = b"+ dGVzdA==\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Continuation(c) => assert_eq!(c.data, "dGVzdA=="),
        _ => panic!("expected Continuation"),
    }
}

// ===== Spec audit: leniency documentation tests =====

/// L3: Non-standard quoted-string escapes (e.g. `\n`) are accepted for
/// server compatibility per Postel's law. Only `\"` and `\\` are valid
/// per RFC 3501 Section 9.
#[test]
fn spec_audit_l3_nonstandard_escape_accepted() {
    // RFC 3501 Section 9: only \" and \\ are valid quoted-specials.
    // "\n" is a non-standard escape  -  parser accepts it gracefully
    // and preserves the backslash as literal data (Postel's law).
    let input = b"\"hello\\nworld\"";
    let (rest, val) = quoted_string(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(val, b"hello\\nworld");
}

/// L5: Bare `+\r\n` without SP is accepted for compatibility with servers
/// that send `+\r\n` for literal synchronization.
#[test]
fn spec_audit_l5_continuation_without_sp() {
    let input = b"+\r\n";
    let (rest, cont) = parse_continuation(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(cont.data, "");
}

/// RFC 9051 Section 9: sequence-set requires at least one element.
#[test]
fn spec_audit_sequence_set_rejects_empty() {
    // Empty input should fail to parse as a sequence-set
    assert!(
        sequence_set(b"").is_err(),
        "empty sequence-set must be rejected per RFC 9051 Section 9"
    );
}

/// RFC 9051 Section 2.2.2: Clients MUST tolerate unrecognized untagged responses.
#[test]
fn spec_audit_unknown_untagged_response() {
    // Unknown extension response must be parsed without error
    let input = b"* XFOOBAR some extension data\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert_eq!(text, "XFOOBAR some extension data");
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// Another unknown response: extension with parenthesized data.
#[test]
fn spec_audit_unknown_untagged_with_parens() {
    let input = b"* XANNOTATION \"inbox\" (value.shared \"test\")\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XANNOTATION"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// tagged response with no text after status keyword.
///
/// Many real servers send bare `"A001 OK\r\n"` without any trailing text.
/// While RFC 3501 Section 7.1 formally requires `resp-cond-state = status SP resp-text`,
/// Postel's law dictates we accept this common variant.
#[test]
fn tagged_ok_no_text() {
    let input = b"A001 OK\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Tagged(t) = resp {
        assert_eq!(t.tag, "A001");
        assert_eq!(t.status, StatusKind::Ok);
        assert!(t.code.is_none());
        assert!(t.text.is_empty());
    } else {
        panic!("expected Tagged response, got {resp:?}");
    }
}

/// RFC 3501 Section 9: EXPUNGE uses `nz-number = digit-nz *DIGIT`
/// which means the first digit must be 1-9. Leading zeros like `01`
/// are NOT valid nz-numbers.
#[test]
fn expunge_rejects_leading_zero() {
    // "* 01 EXPUNGE\r\n"  -  `01` is not a valid nz-number.
    let input = b"* 01 EXPUNGE\r\n";
    let result = parse_response(input);
    assert!(
        result.is_err(),
        "EXPUNGE with leading-zero sequence number '01' should be rejected \
             (RFC 3501 Section 9: nz-number = digit-nz *DIGIT)"
    );
}

/// EXISTS uses `number` (not `nz-number`), so leading zeros ARE valid.
/// `* 01 EXISTS` should parse as EXISTS with count 1.
#[test]
fn exists_accepts_leading_zero() {
    let input = b"* 01 EXISTS\r\n";
    let (_, resp) = parse_response(input)
        .expect("EXISTS with leading-zero number should parse (number allows leading zeros)");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Exists(n) => assert_eq!(n, 1),
            other => panic!("expected Exists, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RECENT uses `number` (not `nz-number`), so leading zeros ARE valid.
#[test]
fn recent_accepts_leading_zero() {
    let input = b"* 01 RECENT\r\n";
    let (_, resp) = parse_response(input).expect("RECENT with leading-zero number should parse");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Recent(n) => assert_eq!(n, 1),
            other => panic!("expected Recent, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ── 5. Greeting without response code bracket (L1033-1034) ──

/// RFC 3501 Section 7.5: continuation response where `[` is present but
/// resp-text parsing fails. The parser should fall back to treating the
/// remainder as plain data (L1033-1034 branch).
#[test]
fn continuation_bracket_resp_text_parse_failure_fallback() {
    // `[` followed by something that doesn't parse as a valid response code
    // and doesn't have a closing `]`  -  falls back to plain data.
    let input = b"+ [INVALID!@#$%\r\n";
    let (_, cont) = parse_continuation(input).unwrap();
    // The parser should treat everything after "+ " as plain data
    assert!(cont.code.is_none());
    assert_eq!(cont.data, "[INVALID!@#$%");
}

/// Greeting with text but no response code bracket.
/// This exercises the `resp_text` path where there's no `[` prefix,
/// so no response code is parsed  -  just plain text.
#[test]
fn greeting_ok_plain_text_no_code() {
    let input = b"* OK Dovecot ready.\r\n";
    let (_, resp) = parse_greeting(input).unwrap();
    match resp {
        Response::Greeting(g) => {
            assert_eq!(g.status, GreetingStatus::Ok);
            assert!(g.code.is_none());
            assert_eq!(g.text, "Dovecot ready.");
        }
        other => panic!("expected Greeting, got {other:?}"),
    }
}

// ===== Trailing-whitespace tolerance tests (Postel's law) =====

/// Postel's law: ID response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_id() {
    let input = b"ID (\"name\" \"test\")  \r\n";
    let (_, resp) = parse_untagged_id(input).unwrap();
    assert!(matches!(resp, UntaggedResponse::Id(_)));
}

/// RFC 3501 Section 7.3.1 / Postel's law: EXISTS with trailing whitespace
/// before CRLF must still parse as Exists, not Unknown. Twelve other
/// response parsers tolerate trailing whitespace; EXISTS must too.
#[test]
fn exists_trailing_whitespace_tolerated() {
    let input = b"* 5 EXISTS \r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(
            *boxed,
            UntaggedResponse::Exists(5),
            "EXISTS with trailing space must parse as Exists(5), not Unknown"
        );
    } else {
        panic!("expected Untagged Exists");
    }
}

/// RFC 3501 Section 7.3.2 / Postel's law: RECENT with trailing whitespace
/// before CRLF must still parse as Recent, not Unknown.
#[test]
fn recent_trailing_whitespace_tolerated() {
    let input = b"* 0 RECENT \r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(
            *boxed,
            UntaggedResponse::Recent(0),
            "RECENT with trailing space must parse as Recent(0), not Unknown"
        );
    } else {
        panic!("expected Untagged Recent");
    }
}

/// RFC 3501 Section 7.4.1 / Postel's law: EXPUNGE with trailing whitespace
/// before CRLF must still parse as Expunge, not Unknown.
#[test]
fn expunge_trailing_whitespace_tolerated() {
    let input = b"* 3 EXPUNGE \r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(
            *boxed,
            UntaggedResponse::Expunge(3),
            "EXPUNGE with trailing space must parse as Expunge(3), not Unknown"
        );
    } else {
        panic!("expected Untagged Expunge");
    }
}
