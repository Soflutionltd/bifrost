use super::*;

// ── flag_fetch_list tests (lock down behaviour before refactoring) ──

/// `flag_list` (used for both FLAGS and FETCH FLAGS contexts) must filter
/// `\*`  -  RFC 3501 Section 9: `flag-fetch = flag / "\Recent"`.
#[test]
fn flag_list_in_fetch_context_filters_wildcard() {
    let (_, flags) = flag_list(b"(\\Seen \\*)").unwrap();
    assert_eq!(flags.len(), 1, "\\* must be filtered from flag_list");
    assert_eq!(flags[0], Flag::Seen);
}

/// `flag_list` must retain `\Recent`  -  it is valid in both FLAGS
/// and FETCH FLAGS contexts (RFC 3501 Section 9: `flag-fetch = flag / "\Recent"`).
#[test]
fn flag_list_in_fetch_context_retains_recent() {
    let (_, flags) = flag_list(b"(\\Seen \\Recent \\Flagged)").unwrap();
    assert_eq!(flags.len(), 3, "\\Recent must be retained in flag_list");
    assert!(
        flags.contains(&Flag::Recent),
        "\\Recent was filtered from flag_list"
    );
}

/// `flag_list` parses standard flags in FETCH FLAGS context.
#[test]
fn flag_list_in_fetch_context_basic() {
    let (_, flags) = flag_list(b"(\\Seen \\Answered \\Deleted)").unwrap();
    assert_eq!(flags.len(), 3);
    assert_eq!(flags[0], Flag::Seen);
    assert_eq!(flags[1], Flag::Answered);
    assert_eq!(flags[2], Flag::Deleted);
}

/// `flag_list` handles empty list in FETCH FLAGS context.
#[test]
fn flag_list_in_fetch_context_empty() {
    let (_, flags) = flag_list(b"()").unwrap();
    assert!(flags.is_empty());
}

// ===== Envelope + FETCH tests =====

#[test]
fn address_simple() {
    let (_, addr) = address(
        b"(\"Alice\" NIL \"alice\" \"example.com\")",
        crate::types::wire::WireMode::ModifiedUtf7,
    )
    .unwrap();
    assert_eq!(addr.name.as_deref(), Some("Alice"));
    assert_eq!(addr.mailbox.as_deref(), Some("alice"));
    assert_eq!(addr.host.as_deref(), Some("example.com"));
}

#[test]
fn address_all_nil() {
    let (_, addr) = address(
        b"(NIL NIL NIL NIL)",
        crate::types::wire::WireMode::ModifiedUtf7,
    )
    .unwrap();
    assert!(addr.name.is_none());
    assert!(addr.mailbox.is_none());
    assert!(addr.host.is_none());
}

#[test]
fn address_rfc2047_name() {
    let (_, addr) = address(
        b"(\"=?UTF-8?B?QWxpY2U=?=\" NIL \"alice\" \"example.com\")",
        crate::types::wire::WireMode::ModifiedUtf7,
    )
    .unwrap();
    assert_eq!(addr.name.as_deref(), Some("Alice"));
}

#[test]
fn address_list_nil() {
    let (_, list) = address_list(b"NIL", crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(list.is_empty());
}

#[test]
fn address_list_multi() {
    let (_, list) = address_list(
        b"((\"A\" NIL \"a\" \"x.com\")(\"B\" NIL \"b\" \"y.com\"))",
        crate::types::wire::WireMode::ModifiedUtf7,
    )
    .unwrap();
    assert_eq!(list.len(), 2);
}

#[test]
fn envelope_full() {
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test Subject\" \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            ((\"Recipient\" NIL \"rcpt\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg-id@example.com>\")";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(env.subject.as_deref(), Some("Test Subject"));
    assert_eq!(env.from.len(), 1);
    assert_eq!(env.to.len(), 1);
    assert_eq!(env.message_id.as_deref(), Some("<msg-id@example.com>"));
}

#[test]
fn envelope_all_nil() {
    let input = b"(NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(env.date.is_none());
    assert!(env.subject.is_none());
    assert!(env.from.is_empty());
}

/// RFC 3501 Section 7.4.2: "If the Sender or Reply-To lines are absent
/// in the [RFC-2822] header, or are present but empty, the server sets
/// the corresponding member of the envelope to be the same value as the
/// from member (the client is not expected to know to do this)."
///
/// When a server sends NIL for sender/reply-to but populates from,
/// the parser defaults them to from per the RFC.
#[test]
fn envelope_nil_sender_reply_to_preserved() {
    // from is populated, but sender and reply-to are NIL
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            NIL \
            NIL \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();

    // RFC 3501 Section 7.4.2: sender and reply_to default to from when NIL
    assert_eq!(env.from.len(), 1);
    assert_eq!(
        env.sender, env.from,
        "sender must default to from when NIL (RFC 3501 Section 7.4.2)"
    );
    assert_eq!(
        env.reply_to, env.from,
        "reply_to must default to from when NIL (RFC 3501 Section 7.4.2)"
    );
}

/// RFC 3501 Section 7.4.2: "If the Sender or Reply-To lines are absent
/// in the [RFC-2822] header, or are present but empty, the server sets
/// the corresponding member of the envelope to be the same value as the
/// from member (the client is not expected to know to do this)."
///
/// Full `parse_response` round-trip: sender and reply-to default to from
/// when the server sends NIL.
#[test]
fn envelope_preserves_nil_sender_reply_to() {
    // Envelope with from=(alice@ex.com), sender=NIL, reply-to=NIL
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024 00:00:00 +0000\" \"Test\" ((\"Alice\" NIL \"alice\" \"ex.com\")) NIL NIL ((\"Bob\" NIL \"bob\" \"ex.com\")) NIL NIL NIL \"<msg@ex.com>\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let env = f.envelope.as_ref().unwrap();
            assert_eq!(env.from.len(), 1, "from should have one address");
            // RFC 3501 Section 7.4.2: sender and reply_to default to from when NIL
            assert_eq!(
                env.sender, env.from,
                "sender must default to from when NIL (RFC 3501 Section 7.4.2); got {:?}",
                env.sender
            );
            assert_eq!(
                env.reply_to, env.from,
                "reply_to must default to from when NIL (RFC 3501 Section 7.4.2); got {:?}",
                env.reply_to
            );
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// When sender/reply-to are explicitly provided (non-NIL), they should
/// NOT be overridden with the from value.
#[test]
fn envelope_explicit_sender_reply_to_preserved() {
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            ((\"Secretary\" NIL \"secretary\" \"example.com\")) \
            ((\"ReplyAddr\" NIL \"reply\" \"example.com\")) \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();

    assert_eq!(env.sender.len(), 1);
    assert_eq!(env.sender[0].mailbox.as_deref(), Some("secretary"));
    assert_eq!(env.reply_to.len(), 1);
    assert_eq!(env.reply_to[0].mailbox.as_deref(), Some("reply"));
}

#[test]
fn fetch_uid_flags() {
    let input = b"(UID 42 FLAGS (\\Seen \\Flagged))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(42));
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
    assert!(flags.contains(&Flag::Flagged));
}

#[test]
fn fetch_rfc822_size() {
    let input = b"(RFC822.SIZE 1234)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.rfc822_size, Some(1234));
}

#[test]
fn fetch_with_envelope() {
    let input = b"(UID 10 ENVELOPE (NIL \"Hello\" \
            ((\"Test\" NIL \"test\" \"example.com\")) \
            NIL NIL NIL NIL NIL NIL \
            \"<msg@example.com>\"))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(10));
    let env = fr.envelope.unwrap();
    assert_eq!(env.subject.as_deref(), Some("Hello"));
}

#[test]
fn fetch_modseq() {
    let input = b"(UID 5 MODSEQ (12345))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.mod_seq.map(crate::types::ModSeqValzer::get), Some(12345));
}

#[test]
fn fetch_body_section() {
    let input = b"(BODY[] \"hello\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "");
    assert_eq!(fr.body_sections[0].data.as_deref(), Some(b"hello".as_ref()));
}

#[test]
fn full_fetch_response() {
    let input = b"* 1 FETCH (UID 100 FLAGS (\\Seen) RFC822.SIZE 500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(fr.seq, 1);
            assert_eq!(fr.uid, Some(100));
            assert_eq!(fr.rfc822_size, Some(500));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn status_item_highestmodseq() {
    let (_, items) = status_items(b"HIGHESTMODSEQ 99999").unwrap();
    assert_eq!(items, vec![StatusItem::HighestModSeq(99999)]);
}

// ===== FETCH with INTERNALDATE =====

#[test]
fn fetch_internaldate() {
    let input = b"(UID 42 INTERNALDATE \"17-Jul-1996 02:44:25 -0700\")\r\n";
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 42 INTERNALDATE \"17-Jul-1996 02:44:25 -0700\")\r\n")
            .unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.uid, Some(42));
            assert_eq!(
                fr.internal_date.as_deref(),
                Some("17-Jul-1996 02:44:25 -0700")
            );
        } else {
            panic!("expected Fetch, got {boxed:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
    // Also test standalone parser
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(
        fr.internal_date.as_deref(),
        Some("17-Jul-1996 02:44:25 -0700")
    );
}

/// RFC 3501 Section 7.4.2: INTERNALDATE is formally `date-time` (always
/// quoted), but some servers send NIL for malformed/draft messages.
/// The parser must handle NIL gracefully rather than failing the entire
/// FETCH response  -  losing UID, FLAGS, ENVELOPE, etc. would be far worse
/// than having `internal_date = None`.
#[test]
fn fetch_internaldate_nil_accepted() {
    let input = b"* 5 FETCH (UID 100 FLAGS (\\Seen) INTERNALDATE NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.uid, Some(100));
                assert_eq!(fr.flags.as_ref().map(std::vec::Vec::len), Some(1));
                assert_eq!(
                    fr.internal_date, None,
                    "INTERNALDATE NIL must parse as None, not fail the FETCH response"
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Verify that INTERNALDATE NIL doesn't cause loss of subsequent FETCH
/// attributes. The UID and FLAGS after INTERNALDATE must still be parsed.
#[test]
fn fetch_internaldate_nil_preserves_other_attrs() {
    let input = b"* 1 FETCH (INTERNALDATE NIL UID 42 RFC822.SIZE 1024)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.internal_date, None);
                assert_eq!(fr.uid, Some(42));
                assert_eq!(fr.rfc822_size, Some(1024));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== FETCH variant tests =====

#[test]
fn fetch_body_header() {
    let input = b"(BODY[HEADER] \"Subject: Test\\r\\n\\r\\n\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "HEADER");
    assert!(fr.body_sections[0].data.is_some());
}

#[test]
fn fetch_body_text() {
    let input = b"(BODY[TEXT] \"message body here\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "TEXT");
    assert_eq!(
        fr.body_sections[0].data.as_deref(),
        Some(b"message body here".as_ref())
    );
}

#[test]
fn fetch_body_header_fields() {
    let input = b"(BODY[HEADER.FIELDS (Subject From)] \"Subject: Hi\\r\\nFrom: a@b\\r\\n\\r\\n\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "HEADER.FIELDS (Subject From)");
}

#[test]
fn fetch_body_part_number() {
    let input = b"(BODY[1.2] \"part data\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "1.2");
    assert_eq!(
        fr.body_sections[0].data.as_deref(),
        Some(b"part data".as_ref())
    );
}

#[test]
fn fetch_body_partial_origin() {
    let input = b"(BODY[]<0> \"first 100 bytes\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "");
    assert_eq!(fr.body_sections[0].origin, Some(0));
}

#[test]
fn fetch_body_nil_data() {
    let input = b"(BODY[TEXT] NIL)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "TEXT");
    assert!(fr.body_sections[0].data.is_none());
}

#[test]
fn fetch_multiple_body_sections() {
    let input = b"(BODY[HEADER] \"hdr\" BODY[TEXT] \"body\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 2);
    assert_eq!(fr.body_sections[0].section, "HEADER");
    assert_eq!(fr.body_sections[1].section, "TEXT");
}

#[test]
fn fetch_body_with_literal() {
    let input = b"(BODY[] {5}\r\nhello)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].data.as_deref(), Some(b"hello".as_ref()));
}

#[test]
fn fetch_all_items_combined() {
    let input = b"(UID 99 FLAGS (\\Seen) RFC822.SIZE 2048 \
            INTERNALDATE \"01-Jan-2024 00:00:00 +0000\" \
            BODY[HEADER] \"From: test\\r\\n\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(99));
    assert_eq!(fr.rfc822_size, Some(2048));
    assert_eq!(
        fr.internal_date.as_deref(),
        Some("01-Jan-2024 00:00:00 +0000")
    );
    assert_eq!(fr.body_sections.len(), 1);
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
}

#[test]
fn fetch_unknown_attribute_skipped() {
    // Future attributes should be skipped gracefully
    let input = b"(UID 1 FUTUREATTR \"value\" FLAGS (\\Seen))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(1));
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
}

// ===== BINARY extension tests (RFC 3516) =====

#[test]
fn fetch_binary_simple_section() {
    // BINARY[1] with quoted data (RFC 3516 Section 4.2)
    let input = b"(BINARY[1] \"binary data\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert_eq!(fr.binary_sections[0].origin, None);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"binary data".as_ref())
    );
}

#[test]
fn fetch_binary_nested_section() {
    // BINARY[1.2.3] with literal data (RFC 3516 Section 4.2)
    let input = b"(BINARY[1.2.3] {5}\r\nhello)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1, 2, 3]);
    assert_eq!(fr.binary_sections[0].origin, None);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"hello".as_ref())
    );
}

#[test]
fn fetch_binary_with_origin() {
    // BINARY[2]<0>  -  partial binary fetch with origin offset (RFC 3516 Section 4.2)
    let input = b"(BINARY[2]<0> \"partial\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![2]);
    assert_eq!(fr.binary_sections[0].origin, Some(0));
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"partial".as_ref())
    );
}

#[test]
fn fetch_binary_nil_data() {
    // BINARY[1] NIL  -  server returns NIL for absent data (RFC 3516 Section 4.2)
    let input = b"(BINARY[1] NIL)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert!(fr.binary_sections[0].data.is_none());
}

#[test]
fn fetch_binary_size() {
    // BINARY.SIZE[1] number (RFC 3516 Section 4.3)
    let input = b"(BINARY.SIZE[1] 2048)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![1], 2048));
}

#[test]
fn fetch_binary_size_nested() {
    // BINARY.SIZE[1.2]  -  nested section (RFC 3516 Section 4.3)
    let input = b"(BINARY.SIZE[1.2] 512)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![1, 2], 512));
}

#[test]
fn fetch_binary_mixed_with_other_attrs() {
    // BINARY and BINARY.SIZE alongside regular FETCH attributes
    let input = b"(UID 42 BINARY[1] \"data\" BINARY.SIZE[2] 1024 FLAGS (\\Seen))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(42));
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"data".as_ref())
    );
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![2], 1024));
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
}

#[test]
fn fetch_multiple_binary_sections() {
    // Multiple BINARY sections in one response (RFC 3516 Section 4.2)
    let input = b"(BINARY[1] \"part1\" BINARY[2] \"part2\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sections.len(), 2);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"part1".as_ref())
    );
    assert_eq!(fr.binary_sections[1].section, vec![2]);
    assert_eq!(
        fr.binary_sections[1].data.as_deref(),
        Some(b"part2".as_ref())
    );
}

#[test]
fn fetch_binary_empty_section() {
    // RFC 9051 Section 9: section-binary = "[" [section-part] "]"
    // Empty section-part is valid and produces an empty section vec.
    let input = b"(BINARY[] \"all\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert!(
        fr.binary_sections[0].section.is_empty(),
        "RFC 9051: empty section-part must produce empty vec"
    );
    assert_eq!(fr.binary_sections[0].data.as_deref(), Some(b"all".as_ref()));
}

#[test]
fn fetch_binary_size_empty_section() {
    // RFC 9051 Section 9: section-binary = "[" [section-part] "]"
    // Empty section-part is valid and produces an empty section vec.
    let input = b"(BINARY.SIZE[] 4096)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    let (ref section, size) = fr.binary_sizes[0];
    assert!(section.is_empty(), "RFC 9051: empty section-part");
    assert_eq!(size, 4096);
}

#[test]
fn binary_size_number64() {
    // RFC 9051 Section 9 formal syntax specifies `number` (u32) for
    // BINARY.SIZE, but the parser accepts number64 as a Postel's-law
    // leniency. This test verifies values exceeding u32::MAX parse correctly.
    let input = b"(BINARY.SIZE[1] 5000000000)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![1], 5_000_000_000u64));
}

#[test]
fn parse_fetch_binary_empty_section_rfc9051() {
    // RFC 9051 Section 9: section-binary = "[" [section-part] "]"
    // Empty section-part must be accepted.
    let input = b"* 1 FETCH (BINARY[] ~{5}\r\nhello)\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(inner) = resp {
        if let UntaggedResponse::Fetch(fetch) = *inner {
            assert_eq!(fetch.binary_sections.len(), 1);
            assert!(
                fetch.binary_sections[0].section.is_empty(),
                "RFC 9051: empty section-part must produce empty vec"
            );
            assert_eq!(
                fetch.binary_sections[0].data.as_deref(),
                Some(b"hello".as_slice())
            );
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn parse_fetch_binary_size_empty_section_rfc9051() {
    // RFC 9051 Section 9: BINARY.SIZE with empty section-part
    let input = b"* 1 FETCH (BINARY.SIZE[] 4096)\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(inner) = resp {
        if let UntaggedResponse::Fetch(fetch) = *inner {
            assert_eq!(fetch.binary_sizes.len(), 1);
            let (ref section, size) = fetch.binary_sizes[0];
            assert!(section.is_empty(), "RFC 9051: empty section-part");
            assert_eq!(size, 4096);
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_fetch_missing_closing_paren() {
    // Mismatched parens: the FETCH parser fails, so it falls through
    // to the Unknown catch-all (RFC 9051 Section 2.2.2).
    let result = parse_response(b"* 1 FETCH (UID 42\r\n");
    match result {
        Ok((_, Response::Untagged(boxed))) => {
            assert!(
                matches!(*boxed, UntaggedResponse::Unknown(_)),
                "Malformed FETCH should fall through to Unknown, got {boxed:?}"
            );
        }
        Ok((_, other)) => panic!("Expected Untagged(Unknown), got {other:?}"),
        Err(_) => { /* also acceptable if parser rejects outright */ }
    }
}

// ===== Incomplete->Error in complete-mode parsers =====
//
// nom's `alt()` only catches `Error`, not `Incomplete`. Returning
// `Incomplete` from a complete-mode parser prevents fallthrough to
// the `parse_untagged_unknown` catch-all (RFC 9051 Section 2.2.2).

#[test]
fn quoted_string_incomplete_in_envelope_is_a_parse_failure() {
    // A FETCH response whose ENVELOPE has an unterminated quoted string.
    // The quoted_string parser must return Error (not Incomplete); ENVELOPE's
    // closed ten-field prefix then promotes it to the connection-fatal
    // recognized-response path instead of the Unknown fallback.
    let input = b"* 1 FETCH (ENVELOPE (\"unterminated subject))\r\n";
    let result = parse_response(input);
    assert!(!matches!(result, Err(nom::Err::Incomplete(_))));
    assert!(result.is_err());
}

#[test]
fn envelope_truncated() {
    // Envelope missing most fields
    let result = envelope(
        b"(\"date\" \"subject\"",
        crate::types::wire::WireMode::ModifiedUtf7,
    );
    assert!(result.is_err());
}

#[test]
fn skip_unknown_fetch_attribute() {
    // Unknown attributes should be skipped without error
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 10 X-CUSTOM \"value\" FLAGS (\\Seen))\r\n").unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.uid, Some(10));
            assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn a_raw_crlf_in_a_quoted_fetch_value_does_not_swallow_the_next_response() {
    // End to end: the unknown-attribute skip path must stop at the response
    // terminator, leaving `* 2 EXISTS` for the next parse rather than
    // consuming it as part of the malformed first response.
    let input = b"* 1 FETCH (X-JUNK (\"value\r\n* 2 EXISTS\r\n\"))\r\n";
    let (rest, _) = parse_response(input).expect("first response parses or errors, not both");
    assert!(
        rest.starts_with(b"* 2 EXISTS\r\n"),
        "scanner consumed past the response terminator: {:?}",
        String::from_utf8_lossy(rest)
    );
}

// ===== FETCH with SAVEDATE (RFC 8514) =====

#[test]
fn fetch_savedate_quoted() {
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 42 SAVEDATE \"28-Dec-2023 10:30:00 +0000\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.save_date.as_deref(), Some("28-Dec-2023 10:30:00 +0000"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_savedate_nil() {
    let (_, resp) = parse_response(b"* 5 FETCH (UID 100 SAVEDATE NIL)\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(100));
        assert!(fr.save_date.is_none());
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_savedate_with_internaldate() {
    let (_, resp) = parse_response(
            b"* 3 FETCH (UID 7 INTERNALDATE \"01-Jan-2024 00:00:00 +0000\" SAVEDATE \"15-Feb-2024 12:00:00 +0000\")\r\n",
        )
        .unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(7));
        assert_eq!(
            fr.internal_date.as_deref(),
            Some("01-Jan-2024 00:00:00 +0000")
        );
        assert_eq!(fr.save_date.as_deref(), Some("15-Feb-2024 12:00:00 +0000"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_savedate_case_insensitive() {
    let (_, resp) =
        parse_response(b"* 1 FETCH (savedate \"01-Jan-2025 00:00:00 +0000\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.save_date.as_deref(), Some("01-Jan-2025 00:00:00 +0000"));
        return;
    }
    panic!("expected Fetch response");
}

// ===== OBJECTID (RFC 8474) parser tests =====

#[test]
fn fetch_emailid() {
    let input = b"* 1 FETCH (UID 42 EMAILID (M6d99ac3275826486))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.email_id.as_deref(), Some("M6d99ac3275826486"));
        return;
    }
    panic!("expected Fetch response with EMAILID");
}

#[test]
fn fetch_threadid() {
    let input = b"* 1 FETCH (UID 42 THREADID (T64b478a75b7ea9fd))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.thread_id.as_deref(), Some("T64b478a75b7ea9fd"));
        return;
    }
    panic!("expected Fetch response with THREADID");
}

#[test]
fn fetch_threadid_nil() {
    let input = b"* 1 FETCH (UID 42 THREADID NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert!(fr.thread_id.is_none());
        return;
    }
    panic!("expected Fetch response with THREADID NIL");
}

#[test]
fn fetch_emailid_and_threadid_combined() {
    let input = b"* 5 FETCH (UID 100 FLAGS (\\Seen) EMAILID (Mabcdef1234567890) THREADID (T0987654321fedcba))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.seq, 5);
        assert_eq!(fr.uid, Some(100));
        assert_eq!(fr.email_id.as_deref(), Some("Mabcdef1234567890"));
        assert_eq!(fr.thread_id.as_deref(), Some("T0987654321fedcba"));
        assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        return;
    }
    panic!("expected Fetch response with EMAILID and THREADID");
}

#[test]
fn fetch_gmail_msgid_and_thrid() {
    let input =
        b"* 5 FETCH (UID 100 X-GM-MSGID 1278455344230334864 X-GM-THRID 1278455344230334865)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.seq, 5);
        assert_eq!(fr.uid, Some(100));
        assert_eq!(
            fr.gmail_msg_id,
            Some(crate::types::GmailMessageId::new(1_278_455_344_230_334_864))
        );
        assert_eq!(
            fr.gmail_thread_id,
            Some(crate::types::GmailThreadId::new(1_278_455_344_230_334_865))
        );
        return;
    }
    panic!("expected Fetch response with Gmail message and thread ids");
}

#[test]
fn fetch_emailid_case_insensitive() {
    let input = b"* 1 FETCH (emailid (Mabc123))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some("Mabc123"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_threadid_nil_case_insensitive() {
    let input = b"* 1 FETCH (threadid nil)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert!(fr.thread_id.is_none());
        return;
    }
    panic!("expected Fetch response");
}

/// RFC 8474 Section 7: EMAILID is always `(objectid)`, never NIL.
/// Only THREADID has the `nil` alternative in the formal syntax.
/// `EMAILID NIL` must not produce a valid `FetchResponse` with `email_id` = None.
#[test]
fn fetch_emailid_nil_rejected() {
    let input = b"* 1 FETCH (EMAILID NIL)\r\n";
    let result = parse_response(input);
    // The parser may return an error or fall through to Unknown  -
    // either is acceptable. What matters is that it does NOT produce
    // a FetchResponse with email_id silently set to None.
    match result {
        Err(_) => {} // parse error  -  correct
        Ok((_, Response::Untagged(boxed))) => {
            if let UntaggedResponse::Fetch(fr) = *boxed {
                panic!(
                    "EMAILID NIL must not be silently accepted as FetchResponse \
                         per RFC 8474 Section 7, got: {fr:?}"
                );
            }
            // Fell through to Unknown or other non-Fetch variant  -  acceptable.
        }
        Ok((_, other)) => {
            // Tagged or continuation  -  fine, not a Fetch.
            let _ = other;
        }
    }
}

/// RFC 8474 Section 7: EMAILID with valid `(objectid)` must still parse.
#[test]
fn fetch_emailid_valid_objectid() {
    let input = b"* 1 FETCH (EMAILID (V001))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some("V001"));
        return;
    }
    panic!("expected Fetch response with EMAILID (V001)");
}

/// Regression: EMAILID FETCH response with a >255-char objectid must parse
/// successfully. Previously, the objectid parser truncated at 255 characters
/// and returned the excess to the input stream, causing the closing ')'
/// parse to fail (RFC 8474 Section 7, Postel's law).
#[test]
fn fetch_emailid_oversized_objectid_parses() {
    let long_id: String = "A".repeat(300);
    let input = format!("* 1 FETCH (EMAILID ({long_id}))\r\n");
    let (_, resp) = parse_response(input.as_bytes()).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some(long_id.as_str()));
        return;
    }
    panic!("expected Fetch response with EMAILID");
}

/// EMAILID FETCH response with a valid objectid per RFC 8474 Section 7.
#[test]
fn fetch_emailid_rfc8474_valid_objectid() {
    let input = b"* 1 FETCH (EMAILID (M_abc-XYZ_123))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some("M_abc-XYZ_123"));
        return;
    }
    panic!("expected Fetch response with EMAILID");
}

/// THREADID FETCH response with a valid objectid per RFC 8474 Section 7.
#[test]
fn fetch_threadid_rfc8474_valid_objectid() {
    let input = b"* 1 FETCH (THREADID (T_thread-99))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.thread_id.as_deref(), Some("T_thread-99"));
        return;
    }
    panic!("expected Fetch response with THREADID");
}

/// RFC 5464 Section 5: value = nstring / literal8
/// literal8 can contain arbitrary binary data including non-UTF-8 bytes.
/// MetadataEntry.value must be `Option<Vec<u8>>` to preserve binary fidelity.
#[test]
fn parse_metadata_binary_literal8_value() {
    let input = b"* METADATA \"INBOX\" (\"/private/vendor/bin\" ~{4}\r\n\x80\x81\x82\x83)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/vendor/bin");
        assert_eq!(
            entries[0].value.as_deref(),
            Some(b"\x80\x81\x82\x83".as_slice())
        );
        return;
    }
    panic!("expected Metadata response with binary value");
}

/// FETCH with BODY section partial origin (RFC 3501 Section 7.4.2).
#[test]
fn fetch_body_partial_with_origin() {
    let input = b"* 1 FETCH (BODY[]<0> {10}\r\n0123456789)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        assert_eq!(fetch.body_sections.len(), 1);
        let sec = &fetch.body_sections[0];
        assert_eq!(sec.origin, Some(0));
        assert_eq!(sec.data.as_deref(), Some(b"0123456789".as_slice()));
        return;
    }
    panic!("expected FETCH with partial origin");
}

/// Truncated envelope should fail gracefully.
#[test]
fn envelope_truncated_fails() {
    // Only date and subject, then end of input
    let input = b"(\"Mon, 01 Jan 2024\" \"Subject\"";
    assert!(envelope(input, crate::types::wire::WireMode::ModifiedUtf7).is_err());
}

// ===== Address group syntax tests (RFC 3501 Section 7.4.2 / RFC 5322 Section 3.4) =====

/// Group start marker: host NIL, mailbox holds group name.
#[test]
fn address_group_start() {
    let input = b"(NIL NIL \"Friends\" NIL)";
    let (_, addr) = address(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(addr.is_group_start());
    assert!(!addr.is_group_end());
    assert_eq!(addr.mailbox.as_deref(), Some("Friends"));
    assert!(addr.host.is_none());
}

/// Group end marker: both host and mailbox NIL.
#[test]
fn address_group_end() {
    let input = b"(NIL NIL NIL NIL)";
    let (_, addr) = address(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(addr.is_group_end());
    assert!(!addr.is_group_start());
}

/// Address list with group markers wrapping real addresses.
#[test]
fn address_list_with_group() {
    // RFC 3501 Section 7.4.2: group is encoded as start marker, members, end marker.
    let input =
        b"((NIL NIL \"Team\" NIL)(\"Alice\" NIL \"alice\" \"example.com\")(NIL NIL NIL NIL))";
    let (_, addrs) = address_list(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(addrs.len(), 3);
    assert!(addrs[0].is_group_start());
    assert_eq!(addrs[0].mailbox.as_deref(), Some("Team"));
    assert!(addrs[1].is_address());
    assert_eq!(addrs[1].email(), Some("alice@example.com".into()));
    assert!(addrs[2].is_group_end());
}

// ===== FETCH attribute case insensitivity tests =====

/// FETCH attributes in lowercase (RFC 3501 Section 7.4.2).
#[test]
fn fetch_lowercase_attributes() {
    let input = b"(uid 42 flags (\\Seen) rfc822.size 1024)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(42));
    assert_eq!(fr.flags, Some(vec![Flag::Seen]));
    assert_eq!(fr.rfc822_size, Some(1024));
}

/// FETCH attributes in mixed case.
#[test]
fn fetch_mixed_case_attributes() {
    let input = b"(Uid 99 Flags (\\Deleted \\Draft) Rfc822.Size 2048)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(99));
    assert_eq!(fr.flags, Some(vec![Flag::Deleted, Flag::Draft]));
    assert_eq!(fr.rfc822_size, Some(2048));
}

/// MODSEQ in lowercase.
#[test]
fn fetch_lowercase_modseq() {
    let input = b"(modseq (12345))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.mod_seq.map(crate::types::ModSeqValzer::get), Some(12345));
}

/// INTERNALDATE in lowercase.
#[test]
fn fetch_lowercase_internaldate() {
    let input = b"(internaldate \"17-Jul-1996 02:44:25 -0700\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(
        fr.internal_date.as_deref(),
        Some("17-Jul-1996 02:44:25 -0700")
    );
}

// ===== Multiple unknown FETCH attributes =====

/// Multiple consecutive unknown attributes should be skipped gracefully.
#[test]
fn fetch_multiple_unknown_attributes_skipped() {
    let input = b"(UID 42 X-CUSTOM1 \"value1\" X-CUSTOM2 (nested data) FLAGS (\\Seen))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(42));
    assert_eq!(fr.flags, Some(vec![Flag::Seen]));
}

/// Unknown attribute with NIL value.
#[test]
fn fetch_unknown_attribute_nil_value() {
    let input = b"(UID 10 X-UNKNOWN NIL FLAGS ())";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(10));
    assert_eq!(fr.flags, Some(vec![]));
}

/// Unknown attribute with literal value.
#[test]
fn fetch_unknown_attribute_literal_value() {
    let input = b"(UID 7 X-DATA {5}\r\nhello FLAGS (\\Flagged))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(7));
    assert_eq!(fr.flags, Some(vec![Flag::Flagged]));
}

// ===== Task 6: Binary section parser tests (RFC 3516) =====

/// `binary_section_spec` with a simple section [1] (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_simple() {
    let (rest, parts) = binary_section_spec(b"[1]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1]);
}

/// `binary_section_spec` with nested path [1.2.3] (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_nested() {
    let (rest, parts) = binary_section_spec(b"[1.2.3]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2, 3]);
}

/// `binary_section_spec` with empty section `[]` (RFC 9051 Section 9).
///
/// RFC 9051 Section 9: `section-binary = "[" [section-part] "]"`  -  the
/// section-part is optional, so `[]` is valid and yields an empty vec.
#[test]
fn binary_section_spec_empty() {
    let (rest, parts) = binary_section_spec(b"[]").unwrap();
    assert!(rest.is_empty());
    assert!(parts.is_empty());
}

/// `binary_section_spec` with deeply nested path (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_deep() {
    let (rest, parts) = binary_section_spec(b"[1.2.3.4.5]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2, 3, 4, 5]);
}

/// `binary_section_spec` with truncated input should fail (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_truncated() {
    // Missing closing bracket
    assert!(binary_section_spec(b"[1.2").is_err());
    // Missing opening bracket
    assert!(binary_section_spec(b"1]").is_err());
}

/// `binary_section_with_origin` with origin offset (RFC 3516 Section 4.2).
#[test]
fn binary_section_with_origin_offset() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[1]<100>").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1]);
    assert_eq!(origin, Some(100));
}

/// `binary_section_with_origin` without origin (RFC 3516 Section 4.2).
#[test]
fn binary_section_with_origin_no_origin() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[1.2]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2]);
    assert!(origin.is_none());
}

/// `binary_section_with_origin` with zero offset (RFC 3516 Section 4.2).
#[test]
fn binary_section_with_origin_zero() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[2]<0>").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![2]);
    assert_eq!(origin, Some(0));
}

/// `binary_section_with_origin` with empty section `[]` (RFC 9051 Section 9).
///
/// RFC 9051 Section 9: `section-binary = "[" [section-part] "]"`  -  the
/// section-part is optional, so `[]<500>` is valid.
#[test]
fn binary_section_with_origin_empty_section() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[]<500>").unwrap();
    assert!(rest.is_empty());
    assert!(parts.is_empty());
    assert_eq!(origin, Some(500));
}

/// BINARY.SIZE via FETCH integration (RFC 3516 Section 4.3).
#[test]
fn fetch_binary_size_via_parse_response() {
    let input = b"* 1 FETCH (BINARY.SIZE[1] 4096)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.binary_sizes.len(), 1);
        assert_eq!(fr.binary_sizes[0], (vec![1], 4096));
        return;
    }
    panic!("expected BINARY.SIZE via FETCH");
}

// ===== Additional SAVEDATE (RFC 8514) coverage =====

/// SAVEDATE in a full `parse_response` round-trip (RFC 8514 Section 3).
#[test]
fn fetch_savedate_via_parse_response() {
    let input = b"* 3 FETCH (UID 10 SAVEDATE \"15-Mar-2026 12:00:00 +0000\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(10));
        assert_eq!(fr.save_date.as_deref(), Some("15-Mar-2026 12:00:00 +0000"));
        return;
    }
    panic!("expected Fetch with SAVEDATE");
}

/// SAVEDATE combined with other FETCH attributes (RFC 8514 Section 3).
#[test]
fn fetch_savedate_with_flags_and_uid() {
    let input = b"* 1 FETCH (UID 42 FLAGS (\\Seen) SAVEDATE \"01-Jan-2025 00:00:00 +0000\" RFC822.SIZE 1234)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        assert_eq!(fr.save_date.as_deref(), Some("01-Jan-2025 00:00:00 +0000"));
        assert_eq!(fr.rfc822_size, Some(1234));
        return;
    }
    panic!("expected Fetch with SAVEDATE and other attrs");
}

/// SAVEDATE with unusual timezone offset (RFC 8514 Section 3).
#[test]
fn fetch_savedate_unusual_timezone() {
    let input = b"* 1 FETCH (SAVEDATE \"31-Dec-2099 23:59:59 -1200\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.save_date.as_deref(), Some("31-Dec-2099 23:59:59 -1200"));
        return;
    }
    panic!("expected Fetch with SAVEDATE");
}

/// Large number at `u32` boundary in FETCH UID.
#[test]
fn fetch_uid_u32_max() {
    let input = b"* 1 FETCH (UID 4294967295)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(u32::MAX));
        return;
    }
    panic!("expected Fetch with max UID");
}

/// FETCH with unknown attribute is gracefully skipped (stress variant).
#[test]
fn fetch_unknown_attribute_skipped_stress() {
    let input = b"* 1 FETCH (UID 42 X-CUSTOM-ATTR \"some value\" FLAGS (\\Seen))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        return;
    }
    panic!("expected Fetch with unknown attr skipped");
}

/// Malformed FETCH with truncated input returns error (not infinite loop).
#[test]
fn fetch_truncated_returns_error() {
    // Missing closing paren and CRLF
    assert!(parse_response(b"* 1 FETCH (UID 42").is_err());
}

/// Binary section with large offset value.
#[test]
fn fetch_binary_large_origin() {
    let input = b"* 1 FETCH (BINARY[1]<4294967295> NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.binary_sections.len(), 1);
        assert_eq!(fr.binary_sections[0].origin, Some(u64::from(u32::MAX)));
        assert!(fr.binary_sections[0].data.is_none());
        return;
    }
    panic!("expected Fetch with large binary origin");
}

/// CONDSTORE MODSEQ with `i64::MAX` value (RFC 9051 number64 limit).
#[test]
fn fetch_modseq_i64_max() {
    let input = b"* 1 FETCH (MODSEQ (9223372036854775807))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(
            fr.mod_seq.map(crate::types::ModSeqValzer::get),
            Some(i64::MAX as u64)
        );
        return;
    }
    panic!("expected Fetch with max MODSEQ");
}

/// CONDSTORE MODSEQ with `u64::MAX` must be rejected (exceeds number64 range).
/// The malformed MODSEQ is skipped gracefully; the FETCH response is still
/// parsed with `mod_seq` = None (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn fetch_modseq_u64_max_rejected() {
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

/// Verify that a full untagged OK with `[HIGHESTMODSEQ 0]` preserves the
/// response code structurally rather than losing it into the text field.
#[test]
fn highestmodseq_zero_in_full_response_preserved() {
    let input = b"* OK [HIGHESTMODSEQ 0] Strstrumpf\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Status { code, text, .. } => {
                assert_eq!(
                    code,
                    Some(ResponseCode::HighestModSeq(0)),
                    "HIGHESTMODSEQ 0 response code must be preserved structurally, \
                         not consumed into the text field"
                );
                assert_eq!(text, "Strstrumpf");
            }
            other => panic!("expected Status, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== RFC 6532/6855 UTF-8 mode tests =====

/// `utf8_mode=true` skips RFC 2047 decoding on envelope subject.
#[test]
fn envelope_utf8_mode_subject_passthrough() {
    // Subject contains a literal `=?...?=` that should NOT be decoded in utf8_mode
    let input = b"(\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"To\" NIL \"t\" \"x.com\")) \
            NIL NIL NIL \"<id@x.com>\")";
    let (_, env) = envelope(input, crate::types::wire::WireMode::Utf8).unwrap();
    // In utf8_mode the raw string is preserved, not decoded
    assert_eq!(env.subject.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
}

/// `utf8_mode=false` still applies RFC 2047 decoding on envelope subject.
#[test]
fn envelope_non_utf8_mode_subject_decoded() {
    let input = b"(\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"To\" NIL \"t\" \"x.com\")) \
            NIL NIL NIL \"<id@x.com>\")";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(env.subject.as_deref(), Some("Alice"));
}

/// `utf8_mode=true` skips RFC 2047 decoding on address display name.
#[test]
fn address_utf8_mode_name_passthrough() {
    let (_, addr) = address(
        b"(\"=?UTF-8?B?QWxpY2U=?=\" NIL \"alice\" \"example.com\")",
        crate::types::wire::WireMode::Utf8,
    )
    .unwrap();
    // In utf8_mode the raw string is preserved
    assert_eq!(addr.name.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
}

/// `utf8_mode=false` still decodes RFC 2047 in address display name.
#[test]
fn address_non_utf8_mode_name_decoded() {
    let (_, addr) = address(
        b"(\"=?UTF-8?B?QWxpY2U=?=\" NIL \"alice\" \"example.com\")",
        crate::types::wire::WireMode::ModifiedUtf7,
    )
    .unwrap();
    assert_eq!(addr.name.as_deref(), Some("Alice"));
}

/// `utf8_mode=true` with raw UTF-8 display name (RFC 6532 Section 3).
#[test]
fn address_utf8_mode_raw_utf8_name() {
    // Raw UTF-8 name: "日本太郎"
    let raw_name = "日本太郎";
    let input = format!("(\"{raw_name}\" NIL \"taro\" \"example.jp\")");
    let (_, addr) = address(input.as_bytes(), crate::types::wire::WireMode::Utf8).unwrap();
    assert_eq!(addr.name.as_deref(), Some("日本太郎"));
}

/// `utf8_mode` threads through `parse_response_utf8` to FETCH ENVELOPE.
#[test]
fn parse_response_utf8_fetch_envelope() {
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            NIL NIL NIL NIL \"<id@x.com>\"))\r\n";
    // utf8_mode=true: no RFC 2047 decoding
    let (_, resp) = parse_response_utf8(input, crate::types::wire::WireMode::Utf8).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = *u {
            let env = fr.envelope.unwrap();
            assert_eq!(env.subject.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
            assert_eq!(env.from[0].name.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// literal8 in a FETCH BODY[] context.
#[test]
fn fetch_literal8_body_section() {
    let body = b"Hello world";
    let input = format!(
        "(BODY[] ~{{{}}}\r\n{})",
        body.len(),
        std::str::from_utf8(body).unwrap()
    );
    let (_, fr) =
        fetch_response_inner(input.as_bytes(), crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].data.as_deref(), Some(&body[..]));
}

/// literal8 in a FETCH ENVELOPE subject field.
#[test]
fn fetch_envelope_literal8_subject() {
    let subject = "テスト件名";
    let lit = format!("~{{{}}}\r\n{}", subject.len(), subject);
    let input = format!(
        "(ENVELOPE (\"date\" {lit} \
            ((\"From\" NIL \"f\" \"x.com\")) \
            ((\"From\" NIL \"f\" \"x.com\")) \
            ((\"From\" NIL \"f\" \"x.com\")) \
            NIL NIL NIL NIL \"<id@x.com>\"))"
    );
    let (_, fr) =
        fetch_response_inner(input.as_bytes(), crate::types::wire::WireMode::Utf8).unwrap();
    let env = fr.envelope.unwrap();
    assert_eq!(env.subject.as_deref(), Some("テスト件名"));
}

// ===== Postel's law: accept empty "()" from non-conformant servers =====
// RFC 3501 Section 9: env-from = "(" 1*address ")" / nil  -  strictly requires
// at least one address, but non-conformant servers send "()" instead of NIL.
// Accepted per Postel's law (RFC 1122 Section 1.2.2).

#[test]
fn address_list_accepts_empty_parens() {
    // "()"  -  accepted per Postel's law even though RFC 3501 requires 1*address
    let (rest, addrs) = address_list(b"()", crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(rest.is_empty());
    assert!(addrs.is_empty());
}

#[test]
fn address_list_accepts_nil() {
    // NIL is the valid way to express "no addresses"
    let (rest, addrs) = address_list(b"NIL", crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(rest.is_empty());
    assert!(addrs.is_empty());
}

// ===== SEARCH with MODSEQ (RFC 7162 Section 3.1.5) =====
// When the client searches with a MODSEQ criterion and gets results,
// the server MUST append (MODSEQ <n>) to the SEARCH response.

/// SEARCH response with trailing (MODSEQ n) must parse (RFC 7162 Section 3.1.5).
#[test]
fn search_with_modseq_trailing() {
    // RFC 7162 Section 3.1.5 example:
    // * SEARCH 2 5 6 7 11 12 18 19 20 23 (MODSEQ 917162500)
    let input = b"* SEARCH 2 5 6 7 11 12 18 19 20 23 (MODSEQ 917162500)\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "SEARCH with trailing (MODSEQ n) should parse per RFC 7162 Section 3.1.5, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = *u
        {
            assert_eq!(uids, vec![2, 5, 6, 7, 11, 12, 18, 19, 20, 23]);
            assert_eq!(
                mod_seq,
                Some(crate::types::ModSeq::new(917_162_500).expect("valid protocol test value"))
            );
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected untagged response");
    }
}

/// SEARCH MODSEQ value of 0 must be rejected (RFC 7162 Section 3.1.6).
///
/// `search-sort-mod-seq = "(" "MODSEQ" SP mod-sequence-value ")"` where
/// `mod-sequence-value` must be >= 1. Only `mod-sequence-valzer` allows 0,
/// and that is used only in UNCHANGEDSINCE and STATUS HIGHESTMODSEQ contexts.
#[test]
fn search_modseq_rejects_zero() {
    let input = b"* SEARCH 1 5 (MODSEQ 0)\r\n";
    let (_, resp) = parse_response(input)
        .expect("SEARCH with MODSEQ 0 must still parse  -  UIDs must be preserved");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = &*u
        {
            assert_eq!(*uids, vec![1, 5], "UIDs must be preserved");
            assert_eq!(
                *mod_seq, None,
                "SEARCH MODSEQ 0 must be rejected per RFC 7162 Section 3.1.6"
            );
        } else {
            panic!("expected Search, got {u:?}  -  must not fall through to Unknown");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// SEARCH MODSEQ value of 1 must be accepted (RFC 7162 Section 3.1.6).
#[test]
fn search_modseq_accepts_one() {
    let input = b"* SEARCH 1 5 (MODSEQ 1)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = *u
        {
            assert_eq!(uids, vec![1, 5]);
            assert_eq!(
                mod_seq,
                Some(crate::types::ModSeq::new(1).expect("valid protocol test value")),
                "SEARCH MODSEQ 1 must be accepted per RFC 7162 Section 3.1.6"
            );
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected untagged response");
    }
}

/// ESEARCH response with MODSEQ result option (RFC 7162 Section 3.1.10).
#[test]
fn esearch_with_modseq_result() {
    // RFC 7162 Section 3.1.10: ESEARCH MUST return MODSEQ result option
    let input = b"ESEARCH (TAG \"A002\") UID ALL 2,10:15 MODSEQ 917162500\r\n";
    let result = parse_untagged_esearch(input);
    assert!(
        result.is_ok(),
        "ESEARCH with MODSEQ result should parse per RFC 7162 Section 3.1.10, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Esearch(esearch) = resp {
        assert_eq!(
            esearch.mod_seq.map(crate::types::ModSeq::get),
            Some(917_162_500),
            "ESEARCH MODSEQ value must be retained per RFC 7162 Section 3.1.10"
        );
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

#[test]
fn fetch_attr_atom_stops_at_open_bracket() {
    let (rest, val) = fetch_attr_atom(b"BODY[TEXT] rest").unwrap();
    assert_eq!(val, b"BODY");
    assert_eq!(rest, b"[TEXT] rest");
}

#[test]
fn fetch_attr_atom_no_bracket() {
    let (rest, val) = fetch_attr_atom(b"FLAGS rest").unwrap();
    assert_eq!(val, b"FLAGS");
    assert_eq!(rest, b" rest");
}

#[test]
fn malformed_fetch_uid_zero_is_parse_failure() {
    // uniqueid = nz-number per RFC 3501 Section 9.
    let input = b"* 1 FETCH (UID 0)\r\n";
    assert!(parse_response(input).is_err());
}

#[test]
fn malformed_closed_grammar_fetch_attributes_are_parse_failures() {
    // Every attribute whose `msg-att` grammar is closed must surface a parse
    // failure rather than being laundered into `Unknown`, which would drop the
    // whole FETCH silently while the sync cursor advanced past it.
    for input in [
        &b"* 1 FETCH (FLAGS nope)\r\n"[..],
        b"* 1 FETCH (RFC822.SIZE huge)\r\n",
        b"* 1 FETCH (RFC822.TEXT 12)\r\n",
        b"* 1 FETCH (INTERNALDATE 5)\r\n",
        b"* 1 FETCH (SAVEDATE 5)\r\n",
        b"* 1 FETCH (PREVIEW 5)\r\n",
        b"* 1 FETCH (MODSEQ 12345)\r\n",
        b"* 1 FETCH (EMAILID M00000001)\r\n",
        b"* 1 FETCH (THREADID T1)\r\n",
        b"* 1 FETCH (X-GM-MSGID nope)\r\n",
        b"* 1 FETCH (X-GM-THRID nope)\r\n",
        b"* 1 FETCH (BINARY.SIZE[1] nope)\r\n",
        b"* 1 FETCH (BODY[TEXT] 12)\r\n",
    ] {
        assert!(
            parse_response(input).is_err(),
            "expected a parse failure for {:?}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn unmodelled_and_open_ended_fetch_attributes_stay_tolerated() {
    // Syntax this parser does not model must never cost the connection.
    for input in [
        // Unrecognized attribute with a well-formed value: parsed, skipped.
        &b"* 1 FETCH (UID 7 X-FUTURE-THING (a b c))\r\n"[..],
        b"* 1 FETCH (UID 7 X-FUTURE-THING \"opaque\")\r\n",
    ] {
        assert!(
            matches!(
                parse_response(input),
                Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Fetch(_))
            ),
            "expected a tolerated FETCH for {:?}",
            String::from_utf8_lossy(input)
        );
    }

    // Valid fixed prefixes followed by a tail this parser does not model stay
    // tolerant. The tests use an unsupported field after a valid basic body
    // prefix (top-level and nested inside a multipart child) and an
    // address-list shape that is balanced but not typed.
    for input in [
        &b"* 1 FETCH (ENVELOPE (\"date\" \"subject\" (NIL) NIL NIL NIL NIL NIL NIL NIL))\r\n"[..],
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL (bad)))\r\n",
        b"* 1 FETCH (BODYSTRUCTURE ((\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL (bad)) \"MIXED\"))\r\n",
    ] {
        assert!(
            matches!(
                parse_response(input),
                Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Unknown(_))
            ),
            "expected tolerated degradation for {:?}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn malformed_open_ended_fetch_fixed_prefixes_are_parse_failures() {
    // ENVELOPE has ten mandatory outer fields. BODYSTRUCTURE and bare BODY
    // share the single-part fixed prefix: media type/subtype, params, id,
    // description, encoding, and size. A malformed prefix is a provider
    // contract violation, not an extension-tail modelling gap.
    for input in [
        &b"* 1 FETCH (ENVELOPE (\"date\"))\r\n"[..],
        b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\"))\r\n",
        b"* 1 FETCH (BODY (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" huge))\r\n",
        b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\")))\r\n",
        // A multipart child is a body too: its fixed-arity prefix is checked
        // recursively, so an arity violation cannot hide one level down (or
        // deeper) behind a balanced group.
        b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\") \"MIXED\"))\r\n",
        b"* 1 FETCH (BODYSTRUCTURE (((\"TEXT\") \"ALTERNATIVE\") \"MIXED\"))\r\n",
        // Truncation of the outer structure: the fixed prefix parses, but the
        // body never closes. An open-ended extension tail is open in its
        // contents, not in its framing, so requiring the balanced close costs
        // no server tolerance.
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000\r\n",
        b"* 1 FETCH (BODY (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL (\"INLINE\"\r\n",
        b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) \"MIXED\"\r\n",
    ] {
        assert!(
            parse_response(input).is_err(),
            "expected a parse failure for {:?}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn fetch_seq_zero_rejected() {
    // message-data uses nz-number for FETCH (RFC 3501 Section 7.4.2). FETCH
    // is excluded from the numbered known-response guard because its msg-att
    // body is open-ended, but the sequence number itself is fully modelled:
    // zero is a contract violation, not an extension, and treating it as
    // `Unknown` would silently drop the whole FETCH.
    let input = b"* 0 FETCH (UID 1 FLAGS (\\Seen))\r\n";
    assert!(parse_response(input).is_err());
    // Control: a zero-numbered response with an unmodelled keyword is still
    // an extension.
    assert!(matches!(
        parse_response(b"* 0 XSOMETHING (a b)\r\n"),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Unknown(_))
    ));
}

// M1: RFC822.SIZE as u64 (RFC 9051 number64)
#[test]
fn fetch_rfc822_size_large_u64() {
    // Value exceeding u32::MAX
    let input = b"* 1 FETCH (RFC822.SIZE 5000000000)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.rfc822_size, Some(5_000_000_000u64));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// M2: RFC822, RFC822.HEADER, RFC822.TEXT FETCH items
#[test]
fn fetch_rfc822_full_message() {
    let input = b"* 1 FETCH (RFC822 \"From: a@b.com\\r\\nHello\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.body_sections.len(), 1);
            assert_eq!(fr.body_sections[0].section, "");
            assert!(fr.body_sections[0].data.is_some());
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn fetch_rfc822_header() {
    let input = b"* 1 FETCH (RFC822.HEADER \"Subject: Test\\r\\n\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.body_sections.len(), 1);
            assert_eq!(fr.body_sections[0].section, "HEADER");
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn fetch_rfc822_text() {
    let input = b"* 1 FETCH (RFC822.TEXT \"Body content\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.body_sections.len(), 1);
            assert_eq!(fr.body_sections[0].section, "TEXT");
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 7162 Section 3.1.6: SORT response with MODSEQ trailer.
/// When a MODSEQ search criterion is used, the server MUST return
/// the MODSEQ result option: `sort-data = "SORT" *(SP nz-number)
/// [SP search-sort-mod-seq]` where `search-sort-mod-seq = "("
/// "MODSEQ" SP mod-sequence-value ")"`.
#[test]
fn sort_with_modseq() {
    let (_, resp) = parse_response(b"* SORT 2 5 6 (MODSEQ 917162500)\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        match &*u {
            UntaggedResponse::Sort { nums, mod_seq } => {
                assert_eq!(nums, &[2, 5, 6]);
                assert_eq!(
                    *mod_seq,
                    Some(crate::types::ModSeq::new(917162500).expect("valid protocol test value"))
                );
            }
            other => panic!("expected Sort, got {other:?}"),
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M2: THREADID NIL must be case-insensitive.
/// RFC 8474 Section 4.
#[test]
fn audit_m2_threadid_mixed_case_nil() {
    let input = b"(THREADID Nil UID 42)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(fr.thread_id.is_none(), "THREADID Nil should parse as None");
    assert_eq!(fr.uid, Some(42));
}

/// THREADID NIL match must verify a token boundary follows "NIL".
/// Without a delimiter check, `tag_no_case(&b"NIL"[..])` would greedily match
/// the first 3 bytes of a token like "NIL2", incorrectly yielding
/// `thread_id = None` and leaving unparsed residue that gets silently
/// consumed by the unknown-attribute handler.
/// RFC 8474 Section 4 / RFC 3501 Section 9 (formal syntax: atoms are
/// delimited by SP, "(", ")", CR, and other specials).
#[test]
fn threadid_nil_requires_token_boundary() {
    // "NIL2" is not the NIL token. Without the boundary check, the parser
    // would match "NIL" from "NIL2", set thread_id = None, and the residue
    // "2 42" would be silently consumed by the unknown-attribute handler.
    //
    // With the fix, the NIL branch correctly rejects "NIL2" (no token
    // boundary after "NIL"), so the parser falls through to the
    // parenthesized "(objectid)" branch, which fails because "NIL2" does
    // not start with "(". The overall parse must therefore error.
    let input = b"(UID 5 THREADID NIL2 42)";
    assert!(
        fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).is_err(),
        "THREADID NIL2 must not be parsed as THREADID NIL: \
             'NIL2' is not the NIL token (RFC 8474 Section 4, RFC 3501 Section 9)"
    );
}

/// THREADID NIL followed by SP and another attribute must parse correctly.
/// RFC 8474 Section 4.
#[test]
fn threadid_nil_followed_by_sp_attr() {
    let input = b"(THREADID NIL UID 42)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(fr.thread_id.is_none(), "THREADID NIL should parse as None");
    assert_eq!(fr.uid, Some(42));
}

/// THREADID NIL at end of FETCH (followed by ')') must parse correctly.
/// RFC 8474 Section 4.
#[test]
fn threadid_nil_at_end_of_fetch() {
    let input = b"(UID 5 THREADID NIL)";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.uid, Some(5));
    assert!(fr.thread_id.is_none());
}

/// M5: BINARY section parts must use `nz-number`.
/// RFC 3516 Section 7.
#[test]
fn audit_m5_binary_section_rejects_zero_part() {
    assert!(binary_section_spec(b"[0]").is_err());
}

#[test]
fn audit_m5_binary_section_rejects_zero_in_dotted_path() {
    assert!(binary_section_spec(b"[1.0.3]").is_err());
}

/// `binary_section_spec` accepts empty section `[]` (RFC 9051 Section 9).
///
/// RFC 9051 Section 9: `section-binary = "[" [section-part] "]"`  -  the
/// section-part is optional, so `[]` is valid and yields an empty vec.
#[test]
fn binary_section_spec_accepts_empty() {
    let (rest, parts) = binary_section_spec(b"[]").unwrap();
    assert!(rest.is_empty());
    assert!(parts.is_empty());
}

/// `binary_section_spec` accepts a valid dotted section path (RFC 3516 Section 4).
#[test]
fn binary_section_spec_valid_dotted_path() {
    let (rest, parts) = binary_section_spec(b"[1.2.3]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2, 3]);
}

/// L6: MODSEQ in FETCH must be >= 1.
/// RFC 7162 Section 7.
/// A malformed MODSEQ (0) must be skipped gracefully, and other
/// attributes (e.g. UID) must be preserved (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn audit_l6_fetch_modseq_accepts_zero() {
    // RFC 7162 Section 3.1.3 formally says mod-sequence-value >= 1, but
    // we accept 0 per Postel's law (RFC 1122 Section 1.2.2) for
    // consistency with the STATUS HIGHESTMODSEQ parser.
    let input = b"(UID 1 MODSEQ (0))";
    let result = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7);
    let (_, fr) = result.expect("FETCH with MODSEQ (0) must parse  -  0 accepted per Postel's law");
    assert_eq!(
        fr.mod_seq.map(crate::types::ModSeqValzer::get),
        Some(0),
        "MODSEQ 0 is accepted without supplying a concurrency guard"
    );
    assert_eq!(
        fr.uid,
        Some(1),
        "UID must be preserved alongside MODSEQ (0)"
    );
}

/// L6: HIGHESTMODSEQ response code value must be >= 1.
/// RFC 7162 Section 3.1.2.
#[test]
fn audit_l6_highestmodseq_response_code_accepts_zero() {
    // RFC 7162 Section 3.1.2: formally mod-sequence-value >= 1, but
    // real servers (Cyrus, Dovecot) send [HIGHESTMODSEQ 0] for empty/new
    // mailboxes. Accepted per Postel's law to preserve structured data.
    let input = b"* OK [HIGHESTMODSEQ 0] done\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Status { code, text, .. } => {
                assert_eq!(code, Some(ResponseCode::HighestModSeq(0)));
                assert_eq!(text, "done");
            }
            other => panic!("expected Status, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// L13: EMAILID never allows NIL per RFC 8474 Section 7 formal syntax:
///   fetch-emailid-resp = "EMAILID" SP "(" objectid ")"
/// Only THREADID has the `nil` alternative. When a server sends EMAILID NIL,
/// the parser must reject it (the `(` parser fails on NIL).
#[test]
fn audit_l13_emailid_nil_rejected() {
    let input = b"(EMAILID NIL UID 42)";
    let result = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7);
    assert!(
        result.is_err(),
        "EMAILID NIL must be rejected per RFC 8474 Section 7: \
             only THREADID allows NIL, got: {result:?}"
    );
}

/// RFC 7162 Section3.1.6 / RFC 5234 Section2.3: MODSEQ keyword in search-sort-mod-seq
/// must be matched case-insensitively per ABNF rules.
#[test]
fn spec_audit_search_modseq_case_insensitive() {
    // Lowercase "modseq"  -  must still be parsed
    let input = b"* SEARCH 2 5 (modseq 917162500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Search {
                numbers: uids,
                mod_seq,
            } => {
                assert_eq!(uids, vec![2, 5]);
                assert_eq!(
                    mod_seq,
                    Some(crate::types::ModSeq::new(917162500).expect("valid protocol test value"))
                );
            }
            other => panic!("expected Search, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

#[test]
fn spec_audit_sort_modseq_case_insensitive() {
    // Mixed case "Modseq"  -  must still be parsed
    let input = b"* SORT 2 3 6 (Modseq 917162500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Sort { nums, mod_seq } => {
                assert_eq!(nums, vec![2, 3, 6]);
                assert_eq!(
                    mod_seq,
                    Some(crate::types::ModSeq::new(917162500).expect("valid protocol test value"))
                );
            }
            other => panic!("expected Sort, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== FETCH during IDLE must preserve data (RFC 2177 Section 3) =====

/// RFC 2177 Section 3: the server can send `* n FETCH (FLAGS (...))` during
/// IDLE when message attributes change.  The parser must produce a
/// `FetchResponse` with the correct sequence number and flags so the
/// `TypedEvent::FetchUpdate` variant can carry the full data to callers.
///
/// Bug fix: the IDLE handler discarded the the IDLE handler discarded the
/// `FetchResponse` and returned a unit `FlagsChanged` variant, losing the
/// sequence number and flag data.
#[test]
fn fetch_response_preserves_seq_and_flags_for_idle() {
    // Typical unsolicited FETCH the server sends during IDLE when another
    // session changes flags on message 5.
    let input = b"* 5 FETCH (FLAGS (\\Seen \\Answered) UID 300)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "parser should consume all input");

    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                // RFC 3501 Section 7.4.2: sequence number from the
                // `* n FETCH` prefix.
                assert_eq!(fr.seq, 5, "sequence number must be preserved");
                assert_eq!(fr.uid, Some(300), "UID must be preserved");

                let flags = fr.flags.as_ref().expect("FLAGS must be present");
                assert!(
                    flags.contains(&Flag::Seen),
                    "expected \\Seen in flags, got {flags:?}"
                );
                assert!(
                    flags.contains(&Flag::Answered),
                    "expected \\Answered in flags, got {flags:?}"
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 3501 Section 9: FETCH FLAGS uses `flag-fetch = flag / "\Recent"`,
/// which does NOT include `\*`.  The `\*` wildcard is only valid in
/// `flag-perm` (PERMANENTFLAGS response code).
///
/// The `flag` production itself excludes `\*` because `*` is a
/// list-wildcard (atom-special) and therefore not a valid ATOM-CHAR,
/// so `\*` does not match `flag-extension = "\" atom`.
#[test]
fn fetch_flags_must_exclude_wildcard() {
    let input = b"(FLAGS (\\Seen \\*))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    let flags = fr.flags.expect("flags must be present");
    assert!(
        !flags.contains(&Flag::Wildcard),
        "FETCH FLAGS must not accept \\*  -  \\* is only valid in \
             flag-perm (PERMANENTFLAGS), not flag-fetch (RFC 3501 Section 9). \
             Got: {flags:?}"
    );
}

/// RFC 9051 Section 7.5.2: partial fetch origins can exceed u32 for
/// messages > 4 GiB (RFC822.SIZE is number64).
#[test]
fn body_section_origin_u64() {
    let input = b"* 1 FETCH (BODY[]<5000000000> {5}\r\nhello)\r\n";
    let (_, resp) = parse_response(input).expect("should parse u64 origin");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.body_sections.len(), 1);
                assert_eq!(fr.body_sections[0].origin, Some(5_000_000_000u64));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 9051 Section 7.5.2: partial fetch origins can exceed u32.
#[test]
fn binary_section_origin_u64() {
    let input = b"* 1 FETCH (BINARY[1]<5000000000> {5}\r\nhello)\r\n";
    let (_, resp) = parse_response(input).expect("should parse u64 BINARY origin");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.binary_sections.len(), 1);
                assert_eq!(fr.binary_sections[0].origin, Some(5_000_000_000u64));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 3501 Section 7.4.2: BODY[HEADER.FIELDS (Subject)] with literal data
/// should parse correctly (baseline  -  `]` is outside the header-list parens).
#[test]
fn body_section_header_fields_literal() {
    // {15}\r\n = 15-byte literal: "Subject: Hi\r\n\r\n" (11 + 2 + 2)
    let input = b"* 1 FETCH (BODY[HEADER.FIELDS (Subject)] {15}\r\nSubject: Hi\r\n\r\n)\r\n";
    let (_, resp) = parse_response(input).expect("should parse HEADER.FIELDS with literal");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.body_sections.len(), 1);
                assert_eq!(fr.body_sections[0].section, "HEADER.FIELDS (Subject)");
                assert_eq!(
                    fr.body_sections[0].data.as_deref(),
                    Some(b"Subject: Hi\r\n\r\n".as_ref())
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 3501 Section 7.4.2: header-fld-name = astring, and
/// ASTRING-CHAR includes resp-specials = "]" (RFC 3501 Section 9).
/// A quoted header field name containing `]` must not cause the section
/// parser to stop prematurely.
#[test]
fn body_section_quoted_bracket_in_header_field_name() {
    // The `]` inside the quoted string `"X]Field"` is not the section closer.
    let input = b"(BODY[HEADER.FIELDS (\"X]Field\")] \"data\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7)
        .expect("should parse HEADER.FIELDS with quoted ] in field name");
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "HEADER.FIELDS (\"X]Field\")");
    assert_eq!(fr.body_sections[0].data.as_deref(), Some(b"data".as_ref()));
}

/// RFC 3501 Section 7.4.2: BODY[1.MIME] should parse correctly (baseline).
#[test]
fn body_section_mime() {
    let input = b"(BODY[1.MIME] \"Content-Type: text/plain\")";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "1.MIME");
    assert_eq!(
        fr.body_sections[0].data.as_deref(),
        Some(b"Content-Type: text/plain".as_ref())
    );
}

/// RFC 3501 Section 9: FETCH FLAGS uses the `flag-fetch` production
/// which includes `\Recent` (unlike `flag-list`). The FETCH FLAGS
/// response MUST preserve `\Recent`.
#[test]
fn spec_audit_fetch_flags_includes_recent() {
    let input = b"* 1 FETCH (FLAGS (\\Seen \\Recent))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        let flags = fetch.flags.as_ref().expect("FLAGS should be present");
        assert!(
            flags.contains(&Flag::Recent),
            "RFC 3501 Section 9: flag-fetch includes \\Recent, \
                 but parser excluded it: {flags:?}"
        );
        assert!(flags.contains(&Flag::Seen));
        return;
    }
    panic!("expected FETCH response");
}

/// a non-conformant `(MODSEQ 0)` in SEARCH must NOT drop
/// the search results. RFC 7162 Section 3.1.6 says mod-sequence-value
/// must be >= 1, but the parser must preserve UIDs and discard the
/// malformed MODSEQ rather than losing the entire response.
#[test]
fn regression_search_modseq_zero_preserves_uids() {
    let input = b"* SEARCH 1 5 10 (MODSEQ 0)\r\n";
    let (_, resp) = parse_response(input)
        .expect("SEARCH with malformed MODSEQ must still parse  -  UIDs must not be silently lost");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = &*u
        {
            assert_eq!(
                *uids,
                vec![1, 5, 10],
                "SEARCH UIDs must be preserved even when MODSEQ suffix is malformed"
            );
            assert_eq!(
                *mod_seq, None,
                "malformed MODSEQ should be discarded (None), not Some(0)"
            );
        } else {
            panic!(
                "expected Search response, got {u:?}  -  malformed MODSEQ must not \
                     cause fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// FETCH MODSEQ (0) must NOT drop all other attributes.
/// RFC 7162 Section 3.1.3: mod-sequence-value must be >= 1.
/// A non-conformant server sending MODSEQ (0) must NOT cause UID,
/// FLAGS, and other attributes to be silently lost.
#[test]
fn regression_fetch_modseq_zero_preserves_other_attrs() {
    // RFC 7162 Section 3.1.3 / Postel's law: MODSEQ (0) is accepted and
    // preserved as Some(0) for consistency with STATUS HIGHESTMODSEQ.
    let input = b"* 1 FETCH (UID 42 FLAGS (\\Seen) MODSEQ (0) RFC822.SIZE 1024)\r\n";
    let (_, resp) = parse_response(input)
        .expect("FETCH with MODSEQ (0) must parse  -  0 accepted per Postel's law");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(fr.seq, 1, "sequence number must be preserved");
            assert_eq!(fr.uid, Some(42), "UID must be preserved");
            assert!(
                fr.flags
                    .as_ref()
                    .is_some_and(|f| f.contains(&crate::types::Flag::Seen)),
                "FLAGS must be preserved"
            );
            assert_eq!(fr.rfc822_size, Some(1024), "RFC822.SIZE must be preserved");
            assert_eq!(
                fr.mod_seq.map(crate::types::ModSeqValzer::get),
                Some(0),
                "MODSEQ 0 is an absent concurrency guard"
            );
        } else {
            panic!("expected Fetch, got {u:?}  -  MODSEQ (0) must not drop the response");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// a non-conformant `(MODSEQ 0)` in SORT must NOT drop
/// the sort results. Same root cause as the SEARCH case.
#[test]
fn regression_sort_modseq_zero_preserves_nums() {
    let input = b"* SORT 3 1 2 (MODSEQ 0)\r\n";
    let (_, resp) = parse_response(input).expect(
        "SORT with malformed MODSEQ must still parse  -  numbers must not be silently lost",
    );
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(
                *nums,
                vec![3, 1, 2],
                "SORT numbers must be preserved even when MODSEQ suffix is malformed"
            );
            assert_eq!(
                *mod_seq, None,
                "malformed MODSEQ should be discarded (None), not Some(0)"
            );
        } else {
            panic!(
                "expected Sort response, got {u:?}  -  malformed MODSEQ must not \
                     cause fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// MODSEQ value exceeding 2^63-1 must NOT drop SEARCH results.
#[test]
fn regression_search_modseq_overflow_preserves_uids() {
    // 2^63 = 9223372036854775808, exceeds i64::MAX
    let input = b"* SEARCH 42 (MODSEQ 9223372036854775808)\r\n";
    let (_, resp) = parse_response(input).expect("SEARCH with overflowing MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = &*u
        {
            assert_eq!(*uids, vec![42]);
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ESEARCH with malformed MODSEQ 0 must preserve other results.
/// RFC 7162 Section 3.1.10: mod-sequence-value must be >= 1.
/// A non-conformant server sending MODSEQ 0 must NOT cause the entire
/// ESEARCH response (MIN, MAX, COUNT, ALL) to be silently lost.
#[test]
fn regression_esearch_modseq_zero_preserves_results() {
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 5 ALL 1:3,7,9 MODSEQ 0\r\n";
    let (_, resp) = parse_response(input).expect("ESEARCH with malformed MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.count, Some(5), "COUNT must be preserved");
            assert_eq!(
                esearch.all(),
                vec![
                    UidRange::range(1, 3),
                    UidRange::single(7),
                    UidRange::single(9)
                ],
                "ALL must be preserved"
            );
            assert!(esearch.is_uid(), "UID indicator must be preserved");
            assert_eq!(esearch.tag.as_deref(), Some("A1"), "TAG must be preserved");
        } else {
            panic!(
                "expected Esearch, got {u:?}  -  malformed MODSEQ must not cause \
                     fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ESEARCH with MODSEQ overflow must preserve other results.
#[test]
fn regression_esearch_modseq_overflow_preserves_results() {
    // 2^63 exceeds i64::MAX
    let input = b"* ESEARCH (TAG \"A2\") UID MIN 1 MAX 100 MODSEQ 9223372036854775808\r\n";
    let (_, resp) =
        parse_response(input).expect("ESEARCH with overflowing MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min(), Some(1), "MIN must be preserved");
            assert_eq!(esearch.max(), Some(100), "MAX must be preserved");
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ESEARCH with MODSEQ missing its required space must preserve other results.
/// RFC 7162 Section 3.1.10: grammar is "MODSEQ SP mod-sequence-value", but
/// a non-conformant server might omit the space (e.g., MODSEQ immediately
/// followed by CRLF). The parser must not propagate the `sp()` error and
/// silently lose all previously parsed ESEARCH data
/// (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn regression_esearch_modseq_missing_space_preserves_results() {
    // MODSEQ at end of line with no space or value  -  sp(rest) will fail.
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 5 MODSEQ\r\n";
    let (_, resp) = parse_response(input).expect("ESEARCH with space-less MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.count, Some(5), "COUNT must be preserved");
            assert!(esearch.is_uid(), "UID indicator must be preserved");
            assert_eq!(esearch.tag.as_deref(), Some("A1"), "TAG must be preserved");
            assert_eq!(
                esearch.mod_seq.map(crate::types::ModSeq::get),
                None,
                "MODSEQ should be None when space is missing"
            );
        } else {
            panic!(
                "expected Esearch, got {u:?}  -  malformed MODSEQ must not cause \
                     fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// FETCH MODSEQ with overflow value must preserve other attrs.
#[test]
fn regression_fetch_modseq_overflow_preserves_other_attrs() {
    // 2^63 exceeds i64::MAX
    let input = b"* 5 FETCH (UID 100 MODSEQ (9223372036854775808) FLAGS (\\Flagged))\r\n";
    let (_, resp) = parse_response(input).expect("FETCH with overflowing MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(fr.uid, Some(100), "UID must be preserved");
            assert!(
                fr.flags
                    .as_ref()
                    .is_some_and(|f| f.contains(&crate::types::Flag::Flagged)),
                "FLAGS must be preserved"
            );
            assert_eq!(
                fr.mod_seq.map(crate::types::ModSeqValzer::get),
                None,
                "overflowing MODSEQ should be None"
            );
        } else {
            panic!("expected Fetch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 7162 Section 3.1.3 / Postel's law: a server sending `MODSEQ (0)`
/// in a FETCH response must be accepted and yield `Some(0)` rather than
/// silently discarding the value into `None`. This matches the STATUS
/// HIGHESTMODSEQ parser which already accepts 0.
#[test]
fn fetch_modseq_zero() {
    let input = b"* 1 FETCH (MODSEQ (0))\r\n";
    let (_, resp) = parse_response(input).expect("FETCH with MODSEQ (0) must parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(
                fr.mod_seq.map(crate::types::ModSeqValzer::get),
                Some(0),
                "MODSEQ 0 is an absent concurrency guard"
            );
        } else {
            panic!("expected Fetch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 3501 Section 9: FETCH uses `nz-number`  -  leading zeros are rejected.
#[test]
fn fetch_rejects_leading_zero() {
    // "* 01 FETCH (UID 5)\r\n"  -  `01` is not a valid nz-number.
    let input = b"* 01 FETCH (UID 5)\r\n";
    let result = parse_response(input);
    assert!(
        result.is_err(),
        "FETCH with leading-zero sequence number '01' should be rejected \
             (RFC 3501 Section 9: nz-number = digit-nz *DIGIT)"
    );
}

// ===== Postel's law: accept empty "()" address list from non-conformant servers =====
// RFC 3501 Section 9: env-from = "(" 1*address ")" / nil  -  strictly requires
// at least one address. However, non-conformant servers (e.g., older Exchange,
// some webmail backends) may send "()" instead of NIL for empty address lists.
// Per Postel's law (RFC 1122 Section 1.2.2), accept gracefully.

#[test]
fn spec_audit_address_list_empty_parens() {
    // "()" should parse as an empty address list per Postel's law (RFC 1122 Section1.2.2),
    // even though RFC 3501 Section9 formally requires 1*address in the parenthesized form.
    let (rest, addrs) = address_list(b"()", crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(rest.is_empty());
    assert!(
        addrs.is_empty(),
        "empty parens should yield empty Vec<EnvelopeAddress>"
    );
}

#[test]
fn spec_audit_envelope_empty_paren_address_field() {
    // FETCH ENVELOPE where the `to` field is "()" instead of NIL.
    // Non-conformant servers (e.g., older Exchange) may send this.
    // The entire FETCH response must still parse successfully.
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024 00:00:00 +0000\" \"Subject\" ((NIL NIL \"user\" \"example.com\")) ((NIL NIL \"user\" \"example.com\")) ((NIL NIL \"user\" \"example.com\")) () NIL NIL NIL \"<msg@example.com>\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let env = f.envelope.as_ref().unwrap();
            assert_eq!(env.from.len(), 1, "from should have one address");
            assert!(
                env.to.is_empty(),
                "to should be empty (parsed from '()'), got {:?}",
                env.to
            );
            assert_eq!(
                env.message_id.as_deref(),
                Some("<msg@example.com>"),
                "message-id should be parsed correctly"
            );
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== FETCH with PREVIEW (RFC 8970) =====

#[test]
fn fetch_preview_quoted() {
    // RFC 8970 Section 3: PREVIEW data item is an nstring.
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 42 PREVIEW \"Meeting tomorrow at 3pm\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.preview.as_deref(), Some("Meeting tomorrow at 3pm"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_preview_nil() {
    // RFC 8970 Section 3: PREVIEW NIL (e.g., LAZY mode not yet computed).
    let (_, resp) = parse_response(b"* 5 FETCH (UID 100 PREVIEW NIL)\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(100));
        assert!(fr.preview.is_none());
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_preview_empty_string() {
    // RFC 8970 Section 3: PREVIEW with empty quoted string.
    let (_, resp) = parse_response(b"* 1 FETCH (PREVIEW \"\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.preview.as_deref(), Some(""));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_preview_with_other_items() {
    // PREVIEW alongside UID, FLAGS, and ENVELOPE.
    let (_, resp) =
        parse_response(b"* 3 FETCH (UID 7 FLAGS (\\Seen) PREVIEW \"Hello world\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(7));
        assert_eq!(fr.preview.as_deref(), Some("Hello world"));
        assert!(fr.flags.is_some());
        return;
    }
    panic!("expected Fetch response");
}

// ===== PREVIEW capability (RFC 8970 Section 4) =====

#[test]
fn capability_preview_parsed() {
    let (_, resp) = parse_response(b"* OK [CAPABILITY IMAP4rev1 PREVIEW] Ready\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Status {
            code: Some(ResponseCode::Capability(caps)),
            ..
        } = *boxed
    {
        assert!(
            caps.contains(&Capability::Preview),
            "PREVIEW capability must be parsed"
        );
        return;
    }
    panic!("expected OK with CAPABILITY");
}

/// RFC 3501 Section 7.4.2: envelope with NIL fields for in-reply-to
/// and message-id. These are `nstring` per the formal syntax and must
/// parse as `None`.
#[test]
fn envelope_nil_in_reply_to_and_message_id() {
    let input = b"(\"Mon, 1 Jan 2024 00:00:00 +0000\" \"Test\" \
            ((\"A\" NIL \"a\" \"x.com\")) \
            NIL NIL \
            ((\"B\" NIL \"b\" \"y.com\")) \
            NIL NIL \
            NIL NIL)";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(env.in_reply_to.is_none(), "in-reply-to NIL must be None");
    assert!(env.message_id.is_none(), "message-id NIL must be None");
    assert!(env.cc.is_empty(), "cc NIL must be empty Vec");
    assert!(env.bcc.is_empty(), "bcc NIL must be empty Vec");
}

/// RFC 3501 Section 7.4.2: envelope with all address lists as NIL.
#[test]
fn envelope_all_address_lists_nil() {
    let input = b"(NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(env.date.is_none());
    assert!(env.subject.is_none());
    assert!(env.from.is_empty());
    assert!(env.sender.is_empty());
    assert!(env.reply_to.is_empty());
    assert!(env.to.is_empty());
    assert!(env.cc.is_empty());
    assert!(env.bcc.is_empty());
    assert!(env.in_reply_to.is_none());
    assert!(env.message_id.is_none());
}

/// Regression: HIGHESTMODSEQ value exceeding `i64::MAX` should not discard the
/// entire STATUS response (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn status_highestmodseq_overflow_preserves_other_items() {
    // HIGHESTMODSEQ exceeds i64::MAX (9223372036854775808 = 2^63).
    let input = b"* STATUS INBOX (MESSAGES 10 HIGHESTMODSEQ 9223372036854775808)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            // MESSAGES 10 must be preserved.
            assert!(
                items.iter().any(|i| matches!(i, StatusItem::Messages(10))),
                "MESSAGES 10 missing: {items:?}"
            );
            // HIGHESTMODSEQ was unparseable  -  must NOT appear.
            assert!(
                !items
                    .iter()
                    .any(|i| matches!(i, StatusItem::HighestModSeq(_))),
                "overflowed HIGHESTMODSEQ should have been skipped: {items:?}"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged Status response, not Unknown");
    }
}

// ===== Edge-case bug-hunting tests =====

/// RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2: ENVELOPE with
/// NIL subject AND NIL from  -  common on drafts. The parser must not
/// panic or error; all fields should be None/empty.
#[test]
fn edge_envelope_all_nil_fields() {
    let input = b"* 1 FETCH (ENVELOPE (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let env = f.envelope.as_ref().expect("envelope should be present");
            assert!(
                env.subject.is_none(),
                "NIL subject must be None, got {:?}",
                env.subject
            );
            assert!(
                env.from.is_empty(),
                "NIL from must be empty, got {:?}",
                env.from
            );
            assert!(env.to.is_empty(), "NIL to must be empty, got {:?}", env.to);
            assert!(
                env.message_id.is_none(),
                "NIL message-id must be None, got {:?}",
                env.message_id
            );
        } else {
            panic!("expected Fetch response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 3501 Section 7.4.2: ENVELOPE with NIL date but valid subject
/// and addresses. Drafts commonly have missing dates.
#[test]
fn edge_envelope_nil_date_valid_subject() {
    let input = b"(NIL \"Draft subject\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            NIL NIL \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL NIL)";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(env.date.is_none(), "NIL date must be None");
    assert_eq!(env.subject.as_deref(), Some("Draft subject"));
    assert_eq!(env.from.len(), 1);
    assert_eq!(env.from[0].mailbox.as_deref(), Some("alice"));
}

/// ENVELOPE with RFC 2047 encoded subject that uses Q-encoding with
/// underscores. The IMAP parser should decode underscores as spaces
/// per RFC 2047 Section 4.2.
#[test]
fn edge_envelope_q_encoded_subject_underscore() {
    let input = b"(NIL \"=?UTF-8?Q?hello_world?=\" \
            ((\"Test\" NIL \"test\" \"ex.com\")) \
            NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(
        env.subject.as_deref(),
        Some("hello world"),
        "RFC 2047 Section 4.2: underscore in Q-encoding must decode to space \
             in IMAP ENVELOPE subject"
    );
}

/// RFC 3501 Section 7.4.2: "If the Sender or Reply-To lines are absent
/// in the [RFC-2822] header, or are present but empty, the server sets
/// the corresponding member of the envelope to be the same value as the
/// from member (the client is not expected to know to do this)."
///
/// Some servers violate this by sending NIL for sender/reply-to even when
/// from is populated. The parser must default sender and `reply_to` to from
/// when they are NIL, because consumer code relies on these fields being
/// populated per the RFC contract.
#[test]
fn envelope_nil_sender_reply_to_defaults_to_from() {
    // from=(Alice), sender=NIL, reply-to=NIL
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            NIL \
            NIL \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(env.from.len(), 1);
    // RFC 3501 Section 7.4.2: sender defaults to from when NIL
    assert_eq!(
        env.sender, env.from,
        "sender must default to from when server sends NIL (RFC 3501 Section 7.4.2)"
    );
    // RFC 3501 Section 7.4.2: reply_to defaults to from when NIL
    assert_eq!(
        env.reply_to, env.from,
        "reply_to must default to from when server sends NIL (RFC 3501 Section 7.4.2)"
    );
}

/// RFC 3501 Section 7.4.2: when from is also NIL (all-NIL envelope),
/// sender and `reply_to` should remain empty --- there is nothing to default to.
#[test]
fn envelope_nil_sender_reply_to_stays_empty_when_from_nil() {
    let input = b"(NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(env.from.is_empty());
    assert!(
        env.sender.is_empty(),
        "sender must remain empty when from is also NIL"
    );
    assert!(
        env.reply_to.is_empty(),
        "reply_to must remain empty when from is also NIL"
    );
}

/// RFC 3501 Section 7.4.2: when sender/reply-to are explicitly provided
/// (non-NIL), they must NOT be overridden with from.
#[test]
fn envelope_explicit_sender_reply_to_not_overridden() {
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            ((\"Secretary\" NIL \"secretary\" \"example.com\")) \
            ((\"ReplyAddr\" NIL \"reply\" \"example.com\")) \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert_eq!(env.sender.len(), 1);
    assert_eq!(env.sender[0].mailbox.as_deref(), Some("secretary"));
    assert_eq!(env.reply_to.len(), 1);
    assert_eq!(env.reply_to[0].mailbox.as_deref(), Some("reply"));
}

// ===== Edge-case bug probe tests =====

/// RFC 3501 Section 7.4.2: ENVELOPE with all ten fields set to NIL.
/// A completely empty envelope (no date, no subject, no addresses, no
/// in-reply-to, no message-id) must parse successfully through the full
/// `parse_response` path and produce an Envelope with all fields empty/None.
///
/// This exercises the `envelope()` parser when every field is NIL,
/// including all eight address lists. When from is NIL, sender and
/// reply-to must also remain empty (not default to from, since from
/// is empty  -  RFC 3501 Section 7.4.2).
#[test]
fn edge_envelope_all_nil_full_response() {
    let input = b"* 1 FETCH (ENVELOPE (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input)
        .expect("ENVELOPE with all-NIL fields must parse (RFC 3501 Section 7.4.2)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        let env = fr
            .envelope
            .expect("FETCH response should contain an envelope");
        assert!(
            env.date.is_none(),
            "date must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.subject.is_none(),
            "subject must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.from.is_empty(),
            "from must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.sender.is_empty(),
            "sender must be empty when from is also NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.reply_to.is_empty(),
            "reply_to must be empty when from is also NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.to.is_empty(),
            "to must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.cc.is_empty(),
            "cc must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.bcc.is_empty(),
            "bcc must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.in_reply_to.is_none(),
            "in_reply_to must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.message_id.is_none(),
            "message_id must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        return;
    }
    panic!("expected Untagged Fetch with Envelope");
}

/// RFC 3501 Section 7.4.2 / Postel's law (RFC 1122 Section 1.2.2):
/// FETCH response with unknown data items must be skipped gracefully.
/// The parser should preserve all recognized items (UID, FLAGS) and
/// silently skip unrecognized ones (XFUTURE, XBLOB).
#[test]
fn edge_fetch_unknown_data_items_skipped() {
    // FETCH with UID, an unknown atom-valued item, FLAGS, and an unknown
    // parenthesized-valued item. The parser must skip both unknowns and
    // still extract UID and FLAGS correctly.
    let input = b"* 1 FETCH (UID 42 XFUTURE somevalue FLAGS (\\Seen) XBLOB (nested stuff))\r\n";
    let (_, resp) =
        parse_response(input).expect("FETCH with unknown data items must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(
            fr.uid,
            Some(42),
            "UID must be preserved when unknown items are present"
        );
        let flags = fr.flags.expect("FLAGS must be preserved");
        assert_eq!(flags.len(), 1);
        assert_eq!(flags[0], Flag::Seen);
        return;
    }
    panic!("expected Untagged Fetch response");
}

/// RFC 3501 Section 9: literal with CRLF in a full FETCH BODY[] response.
/// The literal byte count includes the CRLF bytes, so a literal containing
/// `line1\r\nline2` is 12 bytes and must be preserved verbatim.
#[test]
fn edge_fetch_literal_with_crlf() {
    // BODY[] with a literal value containing embedded CRLF.
    // "line1\r\nline2" is 12 bytes.
    let input = b"* 1 FETCH (BODY[] {12}\r\nline1\r\nline2)\r\n";
    let (_, resp) = parse_response(input)
        .expect("FETCH with literal containing CRLF must parse (RFC 3501 Section 9)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.body_sections.len(), 1, "should have one body section");
        let section = &fr.body_sections[0];
        assert_eq!(
            section.data.as_deref(),
            Some(b"line1\r\nline2".as_slice()),
            "literal must preserve embedded CRLF bytes (RFC 3501 Section 9)"
        );
        return;
    }
    panic!("expected Untagged Fetch response");
}

/// Gmail's `X-GM-LABELS` FETCH data item is decoded alongside the other Gmail
/// extension attributes. The generic IMAP account layer deliberately does not
/// interpret those labels as folder membership.
#[test]
fn fetch_x_gm_labels_are_exposed_without_losing_other_attributes() {
    // Google's IMAP extension documentation shows system labels as bare
    // backslash-prefixed atoms, not quoted strings: `\` is a quoted-special
    // and so cannot appear in an `astring` atom. This is the real wire form.
    let input = b"* 1 FETCH (UID 9 X-GM-MSGID 1278455344230334865 \
                  X-GM-LABELS (\\Inbox \\Sent Important \"Muy Importante\" \"&ZeVnLIqe-\") \
                  FLAGS (\\Seen))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = *u {
            assert_eq!(fr.uid, Some(9), "UID before the unknown item survives");
            assert_eq!(
                fr.gmail_msg_id,
                Some(crate::types::GmailMessageId::new(1_278_455_344_230_334_865))
            );
            assert_eq!(
                fr.gmail_labels,
                Some(vec![
                    "\\Inbox".to_owned(),
                    "\\Sent".to_owned(),
                    "Important".to_owned(),
                    "Muy Importante".to_owned(),
                    "日本語".to_owned(),
                ]),
                "system labels arrive unquoted with a leading backslash; user \
                 labels are astrings in modified UTF-7"
            );
            assert_eq!(
                fr.flags,
                Some(vec![Flag::Seen]),
                "FLAGS after the unknown item survives"
            );
        } else {
            panic!("expected Fetch, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

#[test]
fn skip_tagged_ext_simple_nil_form_across_terminator_sets() {
    // SP is a terminator for both, so the NIL arm claims it in both.
    assert_skip_ext(b"NIL rest", Some(" rest"), Some(" rest"));
    // End of input satisfies the NIL boundary check for both (RFC 9051
    // Section 4: `tagged-ext-simple` may be the last token).
    assert_skip_ext(b"NIL", Some(""), Some(""));
    // Case-insensitive per the ABNF's literal-string rules.
    assert_skip_ext(b"nil rest", Some(" rest"), Some(" rest"));

    // `)` terminates only for STATUS. For ESEARCH the boundary check fails and
    // the atom fallback swallows the paren too - harmless there, since ESEARCH
    // values never sit inside a parenthesized list.
    assert_skip_ext(b"NIL)", Some(""), Some(")"));

    // The mirror image: CR is a declared terminator only for ESEARCH, so the
    // STATUS NIL arm's boundary check fails and the atom fallback runs. It
    // stops at the CR anyway - CR/LF end the atom scan for every caller - so
    // both sets leave the response-terminating CRLF in place.
    assert_skip_ext(b"NIL\r\n", Some("\r\n"), Some("\r\n"));

    // An atom that merely starts with NIL must not be partially consumed
    // (the boundary check exists for exactly this).
    assert_skip_ext(b"NILSIMSA rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"NILSIMSA", Some(""), Some(""));
}

#[test]
fn skip_paren_group_valid() {
    // Well-formed group should succeed.
    let (rest, content) = skip_paren_group(b"(foo \"bar\") tail").unwrap();
    assert_eq!(content, b"foo \"bar\"");
    assert_eq!(rest, b" tail");
}

#[test]
fn paren_skippers_do_not_scan_past_backslash_crlf() {
    // Each input contains an apparently matching delimiter only after a
    // response-terminating CRLF. A scanner must reject rather than accept a
    // later response as part of the malformed quoted value.
    let group = b"(\"value\\\r\n* 2 EXISTS\r\n\"next\")";
    assert!(skip_paren_group(group).is_err());
    assert!(skip_parenthesized_block(group).is_err());

    let section = b"HEADER.FIELDS (\"X\\\r\n* 2 EXISTS\r\n\"Y\") ]";
    assert!(scan_section_spec(section).is_err());
}

#[test]
fn skip_paren_group_trailing_escape_in_quote() {
    // Backslash at the very end of input inside a quoted string.
    let input = b"(\"trail\\";
    let result = skip_paren_group(input);
    assert!(result.is_err(), "trailing escape should error");
}

// -- skip_paren_group edge cases --

#[test]
fn skip_paren_group_unclosed_quote() {
    // Unclosed quoted string inside a parenthesized group must return
    // Incomplete, not panic from an out-of-bounds slice.
    let input = b"(\"unclosed)";
    let result = skip_paren_group(input);
    assert!(
        result.is_err(),
        "unclosed quote in paren group should error"
    );
}

#[test]
fn skip_paren_group_incomplete_is_an_error_not_incomplete() {
    // A FETCH BODYSTRUCTURE with unclosed parentheses in extension data.
    // skip_paren_group must return Error, never Incomplete: this codec runs
    // in complete mode, and `Incomplete` would stall the reader waiting for
    // bytes that are already all present.
    //
    // The outer body is unbalanced, so the BODYSTRUCTURE prefix gate treats
    // it as a provider contract violation rather than laundering the FETCH
    // into `Unknown` and silently dropping the message's body (C4).
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"text\" \"plain\" NIL NIL NIL \"7bit\" 42 3 NIL NIL NIL NIL (unclosed-ext\r\n";
    let result = parse_response(input);
    assert!(!matches!(result, Err(nom::Err::Incomplete(_))));
    assert!(result.is_err(), "got: {result:?}");
}

#[test]
fn scan_section_spec_incomplete_is_a_parse_failure_not_incomplete() {
    // A FETCH response with an unterminated BODY section (missing `]`).
    // `BODY[...]` is a closed grammar, so this is the server violating it:
    // the result must be a hard parse error, never `Incomplete` (which would
    // stall the reader waiting for bytes that are not coming) and never
    // `Unknown` (which would silently drop the FETCH).
    let input = b"* 1 FETCH (BODY[HEADER no-close\r\n";
    let result = parse_response(input);
    assert!(!matches!(result, Err(nom::Err::Incomplete(_))));
    assert!(result.is_err());
}
