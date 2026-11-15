use super::*;
use bifrost_types::{AccountErrorKind, Address, RequestErrorKind};

#[test]
fn draft_send_round_trips_escaped_and_encoded_display_names() {
    for name in ["a\", b", "a\\, b", "Janne \u{e4}"] {
        let address = bifrost_types::Address::named(name, "x@y");
        let wire_address = bifrost_types::mime::format_address(&address);
        let raw = format!(
            "From: {wire_address}\r\nTo: {wire_address}\r\nBcc: {wire_address}\r\nMessage-ID: <id@test>\r\n\r\nbody"
        );
        let parsed = parse_draft_for_submission(
            raw.as_bytes(),
            &bifrost_types::Address::bare("fallback@test"),
            AccountOperation::DraftSend,
        )
        .expect("rendered addresses parse");
        assert_eq!(parsed.envelope.from.name.as_deref(), Some(name));
        assert_eq!(parsed.envelope.from.address, "x@y");
        assert_eq!(parsed.envelope.recipients.len(), 2);
        for recipient in &parsed.envelope.recipients {
            assert_eq!(recipient.address, "x@y");
            assert_eq!(recipient.name.as_deref(), Some(name));
        }
        assert_eq!(parsed.message_id, "id@test");
        let body = String::from_utf8(parsed.body).expect("UTF-8 draft");
        assert!(!body.contains("Bcc:"));
        assert!(body.ends_with("\r\n\r\nbody"));
    }
}

#[test]
fn draft_send_refuses_rejected_envelope_addresses() {
    for field in ["From", "Sender", "To", "Cc", "Bcc"] {
        let raw = format!(
            "From: sender@example.test\r\nTo: recipient@example.test\r\n{field}: accepted@example.test, rejected-item\r\n\r\nbody"
        );
        let error = parse_draft_for_submission(
            raw.as_bytes(),
            &bifrost_types::Address::bare("fallback@example.test"),
            AccountOperation::DraftSend,
        )
        .expect_err("a rejected envelope address must stop draft submission");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
            "field {field}"
        );
        assert_eq!(
            error.operation(),
            Some(AccountOperation::DraftSend),
            "field {field}"
        );
    }
}

#[test]
fn draft_send_strips_bcc_into_envelope() {
    let raw = b"From: Me <me@sender.test>\r\n\
To: Ann <ann@to.test>\r\n\
Cc: cc@cc.test\r\n\
Bcc: blind1@bcc.test, Blind Two <blind2@bcc.test>\r\n\
Subject: Hi\r\n\
Message-ID: <draft-123@sender.test>\r\n\
\r\n\
body text\r\n";
    let default_from = bifrost_types::Address::bare("fallback@sender.test");
    let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
        .expect("parses");

    // Bcc addresses are folded into the envelope recipients.
    let recipients: Vec<&str> = parsed
        .envelope
        .recipients
        .iter()
        .map(|a| a.address.as_str())
        .collect();
    assert_eq!(
        recipients,
        [
            "ann@to.test",
            "cc@cc.test",
            "blind1@bcc.test",
            "blind2@bcc.test"
        ]
    );
    assert_eq!(parsed.envelope.from.address, "me@sender.test");
    assert_eq!(parsed.message_id, "draft-123@sender.test");

    // Bcc header is stripped from the transmitted body; To/Cc remain.
    let body = String::from_utf8(parsed.body).expect("utf8");
    assert!(!body.to_ascii_lowercase().contains("bcc:"));
    assert!(body.contains("To: Ann <ann@to.test>"));
    assert!(body.contains("Cc: cc@cc.test"));
    assert!(body.contains("body text"));
}

#[test]
fn raw_submissions_without_message_id_get_distinct_correlation_ids() {
    let raw = b"From: shared@example.test\r\nSender: delegate@example.test\r\nTo: target@example.test\r\n\r\nbody";
    let default_from = Address::bare("default@example.test");
    let first = parse_draft_for_submission(raw, &default_from, AccountOperation::Send)
        .expect("first submission");
    let second = parse_draft_for_submission(raw, &default_from, AccountOperation::Send)
        .expect("second submission");
    assert_eq!(first.envelope.from.address, "shared@example.test");
    assert_ne!(first.message_id, second.message_id);
    assert!(first.message_id.ends_with("@example.test"));
}

#[test]
fn draft_send_falls_back_to_default_from_and_requires_recipient() {
    let default_from = bifrost_types::Address::bare("fallback@sender.test");

    // No From header: reverse path falls back to default_from.
    let raw = b"To: ann@to.test\r\n\r\nhi\r\n";
    let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
        .expect("parses");
    assert_eq!(parsed.envelope.from.address, "fallback@sender.test");

    // No recipients at all: malformed.
    let raw = b"From: me@sender.test\r\n\r\nhi\r\n";
    let err = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
        .expect_err("no recipients");
    assert_eq!(
        *err.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
}

#[test]
fn draft_send_preserves_non_utf8_body_bytes_verbatim() {
    // An 8-bit / binary body octet (here 0xFF, invalid UTF-8) must
    // survive byte-exact in the transmitted body. The previous
    // `from_utf8_lossy` path turned it into U+FFFD (0xEF 0xBF 0xBD).
    let mut raw = b"From: me@sender.test\r\nTo: ann@to.test\r\n\r\n".to_vec();
    raw.extend_from_slice(&[0xFF, 0x00, 0xFE, b'\r', b'\n']);
    let default_from = bifrost_types::Address::bare("fallback@sender.test");
    let parsed = parse_draft_for_submission(&raw, &default_from, AccountOperation::DraftSend)
        .expect("parses");

    // Body retains the verbatim 8-bit octets; no U+FFFD substitution.
    assert!(parsed.body.ends_with(&[0xFF, 0x00, 0xFE, b'\r', b'\n']));
    assert!(!parsed.body.windows(3).any(|w| w == [0xEF, 0xBF, 0xBD]));
}

#[test]
fn draft_send_bare_lf_split_and_strip_keeps_lf_endings() {
    // A bare-LF draft (no CRLF) must split on `\n\n` and the rebuilt
    // body must not gain a stray CRLF separator (no mixed endings).
    let raw = b"From: me@sender.test\nTo: ann@to.test\nBcc: blind@bcc.test\n\nbody line\n";
    let default_from = bifrost_types::Address::bare("fallback@sender.test");
    let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
        .expect("parses");

    let body = parsed.body;
    // Bcc folded into recipients.
    assert!(
        parsed
            .envelope
            .recipients
            .iter()
            .any(|a| a.address == "blind@bcc.test")
    );
    // No CRLF anywhere: the bare-LF draft stays bare-LF.
    assert!(!body.windows(2).any(|w| w == b"\r\n"));
    // Bcc header line is gone; To survives.
    let text = String::from_utf8(body).expect("ascii");
    assert!(!text.to_ascii_lowercase().contains("bcc:"));
    assert!(text.contains("To: ann@to.test"));
    assert!(text.contains("body line"));
}

#[test]
fn draft_send_header_only_draft_has_empty_body() {
    // A header-only draft (no blank line, no body) is degenerate but
    // recoverable: the split yields an empty body and the envelope is
    // still built from the headers.
    let raw = b"From: me@sender.test\r\nTo: ann@to.test\r\n";
    let default_from = bifrost_types::Address::bare("fallback@sender.test");
    let parsed = parse_draft_for_submission(raw, &default_from, AccountOperation::DraftSend)
        .expect("parses");
    assert_eq!(parsed.envelope.recipients[0].address, "ann@to.test");
    // Header block ends with CRLF + a synthesized blank line, then no body.
    let text = String::from_utf8(parsed.body).expect("ascii");
    assert!(text.contains("To: ann@to.test"));
    assert!(text.ends_with("\r\n\r\n"));
}

#[test]
fn split_header_body_handles_mixed_and_header_only() {
    // CRLF blank line: header_end after `A: 1\r\nB: 2\r\n` (12),
    // body_start after the second `\r\n` (14).
    assert_eq!(split_header_body(b"A: 1\r\nB: 2\r\n\r\nbody"), (12, 14));
    // Bare-LF blank line: header_end after `A: 1\nB: 2\n` (10),
    // body_start after the second `\n` (11).
    assert_eq!(split_header_body(b"A: 1\nB: 2\n\nbody"), (10, 11));
    // No blank line: header-only.
    let raw = b"A: 1\r\n";
    assert_eq!(split_header_body(raw), (raw.len(), raw.len()));
}

#[test]
fn header_name_matching_is_trimmed_and_case_insensitive() {
    assert!(header_name_is(b"Bcc: x@y", b"bcc"));
    assert!(header_name_is(b"BCC:x@y", b"bcc"));
    assert!(header_name_is(b" bcc : x@y", b"bcc"));
    assert!(!header_name_is(b"Bcc-Original: x@y", b"bcc"));
    assert!(!header_name_is(b"X-Original-Bcc: x@y", b"bcc"));
    assert!(!header_name_is(b"Bcc x@y", b"bcc"), "no colon, no header");
}

fn fixed_now() -> jiff::Timestamp {
    "2026-11-05T12:00:00Z".parse().expect("timestamp")
}

#[test]
fn restamp_replaces_every_date_field_and_keeps_lookalikes() {
    let raw = b"Date: Mon, 2 Nov 2026 09:00:00 +0000\r\nTo: a@b.test\r\nDATE: Tue,\r\n 3 Nov 2026 09:00:00 +0000\r\nResent-Date: kept\r\nX-Date: kept\r\n\r\nDate: in the body\r\n";
    let out = restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("restamped");
    let text = String::from_utf8(out).expect("ascii");
    assert_eq!(
        text,
        "Date: Thu, 5 Nov 2026 12:00:00 +0000\r\nTo: a@b.test\r\nResent-Date: kept\r\nX-Date: kept\r\n\r\nDate: in the body\r\n"
    );
}

#[test]
fn restamp_inserts_a_date_into_a_draft_without_one() {
    let raw = b"To: a@b.test\nSubject: hi\n\nbody\n";
    let out = restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("restamped");
    assert_eq!(
        out,
        b"Date: Thu, 5 Nov 2026 12:00:00 +0000\nTo: a@b.test\nSubject: hi\n\nbody\n"
    );
}

#[test]
fn restamp_handles_an_unterminated_header_only_draft() {
    let raw = b"To: a@b.test\r\nDate: old";
    let out = restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("restamped");
    assert_eq!(
        out,
        b"Date: Thu, 5 Nov 2026 12:00:00 +0000\r\nTo: a@b.test\r\n\r\n"
    );
}

#[test]
fn restamp_leaves_a_dkim_signed_draft_verbatim() {
    let raw = b"DKIM-Signature: v=1; h=date:from\r\nDate: old\r\nTo: a@b.test\r\n\r\nbody";
    let out = restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend).expect("verbatim");
    assert_eq!(out, raw);
}

#[test]
fn restamp_rejects_a_leading_continuation_line() {
    let raw = b" bcc: secret@example.test\r\nTo: a@b.test\r\n\r\nbody";
    let err = restamp_draft_date(raw, fixed_now(), AccountOperation::DraftSend)
        .expect_err("orphan continuation");
    assert_eq!(
        *err.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
}

#[test]
fn strip_bcc_drops_the_folded_continuation_lines_too() {
    // A folded `Bcc:` whose continuation lines were kept would leak
    // the blind recipients as a dangling header fragment.
    let header = b"To: a@b.test\r\nBcc: x@y.test,\r\n\tz@w.test\r\nSubject: hi\r\n";
    let out = strip_bcc_header(header, b"body");
    let text = String::from_utf8(out).expect("ascii");
    assert!(!text.to_ascii_lowercase().contains("bcc"));
    assert!(
        !text.contains("z@w.test"),
        "the fold must go with its header"
    );
    assert!(text.contains("To: a@b.test"));
    assert!(text.contains("Subject: hi"));
    assert!(text.ends_with("\r\n\r\nbody"));
}

#[test]
fn draft_send_collects_folded_repeated_headers_in_order() {
    let raw = b"To: a@b.test,\r\n  c@d.test\r\nTo: e@f.test\r\n\r\nbody";
    let parsed = parse_draft_for_submission(
        raw,
        &bifrost_types::Address::bare("sender@test"),
        AccountOperation::DraftSend,
    )
    .expect("parses");
    let addresses: Vec<_> = parsed
        .envelope
        .recipients
        .iter()
        .map(|address| address.address.as_str())
        .collect();
    assert_eq!(addresses, ["a@b.test", "c@d.test", "e@f.test"]);
}

#[test]
fn parse_address_list_respects_quoted_commas() {
    // A display name this crate's own assembler quotes (`"Last, First"`)
    // must not split into a phantom recipient.
    let parsed = bifrost_types::Address::parse_list("\"Doe, Jane\" <jane@to.test>, bob@to.test");
    let addrs: Vec<&str> = parsed.iter().map(|a| a.address.as_str()).collect();
    assert_eq!(addrs, ["jane@to.test", "bob@to.test"]);
    assert_eq!(parsed[0].name.as_deref(), Some("Doe, Jane"));
}
