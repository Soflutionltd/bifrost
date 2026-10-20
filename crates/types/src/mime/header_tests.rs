use super::*;

fn headers(raw: &[u8]) -> HeaderMap {
    split_headers(raw, &MimeLimits::default(), &mut Vec::new()).0
}

#[test]
fn header_unfolds_continuation_lines() {
    assert_eq!(
        headers(b"Subject: first\r\n second\r\n\r\nbody").get("subject"),
        Some("first second")
    );
}

#[test]
fn header_lookup_is_case_insensitive() {
    assert_eq!(headers(b"X-Test: one\r\n\r\n").get("X-TEST"), Some("one"));
}

#[test]
fn header_get_all_preserves_wire_order() {
    assert_eq!(
        headers(b"X-Test: one\r\nx-test: two\r\n\r\n")
            .get_all("x-test")
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
}

#[test]
fn header_addresses_parse_display_name_and_group() {
    let parsed = headers(b"To: Team: Ada <ada@example.test>, \"B, C\" <bc@example.test>;\r\n\r\n");
    let addresses = parsed.addresses("to");
    assert_eq!(addresses.len(), 2);
    assert_eq!(addresses[0].address, "ada@example.test");
    assert_eq!(addresses[1].name.as_deref(), Some("B, C"));
}

#[test]
fn header_addresses_ignore_comma_inside_quotes() {
    let parsed = headers(b"To: \"Smith, John\" <john@test>, jane@test\r\n\r\n");
    let addresses = parsed.addresses("to");
    assert_eq!(addresses.len(), 2);
    assert_eq!(addresses[0].name.as_deref(), Some("Smith, John"));
}

#[test]
fn header_addresses_decode_encoded_word_display_name() {
    let parsed = headers(b"To: =?UTF-8?Q?Doe=2C_Jane?= <jane@test>\r\n\r\n");
    assert_eq!(parsed.addresses("to")[0].name.as_deref(), Some("Doe, Jane"));
}

#[test]
fn header_date_parses_rfc5322_and_obsolete_forms() {
    // Four-digit year with a numeric zone, then the obsolete alphabetic zone
    // and a missing day-of-week comma, which real archives both carry.
    assert_eq!(
        headers(b"Date: Thu, 1 Jan 1970 00:00:00 +0000\r\n\r\n").date("date"),
        Some(std::time::UNIX_EPOCH)
    );
    assert!(
        headers(b"Date: 21 Nov 1997 09:55:06 GMT\r\n\r\n")
            .date("date")
            .is_some()
    );
    assert!(
        headers(b"Date: Fri 21 Nov 1997 09:55 EST\r\n\r\n")
            .date("date")
            .is_some()
    );
    assert!(headers(b"Date: not a date\r\n\r\n").date("date").is_none());
}

#[test]
fn header_date_two_digit_year_windows_correctly() {
    assert_eq!(
        headers(b"Date: Fri, 21 Nov 97 09:55:06 +0000\r\n\r\n").date("date"),
        headers(b"Date: Fri, 21 Nov 1997 09:55:06 +0000\r\n\r\n").date("date")
    );
    assert_eq!(
        headers(b"Date: Sat, 1 Jan 05 00:00:00 +0000\r\n\r\n").date("date"),
        headers(b"Date: Sat, 1 Jan 2005 00:00:00 +0000\r\n\r\n").date("date")
    );
}

#[test]
fn header_message_ids_strip_angle_brackets() {
    let parsed = headers(b"References: <a@test>\r\n <b@test>\r\nIn-Reply-To: <b@test>\r\n\r\n");
    assert_eq!(parsed.message_ids("references"), ["a@test", "b@test"]);
    assert_eq!(parsed.message_ids("in-reply-to"), ["b@test"]);
}

#[test]
fn header_block_over_limit_records_defect() {
    let mut raw = b"Subject: ".to_vec();
    raw.extend(std::iter::repeat_n(b'x', 4096));
    raw.extend_from_slice(b"\r\n\r\nbody");
    let mut defects = Vec::new();
    let limits = MimeLimits {
        max_header_bytes: 64,
        ..MimeLimits::default()
    };
    let (parsed, body) = split_headers(&raw, &limits, &mut defects);
    assert!(defects.contains(&Defect::HeaderBlockTooLarge));
    assert!(
        parsed
            .get("subject")
            .is_some_and(|value| value.len() < 4096)
    );
    // The body survives a capped header block; it used to be discarded.
    assert_eq!(body, b"body");
}

#[test]
fn header_missing_separator_header_like_input_is_all_headers() {
    let mut defects = Vec::new();
    let (parsed, body) = split_headers(
        b"Subject: truncated fetch",
        &MimeLimits::default(),
        &mut defects,
    );
    assert!(defects.contains(&Defect::MissingHeaderSeparator));
    assert_eq!(parsed.get("subject"), Some("truncated fetch"));
    assert!(body.is_empty());
}

#[test]
fn header_missing_separator_bare_body_input_is_all_body() {
    let mut defects = Vec::new();
    let (parsed, body) = split_headers(
        b"just some prose\r\nwith no headers",
        &MimeLimits::default(),
        &mut defects,
    );
    assert!(defects.contains(&Defect::MissingHeaderSeparator));
    assert!(parsed.is_empty());
    assert_eq!(body, b"just some prose\r\nwith no headers");
}

#[test]
fn address_list_round_trips_rendered_quoted_pairs_and_encoded_words() {
    for name in ["a\", b", "a\\, b", "a\\\", b", "Doe, Jane", "Janne \u{e4}"] {
        let address = Address::named(name, "x@y");
        let value = crate::mime::format_address(&address);
        let parsed = Address::parse_list(&value);
        assert_eq!(parsed.len(), 1, "{value}");
        assert_eq!(parsed[0].address, address.address);
        assert_eq!(parsed[0].name, address.name);
    }
}

#[test]
fn standalone_header_block_unfolds_and_preserves_repeated_addresses() {
    let parsed = HeaderMap::parse(
        b"To: a@b.test,\r\n  c@d.test\r\ntO: e@f.test\r\nMessage-ID: <id@test>\r\n",
    );
    let addresses: Vec<_> = parsed
        .addresses("TO")
        .into_iter()
        .map(|address| address.address)
        .collect();
    assert_eq!(addresses, ["a@b.test", "c@d.test", "e@f.test"]);
    assert_eq!(parsed.message_ids("message-id"), ["id@test"]);
}

#[test]
fn address_split_keeps_commas_inside_angle_brackets() {
    assert_eq!(split_address_items("<a@b, c>, d@e"), ["<a@b, c>", " d@e"]);
}

#[test]
fn address_group_colons_respect_quoted_names_comments_and_angles() {
    let value = r#"Team (nested (label: ignored), still ignored): "Role: more text" <role@example.test>, "local:part"@example.test; "Angle" <"angle:part"@example.test>"#;
    let parsed = Address::parse_list_checked(value).expect("valid mailboxes");
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0].name.as_deref(), Some("Role: more text"));
    assert_eq!(parsed[0].address, "role@example.test");
    assert_eq!(parsed[1].address, r#""local:part"@example.test"#);
    assert_eq!(parsed[2].address, r#""angle:part"@example.test"#);
    assert_eq!(
        address_syntax(r#"Name (nested (comment: ignored)) <"a:b"@example.test>"#)
            .into_iter()
            .map(|(_, ch)| ch)
            .collect::<Vec<_>>(),
        ['<', '>']
    );
}

#[test]
fn address_quoted_local_parts_preserve_whitespace_and_quoted_pairs() {
    for local in [
        r#""a b""#,
        "\"a\tb\"",
        r#""a\" b""#,
        r#""a\\ b""#,
        r#""a@b, c:d""#,
    ] {
        let address = format!("{local}@example.test");
        for value in [address.clone(), format!("Mailbox <{address}>")] {
            let parsed = Address::parse_list_checked(&value).expect("quoted local part");
            assert_eq!(parsed.len(), 1, "{value}");
            assert_eq!(parsed[0].address, address);
            let tolerant = Address::parse_list(&value);
            assert_eq!(tolerant.len(), 1);
            assert_eq!(tolerant[0].address, address);
        }
    }
}

#[test]
fn address_checked_parser_reports_rejected_items_instead_of_partial_success() {
    let value = "good@example.test, missing-at, bad local@example.test, <missing@example.test";
    assert_eq!(
        Address::parse_list_checked(value).expect_err("must not return a reduced list"),
        [
            "missing-at",
            "bad local@example.test",
            "<missing@example.test"
        ]
    );
    for value in [
        r#""unclosed@example.test"#,
        r#""dangling\"#,
        "@example.test",
        "local@",
    ] {
        assert_eq!(Address::parse_list_checked(value).unwrap_err(), [value]);
    }
}

#[test]
fn header_checked_addresses_collect_rejections_across_repeated_fields() {
    let headers =
        HeaderMap::parse(b"To: good@example.test, bad local@example.test\r\nTo: missing-at\r\n");
    assert_eq!(
        headers.addresses_checked("to").unwrap_err(),
        ["bad local@example.test", "missing-at"]
    );
    assert!(headers.addresses_checked("bcc").unwrap().is_empty());
    assert!(
        Address::parse_list_checked("Undisclosed:;")
            .unwrap()
            .is_empty()
    );
}
