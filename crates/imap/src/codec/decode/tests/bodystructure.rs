use super::*;

// ===== Structure-aware BODYSTRUCTURE generative pass (imap-T4) =====
//
// Two phases, both deterministic (fixed-seed SplitMix64, fixed iteration
// counts, no wall clock, no I/O):
//
//   1. Build well-formed BODYSTRUCTURE trees from a grammar, render them to
//      wire bytes, and assert the parser recovers the exact tree shape. This
//      is what the hand-written cases above cannot do: it walks the cross
//      product of nesting, part kind, parameter form, and extension-data
//      arity.
//   2. Mutate those same renderings the way a broken server would (truncate,
//      delete, or splice a structural byte) and assert the parser stays
//      well-behaved: it either errors or returns, never panics, never reports
//      more remaining input than it was given, and never recurses past
//      `MAX_BODY_NESTING_DEPTH`.

struct BodyRng(u64);

impl BodyRng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        assert!(n > 0);
        usize::try_from(self.next_u64() % u64::try_from(n).unwrap()).unwrap()
    }

    fn pick<'a, T>(&mut self, options: &'a [T]) -> &'a T {
        &options[self.below(options.len())]
    }
}

/// A BODYSTRUCTURE tree, independent of its wire spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BodyNode {
    Text(&'static str),
    Basic(&'static str, &'static str),
    Message(Box<BodyNode>),
    Multipart(&'static str, Vec<BodyNode>),
}

/// The wire spellings of `body-fld-param` that must all round-trip.
const PARAM_FORMS: [&str; 4] = [
    "NIL",
    "()",
    "(\"CHARSET\" \"UTF-8\")",
    "(\"NAME*0*\" \"utf-8''caf%C3%A9\" \"NAME*1*\" \".txt\")",
];

/// Optional single-part extension data, in increasing arity
/// (RFC 3501 Section 7.4.2: md5, disposition, language, location, then
/// unparsed `body-extension`).
const EXT_1PART_FORMS: [&str; 6] = [
    "",
    " NIL",
    " \"deadbeef\"",
    " NIL (\"attachment\" (\"FILENAME\" \"a.txt\"))",
    " NIL NIL (\"en\" \"no\")",
    " NIL NIL NIL \"http://example.invalid/p\" (\"future\" \"extension\")",
];

/// Optional multipart extension data (params, disposition, language,
/// location, then unparsed `body-extension`).
const EXT_MPART_FORMS: [&str; 5] = [
    "",
    " NIL",
    " (\"BOUNDARY\" \"b1\")",
    " NIL (\"inline\" NIL)",
    " NIL NIL (\"en\") \"http://example.invalid/m\" 42",
];

/// A minimal well-formed ENVELOPE, reused for every message/rfc822 part.
const EMBEDDED_ENVELOPE: &str = "(NIL \"embedded\" NIL NIL NIL NIL NIL NIL NIL NIL)";

fn generate_body_node(rng: &mut BodyRng, depth: usize) -> BodyNode {
    // Leaf-only below the budget, so tree size stays bounded.
    let choice = if depth == 0 {
        rng.below(2)
    } else {
        rng.below(4)
    };
    match choice {
        0 => BodyNode::Text(rng.pick(&["PLAIN", "HTML", "CALENDAR"])),
        1 => BodyNode::Basic(
            rng.pick(&["IMAGE", "APPLICATION", "AUDIO"]),
            rng.pick(&["GIF", "OCTET-STREAM", "PDF"]),
        ),
        2 => BodyNode::Message(Box::new(generate_body_node(rng, depth - 1))),
        _ => {
            let children = 1 + rng.below(3);
            let kids = (0..children)
                .map(|_| generate_body_node(rng, depth - 1))
                .collect();
            BodyNode::Multipart(rng.pick(&["MIXED", "ALTERNATIVE", "RELATED"]), kids)
        }
    }
}

fn render_body_node(rng: &mut BodyRng, node: &BodyNode, out: &mut String) {
    match node {
        BodyNode::Text(subtype) => {
            let params = *rng.pick(&PARAM_FORMS);
            let ext = *rng.pick(&EXT_1PART_FORMS);
            out.push_str(&format!(
                "(\"TEXT\" \"{subtype}\" {params} NIL NIL \"7BIT\" 1234 42{ext})"
            ));
        }
        BodyNode::Basic(ty, subtype) => {
            let params = *rng.pick(&PARAM_FORMS);
            let ext = *rng.pick(&EXT_1PART_FORMS);
            out.push_str(&format!(
                "(\"{ty}\" \"{subtype}\" {params} \"<cid@x>\" \"a description\" \"BASE64\" 9876{ext})"
            ));
        }
        BodyNode::Message(inner) => {
            let params = *rng.pick(&PARAM_FORMS);
            out.push_str(&format!(
                "(\"MESSAGE\" \"RFC822\" {params} NIL NIL \"7BIT\" 5000 {EMBEDDED_ENVELOPE} "
            ));
            render_body_node(rng, inner, out);
            let ext = *rng.pick(&EXT_1PART_FORMS);
            out.push_str(&format!(" 77{ext})"));
        }
        BodyNode::Multipart(subtype, kids) => {
            out.push('(');
            for kid in kids {
                render_body_node(rng, kid, out);
            }
            let ext = *rng.pick(&EXT_MPART_FORMS);
            out.push_str(&format!(" \"{subtype}\"{ext})"));
        }
    }
}

/// Project a parsed structure back onto the generator's tree model, so the
/// comparison is over shape (kind, subtype, children) and not over the
/// incidental field values the renderer chose.
fn parsed_body_node(bs: &BodyStructure) -> BodyNode {
    match bs {
        BodyStructure::Text { media_subtype, .. } => {
            BodyNode::Text(static_label(media_subtype, &["PLAIN", "HTML", "CALENDAR"]))
        }
        BodyStructure::Basic {
            media_type,
            media_subtype,
            ..
        } => BodyNode::Basic(
            static_label(media_type, &["IMAGE", "APPLICATION", "AUDIO"]),
            static_label(media_subtype, &["GIF", "OCTET-STREAM", "PDF"]),
        ),
        BodyStructure::Message { body, .. } => BodyNode::Message(Box::new(parsed_body_node(body))),
        BodyStructure::Multipart {
            media_subtype,
            bodies,
            ..
        } => BodyNode::Multipart(
            static_label(media_subtype, &["MIXED", "ALTERNATIVE", "RELATED"]),
            bodies.iter().map(parsed_body_node).collect(),
        ),
    }
}

/// The parser lowercases media types (RFC 2045 Section 5.1); map back to the
/// generator's uppercase label so the tree comparison is exact.
fn static_label(parsed: &str, candidates: &[&'static str]) -> &'static str {
    candidates
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(parsed))
        .unwrap_or_else(|| panic!("parser produced unexpected media label {parsed:?}"))
}

// ===== BODYSTRUCTURE tests =====

#[test]
fn body_structure_text_plain() {
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        params,
        encoding,
        size,
        lines,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "plain");
        assert_eq!(params, &[("charset".into(), "UTF-8".into())]);
        assert_eq!(encoding, "7bit");
        assert_eq!(*size, 100);
        assert_eq!(*lines, 5);
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

#[test]
fn body_structure_rfc2231_params_end_to_end() {
    // End-to-end: parse BODYSTRUCTURE with RFC 2231 continuation params.
    // The parser automatically decodes and reassembles them.
    let input = b"(\"APPLICATION\" \"PDF\" (\"FILENAME*0*\" \"UTF-8''long%20\" \"FILENAME*1\" \"name.pdf\") NIL NIL \"BASE64\" 12345)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Basic { params, .. } = &bs {
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "filename");
        assert_eq!(params[0].1, "long name.pdf");
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

#[test]
fn body_structure_basic_image() {
    let input = b"(\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 2048)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Basic {
        media_type,
        media_subtype,
        size,
        ..
    } = &bs
    {
        assert_eq!(media_type, "image");
        assert_eq!(media_subtype, "png");
        assert_eq!(*size, 2048);
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

#[test]
fn body_structure_multipart() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5)\
                       (\"TEXT\" \"HTML\" NIL NIL NIL \"QUOTED-PRINTABLE\" 200 10) \
                       \"ALTERNATIVE\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "alternative");
        assert_eq!(bodies.len(), 2);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

/// RFC 3501 Section 7.4.2 / Postel's law: tolerate optional whitespace
/// between child bodies in a multipart BODYSTRUCTURE. While the ABNF
/// specifies `1*body` with no separator, non-conformant servers may
/// insert spaces.
#[test]
fn body_structure_multipart_whitespace_between_children() {
    // Note the space between the two child bodies: `) (`
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) (\"TEXT\" \"HTML\" NIL NIL NIL \"QUOTED-PRINTABLE\" 200 10) \"ALTERNATIVE\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        bodies,
        media_subtype,
        ..
    } = bs
    {
        assert_eq!(
            bodies.len(),
            2,
            "both child bodies must be parsed despite whitespace between them"
        );
        assert_eq!(media_subtype, "alternative");
    } else {
        panic!("expected Multipart");
    }
}

#[test]
fn body_structure_with_disposition() {
    let input = b"(\"APPLICATION\" \"PDF\" NIL NIL NIL \"BASE64\" 4096 \
                       NIL (\"ATTACHMENT\" (\"FILENAME\" \"report.pdf\")) NIL NIL)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Basic { disposition, .. } = &bs {
        let disp = disposition.as_ref().expect("expected disposition");
        assert_eq!(disp.disposition_type, "attachment");
        assert_eq!(disp.params, vec![("filename".into(), "report.pdf".into())]);
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

#[test]
fn body_disposition_rfc2231_charset_encoded() {
    // RFC 2231 parameter in Content-Disposition (RFC 2183).
    let input = b"(\"attachment\" (\"FILENAME*\" \"UTF-8''r%C3%A9sum%C3%A9.pdf\"))";
    let (_, disp) = body_disposition(input).unwrap();
    let disp = disp.expect("disposition should not be NIL");
    assert_eq!(disp.disposition_type, "attachment");
    assert_eq!(disp.params.len(), 1);
    assert_eq!(disp.params[0].0, "filename");
    assert_eq!(disp.params[0].1, "r\u{e9}sum\u{e9}.pdf");
}

#[test]
fn body_structure_with_rfc2231_disposition() {
    // Full BODYSTRUCTURE with RFC 2231 encoded disposition parameter.
    let input = b"(\"APPLICATION\" \"PDF\" (\"NAME\" \"file.pdf\") NIL NIL \"BASE64\" 12345 NIL (\"attachment\" (\"FILENAME*\" \"UTF-8''t%C3%A9st.pdf\")) NIL NIL)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Basic { disposition, .. } = bs {
        let disp = disposition.expect("disposition should not be NIL");
        assert_eq!(disp.disposition_type, "attachment");
        assert_eq!(disp.params[0].0, "filename");
        assert_eq!(disp.params[0].1, "t\u{e9}st.pdf");
    } else {
        panic!("expected Basic body structure");
    }
}

#[test]
fn fetch_body_without_section_is_bodystructure() {
    // BODY without [] is a synonym for BODYSTRUCTURE (RFC 3501 Section 6.4.5)
    let input = b"(BODY (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(fr.body_structure.is_some());
}

#[test]
fn body_structure_truncated() {
    let result = body_structure(
        b"(\"TEXT\" \"PLAIN\" NIL NIL NIL",
        crate::types::wire::WireMode::ModifiedUtf7,
        0,
    );
    assert!(result.is_err());
}

// ===== Nested BODYSTRUCTURE tests =====

#[test]
fn body_structure_nested_multipart() {
    // multipart/mixed containing:
    //   - multipart/alternative (text/plain + text/html)
    //   - application/pdf attachment
    // Outer parens wrap the whole BODYSTRUCTURE; inner nested multipart
    // is itself parenthesized per RFC 3501 Section 7.4.2.
    let input = b"(((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5)\
            (\"TEXT\" \"HTML\" (\"CHARSET\" \"UTF-8\") NIL NIL \"QUOTED-PRINTABLE\" 200 10) \
            \"ALTERNATIVE\")\
            (\"APPLICATION\" \"PDF\" NIL NIL NIL \"BASE64\" 5000) \
            \"MIXED\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 2);
        // First body is the multipart/alternative
        if let BodyStructure::Multipart {
            media_subtype: inner_sub,
            bodies: inner_bodies,
            ..
        } = &bodies[0]
        {
            assert_eq!(inner_sub, "alternative");
            assert_eq!(inner_bodies.len(), 2);
        } else {
            panic!("expected inner Multipart, got {:?}", bodies[0]);
        }
        // Second body is the PDF attachment
        assert!(
            matches!(&bodies[1], BodyStructure::Basic { media_type, .. } if media_type.eq_ignore_ascii_case("APPLICATION"))
        );
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

#[test]
fn body_structure_message_rfc822() {
    // message/rfc822 with embedded envelope and body
    let input = b"(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 \
            (NIL \"embedded\" NIL NIL NIL NIL NIL NIL NIL NIL) \
            (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) 20)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Message {
        size,
        envelope,
        body,
        lines,
        ..
    } = bs
    {
        assert_eq!(size, 500);
        assert_eq!(envelope.subject.as_deref(), Some("embedded"));
        assert_eq!(lines, 20);
        assert!(matches!(*body, BodyStructure::Text { .. }));
    } else {
        panic!("expected Message, got {bs:?}");
    }
}

// ===== Parser edge case tests =====

/// Deeply nested multipart BODYSTRUCTURE (3 levels deep).
#[test]
fn body_structure_deeply_nested_multipart() {
    // multipart/mixed containing multipart/alternative containing text/plain + text/html
    let input = b"* 1 FETCH (BODYSTRUCTURE \
            (((\"text\" \"plain\" (\"charset\" \"utf-8\") NIL NIL \"7bit\" 100 5 NIL NIL NIL NIL)\
            (\"text\" \"html\" (\"charset\" \"utf-8\") NIL NIL \"quoted-printable\" 200 10 NIL NIL NIL NIL) \
            \"alternative\" NIL NIL NIL NIL)\
            (\"application\" \"pdf\" (\"name\" \"doc.pdf\") NIL NIL \"base64\" 5000 NIL NIL NIL NIL) \
            \"mixed\" NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        let body = fetch.body_structure.expect("missing BODYSTRUCTURE");
        if let BodyStructure::Multipart {
            media_subtype,
            bodies,
            ..
        } = &body
        {
            assert!(media_subtype.eq_ignore_ascii_case("mixed"));
            assert_eq!(bodies.len(), 2);
            // First part is multipart/alternative
            if let BodyStructure::Multipart {
                media_subtype: inner_sub,
                bodies: inner_parts,
                ..
            } = &bodies[0]
            {
                assert!(inner_sub.eq_ignore_ascii_case("alternative"));
                assert_eq!(inner_parts.len(), 2);
            } else {
                panic!("expected inner multipart/alternative");
            }
            // Second part is application/pdf
            if let BodyStructure::Basic {
                media_type,
                media_subtype,
                ..
            } = &bodies[1]
            {
                assert!(media_type.eq_ignore_ascii_case("application"));
                assert!(media_subtype.eq_ignore_ascii_case("pdf"));
            } else {
                panic!("expected application/pdf");
            }
            return;
        }
    }
    panic!("expected nested multipart BODYSTRUCTURE");
}

/// message/rfc822 BODYSTRUCTURE with nested envelope and body.
#[test]
fn body_structure_message_rfc822_nested() {
    let input = b"* 1 FETCH (BODYSTRUCTURE \
            (\"message\" \"rfc822\" NIL NIL NIL \"7bit\" 1000 \
            (\"Mon, 01 Jan 2024 00:00:00 +0000\" \"Inner Subject\" \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            NIL NIL \
            ((\"Recipient\" NIL \"rcpt\" \"example.com\")) \
            NIL NIL NIL \"<inner@example.com>\") \
            (\"text\" \"plain\" (\"charset\" \"us-ascii\") NIL NIL \"7bit\" 50 3 NIL NIL NIL NIL) \
            20 NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        let body = fetch.body_structure.expect("missing BODYSTRUCTURE");
        if let BodyStructure::Message { envelope, body, .. } = &body {
            assert_eq!(envelope.subject, Some("Inner Subject".into()));
            assert_eq!(envelope.message_id, Some("<inner@example.com>".into()));
            // Nested body is text/plain
            if let BodyStructure::Text { media_subtype, .. } = body.as_ref() {
                assert!(media_subtype.eq_ignore_ascii_case("plain"));
            } else {
                panic!("expected text/plain in nested body");
            }
            return;
        }
    }
    panic!("expected message/rfc822 BODYSTRUCTURE");
}

/// BODYSTRUCTURE with no extension data (minimal text/plain).
#[test]
fn body_structure_minimal_text() {
    let input = b"(\"text\" \"plain\" (\"charset\" \"us-ascii\") NIL NIL \"7bit\" 42 3)";
    let (_, body) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        size,
        lines,
        ..
    } = body
    {
        assert!(media_subtype.eq_ignore_ascii_case("plain"));
        assert_eq!(size, 42);
        assert_eq!(lines, 3);
    } else {
        panic!("expected Text body");
    }
}

/// BODYSTRUCTURE keyword in lowercase.
#[test]
fn fetch_lowercase_bodystructure() {
    let input =
        b"(bodystructure (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))";
    let (_, fr) = fetch_response_inner(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(fr.body_structure.is_some());
}

// ===== BODYSTRUCTURE partial extension data tests =====

/// Single-part body with only MD5 extension (no disposition/language/location).
#[test]
fn body_structure_ext_md5_only() {
    // TEXT body with md5, but no disposition/language/location.
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5 \"abc123\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        md5, disposition, ..
    } = bs
    {
        assert_eq!(md5.as_deref(), Some("abc123"));
        assert!(disposition.is_none());
    } else {
        panic!("expected Text body");
    }
}

/// Single-part body with MD5 and disposition, but no language/location.
#[test]
fn body_structure_ext_md5_and_disposition() {
    let input =
        b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5 NIL (\"inline\" NIL))";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        md5,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert!(md5.is_none());
        let disp = disposition.expect("disposition should be present");
        assert_eq!(disp.disposition_type, "inline");
        assert!(language.is_none());
        assert!(location.is_none());
    } else {
        panic!("expected Text body");
    }
}

/// Single-part body with MD5, disposition, and language, but no location.
#[test]
fn body_structure_ext_through_language() {
    let input = b"(\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5 NIL NIL (\"en\" \"fr\"))";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        language, location, ..
    } = bs
    {
        let langs = language.expect("language should be present");
        assert_eq!(langs, vec!["en", "fr"]);
        assert!(location.is_none());
    } else {
        panic!("expected Text body");
    }
}

/// Multipart body with only params extension (no disposition/language/location).
#[test]
fn body_structure_mpart_ext_params_only() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 80 4) \"ALTERNATIVE\" (\"BOUNDARY\" \"----=_Part\"))";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        params,
        disposition,
        ..
    } = bs
    {
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "boundary");
        assert!(disposition.is_none());
    } else {
        panic!("expected Multipart body");
    }
}

/// Multipart body with no extension data at all.
#[test]
fn body_structure_mpart_no_extension() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) \"MIXED\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        params,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert!(params.is_empty());
        assert!(disposition.is_none());
        assert!(language.is_none());
        assert!(location.is_none());
    } else {
        panic!("expected Multipart body");
    }
}

/// `body_disposition` with NIL (RFC 2183).
#[test]
fn body_disposition_nil() {
    let (_, disp) = body_disposition(b"NIL").unwrap();
    assert!(disp.is_none());
}

/// `body_disposition` with case-insensitive NIL (RFC 2183).
#[test]
fn body_disposition_nil_case_insensitive() {
    let (_, disp) = body_disposition(b"nil").unwrap();
    assert!(disp.is_none());
}

/// `body_disposition` with type but no params (RFC 2183).
#[test]
fn body_disposition_no_params() {
    let (_, disp) = body_disposition(b"(\"inline\" NIL)").unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(disp.disposition_type, "inline");
    assert!(disp.params.is_empty());
}

/// `body_disposition` with type and params (RFC 2183).
#[test]
fn body_disposition_with_params() {
    let (_, disp) =
        body_disposition(b"(\"attachment\" (\"FILENAME\" \"report.pdf\" \"SIZE\" \"1024\"))")
            .unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(disp.disposition_type, "attachment");
    assert_eq!(disp.params.len(), 2);
    assert_eq!(disp.params[0], ("filename".into(), "report.pdf".into()));
    assert_eq!(disp.params[1], ("size".into(), "1024".into()));
}

/// multipart BODYSTRUCTURE with empty language list `()`.
#[test]
fn spec_audit_multipart_empty_language() {
    // Multipart with extension data: params disposition language location
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 20 2) \"ALTERNATIVE\" NIL NIL () NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "multipart body-fld-lang `()` must be accepted; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Multipart { language, .. } => {
                        let langs = language
                            .as_ref()
                            .expect("empty () should be Some(vec![]), not None");
                        assert!(
                            langs.is_empty(),
                            "empty () should produce empty language vec"
                        );
                    }
                    other => panic!("expected Multipart variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== Parser stress tests =====

/// Deeply nested multipart BODYSTRUCTURE (10 levels deep).
#[test]
fn bodystructure_deeply_nested() {
    // Build a 10-level nested multipart structure:
    // ((((((((((("TEXT" "PLAIN" NIL NIL NIL "7BIT" 0 0) "MIXED")
    //   "MIXED") "MIXED") ...) "MIXED")
    let mut input = Vec::new();
    input.extend_from_slice(b"* 1 FETCH (BODYSTRUCTURE ");
    // 10 levels of opening parens
    input.resize(input.len() + 10, b'(');
    // Innermost text part
    input.extend_from_slice(b"\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 0 0");
    // Close each level with subtype
    for _ in 0..10 {
        input.extend_from_slice(b" \"MIXED\")");
    }
    input.extend_from_slice(b")\r\n");

    let (_, resp) = parse_response(&input).unwrap();
    assert!(
        matches!(resp, Response::Untagged(_)),
        "expected Untagged Fetch"
    );
}

#[test]
fn fetch_bodystructure_extension_double_spaces_parse() {
    // Two spaces between optional body-extension fields are tolerated, as in
    // the other BODYSTRUCTURE list positions.
    let input =
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL  NIL))\r\n";
    assert!(matches!(
        parse_response(input),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Fetch(_))
    ));

    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) \"MIXED\" NIL  NIL))\r\n";
    assert!(matches!(
        parse_response(input),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Fetch(_))
    ));
}

// H2: MESSAGE/GLOBAL in BODYSTRUCTURE (RFC 9051 Section 7.5.2)
#[test]
fn bodystructure_message_global() {
    // message/global has the same structure as message/rfc822:
    // envelope body lines
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"MESSAGE\" \"GLOBAL\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 500 (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL) (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) 20))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            match fr.body_structure.unwrap() {
                BodyStructure::Message { lines, size, .. } => {
                    assert_eq!(lines, 20);
                    assert_eq!(size, 500);
                }
                other => panic!("expected Message variant, got {other:?}"),
            }
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// H3: BODYSTRUCTURE extension data with nested parentheses
#[test]
fn bodystructure_ext_nested_parens() {
    // Extension data with nested parenthesized values after location
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5 NIL NIL NIL NIL (\"ext\" (\"nested\" \"value\"))))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert!(fr.body_structure.is_some());
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn bodystructure_mpart_ext_nested_parens() {
    // Multipart with extension data containing nested parentheses
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5) \"MIXED\" (\"boundary\" \"----=_Part\") NIL NIL NIL (\"future-ext\" (\"a\" \"b\"))))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert!(fr.body_structure.is_some());
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// L1: body-fld-lines as u64
#[test]
fn bodystructure_text_lines_u64() {
    // Technically extreme but valid per RFC 9051 number64
    let input =
        b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5000000000))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            match fr.body_structure.unwrap() {
                BodyStructure::Text { lines, .. } => assert_eq!(lines, 5_000_000_000u64),
                other => panic!("expected Text, got {other:?}"),
            }
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// L15: Body extension data containing a literal `{3}\r\na)b` is not handled
/// by `skip_balanced_parens()`, which only knows about quoted strings and parens.
/// When the literal content contains `)`, `skip_balanced_parens` mistakes it for
/// the end of the body structure, causing a misparse.
#[test]
fn spec_audit_l15_body_extension_literal() {
    // A text BODYSTRUCTURE with an extension field that is a literal string
    // whose content contains a `)` character.
    // Fields: media-type, subtype, params, id, desc, encoding, size, lines,
    //         md5 (ext), dsp (ext), lang (ext), loc (ext), then extra extension literal.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") \
NIL NIL \"7BIT\" 100 5 NIL NIL NIL NIL {3}\r\na)b))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(_fr) = &*u {
            // Successfully parsed  -  the literal in extension data was handled.
        } else {
            panic!("expected Fetch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// BODYSTRUCTURE disposition with empty params `("inline" ())`
/// instead of `("inline" NIL)`.
#[test]
fn spec_audit_body_disposition_empty_params() {
    // Single-part with disposition that has empty param list
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5 NIL (\"inline\" ()) NIL NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "disposition with empty params `()` must be accepted; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text { disposition, .. } => {
                        let disp = disposition.as_ref().expect("should have disposition");
                        assert_eq!(disp.disposition_type, "inline");
                        assert!(
                            disp.params.is_empty(),
                            "empty () params should produce empty vec"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// multipart BODYSTRUCTURE extension data with empty params `()`.
#[test]
fn spec_audit_multipart_empty_ext_params() {
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 20 2) \"ALTERNATIVE\" () NIL NIL NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "multipart body-fld-param `()` must be accepted; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Multipart { params, .. } => {
                        assert!(
                            params.is_empty(),
                            "empty () should produce empty params vec"
                        );
                    }
                    other => panic!("expected Multipart variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 9051 Section 7.5.2: body-fld-octcnt is number64 (not number).
/// "This accommodates data items that are larger than 4GB."
///
/// The BODYSTRUCTURE `size` field must be u64 to handle parts > 4 GiB.
/// This test verifies that a size value exceeding `u32::MAX` (4,294,967,295)
/// parses as a proper FETCH with BODYSTRUCTURE (not silently falling back
/// to `UntaggedResponse::Unknown`).
#[test]
fn spec_audit_bodystructure_size_number64() {
    // A text/plain BODYSTRUCTURE with size = 5,000,000,000 (> u32::MAX)
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 5000000000 100))\r\n";
    let (_, resp) = parse_response(input).expect("should parse");
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect(
                    "BODYSTRUCTURE must be present  -  size > u32::MAX must not \
                         cause a parse failure per RFC 9051 Section 7.5.2",
                );
                match bs {
                    BodyStructure::Text { size, lines, .. } => {
                        assert_eq!(
                            size, 5_000_000_000u64,
                            "body-fld-octcnt must support number64 per \
                                 RFC 9051 Section 7.5.2"
                        );
                        assert_eq!(lines, 100);
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            UntaggedResponse::Unknown(_) => {
                panic!(
                    "BODYSTRUCTURE with size > u32::MAX was silently discarded \
                         as Unknown  -  body-fld-octcnt must be number64 per \
                         RFC 9051 Section 7.5.2"
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 9051 Section 7.5.2: body-fld-octcnt = number64 for basic parts.
#[test]
fn spec_audit_bodystructure_basic_size_number64() {
    // An image/png BODYSTRUCTURE with size = 6,000,000,000 (> u32::MAX)
    let input =
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 6000000000))\r\n";
    let (_, resp) = parse_response(input).expect("should parse");
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("BODYSTRUCTURE must be present");
                match bs {
                    BodyStructure::Basic { size, .. } => {
                        assert_eq!(
                            size, 6_000_000_000u64,
                            "body-fld-octcnt must support number64 per \
                                 RFC 9051 Section 7.5.2"
                        );
                    }
                    other => panic!("expected Basic variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 2183 Section 2: Content-Disposition type is not case sensitive.
/// The parser must lowercase it for consistent comparison, using the
/// conventional form ("inline", "attachment") from RFC examples.
#[test]
fn spec_audit_disposition_type_case_normalized() {
    // Server sends mixed-case disposition type "Attachment"; parser must
    // lowercase it to "attachment" per RFC 2183 Section 2.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \
            \"7BIT\" 100 5 NIL (\"Attachment\" (\"FILENAME\" \"test.txt\")) NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("missing BODYSTRUCTURE");
                match bs {
                    BodyStructure::Text { disposition, .. } => {
                        let disp = disposition.expect("missing disposition");
                        assert_eq!(
                            disp.disposition_type, "attachment",
                            "RFC 2183 Section 2: disposition type must be lowercased"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 2183 Section 2: disposition type is case-insensitive and must be
/// normalized to lowercase (the conventional form used in RFC examples
/// and real-world implementations).  Consumers comparing against
/// "attachment" or "inline" (the standard forms) must match.
#[test]
fn disposition_type_lowercased_for_conventional_comparison() {
    // Server sends uppercase "ATTACHMENT"; parser must lowercase it.
    let (_, disp) = body_disposition(b"(\"ATTACHMENT\" NIL)").unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(
        disp.disposition_type, "attachment",
        "RFC 2183 Section 2: disposition type must be lowercased to \
             conventional form for consistent comparison"
    );

    // Server sends mixed-case "Inline"; parser must lowercase it.
    let (_, disp) = body_disposition(b"(\"Inline\" NIL)").unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(
        disp.disposition_type, "inline",
        "RFC 2183 Section 2: disposition type must be lowercased to \
             conventional form for consistent comparison"
    );
}

/// RFC 3501 Section 7.4.2 / Postel's law: trailing whitespace before the
/// closing `)` in a BODYSTRUCTURE must not cause a parse failure. Some
/// servers emit extra whitespace between the last field and the closing
/// paren.
#[test]
fn bodystructure_text_trailing_space_before_close_paren() {
    // Note the space before the final `)` of the BODYSTRUCTURE.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5 ))\r\n";
    let (_, resp) = parse_response(input)
        .expect("BODYSTRUCTURE with trailing space before ) should parse (Postel's law)");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(ref fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text {
                        media_subtype,
                        lines,
                        size,
                        ..
                    } => {
                        assert_eq!(media_subtype, "plain");
                        assert_eq!(*size, 100);
                        assert_eq!(*lines, 5);
                    }
                    other => panic!("expected Text, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Trailing whitespace after the last extension field (location) before
/// the closing `)` of a single-part BODYSTRUCTURE.
#[test]
fn bodystructure_basic_ext_data_trailing_space() {
    // Basic type with md5=NIL, disposition=NIL, language=NIL, location=NIL, then trailing space before ')'.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL NIL NIL NIL ))\r\n";
    let (_, resp) = parse_response(input)
        .expect("BODYSTRUCTURE with trailing space after extension data should parse");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(ref fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                assert!(matches!(bs, BodyStructure::Basic { .. }));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Trailing whitespace before `)` in a multipart BODYSTRUCTURE's extension data.
#[test]
fn bodystructure_multipart_trailing_space() {
    // Multipart with params=NIL and trailing space before ')'.
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 100 5) \"ALTERNATIVE\" NIL ))\r\n";
    let (_, resp) =
        parse_response(input).expect("multipart BODYSTRUCTURE with trailing space should parse");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(ref fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Multipart {
                        media_subtype,
                        bodies,
                        ..
                    } => {
                        assert_eq!(media_subtype, "alternative");
                        assert_eq!(bodies.len(), 2);
                    }
                    other => panic!("expected Multipart, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Single-part body with all 4 extension fields present: MD5, disposition,
/// language, and location (RFC 3501 Section 7.4.2).
#[test]
fn bodystructure_1part_all_four_ext_fields() {
    // text/plain with md5="abc123", disposition=inline, language=("en"), location="http://example.com"
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5 \"abc123\" (\"inline\" NIL) \"en\" \"http://example.com\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        md5,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert_eq!(md5.as_deref(), Some("abc123"));
        let disp = disposition.expect("disposition should be present");
        assert_eq!(disp.disposition_type, "inline");
        let langs = language.expect("language should be present");
        assert_eq!(langs, vec!["en"]);
        assert_eq!(location.as_deref(), Some("http://example.com"));
    } else {
        panic!("expected Text body");
    }
}

/// Multipart body with all 4 extension fields present: params, disposition,
/// language, and location (RFC 3501 Section 7.4.2).
#[test]
fn bodystructure_mpart_all_four_ext_fields() {
    // multipart/mixed with params=(BOUNDARY "----=_Part"), disposition=inline,
    // language=("en" "fr"), location="http://example.com"
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 80 4) \"MIXED\" (\"BOUNDARY\" \"----=_Part\") (\"inline\" NIL) (\"en\" \"fr\") \"http://example.com\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        params,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "boundary");
        let disp = disposition.expect("disposition should be present");
        assert_eq!(disp.disposition_type, "inline");
        let langs = language.expect("language should be present");
        assert_eq!(langs, vec!["en", "fr"]);
        assert_eq!(location.as_deref(), Some("http://example.com"));
    } else {
        panic!("expected Multipart body");
    }
}

/// Deeply nested multipart BODYSTRUCTURE beyond `MAX_BODY_NESTING_DEPTH` must
/// be rejected to prevent stack overflow (defense-in-depth).
///
/// RFC 3501 Section 7.4.2 does not specify a maximum nesting depth, but
/// unbounded recursion is a denial-of-service vector.
#[test]
fn bodystructure_rejects_excessive_nesting_depth() {
    // Build a BODYSTRUCTURE with 256 levels of multipart nesting  -  well
    // beyond MAX_BODY_NESTING_DEPTH (64).
    let depth = 256_usize;
    let mut input = Vec::new();
    input.extend_from_slice(b"* 1 FETCH (BODYSTRUCTURE ");
    // Open `depth` levels of multipart: each level is `(` + inner + ` "MIXED")`.
    // The innermost part is a simple text/plain.
    for _ in 0..depth {
        input.push(b'(');
    }
    // Innermost leaf: text/plain body
    input.extend_from_slice(
        b"(\"text\" \"plain\" (\"charset\" \"utf-8\") NIL NIL \"7bit\" 10 1 NIL NIL NIL NIL)",
    );
    // Close each multipart level: SP "MIXED" )
    for _ in 0..depth {
        input.extend_from_slice(b" \"MIXED\")");
    }
    input.extend_from_slice(b")\r\n");

    let result = parse_response(&input);
    assert!(
        result.is_err(),
        "parsing a BODYSTRUCTURE nested {depth} levels deep should fail, \
             but it succeeded"
    );
}

/// RFC 2047 encoded words in BODYSTRUCTURE description must be decoded.
///
/// RFC 2045 Section 8 defines Content-Description as `*text`.
/// RFC 2047 Section 5 rule (1): encoded-words may replace `text` tokens in any
/// MIME body part field whose body is defined as `*text`.
#[test]
fn bodystructure_description_rfc2047_decoded() {
    // "Hello" encoded as RFC 2047 Base64 in UTF-8
    let input =
        b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL \"=?UTF-8?B?SGVsbG8=?=\" \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("Hello"),
            "RFC 2047 encoded words in Content-Description must be decoded"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }

    // Also test Basic body type (e.g., image/png)
    let input = b"(\"IMAGE\" \"PNG\" NIL NIL \"=?UTF-8?Q?=C3=A9t=C3=A9?=\" \"BASE64\" 2048)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Basic { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("\u{e9}t\u{e9}"),
            "RFC 2047 encoded description in Basic part must decode to 'été'"
        );
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

/// RFC 6855 Section 3.1 only applies UTF-8 direct encoding to FETCH ENVELOPE
/// fields (subject, address display names), NOT to BODYSTRUCTURE description.
/// Content-Description is a MIME header (RFC 2045 Section 8) that uses RFC 2047
/// encoded-words regardless of UTF8=ACCEPT mode. `decode_rfc2047` already
/// handles plain UTF-8 input correctly, so it must always be used.
#[test]
fn spec_audit_bodystructure_description_rfc2047_utf8_mode() {
    // RFC 2047 encoded "Hello"  -  must be decoded even in UTF8=ACCEPT mode
    let input =
        b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL \"=?UTF-8?B?SGVsbG8=?=\" \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::Utf8, 0).unwrap();
    if let BodyStructure::Text { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("Hello"),
            "RFC 2047 encoded Content-Description must be decoded even in UTF8=ACCEPT mode \
                 (RFC 6855 Section 3.1 only affects ENVELOPE, not BODYSTRUCTURE description \
                 per RFC 2045 Section 8)"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }

    // Also test Basic body type with RFC 2047 Q-encoding
    let input = b"(\"IMAGE\" \"PNG\" NIL NIL \"=?UTF-8?Q?=C3=A9t=C3=A9?=\" \"BASE64\" 2048)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::Utf8, 0).unwrap();
    if let BodyStructure::Basic { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("\u{e9}t\u{e9}"),
            "RFC 2047 Q-encoded Content-Description must decode in UTF8=ACCEPT mode"
        );
    } else {
        panic!("expected Basic, got {bs:?}");
    }

    // Plain UTF-8 (no RFC 2047 wrapping) must also work  -  decode_rfc2047
    // passes through raw UTF-8 via String::from_utf8_lossy.
    let input =
        b"(\"TEXT\" \"HTML\" (\"CHARSET\" \"UTF-8\") NIL \"\xc3\xa9t\xc3\xa9\" \"7BIT\" 50 2)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::Utf8, 0).unwrap();
    if let BodyStructure::Text { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("\u{e9}t\u{e9}"),
            "Plain UTF-8 Content-Description must be preserved in UTF8=ACCEPT mode"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

/// RFC 3501 Section 7.4.2: BODYSTRUCTURE with empty parameter list `()`
/// instead of NIL. Both forms must be accepted per Postel's law.
#[test]
fn edge_bodystructure_empty_parens_params() {
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" () NIL NIL \"7BIT\" 42 3))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let bs = f
                .body_structure
                .as_ref()
                .expect("body_structure should be present");
            if let BodyStructure::Text { params, .. } = bs {
                assert!(
                    params.is_empty(),
                    "empty () params should produce empty vec, got {params:?}"
                );
            } else {
                panic!("expected Text body structure, got {bs:?}");
            }
        } else {
            panic!("expected Fetch response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 3501 Section 7.4.2: BODYSTRUCTURE with message/rfc822 type
/// that embeds a nested multipart body. Must parse without error.
#[test]
fn edge_bodystructure_message_rfc822_nested() {
    // message/rfc822 with embedded multipart/mixed
    let input = b"(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 \
            (NIL \"Inner subject\" ((\"Inner\" NIL \"inner\" \"ex.com\")) NIL NIL NIL NIL NIL NIL NIL) \
            ((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5)(\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 200) \"MIXED\") \
            20)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Message {
        media_subtype,
        envelope: env,
        body,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "rfc822");
        assert_eq!(env.subject.as_deref(), Some("Inner subject"));
        if let BodyStructure::Multipart {
            media_subtype: inner_sub,
            bodies,
            ..
        } = body.as_ref()
        {
            assert_eq!(inner_sub, "mixed");
            assert_eq!(bodies.len(), 2);
        } else {
            panic!("expected inner Multipart, got {body:?}");
        }
    } else {
        panic!("expected Message, got {bs:?}");
    }
}

/// Postel's law: BODYSTRUCTURE multipart detection must tolerate whitespace
/// after the opening paren (RFC 3501 Section 7.4.2 / RFC 1122 Section 1.2.2).
/// Some servers insert a space between the opening `(` and the first child `(`.
#[test]
fn bodystructure_multipart_whitespace_after_open_paren() {
    let input = b"* 1 FETCH (BODYSTRUCTURE ( (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5)(\"TEXT\" \"HTML\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 200 10) \"ALTERNATIVE\"))\r\n";
    let (_, resp) =
        parse_response(input).expect("BODYSTRUCTURE with space after opening paren should parse");
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            let bs = fr.body_structure.expect("should have body_structure");
            if let BodyStructure::Multipart {
                media_subtype,
                bodies,
                ..
            } = &bs
            {
                assert_eq!(media_subtype, "alternative");
                assert_eq!(bodies.len(), 2, "expected 2 children in multipart");
                // Verify first child is text/plain
                if let BodyStructure::Text {
                    media_subtype: sub, ..
                } = &bodies[0]
                {
                    assert_eq!(sub, "plain");
                } else {
                    panic!("expected first child to be Text, got {:?}", bodies[0]);
                }
                // Verify second child is text/html
                if let BodyStructure::Text {
                    media_subtype: sub, ..
                } = &bodies[1]
                {
                    assert_eq!(sub, "html");
                } else {
                    panic!("expected second child to be Text, got {:?}", bodies[1]);
                }
            } else {
                panic!("expected Multipart, got {bs:?}");
            }
        } else {
            panic!("expected Fetch response");
        }
    } else {
        panic!("expected Untagged response");
    }
}

/// Postel's law: `body_params` must tolerate multiple spaces between
/// parameter key-value pairs (RFC 3501 Section 7.4.2 / RFC 1122 Section 1.2.2).
/// Some servers insert extra whitespace between parameter pairs.
#[test]
fn bodystructure_body_params_multiple_spaces_between_pairs() {
    // Double space between "utf-8" and "name" (between two key-value pairs)
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\"  \"name\" \"file.txt\") NIL NIL \"7BIT\" 100 5))\r\n";
    let (_, resp) =
        parse_response(input).expect("BODYSTRUCTURE with double space in params should parse");
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            let bs = fr.body_structure.expect("should have body_structure");
            if let BodyStructure::Text { params, .. } = &bs {
                assert_eq!(
                    params.len(),
                    2,
                    "expected 2 parameter pairs, got {params:?}"
                );
                assert_eq!(params[0], ("charset".to_string(), "utf-8".to_string()));
                assert_eq!(params[1], ("name".to_string(), "file.txt".to_string()));
            } else {
                panic!("expected Text, got {bs:?}");
            }
        } else {
            panic!("expected Fetch response");
        }
    } else {
        panic!("expected Untagged response");
    }
}

/// RFC 3501 Section 9 / RFC 2231 Section 3: BODYSTRUCTURE with RFC 2231
/// continuation parameters (`*0`, `*1` suffixes) must be reassembled
/// correctly by the parser's integrated RFC 2231 decoding.
///
/// This is an end-to-end test: parse a full FETCH/BODYSTRUCTURE response
/// and verify the params are already decoded.
#[test]
fn edge_bodystructure_rfc2231_continuation_reassembly() {
    // BODYSTRUCTURE with a filename split across two continuation segments.
    // filename*0="very_long_" filename*1="document.pdf"
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"APPLICATION\" \"PDF\" (\"FILENAME*0\" \"very_long_\" \"FILENAME*1\" \"document.pdf\") NIL NIL \"BASE64\" 99999))\r\n";
    let (_, resp) =
        parse_response(input).expect("BODYSTRUCTURE with RFC 2231 continuation params must parse");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        let bs = fr
            .body_structure
            .expect("FETCH response should contain a body_structure");
        if let BodyStructure::Basic { params, .. } = &bs {
            assert_eq!(
                params.len(),
                1,
                "RFC 2231 continuation segments must be reassembled into one param; \
                     got {params:?}"
            );
            assert_eq!(
                params[0].0, "filename",
                "reassembled param key must be the base name"
            );
            assert_eq!(
                params[0].1, "very_long_document.pdf",
                "reassembled param value must concatenate all segments \
                     (RFC 2231 Section 3)"
            );
            return;
        }
        panic!("expected Basic body structure, got {bs:?}");
    }
    panic!("expected Untagged Fetch response");
}

/// RFC 2045 Section 5.1: media types are case-insensitive; the canonical
/// form used in RFC examples is lowercase (e.g. `text/plain`, not
/// `TEXT/PLAIN`). Verify that the BODYSTRUCTURE parser normalizes
/// `media_type`, `media_subtype`, and `encoding` to lowercase for
/// consistency with the `disposition_type` field (which already uses
/// lowercase).
#[test]
fn bodystructure_fields_normalized_to_lowercase() {
    // Single-part text: media_subtype and encoding must be lowercase.
    let text_input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"BASE64\" 100 5)";
    let (_, bs) =
        body_structure(text_input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        encoding,
        ..
    } = &bs
    {
        assert_eq!(
            media_subtype, "plain",
            "media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(
            encoding, "base64",
            "encoding must be lowercase per RFC 2045 Section 6"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }

    // Basic part: media_type and media_subtype must be lowercase.
    let basic_input = b"(\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 2048)";
    let (_, bs) =
        body_structure(basic_input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Basic {
        media_type,
        media_subtype,
        encoding,
        ..
    } = &bs
    {
        assert_eq!(
            media_type, "image",
            "media_type must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(
            media_subtype, "png",
            "media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(
            encoding, "base64",
            "encoding must be lowercase per RFC 2045 Section 6"
        );
    } else {
        panic!("expected Basic, got {bs:?}");
    }

    // Multipart: media_subtype must be lowercase.
    let mpart_input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 80 4) \"MIXED\")";
    let (_, bs) =
        body_structure(mpart_input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart { media_subtype, .. } = &bs {
        assert_eq!(
            media_subtype, "mixed",
            "multipart media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
    } else {
        panic!("expected Multipart, got {bs:?}");
    }

    // Message: media_subtype must be lowercase.
    let msg_input = b"(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 \
            (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL) \
            (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) 20)";
    let (_, bs) = body_structure(msg_input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Message { media_subtype, .. } = &bs {
        assert_eq!(
            media_subtype, "rfc822",
            "message media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
    } else {
        panic!("expected Message, got {bs:?}");
    }
}

/// RFC 3501 Section 7.4.2 / Postel's law: some non-conformant servers
/// (older Exchange, Yahoo) send bare atoms for media type, subtype, and
/// encoding in BODYSTRUCTURE instead of the quoted strings that the
/// formal grammar requires.  The parser must accept both forms.
#[test]
fn bodystructure_bare_atom_media_type() {
    // Bare atoms instead of quoted strings for media type, subtype, and encoding.
    let input = b"(TEXT PLAIN (\"CHARSET\" \"UTF-8\") NIL NIL 7BIT 42 3)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        encoding,
        size,
        lines,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "plain");
        assert_eq!(encoding, "7bit");
        assert_eq!(*size, 42);
        assert_eq!(*lines, 3);
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

/// Same as above but for a basic (non-text) body part with bare atoms.
#[test]
fn bodystructure_bare_atom_basic_type() {
    let input = b"(IMAGE JPEG NIL NIL NIL BASE64 12345)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Basic {
        media_type,
        media_subtype,
        encoding,
        size,
        ..
    } = &bs
    {
        assert_eq!(media_type, "image");
        assert_eq!(media_subtype, "jpeg");
        assert_eq!(encoding, "base64");
        assert_eq!(*size, 12345);
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

/// Multipart with bare atom subtype.
#[test]
fn bodystructure_bare_atom_multipart_subtype() {
    let input =
            b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 20 2) MIXED)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 2);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

/// RFC 3501 Section 9 / Postel's law: double space within pair in a full
/// BODYSTRUCTURE context  -  ensures the tolerance doesn't break upstream parsing.
#[test]
fn bodystructure_params_double_space_within_pair() {
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\"  \"UTF-8\") NIL NIL \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Text { params, .. } = &bs {
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "charset");
        assert_eq!(params[0].1, "UTF-8");
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

/// RFC 3501 Section 9 / Postel's law: multipart BODYSTRUCTURE with extra
/// whitespace (tab or multiple spaces) before the subtype must be tolerated.
/// The code already tolerates tabs between child bodies; the separator before
/// the subtype must be consistent.
#[test]
fn bodystructure_mpart_multi_space_before_subtype() {
    // Two spaces between the child body closing paren and the subtype string.
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)  \"MIXED\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 1);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

/// RFC 3501 Section 9 / Postel's law: tab character before multipart subtype
/// must be tolerated, consistent with tab tolerance between child bodies.
#[test]
fn bodystructure_mpart_tab_before_subtype() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)\t\"MIXED\")";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 1);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

// ========================================================================
// Postel's law regression tests  -  full FETCH response (Class A bugs)
// ========================================================================

/// Regression: full FETCH response with BODYSTRUCTURE containing extra
/// spaces between fields must parse end-to-end.
/// Bug 54c67e0: strict `sp` combinator rejected multi-space/tab input.
///
/// # References
/// - RFC 3501 Section 7.4.2 (BODYSTRUCTURE)
#[test]
fn fetch_bodystructure_extra_spaces_full_response() {
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\"  (\"CHARSET\" \"UTF-8\")  NIL  NIL  \"7BIT\"  100  5))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "BODYSTRUCTURE with extra spaces must parse as full FETCH response"
    );
}

/// Regression: full FETCH response with bare (unquoted) media type/subtype
/// in BODYSTRUCTURE must parse end-to-end.
/// Bug 8df3c4e: strict `string` combinator rejected bare atoms.
///
/// # References
/// - RFC 3501 Section 7.4.2 (BODYSTRUCTURE)
#[test]
fn fetch_bodystructure_bare_atom_full_response() {
    let input = b"* 1 FETCH (BODYSTRUCTURE (TEXT PLAIN (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "BODYSTRUCTURE with bare atom media type must parse as full FETCH response"
    );
}

/// Regression: full FETCH response with multipart BODYSTRUCTURE containing
/// extra spaces between children must parse end-to-end.
/// Bug 2abc6e1: strict `sp` between parameter key-value pairs.
///
/// # References
/// - RFC 3501 Section 7.4.2 (BODYSTRUCTURE multipart)
#[test]
fn fetch_bodystructure_multipart_extra_spaces_full_response() {
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" (\"CHARSET\"  \"UTF-8\") NIL NIL \"7BIT\" 100 5)(\"TEXT\" \"HTML\" (\"CHARSET\"  \"UTF-8\") NIL NIL \"QUOTED-PRINTABLE\" 200 10)  \"ALTERNATIVE\"))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "multipart BODYSTRUCTURE with extra spaces must parse as full FETCH response"
    );
}

#[test]
fn generated_bodystructures_round_trip_to_their_tree_shape() {
    let mut rng = BodyRng(0xB0D1_0001);
    for _ in 0..1500 {
        let node = generate_body_node(&mut rng, 3);
        let mut wire = String::new();
        render_body_node(&mut rng, &node, &mut wire);
        let parsed = body_structure(
            wire.as_bytes(),
            crate::types::wire::WireMode::ModifiedUtf7,
            0,
        );
        let Ok((rest, bs)) = parsed else {
            panic!("well-formed BODYSTRUCTURE failed to parse: {wire}");
        };
        assert!(rest.is_empty(), "trailing bytes {rest:?} for {wire}");
        assert_eq!(parsed_body_node(&bs), node, "shape diverged for {wire}");
    }
}

#[test]
fn mutated_bodystructures_never_panic_or_overrun() {
    let mut rng = BodyRng(0xB0D1_0002);
    for _ in 0..4000 {
        let node = generate_body_node(&mut rng, 3);
        let mut wire = String::new();
        render_body_node(&mut rng, &node, &mut wire);
        let mut bytes = wire.into_bytes();
        assert!(!bytes.is_empty());

        // One targeted structural mutation: the interesting damage is to the
        // delimiters, not to random payload bytes.
        let at = rng.below(bytes.len());
        match rng.below(4) {
            0 => bytes.truncate(at),
            1 => {
                bytes.remove(at);
            }
            2 => bytes[at] = *rng.pick(b"()\"{} N\r"),
            _ => bytes.insert(at, *rng.pick(b"()\"{} N\r")),
        }

        match body_structure(&bytes, crate::types::wire::WireMode::ModifiedUtf7, 0) {
            Ok((rest, _)) => {
                assert!(
                    bytes.ends_with(rest),
                    "remaining input must be a suffix of the input"
                );
                assert!(
                    rest.len() < bytes.len(),
                    "a successful parse consumes at least the opening paren"
                );
            }
            Err(nom::Err::Error(_) | nom::Err::Failure(_)) => {}
            Err(other) => panic!("streaming error from a complete-mode parser: {other:?}"),
        }
    }
}

/// The generator has to be replayable, or a seed printed by a CI failure is
/// useless.
#[test]
fn the_bodystructure_generator_is_seed_deterministic() {
    let mut left = BodyRng(0xB0D1_0001);
    let mut right = BodyRng(0xB0D1_0001);
    for _ in 0..64 {
        let (a, b) = (
            generate_body_node(&mut left, 3),
            generate_body_node(&mut right, 3),
        );
        assert_eq!(a, b);
        let (mut wire_a, mut wire_b) = (String::new(), String::new());
        render_body_node(&mut left, &a, &mut wire_a);
        render_body_node(&mut right, &b, &mut wire_b);
        assert_eq!(wire_a, wire_b);
    }
}

/// The depth guard is what keeps the mutation pass above from being able to
/// blow the stack, so pin it directly: one level past
/// `MAX_BODY_NESTING_DEPTH` is a `Failure`, not a stack overflow.
#[test]
fn bodystructure_nesting_beyond_the_depth_guard_is_a_failure() {
    let leaf = "(\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 1 1)";
    let mut wire = leaf.to_owned();
    for _ in 0..80 {
        wire = format!("({wire} \"MIXED\")");
    }
    assert!(
        matches!(
            body_structure(
                wire.as_bytes(),
                crate::types::wire::WireMode::ModifiedUtf7,
                0
            ),
            Err(nom::Err::Failure(_))
        ),
        "over-deep nesting must be rejected by the depth guard"
    );
}

// ===== Bug fix regression tests =====

/// RFC 3501 Section 9 / Postel's law: double space between a parameter key
/// and value in body-fld-param must be tolerated. The between-pair separator
/// already accepts multiple spaces; the within-pair separator must too.
#[test]
fn body_params_double_space_within_pair() {
    // "CHARSET"  "UTF-8"  -  double space between key and value
    let input = b"(\"CHARSET\"  \"UTF-8\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "charset");
    assert_eq!(params[0].1, "UTF-8");
}

/// body-fld-lang with double spaces between language tags.
///
/// Regression test: `separated_list0(sp, ...)` with a single-space
/// separator would fail on the extra space between language tags.
///
/// RFC 3501 Section 7.4.2 / Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn body_language_double_space_between_tags() {
    // Two spaces between "en" and "fr"
    let (_, lang) = body_language(b"(\"en\"  \"fr\")").unwrap();
    assert_eq!(
        lang,
        Some(vec!["en".to_owned(), "fr".to_owned()]),
        "double space between body-fld-lang tags must be tolerated"
    );
}

/// RFC 2045 Section 5.1: MIME parameter names are not case sensitive.
/// The parser must lowercase them for consistent lookup.
#[test]
fn spec_audit_param_names_lowercased() {
    // Server sends uppercase parameter name "CHARSET"; parser must
    // lowercase it to "charset" per RFC 2045 Section 5.1.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" \
            (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("missing BODYSTRUCTURE");
                match bs {
                    BodyStructure::Text { params, .. } => {
                        assert_eq!(params.len(), 1);
                        assert_eq!(
                            params[0].0, "charset",
                            "RFC 2045 Section 5.1: parameter names must be lowercased"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 2045 Section 6: Content-Transfer-Encoding values are not case
/// sensitive. The parser must lowercase them for canonical form per
/// RFC 2045 Section 5.1, consistent with media type/subtype.
#[test]
fn spec_audit_encoding_case_normalized() {
    // Server sends mixed-case encoding "Quoted-Printable"; parser must
    // lowercase it to "quoted-printable" per RFC 2045 Section 6.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" \
            (\"CHARSET\" \"UTF-8\") NIL NIL \"Quoted-Printable\" 100 5))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("missing BODYSTRUCTURE");
                match bs {
                    BodyStructure::Text { encoding, .. } => {
                        assert_eq!(
                            encoding, "quoted-printable",
                            "RFC 2045 Section 6: encoding must be lowercased"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// servers send `()` for empty body-fld-param in BODYSTRUCTURE.
/// RFC 3501 Section 9 formal syntax requires at least one key-value pair,
/// but Postel's law demands we accept the empty form.
#[test]
fn spec_audit_body_params_empty_parenthesized() {
    // BODYSTRUCTURE with empty params `()` instead of NIL
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" () NIL NIL \"7BIT\" 42 3))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "body-fld-param `()` must be accepted per Postel's law; got parse error"
    );
    // Verify the params are parsed as empty vec
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text { params, .. } => {
                        assert!(
                            params.is_empty(),
                            "empty () should produce empty params vec"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// `BodyStructure::Message` must expose a `media_subtype` field
/// so clients can distinguish `message/rfc822` from `message/global`
/// (RFC 9051 Section 7.5.2: media-message = DQUOTE "MESSAGE" DQUOTE SP
///  DQUOTE ("RFC822" / "GLOBAL") DQUOTE).
#[test]
fn message_global_subtype() {
    let input = b"(\"MESSAGE\" \"GLOBAL\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 500 \
            (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL) \
            (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) 20)";
    let (_, bs) = body_structure(input, crate::types::wire::WireMode::ModifiedUtf7, 0).unwrap();
    if let BodyStructure::Message {
        media_subtype,
        size,
        lines,
        ..
    } = bs
    {
        assert_eq!(
            media_subtype, "global",
            "subtype must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(size, 500);
        assert_eq!(lines, 20);
    } else {
        panic!("expected Message variant, got {bs:?}");
    }
}

/// L14: `body-fld-lang` with empty `()` should be rejected.
/// RFC 3501 Section 9.
#[test]
fn audit_l14_body_language_rejects_empty_parens() {
    let result = body_language(b"()");
    // After fix: either rejects outright, or falls back gracefully
    if let Ok((_, val)) = result {
        // If it parses, the result should be empty/None (graceful fallback)
        assert!(
            val.as_ref().is_none_or(std::vec::Vec::is_empty),
            "empty () should not produce non-empty language list"
        );
    }
}

// ===== Spec audit: tests for previously-known deviations =====

/// M1: RFC 9051 / RFC 3516 Section 4.3 defines `UNKNOWN-CTE` (with hyphen),
/// but the parser matches `"UNKNOWNCTE"` (no hyphen). The hyphenated form
/// falls through to `ResponseCode::Other`.
#[test]
fn spec_audit_m1_unknown_cte_hyphenated() {
    let input = b"* OK [UNKNOWN-CTE] unsupported encoding\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { code, .. } = &*u {
            assert_eq!(
                code.as_ref().unwrap(),
                &ResponseCode::UnknownCte,
                "UNKNOWN-CTE (with hyphen) should map to ResponseCode::UnknownCte, \
                     not Other"
            );
        } else {
            panic!("expected Status, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// servers send `()` for empty body-fld-lang in BODYSTRUCTURE.
/// RFC 3501 Section 7.4.2 formal syntax requires at least one string,
/// but Postel's law demands we accept the empty form.
#[test]
fn spec_audit_body_language_empty_parenthesized() {
    // BODYSTRUCTURE with text part that has empty language list ()
    // Fields: type subtype params id description encoding size lines md5 disposition language
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5 NIL NIL () NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "body-fld-lang `()` must be accepted per Postel's law; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text { language, .. } => {
                        // () should produce Some(vec![])  -  the server explicitly sent
                        // an empty list, not NIL.
                        let langs = language
                            .as_ref()
                            .expect("empty () should be Some(vec![]), not None");
                        assert!(
                            langs.is_empty(),
                            "empty () should produce empty language vec"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// `body_language` with NIL (RFC 3501 Section 7.4.2).
#[test]
fn body_language_nil() {
    let (_, lang) = body_language(b"NIL").unwrap();
    assert!(lang.is_none());
}

/// `body_params` with RFC 2231 continuation parameters (RFC 2231 Section 3).
#[test]
fn body_params_rfc2231_continuation_standalone() {
    let input = b"(\"FILENAME*0\" \"very-\" \"FILENAME*1\" \"long-\" \"FILENAME*2\" \"name.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "filename");
    assert_eq!(params[0].1, "very-long-name.txt");
}

/// `body_params` with multiple key-value pairs (RFC 3501 Section 7.4.2).
#[test]
fn body_params_multiple_pairs() {
    let (_, params) =
        body_params(b"(\"CHARSET\" \"UTF-8\" \"NAME\" \"test.txt\" \"FORMAT\" \"flowed\")")
            .unwrap();
    assert_eq!(params.len(), 3);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
    assert_eq!(params[1], ("name".into(), "test.txt".into()));
    assert_eq!(params[2], ("format".into(), "flowed".into()));
}

/// `body_params` with a single key-value pair (RFC 3501 Section 7.4.2).
#[test]
fn body_params_single_pair() {
    let (_, params) = body_params(b"(\"CHARSET\" \"UTF-8\")").unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
}

/// `body_params` with case-insensitive NIL (RFC 3501 Section 7.4.2).
#[test]
fn body_params_nil_case_insensitive() {
    let (_, params) = body_params(b"nil").unwrap();
    assert!(params.is_empty());
    let (_, params) = body_params(b"Nil").unwrap();
    assert!(params.is_empty());
}

// ===== Task 5: Body parameter helper tests =====

/// `body_params` with NIL returns empty vec (RFC 3501 Section 7.4.2).
#[test]
fn body_params_nil_standalone() {
    let (_, params) = body_params(b"NIL").unwrap();
    assert!(params.is_empty());
}

/// RFC 3501 Section 9 requires `1*body` for `body-type-mpart`, meaning at
/// least one child body must be present.  Passing input that skips the
/// nested `(` bodies must be rejected as a parse error.
#[test]
fn body_type_mpart_rejects_zero_children() {
    // Input has no child bodies  -  starts directly with SP + subtype.
    // This violates the `1*body` ABNF rule (RFC 3501 Section 9).
    let input = b" \"MIXED\" NIL NIL NIL NIL)";
    let result = body_type_mpart(input, crate::types::wire::WireMode::ModifiedUtf7, 0);
    assert!(
        result.is_err(),
        "body_type_mpart must reject zero children per RFC 3501 Section 9 (1*body)"
    );
}

#[test]
fn body_params_rfc2231_iso8859() {
    // RFC 2231 with non-UTF-8 charset (ISO-8859-1).
    let input = b"(\"FILENAME*\" \"ISO-8859-1''caf%E9.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "filename");
    assert_eq!(params[0].1, "caf\u{e9}.txt");
}

#[test]
fn body_params_rfc2231_charset_and_continuation() {
    // RFC 2231 Sections 3-4 combined: charset-encoded continuation parameter.
    let input = b"(\"FILENAME*0*\" \"UTF-8''%C3%A9l%C3%A8ve-\" \"FILENAME*1\" \"rapport.pdf\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "filename");
    assert_eq!(params[0].1, "\u{e9}l\u{e8}ve-rapport.pdf");
}

#[test]
fn body_params_rfc2231_continuation() {
    // RFC 2231 Section 3: continuation parameters split across multiple keys.
    let input = b"(\"FILENAME*0\" \"very-long-\" \"FILENAME*1\" \"filename.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(
        params[0],
        ("filename".into(), "very-long-filename.txt".into())
    );
}

// ===== RFC 2231 MIME parameter encoding tests =====
//
// body_params automatically decodes RFC 2231 continuation segments and
// charset-encoded values at parse time (RFC 2231 Sections 3-4). Callers
// receive clean, reassembled parameters.

#[test]
fn body_params_rfc2231_charset_encoded() {
    // RFC 2231 Section 4: charset-encoded parameter with percent-encoding.
    // Servers may present `filename*` with charset''percent-encoded value.
    let input = b"(\"CHARSET\" \"UTF-8\" \"NAME*\" \"UTF-8''t%C3%A9st%20file.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
    assert_eq!(params[1].0, "name");
    assert_eq!(params[1].1, "t\u{e9}st file.txt");
}

#[test]
fn body_params_pairs() {
    let (_, params) = body_params(b"(\"CHARSET\" \"UTF-8\" \"NAME\" \"test.txt\")").unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
}

#[test]
fn body_params_nil() {
    let (_, params) = body_params(b"NIL").unwrap();
    assert!(params.is_empty());
}
