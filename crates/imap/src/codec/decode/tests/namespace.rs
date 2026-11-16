use super::*;

#[test]
fn untagged_namespace_full() {
    let input = b"NAMESPACE ((\"\" \"/\")) ((\"~\" \"/\")) ((\"#shared.\" \".\"))\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert_eq!(other.len(), 1);
        assert_eq!(other[0].prefix, "~");
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].prefix, "#shared.");
        assert_eq!(shared[0].delimiter, Some('.'));
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

#[test]
fn untagged_namespace_all_nil() {
    let input = b"NAMESPACE NIL NIL NIL\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert!(personal.is_empty());
        assert!(other.is_empty());
        assert!(shared.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

#[test]
fn namespace_with_extension_data() {
    // Namespace descriptors may have extension data after the delimiter.
    // RFC 2342 Section6: Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"
    let input = b"NAMESPACE ((\"\" \"/\" \"X-EXT\" (\"val\"))) NIL NIL\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert_eq!(
            personal[0].extensions,
            vec![("X-EXT".to_string(), vec!["val".to_string()])]
        );
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

// ===== NAMESPACE nested extension data (RFC 2342) =====
// RFC 2342 example: * NAMESPACE (("" "/")("#mh/" "/" "X-PARAM" ("FLAG1" "FLAG2"))) NIL NIL
// The extension data contains nested parentheses which the current skipper mishandles.

/// NAMESPACE with nested parenthesized extension data (RFC 2342).
#[test]
fn namespace_nested_extension_data() {
    // Extension data with nested parens: "X-PARAM" ("FLAG1" "FLAG2")
    let input = b"NAMESPACE ((\"\" \"/\" \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let result = parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7);
    assert!(
        result.is_ok(),
        "NAMESPACE with nested extension parens should parse per RFC 2342, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert_eq!(personal.len(), 1, "should have one personal namespace");
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert!(other.is_empty());
        assert!(shared.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// NAMESPACE with multiple descriptors and nested extensions (RFC 2342).
#[test]
fn namespace_multiple_descriptors_nested_ext() {
    let input =
        b"NAMESPACE ((\"\" \"/\")(\"#mh/\" \"/\" \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let result = parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7);
    assert!(
        result.is_ok(),
        "NAMESPACE with multiple descriptors and nested extensions should parse, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 2);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[1].prefix, "#mh/");
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

// ===== NAMESPACE extension data must be preserved (RFC 2342 Section6) =====

/// NAMESPACE with extension data must preserve extensions in the parsed output.
/// RFC 2342 Section6 ABNF: `*(Namespace_Response_Extension)`
/// where `Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"`.
#[test]
fn namespace_extension_data_preserved() {
    let input = b"NAMESPACE ((\"\" \"/\" \"TRANSLATION\" (\"strstrumpf\"))) NIL NIL\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert_eq!(
            personal[0].extensions,
            vec![("TRANSLATION".to_string(), vec!["strstrumpf".to_string()])]
        );
        assert!(other.is_empty());
        assert!(shared.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// NAMESPACE with multiple extension key-value-list pairs (RFC 2342 Section6).
#[test]
fn namespace_multiple_extensions_preserved() {
    let input = b"NAMESPACE ((\"\" \"/\" \"TRANSLATION\" (\"strstrumpf\") \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].extensions.len(), 2);
        assert_eq!(
            personal[0].extensions[0],
            ("TRANSLATION".to_string(), vec!["strstrumpf".to_string()])
        );
        assert_eq!(
            personal[0].extensions[1],
            (
                "X-PARAM".to_string(),
                vec!["FLAG1".to_string(), "FLAG2".to_string()]
            )
        );
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// NAMESPACE without extension data should have empty extensions vec.
#[test]
fn namespace_no_extension_data_empty_vec() {
    let input = b"NAMESPACE ((\"\" \"/\")) NIL NIL\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert!(personal[0].extensions.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

// M14: NAMESPACE delimiter multibyte UTF-8
#[test]
fn namespace_multibyte_delimiter() {
    // Use a 2-byte UTF-8 character as delimiter (U+00F7 division sign = 0xC3 0xB7)
    let mut input: Vec<u8> = b"* NAMESPACE ((\"\" ".to_vec();
    input.push(b'"');
    input.extend_from_slice(&[0xC3, 0xB7]); // ÷ in UTF-8
    input.push(b'"');
    input.extend_from_slice(b")) NIL NIL\r\n");
    let (_, resp) = parse_response(&input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Namespace { personal, .. } = *boxed {
            assert_eq!(personal[0].delimiter, Some('\u{00F7}'));
        } else {
            panic!("expected Namespace");
        }
    } else {
        panic!("expected Untagged");
    }
}

// L13: NAMESPACE with extension data is parsed and preserved.
// RFC 2342 Section6 allows extension key-value pairs after the delimiter.
#[test]
fn namespace_with_extension_data_l13() {
    let input = b"* NAMESPACE ((\"\" \"/\" \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Namespace { personal, .. } = *boxed {
            assert_eq!(personal.len(), 1);
            assert_eq!(personal[0].prefix, "");
            assert_eq!(personal[0].delimiter, Some('/'));
        } else {
            panic!("expected Namespace");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// L7: NAMESPACE with empty `()` should be accepted per Postel's law.
/// RFC 2342 Section 6 strictly requires `1*` (at least one descriptor),
/// but non-conformant servers send `()` instead of `NIL` for empty
/// namespace categories. Accepted per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn audit_l7_namespace_accepts_empty_parens() {
    // Empty parens in NAMESPACE are technically invalid per the ABNF,
    // but accepted per Postel's law to avoid losing the entire response.
    let input = b"* NAMESPACE () NIL NIL\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::Namespace {
                personal,
                other,
                shared,
            } = *boxed
            {
                assert!(
                    personal.is_empty(),
                    "`()` should produce empty personal namespace"
                );
                assert!(other.is_empty());
                assert!(shared.is_empty());
            } else {
                panic!("expected Namespace, got {boxed:?}");
            }
        }
        other => panic!("Expected Untagged(Namespace), got {other:?}"),
    }
}

/// Postel's law (RFC 2342 Section 6): a non-conformant server sending a
/// multi-character namespace delimiter (e.g., "ab") should not cause a hard
/// parse failure. The parser takes the first character and tolerates the rest.
#[test]
fn test_namespace_tolerates_multichar_delimiter() {
    let input = b"* NAMESPACE ((\"\" \"ab\")) NIL NIL\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::Namespace { personal, .. } = *boxed {
                assert_eq!(personal.len(), 1);
                assert_eq!(
                    personal[0].delimiter,
                    Some('a'),
                    "multi-char namespace delimiter should take the first character"
                );
            } else {
                panic!("expected Namespace response, got {boxed:?}");
            }
        }
        other => panic!("expected Untagged(Namespace), got {other:?}"),
    }
}

/// non-conformant servers send `()` instead of `NIL` for an empty
/// namespace category. RFC 2342 Section 6 strictly requires at least one
/// descriptor in the parenthesized form, but per Postel's law
/// (RFC 1122 Section 1.2.2) we should accept this gracefully.
#[test]
fn spec_audit_namespace_empty_parens() {
    // "other users" namespace is `()` instead of NIL
    let input = b"* NAMESPACE ((\"\" \"/\")) () NIL\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "namespace `()` must be accepted per Postel's law; got parse error: {result:?}"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => {
            if let UntaggedResponse::Namespace {
                personal,
                other,
                shared,
            } = *inner
            {
                assert_eq!(personal.len(), 1, "should have one personal namespace");
                assert_eq!(personal[0].prefix, "");
                assert_eq!(personal[0].delimiter, Some('/'));
                assert!(
                    other.is_empty(),
                    "`()` should produce empty other namespace"
                );
                assert!(
                    shared.is_empty(),
                    "NIL should produce empty shared namespace"
                );
            } else {
                panic!("expected Namespace, got {inner:?}");
            }
        }
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ── 8. NIL optional fields (L1892) ──

/// RFC 2342 Section 6: namespace descriptor with NIL delimiter.
/// The delimiter field `None` maps to `None` in the parsed `NamespaceDescriptor`.
#[test]
fn namespace_descriptor_nil_delimiter() {
    // Namespace with NIL delimiter (flat namespace, no hierarchy)
    let input = b"NAMESPACE ((\"\" NIL)) NIL NIL\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, None);
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// RFC 2342 Section 6: namespace descriptor with empty quoted string delimiter.
/// Empty string `""` is treated as `None` (no delimiter).
#[test]
fn namespace_descriptor_empty_string_delimiter() {
    let input = b"NAMESPACE ((\"\" \"\")) NIL NIL\r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].delimiter, None);
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// Postel's law: NAMESPACE response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_namespace() {
    let input = b"NAMESPACE ((\"\" \"/\")) NIL NIL  \r\n";
    let (_, resp) =
        parse_untagged_namespace(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(matches!(resp, UntaggedResponse::Namespace { .. }));
}
