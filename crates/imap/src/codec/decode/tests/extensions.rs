use super::*;

// ===== METADATA (RFC 5464) parser tests =====

#[test]
fn parse_metadata_response() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" \"My comment\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"My comment".as_slice()));
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_multiple_entries() {
    let input =
        b"* METADATA \"INBOX\" (\"/private/comment\" \"hello\" \"/shared/vendor/x\" \"world\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(entries[1].name, "/shared/vendor/x");
        assert_eq!(entries[1].value.as_deref(), Some(b"world".as_slice()));
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_nil_value() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert!(entries[0].value.is_none());
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_literal_value() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" {5}\r\nhello)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"hello".as_slice()));
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_empty_entries() {
    let input = b"* METADATA \"INBOX\" ()\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(entries.is_empty());
        return;
    }
    panic!("expected Metadata response");
}

// ===== QUOTA (RFC 2087) parser tests =====

#[test]
fn parse_quota_single_resource() {
    let input = b"* QUOTA \"\" (STORAGE 10 512)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 10);
        assert_eq!(resources[0].limit, 512);
        return;
    }
    panic!("expected Quota response");
}

#[test]
fn parse_quota_multiple_resources() {
    let input = b"* QUOTA \"user.alice\" (STORAGE 100 1024 MESSAGE 50 500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "user.alice");
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 100);
        assert_eq!(resources[0].limit, 1024);
        assert_eq!(resources[1].name, "MESSAGE");
        assert_eq!(resources[1].usage, 50);
        assert_eq!(resources[1].limit, 500);
        return;
    }
    panic!("expected Quota response with multiple resources");
}

#[test]
fn parse_quotaroot_single_root() {
    let input = b"* QUOTAROOT INBOX \"\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec![""]);
        return;
    }
    panic!("expected QuotaRoot response");
}

#[test]
fn parse_quotaroot_multiple_roots() {
    let input = b"* QUOTAROOT INBOX \"\" \"user.bob\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec!["", "user.bob"]);
        return;
    }
    panic!("expected QuotaRoot response with multiple roots");
}

#[test]
fn parse_quotaroot_no_roots() {
    let input = b"* QUOTAROOT INBOX\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(roots.is_empty());
        return;
    }
    panic!("expected QuotaRoot response with no roots");
}

#[test]
fn parse_quota_case_insensitive() {
    let input = b"* quota \"\" (STORAGE 5 100)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name, "STORAGE");
        return;
    }
    panic!("expected case-insensitive Quota response");
}

// ===== ACL (RFC 4314) parser tests =====

#[test]
fn parse_acl_single_entry() {
    let input = b"* ACL INBOX fred lrswipcda\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].identifier, "fred");
        assert_eq!(entries[0].rights, "lrswipcda");
        return;
    }
    panic!("expected ACL response");
}

#[test]
fn parse_acl_multiple_entries() {
    let input = b"* ACL INBOX fred lrswipcda chris lrs\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].identifier, "fred");
        assert_eq!(entries[0].rights, "lrswipcda");
        assert_eq!(entries[1].identifier, "chris");
        assert_eq!(entries[1].rights, "lrs");
        return;
    }
    panic!("expected ACL response with multiple entries");
}

#[test]
fn parse_acl_no_entries() {
    let input = b"* ACL INBOX\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(entries.is_empty());
        return;
    }
    panic!("expected ACL response with no entries");
}

#[test]
fn parse_myrights() {
    let input = b"* MYRIGHTS INBOX lrswipcda\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::MyRights { mailbox, rights } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(rights, "lrswipcda");
        return;
    }
    panic!("expected MYRIGHTS response");
}

#[test]
fn parse_acl_case_insensitive() {
    let input = b"* acl INBOX alice lrs\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].identifier, "alice");
        return;
    }
    panic!("expected case-insensitive ACL response");
}

// ===== Task 1: QUOTA/QUOTAROOT parser tests (RFC 2087 Section 5.1-5.2) =====

/// QUOTA with a named root and a single resource triplet (RFC 2087 Section 5.1).
#[test]
fn quota_named_root_single_resource() {
    let input = b"QUOTA \"user.alice\" (STORAGE 200 1024)\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { root, resources } = resp {
        assert_eq!(root, "user.alice");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 200);
        assert_eq!(resources[0].limit, 1024);
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with multiple resource triplets (RFC 2087 Section 5.1).
#[test]
fn quota_multiple_resources_triplets() {
    let input = b"QUOTA \"\" (STORAGE 500 2048 MESSAGE 100 1000 MAILBOX 5 10)\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { root, resources } = resp {
        assert_eq!(root, "");
        assert_eq!(resources.len(), 3);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 500);
        assert_eq!(resources[0].limit, 2048);
        assert_eq!(resources[1].name, "MESSAGE");
        assert_eq!(resources[1].usage, 100);
        assert_eq!(resources[1].limit, 1000);
        assert_eq!(resources[2].name, "MAILBOX");
        assert_eq!(resources[2].usage, 5);
        assert_eq!(resources[2].limit, 10);
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with an empty resource list (RFC 2087 Section 5.1).
#[test]
fn quota_empty_resources() {
    let input = b"QUOTA \"root\" ()\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { root, resources } = resp {
        assert_eq!(root, "root");
        assert!(resources.is_empty());
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with large 64-bit usage/limit values (RFC 2087 Section 5.1).
#[test]
fn quota_large_values() {
    let input = b"QUOTA \"\" (STORAGE 4294967296 8589934592)\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { resources, .. } = resp {
        assert_eq!(resources[0].usage, 4_294_967_296);
        assert_eq!(resources[0].limit, 8_589_934_592);
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with truncated input should fail (RFC 2087 Section 5.1).
#[test]
fn quota_truncated_input() {
    // Missing CRLF
    assert!(parse_untagged_quota(b"QUOTA \"\" (STORAGE 10 512)").is_err());
    // Missing closing paren of resource list
    assert!(parse_untagged_quota(b"QUOTA \"\" (STORAGE 10 512\r\n").is_err());
    // Missing resource values
    assert!(parse_untagged_quota(b"QUOTA \"\" (STORAGE\r\n").is_err());
}

/// QUOTA with garbage data should fail (RFC 2087 Section 5.1).
#[test]
fn quota_garbage_data() {
    assert!(parse_untagged_quota(b"QUOTA !!!GARBAGE!!!\r\n").is_err());
}

/// QUOTAROOT with a single root (RFC 2087 Section 5.2).
#[test]
fn quotaroot_single_root_atom() {
    // Mailbox as atom, root as quoted string
    let input = b"QUOTAROOT INBOX \"root\"\r\n";
    let (_, resp) =
        parse_untagged_quotaroot(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::QuotaRoot { mailbox, roots } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec!["root"]);
    } else {
        panic!("expected QuotaRoot, got {resp:?}");
    }
}

/// QUOTAROOT with no roots  -  mailbox has no quota roots (RFC 2087 Section 5.2).
#[test]
fn quotaroot_no_roots_at_all() {
    let input = b"QUOTAROOT \"Sent Items\"\r\n";
    let (_, resp) =
        parse_untagged_quotaroot(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::QuotaRoot { mailbox, roots } = resp {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert!(roots.is_empty());
    } else {
        panic!("expected QuotaRoot, got {resp:?}");
    }
}

/// QUOTAROOT with truncated input should fail (RFC 2087 Section 5.2).
#[test]
fn quotaroot_truncated_input() {
    // Missing CRLF after mailbox
    assert!(
        parse_untagged_quotaroot(
            b"QUOTAROOT INBOX",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// ACL with multiple entries (RFC 4314 Section 3.6).
#[test]
fn acl_multiple_entries_varied() {
    let input = b"ACL INBOX alice lrswipcda bob lr chris lrswip\r\n";
    let (_, resp) = parse_untagged_acl(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Acl { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].identifier, "alice");
        assert_eq!(entries[0].rights, "lrswipcda");
        assert_eq!(entries[1].identifier, "bob");
        assert_eq!(entries[1].rights, "lr");
        assert_eq!(entries[2].identifier, "chris");
        assert_eq!(entries[2].rights, "lrswip");
    } else {
        panic!("expected Acl, got {resp:?}");
    }
}

/// ACL with no entries  -  empty ACL for the mailbox (RFC 4314 Section 3.6).
#[test]
fn acl_no_entries_empty() {
    let input = b"ACL \"Archive\"\r\n";
    let (_, resp) = parse_untagged_acl(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Acl { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "Archive");
        assert!(entries.is_empty());
    } else {
        panic!("expected Acl, got {resp:?}");
    }
}

/// ACL with truncated input should fail (RFC 4314 Section 3.6).
#[test]
fn acl_truncated_input() {
    // Missing CRLF
    assert!(
        parse_untagged_acl(
            b"ACL INBOX alice lrs",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// ACL with non-astring mailbox should fail (RFC 4314 Section 3.6).
#[test]
fn acl_garbage_data() {
    // Input missing the "ACL " prefix entirely, so tag_no_case fails
    assert!(
        parse_untagged_acl(
            b"GARBAGE data\r\n",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// MYRIGHTS with truncated input should fail (RFC 4314 Section 3.8).
#[test]
fn myrights_truncated_input() {
    // Missing rights and CRLF
    assert!(
        parse_untagged_myrights(
            b"MYRIGHTS INBOX",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
    // Missing CRLF
    assert!(
        parse_untagged_myrights(
            b"MYRIGHTS INBOX lrs",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// MYRIGHTS with garbage data should fail (RFC 4314 Section 3.8).
#[test]
fn myrights_garbage_data() {
    assert!(
        parse_untagged_myrights(
            b"MYRIGHTS !!!GARBAGE\r\n",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

// ===== Task 3: METADATA parser tests (RFC 5464 Section 4.4) =====

/// METADATA with a single entry (RFC 5464 Section 4.4).
#[test]
fn metadata_single_entry() {
    let input = b"METADATA \"INBOX\" (\"/private/comment\" \"My folder\")\r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Metadata { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"My folder".as_slice()));
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with multiple entries (RFC 5464 Section 4.4).
#[test]
fn metadata_multiple_entries_mixed() {
    let input = b"METADATA \"INBOX\" (\"/private/comment\" \"hello\" \"/shared/comment\" \"world\" \"/private/vendor/foo\" \"bar\")\r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Metadata { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(entries[1].name, "/shared/comment");
        assert_eq!(entries[1].value.as_deref(), Some(b"world".as_slice()));
        assert_eq!(entries[2].name, "/private/vendor/foo");
        assert_eq!(entries[2].value.as_deref(), Some(b"bar".as_slice()));
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with complex vendor key paths (RFC 5464 Section 3.2).
#[test]
fn metadata_complex_vendor_keys() {
    let input = b"METADATA \"INBOX\" (\"/shared/vendor/example.com/widget\" \"config-value\")\r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Metadata { entries, .. } = resp {
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/shared/vendor/example.com/widget");
        assert_eq!(
            entries[0].value.as_deref(),
            Some(b"config-value".as_slice())
        );
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with NIL value  -  entry does not exist (RFC 5464 Section 4.4).
#[test]
fn metadata_nil_value_deleted() {
    let input =
        b"METADATA \"INBOX\" (\"/private/comment\" NIL \"/shared/comment\" \"present\")\r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Metadata { entries, .. } = resp {
        assert_eq!(entries.len(), 2);
        assert!(entries[0].value.is_none());
        assert_eq!(entries[1].value.as_deref(), Some(b"present".as_slice()));
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with all-NIL values (RFC 5464 Section 4.4).
#[test]
fn metadata_all_nil_values() {
    let input = b"METADATA \"INBOX\" (\"/private/comment\" NIL \"/shared/comment\" NIL)\r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Metadata { entries, .. } = resp {
        assert_eq!(entries.len(), 2);
        assert!(entries[0].value.is_none());
        assert!(entries[1].value.is_none());
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with truncated input should fail (RFC 5464 Section 4.4).
#[test]
fn metadata_truncated_input() {
    // Missing closing paren
    assert!(
        parse_untagged_metadata(
            b"METADATA \"INBOX\" (\"/private/comment\" \"val\"\r\n",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
    // Missing CRLF
    assert!(
        parse_untagged_metadata(
            b"METADATA \"INBOX\" (\"/private/comment\" \"val\")",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
    // Missing value after key
    assert!(
        parse_untagged_metadata(
            b"METADATA \"INBOX\" (\"/private/comment\"",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// METADATA with garbage data should fail (RFC 5464 Section 4.4).
#[test]
fn metadata_garbage_data() {
    assert!(
        parse_untagged_metadata(
            b"METADATA !!!GARBAGE\r\n",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// METADATA via `parse_response` to verify integration (RFC 5464 Section 4.4).
#[test]
fn metadata_via_parse_response() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" \"test\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        return;
    }
    panic!("expected Metadata via parse_response");
}

/// A future METADATA sub-code that carries additional data (e.g.,
/// `[METADATA XFOO 42]`) must not cause the entire response code
/// parse to fail. The unknown sub-code fallback must consume any
/// trailing value before the closing `]`, matching the generic
/// unknown response code handler's behavior.
///
/// Regression: the METADATA `_` arm returned `ResponseCode::Other`
/// with `value: None` without consuming trailing data, causing
/// `char(']')` to fail on ` 42]`.
#[test]
fn metadata_unknown_subcode_with_value() {
    let input = b"A001 NO [METADATA XFOO 42] some text\r\n";
    let (rest, resp) = parse_response(input).expect(
        "METADATA unknown sub-code with value must parse \
             (forward compatibility per RFC 5464)",
    );
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert!(
            tagged.code.is_some(),
            "response code must be preserved, got None"
        );
    } else {
        panic!("expected Tagged");
    }
}

/// RFC 5464 Section 4.2.1: When a non-conformant server sends non-ASCII
/// bytes in the METADATA sub-atom (e.g., `LONGENTRI\x80S`), the parser
/// should still produce a meaningful `ResponseCode::Other` with the
/// lossy-decoded sub-atom rather than failing the response code parse
/// entirely.
///
/// With `unwrap_or("")`, the sub-atom becomes `""` and falls through to
/// `ResponseCode::Other { name: "METADATA ", value: None }`  -  but this
/// leaves unconsumed bytes (` 2048`) before the closing `]`, causing the
/// response code parser to fail. The tagged response then has `code: None`,
/// losing the response code entirely.
///
/// With `from_utf8_lossy`, the replacement characters keep the string
/// non-empty, and unrecognized sub-codes produce a meaningful `Other` name
/// that correctly consumes the remaining value text.
#[test]
fn metadata_sub_atom_non_ascii_uses_lossy_conversion() {
    // Sub-atom "TOOMANY" with a non-ASCII byte: "TOOMAN\x80Y"
    // 0x80 alone is an invalid UTF-8 continuation byte without a leader.
    // Using TOOMANY because it has no trailing number, so the parser
    // only needs to match the sub-atom and then hit `]`.
    let input = b"A001 NO [METADATA TOOMAN\x80Y] too many annotations\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::No);
        // With from_utf8_lossy: sub = "TOOMAN\u{FFFD}Y" (uppercased),
        // which doesn't match "TOOMANY", so falls to Other with a
        // meaningful name like "METADATA TOOMAN\u{FFFD}Y".
        //
        // With unwrap_or(""): sub = "", name = "METADATA ", and the
        // response code parse fails entirely -> code is None.
        assert!(
            tagged.code.is_some(),
            "response code should be Some (not None); \
                 unwrap_or(\"\") causes the response code parser to fail entirely"
        );
        match tagged.code {
            Some(ResponseCode::Other { ref name, .. }) => {
                assert!(
                    name.starts_with("METADATA TOOMAN"),
                    "sub-atom should be preserved via lossy conversion, got: {name:?}"
                );
            }
            other => {
                panic!("expected ResponseCode::Other with METADATA prefix, got {other:?}");
            }
        }
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

// ==============================================================    // Audit tests (2026-03-16)
// ==============================================================
/// H1: METADATA entry names must accept `astring`, not just `string`.
/// RFC 5464 Section 5: `entry = astring`.
#[test]
fn audit_h1_metadata_entry_names_accept_astring() {
    let input = b"* METADATA \"INBOX\" (/shared/comment \"A shared comment\")\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Metadata { mailbox, entries } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].name, "/shared/comment");
        } else {
            panic!("expected Metadata, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 2087 Section 5.2: QUOTAROOT with trailing SP after roots
/// triggers the CRLF-check `break` at L2009. The loop breaks but
/// `input` still has the trailing space, so `crlf()` fails and
/// the response falls through to Unknown. This exercises the break path.
#[test]
fn quotaroot_trailing_space_tolerated_per_postels_law() {
    // Trailing space after last root is now tolerated per Postel's law
    // (RFC 1122 Section 1.2.2). The parser strips trailing whitespace
    // before CRLF, matching the pattern used by CAPABILITY, FLAGS, etc.
    let input = b"QUOTAROOT INBOX \"\" \r\n";
    let result = parse_untagged_quotaroot(input, crate::types::wire::WireMode::ModifiedUtf7);
    assert!(
        result.is_ok(),
        "trailing space after last root should be tolerated (Postel's law); \
             got: {result:?}"
    );
}

/// RFC 2087 Section 5.2: QUOTAROOT with no roots at all  -  the loop
/// breaks immediately because `sp()` fails on `\r\n`.
#[test]
fn quotaroot_no_roots_loop_break() {
    let input = b"* QUOTAROOT INBOX\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(roots.is_empty());
        return;
    }
    panic!("expected QuotaRoot response");
}

/// Postel's law: QUOTA response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_quota() {
    let input = b"QUOTA \"\" (STORAGE 10 512)  \r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    assert!(matches!(resp, UntaggedResponse::Quota { .. }));
}

/// Postel's law: QUOTAROOT response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_quotaroot() {
    let input = b"QUOTAROOT INBOX root1  \r\n";
    let (_, resp) =
        parse_untagged_quotaroot(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(matches!(resp, UntaggedResponse::QuotaRoot { .. }));
}

/// Postel's law: ACL response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_acl() {
    let input = b"ACL INBOX alice lrs  \r\n";
    let (_, resp) = parse_untagged_acl(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(matches!(resp, UntaggedResponse::Acl { .. }));
}

/// Postel's law: MYRIGHTS response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_myrights() {
    let input = b"MYRIGHTS INBOX lrs  \r\n";
    let (_, resp) =
        parse_untagged_myrights(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(matches!(resp, UntaggedResponse::MyRights { .. }));
}

/// Postel's law: METADATA response (Form 1, parenthesized) with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_metadata_form1() {
    let input = b"METADATA \"INBOX\" (/private/comment \"value\")  \r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(matches!(resp, UntaggedResponse::Metadata { .. }));
}

/// Postel's law: METADATA response (Form 2, unsolicited entry-list) with trailing spaces.
#[test]
fn trailing_whitespace_metadata_form2() {
    let input = b"METADATA \"INBOX\" /private/comment  \r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(matches!(resp, UntaggedResponse::Metadata { .. }));
}

/// `STATUS` response (used for non-selected mailbox NOTIFY events) parses correctly.
#[test]
fn status_response_for_notify_event() {
    let input = b"* STATUS \"Lists/Lemonade\" (UIDVALIDITY 4 UIDNEXT 10000 MESSAGES 501)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = *u {
            assert_eq!(mailbox.as_str(), "Lists/Lemonade");
            assert!(
                items.len() >= 3,
                "should have UIDVALIDITY, UIDNEXT, MESSAGES, got: {items:?}"
            );
        } else {
            panic!("expected MailboxStatus, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}
