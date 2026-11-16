use super::*;

#[test]
fn untagged_list() {
    let (_, resp) = parse_response(b"* LIST (\\HasNoChildren) \"/\" \"INBOX\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(info.delimiter, Some('/'));
            assert!(info.attributes.contains(&MailboxAttribute::HasNoChildren));
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_list_nil_delimiter() {
    let (_, resp) = parse_response(b"* LIST () NIL \"INBOX\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.delimiter, None);
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// `\Noinferiors` attribute parsed in LIST response (RFC 3501 Section 7.2.2).
#[test]
fn untagged_list_noinferiors() {
    let (_, resp) = parse_response(b"* LIST (\\Noinferiors) \"/\" \"Leaf\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "Leaf");
            assert!(info.attributes.contains(&MailboxAttribute::NoInferiors));
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_list_special_use() {
    let (_, resp) = parse_response(b"* LIST (\\Sent \\HasNoChildren) \".\" \"Sent\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert!(info.attributes.contains(&MailboxAttribute::Sent));
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with trailing space after mailbox name.
///
/// RFC 3501 Section 7.2.2: `"LIST" SP mailbox-list`
#[test]
fn list_trailing_space() {
    let (_, resp) = parse_response(b"* LIST (\\HasNoChildren) \"/\" INBOX \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LSUB with trailing space after mailbox name.
///
/// RFC 3501 Section 7.2.3: `"LSUB" SP mailbox-list`
#[test]
fn lsub_trailing_space() {
    let (_, resp) = parse_response(b"* LSUB () \"/\" INBOX \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_status_mailbox() {
    let input = b"STATUS \"INBOX\" (MESSAGES 17 UNSEEN 5 UIDNEXT 100 UIDVALIDITY 1234)\r\n";
    let (_, resp) =
        parse_untagged_status_mailbox(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(items.len(), 4);
        assert_eq!(items[0], StatusItem::Messages(17));
        assert_eq!(items[1], StatusItem::Unseen(5));
        assert_eq!(items[2], StatusItem::UidNext(100));
        assert_eq!(items[3], StatusItem::UidValidity(1234));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

#[test]
fn status_item_size() {
    let (_, items) = status_items(b"SIZE 1048576").unwrap();
    assert_eq!(items, vec![StatusItem::Size(1_048_576)]);
}

#[test]
fn status_item_unknown_skipped() {
    // Unknown attributes are now gracefully skipped, not errors.
    let (_, items) = status_items(b"BOGUS 42").unwrap();
    assert!(items.is_empty(), "unknown attribute should be skipped");
}

// ===== Mailbox attribute tests =====

#[test]
fn mailbox_attribute_all_base() {
    assert_eq!(
        parse_mailbox_attribute("\\Noinferiors"),
        MailboxAttribute::NoInferiors
    );
    assert_eq!(
        parse_mailbox_attribute("\\Noselect"),
        MailboxAttribute::NoSelect
    );
    assert_eq!(
        parse_mailbox_attribute("\\NonExistent"),
        MailboxAttribute::NonExistent
    );
    assert_eq!(
        parse_mailbox_attribute("\\HasChildren"),
        MailboxAttribute::HasChildren
    );
    assert_eq!(
        parse_mailbox_attribute("\\HasNoChildren"),
        MailboxAttribute::HasNoChildren
    );
    assert_eq!(
        parse_mailbox_attribute("\\Marked"),
        MailboxAttribute::Marked
    );
    assert_eq!(
        parse_mailbox_attribute("\\Unmarked"),
        MailboxAttribute::Unmarked
    );
    assert_eq!(
        parse_mailbox_attribute("\\Subscribed"),
        MailboxAttribute::Subscribed
    );
    assert_eq!(
        parse_mailbox_attribute("\\Remote"),
        MailboxAttribute::Remote
    );
}

#[test]
fn mailbox_attribute_special_use() {
    assert_eq!(parse_mailbox_attribute("\\All"), MailboxAttribute::All);
    assert_eq!(
        parse_mailbox_attribute("\\Archive"),
        MailboxAttribute::Archive
    );
    assert_eq!(
        parse_mailbox_attribute("\\Drafts"),
        MailboxAttribute::Drafts
    );
    assert_eq!(
        parse_mailbox_attribute("\\Flagged"),
        MailboxAttribute::Flagged
    );
    assert_eq!(parse_mailbox_attribute("\\Junk"), MailboxAttribute::Junk);
    assert_eq!(parse_mailbox_attribute("\\Sent"), MailboxAttribute::Sent);
    assert_eq!(parse_mailbox_attribute("\\Trash"), MailboxAttribute::Trash);
    assert_eq!(
        parse_mailbox_attribute("\\Important"),
        MailboxAttribute::Important
    );
}

#[test]
fn mailbox_attribute_case_insensitive() {
    assert_eq!(
        parse_mailbox_attribute("\\NOINFERIORS"),
        MailboxAttribute::NoInferiors
    );
    assert_eq!(
        parse_mailbox_attribute("\\NOSELECT"),
        MailboxAttribute::NoSelect
    );
    assert_eq!(
        parse_mailbox_attribute("\\haschildren"),
        MailboxAttribute::HasChildren
    );
    assert_eq!(parse_mailbox_attribute("\\SENT"), MailboxAttribute::Sent);
}

#[test]
fn mailbox_attribute_custom() {
    let attr = parse_mailbox_attribute("\\MyCustom");
    assert_eq!(attr, MailboxAttribute::Custom("\\MyCustom".to_owned()));
}

#[test]
fn untagged_list_with_all_attributes() {
    let input = b"LIST (\\HasChildren \\Subscribed) \".\" \"INBOX\"\r\n";
    let (_, resp) = parse_untagged_list(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.name.as_str(), "INBOX");
        assert_eq!(info.delimiter, Some('.'));
        assert!(info.attributes.contains(&MailboxAttribute::HasChildren));
        assert!(info.attributes.contains(&MailboxAttribute::Subscribed));
    } else {
        panic!("expected List, got {resp:?}");
    }
}

// ===== LSUB parser tests (RFC 3501 Section 7.2.3) =====

#[test]
fn lsub_basic() {
    let input = b"* LSUB () \"/\" \"INBOX\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(info.delimiter, Some('/'));
            assert!(info.attributes.is_empty());
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

#[test]
fn lsub_with_attributes() {
    let input = b"* LSUB (\\NoSelect) \".\" \"Archive\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "Archive");
            assert_eq!(info.delimiter, Some('.'));
            assert!(info.attributes.contains(&MailboxAttribute::NoSelect));
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

/// RFC 3501 Section 7.2.3: "The data is identical in format to the
/// unstripped LIST response." LSUB must parse LIST-EXTENDED data
/// (OLDNAME, CHILDINFO) the same way LIST does (RFC 5258 Section 6).
#[test]
fn lsub_preserves_extended_data() {
    // LSUB with OLDNAME extended data  -  same format as LIST.
    let input = b"* LSUB () \"/\" \"NewName\" (\"OLDNAME\" (\"OldName\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "NewName");
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("OldName"),
                "LSUB must parse OLDNAME extended data like LIST \
                     (RFC 3501 Section 7.2.3, RFC 5258 Section 6)"
            );
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

/// RFC 5258 Section 4: CHILDINFO in LSUB responses must be captured.
#[test]
fn lsub_preserves_childinfo() {
    let input = b"* LSUB () \".\" \"Parent\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "Parent");
            assert!(
                info.child_info.contains(&"SUBSCRIBED".to_string()),
                "LSUB must parse CHILDINFO extended data like LIST \
                     (RFC 3501 Section 7.2.3, RFC 5258 Section 4)"
            );
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

#[test]
fn status_item_mailboxid() {
    let (_, items) = status_items(b"MAILBOXID (F2212ea87-6097-4256)").unwrap();
    assert_eq!(
        items,
        vec![StatusItem::MailboxId("F2212ea87-6097-4256".into())]
    );
}

/// STATUS MAILBOXID response with a valid objectid per RFC 8474 Section 7.
#[test]
fn status_mailboxid_rfc8474_valid_objectid() {
    let (_, items) = status_items(b"MAILBOXID (F_box-42)").unwrap();
    assert_eq!(items, vec![StatusItem::MailboxId("F_box-42".into())]);
}

#[test]
fn parse_quota_empty_resource_list() {
    let input = b"* QUOTA \"\" ()\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "");
        assert!(resources.is_empty());
        return;
    }
    panic!("expected Quota response with empty resources");
}

#[test]
fn parse_myrights_quoted_mailbox() {
    let input = b"* MYRIGHTS \"Sent Items\" lrs\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::MyRights { mailbox, rights } = *boxed
    {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert_eq!(rights, "lrs");
        return;
    }
    panic!("expected MYRIGHTS response with quoted mailbox");
}

#[test]
fn parse_listrights_with_optional() {
    let input = b"* LISTRIGHTS INBOX fred lr s w i p c d a\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::ListRights {
            mailbox,
            identifier,
            required,
            optional,
        } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(identifier, "fred");
        assert_eq!(required, "lr");
        assert_eq!(optional, vec!["s", "w", "i", "p", "c", "d", "a"]);
        return;
    }
    panic!("expected LISTRIGHTS response");
}

#[test]
fn parse_listrights_no_optional() {
    let input = b"* LISTRIGHTS INBOX fred lrswipcda\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::ListRights {
            mailbox,
            identifier,
            required,
            optional,
        } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(identifier, "fred");
        assert_eq!(required, "lrswipcda");
        assert!(optional.is_empty());
        return;
    }
    panic!("expected LISTRIGHTS response with no optional");
}

/// QUOTAROOT with multiple roots (RFC 2087 Section 5.2).
#[test]
fn quotaroot_multiple_roots_list() {
    let input = b"QUOTAROOT INBOX \"root1\" \"root2\" \"root3\"\r\n";
    let (_, resp) =
        parse_untagged_quotaroot(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::QuotaRoot { mailbox, roots } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec!["root1", "root2", "root3"]);
    } else {
        panic!("expected QuotaRoot, got {resp:?}");
    }
}

// ===== Task 2: ACL/LISTRIGHTS/MYRIGHTS parser tests (RFC 4314 Section 3.6-3.8) =====

/// ACL with a single entry and quoted mailbox (RFC 4314 Section 3.6).
#[test]
fn acl_single_entry_quoted_mailbox() {
    let input = b"ACL \"Sent Items\" alice lrswipcda\r\n";
    let (_, resp) = parse_untagged_acl(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Acl { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].identifier, "alice");
        assert_eq!(entries[0].rights, "lrswipcda");
    } else {
        panic!("expected Acl, got {resp:?}");
    }
}

/// LISTRIGHTS with required and multiple optional rights groups (RFC 4314 Section 3.7).
#[test]
fn listrights_required_and_optional() {
    let input = b"LISTRIGHTS \"Sent Items\" bob lr s w i p c d a\r\n";
    let (_, resp) =
        parse_untagged_listrights(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::ListRights {
        mailbox,
        identifier,
        required,
        optional,
    } = resp
    {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert_eq!(identifier, "bob");
        assert_eq!(required, "lr");
        assert_eq!(optional.len(), 7);
        assert_eq!(optional, vec!["s", "w", "i", "p", "c", "d", "a"]);
    } else {
        panic!("expected ListRights, got {resp:?}");
    }
}

/// LISTRIGHTS with required rights only (no optional) (RFC 4314 Section 3.7).
#[test]
fn listrights_required_only() {
    let input = b"LISTRIGHTS INBOX alice lrswipcda\r\n";
    let (_, resp) =
        parse_untagged_listrights(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::ListRights {
        required, optional, ..
    } = resp
    {
        assert_eq!(required, "lrswipcda");
        assert!(optional.is_empty());
    } else {
        panic!("expected ListRights, got {resp:?}");
    }
}

/// LISTRIGHTS with truncated input should fail (RFC 4314 Section 3.7).
#[test]
fn listrights_truncated_input() {
    // Missing required rights and CRLF
    assert!(
        parse_untagged_listrights(
            b"LISTRIGHTS INBOX fred",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
    // Missing CRLF
    assert!(
        parse_untagged_listrights(
            b"LISTRIGHTS INBOX fred lr",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// LISTRIGHTS with garbage data should fail (RFC 4314 Section 3.7).
#[test]
fn listrights_garbage_data() {
    assert!(
        parse_untagged_listrights(
            b"LISTRIGHTS !!!GARBAGE\r\n",
            crate::types::wire::WireMode::ModifiedUtf7
        )
        .is_err()
    );
}

/// MYRIGHTS with an atom mailbox (RFC 4314 Section 3.8).
#[test]
fn myrights_atom_mailbox() {
    let input = b"MYRIGHTS INBOX lr\r\n";
    let (_, resp) =
        parse_untagged_myrights(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MyRights { mailbox, rights } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(rights, "lr");
    } else {
        panic!("expected MyRights, got {resp:?}");
    }
}

/// METADATA with an empty list (RFC 5464 Section 4.4).
#[test]
fn metadata_empty_list_standalone() {
    let input = b"METADATA \"Archive\" ()\r\n";
    let (_, resp) =
        parse_untagged_metadata(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::Metadata { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "Archive");
        assert!(entries.is_empty());
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// RFC 3501 Section 9: body-fld-param formally requires at least one key-value pair,
/// but many real servers send `()` for empty parameter lists.
/// Accept the empty form per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn body_params_empty_list_accepted() {
    let (_, params) = body_params(b"()").unwrap();
    assert!(
        params.is_empty(),
        "empty () should produce empty params vec, got: {params:?}"
    );
}

/// `body_language` with a parenthesized list (RFC 3501 Section 7.4.2).
#[test]
fn body_language_list() {
    let (_, lang) = body_language(b"(\"en\" \"fr\" \"de\")").unwrap();
    let langs = lang.expect("should not be None");
    assert_eq!(langs, vec!["en", "fr", "de"]);
}

/// `body_language` with an empty list `()` should be accepted per Postel's law
/// (RFC 1122 Section 1.2.2). RFC 3501 Section 7.4.2 formal ABNF requires ≥1
/// string, but real servers send `()` for empty language lists.
#[test]
fn body_language_empty_list() {
    let (_, lang) = body_language(b"()").unwrap();
    let langs = lang.expect("empty () should be Some(vec![]), not None");
    assert!(
        langs.is_empty(),
        "empty () should produce empty language vec"
    );
}

// ===== LIST delimiter must handle multi-byte UTF-8 =====
// RFC 3501 Section 7.2.2: hierarchy delimiter is a single character.
// When quoted, multi-byte UTF-8 chars must be decoded, not byte-truncated.

#[test]
fn list_delimiter_multibyte_utf8() {
    // "÷" is U+00F7, encoded as [0xC3, 0xB7] in UTF-8
    let input = b"LIST (\\HasNoChildren) \"\xC3\xB7\" INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(
            info.delimiter,
            Some('\u{00F7}'),
            "multi-byte UTF-8 delimiter should be decoded as full char, not first byte"
        );
    } else {
        panic!("expected List response");
    }
}

// ===== LIST delimiter must be exactly one QUOTED-CHAR (RFC 3501 Section9) =====
// RFC 3501 Section 9: mailbox-list = ... SP (DQUOTE QUOTED-CHAR DQUOTE / nil) SP mailbox
// Multi-character quoted strings in the delimiter position are a protocol error.

/// Single-char delimiter "/" must parse correctly (RFC 3501 Section 7.2.2).
#[test]
fn list_delimiter_single_char_valid() {
    let input = b"LIST () \"/\" INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.delimiter, Some('/'));
    } else {
        panic!("expected List response");
    }
}

/// NIL delimiter must parse correctly (RFC 3501 Section 7.2.2).
#[test]
fn list_delimiter_nil_valid() {
    let input = b"LIST () NIL INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.delimiter, None);
    } else {
        panic!("expected List response");
    }
}

/// Postel's law (RFC 3501 Section 9): a non-conformant server sending a
/// multi-character delimiter (e.g., "//") should not cause a hard parse
/// failure. The parser takes the first character and tolerates the rest.
#[test]
fn list_delimiter_multi_char_tolerant() {
    let input = b"LIST () \"//\" INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, crate::types::wire::WireMode::ModifiedUtf7).expect(
        "multi-character delimiter must be tolerated per Postel's law \
             (RFC 3501 Section 9): take the first char, not a hard error",
    );
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(
            info.delimiter,
            Some('/'),
            "multi-char delimiter should take the first character"
        );
    } else {
        panic!("expected List response");
    }
}

// ===== LIST-EXTENDED / OLDNAME trailing data (RFC 5258 / RFC 9051) =====
// RFC 5258 Section 6: mailbox-list = ... mailbox [SP mbox-list-extended]
// RFC 9051 Section 6.3.9.7 example: * LIST () "/" "NewMailbox" ("OLDNAME" ("OldMailbox"))

/// LIST with OLDNAME extended data must parse without error (RFC 9051 Section 6.3.9.7).
#[test]
fn list_with_oldname_extended_data() {
    let input = b"LIST () \"/\" \"NewMailbox\" (\"OLDNAME\" (\"OldMailbox\"))\r\n";
    let result = parse_untagged_list(input, crate::types::wire::WireMode::ModifiedUtf7);
    assert!(
        result.is_ok(),
        "LIST with OLDNAME extended data should parse successfully \
             per RFC 5258 / RFC 9051 Section 6.3.9.7, got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.name.as_str(), "NewMailbox");
        assert_eq!(info.delimiter, Some('/'));
    } else {
        panic!("expected List response, got {resp:?}");
    }
}

/// LIST with CHILDINFO extended data must parse without error (RFC 5258 Section 4).
#[test]
fn list_with_childinfo_extended_data() {
    let input = b"LIST (\\HasChildren) \"/\" \"INBOX\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let result = parse_untagged_list(input, crate::types::wire::WireMode::ModifiedUtf7);
    assert!(
        result.is_ok(),
        "LIST with CHILDINFO extended data should parse successfully \
             per RFC 5258, got: {result:?}"
    );
}

// M12: SpecialUse::All
#[test]
fn special_use_all_attribute() {
    use crate::types::mailbox::{MailboxAttribute, MailboxInfo, SpecialUse};
    let info = MailboxInfo {
        name: MailboxName::new("All Mail").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::All],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::All)
    );
}

// ===== Audit finding #4: LIST-EXTENDED data parsed and discarded =====

/// RFC 9051 Section 7.2.2 / RFC 5258 Section 6: LIST may carry
/// `mbox-list-extended` items such as OLDNAME. The parser must not
/// fail on these. Extended data items (OLDNAME, CHILDINFO) are captured.
#[test]
fn audit_finding4_list_extended_oldname_parses_without_error() {
    // LIST response with OLDNAME extended data (RFC 9051 Section 6.3.9.7).
    let input = b"* LIST (\\HasNoChildren) \"/\" \"NewName\" (\"OLDNAME\" (\"OldName\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "NewName");
            assert_eq!(info.delimiter, Some('/'));
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("OldName"),
                "OLDNAME should be captured in MailboxInfo"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with CHILDINFO extended data (RFC 5258 Section 4).
#[test]
fn audit_finding4_list_extended_childinfo_parsed() {
    let input = b"* LIST (\\HasChildren) \"/\" \"INBOX\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(
                info.child_info,
                vec!["SUBSCRIBED"],
                "CHILDINFO should be captured in MailboxInfo"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with multiple extended data items.
#[test]
fn audit_finding4_list_extended_multiple_items_parsed() {
    let input = b"* LIST (\\HasChildren) \"/\" \"folder\" (\"OLDNAME\" (\"old\") \"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "folder");
            assert_eq!(info.old_name.as_ref().map(MailboxName::as_str), Some("old"));
            assert_eq!(info.child_info, vec!["SUBSCRIBED"]);
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST without extended data still parses normally.
#[test]
fn audit_finding4_list_without_extended_data_still_works() {
    let input = b"* LIST (\\Noselect) \"/\" \"Archive\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "Archive");
            assert_eq!(info.delimiter, Some('/'));
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with an unknown extended item whose value is a simple number
/// (not parenthesized). Per RFC 5258 Section 6 and RFC 9051 Section 9,
/// `tagged-ext-val` can be `tagged-ext-simple` (a number64 or sequence-set),
/// which is NOT wrapped in parentheses.
#[test]
fn test_list_extended_unknown_simple_value() {
    let input = b"* LIST (\\HasNoChildren) \"/\" \"INBOX\" (\"x-future-ext\" 42)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(info.delimiter, Some('/'));
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with an unknown extended item whose value is a sequence-set
/// (not parenthesized). Per RFC 9051 Section 9,
/// `tagged-ext-simple = sequence-set / number64`.
#[test]
fn test_list_extended_unknown_sequence_set_value() {
    let input = b"* LIST (\\HasNoChildren) \"/\" \"INBOX\" (\"x-seq-ext\" 1:5,8:*)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with a mix of known and unknown extended items, where the
/// unknown has a simple value and is followed by a known item.
#[test]
fn test_list_extended_unknown_simple_then_known() {
    let input =
        b"* LIST (\\HasChildren) \"/\" \"folder\" (\"x-count\" 99 \"OLDNAME\" (\"old\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "folder");
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("old"),
                "OLDNAME after unknown simple item should still be parsed"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// H2: Unsolicited METADATA notifications use `entry-list` without parens.
/// RFC 5464 Section 4.4.2.
#[test]
fn audit_h2_metadata_unsolicited_entry_list_no_parens() {
    let input = b"* METADATA \"INBOX\" /shared/comment /private/comment\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Metadata { mailbox, entries } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].name, "/shared/comment");
            assert_eq!(entries[1].name, "/private/comment");
            assert!(entries[0].value.is_none());
            assert!(entries[1].value.is_none());
        } else {
            panic!("expected Metadata, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ── 6. Body structure skip (L1290-1291) ──

/// RFC 5258 Section 6: unknown LIST-EXTENDED items with parenthesized
/// values must be silently skipped via `skip_parenthesized_block`.
#[test]
fn list_extended_unknown_item_parenthesized_value_skipped() {
    // LIST response with an unknown extended data item that has a
    // parenthesized value  -  the parser should skip it gracefully.
    let input = b"* LIST (\\HasNoChildren) \"/\" \"INBOX\" (\"XFUTURE\" (\"some\" \"data\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        assert_eq!(info.delimiter, Some('/'));
        assert!(info.attributes.contains(&MailboxAttribute::HasNoChildren));
        // The unknown extended item should be silently skipped
        assert!(info.old_name.is_none());
        assert!(info.child_info.is_empty());
        return;
    }
    panic!("expected List response");
}

/// RFC 5258 Section 6: unknown LIST-EXTENDED items with simple (non-parenthesized)
/// values are also skipped.
#[test]
fn list_extended_unknown_item_simple_value_skipped() {
    // Unknown item with a simple token value (not parenthesized)
    let input = b"* LIST () \"/\" \"INBOX\" (\"XTOKEN\" 12345)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        return;
    }
    panic!("expected List response");
}

/// RFC 5258 Section 6: multiple unknown LIST-EXTENDED items, mixing
/// parenthesized and simple values.
#[test]
fn list_extended_multiple_unknown_items_skipped() {
    let input = b"* LIST () \"/\" \"INBOX\" (\"XFOO\" (\"nested\") \"XBAR\" 42)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        return;
    }
    panic!("expected List response");
}

/// Postel's law: LISTRIGHTS response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_listrights() {
    let input = b"LISTRIGHTS INBOX fred lr s w  \r\n";
    let (_, resp) =
        parse_untagged_listrights(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    assert!(matches!(resp, UntaggedResponse::ListRights { .. }));
}

/// RFC 3501 Section 7.2.2 / RFC 3348: LIST response with contradictory
/// mailbox attributes `\HasChildren` and `\HasNoChildren`.
/// While semantically contradictory, both attributes must be preserved
/// verbatim per Postel's law (RFC 1122 Section 1.2.2). The parser must
/// not discard or de-duplicate attributes.
#[test]
fn edge_list_contradictory_attributes_preserved() {
    let input = b"* LIST (\\HasChildren \\HasNoChildren) \"/\" \"INBOX\"\r\n";
    let (_, resp) = parse_response(input)
        .expect("LIST with contradictory attributes must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        assert_eq!(info.delimiter, Some('/'));
        assert_eq!(
            info.attributes.len(),
            2,
            "both contradictory attributes must be preserved; got {:?}",
            info.attributes
        );
        assert_eq!(
            info.attributes[0],
            MailboxAttribute::HasChildren,
            "first attribute must be \\HasChildren"
        );
        assert_eq!(
            info.attributes[1],
            MailboxAttribute::HasNoChildren,
            "second attribute must be \\HasNoChildren"
        );
        return;
    }
    panic!("expected Untagged List response");
}

/// RFC 4731 Section 3.1: ESEARCH with TAG and UID indicator but no
/// result data  -  `* ESEARCH (TAG "A001") UID\r\n`.
/// This represents an empty search result. All result fields (MIN, MAX,
/// COUNT, ALL) must be None/empty.
#[test]
fn edge_esearch_empty_uid_list() {
    let input = b"* ESEARCH (TAG \"A001\") UID\r\n";
    let (_, resp) = parse_response(input)
        .expect("ESEARCH with empty UID result must parse (RFC 4731 Section 3.1)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert_eq!(
            esearch.tag.as_deref(),
            Some("A001"),
            "TAG must be preserved"
        );
        assert!(esearch.is_uid(), "UID indicator must be true");
        assert!(
            esearch.all().is_empty(),
            "ALL must be empty for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.min(),
            None,
            "MIN must be None for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.max(),
            None,
            "MAX must be None for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.count, None,
            "COUNT must be None for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.mod_seq.map(crate::types::ModSeq::get),
            None,
            "MODSEQ must be None for an empty ESEARCH result"
        );
        return;
    }
    panic!("expected Untagged Esearch response");
}

/// LIST attribute parser must tolerate multiple spaces between attributes
/// per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn list_attributes_tolerate_multiple_spaces() {
    let input = b"* LIST (\\NoSelect  \\HasChildren) \"/\" \"Archive\"\r\n";
    let (_, resp) = parse_response(input)
        .expect("LIST with double-spaced attributes must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "Archive");
        assert_eq!(
            info.attributes.len(),
            2,
            "both attributes must be parsed despite double spaces; got {:?}",
            info.attributes
        );
        assert_eq!(info.attributes[0], MailboxAttribute::NoSelect);
        assert_eq!(info.attributes[1], MailboxAttribute::HasChildren);
        return;
    }
    panic!("expected Untagged List response");
}

/// LIST CHILDINFO extended data with double spaces between items.
///
/// Regression test: `separated_list0(sp, ...)` with a single-space
/// separator would fail on extra whitespace inside the CHILDINFO
/// parenthesized list.
///
/// RFC 5258 Section 6 / Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn list_childinfo_double_space_between_items() {
    // Two spaces between "SUBSCRIBED" and "REMOTE" in CHILDINFO
    let input =
        b"* LIST (\\HasChildren) \".\" \"Parent\" (\"CHILDINFO\" (\"SUBSCRIBED\"  \"REMOTE\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(body) = resp {
        if let UntaggedResponse::List(mailbox) = body.as_ref() {
            assert_eq!(
                mailbox.child_info,
                vec!["SUBSCRIBED", "REMOTE"],
                "double space between CHILDINFO items must be tolerated"
            );
        } else {
            panic!("expected List, got {body:?}");
        }
    } else {
        panic!("expected Untagged response, got {resp:?}");
    }
}

/// `\NoAccess` mailbox attribute parsed from LIST response
/// (RFC 5465 Section 5.9, Section 8).
#[test]
fn list_noaccess_attribute_parsed() {
    let input = b"* LIST (\\NoAccess) \"/\" \"SharedFolder\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert!(
                info.attributes
                    .contains(&crate::types::MailboxAttribute::NoAccess),
                "should parse \\NoAccess attribute, got: {:?}",
                info.attributes
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// `\NoAccess` attribute is case-insensitive.
#[test]
fn list_noaccess_case_insensitive() {
    let input = b"* LIST (\\NOACCESS) \"/\" \"folder\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert!(
                info.attributes
                    .contains(&crate::types::MailboxAttribute::NoAccess),
                "\\NOACCESS (all caps) should match NoAccess, got: {:?}",
                info.attributes
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// LIST with `OLDNAME` extended data used for `MailboxName` rename events
/// (RFC 5465 Section 5.5, RFC 9051 Section 6.3.9.7).
#[test]
fn list_with_oldname_for_notify_rename() {
    let input = b"* LIST () \"/\" \"NewMailbox\" (\"OLDNAME\" (\"OldMailbox\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert_eq!(info.name.as_str(), "NewMailbox");
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("OldMailbox"),
                "OLDNAME must be captured for NOTIFY rename events"
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// A wire-decoded mailbox name is the server's own identifier, so it is kept
/// byte-for-byte even when it contains octets `MailboxName::new` rejects
/// (RFC 3501 Section 5.1: MUTF-7 can represent any code point). Rewriting it
/// would make the client address a mailbox the server never advertised; CRLF
/// safety is enforced when the name is re-encoded, not here.
#[test]
fn list_mailbox_name_preserves_control_characters_from_the_wire() {
    let input = b"* LIST () \"/\" \"&AAoALQ-\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert_eq!(
                info.name.as_str(),
                "\n-",
                "the server's name is preserved verbatim, LF and all"
            );
            assert!(
                MailboxName::new(info.name.as_str()).is_err(),
                "the validating constructor is stricter than the wire path on \
                 purpose; consumers must not assume its invariant for parsed names"
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// A malformed form of a known response is a parse failure, not an extension
/// response. The driver closes the connection and the account boundary maps
/// it to `Protocol(ParseFailed)`.
#[test]
fn malformed_status_is_a_parse_failure() {
    let input = b"* STATUS \"INBOX\" (MESSAGES)\r\n";
    assert!(parse_response(input).is_err());
}

/// RFC 3501 Section 7.2.4 / Postel's law (RFC 1122 Section 1.2.2):
/// STATUS response with an unknown status item (UNKNOWN) must skip the
/// unrecognized item gracefully and still parse recognized items.
#[test]
fn edge_status_unknown_items_skipped() {
    let input = b"* STATUS INBOX (MESSAGES 5 UNKNOWN 3)\r\n";
    let (_, resp) =
        parse_response(input).expect("STATUS with unknown items must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::MailboxStatus { mailbox, items } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        // Only MESSAGES should be recognized; UNKNOWN should be skipped.
        assert_eq!(
            items.len(),
            1,
            "only recognized status items should be present; got {items:?}"
        );
        assert_eq!(
            items[0],
            StatusItem::Messages(5),
            "MESSAGES value must be preserved"
        );
        return;
    }
    panic!("expected Untagged MailboxStatus response");
}

/// A STATUS attribute value that overflows its numeric type should not
/// discard the entire response. The overflowing attribute is silently
/// skipped and all other attributes are preserved
/// (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn status_messages_overflow_preserves_other_items() {
    // MESSAGES value exceeds u32::MAX (4294967296 = 2^32).
    let input = b"* STATUS INBOX (UIDVALIDITY 1 MESSAGES 4294967296)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            // UIDVALIDITY 1 must be preserved.
            assert!(
                items
                    .iter()
                    .any(|i| matches!(i, StatusItem::UidValidity(1))),
                "UIDVALIDITY 1 missing: {items:?}"
            );
            // MESSAGES was unparseable  -  must NOT appear.
            assert!(
                !items.iter().any(|i| matches!(i, StatusItem::Messages(_))),
                "overflowed MESSAGES should have been skipped: {items:?}"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged Status response, not Unknown");
    }
}

/// RFC 9208 Section 3: DELETED-STORAGE with large u64 value.
#[test]
fn status_deleted_storage_large_value() {
    let (_, items) = status_items(b"DELETED-STORAGE 5000000000").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::DeletedStorage(5_000_000_000u64));
}

/// RFC 9208 Section 3: DELETED-STORAGE alongside other STATUS items.
#[test]
fn status_deleted_storage_with_other_items() {
    let input = b"STATUS \"INBOX\" (MESSAGES 10 DELETED-STORAGE 512)\r\n";
    let (_, resp) =
        parse_untagged_status_mailbox(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(items.len(), 2);
        assert!(items.contains(&StatusItem::Messages(10)));
        assert!(items.contains(&StatusItem::DeletedStorage(512)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

// ── 7. Status DELETEDSTORAGE (L1742-1745) ──

/// RFC 9208 Section 3: STATUS response containing DELETED-STORAGE item
/// reports disk space consumed by deleted messages.
#[test]
fn status_deleted_storage() {
    let (_, items) = status_items(b"DELETED-STORAGE 2048").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::DeletedStorage(2048));
}

/// RFC 5258 Section 6: CHILDINFO formally requires ≥1 item, but empty `()`
/// is accepted per Postel's law.  An empty CHILDINFO is treated as absent.
#[test]
fn spec_audit_childinfo_empty_accepted() {
    let input = b"* LIST (\\HasChildren) \"/\" \"INBOX\" (\"CHILDINFO\" ())\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "parse must consume the full response");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert!(
                info.child_info.is_empty(),
                "empty CHILDINFO () should produce empty vec"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn spec_audit_status_unknown_nested_parenthesized_value() {
    // Nested parentheses in the unknown value
    let input = b"* STATUS \"INBOX\" (MESSAGES 5 XCOMPLEX (a (b c) d) UIDNEXT 100)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::MailboxStatus { ref items, .. } => {
                let messages = items.iter().find_map(|i| match i {
                    StatusItem::Messages(n) => Some(*n),
                    _ => None,
                });
                let uidnext = items.iter().find_map(|i| match i {
                    StatusItem::UidNext(n) => Some(*n),
                    _ => None,
                });
                assert_eq!(messages, Some(5));
                assert_eq!(
                    uidnext,
                    Some(100),
                    "UIDNEXT after nested parenthesized value should be parsed"
                );
            }
            other => panic!("expected MailboxStatus, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 9051 Section9: unknown STATUS attributes may have parenthesized values
/// (status-att-val-ext = tagged-ext-val, which includes "(" ... ")").
/// The parser must skip them without breaking subsequent items.
#[test]
fn spec_audit_status_unknown_parenthesized_value() {
    let input = b"* STATUS \"INBOX\" (MESSAGES 17 XFUTURE (some data) UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::MailboxStatus {
                ref mailbox,
                ref items,
            } => {
                assert_eq!(mailbox.as_str(), "INBOX");
                // Must see both MESSAGES and UNSEEN despite unknown XFUTURE in between
                let messages = items.iter().find_map(|i| match i {
                    StatusItem::Messages(n) => Some(*n),
                    _ => None,
                });
                let unseen = items.iter().find_map(|i| match i {
                    StatusItem::Unseen(n) => Some(*n),
                    _ => None,
                });
                assert_eq!(messages, Some(17), "MESSAGES should be parsed");
                assert_eq!(
                    unseen,
                    Some(3),
                    "UNSEEN after unknown parenthesized value should be parsed"
                );
            }
            other => panic!("expected MailboxStatus, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// L7: Unknown STATUS attributes (e.g. XFOO) cause a parse error instead of
/// being gracefully skipped.
#[test]
fn spec_audit_l7_status_unknown_attribute_skipped() {
    let input = b"* STATUS \"INBOX\" (MESSAGES 10 XFOO 42 UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            let messages = items.iter().find(|i| matches!(i, StatusItem::Messages(_)));
            let unseen = items.iter().find(|i| matches!(i, StatusItem::Unseen(_)));
            assert_eq!(
                messages,
                Some(&StatusItem::Messages(10)),
                "MESSAGES should be parsed despite unknown XFOO"
            );
            assert_eq!(
                unseen,
                Some(&StatusItem::Unseen(3)),
                "UNSEEN should be parsed despite unknown XFOO"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn status_uidvalidity_zero_accepted() {
    let input = b"* STATUS INBOX (UIDVALIDITY 0)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::MailboxStatus { mailbox, items } = *boxed {
                assert_eq!(mailbox.as_str(), "INBOX");
                assert_eq!(items, vec![StatusItem::UidValidity(0)]);
            } else {
                panic!("expected MailboxStatus, got {boxed:?}");
            }
        }
        other => panic!("Expected Untagged(MailboxStatus), got {other:?}"),
    }
}

// L3: STATUS UIDNEXT/UIDVALIDITY formerly rejected zero (nz-number). Now
// accepts 0 per Postel's law (RFC 1122 Section 1.2.2)  -  see the
// status_uidnext_zero_accepted / status_uidvalidity_zero_accepted tests below.

// Regression: non-conformant servers send UIDNEXT 0 / UIDVALIDITY 0 for empty
// mailboxes. Per Postel's law (RFC 1122 Section 1.2.2), we accept 0 even though
// RFC 9051 Section 9 says nz-number.
#[test]
fn status_uidnext_zero_accepted() {
    let input = b"* STATUS INBOX (UIDNEXT 0)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::MailboxStatus { mailbox, items } = *boxed {
                assert_eq!(mailbox.as_str(), "INBOX");
                assert_eq!(items, vec![StatusItem::UidNext(0)]);
            } else {
                panic!("expected MailboxStatus, got {boxed:?}");
            }
        }
        other => panic!("Expected Untagged(MailboxStatus), got {other:?}"),
    }
}

// M3: DELETED STATUS item (RFC 9051)
#[test]
fn status_deleted_item() {
    let input = b"* STATUS INBOX (MESSAGES 10 DELETED 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::MailboxStatus { items, .. } = *boxed {
            assert!(items.iter().any(|i| matches!(i, StatusItem::Deleted(3))));
        } else {
            panic!("expected MailboxStatus");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn status_all_items() {
    let input = b"STATUS \"INBOX\" (MESSAGES 10 RECENT 3 UNSEEN 2 UIDNEXT 100 UIDVALIDITY 1)\r\n";
    let (_, resp) =
        parse_untagged_status_mailbox(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MailboxStatus { items, .. } = resp {
        assert_eq!(items.len(), 5);
        assert!(items.contains(&StatusItem::Messages(10)));
        assert!(items.contains(&StatusItem::Recent(3)));
        assert!(items.contains(&StatusItem::Unseen(2)));
        assert!(items.contains(&StatusItem::UidNext(100)));
        assert!(items.contains(&StatusItem::UidValidity(1)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

#[test]
fn status_recent() {
    let input = b"STATUS \"INBOX\" (RECENT 5)\r\n";
    let (_, resp) =
        parse_untagged_status_mailbox(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MailboxStatus { items, .. } = resp {
        assert_eq!(items.len(), 1);
        assert!(items.contains(&StatusItem::Recent(5)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

#[test]
fn status_uidnext_uidvalidity() {
    let input = b"STATUS \"INBOX\" (UIDNEXT 4392 UIDVALIDITY 3857529045)\r\n";
    let (_, resp) =
        parse_untagged_status_mailbox(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MailboxStatus { items, .. } = resp {
        assert!(items.contains(&StatusItem::UidNext(4392)));
        assert!(items.contains(&StatusItem::UidValidity(3_857_529_045)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

// ===== STATUS item parser tests (RFC 3501 Section 7.2.4) =====

#[test]
fn status_messages_unseen() {
    let input = b"STATUS \"INBOX\" (MESSAGES 17 UNSEEN 2)\r\n";
    let (_, resp) =
        parse_untagged_status_mailbox(input, crate::types::wire::WireMode::ModifiedUtf7).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(items.len(), 2);
        assert!(items.contains(&StatusItem::Messages(17)));
        assert!(items.contains(&StatusItem::Unseen(2)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}
