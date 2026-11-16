#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::validated::MailboxName;

#[test]
fn special_use_from_attribute() {
    let info = MailboxInfo {
        name: MailboxName::new("MyArchive").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::Archive],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Archive)
    );
}

#[test]
fn special_use_name_fallback() {
    let info = MailboxInfo {
        name: MailboxName::new("Spam").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Junk)
    );
}

#[test]
fn special_use_case_insensitive() {
    let info = MailboxInfo {
        name: MailboxName::new("SENT").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Sent)
    );
}

#[test]
fn special_use_nested_name() {
    let info = MailboxInfo {
        name: MailboxName::new("INBOX/Trash").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Trash)
    );
}

#[test]
fn special_use_attribute_overrides_name() {
    let info = MailboxInfo {
        name: MailboxName::new("Trash").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::Sent],
        ..Default::default()
    };
    // Attribute wins over name.
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Sent)
    );
}

#[test]
fn special_use_dot_delimiter() {
    let info = MailboxInfo {
        name: MailboxName::new("INBOX.Trash").unwrap(),
        delimiter: Some('.'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Trash)
    );
}

#[test]
fn special_use_no_delimiter() {
    let info = MailboxInfo {
        name: MailboxName::new("Sent").unwrap(),
        delimiter: None,
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Sent)
    );
}

#[test]
fn special_use_unknown() {
    let info = MailboxInfo {
        name: MailboxName::new("Work").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        None
    );
}

#[test]
fn special_use_from_drafts_attribute() {
    // RFC 6154 Section 2: \Drafts attribute.
    let info = MailboxInfo {
        name: MailboxName::new("MyDrafts").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::Drafts],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Drafts)
    );
}

#[test]
fn special_use_from_junk_attribute() {
    // RFC 6154 Section 2: \Junk attribute.
    let info = MailboxInfo {
        name: MailboxName::new("SpamFolder").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::Junk],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Junk)
    );
}

#[test]
fn special_use_from_trash_attribute() {
    // RFC 6154 Section 2: \Trash attribute.
    let info = MailboxInfo {
        name: MailboxName::new("Bin").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::Trash],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Trash)
    );
}

#[test]
fn special_use_from_flagged_attribute() {
    // RFC 6154 Section 2: \Flagged attribute.
    let info = MailboxInfo {
        name: MailboxName::new("Stars").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::Flagged],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Flagged)
    );
}

#[test]
fn special_use_from_important_attribute() {
    // RFC 8457: \Important attribute.
    let info = MailboxInfo {
        name: MailboxName::new("Priority").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::Important],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Important)
    );
}

#[test]
fn special_use_name_fallback_infers_important() {
    let info = MailboxInfo {
        name: MailboxName::new("Important").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Important)
    );
}

#[test]
fn special_use_name_fallback_infers_all_mail() {
    let info = MailboxInfo {
        name: MailboxName::new("[Gmail]/All Mail").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::All)
    );
}

#[test]
fn special_use_name_fallback_infers_flagged_from_starred() {
    let info = MailboxInfo {
        name: MailboxName::new("Starred").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        Some(SpecialUse::Flagged)
    );
}

#[test]
fn special_use_name_fallback_returns_none_for_unknown_name() {
    // When no RFC 6154 attribute is present and the mailbox name does not
    // match any well-known name convention, special_use() returns None.
    let info = MailboxInfo {
        name: MailboxName::new("Projects/2024/reports").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::HasNoChildren],
        ..Default::default()
    };
    assert_eq!(
        SpecialUse::classify(&info.attributes, info.name.as_str(), info.delimiter),
        None
    );
}

#[test]
fn status_item_mailboxid() {
    let item = StatusItem::MailboxId("F2212ea87-6097-4256".into());
    assert_eq!(item, StatusItem::MailboxId("F2212ea87-6097-4256".into()));
}

// --- MailboxAttribute::as_imap_str() ---

#[test]
fn as_imap_str_base_attributes() {
    assert_eq!(MailboxAttribute::NoInferiors.as_imap_str(), "\\Noinferiors");
    assert_eq!(MailboxAttribute::NoSelect.as_imap_str(), "\\Noselect");
    assert_eq!(MailboxAttribute::NonExistent.as_imap_str(), "\\NonExistent");
    assert_eq!(MailboxAttribute::HasChildren.as_imap_str(), "\\HasChildren");
    assert_eq!(
        MailboxAttribute::HasNoChildren.as_imap_str(),
        "\\HasNoChildren"
    );
    assert_eq!(MailboxAttribute::Marked.as_imap_str(), "\\Marked");
    assert_eq!(MailboxAttribute::Unmarked.as_imap_str(), "\\Unmarked");
    assert_eq!(MailboxAttribute::Subscribed.as_imap_str(), "\\Subscribed");
    assert_eq!(MailboxAttribute::Remote.as_imap_str(), "\\Remote");
}

#[test]
fn as_imap_str_special_use_attributes() {
    assert_eq!(MailboxAttribute::All.as_imap_str(), "\\All");
    assert_eq!(MailboxAttribute::Archive.as_imap_str(), "\\Archive");
    assert_eq!(MailboxAttribute::Drafts.as_imap_str(), "\\Drafts");
    assert_eq!(MailboxAttribute::Flagged.as_imap_str(), "\\Flagged");
    assert_eq!(MailboxAttribute::Junk.as_imap_str(), "\\Junk");
    assert_eq!(MailboxAttribute::Sent.as_imap_str(), "\\Sent");
    assert_eq!(MailboxAttribute::Trash.as_imap_str(), "\\Trash");
    assert_eq!(MailboxAttribute::Important.as_imap_str(), "\\Important");
}

#[test]
fn as_imap_str_non_standard_attributes() {
    // Google-origin non-standard attributes (widely seen in the wild).
    assert_eq!(MailboxAttribute::Memos.as_imap_str(), "\\Memos");
    assert_eq!(MailboxAttribute::Scheduled.as_imap_str(), "\\Scheduled");
    assert_eq!(MailboxAttribute::Snoozed.as_imap_str(), "\\Snoozed");
}

#[test]
fn as_imap_str_custom() {
    let attr = MailboxAttribute::Custom("\\MyCustom".into());
    assert_eq!(attr.as_imap_str(), "\\MyCustom");
}

// ===== Spec audit tests for L13/L7 =====

// ===== Case-insensitive Custom attribute equality (RFC 3501 Section 7.2.2) =====

#[test]
fn custom_attribute_equality_is_case_insensitive() {
    // RFC 3501 Section 7.2.2: mailbox attributes use flag-extension = "\" atom.
    // IMAP atoms are case-insensitive, so custom attributes must compare
    // case-insensitively.
    assert_eq!(
        MailboxAttribute::Custom("\\MyCustom".into()),
        MailboxAttribute::Custom("\\mycustom".into()),
        "Custom mailbox attributes must compare case-insensitively per RFC 3501 Section 7.2.2"
    );
}

#[test]
fn custom_attribute_hash_is_case_insensitive() {
    // RFC 3501 Section 7.2.2: case-insensitive custom attributes must hash equally.
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(MailboxAttribute::Custom("\\MyCustom".into()));
    set.insert(MailboxAttribute::Custom("\\mycustom".into()));
    assert_eq!(
        set.len(),
        1,
        "Case-insensitively equal Custom attributes must have the same Hash per RFC 3501 Section 7.2.2"
    );
}

// ===== RFC 3501 Section 7.2.2 audit: cross-representation equality =====

#[test]
fn custom_noselect_equals_noselect_variant() {
    // RFC 3501 Section 7.2.2: mailbox attributes are case-insensitive atoms.
    // Custom("\\Noselect") represents the same attribute as NoSelect.
    assert_eq!(
        MailboxAttribute::Custom("\\Noselect".into()),
        MailboxAttribute::NoSelect,
        "Custom(\"\\\\Noselect\") must equal MailboxAttribute::NoSelect \
             per RFC 3501 Section 7.2.2"
    );
}

#[test]
fn custom_haschildren_equals_haschildren_variant() {
    // RFC 3348: \HasChildren attribute.
    assert_eq!(
        MailboxAttribute::Custom("\\HasChildren".into()),
        MailboxAttribute::HasChildren,
        "Custom(\"\\\\HasChildren\") must equal MailboxAttribute::HasChildren \
             per RFC 3501 Section 7.2.2"
    );
}

#[test]
fn custom_sent_equals_sent_variant() {
    // RFC 6154 Section 2: \Sent special-use attribute.
    assert_eq!(
        MailboxAttribute::Custom("\\Sent".into()),
        MailboxAttribute::Sent,
        "Custom(\"\\\\Sent\") must equal MailboxAttribute::Sent \
             per RFC 6154 Section 2"
    );
}

#[test]
fn custom_attribute_cross_representation_hash() {
    // RFC 3501 Section 7.2.2: equal attributes must hash identically.
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(MailboxAttribute::NoSelect);
    set.insert(MailboxAttribute::Custom("\\Noselect".into()));
    assert_eq!(
        set.len(),
        1,
        "Custom(\"\\\\Noselect\") and NoSelect must hash the same \
             per RFC 3501 Section 7.2.2"
    );
}

// ===== is_special_use() regression tests (RFC 6154 Section 2) =====
//
// Custom variants whose wire form matches a known base LIST attribute
// (RFC 3501 Section 7.2.2) must NOT be classified as special-use.

#[test]
fn is_special_use_custom_noselect_is_not_special_use() {
    // RFC 3501 Section 7.2.2: \Noselect is a base LIST attribute, not a
    // special-use attribute (RFC 6154 Section 2).
    assert!(
        !MailboxAttribute::Custom("\\Noselect".into()).is_special_use(),
        "Custom(\"\\\\Noselect\") must not be special-use  -  it is a base LIST attribute"
    );
}

#[test]
fn is_special_use_custom_haschildren_is_not_special_use() {
    // RFC 3348: \HasChildren is a base LIST attribute.
    assert!(
        !MailboxAttribute::Custom("\\HasChildren".into()).is_special_use(),
        "Custom(\"\\\\HasChildren\") must not be special-use  -  it is a base LIST attribute"
    );
}

#[test]
fn is_special_use_custom_marked_is_not_special_use() {
    // RFC 3501 Section 7.2.2: \Marked is a base LIST attribute.
    assert!(
        !MailboxAttribute::Custom("\\Marked".into()).is_special_use(),
        "Custom(\"\\\\Marked\") must not be special-use  -  it is a base LIST attribute"
    );
}

#[test]
fn is_special_use_custom_unknown_is_special_use() {
    // RFC 6154 Section 2: genuinely unknown attributes are potential
    // use-attr-ext and should be treated as special-use.
    assert!(
        MailboxAttribute::Custom("\\MyCustomFlag".into()).is_special_use(),
        "Custom(\"\\\\MyCustomFlag\") should be special-use (potential use-attr-ext)"
    );
}

#[test]
fn is_special_use_custom_missing_backslash_is_not_special_use() {
    // RFC 6154 Section 6: use-attr-ext = "\" atom. A custom special-use
    // attribute without the leading backslash is not valid use-attr-ext.
    assert!(
        !MailboxAttribute::Custom("MyCustomFlag".into()).is_special_use(),
        "Custom(\"MyCustomFlag\") must not be special-use because use-attr-ext requires a leading backslash"
    );
}

#[test]
fn is_special_use_custom_with_invalid_atom_char_is_not_special_use() {
    // RFC 6154 Section 6 imports IMAP atom syntax from RFC 3501
    // Section 9, so SP is not legal inside the atom that follows "\".
    assert!(
        !MailboxAttribute::Custom("\\Bad Attr".into()).is_special_use(),
        "Custom(\"\\\\Bad Attr\") must not be special-use because SP is not legal in IMAP atoms"
    );
}

#[test]
fn is_special_use_known_special_use_variants() {
    // RFC 6154 Section 2 defined special-use attributes.
    assert!(MailboxAttribute::All.is_special_use());
    assert!(MailboxAttribute::Archive.is_special_use());
    assert!(MailboxAttribute::Drafts.is_special_use());
    assert!(MailboxAttribute::Flagged.is_special_use());
    assert!(MailboxAttribute::Junk.is_special_use());
    assert!(MailboxAttribute::Sent.is_special_use());
    assert!(MailboxAttribute::Trash.is_special_use());
    assert!(MailboxAttribute::Important.is_special_use());
}

#[test]
fn is_special_use_base_list_variants_are_not_special_use() {
    // RFC 3501 Section 7.2.2: base LIST attributes are never special-use.
    assert!(!MailboxAttribute::NoInferiors.is_special_use());
    assert!(!MailboxAttribute::NoSelect.is_special_use());
    assert!(!MailboxAttribute::NonExistent.is_special_use());
    assert!(!MailboxAttribute::HasChildren.is_special_use());
    assert!(!MailboxAttribute::HasNoChildren.is_special_use());
    assert!(!MailboxAttribute::Marked.is_special_use());
    assert!(!MailboxAttribute::Unmarked.is_special_use());
    assert!(!MailboxAttribute::Subscribed.is_special_use());
    assert!(!MailboxAttribute::Remote.is_special_use());
}

#[test]
fn is_special_use_custom_base_attr_case_insensitive() {
    // RFC 3501 Section 7.2.2: IMAP atoms are case-insensitive, so a Custom
    // variant with any casing of a base LIST attribute wire form must not be
    // classified as special-use.
    assert!(
        !MailboxAttribute::Custom("\\NOSELECT".into()).is_special_use(),
        "Case-insensitive match: \\NOSELECT is \\Noselect"
    );
    assert!(
        !MailboxAttribute::Custom("\\haschildren".into()).is_special_use(),
        "Case-insensitive match: \\haschildren is \\HasChildren"
    );
    assert!(
        !MailboxAttribute::Custom("\\NOINFERIORS".into()).is_special_use(),
        "Case-insensitive match: \\NOINFERIORS is \\Noinferiors"
    );
}

#[test]
fn spec_audit_l13_appendlimit_status_attribute() {
    // RFC 7889 Section 3: APPENDLIMIT is a per-mailbox STATUS attribute.
    let input = b"* STATUS \"INBOX\" (MESSAGES 10 APPENDLIMIT 1048576)\r\n";
    let (_, resp) = crate::codec::decode::parse_response(input).expect("should parse APPENDLIMIT");
    match resp {
        crate::types::response::Response::Untagged(inner) => match *inner {
            crate::types::response::UntaggedResponse::MailboxStatus { ref items, .. } => {
                let has_appendlimit = items
                    .iter()
                    .any(|item| matches!(item, StatusItem::AppendLimit(Some(1_048_576))));
                assert!(
                    has_appendlimit,
                    "STATUS response should contain AppendLimit(Some(1048576)); got {items:?}"
                );
            }
            other => panic!("expected MailboxStatus, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== MailboxAttribute::from_imap_str / From<String> / From<&str> =====

#[test]
fn mailbox_attr_from_str_known_variants() {
    // RFC 3501 Section 7.2.2 / RFC 6154 Section 2: known attributes.
    assert_eq!(
        MailboxAttribute::from("\\Noselect"),
        MailboxAttribute::NoSelect
    );
    assert_eq!(
        MailboxAttribute::from("\\HasChildren"),
        MailboxAttribute::HasChildren
    );
    assert_eq!(MailboxAttribute::from("\\Sent"), MailboxAttribute::Sent);
    assert_eq!(MailboxAttribute::from("\\Drafts"), MailboxAttribute::Drafts);
    assert_eq!(MailboxAttribute::from("\\Trash"), MailboxAttribute::Trash);
    assert_eq!(MailboxAttribute::from("\\Junk"), MailboxAttribute::Junk);
    assert_eq!(
        MailboxAttribute::from("\\Archive"),
        MailboxAttribute::Archive
    );
    assert_eq!(
        MailboxAttribute::from("\\Important"),
        MailboxAttribute::Important
    );
    assert_eq!(
        MailboxAttribute::from("\\NonExistent"),
        MailboxAttribute::NonExistent
    );
}

#[test]
fn mailbox_attr_from_string_known_variant() {
    // RFC 3501 Section 7.2.2: via owned String.
    assert_eq!(
        MailboxAttribute::from("\\Sent".to_owned()),
        MailboxAttribute::Sent
    );
    assert_eq!(
        MailboxAttribute::from("\\Trash".to_owned()),
        MailboxAttribute::Trash
    );
}

#[test]
fn mailbox_attr_from_str_case_insensitive() {
    // RFC 3501 Section 7.2.2: mailbox attributes are case-insensitive.
    assert_eq!(
        MailboxAttribute::from("\\NOSELECT"),
        MailboxAttribute::NoSelect
    );
    assert_eq!(
        MailboxAttribute::from("\\noselect"),
        MailboxAttribute::NoSelect
    );
    assert_eq!(
        MailboxAttribute::from("\\HASCHILDREN"),
        MailboxAttribute::HasChildren
    );
    assert_eq!(MailboxAttribute::from("\\sent"), MailboxAttribute::Sent);
}

#[test]
fn mailbox_attr_from_str_custom() {
    // Unrecognized attributes preserved verbatim.
    assert_eq!(
        MailboxAttribute::from("\\MyCustom"),
        MailboxAttribute::Custom("\\MyCustom".to_owned())
    );
}

#[test]
fn mailbox_attr_from_str_non_standard_google() {
    // Non-standard Google-origin attributes.
    assert_eq!(MailboxAttribute::from("\\Memos"), MailboxAttribute::Memos);
    assert_eq!(
        MailboxAttribute::from("\\Scheduled"),
        MailboxAttribute::Scheduled
    );
    assert_eq!(
        MailboxAttribute::from("\\Snoozed"),
        MailboxAttribute::Snoozed
    );
}
