#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

// --- SequenceSet ---

#[test]
fn sequence_set_valid_single_number() {
    assert!(SequenceSet::new("1").is_ok());
    assert!(SequenceSet::new("42").is_ok());
    assert!(SequenceSet::new("4294967295").is_ok()); // u32::MAX
}

#[test]
fn sequence_set_valid_range() {
    assert!(SequenceSet::new("1:100").is_ok());
    assert!(SequenceSet::new("1:*").is_ok());
    assert!(SequenceSet::new("*:1").is_ok());
}

#[test]
fn sequence_set_valid_comma_separated() {
    assert!(SequenceSet::new("1,3:5,*").is_ok());
    assert!(SequenceSet::new("1,2,3").is_ok());
    assert!(SequenceSet::new("1:5,10:20").is_ok());
}

#[test]
fn sequence_set_valid_dollar_sign() {
    // RFC 5182 Section 5: "$" references saved search results.
    assert!(SequenceSet::new("$").is_ok());
    assert!(SequenceSet::new("1,$").is_ok());
}

#[test]
fn sequence_set_valid_star() {
    assert!(SequenceSet::new("*").is_ok());
    assert!(SequenceSet::new("1:*").is_ok());
}

#[test]
fn sequence_set_rejects_empty() {
    assert!(SequenceSet::new("").is_err());
}

#[test]
fn sequence_set_rejects_zero() {
    assert!(SequenceSet::new("0").is_err());
}

#[test]
fn sequence_set_rejects_leading_zero() {
    assert!(SequenceSet::new("01").is_err());
}

#[test]
fn sequence_set_rejects_overflow() {
    assert!(SequenceSet::new("4294967296").is_err()); // u32::MAX + 1
}

#[test]
fn sequence_set_rejects_non_digit() {
    assert!(SequenceSet::new("abc").is_err());
    assert!(SequenceSet::new("1 2").is_err());
}

#[test]
fn sequence_set_rejects_trailing_comma() {
    assert!(SequenceSet::new("1,").is_err());
}

#[test]
fn sequence_set_rejects_double_colon() {
    assert!(SequenceSet::new("1::2").is_err());
}

#[test]
fn sequence_set_rejects_triple_colon() {
    assert!(SequenceSet::new("1:2:3").is_err());
}

// --- SequenceSet::new_known ---

#[test]
fn known_sequence_set_valid() {
    assert!(SequenceSet::new_known("1").is_ok());
    assert!(SequenceSet::new_known("1:100").is_ok());
    assert!(SequenceSet::new_known("1,2,3").is_ok());
    assert!(SequenceSet::new_known("1:5,10:20").is_ok());
    assert!(SequenceSet::new_known("4294967295").is_ok()); // u32::MAX
}

#[test]
fn known_sequence_set_rejects_star() {
    assert!(SequenceSet::new_known("*").is_err());
    assert!(SequenceSet::new_known("1:*").is_err());
}

#[test]
fn known_sequence_set_rejects_dollar() {
    assert!(SequenceSet::new_known("$").is_err());
    assert!(SequenceSet::new_known("1,$").is_err());
}

#[test]
fn known_sequence_set_rejects_empty() {
    assert!(SequenceSet::new_known("").is_err());
}

#[test]
fn known_sequence_set_rejects_zero() {
    assert!(SequenceSet::new_known("0").is_err());
}

#[test]
fn known_sequence_set_rejects_leading_zero() {
    assert!(SequenceSet::new_known("01").is_err());
}

#[test]
fn known_sequence_set_rejects_overflow() {
    assert!(SequenceSet::new_known("4294967296").is_err());
}

#[test]
fn known_sequence_set_rejects_trailing_comma() {
    assert!(SequenceSet::new_known("1,").is_err());
}

#[test]
fn known_sequence_set_rejects_double_colon() {
    assert!(SequenceSet::new_known("1::2").is_err());
}

// --- SequenceSet Display / AsRef / TryFrom ---

#[test]
fn sequence_set_display() {
    let ss = SequenceSet::new("1:*").unwrap();
    assert_eq!(ss.to_string(), "1:*");
}

#[test]
fn sequence_set_as_ref() {
    let ss = SequenceSet::new("1,2,3").unwrap();
    let r: &str = ss.as_ref();
    assert_eq!(r, "1,2,3");
}

#[test]
fn sequence_set_as_str() {
    let ss = SequenceSet::new("1:100").unwrap();
    assert_eq!(ss.as_str(), "1:100");
}

#[test]
fn sequence_set_try_from_string() {
    let ss: Result<SequenceSet, _> = "1:*".to_string().try_into();
    assert!(ss.is_ok());
}

#[test]
fn sequence_set_try_from_str() {
    let ss: Result<SequenceSet, _> = SequenceSet::try_from("1:*");
    assert!(ss.is_ok());
}

#[test]
fn sequence_set_try_from_invalid() {
    let ss: Result<SequenceSet, _> = String::new().try_into();
    assert!(ss.is_err());
}

// --- ImapAtom ---

#[test]
fn atom_valid() {
    assert!(ImapAtom::new("PLAIN").is_ok());
    assert!(ImapAtom::new("a").is_ok());
    assert!(ImapAtom::new("AUTH=PLAIN").is_ok());
}

#[test]
fn atom_rejects_empty() {
    assert!(ImapAtom::new("").is_err());
}

#[test]
fn atom_rejects_specials() {
    assert!(ImapAtom::new("(").is_err());
    assert!(ImapAtom::new(")").is_err());
    assert!(ImapAtom::new("{").is_err());
    assert!(ImapAtom::new(" ").is_err());
    assert!(ImapAtom::new("%").is_err());
    assert!(ImapAtom::new("*").is_err());
    assert!(ImapAtom::new("\"").is_err());
    assert!(ImapAtom::new("\\").is_err());
    assert!(ImapAtom::new("]").is_err());
}

#[test]
fn atom_rejects_ctl() {
    assert!(ImapAtom::new("\x00").is_err());
    assert!(ImapAtom::new("\x1F").is_err());
    assert!(ImapAtom::new("\x7F").is_err());
}

#[test]
fn atom_rejects_non_ascii() {
    assert!(ImapAtom::new("\u{00e9}").is_err()); // é
}

#[test]
fn atom_rejects_crlf() {
    assert!(ImapAtom::new("A\r\nB").is_err());
}

#[test]
fn atom_display() {
    let a = ImapAtom::new("PLAIN").unwrap();
    assert_eq!(a.to_string(), "PLAIN");
}

#[test]
fn atom_as_ref() {
    let a = ImapAtom::new("XOAUTH2").unwrap();
    let r: &str = a.as_ref();
    assert_eq!(r, "XOAUTH2");
}

#[test]
fn atom_try_from_string() {
    let a: Result<ImapAtom, _> = "PLAIN".to_string().try_into();
    assert!(a.is_ok());
}

#[test]
fn atom_try_from_str() {
    let a: Result<ImapAtom, _> = ImapAtom::try_from("PLAIN");
    assert!(a.is_ok());
}

#[test]
fn atom_try_from_invalid() {
    let a: Result<ImapAtom, _> = String::new().try_into();
    assert!(a.is_err());
}

// --- MailboxName ---

#[test]
fn mailbox_name_valid() {
    assert!(MailboxName::new("INBOX").is_ok());
    assert!(MailboxName::new("My Drafts").is_ok()); // spaces are fine
    assert!(MailboxName::new("日本語").is_ok()); // non-ASCII is fine
    assert!(MailboxName::new("Archive/2024").is_ok());
}

#[test]
fn mailbox_name_allows_empty() {
    // RFC 5464 Section 4.2: empty mailbox name is used for server-level
    // METADATA queries. RFC 3501 Section 6.3.8: LIST reference can be "".
    assert!(MailboxName::new("").is_ok());
}

#[test]
fn mailbox_name_rejects_cr() {
    assert!(MailboxName::new("INBOX\r").is_err());
}

#[test]
fn mailbox_name_rejects_lf() {
    assert!(MailboxName::new("INBOX\n").is_err());
}

#[test]
fn mailbox_name_rejects_crlf() {
    assert!(MailboxName::new("INBOX\r\nDELETE").is_err());
}

#[test]
fn mailbox_name_rejects_nul() {
    assert!(MailboxName::new("IN\0BOX").is_err());
}

#[test]
fn mailbox_name_display() {
    let m = MailboxName::new("INBOX").unwrap();
    assert_eq!(m.to_string(), "INBOX");
}

#[test]
fn mailbox_name_as_ref() {
    let m = MailboxName::new("Sent").unwrap();
    let r: &str = m.as_ref();
    assert_eq!(r, "Sent");
}

#[test]
fn mailbox_name_as_str() {
    let m = MailboxName::new("Archive").unwrap();
    assert_eq!(m.as_str(), "Archive");
}

#[test]
fn mailbox_name_try_from_string() {
    let m: Result<MailboxName, _> = "INBOX".to_string().try_into();
    assert!(m.is_ok());
}

#[test]
fn mailbox_name_try_from_invalid() {
    let m: Result<MailboxName, _> = "INBOX\r\n".to_string().try_into();
    assert!(m.is_err());
}

// --- ObjectId ---

#[test]
fn object_id_valid() {
    assert!(ObjectId::new("abc123").is_ok());
    assert!(ObjectId::new("A-B_C").is_ok());
    assert!(ObjectId::new("x").is_ok());
    // Exactly 255 chars
    assert!(ObjectId::new("a".repeat(255)).is_ok());
}

#[test]
fn object_id_rejects_empty() {
    assert!(ObjectId::new("").is_err());
}

#[test]
fn object_id_rejects_too_long() {
    assert!(ObjectId::new("a".repeat(256)).is_err());
}

#[test]
fn object_id_rejects_special_chars() {
    assert!(ObjectId::new("abc!def").is_err());
    assert!(ObjectId::new("abc def").is_err());
    assert!(ObjectId::new("abc.def").is_err());
}

#[test]
fn object_id_rejects_non_ascii() {
    assert!(ObjectId::new("abc\u{00e9}").is_err());
}

#[test]
fn object_id_display() {
    let oid = ObjectId::new("M6d99ac3275826039").unwrap();
    assert_eq!(oid.to_string(), "M6d99ac3275826039");
}

#[test]
fn object_id_as_ref() {
    let oid = ObjectId::new("abc123").unwrap();
    let r: &str = oid.as_ref();
    assert_eq!(r, "abc123");
}

#[test]
fn object_id_as_str() {
    let oid = ObjectId::new("abc123").unwrap();
    assert_eq!(oid.as_str(), "abc123");
}

#[test]
fn object_id_try_from_string() {
    let oid: Result<ObjectId, _> = "abc123".to_string().try_into();
    assert!(oid.is_ok());
}

#[test]
fn object_id_try_from_str() {
    let oid: Result<ObjectId, _> = ObjectId::try_from("abc123");
    assert!(oid.is_ok());
}

#[test]
fn object_id_try_from_invalid() {
    let oid: Result<ObjectId, _> = String::new().try_into();
    assert!(oid.is_err());
}

// --- ParsedUidSet ---

use crate::types::UidRange;

/// Helper to extract the intervals from a `ParsedUidSet` for assertion.
fn intervals(set: &ParsedUidSet) -> &[(u32, u32)] {
    &set.0
}

#[test]
fn parsed_uid_set_single_uid() {
    let ss = SequenceSet::new("5").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(5, 5)]);
}

#[test]
fn parsed_uid_set_range() {
    let ss = SequenceSet::new("5:10").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(5, 10)]);
}

#[test]
fn parsed_uid_set_multiple_disjoint() {
    let ss = SequenceSet::new("1:5,10:20").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(1, 5), (10, 20)]);
}

#[test]
fn parsed_uid_set_overlapping_merged() {
    let ss = SequenceSet::new("1:10,5:15").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(1, 15)]);
}

#[test]
fn parsed_uid_set_adjacent_merged() {
    let ss = SequenceSet::new("1:5,6:10").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(1, 10)]);
}

#[test]
fn parsed_uid_set_reversed_range() {
    let ss = SequenceSet::new("10:5").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(5, 10)]);
}

#[test]
fn parsed_uid_set_star_in_range() {
    let ss = SequenceSet::new("5:*").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(5, u32::MAX)]);
}

#[test]
fn parsed_uid_set_star_alone() {
    let ss = SequenceSet::new("*").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(u32::MAX, u32::MAX)]);
}

#[test]
fn parsed_uid_set_dollar_returns_none() {
    let ss = SequenceSet::new("$").unwrap();
    assert!(ParsedUidSet::new(&ss).is_none());
}

#[test]
fn parsed_uid_set_dollar_mixed_returns_none() {
    let ss = SequenceSet::new("$,1:5").unwrap();
    assert!(ParsedUidSet::new(&ss).is_none());
}

#[test]
fn parsed_uid_set_u32_max() {
    let ss = SequenceSet::new("4294967295").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(u32::MAX, u32::MAX)]);
}

#[test]
fn parsed_uid_set_complex_merge() {
    // Out-of-order, overlapping, and adjacent  -  should produce two intervals.
    let ss = SequenceSet::new("20:25,1:3,2:5,6:10").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    assert_eq!(intervals(&set), &[(1, 10), (20, 25)]);
}

// --- ParsedUidSet::intersect_uid_ranges ---

#[test]
fn intersect_total_containment() {
    // Requested set: 1:20. Vanished: 5:10. All within set.
    let ss = SequenceSet::new("1:20").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::range(5, 10)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert_eq!(result, vec![UidRange::range(5, 10)]);
    assert_eq!(dropped, 0);
}

#[test]
fn intersect_total_exclusion() {
    // Requested set: 1:5. Vanished: 10:20. No overlap.
    let ss = SequenceSet::new("1:5").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::range(10, 20)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert!(result.is_empty());
    assert_eq!(dropped, 11); // UIDs 10..=20
}

#[test]
fn intersect_partial_overlap() {
    // Requested set: 5:15. Vanished: 1:10. Overlap is 5:10.
    let ss = SequenceSet::new("5:15").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::range(1, 10)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert_eq!(result, vec![UidRange::range(5, 10)]);
    assert_eq!(dropped, 4); // UIDs 1,2,3,4
}

#[test]
fn intersect_spanning_gap() {
    // Requested set: 1:5,15:20. Vanished: 3:18. Overlap: 3:5, 15:18.
    let ss = SequenceSet::new("1:5,15:20").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::range(3, 18)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert_eq!(result, vec![UidRange::range(3, 5), UidRange::range(15, 18)]);
    assert_eq!(dropped, 9); // UIDs 6..=14
}

#[test]
fn intersect_single_uid_match() {
    // Requested set: 1:10. Vanished: single UID 5.
    let ss = SequenceSet::new("1:10").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::single(5)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert_eq!(result, vec![UidRange::single(5)]);
    assert_eq!(dropped, 0);
}

#[test]
fn intersect_single_uid_miss() {
    // Requested set: 1:5. Vanished: single UID 10. No overlap.
    let ss = SequenceSet::new("1:5").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::single(10)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert!(result.is_empty());
    assert_eq!(dropped, 1);
}

#[test]
fn intersect_empty_input() {
    let ss = SequenceSet::new("1:100").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let (result, dropped) = set.intersect_uid_ranges(&[]);
    assert!(result.is_empty());
    assert_eq!(dropped, 0);
}

#[test]
fn intersect_reversed_uid_range() {
    // Defensive: server sends reversed range (end < start).
    let ss = SequenceSet::new("1:10").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::range(8, 3)]; // reversed: means 3:8
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert_eq!(result, vec![UidRange::range(3, 8)]);
    assert_eq!(dropped, 0);
}

#[test]
fn intersect_vanished_entirely_in_gap() {
    // Requested set: 1:5,20:25. Vanished: 10:15. Entirely in the gap.
    let ss = SequenceSet::new("1:5,20:25").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::range(10, 15)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert!(result.is_empty());
    assert_eq!(dropped, 6);
}

#[test]
fn intersect_multiple_vanished_ranges() {
    // Multiple vanished ranges, some in-set, some out.
    let ss = SequenceSet::new("1:10,20:30").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![
        UidRange::range(5, 8),   // fully in first interval
        UidRange::range(12, 18), // fully in gap -> dropped
        UidRange::range(25, 35), // partially in second interval -> 25:30
    ];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert_eq!(result, vec![UidRange::range(5, 8), UidRange::range(25, 30)]);
    assert_eq!(dropped, 7 + 5); // 7 from gap, 5 from 31:35
}

#[test]
fn intersect_single_uid_overlap_produces_single() {
    // Vanished range overlaps with set at exactly one UID.
    let ss = SequenceSet::new("5:10").unwrap();
    let set = ParsedUidSet::new(&ss).unwrap();
    let vanished = vec![UidRange::range(1, 5)];
    let (result, dropped) = set.intersect_uid_ranges(&vanished);
    assert_eq!(result, vec![UidRange::single(5)]);
    assert_eq!(dropped, 4); // UIDs 1,2,3,4
}

#[test]
fn mailbox_inbox_identity_folds_without_migrating_persisted_ids() {
    use std::collections::HashSet;

    let upper = MailboxName::new("INBOX").unwrap();
    let lower = MailboxName::new("inbox").unwrap();
    let wire = MailboxName::from_wire("InBoX".into(), b"InBoX", false);
    assert_eq!(upper, lower);
    assert_eq!(upper, wire);
    assert!(upper.matches_mailbox(&lower));
    assert_eq!(HashSet::from([upper, lower.clone(), wire]).len(), 1);
    assert_eq!(lower.account_id(), "inbox");
    assert_eq!(
        MailboxName::from_account_id("inbox").unwrap().account_id(),
        "inbox"
    );

    let encoded = MailboxName::from_wire("INBOX".into(), b"&AEkATgBCAE8AWA-", false);
    assert_ne!(lower, encoded);
    assert!(!lower.matches_mailbox(&encoded));
    for child in ["inbox/Sub", "inbox.Sub", "inbox-Sub", "inbox_Sub"] {
        let name = MailboxName::new(child).unwrap();
        assert_ne!(
            name,
            MailboxName::new(child.replacen("inbox", "INBOX", 1)).unwrap()
        );
        assert_eq!(name.account_id(), child);
    }
}
