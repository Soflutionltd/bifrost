#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
type SequenceSet = super::SequenceSet<crate::types::Uid>;

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
