#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn uid_rejects_zero() {
    assert!(Uid::new(0).is_none());
    assert_eq!(Uid::new(42).unwrap().get(), 42);
    assert_eq!(Uid::new(u32::MAX).unwrap().get(), u32::MAX);
}

#[test]
fn uid_set_coalesces_numbers() {
    let set = UidSet::from_uids([
        Uid::new(5).unwrap(),
        Uid::new(1).unwrap(),
        Uid::new(2).unwrap(),
        Uid::new(4).unwrap(),
    ])
    .unwrap();
    assert_eq!(set.to_string(), "1:2,4:5");
}

// The QRESYNC baseline arrives range-compressed and must reach the wire
// without a detour through one `Uid` per message.
#[test]
fn uid_set_from_ranges_emits_the_wire_form_directly() {
    let set = UidSet::from_ranges(&[
        UidRange::range(1, 3),
        UidRange::single(7),
        UidRange::range(9, 10),
    ])
    .unwrap();
    assert_eq!(set.to_string(), "1:3,7,9:10");

    assert!(UidSet::from_ranges(&[]).is_none());
    // 0 is not a legal UID. Default remains an invalid internal value, and
    // the reachable contract is "empty in, None out".
    assert!(UidSet::from_ranges(&[UidRange::default()]).is_none());
    // A degenerate range (end == start) is a single UID, not "4:4".
    assert_eq!(
        UidSet::from_ranges(&[UidRange::range(4, 4)])
            .unwrap()
            .to_string(),
        "4",
    );
}

#[test]
fn uid_set_static_sets_are_valid() {
    assert_eq!(UidSet::all().to_string(), "1:*");
    assert_eq!(UidSet::saved_search().to_string(), "$");
    assert_eq!(UidSet::one(Uid::new(9).unwrap()).to_string(), "9");
}

#[test]
fn uid_set_parse_round_trips_raw_sequence_set() {
    assert_eq!(UidSet::parse("7,9:11").unwrap().to_string(), "7,9:11");
    assert_eq!(UidSet::parse("$").unwrap().to_string(), "$");
}

#[test]
fn seq_set_keeps_sequence_semantics_separate() {
    let set = SeqSet::range(Seq::new(3).unwrap(), Seq::new(8).unwrap());
    assert_eq!(set.to_string(), "3:8");
}

#[test]
fn seq_set_static_sets_are_valid() {
    assert_eq!(SeqSet::all().to_string(), "1:*");
    assert_eq!(SeqSet::saved_search().to_string(), "$");
    assert_eq!(SeqSet::one(Seq::new(1).unwrap()).to_string(), "1");
}

#[test]
fn seq_set_coalesces_numbers() {
    let set = SeqSet::from_seqs([
        Seq::new(10).unwrap(),
        Seq::new(8).unwrap(),
        Seq::new(9).unwrap(),
        Seq::new(10).unwrap(),
    ])
    .unwrap();
    assert_eq!(set.to_string(), "8:10");
}

#[test]
fn gmail_ids_require_explicit_constructors() {
    let message_id = GmailMessageId::new(1);
    let thread_id = GmailThreadId::new(1);
    assert_eq!(message_id.get(), thread_id.get());
    assert_eq!(u64::from(message_id), 1);
    assert_eq!(u64::from(thread_id), 1);
}
