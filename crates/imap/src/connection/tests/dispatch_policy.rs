use super::*;

// ---------------------------------------------------------------------------
// filter_store_flags (RFC 3501 Section 2.3.2)
// ---------------------------------------------------------------------------

#[test]
fn store_flag_filter_drops_server_only_and_wildcard_flags() {
    let out = filter_store_flags(&[Flag::Recent, Flag::Seen, Flag::Wildcard, Flag::Deleted]);
    assert_eq!(out, vec![Flag::Seen, Flag::Deleted]);
}

// ---------------------------------------------------------------------------
// expand_uid_ranges (RFC 4731 Section 3.1)
// ---------------------------------------------------------------------------

#[test]
fn expand_uid_ranges_singles_and_ranges() {
    let uids = expand_uid_ranges(&[UidRange::single(1), UidRange::range(4, 6)])
        .expect("small ranges expand");
    assert_eq!(uids, vec![1, 4, 5, 6]);
}

#[test]
fn expand_uid_ranges_empty_input() {
    let uids = expand_uid_ranges(&[]).expect("empty range list expands");
    assert!(uids.is_empty());
}

#[test]
fn expand_uid_ranges_star_bound_is_not_expanded() {
    // RFC 4731 Section 3.1: `*` is the highest UID in the mailbox, which
    // the ESEARCH response alone does not reveal.
    let err = expand_uid_ranges(&[UidRange::from_sequence_bounds(
        crate::types::SeqBound::Number(10),
        Some(crate::types::SeqBound::Star),
    )])
    .expect_err("an unexpandable `*` range must not look complete");
    assert!(matches!(
        err,
        Error::ResultLimit(crate::error::ResultLimit::SearchTruncated {
            returned: 0,
            omitted: None
        })
    ));
}

#[test]
fn expand_uid_ranges_accepts_the_largest_numeric_uid() {
    assert_eq!(
        expand_uid_ranges(&[UidRange::single(u32::MAX)])
            .expect("the maximum numeric UID is concrete"),
        vec![u32::MAX]
    );
}

#[test]
fn expand_uid_ranges_caps_at_one_million() {
    let err = expand_uid_ranges(&[UidRange::range(1, 3_000_000)])
        .expect_err("an over-cap range must not be returned partially");
    assert!(matches!(
        err,
        Error::ResultLimit(crate::error::ResultLimit::SearchTruncated {
            returned: 1_000_000,
            omitted: Some(2_000_000)
        })
    ));
}

#[test]
fn expand_uid_ranges_normalizes_reversed_numeric_ranges() {
    let uids = expand_uid_ranges(&[UidRange::range(10, 5)])
        .expect("reversed ranges have the same concrete membership");
    assert_eq!(uids, vec![5, 6, 7, 8, 9, 10]);
}

/// RFC 9051 Section 9: `seq-number = nz-number / "*"`, so `*` is special
/// wherever it appears, and a sequence-set may repeat or overlap elements.
#[test]
fn expand_uid_ranges_handles_bare_star_and_overlap() {
    let err = expand_uid_ranges(&[UidRange::from_sequence_bounds(
        crate::types::SeqBound::Star,
        None,
    )])
    .expect_err("a standalone `*` is not the literal id 4294967295");
    assert!(matches!(
        err,
        Error::ResultLimit(crate::error::ResultLimit::SearchTruncated {
            returned: 0,
            omitted: None
        })
    ));

    // Overlap is legal and must be counted once, both for the cap and for the
    // expansion itself.
    let uids = expand_uid_ranges(&[UidRange::range(1, 600_000), UidRange::range(1, 600_000)])
        .expect("600000 distinct UIDs are under the 1e6 cap");
    assert_eq!(uids.len(), 600_000);

    // Partial overlap merges rather than duplicating, and the result is
    // ascending.
    let uids = expand_uid_ranges(&[
        UidRange::range(5, 9),
        UidRange::range(1, 6),
        UidRange::single(7),
    ])
    .expect("small overlapping ranges expand");
    assert_eq!(uids, (1..=9).collect::<Vec<u32>>());
}
