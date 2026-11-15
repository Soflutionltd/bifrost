//! Canonical finite sets of IMAP identifiers. Wire expressions live in SequenceSet.

use std::fmt;
use std::ops::RangeInclusive;

use super::ids::{Seq, Uid};
use super::{SequenceSet, UidRange};

pub(crate) trait SetId: Copy + Ord + fmt::Display {
    fn from_number(value: u32) -> Option<Self>;
    fn number(self) -> u32;
}

impl SetId for Uid {
    fn from_number(value: u32) -> Option<Self> {
        Self::new(value)
    }
    fn number(self) -> u32 {
        self.get()
    }
}
impl SetId for Seq {
    fn from_number(value: u32) -> Option<Self> {
        Self::new(value)
    }
    fn number(self) -> u32 {
        self.get()
    }
}

/// A finite set. Storage has no zero, wildcard, reversed or duplicate span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct IdRange<I> {
    lo: I,
    hi: I,
}

impl<I: SetId> IdRange<I> {
    fn start(self) -> u32 {
        self.lo.number()
    }
    fn end(self) -> Option<u32> {
        (self.lo != self.hi).then(|| self.hi.number())
    }
    fn new(start: u32, end: u32) -> Self {
        Self {
            lo: I::from_number(start).expect("finite set lower bound is nonzero"),
            hi: I::from_number(end).expect("finite set upper bound is nonzero"),
        }
    }
}

pub(crate) type UidSet = NumberSet<Uid>;
pub(crate) type SeqNumSet = NumberSet<Seq>;

/// Sorted, disjoint, maximally merged inclusive identifier ranges.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NumberSet<I> {
    ranges: Vec<IdRange<I>>,
}

impl<I> Default for NumberSet<I> {
    fn default() -> Self {
        Self { ranges: Vec::new() }
    }
}

impl<I: SetId> NumberSet<I> {
    pub(crate) fn from_uids(uids: impl IntoIterator<Item = I>) -> Self {
        Self::normalize(
            uids.into_iter()
                .map(|uid| IdRange { lo: uid, hi: uid })
                .collect(),
        )
    }

    /// Boundary adapter for raw SEARCH observations; zero is not membership.
    pub(crate) fn from_numbers(uids: impl IntoIterator<Item = u32>) -> Self {
        Self::from_uids(uids.into_iter().filter_map(I::from_number))
    }

    pub(crate) fn from_ranges(ranges: Vec<UidRange>) -> Self {
        Self::normalize(
            ranges
                .into_iter()
                .filter(UidRange::is_numeric)
                .map(|range| IdRange::new(range.start(), range.end().unwrap_or(range.start())))
                .collect(),
        )
    }

    fn normalize(mut ranges: Vec<IdRange<I>>) -> Self {
        ranges.sort_unstable_by_key(|range| (range.lo, range.hi));
        let mut merged: Vec<IdRange<I>> = Vec::with_capacity(ranges.len());
        for range in ranges {
            if let Some(last) = merged.last_mut()
                && range.lo.number() <= last.hi.number().saturating_add(1)
            {
                last.hi = last.hi.max(range.hi);
            } else {
                merged.push(range);
            }
        }
        Self { ranges: merged }
    }

    pub(crate) fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub(crate) fn ranges(&self) -> Vec<UidRange> {
        self.ranges
            .iter()
            .map(|range| UidRange::range(range.start(), range.hi.number()))
            .collect()
    }

    pub(crate) fn typed_ranges(&self) -> impl Iterator<Item = RangeInclusive<I>> + '_ {
        self.ranges.iter().map(|range| range.lo..=range.hi)
    }

    pub(crate) fn expression(&self) -> Option<SequenceSet> {
        if self.is_empty() {
            None
        } else {
            Some(SequenceSet::from_finite(self.ranges()))
        }
    }

    /// Client-side filtering envelope. Saved results cannot be resolved here.
    /// Wildcards require mailbox state and cannot supply a finite filter.
    pub(crate) fn from_expression(expr: &SequenceSet) -> Option<Self> {
        let ranges = expr.ranges()?;
        // The mailbox maximum can be below either numeric endpoint. An
        // enterprise user expects every reported deletion to be reconciled;
        // do not clip an unresolved query against a guessed maximum.
        if ranges.iter().any(|range| !range.is_numeric()) {
            return None;
        }
        Some(Self::from_ranges(
            ranges
                .iter()
                .map(|r| UidRange::range(r.start(), r.end().unwrap_or(r.start())))
                .collect(),
        ))
    }

    pub(crate) fn intersect_uid_ranges(&self, ranges: &[UidRange]) -> (Vec<UidRange>, usize) {
        let other = Self::from_ranges(ranges.to_vec());
        let mut intersection = Vec::new();
        let mut i = 0;
        for r in &other.ranges {
            let lo = r.start();
            let hi = r.hi.number();
            while i < self.ranges.len() && self.ranges[i].hi.number() < lo {
                i += 1;
            }
            for keep in self.ranges[i..]
                .iter()
                .take_while(|keep| keep.start() <= hi)
            {
                intersection.push(UidRange::range(
                    lo.max(keep.start()),
                    hi.min(keep.hi.number()),
                ));
            }
        }
        let kept = Self::from_ranges(intersection);
        let dropped = other.uid_count().saturating_sub(kept.uid_count());
        (kept.ranges(), dropped)
    }

    pub(crate) fn identifiers(&self) -> impl Iterator<Item = I> + '_ {
        self.iter()
            .map(|value| I::from_number(value).expect("finite set members are nonzero"))
    }

    pub(crate) fn to_uids(&self) -> Vec<u32> {
        self.iter().collect()
    }

    /// Number of individual UIDs represented by the compact ranges.
    pub(crate) fn uid_count(&self) -> usize {
        self.ranges
            .iter()
            .map(|range| {
                range
                    .end()
                    .map(|end| end.saturating_sub(range.start()).saturating_add(1))
                    .unwrap_or(1)
            })
            .map(|count| usize::try_from(count).unwrap_or(usize::MAX))
            .sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Membership test over the compact ranges. `from_uids` leaves them
    /// sorted and disjoint, so this is a binary search rather than an
    /// expansion of the whole baseline.
    pub(crate) fn contains(&self, uid: u32) -> bool {
        self.ranges
            .binary_search_by(|range| {
                let end = range.end().unwrap_or(range.start());
                if end < uid {
                    std::cmp::Ordering::Less
                } else if range.start() > uid {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    /// Insert one UID while retaining sorted, disjoint, maximally merged ranges.
    /// Returns whether the UID was absent before the insertion.
    pub(crate) fn insert(&mut self, uid: u32) -> bool {
        if uid == 0 || self.contains(uid) {
            return false;
        }
        self.insert_range(uid, uid);
        true
    }

    /// Remove one UID while retaining sorted, disjoint ranges.
    /// Returns whether the UID was present before the removal.
    pub(crate) fn remove(&mut self, uid: u32) -> bool {
        let Ok(index) = self.ranges.binary_search_by(|range| {
            let end = range.end().unwrap_or(range.start());
            if end < uid {
                std::cmp::Ordering::Less
            } else if range.start() > uid {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        }) else {
            return false;
        };
        let range = self.ranges[index];
        let end = range.end().unwrap_or(range.start());
        match (uid == range.start(), uid == end) {
            (true, true) => {
                self.ranges.remove(index);
            }
            (true, false) => {
                self.ranges[index] = IdRange::new(uid + 1, end);
            }
            (false, true) => {
                self.ranges[index] = IdRange::new(range.start(), uid - 1);
            }
            (false, false) => {
                self.ranges[index] = IdRange::new(range.start(), uid - 1);
                self.ranges.insert(index + 1, IdRange::new(uid + 1, end));
            }
        }
        true
    }

    /// Remove a wire range without expanding its numeric span. The returned
    /// set is only the intersection with known membership, including when the
    /// server coalesces holes or names UIDs never seen by this consumer.
    pub(crate) fn remove_range(&mut self, range: UidRange) -> Self {
        let end = range.end().unwrap_or(range.start());
        let start = range.start();
        if !range.is_numeric() {
            return Self::default();
        }
        let mut kept = Vec::new();
        let mut removed = Vec::new();
        for known in &self.ranges {
            let known_end = known.end().unwrap_or(known.start());
            let lo = known.start().max(start);
            let hi = known_end.min(end);
            if lo > hi {
                kept.push(*known);
                continue;
            }
            removed.push(IdRange::new(lo, hi));
            if known.start() < lo {
                kept.push(IdRange::new(known.start(), lo - 1));
            }
            if hi < known_end {
                kept.push(IdRange::new(hi + 1, known_end));
            }
        }
        self.ranges = kept;
        Self { ranges: removed }
    }

    fn insert_range(&mut self, mut start: u32, mut end: u32) {
        let index = self.ranges.partition_point(|range| {
            range.end().unwrap_or(range.start()).saturating_add(1) < start
        });
        while index < self.ranges.len() && self.ranges[index].start() <= end.saturating_add(1) {
            let range = self.ranges.remove(index);
            start = start.min(range.start());
            end = end.max(range.end().unwrap_or(range.start()));
        }
        self.ranges.insert(index, IdRange::new(start, end));
    }

    /// Ascending UIDs, produced lazily from the ranges. Callers that only
    /// walk the set in order must use this rather than `to_uids`, which
    /// materialises one `u32` per UID.
    pub(crate) fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.ranges
            .iter()
            .flat_map(|range| range.start()..=range.end().unwrap_or(range.start()))
    }

    /// Linear subtraction of two sorted range lists. Equal spans are skipped
    /// as ranges, so work is proportional to the number of ranges plus the
    /// UIDs that actually changed, not the total mailbox size.
    pub(crate) fn diff(&self, newer: &Self) -> UidSetDiff {
        UidSetDiff {
            added: subtract_ranges(&newer.ranges, &self.ranges),
            removed: subtract_ranges(&self.ranges, &newer.ranges),
        }
    }
}

fn subtract_ranges<I: SetId>(source: &[IdRange<I>], cover: &[IdRange<I>]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut cover_index = 0;
    for range in source {
        let end = u64::from(range.end().unwrap_or(range.start()));
        let mut cursor = u64::from(range.start());
        while cover_index < cover.len()
            && u64::from(
                cover[cover_index]
                    .end()
                    .unwrap_or(cover[cover_index].start()),
            ) < cursor
        {
            cover_index += 1;
        }
        let mut index = cover_index;
        while index < cover.len() && u64::from(cover[index].start()) <= end {
            let covered = cover[index];
            let covered_start = u64::from(covered.start());
            let covered_end = u64::from(covered.end().unwrap_or(covered.start()));
            if cursor < covered_start {
                out.extend((cursor..covered_start.min(end + 1)).map(uid_from_wide));
            }
            cursor = cursor.max(covered_end + 1);
            if cursor > end {
                break;
            }
            index += 1;
        }
        if cursor <= end {
            out.extend((cursor..=end).map(uid_from_wide));
        }
        cover_index = index;
    }
    out
}

#[allow(clippy::cast_possible_truncation)]
fn uid_from_wide(uid: u64) -> u32 {
    debug_assert!(uid <= u64::from(u32::MAX));
    uid as u32
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct UidSetDiff {
    pub(crate) added: Vec<u32>,
    pub(crate) removed: Vec<u32>,
}

pub(crate) fn expand_range(range: UidRange) -> Vec<u32> {
    UidSet::from_ranges(vec![range]).to_uids()
}

impl<I: SetId> fmt::Display for NumberSet<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, range) in self.ranges.iter().enumerate() {
            if index != 0 {
                f.write_str(",")?;
            }
            range.lo.fmt(f)?;
            if range.lo != range.hi {
                write!(f, ":{}", range.hi)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    // --- UidSet ---

    use crate::types::UidRange;

    /// Helper to extract the intervals from a `UidSet` for assertion.
    fn intervals(set: &UidSet) -> Vec<(u32, u32)> {
        set.ranges()
            .iter()
            .map(|r| (r.start(), r.end().unwrap_or(r.start())))
            .collect()
    }

    #[test]
    fn parsed_uid_set_single_uid() {
        let ss = SequenceSet::new("5").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(5, 5)]);
    }

    #[test]
    fn parsed_uid_set_range() {
        let ss = SequenceSet::new("5:10").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(5, 10)]);
    }

    #[test]
    fn parsed_uid_set_multiple_disjoint() {
        let ss = SequenceSet::new("1:5,10:20").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(1, 5), (10, 20)]);
    }

    #[test]
    fn parsed_uid_set_overlapping_merged() {
        let ss = SequenceSet::new("1:10,5:15").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(1, 15)]);
    }

    #[test]
    fn parsed_uid_set_adjacent_merged() {
        let ss = SequenceSet::new("1:5,6:10").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(1, 10)]);
    }

    #[test]
    fn parsed_uid_set_reversed_range() {
        let ss = SequenceSet::new("10:5").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(5, 10)]);
    }

    #[test]
    fn parsed_uid_set_star_in_range() {
        let ss = SequenceSet::new("5:*").unwrap();
        assert!(UidSet::from_expression(&ss).is_none());
    }

    #[test]
    fn parsed_uid_set_star_alone() {
        let ss = SequenceSet::new("*").unwrap();
        assert!(UidSet::from_expression(&ss).is_none());
    }

    #[test]
    fn parsed_uid_set_dollar_returns_none() {
        let ss = SequenceSet::new("$").unwrap();
        assert!(UidSet::from_expression(&ss).is_none());
    }

    #[test]
    fn parsed_uid_set_dollar_mixed_returns_none() {
        let ss = SequenceSet::new("$,1:5").unwrap();
        assert!(UidSet::from_expression(&ss).is_none());
    }

    #[test]
    fn parsed_uid_set_u32_max() {
        let ss = SequenceSet::new("4294967295").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(u32::MAX, u32::MAX)]);
    }

    #[test]
    fn parsed_uid_set_complex_merge() {
        // Out-of-order, overlapping, and adjacent  -  should produce two intervals.
        let ss = SequenceSet::new("20:25,1:3,2:5,6:10").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        assert_eq!(intervals(&set), &[(1, 10), (20, 25)]);
    }

    // --- UidSet::intersect_uid_ranges ---

    #[test]
    fn intersect_total_containment() {
        // Requested set: 1:20. Vanished: 5:10. All within set.
        let ss = SequenceSet::new("1:20").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::range(5, 10)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert_eq!(result, vec![UidRange::range(5, 10)]);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn intersect_total_exclusion() {
        // Requested set: 1:5. Vanished: 10:20. No overlap.
        let ss = SequenceSet::new("1:5").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::range(10, 20)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert!(result.is_empty());
        assert_eq!(dropped, 11); // UIDs 10..=20
    }

    #[test]
    fn intersect_partial_overlap() {
        // Requested set: 5:15. Vanished: 1:10. Overlap is 5:10.
        let ss = SequenceSet::new("5:15").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::range(1, 10)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert_eq!(result, vec![UidRange::range(5, 10)]);
        assert_eq!(dropped, 4); // UIDs 1,2,3,4
    }

    #[test]
    fn intersect_spanning_gap() {
        // Requested set: 1:5,15:20. Vanished: 3:18. Overlap: 3:5, 15:18.
        let ss = SequenceSet::new("1:5,15:20").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::range(3, 18)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert_eq!(result, vec![UidRange::range(3, 5), UidRange::range(15, 18)]);
        assert_eq!(dropped, 9); // UIDs 6..=14
    }

    #[test]
    fn intersect_single_uid_match() {
        // Requested set: 1:10. Vanished: single UID 5.
        let ss = SequenceSet::new("1:10").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::single(5)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert_eq!(result, vec![UidRange::single(5)]);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn intersect_single_uid_miss() {
        // Requested set: 1:5. Vanished: single UID 10. No overlap.
        let ss = SequenceSet::new("1:5").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::single(10)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert!(result.is_empty());
        assert_eq!(dropped, 1);
    }

    #[test]
    fn intersect_empty_input() {
        let ss = SequenceSet::new("1:100").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let (result, dropped) = set.intersect_uid_ranges(&[]);
        assert!(result.is_empty());
        assert_eq!(dropped, 0);
    }

    #[test]
    fn intersect_reversed_uid_range() {
        // Defensive: server sends reversed range (end < start).
        let ss = SequenceSet::new("1:10").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::range(8, 3)]; // reversed: means 3:8
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert_eq!(result, vec![UidRange::range(3, 8)]);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn intersect_vanished_entirely_in_gap() {
        // Requested set: 1:5,20:25. Vanished: 10:15. Entirely in the gap.
        let ss = SequenceSet::new("1:5,20:25").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::range(10, 15)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert!(result.is_empty());
        assert_eq!(dropped, 6);
    }

    #[test]
    fn intersect_multiple_vanished_ranges() {
        // Multiple vanished ranges, some in-set, some out.
        let ss = SequenceSet::new("1:10,20:30").unwrap();
        let set = UidSet::from_expression(&ss).unwrap();
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
        let set = UidSet::from_expression(&ss).unwrap();
        let vanished = vec![UidRange::range(1, 5)];
        let (result, dropped) = set.intersect_uid_ranges(&vanished);
        assert_eq!(result, vec![UidRange::single(5)]);
        assert_eq!(dropped, 4); // UIDs 1,2,3,4
    }
}

#[cfg(test)]
mod shared_path_tests {
    use super::*;

    #[test]
    fn reversed_ranges_agree_across_finite_and_wire_consumers() {
        let reversed = UidRange::try_range(9, 3).expect("nonzero bounds");
        let expected = UidSet::from_numbers(3..=9);
        let finite = UidSet::from_ranges(vec![reversed, UidRange::single(5)]);
        assert_eq!(finite, expected);
        let operand = super::super::UidSetExpr::from_ranges([reversed]).expect("nonempty");
        assert_eq!(operand.to_string(), "3:9");
        let parsed = UidSet::from_expression(&SequenceSet::new("9:3,5").expect("valid set"))
            .expect("finite");
        assert_eq!(parsed, finite);
        assert_eq!(
            parsed.intersect_uid_ranges(&[UidRange::range(11, 7)]),
            (vec![UidRange::range(7, 9)], 2)
        );
        let mut removed = finite;
        assert_eq!(removed.remove_range(UidRange::range(9, 3)), expected);
        assert!(removed.is_empty());
    }
}

#[cfg(test)]
mod algebra_tests {
    use super::*;
    #[test]
    fn compact_uid_set_roundtrips_and_diffs() {
        let set = UidSet::from_numbers([1, 2, 3, 7, 9, 10]);
        assert_eq!(set.to_uids(), vec![1, 2, 3, 7, 9, 10]);
        assert_eq!(
            set.ranges(),
            &[
                UidRange::range(1, 3),
                UidRange::single(7),
                UidRange::range(9, 10)
            ]
        );

        let newer = UidSet::from_numbers([2, 3, 4, 9]);
        let diff = set.diff(&newer);
        assert_eq!(diff.added, vec![4]);
        assert_eq!(diff.removed, vec![1, 7, 10]);
        assert_eq!(set.uid_count(), 6);
    }

    #[test]
    fn compact_uid_set_mutation_splits_and_merges_ranges() {
        let mut set = UidSet::from_ranges(vec![UidRange::range(1, 500_000)]);
        assert!(set.remove(250_000));
        assert!(!set.contains(250_000));
        assert_eq!(set.ranges().len(), 2);
        assert!(!set.remove(250_000));

        assert!(set.insert(250_000));
        assert_eq!(set.ranges(), &[UidRange::range(1, 500_000)]);
        assert!(!set.insert(250_000));

        assert!(set.remove(1));
        assert!(set.remove(500_000));
        assert_eq!(set.ranges(), &[UidRange::range(2, 499_999)]);
    }

    #[test]
    fn compact_uid_set_remove_normalises_one_element_residue() {
        // Trimming the low end down to a single survivor.
        let mut set = UidSet::from_numbers([1, 2]);
        assert!(set.remove(1));
        assert_eq!(set, UidSet::from_numbers([2]));
        assert_eq!(set.ranges(), &[UidRange::single(2)]);

        // Trimming the high end down to a single survivor.
        let mut set = UidSet::from_numbers([1, 2]);
        assert!(set.remove(2));
        assert_eq!(set, UidSet::from_numbers([1]));
        assert_eq!(set.ranges(), &[UidRange::single(1)]);

        // Splitting from the middle, leaving a single survivor on each side.
        let mut set = UidSet::from_numbers([1, 2, 3]);
        assert!(set.remove(2));
        assert_eq!(set, UidSet::from_numbers([1, 3]));
        assert_eq!(set.ranges(), &[UidRange::single(1), UidRange::single(3)]);

        // Splitting with a multi-UID remainder on one side only.
        let mut set = UidSet::from_numbers([1, 2, 3, 4]);
        assert!(set.remove(2));
        assert_eq!(set, UidSet::from_numbers([1, 3, 4]));
        assert_eq!(set.ranges(), &[UidRange::single(1), UidRange::range(3, 4)]);
    }

    #[test]
    fn compact_uid_set_mutation_handles_empty_and_single_element_sets() {
        let mut set = UidSet::default();
        assert!(!set.remove(1), "removing from an empty set reports absent");
        assert!(set.is_empty());

        assert!(set.insert(5));
        assert_eq!(set.ranges(), &[UidRange::single(5)]);
        assert!(!set.insert(5), "re-inserting reports already present");

        assert!(set.remove(5));
        assert!(set.is_empty(), "removing the only UID empties the set");
        assert_eq!(set.ranges(), &[]);

        // UID 0 is not an `nz-number` and must never enter the set.
        assert!(!set.insert(0));
        assert!(set.is_empty());
        assert!(!set.remove(0));
    }

    #[test]
    fn compact_uid_set_insert_merges_across_range_boundaries() {
        let mut set = UidSet::from_numbers([1, 2, 4, 5]);
        assert_eq!(
            set.ranges(),
            &[UidRange::range(1, 2), UidRange::range(4, 5)]
        );

        // The gap UID bridges two neighbours into one range.
        assert!(set.insert(3));
        assert_eq!(set.ranges(), &[UidRange::range(1, 5)]);
        assert_eq!(set.uid_count(), 5);

        // Extending past each endpoint keeps a single merged range.
        assert!(set.insert(6));
        assert_eq!(set.ranges(), &[UidRange::range(1, 6)]);
        assert!(set.insert(7));
        assert_eq!(set.ranges(), &[UidRange::range(1, 7)]);

        // A UID two past the end is disjoint, not adjacent.
        assert!(set.insert(9));
        assert_eq!(set.ranges(), &[UidRange::range(1, 7), UidRange::single(9)]);
        // ...and filling the hole merges it back in.
        assert!(set.insert(8));
        assert_eq!(set.ranges(), &[UidRange::range(1, 9)]);

        // A single UID adjacent below the first range extends it downward.
        let mut set = UidSet::from_numbers([5, 6]);
        assert!(set.insert(4));
        assert_eq!(set.ranges(), &[UidRange::range(4, 6)]);
        assert!(set.insert(1));
        assert_eq!(set.ranges(), &[UidRange::single(1), UidRange::range(4, 6)]);
    }

    #[test]
    fn compact_uid_set_mutations_agree_with_a_reference_set() {
        let mut set = UidSet::default();
        let mut reference = std::collections::BTreeSet::new();

        // A deterministic walk that hits adjacency, splitting and re-merging.
        let script: [(bool, u32); 18] = [
            (true, 10),
            (true, 11),
            (true, 12),
            (true, 14),
            (true, 13),
            (false, 12),
            (true, 1),
            (true, 2),
            (false, 1),
            (false, 2),
            (true, 20),
            (false, 20),
            (true, 12),
            (false, 14),
            (false, 10),
            (true, 10),
            (false, 11),
            (false, 13),
        ];
        for (insert, uid) in script {
            if insert {
                assert_eq!(set.insert(uid), reference.insert(uid), "insert {uid}");
            } else {
                assert_eq!(set.remove(uid), reference.remove(&uid), "remove {uid}");
            }
            let expected = UidSet::from_numbers(reference.iter().copied());
            assert_eq!(
                set,
                expected,
                "after {}{uid}",
                if insert { '+' } else { '-' }
            );
            assert_eq!(set.uid_count(), reference.len());
            assert_eq!(set.to_uids(), reference.iter().copied().collect::<Vec<_>>());
            for uid in 1..25 {
                assert_eq!(
                    set.contains(uid),
                    reference.contains(&uid),
                    "contains {uid}"
                );
            }
        }
    }

    #[test]
    fn compact_uid_set_diff_handles_exhausted_sides() {
        let empty = UidSet::default();
        let some = UidSet::from_numbers([4, 5, 9]);

        let from_empty = empty.diff(&some);
        assert_eq!(from_empty.added, vec![4, 5, 9]);
        assert!(from_empty.removed.is_empty());

        let to_empty = some.diff(&empty);
        assert!(to_empty.added.is_empty());
        assert_eq!(to_empty.removed, vec![4, 5, 9]);

        assert_eq!(some.diff(&some), UidSetDiff::default());

        // Disjoint sets: every UID on each side must appear exactly once,
        // interleaved rather than concatenated.
        let disjoint = UidSet::from_numbers([1, 6, 7]);
        let diff = disjoint.diff(&some);
        assert_eq!(diff.added, vec![4, 5, 9]);
        assert_eq!(diff.removed, vec![1, 6, 7]);
    }

    #[test]
    fn compact_uid_set_diff_handles_max_uid_without_false_change() {
        let max = UidSet::from_numbers([u32::MAX]);
        assert_eq!(max.diff(&max), UidSetDiff::default());
        assert_eq!(UidSet::default().diff(&max).added, vec![u32::MAX]);
        assert_eq!(max.diff(&UidSet::default()).removed, vec![u32::MAX]);
    }

    #[test]
    fn compact_uid_set_diff_matches_set_semantics_over_generated_pairs() {
        fn naive(a: &UidSet, b: &UidSet) -> Vec<u32> {
            let cover: std::collections::BTreeSet<u32> = b.iter().collect();
            a.iter().filter(|uid| !cover.contains(uid)).collect()
        }

        let mut seed = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..400 {
            let mut left = Vec::new();
            let mut right = Vec::new();
            // A small universe with dense, streaky membership is what produces
            // long equal runs and boundary-straddling covers; uniform sparse
            // picks would almost never generate either.
            for uid in 1..=64_u32 {
                if next() % 3 != 0 {
                    left.push(uid);
                }
                if next() % 3 != 0 {
                    right.push(uid);
                }
            }
            let a = UidSet::from_numbers(left);
            let b = UidSet::from_numbers(right);
            let diff = a.diff(&b);
            assert_eq!(diff.added, naive(&b, &a), "added: {a:?} -> {b:?}");
            assert_eq!(diff.removed, naive(&a, &b), "removed: {a:?} -> {b:?}");
        }
    }

    #[test]
    fn compact_uid_set_contains_probes_the_ranges() {
        let set = UidSet::from_numbers([1, 2, 3, 7, 9, 10]);
        for uid in set.to_uids() {
            assert!(set.contains(uid), "{uid} is in the set");
        }
        for uid in [0, 4, 5, 6, 8, 11, u32::MAX] {
            assert!(!set.contains(uid), "{uid} is not in the set");
        }
        assert!(!UidSet::default().contains(1));
    }

    #[test]
    fn compact_uid_set_drops_zero_sorts_and_dedupes() {
        // UIDs are nz-number: a 0 is not a UID and must never reach a
        // range. Input order and duplicates must not survive either.
        let set = UidSet::from_numbers([9, 0, 2, 2, 1, 0, 3]);
        assert_eq!(set.to_uids(), vec![1, 2, 3, 9]);
        assert_eq!(set.ranges(), &[UidRange::range(1, 3), UidRange::single(9)]);
        assert_eq!(set.uid_count(), 4);

        assert!(UidSet::from_numbers([0, 0]).is_empty());
        assert!(UidSet::default().is_empty());
        assert_eq!(UidSet::default().uid_count(), 0);
    }

    #[test]
    fn compact_uid_set_from_ranges_normalizes_overlap_and_order() {
        // `from_ranges` is the cursor-decode entry point, so it must cope
        // with a set that was written out of order or with overlaps and
        // still produce one canonical merged range list.
        let set = UidSet::from_ranges(vec![
            UidRange::range(5, 8),
            UidRange::single(1),
            UidRange::range(2, 6),
        ]);
        assert_eq!(set.ranges(), &[UidRange::range(1, 8)]);
        assert_eq!(set.uid_count(), 8);
    }

    #[test]
    fn compact_uid_set_diff_reports_both_directions() {
        let old = UidSet::from_numbers([1, 2, 3]);
        let new = UidSet::from_numbers([2, 3, 4]);
        let forward = old.diff(&new);
        assert_eq!(forward.added, vec![4]);
        assert_eq!(forward.removed, vec![1]);
        let backward = new.diff(&old);
        assert_eq!(backward.added, vec![1]);
        assert_eq!(backward.removed, vec![4]);
        // A set diffed against itself reports nothing.
        assert_eq!(old.diff(&old), UidSetDiff::default());
    }

    #[test]
    fn expand_range_covers_single_and_inclusive_range() {
        assert_eq!(expand_range(UidRange::single(4)), vec![4]);
        assert_eq!(expand_range(UidRange::range(4, 7)), vec![4, 5, 6, 7]);
        assert_eq!(expand_range(UidRange::range(4, 4)), vec![4]);
    }
}
