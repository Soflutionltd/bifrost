/// A numeric sequence bound or the IMAP `*` bound.
///
/// RFC 3501 Section 9 permits `*` wherever a sequence number is allowed. It
/// means the highest number in use, while `Number(u32::MAX)` is a legal
/// numeric UID. Keeping the two forms distinct lets callers decide whether a
/// sequence-set can be expanded without treating that legal UID as `*`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SeqBound {
    /// A concrete non-zero sequence number or UID.
    Number(u32),
    /// The highest sequence number in use, whose value depends on the mailbox.
    Star,
}

/// A UID range (e.g. `1:100`, or a single UID `42`)
/// (RFC 3501 Section 9 / RFC 4315 Section 2.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct UidRange {
    /// First UID in this range (RFC 3501 Section 9 / RFC 4315 Section 2.1).
    ///
    /// For a parsed sequence-set, `u32::MAX` is also the numeric projection of
    /// `*`. Use [`start_bound`](Self::start_bound) to distinguish those cases.
    pub start: u32,
    /// `None` means a single UID (not a range) (RFC 3501 Section 9 / RFC 4315 Section 2.1).
    ///
    /// For a parsed sequence-set, `Some(u32::MAX)` is also the numeric
    /// projection of `*`. Use [`end_bound`](Self::end_bound) to distinguish
    /// those cases.
    pub end: Option<u32>,
    start_is_star: bool,
    end_is_star: bool,
}

impl UidRange {
    /// Return the start as an explicit numeric or `*` bound.
    pub const fn start_bound(&self) -> SeqBound {
        if self.start_is_star {
            SeqBound::Star
        } else {
            SeqBound::Number(self.start)
        }
    }

    /// Return the optional end as an explicit numeric or `*` bound.
    pub const fn end_bound(&self) -> Option<SeqBound> {
        match self.end {
            Some(_) if self.end_is_star => Some(SeqBound::Star),
            Some(end) => Some(SeqBound::Number(end)),
            None => None,
        }
    }

    /// Build a parsed sequence range, normalizing reversed endpoints.
    pub(crate) fn from_sequence_bounds(first: SeqBound, second: Option<SeqBound>) -> Self {
        let (start, end) = match second {
            Some(end) if first > end => (end, Some(first)),
            other => (first, other),
        };
        Self {
            start: bound_number(start),
            end: end.map(bound_number),
            start_is_star: matches!(start, SeqBound::Star),
            end_is_star: matches!(end, Some(SeqBound::Star)),
        }
    }

    /// Create a single-UID range.
    ///
    /// # Panics (debug builds only)
    /// Panics if `uid` is 0  -  UIDs are `nz-number` per RFC 3501 Section 9.
    pub const fn single(uid: u32) -> Self {
        debug_assert!(
            uid != 0,
            "UID must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        Self {
            start: uid,
            end: None,
            start_is_star: false,
            end_is_star: false,
        }
    }

    /// Create an inclusive UID range.
    ///
    /// # Panics (debug builds only)
    /// Panics if `start` or `end` is 0  -  UIDs are `nz-number` per RFC 3501 Section 9.
    pub const fn range(start: u32, end: u32) -> Self {
        debug_assert!(
            start != 0,
            "UID start must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        debug_assert!(
            end != 0,
            "UID end must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        Self {
            start,
            end: Some(end),
            start_is_star: false,
            end_is_star: false,
        }
    }

    /// Try to create a single-UID range, returning `None` if `uid` is 0
    /// (RFC 3501 Section 9: uniqueid = nz-number).
    pub const fn try_single(uid: u32) -> Option<Self> {
        if uid == 0 {
            None
        } else {
            Some(Self {
                start: uid,
                end: None,
                start_is_star: false,
                end_is_star: false,
            })
        }
    }

    /// Try to create an inclusive UID range, returning `None` if `start` or `end` is 0
    /// (RFC 3501 Section 9: uniqueid = nz-number).
    pub const fn try_range(start: u32, end: u32) -> Option<Self> {
        if start == 0 || end == 0 {
            None
        } else {
            Some(Self {
                start,
                end: Some(end),
                start_is_star: false,
                end_is_star: false,
            })
        }
    }
}

const fn bound_number(bound: SeqBound) -> u32 {
    match bound {
        SeqBound::Number(number) => number,
        SeqBound::Star => u32::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::{SeqBound, UidRange};

    #[test]
    fn single_and_range_carry_the_expected_shape() {
        let single = UidRange::single(7);
        assert_eq!(single.start, 7);
        assert_eq!(single.end, None, "a single UID is not a range");

        let range = UidRange::range(3, 9);
        assert_eq!(range.start, 3);
        assert_eq!(range.end, Some(9));

        // A one-element range is representable both ways, and the two
        // spellings are NOT equal: `end` distinguishes them.
        assert_ne!(UidRange::range(3, 3), UidRange::single(3));
    }

    #[test]
    fn numeric_maximum_and_star_are_distinct_bounds() {
        let numeric = UidRange::single(u32::MAX);
        let star = UidRange::from_sequence_bounds(SeqBound::Star, None);

        assert_eq!(numeric.start, star.start);
        assert_eq!(numeric.start_bound(), SeqBound::Number(u32::MAX));
        assert_eq!(star.start_bound(), SeqBound::Star);
        assert_ne!(numeric, star);
    }

    #[test]
    fn sequence_ranges_normalize_star_as_the_highest_bound() {
        let range =
            UidRange::from_sequence_bounds(SeqBound::Star, Some(SeqBound::Number(u32::MAX - 1)));

        assert_eq!(range.start_bound(), SeqBound::Number(u32::MAX - 1));
        assert_eq!(range.end_bound(), Some(SeqBound::Star));
    }

    // RFC 3501 Section 9: uniqueid = nz-number. The checked constructors
    // are the ones decode paths must use, because the unchecked pair only
    // asserts in debug builds - a release build would silently mint a
    // zero-UID range from corrupt input.
    #[test]
    fn checked_constructors_reject_zero_uids() {
        assert_eq!(UidRange::try_single(0), None);
        assert_eq!(UidRange::try_range(0, 5), None);
        assert_eq!(UidRange::try_range(5, 0), None);
        assert_eq!(UidRange::try_range(0, 0), None);

        assert_eq!(UidRange::try_single(1), Some(UidRange::single(1)));
        assert_eq!(UidRange::try_range(1, 2), Some(UidRange::range(1, 2)));
    }

    #[test]
    fn checked_range_does_not_police_direction() {
        // `try_range` only enforces nz-number, not ordering: a backwards
        // range is the caller's problem (cursor decode rejects it
        // explicitly in `account::envelope`).
        assert_eq!(UidRange::try_range(9, 3), Some(UidRange::range(9, 3)));
    }

    #[test]
    fn default_is_the_degenerate_zero_single() {
        // `Default` exists for struct-update syntax; it is deliberately
        // NOT a valid UID, so nothing may treat it as one.
        let default = UidRange::default();
        assert_eq!(default.start, 0);
        assert_eq!(default.end, None);
        assert_eq!(UidRange::try_single(default.start), None);
    }
}
