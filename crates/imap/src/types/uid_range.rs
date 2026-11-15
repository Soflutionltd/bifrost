use super::Uid;

/// A numeric sequence bound or the mailbox-dependent IMAP `*` bound.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SeqBound {
    Number(u32),
    Star,
}

/// One canonical wire range. Finite sets use `set::NumberSet` instead.
///
/// Numeric construction normalizes direction and singletons once, so every
/// consumer sees the same members. A wildcard is never a numeric maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UidRange {
    first: Bound,
    last: Bound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Bound {
    Number(Uid),
    Star,
}

impl Bound {
    fn from_wire(bound: SeqBound) -> Self {
        match bound {
            SeqBound::Number(n) => Self::Number(Uid::new(n).expect("sequence bounds are nonzero")),
            SeqBound::Star => Self::Star,
        }
    }
    const fn wire(self) -> SeqBound {
        match self {
            Self::Number(n) => SeqBound::Number(n.get()),
            Self::Star => SeqBound::Star,
        }
    }
    const fn number(self) -> u32 {
        match self {
            Self::Number(n) => n.get(),
            Self::Star => u32::MAX,
        }
    }
}

impl UidRange {
    pub const fn start_bound(&self) -> SeqBound {
        self.first.wire()
    }
    pub fn end_bound(&self) -> Option<SeqBound> {
        (self.first != self.last).then(|| self.last.wire())
    }
    pub const fn start(&self) -> u32 {
        self.first.number()
    }
    pub fn end(&self) -> Option<u32> {
        (self.first != self.last).then(|| self.last.number())
    }
    pub const fn is_numeric(&self) -> bool {
        matches!(
            (self.first, self.last),
            (Bound::Number(_), Bound::Number(_))
        )
    }
    pub(crate) fn from_sequence_bounds(first: SeqBound, second: Option<SeqBound>) -> Self {
        let first = Bound::from_wire(first);
        let last = second.map(Bound::from_wire).unwrap_or(first);
        Self {
            first: first.min(last),
            last: first.max(last),
        }
    }
    pub fn single(uid: u32) -> Self {
        Self::try_single(uid).expect("UID must be nonzero")
    }
    pub fn range(start: u32, end: u32) -> Self {
        Self::try_range(start, end).expect("UID bounds must be nonzero")
    }
    pub fn try_single(uid: u32) -> Option<Self> {
        Self::try_range(uid, uid)
    }
    pub fn try_range(start: u32, end: u32) -> Option<Self> {
        let start = Bound::Number(Uid::new(start)?);
        let end = Bound::Number(Uid::new(end)?);
        Some(Self {
            first: start.min(end),
            last: start.max(end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{SeqBound, UidRange};

    #[test]
    fn numeric_ranges_have_one_nonzero_ascending_spelling() {
        assert_eq!(UidRange::range(9, 3), UidRange::range(3, 9));
        assert_eq!(UidRange::range(3, 3), UidRange::single(3));
        assert_eq!(UidRange::try_single(0), None);
        assert_eq!(UidRange::try_range(1, 0), None);
        assert_eq!(UidRange::try_range(0, 1), None);
    }

    #[test]
    fn numeric_maximum_and_star_remain_distinct() {
        let numeric = UidRange::single(u32::MAX);
        let star = UidRange::from_sequence_bounds(SeqBound::Star, None);
        assert_ne!(numeric, star);
        assert!(numeric.is_numeric());
        assert!(!star.is_numeric());
        let range = UidRange::from_sequence_bounds(SeqBound::Star, Some(SeqBound::Number(3)));
        assert_eq!(range.start_bound(), SeqBound::Number(3));
        assert_eq!(range.end_bound(), Some(SeqBound::Star));
    }
}
