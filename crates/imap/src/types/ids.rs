//! Strongly typed IMAP identifiers and typed sequence-set wrappers.

use std::fmt;
use std::num::NonZeroU32;

use super::{SequenceSet, UidRange, ValidationError};

macro_rules! nonzero_u32_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU32);

        impl $name {
            /// Create the identifier. Returns `None` for zero, which is not
            /// valid for IMAP `nz-number` identifiers.
            pub fn new(value: u32) -> Option<Self> {
                NonZeroU32::new(value).map(Self)
            }

            /// Create the identifier, returning a validation error for zero.
            pub fn try_new(value: u32) -> Result<Self, ValidationError> {
                Self::new(value).ok_or_else(|| {
                    ValidationError::new(
                        super::validated::ValidationReason::ZeroIdentifier,
                        concat!(stringify!($name), " must be greater than zero"),
                    )
                })
            }

            /// Return the raw protocol value.
            pub const fn get(self) -> u32 {
                self.0.get()
            }
        }

        impl TryFrom<u32> for $name {
            type Error = ValidationError;

            fn try_from(value: u32) -> Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }

        impl From<$name> for u32 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(f)
            }
        }
    };
}

nonzero_u32_id!(Uid, "A stable IMAP UID within a UIDVALIDITY epoch.");
nonzero_u32_id!(Seq, "A volatile IMAP message sequence number.");
nonzero_u32_id!(UidValidity, "A UIDVALIDITY value for a mailbox.");

macro_rules! u64_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            /// Create the identifier from its raw value.
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Return the raw protocol value.
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(f)
            }
        }
    };
}

/// A positive RFC 7162 modification sequence, bounded to 63 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModSeq(u64);

impl ModSeq {
    /// Shared wire ceiling for mod-sequence-value and mod-sequence-valzer.
    pub const MAX: u64 = i64::MAX as u64;

    pub const fn new(value: u64) -> Option<Self> {
        if value > 0 && value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ModSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// UNCHANGEDSINCE additionally admits zero (RFC 7162 Section 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModSeqValzer(u64);

impl ModSeqValzer {
    pub const fn new(value: u64) -> Option<Self> {
        if value <= ModSeq::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

// protocol-specific: X-GM-MSGID is a Gmail IMAP extension value, not a shared object id.
u64_id!(GmailMessageId, "A Gmail X-GM-MSGID value.");
// protocol-specific: X-GM-THRID is exposed through IMAP as u64, not bifrost_types::ThreadId.
u64_id!(GmailThreadId, "A Gmail X-GM-THRID value.");

/// A typed UID set for UID commands.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UidSet(SequenceSet);

impl UidSet {
    /// All UIDs in the selected mailbox.
    pub fn all() -> Self {
        Self(SequenceSet::new("1:*").expect("static UID set is valid"))
    }

    /// The server-side saved search result `$`.
    pub fn saved_search() -> Self {
        Self(SequenceSet::new("$").expect("static UID set is valid"))
    }

    /// A single UID.
    pub fn one(uid: Uid) -> Self {
        Self(SequenceSet::new(uid.to_string()).expect("UID is valid sequence-set member"))
    }

    /// An inclusive UID range.
    pub fn range(start: Uid, end: Uid) -> Self {
        let set = format!("{start}:{end}");
        Self(SequenceSet::new(set).expect("UID range is valid sequence set"))
    }

    /// Build a compact UID set from individual UIDs.
    ///
    /// Values are sorted and deduplicated because IMAP sequence sets are
    /// unordered set operands, not ordered result lists.
    ///
    /// Empty input returns `None` so callers cannot accidentally send an empty
    /// command operand.
    pub fn from_uids(uids: impl IntoIterator<Item = Uid>) -> Option<Self> {
        let mut values: Vec<u32> = uids.into_iter().map(Uid::get).collect();
        if values.is_empty() {
            return None;
        }
        values.sort_unstable();
        values.dedup();
        Some(Self(
            SequenceSet::new(coalesce_numbers(&values)).expect("coalesced UID set is valid"),
        ))
    }

    /// Build a UID set from already-coalesced ascending ranges.
    ///
    /// The counterpart to [`from_uids`](Self::from_uids) for callers that
    /// already hold range-compressed UIDs - a QRESYNC known-UID baseline,
    /// for one, which can name hundreds of thousands of messages. Expanding
    /// those into individual `Uid`s only to re-coalesce them is the whole
    /// cost of the call.
    ///
    /// Ranges are emitted in the order given; `0` is not a legal UID, so a
    /// range starting at zero is skipped. Empty input (or input that is
    /// entirely skipped) returns `None`.
    pub fn from_ranges<'a>(ranges: impl IntoIterator<Item = &'a UidRange>) -> Option<Self> {
        let mut set = String::new();
        for range in ranges {
            if range.start == 0 {
                continue;
            }
            if !set.is_empty() {
                set.push(',');
            }
            match range.end {
                Some(end) if end > range.start => {
                    use std::fmt::Write as _;
                    let _ = write!(set, "{}:{end}", range.start);
                }
                _ => {
                    use std::fmt::Write as _;
                    let _ = write!(set, "{}", range.start);
                }
            }
        }
        if set.is_empty() {
            return None;
        }
        Some(Self(
            SequenceSet::new(set).expect("coalesced UID ranges are a valid sequence set"),
        ))
    }

    /// Parse a raw IMAP sequence-set string as a UID set.
    ///
    /// Prefer typed constructors such as [`one`](Self::one),
    /// [`range`](Self::range), and [`from_uids`](Self::from_uids). This
    /// raw parser exists for protocol features such as saved-search `$` and
    /// already-validated strings from higher-level code.
    #[doc(hidden)]
    pub fn parse(value: impl Into<String>) -> Result<Self, ValidationError> {
        SequenceSet::new(value).map(Self)
    }

    /// Borrow the underlying validated sequence set.
    pub fn as_sequence_set(&self) -> &SequenceSet {
        &self.0
    }

    /// Consume the wrapper and return the underlying sequence set.
    pub fn into_sequence_set(self) -> SequenceSet {
        self.0
    }
}

impl AsRef<SequenceSet> for UidSet {
    fn as_ref(&self) -> &SequenceSet {
        self.as_sequence_set()
    }
}

impl fmt::Display for UidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A typed message sequence-number set for non-UID commands.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeqSet(SequenceSet);

impl SeqSet {
    /// All sequence numbers in the selected mailbox.
    pub fn all() -> Self {
        Self(SequenceSet::new("1:*").expect("static sequence set is valid"))
    }

    /// The server-side saved search result `$`.
    pub fn saved_search() -> Self {
        Self(SequenceSet::new("$").expect("static sequence set is valid"))
    }

    /// A single sequence number.
    pub fn one(seq: Seq) -> Self {
        Self(SequenceSet::new(seq.to_string()).expect("sequence number is valid"))
    }

    /// An inclusive sequence range.
    pub fn range(start: Seq, end: Seq) -> Self {
        let set = format!("{start}:{end}");
        Self(SequenceSet::new(set).expect("sequence range is valid sequence set"))
    }

    /// Build a compact sequence set from individual sequence numbers.
    ///
    /// Values are sorted and deduplicated because IMAP sequence sets are
    /// unordered set operands, not ordered result lists.
    ///
    /// Empty input returns `None` so callers cannot accidentally send an empty
    /// command operand.
    pub fn from_seqs(seqs: impl IntoIterator<Item = Seq>) -> Option<Self> {
        let mut values: Vec<u32> = seqs.into_iter().map(Seq::get).collect();
        if values.is_empty() {
            return None;
        }
        values.sort_unstable();
        values.dedup();
        Some(Self(
            SequenceSet::new(coalesce_numbers(&values)).expect("coalesced sequence set is valid"),
        ))
    }

    /// Parse a raw IMAP sequence-set string as a sequence-number set.
    ///
    /// Prefer typed constructors such as [`one`](Self::one),
    /// [`range`](Self::range), and [`from_seqs`](Self::from_seqs). This
    /// raw parser exists for protocol features such as saved-search `$` and
    /// already-validated strings from higher-level code.
    #[doc(hidden)]
    pub fn parse(value: impl Into<String>) -> Result<Self, ValidationError> {
        SequenceSet::new(value).map(Self)
    }

    /// Borrow the underlying validated sequence set.
    pub fn as_sequence_set(&self) -> &SequenceSet {
        &self.0
    }

    /// Consume the wrapper and return the underlying sequence set.
    pub fn into_sequence_set(self) -> SequenceSet {
        self.0
    }
}

impl AsRef<SequenceSet> for SeqSet {
    fn as_ref(&self) -> &SequenceSet {
        self.as_sequence_set()
    }
}

impl fmt::Display for SeqSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn coalesce_numbers(values: &[u32]) -> String {
    let mut parts = Vec::new();
    let mut start = values[0];
    let mut prev = start;

    for &value in &values[1..] {
        if value == prev.saturating_add(1) {
            prev = value;
            continue;
        }
        push_range(&mut parts, start, prev);
        start = value;
        prev = value;
    }
    push_range(&mut parts, start, prev);
    parts.join(",")
}

fn push_range(parts: &mut Vec<String>, start: u32, end: u32) {
    if start == end {
        parts.push(start.to_string());
    } else {
        parts.push(format!("{start}:{end}"));
    }
}

#[cfg(test)]
#[path = "ids_tests.rs"]
mod tests;
