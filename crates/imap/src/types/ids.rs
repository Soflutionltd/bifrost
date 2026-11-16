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
            pub const fn new(value: u32) -> Option<Self> {
                match NonZeroU32::new(value) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
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

impl From<ModSeq> for ModSeqValzer {
    fn from(value: ModSeq) -> Self {
        Self(value.get())
    }
}

impl fmt::Display for ModSeqValzer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// protocol-specific: X-GM-MSGID is a Gmail IMAP extension value, not a shared object id.
u64_id!(GmailMessageId, "A Gmail X-GM-MSGID value.");
// protocol-specific: X-GM-THRID is exposed through IMAP as u64, not bifrost_types::ThreadId.
u64_id!(GmailThreadId, "A Gmail X-GM-THRID value.");

/// One appended message, with distinct mailbox epoch and UID slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendUid {
    pub(crate) validity: UidValidity,
    pub(crate) uid: Uid,
}

/// A typed UID set for UID commands.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UidSetExpr(SequenceSet);

#[allow(
    dead_code,
    reason = "Retain command operand builders beyond the account-selected UID sets"
)]
impl UidSetExpr {
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
        Self(SequenceSet::from_finite(vec![UidRange::single(uid.get())]))
    }

    /// An inclusive UID range.
    pub fn range(start: Uid, end: Uid) -> Self {
        Self(SequenceSet::from_finite(vec![UidRange::range(
            start.get(),
            end.get(),
        )]))
    }

    /// Build a compact UID set from individual UIDs.
    ///
    /// Values are sorted and deduplicated because IMAP sequence sets are
    /// unordered set operands, not ordered result lists.
    ///
    /// Empty input returns `None` so callers cannot accidentally send an empty
    /// command operand.
    pub fn from_uids(uids: impl IntoIterator<Item = Uid>) -> Option<Self> {
        super::set::UidSet::from_uids(uids).expression().map(Self)
    }

    /// Finite range construction shares the cursor set's normalization.
    pub fn from_ranges(ranges: impl IntoIterator<Item = UidRange>) -> Option<Self> {
        super::set::UidSet::from_ranges(ranges.into_iter().collect())
            .expression()
            .map(Self)
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

impl AsRef<SequenceSet> for UidSetExpr {
    fn as_ref(&self) -> &SequenceSet {
        self.as_sequence_set()
    }
}

impl fmt::Display for UidSetExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A typed message sequence-number set for non-UID commands.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(
    dead_code,
    reason = "Sequence-addressed raw commands retain their typed operand builder"
)]
pub struct SeqSetExpr(SequenceSet<Seq>);

#[allow(
    dead_code,
    reason = "Sequence-addressed raw commands retain their typed operand builder"
)]
impl SeqSetExpr {
    /// All sequence numbers in the selected mailbox.
    pub fn all() -> Self {
        Self(SequenceSet::<Seq>::new("1:*").expect("static sequence set is valid"))
    }

    /// The server-side saved search result `$`.
    pub fn saved_search() -> Self {
        Self(SequenceSet::<Seq>::new("$").expect("static sequence set is valid"))
    }

    /// A single sequence number.
    pub fn one(seq: Seq) -> Self {
        Self(SequenceSet::<Seq>::from_finite(vec![UidRange::single(
            seq.get(),
        )]))
    }

    /// An inclusive sequence range.
    pub fn range(start: Seq, end: Seq) -> Self {
        Self(SequenceSet::<Seq>::from_finite(vec![UidRange::range(
            start.get(),
            end.get(),
        )]))
    }

    /// Build a compact sequence set from individual sequence numbers.
    ///
    /// Values are sorted and deduplicated because IMAP sequence sets are
    /// unordered set operands, not ordered result lists.
    ///
    /// Empty input returns `None` so callers cannot accidentally send an empty
    /// command operand.
    pub fn from_seqs(seqs: impl IntoIterator<Item = Seq>) -> Option<Self> {
        super::set::SeqNumSet::from_uids(seqs)
            .expression()
            .map(Self)
    }

    /// Parse a raw IMAP sequence-set string as a sequence-number set.
    ///
    /// Prefer typed constructors such as [`one`](Self::one),
    /// [`range`](Self::range), and [`from_seqs`](Self::from_seqs). This
    /// raw parser exists for protocol features such as saved-search `$` and
    /// already-validated strings from higher-level code.
    #[doc(hidden)]
    pub fn parse(value: impl Into<String>) -> Result<Self, ValidationError> {
        SequenceSet::<Seq>::new(value).map(Self)
    }

    /// Borrow the underlying validated sequence set.
    pub fn as_sequence_set(&self) -> &SequenceSet<Seq> {
        &self.0
    }

    /// Consume the wrapper and return the underlying sequence set.
    pub fn into_sequence_set(self) -> SequenceSet<Seq> {
        self.0
    }
}

impl AsRef<SequenceSet<Seq>> for SeqSetExpr {
    fn as_ref(&self) -> &SequenceSet<Seq> {
        self.as_sequence_set()
    }
}

impl fmt::Display for SeqSetExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
#[path = "ids_tests.rs"]
mod tests;
