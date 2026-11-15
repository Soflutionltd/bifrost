//! Flag identity at the account boundary.

use std::collections::HashSet;

/// A mail flag or keyword, retaining its spelling for provider writes.
///
/// Keywords have ASCII-case-insensitive identity. Use [`Flag::literal`] for
/// opaque provider tokens, such as Gmail label ids. Retaining that distinction
/// prevents a case-only difference from editing another enterprise user's label.
#[derive(Debug, Clone)]
pub struct Flag {
    spelling: String,
    identity: String,
}

impl Flag {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        let spelling = value.into();
        let identity = spelling.to_ascii_lowercase();
        Self { spelling, identity }
    }

    /// Preserve exact identity for a provider token rather than an IMAP/JMAP
    /// keyword. This does not validate the provider's token grammar.
    #[must_use]
    pub fn literal(value: impl Into<String>) -> Self {
        let spelling = value.into();
        let identity = spelling.clone();
        Self { spelling, identity }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.spelling
    }

    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }
}

impl std::ops::Deref for Flag {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl From<&String> for Flag {
    fn from(value: &String) -> Self {
        Self::new(value)
    }
}

impl From<&Flag> for Flag {
    fn from(value: &Flag) -> Self {
        value.clone()
    }
}

impl PartialEq for Flag {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl Eq for Flag {}

impl std::hash::Hash for Flag {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

impl AsRef<str> for Flag {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for Flag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for Flag {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for Flag {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// A set whose membership, equality and set operations use [`Flag`] identity.
///
/// Iteration yields retained spellings; no mutable spelling escapes the set.
/// Inserting an equivalent spelling keeps the first spelling, as `HashSet` does.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlagSet(HashSet<Flag>);

impl FlagSet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn insert(&mut self, flag: impl Into<Flag>) -> bool {
        self.0.insert(flag.into())
    }

    #[must_use]
    pub fn contains(&self, flag: impl Into<Flag>) -> bool {
        self.0.contains(&flag.into())
    }

    pub fn remove(&mut self, flag: impl Into<Flag>) -> bool {
        self.0.remove(&flag.into())
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Flag> {
        self.0.iter()
    }

    #[must_use]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    #[must_use]
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.0.is_disjoint(&other.0)
    }
}

impl<T: Into<Flag>> FromIterator<T> for FlagSet {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut flags = Self::new();
        flags.extend(iter);
        flags
    }
}

impl<T: Into<Flag>> Extend<T> for FlagSet {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for flag in iter {
            self.insert(flag);
        }
    }
}

impl<const N: usize> From<[String; N]> for FlagSet {
    fn from(flags: [String; N]) -> Self {
        flags.into_iter().collect()
    }
}

impl<'a> IntoIterator for &'a FlagSet {
    type Item = &'a Flag;
    type IntoIter = std::collections::hash_set::Iter<'a, Flag>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for FlagSet {
    type Item = Flag;
    type IntoIter = std::collections::hash_set::IntoIter<Flag>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FlagOp, FlagOpValidationError, FlagTargetState, canonical_flags_hash, flag_target_state,
    };

    #[test]
    fn flag_identity_drives_sets_hashes_validation_and_observation() {
        let mut flags: FlagSet = ["\\Seen", "\\SEEN", "$Important"].into_iter().collect();
        assert_eq!(flags.len(), 2);
        assert!(flags.contains("\\seen"));
        assert!(!flags.insert("\\sEeN"));
        assert!(flags.iter().any(|flag| flag.as_str() == "\\Seen"));
        let same: FlagSet = ["$IMPORTANT", "\\seen"].into_iter().collect();
        assert_eq!(flags, same);
        assert_eq!(canonical_flags_hash(&flags), canonical_flags_hash(&same));
        assert_eq!(
            flag_target_state(&flags, &FlagOp::Set(same)),
            FlagTargetState::Satisfied
        );
        assert_eq!(
            FlagOp::Patch {
                add: flags.clone(),
                remove: ["\\seen"].into_iter().collect()
            }
            .validate(),
            Err(FlagOpValidationError::ContradictoryPatch)
        );
        assert!(flags.remove("\\seen"));
        assert!(!flags.contains("\\SEEN"));
        let keys = HashSet::from([Flag::from("$Important"), Flag::from("$IMPORTANT")]);
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn flag_sets_keep_opaque_label_identity() {
        let upper = Flag::literal("$gmail-label:Label_1:Work");
        let lower = Flag::literal("$gmail-label:label_1:Work");
        let labels: FlagSet = [upper.clone(), lower.clone()].into_iter().collect();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&upper));
        let upper: FlagSet = [upper].into_iter().collect();
        let lower: FlagSet = [lower].into_iter().collect();
        assert!(upper.is_disjoint(&lower));
        assert_ne!(canonical_flags_hash(&upper), canonical_flags_hash(&lower));
        assert!(
            FlagOp::Patch {
                add: upper.clone(),
                remove: lower.clone()
            }
            .validate()
            .is_ok()
        );
        assert_eq!(
            flag_target_state(&upper, &FlagOp::Add(lower)),
            FlagTargetState::Unsatisfied
        );
        assert_eq!(labels.clone().into_iter().collect::<FlagSet>(), labels);
        // These are ordinary, valid JMAP keywords too: shared identity must
        // never infer a provider from a string prefix.
        assert_eq!(Flag::new("Category:Work"), Flag::new("category:work"));
    }

    #[test]
    fn flag_sets_fold_ascii_only_and_extend_without_duplicate_members() {
        let mut flags: FlagSet = ["M\u{00e9}mo", "M\u{00c9}mo"].into_iter().collect();
        flags.extend(["m\u{00e9}MO", "m\u{00c9}MO"]);
        assert_eq!(flags.len(), 2);
        assert!(flags.contains("M\u{00e9}MO"));
        let roundtrip: FlagSet = flags.clone().into_iter().collect();
        assert_eq!(roundtrip, flags);
    }
}
