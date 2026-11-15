//! IMAP message flags (RFC 3501 Section 2.3.2, RFC 9051 Section 2.3.2).

/// An IMAP message flag (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
///
/// System flags are modeled as enum variants; server-specific keywords
/// are captured in `Custom`.
///
/// Comparison and hashing are case-insensitive per RFC 3501 Section 2.3.2.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Flag {
    /// `\Seen`  -  Message has been read (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    Seen,
    /// `\Answered`  -  Message has been replied to (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    Answered,
    /// `\Flagged`  -  Message is "flagged" / starred (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    Flagged,
    /// `\Deleted`  -  Message is marked for deletion (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    Deleted,
    /// `\Draft`  -  Message is a draft (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    Draft,
    /// `\Recent`  -  Message is "recent" (RFC 3501 Section 2.3.2 only, read-only, removed in RFC 9051).
    Recent,
    /// Permanent flag wildcard (\*), meaning client can create new keywords (RFC 3501 Section 7.1).
    Wildcard,
    /// A keyword flag (e.g. `$Important`, `$Junk`, `NonJunk`, or any custom keyword)
    /// (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    Custom(String),
}

impl Flag {
    /// Convert a provider-neutral mutation keyword to an IMAP flag.
    ///
    /// The neutral `$seen`, `$answered`, and `$flagged` aliases name system
    /// flags. Wire parsing deliberately keeps those literal keywords distinct.
    pub fn from_neutral_keyword(keyword: &str) -> Self {
        if keyword.eq_ignore_ascii_case("$seen") {
            Self::Seen
        } else if keyword.eq_ignore_ascii_case("$answered") {
            Self::Answered
        } else if keyword.eq_ignore_ascii_case("$flagged") {
            Self::Flagged
        } else {
            Self::from_imap_str(keyword)
        }
    }

    /// Returns the wire representation of this flag (e.g. `\Seen`, `$Important`)
    /// (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    pub fn as_imap_str(&self) -> &str {
        match self {
            Self::Seen => "\\Seen",
            Self::Answered => "\\Answered",
            Self::Flagged => "\\Flagged",
            Self::Deleted => "\\Deleted",
            Self::Draft => "\\Draft",
            Self::Recent => "\\Recent",
            Self::Wildcard => "\\*",
            Self::Custom(s) => s,
        }
    }

    /// Parse a flag from its wire representation
    /// (RFC 3501 Section 2.3.2, case-insensitive per RFC 9051 Section 2.3.2).
    pub fn from_imap_str(s: &str) -> Self {
        // RFC 3501: flag comparisons are case-insensitive.
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
            "\\seen" => Self::Seen,
            "\\answered" => Self::Answered,
            "\\flagged" => Self::Flagged,
            "\\deleted" => Self::Deleted,
            "\\draft" => Self::Draft,
            "\\recent" => Self::Recent,
            "\\*" => Self::Wildcard,
            _ => Self::Custom(s.to_owned()),
        }
    }
}

impl std::fmt::Display for Flag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_imap_str())
    }
}

/// RFC 3501 Section 2.3.2: "Flag names are case-insensitive."
///
/// All variants compare through their protocol spelling.
/// `Custom` flags compare using ASCII case-insensitive comparison so that
/// e.g. `$Important` and `$important` are treated as the same flag.
///
/// Cross-representation is also handled: `Custom("\\Seen")` equals `Seen`,
/// because they denote the same protocol flag.
// Escape variants are also constructed directly by account callers. Keep
// protocol-spelling equality until those producers can use parser-only values;
// deriving enum equality here would make equivalent wire tokens unequal.
impl PartialEq for Flag {
    fn eq(&self, other: &Self) -> bool {
        self.as_imap_str().eq_ignore_ascii_case(other.as_imap_str())
    }
}

/// RFC 3501 Section 2.3.2: flag equality is reflexive, symmetric, transitive.
impl Eq for Flag {}

/// RFC 3501 Section 2.3.2: "Flag names are case-insensitive."
///
/// The `Hash` implementation must be consistent with `PartialEq`: flags that
/// compare equal must hash to the same value. Because `Custom("\\Seen")` must
/// equal `Seen`, we hash the lowercased wire form (`as_imap_str()`) for all
/// variants, which is identical for cross-representation equivalents.
impl std::hash::Hash for Flag {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // RFC 3501 Section 2.3.2: case-insensitive hashing via wire form.
        // Custom("\\Seen") and Seen both yield "\\Seen", so lowercasing
        // produces the same hash.
        for byte in self.as_imap_str().as_bytes() {
            byte.to_ascii_lowercase().hash(state);
        }
    }
}

/// Converts a string to a `Flag` using case-insensitive matching
/// (RFC 3501 Section 2.3.2).
impl From<String> for Flag {
    fn from(s: String) -> Self {
        Self::from_imap_str(&s)
    }
}

/// Converts a string slice to a `Flag` using case-insensitive matching
/// (RFC 3501 Section 2.3.2).
impl From<&str> for Flag {
    fn from(s: &str) -> Self {
        Self::from_imap_str(s)
    }
}

#[cfg(test)]
#[path = "flag_tests.rs"]
mod tests;
