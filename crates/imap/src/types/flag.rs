//! IMAP message flags (RFC 3501 Section 2.3.2, RFC 9051 Section 2.3.2).

/// A parser-owned unknown flag spelling. Construction is confined to this module.
#[derive(Debug, Clone)]
pub struct FlagKeyword(String);

impl std::ops::Deref for FlagKeyword {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

/// A validated writable view. Observed flags remain lossless, including malformed
/// extension spellings; only this view is admitted to STORE and APPEND output.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StoreFlag<'a>(&'a Flag);

impl<'a> StoreFlag<'a> {
    pub(crate) fn from_observed(flag: &'a Flag) -> Result<Option<Self>, crate::Error> {
        if !flag.is_writable() {
            return Ok(None);
        }
        if let Flag::Custom(keyword) = flag {
            super::validated::validate_atom_bytes(keyword.as_bytes(), "flag keyword")
                .map_err(|err| crate::Error::InvalidInput(err.to_string().into()))?;
        }
        Ok(Some(Self(flag)))
    }

    pub(crate) fn as_imap_str(self) -> &'a str {
        self.0.as_imap_str()
    }
}

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
    // Observations deliberately share one lossless enum across FLAGS and
    // PERMANENTFLAGS: a malformed server observation must remain inspectable.
    // Client output instead requires StoreFlag, which excludes both server-only
    // values and validates keyword syntax before any write.
    /// Permanent flag wildcard (\*), meaning client can create new keywords (RFC 3501 Section 7.1).
    Wildcard,
    /// A keyword flag (e.g. `$Important`, `$Junk`, `NonJunk`, or any custom keyword)
    /// (RFC 3501 Section 2.3.2 / RFC 9051 Section 2.3.2).
    Custom(FlagKeyword),
}

impl Flag {
    /// Whether the protocol permits a client to request this flag.
    pub(crate) fn is_writable(&self) -> bool {
        !matches!(self, Self::Recent | Self::Wildcard)
    }

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
            _ => Self::Custom(FlagKeyword(s.to_owned())),
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
/// Known tokens are normalized by the parser before a value is constructed.
// Unknown spellings are parser-owned; known flags cannot hide in Custom.
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
/// compare equal must hash to the same value. Hash the folded wire spelling.
impl std::hash::Hash for Flag {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // RFC 3501 Section 2.3.2: case-insensitive hashing via wire form.
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
