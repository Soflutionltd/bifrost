//! Typed builder for IMAP SEARCH criteria.
//!
//! RFC 3501 Section 6.4.4 (`IMAP4rev1` SEARCH command) and
//! RFC 9051 Section 6.4.4 (`IMAP4rev2` SEARCH command).
//!
//! The [`SearchCriteria`] builder retains structured IMAP SEARCH operands
//! by construction. Individual criteria are AND-combined by chaining method
//! calls, matching IMAP's implicit conjunction semantics (RFC 3501 Section
//! 6.4.4: "When multiple keys are specified, the result is the intersection
//! (AND function) of all the messages that match those keys.").
//!
//! This builder is internal to the account implementation, so external
//! doctests cannot import it. The in-crate `chain_multiple_criteria` test
//! exercises chaining UNSEEN, SINCE and FROM and checks the encoded string.

use std::fmt;

use super::validated::{is_valid_sequence_set, validate_atom_bytes};

#[path = "search_helpers.rs"]
mod search_helpers;

use search_helpers::{append_search_string, push_parenthesized_if_compound, validate_imap_date};
#[cfg(test)]
use search_helpers::{count_top_level_tokens, is_compound};

/// A builder for type-safe IMAP SEARCH criteria (RFC 3501 Section 6.4.4).
///
/// Retains typed operands until command encoding. Compound criteria are
/// built by chaining method calls -- each call adds an AND condition,
/// matching IMAP's implicit conjunction semantics (RFC 3501 Section 6.4.4).
///
/// # References
/// - RFC 3501 Section 6.4.4 (SEARCH command)
/// - RFC 9051 Section 6.4.4 (`IMAP4rev2` SEARCH)
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SearchCriteria {
    /// Explicit text view for diagnostics and callers requesting raw syntax.
    buf: String,
    keys: Vec<SearchKey>,
}

/// A structured search operand. The builder validates dates, flags and sets;
/// caller-provided extension grammar stays explicitly raw.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(
    dead_code,
    reason = "Typed SEARCH supports operands beyond the account filter compiler"
)]
pub(crate) enum SearchKey {
    Keyword(&'static str),
    Date {
        key: &'static str,
        value: String,
    },
    Number {
        key: &'static str,
        value: u64,
    },
    Flag {
        excluded: bool,
        value: String,
    },
    Uid(String),
    Sequence(String),
    String {
        prefix: String,
        value: String,
    },
    Not(Box<SearchCriteria>),
    Or(Box<SearchCriteria>, Box<SearchCriteria>),
    Group(SearchCriteria),
    Raw {
        text: String,
        requires: Vec<super::Capability>,
    },
}

#[allow(
    dead_code,
    reason = "Typed SEARCH builders support operands beyond the account filter compiler"
)]
impl SearchCriteria {
    /// Create a new, empty criteria builder.
    ///
    /// An empty builder produces no criteria. At least one criterion must
    /// be added before passing to a SEARCH command.
    ///
    /// # References
    /// RFC 3501 Section 6.4.4 (SEARCH command)
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            keys: Vec::new(),
        }
    }

    /// Return an explicit raw syntax view. Pass the builder itself to preserve
    /// typed operands when calling
    /// `ImapConnection::search`,
    /// `ImapConnection::uid_search`,
    /// `ImapConnection::sort`, and similar methods.
    ///
    /// # References
    /// RFC 3501 Section 6.4.4 (SEARCH command)
    pub fn as_str(&self) -> &str {
        &self.buf
    }

    /// Preserve arbitrary extension grammar, including caller-written literals.
    /// Declared requirements supplement the standard raw grammar scanner.
    pub(crate) fn raw(text: String, requires: Vec<super::Capability>) -> Self {
        Self {
            buf: text.clone(),
            keys: vec![SearchKey::Raw { text, requires }],
        }
    }

    pub(crate) fn keys(&self) -> &[SearchKey] {
        &self.keys
    }

    pub(crate) fn and(mut self, other: Self) -> Self {
        if !other.buf.is_empty() {
            self.sep();
            self.buf.push_str(&other.buf);
            self.keys.extend(other.keys);
        }
        self
    }

    /// Keep an explicit group when compiling account-level Boolean filters.
    pub(crate) fn group(mut self, other: Self) -> Self {
        self.sep();
        self.buf.push('(');
        self.buf.push_str(other.as_str());
        self.buf.push(')');
        self.keys.push(SearchKey::Group(other));
        self
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Append a space separator if the buffer is non-empty.
    fn sep(&mut self) {
        if !self.buf.is_empty() {
            self.buf.push(' ');
        }
    }

    /// Append a bare keyword criterion (e.g. `UNSEEN`, `ALL`).
    fn push_keyword(mut self, keyword: &'static str) -> Self {
        self.sep();
        self.buf.push_str(keyword);
        self.keys.push(SearchKey::Keyword(keyword));
        self
    }

    /// Append a criterion with a date argument in IMAP date format.
    ///
    /// Validates that `date` matches the RFC 3501 Section 9 `date` production:
    /// `date-day "-" date-month "-" date-year` (e.g. `13-Feb-2025`).
    /// The date is written as a bare token (not quoted) per the ABNF.
    ///
    /// Returns an error if the date string does not match the expected format.
    fn push_date(mut self, key: &'static str, date: &str) -> Result<Self, crate::Error> {
        validate_imap_date(date)?;
        self.sep();
        self.buf.push_str(key);
        self.buf.push(' ');
        self.buf.push_str(date);
        self.keys.push(SearchKey::Date {
            key,
            value: date.to_owned(),
        });
        Ok(self)
    }

    /// Append a criterion with a string argument.
    ///
    /// The string value is retained until command encoding per RFC 3501 Section 9
    /// (`astring = 1*ASTRING-CHAR / string`, `string = quoted / literal`).
    /// Backslash and double-quote within the value are escaped per
    /// RFC 3501 Section 9: `quoted-specials = DQUOTE / "\"`.
    ///
    /// Returns an error if the value contains NUL  -  bytes that
    /// cannot appear in an IMAP quoted string (RFC 3501 Section 9).
    fn push_string(mut self, key: &str, value: &str) -> Result<Self, crate::Error> {
        self.sep();
        self.buf.push_str(key);
        self.buf.push(' ');
        append_search_string(&mut self.buf, value)?;
        self.keys.push(SearchKey::String {
            prefix: key.to_owned(),
            value: value.to_owned(),
        });
        Ok(self)
    }

    /// Append a criterion with a numeric argument.
    fn push_number(mut self, key: &'static str, n: u64) -> Self {
        self.sep();
        self.buf.push_str(key);
        self.buf.push(' ');
        self.buf.push_str(&n.to_string());
        self.keys.push(SearchKey::Number { key, value: n });
        self
    }

    // -----------------------------------------------------------------------
    // ALL (RFC 3501 Section 6.4.4)
    // -----------------------------------------------------------------------

    /// ALL -- all messages in the mailbox.
    ///
    /// RFC 3501 Section 6.4.4: "All messages in the mailbox; the default
    /// initial key for `ANDing`."
    #[must_use]
    pub fn all(self) -> Self {
        self.push_keyword("ALL")
    }

    // -----------------------------------------------------------------------
    // Message flag criteria (RFC 3501 Section 6.4.4)
    // -----------------------------------------------------------------------

    /// ANSWERED -- messages with the `\Answered` flag set.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn answered(self) -> Self {
        self.push_keyword("ANSWERED")
    }

    /// DELETED -- messages with the `\Deleted` flag set.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn deleted(self) -> Self {
        self.push_keyword("DELETED")
    }

    /// DRAFT -- messages with the `\Draft` flag set.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn draft(self) -> Self {
        self.push_keyword("DRAFT")
    }

    /// FLAGGED -- messages with the `\Flagged` flag set.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn flagged(self) -> Self {
        self.push_keyword("FLAGGED")
    }

    /// SEEN -- messages with the `\Seen` flag set.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn seen(self) -> Self {
        self.push_keyword("SEEN")
    }

    /// RECENT -- messages with the `\Recent` flag set.
    ///
    /// RFC 3501 Section 6.4.4 (`IMAP4rev1` only; `\Recent` is removed in
    /// RFC 9051 `IMAP4rev2`).
    #[must_use]
    pub fn recent(self) -> Self {
        self.push_keyword("RECENT")
    }

    /// NEW -- equivalent to `(RECENT UNSEEN)`.
    ///
    /// RFC 3501 Section 6.4.4: "Messages that have the \Recent flag set
    /// but not the \Seen flag. This is functionally equivalent to
    /// \"(RECENT UNSEEN)\"."
    #[must_use]
    pub fn new_messages(self) -> Self {
        self.push_keyword("NEW")
    }

    /// OLD -- messages that do not have the `\Recent` flag set.
    ///
    /// RFC 3501 Section 6.4.4: "Messages that do not have the \Recent
    /// flag set. This is functionally equivalent to \"NOT RECENT\"
    /// (as stringuished from \"NOT NEW\")."
    /// (`IMAP4rev1` only; `\Recent` is removed in RFC 9051.)
    #[must_use]
    pub fn old(self) -> Self {
        self.push_keyword("OLD")
    }

    /// UNANSWERED -- messages without the `\Answered` flag.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn unanswered(self) -> Self {
        self.push_keyword("UNANSWERED")
    }

    /// UNDELETED -- messages without the `\Deleted` flag.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn undeleted(self) -> Self {
        self.push_keyword("UNDELETED")
    }

    /// UNDRAFT -- messages without the `\Draft` flag.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn undraft(self) -> Self {
        self.push_keyword("UNDRAFT")
    }

    /// UNFLAGGED -- messages without the `\Flagged` flag.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn unflagged(self) -> Self {
        self.push_keyword("UNFLAGGED")
    }

    /// UNSEEN -- messages without the `\Seen` flag.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4.
    #[must_use]
    pub fn unseen(self) -> Self {
        self.push_keyword("UNSEEN")
    }

    /// KEYWORD -- messages with the specified keyword flag set.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"KEYWORD" SP flag-keyword` where `flag-keyword = atom`
    /// (RFC 3501 Section 9).
    ///
    /// Returns an error if `flag` is not a valid atom (empty, or contains
    /// atom-specials, CTL characters, or non-ASCII bytes).
    pub fn keyword(mut self, flag: &str) -> Result<Self, crate::Error> {
        // RFC 3501 Section 9: flag-keyword = atom.
        validate_atom_bytes(flag.as_bytes(), "KEYWORD flag")?;
        self.sep();
        self.buf.push_str("KEYWORD ");
        self.buf.push_str(flag);
        self.keys.push(SearchKey::Flag {
            excluded: false,
            value: flag.to_owned(),
        });
        Ok(self)
    }

    /// UNKEYWORD -- messages without the specified keyword flag.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"UNKEYWORD" SP flag-keyword` where `flag-keyword = atom`
    /// (RFC 3501 Section 9).
    ///
    /// Returns an error if `flag` is not a valid atom.
    pub fn unkeyword(mut self, flag: &str) -> Result<Self, crate::Error> {
        // RFC 3501 Section 9: flag-keyword = atom.
        validate_atom_bytes(flag.as_bytes(), "UNKEYWORD flag")?;
        self.sep();
        self.buf.push_str("UNKEYWORD ");
        self.buf.push_str(flag);
        self.keys.push(SearchKey::Flag {
            excluded: true,
            value: flag.to_owned(),
        });
        Ok(self)
    }

    // -----------------------------------------------------------------------
    // Header / body criteria (RFC 3501 Section 6.4.4)
    // -----------------------------------------------------------------------

    /// BCC -- messages whose BCC field contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"BCC" SP astring`.
    ///
    /// Returns an error if `s` contains NUL (RFC 3501 Section 9).
    pub fn bcc(self, s: &str) -> Result<Self, crate::Error> {
        self.push_string("BCC", s)
    }

    /// CC -- messages whose CC field contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"CC" SP astring`.
    ///
    /// Returns an error if `s` contains NUL (RFC 3501 Section 9).
    pub fn cc(self, s: &str) -> Result<Self, crate::Error> {
        self.push_string("CC", s)
    }

    /// FROM -- messages whose From field contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"FROM" SP astring`.
    ///
    /// Returns an error if `s` contains NUL (RFC 3501 Section 9).
    pub fn from(self, s: &str) -> Result<Self, crate::Error> {
        self.push_string("FROM", s)
    }

    /// TO -- messages whose To field contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"TO" SP astring`.
    ///
    /// Returns an error if `s` contains NUL (RFC 3501 Section 9).
    pub fn to(self, s: &str) -> Result<Self, crate::Error> {
        self.push_string("TO", s)
    }

    /// SUBJECT -- messages whose Subject field contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"SUBJECT" SP astring`.
    ///
    /// Returns an error if `s` contains NUL (RFC 3501 Section 9).
    pub fn subject(self, s: &str) -> Result<Self, crate::Error> {
        self.push_string("SUBJECT", s)
    }

    /// HEADER -- messages that have the named header field and whose value
    /// contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"HEADER" SP header-fld-name SP astring`.
    ///
    /// Returns an error if `name` is not an IMAP atom or `value` contains
    /// NUL (RFC 3501 Section 9).
    pub fn header(mut self, name: &str, value: &str) -> Result<Self, crate::Error> {
        validate_atom_bytes(name.as_bytes(), "HEADER field name")?;
        self.sep();
        self.buf.push_str("HEADER ");
        self.buf.push_str(name);
        self.buf.push(' ');
        append_search_string(&mut self.buf, value)?;
        self.keys.push(SearchKey::String {
            prefix: format!("HEADER {name}"),
            value: value.to_owned(),
        });
        Ok(self)
    }

    /// BODY -- messages whose body contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"BODY" SP astring`.
    ///
    /// Returns an error if `s` contains NUL (RFC 3501 Section 9).
    pub fn body(self, s: &str) -> Result<Self, crate::Error> {
        self.push_string("BODY", s)
    }

    /// TEXT -- messages whose header or body contains the specified string.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"TEXT" SP astring`.
    ///
    /// Returns an error if `s` contains NUL (RFC 3501 Section 9).
    pub fn text(self, s: &str) -> Result<Self, crate::Error> {
        self.push_string("TEXT", s)
    }

    // -----------------------------------------------------------------------
    // Date criteria (RFC 3501 Section 6.4.4)
    // -----------------------------------------------------------------------

    /// BEFORE -- messages whose internal date is earlier than the given date.
    ///
    /// `date` must be in IMAP date format: `DD-Mon-YYYY` (e.g. `13-Feb-2025`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"BEFORE" SP date`.
    ///
    /// Returns an error if `date` does not match the IMAP `date` production
    /// (RFC 3501 Section 9).
    pub fn before(self, date: &str) -> Result<Self, crate::Error> {
        self.push_date("BEFORE", date)
    }

    /// ON -- messages whose internal date is the given date.
    ///
    /// `date` must be in IMAP date format: `DD-Mon-YYYY` (e.g. `13-Feb-2025`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"ON" SP date`.
    ///
    /// Returns an error if `date` does not match the IMAP `date` production
    /// (RFC 3501 Section 9).
    pub fn on(self, date: &str) -> Result<Self, crate::Error> {
        self.push_date("ON", date)
    }

    /// SINCE -- messages whose internal date is on or after the given date.
    ///
    /// `date` must be in IMAP date format: `DD-Mon-YYYY` (e.g. `13-Feb-2025`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"SINCE" SP date`.
    ///
    /// Returns an error if `date` does not match the IMAP `date` production
    /// (RFC 3501 Section 9).
    pub fn since(self, date: &str) -> Result<Self, crate::Error> {
        self.push_date("SINCE", date)
    }

    /// SENTBEFORE -- messages whose Date: header is earlier than the given date.
    ///
    /// `date` must be in IMAP date format: `DD-Mon-YYYY` (e.g. `13-Feb-2025`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"SENTBEFORE" SP date`.
    ///
    /// Returns an error if `date` does not match the IMAP `date` production
    /// (RFC 3501 Section 9).
    pub fn sent_before(self, date: &str) -> Result<Self, crate::Error> {
        self.push_date("SENTBEFORE", date)
    }

    /// SENTON -- messages whose Date: header is the given date.
    ///
    /// `date` must be in IMAP date format: `DD-Mon-YYYY` (e.g. `13-Feb-2025`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"SENTON" SP date`.
    ///
    /// Returns an error if `date` does not match the IMAP `date` production
    /// (RFC 3501 Section 9).
    pub fn sent_on(self, date: &str) -> Result<Self, crate::Error> {
        self.push_date("SENTON", date)
    }

    /// SENTSINCE -- messages whose Date: header is on or after the given date.
    ///
    /// `date` must be in IMAP date format: `DD-Mon-YYYY` (e.g. `13-Feb-2025`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"SENTSINCE" SP date`.
    ///
    /// Returns an error if `date` does not match the IMAP `date` production
    /// (RFC 3501 Section 9).
    pub fn sent_since(self, date: &str) -> Result<Self, crate::Error> {
        self.push_date("SENTSINCE", date)
    }

    // -----------------------------------------------------------------------
    // Size criteria (RFC 3501 Section 6.4.4)
    // -----------------------------------------------------------------------

    /// LARGER -- messages with an RFC 2822 size larger than the given number.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"LARGER" SP number`.
    #[must_use]
    pub fn larger(self, n: u64) -> Self {
        self.push_number("LARGER", n)
    }

    /// SMALLER -- messages with an RFC 2822 size smaller than the given number.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"SMALLER" SP number`.
    #[must_use]
    pub fn smaller(self, n: u64) -> Self {
        self.push_number("SMALLER", n)
    }

    // -----------------------------------------------------------------------
    // Sequence / UID criteria (RFC 3501 Section 6.4.4)
    // -----------------------------------------------------------------------

    /// UID -- messages with UIDs in the given set.
    ///
    /// `set` is a sequence-set string (e.g. `"1:*"`, `"1,2,5:10"`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"UID" SP sequence-set`.
    ///
    /// Returns an error if `set` is not a valid sequence-set per
    /// RFC 3501 Section 9.
    pub fn uid(mut self, set: &str) -> Result<Self, crate::Error> {
        // RFC 3501 Section 9: validate sequence-set ABNF.
        if !is_valid_sequence_set(set) {
            return Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    format!("invalid sequence-set for UID search per RFC 3501 Section 9: {set:?}")
                        .into(),
                ),
            ));
        }
        self.sep();
        self.buf.push_str("UID ");
        self.buf.push_str(set);
        self.keys.push(SearchKey::Uid(set.to_owned()));
        Ok(self)
    }

    /// Messages with sequence numbers in the given set.
    ///
    /// `set` is a sequence-set string (e.g. `"1:100"`, `"1,5,10:*"`).
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `sequence-set` (bare, without a keyword prefix).
    ///
    /// Returns an error if `set` is not a valid sequence-set per
    /// RFC 3501 Section 9.
    pub fn sequence(mut self, set: &str) -> Result<Self, crate::Error> {
        // RFC 3501 Section 9: validate sequence-set ABNF.
        if !is_valid_sequence_set(set) {
            return Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    format!("invalid sequence-set per RFC 3501 Section 9: {set:?}").into(),
                ),
            ));
        }
        self.sep();
        self.buf.push_str(set);
        self.keys.push(SearchKey::Sequence(set.to_owned()));
        Ok(self)
    }

    // -----------------------------------------------------------------------
    // CONDSTORE extension (RFC 7162 Section 3.1.5)
    // -----------------------------------------------------------------------

    /// MODSEQ -- messages whose mod-sequence value is equal to or greater
    /// than the given value.
    ///
    /// This is the simple form without entry-name/entry-type qualifiers.
    ///
    /// RFC 7162 Section 3.1.5:
    /// `"MODSEQ" [entry-name entry-type-req] SP mod-sequence-valzer`
    ///
    /// `mod-sequence-valzer` is a 63-bit unsigned integer (0 to 2^63-1),
    /// and values outside that range return an invalid-input error locally.
    ///
    /// Requires [`Capability::Condstore`](crate::types::Capability::Condstore).
    pub fn mod_seq(self, value: u64) -> Result<Self, crate::Error> {
        let value = super::ModSeqValzer::new(value).ok_or_else(|| {
            crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                "SEARCH MODSEQ exceeds the 63-bit protocol ceiling".into(),
            ))
        })?;
        Ok(self.push_number("MODSEQ", value.get()))
    }

    // -----------------------------------------------------------------------
    // Combinators (RFC 3501 Section 6.4.4)
    // -----------------------------------------------------------------------

    /// NOT -- negate a criteria expression.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"NOT" SP search-key`. The inner criteria are parenthesized when
    /// they contain multiple keys.
    #[must_use]
    pub fn not(mut self, criteria: &Self) -> Self {
        self.sep();
        self.buf.push_str("NOT ");
        push_parenthesized_if_compound(&mut self.buf, criteria.as_str());
        self.keys.push(SearchKey::Not(Box::new(criteria.clone())));
        self
    }

    /// OR -- disjunction of two criteria expressions.
    ///
    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4:
    /// `"OR" SP search-key SP search-key`. Each operand is parenthesized
    /// when it contains multiple keys.
    #[must_use]
    pub fn or(mut self, a: &Self, b: &Self) -> Self {
        self.sep();
        self.buf.push_str("OR ");
        push_parenthesized_if_compound(&mut self.buf, a.as_str());
        self.buf.push(' ');
        push_parenthesized_if_compound(&mut self.buf, b.as_str());
        self.keys
            .push(SearchKey::Or(Box::new(a.clone()), Box::new(b.clone())));
        self
    }
}

impl Default for SearchCriteria {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SearchCriteria {
    /// Formats the search criteria as the IMAP wire string
    /// (RFC 3501 Section 6.4.4).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.buf)
    }
}

impl AsRef<str> for SearchCriteria {
    fn as_ref(&self) -> &str {
        &self.buf
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;

impl From<String> for SearchCriteria {
    fn from(text: String) -> Self {
        Self::raw(text, Vec::new())
    }
}
impl From<&str> for SearchCriteria {
    fn from(text: &str) -> Self {
        text.to_owned().into()
    }
}
impl From<&String> for SearchCriteria {
    fn from(text: &String) -> Self {
        text.as_str().into()
    }
}
impl From<&SearchCriteria> for SearchCriteria {
    fn from(criteria: &SearchCriteria) -> Self {
        criteria.clone()
    }
}
impl std::ops::Deref for SearchCriteria {
    type Target = str;
    fn deref(&self) -> &str {
        &self.buf
    }
}
impl PartialEq<str> for SearchCriteria {
    fn eq(&self, other: &str) -> bool {
        self.buf == other
    }
}
impl PartialEq<&str> for SearchCriteria {
    fn eq(&self, other: &&str) -> bool {
        self.buf == *other
    }
}

/// Result of a SEARCH or UID SEARCH command.
///
/// Contains both the matching sequence numbers/UIDs and the optional
/// highest mod-sequence value (RFC 7162 Section 3.1.5).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SearchResult<I = super::Uid> {
    addressing: std::marker::PhantomData<I>,
    /// Numeric projection for the account identity boundary. The result's
    /// `I` retains whether these observations identify UIDs or sequences.
    pub(crate) ids: Vec<u32>,
    /// Highest mod-sequence of matching messages, if MODSEQ was used
    /// in the search criteria (RFC 7162 Section 3.1.5).
    pub(crate) mod_seq: Option<crate::types::ModSeq>,
}

impl<I> SearchResult<I> {
    pub(crate) fn new(ids: Vec<u32>, mod_seq: Option<super::ModSeq>) -> Self {
        Self {
            ids,
            mod_seq,
            addressing: std::marker::PhantomData,
        }
    }
}

impl<I> Default for SearchResult<I> {
    fn default() -> Self {
        Self::new(Vec::new(), None)
    }
}

/// Addressing supplied by the command for legacy SEARCH/SORT responses, whose
/// wire syntax contains no unit. ESEARCH supplies its own unit and must agree.
pub(crate) trait SearchAddress: Send + 'static {
    const UID: bool;
}
impl SearchAddress for super::Uid {
    const UID: bool = true;
}
impl SearchAddress for super::Seq {
    const UID: bool = false;
}
