//! Validated newtypes for IMAP protocol strings.
//!
//! Caller constructors enforce syntactic validity. Server object-id
//! observations preserve the decoder's established atom tolerance; they are
//! distinct from admitting that spelling into a caller's command operand.
//!
//! - [`SequenceSet`]  -  RFC 3501 Section 9 / RFC 9051 Section 9
//! - [`ImapAtom`]  -  RFC 3501 Section 9 / RFC 9051 Section 9
//! - [`MailboxName`]  -  RFC 3501 Section 5.1 / RFC 9051 Section 5.1
//! - [`ObjectId`]  -  RFC 8474 Section 4

use std::fmt;

/// Error returned when constructing a validated IMAP protocol type from an
/// invalid string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ValidationError {
    pub(crate) reason: ValidationReason,
    message: String,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationReason {
    InvalidSequenceSet,
    WildcardInKnownSet,
    SearchResultInKnownSet,
    EmptyAtom,
    InvalidAtomByte(u8),
    MailboxControlCharacter,
    InvalidMailboxIdentity,
    NonAsciiHierarchyDelimiter,
    EmptyObjectId,
    ObjectIdTooLong { length: usize, limit: usize },
    InvalidObjectIdByte(u8),
    ZeroIdentifier,
}

impl ValidationError {
    /// Create a new validation error with the given message.
    pub fn new(reason: ValidationReason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }

    /// Return the error message as a string slice.
    pub fn message(&self) -> &str {
        &self.message
    }
}

// ---------------------------------------------------------------------------
// SequenceSet  -  RFC 3501 Section 9
// ---------------------------------------------------------------------------

/// A validated IMAP sequence-set expression addressed by `I` (UID by default).
/// The unit survives borrowing the expression from a typed wrapper, so a
/// sequence-number operand cannot be passed to a UID command.
///
/// RFC 3501 Section 9 formal syntax:
/// ```text
/// sequence-set    = (seq-number / seq-range) *("," sequence-set)
/// seq-number      = nz-number / "*"
/// seq-range       = seq-number ":" seq-number
/// nz-number       = digit-nz *DIGIT      ; non-zero unsigned 32-bit integer
/// ```
///
/// RFC 5182 Section 5 extends the grammar with `"$"` as a standalone
/// element referencing saved search results.
///
/// Construction validates the full ABNF and rejects empty, malformed,
/// or overflowing values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SequenceSet<I = super::Uid> {
    addressing: std::marker::PhantomData<I>,
    wire: String,
    members: Vec<SetMember>,
}

/// Saved-result operands are deliberately separate from finite membership.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
enum SetMember {
    Range(super::UidRange),
    SavedResult,
}

impl<I> SequenceSet<I> {
    fn from_validated(wire: String) -> Self {
        let members = wire
            .split(',')
            .map(|part| {
                if part == "$" {
                    return SetMember::SavedResult;
                }
                let bound = |s: &str| {
                    if s == "*" {
                        super::SeqBound::Star
                    } else {
                        super::SeqBound::Number(s.parse().expect("sequence-set grammar validated"))
                    }
                };
                let (start, end) = match part.split_once(':') {
                    Some((a, b)) => (bound(a), Some(bound(b))),
                    None => (bound(part), None),
                };
                SetMember::Range(super::UidRange::from_sequence_bounds(start, end))
            })
            .collect();
        Self {
            wire,
            members,
            addressing: std::marker::PhantomData,
        }
    }

    pub(crate) fn from_finite(ranges: Vec<super::UidRange>) -> Self {
        assert!(!ranges.is_empty() && ranges.iter().all(super::UidRange::is_numeric));
        let wire = ranges
            .iter()
            .map(|r| match r.end() {
                Some(end) => format!("{}:{end}", r.start()),
                None => r.start().to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        Self {
            addressing: std::marker::PhantomData,
            wire,
            members: ranges.into_iter().map(SetMember::Range).collect(),
        }
    }

    pub(crate) fn ranges(&self) -> Option<Vec<super::UidRange>> {
        self.members
            .iter()
            .map(|member| match member {
                SetMember::Range(range) => Some(*range),
                SetMember::SavedResult => None,
            })
            .collect()
    }

    pub(crate) fn is_finite(&self) -> bool {
        self.members
            .iter()
            .all(|member| matches!(member, SetMember::Range(r) if r.is_numeric()))
    }

    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.wire
    }

    /// Create a general sequence set (allows `*` and `$`).
    ///
    /// Validates the input against RFC 3501 Section 9 `sequence-set` ABNF,
    /// extended by RFC 5182 Section 5 for `$`.
    pub fn new(s: impl Into<String>) -> Result<Self, ValidationError> {
        let s = s.into();
        if !is_valid_sequence_set(&s) {
            return Err(ValidationError::new(
                ValidationReason::InvalidSequenceSet,
                format!("invalid sequence set per RFC 3501 Section 9: {s:?}"),
            ));
        }
        Ok(Self::from_validated(s))
    }

    /// Create a "known" sequence set that rejects `*` and `$`.
    ///
    /// Used for QRESYNC `known-uids` and `seq-match-data` where the
    /// wildcard `*` is explicitly disallowed (RFC 7162 Section 3.2.5.2)
    /// and `$` (RFC 5182 search result reference) is not meaningful.
    pub fn new_known(s: impl Into<String>) -> Result<Self, ValidationError> {
        let s = s.into();
        // RFC 7162 Section 3.2.5.2: "*" is not allowed in known-uids/sequence-set.
        if s.contains('*') {
            return Err(ValidationError::new(
                ValidationReason::WildcardInKnownSet,
                "\"*\" is not allowed in QRESYNC known-uids/sequence-set \
                 (RFC 7162 Section 3.2.5.2)",
            ));
        }
        // RFC 5182 search result reference is not valid in QRESYNC context.
        if s.contains('$') {
            return Err(ValidationError::new(
                ValidationReason::SearchResultInKnownSet,
                "\"$\" (search result reference) is not allowed in QRESYNC \
                 known-uids/sequence-set (RFC 7162 Section 7)",
            ));
        }
        // Validate full sequence-set ABNF (since * and $ are already excluded,
        // this validates nz-number ranges only).
        if !is_valid_sequence_set(&s) {
            return Err(ValidationError::new(
                ValidationReason::InvalidSequenceSet,
                "QRESYNC known-uids/sequence-set is not a valid sequence-set \
                 (RFC 7162 Section 7: known-uids = sequence-set)",
            ));
        }
        Ok(Self::from_validated(s))
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.wire
    }
}

impl<I> AsRef<str> for SequenceSet<I> {
    fn as_ref(&self) -> &str {
        &self.wire
    }
}

impl<I> fmt::Display for SequenceSet<I> {
    /// Formats the sequence set as its wire representation
    /// (RFC 3501 Section 9).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.wire)
    }
}

impl<I> TryFrom<String> for SequenceSet<I> {
    type Error = ValidationError;

    /// Convert from `String`, validating per RFC 3501 Section 9.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl<I> TryFrom<&str> for SequenceSet<I> {
    type Error = ValidationError;

    /// Convert from `&str`, validating per RFC 3501 Section 9.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl<I> From<SequenceSet<I>> for String {
    fn from(s: SequenceSet<I>) -> Self {
        s.into_inner()
    }
}

/// Validates that `s` matches the RFC 3501 Section 9 `sequence-set` ABNF:
///
/// ```text
/// sequence-set = (seq-number / seq-range) *("," sequence-set)
/// seq-number   = nz-number / "*"
/// seq-range    = seq-number ":" seq-number
/// nz-number    = digit-nz *DIGIT
/// ```
///
/// RFC 5182 Section 5 extends the grammar: `sequence-set =/ seq-last-command`,
/// where `seq-last-command = "$"`.
pub(crate) fn is_valid_sequence_set(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    s.split(',').all(|part| {
        if part.is_empty() {
            return false;
        }
        // RFC 5182 Section 2: "$" references saved search results.
        if part == "$" {
            return true;
        }
        part.split(':').all(|num| {
            if num == "*" {
                return true;
            }
            // Must be non-empty, all digits, and not start with '0'.
            if num.is_empty() || num.starts_with('0') || !num.chars().all(|c| c.is_ascii_digit()) {
                return false;
            }
            // RFC 3501 Section 9: nz-number is a u32 (0 < n < 4,294,967,296).
            num.parse::<u32>().is_ok()
        }) && part.matches(':').count() <= 1
    })
}

// ---------------------------------------------------------------------------
// ImapAtom  -  RFC 3501 Section 9
// ---------------------------------------------------------------------------

/// A validated IMAP atom string.
///
/// RFC 3501 Section 9 / RFC 9051 Section 9:
/// ```text
/// atom            = 1*ATOM-CHAR
/// ATOM-CHAR       = <any CHAR except atom-specials>
/// atom-specials   = "(" / ")" / "{" / SP / CTL / list-wildcards
///                 / quoted-specials / resp-specials
/// list-wildcards  = "%" / "*"
/// quoted-specials = DQUOTE / "\"
/// resp-specials   = "]"
/// ```
///
/// Used for flag keywords (`flag-keyword = atom`, RFC 3501 Section 9)
/// and other protocol atoms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImapAtom(String);

impl ImapAtom {
    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Create a validated atom.
    ///
    /// Returns an error if `s` is empty or contains any byte that is not
    /// an ATOM-CHAR per RFC 3501 Section 9.
    pub fn new(s: impl Into<String>) -> Result<Self, ValidationError> {
        let s = s.into();
        validate_atom_bytes(s.as_bytes(), "atom")?;
        Ok(Self(s))
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ImapAtom {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ImapAtom {
    /// Formats the atom as its wire representation (RFC 3501 Section 9).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ImapAtom {
    type Error = ValidationError;

    /// Convert from `String`, validating per RFC 3501 Section 9.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<&str> for ImapAtom {
    type Error = ValidationError;

    /// Convert from `&str`, validating per RFC 3501 Section 9.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<ImapAtom> for String {
    fn from(a: ImapAtom) -> Self {
        a.into_inner()
    }
}

/// Core atom validation logic shared by [`ImapAtom::new`] and the encoder's
/// `validate_atom` helper.
///
/// RFC 3501 Section 9: `atom = 1*ATOM-CHAR`,
/// `ATOM-CHAR = <any CHAR except atom-specials>`,
/// `atom-specials = "(" / ")" / "{" / SP / CTL / list-wildcards / quoted-specials / resp-specials`,
/// `list-wildcards = "%" / "*"`, `quoted-specials = DQUOTE / "\"`,
/// `resp-specials = "]"`.
pub(crate) fn validate_atom_bytes(bytes: &[u8], context: &str) -> Result<(), ValidationError> {
    if bytes.is_empty() {
        return Err(ValidationError::new(
            ValidationReason::EmptyAtom,
            format!(
                "{context} must be at least one character \
             (RFC 3501 Section 9: atom = 1*ATOM-CHAR)"
            ),
        ));
    }
    for &b in bytes {
        let is_atom_special = matches!(
            b,
            b'(' | b')' | b'{' | b' ' | b'%' | b'*' | b'"' | b'\\' | b']'
        );
        let is_ctl = b < 0x20 || b == 0x7F;
        let is_outside_char = b == 0 || b > 0x7F;
        if is_atom_special || is_ctl || is_outside_char {
            return Err(ValidationError::new(
                ValidationReason::InvalidAtomByte(b),
                format!(
                    "{context} contains invalid byte 0x{b:02X}  -  must be an atom \
                 (RFC 3501 Section 9: ATOM-CHAR excludes atom-specials, CTL, non-ASCII)"
                ),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// MailboxName  -  RFC 3501 Section 5.1
// ---------------------------------------------------------------------------

/// A mailbox name string with minimal validation.
///
/// RFC 3501 Section 5.1 / RFC 9051 Section 5.1: IMAP mailbox names can
/// contain almost any character (transmitted as quoted strings or literals).
/// The type separates the server address from its lenient display decoding.
/// IMAP strings forbid NUL (RFC 3501 Section 9 / RFC 9051 Section 9).
/// Caller input also excludes CRLF; server CRLF is framed as a literal.
///
/// The empty string `""` is allowed as a special case for server-level
/// METADATA queries (RFC 5464 Section 4.2) and LIST reference names
/// (RFC 3501 Section 6.3.8).
///
/// Validation, on the [`MailboxName::new`] path only:
/// - No NUL, CR, or LF bytes
///
/// # Parse-don't-validate discipline
///
/// `MailboxName` has caller and codec construction paths: [`MailboxName::new`]
/// (public, validating) and `from_wire` / `from_decoded` (crate-private, **not**
/// validating). `from_wire` retains the original address; `from_decoded`
/// retains only display text. A parsed name's display form is valid UTF-8; do not assume the NUL / CR /
/// LF exclusion for names you did not construct yourself. Injection safety on
/// the wire comes from the encoder, not from this type.
///
/// There is no `From<String>` or `From<&str>`: callers must use validating
/// construction. In-crate compile-time assertions pin both exclusions against
/// this internal type; external doctests cannot name it through the private
/// protocol module.
#[derive(Debug, Clone)]
pub struct MailboxName(String, MailboxWire, (bool, Vec<u8>));

#[derive(Debug, Clone)]
enum MailboxWire {
    Canonical,
    Raw {
        bytes: Vec<u8>,
        mode: crate::types::wire::WireMode,
        canonical: bool,
    },
}

#[cfg(test)]
mod mailbox_conversion_contract {
    use super::MailboxName;

    // If the forbidden conversion exists, both implementations apply and the
    // inferred marker becomes ambiguous. Unlike an external compile-fail
    // example, this assertion must resolve the actual internal type first.
    #[test]
    fn mailbox_name_has_no_infallible_string_conversion() {
        trait AmbiguousIfFrom<A> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfFrom<()> for T {}
        impl<T: From<String>> AmbiguousIfFrom<u8> for T {}
        let _ = <MailboxName as AmbiguousIfFrom<_>>::check;
    }

    #[test]
    fn mailbox_name_has_no_infallible_str_conversion() {
        trait AmbiguousIfFrom<A> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfFrom<()> for T {}
        impl<T: From<&'static str>> AmbiguousIfFrom<u8> for T {}
        let _ = <MailboxName as AmbiguousIfFrom<_>>::check;
    }
}

impl MailboxName {
    /// Create a validated mailbox name.
    ///
    /// Rejects strings containing NUL (forbidden in IMAP strings by
    /// RFC 3501 Section 9 / RFC 9051 Section 9) or CR/LF (CRLF injection
    /// prevention per RFC 3501 Section 2.2 / RFC 9051 Section 2.2).
    /// The empty string is allowed for server-level METADATA queries
    /// (RFC 5464 Section 4.2).
    pub fn new(s: impl Into<String>) -> Result<Self, ValidationError> {
        let s = s.into();
        if s.bytes().any(|b| matches!(b, b'\0' | b'\r' | b'\n')) {
            return Err(ValidationError::new(
                ValidationReason::MailboxControlCharacter,
                "mailbox name must not contain NUL, CR, or LF  -  IMAP strings \
                 forbid NUL (RFC 3501 Section 9 / RFC 9051 Section 9) and \
                 commands are CRLF-delimited (RFC 3501 Section 2.2 / \
                 RFC 9051 Section 2.2)",
            ));
        }
        Ok(Self::from_decoded(s))
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Retain the address separately from the lenient display decoding.
    pub(crate) fn from_wire(
        s: String,
        bytes: &[u8],
        utf8: impl Into<crate::types::wire::WireMode>,
    ) -> Self {
        let mode = utf8.into();
        let (decoded, canonical) = mode.decode(bytes);
        // A mismatched display must never authorize a mode conversion.
        let canonical = canonical && decoded == s;
        Self::from_wire_decoded(s, bytes, mode, canonical)
    }

    pub(crate) fn from_wire_decoded(
        s: String,
        bytes: &[u8],
        mode: crate::types::wire::WireMode,
        canonical: bool,
    ) -> Self {
        let utf8 = mode == crate::types::wire::WireMode::Utf8;
        let identity = if canonical {
            Self::canonical_identity(&s)
        } else {
            (utf8 && bytes != s.as_bytes(), bytes.to_vec())
        };
        Self(
            s,
            MailboxWire::Raw {
                bytes: bytes.to_vec(),
                mode,
                canonical,
            },
            identity,
        )
    }

    /// Original octets apply only in the mode in which they were received.
    pub(crate) fn wire_bytes(
        &self,
        utf8: impl Into<crate::types::wire::WireMode>,
    ) -> Option<&[u8]> {
        let wire_mode = utf8.into();
        match &self.1 {
            MailboxWire::Raw { bytes, mode, .. } if *mode == wire_mode => Some(bytes),
            _ => None,
        }
    }

    fn canonical_identity(display: &str) -> (bool, Vec<u8>) {
        (
            false,
            crate::types::wire::WireMode::ModifiedUtf7
                .encode(display)
                .into_bytes(),
        )
    }

    // Construction pays the conversion cost once; registry probes only borrow.
    pub(crate) fn identity(&self) -> &(bool, Vec<u8>) {
        &self.2
    }

    pub(crate) fn child(&self, delimiter: char, leaf: &str) -> Result<Self, ValidationError> {
        let display = Self::new(format!("{}{delimiter}{leaf}", self.0))?;
        Self::new(leaf)?;
        if let MailboxWire::Raw { bytes, mode, .. } = &self.1 {
            // Hierarchy syntax is meaningful in the wire address, not in a
            // lenient display decoding. Never reconstruct a noncanonical parent.
            if !delimiter.is_ascii() && !self.can_reencode() {
                return Err(ValidationError::new(
                    ValidationReason::NonAsciiHierarchyDelimiter,
                    "noncanonical mailbox requires an ASCII hierarchy delimiter",
                ));
            }
            if delimiter.is_ascii() {
                let mut raw = bytes.clone();
                raw.extend_from_slice(delimiter.to_string().as_bytes());
                let leaf = mode.encode_component(leaf);
                raw.extend_from_slice(leaf.as_bytes());
                return Ok(Self::from_wire(display.0, &raw, *mode));
            }
        }
        Ok(display)
    }

    pub(crate) fn parent(&self, delimiter: char) -> Result<Option<Self>, ValidationError> {
        if let MailboxWire::Raw { bytes, mode, .. } = &self.1 {
            if !delimiter.is_ascii() && !self.can_reencode() {
                return Err(ValidationError::new(
                    ValidationReason::NonAsciiHierarchyDelimiter,
                    "noncanonical mailbox requires an ASCII hierarchy delimiter",
                ));
            }
            if delimiter.is_ascii() {
                let separator = delimiter.to_string();
                let Some(index) = bytes
                    .iter()
                    .rposition(|byte| *byte == separator.as_bytes()[0])
                else {
                    return Ok(None);
                };
                let raw = &bytes[..index];
                let (display, _) = mode.decode(raw);
                return Ok(Some(Self::from_wire(display, raw, *mode)));
            }
        }
        self.0
            .rsplit_once(delimiter)
            .map(|(parent, _)| Self::new(parent))
            .transpose()
    }

    pub(crate) fn can_reencode(&self) -> bool {
        matches!(
            self.1,
            MailboxWire::Canonical
                | MailboxWire::Raw {
                    canonical: true,
                    ..
                }
        )
    }

    /// Command correlation uses the same identity as registry keys and hashing.
    pub(crate) fn matches_mailbox(&self, other: &Self) -> bool {
        self == other
    }

    /// Construct from already-decoded bytes produced by the codec's decoder.
    /// Callers must guarantee the input has already been MUTF-7 decoded (or
    /// passed through in UTF-8 mode per RFC 6855).
    ///
    /// This display-only constructor bypasses validation. Without wire octets,
    /// it cannot recover a noncanonical server spelling; the parser uses
    /// `from_wire` instead. CR/LF are safely framed by the command writer.
    pub(crate) fn from_decoded(s: String) -> Self {
        let identity = Self::canonical_identity(&s);
        Self(s, MailboxWire::Canonical, identity)
    }
}

/// Borrowed input for mailbox commands. Typed names retain their wire address;
/// caller text still passes the normal validation boundary.
pub(crate) trait MailboxInput: Sync {
    fn mailbox_name(&self) -> Result<MailboxName, ValidationError>;
}
impl MailboxInput for str {
    fn mailbox_name(&self) -> Result<MailboxName, ValidationError> {
        MailboxName::new(self)
    }
}
impl MailboxInput for String {
    fn mailbox_name(&self) -> Result<MailboxName, ValidationError> {
        MailboxName::new(self.clone())
    }
}
impl MailboxInput for MailboxName {
    fn mailbox_name(&self) -> Result<MailboxName, ValidationError> {
        Ok(self.clone())
    }
}
impl<T: MailboxInput + ?Sized> MailboxInput for &T {
    fn mailbox_name(&self) -> Result<MailboxName, ValidationError> {
        T::mailbox_name(*self)
    }
}

/// Command arguments retain caller text until the encoder invokes the same
/// validating constructor as mailbox command helpers. Server addresses stay typed.
#[derive(Debug, Clone)]
pub(crate) enum MailboxArgument {
    Text(String),
    Name(MailboxName),
}

impl MailboxArgument {
    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Text(text) => text,
            Self::Name(name) => name.as_str(),
        }
    }

    fn identity(&self) -> std::borrow::Cow<'_, (bool, Vec<u8>)> {
        match self {
            Self::Text(text) => std::borrow::Cow::Owned(MailboxName::canonical_identity(text)),
            Self::Name(name) => std::borrow::Cow::Borrowed(name.identity()),
        }
    }
}

impl PartialEq for MailboxArgument {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}
impl Eq for MailboxArgument {}
impl std::hash::Hash for MailboxArgument {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.identity().as_ref(), state);
    }
}

impl From<String> for MailboxArgument {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}
impl From<&str> for MailboxArgument {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}
impl From<MailboxName> for MailboxArgument {
    fn from(value: MailboxName) -> Self {
        Self::Name(value)
    }
}
impl From<&MailboxName> for MailboxArgument {
    fn from(value: &MailboxName) -> Self {
        Self::Name(value.clone())
    }
}

impl PartialEq for MailboxName {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for MailboxName {}

impl std::hash::Hash for MailboxName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.identity(), state);
    }
}

impl Default for MailboxName {
    /// Returns an empty mailbox name.
    ///
    /// The empty string is valid per RFC 5464 Section 4.2 (server-level
    /// METADATA) and RFC 3501 Section 6.3.8 (LIST reference name).
    fn default() -> Self {
        Self::from_decoded(String::new())
    }
}

impl AsRef<str> for MailboxName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MailboxName {
    /// Formats the mailbox name (RFC 3501 Section 5.1).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for MailboxName {
    type Error = ValidationError;

    /// Convert from `String`, validating per RFC 3501 Section 5.1.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<MailboxName> for String {
    fn from(m: MailboxName) -> Self {
        m.0
    }
}

// ---------------------------------------------------------------------------
// ObjectId  -  RFC 8474 Section 4
// ---------------------------------------------------------------------------

/// An IMAP object identifier (MAILBOXID, EMAILID, THREADID).
///
/// RFC 8474 Section 4:
/// ```text
/// objectid = 1*255(ALPHA / DIGIT / "_" / "-")
/// ```
///
/// Used for `MAILBOXID`, `EMAILID`, and `THREADID` values.
// protocol-specific: RFC 8474 objectid is validated IMAP metadata, not bifrost_types::ObjectId.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectId(String);

impl ObjectId {
    /// Preserve a server atom even when it exceeds RFC 8474's stricter grammar.
    /// Observation tolerance does not admit that spelling as a command operand.
    pub(crate) fn from_observed(value: String) -> Self {
        Self(value)
    }

    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Create a validated object identifier.
    ///
    /// RFC 8474 Section 4: 1-255 characters, each alphanumeric, dash, or
    /// underscore.
    pub fn new(s: impl Into<String>) -> Result<Self, ValidationError> {
        let s = s.into();
        if s.is_empty() {
            return Err(ValidationError::new(
                ValidationReason::EmptyObjectId,
                "object identifier must not be empty (RFC 8474 Section 4)",
            ));
        }
        if s.len() > 255 {
            return Err(ValidationError::new(
                ValidationReason::ObjectIdTooLong {
                    length: s.len(),
                    limit: 255,
                },
                format!(
                    "object identifier exceeds 255 characters ({} bytes) \
                 (RFC 8474 Section 4: objectid = 1*255(...))",
                    s.len()
                ),
            ));
        }
        for &b in s.as_bytes() {
            let valid = b.is_ascii_alphanumeric() || b == b'-' || b == b'_';
            if !valid {
                return Err(ValidationError::new(
                    ValidationReason::InvalidObjectIdByte(b),
                    format!(
                        "object identifier contains invalid byte 0x{b:02X}  -  \
                     only ALPHA / DIGIT / \"_\" / \"-\" are allowed \
                     (RFC 8474 Section 4)"
                    ),
                ));
            }
        }
        Ok(Self(s))
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for ObjectId {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for ObjectId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectId {
    /// Formats the object identifier (RFC 8474 Section 4).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ObjectId {
    type Error = ValidationError;

    /// Convert from `String`, validating per RFC 8474 Section 4.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<&str> for ObjectId {
    type Error = ValidationError;

    /// Convert from `&str`, validating per RFC 8474 Section 4.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<ObjectId> for String {
    fn from(o: ObjectId) -> Self {
        o.into_inner()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "validated_tests.rs"]
mod tests;
