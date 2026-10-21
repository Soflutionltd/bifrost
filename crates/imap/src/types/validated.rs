//! Validated newtypes for IMAP protocol strings.
//!
//! These types enforce syntactic validity at construction time so that
//! downstream code (the encoder, the connection layer) can rely on
//! well-formedness without re-validating.
//!
//! - [`SequenceSet`]  -  RFC 3501 Section 9 / RFC 9051 Section 9
//! - [`ImapAtom`]  -  RFC 3501 Section 9 / RFC 9051 Section 9
//! - [`MailboxName`]  -  RFC 3501 Section 5.1 / RFC 9051 Section 5.1
//! - [`ObjectId`]  -  RFC 8474 Section 4

use std::fmt;

/// Error returned when constructing a validated IMAP protocol type from an
/// invalid string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ValidationError(String);

impl ValidationError {
    /// Create a new validation error with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// Return the error message as a string slice.
    pub fn message(&self) -> &str {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// SequenceSet  -  RFC 3501 Section 9
// ---------------------------------------------------------------------------

/// A validated IMAP sequence-set string.
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
pub struct SequenceSet(String);

impl SequenceSet {
    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Create a general sequence set (allows `*` and `$`).
    ///
    /// Validates the input against RFC 3501 Section 9 `sequence-set` ABNF,
    /// extended by RFC 5182 Section 5 for `$`.
    pub fn new(s: impl Into<String>) -> Result<Self, ValidationError> {
        let s = s.into();
        if !is_valid_sequence_set(&s) {
            return Err(ValidationError::new(format!(
                "invalid sequence set per RFC 3501 Section 9: {s:?}"
            )));
        }
        Ok(Self(s))
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
                "\"*\" is not allowed in QRESYNC known-uids/sequence-set \
                 (RFC 7162 Section 3.2.5.2)",
            ));
        }
        // RFC 5182 search result reference is not valid in QRESYNC context.
        if s.contains('$') {
            return Err(ValidationError::new(
                "\"$\" (search result reference) is not allowed in QRESYNC \
                 known-uids/sequence-set (RFC 7162 Section 7)",
            ));
        }
        // Validate full sequence-set ABNF (since * and $ are already excluded,
        // this validates nz-number ranges only).
        if !is_valid_sequence_set(&s) {
            return Err(ValidationError::new(
                "QRESYNC known-uids/sequence-set is not a valid sequence-set \
                 (RFC 7162 Section 7: known-uids = sequence-set)",
            ));
        }
        Ok(Self(s))
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SequenceSet {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SequenceSet {
    /// Formats the sequence set as its wire representation
    /// (RFC 3501 Section 9).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for SequenceSet {
    type Error = ValidationError;

    /// Convert from `String`, validating per RFC 3501 Section 9.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<&str> for SequenceSet {
    type Error = ValidationError;

    /// Convert from `&str`, validating per RFC 3501 Section 9.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<SequenceSet> for String {
    fn from(s: SequenceSet) -> Self {
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
        return Err(ValidationError::new(format!(
            "{context} must be at least one character \
             (RFC 3501 Section 9: atom = 1*ATOM-CHAR)"
        )));
    }
    for &b in bytes {
        let is_atom_special = matches!(
            b,
            b'(' | b')' | b'{' | b' ' | b'%' | b'*' | b'"' | b'\\' | b']'
        );
        let is_ctl = b < 0x20 || b == 0x7F;
        let is_outside_char = b == 0 || b > 0x7F;
        if is_atom_special || is_ctl || is_outside_char {
            return Err(ValidationError::new(format!(
                "{context} contains invalid byte 0x{b:02X}  -  must be an atom \
                 (RFC 3501 Section 9: ATOM-CHAR excludes atom-specials, CTL, non-ASCII)"
            )));
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
/// (public, validating) and `from_wire` / `from_decoded` (codec-private, **not**
/// validating). `from_wire` retains the original address; `from_decoded`
/// retains only display text. A parsed name's display form is valid UTF-8; do not assume the NUL / CR /
/// LF exclusion for names you did not construct yourself. Injection safety on
/// the wire comes from the encoder, not from this type.
///
/// There is no `From<String>` or `From<&str>`  -  constructing the type
/// outside these paths is a compile error:
///
/// ```compile_fail
/// use bifrost_imap::MailboxName;
/// // There is no From<String>. This must fail to compile.
/// let _: MailboxName = "Inbox".to_string().into();
/// ```
///
/// ```compile_fail
/// use bifrost_imap::MailboxName;
/// // There is no From<&str>. This must fail to compile.
/// let _: MailboxName = "Inbox".into();
/// ```
#[derive(Debug, Clone)]
pub struct MailboxName(String, Option<(Vec<u8>, bool)>);

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
                "mailbox name must not contain NUL, CR, or LF  -  IMAP strings \
                 forbid NUL (RFC 3501 Section 9 / RFC 9051 Section 9) and \
                 commands are CRLF-delimited (RFC 3501 Section 2.2 / \
                 RFC 9051 Section 2.2)",
            ));
        }
        Ok(Self(s, None))
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Retain the address separately from the lenient display decoding.
    pub(crate) fn from_wire(s: String, bytes: &[u8], utf8: bool) -> Self {
        Self(s, Some((bytes.to_vec(), utf8)))
    }

    /// Original octets apply only in the mode in which they were received.
    pub(crate) fn wire_bytes(&self, utf8: bool) -> Option<&[u8]> {
        self.1
            .as_ref()
            .filter(|(_, mode)| *mode == utf8)
            .map(|(bytes, _)| bytes.as_slice())
    }

    // Canonical server spellings compare equal to caller-created names.
    // Noncanonical octets must remain distinct even when display decoding
    // collapses them; malformed UTF-8 needs a separate identity domain.
    fn identity(&self) -> (bool, Vec<u8>) {
        let canonical = crate::codec::utf7::encode_utf7(&self.0).into_bytes();
        match &self.1 {
            Some((bytes, false)) => (false, bytes.clone()),
            Some((bytes, true)) if bytes != self.0.as_bytes() => (true, bytes.clone()),
            _ => (false, canonical),
        }
    }

    /// Stable account identity. Ordinary canonical names keep their existing ids;
    /// noncanonical wire addresses and reserved-prefix names use an escaped form.
    pub(crate) fn account_id(&self) -> String {
        use base64::Engine;
        const PREFIX: &str = "~imap-wire:";
        if self.identity() == Self::from_decoded(self.0.clone()).identity()
            && !self.0.starts_with(PREFIX)
            && Self::new(self.0.clone()).is_ok()
        {
            return self.0.clone();
        }
        let (utf8, bytes) = self.identity();
        let mut payload = vec![u8::from(utf8)];
        payload.extend_from_slice(&bytes);
        format!(
            "{PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
        )
    }

    /// Stable display ordering with canonical wire identity as the tie-breaker.
    pub(crate) fn cmp_account_order(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str()).then_with(|| {
            let (mode, bytes) = self.identity();
            let (other_mode, other_bytes) = other.identity();
            bytes.cmp(&other_bytes).then(mode.cmp(&other_mode))
        })
    }

    pub(crate) fn from_account_id(id: &str) -> Result<Self, ValidationError> {
        use base64::Engine;
        let Some(payload) = id.strip_prefix("~imap-wire:") else {
            return Self::new(id);
        };
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| ValidationError::new("invalid mailbox identity"))?;
        let Some((&mode, bytes)) = payload.split_first() else {
            return Err(ValidationError::new("empty mailbox identity"));
        };
        if mode > 1 || bytes.contains(&0) {
            return Err(ValidationError::new("invalid mailbox identity payload"));
        }
        let display = if mode == 1 {
            String::from_utf8_lossy(bytes).into_owned()
        } else {
            crate::codec::utf7::decode_utf7(bytes)
        };
        Ok(Self::from_wire(display, bytes, mode == 1))
    }

    pub(crate) fn child(&self, delimiter: char, leaf: &str) -> Result<Self, ValidationError> {
        let display = Self::new(format!("{}{delimiter}{leaf}", self.0))?;
        Self::new(leaf)?;
        if let Some((bytes, utf8)) = &self.1 {
            // Hierarchy syntax is meaningful in the wire address, not in a
            // lenient display decoding. Never reconstruct a noncanonical parent.
            if !delimiter.is_ascii() && !self.can_reencode() {
                return Err(ValidationError::new(
                    "noncanonical mailbox requires an ASCII hierarchy delimiter",
                ));
            }
            if delimiter.is_ascii() {
                let mut raw = bytes.clone();
                raw.extend_from_slice(delimiter.to_string().as_bytes());
                let leaf = if *utf8 {
                    leaf.to_owned()
                } else {
                    crate::codec::utf7::encode_utf7(leaf)
                };
                raw.extend_from_slice(leaf.as_bytes());
                return Ok(Self::from_wire(display.0, &raw, *utf8));
            }
        }
        Ok(display)
    }

    pub(crate) fn parent(&self, delimiter: char) -> Result<Option<Self>, ValidationError> {
        if let Some((bytes, utf8)) = &self.1 {
            if !delimiter.is_ascii() && !self.can_reencode() {
                return Err(ValidationError::new(
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
                let display = if *utf8 {
                    String::from_utf8_lossy(raw).into_owned()
                } else {
                    crate::codec::utf7::decode_utf7(raw)
                };
                return Ok(Some(Self::from_wire(display, raw, *utf8)));
            }
        }
        self.0
            .rsplit_once(delimiter)
            .map(|(parent, _)| Self::new(parent))
            .transpose()
    }

    pub(crate) fn can_reencode(&self) -> bool {
        self.identity() == Self::from_decoded(self.0.clone()).identity()
    }

    /// Whether two decoded mailbox addresses name the same mailbox for a
    /// command response. Canonical encodings compare by their decoded name so
    /// caller-created targets match responses in either encoding mode. When
    /// either address is noncanonical, compare the retained wire identity so
    /// lossy display decoding cannot pair two distinct mailboxes. INBOX keeps
    /// its RFC-defined ASCII case-insensitive comparison for canonical names.
    pub(crate) fn matches_mailbox(&self, other: &Self) -> bool {
        if self.can_reencode() && other.can_reencode() {
            (self.0.eq_ignore_ascii_case("INBOX") && other.0.eq_ignore_ascii_case("INBOX"))
                || self.0 == other.0
        } else {
            self.identity() == other.identity()
        }
    }

    /// Construct from already-decoded bytes produced by the codec's decoder.
    /// Callers must guarantee the input has already been MUTF-7 decoded (or
    /// passed through in UTF-8 mode per RFC 6855).
    ///
    /// This display-only constructor bypasses validation. Without wire octets,
    /// it cannot recover a noncanonical server spelling; the parser uses
    /// `from_wire` instead. CR/LF are safely framed by the command writer.
    pub(crate) fn from_decoded(s: String) -> Self {
        Self(s, None)
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

/// Command arguments accept caller text or a retained server address. Text is
/// validated by the encoder, where literal framing safely handles CR and LF.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MailboxArgument(pub(crate) MailboxName);

impl From<String> for MailboxArgument {
    fn from(value: String) -> Self {
        Self(MailboxName::from_decoded(value))
    }
}
impl From<&str> for MailboxArgument {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}
impl From<MailboxName> for MailboxArgument {
    fn from(value: MailboxName) -> Self {
        Self(value)
    }
}
impl From<&MailboxName> for MailboxArgument {
    fn from(value: &MailboxName) -> Self {
        Self(value.clone())
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
        std::hash::Hash::hash(&self.identity(), state);
    }
}

impl Default for MailboxName {
    /// Returns an empty mailbox name.
    ///
    /// The empty string is valid per RFC 5464 Section 4.2 (server-level
    /// METADATA) and RFC 3501 Section 6.3.8 (LIST reference name).
    fn default() -> Self {
        Self(String::new(), None)
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
                "object identifier must not be empty (RFC 8474 Section 4)",
            ));
        }
        if s.len() > 255 {
            return Err(ValidationError::new(format!(
                "object identifier exceeds 255 characters ({} bytes) \
                 (RFC 8474 Section 4: objectid = 1*255(...))",
                s.len()
            )));
        }
        for &b in s.as_bytes() {
            let valid = b.is_ascii_alphanumeric() || b == b'-' || b == b'_';
            if !valid {
                return Err(ValidationError::new(format!(
                    "object identifier contains invalid byte 0x{b:02X}  -  \
                     only ALPHA / DIGIT / \"_\" / \"-\" are allowed \
                     (RFC 8474 Section 4)"
                )));
            }
        }
        Ok(Self(s))
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
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
// ParsedUidSet  -  RFC 7162 Section 3.2.6 defensive filtering
// ---------------------------------------------------------------------------

use crate::types::UidRange;

/// A parsed, normalized set of UID intervals for efficient containment checks.
///
/// Constructed from a [`SequenceSet`] by parsing its string representation into
/// sorted, merged, non-overlapping inclusive `(start, end)` intervals.
///
/// Used by `FetchVanishedConsumer` to defensively filter `VANISHED (EARLIER)`
/// UIDs that fall outside the requested set (RFC 7162 Section 3.2.6).
///
/// Returns `None` from [`ParsedUidSet::new`] when the set contains `$`
/// (RFC 5182 search result reference), which cannot be resolved client-side.
/// `*` is mapped to `u32::MAX` as a conservative upper bound  -  safe because `*`
/// means "the highest UID in the mailbox" (RFC 3501 Section 6.4.8), and all
/// reportable UIDs are <= that value.
pub(crate) struct ParsedUidSet(Vec<(u32, u32)>);

impl ParsedUidSet {
    /// Parse a [`SequenceSet`] into sorted, merged, non-overlapping intervals.
    ///
    /// Returns `None` if the set contains `$` (RFC 5182 search result
    /// reference), which cannot be resolved client-side  -  the caller should
    /// skip filtering entirely in that case.
    ///
    /// `*` is mapped to `u32::MAX` per RFC 3501 Section 6.4.8.
    pub(crate) fn new(set: &SequenceSet) -> Option<Self> {
        let s = set.as_str();
        let mut intervals: Vec<(u32, u32)> = Vec::new();

        for part in s.split(',') {
            // RFC 5182 Section 2: "$" references saved search results  -
            // unresolvable client-side.
            if part.contains('$') {
                return None;
            }

            if let Some((left, right)) = part.split_once(':') {
                // seq-range: two seq-numbers separated by ":"
                let start = Self::parse_seq_number(left)?;
                let end = Self::parse_seq_number(right)?;
                // Normalize: RFC 3501 Section 9 allows reversed ranges (e.g. "10:5").
                let (lo, hi) = if start <= end {
                    (start, end)
                } else {
                    (end, start)
                };
                intervals.push((lo, hi));
            } else {
                // Single seq-number
                let n = Self::parse_seq_number(part)?;
                intervals.push((n, n));
            }
        }

        // Sort by start, then merge overlapping/adjacent intervals.
        intervals.sort_unstable_by_key(|&(start, _)| start);
        let mut merged: Vec<(u32, u32)> = Vec::with_capacity(intervals.len());
        for (lo, hi) in intervals {
            if let Some(last) = merged.last_mut() {
                // Adjacent: last.end + 1 == lo (with saturating add to avoid overflow
                // when last.end == u32::MAX).
                if lo <= last.1 || lo == last.1.saturating_add(1) {
                    last.1 = last.1.max(hi);
                    continue;
                }
            }
            merged.push((lo, hi));
        }

        Some(Self(merged))
    }

    /// Parse a single seq-number: `"*"` -> `u32::MAX`, otherwise a non-zero u32.
    fn parse_seq_number(s: &str) -> Option<u32> {
        if s == "*" {
            Some(u32::MAX)
        } else {
            s.parse::<u32>().ok()
        }
    }

    /// Intersect a list of server-reported `VANISHED (EARLIER)` UID ranges
    /// against this parsed set, keeping only UIDs that fall within the
    /// requested set.
    ///
    /// Returns `(filtered_ranges, dropped_count)` where `dropped_count` is
    /// the number of individual UIDs that were removed by filtering.
    ///
    /// Uses a two-pointer sweep: for each vanished interval, walk the
    /// `ParsedUidSet` intervals to find overlapping regions.
    ///
    /// RFC 7162 Section 3.2.6: the server SHOULD limit `VANISHED (EARLIER)`
    /// to the requested UID set, but non-conformant servers may not.
    pub(crate) fn intersect_uid_ranges(&self, ranges: &[UidRange]) -> (Vec<UidRange>, usize) {
        let mut result: Vec<UidRange> = Vec::new();
        let mut total_input_uids: u64 = 0;
        let mut total_output_uids: u64 = 0;

        for range in ranges {
            let v_start = range.start;
            let v_end = range.end.unwrap_or(range.start);
            // Defensive: normalize reversed ranges from buggy servers.
            let (v_lo, v_hi) = if v_start <= v_end {
                (v_start, v_end)
            } else {
                (v_end, v_start)
            };
            total_input_uids += u64::from(v_hi) - u64::from(v_lo) + 1;

            // Two-pointer sweep against sorted, non-overlapping set intervals.
            for &(s_lo, s_hi) in &self.0 {
                // No overlap possible if the set interval is entirely after
                // the vanished interval.
                if s_lo > v_hi {
                    break;
                }
                // No overlap if the set interval is entirely before.
                if s_hi < v_lo {
                    continue;
                }
                // Compute overlap.
                let overlap_start = v_lo.max(s_lo);
                let overlap_end = v_hi.min(s_hi);
                // overlap_start <= overlap_end is guaranteed by the checks above.
                total_output_uids += u64::from(overlap_end) - u64::from(overlap_start) + 1;
                if overlap_start == overlap_end {
                    result.push(UidRange::single(overlap_start));
                } else {
                    result.push(UidRange::range(overlap_start, overlap_end));
                }
            }
        }

        // Dropped = total input UIDs minus total output UIDs.
        // In practice this count fits in usize  -  a VANISHED response cannot
        // reference more than u32::MAX UIDs  -  but we use saturating conversion
        // for 32-bit target safety.
        let dropped_u64 = total_input_uids - total_output_uids;
        let dropped = usize::try_from(dropped_u64).unwrap_or(usize::MAX);
        (result, dropped)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "validated_tests.rs"]
mod tests;

#[cfg(test)]
mod account_identity_tests {
    use super::MailboxName;

    #[test]
    fn equal_canonical_names_have_equal_escaped_ids_in_both_modes() {
        for display in ["~imap-wire:literal", "~imap-wire:旅行", "line\r\nbreak"] {
            let caller = MailboxName::from_decoded(display.into());
            let rev1_bytes = crate::codec::utf7::encode_utf7(display).into_bytes();
            let rev1 = MailboxName::from_wire(display.into(), &rev1_bytes, false);
            let utf8 = MailboxName::from_wire(display.into(), display.as_bytes(), true);
            for name in [caller.clone(), rev1, utf8] {
                assert_eq!(name, caller);
                assert_eq!(name.account_id(), caller.account_id());
                assert_eq!(
                    MailboxName::from_account_id(&name.account_id()).unwrap(),
                    caller
                );
            }
        }
    }
}
