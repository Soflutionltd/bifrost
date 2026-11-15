//! Mailbox-related types (RFC 3501 Sections 6.3, 7.2.2 / RFC 9051 Sections 6.3, 7.2.2).
//!
//! LIST responses, SELECT state, special-use attributes (RFC 6154), and STATUS items.

use super::fetch::FetchResponse;
use super::flag::Flag;
use super::response::UidRange;
use super::validated::MailboxName;

/// Information about a mailbox, as returned by LIST
/// (RFC 3501 Section 7.2.2 / RFC 9051 Section 7.2.2).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct MailboxInfo {
    /// Mailbox name (UTF-8; MUTF-7 decoded if necessary)
    /// (RFC 3501 Section 7.2.2 / RFC 9051 Section 7.2.2).
    pub name: MailboxName,
    /// Hierarchy delimiter (e.g. `'/'`), or `None` if the server has no hierarchy
    /// (RFC 3501 Section 7.2.2 / RFC 9051 Section 7.2.2).
    pub delimiter: Option<char>,
    /// Mailbox attributes (`\Noselect`, `\HasChildren`, special-use, etc.)
    /// (RFC 3501 Section 7.2.2 / RFC 9051 Section 7.2.2).
    pub attributes: Vec<MailboxAttribute>,
    /// Previous name of this mailbox (RFC 9051 Section 6.3.9.7 OLDNAME).
    ///
    /// Populated when a LIST response includes OLDNAME extended data,
    /// e.g. after a RENAME operation.
    pub old_name: Option<MailboxName>,
    /// CHILDINFO extended data (RFC 5258 Section 4).
    ///
    /// Lists the kinds of information available about children,
    /// e.g. `["SUBSCRIBED"]`.
    pub child_info: Vec<String>,
}

/// Mailbox attributes from LIST responses.
///
/// Covers both RFC 3501 base attributes and RFC 6154 special-use attributes.
///
/// Comparison and hashing are case-insensitive per RFC 5258 Section 4.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum MailboxAttribute {
    // --- RFC 3501 Section 7.2.2 / RFC 9051 Section 7.2.2 base attributes ---
    /// `\Noinferiors`  -  no child mailboxes possible (RFC 3501 Section 7.2.2).
    NoInferiors,
    /// `\Noselect`  -  not a selectable mailbox (RFC 3501 Section 7.2.2).
    NoSelect,
    /// `\NonExistent`  -  mailbox does not exist (RFC 9051 Section 7.2.2).
    NonExistent,
    /// `\HasChildren` (RFC 3348 / RFC 9051 Section 7.2.2).
    HasChildren,
    /// `\HasNoChildren` (RFC 3348 / RFC 9051 Section 7.2.2).
    HasNoChildren,
    /// `\Marked` (RFC 3501 Section 7.2.2).
    Marked,
    /// `\Unmarked` (RFC 3501 Section 7.2.2).
    Unmarked,
    /// `\Subscribed` (RFC 5258 Section 3.4).
    Subscribed,
    /// `\Remote` (RFC 5258 Section 3.4).
    Remote,

    // --- RFC 6154 special-use attributes ---
    /// `\All` (RFC 6154 Section 2).
    All,
    /// `\Archive` (RFC 6154 Section 2).
    Archive,
    /// `\Drafts` (RFC 6154 Section 2).
    Drafts,
    /// `\Flagged` (RFC 6154 Section 2).
    Flagged,
    /// `\Junk` (RFC 6154 Section 2).
    Junk,
    /// `\Sent` (RFC 6154 Section 2).
    Sent,
    /// `\Trash` (RFC 6154 Section 2).
    Trash,
    /// `\Important` (RFC 8457 Section 2).
    Important,

    // --- NOTIFY (RFC 5465) ---
    /// `\NoAccess`  -  client lacks access rights to this mailbox
    /// (RFC 5465 Section 5.9, Section 8: `mbx-list-oflag =/ "\NoAccess"`).
    ///
    /// Sent in LIST responses when the client loses the `l` (lookup) ACL right
    /// on a monitored mailbox. If access is later restored, the server sends
    /// a LIST without `\NoAccess`.
    NoAccess,

    /// Non-standard Inbox observation. Only the exact mailbox name INBOX
    /// establishes the account Inbox role.
    Inbox,

    // --- Non-standard (Google-origin) but widely seen in the wild ---
    /// `\Memos`  -  memos/notes folder (Google-origin, non-standard).
    Memos,
    /// `\Scheduled`  -  scheduled send folder (Google-origin, non-standard).
    Scheduled,
    /// `\Snoozed`  -  snoozed messages folder (Google-origin, non-standard).
    Snoozed,

    /// An unrecognized attribute  -  preserved verbatim.
    Custom(String),
}

impl MailboxAttribute {
    /// Returns the IMAP wire form of this attribute (e.g. `\Sent`, `\Drafts`).
    ///
    /// RFC 6154 Section 2 defines the special-use attributes as `use-attr` values:
    /// `use-attr = "\All" / "\Archive" / "\Drafts" / "\Flagged" /
    ///             "\Junk" / "\Sent" / "\Trash" / use-attr-ext`
    ///
    /// Base attributes are defined in RFC 3501 Section 7.2.2:
    /// `mbx-list-sflag = "\Noselect" / "\Marked" / "\Unmarked"`
    /// `mbx-list-oflag = "\Noinferiors" / child-mbox-flag / new-pflag / flag-extension`
    pub fn as_imap_str(&self) -> &str {
        match self {
            Self::NoInferiors => "\\Noinferiors",
            Self::NoSelect => "\\Noselect",
            Self::NonExistent => "\\NonExistent",
            Self::HasChildren => "\\HasChildren",
            Self::HasNoChildren => "\\HasNoChildren",
            Self::Marked => "\\Marked",
            Self::Unmarked => "\\Unmarked",
            Self::Subscribed => "\\Subscribed",
            Self::Remote => "\\Remote",
            Self::All => "\\All",
            Self::Archive => "\\Archive",
            Self::Drafts => "\\Drafts",
            Self::Flagged => "\\Flagged",
            Self::Junk => "\\Junk",
            Self::Sent => "\\Sent",
            Self::Trash => "\\Trash",
            Self::Important => "\\Important",
            Self::NoAccess => "\\NoAccess",
            Self::Inbox => "\\Inbox",
            Self::Memos => "\\Memos",
            Self::Scheduled => "\\Scheduled",
            Self::Snoozed => "\\Snoozed",
            Self::Custom(s) => s,
        }
    }

    /// Returns `true` if this attribute is a valid `use-attr` for the
    /// CREATE USE parameter (RFC 6154 Section 3 / Section 6 ABNF).
    ///
    /// `use-attr = "\All" / "\Archive" / "\Drafts" / "\Flagged" /
    ///             "\Junk" / "\Sent" / "\Trash" / use-attr-ext`
    ///
    /// `\Important` (RFC 8457 Section 2) is also a special-use attribute.
    /// Non-standard Google-origin attributes (`\Memos`, `\Scheduled`,
    /// `\Snoozed`) are accepted as `use-attr-ext` per Postel's law.
    /// `Custom` values are accepted only when they are valid `\atom`
    /// mailbox attributes and do not alias a base LIST attribute.
    ///
    /// Base LIST attributes (`\Noselect`, `\HasChildren`, etc.) are NOT
    /// special-use and MUST NOT appear in the USE parameter.
    pub fn is_special_use(&self) -> bool {
        match self {
            Self::All
            | Self::Archive
            | Self::Drafts
            | Self::Flagged
            | Self::Junk
            | Self::Sent
            | Self::Trash
            | Self::Important
            | Self::Inbox
            | Self::Memos
            | Self::Scheduled
            | Self::Snoozed => true,
            // RFC 6154 Section 6: use-attr-ext = "\" atom. Custom values are
            // only special-use when they are valid mailbox attributes with a
            // leading "\" and an IMAP atom payload (RFC 3501 Section 9 /
            // RFC 9051 Section 9), and when they do not alias a base LIST
            // attribute wire form.
            Self::Custom(s) => {
                Self::is_valid_special_use_attr_ext(s) && !Self::is_known_base_list_attr(s)
            }
            _ => false,
        }
    }

    /// Returns `true` if `s` is a valid `use-attr-ext` wire form.
    ///
    /// RFC 6154 Section 6: `use-attr-ext = "\" atom`.
    /// RFC 3501 Section 9 / RFC 9051 Section 9: `atom = 1*ATOM-CHAR`.
    fn is_valid_special_use_attr_ext(s: &str) -> bool {
        let Some(atom) = s.strip_prefix('\\') else {
            return false;
        };

        crate::types::validated::validate_atom_bytes(atom.as_bytes(), "special-use attribute")
            .is_ok()
    }

    /// Returns `true` if `s` (case-insensitive) matches the wire form of any
    /// known base LIST attribute.
    ///
    /// Base LIST attributes per RFC 3501 Section 7.2.2, RFC 3348, RFC 5258
    /// Section 3.4, and RFC 9051 Section 7.2.2:
    /// `\Noinferiors`, `\Noselect`, `\NonExistent`, `\HasChildren`,
    /// `\HasNoChildren`, `\Marked`, `\Unmarked`, `\Subscribed`, `\Remote`.
    fn is_known_base_list_attr(s: &str) -> bool {
        // RFC 3501 Section 9: IMAP atoms are case-insensitive.
        s.eq_ignore_ascii_case("\\Noinferiors")
            || s.eq_ignore_ascii_case("\\Noselect")
            || s.eq_ignore_ascii_case("\\NonExistent")
            || s.eq_ignore_ascii_case("\\HasChildren")
            || s.eq_ignore_ascii_case("\\HasNoChildren")
            || s.eq_ignore_ascii_case("\\Marked")
            || s.eq_ignore_ascii_case("\\Unmarked")
            || s.eq_ignore_ascii_case("\\Subscribed")
            || s.eq_ignore_ascii_case("\\Remote")
            // RFC 5465 Section 8: mbx-list-oflag =/ "\NoAccess"
            || s.eq_ignore_ascii_case("\\NoAccess")
    }
}

/// RFC 3501 Section 7.2.2: mailbox attributes use `flag-extension = "\" atom`,
/// and IMAP atoms are case-insensitive.
///
/// All variants compare through their protocol spelling.
/// `Custom` attributes compare using ASCII case-insensitive comparison so that
/// e.g. `\MyCustom` and `\mycustom` are treated as the same attribute.
///
/// Cross-representation is also handled: `Custom("\\Noselect")` equals
/// `NoSelect`, because they denote the same protocol attribute.
// Escape variants are also constructed directly by account callers. Keep
// protocol-spelling equality until those producers can use parser-only values;
// deriving enum equality here would make equivalent wire tokens unequal.
impl PartialEq for MailboxAttribute {
    fn eq(&self, other: &Self) -> bool {
        self.as_imap_str().eq_ignore_ascii_case(other.as_imap_str())
    }
}

/// RFC 3501 Section 7.2.2: attribute equality is reflexive, symmetric, transitive.
impl Eq for MailboxAttribute {}

/// RFC 3501 Section 7.2.2: mailbox attributes use `flag-extension = "\" atom`,
/// and IMAP atoms are case-insensitive.
///
/// The `Hash` implementation must be consistent with `PartialEq`: attributes that
/// compare equal must hash to the same value. Because `Custom("\\Noselect")`
/// must equal `NoSelect`, we hash the lowercased wire form (`as_imap_str()`)
/// for all variants, which is identical for cross-representation equivalents.
impl std::hash::Hash for MailboxAttribute {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // RFC 3501 Section 7.2.2: case-insensitive hashing via wire form.
        // Custom("\\Noselect") and NoSelect both yield "\\Noselect", so
        // lowercasing produces the same hash.
        for byte in self.as_imap_str().as_bytes() {
            byte.to_ascii_lowercase().hash(state);
        }
    }
}

impl MailboxAttribute {
    /// Parse a mailbox attribute from its IMAP wire representation
    /// (RFC 3501 Section 7.2.2, RFC 6154 Section 2).
    ///
    /// Case-insensitive per RFC 3501 Section 7.2.2: mailbox attributes are
    /// `flag-extension = "\" atom`, and IMAP atoms are case-insensitive.
    pub fn from_imap_str(s: &str) -> Self {
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
            "\\noselect" | "\\nonexistent" => {
                // RFC 9051 Section 7.2.2: \NonExistent supersedes \Noselect.
                if lower == "\\nonexistent" {
                    Self::NonExistent
                } else {
                    Self::NoSelect
                }
            }
            "\\noinferiors" => Self::NoInferiors,
            "\\haschildren" => Self::HasChildren,
            "\\hasnochildren" => Self::HasNoChildren,
            "\\marked" => Self::Marked,
            "\\unmarked" => Self::Unmarked,
            "\\subscribed" => Self::Subscribed,
            "\\remote" => Self::Remote,
            "\\all" => Self::All,
            "\\archive" => Self::Archive,
            "\\drafts" => Self::Drafts,
            "\\flagged" => Self::Flagged,
            "\\junk" => Self::Junk,
            "\\sent" => Self::Sent,
            "\\trash" => Self::Trash,
            "\\important" => Self::Important,
            // RFC 5465 Section 5.9 / Section 8: mbx-list-oflag =/ "\NoAccess"
            "\\noaccess" => Self::NoAccess,
            // Non-standard (Google-origin) but widely seen in the wild.
            "\\inbox" => Self::Inbox,
            "\\memos" => Self::Memos,
            "\\scheduled" => Self::Scheduled,
            "\\snoozed" => Self::Snoozed,
            _ => Self::Custom(s.to_owned()),
        }
    }
}

/// Converts a string to a `MailboxAttribute` using case-insensitive matching
/// (RFC 3501 Section 7.2.2).
impl From<String> for MailboxAttribute {
    fn from(s: String) -> Self {
        Self::from_imap_str(&s)
    }
}

/// Converts a string slice to a `MailboxAttribute` using case-insensitive matching
/// (RFC 3501 Section 7.2.2).
impl From<&str> for MailboxAttribute {
    fn from(s: &str) -> Self {
        Self::from_imap_str(s)
    }
}

impl std::fmt::Display for MailboxAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_imap_str())
    }
}

/// The "special use" category of a mailbox, determined by combining RFC 6154 attributes
/// with name-based fallback (case-insensitive match on well-known names).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpecialUse {
    /// The primary mailbox (implicit, not defined in RFC 6154).
    Inbox,
    /// All messages (RFC 6154 Section 2).
    All,
    /// Long-term storage (RFC 6154 Section 2).
    Archive,
    /// Drafts (RFC 6154 Section 2).
    Drafts,
    /// Sent messages (RFC 6154 Section 2).
    Sent,
    /// Spam / junk (RFC 6154 Section 2).
    Junk,
    /// Deleted messages (RFC 6154 Section 2).
    Trash,
    /// Flagged / starred messages (RFC 6154 Section 2).
    Flagged,
    /// Important messages (RFC 8457).
    Important,
}

impl MailboxInfo {
    /// Detect the special-use role of this mailbox.
    ///
    /// Exact INBOX is implicit; otherwise checks RFC 6154 attributes, then name matching
    /// on well-known names ("All Mail", "Sent", "Drafts", "Trash", "Spam",
    /// "Junk", "Archive", "Starred", "Important").
    pub fn special_use(&self) -> Option<SpecialUse> {
        SpecialUse::classify(&self.attributes, self.name.as_str(), self.delimiter)
    }
}

impl SpecialUse {
    /// The single SPECIAL-USE attribute classifier, also used to rank account roles.
    pub(crate) fn from_attributes(attributes: &[MailboxAttribute]) -> Option<Self> {
        attributes.iter().find_map(|attr| {
            match MailboxAttribute::from_imap_str(attr.as_imap_str()) {
                MailboxAttribute::All => Some(Self::All),
                MailboxAttribute::Archive => Some(Self::Archive),
                MailboxAttribute::Drafts => Some(Self::Drafts),
                MailboxAttribute::Sent => Some(Self::Sent),
                MailboxAttribute::Junk => Some(Self::Junk),
                MailboxAttribute::Trash => Some(Self::Trash),
                MailboxAttribute::Flagged => Some(Self::Flagged),
                MailboxAttribute::Important => Some(Self::Important),
                // A server's non-standard attribute cannot redirect primary
                // mail away from INBOX. Enterprise users expect the protocol's
                // primary mailbox, regardless of another folder's claim.
                MailboxAttribute::Inbox => None,
                _ => None,
            }
        })
    }

    pub(crate) fn classify(
        attributes: &[MailboxAttribute],
        name: &str,
        delimiter: Option<char>,
    ) -> Option<Self> {
        // Only the protocol INBOX is Inbox. An enterprise user's personal
        // Inbox must not be displaced by a nested name or a custom attribute.
        if name.eq_ignore_ascii_case("INBOX") {
            return Some(Self::Inbox);
        }
        if let Some(role) = Self::from_attributes(attributes) {
            return Some(role);
        }
        let leaf = Self::leaf_name(name, delimiter).to_ascii_lowercase();
        match leaf.as_str() {
            "all" | "all mail" => Some(Self::All),
            "sent" | "sent items" | "sent mail" | "sent messages" => Some(Self::Sent),
            "drafts" | "draft" => Some(Self::Drafts),
            "trash" | "deleted" | "deleted items" | "deleted messages" => Some(Self::Trash),
            "spam" | "junk" | "junk e-mail" | "junk mail" | "bulk mail" => Some(Self::Junk),
            "archive" | "archives" => Some(Self::Archive),
            "flagged" | "starred" => Some(Self::Flagged),
            "important" => Some(Self::Important),
            _ => None,
        }
    }

    pub(crate) fn leaf_name(name: &str, delimiter: Option<char>) -> &str {
        match delimiter {
            Some(delimiter) => name.rsplit_once(delimiter).map_or(name, |(_, leaf)| leaf),
            None => name,
        }
    }
}

/// SELECT's single authority for modification-sequence support.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum ModSeqSupport {
    #[default]
    Unknown,
    NoModSeq,
    Highest(std::num::NonZeroU64),
}

impl ModSeqSupport {
    /// Zero is an unusable concurrency hint, equivalent to NOMODSEQ.
    pub fn from_highest(value: u64) -> Self {
        std::num::NonZeroU64::new(value).map_or(Self::NoModSeq, Self::Highest)
    }
}

impl SelectedMailbox {
    pub fn modseq(&self) -> Option<super::ModSeq> {
        match self.modseq_support {
            ModSeqSupport::Highest(value) => super::ModSeq::new(value.get()),
            ModSeqSupport::Unknown | ModSeqSupport::NoModSeq => None,
        }
    }
}

/// State of a selected (or examined) mailbox
/// (RFC 3501 Section 6.3.1 / RFC 9051 Section 6.3.1).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct SelectedMailbox {
    /// Number of messages in the mailbox (RFC 3501 Section 7.3.1).
    pub exists: u32,
    /// Number of recent messages (RFC 3501 Section 7.3.2; always 0 for `IMAP4rev2`).
    pub recent: u32,
    /// UIDVALIDITY value (RFC 3501 Section 7.1).
    ///
    /// `None` when the server omits UIDVALIDITY or sends zero (a protocol
    /// violation; neither is a usable mailbox epoch).
    /// RFC 3501 Section 9 defines UIDVALIDITY as `nz-number` (> 0).
    pub uid_validity: Option<super::UidValidity>,
    /// Predicted next UID (RFC 3501 Section 7.1).
    pub uid_next: Option<super::Uid>,
    /// Flags defined in the mailbox (RFC 3501 Section 7.2.6).
    pub flags: Vec<Flag>,
    /// Flags the client can change permanently (RFC 3501 Section 7.1).
    pub permanent_flags: Vec<Flag>,
    /// Mailbox modification-sequence support established by SELECT.
    pub modseq_support: ModSeqSupport,
    /// First unseen message sequence number from `[UNSEEN n]` response code (RFC 3501 Section 7.1).
    ///
    /// Dropped in `IMAP4rev2` (RFC 9051), but commonly sent by rev1 servers.
    pub unseen: Option<u32>,
    /// Unique mailbox identifier from `[MAILBOXID (<id>)]` response code
    /// (RFC 8474 Section 5.1).
    ///
    /// The server SHOULD return this in the OK response to SELECT/EXAMINE.
    /// `None` when the server does not advertise OBJECTID or omits the code.
    pub mailbox_id: Option<String>,
    /// `true` if the mailbox was opened read-only (EXAMINE or `[READ-ONLY]`) (RFC 3501 Section 7.1).
    pub read_only: bool,
    /// `true` when the server sent `[UIDNOTSTICKY]` in the SELECT/EXAMINE
    /// response, indicating that UIDs assigned to messages in this mailbox
    /// are not persistent across sessions (RFC 4315 Section 2 / RFC 9051 Section 7.1).
    ///
    /// When set, clients must not cache UIDs for this mailbox.
    pub uid_not_sticky: bool,
    /// UIDs vanished since the client's last sync point (`VANISHED (EARLIER)`)
    /// (RFC 7162 Section 3.2.5.2).
    pub vanished: Vec<UidRange>,
    /// Messages with changed flags since the client's last sync point
    /// (RFC 7162 Section 3.2.5.2).
    pub changed_messages: Vec<FetchResponse>,
}

/// A STATUS response item (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StatusItem {
    /// `MESSAGES`  -  number of messages in the mailbox (RFC 3501 Section 6.3.10).
    Messages(u32),
    /// `RECENT`  -  number of recent messages (RFC 3501 Section 6.3.10).
    Recent(u32),
    /// `UNSEEN`  -  number of unseen messages (RFC 3501 Section 6.3.10).
    Unseen(u32),
    /// `UIDNEXT`  -  predicted next UID (RFC 3501 Section 6.3.10).
    UidNext(u32),
    /// `UIDVALIDITY`  -  UID validity value (RFC 3501 Section 6.3.10).
    UidValidity(u32),
    /// `DELETED`  -  number of messages with \Deleted flag (RFC 9051 Section 6.3.11).
    Deleted(u32),
    /// `HIGHESTMODSEQ`  -  highest mod-sequence value (RFC 7162 Section 3.1.2).
    HighestModSeq(u64),
    /// `SIZE`  -  total size of the mailbox in octets (RFC 8438).
    Size(u64),
    /// `MAILBOXID`  -  unique mailbox identifier (RFC 8474 Section 5.1).
    MailboxId(String),
    /// Per-mailbox append size limit in octets, None means no limit (RFC 7889 Section 3).
    AppendLimit(Option<u64>),
    /// `DELETED-STORAGE`  -  disk space consumed by deleted (not yet expunged)
    /// messages in octets (RFC 9208 Section 3).
    DeletedStorage(u64),
}

/// Result of a `STATUS` command (RFC 3501 Section 6.3.10).
///
/// When NOTIFY STATUS is active (RFC 5465 Section 4), the protocol provides
/// no marker to distinguish a solicited `STATUS` response from an unsolicited
/// NOTIFY `STATUS` for the same mailbox  -  they are wire-identical.  Rather
/// than silently classifying the ambiguous responses (which leads to dropped,
/// duplicated, or misrouted state transitions), this struct exposes them.
///
/// When NOTIFY is not active, [`ambiguous`](Self::ambiguous) is always empty.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusResult {
    /// Status items from the primary (solicited) response.
    ///
    /// Heuristic: last matching `STATUS` response for the requested mailbox
    /// (servers typically flush queued notifications before processing the
    /// new command  -  RFC 5465 Section 4, RFC 3501 Section 6.3.10).
    pub items: Vec<StatusItem>,
    /// Additional same-mailbox `STATUS` responses that arrived during the
    /// command while NOTIFY STATUS was active (RFC 5465 Section 4).
    ///
    /// Each entry is the item-list from one ambiguous `* STATUS` line.
    /// These could be the actual solicited result (if the heuristic picked
    /// wrong) or NOTIFY events.  The caller can decide how to handle them
    ///  -  e.g. re-inject into an event pipeline, merge, or discard.
    ///
    /// Empty when NOTIFY is not active or when only one `STATUS` response
    /// was received (the common case).
    pub ambiguous: Vec<Vec<StatusItem>>,
}

#[cfg(test)]
#[path = "mailbox_tests.rs"]
mod tests;

#[cfg(test)]
mod classifier_tests {
    use super::{MailboxAttribute, SpecialUse};

    #[test]
    fn inbox_attribute_is_typed_without_redirecting_the_primary_role() {
        assert_eq!(
            MailboxAttribute::from_imap_str("\\iNbOx"),
            MailboxAttribute::Inbox
        );
        assert_eq!(MailboxAttribute::Inbox.as_imap_str(), "\\Inbox");
        assert_eq!(
            SpecialUse::classify(&[MailboxAttribute::Inbox], "Other", None),
            None
        );
        assert_eq!(
            SpecialUse::classify(&[MailboxAttribute::Inbox], "INBOX", None),
            Some(SpecialUse::Inbox)
        );
    }

    #[test]
    fn classifier_unifies_aliases_and_respects_namespace_and_authority() {
        for (name, expected) in [
            ("Sent Items", SpecialUse::Sent),
            ("Sent Mail", SpecialUse::Sent),
            ("Deleted Items", SpecialUse::Trash),
            ("Junk E-mail", SpecialUse::Junk),
            ("Junk Mail", SpecialUse::Junk),
        ] {
            assert_eq!(SpecialUse::classify(&[], name, None), Some(expected));
        }
        assert_eq!(SpecialUse::classify(&[], "Projects/Sent", None), None);
        assert_eq!(SpecialUse::classify(&[], "Projects/Inbox", Some('/')), None);
        assert_eq!(
            SpecialUse::classify(&[], "INBOX.Sent Mail", Some('.')),
            Some(SpecialUse::Sent)
        );
        assert_eq!(
            SpecialUse::classify(&[MailboxAttribute::All], "Sent", None),
            Some(SpecialUse::All)
        );
        assert_eq!(
            SpecialUse::classify(&[MailboxAttribute::Custom("\\sEnT".into())], "Other", None),
            Some(SpecialUse::Sent)
        );
    }
}
