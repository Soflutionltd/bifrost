//! IMAP server response types (RFC 3501 Section 7 / RFC 9051 Section 7).
//!
//! Models the full grammar of IMAP responses: greeting, tagged, untagged, and continuation.
//! Extended response codes per RFC 5530.

use super::validated::MailboxName;
use super::{FetchResponse, Flag, MailboxInfo, StatusItem};

pub use super::capability::Capability;
pub use super::uid_range::UidRange;

/// A complete response line from the IMAP server
/// (RFC 3501 Section 2.2.2 / RFC 9051 Section 2.2.2).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Response {
    /// Initial server greeting on connection (RFC 3501 Section 7.1).
    Greeting(GreetingResponse),
    /// Tagged response to a client command (RFC 3501 Section 2.2.2).
    Tagged(TaggedResponse),
    /// Untagged (unsolicited or data) response (RFC 3501 Section 2.2.2).
    Untagged(Box<UntaggedResponse>),
    /// Continuation request (`+ ...`) (RFC 3501 Section 7.5).
    Continuation(ContinuationRequest),
}

/// Initial greeting sent by the server upon connection
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct GreetingResponse {
    /// Greeting status (`OK`, `PREAUTH`, or `BYE`) (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub status: GreetingStatus,
    /// Optional response code in square brackets (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub code: Option<ResponseCode>,
    /// Human-readable text following the status (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub text: String,
}

/// Status of the server greeting (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum GreetingStatus {
    /// `* OK`  -  server ready, client should authenticate.
    #[default]
    Ok,
    /// `* PREAUTH`  -  already authenticated (e.g. via TLS client cert).
    PreAuth,
    /// `* BYE`  -  server refusing connections.
    Bye,
}

/// Tagged response (response to a specific client command)
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct TaggedResponse {
    /// Command tag that this response corresponds to (RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1).
    pub tag: String,
    /// Completion status (`OK`, `NO`, or `BAD`) (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub status: StatusKind,
    /// Optional response code in square brackets (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub code: Option<ResponseCode>,
    /// Human-readable text following the status (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub text: String,
}

impl TaggedResponse {
    /// Check that the response indicates `OK` status. On success returns the
    /// response itself so callers can still access fields like `code`. On
    /// failure returns an appropriate [`Error`] for `NO` / `BAD`.
    ///
    /// RFC 3501 Section 7.1 / RFC 9051 Section 7.1: `OK` indicates success,
    /// `NO` an operational error, `BAD` a protocol-level error.
    pub(crate) fn require_ok(self) -> Result<Self, crate::error::Error> {
        match self.status {
            StatusKind::Ok => Ok(self),
            StatusKind::No => Err(crate::error::Error::no_with_code(self.text, self.code)),
            StatusKind::Bad => Err(crate::error::Error::bad_with_code(self.text, self.code)),
        }
    }
}

/// Status of a tagged response (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum StatusKind {
    #[default]
    Ok,
    No,
    Bad,
}

/// Status of an untagged status response (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum UntaggedStatus {
    #[default]
    Ok,
    No,
    Bad,
    Bye,
}

/// Untagged server response (RFC 3501 Section 7 / RFC 9051 Section 7).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UntaggedResponse {
    /// `* OK/NO/BAD/BYE [code] text` (RFC 3501 Section 7.1).
    Status {
        status: UntaggedStatus,
        code: Option<ResponseCode>,
        text: String,
    },
    /// `* <n> EXISTS` (RFC 3501 Section 7.3.1).
    Exists(u32),
    /// `* <n> RECENT` (RFC 3501 Section 7.3.2).
    Recent(u32),
    /// `* <n> EXPUNGE` (RFC 3501 Section 7.4.1).
    Expunge(u32),
    /// `* <n> FETCH (...)` (RFC 3501 Section 7.4.2).
    Fetch(Box<FetchResponse>),
    /// `* LIST (\attrs) "/" "name"` (RFC 3501 Section 7.2.2).
    List(MailboxInfo),
    /// `* LSUB (\attrs) "/" "name"` (RFC 3501 Section 7.2.3).
    Lsub(MailboxInfo),
    /// `* FLAGS (...)` (RFC 3501 Section 7.2.6).
    Flags(Vec<Flag>),
    /// `* SEARCH 1 2 3 ... [(MODSEQ n)]` (RFC 3501 Section 7.2.5, RFC 7162 Section 3.1.5).
    ///
    /// The optional `mod_seq` is present when the client searched with a MODSEQ
    /// criterion and the result is non-empty (RFC 7162 Section 3.1.5).
    Search {
        /// Matching message sequence numbers or UIDs.
        uids: Vec<u32>,
        /// Highest mod-sequence of matching messages (RFC 7162 Section 3.1.5).
        mod_seq: Option<u64>,
    },
    /// `* ESEARCH (TAG "tag") [UID] result-data` (RFC 4731 Section 3.1).
    ///
    /// RFC 4731 Section 3.1 ABNF:
    /// `search-return-data = "MIN" SP nz-number / "MAX" SP nz-number /
    ///                        "ALL" SP sequence-set / "COUNT" SP number`
    Esearch(EsearchResponse),
    /// `* STATUS "mailbox" (...)` (RFC 3501 Section 7.2.4).
    MailboxStatus {
        mailbox: MailboxName,
        items: Vec<StatusItem>,
    },
    /// `* CAPABILITY ...` (RFC 3501 Section 7.2.1).
    Capability(Vec<Capability>),
    /// `* ENABLED ...` (RFC 5161 Section 3.2).
    Enabled(Vec<String>),
    /// `* VANISHED (EARLIER) 1:5` (RFC 7162 QRESYNC).
    Vanished { earlier: bool, uids: Vec<UidRange> },
    /// `* ID (...)` (RFC 2971 Section 3.2).
    Id(Vec<(String, Option<String>)>),
    /// `* NAMESPACE personal other shared` (RFC 2342).
    Namespace {
        personal: Vec<NamespaceDescriptor>,
        other: Vec<NamespaceDescriptor>,
        shared: Vec<NamespaceDescriptor>,
    },

    // --- QUOTA (RFC 2087) ---
    /// `* QUOTA <root> (STORAGE <usage> <limit>)` (RFC 2087 Section 5.1).
    Quota {
        root: String,
        resources: Vec<QuotaResource>,
    },
    /// `* QUOTAROOT <mailbox> <root1> <root2> ...` (RFC 2087 Section 5.2).
    QuotaRoot {
        mailbox: MailboxName,
        roots: Vec<String>,
    },

    // --- ACL (RFC 4314) ---
    /// `* ACL <mailbox> <id1> <rights1> ...` (RFC 4314 Section 3.6).
    Acl {
        mailbox: MailboxName,
        entries: Vec<AclEntry>,
    },
    /// `* MYRIGHTS <mailbox> <rights>` (RFC 4314 Section 3.8).
    MyRights {
        mailbox: MailboxName,
        rights: String,
    },
    /// `* LISTRIGHTS <mailbox> <id> <required> <optional1> ...` (RFC 4314 Section 3.7).
    ListRights {
        mailbox: MailboxName,
        identifier: String,
        required: String,
        optional: Vec<String>,
    },
    /// `* METADATA "mailbox" (entry1 value1 ...)` (RFC 5464 Section 4.4).
    Metadata {
        mailbox: MailboxName,
        entries: Vec<MetadataEntry>,
    },
    /// `* THREAD (...)` (RFC 5256 Section 4).
    Thread(Vec<ThreadNode>),
    /// SORT response  -  sorted message numbers and optional MODSEQ
    /// (RFC 5256 Section 4, RFC 7162 Section 3.1.6).
    ///
    /// RFC 7162 Section 3.1.6: when a MODSEQ search criterion is used and the
    /// SORT result is non-empty, the server appends `(MODSEQ <n>)`.
    Sort {
        /// Sorted message numbers or UIDs (RFC 5256 Section 4).
        nums: Vec<u32>,
        /// Highest mod-sequence value of matching messages (RFC 7162 Section 3.1.6).
        mod_seq: Option<u64>,
    },
    /// Unknown or unrecognized untagged response (RFC 9051 Section 2.2.2).
    ///
    /// Servers may send extension responses that this client does not yet
    /// implement. Per RFC 9051, clients MUST tolerate such responses.
    Unknown(String),
}

/// Continuation request from the server (RFC 3501 Section 7.5 / RFC 9051 Section 7.5).
///
/// RFC 3501 Section 7.5: `continue-req = "+" SP (resp-text / base64) CRLF`
/// where `resp-text = ["[" resp-text-code "]" SP] text`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ContinuationRequest {
    /// Optional response code in square brackets (RFC 3501 Section 7.5 / RFC 9051 Section 7.5).
    ///
    /// Present when the server sends a continuation like `+ [ALERT] text\r\n`.
    /// Base64 SASL challenges never start with `[`, so this is `None` for those.
    pub code: Option<ResponseCode>,
    /// Text or base64 challenge from the server
    /// (RFC 3501 Section 7.5 / RFC 9051 Section 7.5.1).
    pub data: String,
}

/// Response code in square brackets (e.g. `[UIDVALIDITY 12345]`)
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResponseCode {
    /// `[ALERT]`  -  must be presented to the user (RFC 3501 Section 7.1).
    Alert,
    /// `[BADCHARSET (charsets)]`  -  search charset not supported (RFC 3501 Section 7.1).
    BadCharset(Vec<String>),
    /// `[CAPABILITY ...]`  -  capability list (RFC 3501 Section 7.1).
    Capability(Vec<Capability>),
    /// `[PARSE]`  -  message headers could not be parsed (RFC 3501 Section 7.1).
    Parse,
    /// `[PERMANENTFLAGS (flags)]`  -  flags the client can change permanently (RFC 3501 Section 7.1).
    PermanentFlags(Vec<Flag>),
    /// `[READ-ONLY]`  -  mailbox is read-only (RFC 3501 Section 7.1).
    ReadOnly,
    /// `[READ-WRITE]`  -  mailbox is read-write (RFC 3501 Section 7.1).
    ReadWrite,
    /// `[TRYCREATE]`  -  attempt to CREATE the target mailbox (RFC 3501 Section 7.1).
    TryCreate,
    /// `[UIDNEXT n]`  -  predicted next UID (RFC 3501 Section 7.1).
    UidNext(u32),
    /// `[UIDVALIDITY n]`  -  UID validity value (RFC 3501 Section 7.1).
    UidValidity(u32),
    /// `[UNSEEN n]`  -  first unseen message sequence number (RFC 3501 Section 7.1).
    Unseen(u32),
    /// `[APPENDUID uidvalidity uid-set]` (RFC 4315 UIDPLUS Section 3).
    ///
    /// For a single APPEND, `uids` contains one range. For MULTIAPPEND
    /// (RFC 3502), `uids` may contain multiple ranges.
    AppendUid {
        uid_validity: u32,
        uids: Vec<UidRange>,
    },
    /// `[COPYUID uidvalidity source-uids dest-uids]` (RFC 4315 UIDPLUS).
    CopyUid {
        uid_validity: u32,
        source_uids: Vec<UidRange>,
        dest_uids: Vec<UidRange>,
    },
    /// `[HIGHESTMODSEQ n]` (RFC 7162 CONDSTORE).
    HighestModSeq(u64),
    /// `[MODIFIED sequence-set]` (RFC 7162 Section 3.1.3 / Section 7).
    ///
    /// For `STORE`, the set contains message sequence numbers. For `UID STORE`,
    /// it contains UIDs.
    Modified(Vec<UidRange>),
    /// `[NOMODSEQ]`  -  mailbox does not support mod-sequences (RFC 7162 Section 3.1.2).
    NoModSeq,
    /// `[CLOSED]`  -  previously selected mailbox is now closed (RFC 7162 QRESYNC).
    Closed,
    /// `[MAILBOXID (objectid)]`  -  unique mailbox identifier (RFC 8474 Section 5.1).
    MailboxId(String),

    // --- RFC 5530 extended response codes ---
    /// `[UNAVAILABLE]`  -  server temporarily unavailable (RFC 5530 Section 3).
    Unavailable,
    /// `[AUTHENTICATIONFAILED]`  -  authentication credentials invalid (RFC 5530 Section 3).
    AuthenticationFailed,
    /// `[AUTHORIZATIONFAILED]`  -  authorization identity not permitted (RFC 5530 Section 3).
    AuthorizationFailed,
    /// `[EXPIRED]`  -  credentials have expired (RFC 5530 Section 3).
    Expired,
    /// `[PRIVACYREQUIRED]`  -  operation requires encryption (RFC 5530 Section 3).
    PrivacyRequired,
    /// `[CONTACTADMIN]`  -  contact server administrator (RFC 5530 Section 3).
    ContactAdmin,
    /// `[NOPERM]`  -  no permission to perform the operation (RFC 5530 Section 3).
    NoPerm,
    /// `[INUSE]`  -  resource is in use by another session (RFC 5530 Section 3).
    InUse,
    /// `[EXPUNGEISSUED]`  -  expunge occurred during operation (RFC 5530 Section 3).
    ExpungeIssued,
    /// `[CORRUPTION]`  -  server detected data corruption (RFC 5530 Section 3).
    Corruption,
    /// `[SERVERBUG]`  -  server encountered an internal bug (RFC 5530 Section 3).
    ServerBug,
    /// `[CLIENTBUG]`  -  client sent malformed or nonsensical data (RFC 5530 Section 3).
    ClientBug,
    /// `[CANNOT]`  -  operation is not supported on this mailbox/server (RFC 5530 Section 3).
    Cannot,
    /// `[LIMIT]`  -  operation exceeds a server-imposed limit (RFC 5530 Section 3).
    Limit,
    /// `[OVERQUOTA]`  -  user has exceeded their storage quota (RFC 5530 Section 3).
    OverQuota,
    /// `[ALREADYEXISTS]`  -  mailbox already exists (e.g. on CREATE) (RFC 5530 Section 3).
    AlreadyExists,
    /// `[NONEXISTENT]`  -  mailbox does not exist (e.g. on SELECT/DELETE) (RFC 5530 Section 3).
    NonExistent,
    /// `[NEWNAME ...]`  -  registered response code, obsolete but still standardized;
    /// trailing data is preserved verbatim (RFC 5530 Section 6).
    NewName(Option<String>),
    /// `[REFERRAL ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    Referral(Option<String>),
    /// `[URLMECH ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    UrlMech(Option<String>),
    /// `[BADURL ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    BadUrl(Option<String>),
    /// `[BADCOMPARATOR ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5530 Section 6).
    BadComparator(Option<String>),
    /// `[ANNOTATE ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    Annotate(Option<String>),
    /// `[ANNOTATIONS ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    Annotations(Option<String>),
    /// `[TEMPFAIL ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    TempFail(Option<String>),
    /// `[MAXCONVERTMESSAGES ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5530 Section 6).
    MaxConvertMessages(Option<String>),
    /// `[MAXCONVERTPARTS ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5530 Section 6).
    MaxConvertParts(Option<String>),
    /// `[NOUPDATE ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    NoUpdate(Option<String>),
    /// `[NOTIFICATIONOVERFLOW ...]`  -  registered response code; trailing data
    /// is preserved verbatim for consumers (RFC 5465 Section 5.8 / RFC 5530 Section 6).
    NotificationOverflow(Option<String>),
    /// `[BADEVENT ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5465 Section 5 / RFC 5530 Section 6).
    BadEvent(Option<String>),
    /// `[UNDEFINED-FILTER ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5465 Section 8 / RFC 5530 Section 6).
    UndefinedFilter(Option<String>),

    /// `[UIDNOTSTICKY]`  -  assigned UIDs are not persistent (RFC 4315 Section 2 / RFC 9051 Section 7.1).
    UidNotSticky,
    /// `[NOTSAVED]`  -  search result variable `$` is empty (RFC 5182 Section 2.1).
    NotSaved,
    /// `[HASCHILDREN]`  -  mailbox has child mailboxes (RFC 9051 Section 7.1).
    HasChildren,
    /// `[UNKNOWN-CTE]`  -  BINARY fetch failed due to unknown CTE (RFC 3516 Section 4.3).
    UnknownCte,
    /// `[TOOBIG]`  -  message too large for APPEND (RFC 7889 Section 4).
    TooBig,
    /// `[COMPRESSIONACTIVE]`  -  compression layer already active (RFC 4978 Section 3).
    CompressionActive,
    /// `[USEATTR]`  -  special-use attribute not supported (RFC 6154 Section 6).
    UseAttr,

    // --- METADATA (RFC 5464) ---
    /// `[METADATA LONGENTRIES n]`  -  entry values were truncated at `n` bytes
    /// (RFC 5464 Section 4.2.1).
    MetadataLongEntries(u64),
    /// `[METADATA MAXSIZE n]`  -  server's maximum annotation size
    /// (RFC 5464 Section 4.3).
    MetadataMaxSize(u64),
    /// `[METADATA TOOMANY]`  -  too many annotations on this mailbox
    /// (RFC 5464 Section 4.3).
    MetadataTooMany,
    /// `[METADATA NOPRIVATE]`  -  server does not support private annotations
    /// (RFC 5464 Section 4.3).
    MetadataNoPrivate,

    /// Unrecognized response code  -  preserved for forward compatibility.
    Other { name: String, value: Option<String> },
}

/// A single namespace entry from a NAMESPACE response (RFC 2342).
///
/// RFC 2342 Section 6 ABNF:
/// ```text
/// Namespace = nil / "(" 1*( "(" string SP  (<"> QUOTED_CHAR <"> / nil)
///                    *(Namespace_Response_Extension) ")" ) ")"
/// Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct NamespaceDescriptor {
    /// Namespace prefix (e.g. `""`, `"INBOX."`, `"#shared."`) (RFC 2342 Section 5).
    pub prefix: String,
    /// Hierarchy delimiter for this namespace, or `None` if flat (RFC 2342 Section 5).
    pub delimiter: Option<char>,
    /// Extension key-value-list pairs (RFC 2342 Section 6).
    ///
    /// Each entry is `(key, values)` where key is a string and values is a
    /// non-empty list of strings, corresponding to one
    /// `Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"`.
    pub extensions: Vec<(String, Vec<String>)>,
}

/// Result of a NAMESPACE command (RFC 2342 Section 5).
///
/// Groups the three namespace categories into named fields instead of a
/// positional tuple, making it safe to extend in the future.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct NamespaceResponse {
    /// Personal namespaces (RFC 2342 Section 5).
    pub personal: Vec<NamespaceDescriptor>,
    /// Other users' namespaces (RFC 2342 Section 5).
    pub other: Vec<NamespaceDescriptor>,
    /// Shared namespaces (RFC 2342 Section 5).
    pub shared: Vec<NamespaceDescriptor>,
}

/// Result of a GETQUOTAROOT command (RFC 2087 Section 4.3 / RFC 9208 Section 4.1.2).
///
/// Contains the quota root names and the quota resources associated with
/// each root.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct QuotaRootResponse {
    /// Quota root names returned by the server (RFC 2087 Section 4.3).
    pub roots: Vec<String>,
    /// Quota resources keyed by root name (RFC 2087 Section 4.3).
    ///
    /// Each entry is `(root_name, resources)` where `resources` is the list
    /// of resource triplets for that root.
    pub resources: Vec<(String, Vec<QuotaResource>)>,
}

/// Result of a LISTRIGHTS command (RFC 4314 Section 3.4).
///
/// Contains the rights that are always granted and the groups of optional
/// rights that can be independently granted.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ListRightsResponse {
    /// Rights that are always granted to the identifier (RFC 4314 Section 3.4).
    pub required: String,
    /// Groups of optional rights that can be independently granted
    /// (RFC 4314 Section 3.4).
    ///
    /// Each element is a string of right characters that form an indivisible
    /// group: granting any character in the group grants all of them.
    pub optional: Vec<String>,
}

/// ESEARCH response data (RFC 4731 Section 3.1).
///
/// RFC 4731 Section 3.1 ABNF:
/// `search-return-data = "MIN" SP nz-number / "MAX" SP nz-number /
///                        "ALL" SP sequence-set / "COUNT" SP number`
///
/// Normative rules:
/// - MIN: "Return the lowest message number/UID that satisfies the SEARCH criteria.
///   If the SEARCH results in no matches, the server MUST NOT include the MIN result
///   option in the ESEARCH response."
/// - MAX: "Return the highest message number/UID that satisfies the SEARCH criteria.
///   If the SEARCH results in no matches, the server MUST NOT include the MAX result
///   option in the ESEARCH response."
/// - ALL: Returns matching messages as a sequence-set rather than space-separated.
///   "If the SEARCH results in no matches, the server MUST NOT include the ALL result
///   option in the ESEARCH response."
/// - COUNT: "Return number of the messages that satisfy the SEARCH criteria. This result
///   option MUST always be included in the ESEARCH response."
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct EsearchResponse {
    /// Correlating tag from `(TAG "tagstring")`, if present
    /// (RFC 4466 Section 2.6.2 `search-correlator`).
    pub tag: Option<String>,
    /// `true` when the response includes the `UID` indicator,
    /// meaning all returned numbers are UIDs rather than sequence numbers
    /// (RFC 4731 Section 3.1).
    pub uid: bool,
    /// MIN  -  lowest matching message number/UID (RFC 4731 Section 3.1).
    pub min: Option<u32>,
    /// MAX  -  highest matching message number/UID (RFC 4731 Section 3.1).
    pub max: Option<u32>,
    /// COUNT  -  number of matching messages (RFC 4731 Section 3.1).
    pub count: Option<u32>,
    /// ALL  -  matching message numbers/UIDs as a uid-set (RFC 4731 Section 3.1).
    pub all: Vec<UidRange>,
    /// MODSEQ  -  highest mod-sequence of matching messages (RFC 7162 Section 3.1.10).
    pub mod_seq: Option<u64>,
}

/// Removals an EXPUNGE, UID EXPUNGE or MOVE reported (RFC 3501 Section
/// 7.4.1 / RFC 7162 Section 3.2.10), in the order the server sent them.
///
/// The form of each removal is whatever ARRIVED, never inferred from the
/// connection's QRESYNC state. After `ENABLE QRESYNC` a mailbox with
/// persistent mod-sequences reports `VANISHED`, but a `[NOMODSEQ]` mailbox on
/// the same connection still reports `EXPUNGE` (RFC 7162 Section 3.2.10), and
/// RFC 6851 Section 4.4 tells a QRESYNC client to handle both during MOVE.
/// Arrival order matters for the sequence-number form: each `* n EXPUNGE`
/// renumbers the messages after it.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ExpungeResult {
    /// Every removal, in arrival order.
    pub removals: Vec<Removal>,
}

/// One removal report (RFC 3501 Section 7.4.1 / RFC 7162 Section 3.2.10).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Removal {
    /// `* n EXPUNGE`: the message at sequence number `n` was removed.
    Seq(u32),
    /// `* VANISHED [(EARLIER)] uid-set`. `earlier` marks a historical report
    /// of UIDs already gone, which does not change the message count.
    Uids { earlier: bool, uids: Vec<UidRange> },
}

/// Result of a MOVE command (RFC 6851 Section 3).
///
/// RFC 6851 Section 3 specifies that the server sends EXPUNGE (or VANISHED
/// when QRESYNC is enabled per RFC 7162 Section 3.2.10) responses *before*
/// the tagged OK. RFC 6851 Section 4.3 asks a UIDPLUS server to send the
/// COPYUID response code in an untagged OK before those expunges, though
/// some servers put it on the tagged OK instead. This struct captures both
/// pieces of information.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct MoveResult {
    /// The response code from the tagged OK when it carries one, otherwise
    /// the code from the first untagged `OK [COPYUID ...]`. Under QRESYNC
    /// the tagged OK typically carries `HIGHESTMODSEQ` (RFC 7162), in which
    /// case the COPYUID is only in [`copy_uid`](Self::copy_uid). On the
    /// COPY + UID EXPUNGE fallback for a server without MOVE, the UID
    /// EXPUNGE's tagged OK plays the MOVE's role, falling back to the
    /// COPY's code when the EXPUNGE carried none.
    pub code: Option<ResponseCode>,
    /// The `COPYUID` response code (RFC 4315 Section 3, RFC 6851 Section
    /// 4.3): the tagged OK's code when that is COPYUID, otherwise the first
    /// untagged `OK [COPYUID ...]`, whatever code the tagged OK carries.
    /// Always a [`ResponseCode::CopyUid`] when present. `None` when the
    /// server sent no COPYUID at all.
    pub copy_uid: Option<ResponseCode>,
    /// The EXPUNGE or VANISHED responses that preceded the tagged OK
    /// (RFC 6851 Section 3 / RFC 7162 Section 3.2.10).
    pub expunged: ExpungeResult,
}

/// Result of a COPY or UID COPY command (RFC 3501 Section 6.4.7).
///
/// RFC 4315 Section 3 specifies that the server SHOULD respond with a
/// `[COPYUID uid-validity source-uids dest-uids]` response code in the
/// tagged OK; some servers send it in an untagged OK instead. This struct
/// captures the tagged code and the COPYUID separately, because the tagged
/// OK can carry a different code of its own.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct CopyResult {
    /// The response code from the tagged OK when it carries one, otherwise
    /// the code from the first untagged `OK [COPYUID ...]`. `None` when the
    /// server sent neither. When the tagged OK carries a code other than
    /// COPYUID, the COPYUID is only in [`copy_uid`](Self::copy_uid).
    pub code: Option<ResponseCode>,
    /// The `COPYUID` response code (RFC 4315 Section 3): the tagged OK's
    /// code when that is COPYUID, otherwise the first untagged
    /// `OK [COPYUID ...]`, whatever code the tagged OK carries. Always a
    /// [`ResponseCode::CopyUid`] when present. `None` when the server sent
    /// no COPYUID at all.
    pub copy_uid: Option<ResponseCode>,
}

/// Parameters for QRESYNC-enabled SELECT/EXAMINE (RFC 7162 Section 3.2.5.2).
///
/// Allows the client to provide its last known state so the server can send
/// only the changes since the last synchronization point.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct QresyncParams {
    /// The UIDVALIDITY value from the last session (RFC 7162 Section 3.2.5.2).
    pub uid_validity: u32,
    /// The highest MODSEQ value the client has cached (RFC 7162 Section 3.2.5.2).
    pub mod_seq: u64,
    /// Optional set of known UIDs for more efficient resync (RFC 7162 Section 3.2.5.2).
    pub known_uids: Option<String>,
    /// Optional sequence-to-UID mapping for detecting message renumbering
    /// (RFC 7162 Section 3.2.5.2).
    ///
    /// `seq-match-data = "(" known-sequence-set SP known-uid-set ")"`
    pub seq_match_data: Option<(String, String)>,
}

impl QresyncParams {
    /// Create QRESYNC parameters with the required fields
    /// (RFC 7162 Section 3.2.5.2).
    ///
    /// Optional fields (`known_uids`, `seq_match_data`) default to `None`.
    pub fn new(uid_validity: u32, mod_seq: u64) -> Self {
        Self {
            uid_validity,
            mod_seq,
            known_uids: None,
            seq_match_data: None,
        }
    }
}

/// Options for SELECT/EXAMINE commands beyond the basic mailbox name
/// (RFC 3501 Sections 6.3.1/6.3.2, RFC 7162 Sections 3.1.8 and 3.2.5.2).
///
/// Used with [`ImapConnection::select_with`] and [`ImapConnection::examine_with`]
/// to request CONDSTORE or QRESYNC extensions without requiring separate method
/// variants for each combination.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct SelectOptions {
    /// Enable CONDSTORE per-message mod-sequence tracking
    /// (RFC 7162 Section 3.1.1).
    ///
    /// When `true`, the server includes `HIGHESTMODSEQ` in the OK response
    /// and tracks per-message MODSEQ values for the selected mailbox.
    /// Requires the CONDSTORE or QRESYNC capability (RFC 7162 Section 3.1).
    pub condstore: bool,
    /// QRESYNC parameters for efficient delta sync
    /// (RFC 7162 Section 3.2.5.2).
    ///
    /// Provides the server with the client's last known UIDVALIDITY and MODSEQ
    /// so it can send `VANISHED (EARLIER)` and `FETCH (FLAGS)` for changed
    /// messages instead of a full resync.
    /// Requires the QRESYNC capability to have been enabled first
    /// (RFC 7162 Section 3.2.5).
    pub qresync: Option<QresyncParams>,
}

impl SelectOptions {
    /// Create options with CONDSTORE enabled (RFC 7162 Section 3.1.1).
    pub fn condstore() -> Self {
        Self {
            condstore: true,
            ..Self::default()
        }
    }

    /// Create options with QRESYNC parameters (RFC 7162 Section 3.2.5.2).
    pub fn qresync(params: QresyncParams) -> Self {
        Self {
            qresync: Some(params),
            ..Self::default()
        }
    }
}

/// A single quota resource from a QUOTA response (RFC 2087 Section 5.1).
///
/// Each resource triplet consists of a name (e.g. `STORAGE`, `MESSAGE`),
/// the current usage, and the limit.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct QuotaResource {
    /// Resource name (e.g. `"STORAGE"`, `"MESSAGE"`) (RFC 2087 Section 5.1).
    pub name: String,
    /// Current usage of this resource (RFC 2087 Section 5.1).
    pub usage: u64,
    /// Server-imposed limit for this resource (RFC 2087 Section 5.1).
    pub limit: u64,
}

/// A single ACL entry from an ACL response (RFC 4314 Section 3.6).
///
/// Each entry pairs an identifier (user or group name) with a rights string.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct AclEntry {
    /// The identifier (user or group) this entry applies to (RFC 4314 Section 3.6).
    pub identifier: String,
    /// The rights string for this identifier (RFC 4314 Section 3.6).
    pub rights: String,
}

/// A single metadata entry from a METADATA response (RFC 5464 Section 4.4).
///
/// Each entry has a name (e.g. `/private/comment`) and an optional value.
/// A `None` value indicates the entry does not exist or has been deleted.
///
/// RFC 5464 Section 5 formal syntax: `value = nstring / literal8`.
/// The `literal8` form (`~{n}\r\n<data>`) allows arbitrary binary octets,
/// so the value is stored as raw bytes rather than a UTF-8 string.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct MetadataEntry {
    /// Entry name (e.g. `/private/comment`, `/shared/vendor/foo`) (RFC 5464 Section 3.2).
    pub name: String,
    /// Entry value as raw bytes, or `None` if the entry does not exist (RFC 5464 Section 4.4).
    ///
    /// RFC 5464 Section 5: `value = nstring / literal8`  -  values may contain
    /// arbitrary binary data via the `literal8` syntax, so `Vec<u8>` is used
    /// instead of `String` to preserve binary fidelity.
    pub value: Option<Vec<u8>>,
}

/// Result of a `GETMETADATA` command (RFC 5464 Section 4.2).
///
/// When NOTIFY metadata is active (RFC 5465 Sections 5.6-5.8), the protocol
/// provides no marker to distinguish solicited `METADATA` responses from
/// unsolicited NOTIFY `METADATA` for the same mailbox  -  they are
/// wire-identical.  Unlike `STATUS` (which expects exactly one solicited
/// response, enabling a last-match heuristic), `GETMETADATA` can legitimately
/// produce multiple solicited `METADATA` response lines, so no reliable
/// heuristic exists.
///
/// This struct exposes the ambiguity via [`notify_ambiguity`](Self::notify_ambiguity)
/// so callers can take appropriate action (e.g. treat entries as potentially
/// stale, re-query without NOTIFY, or duplicate entries to a NOTIFY event
/// pipeline).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResult {
    /// All metadata entries from matching `METADATA` responses, merged.
    pub entries: Vec<MetadataEntry>,
    /// `true` when NOTIFY metadata was active during the call (RFC 5465
    /// Sections 5.6-5.8), meaning some entries may be from unsolicited
    /// NOTIFY events that were indistinguishable from the solicited response.
    ///
    /// When `true`, callers that require unambiguous results should avoid
    /// issuing `GETMETADATA` while NOTIFY metadata delivery is active, or
    /// treat all returned entries as potentially including interleaved
    /// notifications.
    pub notify_ambiguity: bool,
}

/// A node in a THREAD response tree (RFC 5256 Section 4).
///
/// Each node represents a message in a thread. A dummy parent (`id == None`)
/// is used when the threading algorithm infers a parent that does not
/// correspond to an existing message.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ThreadNode {
    /// UID (or sequence number) of this message, or `None` if this is a
    /// dummy parent (RFC 5256 Section 4).
    pub id: Option<u32>,
    /// Child thread nodes (RFC 5256 Section 4).
    pub children: Vec<Self>,
}

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;
