//! FETCH response and request types, APPEND helpers, and BINARY extension types.
//!
//! FETCH responses are defined in RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2.
//! FETCH command attributes are defined in RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5.
//! MULTIAPPEND is defined in RFC 3502.
//! BINARY extension is defined in RFC 3516.

use super::{BodyStructure, Envelope, Flag};
use bytes::Bytes;

/// A parsed FETCH response for a single message
/// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct FetchResponse {
    /// Message sequence number (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    pub seq: u32,
    /// `UID` value (RFC 3501 Section 2.3.1.1).
    pub uid: Option<u32>,
    /// `FLAGS` value (RFC 3501 Section 2.3.2).
    pub flags: Option<Vec<Flag>>,
    /// Parsed `ENVELOPE` (RFC 3501 Section 7.4.2).
    pub envelope: Option<Envelope>,
    /// Parsed `BODYSTRUCTURE` (RFC 3501 Section 7.4.2).
    pub body_structure: Option<BodyStructure>,
    /// `RFC822.SIZE` in bytes (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    ///
    /// RFC 9051 widens this from `number` to `number64` to support messages > 4 GiB.
    pub rfc822_size: Option<u64>,
    /// `INTERNALDATE` timestamp string (RFC 3501 Section 7.4.2).
    ///
    /// The date-time string in IMAP format, e.g. `"17-Jul-1996 02:44:25 -0700"`.
    pub internal_date: Option<String>,
    /// Fetched body sections per `BODY[section]<partial>` (RFC 3501 Section 6.4.5).
    pub body_sections: Vec<BodySection>,
    /// `MODSEQ` value (RFC 7162 CONDSTORE Section 3.1.3).
    pub mod_seq: Option<u64>,
    /// `SAVEDATE` timestamp string (RFC 8514 Section 3).
    ///
    /// The date-time when the message was saved to the mailbox, or `None` if
    /// the server returned NIL (message predates SAVEDATE tracking).
    pub save_date: Option<String>,
    /// `BINARY[section]` data items (RFC 3516 Section 4.2).
    pub binary_sections: Vec<BinarySection>,
    /// `BINARY.SIZE[section]` values (RFC 3516 Section 4.3 / RFC 9051 Section 7.5.2).
    ///
    /// Each entry is `(section_parts, size_in_bytes)`.
    /// RFC 9051 Section 9 defines the size as `number` (u32), but stored as
    /// `u64` for Postel's-law leniency with servers that send larger values.
    pub binary_sizes: Vec<(Vec<u32>, u64)>,
    /// `PREVIEW` text (RFC 8970 Section 3).
    ///
    /// A short plaintext snippet of the message, or `None` if the server
    /// returned NIL (e.g., for LAZY requests where the preview is not yet computed).
    pub preview: Option<String>,
    /// `EMAILID` value (RFC 8474 Section 4).
    ///
    /// A server-assigned unique identifier for this particular instance of a message.
    /// The value is an `objectid` string (1-255 alphanumeric/dash/underscore characters).
    pub email_id: Option<String>,
    /// `THREADID` value (RFC 8474 Section 4).
    ///
    /// A server-assigned identifier grouping related messages into threads,
    /// or `None` if the server returned NIL (no thread association).
    pub thread_id: Option<String>,
    /// Gmail `X-GM-MSGID` value.
    ///
    /// Gmail assigns this stable unsigned 64-bit identifier to each message.
    pub gmail_msg_id: Option<u64>,
    /// Gmail `X-GM-THRID` value.
    ///
    /// Gmail assigns this stable unsigned 64-bit identifier to each thread.
    pub gmail_thread_id: Option<u64>,
    /// Gmail `X-GM-LABELS` values.
    ///
    /// These are exposed as the server returned them. The generic IMAP
    /// account layer does not assign Gmail-specific label semantics.
    pub gmail_labels: Option<Vec<String>>,
}

/// A single fetched BINARY section (RFC 3516 Section 4.2).
///
/// Returned as `BINARY[section]<origin>` in FETCH responses.
/// The content has been decoded from its Content-Transfer-Encoding
/// (RFC 3516 Section 4.2).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct BinarySection {
    /// The numeric section path (e.g. `[1, 2, 3]` for `BINARY[1.2.3]`).
    pub section: Vec<u32>,
    /// Byte offset if this was a partial fetch (`<origin>` per RFC 3516 Section 4.2).
    ///
    /// RFC 9051 Section 7.5.2 widens this from `number` (u32) to `number64` (u64)
    /// to support messages > 4 GiB.
    pub origin: Option<u64>,
    /// The decoded binary data, or `None` if the server returned NIL.
    pub data: Option<Bytes>,
}

/// A single fetched body section (RFC 3501 Section 6.4.5).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct BodySection {
    /// The section specification (e.g. `"HEADER"`, `"1.2"`, `"TEXT"`).
    pub section: String,
    /// Byte offset if this was a partial fetch (`<origin.count>`).
    ///
    /// RFC 9051 Section 7.5.2 widens this from `number` (u32) to `number64` (u64)
    /// to support messages > 4 GiB.
    pub origin: Option<u64>,
    /// The raw body data, or `None` if the server returned NIL.
    pub data: Option<Bytes>,
}

/// FETCH item attributes the client can request
/// (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FetchAttr {
    /// UID (RFC 3501 Section 2.3.1.1).
    Uid,
    /// FLAGS (RFC 3501 Section 2.3.2).
    Flags,
    /// ENVELOPE (RFC 3501 Section 7.4.2).
    Envelope,
    /// BODYSTRUCTURE (RFC 3501 Section 7.4.2).
    BodyStructure,
    /// RFC822.SIZE (RFC 3501 Section 7.4.2).
    Rfc822Size,
    /// INTERNALDATE (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
    InternalDate,
    /// RFC822  -  functionally equivalent to `BODY[]` (RFC 3501 Section 6.4.5).
    Rfc822,
    /// RFC822.HEADER  -  functionally equivalent to `BODY.PEEK[HEADER]` (RFC 3501 Section 6.4.5).
    Rfc822Header,
    /// RFC822.TEXT  -  functionally equivalent to `BODY[TEXT]` (RFC 3501 Section 6.4.5).
    Rfc822Text,
    /// `BODY.PEEK[section]<partial>` or `BODY[section]<partial>`
    /// (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
    ///
    /// RFC 9051 widens partial ranges from `number` to `number64` / `nz-number64`.
    BodySection {
        peek: bool,
        section: Option<String>,
        partial: Option<(u64, u64)>,
    },
    /// MODSEQ (RFC 7162 CONDSTORE Section 3.1.3).
    ModSeq,
    /// SAVEDATE (RFC 8514 Section 3).
    ///
    /// Returns the date-time the message was saved to the mailbox.
    /// Value is a quoted date-time string (same format as INTERNALDATE) or NIL.
    SaveDate,
    /// `BINARY[section]<partial>` or `BINARY.PEEK[section]<partial>`
    /// (RFC 3516 Section 4.5.1).
    ///
    /// Fetches the content of a MIME part, decoded from its
    /// Content-Transfer-Encoding. The section is a dot-separated list of
    /// part numbers (e.g. `[1, 2]` for section `1.2`).
    Binary {
        /// Whether to use `BINARY.PEEK` (no `\Seen` flag set) or `BINARY`.
        peek: bool,
        /// Numeric section path (e.g. `[1, 2, 3]`).
        section: Vec<u32>,
        /// Optional partial range `(offset, count)`.
        ///
        /// RFC 9051 widens partial ranges from `number` to `number64` / `nz-number64`.
        partial: Option<(u64, u64)>,
    },
    /// `BINARY.SIZE[section]` (RFC 3516 Section 4.5.2).
    ///
    /// Returns the decoded size in bytes of a MIME part.
    BinarySize {
        /// Numeric section path (e.g. `[1, 2, 3]`).
        section: Vec<u32>,
    },
    /// `PREVIEW` (RFC 8970 Section 3).
    ///
    /// Returns a short plaintext snippet of the message. The server generates
    /// the preview text from the message body.
    Preview,
    /// `PREVIEW (LAZY)` (RFC 8970 Section 3).
    ///
    /// Like `Preview`, but allows the server to return NIL if the preview
    /// is not yet computed, avoiding delays.
    PreviewLazy,
    /// `EMAILID` (RFC 8474 Section 4).
    ///
    /// Returns the server-assigned unique identifier for this message instance.
    EmailId,
    /// `THREADID` (RFC 8474 Section 4).
    ///
    /// Returns the server-assigned thread identifier, or NIL if no thread association.
    ThreadId,
    /// Gmail `X-GM-MSGID`.
    GmailMsgId,
    /// Gmail `X-GM-THRID`.
    GmailThreadId,
    /// Gmail `X-GM-LABELS`.
    GmailLabels,
}

impl FetchAttr {
    /// Serialize this attribute to its IMAP wire representation
    /// (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
    pub fn to_imap_string(&self) -> String {
        use std::fmt::Write;
        match self {
            Self::Uid => "UID".into(),
            Self::Flags => "FLAGS".into(),
            Self::Envelope => "ENVELOPE".into(),
            Self::BodyStructure => "BODYSTRUCTURE".into(),
            Self::Rfc822Size => "RFC822.SIZE".into(),
            Self::InternalDate => "INTERNALDATE".into(),
            Self::Rfc822 => "RFC822".into(),
            Self::Rfc822Header => "RFC822.HEADER".into(),
            Self::Rfc822Text => "RFC822.TEXT".into(),
            Self::BodySection {
                peek,
                section,
                partial,
            } => {
                // RFC 3501 Section 6.4.5: BODY[<section>]<<partial>>
                // or BODY.PEEK[<section>]<<partial>>.
                let mut s = if *peek {
                    "BODY.PEEK[".to_owned()
                } else {
                    "BODY[".to_owned()
                };
                if let Some(sec) = section {
                    s.push_str(sec);
                }
                s.push(']');
                if let Some((offset, count)) = partial {
                    // RFC 3501 Section 6.4.5: partial range <offset.count>.
                    let _ = write!(s, "<{offset}.{count}>");
                }
                s
            }
            Self::ModSeq => "MODSEQ".into(),
            Self::SaveDate => "SAVEDATE".into(),
            Self::Binary {
                peek,
                section,
                partial,
            } => {
                // RFC 3516 Section 4.5.1: BINARY[section]<partial>
                // or BINARY.PEEK[section]<partial>.
                let mut s = if *peek {
                    "BINARY.PEEK[".to_owned()
                } else {
                    "BINARY[".to_owned()
                };
                let sec_str: Vec<String> = section
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect();
                s.push_str(&sec_str.join("."));
                s.push(']');
                if let Some((offset, count)) = partial {
                    // RFC 3516 Section 4.5.1: partial range <offset.count>.
                    let _ = write!(s, "<{offset}.{count}>");
                }
                s
            }
            Self::BinarySize { section } => {
                // RFC 3516 Section 4.5.2: BINARY.SIZE[section].
                let sec_str: Vec<String> = section
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect();
                format!("BINARY.SIZE[{}]", sec_str.join("."))
            }
            Self::Preview => "PREVIEW".into(),
            Self::PreviewLazy => "PREVIEW (LAZY)".into(),
            Self::EmailId => "EMAILID".into(),
            Self::ThreadId => "THREADID".into(),
            Self::GmailMsgId => "X-GM-MSGID".into(),
            Self::GmailThreadId => "X-GM-THRID".into(),
            Self::GmailLabels => "X-GM-LABELS".into(),
        }
    }
}

/// Serialize a list of fetch attributes to the parenthesized IMAP wire format
/// (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
///
/// A single attribute is serialized without parentheses; multiple attributes
/// are joined with spaces inside parentheses.
pub(crate) fn format_fetch_attrs(attrs: &[FetchAttr]) -> String {
    if attrs.len() == 1 {
        attrs[0].to_imap_string()
    } else {
        let items: Vec<String> = attrs.iter().map(FetchAttr::to_imap_string).collect();
        format!("({})", items.join(" "))
    }
}

/// How flags should be modified in a STORE command
/// (RFC 3501 Section 6.4.6 / RFC 9051 Section 6.4.6).
///
/// RFC 3501 Section 6.4.6 defines:
/// `store-att-flags = (["+" / "-"] "FLAGS" [".SILENT"]) SP (flag-list / NIL)`
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreOperation {
    /// `+FLAGS`  -  add flags.
    Add,
    /// `-FLAGS`  -  remove flags.
    Remove,
    /// `FLAGS`  -  replace all flags.
    Replace,
    /// `+FLAGS.SILENT`  -  add flags, suppress implicit FETCH response
    /// (RFC 3501 Section 6.4.6).
    AddSilent,
    /// `-FLAGS.SILENT`  -  remove flags, suppress implicit FETCH response
    /// (RFC 3501 Section 6.4.6).
    RemoveSilent,
    /// `FLAGS.SILENT`  -  replace all flags, suppress implicit FETCH response
    /// (RFC 3501 Section 6.4.6).
    ReplaceSilent,
}

/// Result of a STORE or UID STORE command (RFC 3501 Section 6.4.6, RFC 7162 Section 3.1.3).
///
/// When UNCHANGEDSINCE is used (CONDSTORE), the server may return a
/// `[MODIFIED sequence-set]` response code indicating which messages failed
/// the precondition check (RFC 7162 Section 3.1.3). `STORE` uses message
/// sequence numbers, while `UID STORE` uses UIDs. This struct preserves that
/// information so callers can detect partial failures.
///
/// Its public visibility was questioned and kept: the raw connection surface
/// that produces it (`uid_store`) is crate-private, so no external consumer can
/// reach a call that returns one or needs to name the type. Nothing is leaked
/// by it being `pub`, and narrowing a published item is not a change to make on
/// a visibility audit's say-so.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct StoreResult {
    /// Implicit FETCH responses with updated flags (RFC 3501 Section 6.4.6).
    ///
    /// Empty when `.SILENT` operations are used.
    pub fetches: Vec<FetchResponse>,
    /// Completion status from the tagged response.
    ///
    /// Always `Ok` for a STORE sent without `UNCHANGEDSINCE` - a tagged
    /// `NO` there is a plain refusal and surfaces as `Error::No`. A
    /// conditional STORE preserves tagged `NO` so callers can attribute a
    /// `[MODIFIED ...]` rejection to the named messages.
    pub status: super::response::StatusKind,
    /// Response code from the tagged response, if any.
    ///
    /// When UNCHANGEDSINCE is used, this will be `Some(ResponseCode::Modified(...))`
    /// for messages that failed the precondition (RFC 7162 Section 3.1.3).
    /// `None` when no response code was returned.
    pub code: Option<super::response::ResponseCode>,
}

/// A message to be appended via MULTIAPPEND (RFC 3502).
///
/// Each message carries its own flags, optional internal date, and raw message data.
/// Multiple `AppendMessage` values are sent in a single APPEND command per
/// RFC 3502 Section 3.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AppendMessage {
    /// Flags to set on the appended message (RFC 3501 Section 6.3.11).
    pub flags: Vec<Flag>,
    /// Optional INTERNALDATE in IMAP date-time format (RFC 3501 Section 6.3.11).
    pub date: Option<String>,
    /// Raw RFC 5322 message data.
    ///
    /// Held as [`bytes::Bytes`] so the driver can put the body on the wire
    /// by reference: the same allocation travels from the caller through the
    /// driver channel to the socket writes without being copied.
    pub data: bytes::Bytes,
}

impl AppendMessage {
    /// Create an append message with only the raw data (RFC 3501 Section 6.3.11).
    ///
    /// Flags default to empty and date defaults to `None`. A `Vec<u8>` or
    /// `String` is taken over without copying; a `Bytes` is shared.
    pub fn new(data: impl Into<bytes::Bytes>) -> Self {
        Self {
            flags: Vec::new(),
            date: None,
            data: data.into(),
        }
    }
}

/// Prints the body's length, never its content. APPEND is an ordinary
/// `Command`, and the driver traces commands with `?cmd`: a derived `Debug`
/// would put whole messages into logs.
impl std::fmt::Debug for AppendMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendMessage")
            .field("flags", &self.flags)
            .field("date", &self.date)
            .field("data_len", &self.data.len())
            .finish()
    }
}

#[cfg(test)]
#[path = "fetch_tests.rs"]
mod tests;
