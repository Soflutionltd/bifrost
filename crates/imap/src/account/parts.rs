//! BODYSTRUCTURE part traversal and the durable IMAP part-handle encoding.
//!
//! Stage one of imap-G1. Nothing here is wired into inventory, hydration, or
//! the blob openers yet, and `AccountCapabilities::blob_range` deliberately
//! stays `BlobRangeSupport::No` until a projection actually mints these
//! handles: a capability that claims a byte path before handles exist is a
//! promise the crate cannot keep.
//!
//! Two pieces:
//!
//! 1. [`walk_bodystructure`] - turns a parsed [`BodyStructure`] into the flat
//!    list of individually fetchable parts, each carrying the facts a FETCH
//!    and a consumer-facing projection need: the IMAP part path, media
//!    type/subtype, transfer encoding (classified AND as the server spelled
//!    it), the server-reported octet size, and whatever identity the server
//!    gave (content-id, content-location, filename). It returns a
//!    [`PartWalk`], not a bare `Vec`, so a caller can never mistake a
//!    depth-limited walk for a complete enumeration.
//! 2. [`encode_part_handle`] / [`decode_part_handle`] - the versioned,
//!    length-prefixed handle string that survives a round trip through a
//!    consumer's storage.
//!
//! The decoded MIME part tree in `bifrost_types::mime` (`ParsedMessage` /
//! `MimePart`) is the *other* half of this story and is not duplicated here:
//! that tree is built from octets we already hold, whereas BODYSTRUCTURE is
//! the server's description of octets we have not fetched. This module reuses
//! `bifrost_types::mime` for every piece of decoding the two share - RFC 2231
//! parameter reassembly, RFC 2047 encoded words, and the `TransferEncoding`
//! vocabulary - so there is one definition of each, not two.
//!
//! # Part numbering rules implemented
//!
//! From RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5. The numbering is
//! defined per *message context*, not per node, which is the distinction a
//! naive tree walk misses:
//!
//! - **Single-part message.** A message whose top-level body is not
//!   `multipart/*` has exactly one part, numbered `1`. `BODY[1]` is its
//!   content and `BODY[]` is the whole message. A walk that numbered the root
//!   node itself, or emitted no number at all, would be wrong here.
//! - **Multipart message.** The `multipart/*` container itself has no part
//!   number in its own right; its children are `1`, `2`, ... A nested
//!   `multipart/*` at path `2` numbers its children `2.1`, `2.2`, ... - so a
//!   container's path is a *prefix* of its children's, never a part number
//!   this module mints a handle for. Container nodes are therefore not
//!   emitted: there is nothing useful to hand a byte-range reader for one.
//! - **`message/rfc822` (and `message/global`) nesting.** A part at path `P`
//!   is itself fetchable (`BODY[P]` yields the encapsulated message including
//!   its headers), so it IS emitted, and then its inner body opens a fresh
//!   message context with prefix `P`: an inner `multipart/*` numbers its
//!   children `P.1`, `P.2`, and an inner *single* part is `P.1` - the
//!   single-part rule applied one level down, which is where the two rules
//!   compose and where implementations most often go wrong.
//!
//! # Shapes tested (`parts_tests.rs`)
//!
//! Real server-shaped structures, not invented ones: a plain `text/plain`
//! message, `multipart/alternative`, `multipart/mixed` with an attachment,
//! `multipart/mixed` wrapping a nested `multipart/alternative`, a nested
//! `message/rfc822` whose inner body is multipart, a nested `message/rfc822`
//! whose inner body is single-part, and `multipart/signed`. Every test pins
//! the exact part paths, because a wrong implementation produces a plausible
//! looking list with subtly wrong numbers. Two further cases pin the nesting
//! bound from both sides: the deepest structure the parser accepts walks
//! complete, and one level deeper is refused by the parser rather than
//! silently shortened by the walk.

use bifrost_types::mime::{TransferEncoding, decode_encoded_words, decode_rfc2231_params};
use bifrost_types::{AccountError, AccountOperation};

use crate::types::body::ContentDisposition;
use crate::types::{BodyStructure, MailboxName};

use super::envelope::malformed;

/// Handle format version. Bump when the field list or separator rules change.
///
/// v3 adds escaped mailbox wire identity; v2 folders decode literally.
/// v2 added the length-prefixed original transfer-encoding token (see
/// [`encode_part_handle`]). v1 handles are refused outright rather than
/// migrated: nothing has ever minted one - this module is unwired and
/// `AccountCapabilities::blob_range` still reports `No` - so there is no
/// stored v1 handle anywhere to keep working, and a default-filled upgrade
/// path would exist only to serve handles that cannot exist.
const PART_HANDLE_VERSION: u32 = 3;
const PART_HANDLE_PREFIX: &str = "imappart";

/// Maximum BODYSTRUCTURE nesting this traversal will descend.
///
/// Deliberately the same number as `MAX_BODY_NESTING_DEPTH` in
/// `crate::codec::decode::bodystructure`, and counted the same way: one level
/// per `multipart/*` container AND one per `message/rfc822`, with the
/// top-level body at depth 0 and the bound applied as `depth > MAX`. That
/// parity is the whole point. The parser REFUSES input nested deeper than
/// this, so for any structure that came off the wire the traversal never
/// reaches the bound - a truncated walk is not something a server can
/// provoke, only a hand-built structure can. The guard is therefore a stack
/// bound for structures that arrived some other way, and when it fires it is
/// reported ([`PartWalk::truncated`]) rather than silently shortening the
/// list.
///
/// The parity is pinned behaviourally in `parts_tests.rs` (a 64-deep chain
/// walks complete, a 65-deep one is refused by the parser) rather than by
/// naming the parser's private constant, so a change to either side fails a
/// test instead of drifting.
const MAX_PART_DEPTH: usize = 64;

/// The IMAP part path a `BODY[...]` section specifier takes, e.g. `1.2.3`.
///
/// Always non-empty: every fetchable part has at least one number. The
/// container-only nodes that have no number of their own are not represented.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PartPath(Vec<u32>);

impl PartPath {
    /// The `BODY[<section>]` section specifier for this part's content.
    pub(crate) fn section(&self) -> String {
        self.0
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Parse a dotted part path. Every component must be an `nz-number`:
    /// IMAP part numbers are 1-based, so a `0` component is not a path this
    /// crate could ever have minted and is refused rather than normalized.
    fn parse(value: &str) -> Option<Self> {
        if value.is_empty() {
            return None;
        }
        let mut numbers = Vec::new();
        for component in value.split('.') {
            let number = component.parse::<u32>().ok()?;
            if number == 0 {
                return None;
            }
            numbers.push(number);
        }
        Some(Self(numbers))
    }
}

/// A part's transfer encoding: this crate's classification AND the token the
/// server actually sent.
///
/// Both halves are kept because neither implies the other. `classified` is
/// what a decoder can act on; `token` is what the server said, and for an
/// encoding this crate does not model (`X-UUENCODE`, and the long tail of
/// `x-` spellings that still appear) it is the ONLY surviving description of
/// how those octets are encoded. Folding such a part to `Unknown` alone
/// destroys it: a consumer could then be told neither how to decode the bytes
/// nor what the server claimed, and no later layer can recover the token
/// without a second BODYSTRUCTURE round trip.
///
/// What a consumer-facing surface does with an unmodelled token - reject,
/// pass through undecoded, expose the spelling - is not decided here. This
/// layer owes the information, not the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartEncoding {
    /// This crate's classification of `token`.
    pub(crate) classified: TransferEncoding,
    /// The `Content-Transfer-Encoding` token the server sent, as this
    /// crate's BODYSTRUCTURE parser reports it.
    ///
    /// That parser ASCII-lowercases the token on the way in, because RFC 2045
    /// Section 5.1 makes it case-insensitive, so this is the server's token
    /// modulo case and nothing else. Case is the only thing not preserved,
    /// and it carries no meaning - `X-UUENCODE` arrives as `x-uuencode`.
    pub(crate) token: String,
}

impl PartEncoding {
    /// Classify a server token while keeping its original spelling.
    fn from_server_token(token: &str) -> Self {
        Self {
            classified: TransferEncoding::from_token(token),
            token: token.to_owned(),
        }
    }

    /// A canonical encoding, for the container nodes and test call sites that
    /// have no server token of their own.
    fn canonical(classified: TransferEncoding) -> Self {
        Self {
            token: encoding_token(classified).to_owned(),
            classified,
        }
    }

    /// True when the server's token is one this crate does not model, i.e.
    /// `token` carries information `classified` does not.
    pub(crate) fn is_unmodelled(&self) -> bool {
        matches!(self.classified, TransferEncoding::Unknown)
    }
}

/// One individually fetchable MIME part, as described by BODYSTRUCTURE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessagePart {
    /// IMAP part path (`BODY[<path>]`).
    pub(crate) path: PartPath,
    /// Lowercased media type, e.g. `text`, `image`, `message`.
    pub(crate) media_type: String,
    /// Lowercased media subtype, e.g. `plain`, `png`, `rfc822`.
    pub(crate) media_subtype: String,
    /// Transfer encoding the octets at `path` are in, classified and verbatim.
    pub(crate) encoding: PartEncoding,
    /// Octet count the *server* reported for the encoded body.
    ///
    /// This is the encoded size, not the decoded size, and it is the server's
    /// claim rather than a measurement; a byte-range reader ranges over these
    /// octets, so it is the right number for that lane and the wrong number
    /// to present as "attachment size" without decoding.
    pub(crate) size: u64,
    /// `Content-ID`, verbatim as the server sent it (angle brackets included
    /// when present - stripping them here would lose the distinction between
    /// a `cid:` reference target and a bare token).
    pub(crate) content_id: Option<String>,
    /// `Content-Location`.
    pub(crate) content_location: Option<String>,
    /// Filename from `Content-Disposition`, falling back to the content-type
    /// `name` parameter. RFC 2231 continuations are reassembled and RFC 2047
    /// encoded words are decoded (servers emit both here in practice).
    pub(crate) filename: Option<String>,
    /// Lowercased disposition type (`inline`, `attachment`), when given.
    pub(crate) disposition: Option<String>,
    /// True for `message/rfc822` / `message/global`: fetching `path` yields a
    /// whole encapsulated message including its headers, and this part's own
    /// children appear later in the list under `path` as a prefix.
    pub(crate) is_embedded_message: bool,
}

/// The result of a traversal: the parts, and whether the list is all of them.
///
/// `truncated` exists so exhaustion is never silent. A caller that builds a
/// projection claiming "these are the message's parts" must not be able to
/// make that claim from a walk that gave up partway; a `Vec` alone cannot
/// tell it apart from a complete one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartWalk {
    /// Fetchable parts in document order.
    pub(crate) parts: Vec<MessagePart>,
    /// True when [`MAX_PART_DEPTH`] stopped the descent, so `parts` is a
    /// prefix of the message's real part list rather than all of it. Always
    /// false for a structure produced by this crate's BODYSTRUCTURE parser,
    /// which refuses deeper input before the traversal ever sees it.
    pub(crate) truncated: bool,
}

/// Flatten a message's BODYSTRUCTURE into its fetchable parts, in document
/// order. `structure` must be the structure of the *whole* message.
pub(crate) fn walk_bodystructure(structure: &BodyStructure) -> PartWalk {
    let mut walk = PartWalk {
        parts: Vec::new(),
        truncated: false,
    };
    walk_message(structure, &[], 0, &mut walk);
    walk
}

/// Number the body of one message context (the top-level message, or the
/// inside of a `message/rfc822`). `prefix` is empty for the top level.
///
/// `depth` is the nesting level of `body` counted exactly as the
/// BODYSTRUCTURE parser counts it, so the two bounds coincide.
fn walk_message(body: &BodyStructure, prefix: &[u32], depth: usize, out: &mut PartWalk) {
    if depth > MAX_PART_DEPTH {
        out.truncated = true;
        return;
    }
    match body {
        // A multipart body's children take the numbers; the container has
        // none of its own.
        BodyStructure::Multipart { bodies, .. } => {
            for (index, child) in bodies.iter().enumerate() {
                walk_part(child, child_path(prefix, index), depth + 1, out);
            }
        }
        // A single-part body IS part 1 of its message context. At the top
        // level that is `1`; inside `message/rfc822` at `2` it is `2.1`.
        single => walk_part(single, child_path(prefix, 0), depth, out),
    }
}

/// Number a node that already has a path of its own.
fn walk_part(body: &BodyStructure, path: Vec<u32>, depth: usize, out: &mut PartWalk) {
    if depth > MAX_PART_DEPTH {
        out.truncated = true;
        return;
    }
    match body {
        BodyStructure::Multipart { bodies, .. } => {
            for (index, child) in bodies.iter().enumerate() {
                walk_part(child, child_path(&path, index), depth + 1, out);
            }
        }
        BodyStructure::Message { body: inner, .. } => {
            out.parts.push(leaf(body, PartPath(path.clone())));
            // A fresh message context: the encapsulated message renumbers
            // from this part's path.
            walk_message(inner, &path, depth + 1, out);
        }
        _ => out.parts.push(leaf(body, PartPath(path))),
    }
}

fn child_path(prefix: &[u32], index: usize) -> Vec<u32> {
    let mut path = prefix.to_vec();
    // Saturating rather than wrapping: a structure with more than u32::MAX
    // siblings cannot be addressed by IMAP part numbers at all, and a
    // wrapped number would silently name a different part.
    path.push(u32::try_from(index).unwrap_or(u32::MAX).saturating_add(1));
    path
}

fn leaf(body: &BodyStructure, path: PartPath) -> MessagePart {
    let (media_type, media_subtype, params, id, encoding, size, disposition, location) = match body
    {
        BodyStructure::Basic {
            media_type,
            media_subtype,
            params,
            id,
            encoding,
            size,
            disposition,
            location,
            ..
        } => (
            media_type.to_ascii_lowercase(),
            media_subtype.to_ascii_lowercase(),
            params,
            id,
            encoding,
            *size,
            disposition,
            location,
        ),
        BodyStructure::Text {
            media_subtype,
            params,
            id,
            encoding,
            size,
            disposition,
            location,
            ..
        } => (
            "text".to_owned(),
            media_subtype.to_ascii_lowercase(),
            params,
            id,
            encoding,
            *size,
            disposition,
            location,
        ),
        BodyStructure::Message {
            media_subtype,
            params,
            id,
            encoding,
            size,
            disposition,
            location,
            ..
        } => (
            "message".to_owned(),
            media_subtype.to_ascii_lowercase(),
            params,
            id,
            encoding,
            *size,
            disposition,
            location,
        ),
        // Unreachable: `walk_part` never routes a container here. Kept
        // total rather than panicking - a wrong list beats a torn stream.
        BodyStructure::Multipart { media_subtype, .. } => {
            return MessagePart {
                path,
                media_type: "multipart".to_owned(),
                media_subtype: media_subtype.to_ascii_lowercase(),
                encoding: PartEncoding::canonical(TransferEncoding::SevenBit),
                size: 0,
                content_id: None,
                content_location: None,
                filename: None,
                disposition: None,
                is_embedded_message: false,
            };
        }
    };
    MessagePart {
        is_embedded_message: media_type == "message"
            && (media_subtype == "rfc822" || media_subtype == "global"),
        path,
        encoding: PartEncoding::from_server_token(encoding),
        size,
        content_id: id.clone(),
        content_location: location.clone(),
        filename: filename_of(disposition.as_ref(), params),
        disposition: disposition
            .as_ref()
            .map(|d| d.disposition_type.to_ascii_lowercase()),
        media_type,
        media_subtype,
    }
}

/// `Content-Disposition` `filename`, else the content-type `name` parameter.
///
/// Both are matched case-insensitively (servers echo the sender's spelling),
/// reassembled through RFC 2231 continuation handling, and passed through
/// RFC 2047 decoding: `name*0*=`-style continuations and raw `=?utf-8?B?...?=`
/// filenames both appear in the wild, and neither is decoded by the
/// BODYSTRUCTURE parser, which faithfully reports what the server sent.
fn filename_of(
    disposition: Option<&ContentDisposition>,
    params: &[(String, String)],
) -> Option<String> {
    let from_disposition = disposition
        .map(|d| decode_rfc2231_params(&d.params))
        .and_then(|decoded| lookup(&decoded, "filename"));
    from_disposition
        .or_else(|| lookup(&decode_rfc2231_params(params), "name"))
        .map(|value| decode_encoded_words(value.as_bytes()))
        .filter(|value| !value.is_empty())
}

fn lookup(params: &[(String, String)], key: &str) -> Option<String> {
    params
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.clone())
}

/// A decoded part handle: everything needed to re-issue the FETCH.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedPartHandle {
    pub(crate) folder: MailboxName,
    pub(crate) uidvalidity: u32,
    pub(crate) uid: u32,
    pub(crate) path: PartPath,
    /// Classification plus the server's original token, both as they were at
    /// mint time.
    pub(crate) encoding: PartEncoding,
}

impl DecodedPartHandle {
    /// Refuse a handle minted under a different mailbox epoch.
    ///
    /// A UIDVALIDITY change renumbers the mailbox, so the UID in a handle
    /// minted before it names a message that no longer exists - and the same
    /// UID very likely names a *different* message now. The handle carries
    /// UIDVALIDITY precisely so this is detectable at redemption time
    /// instead of quietly streaming the wrong message's bytes.
    pub(crate) fn verify_uidvalidity(
        &self,
        live: u32,
        op: AccountOperation,
    ) -> Result<(), AccountError> {
        if self.uidvalidity == live {
            Ok(())
        } else {
            Err(malformed(
                "IMAP part handle was minted under a different UIDVALIDITY",
                op,
            ))
        }
    }
}

/// Encode a durable part handle.
///
/// Format:
/// `imappart<version>:<folder-len>:<folder>:<uidvalidity>:<uid>:<path>:<encoding>:<token-len>:<token>`
///
/// Shape notes, all load-bearing:
///
/// - The **version rides in the prefix token**, not in a field, so a decoder
///   can tell "a part handle of a version I do not know" apart from "not a
///   part handle" before it has parsed anything else.
/// - The **folder is length-prefixed** rather than escaped, matching
///   `encode_object_id`. IMAP mailbox names are arbitrary strings and may
///   contain `:`; a split-on-separator decode of an escaped form is exactly
///   the kind of thing that misreads one mailbox as another.
/// - **UIDVALIDITY is present** so redemption can refuse a stale epoch
///   rather than fetching whatever UID means today.
/// - The **transfer encoding is carried** so a range reader knows what the
///   octets are without a second BODYSTRUCTURE round trip. It is stored as a
///   canonical token; a server encoding this crate does not model becomes
///   `unknown`, which means "hand the octets back undecoded" - never a guess.
/// - The **server's original encoding token rides alongside it**, so
///   `unknown` is not the end of the information. It is length-prefixed for
///   the same reason the folder is: BODYSTRUCTURE reports the token as a
///   quoted string, so a hostile or merely odd server can put `:` in it, and
///   a split-on-separator decode would then read one field as another.
pub(crate) fn encode_part_handle(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: u32,
    path: &PartPath,
    encoding: &PartEncoding,
) -> String {
    format!(
        "{PART_HANDLE_PREFIX}{PART_HANDLE_VERSION}:{}:{}:{uidvalidity}:{uid}:{}:{}:{}:{}",
        folder.account_id().len(),
        folder.account_id(),
        path.section(),
        encoding_token(encoding.classified),
        encoding.token.len(),
        encoding.token,
    )
}

/// Decode a part handle, refusing anything it does not recognise.
///
/// An unrecognised version is refused, never reinterpreted: a v2 handle
/// parsed under v1 field rules would produce a well-formed-looking handle
/// naming the wrong bytes, which is worse than an error.
pub(crate) fn decode_part_handle(
    value: &str,
    op: AccountOperation,
) -> Result<DecodedPartHandle, AccountError> {
    let rest = value
        .strip_prefix(PART_HANDLE_PREFIX)
        .ok_or_else(|| malformed("invalid IMAP part handle prefix", op))?;
    let (version, rest) = rest
        .split_once(':')
        .ok_or_else(|| malformed("invalid IMAP part handle prefix", op))?;
    let version = version
        .parse::<u32>()
        .map_err(|_| malformed("invalid IMAP part handle version", op))?;
    if version != 2 && version != PART_HANDLE_VERSION {
        return Err(malformed("unsupported IMAP part handle version", op));
    }

    let (folder, rest) = length_prefixed(rest, "IMAP part handle folder", op)?;
    let folder = if version == 2 {
        MailboxName::new(folder)
    } else {
        MailboxName::from_account_id(folder)
    }
    .map_err(|e| malformed(&e.to_string(), op))?;
    let rest = rest.ok_or_else(|| malformed("invalid IMAP part handle separator", op))?;

    let (uidvalidity, rest) = split_field(rest, "IMAP part handle uidvalidity", op)?;
    let uidvalidity = nonzero(uidvalidity, "IMAP part handle uidvalidity", op)?;
    let (uid, rest) = split_field(rest, "IMAP part handle uid", op)?;
    let uid = nonzero(uid, "IMAP part handle uid", op)?;
    let (path, rest) = split_field(rest, "IMAP part handle path", op)?;
    let path =
        PartPath::parse(path).ok_or_else(|| malformed("invalid IMAP part handle path", op))?;
    let (classified, rest) = split_field(rest, "IMAP part handle encoding", op)?;
    let classified = encoding_from_token(classified)
        .ok_or_else(|| malformed("invalid IMAP part handle encoding", op))?;

    let (token, rest) = length_prefixed(rest, "IMAP part handle encoding token", op)?;
    if rest.is_some() {
        return Err(malformed("trailing IMAP part handle field", op));
    }
    // The classification and the original token are redundant by
    // construction, so a disagreement means the handle was edited or
    // corrupted - EXCEPT in the one direction that is legitimate: a token
    // this build does not model classifies as `unknown`, and a handle minted
    // by a build that models more encodings than this one would look exactly
    // like that. Checking only the modelled direction refuses corruption
    // ("base64" paired with the token "b64") without refusing a handle a
    // future sibling build legitimately minted.
    if !matches!(classified, TransferEncoding::Unknown)
        && TransferEncoding::from_token(token) != classified
    {
        return Err(malformed(
            "IMAP part handle encoding disagrees with its original token",
            op,
        ));
    }
    Ok(DecodedPartHandle {
        folder,
        uidvalidity,
        uid,
        path,
        encoding: PartEncoding {
            classified,
            token: token.to_owned(),
        },
    })
}

/// Split one `:`-terminated field off the front. The field itself may not
/// contain `:`; the two that can (folder, encoding token) are length-prefixed
/// instead.
fn split_field<'a>(
    rest: &'a str,
    detail: &str,
    op: AccountOperation,
) -> Result<(&'a str, &'a str), AccountError> {
    rest.split_once(':').ok_or_else(|| malformed(detail, op))
}

/// Read a `<len>:<value>` field, returning the value and whatever follows the
/// separator after it (`None` when the value ends the string).
fn length_prefixed<'a>(
    rest: &'a str,
    detail: &str,
    op: AccountOperation,
) -> Result<(&'a str, Option<&'a str>), AccountError> {
    let (len, rest) = rest.split_once(':').ok_or_else(|| malformed(detail, op))?;
    let len = len.parse::<usize>().map_err(|_| malformed(detail, op))?;
    let value = rest.get(..len).ok_or_else(|| malformed(detail, op))?;
    let tail = rest.get(len..).ok_or_else(|| malformed(detail, op))?;
    if tail.is_empty() {
        Ok((value, None))
    } else {
        let tail = tail
            .strip_prefix(':')
            .ok_or_else(|| malformed(detail, op))?;
        Ok((value, Some(tail)))
    }
}

fn nonzero(value: &str, detail: &str, op: AccountOperation) -> Result<u32, AccountError> {
    let parsed = value.parse::<u32>().map_err(|_| malformed(detail, op))?;
    if parsed == 0 {
        return Err(malformed(detail, op));
    }
    Ok(parsed)
}

/// Canonical wire token per encoding.
///
/// `TransferEncoding` is `#[non_exhaustive]`, so this match CANNOT be
/// exhaustive from here and the compiler will not force a decision when a
/// variant is added upstream: a new variant silently takes the `_` arm and
/// encodes as `unknown`. Nothing in this module can make that a compile
/// error, and no test can either - both would just be restating the
/// wildcard. What limits the damage is that the original server token rides
/// in the handle beside this one, so a variant nobody added an arm for
/// degrades the classification without destroying the evidence.
fn encoding_token(encoding: TransferEncoding) -> &'static str {
    match encoding {
        TransferEncoding::SevenBit => "7bit",
        TransferEncoding::EightBit => "8bit",
        TransferEncoding::Binary => "binary",
        TransferEncoding::Base64 => "base64",
        TransferEncoding::QuotedPrintable => "quoted-printable",
        TransferEncoding::Unknown => "unknown",
        _ => "unknown",
    }
}

/// Inverse of [`encoding_token`]. Deliberately NOT
/// `TransferEncoding::from_token`: that helper maps anything it does not
/// recognise to `Unknown`, which for a *handle* would turn a corrupted field
/// into a silently accepted "undecoded octets" instruction.
fn encoding_from_token(token: &str) -> Option<TransferEncoding> {
    Some(match token {
        "7bit" => TransferEncoding::SevenBit,
        "8bit" => TransferEncoding::EightBit,
        "binary" => TransferEncoding::Binary,
        "base64" => TransferEncoding::Base64,
        "quoted-printable" => TransferEncoding::QuotedPrintable,
        "unknown" => TransferEncoding::Unknown,
        _ => return None,
    })
}

#[cfg(test)]
#[path = "parts_tests.rs"]
mod tests;

#[cfg(test)]
mod wire_identity_tests {
    use super::*;

    #[test]
    fn legacy_part_folders_remain_literal() {
        let literal = MailboxName::from_wire("a".into(), b"&AGE-", false).account_id();
        let folder = MailboxName::new(&literal).unwrap();
        let old = format!(
            "imappart2:{}:{literal}:7:9:1:base64:6:base64",
            literal.len()
        );
        let op = AccountOperation::SyncChanges;
        assert_eq!(decode_part_handle(&old, op).unwrap().folder, folder);
        let encoding = PartEncoding {
            classified: TransferEncoding::Base64,
            token: "base64".into(),
        };
        let current = encode_part_handle(&folder, 7, 9, &PartPath(vec![1]), &encoding);
        assert!(current.starts_with("imappart3:"));
        assert_eq!(decode_part_handle(&current, op).unwrap().folder, folder);
    }
}
