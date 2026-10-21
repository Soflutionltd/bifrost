//! nom-based IMAP response parser.
//!
//! Parses raw `&[u8]` into [`Response`] types. Must handle:
//! - Both `IMAP4rev1` (RFC 3501) and `IMAP4rev2` (RFC 9051) grammar
//! - Non-ASCII bytes (0x01-0xFF) in quoted strings and text
//! - RFC 2047 encoded words in ENVELOPE fields
//! - Raw UTF-8 in ENVELOPE fields when UTF8=ACCEPT is active (RFC 6532, RFC 6855)
//! - literal8 syntax `~{n}\r\n<data>` (RFC 6855 Section 4)
//! - Malformed responses from non-conformant servers
//!
//! # Wire format reference
//!
//! ```text
//! response        = *(continue-req / response-data) response-done
//! response-data   = "*" SP (resp-cond-state / resp-cond-bye /
//!                   mailbox-data / message-data / capability-data) CRLF
//! response-done   = response-tagged / response-fatal
//! response-tagged = tag SP resp-cond-state CRLF
//! continue-req    = "+" SP (resp-text / base64) CRLF
//! ```

use bytes::Bytes;
use nom::IResult;
use nom::Parser;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take, take_while, take_while1};
use nom::character::complete::{char, digit1};
use nom::combinator::{eof, map, opt, peek, value, verify};
use nom::multi::{many0, separated_list0, separated_list1};
use nom::sequence::{delimited, preceded, terminated};

use crate::types::body::{BodyStructure, ContentDisposition};
use crate::types::envelope::{Envelope, EnvelopeAddress};
use crate::types::fetch::{BinarySection, BodySection, FetchResponse};
use crate::types::flag::Flag;
use crate::types::mailbox::{MailboxAttribute, MailboxInfo, StatusItem};
use crate::types::response::{
    AclEntry, Capability, ContinuationRequest, EsearchResponse, GreetingResponse, GreetingStatus,
    MetadataEntry, NamespaceDescriptor, QuotaResource, Response, ResponseCode, StatusKind,
    TaggedResponse, ThreadNode, UidRange, UntaggedResponse, UntaggedStatus,
};
use crate::types::validated::MailboxName;

pub(crate) mod bodystructure;
mod envelope_fetch;
mod extensions;
mod flags_caps;
mod mailbox;
mod namespace_response;
mod primitives;
mod response;
mod search_response;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

// Re-export pub(crate) items that were visible from the original module.
pub(crate) use bifrost_types::mime::decode_encoded_words as decode_rfc2047;

// Re-export sub-module items so they are visible within this module's namespace
// (required for tests and cross-module function calls via `use super::*`).
#[allow(unused_imports)]
pub(super) use bodystructure::*;
#[allow(unused_imports)]
pub(super) use envelope_fetch::*;
#[allow(unused_imports)]
pub(super) use extensions::*;
#[allow(unused_imports)]
pub(super) use flags_caps::*;
#[allow(unused_imports)]
pub(super) use mailbox::*;
#[allow(unused_imports)]
pub(super) use namespace_response::*;
#[allow(unused_imports)]
pub(super) use primitives::*;
#[allow(unused_imports)]
pub(super) use response::*;
#[allow(unused_imports)]
pub(super) use search_response::*;

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Parse a single complete IMAP response line (including CRLF).
///
/// Handles tagged, untagged, and continuation responses per
/// RFC 3501 Section 7 / RFC 9051 Section 7.
///
/// Equivalent to `parse_response_utf8(input, false)`  -  RFC 2047 decoding is applied.
#[cfg(test)]
pub(crate) fn parse_response(input: &[u8]) -> IResult<&[u8], Response> {
    parse_response_utf8(input, false)
}

/// Parse a single IMAP response line, with UTF-8 mode for RFC 6532/6855.
///
/// When `utf8_mode` is `true` (UTF8=ACCEPT enabled per RFC 6855 Section 3), ENVELOPE
/// fields contain raw UTF-8 (RFC 6532 Section 3) and RFC 2047 decoding is skipped.
pub(crate) fn parse_response_utf8(input: &[u8], utf8_mode: bool) -> IResult<&[u8], Response> {
    parse_response_with_backing(input, utf8_mode, None)
}

/// Parse one response backed by immutable `Bytes`, allowing FETCH literals to
/// retain slices of the reader's response buffer.
pub(crate) fn parse_response_bytes(input: &Bytes, utf8_mode: bool) -> IResult<&[u8], Response> {
    parse_response_with_backing(input.as_ref(), utf8_mode, Some(input))
}

fn parse_response_with_backing<'a>(
    input: &'a [u8],
    utf8_mode: bool,
    backing: Option<&Bytes>,
) -> IResult<&'a [u8], Response> {
    alt((
        map(response::parse_continuation, Response::Continuation),
        map(
            |i| response::parse_untagged_with_backing(i, utf8_mode, backing),
            |u| Response::Untagged(Box::new(u)),
        ),
        map(response::parse_tagged, Response::Tagged),
    ))
    .parse(input)
}

/// Parse the initial server greeting (RFC 3501 Section 7.1).
///
/// Called once on connection. Subsequent `* OK` lines are parsed as
/// `UntaggedResponse::Status` by `parse_response`.
pub(crate) fn parse_greeting(input: &[u8]) -> IResult<&[u8], Response> {
    let (input, _) = tag(&b"* "[..]).parse(input)?;
    let (input, status) = alt((
        value(GreetingStatus::Ok, tag_no_case(&b"OK"[..])),
        value(GreetingStatus::PreAuth, tag_no_case(&b"PREAUTH"[..])),
        value(GreetingStatus::Bye, tag_no_case(&b"BYE"[..])),
    ))
    .parse(input)?;
    // SP is formally required (RFC 3501 Section 7.1) but tolerated as absent
    // for servers that send bare greeting with no resp-text (Postel's law),
    // consistent with parse_tagged and parse_untagged_status.
    let (input, maybe_sp) = opt(primitives::sp).parse(input)?;
    if maybe_sp.is_some() {
        let (input, (code, text)) = flags_caps::resp_text(input)?;
        let (input, _) = primitives::crlf(input)?;
        Ok((
            input,
            Response::Greeting(GreetingResponse { status, code, text }),
        ))
    } else {
        let (input, _) = primitives::crlf(input)?;
        Ok((
            input,
            Response::Greeting(GreetingResponse {
                status,
                code: None,
                text: String::new(),
            }),
        ))
    }
}
