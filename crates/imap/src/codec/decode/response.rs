//! Top-level response parsing (RFC 3501 Section 7 / RFC 9051 Section 7).

use bytes::Bytes;

use super::envelope_fetch::fetch_response_inner_with_backing;
use super::extensions::{
    parse_untagged_acl, parse_untagged_listrights, parse_untagged_metadata,
    parse_untagged_myrights, parse_untagged_quota, parse_untagged_quotaroot, parse_untagged_thread,
    parse_untagged_unknown,
};
use super::flags_caps::{capability_list, flag_list, resp_text, uid_set};
#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;

/// Parse a continuation request: `+ text\r\n` (RFC 3501 Section 7.5 / RFC 9051 Section 7.6).
///
/// RFC 3501 defines `continue-req = "+" SP (resp-text / base64) CRLF`
/// where `resp-text = ["[" resp-text-code "]" SP] text`.
///
/// If the data after `+ ` starts with `[`, we attempt to parse as `[response-code] text`.
/// Base64 SASL challenges never start with `[`, so this is unambiguous.
/// If the bracket parse fails, we fall back to treating everything as plain data.
///
/// RFC 9051 makes resp-text optional (`resp-text = ["[" resp-text-code "]" SP] [text]`),
/// so bare `+\r\n` with no space or text is valid. Many servers also send this form
/// for literal synchronization.
pub(super) fn parse_continuation(input: &[u8]) -> IResult<&[u8], ContinuationRequest> {
    let (input, _) = tag(&b"+"[..]).parse(input)?;
    // RFC 3501/9051 Section 9: continue-req = "+" SP (resp-text / base64) CRLF
    // We intentionally accept bare "+" without SP for compatibility with servers
    // that send "+\r\n" for literal synchronization (common in practice).
    let (input, _) = opt(char(' ')).parse(input)?;

    // RFC 3501 Section 7.5: try to parse resp-text with optional [response-code].
    // If data starts with '[', attempt structured resp-text parse; otherwise plain data.
    let (input, code, data) = if input.first() == Some(&b'[') {
        // Try parsing as resp-text: "[response-code] text"
        if let Ok((rest, (code, text))) = resp_text(input) {
            (rest, code, text)
        } else {
            // Bracket parse failed  -  treat entire remainder as plain data.
            let (rest, data_bytes) = take_while(|b: u8| b != b'\r' && b != b'\n').parse(input)?;
            (rest, None, String::from_utf8_lossy(data_bytes).into_owned())
        }
    } else {
        let (rest, data_bytes) = take_while(|b: u8| b != b'\r' && b != b'\n').parse(input)?;
        (rest, None, String::from_utf8_lossy(data_bytes).into_owned())
    };

    let (input, _) = crlf(input)?;
    Ok((input, ContinuationRequest { code, data }))
}

/// Parse a tagged response: `tag SP status [SP resp-text] CRLF`
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
///
/// The formal grammar requires SP after the status keyword, but many real servers
/// send bare `"tag OK\r\n"` with no trailing text. We tolerate the missing SP
/// per Postel's law (RFC 1122 Section 1.2.2).
pub(super) fn parse_tagged(input: &[u8]) -> IResult<&[u8], TaggedResponse> {
    let (input, tag_bytes) = tag_str(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, status) = alt((
        value(StatusKind::Ok, tag_no_case(&b"OK"[..])),
        value(StatusKind::No, tag_no_case(&b"NO"[..])),
        value(StatusKind::Bad, tag_no_case(&b"BAD"[..])),
    ))
    .parse(input)?;
    // SP is formally required (RFC 3501 Section 7.1) but tolerated as absent
    // for servers that send bare status with no resp-text.
    let (input, maybe_sp) = opt(sp).parse(input)?;
    if maybe_sp.is_some() {
        let (input, (code, text)) = resp_text(input)?;
        let (input, _) = crlf(input)?;
        Ok((
            input,
            TaggedResponse {
                tag: String::from_utf8_lossy(tag_bytes).into_owned(),
                status,
                code,
                text,
            },
        ))
    } else {
        let (input, _) = crlf(input)?;
        Ok((
            input,
            TaggedResponse {
                tag: String::from_utf8_lossy(tag_bytes).into_owned(),
                status,
                code: None,
                text: String::new(),
            },
        ))
    }
}

/// Parse an untagged response: `* SP ...` (RFC 3501 Section 2.2.2 / RFC 9051 Section 2.2.2).
///
/// When `utf8_mode` is true, ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
pub(super) fn parse_untagged(input: &[u8], utf8_mode: bool) -> IResult<&[u8], UntaggedResponse> {
    parse_untagged_with_backing(input, utf8_mode, None)
}

pub(super) fn parse_untagged_with_backing<'a>(
    input: &'a [u8],
    utf8_mode: bool,
    backing: Option<&Bytes>,
) -> IResult<&'a [u8], UntaggedResponse> {
    let (input, _) = tag(&b"* "[..]).parse(input)?;
    alt((
        alt((
            parse_untagged_status,
            |i| parse_untagged_numbered_with_backing(i, utf8_mode, backing),
            parse_untagged_capability,
            parse_untagged_flags,
            |i| parse_untagged_list(i, utf8_mode),
            |i| parse_untagged_lsub(i, utf8_mode),
            parse_untagged_esearch,
            parse_untagged_search,
            parse_untagged_sort,
            |i| parse_untagged_status_mailbox(i, utf8_mode),
        )),
        alt((
            parse_untagged_enabled,
            parse_untagged_vanished,
            parse_untagged_id,
            |i| parse_untagged_namespace(i, utf8_mode),
            |i| parse_untagged_quotaroot(i, utf8_mode),
            parse_untagged_quota,
            |i| parse_untagged_acl(i, utf8_mode),
            |i| parse_untagged_myrights(i, utf8_mode),
            |i| parse_untagged_listrights(i, utf8_mode),
            |i| parse_untagged_metadata(i, utf8_mode),
        )),
        alt((parse_untagged_thread, parse_untagged_unknown)),
    ))
    .parse(input)
}

/// Parse untagged status: `OK/NO/BAD/BYE [code] text` (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
///
/// SP is formally required (RFC 3501 Section 7.1) but tolerated as absent
/// for servers that send bare status with no resp-text (Postel's law),
/// matching the tolerance already present in `parse_tagged`.
pub(super) fn parse_untagged_status(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, status) = alt((
        value(UntaggedStatus::Ok, tag_no_case(&b"OK"[..])),
        value(UntaggedStatus::No, tag_no_case(&b"NO"[..])),
        value(UntaggedStatus::Bad, tag_no_case(&b"BAD"[..])),
        value(UntaggedStatus::Bye, tag_no_case(&b"BYE"[..])),
    ))
    .parse(input)?;
    let (input, maybe_sp) = opt(sp).parse(input)?;
    if maybe_sp.is_some() {
        let (input, (code, text)) = resp_text(input)?;
        let (input, _) = crlf(input)?;
        Ok((input, UntaggedResponse::Status { status, code, text }))
    } else {
        let (input, _) = crlf(input)?;
        Ok((
            input,
            UntaggedResponse::Status {
                status,
                code: None,
                text: String::new(),
            },
        ))
    }
}

/// Parse `* <n> EXISTS/RECENT/EXPUNGE/FETCH` (RFC 3501 Section 7.3 / RFC 9051 Section 7.3).
///
/// EXISTS and RECENT use `number` (zero is valid for empty mailboxes).
/// EXPUNGE and FETCH use `nz-number` per `message-data` (RFC 3501 Section 9).
///
/// When `utf8_mode` is true, FETCH ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
pub(super) fn parse_untagged_numbered(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    parse_untagged_numbered_with_backing(input, utf8_mode, None)
}

fn parse_untagged_numbered_with_backing<'a>(
    input: &'a [u8],
    utf8_mode: bool,
    backing: Option<&Bytes>,
) -> IResult<&'a [u8], UntaggedResponse> {
    let num_start = input; // Save position for leading-zero check
    let (input, n) = number(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;

    // Peek at the keyword to decide which response type we have.
    if input.len() >= 6 && input[..6].eq_ignore_ascii_case(b"EXISTS") {
        // mailbox-data: number SP "EXISTS"  -  zero is valid (RFC 3501 Section 7.3.1)
        let (input, _) = tag_no_case(&b"EXISTS"[..]).parse(input)?;
        // Tolerate trailing whitespace before CRLF (Postel's law).
        let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
        let (input, _) = crlf(input)?;
        return Ok((input, UntaggedResponse::Exists(n)));
    }
    if input.len() >= 6 && input[..6].eq_ignore_ascii_case(b"RECENT") {
        // mailbox-data: number SP "RECENT"  -  zero is valid (RFC 3501 Section 7.3.2)
        let (input, _) = tag_no_case(&b"RECENT"[..]).parse(input)?;
        // Tolerate trailing whitespace before CRLF (Postel's law).
        let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
        let (input, _) = crlf(input)?;
        return Ok((input, UntaggedResponse::Recent(n)));
    }

    // message-data uses nz-number (RFC 3501 Section 9).
    if n == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    // nz-number = digit-nz *DIGIT  -  the first digit must be 1-9 (no leading zeros).
    // Use Failure (not Error) so that `alt` does not fall through to the unknown-response
    // catch-all  -  a leading-zero sequence number like `01` is a hard protocol violation.
    if num_start.first() == Some(&b'0') {
        return Err(nom::Err::Failure(nom::error::Error::new(
            num_start,
            nom::error::ErrorKind::Verify,
        )));
    }

    if input.len() >= 7 && input[..7].eq_ignore_ascii_case(b"EXPUNGE") {
        // message-data: nz-number SP "EXPUNGE" (RFC 3501 Section 7.4.1)
        let (input, _) = tag_no_case(&b"EXPUNGE"[..]).parse(input)?;
        // Tolerate trailing whitespace before CRLF (Postel's law).
        let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
        let (input, _) = crlf(input)?;
        return Ok((input, UntaggedResponse::Expunge(n)));
    }

    // message-data: nz-number SP msg-att (RFC 3501 Section 7.4.2)
    let (input, _) = tag_no_case(&b"FETCH"[..]).parse(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, mut fr) = fetch_response_inner_with_backing(input, utf8_mode, backing)?;
    let (input, _) = crlf(input)?;
    fr.seq = n;
    Ok((input, UntaggedResponse::Fetch(Box::new(fr))))
}

/// Parse `* CAPABILITY ...` (RFC 3501 Section 7.2.1 / RFC 9051 Section 7.2.1).
pub(super) fn parse_untagged_capability(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"CAPABILITY"[..]).parse(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    // RFC 3501 Section 7.2.1: capability-data = "CAPABILITY" *(SP capability)
    let (input, caps) = capability_list(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Capability(caps)))
}

/// Parse `* FLAGS (...)` (RFC 3501 Section 7.2.6 / RFC 9051 Section 7.2.6).
pub(super) fn parse_untagged_flags(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"FLAGS"[..]).parse(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    // RFC 3501 Section 7.2.6: "FLAGS" SP flag-list
    let (input, flags) = flag_list(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Flags(flags)))
}

/// Parse LIST-EXTENDED data block: `"(" extended-item *(SP extended-item) ")"`.
///
/// Returns `(old_name, child_info)` extracted from known extended items.
/// Unknown items are silently skipped (forward compatibility).
///
/// Known items:
/// - `"OLDNAME" "(" mailbox-name ")"` (RFC 9051 Section 6.3.9.7)
/// - `"CHILDINFO" "(" astring *(SP astring) ")"` (RFC 5258 Section 4)
fn parse_list_extended_data(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], (Option<MailboxName>, Vec<String>)> {
    let (mut input, _) = char('(').parse(input)?;
    let mut old_name = None;
    let mut child_info = Vec::new();

    loop {
        // Skip optional whitespace between items.
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input)?;
        input = rest;

        // End of extended data block.
        if input.first() == Some(&b')') {
            input = &input[1..];
            break;
        }

        // Parse extended item tag (an atom like "OLDNAME" or "CHILDINFO").
        let (rest, tag_bytes) = astring_utf8(input)?;
        let (rest, _) = sp(rest)?;
        let tag_upper = tag_bytes.to_ascii_uppercase();

        match tag_upper.as_str() {
            "OLDNAME" => {
                // RFC 9051 Section 6.3.9.7: OLDNAME "(" mailbox-name ")"
                let (rest2, _) = char('(').parse(rest)?;
                let (rest2, name_bytes) = astring(rest2)?;
                let (rest2, _) = char(')').parse(rest2)?;
                // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
                // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
                old_name = Some(decode_mailbox_from_wire(&name_bytes, utf8_mode));
                input = rest2;
            }
            "CHILDINFO" => {
                // RFC 5258 Section 6: childinfo-extended-item =
                //   "CHILDINFO" SP "(" list-select-base-opt-quoted
                //                      *(SP list-select-base-opt-quoted) ")"
                // Formally requires at least one item (1*), but accept empty `()`
                // per Postel's law (RFC 1122 Section 1.2.2)  -  a CHILDINFO with
                // no selection options is semantically equivalent to absent CHILDINFO.
                // Postel's law (RFC 1122 Section 1.2.2): accept one or more
                // spaces between items  -  some non-conformant servers insert
                // extra whitespace.
                let (rest2, _) = char('(').parse(rest)?;
                let (rest2, items) =
                    separated_list0(take_while1(|b: u8| b == b' '), astring_utf8).parse(rest2)?;
                let (rest2, _) = char(')').parse(rest2)?;
                child_info = items;
                input = rest2;
            }
            _ => {
                // Unknown extended item  -  skip its value.
                // Per RFC 5258 Section 6 and RFC 9051 Section 9,
                // tagged-ext-val = tagged-ext-simple / "(" [tagged-ext-comp] ")"
                // tagged-ext-simple = sequence-set / number64
                // So the value may be parenthesized OR a simple token.
                if rest.first() == Some(&b'(') {
                    let (rest2, ()) = skip_parenthesized_block(rest)?;
                    input = rest2;
                } else {
                    // Simple value (number64 or sequence-set): consume until SP or ')'.
                    let (rest2, _) = take_while1(|b: u8| b != b' ' && b != b')').parse(rest)?;
                    input = rest2;
                }
            }
        }
    }

    Ok((input, (old_name, child_info)))
}

/// Parse a `* LIST` or `* LSUB` mailbox response (RFC 3501 Section 7.2.2 / 7.2.3).
///
/// LIST and LSUB share identical syntax; only the keyword and the
/// [`UntaggedResponse`] variant differ.
fn parse_mailbox_list_response<'a, F>(
    input: &'a [u8],
    keyword: &'static [u8],
    utf8_mode: bool,
    wrap: F,
) -> IResult<&'a [u8], UntaggedResponse>
where
    F: FnOnce(MailboxInfo) -> UntaggedResponse,
{
    let (input, _) = tag_no_case(keyword).parse(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    // Attributes
    let (input, attrs) = delimited(
        char('('),
        // Postel's law (RFC 1122 Section 1.2.2): accept one or more spaces
        // between attributes  -  some non-conformant servers insert extra whitespace.
        separated_list0(
            take_while1(|b: u8| b == b' '),
            map(
                alt((
                    map((char('\\'), atom), |(_, a)| {
                        format!("\\{}", String::from_utf8_lossy(a))
                    }),
                    map(atom, |a| String::from_utf8_lossy(a).into_owned()),
                )),
                |s| parse_mailbox_attribute(&s),
            ),
        ),
        char(')'),
    )
    .parse(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    // Delimiter: NIL or single QUOTED-CHAR  -  RFC 3501 Section 9.
    let (input, delimiter) = mailbox_delimiter(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    // Mailbox name  -  decode from wire form (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, name_bytes) = astring(input)?;
    let name = decode_mailbox_from_wire(&name_bytes, utf8_mode);
    // Optional LIST-EXTENDED data: SP "(" ... ")" (RFC 5258 Section 6 / RFC 9051).
    // Examples: ("OLDNAME" ("OldMailbox")), ("CHILDINFO" ("SUBSCRIBED"))
    let (input, ext_data) =
        opt(preceded(sp, |i| parse_list_extended_data(i, utf8_mode))).parse(input)?;
    let (old_name, child_info) = ext_data.unwrap_or_default();
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        wrap(MailboxInfo {
            name,
            delimiter,
            attributes: attrs,
            old_name,
            child_info,
        }),
    ))
}

/// Parse `* LIST (\attrs) "delimiter" "name" [SP mbox-list-extended]`
/// (RFC 3501 Section 7.2.2, RFC 5258 Section 6, RFC 9051 Section 7.2.2).
///
/// The optional `mbox-list-extended` block carries data like OLDNAME (RFC 9051
/// Section 6.3.9.7) or CHILDINFO (RFC 5258 Section 4). It is parsed and
/// discarded for now, but its presence must not cause a parse failure.
pub(super) fn parse_untagged_list(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    parse_mailbox_list_response(input, b"LIST", utf8_mode, UntaggedResponse::List)
}

/// Parse `* LSUB (\attrs) "delimiter" "name"` (RFC 3501 Section 7.2.3).
///
/// Identical format to LIST. LSUB is obsoleted by LIST-EXTENDED (RFC 5258)
/// but still sent by some servers.
pub(super) fn parse_untagged_lsub(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    parse_mailbox_list_response(input, b"LSUB", utf8_mode, UntaggedResponse::Lsub)
}

/// Parse a mailbox-list delimiter: NIL or exactly one QUOTED-CHAR (RFC 3501 Section 9).
///
/// ```text
/// mailbox-list = "(" [mbx-list-flags] ")" SP
///                (DQUOTE QUOTED-CHAR DQUOTE / nil) SP mailbox
/// ```
///
/// Per Postel's law (RFC 3501 Section 9): if a non-conformant server sends a
/// multi-character quoted string, the first character is taken and a warning
/// is logged instead of returning a hard parse error.
fn mailbox_delimiter(input: &[u8]) -> IResult<&[u8], Option<char>> {
    let (rest, delimiter_bytes) =
        alt((value(None, nil_token), map(quoted_string, Some))).parse(input)?;
    match delimiter_bytes {
        None => Ok((rest, None)),
        Some(v) if v.is_empty() => Ok((rest, None)),
        Some(v) => {
            let s = String::from_utf8_lossy(&v);
            let mut chars = s.chars();
            let ch = chars.next();
            if chars.next().is_some() {
                // RFC 3501 Section 9: delimiter should be exactly one QUOTED-CHAR.
                // Postel's law: tolerate multi-character delimiters from
                // non-conformant servers by taking only the first character.
                tracing::warn!(
                    delimiter = %s,
                    "LIST delimiter has multiple characters (RFC 3501 Section 9 \
                     requires exactly one QUOTED-CHAR); taking only the first character"
                );
            }
            Ok((rest, ch))
        }
    }
}

/// Map a string to a `MailboxAttribute` (RFC 3501 Section 7.2.2, RFC 6154).
///
/// Delegates to [`MailboxAttribute::from_imap_str`] for the string-to-variant mapping.
pub(super) fn parse_mailbox_attribute(s: &str) -> MailboxAttribute {
    MailboxAttribute::from_imap_str(s)
}

/// Parse `* STATUS "mailbox" (...)` (RFC 3501 Section 7.2.4).
pub(super) fn parse_untagged_status_mailbox(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"STATUS"[..]).parse(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, mailbox_bytes) = astring(input)?;
    let mailbox = decode_mailbox_from_wire(&mailbox_bytes, utf8_mode);
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, items) = delimited(char('('), status_items, char(')')).parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::MailboxStatus { mailbox, items }))
}

/// Parse STATUS items inside parentheses (RFC 3501 Section 6.3.10).
///
/// Uses a loop instead of `separated_list0` so that unknown attributes from
/// future extensions can be gracefully skipped without failing the parse.
#[allow(clippy::too_many_lines)]
pub(super) fn status_items(input: &[u8]) -> IResult<&[u8], Vec<StatusItem>> {
    let mut items = Vec::new();
    let mut input_loop = input;

    loop {
        // Skip optional leading whitespace
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input_loop)?;
        input_loop = rest;

        // Check for end of parenthesized group
        if input_loop.first() == Some(&b')') || input_loop.is_empty() {
            break;
        }

        let (rest, name) = atom(input_loop)?;
        let upper = String::from_utf8_lossy(name).to_ascii_uppercase();

        match upper.as_str() {
            "MESSAGES" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Messages(val));
                }
                input_loop = rest;
            }
            "RECENT" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Recent(val));
                }
                input_loop = rest;
            }
            "UNSEEN" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Unseen(val));
                }
                input_loop = rest;
            }
            // RFC 9051 Section 9 says nz-number for UIDNEXT and UIDVALIDITY,
            // but we accept 0 per Postel's law (RFC 1122 Section 1.2.2) because
            // non-conformant servers (e.g. some Dovecot configurations) send 0
            // for empty mailboxes.
            "UIDNEXT" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::UidNext(val));
                }
                input_loop = rest;
            }
            "UIDVALIDITY" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::UidValidity(val));
                }
                input_loop = rest;
            }
            // RFC 9051 Section 6.3.11: DELETED is a standard rev2 STATUS item.
            "DELETED" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Deleted(val));
                }
                input_loop = rest;
            }
            "HIGHESTMODSEQ" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number64_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::HighestModSeq(val));
                }
                input_loop = rest;
            }
            "SIZE" => {
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number64_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Size(val));
                }
                input_loop = rest;
            }
            "MAILBOXID" => {
                // RFC 8474 Section 5.1: MAILBOXID (objectid-val)
                // objectid = 1*255(ALPHA / DIGIT / "_" / "-") per RFC 8474 Section 7.
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                let (rest, _) = char('(').parse(rest)?;
                let (rest, val) =
                    map(objectid, |a| String::from_utf8_lossy(a).into_owned()).parse(rest)?;
                let (rest, _) = char(')').parse(rest)?;
                items.push(StatusItem::MailboxId(val));
                input_loop = rest;
            }
            "APPENDLIMIT" => {
                // RFC 7889: APPENDLIMIT in STATUS can be NIL (no limit) or a number.
                // Overflow is silently skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                let (rest, val) = alt((
                    map(nil_token, |_| Some(None::<u64>)),
                    map(number64_tolerant, |opt| opt.map(Some)),
                ))
                .parse(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::AppendLimit(val));
                }
                input_loop = rest;
            }
            "DELETED-STORAGE" => {
                // RFC 9208 Section 3: disk space consumed by deleted messages.
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                let (rest, val) = number64_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::DeletedStorage(val));
                }
                input_loop = rest;
            }
            _ => {
                // RFC 9051 Section9: future extensions may use tagged-ext-val which
                // includes parenthesized forms, quoted strings, and literals.
                // tagged-ext-val = tagged-ext-simple /
                //   "(" [tagged-ext-comp] ")"
                let (rest, _) = take_while1(|b: u8| b == b' ').parse(rest)?;
                if rest.first() == Some(&b'(') {
                    // Parenthesized value  -  skip balanced group.
                    let (rest, ()) = skip_parenthesized_block(rest)?;
                    input_loop = rest;
                } else {
                    // STATUS values are terminated by SP or `)`.
                    let (rest, ()) =
                        super::envelope_fetch::skip_tagged_ext_simple(|b| b == b' ' || b == b')')(
                            rest,
                        )?;
                    input_loop = rest;
                }
            }
        }
    }

    Ok((input_loop, items))
}

/// Parse `* ENABLED cap1 cap2 ...` (RFC 5161 Section 3.2).
pub(super) fn parse_untagged_enabled(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"ENABLED"[..]).parse(input)?;
    // RFC 5161 Section 3.2: "ENABLED" *(SP capability)
    let (input, caps) = many0(preceded(
        take_while1(|b: u8| b == b' '),
        map(atom, |a| String::from_utf8_lossy(a).into_owned()),
    ))
    .parse(input)?;
    // Tolerate trailing whitespace before CRLF (same trailing-SP issue
    // as SEARCH/SORT  -  non-conformant per RFC 5161 Section 3.2 formal
    // syntax, but accepted per Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Enabled(caps)))
}

/// Parse `* VANISHED (EARLIER) known-uids` (RFC 7162 Section 3.2.10).
///
/// ABNF: `expunged-resp = "VANISHED" [SP "(EARLIER)"] SP known-uids`
/// where `known-uids = sequence-set` (RFC 3501 Section 9).
pub(super) fn parse_untagged_vanished(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"VANISHED"[..]).parse(input)?;
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, earlier) = opt(delimited(
        char('('),
        tag_no_case(&b"EARLIER"[..]),
        char(')'),
    ))
    .parse(input)?;
    let input = if earlier.is_some() {
        let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
        input
    } else {
        input
    };
    // RFC 7162 Section 6 ABNF: known-uids = sequence-set
    //   ;; Sequence of UIDs; "*" is not allowed.
    // Despite referencing `sequence-set`, the ABNF comment explicitly prohibits
    // `*`. We therefore use `uid_set` which only accepts `nz-number`.
    let (input, uids) = uid_set(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    // RFC 7162 Section 3.2.10: "VANISHED" [SP "(EARLIER)"] SP known-uids
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        UntaggedResponse::Vanished {
            earlier: earlier.is_some(),
            uids,
        },
    ))
}

/// Parse `* ID (key val ...)` (RFC 2971 Section 3.2).
pub(super) fn parse_untagged_id(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"ID"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    let (input, params) = alt((
        value(vec![], nil_token),
        delimited(
            char('('),
            many0(map(
                (
                    preceded(take_while(|b: u8| b == b' '), string_utf8),
                    preceded(sp, nstring_utf8),
                ),
                |(k, v)| (k, v),
            )),
            char(')'),
        ),
    ))
    .parse(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Id(params)))
}
