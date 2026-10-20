//! ENVELOPE + FETCH response parsing (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).

use super::bodystructure::body_structure;
use super::decode_rfc2047;
use super::flags_caps::flag_list;
#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;

/// Preserve a failure from a recognized FETCH attribute through the untagged
/// response alternatives. Unknown attributes stay on the tolerant skip path.
fn recognized_fetch_failure(
    error: nom::Err<nom::error::Error<&[u8]>>,
) -> nom::Err<nom::error::Error<&[u8]>> {
    match error {
        nom::Err::Error(error) | nom::Err::Failure(error) => nom::Err::Failure(error),
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
    }
}

/// Parse a single address: `(name adl mailbox host)` (RFC 3501 Section 7.4.2).
///
/// When `utf8_mode` is true, the display name is raw UTF-8 per RFC 6532 Section 3 /
/// RFC 6855 Section 3. When false, RFC 2047 encoded words in the display name are decoded.
pub(super) fn address(input: &[u8], utf8_mode: bool) -> IResult<&[u8], EnvelopeAddress> {
    let (input, _) = char('(').parse(input)?;
    let (input, name_raw) = nstring(input)?;
    let (input, _) = sp(input)?;
    let (input, adl) = nstring_utf8(input)?;
    let (input, _) = sp(input)?;
    let (input, mailbox) = nstring_utf8(input)?;
    let (input, _) = sp(input)?;
    let (input, host) = nstring_utf8(input)?;
    let (input, _) = char(')').parse(input)?;

    // RFC 6532 Section 3 / RFC 6855 Section 3: skip RFC 2047 decoding when UTF8=ACCEPT is active
    let name = name_raw.map(|v| {
        if utf8_mode {
            String::from_utf8_lossy(&v).into_owned()
        } else {
            decode_rfc2047(&v)
        }
    });

    Ok((
        input,
        EnvelopeAddress {
            name,
            adl,
            mailbox,
            host,
        },
    ))
}

/// Parse an address list: NIL or `(addr1 addr2 ...)` (RFC 3501 Section 7.4.2).
///
/// When `utf8_mode` is true, address display names use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
pub(super) fn address_list(input: &[u8], utf8_mode: bool) -> IResult<&[u8], Vec<EnvelopeAddress>> {
    alt((
        value(vec![], nil_token),
        // RFC 3501 Section 7.4.2: address-list = "(" 1*address ")" / NIL
        // Strictly, the parenthesized form requires at least one address (1*address).
        // However, non-conformant servers (e.g., older Exchange, some webmail backends)
        // may send "()" for an empty address list instead of NIL. Accept per Postel's
        // law (RFC 1122 Section 1.2.2) to avoid losing the entire FETCH/ENVELOPE response.
        delimited(char('('), many0(|i| address(i, utf8_mode)), char(')')),
    ))
    .parse(input)
}

/// Parse an ENVELOPE structure (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
///
/// Ten fields: date, subject, from, sender, reply-to, to, cc, bcc, in-reply-to, message-id.
///
/// When `utf8_mode` is true (UTF8=ACCEPT per RFC 6855 Section 3), the subject and
/// address display names contain raw UTF-8 per RFC 6532 Section 3, and RFC 2047
/// decoding is skipped.
pub(super) fn envelope(input: &[u8], utf8_mode: bool) -> IResult<&[u8], Envelope> {
    let (input, _) = char('(').parse(input)?;
    let (input, date) = nstring_utf8(input)?;
    let (input, _) = sp(input)?;
    // RFC 6532 Section 3 / RFC 6855 Section 3: skip RFC 2047 decoding when UTF8=ACCEPT is active
    let (input, subject_raw) = nstring(input)?;
    let subject = subject_raw.map(|v| {
        if utf8_mode {
            String::from_utf8_lossy(&v).into_owned()
        } else {
            decode_rfc2047(&v)
        }
    });
    let (input, _) = sp(input)?;
    let (input, from) = address_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, sender) = address_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, reply_to) = address_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, to) = address_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, cc) = address_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, bcc) = address_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, in_reply_to) = nstring_utf8(input)?;
    let (input, _) = sp(input)?;
    let (input, message_id) = nstring_utf8(input)?;
    let (input, _) = char(')').parse(input)?;

    // RFC 3501 Section 7.4.2: "If the Sender or Reply-To lines are absent
    // in the [RFC-2822] header, or are present but empty, the server sets
    // the corresponding member of the envelope to be the same value as the
    // from member (the client is not expected to know to do this)."
    //
    // Some servers violate this by sending NIL for sender/reply-to even when
    // from is populated. We apply the default client-side so consumers always
    // see the RFC-mandated invariant: sender and reply-to are never empty
    // when from is populated.
    let sender = if sender.is_empty() && !from.is_empty() {
        // RFC 3501 Section 7.4.2: default sender to from when absent.
        from.clone()
    } else {
        sender
    };
    let reply_to = if reply_to.is_empty() && !from.is_empty() {
        // RFC 3501 Section 7.4.2: default reply-to to from when absent.
        from.clone()
    } else {
        reply_to
    };

    Ok((
        input,
        Envelope {
            date,
            subject,
            from,
            sender,
            reply_to,
            to,
            cc,
            bcc,
            in_reply_to,
            message_id,
        },
    ))
}

/// Parse the parenthesized content of a FETCH response (RFC 3501 Section 7.4.2).
///
/// Called after `* <n> FETCH ` is consumed.
/// When `utf8_mode` is true, ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
pub(super) fn fetch_response_inner(input: &[u8], utf8_mode: bool) -> IResult<&[u8], FetchResponse> {
    let (input, _) = char('(').parse(input)?;
    let mut fr = FetchResponse::default();
    let mut input = input;

    loop {
        // Skip whitespace between attributes
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input)?;
        input = rest;

        if input.first() == Some(&b')') {
            input = &input[1..];
            break;
        }

        // Parse attribute name, stopping at '[' so BODY[section] splits correctly.
        let (rest, attr_name) = fetch_attr_atom(input)?;
        let upper = String::from_utf8_lossy(attr_name).to_ascii_uppercase();

        input = rest;
        let parsed = fetch_attr_value(&upper, input, utf8_mode, &mut fr);
        let (rest, ()) = parsed?;
        input = rest;
    }
    Ok((input, fr))
}

/// Whether a FETCH attribute's failed parse must survive as a connection-fatal
/// recognized response failure.
///
/// Fully modelled attributes always return true. For the three open-ended
/// bodies, only a malformed fixed-arity prefix returns true: their extension
/// tail remains tolerant so an unmodelled but conformant shape cannot make the
/// driver close the connection. Unrecognized attributes remain false and use
/// the forward-compatible skip path.
fn has_closed_grammar(upper: &str, input: &[u8]) -> bool {
    match upper {
        "UID" | "FLAGS" | "RFC822.SIZE" | "RFC822" | "RFC822.HEADER" | "RFC822.TEXT"
        | "INTERNALDATE" | "MODSEQ" | "SAVEDATE" | "PREVIEW" | "EMAILID" | "THREADID"
        | "X-GM-MSGID" | "X-GM-THRID" | "X-GM-LABELS" => true,
        "ENVELOPE" => envelope_fixed_prefix_is_malformed(input),
        "BODYSTRUCTURE" => bodystructure_fixed_prefix_is_malformed(input),
        // Sectioned forms are `section-spec` plus an `nstring` / `number`; the
        // bare `BODY` form is a BODYSTRUCTURE synonym with the same prefix
        // gate as BODYSTRUCTURE.
        "BINARY.SIZE" | "BINARY" => input.first() == Some(&b'['),
        "BODY" if input.first() == Some(&b'[') => true,
        "BODY" => bodystructure_fixed_prefix_is_malformed(input),
        _ => false,
    }
}

/// Check ENVELOPE's closed ten-field outer shape without requiring every
/// address-list variation to fit our typed decoder. This closes truncated and
/// unbalanced envelopes while keeping unfamiliar but balanced address forms
/// on the tolerant path.
fn envelope_fixed_prefix_is_malformed(input: &[u8]) -> bool {
    let Ok((mut input, _)) = attr_sp(input) else {
        return true;
    };
    let Some(rest) = input.strip_prefix(b"(") else {
        return true;
    };
    input = rest;

    for field in 0..10 {
        let Ok((rest, ())) = skip_fetch_value(input) else {
            return true;
        };
        input = rest;
        if field < 9 {
            let Ok((rest, _)) = attr_sp(input) else {
                return true;
            };
            input = rest;
        }
    }
    !input.starts_with(b")")
}

/// Check the BODYSTRUCTURE fixed prefix. A single part has media type/subtype,
/// params, id, description, encoding, and size. A multipart has one or more
/// child bodies followed by its subtype, and every child is checked with the
/// same rule recursively, so an arity violation such as `("TEXT")` is caught
/// at any depth. Extension tails retain their tolerance: only the fixed-arity
/// prefix of each body is required, never the tail contents.
///
/// The outer structure must also close. Extension tails are open-ended in
/// their *contents*, not in their framing: a truncated
/// `BODYSTRUCTURE ("IMAGE" "PNG" NIL NIL NIL "BASE64" 5000` has no shape a
/// conformant server can produce, so requiring the balanced close costs no
/// tolerance and removes the last top-level path on which a malformed body
/// could be laundered into `Unknown` and dropped.
fn bodystructure_fixed_prefix_is_malformed(input: &[u8]) -> bool {
    let Ok((input, _)) = attr_sp(input) else {
        return true;
    };
    body_group_is_malformed(input, 0)
}

/// Nesting bound for the recursive body-prefix check. Matches the typed
/// decoder's own depth cap; anything deeper is a shape the typed parser
/// refuses on its own terms, and the prefix check stays tolerant rather than
/// promoting a depth limit of ours into a provider contract violation.
const BODY_PREFIX_MAX_DEPTH: u32 = 64;

/// Check one parenthesized body group: balanced framing plus the fixed-arity
/// prefix of whichever body form it opens with. `input` must start at `(`.
fn body_group_is_malformed(input: &[u8], depth: u32) -> bool {
    if depth > BODY_PREFIX_MAX_DEPTH {
        return false;
    }
    if skip_paren_group(input).is_err() {
        return true;
    }
    let Some(mut input) = input.strip_prefix(b"(") else {
        return true;
    };
    while input.first() == Some(&b' ') {
        input = &input[1..];
    }
    if input.first() == Some(&b'(') {
        return multipart_fixed_prefix_is_malformed(input, depth);
    }

    for _ in 0..6 {
        let Ok((rest, ())) = skip_fetch_value(input) else {
            return true;
        };
        let Ok((rest, _)) = attr_sp(rest) else {
            return true;
        };
        input = rest;
    }
    number64(input).is_err()
}

/// Check a multipart body's prefix: one or more child bodies, each satisfying
/// [`body_group_is_malformed`] recursively, followed by the subtype. Only the
/// fixed-arity prefix of each child is required; a child whose extension tail
/// this typed decoder does not support remains connection-safe.
fn multipart_fixed_prefix_is_malformed(mut input: &[u8], depth: u32) -> bool {
    let mut child_count = 0;
    loop {
        if input.first() != Some(&b'(') {
            break;
        }
        if body_group_is_malformed(input, depth + 1) {
            return true;
        }
        let Ok((rest, _)) = skip_paren_group(input) else {
            return true;
        };
        child_count += 1;
        input = rest;

        if input.first() == Some(&b'(') {
            continue;
        }
        let Ok((rest, _)) = attr_sp(input) else {
            return true;
        };
        if rest.first() == Some(&b'(') {
            input = rest;
            continue;
        }
        return skip_fetch_value(rest).is_err();
    }
    child_count == 0
}

/// Parse one `X-GM-LABELS` label.
///
/// Google's IMAP extension documentation shows system labels on the wire as
/// bare backslash-prefixed atoms (`(\Inbox \Sent Important)`), which the
/// `astring` grammar cannot express: `\` is a `quoted-special` and therefore
/// excluded from `ATOM-CHAR`. Since this attribute is gated strict, parsing
/// labels as plain `astring` would make an ordinary Gmail FETCH response
/// connection-fatal. User labels arrive as normal astrings, quoted when they
/// contain spaces, in the negotiated mailbox encoding.
fn gmail_label(input: &[u8], utf8_mode: bool) -> IResult<&[u8], String> {
    alt((
        map((char('\\'), atom), |(_, name)| {
            format!("\\{}", String::from_utf8_lossy(name))
        }),
        map(astring, |label| {
            // Labels follow the negotiated mailbox encoding, including literal ampersands.
            decode_mailbox_from_wire(&label, utf8_mode)
                .as_str()
                .to_owned()
        }),
    ))
    .parse(input)
}

/// Parse an `X-GM-MSGID` / `X-GM-THRID` value.
///
/// Gmail sends these as bare unsigned 64-bit decimals. Some proxies in front
/// of Gmail quote them; both attributes are gated strict, so the quoted form
/// is accepted rather than dropping the connection over a shape that carries
/// the same value.
fn gmail_id(input: &[u8]) -> IResult<&[u8], u64> {
    alt((number64, delimited(char('"'), number64, char('"')))).parse(input)
}

/// Separator before a FETCH attribute value.
///
/// The grammar says a single SP, but the attribute loop already tolerates
/// multi-space runs between attributes, and since a failure inside a modelled
/// attribute is now hard (see [`has_closed_grammar`]) the tolerance has to be
/// uniform: a server padding with two spaces must not cost the connection.
fn attr_sp(input: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while1(|b: u8| b == b' ').parse(input)
}

/// Parse one FETCH attribute value into `fr`, given the uppercased attribute
/// name already consumed from `input`.
#[allow(clippy::too_many_lines)]
fn fetch_attr_value<'a>(
    upper: &str,
    input: &'a [u8],
    utf8_mode: bool,
    fr: &mut FetchResponse,
) -> IResult<&'a [u8], ()> {
    let strict = has_closed_grammar(upper, input);
    let parsed = fetch_attr_value_inner(upper, input, utf8_mode, fr);
    if strict {
        // Keep this decision at the value boundary. `alt` may otherwise turn
        // an error in a recognised response into `Unknown`, which the driver
        // treats as unsolicited rather than connection-fatal.
        parsed.map_err(recognized_fetch_failure)
    } else {
        parsed
    }
}

/// Parse one FETCH attribute value after [`fetch_attr_value`] has selected
/// the strictness policy for this attribute.
#[allow(clippy::too_many_lines)]
fn fetch_attr_value_inner<'a>(
    upper: &str,
    input: &'a [u8],
    utf8_mode: bool,
    fr: &mut FetchResponse,
) -> IResult<&'a [u8], ()> {
    match upper {
        "UID" => {
            // uniqueid = nz-number (RFC 3501 Section 9)
            let (rest, _) = attr_sp(input)?;
            let (rest, n) = nz_number(rest)?;
            fr.uid = Some(n);
            Ok((rest, ()))
        }
        "FLAGS" => {
            let (rest, _) = attr_sp(input)?;
            // RFC 3501 Section 9: msg-att-dynamic uses flag-fetch (= flag / "\Recent"),
            // which has the same parsing rules as flag-list (both exclude `\*`).
            let (rest, flags) = flag_list(rest)?;
            fr.flags = Some(flags);
            Ok((rest, ()))
        }
        "ENVELOPE" => {
            let (rest, _) = attr_sp(input)?;
            let (rest, env) = envelope(rest, utf8_mode)?;
            fr.envelope = Some(env);
            Ok((rest, ()))
        }
        "BODYSTRUCTURE" => {
            let (rest, _) = attr_sp(input)?;
            let (rest, bs) = body_structure(rest, utf8_mode, 0)?;
            fr.body_structure = Some(bs);
            Ok((rest, ()))
        }
        "RFC822.SIZE" => {
            // RFC 9051 Section 9: "RFC822.SIZE" SP number64
            let (rest, _) = attr_sp(input)?;
            let (rest, n) = number64(rest)?;
            fr.rfc822_size = Some(n);
            Ok((rest, ()))
        }
        "RFC822" => {
            // RFC 3501 Section 7.4.2: RFC822 SP nstring
            // Functionally equivalent to BODY[] (entire message).
            let (rest, _) = attr_sp(input)?;
            let (rest, data) = nstring(rest)?;
            fr.body_sections.push(BodySection {
                section: String::new(),
                origin: None,
                data,
            });
            Ok((rest, ()))
        }
        "RFC822.HEADER" => {
            // RFC 3501 Section 7.4.2: RFC822.HEADER SP nstring
            // Functionally equivalent to BODY[HEADER].
            let (rest, _) = attr_sp(input)?;
            let (rest, data) = nstring(rest)?;
            fr.body_sections.push(BodySection {
                section: "HEADER".to_owned(),
                origin: None,
                data,
            });
            Ok((rest, ()))
        }
        "RFC822.TEXT" => {
            // RFC 3501 Section 7.4.2: RFC822.TEXT SP nstring
            // Functionally equivalent to BODY[TEXT].
            let (rest, _) = attr_sp(input)?;
            let (rest, data) = nstring(rest)?;
            fr.body_sections.push(BodySection {
                section: "TEXT".to_owned(),
                origin: None,
                data,
            });
            Ok((rest, ()))
        }
        "INTERNALDATE" => {
            // RFC 3501 Section 7.4.2: INTERNALDATE is formally `date-time`
            // (always a quoted string). However, some servers send NIL for
            // malformed or draft messages. Accept NIL per Postel's law
            // (RFC 1122 Section 1.2.2) rather than failing the entire FETCH
            // response and losing all other attributes (UID, FLAGS, etc.).
            let (rest, _) = attr_sp(input)?;
            let (rest, date_val) = nstring_utf8(rest)?;
            fr.internal_date = date_val;
            Ok((rest, ()))
        }
        "MODSEQ" => {
            // RFC 7162 Section 3.1.3 formally says mod-sequence-value >= 1, but we
            // accept 0 per Postel's law (RFC 1122 Section 1.2.2) for consistency
            // with the STATUS HIGHESTMODSEQ parser which also accepts 0. Real
            // servers may send 0 as a sentinel for new/empty mailboxes.
            // If the value is malformed (e.g., overflow), skip this attribute
            // gracefully rather than losing ALL other FETCH attributes for this
            // message.
            let (rest, _) = attr_sp(input)?;
            let (rest, _) = char('(').parse(rest)?;
            if let Ok((rest, n)) = number64(rest) {
                let (rest, _) = char(')').parse(rest)?;
                fr.mod_seq = Some(n);
                Ok((rest, ()))
            } else {
                // Malformed MODSEQ value  -  skip past the closing ')'.
                let (rest, _) = take_while(|b: u8| b != b')').parse(rest)?;
                let (rest, _) = char(')').parse(rest)?;
                Ok((rest, ()))
            }
        }
        "SAVEDATE" => {
            // SAVEDATE is a quoted date-time string or NIL (RFC 8514 Section 3).
            // Same format as INTERNALDATE when present.
            let (rest, _) = attr_sp(input)?;
            let (rest, val) = nstring_utf8(rest)?;
            fr.save_date = val;
            Ok((rest, ()))
        }
        "PREVIEW" => {
            // PREVIEW is a nstring (quoted string or NIL) per RFC 8970 Section 3.
            let (rest, _) = attr_sp(input)?;
            let (rest, val) = nstring_utf8(rest)?;
            fr.preview = val;
            Ok((rest, ()))
        }
        "EMAILID" => {
            // RFC 8474 Section 7: EMAILID is always "(objectid)", never NIL.
            // fetch-emailid-resp = "EMAILID" SP "(" objectid ")"
            // objectid = 1*255(ALPHA / DIGIT / "_" / "-") per RFC 8474 Section 7.
            let (rest, _) = attr_sp(input)?;
            let (rest, _) = char('(').parse(rest)?;
            let (rest, val) =
                map(objectid, |a| String::from_utf8_lossy(a).into_owned()).parse(rest)?;
            let (rest, _) = char(')').parse(rest)?;
            fr.email_id = Some(val);
            Ok((rest, ()))
        }
        "THREADID" => {
            // THREADID (objectid-val) or THREADID NIL per RFC 8474 Section 4.
            // objectid = 1*255(ALPHA / DIGIT / "_" / "-") per RFC 8474 Section 7.
            let (rest, _) = attr_sp(input)?;
            // Use opt(terminated(..., peek(...))) to handle all case variants of
            // NIL while verifying a token boundary follows (any atom-special
            // or EOF). Plain `tag_no_case(&b"NIL"[..])` would greedily match the
            // prefix of atoms like "NIL2", corrupting the parse
            // (RFC 3501 Section 9).
            let (rest, nil_match) = opt(terminated(
                tag_no_case(&b"NIL"[..]),
                peek(alt((
                    // Any atom-special byte (RFC 3501 Section 9):
                    //   atom-specials = "(" / ")" / "{" / SP / CTL /
                    //                   list-wildcards / quoted-specials /
                    //                   resp-specials
                    value((), verify(take(1u8), |b: &[u8]| !is_atom_char(b[0]))),
                    value((), eof),
                ))),
            ))
            .parse(rest)?;
            if nil_match.is_some() {
                fr.thread_id = None;
                Ok((rest, ()))
            } else {
                let (rest, _) = char('(').parse(rest)?;
                let (rest, val) =
                    map(objectid, |a| String::from_utf8_lossy(a).into_owned()).parse(rest)?;
                let (rest, _) = char(')').parse(rest)?;
                fr.thread_id = Some(val);
                Ok((rest, ()))
            }
        }
        "X-GM-MSGID" => {
            let (rest, _) = attr_sp(input)?;
            let (rest, value) = gmail_id(rest)?;
            fr.gmail_msg_id = Some(value);
            Ok((rest, ()))
        }
        "X-GM-THRID" => {
            let (rest, _) = attr_sp(input)?;
            let (rest, value) = gmail_id(rest)?;
            fr.gmail_thread_id = Some(value);
            Ok((rest, ()))
        }
        "X-GM-LABELS" => {
            let (rest, _) = attr_sp(input)?;
            let (rest, labels) = delimited(
                char('('),
                separated_list0(attr_sp, |i| gmail_label(i, utf8_mode)),
                char(')'),
            )
            .parse(rest)?;
            fr.gmail_labels = Some(labels);
            Ok((rest, ()))
        }
        "BINARY.SIZE" if input.first() == Some(&b'[') => {
            // BINARY.SIZE[section]  -  RFC 9051 Section 7.5.2 / Section 9 formal
            // syntax specifies `number` (u32), but we accept number64 as a
            // Postel's-law leniency for servers that may exceed u32 range.
            let (rest, section_parts) = binary_section_spec(input)?;
            let (rest, _) = attr_sp(rest)?;
            let (rest, size) = number64(rest)?;
            fr.binary_sizes.push((section_parts, size));
            Ok((rest, ()))
        }
        "BINARY" if input.first() == Some(&b'[') => {
            // BINARY[section]<origin> nstring (RFC 3516 Section 4.2)
            let (rest, (section_parts, origin)) = binary_section_with_origin(input)?;
            let (rest, _) = attr_sp(rest)?;
            let (rest, data) = nstring(rest)?;
            fr.binary_sections.push(BinarySection {
                section: section_parts,
                origin,
                data,
            });
            Ok((rest, ()))
        }
        "BODY" if input.first() == Some(&b'[') => {
            // BODY[section]<origin> nstring (RFC 3501 Section 7.4.2)
            let (rest, section) = body_section_spec(input)?;
            let (rest, _) = attr_sp(rest)?;
            let (rest, data) = nstring(rest)?;
            fr.body_sections.push(BodySection {
                section: section.0,
                origin: section.1,
                data,
            });
            Ok((rest, ()))
        }
        "BODY" => {
            // BODY without [section]  -  BODYSTRUCTURE synonym
            let (rest, _) = attr_sp(input)?;
            let (rest, bs) = body_structure(rest, utf8_mode, 0)?;
            fr.body_structure = Some(bs);
            Ok((rest, ()))
        }
        _ => {
            // Unknown attribute  -  skip value. Try to skip past space + next value.
            // This is a best-effort approach for forward compatibility.
            let (rest, _) = attr_sp(input)?;
            let (rest, ()) = skip_fetch_value(rest)?;
            Ok((rest, ()))
        }
    }
}

/// Parse `[section]<origin>` from a BODY fetch attribute (RFC 3501 Section 7.4.2).
///
/// Returns a tuple of (section string, optional origin offset).
/// RFC 9051 Section 9 defines the response origin as `number` (u32), but we
/// accept `number64` (u64) as a Postel's-law leniency for large offsets.
fn body_section_spec(input: &[u8]) -> IResult<&[u8], (String, Option<u64>)> {
    let (input, _) = char('[').parse(input)?;
    let (input, section_bytes) = scan_section_spec(input)?;
    let section = String::from_utf8_lossy(section_bytes).into_owned();
    let (input, _) = char(']').parse(input)?;
    let (input, origin) = opt(delimited(char('<'), number64, char('>'))).parse(input)?;
    Ok((input, (section, origin)))
}

/// Scan the contents of a section spec `[...]`, stopping at the unquoted,
/// unparenthesized `]` that closes the section (RFC 3501 Section 7.4.2).
///
/// Per the grammar:
/// ```text
/// section-spec       = section-msgpart / section-text
/// section-text       = section-msgpart-text / "MIME"
/// section-msgpart-text = "HEADER" / "HEADER.FIELDS" [".NOT"] SP header-list / "TEXT"
/// header-list        = "(" header-fld-name *(SP header-fld-name) ")"
/// header-fld-name    = astring
/// astring            = 1*ASTRING-CHAR / string
/// ASTRING-CHAR       = ATOM-CHAR / resp-specials
/// resp-specials      = "]"
/// ```
///
/// Because `astring` includes `resp-specials` (`]`), a `]` can appear inside:
/// - A quoted string: `"X]Field"` (RFC 3501 Section 9)
/// - A literal string: `{8}\r\nX]Field\r\n` (RFC 3501 Section 9)
/// - An unquoted astring: bare `]` as an ASTRING-CHAR (inside parens of header-list)
///
/// This scanner handles all three cases so the section closer is found correctly.
/// Modeled after [`skip_balanced_parens`].
pub(super) fn scan_section_spec(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let start = input;
    let mut pos = 0;
    let mut paren_depth: u32 = 0;

    while pos < input.len() {
        match input[pos] {
            // CRLF cannot appear in a section spec outside a literal. Stop
            // here so malformed quoted syntax cannot absorb the next line.
            b'\r' | b'\n' => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    &input[pos..],
                    nom::error::ErrorKind::CrLf,
                )));
            }
            // RFC 3501 Section 7.4.2: `]` at paren depth 0 is the section closer.
            b']' if paren_depth == 0 => {
                return Ok((&input[pos..], &start[..pos]));
            }
            // Track parenthesized header-list groups (RFC 3501 Section 7.4.2).
            b'(' => {
                paren_depth += 1;
                pos += 1;
            }
            b')' if paren_depth > 0 => {
                paren_depth -= 1;
                pos += 1;
            }
            // Skip quoted strings so `]` inside them is not treated as the
            // section closer (RFC 3501 Section 9: quoted = DQUOTE *QUOTED-CHAR DQUOTE).
            b'"' => {
                pos += 1; // skip opening quote
                while pos < input.len() && input[pos] != b'"' {
                    // RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF,
                    // escaped or not. A raw CR/LF means the quote is
                    // unterminated  -  stop so the scan cannot run into the
                    // next response.
                    if input[pos] == b'\r' || input[pos] == b'\n' {
                        break;
                    }
                    if input[pos] == b'\\' && pos + 1 < input.len() {
                        // Guard against escapes consuming CR/LF:
                        // RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
                        // A backslash before CR/LF is malformed  -  break out
                        // so the scanner does not skip past the response-
                        // terminating CRLF.
                        if input[pos + 1] == b'\r' || input[pos + 1] == b'\n' {
                            break;
                        }
                        // RFC 3501 Section 9: quoted-specials = DQUOTE / "\"
                        pos += 2; // skip backslash + escaped char
                    } else {
                        pos += 1;
                    }
                }
                if input.get(pos) == Some(&b'"') {
                    pos += 1; // skip closing quote
                }
                // Otherwise the quote is unterminated and `pos` still points at
                // the CR/LF, which the outer terminator arm rejects.
            }
            // Handle literal8 prefix: ~{n}\r\n<n bytes> (RFC 6855 Section 4).
            b'~' if pos + 1 < input.len() && input[pos + 1] == b'{' => {
                pos += 1; // skip '~', fall through to '{' on next iteration
            }
            // Handle literal: {n}\r\n<n bytes> (RFC 3501 Section 9)
            // and literal+: {n+}\r\n<n bytes> (RFC 7888).
            b'{' => {
                pos += 1; // skip '{'
                let digit_start = pos;
                while pos < input.len() && input[pos].is_ascii_digit() {
                    pos += 1;
                }
                if pos > digit_start && pos < input.len() {
                    let count_end = pos;
                    // Skip optional '+' for LITERAL+ (RFC 7888)
                    if input[pos] == b'+' {
                        pos += 1;
                    }
                    if pos < input.len() && input[pos] == b'}' {
                        pos += 1; // skip '}'
                        // Skip CRLF after '}'
                        if pos + 1 < input.len() && input[pos] == b'\r' && input[pos + 1] == b'\n' {
                            pos += 2;
                            // Parse the byte count and skip that many bytes
                            // (RFC 3501 Section 9: literal = "{" number "}" CRLF *CHAR8).
                            // Use checked_add to prevent overflow, and bounds-check
                            // to avoid reading past the end of input.
                            if let Ok(s) = std::str::from_utf8(&input[digit_start..count_end])
                                && let Ok(count) = s.parse::<usize>()
                                && let Some(new_pos) = pos.checked_add(count)
                                && new_pos <= input.len()
                            {
                                pos = new_pos;
                            }
                        }
                    }
                }
                // If literal parsing failed, continue (already past '{')
            }
            _ => {
                pos += 1;
            }
        }
    }

    // Complete-mode: all input available  -  unterminated section is an error,
    // not "need more data" (RFC 3501 Section 7.4.2).
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Char,
    )))
}

/// Parse `[section]` for BINARY / BINARY.SIZE attributes.
///
/// RFC 9051 Section 9 defines:
///   `section-binary = "[" [section-part] "]"`
/// where `section-part` is OPTIONAL (note the brackets around `section-part`).
/// An empty section `[]` is valid and produces an empty `Vec<u32>`.
///
/// When present, `section-part = nz-number *("." nz-number)` (RFC 3501 Section 9).
pub(super) fn binary_section_spec(input: &[u8]) -> IResult<&[u8], Vec<u32>> {
    let (input, _) = char('[').parse(input)?;
    // RFC 9051 Section 9: section-part is optional inside the brackets.
    // Use separated_list0 so that `[]` yields an empty vec.
    let (input, parts) = separated_list0(char('.'), nz_number).parse(input)?;
    let (input, _) = char(']').parse(input)?;
    Ok((input, parts))
}

/// Parse `[section]<origin>` for a BINARY fetch attribute (RFC 3516 Section 4.2,
/// RFC 9051 Section 9).
///
/// The section is an optional dot-separated list of part numbers (e.g. `[1.2.3]`
/// or `[]` for the top-level message). Returns `(section_parts, optional_origin)`.
/// RFC 9051 Section 9 defines the response origin as `number` (u32), but we
/// accept `number64` (u64) as a Postel's-law leniency for large offsets.
pub(super) fn binary_section_with_origin(input: &[u8]) -> IResult<&[u8], (Vec<u32>, Option<u64>)> {
    let (input, parts) = binary_section_spec(input)?;
    let (input, origin) = opt(delimited(char('<'), number64, char('>'))).parse(input)?;
    Ok((input, (parts, origin)))
}

/// Skip a non-parenthesized extension value (RFC 9051 Section 9).
///
/// Handles all forms that `tagged-ext-simple` may take:
/// - `NIL`
/// - Literal (`{N}\r\n<N bytes>`, RFC 9051 Section 4)
/// - Quoted string (`"..."`, RFC 9051 Section 4)
/// - Atom/number (any run of non-delimiter bytes)
///
/// The `terminator` predicate identifies bytes that mark the end of the atom
/// fallback (e.g. SP and CR for ESEARCH, SP and `)` for STATUS). CR and LF end
/// the atom scan regardless of what the caller's predicate says: they close the
/// response line, so a scan that crossed them would read into whatever response
/// is buffered behind this one. Every sibling skip helper here guards the same
/// way.
///
/// Parenthesized groups are NOT handled here  -  callers must check for `(`
/// before invoking this function.
pub(super) fn skip_tagged_ext_simple<F>(terminator: F) -> impl FnMut(&[u8]) -> IResult<&[u8], ()>
where
    F: Fn(u8) -> bool + Copy,
{
    move |input: &[u8]| {
        alt((
            // NIL  -  must verify a token boundary follows (terminator byte or
            // end-of-input) so that atoms starting with "NIL" (e.g. "NILSIMSA")
            // fall through to the atom branch instead of being partially consumed
            // (RFC 9051 Section 4, tagged-ext-simple ABNF).
            map(
                terminated(
                    tag_no_case(&b"NIL"[..]),
                    peek(alt((
                        value((), verify(take(1u8), move |b: &[u8]| terminator(b[0]))),
                        value((), eof),
                    ))),
                ),
                |_| (),
            ),
            // Literal: {N}\r\n<N bytes> (RFC 9051 Section 4)
            map(literal, |_| ()),
            // Quoted string: "..." (RFC 9051 Section 4)
            map(quoted_string, |_| ()),
            // Atom / number / sequence-set  -  consume until a terminator byte
            // or the end of the response line.
            map(
                take_while1(move |b: u8| !terminator(b) && b != b'\r' && b != b'\n'),
                |_| (),
            ),
        ))
        .parse(input)
    }
}

/// Skip over a single FETCH attribute value (used for unknown attributes) (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
fn skip_fetch_value(input: &[u8]) -> IResult<&[u8], ()> {
    alt((
        // Parenthesized list  -  skip matching parens
        map(skip_paren_group, |_| ()),
        // NIL  -  boundary-checked to avoid partial match on atoms like "NILSIMSA"
        map(nil_token, |_| ()),
        // Literal
        map(literal, |_| ()),
        // Quoted string
        map(quoted_string, |_| ()),
        // Number or atom
        map(
            take_while1(|b: u8| b != b' ' && b != b')' && b != b'\r'),
            |_| (),
        ),
    ))
    .parse(input)
}

/// Skip a parenthesized group, handling nesting, quoted strings, and literals.
///
/// Used by the FETCH parser to skip unknown attributes
/// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
pub(super) fn skip_paren_group(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, _) = char('(').parse(input)?;
    let mut depth: u32 = 1;
    let mut i = 0;
    while i < input.len() && depth > 0 {
        match input[i] {
            // A response terminator is invalid in a parenthesized value
            // unless it belongs to a literal, which is skipped below.
            b'\r' | b'\n' => break,
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'"' => {
                // Skip quoted string contents.
                i += 1;
                while i < input.len() && input[i] != b'"' {
                    // RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF,
                    // escaped or not. A raw CR/LF means the quote is
                    // unterminated  -  stop so the scan cannot run into the
                    // next response.
                    if input[i] == b'\r' || input[i] == b'\n' {
                        break;
                    }
                    if input[i] == b'\\' {
                        // Guard against escapes consuming CR/LF:
                        // RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
                        // A backslash before CR/LF is malformed  -  break out
                        // so the outer loop can handle the line ending.
                        if i + 1 >= input.len() {
                            break;
                        }
                        if input[i + 1] == b'\r' || input[i + 1] == b'\n' {
                            break;
                        }
                        i += 1; // skip escaped char
                    }
                    i += 1;
                }
                // If the quote was never closed  -  end of input, or a raw
                // CR/LF  -  bail out with `depth` still non-zero so the
                // unclosed-paren check below turns this into an error.
                if input.get(i) != Some(&b'"') {
                    break;
                }
            }
            // Handle literal8 prefix: ~{n}\r\n<n bytes> (RFC 6855 Section 4)
            b'~' if i + 1 < input.len() && input[i + 1] == b'{' => {
                i += 1; // skip '~', fall through to '{' on next iteration
                continue;
            }
            b'{' => {
                // Handle literal: {n}\r\n<n bytes> (RFC 3501 Section 9)
                // and literal+: {n+}\r\n<n bytes> (RFC 7888)
                let start = i + 1;
                let mut end = start;
                while end < input.len() && input[end].is_ascii_digit() {
                    end += 1;
                }
                if end > start && end < input.len() {
                    let count_end = end;
                    // Skip optional '+' for LITERAL+ (RFC 7888)
                    if input[end] == b'+' {
                        end += 1;
                    }
                    if end < input.len() && input[end] == b'}' {
                        end += 1; // skip '}'
                        // Skip CRLF after '}'
                        if end + 1 < input.len() && input[end] == b'\r' && input[end + 1] == b'\n' {
                            end += 2;
                            // Parse the byte count and skip that many bytes.
                            // Use checked_add to prevent wrapping on crafted counts
                            // near usize::MAX (RFC 3501 Section 9 / RFC 9051 Section 9).
                            if let Ok(s) = std::str::from_utf8(&input[start..count_end])
                                && let Ok(count) = s.parse::<usize>()
                            {
                                match end.checked_add(count) {
                                    Some(new_end) if new_end <= input.len() => {
                                        i = new_end;
                                        continue;
                                    }
                                    _ => {
                                        // Literal body exceeds available data or
                                        // overflows usize  -  stop scanning to avoid
                                        // misinterpreting literal body bytes as
                                        // parenthesized structure
                                        // (RFC 3501 Section 9 / RFC 9051 Section 9).
                                        return Err(nom::Err::Error(nom::error::Error::new(
                                            input,
                                            nom::error::ErrorKind::Eof,
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
                // If literal parsing failed, just continue past '{'
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        // Complete-mode: all input available  -  unclosed parens is an error,
        // not "need more data" (RFC 3501 Section 7.4.2).
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Char,
        )));
    }
    Ok((&input[i..], &input[..i - 1]))
}

#[cfg(test)]
mod label_encoding_tests {
    use super::gmail_label;

    #[test]
    fn gmail_labels_follow_negotiated_mailbox_encoding() {
        assert_eq!(gmail_label(b"\"&AGE-\"", false).unwrap().1, "a");
        assert_eq!(gmail_label(b"\"&AGE-\"", true).unwrap().1, "&AGE-");
        assert_eq!(gmail_label("\"café\"".as_bytes(), true).unwrap().1, "café");
        for utf8 in [false, true] {
            assert_eq!(gmail_label(b"\\Inbox", utf8).unwrap().1, "\\Inbox");
        }
    }
}
