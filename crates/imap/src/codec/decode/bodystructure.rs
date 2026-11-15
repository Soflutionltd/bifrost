//! BODYSTRUCTURE parser (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).

use super::decode_rfc2047;
use super::envelope_fetch::envelope;
#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;
use bifrost_types::mime::decode_rfc2231_params;

/// Maximum nesting depth for BODYSTRUCTURE parsing (defense-in-depth).
///
/// RFC 3501 Section 7.4.2 does not specify a maximum nesting depth for
/// multipart bodies, but unbounded recursion is a denial-of-service vector:
/// a crafted response with deeply nested `(`-delimited parts could exhaust
/// the stack.  64 levels is far beyond any legitimate message structure.
const MAX_BODY_NESTING_DEPTH: u32 = 64;

/// Parse BODYSTRUCTURE per RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2.
///
/// Entry point  -  multipart starts with nested `(`, single-part starts with a string.
/// When `utf8_mode` is `WireMode::Utf8`, embedded ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
///
/// `depth` tracks the current nesting level; callers at the top level pass 0.
/// Returns a nom error if `depth` exceeds [`MAX_BODY_NESTING_DEPTH`].
// `pub(crate)`, not `pub(super)`: the account-layer part traversal
// (`account/parts.rs`) tests itself against real server BODYSTRUCTURE bytes
// rather than hand-built enum values, so it needs the real parser.
pub(crate) fn body_structure(
    input: &[u8],
    utf8_mode: crate::types::wire::WireMode,
    depth: u32,
) -> IResult<&[u8], BodyStructure> {
    // Defense-in-depth: reject excessively nested BODYSTRUCTURE to prevent
    // stack overflow from crafted server responses.
    if depth > MAX_BODY_NESTING_DEPTH {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }
    let (input, _) = char('(').parse(input)?;
    // Postel's law (RFC 1122 Section 1.2.2): skip optional whitespace after
    // the opening paren  -  some servers insert spaces before the first child.
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    // If the first char after '(' (and optional whitespace) is another '(', it's multipart
    if input.first() == Some(&b'(') {
        body_type_mpart(input, utf8_mode, depth)
    } else {
        body_type_single(input, utf8_mode, depth)
    }
}

/// Parse a single-part body: text, message/rfc822, or basic (RFC 3501 Section 7.4.2).
///
/// When `utf8_mode` is `WireMode::Utf8`, embedded ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
///
/// `depth` tracks the current nesting level for recursion-depth enforcement.
fn body_type_single(
    input: &[u8],
    utf8_mode: crate::types::wire::WireMode,
    depth: u32,
) -> IResult<&[u8], BodyStructure> {
    // media-type SP media-subtype
    // RFC 2045 Section 5.1: media types are case-insensitive  -  lowercase
    // is the canonical form used in RFC examples (e.g. `text/plain`).
    // RFC 3501 Section 9: `body-fld-media` formally uses `string` (quoted/literal),
    // but some non-conformant servers (older Exchange, Yahoo) send bare atoms
    // (e.g. `TEXT PLAIN` instead of `"TEXT" "PLAIN"`).  Accept both per Postel's law.
    let (input, media_type_raw) = astring(input)?;
    let media_type = String::from_utf8_lossy(&media_type_raw).to_ascii_lowercase();
    let (input, _) = sp(input)?;
    let (input, media_subtype_raw) = astring(input)?;
    let media_subtype = String::from_utf8_lossy(&media_subtype_raw).to_ascii_lowercase();
    let (input, _) = sp(input)?;

    // body-fields: params SP id SP description SP encoding SP size
    let (input, params) = body_params(input)?;
    let (input, _) = sp(input)?;
    let (input, id) = nstring_utf8(input)?;
    let (input, _) = sp(input)?;
    let (input, description_raw) = nstring(input)?;
    // RFC 2045 Section 8: Content-Description is `*text`, so RFC 2047 encoded
    // words may appear (RFC 2047 Section 5, rule 1). Always decode them  -
    // RFC 6855 Section 3.1 only applies UTF-8 direct encoding to FETCH ENVELOPE
    // fields (subject, address display names), not to BODYSTRUCTURE description.
    // `decode_rfc2047` handles plain UTF-8 correctly via `String::from_utf8_lossy`.
    let description = description_raw.map(|v| decode_rfc2047(&v));
    let (input, _) = sp(input)?;
    // RFC 3501 Section 9: `body-fld-enc` formally uses `string`, but accept
    // bare atoms (e.g. `7BIT` instead of `"7BIT"`) for Postel's law compatibility
    // with non-conformant servers.
    let (input, encoding_raw) = astring(input)?;
    // RFC 2045 Section 6: Content-Transfer-Encoding values are not case
    // sensitive  -  lowercase for canonical form per RFC 2045 Section 5.1.
    let encoding = String::from_utf8_lossy(&encoding_raw).to_ascii_lowercase();
    let (input, _) = sp(input)?;
    // RFC 9051 Section 9: body-fld-octets = number (u32), but we accept
    // number64 as Postel's-law leniency for servers with large parts.
    let (input, size) = number64(input)?;

    if media_type == "text" {
        // text/* has an extra `lines` field
        // RFC 9051 Section 9: body-fld-lines = number64
        let (input, _) = sp(input)?;
        let (input, lines) = number64(input)?;
        // Extension data
        let (input, ext) = body_ext_1part(input)?;
        let (input, _) = char(')').parse(input)?;
        Ok((
            input,
            BodyStructure::Text {
                media_subtype,
                params,
                id,
                description,
                encoding,
                size,
                lines,
                md5: ext.0,
                disposition: ext.1,
                language: ext.2,
                location: ext.3,
            },
        ))
    } else if media_type == "message" && (media_subtype == "rfc822" || media_subtype == "global") {
        // message/rfc822 and message/global have: envelope body lines
        // RFC 9051 Section 7.5.2: media-message = DQUOTE "MESSAGE" DQUOTE SP
        //   DQUOTE ("RFC822" / "GLOBAL") DQUOTE
        // RFC 6532 defines MESSAGE/GLOBAL for internationalized email.
        let (input, _) = sp(input)?;
        let (input, env) = envelope(input, utf8_mode)?;
        let (input, _) = sp(input)?;
        // message/rfc822 embeds a full BODYSTRUCTURE  -  increment depth.
        let (input, body) = body_structure(input, utf8_mode, depth + 1)?;
        let (input, _) = sp(input)?;
        // RFC 9051 Section 9: body-fld-lines = number64
        let (input, lines) = number64(input)?;
        // Extension data
        let (input, ext) = body_ext_1part(input)?;
        let (input, _) = char(')').parse(input)?;
        Ok((
            input,
            BodyStructure::Message {
                media_subtype,
                params,
                id,
                description,
                encoding,
                size,
                envelope: Box::new(env),
                body: Box::new(body),
                lines,
                md5: ext.0,
                disposition: ext.1,
                language: ext.2,
                location: ext.3,
            },
        ))
    } else {
        // Basic type (image, application, etc.)
        let (input, ext) = body_ext_1part(input)?;
        let (input, _) = char(')').parse(input)?;
        Ok((
            input,
            BodyStructure::Basic {
                media_type,
                media_subtype,
                params,
                id,
                description,
                encoding,
                size,
                md5: ext.0,
                disposition: ext.1,
                language: ext.2,
                location: ext.3,
            },
        ))
    }
}

/// Parse multipart body (RFC 3501 Section 7.4.2).
///
/// Starts after the opening `(` of the BODYSTRUCTURE and the first nested `(`.
/// When `utf8_mode` is `WireMode::Utf8`, embedded ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
///
/// `depth` tracks the current nesting level for recursion-depth enforcement.
pub(super) fn body_type_mpart(
    input: &[u8],
    utf8_mode: crate::types::wire::WireMode,
    depth: u32,
) -> IResult<&[u8], BodyStructure> {
    // One or more nested body structures  -  each child increments the depth.
    // RFC 3501 Section 9: `body-type-mpart = 1*body SP media-subtype ...`
    let mut bodies = Vec::new();
    let mut input = input;
    while input.first() == Some(&b'(') {
        let (rest, bs) = body_structure(input, utf8_mode, depth + 1)?;
        bodies.push(bs);
        input = rest;
        // Postel's law: consume optional whitespace between child bodies.
        // The ABNF (RFC 3501 Section 9: `1*body`) specifies no separator,
        // but some servers may insert spaces. We only strip whitespace when
        // the next non-whitespace byte is `(` (another child body), so we
        // do not swallow the `SP` that precedes `media-subtype`.
        let trimmed = input
            .iter()
            .position(|&b| b != b' ' && b != b'\t')
            .map_or(&[][..], |pos| &input[pos..]);
        if trimmed.first() == Some(&b'(') {
            input = trimmed;
        }
    }

    // RFC 3501 Section 9 requires `1*body`  -  at least one child body.
    if bodies.is_empty() {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Many1,
        )));
    }

    // SP subtype
    // RFC 2045 Section 5.1: media types are case-insensitive  -  lowercase
    // is the canonical form used in RFC examples (e.g. `multipart/mixed`).
    // Accept bare atoms per Postel's law (see comment in body_type_single).
    // Postel's law: accept one or more spaces/tabs before the subtype,
    // consistent with the whitespace tolerance between child bodies above.
    let (input, _) = take_while1(|b: u8| b == b' ' || b == b'\t').parse(input)?;
    let (input, subtype_raw) = astring(input)?;
    let media_subtype = String::from_utf8_lossy(&subtype_raw).to_ascii_lowercase();

    // Extension data for multipart
    let (input, ext) = body_ext_mpart(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((
        input,
        BodyStructure::Multipart {
            media_subtype,
            bodies,
            params: ext.0,
            disposition: ext.1,
            language: ext.2,
            location: ext.3,
        },
    ))
}

/// Parse body parameters: NIL or `("key" "val" ...)` (RFC 3501 Section 9).
///
/// ```text
/// body-fld-param = "(" string SP string *(SP string SP string) ")" / nil
/// ```
///
/// At least one key-value pair is required when parenthesized (RFC 3501 Section 9).
pub(super) fn body_params(input: &[u8]) -> IResult<&[u8], Vec<(String, String)>> {
    alt((
        value(vec![], nil_token),
        map(
            delimited(
                char('('),
                // RFC 3501 Section 9: body-fld-param formally requires at least one
                // key-value pair, but many servers send `()` for empty parameter lists.
                // Accept the empty form per Postel's law (RFC 1122 Section 1.2.2).
                separated_list0(
                    // Postel's law (RFC 1122 Section 1.2.2): accept one or more spaces
                    // between parameter pairs  -  some servers insert extra whitespace.
                    take_while1(|b: u8| b == b' '),
                    map(
                        (
                            string_utf8,
                            // Postel's law: accept one or more spaces between key and
                            // value, matching the between-pair tolerance above. Some
                            // servers insert extra whitespace within pairs too.
                            preceded(take_while1(|b: u8| b == b' '), string_utf8),
                        ),
                        |(k, v)| {
                            // RFC 2045 Section 5.1: parameter names are not case
                            // sensitive  -  lowercase for consistent lookup.
                            (k.to_ascii_lowercase(), v)
                        },
                    ),
                ),
                char(')'),
            ),
            // RFC 2231 Sections 3-4: decode continuations and charset-encoded values.
            |params| decode_rfc2231_params(&params),
        ),
    ))
    .parse(input)
}

/// Parse Content-Disposition: NIL or `("type" (params))` (RFC 2183).
pub(super) fn body_disposition(input: &[u8]) -> IResult<&[u8], Option<ContentDisposition>> {
    alt((
        value(None, nil_token),
        map(
            delimited(
                char('('),
                (string_utf8, preceded(sp, body_params)),
                char(')'),
            ),
            |(disposition_type, params)| {
                Some(ContentDisposition {
                    // RFC 2183 Section 2: disposition type is not case
                    // sensitive  -  lowercase for consistent comparison
                    // (conventional form used in RFC examples and real-world
                    // implementations: "inline", "attachment").
                    disposition_type: disposition_type.to_ascii_lowercase(),
                    params,
                })
            },
        ),
    ))
    .parse(input)
}

/// Parse body language: NIL, string, or `(list)` (RFC 3501 Section 7.4.2).
///
/// The parenthesized form requires at least one language per
/// RFC 3501 Section 7.4.2: `body-fld-lang = nstring / "(" string *(SP string) ")"`.
pub(super) fn body_language(input: &[u8]) -> IResult<&[u8], Option<Vec<String>>> {
    alt((
        value(None, nil_token),
        map(
            delimited(
                char('('),
                // RFC 3501 Section 7.4.2: body-fld-lang formally requires at least
                // one string in the parenthesized form, but some servers send `()`
                // for empty language lists. Accept per Postel's law (RFC 1122 Section 1.2.2).
                // Postel's law (RFC 1122 Section 1.2.2): accept one or more spaces
                // between language tags  -  some non-conformant servers insert extra
                // whitespace.
                separated_list0(take_while1(|b: u8| b == b' '), string_utf8),
                char(')'),
            ),
            Some,
        ),
        map(string_utf8, |s| Some(vec![s])),
    ))
    .parse(input)
}

/// Extension data common to single-part and multipart BODYSTRUCTURE responses.
type BodyExtData = (
    Option<String>,
    Option<ContentDisposition>,
    Option<Vec<String>>,
    Option<String>,
);

/// Multipart extension data: params, disposition, language, location.
type MpartExtData = (
    Vec<(String, String)>,
    Option<ContentDisposition>,
    Option<Vec<String>>,
    Option<String>,
);

/// Check if the next non-space byte is `)`, indicating no more optional body
/// extension fields (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
///
/// Tolerates trailing whitespace before `)` from non-conformant servers
/// (Postel's law / RFC 1122 Section 1.2.2). When `)` is found, advances past
/// the whitespace so the caller sees `)` next. Otherwise, returns the original
/// input position so no bytes are consumed.
fn at_body_ext_end(input: &[u8]) -> IResult<&[u8], bool> {
    let (trimmed, _) = take_while(|b: u8| b == b' ').parse(input)?;
    if trimmed.first() == Some(&b')') {
        Ok((trimmed, true))
    } else {
        Ok((input, false))
    }
}

/// Parse single-part extension data: md5, disposition, language, location
/// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
///
/// All fields are optional  -  stop when we see ')'.
fn body_ext_1part(input: &[u8]) -> IResult<&[u8], BodyExtData> {
    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (None, None, None, None)));
    }

    // MD5
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, md5) = nstring_utf8(input)?;

    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (md5, None, None, None)));
    }

    // Disposition
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, disposition) = body_disposition(input)?;

    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (md5, disposition, None, None)));
    }

    // Language
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, language) = body_language(input)?;

    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (md5, disposition, language, None)));
    }

    // Location
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, location) = nstring_utf8(input)?;

    // Skip any further extension data we don't understand.
    // body-extension = nstring / number / "(" body-extension *(SP body-extension) ")"
    // Must track nesting depth for parenthesized extension data (RFC 3501 Section 9).
    let (input, ()) = skip_balanced_parens(input)?;

    Ok((input, (md5, disposition, language, location)))
}

/// Parse multipart extension data: params, disposition, language, location
/// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
fn body_ext_mpart(input: &[u8]) -> IResult<&[u8], MpartExtData> {
    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (vec![], None, None, None)));
    }

    // Params
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, params) = body_params(input)?;

    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (params, None, None, None)));
    }

    // Disposition
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, disposition) = body_disposition(input)?;

    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (params, disposition, None, None)));
    }

    // Language
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, language) = body_language(input)?;

    let (input, at_end) = at_body_ext_end(input)?;
    if at_end {
        return Ok((input, (params, disposition, language, None)));
    }

    // Location
    let (input, _) = take_while1(|b: u8| b == b' ').parse(input)?;
    let (input, location) = nstring_utf8(input)?;

    // Skip any further extension data with proper nesting (RFC 3501 Section 9).
    let (input, ()) = skip_balanced_parens(input)?;

    Ok((input, (params, disposition, language, location)))
}
