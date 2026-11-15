//! NAMESPACE response parser (RFC 2342).

#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;

/// Parse `* NAMESPACE personal other shared` (RFC 2342 Section 5).
///
/// When `utf8_mode` is false, namespace prefixes are decoded from Modified
/// UTF-7 (RFC 3501 Section 5.1.3). When true, they are raw UTF-8
/// (RFC 9051 Section 5.1 / RFC 6855 Section 3).
pub(super) fn parse_untagged_namespace(
    input: &[u8],
    utf8_mode: crate::types::wire::WireMode,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"NAMESPACE"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    let (input, personal) = namespace_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, other) = namespace_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, shared) = namespace_list(input, utf8_mode)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        UntaggedResponse::Namespace {
            personal,
            other,
            shared,
        },
    ))
}

/// Parse a namespace descriptor list: NIL or `((prefix delimiter) ...)` (RFC 2342 Section 5).
fn namespace_list(
    input: &[u8],
    utf8_mode: crate::types::wire::WireMode,
) -> IResult<&[u8], Vec<NamespaceDescriptor>> {
    alt((
        value(vec![], nil_token),
        // RFC 2342 Section 6: Namespace = nil / "(" 1*(...) ")"
        // Non-conformant servers may send "()" for an empty namespace
        // category instead of NIL. Accept per Postel's law.
        delimited(
            char('('),
            many0(|i| namespace_descriptor(i, utf8_mode)),
            char(')'),
        ),
    ))
    .parse(input)
}

/// Parse a single namespace descriptor: `(prefix delimiter [extensions])` (RFC 2342 Section 6).
fn namespace_descriptor(
    input: &[u8],
    utf8_mode: crate::types::wire::WireMode,
) -> IResult<&[u8], NamespaceDescriptor> {
    let (input, _) = char('(').parse(input)?;
    let (input, raw_prefix) = string_utf8(input)?;
    // RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1: decode namespace prefix
    // from Modified UTF-7 when not in UTF-8 mode.
    let prefix = if utf8_mode == crate::types::wire::WireMode::Utf8 {
        raw_prefix
    } else {
        crate::types::wire::decode_utf7(raw_prefix.as_bytes())
    };
    let (input, _) = sp(input)?;
    // RFC 2342 Section 6 / RFC 9051 Section 9: the delimiter is
    // `DQUOTE QUOTED-CHAR DQUOTE / nil` and should be one character.
    let (input, delim_bytes) =
        alt((value(None, nil_token), map(quoted_string, Some))).parse(input)?;
    let delim = match delim_bytes {
        None => None,
        Some(v) if v.is_empty() => None,
        Some(v) => {
            let s = String::from_utf8_lossy(&v);
            let mut chars = s.chars();
            let ch = chars.next();
            if chars.next().is_some() {
                tracing::warn!(
                    delimiter = %s,
                    "NAMESPACE delimiter has multiple characters (RFC 2342 Section 6 \
                     requires exactly one QUOTED-CHAR); taking only the first character"
                );
            }
            ch
        }
    };
    let (input, extensions) = many0(namespace_response_extension).parse(input)?;
    let (input, _) = char(')').parse(input)?;
    Ok((
        input,
        NamespaceDescriptor {
            prefix,
            delimiter: delim,
            extensions,
        },
    ))
}

/// Parse a single `Namespace_Response_Extension` (RFC 2342 Section 6).
///
/// ABNF: `SP string SP "(" string *(SP string) ")"`
fn namespace_response_extension(input: &[u8]) -> IResult<&[u8], (String, Vec<String>)> {
    let (input, _) = sp(input)?;
    let (input, key) = string_utf8(input)?;
    let (input, _) = sp(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, first_val) = string_utf8(input)?;
    let (input, rest_vals) = many0(preceded(sp, string_utf8)).parse(input)?;
    let (input, _) = char(')').parse(input)?;
    let mut values = Vec::with_capacity(1 + rest_vals.len());
    values.push(first_val);
    values.extend(rest_vals);
    Ok((input, (key, values)))
}
