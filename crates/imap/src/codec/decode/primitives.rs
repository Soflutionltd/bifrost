//! Core IMAP grammar primitives (RFC 3501 Section 9 / RFC 9051 Section 9).

#[allow(clippy::wildcard_imports)]
use super::*;

/// CRLF terminator (RFC 3501 Section 9 / RFC 9051 Section 9).
pub(super) fn crlf(input: &[u8]) -> IResult<&[u8], &[u8]> {
    tag(&b"\r\n"[..]).parse(input)
}

/// SP (single space) (RFC 3501 Section 9 / RFC 9051 Section 9).
pub(super) fn sp(input: &[u8]) -> IResult<&[u8], u8> {
    nom::character::complete::char(' ')
        .parse(input)
        .map(|(i, _)| (i, b' '))
}

/// Skip a complete parenthesized block `(...)` with balanced nesting.
///
/// Used to skip LIST-EXTENDED data (RFC 5258) like OLDNAME or CHILDINFO.
pub(super) fn skip_parenthesized_block(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, _) = char('(').parse(input)?;
    let (input, ()) = skip_balanced_parens(input)?;
    let (input, _) = char(')').parse(input)?;
    Ok((input, ()))
}

/// Skip bytes until the next unmatched `)`, handling nested parentheses.
///
/// Used to skip extension data that may contain nested parenthesized values,
/// e.g. in NAMESPACE descriptors (RFC 2342) or LIST-EXTENDED data (RFC 5258).
/// Quoted strings and literal strings within the skipped region are handled
/// so that parentheses inside them do not affect depth tracking.
///
/// Handles:
/// - Nested `(...)` groups with balanced depth tracking
/// - Quoted strings `"..."` with `\` escape handling
/// - Literal strings `{n}\r\n<n bytes>` (RFC 3501 Section 9)
/// - Literal+ `{n+}\r\n<n bytes>` (RFC 7888)
/// - Literal8 `~{n}\r\n<n bytes>` (RFC 6855 Section 4)
pub(super) fn skip_balanced_parens(mut input: &[u8]) -> IResult<&[u8], ()> {
    let mut depth: u32 = 0;
    loop {
        if input.is_empty() {
            // If we've consumed all input while parentheses are still open,
            // signal incomplete  -  the caller will either wait for more data
            // or surface a parse error. Returning Ok here would silently
            // consume bytes from subsequent responses (RFC 3501 Section 7.4.2).
            return if depth == 0 {
                Ok((input, ()))
            } else {
                Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Char,
                )))
            };
        }
        if depth == 0 && input[0] == b')' {
            return Ok((input, ()));
        }
        match input[0] {
            // CRLF cannot occur inside a parenthesized extension value except
            // as part of a literal, which is consumed by the literal arm.
            // Do not scan into the next response after malformed quoting.
            b'\r' | b'\n' => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::CrLf,
                )));
            }
            b'(' => {
                depth += 1;
                input = &input[1..];
            }
            b')' if depth > 0 => {
                depth -= 1;
                input = &input[1..];
            }
            b'"' => {
                // Skip quoted string contents (may contain parens).
                input = &input[1..];
                while !input.is_empty() && input[0] != b'"' {
                    // RFC 3501 Section 9: quoted = DQUOTE *QUOTED-CHAR DQUOTE
                    // and QUOTED-CHAR excludes CR and LF, escaped or not. A raw
                    // CR/LF means the quote is unterminated  -  stop here so the
                    // scan cannot run into the next response.
                    if input[0] == b'\r' || input[0] == b'\n' {
                        break;
                    }
                    if input[0] == b'\\' && input.len() > 1 {
                        // Guard against escapes consuming CR/LF:
                        // RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
                        // A backslash before CR/LF is malformed  -  break out
                        // so the outer loop does not skip past the response-
                        // terminating CRLF.
                        if input[1] == b'\r' || input[1] == b'\n' {
                            break;
                        }
                        input = &input[2..]; // skip escaped char
                    } else {
                        input = &input[1..];
                    }
                }
                if input.first() == Some(&b'"') {
                    input = &input[1..]; // skip closing quote
                }
                // Otherwise the quote was unterminated; leave the CR/LF in
                // place for the outer loop's terminator arm to reject.
            }
            // Handle literal8 prefix: ~{n}\r\n<n bytes> (RFC 6855 Section 4)
            b'~' if input.len() > 1 && input[1] == b'{' => {
                input = &input[1..]; // skip '~', fall through to '{' on next iteration
            }
            b'{' => {
                // Handle literal: {n}\r\n<n bytes> (RFC 3501 Section 9)
                // and literal+: {n+}\r\n<n bytes> (RFC 7888)
                input = &input[1..]; // skip '{'
                // Extract the count digits
                let start = 0;
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
                                        input = &input[new_end..];
                                    }
                                    _ => {
                                        // Literal body exceeds available data or
                                        // overflows usize  -  stop scanning to avoid
                                        // misinterpreting literal body bytes as
                                        // parenthesized structure
                                        // (RFC 3501 Section 9 / RFC 9051 Section 9).
                                        return Ok((input, ()));
                                    }
                                }
                            }
                        }
                    }
                }
                // If literal parsing failed, just continue (already past '{')
            }
            _ => {
                input = &input[1..];
            }
        }
    }
}

/// Parse an atom: 1*ATOM-CHAR (RFC 3501 Section 9 / RFC 9051 Section 9).
pub(super) fn atom(input: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while1(is_atom_char).parse(input)
}

/// Parse an objectid value per RFC 8474 Section 7.
///
/// `objectid = 1*255(ALPHA / DIGIT / "_" / "-")`
///
/// Delegates to the generic [`atom`] parser so that non-compliant servers
/// (characters outside the restricted set or values exceeding the 255-char
/// limit) remain interoperable via Postel's law (RFC 1122 Section 1.2.2).
/// Truncating oversized values would poison the input stream and cause
/// downstream parse failures.
pub(super) fn objectid(input: &[u8]) -> IResult<&[u8], &[u8]> {
    atom(input)
}

/// Parse a FETCH attribute name  -  like [`atom`] but stops at `[` so that
/// `BODY[section]` is split into atom `BODY` and section `[section]`.
pub(super) fn fetch_attr_atom(input: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while1(is_atom_char_no_bracket).parse(input)
}

/// Parse a tag: `1*<any ASTRING-CHAR except "+">` (RFC 3501 Section 9 / RFC 9051 Section 9).
///
/// ASTRING-CHAR = ATOM-CHAR / resp-specials, so tags allow `]` but exclude `+`.
pub(super) fn tag_str(input: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while1(is_tag_char).parse(input)
}

/// Check if byte is a valid tag character per RFC 3501 Section 9.
///
/// `tag = 1*<any ASTRING-CHAR except "+">` where `ASTRING-CHAR = ATOM-CHAR / resp-specials`.
/// `resp-specials = "]"`, so tags allow `]` in addition to ATOM-CHAR.
fn is_tag_char(b: u8) -> bool {
    (is_atom_char(b) || b == b']') && b != b'+'
}

/// Check if byte is a valid ATOM-CHAR per RFC 3501 Section 9 / RFC 9051 Section 9.
///
/// `atom-specials = "(" / ")" / "{" / SP / CTL / list-wildcards / quoted-specials / resp-specials`
/// `list-wildcards = "%" / "*"`
/// `quoted-specials = DQUOTE / "\"`
/// `resp-specials = "]"`
///
/// Note: `[` is NOT an atom-special per the RFC grammar. It is valid in atoms.
///
/// RFC 3501 Section 9: ATOM-CHAR = <any CHAR except atom-specials> where CHAR = %x01-7f.
/// We intentionally accept bytes 0x80-0xFF for compatibility with servers that send
/// non-ASCII bytes in atoms (common with non-conformant servers). This follows Postel's law.
pub(super) fn is_atom_char(b: u8) -> bool {
    b > 0x1F
        && b != 0x7F
        && b != b' '
        && b != b'('
        && b != b')'
        && b != b'{'
        && b != b'%'
        && b != b'*'
        && b != b'"'
        && b != b'\\'
        && b != b']'
}

/// Like [`is_atom_char`] but also excludes `[`.
///
/// Used in contexts where `[` acts as a delimiter  -  specifically FETCH response
/// attribute names (e.g., `BODY[section]`, `BINARY[section]`)  -  so the atom must
/// stop before `[`.
pub(super) fn is_atom_char_no_bracket(b: u8) -> bool {
    is_atom_char(b) && b != b'['
}

/// Parse a quoted string (RFC 3501 Section 9 / RFC 9051 Section 9).
///
/// Handles non-ASCII bytes 0x01-0xFF per real-world server behavior.
/// Returns the unescaped content.
pub(super) fn quoted_string(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    let (input, _) = char('"').parse(input)?;
    let mut result = Vec::new();
    let mut i = input;
    loop {
        if i.is_empty() {
            // Complete-mode: all input available  -  unterminated quote is an error,
            // not "need more data" (RFC 3501 Section 9).
            return Err(nom::Err::Error(nom::error::Error::new(
                i,
                nom::error::ErrorKind::Char,
            )));
        }
        match i[0] {
            b'"' => {
                return Ok((&i[1..], result));
            }
            b'\\' => {
                // Escaped character: \" or \\ per RFC 3501 Section 9.
                if i.len() < 2 {
                    // Complete-mode: truncated escape is an error (RFC 3501 Section 9).
                    return Err(nom::Err::Error(nom::error::Error::new(
                        i,
                        nom::error::ErrorKind::Char,
                    )));
                }
                let escaped = i[1];
                // RFC 3501 Section 9: NUL (%x00) MUST NOT be used at any time.
                // QUOTED-CHAR also excludes CR and LF.
                // A backslash before NUL/CR/LF is malformed  -  reject it so
                // the escape cannot smuggle a NUL or skip past the
                // response-terminating CRLF.
                if escaped == 0 || escaped == b'\r' || escaped == b'\n' {
                    return Err(nom::Err::Error(nom::error::Error::new(
                        i,
                        nom::error::ErrorKind::Char,
                    )));
                }
                if escaped != b'"' && escaped != b'\\' {
                    // RFC 3501 Section 9: only \" and \\ are valid quoted-specials.
                    // QUOTED-CHAR = <any TEXT-CHAR except quoted-specials> /
                    //               "\" quoted-specials
                    // A backslash followed by a non-quoted-special is malformed.
                    // Preserve the backslash as literal data per Postel's law  -
                    // the sender clearly intended to include it.
                    tracing::debug!(
                        escaped_byte = escaped,
                        "non-standard quoted-string escape: preserving backslash as literal data"
                    );
                    result.push(b'\\');
                }
                result.push(escaped);
                i = &i[2..];
            }
            0 => {
                // NUL not allowed in quoted strings
                return Err(nom::Err::Error(nom::error::Error::new(
                    i,
                    nom::error::ErrorKind::Char,
                )));
            }
            b => {
                // RFC 3501: QUOTED-CHAR = any TEXT-CHAR except quoted-specials
                // TEXT-CHAR = any CHAR except CR and LF
                // Real servers send 0x80-0xFF (non-ASCII)  -  accept them.
                if b == b'\r' || b == b'\n' {
                    return Err(nom::Err::Error(nom::error::Error::new(
                        i,
                        nom::error::ErrorKind::Char,
                    )));
                }
                result.push(b);
                i = &i[1..];
            }
        }
    }
}

/// Parse a literal: `{count}\r\n<bytes>` (RFC 3501 Section 9 / RFC 9051 Section 9).
///
/// Also handles LITERAL+ non-synchronizing literals `{count+}` (RFC 7888 Section 4)
/// and literal8 `~{count}\r\n<bytes>` (RFC 6855 Section 4).
pub(super) fn literal(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    // Optional '~' prefix for literal8 (RFC 6855 Section 4)
    let (input, _is_literal8) = opt(char('~')).parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, count_bytes) = digit1(input)?;
    let count_str = std::str::from_utf8(count_bytes).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    // Parse as u64 first to avoid truncation on 32-bit platforms where usize
    // is 32 bits (RFC 9051 Section 9: literal uses number64, 0..2^63-1).
    let count_u64: u64 = count_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    if count_u64 > i64::MAX as u64 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    // Convert to usize for take(). On 32-bit platforms, counts > 4GB are
    // rejected here rather than silently truncating.
    let count: usize = usize::try_from(count_u64).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    // RFC 9051 Section 9: literal8 = "~{" number64 "}"  -  no `["+"]`.
    // RFC 7888 Section 4: LITERAL+ `{n+}` is for regular literals only.
    // Per Postel's law, we tolerate `~{n+}` from non-conformant servers
    // since the `+` has no semantic impact on server-to-client data.
    let (input, _has_plus) = opt(char('+')).parse(input)?;
    let (input, _) = char('}').parse(input)?;
    let (input, _) = crlf(input)?;
    let (input, data) = take(count).parse(input)?;
    // The parser accepts borrowed slices and string/nstring as well as FETCH
    // response fields own Vec<u8>. Freezing WireReader's buffer alone cannot
    // make these zero-copy: that requires changing those response types and
    // threading the backing Bytes through the parser. It also would not emit
    // a section until the complete response has been framed and parsed.
    Ok((input, data.to_vec()))
}

/// Parse a `string`: quoted string or literal (RFC 3501 Section 9).
pub(super) fn string(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    alt((quoted_string, literal)).parse(input)
}

/// Parse an `astring`: atom or string (RFC 3501 Section 9).
///
/// Used for mailbox names and other contexts where atoms are accepted.
pub(super) fn astring(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    alt((
        // Also accept resp-specials ']' and atom-specials in astring-char context
        map(astring_chars, |s: &[u8]| s.to_vec()),
        string,
    ))
    .parse(input)
}

/// 1*ASTRING-CHAR: atom chars plus ']' (RFC 3501 Section 9).
fn astring_chars(input: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while1(|b: u8| is_atom_char(b) || b == b']').parse(input)
}

/// Match the `NIL` atom with token-boundary verification (RFC 3501 Section 9).
///
/// Plain `tag_no_case(&b"NIL"[..])` would greedily match the prefix of atoms like
/// "NILSIMSA", corrupting the parse. This combinator ensures the three-byte
/// match is followed by an atom-special or end-of-input, so only the
/// standalone `NIL` token is accepted.
///
/// RFC 3501 Section 9 ABNF:
///   atom-specials = "(" / ")" / "{" / SP / CTL / list-wildcards /
///                   quoted-specials / resp-specials
/// where list-wildcards = "%" / "*", quoted-specials = DQUOTE / "\",
/// resp-specials = "]".
pub(super) fn nil_token(input: &[u8]) -> IResult<&[u8], &[u8]> {
    terminated(
        tag_no_case(&b"NIL"[..]),
        peek(alt((
            // Any atom-special byte (RFC 3501 Section 9):
            //   atom-specials = "(" / ")" / "{" / SP / CTL /
            //                   list-wildcards / quoted-specials / resp-specials
            // This is exactly the set of bytes that are NOT atom-chars,
            // i.e., `!is_atom_char(b)`.
            value((), verify(take(1u8), |b: &[u8]| !is_atom_char(b[0]))),
            // End-of-input
            value((), eof),
        ))),
    )
    .parse(input)
}

/// Parse an `nstring`: NIL or string (RFC 3501 Section 9 / RFC 9051 Section 9).
pub(super) fn nstring(input: &[u8]) -> IResult<&[u8], Option<Vec<u8>>> {
    alt((value(None, nil_token), map(string, Some))).parse(input)
}

/// Parse a number: 1*DIGIT (RFC 3501 Section 9 / RFC 9051 Section 9).
///
/// Returns u32. Errors gracefully on overflow.
pub(super) fn number(input: &[u8]) -> IResult<&[u8], u32> {
    let (input, digits) = digit1(input)?;
    let s = std::str::from_utf8(digits).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    let n: u32 = s.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((input, n))
}

/// Parse a non-zero number: `nz-number = digit-nz *DIGIT` (RFC 3501 Section 9 / RFC 9051 Section 9).
///
/// Used for message sequence numbers, UIDs, and ESEARCH MIN/MAX values
/// where zero is never valid.
pub(super) fn nz_number(input: &[u8]) -> IResult<&[u8], u32> {
    // RFC 3501 Section 9: nz-number = digit-nz *DIGIT
    // digit-nz = %x31-39 ; 1-9  -  leading '0' is never valid.
    if input.first() == Some(&b'0') {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (rest, n) = number(input)?;
    if n == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    Ok((rest, n))
}

/// Parse a 64-bit number for MODSEQ values (RFC 7162 Section 3.1.3).
///
/// RFC 9051 Section 4: `mod-sequence-value = 1*DIGIT` constrained to non-negative
/// 63-bit values (0 .. 2^63-1). Values above `i64::MAX` are rejected.
pub(super) fn number64(input: &[u8]) -> IResult<&[u8], u64> {
    let (input, digits) = digit1(input)?;
    let s = std::str::from_utf8(digits).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    let n: u64 = s.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    // RFC 9051 Section 4: mod-sequence-value is limited to 63 bits (non-negative signed).
    if n > i64::MAX as u64 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    Ok((input, n))
}

/// Parse a non-zero 64-bit number: `nz-number` variant for mod-sequence-value
/// (RFC 7162 Section 3.1.3).
///
/// Used where MODSEQ/HIGHESTMODSEQ values must be >= 1 (e.g. FETCH MODSEQ,
/// ESEARCH MODSEQ, response-code HIGHESTMODSEQ). STATUS HIGHESTMODSEQ
/// correctly allows 0 and should continue using `number64`.
pub(super) fn nz_number64(input: &[u8]) -> IResult<&[u8], u64> {
    // RFC 9051 Section 9: nz-number64 = digit-nz *DIGIT
    // digit-nz = %x31-39 ; 1-9  -  leading '0' is never valid.
    if input.first() == Some(&b'0') {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (rest, n) = number64(input)?;
    if n == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    Ok((rest, n))
}

/// Tolerant version of [`number`] for contexts where an overflowing value
/// should be skipped rather than rejected. Consumes the digit run and
/// returns `None` if the value exceeds `u32::MAX`
/// (Postel's law  -  RFC 1122 Section 1.2.2).
pub(super) fn number_tolerant(input: &[u8]) -> IResult<&[u8], Option<u32>> {
    let (rest, digits) = digit1(input)?;
    let val = std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse::<u32>().ok());
    Ok((rest, val))
}

/// Tolerant version of [`number64`] for contexts where an overflowing value
/// should be skipped rather than rejected. Consumes the digit run and
/// returns `None` if the value exceeds `i64::MAX`
/// (Postel's law  -  RFC 1122 Section 1.2.2, RFC 9051 Section 4).
pub(super) fn number64_tolerant(input: &[u8]) -> IResult<&[u8], Option<u64>> {
    let (rest, digits) = digit1(input)?;
    let val = std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| i64::try_from(n).is_ok());
    Ok((rest, val))
}

/// Parse nstring and convert to Option<String> (lossy UTF-8) (RFC 3501 Section 9 / RFC 9051 Section 9).
pub(super) fn nstring_utf8(input: &[u8]) -> IResult<&[u8], Option<String>> {
    let (input, val) = nstring(input)?;
    Ok((input, val.map(|v| String::from_utf8_lossy(&v).into_owned())))
}

/// Parse a string and return as String (lossy UTF-8) (RFC 3501 Section 9 / RFC 9051 Section 9).
pub(super) fn string_utf8(input: &[u8]) -> IResult<&[u8], String> {
    let (input, val) = string(input)?;
    Ok((input, String::from_utf8_lossy(&val).into_owned()))
}

/// Parse an astring and return as String (lossy UTF-8) (RFC 3501 Section 9 / RFC 9051 Section 9).
pub(super) fn astring_utf8(input: &[u8]) -> IResult<&[u8], String> {
    let (input, val) = astring(input)?;
    Ok((input, String::from_utf8_lossy(&val).into_owned()))
}
