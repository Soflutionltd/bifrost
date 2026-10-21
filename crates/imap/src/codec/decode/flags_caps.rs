//! Flag, Capability, and Response Code parsers
//! (RFC 3501 Section 7.1-7.2, RFC 9051 Section 7.1-7.2, RFC 5530).

#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;

/// Parse a single flag, including `\*` for parse robustness.
///
/// Accepts all flag forms: system flags (`\Seen`, `\Flagged`, etc.),
/// keywords (`$Important`, `$Forwarded`), flag-extensions (`\<atom>`),
/// and the `\*` wildcard.
///
/// Per RFC 3501 Section 9, `\*` is only valid in the `flag-perm` production
/// (PERMANENTFLAGS), not in `flag` or `flag-fetch`.  Callers that need
/// strict `flag` semantics should filter `Flag::Wildcard` from results
/// (see [`flag_list`]).
pub(super) fn flag_or_perm(input: &[u8]) -> IResult<&[u8], Flag> {
    alt((
        map(tag(&b"\\*"[..]), |_| Flag::Wildcard),
        map((char('\\'), atom), |(_, name)| {
            let full = format!("\\{}", String::from_utf8_lossy(name));
            Flag::from_imap_str(&full)
        }),
        map(atom, |a| Flag::from_imap_str(&String::from_utf8_lossy(a))),
    ))
    .parse(input)
}

/// Parse a parenthesized list of flags, optionally filtering `\*`.
///
/// RFC 3501 Section 9: `\*` is only valid in the `flag-perm` production
/// (PERMANENTFLAGS). When `allow_wildcard` is `false`, `\*` is parsed
/// without error but silently filtered from the result (Postel's law).
/// Also handles empty list `()`.
fn parse_flag_list(input: &[u8], allow_wildcard: bool) -> IResult<&[u8], Vec<Flag>> {
    let (input, flags) = delimited(
        char('('),
        // Postel's law (RFC 1122 Section 1.2.2): accept one or more spaces
        // between flags  -  some non-conformant servers insert extra whitespace.
        separated_list0(take_while1(|b: u8| b == b' '), flag_or_perm),
        char(')'),
    )
    .parse(input)?;
    if allow_wildcard {
        Ok((input, flags))
    } else {
        // RFC 3501 Section 9: filter \* which is only valid in flag-perm.
        Ok((
            input,
            flags.into_iter().filter(|f| *f != Flag::Wildcard).collect(),
        ))
    }
}

/// Parse a flag list excluding `\*`: `(\Seen \Flagged $Important)`
/// (RFC 3501 Section 2.3.2).
///
/// Used for both:
/// - `flag-list` in untagged FLAGS responses (RFC 3501 Section 7.2.6),
/// - `flag-fetch` in FETCH FLAGS (RFC 3501 Section 9:
///   `msg-att-dynamic = "FLAGS" SP "(" [flag-fetch *(SP flag-fetch)] ")"`
///   where `flag-fetch = flag / "\Recent"`).
///
/// Both productions exclude `\*` (which is only valid in `flag-perm` for
/// PERMANENTFLAGS).  For robustness against non-conformant servers, `\*`
/// is parsed without error but silently filtered (Postel's law).
///
/// Also handles empty list `()`.
pub(super) fn flag_list(input: &[u8]) -> IResult<&[u8], Vec<Flag>> {
    parse_flag_list(input, false)
}

/// Parse a `flag-perm` list for PERMANENTFLAGS: `(\Seen \Flagged \*)`.
///
/// RFC 3501 Section 7.1: `flag-perm = flag / "\*"`.
/// This is the only context where `\*` is valid.
pub(super) fn flag_perm_list(input: &[u8]) -> IResult<&[u8], Vec<Flag>> {
    parse_flag_list(input, true)
}

/// Parse a single capability and map to `Capability` enum (RFC 3501 Section 7.2.1).
///
/// Delegates to [`Capability::from_imap_str`] for the string-to-variant mapping.
pub(super) fn capability(input: &[u8]) -> IResult<&[u8], Capability> {
    let (input, raw) = atom(input)?;
    let s = String::from_utf8_lossy(raw);
    Ok((input, Capability::from_imap_str(&s)))
}

/// Parse a space-separated list of capabilities (RFC 3501 Section 7.2.1).
pub(super) fn capability_list(input: &[u8]) -> IResult<&[u8], Vec<Capability>> {
    // Postel's law (RFC 1122 Section 1.2.2): accept one or more spaces or
    // tabs between capabilities  -  some non-conformant servers insert HTABs
    // instead of SP in CAPABILITY data.
    separated_list0(take_while1(|b: u8| b == b' ' || b == b'\t'), capability).parse(input)
}

/// Parse a UID range: `n` or `n:m` (RFC 4315 Section 2.1).
///
/// Uses `nz-number` because `uniqueid = nz-number` (RFC 3501 Section 9).
pub(super) fn uid_range(input: &[u8]) -> IResult<&[u8], UidRange> {
    let (input, first) = nz_number(input)?;
    // If a ':' is present, the end must be a valid nz-number (RFC 3501 Section 9:
    // uid-range = (uniqueid ":" uniqueid), where uniqueid = nz-number).
    let (input, second) = if input.first() == Some(&b':') {
        let (input, _) = char(':').parse(input)?;
        let (input, end) = nz_number(input)?;
        (input, Some(end))
    } else {
        (input, None)
    };
    // Postel's law (RFC 1122 Section 1.2.2): normalize reversed ranges
    // where start > end. Some non-conformant servers emit descending ranges
    // (e.g., `100:1`). Swap the endpoints so start ≤ end, consistent with
    // the expected UID ordering (RFC 3501 Section 9).
    let (start, end) = match second {
        Some(end) if first > end => (end, Some(first)),
        other => (first, other),
    };
    Ok((
        input,
        match end {
            Some(end) => UidRange::range(start, end),
            None => UidRange::single(start),
        },
    ))
}

/// Parse a comma-separated UID set (RFC 4315 Section 4).
/// Grammar: `uid-set = (uniqueid / uid-range) *("," uid-set)`  -  requires at least one element.
pub(super) fn uid_set(input: &[u8]) -> IResult<&[u8], Vec<UidRange>> {
    separated_list1(char(','), uid_range).parse(input)
}

/// Parse a sequence number: `seq-number = nz-number / "*"` (RFC 3501 Section 9).
///
/// `*` remains distinct from every numeric UID, including `u32::MAX`.
fn seq_number(input: &[u8]) -> IResult<&[u8], crate::types::SeqBound> {
    if input.first() == Some(&b'*') {
        // RFC 3501 Section 9: `"*"` refers to the largest number in use.
        Ok((&input[1..], crate::types::SeqBound::Star))
    } else {
        nz_number(input).map(|(input, number)| (input, crate::types::SeqBound::Number(number)))
    }
}

/// Parse a sequence range: `seq-range = seq-number ":" seq-number` or a single
/// `seq-number` (RFC 3501 Section 9).
pub(super) fn seq_range(input: &[u8]) -> IResult<&[u8], UidRange> {
    let (input, first) = seq_number(input)?;
    // RFC 3501 Section 9: seq-range = seq-number ":" seq-number
    let (input, second) = if input.first() == Some(&b':') {
        let (input, _) = char(':').parse(input)?;
        let (input, end) = seq_number(input)?;
        (input, Some(end))
    } else {
        (input, None)
    };
    // Postel's law (RFC 1122 Section 1.2.2): normalize reversed ranges,
    // treating `*` as greater than every numeric endpoint.
    Ok((input, UidRange::from_sequence_bounds(first, second)))
}

/// Parse a comma-separated sequence set: `sequence-set = (seq-number / seq-range) *("," sequence-set)`
/// (RFC 3501 Section 9).
///
/// Unlike `uid_set`, this accepts `*` as a valid seq-number.
/// Used by ESEARCH ALL (RFC 4731 Section 3.1) and MODIFIED (RFC 7162 Section 3.1.3).
pub(super) fn sequence_set(input: &[u8]) -> IResult<&[u8], Vec<UidRange>> {
    separated_list1(char(','), seq_range).parse(input)
}

/// Parse a response code in square brackets: `[UIDVALIDITY 12345]`
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1, RFC 5530 for extended codes).
pub(super) fn response_code(input: &[u8]) -> IResult<&[u8], ResponseCode> {
    let (input, _) = char('[').parse(input)?;
    let (input, code_atom) = atom(input)?;
    let code_str = String::from_utf8_lossy(code_atom);
    let upper = code_str.to_ascii_uppercase();

    match response_code_inner(input, &code_str, &upper) {
        Ok((input, code)) => {
            let (input, _) = char(']').parse(input)?;
            Ok((input, code))
        }
        Err(nom::Err::Incomplete(needed)) => Err(nom::Err::Incomplete(needed)),
        Err(_err @ (nom::Err::Error(_) | nom::Err::Failure(_)))
            if response_code_has_overflow(input, &upper) =>
        {
            // A malformed value for a recognized code, most commonly an
            // overflowed number, must not make `opt(response_code)` demote
            // the entire bracket group into display text. Preserve its name
            // and opaque value without inventing a typed numeric value.
            let (input, value) = response_code_optional_tail(input)?;
            let (input, _) = char(']').parse(input)?;
            Ok((
                input,
                ResponseCode::Other {
                    name: code_str.into_owned(),
                    value,
                },
            ))
        }
        Err(err) => Err(err),
    }
}

/// Whether a recognized numeric response-code value exceeds the width its
/// typed variant can represent. This is deliberately narrower than generic
/// malformed-code recovery: a missing APPENDUID set, for example, remains a
/// parse error rather than being treated as an opaque extension.
fn response_code_has_overflow(input: &[u8], upper: &str) -> bool {
    let max = match upper {
        "UIDNEXT" | "UIDVALIDITY" | "UNSEEN" | "APPENDUID" | "COPYUID" | "MODIFIED" => {
            u64::from(u32::MAX)
        }
        "HIGHESTMODSEQ" | "METADATA" => i64::MAX as u64,
        _ => return false,
    };

    // Only the code's own value can overflow. Scanning past the closing
    // bracket would let unrelated status text, or a coalesced later response
    // in the same buffer, be mistaken for an oversized value and turn a real
    // parse error into an opaque code.
    let end = input
        .iter()
        .position(|b| matches!(*b, b']' | b'\r' | b'\n'))
        .unwrap_or(input.len());
    let input = &input[..end];

    let mut pos = 0;
    while pos < input.len() {
        if input[pos].is_ascii_digit() {
            let start = pos;
            while pos < input.len() && input[pos].is_ascii_digit() {
                pos += 1;
            }
            let overflowed = std::str::from_utf8(&input[start..pos])
                .ok()
                .and_then(|digits| digits.parse::<u64>().ok())
                .is_none_or(|number| number > max);
            if overflowed {
                return true;
            }
        } else {
            pos += 1;
        }
    }
    false
}

/// Parse the optional text that follows a response-code atom before the closing
/// `]`, preserving extension-specific payloads verbatim for typed passthrough
/// variants (RFC 5530 Section 6 registry entries).
fn response_code_optional_tail(input: &[u8]) -> IResult<&[u8], Option<String>> {
    let (input, val) = opt(preceded(sp, take_while(|b: u8| b != b']'))).parse(input)?;
    Ok((input, val.map(|v| String::from_utf8_lossy(v).into_owned())))
}

/// Dispatch response code by name after the atom has been consumed (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[allow(clippy::too_many_lines)]
fn response_code_inner<'a>(
    input: &'a [u8],
    code_str: &str,
    upper: &str,
) -> IResult<&'a [u8], ResponseCode> {
    match upper {
        "ALERT" => Ok((input, ResponseCode::Alert)),
        "PARSE" => Ok((input, ResponseCode::Parse)),
        "READ-ONLY" => Ok((input, ResponseCode::ReadOnly)),
        "READ-WRITE" => Ok((input, ResponseCode::ReadWrite)),
        "TRYCREATE" => Ok((input, ResponseCode::TryCreate)),
        "NOMODSEQ" => Ok((input, ResponseCode::NoModSeq)),
        "CLOSED" => Ok((input, ResponseCode::Closed)),
        // RFC 5530 codes
        "UNAVAILABLE" => Ok((input, ResponseCode::Unavailable)),
        "AUTHENTICATIONFAILED" => Ok((input, ResponseCode::AuthenticationFailed)),
        "AUTHORIZATIONFAILED" => Ok((input, ResponseCode::AuthorizationFailed)),
        "EXPIRED" => Ok((input, ResponseCode::Expired)),
        "PRIVACYREQUIRED" => Ok((input, ResponseCode::PrivacyRequired)),
        "CONTACTADMIN" => Ok((input, ResponseCode::ContactAdmin)),
        "NOPERM" => Ok((input, ResponseCode::NoPerm)),
        "INUSE" => Ok((input, ResponseCode::InUse)),
        "EXPUNGEISSUED" => Ok((input, ResponseCode::ExpungeIssued)),
        "CORRUPTION" => Ok((input, ResponseCode::Corruption)),
        "SERVERBUG" => Ok((input, ResponseCode::ServerBug)),
        "CLIENTBUG" => Ok((input, ResponseCode::ClientBug)),
        "CANNOT" => Ok((input, ResponseCode::Cannot)),
        "LIMIT" => Ok((input, ResponseCode::Limit)),
        "OVERQUOTA" => Ok((input, ResponseCode::OverQuota)),
        "ALREADYEXISTS" => Ok((input, ResponseCode::AlreadyExists)),
        "NONEXISTENT" => Ok((input, ResponseCode::NonExistent)),
        // RFC 5530 Section 6 registry: these standardized response codes come
        // from extension RFCs that may carry extension-specific payloads.
        // Preserve that trailing data verbatim instead of flattening the code
        // into `Other`, so consumers can distinguish the standardized code
        // while still inspecting the original extension data.
        "NEWNAME" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::NewName(value)))
        }
        "REFERRAL" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::Referral(value)))
        }
        "URLMECH" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::UrlMech(value)))
        }
        "BADURL" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::BadUrl(value)))
        }
        "BADCOMPARATOR" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::BadComparator(value)))
        }
        "ANNOTATE" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::Annotate(value)))
        }
        "ANNOTATIONS" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::Annotations(value)))
        }
        "TEMPFAIL" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::TempFail(value)))
        }
        "MAXCONVERTMESSAGES" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::MaxConvertMessages(value)))
        }
        "MAXCONVERTPARTS" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::MaxConvertParts(value)))
        }
        "NOUPDATE" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::NoUpdate(value)))
        }
        "NOTIFICATIONOVERFLOW" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::NotificationOverflow(value)))
        }
        "BADEVENT" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::BadEvent(value)))
        }
        "UNDEFINED-FILTER" => {
            let (input, value) = response_code_optional_tail(input)?;
            Ok((input, ResponseCode::UndefinedFilter(value)))
        }
        // RFC 4315 Section 2 / RFC 9051 Section 7.1
        "UIDNOTSTICKY" => Ok((input, ResponseCode::UidNotSticky)),
        // RFC 5182 Section 2.1
        "NOTSAVED" => Ok((input, ResponseCode::NotSaved)),
        // RFC 9051 Section 7.1
        "HASCHILDREN" => Ok((input, ResponseCode::HasChildren)),
        // RFC 3516 Section 4.3 / RFC 9051 Section 7.1: response code is "UNKNOWN-CTE"
        // (with hyphen). The atom parser preserves hyphens, and to_ascii_uppercase
        // produces "UNKNOWN-CTE".
        "UNKNOWN-CTE" => Ok((input, ResponseCode::UnknownCte)),
        // RFC 7889 Section 4
        "TOOBIG" => Ok((input, ResponseCode::TooBig)),
        // RFC 4978 Section 3
        "COMPRESSIONACTIVE" => Ok((input, ResponseCode::CompressionActive)),
        // RFC 6154 Section 6
        "USEATTR" => Ok((input, ResponseCode::UseAttr)),
        "UIDNEXT" => {
            // RFC 9051 Section 9 says nz-number, but we accept 0 per Postel's
            // law (RFC 1122 Section 1.2.2) because non-conformant servers
            // (e.g. some Dovecot configurations) send 0 for empty mailboxes.
            // Consistent with the STATUS parser's tolerance for UIDNEXT 0.
            let (input, _) = sp(input)?;
            let (input, n) = number(input)?;
            Ok((input, ResponseCode::UidNext(n)))
        }
        "UIDVALIDITY" => {
            // RFC 9051 Section 9 says nz-number, but we accept 0 per Postel's
            // law (RFC 1122 Section 1.2.2) because non-conformant servers
            // (e.g. some Dovecot configurations) send 0 for empty mailboxes.
            // Consistent with the STATUS parser's tolerance for UIDVALIDITY 0.
            let (input, _) = sp(input)?;
            let (input, n) = number(input)?;
            Ok((input, ResponseCode::UidValidity(n)))
        }
        "UNSEEN" => {
            // RFC 3501 Section 7.1: formally nz-number (>= 1) in the UNSEEN
            // response code. However, real servers (Dovecot) send `[UNSEEN 0]`
            // for empty mailboxes. Accept 0 per Postel's law (RFC 1122
            // Section 1.2.2) to preserve the structured information rather
            // than losing it into the text field.
            let (input, _) = sp(input)?;
            let (input, n) = number(input)?;
            Ok((input, ResponseCode::Unseen(n)))
        }
        "HIGHESTMODSEQ" => {
            // RFC 7162 Section 3.1.2.1: formally mod-sequence-value >= 1 in
            // response codes. However, real servers (Cyrus, Dovecot) send
            // `[HIGHESTMODSEQ 0]` for empty/new mailboxes. Accept 0 per
            // Postel's law (RFC 1122 Section 1.2.2) to preserve the
            // structured information rather than losing it into the text field.
            let (input, _) = sp(input)?;
            let (input, n) = number64(input)?;
            Ok((input, ResponseCode::HighestModSeq(n)))
        }
        "CAPABILITY" => {
            let (input, _) = sp(input)?;
            let (input, caps) = capability_list(input)?;
            Ok((input, ResponseCode::Capability(caps)))
        }
        "PERMANENTFLAGS" => {
            // RFC 3501 Section 7.1: flag-perm = flag / "\*"
            let (input, _) = sp(input)?;
            let (input, flags) = flag_perm_list(input)?;
            Ok((input, ResponseCode::PermanentFlags(flags)))
        }
        "BADCHARSET" => {
            // RFC 3501 Section 9 / RFC 9051 Section 9:
            //   "BADCHARSET" [SP "(" astring *(SP astring) ")"]
            // The parenthesized form formally requires ≥1 charset, but
            // accept empty `()` per Postel's law (RFC 1122 Section 1.2.2)
            //  -  rejecting would lose the structured BADCHARSET signal and
            // fall through to the unknown-code handler.
            // Postel's law (RFC 1122 Section 1.2.2): accept one or more
            // spaces between charsets  -  some non-conformant servers insert
            // extra whitespace.
            let (input, charsets) = opt(preceded(
                sp,
                delimited(
                    char('('),
                    separated_list0(take_while1(|b: u8| b == b' '), astring_utf8),
                    char(')'),
                ),
            ))
            .parse(input)?;
            Ok((
                input,
                ResponseCode::BadCharset(charsets.unwrap_or_default()),
            ))
        }
        "APPENDUID" => {
            // RFC 4315 Section 3: APPENDUID uidvalidity append-uid
            // append-uid = uniqueid / uid-set (uid-set for MULTIAPPEND)
            // RFC 3501 Section 9 says nz-number, but we accept 0 per
            // Postel's law (RFC 1122 Section 1.2.2) because non-conformant
            // servers may send 0. Consistent with the UIDVALIDITY response
            // code handler and STATUS parser's tolerance for uidvalidity 0.
            let (input, _) = sp(input)?;
            let (input, uid_validity) = number(input)?;
            let (input, _) = sp(input)?;
            let (input, uids) = uid_set(input)?;
            Ok((input, ResponseCode::AppendUid { uid_validity, uids }))
        }
        "COPYUID" => {
            // RFC 4315: COPYUID uidvalidity source-uids dest-uids
            // RFC 3501 Section 9 says nz-number, but we accept 0 per
            // Postel's law (RFC 1122 Section 1.2.2), consistent with
            // the UIDVALIDITY response code and STATUS parser tolerance.
            let (input, _) = sp(input)?;
            let (input, uid_validity) = number(input)?;
            let (input, _) = sp(input)?;
            let (input, source_uids) = uid_set(input)?;
            let (input, _) = sp(input)?;
            let (input, dest_uids) = uid_set(input)?;
            Ok((
                input,
                ResponseCode::CopyUid {
                    uid_validity,
                    source_uids,
                    dest_uids,
                },
            ))
        }
        "MODIFIED" => {
            // RFC 7162 Section 3.1.3: "MODIFIED" SP sequence-set
            // RFC 7162 Section 3.1.3 explicitly says STORE returns a message set,
            // while UID STORE returns a set of UIDs. Both use the `sequence-set`
            // grammar from RFC 3501 Section 9, which allows `*`.
            let (input, _) = sp(input)?;
            let (input, ranges) = sequence_set(input)?;
            Ok((input, ResponseCode::Modified(ranges)))
        }
        "MAILBOXID" => {
            // RFC 8474 Section 5.1: MAILBOXID (objectid-val)
            // objectid = 1*255(ALPHA / DIGIT / "_" / "-") per RFC 8474 Section 7.
            let (input, _) = sp(input)?;
            let (input, _) = char('(').parse(input)?;
            let (input, val) =
                map(objectid, |a| String::from_utf8_lossy(a).into_owned()).parse(input)?;
            let (input, _) = char(')').parse(input)?;
            Ok((input, ResponseCode::MailboxId(val)))
        }
        // RFC 5464 Section 4.2.1 / Section 4.3: METADATA response codes.
        // `[METADATA LONGENTRIES n]`, `[METADATA MAXSIZE n]`,
        // `[METADATA TOOMANY]`, `[METADATA NOPRIVATE]`.
        "METADATA" => {
            let (input, _) = sp(input)?;
            let (input, sub_atom) = atom(input)?;
            let sub = String::from_utf8_lossy(sub_atom).to_ascii_uppercase();
            match sub.as_str() {
                "LONGENTRIES" => {
                    let (input, _) = sp(input)?;
                    let (input, n) = number64(input)?;
                    Ok((input, ResponseCode::MetadataLongEntries(n)))
                }
                "MAXSIZE" => {
                    let (input, _) = sp(input)?;
                    let (input, n) = number64(input)?;
                    Ok((input, ResponseCode::MetadataMaxSize(n)))
                }
                "TOOMANY" => Ok((input, ResponseCode::MetadataTooMany)),
                "NOPRIVATE" => Ok((input, ResponseCode::MetadataNoPrivate)),
                _ => {
                    // Unknown METADATA sub-code  -  consume optional trailing
                    // value (forward compatibility with future RFC 5464
                    // extensions), matching the generic unknown response
                    // code handler's pattern.
                    let (input, val) =
                        opt(preceded(sp, take_while(|b: u8| b != b']'))).parse(input)?;
                    Ok((
                        input,
                        ResponseCode::Other {
                            name: format!("METADATA {sub}"),
                            value: val.map(|v| String::from_utf8_lossy(v).into_owned()),
                        },
                    ))
                }
            }
        }
        _ => {
            // Unknown code  -  capture optional value text
            let (input, value) = response_code_optional_tail(input)?;
            Ok((
                input,
                ResponseCode::Other {
                    name: code_str.to_owned(),
                    value,
                },
            ))
        }
    }
}

/// Parse `resp-text` (RFC 3501 Section 7.1 / RFC 9051 Section 7.1): optional `[response-code]` followed by optional human text.
///
/// The space between the response code and text is optional  -  some servers
/// (e.g. `GreenMail`) send `* OK [UIDVALIDITY 12345]\r\n` with no trailing text.
pub(super) fn resp_text(input: &[u8]) -> IResult<&[u8], (Option<ResponseCode>, String)> {
    let (input, code) = opt(response_code).parse(input)?;
    // Optional space before human-readable text
    let (input, _) = opt(char(' ')).parse(input)?;
    // Take everything up to CRLF as human-readable text.
    let (input, text_bytes) = take_while(|b: u8| b != b'\r' && b != b'\n').parse(input)?;
    let text = String::from_utf8_lossy(text_bytes).into_owned();
    Ok((input, (code, text)))
}
