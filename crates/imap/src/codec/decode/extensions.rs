//! Extension parsers: QUOTA (RFC 2087), ACL (RFC 4314), METADATA (RFC 5464),
//! THREAD (RFC 5256), and unknown response handling (RFC 9051 Section 2.2.2).

#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;

// ---------------------------------------------------------------------------
// QUOTA (RFC 2087) parsers
// ---------------------------------------------------------------------------

/// Parse `* QUOTA <root> (STORAGE <usage> <limit> ...)` (RFC 2087 Section 5.1).
///
/// The quota response contains a quota root name followed by a parenthesized
/// list of resource triplets: `(resource_name usage limit ...)`.
pub(super) fn parse_untagged_quota(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"QUOTA "[..]).parse(input)?;
    // The required trailing space makes this structurally distinct from
    // QUOTAROOT; parser ordering is not part of that distinction.
    // Quota root is a server-defined opaque identifier (RFC 2087 Section 2),
    // not a mailbox name  -  no MUTF-7 decoding.
    let (input, root) = astring_utf8(input)?;
    let (input, _) = sp(input)?;
    let (input, resources) = parse_quota_resource_list(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Quota { root, resources }))
}

/// Parse the parenthesized resource list in a QUOTA response (RFC 2087 Section 5.1).
///
/// Format: `(resource_name usage limit resource_name2 usage2 limit2 ...)`.
///
/// RFC 2087 Section 6 defines `quota_resource ::= atom SP number SP number`
/// where `number` is the standard IMAP `number` (u32). We parse usage and
/// limit as `number64` (u64) for Postel's-law leniency: STORAGE quotas are
/// in KB, so values above 4 GB (4 TB of storage) require more than 32 bits.
fn parse_quota_resource_list(input: &[u8]) -> IResult<&[u8], Vec<QuotaResource>> {
    let (input, _) = char('(').parse(input)?;
    let mut resources = Vec::new();
    let mut input = input;
    loop {
        // Skip optional whitespace
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input)?;
        input = rest;
        if input.first() == Some(&b')') {
            input = &input[1..];
            break;
        }
        // Parse resource triplet: name SP usage SP limit
        let (rest, name_bytes) = atom(input)?;
        let name = String::from_utf8_lossy(name_bytes).into_owned();
        let (rest, _) = sp(rest)?;
        let (rest, usage) = number64(rest)?;
        let (rest, _) = sp(rest)?;
        let (rest, limit) = number64(rest)?;
        resources.push(QuotaResource { name, usage, limit });
        input = rest;
    }
    Ok((input, resources))
}

/// Parse `* QUOTAROOT <mailbox> <root1> <root2> ...` (RFC 2087 Section 5.2).
///
/// The response contains a mailbox name followed by zero or more quota root names.
pub(super) fn parse_untagged_quotaroot(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"QUOTAROOT "[..]).parse(input)?;
    // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, mailbox_bytes) = astring(input)?;
    let mailbox = decode_mailbox_from_wire(&mailbox_bytes, utf8_mode);
    // Remaining tokens before CRLF are quota root names (space-separated astrings).
    let mut roots = Vec::new();
    let mut input = input;
    while let Ok((rest, _)) = sp(input) {
        // Check if we hit CRLF
        if rest.starts_with(b"\r\n") {
            break;
        }
        let Ok((rest, root)) = astring_utf8(rest) else {
            break;
        };
        roots.push(root);
        input = rest;
    }
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::QuotaRoot { mailbox, roots }))
}

// ---------------------------------------------------------------------------
// ACL (RFC 4314) parsers
// ---------------------------------------------------------------------------

/// Parse `* ACL <mailbox> <id1> <rights1> <id2> <rights2> ...` (RFC 4314 Section 3.6).
///
/// The ACL response lists all access control entries for the specified mailbox.
pub(super) fn parse_untagged_acl(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"ACL "[..]).parse(input)?;
    // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, mailbox_bytes) = astring(input)?;
    let mailbox = decode_mailbox_from_wire(&mailbox_bytes, utf8_mode);
    let mut entries = Vec::new();
    let mut input = input;
    while let Ok((rest, _)) = sp(input) {
        if rest.starts_with(b"\r\n") {
            break;
        }
        let Ok((rest, identifier)) = astring_utf8(rest) else {
            break;
        };
        let Ok((rest, _)) = sp(rest) else {
            break;
        };
        let Ok((rest, rights)) = astring_utf8(rest) else {
            break;
        };
        entries.push(AclEntry { identifier, rights });
        input = rest;
    }
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Acl { mailbox, entries }))
}

/// Parse `* MYRIGHTS <mailbox> <rights>` (RFC 4314 Section 3.8).
pub(super) fn parse_untagged_myrights(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"MYRIGHTS "[..]).parse(input)?;
    // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, mailbox_bytes) = astring(input)?;
    let mailbox = decode_mailbox_from_wire(&mailbox_bytes, utf8_mode);
    let (input, _) = sp(input)?;
    let (input, rights) = astring_utf8(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::MyRights { mailbox, rights }))
}

/// Parse `* LISTRIGHTS <mailbox> <id> <required> <optional1> <optional2> ...`
/// (RFC 4314 Section 3.7).
///
/// The required rights string is followed by zero or more optional rights
/// strings, each representing a set of rights that can be granted independently.
pub(super) fn parse_untagged_listrights(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"LISTRIGHTS "[..]).parse(input)?;
    // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, mailbox_bytes) = astring(input)?;
    let mailbox = decode_mailbox_from_wire(&mailbox_bytes, utf8_mode);
    let (input, _) = sp(input)?;
    let (input, identifier) = astring_utf8(input)?;
    let (input, _) = sp(input)?;
    let (input, required) = astring_utf8(input)?;
    // Remaining tokens are optional rights groups
    let mut optional = Vec::new();
    let mut input = input;
    while let Ok((rest, _)) = sp(input) {
        if rest.starts_with(b"\r\n") {
            break;
        }
        let Ok((rest, rights_group)) = astring_utf8(rest) else {
            break;
        };
        optional.push(rights_group);
        input = rest;
    }
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        UntaggedResponse::ListRights {
            mailbox,
            identifier,
            required,
            optional,
        },
    ))
}

// ---------------------------------------------------------------------------
// METADATA (RFC 5464) parsers
// ---------------------------------------------------------------------------

/// Parse `* METADATA "<mailbox>" ...` (RFC 5464 Section 4.4).
///
/// Two forms:
/// 1. Parenthesized entry/value pairs: `(entry1 value1 entry2 value2 ...)`
/// 2. Unsolicited entry-list (RFC 5464 Section 4.4.2): space-separated entry names
///    without values, terminated by CRLF. Each produces `MetadataEntry { name, value: None }`.
pub(super) fn parse_untagged_metadata(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"METADATA "[..]).parse(input)?;
    // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, mailbox_bytes) = astring(input)?;
    let mailbox = decode_mailbox_from_wire(&mailbox_bytes, utf8_mode);
    let (input, _) = sp(input)?;

    let (input, entries) = if input.first() == Some(&b'(') {
        // Form 1: parenthesized entry/value pairs
        parse_metadata_entry_list(input)?
    } else {
        // Form 2: unsolicited entry-list  -  space-separated astring entry names
        // without values, terminated by CRLF (RFC 5464 Section 4.4.2).
        let mut entries = Vec::new();
        let (rest, first_name) = astring_utf8(input)?;
        entries.push(MetadataEntry {
            name: first_name,
            value: None,
        });
        let mut input = rest;
        loop {
            if input.starts_with(b"\r\n") || input.is_empty() {
                break;
            }
            let Ok((rest, _)) = sp(input) else {
                break;
            };
            if rest.starts_with(b"\r\n") {
                break;
            }
            let Ok((rest, name)) = astring_utf8(rest) else {
                break;
            };
            entries.push(MetadataEntry { name, value: None });
            input = rest;
        }
        (input, entries)
    };

    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Metadata { mailbox, entries }))
}

/// Parse the parenthesized entry/value list in a METADATA response (RFC 5464 Section 4.4).
///
/// Format: `(entry1 value1 entry2 value2 ...)` where values are strings or NIL.
fn parse_metadata_entry_list(input: &[u8]) -> IResult<&[u8], Vec<MetadataEntry>> {
    let (input, _) = char('(').parse(input)?;
    let mut entries = Vec::new();
    let mut input = input;
    loop {
        // Skip optional whitespace
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input)?;
        input = rest;
        if input.first() == Some(&b')') {
            input = &input[1..];
            break;
        }
        // Parse entry name (astring per RFC 5464 Section 4.4) and value
        // RFC 5464 Section 5: value = nstring / literal8  -  use raw byte parser
        // to preserve binary fidelity for literal8 values.
        let (rest, name) = astring_utf8(input)?;
        let (rest, _) = sp(rest)?;
        let (rest, value) = nstring(rest)?;
        entries.push(MetadataEntry { name, value });
        input = rest;
    }
    Ok((input, entries))
}

// ---------------------------------------------------------------------------
// THREAD (RFC 5256) parsers
// ---------------------------------------------------------------------------

/// Maximum recursion depth for nested THREAD groups (defense-in-depth).
///
/// RFC 5256 Section 4 does not specify a maximum nesting depth, but unbounded
/// recursion on attacker-controlled input risks stack overflow. 64 levels is
/// far beyond any legitimate thread depth observed in the wild.
const MAX_THREAD_NESTING_DEPTH: u32 = 64;

/// Parse `* THREAD (uid1 uid2 (uid3 uid4)) ...` (RFC 5256 Section 4).
///
/// The THREAD response is a list of thread groups at the top level.
/// Each `(...)` is a separate thread root. Groups can be separated by
/// spaces or adjacent (e.g. `(1)(2)`).
pub(super) fn parse_untagged_thread(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"THREAD"[..]).parse(input)?;
    let mut threads = Vec::new();
    let mut input = input;
    loop {
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input)?;
        input = rest;
        if input.starts_with(b"\r\n") || input.is_empty() {
            break;
        }
        if input.first() != Some(&b'(') {
            break;
        }
        let (rest, node) = parse_thread_node(input, 0)?;
        threads.push(node);
        input = rest;
    }
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Thread(threads)))
}

/// Catch-all for unrecognized untagged responses (RFC 9051 Section 2.2.2).
///
/// Clients MUST tolerate extension responses they do not implement.
/// This scans through the response, properly skipping over:
/// - Quoted strings `"..."` with `\"` and `\\` escapes (RFC 3501 Section 9: quoted)
/// - Literal strings `{N}\r\n<N bytes>` (RFC 3501 Section 9: literal)
/// - LITERAL+ `{N+}\r\n<N bytes>` (RFC 7888)
/// - Literal8 `~{N}\r\n<N bytes>` (RFC 3516 / RFC 6855 Section 4)
///
/// Only the final CRLF (not embedded in a literal or quoted string) terminates
/// the response.
pub(super) fn parse_untagged_unknown(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    // An extension response is allowed here, but a malformed form of a
    // response we claim to understand is a server protocol violation. Let it
    // reach WireReader as Error::Parse rather than silently routing it as an
    // unsolicited unknown response.
    if starts_known_untagged_response(input) {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    let (input, raw) = scan_unknown_response(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        UntaggedResponse::Unknown(String::from_utf8_lossy(raw).into_owned()),
    ))
}

/// Numbered response failures need special treatment: FETCH has an open
/// attribute grammar, while EXISTS, RECENT and EXPUNGE are fully modelled.
/// Direct responses are committed by their dispatch rows in response.rs.
fn starts_known_untagged_response(input: &[u8]) -> bool {
    const NUMBERED_KEYWORDS: &[&[u8]] = &[b"EXISTS", b"RECENT", b"EXPUNGE"];

    fn starts_keyword(value: &[u8], keywords: &[&[u8]]) -> bool {
        keywords.iter().any(|keyword| {
            value
                .get(..keyword.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
                && value
                    .get(keyword.len())
                    .is_none_or(|next| matches!(*next, b' ' | b'\r' | b'\n'))
        })
    }

    let digits = input.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == 0 {
        return false;
    }
    // The numbered parsers tolerate runs of spaces after the number
    // (Postel's law), so the guard must too or it stops recognizing exactly
    // the malformed-but-recognizable forms it exists for.
    let spaces = input[digits..].iter().take_while(|b| **b == b' ').count();
    if spaces == 0 {
        return false;
    }
    let keyword = &input[digits + spaces..];
    if starts_keyword(keyword, NUMBERED_KEYWORDS) {
        return true;
    }
    // FETCH stays out of the numbered table because its msg-att body is
    // open-ended, but its sequence number is nz-number, a grammar this codec
    // fully implements. A zero sequence number is a contract violation on a
    // fully recognized form, and tolerating it silently drops the FETCH.
    input[..digits].iter().all(|b| *b == b'0') && starts_keyword(keyword, &[b"FETCH"])
}

/// Scans through an unknown response body, consuming all bytes including any
/// embedded literals, until the response-terminating CRLF is reached.
///
/// Returns the consumed bytes (including literal data) as a single slice by
/// tracking the start position and total bytes consumed.
///
/// Handles:
/// - Quoted strings with `\"` and `\\` escapes (RFC 3501 Section 9)
/// - Literal `{N}\r\n<data>` (RFC 3501 Section 9)
/// - LITERAL+ `{N+}\r\n<data>` (RFC 7888)
/// - Literal8 `~{N}\r\n<data>` (RFC 3516 / RFC 6855 Section 4)
pub(super) fn scan_unknown_response(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let start = input;
    let mut pos = 0;

    while pos < input.len() {
        let b = input[pos];

        // CRLF that is NOT inside a literal/quoted string terminates the response.
        if b == b'\r' || b == b'\n' {
            return Ok((&input[pos..], &start[..pos]));
        }

        // Quoted string: skip to closing `"`, handling `\"` and `\\` escapes
        // (RFC 3501 Section 9: quoted = DQUOTE *QUOTED-CHAR DQUOTE).
        // Break on CR/LF since they are not valid QUOTED-CHAR; an unterminated
        // quote must not consume past the response-terminating CRLF.
        if b == b'"' {
            pos += 1; // skip opening quote
            while pos < input.len() {
                match input[pos] {
                    b'\\' => {
                        // Escaped character  -  skip the backslash and the next byte
                        // (RFC 3501 Section 9: quoted-specials = DQUOTE / "\").
                        // Guard against truncated input: if the backslash is the
                        // last byte, or the next byte is CR/LF (not a valid
                        // QUOTED-CHAR), break out so the outer loop can find the
                        // response-terminating CRLF.
                        if pos + 1 < input.len()
                            && input[pos + 1] != b'\r'
                            && input[pos + 1] != b'\n'
                        {
                            pos += 2;
                        } else {
                            pos += 1;
                            break;
                        }
                    }
                    b'"' => {
                        pos += 1; // skip closing quote
                        break;
                    }
                    // RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
                    // If we hit CR/LF, the quote is unterminated; break out
                    // so the outer loop can find the response-terminating CRLF.
                    b'\r' | b'\n' => break,
                    _ => {
                        pos += 1;
                    }
                }
            }
            continue;
        }

        // Literal8: `~{N}\r\n<N bytes>` (RFC 3516 / RFC 6855 Section 4).
        // Check for `~` followed by `{`.
        if b == b'~'
            && pos + 1 < input.len()
            && input[pos + 1] == b'{'
            && let Some(advance) = try_skip_literal(&input[pos + 1..])
        {
            pos += 1 + advance; // 1 for `~`, plus the literal bytes
            continue;
        }

        // Literal `{N}\r\n<data>` or LITERAL+ `{N+}\r\n<data>`
        // (RFC 3501 Section 9 / RFC 7888).
        if b == b'{'
            && let Some(advance) = try_skip_literal(&input[pos..])
        {
            pos += advance;
            continue;
        }

        pos += 1;
    }

    // Ran out of input without finding a terminating CRLF.
    // In complete-mode parsing, this is an error (not Incomplete).
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::CrLf,
    )))
}

/// Try to parse and skip a literal at the current position.
///
/// Expects `input` to start with `{`. Parses `{N}\r\n` or `{N+}\r\n`
/// (RFC 3501 Section 9 / RFC 7888), then skips N bytes of literal data.
///
/// Returns `Some(total_bytes_consumed)` on success, or `None` if the
/// pattern does not match (e.g., `{` is not followed by digits).
pub(super) fn try_skip_literal(input: &[u8]) -> Option<usize> {
    // input[0] must be `{`
    if input.is_empty() || input[0] != b'{' {
        return None;
    }

    let mut pos = 1; // skip `{`

    // Parse the decimal count: number = 1*DIGIT (RFC 3501 Section 9).
    let digit_start = pos;
    while pos < input.len() && input[pos].is_ascii_digit() {
        pos += 1;
    }
    let digit_end = pos;
    if digit_end == digit_start {
        // No digits after `{`  -  not a literal.
        return None;
    }

    // Optional `+` for LITERAL+ (RFC 7888).
    if pos < input.len() && input[pos] == b'+' {
        pos += 1;
    }

    // Must have `}\r\n`.
    if pos + 2 >= input.len()
        || input[pos] != b'}'
        || input[pos + 1] != b'\r'
        || input[pos + 2] != b'\n'
    {
        return None;
    }
    pos += 3; // skip `}\r\n`

    // Parse the count from the digit substring.
    // All bytes in digit_start..digit_end are ASCII digits, so from_utf8 is infallible.
    let count_str = std::str::from_utf8(&input[digit_start..digit_end]).ok()?;
    let count: usize = count_str.parse().ok()?;

    // Skip the literal data.
    // Use checked_add to prevent wrapping on crafted counts near usize::MAX
    // (RFC 3501 Section 9 / RFC 9051 Section 9).
    let total = pos.checked_add(count)?;
    if total > input.len() {
        // Not enough data for the literal body  -  need more input.
        return None;
    }

    Some(total)
}

/// Parse a single thread node: a parenthesized group (RFC 5256 Section 4).
///
/// Within a parenthesized group, bare UIDs form a linear chain (each is the
/// child of the previous). Nested `(...)` groups are sibling branches that
/// attach to the last bare UID in the chain.
///
/// Examples (RFC 5256 Section 4):
/// - `(2)`  -  single message, no children
/// - `(3 6)`  -  3 -> 6 (message 6 replies to 3)
/// - `(4 23 44)`  -  4 -> 23 -> 44 (linear chain)
/// - `(3 6 (4 23)(44 7 96))`  -  3 -> 6 -> [(4 -> 23), (44 -> 7 -> 96)]
/// - `((1)(2))`  -  dummy root (id=None) with children 1, 2
fn parse_thread_node(input: &[u8], depth: u32) -> IResult<&[u8], ThreadNode> {
    // Defense-in-depth: reject excessively nested threads to prevent stack
    // overflow on crafted input (RFC 5256 Section 4 does not cap depth).
    if depth > MAX_THREAD_NESTING_DEPTH {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    let (input, _) = char('(').parse(input)?;
    let mut input = input;

    // The first element is either a bare UID or another '(' (dummy parent).
    // UIDs are nz-number per RFC 3501 Section 9 (uniqueid = nz-number).
    let root_id: Option<u32> = if input.first().is_none_or(|b| *b == b'(' || *b == b')') {
        // Dummy parent (RFC 5256 Section 4)  -  no UID.
        None
    } else {
        let (rest, uid) = nz_number(input)?;
        input = rest;
        Some(uid)
    };

    // Collect children. Bare UIDs form a chain; nested groups are siblings.
    // We track the "chain tip"  -  the deepest node in the linear UID chain.
    // Nested groups attach as children of the chain tip.
    let mut chain_uids: Vec<Option<u32>> = Vec::new();
    let mut branch_groups: Vec<Vec<ThreadNode>> = Vec::new();

    loop {
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input)?;
        input = rest;
        if input.first() == Some(&b')') {
            input = &input[1..];
            break;
        }

        if input.first() == Some(&b'(') {
            // RFC 5256 permits nested groups only after the bare UID chain.
            // Keep accepting other shapes without assigning them new meaning:
            // this bucket layout remains a parser detail for non-conformant input.
            let (rest, child) = parse_thread_node(input, depth + 1)?;
            // Accumulate the branch in the current chain slot.
            if let Some(last) = branch_groups.last_mut() {
                last.push(child);
            } else {
                branch_groups.push(vec![child]);
            }
            input = rest;
        } else {
            // Bare UID  -  extends the linear chain.
            // UIDs are nz-number per RFC 3501 Section 9 (uniqueid = nz-number).
            let (rest, uid) = nz_number(input)?;
            chain_uids.push(Some(uid));
            // Start the slot for branches following this UID.
            branch_groups.push(Vec::new());
            input = rest;
        }
    }

    // Build the tree from the chain and branch groups.
    //
    // chain_uids:    [6]
    // branch_groups: [[], [(4->23), (44->7->96)]]
    //
    // For `(3 6 (4 23)(44 7 96))`:
    //   root_id = 3
    //   chain_uids = [6]
    //   branch_groups = [[], [(4->23), (44->7->96)]]
    //
    // Result: 3 -> 6 -> [(4->23), (44->7->96)]
    //
    // We build bottom-up: start from the deepest chain UID.
    let children = build_thread_tree(&chain_uids, &branch_groups);

    let mut node = ThreadNode {
        id: root_id,
        children,
    };

    // RFC 5256 Section 5: `thread-nested = 2*thread-list`  -  a dummy parent
    // (id == None) must have at least 2 children.  Non-conformant servers
    // may send `((msg))` with only 1 child; accept per Postel's law
    // (RFC 1122 Section 1.2.2) but collapse the redundant wrapper so the
    // result is equivalent to the valid form `(msg)`.
    if node.id.is_none() && node.children.len() == 1 {
        // Safe: `Vec::pop` on a len()==1 vec always returns `Some`.
        if let Some(child) = node.children.pop() {
            node = child;
        }
    }

    Ok((input, node))
}

/// Build a thread tree from a chain of UIDs and their branch groups (RFC 5256 Section 4).
///
/// `chain_uids[i]` are bare UIDs that form a linear chain.
/// `branch_groups[i]` contains nested groups parsed after `chain_uids[i]`.
/// If groups appear before any UID, `chain_uids` is empty and `branch_groups`
/// has one entry with all those groups.
pub(super) fn build_thread_tree(
    chain_uids: &[Option<u32>],
    branch_groups: &[Vec<ThreadNode>],
) -> Vec<ThreadNode> {
    if chain_uids.is_empty() {
        // No bare UIDs  -  all children are nested groups (e.g. `((1)(2))`).
        return branch_groups.iter().flatten().cloned().collect();
    }

    // Build bottom-up from the last chain UID.
    let last = chain_uids.len() - 1;

    // Start with the branches after the last UID as its children.
    let last_children: Vec<ThreadNode> = if last < branch_groups.len() {
        branch_groups[last].clone()
    } else {
        vec![]
    };

    // Wrap the last UID.
    let mut result = ThreadNode {
        id: chain_uids[last],
        children: last_children,
    };

    // Walk backwards through the remaining chain UIDs.
    for i in (0..last).rev() {
        let mut children: Vec<ThreadNode> = if i < branch_groups.len() {
            branch_groups[i].clone()
        } else {
            vec![]
        };
        // The next UID in the chain is a child.
        children.push(result);
        result = ThreadNode {
            id: chain_uids[i],
            children,
        };
    }

    vec![result]
}
