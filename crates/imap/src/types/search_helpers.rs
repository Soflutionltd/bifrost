/// Append a DQUOTE-quoted IMAP string to `buf`, escaping backslash and
/// double-quote per RFC 3501 Section 9: `quoted-specials = DQUOTE / "\"`.
///
/// This always uses the quoted form. The SEARCH criteria ABNF uses
/// `astring` for string arguments (RFC 3501 Section 6.4.4), and quoted
/// strings are always valid `astring` values (RFC 3501 Section 9:
/// `astring = 1*ASTRING-CHAR / string`, `string = quoted / literal`).
///
/// Returns an error if `value` contains bytes that cannot appear in an
/// IMAP quoted string:
/// - NUL (`\0`): not a valid CHAR per RFC 3501 Section 9
///   (`CHAR = <any 7-bit US-ASCII character except NUL>`)
/// - CR (`\r`) and LF (`\n`): not TEXT-CHAR per RFC 3501 Section 9
///   (`TEXT-CHAR = <any CHAR except CR and LF>`,
///   `QUOTED-CHAR = TEXT-CHAR / quoted-specials`)
pub(super) fn quote_imap_string(buf: &mut String, value: &str) -> Result<(), crate::Error> {
    // RFC 3501 Section 9: QUOTED-CHAR = TEXT-CHAR / quoted-specials,
    // TEXT-CHAR = <any CHAR except CR and LF>,
    // CHAR = <any 7-bit US-ASCII except NUL, 0x01 - 0x7F>.
    // NUL, CR, and LF cannot appear in a quoted string.
    for &b in value.as_bytes() {
        if b == 0 {
            return Err(crate::Error::InvalidInput(
                "quoted string must not contain NUL  -  NUL is not a valid CHAR \
                 (RFC 3501 Section 9: CHAR = <any 7-bit US-ASCII except NUL>)"
                    .into(),
            ));
        }
        if b == b'\r' {
            return Err(crate::Error::InvalidInput(
                "quoted string must not contain CR  -  CR is not a TEXT-CHAR \
                 (RFC 3501 Section 9: TEXT-CHAR = <any CHAR except CR and LF>)"
                    .into(),
            ));
        }
        if b == b'\n' {
            return Err(crate::Error::InvalidInput(
                "quoted string must not contain LF  -  LF is not a TEXT-CHAR \
                 (RFC 3501 Section 9: TEXT-CHAR = <any CHAR except CR and LF>)"
                    .into(),
            ));
        }
    }
    buf.push('"');
    for ch in value.chars() {
        // RFC 3501 Section 9: quoted-specials (backslash, double-quote)
        // must be escaped with a preceding backslash.
        if ch == '\\' || ch == '"' {
            buf.push('\\');
        }
        buf.push(ch);
    }
    buf.push('"');
    Ok(())
}

/// Validate that `date` conforms to the IMAP `date` production from
/// RFC 3501 Section 9:
///
/// ```text
/// date        = date-text / DQUOTE date-text DQUOTE
/// date-text   = date-day "-" date-month "-" date-year
/// date-day    = 1*2DIGIT
/// date-month  = "Jan" / "Feb" / "Mar" / "Apr" / "May" / "Jun" /
///               "Jul" / "Aug" / "Sep" / "Oct" / "Nov" / "Dec"
/// date-year   = 4DIGIT
/// ```
///
/// The SEARCH command uses the bare `date` (without DQUOTE wrapping),
/// so this function validates `date-text` directly.
pub(super) fn validate_imap_date(date: &str) -> Result<(), crate::Error> {
    // RFC 3501 Section 9: date-month = "Jan" / "Feb" / ... / "Dec".
    const VALID_MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    // RFC 3501 Section 9: date-text = date-day "-" date-month "-" date-year.
    let parts: Vec<&str> = date.splitn(3, '-').collect();
    if parts.len() != 3 {
        return Err(crate::Error::InvalidInput(format!(
            "invalid IMAP date {date:?}  -  must be date-day \"-\" date-month \"-\" date-year \
             (RFC 3501 Section 9)"
        )));
    }
    let (day, month, year) = (parts[0], parts[1], parts[2]);

    // RFC 3501 Section 9: date-day = 1*2DIGIT.
    if day.is_empty() || day.len() > 2 || !day.bytes().all(|b| b.is_ascii_digit()) {
        return Err(crate::Error::InvalidInput(format!(
            "invalid IMAP date day {day:?}  -  must be 1 or 2 digits \
             (RFC 3501 Section 9: date-day = 1*2DIGIT)"
        )));
    }

    if !VALID_MONTHS
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(month))
    {
        return Err(crate::Error::InvalidInput(format!(
            "invalid IMAP date month {month:?}  -  must be one of Jan, Feb, Mar, Apr, \
             May, Jun, Jul, Aug, Sep, Oct, Nov, Dec \
             (RFC 3501 Section 9: date-month)"
        )));
    }

    // RFC 3501 Section 9: date-year = 4DIGIT.
    if year.len() != 4 || !year.bytes().all(|b| b.is_ascii_digit()) {
        return Err(crate::Error::InvalidInput(format!(
            "invalid IMAP date year {year:?}  -  must be exactly 4 digits \
             (RFC 3501 Section 9: date-year = 4DIGIT)"
        )));
    }

    // Use the APPEND date-time validator's calendar rules as well as its
    // case-insensitive month vocabulary. SEARCH permits an unpadded day.
    let fixed_day = if day.len() == 1 {
        format!("0{day}")
    } else {
        day.to_owned()
    };
    crate::codec::encode::validate_append_datetime(&format!(
        "{fixed_day}-{month}-{year} 00:00:00 +0000"
    ))
    .map_err(|error| crate::Error::InvalidInput(error.to_string()))?;

    Ok(())
}

/// If `criteria` contains multiple search keys (i.e. contains a space
/// outside of quoted strings), wrap it in parentheses to form a single
/// `search-key` per RFC 3501 Section 6.4.4:
/// `search-key =/ "(" search-key *(SP search-key) ")"`.
///
/// Single-key criteria are emitted bare.
pub(super) fn push_parenthesized_if_compound(buf: &mut String, criteria: &str) {
    if is_compound(criteria) {
        buf.push('(');
        buf.push_str(criteria);
        buf.push(')');
    } else {
        buf.push_str(criteria);
    }
}

/// Determine whether a criteria string contains multiple top-level search keys.
///
/// A compound criteria has spaces outside of quoted strings, indicating
/// multiple keys at the top level. Single keywords like `UNSEEN`, or
/// single key+argument pairs like `FROM "alice"`, are not compound.
///
/// This function tracks quoted-string boundaries to avoid splitting on
/// spaces inside quoted arguments.
pub(super) fn is_compound(criteria: &str) -> bool {
    // Count top-level tokens (tokens separated by unquoted spaces).
    // A single keyword like "UNSEEN" = 1 token, not compound.
    // A key+arg like `FROM "alice"` = 2 tokens, not compound (it's one search-key).
    // Multiple keys like `UNSEEN FROM "alice"` = 3+ tokens, compound.
    //
    // Search keys that take an argument consume exactly one token after the
    // keyword. Keys that take two arguments (HEADER, OR) consume two tokens.
    // We need to count actual search-keys, not just whitespace-separated tokens.
    let tokens = count_top_level_tokens(criteria);

    // Single-argument keys: keyword + 1 argument = 2 tokens = 1 search-key.
    // Two-argument keys (HEADER): keyword + 2 arguments = 3 tokens = 1 search-key.
    // OR: keyword + 2 search-key arguments (already parenthesized) = 3 tokens = 1 search-key.
    // NOT: keyword + 1 search-key argument = 2 tokens = 1 search-key.
    //
    // Rather than trying to fully parse the IMAP grammar, we use a simpler
    // heuristic: if the first token is a known single-arg keyword, then
    // <=2 tokens is one search-key. If it's a known double-arg keyword,
    // <=3 tokens is one search-key. Otherwise <=1 token is one search-key.
    let first_token = top_level_first_token(criteria);

    let max_tokens_for_single_key = match first_token.to_uppercase().as_str() {
        // RFC 3501 Section 6.4.4: these take one astring/date/number argument.
        // RFC 7162 Section 3.1.5: MODSEQ (simple form) takes one number argument.
        "BCC" | "CC" | "FROM" | "TO" | "SUBJECT" | "BODY" | "TEXT" | "BEFORE" | "ON" | "SINCE"
        | "SENTBEFORE" | "SENTON" | "SENTSINCE" | "LARGER" | "SMALLER" | "UID" | "KEYWORD"
        | "UNKEYWORD" | "NOT" | "MODSEQ" => 2,
        // RFC 3501 Section 6.4.4: HEADER takes header-name + astring (2 args).
        // OR takes two search-keys (2 args, but they may be parenthesized groups).
        "HEADER" | "OR" => 3,
        // All other keys (flag keywords, ALL, bare sequence sets) take no arguments.
        _ => 1,
    };

    tokens > max_tokens_for_single_key
}

/// Count top-level tokens in a criteria string, respecting quoted strings
/// and parenthesized groups.
///
/// A "token" is a run of non-space characters at the top level, or a
/// quoted string (which counts as one token including the quotes), or a
/// parenthesized group (which counts as one token).
pub(super) fn count_top_level_tokens(s: &str) -> usize {
    let mut count = 0;
    let mut in_quote = false;
    let mut in_token = false;
    let mut paren_depth: u32 = 0;
    let mut prev_backslash = false;

    for ch in s.chars() {
        if in_quote {
            // RFC 3501 Section 9: backslash escapes the next character
            // inside a quoted string.
            if prev_backslash {
                prev_backslash = false;
                continue;
            }
            if ch == '\\' {
                prev_backslash = true;
                continue;
            }
            if ch == '"' {
                in_quote = false;
            }
            continue;
        }

        if ch == '"' {
            if !in_token {
                count += 1;
                in_token = true;
            }
            in_quote = true;
            continue;
        }

        if ch == '(' {
            if paren_depth == 0 && !in_token {
                count += 1;
                in_token = true;
            }
            paren_depth = paren_depth.saturating_add(1);
            continue;
        }

        if ch == ')' {
            paren_depth = paren_depth.saturating_sub(1);
            if paren_depth == 0 {
                in_token = false;
            }
            continue;
        }

        if paren_depth > 0 {
            continue;
        }

        if ch == ' ' {
            in_token = false;
        } else if !in_token {
            count += 1;
            in_token = true;
        }
    }
    count
}

/// Extract the first top-level token from a criteria string.
fn top_level_first_token(s: &str) -> &str {
    let trimmed = s.trim_start();
    // Find the end of the first whitespace-delimited token.
    match trimmed.find(' ') {
        Some(pos) => &trimmed[..pos],
        None => trimmed,
    }
}
