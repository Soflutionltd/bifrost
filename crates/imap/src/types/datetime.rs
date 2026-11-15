//! Shared IMAP calendar and date-time operand validation.

/// Validate an APPEND date-time string against RFC 3501 Section 9.
///
/// The expected format (without the surrounding DQUOTEs, which the caller adds):
///
/// ```text
/// date-day-fixed "-" date-month "-" date-year SP time SP zone
/// date-day-fixed = (SP DIGIT) / 2DIGIT          ; 1-31, space-padded or zero-padded
/// date-month     = "Jan" / "Feb" / "Mar" / "Apr" / "May" / "Jun" /
///                  "Jul" / "Aug" / "Sep" / "Oct" / "Nov" / "Dec"
/// date-year      = 4DIGIT
/// time           = 2DIGIT ":" 2DIGIT ":" 2DIGIT  ; HH:MM:SS
/// zone           = ("+" / "-") 4DIGIT
/// ```
#[allow(clippy::too_many_lines)]
pub(crate) fn validate_append_datetime(date: &str) -> Result<(), crate::Error> {
    const MONTHS: [&[u8]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];

    let b = date.as_bytes();

    // Expected length: " 7-Jul-1996 02:44:25 -0700" = 26 bytes.
    // date-day-fixed (2) + "-" (1) + month (3) + "-" (1) + year (4)
    //   + SP (1) + time (8) + SP (1) + zone (5) = 26
    if b.len() != 26 {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "expected 26 characters, got {}  -  \
             date-time must be date-day-fixed \"-\" date-month \"-\" date-year \
             SP time SP zone (RFC 3501 Section 9)",
                b.len()
            )),
        ));
    }

    // date-day-fixed = (SP DIGIT) / 2DIGIT (RFC 3501 Section 9).
    //
    // The `(SP DIGIT)` form covers space-padded single-digit days (` 1`-` 9`).
    // The `2DIGIT` form covers any two-digit day; semantically valid days are
    // `01`-`31`. Zero-padded single-digit days (`01`-`09`) are valid per the
    // `2DIGIT` alternative  -  RFC 5234 Section 3.4 defines `2DIGIT = DIGIT DIGIT`.
    let day_hi = b[0];
    let day_lo = b[1];
    let valid_day = match day_hi {
        b' ' => day_lo.is_ascii_digit() && day_lo != b'0',
        b'0' => (b'1'..=b'9').contains(&day_lo),
        b'1'..=b'2' => day_lo.is_ascii_digit(),
        b'3' => day_lo == b'0' || day_lo == b'1',
        _ => false,
    };
    if !valid_day {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid day {:?}  -  date-day-fixed must be (SP DIGIT) / 2DIGIT \
             with values 1-31 (RFC 3501 Section 9)",
                std::str::from_utf8(&b[0..2]).unwrap_or("??")
            )),
        ));
    }

    // First separator: "-" (RFC 3501 Section 9).
    if b[2] != b'-' {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "expected '-' at position 2, got {:?} (RFC 3501 Section 9)",
                b[2] as char
            )),
        ));
    }

    // date-month (RFC 3501 Section 9).
    // RFC 3501 Section 9 paragraph (1): "all alphabetic characters are
    // case-insensitive ... Implementations MUST accept these strings in a
    // case-insensitive fashion."  RFC 5234 Section 2.3 confirms ABNF
    // literal strings are case-insensitive.
    let month = &b[3..6];
    if !MONTHS.iter().any(|m| m.eq_ignore_ascii_case(month)) {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid month {:?}  -  date-month must be Jan/Feb/.../Dec (RFC 3501 Section 9)",
                std::str::from_utf8(month).unwrap_or("???")
            )),
        ));
    }

    let day: u8 = match day_hi {
        b' ' => day_lo - b'0',
        _ => (day_hi - b'0') * 10 + (day_lo - b'0'),
    };

    // Second separator: "-" (RFC 3501 Section 9).
    if b[6] != b'-' {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "expected '-' at position 6, got {:?} (RFC 3501 Section 9)",
                b[6] as char
            )),
        ));
    }

    // date-year = 4DIGIT (RFC 3501 Section 9).
    if !b[7..11].iter().all(u8::is_ascii_digit) {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid year {:?}  -  date-year must be 4DIGIT (RFC 3501 Section 9)",
                std::str::from_utf8(&b[7..11]).unwrap_or("????")
            )),
        ));
    }

    // Parse year for the day-vs-month cross-check below (needed for
    // leap-year validation of February 29).
    let year: u16 = u16::from(b[7] - b'0') * 1000
        + u16::from(b[8] - b'0') * 100
        + u16::from(b[9] - b'0') * 10
        + u16::from(b[10] - b'0');

    // Day-vs-month cross-check (RFC 3501 Section 9).
    //
    // The ABNF `date-day-fixed` allows 1-31 structurally, but not every day
    // is valid for every month.  Postel's law says "be conservative in what
    // you send", so we reject invalid dates before transmitting them.
    //   - 31-day months: Jan, Mar, May, Jul, Aug, Oct, Dec
    //   - 30-day months: Apr, Jun, Sep, Nov
    //   - Feb: 29 in leap years, 28 otherwise
    let max_day: u8 = match month[0].to_ascii_lowercase() {
        // Feb  -  28 or 29 depending on leap year.
        // Leap year rules: divisible by 400 -> leap; else by 100 -> not;
        // else by 4 -> leap; else -> not.
        b'f' => {
            let y = u32::from(year);
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        // Apr -> 30
        b'a' if month[1].eq_ignore_ascii_case(&b'p') => 30,
        // Jun -> 30
        b'j' if month[1].eq_ignore_ascii_case(&b'u') && month[2].eq_ignore_ascii_case(&b'n') => 30,
        // Sep, Nov -> 30
        b's' | b'n' => 30,
        // Jan, Mar, May, Jul, Aug, Oct, Dec -> 31
        _ => 31,
    };
    if day > max_day {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid day {} for month {:?} year {}  -  maximum is {} (RFC 3501 Section 9)",
                day,
                std::str::from_utf8(month).unwrap_or("???"),
                year,
                max_day
            )),
        ));
    }

    // SP between date and time (RFC 3501 Section 9).
    if b[11] != b' ' {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "expected SP at position 11, got {:?} (RFC 3501 Section 9)",
                b[11] as char
            )),
        ));
    }

    // time = 2DIGIT ":" 2DIGIT ":" 2DIGIT (RFC 3501 Section 9).
    let time = &b[12..20];
    let valid_time = time[0].is_ascii_digit()
        && time[1].is_ascii_digit()
        && time[2] == b':'
        && time[3].is_ascii_digit()
        && time[4].is_ascii_digit()
        && time[5] == b':'
        && time[6].is_ascii_digit()
        && time[7].is_ascii_digit();
    if !valid_time {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid time {:?}  -  time must be 2DIGIT \":\" 2DIGIT \":\" 2DIGIT \
             (RFC 3501 Section 9)",
                std::str::from_utf8(time).unwrap_or("????????")
            )),
        ));
    }

    // Semantic range checks for time components (RFC 3501 Section 9).
    // The ABNF `time = 2DIGIT ":" 2DIGIT ":" 2DIGIT` is purely structural,
    // but since we already enforce semantic day ranges (1-31) and month names,
    // we apply the same rigour here.  Valid ranges follow RFC 5322 Section 3.3:
    //   hour   = 00-23
    //   minute = 00-59
    //   second = 00-60  (60 allows for leap seconds)
    let hour = (time[0] - b'0') * 10 + (time[1] - b'0');
    let minute = (time[3] - b'0') * 10 + (time[4] - b'0');
    let second = (time[6] - b'0') * 10 + (time[7] - b'0');

    if hour > 23 {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid hour {hour:02}  -  must be 00-23 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3)"
            )),
        ));
    }
    if minute > 59 {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid minute {minute:02}  -  must be 00-59 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3)"
            )),
        ));
    }
    if second > 60 {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid second {second:02}  -  must be 00-60 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3; 60 permits leap seconds)"
            )),
        ));
    }

    // SP between time and zone (RFC 3501 Section 9).
    if b[20] != b' ' {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "expected SP at position 20, got {:?} (RFC 3501 Section 9)",
                b[20] as char
            )),
        ));
    }

    // zone = ("+" / "-") 4DIGIT (RFC 3501 Section 9).
    let zone = &b[21..26];
    let valid_zone =
        (zone[0] == b'+' || zone[0] == b'-') && zone[1..].iter().all(u8::is_ascii_digit);
    if !valid_zone {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid zone {:?}  -  zone must be (\"+\" / \"-\") 4DIGIT (RFC 3501 Section 9)",
                std::str::from_utf8(zone).unwrap_or("?????")
            )),
        ));
    }

    // Semantic range checks for zone components (RFC 3501 Section 9).
    // The zone is ±HHMM.  RFC 5322 Section 4.3 (obs-zone) does not
    // constrain the digits, but real UTC offsets range from -1200 to +1400,
    // and minutes are always 00-59.  We enforce those limits for consistency
    // with the semantic day/time checks above.
    let zone_hh = (zone[1] - b'0') * 10 + (zone[2] - b'0');
    let zone_mm = (zone[3] - b'0') * 10 + (zone[4] - b'0');

    if zone_hh > 14 {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid zone hour {zone_hh:02}  -  must be 00-14 \
             (maximum real UTC offset is ±14:00, RFC 3501 Section 9)"
            )),
        ));
    }
    if zone_mm > 59 {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidAppendDate(format!(
                "invalid zone minute {zone_mm:02}  -  must be 00-59 \
             (RFC 3501 Section 9)"
            )),
        ));
    }
    // RFC 3501 Section 9: `zone = ("+" / "-") 4DIGIT`  -  the grammar
    // imposes no semantic constraint beyond valid digits.  We cap the
    // hour at 14 and minute at 59 above for basic sanity, but do NOT
    // reject non-zero minutes when the hour is 14.  Real-world offsets
    // with sub-hour components exist (e.g., +12:45 Chatham Islands
    // standard, +13:45 Chatham Islands DST), and future timezone
    // changes could introduce new offsets at any hour.

    Ok(())
}
