use super::*;

/// An RFC 2047 encoded word cannot span whitespace. Keeping delimiter scans
/// inside this candidate window avoids rescanning the rest of a hostile header
/// for every invalid `=?` prefix.
#[test]
fn rfc2047_candidate_window_stops_at_whitespace() {
    assert_eq!(
        super::encoded_words::encoded_word_window("UTF-8?Q?x?= rest"),
        "UTF-8?Q?x?="
    );
    assert_eq!(
        super::encoded_words::encoded_word_window("UTF-8?Q?x?=é"),
        "UTF-8?Q?x?="
    );
}

/// Whitespace alone is not enough of a bound: a hostile header can be one
/// unbroken printable run, so the candidate window is also capped by a
/// constant. Without the cap each of the N failing `=?` candidates rescans
/// O(N) bytes and RFC 2047 decoding is quadratic in the header length.
#[test]
fn rfc2047_candidate_window_is_capped_for_unbroken_printable_runs() {
    let hostile = "=?".repeat(50_000);
    let window = super::encoded_words::encoded_word_window(&hostile);
    assert!(
        window.len() <= 998,
        "candidate scan must be bounded by a constant, scanned {} bytes",
        window.len()
    );
    // The bound must not truncate an overlong-but-real encoded word, which
    // this decoder still accepts (regression IMAP-001).
    let long_word = format!("UTF-8?B?{}?=", "QQ==".repeat(20));
    assert_eq!(
        super::encoded_words::encoded_word_window(&long_word),
        long_word
    );
}

#[test]
fn rfc2047_repeated_shift_prefixes_are_passed_through_verbatim() {
    let input = "=? ".repeat(2000);
    assert_eq!(
        decode_rfc2047(input.as_bytes()),
        input,
        "an unparseable `=?` run must be emitted verbatim (RFC 2047 Section 6.3)"
    );
}

/// RFC 2047 Section 6.3: adjacent `=?` prefixes that never close are text.
/// Companion to the test above with no whitespace separators, which takes a
/// different branch (`candidate_has_valid_prefix` is false from the second
/// candidate onward, so `parse_encoded_word` is never re-entered).
#[test]
fn rfc2047_adjacent_shift_prefixes_are_passed_through_verbatim() {
    let input = "=?x".repeat(2000);
    assert_eq!(decode_rfc2047(input.as_bytes()), input);
}
