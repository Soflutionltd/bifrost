use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Parser never panics on arbitrary input (RFC 3501  -  Postel's law).
    #[test]
    fn parse_response_never_panics(data in prop::collection::vec(any::<u8>(), 0..500)) {
        let _ = parse_response_utf8(&data, crate::types::wire::WireMode::ModifiedUtf7);
        let _ = parse_response_utf8(&data, crate::types::wire::WireMode::Utf8);
    }

    /// Parser never panics on greeting input.
    #[test]
    fn parse_greeting_never_panics(data in prop::collection::vec(any::<u8>(), 0..500)) {
        let _ = parse_greeting(&data);
    }

    /// Successful parse always consumes at least one byte (no infinite loops).
    ///
    /// If parse_response_utf8 returns Ok, the remaining input must be
    /// strictly shorter than the original input. This prevents infinite
    /// loops where the parser succeeds but consumes nothing.
    #[test]
    fn parse_response_consumes_bytes(data in prop::collection::vec(any::<u8>(), 1..500)) {
        if let Ok((remaining, _)) = parse_response_utf8(&data, crate::types::wire::WireMode::ModifiedUtf7) {
            prop_assert!(
                remaining.len() < data.len(),
                "parser must consume at least one byte on success"
            );
        }
    }

    /// UIDs in SEARCH responses are always >= 1 (RFC 3501 Section 9).
    ///
    /// UID 0 is not a valid message UID. If the parser returns a SEARCH
    /// response, all UIDs must be >= 1.
    #[test]
    fn search_uids_nonzero(data in prop::collection::vec(any::<u8>(), 0..500)) {
        if let Ok((_, Response::Untagged(boxed))) = parse_response_utf8(&data, crate::types::wire::WireMode::ModifiedUtf7).as_ref()
            && let UntaggedResponse::Search { numbers: uids, .. } = boxed.as_ref()
        {
            for uid in uids {
                prop_assert!(*uid >= 1, "UID must be >= 1 (RFC 3501 Section 9), got {}", uid);
            }
        }
    }

    /// Parser must not consume bytes from a subsequent response.
    ///
    /// Regression: unbalanced parentheses in BODYSTRUCTURE could cause
    /// the parser to silently consume bytes from the next response in
    /// the buffer, corrupting downstream parsing.
    ///
    /// Strategy: only check when `data` alone parses successfully,
    /// then verify appending a sentinel does not cause the parser to
    /// consume into the sentinel bytes.
    ///
    /// # References
    /// - RFC 3501 Section 7.4.2 (BODYSTRUCTURE)
    #[test]
    fn parse_response_does_not_overconsume(data in prop::collection::vec(any::<u8>(), 0..200)) {
        // Only test when the data alone produces a successful parse.
        if let Ok((rest_alone, _)) = parse_response_utf8(&data, crate::types::wire::WireMode::ModifiedUtf7) {
            let sentinel = b"A001 OK done\r\n";
            let mut combined = data.clone();
            combined.extend_from_slice(sentinel);
            if let Ok((remaining, _)) = parse_response_utf8(&combined, crate::types::wire::WireMode::ModifiedUtf7) {
                // The parser should consume no more from `combined` than
                // it consumed from `data` alone  -  so remaining must still
                // include the full sentinel.
                prop_assert!(
                    remaining.len() >= sentinel.len(),
                    "parser consumed into sentinel: remaining={} < sentinel={}, \
                     data.len()={}, rest_alone.len()={}",
                    remaining.len(), sentinel.len(),
                    data.len(), rest_alone.len()
                );
            }
        }
    }
}

// ====================================================================
// T1: Whitespace Tolerance
// ====================================================================

/// Known-valid IMAP *untagged* response wire strings (representative
/// samples) covering each major response type.
///
/// Only untagged responses are included because the historical
/// whitespace-tolerance bugs are all in data-bearing positions within
/// untagged responses (flag lists, BODYSTRUCTURE fields, etc.), not
/// in the `tag SP status` framing of tagged responses.
///
/// # References
/// - RFC 3501 Section 7 (server responses)
/// - RFC 9051 Section 7 (`IMAP4rev2` responses)
fn valid_imap_responses() -> Vec<&'static [u8]> {
    vec![
            b"* 42 EXISTS\r\n",
            b"* 5 RECENT\r\n",
            b"* 3 EXPUNGE\r\n",
            b"* FLAGS (\\Seen \\Answered \\Flagged \\Deleted \\Draft)\r\n",
            b"* CAPABILITY IMAP4rev1 STARTTLS AUTH=PLAIN IDLE\r\n",
            b"* LIST (\\HasNoChildren) \"/\" \"INBOX\"\r\n",
            b"* SEARCH 1 2 3 10 20\r\n",
            b"* STATUS INBOX (MESSAGES 10 UNSEEN 3)\r\n",
            b"* OK [READ-WRITE] SELECT completed\r\n",
            b"* NO [AUTHENTICATIONFAILED] Login failed\r\n",
            b"* 1 FETCH (UID 42 FLAGS (\\Seen) RFC822.SIZE 1024)\r\n",
            b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))\r\n",
            b"* 1 FETCH (ENVELOPE (\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Subject\" ((\"Name\" NIL \"user\" \"host.com\")) NIL NIL ((\"To\" NIL \"rcpt\" \"host.com\")) NIL NIL NIL \"<msgid@host.com>\"))\r\n",
            b"* ENABLED CONDSTORE\r\n",
            b"* VANISHED 1:3,5\r\n",
            b"* SORT 5 3 1 10\r\n",
        ]
}

/// Injects extra whitespace at random SP positions in the input,
/// skipping the `"* "` protocol prefix (byte offset 0..2).
///
/// Only adds spaces where there is already a space  -  avoids breaking
/// atoms, quoted strings, or CRLF sequences. The `"* "` prefix is
/// defined as `"*" SP` in RFC 3501 Section 9 and is always strict;
/// the historical whitespace bugs are in data-bearing positions after
/// this prefix.
///
/// # References
/// - RFC 3501 Section 9 (formal syntax): SP = %x20
fn inject_extra_whitespace(input: &[u8], insertions: &[(usize, u8)]) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() + insertions.len());
    // Skip the "* " prefix (first 2 bytes) and trailing CRLF from
    // injection candidates  -  only target data-bearing SP positions.
    let data_start = 2;
    let data_end = input.len().saturating_sub(2); // exclude \r\n
    let sp_positions: Vec<usize> = input
        .iter()
        .enumerate()
        .filter(|&(i, &b)| b == b' ' && i >= data_start && i < data_end)
        .map(|(i, _)| i)
        .collect();

    if sp_positions.is_empty() {
        return input.to_vec();
    }

    let mut extra_at: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for &(idx, _) in insertions {
        extra_at.insert(sp_positions[idx % sp_positions.len()]);
    }

    for (i, &b) in input.iter().enumerate() {
        result.push(b);
        if extra_at.contains(&i) {
            // Insert 1 extra space after this space
            result.push(b' ');
        }
    }
    result
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Any valid IMAP response with extra whitespace injected at SP
    /// positions must still parse successfully.
    ///
    /// This catches the entire class of "strict sp combinator" bugs
    /// (RFC 3501 Section 9  -  SP is formally single-space but servers
    /// may emit multiple spaces, and per Postel's law we must tolerate
    /// this):
    /// - 54c67e0: multi-space before multipart subtype
    /// - 2abc6e1: multi-space in parameter key-value pairs
    /// - 95516cb: trailing whitespace in EXISTS/RECENT/EXPUNGE
    /// - c5b0773: extra whitespace in BADCHARSET/CHILDINFO lists
    /// - de4371d: multiple spaces in flag/capability/LIST attribute lists
    /// - b69645e: missing space after MODSEQ in ESEARCH
    #[test]
    fn extra_whitespace_still_parses(
        response_idx in 0..16usize,
        insertions in prop::collection::vec((0..50usize, any::<u8>()), 1..5),
    ) {
        let responses = valid_imap_responses();
        let response = responses[response_idx % responses.len()];
        let mutated = inject_extra_whitespace(response, &insertions);

        // Must parse successfully  -  not necessarily the same result,
        // just must not reject valid input with extra whitespace.
        let result = parse_response(&mutated);
        prop_assert!(
            result.is_ok(),
            "valid response with extra whitespace must still parse.\n\
             Original: {:?}\nMutated:  {:?}",
            String::from_utf8_lossy(response),
            String::from_utf8_lossy(&mutated)
        );
    }

    // ================================================================
    // T2: Quoting Tolerance
    // ================================================================

    /// BODYSTRUCTURE media types must be accepted both quoted and
    /// unquoted per Postel's law (RFC 3501 Section 7.4.2).
    ///
    /// Some servers send media type/subtype as atoms (`TEXT PLAIN`),
    /// others as quoted strings (`"TEXT" "PLAIN"`). Both forms are
    /// valid and must parse.
    ///
    /// Regression: 8df3c4e, 3c2596b, f113fd2
    #[test]
    fn bodystructure_quoted_and_unquoted_media_types(
        media_type in prop_oneof![
            Just("TEXT"), Just("IMAGE"), Just("APPLICATION"),
            Just("AUDIO"), Just("VIDEO"), Just("MESSAGE"),
        ],
        subtype in prop_oneof![
            Just("PLAIN"), Just("HTML"), Just("MIXED"),
            Just("ALTERNATIVE"), Just("OCTET-STREAM"), Just("PDF"),
            Just("PNG"),
        ],
    ) {
        // Quoted form (RFC 3501 Section 7.4.2  -  body-type-basic uses
        // `media-basic = ((DQUOTE ...`) for interop)
        let quoted = format!(
            "* 1 FETCH (BODYSTRUCTURE (\
             \"{media_type}\" \"{subtype}\" \
             (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))\r\n"
        );
        let r1 = parse_response(quoted.as_bytes());
        prop_assert!(r1.is_ok(), "quoted form must parse: {:?}", quoted);

        // Unquoted form
        let unquoted = format!(
            "* 1 FETCH (BODYSTRUCTURE (\
             {media_type} {subtype} \
             (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))\r\n"
        );
        let r2 = parse_response(unquoted.as_bytes());
        prop_assert!(r2.is_ok(), "unquoted form must parse: {:?}", unquoted);
    }

    // ================================================================
    // T6: Output Normalization
    // ================================================================

    /// BODYSTRUCTURE media subtype and encoding must always be
    /// lowercase in parsed output, regardless of input case.
    ///
    /// RFC 2045 Section 5.1: media types are case-insensitive, so the
    /// parser normalizes them to lowercase for consistent downstream
    /// comparison.
    ///
    /// Regression: 3515dde
    #[test]
    fn bodystructure_fields_always_lowercase(
        media in prop_oneof![
            Just("TEXT"), Just("text"), Just("Text"), Just("tExT"),
        ],
        subtype in prop_oneof![
            Just("PLAIN"), Just("plain"), Just("Plain"), Just("pLaIn"),
        ],
        encoding in prop_oneof![
            Just("7BIT"), Just("7bit"), Just("BASE64"),
            Just("base64"), Just("QUOTED-PRINTABLE"),
        ],
    ) {
        let input = format!(
            "* 1 FETCH (BODYSTRUCTURE (\
             \"{media}\" \"{subtype}\" NIL NIL NIL \
             \"{encoding}\" 100 5))\r\n"
        );
        if let Ok((_, Response::Untagged(u))) =
            parse_response(input.as_bytes()).as_ref()
            && let UntaggedResponse::Fetch(f) = u.as_ref()
            && let Some(BodyStructure::Text {
                media_subtype,
                encoding: enc,
                ..
            }) = &f.body_structure
        {
            prop_assert_eq!(
                media_subtype,
                &subtype.to_ascii_lowercase(),
                "subtype must be lowercase"
            );
            prop_assert_eq!(
                enc,
                &encoding.to_ascii_lowercase(),
                "encoding must be lowercase"
            );
        }
    }
}
