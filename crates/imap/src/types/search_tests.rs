#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

// ---------------------------------------------------------------------------
// Basic flag criteria (RFC 3501 Section 6.4.4)
// ---------------------------------------------------------------------------

#[test]
fn all() {
    let c = SearchCriteria::new().all();
    assert_eq!(c.as_str(), "ALL");
}

#[test]
fn answered() {
    assert_eq!(SearchCriteria::new().answered().as_str(), "ANSWERED");
}

#[test]
fn deleted() {
    assert_eq!(SearchCriteria::new().deleted().as_str(), "DELETED");
}

#[test]
fn draft() {
    assert_eq!(SearchCriteria::new().draft().as_str(), "DRAFT");
}

#[test]
fn flagged() {
    assert_eq!(SearchCriteria::new().flagged().as_str(), "FLAGGED");
}

#[test]
fn seen() {
    assert_eq!(SearchCriteria::new().seen().as_str(), "SEEN");
}

#[test]
fn recent() {
    assert_eq!(SearchCriteria::new().recent().as_str(), "RECENT");
}

#[test]
fn new_messages() {
    assert_eq!(SearchCriteria::new().new_messages().as_str(), "NEW");
}

#[test]
fn old() {
    assert_eq!(SearchCriteria::new().old().as_str(), "OLD");
}

#[test]
fn unanswered() {
    assert_eq!(SearchCriteria::new().unanswered().as_str(), "UNANSWERED");
}

#[test]
fn undeleted() {
    assert_eq!(SearchCriteria::new().undeleted().as_str(), "UNDELETED");
}

#[test]
fn undraft() {
    assert_eq!(SearchCriteria::new().undraft().as_str(), "UNDRAFT");
}

#[test]
fn unflagged() {
    assert_eq!(SearchCriteria::new().unflagged().as_str(), "UNFLAGGED");
}

#[test]
fn unseen() {
    assert_eq!(SearchCriteria::new().unseen().as_str(), "UNSEEN");
}

#[test]
fn keyword() {
    assert_eq!(
        SearchCriteria::new()
            .keyword("$Important")
            .unwrap()
            .as_str(),
        "KEYWORD $Important"
    );
}

#[test]
fn unkeyword() {
    assert_eq!(
        SearchCriteria::new().unkeyword("$Junk").unwrap().as_str(),
        "UNKEYWORD $Junk"
    );
}

// ---------------------------------------------------------------------------
// Header / body criteria (RFC 3501 Section 6.4.4)
// ---------------------------------------------------------------------------

#[test]
fn bcc() {
    assert_eq!(
        SearchCriteria::new()
            .bcc("alice@example.com")
            .unwrap()
            .as_str(),
        "BCC \"alice@example.com\""
    );
}

#[test]
fn cc() {
    assert_eq!(
        SearchCriteria::new()
            .cc("bob@example.com")
            .unwrap()
            .as_str(),
        "CC \"bob@example.com\""
    );
}

#[test]
fn from() {
    assert_eq!(
        SearchCriteria::new()
            .from("alice@example.com")
            .unwrap()
            .as_str(),
        "FROM \"alice@example.com\""
    );
}

#[test]
fn to() {
    assert_eq!(
        SearchCriteria::new()
            .to("bob@example.com")
            .unwrap()
            .as_str(),
        "TO \"bob@example.com\""
    );
}

#[test]
fn subject() {
    assert_eq!(
        SearchCriteria::new()
            .subject("hello world")
            .unwrap()
            .as_str(),
        "SUBJECT \"hello world\""
    );
}

#[test]
fn header() {
    assert_eq!(
        SearchCriteria::new()
            .header("X-Mailer", "thunderbird")
            .unwrap()
            .as_str(),
        "HEADER X-Mailer \"thunderbird\""
    );
}

#[test]
fn header_rejects_an_injectable_field_name() {
    assert!(
        SearchCriteria::new()
            .header("X-Trace OR ALL", "value")
            .is_err()
    );
    assert!(
        SearchCriteria::new()
            .header("X-Trace\r\nALL", "value")
            .is_err()
    );
}

#[test]
fn body() {
    assert_eq!(
        SearchCriteria::new()
            .body("meeting notes")
            .unwrap()
            .as_str(),
        "BODY \"meeting notes\""
    );
}

#[test]
fn text() {
    assert_eq!(
        SearchCriteria::new().text("urgent").unwrap().as_str(),
        "TEXT \"urgent\""
    );
}

// ---------------------------------------------------------------------------
// Date criteria (RFC 3501 Section 6.4.4)
// ---------------------------------------------------------------------------

#[test]
fn before() {
    assert_eq!(
        SearchCriteria::new()
            .before("13-Feb-2025")
            .unwrap()
            .as_str(),
        "BEFORE 13-Feb-2025"
    );
}

#[test]
fn on() {
    assert_eq!(
        SearchCriteria::new().on("13-Feb-2025").unwrap().as_str(),
        "ON 13-Feb-2025"
    );
}

#[test]
fn since() {
    assert_eq!(
        SearchCriteria::new().since("1-Jan-2024").unwrap().as_str(),
        "SINCE 1-Jan-2024"
    );
}

#[test]
fn sent_before() {
    assert_eq!(
        SearchCriteria::new()
            .sent_before("1-Mar-2025")
            .unwrap()
            .as_str(),
        "SENTBEFORE 1-Mar-2025"
    );
}

#[test]
fn sent_on() {
    assert_eq!(
        SearchCriteria::new()
            .sent_on("1-Mar-2025")
            .unwrap()
            .as_str(),
        "SENTON 1-Mar-2025"
    );
}

#[test]
fn sent_since() {
    assert_eq!(
        SearchCriteria::new()
            .sent_since("1-Mar-2025")
            .unwrap()
            .as_str(),
        "SENTSINCE 1-Mar-2025"
    );
}

// ---------------------------------------------------------------------------
// Size criteria (RFC 3501 Section 6.4.4)
// ---------------------------------------------------------------------------

#[test]
fn larger() {
    assert_eq!(SearchCriteria::new().larger(10240).as_str(), "LARGER 10240");
}

#[test]
fn smaller() {
    assert_eq!(SearchCriteria::new().smaller(1024).as_str(), "SMALLER 1024");
}

// ---------------------------------------------------------------------------
// Sequence / UID criteria (RFC 3501 Section 6.4.4)
// ---------------------------------------------------------------------------

#[test]
fn uid_set() {
    assert_eq!(
        SearchCriteria::new().uid("1:100").unwrap().as_str(),
        "UID 1:100"
    );
}

#[test]
fn uid_star() {
    assert_eq!(
        SearchCriteria::new().uid("1:*").unwrap().as_str(),
        "UID 1:*"
    );
}

#[test]
fn sequence_set() {
    assert_eq!(
        SearchCriteria::new().sequence("1:50").unwrap().as_str(),
        "1:50"
    );
}

#[test]
fn sequence_comma_separated() {
    assert_eq!(
        SearchCriteria::new().sequence("1,2,5:10").unwrap().as_str(),
        "1,2,5:10"
    );
}

// ---------------------------------------------------------------------------
// Combinators (RFC 3501 Section 6.4.4)
// ---------------------------------------------------------------------------

#[test]
fn not_single_key() {
    let inner = SearchCriteria::new().seen();
    let c = SearchCriteria::new().not(&inner);
    // Single key: no parens needed.
    assert_eq!(c.as_str(), "NOT SEEN");
}

#[test]
fn not_compound() {
    let inner = SearchCriteria::new()
        .seen()
        .from("alice@example.com")
        .unwrap();
    let c = SearchCriteria::new().not(&inner);
    // Compound inner criteria gets parenthesized.
    assert_eq!(c.as_str(), "NOT (SEEN FROM \"alice@example.com\")");
}

#[test]
fn or_simple() {
    let a = SearchCriteria::new().seen();
    let b = SearchCriteria::new().flagged();
    let c = SearchCriteria::new().or(&a, &b);
    assert_eq!(c.as_str(), "OR SEEN FLAGGED");
}

#[test]
fn or_compound_operands() {
    let a = SearchCriteria::new().unseen().since("1-Jan-2025").unwrap();
    let b = SearchCriteria::new()
        .flagged()
        .from("bob@example.com")
        .unwrap();
    let c = SearchCriteria::new().or(&a, &b);
    assert_eq!(
        c.as_str(),
        "OR (UNSEEN SINCE 1-Jan-2025) (FLAGGED FROM \"bob@example.com\")"
    );
}

// ---------------------------------------------------------------------------
// Chaining (AND semantics) (RFC 3501 Section 6.4.4)
// ---------------------------------------------------------------------------

#[test]
fn chain_multiple_criteria() {
    let c = SearchCriteria::new()
        .unseen()
        .since("13-Feb-2025")
        .unwrap()
        .from("alice@example.com")
        .unwrap();
    assert_eq!(
        c.as_str(),
        "UNSEEN SINCE 13-Feb-2025 FROM \"alice@example.com\""
    );
}

#[test]
fn chain_flags_and_size() {
    let c = SearchCriteria::new()
        .undeleted()
        .unseen()
        .larger(5000)
        .smaller(1_000_000);
    assert_eq!(c.as_str(), "UNDELETED UNSEEN LARGER 5000 SMALLER 1000000");
}

#[test]
fn chain_with_or_and_not() {
    let deleted = SearchCriteria::new().deleted();
    let draft = SearchCriteria::new().draft();
    let c = SearchCriteria::new()
        .unseen()
        .or(&deleted, &draft)
        .since("1-Jan-2025")
        .unwrap();
    assert_eq!(c.as_str(), "UNSEEN OR DELETED DRAFT SINCE 1-Jan-2025");
}

// ---------------------------------------------------------------------------
// String quoting (RFC 3501 Section 9)
// ---------------------------------------------------------------------------

#[test]
fn quote_backslash_in_string() {
    let c = SearchCriteria::new().from("a\\b").unwrap();
    assert_eq!(c.as_str(), "FROM \"a\\\\b\"");
}

#[test]
fn quote_double_quote_in_string() {
    let c = SearchCriteria::new().subject("say \"hello\"").unwrap();
    assert_eq!(c.as_str(), "SUBJECT \"say \\\"hello\\\"\"");
}

#[test]
fn empty_string_value() {
    // An empty astring value is quoted as "" per RFC 3501 Section 9.
    let c = SearchCriteria::new().from("").unwrap();
    assert_eq!(c.as_str(), "FROM \"\"");
}

// ---------------------------------------------------------------------------
// Display and AsRef<str> traits
// ---------------------------------------------------------------------------

#[test]
fn display_trait() {
    let c = SearchCriteria::new().unseen().flagged();
    assert_eq!(format!("{c}"), "UNSEEN FLAGGED");
}

#[test]
fn as_ref_str() {
    let c = SearchCriteria::new().all();
    let s: &str = c.as_ref();
    assert_eq!(s, "ALL");
}

#[test]
fn as_str_method() {
    let c = SearchCriteria::new().unseen();
    assert_eq!(c.as_str(), "UNSEEN");
}

// ---------------------------------------------------------------------------
// Default trait
// ---------------------------------------------------------------------------

#[test]
fn default_is_empty() {
    let c = SearchCriteria::default();
    assert_eq!(c.as_str(), "");
}

// ---------------------------------------------------------------------------
// Clone and equality
// ---------------------------------------------------------------------------

#[test]
fn clone_and_eq() {
    let c1 = SearchCriteria::new()
        .unseen()
        .from("alice@example.com")
        .unwrap();
    let c2 = c1.clone();
    assert_eq!(c1, c2);
}

#[test]
fn ne_different_criteria() {
    let c1 = SearchCriteria::new().unseen();
    let c2 = SearchCriteria::new().seen();
    assert_ne!(c1, c2);
}

// ---------------------------------------------------------------------------
// Complex real-world examples
// ---------------------------------------------------------------------------

#[test]
fn real_world_unread_from_sender_this_month() {
    let c = SearchCriteria::new()
        .unseen()
        .since("1-Mar-2026")
        .unwrap()
        .from("notifications@github.com")
        .unwrap();
    assert_eq!(
        c.as_str(),
        "UNSEEN SINCE 1-Mar-2026 FROM \"notifications@github.com\""
    );
}

#[test]
fn real_world_large_unseen_or_flagged() {
    let unseen = SearchCriteria::new().unseen();
    let flagged = SearchCriteria::new().flagged();
    let c = SearchCriteria::new().larger(100_000).or(&unseen, &flagged);
    assert_eq!(c.as_str(), "LARGER 100000 OR UNSEEN FLAGGED");
}

#[test]
fn real_world_not_deleted_not_draft() {
    let deleted = SearchCriteria::new().deleted();
    let draft = SearchCriteria::new().draft();
    let c = SearchCriteria::new().not(&deleted).not(&draft);
    assert_eq!(c.as_str(), "NOT DELETED NOT DRAFT");
}

#[test]
fn real_world_header_search() {
    let c = SearchCriteria::new()
        .header("X-Priority", "1")
        .unwrap()
        .unseen()
        .since("1-Jan-2026")
        .unwrap();
    assert_eq!(
        c.as_str(),
        "HEADER X-Priority \"1\" UNSEEN SINCE 1-Jan-2026"
    );
}

// ---------------------------------------------------------------------------
// CONDSTORE MODSEQ criterion (RFC 7162 Section 3.1.5)
// ---------------------------------------------------------------------------

#[test]
fn mod_seq_simple() {
    // RFC 7162 Section 3.1.5: simple MODSEQ form with a mod-sequence value.
    assert_eq!(
        SearchCriteria::new().mod_seq(12345).unwrap().as_str(),
        "MODSEQ 12345"
    );
}

#[test]
fn mod_seq_zero() {
    // RFC 7162 Section 3.1.5: zero is a valid mod-sequence-valzer.
    assert_eq!(
        SearchCriteria::new().mod_seq(0).unwrap().as_str(),
        "MODSEQ 0"
    );
}

#[test]
fn mod_seq_combined_with_unseen() {
    // MODSEQ combined with other criteria via AND semantics
    // (RFC 3501 Section 6.4.4).
    let c = SearchCriteria::new().unseen().mod_seq(100).unwrap();
    assert_eq!(c.as_str(), "UNSEEN MODSEQ 100");
}

#[test]
fn mod_seq_large_value() {
    // RFC 7162 Section 3.1.5: mod-sequence-valzer can be up to 63 bits.
    let c = SearchCriteria::new().mod_seq(9_999_999_999_999).unwrap();
    assert_eq!(c.as_str(), "MODSEQ 9999999999999");
}

#[test]
fn is_compound_modseq_single() {
    // MODSEQ <value> is a single search-key (keyword + 1 arg = 2 tokens).
    assert!(!is_compound("MODSEQ 12345"));
}

// ---------------------------------------------------------------------------
// is_compound helper tests
// ---------------------------------------------------------------------------

#[test]
fn is_compound_single_keyword() {
    assert!(!is_compound("UNSEEN"));
}

#[test]
fn is_compound_keyword_with_arg() {
    assert!(!is_compound("FROM \"alice\""));
}

#[test]
fn is_compound_multiple_keywords() {
    assert!(is_compound("UNSEEN FLAGGED"));
}

#[test]
fn is_compound_header_two_args() {
    assert!(!is_compound("HEADER X-Mailer \"thunderbird\""));
}

#[test]
fn is_compound_or_two_args() {
    assert!(!is_compound("OR SEEN FLAGGED"));
}

#[test]
fn is_compound_not_with_arg() {
    assert!(!is_compound("NOT SEEN"));
}

#[test]
fn is_compound_not_with_parenthesized_group() {
    // NOT (SEEN FLAGGED) is one search-key (NOT + 1 parenthesized group = 2 tokens).
    assert!(!is_compound("NOT (SEEN FLAGGED)"));
}

#[test]
fn is_compound_or_with_parenthesized_groups() {
    // OR (UNSEEN) (FLAGGED) is one search-key (OR + 2 parenthesized groups = 3 tokens).
    assert!(!is_compound("OR (UNSEEN) (FLAGGED)"));
}

#[test]
fn is_compound_bare_sequence_set() {
    // A bare sequence set is a single search-key.
    assert!(!is_compound("1:100"));
}

// ---------------------------------------------------------------------------
// count_top_level_tokens helper tests
// ---------------------------------------------------------------------------

#[test]
fn count_tokens_empty() {
    assert_eq!(count_top_level_tokens(""), 0);
}

#[test]
fn count_tokens_single_word() {
    assert_eq!(count_top_level_tokens("UNSEEN"), 1);
}

#[test]
fn count_tokens_two_words() {
    assert_eq!(count_top_level_tokens("UNSEEN FLAGGED"), 2);
}

#[test]
fn count_tokens_quoted_string_with_spaces() {
    // The quoted string counts as one token despite containing spaces.
    assert_eq!(count_top_level_tokens("FROM \"alice bob\""), 2);
}

#[test]
fn count_tokens_parenthesized_group() {
    // A parenthesized group counts as one token.
    assert_eq!(count_top_level_tokens("NOT (SEEN FLAGGED)"), 2);
}

#[test]
fn count_tokens_escaped_quote_in_string() {
    // The escaped quote inside the string does not terminate the quoted region.
    assert_eq!(count_top_level_tokens("SUBJECT \"say \\\"hi\\\"\""), 2);
}

// ---------------------------------------------------------------------------
// SEARCH strings reject NUL and retain controls for literal encoding.
// ---------------------------------------------------------------------------

#[test]
fn quote_imap_string_rejects_nul() {
    // NUL (\0) is not a valid CHAR per RFC 3501 Section 9:
    // CHAR = <any 7-bit US-ASCII character except NUL, 0x01 - 0x7f>
    // A quoted string cannot contain NUL.
    let result = SearchCriteria::new().from("hello\0world");
    assert!(
        result.is_err(),
        "NUL byte must be rejected in quoted strings"
    );
}

#[test]
fn search_string_retains_cr_for_literal_encoding() {
    let criteria = SearchCriteria::new().from("hello\rworld").unwrap();
    assert_eq!(criteria.as_str(), "FROM {11}\r\nhello\rworld");
}

#[test]
fn search_string_retains_lf_for_literal_encoding() {
    let criteria = SearchCriteria::new().from("hello\nworld").unwrap();
    assert_eq!(criteria.as_str(), "FROM {11}\r\nhello\nworld");
}

#[test]
fn quote_imap_string_rejects_nul_in_header_value() {
    // The header() method also validates its search string for the value argument.
    let result = SearchCriteria::new().header("X-Test", "val\0ue");
    assert!(result.is_err(), "NUL in header value must be rejected");
}

// ---------------------------------------------------------------------------
// IMAP-002: keyword/unkeyword must validate atom chars (RFC 3501 Section 9)
// ---------------------------------------------------------------------------

#[test]
fn keyword_rejects_space() {
    // RFC 3501 Section 9: flag-keyword = atom, atom = 1*ATOM-CHAR.
    // Space is an atom-special and must be rejected.
    let result = SearchCriteria::new().keyword("my flag");
    assert!(result.is_err(), "space in keyword must be rejected");
}

#[test]
fn keyword_rejects_close_paren() {
    // ')' is an atom-special per RFC 3501 Section 9.
    let result = SearchCriteria::new().keyword("$foo)");
    assert!(result.is_err(), "closing paren in keyword must be rejected");
}

#[test]
fn keyword_rejects_empty() {
    // RFC 3501 Section 9: atom = 1*ATOM-CHAR  -  must be at least one character.
    let result = SearchCriteria::new().keyword("");
    assert!(result.is_err(), "empty keyword must be rejected");
}

#[test]
fn unkeyword_rejects_space() {
    let result = SearchCriteria::new().unkeyword("my flag");
    assert!(result.is_err(), "space in unkeyword must be rejected");
}

#[test]
fn unkeyword_rejects_empty() {
    let result = SearchCriteria::new().unkeyword("");
    assert!(result.is_err(), "empty unkeyword must be rejected");
}

// ---------------------------------------------------------------------------
// IMAP-003: uid/sequence must validate sequence sets (RFC 3501 Section 9)
// ---------------------------------------------------------------------------

#[test]
fn uid_rejects_non_set() {
    // "not a set" is not a valid sequence-set per RFC 3501 Section 9.
    let result = SearchCriteria::new().uid("not a set");
    assert!(
        result.is_err(),
        "invalid sequence set must be rejected by uid()"
    );
}

#[test]
fn uid_rejects_empty() {
    let result = SearchCriteria::new().uid("");
    assert!(
        result.is_err(),
        "empty sequence set must be rejected by uid()"
    );
}

#[test]
fn sequence_rejects_spaces() {
    // "1 2 3" contains spaces, which are not valid in a sequence-set.
    let result = SearchCriteria::new().sequence("1 2 3");
    assert!(
        result.is_err(),
        "spaces in sequence set must be rejected by sequence()"
    );
}

#[test]
fn sequence_rejects_empty() {
    let result = SearchCriteria::new().sequence("");
    assert!(
        result.is_err(),
        "empty sequence set must be rejected by sequence()"
    );
}

// ---------------------------------------------------------------------------
// IMAP-004: Date methods must validate IMAP date format (RFC 3501 Section 9)
// ---------------------------------------------------------------------------

#[test]
fn before_rejects_non_date() {
    // "not-a-date" doesn't match DD-Mon-YYYY.
    let result = SearchCriteria::new().before("not-a-date");
    assert!(
        result.is_err(),
        "non-date string must be rejected by before()"
    );
}

#[test]
fn before_rejects_empty() {
    let result = SearchCriteria::new().before("");
    assert!(result.is_err(), "empty date must be rejected by before()");
}

#[test]
fn before_rejects_invalid_month() {
    // "31-Foo-2025" has an invalid month abbreviation.
    let result = SearchCriteria::new().before("31-Foo-2025");
    assert!(
        result.is_err(),
        "invalid month abbreviation must be rejected"
    );
}

#[test]
fn since_rejects_wrong_format() {
    // "2025-01-13" is ISO format, not IMAP date format.
    let result = SearchCriteria::new().since("2025-01-13");
    assert!(result.is_err(), "ISO date format must be rejected");
}

#[test]
fn sent_on_rejects_garbage() {
    let result = SearchCriteria::new().sent_on("garbage");
    assert!(
        result.is_err(),
        "garbage date must be rejected by sent_on()"
    );
}

// ---------------------------------------------------------------------------
// validate_imap_date helper tests (RFC 3501 Section 9)
// ---------------------------------------------------------------------------

#[test]
fn validate_imap_date_valid_single_digit_day() {
    // Single-digit day is valid per RFC 3501 Section 9: date-day = 1*2DIGIT.
    assert!(validate_imap_date("1-Jan-2025").is_ok());
}

#[test]
fn validate_imap_date_valid_double_digit_day() {
    assert!(validate_imap_date("13-Feb-2025").is_ok());
}

#[test]
fn validate_imap_date_rejects_three_digit_day() {
    assert!(validate_imap_date("123-Jan-2025").is_err());
}

#[test]
fn validate_imap_date_rejects_two_digit_year() {
    assert!(validate_imap_date("1-Jan-25").is_err());
}

#[test]
fn validate_imap_date_all_months_valid() {
    for month in [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ] {
        let date = format!("1-{month}-2025");
        assert!(
            validate_imap_date(&date).is_ok(),
            "{month} should be accepted"
        );
    }
}

#[test]
fn search_dates_share_append_calendar_rules() {
    for date in ["13-feb-2025", "29-FEB-2000", "1-jAn-2025"] {
        assert!(validate_imap_date(date).is_ok(), "{date}");
    }
    for date in ["30-Feb-2025", "29-Feb-1900", "31-Apr-2025", "0-Jan-2025"] {
        assert!(validate_imap_date(date).is_err(), "{date}");
    }
}

#[test]
fn modseq_refuses_values_above_protocol_ceiling() {
    assert!(
        SearchCriteria::new()
            .mod_seq(crate::types::ModSeq::MAX)
            .is_ok()
    );
    assert!(
        SearchCriteria::new()
            .mod_seq(crate::types::ModSeq::MAX + 1)
            .is_err()
    );
    assert!(SearchCriteria::new().mod_seq(u64::MAX).is_err());
}
