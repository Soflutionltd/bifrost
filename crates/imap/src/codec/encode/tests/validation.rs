use super::*;

/// `validate_no_crlf` accepts clean input.
#[test]
fn validate_no_crlf_accepts_clean_input() {
    let result = validate_no_crlf("ALL UNSEEN", "test");
    assert!(result.is_ok());
}

/// `validate_no_crlf` rejects CRLF (RFC 3501 Section 2.2).
#[test]
fn validate_no_crlf_rejects_crlf() {
    let result = validate_no_crlf("foo\r\nbar", "test");
    assert!(result.is_err());
}

/// `validate_no_crlf` rejects LF (RFC 3501 Section 2.2).
#[test]
fn validate_no_crlf_rejects_lf() {
    let result = validate_no_crlf("foo\nbar", "test");
    assert!(result.is_err());
}

/// `validate_no_crlf` rejects CR (RFC 3501 Section 2.2).
#[test]
fn validate_no_crlf_rejects_cr() {
    let result = validate_no_crlf("foo\rbar", "test");
    assert!(result.is_err());
}

/// A command whose prerequisite capability is not usable is refused as
/// `MissingCapability`, worded `<cmd> requires <cap>` for every command.
#[test]
fn an_encoder_capability_refusal_names_the_command_and_the_capability() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS".into(),
        changed_since: Some(crate::types::ModSeq::new(5).expect("valid protocol test value")),
        vanished: false,
    };
    let no_condstore = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::ModifiedUtf7,
        literal_mode: LiteralMode::Synchronizing,
        session: crate::types::profile::SessionCapabilities::new(
            vec![Capability::Imap4Rev1],
            Vec::new(),
        ),
    };
    let err = admit_and_encode("A001", &cmd, &no_condstore).unwrap_err();
    assert_eq!(
        err,
        crate::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(
            crate::error::CapabilityFailure::new(
                Capability::Condstore,
                crate::error::CapabilityFailureReason::NotAdvertised,
                "UID FETCH (CHANGEDSINCE) requires CONDSTORE",
            )
        ))
    );
}

/// A validation refusal arrives as `InvalidInput` with its message as the
/// encoder wrote it, not prefixed with another variant's display text.
#[test]
fn an_encoder_validation_refusal_arrives_as_invalid_input() {
    let cmd = Command::Search {
        criteria: String::new().into(),
    };
    let mut buf = BytesMut::new();
    let err = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect_err("empty SEARCH criteria are refused");
    let crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(message)) = err else {
        panic!("expected InvalidInput, got {err:?}");
    };
    assert!(
        message.contains("at least one search criterion"),
        "got {message:?}"
    );
}

/// Bare CR without LF is also rejected (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_bare_cr_rejected() {
    let cmd = Command::Search {
        criteria: "ALL\rA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "bare CR in SEARCH criteria must be rejected (RFC 3501 Section 2.2)"
    );
}

/// Bare LF without CR is also rejected (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_bare_lf_rejected() {
    let cmd = Command::Search {
        criteria: "ALL\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "bare LF in SEARCH criteria must be rejected (RFC 3501 Section 2.2)"
    );
}

// ---------------------------------------------------------------
// Pre-refactor coverage: sequence-set validation via SequenceSet newtype
// RFC 3501 Section 9: sequence-set = (seq-number / seq-range) *("," ...)
// where seq-number = nz-number / "*", nz-number = digit-nz *DIGIT.
// "0", "abc", and "" are all invalid per the ABNF and are rejected at
// SequenceSet construction time.
// ---------------------------------------------------------------

/// RFC 3501 Section 9: invalid sequence sets are rejected at construction.
#[test]
fn sequence_set_newtype_rejects_invalid_values() {
    // "0"  -  nz-number does not allow zero
    assert!(SequenceSet::<crate::types::Uid>::new("0").is_err());
    // "abc"  -  not a valid nz-number or seq-range
    assert!(SequenceSet::<crate::types::Uid>::new("abc").is_err());
    // ""  -  empty string is not a valid sequence-set
    assert!(SequenceSet::<crate::types::Uid>::new("").is_err());
}

/// RFC 7162 Section 7: known-uids / known-sequence-set / known-uid-set
/// must reject values that are not syntactically valid `sequence-set`
/// or that contain `*` or `$`.
#[test]
fn known_sequence_set_rejects_invalid() {
    // Empty string  -  not a valid sequence-set
    assert!(SequenceSet::<crate::types::Uid>::new_known("").is_err());
    // Non-numeric content  -  not valid nz-number
    assert!(SequenceSet::<crate::types::Uid>::new_known("abc").is_err());
    // Contains spaces  -  not valid sequence-set syntax
    assert!(SequenceSet::<crate::types::Uid>::new_known("1 2").is_err());
    // Contains wildcard  -  disallowed per RFC 7162 Section 3.2.5.2
    assert!(SequenceSet::<crate::types::Uid>::new_known("1:*").is_err());
    assert!(SequenceSet::<crate::types::Uid>::new_known("*").is_err());
    // Contains $ (RFC 5182 search result reference)  -  not meaningful in QRESYNC
    assert!(SequenceSet::<crate::types::Uid>::new_known("$").is_err());
    assert!(SequenceSet::<crate::types::Uid>::new_known("1,$").is_err());
    // Standalone zero  -  not a valid nz-number
    assert!(SequenceSet::<crate::types::Uid>::new_known("0").is_err());
    // Leading zeros  -  not a valid nz-number (digit-nz *DIGIT)
    assert!(SequenceSet::<crate::types::Uid>::new_known("01").is_err());
    // Overflow  -  exceeds u32::MAX
    assert!(SequenceSet::<crate::types::Uid>::new_known("4294967296").is_err());
    // Trailing comma  -  produces empty part
    assert!(SequenceSet::<crate::types::Uid>::new_known("1,").is_err());
    // Double colon  -  produces empty range element
    assert!(SequenceSet::<crate::types::Uid>::new_known("1::2").is_err());
}

/// RFC 7162 Section 7: known-uids / known-sequence-set / known-uid-set
/// must be syntactically valid `sequence-set` (with `*` and `$` disallowed).
#[test]
fn known_sequence_set_accepts_valid() {
    // Simple nz-numbers and ranges
    assert!(SequenceSet::<crate::types::Uid>::new_known("1").is_ok());
    assert!(SequenceSet::<crate::types::Uid>::new_known("1:100").is_ok());
    assert!(SequenceSet::<crate::types::Uid>::new_known("1,2,3").is_ok());
    assert!(SequenceSet::<crate::types::Uid>::new_known("1:5,10:20").is_ok());
    assert!(SequenceSet::<crate::types::Uid>::new_known("4294967295").is_ok()); // u32::MAX
}

/// RFC 3501 Section 9: zero is not a valid nz-number.
#[test]
fn sequence_set_rejects_zero() {
    assert!(
        SequenceSet::<crate::types::Uid>::new("0").is_err(),
        "Zero must be rejected per RFC 3501 Section 9 (nz-number)"
    );
}

/// RFC 3501 Section 9: u32-overflow sequence set is rejected at construction time.
#[test]
fn sequence_set_rejects_overflow() {
    assert!(
        SequenceSet::<crate::types::Uid>::new("99999999999").is_err(),
        "u32 overflow must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9: non-numeric sequence set is rejected at construction time.
#[test]
fn sequence_set_rejects_alphabetic() {
    assert!(
        SequenceSet::<crate::types::Uid>::new("abc").is_err(),
        "Alphabetic string must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9: empty sequence set is rejected at construction time.
#[test]
fn sequence_set_rejects_empty() {
    assert!(
        SequenceSet::<crate::types::Uid>::new("").is_err(),
        "Empty string must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9: invalid sequence set "0" is rejected at
/// construction time by `SequenceSet::new()`.
#[test]
fn sequence_set_rejects_zero_via_newtype() {
    assert!(
        SequenceSet::<crate::types::Uid>::new("0").is_err(),
        "\"0\" must be rejected per RFC 3501 Section 9 (nz-number)"
    );
}

/// RFC 3501 Section 9: `nz-number` is a non-zero unsigned 32-bit integer
/// (0 < n < 4,294,967,296). Values exceeding `u32::MAX` must be rejected.
#[test]
fn sequence_set_rejects_overflow_u32() {
    // 99999999999 exceeds u32::MAX (4294967295)
    assert!(SequenceSet::<crate::types::Uid>::new("99999999999").is_err());
    // u32::MAX itself is valid
    assert!(SequenceSet::<crate::types::Uid>::new("4294967295").is_ok());
    // u32::MAX + 1 is invalid
    assert!(SequenceSet::<crate::types::Uid>::new("4294967296").is_err());
    // Very large number in a range
    assert!(SequenceSet::<crate::types::Uid>::new("1:99999999999").is_err());
    assert!(SequenceSet::<crate::types::Uid>::new("99999999999:1").is_err());
}

/// L9: `SequenceSet::new` rejects invalid input (RFC 3501 Section 9).
#[test]
fn audit_l9_sequence_set_rejects_invalid() {
    assert!(SequenceSet::<crate::types::Uid>::new("abc").is_err());
    assert!(SequenceSet::<crate::types::Uid>::new("0").is_err());
    assert!(SequenceSet::<crate::types::Uid>::new("").is_err());
    assert!(SequenceSet::<crate::types::Uid>::new("1,,2").is_err());
    assert!(SequenceSet::<crate::types::Uid>::new("1:*").is_ok());
    assert!(SequenceSet::<crate::types::Uid>::new("1,2,3").is_ok());
}

/// L14: Validate sequence set format.
///
/// RFC 3501 Section 9: `sequence-set = (seq-number / seq-range) *("," sequence-set)`
/// where `seq-number = nz-number / "*"` and `nz-number = digit-nz *DIGIT`.
/// Invalid sequence sets like empty strings, alphabetic text, or space-separated
/// L14: sequence-set validation  -  RFC 3501 Section 9 says non-digit sequence
/// numbers must be rejected. The `SequenceSet` newtype catches these at
/// construction time.
#[test]
fn spec_audit_l14_invalid_sequence_set() {
    // "abc" is not a valid sequence set per RFC 3501 Section 9.
    // SequenceSet::new() rejects it at construction time.
    assert!(SequenceSet::<crate::types::Uid>::new("abc").is_err());
}
