use super::*;

#[test]
fn system_flags_round_trip() {
    let flags = [
        Flag::Seen,
        Flag::Answered,
        Flag::Flagged,
        Flag::Deleted,
        Flag::Draft,
        Flag::Recent,
    ];
    for flag in &flags {
        let s = flag.as_imap_str();
        let parsed = Flag::from_imap_str(s);
        assert_eq!(*flag, parsed);
    }
}

#[test]
fn case_insensitive_parsing() {
    assert_eq!(Flag::from_imap_str("\\SEEN"), Flag::Seen);
    assert_eq!(Flag::from_imap_str("\\seen"), Flag::Seen);
    assert_eq!(Flag::from_imap_str("\\Seen"), Flag::Seen);
}

#[test]
fn custom_flag_preserved() {
    let flag = Flag::from_imap_str("$Important");
    assert_eq!(flag, Flag::from_imap_str("$Important"));
}

// ===== Spec audit: failing tests for known deviations =====

#[test]
fn spec_audit_l1_permanent_flag_wildcard_has_dedicated_variant() {
    // RFC 3501 Section 7.1: `flag-perm = flag / "\*"`  -  the wildcard
    // means "client can create new keywords." It deserves a dedicated
    // variant (e.g. Flag::Wildcard) rather than falling through to
    // Flag::Custom("\\*").
    let flag = Flag::from_imap_str("\\*");
    assert!(
        !matches!(flag, Flag::Custom(_)),
        "\\* should have a dedicated Flag variant, not Flag::Custom; got {flag:?}"
    );
    assert_eq!(flag, Flag::Wildcard);
}

#[test]
fn wildcard_round_trip() {
    // RFC 3501 Section 7.1: `flag-perm = flag / "\*"`
    let flag = Flag::Wildcard;
    let wire = flag.as_imap_str();
    assert_eq!(wire, "\\*");
    let parsed = Flag::from_imap_str(wire);
    assert_eq!(parsed, Flag::Wildcard);
}

// ===== Case-insensitive Custom flag equality (RFC 3501 Section 2.3.2) =====

#[test]
fn custom_flag_equality_is_case_insensitive() {
    // RFC 3501 Section 2.3.2: "A flag can be permanent or session-only...
    // Flag names are case-insensitive."
    assert_eq!(
        Flag::from_imap_str("$Important"),
        Flag::from_imap_str("$important"),
        "Custom flags must compare case-insensitively per RFC 3501 Section 2.3.2"
    );
    assert_eq!(
        Flag::from_imap_str("$JUNK"),
        Flag::from_imap_str("$Junk"),
        "Custom flags must compare case-insensitively per RFC 3501 Section 2.3.2"
    );
}

#[test]
fn custom_flag_hash_is_case_insensitive() {
    // RFC 3501 Section 2.3.2: Flag names are case-insensitive, so
    // case-different Custom flags must hash to the same bucket.
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(Flag::from_imap_str("$Important"));
    // Inserting the same flag in different case should not grow the set.
    set.insert(Flag::from_imap_str("$important"));
    assert_eq!(
        set.len(),
        1,
        "Case-insensitively equal Custom flags must have the same Hash per RFC 3501 Section 2.3.2"
    );
}

// ===== RFC 3501 Section 2.3.2 audit: cross-representation equality =====

#[test]
fn custom_seen_equals_seen_variant() {
    // RFC 3501 Section 2.3.2: "Flag names are case-insensitive."
    // Custom("\\Seen") represents the same flag as Flag::Seen.
    assert_eq!(
        Flag::from_imap_str("\\Seen"),
        Flag::Seen,
        "Custom(\"\\\\Seen\") must equal Flag::Seen per RFC 3501 Section 2.3.2"
    );
}

#[test]
fn custom_system_flag_cross_representation_hash() {
    // RFC 3501 Section 2.3.2: equal flags must hash identically.
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(Flag::Seen);
    set.insert(Flag::from_imap_str("\\Seen"));
    assert_eq!(
        set.len(),
        1,
        "Custom(\"\\\\Seen\") and Flag::Seen must hash the same per RFC 3501 Section 2.3.2"
    );
}

#[test]
fn custom_wildcard_equals_wildcard_variant() {
    // RFC 3501 Section 7.1: permanent flag wildcard.
    assert_eq!(
        Flag::from_imap_str("\\*"),
        Flag::Wildcard,
        "Custom(\"\\\\*\") must equal Flag::Wildcard per RFC 3501 Section 7.1"
    );
}

// ===== From<String> and From<&str> (RFC 3501 Section 2.3.2) =====

#[test]
fn from_str_known_system_flag() {
    // RFC 3501 Section 2.3.2: system flags.
    assert_eq!(Flag::from("\\Seen"), Flag::Seen);
    assert_eq!(Flag::from("\\Answered"), Flag::Answered);
    assert_eq!(Flag::from("\\Flagged"), Flag::Flagged);
    assert_eq!(Flag::from("\\Deleted"), Flag::Deleted);
    assert_eq!(Flag::from("\\Draft"), Flag::Draft);
    assert_eq!(Flag::from("\\Recent"), Flag::Recent);
    assert_eq!(Flag::from("\\*"), Flag::Wildcard);
}

#[test]
fn from_string_known_system_flag() {
    // RFC 3501 Section 2.3.2: system flags via owned String.
    assert_eq!(Flag::from("\\Seen".to_owned()), Flag::Seen);
    assert_eq!(Flag::from("\\Flagged".to_owned()), Flag::Flagged);
}

#[test]
fn from_str_case_insensitive() {
    // RFC 3501 Section 2.3.2: flag names are case-insensitive.
    assert_eq!(Flag::from("\\SEEN"), Flag::Seen);
    assert_eq!(Flag::from("\\seen"), Flag::Seen);
    assert_eq!(Flag::from("\\DRAFT"), Flag::Draft);
}

#[test]
fn from_str_custom_flag() {
    // RFC 3501 Section 2.3.2: keyword flags.
    assert_eq!(Flag::from("$Custom"), Flag::from_imap_str("$Custom"));
    assert_eq!(Flag::from("$Important"), Flag::from_imap_str("$Important"));
}

#[test]
fn writable_view_excludes_server_only_flags_and_validates_keywords() {
    use super::StoreFlag;
    for token in ["\\recent", "\\*"] {
        let flag = Flag::from_imap_str(token);
        assert!(StoreFlag::from_observed(&flag).unwrap().is_none());
    }
    for token in ["", "has space", "bad(flag", "bad\\flag", "nonascii-\u{e9}"] {
        let flag = Flag::from_imap_str(token);
        assert!(StoreFlag::from_observed(&flag).is_err(), "{token:?}");
    }
    let flag = Flag::from_imap_str("$Important");
    assert_eq!(
        StoreFlag::from_observed(&flag)
            .unwrap()
            .unwrap()
            .as_imap_str(),
        "$Important"
    );
}
