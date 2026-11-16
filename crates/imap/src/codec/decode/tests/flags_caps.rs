use super::*;

// ===== Flag / Capability / Response code tests =====

#[test]
fn flag_system() {
    let (_, f) = flag_or_perm(b"\\Seen rest").unwrap();
    assert_eq!(f, Flag::Seen);
}

#[test]
fn flag_custom() {
    let (_, f) = flag_or_perm(b"$Important rest").unwrap();
    assert_eq!(f, Flag::from_imap_str("$Important"));
}

/// `\*` is only valid in `flag-perm` (PERMANENTFLAGS), not `flag`
/// (RFC 3501 Section 9).  `flag_or_perm` accepts it for parse robustness;
/// `flag_list` filters it out.
#[test]
fn flag_or_perm_accepts_wildcard() {
    let (_, f) = flag_or_perm(b"\\* rest").unwrap();
    assert_eq!(f, Flag::Wildcard);
}

/// `flag_list` must filter `\*` per RFC 3501 Section 9.
#[test]
fn flag_list_filters_wildcard() {
    let (_, flags) = flag_list(b"(\\Seen \\*)").unwrap();
    assert_eq!(flags.len(), 1, "\\* must be filtered from flag_list");
    assert_eq!(flags[0], Flag::Seen);
}

/// `flag_perm_list` retains `\*` per RFC 3501 Section 7.1.
#[test]
fn flag_perm_list_retains_wildcard() {
    let (_, flags) = flag_perm_list(b"(\\Seen \\*)").unwrap();
    assert_eq!(flags.len(), 2, "\\* must be retained in flag_perm_list");
    assert!(flags.contains(&Flag::Wildcard));
}

#[test]
fn flag_list_basic() {
    let (_, flags) = flag_list(b"(\\Seen \\Flagged $Important)").unwrap();
    assert_eq!(flags.len(), 3);
    assert_eq!(flags[0], Flag::Seen);
    assert_eq!(flags[1], Flag::Flagged);
}

/// `flag_list` must retain `\Recent` for `* FLAGS` responses.
///
/// Many servers include `\Recent` in `* FLAGS (...)` even though the
/// RFC 3501 Section 9 `flag` production excludes it. Filtering it
/// loses information about server-supported flags. Postel's law
/// mandates accepting this common server behavior.
#[test]
fn flag_list_retains_recent() {
    let (_, flags) = flag_list(b"(\\Seen \\Recent \\Flagged)").unwrap();
    assert_eq!(flags.len(), 3, "\\Recent must be retained in flag_list");
    assert!(
        flags.contains(&Flag::Recent),
        "\\Recent was filtered from flag_list, losing server flag info"
    );
}

#[test]
fn flag_list_empty() {
    let (_, flags) = flag_list(b"()").unwrap();
    assert!(flags.is_empty());
}

#[test]
fn flag_perm_list_empty() {
    let (_, flags) = flag_perm_list(b"()").unwrap();
    assert!(flags.is_empty());
}

#[test]
fn capability_known() {
    let (_, cap) = capability(b"IMAP4rev1").unwrap();
    assert_eq!(cap, Capability::Imap4Rev1);
}

#[test]
fn capability_auth() {
    let (_, cap) = capability(b"AUTH=PLAIN").unwrap();
    assert_eq!(cap, Capability::from_imap_str("AUTH=PLAIN"));
}

/// RFC 3501 Section 9 / RFC 9051 Section 9:
/// `capability = ("AUTH=" auth-type) / atom`
/// and `auth-type = atom`, so a bare `AUTH=` is not a valid AUTH
/// capability. It must be preserved as an unknown atom instead of being
/// modeled as `Capability::Auth("")`.
#[test]
fn capability_auth_empty_suffix_is_other() {
    let (_, cap) = capability(b"AUTH=").unwrap();
    assert!(
        matches!(cap, Capability::Other(ref s) if s == "AUTH="),
        "malformed AUTH= capability must stay Other(\"AUTH=\"), got {cap:?}"
    );
}

/// RFC 5256 says the THREAD capability is `THREAD=` followed by a
/// supported threading algorithm name. An empty suffix is malformed and
/// must remain `Other(...)` for forward-compatible tolerance.
#[test]
fn capability_thread_empty_suffix_is_other() {
    let (_, cap) = capability(b"THREAD=").unwrap();
    assert!(
        matches!(cap, Capability::Other(ref s) if s == "THREAD="),
        "malformed THREAD= capability must stay Other(\"THREAD=\"), got {cap:?}"
    );
}

/// RFC 9208 Section 8 defines `capa-quota-res = "QUOTA=RES-" resource-name`.
/// The resource-name is required, so a bare `QUOTA=RES-` must not become
/// `Capability::QuotaResource("")`.
#[test]
fn capability_quota_resource_empty_suffix_is_other() {
    let (_, cap) = capability(b"QUOTA=RES-").unwrap();
    assert!(
        matches!(cap, Capability::Other(ref s) if s == "QUOTA=RES-"),
        "malformed QUOTA=RES- capability must stay Other(\"QUOTA=RES-\"), got {cap:?}"
    );
}

/// RFC 4314 defines `rights-capa = "RIGHTS=" new-rights` with
/// `new-rights = 1*LOWER-ALPHA`, so an empty `RIGHTS=` token is malformed
/// and must be preserved as an unknown capability atom.
#[test]
fn capability_rights_empty_suffix_is_other() {
    let (_, cap) = capability(b"RIGHTS=").unwrap();
    assert!(
        matches!(cap, Capability::Other(ref s) if s == "RIGHTS="),
        "malformed RIGHTS= capability must stay Other(\"RIGHTS=\"), got {cap:?}"
    );
}

#[test]
fn capability_unknown() {
    let (_, cap) = capability(b"XYZZY").unwrap();
    assert_eq!(cap, Capability::from_imap_str("XYZZY"));
}

#[test]
fn response_code_uidvalidity() {
    let (_, code) = response_code(b"[UIDVALIDITY 12345]").unwrap();
    assert_eq!(code, ResponseCode::UidValidity(12345));
}

#[test]
fn response_code_permanentflags() {
    let (_, code) = response_code(b"[PERMANENTFLAGS (\\Seen \\Flagged \\*)]").unwrap();
    if let ResponseCode::PermanentFlags(flags) = &code {
        assert_eq!(flags.len(), 3);
    } else {
        panic!("expected PermanentFlags, got {code:?}");
    }
}

#[test]
fn response_code_appenduid() {
    let (_, code) = response_code(b"[APPENDUID 1234 5678]").unwrap();
    assert_eq!(
        code,
        ResponseCode::AppendUid {
            uid_validity: 1234,
            uids: vec![UidRange::single(5678)],
        }
    );
}

/// APPENDUID with a uid-set (MULTIAPPEND, RFC 3502 / RFC 4315 Section 3).
#[test]
fn response_code_appenduid_set() {
    let (_, code) = response_code(b"[APPENDUID 1234 100:102]").unwrap();
    assert_eq!(
        code,
        ResponseCode::AppendUid {
            uid_validity: 1234,
            uids: vec![UidRange::range(100, 102)],
        }
    );
}

/// Non-conformant servers (e.g., some Dovecot configurations) may
/// send uidvalidity 0. Per Postel's law (RFC 1122 Section 1.2.2)
/// and consistency with the UIDVALIDITY response code parser which
/// already accepts 0 (RFC 3501 Section 7.1), APPENDUID must also
/// accept 0.
#[test]
fn response_code_appenduid_uidvalidity_zero() {
    let (_, code) = response_code(b"[APPENDUID 0 42]").expect(
        "APPENDUID with uidvalidity 0 must parse per Postel's law, \
             consistent with UIDVALIDITY response code tolerance",
    );
    assert_eq!(
        code,
        ResponseCode::AppendUid {
            uid_validity: 0,
            uids: vec![UidRange::single(42)],
        }
    );
}

/// Same Postel's-law tolerance as APPENDUID for uidvalidity 0.
#[test]
fn response_code_copyuid_uidvalidity_zero() {
    let (_, code) = response_code(b"[COPYUID 0 1 2]").expect(
        "COPYUID with uidvalidity 0 must parse per Postel's law, \
             consistent with UIDVALIDITY response code tolerance",
    );
    if let ResponseCode::CopyUid(crate::types::response::CopyUid {
        uid_validity,
        source_uids,
        dest_uids,
    }) = &code
    {
        assert_eq!(*uid_validity, 0);
        assert_eq!(source_uids.len(), 1);
        assert_eq!(dest_uids.len(), 1);
    } else {
        panic!("expected CopyUid, got {code:?}");
    }
}

#[test]
fn response_code_copyuid() {
    let (_, code) = response_code(b"[COPYUID 1234 1:5 10:14]").unwrap();
    if let ResponseCode::CopyUid(crate::types::response::CopyUid {
        uid_validity,
        source_uids,
        dest_uids,
    }) = &code
    {
        assert_eq!(*uid_validity, 1234);
        assert_eq!(source_uids.len(), 1);
        assert_eq!(source_uids[0], UidRange::range(1, 5));
        assert_eq!(dest_uids.len(), 1);
    } else {
        panic!("expected CopyUid");
    }
}

#[test]
fn response_code_rfc5530() {
    let (_, code) = response_code(b"[UNAVAILABLE]").unwrap();
    assert_eq!(code, ResponseCode::Unavailable);
    let (_, code) = response_code(b"[NOPERM]").unwrap();
    assert_eq!(code, ResponseCode::NoPerm);
}

#[test]
fn response_code_unknown() {
    let (_, code) = response_code(b"[XFOO bar baz]").unwrap();
    assert_eq!(
        code,
        ResponseCode::Other {
            name: "XFOO".into(),
            value: Some("bar baz".into()),
        }
    );
}

#[test]
fn uid_range_single() {
    let (_, r) = uid_range(b"42").unwrap();
    assert_eq!(r, UidRange::single(42));
}

#[test]
fn uid_range_pair() {
    let (_, r) = uid_range(b"1:100").unwrap();
    assert_eq!(r, UidRange::range(1, 100));
}

#[test]
fn uid_set_multiple() {
    let (_, set) = uid_set(b"1:5,10,20:30").unwrap();
    assert_eq!(set.len(), 3);
}

#[test]
fn greeting_with_capability() {
    let (_, resp) = parse_greeting(b"* OK [CAPABILITY IMAP4rev1 IDLE] ready\r\n").unwrap();
    if let Response::Greeting(g) = resp {
        assert_eq!(g.status, GreetingStatus::Ok);
        if let Some(ResponseCode::Capability(caps)) = &g.code {
            assert!(caps.contains(&Capability::Imap4Rev1));
            assert!(caps.contains(&Capability::Idle));
        } else {
            panic!("expected Capability code, got {:?}", g.code);
        }
    } else {
        panic!("expected Greeting");
    }
}

#[test]
fn untagged_capability() {
    let (_, resp) = parse_response(b"* CAPABILITY IMAP4rev1 IDLE LITERAL+\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Capability(caps) = &*u {
            assert!(caps.contains(&Capability::Imap4Rev1));
            assert!(caps.contains(&Capability::Idle));
            assert!(caps.contains(&Capability::LiteralPlus));
        } else {
            panic!("expected Capability");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_flags() {
    let (_, resp) =
        parse_response(b"* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Flags(flags) = &*u {
            assert_eq!(flags.len(), 5);
        } else {
            panic!("expected Flags");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ENABLED has the same `*(SP atom)` pattern.
/// A trailing space in `* ENABLED \r\n` must be tolerated.
///
/// RFC 5161 Section 3.2: `"ENABLED" *(SP capability)`
#[test]
fn enabled_trailing_space_empty() {
    let (_, resp) = parse_response(b"* ENABLED \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Enabled(caps) = &*u {
            assert!(caps.is_empty());
        } else {
            panic!("expected Enabled, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ENABLED with capabilities and trailing space.
#[test]
fn enabled_trailing_space_with_caps() {
    let (_, resp) = parse_response(b"* ENABLED CONDSTORE QRESYNC \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Enabled(caps) = &*u {
            assert_eq!(caps, &["CONDSTORE", "QRESYNC"]);
        } else {
            panic!("expected Enabled, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// CAPABILITY with trailing space before CRLF.
/// `separated_list0(char(' '), capability)` stops before the trailing SP,
/// then `crlf` must tolerate it.
///
/// RFC 3501 Section 7.2.1: `capability-data = "CAPABILITY" *(SP capability)`
#[test]
fn capability_trailing_space() {
    let (_, resp) = parse_response(b"* CAPABILITY IMAP4rev1 IDLE \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Capability(caps) = &*u {
            assert!(
                caps.len() >= 2,
                "should parse capabilities despite trailing space"
            );
        } else {
            panic!("expected Capability, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// FLAGS with trailing space before CRLF.
/// `flag_list` parses the parenthesized list, leaving trailing SP for `crlf`.
///
/// RFC 3501 Section 7.2.6: `"FLAGS" SP flag-list`
#[test]
fn flags_trailing_space() {
    let (_, resp) = parse_response(b"* FLAGS (\\Seen \\Answered) \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Flags(flags) = &*u {
            assert_eq!(flags.len(), 2);
        } else {
            panic!("expected Flags, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== Extension response parser tests =====

#[test]
fn untagged_enabled() {
    let input = b"ENABLED CONDSTORE UTF8=ACCEPT\r\n";
    let (_, resp) = parse_untagged_enabled(input).unwrap();
    if let UntaggedResponse::Enabled(caps) = resp {
        assert_eq!(caps, vec!["CONDSTORE", "UTF8=ACCEPT"]);
    } else {
        panic!("expected Enabled, got {resp:?}");
    }
}

#[test]
fn untagged_enabled_empty() {
    let input = b"ENABLED\r\n";
    let (_, resp) = parse_untagged_enabled(input).unwrap();
    if let UntaggedResponse::Enabled(caps) = resp {
        assert!(caps.is_empty());
    } else {
        panic!("expected Enabled, got {resp:?}");
    }
}

// ===== ResponseCode variant tests =====

#[test]
fn response_code_alert() {
    let (_, code) = response_code(b"[ALERT]").unwrap();
    assert_eq!(code, ResponseCode::Alert);
}

#[test]
fn response_code_read_only() {
    let (_, code) = response_code(b"[READ-ONLY]").unwrap();
    assert_eq!(code, ResponseCode::ReadOnly);
}

#[test]
fn response_code_read_write() {
    let (_, code) = response_code(b"[READ-WRITE]").unwrap();
    assert_eq!(code, ResponseCode::ReadWrite);
}

#[test]
fn response_code_trycreate() {
    let (_, code) = response_code(b"[TRYCREATE]").unwrap();
    assert_eq!(code, ResponseCode::TryCreate);
}

#[test]
fn response_code_parse() {
    let (_, code) = response_code(b"[PARSE]").unwrap();
    assert_eq!(code, ResponseCode::Parse);
}

#[test]
fn response_code_unseen() {
    let (_, code) = response_code(b"[UNSEEN 17]").unwrap();
    assert_eq!(code, ResponseCode::Unseen(17));
}

#[test]
fn response_code_uidnext() {
    let (_, code) = response_code(b"[UIDNEXT 4392]").unwrap();
    assert_eq!(code, ResponseCode::UidNext(4392));
}

#[test]
fn response_code_closed() {
    let (_, code) = response_code(b"[CLOSED]").unwrap();
    assert_eq!(code, ResponseCode::Closed);
}

#[test]
fn response_code_modified() {
    let (_, code) = response_code(b"[MODIFIED 1:5,10]").unwrap();
    if let ResponseCode::Modified(uids) = code {
        assert_eq!(uids.len(), 2);
        assert_eq!(uids[0], UidRange::range(1, 5));
        assert_eq!(uids[1], UidRange::single(10));
    } else {
        panic!("expected Modified, got {code:?}");
    }
}

/// RFC 7162 Section 3.1.3 / Section 7: MODIFIED carries a `sequence-set`.
/// For `STORE`, that means message sequence numbers, so the `*` wildcard is
/// legal and must parse.
#[test]
fn response_code_modified_accepts_star_range() {
    let (_, code) = response_code(b"[MODIFIED 1:*]").unwrap();
    if let ResponseCode::Modified(ranges) = code {
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start(), 1);
        assert_eq!(ranges[0].end_bound(), Some(crate::types::SeqBound::Star));
    } else {
        panic!("expected Modified, got {code:?}");
    }
}

/// RFC 7162 Section 3.1.3 / Section 7: bare `*` is also a legal
/// `sequence-set` response for `STORE`.
#[test]
fn response_code_modified_accepts_bare_star() {
    let (_, code) = response_code(b"[MODIFIED *]").unwrap();
    if let ResponseCode::Modified(ranges) = code {
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start(), u32::MAX);
        assert_eq!(ranges[0].end(), None);
        assert_eq!(ranges[0].start_bound(), crate::types::SeqBound::Star);
    } else {
        panic!("expected Modified, got {code:?}");
    }
}

/// RFC 7162 Section 3.1.3: MODIFIED with mixed numeric entries but no `*`
/// must still parse successfully.
#[test]
fn response_code_modified_star_mixed_no_star() {
    let (_, code) = response_code(b"[MODIFIED 1,5:20,10]").unwrap();
    if let ResponseCode::Modified(ranges) = code {
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], UidRange::single(1));
        assert_eq!(ranges[1], UidRange::range(5, 20));
        assert_eq!(ranges[2], UidRange::single(10));
    } else {
        panic!("expected Modified, got {code:?}");
    }
}

#[test]
fn response_code_badcharset() {
    let (_, code) = response_code(b"[BADCHARSET (\"UTF-8\" \"US-ASCII\")]").unwrap();
    if let ResponseCode::BadCharset(charsets) = code {
        assert_eq!(charsets, vec!["UTF-8", "US-ASCII"]);
    } else {
        panic!("expected BadCharset, got {code:?}");
    }
}

#[test]
fn response_code_badcharset_empty() {
    let (_, code) = response_code(b"[BADCHARSET]").unwrap();
    if let ResponseCode::BadCharset(charsets) = code {
        assert!(charsets.is_empty());
    } else {
        panic!("expected BadCharset, got {code:?}");
    }
}

#[test]
fn response_code_capability() {
    let (_, code) = response_code(b"[CAPABILITY IMAP4rev1 IDLE LITERAL+]").unwrap();
    if let ResponseCode::Capability(caps) = code {
        assert_eq!(caps.len(), 3);
        assert_eq!(caps[0], Capability::Imap4Rev1);
        assert_eq!(caps[1], Capability::Idle);
        assert_eq!(caps[2], Capability::LiteralPlus);
    } else {
        panic!("expected Capability, got {code:?}");
    }
}

#[test]
fn response_code_rfc5530_all() {
    // Test all RFC 5530 codes
    let codes: Vec<(&[u8], ResponseCode)> = vec![
        (b"[UNAVAILABLE]", ResponseCode::Unavailable),
        (
            b"[AUTHENTICATIONFAILED]",
            ResponseCode::AuthenticationFailed,
        ),
        (b"[AUTHORIZATIONFAILED]", ResponseCode::AuthorizationFailed),
        (b"[EXPIRED]", ResponseCode::Expired),
        (b"[PRIVACYREQUIRED]", ResponseCode::PrivacyRequired),
        (b"[CONTACTADMIN]", ResponseCode::ContactAdmin),
        (b"[NOPERM]", ResponseCode::NoPerm),
        (b"[INUSE]", ResponseCode::InUse),
        (b"[EXPUNGEISSUED]", ResponseCode::ExpungeIssued),
        (b"[CORRUPTION]", ResponseCode::Corruption),
        (b"[SERVERBUG]", ResponseCode::ServerBug),
        (b"[CLIENTBUG]", ResponseCode::ClientBug),
        (b"[CANNOT]", ResponseCode::Cannot),
        (b"[LIMIT]", ResponseCode::Limit),
        (b"[OVERQUOTA]", ResponseCode::OverQuota),
        (b"[ALREADYEXISTS]", ResponseCode::AlreadyExists),
        (b"[NONEXISTENT]", ResponseCode::NonExistent),
    ];
    for (input, expected) in codes {
        let (_, code) = response_code(input).unwrap();
        assert_eq!(
            code,
            expected,
            "failed for input {:?}",
            std::str::from_utf8(input)
        );
    }
}

/// RFC 5530 Section 6 registers additional standard response codes beyond
/// the core RFC 5530 Section 3 set. They must not fall through to
/// `ResponseCode::Other`; consumers need dedicated variants even when the
/// extension-specific payload is preserved verbatim.
#[test]
fn response_code_registry_extensions_are_typed() {
    let cases: [(&[u8], &str, &str); 8] = [
        (
            b"[REFERRAL imap://example.com/]",
            "Referral",
            "imap://example.com/",
        ),
        (b"[URLMECH INTERNAL]", "UrlMech", "INTERNAL"),
        (
            b"[BADURL imap://bad.example/]",
            "BadUrl",
            "imap://bad.example/",
        ),
        (
            b"[BADCOMPARATOR i;ascii-casemap]",
            "BadComparator",
            "i;ascii-casemap",
        ),
        (b"[ANNOTATE /comment]", "Annotate", "/comment"),
        (b"[MAXCONVERTMESSAGES 25]", "MaxConvertMessages", "25"),
        (b"[MAXCONVERTPARTS 10]", "MaxConvertParts", "10"),
        (b"[NOUPDATE]", "NoUpdate", ""),
    ];

    for (input, expected_variant, expected_payload) in cases {
        let (_, code) = response_code(input).unwrap();
        assert!(
            !matches!(code, ResponseCode::Other { .. }),
            "registered response code {:?} must not fall through to Other: {code:?}",
            std::str::from_utf8(input)
        );
        let dbg = format!("{code:?}");
        assert!(
            dbg.contains(expected_variant),
            "expected {expected_variant} variant for {:?}, got {dbg}",
            std::str::from_utf8(input)
        );
        if !expected_payload.is_empty() {
            assert!(
                dbg.contains(expected_payload),
                "expected payload {expected_payload:?} to be preserved for {:?}, got {dbg}",
                std::str::from_utf8(input)
            );
        }
    }
}

#[test]
fn flag_list_mismatched_paren() {
    assert!(flag_list(b"(\\Seen").is_err());
}

#[test]
fn response_code_case_insensitive() {
    let (_, code) = response_code(b"[alert]").unwrap();
    assert_eq!(code, ResponseCode::Alert);

    let (_, code) = response_code(b"[read-only]").unwrap();
    assert_eq!(code, ResponseCode::ReadOnly);
}

#[test]
fn skip_unknown_response_code() {
    let (_, code) = response_code(b"[XYZFUTURE 42]").unwrap();
    if let ResponseCode::Other { name, value } = code {
        assert_eq!(name, "XYZFUTURE");
        assert_eq!(value.as_deref(), Some("42"));
    } else {
        panic!("expected Other, got {code:?}");
    }
}

/// continuation responses must parse `[response-code]` from resp-text.
/// RFC 3501 Section 7.5: `continue-req = "+" SP (resp-text / base64) CRLF`
/// where `resp-text = ["[" resp-text-code "]" SP] text`.
#[test]
fn continuation_with_response_code() {
    let (_, cont) = parse_continuation(b"+ [ALERT] Please continue\r\n").unwrap();
    assert_eq!(cont.code, Some(ResponseCode::Alert));
    assert_eq!(cont.data, "Please continue");
}

/// Continuation with response code but no trailing text.
/// RFC 3501 Section 7.5.
#[test]
fn continuation_with_response_code_no_text() {
    let (_, cont) = parse_continuation(b"+ [ALERT]\r\n").unwrap();
    assert_eq!(cont.code, Some(ResponseCode::Alert));
    assert_eq!(cont.data, "");
}

// ===== RFC 5530 extended response code tests =====

#[test]
fn response_code_rfc5530_unavailable() {
    let input = b"A001 NO [UNAVAILABLE] Try again later\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::Unavailable));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_overquota() {
    let input = b"A001 NO [OVERQUOTA] Mailbox is full\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::OverQuota));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_alreadyexists() {
    let input = b"A001 NO [ALREADYEXISTS] Mailbox already exists\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::AlreadyExists));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_nonexistent() {
    let input = b"A001 NO [NONEXISTENT] Mailbox does not exist\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::NonExistent));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_noperm() {
    let input = b"A001 NO [NOPERM] Permission denied\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::NoPerm));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_authenticationfailed() {
    let input = b"A001 NO [AUTHENTICATIONFAILED] Invalid credentials\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::AuthenticationFailed));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_expired() {
    let input = b"A001 NO [EXPIRED] Credentials expired\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::Expired));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_contactadmin() {
    let input = b"A001 NO [CONTACTADMIN] Contact administrator\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::ContactAdmin));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_inuse() {
    let input = b"A001 NO [INUSE] Resource locked\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::InUse));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_corruption() {
    let input = b"A001 NO [CORRUPTION] Data corruption detected\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::Corruption));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_serverbug() {
    let input = b"A001 NO [SERVERBUG] Internal error\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::ServerBug));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_clientbug() {
    let input = b"A001 BAD [CLIENTBUG] Nonsensical request\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::ClientBug));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_cannot() {
    let input = b"A001 NO [CANNOT] Operation not supported\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::Cannot));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_limit() {
    let input = b"A001 NO [LIMIT] Too many messages\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::Limit));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_privacyrequired() {
    let input = b"A001 NO [PRIVACYREQUIRED] Encryption required\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::PrivacyRequired));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_authorizationfailed() {
    let input = b"A001 NO [AUTHORIZATIONFAILED] Not authorized\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::AuthorizationFailed));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

#[test]
fn response_code_rfc5530_expungeissued() {
    let input = b"* OK [EXPUNGEISSUED] Expunge occurred\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { code, .. } = *u {
            assert_eq!(code, Some(ResponseCode::ExpungeIssued));
        } else {
            panic!("expected Status, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

#[test]
fn response_code_mailboxid() {
    let (_, code) = response_code(b"[MAILBOXID (F2212ea87-6097-4256-9d51-71c6f)]").unwrap();
    assert_eq!(
        code,
        ResponseCode::MailboxId("F2212ea87-6097-4256-9d51-71c6f".into())
    );
}

#[test]
fn response_code_mailboxid_simple() {
    let (_, code) = response_code(b"[MAILBOXID (abc123)]").unwrap();
    assert_eq!(code, ResponseCode::MailboxId("abc123".into()));
}

/// `ResponseCode` MAILBOXID with a valid objectid per RFC 8474 Section 7.
#[test]
fn response_code_mailboxid_rfc8474_valid_objectid() {
    let (_, code) = response_code(b"[MAILBOXID (M_id-test_123)]").unwrap();
    assert_eq!(code, ResponseCode::MailboxId("M_id-test_123".into()));
}

#[test]
fn capability_metadata() {
    let (_, cap) = capability(b"METADATA").unwrap();
    assert_eq!(cap, Capability::Metadata);
}

// ===== THREAD (RFC 5256) parser tests =====

#[test]
fn capability_thread_references() {
    let (_, cap) = capability(b"THREAD=REFERENCES").unwrap();
    assert_eq!(cap, Capability::from_imap_str("THREAD=REFERENCES"));
}

#[test]
fn capability_thread_orderedsubject() {
    let (_, cap) = capability(b"THREAD=ORDEREDSUBJECT").unwrap();
    assert_eq!(cap, Capability::from_imap_str("THREAD=ORDEREDSUBJECT"));
}

#[test]
fn capability_quota() {
    let input = b"* CAPABILITY IMAP4rev1 QUOTA\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = *boxed
    {
        assert!(caps.contains(&Capability::Quota));
        return;
    }
    panic!("expected QUOTA capability");
}

#[test]
fn capability_quotaset() {
    let input = b"* CAPABILITY IMAP4rev1 QUOTASET\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = *boxed
    {
        assert!(caps.contains(&Capability::QuotaSet));
        return;
    }
    panic!("expected QUOTASET capability");
}

#[test]
fn capability_quota_resource() {
    let input = b"* CAPABILITY IMAP4rev1 QUOTA=RES-STORAGE\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = *boxed
    {
        assert!(caps.contains(&Capability::from_imap_str("QUOTA=RES-STORAGE")));
        return;
    }
    panic!("expected QUOTA=RES-STORAGE capability");
}

#[test]
fn capability_acl() {
    let input = b"* CAPABILITY IMAP4rev1 ACL\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = *boxed
    {
        assert!(caps.contains(&Capability::Acl));
        return;
    }
    panic!("expected ACL capability");
}

/// RFC 4314 Section 6: RIGHTS=texk must be parsed as a dedicated capability.
#[test]
fn capability_rights_parsed() {
    let (_, cap) = capability(b"RIGHTS=texk").unwrap();
    match cap {
        Capability::Rights(rights) => assert_eq!(rights, "texk"),
        other => panic!("expected Rights variant, got {other:?}"),
    }
}

/// RFC 4314 Section 6: RIGHTS= is case-insensitive and the rights chars are canonicalized.
#[test]
fn capability_rights_case_insensitive() {
    let (_, cap) = capability(b"rights=TEXK").unwrap();
    match cap {
        Capability::Rights(rights) => assert_eq!(rights, "texk"),
        other => panic!("expected Rights variant, got {other:?}"),
    }
}

/// RFC 4314 Section 6: RIGHTS= in a full capability response.
#[test]
fn capability_response_includes_rights() {
    let input = b"* CAPABILITY IMAP4rev1 ACL RIGHTS=texk\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = *boxed
    {
        assert!(
            caps.iter()
                .any(|c| matches!(c, Capability::Rights(r) if r == "texk")),
            "RIGHTS=texk not found in capabilities: {caps:?}"
        );
        return;
    }
    panic!("expected capability response");
}

/// UID range parsing: single UID.
#[test]
fn uid_range_single_value() {
    let (_, ranges) = uid_set(b"42").unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].start(), 42);
    assert_eq!(ranges[0].end(), None);
}

/// UID range parsing: comma-separated set with ranges.
#[test]
fn uid_set_complex() {
    let (_, ranges) = uid_set(b"1:5,10,20:30").unwrap();
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[0], UidRange::range(1, 5));
    assert_eq!(ranges[1], UidRange::single(10));
    assert_eq!(ranges[2], UidRange::range(20, 30));
}

/// APPENDUID with multi-message uid-set (RFC 4315 / RFC 3502).
#[test]
fn response_code_appenduid_multi() {
    let (_, code) = response_code(b"[APPENDUID 1234 100,101,102]").unwrap();
    if let ResponseCode::AppendUid { uid_validity, uids } = code {
        assert_eq!(uid_validity, 1234);
        assert_eq!(uids.len(), 3);
        assert_eq!(uids[0].start(), 100);
        assert_eq!(uids[1].start(), 101);
        assert_eq!(uids[2].start(), 102);
    } else {
        panic!("expected AppendUid");
    }
}

// ===== MULTIAPPEND edge case tests (RFC 3502 / RFC 4315) =====

/// APPENDUID with non-contiguous UIDs from MULTIAPPEND (RFC 3502 / RFC 4315 Section 3).
#[test]
fn response_code_appenduid_non_contiguous() {
    let (_, code) = response_code(b"[APPENDUID 67890 100,105,110:112]").unwrap();
    if let ResponseCode::AppendUid { uid_validity, uids } = code {
        assert_eq!(uid_validity, 67890);
        assert_eq!(uids.len(), 3);
        assert_eq!(uids[0], UidRange::single(100));
        assert_eq!(uids[1], UidRange::single(105));
        assert_eq!(uids[2], UidRange::range(110, 112));
    } else {
        panic!("expected AppendUid");
    }
}

/// APPENDUID with large uid-validity and uid values (RFC 4315 Section 3).
#[test]
fn response_code_appenduid_large_values() {
    let (_, code) = response_code(b"[APPENDUID 4294967295 4294967290:4294967295]").unwrap();
    if let ResponseCode::AppendUid { uid_validity, uids } = code {
        assert_eq!(uid_validity, u32::MAX);
        assert_eq!(uids.len(), 1);
        assert_eq!(uids[0], UidRange::range(4_294_967_290, u32::MAX));
    } else {
        panic!("expected AppendUid");
    }
}

/// Tagged OK with APPENDUID after MULTIAPPEND (RFC 3502 / RFC 4315).
#[test]
fn tagged_ok_appenduid_multiappend() {
    let input = b"A001 OK [APPENDUID 12345 100,101,102] APPEND completed\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.tag, "A001");
        assert_eq!(tagged.status, StatusKind::Ok);
        if let Some(ResponseCode::AppendUid { uid_validity, uids }) = tagged.code {
            assert_eq!(uid_validity, 12345);
            assert_eq!(uids.len(), 3);
        } else {
            panic!("expected APPENDUID response code");
        }
    } else {
        panic!("expected tagged response");
    }
}

/// APPENDUID with single UID range representing multiple messages (RFC 3502).
#[test]
fn response_code_appenduid_range_multiappend() {
    // A server may assign contiguous UIDs to multiple appended messages.
    let (_, code) = response_code(b"[APPENDUID 555 200:204]").unwrap();
    if let ResponseCode::AppendUid { uid_validity, uids } = code {
        assert_eq!(uid_validity, 555);
        assert_eq!(uids.len(), 1);
        assert_eq!(uids[0], UidRange::range(200, 204));
    } else {
        panic!("expected AppendUid");
    }
}

/// Very long atom in response code falls through to Other.
#[test]
fn response_code_very_long_atom() {
    let long_name: String = "X".repeat(200);
    let input = format!("[{long_name}]");
    let (_, code) = response_code(input.as_bytes()).unwrap();
    match code {
        ResponseCode::Other { name, value } => {
            assert_eq!(name.len(), 200);
            assert!(value.is_none());
        }
        _ => panic!("expected Other response code"),
    }
}

#[test]
fn uid_range_zero_start_rejected() {
    assert!(uid_range(b"0:5").is_err());
}

#[test]
fn uid_range_zero_end_rejected() {
    assert!(uid_range(b"1:0").is_err());
}

/// Postel's law (RFC 1122 Section 1.2.2): tolerate reversed UID ranges
/// where start > end. Some non-conformant servers emit ranges like
/// `100:1` instead of `1:100`. The parser normalizes them so that
/// `start` is always ≤ `end` (RFC 3501 Section 9).
#[test]
fn uid_range_reversed_normalized() {
    let (_, r) = uid_range(b"100:1").unwrap();
    assert_eq!(r.start(), 1, "reversed range must normalize start to min");
    assert_eq!(
        r.end(),
        Some(100),
        "reversed range must normalize end to max"
    );
}

/// RFC 9051 Section 9 says nz-number for UIDVALIDITY, but real servers
/// (some Dovecot configurations) send 0 for empty mailboxes.  Accept it
/// per Postel's law, consistent with the STATUS parser (RFC 1122 Section1.2.2).
#[test]
fn response_code_uidvalidity_zero_accepted() {
    let (_, code) = response_code(b"[UIDVALIDITY 0]").unwrap();
    assert_eq!(
        code,
        ResponseCode::UidValidity(0),
        "UIDVALIDITY 0 must parse per Postel's law, matching STATUS tolerance"
    );
}

/// RFC 9051 Section 9 says nz-number for UIDNEXT, but real servers
/// (some Dovecot configurations) send 0 for empty mailboxes.  Accept it
/// per Postel's law, consistent with the STATUS parser (RFC 1122 Section1.2.2).
#[test]
fn response_code_uidnext_zero_accepted() {
    let (_, code) = response_code(b"[UIDNEXT 0]").unwrap();
    assert_eq!(
        code,
        ResponseCode::UidNext(0),
        "UIDNEXT 0 must parse per Postel's law, matching STATUS tolerance"
    );
}

/// RFC 3501 Section 7.1: some servers (Dovecot) send `[UNSEEN 0]` for
/// empty mailboxes. While strictly non-conformant (nz-number >= 1), this
/// must parse as `ResponseCode::Unseen(0)` per Postel's law rather than
/// silently falling into the human-readable text field.
#[test]
fn response_code_unseen_zero_accepted() {
    let (_, code) = response_code(b"[UNSEEN 0]").unwrap();
    assert_eq!(
        code,
        ResponseCode::Unseen(0),
        "UNSEEN 0 must parse as Unseen(0) per Postel's law \
             (RFC 3501 Section 7.1  -  real servers send this for empty mailboxes)"
    );
}

#[test]
fn response_code_appenduid_zero_uidvalidity_accepted() {
    // Per Postel's law (RFC 1122 Section 1.2.2) and consistency with
    // the UIDVALIDITY response code and STATUS parser, uidvalidity 0
    // is accepted (non-conformant servers may send it).
    let (_, code) = response_code(b"[APPENDUID 0 5678]")
        .expect("APPENDUID with uidvalidity 0 must be accepted");
    assert_eq!(
        code,
        ResponseCode::AppendUid {
            uid_validity: 0,
            uids: vec![UidRange::single(5678)],
        }
    );
}

/// RFC 4315 Section 4: uid-set requires at least one element.
/// An APPENDUID with an empty uid-set after uidvalidity must not parse
/// as a valid `AppendUid` response code.
#[test]
fn uid_set_empty_rejected() {
    // Empty uid-set must fail: separated_list1 requires >= 1 element.
    assert!(uid_set(b"").is_err());
}

/// RFC 4315 Section 4: uid-set requires at least one element.
/// `[APPENDUID 1234 ]` has an empty uid-set  -  must produce a parse error
/// or fall through to an unknown response code.
#[test]
fn response_code_appenduid_empty_uid_set_rejected() {
    assert!(response_code(b"[APPENDUID 1234 ]").is_err());
}

/// RFC 4315 Section 4: valid uid-set with ranges and single values must still parse.
#[test]
fn response_code_appenduid_valid_uid_set_with_ranges() {
    let (_, code) = response_code(b"[APPENDUID 1234 5:10,15]").unwrap();
    if let ResponseCode::AppendUid { uid_validity, uids } = code {
        assert_eq!(uid_validity, 1234);
        assert_eq!(uids.len(), 2);
        assert_eq!(uids[0], UidRange::range(5, 10));
        assert_eq!(uids[1], UidRange::single(15));
    } else {
        panic!("expected AppendUid response code");
    }
}

#[test]
fn response_code_copyuid_zero_uidvalidity_accepted() {
    // Per Postel's law (RFC 1122 Section 1.2.2) and consistency with
    // the UIDVALIDITY response code and STATUS parser, uidvalidity 0
    // is accepted (non-conformant servers may send it).
    let (_, code) = response_code(b"[COPYUID 0 1:5 10:14]")
        .expect("COPYUID with uidvalidity 0 must be accepted");
    if let ResponseCode::CopyUid(crate::types::response::CopyUid { uid_validity, .. }) = &code {
        assert_eq!(*uid_validity, 0);
    } else {
        panic!("expected CopyUid, got {code:?}");
    }
}

// ===== L4: Typed ResponseCode variants =====

/// `UIDNOTSTICKY` response code (RFC 4315 Section 2 / RFC 9051 Section 7.1).
#[test]
fn response_code_uidnotsticky() {
    let input = b"A001 OK [UIDNOTSTICKY] done\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::UidNotSticky));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

/// `NOTSAVED` response code (RFC 5182 Section 2.1).
#[test]
fn response_code_notsaved() {
    let input = b"A001 NO [NOTSAVED] search result empty\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::NotSaved));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

/// `HASCHILDREN` response code (RFC 9051 Section 7.1).
#[test]
fn response_code_haschildren() {
    let input = b"A001 NO [HASCHILDREN] mailbox has children\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::HasChildren));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

/// `UNKNOWN-CTE` response code (RFC 9051 Section 9, RFC 3516 Section 4.3).
#[test]
fn response_code_unknowncte() {
    // RFC 9051 defines the code as "UNKNOWN-CTE" (with hyphen)
    let input = b"A001 NO [UNKNOWN-CTE] unknown content-transfer-encoding\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::UnknownCte));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

/// the wire form of the response code is `UNKNOWN-CTE` (with hyphen),
/// not `UNKNOWNCTE`. Confirms the parser maps it to `ResponseCode::UnknownCte`
/// (RFC 3516 Section 4.3 / RFC 9051 Section 7.1).
#[test]
fn spec_audit_unknown_cte_response_code_has_hyphen() {
    let input = b"* NO [UNKNOWN-CTE] unsupported encoding\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { code, .. } = &*u {
            assert_eq!(
                code.as_ref().unwrap(),
                &ResponseCode::UnknownCte,
                "UNKNOWN-CTE (with hyphen) should map to ResponseCode::UnknownCte"
            );
        } else {
            panic!("expected Status, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== RFC 7889 TOOBIG response code =====

/// `[TOOBIG]` response code parsed directly (RFC 7889 Section 4).
#[test]
fn response_code_toobig() {
    let (_, code) = response_code(b"[TOOBIG]").unwrap();
    assert_eq!(code, ResponseCode::TooBig);
}

/// Full tagged NO response with `[TOOBIG]` (RFC 7889 Section 4).
#[test]
fn response_code_toobig_full_response() {
    let input = b"A001 NO [TOOBIG] Message too large\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.code, Some(ResponseCode::TooBig));
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

// ===== RFC 4978 COMPRESSIONACTIVE response code =====

/// `[COMPRESSIONACTIVE]` response code (RFC 4978 Section 3).
/// Sent when the server rejects COMPRESS because compression is already active.
#[test]
fn response_code_compressionactive() {
    let input = b"A001 NO [COMPRESSIONACTIVE] DEFLATE active via TLS\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(
            t.code,
            Some(ResponseCode::CompressionActive),
            "COMPRESSIONACTIVE must be parsed as a typed variant, \
                 not Other (RFC 4978 Section 3)"
        );
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

// ===== RFC 6154 USEATTR response code =====

/// `[USEATTR]` response code (RFC 6154 Section 6).
/// Sent when a CREATE with special-use attributes fails because the
/// server does not support the requested attribute.
#[test]
fn spec_audit_useattr_response_code() {
    let input = b"* NO [USEATTR] \\All not supported\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Status { code, .. } = *boxed {
            assert_eq!(
                code,
                Some(ResponseCode::UseAttr),
                "USEATTR must be parsed as a typed variant, \
                     not Other (RFC 6154 Section 6)"
            );
        } else {
            panic!("expected Status, got {boxed:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

// ===== L40/L17: UTF8=ONLY capability =====

/// `UTF8=ONLY` capability (RFC 6855 Section 4).
#[test]
fn capability_utf8_only() {
    let input = b"* CAPABILITY IMAP4rev1 UTF8=ONLY\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Capability(caps) = *boxed {
            assert!(caps.contains(&Capability::Imap4Rev1));
            assert!(caps.contains(&Capability::Utf8Only));
        } else {
            panic!("expected Capability, got {boxed:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

// ===== Audit finding #7: METADATA response codes not parsed =====

/// RFC 5464 Section 4.2.1: the server may include `[METADATA LONGENTRIES n]`
/// in a tagged OK response to indicate truncated values. This response code
/// is currently flattened into generic tagged status handling.
///
/// This test documents that METADATA-specific response codes are not parsed
/// into distinct `ResponseCode` variants.
/// `[METADATA LONGENTRIES n]` response code (RFC 5464 Section 4.2.1).
#[test]
fn audit_finding7_metadata_longentries_response_code() {
    let input = b"A001 OK [METADATA LONGENTRIES 2048] completed\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::Ok);
        assert_eq!(
            tagged.code,
            Some(ResponseCode::MetadataLongEntries(2048)),
            "METADATA LONGENTRIES should be parsed as a distinct variant"
        );
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

/// `[METADATA MAXSIZE n]` response code (RFC 5464 Section 4.3).
#[test]
fn audit_finding7_metadata_maxsize_response_code() {
    let input = b"A001 NO [METADATA MAXSIZE 1024] value too large\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.code, Some(ResponseCode::MetadataMaxSize(1024)));
    } else {
        panic!("expected Tagged");
    }
}

/// `[METADATA TOOMANY]` response code (RFC 5464 Section 4.3).
#[test]
fn audit_finding7_metadata_toomany_response_code() {
    let input = b"A001 NO [METADATA TOOMANY] too many annotations\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.code, Some(ResponseCode::MetadataTooMany));
    } else {
        panic!("expected Tagged");
    }
}

/// `[METADATA NOPRIVATE]` response code (RFC 5464 Section 4.3).
#[test]
fn audit_finding7_metadata_noprivate_response_code() {
    let input = b"A001 NO [METADATA NOPRIVATE] private annotations not supported\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.code, Some(ResponseCode::MetadataNoPrivate));
    } else {
        panic!("expected Tagged");
    }
}

/// M11: `METADATA-SERVER` should be a dedicated Capability variant.
/// RFC 5464 Section 1.
#[test]
fn audit_m11_metadata_server_capability() {
    let (_, cap) = capability(b"METADATA-SERVER").unwrap();
    assert!(matches!(cap, Capability::MetadataServer));
}

/// L11: STARTTLS should be a dedicated Capability variant.
#[test]
fn audit_l11_starttls_capability_variant() {
    let (_, cap) = capability(b"STARTTLS").unwrap();
    assert!(matches!(cap, Capability::StartTls));
}

/// L11: LOGINDISABLED should be a dedicated Capability variant.
#[test]
fn audit_l11_logindisabled_capability_variant() {
    let (_, cap) = capability(b"LOGINDISABLED").unwrap();
    assert!(matches!(cap, Capability::LoginDisabled));
}

/// L17: `SORT=DISPLAY` should be recognized as a Sort variant.
/// RFC 5256 Section 2.
#[test]
fn audit_l17_sort_display_capability() {
    let (_, cap) = capability(b"SORT=DISPLAY").unwrap();
    assert!(
        !matches!(cap, Capability::Other(_)),
        "SORT=DISPLAY should not be Other: {cap:?}"
    );
}

/// `* FLAGS (...)` uses `flag-list` which contains `flag` (not `flag-perm`).
/// `\*` is not a valid `flag` (since `*` is not an ATOM-CHAR) and must be
/// excluded from the FLAGS untagged response per RFC 3501 Section 9.
#[test]
fn untagged_flags_must_exclude_wildcard() {
    let input = b"* FLAGS (\\Seen \\Answered \\*)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Flags(flags) = *boxed {
            assert!(
                !flags.contains(&Flag::Wildcard),
                "* FLAGS must not accept \\*  -  \\* is only valid in \
                     flag-perm (PERMANENTFLAGS), not flag (RFC 3501 Section 9). \
                     Got: {flags:?}"
            );
        } else {
            panic!("expected Flags, got {boxed:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

/// PERMANENTFLAGS MUST still accept `\*`  -  it uses `flag-perm = flag / "\*"`
/// (RFC 3501 Section 7.1).
#[test]
fn permanentflags_must_accept_wildcard() {
    let input = b"[PERMANENTFLAGS (\\Seen \\Flagged \\*)]";
    let (_, code) = response_code(input).unwrap();
    if let ResponseCode::PermanentFlags(flags) = &code {
        assert!(
            flags.contains(&Flag::Wildcard),
            "PERMANENTFLAGS must accept \\* per RFC 3501 Section 7.1 \
                 (flag-perm = flag / \"\\*\"). Got: {flags:?}"
        );
    } else {
        panic!("expected PermanentFlags, got {code:?}");
    }
}

/// Although RFC 3501 Section 9 `flag-list` uses the `flag` production
/// which formally excludes `\Recent`, many servers include it in
/// `* FLAGS (...)` responses. Per Postel's law, the parser retains
/// `\Recent` to preserve server flag information.
#[test]
fn spec_audit_flags_response_retains_recent() {
    let input = b"* FLAGS (\\Seen \\Answered \\Recent \\Flagged)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Flags(flags) = *boxed
    {
        assert!(
            flags.contains(&Flag::Recent),
            "\\Recent must be retained in FLAGS for Postel's law compatibility: {flags:?}"
        );
        assert!(flags.contains(&Flag::Seen));
        assert!(flags.contains(&Flag::Answered));
        assert!(flags.contains(&Flag::Flagged));
        return;
    }
    panic!("expected FLAGS response");
}

// ===== WITHIN capability (RFC 5032) =====

#[test]
fn capability_within_parsed() {
    // RFC 5032 Section 3: WITHIN capability enables OLDER/YOUNGER search keys.
    let (_, resp) = parse_response(b"* OK [CAPABILITY IMAP4rev1 WITHIN] Ready\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Status {
            code: Some(ResponseCode::Capability(caps)),
            ..
        } = *boxed
    {
        assert!(
            caps.contains(&Capability::Within),
            "WITHIN capability must be parsed"
        );
        return;
    }
    panic!("expected OK with CAPABILITY");
}

// ── 4. Capability edge cases (L703, L708) ──

/// RFC 7889 Section 2: `APPENDLIMIT` with no `=value` means the server-wide
/// limit must be checked per-mailbox.
#[test]
fn capability_appendlimit_no_value() {
    let (_, cap) = capability(b"APPENDLIMIT").unwrap();
    assert_eq!(cap, Capability::AppendLimit(None));
}

/// RFC 7889 Section 2: `APPENDLIMIT=N` provides a specific byte limit.
#[test]
fn capability_appendlimit_with_value() {
    let (_, cap) = capability(b"APPENDLIMIT=1048576").unwrap();
    assert_eq!(cap, Capability::AppendLimit(Some(1_048_576)));
}

/// RFC 7889: `APPENDLIMIT=` with non-numeric value falls back to `Other`.
#[test]
fn capability_appendlimit_non_numeric_falls_to_other() {
    let (_, cap) = capability(b"APPENDLIMIT=abc").unwrap();
    assert_eq!(cap, Capability::from_imap_str("APPENDLIMIT=abc"));
}

/// RFC 7889 Section 5: only bare `APPENDLIMIT` or `APPENDLIMIT=<number>`
/// are valid capability forms. A token that merely starts with the
/// keyword must be preserved as an unknown capability, not normalized to
/// bare `APPENDLIMIT`.
#[test]
fn capability_appendlimit_prefix_without_separator_falls_to_other() {
    let (_, cap) = capability(b"APPENDLIMITXYZ").unwrap();
    assert_eq!(cap, Capability::from_imap_str("APPENDLIMITXYZ"));
}

/// RFC 7889 Section 5 imports RFC 3501's `number` production
/// (`1*DIGIT`), so `APPENDLIMIT=+123` is malformed and must stay an
/// unknown capability instead of being normalized into a byte limit.
#[test]
fn capability_appendlimit_leading_plus_falls_to_other() {
    let (_, cap) = capability(b"APPENDLIMIT=+123").unwrap();
    assert_eq!(cap, Capability::from_imap_str("APPENDLIMIT=+123"));
}

/// Unknown capability string falls through to `Capability::Other`.
#[test]
fn capability_other_unknown_string() {
    let (_, cap) = capability(b"X-CUSTOM-CAP").unwrap();
    assert_eq!(cap, Capability::from_imap_str("X-CUSTOM-CAP"));
}

/// Unknown capability with mixed case is preserved in `Other`.
#[test]
fn capability_other_preserves_original_case() {
    let (_, cap) = capability(b"XMixedCase").unwrap();
    assert_eq!(cap, Capability::from_imap_str("XMixedCase"));
}

// ── Additional edge cases for completeness ──

/// RFC 3501 Section 9: `APPENDLIMIT` in a full capability response.
#[test]
fn capability_appendlimit_in_full_response() {
    let input = b"* CAPABILITY IMAP4rev1 APPENDLIMIT\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = *boxed
    {
        assert!(
            caps.contains(&Capability::AppendLimit(None)),
            "Bare APPENDLIMIT must parse as AppendLimit(None): {caps:?}"
        );
        return;
    }
    panic!("expected Capability response");
}

/// RFC 7889 Section 2: `APPENDLIMIT=N` in a full capability response.
#[test]
fn capability_appendlimit_with_value_in_full_response() {
    let input = b"* CAPABILITY IMAP4rev1 APPENDLIMIT=1048576\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = *boxed
    {
        assert!(
            caps.contains(&Capability::AppendLimit(Some(1_048_576))),
            "APPENDLIMIT=1048576 must parse as AppendLimit(Some(1048576)): {caps:?}"
        );
        return;
    }
    panic!("expected Capability response");
}

// ── Postel's law: multiple spaces between list items (RFC 1122 Section1.2.2) ──

/// Some non-conformant servers insert extra whitespace between flags.
/// `parse_flag_list` must tolerate multiple spaces per Postel's law
/// (RFC 1122 Section 1.2.2).
#[test]
fn flag_list_tolerates_multiple_spaces() {
    let (_, flags) = flag_list(b"(\\Seen  \\Flagged  \\Answered)").unwrap();
    assert_eq!(
        flags.len(),
        3,
        "double spaces between flags must be tolerated"
    );
    assert_eq!(flags[0], Flag::Seen);
    assert_eq!(flags[1], Flag::Flagged);
    assert_eq!(flags[2], Flag::Answered);
}

/// `capability_list` must tolerate multiple spaces between capabilities
/// per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn capability_list_tolerates_multiple_spaces() {
    let (_, resp) = parse_response(b"* CAPABILITY IMAP4rev1  IDLE  LITERAL+\r\n").unwrap();
    if let Response::Untagged(u) = resp
        && let UntaggedResponse::Capability(caps) = &*u
    {
        assert!(
            caps.contains(&Capability::Imap4Rev1),
            "IMAP4rev1 missing after double spaces"
        );
        assert!(
            caps.contains(&Capability::Idle),
            "IDLE missing after double spaces"
        );
        assert!(
            caps.contains(&Capability::LiteralPlus),
            "LITERAL+ missing after double spaces"
        );
        return;
    }
    panic!("expected Untagged Capability response");
}

/// Postel tolerance: some non-conformant servers use HTAB between
/// CAPABILITY atoms. The parser should still recover the capability list.
#[test]
fn capability_list_tolerates_tabs() {
    let (_, resp) = parse_response(b"* CAPABILITY IMAP4rev1\tIDLE\tLITERAL+\r\n").unwrap();
    if let Response::Untagged(u) = resp
        && let UntaggedResponse::Capability(caps) = &*u
    {
        assert!(caps.contains(&Capability::Imap4Rev1));
        assert!(caps.contains(&Capability::Idle));
        assert!(caps.contains(&Capability::LiteralPlus));
        return;
    }
    panic!("expected Untagged Capability response");
}

// ===== NOTIFY (RFC 5465) decoder tests =====

/// `Capability::Notify` is parsed from the wire representation.
#[test]
fn capability_notify_parsed() {
    let input = b"* CAPABILITY IMAP4rev1 NOTIFY\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Capability(caps) = *u {
            assert!(
                caps.contains(&Capability::Notify),
                "should parse NOTIFY capability, got: {caps:?}"
            );
        } else {
            panic!("expected Capability, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// `Capability::Notify` round-trips through `from_imap_str`/`as_imap_str`.
#[test]
fn capability_notify_round_trip() {
    let cap = Capability::from_imap_str("NOTIFY");
    assert_eq!(cap, Capability::Notify);
    assert_eq!(cap.as_imap_str(), "NOTIFY");
}

/// `Capability::Notify` is case-insensitive.
#[test]
fn capability_notify_case_insensitive() {
    assert_eq!(Capability::from_imap_str("notify"), Capability::Notify);
    assert_eq!(Capability::from_imap_str("Notify"), Capability::Notify);
}

/// `Capability::Notify` equals `Other("NOTIFY")` for cross-representation.
#[test]
fn capability_notify_cross_representation() {
    assert_eq!(Capability::Notify, Capability::from_imap_str("NOTIFY"));
    assert_eq!(Capability::Notify, Capability::from_imap_str("notify"));
}

/// `NOTIFICATIONOVERFLOW` response code is parsed (RFC 5465 Section 5.8).
#[test]
fn response_code_notification_overflow() {
    let input = b"* OK [NOTIFICATIONOVERFLOW] server cannot keep up\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { code, .. } = *u {
            assert!(
                matches!(code, Some(ResponseCode::NotificationOverflow(_))),
                "should parse NOTIFICATIONOVERFLOW, got: {code:?}"
            );
        } else {
            panic!("expected Status, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// `BADEVENT` response code is parsed (RFC 5465 Section 5).
#[test]
fn response_code_badevent() {
    let input = b"A001 NO [BADEVENT (AnnotationChange)] unsupported events\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::No);
        assert!(
            matches!(tagged.code, Some(ResponseCode::BadEvent(_))),
            "should parse BADEVENT, got: {:?}",
            tagged.code
        );
    } else {
        panic!("expected Tagged, got: {resp:?}");
    }
}

/// `BADEVENT` with multiple events in parenthesized list (RFC 5465 Section 5).
#[test]
fn response_code_badevent_multiple_events() {
    let input =
        b"A001 NO [BADEVENT (AnnotationChange FlagChange ServerMetadataChange)] unsupported\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::No);
        if let Some(ResponseCode::BadEvent(Some(ref events))) = tagged.code {
            // The trailing data should contain the event names.
            assert!(
                events.contains("AnnotationChange"),
                "should contain AnnotationChange, got: {events}"
            );
            assert!(
                events.contains("FlagChange"),
                "should contain FlagChange, got: {events}"
            );
            assert!(
                events.contains("ServerMetadataChange"),
                "should contain ServerMetadataChange, got: {events}"
            );
        } else {
            panic!("expected BadEvent with event list, got: {:?}", tagged.code);
        }
    } else {
        panic!("expected Tagged, got: {resp:?}");
    }
}

/// `BADEVENT` with empty parentheses (RFC 5465 Section 5).
#[test]
fn response_code_badevent_empty_parens() {
    let input = b"A001 NO [BADEVENT ()] no events supported\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::No);
        assert!(
            matches!(tagged.code, Some(ResponseCode::BadEvent(_))),
            "should parse BADEVENT with empty parens, got: {:?}",
            tagged.code
        );
    } else {
        panic!("expected Tagged, got: {resp:?}");
    }
}

/// The overflow recovery above must look only at the code's own value. A
/// number in the surrounding status text (or in a response coalesced into the
/// same buffer) is not evidence that the code overflowed, so a genuinely
/// malformed code stays a parse error and keeps falling back to display text.
#[test]
fn response_code_overflow_recovery_stops_at_the_closing_bracket() {
    // APPENDUID with no uid-set is malformed, not overflowing; the 4294967296
    // lives in the text after `]`.
    let input = b"* OK [APPENDUID 1234 ] saved as 4294967296\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { code, text, .. } = *u {
            assert_eq!(
                code, None,
                "a malformed APPENDUID must not be laundered into an opaque code"
            );
            assert_eq!(text, "[APPENDUID 1234 ] saved as 4294967296");
        } else {
            panic!("expected Status, got: {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// An overflowing numeric response code remains structured as an opaque code
/// rather than being demoted into the human-readable status text.
#[test]
fn response_code_uidnext_overflow_preserves_the_code() {
    let input = b"* OK [UIDNEXT 4294967296] Predicted next UID\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { code, text, .. } = *u {
            assert_eq!(
                code,
                Some(ResponseCode::Other {
                    name: "UIDNEXT".into(),
                    value: Some("4294967296".into()),
                })
            );
            assert_eq!(text, "Predicted next UID");
        } else {
            panic!("expected Status, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

// ===================================================================
// Double-space tolerance in separated lists (Postel's law)
// ===================================================================
//
// RFC 3501 Section 9 uses SP (single space) between list items, but
// non-conformant servers may insert extra whitespace. Parsers must
// tolerate this per Postel's law (RFC 1122 Section 1.2.2).

/// BADCHARSET response code with double spaces between charsets.
///
/// Regression test: `separated_list0(sp, ...)` with a single-space
/// separator would fail because the second space is not a valid
/// astring start, causing the parser to stop after the first charset
/// and then fail on the closing `)`.
///
/// RFC 3501 Section 9 / Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn badcharset_double_space_between_charsets() {
    // Two spaces between "UTF-8" and "US-ASCII"
    let (_, code) = response_code(b"[BADCHARSET (\"UTF-8\"  \"US-ASCII\")]").unwrap();
    if let ResponseCode::BadCharset(charsets) = code {
        assert_eq!(
            charsets,
            vec!["UTF-8", "US-ASCII"],
            "double space between BADCHARSET charsets must be tolerated"
        );
    } else {
        panic!("expected BadCharset, got {code:?}");
    }
}

/// RFC 9208 Section 3: STATUS APPENDLIMIT NIL (no limit).
#[test]
fn status_appendlimit_nil() {
    let (_, items) = status_items(b"APPENDLIMIT NIL").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::AppendLimit(None));
}

/// RFC 9208 Section 3: STATUS APPENDLIMIT value (distinct from capability).
#[test]
fn status_appendlimit_with_value() {
    let (_, items) = status_items(b"APPENDLIMIT 104857600").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::AppendLimit(Some(104_857_600)));
}

/// APPENDLIMIT values exceeding `u32::MAX` must be parsed as
/// `Capability::AppendLimit(Some(n))`, not degraded to `Other`.
/// RFC 7889 Section 5: `capability =/ "APPENDLIMIT" ["=" number]`.
#[test]
fn regression_appendlimit_large_value_parsed_as_u64() {
    let input = b"* CAPABILITY IMAP4rev1 APPENDLIMIT=5000000000\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = &*boxed
    {
        assert!(
            caps.contains(&Capability::AppendLimit(Some(5_000_000_000))),
            "APPENDLIMIT=5000000000 must be parsed as AppendLimit(Some(5000000000)), \
                 got: {caps:?}"
        );
        return;
    }
    panic!("expected Capabilities untagged response");
}

/// APPENDLIMIT capability with a value that overflows u32.
///
/// Per RFC 7889 Section 2, the APPENDLIMIT value is a number, but when a server
/// sends a value exceeding `u32::MAX` it should not silently become
/// `AppendLimit(None)` (which means "no global limit"). Instead it should be
/// preserved as `Capability::Other(...)`.
#[test]
fn appendlimit_large_value_parsed_as_u64() {
    let input = b"* CAPABILITY IMAP4rev1 APPENDLIMIT=99999999999\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Capability(caps) = &*boxed
    {
        // Now that the type is u64, this should parse as AppendLimit(Some(99999999999)).
        assert!(
            caps.contains(&Capability::AppendLimit(Some(99_999_999_999))),
            "large value must be parsed as AppendLimit(Some(99999999999)), got: {caps:?}"
        );
        return;
    }
    panic!("expected Capabilities untagged response");
}

#[test]
fn seq_range_keeps_numeric_max_distinct_from_star() {
    let (_, numeric) = seq_range(b"4294967295").unwrap();
    let (_, star) = seq_range(b"*").unwrap();

    assert_eq!(
        numeric.start_bound(),
        crate::types::SeqBound::Number(u32::MAX)
    );
    assert_eq!(star.start_bound(), crate::types::SeqBound::Star);
}

/// RFC 3501 Section 9: `seq-range = seq-number ":" seq-number` where
/// `seq-number` may be `*`. When `*` appears as the first element (`*:1`),
/// reversed-range normalization must put it after the concrete lower bound.
#[test]
fn seq_range_wildcard_reversed_normalized() {
    let (_, r) = seq_range(b"*:1").unwrap();
    assert_eq!(r.start(), 1, "seq_range *:1 must normalize start to 1");
    assert_eq!(
        r.end_bound(),
        Some(crate::types::SeqBound::Star),
        "seq_range *:1 must preserve the wildcard as a typed bound"
    );
}

/// Postel's law: `seq_range` must also normalize reversed ranges,
/// consistent with `uid_range`. ESEARCH ALL (RFC 4731 Section 3.1)
/// and VANISHED (RFC 7162 Section 3.2.10) use `sequence_set`, so
/// reversed ranges from non-conformant servers must be handled.
#[test]
fn seq_range_reversed_normalized() {
    let (_, r) = seq_range(b"100:1").unwrap();
    assert_eq!(
        r.start(),
        1,
        "seq_range reversed range must normalize start to min"
    );
    assert_eq!(
        r.end(),
        Some(100),
        "seq_range reversed range must normalize end to max"
    );
}

/// RFC 3501 Section 9: BADCHARSET with empty parens `()` formally requires
/// ≥1 charset, but is accepted per Postel's law (RFC 1122 Section 1.2.2).
/// An empty charset list is semantically equivalent to no charsets.
#[test]
fn spec_audit_badcharset_empty_parens_accepted() {
    let (_, code) = response_code(b"[BADCHARSET ()]").unwrap();
    if let ResponseCode::BadCharset(charsets) = code {
        assert!(
            charsets.is_empty(),
            "BADCHARSET () should produce empty charset vec"
        );
    } else {
        panic!("expected BadCharset, got {code:?}");
    }
}

/// RFC 7162 Section 3.1.2: some servers (Cyrus, Dovecot) send
/// `[HIGHESTMODSEQ 0]` for empty/new mailboxes or those without CONDSTORE.
/// While strictly non-conformant (mod-sequence-value is >= 1), this must
/// parse as `ResponseCode::HighestModSeq(0)` per Postel's law rather than
/// silently falling into the human-readable text field.
#[test]
fn response_code_highestmodseq_zero_accepted() {
    let (_, code) = response_code(b"[HIGHESTMODSEQ 0]").unwrap();
    assert_eq!(
        code,
        ResponseCode::HighestModSeq(0),
        "HIGHESTMODSEQ 0 must parse as HighestModSeq(0) per Postel's law \
             (RFC 7162 Section 3.1.2  -  real servers send this for empty mailboxes)"
    );
}

/// HIGHESTMODSEQ response code with `i64::MAX` value (RFC 9051 number64 limit).
#[test]
fn response_code_highestmodseq_i64_max() {
    let (_, code) = response_code(b"[HIGHESTMODSEQ 9223372036854775807]").unwrap();
    assert_eq!(code, ResponseCode::HighestModSeq(i64::MAX as u64));
}

#[test]
fn response_code_nomodseq() {
    let (_, code) = response_code(b"[NOMODSEQ]").unwrap();
    assert_eq!(code, ResponseCode::NoModSeq);
}

#[test]
fn response_code_highestmodseq() {
    let (_, code) = response_code(b"[HIGHESTMODSEQ 99999]").unwrap();
    assert_eq!(code, ResponseCode::HighestModSeq(99999));
}
