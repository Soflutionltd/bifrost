#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::similar_names,
    clippy::unreadable_literal,
    clippy::wildcard_in_or_patterns,
    clippy::vec_init_then_push,
    clippy::match_wild_err_arm,
    clippy::same_item_push
)]

use super::*;
use crate::types::validated::MailboxName;

// ===== Primitive parser tests =====

#[test]
fn atom_valid() {
    let (rest, val) = atom(b"INBOX rest").unwrap();
    assert_eq!(val, b"INBOX");
    assert_eq!(rest, b" rest");
}

#[test]
fn atom_rejects_specials() {
    assert!(atom(b"(bad").is_err());
    assert!(atom(b")bad").is_err());
    assert!(atom(b"{bad").is_err());
    assert!(atom(b"\"bad").is_err());
}

#[test]
fn atom_empty_fails() {
    assert!(atom(b" oops").is_err());
}

#[test]
fn quoted_string_simple() {
    let (rest, val) = quoted_string(b"\"hello\" rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
}

#[test]
fn quoted_string_escapes() {
    let (_, val) = quoted_string(b"\"a\\\"b\\\\c\"").unwrap();
    assert_eq!(val, b"a\"b\\c");
}

#[test]
fn quoted_string_non_ascii() {
    let (_, val) = quoted_string(b"\"caf\xc3\xa9\"").unwrap();
    assert_eq!(val, b"caf\xc3\xa9");
}

#[test]
fn quoted_string_empty() {
    let (_, val) = quoted_string(b"\"\"").unwrap();
    assert!(val.is_empty());
}

#[test]
fn literal_simple() {
    let (rest, val) = literal(b"{5}\r\nhello rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
}

#[test]
fn literal_plus() {
    let (_, val) = literal(b"{3+}\r\nabc").unwrap();
    assert_eq!(val, b"abc");
}

#[test]
fn literal_zero_length() {
    let (_, val) = literal(b"{0}\r\n").unwrap();
    assert!(val.is_empty());
}

#[test]
fn nstring_nil() {
    let (_, val) = nstring(b"NIL").unwrap();
    assert!(val.is_none());
}

#[test]
fn nstring_nil_case_insensitive() {
    let (_, val) = nstring(b"nil").unwrap();
    assert!(val.is_none());
    let (_, val) = nstring(b"Nil").unwrap();
    assert!(val.is_none());
}

#[test]
fn nstring_string() {
    let (_, val) = nstring(b"\"hello\"").unwrap();
    assert_eq!(val.unwrap(), b"hello");
}

/// Regression: `nstring` must verify a token boundary follows "NIL"
/// so that atoms starting with "NIL" (e.g. "NILX") are not partially
/// consumed as the NIL token (RFC 3501 Section 9).
#[test]
fn nstring_nil_requires_token_boundary() {
    // "NILX" is an atom, not the NIL token followed by "X".
    // Without boundary checking, `tag_no_case(&b"NIL"[..])` greedily
    // matches "NIL" and leaves "X" as unparsed residue.
    // With the fix, the parser correctly rejects "NILX" as not
    // matching either NIL or a quoted/literal string.
    assert!(
        nstring(b"NILX").is_err(),
        "nstring must not partially match 'NIL' from 'NILX' \
             (RFC 3501 Section 9: atoms are delimited tokens)"
    );
}

/// `nstring` NIL followed by a valid delimiter must still parse.
#[test]
fn nstring_nil_with_delimiter() {
    let (rest, val) = nstring(b"NIL rest").unwrap();
    assert!(val.is_none());
    assert_eq!(rest, b" rest");

    let (rest, val) = nstring(b"NIL)").unwrap();
    assert!(val.is_none());
    assert_eq!(rest, b")");

    let (rest, val) = nstring(b"NIL\r\n").unwrap();
    assert!(val.is_none());
    assert_eq!(rest, b"\r\n");
}

/// Regression: `nil_token` must recognise all RFC 3501 Section 9 atom-specials
/// as valid token boundaries, not just SP/`)`/CR/`]`/EOF.
///
/// RFC 3501 Section 9 ABNF:
///   atom-specials = "(" / ")" / "{" / SP / CTL / list-wildcards /
///                   quoted-specials / resp-specials
///
/// `(` and `{` are atom-specials, so `NIL(` and `NIL{5}` are the NIL
/// token followed by another syntactic element.  Without these delimiters
/// in the peek set, `nil_token` rejects the input and the outer `alt`
/// (e.g., `nstring`) fails entirely.
#[test]
fn nil_token_open_paren_boundary() {
    // NIL immediately followed by `(`  -  the `(` is an atom-special
    // per RFC 3501 Section 9, so NIL is a complete token here.
    let (rest, val) = nstring(b"NIL(").unwrap();
    assert!(val.is_none(), "NIL( should parse NIL as None");
    assert_eq!(rest, b"(");
}

#[test]
fn nil_token_open_brace_boundary() {
    // NIL immediately followed by `{`  -  another atom-special.
    let (rest, val) = nstring(b"NIL{5}\r\nhello").unwrap();
    assert!(val.is_none(), "NIL{{ should parse NIL as None");
    assert_eq!(rest, b"{5}\r\nhello");
}

/// Regression: `nil_token` must recognise all CTL bytes (0x00-0x1F, 0x7F)
/// as valid token boundaries, not just CR and LF.
///
/// RFC 3501 Section 9:
///   atom-specials = "(" / ")" / "{" / SP / CTL / ...
///   CTL           = %x00-1F / %x7F
///
/// A TAB (0x09) after NIL is a CTL byte and therefore an atom-special,
/// so `NIL\t` must parse as the NIL token.
#[test]
fn nil_token_tab_boundary() {
    let (rest, val) = nstring(b"NIL\t").unwrap();
    assert!(val.is_none(), "NIL followed by TAB should parse as None");
    assert_eq!(rest, b"\t");
}

/// NUL (0x00) is a CTL byte and must terminate the NIL token.
#[test]
fn nil_token_nul_boundary() {
    let (rest, val) = nstring(b"NIL\x00").unwrap();
    assert!(val.is_none(), "NIL followed by NUL should parse as None");
    assert_eq!(rest, b"\x00");
}

/// DEL (0x7F) is a CTL byte and must terminate the NIL token.
#[test]
fn nil_token_del_boundary() {
    let (rest, val) = nstring(b"NIL\x7F").unwrap();
    assert!(val.is_none(), "NIL followed by DEL should parse as None");
    assert_eq!(rest, b"\x7F");
}

#[test]
fn number_valid() {
    let (_, val) = number(b"12345").unwrap();
    assert_eq!(val, 12345);
}

#[test]
fn number_overflow() {
    assert!(number(b"99999999999").is_err());
}

#[test]
fn number64_valid() {
    // RFC 9051 Section 4: number64 is 0..=i64::MAX (2^63-1)
    let (_, val) = number64(b"9223372036854775807").unwrap();
    assert_eq!(val, i64::MAX as u64);
}

// ===== Flag / Capability / Response code tests =====

#[test]
fn flag_system() {
    let (_, f) = flag_or_perm(b"\\Seen rest").unwrap();
    assert_eq!(f, Flag::Seen);
}

#[test]
fn flag_custom() {
    let (_, f) = flag_or_perm(b"$Important rest").unwrap();
    assert_eq!(f, Flag::Custom("$Important".into()));
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

// ── flag_fetch_list tests (lock down behaviour before refactoring) ──

/// `flag_list` (used for both FLAGS and FETCH FLAGS contexts) must filter
/// `\*`  -  RFC 3501 Section 9: `flag-fetch = flag / "\Recent"`.
#[test]
fn flag_list_in_fetch_context_filters_wildcard() {
    let (_, flags) = flag_list(b"(\\Seen \\*)").unwrap();
    assert_eq!(flags.len(), 1, "\\* must be filtered from flag_list");
    assert_eq!(flags[0], Flag::Seen);
}

/// `flag_list` must retain `\Recent`  -  it is valid in both FLAGS
/// and FETCH FLAGS contexts (RFC 3501 Section 9: `flag-fetch = flag / "\Recent"`).
#[test]
fn flag_list_in_fetch_context_retains_recent() {
    let (_, flags) = flag_list(b"(\\Seen \\Recent \\Flagged)").unwrap();
    assert_eq!(flags.len(), 3, "\\Recent must be retained in flag_list");
    assert!(
        flags.contains(&Flag::Recent),
        "\\Recent was filtered from flag_list"
    );
}

/// `flag_list` parses standard flags in FETCH FLAGS context.
#[test]
fn flag_list_in_fetch_context_basic() {
    let (_, flags) = flag_list(b"(\\Seen \\Answered \\Deleted)").unwrap();
    assert_eq!(flags.len(), 3);
    assert_eq!(flags[0], Flag::Seen);
    assert_eq!(flags[1], Flag::Answered);
    assert_eq!(flags[2], Flag::Deleted);
}

/// `flag_list` handles empty list in FETCH FLAGS context.
#[test]
fn flag_list_in_fetch_context_empty() {
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
    assert_eq!(cap, Capability::Auth("PLAIN".into()));
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
    assert_eq!(cap, Capability::Other("XYZZY".into()));
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
    if let ResponseCode::CopyUid {
        uid_validity,
        source_uids,
        dest_uids,
    } = &code
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
    if let ResponseCode::CopyUid {
        uid_validity,
        source_uids,
        dest_uids,
    } = &code
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
fn response_code_highestmodseq() {
    let (_, code) = response_code(b"[HIGHESTMODSEQ 99999]").unwrap();
    assert_eq!(code, ResponseCode::HighestModSeq(99999));
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

// ===== Top-level response parser tests =====

#[test]
fn greeting_ok() {
    let (_, resp) = parse_greeting(b"* OK Dovecot ready.\r\n").unwrap();
    if let Response::Greeting(g) = resp {
        assert_eq!(g.status, GreetingStatus::Ok);
        assert_eq!(g.text, "Dovecot ready.");
    } else {
        panic!("expected Greeting");
    }
}

#[test]
fn greeting_preauth() {
    let (_, resp) = parse_greeting(b"* PREAUTH already authed\r\n").unwrap();
    if let Response::Greeting(g) = resp {
        assert_eq!(g.status, GreetingStatus::PreAuth);
    } else {
        panic!("expected Greeting");
    }
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

/// RFC 3501 Section 7.1 / Postel's law: a greeting with no SP or text
/// after the status keyword should parse, consistent with how
/// `parse_tagged` and `parse_untagged_status` tolerate missing SP.
#[test]
fn greeting_bare_ok_no_text() {
    let input = b"* OK\r\n";
    let (_, resp) = parse_greeting(input).expect(
        "bare '* OK\\r\\n' greeting should parse (Postel's law, consistent with parse_tagged)",
    );
    match resp {
        Response::Greeting(g) => {
            assert_eq!(g.status, GreetingStatus::Ok);
            assert!(g.code.is_none());
            assert!(g.text.is_empty());
        }
        other => panic!("expected Greeting, got {other:?}"),
    }
}

/// Bare BYE greeting with no text.
#[test]
fn greeting_bare_bye_no_text() {
    let input = b"* BYE\r\n";
    let (_, resp) = parse_greeting(input).expect("bare '* BYE\\r\\n' greeting should parse");
    match resp {
        Response::Greeting(g) => {
            assert_eq!(g.status, GreetingStatus::Bye);
            assert!(g.text.is_empty());
        }
        other => panic!("expected Greeting, got {other:?}"),
    }
}

/// Greeting with response code but no space before it: `* OK[CAPABILITY ...]\r\n`
#[test]
fn greeting_ok_with_code_no_space() {
    let input = b"* OK[CAPABILITY IMAP4REV1]\r\n";
    // This should also work - the code is directly after OK without SP
    let result = parse_greeting(input);
    // This is a stretch - most servers send SP. Just verify it doesn't panic.
    // If it parses, great. If not, that's also acceptable since the RFC requires SP.
    let _ = result;
}

#[test]
fn tagged_ok() {
    let (_, resp) = parse_response(b"A001 OK done\r\n").unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.tag, "A001");
        assert_eq!(t.status, StatusKind::Ok);
        assert_eq!(t.text, "done");
    } else {
        panic!("expected Tagged");
    }
}

#[test]
fn tagged_no_with_code() {
    let (_, resp) = parse_response(b"A002 NO [NOPERM] not allowed\r\n").unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.status, StatusKind::No);
        assert_eq!(t.code, Some(ResponseCode::NoPerm));
    } else {
        panic!("expected Tagged");
    }
}

#[test]
fn tagged_bad() {
    let (_, resp) = parse_response(b"A003 BAD syntax error\r\n").unwrap();
    if let Response::Tagged(t) = resp {
        assert_eq!(t.status, StatusKind::Bad);
    } else {
        panic!("expected Tagged");
    }
}

#[test]
fn continuation() {
    let (_, resp) = parse_response(b"+ go ahead\r\n").unwrap();
    if let Response::Continuation(c) = resp {
        assert_eq!(c.data, "go ahead");
    } else {
        panic!("expected Continuation");
    }
}

#[test]
fn untagged_exists() {
    let (_, resp) = parse_response(b"* 23 EXISTS\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        assert_eq!(*u, UntaggedResponse::Exists(23));
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_recent() {
    let (_, resp) = parse_response(b"* 5 RECENT\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        assert_eq!(*u, UntaggedResponse::Recent(5));
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_expunge() {
    let (_, resp) = parse_response(b"* 3 EXPUNGE\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        assert_eq!(*u, UntaggedResponse::Expunge(3));
    } else {
        panic!("expected Untagged");
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

#[test]
fn untagged_list() {
    let (_, resp) = parse_response(b"* LIST (\\HasNoChildren) \"/\" \"INBOX\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(info.delimiter, Some('/'));
            assert!(info.attributes.contains(&MailboxAttribute::HasNoChildren));
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_list_nil_delimiter() {
    let (_, resp) = parse_response(b"* LIST () NIL \"INBOX\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.delimiter, None);
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// `\Noinferiors` attribute parsed in LIST response (RFC 3501 Section 7.2.2).
#[test]
fn untagged_list_noinferiors() {
    let (_, resp) = parse_response(b"* LIST (\\Noinferiors) \"/\" \"Leaf\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "Leaf");
            assert!(info.attributes.contains(&MailboxAttribute::NoInferiors));
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_list_special_use() {
    let (_, resp) = parse_response(b"* LIST (\\Sent \\HasNoChildren) \".\" \"Sent\"\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert!(info.attributes.contains(&MailboxAttribute::Sent));
        } else {
            panic!("expected List");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_search() {
    let (_, resp) = parse_response(b"* SEARCH 1 5 10\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = &*u {
            assert_eq!(uids, &[1, 5, 10]);
            assert!(mod_seq.is_none());
        } else {
            panic!("expected Search");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_search_empty() {
    let (_, resp) = parse_response(b"* SEARCH\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, .. } = &*u {
            assert!(uids.is_empty());
        } else {
            panic!("expected Search");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Zoho sends `* SEARCH \r\n` (trailing space, no results)
/// after UID MOVE empties a mailbox. The parser must tolerate trailing
/// whitespace between the SEARCH keyword and CRLF.
///
/// RFC 3501 Section 7.2.5 formal syntax:
///   `mailbox-data = "SEARCH" *(SP nz-number)`
/// The trailing SP without a subsequent nz-number is technically
/// non-conformant, but common enough to require tolerance (Postel's law).
#[test]
fn search_trailing_space_empty() {
    let (_, resp) = parse_response(b"* SEARCH \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = &*u {
            assert!(
                uids.is_empty(),
                "trailing space should not produce phantom results"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Regression test: UID 0 in SEARCH results must be filtered out.
///
/// RFC 3501 Section 7.2.5 / Section 9: SEARCH results are `nz-number`.
/// UIDs and sequence numbers are never zero. Some non-conformant
/// servers include 0 in results; the parser accepts it during parsing
/// (to avoid `many0` silently dropping subsequent numbers) but must
/// discard it before returning results to the caller.
///
/// # References
/// - RFC 3501 Section 9 (`nz-number = digit-nz *DIGIT`)
/// - RFC 3501 Section 7.2.5 (`mailbox-data = "SEARCH" *(SP nz-number)`)
#[test]
fn search_filters_uid_zero() {
    let (_, resp) = parse_response(b"* SEARCH 5 0 10\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, .. } = &*u {
            assert_eq!(
                *uids,
                vec![5, 10],
                "UID 0 must be filtered from SEARCH results"
            );
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Regression test: UID 0 in SORT results must also be filtered out.
///
/// # References
/// - RFC 5256 Section 4 (SORT response uses same format as SEARCH)
#[test]
fn sort_filters_uid_zero() {
    let (_, resp) = parse_response(b"* SORT 3 0 7\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, .. } = &*u {
            assert_eq!(
                *nums,
                vec![3, 7],
                "UID 0 must be filtered from SORT results"
            );
        } else {
            panic!("expected Sort, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// multiple trailing spaces after SEARCH with results.
/// Ensures the trailing-space fix doesn't break normal SEARCH responses.
#[test]
fn search_trailing_spaces_with_results() {
    let (_, resp) = parse_response(b"* SEARCH 5 10 15  \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, .. } = &*u {
            assert_eq!(*uids, vec![5, 10, 15]);
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// trailing space in a full response with tag.
/// Simulates the exact Zoho wire sequence: `* SEARCH \r\nA001 OK Success\r\n`
#[test]
fn search_trailing_space_followed_by_tagged() {
    // Parse just the untagged part
    let (rest, resp) = parse_response(b"* SEARCH \r\nA001 OK Success\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, .. } = &*u {
            assert!(uids.is_empty());
        } else {
            panic!("expected Search");
        }
    } else {
        panic!("expected Untagged");
    }
    // The tagged response should remain for subsequent parsing
    assert_eq!(rest, b"A001 OK Success\r\n");
}

/// SORT has the same `*(SP nz-number)` pattern as SEARCH.
/// A trailing space in `* SORT \r\n` must be tolerated.
///
/// RFC 5256 Section 4: `sort-data = "SORT" *(SP nz-number)`
#[test]
fn sort_trailing_space_empty() {
    let (_, resp) = parse_response(b"* SORT \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert!(
                nums.is_empty(),
                "trailing space should not produce phantom results"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// SORT with results and trailing space.
#[test]
fn sort_trailing_space_with_results() {
    let (_, resp) = parse_response(b"* SORT 3 1 2 \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(*nums, vec![3, 1, 2]);
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort, got {u:?}");
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

/// LIST with trailing space after mailbox name.
///
/// RFC 3501 Section 7.2.2: `"LIST" SP mailbox-list`
#[test]
fn list_trailing_space() {
    let (_, resp) = parse_response(b"* LIST (\\HasNoChildren) \"/\" INBOX \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LSUB with trailing space after mailbox name.
///
/// RFC 3501 Section 7.2.3: `"LSUB" SP mailbox-list`
#[test]
fn lsub_trailing_space() {
    let (_, resp) = parse_response(b"* LSUB () \"/\" INBOX \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// VANISHED with trailing space after uid-set.
///
/// RFC 7162 Section 3.2.10: `"VANISHED" [SP "(EARLIER)"] SP known-uids`
#[test]
fn vanished_trailing_space() {
    let (_, resp) = parse_response(b"* VANISHED 1:5 \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Vanished { earlier, uids } = &*u {
            assert!(!earlier);
            assert!(!uids.is_empty());
        } else {
            panic!("expected Vanished, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_with_all() {
    let input = b"* ESEARCH (TAG \"A001\") UID ALL 1:3,5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A001"));
            assert!(esearch.uid);
            assert_eq!(
                esearch.all,
                vec![UidRange::range(1, 3), UidRange::single(5)]
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_with_min_max_count() {
    let input = b"* ESEARCH (TAG \"A002\") UID MIN 1 MAX 100 COUNT 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A002"));
            assert!(esearch.uid);
            assert_eq!(esearch.min, Some(1));
            assert_eq!(esearch.max, Some(100));
            assert_eq!(esearch.count, Some(5));
            assert!(esearch.all.is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_empty() {
    let input = b"* ESEARCH (TAG \"A003\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A003"));
            assert!(!esearch.uid);
            assert!(esearch.all.is_empty());
            assert_eq!(esearch.min, None);
            assert_eq!(esearch.max, None);
            assert_eq!(esearch.count, None);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_all_fields_together() {
    let input = b"* ESEARCH (TAG \"A004\") UID MIN 2 MAX 50 COUNT 10 ALL 2:5,10,20:50\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A004"));
            assert!(esearch.uid);
            assert_eq!(esearch.min, Some(2));
            assert_eq!(esearch.max, Some(50));
            assert_eq!(esearch.count, Some(10));
            assert_eq!(
                esearch.all,
                vec![
                    UidRange::range(2, 5),
                    UidRange::single(10),
                    UidRange::range(20, 50),
                ]
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_no_uid_indicator() {
    // ESEARCH without UID indicator  -  sequence numbers, not UIDs.
    let input = b"* ESEARCH (TAG \"A005\") COUNT 3 ALL 1,5,10\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A005"));
            assert!(!esearch.uid);
            assert_eq!(esearch.count, Some(3));
            assert_eq!(
                esearch.all,
                vec![
                    UidRange::single(1),
                    UidRange::single(5),
                    UidRange::single(10),
                ]
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_no_tag() {
    // Some servers may omit the TAG correlator.
    let input = b"* ESEARCH UID COUNT 0\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert!(esearch.tag.is_none());
            assert!(esearch.uid);
            assert_eq!(esearch.count, Some(0));
            assert!(esearch.all.is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_only_min() {
    let input = b"* ESEARCH (TAG \"A1\") UID MIN 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min, Some(5));
            assert_eq!(esearch.max, None);
            assert_eq!(esearch.count, None);
            assert!(esearch.all.is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_only_max() {
    let input = b"* ESEARCH (TAG \"A1\") UID MAX 99\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min, None);
            assert_eq!(esearch.max, Some(99));
            assert_eq!(esearch.count, None);
            assert!(esearch.all.is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_only_count() {
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 0\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min, None);
            assert_eq!(esearch.max, None);
            assert_eq!(esearch.count, Some(0));
            assert!(esearch.all.is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_all_single_uid() {
    let input = b"* ESEARCH (TAG \"A1\") UID ALL 42\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.all, vec![UidRange::single(42)]);
            assert_eq!(esearch.min, None);
            assert_eq!(esearch.max, None);
            assert_eq!(esearch.count, None);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_all_and_count_no_minmax() {
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 3 ALL 1,5,10\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.count, Some(3));
            assert_eq!(
                esearch.all,
                vec![
                    UidRange::single(1),
                    UidRange::single(5),
                    UidRange::single(10),
                ]
            );
            assert_eq!(esearch.min, None);
            assert_eq!(esearch.max, None);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_keywords_case_insensitive() {
    // Result-data keywords are parsed via to_ascii_uppercase(); verify
    // lowercase and mixed-case variants work.
    let input = b"* ESEARCH (TAG \"A1\") uid min 1 Max 100 count 5 all 1:3\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert!(esearch.uid);
            assert_eq!(esearch.min, Some(1));
            assert_eq!(esearch.max, Some(100));
            assert_eq!(esearch.count, Some(5));
            assert_eq!(esearch.all, vec![UidRange::range(1, 3)]);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_status_ok_with_code() {
    let (_, resp) = parse_response(b"* OK [UIDVALIDITY 3857529045] UIDs valid\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { status, code, text } = &*u {
            assert_eq!(*status, UntaggedStatus::Ok);
            assert_eq!(
                code.as_ref().unwrap(),
                &ResponseCode::UidValidity(3_857_529_045)
            );
            assert_eq!(text, "UIDs valid");
        } else {
            panic!("expected Status");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_bye() {
    let (_, resp) = parse_response(b"* BYE server shutting down\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { status, .. } = &*u {
            assert_eq!(*status, UntaggedStatus::Bye);
        } else {
            panic!("expected Status");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== RFC 2047 tests =====

#[test]
fn rfc2047_base64() {
    let result = decode_rfc2047(b"=?UTF-8?B?SGVsbG8gV29ybGQ=?=");
    assert_eq!(result, "Hello World");
}

#[test]
fn rfc2047_quoted_printable() {
    let result = decode_rfc2047(b"=?UTF-8?Q?Hello_World?=");
    assert_eq!(result, "Hello World");
}

#[test]
fn rfc2047_iso8859() {
    // "café" in ISO-8859-1 Q-encoding
    let result = decode_rfc2047(b"=?ISO-8859-1?Q?caf=E9?=");
    assert_eq!(result, "caf\u{e9}");
}

#[test]
fn rfc2047_consecutive() {
    // Whitespace between consecutive encoded words should be removed
    let result = decode_rfc2047(b"=?UTF-8?Q?Hello?= =?UTF-8?Q?_World?=");
    assert_eq!(result, "Hello World");
}

#[test]
fn rfc2047_mixed() {
    let result = decode_rfc2047(b"Re: =?UTF-8?B?SGVsbG8=?= there");
    assert_eq!(result, "Re: Hello there");
}

#[test]
fn rfc2047_plain_text() {
    let result = decode_rfc2047(b"no encoding here");
    assert_eq!(result, "no encoding here");
}

#[test]
fn rfc2047_garbage() {
    // Malformed  -  should pass through literally
    let result = decode_rfc2047(b"=?broken");
    assert_eq!(result, "=?broken");
}

/// RFC 2231 Section 5: encoded-word charset may include `*language` suffix.
#[test]
fn spec_audit_rfc2047_with_rfc2231_language_tag() {
    // =?charset*language?encoding?text?= per RFC 2231 Section 5
    let input = b"=?UTF-8*EN?Q?Hello_World?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Hello World",
        "RFC 2231 language tag in charset must be stripped"
    );
}

/// RFC 2231 Section 5: language tag with non-UTF-8 charset.
#[test]
fn spec_audit_rfc2047_with_rfc2231_language_tag_iso8859() {
    // ISO-8859-1 with language tag
    let input = b"=?ISO-8859-1*DE?Q?Gr=FC=DFe?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Grüße",
        "RFC 2231 language tag must work with non-UTF-8 charsets"
    );
}

/// RFC 2231 Section 5: language tag with Base64 encoding.
#[test]
fn spec_audit_rfc2047_with_rfc2231_language_tag_b_encoding() {
    // UTF-8*EN with B encoding
    let input = b"=?UTF-8*EN?B?SGVsbG8=?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Hello",
        "RFC 2231 language tag must work with B encoding"
    );
}

/// `decode_with_bom_removal` incorrectly strips a leading BOM
/// from RFC 2047 encoded-word payloads. Encoded words are header text
/// fragments (RFC 2047 Section 2), not standalone documents, so a leading
/// U+FEFF should be preserved as content (ZERO WIDTH NO-BREAK SPACE).
///
/// Test vector: UTF-16BE payload [0xFE, 0xFF, 0x00, 0x68, 0x00, 0x65,
/// 0x00, 0x6C, 0x00, 0x6C, 0x00, 0x6F] which is BOM + "hello".
/// `decode_with_bom_removal` strips the BOM producing "hello";
/// `decode_without_bom_handling` preserves it as "\u{FEFF}hello".
#[test]
fn regression_rfc2047_bom_not_stripped() {
    // Base64 of [0xFE, 0xFF, 0x00, 0x68, 0x00, 0x65, 0x00, 0x6C, 0x00, 0x6C, 0x00, 0x6F]
    let input = b"=?UTF-16BE?B?/v8AaABlAGwAbABv?=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "\u{FEFF}hello",
        "RFC 2047 Section 2: encoded words are header fragments, not standalone \
             documents  -  a leading U+FEFF must be preserved as content, not stripped as BOM"
    );
}

#[test]
fn spec_audit_rfc2047_unknown_encoding_preserved_verbatim() {
    // RFC 2047 Section 6.3: If an encoded word is not recognized,
    // it MUST be displayed as-is.
    // Bug: parse_encoded_word advanced the remaining pointer before
    // returning None on unknown encoding 'X', dropping characters.
    let input = b"=?UTF-8?X?hello?= world";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "=?UTF-8?X?hello?= world",
        "Unknown encoding 'X' must preserve entire encoded word verbatim (RFC 2047 Section 6.3)"
    );
}

#[test]
fn spec_audit_rfc2047_invalid_base64_preserved_verbatim() {
    // RFC 2047 Section 6.3: If decoding fails, the encoded word
    // MUST be displayed as-is.
    // Bug: parse_encoded_word advanced past charset/encoding/text
    // before base64 decode failed, losing the consumed characters.
    let input = b"=?UTF-8?B?invalid base64!!!?= tail";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "=?UTF-8?B?invalid base64!!!?= tail",
        "Failed base64 decode must preserve entire encoded word verbatim (RFC 2047 Section 6.3)"
    );
}

#[test]
fn rfc2047_whitespace_preserved_before_invalid_encoded_word() {
    // RFC 2047 Section 6.2: whitespace between adjacent encoded words is
    // ignored, but ONLY when both tokens are valid encoded words.
    // RFC 2047 Section 6.3: unrecognized encoded words must be displayed
    // as ordinary text, so whitespace before them must be preserved.
    let input = b"=?UTF-8?Q?Hello?= =?UTF-8?X?fail?= end";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Hello =?UTF-8?X?fail?= end",
        "Whitespace before an invalid encoded word must be preserved (RFC 2047 Section 6.3)"
    );
}

#[test]
fn rfc2047_requires_lwsp_boundaries_in_text() {
    // RFC 2047 Section 5: an encoded-word in `*text` must be separated
    // from adjacent text by linear whitespace. A glued token is not a
    // valid encoded-word occurrence and must remain literal.
    let input = b"Prefix=?UTF-8?Q?caf=C3=A9?=Suffix";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "Prefix=?UTF-8?Q?caf=C3=A9?=Suffix",
        "RFC 2047 Section 5: glued encoded-words must not be decoded"
    );
}

/// Regression IMAP-001: overlong encoded words must be decoded per Postel's law.
///
/// RFC 2047 Section 2 says encoded words "MUST NOT be more than 75 characters
/// long", but that is an ENCODER constraint. Per Postel's law (be liberal in
/// what you accept), the decoder must accept structurally valid encoded words
/// regardless of length. Many real-world servers produce overlong encoded words.
#[test]
fn imap_001_overlong_encoded_word_decoded_per_postels_law() {
    use base64::Engine;
    // Build a base64 encoded word well over 75 characters.
    // "Hello World! This is a long subject line for testing overlong encoded words in IMAP"
    // base64 of this is ~116 chars, total encoded word is ~130 chars.
    let long_text =
        "Hello World! This is a long subject line for testing overlong encoded words in IMAP";
    let b64 = base64::engine::general_purpose::STANDARD.encode(long_text.as_bytes());
    let encoded_word = format!("=?UTF-8?B?{b64}?=");
    // Verify the encoded word exceeds 75 characters
    assert!(
        encoded_word.len() > 75,
        "test setup: encoded word must exceed 75 chars, got {}",
        encoded_word.len()
    );
    let result = decode_rfc2047(encoded_word.as_bytes());
    assert_eq!(
        result, long_text,
        "Postel's law: overlong encoded words must be decoded, not preserved verbatim"
    );
}

/// Regression IMAP-001: Q-encoded overlong word must also decode.
#[test]
fn imap_001_overlong_q_encoded_word_decoded() {
    // 64 A's in Q-encoding: =?UTF-8?Q?<64 A's>?= = 2+5+1+1+1+64+2 = 76 chars
    let input = b"=?UTF-8?Q?AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA?=";
    assert!(input.len() > 75, "test setup: must exceed 75 chars");
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "Postel's law: overlong Q-encoded words must be decoded"
    );
}

/// Regression IMAP-002: empty encoded-text must decode to empty string per Postel's law.
///
/// RFC 2047 Section 2 defines `encoded-text = 1*<any printable ASCII except ?>`,
/// so empty payload is technically malformed. But per Postel's law, some servers
/// encode empty strings as `=?UTF-8?B??=`. The decoder should accept this and
/// produce an empty string.
#[test]
fn imap_002_empty_encoded_text_base64_decoded() {
    let input = b"=?UTF-8?B??=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "",
        "Postel's law: empty base64 encoded-text must decode to empty string"
    );
}

/// Regression IMAP-002: empty Q-encoded text must also decode to empty string.
#[test]
fn imap_002_empty_encoded_text_q_decoded() {
    let input = b"=?UTF-8?Q??=";
    let result = decode_rfc2047(input);
    assert_eq!(
        result, "",
        "Postel's law: empty Q-encoded text must decode to empty string"
    );
}

// ===== Envelope + FETCH tests =====

#[test]
fn address_simple() {
    let (_, addr) = address(b"(\"Alice\" NIL \"alice\" \"example.com\")", false).unwrap();
    assert_eq!(addr.name.as_deref(), Some("Alice"));
    assert_eq!(addr.mailbox.as_deref(), Some("alice"));
    assert_eq!(addr.host.as_deref(), Some("example.com"));
}

#[test]
fn address_all_nil() {
    let (_, addr) = address(b"(NIL NIL NIL NIL)", false).unwrap();
    assert!(addr.name.is_none());
    assert!(addr.mailbox.is_none());
    assert!(addr.host.is_none());
}

#[test]
fn address_rfc2047_name() {
    let (_, addr) = address(
        b"(\"=?UTF-8?B?QWxpY2U=?=\" NIL \"alice\" \"example.com\")",
        false,
    )
    .unwrap();
    assert_eq!(addr.name.as_deref(), Some("Alice"));
}

#[test]
fn address_list_nil() {
    let (_, list) = address_list(b"NIL", false).unwrap();
    assert!(list.is_empty());
}

#[test]
fn address_list_multi() {
    let (_, list) = address_list(
        b"((\"A\" NIL \"a\" \"x.com\")(\"B\" NIL \"b\" \"y.com\"))",
        false,
    )
    .unwrap();
    assert_eq!(list.len(), 2);
}

#[test]
fn envelope_full() {
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test Subject\" \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            ((\"Recipient\" NIL \"rcpt\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg-id@example.com>\")";
    let (_, env) = envelope(input, false).unwrap();
    assert_eq!(env.subject.as_deref(), Some("Test Subject"));
    assert_eq!(env.from.len(), 1);
    assert_eq!(env.to.len(), 1);
    assert_eq!(env.message_id.as_deref(), Some("<msg-id@example.com>"));
}

#[test]
fn envelope_all_nil() {
    let input = b"(NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, false).unwrap();
    assert!(env.date.is_none());
    assert!(env.subject.is_none());
    assert!(env.from.is_empty());
}

/// RFC 3501 Section 7.4.2: "If the Sender or Reply-To lines are absent
/// in the [RFC-2822] header, or are present but empty, the server sets
/// the corresponding member of the envelope to be the same value as the
/// from member (the client is not expected to know to do this)."
///
/// When a server sends NIL for sender/reply-to but populates from,
/// the parser defaults them to from per the RFC.
#[test]
fn envelope_nil_sender_reply_to_preserved() {
    // from is populated, but sender and reply-to are NIL
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            NIL \
            NIL \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, false).unwrap();

    // RFC 3501 Section 7.4.2: sender and reply_to default to from when NIL
    assert_eq!(env.from.len(), 1);
    assert_eq!(
        env.sender, env.from,
        "sender must default to from when NIL (RFC 3501 Section 7.4.2)"
    );
    assert_eq!(
        env.reply_to, env.from,
        "reply_to must default to from when NIL (RFC 3501 Section 7.4.2)"
    );
}

/// RFC 3501 Section 7.4.2: "If the Sender or Reply-To lines are absent
/// in the [RFC-2822] header, or are present but empty, the server sets
/// the corresponding member of the envelope to be the same value as the
/// from member (the client is not expected to know to do this)."
///
/// Full `parse_response` round-trip: sender and reply-to default to from
/// when the server sends NIL.
#[test]
fn envelope_preserves_nil_sender_reply_to() {
    // Envelope with from=(alice@ex.com), sender=NIL, reply-to=NIL
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024 00:00:00 +0000\" \"Test\" ((\"Alice\" NIL \"alice\" \"ex.com\")) NIL NIL ((\"Bob\" NIL \"bob\" \"ex.com\")) NIL NIL NIL \"<msg@ex.com>\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let env = f.envelope.as_ref().unwrap();
            assert_eq!(env.from.len(), 1, "from should have one address");
            // RFC 3501 Section 7.4.2: sender and reply_to default to from when NIL
            assert_eq!(
                env.sender, env.from,
                "sender must default to from when NIL (RFC 3501 Section 7.4.2); got {:?}",
                env.sender
            );
            assert_eq!(
                env.reply_to, env.from,
                "reply_to must default to from when NIL (RFC 3501 Section 7.4.2); got {:?}",
                env.reply_to
            );
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// When sender/reply-to are explicitly provided (non-NIL), they should
/// NOT be overridden with the from value.
#[test]
fn envelope_explicit_sender_reply_to_preserved() {
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            ((\"Secretary\" NIL \"secretary\" \"example.com\")) \
            ((\"ReplyAddr\" NIL \"reply\" \"example.com\")) \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, false).unwrap();

    assert_eq!(env.sender.len(), 1);
    assert_eq!(env.sender[0].mailbox.as_deref(), Some("secretary"));
    assert_eq!(env.reply_to.len(), 1);
    assert_eq!(env.reply_to[0].mailbox.as_deref(), Some("reply"));
}

#[test]
fn fetch_uid_flags() {
    let input = b"(UID 42 FLAGS (\\Seen \\Flagged))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(42));
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
    assert!(flags.contains(&Flag::Flagged));
}

#[test]
fn fetch_rfc822_size() {
    let input = b"(RFC822.SIZE 1234)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.rfc822_size, Some(1234));
}

#[test]
fn fetch_with_envelope() {
    let input = b"(UID 10 ENVELOPE (NIL \"Hello\" \
            ((\"Test\" NIL \"test\" \"example.com\")) \
            NIL NIL NIL NIL NIL NIL \
            \"<msg@example.com>\"))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(10));
    let env = fr.envelope.unwrap();
    assert_eq!(env.subject.as_deref(), Some("Hello"));
}

#[test]
fn fetch_modseq() {
    let input = b"(UID 5 MODSEQ (12345))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.mod_seq, Some(12345));
}

#[test]
fn fetch_body_section() {
    let input = b"(BODY[] \"hello\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "");
    assert_eq!(fr.body_sections[0].data.as_deref(), Some(b"hello".as_ref()));
}

#[test]
fn full_fetch_response() {
    let input = b"* 1 FETCH (UID 100 FLAGS (\\Seen) RFC822.SIZE 500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(fr.seq, 1);
            assert_eq!(fr.uid, Some(100));
            assert_eq!(fr.rfc822_size, Some(500));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== BODYSTRUCTURE tests =====

#[test]
fn body_structure_text_plain() {
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        params,
        encoding,
        size,
        lines,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "plain");
        assert_eq!(params, &[("charset".into(), "UTF-8".into())]);
        assert_eq!(encoding, "7bit");
        assert_eq!(*size, 100);
        assert_eq!(*lines, 5);
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

#[test]
fn body_structure_rfc2231_params_end_to_end() {
    // End-to-end: parse BODYSTRUCTURE with RFC 2231 continuation params.
    // The parser automatically decodes and reassembles them.
    let input = b"(\"APPLICATION\" \"PDF\" (\"FILENAME*0*\" \"UTF-8''long%20\" \"FILENAME*1\" \"name.pdf\") NIL NIL \"BASE64\" 12345)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Basic { params, .. } = &bs {
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "filename");
        assert_eq!(params[0].1, "long name.pdf");
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

#[test]
fn body_structure_basic_image() {
    let input = b"(\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 2048)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Basic {
        media_type,
        media_subtype,
        size,
        ..
    } = &bs
    {
        assert_eq!(media_type, "image");
        assert_eq!(media_subtype, "png");
        assert_eq!(*size, 2048);
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

#[test]
fn body_structure_multipart() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5)\
                       (\"TEXT\" \"HTML\" NIL NIL NIL \"QUOTED-PRINTABLE\" 200 10) \
                       \"ALTERNATIVE\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "alternative");
        assert_eq!(bodies.len(), 2);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

/// RFC 3501 Section 7.4.2 / Postel's law: tolerate optional whitespace
/// between child bodies in a multipart BODYSTRUCTURE. While the ABNF
/// specifies `1*body` with no separator, non-conformant servers may
/// insert spaces.
#[test]
fn body_structure_multipart_whitespace_between_children() {
    // Note the space between the two child bodies: `) (`
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) (\"TEXT\" \"HTML\" NIL NIL NIL \"QUOTED-PRINTABLE\" 200 10) \"ALTERNATIVE\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        bodies,
        media_subtype,
        ..
    } = bs
    {
        assert_eq!(
            bodies.len(),
            2,
            "both child bodies must be parsed despite whitespace between them"
        );
        assert_eq!(media_subtype, "alternative");
    } else {
        panic!("expected Multipart");
    }
}

#[test]
fn body_structure_with_disposition() {
    let input = b"(\"APPLICATION\" \"PDF\" NIL NIL NIL \"BASE64\" 4096 \
                       NIL (\"ATTACHMENT\" (\"FILENAME\" \"report.pdf\")) NIL NIL)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Basic { disposition, .. } = &bs {
        let disp = disposition.as_ref().expect("expected disposition");
        assert_eq!(disp.disposition_type, "attachment");
        assert_eq!(disp.params, vec![("filename".into(), "report.pdf".into())]);
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

#[test]
fn body_params_nil() {
    let (_, params) = body_params(b"NIL").unwrap();
    assert!(params.is_empty());
}

#[test]
fn body_params_pairs() {
    let (_, params) = body_params(b"(\"CHARSET\" \"UTF-8\" \"NAME\" \"test.txt\")").unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
}

// ===== RFC 2231 MIME parameter encoding tests =====
//
// body_params automatically decodes RFC 2231 continuation segments and
// charset-encoded values at parse time (RFC 2231 Sections 3-4). Callers
// receive clean, reassembled parameters.

#[test]
fn body_params_rfc2231_charset_encoded() {
    // RFC 2231 Section 4: charset-encoded parameter with percent-encoding.
    // Servers may present `filename*` with charset''percent-encoded value.
    let input = b"(\"CHARSET\" \"UTF-8\" \"NAME*\" \"UTF-8''t%C3%A9st%20file.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
    assert_eq!(params[1].0, "name");
    assert_eq!(params[1].1, "t\u{e9}st file.txt");
}

#[test]
fn body_params_rfc2231_continuation() {
    // RFC 2231 Section 3: continuation parameters split across multiple keys.
    let input = b"(\"FILENAME*0\" \"very-long-\" \"FILENAME*1\" \"filename.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(
        params[0],
        ("filename".into(), "very-long-filename.txt".into())
    );
}

#[test]
fn body_params_rfc2231_charset_and_continuation() {
    // RFC 2231 Sections 3-4 combined: charset-encoded continuation parameter.
    let input = b"(\"FILENAME*0*\" \"UTF-8''%C3%A9l%C3%A8ve-\" \"FILENAME*1\" \"rapport.pdf\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "filename");
    assert_eq!(params[0].1, "\u{e9}l\u{e8}ve-rapport.pdf");
}

#[test]
fn body_disposition_rfc2231_charset_encoded() {
    // RFC 2231 parameter in Content-Disposition (RFC 2183).
    let input = b"(\"attachment\" (\"FILENAME*\" \"UTF-8''r%C3%A9sum%C3%A9.pdf\"))";
    let (_, disp) = body_disposition(input).unwrap();
    let disp = disp.expect("disposition should not be NIL");
    assert_eq!(disp.disposition_type, "attachment");
    assert_eq!(disp.params.len(), 1);
    assert_eq!(disp.params[0].0, "filename");
    assert_eq!(disp.params[0].1, "r\u{e9}sum\u{e9}.pdf");
}

#[test]
fn body_params_rfc2231_iso8859() {
    // RFC 2231 with non-UTF-8 charset (ISO-8859-1).
    let input = b"(\"FILENAME*\" \"ISO-8859-1''caf%E9.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "filename");
    assert_eq!(params[0].1, "caf\u{e9}.txt");
}

#[test]
fn body_structure_with_rfc2231_disposition() {
    // Full BODYSTRUCTURE with RFC 2231 encoded disposition parameter.
    let input = b"(\"APPLICATION\" \"PDF\" (\"NAME\" \"file.pdf\") NIL NIL \"BASE64\" 12345 NIL (\"attachment\" (\"FILENAME*\" \"UTF-8''t%C3%A9st.pdf\")) NIL NIL)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Basic { disposition, .. } = bs {
        let disp = disposition.expect("disposition should not be NIL");
        assert_eq!(disp.disposition_type, "attachment");
        assert_eq!(disp.params[0].0, "filename");
        assert_eq!(disp.params[0].1, "t\u{e9}st.pdf");
    } else {
        panic!("expected Basic body structure");
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

#[test]
fn untagged_vanished_earlier() {
    let input = b"VANISHED (EARLIER) 1:5,10\r\n";
    let (_, resp) = parse_untagged_vanished(input).unwrap();
    if let UntaggedResponse::Vanished { earlier, uids } = resp {
        assert!(earlier);
        assert_eq!(uids.len(), 2);
        assert_eq!(uids[0], UidRange::range(1, 5));
        assert_eq!(uids[1], UidRange::single(10));
    } else {
        panic!("expected Vanished, got {resp:?}");
    }
}

#[test]
fn untagged_vanished_no_earlier() {
    let input = b"VANISHED 42\r\n";
    let (_, resp) = parse_untagged_vanished(input).unwrap();
    if let UntaggedResponse::Vanished { earlier, uids } = resp {
        assert!(!earlier);
        assert_eq!(uids, vec![UidRange::single(42)]);
    } else {
        panic!("expected Vanished, got {resp:?}");
    }
}

#[test]
fn untagged_id_with_pairs() {
    let input = b"ID (\"name\" \"Dovecot\" \"version\" \"2.3.16\")\r\n";
    let (_, resp) = parse_untagged_id(input).unwrap();
    if let UntaggedResponse::Id(params) = resp {
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].0, "name");
        assert_eq!(params[0].1, Some("Dovecot".to_owned()));
        assert_eq!(params[1].0, "version");
        assert_eq!(params[1].1, Some("2.3.16".to_owned()));
    } else {
        panic!("expected Id, got {resp:?}");
    }
}

#[test]
fn untagged_id_nil() {
    let input = b"ID NIL\r\n";
    let (_, resp) = parse_untagged_id(input).unwrap();
    if let UntaggedResponse::Id(params) = resp {
        assert!(params.is_empty());
    } else {
        panic!("expected Id, got {resp:?}");
    }
}

#[test]
fn untagged_namespace_full() {
    let input = b"NAMESPACE ((\"\" \"/\")) ((\"~\" \"/\")) ((\"#shared.\" \".\"))\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert_eq!(other.len(), 1);
        assert_eq!(other[0].prefix, "~");
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].prefix, "#shared.");
        assert_eq!(shared[0].delimiter, Some('.'));
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

#[test]
fn untagged_namespace_all_nil() {
    let input = b"NAMESPACE NIL NIL NIL\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert!(personal.is_empty());
        assert!(other.is_empty());
        assert!(shared.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

#[test]
fn untagged_status_mailbox() {
    let input = b"STATUS \"INBOX\" (MESSAGES 17 UNSEEN 5 UIDNEXT 100 UIDVALIDITY 1234)\r\n";
    let (_, resp) = parse_untagged_status_mailbox(input, false).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(items.len(), 4);
        assert_eq!(items[0], StatusItem::Messages(17));
        assert_eq!(items[1], StatusItem::Unseen(5));
        assert_eq!(items[2], StatusItem::UidNext(100));
        assert_eq!(items[3], StatusItem::UidValidity(1234));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

#[test]
fn status_item_highestmodseq() {
    let (_, items) = status_items(b"HIGHESTMODSEQ 99999").unwrap();
    assert_eq!(items, vec![StatusItem::HighestModSeq(99999)]);
}

#[test]
fn status_item_size() {
    let (_, items) = status_items(b"SIZE 1048576").unwrap();
    assert_eq!(items, vec![StatusItem::Size(1_048_576)]);
}

#[test]
fn status_item_unknown_skipped() {
    // Unknown attributes are now gracefully skipped, not errors.
    let (_, items) = status_items(b"BOGUS 42").unwrap();
    assert!(items.is_empty(), "unknown attribute should be skipped");
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
fn response_code_nomodseq() {
    let (_, code) = response_code(b"[NOMODSEQ]").unwrap();
    assert_eq!(code, ResponseCode::NoModSeq);
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
        assert_eq!(ranges[0].start, 1);
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
        assert_eq!(ranges[0].start, u32::MAX);
        assert_eq!(ranges[0].end, None);
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

// ===== Mailbox attribute tests =====

#[test]
fn mailbox_attribute_all_base() {
    assert_eq!(
        parse_mailbox_attribute("\\Noinferiors"),
        MailboxAttribute::NoInferiors
    );
    assert_eq!(
        parse_mailbox_attribute("\\Noselect"),
        MailboxAttribute::NoSelect
    );
    assert_eq!(
        parse_mailbox_attribute("\\NonExistent"),
        MailboxAttribute::NonExistent
    );
    assert_eq!(
        parse_mailbox_attribute("\\HasChildren"),
        MailboxAttribute::HasChildren
    );
    assert_eq!(
        parse_mailbox_attribute("\\HasNoChildren"),
        MailboxAttribute::HasNoChildren
    );
    assert_eq!(
        parse_mailbox_attribute("\\Marked"),
        MailboxAttribute::Marked
    );
    assert_eq!(
        parse_mailbox_attribute("\\Unmarked"),
        MailboxAttribute::Unmarked
    );
    assert_eq!(
        parse_mailbox_attribute("\\Subscribed"),
        MailboxAttribute::Subscribed
    );
    assert_eq!(
        parse_mailbox_attribute("\\Remote"),
        MailboxAttribute::Remote
    );
}

#[test]
fn mailbox_attribute_special_use() {
    assert_eq!(parse_mailbox_attribute("\\All"), MailboxAttribute::All);
    assert_eq!(
        parse_mailbox_attribute("\\Archive"),
        MailboxAttribute::Archive
    );
    assert_eq!(
        parse_mailbox_attribute("\\Drafts"),
        MailboxAttribute::Drafts
    );
    assert_eq!(
        parse_mailbox_attribute("\\Flagged"),
        MailboxAttribute::Flagged
    );
    assert_eq!(parse_mailbox_attribute("\\Junk"), MailboxAttribute::Junk);
    assert_eq!(parse_mailbox_attribute("\\Sent"), MailboxAttribute::Sent);
    assert_eq!(parse_mailbox_attribute("\\Trash"), MailboxAttribute::Trash);
    assert_eq!(
        parse_mailbox_attribute("\\Important"),
        MailboxAttribute::Important
    );
}

#[test]
fn mailbox_attribute_case_insensitive() {
    assert_eq!(
        parse_mailbox_attribute("\\NOINFERIORS"),
        MailboxAttribute::NoInferiors
    );
    assert_eq!(
        parse_mailbox_attribute("\\NOSELECT"),
        MailboxAttribute::NoSelect
    );
    assert_eq!(
        parse_mailbox_attribute("\\haschildren"),
        MailboxAttribute::HasChildren
    );
    assert_eq!(parse_mailbox_attribute("\\SENT"), MailboxAttribute::Sent);
}

#[test]
fn mailbox_attribute_custom() {
    let attr = parse_mailbox_attribute("\\MyCustom");
    assert_eq!(attr, MailboxAttribute::Custom("\\MyCustom".to_owned()));
}

// ===== FETCH with INTERNALDATE =====

#[test]
fn fetch_internaldate() {
    let input = b"(UID 42 INTERNALDATE \"17-Jul-1996 02:44:25 -0700\")\r\n";
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 42 INTERNALDATE \"17-Jul-1996 02:44:25 -0700\")\r\n")
            .unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.uid, Some(42));
            assert_eq!(
                fr.internal_date.as_deref(),
                Some("17-Jul-1996 02:44:25 -0700")
            );
        } else {
            panic!("expected Fetch, got {boxed:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
    // Also test standalone parser
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(
        fr.internal_date.as_deref(),
        Some("17-Jul-1996 02:44:25 -0700")
    );
}

/// RFC 3501 Section 7.4.2: INTERNALDATE is formally `date-time` (always
/// quoted), but some servers send NIL for malformed/draft messages.
/// The parser must handle NIL gracefully rather than failing the entire
/// FETCH response  -  losing UID, FLAGS, ENVELOPE, etc. would be far worse
/// than having `internal_date = None`.
#[test]
fn fetch_internaldate_nil_accepted() {
    let input = b"* 5 FETCH (UID 100 FLAGS (\\Seen) INTERNALDATE NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.uid, Some(100));
                assert_eq!(fr.flags.as_ref().map(std::vec::Vec::len), Some(1));
                assert_eq!(
                    fr.internal_date, None,
                    "INTERNALDATE NIL must parse as None, not fail the FETCH response"
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Verify that INTERNALDATE NIL doesn't cause loss of subsequent FETCH
/// attributes. The UID and FLAGS after INTERNALDATE must still be parsed.
#[test]
fn fetch_internaldate_nil_preserves_other_attrs() {
    let input = b"* 1 FETCH (INTERNALDATE NIL UID 42 RFC822.SIZE 1024)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.internal_date, None);
                assert_eq!(fr.uid, Some(42));
                assert_eq!(fr.rfc822_size, Some(1024));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== FETCH variant tests =====

#[test]
fn fetch_body_header() {
    let input = b"(BODY[HEADER] \"Subject: Test\\r\\n\\r\\n\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "HEADER");
    assert!(fr.body_sections[0].data.is_some());
}

#[test]
fn fetch_body_text() {
    let input = b"(BODY[TEXT] \"message body here\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "TEXT");
    assert_eq!(
        fr.body_sections[0].data.as_deref(),
        Some(b"message body here".as_ref())
    );
}

#[test]
fn fetch_body_header_fields() {
    let input = b"(BODY[HEADER.FIELDS (Subject From)] \"Subject: Hi\\r\\nFrom: a@b\\r\\n\\r\\n\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "HEADER.FIELDS (Subject From)");
}

#[test]
fn fetch_body_part_number() {
    let input = b"(BODY[1.2] \"part data\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "1.2");
    assert_eq!(
        fr.body_sections[0].data.as_deref(),
        Some(b"part data".as_ref())
    );
}

#[test]
fn fetch_body_partial_origin() {
    let input = b"(BODY[]<0> \"first 100 bytes\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "");
    assert_eq!(fr.body_sections[0].origin, Some(0));
}

#[test]
fn fetch_body_nil_data() {
    let input = b"(BODY[TEXT] NIL)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "TEXT");
    assert!(fr.body_sections[0].data.is_none());
}

#[test]
fn fetch_multiple_body_sections() {
    let input = b"(BODY[HEADER] \"hdr\" BODY[TEXT] \"body\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 2);
    assert_eq!(fr.body_sections[0].section, "HEADER");
    assert_eq!(fr.body_sections[1].section, "TEXT");
}

#[test]
fn fetch_body_without_section_is_bodystructure() {
    // BODY without [] is a synonym for BODYSTRUCTURE (RFC 3501 Section 6.4.5)
    let input = b"(BODY (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert!(fr.body_structure.is_some());
}

#[test]
fn fetch_body_with_literal() {
    let input = b"(BODY[] {5}\r\nhello)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].data.as_deref(), Some(b"hello".as_ref()));
}

#[test]
fn fetch_all_items_combined() {
    let input = b"(UID 99 FLAGS (\\Seen) RFC822.SIZE 2048 \
            INTERNALDATE \"01-Jan-2024 00:00:00 +0000\" \
            BODY[HEADER] \"From: test\\r\\n\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(99));
    assert_eq!(fr.rfc822_size, Some(2048));
    assert_eq!(
        fr.internal_date.as_deref(),
        Some("01-Jan-2024 00:00:00 +0000")
    );
    assert_eq!(fr.body_sections.len(), 1);
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
}

#[test]
fn fetch_unknown_attribute_skipped() {
    // Future attributes should be skipped gracefully
    let input = b"(UID 1 FUTUREATTR \"value\" FLAGS (\\Seen))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(1));
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
}

// ===== BINARY extension tests (RFC 3516) =====

#[test]
fn fetch_binary_simple_section() {
    // BINARY[1] with quoted data (RFC 3516 Section 4.2)
    let input = b"(BINARY[1] \"binary data\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert_eq!(fr.binary_sections[0].origin, None);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"binary data".as_ref())
    );
}

#[test]
fn fetch_binary_nested_section() {
    // BINARY[1.2.3] with literal data (RFC 3516 Section 4.2)
    let input = b"(BINARY[1.2.3] {5}\r\nhello)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1, 2, 3]);
    assert_eq!(fr.binary_sections[0].origin, None);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"hello".as_ref())
    );
}

#[test]
fn fetch_binary_with_origin() {
    // BINARY[2]<0>  -  partial binary fetch with origin offset (RFC 3516 Section 4.2)
    let input = b"(BINARY[2]<0> \"partial\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![2]);
    assert_eq!(fr.binary_sections[0].origin, Some(0));
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"partial".as_ref())
    );
}

#[test]
fn fetch_binary_nil_data() {
    // BINARY[1] NIL  -  server returns NIL for absent data (RFC 3516 Section 4.2)
    let input = b"(BINARY[1] NIL)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert!(fr.binary_sections[0].data.is_none());
}

#[test]
fn fetch_binary_size() {
    // BINARY.SIZE[1] number (RFC 3516 Section 4.3)
    let input = b"(BINARY.SIZE[1] 2048)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![1], 2048));
}

#[test]
fn fetch_binary_size_nested() {
    // BINARY.SIZE[1.2]  -  nested section (RFC 3516 Section 4.3)
    let input = b"(BINARY.SIZE[1.2] 512)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![1, 2], 512));
}

#[test]
fn fetch_binary_mixed_with_other_attrs() {
    // BINARY and BINARY.SIZE alongside regular FETCH attributes
    let input = b"(UID 42 BINARY[1] \"data\" BINARY.SIZE[2] 1024 FLAGS (\\Seen))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(42));
    assert_eq!(fr.binary_sections.len(), 1);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"data".as_ref())
    );
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![2], 1024));
    let flags = fr.flags.unwrap();
    assert!(flags.contains(&Flag::Seen));
}

#[test]
fn fetch_multiple_binary_sections() {
    // Multiple BINARY sections in one response (RFC 3516 Section 4.2)
    let input = b"(BINARY[1] \"part1\" BINARY[2] \"part2\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sections.len(), 2);
    assert_eq!(fr.binary_sections[0].section, vec![1]);
    assert_eq!(
        fr.binary_sections[0].data.as_deref(),
        Some(b"part1".as_ref())
    );
    assert_eq!(fr.binary_sections[1].section, vec![2]);
    assert_eq!(
        fr.binary_sections[1].data.as_deref(),
        Some(b"part2".as_ref())
    );
}

#[test]
fn fetch_binary_empty_section() {
    // RFC 9051 Section 9: section-binary = "[" [section-part] "]"
    // Empty section-part is valid and produces an empty section vec.
    let input = b"(BINARY[] \"all\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sections.len(), 1);
    assert!(
        fr.binary_sections[0].section.is_empty(),
        "RFC 9051: empty section-part must produce empty vec"
    );
    assert_eq!(fr.binary_sections[0].data.as_deref(), Some(b"all".as_ref()));
}

#[test]
fn fetch_binary_size_empty_section() {
    // RFC 9051 Section 9: section-binary = "[" [section-part] "]"
    // Empty section-part is valid and produces an empty section vec.
    let input = b"(BINARY.SIZE[] 4096)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    let (ref section, size) = fr.binary_sizes[0];
    assert!(section.is_empty(), "RFC 9051: empty section-part");
    assert_eq!(size, 4096);
}

#[test]
fn binary_size_number64() {
    // RFC 9051 Section 9 formal syntax specifies `number` (u32) for
    // BINARY.SIZE, but the parser accepts number64 as a Postel's-law
    // leniency. This test verifies values exceeding u32::MAX parse correctly.
    let input = b"(BINARY.SIZE[1] 5000000000)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.binary_sizes.len(), 1);
    assert_eq!(fr.binary_sizes[0], (vec![1], 5_000_000_000u64));
}

#[test]
fn parse_fetch_binary_empty_section_rfc9051() {
    // RFC 9051 Section 9: section-binary = "[" [section-part] "]"
    // Empty section-part must be accepted.
    let input = b"* 1 FETCH (BINARY[] ~{5}\r\nhello)\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(inner) = resp {
        if let UntaggedResponse::Fetch(fetch) = *inner {
            assert_eq!(fetch.binary_sections.len(), 1);
            assert!(
                fetch.binary_sections[0].section.is_empty(),
                "RFC 9051: empty section-part must produce empty vec"
            );
            assert_eq!(
                fetch.binary_sections[0].data.as_deref(),
                Some(b"hello".as_slice())
            );
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn parse_fetch_binary_size_empty_section_rfc9051() {
    // RFC 9051 Section 9: BINARY.SIZE with empty section-part
    let input = b"* 1 FETCH (BINARY.SIZE[] 4096)\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(inner) = resp {
        if let UntaggedResponse::Fetch(fetch) = *inner {
            assert_eq!(fetch.binary_sizes.len(), 1);
            let (ref section, size) = fetch.binary_sizes[0];
            assert!(section.is_empty(), "RFC 9051: empty section-part");
            assert_eq!(size, 4096);
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== Negative / malformed input tests =====

#[test]
fn quoted_string_rejects_nul() {
    assert!(quoted_string(b"\"abc\x00def\"").is_err());
}

#[test]
fn quoted_string_rejects_bare_cr() {
    assert!(quoted_string(b"\"abc\rdef\"").is_err());
}

#[test]
fn quoted_string_rejects_bare_lf() {
    assert!(quoted_string(b"\"abc\ndef\"").is_err());
}

/// RFC 3501 Section 9: QUOTED-CHAR = any TEXT-CHAR except quoted-specials,
/// and TEXT-CHAR excludes CR and LF.  A backslash followed by CR is
/// malformed  -  it must not silently include the CR in the result or skip
/// past the CRLF response terminator.
#[test]
fn quoted_string_rejects_escaped_cr() {
    // \<CR> is not a valid QUOTED-CHAR  -  must error.
    assert!(quoted_string(b"\"test\\\rmore\"").is_err());
}

/// Same as above but for LF.
#[test]
fn quoted_string_rejects_escaped_lf() {
    // \<LF> is not a valid QUOTED-CHAR  -  must error.
    assert!(quoted_string(b"\"test\\\nmore\"").is_err());
}

/// RFC 3501 Section 9: "The ASCII NUL character, %x00, MUST NOT be used
/// at any time."  A backslash followed by NUL must be rejected  -  it must
/// not silently push NUL into the output.
#[test]
fn quoted_string_rejects_nul_after_backslash() {
    // \<NUL> is not a valid QUOTED-CHAR  -  must error.
    assert!(quoted_string(b"\"hello\\\x00world\"").is_err());
}

#[test]
fn quoted_string_unterminated() {
    // Complete-mode: unterminated quote must return Error (not Incomplete)
    // so that alt() can fall through to alternative parsers.
    let result = quoted_string(b"\"hello");
    assert!(matches!(result, Err(nom::Err::Error(_))));
}

#[test]
fn quoted_string_unterminated_escape() {
    // Complete-mode: truncated escape must return Error (not Incomplete).
    let result = quoted_string(b"\"hello\\");
    assert!(matches!(result, Err(nom::Err::Error(_))));
}

#[test]
fn literal_truncated_data() {
    // Literal says 10 bytes, but only 5 provided
    let result = literal(b"{10}\r\nhello");
    assert!(result.is_err());
}

#[test]
fn literal_missing_crlf() {
    let result = literal(b"{5}hello");
    assert!(result.is_err());
}

#[test]
fn literal_overflow_count() {
    // Absurdly large count should fail gracefully
    let result = literal(b"{99999999999999999999}\r\n");
    assert!(result.is_err());
}

/// RFC 9051 Section 9: literal count is `number64` (0..2^63-1).
/// Counts exceeding `i64::MAX` must be rejected regardless of platform
/// pointer width. The parser must use u64 for the initial parse, not
/// usize, to avoid truncation on 32-bit platforms.
#[test]
fn literal_count_exceeds_i64_max() {
    // i64::MAX + 1 = 9223372036854775808
    let result = literal(b"{9223372036854775808}\r\n");
    assert!(result.is_err(), "count > i64::MAX must be rejected");
}

/// Literal count at exactly `i64::MAX` should not be rejected by the
/// range check (though it will fail due to insufficient input data).
#[test]
fn literal_count_at_i64_max() {
    // i64::MAX = 9223372036854775807  -  valid per RFC 9051, but
    // take(count) will fail since we don't have that many bytes.
    let result = literal(b"{9223372036854775807}\r\n");
    assert!(
        result.is_err(),
        "should fail from insufficient data, not range check"
    );
}

#[test]
fn number_rejects_non_digit() {
    assert!(number(b"abc").is_err());
}

#[test]
fn number_rejects_empty() {
    assert!(number(b"").is_err());
}

#[test]
fn number_rejects_negative() {
    assert!(number(b"-1").is_err());
}

#[test]
fn parse_response_garbage() {
    // Completely invalid input should fail
    assert!(parse_response(b"!!GARBAGE!!\r\n").is_err());
}

#[test]
fn parse_response_empty() {
    assert!(parse_response(b"").is_err());
}

#[test]
fn parse_response_only_crlf() {
    assert!(parse_response(b"\r\n").is_err());
}

#[test]
fn parse_response_truncated_tagged() {
    // Missing CRLF
    assert!(parse_response(b"A001 OK done").is_err());
}

#[test]
fn parse_response_invalid_status() {
    // Invalid status keyword
    assert!(parse_response(b"A001 INVALID text\r\n").is_err());
}

#[test]
fn greeting_rejects_non_greeting() {
    assert!(parse_greeting(b"* FETCH 1\r\n").is_err());
}

#[test]
fn untagged_fetch_missing_closing_paren() {
    // Mismatched parens: the FETCH parser fails, so it falls through
    // to the Unknown catch-all (RFC 9051 Section 2.2.2).
    let result = parse_response(b"* 1 FETCH (UID 42\r\n");
    match result {
        Ok((_, Response::Untagged(boxed))) => {
            assert!(
                matches!(*boxed, UntaggedResponse::Unknown(_)),
                "Malformed FETCH should fall through to Unknown, got {boxed:?}"
            );
        }
        Ok((_, other)) => panic!("Expected Untagged(Unknown), got {other:?}"),
        Err(_) => { /* also acceptable if parser rejects outright */ }
    }
}

// ===== Incomplete->Error in complete-mode parsers =====
//
// nom's `alt()` only catches `Error`, not `Incomplete`. Returning
// `Incomplete` from a complete-mode parser prevents fallthrough to
// the `parse_untagged_unknown` catch-all (RFC 9051 Section 2.2.2).

#[test]
fn quoted_string_incomplete_in_envelope_is_a_parse_failure() {
    // A FETCH response whose ENVELOPE has an unterminated quoted string.
    // The quoted_string parser must return Error (not Incomplete); ENVELOPE's
    // closed ten-field prefix then promotes it to the connection-fatal
    // recognized-response path instead of the Unknown fallback.
    let input = b"* 1 FETCH (ENVELOPE (\"unterminated subject))\r\n";
    let result = parse_response(input);
    assert!(!matches!(result, Err(nom::Err::Incomplete(_))));
    assert!(result.is_err());
}

#[test]
fn scan_section_spec_incomplete_is_a_parse_failure_not_incomplete() {
    // A FETCH response with an unterminated BODY section (missing `]`).
    // `BODY[...]` is a closed grammar, so this is the server violating it:
    // the result must be a hard parse error, never `Incomplete` (which would
    // stall the reader waiting for bytes that are not coming) and never
    // `Unknown` (which would silently drop the FETCH).
    let input = b"* 1 FETCH (BODY[HEADER no-close\r\n";
    let result = parse_response(input);
    assert!(!matches!(result, Err(nom::Err::Incomplete(_))));
    assert!(result.is_err());
}

#[test]
fn untagged_status_bare_ok_no_text() {
    // Some servers send `* OK\r\n` with no SP or resp-text.
    // parse_tagged tolerates this (RFC 3501 Section 7.1, Postel's law)
    // but parse_untagged_status must also tolerate it for consistency.
    let input = b"* OK\r\n";
    let result = parse_response(input);
    match result {
        Ok((_, Response::Untagged(boxed))) => match *boxed {
            UntaggedResponse::Status { status, code, text } => {
                assert_eq!(status, UntaggedStatus::Ok);
                assert!(code.is_none());
                assert!(text.is_empty());
            }
            other => panic!("Expected Status, got {other:?}"),
        },
        Ok((_, other)) => panic!("Expected Untagged(Status), got {other:?}"),
        Err(e) => {
            panic!("BUG: bare `* OK\\r\\n` should parse like bare tagged status; got error: {e:?}")
        }
    }
}

#[test]
fn untagged_status_bare_bye_no_text() {
    // Server sends `* BYE\r\n` with no text (e.g., abrupt disconnect notice).
    let input = b"* BYE\r\n";
    let result = parse_response(input);
    match result {
        Ok((_, Response::Untagged(boxed))) => match *boxed {
            UntaggedResponse::Status { status, .. } => {
                assert_eq!(status, UntaggedStatus::Bye);
            }
            other => panic!("Expected Status, got {other:?}"),
        },
        Ok((_, other)) => panic!("Expected Untagged(Status), got {other:?}"),
        Err(e) => panic!("BUG: bare `* BYE\\r\\n` should parse; got error: {e:?}"),
    }
}

#[test]
fn skip_paren_group_incomplete_is_an_error_not_incomplete() {
    // A FETCH BODYSTRUCTURE with unclosed parentheses in extension data.
    // skip_paren_group must return Error, never Incomplete: this codec runs
    // in complete mode, and `Incomplete` would stall the reader waiting for
    // bytes that are already all present.
    //
    // The outer body is unbalanced, so the BODYSTRUCTURE prefix gate treats
    // it as a provider contract violation rather than laundering the FETCH
    // into `Unknown` and silently dropping the message's body (C4).
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"text\" \"plain\" NIL NIL NIL \"7bit\" 42 3 NIL NIL NIL NIL (unclosed-ext\r\n";
    let result = parse_response(input);
    assert!(!matches!(result, Err(nom::Err::Incomplete(_))));
    assert!(result.is_err(), "got: {result:?}");
}

#[test]
fn nstring_garbage() {
    // Not NIL and not a string
    assert!(nstring(b"GARBAGE").is_err());
}

#[test]
fn flag_list_mismatched_paren() {
    assert!(flag_list(b"(\\Seen").is_err());
}

#[test]
fn envelope_truncated() {
    // Envelope missing most fields
    let result = envelope(b"(\"date\" \"subject\"", false);
    assert!(result.is_err());
}

#[test]
fn body_structure_truncated() {
    let result = body_structure(b"(\"TEXT\" \"PLAIN\" NIL NIL NIL", false, 0);
    assert!(result.is_err());
}

#[test]
fn high_byte_in_atom_accepted() {
    // Bytes 0x80-0xFF are valid in atoms (not CTL, not specials)
    let (_, val) = atom(b"\xC0\xC1\xC2 rest").unwrap();
    assert_eq!(val, b"\xC0\xC1\xC2");
}

#[test]
fn response_code_case_insensitive() {
    let (_, code) = response_code(b"[alert]").unwrap();
    assert_eq!(code, ResponseCode::Alert);

    let (_, code) = response_code(b"[read-only]").unwrap();
    assert_eq!(code, ResponseCode::ReadOnly);
}

#[test]
fn skip_unknown_fetch_attribute() {
    // Unknown attributes should be skipped without error
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 10 X-CUSTOM \"value\" FLAGS (\\Seen))\r\n").unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.uid, Some(10));
            assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
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

#[test]
fn continuation_empty_text() {
    // Bare continuation with just a space
    let (_, cont) = parse_continuation(b"+ \r\n").unwrap();
    assert_eq!(cont.data, "");
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

/// Base64 continuation challenge must not be mistaken for a response code.
/// RFC 3501 Section 7.5: base64 data never starts with `[`.
#[test]
fn continuation_base64_no_code() {
    let (_, cont) = parse_continuation(b"+ dGVzdA==\r\n").unwrap();
    assert_eq!(cont.code, None);
    assert_eq!(cont.data, "dGVzdA==");
}

/// Plain-text continuation must have `code: None`.
#[test]
fn continuation_plain_text_no_code() {
    let (_, cont) = parse_continuation(b"+ go ahead\r\n").unwrap();
    assert_eq!(cont.code, None);
    assert_eq!(cont.data, "go ahead");
}

#[test]
fn untagged_list_with_all_attributes() {
    let input = b"LIST (\\HasChildren \\Subscribed) \".\" \"INBOX\"\r\n";
    let (_, resp) = parse_untagged_list(input, false).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.name.as_str(), "INBOX");
        assert_eq!(info.delimiter, Some('.'));
        assert!(info.attributes.contains(&MailboxAttribute::HasChildren));
        assert!(info.attributes.contains(&MailboxAttribute::Subscribed));
    } else {
        panic!("expected List, got {resp:?}");
    }
}

#[test]
fn esearch_unknown_key_skipped() {
    // ESEARCH with unknown keys should not fail
    let input = b"ESEARCH (TAG \"A001\") UID ALL 1:3 XFUTURE foo\r\n";
    let (_, resp) = parse_untagged_esearch(input).unwrap();
    if let UntaggedResponse::Esearch(esearch) = resp {
        assert_eq!(esearch.all, vec![UidRange::range(1, 3)]);
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

#[test]
fn namespace_with_extension_data() {
    // Namespace descriptors may have extension data after the delimiter.
    // RFC 2342 Section6: Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"
    let input = b"NAMESPACE ((\"\" \"/\" \"X-EXT\" (\"val\"))) NIL NIL\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert_eq!(
            personal[0].extensions,
            vec![("X-EXT".to_string(), vec!["val".to_string()])]
        );
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

// ===== LSUB parser tests (RFC 3501 Section 7.2.3) =====

#[test]
fn lsub_basic() {
    let input = b"* LSUB () \"/\" \"INBOX\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(info.delimiter, Some('/'));
            assert!(info.attributes.is_empty());
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

#[test]
fn lsub_with_attributes() {
    let input = b"* LSUB (\\NoSelect) \".\" \"Archive\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "Archive");
            assert_eq!(info.delimiter, Some('.'));
            assert!(info.attributes.contains(&MailboxAttribute::NoSelect));
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

/// RFC 3501 Section 7.2.3: "The data is identical in format to the
/// unstripped LIST response." LSUB must parse LIST-EXTENDED data
/// (OLDNAME, CHILDINFO) the same way LIST does (RFC 5258 Section 6).
#[test]
fn lsub_preserves_extended_data() {
    // LSUB with OLDNAME extended data  -  same format as LIST.
    let input = b"* LSUB () \"/\" \"NewName\" (\"OLDNAME\" (\"OldName\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "NewName");
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("OldName"),
                "LSUB must parse OLDNAME extended data like LIST \
                     (RFC 3501 Section 7.2.3, RFC 5258 Section 6)"
            );
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

/// RFC 5258 Section 4: CHILDINFO in LSUB responses must be captured.
#[test]
fn lsub_preserves_childinfo() {
    let input = b"* LSUB () \".\" \"Parent\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Lsub(info) = *u {
            assert_eq!(info.name.as_str(), "Parent");
            assert!(
                info.child_info.contains(&"SUBSCRIBED".to_string()),
                "LSUB must parse CHILDINFO extended data like LIST \
                     (RFC 3501 Section 7.2.3, RFC 5258 Section 4)"
            );
        } else {
            panic!("expected Lsub, got {u:?}");
        }
    } else {
        panic!("expected Untagged, got {resp:?}");
    }
}

// ===== STATUS item parser tests (RFC 3501 Section 7.2.4) =====

#[test]
fn status_messages_unseen() {
    let input = b"STATUS \"INBOX\" (MESSAGES 17 UNSEEN 2)\r\n";
    let (_, resp) = parse_untagged_status_mailbox(input, false).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(items.len(), 2);
        assert!(items.contains(&StatusItem::Messages(17)));
        assert!(items.contains(&StatusItem::Unseen(2)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

#[test]
fn status_uidnext_uidvalidity() {
    let input = b"STATUS \"INBOX\" (UIDNEXT 4392 UIDVALIDITY 3857529045)\r\n";
    let (_, resp) = parse_untagged_status_mailbox(input, false).unwrap();
    if let UntaggedResponse::MailboxStatus { items, .. } = resp {
        assert!(items.contains(&StatusItem::UidNext(4392)));
        assert!(items.contains(&StatusItem::UidValidity(3_857_529_045)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

#[test]
fn status_recent() {
    let input = b"STATUS \"INBOX\" (RECENT 5)\r\n";
    let (_, resp) = parse_untagged_status_mailbox(input, false).unwrap();
    if let UntaggedResponse::MailboxStatus { items, .. } = resp {
        assert_eq!(items.len(), 1);
        assert!(items.contains(&StatusItem::Recent(5)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

#[test]
fn status_all_items() {
    let input = b"STATUS \"INBOX\" (MESSAGES 10 RECENT 3 UNSEEN 2 UIDNEXT 100 UIDVALIDITY 1)\r\n";
    let (_, resp) = parse_untagged_status_mailbox(input, false).unwrap();
    if let UntaggedResponse::MailboxStatus { items, .. } = resp {
        assert_eq!(items.len(), 5);
        assert!(items.contains(&StatusItem::Messages(10)));
        assert!(items.contains(&StatusItem::Recent(3)));
        assert!(items.contains(&StatusItem::Unseen(2)));
        assert!(items.contains(&StatusItem::UidNext(100)));
        assert!(items.contains(&StatusItem::UidValidity(1)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

/// Regression: unknown STATUS attribute with an atom value starting with
/// "NIL" (e.g. "NILSIMSA") must be skipped completely. The old code used
/// `tag_no_case(&b"NIL"[..])` without a token boundary check, so it consumed
/// only "NIL" and left the residue ("SIMSA") in the stream, corrupting
/// subsequent attribute parsing (RFC 9051 Section 7.1).
#[test]
fn status_unknown_attr_nil_prefix_atom() {
    let input = b"STATUS \"INBOX\" (MESSAGES 10 XHASH NILSIMSA UNSEEN 3)\r\n";
    let (_, resp) = parse_untagged_status_mailbox(input, false).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(
            items.contains(&StatusItem::Messages(10)),
            "MESSAGES 10 missing: {items:?}"
        );
        assert!(
            items.contains(&StatusItem::Unseen(3)),
            "UNSEEN 3 missing: {items:?}"
        );
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

// ===== Nested BODYSTRUCTURE tests =====

#[test]
fn body_structure_nested_multipart() {
    // multipart/mixed containing:
    //   - multipart/alternative (text/plain + text/html)
    //   - application/pdf attachment
    // Outer parens wrap the whole BODYSTRUCTURE; inner nested multipart
    // is itself parenthesized per RFC 3501 Section 7.4.2.
    let input = b"(((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5)\
            (\"TEXT\" \"HTML\" (\"CHARSET\" \"UTF-8\") NIL NIL \"QUOTED-PRINTABLE\" 200 10) \
            \"ALTERNATIVE\")\
            (\"APPLICATION\" \"PDF\" NIL NIL NIL \"BASE64\" 5000) \
            \"MIXED\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 2);
        // First body is the multipart/alternative
        if let BodyStructure::Multipart {
            media_subtype: inner_sub,
            bodies: inner_bodies,
            ..
        } = &bodies[0]
        {
            assert_eq!(inner_sub, "alternative");
            assert_eq!(inner_bodies.len(), 2);
        } else {
            panic!("expected inner Multipart, got {:?}", bodies[0]);
        }
        // Second body is the PDF attachment
        assert!(
            matches!(&bodies[1], BodyStructure::Basic { media_type, .. } if media_type.eq_ignore_ascii_case("APPLICATION"))
        );
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

#[test]
fn body_structure_message_rfc822() {
    // message/rfc822 with embedded envelope and body
    let input = b"(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 \
            (NIL \"embedded\" NIL NIL NIL NIL NIL NIL NIL NIL) \
            (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) 20)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Message {
        size,
        envelope,
        body,
        lines,
        ..
    } = bs
    {
        assert_eq!(size, 500);
        assert_eq!(envelope.subject.as_deref(), Some("embedded"));
        assert_eq!(lines, 20);
        assert!(matches!(*body, BodyStructure::Text { .. }));
    } else {
        panic!("expected Message, got {bs:?}");
    }
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

// -- skip_paren_group edge cases --

#[test]
fn skip_paren_group_unclosed_quote() {
    // Unclosed quoted string inside a parenthesized group must return
    // Incomplete, not panic from an out-of-bounds slice.
    let input = b"(\"unclosed)";
    let result = skip_paren_group(input);
    assert!(
        result.is_err(),
        "unclosed quote in paren group should error"
    );
}

#[test]
fn skip_paren_group_trailing_escape_in_quote() {
    // Backslash at the very end of input inside a quoted string.
    let input = b"(\"trail\\";
    let result = skip_paren_group(input);
    assert!(result.is_err(), "trailing escape should error");
}

/// Regression: `skip_paren_group` must not let a backslash escape
/// consume CR/LF bytes. CR/LF are not valid QUOTED-CHAR (RFC 3501
/// Section 9), so the escape must stop and the outer loop must
/// resume. Without this guard, a `\"` followed by `\r\n` would
/// skip the CR, corrupting the parse.
#[test]
fn skip_paren_group_backslash_crlf_in_quoted_string() {
    // ("val\<CR><LF>...)  -  the backslash must NOT skip the CR.
    // After breaking out of the quoted string, the outer loop
    // sees CR and (in a real response) would stop. For
    // skip_paren_group in isolation, the unclosed paren causes
    // an error, which is the correct outcome for malformed input.
    let input = b"(\"val\\\r\n\")";
    let result = skip_paren_group(input);
    assert!(result.is_err());
}

#[test]
fn paren_skippers_do_not_scan_past_backslash_crlf() {
    // Each input contains an apparently matching delimiter only after a
    // response-terminating CRLF. A scanner must reject rather than accept a
    // later response as part of the malformed quoted value.
    let group = b"(\"value\\\r\n* 2 EXISTS\r\n\"next\")";
    assert!(skip_paren_group(group).is_err());
    assert!(skip_parenthesized_block(group).is_err());

    let section = b"HEADER.FIELDS (\"X\\\r\n* 2 EXISTS\r\n\"Y\") ]";
    assert!(scan_section_spec(section).is_err());
}

#[test]
fn paren_skippers_do_not_scan_past_raw_crlf_in_a_quoted_value() {
    // RFC 3501 Section 9: quoted = DQUOTE *QUOTED-CHAR DQUOTE, and QUOTED-CHAR
    // excludes CR and LF - no backslash required. An unterminated quote must
    // not let a scanner adopt the *next* response as part of this value: here
    // the closing quote and paren only appear after `* 2 EXISTS\r\n`.
    let group = b"(\"value\r\n* 2 EXISTS\r\n\")";
    assert!(skip_paren_group(group).is_err());
    assert!(skip_parenthesized_block(group).is_err());

    let section = b"HEADER.FIELDS (\"value\r\n* 2 EXISTS\r\n\") ]";
    assert!(scan_section_spec(section).is_err());
}

#[test]
fn a_raw_crlf_in_a_quoted_fetch_value_does_not_swallow_the_next_response() {
    // End to end: the unknown-attribute skip path must stop at the response
    // terminator, leaving `* 2 EXISTS` for the next parse rather than
    // consuming it as part of the malformed first response.
    let input = b"* 1 FETCH (X-JUNK (\"value\r\n* 2 EXISTS\r\n\"))\r\n";
    let (rest, _) = parse_response(input).expect("first response parses or errors, not both");
    assert!(
        rest.starts_with(b"* 2 EXISTS\r\n"),
        "scanner consumed past the response terminator: {:?}",
        String::from_utf8_lossy(rest)
    );
}

#[test]
fn skip_paren_group_valid() {
    // Well-formed group should succeed.
    let (rest, content) = skip_paren_group(b"(foo \"bar\") tail").unwrap();
    assert_eq!(content, b"foo \"bar\"");
    assert_eq!(rest, b" tail");
}

// ===== FETCH with SAVEDATE (RFC 8514) =====

#[test]
fn fetch_savedate_quoted() {
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 42 SAVEDATE \"28-Dec-2023 10:30:00 +0000\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.save_date.as_deref(), Some("28-Dec-2023 10:30:00 +0000"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_savedate_nil() {
    let (_, resp) = parse_response(b"* 5 FETCH (UID 100 SAVEDATE NIL)\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(100));
        assert!(fr.save_date.is_none());
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_savedate_with_internaldate() {
    let (_, resp) = parse_response(
            b"* 3 FETCH (UID 7 INTERNALDATE \"01-Jan-2024 00:00:00 +0000\" SAVEDATE \"15-Feb-2024 12:00:00 +0000\")\r\n",
        )
        .unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(7));
        assert_eq!(
            fr.internal_date.as_deref(),
            Some("01-Jan-2024 00:00:00 +0000")
        );
        assert_eq!(fr.save_date.as_deref(), Some("15-Feb-2024 12:00:00 +0000"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_savedate_case_insensitive() {
    let (_, resp) =
        parse_response(b"* 1 FETCH (savedate \"01-Jan-2025 00:00:00 +0000\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.save_date.as_deref(), Some("01-Jan-2025 00:00:00 +0000"));
        return;
    }
    panic!("expected Fetch response");
}

// ===== OBJECTID (RFC 8474) parser tests =====

#[test]
fn fetch_emailid() {
    let input = b"* 1 FETCH (UID 42 EMAILID (M6d99ac3275826486))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.email_id.as_deref(), Some("M6d99ac3275826486"));
        return;
    }
    panic!("expected Fetch response with EMAILID");
}

#[test]
fn fetch_threadid() {
    let input = b"* 1 FETCH (UID 42 THREADID (T64b478a75b7ea9fd))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.thread_id.as_deref(), Some("T64b478a75b7ea9fd"));
        return;
    }
    panic!("expected Fetch response with THREADID");
}

#[test]
fn fetch_threadid_nil() {
    let input = b"* 1 FETCH (UID 42 THREADID NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert!(fr.thread_id.is_none());
        return;
    }
    panic!("expected Fetch response with THREADID NIL");
}

#[test]
fn fetch_emailid_and_threadid_combined() {
    let input = b"* 5 FETCH (UID 100 FLAGS (\\Seen) EMAILID (Mabcdef1234567890) THREADID (T0987654321fedcba))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.seq, 5);
        assert_eq!(fr.uid, Some(100));
        assert_eq!(fr.email_id.as_deref(), Some("Mabcdef1234567890"));
        assert_eq!(fr.thread_id.as_deref(), Some("T0987654321fedcba"));
        assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        return;
    }
    panic!("expected Fetch response with EMAILID and THREADID");
}

#[test]
fn fetch_gmail_msgid_and_thrid() {
    let input =
        b"* 5 FETCH (UID 100 X-GM-MSGID 1278455344230334864 X-GM-THRID 1278455344230334865)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.seq, 5);
        assert_eq!(fr.uid, Some(100));
        assert_eq!(fr.gmail_msg_id, Some(1_278_455_344_230_334_864));
        assert_eq!(fr.gmail_thread_id, Some(1_278_455_344_230_334_865));
        return;
    }
    panic!("expected Fetch response with Gmail message and thread ids");
}

#[test]
fn fetch_emailid_case_insensitive() {
    let input = b"* 1 FETCH (emailid (Mabc123))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some("Mabc123"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_threadid_nil_case_insensitive() {
    let input = b"* 1 FETCH (threadid nil)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert!(fr.thread_id.is_none());
        return;
    }
    panic!("expected Fetch response");
}

/// RFC 8474 Section 7: EMAILID is always `(objectid)`, never NIL.
/// Only THREADID has the `nil` alternative in the formal syntax.
/// `EMAILID NIL` must not produce a valid `FetchResponse` with `email_id` = None.
#[test]
fn fetch_emailid_nil_rejected() {
    let input = b"* 1 FETCH (EMAILID NIL)\r\n";
    let result = parse_response(input);
    // The parser may return an error or fall through to Unknown  -
    // either is acceptable. What matters is that it does NOT produce
    // a FetchResponse with email_id silently set to None.
    match result {
        Err(_) => {} // parse error  -  correct
        Ok((_, Response::Untagged(boxed))) => {
            if let UntaggedResponse::Fetch(fr) = *boxed {
                panic!(
                    "EMAILID NIL must not be silently accepted as FetchResponse \
                         per RFC 8474 Section 7, got: {fr:?}"
                );
            }
            // Fell through to Unknown or other non-Fetch variant  -  acceptable.
        }
        Ok((_, other)) => {
            // Tagged or continuation  -  fine, not a Fetch.
            let _ = other;
        }
    }
}

/// RFC 8474 Section 7: EMAILID with valid `(objectid)` must still parse.
#[test]
fn fetch_emailid_valid_objectid() {
    let input = b"* 1 FETCH (EMAILID (V001))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some("V001"));
        return;
    }
    panic!("expected Fetch response with EMAILID (V001)");
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

#[test]
fn status_item_mailboxid() {
    let (_, items) = status_items(b"MAILBOXID (F2212ea87-6097-4256)").unwrap();
    assert_eq!(
        items,
        vec![StatusItem::MailboxId("F2212ea87-6097-4256".into())]
    );
}

// ===== OBJECTID strict parser (RFC 8474 Section 7) tests =====

/// RFC 8474 Section 7: `objectid = 1*255(ALPHA / DIGIT / "_" / "-")`
/// The objectid parser must accept valid characters.
#[test]
fn objectid_valid_chars() {
    let (rest, val) = objectid(b"Abc_123-xyz rest").unwrap();
    assert_eq!(val, b"Abc_123-xyz");
    assert_eq!(rest, b" rest");
}

/// RFC 8474 Section 7: objectid must reject empty input.
#[test]
fn objectid_empty_fails() {
    assert!(objectid(b" rest").is_err());
}

/// RFC 8474 Section 7: objectid has a 255-character maximum.
/// Exactly 255 characters must be accepted.
#[test]
fn objectid_exactly_255_chars() {
    let mut input: Vec<u8> = std::iter::repeat_n(b'A', 255).collect();
    input.push(b')'); // delimiter after objectid
    let (rest, val) = objectid(&input).unwrap();
    assert_eq!(val.len(), 255);
    assert_eq!(rest, b")");
}

/// RFC 8474 Section 7 / Postel's law: objectid > 255 characters must be
/// accepted in full via atom fallback. Truncating to 255 would leave
/// leftover bytes in the input stream, poisoning downstream parsers.
#[test]
fn objectid_256_chars_accepted_in_full() {
    let mut input: Vec<u8> = std::iter::repeat_n(b'A', 256).collect();
    input.push(b')');
    let (rest, val) = objectid(&input).unwrap();
    // Must accept the full 256-char atom, not truncate to 255
    assert_eq!(val.len(), 256);
    assert_eq!(rest, b")");
}

/// RFC 8474 Section 7 / Postel's law: an objectid > 255 characters should
/// be accepted in full (via atom fallback) rather than truncated. Truncation
/// poisons the input stream  -  the leftover atom bytes cause downstream
/// parsers (e.g., the closing `)` in EMAILID) to fail.
#[test]
fn objectid_oversized_accepted_via_postel() {
    let mut input: Vec<u8> = std::iter::repeat_n(b'A', 300).collect();
    input.push(b')');
    let (rest, val) = objectid(&input).unwrap();
    // Must accept the full 300-char value, not truncate to 255
    assert_eq!(
        val.len(),
        300,
        "oversized objectid must be accepted in full"
    );
    assert_eq!(
        rest, b")",
        "rest must be the delimiter, not leftover atom bytes"
    );
}

/// Regression: EMAILID FETCH response with a >255-char objectid must parse
/// successfully. Previously, the objectid parser truncated at 255 characters
/// and returned the excess to the input stream, causing the closing ')'
/// parse to fail (RFC 8474 Section 7, Postel's law).
#[test]
fn fetch_emailid_oversized_objectid_parses() {
    let long_id: String = "A".repeat(300);
    let input = format!("* 1 FETCH (EMAILID ({long_id}))\r\n");
    let (_, resp) = parse_response(input.as_bytes()).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some(long_id.as_str()));
        return;
    }
    panic!("expected Fetch response with EMAILID");
}

/// RFC 8474 Section 7: objectid with characters outside the restricted set
/// should still parse via atom fallback (Postel's law).
#[test]
fn objectid_non_compliant_falls_back_to_atom() {
    // Dot '.' is not in the RFC 8474 objectid charset but is a valid atom char.
    let (rest, val) = objectid(b"F2212ea87.6097 rest").unwrap();
    assert_eq!(val, b"F2212ea87.6097");
    assert_eq!(rest, b" rest");
}

/// EMAILID FETCH response with a valid objectid per RFC 8474 Section 7.
#[test]
fn fetch_emailid_rfc8474_valid_objectid() {
    let input = b"* 1 FETCH (EMAILID (M_abc-XYZ_123))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.email_id.as_deref(), Some("M_abc-XYZ_123"));
        return;
    }
    panic!("expected Fetch response with EMAILID");
}

/// THREADID FETCH response with a valid objectid per RFC 8474 Section 7.
#[test]
fn fetch_threadid_rfc8474_valid_objectid() {
    let input = b"* 1 FETCH (THREADID (T_thread-99))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.thread_id.as_deref(), Some("T_thread-99"));
        return;
    }
    panic!("expected Fetch response with THREADID");
}

/// STATUS MAILBOXID response with a valid objectid per RFC 8474 Section 7.
#[test]
fn status_mailboxid_rfc8474_valid_objectid() {
    let (_, items) = status_items(b"MAILBOXID (F_box-42)").unwrap();
    assert_eq!(items, vec![StatusItem::MailboxId("F_box-42".into())]);
}

/// `ResponseCode` MAILBOXID with a valid objectid per RFC 8474 Section 7.
#[test]
fn response_code_mailboxid_rfc8474_valid_objectid() {
    let (_, code) = response_code(b"[MAILBOXID (M_id-test_123)]").unwrap();
    assert_eq!(code, ResponseCode::MailboxId("M_id-test_123".into()));
}

// ===== METADATA (RFC 5464) parser tests =====

#[test]
fn parse_metadata_response() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" \"My comment\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"My comment".as_slice()));
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_multiple_entries() {
    let input =
        b"* METADATA \"INBOX\" (\"/private/comment\" \"hello\" \"/shared/vendor/x\" \"world\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(entries[1].name, "/shared/vendor/x");
        assert_eq!(entries[1].value.as_deref(), Some(b"world".as_slice()));
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_nil_value() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert!(entries[0].value.is_none());
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_literal_value() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" {5}\r\nhello)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"hello".as_slice()));
        return;
    }
    panic!("expected Metadata response");
}

#[test]
fn parse_metadata_empty_entries() {
    let input = b"* METADATA \"INBOX\" ()\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(entries.is_empty());
        return;
    }
    panic!("expected Metadata response");
}

/// RFC 5464 Section 5: value = nstring / literal8
/// literal8 can contain arbitrary binary data including non-UTF-8 bytes.
/// MetadataEntry.value must be `Option<Vec<u8>>` to preserve binary fidelity.
#[test]
fn parse_metadata_binary_literal8_value() {
    let input = b"* METADATA \"INBOX\" (\"/private/vendor/bin\" ~{4}\r\n\x80\x81\x82\x83)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/vendor/bin");
        assert_eq!(
            entries[0].value.as_deref(),
            Some(b"\x80\x81\x82\x83".as_slice())
        );
        return;
    }
    panic!("expected Metadata response with binary value");
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
    assert_eq!(cap, Capability::Thread("REFERENCES".into()));
}

#[test]
fn capability_thread_orderedsubject() {
    let (_, cap) = capability(b"THREAD=ORDEREDSUBJECT").unwrap();
    assert_eq!(cap, Capability::Thread("ORDEREDSUBJECT".into()));
}

#[test]
fn parse_thread_simple() {
    // Simple linear thread: (1 2 3)
    let input = b"* THREAD (1 2 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 1);
        assert_eq!(threads[0].children[0].id, Some(2));
        assert_eq!(threads[0].children[0].children.len(), 1);
        assert_eq!(threads[0].children[0].children[0].id, Some(3));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_multiple_roots() {
    // Two separate threads: (1)(2)
    let input = b"* THREAD (1)(2)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 2);
        assert_eq!(threads[0].id, Some(1));
        assert!(threads[0].children.is_empty());
        assert_eq!(threads[1].id, Some(2));
        assert!(threads[1].children.is_empty());
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_nested() {
    // Thread with branching: (1 (2)(3))
    let input = b"* THREAD (1 (2)(3))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 2);
        assert_eq!(threads[0].children[0].id, Some(2));
        assert_eq!(threads[0].children[1].id, Some(3));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_dummy_parent() {
    // Dummy parent (no UID): ((1)(2))
    let input = b"* THREAD ((1)(2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        // Dummy parent has id=None (RFC 5256 Section 4)
        assert_eq!(threads[0].id, None);
        assert_eq!(threads[0].children.len(), 2);
        assert_eq!(threads[0].children[0].id, Some(1));
        assert_eq!(threads[0].children[1].id, Some(2));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_empty() {
    // Empty thread response
    let input = b"* THREAD\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert!(threads.is_empty());
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_complex() {
    // Complex: (1 (2 3)(4 5 6))
    let input = b"* THREAD (1 (2 3)(4 5 6))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 2);
        // First child: (2 3)  -  2 with child 3
        assert_eq!(threads[0].children[0].id, Some(2));
        assert_eq!(threads[0].children[0].children.len(), 1);
        assert_eq!(threads[0].children[0].children[0].id, Some(3));
        // Second child: (4 5 6)  -  4 with child 5 with child 6
        assert_eq!(threads[0].children[1].id, Some(4));
        assert_eq!(threads[0].children[1].children.len(), 1);
        assert_eq!(threads[0].children[1].children[0].id, Some(5));
        assert_eq!(threads[0].children[1].children[0].children.len(), 1);
        assert_eq!(threads[0].children[1].children[0].children[0].id, Some(6));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_rfc_example() {
    // Example from RFC 5256 Section 4: (2)(3 6 (4 23)(44 7 96))
    let input = b"* THREAD (2)(3 6 (4 23)(44 7 96))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 2);
        // First thread: just message 2
        assert_eq!(threads[0].id, Some(2));
        assert!(threads[0].children.is_empty());
        // Second thread: 3 -> 6 -> [(4 -> 23), (44 -> 7 -> 96)]
        assert_eq!(threads[1].id, Some(3));
        assert_eq!(threads[1].children.len(), 1);
        assert_eq!(threads[1].children[0].id, Some(6));
        assert_eq!(threads[1].children[0].children.len(), 2);
        // Branch 1: (4 23)
        assert_eq!(threads[1].children[0].children[0].id, Some(4));
        assert_eq!(threads[1].children[0].children[0].children.len(), 1);
        assert_eq!(threads[1].children[0].children[0].children[0].id, Some(23));
        // Branch 2: (44 7 96)
        assert_eq!(threads[1].children[0].children[1].id, Some(44));
        assert_eq!(threads[1].children[0].children[1].children.len(), 1);
        assert_eq!(threads[1].children[0].children[1].children[0].id, Some(7));
        assert_eq!(
            threads[1].children[0].children[1].children[0]
                .children
                .len(),
            1
        );
        assert_eq!(
            threads[1].children[0].children[1].children[0].children[0].id,
            Some(96)
        );
        return;
    }
    panic!("expected Thread response");
}

/// RFC 5256 Section 4: dummy parents (empty parentheses with no UID)
/// must be distinguishable from real messages. Using `Option<u32>`
/// (None for dummy) is more precise than the u32/0 sentinel.
#[test]
fn spec_audit_thread_dummy_parent_uses_option() {
    // Thread with dummy parent: ((1)(2))
    let input = b"* THREAD ((1)(2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        // Dummy parent must have id == None per RFC 5256 Section 4
        assert_eq!(
            threads[0].id, None,
            "RFC 5256 Section 4: dummy parent has no UID, expected None, got {:?}",
            threads[0].id
        );
        assert_eq!(threads[0].children.len(), 2);
        assert_eq!(threads[0].children[0].id, Some(1));
        assert_eq!(threads[0].children[1].id, Some(2));
        return;
    }
    panic!("expected Thread response");
}

/// RFC 5256 Section 5: `thread-nested = 2*thread-list` requires at least 2
/// groups inside a dummy parent.  `((1))` has only 1 group, making it
/// syntactically invalid.  Per Postel's law (RFC 1122 Section 1.2.2) we
/// accept the input, but collapse the unnecessary dummy parent so the
/// result is equivalent to the valid form `(1)`.
#[test]
fn spec_audit_thread_single_child_dummy_collapsed() {
    let input = b"* THREAD ((1))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1, "should have one top-level thread");
        // After collapsing, the thread should be a direct message
        // (id=Some(1)), NOT a dummy parent wrapping one child.
        assert_eq!(
            threads[0].id,
            Some(1),
            "RFC 5256 Section 5: single-child dummy parent must be \
                 collapsed  -  expected id=Some(1), got id={:?}",
            threads[0].id
        );
        assert!(
            threads[0].children.is_empty(),
            "collapsed thread should have no children"
        );
        return;
    }
    panic!("expected Thread response");
}

/// Same as above but with a chain inside: `((1 2))` should collapse to
/// `ThreadNode { id: 1, children: [ThreadNode { id: 2 }] }`.
#[test]
fn spec_audit_thread_single_child_dummy_collapsed_chain() {
    let input = b"* THREAD ((1 2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(
            threads[0].id,
            Some(1),
            "RFC 5256 Section 5: single-child dummy parent must be \
                 collapsed  -  expected id=Some(1), got id={:?}",
            threads[0].id
        );
        assert_eq!(threads[0].children.len(), 1);
        assert_eq!(threads[0].children[0].id, Some(2));
        return;
    }
    panic!("expected Thread response");
}

/// Dummy parent with 2+ children is valid (`thread-nested = 2*thread-list`)
/// and must NOT be collapsed.
#[test]
fn spec_audit_thread_multi_child_dummy_not_collapsed() {
    let input = b"* THREAD ((1)(2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(
            threads[0].id, None,
            "valid dummy parent (2+ children) must keep id=None"
        );
        assert_eq!(threads[0].children.len(), 2);
        return;
    }
    panic!("expected Thread response");
}

// ===== QUOTA (RFC 2087) parser tests =====

#[test]
fn parse_quota_single_resource() {
    let input = b"* QUOTA \"\" (STORAGE 10 512)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 10);
        assert_eq!(resources[0].limit, 512);
        return;
    }
    panic!("expected Quota response");
}

#[test]
fn parse_quota_multiple_resources() {
    let input = b"* QUOTA \"user.alice\" (STORAGE 100 1024 MESSAGE 50 500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "user.alice");
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 100);
        assert_eq!(resources[0].limit, 1024);
        assert_eq!(resources[1].name, "MESSAGE");
        assert_eq!(resources[1].usage, 50);
        assert_eq!(resources[1].limit, 500);
        return;
    }
    panic!("expected Quota response with multiple resources");
}

#[test]
fn parse_quota_empty_resource_list() {
    let input = b"* QUOTA \"\" ()\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "");
        assert!(resources.is_empty());
        return;
    }
    panic!("expected Quota response with empty resources");
}

#[test]
fn parse_quotaroot_single_root() {
    let input = b"* QUOTAROOT INBOX \"\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec![""]);
        return;
    }
    panic!("expected QuotaRoot response");
}

#[test]
fn parse_quotaroot_multiple_roots() {
    let input = b"* QUOTAROOT INBOX \"\" \"user.bob\"\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec!["", "user.bob"]);
        return;
    }
    panic!("expected QuotaRoot response with multiple roots");
}

#[test]
fn parse_quotaroot_no_roots() {
    let input = b"* QUOTAROOT INBOX\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(roots.is_empty());
        return;
    }
    panic!("expected QuotaRoot response with no roots");
}

#[test]
fn parse_quota_case_insensitive() {
    let input = b"* quota \"\" (STORAGE 5 100)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Quota { root, resources } = *boxed
    {
        assert_eq!(root, "");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name, "STORAGE");
        return;
    }
    panic!("expected case-insensitive Quota response");
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
        assert!(caps.contains(&Capability::QuotaResource("STORAGE".into())));
        return;
    }
    panic!("expected QUOTA=RES-STORAGE capability");
}

// ===== ACL (RFC 4314) parser tests =====

#[test]
fn parse_acl_single_entry() {
    let input = b"* ACL INBOX fred lrswipcda\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].identifier, "fred");
        assert_eq!(entries[0].rights, "lrswipcda");
        return;
    }
    panic!("expected ACL response");
}

#[test]
fn parse_acl_multiple_entries() {
    let input = b"* ACL INBOX fred lrswipcda chris lrs\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].identifier, "fred");
        assert_eq!(entries[0].rights, "lrswipcda");
        assert_eq!(entries[1].identifier, "chris");
        assert_eq!(entries[1].rights, "lrs");
        return;
    }
    panic!("expected ACL response with multiple entries");
}

#[test]
fn parse_acl_no_entries() {
    let input = b"* ACL INBOX\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(entries.is_empty());
        return;
    }
    panic!("expected ACL response with no entries");
}

#[test]
fn parse_myrights() {
    let input = b"* MYRIGHTS INBOX lrswipcda\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::MyRights { mailbox, rights } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(rights, "lrswipcda");
        return;
    }
    panic!("expected MYRIGHTS response");
}

#[test]
fn parse_myrights_quoted_mailbox() {
    let input = b"* MYRIGHTS \"Sent Items\" lrs\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::MyRights { mailbox, rights } = *boxed
    {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert_eq!(rights, "lrs");
        return;
    }
    panic!("expected MYRIGHTS response with quoted mailbox");
}

#[test]
fn parse_listrights_with_optional() {
    let input = b"* LISTRIGHTS INBOX fred lr s w i p c d a\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::ListRights {
            mailbox,
            identifier,
            required,
            optional,
        } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(identifier, "fred");
        assert_eq!(required, "lr");
        assert_eq!(optional, vec!["s", "w", "i", "p", "c", "d", "a"]);
        return;
    }
    panic!("expected LISTRIGHTS response");
}

#[test]
fn parse_listrights_no_optional() {
    let input = b"* LISTRIGHTS INBOX fred lrswipcda\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::ListRights {
            mailbox,
            identifier,
            required,
            optional,
        } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(identifier, "fred");
        assert_eq!(required, "lrswipcda");
        assert!(optional.is_empty());
        return;
    }
    panic!("expected LISTRIGHTS response with no optional");
}

#[test]
fn parse_acl_case_insensitive() {
    let input = b"* acl INBOX alice lrs\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Acl { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].identifier, "alice");
        return;
    }
    panic!("expected case-insensitive ACL response");
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

/// RFC 4314 Section 6: RIGHTS= is case-insensitive but the rights chars are preserved.
#[test]
fn capability_rights_case_insensitive() {
    let (_, cap) = capability(b"rights=TEXK").unwrap();
    match cap {
        Capability::Rights(rights) => assert_eq!(rights, "TEXK"),
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

// ===== Parser edge case tests =====

/// Deeply nested multipart BODYSTRUCTURE (3 levels deep).
#[test]
fn body_structure_deeply_nested_multipart() {
    // multipart/mixed containing multipart/alternative containing text/plain + text/html
    let input = b"* 1 FETCH (BODYSTRUCTURE \
            (((\"text\" \"plain\" (\"charset\" \"utf-8\") NIL NIL \"7bit\" 100 5 NIL NIL NIL NIL)\
            (\"text\" \"html\" (\"charset\" \"utf-8\") NIL NIL \"quoted-printable\" 200 10 NIL NIL NIL NIL) \
            \"alternative\" NIL NIL NIL NIL)\
            (\"application\" \"pdf\" (\"name\" \"doc.pdf\") NIL NIL \"base64\" 5000 NIL NIL NIL NIL) \
            \"mixed\" NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        let body = fetch.body_structure.expect("missing BODYSTRUCTURE");
        if let BodyStructure::Multipart {
            media_subtype,
            bodies,
            ..
        } = &body
        {
            assert!(media_subtype.eq_ignore_ascii_case("mixed"));
            assert_eq!(bodies.len(), 2);
            // First part is multipart/alternative
            if let BodyStructure::Multipart {
                media_subtype: inner_sub,
                bodies: inner_parts,
                ..
            } = &bodies[0]
            {
                assert!(inner_sub.eq_ignore_ascii_case("alternative"));
                assert_eq!(inner_parts.len(), 2);
            } else {
                panic!("expected inner multipart/alternative");
            }
            // Second part is application/pdf
            if let BodyStructure::Basic {
                media_type,
                media_subtype,
                ..
            } = &bodies[1]
            {
                assert!(media_type.eq_ignore_ascii_case("application"));
                assert!(media_subtype.eq_ignore_ascii_case("pdf"));
            } else {
                panic!("expected application/pdf");
            }
            return;
        }
    }
    panic!("expected nested multipart BODYSTRUCTURE");
}

/// message/rfc822 BODYSTRUCTURE with nested envelope and body.
#[test]
fn body_structure_message_rfc822_nested() {
    let input = b"* 1 FETCH (BODYSTRUCTURE \
            (\"message\" \"rfc822\" NIL NIL NIL \"7bit\" 1000 \
            (\"Mon, 01 Jan 2024 00:00:00 +0000\" \"Inner Subject\" \
            ((\"Sender\" NIL \"sender\" \"example.com\")) \
            NIL NIL \
            ((\"Recipient\" NIL \"rcpt\" \"example.com\")) \
            NIL NIL NIL \"<inner@example.com>\") \
            (\"text\" \"plain\" (\"charset\" \"us-ascii\") NIL NIL \"7bit\" 50 3 NIL NIL NIL NIL) \
            20 NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        let body = fetch.body_structure.expect("missing BODYSTRUCTURE");
        if let BodyStructure::Message { envelope, body, .. } = &body {
            assert_eq!(envelope.subject, Some("Inner Subject".into()));
            assert_eq!(envelope.message_id, Some("<inner@example.com>".into()));
            // Nested body is text/plain
            if let BodyStructure::Text { media_subtype, .. } = body.as_ref() {
                assert!(media_subtype.eq_ignore_ascii_case("plain"));
            } else {
                panic!("expected text/plain in nested body");
            }
            return;
        }
    }
    panic!("expected message/rfc822 BODYSTRUCTURE");
}

/// FETCH with BODY section partial origin (RFC 3501 Section 7.4.2).
#[test]
fn fetch_body_partial_with_origin() {
    let input = b"* 1 FETCH (BODY[]<0> {10}\r\n0123456789)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        assert_eq!(fetch.body_sections.len(), 1);
        let sec = &fetch.body_sections[0];
        assert_eq!(sec.origin, Some(0));
        assert_eq!(sec.data.as_deref(), Some(b"0123456789".as_slice()));
        return;
    }
    panic!("expected FETCH with partial origin");
}

/// SEARCH response with no results.
#[test]
fn search_empty_result() {
    let input = b"* SEARCH\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Search { uids, .. } = *boxed
    {
        assert!(uids.is_empty());
        return;
    }
    panic!("expected empty SEARCH");
}

/// SEARCH response with multiple UIDs.
#[test]
fn search_multiple_uids() {
    let input = b"* SEARCH 1 5 10 42\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Search { uids, .. } = *boxed
    {
        assert_eq!(uids, vec![1, 5, 10, 42]);
        return;
    }
    panic!("expected SEARCH with UIDs");
}

/// ESEARCH with ALL expands uid-set into individual UIDs.
#[test]
fn esearch_all_preserves_ranges() {
    let input = b"* ESEARCH (TAG \"A001\") ALL 1:3,5,10:12\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert_eq!(esearch.tag.as_deref(), Some("A001"));
        assert!(!esearch.uid);
        assert_eq!(
            esearch.all,
            vec![
                UidRange::range(1, 3),
                UidRange::single(5),
                UidRange::range(10, 12),
            ]
        );
        return;
    }
    panic!("expected ESEARCH with ALL ranges");
}

/// UID range parsing: single UID.
#[test]
fn uid_range_single_value() {
    let (_, ranges) = uid_set(b"42").unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].start, 42);
    assert_eq!(ranges[0].end, None);
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
        assert_eq!(uids[0].start, 100);
        assert_eq!(uids[1].start, 101);
        assert_eq!(uids[2].start, 102);
    } else {
        panic!("expected AppendUid");
    }
}

/// Truncated envelope should fail gracefully.
#[test]
fn envelope_truncated_fails() {
    // Only date and subject, then end of input
    let input = b"(\"Mon, 01 Jan 2024\" \"Subject\"";
    assert!(envelope(input, false).is_err());
}

/// BODYSTRUCTURE with no extension data (minimal text/plain).
#[test]
fn body_structure_minimal_text() {
    let input = b"(\"text\" \"plain\" (\"charset\" \"us-ascii\") NIL NIL \"7bit\" 42 3)";
    let (_, body) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        size,
        lines,
        ..
    } = body
    {
        assert!(media_subtype.eq_ignore_ascii_case("plain"));
        assert_eq!(size, 42);
        assert_eq!(lines, 3);
    } else {
        panic!("expected Text body");
    }
}

/// OK response with no response code, just text.
#[test]
fn tagged_ok_no_code() {
    let input = b"A001 OK NOOP completed\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.tag, "A001");
        assert_eq!(tagged.status, StatusKind::Ok);
        assert!(tagged.code.is_none());
        assert_eq!(tagged.text, "NOOP completed");
    } else {
        panic!("expected tagged response");
    }
}

/// BAD response with text.
#[test]
fn tagged_bad_response() {
    let input = b"A001 BAD Command syntax error\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::Bad);
        assert_eq!(tagged.text, "Command syntax error");
    } else {
        panic!("expected tagged BAD");
    }
}

/// Untagged BYE with response text.
#[test]
fn untagged_bye_with_text() {
    let input = b"* BYE Server shutting down\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Status { status, text, .. } = &*boxed
    {
        assert_eq!(*status, UntaggedStatus::Bye);
        assert_eq!(text, "Server shutting down");
        return;
    }
    panic!("expected BYE");
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

// ===== Address group syntax tests (RFC 3501 Section 7.4.2 / RFC 5322 Section 3.4) =====

/// Group start marker: host NIL, mailbox holds group name.
#[test]
fn address_group_start() {
    let input = b"(NIL NIL \"Friends\" NIL)";
    let (_, addr) = address(input, false).unwrap();
    assert!(addr.is_group_start());
    assert!(!addr.is_group_end());
    assert_eq!(addr.mailbox.as_deref(), Some("Friends"));
    assert!(addr.host.is_none());
}

/// Group end marker: both host and mailbox NIL.
#[test]
fn address_group_end() {
    let input = b"(NIL NIL NIL NIL)";
    let (_, addr) = address(input, false).unwrap();
    assert!(addr.is_group_end());
    assert!(!addr.is_group_start());
}

/// Address list with group markers wrapping real addresses.
#[test]
fn address_list_with_group() {
    // RFC 3501 Section 7.4.2: group is encoded as start marker, members, end marker.
    let input =
        b"((NIL NIL \"Team\" NIL)(\"Alice\" NIL \"alice\" \"example.com\")(NIL NIL NIL NIL))";
    let (_, addrs) = address_list(input, false).unwrap();
    assert_eq!(addrs.len(), 3);
    assert!(addrs[0].is_group_start());
    assert_eq!(addrs[0].mailbox.as_deref(), Some("Team"));
    assert!(addrs[1].is_address());
    assert_eq!(addrs[1].email(), Some("alice@example.com".into()));
    assert!(addrs[2].is_group_end());
}

// ===== FETCH attribute case insensitivity tests =====

/// FETCH attributes in lowercase (RFC 3501 Section 7.4.2).
#[test]
fn fetch_lowercase_attributes() {
    let input = b"(uid 42 flags (\\Seen) rfc822.size 1024)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(42));
    assert_eq!(fr.flags, Some(vec![Flag::Seen]));
    assert_eq!(fr.rfc822_size, Some(1024));
}

/// FETCH attributes in mixed case.
#[test]
fn fetch_mixed_case_attributes() {
    let input = b"(Uid 99 Flags (\\Deleted \\Draft) Rfc822.Size 2048)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(99));
    assert_eq!(fr.flags, Some(vec![Flag::Deleted, Flag::Draft]));
    assert_eq!(fr.rfc822_size, Some(2048));
}

/// BODYSTRUCTURE keyword in lowercase.
#[test]
fn fetch_lowercase_bodystructure() {
    let input =
        b"(bodystructure (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert!(fr.body_structure.is_some());
}

/// MODSEQ in lowercase.
#[test]
fn fetch_lowercase_modseq() {
    let input = b"(modseq (12345))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.mod_seq, Some(12345));
}

/// INTERNALDATE in lowercase.
#[test]
fn fetch_lowercase_internaldate() {
    let input = b"(internaldate \"17-Jul-1996 02:44:25 -0700\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(
        fr.internal_date.as_deref(),
        Some("17-Jul-1996 02:44:25 -0700")
    );
}

// ===== BODYSTRUCTURE partial extension data tests =====

/// Single-part body with only MD5 extension (no disposition/language/location).
#[test]
fn body_structure_ext_md5_only() {
    // TEXT body with md5, but no disposition/language/location.
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5 \"abc123\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text {
        md5, disposition, ..
    } = bs
    {
        assert_eq!(md5.as_deref(), Some("abc123"));
        assert!(disposition.is_none());
    } else {
        panic!("expected Text body");
    }
}

/// Single-part body with MD5 and disposition, but no language/location.
#[test]
fn body_structure_ext_md5_and_disposition() {
    let input =
        b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5 NIL (\"inline\" NIL))";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text {
        md5,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert!(md5.is_none());
        let disp = disposition.expect("disposition should be present");
        assert_eq!(disp.disposition_type, "inline");
        assert!(language.is_none());
        assert!(location.is_none());
    } else {
        panic!("expected Text body");
    }
}

/// Single-part body with MD5, disposition, and language, but no location.
#[test]
fn body_structure_ext_through_language() {
    let input = b"(\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5 NIL NIL (\"en\" \"fr\"))";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text {
        language, location, ..
    } = bs
    {
        let langs = language.expect("language should be present");
        assert_eq!(langs, vec!["en", "fr"]);
        assert!(location.is_none());
    } else {
        panic!("expected Text body");
    }
}

/// Multipart body with only params extension (no disposition/language/location).
#[test]
fn body_structure_mpart_ext_params_only() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 80 4) \"ALTERNATIVE\" (\"BOUNDARY\" \"----=_Part\"))";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        params,
        disposition,
        ..
    } = bs
    {
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "boundary");
        assert!(disposition.is_none());
    } else {
        panic!("expected Multipart body");
    }
}

/// Multipart body with no extension data at all.
#[test]
fn body_structure_mpart_no_extension() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) \"MIXED\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        params,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert!(params.is_empty());
        assert!(disposition.is_none());
        assert!(language.is_none());
        assert!(location.is_none());
    } else {
        panic!("expected Multipart body");
    }
}

/// RFC 3501 Section 9 requires `1*body` for `body-type-mpart`, meaning at
/// least one child body must be present.  Passing input that skips the
/// nested `(` bodies must be rejected as a parse error.
#[test]
fn body_type_mpart_rejects_zero_children() {
    // Input has no child bodies  -  starts directly with SP + subtype.
    // This violates the `1*body` ABNF rule (RFC 3501 Section 9).
    let input = b" \"MIXED\" NIL NIL NIL NIL)";
    let result = body_type_mpart(input, false, 0);
    assert!(
        result.is_err(),
        "body_type_mpart must reject zero children per RFC 3501 Section 9 (1*body)"
    );
}

// ===== Multiple unknown FETCH attributes =====

/// Multiple consecutive unknown attributes should be skipped gracefully.
#[test]
fn fetch_multiple_unknown_attributes_skipped() {
    let input = b"(UID 42 X-CUSTOM1 \"value1\" X-CUSTOM2 (nested data) FLAGS (\\Seen))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(42));
    assert_eq!(fr.flags, Some(vec![Flag::Seen]));
}

/// Unknown attribute with NIL value.
#[test]
fn fetch_unknown_attribute_nil_value() {
    let input = b"(UID 10 X-UNKNOWN NIL FLAGS ())";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(10));
    assert_eq!(fr.flags, Some(vec![]));
}

/// Unknown attribute with literal value.
#[test]
fn fetch_unknown_attribute_literal_value() {
    let input = b"(UID 7 X-DATA {5}\r\nhello FLAGS (\\Flagged))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(7));
    assert_eq!(fr.flags, Some(vec![Flag::Flagged]));
}

// ===== Large literal tests =====

/// Literal with a large byte count parses correctly.
#[test]
fn literal_large_payload() {
    let size = 100_000;
    let mut input = format!("{{{size}}}\r\n").into_bytes();
    input.extend(vec![b'X'; size]);
    let (rest, val) = literal(&input).unwrap();
    assert_eq!(val.len(), size);
    assert!(rest.is_empty());
}

/// Literal count at `u32::MAX` boundary is accepted by the size parser but
/// rejected because there isn't enough trailing data (Incomplete/Error).
#[test]
fn literal_u32_max_count_insufficient_data() {
    let input = b"{4294967295}\r\nshort";
    assert!(literal(input).is_err());
}

/// Literal count exceeding u32 range fails to parse.
#[test]
fn literal_count_overflow() {
    let input = b"{99999999999}\r\ndata";
    assert!(literal(input).is_err());
}

// ===== Malformed tagged response tests =====

/// Tagged response with no space between tag and status.
#[test]
fn tagged_response_no_space_after_tag() {
    assert!(parse_response(b"A001OK done\r\n").is_err());
}

/// Tagged response with empty/missing status.
#[test]
fn tagged_response_empty_status() {
    assert!(parse_response(b"A001  done\r\n").is_err());
}

/// Tag starting with non-atom char is not a valid tagged response.
#[test]
fn tagged_response_invalid_tag_char() {
    // '{' is not an atom char, so this can't be parsed as tagged
    assert!(parse_response(b"{BAD} OK done\r\n").is_err());
}

/// Tagged response with only a tag and status, no text after status.
///
/// Many servers send bare `"A001 OK\r\n"` without trailing text.
/// We tolerate this per Postel's law (RFC 1122 Section 1.2.2), even though
/// RFC 3501 Section 7.1 formally requires SP resp-text.
#[test]
fn tagged_response_no_text_after_status() {
    let (rem, resp) = parse_response(b"A001 OK\r\n").unwrap();
    assert!(rem.is_empty());
    if let Response::Tagged(t) = resp {
        assert_eq!(t.tag, "A001");
        assert_eq!(t.status, StatusKind::Ok);
        assert!(t.code.is_none());
        assert!(t.text.is_empty());
    } else {
        panic!("expected Tagged response, got {resp:?}");
    }
}

/// Completely empty input fails.
#[test]
fn parse_response_empty_input() {
    assert!(parse_response(b"").is_err());
}

/// Garbage data with special characters fails to parse.
#[test]
fn parse_response_garbage_special_chars() {
    assert!(parse_response(b"!@#$%^&\r\n").is_err());
}

/// Tagged response with bare LF instead of CRLF.
#[test]
fn tagged_response_bare_lf() {
    assert!(parse_response(b"A001 OK done\n").is_err());
}

// ===== Task 1: QUOTA/QUOTAROOT parser tests (RFC 2087 Section 5.1-5.2) =====

/// QUOTA with a named root and a single resource triplet (RFC 2087 Section 5.1).
#[test]
fn quota_named_root_single_resource() {
    let input = b"QUOTA \"user.alice\" (STORAGE 200 1024)\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { root, resources } = resp {
        assert_eq!(root, "user.alice");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 200);
        assert_eq!(resources[0].limit, 1024);
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with multiple resource triplets (RFC 2087 Section 5.1).
#[test]
fn quota_multiple_resources_triplets() {
    let input = b"QUOTA \"\" (STORAGE 500 2048 MESSAGE 100 1000 MAILBOX 5 10)\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { root, resources } = resp {
        assert_eq!(root, "");
        assert_eq!(resources.len(), 3);
        assert_eq!(resources[0].name, "STORAGE");
        assert_eq!(resources[0].usage, 500);
        assert_eq!(resources[0].limit, 2048);
        assert_eq!(resources[1].name, "MESSAGE");
        assert_eq!(resources[1].usage, 100);
        assert_eq!(resources[1].limit, 1000);
        assert_eq!(resources[2].name, "MAILBOX");
        assert_eq!(resources[2].usage, 5);
        assert_eq!(resources[2].limit, 10);
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with an empty resource list (RFC 2087 Section 5.1).
#[test]
fn quota_empty_resources() {
    let input = b"QUOTA \"root\" ()\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { root, resources } = resp {
        assert_eq!(root, "root");
        assert!(resources.is_empty());
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with large 64-bit usage/limit values (RFC 2087 Section 5.1).
#[test]
fn quota_large_values() {
    let input = b"QUOTA \"\" (STORAGE 4294967296 8589934592)\r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    if let UntaggedResponse::Quota { resources, .. } = resp {
        assert_eq!(resources[0].usage, 4_294_967_296);
        assert_eq!(resources[0].limit, 8_589_934_592);
    } else {
        panic!("expected Quota, got {resp:?}");
    }
}

/// QUOTA with truncated input should fail (RFC 2087 Section 5.1).
#[test]
fn quota_truncated_input() {
    // Missing CRLF
    assert!(parse_untagged_quota(b"QUOTA \"\" (STORAGE 10 512)").is_err());
    // Missing closing paren of resource list
    assert!(parse_untagged_quota(b"QUOTA \"\" (STORAGE 10 512\r\n").is_err());
    // Missing resource values
    assert!(parse_untagged_quota(b"QUOTA \"\" (STORAGE\r\n").is_err());
}

/// QUOTA with garbage data should fail (RFC 2087 Section 5.1).
#[test]
fn quota_garbage_data() {
    assert!(parse_untagged_quota(b"QUOTA !!!GARBAGE!!!\r\n").is_err());
}

/// QUOTAROOT with a single root (RFC 2087 Section 5.2).
#[test]
fn quotaroot_single_root_atom() {
    // Mailbox as atom, root as quoted string
    let input = b"QUOTAROOT INBOX \"root\"\r\n";
    let (_, resp) = parse_untagged_quotaroot(input, false).unwrap();
    if let UntaggedResponse::QuotaRoot { mailbox, roots } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec!["root"]);
    } else {
        panic!("expected QuotaRoot, got {resp:?}");
    }
}

/// QUOTAROOT with multiple roots (RFC 2087 Section 5.2).
#[test]
fn quotaroot_multiple_roots_list() {
    let input = b"QUOTAROOT INBOX \"root1\" \"root2\" \"root3\"\r\n";
    let (_, resp) = parse_untagged_quotaroot(input, false).unwrap();
    if let UntaggedResponse::QuotaRoot { mailbox, roots } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(roots, vec!["root1", "root2", "root3"]);
    } else {
        panic!("expected QuotaRoot, got {resp:?}");
    }
}

/// QUOTAROOT with no roots  -  mailbox has no quota roots (RFC 2087 Section 5.2).
#[test]
fn quotaroot_no_roots_at_all() {
    let input = b"QUOTAROOT \"Sent Items\"\r\n";
    let (_, resp) = parse_untagged_quotaroot(input, false).unwrap();
    if let UntaggedResponse::QuotaRoot { mailbox, roots } = resp {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert!(roots.is_empty());
    } else {
        panic!("expected QuotaRoot, got {resp:?}");
    }
}

/// QUOTAROOT with truncated input should fail (RFC 2087 Section 5.2).
#[test]
fn quotaroot_truncated_input() {
    // Missing CRLF after mailbox
    assert!(parse_untagged_quotaroot(b"QUOTAROOT INBOX", false).is_err());
}

// ===== Task 2: ACL/LISTRIGHTS/MYRIGHTS parser tests (RFC 4314 Section 3.6-3.8) =====

/// ACL with a single entry and quoted mailbox (RFC 4314 Section 3.6).
#[test]
fn acl_single_entry_quoted_mailbox() {
    let input = b"ACL \"Sent Items\" alice lrswipcda\r\n";
    let (_, resp) = parse_untagged_acl(input, false).unwrap();
    if let UntaggedResponse::Acl { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].identifier, "alice");
        assert_eq!(entries[0].rights, "lrswipcda");
    } else {
        panic!("expected Acl, got {resp:?}");
    }
}

/// ACL with multiple entries (RFC 4314 Section 3.6).
#[test]
fn acl_multiple_entries_varied() {
    let input = b"ACL INBOX alice lrswipcda bob lr chris lrswip\r\n";
    let (_, resp) = parse_untagged_acl(input, false).unwrap();
    if let UntaggedResponse::Acl { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].identifier, "alice");
        assert_eq!(entries[0].rights, "lrswipcda");
        assert_eq!(entries[1].identifier, "bob");
        assert_eq!(entries[1].rights, "lr");
        assert_eq!(entries[2].identifier, "chris");
        assert_eq!(entries[2].rights, "lrswip");
    } else {
        panic!("expected Acl, got {resp:?}");
    }
}

/// ACL with no entries  -  empty ACL for the mailbox (RFC 4314 Section 3.6).
#[test]
fn acl_no_entries_empty() {
    let input = b"ACL \"Archive\"\r\n";
    let (_, resp) = parse_untagged_acl(input, false).unwrap();
    if let UntaggedResponse::Acl { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "Archive");
        assert!(entries.is_empty());
    } else {
        panic!("expected Acl, got {resp:?}");
    }
}

/// ACL with truncated input should fail (RFC 4314 Section 3.6).
#[test]
fn acl_truncated_input() {
    // Missing CRLF
    assert!(parse_untagged_acl(b"ACL INBOX alice lrs", false).is_err());
}

/// ACL with non-astring mailbox should fail (RFC 4314 Section 3.6).
#[test]
fn acl_garbage_data() {
    // Input missing the "ACL " prefix entirely, so tag_no_case fails
    assert!(parse_untagged_acl(b"GARBAGE data\r\n", false).is_err());
}

/// LISTRIGHTS with required and multiple optional rights groups (RFC 4314 Section 3.7).
#[test]
fn listrights_required_and_optional() {
    let input = b"LISTRIGHTS \"Sent Items\" bob lr s w i p c d a\r\n";
    let (_, resp) = parse_untagged_listrights(input, false).unwrap();
    if let UntaggedResponse::ListRights {
        mailbox,
        identifier,
        required,
        optional,
    } = resp
    {
        assert_eq!(mailbox.as_str(), "Sent Items");
        assert_eq!(identifier, "bob");
        assert_eq!(required, "lr");
        assert_eq!(optional.len(), 7);
        assert_eq!(optional, vec!["s", "w", "i", "p", "c", "d", "a"]);
    } else {
        panic!("expected ListRights, got {resp:?}");
    }
}

/// LISTRIGHTS with required rights only (no optional) (RFC 4314 Section 3.7).
#[test]
fn listrights_required_only() {
    let input = b"LISTRIGHTS INBOX alice lrswipcda\r\n";
    let (_, resp) = parse_untagged_listrights(input, false).unwrap();
    if let UntaggedResponse::ListRights {
        required, optional, ..
    } = resp
    {
        assert_eq!(required, "lrswipcda");
        assert!(optional.is_empty());
    } else {
        panic!("expected ListRights, got {resp:?}");
    }
}

/// LISTRIGHTS with truncated input should fail (RFC 4314 Section 3.7).
#[test]
fn listrights_truncated_input() {
    // Missing required rights and CRLF
    assert!(parse_untagged_listrights(b"LISTRIGHTS INBOX fred", false).is_err());
    // Missing CRLF
    assert!(parse_untagged_listrights(b"LISTRIGHTS INBOX fred lr", false).is_err());
}

/// LISTRIGHTS with garbage data should fail (RFC 4314 Section 3.7).
#[test]
fn listrights_garbage_data() {
    assert!(parse_untagged_listrights(b"LISTRIGHTS !!!GARBAGE\r\n", false).is_err());
}

/// MYRIGHTS with an atom mailbox (RFC 4314 Section 3.8).
#[test]
fn myrights_atom_mailbox() {
    let input = b"MYRIGHTS INBOX lr\r\n";
    let (_, resp) = parse_untagged_myrights(input, false).unwrap();
    if let UntaggedResponse::MyRights { mailbox, rights } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(rights, "lr");
    } else {
        panic!("expected MyRights, got {resp:?}");
    }
}

/// MYRIGHTS with truncated input should fail (RFC 4314 Section 3.8).
#[test]
fn myrights_truncated_input() {
    // Missing rights and CRLF
    assert!(parse_untagged_myrights(b"MYRIGHTS INBOX", false).is_err());
    // Missing CRLF
    assert!(parse_untagged_myrights(b"MYRIGHTS INBOX lrs", false).is_err());
}

/// MYRIGHTS with garbage data should fail (RFC 4314 Section 3.8).
#[test]
fn myrights_garbage_data() {
    assert!(parse_untagged_myrights(b"MYRIGHTS !!!GARBAGE\r\n", false).is_err());
}

// ===== Task 3: METADATA parser tests (RFC 5464 Section 4.4) =====

/// METADATA with a single entry (RFC 5464 Section 4.4).
#[test]
fn metadata_single_entry() {
    let input = b"METADATA \"INBOX\" (\"/private/comment\" \"My folder\")\r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    if let UntaggedResponse::Metadata { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"My folder".as_slice()));
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with multiple entries (RFC 5464 Section 4.4).
#[test]
fn metadata_multiple_entries_mixed() {
    let input = b"METADATA \"INBOX\" (\"/private/comment\" \"hello\" \"/shared/comment\" \"world\" \"/private/vendor/foo\" \"bar\")\r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    if let UntaggedResponse::Metadata { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].name, "/private/comment");
        assert_eq!(entries[0].value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(entries[1].name, "/shared/comment");
        assert_eq!(entries[1].value.as_deref(), Some(b"world".as_slice()));
        assert_eq!(entries[2].name, "/private/vendor/foo");
        assert_eq!(entries[2].value.as_deref(), Some(b"bar".as_slice()));
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with complex vendor key paths (RFC 5464 Section 3.2).
#[test]
fn metadata_complex_vendor_keys() {
    let input = b"METADATA \"INBOX\" (\"/shared/vendor/example.com/widget\" \"config-value\")\r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    if let UntaggedResponse::Metadata { entries, .. } = resp {
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/shared/vendor/example.com/widget");
        assert_eq!(
            entries[0].value.as_deref(),
            Some(b"config-value".as_slice())
        );
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with NIL value  -  entry does not exist (RFC 5464 Section 4.4).
#[test]
fn metadata_nil_value_deleted() {
    let input =
        b"METADATA \"INBOX\" (\"/private/comment\" NIL \"/shared/comment\" \"present\")\r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    if let UntaggedResponse::Metadata { entries, .. } = resp {
        assert_eq!(entries.len(), 2);
        assert!(entries[0].value.is_none());
        assert_eq!(entries[1].value.as_deref(), Some(b"present".as_slice()));
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with all-NIL values (RFC 5464 Section 4.4).
#[test]
fn metadata_all_nil_values() {
    let input = b"METADATA \"INBOX\" (\"/private/comment\" NIL \"/shared/comment\" NIL)\r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    if let UntaggedResponse::Metadata { entries, .. } = resp {
        assert_eq!(entries.len(), 2);
        assert!(entries[0].value.is_none());
        assert!(entries[1].value.is_none());
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with an empty list (RFC 5464 Section 4.4).
#[test]
fn metadata_empty_list_standalone() {
    let input = b"METADATA \"Archive\" ()\r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    if let UntaggedResponse::Metadata { mailbox, entries } = resp {
        assert_eq!(mailbox.as_str(), "Archive");
        assert!(entries.is_empty());
    } else {
        panic!("expected Metadata, got {resp:?}");
    }
}

/// METADATA with truncated input should fail (RFC 5464 Section 4.4).
#[test]
fn metadata_truncated_input() {
    // Missing closing paren
    assert!(
        parse_untagged_metadata(
            b"METADATA \"INBOX\" (\"/private/comment\" \"val\"\r\n",
            false
        )
        .is_err()
    );
    // Missing CRLF
    assert!(
        parse_untagged_metadata(b"METADATA \"INBOX\" (\"/private/comment\" \"val\")", false)
            .is_err()
    );
    // Missing value after key
    assert!(parse_untagged_metadata(b"METADATA \"INBOX\" (\"/private/comment\"", false).is_err());
}

/// METADATA with garbage data should fail (RFC 5464 Section 4.4).
#[test]
fn metadata_garbage_data() {
    assert!(parse_untagged_metadata(b"METADATA !!!GARBAGE\r\n", false).is_err());
}

/// METADATA via `parse_response` to verify integration (RFC 5464 Section 4.4).
#[test]
fn metadata_via_parse_response() {
    let input = b"* METADATA \"INBOX\" (\"/private/comment\" \"test\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Metadata { mailbox, entries } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "/private/comment");
        return;
    }
    panic!("expected Metadata via parse_response");
}

// ===== Task 4: THREAD parser tests (RFC 5256 Section 4) =====

/// Single flat thread: (5)  -  one message with no children (RFC 5256 Section 4).
#[test]
fn thread_single_flat() {
    let input = b"THREAD (5)\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(5));
        assert!(threads[0].children.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Chained thread: (10 20 30)  -  linear chain (RFC 5256 Section 4).
#[test]
fn thread_chained() {
    let input = b"THREAD (10 20 30)\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(10));
        assert_eq!(threads[0].children.len(), 1);
        assert_eq!(threads[0].children[0].id, Some(20));
        assert_eq!(threads[0].children[0].children.len(), 1);
        assert_eq!(threads[0].children[0].children[0].id, Some(30));
        assert!(threads[0].children[0].children[0].children.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Nested with branches: (1 2 (3)(4)) (RFC 5256 Section 4).
#[test]
fn thread_nested_with_branches() {
    let input = b"THREAD (1 2 (3)(4))\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 1);
        let child = &threads[0].children[0];
        assert_eq!(child.id, Some(2));
        assert_eq!(child.children.len(), 2);
        assert_eq!(child.children[0].id, Some(3));
        assert_eq!(child.children[1].id, Some(4));
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Multiple top-level threads: (1)(2)(3) (RFC 5256 Section 4).
#[test]
fn thread_multiple_top_level() {
    let input = b"THREAD (1)(2)(3)\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 3);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[1].id, Some(2));
        assert_eq!(threads[2].id, Some(3));
        assert!(threads[0].children.is_empty());
        assert!(threads[1].children.is_empty());
        assert!(threads[2].children.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Empty thread response (RFC 5256 Section 4).
#[test]
fn thread_empty_response() {
    let input = b"THREAD\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert!(threads.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Dummy root node: ((5)(10))  -  parent with no UID (RFC 5256 Section 4).
#[test]
fn thread_dummy_root_nodes() {
    let input = b"THREAD ((5)(10)(15))\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, None); // dummy parent (RFC 5256 Section 4)
        assert_eq!(threads[0].children.len(), 3);
        assert_eq!(threads[0].children[0].id, Some(5));
        assert_eq!(threads[0].children[1].id, Some(10));
        assert_eq!(threads[0].children[2].id, Some(15));
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Deeply nested thread: (1 (2 (3 (4 5)))) (RFC 5256 Section 4).
#[test]
fn thread_deeply_nested() {
    let input = b"THREAD (1 (2 (3 (4 5))))\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        // 1 -> (2 -> (3 -> (4 -> 5)))
        let n2 = &threads[0].children[0];
        assert_eq!(n2.id, Some(2));
        let n3 = &n2.children[0];
        assert_eq!(n3.id, Some(3));
        let n4 = &n3.children[0];
        assert_eq!(n4.id, Some(4));
        assert_eq!(n4.children.len(), 1);
        assert_eq!(n4.children[0].id, Some(5));
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Thread with truncated input should fail (RFC 5256 Section 4).
#[test]
fn thread_truncated_input() {
    // Missing closing paren and CRLF
    assert!(parse_untagged_thread(b"THREAD (1 2").is_err());
    // Missing CRLF
    assert!(parse_untagged_thread(b"THREAD (1 2)").is_err());
}

/// `build_thread_tree` with an empty chain and empty branch groups (RFC 5256 Section 4).
#[test]
fn build_thread_tree_empty() {
    let result = build_thread_tree(&[], &[]);
    assert!(result.is_empty());
}

/// `build_thread_tree` with a single UID and no branches (RFC 5256 Section 4).
#[test]
fn build_thread_tree_single_uid() {
    let result = build_thread_tree(&[Some(42)], &[vec![]]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, Some(42));
    assert!(result[0].children.is_empty());
}

/// Excessively nested THREAD response must be rejected to prevent stack
/// overflow (defense-in-depth; RFC 5256 Section 4 does not cap depth).
#[test]
fn thread_rejects_excessive_nesting_depth() {
    // Build a deeply nested thread: (1 (2 (3 (4 ... ))))
    // with 256 levels of nesting  -  well beyond MAX_THREAD_NESTING_DEPTH (64).
    let depth = 256;
    let mut input = String::from("THREAD ");
    for i in 1..=depth {
        input.push('(');
        input.push_str(&i.to_string());
        input.push(' ');
    }
    for _ in 1..=depth {
        input.push(')');
    }
    input.push_str("\r\n");

    let result = parse_untagged_thread(input.as_bytes());
    assert!(
        result.is_err(),
        "deeply nested THREAD response (depth {depth}) should be rejected"
    );
}

// ===== Task 5: Body parameter helper tests =====

/// `body_params` with NIL returns empty vec (RFC 3501 Section 7.4.2).
#[test]
fn body_params_nil_standalone() {
    let (_, params) = body_params(b"NIL").unwrap();
    assert!(params.is_empty());
}

/// `body_params` with case-insensitive NIL (RFC 3501 Section 7.4.2).
#[test]
fn body_params_nil_case_insensitive() {
    let (_, params) = body_params(b"nil").unwrap();
    assert!(params.is_empty());
    let (_, params) = body_params(b"Nil").unwrap();
    assert!(params.is_empty());
}

/// RFC 3501 Section 9: body-fld-param formally requires at least one key-value pair,
/// but many real servers send `()` for empty parameter lists.
/// Accept the empty form per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn body_params_empty_list_accepted() {
    let (_, params) = body_params(b"()").unwrap();
    assert!(
        params.is_empty(),
        "empty () should produce empty params vec, got: {params:?}"
    );
}

/// `body_params` with a single key-value pair (RFC 3501 Section 7.4.2).
#[test]
fn body_params_single_pair() {
    let (_, params) = body_params(b"(\"CHARSET\" \"UTF-8\")").unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
}

/// `body_params` with multiple key-value pairs (RFC 3501 Section 7.4.2).
#[test]
fn body_params_multiple_pairs() {
    let (_, params) =
        body_params(b"(\"CHARSET\" \"UTF-8\" \"NAME\" \"test.txt\" \"FORMAT\" \"flowed\")")
            .unwrap();
    assert_eq!(params.len(), 3);
    assert_eq!(params[0], ("charset".into(), "UTF-8".into()));
    assert_eq!(params[1], ("name".into(), "test.txt".into()));
    assert_eq!(params[2], ("format".into(), "flowed".into()));
}

/// `body_params` with RFC 2231 continuation parameters (RFC 2231 Section 3).
#[test]
fn body_params_rfc2231_continuation_standalone() {
    let input = b"(\"FILENAME*0\" \"very-\" \"FILENAME*1\" \"long-\" \"FILENAME*2\" \"name.txt\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "filename");
    assert_eq!(params[0].1, "very-long-name.txt");
}

/// `body_disposition` with NIL (RFC 2183).
#[test]
fn body_disposition_nil() {
    let (_, disp) = body_disposition(b"NIL").unwrap();
    assert!(disp.is_none());
}

/// `body_disposition` with case-insensitive NIL (RFC 2183).
#[test]
fn body_disposition_nil_case_insensitive() {
    let (_, disp) = body_disposition(b"nil").unwrap();
    assert!(disp.is_none());
}

/// `body_disposition` with type but no params (RFC 2183).
#[test]
fn body_disposition_no_params() {
    let (_, disp) = body_disposition(b"(\"inline\" NIL)").unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(disp.disposition_type, "inline");
    assert!(disp.params.is_empty());
}

/// `body_disposition` with type and params (RFC 2183).
#[test]
fn body_disposition_with_params() {
    let (_, disp) =
        body_disposition(b"(\"attachment\" (\"FILENAME\" \"report.pdf\" \"SIZE\" \"1024\"))")
            .unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(disp.disposition_type, "attachment");
    assert_eq!(disp.params.len(), 2);
    assert_eq!(disp.params[0], ("filename".into(), "report.pdf".into()));
    assert_eq!(disp.params[1], ("size".into(), "1024".into()));
}

/// `body_language` with NIL (RFC 3501 Section 7.4.2).
#[test]
fn body_language_nil() {
    let (_, lang) = body_language(b"NIL").unwrap();
    assert!(lang.is_none());
}

/// `body_language` with a single string (RFC 3501 Section 7.4.2).
#[test]
fn body_language_single_string() {
    let (_, lang) = body_language(b"\"en\"").unwrap();
    let langs = lang.expect("should not be None");
    assert_eq!(langs, vec!["en"]);
}

/// `body_language` with a parenthesized list (RFC 3501 Section 7.4.2).
#[test]
fn body_language_list() {
    let (_, lang) = body_language(b"(\"en\" \"fr\" \"de\")").unwrap();
    let langs = lang.expect("should not be None");
    assert_eq!(langs, vec!["en", "fr", "de"]);
}

/// `body_language` with an empty list `()` should be accepted per Postel's law
/// (RFC 1122 Section 1.2.2). RFC 3501 Section 7.4.2 formal ABNF requires ≥1
/// string, but real servers send `()` for empty language lists.
#[test]
fn body_language_empty_list() {
    let (_, lang) = body_language(b"()").unwrap();
    let langs = lang.expect("empty () should be Some(vec![]), not None");
    assert!(
        langs.is_empty(),
        "empty () should produce empty language vec"
    );
}

/// servers send `()` for empty body-fld-lang in BODYSTRUCTURE.
/// RFC 3501 Section 7.4.2 formal syntax requires at least one string,
/// but Postel's law demands we accept the empty form.
#[test]
fn spec_audit_body_language_empty_parenthesized() {
    // BODYSTRUCTURE with text part that has empty language list ()
    // Fields: type subtype params id description encoding size lines md5 disposition language
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5 NIL NIL () NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "body-fld-lang `()` must be accepted per Postel's law; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text { language, .. } => {
                        // () should produce Some(vec![])  -  the server explicitly sent
                        // an empty list, not NIL.
                        let langs = language
                            .as_ref()
                            .expect("empty () should be Some(vec![]), not None");
                        assert!(
                            langs.is_empty(),
                            "empty () should produce empty language vec"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// multipart BODYSTRUCTURE with empty language list `()`.
#[test]
fn spec_audit_multipart_empty_language() {
    // Multipart with extension data: params disposition language location
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 20 2) \"ALTERNATIVE\" NIL NIL () NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "multipart body-fld-lang `()` must be accepted; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Multipart { language, .. } => {
                        let langs = language
                            .as_ref()
                            .expect("empty () should be Some(vec![]), not None");
                        assert!(
                            langs.is_empty(),
                            "empty () should produce empty language vec"
                        );
                    }
                    other => panic!("expected Multipart variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== Task 6: Binary section parser tests (RFC 3516) =====

/// `binary_section_spec` with a simple section [1] (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_simple() {
    let (rest, parts) = binary_section_spec(b"[1]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1]);
}

/// `binary_section_spec` with nested path [1.2.3] (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_nested() {
    let (rest, parts) = binary_section_spec(b"[1.2.3]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2, 3]);
}

/// `binary_section_spec` with empty section `[]` (RFC 9051 Section 9).
///
/// RFC 9051 Section 9: `section-binary = "[" [section-part] "]"`  -  the
/// section-part is optional, so `[]` is valid and yields an empty vec.
#[test]
fn binary_section_spec_empty() {
    let (rest, parts) = binary_section_spec(b"[]").unwrap();
    assert!(rest.is_empty());
    assert!(parts.is_empty());
}

/// `binary_section_spec` with deeply nested path (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_deep() {
    let (rest, parts) = binary_section_spec(b"[1.2.3.4.5]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2, 3, 4, 5]);
}

/// `binary_section_spec` with truncated input should fail (RFC 3516 Section 4.3).
#[test]
fn binary_section_spec_truncated() {
    // Missing closing bracket
    assert!(binary_section_spec(b"[1.2").is_err());
    // Missing opening bracket
    assert!(binary_section_spec(b"1]").is_err());
}

/// `binary_section_with_origin` with origin offset (RFC 3516 Section 4.2).
#[test]
fn binary_section_with_origin_offset() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[1]<100>").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1]);
    assert_eq!(origin, Some(100));
}

/// `binary_section_with_origin` without origin (RFC 3516 Section 4.2).
#[test]
fn binary_section_with_origin_no_origin() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[1.2]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2]);
    assert!(origin.is_none());
}

/// `binary_section_with_origin` with zero offset (RFC 3516 Section 4.2).
#[test]
fn binary_section_with_origin_zero() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[2]<0>").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![2]);
    assert_eq!(origin, Some(0));
}

/// `binary_section_with_origin` with empty section `[]` (RFC 9051 Section 9).
///
/// RFC 9051 Section 9: `section-binary = "[" [section-part] "]"`  -  the
/// section-part is optional, so `[]<500>` is valid.
#[test]
fn binary_section_with_origin_empty_section() {
    let (rest, (parts, origin)) = binary_section_with_origin(b"[]<500>").unwrap();
    assert!(rest.is_empty());
    assert!(parts.is_empty());
    assert_eq!(origin, Some(500));
}

/// BINARY.SIZE via FETCH integration (RFC 3516 Section 4.3).
#[test]
fn fetch_binary_size_via_parse_response() {
    let input = b"* 1 FETCH (BINARY.SIZE[1] 4096)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.binary_sizes.len(), 1);
        assert_eq!(fr.binary_sizes[0], (vec![1], 4096));
        return;
    }
    panic!("expected BINARY.SIZE via FETCH");
}

#[cfg(any())]
mod moved_q_encoding_tests {
    use super::*;

    // ===== Task 7: decode_q_encoding and hex_digit tests (RFC 2047 Section 4.2) =====

    /// Simple hex-encoded byte: =E9 -> 0xE9 (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_simple_hex() {
        let result = decode_q_encoding("=E9");
        assert_eq!(result, vec![0xE9]);
    }

    /// Underscore maps to space in Q-encoding (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_underscore_to_space() {
        let result = decode_q_encoding("Hello_World");
        assert_eq!(result, b"Hello World");
    }

    /// Mixed literal and encoded characters (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_mixed_literal_and_encoded() {
        let result = decode_q_encoding("caf=E9");
        assert_eq!(result, b"caf\xE9");
    }

    /// Consecutive encoded bytes (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_consecutive_encoded() {
        let result = decode_q_encoding("=C3=A9");
        assert_eq!(result, vec![0xC3, 0xA9]);
    }

    /// Trailing incomplete =E  -  should emit '=' literally (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_trailing_incomplete() {
        let result = decode_q_encoding("test=E");
        // '=' is not followed by two hex digits, so it stays literal
        assert_eq!(result, b"test=E");
    }

    /// Invalid hex digits =GG  -  should emit '=' literally (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_invalid_hex_digits() {
        let result = decode_q_encoding("test=GG");
        assert_eq!(result, b"test=GG");
    }

    /// Empty input produces empty output (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_empty_input() {
        let result = decode_q_encoding("");
        assert!(result.is_empty());
    }

    /// All-encoded input (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_all_encoded() {
        let result = decode_q_encoding("=48=65=6C=6C=6F");
        assert_eq!(result, b"Hello");
    }

    /// Lowercase hex digits in Q-encoding (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_lowercase_hex() {
        let result = decode_q_encoding("=e9=c3");
        assert_eq!(result, vec![0xE9, 0xC3]);
    }

    /// Underscore and encoded mixed (RFC 2047 Section 4.2).
    #[test]
    fn q_encoding_underscore_and_encoded_mixed() {
        let result = decode_q_encoding("=E9l=E8ve_du_coll=E8ge");
        assert_eq!(
            result,
            vec![
                0xE9, b'l', 0xE8, b'v', b'e', b' ', b'd', b'u', b' ', b'c', b'o', b'l', b'l', 0xE8,
                b'g', b'e'
            ]
        );
    }

    /// `hex_digit` with valid digits (RFC 2047 Section 4.2).
    #[test]
    fn hex_digit_valid() {
        assert_eq!(hex_digit(b'0'), Some(0));
        assert_eq!(hex_digit(b'5'), Some(5));
        assert_eq!(hex_digit(b'9'), Some(9));
        assert_eq!(hex_digit(b'A'), Some(10));
        assert_eq!(hex_digit(b'F'), Some(15));
        assert_eq!(hex_digit(b'a'), Some(10));
        assert_eq!(hex_digit(b'f'), Some(15));
    }

    /// `hex_digit` with invalid characters (RFC 2047 Section 4.2).
    #[test]
    fn hex_digit_invalid() {
        assert_eq!(hex_digit(b'G'), None);
        assert_eq!(hex_digit(b'g'), None);
        assert_eq!(hex_digit(b'z'), None);
        assert_eq!(hex_digit(b' '), None);
        assert_eq!(hex_digit(b'!'), None);
        assert_eq!(hex_digit(b'\x00'), None);
    }

    /// `hex_digit` boundary values (RFC 2047 Section 4.2).
    #[test]
    fn hex_digit_boundaries() {
        // Just below '0'
        assert_eq!(hex_digit(b'/'), None);
        // Just above '9'
        assert_eq!(hex_digit(b':'), None);
        // Just below 'A'
        assert_eq!(hex_digit(b'@'), None);
        // Just above 'F'
        assert_eq!(hex_digit(b'G'), None);
        // Just below 'a'
        assert_eq!(hex_digit(b'`'), None);
        // Just above 'f'
        assert_eq!(hex_digit(b'g'), None);
    }

    // ===== Q-encoding soft line break stripping (Postel's law leniency) =====
    // RFC 2047 Section 4.2 Q-encoding does NOT define soft line breaks.
    // Soft line breaks are a Quoted-Printable concept from RFC 2045 Section 6.7.
    // We strip them as leniency for non-conformant encoders.

    /// `=\r\n` stripped as Postel's-law leniency (not in RFC 2047 Section 4.2;
    /// borrowed from RFC 2045 Section 6.7 Quoted-Printable).
    #[test]
    fn q_encoding_soft_line_break_crlf() {
        let result = decode_q_encoding("Hel=\r\nlo");
        assert_eq!(result, b"Hello");
    }

    /// `=\n` (bare LF) stripped as Postel's-law leniency (not in RFC 2047
    /// Section 4.2; borrowed from RFC 2045 Section 6.7 Quoted-Printable).
    #[test]
    fn q_encoding_soft_line_break_lf() {
        let result = decode_q_encoding("Hel=\nlo");
        assert_eq!(result, b"Hello");
    }

    /// `=\r\n` at end of input stripped as Postel's-law leniency.
    #[test]
    fn q_encoding_soft_line_break_at_end() {
        let result = decode_q_encoding("Hello=\r\n");
        assert_eq!(result, b"Hello");
    }

    /// `=\r\n` mixed with hex-encoded bytes, stripped as Postel's-law leniency.
    #[test]
    fn q_encoding_soft_break_with_hex() {
        let result = decode_q_encoding("caf=\r\n=E9");
        assert_eq!(result, b"caf\xE9");
    }

    /// RFC 2047 Section 4.2 Q-encoding defines only three transformations:
    /// `=XX` hex-encoded bytes, `_` as space, and printable ASCII pass-through.
    /// It does NOT define soft line breaks (`=\r\n`). Soft line breaks are a
    /// Quoted-Printable concept from RFC 2045 Section 6.7. Since encoded-words
    /// cannot contain CR/LF (RFC 2047 Section 2), `=\r\n` inside one is already
    /// malformed. We strip it as a Postel's-law leniency for non-conformant
    /// encoders  -  this test documents that intentional behavior.
    #[test]
    fn spec_audit_q_encoding_soft_line_break_is_postel_leniency() {
        let result = decode_q_encoding("Hello=\r\nWorld");
        assert_eq!(result, b"HelloWorld");
    }
}

// ===== Additional SAVEDATE (RFC 8514) coverage =====

/// SAVEDATE in a full `parse_response` round-trip (RFC 8514 Section 3).
#[test]
fn fetch_savedate_via_parse_response() {
    let input = b"* 3 FETCH (UID 10 SAVEDATE \"15-Mar-2026 12:00:00 +0000\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(10));
        assert_eq!(fr.save_date.as_deref(), Some("15-Mar-2026 12:00:00 +0000"));
        return;
    }
    panic!("expected Fetch with SAVEDATE");
}

/// SAVEDATE combined with other FETCH attributes (RFC 8514 Section 3).
#[test]
fn fetch_savedate_with_flags_and_uid() {
    let input = b"* 1 FETCH (UID 42 FLAGS (\\Seen) SAVEDATE \"01-Jan-2025 00:00:00 +0000\" RFC822.SIZE 1234)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        assert_eq!(fr.save_date.as_deref(), Some("01-Jan-2025 00:00:00 +0000"));
        assert_eq!(fr.rfc822_size, Some(1234));
        return;
    }
    panic!("expected Fetch with SAVEDATE and other attrs");
}

/// SAVEDATE with unusual timezone offset (RFC 8514 Section 3).
#[test]
fn fetch_savedate_unusual_timezone() {
    let input = b"* 1 FETCH (SAVEDATE \"31-Dec-2099 23:59:59 -1200\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.save_date.as_deref(), Some("31-Dec-2099 23:59:59 -1200"));
        return;
    }
    panic!("expected Fetch with SAVEDATE");
}

// ===== Parser stress tests =====

/// Deeply nested multipart BODYSTRUCTURE (10 levels deep).
#[test]
fn bodystructure_deeply_nested() {
    // Build a 10-level nested multipart structure:
    // ((((((((((("TEXT" "PLAIN" NIL NIL NIL "7BIT" 0 0) "MIXED")
    //   "MIXED") "MIXED") ...) "MIXED")
    let mut input = Vec::new();
    input.extend_from_slice(b"* 1 FETCH (BODYSTRUCTURE ");
    // 10 levels of opening parens
    input.resize(input.len() + 10, b'(');
    // Innermost text part
    input.extend_from_slice(b"\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 0 0");
    // Close each level with subtype
    for _ in 0..10 {
        input.extend_from_slice(b" \"MIXED\")");
    }
    input.extend_from_slice(b")\r\n");

    let (_, resp) = parse_response(&input).unwrap();
    assert!(
        matches!(resp, Response::Untagged(_)),
        "expected Untagged Fetch"
    );
}

/// Large number at `u32` boundary in FETCH UID.
#[test]
fn fetch_uid_u32_max() {
    let input = b"* 1 FETCH (UID 4294967295)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(u32::MAX));
        return;
    }
    panic!("expected Fetch with max UID");
}

/// Number overflow beyond `u32` returns parse error.
#[test]
fn number_overflow_u32() {
    // 4294967296 = u32::MAX + 1
    assert!(number(b"4294967296").is_err());
}

/// Number overflow beyond `u64` returns parse error.
#[test]
fn number_overflow_u64() {
    // 18446744073709551616 = u64::MAX + 1
    assert!(number64(b"18446744073709551616").is_err());
}

/// EXISTS with large count near `u32::MAX`.
#[test]
fn exists_large_count() {
    let input = b"* 4294967295 EXISTS\r\n";
    let (_, resp) = parse_response(input).unwrap();
    assert_eq!(
        resp,
        Response::Untagged(Box::new(UntaggedResponse::Exists(u32::MAX)))
    );
}

/// FETCH with unknown attribute is gracefully skipped (stress variant).
#[test]
fn fetch_unknown_attribute_skipped_stress() {
    let input = b"* 1 FETCH (UID 42 X-CUSTOM-ATTR \"some value\" FLAGS (\\Seen))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.flags, Some(vec![Flag::Seen]));
        return;
    }
    panic!("expected Fetch with unknown attr skipped");
}

/// Malformed FETCH with truncated input returns error (not infinite loop).
#[test]
fn fetch_truncated_returns_error() {
    // Missing closing paren and CRLF
    assert!(parse_response(b"* 1 FETCH (UID 42").is_err());
}

/// Garbage data as response returns error.
#[test]
fn garbage_data_returns_error() {
    assert!(parse_response(b"GARBAGE\r\n").is_err());
    assert!(parse_response(b"\x00\x01\x02\r\n").is_err());
    assert!(parse_response(b"").is_err());
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

/// ESEARCH with unknown key is skipped gracefully (stress variant).
#[test]
fn esearch_unknown_key_skipped_stress() {
    let input = b"* ESEARCH (TAG \"A001\") UID XFUTURE somevalue\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert!(esearch.all.is_empty(), "unknown key should be skipped");
        assert!(esearch.uid);
        return;
    }
    panic!("expected Esearch response");
}

/// VANISHED with overlapping UID ranges parses correctly.
#[test]
fn vanished_overlapping_ranges() {
    let input = b"* VANISHED (EARLIER) 1:5,3:8,10\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Vanished { earlier, uids } = *boxed
    {
        assert!(earlier);
        assert_eq!(uids.len(), 3);
        assert_eq!(uids[0], UidRange::range(1, 5));
        assert_eq!(uids[1], UidRange::range(3, 8));
        assert_eq!(uids[2], UidRange::single(10));
        return;
    }
    panic!("expected Vanished response");
}

/// VANISHED known-uids must reject `*` per RFC 7162 Section 6.
///
/// RFC 7162 Section 6 ABNF:
///   known-uids    = sequence-set
///                   ;; Sequence of UIDs; "*" is not allowed.
///   expunged-resp = "VANISHED" [SP "(EARLIER)"] SP known-uids
///
/// Although the grammar references `sequence-set`, the ABNF comment
/// explicitly prohibits `*`. The parser must not produce a `Vanished`
/// response when `*` appears in the UID set.
#[test]
fn vanished_sequence_set_with_star() {
    // `*` must be rejected in VANISHED known-uids per RFC 7162 Section 6.
    let input = b"* VANISHED 1:5,*\r\n";
    assert!(parse_response(input).is_err());
}

/// VANISHED known-uids must reject `*` per RFC 7162 Section 6.
///
/// RFC 7162 Section 6 ABNF:
///   known-uids    = sequence-set
///                   ;; Sequence of UIDs; "*" is not allowed.
///   expunged-resp = "VANISHED" [SP "(EARLIER)"] SP known-uids
///
/// The `*` wildcard is explicitly prohibited in VANISHED known-uids,
/// even though the grammar references `sequence-set`. The parser must
/// use `uid-set` (nz-number only) to enforce this restriction.
/// Also tests `*` in a range position (e.g. `1:*`).
#[test]
fn vanished_rejects_star_in_known_uids() {
    // Bare `*` must be rejected.
    let input = b"* VANISHED *\r\n";
    assert!(parse_response(input).is_err());

    // `*` in a range (`1:*`) must also be rejected.
    let input = b"* VANISHED 1:*\r\n";
    assert!(parse_response(input).is_err());

    // `*` with EARLIER must also be rejected.
    let input = b"* VANISHED (EARLIER) 1:5,*\r\n";
    assert!(parse_response(input).is_err());
}

/// Non-ASCII bytes in quoted strings are handled gracefully.
#[test]
fn quoted_string_non_ascii_bytes() {
    // Subject with raw non-ASCII in quoted string (some servers do this)
    let input =
        b"* 1 FETCH (UID 1 ENVELOPE (NIL \"caf\xc3\xa9\" NIL NIL NIL NIL NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(
            fr.envelope.as_ref().and_then(|e| e.subject.as_deref()),
            Some("café")
        );
        return;
    }
    panic!("expected Fetch with non-ASCII envelope subject");
}

/// Binary section with large offset value.
#[test]
fn fetch_binary_large_origin() {
    let input = b"* 1 FETCH (BINARY[1]<4294967295> NIL)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.binary_sections.len(), 1);
        assert_eq!(fr.binary_sections[0].origin, Some(u64::from(u32::MAX)));
        assert!(fr.binary_sections[0].data.is_none());
        return;
    }
    panic!("expected Fetch with large binary origin");
}

/// CONDSTORE MODSEQ with `i64::MAX` value (RFC 9051 number64 limit).
#[test]
fn fetch_modseq_i64_max() {
    let input = b"* 1 FETCH (MODSEQ (9223372036854775807))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.mod_seq, Some(i64::MAX as u64));
        return;
    }
    panic!("expected Fetch with max MODSEQ");
}

/// CONDSTORE MODSEQ with `u64::MAX` must be rejected (exceeds number64 range).
/// The malformed MODSEQ is skipped gracefully; the FETCH response is still
/// parsed with `mod_seq` = None (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn fetch_modseq_u64_max_rejected() {
    // u64::MAX exceeds number64 range (RFC 9051 Section 4).
    // The MODSEQ attribute is skipped, but the FETCH response is preserved.
    let input = b"* 1 FETCH (MODSEQ (18446744073709551615))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = &*boxed {
            assert_eq!(
                fr.mod_seq, None,
                "u64::MAX MODSEQ should be skipped (None), not stored"
            );
        } else {
            panic!("expected Fetch with mod_seq=None, got {boxed:?}");
        }
    } else {
        panic!("Expected Untagged, got {resp:?}");
    }
}

/// HIGHESTMODSEQ response code with `i64::MAX` value (RFC 9051 number64 limit).
#[test]
fn response_code_highestmodseq_i64_max() {
    let (_, code) = response_code(b"[HIGHESTMODSEQ 9223372036854775807]").unwrap();
    assert_eq!(code, ResponseCode::HighestModSeq(i64::MAX as u64));
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

/// Verify that a full untagged OK with `[HIGHESTMODSEQ 0]` preserves the
/// response code structurally rather than losing it into the text field.
#[test]
fn highestmodseq_zero_in_full_response_preserved() {
    let input = b"* OK [HIGHESTMODSEQ 0] Strstrumpf\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Status { code, text, .. } => {
                assert_eq!(
                    code,
                    Some(ResponseCode::HighestModSeq(0)),
                    "HIGHESTMODSEQ 0 response code must be preserved structurally, \
                         not consumed into the text field"
                );
                assert_eq!(text, "Strstrumpf");
            }
            other => panic!("expected Status, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== RFC 6855 literal8 tests =====

/// literal8: `~{n}\r\n<data>` accepts the tilde prefix (RFC 6855 Section 4).
#[test]
fn literal8_simple() {
    let (rest, val) = literal(b"~{5}\r\nhello rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
}

/// Per Postel's law, tolerate `~{n+}` (literal8 combined with LITERAL+) from
/// non-conformant servers. RFC 9051 Section 9 does not define `+` for literal8,
/// and RFC 7888 Section 4 defines LITERAL+ for regular literals only, but the
/// `+` has no semantic impact on server-to-client data so we accept it.
#[test]
fn literal8_plus_tolerated() {
    let (rest, val) = literal(b"~{3+}\r\nabc rest").unwrap();
    assert_eq!(val, b"abc");
    assert_eq!(rest, b" rest");
}

/// literal8 with zero length.
#[test]
fn literal8_zero_length() {
    let (_, val) = literal(b"~{0}\r\n").unwrap();
    assert!(val.is_empty());
}

/// literal8 with UTF-8 content (RFC 6855 Section 4).
#[test]
fn literal8_utf8_content() {
    let data = "日本語";
    let header = format!("~{{{}}}\r\n{}", data.len(), data);
    let (_, val) = literal(header.as_bytes()).unwrap();
    assert_eq!(val, data.as_bytes());
}

/// RFC 9051 Section 9: literal8 (`~{n}`) does not define the `+` suffix,
/// but per Postel's law we tolerate `~{n+}` from non-conformant servers.
#[test]
fn spec_audit_literal8_tolerates_non_sync() {
    // Regular literal with '+' (LITERAL+) should be accepted
    assert!(
        literal(b"{5+}\r\nhello").is_ok(),
        "LITERAL+ must be accepted"
    );
    // literal8 without '+' should be accepted
    assert!(
        literal(b"~{5}\r\nhello").is_ok(),
        "literal8 must be accepted"
    );
    // literal8 with '+'  -  tolerated per Postel's law (RFC 9051 Section 9
    // does not define it, but the `+` has no semantic impact on
    // server-to-client data).
    assert!(
        literal(b"~{5+}\r\nhello").is_ok(),
        "literal8 with + must be tolerated per Postel's law"
    );
}

/// literal8 in an nstring context (via string -> literal).
#[test]
fn nstring_literal8() {
    let (_, val) = nstring(b"~{4}\r\ntest").unwrap();
    assert_eq!(val, Some(b"test".to_vec()));
}

// ===== RFC 6532/6855 UTF-8 mode tests =====

/// `utf8_mode=true` skips RFC 2047 decoding on envelope subject.
#[test]
fn envelope_utf8_mode_subject_passthrough() {
    // Subject contains a literal `=?...?=` that should NOT be decoded in utf8_mode
    let input = b"(\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"To\" NIL \"t\" \"x.com\")) \
            NIL NIL NIL \"<id@x.com>\")";
    let (_, env) = envelope(input, true).unwrap();
    // In utf8_mode the raw string is preserved, not decoded
    assert_eq!(env.subject.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
}

/// `utf8_mode=false` still applies RFC 2047 decoding on envelope subject.
#[test]
fn envelope_non_utf8_mode_subject_decoded() {
    let input = b"(\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"Sender\" NIL \"s\" \"x.com\")) \
            ((\"To\" NIL \"t\" \"x.com\")) \
            NIL NIL NIL \"<id@x.com>\")";
    let (_, env) = envelope(input, false).unwrap();
    assert_eq!(env.subject.as_deref(), Some("Alice"));
}

/// `utf8_mode=true` skips RFC 2047 decoding on address display name.
#[test]
fn address_utf8_mode_name_passthrough() {
    let (_, addr) = address(
        b"(\"=?UTF-8?B?QWxpY2U=?=\" NIL \"alice\" \"example.com\")",
        true,
    )
    .unwrap();
    // In utf8_mode the raw string is preserved
    assert_eq!(addr.name.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
}

/// `utf8_mode=false` still decodes RFC 2047 in address display name.
#[test]
fn address_non_utf8_mode_name_decoded() {
    let (_, addr) = address(
        b"(\"=?UTF-8?B?QWxpY2U=?=\" NIL \"alice\" \"example.com\")",
        false,
    )
    .unwrap();
    assert_eq!(addr.name.as_deref(), Some("Alice"));
}

/// `utf8_mode=true` with raw UTF-8 display name (RFC 6532 Section 3).
#[test]
fn address_utf8_mode_raw_utf8_name() {
    // Raw UTF-8 name: "日本太郎"
    let raw_name = "日本太郎";
    let input = format!("(\"{raw_name}\" NIL \"taro\" \"example.jp\")");
    let (_, addr) = address(input.as_bytes(), true).unwrap();
    assert_eq!(addr.name.as_deref(), Some("日本太郎"));
}

/// `utf8_mode` threads through `parse_response_utf8` to FETCH ENVELOPE.
#[test]
fn parse_response_utf8_fetch_envelope() {
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            NIL NIL NIL NIL \"<id@x.com>\"))\r\n";
    // utf8_mode=true: no RFC 2047 decoding
    let (_, resp) = parse_response_utf8(input, true).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = *u {
            let env = fr.envelope.unwrap();
            assert_eq!(env.subject.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
            assert_eq!(env.from[0].name.as_deref(), Some("=?UTF-8?B?QWxpY2U=?="));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// `parse_response_utf8` with `utf8_mode=false` behaves like `parse_response`.
#[test]
fn parse_response_utf8_false_decodes_rfc2047() {
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024\" \"=?UTF-8?B?QWxpY2U=?=\" \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            ((\"=?UTF-8?B?QWxpY2U=?=\" NIL \"a\" \"x.com\")) \
            NIL NIL NIL NIL \"<id@x.com>\"))\r\n";
    let (_, resp) = parse_response_utf8(input, false).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = *u {
            let env = fr.envelope.unwrap();
            assert_eq!(env.subject.as_deref(), Some("Alice"));
            assert_eq!(env.from[0].name.as_deref(), Some("Alice"));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// literal8 in a FETCH BODY[] context.
#[test]
fn fetch_literal8_body_section() {
    let body = b"Hello world";
    let input = format!(
        "(BODY[] ~{{{}}}\r\n{})",
        body.len(),
        std::str::from_utf8(body).unwrap()
    );
    let (_, fr) = fetch_response_inner(input.as_bytes(), false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].data, Some(body.to_vec()));
}

/// literal8 in a FETCH ENVELOPE subject field.
#[test]
fn fetch_envelope_literal8_subject() {
    let subject = "テスト件名";
    let lit = format!("~{{{}}}\r\n{}", subject.len(), subject);
    let input = format!(
        "(ENVELOPE (\"date\" {lit} \
            ((\"From\" NIL \"f\" \"x.com\")) \
            ((\"From\" NIL \"f\" \"x.com\")) \
            ((\"From\" NIL \"f\" \"x.com\")) \
            NIL NIL NIL NIL \"<id@x.com>\"))"
    );
    let (_, fr) = fetch_response_inner(input.as_bytes(), true).unwrap();
    let env = fr.envelope.unwrap();
    assert_eq!(env.subject.as_deref(), Some("テスト件名"));
}

// ===== Postel's law: accept empty "()" from non-conformant servers =====
// RFC 3501 Section 9: env-from = "(" 1*address ")" / nil  -  strictly requires
// at least one address, but non-conformant servers send "()" instead of NIL.
// Accepted per Postel's law (RFC 1122 Section 1.2.2).

#[test]
fn address_list_accepts_empty_parens() {
    // "()"  -  accepted per Postel's law even though RFC 3501 requires 1*address
    let (rest, addrs) = address_list(b"()", false).unwrap();
    assert!(rest.is_empty());
    assert!(addrs.is_empty());
}

#[test]
fn address_list_accepts_nil() {
    // NIL is the valid way to express "no addresses"
    let (rest, addrs) = address_list(b"NIL", false).unwrap();
    assert!(rest.is_empty());
    assert!(addrs.is_empty());
}

// ===== ESEARCH MIN/MAX must reject zero =====
// RFC 4731 Section 3.1: MIN SP nz-number / MAX SP nz-number
// Zero is not a valid message number or UID.

#[test]
fn esearch_min_rejects_zero() {
    // "* ESEARCH (TAG \"A1\") MIN 0\r\n"  -  MIN 0 is invalid per RFC 4731.
    let input = b"* ESEARCH (TAG \"A1\") MIN 0\r\n";
    assert!(parse_response(input).is_err());
}

#[test]
fn esearch_max_rejects_zero() {
    // "* ESEARCH (TAG \"A1\") MAX 0\r\n"  -  MAX 0 is invalid per RFC 4731.
    let input = b"* ESEARCH (TAG \"A1\") MAX 0\r\n";
    assert!(parse_response(input).is_err());
}

#[test]
fn esearch_min_accepts_one() {
    // MIN 1 is the smallest valid nz-number
    let input = b"* ESEARCH (TAG \"A1\") MIN 1\r\n";
    let result = parse_response(input);
    assert!(result.is_ok());
}

// ===== LIST delimiter must handle multi-byte UTF-8 =====
// RFC 3501 Section 7.2.2: hierarchy delimiter is a single character.
// When quoted, multi-byte UTF-8 chars must be decoded, not byte-truncated.

#[test]
fn list_delimiter_multibyte_utf8() {
    // "÷" is U+00F7, encoded as [0xC3, 0xB7] in UTF-8
    let input = b"LIST (\\HasNoChildren) \"\xC3\xB7\" INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, false).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(
            info.delimiter,
            Some('\u{00F7}'),
            "multi-byte UTF-8 delimiter should be decoded as full char, not first byte"
        );
    } else {
        panic!("expected List response");
    }
}

// ===== LIST delimiter must be exactly one QUOTED-CHAR (RFC 3501 Section9) =====
// RFC 3501 Section 9: mailbox-list = ... SP (DQUOTE QUOTED-CHAR DQUOTE / nil) SP mailbox
// Multi-character quoted strings in the delimiter position are a protocol error.

/// Single-char delimiter "/" must parse correctly (RFC 3501 Section 7.2.2).
#[test]
fn list_delimiter_single_char_valid() {
    let input = b"LIST () \"/\" INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, false).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.delimiter, Some('/'));
    } else {
        panic!("expected List response");
    }
}

/// NIL delimiter must parse correctly (RFC 3501 Section 7.2.2).
#[test]
fn list_delimiter_nil_valid() {
    let input = b"LIST () NIL INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, false).unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.delimiter, None);
    } else {
        panic!("expected List response");
    }
}

/// Postel's law (RFC 3501 Section 9): a non-conformant server sending a
/// multi-character delimiter (e.g., "//") should not cause a hard parse
/// failure. The parser takes the first character and tolerates the rest.
#[test]
fn list_delimiter_multi_char_tolerant() {
    let input = b"LIST () \"//\" INBOX\r\n";
    let (_, resp) = parse_untagged_list(input, false).expect(
        "multi-character delimiter must be tolerated per Postel's law \
             (RFC 3501 Section 9): take the first char, not a hard error",
    );
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(
            info.delimiter,
            Some('/'),
            "multi-char delimiter should take the first character"
        );
    } else {
        panic!("expected List response");
    }
}

// ===== Tag parser must reject "+" (RFC 3501 Section 9) =====
// RFC 3501 Section 9: tag = 1*<any ASTRING-CHAR except "+">
// "+" is reserved for continuation responses, so it must not appear in tags.

/// Tag containing "+" must be rejected per RFC 3501 Section 9.
#[test]
fn tagged_response_rejects_plus_in_tag() {
    // "A+01 OK done\r\n"  -  "+" is not valid in a tag
    let result = parse_response(b"A+01 OK done\r\n");
    assert!(
        result.is_err(),
        "tag containing '+' should be rejected per RFC 3501 Section 9"
    );
}

/// Tag that is solely "+" must be rejected.
#[test]
fn tagged_response_rejects_bare_plus_tag() {
    let result = parse_response(b"+ OK done\r\n");
    // This should be parsed as a continuation request, not a tagged response.
    // If it's parsed at all, the tag must not be "+".
    if let Ok((_, Response::Tagged(t))) = result {
        panic!("'+' should not be accepted as a tag, got tagged response: {t:?}");
    }
}

// ===== LIST-EXTENDED / OLDNAME trailing data (RFC 5258 / RFC 9051) =====
// RFC 5258 Section 6: mailbox-list = ... mailbox [SP mbox-list-extended]
// RFC 9051 Section 6.3.9.7 example: * LIST () "/" "NewMailbox" ("OLDNAME" ("OldMailbox"))

/// LIST with OLDNAME extended data must parse without error (RFC 9051 Section 6.3.9.7).
#[test]
fn list_with_oldname_extended_data() {
    let input = b"LIST () \"/\" \"NewMailbox\" (\"OLDNAME\" (\"OldMailbox\"))\r\n";
    let result = parse_untagged_list(input, false);
    assert!(
        result.is_ok(),
        "LIST with OLDNAME extended data should parse successfully \
             per RFC 5258 / RFC 9051 Section 6.3.9.7, got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::List(info) = resp {
        assert_eq!(info.name.as_str(), "NewMailbox");
        assert_eq!(info.delimiter, Some('/'));
    } else {
        panic!("expected List response, got {resp:?}");
    }
}

/// LIST with CHILDINFO extended data must parse without error (RFC 5258 Section 4).
#[test]
fn list_with_childinfo_extended_data() {
    let input = b"LIST (\\HasChildren) \"/\" \"INBOX\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let result = parse_untagged_list(input, false);
    assert!(
        result.is_ok(),
        "LIST with CHILDINFO extended data should parse successfully \
             per RFC 5258, got: {result:?}"
    );
}

// ===== SEARCH with MODSEQ (RFC 7162 Section 3.1.5) =====
// When the client searches with a MODSEQ criterion and gets results,
// the server MUST append (MODSEQ <n>) to the SEARCH response.

/// SEARCH response with trailing (MODSEQ n) must parse (RFC 7162 Section 3.1.5).
#[test]
fn search_with_modseq_trailing() {
    // RFC 7162 Section 3.1.5 example:
    // * SEARCH 2 5 6 7 11 12 18 19 20 23 (MODSEQ 917162500)
    let input = b"* SEARCH 2 5 6 7 11 12 18 19 20 23 (MODSEQ 917162500)\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "SEARCH with trailing (MODSEQ n) should parse per RFC 7162 Section 3.1.5, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = *u {
            assert_eq!(uids, vec![2, 5, 6, 7, 11, 12, 18, 19, 20, 23]);
            assert_eq!(mod_seq, Some(917_162_500));
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected untagged response");
    }
}

/// SEARCH MODSEQ value of 0 must be rejected (RFC 7162 Section 3.1.6).
///
/// `search-sort-mod-seq = "(" "MODSEQ" SP mod-sequence-value ")"` where
/// `mod-sequence-value` must be >= 1. Only `mod-sequence-valzer` allows 0,
/// and that is used only in UNCHANGEDSINCE and STATUS HIGHESTMODSEQ contexts.
#[test]
fn search_modseq_rejects_zero() {
    let input = b"* SEARCH 1 5 (MODSEQ 0)\r\n";
    let (_, resp) = parse_response(input)
        .expect("SEARCH with MODSEQ 0 must still parse  -  UIDs must be preserved");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = &*u {
            assert_eq!(*uids, vec![1, 5], "UIDs must be preserved");
            assert_ne!(
                *mod_seq,
                Some(0),
                "SEARCH MODSEQ 0 must be rejected per RFC 7162 Section 3.1.6"
            );
        } else {
            panic!("expected Search, got {u:?}  -  must not fall through to Unknown");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// SEARCH MODSEQ value of 1 must be accepted (RFC 7162 Section 3.1.6).
#[test]
fn search_modseq_accepts_one() {
    let input = b"* SEARCH 1 5 (MODSEQ 1)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = *u {
            assert_eq!(uids, vec![1, 5]);
            assert_eq!(
                mod_seq,
                Some(1),
                "SEARCH MODSEQ 1 must be accepted per RFC 7162 Section 3.1.6"
            );
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected untagged response");
    }
}

/// ESEARCH response with MODSEQ result option (RFC 7162 Section 3.1.10).
#[test]
fn esearch_with_modseq_result() {
    // RFC 7162 Section 3.1.10: ESEARCH MUST return MODSEQ result option
    let input = b"ESEARCH (TAG \"A002\") UID ALL 2,10:15 MODSEQ 917162500\r\n";
    let result = parse_untagged_esearch(input);
    assert!(
        result.is_ok(),
        "ESEARCH with MODSEQ result should parse per RFC 7162 Section 3.1.10, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Esearch(esearch) = resp {
        assert_eq!(
            esearch.mod_seq,
            Some(917_162_500),
            "ESEARCH MODSEQ value must be retained per RFC 7162 Section 3.1.10"
        );
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

/// RFC 4731 Section 3.1: ESEARCH ALL uses `sequence-set` which allows `*`.
/// previously used `uid_set()` which only accepts `nz-number`.
#[test]
fn spec_audit_esearch_all_accepts_wildcard() {
    let input = b"* ESEARCH (TAG \"A1\") ALL 1:*\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(es) = &*u {
            assert_eq!(es.all.len(), 1);
            assert_eq!(es.all[0].start, 1);
            assert_eq!(es.all[0].end_bound(), Some(crate::types::SeqBound::Star));
        } else {
            panic!("Expected Esearch, got {u:?}");
        }
    } else {
        panic!("Expected Untagged response");
    }
}

/// Regression: `tag_no_case(&b"UID"[..])` without a token boundary check
/// greedily matches the prefix of atoms like "UIDFOO", setting `uid = true`
/// and leaving the remaining "FOO" fragment to corrupt subsequent parsing.
/// The UID indicator must be a standalone keyword per RFC 4731 Section 3.1.
#[test]
fn esearch_uid_keyword_boundary() {
    // The unknown extension keyword "UIDFOO" starts with "UID".
    // Without a token boundary check, the parser greedily consumes "UID",
    // sets uid=true, and then fails on the leftover "FOO".
    // Correct behaviour: "UIDFOO" is NOT the UID indicator; it should be
    // parsed as an unknown key (and its value skipped) with uid=false.
    let input = b"ESEARCH (TAG \"A1\") UIDFOO 42 COUNT 3\r\n";
    let result = parse_untagged_esearch(input);
    assert!(
        result.is_ok(),
        "ESEARCH with atom starting with 'UID' must not fail \
             (RFC 4731 Section 3.1 token boundary), got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Esearch(es) = resp {
        assert!(
            !es.uid,
            "uid must be false when the atom is 'UIDFOO', not 'UID'"
        );
        assert_eq!(
            es.count,
            Some(3),
            "COUNT must still be parsed after skipping unknown 'UIDFOO' key"
        );
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

// ===== NAMESPACE nested extension data (RFC 2342) =====
// RFC 2342 example: * NAMESPACE (("" "/")("#mh/" "/" "X-PARAM" ("FLAG1" "FLAG2"))) NIL NIL
// The extension data contains nested parentheses which the current skipper mishandles.

/// NAMESPACE with nested parenthesized extension data (RFC 2342).
#[test]
fn namespace_nested_extension_data() {
    // Extension data with nested parens: "X-PARAM" ("FLAG1" "FLAG2")
    let input = b"NAMESPACE ((\"\" \"/\" \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let result = parse_untagged_namespace(input, false);
    assert!(
        result.is_ok(),
        "NAMESPACE with nested extension parens should parse per RFC 2342, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert_eq!(personal.len(), 1, "should have one personal namespace");
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert!(other.is_empty());
        assert!(shared.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// NAMESPACE with multiple descriptors and nested extensions (RFC 2342).
#[test]
fn namespace_multiple_descriptors_nested_ext() {
    let input =
        b"NAMESPACE ((\"\" \"/\")(\"#mh/\" \"/\" \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let result = parse_untagged_namespace(input, false);
    assert!(
        result.is_ok(),
        "NAMESPACE with multiple descriptors and nested extensions should parse, \
             got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 2);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[1].prefix, "#mh/");
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

// ===== NAMESPACE extension data must be preserved (RFC 2342 Section6) =====

/// NAMESPACE with extension data must preserve extensions in the parsed output.
/// RFC 2342 Section6 ABNF: `*(Namespace_Response_Extension)`
/// where `Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"`.
#[test]
fn namespace_extension_data_preserved() {
    let input = b"NAMESPACE ((\"\" \"/\" \"TRANSLATION\" (\"strstrumpf\"))) NIL NIL\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace {
        personal,
        other,
        shared,
    } = resp
    {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, Some('/'));
        assert_eq!(
            personal[0].extensions,
            vec![("TRANSLATION".to_string(), vec!["strstrumpf".to_string()])]
        );
        assert!(other.is_empty());
        assert!(shared.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// NAMESPACE with multiple extension key-value-list pairs (RFC 2342 Section6).
#[test]
fn namespace_multiple_extensions_preserved() {
    let input = b"NAMESPACE ((\"\" \"/\" \"TRANSLATION\" (\"strstrumpf\") \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].extensions.len(), 2);
        assert_eq!(
            personal[0].extensions[0],
            ("TRANSLATION".to_string(), vec!["strstrumpf".to_string()])
        );
        assert_eq!(
            personal[0].extensions[1],
            (
                "X-PARAM".to_string(),
                vec!["FLAG1".to_string(), "FLAG2".to_string()]
            )
        );
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// NAMESPACE without extension data should have empty extensions vec.
#[test]
fn namespace_no_extension_data_empty_vec() {
    let input = b"NAMESPACE ((\"\" \"/\")) NIL NIL\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert!(personal[0].extensions.is_empty());
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

// ===== atom parser allows `[` (RFC 3501 Section 9) =====

#[test]
fn atom_accepts_open_bracket() {
    // '[' is a valid ATOM-CHAR per RFC 3501 Section 9
    let (rest, val) = atom(b"BODY[TEXT] rest").unwrap();
    assert_eq!(val, b"BODY[TEXT");
    assert_eq!(rest, b"] rest");
}

#[test]
fn atom_still_stops_at_close_bracket() {
    let (rest, val) = atom(b"FOO]bar").unwrap();
    assert_eq!(val, b"FOO");
    assert_eq!(rest, b"]bar");
}

#[test]
fn fetch_attr_atom_stops_at_open_bracket() {
    let (rest, val) = fetch_attr_atom(b"BODY[TEXT] rest").unwrap();
    assert_eq!(val, b"BODY");
    assert_eq!(rest, b"[TEXT] rest");
}

#[test]
fn fetch_attr_atom_no_bracket() {
    let (rest, val) = fetch_attr_atom(b"FLAGS rest").unwrap();
    assert_eq!(val, b"FLAGS");
    assert_eq!(rest, b" rest");
}

// ===== nz_number enforcement (RFC 3501 Section 9) =====

#[test]
fn search_accepts_zero_in_results() {
    // RFC 3501 Section 7.2.5 ABNF says `nz-number`. Per Postel's law
    // (RFC 1122 Section 1.2.2) we accept 0 during parsing (to avoid
    // `many0` stopping early and losing subsequent valid results), but
    // filter it out before returning since UID 0 is semantically invalid.
    let input = b"* SEARCH 1 0 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = &*u {
            assert_eq!(
                *uids,
                vec![1, 5],
                "0 must be filtered; valid UIDs must be preserved"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search response, got {u:?}  -  valid UIDs must not be lost");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn search_empty_results_still_works() {
    let input = b"* SEARCH\r\n";
    let (_, resp) = parse_response(input).unwrap();
    // empty SEARCH is fine (many0 accepts zero results)
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Search { uids, .. } = *boxed
    {
        assert!(uids.is_empty());
        return;
    }
    panic!("expected Search");
}

#[test]
fn malformed_fetch_uid_zero_is_parse_failure() {
    // uniqueid = nz-number per RFC 3501 Section 9.
    let input = b"* 1 FETCH (UID 0)\r\n";
    assert!(parse_response(input).is_err());
}

#[test]
fn malformed_closed_grammar_fetch_attributes_are_parse_failures() {
    // Every attribute whose `msg-att` grammar is closed must surface a parse
    // failure rather than being laundered into `Unknown`, which would drop the
    // whole FETCH silently while the sync cursor advanced past it.
    for input in [
        &b"* 1 FETCH (FLAGS nope)\r\n"[..],
        b"* 1 FETCH (RFC822.SIZE huge)\r\n",
        b"* 1 FETCH (RFC822.TEXT 12)\r\n",
        b"* 1 FETCH (INTERNALDATE 5)\r\n",
        b"* 1 FETCH (SAVEDATE 5)\r\n",
        b"* 1 FETCH (PREVIEW 5)\r\n",
        b"* 1 FETCH (MODSEQ 12345)\r\n",
        b"* 1 FETCH (EMAILID M00000001)\r\n",
        b"* 1 FETCH (THREADID T1)\r\n",
        b"* 1 FETCH (X-GM-MSGID nope)\r\n",
        b"* 1 FETCH (X-GM-THRID nope)\r\n",
        b"* 1 FETCH (BINARY.SIZE[1] nope)\r\n",
        b"* 1 FETCH (BODY[TEXT] 12)\r\n",
    ] {
        assert!(
            parse_response(input).is_err(),
            "expected a parse failure for {:?}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn unmodelled_and_open_ended_fetch_attributes_stay_tolerated() {
    // Syntax this parser does not model must never cost the connection.
    for input in [
        // Unrecognized attribute with a well-formed value: parsed, skipped.
        &b"* 1 FETCH (UID 7 X-FUTURE-THING (a b c))\r\n"[..],
        b"* 1 FETCH (UID 7 X-FUTURE-THING \"opaque\")\r\n",
    ] {
        assert!(
            matches!(
                parse_response(input),
                Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Fetch(_))
            ),
            "expected a tolerated FETCH for {:?}",
            String::from_utf8_lossy(input)
        );
    }

    // Valid fixed prefixes followed by a tail this parser does not model stay
    // tolerant. The tests use an unsupported field after a valid basic body
    // prefix (top-level and nested inside a multipart child) and an
    // address-list shape that is balanced but not typed.
    for input in [
        &b"* 1 FETCH (ENVELOPE (\"date\" \"subject\" (NIL) NIL NIL NIL NIL NIL NIL NIL))\r\n"[..],
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL (bad)))\r\n",
        b"* 1 FETCH (BODYSTRUCTURE ((\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL (bad)) \"MIXED\"))\r\n",
    ] {
        assert!(
            matches!(
                parse_response(input),
                Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Unknown(_))
            ),
            "expected tolerated degradation for {:?}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn malformed_open_ended_fetch_fixed_prefixes_are_parse_failures() {
    // ENVELOPE has ten mandatory outer fields. BODYSTRUCTURE and bare BODY
    // share the single-part fixed prefix: media type/subtype, params, id,
    // description, encoding, and size. A malformed prefix is a provider
    // contract violation, not an extension-tail modelling gap.
    for input in [
        &b"* 1 FETCH (ENVELOPE (\"date\"))\r\n"[..],
        b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\"))\r\n",
        b"* 1 FETCH (BODY (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" huge))\r\n",
        b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\")))\r\n",
        // A multipart child is a body too: its fixed-arity prefix is checked
        // recursively, so an arity violation cannot hide one level down (or
        // deeper) behind a balanced group.
        b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\") \"MIXED\"))\r\n",
        b"* 1 FETCH (BODYSTRUCTURE (((\"TEXT\") \"ALTERNATIVE\") \"MIXED\"))\r\n",
        // Truncation of the outer structure: the fixed prefix parses, but the
        // body never closes. An open-ended extension tail is open in its
        // contents, not in its framing, so requiring the balanced close costs
        // no server tolerance.
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000\r\n",
        b"* 1 FETCH (BODY (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL (\"INLINE\"\r\n",
        b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) \"MIXED\"\r\n",
    ] {
        assert!(
            parse_response(input).is_err(),
            "expected a parse failure for {:?}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn fetch_bodystructure_extension_double_spaces_parse() {
    // Two spaces between optional body-extension fields are tolerated, as in
    // the other BODYSTRUCTURE list positions.
    let input =
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL  NIL))\r\n";
    assert!(matches!(
        parse_response(input),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Fetch(_))
    ));

    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) \"MIXED\" NIL  NIL))\r\n";
    assert!(matches!(
        parse_response(input),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Fetch(_))
    ));
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
    assert_eq!(r.start, 1, "reversed range must normalize start to min");
    assert_eq!(r.end, Some(100), "reversed range must normalize end to max");
}

/// Postel's law: `seq_range` must also normalize reversed ranges,
/// consistent with `uid_range`. ESEARCH ALL (RFC 4731 Section 3.1)
/// and VANISHED (RFC 7162 Section 3.2.10) use `sequence_set`, so
/// reversed ranges from non-conformant servers must be handled.
#[test]
fn seq_range_reversed_normalized() {
    let (_, r) = seq_range(b"100:1").unwrap();
    assert_eq!(
        r.start, 1,
        "seq_range reversed range must normalize start to min"
    );
    assert_eq!(
        r.end,
        Some(100),
        "seq_range reversed range must normalize end to max"
    );
}

/// RFC 3501 Section 9: `seq-range = seq-number ":" seq-number` where
/// `seq-number` may be `*`. When `*` appears as the first element (`*:1`),
/// reversed-range normalization must put it after the concrete lower bound.
#[test]
fn seq_range_wildcard_reversed_normalized() {
    let (_, r) = seq_range(b"*:1").unwrap();
    assert_eq!(r.start, 1, "seq_range *:1 must normalize start to 1");
    assert_eq!(
        r.end_bound(),
        Some(crate::types::SeqBound::Star),
        "seq_range *:1 must preserve the wildcard as a typed bound"
    );
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

/// Verify that a full untagged OK with `[UNSEEN 0]` preserves the
/// response code structurally rather than losing it into the text field.
#[test]
fn unseen_zero_in_full_response_preserved() {
    let input = b"* OK [UNSEEN 0] No unseen messages\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Status { code, text, .. } => {
                assert_eq!(
                    code,
                    Some(ResponseCode::Unseen(0)),
                    "UNSEEN 0 response code must be preserved structurally, \
                         not consumed into the text field"
                );
                assert_eq!(text, "No unseen messages");
            }
            other => panic!("expected Status, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
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
    if let ResponseCode::CopyUid { uid_validity, .. } = &code {
        assert_eq!(*uid_validity, 0);
    } else {
        panic!("expected CopyUid, got {code:?}");
    }
}

#[test]
fn expunge_zero_rejected() {
    // message-data uses nz-number for EXPUNGE (RFC 3501 Section 7.4.1).
    // `number SP EXPUNGE` is a form this codec fully implements, so a
    // violation is a server contract violation, not an unknown extension: it
    // must surface as a parse failure the account boundary maps to
    // Protocol(ParseFailed).
    assert!(parse_response(b"* 0 EXPUNGE\r\n").is_err());
    // The same holds for EXISTS and RECENT, and through a tolerated run of
    // spaces after the number.
    assert!(parse_response(b"* 0 EXISTS trailing\r\n").is_err());
    assert!(parse_response(b"* 1  RECENT junk\r\n").is_err());
    // A numbered response we do not model stays an extension.
    assert!(matches!(
        parse_response(b"* 1 XSOMETHING (a b)\r\n"),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Unknown(_))
    ));
}

#[test]
fn fetch_seq_zero_rejected() {
    // message-data uses nz-number for FETCH (RFC 3501 Section 7.4.2). FETCH
    // is excluded from the numbered known-response guard because its msg-att
    // body is open-ended, but the sequence number itself is fully modelled:
    // zero is a contract violation, not an extension, and treating it as
    // `Unknown` would silently drop the whole FETCH.
    let input = b"* 0 FETCH (UID 1 FLAGS (\\Seen))\r\n";
    assert!(parse_response(input).is_err());
    // Control: a zero-numbered response with an unmodelled keyword is still
    // an extension.
    assert!(matches!(
        parse_response(b"* 0 XSOMETHING (a b)\r\n"),
        Ok((_, Response::Untagged(response))) if matches!(*response, UntaggedResponse::Unknown(_))
    ));
}

#[test]
fn exists_zero_still_valid() {
    // EXISTS uses number, not nz-number (RFC 3501 Section 7.3.1)
    let input = b"* 0 EXISTS\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(*boxed, UntaggedResponse::Exists(0));
    } else {
        panic!("expected Exists(0)");
    }
}

#[test]
fn recent_zero_still_valid() {
    // RECENT uses number, not nz-number (RFC 3501 Section 7.3.2)
    let input = b"* 0 RECENT\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(*boxed, UntaggedResponse::Recent(0));
    } else {
        panic!("expected Recent(0)");
    }
}

// ===== Tests for audit fixes =====

// H1: Bare `+\r\n` continuation request (RFC 9051 Section 7.6)
#[test]
fn continuation_bare_plus_crlf() {
    let input = b"+\r\n";
    let (remaining, response) = parse_response(input).unwrap();
    assert!(remaining.is_empty());
    match response {
        Response::Continuation(c) => assert_eq!(c.data, ""),
        _ => panic!("expected Continuation"),
    }
}

#[test]
fn continuation_with_space_and_text() {
    let input = b"+ Ready for literal\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Continuation(c) => assert_eq!(c.data, "Ready for literal"),
        _ => panic!("expected Continuation"),
    }
}

#[test]
fn continuation_bare_plus_base64() {
    // AUTHENTICATE continuation with base64 data after space
    let input = b"+ dGVzdA==\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Continuation(c) => assert_eq!(c.data, "dGVzdA=="),
        _ => panic!("expected Continuation"),
    }
}

// H2: MESSAGE/GLOBAL in BODYSTRUCTURE (RFC 9051 Section 7.5.2)
#[test]
fn bodystructure_message_global() {
    // message/global has the same structure as message/rfc822:
    // envelope body lines
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"MESSAGE\" \"GLOBAL\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 500 (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL) (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) 20))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            match fr.body_structure.unwrap() {
                BodyStructure::Message { lines, size, .. } => {
                    assert_eq!(lines, 20);
                    assert_eq!(size, 500);
                }
                other => panic!("expected Message variant, got {other:?}"),
            }
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// H3: BODYSTRUCTURE extension data with nested parentheses
#[test]
fn bodystructure_ext_nested_parens() {
    // Extension data with nested parenthesized values after location
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5 NIL NIL NIL NIL (\"ext\" (\"nested\" \"value\"))))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert!(fr.body_structure.is_some());
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn bodystructure_mpart_ext_nested_parens() {
    // Multipart with extension data containing nested parentheses
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5) \"MIXED\" (\"boundary\" \"----=_Part\") NIL NIL NIL (\"future-ext\" (\"a\" \"b\"))))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert!(fr.body_structure.is_some());
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// M1: RFC822.SIZE as u64 (RFC 9051 number64)
#[test]
fn fetch_rfc822_size_large_u64() {
    // Value exceeding u32::MAX
    let input = b"* 1 FETCH (RFC822.SIZE 5000000000)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.rfc822_size, Some(5_000_000_000u64));
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// L1: body-fld-lines as u64
#[test]
fn bodystructure_text_lines_u64() {
    // Technically extreme but valid per RFC 9051 number64
    let input =
        b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5000000000))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            match fr.body_structure.unwrap() {
                BodyStructure::Text { lines, .. } => assert_eq!(lines, 5_000_000_000u64),
                other => panic!("expected Text, got {other:?}"),
            }
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// M2: RFC822, RFC822.HEADER, RFC822.TEXT FETCH items
#[test]
fn fetch_rfc822_full_message() {
    let input = b"* 1 FETCH (RFC822 \"From: a@b.com\\r\\nHello\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.body_sections.len(), 1);
            assert_eq!(fr.body_sections[0].section, "");
            assert!(fr.body_sections[0].data.is_some());
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn fetch_rfc822_header() {
    let input = b"* 1 FETCH (RFC822.HEADER \"Subject: Test\\r\\n\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.body_sections.len(), 1);
            assert_eq!(fr.body_sections[0].section, "HEADER");
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn fetch_rfc822_text() {
    let input = b"* 1 FETCH (RFC822.TEXT \"Body content\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            assert_eq!(fr.body_sections.len(), 1);
            assert_eq!(fr.body_sections[0].section, "TEXT");
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// M3: DELETED STATUS item (RFC 9051)
#[test]
fn status_deleted_item() {
    let input = b"* STATUS INBOX (MESSAGES 10 DELETED 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::MailboxStatus { items, .. } = *boxed {
            assert!(items.iter().any(|i| matches!(i, StatusItem::Deleted(3))));
        } else {
            panic!("expected MailboxStatus");
        }
    } else {
        panic!("expected Untagged");
    }
}

// L3: STATUS UIDNEXT/UIDVALIDITY formerly rejected zero (nz-number). Now
// accepts 0 per Postel's law (RFC 1122 Section 1.2.2)  -  see the
// status_uidnext_zero_accepted / status_uidvalidity_zero_accepted tests below.

// Regression: non-conformant servers send UIDNEXT 0 / UIDVALIDITY 0 for empty
// mailboxes. Per Postel's law (RFC 1122 Section 1.2.2), we accept 0 even though
// RFC 9051 Section 9 says nz-number.
#[test]
fn status_uidnext_zero_accepted() {
    let input = b"* STATUS INBOX (UIDNEXT 0)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::MailboxStatus { mailbox, items } = *boxed {
                assert_eq!(mailbox.as_str(), "INBOX");
                assert_eq!(items, vec![StatusItem::UidNext(0)]);
            } else {
                panic!("expected MailboxStatus, got {boxed:?}");
            }
        }
        other => panic!("Expected Untagged(MailboxStatus), got {other:?}"),
    }
}

#[test]
fn status_uidvalidity_zero_accepted() {
    let input = b"* STATUS INBOX (UIDVALIDITY 0)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::MailboxStatus { mailbox, items } = *boxed {
                assert_eq!(mailbox.as_str(), "INBOX");
                assert_eq!(items, vec![StatusItem::UidValidity(0)]);
            } else {
                panic!("expected MailboxStatus, got {boxed:?}");
            }
        }
        other => panic!("Expected Untagged(MailboxStatus), got {other:?}"),
    }
}

// M12: SpecialUse::All
#[test]
fn special_use_all_attribute() {
    use crate::types::mailbox::{MailboxAttribute, MailboxInfo, SpecialUse};
    let info = MailboxInfo {
        name: MailboxName::new("All Mail").unwrap(),
        delimiter: Some('/'),
        attributes: vec![MailboxAttribute::All],
        ..Default::default()
    };
    assert_eq!(info.special_use(), Some(SpecialUse::All));
}

// M14: NAMESPACE delimiter multibyte UTF-8
#[test]
fn namespace_multibyte_delimiter() {
    // Use a 2-byte UTF-8 character as delimiter (U+00F7 division sign = 0xC3 0xB7)
    let mut input: Vec<u8> = b"* NAMESPACE ((\"\" ".to_vec();
    input.push(b'"');
    input.extend_from_slice(&[0xC3, 0xB7]); // ÷ in UTF-8
    input.push(b'"');
    input.extend_from_slice(b")) NIL NIL\r\n");
    let (_, resp) = parse_response(&input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Namespace { personal, .. } = *boxed {
            assert_eq!(personal[0].delimiter, Some('\u{00F7}'));
        } else {
            panic!("expected Namespace");
        }
    } else {
        panic!("expected Untagged");
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

// L2: Quoted string accepts any \X escape (deliberate leniency).
// RFC 3501 Section 9 says only \" and \\ are valid, but servers
// in the wild send other escapes. We accept them as literal X.
#[test]
fn quoted_string_lenient_escapes() {
    // RFC 3501 Section 9: only \" and \\ are valid quoted-specials.
    // A \n is malformed  -  the backslash is preserved as literal data
    // per Postel's law (the sender intended to include it).
    let input = b"\"hello\\nworld\"\r\n";
    let (_, val) = super::quoted_string(input).unwrap();
    assert_eq!(val, b"hello\\nworld");
}

// L13: NAMESPACE with extension data is parsed and preserved.
// RFC 2342 Section6 allows extension key-value pairs after the delimiter.
#[test]
fn namespace_with_extension_data_l13() {
    let input = b"* NAMESPACE ((\"\" \"/\" \"X-PARAM\" (\"FLAG1\" \"FLAG2\"))) NIL NIL\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Namespace { personal, .. } = *boxed {
            assert_eq!(personal.len(), 1);
            assert_eq!(personal[0].prefix, "");
            assert_eq!(personal[0].delimiter, Some('/'));
        } else {
            panic!("expected Namespace");
        }
    } else {
        panic!("expected Untagged");
    }
}

// L4: number64 is now capped at 2^63-1 per RFC 9051 Section 4.
// Values above i64::MAX are rejected. The malformed MODSEQ is skipped
// gracefully; the FETCH response is preserved with mod_seq = None
// (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn number64_rejects_above_i64_max() {
    // u64::MAX exceeds number64 range (RFC 9051 Section 4).
    // The MODSEQ attribute is skipped, but the FETCH response is preserved.
    let input = b"* 1 FETCH (MODSEQ (18446744073709551615))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = &*boxed {
            assert_eq!(
                fr.mod_seq, None,
                "u64::MAX MODSEQ should be skipped (None), not stored"
            );
        } else {
            panic!("expected Fetch with mod_seq=None, got {boxed:?}");
        }
    } else {
        panic!("Expected Untagged, got {resp:?}");
    }
}

/// literal count exceeding `i64::MAX` (number64 range per
/// RFC 9051 Section 9) must be rejected by the parser.
#[test]
fn regression_literal_count_exceeds_number64_range() {
    // i64::MAX + 1 = 9223372036854775808, which exceeds the number64 range.
    // Test the literal parser directly via a string context.
    // A quoted-or-literal string `{9223372036854775808}\r\n` should fail.
    let input = b"{9223372036854775808}\r\n";
    let result = literal(input);
    assert!(
        result.is_err(),
        "literal count exceeding i64::MAX must be rejected \
             (RFC 9051 Section 9: number64 = 0..2^63-1); got: {result:?}"
    );
}

// ===== Spec audit: tests for previously-known deviations =====

/// M1: RFC 9051 / RFC 3516 Section 4.3 defines `UNKNOWN-CTE` (with hyphen),
/// but the parser matches `"UNKNOWNCTE"` (no hyphen). The hyphenated form
/// falls through to `ResponseCode::Other`.
#[test]
fn spec_audit_m1_unknown_cte_hyphenated() {
    let input = b"* OK [UNKNOWN-CTE] unsupported encoding\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Status { code, .. } = &*u {
            assert_eq!(
                code.as_ref().unwrap(),
                &ResponseCode::UnknownCte,
                "UNKNOWN-CTE (with hyphen) should map to ResponseCode::UnknownCte, \
                     not Other"
            );
        } else {
            panic!("expected Status, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M2: ESEARCH unknown keys with parenthesized values are not skipped correctly.
/// The current skip logic only consumes a single token, so a parenthesized group
/// like `(1 2)` causes a parse failure because after consuming `(1` the inner
/// `2` becomes an "unknown key" whose `sp` call hits `)` and errors out.
#[test]
fn spec_audit_m2_esearch_parenthesized_unknown_value() {
    let input = b"* ESEARCH (TAG \"A001\") XFOO (1 2) COUNT 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(
                esearch.count,
                Some(5),
                "COUNT after an unknown key with parenthesized value should be parsed"
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M5: `* SORT 2 3 6` should parse into `UntaggedResponse::Sort { nums: vec![2, 3, 6], mod_seq: None }`
/// per RFC 5256 Section 4.
#[test]
fn spec_audit_m5_sort_response() {
    let input = b"* SORT 2 3 6\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(nums, &[2, 3, 6], "SORT response should contain [2, 3, 6]");
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 7162 Section 3.1.6: SORT response with MODSEQ trailer.
/// When a MODSEQ search criterion is used, the server MUST return
/// the MODSEQ result option: `sort-data = "SORT" *(SP nz-number)
/// [SP search-sort-mod-seq]` where `search-sort-mod-seq = "("
/// "MODSEQ" SP mod-sequence-value ")"`.
#[test]
fn sort_with_modseq() {
    let (_, resp) = parse_response(b"* SORT 2 5 6 (MODSEQ 917162500)\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        match &*u {
            UntaggedResponse::Sort { nums, mod_seq } => {
                assert_eq!(nums, &[2, 5, 6]);
                assert_eq!(*mod_seq, Some(917162500));
            }
            other => panic!("expected Sort, got {other:?}"),
        }
    } else {
        panic!("expected Untagged");
    }
}

/// L4: RFC 9051 Section 4 limits number64 to 2^63-1 (non-negative signed 63-bit).
/// The parser currently uses u64 and happily accepts values above `i64::MAX`.
#[test]
fn spec_audit_l4_number64_exceeds_63bit() {
    // 9999999999999999999 is > i64::MAX (9223372036854775807) but < u64::MAX
    let result = number64(b"9999999999999999999 rest");
    assert!(
        result.is_err(),
        "number64 should reject values > 2^63-1 per RFC 9051 Section 4, \
             but it accepted the value"
    );
}

/// L7: Unknown STATUS attributes (e.g. XFOO) cause a parse error instead of
/// being gracefully skipped.
#[test]
fn spec_audit_l7_status_unknown_attribute_skipped() {
    let input = b"* STATUS \"INBOX\" (MESSAGES 10 XFOO 42 UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            let messages = items.iter().find(|i| matches!(i, StatusItem::Messages(_)));
            let unseen = items.iter().find(|i| matches!(i, StatusItem::Unseen(_)));
            assert_eq!(
                messages,
                Some(&StatusItem::Messages(10)),
                "MESSAGES should be parsed despite unknown XFOO"
            );
            assert_eq!(
                unseen,
                Some(&StatusItem::Unseen(3)),
                "UNSEEN should be parsed despite unknown XFOO"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 9051 Section9: unknown STATUS attributes may have parenthesized values
/// (status-att-val-ext = tagged-ext-val, which includes "(" ... ")").
/// The parser must skip them without breaking subsequent items.
#[test]
fn spec_audit_status_unknown_parenthesized_value() {
    let input = b"* STATUS \"INBOX\" (MESSAGES 17 XFUTURE (some data) UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::MailboxStatus {
                ref mailbox,
                ref items,
            } => {
                assert_eq!(mailbox.as_str(), "INBOX");
                // Must see both MESSAGES and UNSEEN despite unknown XFUTURE in between
                let messages = items.iter().find_map(|i| match i {
                    StatusItem::Messages(n) => Some(*n),
                    _ => None,
                });
                let unseen = items.iter().find_map(|i| match i {
                    StatusItem::Unseen(n) => Some(*n),
                    _ => None,
                });
                assert_eq!(messages, Some(17), "MESSAGES should be parsed");
                assert_eq!(
                    unseen,
                    Some(3),
                    "UNSEEN after unknown parenthesized value should be parsed"
                );
            }
            other => panic!("expected MailboxStatus, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

#[test]
fn spec_audit_status_unknown_nested_parenthesized_value() {
    // Nested parentheses in the unknown value
    let input = b"* STATUS \"INBOX\" (MESSAGES 5 XCOMPLEX (a (b c) d) UIDNEXT 100)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::MailboxStatus { ref items, .. } => {
                let messages = items.iter().find_map(|i| match i {
                    StatusItem::Messages(n) => Some(*n),
                    _ => None,
                });
                let uidnext = items.iter().find_map(|i| match i {
                    StatusItem::UidNext(n) => Some(*n),
                    _ => None,
                });
                assert_eq!(messages, Some(5));
                assert_eq!(
                    uidnext,
                    Some(100),
                    "UIDNEXT after nested parenthesized value should be parsed"
                );
            }
            other => panic!("expected MailboxStatus, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// L15: Body extension data containing a literal `{3}\r\na)b` is not handled
/// by `skip_balanced_parens()`, which only knows about quoted strings and parens.
/// When the literal content contains `)`, `skip_balanced_parens` mistakes it for
/// the end of the body structure, causing a misparse.
#[test]
fn spec_audit_l15_body_extension_literal() {
    // A text BODYSTRUCTURE with an extension field that is a literal string
    // whose content contains a `)` character.
    // Fields: media-type, subtype, params, id, desc, encoding, size, lines,
    //         md5 (ext), dsp (ext), lang (ext), loc (ext), then extra extension literal.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") \
NIL NIL \"7BIT\" 100 5 NIL NIL NIL NIL {3}\r\na)b))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(_fr) = &*u {
            // Successfully parsed  -  the literal in extension data was handled.
        } else {
            panic!("expected Fetch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== Spec audit: leniency documentation tests =====

/// L3: Non-standard quoted-string escapes (e.g. `\n`) are accepted for
/// server compatibility per Postel's law. Only `\"` and `\\` are valid
/// per RFC 3501 Section 9.
#[test]
fn spec_audit_l3_nonstandard_escape_accepted() {
    // RFC 3501 Section 9: only \" and \\ are valid quoted-specials.
    // "\n" is a non-standard escape  -  parser accepts it gracefully
    // and preserves the backslash as literal data (Postel's law).
    let input = b"\"hello\\nworld\"";
    let (rest, val) = quoted_string(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(val, b"hello\\nworld");
}

/// L5: Bare `+\r\n` without SP is accepted for compatibility with servers
/// that send `+\r\n` for literal synchronization.
#[test]
fn spec_audit_l5_continuation_without_sp() {
    let input = b"+\r\n";
    let (rest, cont) = parse_continuation(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(cont.data, "");
}

/// L6: Atoms with bytes > 0x7F are accepted for compatibility with
/// non-conformant servers. RFC 3501 CHAR = %x01-7f but many servers
/// send non-ASCII bytes in atoms.
#[test]
fn spec_audit_l6_atom_with_high_bytes() {
    // An atom containing a byte > 0x7F (e.g. 0xC0, 0xE9)
    let input = b"\xc0\xe9abc rest";
    let (rest, val) = atom(input).unwrap();
    assert_eq!(rest, b" rest");
    assert_eq!(val, b"\xc0\xe9abc");
}

// ===== Audit finding #4: LIST-EXTENDED data parsed and discarded =====

/// RFC 9051 Section 7.2.2 / RFC 5258 Section 6: LIST may carry
/// `mbox-list-extended` items such as OLDNAME. The parser must not
/// fail on these. Extended data items (OLDNAME, CHILDINFO) are captured.
#[test]
fn audit_finding4_list_extended_oldname_parses_without_error() {
    // LIST response with OLDNAME extended data (RFC 9051 Section 6.3.9.7).
    let input = b"* LIST (\\HasNoChildren) \"/\" \"NewName\" (\"OLDNAME\" (\"OldName\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "NewName");
            assert_eq!(info.delimiter, Some('/'));
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("OldName"),
                "OLDNAME should be captured in MailboxInfo"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with CHILDINFO extended data (RFC 5258 Section 4).
#[test]
fn audit_finding4_list_extended_childinfo_parsed() {
    let input = b"* LIST (\\HasChildren) \"/\" \"INBOX\" (\"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(
                info.child_info,
                vec!["SUBSCRIBED"],
                "CHILDINFO should be captured in MailboxInfo"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 5258 Section 6: CHILDINFO formally requires ≥1 item, but empty `()`
/// is accepted per Postel's law.  An empty CHILDINFO is treated as absent.
#[test]
fn spec_audit_childinfo_empty_accepted() {
    let input = b"* LIST (\\HasChildren) \"/\" \"INBOX\" (\"CHILDINFO\" ())\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "parse must consume the full response");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert!(
                info.child_info.is_empty(),
                "empty CHILDINFO () should produce empty vec"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with multiple extended data items.
#[test]
fn audit_finding4_list_extended_multiple_items_parsed() {
    let input = b"* LIST (\\HasChildren) \"/\" \"folder\" (\"OLDNAME\" (\"old\") \"CHILDINFO\" (\"SUBSCRIBED\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "folder");
            assert_eq!(info.old_name.as_ref().map(MailboxName::as_str), Some("old"));
            assert_eq!(info.child_info, vec!["SUBSCRIBED"]);
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST without extended data still parses normally.
#[test]
fn audit_finding4_list_without_extended_data_still_works() {
    let input = b"* LIST (\\Noselect) \"/\" \"Archive\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "Archive");
            assert_eq!(info.delimiter, Some('/'));
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with an unknown extended item whose value is a simple number
/// (not parenthesized). Per RFC 5258 Section 6 and RFC 9051 Section 9,
/// `tagged-ext-val` can be `tagged-ext-simple` (a number64 or sequence-set),
/// which is NOT wrapped in parentheses.
#[test]
fn test_list_extended_unknown_simple_value() {
    let input = b"* LIST (\\HasNoChildren) \"/\" \"INBOX\" (\"x-future-ext\" 42)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
            assert_eq!(info.delimiter, Some('/'));
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with an unknown extended item whose value is a sequence-set
/// (not parenthesized). Per RFC 9051 Section 9,
/// `tagged-ext-simple = sequence-set / number64`.
#[test]
fn test_list_extended_unknown_sequence_set_value() {
    let input = b"* LIST (\\HasNoChildren) \"/\" \"INBOX\" (\"x-seq-ext\" 1:5,8:*)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "INBOX");
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// LIST with a mix of known and unknown extended items, where the
/// unknown has a simple value and is followed by a known item.
#[test]
fn test_list_extended_unknown_simple_then_known() {
    let input =
        b"* LIST (\\HasChildren) \"/\" \"folder\" (\"x-count\" 99 \"OLDNAME\" (\"old\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "should consume entire input");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = &*u {
            assert_eq!(info.name.as_str(), "folder");
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("old"),
                "OLDNAME after unknown simple item should still be parsed"
            );
        } else {
            panic!("expected List, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
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

/// A future METADATA sub-code that carries additional data (e.g.,
/// `[METADATA XFOO 42]`) must not cause the entire response code
/// parse to fail. The unknown sub-code fallback must consume any
/// trailing value before the closing `]`, matching the generic
/// unknown response code handler's behavior.
///
/// Regression: the METADATA `_` arm returned `ResponseCode::Other`
/// with `value: None` without consuming trailing data, causing
/// `char(']')` to fail on ` 42]`.
#[test]
fn metadata_unknown_subcode_with_value() {
    let input = b"A001 NO [METADATA XFOO 42] some text\r\n";
    let (rest, resp) = parse_response(input).expect(
        "METADATA unknown sub-code with value must parse \
             (forward compatibility per RFC 5464)",
    );
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert!(
            tagged.code.is_some(),
            "response code must be preserved, got None"
        );
    } else {
        panic!("expected Tagged");
    }
}

/// RFC 5464 Section 4.2.1: When a non-conformant server sends non-ASCII
/// bytes in the METADATA sub-atom (e.g., `LONGENTRI\x80S`), the parser
/// should still produce a meaningful `ResponseCode::Other` with the
/// lossy-decoded sub-atom rather than failing the response code parse
/// entirely.
///
/// With `unwrap_or("")`, the sub-atom becomes `""` and falls through to
/// `ResponseCode::Other { name: "METADATA ", value: None }`  -  but this
/// leaves unconsumed bytes (` 2048`) before the closing `]`, causing the
/// response code parser to fail. The tagged response then has `code: None`,
/// losing the response code entirely.
///
/// With `from_utf8_lossy`, the replacement characters keep the string
/// non-empty, and unrecognized sub-codes produce a meaningful `Other` name
/// that correctly consumes the remaining value text.
#[test]
fn metadata_sub_atom_non_ascii_uses_lossy_conversion() {
    // Sub-atom "TOOMANY" with a non-ASCII byte: "TOOMAN\x80Y"
    // 0x80 alone is an invalid UTF-8 continuation byte without a leader.
    // Using TOOMANY because it has no trailing number, so the parser
    // only needs to match the sub-atom and then hit `]`.
    let input = b"A001 NO [METADATA TOOMAN\x80Y] too many annotations\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Tagged(tagged) = resp {
        assert_eq!(tagged.status, StatusKind::No);
        // With from_utf8_lossy: sub = "TOOMAN\u{FFFD}Y" (uppercased),
        // which doesn't match "TOOMANY", so falls to Other with a
        // meaningful name like "METADATA TOOMAN\u{FFFD}Y".
        //
        // With unwrap_or(""): sub = "", name = "METADATA ", and the
        // response code parse fails entirely -> code is None.
        assert!(
            tagged.code.is_some(),
            "response code should be Some (not None); \
                 unwrap_or(\"\") causes the response code parser to fail entirely"
        );
        match tagged.code {
            Some(ResponseCode::Other { ref name, .. }) => {
                assert!(
                    name.starts_with("METADATA TOOMAN"),
                    "sub-atom should be preserved via lossy conversion, got: {name:?}"
                );
            }
            other => {
                panic!("expected ResponseCode::Other with METADATA prefix, got {other:?}");
            }
        }
    } else {
        panic!("expected Tagged, got {resp:?}");
    }
}

// ==============================================================    // Audit tests (2026-03-16)
// ==============================================================
/// H1: METADATA entry names must accept `astring`, not just `string`.
/// RFC 5464 Section 5: `entry = astring`.
#[test]
fn audit_h1_metadata_entry_names_accept_astring() {
    let input = b"* METADATA \"INBOX\" (/shared/comment \"A shared comment\")\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Metadata { mailbox, entries } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].name, "/shared/comment");
        } else {
            panic!("expected Metadata, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// H2: Unsolicited METADATA notifications use `entry-list` without parens.
/// RFC 5464 Section 4.4.2.
#[test]
fn audit_h2_metadata_unsolicited_entry_list_no_parens() {
    let input = b"* METADATA \"INBOX\" /shared/comment /private/comment\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Metadata { mailbox, entries } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].name, "/shared/comment");
            assert_eq!(entries[1].name, "/private/comment");
            assert!(entries[0].value.is_none());
            assert!(entries[1].value.is_none());
        } else {
            panic!("expected Metadata, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M1: ESEARCH `tag-string` must accept `astring`.
/// RFC 9051 Section 9: `tag-string = astring`.
#[test]
fn audit_m1_esearch_tag_string_accepts_astring() {
    let input = b"* ESEARCH (TAG A001) UID COUNT 5\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(e) = &*u {
            assert_eq!(e.tag.as_deref(), Some("A001"));
            assert!(e.uid);
            assert_eq!(e.count, Some(5));
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M2: THREADID NIL must be case-insensitive.
/// RFC 8474 Section 4.
#[test]
fn audit_m2_threadid_mixed_case_nil() {
    let input = b"(THREADID Nil UID 42)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert!(fr.thread_id.is_none(), "THREADID Nil should parse as None");
    assert_eq!(fr.uid, Some(42));
}

/// THREADID NIL match must verify a token boundary follows "NIL".
/// Without a delimiter check, `tag_no_case(&b"NIL"[..])` would greedily match
/// the first 3 bytes of a token like "NIL2", incorrectly yielding
/// `thread_id = None` and leaving unparsed residue that gets silently
/// consumed by the unknown-attribute handler.
/// RFC 8474 Section 4 / RFC 3501 Section 9 (formal syntax: atoms are
/// delimited by SP, "(", ")", CR, and other specials).
#[test]
fn threadid_nil_requires_token_boundary() {
    // "NIL2" is not the NIL token. Without the boundary check, the parser
    // would match "NIL" from "NIL2", set thread_id = None, and the residue
    // "2 42" would be silently consumed by the unknown-attribute handler.
    //
    // With the fix, the NIL branch correctly rejects "NIL2" (no token
    // boundary after "NIL"), so the parser falls through to the
    // parenthesized "(objectid)" branch, which fails because "NIL2" does
    // not start with "(". The overall parse must therefore error.
    let input = b"(UID 5 THREADID NIL2 42)";
    assert!(
        fetch_response_inner(input, false).is_err(),
        "THREADID NIL2 must not be parsed as THREADID NIL: \
             'NIL2' is not the NIL token (RFC 8474 Section 4, RFC 3501 Section 9)"
    );
}

/// THREADID NIL followed by SP and another attribute must parse correctly.
/// RFC 8474 Section 4.
#[test]
fn threadid_nil_followed_by_sp_attr() {
    let input = b"(THREADID NIL UID 42)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert!(fr.thread_id.is_none(), "THREADID NIL should parse as None");
    assert_eq!(fr.uid, Some(42));
}

/// THREADID NIL at end of FETCH (followed by ')') must parse correctly.
/// RFC 8474 Section 4.
#[test]
fn threadid_nil_at_end_of_fetch() {
    let input = b"(UID 5 THREADID NIL)";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.uid, Some(5));
    assert!(fr.thread_id.is_none());
}

/// M3: `skip_paren_group` must handle literals.
/// RFC 3501 Section 9: body-extension may contain nstring with literals.
#[test]
fn audit_m3_skip_paren_group_handles_literal() {
    let input = b"({5}\r\nhel)o) rest";
    let (rest, _) = skip_paren_group(input).unwrap();
    assert_eq!(rest, b" rest");
}

/// M4: THREAD member UIDs must use `nz-number`  -  UID 0 is invalid.
/// RFC 5256 Section 5.
#[test]
fn audit_m4_thread_rejects_zero_uid() {
    // THREAD must reject UID 0 per RFC 5256.
    let input = b"* THREAD (5 0 3)\r\n";
    assert!(parse_response(input).is_err());
}

/// M5: BINARY section parts must use `nz-number`.
/// RFC 3516 Section 7.
#[test]
fn audit_m5_binary_section_rejects_zero_part() {
    assert!(binary_section_spec(b"[0]").is_err());
}

#[test]
fn audit_m5_binary_section_rejects_zero_in_dotted_path() {
    assert!(binary_section_spec(b"[1.0.3]").is_err());
}

/// `binary_section_spec` accepts empty section `[]` (RFC 9051 Section 9).
///
/// RFC 9051 Section 9: `section-binary = "[" [section-part] "]"`  -  the
/// section-part is optional, so `[]` is valid and yields an empty vec.
#[test]
fn binary_section_spec_accepts_empty() {
    let (rest, parts) = binary_section_spec(b"[]").unwrap();
    assert!(rest.is_empty());
    assert!(parts.is_empty());
}

/// `binary_section_spec` accepts a valid dotted section path (RFC 3516 Section 4).
#[test]
fn binary_section_spec_valid_dotted_path() {
    let (rest, parts) = binary_section_spec(b"[1.2.3]").unwrap();
    assert!(rest.is_empty());
    assert_eq!(parts, vec![1, 2, 3]);
}

/// M11: `METADATA-SERVER` should be a dedicated Capability variant.
/// RFC 5464 Section 1.
#[test]
fn audit_m11_metadata_server_capability() {
    let (_, cap) = capability(b"METADATA-SERVER").unwrap();
    assert!(matches!(cap, Capability::MetadataServer));
}

/// L5: `nz-number` must reject leading zeros.
/// RFC 3501 Section 9: `nz-number = digit-nz *DIGIT`.
#[test]
fn audit_l5_nz_number_rejects_leading_zeros() {
    assert!(nz_number(b"007 ").is_err());
}

/// L6: MODSEQ in FETCH must be >= 1.
/// RFC 7162 Section 7.
/// A malformed MODSEQ (0) must be skipped gracefully, and other
/// attributes (e.g. UID) must be preserved (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn audit_l6_fetch_modseq_accepts_zero() {
    // RFC 7162 Section 3.1.3 formally says mod-sequence-value >= 1, but
    // we accept 0 per Postel's law (RFC 1122 Section 1.2.2) for
    // consistency with the STATUS HIGHESTMODSEQ parser.
    let input = b"(UID 1 MODSEQ (0))";
    let result = fetch_response_inner(input, false);
    let (_, fr) = result.expect("FETCH with MODSEQ (0) must parse  -  0 accepted per Postel's law");
    assert_eq!(
        fr.mod_seq,
        Some(0),
        "MODSEQ 0 must be accepted per Postel's law (should be Some(0), not None)"
    );
    assert_eq!(
        fr.uid,
        Some(1),
        "UID must be preserved alongside MODSEQ (0)"
    );
}

/// L6: HIGHESTMODSEQ response code value must be >= 1.
/// RFC 7162 Section 3.1.2.
#[test]
fn audit_l6_highestmodseq_response_code_accepts_zero() {
    // RFC 7162 Section 3.1.2: formally mod-sequence-value >= 1, but
    // real servers (Cyrus, Dovecot) send [HIGHESTMODSEQ 0] for empty/new
    // mailboxes. Accepted per Postel's law to preserve structured data.
    let input = b"* OK [HIGHESTMODSEQ 0] done\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Status { code, text, .. } => {
                assert_eq!(code, Some(ResponseCode::HighestModSeq(0)));
                assert_eq!(text, "done");
            }
            other => panic!("expected Status, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// L7: NAMESPACE with empty `()` should be accepted per Postel's law.
/// RFC 2342 Section 6 strictly requires `1*` (at least one descriptor),
/// but non-conformant servers send `()` instead of `NIL` for empty
/// namespace categories. Accepted per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn audit_l7_namespace_accepts_empty_parens() {
    // Empty parens in NAMESPACE are technically invalid per the ABNF,
    // but accepted per Postel's law to avoid losing the entire response.
    let input = b"* NAMESPACE () NIL NIL\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::Namespace {
                personal,
                other,
                shared,
            } = *boxed
            {
                assert!(
                    personal.is_empty(),
                    "`()` should produce empty personal namespace"
                );
                assert!(other.is_empty());
                assert!(shared.is_empty());
            } else {
                panic!("expected Namespace, got {boxed:?}");
            }
        }
        other => panic!("Expected Untagged(Namespace), got {other:?}"),
    }
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

/// L13: EMAILID never allows NIL per RFC 8474 Section 7 formal syntax:
///   fetch-emailid-resp = "EMAILID" SP "(" objectid ")"
/// Only THREADID has the `nil` alternative. When a server sends EMAILID NIL,
/// the parser must reject it (the `(` parser fails on NIL).
#[test]
fn audit_l13_emailid_nil_rejected() {
    let input = b"(EMAILID NIL UID 42)";
    let result = fetch_response_inner(input, false);
    assert!(
        result.is_err(),
        "EMAILID NIL must be rejected per RFC 8474 Section 7: \
             only THREADID allows NIL, got: {result:?}"
    );
}

/// L14: `body-fld-lang` with empty `()` should be rejected.
/// RFC 3501 Section 9.
#[test]
fn audit_l14_body_language_rejects_empty_parens() {
    let result = body_language(b"()");
    // After fix: either rejects outright, or falls back gracefully
    if let Ok((_, val)) = result {
        // If it parses, the result should be empty/None (graceful fallback)
        assert!(
            val.as_ref().is_none_or(std::vec::Vec::is_empty),
            "empty () should not produce non-empty language list"
        );
    }
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

#[test]
fn audit_sort_unknown_variant_preserved_as_other() {
    let (_, cap) = capability(b"SORT=XYZZY").unwrap();
    assert!(
        matches!(cap, Capability::Other(ref raw) if raw == "SORT=XYZZY"),
        "unknown SORT=* capability must be preserved as Other, got {cap:?}"
    );
}

/// RFC 9051 Section 9: sequence-set requires at least one element.
#[test]
fn spec_audit_sequence_set_rejects_empty() {
    // Empty input should fail to parse as a sequence-set
    assert!(
        sequence_set(b"").is_err(),
        "empty sequence-set must be rejected per RFC 9051 Section 9"
    );
}

/// RFC 9051 Section 9: nz-number64 = digit-nz *DIGIT  -  leading zeros are invalid.
#[test]
fn spec_audit_nz_number64_rejects_leading_zeros() {
    // "01" starts with '0', violating digit-nz requirement
    assert!(
        nz_number64(b"01").is_err(),
        "nz-number64 must reject leading zeros per RFC 9051 Section 9"
    );
    assert!(
        nz_number64(b"007").is_err(),
        "nz-number64 must reject leading zeros per RFC 9051 Section 9"
    );
    // "0" alone should also fail (it's zero)
    assert!(nz_number64(b"0").is_err(), "nz-number64 must reject zero");
    // Valid inputs should still work
    assert_eq!(nz_number64(b"1").unwrap().1, 1);
    assert_eq!(nz_number64(b"42").unwrap().1, 42);
}

/// RFC 9051 Section 2.2.2: Clients MUST tolerate unrecognized untagged responses.
#[test]
fn spec_audit_unknown_untagged_response() {
    // Unknown extension response must be parsed without error
    let input = b"* XFOOBAR some extension data\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert_eq!(text, "XFOOBAR some extension data");
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// `BodyStructure::Message` must expose a `media_subtype` field
/// so clients can distinguish `message/rfc822` from `message/global`
/// (RFC 9051 Section 7.5.2: media-message = DQUOTE "MESSAGE" DQUOTE SP
///  DQUOTE ("RFC822" / "GLOBAL") DQUOTE).
#[test]
fn message_global_subtype() {
    let input = b"(\"MESSAGE\" \"GLOBAL\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 500 \
            (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL) \
            (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) 20)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Message {
        media_subtype,
        size,
        lines,
        ..
    } = bs
    {
        assert_eq!(
            media_subtype, "global",
            "subtype must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(size, 500);
        assert_eq!(lines, 20);
    } else {
        panic!("expected Message variant, got {bs:?}");
    }
}

/// Another unknown response: extension with parenthesized data.
#[test]
fn spec_audit_unknown_untagged_with_parens() {
    let input = b"* XANNOTATION \"inbox\" (value.shared \"test\")\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XANNOTATION"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// tagged response with no text after status keyword.
///
/// Many real servers send bare `"A001 OK\r\n"` without any trailing text.
/// While RFC 3501 Section 7.1 formally requires `resp-cond-state = status SP resp-text`,
/// Postel's law dictates we accept this common variant.
#[test]
fn tagged_ok_no_text() {
    let input = b"A001 OK\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Tagged(t) = resp {
        assert_eq!(t.tag, "A001");
        assert_eq!(t.status, StatusKind::Ok);
        assert!(t.code.is_none());
        assert!(t.text.is_empty());
    } else {
        panic!("expected Tagged response, got {resp:?}");
    }
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

/// RFC 7162 Section3.1.6 / RFC 5234 Section2.3: MODSEQ keyword in search-sort-mod-seq
/// must be matched case-insensitively per ABNF rules.
#[test]
fn spec_audit_search_modseq_case_insensitive() {
    // Lowercase "modseq"  -  must still be parsed
    let input = b"* SEARCH 2 5 (modseq 917162500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Search { uids, mod_seq } => {
                assert_eq!(uids, vec![2, 5]);
                assert_eq!(mod_seq, Some(917162500));
            }
            other => panic!("expected Search, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

#[test]
fn spec_audit_sort_modseq_case_insensitive() {
    // Mixed case "Modseq"  -  must still be parsed
    let input = b"* SORT 2 3 6 (Modseq 917162500)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Sort { nums, mod_seq } => {
                assert_eq!(nums, vec![2, 3, 6]);
                assert_eq!(mod_seq, Some(917162500));
            }
            other => panic!("expected Sort, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Postel's law (RFC 2342 Section 6): a non-conformant server sending a
/// multi-character namespace delimiter (e.g., "ab") should not cause a hard
/// parse failure. The parser takes the first character and tolerates the rest.
#[test]
fn test_namespace_tolerates_multichar_delimiter() {
    let input = b"* NAMESPACE ((\"\" \"ab\")) NIL NIL\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => {
            if let UntaggedResponse::Namespace { personal, .. } = *boxed {
                assert_eq!(personal.len(), 1);
                assert_eq!(
                    personal[0].delimiter,
                    Some('a'),
                    "multi-char namespace delimiter should take the first character"
                );
            } else {
                panic!("expected Namespace response, got {boxed:?}");
            }
        }
        other => panic!("expected Untagged(Namespace), got {other:?}"),
    }
}

/// non-conformant servers send `()` instead of `NIL` for an empty
/// namespace category. RFC 2342 Section 6 strictly requires at least one
/// descriptor in the parenthesized form, but per Postel's law
/// (RFC 1122 Section 1.2.2) we should accept this gracefully.
#[test]
fn spec_audit_namespace_empty_parens() {
    // "other users" namespace is `()` instead of NIL
    let input = b"* NAMESPACE ((\"\" \"/\")) () NIL\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "namespace `()` must be accepted per Postel's law; got parse error: {result:?}"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => {
            if let UntaggedResponse::Namespace {
                personal,
                other,
                shared,
            } = *inner
            {
                assert_eq!(personal.len(), 1, "should have one personal namespace");
                assert_eq!(personal[0].prefix, "");
                assert_eq!(personal[0].delimiter, Some('/'));
                assert!(
                    other.is_empty(),
                    "`()` should produce empty other namespace"
                );
                assert!(
                    shared.is_empty(),
                    "NIL should produce empty shared namespace"
                );
            } else {
                panic!("expected Namespace, got {inner:?}");
            }
        }
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// servers send `()` for empty body-fld-param in BODYSTRUCTURE.
/// RFC 3501 Section 9 formal syntax requires at least one key-value pair,
/// but Postel's law demands we accept the empty form.
#[test]
fn spec_audit_body_params_empty_parenthesized() {
    // BODYSTRUCTURE with empty params `()` instead of NIL
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" () NIL NIL \"7BIT\" 42 3))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "body-fld-param `()` must be accepted per Postel's law; got parse error"
    );
    // Verify the params are parsed as empty vec
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text { params, .. } => {
                        assert!(
                            params.is_empty(),
                            "empty () should produce empty params vec"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// BODYSTRUCTURE disposition with empty params `("inline" ())`
/// instead of `("inline" NIL)`.
#[test]
fn spec_audit_body_disposition_empty_params() {
    // Single-part with disposition that has empty param list
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5 NIL (\"inline\" ()) NIL NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "disposition with empty params `()` must be accepted; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text { disposition, .. } => {
                        let disp = disposition.as_ref().expect("should have disposition");
                        assert_eq!(disp.disposition_type, "inline");
                        assert!(
                            disp.params.is_empty(),
                            "empty () params should produce empty vec"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// multipart BODYSTRUCTURE extension data with empty params `()`.
#[test]
fn spec_audit_multipart_empty_ext_params() {
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 20 2) \"ALTERNATIVE\" () NIL NIL NIL))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "multipart body-fld-param `()` must be accepted; got parse error"
    );
    let (_, resp) = result.unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Multipart { params, .. } => {
                        assert!(
                            params.is_empty(),
                            "empty () should produce empty params vec"
                        );
                    }
                    other => panic!("expected Multipart variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== FETCH during IDLE must preserve data (RFC 2177 Section 3) =====

/// RFC 2177 Section 3: the server can send `* n FETCH (FLAGS (...))` during
/// IDLE when message attributes change.  The parser must produce a
/// `FetchResponse` with the correct sequence number and flags so the
/// `IdleEvent::Fetch` variant can carry the full data to callers.
///
/// Bug fix: the IDLE handler discarded the the IDLE handler discarded the
/// `FetchResponse` and returned a unit `FlagsChanged` variant, losing the
/// sequence number and flag data.
#[test]
fn fetch_response_preserves_seq_and_flags_for_idle() {
    // Typical unsolicited FETCH the server sends during IDLE when another
    // session changes flags on message 5.
    let input = b"* 5 FETCH (FLAGS (\\Seen \\Answered) UID 300)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty(), "parser should consume all input");

    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                // RFC 3501 Section 7.4.2: sequence number from the
                // `* n FETCH` prefix.
                assert_eq!(fr.seq, 5, "sequence number must be preserved");
                assert_eq!(fr.uid, Some(300), "UID must be preserved");

                let flags = fr.flags.as_ref().expect("FLAGS must be present");
                assert!(
                    flags.contains(&Flag::Seen),
                    "expected \\Seen in flags, got {flags:?}"
                );
                assert!(
                    flags.contains(&Flag::Answered),
                    "expected \\Answered in flags, got {flags:?}"
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 9051 Section 7.5.2: body-fld-octcnt is number64 (not number).
/// "This accommodates data items that are larger than 4GB."
///
/// The BODYSTRUCTURE `size` field must be u64 to handle parts > 4 GiB.
/// This test verifies that a size value exceeding `u32::MAX` (4,294,967,295)
/// parses as a proper FETCH with BODYSTRUCTURE (not silently falling back
/// to `UntaggedResponse::Unknown`).
#[test]
fn spec_audit_bodystructure_size_number64() {
    // A text/plain BODYSTRUCTURE with size = 5,000,000,000 (> u32::MAX)
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 5000000000 100))\r\n";
    let (_, resp) = parse_response(input).expect("should parse");
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect(
                    "BODYSTRUCTURE must be present  -  size > u32::MAX must not \
                         cause a parse failure per RFC 9051 Section 7.5.2",
                );
                match bs {
                    BodyStructure::Text { size, lines, .. } => {
                        assert_eq!(
                            size, 5_000_000_000u64,
                            "body-fld-octcnt must support number64 per \
                                 RFC 9051 Section 7.5.2"
                        );
                        assert_eq!(lines, 100);
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            UntaggedResponse::Unknown(_) => {
                panic!(
                    "BODYSTRUCTURE with size > u32::MAX was silently discarded \
                         as Unknown  -  body-fld-octcnt must be number64 per \
                         RFC 9051 Section 7.5.2"
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 9051 Section 7.5.2: body-fld-octcnt = number64 for basic parts.
#[test]
fn spec_audit_bodystructure_basic_size_number64() {
    // An image/png BODYSTRUCTURE with size = 6,000,000,000 (> u32::MAX)
    let input =
        b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 6000000000))\r\n";
    let (_, resp) = parse_response(input).expect("should parse");
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("BODYSTRUCTURE must be present");
                match bs {
                    BodyStructure::Basic { size, .. } => {
                        assert_eq!(
                            size, 6_000_000_000u64,
                            "body-fld-octcnt must support number64 per \
                                 RFC 9051 Section 7.5.2"
                        );
                    }
                    other => panic!("expected Basic variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 2045 Section 6: Content-Transfer-Encoding values are not case
/// sensitive. The parser must lowercase them for canonical form per
/// RFC 2045 Section 5.1, consistent with media type/subtype.
#[test]
fn spec_audit_encoding_case_normalized() {
    // Server sends mixed-case encoding "Quoted-Printable"; parser must
    // lowercase it to "quoted-printable" per RFC 2045 Section 6.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" \
            (\"CHARSET\" \"UTF-8\") NIL NIL \"Quoted-Printable\" 100 5))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("missing BODYSTRUCTURE");
                match bs {
                    BodyStructure::Text { encoding, .. } => {
                        assert_eq!(
                            encoding, "quoted-printable",
                            "RFC 2045 Section 6: encoding must be lowercased"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 2183 Section 2: Content-Disposition type is not case sensitive.
/// The parser must lowercase it for consistent comparison, using the
/// conventional form ("inline", "attachment") from RFC examples.
#[test]
fn spec_audit_disposition_type_case_normalized() {
    // Server sends mixed-case disposition type "Attachment"; parser must
    // lowercase it to "attachment" per RFC 2183 Section 2.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" NIL NIL NIL \
            \"7BIT\" 100 5 NIL (\"Attachment\" (\"FILENAME\" \"test.txt\")) NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("missing BODYSTRUCTURE");
                match bs {
                    BodyStructure::Text { disposition, .. } => {
                        let disp = disposition.expect("missing disposition");
                        assert_eq!(
                            disp.disposition_type, "attachment",
                            "RFC 2183 Section 2: disposition type must be lowercased"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 2183 Section 2: disposition type is case-insensitive and must be
/// normalized to lowercase (the conventional form used in RFC examples
/// and real-world implementations).  Consumers comparing against
/// "attachment" or "inline" (the standard forms) must match.
#[test]
fn disposition_type_lowercased_for_conventional_comparison() {
    // Server sends uppercase "ATTACHMENT"; parser must lowercase it.
    let (_, disp) = body_disposition(b"(\"ATTACHMENT\" NIL)").unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(
        disp.disposition_type, "attachment",
        "RFC 2183 Section 2: disposition type must be lowercased to \
             conventional form for consistent comparison"
    );

    // Server sends mixed-case "Inline"; parser must lowercase it.
    let (_, disp) = body_disposition(b"(\"Inline\" NIL)").unwrap();
    let disp = disp.expect("should not be None");
    assert_eq!(
        disp.disposition_type, "inline",
        "RFC 2183 Section 2: disposition type must be lowercased to \
             conventional form for consistent comparison"
    );
}

/// RFC 2045 Section 5.1: MIME parameter names are not case sensitive.
/// The parser must lowercase them for consistent lookup.
#[test]
fn spec_audit_param_names_lowercased() {
    // Server sends uppercase parameter name "CHARSET"; parser must
    // lowercase it to "charset" per RFC 2045 Section 5.1.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" \
            (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Fetch(fr) => {
                let bs = fr.body_structure.expect("missing BODYSTRUCTURE");
                match bs {
                    BodyStructure::Text { params, .. } => {
                        assert_eq!(params.len(), 1);
                        assert_eq!(
                            params[0].0, "charset",
                            "RFC 2045 Section 5.1: parameter names must be lowercased"
                        );
                    }
                    other => panic!("expected Text variant, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 3501 Section 9: FETCH FLAGS uses `flag-fetch = flag / "\Recent"`,
/// which does NOT include `\*`.  The `\*` wildcard is only valid in
/// `flag-perm` (PERMANENTFLAGS response code).
///
/// The `flag` production itself excludes `\*` because `*` is a
/// list-wildcard (atom-special) and therefore not a valid ATOM-CHAR,
/// so `\*` does not match `flag-extension = "\" atom`.
#[test]
fn fetch_flags_must_exclude_wildcard() {
    let input = b"(FLAGS (\\Seen \\*))";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    let flags = fr.flags.expect("flags must be present");
    assert!(
        !flags.contains(&Flag::Wildcard),
        "FETCH FLAGS must not accept \\*  -  \\* is only valid in \
             flag-perm (PERMANENTFLAGS), not flag-fetch (RFC 3501 Section 9). \
             Got: {flags:?}"
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

/// RFC 9051 Section 7.5.2: partial fetch origins can exceed u32 for
/// messages > 4 GiB (RFC822.SIZE is number64).
#[test]
fn body_section_origin_u64() {
    let input = b"* 1 FETCH (BODY[]<5000000000> {5}\r\nhello)\r\n";
    let (_, resp) = parse_response(input).expect("should parse u64 origin");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.body_sections.len(), 1);
                assert_eq!(fr.body_sections[0].origin, Some(5_000_000_000u64));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 9051 Section 7.5.2: partial fetch origins can exceed u32.
#[test]
fn binary_section_origin_u64() {
    let input = b"* 1 FETCH (BINARY[1]<5000000000> {5}\r\nhello)\r\n";
    let (_, resp) = parse_response(input).expect("should parse u64 BINARY origin");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.binary_sections.len(), 1);
                assert_eq!(fr.binary_sections[0].origin, Some(5_000_000_000u64));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// ===== parse_untagged_unknown must skip literals (RFC 9051 Section2.2.2) =====

/// RFC 9051 Section 2.2.2: unknown response containing a literal `{N}\r\n<data>`
/// must be fully consumed. The naive `take_while` scanner stops at the literal's
/// CRLF, leaving data in the buffer (RFC 3501 Section 9: literal = "{" number "}" CRLF *CHAR8).
#[test]
fn unknown_with_literal() {
    let input = b"* XFOO {5}\r\nhello\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with literal");
    assert!(
        rem.is_empty(),
        "remaining bytes should be empty, got {:?}",
        String::from_utf8_lossy(rem)
    );
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(
                    text.starts_with("XFOO"),
                    "Unknown text should start with XFOO, got: {text}"
                );
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// Baseline: unknown response with a quoted string (no literal) should still work.
#[test]
fn unknown_with_quoted_string() {
    let input = b"* XBAR \"quoted\"\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with quoted string");
    assert!(rem.is_empty());
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XBAR"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// RFC 3516 / RFC 6855 Section 4: literal8 `~{N}\r\n<data>` in unknown response.
#[test]
fn unknown_with_literal8() {
    let input = b"* XBAZ ~{3}\r\nabc\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with literal8");
    assert!(
        rem.is_empty(),
        "remaining bytes should be empty, got {:?}",
        String::from_utf8_lossy(rem)
    );
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XBAZ"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// Regression: a quoted string ending with a backslash right before the
/// response-terminating CRLF in `scan_unknown_response` causes the `pos += 2`
/// to skip over `\r`, swallowing the response terminator. The outer loop then
/// cannot find a proper CRLF, causing a parse failure on well-formed input.
///
/// The fix adds a bounds check before advancing (mirroring `skip_balanced_parens`
/// at line ~162 and `quoted_string` at line ~318): if the next byte is `\r`, `\n`,
/// or past end-of-input, the escape is truncated and the scanner must break out
/// of the quoted-string loop.
///
/// RFC 3501 Section 9: quoted = DQUOTE *QUOTED-CHAR DQUOTE;
/// QUOTED-CHAR excludes CR and LF.
#[test]
fn unknown_with_truncated_quoted_backslash_no_panic() {
    // The backslash at the end of the quoted string would `pos += 2`, skipping
    // the `\r` of the CRLF terminator. Without the fix, `parse_response` fails
    // because `crlf` cannot find `\r\n`  -  only `\n` remains.
    let input = b"* XFOO \"trail\\\r\n";
    let result = parse_response(input);
    match result {
        Ok((rem, Response::Untagged(boxed))) => {
            assert!(
                rem.is_empty(),
                "remaining bytes should be empty, got {:?}",
                String::from_utf8_lossy(rem)
            );
            match *boxed {
                UntaggedResponse::Unknown(ref text) => {
                    assert!(
                        text.starts_with("XFOO"),
                        "Unknown text should start with XFOO, got: {text}"
                    );
                }
                other => panic!("Expected Unknown, got {other:?}"),
            }
        }
        Ok((_, other)) => panic!("Expected Untagged(Unknown), got {other:?}"),
        Err(e) => panic!(
            "BUG: parse_response failed on truncated quoted escape: {e:?}  -  \
                 the backslash swallowed the CRLF terminator"
        ),
    }

    // Also verify: backslash as the very last byte of the input (no CRLF at all).
    // The scanner must not panic; it should return an error (no CRLF found).
    let direct_input = b"XFOO \"trail\\";
    let result2 = scan_unknown_response(direct_input);
    assert!(
        result2.is_err(),
        "scan_unknown_response on truncated input with trailing backslash \
             should return Err (no CRLF), got {result2:?}"
    );
}

/// RFC 7888: LITERAL+ `{N+}\r\n<data>` in unknown response.
#[test]
fn unknown_with_literal_plus() {
    let input = b"* XQUX {3+}\r\nabc\r\n";
    let (rem, resp) = parse_response(input).expect("should parse unknown with LITERAL+");
    assert!(
        rem.is_empty(),
        "remaining bytes should be empty, got {:?}",
        String::from_utf8_lossy(rem)
    );
    match resp {
        Response::Untagged(boxed) => match *boxed {
            UntaggedResponse::Unknown(text) => {
                assert!(text.starts_with("XQUX"));
            }
            other => panic!("Expected Unknown, got {other:?}"),
        },
        other => panic!("Expected Untagged, got {other:?}"),
    }
}

/// RFC 3501 Section 7.4.2: BODY[HEADER.FIELDS (Subject)] with literal data
/// should parse correctly (baseline  -  `]` is outside the header-list parens).
#[test]
fn body_section_header_fields_literal() {
    // {15}\r\n = 15-byte literal: "Subject: Hi\r\n\r\n" (11 + 2 + 2)
    let input = b"* 1 FETCH (BODY[HEADER.FIELDS (Subject)] {15}\r\nSubject: Hi\r\n\r\n)\r\n";
    let (_, resp) = parse_response(input).expect("should parse HEADER.FIELDS with literal");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(fr) => {
                assert_eq!(fr.body_sections.len(), 1);
                assert_eq!(fr.body_sections[0].section, "HEADER.FIELDS (Subject)");
                assert_eq!(
                    fr.body_sections[0].data.as_deref(),
                    Some(b"Subject: Hi\r\n\r\n".as_ref())
                );
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RFC 3501 Section 7.4.2: header-fld-name = astring, and
/// ASTRING-CHAR includes resp-specials = "]" (RFC 3501 Section 9).
/// A quoted header field name containing `]` must not cause the section
/// parser to stop prematurely.
#[test]
fn body_section_quoted_bracket_in_header_field_name() {
    // The `]` inside the quoted string `"X]Field"` is not the section closer.
    let input = b"(BODY[HEADER.FIELDS (\"X]Field\")] \"data\")";
    let (_, fr) = fetch_response_inner(input, false)
        .expect("should parse HEADER.FIELDS with quoted ] in field name");
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "HEADER.FIELDS (\"X]Field\")");
    assert_eq!(fr.body_sections[0].data.as_deref(), Some(b"data".as_ref()));
}

/// RFC 3501 Section 7.4.2: BODY[1.MIME] should parse correctly (baseline).
#[test]
fn body_section_mime() {
    let input = b"(BODY[1.MIME] \"Content-Type: text/plain\")";
    let (_, fr) = fetch_response_inner(input, false).unwrap();
    assert_eq!(fr.body_sections.len(), 1);
    assert_eq!(fr.body_sections[0].section, "1.MIME");
    assert_eq!(
        fr.body_sections[0].data.as_deref(),
        Some(b"Content-Type: text/plain".as_ref())
    );
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

/// RFC 3501 Section 9: FETCH FLAGS uses the `flag-fetch` production
/// which includes `\Recent` (unlike `flag-list`). The FETCH FLAGS
/// response MUST preserve `\Recent`.
#[test]
fn spec_audit_fetch_flags_includes_recent() {
    let input = b"* 1 FETCH (FLAGS (\\Seen \\Recent))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fetch) = *boxed
    {
        let flags = fetch.flags.as_ref().expect("FLAGS should be present");
        assert!(
            flags.contains(&Flag::Recent),
            "RFC 3501 Section 9: flag-fetch includes \\Recent, \
                 but parser excluded it: {flags:?}"
        );
        assert!(flags.contains(&Flag::Seen));
        return;
    }
    panic!("expected FETCH response");
}

// ===== integer overflow in literal count arithmetic (RFC 3501 Section 9) =====

/// RFC 3501 Section 9: literal count near `usize::MAX` must not wrap around
/// in `try_skip_literal`. The function must return `None` instead.
#[test]
fn try_skip_literal_overflow_returns_none() {
    // Construct a literal header with count = usize::MAX.
    // On 64-bit: {18446744073709551615}\r\n
    let count_str = format!("{{{}}}\r\n", usize::MAX);
    let input = count_str.as_bytes();
    // pos after parsing header will be small, so pos + usize::MAX would wrap.
    // The function must return None (not enough data / overflow).
    assert_eq!(
        try_skip_literal(input),
        None,
        "try_skip_literal must return None when literal count overflows usize"
    );
}

/// RFC 3501 Section 7.4.2: `skip_balanced_parens` must return an error
/// when input is exhausted with unbalanced parentheses (depth > 0),
/// rather than silently returning Ok  -  which would consume bytes from
/// subsequent responses and corrupt downstream parsing.
#[test]
fn skip_balanced_parens_unbalanced_depth_returns_error() {
    // Open two parens, close only one  -  then input ends.
    let input = b"(\"nested\" (\"inner\")";
    let result = skip_balanced_parens(input);
    assert!(
        result.is_err(),
        "skip_balanced_parens must return Err when input is exhausted with \
             depth > 0 (unbalanced parens), got Ok with rest = {:?}",
        result
            .ok()
            .map(|(r, ())| String::from_utf8_lossy(r).to_string())
    );
}

/// RFC 3501 Section 7.4.2: `skip_balanced_parens` must still return Ok
/// when input is exhausted at depth == 0 (no parens at all).
#[test]
fn skip_balanced_parens_empty_at_zero_depth_returns_ok() {
    let input = b"";
    let result = skip_balanced_parens(input);
    assert!(
        result.is_ok(),
        "skip_balanced_parens must return Ok when input is empty at depth 0"
    );
}

/// RFC 3501 Section 9: literal count near `usize::MAX` must not wrap around
/// in `skip_balanced_parens`. The function must skip the malformed literal
/// rather than advancing to a wrapped-around position.
#[test]
fn skip_balanced_parens_literal_overflow_no_wrap() {
    // Build: {<usize::MAX>}\r\n)
    // The literal count is huge; the function must not wrap and must
    // return early to avoid misinterpreting literal body bytes as structure.
    let literal = format!("{{{}}}\r\n", usize::MAX);
    let mut input = literal.into_bytes();
    input.push(b')'); // closing paren after the literal
    let result = skip_balanced_parens(&input);
    // The function should succeed  -  returning early at the overflow point
    // rather than consuming body bytes as parenthesized structure.
    assert!(
        result.is_ok(),
        "skip_balanced_parens must handle overflow gracefully"
    );
    let (rest, ()) = result.unwrap();
    // With the early-return fix, rest includes the literal digits/marker
    // and the trailing ')'. The key invariant is that the function did not
    // wrap around or panic.
    assert!(
        rest.ends_with(b")"),
        "skip_balanced_parens must not consume past overflow point; rest = {:?}",
        String::from_utf8_lossy(rest)
    );
}

/// RFC 3501 Section 9: literal count near `usize::MAX` must not wrap around
/// in `skip_paren_group`. The function must skip the malformed literal
/// rather than advancing to a wrapped-around position.
#[test]
fn skip_paren_group_literal_overflow_no_wrap() {
    // Build: ({<usize::MAX>}\r\n)
    let literal = format!("({{{}}}\r\n)", usize::MAX);
    let input = literal.as_bytes();
    let result = skip_paren_group(input);
    // The function should return Err  -  the literal count overflows, so it
    // cannot find the matching ')' without misinterpreting body bytes.
    assert!(
        result.is_err(),
        "skip_paren_group must return Err on literal overflow \
             (RFC 3501 Section 9 / RFC 9051 Section 9)"
    );
}

#[test]
fn scan_section_spec_literal_overflow_does_not_panic() {
    // scan_section_spec did not bounds-check after advancing
    // `pos` by the literal byte count. A literal count exceeding the
    // remaining input (e.g. {999}) must not cause an out-of-bounds access
    // or integer overflow  -  the parser should return an error or fall
    // through gracefully (RFC 3501 Section 7.4.2, Section 9).

    // Case 1: Literal count (999) exceeds remaining input. Before the fix
    // this would read past the buffer; now the literal skip is silently
    // ignored and the parser continues without panic.
    let input = b"* 1 FETCH (BODY[HEADER.FIELDS ({999}\r\nXX)] \"test\")\r\n";
    let result = parse_response(input);
    // Any non-panicking result is acceptable  -  the key invariant is that
    // the parser does not access out-of-bounds memory.
    assert!(
        result.is_ok() || result.is_err(),
        "scan_section_spec must not panic on oversized literal count"
    );

    // Case 2: Literal count that would overflow usize when added to the
    // current position (RFC 3501 Section 4.3).
    let input2 = format!(
        "* 1 FETCH (BODY[HEADER.FIELDS ({{{}}}\r\nAB)] \"x\")\r\n",
        usize::MAX
    );
    let result2 = parse_response(input2.as_bytes());
    assert!(
        result2.is_ok() || result2.is_err(),
        "scan_section_spec must not panic on usize::MAX literal count"
    );
}

/// RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
/// Inside `scan_section_spec`, a backslash before CR/LF in a quoted string
/// must NOT cause the scanner to skip past the CRLF response terminator  -
/// same bug class as `skip_paren_group` (fc4afd9).
#[test]
fn scan_section_spec_backslash_crlf_in_quoted_string() {
    // Input to scan_section_spec: the bytes after `[` up to the unquoted `]`.
    // Here the quoted string contains \<CR><LF> which is malformed.
    // The scanner must NOT skip the CR via the backslash escape.
    let input = b"HEADER.FIELDS (\"val\\\r\nmore\")]rest";
    let result = scan_section_spec(input);
    if let Ok((rest, section)) = result {
        // If it parses, the section content must NOT include bytes past
        // the CRLF  -  the scanner must break out of the quoted string
        // at the CR/LF rather than consuming past it.
        let section_str = String::from_utf8_lossy(section);
        assert!(
            !section_str.contains("more"),
            "scan_section_spec consumed past CRLF: section = {section_str:?}",
        );
        // The `]` and `rest` must remain unconsumed or be just past `]`.
        assert!(
            rest == b"rest" || rest.starts_with(b"]") || rest.contains(&b'\n'),
            "unexpected rest after scan_section_spec: {:?}",
            String::from_utf8_lossy(rest)
        );
    }
    // Err is also acceptable  -  malformed input.
}

/// a non-conformant `(MODSEQ 0)` in SEARCH must NOT drop
/// the search results. RFC 7162 Section 3.1.6 says mod-sequence-value
/// must be >= 1, but the parser must preserve UIDs and discard the
/// malformed MODSEQ rather than losing the entire response.
#[test]
fn regression_search_modseq_zero_preserves_uids() {
    let input = b"* SEARCH 1 5 10 (MODSEQ 0)\r\n";
    let (_, resp) = parse_response(input)
        .expect("SEARCH with malformed MODSEQ must still parse  -  UIDs must not be silently lost");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = &*u {
            assert_eq!(
                *uids,
                vec![1, 5, 10],
                "SEARCH UIDs must be preserved even when MODSEQ suffix is malformed"
            );
            assert_eq!(
                *mod_seq, None,
                "malformed MODSEQ should be discarded (None), not Some(0)"
            );
        } else {
            panic!(
                "expected Search response, got {u:?}  -  malformed MODSEQ must not \
                     cause fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// FETCH MODSEQ (0) must NOT drop all other attributes.
/// RFC 7162 Section 3.1.3: mod-sequence-value must be >= 1.
/// A non-conformant server sending MODSEQ (0) must NOT cause UID,
/// FLAGS, and other attributes to be silently lost.
#[test]
fn regression_fetch_modseq_zero_preserves_other_attrs() {
    // RFC 7162 Section 3.1.3 / Postel's law: MODSEQ (0) is accepted and
    // preserved as Some(0) for consistency with STATUS HIGHESTMODSEQ.
    let input = b"* 1 FETCH (UID 42 FLAGS (\\Seen) MODSEQ (0) RFC822.SIZE 1024)\r\n";
    let (_, resp) = parse_response(input)
        .expect("FETCH with MODSEQ (0) must parse  -  0 accepted per Postel's law");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(fr.seq, 1, "sequence number must be preserved");
            assert_eq!(fr.uid, Some(42), "UID must be preserved");
            assert!(
                fr.flags
                    .as_ref()
                    .is_some_and(|f| f.contains(&crate::types::Flag::Seen)),
                "FLAGS must be preserved"
            );
            assert_eq!(fr.rfc822_size, Some(1024), "RFC822.SIZE must be preserved");
            assert_eq!(
                fr.mod_seq,
                Some(0),
                "MODSEQ (0) should be preserved as Some(0) per Postel's law"
            );
        } else {
            panic!("expected Fetch, got {u:?}  -  MODSEQ (0) must not drop the response");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// a non-conformant `(MODSEQ 0)` in SORT must NOT drop
/// the sort results. Same root cause as the SEARCH case.
#[test]
fn regression_sort_modseq_zero_preserves_nums() {
    let input = b"* SORT 3 1 2 (MODSEQ 0)\r\n";
    let (_, resp) = parse_response(input).expect(
        "SORT with malformed MODSEQ must still parse  -  numbers must not be silently lost",
    );
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(
                *nums,
                vec![3, 1, 2],
                "SORT numbers must be preserved even when MODSEQ suffix is malformed"
            );
            assert_eq!(
                *mod_seq, None,
                "malformed MODSEQ should be discarded (None), not Some(0)"
            );
        } else {
            panic!(
                "expected Sort response, got {u:?}  -  malformed MODSEQ must not \
                     cause fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// MODSEQ value exceeding 2^63-1 must NOT drop SEARCH results.
#[test]
fn regression_search_modseq_overflow_preserves_uids() {
    // 2^63 = 9223372036854775808, exceeds i64::MAX
    let input = b"* SEARCH 42 (MODSEQ 9223372036854775808)\r\n";
    let (_, resp) = parse_response(input).expect("SEARCH with overflowing MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = &*u {
            assert_eq!(*uids, vec![42]);
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ESEARCH with malformed MODSEQ 0 must preserve other results.
/// RFC 7162 Section 3.1.10: mod-sequence-value must be >= 1.
/// A non-conformant server sending MODSEQ 0 must NOT cause the entire
/// ESEARCH response (MIN, MAX, COUNT, ALL) to be silently lost.
#[test]
fn regression_esearch_modseq_zero_preserves_results() {
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 5 ALL 1:3,7,9 MODSEQ 0\r\n";
    let (_, resp) = parse_response(input).expect("ESEARCH with malformed MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.count, Some(5), "COUNT must be preserved");
            assert_eq!(
                esearch.all,
                vec![
                    UidRange::range(1, 3),
                    UidRange::single(7),
                    UidRange::single(9)
                ],
                "ALL must be preserved"
            );
            assert!(esearch.uid, "UID indicator must be preserved");
            assert_eq!(esearch.tag.as_deref(), Some("A1"), "TAG must be preserved");
        } else {
            panic!(
                "expected Esearch, got {u:?}  -  malformed MODSEQ must not cause \
                     fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ESEARCH with MODSEQ overflow must preserve other results.
#[test]
fn regression_esearch_modseq_overflow_preserves_results() {
    // 2^63 exceeds i64::MAX
    let input = b"* ESEARCH (TAG \"A2\") UID MIN 1 MAX 100 MODSEQ 9223372036854775808\r\n";
    let (_, resp) =
        parse_response(input).expect("ESEARCH with overflowing MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min, Some(1), "MIN must be preserved");
            assert_eq!(esearch.max, Some(100), "MAX must be preserved");
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// ESEARCH with MODSEQ missing its required space must preserve other results.
/// RFC 7162 Section 3.1.10: grammar is "MODSEQ SP mod-sequence-value", but
/// a non-conformant server might omit the space (e.g., MODSEQ immediately
/// followed by CRLF). The parser must not propagate the `sp()` error and
/// silently lose all previously parsed ESEARCH data
/// (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn regression_esearch_modseq_missing_space_preserves_results() {
    // MODSEQ at end of line with no space or value  -  sp(rest) will fail.
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 5 MODSEQ\r\n";
    let (_, resp) = parse_response(input).expect("ESEARCH with space-less MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.count, Some(5), "COUNT must be preserved");
            assert!(esearch.uid, "UID indicator must be preserved");
            assert_eq!(esearch.tag.as_deref(), Some("A1"), "TAG must be preserved");
            assert_eq!(
                esearch.mod_seq, None,
                "MODSEQ should be None when space is missing"
            );
        } else {
            panic!(
                "expected Esearch, got {u:?}  -  malformed MODSEQ must not cause \
                     fallthrough to Unknown"
            );
        }
    } else {
        panic!("expected Untagged");
    }
}

/// FETCH MODSEQ with overflow value must preserve other attrs.
#[test]
fn regression_fetch_modseq_overflow_preserves_other_attrs() {
    // 2^63 exceeds i64::MAX
    let input = b"* 5 FETCH (UID 100 MODSEQ (9223372036854775808) FLAGS (\\Flagged))\r\n";
    let (_, resp) = parse_response(input).expect("FETCH with overflowing MODSEQ must still parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(fr.uid, Some(100), "UID must be preserved");
            assert!(
                fr.flags
                    .as_ref()
                    .is_some_and(|f| f.contains(&crate::types::Flag::Flagged)),
                "FLAGS must be preserved"
            );
            assert_eq!(fr.mod_seq, None, "overflowing MODSEQ should be None");
        } else {
            panic!("expected Fetch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 7162 Section 3.1.3 / Postel's law: a server sending `MODSEQ (0)`
/// in a FETCH response must be accepted and yield `Some(0)` rather than
/// silently discarding the value into `None`. This matches the STATUS
/// HIGHESTMODSEQ parser which already accepts 0.
#[test]
fn fetch_modseq_zero() {
    let input = b"* 1 FETCH (MODSEQ (0))\r\n";
    let (_, resp) = parse_response(input).expect("FETCH with MODSEQ (0) must parse");
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = &*u {
            assert_eq!(
                fr.mod_seq,
                Some(0),
                "MODSEQ (0) should be preserved as Some(0), not silently discarded"
            );
        } else {
            panic!("expected Fetch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 3501 Section 7.4.2 / Postel's law: trailing whitespace before the
/// closing `)` in a BODYSTRUCTURE must not cause a parse failure. Some
/// servers emit extra whitespace between the last field and the closing
/// paren.
#[test]
fn bodystructure_text_trailing_space_before_close_paren() {
    // Note the space before the final `)` of the BODYSTRUCTURE.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5 ))\r\n";
    let (_, resp) = parse_response(input)
        .expect("BODYSTRUCTURE with trailing space before ) should parse (Postel's law)");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(ref fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Text {
                        media_subtype,
                        lines,
                        size,
                        ..
                    } => {
                        assert_eq!(media_subtype, "plain");
                        assert_eq!(*size, 100);
                        assert_eq!(*lines, 5);
                    }
                    other => panic!("expected Text, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Trailing whitespace after the last extension field (location) before
/// the closing `)` of a single-part BODYSTRUCTURE.
#[test]
fn bodystructure_basic_ext_data_trailing_space() {
    // Basic type with md5=NIL, disposition=NIL, language=NIL, location=NIL, then trailing space before ')'.
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 5000 NIL NIL NIL NIL ))\r\n";
    let (_, resp) = parse_response(input)
        .expect("BODYSTRUCTURE with trailing space after extension data should parse");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(ref fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                assert!(matches!(bs, BodyStructure::Basic { .. }));
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Trailing whitespace before `)` in a multipart BODYSTRUCTURE's extension data.
#[test]
fn bodystructure_multipart_trailing_space() {
    // Multipart with params=NIL and trailing space before ')'.
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 100 5) \"ALTERNATIVE\" NIL ))\r\n";
    let (_, resp) =
        parse_response(input).expect("multipart BODYSTRUCTURE with trailing space should parse");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Fetch(ref fr) => {
                let bs = fr
                    .body_structure
                    .as_ref()
                    .expect("should have body_structure");
                match bs {
                    BodyStructure::Multipart {
                        media_subtype,
                        bodies,
                        ..
                    } => {
                        assert_eq!(media_subtype, "alternative");
                        assert_eq!(bodies.len(), 2);
                    }
                    other => panic!("expected Multipart, got {other:?}"),
                }
            }
            other => panic!("expected Fetch, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Single-part body with all 4 extension fields present: MD5, disposition,
/// language, and location (RFC 3501 Section 7.4.2).
#[test]
fn bodystructure_1part_all_four_ext_fields() {
    // text/plain with md5="abc123", disposition=inline, language=("en"), location="http://example.com"
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5 \"abc123\" (\"inline\" NIL) \"en\" \"http://example.com\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text {
        md5,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert_eq!(md5.as_deref(), Some("abc123"));
        let disp = disposition.expect("disposition should be present");
        assert_eq!(disp.disposition_type, "inline");
        let langs = language.expect("language should be present");
        assert_eq!(langs, vec!["en"]);
        assert_eq!(location.as_deref(), Some("http://example.com"));
    } else {
        panic!("expected Text body");
    }
}

/// Multipart body with all 4 extension fields present: params, disposition,
/// language, and location (RFC 3501 Section 7.4.2).
#[test]
fn bodystructure_mpart_all_four_ext_fields() {
    // multipart/mixed with params=(BOUNDARY "----=_Part"), disposition=inline,
    // language=("en" "fr"), location="http://example.com"
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 80 4) \"MIXED\" (\"BOUNDARY\" \"----=_Part\") (\"inline\" NIL) (\"en\" \"fr\") \"http://example.com\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        params,
        disposition,
        language,
        location,
        ..
    } = bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "boundary");
        let disp = disposition.expect("disposition should be present");
        assert_eq!(disp.disposition_type, "inline");
        let langs = language.expect("language should be present");
        assert_eq!(langs, vec!["en", "fr"]);
        assert_eq!(location.as_deref(), Some("http://example.com"));
    } else {
        panic!("expected Multipart body");
    }
}

/// RFC 3501 Section 9: EXPUNGE uses `nz-number = digit-nz *DIGIT`
/// which means the first digit must be 1-9. Leading zeros like `01`
/// are NOT valid nz-numbers.
#[test]
fn expunge_rejects_leading_zero() {
    // "* 01 EXPUNGE\r\n"  -  `01` is not a valid nz-number.
    let input = b"* 01 EXPUNGE\r\n";
    let result = parse_response(input);
    assert!(
        result.is_err(),
        "EXPUNGE with leading-zero sequence number '01' should be rejected \
             (RFC 3501 Section 9: nz-number = digit-nz *DIGIT)"
    );
}

/// RFC 3501 Section 9: FETCH uses `nz-number`  -  leading zeros are rejected.
#[test]
fn fetch_rejects_leading_zero() {
    // "* 01 FETCH (UID 5)\r\n"  -  `01` is not a valid nz-number.
    let input = b"* 01 FETCH (UID 5)\r\n";
    let result = parse_response(input);
    assert!(
        result.is_err(),
        "FETCH with leading-zero sequence number '01' should be rejected \
             (RFC 3501 Section 9: nz-number = digit-nz *DIGIT)"
    );
}

/// EXISTS uses `number` (not `nz-number`), so leading zeros ARE valid.
/// `* 01 EXISTS` should parse as EXISTS with count 1.
#[test]
fn exists_accepts_leading_zero() {
    let input = b"* 01 EXISTS\r\n";
    let (_, resp) = parse_response(input)
        .expect("EXISTS with leading-zero number should parse (number allows leading zeros)");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Exists(n) => assert_eq!(n, 1),
            other => panic!("expected Exists, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// RECENT uses `number` (not `nz-number`), so leading zeros ARE valid.
#[test]
fn recent_accepts_leading_zero() {
    let input = b"* 01 RECENT\r\n";
    let (_, resp) = parse_response(input).expect("RECENT with leading-zero number should parse");
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Recent(n) => assert_eq!(n, 1),
            other => panic!("expected Recent, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

/// Deeply nested multipart BODYSTRUCTURE beyond `MAX_BODY_NESTING_DEPTH` must
/// be rejected to prevent stack overflow (defense-in-depth).
///
/// RFC 3501 Section 7.4.2 does not specify a maximum nesting depth, but
/// unbounded recursion is a denial-of-service vector.
#[test]
fn bodystructure_rejects_excessive_nesting_depth() {
    // Build a BODYSTRUCTURE with 256 levels of multipart nesting  -  well
    // beyond MAX_BODY_NESTING_DEPTH (64).
    let depth = 256_usize;
    let mut input = Vec::new();
    input.extend_from_slice(b"* 1 FETCH (BODYSTRUCTURE ");
    // Open `depth` levels of multipart: each level is `(` + inner + ` "MIXED")`.
    // The innermost part is a simple text/plain.
    for _ in 0..depth {
        input.push(b'(');
    }
    // Innermost leaf: text/plain body
    input.extend_from_slice(
        b"(\"text\" \"plain\" (\"charset\" \"utf-8\") NIL NIL \"7bit\" 10 1 NIL NIL NIL NIL)",
    );
    // Close each multipart level: SP "MIXED" )
    for _ in 0..depth {
        input.extend_from_slice(b" \"MIXED\")");
    }
    input.extend_from_slice(b")\r\n");

    let result = parse_response(&input);
    assert!(
        result.is_err(),
        "parsing a BODYSTRUCTURE nested {depth} levels deep should fail, \
             but it succeeded"
    );
}

/// RFC 2047 encoded words in BODYSTRUCTURE description must be decoded.
///
/// RFC 2045 Section 8 defines Content-Description as `*text`.
/// RFC 2047 Section 5 rule (1): encoded-words may replace `text` tokens in any
/// MIME body part field whose body is defined as `*text`.
#[test]
fn bodystructure_description_rfc2047_decoded() {
    // "Hello" encoded as RFC 2047 Base64 in UTF-8
    let input =
        b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL \"=?UTF-8?B?SGVsbG8=?=\" \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("Hello"),
            "RFC 2047 encoded words in Content-Description must be decoded"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }

    // Also test Basic body type (e.g., image/png)
    let input = b"(\"IMAGE\" \"PNG\" NIL NIL \"=?UTF-8?Q?=C3=A9t=C3=A9?=\" \"BASE64\" 2048)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Basic { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("\u{e9}t\u{e9}"),
            "RFC 2047 encoded description in Basic part must decode to 'été'"
        );
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

/// RFC 6855 Section 3.1 only applies UTF-8 direct encoding to FETCH ENVELOPE
/// fields (subject, address display names), NOT to BODYSTRUCTURE description.
/// Content-Description is a MIME header (RFC 2045 Section 8) that uses RFC 2047
/// encoded-words regardless of UTF8=ACCEPT mode. `decode_rfc2047` already
/// handles plain UTF-8 input correctly, so it must always be used.
#[test]
fn spec_audit_bodystructure_description_rfc2047_utf8_mode() {
    // RFC 2047 encoded "Hello"  -  must be decoded even in UTF8=ACCEPT mode
    let input =
        b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL \"=?UTF-8?B?SGVsbG8=?=\" \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, true, 0).unwrap();
    if let BodyStructure::Text { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("Hello"),
            "RFC 2047 encoded Content-Description must be decoded even in UTF8=ACCEPT mode \
                 (RFC 6855 Section 3.1 only affects ENVELOPE, not BODYSTRUCTURE description \
                 per RFC 2045 Section 8)"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }

    // Also test Basic body type with RFC 2047 Q-encoding
    let input = b"(\"IMAGE\" \"PNG\" NIL NIL \"=?UTF-8?Q?=C3=A9t=C3=A9?=\" \"BASE64\" 2048)";
    let (_, bs) = body_structure(input, true, 0).unwrap();
    if let BodyStructure::Basic { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("\u{e9}t\u{e9}"),
            "RFC 2047 Q-encoded Content-Description must decode in UTF8=ACCEPT mode"
        );
    } else {
        panic!("expected Basic, got {bs:?}");
    }

    // Plain UTF-8 (no RFC 2047 wrapping) must also work  -  decode_rfc2047
    // passes through raw UTF-8 via String::from_utf8_lossy.
    let input =
        b"(\"TEXT\" \"HTML\" (\"CHARSET\" \"UTF-8\") NIL \"\xc3\xa9t\xc3\xa9\" \"7BIT\" 50 2)";
    let (_, bs) = body_structure(input, true, 0).unwrap();
    if let BodyStructure::Text { description, .. } = &bs {
        assert_eq!(
            description.as_deref(),
            Some("\u{e9}t\u{e9}"),
            "Plain UTF-8 Content-Description must be preserved in UTF8=ACCEPT mode"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

// ===== Postel's law: accept empty "()" address list from non-conformant servers =====
// RFC 3501 Section 9: env-from = "(" 1*address ")" / nil  -  strictly requires
// at least one address. However, non-conformant servers (e.g., older Exchange,
// some webmail backends) may send "()" instead of NIL for empty address lists.
// Per Postel's law (RFC 1122 Section 1.2.2), accept gracefully.

#[test]
fn spec_audit_address_list_empty_parens() {
    // "()" should parse as an empty address list per Postel's law (RFC 1122 Section1.2.2),
    // even though RFC 3501 Section9 formally requires 1*address in the parenthesized form.
    let (rest, addrs) = address_list(b"()", false).unwrap();
    assert!(rest.is_empty());
    assert!(
        addrs.is_empty(),
        "empty parens should yield empty Vec<EnvelopeAddress>"
    );
}

#[test]
fn spec_audit_envelope_empty_paren_address_field() {
    // FETCH ENVELOPE where the `to` field is "()" instead of NIL.
    // Non-conformant servers (e.g., older Exchange) may send this.
    // The entire FETCH response must still parse successfully.
    let input = b"* 1 FETCH (ENVELOPE (\"Mon, 1 Jan 2024 00:00:00 +0000\" \"Subject\" ((NIL NIL \"user\" \"example.com\")) ((NIL NIL \"user\" \"example.com\")) ((NIL NIL \"user\" \"example.com\")) () NIL NIL NIL \"<msg@example.com>\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let env = f.envelope.as_ref().unwrap();
            assert_eq!(env.from.len(), 1, "from should have one address");
            assert!(
                env.to.is_empty(),
                "to should be empty (parsed from '()'), got {:?}",
                env.to
            );
            assert_eq!(
                env.message_id.as_deref(),
                Some("<msg@example.com>"),
                "message-id should be parsed correctly"
            );
        } else {
            panic!("expected Fetch");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== ESEARCH/STATUS unknown-value skip must handle quoted strings and literals =====
// RFC 9051 Section9: tagged-ext-val = tagged-ext-simple / "(" [tagged-ext-comp] ")"
// tagged-ext-simple = sequence-set / number / number64 / astring
// astring = 1*ASTRING-CHAR / string
// string = quoted / literal
// The previous skip logic used take_while1 which only handles atom-like
// values, breaking on spaces (in quoted strings) or \r\n (in literals).

#[test]
fn regression_esearch_unknown_key_quoted_string_value() {
    // Unknown ESEARCH key with a quoted string value containing spaces.
    // The quoted string's internal spaces must not prematurely terminate
    // the skip, corrupting parsing of subsequent known keys.
    let input = b"* ESEARCH (TAG \"A001\") UID XFUTURE \"hello world\" COUNT 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(
                esearch.count,
                Some(5),
                "COUNT must be parsed after skipping unknown quoted-string value"
            );
            assert!(esearch.uid);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn regression_esearch_unknown_key_literal_value() {
    // Unknown ESEARCH key with a literal value.
    // The literal's {N}\r\n prefix must be handled correctly.
    let input = b"* ESEARCH (TAG \"A001\") UID XBLOB {5}\r\nhello COUNT 3\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(
                esearch.count,
                Some(3),
                "COUNT must be parsed after skipping unknown literal value"
            );
            assert!(esearch.uid);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn regression_status_unknown_attr_quoted_string_value() {
    // Unknown STATUS attribute with a quoted string value containing spaces.
    // The quoted string's internal spaces must not prematurely terminate
    // the skip, corrupting parsing of subsequent known attributes.
    let input = b"* STATUS \"INBOX\" (MESSAGES 10 XFOO \"some value\" UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            let messages = items.iter().find_map(|i| match i {
                StatusItem::Messages(n) => Some(*n),
                _ => None,
            });
            let unseen = items.iter().find_map(|i| match i {
                StatusItem::Unseen(n) => Some(*n),
                _ => None,
            });
            assert_eq!(
                messages,
                Some(10),
                "MESSAGES must be parsed before unknown quoted-string attr"
            );
            assert_eq!(
                unseen,
                Some(3),
                "UNSEEN must be parsed after skipping unknown quoted-string value"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn regression_status_unknown_attr_literal_value() {
    // Unknown STATUS attribute with a literal value containing spaces.
    // The literal's {N}\r\n<N bytes> must be consumed as a unit, even
    // when the payload contains spaces or parens.
    let input = b"* STATUS \"INBOX\" (MESSAGES 10 XBLOB {5}\r\na b ) UNSEEN 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            let messages = items.iter().find_map(|i| match i {
                StatusItem::Messages(n) => Some(*n),
                _ => None,
            });
            let unseen = items.iter().find_map(|i| match i {
                StatusItem::Unseen(n) => Some(*n),
                _ => None,
            });
            assert_eq!(
                messages,
                Some(10),
                "MESSAGES must be parsed before unknown literal attr"
            );
            assert_eq!(
                unseen,
                Some(3),
                "UNSEEN must be parsed after skipping unknown literal value"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== FETCH with PREVIEW (RFC 8970) =====

#[test]
fn fetch_preview_quoted() {
    // RFC 8970 Section 3: PREVIEW data item is an nstring.
    let (_, resp) =
        parse_response(b"* 1 FETCH (UID 42 PREVIEW \"Meeting tomorrow at 3pm\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(42));
        assert_eq!(fr.preview.as_deref(), Some("Meeting tomorrow at 3pm"));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_preview_nil() {
    // RFC 8970 Section 3: PREVIEW NIL (e.g., LAZY mode not yet computed).
    let (_, resp) = parse_response(b"* 5 FETCH (UID 100 PREVIEW NIL)\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(100));
        assert!(fr.preview.is_none());
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_preview_empty_string() {
    // RFC 8970 Section 3: PREVIEW with empty quoted string.
    let (_, resp) = parse_response(b"* 1 FETCH (PREVIEW \"\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.preview.as_deref(), Some(""));
        return;
    }
    panic!("expected Fetch response");
}

#[test]
fn fetch_preview_with_other_items() {
    // PREVIEW alongside UID, FLAGS, and ENVELOPE.
    let (_, resp) =
        parse_response(b"* 3 FETCH (UID 7 FLAGS (\\Seen) PREVIEW \"Hello world\")\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.uid, Some(7));
        assert_eq!(fr.preview.as_deref(), Some("Hello world"));
        assert!(fr.flags.is_some());
        return;
    }
    panic!("expected Fetch response");
}

// ===== PREVIEW capability (RFC 8970 Section 4) =====

#[test]
fn capability_preview_parsed() {
    let (_, resp) = parse_response(b"* OK [CAPABILITY IMAP4rev1 PREVIEW] Ready\r\n").unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Status {
            code: Some(ResponseCode::Capability(caps)),
            ..
        } = *boxed
    {
        assert!(
            caps.contains(&Capability::Preview),
            "PREVIEW capability must be parsed"
        );
        return;
    }
    panic!("expected OK with CAPABILITY");
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

// ===== Coverage gap tests =====
//
// Tests below target specific uncovered lines identified by coverage analysis.

// ── 1. Escaped chars in string parsing (L163, L173-175) ──

/// RFC 3501 Section 9 / RFC 9051 Section 9: `skip_balanced_parens` must handle
/// escaped characters inside quoted strings so that `\"` does not prematurely
/// end the string and throw off depth tracking.
#[test]
fn skip_balanced_parens_escaped_chars_in_quoted_string() {
    // Quoted string with escaped quote and escaped backslash inside parens.
    // The `\"` and `\\` sequences must be correctly skipped.
    let input = b"(\"a\\\"b\\\\c\") tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

/// RFC 3501 Section 9: QUOTED-CHAR excludes CR and LF.
/// A backslash before CR/LF in a quoted string inside a parenthesized
/// block must NOT cause the parser to skip past the CRLF response
/// terminator  -  same bug class as `skip_paren_group` (fc4afd9).
#[test]
fn skip_balanced_parens_backslash_crlf_in_quoted_string() {
    // ("val\<CR><LF>...)  -  the backslash must NOT skip the CR.
    let input = b"(\"val\\\r\n\")";
    let result = skip_parenthesized_block(input);
    // If it parses, the CRLF + trailing `")` must NOT have been consumed
    // as part of the quoted string escape.
    if let Ok((rest, ())) = result {
        assert!(
            !rest.is_empty(),
            "skip_balanced_parens consumed past CRLF boundary"
        );
    }
    // Err is also acceptable  -  malformed input.
}

/// RFC 6855 Section 4: `~{n}\r\n<data>` literal8 prefix in `skip_balanced_parens`.
/// The `~` prefix must be consumed so the `{` on the next iteration handles
/// the literal byte count correctly.
#[test]
fn skip_balanced_parens_literal8_prefix() {
    // ~{5}\r\nhello inside a parenthesized block
    let input = b"(~{5}\r\nhello) tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

/// RFC 6855 Section 4: literal8 `~{n}\r\n<data>` with nested parentheses
/// in the literal data must not affect depth tracking.
#[test]
fn skip_balanced_parens_literal8_with_parens_in_data() {
    // literal8 containing bytes that look like parens  -  must be skipped by count
    let input = b"(~{3}\r\n(x)) tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

// ── 2. Literal+ / literal counting edge cases (L190, L207-211) ──

/// RFC 7888 Section 4: `{n+}` non-synchronizing literal in `skip_balanced_parens`.
/// The `+` after the count must be correctly handled.
#[test]
fn skip_balanced_parens_literal_plus() {
    let input = b"({5+}\r\nhello) tail";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" tail");
}

/// RFC 3501 Section 9: literal `{n}\r\n<data>` in nested parens.
/// The skip function must handle both literal counting AND nested depth.
#[test]
fn skip_balanced_parens_literal_in_nested_parens() {
    let input = b"((\"key\" {3}\r\nval)) rest";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" rest");
}

/// Edge case: literal with invalid (non-digit) count inside `skip_balanced_parens`.
/// Per Postel's law, the parser should not panic  -  it should skip past the `{`
/// and continue scanning.
#[test]
fn skip_balanced_parens_literal_bad_count() {
    // `{abc}` is not a valid literal  -  parser skips past `{` and continues
    let input = b"({abc}) rest";
    let (rest, ()) = skip_parenthesized_block(input).unwrap();
    assert_eq!(rest, b" rest");
}

// ── 3. Number parsing error paths (L402-403, L418-419, L467-468, L490-493, L505-506, L537-540) ──

/// RFC 3501 Section 9: `number` rejects non-digit input.
#[test]
fn number_rejects_alpha_string() {
    assert!(number(b"abc").is_err());
}

/// RFC 3501 Section 9: `number` rejects u32 overflow values.
#[test]
fn number_rejects_u32_overflow() {
    // 4294967296 = u32::MAX + 1
    assert!(number(b"4294967296").is_err());
}

/// RFC 9051 Section 4: `number64` rejects values exceeding `i64::MAX` (63-bit limit).
#[test]
fn number64_rejects_above_i64_max_boundary() {
    // i64::MAX + 1 = 9223372036854775808
    assert!(number64(b"9223372036854775808").is_err());
}

/// RFC 9051 Section 4: `number64` rejects complete u64 overflow.
#[test]
fn number64_rejects_u64_overflow_parse_error() {
    // 18446744073709551616 = u64::MAX + 1
    assert!(number64(b"18446744073709551616").is_err());
}

/// RFC 3501 Section 9: `nz_number` rejects zero.
#[test]
fn nz_number_rejects_zero() {
    assert!(nz_number(b"0").is_err());
}

/// RFC 3501 Section 9: `nz_number` rejects leading zero (e.g. "01").
/// `digit-nz = %x31-39`  -  leading zero is never valid.
#[test]
fn nz_number_rejects_leading_zero() {
    assert!(nz_number(b"01").is_err());
}

/// RFC 3501 Section 9: `nz_number` accepts valid non-zero values.
#[test]
fn nz_number_accepts_valid() {
    let (_, val) = nz_number(b"42").unwrap();
    assert_eq!(val, 42);
}

/// RFC 9051 Section 9: `nz_number64` rejects zero.
#[test]
fn nz_number64_rejects_zero() {
    assert!(nz_number64(b"0").is_err());
}

/// RFC 9051 Section 9: `nz_number64` rejects leading zero.
#[test]
fn nz_number64_rejects_leading_zero() {
    assert!(nz_number64(b"01").is_err());
}

/// RFC 9051 Section 9: `nz_number64` accepts valid non-zero values.
#[test]
fn nz_number64_accepts_valid() {
    let (_, val) = nz_number64(b"12345678901234").unwrap();
    assert_eq!(val, 12345678901234);
}

/// RFC 3501 Section 9: literal `from_utf8` error path (L401-403).
/// `digit1` always returns ASCII digits, so this path is unreachable
/// in normal use, but we exercise the number path with an overflow
/// that goes through `parse::<u64>` failure (L406-408).
#[test]
fn literal_count_u64_overflow() {
    // A number so large it overflows u64::MAX
    let result = literal(b"{99999999999999999999999}\r\n");
    assert!(result.is_err());
}

/// RFC 9051 Section 9: literal8 does not define `+` suffix, but per
/// Postel's law we tolerate `~{n+}` since `+` has no semantic impact.
#[test]
fn literal8_tolerates_plus_suffix() {
    let result = literal(b"~{5+}\r\nhello");
    assert!(
        result.is_ok(),
        "literal8 ~{{n+}} must be tolerated per Postel's law"
    );
    let (rest, val) = result.unwrap();
    assert_eq!(val, b"hello");
    assert!(rest.is_empty());
}

/// RFC 6855 Section 4: literal8 `~{n}\r\n<data>` must parse correctly.
#[test]
fn literal8_basic() {
    let (rest, val) = literal(b"~{5}\r\nhello rest").unwrap();
    assert_eq!(val, b"hello");
    assert_eq!(rest, b" rest");
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
    assert_eq!(cap, Capability::Other("APPENDLIMIT=abc".into()));
}

/// RFC 7889 Section 5: only bare `APPENDLIMIT` or `APPENDLIMIT=<number>`
/// are valid capability forms. A token that merely starts with the
/// keyword must be preserved as an unknown capability, not normalized to
/// bare `APPENDLIMIT`.
#[test]
fn capability_appendlimit_prefix_without_separator_falls_to_other() {
    let (_, cap) = capability(b"APPENDLIMITXYZ").unwrap();
    assert_eq!(cap, Capability::Other("APPENDLIMITXYZ".into()));
}

/// RFC 7889 Section 5 imports RFC 3501's `number` production
/// (`1*DIGIT`), so `APPENDLIMIT=+123` is malformed and must stay an
/// unknown capability instead of being normalized into a byte limit.
#[test]
fn capability_appendlimit_leading_plus_falls_to_other() {
    let (_, cap) = capability(b"APPENDLIMIT=+123").unwrap();
    assert_eq!(cap, Capability::Other("APPENDLIMIT=+123".into()));
}

/// Unknown capability string falls through to `Capability::Other`.
#[test]
fn capability_other_unknown_string() {
    let (_, cap) = capability(b"X-CUSTOM-CAP").unwrap();
    assert_eq!(cap, Capability::Other("X-CUSTOM-CAP".into()));
}

/// Unknown capability with mixed case is preserved in `Other`.
#[test]
fn capability_other_preserves_original_case() {
    let (_, cap) = capability(b"XMixedCase").unwrap();
    assert_eq!(cap, Capability::Other("XMixedCase".into()));
}

// ── 5. Greeting without response code bracket (L1033-1034) ──

/// RFC 3501 Section 7.5: continuation response where `[` is present but
/// resp-text parsing fails. The parser should fall back to treating the
/// remainder as plain data (L1033-1034 branch).
#[test]
fn continuation_bracket_resp_text_parse_failure_fallback() {
    // `[` followed by something that doesn't parse as a valid response code
    // and doesn't have a closing `]`  -  falls back to plain data.
    let input = b"+ [INVALID!@#$%\r\n";
    let (_, cont) = parse_continuation(input).unwrap();
    // The parser should treat everything after "+ " as plain data
    assert!(cont.code.is_none());
    assert_eq!(cont.data, "[INVALID!@#$%");
}

/// Greeting with text but no response code bracket.
/// This exercises the `resp_text` path where there's no `[` prefix,
/// so no response code is parsed  -  just plain text.
#[test]
fn greeting_ok_plain_text_no_code() {
    let input = b"* OK Dovecot ready.\r\n";
    let (_, resp) = parse_greeting(input).unwrap();
    match resp {
        Response::Greeting(g) => {
            assert_eq!(g.status, GreetingStatus::Ok);
            assert!(g.code.is_none());
            assert_eq!(g.text, "Dovecot ready.");
        }
        other => panic!("expected Greeting, got {other:?}"),
    }
}

// ── 6. Body structure skip (L1290-1291) ──

/// RFC 5258 Section 6: unknown LIST-EXTENDED items with parenthesized
/// values must be silently skipped via `skip_parenthesized_block`.
#[test]
fn list_extended_unknown_item_parenthesized_value_skipped() {
    // LIST response with an unknown extended data item that has a
    // parenthesized value  -  the parser should skip it gracefully.
    let input = b"* LIST (\\HasNoChildren) \"/\" \"INBOX\" (\"XFUTURE\" (\"some\" \"data\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        assert_eq!(info.delimiter, Some('/'));
        assert!(info.attributes.contains(&MailboxAttribute::HasNoChildren));
        // The unknown extended item should be silently skipped
        assert!(info.old_name.is_none());
        assert!(info.child_info.is_empty());
        return;
    }
    panic!("expected List response");
}

/// RFC 5258 Section 6: unknown LIST-EXTENDED items with simple (non-parenthesized)
/// values are also skipped.
#[test]
fn list_extended_unknown_item_simple_value_skipped() {
    // Unknown item with a simple token value (not parenthesized)
    let input = b"* LIST () \"/\" \"INBOX\" (\"XTOKEN\" 12345)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        return;
    }
    panic!("expected List response");
}

/// RFC 5258 Section 6: multiple unknown LIST-EXTENDED items, mixing
/// parenthesized and simple values.
#[test]
fn list_extended_multiple_unknown_items_skipped() {
    let input = b"* LIST () \"/\" \"INBOX\" (\"XFOO\" (\"nested\") \"XBAR\" 42)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        return;
    }
    panic!("expected List response");
}

// ── 7. Status DELETEDSTORAGE (L1742-1745) ──

/// RFC 9208 Section 3: STATUS response containing DELETED-STORAGE item
/// reports disk space consumed by deleted messages.
#[test]
fn status_deleted_storage() {
    let (_, items) = status_items(b"DELETED-STORAGE 2048").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::DeletedStorage(2048));
}

/// RFC 9208 Section 3: DELETED-STORAGE alongside other STATUS items.
#[test]
fn status_deleted_storage_with_other_items() {
    let input = b"STATUS \"INBOX\" (MESSAGES 10 DELETED-STORAGE 512)\r\n";
    let (_, resp) = parse_untagged_status_mailbox(input, false).unwrap();
    if let UntaggedResponse::MailboxStatus { mailbox, items } = resp {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert_eq!(items.len(), 2);
        assert!(items.contains(&StatusItem::Messages(10)));
        assert!(items.contains(&StatusItem::DeletedStorage(512)));
    } else {
        panic!("expected MailboxStatus, got {resp:?}");
    }
}

/// RFC 9208 Section 3: DELETED-STORAGE with large u64 value.
#[test]
fn status_deleted_storage_large_value() {
    let (_, items) = status_items(b"DELETED-STORAGE 5000000000").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::DeletedStorage(5_000_000_000u64));
}

// ── 8. NIL optional fields (L1892) ──

/// RFC 2342 Section 6: namespace descriptor with NIL delimiter.
/// The delimiter field `None` maps to `None` in the parsed `NamespaceDescriptor`.
#[test]
fn namespace_descriptor_nil_delimiter() {
    // Namespace with NIL delimiter (flat namespace, no hierarchy)
    let input = b"NAMESPACE ((\"\" NIL)) NIL NIL\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].prefix, "");
        assert_eq!(personal[0].delimiter, None);
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// RFC 2342 Section 6: namespace descriptor with empty quoted string delimiter.
/// Empty string `""` is treated as `None` (no delimiter).
#[test]
fn namespace_descriptor_empty_string_delimiter() {
    let input = b"NAMESPACE ((\"\" \"\")) NIL NIL\r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    if let UntaggedResponse::Namespace { personal, .. } = resp {
        assert_eq!(personal.len(), 1);
        assert_eq!(personal[0].delimiter, None);
    } else {
        panic!("expected Namespace, got {resp:?}");
    }
}

/// RFC 3501 Section 7.4.2: envelope with NIL fields for in-reply-to
/// and message-id. These are `nstring` per the formal syntax and must
/// parse as `None`.
#[test]
fn envelope_nil_in_reply_to_and_message_id() {
    let input = b"(\"Mon, 1 Jan 2024 00:00:00 +0000\" \"Test\" \
            ((\"A\" NIL \"a\" \"x.com\")) \
            NIL NIL \
            ((\"B\" NIL \"b\" \"y.com\")) \
            NIL NIL \
            NIL NIL)";
    let (_, env) = envelope(input, false).unwrap();
    assert!(env.in_reply_to.is_none(), "in-reply-to NIL must be None");
    assert!(env.message_id.is_none(), "message-id NIL must be None");
    assert!(env.cc.is_empty(), "cc NIL must be empty Vec");
    assert!(env.bcc.is_empty(), "bcc NIL must be empty Vec");
}

/// RFC 3501 Section 7.4.2: envelope with all address lists as NIL.
#[test]
fn envelope_all_address_lists_nil() {
    let input = b"(NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, false).unwrap();
    assert!(env.date.is_none());
    assert!(env.subject.is_none());
    assert!(env.from.is_empty());
    assert!(env.sender.is_empty());
    assert!(env.reply_to.is_empty());
    assert!(env.to.is_empty());
    assert!(env.cc.is_empty());
    assert!(env.bcc.is_empty());
    assert!(env.in_reply_to.is_none());
    assert!(env.message_id.is_none());
}

// ── 9. Loop break on incomplete (L1550, L2009) ──

/// RFC 4731 Section 3.1: ESEARCH with trailing SP after last result data
/// Trailing whitespace before CRLF in an ESEARCH response must be
/// tolerated per Postel's law (RFC 1122 Section 1.2.2), consistent
/// with every other parser in the crate (FLAGS, CAPABILITY, LIST,
/// VANISHED, ENABLED, ID, SEARCH, SORT, QUOTA, ACL).
///
/// Regression: previously the loop broke when `rest.first() == Some(&b'\r')`
/// but did not advance `input` past the trailing space, causing `crlf()`
/// to fail on ` \r\n`. The structured ESEARCH data was silently lost.
#[test]
fn esearch_trailing_space_tolerated() {
    let input = b"ESEARCH (TAG \"A001\") UID COUNT 3 \r\n";
    let (_, resp) = parse_untagged_esearch(input)
        .expect("ESEARCH with trailing space must parse (Postel's law)");
    if let UntaggedResponse::Esearch(esearch) = resp {
        assert_eq!(esearch.tag.as_deref(), Some("A001"));
        assert!(esearch.uid);
        assert_eq!(esearch.count, Some(3));
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

/// RFC 4731 Section 3.1: ESEARCH with no result data at all  -  just
/// tag and UID, followed immediately by CRLF. The while loop at L1548
/// should not enter the body because `sp()` fails on `\r\n`.
#[test]
fn esearch_no_result_data_immediate_crlf() {
    let input = b"* ESEARCH (TAG \"A001\") UID\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert!(esearch.uid);
        assert!(esearch.all.is_empty());
        assert_eq!(esearch.min, None);
        assert_eq!(esearch.max, None);
        assert_eq!(esearch.count, None);
        return;
    }
    panic!("expected Esearch response");
}

/// RFC 2087 Section 5.2: QUOTAROOT with trailing SP after roots
/// triggers the CRLF-check `break` at L2009. The loop breaks but
/// `input` still has the trailing space, so `crlf()` fails and
/// the response falls through to Unknown. This exercises the break path.
#[test]
fn quotaroot_trailing_space_tolerated_per_postels_law() {
    // Trailing space after last root is now tolerated per Postel's law
    // (RFC 1122 Section 1.2.2). The parser strips trailing whitespace
    // before CRLF, matching the pattern used by CAPABILITY, FLAGS, etc.
    let input = b"QUOTAROOT INBOX \"\" \r\n";
    let result = parse_untagged_quotaroot(input, false);
    assert!(
        result.is_ok(),
        "trailing space after last root should be tolerated (Postel's law); \
             got: {result:?}"
    );
}

/// RFC 2087 Section 5.2: QUOTAROOT with no roots at all  -  the loop
/// breaks immediately because `sp()` fails on `\r\n`.
#[test]
fn quotaroot_no_roots_loop_break() {
    let input = b"* QUOTAROOT INBOX\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::QuotaRoot { mailbox, roots } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        assert!(roots.is_empty());
        return;
    }
    panic!("expected QuotaRoot response");
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

/// RFC 9208 Section 3: STATUS APPENDLIMIT value (distinct from capability).
#[test]
fn status_appendlimit_with_value() {
    let (_, items) = status_items(b"APPENDLIMIT 104857600").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::AppendLimit(Some(104_857_600)));
}

/// RFC 9208 Section 3: STATUS APPENDLIMIT NIL (no limit).
#[test]
fn status_appendlimit_nil() {
    let (_, items) = status_items(b"APPENDLIMIT NIL").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], StatusItem::AppendLimit(None));
}

/// A STATUS attribute value that overflows its numeric type should not
/// discard the entire response. The overflowing attribute is silently
/// skipped and all other attributes are preserved
/// (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn status_messages_overflow_preserves_other_items() {
    // MESSAGES value exceeds u32::MAX (4294967296 = 2^32).
    let input = b"* STATUS INBOX (UIDVALIDITY 1 MESSAGES 4294967296)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            // UIDVALIDITY 1 must be preserved.
            assert!(
                items
                    .iter()
                    .any(|i| matches!(i, StatusItem::UidValidity(1))),
                "UIDVALIDITY 1 missing: {items:?}"
            );
            // MESSAGES was unparseable  -  must NOT appear.
            assert!(
                !items.iter().any(|i| matches!(i, StatusItem::Messages(_))),
                "overflowed MESSAGES should have been skipped: {items:?}"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged Status response, not Unknown");
    }
}

/// Regression: HIGHESTMODSEQ value exceeding `i64::MAX` should not discard the
/// entire STATUS response (Postel's law  -  RFC 1122 Section 1.2.2).
#[test]
fn status_highestmodseq_overflow_preserves_other_items() {
    // HIGHESTMODSEQ exceeds i64::MAX (9223372036854775808 = 2^63).
    let input = b"* STATUS INBOX (MESSAGES 10 HIGHESTMODSEQ 9223372036854775808)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = &*u {
            assert_eq!(mailbox.as_str(), "INBOX");
            // MESSAGES 10 must be preserved.
            assert!(
                items.iter().any(|i| matches!(i, StatusItem::Messages(10))),
                "MESSAGES 10 missing: {items:?}"
            );
            // HIGHESTMODSEQ was unparseable  -  must NOT appear.
            assert!(
                !items
                    .iter()
                    .any(|i| matches!(i, StatusItem::HighestModSeq(_))),
                "overflowed HIGHESTMODSEQ should have been skipped: {items:?}"
            );
        } else {
            panic!("expected MailboxStatus, got {u:?}");
        }
    } else {
        panic!("expected Untagged Status response, not Unknown");
    }
}

/// Regression: non-conformant server sends 0 in SEARCH results.
/// RFC 3501 Section 7.2.5 ABNF says `nz-number`, but some servers
/// emit 0. Per Postel's law (RFC 1122 Section 1.2.2), the parser
/// must accept 0 rather than silently dropping it and all subsequent
/// results.
#[test]
fn search_zero_in_results_filtered() {
    // 0 is accepted during parsing (Postel's law) to avoid losing
    // subsequent valid results, but filtered from the final output
    // since UID/sequence-number 0 is invalid (RFC 3501 Section 9).
    let input = b"* SEARCH 1 0 3\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { uids, mod_seq } = &*u {
            assert_eq!(
                *uids,
                vec![1, 3],
                "0 must be filtered; valid UIDs (1, 3) preserved"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Regression: non-conformant server sends 0 in SORT results.
/// 0 is accepted during parsing but filtered from the final output.
#[test]
fn sort_zero_in_results_filtered() {
    let input = b"* SORT 2 0 7\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(
                *nums,
                vec![2, 7],
                "0 must be filtered; valid UIDs (2, 7) preserved"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ===== Edge-case bug-hunting tests =====

/// RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2: ENVELOPE with
/// NIL subject AND NIL from  -  common on drafts. The parser must not
/// panic or error; all fields should be None/empty.
#[test]
fn edge_envelope_all_nil_fields() {
    let input = b"* 1 FETCH (ENVELOPE (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let env = f.envelope.as_ref().expect("envelope should be present");
            assert!(
                env.subject.is_none(),
                "NIL subject must be None, got {:?}",
                env.subject
            );
            assert!(
                env.from.is_empty(),
                "NIL from must be empty, got {:?}",
                env.from
            );
            assert!(env.to.is_empty(), "NIL to must be empty, got {:?}", env.to);
            assert!(
                env.message_id.is_none(),
                "NIL message-id must be None, got {:?}",
                env.message_id
            );
        } else {
            panic!("expected Fetch response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 3501 Section 7.4.2: BODYSTRUCTURE with empty parameter list `()`
/// instead of NIL. Both forms must be accepted per Postel's law.
#[test]
fn edge_bodystructure_empty_parens_params() {
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" () NIL NIL \"7BIT\" 42 3))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(f) = &*u {
            let bs = f
                .body_structure
                .as_ref()
                .expect("body_structure should be present");
            if let BodyStructure::Text { params, .. } = bs {
                assert!(
                    params.is_empty(),
                    "empty () params should produce empty vec, got {params:?}"
                );
            } else {
                panic!("expected Text body structure, got {bs:?}");
            }
        } else {
            panic!("expected Fetch response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 3501 Section 5.1.3: `&-` (escaped ampersand) must decode to
/// literal `&`. An empty Base64 section between `&` and `-` is the
/// canonical encoding for the ampersand character.
#[test]
fn edge_utf7_escaped_ampersand() {
    use crate::codec::utf7::{decode_utf7, encode_utf7};

    // &- must decode to &
    assert_eq!(
        decode_utf7(b"&-"),
        "&",
        "RFC 3501 Section 5.1.3: &- must decode to literal &"
    );

    // & must encode to &-
    assert_eq!(
        encode_utf7("&"),
        "&-",
        "RFC 3501 Section 5.1.3: & must encode as &-"
    );

    // Multiple &- in sequence
    assert_eq!(
        decode_utf7(b"&-&-"),
        "&&",
        "RFC 3501 Section 5.1.3: consecutive &- must each decode to &"
    );

    // &- mixed with other text
    assert_eq!(
        decode_utf7(b"AT&-T"),
        "AT&T",
        "RFC 3501 Section 5.1.3: &- in the middle of text"
    );
}

/// RFC 3501 Section 7.4.2: ENVELOPE with NIL date but valid subject
/// and addresses. Drafts commonly have missing dates.
#[test]
fn edge_envelope_nil_date_valid_subject() {
    let input = b"(NIL \"Draft subject\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            NIL NIL \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL NIL)";
    let (_, env) = envelope(input, false).unwrap();
    assert!(env.date.is_none(), "NIL date must be None");
    assert_eq!(env.subject.as_deref(), Some("Draft subject"));
    assert_eq!(env.from.len(), 1);
    assert_eq!(env.from[0].mailbox.as_deref(), Some("alice"));
}

/// RFC 3501 Section 7.4.2: BODYSTRUCTURE with message/rfc822 type
/// that embeds a nested multipart body. Must parse without error.
#[test]
fn edge_bodystructure_message_rfc822_nested() {
    // message/rfc822 with embedded multipart/mixed
    let input = b"(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 \
            (NIL \"Inner subject\" ((\"Inner\" NIL \"inner\" \"ex.com\")) NIL NIL NIL NIL NIL NIL NIL) \
            ((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5)(\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 200) \"MIXED\") \
            20)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Message {
        media_subtype,
        envelope: env,
        body,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "rfc822");
        assert_eq!(env.subject.as_deref(), Some("Inner subject"));
        if let BodyStructure::Multipart {
            media_subtype: inner_sub,
            bodies,
            ..
        } = body.as_ref()
        {
            assert_eq!(inner_sub, "mixed");
            assert_eq!(bodies.len(), 2);
        } else {
            panic!("expected inner Multipart, got {body:?}");
        }
    } else {
        panic!("expected Message, got {bs:?}");
    }
}

/// ENVELOPE with RFC 2047 encoded subject that uses Q-encoding with
/// underscores. The IMAP parser should decode underscores as spaces
/// per RFC 2047 Section 4.2.
#[test]
fn edge_envelope_q_encoded_subject_underscore() {
    let input = b"(NIL \"=?UTF-8?Q?hello_world?=\" \
            ((\"Test\" NIL \"test\" \"ex.com\")) \
            NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, false).unwrap();
    assert_eq!(
        env.subject.as_deref(),
        Some("hello world"),
        "RFC 2047 Section 4.2: underscore in Q-encoding must decode to space \
             in IMAP ENVELOPE subject"
    );
}

/// RFC 3501 Section 7.4.2: "If the Sender or Reply-To lines are absent
/// in the [RFC-2822] header, or are present but empty, the server sets
/// the corresponding member of the envelope to be the same value as the
/// from member (the client is not expected to know to do this)."
///
/// Some servers violate this by sending NIL for sender/reply-to even when
/// from is populated. The parser must default sender and `reply_to` to from
/// when they are NIL, because consumer code relies on these fields being
/// populated per the RFC contract.
#[test]
fn envelope_nil_sender_reply_to_defaults_to_from() {
    // from=(Alice), sender=NIL, reply-to=NIL
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            NIL \
            NIL \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, false).unwrap();
    assert_eq!(env.from.len(), 1);
    // RFC 3501 Section 7.4.2: sender defaults to from when NIL
    assert_eq!(
        env.sender, env.from,
        "sender must default to from when server sends NIL (RFC 3501 Section 7.4.2)"
    );
    // RFC 3501 Section 7.4.2: reply_to defaults to from when NIL
    assert_eq!(
        env.reply_to, env.from,
        "reply_to must default to from when server sends NIL (RFC 3501 Section 7.4.2)"
    );
}

/// RFC 3501 Section 7.4.2: when from is also NIL (all-NIL envelope),
/// sender and `reply_to` should remain empty --- there is nothing to default to.
#[test]
fn envelope_nil_sender_reply_to_stays_empty_when_from_nil() {
    let input = b"(NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL)";
    let (_, env) = envelope(input, false).unwrap();
    assert!(env.from.is_empty());
    assert!(
        env.sender.is_empty(),
        "sender must remain empty when from is also NIL"
    );
    assert!(
        env.reply_to.is_empty(),
        "reply_to must remain empty when from is also NIL"
    );
}

/// RFC 3501 Section 7.4.2: when sender/reply-to are explicitly provided
/// (non-NIL), they must NOT be overridden with from.
#[test]
fn envelope_explicit_sender_reply_to_not_overridden() {
    let input = b"(\"Mon, 7 Feb 2022 21:52:25 -0800\" \"Test\" \
            ((\"Alice\" NIL \"alice\" \"example.com\")) \
            ((\"Secretary\" NIL \"secretary\" \"example.com\")) \
            ((\"ReplyAddr\" NIL \"reply\" \"example.com\")) \
            ((\"Bob\" NIL \"bob\" \"example.com\")) \
            NIL NIL NIL \
            \"<msg@example.com>\")";
    let (_, env) = envelope(input, false).unwrap();
    assert_eq!(env.sender.len(), 1);
    assert_eq!(env.sender[0].mailbox.as_deref(), Some("secretary"));
    assert_eq!(env.reply_to.len(), 1);
    assert_eq!(env.reply_to[0].mailbox.as_deref(), Some("reply"));
}

/// RFC 9051 Section 9: literal8 syntax is `~{number64}`  -  the `+`
/// (non-synchronizing) modifier is not defined. Per Postel's law, we
/// tolerate `~{N+}` since `+` has no semantic impact on server-to-client data.
#[test]
fn literal8_tolerates_non_synchronizing_modifier() {
    // ~{100+}\r\n followed by 100 bytes of data.
    let mut input = b"~{100+}\r\n".to_vec();
    input.extend(std::iter::repeat_n(b'x', 100));
    let result = literal(&input);
    assert!(
        result.is_ok(),
        "literal8 with '+' modifier must be tolerated per Postel's law; got {result:?}"
    );
    let (rest, val) = result.unwrap();
    assert_eq!(val.len(), 100);
    assert!(rest.is_empty());
}

/// RFC 9051 Section 9: valid literal8 syntax `~{N}` must be accepted.
#[test]
fn literal8_accepts_valid_syntax() {
    let mut input = b"~{5}\r\n".to_vec();
    input.extend(b"HELLO");
    let result = literal(&input);
    assert!(
        result.is_ok(),
        "valid literal8 ~{{5}} must be accepted (RFC 9051 Section 9); got {result:?}"
    );
    let (remaining, data) = result.unwrap();
    assert_eq!(data, b"HELLO");
    assert!(remaining.is_empty());
}

// ===== skip_balanced_parens literal overflow tests =====

/// RFC 3501 Section 9: when a literal count exceeds the available input,
/// `skip_balanced_parens` must break out of the loop rather than treating
/// the literal body bytes as parenthesized structure. If it continues,
/// parentheses inside the literal body would corrupt the depth tracking.
#[test]
fn skip_balanced_parens_literal_overflow_must_not_process_body_bytes() {
    // Scenario: we are inside a parenthesized group at depth 1, and the
    // content has a literal `{100}` whose body exceeds available data.
    // The literal body contains a `)` which, if treated as structure,
    // would decrement depth to 0 and cause premature return  -  breaking
    // out before the actual closing `)` of the group.
    //
    // Input layout (depth 1 context  -  the outer `(` was consumed by caller):
    //   "key {100}\r\n)garbage real_close)"
    //
    // Without fix: digits/}\r\n consumed byte-by-byte, then `)` at depth 0
    //   causes immediate return with rest = "garbage real_close)".
    // With fix: break on overflow, return remaining from overflow point.
    let input = b"key {100}\r\n)garbage real_close)after";
    let (rest, ()) = skip_balanced_parens(input).unwrap();
    // Without the fix, the function treats the `)` in the literal body as
    // a structural close-paren (depth was 0), returning ")garbage real_close)after".
    // With the fix, the function returns early at the overflow point. The
    // returned rest should include the literal body bytes (not consumed),
    // and crucially the `)` in the literal body should NOT have been
    // treated as a structural close-paren.
    assert!(
        rest.len() > b")garbage real_close)after".len(),
        "skip_balanced_parens must return early on literal overflow, not \
             consume body bytes as structure (RFC 3501 Section 9); \
             rest = {:?}",
        String::from_utf8_lossy(rest)
    );
}

/// RFC 3501 Section 9 / RFC 9051 Section 9: when `checked_add` overflows
/// (crafted literal count near `usize::MAX`), `skip_balanced_parens` must
/// break rather than silently continuing.
#[test]
fn skip_balanced_parens_literal_checked_add_overflow() {
    // A literal count of 18446744073709551615 (u64::MAX) would overflow
    // usize::MAX on checked_add. The function must not panic.
    let input = b"ext {18446744073709551615}\r\ndata) rest";
    let result = skip_balanced_parens(input);
    assert!(
        result.is_ok(),
        "skip_balanced_parens must not panic on overflow: {result:?}"
    );
}

// ===== Trailing-whitespace tolerance tests (Postel's law) =====

/// Postel's law: ID response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_id() {
    let input = b"ID (\"name\" \"test\")  \r\n";
    let (_, resp) = parse_untagged_id(input).unwrap();
    assert!(matches!(resp, UntaggedResponse::Id(_)));
}

/// Postel's law: NAMESPACE response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_namespace() {
    let input = b"NAMESPACE ((\"\" \"/\")) NIL NIL  \r\n";
    let (_, resp) = parse_untagged_namespace(input, false).unwrap();
    assert!(matches!(resp, UntaggedResponse::Namespace { .. }));
}

/// Postel's law: QUOTA response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_quota() {
    let input = b"QUOTA \"\" (STORAGE 10 512)  \r\n";
    let (_, resp) = parse_untagged_quota(input).unwrap();
    assert!(matches!(resp, UntaggedResponse::Quota { .. }));
}

/// Postel's law: QUOTAROOT response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_quotaroot() {
    let input = b"QUOTAROOT INBOX root1  \r\n";
    let (_, resp) = parse_untagged_quotaroot(input, false).unwrap();
    assert!(matches!(resp, UntaggedResponse::QuotaRoot { .. }));
}

/// Postel's law: ACL response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_acl() {
    let input = b"ACL INBOX alice lrs  \r\n";
    let (_, resp) = parse_untagged_acl(input, false).unwrap();
    assert!(matches!(resp, UntaggedResponse::Acl { .. }));
}

/// Postel's law: MYRIGHTS response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_myrights() {
    let input = b"MYRIGHTS INBOX lrs  \r\n";
    let (_, resp) = parse_untagged_myrights(input, false).unwrap();
    assert!(matches!(resp, UntaggedResponse::MyRights { .. }));
}

/// RFC 4731 Section 3.1: ESEARCH with an unknown key that has no value
/// (immediately followed by CRLF) must not cause a parse failure.
/// Servers may extend ESEARCH with new keys, and clients should skip
/// unknown keys gracefully per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn regression_esearch_unknown_key_no_value() {
    let input = b"* ESEARCH (TAG \"t1\") UID COUNT 5 XFUTURE\r\n";
    let resp = parse_response(input);
    let (_, resp) = resp.expect(
        "ESEARCH with trailing unknown key (no value) must parse \
             per Postel's law (RFC 1122 Section 1.2.2)",
    );
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert_eq!(
            esearch.count,
            Some(5),
            "COUNT must be preserved when trailing unknown key has no value"
        );
        assert!(esearch.uid, "UID indicator must be preserved");
        assert_eq!(esearch.tag.as_deref(), Some("t1"), "TAG must be preserved");
        return;
    }
    panic!("expected ESEARCH response");
}

/// Postel's law: LISTRIGHTS response with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_listrights() {
    let input = b"LISTRIGHTS INBOX fred lr s w  \r\n";
    let (_, resp) = parse_untagged_listrights(input, false).unwrap();
    assert!(matches!(resp, UntaggedResponse::ListRights { .. }));
}

/// Postel's law: METADATA response (Form 1, parenthesized) with trailing spaces before CRLF.
#[test]
fn trailing_whitespace_metadata_form1() {
    let input = b"METADATA \"INBOX\" (/private/comment \"value\")  \r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    assert!(matches!(resp, UntaggedResponse::Metadata { .. }));
}

/// Postel's law: METADATA response (Form 2, unsolicited entry-list) with trailing spaces.
#[test]
fn trailing_whitespace_metadata_form2() {
    let input = b"METADATA \"INBOX\" /private/comment  \r\n";
    let (_, resp) = parse_untagged_metadata(input, false).unwrap();
    assert!(matches!(resp, UntaggedResponse::Metadata { .. }));
}

/// Postel's law: BODYSTRUCTURE multipart detection must tolerate whitespace
/// after the opening paren (RFC 3501 Section 7.4.2 / RFC 1122 Section 1.2.2).
/// Some servers insert a space between the opening `(` and the first child `(`.
#[test]
fn bodystructure_multipart_whitespace_after_open_paren() {
    let input = b"* 1 FETCH (BODYSTRUCTURE ( (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 100 5)(\"TEXT\" \"HTML\" (\"charset\" \"utf-8\") NIL NIL \"7BIT\" 200 10) \"ALTERNATIVE\"))\r\n";
    let (_, resp) =
        parse_response(input).expect("BODYSTRUCTURE with space after opening paren should parse");
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            let bs = fr.body_structure.expect("should have body_structure");
            if let BodyStructure::Multipart {
                media_subtype,
                bodies,
                ..
            } = &bs
            {
                assert_eq!(media_subtype, "alternative");
                assert_eq!(bodies.len(), 2, "expected 2 children in multipart");
                // Verify first child is text/plain
                if let BodyStructure::Text {
                    media_subtype: sub, ..
                } = &bodies[0]
                {
                    assert_eq!(sub, "plain");
                } else {
                    panic!("expected first child to be Text, got {:?}", bodies[0]);
                }
                // Verify second child is text/html
                if let BodyStructure::Text {
                    media_subtype: sub, ..
                } = &bodies[1]
                {
                    assert_eq!(sub, "html");
                } else {
                    panic!("expected second child to be Text, got {:?}", bodies[1]);
                }
            } else {
                panic!("expected Multipart, got {bs:?}");
            }
        } else {
            panic!("expected Fetch response");
        }
    } else {
        panic!("expected Untagged response");
    }
}

/// Postel's law: `body_params` must tolerate multiple spaces between
/// parameter key-value pairs (RFC 3501 Section 7.4.2 / RFC 1122 Section 1.2.2).
/// Some servers insert extra whitespace between parameter pairs.
#[test]
fn bodystructure_body_params_multiple_spaces_between_pairs() {
    // Double space between "utf-8" and "name" (between two key-value pairs)
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"charset\" \"utf-8\"  \"name\" \"file.txt\") NIL NIL \"7BIT\" 100 5))\r\n";
    let (_, resp) =
        parse_response(input).expect("BODYSTRUCTURE with double space in params should parse");
    if let Response::Untagged(boxed) = resp {
        if let UntaggedResponse::Fetch(fr) = *boxed {
            let bs = fr.body_structure.expect("should have body_structure");
            if let BodyStructure::Text { params, .. } = &bs {
                assert_eq!(
                    params.len(),
                    2,
                    "expected 2 parameter pairs, got {params:?}"
                );
                assert_eq!(params[0], ("charset".to_string(), "utf-8".to_string()));
                assert_eq!(params[1], ("name".to_string(), "file.txt".to_string()));
            } else {
                panic!("expected Text, got {bs:?}");
            }
        } else {
            panic!("expected Fetch response");
        }
    } else {
        panic!("expected Untagged response");
    }
}

// ===== Edge-case bug probe tests =====

/// RFC 3501 Section 7.4.2: ENVELOPE with all ten fields set to NIL.
/// A completely empty envelope (no date, no subject, no addresses, no
/// in-reply-to, no message-id) must parse successfully through the full
/// `parse_response` path and produce an Envelope with all fields empty/None.
///
/// This exercises the `envelope()` parser when every field is NIL,
/// including all eight address lists. When from is NIL, sender and
/// reply-to must also remain empty (not default to from, since from
/// is empty  -  RFC 3501 Section 7.4.2).
#[test]
fn edge_envelope_all_nil_full_response() {
    let input = b"* 1 FETCH (ENVELOPE (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL))\r\n";
    let (_, resp) = parse_response(input)
        .expect("ENVELOPE with all-NIL fields must parse (RFC 3501 Section 7.4.2)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        let env = fr
            .envelope
            .expect("FETCH response should contain an envelope");
        assert!(
            env.date.is_none(),
            "date must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.subject.is_none(),
            "subject must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.from.is_empty(),
            "from must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.sender.is_empty(),
            "sender must be empty when from is also NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.reply_to.is_empty(),
            "reply_to must be empty when from is also NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.to.is_empty(),
            "to must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.cc.is_empty(),
            "cc must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.bcc.is_empty(),
            "bcc must be empty for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.in_reply_to.is_none(),
            "in_reply_to must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        assert!(
            env.message_id.is_none(),
            "message_id must be None for NIL (RFC 3501 Section 7.4.2)"
        );
        return;
    }
    panic!("expected Untagged Fetch with Envelope");
}

/// RFC 3501 Section 7.4.2 / Postel's law (RFC 1122 Section 1.2.2):
/// FETCH response with unknown data items must be skipped gracefully.
/// The parser should preserve all recognized items (UID, FLAGS) and
/// silently skip unrecognized ones (XFUTURE, XBLOB).
#[test]
fn edge_fetch_unknown_data_items_skipped() {
    // FETCH with UID, an unknown atom-valued item, FLAGS, and an unknown
    // parenthesized-valued item. The parser must skip both unknowns and
    // still extract UID and FLAGS correctly.
    let input = b"* 1 FETCH (UID 42 XFUTURE somevalue FLAGS (\\Seen) XBLOB (nested stuff))\r\n";
    let (_, resp) =
        parse_response(input).expect("FETCH with unknown data items must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(
            fr.uid,
            Some(42),
            "UID must be preserved when unknown items are present"
        );
        let flags = fr.flags.expect("FLAGS must be preserved");
        assert_eq!(flags.len(), 1);
        assert_eq!(flags[0], Flag::Seen);
        return;
    }
    panic!("expected Untagged Fetch response");
}

/// RFC 3501 Section 7.2.2 / RFC 3348: LIST response with contradictory
/// mailbox attributes `\HasChildren` and `\HasNoChildren`.
/// While semantically contradictory, both attributes must be preserved
/// verbatim per Postel's law (RFC 1122 Section 1.2.2). The parser must
/// not discard or de-duplicate attributes.
#[test]
fn edge_list_contradictory_attributes_preserved() {
    let input = b"* LIST (\\HasChildren \\HasNoChildren) \"/\" \"INBOX\"\r\n";
    let (_, resp) = parse_response(input)
        .expect("LIST with contradictory attributes must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "INBOX");
        assert_eq!(info.delimiter, Some('/'));
        assert_eq!(
            info.attributes.len(),
            2,
            "both contradictory attributes must be preserved; got {:?}",
            info.attributes
        );
        assert_eq!(
            info.attributes[0],
            MailboxAttribute::HasChildren,
            "first attribute must be \\HasChildren"
        );
        assert_eq!(
            info.attributes[1],
            MailboxAttribute::HasNoChildren,
            "second attribute must be \\HasNoChildren"
        );
        return;
    }
    panic!("expected Untagged List response");
}

/// RFC 4731 Section 3.1: ESEARCH with TAG and UID indicator but no
/// result data  -  `* ESEARCH (TAG "A001") UID\r\n`.
/// This represents an empty search result. All result fields (MIN, MAX,
/// COUNT, ALL) must be None/empty.
#[test]
fn edge_esearch_empty_uid_list() {
    let input = b"* ESEARCH (TAG \"A001\") UID\r\n";
    let (_, resp) = parse_response(input)
        .expect("ESEARCH with empty UID result must parse (RFC 4731 Section 3.1)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert_eq!(
            esearch.tag.as_deref(),
            Some("A001"),
            "TAG must be preserved"
        );
        assert!(esearch.uid, "UID indicator must be true");
        assert!(
            esearch.all.is_empty(),
            "ALL must be empty for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.min, None,
            "MIN must be None for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.max, None,
            "MAX must be None for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.count, None,
            "COUNT must be None for an empty ESEARCH result"
        );
        assert_eq!(
            esearch.mod_seq, None,
            "MODSEQ must be None for an empty ESEARCH result"
        );
        return;
    }
    panic!("expected Untagged Esearch response");
}

/// RFC 3501 Section 9: quoted-string with trailing escaped backslash
/// `"hello\\"` must parse as the five bytes `hello\`.
/// The `\\` is a valid `quoted-specials` escape per the ABNF:
/// `quoted-specials = DQUOTE / "\"`
#[test]
fn edge_quoted_string_trailing_escaped_backslash() {
    let (rest, val) = quoted_string(b"\"hello\\\\\" rest").unwrap();
    assert_eq!(
        val, b"hello\\",
        "\\\\  at end of quoted string must parse as single backslash \
             (RFC 3501 Section 9: quoted-specials)"
    );
    assert_eq!(rest, b" rest", "rest must follow the closing quote");
}

/// RFC 3501 Section 9: literal strings can contain arbitrary bytes,
/// including CRLF sequences. A literal `{10}\r\nhello\r\nbye` must
/// preserve the embedded `\r\n` in the data.
/// "hello\r\nbye" = 10 bytes: h(1)e(2)l(3)l(4)o(5)\r(6)\n(7)b(8)y(9)e(10).
#[test]
fn edge_literal_containing_crlf() {
    let input = b"{10}\r\nhello\r\nbye rest";
    let (rest, val) = literal(input).unwrap();
    assert_eq!(
        val, b"hello\r\nbye",
        "literal must preserve embedded CRLF (RFC 3501 Section 9)"
    );
    assert_eq!(rest, b" rest");
}

/// RFC 3501 Section 7.2.4 / Postel's law (RFC 1122 Section 1.2.2):
/// STATUS response with an unknown status item (UNKNOWN) must skip the
/// unrecognized item gracefully and still parse recognized items.
#[test]
fn edge_status_unknown_items_skipped() {
    let input = b"* STATUS INBOX (MESSAGES 5 UNKNOWN 3)\r\n";
    let (_, resp) =
        parse_response(input).expect("STATUS with unknown items must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::MailboxStatus { mailbox, items } = *boxed
    {
        assert_eq!(mailbox.as_str(), "INBOX");
        // Only MESSAGES should be recognized; UNKNOWN should be skipped.
        assert_eq!(
            items.len(),
            1,
            "only recognized status items should be present; got {items:?}"
        );
        assert_eq!(
            items[0],
            StatusItem::Messages(5),
            "MESSAGES value must be preserved"
        );
        return;
    }
    panic!("expected Untagged MailboxStatus response");
}

/// RFC 3501 Section 9 / RFC 2231 Section 3: BODYSTRUCTURE with RFC 2231
/// continuation parameters (`*0`, `*1` suffixes) must be reassembled
/// correctly by the parser's integrated RFC 2231 decoding.
///
/// This is an end-to-end test: parse a full FETCH/BODYSTRUCTURE response
/// and verify the params are already decoded.
#[test]
fn edge_bodystructure_rfc2231_continuation_reassembly() {
    // BODYSTRUCTURE with a filename split across two continuation segments.
    // filename*0="very_long_" filename*1="document.pdf"
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"APPLICATION\" \"PDF\" (\"FILENAME*0\" \"very_long_\" \"FILENAME*1\" \"document.pdf\") NIL NIL \"BASE64\" 99999))\r\n";
    let (_, resp) =
        parse_response(input).expect("BODYSTRUCTURE with RFC 2231 continuation params must parse");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        let bs = fr
            .body_structure
            .expect("FETCH response should contain a body_structure");
        if let BodyStructure::Basic { params, .. } = &bs {
            assert_eq!(
                params.len(),
                1,
                "RFC 2231 continuation segments must be reassembled into one param; \
                     got {params:?}"
            );
            assert_eq!(
                params[0].0, "filename",
                "reassembled param key must be the base name"
            );
            assert_eq!(
                params[0].1, "very_long_document.pdf",
                "reassembled param value must concatenate all segments \
                     (RFC 2231 Section 3)"
            );
            return;
        }
        panic!("expected Basic body structure, got {bs:?}");
    }
    panic!("expected Untagged Fetch response");
}

/// RFC 3501 Section 9: literal with CRLF in a full FETCH BODY[] response.
/// The literal byte count includes the CRLF bytes, so a literal containing
/// `line1\r\nline2` is 12 bytes and must be preserved verbatim.
#[test]
fn edge_fetch_literal_with_crlf() {
    // BODY[] with a literal value containing embedded CRLF.
    // "line1\r\nline2" is 12 bytes.
    let input = b"* 1 FETCH (BODY[] {12}\r\nline1\r\nline2)\r\n";
    let (_, resp) = parse_response(input)
        .expect("FETCH with literal containing CRLF must parse (RFC 3501 Section 9)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Fetch(fr) = *boxed
    {
        assert_eq!(fr.body_sections.len(), 1, "should have one body section");
        let section = &fr.body_sections[0];
        assert_eq!(
            section.data.as_deref(),
            Some(b"line1\r\nline2".as_slice()),
            "literal must preserve embedded CRLF bytes (RFC 3501 Section 9)"
        );
        return;
    }
    panic!("expected Untagged Fetch response");
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

/// LIST attribute parser must tolerate multiple spaces between attributes
/// per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn list_attributes_tolerate_multiple_spaces() {
    let input = b"* LIST (\\NoSelect  \\HasChildren) \"/\" \"Archive\"\r\n";
    let (_, resp) = parse_response(input)
        .expect("LIST with double-spaced attributes must parse (Postel's law)");
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::List(info) = *boxed
    {
        assert_eq!(info.name.as_str(), "Archive");
        assert_eq!(
            info.attributes.len(),
            2,
            "both attributes must be parsed despite double spaces; got {:?}",
            info.attributes
        );
        assert_eq!(info.attributes[0], MailboxAttribute::NoSelect);
        assert_eq!(info.attributes[1], MailboxAttribute::HasChildren);
        return;
    }
    panic!("expected Untagged List response");
}

/// RFC 2045 Section 5.1: media types are case-insensitive; the canonical
/// form used in RFC examples is lowercase (e.g. `text/plain`, not
/// `TEXT/PLAIN`). Verify that the BODYSTRUCTURE parser normalizes
/// `media_type`, `media_subtype`, and `encoding` to lowercase for
/// consistency with the `disposition_type` field (which already uses
/// lowercase).
#[test]
fn bodystructure_fields_normalized_to_lowercase() {
    // Single-part text: media_subtype and encoding must be lowercase.
    let text_input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"BASE64\" 100 5)";
    let (_, bs) = body_structure(text_input, false, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        encoding,
        ..
    } = &bs
    {
        assert_eq!(
            media_subtype, "plain",
            "media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(
            encoding, "base64",
            "encoding must be lowercase per RFC 2045 Section 6"
        );
    } else {
        panic!("expected Text, got {bs:?}");
    }

    // Basic part: media_type and media_subtype must be lowercase.
    let basic_input = b"(\"IMAGE\" \"PNG\" NIL NIL NIL \"BASE64\" 2048)";
    let (_, bs) = body_structure(basic_input, false, 0).unwrap();
    if let BodyStructure::Basic {
        media_type,
        media_subtype,
        encoding,
        ..
    } = &bs
    {
        assert_eq!(
            media_type, "image",
            "media_type must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(
            media_subtype, "png",
            "media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
        assert_eq!(
            encoding, "base64",
            "encoding must be lowercase per RFC 2045 Section 6"
        );
    } else {
        panic!("expected Basic, got {bs:?}");
    }

    // Multipart: media_subtype must be lowercase.
    let mpart_input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 80 4) \"MIXED\")";
    let (_, bs) = body_structure(mpart_input, false, 0).unwrap();
    if let BodyStructure::Multipart { media_subtype, .. } = &bs {
        assert_eq!(
            media_subtype, "mixed",
            "multipart media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
    } else {
        panic!("expected Multipart, got {bs:?}");
    }

    // Message: media_subtype must be lowercase.
    let msg_input = b"(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 \
            (NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL) \
            (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 100 5) 20)";
    let (_, bs) = body_structure(msg_input, false, 0).unwrap();
    if let BodyStructure::Message { media_subtype, .. } = &bs {
        assert_eq!(
            media_subtype, "rfc822",
            "message media_subtype must be lowercase per RFC 2045 Section 5.1"
        );
    } else {
        panic!("expected Message, got {bs:?}");
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

/// body-fld-lang with double spaces between language tags.
///
/// Regression test: `separated_list0(sp, ...)` with a single-space
/// separator would fail on the extra space between language tags.
///
/// RFC 3501 Section 7.4.2 / Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn body_language_double_space_between_tags() {
    // Two spaces between "en" and "fr"
    let (_, lang) = body_language(b"(\"en\"  \"fr\")").unwrap();
    assert_eq!(
        lang,
        Some(vec!["en".to_owned(), "fr".to_owned()]),
        "double space between body-fld-lang tags must be tolerated"
    );
}

/// LIST CHILDINFO extended data with double spaces between items.
///
/// Regression test: `separated_list0(sp, ...)` with a single-space
/// separator would fail on extra whitespace inside the CHILDINFO
/// parenthesized list.
///
/// RFC 5258 Section 6 / Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn list_childinfo_double_space_between_items() {
    // Two spaces between "SUBSCRIBED" and "REMOTE" in CHILDINFO
    let input =
        b"* LIST (\\HasChildren) \".\" \"Parent\" (\"CHILDINFO\" (\"SUBSCRIBED\"  \"REMOTE\"))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(body) = resp {
        if let UntaggedResponse::List(mailbox) = body.as_ref() {
            assert_eq!(
                mailbox.child_info,
                vec!["SUBSCRIBED", "REMOTE"],
                "double space between CHILDINFO items must be tolerated"
            );
        } else {
            panic!("expected List, got {body:?}");
        }
    } else {
        panic!("expected Untagged response, got {resp:?}");
    }
}

/// RFC 3501 Section 7.4.2 / Postel's law: some non-conformant servers
/// (older Exchange, Yahoo) send bare atoms for media type, subtype, and
/// encoding in BODYSTRUCTURE instead of the quoted strings that the
/// formal grammar requires.  The parser must accept both forms.
#[test]
fn bodystructure_bare_atom_media_type() {
    // Bare atoms instead of quoted strings for media type, subtype, and encoding.
    let input = b"(TEXT PLAIN (\"CHARSET\" \"UTF-8\") NIL NIL 7BIT 42 3)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text {
        media_subtype,
        encoding,
        size,
        lines,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "plain");
        assert_eq!(encoding, "7bit");
        assert_eq!(*size, 42);
        assert_eq!(*lines, 3);
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

/// Same as above but for a basic (non-text) body part with bare atoms.
#[test]
fn bodystructure_bare_atom_basic_type() {
    let input = b"(IMAGE JPEG NIL NIL NIL BASE64 12345)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Basic {
        media_type,
        media_subtype,
        encoding,
        size,
        ..
    } = &bs
    {
        assert_eq!(media_type, "image");
        assert_eq!(media_subtype, "jpeg");
        assert_eq!(encoding, "base64");
        assert_eq!(*size, 12345);
    } else {
        panic!("expected Basic, got {bs:?}");
    }
}

/// Multipart with bare atom subtype.
#[test]
fn bodystructure_bare_atom_multipart_subtype() {
    let input =
            b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)(\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 20 2) MIXED)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 2);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

// ===== Bug fix regression tests =====

/// RFC 3501 Section 9 / Postel's law: double space between a parameter key
/// and value in body-fld-param must be tolerated. The between-pair separator
/// already accepts multiple spaces; the within-pair separator must too.
#[test]
fn body_params_double_space_within_pair() {
    // "CHARSET"  "UTF-8"  -  double space between key and value
    let input = b"(\"CHARSET\"  \"UTF-8\")";
    let (_, params) = body_params(input).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].0, "charset");
    assert_eq!(params[0].1, "UTF-8");
}

/// RFC 3501 Section 9 / Postel's law: double space within pair in a full
/// BODYSTRUCTURE context  -  ensures the tolerance doesn't break upstream parsing.
#[test]
fn bodystructure_params_double_space_within_pair() {
    let input = b"(\"TEXT\" \"PLAIN\" (\"CHARSET\"  \"UTF-8\") NIL NIL \"7BIT\" 100 5)";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Text { params, .. } = &bs {
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "charset");
        assert_eq!(params[0].1, "UTF-8");
    } else {
        panic!("expected Text, got {bs:?}");
    }
}

/// RFC 3501 Section 9 / Postel's law: multipart BODYSTRUCTURE with extra
/// whitespace (tab or multiple spaces) before the subtype must be tolerated.
/// The code already tolerates tabs between child bodies; the separator before
/// the subtype must be consistent.
#[test]
fn bodystructure_mpart_multi_space_before_subtype() {
    // Two spaces between the child body closing paren and the subtype string.
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)  \"MIXED\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 1);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

/// RFC 3501 Section 9 / Postel's law: tab character before multipart subtype
/// must be tolerated, consistent with tab tolerance between child bodies.
#[test]
fn bodystructure_mpart_tab_before_subtype() {
    let input = b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)\t\"MIXED\")";
    let (_, bs) = body_structure(input, false, 0).unwrap();
    if let BodyStructure::Multipart {
        media_subtype,
        bodies,
        ..
    } = &bs
    {
        assert_eq!(media_subtype, "mixed");
        assert_eq!(bodies.len(), 1);
    } else {
        panic!("expected Multipart, got {bs:?}");
    }
}

/// RFC 3501 Section 7.3.1 / Postel's law: EXISTS with trailing whitespace
/// before CRLF must still parse as Exists, not Unknown. Twelve other
/// response parsers tolerate trailing whitespace; EXISTS must too.
#[test]
fn exists_trailing_whitespace_tolerated() {
    let input = b"* 5 EXISTS \r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(
            *boxed,
            UntaggedResponse::Exists(5),
            "EXISTS with trailing space must parse as Exists(5), not Unknown"
        );
    } else {
        panic!("expected Untagged Exists");
    }
}

/// RFC 3501 Section 7.3.2 / Postel's law: RECENT with trailing whitespace
/// before CRLF must still parse as Recent, not Unknown.
#[test]
fn recent_trailing_whitespace_tolerated() {
    let input = b"* 0 RECENT \r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(
            *boxed,
            UntaggedResponse::Recent(0),
            "RECENT with trailing space must parse as Recent(0), not Unknown"
        );
    } else {
        panic!("expected Untagged Recent");
    }
}

/// RFC 3501 Section 7.4.1 / Postel's law: EXPUNGE with trailing whitespace
/// before CRLF must still parse as Expunge, not Unknown.
#[test]
fn expunge_trailing_whitespace_tolerated() {
    let input = b"* 3 EXPUNGE \r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp {
        assert_eq!(
            *boxed,
            UntaggedResponse::Expunge(3),
            "EXPUNGE with trailing space must parse as Expunge(3), not Unknown"
        );
    } else {
        panic!("expected Untagged Expunge");
    }
}

// ========================================================================
// Postel's law regression tests  -  full FETCH response (Class A bugs)
// ========================================================================

/// Regression: full FETCH response with BODYSTRUCTURE containing extra
/// spaces between fields must parse end-to-end.
/// Bug 54c67e0: strict `sp` combinator rejected multi-space/tab input.
///
/// # References
/// - RFC 3501 Section 7.4.2 (BODYSTRUCTURE)
#[test]
fn fetch_bodystructure_extra_spaces_full_response() {
    let input = b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\" \"PLAIN\"  (\"CHARSET\" \"UTF-8\")  NIL  NIL  \"7BIT\"  100  5))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "BODYSTRUCTURE with extra spaces must parse as full FETCH response"
    );
}

/// Regression: full FETCH response with bare (unquoted) media type/subtype
/// in BODYSTRUCTURE must parse end-to-end.
/// Bug 8df3c4e: strict `string` combinator rejected bare atoms.
///
/// # References
/// - RFC 3501 Section 7.4.2 (BODYSTRUCTURE)
#[test]
fn fetch_bodystructure_bare_atom_full_response() {
    let input = b"* 1 FETCH (BODYSTRUCTURE (TEXT PLAIN (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" 100 5))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "BODYSTRUCTURE with bare atom media type must parse as full FETCH response"
    );
}

/// Regression: full FETCH response with multipart BODYSTRUCTURE containing
/// extra spaces between children must parse end-to-end.
/// Bug 2abc6e1: strict `sp` between parameter key-value pairs.
///
/// # References
/// - RFC 3501 Section 7.4.2 (BODYSTRUCTURE multipart)
#[test]
fn fetch_bodystructure_multipart_extra_spaces_full_response() {
    let input = b"* 1 FETCH (BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" (\"CHARSET\"  \"UTF-8\") NIL NIL \"7BIT\" 100 5)(\"TEXT\" \"HTML\" (\"CHARSET\"  \"UTF-8\") NIL NIL \"QUOTED-PRINTABLE\" 200 10)  \"ALTERNATIVE\"))\r\n";
    let result = parse_response(input);
    assert!(
        result.is_ok(),
        "multipart BODYSTRUCTURE with extra spaces must parse as full FETCH response"
    );
}

// ========================================================================
// Property-based invariant tests
// ========================================================================

mod prop_invariants {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        /// Parser never panics on arbitrary input (RFC 3501  -  Postel's law).
        #[test]
        fn parse_response_never_panics(data in prop::collection::vec(any::<u8>(), 0..500)) {
            let _ = parse_response_utf8(&data, false);
            let _ = parse_response_utf8(&data, true);
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
            if let Ok((remaining, _)) = parse_response_utf8(&data, false) {
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
            if let Ok((_, Response::Untagged(boxed))) = parse_response_utf8(&data, false).as_ref()
                && let UntaggedResponse::Search { uids, .. } = boxed.as_ref()
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
            if let Ok((rest_alone, _)) = parse_response_utf8(&data, false) {
                let sentinel = b"A001 OK done\r\n";
                let mut combined = data.clone();
                combined.extend_from_slice(sentinel);
                if let Ok((remaining, _)) = parse_response_utf8(&combined, false) {
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
}

/// Tests that parsers terminate in bounded time on adversarial inputs.
/// These catch infinite loops, excessive recursion, and algorithmic
/// complexity attacks that wouldn't be caught by single-call property tests.
mod stuck_tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Maximum time any single parse call should take.
    /// If it exceeds this, the parser is effectively "stuck".
    const MAX_PARSE_TIME: Duration = Duration::from_secs(5);

    fn assert_terminates<F: FnOnce()>(name: &str, f: F) {
        let start = Instant::now();
        f();
        let elapsed = start.elapsed();
        assert!(
            elapsed < MAX_PARSE_TIME,
            "{name} took {elapsed:?}, exceeds {MAX_PARSE_TIME:?}  -  parser may be stuck"
        );
    }

    #[test]
    fn parse_response_repeated_bytes_100kb() {
        // 100KB of \xff bytes  -  tests that non-ASCII bytes don't cause
        // excessive backtracking or infinite loops.
        let data = vec![0xFFu8; 100_000];
        assert_terminates("100KB 0xFF", || {
            let _ = parse_response_utf8(&data, false);
        });
    }

    #[test]
    fn parse_response_repeated_star_100kb() {
        // "* " prefix followed by 100KB of data  -  looks like an untagged
        // response but with garbage body.
        let mut data = b"* ".to_vec();
        data.extend(vec![b'A'; 100_000]);
        data.extend(b"\r\n");
        assert_terminates("100KB untagged garbage", || {
            let _ = parse_response_utf8(&data, false);
        });
    }

    #[test]
    fn parse_response_deeply_nested_parens() {
        // 10,000 nested opening parens  -  tests BODYSTRUCTURE depth limits
        let mut data = b"* 1 FETCH (BODYSTRUCTURE ".to_vec();
        for _ in 0..10_000 {
            data.push(b'(');
        }
        data.extend(b")\r\n");
        assert_terminates("10K nested parens", || {
            let _ = parse_response_utf8(&data, false);
        });
    }

    #[test]
    fn parse_response_long_literal_count() {
        // Literal with absurd byte count  -  should not allocate or loop
        let data = b"* 1 FETCH (BODY[] {999999999}\r\n";
        assert_terminates("huge literal count", || {
            let _ = parse_response_utf8(data, false);
        });
    }

    #[test]
    fn parse_greeting_repeated_bytes_100kb() {
        let mut data = b"* OK ".to_vec();
        data.extend(vec![b'X'; 100_000]);
        data.extend(b"\r\n");
        assert_terminates("100KB greeting", || {
            let _ = parse_greeting(&data);
        });
    }
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
    assert_eq!(Capability::Notify, Capability::Other("NOTIFY".into()));
    assert_eq!(Capability::Notify, Capability::Other("notify".into()));
}

/// `\NoAccess` mailbox attribute parsed from LIST response
/// (RFC 5465 Section 5.9, Section 8).
#[test]
fn list_noaccess_attribute_parsed() {
    let input = b"* LIST (\\NoAccess) \"/\" \"SharedFolder\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert!(
                info.attributes
                    .contains(&crate::types::MailboxAttribute::NoAccess),
                "should parse \\NoAccess attribute, got: {:?}",
                info.attributes
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// `\NoAccess` attribute is case-insensitive.
#[test]
fn list_noaccess_case_insensitive() {
    let input = b"* LIST (\\NOACCESS) \"/\" \"folder\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert!(
                info.attributes
                    .contains(&crate::types::MailboxAttribute::NoAccess),
                "\\NOACCESS (all caps) should match NoAccess, got: {:?}",
                info.attributes
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
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

/// `STATUS` response (used for non-selected mailbox NOTIFY events) parses correctly.
#[test]
fn status_response_for_notify_event() {
    let input = b"* STATUS \"Lists/Lemonade\" (UIDVALIDITY 4 UIDNEXT 10000 MESSAGES 501)\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::MailboxStatus { mailbox, items } = *u {
            assert_eq!(mailbox.as_str(), "Lists/Lemonade");
            assert!(
                items.len() >= 3,
                "should have UIDVALIDITY, UIDNEXT, MESSAGES, got: {items:?}"
            );
        } else {
            panic!("expected MailboxStatus, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

/// LIST with `OLDNAME` extended data used for `MailboxName` rename events
/// (RFC 5465 Section 5.5, RFC 9051 Section 6.3.9.7).
#[test]
fn list_with_oldname_for_notify_rename() {
    let input = b"* LIST () \"/\" \"NewMailbox\" (\"OLDNAME\" (\"OldMailbox\"))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert_eq!(info.name.as_str(), "NewMailbox");
            assert_eq!(
                info.old_name.as_ref().map(MailboxName::as_str),
                Some("OldMailbox"),
                "OLDNAME must be captured for NOTIFY rename events"
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

// ========================================================================
// Adversarial input: pathological headers, silent degradation, dropped
// extension data.
// ========================================================================

#[cfg(any())]
mod moved_encoded_word_window_tests {
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
}

/// A malformed form of a known response is a parse failure, not an extension
/// response. The driver closes the connection and the account boundary maps
/// it to `Protocol(ParseFailed)`.
#[test]
fn malformed_status_is_a_parse_failure() {
    let input = b"* STATUS \"INBOX\" (MESSAGES)\r\n";
    assert!(parse_response(input).is_err());
}

/// Gmail's `X-GM-LABELS` FETCH data item is decoded alongside the other Gmail
/// extension attributes. The generic IMAP account layer deliberately does not
/// interpret those labels as folder membership.
#[test]
fn fetch_x_gm_labels_are_exposed_without_losing_other_attributes() {
    // Google's IMAP extension documentation shows system labels as bare
    // backslash-prefixed atoms, not quoted strings: `\` is a quoted-special
    // and so cannot appear in an `astring` atom. This is the real wire form.
    let input = b"* 1 FETCH (UID 9 X-GM-MSGID 1278455344230334865 \
                  X-GM-LABELS (\\Inbox \\Sent Important \"Muy Importante\" \"&ZeVnLIqe-\") \
                  FLAGS (\\Seen))\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Fetch(fr) = *u {
            assert_eq!(fr.uid, Some(9), "UID before the unknown item survives");
            assert_eq!(fr.gmail_msg_id, Some(1_278_455_344_230_334_865));
            assert_eq!(
                fr.gmail_labels,
                Some(vec![
                    "\\Inbox".to_owned(),
                    "\\Sent".to_owned(),
                    "Important".to_owned(),
                    "Muy Importante".to_owned(),
                    "日本語".to_owned(),
                ]),
                "system labels arrive unquoted with a leading backslash; user \
                 labels are astrings in modified UTF-7"
            );
            assert_eq!(
                fr.flags,
                Some(vec![Flag::Seen]),
                "FLAGS after the unknown item survives"
            );
        } else {
            panic!("expected Fetch, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
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

/// A wire-decoded mailbox name is the server's own identifier, so it is kept
/// byte-for-byte even when it contains octets `MailboxName::new` rejects
/// (RFC 3501 Section 5.1: MUTF-7 can represent any code point). Rewriting it
/// would make the client address a mailbox the server never advertised; CRLF
/// safety is enforced when the name is re-encoded, not here.
#[test]
fn list_mailbox_name_preserves_control_characters_from_the_wire() {
    let input = b"* LIST () \"/\" \"&AAoALQ-\"\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::List(info) = *u {
            assert_eq!(
                info.name.as_str(),
                "\n-",
                "the server's name is preserved verbatim, LF and all"
            );
            assert!(
                MailboxName::new(info.name.as_str()).is_err(),
                "the validating constructor is stricter than the wire path on \
                 purpose; consumers must not assume its invariant for parsed names"
            );
        } else {
            panic!("expected List, got: {u:?}");
        }
    } else {
        panic!("expected Untagged, got: {resp:?}");
    }
}

// ===== skip_tagged_ext_simple terminator matrix (imap-T4) =====
//
// `skip_tagged_ext_simple` has exactly two callers, each with its own
// terminator set: ESEARCH (`parse_untagged_esearch`) passes SP/CR, STATUS
// (`parse_status_items`) passes SP/`)`. The four `tagged-ext-simple` forms
// (NIL, literal, quoted string, atom) interact with the terminator set in
// non-obvious ways, because only the NIL arm boundary-checks against it and
// the atom fallback is defined as "everything that is not a terminator".
// The matrix below pins the whole cross product.

/// The ESEARCH terminator set (RFC 9051 Section 7.3.4): SP or CR.
fn esearch_terminator(b: u8) -> bool {
    b == b' ' || b == b'\r'
}

/// The STATUS terminator set (RFC 9051 Section 7.3.2): SP or `)`.
fn status_terminator(b: u8) -> bool {
    b == b' ' || b == b')'
}

/// Run both caller terminator sets over one input and return the remaining
/// bytes for each, or `None` where the parser rejects the input.
fn skip_ext_both(input: &[u8]) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let esearch = skip_tagged_ext_simple(esearch_terminator)(input)
        .ok()
        .map(|(rest, ())| rest.to_vec());
    let status = skip_tagged_ext_simple(status_terminator)(input)
        .ok()
        .map(|(rest, ())| rest.to_vec());
    (esearch, status)
}

/// `esearch` / `status` give the expected remaining bytes, or `None` where the
/// parser must reject the input.
fn assert_skip_ext(input: &[u8], esearch: Option<&str>, status: Option<&str>) {
    let (got_esearch, got_status) = skip_ext_both(input);
    assert_eq!(
        got_esearch.as_deref(),
        esearch.map(str::as_bytes),
        "ESEARCH terminator set (SP/CR) on {:?}",
        String::from_utf8_lossy(input)
    );
    assert_eq!(
        got_status.as_deref(),
        status.map(str::as_bytes),
        "STATUS terminator set (SP/`)`) on {:?}",
        String::from_utf8_lossy(input)
    );
}

#[test]
fn skip_tagged_ext_simple_nil_form_across_terminator_sets() {
    // SP is a terminator for both, so the NIL arm claims it in both.
    assert_skip_ext(b"NIL rest", Some(" rest"), Some(" rest"));
    // End of input satisfies the NIL boundary check for both (RFC 9051
    // Section 4: `tagged-ext-simple` may be the last token).
    assert_skip_ext(b"NIL", Some(""), Some(""));
    // Case-insensitive per the ABNF's literal-string rules.
    assert_skip_ext(b"nil rest", Some(" rest"), Some(" rest"));

    // `)` terminates only for STATUS. For ESEARCH the boundary check fails and
    // the atom fallback swallows the paren too - harmless there, since ESEARCH
    // values never sit inside a parenthesized list.
    assert_skip_ext(b"NIL)", Some(""), Some(")"));

    // The mirror image: CR is a declared terminator only for ESEARCH, so the
    // STATUS NIL arm's boundary check fails and the atom fallback runs. It
    // stops at the CR anyway - CR/LF end the atom scan for every caller - so
    // both sets leave the response-terminating CRLF in place.
    assert_skip_ext(b"NIL\r\n", Some("\r\n"), Some("\r\n"));

    // An atom that merely starts with NIL must not be partially consumed
    // (the boundary check exists for exactly this).
    assert_skip_ext(b"NILSIMSA rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"NILSIMSA", Some(""), Some(""));
}

#[test]
fn skip_tagged_ext_simple_literal_form_across_terminator_sets() {
    // The literal arm is terminator-blind by construction: the octet count
    // decides where the value ends (RFC 9051 Section 4).
    assert_skip_ext(b"{3}\r\nabc rest", Some(" rest"), Some(" rest"));
    // Terminator bytes inside the literal body are data, not delimiters.
    assert_skip_ext(b"{5}\r\na b)c rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"{2}\r\n\r\n)", Some(")"), Some(")"));
    // literal8 (RFC 6855 Section 4) and LITERAL+ (RFC 7888) prefixes.
    assert_skip_ext(b"~{2}\r\nxy)", Some(")"), Some(")"));
    assert_skip_ext(b"{2+}\r\nxy)", Some(")"), Some(")"));
    // A literal whose body is shorter than its count is not a literal at all;
    // the atom fallback takes over and stops at the CR for both callers.
    assert_skip_ext(b"{9}\r\nab)", Some("\r\nab)"), Some("\r\nab)"));
}

#[test]
fn skip_tagged_ext_simple_quoted_form_across_terminator_sets() {
    // The quoted arm is tried before the atom fallback, so terminator bytes
    // inside the quotes stay inside the value for both callers.
    assert_skip_ext(b"\"a b)c\" rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"\"\" )", Some(" )"), Some(" )"));
    // RFC 9051 Section 4: `\"` and `\\` are the quoted-specials.
    assert_skip_ext(b"\"a\\\"b\" )", Some(" )"), Some(" )"));
    // An unterminated quote is not a quoted string; the atom fallback runs and
    // the two terminator sets diverge on where it stops.
    assert_skip_ext(b"\"abc)", Some(""), Some(")"));
    assert_skip_ext(b"\"abc rest", Some(" rest"), Some(" rest"));
}

#[test]
fn skip_tagged_ext_simple_atom_form_across_terminator_sets() {
    assert_skip_ext(b"123 rest", Some(" rest"), Some(" rest"));
    assert_skip_ext(b"1:*,5 )", Some(" )"), Some(" )"));
    // Numbers and sequence sets butted against the caller's own closer.
    assert_skip_ext(b"42)", Some(""), Some(")"));
    assert_skip_ext(b"42\r\n", Some("\r\n"), Some("\r\n"));

    // Nothing to consume: `take_while1` needs at least one non-terminator
    // byte, so a value position that opens with a terminator - or with the
    // CR/LF that ends the response line - is an error for both callers.
    assert_skip_ext(b"", None, None);
    assert_skip_ext(b" x", None, None);
    assert_skip_ext(b")x", Some(""), None);
    assert_skip_ext(b"\r\n", None, None);
}

/// The atom fallback must not scan past the response-terminating CRLF, whatever
/// the caller's terminator set says. The STATUS set (SP/`)`) does not name CR,
/// so before the CR/LF guard in `skip_tagged_ext_simple` this input consumed the
/// CRLF and the following response's `* ` - a parse failure rather than a
/// mis-parse, since STATUS then never found its `)`, but a scan across a
/// response boundary all the same. Every sibling helper (`skip_balanced_parens`,
/// `skip_paren_group`, `skip_fetch_value`) guards this way.
#[test]
fn skip_tagged_ext_simple_atom_stops_at_the_response_boundary() {
    let input = b"badvalue\r\n* OK next\r\n";
    for (label, rest) in [
        ("STATUS", skip_tagged_ext_simple(status_terminator)(input)),
        ("ESEARCH", skip_tagged_ext_simple(esearch_terminator)(input)),
    ] {
        let (rest, ()) = rest.unwrap();
        assert_eq!(
            rest, b"\r\n* OK next\r\n",
            "{label} terminator set left the CRLF for the response parser"
        );
    }
}

// ===== Structure-aware BODYSTRUCTURE generative pass (imap-T4) =====
//
// Two phases, both deterministic (fixed-seed SplitMix64, fixed iteration
// counts, no wall clock, no I/O):
//
//   1. Build well-formed BODYSTRUCTURE trees from a grammar, render them to
//      wire bytes, and assert the parser recovers the exact tree shape. This
//      is what the hand-written cases above cannot do: it walks the cross
//      product of nesting, part kind, parameter form, and extension-data
//      arity.
//   2. Mutate those same renderings the way a broken server would (truncate,
//      delete, or splice a structural byte) and assert the parser stays
//      well-behaved: it either errors or returns, never panics, never reports
//      more remaining input than it was given, and never recurses past
//      `MAX_BODY_NESTING_DEPTH`.

struct BodyRng(u64);

impl BodyRng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        assert!(n > 0);
        usize::try_from(self.next_u64() % u64::try_from(n).unwrap()).unwrap()
    }

    fn pick<'a, T>(&mut self, options: &'a [T]) -> &'a T {
        &options[self.below(options.len())]
    }
}

/// A BODYSTRUCTURE tree, independent of its wire spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BodyNode {
    Text(&'static str),
    Basic(&'static str, &'static str),
    Message(Box<BodyNode>),
    Multipart(&'static str, Vec<BodyNode>),
}

/// The wire spellings of `body-fld-param` that must all round-trip.
const PARAM_FORMS: [&str; 4] = [
    "NIL",
    "()",
    "(\"CHARSET\" \"UTF-8\")",
    "(\"NAME*0*\" \"utf-8''caf%C3%A9\" \"NAME*1*\" \".txt\")",
];

/// Optional single-part extension data, in increasing arity
/// (RFC 3501 Section 7.4.2: md5, disposition, language, location, then
/// unparsed `body-extension`).
const EXT_1PART_FORMS: [&str; 6] = [
    "",
    " NIL",
    " \"deadbeef\"",
    " NIL (\"attachment\" (\"FILENAME\" \"a.txt\"))",
    " NIL NIL (\"en\" \"no\")",
    " NIL NIL NIL \"http://example.invalid/p\" (\"future\" \"extension\")",
];

/// Optional multipart extension data (params, disposition, language,
/// location, then unparsed `body-extension`).
const EXT_MPART_FORMS: [&str; 5] = [
    "",
    " NIL",
    " (\"BOUNDARY\" \"b1\")",
    " NIL (\"inline\" NIL)",
    " NIL NIL (\"en\") \"http://example.invalid/m\" 42",
];

/// A minimal well-formed ENVELOPE, reused for every message/rfc822 part.
const EMBEDDED_ENVELOPE: &str = "(NIL \"embedded\" NIL NIL NIL NIL NIL NIL NIL NIL)";

fn generate_body_node(rng: &mut BodyRng, depth: usize) -> BodyNode {
    // Leaf-only below the budget, so tree size stays bounded.
    let choice = if depth == 0 {
        rng.below(2)
    } else {
        rng.below(4)
    };
    match choice {
        0 => BodyNode::Text(rng.pick(&["PLAIN", "HTML", "CALENDAR"])),
        1 => BodyNode::Basic(
            rng.pick(&["IMAGE", "APPLICATION", "AUDIO"]),
            rng.pick(&["GIF", "OCTET-STREAM", "PDF"]),
        ),
        2 => BodyNode::Message(Box::new(generate_body_node(rng, depth - 1))),
        _ => {
            let children = 1 + rng.below(3);
            let kids = (0..children)
                .map(|_| generate_body_node(rng, depth - 1))
                .collect();
            BodyNode::Multipart(rng.pick(&["MIXED", "ALTERNATIVE", "RELATED"]), kids)
        }
    }
}

fn render_body_node(rng: &mut BodyRng, node: &BodyNode, out: &mut String) {
    match node {
        BodyNode::Text(subtype) => {
            let params = *rng.pick(&PARAM_FORMS);
            let ext = *rng.pick(&EXT_1PART_FORMS);
            out.push_str(&format!(
                "(\"TEXT\" \"{subtype}\" {params} NIL NIL \"7BIT\" 1234 42{ext})"
            ));
        }
        BodyNode::Basic(ty, subtype) => {
            let params = *rng.pick(&PARAM_FORMS);
            let ext = *rng.pick(&EXT_1PART_FORMS);
            out.push_str(&format!(
                "(\"{ty}\" \"{subtype}\" {params} \"<cid@x>\" \"a description\" \"BASE64\" 9876{ext})"
            ));
        }
        BodyNode::Message(inner) => {
            let params = *rng.pick(&PARAM_FORMS);
            out.push_str(&format!(
                "(\"MESSAGE\" \"RFC822\" {params} NIL NIL \"7BIT\" 5000 {EMBEDDED_ENVELOPE} "
            ));
            render_body_node(rng, inner, out);
            let ext = *rng.pick(&EXT_1PART_FORMS);
            out.push_str(&format!(" 77{ext})"));
        }
        BodyNode::Multipart(subtype, kids) => {
            out.push('(');
            for kid in kids {
                render_body_node(rng, kid, out);
            }
            let ext = *rng.pick(&EXT_MPART_FORMS);
            out.push_str(&format!(" \"{subtype}\"{ext})"));
        }
    }
}

/// Project a parsed structure back onto the generator's tree model, so the
/// comparison is over shape (kind, subtype, children) and not over the
/// incidental field values the renderer chose.
fn parsed_body_node(bs: &BodyStructure) -> BodyNode {
    match bs {
        BodyStructure::Text { media_subtype, .. } => {
            BodyNode::Text(static_label(media_subtype, &["PLAIN", "HTML", "CALENDAR"]))
        }
        BodyStructure::Basic {
            media_type,
            media_subtype,
            ..
        } => BodyNode::Basic(
            static_label(media_type, &["IMAGE", "APPLICATION", "AUDIO"]),
            static_label(media_subtype, &["GIF", "OCTET-STREAM", "PDF"]),
        ),
        BodyStructure::Message { body, .. } => BodyNode::Message(Box::new(parsed_body_node(body))),
        BodyStructure::Multipart {
            media_subtype,
            bodies,
            ..
        } => BodyNode::Multipart(
            static_label(media_subtype, &["MIXED", "ALTERNATIVE", "RELATED"]),
            bodies.iter().map(parsed_body_node).collect(),
        ),
    }
}

/// The parser lowercases media types (RFC 2045 Section 5.1); map back to the
/// generator's uppercase label so the tree comparison is exact.
fn static_label(parsed: &str, candidates: &[&'static str]) -> &'static str {
    candidates
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(parsed))
        .unwrap_or_else(|| panic!("parser produced unexpected media label {parsed:?}"))
}

#[test]
fn generated_bodystructures_round_trip_to_their_tree_shape() {
    let mut rng = BodyRng(0xB0D1_0001);
    for _ in 0..1500 {
        let node = generate_body_node(&mut rng, 3);
        let mut wire = String::new();
        render_body_node(&mut rng, &node, &mut wire);
        let parsed = body_structure(wire.as_bytes(), false, 0);
        let Ok((rest, bs)) = parsed else {
            panic!("well-formed BODYSTRUCTURE failed to parse: {wire}");
        };
        assert!(rest.is_empty(), "trailing bytes {rest:?} for {wire}");
        assert_eq!(parsed_body_node(&bs), node, "shape diverged for {wire}");
    }
}

#[test]
fn mutated_bodystructures_never_panic_or_overrun() {
    let mut rng = BodyRng(0xB0D1_0002);
    for _ in 0..4000 {
        let node = generate_body_node(&mut rng, 3);
        let mut wire = String::new();
        render_body_node(&mut rng, &node, &mut wire);
        let mut bytes = wire.into_bytes();
        assert!(!bytes.is_empty());

        // One targeted structural mutation: the interesting damage is to the
        // delimiters, not to random payload bytes.
        let at = rng.below(bytes.len());
        match rng.below(4) {
            0 => bytes.truncate(at),
            1 => {
                bytes.remove(at);
            }
            2 => bytes[at] = *rng.pick(b"()\"{} N\r"),
            _ => bytes.insert(at, *rng.pick(b"()\"{} N\r")),
        }

        match body_structure(&bytes, false, 0) {
            Ok((rest, _)) => {
                assert!(
                    bytes.ends_with(rest),
                    "remaining input must be a suffix of the input"
                );
                assert!(
                    rest.len() < bytes.len(),
                    "a successful parse consumes at least the opening paren"
                );
            }
            Err(nom::Err::Error(_) | nom::Err::Failure(_)) => {}
            Err(other) => panic!("streaming error from a complete-mode parser: {other:?}"),
        }
    }
}

/// The generator has to be replayable, or a seed printed by a CI failure is
/// useless.
#[test]
fn the_bodystructure_generator_is_seed_deterministic() {
    let mut left = BodyRng(0xB0D1_0001);
    let mut right = BodyRng(0xB0D1_0001);
    for _ in 0..64 {
        let (a, b) = (
            generate_body_node(&mut left, 3),
            generate_body_node(&mut right, 3),
        );
        assert_eq!(a, b);
        let (mut wire_a, mut wire_b) = (String::new(), String::new());
        render_body_node(&mut left, &a, &mut wire_a);
        render_body_node(&mut right, &b, &mut wire_b);
        assert_eq!(wire_a, wire_b);
    }
}

/// The depth guard is what keeps the mutation pass above from being able to
/// blow the stack, so pin it directly: one level past
/// `MAX_BODY_NESTING_DEPTH` is a `Failure`, not a stack overflow.
#[test]
fn bodystructure_nesting_beyond_the_depth_guard_is_a_failure() {
    let leaf = "(\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 1 1)";
    let mut wire = leaf.to_owned();
    for _ in 0..80 {
        wire = format!("({wire} \"MIXED\")");
    }
    assert!(
        matches!(
            body_structure(wire.as_bytes(), false, 0),
            Err(nom::Err::Failure(_))
        ),
        "over-deep nesting must be rejected by the depth guard"
    );
}
