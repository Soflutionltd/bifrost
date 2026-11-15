#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::connection::test_support::detached;
use crate::connection::{ImapConnection, SessionState};
use crate::error::Error;
use crate::types::{Capability, Command, FetchAttr};

fn conn(caps: Vec<Capability>) -> ImapConnection {
    detached(SessionState::Authenticated, caps, &[])
}

fn conn_enabled(caps: Vec<Capability>, enabled: &[&str]) -> ImapConnection {
    detached(SessionState::Authenticated, caps, enabled)
}

/// RFC 6855 Section 5: non-ASCII credentials require AUTHENTICATE. The check
/// runs before the command reaches the driver, and shares the encoder's
/// implementation so the two boundaries cannot drift.
#[tokio::test]
async fn login_rejects_non_ascii_before_submitting_to_the_driver() {
    let connection = detached(SessionState::NotAuthenticated, vec![], &[]);
    let error = connection
        .login("älice", "password", std::time::Duration::from_secs(1))
        .await
        .expect_err("non-ASCII LOGIN credentials must not reach the driver");
    assert!(
        matches!(error, Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(message)) if message.contains("ASCII-only"))
    );
}

/// X-GM-LABELS is a Gmail extension attribute like X-GM-MSGID / X-GM-THRID,
/// so requesting it without X-GM-EXT-1 is a client error, not a request the
/// server gets to reject.
#[test]
fn gmail_labels_fetch_requires_x_gm_ext_1() {
    let without = conn(vec![]);
    assert!(matches!(
        without.validate_requested_fetch_items(&[FetchAttr::GmailLabels]),
        Err(Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(failure))) if failure.capability == Capability::XGmExt1
    ));
    let with = conn(vec![Capability::XGmExt1]);
    assert!(
        with.validate_requested_fetch_items(&[FetchAttr::GmailLabels])
            .is_ok()
    );
}

// ---------------------------------------------------------------------------
// status_item_tokens (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)
// ---------------------------------------------------------------------------

#[test]
fn status_tokens_accept_bare_list() {
    let tokens = ImapConnection::status_item_tokens("MESSAGES UNSEEN").unwrap();
    assert_eq!(tokens, vec!["MESSAGES", "UNSEEN"]);
}

#[test]
fn status_tokens_accept_parenthesized_list() {
    let tokens = ImapConnection::status_item_tokens("(MESSAGES  UIDNEXT)").unwrap();
    assert_eq!(tokens, vec!["MESSAGES", "UIDNEXT"]);
}

#[test]
fn status_tokens_trim_surrounding_whitespace() {
    let tokens = ImapConnection::status_item_tokens("  ( MESSAGES ) ").unwrap();
    assert_eq!(tokens, vec!["MESSAGES"]);
}

#[test]
fn status_tokens_reject_empty() {
    assert!(ImapConnection::status_item_tokens("").is_err());
    assert!(ImapConnection::status_item_tokens("   ").is_err());
    assert!(ImapConnection::status_item_tokens("()").is_err());
    assert!(ImapConnection::status_item_tokens("(   )").is_err());
}

#[test]
fn status_tokens_reject_unbalanced_parentheses() {
    assert!(ImapConnection::status_item_tokens("(MESSAGES").is_err());
    assert!(ImapConnection::status_item_tokens("MESSAGES)").is_err());
}

#[test]
fn status_tokens_reject_nested_parentheses() {
    assert!(ImapConnection::status_item_tokens("(MESSAGES (UNSEEN))").is_err());
    assert!(ImapConnection::status_item_tokens("MESSAGES (UNSEEN)").is_err());
}

// ---------------------------------------------------------------------------
// list_status_return_option_items (RFC 5819 Section 2)
// ---------------------------------------------------------------------------

/// RFC 5819 Section 2: the reserved return option is `STATUS SP "("
/// status-att-list ")"`, so the extracted body is exactly the item list. The
/// items must survive intact or the capability gate downstream
/// (`validate_requested_status_items`) stops recognizing the names it gates:
/// a truncated `RECENT` would pass on an IMAP4rev2 connection and a truncated
/// `HIGHESTMODSEQ` would pass without CONDSTORE.
#[test]
fn list_status_option_extracts_the_whole_item_list() {
    let items = ImapConnection::list_status_return_option_items("STATUS (MESSAGES UNSEEN)")
        .expect("recognized as STATUS")
        .unwrap();
    assert_eq!(items, "MESSAGES UNSEEN");

    let single = ImapConnection::list_status_return_option_items("STATUS (RECENT)")
        .expect("recognized as STATUS")
        .unwrap();
    assert_eq!(single, "RECENT");
}

#[test]
fn list_status_option_keyword_is_case_insensitive() {
    let items = ImapConnection::list_status_return_option_items("status (MESSAGES)")
        .expect("recognized as STATUS")
        .unwrap();
    assert_eq!(items, "MESSAGES");
}

#[test]
fn list_status_option_ignores_longer_atoms() {
    // RFC 5258 option-extension: STATUSX is a different option, not
    // LIST-STATUS with a malformed argument.
    assert!(
        ImapConnection::list_status_return_option_items("STATUSX").is_none(),
        "STATUSX must stay a generic option-extension"
    );
    assert!(ImapConnection::list_status_return_option_items("CHILDREN").is_none());
}

#[test]
fn list_status_option_rejects_missing_parentheses() {
    assert!(
        ImapConnection::list_status_return_option_items("STATUS MESSAGES")
            .expect("recognized as STATUS")
            .is_err()
    );
    assert!(
        ImapConnection::list_status_return_option_items("STATUS")
            .expect("recognized as STATUS")
            .is_err()
    );
    assert!(
        ImapConnection::list_status_return_option_items("STATUS (MESSAGES")
            .expect("recognized as STATUS")
            .is_err()
    );
}

// ---------------------------------------------------------------------------
// quota_resource_name (RFC 9208 Section 3.1.1)
// ---------------------------------------------------------------------------

#[test]
fn quota_resource_name_from_typed_capability() {
    let cap = Capability::from_imap_str("QUOTA=RES-STORAGE");
    assert_eq!(ImapConnection::quota_resource_name(&cap), Some("STORAGE"));
}

#[test]
fn quota_resource_name_uses_capability_parser() {
    let cap = Capability::from_imap_str("QUOTA=RES-MESSAGE");
    assert_eq!(ImapConnection::quota_resource_name(&cap), Some("MESSAGE"));
    let lower = Capability::from_imap_str("quota=res-message");
    // The parser stores `QUOTA=RES-` suffixes uppercase.
    assert_eq!(ImapConnection::quota_resource_name(&lower), Some("MESSAGE"));
}

#[test]
fn quota_resource_name_rejects_bare_prefix_and_others() {
    // Exactly `QUOTA=RES-` names no resource.
    let bare = Capability::from_imap_str("QUOTA=RES-");
    assert_eq!(ImapConnection::quota_resource_name(&bare), None);
    assert_eq!(
        ImapConnection::quota_resource_name(&Capability::Quota),
        None
    );
}

#[test]
fn has_quota_resource_matches_case_insensitively() {
    let c = conn(vec![Capability::from_imap_str("QUOTA=RES-STORAGE")]);
    assert!(c.has_quota_resource("storage"));
    assert!(!c.has_quota_resource("MESSAGE"));
}

// ---------------------------------------------------------------------------
// search_return_requests_save (RFC 5182 Section 2)
// ---------------------------------------------------------------------------

#[test]
fn search_return_save_detected_on_both_forms() {
    let cmd = Command::SearchReturn {
        criteria: "ALL".to_owned().into(),
        return_opts: vec![" save ".into()],
    };
    assert!(ImapConnection::search_return_requests_save(&cmd));

    let uid_cmd = Command::UidSearchReturn {
        criteria: "ALL".to_owned().into(),
        return_opts: vec!["MIN".into(), "SAVE".into()],
    };
    assert!(ImapConnection::search_return_requests_save(&uid_cmd));
}

#[test]
fn search_return_save_absent_and_on_other_commands() {
    let cmd = Command::SearchReturn {
        criteria: "ALL".to_owned().into(),
        return_opts: vec!["COUNT".into()],
    };
    assert!(!ImapConnection::search_return_requests_save(&cmd));
    assert!(!ImapConnection::search_return_requests_save(&Command::Noop));
}

// ---------------------------------------------------------------------------
// Capability gates
// ---------------------------------------------------------------------------

#[test]
fn require_condstore_accepts_either_capability() {
    assert!(
        conn(vec![Capability::Condstore])
            .require_condstore()
            .is_ok()
    );
    // RFC 7162 Section 3.2.3: QRESYNC implies CONDSTORE.
    assert!(conn(vec![Capability::QResync]).require_condstore().is_ok());
    assert!(matches!(
        conn(vec![]).require_condstore(),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
}

/// The handle-side gate helper is a view over the single capability authority:
/// for every capability in every rev2-ACTIVE state it answers what the oracle
/// does, so a capability added to the rev2 baseline reaches every handle gate.
#[test]
fn snapshot_supports_matches_the_authority() {
    use crate::types::profile::capability_matrix::{
        connection_states, every_capability, expected_usable,
    };

    for (capabilities, enabled) in connection_states() {
        let snap = crate::connection::driver::ConnectionStateSnapshot {
            session_state: SessionState::Authenticated,
            session: crate::types::profile::SessionCapabilities::new(
                capabilities.clone(),
                enabled.clone(),
            ),
            mailbox_selected: false,
            selected: None,
        };
        for capability in every_capability() {
            assert_eq!(
                crate::connection::auth::snapshot_supports(&snap, &capability),
                expected_usable(&capabilities, &enabled, &capability),
                "{capability:?} with capabilities={capabilities:?}, enabled={enabled:?}"
            );
        }
    }
}

#[test]
fn require_searchres_is_implied_by_rev2() {
    assert!(
        conn(vec![Capability::SearchRes])
            .require_searchres()
            .is_ok()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .require_searchres()
            .is_ok()
    );
    assert!(matches!(
        conn(vec![Capability::Imap4Rev1]).require_searchres(),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
}

#[test]
fn dual_mode_rev2_requires_explicit_enable() {
    // RFC 9051 Section 6.3.1: rev1+rev2 advertised means rev2 behavior is
    // off until ENABLE IMAP4rev2.
    let dual = conn(vec![Capability::Imap4Rev1, Capability::Imap4Rev2]);
    assert!(!dual.is_rev2());
    let enabled = conn_enabled(
        vec![Capability::Imap4Rev1, Capability::Imap4Rev2],
        &["imap4rev2"],
    );
    assert!(enabled.is_rev2(), "ENABLE match is case-insensitive");
}

#[test]
fn require_state_gates_on_session_state() {
    use crate::types::CommandKind;

    let c = detached(SessionState::Authenticated, vec![], &[]);
    assert!(c.require_state(CommandKind::Enable).is_ok());
    assert!(
        matches!(
            c.require_state(CommandKind::Fetch),
            Err(Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidState(_)
            ))
        ),
        "Authenticated must not satisfy a Selected-only command"
    );
}

#[test]
fn utf8_only_gate_is_released_by_enable_or_rev2() {
    let blocked = conn(vec![Capability::Utf8Only]);
    assert!(blocked.check_utf8_only_enforced().is_err());

    let enabled = conn_enabled(vec![Capability::Utf8Only], &["UTF8=ACCEPT"]);
    assert!(enabled.check_utf8_only_enforced().is_ok());

    // RFC 9051 Appendix A: pure rev2 is already UTF-8 capable.
    let rev2 = conn(vec![Capability::Utf8Only, Capability::Imap4Rev2]);
    assert!(rev2.check_utf8_only_enforced().is_ok());

    assert!(conn(vec![]).check_utf8_only_enforced().is_ok());
}

// ---------------------------------------------------------------------------
// STATUS item validation
// ---------------------------------------------------------------------------

#[test]
fn status_recent_is_rejected_on_rev2() {
    let rev1 = conn(vec![Capability::Imap4Rev1]);
    assert!(rev1.validate_requested_status_items("RECENT").is_ok());
    let rev2 = conn(vec![Capability::Imap4Rev2]);
    assert!(rev2.validate_requested_status_items("RECENT").is_err());
}

#[test]
fn status_deleted_requires_rev2_or_quota_res_message() {
    assert!(
        conn(vec![Capability::Imap4Rev1])
            .validate_requested_status_items("DELETED")
            .is_err()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_status_items("DELETED")
            .is_ok()
    );
    assert!(
        conn(vec![Capability::from_imap_str("QUOTA=RES-MESSAGE")])
            .validate_requested_status_items("DELETED")
            .is_ok()
    );
    // No RFC defines a `STATUS=DELETED` capability, so advertising that
    // token on rev1 opens no route to the item: the gate is rev2 or
    // QUOTA=RES-MESSAGE, not the capability authority.
    assert!(
        conn(vec![Capability::Imap4Rev1, Capability::StatusDeleted])
            .validate_requested_status_items("DELETED")
            .is_err()
    );
}

#[test]
fn status_deleted_storage_requires_quota_res_storage() {
    assert!(matches!(
        conn(vec![Capability::Imap4Rev2]).validate_requested_status_items("DELETED-STORAGE"),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
    assert!(
        conn(vec![Capability::from_imap_str("QUOTA=RES-STORAGE")])
            .validate_requested_status_items("DELETED-STORAGE")
            .is_ok()
    );
}

#[test]
fn status_size_requires_status_size_or_rev2() {
    assert!(
        conn(vec![Capability::Imap4Rev1])
            .validate_requested_status_items("SIZE")
            .is_err()
    );
    assert!(
        conn(vec![Capability::StatusSize])
            .validate_requested_status_items("SIZE")
            .is_ok()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_status_items("SIZE")
            .is_ok()
    );
}

#[test]
fn status_extension_items_are_gated() {
    assert!(
        conn(vec![])
            .validate_requested_status_items("HIGHESTMODSEQ")
            .is_err()
    );
    assert!(
        conn(vec![Capability::Condstore])
            .validate_requested_status_items("HIGHESTMODSEQ")
            .is_ok()
    );
    assert!(
        conn(vec![])
            .validate_requested_status_items("APPENDLIMIT")
            .is_err()
    );
    assert!(
        conn(vec![Capability::AppendLimit(None)])
            .validate_requested_status_items("APPENDLIMIT")
            .is_ok()
    );
    assert!(
        conn(vec![])
            .validate_requested_status_items("MAILBOXID")
            .is_err()
    );
    assert!(
        conn(vec![Capability::ObjectId])
            .validate_requested_status_items("MAILBOXID")
            .is_ok()
    );
}

#[test]
fn status_items_are_matched_case_insensitively() {
    let rev2 = conn(vec![Capability::Imap4Rev2]);
    assert!(rev2.validate_requested_status_items("recent").is_err());
    assert!(
        rev2.validate_requested_status_items("(MESSAGES uidnext)")
            .is_ok()
    );
}

// ---------------------------------------------------------------------------
// FETCH item validation
// ---------------------------------------------------------------------------

#[test]
fn fetch_modseq_requires_condstore_and_is_not_implied_by_rev2() {
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_fetch_items(&[FetchAttr::ModSeq])
            .is_err(),
        "CONDSTORE is not part of the rev2 baseline"
    );
    assert!(
        conn(vec![Capability::QResync])
            .validate_requested_fetch_items(&[FetchAttr::ModSeq])
            .is_ok()
    );
}

/// RFC 9051 folds in nothing from OBJECTID (RFC 8474) or SAVEDATE (RFC
/// 8514): their FETCH items and STATUS item need the advertised token on rev2
/// exactly as on rev1.
#[test]
fn objectid_and_savedate_items_are_not_implied_by_rev2() {
    for item in [FetchAttr::EmailId, FetchAttr::ThreadId, FetchAttr::SaveDate] {
        for caps in [vec![Capability::Imap4Rev1], vec![Capability::Imap4Rev2]] {
            assert!(
                matches!(
                    conn(caps.clone()).validate_requested_fetch_items(std::slice::from_ref(&item)),
                    Err(Error::LocalRefusal(
                        crate::error::LocalRefusal::MissingCapability(_)
                    ))
                ),
                "{item:?} must need its capability on {caps:?}"
            );
        }
    }
    assert!(matches!(
        conn(vec![Capability::Imap4Rev2]).validate_requested_status_items("MAILBOXID"),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));

    let objectid = conn(vec![Capability::Imap4Rev2, Capability::ObjectId]);
    assert!(
        objectid
            .validate_requested_fetch_items(&[FetchAttr::EmailId, FetchAttr::ThreadId])
            .is_ok()
    );
    assert!(
        objectid
            .validate_requested_status_items("MAILBOXID")
            .is_ok()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2, Capability::SaveDate])
            .validate_requested_fetch_items(&[FetchAttr::SaveDate])
            .is_ok()
    );
}

/// RFC 9051 Appendix B: the BINARY FETCH items are base rev2 even though the
/// BINARY token (which also covers RFC 3516's APPEND side) is not implied.
#[test]
fn fetch_binary_items_are_implied_by_rev2() {
    let item = FetchAttr::BinarySize { section: vec![1] };
    assert!(
        conn(vec![Capability::Imap4Rev1])
            .validate_requested_fetch_items(std::slice::from_ref(&item))
            .is_err()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_fetch_items(std::slice::from_ref(&item))
            .is_ok()
    );
    assert!(
        conn(vec![Capability::Binary])
            .validate_requested_fetch_items(std::slice::from_ref(&item))
            .is_ok()
    );
}

#[test]
fn fetch_gmail_items_require_x_gm_ext_1() {
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_fetch_items(&[FetchAttr::GmailMsgId])
            .is_err()
    );
    assert!(
        conn(vec![Capability::XGmExt1])
            .validate_requested_fetch_items(&[FetchAttr::GmailThreadId])
            .is_ok()
    );
}

#[test]
fn fetch_base_items_need_no_capability() {
    assert!(
        conn(vec![])
            .validate_requested_fetch_items(&[FetchAttr::Uid, FetchAttr::Flags])
            .is_ok()
    );
}

// ---------------------------------------------------------------------------
// LIST-EXTENDED request validation (RFC 5258 Section 3)
// ---------------------------------------------------------------------------

#[test]
fn list_extended_requires_at_least_one_pattern() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&[], &[], &[]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
}

#[test]
fn list_extended_multiple_patterns_need_the_capability() {
    let plain = conn(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        plain.validate_list_extended_request(&["a", "b"], &[], &[]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
    assert!(
        plain
            .validate_list_extended_request(&["a"], &[], &[])
            .is_ok()
    );
    let rev2 = conn(vec![Capability::Imap4Rev2]);
    assert!(
        rev2.validate_list_extended_request(&["a", "b"], &[], &[])
            .is_ok()
    );
}

#[test]
fn list_extended_options_need_the_capability() {
    let plain = conn(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        plain.validate_list_extended_request(&["*"], &["SUBSCRIBED"], &[]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
    assert!(matches!(
        plain.validate_list_extended_request(&["*"], &[], &["CHILDREN"]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
}

#[test]
fn list_extended_rejects_empty_option_strings() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["  "], &[]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &[], &["  "]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
}

#[test]
fn list_extended_special_use_option_is_gated() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["SPECIAL-USE"], &[]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
    let with = conn(vec![Capability::ListExtended, Capability::SpecialUse]);
    assert!(
        with.validate_list_extended_request(&["*"], &["SPECIAL-USE"], &[])
            .is_ok()
    );
}

#[test]
fn list_extended_status_return_option_is_gated_by_list_status() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &[], &["STATUS (MESSAGES)"]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::MissingCapability(_)
        ))
    ));
    let with = conn(vec![Capability::ListExtended, Capability::ListStatus]);
    assert!(
        with.validate_list_extended_request(&["*"], &[], &["STATUS (MESSAGES)"])
            .is_ok()
    );
    // The nested STATUS items go through the same item validation.
    assert!(
        with.validate_list_extended_request(&["*"], &[], &["STATUS (BOGUS-ITEM)"])
            .is_ok(),
        "unknown items are passed through to the server"
    );
    assert!(
        with.validate_list_extended_request(&["*"], &[], &["STATUS ()"])
            .is_err()
    );
}

#[test]
fn list_extended_recursivematch_needs_a_partner_option() {
    // RFC 5258 Section 3: RECURSIVEMATCH alone (or with only REMOTE) is
    // a syntax error.
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["RECURSIVEMATCH"], &[]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["RECURSIVEMATCH", "REMOTE"], &[]),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(_)
        ))
    ));
    assert!(
        c.validate_list_extended_request(&["*"], &["RECURSIVEMATCH", "SUBSCRIBED"], &[])
            .is_ok()
    );
}
