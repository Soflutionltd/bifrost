use super::*;

/// The complete truth table for the RFC 9051 Section 6.3.1 dual-mode rule,
/// against the single authority every view now delegates to.
///
/// Worth stating why this table and not a test that calls all four wrappers and
/// asserts they agree: once they delegate, such a test is tautological. It
/// cannot fail when someone re-inlines a CORRECT copy, and it fails only if a
/// wrapper is miswired - so it would pin the wiring on the day it was written
/// and nothing after. No behavioural test can detect a semantically identical
/// re-inlining; preventing future copies is a review property, not a runtime
/// one. What a test CAN do is pin the rule itself, so that a centralized
/// implementation which is wrong is caught however many wrappers delegate to
/// it. That is this table.
///
/// The rule used to be written out five times, over four different views, and
/// only two of the copies had any test. The three without were the ones that
/// decide wire bytes and SELECT validation.
#[test]
fn the_dual_mode_rule_truth_table() {
    let rev1 = || vec![Capability::Imap4Rev1];
    let rev2 = || vec![Capability::Imap4Rev2];
    let both = || vec![Capability::Imap4Rev1, Capability::Imap4Rev2];
    let enabled = |s: &str| vec![s.to_owned()];

    for (caps, en, expected, why) in [
        (Vec::new(), Vec::new(), false, "neither revision advertised"),
        (rev1(), Vec::new(), false, "rev1 only"),
        (rev2(), Vec::new(), true, "rev2 only is rev2 from the start"),
        (
            both(),
            Vec::new(),
            false,
            "dual-mode without ENABLE stays rev1",
        ),
        (
            both(),
            enabled("IMAP4rev2"),
            true,
            "dual-mode with ENABLE is rev2",
        ),
        (
            both(),
            enabled("imap4rev2"),
            true,
            "the ENABLE match is case-insensitive (RFC 9051 capability atoms)",
        ),
        (
            both(),
            enabled("CONDSTORE"),
            false,
            "an unrelated ENABLE does not activate rev2",
        ),
        (
            rev2(),
            enabled("IMAP4rev2"),
            true,
            "rev2-only stays rev2 when redundantly enabled",
        ),
        (
            rev1(),
            enabled("IMAP4rev2"),
            false,
            "ENABLE cannot conjure a revision the server never advertised",
        ),
    ] {
        assert_eq!(
            imap4rev2_active(&caps, &en),
            expected,
            "{why} (capabilities={caps:?}, enabled={en:?})"
        );
    }
}

/// Capability-by-capability, state-by-state matrix for the single authority,
/// against the first-principles oracle. Covers every `Capability` variant in
/// every rev2-ACTIVE state, both unadvertised and advertised, and checks that
/// `ServerProfile::supports` (a view over the authority) gives the same answer.
#[test]
fn supports_matrix_over_every_capability_and_state() {
    use capability_matrix::{connection_states, every_capability, expected_usable};

    for (state_caps, enabled) in connection_states() {
        for capability in every_capability() {
            let expected = expected_usable(&state_caps, &enabled, &capability);
            assert_eq!(
                supports(&state_caps, &enabled, &capability),
                expected,
                "unadvertised {capability:?} with capabilities={state_caps:?}, \
                 enabled={enabled:?}"
            );
            assert_eq!(
                ServerProfile::new(state_caps.clone(), enabled.clone())
                    .supports(capability.clone()),
                expected,
                "ServerProfile::supports({capability:?}) disagrees with the authority \
                 (capabilities={state_caps:?}, enabled={enabled:?})"
            );

            let mut advertised = state_caps.clone();
            advertised.push(capability.clone());
            assert!(
                supports(&advertised, &enabled, &capability),
                "an advertised {capability:?} is always usable (capabilities={advertised:?})"
            );
        }
    }
}

/// The negative half of the contract, stated directly: extensions a server may
/// advertise but which RFC 9051 did not fold into the base protocol get NO rev2
/// clause, so a pure rev2 connection must not treat them as usable.
#[test]
fn rev2_does_not_imply_extensions_outside_the_baseline() {
    let rev2 = vec![Capability::Imap4Rev2];
    for capability in [
        Capability::Condstore,
        Capability::QResync,
        Capability::Sort,
        Capability::Within,
        Capability::Preview,
        Capability::Notify,
        Capability::MultiAppend,
        Capability::CreateSpecialUse,
        Capability::Acl,
        Capability::Quota,
        Capability::QuotaSet,
        Capability::Metadata,
        Capability::MetadataServer,
        Capability::CompressDeflate,
        Capability::Id,
        Capability::Unauthenticate,
        Capability::XGmExt1,
    ] {
        assert!(
            !supports(&rev2, &[], &capability),
            "{capability:?} must not be implied by rev2"
        );
    }
}

/// RFC 7162 Section 3.2.3: QRESYNC implies CONDSTORE, and not the reverse.
#[test]
fn qresync_implies_condstore_but_not_the_reverse() {
    assert!(supports(
        &[Capability::QResync],
        &[],
        &Capability::Condstore
    ));
    assert!(!supports(
        &[Capability::Condstore],
        &[],
        &Capability::QResync
    ));

    let profile = ServerProfile::new(vec![Capability::QResync], vec![]);
    assert!(profile.supports(Capability::Condstore));
    assert!(profile.supports_condstore());
    assert!(profile.supports_qresync());

    let profile = ServerProfile::new(vec![Capability::Condstore], vec![]);
    assert!(profile.supports_condstore());
    assert!(!profile.supports_qresync());
}

#[test]
fn dual_rev_server_requires_enable_for_rev2_profile() {
    let profile = ServerProfile::new(vec![Capability::Imap4Rev1, Capability::Imap4Rev2], vec![]);
    assert!(!profile.imap4rev2);

    let profile = ServerProfile::new(
        vec![Capability::Imap4Rev1, Capability::Imap4Rev2],
        vec!["IMAP4rev2".to_owned()],
    );
    assert!(profile.imap4rev2);
    assert!(profile.supports(Capability::Move));
}

#[test]
fn imap4rev2_profile_implies_base_extensions() {
    let profile = ServerProfile::new(vec![Capability::Imap4Rev2], vec![]);
    let implied = [
        Capability::Enable,
        Capability::Esearch,
        Capability::Idle,
        Capability::ListExtended,
        Capability::ListStatus,
        Capability::LiteralMinus,
        Capability::Move,
        Capability::Namespace,
        Capability::SaslIr,
        Capability::SearchRes,
        Capability::SpecialUse,
        Capability::StatusDeleted,
        Capability::StatusSize,
        Capability::UidPlus,
        Capability::Unselect,
    ];

    for capability in implied {
        assert!(profile.supports(capability));
    }
}

/// RFC 9051 Appendix E folds LITERAL- into rev2, never LITERAL+: a pure rev2
/// server takes non-synchronizing literals up to 4096 octets only (RFC 9051
/// Section 4.3). The profile must not report unbounded LITERAL+ there, and
/// must still report it when the rev2 server advertises it.
///
/// Against the old baseline list, which contained LITERAL+, the first
/// assertion fails.
#[test]
fn imap4rev2_profile_does_not_imply_literal_plus() {
    let pure_rev2 = ServerProfile::new(vec![Capability::Imap4Rev2], vec![]);
    assert!(!pure_rev2.supports(Capability::LiteralPlus));
    assert!(pure_rev2.supports(Capability::LiteralMinus));

    let advertised =
        ServerProfile::new(vec![Capability::Imap4Rev2, Capability::LiteralPlus], vec![]);
    assert!(advertised.supports(Capability::LiteralPlus));
}

/// RFC 9051 defines nothing from OBJECTID (RFC 8474) or SAVEDATE (RFC 8514),
/// and folds in only the FETCH side of BINARY (Appendix B: not RFC 3516's
/// APPEND extension). A pure rev2 server that does not advertise these tokens
/// has not promised them; one that does, has.
///
/// Against the old baseline list, which contained all three, every
/// "not implied" assertion fails.
#[test]
fn imap4rev2_does_not_imply_objectid_savedate_or_the_binary_token() {
    let pure_rev2 = ServerProfile::new(vec![Capability::Imap4Rev2], vec![]);
    for capability in [
        Capability::ObjectId,
        Capability::SaveDate,
        Capability::Binary,
    ] {
        assert!(
            !pure_rev2.supports(capability.clone()),
            "{capability:?} must not be implied by rev2"
        );
        assert!(
            !supports(&[Capability::Imap4Rev2], &[], &capability),
            "{capability:?} must not be implied by rev2"
        );
        let advertised =
            ServerProfile::new(vec![Capability::Imap4Rev2, capability.clone()], vec![]);
        assert!(advertised.supports(capability));
    }
}

/// The FETCH half of BINARY is base rev2 (RFC 9051 Section 6.4.5, Appendix
/// B) even though the token is not implied: the question is asked separately.
#[test]
fn binary_fetch_items_are_usable_under_rev2_or_an_advertised_binary() {
    use capability_matrix::connection_states;

    for (caps, enabled) in connection_states() {
        assert_eq!(
            binary_fetch_usable(&caps, &enabled),
            imap4rev2_active(&caps, &enabled),
            "unadvertised BINARY: FETCH side follows active rev2 \
             (capabilities={caps:?}, enabled={enabled:?})"
        );
        let mut advertised = caps.clone();
        advertised.push(Capability::Binary);
        assert!(
            binary_fetch_usable(&advertised, &enabled),
            "an advertised BINARY always admits the FETCH items \
             (capabilities={advertised:?})"
        );
    }
}

#[test]
fn auth_mechanisms_are_case_insensitive() {
    let profile = ServerProfile::new(vec![Capability::from_imap_str("AUTH=plain")], vec![]);
    assert!(profile.supports_auth(AuthMechanism::Plain));
    assert!(profile.supports_sasl_auth(AuthMechanism::Plain));
    assert!(!profile.supports_auth(AuthMechanism::ScramSha256));
}

#[test]
fn scram_plus_does_not_satisfy_non_plus_scram() {
    let profile = ServerProfile::new(
        vec![Capability::from_imap_str("AUTH=SCRAM-SHA-256-PLUS")],
        vec![],
    );

    assert!(!profile.supports_sasl_auth(AuthMechanism::ScramSha256));
    // The PLUS advertisement is matched by its own variant.
    assert!(profile.supports_sasl_auth(AuthMechanism::ScramSha256Plus));
    assert_eq!(AuthMechanism::ScramSha256Plus.name(), "SCRAM-SHA-256-PLUS");
    assert_eq!(AuthMechanism::ScramSha1Plus.name(), "SCRAM-SHA-1-PLUS");
}

#[test]
fn login_command_is_not_sasl_auth_login() {
    let profile = ServerProfile::new(vec![], vec![]);
    assert!(profile.supports_auth(AuthMechanism::ImapLogin));
    assert!(profile.supports_login_command());
    assert!(!profile.supports_sasl_auth(AuthMechanism::ImapLogin));

    let profile = ServerProfile::new(vec![Capability::LoginDisabled], vec![]);
    assert!(!profile.supports_login_command());
    assert!(!profile.supports_auth(AuthMechanism::ImapLogin));
}

#[test]
fn append_limit_policy_is_explicit() {
    let profile = ServerProfile::new(vec![], vec![]);
    assert_eq!(profile.append_limit, AppendLimitPolicy::NotAdvertised);

    let profile = ServerProfile::new(vec![Capability::AppendLimit(None)], vec![]);
    assert_eq!(
        profile.append_limit,
        AppendLimitPolicy::PerMailbox {
            global_ceiling: None,
        },
    );

    let profile = ServerProfile::new(vec![Capability::AppendLimit(Some(1024))], vec![]);
    assert_eq!(profile.append_limit, AppendLimitPolicy::Limit(1024));

    let profile = ServerProfile::new(
        vec![
            Capability::AppendLimit(Some(2048)),
            Capability::AppendLimit(Some(1024)),
            Capability::AppendLimit(None),
        ],
        vec![],
    );
    assert_eq!(
        profile.append_limit,
        AppendLimitPolicy::PerMailbox {
            global_ceiling: Some(1024),
        },
    );
}

#[test]
fn thread_algorithms_are_normalized() {
    let profile = ServerProfile::new(vec![Capability::from_imap_str("THREAD=references")], vec![]);
    assert_eq!(profile.thread_algorithms, vec!["REFERENCES"]);
}

#[test]
fn enabled_checks_are_case_insensitive() {
    let profile = ServerProfile::new(vec![], vec!["qresync".to_owned()]);
    assert!(profile.enabled("QRESYNC"));
}
