//! Exhaustiveness tests for [`super::classify`].
//!
//! Invariant I5: every `(CommandKind, UntaggedResponse)` pair that can
//! appear per RFC 3501 has an explicit row in `classify`. Missing rows
//! are caught by a `debug_assert!(false, ...)` in the default arm, which
//! panics in debug builds. This test enumerates all RFC 3501 pairs and
//! asserts none of them trigger that panic.

#![allow(clippy::unwrap_used)]

use std::panic::AssertUnwindSafe;

use crate::connection::NotifyFlags;
use crate::types::CommandKind as CK;
use crate::types::response::{EsearchResponse, UntaggedResponse as UR, UntaggedStatus};
use crate::types::validated::MailboxName;

use super::{ClassificationContext, SolicitationRule, classify};

/// All RFC 3501 command kinds (Section6.1-Section6.4).
fn all_rfc3501_command_kinds() -> Vec<CK> {
    vec![
        // Section6.1 Any state
        CK::Capability,
        CK::Noop,
        CK::Logout,
        // Section6.2 Not authenticated
        CK::Login,
        CK::Authenticate,
        CK::StartTls,
        // Section6.3 Authenticated
        CK::Select,
        CK::Examine,
        CK::Create,
        CK::Delete,
        CK::Rename,
        CK::Subscribe,
        CK::Unsubscribe,
        CK::List,
        CK::Lsub,
        CK::Status,
        CK::Append,
        // Section6.4 Selected
        CK::Check,
        CK::Close,
        CK::Expunge,
        CK::Search,
        CK::Fetch,
        CK::Store,
        CK::Copy,
    ]
}

/// Extension command kinds not in RFC 3501.
fn all_extension_command_kinds() -> Vec<CK> {
    vec![
        CK::Idle,           // RFC 2177
        CK::Id,             // RFC 2971
        CK::Namespace,      // RFC 2342
        CK::GetMetadata,    // RFC 5464
        CK::SetMetadata,    // RFC 5464
        CK::Thread,         // RFC 5256
        CK::Sort,           // RFC 5256
        CK::NotifySet,      // RFC 5465
        CK::NotifyNone,     // RFC 5465
        CK::Compress,       // RFC 4978
        CK::GetQuota,       // RFC 2087 / RFC 9208
        CK::GetQuotaRoot,   // RFC 2087 / RFC 9208
        CK::SetQuota,       // RFC 2087 / RFC 9208
        CK::SetAcl,         // RFC 4314
        CK::DeleteAcl,      // RFC 4314
        CK::GetAcl,         // RFC 4314
        CK::ListRights,     // RFC 4314
        CK::MyRights,       // RFC 4314
        CK::ListStatus,     // RFC 5819
        CK::Move,           // RFC 6851
        CK::SearchReturn,   // RFC 4731
        CK::SearchSave,     // RFC 5182
        CK::Enable,         // RFC 5161
        CK::Unselect,       // RFC 3691
        CK::Unauthenticate, // RFC 8437
    ]
}

/// Every `CommandKind` variant  -  RFC 3501 + extensions.
fn all_command_kinds() -> Vec<CK> {
    let mut all = all_rfc3501_command_kinds();
    all.extend(all_extension_command_kinds());
    all
}

/// One representative instance of every RFC 3501 `UntaggedResponse`
/// variant (Section7.1-Section7.4).
///
/// Payloads are dummy values; the test only inspects discriminants.
fn all_rfc3501_untagged_variants() -> Vec<UR> {
    let mailbox = MailboxName::new("INBOX").unwrap();

    vec![
        // Section7.1  -  Status responses (OK/NO/BAD/BYE)
        UR::Status {
            status: UntaggedStatus::Ok,
            code: None,
            text: String::new(),
        },
        // Section7.2.1  -  CAPABILITY
        UR::Capability(vec![]),
        // Section7.2.2  -  LIST
        UR::List(crate::types::mailbox::MailboxInfo::default()),
        // Section7.2.3  -  LSUB
        UR::Lsub(crate::types::mailbox::MailboxInfo::default()),
        // Section7.2.4  -  STATUS
        UR::MailboxStatus {
            mailbox,
            items: vec![],
        },
        // Section7.2.5  -  SEARCH
        UR::Search {
            uids: vec![],
            mod_seq: None,
        },
        // Section7.2.6  -  FLAGS
        UR::Flags(vec![]),
        // Section7.3.1  -  EXISTS
        UR::Exists(1),
        // Section7.3.2  -  RECENT
        UR::Recent(0),
        // Section7.4.1  -  EXPUNGE
        UR::Expunge(1),
        // Section7.4.2  -  FETCH
        UR::Fetch(Box::default()),
    ]
}

/// Extension-only `UntaggedResponse` variants.
///
/// Payloads are dummy values; the test only inspects discriminants.
/// For `Vanished`, both `earlier: true` and `earlier: false` are
/// tested because `classify` branches on that flag.
fn all_extension_untagged_variants() -> Vec<UR> {
    let mailbox = MailboxName::new("INBOX").unwrap();

    vec![
        // RFC 4731 Section3.1  -  ESEARCH
        UR::Esearch(EsearchResponse::default()),
        // RFC 5161 Section3.2  -  ENABLED
        UR::Enabled(vec![]),
        // RFC 7162 Section3.2.10  -  VANISHED (EARLIER)
        UR::Vanished {
            earlier: true,
            uids: vec![],
        },
        // RFC 7162 Section3.2.10  -  VANISHED (non-EARLIER)
        UR::Vanished {
            earlier: false,
            uids: vec![],
        },
        // RFC 2971 Section3.2  -  ID
        UR::Id(vec![]),
        // RFC 2342 Section5  -  NAMESPACE
        UR::Namespace {
            personal: vec![],
            other: vec![],
            shared: vec![],
        },
        // RFC 2087 Section5.1 / RFC 9208  -  QUOTA
        UR::Quota {
            root: String::new(),
            resources: vec![],
        },
        // RFC 2087 Section5.2 / RFC 9208  -  QUOTAROOT
        UR::QuotaRoot {
            mailbox: mailbox.clone(),
            roots: vec![],
        },
        // RFC 4314 Section3.6  -  ACL
        UR::Acl {
            mailbox: mailbox.clone(),
            entries: vec![],
        },
        // RFC 4314 Section3.8  -  MYRIGHTS
        UR::MyRights {
            mailbox: mailbox.clone(),
            rights: String::new(),
        },
        // RFC 4314 Section3.7  -  LISTRIGHTS
        UR::ListRights {
            mailbox: mailbox.clone(),
            identifier: String::new(),
            required: String::new(),
            optional: vec![],
        },
        // RFC 5464 Section4.4  -  METADATA
        UR::Metadata {
            mailbox,
            entries: vec![],
        },
        // RFC 5256 Section4  -  THREAD
        UR::Thread(vec![]),
        // RFC 5256 Section4  -  SORT
        UR::Sort {
            nums: vec![],
            mod_seq: None,
        },
        // RFC 9051 Section2.2.2  -  Unknown
        UR::Unknown(String::new()),
    ]
}

/// Every `UntaggedResponse` variant  -  RFC 3501 + extensions.
fn all_untagged_variants() -> Vec<UR> {
    let mut all = all_rfc3501_untagged_variants();
    all.extend(all_extension_untagged_variants());
    all
}

/// Invariant I5  -  classify is exhaustive for all RFC 3501 (command, response) pairs.
///
/// For every RFC 3501 `CommandKind` and every RFC 3501 `UntaggedResponse`
/// variant, `classify` must reach an explicit match arm  -  not the default
/// `debug_assert!(false, ...)` catch-all. In debug builds the default arm
/// panics; this test catches that panic via `catch_unwind` and reports the
/// missing pair.
///
/// Note: the STATUS command's match arm contains `debug_assert!(ctx.command_target.is_some())`
/// so we set `command_target` to a dummy mailbox matching the `MailboxStatus`
/// variant's name. This does not weaken the test  -  the goal is to verify
/// row coverage, not ctx-dependent routing.
#[test]
fn invariant_i5_classify_exhaustive_rfc3501() {
    let target = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: Some(&target),
    };

    for cmd in all_rfc3501_command_kinds() {
        for resp in all_rfc3501_untagged_variants() {
            let cmd_copy = cmd;
            let result =
                std::panic::catch_unwind(AssertUnwindSafe(|| classify(cmd_copy, &resp, &ctx)));
            assert!(
                result.is_ok(),
                "classify has no row for ({cmd:?}, {:?})",
                std::mem::discriminant(&resp)
            );
        }
    }
}

#[test]
fn status_correlation_distinguishes_noncanonical_wire_names() {
    let target = MailboxName::new("a").unwrap();
    let collision = MailboxName::from_wire("a".into(), b"&AGE-", false);
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: Some(&target),
    };

    assert!(matches!(
        classify(
            CK::Status,
            &UR::MailboxStatus {
                mailbox: collision,
                items: vec![],
            },
            &ctx,
        ),
        SolicitationRule::OnlyUnsolicited
    ));
    assert!(matches!(
        classify(
            CK::Status,
            &UR::MailboxStatus {
                mailbox: target.clone(),
                items: vec![],
            },
            &ctx,
        ),
        SolicitationRule::OnlySolicited
    ));
}

#[test]
fn inbox_case_folding_does_not_override_noncanonical_wire_identity() {
    let target = MailboxName::new("INBOX").unwrap();
    let lowercase = MailboxName::new("inbox").unwrap();
    let noncanonical = MailboxName::from_wire("INBOX".into(), b"&AEkATgBCAE8AWA-", false);
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: Some(&target),
    };

    assert!(matches!(
        classify(
            CK::Status,
            &UR::MailboxStatus {
                mailbox: lowercase,
                items: vec![],
            },
            &ctx,
        ),
        SolicitationRule::OnlySolicited
    ));
    assert!(matches!(
        classify(
            CK::Status,
            &UR::MailboxStatus {
                mailbox: noncanonical,
                items: vec![],
            },
            &ctx,
        ),
        SolicitationRule::OnlyUnsolicited
    ));
}

/// Invariant I5  -  classify is exhaustive for all (command, response) pairs
/// including extensions.
///
/// Tests every `CommandKind` (RFC 3501 + extensions) against every
/// `UntaggedResponse` variant (RFC 3501 + extensions). Two NOTIFY
/// contexts are tested: one with no NOTIFY flags (default) and one
/// with all NOTIFY flags set, because NOTIFY flags change the
/// classification of LIST, STATUS, and METADATA responses.
#[test]
fn invariant_i5_classify_exhaustive_extensions() {
    let target = MailboxName::new("INBOX").unwrap();

    // Context without NOTIFY flags  -  the common case.
    let ctx_no_notify = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: Some(&target),
    };

    // Context with all NOTIFY flags set  -  exercises the NOTIFY guard arms.
    let ctx_with_notify = ClassificationContext {
        notify: NotifyFlags {
            list: true,
            status: true,
            metadata: true,
        },
        command_target: Some(&target),
    };

    for ctx in [&ctx_no_notify, &ctx_with_notify] {
        for cmd in all_command_kinds() {
            for resp in all_untagged_variants() {
                let cmd_copy = cmd;
                let result =
                    std::panic::catch_unwind(AssertUnwindSafe(|| classify(cmd_copy, &resp, ctx)));
                assert!(
                    result.is_ok(),
                    "classify has no row for ({cmd:?}, {:?}) notify={:?}",
                    std::mem::discriminant(&resp),
                    ctx.notify
                );
            }
        }
    }
}

/// `CommandKind::untagged_routing_key` is a summary of this classifier: two
/// pipelinable kinds with DISTINCT keys may share a pipelined sub-batch, so
/// no untagged response may be `OnlySolicited` for both, under any NOTIFY
/// registration. Otherwise head routing hands one command's answer to the
/// other.
///
/// A new `OnlySolicited` row shared by two kinds fails this until the key
/// groups them. Mapping every kind to itself fails it on QUOTA, SEARCH,
/// ESEARCH, LIST, STATUS and VANISHED (EARLIER).
#[test]
fn distinct_routing_keys_never_share_a_solicited_response() {
    let target = MailboxName::new("INBOX").unwrap();
    let contexts = [
        NotifyFlags::default(),
        NotifyFlags {
            list: true,
            status: true,
            metadata: true,
        },
    ];
    let pipelinable: Vec<CK> = all_command_kinds()
        .into_iter()
        .filter(|kind| kind.pipelinable())
        .collect();

    for notify in contexts {
        let ctx = ClassificationContext {
            notify,
            command_target: Some(&target),
        };
        for (i, &a) in pipelinable.iter().enumerate() {
            for &b in &pipelinable[i + 1..] {
                if a.untagged_routing_key() == b.untagged_routing_key() {
                    continue;
                }
                for resp in all_untagged_variants() {
                    let both = matches!(classify(a, &resp, &ctx), SolicitationRule::OnlySolicited)
                        && matches!(classify(b, &resp, &ctx), SolicitationRule::OnlySolicited);
                    assert!(
                        !both,
                        "{a:?} and {b:?} have distinct routing keys but both solicit {:?} \
                         (notify={notify:?})",
                        std::mem::discriminant(&resp)
                    );
                }
            }
        }
    }
}

// ---- NOTIFY classification correctness tests (RFC 5465) ----
//
// The exhaustiveness tests above verify that every (command, response)
// pair has a row. These tests verify that the NOTIFY-guarded rows
// return the *correct* SolicitationRule variant  -  OnlyUnsolicited when
// the flag is set, Impossible when unset, and OnlySolicited for the
// originating command regardless of flag.

/// RFC 5465 Section5.4-5.5: LIST responses during a non-LIST command are
/// `OnlyUnsolicited` when `ctx.notify.list` is set.
#[test]
fn notify_list_flag_classifies_unsolicited_list() {
    let ctx = ClassificationContext {
        notify: NotifyFlags {
            list: true,
            status: false,
            metadata: false,
        },
        command_target: None,
    };
    let resp = UR::List(crate::types::mailbox::MailboxInfo::default());
    let result = classify(CK::Noop, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlyUnsolicited),
        "LIST during NOOP with notify.list=true should be OnlyUnsolicited, got {result:?}"
    );
}

/// Without NOTIFY list events, LIST during a non-LIST/non-SELECT command
/// is Impossible per RFC 3501.
#[test]
fn no_notify_list_flag_classifies_unsolicited_list_as_impossible() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::List(crate::types::mailbox::MailboxInfo::default());
    let result = classify(CK::Noop, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::Impossible),
        "LIST during NOOP with notify.list=false should be Impossible, got {result:?}"
    );
}

/// LIST during a LIST command is `OnlySolicited` regardless of NOTIFY flags.
#[test]
fn notify_list_flag_does_not_affect_solicited_list() {
    let ctx = ClassificationContext {
        notify: NotifyFlags {
            list: true,
            status: false,
            metadata: false,
        },
        command_target: None,
    };
    let resp = UR::List(crate::types::mailbox::MailboxInfo::default());
    let result = classify(CK::List, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "LIST during LIST with notify.list=true should still be OnlySolicited, got {result:?}"
    );
}

/// RFC 5465 Section5.1-5.2: STATUS responses during a non-STATUS command are
/// `OnlyUnsolicited` when `ctx.notify.status` is set.
#[test]
fn notify_status_flag_classifies_unsolicited_status() {
    let mailbox = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags {
            list: false,
            status: true,
            metadata: false,
        },
        command_target: None,
    };
    let resp = UR::MailboxStatus {
        mailbox,
        items: vec![],
    };
    let result = classify(CK::Noop, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlyUnsolicited),
        "STATUS during NOOP with notify.status=true should be OnlyUnsolicited, got {result:?}"
    );
}

/// Without NOTIFY status events, STATUS during a non-STATUS command
/// is Impossible per RFC 3501 Section7.2.4.
#[test]
fn no_notify_status_flag_classifies_unsolicited_status_as_impossible() {
    let mailbox = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::MailboxStatus {
        mailbox,
        items: vec![],
    };
    let result = classify(CK::Noop, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::Impossible),
        "STATUS during NOOP with notify.status=false should be Impossible, got {result:?}"
    );
}

/// STATUS during a STATUS command with matching target is `OnlySolicited`
/// regardless of NOTIFY flags.
#[test]
fn notify_status_flag_does_not_affect_solicited_status() {
    let mailbox = MailboxName::new("INBOX").unwrap();
    let target = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags {
            list: false,
            status: true,
            metadata: false,
        },
        command_target: Some(&target),
    };
    let resp = UR::MailboxStatus {
        mailbox,
        items: vec![],
    };
    let result = classify(CK::Status, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "STATUS during STATUS with notify.status=true should still be OnlySolicited, got {result:?}"
    );
}

/// RFC 5465 Section5.6-5.7: METADATA responses during a non-GETMETADATA
/// command are `OnlyUnsolicited` when `ctx.notify.metadata` is set.
#[test]
fn notify_metadata_flag_classifies_unsolicited_metadata() {
    let mailbox = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags {
            list: false,
            status: false,
            metadata: true,
        },
        command_target: None,
    };
    let resp = UR::Metadata {
        mailbox,
        entries: vec![],
    };
    let result = classify(CK::Noop, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlyUnsolicited),
        "METADATA during NOOP with notify.metadata=true should be OnlyUnsolicited, got {result:?}"
    );
}

/// Without NOTIFY metadata events, METADATA during a non-GETMETADATA
/// command is Impossible per RFC 5464.
#[test]
fn no_notify_metadata_flag_classifies_unsolicited_metadata_as_impossible() {
    let mailbox = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Metadata {
        mailbox,
        entries: vec![],
    };
    let result = classify(CK::Noop, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::Impossible),
        "METADATA during NOOP with notify.metadata=false should be Impossible, got {result:?}"
    );
}

/// RFC 3501 Section6.3.10: STATUS with a mismatched mailbox name is
/// `OnlyUnsolicited` (the response is for a different mailbox than the
/// one the command queried). This is a distinct code path from the
/// NOTIFY-guarded arm.
#[test]
fn status_mismatched_mailbox_is_unsolicited() {
    let mailbox = MailboxName::new("OtherFolder").unwrap();
    let target = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: Some(&target),
    };
    let resp = UR::MailboxStatus {
        mailbox,
        items: vec![],
    };
    let result = classify(CK::Status, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlyUnsolicited),
        "STATUS response for non-target mailbox should be OnlyUnsolicited, got {result:?}"
    );
}

/// LIST during LIST-STATUS is `OnlySolicited` even when NOTIFY list
/// flag is set  -  the solicited arm takes priority over the NOTIFY guard.
#[test]
fn notify_list_flag_does_not_affect_solicited_list_status() {
    let ctx = ClassificationContext {
        notify: NotifyFlags {
            list: true,
            status: false,
            metadata: false,
        },
        command_target: None,
    };
    let resp = UR::List(crate::types::mailbox::MailboxInfo::default());
    let result = classify(CK::ListStatus, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "LIST during LIST-STATUS with notify.list=true should still be OnlySolicited, got {result:?}"
    );
}

/// METADATA during a GETMETADATA command is `OnlySolicited` regardless
/// of NOTIFY flags.
#[test]
fn notify_metadata_flag_does_not_affect_solicited_metadata() {
    let mailbox = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags {
            list: false,
            status: false,
            metadata: true,
        },
        command_target: None,
    };
    let resp = UR::Metadata {
        mailbox,
        entries: vec![],
    };
    let result = classify(CK::GetMetadata, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "METADATA during GETMETADATA with notify.metadata=true should still be OnlySolicited, got {result:?}"
    );
}

// ---- SEARCH routing for SearchReturn / SearchSave (RFC 5182 fix) ----

/// SEARCH during `SearchReturn` is `OnlySolicited`.
#[test]
fn search_return_routes_search_as_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Search {
        uids: vec![],
        mod_seq: None,
    };
    let result = classify(CK::SearchReturn, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "SEARCH during SearchReturn should be OnlySolicited, got {result:?}"
    );
}

/// SEARCH during `SearchSave` is `OnlySolicited`.
#[test]
fn search_save_routes_search_as_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Search {
        uids: vec![],
        mod_seq: None,
    };
    let result = classify(CK::SearchSave, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "SEARCH during SearchSave should be OnlySolicited, got {result:?}"
    );
}

/// ESEARCH during `SearchReturn` is `OnlySolicited`.
#[test]
fn search_return_routes_esearch_as_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Esearch(EsearchResponse::default());
    let result = classify(CK::SearchReturn, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "ESEARCH during SearchReturn should be OnlySolicited, got {result:?}"
    );
}

/// ESEARCH during `SearchSave` is `OnlySolicited`.
#[test]
fn search_save_routes_esearch_as_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Esearch(EsearchResponse::default());
    let result = classify(CK::SearchSave, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "ESEARCH during SearchSave should be OnlySolicited, got {result:?}"
    );
}

/// SEARCH during base Search is still `OnlySolicited` (baseline).
#[test]
fn search_routes_search_as_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Search {
        uids: vec![],
        mod_seq: None,
    };
    let result = classify(CK::Search, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "SEARCH during Search should be OnlySolicited, got {result:?}"
    );
}

/// ESEARCH during base Search is still `OnlySolicited` (baseline).
#[test]
fn search_routes_esearch_as_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Esearch(EsearchResponse::default());
    let result = classify(CK::Search, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "ESEARCH during Search should be OnlySolicited, got {result:?}"
    );
}

// ---- Pipeline interleave safety (Postel's law, RFC 3501 Section5.5) ----

/// NAMESPACE response is `OnlySolicited` for NAMESPACE.
#[test]
fn pipeline_interleave_namespace_solicited_for_namespace() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Namespace {
        personal: vec![],
        other: vec![],
        shared: vec![],
    };
    assert!(
        matches!(
            classify(CK::Namespace, &resp, &ctx),
            SolicitationRule::OnlySolicited
        ),
        "NAMESPACE during NAMESPACE must be OnlySolicited"
    );
}

/// NAMESPACE response is Impossible for STATUS.
#[test]
fn pipeline_interleave_namespace_impossible_for_status() {
    let target = MailboxName::new("INBOX").unwrap();
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: Some(&target),
    };
    let resp = UR::Namespace {
        personal: vec![],
        other: vec![],
        shared: vec![],
    };
    assert!(
        matches!(
            classify(CK::Status, &resp, &ctx),
            SolicitationRule::Impossible
        ),
        "NAMESPACE during STATUS must be Impossible"
    );
}

/// LIST response is `OnlySolicited` for LIST.
#[test]
fn pipeline_interleave_list_solicited_for_list() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::List(crate::types::mailbox::MailboxInfo::default());
    assert!(
        matches!(
            classify(CK::List, &resp, &ctx),
            SolicitationRule::OnlySolicited
        ),
        "LIST during LIST must be OnlySolicited"
    );
}

/// LIST response is Impossible for NOOP (without NOTIFY).
#[test]
fn pipeline_interleave_list_impossible_for_noop() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::List(crate::types::mailbox::MailboxInfo::default());
    assert!(
        matches!(
            classify(CK::Noop, &resp, &ctx),
            SolicitationRule::Impossible
        ),
        "LIST during NOOP (no NOTIFY) must be Impossible"
    );
}

// ---- ENABLE classification tests (RFC 5161 Section3.2) ----

/// ENABLED response during ENABLE is `OnlySolicited`.
#[test]
fn enable_routes_enabled_as_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Enabled(vec!["CONDSTORE".into()]);
    let result = classify(CK::Enable, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "ENABLED during ENABLE should be OnlySolicited, got {result:?}"
    );
}

/// ENABLED response during a non-ENABLE command is `Impossible`.
#[test]
fn enabled_outside_enable_is_impossible() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Enabled(vec!["CONDSTORE".into()]);
    let result = classify(CK::Noop, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::Impossible),
        "ENABLED during NOOP should be Impossible, got {result:?}"
    );
}

// ---- UNSELECT / CLOSE EXPUNGE routing (RFC 3691 Section3 / RFC 3501 Section6.4.2) ----

/// EXPUNGE during UNSELECT is `Either`  -  UNSELECT does not expunge,
/// so any EXPUNGE is an async notification, not a solicited response.
#[test]
fn unselect_expunge_is_either() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Expunge(1);
    let result = classify(CK::Unselect, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::Either),
        "EXPUNGE during UNSELECT should be Either, got {result:?}"
    );
}

/// EXPUNGE during CLOSE is `OnlySolicited`  -  CLOSE implicitly expunges.
#[test]
fn close_expunge_is_solicited() {
    let ctx = ClassificationContext {
        notify: NotifyFlags::default(),
        command_target: None,
    };
    let resp = UR::Expunge(1);
    let result = classify(CK::Close, &resp, &ctx);
    assert!(
        matches!(result, SolicitationRule::OnlySolicited),
        "EXPUNGE during CLOSE should be OnlySolicited, got {result:?}"
    );
}
