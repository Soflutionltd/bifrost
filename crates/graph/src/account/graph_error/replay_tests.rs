use super::*;
use crate::account::batch::subrequest_replayable;
#[test]
fn a_missing_conditional_subresponse_reconciles() {
    for (op, safe) in [
        (AccountOperation::SetCategory, false),
        (AccountOperation::SetCategory, true),
        (AccountOperation::BulkDestroy, false),
    ] {
        let error = batch_response_missing(op, None, "omitted", safe);
        if safe {
            assert!(matches!(
                error.recovery(),
                bifrost_types::RecoveryClass::Retry(_)
            ));
        } else {
            let bifrost_types::RecoveryClass::Reconcile(a) = error.recovery() else {
                panic!("{error:?}")
            };
            let mut expected = vec![bifrost_types::ReconcileAction::CheckTarget];
            if op == AccountOperation::BulkDestroy {
                expected.push(bifrost_types::ReconcileAction::DedupeByClientId);
            }
            assert_eq!(a.guidance.actions, expected);
        }
    }
}
#[test]
fn subrequest_replayable_follows_the_declaration_rule_and_fails_closed() {
    for (method, header, expected) in [
        ("GET", None, true),
        ("PATCH", None, false),
        ("PATCH", Some("v1"), false),
        ("POST", None, false),
        ("DELETE", None, false),
        ("bad method", None, false),
        ("PATCH", Some("bad\nvalue"), false),
    ] {
        let item = crate::types::BatchRequestItem {
            id: "0".into(),
            method: method.into(),
            url: "/me/messages/m1".into(),
            body: None,
            headers: header.map(|value| [("If-Match".into(), value.into())].into_iter().collect()),
        };
        assert_eq!(
            subrequest_replayable(&item),
            expected,
            "{method} {header:?}"
        );
    }
}
