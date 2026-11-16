mod query_wire {
    use serde_json::json;

    fn wire(value: &impl serde::Serialize) -> serde_json::Value {
        serde_json::to_value(value).expect("query fragments serialize")
    }

    /// RFC 9670 Section 3.2 (`ShareNotification/query`).
    #[test]
    fn share_notification_filter_and_comparator_property_names() {
        use crate::share_notification::query::{Comparator, Filter};

        assert_eq!(
            wire(&Filter::after("2026-01-02T03:04:05Z")),
            json!({"after": "2026-01-02T03:04:05Z"})
        );
        assert_eq!(
            wire(&Filter::before("2026-01-02T03:04:05Z")),
            json!({"before": "2026-01-02T03:04:05Z"})
        );
        assert_eq!(
            wire(&Filter::object_type("Mailbox")),
            json!({"objectType": "Mailbox"})
        );
        assert_eq!(
            wire(&Filter::object_account_id("acct-9")),
            json!({"objectAccountId": "acct-9"})
        );

        assert_eq!(
            wire(&Comparator::created()),
            json!({"isAscending": true, "property": "created"})
        );
    }
}
