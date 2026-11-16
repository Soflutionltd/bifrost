mod push_subscription_wire {
    use crate::core::SetCreate;
    use crate::push_subscription::{PushSubscriptionCreate, PushSubscriptionGet};
    use serde_json::json;

    #[test]
    fn push_subscription_get_has_no_account_id() {
        // RFC 8620 s7.2: PushSubscription is not account-scoped, so the
        // request must not carry an `accountId` even though
        // `Request::call` injects one into every other method.
        use crate::core::method::JmapMethod;
        let mut request = PushSubscriptionGet::new();
        request.set_account_id(&crate::core::id::AccountId::new("a1"));
        let value = serde_json::to_value(&request).unwrap();
        assert!(
            value.get("accountId").is_none(),
            "accountId must stay absent for a non-account-scoped type"
        );
    }

    #[test]
    fn a_fresh_create_omits_the_empty_types_list() {
        let create = PushSubscriptionCreate::new(None);
        assert_eq!(serde_json::to_value(&create).unwrap(), json!({}));
    }
}
