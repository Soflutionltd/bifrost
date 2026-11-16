mod settings_wire {
    use crate::identity::IdentityPatch;
    use serde_json::json;

    #[test]
    fn an_empty_identity_patch_omits_reply_to_and_bcc() {
        assert_eq!(
            serde_json::to_value(IdentityPatch::default()).unwrap(),
            json!({})
        );

        let mut patch = IdentityPatch::default();
        patch.name("Alice");
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"name": "Alice"}),
            "renaming an identity must not touch replyTo/bcc"
        );
    }

    #[test]
    fn identity_patch_clears_a_list_deliberately_with_none() {
        // The same `None`-means-null behaviour is what a caller who
        // *wants* to clear `replyTo` relies on (RFC 8621 s6 types it
        // `EmailAddress[]|null`), which is why the fix has to be a
        // three-state field rather than flipping the predicate.
        let mut patch = IdentityPatch::default();
        patch.reply_to(None::<std::iter::Empty<crate::email::EmailAddress>>);
        assert_eq!(
            serde_json::to_value(&patch).unwrap().get("replyTo"),
            Some(&serde_json::Value::Null)
        );
    }

    #[test]
    fn identity_patch_empty_list_clears_instead_of_omitting() {
        let mut patch = IdentityPatch::default();
        patch.reply_to(Some(std::iter::empty::<crate::email::EmailAddress>()));
        patch.bcc(Some(std::iter::empty::<crate::email::EmailAddress>()));
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"replyTo": [], "bcc": []})
        );
    }

    #[test]
    fn identity_patch_sets_a_reply_to_list() {
        let mut patch = IdentityPatch::default();
        patch.reply_to(Some(
            [crate::email::EmailAddress::new("r@example.com".to_string())].into_iter(),
        ));
        assert_eq!(
            serde_json::to_value(&patch).unwrap().get("replyTo"),
            Some(&json!([{"name": null, "email": "r@example.com"}]))
        );
    }
}
