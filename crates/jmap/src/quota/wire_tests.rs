mod quota_field_three_state {
    use crate::core::field::Field;
    use crate::quota::Quota;

    #[test]
    fn omitted_null_and_present_are_distinguishable() {
        let omitted: Quota = serde_json::from_str(r#"{"id":"q1"}"#).unwrap();
        assert!(omitted.warn_limit_field().is_omitted());

        let null: Quota = serde_json::from_str(r#"{"id":"q1","warnLimit":null}"#).unwrap();
        assert!(null.warn_limit_field().is_null());

        let present: Quota = serde_json::from_str(r#"{"id":"q1","warnLimit":42}"#).unwrap();
        assert_eq!(present.warn_limit_field(), &Field::Value(42));
        assert_eq!(present.warn_limit(), Some(42));
    }

    #[test]
    fn a_null_field_reads_the_same_as_an_omitted_one_through_the_option_getter() {
        // The ergonomic getter collapses the two; the `_field()` sibling
        // is the only way to tell them apart when generating a patch.
        let null: Quota = serde_json::from_str(r#"{"id":"q1","description":null}"#).unwrap();
        let omitted: Quota = serde_json::from_str(r#"{"id":"q1"}"#).unwrap();
        assert_eq!(null.description(), omitted.description());
        assert_ne!(
            null.description_field().is_null(),
            omitted.description_field().is_null()
        );
    }

    #[test]
    fn quota_decodes_the_rfc_9425_shape() {
        let quota: Quota = serde_json::from_str(
            r#"{"id":"q1","resourceType":"octets","used":100,"hardLimit":1000,
                "scope":"account","name":"Storage","types":["Mail"],"softLimit":900}"#,
        )
        .expect("quota decodes");
        assert_eq!(quota.resource_type(), Some("octets"));
        assert_eq!(quota.used(), Some(100));
        assert_eq!(quota.hard_limit(), Some(1000));
        assert_eq!(quota.scope(), Some("account"));
        assert_eq!(quota.soft_limit(), Some(900));
    }
}
