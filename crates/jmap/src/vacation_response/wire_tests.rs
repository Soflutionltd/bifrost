mod settings_wire {
    use crate::vacation_response::VacationResponsePatch;
    use serde_json::json;

    #[test]
    fn vacation_patch_setters_clear_nullable_properties() {
        let mut patch = VacationResponsePatch::default();
        patch.is_enabled(false);
        patch.subject(None::<String>);
        patch.to_date(None);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"isEnabled": false, "subject": null, "toDate": null})
        );
    }

    #[test]
    fn vacation_response_decodes_dates() {
        let vacation: crate::vacation_response::VacationResponse = serde_json::from_str(
            r#"{"id":"singleton","isEnabled":true,"fromDate":"2026-01-02T03:04:05Z",
                "toDate":null,"subject":"Away","textBody":"back soon"}"#,
        )
        .expect("vacation decodes");
        assert!(vacation.is_enabled());
        assert_eq!(vacation.from_date(), Some(1_767_323_045));
        assert_eq!(vacation.to_date(), None);
        assert_eq!(vacation.subject(), Some("Away"));
    }
}
