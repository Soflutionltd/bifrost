mod settings_wire {
    use crate::sieve::SieveScriptSet;
    use serde_json::json;

    #[test]
    fn sieve_activation_arguments() {
        // Activating a script created in the same request references the
        // create-id with a `#`; activating an existing one does not.
        let by_create_id = SieveScriptSet::new().on_success_activate_script("c0");
        assert_eq!(
            serde_json::to_value(&by_create_id)
                .unwrap()
                .get("onSuccessActivateScript"),
            Some(&json!("#c0"))
        );

        let by_id = SieveScriptSet::new()
            .on_success_activate_script_id(crate::sieve::SieveScriptId::new("s1"));
        assert_eq!(
            serde_json::to_value(&by_id)
                .unwrap()
                .get("onSuccessActivateScript"),
            Some(&json!("s1"))
        );

        let deactivate = SieveScriptSet::new().on_success_deactivate_script(true);
        assert_eq!(
            serde_json::to_value(&deactivate)
                .unwrap()
                .get("onSuccessDeactivateScript"),
            Some(&json!(true))
        );
    }

    #[test]
    fn sieve_validate_response_carries_a_set_error() {
        let response: crate::sieve::validate::SieveScriptValidateResponse = serde_json::from_str(
            r#"{"error":{"type":"invalidScript","description":"line 3: syntax error"}}"#,
        )
        .expect("validate response decodes");
        let error = response.into_error().expect("an error");
        assert_eq!(
            error.error_type(),
            &crate::core::set::SetErrorType::InvalidScript
        );
        assert_eq!(error.description(), Some("line 3: syntax error"));
    }
}
