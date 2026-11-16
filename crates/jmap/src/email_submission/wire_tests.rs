mod email_submission_wire {
    use crate::email_submission::{Address, EmailSubmissionSet, UndoStatus};
    use serde_json::json;

    #[test]
    fn envelope_address_parameters() {
        // RFC 4865 FUTURERELEASE rides as a `mailFrom` parameter.
        let held = Address::new("me@example.com")
            .with_parameter("holduntil", Some("2026-01-02T03:04:05Z"));
        assert_eq!(
            serde_json::to_value(&held).unwrap(),
            json!({
                "email": "me@example.com",
                "parameters": {"holduntil": "2026-01-02T03:04:05Z"}
            })
        );

        // A valueless ESMTP parameter is a null value, not an empty
        // string.
        let flagged = Address::new("me@example.com").with_parameter("body", None::<String>);
        assert_eq!(
            serde_json::to_value(&flagged).unwrap(),
            json!({"email": "me@example.com", "parameters": {"body": null}})
        );

        // With no parameters at all the key is still emitted, as an
        // explicit null. RFC 8621 s7 types `parameters` as
        // `String[String|null]|null`: a nullable member, not an optional
        // one, and RFC 8620 s5.3 only permits omitting a create property
        // that has a defined default. Omitting it is therefore a
        // rejection risk on strict servers, so the null must be PRESENT.
        assert_eq!(
            serde_json::to_value(Address::new("me@example.com")).unwrap(),
            json!({"email": "me@example.com", "parameters": null})
        );
    }

    #[test]
    fn create_and_on_success_arguments() {
        let mut set = EmailSubmissionSet::new();
        {
            let create = set.create();
            create.identity_id("identity-1");
            create.email_id("e1");
            create.envelope("me@example.com", ["you@example.com"]);
            create.undo_status(UndoStatus::Pending);
        }
        set.on_success_update_email("c0").keyword(
            &crate::email::keyword::Keyword::parse(crate::email::DRAFT_KEYWORD).unwrap(),
            false,
        );
        let set = set.on_success_destroy_email("c0");

        let value = serde_json::to_value(&set).unwrap();

        let created = value
            .get("create")
            .and_then(|c| c.get("c0"))
            .expect("create entry c0");
        assert_eq!(created.get("identityId"), Some(&json!("identity-1")));
        assert_eq!(created.get("emailId"), Some(&json!("e1")));
        assert_eq!(created.get("undoStatus"), Some(&json!("pending")));
        assert_eq!(
            created.get("envelope"),
            Some(&json!({
                "mailFrom": {"email": "me@example.com", "parameters": null},
                "rcptTo": [{"email": "you@example.com", "parameters": null}]
            }))
        );

        // Both onSuccess arguments key off the create-id with a `#`
        // prefix (RFC 8621 s7.5).
        assert_eq!(
            value.get("onSuccessUpdateEmail"),
            Some(&json!({"#c0": {"keywords/$draft": null}}))
        );
        assert_eq!(value.get("onSuccessDestroyEmail"), Some(&json!(["#c0"])));
    }

    #[test]
    fn a_submission_id_reference_is_not_hash_prefixed() {
        let mut set = EmailSubmissionSet::new();
        set.on_success_update_email_id(crate::email_submission::EmailSubmissionId::new("sub-1"))
            .subject("ignored");
        let value = serde_json::to_value(&set).unwrap();
        assert_eq!(
            value.get("onSuccessUpdateEmail"),
            Some(&json!({"sub-1": {"subject": "ignored"}}))
        );
    }

    #[test]
    fn undo_status_patch() {
        let mut set = EmailSubmissionSet::new();
        set.update(crate::email_submission::EmailSubmissionId::new("sub-1"))
            .undo_status(UndoStatus::Canceled);
        assert_eq!(
            serde_json::to_value(&set).unwrap().get("update"),
            Some(&json!({"sub-1": {"undoStatus": "canceled"}}))
        );
    }

    #[test]
    fn submission_decodes_delivery_status() {
        let submission: crate::email_submission::EmailSubmission = serde_json::from_str(
            r#"{"id":"sub-1","identityId":"i1","emailId":"e1","threadId":"t1",
                "sendAt":"2026-01-02T03:04:05Z","undoStatus":"final",
                "deliveryStatus":{"you@example.com":{"smtpReply":"250 ok",
                    "delivered":"yes","displayed":"unknown"}}}"#,
        )
        .expect("submission decodes");

        assert_eq!(submission.send_at(), Some(1_767_323_045));
        assert_eq!(submission.undo_status(), Some(&UndoStatus::Final));
        let status = submission
            .delivery_status_email("you@example.com")
            .expect("delivery status");
        assert_eq!(status.smtp_reply(), "250 ok");
    }
}

mod unknown_wire_values {
    #[cfg(feature = "mail")]
    #[test]
    fn undo_status_degrades_an_unknown_value() {
        assert_eq!(
            serde_json::from_str::<crate::email_submission::UndoStatus>(r#""queued""#).unwrap(),
            crate::email_submission::UndoStatus::Unknown
        );
    }

    #[cfg(feature = "mail")]
    #[test]
    fn delivery_state_degrades_an_unknown_value() {
        assert_eq!(
            serde_json::from_str::<crate::email_submission::Delivered>(r#""bounced""#).unwrap(),
            crate::email_submission::Delivered::Other
        );
        assert_eq!(
            serde_json::from_str::<crate::email_submission::Displayed>(r#""no""#).unwrap(),
            crate::email_submission::Displayed::Other
        );
    }
}
