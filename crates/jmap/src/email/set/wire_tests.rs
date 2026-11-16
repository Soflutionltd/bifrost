mod email_set_patch_shapes {
    use crate::email::EmailPatch;
    use crate::mailbox::MailboxId;
    use serde_json::json;

    #[test]
    fn dotted_paths_use_null_to_remove_and_true_to_add() {
        let mut patch = EmailPatch::default();
        patch.keyword(
            &crate::email::keyword::Keyword::parse("$seen").unwrap(),
            true,
        );
        patch.keyword(
            &crate::email::keyword::Keyword::parse("$flagged").unwrap(),
            false,
        );
        patch.mailbox_id(&MailboxId::new("mb1"), true);
        patch.mailbox_id(&MailboxId::new("mb2"), false);
        let value = serde_json::to_value(&patch).unwrap();

        assert_eq!(
            value,
            json!({
                "keywords/$seen": true,
                "keywords/$flagged": null,
                "mailboxIds/mb1": true,
                "mailboxIds/mb2": null
            })
        );
    }

    #[test]
    fn a_path_setter_clears_the_wholesale_property() {
        // RFC 8620 s5.3 forbids sending both `keywords` and
        // `keywords/x` in one PatchObject.
        let mut patch = EmailPatch::default();
        patch.keywords([crate::email::keyword::Keyword::parse("$seen").unwrap()]);
        patch.keyword(
            &crate::email::keyword::Keyword::parse("$flagged").unwrap(),
            true,
        );
        let value = serde_json::to_value(&patch).unwrap();
        assert!(value.get("keywords").is_none());
        assert_eq!(value.get("keywords/$flagged"), Some(&json!(true)));
    }

    #[test]
    fn wholesale_setters_clear_previously_set_paths() {
        let mut patch = EmailPatch::default();
        patch.keyword(
            &crate::email::keyword::Keyword::parse("$flagged").unwrap(),
            true,
        );
        patch.keywords([crate::email::keyword::Keyword::parse("$seen").unwrap()]);
        patch.mailbox_id(&MailboxId::new("mb1"), true);
        patch.mailbox_ids([MailboxId::new("mb2")]);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("keywords"), Some(&json!({"$seen": true})));
        assert!(value.get("keywords/$flagged").is_none());
        assert_eq!(value.get("mailboxIds"), Some(&json!({"mb2": true})));
        assert!(value.get("mailboxIds/mb1").is_none());
    }

    // A raw entry for the exact property is not a child path, so the
    // wholesale setter has to remove it by name as well. Serialising is
    // checked on the JSON text, not on a `Value`: two writes of the same
    // key collapse in a `serde_json::Map` but both reach the wire.
    #[test]
    fn wholesale_setters_clear_a_raw_entry_for_the_same_property() {
        let mut patch = EmailPatch::default();
        patch.null_property("keywords");
        patch.keywords([crate::email::keyword::Keyword::parse("$seen").unwrap()]);
        patch
            .raw_property("mailboxIds", &json!({"mb9": true}))
            .expect("serializable");
        patch.mailbox_ids([MailboxId::new("mb2")]);
        let text = serde_json::to_string(&patch).unwrap();
        assert_eq!(
            text.matches("\"keywords\"").count(),
            1,
            "duplicate `keywords` keys in {text}"
        );
        assert_eq!(
            text.matches("\"mailboxIds\"").count(),
            1,
            "duplicate `mailboxIds` keys in {text}"
        );
        assert!(!text.contains("\"keywords\":null"), "{text}");
        assert!(!text.contains("mb9"), "{text}");
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("keywords"), Some(&json!({"$seen": true})));
        assert_eq!(value.get("mailboxIds"), Some(&json!({"mb2": true})));
    }

    #[test]
    fn raw_and_null_property_escape_hatches() {
        let mut patch = EmailPatch::default();
        patch
            .raw_property("keywords/$vendor", &true)
            .expect("serializable");
        patch.null_property("preview");
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("keywords/$vendor"), Some(&json!(true)));
        assert_eq!(value.get("preview"), Some(&serde_json::Value::Null));
    }
}
