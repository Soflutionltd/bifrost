mod mailbox_wire {
    use crate::core::SetCreate;
    use crate::mailbox::ACL;
    use crate::mailbox::{MailboxCreate, MailboxId, MailboxPatch, MailboxSet, Role};
    use serde_json::json;

    #[test]
    fn role_wire_names() {
        for (role, wire) in [
            (Role::Inbox, "inbox"),
            (Role::Sent, "sent"),
            (Role::Trash, "trash"),
            (Role::Drafts, "drafts"),
            (Role::Junk, "junk"),
            (Role::Archive, "archive"),
            (Role::Important, "important"),
        ] {
            assert_eq!(serde_json::to_value(&role).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_str::<Role>(&format!("\"{wire}\"")).unwrap(),
                role
            );
        }
        // Role matching is case-insensitive on the way in.
        assert_eq!(
            serde_json::from_str::<Role>("\"INBOX\"").unwrap(),
            Role::Inbox
        );
    }

    #[test]
    fn unknown_roles_survive_as_other_but_are_lower_cased() {
        // Note the case fold: an `x-` role does NOT round-trip byte for
        // byte, which matters if the value is ever echoed back in a set.
        assert_eq!(
            serde_json::from_str::<Role>("\"x-MyRole\"").unwrap(),
            Role::Other("x-myrole".to_string())
        );
        assert_eq!(
            serde_json::to_value(Role::Other("x-myrole".to_string())).unwrap(),
            json!("x-myrole")
        );
    }

    #[test]
    fn role_decodes_when_the_string_is_not_borrowable() {
        assert_eq!(
            serde_json::from_value::<Role>(json!("inbox")).unwrap(),
            Role::Inbox
        );
        // 92 is the backslash byte, so the JSON below spells `inbox`
        // with a `u0069` escape for the leading `i`.
        let escaped = String::from_utf8(vec![
            b'"', 92, b'u', b'0', b'0', b'6', b'9', b'n', b'b', b'o', b'x', b'"',
        ])
        .expect("ascii");
        assert_eq!(serde_json::from_str::<Role>(&escaped).unwrap(), Role::Inbox);
    }

    #[test]
    fn a_fresh_create_serialises_to_an_empty_object() {
        // Untouched fields must be omitted; empty values remain expressible.
        let create = MailboxCreate::new(None);
        assert_eq!(serde_json::to_value(&create).unwrap(), json!({}));
    }

    #[test]
    fn create_with_an_explicit_none_parent_sends_null() {
        let mut create = MailboxCreate::new(None);
        create.name("Top level");
        create.parent_id(None::<MailboxId>);
        let value = serde_json::to_value(&create).unwrap();
        assert_eq!(value.get("name"), Some(&json!("Top level")));
        assert_eq!(value.get("parentId"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn create_id_references_are_hash_prefixed() {
        let mut create = MailboxCreate::new(None);
        create.parent_id_ref("c0");
        assert_eq!(
            serde_json::to_value(&create).unwrap().get("parentId"),
            Some(&json!("#c0"))
        );
    }

    #[test]
    fn an_empty_mailbox_patch_omits_role_and_share_with() {
        assert_eq!(
            serde_json::to_value(MailboxPatch::default()).unwrap(),
            json!({}),
            "an untouched patch must not remove any properties"
        );

        // The live rename path, verbatim.
        let mut set = MailboxSet::new();
        set.update(MailboxId::new("mb1")).name("Renamed");
        assert_eq!(
            serde_json::to_value(&set).unwrap().get("update"),
            Some(&json!({
                "mb1": {"name": "Renamed"}
            }))
        );
    }

    #[test]
    fn patching_parent_id_to_none_emits_a_null_parent_id() {
        let mut patch = MailboxPatch::default();
        patch.parent_id(None::<MailboxId>);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("parentId"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn patching_parent_id_to_some_moves_the_mailbox() {
        let mut patch = MailboxPatch::default();
        patch.parent_id(Some(MailboxId::new("mb-parent")));
        assert_eq!(
            serde_json::to_value(&patch).unwrap().get("parentId"),
            Some(&json!("mb-parent"))
        );
    }

    #[test]
    fn role_null_clears_and_default_omits() {
        let mut patch = MailboxPatch::default();
        assert_eq!(serde_json::to_value(&patch).unwrap(), json!({}));
        patch.role(None);
        assert_eq!(serde_json::to_value(&patch).unwrap(), json!({"role": null}));
        patch.role(Some(Role::Inbox));
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"role": "inbox"})
        );
    }

    #[test]
    fn share_with_replacement_uses_the_rfc_property_names() {
        let mut patch = MailboxPatch::default();
        patch.acls([("u1", [ACL::ReadItems, ACL::AddItems])]);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(
            value.get("shareWith").and_then(|v| v.get("u1")),
            Some(&json!({"mayReadItems": true, "mayAddItems": true}))
        );
    }

    #[test]
    fn acl_set_builds_a_patch_path_the_server_recognises() {
        let mut patch = MailboxPatch::default();
        patch.acl_set("u1", ACL::ReadItems, true);
        patch.acl_set("u1", ACL::Administer, false);
        let value = serde_json::to_value(&patch).unwrap();

        assert_eq!(value.get("shareWith/u1/mayReadItems"), Some(&json!(true)));
        assert_eq!(value.get("shareWith/u1/mayShare"), Some(&json!(false)));
    }

    #[test]
    fn on_destroy_remove_emails_flattens_into_the_set_request() {
        let set = MailboxSet::new()
            .destroy([MailboxId::new("mb1")])
            .on_destroy_remove_emails(false);
        let value = serde_json::to_value(&set).unwrap();
        assert_eq!(value.get("destroy"), Some(&json!(["mb1"])));
        assert_eq!(value.get("onDestroyRemoveEmails"), Some(&json!(false)));
    }

    #[test]
    fn mailbox_decodes_counts_and_rights() {
        let mailbox: crate::mailbox::Mailbox = serde_json::from_str(
            r#"{"id":"mb1","name":"Inbox","parentId":null,"role":"inbox","sortOrder":0,
                "totalEmails":10,"unreadEmails":3,"totalThreads":8,"unreadThreads":2,
                "isSubscribed":true,
                "myRights":{"mayReadItems":true,"mayAddItems":false}}"#,
        )
        .expect("mailbox decodes");

        assert_eq!(mailbox.name(), Some("Inbox"));
        assert_eq!(mailbox.role(), Some(&Role::Inbox));
        assert_eq!(mailbox.parent_id(), None, "a null parentId reads as absent");
        assert_eq!(mailbox.total_emails(), Some(10));
        assert_eq!(mailbox.unread_threads(), Some(2));
        assert_eq!(mailbox.is_subscribed(), Some(true));

        let rights = mailbox.my_rights().expect("myRights present");
        assert!(rights.may_read_items());
        // Rights the server omitted default to false rather than
        // failing the decode.
        assert!(!rights.may_submit());
        assert_eq!(rights.acl_list(), vec![ACL::ReadItems]);
    }
}
