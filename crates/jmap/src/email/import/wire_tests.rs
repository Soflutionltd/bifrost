mod object_wire {
    use serde_json::json;

    #[test]
    fn email_import_entries_are_keyed_i0_i1() {
        let mut request = crate::email::import::EmailImportRequest::new().if_in_state("s1");
        {
            let entry = request.email("blob-1");
            entry.mailbox_ids([crate::mailbox::MailboxId::new("mb1")]);
            entry.keywords([crate::email::keyword::Keyword::parse("$seen").unwrap()]);
            assert_eq!(entry.create_id(), "i0");
        }
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value.get("ifInState"), Some(&json!("s1")));
        assert_eq!(
            value.get("emails").and_then(|e| e.get("i0")),
            Some(&json!({
                "blobId": "blob-1",
                "mailboxIds": {"mb1": true},
                "keywords": {"$seen": true}
            }))
        );
    }
}
