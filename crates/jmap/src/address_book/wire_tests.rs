mod address_book_wire {
    use crate::address_book::{AddressBook, AddressBookPatch};
    use serde_json::json;

    #[test]
    fn patching_a_nullable_field_to_null_reaches_the_wire() {
        // Like `MailboxPatch::parent_id` and `VacationResponsePatch`, this
        // uses `Field<T>` to preserve explicit property removal.
        let mut patch = AddressBookPatch::default();
        patch.name("Work");
        patch.description(None::<String>);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("name"), Some(&json!("Work")));
        assert_eq!(value.get("description"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn an_untouched_field_is_omitted_from_the_patch() {
        let mut patch = AddressBookPatch::default();
        patch.name("Work");
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"name": "Work"})
        );
    }

    #[test]
    fn address_book_decodes_rights() {
        let book: AddressBook = serde_json::from_str(
            r#"{"id":"ab1","name":"Personal","description":null,"sortOrder":0,
                "isDefault":true,"isSubscribed":true,
                "myRights":{"mayRead":true,"mayWrite":false}}"#,
        )
        .expect("address book decodes");
        assert_eq!(book.name(), Some("Personal"));
        assert!(book.description_field().is_null());
        assert_eq!(book.is_default(), Some(true));
        let rights = book.my_rights().expect("rights");
        assert_eq!(rights.may_read, Some(true));
        assert_eq!(rights.may_write, Some(false));
        assert_eq!(rights.may_share, None, "omitted rights stay unknown");
    }
}
