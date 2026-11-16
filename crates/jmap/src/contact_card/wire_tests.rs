mod contact_card_patch_membership {
    use crate::contact_card::ContactCardPatch;
    use serde_json::json;

    #[test]
    fn patch_address_book_id_uses_dotted_paths_without_overlap() {
        let mut patch = ContactCardPatch::default();
        patch.address_book_ids(["ab-1"]);
        patch.address_book_id("ab-2", false);
        patch.address_book_id("ab-3", true);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"addressBookIds/ab-2": null, "addressBookIds/ab-3": true})
        );
    }
}

mod membership_wire {
    use crate::contact_card::ContactCardCreate;
    use crate::core::SetCreate;
    use serde_json::json;

    #[test]
    fn contact_card_create_removal_omits_membership() {
        let mut card = ContactCardCreate::new(None);
        card.address_book_id("keep", true);
        card.address_book_id("ab-1", true);
        card.address_book_id("ab-1", false);
        card.address_book_id("absent", false);

        let value = serde_json::to_value(&card).unwrap();
        assert_eq!(value["addressBookIds"], json!({"keep": true}));
    }

    #[test]
    fn contact_card_address_book_id_true_produces_true() {
        let mut card = ContactCardCreate::new(None);
        card.address_book_id("ab-1", true);

        let value = serde_json::to_value(&card).unwrap();
        let ab_ids = value.get("addressBookIds").unwrap();
        let entry = ab_ids.get("ab-1").unwrap();
        assert_eq!(entry, &json!(true));
    }
    #[test]
    fn contact_membership_patch_removal_keeps_null_path_operations() {
        use crate::contact_card::ContactCardPatch;
        let mut card = ContactCardPatch::default();
        card.address_book_id("ab~1", false);
        assert_eq!(
            serde_json::to_value(&card).unwrap(),
            json!({"addressBookIds/ab~01": null})
        );
    }
}

mod extension_properties {
    use crate::contact_card::ContactCard;
    use serde_json::json;

    #[test]
    fn contact_card_preserves_extension_properties() {
        let input = json!({
            "uid": "card-1",
            "kind": "individual",
            "example.com:department": "Engineering"
        });

        let card: ContactCard = serde_json::from_value(input.clone()).expect("deser failed");

        assert_eq!(card.uid(), Some("card-1"));
        assert_eq!(card.kind(), Some("individual"));
        assert_eq!(
            card.property("example.com:department"),
            Some(&json!("Engineering"))
        );

        let output = serde_json::to_value(&card).unwrap();
        assert_eq!(
            output.get("example.com:department"),
            Some(&json!("Engineering"))
        );
        assert_eq!(output.get("uid"), Some(&json!("card-1")));
    }
}

mod property_wire {
    use serde_json::json;

    #[test]
    #[cfg(feature = "contacts")]
    fn contact_card_property_other_round_trip() {
        use crate::contact_card::Property;

        let property = Property::Other("vendor:x-field".to_string());
        assert_eq!(property.to_string(), "vendor:x-field");
        assert_eq!(Property::from("vendor:x-field"), property);
        let serialized = serde_json::to_value(&property).unwrap();
        assert_eq!(serialized, json!("vendor:x-field"));
        assert_eq!(
            serde_json::from_value::<Property>(serialized).unwrap(),
            property
        );
    }
}
