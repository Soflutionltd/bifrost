mod principal_rfc9670 {
    use crate::principal::Principal;
    use serde_json::json;

    #[test]
    fn principal_with_accounts_deserializes() {
        let input = json!({
            "id": "p-alice",
            "type": "individual",
            "name": "Alice",
            "email": "alice@example.com",
            "capabilities": {
                "urn:ietf:params:jmap:mail": {},
                "urn:ietf:params:jmap:calendars": {"mayCreateCalendar": true}
            },
            "accounts": {
                "acct-1": {
                    "name": "alice@example.com",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:mail": {}
                    }
                },
                "acct-2": {
                    "name": "shared",
                    "isPersonal": false,
                    "isReadOnly": true,
                    "accountCapabilities": {}
                }
            }
        });

        let principal: Principal = serde_json::from_value(input).unwrap();
        assert_eq!(
            principal.id().map(crate::core::id::Id::as_str),
            Some("p-alice")
        );
        assert_eq!(principal.name(), Some("Alice"));

        let caps = principal.capabilities().unwrap();
        assert!(caps.contains_key("urn:ietf:params:jmap:mail"));
        assert!(caps.contains_key("urn:ietf:params:jmap:calendars"));

        let accounts = principal.accounts().unwrap();
        assert_eq!(accounts.len(), 2);

        let acct1 = &accounts[&crate::core::id::AccountId::new("acct-1")];
        assert_eq!(acct1.name(), Some("alice@example.com"));
        assert!(acct1.is_personal());
        assert!(!acct1.is_read_only());
        assert!(
            acct1
                .account_capabilities()
                .contains_key("urn:ietf:params:jmap:mail")
        );

        let acct2 = &accounts[&crate::core::id::AccountId::new("acct-2")];
        assert_eq!(acct2.name(), Some("shared"));
        assert!(!acct2.is_personal());
        assert!(acct2.is_read_only());
    }

    #[test]
    fn principal_without_accounts_deserializes() {
        let input = json!({
            "id": "p-bob",
            "type": "individual",
            "name": "Bob"
        });

        let principal: Principal = serde_json::from_value(input).unwrap();
        assert_eq!(
            principal.id().map(crate::core::id::Id::as_str),
            Some("p-bob")
        );
        assert!(principal.accounts().is_none());
        assert!(principal.capabilities().is_none());
    }

    #[test]
    fn principal_account_ids_filter_serialization() {
        use crate::principal::query::Filter;
        let f = Filter::account_ids(["acct-1", "acct-2"]);
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"accountIds": ["acct-1", "acct-2"]}));
    }
}

mod principal_property_wire_names {
    use crate::principal::Property;
    use serde_json::json;

    #[test]
    fn property_display_and_serde_match_rfc_9670() {
        for (property, wire) in [
            (Property::Id, "id"),
            (Property::Type, "type"),
            (Property::Name, "name"),
            (Property::Description, "description"),
            (Property::Email, "email"),
            (Property::Timezone, "timeZone"),
            (Property::Capabilities, "capabilities"),
            (Property::Aliases, "aliases"),
            (Property::Secret, "secret"),
            (Property::DKIM, "dkim"),
            (Property::Quota, "quota"),
            (Property::Picture, "picture"),
            (Property::Members, "members"),
            (Property::Accounts, "accounts"),
        ] {
            assert_eq!(property.to_string(), wire);
            assert_eq!(serde_json::to_value(property).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_value::<Property>(json!(wire)).unwrap(),
                property
            );
        }
    }
}

mod unknown_wire_values {
    #[test]
    fn principal_type_degrades_an_unknown_value() {
        // `Type::Other` is a real RFC value, distinct from the
        // deserialize-only catch-all.
        assert_eq!(
            serde_json::from_str::<crate::principal::Type>(r#""room""#).unwrap(),
            crate::principal::Type::Unknown
        );
        assert_eq!(
            serde_json::from_str::<crate::principal::Type>(r#""other""#).unwrap(),
            crate::principal::Type::Other
        );
    }
}
