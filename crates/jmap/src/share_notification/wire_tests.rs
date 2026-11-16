mod share_notification_serde {
    use crate::share_notification::ShareNotification;
    use serde_json::json;

    fn from_json(s: &str) -> ShareNotification {
        serde_json::from_str(s).expect("failed to deserialize ShareNotification")
    }

    #[test]
    fn full_notification_round_trip() {
        let input = json!({
            "id": "notif-1",
            "created": "2024-11-15T10:30:00Z",
            "changedBy": {
                "name": "Alice",
                "email": "alice@example.com",
                "principalId": "p-alice"
            },
            "objectType": "Calendar",
            "objectAccountId": "acct-bob",
            "objectId": "cal-team",
            "oldRights": null,
            "newRights": {
                "mayReadFreeBusy": true,
                "mayReadItems": true,
                "mayWriteAll": false
            },
            "name": "Team Calendar"
        });

        let notif: ShareNotification = serde_json::from_value(input).unwrap();
        assert_eq!(notif.id().map(crate::core::id::Id::as_str), Some("notif-1"));
        assert_eq!(notif.created(), Some("2024-11-15T10:30:00Z"));
        assert_eq!(notif.object_type(), Some("Calendar"));
        assert_eq!(
            notif.object_account_id().map(crate::core::id::Id::as_str),
            Some("acct-bob")
        );
        assert_eq!(notif.object_id(), Some("cal-team"));
        assert_eq!(notif.name(), Some("Team Calendar"));
        assert!(notif.old_rights().is_none());
        let new_rights = notif.new_rights().unwrap();
        assert_eq!(new_rights.get("mayReadFreeBusy"), Some(&true));
        assert_eq!(new_rights.get("mayWriteAll"), Some(&false));

        let changed_by = notif.changed_by().unwrap();
        assert_eq!(changed_by.name(), Some("Alice"));
        assert_eq!(changed_by.email(), Some("alice@example.com"));
        assert_eq!(
            changed_by.principal_id().map(crate::core::id::Id::as_str),
            Some("p-alice")
        );
    }

    #[test]
    fn minimal_notification_deserializes() {
        let notif = from_json(r#"{"id":"n1"}"#);
        assert_eq!(notif.id().map(crate::core::id::Id::as_str), Some("n1"));
        assert!(notif.created().is_none());
        assert!(notif.changed_by().is_none());
        assert!(notif.object_type().is_none());
        assert!(notif.old_rights().is_none());
        assert!(notif.new_rights().is_none());
    }

    #[test]
    fn property_display() {
        use crate::share_notification::Property;
        assert_eq!(Property::ObjectType.to_string(), "objectType");
        assert_eq!(Property::ChangedBy.to_string(), "changedBy");
        assert_eq!(Property::OldRights.to_string(), "oldRights");
        assert_eq!(Property::NewRights.to_string(), "newRights");
        assert_eq!(Property::ObjectAccountId.to_string(), "objectAccountId");
    }

    #[test]
    fn filter_serialization() {
        use crate::share_notification::query::Filter;

        let f = Filter::after("2024-01-01T00:00:00Z");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"after": "2024-01-01T00:00:00Z"}));

        let f = Filter::object_type("Calendar");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"objectType": "Calendar"}));

        let f = Filter::object_account_id("acct-1");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"objectAccountId": "acct-1"}));
    }
}
