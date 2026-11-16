mod decomposition {
    use serde_json::json;

    #[test]
    fn push_decomposition_merges_in_wire_order_and_keeps_last_present_position() {
        let object: crate::PushObject = serde_json::from_value(json!({
            "@type": "Group", "entries": [
                {"@type": "StateChange", "changed": {"a": {"Principal": "old"}}, "pushState": "p1"},
                {"@type": "Group", "entries": [
                    {"@type": "StateChange", "changed": {"a": {"Principal": "new"}, "b": {"Principal": "b1"}}, "pushState": "p2"},
                    {"@type": "StateChange", "changed": {}}
                ]}
            ]
        })).unwrap();
        let block = object.decompose();
        assert!(block.notifications.is_empty());
        let state = block.state.unwrap();
        assert_eq!(state.changed["a"][&crate::DataType::Principal], "new");
        assert_eq!(state.changed["b"][&crate::DataType::Principal], "b1");
        assert_eq!(state.push_state.as_deref(), Some("p2"));
        let empty: crate::PushObject = serde_json::from_value(json!({
            "@type": "Group", "entries": []
        }))
        .unwrap();
        assert!(empty.decompose().state.is_none());
        let empty_state: crate::PushObject = serde_json::from_value(json!({
            "@type": "StateChange", "changed": {}
        }))
        .unwrap();
        assert!(empty_state.decompose().state.is_some());
    }

    #[cfg(feature = "mail")]
    #[test]
    fn push_decomposition_keeps_token_free_notifications_in_wire_order() {
        let object: crate::PushObject = serde_json::from_value(json!({
            "@type": "Group", "entries": [
                {"@type": "StateChange", "changed": {}, "pushState": "p1"},
                {"@type": "EmailPush", "accountId": "a", "email": {"id": "first"}},
                {"@type": "Group", "entries": [
                    {"@type": "EmailPush", "accountId": "b", "email": {"id": "second"}}
                ]}
            ]
        }))
        .unwrap();
        let block = object.decompose();
        let ids: Vec<_> = block
            .notifications
            .into_iter()
            .map(|notification| {
                let crate::PushObject::EmailPush { email, .. } = notification else {
                    panic!("expected token-free email notification");
                };
                email["id"].as_str().unwrap().to_owned()
            })
            .collect();
        assert_eq!(ids, ["first", "second"]);
        assert_eq!(block.state.unwrap().push_state.as_deref(), Some("p1"));
    }
}
