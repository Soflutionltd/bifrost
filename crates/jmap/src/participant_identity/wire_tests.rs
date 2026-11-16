mod participant_identity_wire {
    use crate::core::SetCreate;
    use crate::participant_identity::{
        ParticipantIdentity, ParticipantIdentityCreate, ParticipantIdentityPatch, Property,
    };
    use serde_json::json;

    #[test]
    fn calendar_address_decodes_and_round_trips_through_create_and_patch() {
        let identity: ParticipantIdentity = serde_json::from_value(json!({
            "id": "pi-1",
            "name": "Ada",
            "calendarAddress": "mailto:ada@example.test",
            "isDefault": true
        }))
        .expect("a draft-26 identity decodes");
        assert_eq!(identity.calendar_address(), Some("mailto:ada@example.test"));
        assert_eq!(
            serde_json::to_value(Property::CalendarAddress).unwrap(),
            json!("calendarAddress")
        );

        let mut create = ParticipantIdentityCreate::new(None);
        create.calendar_address("mailto:ada@example.test");
        assert_eq!(
            serde_json::to_value(create).unwrap(),
            json!({"calendarAddress": "mailto:ada@example.test"})
        );

        let mut patch = ParticipantIdentityPatch::default();
        patch.calendar_address("mailto:grace@example.test");
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({"calendarAddress": "mailto:grace@example.test"})
        );
    }

    /// draft-26 §3 makes `calendarAddress` required and non-nullable, so
    /// the patch is two-state: omitted, or a String. A name-only patch
    /// must not emit `"calendarAddress": null` - that is a property
    /// removal the server has to answer with `invalidProperties`.
    #[test]
    fn patch_omits_calendar_address_rather_than_nulling_it() {
        let mut patch = ParticipantIdentityPatch::default();
        patch.name("Grace");
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({"name": "Grace"})
        );
    }
}
