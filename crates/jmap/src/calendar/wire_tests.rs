mod calendar_option_option_serialization {
    use crate::calendar::CalendarCreate;
    use crate::core::SetCreate;
    use serde_json::json;

    #[test]
    fn calendar_description_none_serializes_as_null() {
        let mut cal = CalendarCreate::new(None);
        cal.name("Test Calendar");
        cal.description(None::<String>);

        let value = serde_json::to_value(&cal).unwrap();
        // description is set to Some(None), so it serializes as null
        assert!(
            value.get("description").is_some(),
            "description should be present in JSON"
        );
        assert!(
            value.get("description").unwrap().is_null(),
            "description should be null, got {:?}",
            value.get("description")
        );
    }

    #[test]
    fn calendar_description_unset_is_absent() {
        let mut cal = CalendarCreate::new(None);
        cal.name("Test Calendar");
        // Do NOT call cal.description() - leave it as outer None

        let value = serde_json::to_value(&cal).unwrap();
        assert!(
            value.get("description").is_none(),
            "unset description should be absent from JSON, but got {:?}",
            value.get("description")
        );
    }

    #[test]
    fn calendar_description_some_serializes_as_string() {
        let mut cal = CalendarCreate::new(None);
        cal.name("Test Calendar");
        cal.description(Some("A nice calendar"));

        let value = serde_json::to_value(&cal).unwrap();
        assert_eq!(value.get("description"), Some(&json!("A nice calendar")));
    }
}

mod calendar_wire {
    use crate::calendar::{Calendar, CalendarPatch, IncludeInAvailability};
    use serde_json::json;

    #[test]
    fn calendar_decodes_the_full_draft_shape() {
        let calendar: Calendar = serde_json::from_str(
            r##"{"id":"cal1","name":"Work","description":null,"color":"#112233",
                "sortOrder":1,"isSubscribed":true,"isVisible":true,"isDefault":false,
                "includeInAvailability":"attending","timeZone":"Europe/Oslo",
                "myRights":{"mayReadItems":true,"mayWriteAll":false,"mayRSVP":true}}"##,
        )
        .expect("calendar decodes");

        assert_eq!(calendar.name(), Some("Work"));
        assert!(calendar.description_field().is_null());
        assert_eq!(calendar.color(), Some("#112233"));
        assert_eq!(calendar.time_zone(), Some("Europe/Oslo"));
        assert_eq!(
            calendar.include_in_availability(),
            Some(&IncludeInAvailability::Attending)
        );
        let rights = calendar.my_rights().expect("rights");
        assert_eq!(
            rights.may_rsvp,
            Some(true),
            "`mayRSVP` keeps the RFC casing"
        );
        assert_eq!(rights.may_read_items, Some(true));
    }

    #[test]
    fn calendar_patch_clears_and_sets_alerts() {
        let mut patch = CalendarPatch::default();
        patch.default_alerts_with_time(None);
        patch.is_visible(false);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(
            value.get("defaultAlertsWithTime"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(value.get("isVisible"), Some(&json!(false)));
        assert!(value.get("defaultAlertsWithoutTime").is_none());
    }

    #[test]
    fn calendar_set_arguments_flatten() {
        let set = crate::calendar::CalendarSet::new()
            .destroy([crate::calendar::CalendarId::new("cal1")])
            .on_destroy_remove_events(true)
            .on_success_set_is_default("cal2");
        let value = serde_json::to_value(&set).unwrap();
        assert_eq!(value.get("onDestroyRemoveEvents"), Some(&json!(true)));
        assert_eq!(value.get("onSuccessSetIsDefault"), Some(&json!("cal2")));
    }
}

mod unknown_wire_values {
    #[cfg(feature = "calendars")]
    #[test]
    fn include_in_availability_degrades_an_unknown_value() {
        assert_eq!(
            serde_json::from_str::<crate::calendar::IncludeInAvailability>(r#""maybe""#).unwrap(),
            crate::calendar::IncludeInAvailability::Unknown
        );
    }
}
