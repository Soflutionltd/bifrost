mod calendar_event_nullable_getters {
    use crate::calendar_event::CalendarEvent;
    use crate::core::field::Field;

    /// Helper: deserialize a CalendarEvent from a JSON string.
    fn from_json(s: &str) -> CalendarEvent {
        serde_json::from_str(s).expect("failed to deserialize CalendarEvent")
    }

    // -- time_zone --

    #[test]
    fn time_zone_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.time_zone().is_omitted());
    }

    #[test]
    fn time_zone_null_returns_null() {
        let event = from_json(r#"{"timeZone":null}"#);
        assert!(event.time_zone().is_null());
    }

    #[test]
    fn time_zone_present_returns_value() {
        let event = from_json(r#"{"timeZone":"America/New_York"}"#);
        assert_eq!(event.time_zone(), Field::Value("America/New_York"));
    }

    // -- color --

    #[test]
    fn color_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.color().is_omitted());
    }

    #[test]
    fn color_null_returns_null() {
        let event = from_json(r#"{"color":null}"#);
        assert!(event.color().is_null());
    }

    #[test]
    fn color_present_returns_value() {
        let event = from_json(r##"{"color":"#ff0000"}"##);
        assert_eq!(event.color(), Field::Value("#ff0000"));
    }

    // -- locale --

    #[test]
    fn locale_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.locale().is_omitted());
    }

    #[test]
    fn locale_null_returns_null() {
        let event = from_json(r#"{"locale":null}"#);
        assert!(event.locale().is_null());
    }

    #[test]
    fn locale_present_returns_value() {
        let event = from_json(r#"{"locale":"en-US"}"#);
        assert_eq!(event.locale(), Field::Value("en-US"));
    }

    // -- alerts --

    #[test]
    fn alerts_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.alerts().is_omitted());
    }

    #[test]
    fn alerts_null_returns_null() {
        let event = from_json(r#"{"alerts":null}"#);
        assert!(
            event.alerts().is_null(),
            "null alerts should give Field::Null"
        );
    }

    #[test]
    fn alerts_present_returns_value() {
        let event = from_json(
            r#"{"alerts":{"a1":{"trigger":{"@type":"OffsetTrigger","offset":"-PT15M"},"action":"display"}}}"#,
        );
        let alerts = event.alerts();
        let map = alerts.as_value().expect("alerts should be Value(map)");
        assert!(map.contains_key("a1"));
    }
}

mod alert_trigger_deserialization {
    use crate::calendar_event::{AlertTrigger, RelativeTo};
    use serde_json::json;

    #[test]
    fn offset_trigger_deserializes() {
        let json_str = r#"{"@type":"OffsetTrigger","offset":"-PT15M","relativeTo":"start"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::OffsetTrigger {
                offset,
                relative_to,
            } => {
                assert_eq!(offset, "-PT15M");
                assert_eq!(relative_to, Some(RelativeTo::Start));
            }
            other => panic!("expected OffsetTrigger, got {other:?}"),
        }
    }

    #[test]
    fn offset_trigger_without_relative_to() {
        let json_str = r#"{"@type":"OffsetTrigger","offset":"PT0S"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::OffsetTrigger {
                offset,
                relative_to,
            } => {
                assert_eq!(offset, "PT0S");
                assert_eq!(relative_to, None);
            }
            other => panic!("expected OffsetTrigger, got {other:?}"),
        }
    }

    #[test]
    fn absolute_trigger_deserializes() {
        let json_str = r#"{"@type":"AbsoluteTrigger","when":"2025-06-15T09:00:00Z"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::AbsoluteTrigger { when } => {
                assert_eq!(when, "2025-06-15T09:00:00Z");
            }
            other => panic!("expected AbsoluteTrigger, got {other:?}"),
        }
    }

    #[test]
    fn unknown_trigger_type_deserializes_as_unknown() {
        // #[serde(other)] catches any unrecognized @type value.
        let json_str = r#"{"@type":"FutureTriggerType","foo":"bar"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        assert!(
            matches!(trigger, AlertTrigger::Unknown),
            "expected Unknown, got {trigger:?}"
        );
    }

    #[test]
    fn offset_trigger_serializes() {
        let trigger = AlertTrigger::OffsetTrigger {
            offset: "-PT10M".to_string(),
            relative_to: Some(RelativeTo::End),
        };
        let value = serde_json::to_value(&trigger).unwrap();
        assert_eq!(value.get("@type"), Some(&json!("OffsetTrigger")));
        assert_eq!(value.get("offset"), Some(&json!("-PT10M")));
        assert_eq!(value.get("relativeTo"), Some(&json!("end")));
    }
}

mod calendar_event_patch_nesting {
    use crate::calendar_event::{CalendarEventPatch, CalendarEventSet};
    use serde_json::json;

    #[test]
    fn patch_calendar_id_uses_dotted_paths() {
        let mut patch = CalendarEventPatch::default();
        patch.calendar_id("cal-1", false);
        patch.calendar_id("cal-2", true);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"calendarIds/cal-1": null, "calendarIds/cal-2": true})
        );
    }

    #[test]
    fn calendar_membership_setters_never_overlap_wholesale_and_dotted_forms() {
        let mut patch = CalendarEventPatch::default();
        patch.calendar_id("cal-1", true);
        patch.calendar_ids(["cal-2"]);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"calendarIds": {"cal-2": true}})
        );

        patch.calendar_id("cal-3", false);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"calendarIds/cal-3": null})
        );
    }

    #[test]
    fn dotted_paths_are_available_through_set_property() {
        let mut patch = CalendarEventPatch::default();
        patch.set_property("participants/p1/participationStatus", json!("accepted"));
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"participants/p1/participationStatus": "accepted"})
        );
    }

    #[test]
    fn set_arguments_flatten_into_the_request() {
        let set = CalendarEventSet::new().send_scheduling_messages(false);
        assert_eq!(
            serde_json::to_value(&set)
                .unwrap()
                .get("sendSchedulingMessages"),
            Some(&json!(false))
        );
    }

    #[test]
    fn get_arguments_flatten_into_the_request() {
        let get = crate::calendar_event::CalendarEventGet::new()
            .ids(["ev1"])
            .recurrence_overrides_after("2026-01-01T00:00:00Z".parse().unwrap())
            .reduce_participants(true)
            .time_zone("Europe/Oslo");
        let value = serde_json::to_value(&get).unwrap();
        assert_eq!(
            value.get("recurrenceOverridesAfter"),
            Some(&json!("2026-01-01T00:00:00Z"))
        );
        assert_eq!(value.get("reduceParticipants"), Some(&json!(true)));
        assert_eq!(value.get("timeZone"), Some(&json!("Europe/Oslo")));
        assert!(value.get("recurrenceOverridesBefore").is_none());
    }
}

mod membership_wire {
    use crate::calendar_event::CalendarEventCreate;
    use crate::core::SetCreate;
    use serde_json::json;

    #[test]
    fn calendar_event_create_removal_omits_membership() {
        let mut event = CalendarEventCreate::new(None);
        event.calendar_id("keep", true);
        event.calendar_id("cal-1", true);
        event.calendar_id("cal-1", false);
        event.calendar_id("absent", false);

        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["calendarIds"], json!({"keep": true}));
    }

    #[test]
    fn calendar_event_calendar_id_true_produces_true() {
        let mut event = CalendarEventCreate::new(None);
        event.calendar_id("cal-1", true);

        let value = serde_json::to_value(&event).unwrap();
        let calendar_ids = value.get("calendarIds").unwrap();
        let entry = calendar_ids.get("cal-1").unwrap();
        assert_eq!(entry, &json!(true));
    }

    #[test]
    fn membership_patch_removal_keeps_null_path_operations() {
        use crate::calendar_event::CalendarEventPatch;

        let mut event = CalendarEventPatch::default();
        event.calendar_id("cal/1", false);
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({"calendarIds/cal~11": null})
        );
    }
}

mod extension_properties {
    use crate::calendar_event::CalendarEvent;
    use serde_json::json;

    #[test]
    fn calendar_event_preserves_extension_properties() {
        let input = json!({
            "uid": "evt-1",
            "title": "Meeting",
            "example.com:custom-field": "custom-value",
            "vendor.io:priority": 42
        });

        let event: CalendarEvent = serde_json::from_value(input.clone()).expect("deser failed");

        // Verify typed accessors work
        assert_eq!(event.uid(), Some("evt-1"));
        assert_eq!(event.title(), Some("Meeting"));

        // Verify extension properties are accessible
        assert_eq!(
            event.property("example.com:custom-field"),
            Some(&json!("custom-value"))
        );
        assert_eq!(event.property("vendor.io:priority"), Some(&json!(42)));

        // Reserialize and verify extension properties survive
        let output = serde_json::to_value(&event).unwrap();
        assert_eq!(
            output.get("example.com:custom-field"),
            Some(&json!("custom-value"))
        );
        assert_eq!(output.get("vendor.io:priority"), Some(&json!(42)));
        assert_eq!(output.get("uid"), Some(&json!("evt-1")));
    }
}

mod property_wire {
    use serde_json::json;

    #[test]
    #[cfg(feature = "calendars")]
    fn calendar_event_property_other_round_trip() {
        use crate::calendar_event::Property;

        let property = Property::Other("example.com:custom".to_string());
        assert_eq!(property.to_string(), "example.com:custom");
        assert_eq!(Property::from("example.com:custom"), property);
        let serialized = serde_json::to_value(&property).unwrap();
        assert_eq!(serialized, json!("example.com:custom"));
        assert_eq!(
            serde_json::from_value::<Property>(serialized).unwrap(),
            property
        );
    }
}

mod unknown_wire_values {
    #[cfg(feature = "calendars")]
    #[test]
    fn calendar_enums_degrade_unknown_values() {
        assert_eq!(
            serde_json::from_str::<crate::calendar_event::AlertAction>(r#""audio""#).unwrap(),
            crate::calendar_event::AlertAction::Unknown
        );
        assert_eq!(
            serde_json::from_str::<crate::calendar_event::RelativeTo>(r#""alarm""#).unwrap(),
            crate::calendar_event::RelativeTo::Unknown
        );
        assert_eq!(
            serde_json::from_str::<crate::calendar_event_notification::NotificationType>(
                r#""sent""#
            )
            .unwrap(),
            crate::calendar_event_notification::NotificationType::Unknown
        );
    }
}
