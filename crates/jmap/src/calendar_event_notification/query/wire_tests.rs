mod query_wire {
    use serde_json::json;

    fn wire(value: &impl serde::Serialize) -> serde_json::Value {
        serde_json::to_value(value).expect("query fragments serialize")
    }

    /// draft-ietf-jmap-calendars (`CalendarEventNotification/query`).
    #[cfg(feature = "calendars")]
    #[test]
    fn calendar_event_notification_filter_and_comparator_property_names() {
        use crate::calendar_event_notification::query::{Comparator, Filter};

        assert_eq!(wire(&Filter::type_("created")), json!({"type": "created"}));
        assert_eq!(
            wire(&Filter::calendar_event_id("ev-1")),
            json!({"calendarEventId": "ev-1"})
        );

        assert_eq!(
            wire(&Comparator::created()),
            json!({"isAscending": true, "property": "created"})
        );
    }
}
