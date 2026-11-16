mod filter_wire {
    use crate::calendar_event::query::Filter as CEFilter;
    use serde_json::json;

    #[test]
    fn calendar_event_in_calendar_singular() {
        let filter = CEFilter::in_calendar("cal-1");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inCalendar": "cal-1"}));
    }

    #[test]
    fn calendar_event_in_calendars_plural() {
        let filter = CEFilter::in_calendars(["cal-1", "cal-2"]);
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inCalendars": ["cal-1", "cal-2"]}));
    }

    #[test]
    fn calendar_event_text_filter() {
        let filter = CEFilter::text("meeting");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"text": "meeting"}));
    }

    #[test]
    fn calendar_event_uid_filter() {
        let filter = CEFilter::uid("uid-abc");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"uid": "uid-abc"}));
    }
}
