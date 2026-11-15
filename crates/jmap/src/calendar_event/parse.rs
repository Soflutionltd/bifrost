pub(crate) type CalendarEventParseResponse =
    crate::core::parse::ParseResponse<Vec<super::CalendarEvent>>;

crate::define_parse_method!(
    CalendarEventParseRequest,
    super::CalendarEvent,
    crate::core::capability::CalendarsParse,
    CalendarEventParseResponse
);
