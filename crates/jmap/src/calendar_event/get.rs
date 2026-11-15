use crate::core::field::Field;

use super::{CalendarEvent, CalendarEventId};

impl CalendarEvent {
    /// The calendar event ID. Returns an owned `CalendarEventId` (one
    /// allocation): JSON-map storage means there is no `&CalendarEventId`
    /// to borrow.
    pub(crate) fn id(&self) -> Option<CalendarEventId> {
        self.properties
            .get("id")?
            .as_str()
            .map(CalendarEventId::from)
    }

    pub(crate) fn take_id(&mut self) -> CalendarEventId {
        self.properties
            .remove("id")
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(CalendarEventId::from(s)),
                _ => None,
            })
            .unwrap_or_else(|| CalendarEventId::new(""))
    }

    pub(crate) fn uid(&self) -> Option<&str> {
        self.properties.get("uid")?.as_str()
    }

    pub(crate) fn calendar_ids(
        &self,
    ) -> Option<crate::core::object_set::ObjectSet<crate::calendar::CalendarId>> {
        serde_json::from_value(self.properties.get("calendarIds")?.clone()).ok()
    }

    pub(crate) fn is_draft(&self) -> Option<bool> {
        self.properties.get("isDraft")?.as_bool()
    }

    pub(crate) fn title(&self) -> Option<&str> {
        self.properties.get("title")?.as_str()
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.properties.get("description")?.as_str()
    }

    pub(crate) fn description_content_type(&self) -> Option<&str> {
        self.properties.get("descriptionContentType")?.as_str()
    }

    pub(crate) fn created(&self) -> Option<&str> {
        self.properties.get("created")?.as_str()
    }

    pub(crate) fn updated(&self) -> Option<&str> {
        self.properties.get("updated")?.as_str()
    }

    pub(crate) fn start(&self) -> Option<&str> {
        self.properties.get("start")?.as_str()
    }

    pub(crate) fn duration(&self) -> Option<&str> {
        self.properties.get("duration")?.as_str()
    }

    pub(crate) fn time_zone(&self) -> Field<&str> {
        match self.properties.get("timeZone") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_str() {
                Some(s) => Field::Value(s),
                None => Field::Null,
            },
        }
    }

    pub(crate) fn show_without_time(&self) -> Option<bool> {
        self.properties.get("showWithoutTime")?.as_bool()
    }

    pub(crate) fn status(&self) -> Option<&str> {
        self.properties.get("status")?.as_str()
    }

    pub(crate) fn free_busy_status(&self) -> Option<&str> {
        self.properties.get("freeBusyStatus")?.as_str()
    }

    pub(crate) fn privacy(&self) -> Option<&str> {
        self.properties.get("privacy")?.as_str()
    }

    pub(crate) fn recurrence_id(&self) -> Option<&str> {
        self.properties.get("recurrenceId")?.as_str()
    }

    pub(crate) fn recurrence_rules(&self) -> Option<&Vec<serde_json::Value>> {
        self.properties.get("recurrenceRules")?.as_array()
    }

    pub(crate) fn recurrence_overrides(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("recurrenceOverrides")?.as_object()
    }

    pub(crate) fn excluded_recurrence_rules(&self) -> Option<&Vec<serde_json::Value>> {
        self.properties.get("excludedRecurrenceRules")?.as_array()
    }

    pub(crate) fn excluded_dates(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("excludedDates")?.as_object()
    }

    pub(crate) fn priority(&self) -> Option<u64> {
        self.properties.get("priority")?.as_u64()
    }

    pub(crate) fn color(&self) -> Field<&str> {
        match self.properties.get("color") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_str() {
                Some(s) => Field::Value(s),
                None => Field::Null,
            },
        }
    }

    pub(crate) fn locale(&self) -> Field<&str> {
        match self.properties.get("locale") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_str() {
                Some(s) => Field::Value(s),
                None => Field::Null,
            },
        }
    }

    pub(crate) fn keywords(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("keywords")?.as_object()
    }

    pub(crate) fn categories(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("categories")?.as_object()
    }

    pub(crate) fn prod_id(&self) -> Option<&str> {
        self.properties.get("prodId")?.as_str()
    }

    pub(crate) fn reply_to(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("replyTo")?.as_object()
    }

    pub(crate) fn participants(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("participants")?.as_object()
    }

    pub(crate) fn use_default_alerts(&self) -> Option<bool> {
        self.properties.get("useDefaultAlerts")?.as_bool()
    }

    pub(crate) fn alerts(&self) -> Field<&serde_json::Map<String, serde_json::Value>> {
        match self.properties.get("alerts") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_object() {
                Some(m) => Field::Value(m),
                None => Field::Null,
            },
        }
    }

    pub(crate) fn locations(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("locations")?.as_object()
    }

    pub(crate) fn virtual_locations(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("virtualLocations")?.as_object()
    }

    pub(crate) fn links(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("links")?.as_object()
    }

    pub(crate) fn related_to(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("relatedTo")?.as_object()
    }

    pub(crate) fn method(&self) -> Option<&str> {
        self.properties.get("method")?.as_str()
    }

    pub(crate) fn sequence(&self) -> Option<u64> {
        self.properties.get("sequence")?.as_u64()
    }

    pub(crate) fn property(&self, name: &str) -> Option<&serde_json::Value> {
        self.properties.get(name)
    }

    pub(crate) fn as_properties(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.properties
    }
}
