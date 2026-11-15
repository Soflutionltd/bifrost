//! Graph calendar JSON shapes.

use crate::types::{CalendarEmailAddress, CalendarRecipient};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphCalendar {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
    pub(crate) can_edit: Option<bool>,
    pub(crate) is_default_calendar: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSearchRequest<'a> {
    pub(crate) requests: Vec<GraphSearchEntityRequest<'a>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSearchEntityRequest<'a> {
    pub(crate) entity_types: Vec<&'a str>,
    pub(crate) query: GraphSearchQuery,
    pub(crate) from: u32,
    pub(crate) size: u32,
    pub(crate) fields: &'a [&'a str],
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSearchQuery {
    pub(crate) query_string: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSearchResponse {
    pub(crate) value: Vec<GraphSearchSet>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSearchSet {
    pub(crate) hits_containers: Vec<GraphSearchHitsContainer>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSearchHitsContainer {
    #[serde(default)]
    pub(crate) hits: Vec<GraphSearchHit>,
    pub(crate) more_results_available: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSearchHit {
    pub(crate) resource: Option<GraphEvent>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphEvent {
    pub(crate) id: String,
    pub(crate) subject: Option<String>,
    pub(crate) body: Option<GraphBody>,
    pub(crate) location: Option<GraphLocation>,
    pub(crate) start: Option<GraphDateTime>,
    pub(crate) end: Option<GraphDateTime>,
    pub(crate) is_all_day: Option<bool>,
    pub(crate) show_as: Option<String>,
    pub(crate) sensitivity: Option<String>,
    pub(crate) organizer: Option<CalendarRecipient>,
    pub(crate) attendees: Option<Vec<GraphAttendee>>,
    /// Present only on occurrence / exception events: the instant the
    /// occurrence ORIGINALLY started, before any override moved it -
    /// Graph's analog of iCalendar RECURRENCE-ID and of Google's
    /// `originalStartTime`.
    pub(crate) original_start: Option<String>,
    pub(crate) recurrence: Option<GraphRecurrence>,
    pub(crate) web_link: Option<String>,
    pub(crate) response_status: Option<Value>,
    pub(crate) is_cancelled: Option<bool>,
    pub(crate) change_key: Option<String>,
    /// The zone the event was created in (a Windows id, usually): the zone a
    /// UTC RRULE `UNTIL` is read in when a patch does not restate `start`.
    pub(crate) original_start_time_zone: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphEventPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) location: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start: Option<GraphDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end: Option<GraphDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) is_all_day: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) show_as: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sensitivity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) attendees: Option<Vec<GraphAttendee>>,
    /// `Some(None)` serializes as `null`, which clears the recurrence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recurrence: Option<Option<GraphRecurrence>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphRsvpAction {
    pub(crate) comment: Option<String>,
    pub(crate) send_response: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphDateTime {
    #[serde(rename = "dateTime", skip_serializing_if = "Option::is_none")]
    pub(crate) date_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) time_zone: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphLocation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) content: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphAttendee {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) email_address: Option<CalendarEmailAddress>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<GraphResponseStatus>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphResponseStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphRecurrence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pattern: Option<GraphRecurrencePattern>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) range: Option<GraphRecurrenceRange>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphRecurrencePattern {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) interval: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) month: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) day_of_month: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) days_of_week: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) first_day_of_week: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) index: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphRecurrenceRange {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recurrence_time_zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) number_of_occurrences: Option<u32>,
}
