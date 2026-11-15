use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CalendarListResponse {
    pub(crate) items: Option<Vec<GoogleCalendarListEntry>>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GoogleCalendarListEntry {
    pub(crate) id: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) background_color: Option<String>,
    pub(crate) primary: Option<bool>,
    pub(crate) access_role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventsResponse {
    pub(crate) items: Option<Vec<GoogleEvent>>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GoogleEvent {
    pub(crate) id: Option<String>,
    pub(crate) etag: Option<String>,
    pub(crate) i_cal_uid: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) location: Option<String>,
    pub(crate) start: Option<GoogleEventTime>,
    pub(crate) end: Option<GoogleEventTime>,
    /// True when `end` is a placeholder, not a real end time.
    pub(crate) end_time_unspecified: Option<bool>,
    pub(crate) status: Option<String>,
    pub(crate) visibility: Option<String>,
    pub(crate) organizer: Option<GoogleOrganizer>,
    pub(crate) attendees: Option<Vec<GoogleAttendee>>,
    pub(crate) recurrence: Option<Vec<String>>,
    pub(crate) original_start_time: Option<GoogleEventTime>,
    pub(crate) transparency: Option<String>,
    pub(crate) html_link: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GoogleEventPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) location: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start: Option<GoogleEventTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end: Option<GoogleEventTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) attendees: Option<Vec<GoogleAttendee>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recurrence: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transparency: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GoogleEventTime {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) date: Option<String>,
    #[serde(rename = "dateTime", skip_serializing_if = "Option::is_none")]
    pub(crate) date_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) time_zone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GoogleOrganizer {
    pub(crate) email: Option<String>,
    pub(crate) display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GoogleAttendee {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) optional: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resource: Option<bool>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}
