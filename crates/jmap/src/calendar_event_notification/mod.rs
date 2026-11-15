pub(crate) mod get;
pub(crate) mod query;
pub(crate) mod set;

use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum CalendarEventNotification {}
}
/// Strongly-typed CalendarEventNotification ID.
pub(crate) type CalendarEventNotificationId =
    crate::core::id::Id<marker::CalendarEventNotification>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CalendarEventNotification {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<CalendarEventNotificationId>,

    #[serde(rename = "created")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) created: Option<String>,

    #[serde(rename = "changedBy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) changed_by: Option<ChangedBy>,

    #[serde(rename = "calendarEventId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) calendar_event_id: Option<crate::calendar_event::CalendarEventId>,

    #[serde(rename = "isDraft")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_draft: Option<bool>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) type_: Option<NotificationType>,

    #[serde(rename = "event")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) event: Option<serde_json::Value>,

    #[serde(rename = "eventPatch")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) event_patch: Option<serde_json::Value>,
}

/// Uninhabitable - notifications are server-generated, only destroy is allowed.
#[derive(Debug, Clone, Serialize)]
pub(crate) enum CalendarEventNotificationCreate {}

#[derive(Debug, Clone, Serialize)]
pub(crate) enum CalendarEventNotificationPatch {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChangedBy {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,

    #[serde(rename = "email")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) email: Option<String>,

    #[serde(rename = "principalId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) principal_id: Option<crate::principal::PrincipalId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum NotificationType {
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "updated")]
    Updated,
    #[serde(rename = "destroyed")]
    Destroyed,
    #[serde(other)]
    Unknown,
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Created => "created",
        ChangedBy => "changedBy",
        CalendarEventId => "calendarEventId",
        IsDraft => "isDraft",
        Type => "type",
        Event => "event",
        EventPatch => "eventPatch",
    }
}

impl crate::core::Object for CalendarEventNotification {
    type Property = Property;
    type Id = CalendarEventNotificationId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for CalendarEventNotification {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for CalendarEventNotification {
    type GetArguments = ();
}

impl crate::core::set::SetObject for CalendarEventNotification {
    type Create = CalendarEventNotificationCreate;
    type Patch = CalendarEventNotificationPatch;
    type SetArguments = ();
}

crate::define_get_method!(
    CalendarEventNotificationGet,
    CalendarEventNotification,
    "CalendarEventNotification/get",
    crate::core::capability::Calendars
);
crate::define_set_method!(
    CalendarEventNotificationSet,
    CalendarEventNotification,
    "CalendarEventNotification/set",
    crate::core::capability::Calendars
);
crate::define_changes_method!(
    CalendarEventNotificationChanges,
    CalendarEventNotification,
    "CalendarEventNotification/changes",
    crate::core::capability::Calendars
);
crate::define_query_method!(
    CalendarEventNotificationQuery,
    CalendarEventNotification,
    "CalendarEventNotification/query",
    crate::core::capability::Calendars
);
crate::define_query_changes_method!(
    CalendarEventNotificationQueryChanges,
    CalendarEventNotification,
    "CalendarEventNotification/queryChanges",
    crate::core::capability::Calendars
);
