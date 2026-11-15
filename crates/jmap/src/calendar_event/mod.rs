//! CalendarEvent wraps a JSCalendar Event (RFC 8984) object.

pub(crate) mod get;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod set;

use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum CalendarEvent {}
}
/// Strongly-typed CalendarEvent ID.
pub(crate) type CalendarEventId = crate::core::id::Id<marker::CalendarEvent>;

crate::json_object_struct!(
    CalendarEvent,
    CalendarEventCreate,
    CalendarEventPatch,
    "a JSCalendar object"
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Alert {
    #[serde(rename = "@type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) type_: Option<String>,

    #[serde(rename = "trigger")]
    pub(crate) trigger: AlertTrigger,

    #[serde(rename = "action")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action: Option<AlertAction>,

    #[serde(rename = "acknowledged")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) acknowledged: Option<String>,

    #[serde(rename = "relatedTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) related_to: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "@type")]
#[non_exhaustive]
pub(crate) enum AlertTrigger {
    #[serde(rename = "OffsetTrigger")]
    OffsetTrigger {
        #[serde(rename = "offset")]
        offset: String,

        #[serde(rename = "relativeTo")]
        #[serde(skip_serializing_if = "Option::is_none")]
        relative_to: Option<RelativeTo>,
    },
    #[serde(rename = "AbsoluteTrigger")]
    AbsoluteTrigger {
        #[serde(rename = "when")]
        when: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum AlertAction {
    #[serde(rename = "display")]
    Display,
    #[serde(rename = "email")]
    Email,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum RelativeTo {
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "end")]
    End,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct GetArguments {
    #[serde(rename = "recurrenceOverridesBefore")]
    #[serde(skip_serializing_if = "Option::is_none")]
    // This is UTCDateTime, whereas event start is LocalDateTime and duration
    // is an RFC 5545 duration. Their typed migration must include the account
    // conversion layer so validation does not diverge between reads and writes.
    pub(crate) recurrence_overrides_before: Option<String>,

    #[serde(rename = "recurrenceOverridesAfter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recurrence_overrides_after: Option<String>,

    #[serde(rename = "reduceParticipants")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reduce_participants: Option<bool>,

    #[serde(rename = "timeZone")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) time_zone: Option<String>,
}

impl GetArguments {
    pub(crate) fn recurrence_overrides_before(&mut self, before: impl Into<String>) -> &mut Self {
        self.recurrence_overrides_before = Some(before.into());
        self
    }

    pub(crate) fn recurrence_overrides_after(&mut self, after: impl Into<String>) -> &mut Self {
        self.recurrence_overrides_after = Some(after.into());
        self
    }

    pub(crate) fn reduce_participants(&mut self, reduce: bool) -> &mut Self {
        self.reduce_participants = Some(reduce);
        self
    }

    pub(crate) fn time_zone(&mut self, tz: impl Into<String>) -> &mut Self {
        self.time_zone = Some(tz.into());
        self
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct SetArguments {
    #[serde(rename = "sendSchedulingMessages")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) send_scheduling_messages: Option<bool>,
}

impl SetArguments {
    pub(crate) fn send_scheduling_messages(&mut self, send: bool) -> &mut Self {
        self.send_scheduling_messages = Some(send);
        self
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct QueryArguments {
    #[serde(rename = "expandRecurrences")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expand_recurrences: Option<bool>,

    #[serde(rename = "timeZone")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) time_zone: Option<String>,
}

impl QueryArguments {
    pub(crate) fn expand_recurrences(&mut self, expand: bool) -> &mut Self {
        self.expand_recurrences = Some(expand);
        self
    }

    pub(crate) fn time_zone(&mut self, tz: impl Into<String>) -> &mut Self {
        self.time_zone = Some(tz.into());
        self
    }
}

crate::define_open_property_enum! {
    /// Property names for CalendarEvent/get `properties` lists.
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Uid => "uid",
        CalendarIds => "calendarIds",
        IsDraft => "isDraft",
        Title => "title",
        Description => "description",
        DescriptionContentType => "descriptionContentType",
        Created => "created",
        Updated => "updated",
        Start => "start",
        Duration => "duration",
        TimeZone => "timeZone",
        ShowWithoutTime => "showWithoutTime",
        Status => "status",
        FreeBusyStatus => "freeBusyStatus",
        RecurrenceId => "recurrenceId",
        RecurrenceIdTimeZone => "recurrenceIdTimeZone",
        RecurrenceRules => "recurrenceRules",
        RecurrenceOverrides => "recurrenceOverrides",
        ExcludedRecurrenceRules => "excludedRecurrenceRules",
        Priority => "priority",
        Color => "color",
        Locale => "locale",
        Keywords => "keywords",
        Categories => "categories",
        ProdId => "prodId",
        ReplyTo => "replyTo",
        Participants => "participants",
        UseDefaultAlerts => "useDefaultAlerts",
        Alerts => "alerts",
        Locations => "locations",
        VirtualLocations => "virtualLocations",
        Links => "links",
        RelatedTo => "relatedTo",
        ExcludedDates => "excludedDates",
        Localizations => "localizations",
        Method => "method",
        Sequence => "sequence",
    }
}

impl crate::core::Object for CalendarEvent {
    type Property = Property;
    type Id = CalendarEventId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for CalendarEvent {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for CalendarEvent {
    type GetArguments = GetArguments;
}

impl crate::core::set::SetObject for CalendarEvent {
    type Create = CalendarEventCreate;
    type Patch = CalendarEventPatch;
    type SetArguments = SetArguments;
}

crate::define_get_method!(
    CalendarEventGet,
    CalendarEvent,
    "CalendarEvent/get",
    crate::core::capability::Calendars
);
crate::define_set_method!(
    CalendarEventSet,
    CalendarEvent,
    "CalendarEvent/set",
    crate::core::capability::Calendars
);
crate::define_changes_method!(
    CalendarEventChanges,
    CalendarEvent,
    "CalendarEvent/changes",
    crate::core::capability::Calendars
);
crate::define_query_method!(
    CalendarEventQuery,
    CalendarEvent,
    "CalendarEvent/query",
    crate::core::capability::Calendars
);
crate::define_query_changes_method!(
    CalendarEventQueryChanges,
    CalendarEvent,
    "CalendarEvent/queryChanges",
    crate::core::capability::Calendars
);
crate::define_copy_method!(
    CalendarEventCopy,
    CalendarEvent,
    "CalendarEvent/copy",
    crate::core::capability::Calendars
);

impl CalendarEventGet {
    #[must_use]
    pub(crate) fn recurrence_overrides_before(mut self, before: impl Into<String>) -> Self {
        self.arguments().recurrence_overrides_before(before);
        self
    }

    #[must_use]
    pub(crate) fn recurrence_overrides_after(mut self, after: impl Into<String>) -> Self {
        self.arguments().recurrence_overrides_after(after);
        self
    }

    #[must_use]
    pub(crate) fn reduce_participants(mut self, reduce: bool) -> Self {
        self.arguments().reduce_participants(reduce);
        self
    }

    #[must_use]
    pub(crate) fn time_zone(mut self, tz: impl Into<String>) -> Self {
        self.arguments().time_zone(tz);
        self
    }
}

impl CalendarEventSet {
    #[must_use]
    pub(crate) fn send_scheduling_messages(mut self, send: bool) -> Self {
        self.arguments().send_scheduling_messages(send);
        self
    }
}

impl CalendarEventQuery {
    #[must_use]
    pub(crate) fn expand_recurrences(mut self, expand: bool) -> Self {
        self.arguments().expand_recurrences(expand);
        self
    }

    #[must_use]
    pub(crate) fn time_zone(mut self, tz: impl Into<String>) -> Self {
        self.arguments().time_zone(tz);
        self
    }
}
