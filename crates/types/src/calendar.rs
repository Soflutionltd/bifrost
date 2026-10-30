//! Calendar and event primitives for the unified PIM surface.
//!
//! Providers expose JSCalendar, Google Calendar JSON, Microsoft Graph
//! events, or iCalendar through CalDAV. These types preserve native ids,
//! etags, recurrence text, and provenance while keeping the shared
//! Account surface small.

use crate::cursor::ProtocolKind;

/// Engine-facing calendar identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CalendarId(pub String);

impl From<String> for CalendarId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for CalendarId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Engine-facing event identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(pub String);

/// Wire-level provenance for a calendar or event id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CalendarProvenance {
    /// Which protocol family minted the native id.
    pub provider: ProtocolKind,
    /// Native id string used by the provider on the wire.
    pub native: String,
    /// Native calendar id, when the provider scopes events under a
    /// calendar collection.
    pub calendar_native: Option<String>,
}

/// Calendar collection exposed by a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Calendar {
    pub id: CalendarId,
    pub native_id: String,
    pub name: String,
    pub color: Option<String>,
    pub provenance: CalendarProvenance,
    pub is_default: bool,
    pub can_create_events: bool,
    pub can_update_events: bool,
    pub can_delete_events: bool,
}

/// Event transparency / availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventAvailability {
    Busy,
    Free,
    Tentative,
    OutOfOffice,
    Unknown,
}

/// Event visibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventVisibility {
    Default,
    Public,
    Private,
    Confidential,
}

/// Event lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventStatus {
    Confirmed,
    Tentative,
    Cancelled,
    Unknown,
}

/// Attendee participation status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RsvpStatus {
    NeedsAction,
    Accepted,
    Declined,
    Tentative,
    Delegated,
    Unknown,
}

/// What an RSVP carries to the organizer beyond the status itself.
///
/// The default is the silent status change `AccountCalendar::event_rsvp`
/// performs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RsvpReply {
    /// Note sent along with the response.
    pub comment: Option<String>,
    /// Whether the organizer is notified. Protocols without this switch
    /// keep their own scheduling behaviour.
    pub send_response: bool,
    /// Alternative slot offered with a tentative or declined response.
    pub proposed_time: Option<ProposedTime>,
}

/// Alternative meeting slot proposed by an invitee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposedTime {
    pub start: jiff::Timestamp,
    pub end: jiff::Timestamp,
}

/// Attendee role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AttendeeRole {
    Required,
    Optional,
    Resource,
    Chair,
    Unknown,
}

/// Event timestamp represented as an RFC 3339 date-time string, or a
/// provider-local all-day date in `YYYY-MM-DD` form when `is_all_day`
/// is true.
///
/// All-day interpretation contract: for an all-day event `start` is the
/// first day (inclusive) and `end` is **exclusive** - it names the first
/// day *after* the last day the event covers, matching iCalendar's
/// non-inclusive `DTEND` and Google Calendar's exclusive end date. A
/// single all-day event on 2026-06-02 therefore has `start` `"2026-06-02"`
/// and `end` `"2026-06-03"`. Providers must project all-day ends into this
/// exclusive form (bifrost-caldav no longer decrements the iCalendar
/// `DTEND`, bifrost-google passes Google's exclusive end through verbatim).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTime {
    pub value: String,
    pub timezone: Option<String>,
}

/// Whether a relative reminder offset is measured from the event start
/// or end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReminderRelativeTo {
    Start,
    End,
}

/// When a reminder fires. Faithfully preserves the provider's trigger
/// timing for event-detail display.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReminderTrigger {
    /// Fires at an offset relative to the event start or end. `offset`
    /// is an RFC 5545 / ISO 8601 signed duration text (e.g. `-PT15M`
    /// for fifteen minutes before the reference point).
    Relative {
        offset: String,
        relative_to: ReminderRelativeTo,
    },
    /// Fires at an absolute instant, as an RFC 3339 / iCalendar UTC
    /// date-time string.
    Absolute(String),
}

/// A reminder / alarm attached to an event.
///
/// Read-only projection surfaced for event-detail display. Populated
/// from iCalendar `VALARM` components (CalDAV) and JSCalendar `alerts`
/// (JMAP); providers that do not expose reminders leave
/// `CalendarEvent::reminders` empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventReminder {
    /// When the reminder fires.
    pub trigger: ReminderTrigger,
    /// Provider action (`DISPLAY`, `EMAIL`, `AUDIO`, ...) when supplied.
    pub action: Option<String>,
}

/// Event attendee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventAttendee {
    pub email: String,
    pub name: Option<String>,
    pub role: AttendeeRole,
    pub status: RsvpStatus,
}

/// Event organizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOrganizer {
    pub email: String,
    pub name: Option<String>,
}

/// Canonical recurrence payload. `rrule`, `rdate`, and `exdate` use
/// RFC 5545 text forms. `recurrence_id` identifies an overridden
/// occurrence using the provider's canonical string for that instance.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventRecurrence {
    pub rrule: Option<String>,
    pub rdate: Vec<String>,
    pub exdate: Vec<String>,
    pub recurrence_id: Option<String>,
}

/// Unified calendar event returned by calendar primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarEvent {
    pub id: EventId,
    pub calendar_id: CalendarId,
    pub native_id: String,
    pub uid: Option<String>,
    pub etag: Option<String>,
    pub provenance: CalendarProvenance,
    pub title: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: EventTime,
    pub end: EventTime,
    pub is_all_day: bool,
    pub status: EventStatus,
    pub availability: EventAvailability,
    pub visibility: EventVisibility,
    /// The opened account user's participation status when the provider
    /// exposes it independently from attendee rows.
    pub self_response: RsvpStatus,
    pub organizer: Option<EventOrganizer>,
    pub attendees: Vec<EventAttendee>,
    /// Reminders / alarms attached to the event. Read surface for
    /// event-detail display; empty when the provider exposes none.
    pub reminders: Vec<EventReminder>,
    pub recurrence: EventRecurrence,
    pub html_link: Option<String>,
    pub raw_ical: Option<String>,
}

/// Provider-side event range query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRange {
    pub calendar_id: CalendarId,
    pub start: EventTime,
    pub end: EventTime,
    pub page_cursor: Option<Vec<u8>>,
    pub limit: Option<u32>,
}

/// Event creation payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCreate {
    pub calendar_id: CalendarId,
    pub title: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: EventTime,
    pub end: EventTime,
    pub is_all_day: bool,
    pub status: EventStatus,
    pub availability: EventAvailability,
    pub visibility: EventVisibility,
    pub organizer: Option<EventOrganizer>,
    pub attendees: Vec<EventAttendee>,
    pub recurrence: EventRecurrence,
}

/// Partial event update payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventPatch {
    pub calendar_id: Option<CalendarId>,
    pub title: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub location: Option<Option<String>>,
    pub start: Option<EventTime>,
    pub end: Option<EventTime>,
    pub is_all_day: Option<bool>,
    pub status: Option<EventStatus>,
    pub availability: Option<EventAvailability>,
    pub visibility: Option<EventVisibility>,
    pub attendees: Option<Vec<EventAttendee>>,
    pub recurrence: Option<EventRecurrence>,
}

/// Provider-side event search request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSearchRequest {
    pub query: String,
    pub calendar_id: Option<CalendarId>,
    pub page_cursor: Option<Vec<u8>>,
    pub limit: Option<u32>,
    /// When `false` (the default) events whose status is
    /// [`EventStatus::Cancelled`] are excluded from the results; when `true`
    /// they are returned. Every `event_search` implementation must honour it.
    pub include_cancelled: bool,
}

impl EventSearchRequest {
    #[must_use]
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            calendar_id: None,
            page_cursor: None,
            limit: None,
            include_cancelled: false,
        }
    }
}
