//! Calendar and event primitives for the unified PIM surface.
//!
//! Providers expose JSCalendar, Google Calendar JSON, Microsoft Graph
//! events, or iCalendar through CalDAV. These types preserve native ids,
//! etags, recurrence text, and provenance while keeping the shared
//! Account surface small.

pub mod recurrence;
pub use recurrence::{RecurrenceDay, RecurrenceRule, RecurrenceRuleError, RecurrenceUntil};

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

/// Parsed calendar time, independent of any enclosing event flag.
///
/// Dates are all-day values. Their ends are exclusive: a single day on
/// 2026-06-02 starts on that date and ends on 2026-06-03. Local date-times
/// remain floating unless accompanied by a timezone. Named zones are retained
/// without requiring an IANA lookup, including provider-specific TZIDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTime {
    value: String,
    timezone: Option<String>,
    kind: EventTimeKind,
    local: Option<jiff::civil::DateTime>,
}

/// The parsed meaning of an event bound. Unknown observations are explicit,
/// so a malformed provider value cannot be mistaken for a valid appointment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventTimeKind {
    Date(jiff::civil::Date),
    Floating(jiff::civil::DateTime),
    Zoned(jiff::civil::DateTime),
    Offset(jiff::Timestamp),
    Unspecified,
    Invalid,
}

impl EventTime {
    /// Parse a caller-supplied date, local date-time or offset date-time.
    /// Missing and malformed values are refused here, before provider I/O.
    pub fn parse(
        value: impl Into<String>,
        timezone: Option<String>,
    ) -> Result<Self, EventTimeError> {
        let time = Self::from_observed(value, timezone);
        match time.kind {
            EventTimeKind::Invalid | EventTimeKind::Unspecified => Err(EventTimeError),
            _ => Ok(time),
        }
    }

    /// Retain an observed provider value, including missing or malformed data.
    /// Calendar users must still be able to inspect and edit metadata on a
    /// damaged meeting. Providers must reject invalid bounds when writing them.
    #[must_use]
    pub fn from_observed(value: impl Into<String>, timezone: Option<String>) -> Self {
        let value = value.into();
        let kind = if value.trim().is_empty() {
            EventTimeKind::Unspecified
        } else if value.len() == 10 {
            value
                .parse()
                .map(EventTimeKind::Date)
                .unwrap_or(EventTimeKind::Invalid)
        } else if let Ok(instant) = value.parse::<jiff::Timestamp>() {
            EventTimeKind::Offset(instant)
        } else if !value.contains(['Z', 'z', '+', '['])
            && !value
                .rsplit('T')
                .next()
                .is_some_and(|time| time.contains('-'))
        {
            value
                .parse()
                .map(|local| {
                    if timezone.as_deref().is_some_and(|zone| !zone.is_empty()) {
                        EventTimeKind::Zoned(local)
                    } else {
                        EventTimeKind::Floating(local)
                    }
                })
                .unwrap_or(EventTimeKind::Invalid)
        } else {
            EventTimeKind::Invalid
        };
        let local = match kind {
            EventTimeKind::Date(date) => Some(date.to_datetime(jiff::civil::Time::MIN)),
            EventTimeKind::Floating(local) | EventTimeKind::Zoned(local) => Some(local),
            EventTimeKind::Offset(at) => Some(
                value
                    .parse()
                    .unwrap_or_else(|_| jiff::tz::Offset::UTC.to_datetime(at)),
            ),
            _ => None,
        };
        Self {
            value,
            timezone,
            kind,
            local,
        }
    }

    /// Original wire spelling, retained for lossless projection and diagnostics.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    #[must_use]
    pub fn kind(&self) -> EventTimeKind {
        self.kind
    }

    #[must_use]
    pub fn is_all_day(&self) -> bool {
        matches!(self.kind, EventTimeKind::Date(_))
    }

    #[must_use]
    pub fn is_unspecified(&self) -> bool {
        matches!(self.kind, EventTimeKind::Unspecified)
    }

    /// Return this bound with a different provider timezone annotation.
    #[must_use]
    pub fn with_timezone(mut self, timezone: Option<String>) -> Self {
        if let EventTimeKind::Floating(local) | EventTimeKind::Zoned(local) = self.kind {
            self.kind = if timezone.as_deref().is_some_and(|zone| !zone.is_empty()) {
                EventTimeKind::Zoned(local)
            } else {
                EventTimeKind::Floating(local)
            };
        }
        self.timezone = timezone;
        self
    }

    /// An explicit instant only; floating values are never anchored implicitly.
    #[must_use]
    pub fn instant(&self) -> Option<jiff::Timestamp> {
        match self.kind {
            EventTimeKind::Offset(at) => Some(at),
            _ => None,
        }
    }

    /// The value's wall clock, retaining its own numeric offset rather than
    /// shifting it to UTC. Named-zone resolution remains provider-specific.
    #[must_use]
    pub fn local_datetime(&self) -> Option<jiff::civil::DateTime> {
        self.local
    }

    /// Refuse an absent or malformed observation at a write boundary.
    pub fn validate(&self) -> Result<(), EventTimeError> {
        match self.kind {
            EventTimeKind::Unspecified | EventTimeKind::Invalid => Err(EventTimeError),
            _ => Ok(()),
        }
    }

    /// Validate an exclusive end in the same time domain. Date-only ranges
    /// must cover at least one day; timed events may be explicit instants.
    pub fn validate_end(&self, end: &Self) -> Result<(), EventTimeError> {
        self.validate()?;
        end.validate()?;
        let ordered = match (self.kind, end.kind) {
            (EventTimeKind::Date(start), EventTimeKind::Date(end)) => end > start,
            (EventTimeKind::Offset(start), EventTimeKind::Offset(end)) => end >= start,
            (
                EventTimeKind::Floating(start) | EventTimeKind::Zoned(start),
                EventTimeKind::Floating(end_time) | EventTimeKind::Zoned(end_time),
            ) => self.timezone() != end.timezone() || end_time >= start,
            _ => false,
        };
        if ordered { Ok(()) } else { Err(EventTimeError) }
    }

    /// Convert a stored bound for an explicit all-day/timed patch instruction.
    /// Dates become midnight with their retained timezone annotation, or
    /// floating midnight when no timezone was supplied.
    #[must_use]
    pub fn converted(&self, all_day: bool) -> Self {
        let Some(local) = self.local_datetime() else {
            return self.clone();
        };
        let converted = if all_day {
            Self::from(local.date())
        } else if self.is_all_day() {
            Self::from(local)
        } else {
            return self.clone();
        };
        converted.with_timezone(self.timezone.clone())
    }
}

impl From<jiff::civil::Date> for EventTime {
    fn from(date: jiff::civil::Date) -> Self {
        Self {
            value: date.to_string(),
            timezone: None,
            kind: EventTimeKind::Date(date),
            local: Some(date.to_datetime(jiff::civil::Time::MIN)),
        }
    }
}

impl From<jiff::civil::DateTime> for EventTime {
    fn from(local: jiff::civil::DateTime) -> Self {
        Self {
            value: local.to_string(),
            timezone: None,
            kind: EventTimeKind::Floating(local),
            local: Some(local),
        }
    }
}

impl From<jiff::Timestamp> for EventTime {
    fn from(instant: jiff::Timestamp) -> Self {
        Self {
            value: instant.to_string(),
            timezone: None,
            kind: EventTimeKind::Offset(instant),
            local: Some(jiff::tz::Offset::UTC.to_datetime(instant)),
        }
    }
}

/// A caller supplied a missing or malformed calendar time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expected a date, a local date-time or an offset date-time")]
pub struct EventTimeError;

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
    /// Fires at a parsed offset relative to the event start or end.
    Relative {
        offset: ReminderOffset,
        relative_to: ReminderRelativeTo,
    },
    /// Fires at an absolute instant.
    Absolute(jiff::Timestamp),
    /// Malformed provider data retained for event-detail diagnostics.
    /// `relative_to` distinguishes a broken duration from a broken instant.
    Unparsed {
        value: String,
        relative_to: Option<ReminderRelativeTo>,
    },
}

impl ReminderTrigger {
    /// Decode a provider duration without inventing or dropping an alarm.
    #[must_use]
    pub fn relative(value: &str, relative_to: ReminderRelativeTo) -> Self {
        match value.parse() {
            Ok(offset) => Self::Relative {
                offset,
                relative_to,
            },
            Err(_) => Self::Unparsed {
                value: value.to_string(),
                relative_to: Some(relative_to),
            },
        }
    }

    /// Decode a provider instant without inventing or dropping an alarm.
    #[must_use]
    pub fn absolute(value: &str) -> Self {
        match value.parse() {
            Ok(at) => Self::Absolute(at),
            Err(_) => Self::Unparsed {
                value: value.to_string(),
                relative_to: None,
            },
        }
    }
}

/// A parsed reminder duration. Nominal days and weeks stay distinct from
/// exact hours so daylight-saving transitions do not silently shift alarms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReminderOffset(jiff::SpanFieldwise);

impl ReminderOffset {
    #[must_use]
    pub fn span(self) -> jiff::Span {
        self.0.0
    }
}

impl std::str::FromStr for ReminderOffset {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !value
            .strip_prefix(['-', '+'])
            .unwrap_or(value)
            .starts_with('P')
        {
            return Err("expected an ISO 8601 reminder duration");
        }
        let span = value
            .parse::<jiff::Span>()
            .map_err(|_| "invalid reminder duration")?;
        Ok(Self(span.fieldwise()))
    }
}

impl std::fmt::Display for ReminderOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.0.fmt(f)
    }
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

impl EventRecurrence {
    /// Parse the rule before interpreting or writing it. Raw text stays at the
    /// observation boundary so a malformed server rule does not erase a user's
    /// meeting or prevent unrelated scalar edits. Providers must not implement
    /// separate validity grammars over this retained text.
    pub fn parsed_rule(&self) -> Result<Option<RecurrenceRule>, RecurrenceRuleError> {
        self.rrule.as_deref().map(str::parse).transpose()
    }
}

/// Unified calendar event returned by calendar primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarEvent {
    pub id: EventId,
    pub calendar_id: CalendarId,
    pub native_id: String,
    pub uid: Option<String>,
    pub etag: Option<crate::EntityTag>,
    pub provenance: CalendarProvenance,
    pub title: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: EventTime,
    /// `None` when the provider gave no end: Google's
    /// `endTimeUnspecified`, an iCalendar VEVENT with neither DTEND nor a
    /// resolvable DURATION, a JSCalendar event with no `start` to measure a
    /// duration from, or a tombstone. Echoing it into `EventPatch::end`
    /// leaves the stored end alone.
    pub end: Option<EventTime>,
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

impl CalendarEvent {
    /// Whether the start is a date-only all-day value.
    #[must_use]
    pub fn is_all_day(&self) -> bool {
        self.start.is_all_day()
    }
}

/// Provider-side event range query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRange {
    pub calendar_id: CalendarId,
    pub start: EventTime,
    pub end: EventTime,
    pub page_cursor: Option<crate::PageCursor>,
    pub limit: Option<u32>,
}

impl EventRange {
    /// Construct a calendar window without pagination or a result limit.
    /// Bounds are retained verbatim; providers validate their supported time
    /// forms when executing the query. `admits` remains a conservative guard.
    #[must_use]
    pub fn new(calendar_id: CalendarId, start: EventTime, end: EventTime) -> Self {
        Self {
            calendar_id,
            start,
            end,
            page_cursor: None,
            limit: None,
        }
    }

    /// Defensive overlap guard for provider-selected candidates, not a recurrence
    /// expander or a replacement for the provider's range query. `true` means
    /// overlap is possible; it does not prove an occurrence exists in the window.
    /// Calendar membership and request validation remain the caller's job.
    ///
    /// Floating dates, local times, unknown ends and recurring masters retain
    /// the provider's decision. Calendar users expect matching appointments to
    /// remain visible when their timezone or recurrence cannot be resolved here.
    #[must_use]
    pub fn admits(&self, event: &CalendarEvent) -> bool {
        if event.is_all_day()
            || event
                .recurrence
                .rrule
                .as_ref()
                .is_some_and(|rule| !rule.is_empty())
            || !event.recurrence.rdate.is_empty()
        {
            // UNTIL bounds occurrence starts, not ends; RDATE and overrides can
            // also extend a series. Comparing the master loses valid meetings.
            return true;
        }
        let instant = EventTime::instant;
        let (Some(start), Some(end), Some(event_start)) = (
            instant(&self.start),
            instant(&self.end),
            instant(&event.start),
        ) else {
            return true;
        };
        let event_end = match event.end.as_ref() {
            Some(end) => match instant(end) {
                Some(end) => end,
                None => return true,
            },
            // Missing end can mean an unknown duration, not only an instant.
            None => return true,
        };
        if end <= start || event_end < event_start {
            return true;
        }
        // RFC 4791 section 9.9: instants include the window start; intervals
        // have exclusive ends. Neither shape includes the window end.
        if event_start == event_end {
            start <= event_start && event_start < end
        } else {
            event_start < end && event_end > start
        }
    }
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
    pub status: EventStatus,
    pub availability: EventAvailability,
    pub visibility: EventVisibility,
    pub organizer: Option<EventOrganizer>,
    pub attendees: Vec<EventAttendee>,
    pub recurrence: EventRecurrence,
}

impl EventCreate {
    /// Whether the start is a date-only all-day value.
    #[must_use]
    pub fn is_all_day(&self) -> bool {
        self.start.is_all_day()
    }
}

/// Partial event update payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventPatch {
    /// Caller-observed version. Providers must enforce this atomically or
    /// refuse with `Unsupported`; they must never silently discard it.
    /// `None` retains the provider's ordinary update behavior.
    pub expected_version: Option<crate::mutation::ServerVersion>,
    pub calendar_id: Option<CalendarId>,
    pub title: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub location: Option<Option<String>>,
    pub start: Option<EventTime>,
    pub end: Option<EventTime>,
    /// Explicit conversion of stored bounds. This is an operation, not the
    /// type of any supplied time: supplied dates and date-times carry that
    /// themselves. Providers may require both bounds for a conversion.
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
    pub page_cursor: Option<crate::PageCursor>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_time_classifies_once_and_retains_unknown_observations() {
        let date = EventTime::parse("2028-02-29", None).unwrap();
        assert!(matches!(date.kind(), EventTimeKind::Date(_)));
        assert!(date.is_all_day());
        let floating = EventTime::parse("2026-06-02T12:00:00", None).unwrap();
        assert!(matches!(floating.kind(), EventTimeKind::Floating(_)));
        assert_eq!(floating.instant(), None);
        let zoned = floating
            .clone()
            .with_timezone(Some("Custom/Enterprise".to_string()));
        assert!(matches!(zoned.kind(), EventTimeKind::Zoned(_)));
        assert_eq!(zoned.local_datetime(), floating.local_datetime());
        assert_eq!(zoned.timezone(), Some("Custom/Enterprise"));
        let offset = EventTime::parse("2026-06-02T12:00:00+02:00", None).unwrap();
        assert_eq!(
            offset.instant(),
            Some("2026-06-02T10:00:00Z".parse().unwrap())
        );
        assert_eq!(offset.local_datetime(), floating.local_datetime());
        for value in ["2026-02-30", "2026-06-02T99:00:00", "unknown"] {
            assert!(EventTime::parse(value, None).is_err());
            let observed = EventTime::from_observed(value, None);
            assert_eq!(observed.kind(), EventTimeKind::Invalid);
            assert_eq!(observed.value(), value);
            assert!(observed.validate().is_err());
        }
        assert!(EventTime::parse("", None).is_err());
        assert!(EventTime::from_observed("", None).is_unspecified());
    }

    #[test]
    fn event_time_end_validation_owns_shape_order_and_exclusive_dates() {
        for (start, end, valid) in [
            ("2026-06-02", "2026-06-03", true),
            ("2026-06-02", "2026-06-02", false),
            ("2026-06-02", "2026-06-01", false),
            ("2026-06-02", "2026-06-03T00:00:00Z", false),
            ("2026-06-02T12:00:00", "2026-06-02T12:00:00", true),
            ("2026-06-02T12:00:00", "2026-06-02T11:00:00", false),
            ("2026-06-02T12:00:00", "2026-06-02T13:00:00Z", false),
            ("2026-06-02T12:00:00+02:00", "2026-06-02T10:30:00Z", true),
            ("2026-06-02T12:00:00+02:00", "2026-06-02T09:30:00Z", false),
            ("unknown", "2026-06-03", false),
            ("2026-06-02", "", false),
        ] {
            assert_eq!(
                time(start).validate_end(&time(end)).is_ok(),
                valid,
                "{start} / {end}"
            );
        }
    }

    #[test]
    fn reminder_triggers_have_parsed_timing_and_preserve_broken_observations() {
        let days: ReminderOffset = "-P1D".parse().unwrap();
        let hours: ReminderOffset = "-PT24H".parse().unwrap();
        assert_ne!(days, hours);
        assert_eq!(days.span().get_days(), -1);
        assert_eq!(hours.span().get_hours(), -24);
        assert_eq!(
            ReminderTrigger::relative("-P1D", ReminderRelativeTo::End),
            ReminderTrigger::Relative {
                offset: days,
                relative_to: ReminderRelativeTo::End
            }
        );
        assert!(matches!(
            ReminderTrigger::absolute("2026-06-02T12:00:00Z"),
            ReminderTrigger::Absolute(_)
        ));
        assert!(matches!(
            ReminderTrigger::absolute("broken"),
            ReminderTrigger::Unparsed {
                relative_to: None,
                ..
            }
        ));
        assert!(matches!(
            ReminderTrigger::relative("broken", ReminderRelativeTo::Start),
            ReminderTrigger::Unparsed {
                relative_to: Some(ReminderRelativeTo::Start),
                ..
            }
        ));
    }

    fn time(value: &str) -> EventTime {
        EventTime::from_observed(value.to_string(), None)
    }

    fn event(start: &str, end: &str, expected_all_day: bool) -> CalendarEvent {
        assert_eq!(time(start).is_all_day(), expected_all_day);
        CalendarEvent {
            id: EventId("/cal/one.ics".to_string()),
            calendar_id: CalendarId("/cal/".to_string()),
            native_id: "/cal/one.ics".to_string(),
            uid: None,
            etag: None,
            provenance: CalendarProvenance {
                provider: ProtocolKind::CalDav,
                native: "/cal/one.ics".to_string(),
                calendar_native: None,
            },
            title: None,
            description: None,
            location: None,
            start: time(start),
            end: Some(time(end)).filter(|end| !end.value.is_empty()),
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            self_response: RsvpStatus::Unknown,
            organizer: None,
            attendees: Vec::new(),
            reminders: Vec::new(),
            recurrence: EventRecurrence::default(),
            html_link: None,
            raw_ical: None,
        }
    }

    fn range() -> EventRange {
        EventRange {
            calendar_id: CalendarId("/cal/".to_string()),
            start: time("2026-06-02T00:00:00Z"),
            end: time("2026-06-03T00:00:00Z"),
            page_cursor: None,
            limit: None,
        }
    }

    #[test]
    fn range_admits_interval_and_instant_boundaries() {
        let range = range();
        for (start, end, admitted) in [
            ("2026-06-01T23:00:00Z", "2026-06-02T01:00:00Z", true),
            ("2026-06-01T23:00:00Z", "2026-06-02T00:00:00Z", false),
            ("2026-06-03T00:00:00Z", "2026-06-03T01:00:00Z", false),
            ("2026-06-02T00:00:00Z", "2026-06-02T00:00:00Z", true),
            ("2026-06-03T00:00:00Z", "2026-06-03T00:00:00Z", false),
            ("2026-06-01T23:59:59Z", "2026-06-01T23:59:59Z", false),
            (
                "2026-06-02T02:00:00+02:00",
                "2026-06-02T02:00:00+02:00",
                true,
            ),
        ] {
            assert_eq!(
                range.admits(&event(start, end, false)),
                admitted,
                "{start} / {end}"
            );
        }
    }

    #[test]
    fn range_retains_provider_timezone_and_unknown_end_decisions() {
        let range = range();
        for (start, end, all_day) in [
            ("2026-06-01", "2026-06-02", true),
            ("2026-06-01T23:30:00", "2026-06-01T23:45:00", false),
            ("2026-06-01T23:00:00Z", "", false),
            ("2026-06-01T23:00:00Z", "unknown", false),
            ("", "", false),
        ] {
            assert!(range.admits(&event(start, end, all_day)), "{start} / {end}");
        }
        let mut floating_range = range;
        floating_range.start = time("2026-06-02T00:00:00");
        assert!(floating_range.admits(&event(
            "2026-06-01T23:00:00Z",
            "2026-06-02T00:00:00Z",
            false,
        )));
    }

    #[test]
    fn range_retains_recurring_masters_and_checks_detached_instances() {
        let range = range();
        let mut master = event("2026-05-01T00:00:00Z", "2026-05-01T01:00:00Z", false);
        master.recurrence.rrule = Some("FREQ=DAILY;UNTIL=20260601T233000Z".to_string());
        assert!(range.admits(&master));
        master.recurrence.rrule = None;
        master.recurrence.rdate.push("20260602T120000Z".to_string());
        assert!(range.admits(&master));
        master.recurrence.rdate.clear();
        master.recurrence.recurrence_id = Some("20260501T000000Z".to_string());
        assert!(!range.admits(&master));
    }
}
