//! JSCalendar (RFC 8984) <-> shared calendar types.
//!
//! Every mapping decision in this file was derived STATICALLY, from the RFCs
//! and the crate's own types. No conforming server was ever asked, because
//! the project's testing rules keep live endpoints out of this workspace, so
//! "a conforming server accepts this shape" is a reading of the spec rather
//! than an observation. Treat the mappings accordingly: an in-process round
//! trip through these functions proves self-consistency, not interop. The
//! all-day `DATE` handling is the standing example of the class a self-round
//! trip cannot catch - it encoded and decoded symmetrically while being
//! wrong on the wire. When one of these mappings is disputed, the evidence
//! that settles it is a real server's response, and that evidence has to be
//! gathered downstream.

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFuture, AccountOperation,
    AttendeeRole, Calendar, CalendarEvent, CalendarId, CalendarProvenance, Cause, DiagnosticText,
    EventAttendee, EventAvailability, EventCreate, EventId, EventOrganizer, EventPatch, EventRange,
    EventRecurrence, EventReminder, EventSearchRequest, EventStatus, EventTime, EventVisibility,
    Page, Protocol, ProtocolKind, ReminderRelativeTo, ReminderTrigger, RequestCause,
    RequestErrorKind, RsvpStatus,
};
use jiff::tz::{Offset, TimeZone};
use jiff::{SignedDuration, Span, Timestamp, civil};
use serde_json::{Map, Value, json};

use crate::account::Account as JmapProtoAccount;
use crate::calendar::{
    Calendar as JmapCalendar, CalendarGet, CalendarId as JmapCalendarId, CalendarRights,
};
use crate::calendar_event::query::Filter as EventFilter;
use crate::calendar_event::{
    CalendarEvent as JmapCalendarEvent, CalendarEventCreate, CalendarEventGet, CalendarEventId,
    CalendarEventPatch, CalendarEventSet,
};
use crate::core::SetCreate;
use crate::core::query::Filter as QueryFilter;
use crate::core::transport::HttpTransport;
use crate::sync::page_cursor::PageKind;

type CalendarAccount<T> = JmapProtoAccount<T>;

const PAGE_LIMIT: usize = 250;

pub(crate) fn calendars_list<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::CalendarsList)?;
        let response = calendars
            .call(CalendarGet::new())
            .await
            .map_err(to_acct_err(AccountOperation::CalendarsList))?;
        Ok(response
            .into_list()
            .into_iter()
            .map(calendar_from_jmap)
            .collect())
    })
}

pub(crate) fn events_in_range<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    range: EventRange,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventsInRange)?;
        let cursor = CalendarPages::decode_page_cursor(
            range.page_cursor.clone(),
            AccountOperation::EventsInRange,
        )?;
        let filter = range_filter(&range)?;
        let query = crate::calendar_event::CalendarEventQuery::new()
            .filter(filter)
            .limit(limit(range.limit))
            .calculate_total(true);
        let query = CalendarPages::anchor_query(query, cursor.as_ref());
        let response = calendars
            .call(query)
            .await
            .map_err(CalendarPages::page_call_err(
                AccountOperation::EventsInRange,
                cursor.is_some(),
            ))?;
        // Before the total, before the cursor, before hydration: an
        // inconsistent continuation must expose no items and no successor.
        CalendarPages::verify_query_state(
            cursor.as_ref(),
            response.query_state(),
            AccountOperation::EventsInRange,
        )?;
        let total = response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::EventsInRange))
            .transpose()?;
        let next_cursor = CalendarPages::next_cursor(
            cursor.as_ref(),
            response.position(),
            response.ids(),
            total,
            response.query_state(),
            AccountOperation::EventsInRange,
        )?;
        let hydrated = get_events(
            &calendars,
            response.into_ids(),
            AccountOperation::EventsInRange,
        )
        .await?;
        let events = hydrated
            .events
            .into_iter()
            .filter(|event| event_in_range(event, &range.start, &range.end))
            .collect::<Vec<_>>();
        Ok(Page {
            truncated: false,
            items: events,
            next_cursor,
            estimated_total: total,
            failed_ids: hydrated.failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

fn range_filter(range: &EventRange) -> Result<QueryFilter<EventFilter>, AccountError> {
    Ok(QueryFilter::and(vec![
        QueryFilter::from(EventFilter::in_calendar(range.calendar_id.0.clone())),
        QueryFilter::from(EventFilter::after(jmap_utc_filter_time(
            &range.start,
            "start",
        )?)),
        QueryFilter::from(EventFilter::before(jmap_utc_filter_time(
            &range.end, "end",
        )?)),
    ]))
}

pub(crate) fn get<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventId,
) -> AccountFuture<Result<CalendarEvent, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventGet)?;
        // A single-event read reports a conversion failure as itself. The
        // per-item `failed_ids` lane exists so one unrepresentable event
        // cannot destroy a page of neighbours; with a page of one there is
        // no neighbour to protect, and swallowing the reason would leave
        // the caller with "no event" for an event the server did return.
        let raw = get_raw_event(
            &calendars,
            CalendarEventId::new(event.0),
            AccountOperation::EventGet,
        )
        .await?;
        event_from_jmap(raw, AccountOperation::EventGet)
    })
}

pub(crate) fn create<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventCreate,
) -> AccountFuture<Result<EventId, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventCreate)?;
        validate_shared_recurrence(
            &event.recurrence,
            Some(&event.start),
            event.is_all_day,
            AccountOperation::EventCreate,
        )?;
        validate_shared_attendees(&event.attendees, AccountOperation::EventCreate)?;
        let mut set = CalendarEventSet::new();
        let create_id = set.create_item(jmap_create_from_event(
            &event,
            AccountOperation::EventCreate,
        )?);
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventCreate))?;
        let event = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::EventCreate))?;
        let id = event
            .id()
            .map(CalendarEventId::into_string)
            .unwrap_or_default();
        Ok(EventId(id))
    })
}

pub(crate) fn update<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventId,
    patch: EventPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let expected = super::mutation::caller_state(
            patch.expected_version.as_ref(),
            AccountOperation::EventUpdate,
        )?;
        let calendars = require_calendars(calendars, AccountOperation::EventUpdate)?;
        let id = CalendarEventId::new(event.0);
        // A patch that changes only the recurrence rule carries neither the
        // event start nor its all-day flag, yet `UNTIL` conversion and the
        // start/duration write both need them. Read the current server state
        // for exactly the fields the patch leaves unset rather than defaulting
        // to a timed, floating event and mangling the value. An attendee patch
        // needs the read too: JSCalendar `participants` holds the owner
        // participant beside the attendees, so writing the attendee list as a
        // whole-map replacement without carrying the owner over would silently
        // delete the organizer from the event.
        let context = if event_patch_needs_current(&patch) {
            let raw = get_raw_event(&calendars, id.clone(), AccountOperation::EventUpdate).await?;
            event_context_from_patch(&patch, Some(&raw))
        } else {
            event_context_from_patch(&patch, None)
        };
        if let Some(recurrence) = &patch.recurrence {
            validate_shared_recurrence(
                recurrence,
                context.start.as_ref(),
                context.is_all_day,
                AccountOperation::EventUpdate,
            )?;
        }
        if let Some(attendees) = &patch.attendees {
            validate_shared_attendees(attendees, AccountOperation::EventUpdate)?;
        }
        let jmap_patch =
            jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)?;
        let mut set = CalendarEventSet::new();
        if let Some(state) = expected {
            set = set.if_in_state(state);
        }
        set.update_item(id.clone(), jmap_patch);
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventUpdate))?;
        response
            .updated(&id)
            .map_err(to_acct_err(AccountOperation::EventUpdate))?;
        Ok(())
    })
}

pub(crate) fn delete<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventId,
) -> AccountFuture<Result<(), AccountError>> {
    delete_if_version(calendars, event, None)
}

pub(crate) fn delete_if_version<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventId,
    expected_version: Option<bifrost_types::ServerVersion>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let expected = super::mutation::caller_state(
            expected_version.as_ref(),
            AccountOperation::EventDelete,
        )?;
        let calendars = require_calendars(calendars, AccountOperation::EventDelete)?;
        let id = CalendarEventId::new(event.0);
        let mut set = CalendarEventSet::new().destroy([id.clone()]);
        if let Some(state) = expected {
            set = set.if_in_state(state);
        }
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventDelete))?;
        response
            .destroyed(&id)
            .map_err(to_acct_err(AccountOperation::EventDelete))
    })
}

pub(crate) fn rsvp<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    self_emails: Vec<String>,
    event: EventId,
    status: RsvpStatus,
) -> AccountFuture<Result<(), AccountError>> {
    rsvp_if_version(calendars, self_emails, event, status, None)
}

pub(crate) fn rsvp_if_version<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    self_emails: Vec<String>,
    event: EventId,
    status: RsvpStatus,
    expected_version: Option<bifrost_types::ServerVersion>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let expected =
            super::mutation::caller_state(expected_version.as_ref(), AccountOperation::EventRsvp)?;
        let calendars = require_calendars(calendars, AccountOperation::EventRsvp)?;
        let id = CalendarEventId::new(event.0);
        let current = get_raw_event(&calendars, id.clone(), AccountOperation::EventRsvp).await?;
        let patch = jmap_rsvp_patch_from_participants(current.participants(), &self_emails, status)
            .map_err(|message| unsupported(AccountOperation::EventRsvp, message))?;
        let mut set = CalendarEventSet::new();
        if let Some(state) = expected {
            set = set.if_in_state(state);
        }
        set.update_item(id.clone(), patch);
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventRsvp))?;
        response
            .updated(&id)
            .map_err(to_acct_err(AccountOperation::EventRsvp))?;
        Ok(())
    })
}

pub(crate) fn search<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    request: EventSearchRequest,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventSearch)?;
        let cursor =
            CalendarPages::decode_page_cursor(request.page_cursor, AccountOperation::EventSearch)?;
        let query = crate::calendar_event::CalendarEventQuery::new()
            .filter(EventFilter::text(request.query.clone()))
            .limit(limit(request.limit))
            .calculate_total(true);
        let query = CalendarPages::anchor_query(query, cursor.as_ref());
        let response = calendars
            .call(query)
            .await
            .map_err(CalendarPages::page_call_err(
                AccountOperation::EventSearch,
                cursor.is_some(),
            ))?;
        // Before the total, before the cursor, before hydration: an
        // inconsistent continuation must expose no items and no successor.
        CalendarPages::verify_query_state(
            cursor.as_ref(),
            response.query_state(),
            AccountOperation::EventSearch,
        )?;
        let total = response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::EventSearch))
            .transpose()?;
        // `CalendarEvent/query` has no calendar condition here, so a
        // requested calendar is applied client-side after hydration. The
        // server's total counts the unfiltered result set, which is not
        // the total of what this walk can ever return; reporting it would
        // be a number the consumer cannot reconcile against the pages it
        // receives. No total is the honest answer. The cursor stays live -
        // it addresses the server-side result set, and a page whose every
        // hit belonged to another calendar is legitimately empty with more
        // pages behind it.
        //
        // The cancelled filter is client-side for the same reason
        // (draft-26 has no status filter condition, and `get_events` always
        // hydrates `status`), and suppresses the total by the same argument:
        // the server's count includes the cancelled events dropped here.
        let reported_total = if request.calendar_id.is_some() || !request.include_cancelled {
            None
        } else {
            total
        };
        let next_cursor = CalendarPages::next_cursor(
            cursor.as_ref(),
            response.position(),
            response.ids(),
            total,
            response.query_state(),
            AccountOperation::EventSearch,
        )?;
        let hydrated = get_events(
            &calendars,
            response.into_ids(),
            AccountOperation::EventSearch,
        )
        .await?;
        let events = hydrated
            .events
            .into_iter()
            .filter(|event| {
                request
                    .calendar_id
                    .as_ref()
                    .is_none_or(|calendar| event.calendar_id == *calendar)
                    && (request.include_cancelled || event.status != EventStatus::Cancelled)
            })
            .collect();
        Ok(Page {
            truncated: false,
            items: events,
            next_cursor,
            estimated_total: reported_total,
            failed_ids: hydrated.failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

/// The result of hydrating a page of event ids: the events that
/// materialized, plus every submitted id that did not produce one.
struct HydratedEvents {
    events: Vec<CalendarEvent>,
    failed_ids: Vec<String>,
}

async fn get_events<T: HttpTransport>(
    calendars: &CalendarAccount<T>,
    ids: Vec<CalendarEventId>,
    operation: AccountOperation,
) -> Result<HydratedEvents, AccountError> {
    if ids.is_empty() {
        return Ok(HydratedEvents {
            events: Vec::new(),
            failed_ids: Vec::new(),
        });
    }
    let requested = ids
        .iter()
        .cloned()
        .map(CalendarEventId::into_string)
        .collect::<Vec<_>>();
    let response = calendars
        .call(CalendarEventGet::new().ids(ids))
        .await
        .map_err(to_acct_err(operation))?;
    let not_found = response.not_found().to_vec();
    Ok(reconcile_events(
        requested,
        &not_found,
        response.into_list(),
        operation,
    ))
}

/// Split a `CalendarEvent/get` answer into hydrated events and the ids the
/// walk could not hand back an event for.
///
/// Two distinct losses land in the same lane. An id the server answered in
/// neither `list` nor `notFound` has simply vanished from the page, the
/// shape `contacts.rs::reconcile_cards` exists to prevent: read as absence
/// it looks like a deletion. And an event `event_from_jmap` refuses - a
/// modified recurrence override, multiple or excluded recurrence rules, an
/// unknown participant role - is a resource the provider fetched but could
/// not materialize, which is precisely what `Page::failed_ids` is for.
/// Before this, one such event anywhere in the queried window failed the
/// entire call, permanently, because the event does not go away.
fn reconcile_events(
    requested: Vec<String>,
    not_found: &[CalendarEventId],
    list: Vec<JmapCalendarEvent>,
    operation: AccountOperation,
) -> HydratedEvents {
    let mut answered: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut failed_ids: Vec<String> = Vec::new();
    for missing in not_found {
        let missing = missing.clone().into_string();
        if answered.insert(missing.clone()) {
            failed_ids.push(missing);
        }
    }

    let mut events = Vec::with_capacity(list.len());
    for event in list {
        // An object returned without a usable id is a handle to nothing;
        // the crate's other walks refuse the shape. Dropping it leaves the
        // submitted id unanswered, so it still reaches `failed_ids` below.
        let Some(id) = event.id().map(CalendarEventId::into_string) else {
            continue;
        };
        if id.is_empty() {
            continue;
        }
        match event_from_jmap(event, operation) {
            Ok(event) => {
                answered.insert(id);
                events.push(event);
            }
            Err(_) => {
                if answered.insert(id.clone()) {
                    failed_ids.push(id);
                }
            }
        }
    }

    for id in requested {
        if answered.insert(id.clone()) {
            failed_ids.push(id);
        }
    }
    HydratedEvents { events, failed_ids }
}

async fn get_raw_event<T: HttpTransport>(
    calendars: &CalendarAccount<T>,
    id: CalendarEventId,
    operation: AccountOperation,
) -> Result<JmapCalendarEvent, AccountError> {
    let response = calendars
        .call(CalendarEventGet::new().ids([id]))
        .await
        .map_err(to_acct_err(operation))?;
    response
        .into_list()
        .into_iter()
        .next()
        .ok_or_else(|| unsupported(operation, "JMAP CalendarEvent/get returned no event"))
}

fn calendar_from_jmap(calendar: JmapCalendar) -> Calendar {
    let native = calendar
        .id()
        .cloned()
        .map(JmapCalendarId::into_string)
        .unwrap_or_default();
    let rights = calendar.my_rights();
    let can_write = rights_can_write(rights);
    Calendar {
        id: CalendarId(native.clone()),
        native_id: native.clone(),
        name: calendar.name().unwrap_or("Calendar").to_string(),
        color: calendar.color().map(ToString::to_string),
        provenance: CalendarProvenance {
            provider: ProtocolKind::Jmap,
            native,
            calendar_native: None,
        },
        is_default: calendar.is_default().unwrap_or(false),
        can_create_events: can_write,
        can_update_events: can_write,
        can_delete_events: rights_can_delete(rights),
    }
}

fn event_from_jmap(
    event: JmapCalendarEvent,
    operation: AccountOperation,
) -> Result<CalendarEvent, AccountError> {
    let native = event
        .id()
        .map(CalendarEventId::into_string)
        .unwrap_or_default();
    let calendar_id = event
        .calendar_ids()
        .and_then(|ids| ids.keys().next().cloned())
        .map(CalendarId)
        .unwrap_or_else(|| CalendarId(String::new()));
    let is_all_day = event.show_without_time().unwrap_or(false);
    let raw_start = event.start().unwrap_or_default();
    let raw_end =
        end_from_start_duration(raw_start, event.duration().unwrap_or_default(), is_all_day);
    let timezone = event.time_zone().as_value().map(ToString::to_string);
    let start = EventTime {
        value: shared_time_from_jmap(raw_start, is_all_day),
        timezone: timezone.clone(),
    };
    // An absent `duration` is RFC 8984's `PT0S`, a real zero-length end; only
    // an event with no `start` to measure from has no end at all.
    let end = Some(EventTime {
        value: shared_time_from_jmap(&raw_end, is_all_day),
        timezone: timezone.clone(),
    })
    .filter(|end| !end.value.is_empty());
    let (rdate, mut exdate) = recurrence_dates_from_overrides(event.recurrence_overrides())
        .map_err(|message| unsupported(operation, message))?;
    if let Some(dates) = event.excluded_dates() {
        exdate.extend(dates.keys().cloned());
    }
    let alerts = event.alerts();
    let reminders = reminders_from_alerts(alerts.as_value().copied());
    let recurrence = recurrence_from_jmap(&event, raw_start, is_all_day, timezone.as_deref())
        .map_err(|message| unsupported(operation, message))?;
    let attendees =
        attendees(event.participants()).map_err(|message| unsupported(operation, message))?;
    Ok(CalendarEvent {
        id: EventId(native.clone()),
        calendar_id: calendar_id.clone(),
        native_id: native.clone(),
        uid: event.uid().map(ToString::to_string),
        etag: None,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Jmap,
            native,
            calendar_native: Some(calendar_id.0),
        },
        title: event.title().map(ToString::to_string),
        description: event.description().map(ToString::to_string),
        location: first_location(event.locations()),
        start,
        end,
        is_all_day,
        status: event_status(event.status()),
        availability: availability(event.free_busy_status()),
        visibility: visibility(event.privacy()),
        self_response: RsvpStatus::Unknown,
        organizer: organizer(event.participants()),
        attendees,
        reminders,
        recurrence: EventRecurrence {
            rdate,
            exdate,
            ..recurrence
        },
        html_link: None,
        raw_ical: None,
    })
}

fn recurrence_from_jmap(
    event: &JmapCalendarEvent,
    start: &str,
    is_all_day: bool,
    timezone: Option<&str>,
) -> Result<EventRecurrence, &'static str> {
    if event
        .excluded_recurrence_rules()
        .is_some_and(|rules| !rules.is_empty())
    {
        return Err(
            "JMAP excludedRecurrenceRules cannot be represented by the shared recurrence model",
        );
    }
    let rules = event.recurrence_rules().map(Vec::as_slice).unwrap_or(&[]);
    if rules.len() > 1 {
        return Err(
            "multiple JMAP recurrenceRules cannot be represented by the shared recurrence model",
        );
    }
    let rrule = rules
        .first()
        .map(|rule| rrule_from_jmap_recurrence_rule(rule, start, is_all_day, timezone))
        .transpose()?
        .flatten();
    Ok(EventRecurrence {
        rrule,
        recurrence_id: event.recurrence_id().map(ToString::to_string),
        ..EventRecurrence::default()
    })
}

fn jmap_create_from_event(
    event: &EventCreate,
    operation: AccountOperation,
) -> Result<CalendarEventCreate, AccountError> {
    let mut create = CalendarEventCreate::new(None);
    write_event_create(&mut create, event, operation)?;
    Ok(create)
}

/// The event start and all-day flag that JSCalendar conversion needs but an
/// `EventPatch` is not required to carry, plus the owner participants an
/// attendee patch must preserve.
#[derive(Debug, Default, Clone)]
struct EventContext {
    start: Option<EventTime>,
    is_all_day: bool,
    /// The current event's owner-roled `participants` entries, keyed as the
    /// server stores them. `EventPatch.attendees` describes only attendees,
    /// but JSCalendar keeps the organizer in the same `participants` map, so
    /// a whole-map attendee write must carry these over or it deletes the
    /// organizer. Empty when the patch has no attendees or no read was made.
    owner_participants: Map<String, Value>,
}

fn rrule_has_until(patch: &EventPatch) -> bool {
    patch
        .recurrence
        .as_ref()
        .and_then(|recurrence| recurrence.rrule.as_deref())
        .is_some_and(|rrule| {
            rrule.split(';').any(|part| {
                part.split_once('=')
                    .is_some_and(|(key, _)| key.eq_ignore_ascii_case("UNTIL"))
            })
        })
}

/// True when the patch cannot supply the conversion context itself. `UNTIL`
/// needs the start's timezone; any start/end or `UNTIL` write needs to know
/// whether the event is all-day, and a patch that does not restate
/// `is_all_day` is not asserting that the event became timed.
fn event_patch_needs_context(patch: &EventPatch) -> bool {
    let until = rrule_has_until(patch);
    (until && patch.start.is_none())
        || (patch.is_all_day.is_none() && (until || patch.start.is_some() || patch.end.is_some()))
}

/// Whether `update` must read the current event before building its patch:
/// either the conversion context is missing (`event_patch_needs_context`),
/// or the patch writes the attendee list, whose whole-map write must carry
/// the current owner participants over.
fn event_patch_needs_current(patch: &EventPatch) -> bool {
    event_patch_needs_context(patch) || patch.attendees.is_some()
}

fn event_context_from_patch(
    patch: &EventPatch,
    current: Option<&JmapCalendarEvent>,
) -> EventContext {
    let is_all_day = patch
        .is_all_day
        .or_else(|| current.map(|event| event.show_without_time().unwrap_or(false)))
        .unwrap_or(false);
    let start = patch.start.clone().or_else(|| {
        current.map(|event| EventTime {
            value: shared_time_from_jmap(event.start().unwrap_or_default(), is_all_day),
            timezone: event.time_zone().as_value().map(ToString::to_string),
        })
    });
    let owner_participants = current
        .and_then(JmapCalendarEvent::participants)
        .into_iter()
        .flat_map(Map::iter)
        .filter(|(_, value)| {
            value
                .as_object()
                .and_then(|object| object.get("roles"))
                .and_then(Value::as_object)
                .is_some_and(|roles| role_enabled(roles, "owner"))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    EventContext {
        start,
        is_all_day,
        owner_participants,
    }
}

fn jmap_patch_from_event_patch(
    patch: &EventPatch,
    context: &EventContext,
    operation: AccountOperation,
) -> Result<CalendarEventPatch, AccountError> {
    let mut out = CalendarEventPatch::default();
    if let Some(calendar) = &patch.calendar_id {
        out.calendar_ids([calendar.0.clone()]);
    }
    if let Some(title) = &patch.title {
        match title {
            Some(title) => {
                out.title(title.clone());
            }
            None => {
                out.set_property("title", Value::Null);
            }
        }
    }
    if let Some(description) = &patch.description {
        match description {
            Some(description) => {
                out.description(description.clone());
            }
            None => {
                out.set_property("description", Value::Null);
            }
        }
    }
    if let Some(location) = &patch.location {
        match location {
            Some(location) => {
                out.locations(single_location(location));
            }
            None => {
                out.set_property("locations", Value::Null);
            }
        }
    }
    // JSCalendar models the end as `start` + `duration`; there is no
    // standalone end property. Recomputing one bound requires the other, so a
    // patch carrying only `start` or only `end` cannot be applied losslessly
    // without reading the current event. Reject rather than silently keep a
    // stale duration (start-only) or drop the change entirely (end-only).
    //
    // An empty or unparseable `start` is refused (see `validate_start`), and a
    // non-empty `end` that cannot be parsed, or precedes the start, is refused
    // by `event_duration`. An empty `end`
    // is the projection's "the provider did not say" echoed back by a
    // read-modify-write, and on a patch it honestly means "no duration
    // change": JSCalendar stores the end as `duration`, and a patch that omits
    // `duration` leaves the stored one alone. Encoding it instead would send
    // `PT0S` (`duration` cannot parse an empty end), silently collapsing the
    // event to zero length. So a blank end is dropped from the patch: start
    // and timeZone still apply, duration is untouched, and a blank end with
    // no start is a no-op for the time fields.
    validate_start(patch.start.as_ref(), context.is_all_day, operation)?;
    let end_is_blank = patch.end.as_ref().is_some_and(is_blank_time);
    let end = patch.end.as_ref().filter(|end| !is_blank_time(end));
    match (&patch.start, end) {
        (Some(start), Some(end)) => {
            let all_day = context.is_all_day;
            let value = event_duration(start, end, all_day, operation)?;
            out.start(jmap_time_from_shared(start, all_day, operation)?);
            out.time_zone(start.timezone.clone());
            out.duration(value);
        }
        (Some(start), None) if end_is_blank => {
            out.start(jmap_time_from_shared(start, context.is_all_day, operation)?);
            out.time_zone(start.timezone.clone());
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(unsupported(
                operation,
                "JMAP event time patches must set both start and end; JSCalendar derives the end from start + duration",
            ));
        }
        (None, None) => {}
    }
    if let Some(is_all_day) = patch.is_all_day {
        out.show_without_time(is_all_day);
    }
    if let Some(status) = patch.status {
        out.status(jmap_event_status(status));
    }
    if let Some(availability) = patch.availability {
        out.free_busy_status(jmap_availability(availability));
    }
    if let Some(visibility) = patch.visibility {
        out.set_property("privacy", json!(jmap_visibility(visibility)));
    }
    if let Some(attendees) = &patch.attendees {
        out.participants(merge_owner_participants(
            &context.owner_participants,
            attendees,
        ));
    }
    if let Some(recurrence) = &patch.recurrence {
        match recurrence.rrule.as_deref() {
            // No rule in the shared recurrence means "no recurrence rule":
            // clear whatever the server holds.
            None => {
                out.set_property("recurrenceRules", Value::Null);
            }
            // A rule that fails conversion is an error, never a clear. The
            // callers validate first, but the builder must not depend on
            // that call order: a future caller that skipped validation
            // would otherwise silently erase the event's recurrence.
            Some(rrule) => {
                let rule = jmap_recurrence_rule_from_rrule(
                    rrule,
                    context.start.as_ref(),
                    context.is_all_day,
                )
                .ok_or_else(|| {
                    unsupported(
                        operation,
                        "JMAP shared recurrence contains RRULE fields unsupported by the JSCalendar mapper",
                    )
                })?;
                out.recurrence_rules(vec![rule]);
            }
        }
        let overrides = recurrence_overrides_from_shared(recurrence);
        if overrides.is_empty() {
            out.set_property("recurrenceOverrides", Value::Null);
        } else {
            out.recurrence_overrides(overrides);
        }
    }
    Ok(out)
}

/// Merge a whole-list attendee write with the owner participants the
/// current event holds.
///
/// JSCalendar keeps the organizer in the same `participants` map as the
/// attendees, so replacing the map with only the new attendee entries
/// would silently delete the organizer. The owner entries are carried
/// over under their existing keys. An attendee whose email matches an
/// owner participant updates that entry's name and participation status
/// in place (its owner role is kept - the read path surfaces an owner as
/// a `Chair` attendee, so a read-modify-write round trip stays stable);
/// the rest get fresh keys that never collide with a kept one.
fn merge_owner_participants(
    owner_participants: &Map<String, Value>,
    attendees: &[EventAttendee],
) -> Map<String, Value> {
    let mut participants = owner_participants.clone();
    let mut next_key = 0_usize;
    for attendee in attendees {
        let owner_key = participants.iter().find_map(|(key, value)| {
            let email = value.as_object()?.get("email")?.as_str()?;
            email
                .eq_ignore_ascii_case(&attendee.email)
                .then(|| key.clone())
        });
        if let Some(owner_key) = owner_key {
            if let Some(Value::Object(entry)) = participants.get_mut(&owner_key) {
                match &attendee.name {
                    Some(name) => {
                        entry.insert("name".to_string(), json!(name));
                    }
                    None => {
                        entry.remove("name");
                    }
                }
                entry.insert(
                    "participationStatus".to_string(),
                    json!(rsvp_value(attendee.status)),
                );
            }
            continue;
        }
        let mut key = format!("p{next_key}");
        while participants.contains_key(&key) {
            next_key += 1;
            key = format!("p{next_key}");
        }
        next_key += 1;
        let entry = participants_from_attendees(std::slice::from_ref(attendee))
            .into_iter()
            .next()
            .map(|(_, value)| value)
            .unwrap_or(Value::Null);
        participants.insert(key, entry);
    }
    participants
}

fn jmap_rsvp_patch_from_participants(
    participants: Option<&Map<String, Value>>,
    self_emails: &[String],
    status: RsvpStatus,
) -> Result<CalendarEventPatch, &'static str> {
    let participant_id = rsvp_participant_id(participants, self_emails)?;
    let mut patch = CalendarEventPatch::default();
    patch.set_property(
        format!(
            "participants/{}/participationStatus",
            crate::core::set::escape_json_pointer_token(participant_id)
        ),
        json!(rsvp_value(status)),
    );
    Ok(patch)
}

fn rsvp_participant_id<'a>(
    participants: Option<&'a Map<String, Value>>,
    self_emails: &[String],
) -> Result<&'a str, &'static str> {
    let Some(participants) = participants else {
        return Err("JMAP event has no participants to RSVP");
    };
    let candidates = participants
        .iter()
        .filter_map(|(id, value)| {
            let object = value.as_object()?;
            let has_email = object.get("email").and_then(Value::as_str).is_some();
            let is_owner = object
                .get("roles")
                .and_then(Value::as_object)
                .is_some_and(|roles| role_enabled(roles, "owner"));
            (has_email && !is_owner).then_some((id.as_str(), object))
        })
        .collect::<Vec<_>>();
    if !self_emails.is_empty() {
        let matches = candidates
            .iter()
            .filter_map(|(id, object)| {
                let email = object.get("email")?.as_str()?;
                self_emails
                    .iter()
                    .any(|self_email| email.eq_ignore_ascii_case(self_email))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [id] => return Ok(id),
            [] => {}
            _ => {
                return Err("JMAP event has multiple participants matching the authenticated user");
            }
        }
    }
    match candidates.as_slice() {
        [(id, _)] => Ok(id),
        [] => Err("JMAP event has no attendee participant to RSVP"),
        _ => Err(
            "JMAP event has multiple attendee participants; authenticated attendee lookup is unavailable",
        ),
    }
}

fn write_event_create(
    target: &mut CalendarEventCreate,
    event: &EventCreate,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    target.set_property("@type", json!("Event"));
    target.calendar_ids([event.calendar_id.0.clone()]);
    if let Some(title) = &event.title {
        target.title(title.clone());
    }
    if let Some(description) = &event.description {
        target.description(description.clone());
    }
    if let Some(location) = &event.location {
        target.locations(single_location(location));
    }
    // Neither bound may be empty on a create. An empty start is not a
    // LocalDateTime (`Request(Malformed)`). An empty end cannot be encoded:
    // `duration` would fall back to `PT0S`, and RFC 8984 also defaults an
    // absent `duration` to `PT0S`, so either way the server would store a
    // zero-length event the caller never asked for. The caller must supply the
    // end it wants (`Unsupported`, matching bifrost-google's create).
    validate_start(Some(&event.start), event.is_all_day, operation)?;
    if is_blank_time(&event.end) {
        return Err(unsupported(
            operation,
            "JMAP cannot create an event with no end: JSCalendar derives the end from start + duration, so an empty end would be stored as a zero-length event; a create needs an explicit end",
        ));
    }
    target.start(jmap_time_from_shared(
        &event.start,
        event.is_all_day,
        operation,
    )?);
    target.time_zone(event.start.timezone.clone());
    target.duration(event_duration(
        &event.start,
        &event.end,
        event.is_all_day,
        operation,
    )?);
    target.show_without_time(event.is_all_day);
    target.status(jmap_event_status(event.status));
    target.free_busy_status(jmap_availability(event.availability));
    target.set_property("privacy", json!(jmap_visibility(event.visibility)));
    let participants = participants_from_event_create(event);
    if !participants.is_empty() {
        target.participants(participants);
    }
    // A rule that fails conversion is an error, never a silent omission.
    // `create` validates first, but the builder must not depend on that
    // call order: a future caller that skipped validation would otherwise
    // create the event with its recurrence quietly dropped.
    if let Some(rrule) = event.recurrence.rrule.as_deref() {
        let rule = jmap_recurrence_rule_from_rrule(rrule, Some(&event.start), event.is_all_day)
            .ok_or_else(|| {
                unsupported(
                    operation,
                    "JMAP shared recurrence contains RRULE fields unsupported by the JSCalendar mapper",
                )
            })?;
        target.recurrence_rules(vec![rule]);
    }
    let overrides = recurrence_overrides_from_shared(&event.recurrence);
    if !overrides.is_empty() {
        target.recurrence_overrides(overrides);
    }
    Ok(())
}

fn recurrence_overrides_from_shared(recurrence: &EventRecurrence) -> Map<String, Value> {
    let mut overrides = Map::new();
    for date in &recurrence.rdate {
        overrides.insert(date.clone(), Value::Object(Map::new()));
    }
    for date in &recurrence.exdate {
        overrides.insert(date.clone(), json!({ "excluded": true }));
    }
    overrides
}

fn recurrence_dates_from_overrides(
    overrides: Option<&Map<String, Value>>,
) -> Result<(Vec<String>, Vec<String>), &'static str> {
    let mut rdate = Vec::new();
    let mut exdate = Vec::new();
    let Some(overrides) = overrides else {
        return Ok((rdate, exdate));
    };
    for (date, value) in overrides {
        let Some(object) = value.as_object() else {
            return Err("JMAP recurrenceOverrides entry is not an object");
        };
        // The shared model has only RDATE and EXDATE. An override that merely
        // excludes an occurrence is an EXDATE and one that adds an unmodified
        // occurrence is an RDATE; anything that patches the occurrence itself
        // has no shared representation, and dropping it would hand back the
        // master event as though the modified occurrence did not exist.
        match object.get("excluded").and_then(Value::as_bool) {
            Some(true) => exdate.push(date.clone()),
            _ if object.keys().all(|key| key == "excluded") => rdate.push(date.clone()),
            _ => {
                return Err(
                    "a modified JMAP recurrence override cannot be represented by the shared recurrence model",
                );
            }
        }
    }
    Ok((rdate, exdate))
}

fn validate_shared_recurrence(
    recurrence: &EventRecurrence,
    start: Option<&EventTime>,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if recurrence
        .rrule
        .as_deref()
        .is_some_and(|rrule| jmap_recurrence_rule_from_rrule(rrule, start, is_all_day).is_none())
    {
        return Err(unsupported(
            operation,
            "JMAP shared recurrence contains RRULE fields unsupported by the JSCalendar mapper",
        ));
    }
    Ok(())
}

/// The supported RRULE part set is exactly the match arms below: `FREQ`,
/// `INTERVAL`, `COUNT`, `UNTIL`, `BYDAY`, `BYMONTH`, `BYMONTHDAY`. Every
/// other part returns `None`, which the callers turn into a loud
/// `Unsupported` rather than a silently narrowed rule - a dropped `BYHOUR`
/// would hand the server a recurrence that expands to different occurrences
/// than the caller asked for, which is worse than a refusal.
///
/// This narrowness is an accepted state, not a gap awaiting a bug report.
/// Widening the set (and the inbound direction in
/// `rrule_from_jmap_recurrence_rule`, which must stay symmetric with it) is
/// tracked as deferred work in `reference/jmap/DEFERRED.md`; do not re-file
/// it as a defect. Anything added here needs its inbound counterpart in the
/// same change, or a round trip starts losing the new part.
fn jmap_recurrence_rule_from_rrule(
    rrule: &str,
    start: Option<&EventTime>,
    is_all_day: bool,
) -> Option<Value> {
    let mut object = Map::new();
    object.insert("@type".to_string(), json!("RecurrenceRule"));
    for part in rrule.split(';') {
        let (key, value) = part.split_once('=')?;
        match key.to_ascii_uppercase().as_str() {
            "FREQ" => {
                let frequency = value.to_ascii_lowercase();
                if ![
                    "yearly", "monthly", "weekly", "daily", "hourly", "minutely", "secondly",
                ]
                .contains(&frequency.as_str())
                {
                    return None;
                }
                object.insert("frequency".to_string(), Value::String(frequency));
            }
            "INTERVAL" => {
                object.insert("interval".to_string(), json!(value.parse::<u64>().ok()?));
            }
            "COUNT" => {
                object.insert("count".to_string(), json!(value.parse::<u64>().ok()?));
            }
            "UNTIL" => {
                object.insert(
                    "until".to_string(),
                    Value::String(jmap_until_from_ical(value, start, is_all_day)?),
                );
            }
            "BYDAY" => {
                object.insert("byDay".to_string(), Value::Array(jmap_by_day(value)?));
            }
            "BYMONTH" => {
                // RFC 8984 section 4.3.3 types byMonth as String[] (month
                // numbers as strings, optionally "L"-suffixed for leap
                // months). Emitting integers here fed spec-conforming
                // servers a shape they may reject.
                object.insert(
                    "byMonth".to_string(),
                    Value::Array(month_string_list(value)?),
                );
            }
            "BYMONTHDAY" => {
                object.insert("byMonthDay".to_string(), Value::Array(integer_list(value)?));
            }
            _ => return None,
        }
    }
    object
        .contains_key("frequency")
        .then_some(Value::Object(object))
}

fn rrule_from_jmap_recurrence_rule(
    rule: &Value,
    start: &str,
    is_all_day: bool,
    timezone: Option<&str>,
) -> Result<Option<String>, &'static str> {
    let object = rule
        .as_object()
        .ok_or("JMAP recurrence rule is not an object")?;
    const SUPPORTED: &[&str] = &[
        "@type",
        "frequency",
        "interval",
        "count",
        "until",
        "byDay",
        "byMonth",
        "byMonthDay",
    ];
    if object.keys().any(|key| !SUPPORTED.contains(&key.as_str())) {
        return Err("JMAP recurrence rule contains an unsupported component");
    }
    let mut parts = Vec::new();
    let frequency = object
        .get("frequency")
        .and_then(Value::as_str)
        .ok_or("JMAP recurrence frequency is missing or invalid")?;
    if ![
        "yearly", "monthly", "weekly", "daily", "hourly", "minutely", "secondly",
    ]
    .contains(&frequency)
    {
        return Err("JMAP recurrence frequency is unsupported");
    }
    parts.push(format!("FREQ={}", frequency.to_ascii_uppercase()));
    if let Some(interval) = object.get("interval") {
        parts.push(format!(
            "INTERVAL={}",
            interval
                .as_u64()
                .ok_or("JMAP recurrence interval is invalid")?
        ));
    }
    if let Some(count) = object.get("count") {
        parts.push(format!(
            "COUNT={}",
            count.as_u64().ok_or("JMAP recurrence count is invalid")?
        ));
    }
    if object.contains_key("count") && object.contains_key("until") {
        return Err("JMAP recurrence rule contains both count and until");
    }
    if let Some(until) = object.get("until") {
        parts.push(format!(
            "UNTIL={}",
            ical_until_from_jmap(
                until
                    .as_str()
                    .ok_or("JMAP recurrence until is not a string")?,
                start,
                is_all_day,
                timezone,
            )?
        ));
    }
    if let Some(by_day) = object.get("byDay") {
        parts.push(format!(
            "BYDAY={}",
            rrule_by_day(by_day).ok_or("JMAP recurrence byDay is invalid")?
        ));
    }
    if let Some(by_month) = object.get("byMonth") {
        parts.push(format!(
            "BYMONTH={}",
            rrule_month_list(by_month).ok_or("JMAP recurrence byMonth is invalid")?
        ));
    }
    if let Some(by_month_day) = object.get("byMonthDay") {
        parts.push(format!(
            "BYMONTHDAY={}",
            rrule_integer_list(by_month_day).ok_or("JMAP recurrence byMonthDay is invalid")?
        ));
    }
    Ok(Some(parts.join(";")))
}

fn jmap_until_from_ical(
    value: &str,
    start: Option<&EventTime>,
    is_all_day: bool,
) -> Option<String> {
    if is_all_day {
        let date = civil::Date::strptime("%Y%m%d", value).ok()?;
        return Some(date.strftime("%Y-%m-%dT00:00:00").to_string());
    }
    let raw = value.trim_end_matches(['Z', 'z']);
    let datetime = civil::DateTime::strptime("%Y%m%dT%H%M%S", raw).ok()?;
    if value.ends_with(['Z', 'z']) {
        let start = start?;
        let timezone = TimeZone::get(start.timezone.as_deref()?).ok()?;
        let instant = Offset::UTC.to_timestamp(datetime).ok()?;
        return Some(
            timezone
                .to_datetime(instant)
                .strftime("%Y-%m-%dT%H:%M:%S")
                .to_string(),
        );
    }
    start.filter(|start| start.timezone.is_none())?;
    Some(datetime.strftime("%Y-%m-%dT%H:%M:%S").to_string())
}

fn ical_until_from_jmap(
    value: &str,
    start: &str,
    is_all_day: bool,
    timezone: Option<&str>,
) -> Result<String, &'static str> {
    let until = civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S", value)
        .map_err(|_| "JMAP recurrence until is not a LocalDateTime")?;
    if is_all_day || start.len() == 10 {
        if until.time() != civil::Time::MIN {
            return Err("an all-day JMAP recurrence until must be midnight");
        }
        return Ok(until.date().strftime("%Y%m%d").to_string());
    }
    if let Some(timezone) = timezone {
        let timezone = TimeZone::get(timezone)
            .map_err(|_| "JMAP event timeZone is unknown; recurrence until cannot be normalized")?;
        let instant = timezone
            .to_timestamp(until)
            .map_err(|_| "JMAP recurrence until is ambiguous or invalid in the event timeZone")?;
        return Ok(Offset::UTC
            .to_datetime(instant)
            .strftime("%Y%m%dT%H%M%SZ")
            .to_string());
    }
    Ok(until.strftime("%Y%m%dT%H%M%S").to_string())
}

fn jmap_by_day(value: &str) -> Option<Vec<Value>> {
    value
        .split(',')
        .map(|day| {
            let (nth, day) = split_by_day(day)?;
            let mut object = Map::new();
            object.insert("@type".to_string(), json!("NDay"));
            object.insert("day".to_string(), Value::String(day.to_ascii_lowercase()));
            if let Some(nth) = nth {
                object.insert("nthOfPeriod".to_string(), json!(nth));
            }
            Some(Value::Object(object))
        })
        .collect()
}

fn split_by_day(value: &str) -> Option<(Option<i64>, &str)> {
    if value.len() == 2 {
        return ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]
            .contains(&value.to_ascii_uppercase().as_str())
            .then_some((None, value));
    }
    let split = value.len().checked_sub(2)?;
    let nth = value[..split].parse::<i64>().ok()?;
    let day = &value[split..];
    ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]
        .contains(&day.to_ascii_uppercase().as_str())
        .then_some((Some(nth), day))
}

fn integer_list(value: &str) -> Option<Vec<Value>> {
    value
        .split(',')
        .map(|item| Some(json!(item.parse::<i64>().ok()?)))
        .collect()
}

/// RRULE `BYMONTH=6,12` -> JSCalendar `byMonth: ["6", "12"]`. Validated as
/// integers (RRULE months are numeric), emitted as the String[] RFC 8984
/// section 4.3.3 requires.
fn month_string_list(value: &str) -> Option<Vec<Value>> {
    value
        .split(',')
        .map(|item| Some(Value::String(item.parse::<i64>().ok()?.to_string())))
        .collect()
}

/// JSCalendar `byMonth` -> RRULE `BYMONTH` list. RFC 8984 types the entries
/// as strings ("6", "12", optionally "L"-suffixed for leap months); an "L"
/// suffix has no RRULE representation and is refused rather than silently
/// dropped. Bare integers are also accepted - bifrost's own write side
/// emitted them until the RFC shape landed, and lenient reads keep such
/// events convertible.
fn rrule_month_list(value: &Value) -> Option<String> {
    value
        .as_array()?
        .iter()
        .map(|item| {
            if let Some(number) = item.as_i64() {
                return Some(number.to_string());
            }
            let text = item.as_str()?;
            text.parse::<i64>().ok().map(|number| number.to_string())
        })
        .collect::<Option<Vec<_>>>()
        .map(|items| items.join(","))
}

fn rrule_by_day(value: &Value) -> Option<String> {
    let days = value.as_array()?;
    days.iter()
        .map(|day| {
            let object = day.as_object()?;
            if object
                .keys()
                .any(|key| !["@type", "day", "nthOfPeriod"].contains(&key.as_str()))
            {
                return None;
            }
            let day = object.get("day")?.as_str()?;
            split_by_day(day)?;
            let prefix = object
                .get("nthOfPeriod")
                .and_then(Value::as_i64)
                .map(|nth| nth.to_string())
                .unwrap_or_default();
            Some(format!("{}{}", prefix, day.to_ascii_uppercase()))
        })
        .collect::<Option<Vec<_>>>()
        .map(|days| days.join(","))
}

fn rrule_integer_list(value: &Value) -> Option<String> {
    value
        .as_array()?
        .iter()
        .map(|item| item.as_i64().map(|item| item.to_string()))
        .collect::<Option<Vec<_>>>()
        .map(|items| items.join(","))
}

fn participants_from_attendees(attendees: &[EventAttendee]) -> Map<String, Value> {
    attendees
        .iter()
        .enumerate()
        .map(|(index, attendee)| {
            let mut value = Map::new();
            value.insert("@type".to_string(), json!("Participant"));
            value.insert("email".to_string(), json!(attendee.email));
            if let Some(name) = &attendee.name {
                value.insert("name".to_string(), json!(name));
            }
            value.insert(
                "participationStatus".to_string(),
                json!(rsvp_value(attendee.status)),
            );
            value.insert("expectReply".to_string(), Value::Bool(true));
            if let Some(roles) = roles_from_attendee(attendee.role) {
                value.insert("roles".to_string(), Value::Object(roles));
            }
            (format!("p{index}"), Value::Object(value))
        })
        .collect()
}

fn participants_from_event_create(event: &EventCreate) -> Map<String, Value> {
    let mut participants = Map::new();
    if let Some(organizer) = &event.organizer {
        let mut value = Map::new();
        value.insert("@type".to_string(), json!("Participant"));
        value.insert("email".to_string(), json!(organizer.email));
        if let Some(name) = &organizer.name {
            value.insert("name".to_string(), json!(name));
        }
        value.insert("roles".to_string(), json!({ "owner": true }));
        participants.insert("owner".to_string(), Value::Object(value));
    }
    participants.extend(participants_from_attendees(&event.attendees));
    participants
}

fn roles_from_attendee(role: AttendeeRole) -> Option<Map<String, Value>> {
    let role = match role {
        AttendeeRole::Required => "attendee",
        AttendeeRole::Optional => "optional",
        AttendeeRole::Resource => "resource",
        AttendeeRole::Chair => "chair",
        _ => return None,
    };
    let mut roles = Map::new();
    roles.insert(role.to_string(), Value::Bool(true));
    Some(roles)
}

fn attendees(
    participants: Option<&Map<String, Value>>,
) -> Result<Vec<EventAttendee>, &'static str> {
    participants
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| value.as_object())
        .filter(|object| object.get("email").is_some())
        .map(|object| {
            Ok(EventAttendee {
                email: object
                    .get("email")
                    .and_then(Value::as_str)
                    .ok_or("JMAP participant email is not a string")?
                    .to_string(),
                name: object
                    .get("name")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                role: attendee_role(object.get("roles"))?,
                status: rsvp_status(object.get("participationStatus").and_then(Value::as_str))?,
            })
        })
        .collect()
}

fn attendee_role(value: Option<&Value>) -> Result<AttendeeRole, &'static str> {
    let Some(roles) = value.and_then(Value::as_object) else {
        return Ok(AttendeeRole::Unknown);
    };
    let known = ["chair", "owner", "optional", "resource", "attendee"];
    if roles
        .iter()
        .any(|(role, enabled)| enabled.as_bool() == Some(true) && !known.contains(&role.as_str()))
    {
        return Err("JMAP participant has an unsupported role");
    }
    Ok(
        if role_enabled(roles, "chair") || role_enabled(roles, "owner") {
            AttendeeRole::Chair
        } else if role_enabled(roles, "optional") {
            AttendeeRole::Optional
        } else if role_enabled(roles, "resource") {
            AttendeeRole::Resource
        } else if role_enabled(roles, "attendee") {
            AttendeeRole::Required
        } else {
            AttendeeRole::Unknown
        },
    )
}

fn role_enabled(roles: &Map<String, Value>, role: &str) -> bool {
    roles.get(role).and_then(Value::as_bool).unwrap_or(false)
}

fn organizer(participants: Option<&Map<String, Value>>) -> Option<EventOrganizer> {
    participants
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            object
                .get("roles")
                .and_then(Value::as_object)
                .is_some_and(|roles| roles.contains_key("owner"))
                .then(|| EventOrganizer {
                    email: object
                        .get("email")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    name: object
                        .get("name")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                })
        })
        .next()
}

/// Project JSCalendar `alerts` into `EventReminder`s. Each alert's
/// `trigger` is either an `OffsetTrigger` (relative to the event start or
/// end) or an `AbsoluteTrigger` (a UTC instant); the alert `action`
/// (`display` / `email`) is carried through uppercased.
fn reminders_from_alerts(alerts: Option<&Map<String, Value>>) -> Vec<EventReminder> {
    alerts
        .into_iter()
        .flat_map(Map::values)
        .filter_map(reminder_from_alert)
        .collect()
}

fn reminder_from_alert(value: &Value) -> Option<EventReminder> {
    let alert = value.as_object()?;
    let trigger = alert.get("trigger").and_then(Value::as_object)?;
    let action = alert
        .get("action")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    let trigger = match trigger.get("@type").and_then(Value::as_str) {
        Some("AbsoluteTrigger") => {
            ReminderTrigger::Absolute(trigger.get("when").and_then(Value::as_str)?.to_string())
        }
        // OffsetTrigger is the default when @type is absent.
        _ => {
            let offset = trigger.get("offset").and_then(Value::as_str)?.to_string();
            let relative_to = match trigger.get("relativeTo").and_then(Value::as_str) {
                Some("end") => ReminderRelativeTo::End,
                _ => ReminderRelativeTo::Start,
            };
            ReminderTrigger::Relative {
                offset,
                relative_to,
            }
        }
    };
    Some(EventReminder { trigger, action })
}

fn first_location(locations: Option<&Map<String, Value>>) -> Option<String> {
    locations
        .and_then(|locations| locations.values().next())
        .and_then(Value::as_object)
        .and_then(|location| location.get("name"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn single_location(value: &str) -> Map<String, Value> {
    let mut location = Map::new();
    location.insert("@type".to_string(), json!("Location"));
    location.insert("name".to_string(), json!(value));
    let mut locations = Map::new();
    locations.insert("loc1".to_string(), Value::Object(location));
    locations
}

fn is_blank_time(time: &EventTime) -> bool {
    time.value.trim().is_empty()
}

/// The shapes of event time this crate maps onto JSCalendar.
enum Moment {
    /// A bare date (`2026-06-02`).
    Date(civil::Date),
    /// A zoneless date-time (`2026-06-02T10:00:00`).
    Local(civil::DateTime),
    /// A UTC or offset date-time (`2026-06-02T10:00:00Z`, `...+02:00`).
    Instant(Timestamp),
}

impl Moment {
    /// The calendar date the value falls on, used for all-day arithmetic.
    fn date(&self) -> civil::Date {
        match self {
            Self::Date(date) => *date,
            Self::Local(datetime) => datetime.date(),
            Self::Instant(instant) => Offset::UTC.to_datetime(*instant).date(),
        }
    }
}

fn classify_time(value: &str) -> Option<Moment> {
    if value.len() == 10 {
        return civil::Date::strptime("%Y-%m-%d", value)
            .ok()
            .map(Moment::Date);
    }
    if let Ok(instant) = value.parse::<Timestamp>() {
        return Some(Moment::Instant(instant));
    }
    value.parse::<civil::DateTime>().ok().map(Moment::Local)
}

/// Parse one event bound, refusing anything this crate cannot map. An empty
/// value is refused as its own case: JSCalendar `start` is a mandatory
/// LocalDateTime, and the shared model's empty time is what a provider
/// projects for "did not say" (this crate's own read path produces one when a
/// server omits `start`), so a consumer echoing it back would send
/// `start: ""`. An all-day event accepts only a bare date or a zoneless
/// date-time, since its wire form is midnight on the date.
fn parse_time(
    time: &EventTime,
    field: &'static str,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<Moment, AccountError> {
    if is_blank_time(time) {
        return Err(malformed_time(
            operation,
            field,
            format!("JMAP calendar event `{field}` must not be empty"),
        ));
    }
    match classify_time(&time.value) {
        Some(Moment::Instant(_)) if is_all_day => Err(malformed_time(
            operation,
            field,
            format!("JMAP all-day event `{field}` must be a date or a zoneless date-time"),
        )),
        Some(moment) => Ok(moment),
        None => Err(malformed_time(
            operation,
            field,
            format!(
                "JMAP calendar event `{field}` is not a date, a local date-time or an RFC 3339 date-time"
            ),
        )),
    }
}

/// Validate an event `start` on its own. Refused before any request, as
/// `Request(Malformed)` naming `start`.
fn validate_start(
    start: Option<&EventTime>,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if let Some(start) = start {
        parse_time(start, "start", is_all_day, operation)?;
    }
    Ok(())
}

fn malformed_time(
    operation: AccountOperation,
    field: &'static str,
    message: String,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some(field),
            message: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// The JSCalendar `duration` for a shared start and (exclusive) end.
///
/// Both bounds must parse and the end must not precede the start; anything
/// else is `Request(Malformed)` naming the offending field. There is no
/// fallback duration: a zero-length event is never written for a time this
/// crate could not read.
fn event_duration(
    start: &EventTime,
    end: &EventTime,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    let start = parse_time(start, "start", is_all_day, operation)?;
    let end = parse_time(end, "end", is_all_day, operation)?;
    let before_start = || {
        malformed_time(
            operation,
            "end",
            "JMAP calendar event `end` is before `start`".to_string(),
        )
    };
    let days = |start: civil::Date, end: civil::Date| {
        let days = (end - start).get_days();
        if days < 0 {
            return Err(before_start());
        }
        // `EventTime`'s all-day end is exclusive, so the JSCalendar
        // duration is exactly end - start days (a single all-day event,
        // start D / end D+1, is P1D).
        Ok(format!("P{days}D"))
    };
    let seconds = |start: Timestamp, end: Timestamp| {
        let seconds = end.duration_since(start).as_secs();
        if seconds < 0 {
            return Err(before_start());
        }
        Ok(format!("PT{seconds}S"))
    };
    let local = |datetime: civil::DateTime| {
        Offset::UTC.to_timestamp(datetime).map_err(|_| {
            malformed_time(
                operation,
                "end",
                "JMAP calendar event time is outside the representable range".to_string(),
            )
        })
    };
    if is_all_day {
        return days(start.date(), end.date());
    }
    match (start, end) {
        (Moment::Date(start), Moment::Date(end)) => days(start, end),
        (Moment::Instant(start), Moment::Instant(end)) => seconds(start, end),
        (Moment::Local(start), Moment::Local(end)) => seconds(local(start)?, local(end)?),
        _ => Err(malformed_time(
            operation,
            "end",
            "JMAP calendar event `start` and `end` must use the same time shape".to_string(),
        )),
    }
}

fn shared_time_from_jmap(value: &str, is_all_day: bool) -> String {
    if is_all_day {
        // The shared all-day contract is a bare date; JSCalendar carries
        // midnight on that date. Keep only the date half so a server that
        // stores the conformant LocalDateTime reads back correctly.
        return value.split('T').next().unwrap_or(value).to_string();
    }
    if value.len() == 10 {
        return value.to_string();
    }
    if value.parse::<Timestamp>().is_ok() {
        return value.to_string();
    }
    if let Some(time) = local_datetime(value) {
        return shared_rfc3339(time);
    }
    value.to_string()
}

/// The JSCalendar `start` LocalDateTime for a shared event time.
///
/// The value is parsed here rather than trusted to a prior `validate_start`,
/// so no caller can put raw, unparsed text on the wire: an empty or
/// unparseable time is `Request(Malformed)` naming `start`.
fn jmap_time_from_shared(
    time: &EventTime,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    let wall = match parse_time(time, "start", is_all_day, operation)? {
        // JSCalendar has no DATE type: RFC 8984 `start` is always a
        // LocalDateTime, and an all-day event is midnight on its date with
        // `showWithoutTime` carrying the all-day sense. Writing the shared
        // bare DATE straight through emits a value no conforming server
        // accepts, and only reads back because our own read path truncates
        // the same way.
        Moment::Date(date) => date.to_datetime(civil::Time::MIN),
        Moment::Local(datetime) => datetime,
        // Rendered in the value's own offset, not normalized to UTC: the
        // JSCalendar `timeZone` property carries the zone separately, so the
        // wall clock is what belongs in the LocalDateTime.
        // A numeric offset yields the wall clock directly. A `Z` suffix is
        // rejected by the civil parser (Temporal reads it as an unknown
        // offset), so route that through the instant and read it back at UTC.
        Moment::Instant(instant) => time
            .value
            .parse::<civil::DateTime>()
            .unwrap_or_else(|_| Offset::UTC.to_datetime(instant)),
    };
    Ok(wall.strftime("%Y-%m-%dT%H:%M:%S").to_string())
}

// The JSCalendar event-query `after`/`before` filter conditions are
// LocalDateTime values (draft-ietf-jmap-calendars-26 section 5.11.1): a bare
// wall-clock string with no zone and no trailing `Z`, interpreted in the
// query's `timeZone` argument, which defaults to Etc/UTC. An offset-bearing
// RFC 3339 input is normalized to UTC and rendered bare, matching that
// Etc/UTC default; a bare date is midnight on that date, and a zoneless
// date-time is already a LocalDateTime. An empty or unparseable bound is
// `Request(Malformed)` naming `field`, refused before any request as the
// google and caldav range reads refuse theirs, never sent to the server raw.
fn jmap_utc_filter_time(time: &EventTime, field: &'static str) -> Result<String, AccountError> {
    let wall = match parse_time(time, field, false, AccountOperation::EventsInRange)? {
        Moment::Date(date) => date.to_datetime(civil::Time::MIN),
        Moment::Local(datetime) => datetime,
        Moment::Instant(instant) => Offset::UTC.to_datetime(instant),
    };
    Ok(wall.strftime("%Y-%m-%dT%H:%M:%S").to_string())
}

fn event_in_range(event: &CalendarEvent, start: &EventTime, end: &EventTime) -> bool {
    if start.value.is_empty() || end.value.is_empty() {
        return true;
    }
    let Some((range_start, range_end)) = time_interval(start, Some(end), false) else {
        return true;
    };
    let Some((event_start, event_end)) =
        time_interval(&event.start, event.end.as_ref(), event.is_all_day)
    else {
        return true;
    };
    // Both the query window and event interval use exclusive ends. An event
    // ending exactly at the window start, or starting exactly at its end,
    // does not overlap the window.
    event_start < range_end && event_end > range_start
}

fn time_interval(
    start: &EventTime,
    end: Option<&EventTime>,
    is_all_day: bool,
) -> Option<(Timestamp, Timestamp)> {
    let start = comparable_time(start, is_all_day)?;
    let end = end
        .and_then(|end| comparable_time(end, is_all_day))
        .unwrap_or(start);
    Some((start, end))
}

fn comparable_time(time: &EventTime, is_all_day: bool) -> Option<Timestamp> {
    if is_all_day || time.value.len() == 10 {
        let date = civil::Date::strptime("%Y-%m-%d", &time.value).ok()?;
        return Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok();
    }
    time.value
        .parse::<Timestamp>()
        .ok()
        .or_else(|| local_datetime(&time.value))
}

/// Render an instant for the shared `EventTime` layer, which spells the
/// UTC offset out as `+00:00` rather than `Z`.
fn shared_rfc3339(at: Timestamp) -> String {
    Offset::UTC
        .to_datetime(at)
        .strftime("%Y-%m-%dT%H:%M:%S+00:00")
        .to_string()
}

/// A zoneless JSCalendar LocalDateTime, anchored at UTC so two of them
/// compare against each other and against offset-bearing values on one
/// consistent scale.
fn local_datetime(value: &str) -> Option<Timestamp> {
    let naive = civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S", value).ok()?;
    Offset::UTC.to_timestamp(naive).ok()
}

fn end_from_start_duration(start: &str, duration: &str, is_all_day: bool) -> String {
    let seconds = parse_duration_seconds(duration).unwrap_or(0);
    if (is_all_day || start.len() == 10)
        && let Ok(date) =
            civil::Date::strptime("%Y-%m-%d", start.split('T').next().unwrap_or(start))
    {
        // `EventTime`'s all-day end is exclusive: end = start + duration
        // days (a P1D JSCalendar all-day event, start D, ends D+1).
        let days = seconds.div_ceil(86_400);
        let end = Span::new()
            .try_days(i64::try_from(days).unwrap_or(i64::MAX))
            .and_then(|span| date.checked_add(span))
            .unwrap_or(date);
        return end.strftime("%Y-%m-%d").to_string();
    }
    let offset = SignedDuration::from_secs(i64::try_from(seconds).unwrap_or(i64::MAX));
    if let Ok(start) = start.parse::<Timestamp>() {
        return shared_rfc3339(start.checked_add(offset).unwrap_or(start));
    }
    if let Some(start) = local_datetime(start) {
        let end = start.checked_add(offset).unwrap_or(start);
        return Offset::UTC
            .to_datetime(end)
            .strftime("%Y-%m-%dT%H:%M:%S")
            .to_string();
    }
    start.to_string()
}

fn parse_duration_seconds(value: &str) -> Option<u64> {
    let value = value.strip_prefix('P')?;
    let mut seconds = 0_u64;
    let mut number = String::new();
    let mut in_time = false;
    for ch in value.chars() {
        match ch {
            'T' => in_time = true,
            'D' => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?.checked_mul(86_400)?)?;
                number.clear();
            }
            'H' if in_time => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?.checked_mul(3_600)?)?;
                number.clear();
            }
            'M' if in_time => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?.checked_mul(60)?)?;
                number.clear();
            }
            'S' if in_time => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?)?;
                number.clear();
            }
            digit if digit.is_ascii_digit() => number.push(digit),
            _ => return None,
        }
    }
    number.is_empty().then_some(seconds)
}

fn rights_can_write(rights: Option<&CalendarRights>) -> bool {
    rights
        .and_then(|rights| rights.may_write_all.or(rights.may_write_own))
        .unwrap_or(false)
}

fn rights_can_delete(rights: Option<&CalendarRights>) -> bool {
    rights.and_then(|rights| rights.may_delete).unwrap_or(false)
}

fn event_status(value: Option<&str>) -> EventStatus {
    // An absent status is `confirmed`, the JMAP calendars default, so
    // `Unknown` means only an unrecognized value.
    match value.unwrap_or("confirmed") {
        "confirmed" => EventStatus::Confirmed,
        "tentative" => EventStatus::Tentative,
        "cancelled" => EventStatus::Cancelled,
        _ => EventStatus::Unknown,
    }
}

fn jmap_event_status(value: EventStatus) -> &'static str {
    match value {
        EventStatus::Tentative => "tentative",
        EventStatus::Cancelled => "cancelled",
        _ => "confirmed",
    }
}

fn availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default() {
        "free" => EventAvailability::Free,
        "busy" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn jmap_availability(value: EventAvailability) -> &'static str {
    match value {
        EventAvailability::Free => "free",
        _ => "busy",
    }
}

fn visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default() {
        "public" => EventVisibility::Public,
        "private" => EventVisibility::Private,
        "secret" => EventVisibility::Confidential,
        _ => EventVisibility::Default,
    }
}

fn jmap_visibility(value: EventVisibility) -> &'static str {
    match value {
        EventVisibility::Public => "public",
        EventVisibility::Private => "private",
        EventVisibility::Confidential => "secret",
        EventVisibility::Default => "public",
        _ => "public",
    }
}

fn rsvp_value(value: RsvpStatus) -> &'static str {
    match value {
        RsvpStatus::Accepted => "accepted",
        RsvpStatus::Declined => "declined",
        RsvpStatus::Tentative => "tentative",
        RsvpStatus::Delegated => "delegated",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown => "needs-action",
        _ => "needs-action",
    }
}

fn rsvp_status(value: Option<&str>) -> Result<RsvpStatus, &'static str> {
    match value {
        Some("accepted") => Ok(RsvpStatus::Accepted),
        Some("declined") => Ok(RsvpStatus::Declined),
        Some("tentative") => Ok(RsvpStatus::Tentative),
        Some("delegated") => Ok(RsvpStatus::Delegated),
        Some("needs-action") => Ok(RsvpStatus::NeedsAction),
        None => Ok(RsvpStatus::Unknown),
        Some(_) => Err("JMAP participant has an unsupported participationStatus"),
    }
}

fn validate_shared_attendees(
    attendees: &[EventAttendee],
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if attendees.iter().any(|attendee| {
        matches!(attendee.role, AttendeeRole::Unknown)
            || matches!(attendee.status, RsvpStatus::Unknown)
    }) {
        return Err(unsupported(
            operation,
            "JMAP cannot serialize an unknown attendee role or participation status",
        ));
    }
    Ok(())
}

/// The calendar page walk's parameters for the shared anchored, state-pinned
/// page cursor (`page_cursor`): the id marker, the query builder, and the
/// wording of its diagnostics. Behaviour is not a parameter.
struct CalendarPages;

impl PageKind for CalendarPages {
    type ItemId = CalendarEventId;
    type Query = crate::calendar_event::CalendarEventQuery;

    const COLLECTION: &'static str = "calendar";
    const ITEM: &'static str = "event";
    const ID_LABEL: &'static str = "event";
    const METHOD: &'static str = "CalendarEvent/query";
    const REPEAT: &'static str = "walk";

    fn query_from_start(
        query: crate::calendar_event::CalendarEventQuery,
    ) -> crate::calendar_event::CalendarEventQuery {
        query.position(0)
    }

    fn query_after(
        query: crate::calendar_event::CalendarEventQuery,
        anchor: &str,
    ) -> crate::calendar_event::CalendarEventQuery {
        query.anchor(anchor).anchor_offset(1)
    }
}

fn limit(value: Option<u32>) -> usize {
    value
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(PAGE_LIMIT)
}

fn usize_to_u64(value: usize, operation: AccountOperation) -> Result<u64, AccountError> {
    u64::try_from(value).map_err(|error| {
        super::error::unsupported_error(
            operation,
            None,
            format!("JMAP calendar total does not fit u64: {error}"),
        )
    })
}

fn require_calendars<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    operation: AccountOperation,
) -> Result<CalendarAccount<T>, AccountError> {
    calendars.ok_or_else(|| unsupported(operation, "JMAP Calendars capability is unavailable"))
}

fn unsupported(operation: AccountOperation, message: &str) -> AccountError {
    super::error::unsupported_error(operation, None, message)
}

fn to_acct_err(
    operation: AccountOperation,
) -> impl FnOnce(crate::Error) -> AccountError + Send + 'static {
    move |error| {
        super::error::into_account_error(error, super::error::JmapErrorContext::new(operation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(value: &str) -> EventTime {
        EventTime {
            value: value.to_string(),
            timezone: None,
        }
    }

    /// Answers `CalendarEvent/query` with a fixed id list and total, and
    /// `CalendarEvent/get` with events in one calendar, so the client-side
    /// calendar filter in `search` has something to discard.
    struct SearchTransport;

    impl crate::core::transport::HttpTransport for SearchTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let arguments = match name.as_str() {
                "CalendarEvent/query" => json!({
                    "accountId": "primary",
                    "queryState": "q1",
                    "canCalculateChanges": false,
                    "position": 0,
                    "total": 9,
                    "ids": ["e1", "e2"]
                }),
                "CalendarEvent/get" => json!({
                    "accountId": "primary",
                    "state": "s1",
                    "list": [{
                        "id": "e1",
                        "calendarIds": {"other": true},
                        "start": "2026-06-01T09:00:00",
                        "duration": "PT1H"
                    }, {
                        "id": "e2",
                        "calendarIds": {"other": true},
                        "status": "cancelled",
                        "start": "2026-06-02T09:00:00",
                        "duration": "PT1H"
                    }],
                    "notFound": []
                }),
                other => panic!("unexpected method {other}"),
            };
            let response = json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    fn search_account() -> CalendarAccount<SearchTransport> {
        let session: crate::core::session::Session = serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:calendars": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:calendars": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:calendars": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses");
        let client = crate::client::Client::with_transport(
            SearchTransport,
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        JmapProtoAccount::new(client, "primary")
    }

    #[tokio::test]
    async fn caller_event_state_is_enforced_without_replacing_it() {
        for state in [Some("old"), Some("s1"), None] {
            let transport = ScriptedQueryTransport::new(Vec::new());
            let expected = state.map(|state| bifrost_types::ServerVersion::StateAt(state.into()));
            let stale = state == Some("old");
            let result = update(
                Some(transport.account()),
                EventId("one".into()),
                EventPatch {
                    expected_version: expected.clone(),
                    ..Default::default()
                },
            )
            .await;
            if stale {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.unwrap();
            }
            let result = delete_if_version(
                Some(transport.account()),
                EventId("one".into()),
                expected.clone(),
            )
            .await;
            if stale {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.unwrap();
            }

            let result = rsvp_if_version(
                Some(transport.account()),
                Vec::new(),
                EventId("one".into()),
                RsvpStatus::Accepted,
                expected.clone(),
            )
            .await;
            if stale {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.unwrap();
            }

            let calls = transport.calls();
            for (_, args) in calls
                .iter()
                .filter(|(method, _)| method == "CalendarEvent/set")
            {
                assert_eq!(args.get("ifInState").and_then(Value::as_str), state);
            }
            assert_eq!(
                calls
                    .iter()
                    .filter(|(method, _)| method == "CalendarEvent/set")
                    .count(),
                3
            );
        }
    }

    #[tokio::test]
    async fn wrong_event_version_kind_is_refused_before_io() {
        let transport = ScriptedQueryTransport::new(Vec::new());
        for expected_version in [
            bifrost_types::ServerVersion::ETag("s1".into()),
            bifrost_types::ServerVersion::StateAt(String::new()),
        ] {
            let error = update(
                Some(transport.account()),
                EventId("one".into()),
                EventPatch {
                    expected_version: Some(expected_version),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert_eq!(
                *error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
            );
        }
        assert!(transport.calls().is_empty());
    }

    /// The server total counts the unfiltered query result, so it is not
    /// the total of a walk that then discards every hit outside the
    /// requested calendar. Reporting it hands the consumer a number its
    /// own pages can never add up to.
    #[tokio::test]
    async fn a_calendar_filtered_search_reports_no_server_total() {
        let unfiltered = search(
            Some(search_account()),
            EventSearchRequest {
                include_cancelled: true,
                ..EventSearchRequest::new("standup")
            },
        )
        .await
        .expect("search");
        assert_eq!(unfiltered.items.len(), 2);
        assert_eq!(unfiltered.estimated_total, Some(9));

        let filtered = search(
            Some(search_account()),
            EventSearchRequest {
                calendar_id: Some(CalendarId("wanted".to_string())),
                include_cancelled: true,
                ..EventSearchRequest::new("standup")
            },
        )
        .await
        .expect("search");
        assert!(filtered.items.is_empty());
        assert_eq!(filtered.estimated_total, None);
    }

    /// The default search drops cancelled events client-side, and the
    /// server total (which counts them) is suppressed for the same reason as
    /// the calendar post-filter.
    #[tokio::test]
    async fn a_default_search_drops_cancelled_events_and_reports_no_total() {
        let page = search(Some(search_account()), EventSearchRequest::new("standup"))
            .await
            .expect("search");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].native_id, "e1");
        assert_eq!(page.items[0].status, EventStatus::Confirmed);
        assert_eq!(page.estimated_total, None);
    }

    #[tokio::test]
    async fn an_include_cancelled_search_keeps_cancelled_events() {
        let page = search(
            Some(search_account()),
            EventSearchRequest {
                include_cancelled: true,
                ..EventSearchRequest::new("standup")
            },
        )
        .await
        .expect("search");
        assert_eq!(
            page.items
                .iter()
                .map(|event| event.native_id.as_str())
                .collect::<Vec<_>>(),
            ["e1", "e2"]
        );
        assert_eq!(page.items[1].status, EventStatus::Cancelled);
    }

    #[test]
    fn jmap_event_status_absent_is_confirmed_and_unrecognized_is_unknown() {
        assert_eq!(event_status(None), EventStatus::Confirmed);
        assert_eq!(event_status(Some("tentative")), EventStatus::Tentative);
        assert_eq!(event_status(Some("x-weird")), EventStatus::Unknown);
    }

    fn create_payload(event: &EventCreate) -> CalendarEventCreate {
        jmap_create_from_event(event, AccountOperation::EventCreate).expect("create payload")
    }

    /// Builds the JMAP patch using only the context the `EventPatch` itself
    /// carries, which is what `update` does when it does not need to read the
    /// current event.
    fn patch_with_patch_context(
        patch: &EventPatch,
        operation: AccountOperation,
    ) -> Result<CalendarEventPatch, AccountError> {
        jmap_patch_from_event_patch(patch, &event_context_from_patch(patch, None), operation)
    }

    fn event(start: &str, end: &str, is_all_day: bool) -> CalendarEvent {
        CalendarEvent {
            id: EventId("e1".to_string()),
            calendar_id: CalendarId("c1".to_string()),
            native_id: "e1".to_string(),
            uid: None,
            etag: None,
            provenance: CalendarProvenance {
                provider: ProtocolKind::Jmap,
                native: "e1".to_string(),
                calendar_native: Some("c1".to_string()),
            },
            title: None,
            description: None,
            location: None,
            start: time(start),
            end: Some(time(end)).filter(|end| !end.value.is_empty()),
            is_all_day,
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

    #[test]
    fn duration_handles_all_day_dates() {
        // All-day ends are exclusive: a single all-day event is start D /
        // end D+1 (P1D); a three-day event is start D / end D+3 (P3D).
        let op = AccountOperation::EventCreate;
        let dur = |start: &str, end: &str, all_day: bool| {
            event_duration(&time(start), &time(end), all_day, op).expect("valid times")
        };
        assert_eq!(dur("2026-06-02", "2026-06-03", true), "P1D");
        assert_eq!(dur("2026-06-02", "2026-06-05", true), "P3D");
        assert_eq!(dur("2026-06-02", "2026-06-05", false), "P3D");
    }

    fn assert_malformed_field(error: &AccountError, expected: &str) {
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        let text = format!("{error:?}");
        assert!(
            text.contains(&format!("field: Some(\"{expected}\")")),
            "expected field {expected}: {text}"
        );
    }

    /// Fails if `event_duration` falls back to a zero-length duration for a
    /// time it cannot parse.
    #[test]
    fn unparseable_bounds_are_malformed_naming_the_field() {
        let op = AccountOperation::EventCreate;
        let bad_end = event_duration(&time("2026-06-02T12:00:00Z"), &time("tomorrow"), false, op)
            .expect_err("garbage end");
        assert_malformed_field(&bad_end, "end");
        let bad_start = event_duration(
            &time("not-a-time"),
            &time("2026-06-02T12:00:00Z"),
            false,
            op,
        )
        .expect_err("garbage start");
        assert_malformed_field(&bad_start, "start");
        let bad_date = validate_start(Some(&time("2026-13-45")), true, op).expect_err("bad date");
        assert_malformed_field(&bad_date, "start");
    }

    /// Fails if an end before its start is clamped instead of refused.
    #[test]
    fn end_before_start_is_malformed() {
        let op = AccountOperation::EventCreate;
        for (start, end, all_day) in [
            ("2026-06-02T13:00:00Z", "2026-06-02T12:00:00Z", false),
            ("2026-06-02T13:00:00", "2026-06-02T12:00:00", false),
            ("2026-06-05", "2026-06-02", true),
        ] {
            let error = event_duration(&time(start), &time(end), all_day, op)
                .expect_err("end before start");
            assert_malformed_field(&error, "end");
        }
    }

    /// Fails if a local date-time pair still collapses to `PT0S` (it did not
    /// parse as an instant) or if mixed shapes are silently compared.
    #[test]
    fn local_and_offset_times_have_real_durations() {
        let op = AccountOperation::EventCreate;
        assert_eq!(
            event_duration(
                &time("2026-06-02T12:00:00"),
                &time("2026-06-02T13:30:00"),
                false,
                op
            )
            .expect("local"),
            "PT5400S"
        );
        assert_eq!(
            event_duration(
                &time("2026-06-02T12:00:00+02:00"),
                &time("2026-06-02T12:00:00Z"),
                false,
                op
            )
            .expect("offset"),
            "PT7200S"
        );
        let mixed = event_duration(
            &time("2026-06-02T12:00:00"),
            &time("2026-06-02T13:00:00Z"),
            false,
            op,
        )
        .expect_err("mixed shapes");
        assert_malformed_field(&mixed, "end");
    }

    /// Fails if create or patch send a request-bound value for an
    /// unparseable time.
    #[test]
    fn create_and_patch_refuse_unparseable_times() {
        let error = jmap_create_from_event(
            &create_with("2026-06-02T12:00:00Z", "later"),
            AccountOperation::EventCreate,
        )
        .expect_err("bad end");
        assert_malformed_field(&error, "end");
        let error = jmap_create_from_event(
            &create_with("garbage", "2026-06-02T12:00:00Z"),
            AccountOperation::EventCreate,
        )
        .expect_err("bad start");
        assert_malformed_field(&error, "start");
        let error = patch_with_patch_context(
            &EventPatch {
                start: Some(time("2026-06-02T12:00:00Z")),
                end: Some(time("garbage")),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect_err("bad patch end");
        assert_malformed_field(&error, "end");
        let error = patch_with_patch_context(
            &EventPatch {
                start: Some(time("garbage")),
                end: Some(time("")),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect_err("bad patch start with blank end");
        assert_malformed_field(&error, "start");
    }

    #[test]
    fn end_from_duration_handles_timed_and_all_day_events() {
        assert_eq!(
            end_from_start_duration("2026-06-02T12:00:00Z", "PT90M", false),
            "2026-06-02T13:30:00+00:00"
        );
        // Exclusive end: start 2026-06-02 + P3D = 2026-06-05.
        assert_eq!(
            end_from_start_duration("2026-06-02", "P3D", true),
            "2026-06-05"
        );
    }

    #[test]
    fn jmap_local_datetime_reads_as_shared_rfc3339() {
        assert_eq!(
            shared_time_from_jmap("2026-06-02T12:00:00", false),
            "2026-06-02T12:00:00+00:00"
        );
        assert_eq!(shared_time_from_jmap("2026-06-02", true), "2026-06-02");
    }

    #[test]
    fn shared_rfc3339_writes_as_jmap_local_datetime() {
        assert_eq!(
            jmap_time_from_shared(
                &EventTime {
                    value: "2026-06-02T12:00:00+02:00".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                false,
                AccountOperation::EventCreate,
            )
            .expect("writable"),
            "2026-06-02T12:00:00"
        );
    }

    /// No unparsed text reaches the wire as a `start`: an empty or
    /// unparseable time is refused as `Request(Malformed)` naming `start`,
    /// whichever caller reached the writer. Restoring the raw fallback
    /// returns the input string instead.
    #[test]
    fn an_unparseable_start_is_refused_not_written_raw() {
        for value in ["", "next tuesday", "2026-13-40"] {
            for is_all_day in [false, true] {
                let error =
                    jmap_time_from_shared(&time(value), is_all_day, AccountOperation::EventUpdate)
                        .expect_err("an unparseable start is refused");
                assert_malformed_field(&error, "start");
                assert_eq!(error.operation(), Some(AccountOperation::EventUpdate));
            }
        }
    }

    #[test]
    fn filter_time_normalizes_offset_to_bare_utc_local_datetime() {
        assert_eq!(
            jmap_utc_filter_time(
                &EventTime {
                    value: "2026-06-02T12:00:00+02:00".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                "start",
            )
            .expect("parseable"),
            "2026-06-02T10:00:00"
        );
    }

    /// Every range bound reaches the server as a LocalDateTime: a bare date is
    /// midnight, a zoneless date-time passes as itself, and an empty or
    /// unparseable bound is refused before any request instead of being sent
    /// raw. Restoring the pass-through sends `2026-06-02` and `soon` as-is.
    #[test]
    fn range_bounds_are_local_datetimes_or_refused() {
        assert_eq!(
            jmap_utc_filter_time(&time("2026-06-02"), "start").expect("date"),
            "2026-06-02T00:00:00"
        );
        assert_eq!(
            jmap_utc_filter_time(&time("2026-06-02T08:30:00"), "end").expect("local"),
            "2026-06-02T08:30:00"
        );
        for value in ["", "soon", "2026-06-02T25:00:00"] {
            let error = range_filter(&EventRange {
                calendar_id: CalendarId("cal".to_string()),
                start: time("2026-06-02T00:00:00Z"),
                end: time(value),
                page_cursor: None,
                limit: None,
            })
            .expect_err("an unparseable bound is refused");
            assert_malformed_field(&error, "end");
            assert_eq!(error.operation(), Some(AccountOperation::EventsInRange));
        }
    }

    #[test]
    fn range_filter_uses_overlap_not_lexicographic_start() {
        let event = event("2026-06-01T23:00:00Z", "2026-06-02T01:00:00Z", false);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-02T23:59:59Z")
        ));
    }

    #[test]
    fn range_filter_includes_all_day_on_window_start() {
        // Single all-day event on 2026-06-02 under the exclusive contract:
        // start 2026-06-02, end 2026-06-03.
        let event = event("2026-06-02", "2026-06-03", true);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_excludes_events_touching_only_an_exclusive_boundary() {
        let before = event("2026-06-01T23:00:00Z", "2026-06-02T00:00:00Z", false);
        let after = event("2026-06-03T00:00:00Z", "2026-06-03T01:00:00Z", false);
        let start = time("2026-06-02T00:00:00Z");
        let end = time("2026-06-03T00:00:00Z");
        assert!(!event_in_range(&before, &start, &end));
        assert!(!event_in_range(&after, &start, &end));
    }

    #[test]
    fn range_query_filter_includes_calendar_and_time_bounds() {
        let value = serde_json::to_value(
            range_filter(&EventRange {
                calendar_id: CalendarId("cal".to_string()),
                start: time("2026-06-02T00:00:00Z"),
                end: time("2026-06-03T00:00:00Z"),
                page_cursor: None,
                limit: None,
            })
            .expect("parseable bounds"),
        )
        .expect("filter json");

        assert_eq!(value["operator"].as_str(), Some("AND"));
        assert_eq!(value["conditions"][0]["inCalendar"].as_str(), Some("cal"));
        assert_eq!(
            value["conditions"][1]["after"].as_str(),
            Some("2026-06-02T00:00:00")
        );
        assert_eq!(
            value["conditions"][2]["before"].as_str(),
            Some("2026-06-03T00:00:00")
        );
    }

    fn event_ids(values: &[&str]) -> Vec<CalendarEventId> {
        values.iter().map(|id| CalendarEventId::new(*id)).collect()
    }

    fn cursor(anchor: &str, query_state: &str) -> crate::sync::page_cursor::PageCursor {
        crate::sync::page_cursor::PageCursor {
            anchor: anchor.to_string(),
            query_state: query_state.to_string(),
        }
    }

    /// Build a page cursor the way a real page would have minted it, so no
    /// test hand-writes the encoding.
    fn page_cursor(anchor: &str, query_state: &str) -> Vec<u8> {
        CalendarPages::next_cursor(
            None,
            0,
            &event_ids(&[anchor]),
            Some(1_000),
            query_state,
            AccountOperation::EventsInRange,
        )
        .expect("valid envelope")
        .expect("cursor")
    }

    /// The first page positions at zero; a continuation carries the anchor
    /// and NO position, so an event destroyed behind the cursor cannot shift
    /// the window.
    #[test]
    fn a_continuation_page_queries_by_anchor_not_position() {
        let first = serde_json::to_value(CalendarPages::anchor_query(
            crate::calendar_event::CalendarEventQuery::new().limit(2),
            None,
        ))
        .expect("query serializes");
        assert_eq!(first.get("position"), Some(&json!(0)));
        assert_eq!(first.get("anchor"), None);

        let next = serde_json::to_value(CalendarPages::anchor_query(
            crate::calendar_event::CalendarEventQuery::new().limit(2),
            Some(&cursor("e2", "q1")),
        ))
        .expect("query serializes");
        assert_eq!(next.get("anchor"), Some(&json!("e2")));
        assert_eq!(next.get("anchorOffset"), Some(&json!(1)));
        assert_eq!(next.get("position"), None);
    }

    /// One scripted `CalendarEvent/query` answer: the ids it serves, the
    /// position it claims to have served from, the server-side total, and
    /// the `queryState` that result order belongs to.
    #[derive(Clone)]
    struct ScriptedPage {
        ids: Vec<&'static str>,
        position: i32,
        total: usize,
        query_state: &'static str,
    }

    /// A `CalendarEvent/query` server that answers from a script, one entry
    /// per query, and records every method call it received.
    ///
    /// A script rather than a simulated list because the interesting cases
    /// are not "what would a correct server serve" - they are the answers a
    /// server gives when its result ORDER moved between two pages, which is
    /// precisely the situation no single list can represent.
    #[derive(Clone)]
    struct ScriptedQueryTransport {
        pages: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<ScriptedPage>>>,
        calls: std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>,
    }

    impl ScriptedQueryTransport {
        fn new(pages: Vec<ScriptedPage>) -> Self {
            Self {
                pages: std::sync::Arc::new(std::sync::Mutex::new(pages.into())),
                calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<(String, Value)> {
            self.calls.lock().expect("calls").clone()
        }

        fn queries(&self) -> Vec<Value> {
            self.calls()
                .into_iter()
                .filter(|(name, _)| name == "CalendarEvent/query")
                .map(|(_, args)| args)
                .collect()
        }

        fn account(&self) -> CalendarAccount<Self> {
            let client = crate::client::Client::with_transport(
                self.clone(),
                calendar_session(),
                "https://example.test/.well-known/jmap",
            )
            .expect("client builds");
            JmapProtoAccount::new(client, "primary")
        }
    }

    impl crate::core::transport::HttpTransport for ScriptedQueryTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            self.calls
                .lock()
                .expect("calls")
                .push((name.clone(), call[1].clone()));
            let arguments =
                match name.as_str() {
                    "CalendarEvent/query" => {
                        let page = self
                            .pages
                            .lock()
                            .expect("pages")
                            .pop_front()
                            .expect("script has a page for this query");
                        json!({
                            "accountId": "primary",
                            "queryState": page.query_state,
                            "canCalculateChanges": false,
                            "position": page.position,
                            "total": page.total,
                            "ids": page.ids
                        })
                    }
                    "CalendarEvent/get" => {
                        let list: Vec<Value> = call[1]["ids"]
                            .as_array()
                            .expect("ids")
                            .iter()
                            .map(|id| {
                                json!({
                                    "id": id,
                                    "calendarIds": {"cal": true},
                                    "start": "2026-06-01T09:00:00",
                                    "duration": "PT1H",
                                    "participants": {"p1": {
                                        "email": "attendee@example.test",
                                        "roles": {"attendee": true},
                                        "participationStatus": "needs-action"
                                    }}
                                })
                            })
                            .collect();
                        json!({
                            "accountId": "primary",
                            "state": "s1",
                            "list": list,
                            "notFound": []
                        })
                    }
                    "CalendarEvent/set" => {
                        // A fixed server state, independent of the production guard wiring.
                        if call[1]
                            .get("ifInState")
                            .is_some_and(|state| state.as_str() != Some("s1"))
                        {
                            return Ok(bytes::Bytes::from(json!({
                            "sessionState": "session-1",
                            "methodResponses": [["error", {"type": "stateMismatch"}, call_id]]
                        }).to_string()));
                        }
                        json!({
                            "accountId": "primary", "oldState": "s1", "newState": "s2",
                            "updated": {"one": null}, "destroyed": ["one"]
                        })
                    }
                    other => panic!("unexpected method {other}"),
                };
            let response = json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    fn calendar_session() -> crate::core::session::Session {
        serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:calendars": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:calendars": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:calendars": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses")
    }

    async fn walk_range(
        transport: &ScriptedQueryTransport,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<Page<CalendarEvent>, AccountError> {
        events_in_range(
            Some(transport.account()),
            EventRange {
                calendar_id: CalendarId("cal".to_string()),
                start: time("2026-01-01T00:00:00Z"),
                end: time("2027-01-01T00:00:00Z"),
                limit: Some(2),
                page_cursor,
            },
        )
        .await
    }

    /// The happy path, and the only shape a continuation is allowed to
    /// succeed on: the result set did NOT move, so both pages report the
    /// same `queryState`. The walk sees every event exactly once, and it got
    /// there by anchoring on page one's last id rather than by offsetting.
    ///
    /// The earlier version of this fixture deleted an event between the
    /// pages while continuing to answer `q1`. That models a server claiming
    /// its ordered result list did not change while it did - a
    /// contradiction, not a scenario worth pinning.
    #[tokio::test]
    async fn an_unmoved_result_set_pages_through_every_event_once() {
        let transport = ScriptedQueryTransport::new(vec![
            ScriptedPage {
                ids: vec!["e0", "e1"],
                position: 0,
                total: 4,
                query_state: "q1",
            },
            ScriptedPage {
                ids: vec!["e2", "e3"],
                position: 2,
                total: 4,
                query_state: "q1",
            },
        ]);

        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = walk_range(&transport, cursor.take()).await.expect("page");
            seen.extend(page.items.iter().map(|event| event.id.0.clone()));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(seen, vec!["e0", "e1", "e2", "e3"], "walk saw {seen:?}");

        let queries = transport.queries();
        assert_eq!(queries.len(), 2, "expected two pages");
        assert_eq!(queries[0].get("position"), Some(&json!(0)));
        assert_eq!(queries[1].get("anchor"), Some(&json!("e1")));
        assert_eq!(queries[1].get("anchorOffset"), Some(&json!(1)));
        assert_eq!(queries[1].get("position"), None);
    }

    /// The defect the state pin exists for. Page one serves `[e0, e1]`; `e2`
    /// is then edited so the server's order becomes `[e2, e0, e1, e3]` and
    /// the state moves to `q2`. Resuming after `e1` legitimately returns
    /// `[e3]` - the anchor is exactly where the server says it is - and `e2`
    /// is gone from the walk forever.
    ///
    /// The anchor cannot see this: nothing about `e1`'s position reveals that
    /// something crossed it. Only the moved `queryState` does, so the page is
    /// refused as a `ConcurrencyConflict` instead of silently dropping `e2`.
    #[tokio::test]
    async fn an_item_reordered_across_the_anchor_refuses_the_continuation() {
        let transport = ScriptedQueryTransport::new(vec![
            ScriptedPage {
                ids: vec!["e0", "e1"],
                position: 0,
                total: 4,
                query_state: "q1",
            },
            ScriptedPage {
                ids: vec!["e3"],
                position: 3,
                total: 4,
                query_state: "q2",
            },
        ]);

        let first = walk_range(&transport, None).await.expect("first page");
        let cursor = first.next_cursor.expect("a second page is promised");

        let error = walk_range(&transport, Some(cursor))
            .await
            .expect_err("a moved result set must not page on");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
    }

    /// The check runs BEFORE anything is exposed. An implementation that
    /// compared the state after hydrating would pass the test above and still
    /// hand the caller rows out of a list it had already decided was the
    /// wrong one, so the bite here is the absence of the `CalendarEvent/get`:
    /// the refused page hydrates nothing and, being an `Err`, carries neither
    /// items nor a successor cursor.
    #[tokio::test]
    async fn a_moved_result_set_is_refused_before_the_page_is_hydrated() {
        let transport = ScriptedQueryTransport::new(vec![ScriptedPage {
            ids: vec!["e7", "e8"],
            position: 2,
            total: 9,
            query_state: "q2",
        }]);

        let error = walk_range(&transport, Some(page_cursor("e1", "q1")))
            .await
            .expect_err("a moved result set must not page on");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );

        let methods: Vec<String> = transport
            .calls()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            methods,
            vec!["CalendarEvent/query".to_string()],
            "the refused page must not hydrate: {methods:?}"
        );
    }

    /// A page that looks final - no ids at all, and a total the walk has
    /// already reached - is still refused when the state moved. Otherwise a
    /// walk whose tail was reordered away reports clean completion, which is
    /// the silent version of the same loss.
    #[tokio::test]
    async fn an_empty_final_page_under_a_moved_state_is_still_refused() {
        let transport = ScriptedQueryTransport::new(vec![ScriptedPage {
            ids: vec![],
            position: 2,
            total: 2,
            query_state: "q2",
        }]);

        let error = walk_range(&transport, Some(page_cursor("e1", "q1")))
            .await
            .expect_err("an empty page under a moved state must not read as done");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
    }

    /// A first page captures the response's own `queryState` into the cursor
    /// it mints, so the continuation has something to compare against at
    /// all, and anchors on the last id the QUERY answered with.
    #[tokio::test]
    async fn a_first_page_pins_the_state_and_anchors_on_the_query_result() {
        let transport = ScriptedQueryTransport::new(vec![ScriptedPage {
            ids: vec!["e0", "e1"],
            position: 0,
            total: 4,
            query_state: "q-first",
        }]);

        let page = walk_range(&transport, None).await.expect("first page");
        let minted = page.next_cursor.expect("a second page is promised");
        assert_eq!(
            CalendarPages::decode_page_cursor(Some(minted), AccountOperation::EventsInRange)
                .expect("decodes"),
            Some(cursor("e1", "q-first"))
        );
    }

    #[test]
    fn create_payload_stamps_jscalendar_types() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: Some("Room".to_string()),
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: vec![EventAttendee {
                email: "a@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            }],
            recurrence: EventRecurrence {
                rrule: Some("FREQ=WEEKLY;BYDAY=MO".to_string()),
                rdate: Vec::new(),
                exdate: Vec::new(),
                recurrence_id: None,
            },
        });

        assert_eq!(create.properties.get("@type"), Some(&json!("Event")));
        assert_eq!(
            create.properties["locations"]["loc1"]["@type"],
            json!("Location")
        );
        assert_eq!(
            create.properties["participants"]["p0"]["@type"],
            json!("Participant")
        );
        assert_eq!(
            create.properties["recurrenceRules"][0]["@type"],
            json!("RecurrenceRule")
        );
        assert_eq!(
            create.properties["recurrenceRules"][0]["byDay"][0]["@type"],
            json!("NDay")
        );
    }

    #[test]
    fn event_patch_recomputes_duration_from_both_bounds() {
        let patch = patch_with_patch_context(
            &EventPatch {
                start: Some(time("2026-06-02T12:00:00Z")),
                end: Some(time("2026-06-02T13:30:00Z")),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");

        assert_eq!(
            patch.properties.get("start").and_then(Value::as_str),
            Some("2026-06-02T12:00:00")
        );
        assert_eq!(
            patch.properties.get("duration").and_then(Value::as_str),
            Some("PT5400S")
        );
    }

    fn create_with(start: &str, end: &str) -> EventCreate {
        EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: None,
            start: time(start),
            end: time(end),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        }
    }

    /// The read path itself can hand a consumer an empty time: a server that
    /// omits the mandatory `start` projects an empty start and no end. This
    /// pins that the empty-start write handling below is reachable from this
    /// crate's own output, not just from other providers.
    #[test]
    fn a_server_event_without_start_projects_an_empty_start_and_no_end() {
        let raw: JmapCalendarEvent = serde_json::from_value(json!({
            "id": "e1",
            "calendarIds": {"c1": true},
            "duration": "PT1H"
        }))
        .expect("event parses");
        let event = event_from_jmap(raw, AccountOperation::EventGet).expect("projects");
        assert!(event.start.value.is_empty());
        assert_eq!(event.end, None);
    }

    /// Fails if `validate_start` is removed from `write_event_create`:
    /// `start: ""` would be sent to the server.
    #[test]
    fn create_with_an_empty_start_is_malformed() {
        let error = jmap_create_from_event(
            &create_with("  ", "2026-06-02T13:00:00Z"),
            AccountOperation::EventCreate,
        )
        .expect_err("empty start");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }

    /// Fails if the blank-end refusal is removed from `write_event_create`:
    /// the create would carry `duration: PT0S`, a zero-length event.
    #[test]
    fn create_with_an_empty_end_is_unsupported_not_a_zero_length_event() {
        let error = jmap_create_from_event(
            &create_with("2026-06-02T12:00:00Z", ""),
            AccountOperation::EventCreate,
        )
        .expect_err("empty end");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
    }

    /// Fails if `validate_start` is removed from
    /// `jmap_patch_from_event_patch`.
    #[test]
    fn patch_with_an_empty_start_is_malformed() {
        let error = patch_with_patch_context(
            &EventPatch {
                start: Some(time("")),
                end: Some(time("2026-06-02T13:00:00Z")),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect_err("empty start");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }

    /// A read-modify-write echoing start with an empty end applies the start
    /// and leaves the stored `duration` alone. Fails if the blank-end filter
    /// is removed: the patch would carry `duration: PT0S` (or be refused as a
    /// single-bound patch).
    #[test]
    fn patch_echoing_start_with_an_empty_end_omits_duration() {
        let patch = patch_with_patch_context(
            &EventPatch {
                title: Some(Some("Renamed".to_string())),
                start: Some(time("2026-06-02T12:00:00Z")),
                end: Some(time("")),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("echo is written");
        assert_eq!(
            patch.properties.get("start").and_then(Value::as_str),
            Some("2026-06-02T12:00:00")
        );
        assert!(patch.properties.get("duration").is_none());
    }

    /// An empty end alone is a no-op for the time fields, not an error and
    /// not a duration write.
    #[test]
    fn patch_with_only_an_empty_end_writes_no_time_fields() {
        let patch = patch_with_patch_context(
            &EventPatch {
                title: Some(Some("Renamed".to_string())),
                end: Some(time("")),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("empty end alone is omitted");
        assert!(patch.properties.get("duration").is_none());
        assert!(patch.properties.get("start").is_none());
    }

    #[test]
    fn event_patch_rejects_single_bound_time_change() {
        for patch in [
            EventPatch {
                start: Some(time("2026-06-02T12:00:00Z")),
                ..EventPatch::default()
            },
            EventPatch {
                end: Some(time("2026-06-02T13:00:00Z")),
                ..EventPatch::default()
            },
        ] {
            let error = patch_with_patch_context(&patch, AccountOperation::EventUpdate)
                .expect_err("single-bound time patch should reject");
            assert!(matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
            ));
        }
    }

    #[test]
    fn recurrence_rule_normalizes_until_to_event_local_datetime() {
        let zoned_start = EventTime {
            value: "2025-01-01T09:00:00+01:00".to_string(),
            timezone: Some("Europe/Oslo".to_string()),
        };
        let utc = jmap_recurrence_rule_from_rrule(
            "FREQ=DAILY;UNTIL=20260601T070000Z",
            Some(&zoned_start),
            false,
        )
        .expect("UTC until");
        assert_eq!(utc["until"].as_str(), Some("2026-06-01T09:00:00"));

        let floating_start = EventTime {
            value: "2025-01-01T09:00:00+00:00".to_string(),
            timezone: None,
        };
        let floating = jmap_recurrence_rule_from_rrule(
            "FREQ=DAILY;UNTIL=20260101T000000",
            Some(&floating_start),
            false,
        )
        .expect("floating until");
        assert_eq!(floating["until"].as_str(), Some("2026-01-01T00:00:00"));
        let date_only = jmap_recurrence_rule_from_rrule(
            "FREQ=DAILY;UNTIL=20260101",
            Some(&time("2025-01-01")),
            true,
        )
        .expect("date until");
        assert_eq!(date_only["until"].as_str(), Some("2026-01-01T00:00:00"));
    }

    #[test]
    fn event_patch_clears_nullable_fields_with_null() {
        let patch = patch_with_patch_context(
            &EventPatch {
                title: Some(None),
                description: Some(None),
                location: Some(None),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");

        assert_eq!(patch.properties.get("title"), Some(&Value::Null));
        assert_eq!(patch.properties.get("description"), Some(&Value::Null));
        assert_eq!(patch.properties.get("locations"), Some(&Value::Null));
    }

    #[test]
    fn recurrence_rule_writes_jscalendar_object() {
        let rule = jmap_recurrence_rule_from_rrule(
            "FREQ=WEEKLY;INTERVAL=2;COUNT=5;BYDAY=MO,WE;BYMONTH=6;BYMONTHDAY=2",
            None,
            false,
        )
        .expect("rrule should convert");

        assert_eq!(rule["frequency"].as_str(), Some("weekly"));
        assert_eq!(rule["interval"].as_u64(), Some(2));
        assert_eq!(rule["count"].as_u64(), Some(5));
        assert_eq!(rule["byDay"][0]["day"].as_str(), Some("mo"));
        assert_eq!(rule["byDay"][1]["day"].as_str(), Some("we"));
        // RFC 8984 section 4.3.3: byMonth is String[], byMonthDay is Int[].
        assert_eq!(rule["byMonth"][0].as_str(), Some("6"));
        assert_eq!(rule["byMonthDay"][0].as_i64(), Some(2));
    }

    #[test]
    fn recurrence_rule_reads_rfc8984_string_by_month() {
        // RFC 8984 types byMonth as String[]; a conforming server's
        // `["12"]` must convert, and an "L"-suffixed leap month (no RRULE
        // representation) must refuse rather than silently drop.
        let rrule = rrule_from_jmap_recurrence_rule(
            &json!({
                "@type": "RecurrenceRule",
                "frequency": "yearly",
                "byMonth": ["12"],
                "byMonthDay": [24]
            }),
            "2026-12-24T18:00:00",
            false,
            Some("UTC"),
        )
        .expect("string byMonth should convert")
        .expect("rrule");
        assert_eq!(rrule, "FREQ=YEARLY;BYMONTH=12;BYMONTHDAY=24");

        // Legacy integer entries stay readable.
        let rrule = rrule_from_jmap_recurrence_rule(
            &json!({"frequency": "yearly", "byMonth": [6]}),
            "2026-06-01T09:00:00",
            false,
            None,
        )
        .expect("integer byMonth should convert")
        .expect("rrule");
        assert_eq!(rrule, "FREQ=YEARLY;BYMONTH=6");

        let leap = rrule_from_jmap_recurrence_rule(
            &json!({"frequency": "yearly", "byMonth": ["5L"]}),
            "2026-05-01T09:00:00",
            false,
            None,
        );
        assert!(leap.is_err(), "leap-month byMonth must refuse");
    }

    #[test]
    fn recurrence_rule_reads_jscalendar_object_as_rrule() {
        let rrule = rrule_from_jmap_recurrence_rule(
            &json!({
                "frequency": "monthly",
                "interval": 1,
                "count": 3,
                "byDay": [{"day": "tu", "nthOfPeriod": 2}],
                "byMonthDay": [14]
            }),
            "2025-01-01T09:00:00",
            false,
            None,
        )
        .expect("jscalendar rule should convert")
        .expect("rrule");

        assert_eq!(
            rrule,
            "FREQ=MONTHLY;INTERVAL=1;COUNT=3;BYDAY=2TU;BYMONTHDAY=14"
        );
    }

    #[test]
    fn zoned_jscalendar_until_writes_inclusive_utc_ical_until() {
        let rrule = rrule_from_jmap_recurrence_rule(
            &json!({
                "@type": "RecurrenceRule",
                "frequency": "daily",
                "until": "2026-06-01T09:00:00"
            }),
            "2025-01-01T09:00:00",
            false,
            Some("Europe/Oslo"),
        )
        .expect("supported rule")
        .expect("rrule");
        assert_eq!(rrule, "FREQ=DAILY;UNTIL=20260601T070000Z");
    }

    #[test]
    fn unknown_jscalendar_recurrence_component_is_not_silently_dropped() {
        let error = rrule_from_jmap_recurrence_rule(
            &json!({
                "@type": "RecurrenceRule",
                "frequency": "daily",
                "byHour": [9]
            }),
            "2025-01-01T09:00:00",
            false,
            None,
        )
        .expect_err("unknown recurrence component must reject");
        assert_eq!(
            error,
            "JMAP recurrence rule contains an unsupported component"
        );
    }

    #[test]
    fn an_all_day_start_is_written_as_a_jscalendar_local_datetime() {
        // Absolute wire values, not a round trip: both directions previously
        // shared a bare-DATE assumption, so a round trip proved nothing.
        assert_eq!(
            jmap_time_from_shared(&time("2026-06-02"), true, AccountOperation::EventCreate)
                .expect("writable"),
            "2026-06-02T00:00:00"
        );
        // And a conformant server's midnight LocalDateTime reads back as the
        // shared bare date.
        assert_eq!(
            shared_time_from_jmap("2026-06-02T00:00:00", true),
            "2026-06-02"
        );
        // The exclusive all-day end still lands a day out from either form.
        assert_eq!(
            end_from_start_duration("2026-06-02T00:00:00", "P1D", true),
            "2026-06-03"
        );
        assert_eq!(
            end_from_start_duration("2026-06-02", "P1D", true),
            "2026-06-03"
        );
    }

    #[test]
    fn recurrence_only_patch_with_until_demands_the_current_event() {
        // A patch that touches nothing but the RRULE cannot supply the start
        // timezone or the all-day flag, so `update` must read them back.
        let recurrence_only = EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=DAILY;UNTIL=20260601T070000Z".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        };
        assert!(event_patch_needs_context(&recurrence_only));
        // Without that read the conversion cannot succeed, and the guard that
        // `update` runs first turns it into Unsupported. (The payload builder
        // also rejects a failed conversion on its own, so this validation is
        // an early door, not the only thing standing between a bad patch and
        // an erased recurrence.)
        let error = validate_shared_recurrence(
            recurrence_only.recurrence.as_ref().expect("recurrence"),
            None,
            false,
            AccountOperation::EventUpdate,
        )
        .expect_err("no context, no conversion");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));

        // A recurrence patch with no UNTIL needs nothing extra.
        let countable = EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=DAILY;COUNT=3".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        };
        assert!(!event_patch_needs_context(&countable));
    }

    #[test]
    fn recurrence_context_falls_back_to_the_current_event() {
        let current = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2025-01-01T09:00:00",
                "timeZone": "Europe/Oslo",
                "showWithoutTime": false
            }))
            .expect("event"),
        };
        let patch = EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=DAILY;UNTIL=20260601T070000Z".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        };
        let context = event_context_from_patch(&patch, Some(&current));
        assert_eq!(
            context
                .start
                .as_ref()
                .and_then(|start| start.timezone.as_deref()),
            Some("Europe/Oslo")
        );
        assert!(!context.is_all_day);

        let built = jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)
            .expect("patch");
        // 07:00 UTC is 09:00 Oslo summer wall clock; the inclusive bound holds.
        assert_eq!(
            built.properties["recurrenceRules"][0]["until"].as_str(),
            Some("2026-06-01T09:00:00")
        );
    }

    #[test]
    fn an_all_day_event_keeps_its_date_start_when_a_patch_omits_is_all_day() {
        let current = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2026-06-01T00:00:00",
                "showWithoutTime": true
            }))
            .expect("event"),
        };
        let patch = EventPatch {
            start: Some(time("2026-06-02")),
            end: Some(time("2026-06-03")),
            ..EventPatch::default()
        };
        assert!(event_patch_needs_context(&patch));
        let context = event_context_from_patch(&patch, Some(&current));
        assert!(context.is_all_day);

        let built = jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)
            .expect("patch");
        assert_eq!(
            built.properties["start"].as_str(),
            Some("2026-06-02T00:00:00")
        );
        assert_eq!(built.properties["duration"].as_str(), Some("P1D"));
    }

    #[test]
    fn a_modified_recurrence_override_is_not_silently_discarded() {
        let overrides: Map<String, Value> = serde_json::from_value(json!({
            "2026-06-03T12:00:00": {"title": "moved"}
        }))
        .expect("overrides");
        assert_eq!(
            recurrence_dates_from_overrides(Some(&overrides)),
            Err(
                "a modified JMAP recurrence override cannot be represented by the shared recurrence model"
            )
        );

        // Pure exclusions and pure additions still project.
        let plain: Map<String, Value> = serde_json::from_value(json!({
            "2026-06-04T12:00:00": {"excluded": true},
            "2026-06-05T12:00:00": {}
        }))
        .expect("overrides");
        let (rdate, exdate) =
            recurrence_dates_from_overrides(Some(&plain)).expect("representable overrides");
        assert_eq!(rdate, vec!["2026-06-05T12:00:00".to_string()]);
        assert_eq!(exdate, vec!["2026-06-04T12:00:00".to_string()]);
    }

    #[test]
    fn a_modified_override_fails_event_hydration() {
        let event = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2026-06-01T09:00:00",
                "duration": "PT1H",
                "recurrenceOverrides": {"2026-06-03T09:00:00": {"title": "moved"}}
            }))
            .expect("event"),
        };
        let error = event_from_jmap(event, AccountOperation::EventGet)
            .expect_err("modified override must reject");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventGet)
        ));
    }

    fn jmap_event(properties: Value) -> JmapCalendarEvent {
        JmapCalendarEvent {
            properties: serde_json::from_value(properties).expect("event"),
        }
    }

    /// One unrepresentable event must cost its own row and nothing else,
    /// and every submitted id the server answered in neither `list` nor
    /// `notFound` must reach `failed_ids` rather than vanish - absence
    /// from `items` reads as a deletion downstream.
    #[test]
    fn unconvertible_and_unanswered_event_ids_ride_failed_ids() {
        let hydrated = reconcile_events(
            vec![
                "e0".to_string(),
                "e1".to_string(),
                "e2".to_string(),
                "e3".to_string(),
            ],
            &[CalendarEventId::new("e0")],
            vec![
                jmap_event(json!({
                    "id": "e1",
                    "start": "2026-06-01T09:00:00",
                    "duration": "PT1H"
                })),
                // Refused by `event_from_jmap`: a modified recurrence
                // override has no shared representation.
                jmap_event(json!({
                    "id": "e2",
                    "start": "2026-06-01T09:00:00",
                    "duration": "PT1H",
                    "recurrenceOverrides": {"2026-06-03T09:00:00": {"title": "moved"}}
                })),
            ],
            AccountOperation::EventSearch,
        );

        assert_eq!(hydrated.events.len(), 1);
        assert_eq!(hydrated.events[0].id.0, "e1");
        let mut failed_ids = hydrated.failed_ids;
        failed_ids.sort();
        assert_eq!(
            failed_ids,
            vec!["e0".to_string(), "e2".to_string(), "e3".to_string()]
        );
    }

    /// A declared `notFound` id and an id the answer simply omitted are
    /// the same loss, and must not be counted twice when they overlap.
    #[test]
    fn a_not_found_event_id_is_reported_once() {
        let hydrated = reconcile_events(
            vec!["e0".to_string()],
            &[CalendarEventId::new("e0")],
            Vec::new(),
            AccountOperation::EventsInRange,
        );
        assert!(hydrated.events.is_empty());
        assert_eq!(hydrated.failed_ids, vec!["e0".to_string()]);
    }

    #[test]
    fn recurrence_patch_clears_or_writes_recurrence_rules() {
        let clear = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(EventRecurrence::default()),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");
        assert_eq!(clear.properties.get("recurrenceRules"), Some(&Value::Null));
        assert_eq!(
            clear.properties.get("recurrenceOverrides"),
            Some(&Value::Null)
        );

        let write = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: vec!["2026-06-03T12:00:00".to_string()],
                    exdate: vec!["2026-06-04T12:00:00".to_string()],
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");
        let rules = write.properties["recurrenceRules"]
            .as_array()
            .expect("rules array");
        assert_eq!(rules[0]["frequency"].as_str(), Some("daily"));
        assert_eq!(rules[0]["count"].as_u64(), Some(2));
        assert_eq!(
            write.properties["recurrenceOverrides"]["2026-06-03T12:00:00"],
            json!({})
        );
        assert_eq!(
            write.properties["recurrenceOverrides"]["2026-06-04T12:00:00"]["excluded"].as_bool(),
            Some(true)
        );
    }

    /// The payload builders must reject a failed RRULE conversion on their
    /// own, not clear or drop the recurrence. The callers validate first,
    /// but an invariant that holds only by call order is not a guard: a
    /// future caller that built a payload without validating would have
    /// silently erased (patch) or omitted (create) the recurrence.
    #[test]
    fn payload_builders_reject_a_failed_rrule_conversion_instead_of_clearing() {
        let bad_recurrence = EventRecurrence {
            rrule: Some("FREQ=DAILY;BYHOUR=9".to_string()),
            ..EventRecurrence::default()
        };

        let error = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(bad_recurrence.clone()),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect_err("a failed conversion must not become a clear");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));

        // An explicit no-rule recurrence still clears.
        let clear = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(EventRecurrence::default()),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("clearing patch");
        assert_eq!(clear.properties.get("recurrenceRules"), Some(&Value::Null));

        let error = jmap_create_from_event(
            &EventCreate {
                calendar_id: CalendarId("cal".to_string()),
                title: None,
                description: None,
                location: None,
                start: time("2026-06-02T12:00:00Z"),
                end: time("2026-06-02T13:00:00Z"),
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: bad_recurrence,
            },
            AccountOperation::EventCreate,
        )
        .expect_err("a failed conversion must not become an omission");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
    }

    /// JSCalendar keeps the organizer in the same `participants` map as the
    /// attendees, so a whole-map attendee write must carry the owner entries
    /// over or the patch silently deletes the organizer from the event.
    /// Driven through the whole path `update` takes - the fetch decision,
    /// the context extraction, and the patch builder - so an ablation of
    /// any one stage fails it.
    #[test]
    fn an_attendee_patch_preserves_the_owner_participant() {
        let current = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2026-06-01T09:00:00",
                "participants": {
                    "owner": {
                        "@type": "Participant",
                        "email": "owner@example.test",
                        "roles": {"owner": true}
                    },
                    "x1": {
                        "@type": "Participant",
                        "email": "old@example.test",
                        "roles": {"attendee": true}
                    }
                }
            }))
            .expect("event"),
        };
        let patch = EventPatch {
            attendees: Some(vec![EventAttendee {
                email: "a@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            }]),
            ..EventPatch::default()
        };
        // An attendee-only patch needs no conversion context, but it still
        // has to read the current event for the owner participants.
        assert!(!event_patch_needs_context(&patch));
        assert!(event_patch_needs_current(&patch));

        let context = event_context_from_patch(&patch, Some(&current));
        let built = jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)
            .expect("patch");
        let participants = built.properties["participants"]
            .as_object()
            .expect("participants map");

        assert_eq!(
            participants["owner"]["email"].as_str(),
            Some("owner@example.test"),
            "the owner participant survives an attendee write"
        );
        assert_eq!(
            participants["owner"]["roles"]["owner"].as_bool(),
            Some(true)
        );
        assert_eq!(participants["p0"]["email"].as_str(), Some("a@example.test"));
        assert_eq!(
            participants.len(),
            2,
            "non-owner participants are replaced by the new attendee list"
        );
    }

    /// The read path surfaces an owner as a `Chair` attendee, so a
    /// read-modify-write of the attendee list includes the organizer's
    /// email. That entry must update the kept owner participant in place -
    /// keeping its owner role and key - rather than duplicating it or
    /// demoting it to a plain chair.
    #[test]
    fn an_attendee_matching_the_owner_updates_it_in_place() {
        let owners: Map<String, Value> = serde_json::from_value(json!({
            "owner": {
                "@type": "Participant",
                "email": "owner@example.test",
                "roles": {"owner": true},
                "participationStatus": "needs-action"
            }
        }))
        .expect("owners");

        let merged = merge_owner_participants(
            &owners,
            &[EventAttendee {
                email: "Owner@Example.Test".to_string(),
                name: Some("The Owner".to_string()),
                role: AttendeeRole::Chair,
                status: RsvpStatus::Accepted,
            }],
        );

        assert_eq!(merged.len(), 1, "no duplicate participant for the owner");
        assert_eq!(merged["owner"]["roles"]["owner"].as_bool(), Some(true));
        assert_eq!(
            merged["owner"]["participationStatus"].as_str(),
            Some("accepted")
        );
        assert_eq!(merged["owner"]["name"].as_str(), Some("The Owner"));
    }

    /// Fresh attendee keys must never collide with a kept owner key, even
    /// when the server happened to store its owner participant under the
    /// same `p{n}` shape the attendee writer mints.
    #[test]
    fn fresh_attendee_keys_skip_a_colliding_owner_key() {
        let owners: Map<String, Value> = serde_json::from_value(json!({
            "p0": {
                "@type": "Participant",
                "email": "owner@example.test",
                "roles": {"owner": true}
            }
        }))
        .expect("owners");

        let merged = merge_owner_participants(
            &owners,
            &[EventAttendee {
                email: "a@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            }],
        );

        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged["p0"]["email"].as_str(),
            Some("owner@example.test"),
            "the owner keeps its key"
        );
        assert_eq!(merged["p1"]["email"].as_str(), Some("a@example.test"));
    }

    #[test]
    fn recurrence_validation_rejects_unsupported_rrule_parts() {
        let recurrence = EventRecurrence {
            rrule: Some("FREQ=DAILY;BYHOUR=9".to_string()),
            rdate: Vec::new(),
            exdate: Vec::new(),
            recurrence_id: None,
        };

        let create =
            validate_shared_recurrence(&recurrence, None, false, AccountOperation::EventCreate)
                .expect_err("unsupported create recurrence should fail");
        assert!(matches!(
            create.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));

        let update =
            validate_shared_recurrence(&recurrence, None, false, AccountOperation::EventUpdate)
                .expect_err("unsupported update recurrence should fail");
        assert!(matches!(
            update.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));
    }

    #[test]
    fn recurrence_overrides_read_simple_rdate_and_exdate() {
        let overrides = serde_json::from_value::<Map<String, Value>>(json!({
            "2026-06-03T12:00:00": {},
            "2026-06-04T12:00:00": {"excluded": true}
        }))
        .expect("override map");
        let (rdate, exdate) =
            recurrence_dates_from_overrides(Some(&overrides)).expect("representable overrides");

        assert_eq!(rdate, vec!["2026-06-03T12:00:00"]);
        assert_eq!(exdate, vec!["2026-06-04T12:00:00"]);
    }

    #[test]
    fn alerts_project_offset_and_absolute_reminders() {
        let alerts = serde_json::from_value(json!({
            "a1": {
                "@type": "Alert",
                "trigger": {
                    "@type": "OffsetTrigger",
                    "offset": "-PT15M"
                },
                "action": "display"
            },
            "a2": {
                "@type": "Alert",
                "trigger": {
                    "@type": "OffsetTrigger",
                    "offset": "PT5M",
                    "relativeTo": "end"
                },
                "action": "email"
            },
            "a3": {
                "@type": "Alert",
                "trigger": {
                    "@type": "AbsoluteTrigger",
                    "when": "2026-06-02T11:00:00Z"
                }
            }
        }))
        .expect("alerts");

        let reminders = reminders_from_alerts(Some(&alerts));

        assert_eq!(reminders.len(), 3);
        assert!(reminders.iter().any(|reminder| reminder.trigger
            == ReminderTrigger::Relative {
                offset: "-PT15M".to_string(),
                relative_to: ReminderRelativeTo::Start,
            }
            && reminder.action.as_deref() == Some("DISPLAY")));
        assert!(reminders.iter().any(|reminder| reminder.trigger
            == ReminderTrigger::Relative {
                offset: "PT5M".to_string(),
                relative_to: ReminderRelativeTo::End,
            }
            && reminder.action.as_deref() == Some("EMAIL")));
        assert!(reminders.iter().any(|reminder| reminder.trigger
            == ReminderTrigger::Absolute("2026-06-02T11:00:00Z".to_string())
            && reminder.action.is_none()));
    }

    #[test]
    fn attendees_read_roles_from_jscalendar_roles() {
        let participants = serde_json::from_value(json!({
            "p1": {
                "email": "required@example.test",
                "roles": {"attendee": true},
                "participationStatus": "accepted"
            },
            "p2": {
                "email": "optional@example.test",
                "roles": {"optional": true}
            },
            "p3": {
                "email": "room@example.test",
                "roles": {"resource": true}
            },
            "p4": {
                "email": "chair@example.test",
                "roles": {"chair": true}
            }
        }))
        .expect("participants");

        let attendees = attendees(Some(&participants)).expect("supported participants");

        assert_eq!(attendees[0].role, AttendeeRole::Required);
        assert_eq!(attendees[1].role, AttendeeRole::Optional);
        assert_eq!(attendees[2].role, AttendeeRole::Resource);
        assert_eq!(attendees[3].role, AttendeeRole::Chair);
        assert_eq!(attendees[0].status, RsvpStatus::Accepted);
    }

    #[test]
    fn participants_write_roles_to_jscalendar_roles() {
        let participants = participants_from_attendees(&[
            EventAttendee {
                email: "required@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            },
            EventAttendee {
                email: "room@example.test".to_string(),
                name: None,
                role: AttendeeRole::Resource,
                status: RsvpStatus::Accepted,
            },
        ]);

        assert_eq!(
            participants["p0"]["roles"]["attendee"].as_bool(),
            Some(true)
        );
        assert_eq!(
            participants["p1"]["roles"]["resource"].as_bool(),
            Some(true)
        );
        assert_eq!(participants["p0"]["expectReply"].as_bool(), Some(true));
        assert_eq!(participants["p1"]["expectReply"].as_bool(), Some(true));
    }

    #[test]
    fn unknown_participation_vocabulary_is_not_presented_as_success() {
        let participants = serde_json::from_value(json!({
            "p0": {
                "email": "future@example.test",
                "roles": {"future-role": true},
                "participationStatus": "future-status"
            }
        }))
        .expect("participants");
        assert!(attendees(Some(&participants)).is_err());
    }

    #[test]
    fn create_payload_writes_organizer_as_owner_participant() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: Some(EventOrganizer {
                email: "owner@example.test".to_string(),
                name: Some("Owner".to_string()),
            }),
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        let participants = create
            .properties
            .get("participants")
            .and_then(Value::as_object)
            .expect("participants");
        assert_eq!(
            participants["owner"]["email"].as_str(),
            Some("owner@example.test")
        );
        assert_eq!(
            participants["owner"]["roles"]["owner"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn rsvp_patch_updates_single_participant_status_path() {
        let participants = serde_json::from_value(json!({
            "owner": {
                "email": "owner@example.test",
                "roles": {"owner": true}
            },
            "p1": {
                "email": "attendee@example.test",
                "roles": {"attendee": true},
                "participationStatus": "needs-action"
            }
        }))
        .expect("participants");

        let patch =
            jmap_rsvp_patch_from_participants(Some(&participants), &[], RsvpStatus::Accepted)
                .expect("single attendee should patch");

        assert_eq!(
            patch
                .properties
                .get("participants/p1/participationStatus")
                .and_then(Value::as_str),
            Some("accepted")
        );
        assert!(!patch.properties.contains_key("participants"));
    }

    #[test]
    fn rsvp_patch_uses_authenticated_email_for_multi_attendee_event() {
        let participants = serde_json::from_value(json!({
            "p1": {
                "email": "one@example.test",
                "roles": {"attendee": true}
            },
            "p2": {
                "email": "Two@Example.Test",
                "roles": {"attendee": true}
            }
        }))
        .expect("participants");

        let patch = jmap_rsvp_patch_from_participants(
            Some(&participants),
            &["two@example.test".to_string()],
            RsvpStatus::Declined,
        )
        .expect("authenticated attendee should patch");

        assert_eq!(
            patch
                .properties
                .get("participants/p2/participationStatus")
                .and_then(Value::as_str),
            Some("declined")
        );
    }

    #[test]
    fn rsvp_patch_refuses_ambiguous_participants_without_authenticated_email() {
        let participants = serde_json::from_value(json!({
            "p1": {
                "email": "one@example.test",
                "roles": {"attendee": true}
            },
            "p2": {
                "email": "two@example.test",
                "roles": {"attendee": true}
            }
        }))
        .expect("participants");

        let error =
            jmap_rsvp_patch_from_participants(Some(&participants), &[], RsvpStatus::Accepted)
                .expect_err("ambiguous attendees should fail");

        assert!(error.contains("multiple attendee participants"));
    }

    #[test]
    fn create_payload_does_not_synthesize_uid() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: Some("Planning".to_string()),
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Tentative,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        assert!(!create.properties.contains_key("uid"));
        assert!(create.properties.contains_key("calendarIds"));
        assert_eq!(create.properties.get("status"), Some(&json!("tentative")));
    }

    #[test]
    fn visibility_maps_to_and_from_jscalendar_privacy() {
        assert_eq!(visibility(Some("public")), EventVisibility::Public);
        assert_eq!(visibility(Some("private")), EventVisibility::Private);
        assert_eq!(visibility(Some("secret")), EventVisibility::Confidential);
        assert_eq!(jmap_visibility(EventVisibility::Confidential), "secret");

        let patch = patch_with_patch_context(
            &EventPatch {
                visibility: Some(EventVisibility::Private),
                status: Some(EventStatus::Cancelled),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");
        assert_eq!(patch.properties.get("privacy"), Some(&json!("private")));
        assert_eq!(patch.properties.get("status"), Some(&json!("cancelled")));
    }

    #[test]
    fn create_payload_writes_privacy() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Confidential,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        assert_eq!(create.properties.get("privacy"), Some(&json!("secret")));
    }

    #[test]
    fn create_payload_writes_recurrence_rules_as_objects() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                rdate: vec!["2026-06-03T12:00:00".to_string()],
                exdate: vec!["2026-06-04T12:00:00".to_string()],
                recurrence_id: None,
            },
        });

        let rules = create.properties["recurrenceRules"]
            .as_array()
            .expect("rules array");
        assert_eq!(rules[0]["frequency"].as_str(), Some("daily"));
        assert_eq!(rules[0]["count"].as_u64(), Some(2));
        assert_eq!(
            create.properties["recurrenceOverrides"]["2026-06-03T12:00:00"],
            json!({})
        );
        assert_eq!(
            create.properties["recurrenceOverrides"]["2026-06-04T12:00:00"]["excluded"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn absent_calendar_rights_are_not_writable() {
        assert!(!rights_can_write(None));
        assert!(!rights_can_delete(None));

        let rights = CalendarRights {
            may_read_free_busy: Some(true),
            may_read_items: Some(true),
            may_write_all: None,
            may_write_own: Some(true),
            may_update_private: None,
            may_rsvp: None,
            may_share: None,
            may_delete: Some(false),
        };
        assert!(rights_can_write(Some(&rights)));
        assert!(!rights_can_delete(Some(&rights)));
    }
}
