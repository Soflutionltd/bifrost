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

use super::error::invalid_argument as malformed_time;
use crate::calendar::{Calendar as JmapCalendar, CalendarId as JmapCalendarId, CalendarRights};
use crate::calendar_event::value::{Duration as CalendarDuration, LocalDateTime};
use crate::calendar_event::{
    CalendarEvent as JmapCalendarEvent, CalendarEventCreate, CalendarEventId, CalendarEventPatch,
};
use crate::core::SetCreate;
use bifrost_types::{
    AccountError, AccountOperation, AttendeeRole, Calendar, CalendarEvent, CalendarId,
    CalendarProvenance, EventAttendee, EventAvailability, EventCreate, EventId, EventOrganizer,
    EventPatch, EventRecurrence, EventReminder, EventStatus, EventTime, EventTimeKind,
    EventVisibility, ProtocolKind, ReminderRelativeTo, ReminderTrigger, RsvpStatus,
};
use jiff::tz::{Offset, TimeZone};
use jiff::{Span, Timestamp, civil};
use serde_json::{Map, Value, json};

pub(super) fn calendar_from_jmap(calendar: JmapCalendar) -> Calendar {
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

pub(super) fn event_from_jmap(
    event: JmapCalendarEvent,
    operation: AccountOperation,
) -> Result<CalendarEvent, AccountError> {
    let native = event
        .id()
        .map(CalendarEventId::into_string)
        .unwrap_or_default();
    let calendar_id = event
        .calendar_ids()
        .and_then(|ids| ids.into_iter().min_by(|a, b| a.as_str().cmp(b.as_str())))
        .map(|id| CalendarId(id.into_string()))
        .unwrap_or_else(|| CalendarId(String::new()));
    let is_all_day = event.show_without_time().unwrap_or(false);
    let raw_start = event.start().unwrap_or_default();
    // List and range reads report projection failures through failed_ids and
    // keep valid siblings: an Outlook user must still see the rest of the calendar.
    // Missing duration means PT0S. Malformed or unrepresentable duration is
    // not zero: users must not see a different end silently fabricated.
    let duration = event
        .duration()
        .unwrap_or("PT0S")
        .parse::<CalendarDuration>()
        .map_err(|message| to_acct_err(operation)(crate::Error::NotParsable(message.into())))?;
    if duration.signed().is_none() {
        return Err(to_acct_err(operation)(crate::Error::NotParsable(
            "JSCalendar duration is outside the representable range".into(),
        )));
    }
    let raw_end =
        end_from_start_duration(raw_start, duration.as_str(), is_all_day).ok_or_else(|| {
            to_acct_err(operation)(crate::Error::NotParsable(
                "JSCalendar event start is invalid or its end is outside the representable range"
                    .into(),
            ))
        })?;
    let timezone = event.time_zone().as_value().map(ToString::to_string);
    // With no start, EventTime is Unspecified and cannot retain showWithoutTime:
    // its all-day sense requires a parsed date. Preserve the missing observation
    // rather than inventing a date or hiding a damaged meeting from the user.
    // Retaining that independent flag requires extending the shared EventTime model.
    let start = EventTime::from_observed(
        shared_time_from_jmap(raw_start, is_all_day),
        timezone.clone(),
    );
    // An absent `duration` is RFC 8984's `PT0S`, a real zero-length end; only
    // an event with no `start` to measure from has no end at all.
    let end = Some(EventTime::from_observed(
        shared_time_from_jmap(&raw_end, is_all_day),
        timezone.clone(),
    ))
    .filter(|end| !end.value().is_empty());
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
        // Type state belongs to /get, not this object. Response adapters
        // attach the selected account and state after projection.
        version: None,
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

pub(super) fn recurrence_from_jmap(
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

pub(super) fn jmap_create_from_event(
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
pub(super) struct EventContext {
    pub(super) start: Option<EventTime>,
    pub(super) is_all_day: bool,
    /// The current event's owner-roled `participants` entries, keyed as the
    /// server stores them. `EventPatch.attendees` describes only attendees,
    /// but JSCalendar keeps the organizer in the same `participants` map, so
    /// a whole-map attendee write must carry these over or it deletes the
    /// organizer. Empty when the patch has no attendees or no read was made.
    pub(super) owner_participants: Map<String, Value>,
}

pub(super) fn rrule_has_until(patch: &EventPatch) -> bool {
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
/// needs the start's timezone. The pre-read also supplies stored context when
/// no explicit conversion is requested; supplied typed bounds override its mode.
pub(super) fn event_patch_needs_context(patch: &EventPatch) -> bool {
    let until = rrule_has_until(patch);
    (until && patch.start.is_none())
        || (patch.is_all_day.is_none() && (until || patch.start.is_some() || patch.end.is_some()))
}

/// Whether `update` must read the current event before building its patch:
/// either the conversion context is missing (`event_patch_needs_context`),
/// or the patch writes the attendee list, whose whole-map write must carry
/// the current owner participants over.
pub(super) fn event_patch_needs_current(patch: &EventPatch) -> bool {
    event_patch_needs_context(patch) || patch.attendees.is_some()
}

pub(super) fn event_context_from_patch(
    patch: &EventPatch,
    current: Option<&JmapCalendarEvent>,
) -> EventContext {
    let is_all_day = patch
        .is_all_day
        .or_else(|| {
            patch
                .start
                .as_ref()
                .filter(|time| !time.is_unspecified())
                .map(EventTime::is_all_day)
        })
        .or_else(|| {
            patch
                .end
                .as_ref()
                .filter(|time| !time.is_unspecified())
                .map(EventTime::is_all_day)
        })
        .or_else(|| current.map(|event| event.show_without_time().unwrap_or(false)))
        .unwrap_or(false);
    let start = patch.start.clone().or_else(|| {
        current.map(|event| {
            EventTime::from_observed(
                shared_time_from_jmap(event.start().unwrap_or_default(), is_all_day),
                event.time_zone().as_value().map(ToString::to_string),
            )
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

pub(super) fn jmap_patch_from_event_patch(
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
            out.time_zone(start.timezone().map(str::to_owned));
            out.duration(value);
        }
        (Some(start), None) if end_is_blank => {
            out.start(jmap_time_from_shared(start, context.is_all_day, operation)?);
            out.time_zone(start.timezone().map(str::to_owned));
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(unsupported(
                operation,
                "JMAP event time patches must set both start and end; JSCalendar derives the end from start + duration",
            ));
        }
        (None, None) => {}
    }
    if patch.is_all_day.is_some() || patch.start.is_some() || end.is_some() {
        out.show_without_time(context.is_all_day);
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
        recurrence
            .parsed_rule()
            .map_err(|error| malformed_time(operation, "recurrence.rrule", error.to_string()))?;
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
pub(super) fn merge_owner_participants(
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

pub(super) fn jmap_rsvp_patch_from_participants(
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

pub(super) fn rsvp_participant_id<'a>(
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

pub(super) fn write_event_create(
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
    validate_start(Some(&event.start), event.is_all_day(), operation)?;
    if is_blank_time(&event.end) {
        return Err(unsupported(
            operation,
            "JMAP cannot create an event with no end: JSCalendar derives the end from start + duration, so an empty end would be stored as a zero-length event; a create needs an explicit end",
        ));
    }
    target.start(jmap_time_from_shared(
        &event.start,
        event.is_all_day(),
        operation,
    )?);
    target.time_zone(event.start.timezone().map(str::to_owned));
    target.duration(event_duration(
        &event.start,
        &event.end,
        event.is_all_day(),
        operation,
    )?);
    target.show_without_time(event.is_all_day());
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
    event.recurrence.parsed_rule().map_err(|error| {
        malformed_time(
            AccountOperation::EventCreate,
            "recurrence.rrule",
            error.to_string(),
        )
    })?;
    if let Some(rrule) = event.recurrence.rrule.as_deref() {
        let rule = jmap_recurrence_rule_from_rrule(rrule, Some(&event.start), event.is_all_day())
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

pub(super) fn recurrence_overrides_from_shared(recurrence: &EventRecurrence) -> Map<String, Value> {
    let mut overrides = Map::new();
    for date in &recurrence.rdate {
        overrides.insert(date.clone(), Value::Object(Map::new()));
    }
    for date in &recurrence.exdate {
        overrides.insert(date.clone(), json!({ "excluded": true }));
    }
    overrides
}

pub(super) fn recurrence_dates_from_overrides(
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

pub(super) fn validate_shared_recurrence(
    recurrence: &EventRecurrence,
    start: Option<&EventTime>,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    recurrence
        .parsed_rule()
        .map_err(|error| malformed_time(operation, "recurrence.rrule", error.to_string()))?;
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
pub(super) fn jmap_recurrence_rule_from_rrule(
    rrule: &str,
    start: Option<&EventTime>,
    is_all_day: bool,
) -> Option<Value> {
    let mut object = Map::new();
    object.insert("@type".to_string(), json!("RecurrenceRule"));
    let parsed = rrule
        .parse::<bifrost_types::calendar::RecurrenceRule>()
        .ok()?;
    for (key, value) in parsed.parts() {
        match key.to_ascii_uppercase().as_str() {
            "FREQ" => {
                let frequency = value.to_ascii_lowercase();
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
                    Value::String(jmap_until_from_ical(parsed.until()?, start, is_all_day)?),
                );
            }
            "BYDAY" => {
                object.insert(
                    "byDay".to_string(),
                    Value::Array(jmap_by_day(parsed.by_day())),
                );
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

pub(super) fn rrule_from_jmap_recurrence_rule(
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
    let text = parts.join(";");
    text.parse::<bifrost_types::calendar::RecurrenceRule>()
        .map_err(|_| "JMAP recurrence rule is invalid")?;
    Ok(Some(text))
}

pub(super) fn jmap_until_from_ical(
    until: bifrost_types::calendar::RecurrenceUntil,
    start: Option<&EventTime>,
    is_all_day: bool,
) -> Option<String> {
    use bifrost_types::calendar::RecurrenceUntil;
    match until {
        RecurrenceUntil::Date(date) if is_all_day => Some(format!("{date}T00:00:00")),
        RecurrenceUntil::DateTime {
            date,
            hour,
            minute,
            second,
            utc,
        } if !is_all_day => {
            let time = civil::Time::new(
                i8::try_from(hour).ok()?,
                i8::try_from(minute).ok()?,
                i8::try_from(second).ok()?,
                0,
            )
            .ok()?;
            let datetime = date.to_datetime(time);
            if utc {
                let timezone = TimeZone::get(start?.timezone()?).ok()?;
                let instant = Offset::UTC.to_timestamp(datetime).ok()?;
                Some(timezone.to_datetime(instant).to_string())
            } else {
                start.filter(|start| start.timezone().is_none())?;
                Some(datetime.to_string())
            }
        }
        _ => None,
    }
}

pub(super) fn ical_until_from_jmap(
    value: &str,
    start: &str,
    is_all_day: bool,
    timezone: Option<&str>,
) -> Result<String, &'static str> {
    let until = value
        .parse::<LocalDateTime>()
        .map_err(|_| "JMAP recurrence until is not a LocalDateTime")?
        .civil();
    if until.subsec_nanosecond() != 0 {
        return Err("an RFC 5545 UNTIL cannot express fractional seconds");
    }
    if is_all_day || EventTime::from_observed(start, None).is_all_day() {
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

pub(super) fn jmap_by_day(days: &[bifrost_types::calendar::RecurrenceDay]) -> Vec<Value> {
    days.iter()
        .map(|day| {
            use jiff::civil::Weekday::*;
            let name = match day.weekday {
                Monday => "mo",
                Tuesday => "tu",
                Wednesday => "we",
                Thursday => "th",
                Friday => "fr",
                Saturday => "sa",
                Sunday => "su",
            };
            let mut object = Map::new();
            object.insert("@type".into(), json!("NDay"));
            object.insert("day".into(), json!(name));
            if let Some(ordinal) = day.ordinal {
                object.insert("nthOfPeriod".into(), json!(ordinal));
            }
            Value::Object(object)
        })
        .collect()
}

pub(super) fn integer_list(value: &str) -> Option<Vec<Value>> {
    value
        .split(',')
        .map(|item| Some(json!(item.parse::<i64>().ok()?)))
        .collect()
}

/// RRULE `BYMONTH=6,12` -> JSCalendar `byMonth: ["6", "12"]`. Validated as
/// integers (RRULE months are numeric), emitted as the String[] RFC 8984
/// section 4.3.3 requires.
pub(super) fn month_string_list(value: &str) -> Option<Vec<Value>> {
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
pub(super) fn rrule_month_list(value: &Value) -> Option<String> {
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

pub(super) fn rrule_by_day(value: &Value) -> Option<String> {
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
            if day.len() != 2 {
                return None;
            }
            let prefix = match object.get("nthOfPeriod") {
                Some(nth) => nth.as_i64()?.to_string(),
                None => String::new(),
            };
            Some(format!("{}{}", prefix, day.to_ascii_uppercase()))
        })
        .collect::<Option<Vec<_>>>()
        .map(|days| days.join(","))
}

pub(super) fn rrule_integer_list(value: &Value) -> Option<String> {
    value
        .as_array()?
        .iter()
        .map(|item| item.as_i64().map(|item| item.to_string()))
        .collect::<Option<Vec<_>>>()
        .map(|items| items.join(","))
}

pub(super) fn participants_from_attendees(attendees: &[EventAttendee]) -> Map<String, Value> {
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

pub(super) fn participants_from_event_create(event: &EventCreate) -> Map<String, Value> {
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

pub(super) fn roles_from_attendee(role: AttendeeRole) -> Option<Map<String, Value>> {
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

pub(super) fn attendees(
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

pub(super) fn attendee_role(value: Option<&Value>) -> Result<AttendeeRole, &'static str> {
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

pub(super) fn role_enabled(roles: &Map<String, Value>, role: &str) -> bool {
    roles.get(role).and_then(Value::as_bool).unwrap_or(false)
}

pub(super) fn organizer(participants: Option<&Map<String, Value>>) -> Option<EventOrganizer> {
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
pub(super) fn reminders_from_alerts(alerts: Option<&Map<String, Value>>) -> Vec<EventReminder> {
    alerts
        .into_iter()
        .flat_map(Map::values)
        .filter_map(reminder_from_alert)
        .collect()
}

pub(super) fn reminder_from_alert(value: &Value) -> Option<EventReminder> {
    let alert = value.as_object()?;
    let trigger = alert.get("trigger").and_then(Value::as_object)?;
    let action = alert
        .get("action")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    let trigger = match trigger.get("@type").and_then(Value::as_str) {
        Some("AbsoluteTrigger") => {
            ReminderTrigger::absolute(trigger.get("when").and_then(Value::as_str)?)
        }
        // OffsetTrigger is the default when @type is absent.
        _ => {
            let offset = trigger.get("offset").and_then(Value::as_str)?;
            let relative_to = match trigger.get("relativeTo").and_then(Value::as_str) {
                Some("end") => ReminderRelativeTo::End,
                _ => ReminderRelativeTo::Start,
            };
            ReminderTrigger::relative(offset, relative_to)
        }
    };
    Some(EventReminder { trigger, action })
}

pub(super) fn first_location(locations: Option<&Map<String, Value>>) -> Option<String> {
    locations
        .and_then(|locations| locations.values().next())
        .and_then(Value::as_object)
        .and_then(|location| location.get("name"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

pub(super) fn single_location(value: &str) -> Map<String, Value> {
    let mut location = Map::new();
    location.insert("@type".to_string(), json!("Location"));
    location.insert("name".to_string(), json!(value));
    let mut locations = Map::new();
    locations.insert("loc1".to_string(), Value::Object(location));
    locations
}

pub(super) fn is_blank_time(time: &EventTime) -> bool {
    time.is_unspecified()
}

/// The shapes of event time this crate maps onto JSCalendar.
pub(super) enum Moment {
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

pub(super) fn moment(time: &EventTime) -> Option<Moment> {
    match time.kind() {
        EventTimeKind::Date(date) => Some(Moment::Date(date)),
        EventTimeKind::Floating(local) | EventTimeKind::Zoned(local) => Some(Moment::Local(local)),
        EventTimeKind::Offset(instant) => Some(Moment::Instant(instant)),
        _ => None,
    }
}

/// Parse one event bound, refusing anything this crate cannot map. An empty
/// value is refused as its own case: JSCalendar `start` is a mandatory
/// LocalDateTime, and the shared model's empty time is what a provider
/// projects for "did not say" (this crate's own read path produces one when a
/// server omits `start`), so a consumer echoing it back would send
/// `start: ""`. An all-day event accepts only a bare date or a zoneless
/// date-time, since its wire form is midnight on the date.
pub(super) fn parse_time(
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
    match moment(time) {
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
pub(super) fn validate_start(
    start: Option<&EventTime>,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if let Some(start) = start {
        parse_time(start, "start", is_all_day, operation)?;
    }
    Ok(())
}

/// The JSCalendar `duration` for a shared start and (exclusive) end.
///
/// Both bounds must parse and the end must not precede the start; anything
/// else is `Request(Malformed)` naming the offending field. There is no
/// fallback duration: a zero-length event is never written for a time this
/// crate could not read.
pub(super) fn event_duration(
    start: &EventTime,
    end: &EventTime,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<CalendarDuration, AccountError> {
    let effective_start = start.converted(is_all_day);
    let effective_end = end.converted(is_all_day);
    let validate_pair = !is_all_day || start.instant().is_none() && end.instant().is_none();
    let start = parse_time(start, "start", is_all_day, operation)?;
    let end = parse_time(end, "end", is_all_day, operation)?;
    if validate_pair {
        effective_start.validate_end(&effective_end).map_err(|_| {
            malformed_time(
                operation,
                "end",
                "JMAP calendar event end must follow start in the same time domain".to_string(),
            )
        })?;
    }
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
        format!("P{days}D")
            .parse::<CalendarDuration>()
            .map_err(|error| malformed_time(operation, "end", error.into()))
    };
    let seconds = |start: Timestamp, end: Timestamp| {
        let elapsed = end.duration_since(start);
        if elapsed.is_negative() {
            return Err(before_start());
        }
        let seconds = elapsed.as_secs();
        let fraction = elapsed.subsec_nanos();
        let text = if fraction == 0 {
            format!("PT{seconds}S")
        } else {
            format!("PT{seconds}.{fraction:09}S")
        };
        text.parse::<CalendarDuration>()
            .map_err(|error| malformed_time(operation, "end", error.into()))
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

pub(super) fn shared_time_from_jmap(value: &str, is_all_day: bool) -> String {
    let time = EventTime::from_observed(value, None);
    if is_all_day {
        return time.converted(true).value().to_owned();
    }
    // Preserve the existing projection's UTC anchoring for wire-local values.
    // EventTime itself does not implicitly anchor floating caller input.
    if let EventTimeKind::Floating(local) = time.kind()
        && let Ok(at) = Offset::UTC.to_timestamp(local)
    {
        return shared_rfc3339(at);
    }
    value.to_string()
}

/// The JSCalendar `start` LocalDateTime for a shared event time.
///
/// The value is parsed here rather than trusted to a prior `validate_start`,
/// so no caller can put raw, unparsed text on the wire: an empty or
/// unparseable time is `Request(Malformed)` naming `start`.
pub(super) fn jmap_time_from_shared(
    time: &EventTime,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<LocalDateTime, AccountError> {
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
        Moment::Instant(_) => time.local_datetime().expect("parsed offset wall clock"),
    };
    LocalDateTime::from_civil(wall)
        .map_err(|error| malformed_time(operation, "start", error.into()))
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
pub(super) fn jmap_utc_filter_time(
    time: &EventTime,
    field: &'static str,
) -> Result<LocalDateTime, AccountError> {
    let wall = match parse_time(time, field, false, AccountOperation::EventsInRange)? {
        Moment::Date(date) => date.to_datetime(civil::Time::MIN),
        Moment::Local(datetime) => datetime,
        Moment::Instant(instant) => Offset::UTC.to_datetime(instant),
    };
    LocalDateTime::from_civil(wall)
        .map_err(|error| malformed_time(AccountOperation::EventsInRange, field, error.into()))
}

/// Render an instant for the shared `EventTime` layer, which spells the
/// UTC offset out as `+00:00` rather than `Z`.
pub(super) fn shared_rfc3339(at: Timestamp) -> String {
    format!("{}+00:00", Offset::UTC.to_datetime(at))
}

pub(super) fn end_from_start_duration(
    start: &str,
    duration: &str,
    is_all_day: bool,
) -> Option<String> {
    let offset = duration.parse::<CalendarDuration>().ok()?.signed()?;
    if start.is_empty() {
        return Some(String::new());
    }
    let start = EventTime::from_observed(start, None);
    start.validate().ok()?;
    // Overflow must reject this event, not fabricate a zero-length appointment.
    if is_all_day || start.is_all_day() {
        let date = start.local_datetime()?.date();
        // All-day ends are exclusive and round partial days upward.
        let nanos = offset.as_nanos();
        let nanos_per_day = 86_400_i128 * 1_000_000_000;
        let days = nanos / nanos_per_day + i128::from(nanos % nanos_per_day != 0);
        let span = Span::new().try_days(i64::try_from(days).ok()?).ok()?;
        return Some(date.checked_add(span).ok()?.to_string());
    }
    if let Some(at) = start.instant() {
        return Some(shared_rfc3339(at.checked_add(offset).ok()?));
    }
    Some(
        start
            .local_datetime()?
            .checked_add(offset)
            .ok()?
            .to_string(),
    )
}

#[cfg(test)]
pub(super) fn parse_duration_seconds(value: &str) -> Option<u64> {
    value.parse::<CalendarDuration>().ok()?.whole_seconds()
}

pub(super) fn rights_can_write(rights: Option<&CalendarRights>) -> bool {
    rights
        .and_then(|rights| rights.may_write_all.or(rights.may_write_own))
        .unwrap_or(false)
}

pub(super) fn rights_can_delete(rights: Option<&CalendarRights>) -> bool {
    rights.and_then(|rights| rights.may_delete).unwrap_or(false)
}

pub(super) fn event_status(value: Option<&str>) -> EventStatus {
    // An absent status is `confirmed`, the JMAP calendars default, so
    // `Unknown` means only an unrecognized value.
    match value.unwrap_or("confirmed") {
        "confirmed" => EventStatus::Confirmed,
        "tentative" => EventStatus::Tentative,
        "cancelled" => EventStatus::Cancelled,
        _ => EventStatus::Unknown,
    }
}

pub(super) fn jmap_event_status(value: EventStatus) -> &'static str {
    match value {
        EventStatus::Tentative => "tentative",
        EventStatus::Cancelled => "cancelled",
        _ => "confirmed",
    }
}

pub(super) fn availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default() {
        "free" => EventAvailability::Free,
        "busy" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

pub(super) fn jmap_availability(value: EventAvailability) -> &'static str {
    match value {
        EventAvailability::Free => "free",
        _ => "busy",
    }
}

pub(super) fn visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default() {
        "public" => EventVisibility::Public,
        "private" => EventVisibility::Private,
        "secret" => EventVisibility::Confidential,
        _ => EventVisibility::Default,
    }
}

pub(super) fn jmap_visibility(value: EventVisibility) -> &'static str {
    match value {
        EventVisibility::Public => "public",
        EventVisibility::Private => "private",
        EventVisibility::Confidential => "secret",
        EventVisibility::Default => "public",
        _ => "public",
    }
}

pub(super) fn rsvp_value(value: RsvpStatus) -> &'static str {
    match value {
        RsvpStatus::Accepted => "accepted",
        RsvpStatus::Declined => "declined",
        RsvpStatus::Tentative => "tentative",
        RsvpStatus::Delegated => "delegated",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown => "needs-action",
        _ => "needs-action",
    }
}

pub(super) fn rsvp_status(value: Option<&str>) -> Result<RsvpStatus, &'static str> {
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

pub(super) fn validate_shared_attendees(
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

fn unsupported(operation: AccountOperation, message: &str) -> AccountError {
    super::error::unsupported_error(
        operation,
        None,
        bifrost_types::UnsupportedReason::DataRepresentation,
        message,
    )
}

fn to_acct_err(operation: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |error| {
        super::error::into_account_error(error, super::error::JmapErrorContext::new(operation))
    }
}
