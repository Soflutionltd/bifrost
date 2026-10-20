use bifrost_types::{
    AttendeeRole, CalendarEvent, CalendarId, CalendarProvenance, EventAttendee, EventAvailability,
    EventCreate, EventId, EventOrganizer, EventPatch, EventRecurrence, EventReminder, EventStatus,
    EventTime, EventVisibility, ProtocolKind, ReminderRelativeTo, ReminderTrigger, RsvpStatus,
};
use caldata::{ContentLineParser, LineReader};
use jiff::tz::{AmbiguousOffset, Offset, TimeZone};
use jiff::{SignedDuration, Timestamp, civil};
use uuid::Uuid;

/// Projection failed because the resource body could not be tokenized into
/// content lines (unclosed quoted parameter, missing property name/value,
/// invalid UTF-8). The caller degrades a single bad `.ics` to a per-resource
/// skip routed through `failed_hrefs` rather than failing the whole sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IcalParseError(pub(crate) String);

/// Direct-resource projection can distinguish an invalid iCalendar body from
/// a valid resource that simply does not contain an event component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EventProjectionError {
    Parse(IcalParseError),
    NoVevent,
    /// The event id named a recurrence instance and no VEVENT of the resource
    /// carries that `RECURRENCE-ID` (an override removed since the listing).
    NoInstance,
    /// The event id named the whole resource, which holds several overrides
    /// and no master, so it addresses none of them in particular.
    AmbiguousResource,
}

/// Split an event id into the resource it lives in and, for a
/// recurrence-qualified id (`{uri}#{recurrence_id}`, the id the listing lanes
/// mint for an override), the `RECURRENCE-ID` value it names.
///
/// The first `#` is the separator, and that is correct because no resource URL
/// holds one. A `#` in an href would be a fragment delimiter, so a conforming
/// server percent-encodes a literal one (`%23`), which passes through here
/// untouched. A server that sends one raw is normalized to `%23` where hrefs are
/// resolved (`parse::resolve_href`), so the ids this crate mints hold exactly
/// one `#` at most. Splitting at the FIRST one, not the last, also keeps a
/// `RECURRENCE-ID` value that itself contained a `#` whole in the instance half.
pub(crate) fn split_event_id(id: &str) -> (&str, Option<&str>) {
    match id.split_once('#') {
        Some((resource, instance)) => (resource, Some(instance)),
        None => (id, None),
    }
}

/// The id a VEVENT is listed under: the resource URL for a master, the
/// recurrence-qualified form for an override. `events_from_ical` and
/// `event_from_ical` both mint through this, so an id a listing produced is
/// the id a direct read of the same VEVENT produces.
fn event_id_for(uri: &str, recurrence_id: Option<&str>) -> EventId {
    match recurrence_id {
        Some(recurrence_id) => EventId(format!("{uri}#{recurrence_id}")),
        None => EventId(uri.to_string()),
    }
}

/// Project the one VEVENT an event id addresses (see `select_block_index`):
/// with `instance` the override carrying that `RECURRENCE-ID`, without it the
/// resource's master. Used by the direct `event_get` / `event_update` paths,
/// and the patch writer splices into the same VEVENT.
pub(crate) fn event_from_ical(
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
    instance: Option<&str>,
) -> Result<CalendarEvent, EventProjectionError> {
    let mut blocks = parse_vevents(data).map_err(EventProjectionError::Parse)?;
    let index = select_block_index(&blocks, instance)?;
    let block = blocks.swap_remove(index);
    Ok(project_event(
        event_id_for(&uri, block_recurrence_id(&block)),
        uri,
        calendar_id,
        etag,
        data,
        block,
    ))
}

/// Project *every* VEVENT in a resource: the master plus each
/// recurrence override / cancellation. Each projected event carries its
/// own `RECURRENCE-ID` (in `recurrence.recurrence_id`) and `STATUS`, so
/// a `CANCELLED` override or a moved instance survives the range/search
/// listing instead of being discarded with the rest of the resource.
///
/// Override instances share the resource's native id but take a
/// recurrence-qualified `EventId` so they do not collide with the master
/// in a consumer index; `native_id` / provenance stay the resource uri.
///
/// A tokenizable resource that holds no VEVENT at all (a VTODO or
/// VJOURNAL sharing the collection, which is legal) yields no events
/// rather than a fabricated empty one: it is not a projection failure -
/// nothing is wrong with the resource and a retry cannot change the
/// outcome - so it belongs in neither the item lane nor `failed_ids`.
pub(crate) fn events_from_ical(
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
) -> Result<Vec<CalendarEvent>, IcalParseError> {
    let blocks = parse_vevents(data)?;
    Ok(blocks
        .into_iter()
        .map(|block| {
            let id = event_id_for(&uri, block_recurrence_id(&block));
            project_event(
                id,
                uri.clone(),
                calendar_id.clone(),
                etag.clone(),
                data,
                block,
            )
        })
        .collect())
}

fn project_event(
    id: EventId,
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
    block: VeventBlock,
) -> CalendarEvent {
    let reminders = reminders_from_valarms(&block.alarms);
    let props = Props(block.props);
    let uid = props.first("UID").map(ToString::to_string);
    let title = props.first("SUMMARY").map(unescape_text);
    let description = props.first("DESCRIPTION").map(unescape_text);
    let location = props.first("LOCATION").map(unescape_text);
    // A real-world emitter (Outlook bridges, some CalDAV servers) can send a
    // TZID DTSTART paired with a floating fallback DTSTART. caldata's typed
    // builder would reject that as a singleton conflict; tokenizing instead
    // lets us keep the event and pick the most specific candidate (VALUE=DATE
    // > TZID > UTC > floating) rather than blindly taking document order.
    let dtstart = pick_datetime(&props.all_with_name("DTSTART"));
    let dtend = pick_datetime(&props.all_with_name("DTEND"));
    let start = dtstart
        .map(event_time_from_property)
        .unwrap_or_else(default_time);
    let end = dtend
        .map(event_time_from_property)
        .or_else(|| {
            props
                .first("DURATION")
                .and_then(|duration| event_end_from_duration(&start, duration))
        })
        .filter(|end| !end.value.is_empty());
    let is_all_day =
        dtstart.is_some_and(Prop::value_type_date) || dtend.is_some_and(Prop::value_type_date);
    let organizer = props
        .first_with_name("ORGANIZER")
        .and_then(organizer_from_property);
    let attendees = props
        .all_with_name("ATTENDEE")
        .into_iter()
        .filter_map(attendee_from_property)
        .collect::<Vec<_>>();
    let recurrence = EventRecurrence {
        rrule: props.first("RRULE").map(ToString::to_string),
        rdate: props
            .all("RDATE")
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        exdate: props
            .all("EXDATE")
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        recurrence_id: props.first("RECURRENCE-ID").map(ToString::to_string),
    };
    CalendarEvent {
        id,
        calendar_id: calendar_id.clone(),
        native_id: uri.clone(),
        uid,
        etag,
        provenance: CalendarProvenance {
            provider: ProtocolKind::CalDav,
            native: uri,
            calendar_native: Some(calendar_id.0),
        },
        title,
        description,
        location,
        start,
        end,
        is_all_day,
        status: event_status(props.first("STATUS")),
        availability: event_availability(props.first("TRANSP")),
        visibility: event_visibility(props.first("CLASS")),
        self_response: RsvpStatus::Unknown,
        organizer,
        attendees,
        reminders,
        recurrence,
        html_link: None,
        raw_ical: Some(data.to_string()),
    }
}

/// A consumer value the writers refuse to put on the wire.
///
/// The iCalendar writers emit `NAME:value` lines by concatenation, so a value
/// holding a line break would inject arbitrary content lines into the stored
/// resource. TEXT values are escaped (`escape_text`) and parameter values are
/// encoded (`escape_param`, RFC 6868), but a value type with no escape form -
/// RECUR, DATE-TIME, CAL-ADDRESS - cannot carry a control character at all. It
/// is REFUSED rather than stripped: a stripped value writes something the
/// caller did not ask for. The account layer maps this to `Request(Malformed)`
/// naming `field`, before any request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteError {
    pub(crate) field: &'static str,
    pub(crate) reason: &'static str,
}

const CONTROL_CHARACTER: &str = "contains a control character";
const EMPTY_VALUE: &str = "is empty";

/// A value type with no escaping (RECUR, DATE-TIME, CAL-ADDRESS URI): any ASCII
/// control character, tab included, is refused, as is an empty value (`NAME:`
/// is not a valid content line for any of them).
fn plain_value<'a>(field: &'static str, value: &'a str) -> Result<&'a str, WriteError> {
    if value.is_empty() {
        return Err(WriteError {
            field,
            reason: EMPTY_VALUE,
        });
    }
    if value.chars().any(|ch| ch.is_ascii_control()) {
        return Err(WriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(value)
}

/// A parameter value, encoded for the wire. RFC 5545 s3.2 forbids control
/// characters and DQUOTE inside one; RFC 6868 (`escape_param`) gives CR, LF and
/// DQUOTE a caret form, so those are encoded. Tab is legal (WSP). Any other
/// control character has no encoding and is refused.
fn param_value(field: &'static str, value: &str) -> Result<String, WriteError> {
    if value
        .chars()
        .any(|ch| ch.is_ascii_control() && !matches!(ch, '\t' | '\r' | '\n'))
    {
        return Err(WriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(escape_param(value))
}

/// A TEXT value (RFC 5545 s3.3.11), escaped for the wire. CR and LF are
/// escaped as the `\n` line break (`escape_text`), and tab is legal. Every
/// other ASCII control character (NUL included) is excluded from TEXT by the
/// grammar and has no escape form, so it is refused and named, never stripped.
fn text_value(field: &'static str, value: &str) -> Result<String, WriteError> {
    if value
        .chars()
        .any(|ch| ch.is_ascii_control() && !matches!(ch, '\t' | '\r' | '\n'))
    {
        return Err(WriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(escape_text(value))
}

pub(crate) fn create_to_ical(event: &EventCreate, uid: &str) -> Result<String, WriteError> {
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//folknor//bifrost//EN".to_string(),
    ];
    // The VEVENT is built first so a zone the parameter writers refuse is
    // named by its parameter field ("start timezone") before the VTIMEZONE
    // block, which spells the same value as TEXT, sees it.
    let mut vevent = vec![
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", text_value("uid", uid)?),
    ];
    push_optional(&mut vevent, "SUMMARY", "title", event.title.as_deref())?;
    push_optional(
        &mut vevent,
        "DESCRIPTION",
        "description",
        event.description.as_deref(),
    )?;
    push_optional(
        &mut vevent,
        "LOCATION",
        "location",
        event.location.as_deref(),
    )?;
    push_start(&mut vevent, &event.start, event.is_all_day)?;
    push_end(&mut vevent, &event.end, event.is_all_day)?;
    vevent.push(format!("STATUS:{}", ical_event_status(event.status)));
    vevent.push(format!("TRANSP:{}", transparency(event.availability)));
    if let Some(classification) = classification(event.visibility) {
        vevent.push(format!("CLASS:{classification}"));
    }
    if let Some(organizer) = &event.organizer {
        vevent.push(organizer_to_line(organizer)?);
    }
    push_recurrence(&mut vevent, &event.recurrence)?;
    for attendee in &event.attendees {
        vevent.push(attendee_to_line(attendee)?);
    }
    push_vtimezones(&mut lines, event)?;
    lines.extend(vevent);
    lines.extend(["END:VEVENT".to_string(), "END:VCALENDAR".to_string()]);
    Ok(fold_ical_lines(lines))
}

/// Why a patch could not be spliced into the resource the server returned.
///
/// The causes blame different parties, so they are kept apart rather than
/// collapsed into one message: the caller maps each to its own kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PatchError {
    /// A recurrence replacement on a resource carrying override VEVENTs. The
    /// shared recurrence model cannot rewrite those instances losslessly, so
    /// this is a request this client cannot express - `Unsupported`.
    RecurrenceOverride,
    /// The body holds no complete VEVENT to splice into, or will not tokenize.
    ///
    /// Unreachable through `event_update` / `event_rsvp`, and so an internal
    /// invariant rather than the provider's fault: the event being patched was
    /// projected from this very body by `parse_vevents`, and the splice works
    /// from that same parse (see `BlockSpan`), so a body that projected an
    /// event always has the VEVENT it came from. It was reachable while the
    /// writer re-read the body with rules of its own, and every way it was
    /// reachable was the two readers disagreeing, not the server erring.
    NoSpliceableVevent,
    /// The event carries no source body to splice into. Unreachable: every
    /// event this crate patches came out of `project_event`, which always sets
    /// `raw_ical`. An internal invariant, never the server's fault or the
    /// caller's.
    MissingSourceBody,
    /// A value the patch would write cannot be put on the wire (see
    /// `WriteError`): a request fault, `Request(Malformed)`.
    Write(WriteError),
}

impl From<WriteError> for PatchError {
    fn from(error: WriteError) -> Self {
        Self::Write(error)
    }
}

pub(crate) fn patch_to_ical(
    current: &CalendarEvent,
    patch: &EventPatch,
) -> Result<String, PatchError> {
    let merged = patch_event(current, patch);
    // Every event this crate patches was projected by `project_event`, which
    // always sets `raw_ical`: the only callers hand in `current` straight from
    // `fetch_event_from_url`. The branch this replaces rebuilt the whole
    // resource from the model with `create_to_ical`, which would have silently
    // dropped every property, VALARM, VTIMEZONE and override VEVENT the model
    // does not carry, and PUT the result as a successful edit. It never ran;
    // if it ever could, refusing is the only safe answer.
    let Some(raw_ical) = current.raw_ical.as_deref() else {
        return Err(PatchError::MissingSourceBody);
    };
    // The same parse the projection ran, so the writer reads exactly the
    // VEVENTs, properties and lines the event was built from.
    let blocks = parse_vevents(raw_ical).map_err(|_| PatchError::NoSpliceableVevent)?;
    // An override is a VEVENT carrying a top-level `RECURRENCE-ID`, the same
    // test `events_from_ical` applies when it mints an override's id - so one
    // nested inside a VALARM or a skipped component is not an override here
    // either.
    if patch.recurrence.is_some() && blocks.iter().any(is_override_block) {
        return Err(PatchError::RecurrenceOverride);
    }
    // The VEVENT `event_from_ical` projected, by the same selection: the event
    // carries the `RECURRENCE-ID` of the override it was read from, and none
    // when it is the master.
    let master = select_block_index(&blocks, current.recurrence.recurrence_id.as_deref())
        .ok()
        .and_then(|index| blocks.get(index))
        .ok_or(PatchError::NoSpliceableVevent)?;

    let mut replacements = Vec::new();
    let mut replace_names = Vec::new();
    if patch.title.is_some() {
        push_optional(
            &mut replacements,
            "SUMMARY",
            "title",
            merged.title.as_deref(),
        )?;
        replace_names.push("SUMMARY");
    }
    if patch.description.is_some() {
        push_optional(
            &mut replacements,
            "DESCRIPTION",
            "description",
            merged.description.as_deref(),
        )?;
        replace_names.push("DESCRIPTION");
    }
    if patch.location.is_some() {
        push_optional(
            &mut replacements,
            "LOCATION",
            "location",
            merged.location.as_deref(),
        )?;
        replace_names.push("LOCATION");
    }
    if patch.start.is_some() || patch.is_all_day.is_some() {
        // A start the caller supplied and left empty is a request for an
        // invalid `DTSTART:`, refused. An empty MERGED start with no start in
        // the patch is the stored event's own (a body with no DTSTART, which
        // the projection tolerates): a patch that only flips `is_all_day`
        // leaves it alone rather than fail on a field it never touched.
        if patch.start.is_some() || !merged.start.value.is_empty() {
            push_start(&mut replacements, &merged.start, merged.is_all_day)?;
            replace_names.push("DTSTART");
        }
    }
    if patch.end.is_some() || patch.is_all_day.is_some() {
        // An empty end is "not stated", and it leaves the stored DTEND and
        // DURATION exactly as they are: the projection yields no end for a
        // body with no DTEND and no DURATION, and ALSO for a DURATION it
        // cannot resolve, so neither an echoed `None` (merged here as the
        // empty time) nor an explicit empty end may delete either.
        if !merged.end.value.is_empty() {
            push_end(&mut replacements, &merged.end, merged.is_all_day)?;
            replace_names.extend(["DTEND", "DURATION"]);
        }
    }
    if let Some(status) = patch.status {
        replacements.push(format!("STATUS:{}", ical_event_status(status)));
        replace_names.push("STATUS");
    }
    if patch.availability.is_some() {
        replacements.push(format!("TRANSP:{}", transparency(merged.availability)));
        replace_names.push("TRANSP");
    }
    if patch.visibility.is_some() {
        if let Some(classification) = classification(merged.visibility) {
            replacements.push(format!("CLASS:{classification}"));
        }
        replace_names.push("CLASS");
    }
    if patch.recurrence.is_some() {
        push_recurrence(&mut replacements, &merged.recurrence)?;
        replace_names.extend(["RRULE", "RDATE", "EXDATE", "RECURRENCE-ID"]);
    }
    if patch.attendees.is_some() {
        for attendee in &merged.attendees {
            replacements.push(attendee_to_line(attendee)?);
        }
        replace_names.push("ATTENDEE");
    }

    let removed = master
        .props
        .iter()
        .zip(&master.span.prop_lines)
        .filter(|(prop, _)| {
            replace_names
                .iter()
                .any(|replace| prop.name.eq_ignore_ascii_case(replace))
        })
        .map(|(_, line)| *line)
        .collect::<Vec<_>>();
    let insert_at = master.span.first_nested.unwrap_or(master.span.end);
    splice_lines(raw_ical, insert_at, &removed, replacements).ok_or(PatchError::NoSpliceableVevent)
}

pub(crate) fn patch_event(current: &CalendarEvent, patch: &EventPatch) -> EventCreate {
    EventCreate {
        calendar_id: patch
            .calendar_id
            .clone()
            .unwrap_or_else(|| current.calendar_id.clone()),
        title: patch.title.clone().unwrap_or_else(|| current.title.clone()),
        description: patch
            .description
            .clone()
            .unwrap_or_else(|| current.description.clone()),
        location: patch
            .location
            .clone()
            .unwrap_or_else(|| current.location.clone()),
        start: patch.start.clone().unwrap_or_else(|| current.start.clone()),
        // An `EventCreate` end is not optional, so no end merges as the empty
        // time, which every writer here reads as "write no DTEND".
        end: patch
            .end
            .clone()
            .or_else(|| current.end.clone())
            .unwrap_or_else(default_time),
        is_all_day: patch.is_all_day.unwrap_or(current.is_all_day),
        status: patch.status.unwrap_or(current.status),
        availability: patch.availability.unwrap_or(current.availability),
        visibility: patch.visibility.unwrap_or(current.visibility),
        organizer: current.organizer.clone(),
        attendees: patch
            .attendees
            .clone()
            .unwrap_or_else(|| current.attendees.clone()),
        recurrence: patch
            .recurrence
            .clone()
            .unwrap_or_else(|| current.recurrence.clone()),
    }
}

pub(crate) fn rsvp_patch(
    current: &CalendarEvent,
    status: RsvpStatus,
    self_email: &str,
) -> Result<EventPatch, &'static str> {
    let mut attendees = current.attendees.clone();
    let attendee = attendees
        .iter_mut()
        .find(|attendee| attendee.email.eq_ignore_ascii_case(self_email))
        .ok_or("authenticated attendee not found")?;
    attendee.status = status;
    Ok(EventPatch {
        attendees: Some(attendees),
        ..EventPatch::default()
    })
}

/// Why an iTIP reply could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyError {
    /// The event lacks something a reply needs (attendee, organizer, uid):
    /// this account cannot answer for it, `Unsupported`.
    Missing(&'static str),
    /// A value of the event cannot be written (see `WriteError`).
    Write(WriteError),
}

impl From<WriteError> for ReplyError {
    fn from(error: WriteError) -> Self {
        Self::Write(error)
    }
}

fn reply_sequence(current: &CalendarEvent) -> Option<String> {
    // CalendarEvent does not project SEQUENCE. Read it from the selected
    // source VEVENT with the same parser and selection rules as the projection.
    let raw_ical = current.raw_ical.as_deref()?;
    let blocks = parse_vevents(raw_ical).ok()?;
    let index = select_block_index(&blocks, current.recurrence.recurrence_id.as_deref()).ok()?;
    let value = blocks[index]
        .props
        .iter()
        .find(|prop| prop.name == "SEQUENCE")?
        .value
        .trim();
    let digits = value.strip_prefix('+').unwrap_or(value);
    if digits.is_empty()
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
        || digits.bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some(digits.to_string())
}

pub(crate) fn rsvp_reply_ical(
    current: &CalendarEvent,
    status: RsvpStatus,
    self_email: &str,
) -> Result<String, ReplyError> {
    let mut attendee = current
        .attendees
        .iter()
        .find(|attendee| attendee.email.eq_ignore_ascii_case(self_email))
        .cloned()
        .ok_or(ReplyError::Missing("authenticated attendee not found"))?;
    attendee.status = status;
    let organizer = current
        .organizer
        .as_ref()
        .ok_or(ReplyError::Missing("event organizer not found"))?;
    let uid = current
        .uid
        .as_deref()
        .ok_or(ReplyError::Missing("event uid not found"))?;
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "METHOD:REPLY".to_string(),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", text_value("uid", uid)?),
    ];
    if let Some(sequence) = reply_sequence(current) {
        lines.push(format!("SEQUENCE:{sequence}"));
    }
    lines.push(format!(
        "DTSTAMP:{}",
        Offset::UTC
            .to_datetime(Timestamp::now())
            .strftime("%Y%m%dT%H%M%SZ")
    ));
    push_start(&mut lines, &current.start, current.is_all_day)?;
    if let Some(end) = &current.end {
        push_end(&mut lines, end, current.is_all_day)?;
    }
    lines.push(organizer_to_line(organizer)?);
    lines.push(attendee_to_line(&attendee)?);
    lines.push("END:VEVENT".to_string());
    lines.push("END:VCALENDAR".to_string());
    Ok(fold_ical_lines(lines))
}

pub(crate) fn new_uid() -> String {
    Uuid::new_v4().to_string()
}

/// One VEVENT component: its top-level properties plus each nested
/// VALARM's properties. RFC 5545 permits only VALARM sub-components
/// inside a VEVENT, so any other nested `BEGIN` line stays a top-level
/// property (harmless - nothing reads it).
#[derive(Debug, Default)]
struct VeventBlock {
    props: Vec<Prop>,
    alarms: Vec<Vec<Prop>>,
    span: BlockSpan,
}

/// An override is a VEVENT carrying a top-level `RECURRENCE-ID`.
fn is_override_block(block: &VeventBlock) -> bool {
    block.props.iter().any(|prop| prop.name == "RECURRENCE-ID")
}

/// The top-level `RECURRENCE-ID` value of a VEVENT, the same first-property
/// read that mints an override's listing id and that `project_event` reports in
/// `recurrence.recurrence_id`.
fn block_recurrence_id(block: &VeventBlock) -> Option<&str> {
    block
        .props
        .iter()
        .find(|prop| prop.name == "RECURRENCE-ID")
        .map(|prop| prop.value.as_str())
}

/// The VEVENT an event id addresses.
///
/// A recurrence-qualified id (`instance` is the `RECURRENCE-ID` value) names the
/// first VEVENT carrying exactly that value - the value the listing lanes
/// minted the id from - and is `NoInstance` when none does. A bare resource id
/// names the master: the first VEVENT that is not an override, wherever it sits
/// in the body (servers do not promise document order). A resource holding only
/// overrides has no master; a lone one is the event (a single edited instance
/// stored alone), and several are `AmbiguousResource`, because answering with
/// the first would hand back a VEVENT the id does not name while the listing
/// showed each of them under its own id. Both the projection and the patch
/// writer select through this, so the VEVENT a patch splices into is the one
/// the event was read from.
fn select_block_index(
    blocks: &[VeventBlock],
    instance: Option<&str>,
) -> Result<usize, EventProjectionError> {
    if blocks.is_empty() {
        return Err(EventProjectionError::NoVevent);
    }
    if let Some(instance) = instance {
        return blocks
            .iter()
            .position(|block| block_recurrence_id(block) == Some(instance))
            .ok_or(EventProjectionError::NoInstance);
    }
    match blocks.iter().position(|block| !is_override_block(block)) {
        Some(index) => Ok(index),
        None if blocks.len() == 1 => Ok(0),
        None => Err(EventProjectionError::AmbiguousResource),
    }
}

/// Where a VEVENT block sits in the body, in caldata `LineReader` line
/// numbers: the physical line each logical line starts on, 1-based.
///
/// This is what the patch writer splices by. It used to re-read the body with
/// its own line grouping and its own `BEGIN`/`END` tracking, and every rule the
/// two readers disagreed on - a folded delimiter, a blank line inside a fold,
/// the case of a name, a trimmed name, a `RECURRENCE-ID` inside a nested
/// component - was a body the projection read one way and the writer another.
/// Recording the projection's own answer leaves nothing to disagree about.
#[derive(Debug, Default)]
struct BlockSpan {
    /// The `END:VEVENT` line.
    end: usize,
    /// The first nested component (`VALARM` or anything the projection skips)
    /// opened at the event's own level, which is where new properties go.
    first_nested: Option<usize>,
    /// The line of each entry in `props`, index for index.
    prop_lines: Vec<usize>,
}

/// Tokenize a resource into every VEVENT block using caldata's streaming
/// content line tokenizer. caldata unfolds (stripping exactly one WSP per
/// RFC 5545 sec 3.1, not the whole leading run the old hand-rolled
/// unfolder ate) and splits quoted parameter values that legally contain
/// `:`/`;`/`,` - both classes of bug the previous `split_once(':')` path
/// had. Values are stored raw (caldata never unescapes); text fields are
/// unescaped at the point we read them into the model.
///
/// Every VEVENT is returned in document order (master first, then each
/// recurrence override / cancellation), and each block's nested VALARMs
/// are captured separately so reminders project without polluting the
/// event's own property list.
fn parse_vevents(data: &str) -> Result<Vec<VeventBlock>, IcalParseError> {
    let mut blocks = Vec::new();
    let mut current: Option<VeventBlock> = None;
    let mut alarm: Option<Vec<Prop>> = None;
    // How deep we are inside a nested component this parser does not model.
    let mut unknown_depth: usize = 0;
    for line in LineReader::from_slice(data.as_bytes()) {
        let line = line.map_err(|error| IcalParseError(error.to_string()))?;
        let number = line.number();
        let normalized = normalize_exchange_cn_param(line.as_str());
        let mut parser = ContentLineParser::from_slice(normalized.as_bytes());
        let line = parser
            .next()
            .expect("one logical line produces one content line")
            .map_err(|error| IcalParseError(error.to_string()))?;
        let name = line.name;
        // Inside a sub-component this VEVENT projection does not model
        // (vendor X-components exist in the wild, and VTODO/VJOURNAL can
        // nest), every line belongs to that component and NONE of it belongs
        // to the event. Feeding them into the master's prop list let
        // `pick_datetime`'s specificity ladder prefer a sub-component's
        // DTSTART over the event's own.
        if unknown_depth > 0 {
            if name == "BEGIN" {
                unknown_depth += 1;
            } else if name == "END" {
                unknown_depth -= 1;
            }
            continue;
        }
        if name == "BEGIN" && line.value.eq_ignore_ascii_case("VEVENT") {
            current = Some(VeventBlock::default());
            continue;
        }
        if name == "END" && line.value.eq_ignore_ascii_case("VEVENT") {
            if let Some(mut block) = current.take() {
                block.span.end = number;
                blocks.push(block);
            }
            alarm = None;
            unknown_depth = 0;
            continue;
        }
        let Some(block) = current.as_mut() else {
            // Content outside any VEVENT (VTIMEZONE, VCALENDAR props).
            continue;
        };
        if name == "BEGIN" && line.value.eq_ignore_ascii_case("VALARM") {
            block.span.first_nested.get_or_insert(number);
            alarm = Some(Vec::new());
            continue;
        }
        if name == "END" && line.value.eq_ignore_ascii_case("VALARM") {
            if let Some(alarm) = alarm.take() {
                block.alarms.push(alarm);
            }
            continue;
        }
        // Any other nested component - a vendor X-component, a nested
        // VTODO - is skipped whole, including a VALARM inside it, until its
        // END. A stray END with nothing open is ignored rather than treated
        // as a property.
        if name == "BEGIN" {
            block.span.first_nested.get_or_insert(number);
            unknown_depth = 1;
            continue;
        }
        if name == "END" {
            continue;
        }
        let prop = Prop {
            name,
            params: line.params,
            value: line.value,
        };
        match alarm.as_mut() {
            Some(alarm) => alarm.push(prop),
            None => {
                block.props.push(prop);
                block.span.prop_lines.push(number);
            }
        }
    }
    Ok(blocks)
}

/// Exchange sometimes emits an unquoted text-escaped CN (`CN=Doe\\, John`).
/// RFC 5545 requires a quoted parameter value here, and caldata correctly
/// treats an unquoted comma as a parameter-value separator, so the legacy
/// form has to be repaired before tokenization or the display name is
/// truncated at the comma.
///
/// The repair is deliberately narrow: only an unquoted CN value that
/// actually carries an RFC 5545 *text* escape (`\,`, `\;`, `\n`) is
/// rewritten, and the rewrite resolves those escapes here and re-encodes
/// the result with [`escape_param`]. Confining the tolerance to this one
/// legacy shape is what lets the CN read path stay a pure RFC 6868 decode:
/// a CN that merely contains a literal backslash is left alone and
/// round-trips verbatim, which a blanket `unescape_text` on every CN would
/// silently eat. RFC 6868 defines no escape for a backslash, so verbatim is
/// both the conformant encoding and the only one that survives a round trip.
fn normalize_exchange_cn_param(line: &str) -> String {
    let bytes = line.as_bytes();
    let mut index = 0;
    let mut in_quotes = false;
    let mut escaped = false;

    while index < bytes.len() {
        match bytes[index] {
            b'\\' if !in_quotes => escaped = !escaped,
            b'"' if !escaped => in_quotes = !in_quotes,
            b';' if !in_quotes && !escaped => {
                let name_start = index + 1;
                let mut name_end = name_start;
                while name_end < bytes.len()
                    && bytes[name_end] != b'='
                    && bytes[name_end] != b';'
                    && bytes[name_end] != b':'
                {
                    name_end += 1;
                }
                if bytes.get(name_end) == Some(&b'=')
                    && line[name_start..name_end].eq_ignore_ascii_case("CN")
                    && bytes.get(name_end + 1) != Some(&b'"')
                {
                    let value_start = name_end + 1;
                    let mut value_end = value_start;
                    let mut value_escaped = false;
                    while value_end < bytes.len() {
                        match bytes[value_end] {
                            b'\\' => value_escaped = !value_escaped,
                            b';' | b':' if !value_escaped => break,
                            _ => value_escaped = false,
                        }
                        value_end += 1;
                    }
                    let value = &line[value_start..value_end];
                    // Only a separator escape marks the value as legacy: a
                    // bare `\` (or `\n`) in an unquoted value does not
                    // confuse the tokenizer, so it stays verbatim.
                    if value.contains("\\,") || value.contains("\\;") {
                        return format!(
                            "{}{}{}",
                            &line[..value_start],
                            escape_param(&unescape_text(value)),
                            &line[value_end..]
                        );
                    }
                }
            }
            b':' if !in_quotes && !escaped => break,
            _ => escaped = false,
        }
        index += 1;
    }
    line.to_string()
}

/// Project each VALARM into an `EventReminder`, dropping alarms with no
/// usable TRIGGER.
fn reminders_from_valarms(alarms: &[Vec<Prop>]) -> Vec<EventReminder> {
    alarms
        .iter()
        .filter_map(|alarm| reminder_from_valarm(alarm))
        .collect()
}

fn reminder_from_valarm(alarm: &[Prop]) -> Option<EventReminder> {
    let trigger_prop = alarm.iter().find(|prop| prop.name == "TRIGGER")?;
    let trigger = trigger_from_property(trigger_prop)?;
    let action = alarm
        .iter()
        .find(|prop| prop.name == "ACTION")
        .map(|prop| prop.value.to_ascii_uppercase());
    Some(EventReminder { trigger, action })
}

/// Read a VALARM TRIGGER. Default value type is DURATION (relative to
/// the event start, or end when `RELATED=END`); `VALUE=DATE-TIME`
/// (or a bare UTC date-time) is an absolute trigger.
fn trigger_from_property(prop: &Prop) -> Option<ReminderTrigger> {
    let value = prop.value.trim();
    if value.is_empty() {
        return None;
    }
    let looks_duration = value
        .strip_prefix(['-', '+'])
        .unwrap_or(value)
        .starts_with('P');
    let is_duration = match prop.param("VALUE") {
        Some(kind) if kind.eq_ignore_ascii_case("DATE-TIME") => false,
        Some(kind) if kind.eq_ignore_ascii_case("DURATION") => true,
        _ => looks_duration,
    };
    if is_duration {
        let relative_to = match prop.param("RELATED") {
            Some(related) if related.eq_ignore_ascii_case("END") => ReminderRelativeTo::End,
            _ => ReminderRelativeTo::Start,
        };
        Some(ReminderTrigger::Relative {
            offset: value.to_string(),
            relative_to,
        })
    } else {
        Some(ReminderTrigger::Absolute(format_ical_time(
            value, false, false,
        )))
    }
}

/// Order DTSTART/DTEND candidates by descending specificity so a duplicate
/// property does not silently project to document order. Mirrors ratatoskr's
/// precedence ladder: VALUE=DATE > TZID > UTC (`Z`) > floating.
fn pick_datetime<'a>(candidates: &[&'a Prop]) -> Option<&'a Prop> {
    candidates
        .iter()
        .copied()
        .max_by_key(|prop| datetime_specificity(prop))
}

fn datetime_specificity(prop: &Prop) -> u8 {
    if prop.value_type_date() {
        3
    } else if prop.param("TZID").is_some() {
        2
    } else if prop.value.ends_with('Z') {
        1
    } else {
        0
    }
}

#[derive(Debug)]
struct Props(Vec<Prop>);

impl Props {
    fn first(&self, name: &str) -> Option<&str> {
        self.first_with_name(name).map(|prop| prop.value.as_str())
    }

    fn all(&self, name: &str) -> Vec<&str> {
        self.all_with_name(name)
            .into_iter()
            .map(|prop| prop.value.as_str())
            .collect()
    }

    fn first_with_name(&self, name: &str) -> Option<&Prop> {
        self.0.iter().find(|prop| prop.name == name)
    }

    fn all_with_name(&self, name: &str) -> Vec<&Prop> {
        self.0.iter().filter(|prop| prop.name == name).collect()
    }
}

#[derive(Debug)]
struct Prop {
    name: String,
    params: caldata::parser::ContentLineParams,
    value: String,
}

impl Prop {
    fn param(&self, name: &str) -> Option<&str> {
        self.params.get_param(name)
    }

    /// A parameter carrying free text (CN, TZID) rather than a grammar
    /// token. caldata hands back the raw parameter, so the RFC 6868 caret
    /// decoding that undoes [`escape_param`] happens here; without it a
    /// display name written by us - or by any of the many servers that
    /// caret-encode - reads back with `^'` in it.
    fn text_param(&self, name: &str) -> Option<String> {
        self.param(name).map(unescape_param)
    }

    fn value_type_date(&self) -> bool {
        self.param("VALUE")
            .is_some_and(|value| value.eq_ignore_ascii_case("DATE"))
    }
}

fn event_time_from_property(prop: &Prop) -> EventTime {
    let tzid = prop.text_param("TZID");
    EventTime {
        value: format_ical_time(&prop.value, prop.value_type_date(), tzid.is_some()),
        // Map Microsoft/Windows zone names ("W. Europe Standard Time") to
        // their IANA equivalent when caldata's table knows them; otherwise
        // pass the TZID through verbatim.
        timezone: tzid.as_deref().map(canonical_tzid),
    }
}

/// Resolve a raw TZID to its IANA name when it is a known Microsoft/Windows
/// zone alias, leaving already-IANA (or unknown) names untouched.
fn canonical_tzid(tzid: &str) -> String {
    caldata::types::get_proprietary_tzid(tzid)
        .map(|tz| tz.name().to_string())
        .unwrap_or_else(|| tzid.to_string())
}

fn format_ical_time(value: &str, is_date: bool, has_tzid: bool) -> String {
    if !value.is_ascii() {
        return value.to_string();
    }
    if is_date && value.len() == 8 {
        // All-day dates project verbatim. The iCalendar all-day DTEND is
        // already exclusive and `EventTime`'s all-day contract is exclusive
        // too, so an end date is NOT decremented to an inclusive last day
        // (that would diverge from bifrost-google, which passes Google's
        // exclusive end through verbatim).
        return format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]);
    }
    if value.len() >= 15 {
        let suffix = ical_offset_suffix(value);
        let mut formatted = format!(
            "{}-{}-{}T{}:{}:{}{}",
            &value[0..4],
            &value[4..6],
            &value[6..8],
            &value[9..11],
            &value[11..13],
            &value[13..15],
            suffix
        );
        if value.ends_with('Z') {
            return formatted;
        }
        if !suffix.is_empty() {
            // Offset-bearing and already in canonical second-precision RFC
            // 3339 form; parsing only confirms the reassembled value is well
            // formed, so it is emitted verbatim.
            return formatted;
        }
        // A TZID-bearing local time is a wall-clock value, not UTC: leave it
        // bare so the `timezone` field is the sole source of the zone. Only a
        // truly floating time (no TZID, no offset) is normalized to UTC `Z`.
        if !has_tzid {
            formatted.push('Z');
        }
        return formatted;
    }
    value.to_string()
}

fn ical_offset_suffix(value: &str) -> String {
    if !value.is_ascii() {
        return String::new();
    }
    if value.ends_with('Z') {
        return "Z".to_string();
    }
    if value.len() == 20 {
        let offset = &value[15..20];
        if matches!(&offset[0..1], "+" | "-") {
            return format!("{}:{}", &offset[0..3], &offset[3..5]);
        }
    }
    String::new()
}

fn event_end_from_duration(start: &EventTime, duration: &str) -> Option<EventTime> {
    // `caldata` still speaks chrono, so cross the boundary as a plain
    // second count rather than naming its duration type. This is the ONLY
    // place bifrost crosses that boundary: no bifrost source names chrono or
    // chrono-tz any more, and they remain in the dependency tree solely
    // because caldata depends on both. Converting through `.num_seconds()`
    // instead of the type is deliberate - it keeps caldav free of a chrono
    // dependency - but it is a structural shim rather than a checked
    // conversion: if caldata ever changed `parse_duration` to return some
    // other type that also exposes `num_seconds`, this would keep compiling
    // and silently change meaning. Nothing to fix while caldata speaks
    // chrono; the shim goes away when caldata drops it or is replaced.
    let raw_duration = duration;
    let duration =
        SignedDuration::from_secs(caldata::types::parse_duration(duration).ok()?.num_seconds());
    let value = if let Ok(value) = start.value.parse::<Timestamp>() {
        let end = Offset::UTC.to_datetime(value.checked_add(duration).ok()?);
        if start.value.ends_with('Z') {
            end.strftime("%Y-%m-%dT%H:%M:%SZ").to_string()
        } else {
            end.strftime("%Y-%m-%dT%H:%M:%S+00:00").to_string()
        }
    } else if let Ok(value) = civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S", &start.value) {
        // RFC 5545 s3.3.6 splits a duration in two: the DAY and WEEK parts are
        // NOMINAL (a day is "the same wall time tomorrow", DST or not) while
        // the time parts are EXACT seconds. Civil addition is right for the
        // first and wrong for the second, and this branch is exactly where a
        // TZID-bearing DTSTART lands - so an exact duration under a known zone
        // is added through the zone, and everything else keeps the civil
        // arithmetic that was already correct for it.
        match exact_zone_end(&start.timezone, value, duration, raw_duration) {
            Some(end) => end,
            None => value.checked_add(duration).ok()?,
        }
        .strftime("%Y-%m-%dT%H:%M:%S")
        .to_string()
    } else if let Ok(value) = civil::Date::strptime("%Y-%m-%d", &start.value) {
        value
            .to_datetime(civil::Time::MIN)
            .checked_add(duration)
            .ok()?
            .date()
            .strftime("%Y-%m-%d")
            .to_string()
    } else {
        return None;
    };
    Some(EventTime {
        value,
        timezone: start.timezone.clone(),
    })
}

/// Add an EXACT (time-only) duration to a wall clock through its named zone,
/// so a `DTSTART;TZID=...` plus `PT10H` crossing a DST transition lands on the
/// wall clock that is really that many seconds later.
///
/// Returns `None` - meaning "use civil arithmetic" - when the duration carries
/// a nominal day or week part, when there is no zone, or when the zone is not
/// in the tzdb. Ambiguity discipline matches the rest of this module: a
/// fall-back fold takes the earlier instant, a spring-forward gap the post-gap
/// offset.
fn exact_zone_end(
    timezone: &Option<String>,
    start: civil::DateTime,
    duration: SignedDuration,
    raw_duration: &str,
) -> Option<civil::DateTime> {
    if !duration_is_exact(raw_duration) {
        return None;
    }
    let tz = TimeZone::get(&canonical_tzid(timezone.as_deref()?)).ok()?;
    let start = match tz.to_ambiguous_timestamp(start).offset() {
        AmbiguousOffset::Unambiguous { offset } => offset,
        AmbiguousOffset::Fold { before, .. } => before,
        AmbiguousOffset::Gap { after, .. } => after,
    }
    .to_timestamp(start)
    .ok()?;
    Some(tz.to_datetime(start.checked_add(duration).ok()?))
}

/// True when every component of an RFC 5545 duration is an exact one. `D` and
/// `W` are nominal by the spec and stay on the civil path.
fn duration_is_exact(duration: &str) -> bool {
    !duration
        .chars()
        .any(|character| matches!(character.to_ascii_uppercase(), 'D' | 'W'))
}

fn ical_time_from_event_time(time: &EventTime, is_all_day: bool) -> String {
    if is_all_day {
        // `EventTime`'s all-day end is exclusive, matching iCalendar's
        // exclusive DTEND, so the date serializes verbatim - no +1 day.
        return time.value.replace('-', "");
    }
    if let Ok(instant) = time.value.parse::<Timestamp>() {
        if time.timezone.is_some() {
            // Under a TZID the wall clock is what belongs on the wire. An
            // offset-carrying value renders in its own offset (the civil
            // parser accepts and drops a numeric offset); a `Z` value names
            // an instant, which must be projected into the TZID's wall clock
            // - emitting the UTC wall clock under the zone label would shift
            // the event by the zone offset.
            let wall = time
                .value
                .parse::<civil::DateTime>()
                .unwrap_or_else(|_| instant_zone_wall(time.timezone.as_deref(), instant));
            return wall.strftime("%Y%m%dT%H%M%S").to_string();
        }
        return Offset::UTC
            .to_datetime(instant)
            .strftime("%Y%m%dT%H%M%SZ")
            .to_string();
    }
    let mut value = time.value.replace(['-', ':'], "");
    if time.timezone.is_some() {
        value = value.trim_end_matches('Z').to_string();
    }
    value
}

fn default_time() -> EventTime {
    EventTime {
        value: String::new(),
        timezone: None,
    }
}

/// An absent `STATUS` projects as `Confirmed`. RFC 5545 defines NO default for
/// `STATUS`; this follows universal practice (every calendar client treats an
/// event with no `STATUS` as confirmed) rather than spec text. `Unknown` then
/// means only an unrecognized value, such as `STATUS:X-WEIRD`.
fn event_status(value: Option<&str>) -> EventStatus {
    match value.unwrap_or("CONFIRMED").to_ascii_uppercase().as_str() {
        "CONFIRMED" => EventStatus::Confirmed,
        "TENTATIVE" => EventStatus::Tentative,
        "CANCELLED" => EventStatus::Cancelled,
        _ => EventStatus::Unknown,
    }
}

fn ical_event_status(status: EventStatus) -> &'static str {
    match status {
        EventStatus::Tentative => "TENTATIVE",
        EventStatus::Cancelled => "CANCELLED",
        EventStatus::Confirmed | EventStatus::Unknown => "CONFIRMED",
        _ => "CONFIRMED",
    }
}

fn organizer_from_property(prop: &Prop) -> Option<EventOrganizer> {
    Some(EventOrganizer {
        email: mailto(&prop.value)?,
        // A pure RFC 6868 decode. Legacy Exchange text escapes are resolved
        // before tokenization by `normalize_exchange_cn_param`; applying
        // `unescape_text` again here would eat a literal backslash.
        name: prop.text_param("CN"),
    })
}

fn organizer_to_line(organizer: &EventOrganizer) -> Result<String, WriteError> {
    let mut line = String::from("ORGANIZER");
    if let Some(name) = &organizer.name {
        line.push_str(";CN=");
        line.push_str(&param_value("organizer name", name)?);
    }
    line.push_str(":mailto:");
    line.push_str(plain_value("organizer email", &organizer.email)?);
    Ok(line)
}

fn attendee_from_property(prop: &Prop) -> Option<EventAttendee> {
    Some(EventAttendee {
        email: mailto(&prop.value)?,
        name: prop.text_param("CN"),
        role: attendee_role(prop.param("ROLE")),
        status: rsvp_status(prop.param("PARTSTAT")),
    })
}

fn attendee_to_line(attendee: &EventAttendee) -> Result<String, WriteError> {
    let mut line = String::from("ATTENDEE");
    if let Some(name) = &attendee.name {
        line.push_str(";CN=");
        line.push_str(&param_value("attendee name", name)?);
    }
    line.push_str(";ROLE=");
    line.push_str(match attendee.role {
        AttendeeRole::Required => "REQ-PARTICIPANT",
        AttendeeRole::Optional => "OPT-PARTICIPANT",
        AttendeeRole::Resource => "NON-PARTICIPANT",
        AttendeeRole::Chair => "CHAIR",
        AttendeeRole::Unknown | _ => "REQ-PARTICIPANT",
    });
    line.push_str(";PARTSTAT=");
    line.push_str(match attendee.status {
        RsvpStatus::Accepted => "ACCEPTED",
        RsvpStatus::Declined => "DECLINED",
        RsvpStatus::Tentative => "TENTATIVE",
        RsvpStatus::Delegated => "DELEGATED",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown | _ => "NEEDS-ACTION",
    });
    line.push_str(":mailto:");
    line.push_str(plain_value("attendee email", &attendee.email)?);
    Ok(line)
}

fn attendee_role(value: Option<&str>) -> AttendeeRole {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "REQ-PARTICIPANT" => AttendeeRole::Required,
        "OPT-PARTICIPANT" => AttendeeRole::Optional,
        "NON-PARTICIPANT" => AttendeeRole::Resource,
        "CHAIR" => AttendeeRole::Chair,
        _ => AttendeeRole::Unknown,
    }
}

fn rsvp_status(value: Option<&str>) -> RsvpStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "ACCEPTED" => RsvpStatus::Accepted,
        "DECLINED" => RsvpStatus::Declined,
        "TENTATIVE" => RsvpStatus::Tentative,
        "DELEGATED" => RsvpStatus::Delegated,
        "NEEDS-ACTION" => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
}

fn mailto(value: &str) -> Option<String> {
    value
        .get(.."mailto:".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("mailto:"))
        .and_then(|_| value.get("mailto:".len()..))
        .map(ToString::to_string)
}

fn push_optional(
    lines: &mut Vec<String>,
    name: &str,
    field: &'static str,
    value: Option<&str>,
) -> Result<(), WriteError> {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        lines.push(format!("{name}:{}", text_value(field, value)?));
    }
    Ok(())
}

fn push_vtimezones(lines: &mut Vec<String>, event: &EventCreate) -> Result<(), WriteError> {
    if event.is_all_day {
        return Ok(());
    }
    let mut tzids = Vec::new();
    // An empty end writes no DTEND, so its zone has nothing to describe.
    let end_zone = if event.end.value.is_empty() {
        &None
    } else {
        &event.end.timezone
    };
    for tzid in [&event.start.timezone, end_zone]
        .into_iter()
        .flatten()
        .filter(|tzid| !tzid.is_empty())
    {
        if !tzids.iter().any(|known: &String| known == tzid) {
            tzids.push(tzid.clone());
        }
    }
    for tzid in tzids {
        // Anchor the offset at whichever event time carries this zone (start
        // preferred), so the emitted VTIMEZONE matches the instant the server
        // will resolve the DTSTART/DTEND against.
        let anchor = [&event.start, &event.end]
            .into_iter()
            .find(|time| time.timezone.as_deref() == Some(tzid.as_str()))
            .and_then(event_naive_local);

        // Resolve the real UTC offset for the event's instant. `canonical_tzid`
        // folds Windows/Exchange names ("W. Europe Standard Time") to IANA so
        // the name resolves against the tzdb. An unknown zone, or a value we
        // cannot parse to a naive wall-clock, falls through to `None`.
        let offset = tzid_offset_for_naive(&tzid, anchor);

        lines.push("BEGIN:VTIMEZONE".to_string());
        lines.push(format!("TZID:{}", text_value("vtimezone tzid", &tzid)?));
        // A single STANDARD block carrying the correct offset for the event's
        // instant. This is approximate for a recurring event that spans a DST
        // transition (off by the DST delta on the far side of the transition),
        // but strictly better than the previous fixed `+0000` UTC stub, which
        // made servers that trust the supplied VTIMEZONE read an Oslo
        // wall-clock time as UTC and shift the event by the real offset
        // (1-2h). Full STANDARD/DAYLIGHT transition rules are not generated;
        // most servers re-resolve the TZID by name regardless. That limit was
        // accepted when the offset had to be resolved by hand-walking chrono's
        // `LocalResult`; jiff exposes zone transitions directly, so emitting
        // real transition rules is now substantially cheaper than it was. It
        // is still not obviously worth it - a server that ignores the supplied
        // component gains nothing - so this is a standing re-evaluation, not
        // scheduled work, and the accepted limit is documented in
        // `reference/caldav.md` under "Account behavior" / `event_create`. An
        // unknown /
        // unparseable zone omits the offset sub-block rather than emit a
        // misleading `+0000`: the bare VTIMEZONE still names the TZID for
        // servers that resolve it themselves, and we avoid asserting a wrong
        // offset.
        if let Some(offset) = offset {
            lines.push("BEGIN:STANDARD".to_string());
            lines.push("DTSTART:19700101T000000".to_string());
            lines.push(format!("TZOFFSETFROM:{offset}"));
            lines.push(format!("TZOFFSETTO:{offset}"));
            lines.push("END:STANDARD".to_string());
        }
        lines.push("END:VTIMEZONE".to_string());
    }
    Ok(())
}

/// Derive the wall-clock `NaiveDateTime` an event time names. With a TZID the
/// stored value is a local wall-clock instant (mirrors `push_time`, which
/// strips the trailing `Z` and emits the date/time verbatim under the zone).
fn event_naive_local(time: &EventTime) -> Option<civil::DateTime> {
    if let Ok(parsed) = time.value.parse::<civil::DateTime>() {
        return Some(parsed);
    }
    // A `Z`-suffixed RFC 3339 value names an instant (the civil parser
    // rejects it because Temporal reads `Z` as an unknown offset). The
    // VTIMEZONE anchor must be that instant's wall clock IN THE ZONE;
    // reading the UTC wall clock as zone-local can resolve the offset on
    // the wrong side of a DST transition.
    if let Ok(instant) = time.value.parse::<Timestamp>() {
        return Some(instant_zone_wall(time.timezone.as_deref(), instant));
    }
    // Fall back to parsing a bare iCalendar-style `YYYYMMDDTHHMMSS` value.
    let raw = time.value.replace(['-', ':'], "");
    let raw = raw.trim_end_matches('Z');
    civil::DateTime::strptime("%Y%m%dT%H%M%S", raw).ok()
}

/// Project an instant into `tzid`'s wall clock. Falls back to the UTC wall
/// clock when the zone is absent or unknown to the tzdb - the least-wrong
/// rendering available once the value has already committed to an instant.
fn instant_zone_wall(tzid: Option<&str>, instant: Timestamp) -> civil::DateTime {
    tzid.and_then(|name| TimeZone::get(&canonical_tzid(name)).ok())
        .map_or_else(
            || Offset::UTC.to_datetime(instant),
            |tz| tz.to_datetime(instant),
        )
}

/// Resolve the iCalendar UTC-offset string (`+HHMM` / `-HHMM`) for `tzid` at
/// the given wall-clock instant, or `None` if the zone is unknown.
///
/// Ambiguity discipline mirrors ratatoskr's `resolve_local_to_timestamp`:
/// an unambiguous wall clock is used directly; a fall-back fold picks the
/// earlier instant (matches Outlook/Google/Apple); a spring-forward gap uses
/// the post-gap offset.
fn tzid_offset_for_naive(tzid: &str, naive: Option<civil::DateTime>) -> Option<String> {
    let tz = TimeZone::get(&canonical_tzid(tzid)).ok()?;
    // With no usable anchor, fall back to the Unix epoch so we still emit the
    // zone's standard-time offset rather than nothing.
    let naive = naive.unwrap_or_else(|| Offset::UTC.to_datetime(Timestamp::UNIX_EPOCH));

    let offset = match tz.to_ambiguous_timestamp(naive).offset() {
        AmbiguousOffset::Unambiguous { offset } => offset,
        AmbiguousOffset::Fold { before, .. } => before,
        AmbiguousOffset::Gap { after, .. } => after,
    };
    Some(format_utc_offset(offset))
}

/// Format an offset as the iCalendar UTC-offset form `+HHMM` / `-HHMM`.
fn format_utc_offset(offset: Offset) -> String {
    let total = offset.seconds();
    let sign = if total < 0 { '-' } else { '+' };
    let abs = total.abs();
    format!("{sign}{:02}{:02}", abs / 3600, (abs % 3600) / 60)
}

/// Write `DTSTART`. An empty start is refused: `DTSTART:` is invalid
/// iCalendar, and unlike an end there is no honest reading of "no start" - the
/// event has no position. Providers project an empty `EventTime` for "did not
/// say", and a consumer's read-modify-write echoes it back here.
fn push_start(
    lines: &mut Vec<String>,
    time: &EventTime,
    is_all_day: bool,
) -> Result<(), WriteError> {
    push_time(
        lines,
        "DTSTART",
        time,
        is_all_day,
        ("start", "start timezone"),
    )
}

/// Write `DTEND`, or nothing when the end is empty.
///
/// Unlike DTSTART, an empty end has an honest meaning in iCalendar: RFC 5545
/// allows a VEVENT with neither DTEND nor DURATION (zero-length for a
/// DATE-TIME start, one day for a DATE start), and this crate's own projection
/// yields an empty end for exactly such a body. So an empty end is "no end
/// stated" and is omitted, which round-trips: the stored event reads back with
/// an empty end again. It is neither refused (a valid read-then-create would
/// fail) nor written as `DTEND:` (invalid).
fn push_end(lines: &mut Vec<String>, time: &EventTime, is_all_day: bool) -> Result<(), WriteError> {
    if time.value.is_empty() {
        return Ok(());
    }
    push_time(lines, "DTEND", time, is_all_day, ("end", "end timezone"))
}

fn push_time(
    lines: &mut Vec<String>,
    name: &str,
    time: &EventTime,
    is_all_day: bool,
    (field, timezone_field): (&'static str, &'static str),
) -> Result<(), WriteError> {
    plain_value(field, &time.value)?;
    // An empty TZID (the projection yields `Some("")` for `TZID=`) is no zone:
    // `;TZID=:` is not a valid parameter, and the value must render as it does
    // for a zoneless time (a `Z` instant stays UTC).
    let zoneless;
    let time = if time.timezone.as_deref() == Some("") {
        zoneless = EventTime {
            value: time.value.clone(),
            timezone: None,
        };
        &zoneless
    } else {
        time
    };
    let value = ical_time_from_event_time(time, is_all_day);
    let timezone = time.timezone.as_deref();
    if is_all_day {
        lines.push(format!("{name};VALUE=DATE:{value}"));
    } else if let Some(tzid) = timezone {
        lines.push(format!(
            "{name};TZID={}:{}",
            param_value(timezone_field, tzid)?,
            value
        ));
    } else {
        lines.push(format!("{name}:{value}"));
    }
    Ok(())
}

fn push_recurrence(
    lines: &mut Vec<String>,
    recurrence: &EventRecurrence,
) -> Result<(), WriteError> {
    if let Some(rrule) = &recurrence.rrule {
        lines.push(format!("RRULE:{}", plain_value("recurrence rrule", rrule)?));
    }
    for rdate in &recurrence.rdate {
        lines.push(format!("RDATE:{}", plain_value("recurrence rdate", rdate)?));
    }
    for exdate in &recurrence.exdate {
        lines.push(format!(
            "EXDATE:{}",
            plain_value("recurrence exdate", exdate)?
        ));
    }
    if let Some(recurrence_id) = &recurrence.recurrence_id {
        lines.push(format!(
            "RECURRENCE-ID:{}",
            plain_value("recurrence id", recurrence_id)?
        ));
    }
    Ok(())
}

fn event_availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "TRANSPARENT" => EventAvailability::Free,
        "OPAQUE" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn event_visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "PUBLIC" => EventVisibility::Public,
        "PRIVATE" => EventVisibility::Private,
        "CONFIDENTIAL" => EventVisibility::Confidential,
        _ => EventVisibility::Default,
    }
}

fn transparency(availability: EventAvailability) -> &'static str {
    match availability {
        EventAvailability::Free => "TRANSPARENT",
        EventAvailability::Busy
        | EventAvailability::Tentative
        | EventAvailability::OutOfOffice
        | EventAvailability::Unknown => "OPAQUE",
        _ => "OPAQUE",
    }
}

fn classification(visibility: EventVisibility) -> Option<&'static str> {
    match visibility {
        EventVisibility::Public => Some("PUBLIC"),
        EventVisibility::Private => Some("PRIVATE"),
        EventVisibility::Confidential => Some("CONFIDENTIAL"),
        EventVisibility::Default => None,
        _ => None,
    }
}

/// Splice replacements into a body, operating on *physical* lines.
///
/// `insert_at` and `removed` are caldata `LineReader` line numbers taken from
/// the projection's own parse (`BlockSpan`): the line the replacements go
/// before, and the lines whose logical line they replace. The logical lines
/// are grouped here by the same reader, so a logical line is its head plus
/// every physical line up to the next head - fold continuations, and the
/// blank lines caldata skips inside a fold, alike.
///
/// Untouched logical lines, continuations included, are emitted byte for byte
/// (line endings normalized to CRLF); only the freshly emitted replacement
/// lines are folded, so a long preserved value round-trips losslessly. The
/// previous implementation of this unfolded the whole document and re-folded
/// it, re-wrapping every preserved line and, through an old WSP-eating
/// unfolder, losing characters.
///
/// Refuses (`None`) a body whose physical lines cannot be paired with the
/// reader's line numbers, and a body with no line at `insert_at`. Neither is
/// reachable from a well-formed parse; a silent mis-splice is the outcome that
/// is not allowed, so a disagreement is an error rather than a guess.
fn splice_lines(
    raw_ical: &str,
    insert_at: usize,
    removed: &[usize],
    replacements: Vec<String>,
) -> Option<String> {
    let physical = physical_lines(raw_ical);
    let mut heads = Vec::new();
    let mut unfolded = Vec::new();
    for line in LineReader::from_slice(raw_ical.as_bytes()) {
        let line = line.ok()?;
        heads.push(line.number());
        unfolded.push(line.as_str().to_owned());
    }
    // The pairing is verified, not assumed: every logical line, rebuilt from
    // the physical lines this function will emit, must equal what the reader
    // produced for it. A reader that broke lines differently (a bare CR, say)
    // makes the rebuilt text differ or the numbers run past the end, and the
    // body is refused.
    let start_of = |head: usize| head.checked_sub(1);
    let first_head = match heads.first() {
        Some(&head) => start_of(head)?,
        None => physical.len(),
    };
    if physical
        .get(..first_head)?
        .iter()
        .any(|line| !line.is_empty())
    {
        return None;
    }
    let mut groups = Vec::with_capacity(heads.len());
    for (index, &head) in heads.iter().enumerate() {
        let from = start_of(head)?;
        let to = match heads.get(index + 1) {
            Some(&next) => start_of(next)?,
            None => physical.len(),
        };
        let group = physical.get(from..to).filter(|group| !group.is_empty())?;
        let mut rebuilt = String::from(group[0]);
        for line in &group[1..] {
            if !line.is_empty() {
                rebuilt.push_str(line.get(1..)?);
            }
        }
        if rebuilt != unfolded[index] {
            return None;
        }
        groups.push((head, group));
    }
    if !groups.iter().any(|&(head, _)| head == insert_at) {
        return None;
    }

    let mut out = String::new();
    let mut replacements = Some(replacements);
    for line in &physical[..first_head] {
        out.push_str(line);
        out.push_str("\r\n");
    }
    for (head, group) in groups {
        if head == insert_at
            && let Some(replacements) = replacements.take()
        {
            out.push_str(&fold_ical_lines(replacements));
        }
        if removed.contains(&head) {
            continue;
        }
        for line in group {
            out.push_str(line);
            out.push_str("\r\n");
        }
    }
    Some(out)
}

/// The physical lines of a body, split exactly as caldata's `LineReader` splits
/// them: on `\n` only, dropping ONE `\r` directly before it, and a final
/// segment without a `\n` kept as is. `str::lines` differs at the edges (it
/// strips a further trailing `\r`), and a bare `\r` is a break to neither.
fn physical_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        match rest.split_once('\n') {
            Some((line, tail)) => {
                lines.push(line.strip_suffix('\r').unwrap_or(line));
                rest = tail;
            }
            None => {
                lines.push(rest);
                break;
            }
        }
    }
    lines
}

fn fold_ical_lines(lines: Vec<String>) -> String {
    let mut folded = String::new();
    for line in lines {
        let mut current = String::new();
        let mut prefix = 0;
        for ch in line.chars() {
            if prefix + current.len() + ch.len_utf8() > 75 {
                folded.push_str(&current);
                folded.push_str("\r\n ");
                current.clear();
                // The folding space occupies one octet of the 75-octet
                // budget on every continuation line.
                prefix = 1;
            }
            current.push(ch);
        }
        folded.push_str(&current);
        folded.push_str("\r\n");
    }
    folded
}

fn escape_text(value: &str) -> String {
    // A CRLF pair and a lone CR are each one line break, as `escape_param`
    // reads them. Dropping a lone CR merged the two halves it separated.
    value
        .replace('\\', "\\\\")
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\n', "\\n")
        .replace(',', "\\,")
        .replace(';', "\\;")
}

/// Single left-to-right scan rather than an ordering-dependent chain of
/// `replace()` calls. A chain mis-handles adjacency such as `\\n` (backslash
/// followed by a literal `n`), which a multi-pass replace would turn into a
/// backslash plus newline. RFC 5545 sec 3.3.11 escapes only: `\\`, `\;`,
/// `\,`, and `\N`/`\n` (newline); any other backslash pair is kept verbatim.
fn unescape_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n' | 'N') => out.push('\n'),
            Some(',') => out.push(','),
            Some(';') => out.push(';'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// RFC 6868 parameter-value decoding, the inverse of [`escape_param`]:
/// `^n` is a newline, `^'` a `"`, `^^` a caret. A caret before anything
/// else is literal, so a value from a producer that predates RFC 6868
/// survives unchanged.
fn unescape_param(value: &str) -> String {
    if !value.contains('^') {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '^' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\'') => out.push('"'),
            Some('^') => out.push('^'),
            Some(other) => {
                out.push('^');
                out.push(other);
            }
            None => out.push('^'),
        }
    }
    out
}

fn escape_param(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '^' => escaped.push_str("^^"),
            '"' => escaped.push_str("^'"),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                escaped.push_str("^n");
            }
            '\n' => escaped.push_str("^n"),
            _ => escaped.push(ch),
        }
    }
    if escaped.contains([';', ',', ':']) {
        format!("\"{escaped}\"")
    } else {
        escaped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test shim: the production writer refuses unwritable values; the older
    /// tests feed writable ones and read the body. The refusal tests call
    /// `super::create_to_ical` directly.
    fn create_to_ical(event: &EventCreate, uid: &str) -> String {
        super::create_to_ical(event, uid).expect("a writable event")
    }

    /// Test shim: the production projector is fallible (malformed bodies
    /// degrade to a per-resource skip); these tests feed well-formed input
    /// and expect a successful projection.
    fn parse_event(
        uri: String,
        calendar_id: CalendarId,
        etag: Option<String>,
        data: &str,
    ) -> CalendarEvent {
        event_from_ical(uri, calendar_id, etag, data, None).expect("valid iCalendar projects")
    }

    #[test]
    fn parses_basic_vevent() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Meet\\, now\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;CN=Ada;PARTSTAT=ACCEPTED:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.uid.as_deref(), Some("u1"));
        assert_eq!(event.title.as_deref(), Some("Meet, now"));
        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(event.attendees[0].status, RsvpStatus::Accepted);
    }

    /// A nested component this projection does not model contributes NOTHING
    /// to the event. Its properties used to be appended to the master's prop
    /// list, where `pick_datetime`'s specificity ladder could prefer the
    /// sub-component's TZID-bearing DTSTART over the event's own UTC one -
    /// projecting the wrong time for the event.
    #[test]
    fn an_unknown_nested_component_does_not_leak_into_the_event() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Real\r\n\
             DTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\n\
             BEGIN:X-VENDOR-THING\r\nSUMMARY:Nested\r\n\
             DTSTART;TZID=Europe/Oslo:20200101T090000\r\n\
             BEGIN:VALARM\r\nTRIGGER:-PT5M\r\nEND:VALARM\r\n\
             END:X-VENDOR-THING\r\n\
             BEGIN:VALARM\r\nTRIGGER:-PT10M\r\nEND:VALARM\r\n\
             END:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.title.as_deref(), Some("Real"));
        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(event.start.timezone, None);
        assert_eq!(
            event.reminders.len(),
            1,
            "an alarm inside the vendor component is not the event's reminder"
        );
    }

    /// RFC 5545 s3.3.6: the time parts of a duration are EXACT seconds, so
    /// `PT10H` across Europe/Oslo's spring-forward lands on 21:00 wall clock,
    /// not the 20:00 civil addition produces. The day and week parts stay
    /// nominal, so `P1D` over the same transition keeps its wall clock.
    #[test]
    fn a_duration_end_under_a_tzid_respects_the_dst_transition() {
        let event = |duration: &str| {
            parse_event(
                "/cal/one.ics".to_string(),
                CalendarId("/cal/".to_string()),
                None,
                &format!(
                    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\n\
                     DTSTART;TZID=Europe/Oslo:20260329T010000\r\n\
                     DURATION:{duration}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
                ),
            )
        };

        // 01:00 + 10 exact hours over the 02:00 -> 03:00 jump is 12:00, where
        // DST-blind civil addition answers 11:00.
        let exact = event("PT10H").end.expect("resolved end");
        assert_eq!(exact.value, "2026-03-29T12:00:00");
        assert_eq!(exact.timezone.as_deref(), Some("Europe/Oslo"));

        // A nominal day keeps the wall clock across the same transition.
        assert_eq!(
            event("P1D").end.expect("resolved end").value,
            "2026-03-30T01:00:00"
        );
    }

    #[test]
    fn parses_numeric_offset_datetime() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000+0200\r\nDTEND:20260602T130000+0200\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00+02:00");
        assert_eq!(event.end.expect("end").value, "2026-06-02T13:00:00+02:00");
    }

    #[test]
    fn parses_tzid_datetime_as_wall_clock_with_timezone_metadata() {
        // Previously this test enshrined the false-`Z` bug, asserting the
        // wall-clock value was tagged UTC. A TZID-bearing local time is NOT
        // UTC: the value must stay bare and the zone lives in `timezone`.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=Europe/Oslo:20260602T120000\r\nDTEND;TZID=Europe/Oslo:20260602T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00");
        assert!(!event.start.value.ends_with('Z'));
        assert_eq!(event.start.timezone.as_deref(), Some("Europe/Oslo"));
        let end = event.end.expect("end");
        assert_eq!(end.value, "2026-06-02T13:00:00");
        assert_eq!(end.timezone.as_deref(), Some("Europe/Oslo"));
    }

    #[test]
    fn maps_windows_tzid_to_iana() {
        // caldata ships the Microsoft/CLDR zone table; a Windows zone name
        // must surface as its IANA equivalent, not the opaque vendor string.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=W. Europe Standard Time:20260602T120000\r\nDTEND;TZID=W. Europe Standard Time:20260602T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.timezone.as_deref(), Some("Europe/Berlin"));
        assert_eq!(event.start.value, "2026-06-02T12:00:00");
    }

    #[test]
    fn duplicate_dtstart_prefers_most_specific_candidate() {
        // Outlook bridges emit a floating-fallback DTSTART alongside the
        // TZID one. caldata's typed builder would reject the duplicate; the
        // tokenizer path keeps the event and picks the TZID candidate.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000\r\nDTSTART;TZID=Europe/Oslo:20260602T130000\r\nDTEND:20260602T140000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.timezone.as_deref(), Some("Europe/Oslo"));
        assert_eq!(event.start.value, "2026-06-02T13:00:00");
    }

    #[test]
    fn quoted_parameter_with_colon_and_semicolon_parses() {
        // A quoted CN containing `,` and a quoted TZID containing `:`/`;`
        // must not be mis-split on the first `:` (the old parser bug).
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=\"Custom:Zone;X\":20260602T120000\r\nATTENDEE;CN=\"Doe, John\";PARTSTAT=ACCEPTED:mailto:john@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00");
        assert_eq!(event.start.timezone.as_deref(), Some("Custom:Zone;X"));
        assert_eq!(event.attendees.len(), 1);
        assert_eq!(event.attendees[0].name.as_deref(), Some("Doe, John"));
        assert_eq!(event.attendees[0].email, "john@example.test");
        assert_eq!(event.attendees[0].status, RsvpStatus::Accepted);
    }

    #[test]
    fn malformed_resource_degrades_to_skip_not_hard_failure() {
        // An unterminated quoted parameter is a tokenizer error; the
        // projector returns Err so the caller can route the resource to
        // failed_hrefs instead of failing the whole sync.
        let result = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=\"unterminated:20260602T120000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            None,
        );

        assert!(result.is_err());
    }

    #[test]
    fn long_preserved_line_round_trips_losslessly_through_update() {
        // The H round-trip bug: a long folded preserved value must come back
        // byte-identical after a patch that does not touch it. We feed a
        // DESCRIPTION pre-folded at a non-75 column and assert the unfolded
        // value survives a SUMMARY-only patch.
        let long_value = "x:    y ".repeat(40);
        let mut body = String::from(
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nDESCRIPTION:",
        );
        // Fold the description manually at column ~40 so a refold would move
        // the boundaries (and the old WSP-eating unfolder would drop the
        // run-of-spaces after the colon).
        for (index, chunk) in long_value.as_bytes().chunks(40).enumerate() {
            if index > 0 {
                body.push_str("\r\n ");
            }
            body.push_str(std::str::from_utf8(chunk).unwrap());
        }
        body.push_str("\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n");

        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            &body,
        );
        let original_description = current.description.clone();
        assert_eq!(original_description.as_deref(), Some(long_value.as_str()));

        let patched = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("title patch should serialize");

        let reparsed = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &patched,
        );
        assert!(patched.contains("SUMMARY:New"));
        assert_eq!(reparsed.description, original_description);
    }

    #[test]
    fn all_day_dtend_round_trips_as_exclusive_ical_end() {
        // A single all-day event on 2026-06-02 is start 2026-06-02, end
        // 2026-06-03 under the exclusive-end contract. It serializes to an
        // exclusive iCalendar DTEND verbatim (no +1) and parses back
        // verbatim (no -1).
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-03".to_string(),
                    timezone: None,
                },
                is_all_day: true,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("DTSTART;VALUE=DATE:20260602"));
        assert!(body.contains("DTEND;VALUE=DATE:20260603"));

        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
        );
        assert_eq!(event.start.value, "2026-06-02");
        assert_eq!(event.end.expect("end").value, "2026-06-03");
    }

    #[test]
    fn dtend_date_marks_event_all_day() {
        // The exclusive iCalendar all-day DTEND passes through verbatim
        // into the exclusive `EventTime` end - no inclusive decrement.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T000000Z\r\nDTEND;VALUE=DATE:20260603\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert!(event.is_all_day);
        assert_eq!(event.end.expect("end").value, "2026-06-03");
    }

    #[test]
    fn parses_recurrence_properties_from_master_vevent() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\nRDATE:20260609T120000Z\r\nEXDATE:20260616T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(
            event.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;COUNT=3")
        );
        assert_eq!(event.recurrence.rdate, vec!["20260609T120000Z"]);
        assert_eq!(event.recurrence.exdate, vec!["20260616T120000Z"]);
        assert_eq!(event.recurrence.recurrence_id, None);
    }

    #[test]
    fn multi_vevent_projection_reads_only_first_vevent() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Master\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY;COUNT=2\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Override\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.title.as_deref(), Some("Master"));
        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(
            event.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;COUNT=2")
        );
        assert_eq!(event.recurrence.recurrence_id, None);
    }

    #[test]
    fn events_from_ical_projects_master_and_overrides() {
        // The whole resource projects: master (RRULE) plus a moved
        // override and a cancellation, each carrying its RECURRENCE-ID and
        // STATUS. Overrides take a recurrence-qualified id but keep the
        // resource native id.
        let events = events_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Master\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Moved\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260616T120000Z\r\nSTATUS:CANCELLED\r\nDTSTART:20260616T120000Z\r\nDTEND:20260616T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        )
        .expect("valid iCalendar projects");

        assert_eq!(events.len(), 3);

        let master = &events[0];
        assert_eq!(master.title.as_deref(), Some("Master"));
        assert_eq!(master.id.0, "/cal/one.ics");
        assert_eq!(master.recurrence.recurrence_id, None);
        assert_eq!(
            master.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;COUNT=3")
        );

        let moved = &events[1];
        assert_eq!(moved.title.as_deref(), Some("Moved"));
        assert_eq!(moved.id.0, "/cal/one.ics#20260609T120000Z");
        assert_eq!(moved.native_id, "/cal/one.ics");
        assert_eq!(
            moved.recurrence.recurrence_id.as_deref(),
            Some("20260609T120000Z")
        );

        let cancelled = &events[2];
        assert_eq!(cancelled.status, EventStatus::Cancelled);
        assert_eq!(
            cancelled.recurrence.recurrence_id.as_deref(),
            Some("20260616T120000Z")
        );
    }

    #[test]
    fn valarm_projects_relative_and_absolute_reminders() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\nBEGIN:VALARM\r\nACTION:EMAIL\r\nTRIGGER;RELATED=END:PT5M\r\nEND:VALARM\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER;VALUE=DATE-TIME:20260602T110000Z\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.reminders.len(), 3);
        assert_eq!(
            event.reminders[0].trigger,
            ReminderTrigger::Relative {
                offset: "-PT15M".to_string(),
                relative_to: ReminderRelativeTo::Start,
            }
        );
        assert_eq!(event.reminders[0].action.as_deref(), Some("DISPLAY"));
        assert_eq!(
            event.reminders[1].trigger,
            ReminderTrigger::Relative {
                offset: "PT5M".to_string(),
                relative_to: ReminderRelativeTo::End,
            }
        );
        assert_eq!(
            event.reminders[2].trigger,
            ReminderTrigger::Absolute("2026-06-02T11:00:00Z".to_string())
        );
    }

    #[test]
    fn serializes_create_payload() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: Some("Planning".to_string()),
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("UID:uid-1"));
        assert!(body.contains("SUMMARY:Planning"));
        assert!(body.contains("DTSTART:20260602T120000Z"));
    }

    #[test]
    fn serializes_create_organizer() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: Some(EventOrganizer {
                    email: "ada@example.test".to_string(),
                    name: Some("Ada Lovelace".to_string()),
                }),
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("ORGANIZER;CN=Ada Lovelace:mailto:ada@example.test"));
    }

    #[test]
    fn serializes_numeric_offset_datetime_as_utc() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00+02:00".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00+02:00".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("DTSTART:20260602T100000Z"));
        assert!(body.contains("DTEND:20260602T110000Z"));
    }

    #[test]
    fn serializes_timezone_datetime_as_local_ical_value() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("BEGIN:VTIMEZONE"));
        assert!(body.contains("TZID:Europe/Oslo"));
        assert!(body.contains("BEGIN:STANDARD"));
        // June 2 is summer time in Oslo: CEST = +0200. Previously this stub
        // asserted +0000, which encoded the very bug being fixed (a server
        // trusting the VTIMEZONE read the wall-clock as UTC and shifted the
        // event by 2h).
        assert!(body.contains("TZOFFSETFROM:+0200"));
        assert!(body.contains("TZOFFSETTO:+0200"));
        // The `Z` values name instants; under the TZID they must render as
        // the zone's wall clock (12:00Z = 14:00 CEST), not the UTC wall
        // clock the old code emitted (a silent 2h shift on the wire).
        assert!(body.contains("DTSTART;TZID=Europe/Oslo:20260602T140000"));
        assert!(body.contains("DTEND;TZID=Europe/Oslo:20260602T150000"));
    }

    #[test]
    fn vtimezone_offset_tracks_winter_standard_time() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-01-15T12:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                end: EventTime {
                    value: "2026-01-15T13:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        // January is winter time in Oslo: CET = +0100, so the 12:00Z
        // instant is 13:00 on the local wall clock.
        assert!(body.contains("TZOFFSETTO:+0100"));
        assert!(body.contains("DTSTART;TZID=Europe/Oslo:20260115T130000"));
    }

    #[test]
    fn vtimezone_unknown_zone_omits_offset_block() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: Some("Custom/Bogus".to_string()),
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: Some("Custom/Bogus".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        // The bare VTIMEZONE still names the zone for servers that resolve it
        // themselves, but we never assert a misleading offset for an unknown
        // zone.
        assert!(body.contains("BEGIN:VTIMEZONE"));
        assert!(body.contains("TZID:Custom/Bogus"));
        assert!(!body.contains("TZOFFSETTO"));
        assert!(!body.contains("BEGIN:STANDARD"));
    }

    #[test]
    fn tzid_offset_resolves_known_zone_and_instant() {
        let summer = civil::date(2026, 6, 2).at(12, 0, 0, 0);
        let winter = civil::date(2026, 1, 15).at(12, 0, 0, 0);
        assert_eq!(
            tzid_offset_for_naive("Europe/Oslo", Some(summer)).as_deref(),
            Some("+0200")
        );
        assert_eq!(
            tzid_offset_for_naive("Europe/Oslo", Some(winter)).as_deref(),
            Some("+0100")
        );
        // Windows/Exchange alias folds to IANA before parsing.
        assert_eq!(
            tzid_offset_for_naive("W. Europe Standard Time", Some(summer)).as_deref(),
            Some("+0200")
        );
        // Unknown zone yields no offset (graceful fallback, not +0000).
        assert_eq!(tzid_offset_for_naive("Custom/Bogus", Some(summer)), None);
    }

    #[test]
    fn create_payload_writes_transparency_and_classification() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: EventStatus::Tentative,
                availability: EventAvailability::Free,
                visibility: EventVisibility::Private,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("STATUS:TENTATIVE"));
        assert!(body.contains("TRANSP:TRANSPARENT"));
        assert!(body.contains("CLASS:PRIVATE"));
    }

    #[test]
    fn patch_preserves_unmodeled_vevent_properties() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nSTATUS:CANCELLED\r\nORGANIZER;CN=Owner:mailto:owner@example.test\r\nTRANSP:TRANSPARENT\r\nCLASS:PRIVATE\r\nRECURRENCE-ID:20260609T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("title patch should serialize");

        assert!(body.contains("SUMMARY:New"));
        assert!(!body.contains("SUMMARY:Old"));
        assert!(body.contains("STATUS:CANCELLED"));
        assert!(body.contains("ORGANIZER;CN=Owner:mailto:owner@example.test"));
        assert!(body.contains("TRANSP:TRANSPARENT"));
        assert!(body.contains("CLASS:PRIVATE"));
        assert!(body.contains("RECURRENCE-ID:20260609T120000Z"));
    }

    #[test]
    fn patch_preserves_non_event_calendar_components() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:Europe/Oslo\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART;TZID=Europe/Oslo:20260602T120000\r\nDTEND;TZID=Europe/Oslo:20260602T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("title patch should serialize");

        assert!(body.contains("BEGIN:VTIMEZONE"));
        assert!(body.contains("TZID:Europe/Oslo"));
        assert!(body.contains("END:VTIMEZONE"));
        assert!(body.contains("SUMMARY:New"));
    }

    #[test]
    fn parses_transparency_and_classification() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nTRANSP:TRANSPARENT\r\nCLASS:CONFIDENTIAL\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.availability, EventAvailability::Free);
        assert_eq!(event.visibility, EventVisibility::Confidential);
    }

    #[test]
    fn missing_transparency_and_classification_default_to_unknown_and_default() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.availability, EventAvailability::Unknown);
        assert_eq!(event.visibility, EventVisibility::Default);
    }

    #[test]
    fn absent_status_is_confirmed_and_unrecognized_status_is_unknown() {
        let status_of = |status_line: &str| {
            parse_event(
                "/cal/one.ics".to_string(),
                CalendarId("/cal/".to_string()),
                None,
                &format!(
                    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\n{status_line}END:VEVENT\r\nEND:VCALENDAR\r\n"
                ),
            )
            .status
        };

        assert_eq!(status_of(""), EventStatus::Confirmed);
        assert_eq!(status_of("STATUS:CANCELLED\r\n"), EventStatus::Cancelled);
        assert_eq!(status_of("STATUS:X-WEIRD\r\n"), EventStatus::Unknown);
    }

    #[test]
    fn patch_replaces_transparency_and_classification() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Keep\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nTRANSP:OPAQUE\r\nCLASS:PUBLIC\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                availability: Some(EventAvailability::Free),
                visibility: Some(EventVisibility::Private),
                ..EventPatch::default()
            },
        )
        .expect("availability/visibility patch should serialize");

        assert!(body.contains("TRANSP:TRANSPARENT"));
        assert!(!body.contains("TRANSP:OPAQUE"));
        assert!(body.contains("CLASS:PRIVATE"));
        assert!(!body.contains("CLASS:PUBLIC"));
        assert!(body.contains("SUMMARY:Keep"));

        // Round-trips back to the modeled values written by the patch.
        let updated = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
        );
        assert_eq!(updated.availability, EventAvailability::Free);
        assert_eq!(updated.visibility, EventVisibility::Private);
    }

    #[test]
    fn patch_clearing_visibility_to_default_strips_class() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nCLASS:PRIVATE\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                visibility: Some(EventVisibility::Default),
                ..EventPatch::default()
            },
        )
        .expect("visibility clear should serialize");

        assert!(!body.contains("CLASS:"));
    }

    #[test]
    fn patch_clear_removes_only_targeted_scalar_property() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDESCRIPTION:Details\r\nLOCATION:Room\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nSTATUS:CONFIRMED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                description: Some(None),
                ..EventPatch::default()
            },
        )
        .expect("description patch should serialize");

        assert!(!body.contains("DESCRIPTION:"));
        assert!(body.contains("SUMMARY:Old"));
        assert!(body.contains("LOCATION:Room"));
        assert!(body.contains("STATUS:CONFIRMED"));
    }

    #[test]
    fn rsvp_patch_updates_matching_attendee_only() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;PARTSTAT=NEEDS-ACTION:mailto:one@example.test\r\nATTENDEE;PARTSTAT=NEEDS-ACTION:mailto:two@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let patch = rsvp_patch(&current, RsvpStatus::Accepted, "two@example.test")
            .expect("matching attendee");
        let attendees = patch.attendees.expect("attendee patch");

        assert_eq!(attendees[0].status, RsvpStatus::NeedsAction);
        assert_eq!(attendees[1].status, RsvpStatus::Accepted);
    }

    #[test]
    fn rsvp_patch_rejects_missing_authenticated_attendee() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;PARTSTAT=NEEDS-ACTION:mailto:one@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let error = rsvp_patch(&current, RsvpStatus::Accepted, "two@example.test")
            .expect_err("missing attendee should fail");

        assert_eq!(error, "authenticated attendee not found");
    }

    #[test]
    fn rsvp_reply_ical_builds_itip_reply() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nORGANIZER;CN=Owner:mailto:owner@example.test\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;CN=Ada;PARTSTAT=NEEDS-ACTION:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = rsvp_reply_ical(&current, RsvpStatus::Accepted, "ada@example.test")
            .expect("itip reply");
        let unfolded = LineReader::from_slice(body.as_bytes())
            .map(|line| line.expect("the reply is UTF-8").as_str().to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(unfolded.contains("METHOD:REPLY"));
        assert!(unfolded.contains("UID:u1"));
        assert!(!unfolded.contains("SEQUENCE:"));
        assert!(unfolded.contains("ORGANIZER;CN=Owner:mailto:owner@example.test"));
        assert!(unfolded.contains(
            "ATTENDEE;CN=Ada;ROLE=REQ-PARTICIPANT;PARTSTAT=ACCEPTED:mailto:ada@example.test"
        ));
    }

    #[test]
    fn rsvp_reply_ical_carries_a_nonzero_sequence() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSEQUENCE:12\r\nORGANIZER:mailto:owner@example.test\r\nDTSTART:20260602T120000Z\r\nATTENDEE:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = rsvp_reply_ical(&current, RsvpStatus::Accepted, "ada@example.test")
            .expect("itip reply");
        let unfolded = LineReader::from_slice(body.as_bytes())
            .map(|line| line.expect("the reply is UTF-8").as_str().to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(unfolded.contains("SEQUENCE:12"));
    }

    #[test]
    fn rsvp_reply_ical_omits_a_zero_sequence() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSEQUENCE:0\r\nORGANIZER:mailto:owner@example.test\r\nDTSTART:20260602T120000Z\r\nATTENDEE:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = rsvp_reply_ical(&current, RsvpStatus::Accepted, "ada@example.test")
            .expect("itip reply");

        assert!(!body.contains("SEQUENCE:"));
    }

    #[test]
    fn recurrence_patch_replaces_stale_recurrence_lines() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nRDATE:20260603T120000Z\r\nEXDATE:20260604T120000Z\r\nSTATUS:CONFIRMED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: Vec::new(),
                    exdate: Vec::new(),
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect("recurrence patch should serialize");

        assert!(body.contains("RRULE:FREQ=DAILY;COUNT=2"));
        assert!(!body.contains("RRULE:FREQ=WEEKLY"));
        assert!(!body.contains("RDATE:20260603T120000Z"));
        assert!(!body.contains("EXDATE:20260604T120000Z"));
        assert!(body.contains("STATUS:CONFIRMED"));
    }

    #[test]
    fn recurrence_patch_rejects_override_vevents() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Override\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let error = patch_to_ical(
            &current,
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: Vec::new(),
                    exdate: Vec::new(),
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect_err("recurrence override patch should be refused");

        assert_eq!(error, PatchError::RecurrenceOverride);
    }

    /// Property names are case-insensitive, and caldata uppercases them, so the
    /// projection sees a lowercase override. Against the exact-match guard this
    /// patch was ACCEPTED and spliced into the master with the override left
    /// in place.
    #[test]
    fn recurrence_patch_rejects_a_lowercase_override_vevent() {
        let raw = "BEGIN:VCALENDAR\r\nbegin:vevent\r\nuid:u1\r\ndtstart:20260602T120000Z\r\ndtend:20260602T130000Z\r\nrrule:FREQ=WEEKLY\r\nend:vevent\r\nbegin:vevent\r\nuid:u1\r\nrecurrence-id:20260609T120000Z\r\nsummary:Override\r\ndtstart:20260609T140000Z\r\ndtend:20260609T150000Z\r\nend:vevent\r\nEND:VCALENDAR\r\n";
        let projected = events_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            raw,
        )
        .expect("a lowercase body tokenizes");
        assert_eq!(
            projected.len(),
            2,
            "the projection reads the lowercase override as one"
        );
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            raw,
        );

        let error = patch_to_ical(
            &current,
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: Vec::new(),
                    exdate: Vec::new(),
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect_err("a lowercase override is still an override");

        assert_eq!(error, PatchError::RecurrenceOverride);
    }

    const OVERRIDE_FIRST: &str = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Override\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Master\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

    fn title_patch(title: &str) -> EventPatch {
        EventPatch {
            title: Some(Some(title.to_string())),
            ..EventPatch::default()
        }
    }

    /// A body whose first VEVENT is an override still projects, and patches,
    /// the master. Reverting `select_block_index` to `blocks.first()` in
    /// `event_from_ical` fails the projection assertion; reverting it in
    /// `patch_to_ical` alone splices "Updated" into the override.
    #[test]
    fn an_override_first_body_patches_the_master_it_projected() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            OVERRIDE_FIRST,
        );
        assert_eq!(current.title.as_deref(), Some("Master"));
        assert_eq!(current.recurrence.recurrence_id, None);

        let body = patch_to_ical(&current, &title_patch("Updated")).expect("splices the master");

        let override_end = OVERRIDE_FIRST.find("END:VEVENT").expect("first VEVENT");
        assert!(
            body.starts_with(&OVERRIDE_FIRST[..override_end]),
            "the override VEVENT is byte-identical: {body:?}"
        );
        assert!(body.contains("SUMMARY:Updated\r\n"));
        assert!(!body.contains("SUMMARY:Master"));
        assert!(body.contains("SUMMARY:Override\r\n"));
        // The patched body still projects the same master.
        let reread = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
        );
        assert_eq!(reread.title.as_deref(), Some("Updated"));
    }

    const LONE_OVERRIDE: &str = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Alone\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

    const TWO_OVERRIDES: &str = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:First\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260616T120000Z\r\nSUMMARY:Second\r\nDTSTART:20260616T140000Z\r\nDTEND:20260616T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

    fn project_instance(
        body: &str,
        instance: Option<&str>,
    ) -> Result<CalendarEvent, EventProjectionError> {
        event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            body,
            instance,
        )
    }

    /// A resource holding a single override and no master: the bare id is that
    /// override, a scalar patch lands on it, and a recurrence replacement is
    /// still refused. Reverting the `blocks.len() == 1` arm of
    /// `select_block_index` makes the projection fail with `AmbiguousResource`.
    #[test]
    fn a_lone_override_is_the_event_of_its_bare_resource_id() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            LONE_OVERRIDE,
        );
        assert_eq!(current.title.as_deref(), Some("Alone"));
        // Read by the bare id, it is still listed under its qualified id.
        assert_eq!(current.id.0, "/cal/one.ics#20260609T120000Z");

        let patched = patch_to_ical(&current, &title_patch("Edited")).expect("splices");
        assert!(patched.contains("SUMMARY:Edited\r\n"));
        assert!(!patched.contains("SUMMARY:Alone"));

        let recurrence = EventPatch {
            recurrence: Some(EventRecurrence::default()),
            ..EventPatch::default()
        };
        assert_eq!(
            patch_to_ical(&current, &recurrence),
            Err(PatchError::RecurrenceOverride)
        );
    }

    /// Every id the listing mints for a resource of overrides reads back as the
    /// VEVENT it named, and patches only that VEVENT. Reverting the `instance`
    /// arm of `select_block_index` to the first-override fallback makes the
    /// second instance project "First"; reverting the patch writer's
    /// `current.recurrence.recurrence_id` argument to `None` splices "Edited"
    /// into the first VEVENT instead.
    #[test]
    fn a_listed_override_id_reads_and_patches_the_vevent_it_named() {
        let listed = events_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            TWO_OVERRIDES,
        )
        .expect("lists");
        assert_eq!(listed.len(), 2);
        for (listed_event, title) in listed.iter().zip(["First", "Second"]) {
            let (resource, instance) = split_event_id(&listed_event.id.0);
            assert_eq!(resource, "/cal/one.ics");
            let read = project_instance(TWO_OVERRIDES, instance).expect("resolves");
            assert_eq!(read.id, listed_event.id);
            assert_eq!(read.title.as_deref(), Some(title));
            assert_eq!(read.start, listed_event.start);

            let patched = patch_to_ical(&read, &title_patch("Edited")).expect("splices");
            let expected_other = if title == "First" { "Second" } else { "First" };
            assert!(patched.contains("SUMMARY:Edited\r\n"));
            assert!(!patched.contains(&format!("SUMMARY:{title}")));
            assert!(patched.contains(&format!("SUMMARY:{expected_other}\r\n")));
        }
    }

    /// A bare resource id over several overrides and no master names none of
    /// them, and an instance nobody carries is absent, not the first. Reverting
    /// the `AmbiguousResource` arm restores the silent first-override answer.
    #[test]
    fn a_bare_id_over_several_overrides_and_an_unknown_instance_are_refused() {
        assert_eq!(
            project_instance(TWO_OVERRIDES, None).expect_err("ambiguous"),
            EventProjectionError::AmbiguousResource
        );
        assert_eq!(
            project_instance(TWO_OVERRIDES, Some("20990101T000000Z")).expect_err("absent"),
            EventProjectionError::NoInstance
        );
    }

    /// With a master present the bare id stays the master and a qualified id
    /// reaches the override, wherever each sits. Reverting the instance arm
    /// makes the qualified read return the master.
    #[test]
    fn a_qualified_id_reaches_the_override_beside_a_master() {
        let master = project_instance(OVERRIDE_FIRST, None).expect("master");
        assert_eq!(master.title.as_deref(), Some("Master"));
        let instance =
            project_instance(OVERRIDE_FIRST, Some("20260609T120000Z")).expect("override");
        assert_eq!(instance.title.as_deref(), Some("Override"));
        assert_eq!(instance.id.0, "/cal/one.ics#20260609T120000Z");
    }

    /// Physical lines break on `\n` alone, with one `\r` before it dropped -
    /// caldata's rule. Reverting `physical_lines` to `str::lines` plus a
    /// `\r` strip fails the first case (it drops a second `\r`).
    #[test]
    fn physical_lines_split_exactly_as_the_line_reader_does() {
        assert_eq!(physical_lines("a\r\r\nb\rc\nd"), vec!["a\r", "b\rc", "d"]);
        assert_eq!(physical_lines("a\n\n"), vec!["a", ""]);
        assert_eq!(physical_lines("a\r"), vec!["a\r"]);
        assert!(physical_lines("").is_empty());
    }

    /// A bare CR is not a line break to caldata, so a value holding one is one
    /// logical line and a preserved line ending in `\r\r\n` keeps its `\r`.
    /// Reverting `physical_lines` to `str::lines` plus a `\r` strip fails the
    /// last assertion.
    #[test]
    fn a_bare_cr_inside_a_preserved_line_survives_the_splice() {
        let body = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nX-NOTE:one\rtwo\r\r\nX-TAIL:z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            body,
        );
        let patched = patch_to_ical(&current, &title_patch("New")).expect("splices");
        assert!(patched.contains("SUMMARY:New\r\n"));
        assert!(
            patched.contains("X-NOTE:one\rtwo\r\r\nX-TAIL:z\r\n"),
            "{patched:?}"
        );
    }

    /// A bare CR inside a text value is kept by the reader, projects into the
    /// model as a carriage return, and is written back as a LINE BREAK when the
    /// field is echoed through a patch: it must not vanish (a title "one\rtwo"
    /// became "onetwo") and must not reach the wire raw, where a reader that
    /// does split on a bare CR would see a new content line.
    #[test]
    fn a_bare_cr_in_a_text_value_round_trips_as_a_break_not_a_deletion() {
        let body = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:one\rtwo\r\nDESCRIPTION:a\r\n b\rc\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            body,
        );
        assert_eq!(current.title.as_deref(), Some("one\rtwo"));
        assert_eq!(current.description.as_deref(), Some("ab\rc"));

        let echo = title_patch("one\rtwo");
        let patched = patch_to_ical(&current, &echo).expect("splices");
        assert!(
            patched.contains("SUMMARY:one\\ntwo\r\n"),
            "the CR is a break, not a deletion: {patched:?}"
        );
        assert!(
            patched.contains("DESCRIPTION:a\r\n b\rc\r\n"),
            "the untouched line keeps its CR verbatim: {patched:?}"
        );
    }

    /// A body broken only by bare CRs is one giant line to caldata: nothing
    /// projects, and a patch against such a stored body is refused rather than
    /// spliced at a guessed line. The patch case is refused by
    /// `select_block_index` finding no VEVENT (reverting nothing there passes it
    /// by design: both readers agree there is no event); the direct
    /// `splice_lines` case bites on the landing-line check, which is the only
    /// thing refusing a line number the body does not hold.
    #[test]
    fn a_bare_cr_body_is_refused_not_mis_spliced() {
        let cr_only = OVERRIDE_FIRST.replace("\r\n", "\r");
        assert_eq!(
            event_from_ical(
                "/cal/one.ics".to_string(),
                CalendarId("/cal/".to_string()),
                None,
                &cr_only,
                None
            )
            .expect_err("no VEVENT is reachable"),
            EventProjectionError::NoVevent
        );
        let mut current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            OVERRIDE_FIRST,
        );
        current.raw_ical = Some(cr_only);
        assert_eq!(
            patch_to_ical(&current, &title_patch("New")),
            Err(PatchError::NoSpliceableVevent)
        );

        assert_eq!(
            splice_lines("BEGIN:VEVENT\r\nEND:VEVENT\r\n", 99, &[], Vec::new()),
            None
        );
    }

    #[test]
    fn scalar_patch_preserves_override_vevents() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Master\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Override\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("Updated".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("scalar patch should preserve override VEVENTs");

        assert!(body.contains("SUMMARY:Updated"));
        assert!(body.contains("RECURRENCE-ID:20260609T120000Z"));
        assert!(body.contains("SUMMARY:Override"));
    }

    #[test]
    fn folds_long_output_lines() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: Some("A".repeat(90)),
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("\r\n "));
        // Every physical line, including space-prefixed continuation
        // lines, stays within the RFC 5545 75-octet budget.
        assert!(body.lines().all(|line| line.len() <= 75));
        assert!(body.lines().filter(|line| line.starts_with(' ')).count() >= 1);
    }

    #[test]
    fn fold_counts_continuation_space_in_octet_budget() {
        // 200 'A's forces multiple continuation lines; each continuation
        // line's leading space must count toward the 75-octet limit.
        let folded = fold_ical_lines(vec![format!("SUMMARY:{}", "A".repeat(200))]);
        assert!(folded.lines().all(|line| line.len() <= 75));
        assert!(folded.lines().filter(|line| line.starts_with(' ')).count() >= 2);
    }

    #[test]
    fn non_ascii_datetime_value_does_not_panic_in_projection() {
        let body = format!(
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:{}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            "\u{20ac}".repeat(5)
        );
        let event = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
            None,
        )
        .expect("non-ASCII property value remains a per-resource value");

        assert_eq!(event.start.value, "\u{20ac}".repeat(5));
    }

    #[test]
    fn duration_based_end_is_projected() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDURATION:PT1H\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(event.end.expect("end").value, "2026-06-02T13:00:00Z");
    }

    #[test]
    fn end_patch_replaces_duration_instead_of_emitting_both() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDURATION:PT1H\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                end: Some(EventTime {
                    value: "2026-06-02T14:00:00Z".to_string(),
                    timezone: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect("end patch should serialize");

        assert!(body.contains("DTEND:20260602T140000Z"));
        assert!(!body.contains("DURATION:"));
    }

    #[test]
    fn scalar_patch_preserves_same_named_valarm_properties() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDESCRIPTION:Event\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nDESCRIPTION:Ring\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                description: Some(Some("Updated".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("description patch should serialize");

        assert!(body.contains("DESCRIPTION:Updated\r\nBEGIN:VALARM"));
        assert!(body.contains("DESCRIPTION:Ring\r\nEND:VALARM"));
        assert!(!body.contains("DESCRIPTION:Event"));
    }

    #[test]
    fn resource_without_vevent_is_rejected() {
        let result = event_from_ical(
            "/cal/todo.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:t1\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
            None,
        );

        assert_eq!(
            result.expect_err("VTODO-only resource is not an event"),
            EventProjectionError::NoVevent
        );
    }

    #[test]
    fn unspliceable_body_fails_instead_of_writing_back_unchanged() {
        // A truncated VEVENT reaches neither splice point (the first nested
        // component boundary, or END:VEVENT). Returning the body verbatim
        // would PUT the pre-patch resource back and report success, with the
        // patched properties silently gone.
        let mut current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );
        current.raw_ical =
            Some("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\n".to_string());

        let result = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        );

        assert_eq!(result, Err(PatchError::NoSpliceableVevent));
    }

    /// The writer reads a property name exactly as caldata does, untrimmed.
    /// `SUMMARY :Odd` is a property named `SUMMARY ` to caldata, so the
    /// projection never read it as the title and a title patch must not touch
    /// it. The writer used to trim names and deleted it along with the real
    /// `SUMMARY`, destroying a line the model never held.
    #[test]
    fn a_title_patch_leaves_a_property_caldata_does_not_call_summary() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Real\r\nSUMMARY :Odd\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );
        assert_eq!(current.title.as_deref(), Some("Real"));

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("patch");

        assert!(!body.contains("SUMMARY:Real"), "{body}");
        assert!(body.contains("SUMMARY :Odd\r\n"), "{body}");
        assert!(body.contains("SUMMARY:New\r\n"), "{body}");
    }

    /// An override is a VEVENT with a TOP-LEVEL `RECURRENCE-ID`, as the
    /// projection reads it; one inside a VALARM belongs to the alarm. The
    /// writer used to count any `RECURRENCE-ID` between `BEGIN:VEVENT` and
    /// `END:VEVENT` and refused this recurrence patch on a resource with no
    /// override at all.
    #[test]
    fn a_recurrence_id_inside_an_alarm_is_not_an_override() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nRECURRENCE-ID:20260609T120000Z\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: Vec::new(),
                    exdate: Vec::new(),
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect("no top-level RECURRENCE-ID, so no override");

        assert!(body.contains("RRULE:FREQ=DAILY;COUNT=2\r\n"), "{body}");
        assert!(!body.contains("FREQ=WEEKLY"), "{body}");
        assert!(
            body.contains("RECURRENCE-ID:20260609T120000Z\r\nEND:VALARM"),
            "the alarm's own lines are untouched: {body}"
        );
    }

    /// An event with no source body is refused, not rebuilt from the model.
    /// Against the old fallback this returned `Ok` with a from-scratch
    /// resource - here one that has lost the unmodeled `X-KEEP` property the
    /// server holds, which is exactly the silent loss the refusal prevents.
    #[test]
    fn an_event_without_its_source_body_is_refused_rather_than_rebuilt() {
        let mut current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nX-KEEP:yes\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );
        current.raw_ical = None;

        let result = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        );

        assert_eq!(result, Err(PatchError::MissingSourceBody));
    }

    /// A fold may fall anywhere in a line (RFC 5545 s3.1), inside the
    /// `END:VEVENT` delimiter and inside a property name included. The
    /// projection reads the unfolded line; the splice used to read only the
    /// first physical one, so against the old code this body projected and
    /// then failed with no splice point, and a folded `SUMMARY` would have
    /// escaped replacement and been written beside the new one.
    #[test]
    fn a_folded_delimiter_or_property_name_still_splices() {
        let raw = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMM\r\n ARY:Old\r\nX-KEEP:yes\r\nEND:VEV\r\n ENT\r\nEND:VCALENDAR\r\n";
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            raw,
        );
        assert_eq!(
            current.title.as_deref(),
            Some("Old"),
            "the projection unfolds"
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("a legal folded body offers a splice point");

        assert!(body.contains("SUMMARY:New\r\n"), "{body}");
        assert!(
            !body.contains("ARY:Old"),
            "the folded SUMMARY is replaced: {body}"
        );
        assert!(
            body.contains("X-KEEP:yes\r\nSUMMARY:New\r\nEND:VEV\r\n ENT\r\n"),
            "preserved lines, including the folded delimiter, stay verbatim: {body}"
        );
    }

    #[test]
    fn bulk_projection_skips_a_resource_without_vevent() {
        // The listing lanes must not fabricate an empty event for a VTODO
        // sharing the collection: `event_in_range` waves through an event
        // with an empty start, so the phantom would reach the consumer and
        // then fail to open, `event_get` having rejected the same body.
        let events = events_from_ical(
            "/cal/todo.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:t1\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
        )
        .expect("a VEVENT-less resource is not a projection failure");

        assert!(events.is_empty());
    }

    #[test]
    fn parameter_values_use_rfc6868_caret_encoding() {
        assert_eq!(escape_param("Ada \"Ace\" ^ Team"), "Ada ^'Ace^' ^^ Team");
        assert_eq!(escape_param("one\r\ntwo"), "one^ntwo");
    }

    #[test]
    fn caret_encoded_parameter_round_trips_through_projection() {
        // Writing carets a reader keeps verbatim would surface `^'` as part
        // of an attendee's display name.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nATTENDEE;CN=Ada ^'Ace^' Lovelace:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(
            event.attendees[0].name.as_deref(),
            Some("Ada \"Ace\" Lovelace")
        );
    }

    #[test]
    fn attendee_cn_with_a_literal_backslash_round_trips() {
        let name = "Doe\\n John";
        let line = attendee_to_line(&EventAttendee {
            email: "doe@example.test".to_string(),
            name: Some(name.to_string()),
            role: AttendeeRole::Required,
            status: RsvpStatus::NeedsAction,
        })
        .expect("writable attendee");
        let data = format!(
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\n{line}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
        );

        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &data,
        );

        // RFC 6868 defines no backslash escape, so the wire form is the
        // literal character. Doubling it would be invented syntax that only
        // this crate could read back.
        assert!(line.contains("CN=Doe\\n John"));
        assert_eq!(event.attendees[0].name.as_deref(), Some(name));
    }

    #[test]
    fn tzid_with_a_literal_backslash_round_trips() {
        let tzid = "Custom\\Zone";
        let line = format!("DTSTART;TZID={}:20260602T120000", super::escape_param(tzid));
        let data = format!(
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\n{line}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
        );

        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &data,
        );

        assert!(line.contains("TZID=Custom\\Zone:"));
        assert_eq!(event.start.timezone.as_deref(), Some(tzid));
    }

    #[test]
    fn attendee_cn_accepts_exchange_style_text_escapes_on_read() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nATTENDEE;CN=Doe\\, John:mailto:doe@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.attendees[0].name.as_deref(), Some("Doe, John"));
    }

    #[test]
    fn unescape_text_handles_adjacent_backslashes_in_a_single_pass() {
        // Raw `\\n` is an escaped backslash followed by a literal n, not a
        // newline; an ordering-dependent replace-chain corrupts it.
        assert_eq!(unescape_text("a\\\\nb"), "a\\nb");
        assert_eq!(unescape_text("line\\nbreak"), "line\nbreak");
        assert_eq!(unescape_text("big\\Nbreak"), "big\nbreak");
        assert_eq!(unescape_text("semi\\;comma\\,"), "semi;comma,");
        // Unknown escapes and a trailing backslash pass through verbatim.
        assert_eq!(unescape_text("odd\\x"), "odd\\x");
        assert_eq!(unescape_text("tail\\"), "tail\\");
    }

    #[test]
    fn escape_then_unescape_round_trips_text() {
        let original = "a,b;c\\d\nnewline";
        assert_eq!(unescape_text(&escape_text(original)), original);
    }

    #[test]
    fn ignores_non_mailto_attendees() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;CN=Room:/principals/rooms/one\r\nATTENDEE;CN=Ada:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.attendees.len(), 1);
        assert_eq!(event.attendees[0].email, "ada@example.test");
    }

    #[test]
    fn escapes_text_and_params_without_raw_line_breaks() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: Some("Plan\r\nNext".to_string()),
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo:Main".to_string()),
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo:Main".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("SUMMARY:Plan\\nNext"));
        assert!(body.contains("DTSTART;TZID=\"Europe/Oslo:Main\":"));
    }

    fn writable_create() -> EventCreate {
        EventCreate {
            calendar_id: CalendarId("/cal/".to_string()),
            title: Some("Plan".to_string()),
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00Z".to_string(),
                timezone: None,
            },
            end: EventTime {
                value: "2026-06-02T13:00:00Z".to_string(),
                timezone: None,
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        }
    }

    const INJECTION: &str = "FREQ=DAILY\r\nATTENDEE:mailto:evil@example.test";

    fn attendee(email: &str, name: Option<&str>) -> EventAttendee {
        EventAttendee {
            email: email.to_string(),
            name: name.map(ToString::to_string),
            role: AttendeeRole::Required,
            status: RsvpStatus::NeedsAction,
        }
    }

    fn refusal(event: &EventCreate) -> WriteError {
        super::create_to_ical(event, "uid-1").expect_err("the writer must refuse this value")
    }

    /// A value type with no escape form (RECUR, DATE-TIME, CAL-ADDRESS) that
    /// holds a line break is refused and named, on the create path, instead of
    /// injecting a content line. Reverting `plain_value` to a pass-through
    /// writes the ATTENDEE line these values smuggle in.
    #[test]
    fn create_refuses_line_breaks_in_unescapable_values() {
        let mut event = writable_create();
        event.recurrence.rrule = Some(INJECTION.to_string());
        assert_eq!(refusal(&event).field, "recurrence rrule");

        let mut event = writable_create();
        event.recurrence.rdate = vec![INJECTION.to_string()];
        assert_eq!(refusal(&event).field, "recurrence rdate");

        let mut event = writable_create();
        event.recurrence.exdate = vec!["20260602T120000Z\nX-EVIL:1".to_string()];
        assert_eq!(refusal(&event).field, "recurrence exdate");

        let mut event = writable_create();
        event.recurrence.recurrence_id = Some("20260602T120000Z\rX-EVIL:1".to_string());
        assert_eq!(refusal(&event).field, "recurrence id");

        let mut event = writable_create();
        event.attendees = vec![attendee("a@example.test\r\nX-EVIL:1", None)];
        assert_eq!(refusal(&event).field, "attendee email");

        let mut event = writable_create();
        event.organizer = Some(EventOrganizer {
            email: "o@example.test\nX-EVIL:1".to_string(),
            name: None,
        });
        assert_eq!(refusal(&event).field, "organizer email");

        let mut event = writable_create();
        event.start.value = "2026-06-02T12:00:00Z\r\nX-EVIL:1".to_string();
        assert_eq!(refusal(&event).field, "start");

        let mut event = writable_create();
        event.end.value = "2026-06-02T13:00:00Z\nX-EVIL:1".to_string();
        assert_eq!(refusal(&event).field, "end");
    }

    /// Parameter values are encoded, not refused, for the characters RFC 6868
    /// can carry (DQUOTE, CR, LF), so the line stays ONE line and round-trips;
    /// a control character it cannot carry is refused and named.
    #[test]
    fn create_encodes_parameter_values_and_refuses_unencodable_ones() {
        let mut event = writable_create();
        event.attendees = vec![attendee(
            "a@example.test",
            Some("Ada \"The\" Countess\r\nX-EVIL:1"),
        )];
        event.start.timezone = Some("Europe/Oslo".to_string());
        let body = super::create_to_ical(&event, "uid-1").expect("encodable parameters");
        assert!(!body.contains("\r\nX-EVIL"), "{body}");
        let projected = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
            None,
        )
        .expect("the written body reads back");
        assert_eq!(
            projected.attendees[0].name.as_deref(),
            Some("Ada \"The\" Countess\nX-EVIL:1"),
            "the encoded name decodes to the value the caller gave"
        );

        let mut event = writable_create();
        event.attendees = vec![attendee("a@example.test", Some("Ada\u{0}Lovelace"))];
        assert_eq!(refusal(&event).field, "attendee name");

        let mut event = writable_create();
        event.organizer = Some(EventOrganizer {
            email: "o@example.test".to_string(),
            name: Some("Own\u{7f}er".to_string()),
        });
        assert_eq!(refusal(&event).field, "organizer name");

        let mut event = writable_create();
        event.start.timezone = Some("Europe/\u{1}Oslo".to_string());
        assert_eq!(refusal(&event).field, "start timezone");
    }

    fn patch_target() -> CalendarEvent {
        parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        )
    }

    /// The patch splice writes through the same emitters, so the same values
    /// are refused there, as `PatchError::Write`.
    #[test]
    fn patch_refuses_line_breaks_in_unescapable_values() {
        let current = patch_target();
        let recurrence = EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some(INJECTION.to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        };
        let Err(PatchError::Write(error)) = patch_to_ical(&current, &recurrence) else {
            panic!("an injected RRULE must be refused");
        };
        assert_eq!(error.field, "recurrence rrule");

        let attendees = EventPatch {
            attendees: Some(vec![attendee("a@example.test\r\nX-EVIL:1", None)]),
            ..EventPatch::default()
        };
        let Err(PatchError::Write(error)) = patch_to_ical(&current, &attendees) else {
            panic!("an injected attendee address must be refused");
        };
        assert_eq!(error.field, "attendee email");

        let start = EventPatch {
            start: Some(EventTime {
                value: "2026-06-02T12:00:00Z\nX-EVIL:1".to_string(),
                timezone: None,
            }),
            ..EventPatch::default()
        };
        let Err(PatchError::Write(error)) = patch_to_ical(&current, &start) else {
            panic!("an injected start must be refused");
        };
        assert_eq!(error.field, "start");
    }

    /// TEXT excludes control characters other than tab (RFC 5545 s3.3.11), and
    /// they have no escape form: refused and named, on create and on a patch,
    /// never stripped. CR and LF stay escaped as the `\n` line break, tab is
    /// legal. Reverting `text_value` to a plain `escape_text` writes the NUL.
    #[test]
    fn text_values_refuse_control_characters_and_still_escape_line_breaks() {
        let mut event = writable_create();
        event.title = Some("Sta\u{0}nd-up".to_string());
        assert_eq!(refusal(&event).field, "title");

        let mut event = writable_create();
        event.description = Some("bell\u{7}".to_string());
        assert_eq!(refusal(&event).field, "description");

        let mut event = writable_create();
        event.location = Some("Room\u{7f}1".to_string());
        assert_eq!(refusal(&event).field, "location");

        let error = super::create_to_ical(&writable_create(), "uid\u{1b}1")
            .expect_err("a control character in the uid is refused");
        assert_eq!(error.field, "uid");

        let current = patch_target();
        for (patch, field) in [
            (
                EventPatch {
                    title: Some(Some("a\u{0}b".to_string())),
                    ..EventPatch::default()
                },
                "title",
            ),
            (
                EventPatch {
                    description: Some(Some("a\u{8}b".to_string())),
                    ..EventPatch::default()
                },
                "description",
            ),
            (
                EventPatch {
                    location: Some(Some("a\u{c}b".to_string())),
                    ..EventPatch::default()
                },
                "location",
            ),
        ] {
            let Err(PatchError::Write(error)) = patch_to_ical(&current, &patch) else {
                panic!("a control character in {field} must be refused");
            };
            assert_eq!(error.field, field);
        }

        let mut event = writable_create();
        event.title = Some("one\r\ntwo\tthree\rfour\nfive".to_string());
        let body = super::create_to_ical(&event, "uid-1").expect("line breaks and tab are TEXT");
        assert!(
            body.contains("SUMMARY:one\\ntwo\tthree\\nfour\\nfive\r\n"),
            "{body}"
        );
    }

    /// An empty start is never written: `DTSTART:` is invalid and no reading of
    /// "no start" is honest. Refused on create and on a patch that carries it.
    #[test]
    fn an_empty_start_is_refused_on_create_and_patch() {
        let mut event = writable_create();
        event.start = EventTime {
            value: String::new(),
            timezone: None,
        };
        let error = refusal(&event);
        assert_eq!((error.field, error.reason), ("start", "is empty"));

        let patch = EventPatch {
            start: Some(EventTime {
                value: String::new(),
                timezone: None,
            }),
            ..EventPatch::default()
        };
        let Err(PatchError::Write(error)) = patch_to_ical(&patch_target(), &patch) else {
            panic!("an echoed empty start must be refused");
        };
        assert_eq!(error.field, "start");
    }

    /// An empty end means "no end stated", which iCalendar allows (neither
    /// DTEND nor DURATION): create omits the line, and the stored event reads
    /// back with no end.
    #[test]
    fn an_empty_end_on_create_writes_no_dtend() {
        let mut event = writable_create();
        event.end = EventTime {
            value: String::new(),
            timezone: Some("Europe/Oslo".to_string()),
        };
        let body = super::create_to_ical(&event, "uid-1").expect("an empty end is writable");
        assert!(!body.contains("DTEND"), "{body}");
        assert!(
            !body.contains("TZID:Europe/Oslo"),
            "no zone for no end: {body}"
        );
        let projected = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
            None,
        )
        .expect("reads back");
        assert_eq!(projected.end, None);
        assert_eq!(projected.start.value, "2026-06-02T12:00:00Z");
    }

    /// An empty end on a patch leaves the stored DTEND, and a stored DURATION
    /// the projection could not resolve, untouched: the empty value says "no
    /// end stated", and a patch must not delete what the projection could not
    /// read.
    #[test]
    fn an_empty_end_on_a_patch_leaves_the_stored_end_untouched() {
        let empty_end = EventPatch {
            title: Some(Some("Renamed".to_string())),
            end: Some(EventTime {
                value: String::new(),
                timezone: None,
            }),
            ..EventPatch::default()
        };
        let body = patch_to_ical(&patch_target(), &empty_end).expect("splices");
        assert!(body.contains("DTEND:20260602T130000Z\r\n"), "{body}");
        assert!(body.contains("SUMMARY:Renamed"), "{body}");

        let with_duration = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDURATION:garbage\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );
        assert_eq!(with_duration.end, None, "the duration did not resolve");
        let body = patch_to_ical(&with_duration, &empty_end).expect("splices");
        assert!(body.contains("DURATION:garbage\r\n"), "{body}");
        assert!(!body.contains("DTEND"), "{body}");
    }

    /// An empty TZID projects from `TZID=` and is no zone; writing it back
    /// would emit the invalid `;TZID=:`.
    #[test]
    fn an_empty_timezone_writes_no_tzid_parameter() {
        let mut event = writable_create();
        event.start.timezone = Some(String::new());
        let body = super::create_to_ical(&event, "uid-1").expect("writable");
        assert!(body.contains("DTSTART:20260602T120000Z\r\n"), "{body}");
    }

    /// `split_event_id` splits at the first `#`, so a percent-encoded `%23` in
    /// the resource (what a conforming server sends for a literal one) stays in
    /// the resource half, and the id a listing mints for an override splits back
    /// into the parts it was built from.
    #[test]
    fn split_event_id_keeps_an_encoded_hash_in_the_resource() {
        assert_eq!(
            split_event_id("https://h/cal/a%23b.ics"),
            ("https://h/cal/a%23b.ics", None)
        );
        let minted = event_id_for("https://h/cal/a%23b.ics", Some("20260609T120000Z"));
        assert_eq!(
            split_event_id(&minted.0),
            ("https://h/cal/a%23b.ics", Some("20260609T120000Z"))
        );
        // A `#` inside the instance half stays whole in it.
        assert_eq!(
            split_event_id("https://h/cal/a.ics#2026#odd"),
            ("https://h/cal/a.ics", Some("2026#odd"))
        );
    }
}
