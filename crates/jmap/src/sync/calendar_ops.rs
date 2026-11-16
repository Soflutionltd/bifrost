//! Calendar and CalendarEvent account doors.

use super::calendar_mapping::*;
use crate::account::Account as JmapProtoAccount;
use crate::calendar::CalendarGet;
use crate::calendar_event::query::Filter as EventFilter;
use crate::calendar_event::{
    CalendarEvent as JmapCalendarEvent, CalendarEventGet, CalendarEventId, CalendarEventSet,
};
use crate::core::query::Filter as QueryFilter;
use crate::core::transport::HttpTransport;
use crate::sync::page_cursor::PageKind;
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, Calendar, CalendarEvent, EventCreate, EventId,
    EventPatch, EventRange, EventSearchRequest, EventStatus, Page, RsvpStatus,
};

type CalendarAccount<T> = JmapProtoAccount<T, crate::core::capability::Calendars>;

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
        // The query selected matching occurrences but returns master ids.
        // Retain that decision when recurrence or timezone needs server context.
        let events = hydrated
            .events
            .into_iter()
            .filter(|event| range.admits(event))
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
            event.is_all_day(),
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
            .map_err(super::error::answer_error(AccountOperation::EventCreate))?;
        super::error::require_created_id(
            event.id().as_ref().map(CalendarEventId::as_str),
            AccountOperation::EventCreate,
            "CalendarEvent/set",
        )
        .map(EventId)
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
            .map_err(super::error::answer_error(AccountOperation::EventUpdate))?;
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
            .map_err(super::error::answer_error(AccountOperation::EventDelete))
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
            .map_err(super::error::answer_error(AccountOperation::EventRsvp))?;
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
    let not_found = not_found
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let mut seen = std::collections::HashSet::new();
    let requested = requested
        .into_iter()
        .filter(|id| seen.insert(id.clone()))
        .collect::<Vec<_>>();
    let answers = crate::core::answer::read_answers(&requested, list, &not_found, |object| {
        object
            .id()
            .map(CalendarEventId::into_string)
            .filter(|id| !id.is_empty())
    });
    let mut events = Vec::new();
    let mut failed_ids = Vec::new();
    for (id, answer) in answers {
        match answer {
            crate::core::answer::IdAnswer::Ok(event) => match event_from_jmap(event, operation) {
                Ok(event) => events.push(event),
                Err(_) => failed_ids.push(id),
            },
            _ => failed_ids.push(id),
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
        .call(CalendarEventGet::new().ids([id.clone()]))
        .await
        .map_err(to_acct_err(operation))?;
    let not_found = response.not_found().contains(&id);
    let answer_context = response.answer_context;
    super::single_get::require_one(
        response.into_list(),
        |event| event.id().is_some_and(|returned| returned == id),
        not_found,
        answer_context,
        id.as_str(),
        operation,
        bifrost_types::ErrorScope::Event {
            id: EventId(id.clone().into_string()),
        },
    )
}

/// The calendar page walk's parameters for the shared anchored, state-pinned
/// page cursor (`page_cursor`): the id marker, the query builder, and the
/// wording of its diagnostics. Behaviour is not a parameter.
pub(super) struct CalendarPages;

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
            bifrost_types::UnsupportedReason::DataRepresentation,
            format!("JMAP calendar total does not fit u64: {error}"),
        )
    })
}

fn require_calendars<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    operation: AccountOperation,
) -> Result<CalendarAccount<T>, AccountError> {
    calendars.ok_or_else(|| {
        super::error::unsupported_error(
            operation,
            None,
            bifrost_types::UnsupportedReason::CapabilityUnavailable,
            "JMAP Calendars capability is unavailable",
        )
    })
}

fn unsupported(operation: AccountOperation, message: &str) -> AccountError {
    super::error::unsupported_error(
        operation,
        None,
        bifrost_types::UnsupportedReason::DataRepresentation,
        message,
    )
}

pub(super) fn to_acct_err(
    operation: AccountOperation,
) -> impl FnOnce(crate::Error) -> AccountError + Send + 'static {
    move |error| {
        super::error::into_account_error(error, super::error::JmapErrorContext::new(operation))
    }
}

#[cfg(test)]
mod tests;
