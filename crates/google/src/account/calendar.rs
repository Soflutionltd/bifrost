//! Google Calendar list, range, search, and event CRUD.
//!
//! The Account adapter selects these futures against its shutdown token, so
//! close can interrupt an active request without adding token plumbing to
//! every Calendar operation.
//!
//! Audit boundary: the 2026-07 google bug sweep did NOT line-audit this
//! module; it was skipped as already test-dense. A later auditor should read
//! it fresh rather than assuming that sweep covered it.

use std::collections::HashSet;
use std::sync::Arc;

#[cfg(test)]
use bifrost_types::{
    AccountErrorKind, Cause, Protocol, ProtocolErrorKind, TransmissionState, WireCause,
};
use bifrost_types::{
    AttendeeRole, Calendar, CalendarEvent, CalendarId, CalendarProvenance, DiagnosticText,
    EventAttendee, EventAvailability, EventCreate, EventId, EventOrganizer, EventPatch, EventRange,
    EventRecurrence, EventSearchRequest, EventStatus, EventTime, EventVisibility, Page,
    ProtocolKind, RsvpStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::client::GmailClient;

use super::error::{self, GmailErrorContext};
use bifrost_types::{AccountError, AccountFuture, AccountOperation};

const EVENT_ID_SEPARATOR: &str = "::";
const CALENDAR_LIST_PAGE_SIZE: u16 = 250;
const MAX_CALENDAR_LIST_PAGES: usize = 10_000;

// Returns a whole `Vec`, not a `Page`, and that is a considered choice
// rather than an omission. The walk below pages at 250 per request under a
// repeated-token guard and a finite page budget, so it is bounded; a real
// account has tens of calendars, which is one page. The shared trait draws
// this line deliberately - `contacts_list` returns `Page<ContactCard>`
// because contacts number in the tens of thousands, and calendars do not.
// It would change if a deployment appeared where a single account carries
// hundreds of calendars and the caller needs to act before the walk ends;
// short of that, streaming buys nothing and costs the callers (cross-calendar
// event search among them) their simple finite vector.
pub(crate) fn calendars_list(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
    Box::pin(async move {
        let base_url = format!(
            "{}/users/me/calendarList?maxResults={CALENDAR_LIST_PAGE_SIZE}",
            client.calendar_base()
        );
        let mut calendars = Vec::new();
        let mut page_token = None;
        let mut seen_tokens = HashSet::new();
        for _ in 0..MAX_CALENDAR_LIST_PAGES {
            let mut url = base_url.clone();
            if let Some(token) = page_token.as_deref() {
                url.push_str("&pageToken=");
                url.push_str(&bifrost_net::url::encode_query_value(token));
            }
            let response: CalendarListResponse = client
                .get(&url)
                .await
                .map_err(|error| collection_error(error, AccountOperation::CalendarsList))?;
            calendars.extend(
                response
                    .items
                    .unwrap_or_default()
                    .into_iter()
                    .map(calendar_from_google),
            );
            let Some(next_token) = response.next_page_token else {
                return Ok(calendars);
            };
            if !seen_tokens.insert(next_token.clone()) {
                return Err(page_refusal(
                    AccountOperation::CalendarsList,
                    crate::error::PageRefusal::RepeatedToken(
                        "Google Calendar calendarList repeated a page token".to_string(),
                    ),
                ));
            }
            page_token = Some(next_token);
        }
        Err(page_refusal(
            AccountOperation::CalendarsList,
            crate::error::PageRefusal::BudgetExceeded(format!(
                "Google Calendar calendarList exceeded {MAX_CALENDAR_LIST_PAGES} pages"
            )),
        ))
    })
}

pub(crate) fn events_in_range(
    client: Arc<GmailClient>,
    range: EventRange,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        let page_token = range
            .page_cursor
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| {
                caller_input_error(
                    AccountOperation::EventsInRange,
                    format!("event page cursor is not UTF-8: {error}"),
                )
            })?;
        let calendar_id = range.calendar_id.0;
        let encoded = bifrost_net::url::encode_path_component(&calendar_id);
        let time_min = google_range_bound(&range.start, "timeMin")?;
        let time_max = google_range_bound(&range.end, "timeMax")?;
        let mut url = format!(
            "{}/calendars/{encoded}/events?singleEvents=true&showDeleted=true&orderBy=startTime&timeMin={}&timeMax={}",
            client.calendar_base(),
            bifrost_net::url::encode_query_value(&time_min),
            bifrost_net::url::encode_query_value(&time_max)
        );
        if let Some(limit) = range.limit {
            url.push_str("&maxResults=");
            url.push_str(&limit.min(2500).to_string());
        }
        if let Some(token) = page_token {
            url.push_str("&pageToken=");
            url.push_str(&bifrost_net::url::encode_query_value(&token));
        }
        let response: EventsResponse = client
            .get(&url)
            .await
            .map_err(|error| collection_error(error, AccountOperation::EventsInRange))?;
        page_from_events(calendar_id, response, AccountOperation::EventsInRange)
    })
}

pub(crate) fn get(
    client: Arc<GmailClient>,
    event: EventId,
) -> AccountFuture<Result<CalendarEvent, AccountError>> {
    Box::pin(async move {
        let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventGet)?;
        let url = event_url(client.calendar_base(), &calendar_id, &event_id);
        let event: GoogleEvent = client
            .get(&url)
            .await
            .map_err(|error| event_error(error, AccountOperation::EventGet, event.0.clone()))?;
        event_from_google(calendar_id, event, AccountOperation::EventGet)
    })
}

pub(crate) fn create(
    client: Arc<GmailClient>,
    event: EventCreate,
) -> AccountFuture<Result<EventId, AccountError>> {
    Box::pin(async move {
        reject_create_organizer(&event)?;
        reject_unusable_write_times(
            AccountOperation::EventCreate,
            Some(&event.start),
            Some(&event.end),
            Some(event.is_all_day),
        )?;
        let body = google_event_from_create(&event)?;
        let calendar_id = event.calendar_id.0.clone();
        let encoded = bifrost_net::url::encode_path_component(&calendar_id);
        let url = format!("{}/calendars/{encoded}/events", client.calendar_base());
        let created: GoogleEvent = client
            .post(&url, &body)
            .await
            .map_err(|error| collection_error(error, AccountOperation::EventCreate))?;
        let id = created.id.ok_or_else(|| {
            local_error_with_field(
                AccountOperation::EventCreate,
                "id",
                "created Google event has no id".into(),
            )
        })?;
        Ok(EventId(join_event_id(&calendar_id, &id)))
    })
}

fn reject_create_organizer(event: &EventCreate) -> Result<(), AccountError> {
    if event.organizer.is_some() {
        return Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                AccountOperation::EventCreate,
                "Google Calendar organizer is server-derived on event creation",
            ),
            GmailErrorContext::calendar_collection(AccountOperation::EventCreate),
        ));
    }
    Ok(())
}

pub(crate) fn update(
    client: Arc<GmailClient>,
    event: EventId,
    patch: EventPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (mut calendar_id, native_event_id) =
            split_event_id(&event.0, AccountOperation::EventUpdate)?;
        let has_field_patch = event_patch_has_non_move_fields(&patch);
        // Validate and encode the field patch before a move. The patch's
        // times and attendee statuses cannot be repaired by moving the event,
        // so refusing them afterwards would strand it in the destination.
        let google_patch = if has_field_patch {
            reject_unexpressible_all_day_patch(&patch)?;
            reject_unusable_write_times(
                AccountOperation::EventUpdate,
                patch.start.as_ref(),
                patch.end.as_ref(),
                patch.is_all_day,
            )?;
            Some(google_event_from_patch(&patch)?)
        } else {
            None
        };
        let mut moved = false;
        if let Some(target_calendar) = patch.calendar_id.as_ref()
            && target_calendar.0 != calendar_id
        {
            let _: GoogleEvent = client
                .post(
                    &event_move_url(
                        client.calendar_base(),
                        &calendar_id,
                        &native_event_id,
                        &target_calendar.0,
                    ),
                    &json!({}),
                )
                .await
                .map_err(|error| {
                    event_error(error, AccountOperation::EventUpdate, event.0.clone())
                })?;
            calendar_id = target_calendar.0.clone();
            moved = true;
        }
        if !has_field_patch {
            return Ok(());
        }
        let url = event_url(client.calendar_base(), &calendar_id, &native_event_id);
        // After a move the event lives under the destination calendar, so
        // the composite id the caller passed in no longer addresses it.
        // Scope the patch failure to where the event actually is - a
        // reconcile directive that names the source calendar sends the
        // consumer to look at a resource that is no longer there.
        let target_event_id = join_event_id(&calendar_id, &native_event_id);
        let google_patch = google_patch.expect("a field patch was prepared before the move");
        let _: GoogleEvent = client.patch(&url, &google_patch).await.map_err(|error| {
            let error = event_error(error, AccountOperation::EventUpdate, target_event_id);
            if moved {
                event_move_patch_error(error)
            } else {
                error
            }
        })?;
        Ok(())
    })
}

pub(crate) fn delete(
    client: Arc<GmailClient>,
    event: EventId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventDelete)?;
        client
            .delete(&event_url(client.calendar_base(), &calendar_id, &event_id))
            .await
            .map_err(|error| event_error(error, AccountOperation::EventDelete, event.0.clone()))
    })
}

pub(crate) fn rsvp(
    client: Arc<GmailClient>,
    self_email: String,
    event: EventId,
    status: RsvpStatus,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // Validate the requested status before the GET so an unrepresentable
        // shared value can never turn into an accidental `needsAction` write.
        let response_status = rsvp_request_value(status)?;
        let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventRsvp)?;
        let mut current: GoogleEvent = client
            .get(&event_url(client.calendar_base(), &calendar_id, &event_id))
            .await
            .map_err(|error| event_error(error, AccountOperation::EventRsvp, event.0.clone()))?;
        rsvp_google_attendees_for_self(&mut current.attendees, &self_email, response_status)?;
        let _: GoogleEvent = client
            .patch(
                &event_url(client.calendar_base(), &calendar_id, &event_id),
                &GoogleEventPatch {
                    attendees: current.attendees,
                    ..GoogleEventPatch::default()
                },
            )
            .await
            .map_err(|error| event_error(error, AccountOperation::EventRsvp, event.0.clone()))?;
        Ok(())
    })
}

pub(crate) fn search(
    client: Arc<GmailClient>,
    request: EventSearchRequest,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        if let Some(calendar_id) = request.calendar_id {
            return search_one_calendar(
                &client,
                calendar_id.0,
                &request.query,
                request.limit,
                decode_page_token(request.page_cursor, AccountOperation::EventSearch)?,
                request.include_cancelled,
            )
            .await;
        }

        let calendars = calendars_list(Arc::clone(&client)).await?;
        let calendar_ids = calendars
            .into_iter()
            .map(|calendar| calendar.id.0)
            .collect::<Vec<_>>();
        let (mut index, mut page_token) =
            decode_cross_calendar_cursor(request.page_cursor, &calendar_ids)?;
        let limit = request
            .limit
            .and_then(|limit| usize::try_from(limit).ok())
            .unwrap_or(250)
            .min(2500);
        let mut items = Vec::new();
        // The per-calendar pages can now carry refused ids, and this
        // aggregate is the only Page the caller ever sees. Zeroing the lane
        // here would reinstate the silent drop one layer up.
        let mut failed_ids = Vec::new();
        while let Some(calendar_id) = calendar_ids.get(index) {
            let remaining = limit.saturating_sub(items.len());
            if remaining == 0 {
                return Ok(Page {
                    truncated: false,
                    items,
                    next_cursor: Some(encode_cross_calendar_cursor(calendar_id, &[])),
                    estimated_total: None,
                    failed_ids,
                    skipped_scopes: Vec::new(),
                });
            }
            let page = search_one_calendar(
                &client,
                calendar_id.clone(),
                &request.query,
                Some(u32::try_from(remaining).unwrap_or(2500)),
                page_token.take(),
                request.include_cancelled,
            )
            .await?;
            // Google honours `maxResults`, but guard the cap defensively so
            // a loose page can never push the aggregate over the request
            // limit. Any clipped tail is recoverable: the page carried a
            // next_cursor only when more remained, and the boundary cursor
            // re-enters this calendar otherwise.
            //
            // Accepted residual, not an open defect: resume here is by
            // calendar id AND provider page token, but NOT by position
            // WITHIN an over-delivered page - a page Google delivers longer
            // than `maxResults` has its tail discarded rather than resumed.
            // Adding an intra-page offset would mean minting a third cursor
            // coordinate for a case that requires the provider to violate
            // its own documented cap. Low severity, deliberately left.
            let next_token = page.next_cursor;
            failed_ids.extend(page.failed_ids);
            for item in page.items {
                if items.len() >= limit {
                    break;
                }
                items.push(item);
            }
            if let Some(next_token) = next_token {
                return Ok(Page {
                    truncated: false,
                    items,
                    next_cursor: Some(encode_cross_calendar_cursor(calendar_id, &next_token)),
                    estimated_total: None,
                    failed_ids,
                    skipped_scopes: Vec::new(),
                });
            }
            index += 1;
            if items.len() >= limit {
                return Ok(Page {
                    truncated: false,
                    items,
                    next_cursor: calendar_ids
                        .get(index)
                        .map(|calendar_id| encode_cross_calendar_cursor(calendar_id, &[])),
                    estimated_total: None,
                    failed_ids,
                    skipped_scopes: Vec::new(),
                });
            }
        }
        Ok(Page {
            truncated: false,
            items,
            next_cursor: None,
            estimated_total: None,
            failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

/// The query sends `showDeleted=true` only when the caller set
/// `include_cancelled`. A search is a query surface, so by default it answers
/// with live events only (Google hides cancelled events unless asked); the
/// range walk above is a COVERAGE question and always carries tombstones.
/// Cancelled stubs project through `event_from_google`'s tombstone tolerance,
/// so they cannot poison the page.
async fn search_one_calendar(
    client: &GmailClient,
    calendar_id: String,
    query: &str,
    limit: Option<u32>,
    page_token: Option<String>,
    include_cancelled: bool,
) -> Result<Page<CalendarEvent>, AccountError> {
    let encoded = bifrost_net::url::encode_path_component(&calendar_id);
    let show_deleted = if include_cancelled {
        "&showDeleted=true"
    } else {
        ""
    };
    let mut url = format!(
        "{}/calendars/{encoded}/events?singleEvents=true{show_deleted}&orderBy=startTime&q={}",
        client.calendar_base(),
        bifrost_net::url::encode_query_value(query)
    );
    if let Some(limit) = limit {
        url.push_str("&maxResults=");
        url.push_str(&limit.min(2500).to_string());
    }
    if let Some(token) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(&token));
    }
    let response: EventsResponse = client
        .get(&url)
        .await
        .map_err(|error| collection_error(error, AccountOperation::EventSearch))?;
    page_from_events(calendar_id, response, AccountOperation::EventSearch)
}

// A projection failure is a fact about ONE event, so it is isolated to that
// event rather than destroying the page around it. Collecting through
// `Result` meant a single item the projection refused failed every
// neighbour Google had already returned, and the walk had no way to make
// progress past it. The refused id rides `Page::failed_ids`, which exists
// precisely so a consumer can tell "the provider could not give me this
// one" apart from "this one is gone" - dropping it silently would let a
// range reread read the absence as a deletion. The sibling calendar
// providers (bifrost-jmap, bifrost-caldav) already route per-item
// conversion failures the same way.
//
// This is not a licence to swallow a parse regression: a refused item is
// still absent from `items` and still named in a lane the sync engine
// turns into an operator warning. What it stops being is a page-level
// error.
fn page_from_events(
    calendar_id: String,
    response: EventsResponse,
    operation: AccountOperation,
) -> Result<Page<CalendarEvent>, AccountError> {
    let mut items = Vec::new();
    let mut failed_ids = Vec::new();
    for event in response.items.unwrap_or_default() {
        // Captured before the projection consumes the event, and in the
        // same composite namespace as `CalendarEvent::native_id`, so a
        // consumer can match a refused id against what it already holds.
        let native = join_event_id(&calendar_id, event.id.as_deref().unwrap_or_default());
        match event_from_google(calendar_id.clone(), event, operation) {
            Ok(event) => items.push(event),
            Err(_) => failed_ids.push(native),
        }
    }
    Ok(Page {
        truncated: false,
        items,
        next_cursor: response.next_page_token.map(String::into_bytes),
        estimated_total: None,
        failed_ids,
        skipped_scopes: Vec::new(),
    })
}

fn decode_page_token(
    page_cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<Option<String>, AccountError> {
    page_cursor
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| {
            caller_input_error(
                operation,
                format!("event page cursor is not UTF-8: {error}"),
            )
        })
}

fn encode_cross_calendar_cursor(calendar_id: &str, page_token: &[u8]) -> Vec<u8> {
    let mut cursor = calendar_id.as_bytes().to_vec();
    cursor.push(b'\n');
    cursor.extend_from_slice(page_token);
    cursor
}

fn decode_cross_calendar_cursor(
    page_cursor: Option<Vec<u8>>,
    calendar_ids: &[String],
) -> Result<(usize, Option<String>), AccountError> {
    let Some(cursor) = decode_page_token(page_cursor, AccountOperation::EventSearch)? else {
        return Ok((0, None));
    };
    let (calendar_id, page_token) = cursor.split_once('\n').ok_or_else(|| {
        caller_input_error(
            AccountOperation::EventSearch,
            "cross-calendar search cursor is missing calendar id".to_string(),
        )
    })?;
    // A cursor this crate minted can name a calendar the account no longer
    // lists: it was removed between pages. That is state moving under the
    // caller's pagination, not a caller bug and not a provider fault, so it is
    // refused before the search request as a conflict whose refresh is
    // restarting the search.
    let index = calendar_ids
        .iter()
        .position(|candidate| candidate == calendar_id)
        .ok_or_else(|| {
            bifrost_types::AccountErrorBuilder::new(
                bifrost_types::AccountErrorKind::ConcurrencyConflict,
                bifrost_types::Cause::State(bifrost_types::StateCause::ConcurrencyConflict),
            )
            .push_cause(bifrost_types::Cause::Attempt(
                bifrost_types::AttemptCause::new(bifrost_types::TransmissionState::Unsent),
            ))
            .operation(AccountOperation::EventSearch)
            .provider(bifrost_types::Provider::Gmail)
            .protocol(bifrost_types::Protocol::Gmail)
            .text(bifrost_types::DiagnosticText::support_only(
                "cross-calendar search cursor references a calendar no longer listed; \
                 restart the search",
            ))
            .try_build()
            .expect("valid account error classification")
        })?;
    let page_token = (!page_token.is_empty()).then(|| page_token.to_string());
    Ok((index, page_token))
}

fn calendar_from_google(calendar: GoogleCalendarListEntry) -> Calendar {
    let native = calendar.id.unwrap_or_default();
    let access = calendar.access_role.unwrap_or_default();
    let can_write = matches!(access.as_str(), "owner" | "writer");
    Calendar {
        id: CalendarId(native.clone()),
        native_id: native.clone(),
        name: calendar.summary.unwrap_or_else(|| "Calendar".to_string()),
        color: calendar.background_color,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Gmail,
            native,
            calendar_native: None,
        },
        is_default: calendar.primary.unwrap_or(false),
        can_create_events: can_write,
        can_update_events: can_write,
        can_delete_events: can_write,
    }
}

fn event_from_google(
    calendar_id: String,
    event: GoogleEvent,
    operation: AccountOperation,
) -> Result<CalendarEvent, AccountError> {
    let event_id = event.id.unwrap_or_default();
    let native = join_event_id(&calendar_id, &event_id);
    let is_cancelled = event.status.as_deref() == Some("cancelled");
    let cancelled_instance_time = is_cancelled
        .then(|| event.original_start_time.clone())
        .flatten();
    // `is_all_day` must be read off the EFFECTIVE start, not `event.start`.
    // A cancelled instance of an all-day recurrence arrives with no `start`
    // at all and only `originalStartTime.date`, so deciding all-day-ness
    // from `event.start` alone yields a date-valued start and end paired
    // with `is_all_day: false` - a shape the shared surface treats as a
    // timed event and renders at midnight.
    let effective_start = event.start.or_else(|| cancelled_instance_time.clone());
    let is_all_day = effective_start
        .as_ref()
        .is_some_and(|time| time.date.is_some() && time.date_time.is_none());
    // A tombstone is not a malformed event. Under `showDeleted=true` a
    // cancelled INSTANCE of a recurrence arrives with only
    // `originalStartTime`, and a cancelled STANDALONE event arrives as a
    // bare stub: an id, `status: "cancelled"`, and no `start`, `end` or
    // `originalStartTime` at all. Both are the deletion notice the caller
    // asked for, and the only fact they carry is "this id is gone", so
    // they project with whatever time the provider did send and an empty
    // `EventTime` where it sent none. Refusing them would turn a normal
    // deletion into a projection error. A LIVE event missing its times is
    // still an error - that is a provider contract break, not a deletion.
    // That includes a live event with a start and no end: Google documents
    // that an end is sent even when the event's end is unspecified
    // (`endTimeUnspecified: true`, "an end time is still provided for
    // compatibility reasons"), so projecting a missing one as zero-length
    // would invent a duration the provider promised to state.
    let start = match effective_start.map(event_time) {
        Some(start) => start,
        None if is_cancelled => unknown_event_time(),
        None => {
            return Err(local_error_with_field(
                operation,
                "start",
                "Google event missing start".to_string(),
            ));
        }
    };
    //
    // `endTimeUnspecified: true` is the one case where that provided end is
    // NOT a real end: Google fills `end` with a placeholder (typically the
    // start) only because its API cannot omit the field. It projects as no
    // end at all. Forwarding the placeholder would tell the consumer a
    // fabricated end is real, and substituting the start would invent a
    // zero-length duration. The placeholder is discarded even when `end` is
    // absent, so an unspecified end never trips the missing-end refusal
    // below. A tombstone with no end has none either, rather than a copy of
    // its start.
    let end = match event.end.or(cancelled_instance_time).map(event_time) {
        _ if event.end_time_unspecified == Some(true) => None,
        Some(end) => Some(end),
        None if is_cancelled => None,
        None => {
            return Err(local_error_with_field(
                operation,
                "end",
                "Google event missing end".to_string(),
            ));
        }
    };
    Ok(CalendarEvent {
        id: EventId(native.clone()),
        calendar_id: CalendarId(calendar_id.clone()),
        native_id: native.clone(),
        uid: event.i_cal_uid,
        etag: event.etag,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Gmail,
            native,
            calendar_native: Some(calendar_id),
        },
        title: event.summary,
        description: event.description,
        location: event.location,
        start,
        end,
        is_all_day,
        status: event_status(event.status.as_deref()),
        availability: availability(event.transparency.as_deref()),
        visibility: visibility(event.visibility.as_deref()),
        self_response: RsvpStatus::Unknown,
        organizer: event.organizer.and_then(|organizer| {
            Some(EventOrganizer {
                email: organizer.email?,
                name: organizer.display_name,
            })
        }),
        attendees: event
            .attendees
            .unwrap_or_default()
            .into_iter()
            .filter_map(attendee_from_google)
            .collect(),
        // Google Calendar reminders are not projected onto the shared read
        // surface yet; other providers (CalDAV VALARM, JMAP alerts) supply
        // them.
        reminders: Vec::new(),
        recurrence: EventRecurrence {
            rrule: event
                .recurrence
                .as_ref()
                .and_then(|values| values.iter().find_map(|value| value.strip_prefix("RRULE:")))
                .map(ToString::to_string),
            rdate: prefixed_values(event.recurrence.as_deref(), "RDATE:"),
            exdate: prefixed_values(event.recurrence.as_deref(), "EXDATE:"),
            recurrence_id: event
                .original_start_time
                .map(event_time)
                .map(|time| time.value),
        },
        html_link: event.html_link,
        raw_ical: None,
    })
}

fn google_event_from_create(event: &EventCreate) -> Result<GoogleEventPatch, AccountError> {
    Ok(GoogleEventPatch {
        summary: event.title.clone().map(Some),
        description: event.description.clone().map(Some),
        location: event.location.clone().map(Some),
        start: Some(google_time(&event.start, event.is_all_day)),
        // A blank end never reaches here: `reject_unusable_write_times`
        // refuses it on create.
        end: Some(google_time(&event.end, event.is_all_day)),
        status: Some(google_event_status(event.status).to_string()),
        attendees: google_attendees(&event.attendees, AccountOperation::EventCreate)?,
        recurrence: recurrence_lines(&event.recurrence),
        visibility: Some(match event.visibility {
            EventVisibility::Private => "private".to_string(),
            EventVisibility::Public => "public".to_string(),
            EventVisibility::Confidential => "confidential".to_string(),
            EventVisibility::Default => "default".to_string(),
            _ => "default".to_string(),
        }),
        transparency: Some(transparency(event.availability).to_string()),
    })
}

fn google_event_from_patch(patch: &EventPatch) -> Result<GoogleEventPatch, AccountError> {
    Ok(GoogleEventPatch {
        summary: patch.title.clone(),
        description: patch.description.clone(),
        location: patch.location.clone(),
        start: patch
            .start
            .as_ref()
            .map(|time| google_time(time, patch.is_all_day.unwrap_or(is_ymd_date(&time.value)))),
        // An empty `end` is the projection's "no end" for an
        // `endTimeUnspecified` event (see `reject_unusable_write_times`), so
        // echoing it back leaves the stored end alone: it is omitted rather
        // than sent as `dateTime: ""`.
        end: patch
            .end
            .as_ref()
            .filter(|time| !is_blank_time(time))
            .map(|time| google_time(time, patch.is_all_day.unwrap_or(is_ymd_date(&time.value)))),
        status: patch
            .status
            .map(|status| google_event_status(status).to_string()),
        attendees: patch
            .attendees
            .as_ref()
            .map(|attendees| google_attendee_vec(attendees, AccountOperation::EventUpdate))
            .transpose()?,
        // A present recurrence patch always emits the key. An empty
        // `EventRecurrence` produces an empty array, which is how the
        // Google API clears an existing RRULE/RDATE/EXDATE; an absent
        // patch omits the key and leaves the server value untouched.
        recurrence: patch
            .recurrence
            .as_ref()
            .map(|recurrence| recurrence_lines(recurrence).unwrap_or_default()),
        visibility: patch.visibility.map(|visibility| match visibility {
            EventVisibility::Private => "private".to_string(),
            EventVisibility::Public => "public".to_string(),
            EventVisibility::Confidential => "confidential".to_string(),
            EventVisibility::Default => "default".to_string(),
            _ => "default".to_string(),
        }),
        transparency: patch
            .availability
            .map(|availability| transparency(availability).to_string()),
    })
}

fn attendee_from_google(attendee: GoogleAttendee) -> Option<EventAttendee> {
    Some(EventAttendee {
        email: attendee.email?,
        name: attendee.display_name,
        role: attendee.resource.map_or_else(
            || {
                attendee
                    .optional
                    .map_or(AttendeeRole::Required, |optional| {
                        if optional {
                            AttendeeRole::Optional
                        } else {
                            AttendeeRole::Required
                        }
                    })
            },
            |resource| {
                if resource {
                    AttendeeRole::Resource
                } else {
                    AttendeeRole::Required
                }
            },
        ),
        status: rsvp_status(attendee.response_status.as_deref()),
    })
}

fn google_attendees(
    attendees: &[EventAttendee],
    operation: AccountOperation,
) -> Result<Option<Vec<GoogleAttendee>>, AccountError> {
    if attendees.is_empty() {
        return Ok(None);
    }
    google_attendee_vec(attendees, operation).map(Some)
}

fn google_attendee_vec(
    attendees: &[EventAttendee],
    operation: AccountOperation,
) -> Result<Vec<GoogleAttendee>, AccountError> {
    attendees
        .iter()
        .map(|attendee| google_attendee(attendee, operation))
        .collect()
}

fn google_attendee(
    attendee: &EventAttendee,
    operation: AccountOperation,
) -> Result<GoogleAttendee, AccountError> {
    Ok(GoogleAttendee {
        email: Some(attendee.email.clone()),
        display_name: attendee.name.clone(),
        response_status: Some(attendee_status_value(attendee.status, operation)?.to_string()),
        optional: Some(matches!(attendee.role, AttendeeRole::Optional)),
        resource: matches!(attendee.role, AttendeeRole::Resource).then_some(true),
        extra: Map::new(),
    })
}

fn google_time(time: &EventTime, all_day: bool) -> GoogleEventTime {
    if all_day {
        GoogleEventTime {
            date: Some(time.value.clone()),
            date_time: None,
            time_zone: time.timezone.clone(),
        }
    } else {
        GoogleEventTime {
            date: None,
            date_time: Some(time.value.clone()),
            time_zone: time.timezone.clone(),
        }
    }
}

fn is_blank_time(time: &EventTime) -> bool {
    time.value.trim().is_empty()
}

// Write-side guard for event times, run before any request (and, on update,
// before the calendar move) so a refused patch strands nothing.
//
// `start` must carry a value. The empty `EventTime` is what the read side
// projects for a cancelled tombstone, and Google has no meaning for a start
// of `dateTime: ""`, so an empty start is malformed caller input:
// `Request(Malformed)` naming `start`.
//
// An empty `end` is how a caller says "no end". The read side projects an
// `endTimeUnspecified: true` event with `CalendarEvent::end` `None`
// (documented in `reference/google.md`), which echoes into a patch as no end
// change at all; an empty `EventTime` is the same statement made explicitly.
// `endTimeUnspecified` is not a writable Events property, so "no end" cannot
// be written; forwarding the empty value would send `dateTime: ""`, and
// substituting the start would fabricate a zero-length end. What an empty end
// can honestly mean on a write therefore depends on the write:
// - a patch leaves the stored end untouched, so the empty end is omitted and
//   the read-modify-write round-trips. That holds whether or not the patch
//   carries `start`: a consumer echoing the whole event sends its unchanged
//   start back, and nothing here can tell an echo from a move without
//   fetching the stored event. A real move past Google's stored placeholder
//   end is refused by Google itself, with nothing invented on this side;
// - a create must send some end, so it is `Unsupported`: the caller has to
//   supply the end it wants.
//
// A non-empty value must also have the shape Google will read, checked here
// rather than left to Google's 400. Google does answer a malformed value with
// a 400, which classifies as `Request(Malformed)` (the caller's input) with
// nothing applied, and that is an honest outcome on a create or a plain field
// patch. It is NOT on an update that also moves the event to another
// calendar: `events.move` has already succeeded when the field patch is
// rejected. The destination-scoped second-leg error retains its own recovery
// class and carries support text recording the move. Since this guard runs
// before the move, the shape rules below (a bare `YYYY-MM-DD` date for an
// all-day value, a full RFC 3339 timestamp with an offset or a local
// date-time plus a `timezone` for a timed one) turn the common bad-input cases
// into a refusal that strands nothing.
//
// Accepted limit: the `timezone` NAME is not validated (that would need a
// time-zone database this crate does not carry, and a false refusal is worse
// than Google's 400), and neither are calendar-valid corner cases beyond what
// the date and timestamp parsers reject. An unknown zone name combined with a
// calendar move can therefore still leave the event moved with a failed
// destination patch; the second-leg error retains its classification and
// names the successful move in support diagnostics.
//
// `is_all_day` is the write's all-day flag when it states one; when it does
// not (a patch that leaves it unset) each value is judged by its own shape,
// exactly as `google_event_from_patch` chooses `date` versus `dateTime`.
fn reject_unusable_write_times(
    operation: AccountOperation,
    start: Option<&EventTime>,
    end: Option<&EventTime>,
    is_all_day: Option<bool>,
) -> Result<(), AccountError> {
    if start.is_some_and(is_blank_time) {
        return Err(caller_input_error(
            operation,
            "Google Calendar event `start` must not be empty".to_string(),
        ));
    }
    for (field, time) in [("start", start), ("end", end)] {
        if let Some(time) = time.filter(|time| !is_blank_time(time)) {
            reject_malformed_time_shape(operation, field, time, is_all_day)?;
        }
    }
    if operation == AccountOperation::EventCreate && end.is_some_and(is_blank_time) {
        return Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                operation,
                "Google Calendar cannot create an event with no end: endTimeUnspecified is not \
                 writable, so a create needs an explicit end",
            ),
            GmailErrorContext::calendar_collection(operation),
        ));
    }
    Ok(())
}

fn reject_malformed_time_shape(
    operation: AccountOperation,
    field: &str,
    time: &EventTime,
    is_all_day: Option<bool>,
) -> Result<(), AccountError> {
    let value = time.value.as_str();
    if is_all_day.unwrap_or_else(|| is_ymd_date(value)) {
        if is_ymd_date(value) && value.parse::<jiff::civil::Date>().is_ok() {
            return Ok(());
        }
        return Err(caller_input_error(
            operation,
            format!("Google Calendar all-day `{field}` must be a YYYY-MM-DD date"),
        ));
    }
    let has_time_separator = value.contains('T');
    if has_time_separator && has_rfc3339_offset(value) && value.parse::<jiff::Timestamp>().is_ok() {
        return Ok(());
    }
    // A local date-time is only meaningful beside a `timezone`; Google reads
    // it in that zone. Offset and `Z` forms were handled above, so anything
    // still carrying one here is not a bare local time.
    let is_bare_local = has_time_separator
        && !value.contains(['+', 'Z', 'z', '['])
        && !value
            .rsplit('T')
            .next()
            .is_some_and(|time| time.contains('-'))
        && value.parse::<jiff::civil::DateTime>().is_ok();
    if is_bare_local
        && time
            .timezone
            .as_deref()
            .is_some_and(|zone| !zone.trim().is_empty())
    {
        return Ok(());
    }
    Err(caller_input_error(
        operation,
        format!(
            "Google Calendar timed `{field}` must be an RFC 3339 timestamp with an offset, \
             or a local date-time with a `timezone`"
        ),
    ))
}

fn google_range_bound(time: &EventTime, field: &'static str) -> Result<String, AccountError> {
    if is_ymd_date(&time.value) {
        return Ok(format!("{}T00:00:00Z", time.value));
    }
    if has_rfc3339_offset(&time.value) {
        return Ok(time.value.clone());
    }
    if time
        .timezone
        .as_deref()
        .is_some_and(|timezone| matches!(timezone, "UTC" | "Etc/UTC" | "Etc/GMT"))
    {
        return Ok(format!("{}Z", time.value));
    }
    Err(caller_input_error(
        AccountOperation::EventsInRange,
        format!("Google Calendar range bound `{field}` requires an RFC 3339 offset"),
    ))
}

fn is_ymd_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn has_rfc3339_offset(value: &str) -> bool {
    if value.ends_with('Z') {
        return true;
    }
    let Some(time_index) = value.find('T') else {
        return false;
    };
    let Some(offset) = value.get(value.len().saturating_sub(6)..) else {
        return false;
    };
    value.len() > time_index + 6
        && (offset.starts_with('+') || offset.starts_with('-'))
        && offset.as_bytes()[3] == b':'
        && offset
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 0 || index == 3 || byte.is_ascii_digit())
}

fn event_time(time: GoogleEventTime) -> EventTime {
    EventTime {
        value: time.date_time.or(time.date).unwrap_or_default(),
        timezone: time.time_zone,
    }
}

// The time a tombstone that carries no time at all projects to. Not a new
// shape on the shared surface: `event_time` already yields an empty value
// for any `GoogleEventTime` carrying neither `date` nor `dateTime`. An
// empty value reads as "the provider did not say", which is the truth for
// a cancelled standalone stub, and is distinguishable from any real
// Google timestamp.
fn unknown_event_time() -> EventTime {
    EventTime {
        value: String::new(),
        timezone: None,
    }
}

fn event_url(base: &str, calendar_id: &str, event_id: &str) -> String {
    format!(
        "{base}/calendars/{}/events/{}",
        bifrost_net::url::encode_path_component(calendar_id),
        bifrost_net::url::encode_path_component(event_id)
    )
}

fn event_move_url(
    base: &str,
    calendar_id: &str,
    event_id: &str,
    target_calendar_id: &str,
) -> String {
    format!(
        "{}/move?destination={}",
        event_url(base, calendar_id, event_id),
        // Query encoder, not the path encoder: the two differ on the complete
        // `.` and `..` components, which the path encoder double-escapes
        // against WHATWG path navigation. A calendar id is a query value here.
        bifrost_net::url::encode_query_value(target_calendar_id)
    )
}

// Record the successful first leg as support context while preserving the
// second leg's classification. Replacing it with PartialResponse would change
// recovery, turning a permanent 403 into reconciliation and discarding a 429
// retry deadline. `event_error` already scopes the PATCH failure to the event
// in its destination calendar. The public diagnostic supplies the continuation:
// retry recovery applies to that destination PATCH, not the original composite
// operation. Passing the destination id with the original patch skips the move.
// Keep identifiers in the typed scope rather than diagnostic text.
fn event_move_patch_error(error: AccountError) -> AccountError {
    error
        .into_builder()
        .text(DiagnosticText::user_safe(
            "The calendar move is already committed, but the field update failed. \
             If recovery permits retrying, call event_update with the destination event id \
             in this error's Calendar scope and the original patch. Do not retry with the \
             original source event id. Retry advice and timing apply to the destination patch.",
        ))
        .text(DiagnosticText::support_only(
            "event move succeeded but the destination field patch failed",
        ))
        .try_build()
        .expect("valid decorated Calendar move error")
}

fn event_patch_has_non_move_fields(patch: &EventPatch) -> bool {
    patch.title.is_some()
        || patch.description.is_some()
        || patch.location.is_some()
        || patch.start.is_some()
        || patch.end.is_some()
        || patch.is_all_day.is_some()
        || patch.availability.is_some()
        || patch.visibility.is_some()
        || patch.attendees.is_some()
        || patch.recurrence.is_some()
}

// Google Calendar carries the timed/all-day distinction in the shape of
// the `start`/`end` objects (`date` vs `dateTime`), not in a standalone
// flag. An `is_all_day` flip can only be expressed when the patch also
// carries the start/end values to re-emit under the new shape. A patch
// that sets `is_all_day` without both bounds would issue a PATCH that
// silently never converts the event, so reject it rather than accept a
// no-op (reject-not-drop).
//
// `Unsupported`, not `Request(Malformed)`: the patch is well-formed shared
// input that other providers apply as it stands (CalDAV re-emits the stored
// DTSTART/DTEND under the new value type, JMAP sets `showWithoutTime`). It is
// this implementation that cannot express it without a read of the current
// bounds, so calling it a caller bug (`ClientBug`) would be false.
fn reject_unexpressible_all_day_patch(patch: &EventPatch) -> Result<(), AccountError> {
    if patch.is_all_day.is_some() && !(patch.start.is_some() && patch.end.is_some()) {
        return Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                AccountOperation::EventUpdate,
                "Google Calendar all-day conversion requires both start and end in the same patch",
            ),
            GmailErrorContext::calendar_collection(AccountOperation::EventUpdate),
        ));
    }
    Ok(())
}

fn join_event_id(calendar_id: &str, event_id: &str) -> String {
    format!("{calendar_id}{EVENT_ID_SEPARATOR}{event_id}")
}

fn split_event_id(
    value: &str,
    operation: AccountOperation,
) -> Result<(String, String), AccountError> {
    value
        .split_once(EVENT_ID_SEPARATOR)
        .map(|(calendar, event)| (calendar.to_string(), event.to_string()))
        .ok_or_else(|| {
            caller_input_error(
                operation,
                "Google event id is missing calendar id".to_string(),
            )
        })
}

fn recurrence_lines(recurrence: &EventRecurrence) -> Option<Vec<String>> {
    let mut lines = Vec::new();
    if let Some(rrule) = &recurrence.rrule {
        lines.push(format!("RRULE:{rrule}"));
    }
    lines.extend(
        recurrence
            .rdate
            .iter()
            .map(|value| format!("RDATE:{value}")),
    );
    lines.extend(
        recurrence
            .exdate
            .iter()
            .map(|value| format!("EXDATE:{value}")),
    );
    (!lines.is_empty()).then_some(lines)
}

fn prefixed_values(values: Option<&[String]>, prefix: &str) -> Vec<String> {
    values
        .unwrap_or_default()
        .iter()
        .filter_map(|value| value.strip_prefix(prefix).map(ToString::to_string))
        .collect()
}

fn event_status(value: Option<&str>) -> EventStatus {
    // An absent status is `confirmed`, the documented Google default, so
    // `Unknown` means only an unrecognized value.
    match value.unwrap_or("confirmed") {
        "confirmed" => EventStatus::Confirmed,
        "tentative" => EventStatus::Tentative,
        "cancelled" => EventStatus::Cancelled,
        _ => EventStatus::Unknown,
    }
}

fn google_event_status(value: EventStatus) -> &'static str {
    match value {
        EventStatus::Tentative => "tentative",
        EventStatus::Cancelled => "cancelled",
        EventStatus::Confirmed | EventStatus::Unknown => "confirmed",
        _ => "confirmed",
    }
}

fn visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default() {
        "private" => EventVisibility::Private,
        "public" => EventVisibility::Public,
        "confidential" => EventVisibility::Confidential,
        _ => EventVisibility::Default,
    }
}

fn availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default() {
        "transparent" => EventAvailability::Free,
        "opaque" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn transparency(value: EventAvailability) -> &'static str {
    match value {
        EventAvailability::Free => "transparent",
        EventAvailability::Busy | EventAvailability::Tentative | EventAvailability::OutOfOffice => {
            "opaque"
        }
        EventAvailability::Unknown => "opaque",
        _ => "opaque",
    }
}

fn rsvp_status(value: Option<&str>) -> RsvpStatus {
    match value.unwrap_or_default() {
        "accepted" => RsvpStatus::Accepted,
        "declined" => RsvpStatus::Declined,
        "tentative" => RsvpStatus::Tentative,
        "needsAction" => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
}

fn attendee_status_value(
    value: RsvpStatus,
    operation: AccountOperation,
) -> Result<&'static str, AccountError> {
    match value {
        RsvpStatus::Accepted => Ok("accepted"),
        RsvpStatus::Declined => Ok("declined"),
        RsvpStatus::Tentative => Ok("tentative"),
        RsvpStatus::NeedsAction => Ok("needsAction"),
        // Google Calendar has no wire value for delegated or future shared
        // states. Refuse before a create, update, or move can write a
        // misleading `needsAction` response on the attendee's behalf.
        _ => Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                operation,
                "Google Calendar cannot write this attendee status",
            ),
            GmailErrorContext::calendar_collection(operation),
        )),
    }
}

fn rsvp_request_value(value: RsvpStatus) -> Result<&'static str, AccountError> {
    attendee_status_value(value, AccountOperation::EventRsvp)
}

fn rsvp_google_attendees_for_self(
    attendees: &mut Option<Vec<GoogleAttendee>>,
    self_email: &str,
    response_status: &str,
) -> Result<(), AccountError> {
    let Some(attendees) = attendees.as_mut() else {
        return Err(rsvp_unsupported(
            "Google Calendar event does not contain the account attendee",
        ));
    };
    // Google marks the authenticated account's attendee with `self: true`.
    // That identity is authoritative when the account uses an alias or a
    // secondary/delegated calendar whose attendee email differs from profile.
    let index = attendees
        .iter()
        .position(|attendee| attendee.extra.get("self").and_then(Value::as_bool) == Some(true))
        .or_else(|| {
            attendees.iter().position(|attendee| {
                attendee
                    .email
                    .as_deref()
                    .is_some_and(|email| email.eq_ignore_ascii_case(self_email))
            })
        });
    let Some(attendee) = index.and_then(|index| attendees.get_mut(index)) else {
        // The event state has no attendee for this account. This is an
        // unsupported RSVP target, not malformed caller input.
        return Err(rsvp_unsupported(
            "Google Calendar event does not contain the account attendee",
        ));
    };
    attendee.response_status = Some(response_status.to_string());
    Ok(())
}

fn rsvp_unsupported(detail: impl Into<String>) -> AccountError {
    error::into_account_error(
        crate::error::Error::unsupported_with(AccountOperation::EventRsvp, detail),
        GmailErrorContext::calendar_collection(AccountOperation::EventRsvp),
    )
}

fn collection_error(error: crate::Error, operation: AccountOperation) -> AccountError {
    error::into_account_error(error, GmailErrorContext::calendar_collection(operation))
}

fn event_error(error: crate::Error, operation: AccountOperation, id: String) -> AccountError {
    error::into_account_error(error, GmailErrorContext::calendar_event(operation, id))
}

/// The CALLER's input cannot be used - an event id, page cursor or range
/// bound this crate cannot express. (A cursor that was valid when minted but
/// names a calendar since removed is state that moved, not this.) Refused
/// before the request it would shape is sent, so never a provider fault:
/// `Request(Malformed)`, per the local-refusal rule in `reference/error-model.md`.
fn caller_input_error(operation: AccountOperation, message: String) -> AccountError {
    error::into_account_error(
        crate::error::Error::invalid_request(operation, message),
        GmailErrorContext::calendar_collection(operation),
    )
}

/// A pagination walk refused to follow its next token: a repeated token is
/// the provider's contract breach, the page budget is this crate's own limit.
fn page_refusal(operation: AccountOperation, refusal: crate::error::PageRefusal) -> AccountError {
    error::into_account_error(
        crate::error::Error::from(refusal),
        GmailErrorContext::calendar_collection(operation),
    )
}

/// A complete response Google sent lacks `field`, which it must carry (a
/// created event with no id, a live event with no start or end): a provider
/// fault, `Protocol(MissingField)` with `AttemptCause(Acknowledged)`. Only for
/// use after the response arrived.
fn local_error_with_field(
    operation: AccountOperation,
    field: &'static str,
    message: String,
) -> AccountError {
    error::into_account_error(
        crate::error::Error::missing_field(field, message),
        GmailErrorContext::calendar_collection(operation),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CalendarListResponse {
    items: Option<Vec<GoogleCalendarListEntry>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleCalendarListEntry {
    id: Option<String>,
    summary: Option<String>,
    background_color: Option<String>,
    primary: Option<bool>,
    access_role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventsResponse {
    items: Option<Vec<GoogleEvent>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GoogleEvent {
    id: Option<String>,
    etag: Option<String>,
    i_cal_uid: Option<String>,
    summary: Option<String>,
    description: Option<String>,
    location: Option<String>,
    start: Option<GoogleEventTime>,
    end: Option<GoogleEventTime>,
    /// True when `end` is a placeholder, not a real end time.
    end_time_unspecified: Option<bool>,
    status: Option<String>,
    visibility: Option<String>,
    organizer: Option<GoogleOrganizer>,
    attendees: Option<Vec<GoogleAttendee>>,
    recurrence: Option<Vec<String>>,
    original_start_time: Option<GoogleEventTime>,
    transparency: Option<String>,
    html_link: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GoogleEventPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<GoogleEventTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<GoogleEventTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attendees: Option<Vec<GoogleAttendee>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recurrence: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transparency: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleEventTime {
    #[serde(skip_serializing_if = "Option::is_none")]
    date: Option<String>,
    #[serde(rename = "dateTime", skip_serializing_if = "Option::is_none")]
    date_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_zone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleOrganizer {
    email: Option<String>,
    display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleAttendee {
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    optional: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource: Option<bool>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bifrost_types::RecoveryClass;
    use bytes::Bytes;
    use reqwest::StatusCode;

    use super::*;

    /// Caller input Google Calendar cannot express is refused before any
    /// request, as the caller's fault - never `Protocol(MissingField)`, which
    /// used to blame Google for an id or cursor it never saw.
    #[test]
    fn unusable_caller_input_is_a_client_bug_not_a_provider_fault() {
        let assert_client_bug = |error: AccountError| {
            assert!(
                matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Request(
                        bifrost_types::RequestErrorKind::Malformed
                    )
                ),
                "got {:?}",
                error.kind()
            );
            assert!(matches!(error.recovery(), RecoveryClass::ClientBug));
        };

        assert_client_bug(
            split_event_id("no-separator", AccountOperation::EventGet)
                .expect_err("an event id without a calendar id is refused"),
        );
        assert_client_bug(
            google_range_bound(
                &EventTime {
                    value: "2026-09-29T10:00:00".into(),
                    timezone: Some("Europe/Oslo".into()),
                },
                "timeMin",
            )
            .expect_err("a floating bound in a non-UTC zone is refused"),
        );
        assert_client_bug(
            decode_page_token(Some(vec![0xff]), AccountOperation::EventSearch)
                .expect_err("a non-UTF-8 cursor is refused"),
        );

        // A cursor this crate minted, naming a calendar removed between pages:
        // state that moved, so a conflict whose refresh is restarting the
        // search - neither the caller's fault nor the provider's.
        let stale = decode_cross_calendar_cursor(Some(b"primary\ntoken".to_vec()), &[])
            .expect_err("a cursor naming an unlisted calendar is refused");
        assert_eq!(
            *stale.kind(),
            bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
        assert!(matches!(
            stale.recovery(),
            RecoveryClass::Retry(advice)
                if advice.disposition == bifrost_types::RetryDisposition::AfterStateRefresh
        ));
    }

    fn canned_json(status: StatusCode, value: serde_json::Value) -> Canned {
        Canned::Response {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&value).expect("fixture serializes")),
        }
    }

    fn scripted_client(steps: Vec<Canned>) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
        let script = ScriptedDispatch::new(steps);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        (
            Arc::new(GmailClient::with_account_net("https://gmail.test", net)),
            script,
        )
    }

    fn timed(value: &str) -> GoogleEventTime {
        GoogleEventTime {
            date: None,
            date_time: Some(value.to_string()),
            time_zone: None,
        }
    }

    #[test]
    fn google_event_maps_transparency_and_original_start_time() {
        let event = event_from_google(
            "primary".to_string(),
            GoogleEvent {
                id: Some("e1".to_string()),
                start: Some(timed("2026-06-02T12:00:00Z")),
                end: Some(timed("2026-06-02T13:00:00Z")),
                transparency: Some("transparent".to_string()),
                original_start_time: Some(timed("2026-06-01T12:00:00Z")),
                ..GoogleEvent::default()
            },
            AccountOperation::EventGet,
        )
        .expect("valid event");

        assert_eq!(event.availability, EventAvailability::Free);
        assert_eq!(
            event.recurrence.recurrence_id.as_deref(),
            Some("2026-06-01T12:00:00Z")
        );
    }

    #[test]
    fn google_event_rejects_missing_times() {
        let error = event_from_google(
            "primary".to_string(),
            GoogleEvent {
                id: Some("e1".to_string()),
                end: Some(timed("2026-06-02T13:00:00Z")),
                ..GoogleEvent::default()
            },
            AccountOperation::EventGet,
        )
        .expect_err("missing start should fail");

        assert_eq!(error.operation(), Some(AccountOperation::EventGet));
    }

    #[tokio::test]
    async fn events_in_range_requests_and_surfaces_cancelled_instances() {
        let (client, script) = scripted_client(vec![canned_json(
            StatusCode::OK,
            json!({
                "items": [{
                    "id": "cancelled-instance",
                    "status": "cancelled",
                    "originalStartTime": {"dateTime": "2026-06-02T12:00:00Z"}
                }]
            }),
        )]);
        let range = EventRange {
            calendar_id: CalendarId("primary".to_string()),
            start: bifrost_types::EventTime {
                value: "2026-06-01T00:00:00Z".to_string(),
                timezone: None,
            },
            end: bifrost_types::EventTime {
                value: "2026-06-03T00:00:00Z".to_string(),
                timezone: None,
            },
            limit: None,
            page_cursor: None,
        };

        let page = events_in_range(client, range).await.expect("range loads");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].status, EventStatus::Cancelled);
        assert!(
            !page.items[0].is_all_day,
            "a dateTime tombstone is a timed instance",
        );
        assert!(script.requests()[0].url.query().is_some_and(|query| {
            query.contains("singleEvents=true") && query.contains("showDeleted=true")
        }));
    }

    /// A cancelled instance of an ALL-DAY recurrence carries no `start`
    /// and only `originalStartTime.date`. Reading all-day-ness off
    /// `event.start` alone left `is_all_day: false` beside date-valued
    /// start and end fields, which the shared surface reads as a timed
    /// event at midnight.
    #[tokio::test]
    async fn a_cancelled_all_day_instance_stays_all_day() {
        let (client, _script) = scripted_client(vec![canned_json(
            StatusCode::OK,
            json!({
                "items": [{
                    "id": "cancelled-all-day",
                    "status": "cancelled",
                    "originalStartTime": {"date": "2026-06-02"}
                }]
            }),
        )]);
        let range = EventRange {
            calendar_id: CalendarId("primary".to_string()),
            start: bifrost_types::EventTime {
                value: "2026-06-01T00:00:00Z".to_string(),
                timezone: None,
            },
            end: bifrost_types::EventTime {
                value: "2026-06-03T00:00:00Z".to_string(),
                timezone: None,
            },
            limit: None,
            page_cursor: None,
        };

        let page = events_in_range(client, range).await.expect("range loads");

        let event = &page.items[0];
        assert_eq!(event.status, EventStatus::Cancelled);
        assert_eq!(event.start.value, "2026-06-02");
        assert_eq!(
            event.end.as_ref().map(|end| end.value.as_str()),
            Some("2026-06-02")
        );
        assert!(
            event.is_all_day,
            "a date-valued tombstone must project as an all-day instance",
        );
    }

    fn primary_range() -> EventRange {
        EventRange {
            calendar_id: CalendarId("primary".to_string()),
            start: EventTime {
                value: "2026-06-01T00:00:00Z".to_string(),
                timezone: None,
            },
            end: EventTime {
                value: "2026-06-03T00:00:00Z".to_string(),
                timezone: None,
            },
            limit: None,
            page_cursor: None,
        }
    }

    /// A cancelled STANDALONE event is the shape `showDeleted=true` returns
    /// for an ordinary deletion: an id, `status: "cancelled"`, and no times
    /// whatsoever - not even `originalStartTime`, which only a cancelled
    /// INSTANCE of a recurrence carries. It is a tombstone, not a malformed
    /// event, so it must cross the surface carrying its id and its cancelled
    /// status rather than being refused.
    #[test]
    fn a_cancelled_standalone_stub_projects_as_a_tombstone() {
        let event = event_from_google(
            "primary".to_string(),
            GoogleEvent {
                id: Some("deleted-standalone".to_string()),
                status: Some("cancelled".to_string()),
                ..GoogleEvent::default()
            },
            AccountOperation::EventsInRange,
        )
        .expect("a cancelled stub is a deletion notice, not a projection failure");

        assert_eq!(event.id.0, "primary::deleted-standalone");
        assert_eq!(event.status, EventStatus::Cancelled);
        assert_eq!(event.start.value, "");
        assert_eq!(event.end, None);
        assert!(
            !event.is_all_day,
            "a stub with no times says nothing about all-day-ness",
        );
    }

    /// `endTimeUnspecified: true` means Google's `end` is a placeholder, so
    /// it projects as no end rather than the fabricated value. Fails if the
    /// `end_time_unspecified` arm of the `end` match is removed.
    #[test]
    fn an_unspecified_end_does_not_project_the_placeholder() {
        let event = event_from_google(
            "primary".to_string(),
            serde_json::from_value(json!({
                "id": "open-ended",
                "status": "confirmed",
                "start": {"dateTime": "2026-06-02T12:00:00Z", "timeZone": "UTC"},
                "end": {"dateTime": "2026-06-02T12:00:00Z", "timeZone": "UTC"},
                "endTimeUnspecified": true
            }))
            .expect("fixture deserializes"),
            AccountOperation::EventGet,
        )
        .expect("an unspecified end is a valid event");

        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(event.end, None);
        assert!(!event.is_all_day);
    }

    /// The flag also wins over a missing `end`: a live event whose end is
    /// unspecified is not a contract break, so it must not hit the
    /// missing-end refusal. Fails if the arm is moved below the
    /// `None => Err` arm or the flag stops being deserialized.
    #[test]
    fn an_unspecified_end_with_no_end_object_is_not_refused() {
        let event = event_from_google(
            "primary".to_string(),
            serde_json::from_value(json!({
                "id": "open-ended-bare",
                "status": "confirmed",
                "start": {"date": "2026-06-02"},
                "endTimeUnspecified": true
            }))
            .expect("fixture deserializes"),
            AccountOperation::EventGet,
        )
        .expect("no end plus endTimeUnspecified is valid");

        assert_eq!(event.end, None);
        assert!(event.is_all_day);
    }

    /// `endTimeUnspecified: false` (or absent) leaves a real end alone, so
    /// the fix cannot swallow ordinary ends. Fails if the arm matches on
    /// anything but `Some(true)`.
    #[test]
    fn a_specified_end_is_still_projected() {
        let event = event_from_google(
            "primary".to_string(),
            serde_json::from_value(json!({
                "id": "closed",
                "status": "confirmed",
                "start": {"dateTime": "2026-06-02T12:00:00Z"},
                "end": {"dateTime": "2026-06-02T13:00:00Z"},
                "endTimeUnspecified": false
            }))
            .expect("fixture deserializes"),
            AccountOperation::EventGet,
        )
        .expect("valid event");

        assert_eq!(event.end.expect("end").value, "2026-06-02T13:00:00Z");
    }

    /// The tolerance is gated on the cancelled status, not on the mere
    /// absence of a start: a live event the provider sent without times is
    /// a contract break and stays an error.
    #[test]
    fn a_live_event_missing_start_is_still_refused() {
        let error = event_from_google(
            "primary".to_string(),
            GoogleEvent {
                id: Some("live".to_string()),
                status: Some("confirmed".to_string()),
                original_start_time: Some(timed("2026-06-02T12:00:00Z")),
                ..GoogleEvent::default()
            },
            AccountOperation::EventsInRange,
        )
        .expect_err("a confirmed event with no start is malformed");

        assert_eq!(error.operation(), Some(AccountOperation::EventsInRange));
    }

    /// One unprojectable item used to fail the ENTIRE page, because the
    /// projection collected through `Result`. A deleted standalone event
    /// is reachable on this exact path (`showDeleted=true`), so a single
    /// ordinary deletion could take a whole range page with it.
    #[tokio::test]
    async fn a_deleted_standalone_event_does_not_poison_the_range_page() {
        let (client, _script) = scripted_client(vec![canned_json(
            StatusCode::OK,
            json!({
                "items": [
                    {
                        "id": "deleted-standalone",
                        "status": "cancelled",
                        "etag": "\"tomb\""
                    },
                    {
                        "id": "live",
                        "status": "confirmed",
                        "summary": "Planning",
                        "start": {"dateTime": "2026-06-02T12:00:00Z"},
                        "end": {"dateTime": "2026-06-02T13:00:00Z"}
                    }
                ]
            }),
        )]);

        let page = events_in_range(client, primary_range())
            .await
            .expect("a deletion must not fail the page");

        assert_eq!(
            page.items
                .iter()
                .map(|event| event.native_id.as_str())
                .collect::<Vec<_>>(),
            ["primary::deleted-standalone", "primary::live"]
        );
        assert_eq!(page.items[0].status, EventStatus::Cancelled);
        assert!(
            page.failed_ids.is_empty(),
            "a tombstone is not a failure to report",
        );
    }

    /// The boundary, pinned in one page: a cancelled instance and a
    /// cancelled standalone stub are tolerated as tombstones, a malformed
    /// LIVE event is still a failure - but a per-item one, named on
    /// `failed_ids` and absent from `items`, rather than one that destroys
    /// every neighbour Google already returned.
    #[tokio::test]
    async fn a_malformed_live_event_rides_failed_ids_without_failing_the_page() {
        let (client, _script) = scripted_client(vec![canned_json(
            StatusCode::OK,
            json!({
                "items": [
                    {"id": "broken", "status": "confirmed", "summary": "No times"},
                    {
                        "id": "cancelled-instance",
                        "status": "cancelled",
                        "originalStartTime": {"dateTime": "2026-06-02T12:00:00Z"}
                    },
                    {"id": "bare-tombstone", "status": "cancelled"},
                    {
                        "id": "live",
                        "start": {"dateTime": "2026-06-02T14:00:00Z"},
                        "end": {"dateTime": "2026-06-02T15:00:00Z"}
                    }
                ],
                "nextPageToken": "page-2"
            }),
        )]);

        let page = events_in_range(client, primary_range())
            .await
            .expect("one bad item must not fail the page");

        assert_eq!(
            page.items
                .iter()
                .map(|event| event.native_id.as_str())
                .collect::<Vec<_>>(),
            [
                "primary::cancelled-instance",
                "primary::bare-tombstone",
                "primary::live"
            ]
        );
        // The cancelled instance keeps projecting off `originalStartTime`
        // for both bounds, which is what makes the stub case a widening
        // rather than a replacement.
        assert_eq!(page.items[0].start.value, "2026-06-02T12:00:00Z");
        assert_eq!(
            page.items[0].end.as_ref().map(|end| end.value.as_str()),
            Some("2026-06-02T12:00:00Z")
        );
        assert_eq!(page.failed_ids, vec!["primary::broken".to_string()]);
        assert_eq!(page.next_cursor.as_deref(), Some(b"page-2".as_slice()));
    }

    /// Cross-calendar search assembles its own aggregate `Page`, so the
    /// per-calendar `failed_ids` have to be carried across the join. A
    /// literal `Vec::new()` there would reinstate the silent drop one layer
    /// above the fix.
    #[tokio::test]
    async fn cross_calendar_search_carries_per_calendar_failed_ids() {
        let (client, _script) = scripted_client(vec![
            canned_json(
                StatusCode::OK,
                json!({"items": [{"id": "primary"}, {"id": "work"}]}),
            ),
            canned_json(
                StatusCode::OK,
                json!({"items": [{"id": "broken-a", "summary": "No times"}]}),
            ),
            canned_json(
                StatusCode::OK,
                json!({
                    "items": [
                        {"id": "broken-b", "summary": "No times"},
                        {
                            "id": "good",
                            "start": {"dateTime": "2026-06-02T12:00:00Z"},
                            "end": {"dateTime": "2026-06-02T13:00:00Z"}
                        }
                    ]
                }),
            ),
        ]);

        let page = search(
            client,
            EventSearchRequest {
                calendar_id: None,
                query: "planning".to_string(),
                limit: None,
                page_cursor: None,
                include_cancelled: false,
            },
        )
        .await
        .expect("a bad item in one calendar must not fail the whole search");

        assert_eq!(
            page.items
                .iter()
                .map(|event| event.native_id.as_str())
                .collect::<Vec<_>>(),
            ["work::good"]
        );
        assert_eq!(
            page.failed_ids,
            vec![
                "primary::broken-a".to_string(),
                "work::broken-b".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn search_omits_show_deleted_by_default() {
        let (client, script) =
            scripted_client(vec![canned_json(StatusCode::OK, json!({"items": []}))]);
        let mut request = EventSearchRequest::new("planning");
        request.calendar_id = Some(CalendarId("primary".to_string()));

        search(client, request).await.expect("search loads");

        assert!(
            script.requests()[0]
                .url
                .query()
                .is_some_and(|query| !query.contains("showDeleted")),
            "a default search must not ask for tombstones",
        );
    }

    #[tokio::test]
    async fn search_sends_show_deleted_when_include_cancelled() {
        let (client, script) = scripted_client(vec![canned_json(
            StatusCode::OK,
            json!({
                "items": [{"id": "gone", "status": "cancelled"}]
            }),
        )]);
        let mut request = EventSearchRequest::new("planning");
        request.calendar_id = Some(CalendarId("primary".to_string()));
        request.include_cancelled = true;

        let page = search(client, request).await.expect("search loads");

        assert!(
            script.requests()[0]
                .url
                .query()
                .is_some_and(|query| query.contains("showDeleted=true")),
        );
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].status, EventStatus::Cancelled);
        assert!(page.failed_ids.is_empty());
    }

    #[tokio::test]
    async fn cross_calendar_search_sends_show_deleted_to_every_calendar() {
        let (client, script) = scripted_client(vec![
            canned_json(
                StatusCode::OK,
                json!({"items": [{"id": "primary"}, {"id": "work"}]}),
            ),
            canned_json(StatusCode::OK, json!({"items": []})),
            canned_json(StatusCode::OK, json!({"items": []})),
        ]);
        let mut request = EventSearchRequest::new("planning");
        request.include_cancelled = true;

        search(client, request).await.expect("search loads");

        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        for request in &requests[1..] {
            assert!(
                request
                    .url
                    .query()
                    .is_some_and(|query| query.contains("showDeleted=true")),
            );
        }
    }

    #[test]
    fn event_status_absent_is_confirmed_and_unrecognized_is_unknown() {
        assert_eq!(event_status(None), EventStatus::Confirmed);
        assert_eq!(event_status(Some("confirmed")), EventStatus::Confirmed);
        assert_eq!(event_status(Some("cancelled")), EventStatus::Cancelled);
        assert_eq!(event_status(Some("x-weird")), EventStatus::Unknown);
    }

    #[test]
    fn cross_calendar_search_cursor_round_trips_calendar_and_token() {
        let calendars = vec!["primary".to_string(), "work".to_string()];
        let cursor = encode_cross_calendar_cursor("work", b"next-token");

        let (index, token) =
            decode_cross_calendar_cursor(Some(cursor), &calendars).expect("cursor");

        assert_eq!(index, 1);
        assert_eq!(token.as_deref(), Some("next-token"));
    }

    #[test]
    fn cross_calendar_search_cursor_can_resume_at_calendar_boundary() {
        let calendars = vec!["primary".to_string(), "work".to_string()];
        let cursor = encode_cross_calendar_cursor("work", b"");

        let (index, token) =
            decode_cross_calendar_cursor(Some(cursor), &calendars).expect("cursor");

        assert_eq!(index, 1);
        assert_eq!(token, None);
    }

    #[tokio::test]
    async fn calendars_list_walks_every_page_and_encodes_page_tokens() {
        let (client, script) = scripted_client(vec![
            canned_json(
                StatusCode::OK,
                json!({
                    "items": [{"id": "first", "summary": "First"}],
                    "nextPageToken": "next+page&owner=me"
                }),
            ),
            canned_json(
                StatusCode::OK,
                json!({"items": [{"id": "second", "summary": "Second"}]}),
            ),
        ]);

        let calendars = calendars_list(client).await.expect("all pages load");

        assert_eq!(
            calendars
                .iter()
                .map(|calendar| calendar.id.0.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .url
                .as_str()
                .ends_with("calendarList?maxResults=250")
        );
        assert!(
            requests[1]
                .url
                .as_str()
                .ends_with("calendarList?maxResults=250&pageToken=next%2Bpage%26owner%3Dme")
        );
    }

    #[tokio::test]
    async fn calendars_list_rejects_a_repeated_page_token() {
        let (client, script) = scripted_client(vec![
            canned_json(StatusCode::OK, json!({"nextPageToken": "stalled"})),
            canned_json(StatusCode::OK, json!({"nextPageToken": "stalled"})),
        ]);

        let error = calendars_list(client)
            .await
            .expect_err("repeated token must terminate");

        assert_eq!(error.operation(), Some(AccountOperation::CalendarsList));
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        ));
        assert_eq!(script.requests().len(), 2);
    }

    /// A server handing out a fresh token every page slips past the
    /// repeated-token guard, so the page budget is the only thing left
    /// stopping an unbounded walk. Observe it firing rather than
    /// asserting the constant exists.
    #[tokio::test]
    async fn calendars_list_stops_at_the_page_budget() {
        let steps = (0..MAX_CALENDAR_LIST_PAGES)
            .map(|page| canned_json(StatusCode::OK, json!({"nextPageToken": format!("p{page}")})))
            .collect();
        let (client, script) = scripted_client(steps);

        let error = calendars_list(client)
            .await
            .expect_err("the page budget must terminate the walk");

        // The budget is this crate's own limit (an honestly huge calendar
        // list looks the same), so it is `Internal(LimitExceeded)`, not the
        // provider contract violation the repeated-token arm above is.
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(bifrost_types::InternalErrorKind::LimitExceeded)
        );
        assert_eq!(script.requests().len(), MAX_CALENDAR_LIST_PAGES);
    }

    #[test]
    fn event_move_url_encodes_destination_calendar() {
        // The path and query encoders differ on exactly two inputs: the
        // complete `.` and `..` components, which the path encoder
        // double-escapes so the WHATWG parser cannot resolve them as
        // navigation. `destination` is a query value, where dots carry
        // no structural meaning, so both must survive verbatim.
        let base = "https://www.googleapis.com/calendar/v3";
        let url = event_move_url(base, "primary", "event/1", ".");
        assert!(url.contains("/calendars/primary/events/event%2F1/move"));
        assert!(url.ends_with("destination=."));

        let parent = event_move_url(base, "primary", "event/1", "..");
        assert!(parent.ends_with("destination=.."));

        // Everything else encodes identically under both, so a value
        // carrying query delimiters still pins that the destination is
        // escaped at all.
        let delimited = event_move_url(base, "primary", "event/1", "a&b=c+d");
        assert!(delimited.ends_with("destination=a%26b%3Dc%2Bd"));
    }

    #[tokio::test]
    async fn failed_patch_after_move_keeps_its_classification_and_records_the_move() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::HeaderName::from_static("x-goog-request-id"),
            reqwest::header::HeaderValue::from_static("req-77"),
        );
        let denied_body = Bytes::from(
            serde_json::to_vec(&json!({
                "error": {
                    "code": 403,
                    "message": "denied",
                    "errors": [{"reason": "forbidden", "message": "denied"}]
                }
            }))
            .expect("fixture serializes"),
        );
        let denied = || Canned::Response {
            status: StatusCode::FORBIDDEN,
            headers: headers.clone(),
            body: denied_body.clone(),
        };
        let (client, script) = scripted_client(vec![
            canned_json(StatusCode::OK, json!({"id": "event-1"})),
            denied(),
        ]);
        let (direct_client, direct_script) = scripted_client(vec![denied()]);
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            title: Some(Some("Updated".to_string())),
            ..EventPatch::default()
        };
        let direct_patch = EventPatch {
            title: Some(Some("Updated".to_string())),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("destination patch fails");
        let direct_error = update(
            direct_client,
            EventId("destination::event-1".to_string()),
            direct_patch,
        )
        .await
        .expect_err("the same patch fails without a move");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
        ));
        assert_eq!(error.kind(), direct_error.kind());
        assert_eq!(error.recovery(), direct_error.recovery());
        assert_eq!(error.message_key(), direct_error.message_key());
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            Cause::Attempt(attempt)
                if attempt.transmission_state == TransmissionState::Acknowledged
        )));

        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Calendar {
                id: ("destination::event-1".to_string()).into()
            })
        );
        assert_eq!(error.scope(), direct_error.scope());

        let telemetry = error.telemetry_fields();
        assert_eq!(telemetry.status, Some(403));
        assert_eq!(telemetry.request_id, Some("req-77"));
        assert_eq!(telemetry.provider, Some(bifrost_types::Provider::Gmail));
        assert_eq!(telemetry.protocol, Some(Protocol::Gmail));
        assert!(telemetry.native_code.is_some());
        assert!(
            error
                .support_consented()
                .support_text
                .iter()
                .any(|text| text.contains("denied")),
            "the provider's own diagnostic text must survive decoration"
        );
        assert!(
            error
                .support_consented()
                .support_text
                .iter()
                .any(|text| text.contains("event move succeeded")),
            "support diagnostics record the completed first leg"
        );

        assert!(
            error
                .chain()
                .iter()
                .any(|cause| matches!(cause, Cause::Wire(WireCause::Gmail(_))))
        );

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(requests[1].method, reqwest::Method::PATCH);
        assert_eq!(direct_script.requests().len(), 1);
    }

    #[tokio::test]
    async fn rate_limit_after_move_continues_at_destination_without_repeating_move() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("42"),
        );
        let rate_limited = Canned::Response {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers,
            body: Bytes::from(
                serde_json::to_vec(&json!({
                    "error": {
                        "code": 429,
                        "message": "rate limited",
                        "errors": [{"reason": "rateLimitExceeded", "message": "slow down"}]
                    }
                }))
                .expect("fixture serializes"),
            ),
        };
        let (client, script) = scripted_client(vec![
            canned_json(StatusCode::OK, json!({"id": "event-1"})),
            rate_limited,
            canned_json(StatusCode::OK, json!({"id": "event-1"})),
        ]);
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            title: Some(Some("Updated".to_string())),
            ..EventPatch::default()
        };

        let error = update(
            std::sync::Arc::clone(&client),
            EventId("source::event-1".to_string()),
            patch.clone(),
        )
        .await
        .expect_err("destination patch is rate limited");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Server(bifrost_types::ServerErrorKind::RateLimited)
        ));
        let RecoveryClass::Retry(advice) = error.recovery() else {
            panic!("the failed second leg retains retry recovery");
        };
        assert!(matches!(
            advice.retry_hint,
            Some(bifrost_types::RetryHint::At(_))
        ));
        assert!(
            error
                .support_consented()
                .support_text
                .iter()
                .any(|text| text.contains("event move succeeded")),
            "the move context is added without replacing the retry hint"
        );
        assert_eq!(
            advice.disposition,
            bifrost_types::RetryDisposition::SameRequest
        );
        assert!(error.user_safe_text().any(|text| {
            text.contains("already committed")
                && text.contains("destination event id")
                && text.contains("Do not retry")
        }));
        let Some(bifrost_types::ErrorScope::Calendar { id }) = error.scope() else {
            panic!("continuation must name the destination event");
        };
        assert_eq!(id.0, "destination::event-1");
        // Follow the public continuation with the ORIGINAL patch, including
        // calendar_id. This must send only PATCH, never another events.move.
        update(client, EventId(id.0.clone()), patch)
            .await
            .expect("destination continuation succeeds");
        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(requests[1].method, reqwest::Method::PATCH);
        assert_eq!(requests[2].method, reqwest::Method::PATCH);
        assert_eq!(requests[1].url, requests[2].url);
        assert_eq!(requests[1].body, requests[2].body);
    }

    #[tokio::test]
    async fn auth_failure_after_move_keeps_auth_recovery_and_public_continuation() {
        let (client, script) = scripted_client(vec![
            canned_json(StatusCode::OK, json!({"id": "event-1"})),
            canned_json(
                StatusCode::UNAUTHORIZED,
                json!({"error": {"code": 401, "message": "authentication required"}}),
            ),
            canned_json(
                StatusCode::UNAUTHORIZED,
                json!({"error": {"code": 401, "message": "authentication required"}}),
            ),
        ]);
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            title: Some(Some("Updated".to_string())),
            ..EventPatch::default()
        };
        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("destination patch requires authentication");
        assert!(
            matches!(error.recovery(), RecoveryClass::AuthLost),
            "expected terminal auth recovery, got {:?} for {:?} with {:?}",
            error.recovery(),
            error.kind(),
            error.chain()
        );
        assert!(error.user_safe_text().any(|text| {
            text.contains("already committed") && text.contains("destination event id")
        }));
        assert_eq!(script.requests().len(), 3);
    }

    /// Local patch validation is purely a function of the patch, so it
    /// can run before the move. Doing so keeps a patch that could never
    /// be valid from stranding the event in a new calendar: the cheap
    /// prevention, as opposed to the `TransmissionState` evidence that
    /// covers second-leg failures which genuinely cannot be prevented.
    #[tokio::test]
    async fn an_unexpressible_patch_is_rejected_before_the_move() {
        let (client, script) = scripted_client(Vec::new());
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            is_all_day: Some(true),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("an all-day flip with no bounds is unexpressible");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));
        assert!(
            script.requests().is_empty(),
            "no move may be issued for a patch that can never be applied"
        );
    }

    #[tokio::test]
    async fn failed_patch_without_a_move_keeps_its_own_classification() {
        let (client, script) = scripted_client(vec![canned_json(
            StatusCode::FORBIDDEN,
            json!({"error": {"code": 403, "message": "denied"}}),
        )]);
        let patch = EventPatch {
            title: Some(Some("Updated".to_string())),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("patch fails");

        assert!(
            !matches!(
                error.kind(),
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
            ),
            "a patch that was never preceded by a move is not a partial write"
        );
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Calendar {
                id: ("source::event-1".to_string()).into()
            })
        );
        assert_eq!(script.requests().len(), 1);
    }

    #[test]
    fn event_patch_detects_non_move_fields() {
        assert!(!event_patch_has_non_move_fields(&EventPatch {
            calendar_id: Some(CalendarId("work".to_string())),
            ..EventPatch::default()
        }));
        assert!(event_patch_has_non_move_fields(&EventPatch {
            title: Some(Some("Planning".to_string())),
            ..EventPatch::default()
        }));
    }

    #[test]
    fn google_attendee_maps_resource_role() {
        let attendee = attendee_from_google(GoogleAttendee {
            email: Some("room@example.test".to_string()),
            display_name: None,
            response_status: Some("accepted".to_string()),
            optional: Some(false),
            resource: Some(true),
            extra: Map::new(),
        })
        .expect("attendee");

        assert_eq!(attendee.role, AttendeeRole::Resource);
        assert_eq!(attendee.status, RsvpStatus::Accepted);

        let google = google_attendee(
            &EventAttendee {
                email: "room@example.test".to_string(),
                name: None,
                role: AttendeeRole::Resource,
                status: RsvpStatus::Accepted,
            },
            AccountOperation::EventCreate,
        )
        .expect("accepted is a representable attendee status");
        assert_eq!(google.resource, Some(true));
        assert_eq!(google.optional, Some(false));
    }

    #[test]
    fn rsvp_updates_matching_google_attendee_without_dropping_extra_fields() {
        let mut extra = Map::new();
        extra.insert("comment".to_string(), serde_json::json!("bring notes"));
        extra.insert("self".to_string(), serde_json::json!(true));
        let mut attendees = Some(vec![
            GoogleAttendee {
                email: Some("other@example.test".to_string()),
                display_name: None,
                response_status: Some("needsAction".to_string()),
                optional: None,
                resource: None,
                extra: Map::new(),
            },
            GoogleAttendee {
                email: Some("account-alias@example.test".to_string()),
                display_name: None,
                response_status: Some("needsAction".to_string()),
                optional: None,
                resource: None,
                extra,
            },
        ]);

        rsvp_google_attendees_for_self(&mut attendees, "self@example.test", "accepted")
            .expect("matching attendee");
        let attendees = attendees.expect("attendees");
        assert_eq!(attendees[0].response_status.as_deref(), Some("needsAction"));
        assert_eq!(attendees[1].response_status.as_deref(), Some("accepted"));
        assert_eq!(attendees[1].extra["comment"].as_str(), Some("bring notes"));
        assert_eq!(attendees[1].extra["self"].as_bool(), Some(true));

        assert!(
            rsvp_google_attendees_for_self(&mut None, "self@example.test", "accepted").is_err()
        );
    }

    #[tokio::test]
    async fn unsupported_rsvp_statuses_are_refused_before_any_request() {
        for status in [RsvpStatus::Delegated, RsvpStatus::Unknown] {
            let (client, script) = scripted_client(Vec::new());
            let error = rsvp(
                client,
                "self@example.test".to_string(),
                EventId(join_event_id("primary", "event-1")),
                status,
            )
            .await
            .expect_err("Google cannot represent this RSVP status");

            assert!(matches!(
                error.kind(),
                AccountErrorKind::Unsupported(AccountOperation::EventRsvp)
            ));
            assert!(script.requests().is_empty());
        }
    }

    #[tokio::test]
    async fn unrepresentable_attendee_statuses_are_refused_before_create_or_move() {
        for status in [RsvpStatus::Delegated, RsvpStatus::Unknown] {
            let (client, script) = scripted_client(Vec::new());
            let mut event = create_with("2026-06-02T12:00:00Z", "2026-06-02T13:00:00Z");
            event.attendees.push(EventAttendee {
                email: "person@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status,
            });
            let error = create(client, event)
                .await
                .expect_err("Google cannot represent this attendee status");
            assert!(matches!(
                error.kind(),
                AccountErrorKind::Unsupported(AccountOperation::EventCreate)
            ));
            assert!(script.requests().is_empty());

            let (client, script) = scripted_client(Vec::new());
            let patch = EventPatch {
                calendar_id: Some(CalendarId("destination".to_string())),
                attendees: Some(vec![EventAttendee {
                    email: "person@example.test".to_string(),
                    name: None,
                    role: AttendeeRole::Required,
                    status,
                }]),
                ..EventPatch::default()
            };
            let error = update(client, EventId("source::event-1".to_string()), patch)
                .await
                .expect_err("Google cannot represent this attendee status");
            assert!(matches!(
                error.kind(),
                AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
            ));
            assert!(
                script.requests().is_empty(),
                "unsupported attendee state must be refused before moving the event"
            );
        }
    }

    #[test]
    fn an_event_without_the_account_attendee_is_not_a_caller_input_error() {
        let mut attendees = Some(vec![GoogleAttendee {
            email: Some("other@example.test".to_string()),
            display_name: None,
            response_status: Some("needsAction".to_string()),
            optional: None,
            resource: None,
            extra: Map::new(),
        }]);
        let error = rsvp_google_attendees_for_self(&mut attendees, "self@example.test", "accepted")
            .expect_err("the event has no attendee to answer for this account");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EventRsvp)
        ));
    }

    #[test]
    fn google_event_create_writes_transparency() {
        let patch = google_event_from_create(&EventCreate {
            calendar_id: CalendarId("primary".to_string()),
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
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        })
        .expect("event without attendees is expressible");

        assert_eq!(patch.transparency.as_deref(), Some("transparent"));
        assert_eq!(patch.status.as_deref(), Some("tentative"));
    }

    /// A created event with no id names the field Google left out, `id`,
    /// not the unrelated `calendar` the shared helper once hard-coded.
    #[tokio::test]
    async fn a_created_event_without_an_id_names_the_missing_id_field() {
        let (client, _script) = scripted_client(vec![canned_json(StatusCode::OK, json!({}))]);
        let error = create(
            client,
            EventCreate {
                calendar_id: CalendarId("primary".to_string()),
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
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
        )
        .await
        .expect_err("a created event without an id is refused");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::MissingField
            )
        );
        let support_text = error.support_consented().support_text;
        assert!(
            support_text
                .iter()
                .any(|text| text.contains("missing field `id`")),
            "the missing field must be named `id`, got {support_text:?}"
        );
        assert!(
            !support_text
                .iter()
                .any(|text| text.contains("missing field `calendar`")),
            "got {support_text:?}"
        );
    }

    #[test]
    fn google_event_create_organizer_is_rejected() {
        let error = reject_create_organizer(&EventCreate {
            calendar_id: CalendarId("primary".to_string()),
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
                email: "owner@example.test".to_string(),
                name: None,
            }),
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        })
        .expect_err("organizer should be unsupported");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
    }

    #[test]
    fn google_event_patch_serializes_clears_as_null_and_omits_absent_fields() {
        let patch = google_event_from_patch(&EventPatch {
            title: Some(None),
            description: None,
            location: Some(Some("Room 1".to_string())),
            status: Some(EventStatus::Cancelled),
            ..EventPatch::default()
        })
        .expect("patch is expressible");
        let value = serde_json::to_value(&patch).expect("patch json");

        assert!(value.get("summary").is_some_and(serde_json::Value::is_null));
        assert!(value.get("description").is_none());
        assert_eq!(
            value.get("location").and_then(serde_json::Value::as_str),
            Some("Room 1")
        );
        assert_eq!(
            value.get("status").and_then(serde_json::Value::as_str),
            Some("cancelled")
        );
    }

    #[test]
    fn google_event_patch_keeps_all_day_date_shape() {
        let patch = google_event_from_patch(&EventPatch {
            start: Some(EventTime {
                value: "2026-06-02".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            end: Some(EventTime {
                value: "2026-06-03".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            ..EventPatch::default()
        })
        .expect("patch is expressible");
        let value = serde_json::to_value(&patch).expect("patch json");

        assert_eq!(
            value
                .get("start")
                .and_then(|start| start.get("date"))
                .and_then(serde_json::Value::as_str),
            Some("2026-06-02")
        );
        assert!(
            value
                .get("start")
                .and_then(|start| start.get("dateTime"))
                .is_none()
        );
    }

    #[test]
    fn google_event_patch_clears_recurrence_with_empty_array() {
        let patch = google_event_from_patch(&EventPatch {
            recurrence: Some(EventRecurrence::default()),
            ..EventPatch::default()
        })
        .expect("patch is expressible");
        let value = serde_json::to_value(&patch).expect("patch json");

        assert_eq!(
            value
                .get("recurrence")
                .and_then(serde_json::Value::as_array),
            Some(&Vec::new())
        );
    }

    #[test]
    fn google_event_patch_omits_recurrence_when_absent() {
        let patch = google_event_from_patch(&EventPatch {
            title: Some(Some("Planning".to_string())),
            ..EventPatch::default()
        })
        .expect("patch is expressible");
        let value = serde_json::to_value(&patch).expect("patch json");

        assert!(value.get("recurrence").is_none());
    }

    #[test]
    fn google_event_patch_emits_recurrence_lines_when_present() {
        let patch = google_event_from_patch(&EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=WEEKLY".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        })
        .expect("patch is expressible");

        assert_eq!(
            patch.recurrence.as_deref(),
            Some(["RRULE:FREQ=WEEKLY".to_string()].as_slice())
        );
    }

    #[test]
    fn all_day_only_patch_is_rejected_as_unsupported() {
        let error = reject_unexpressible_all_day_patch(&EventPatch {
            is_all_day: Some(true),
            ..EventPatch::default()
        })
        .expect_err("all-day-only patch should be rejected");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));
    }

    #[test]
    fn all_day_patch_with_both_bounds_is_accepted() {
        reject_unexpressible_all_day_patch(&EventPatch {
            is_all_day: Some(true),
            start: Some(EventTime {
                value: "2026-06-02".to_string(),
                timezone: None,
            }),
            end: Some(EventTime {
                value: "2026-06-03".to_string(),
                timezone: None,
            }),
            ..EventPatch::default()
        })
        .expect("all-day patch carrying both bounds is expressible");
    }

    fn write_time(value: &str) -> EventTime {
        EventTime {
            value: value.to_string(),
            timezone: None,
        }
    }

    fn create_with(start: &str, end: &str) -> EventCreate {
        EventCreate {
            calendar_id: CalendarId("primary".to_string()),
            title: None,
            description: None,
            location: None,
            start: write_time(start),
            end: write_time(end),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        }
    }

    /// An echoed empty end leaves the stored end alone: neither an empty
    /// `dateTime` nor the non-writable `endTimeUnspecified` goes out. Fails if
    /// the blank-end filter in `google_event_from_patch` is removed.
    #[test]
    fn patch_echoing_an_empty_end_omits_it() {
        let value = serde_json::to_value(
            google_event_from_patch(&EventPatch {
                end: Some(write_time("")),
                ..EventPatch::default()
            })
            .expect("patch is expressible"),
        )
        .expect("patch json");

        assert!(value.get("end").is_none(), "no empty dateTime is sent");
        assert!(value.get("endTimeUnspecified").is_none());
    }

    #[test]
    fn patch_with_a_real_end_sends_it() {
        let value = serde_json::to_value(
            google_event_from_patch(&EventPatch {
                end: Some(write_time("2026-06-02T13:00:00Z")),
                ..EventPatch::default()
            })
            .expect("patch is expressible"),
        )
        .expect("patch json");

        assert_eq!(value["end"]["dateTime"], json!("2026-06-02T13:00:00Z"));
    }

    /// Google's insert needs an end and "no end" is not writable, so a create
    /// with an empty end is refused before any request. Fails if the blank-end
    /// arm of `reject_unusable_write_times` is removed.
    #[tokio::test]
    async fn create_with_an_empty_end_is_refused_before_any_request() {
        let (client, script) = scripted_client(Vec::new());

        let error = create(client, create_with("2026-06-02T12:00:00Z", ""))
            .await
            .expect_err("an empty end cannot be created");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
        assert!(script.requests().is_empty());
    }

    /// A consumer echoing the whole event sends its unchanged start back with
    /// the empty end. That must not be refused: the start goes out, the end is
    /// omitted. Fails if the empty-end refusal is widened to any patch that
    /// carries `start`.
    #[tokio::test]
    async fn update_echoing_start_with_an_empty_end_sends_start_and_omits_end() {
        let (client, script) = scripted_client(vec![canned_json(StatusCode::OK, json!({}))]);
        let patch = EventPatch {
            title: Some(Some("Renamed".to_string())),
            start: Some(write_time("2026-06-02T12:00:00Z")),
            end: Some(write_time("")),
            ..EventPatch::default()
        };

        update(client, EventId("primary::event-1".to_string()), patch)
            .await
            .expect("an echoed start with an empty end is written");

        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value =
            serde_json::from_slice(requests[0].body.as_ref().expect("body")).expect("json");
        assert_eq!(body["start"]["dateTime"], json!("2026-06-02T12:00:00Z"));
        assert!(body.get("end").is_none());
        assert!(body.get("endTimeUnspecified").is_none());
    }

    /// Fails if `reject_unusable_write_times` is removed from `create`: the
    /// request would be sent with `dateTime: ""` (the scripted client has no
    /// response, so the call would fail differently and the request list
    /// would be non-empty).
    #[tokio::test]
    async fn create_with_an_empty_start_is_refused_before_any_request() {
        let (client, script) = scripted_client(Vec::new());

        let error = create(client, create_with("", "2026-06-02T13:00:00Z"))
            .await
            .expect_err("an empty start is malformed");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(script.requests().is_empty());
    }

    /// Fails if `reject_unusable_write_times` is removed from `update`, or
    /// moved after the calendar move: a whitespace-only start must not issue
    /// the move nor the patch.
    #[tokio::test]
    async fn update_with_a_blank_start_is_refused_before_the_move() {
        let (client, script) = scripted_client(Vec::new());
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            start: Some(write_time("  ")),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("a blank start is malformed");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(script.requests().is_empty());
    }

    /// Malformed non-empty times are refused locally, as the caller's input,
    /// before any request. Fails if the shape check in
    /// `reject_unusable_write_times` is removed: each case would reach the
    /// (empty) script instead.
    #[tokio::test]
    async fn create_with_malformed_times_is_refused_before_any_request() {
        let cases = [
            // Timed, no offset and no timezone.
            (
                create_with("2026-06-02T12:00:00", "2026-06-02T13:00:00Z"),
                "floating start",
            ),
            // Garbage.
            (
                create_with("tomorrow", "2026-06-02T13:00:00Z"),
                "garbage start",
            ),
            (create_with("2026-06-02T12:00:00Z", "later"), "garbage end"),
            // A date on a timed write.
            (
                create_with("2026-06-02", "2026-06-03"),
                "date on a timed write",
            ),
            // A timestamp on an all-day write.
            (
                EventCreate {
                    is_all_day: true,
                    ..create_with("2026-06-02T12:00:00Z", "2026-06-03T12:00:00Z")
                },
                "timestamp on an all-day write",
            ),
            (
                EventCreate {
                    is_all_day: true,
                    ..create_with("2026-13-45", "2026-06-03")
                },
                "impossible all-day date",
            ),
        ];
        for (event, label) in cases {
            let (client, script) = scripted_client(Vec::new());
            let error = create(client, event).await.expect_err(label);
            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
                ),
                "{label}: {:?}",
                error.kind()
            );
            assert!(script.requests().is_empty(), "{label}");
        }
    }

    /// The forms the guard must keep accepting: offset, `Z`, local plus a
    /// timezone, and an all-day date.
    #[test]
    fn well_formed_times_pass_the_shape_check() {
        let op = AccountOperation::EventCreate;
        let ok = |value: &str, timezone: Option<&str>, all_day: Option<bool>| {
            reject_unusable_write_times(
                op,
                Some(&EventTime {
                    value: value.to_string(),
                    timezone: timezone.map(ToString::to_string),
                }),
                None,
                all_day,
            )
            .unwrap_or_else(|error| panic!("{value}: {:?}", error.kind()));
        };
        ok("2026-06-02T12:00:00Z", None, Some(false));
        ok("2026-06-02T12:00:00+02:00", None, None);
        ok(
            "2026-06-02T12:00:00-05:00",
            Some("America/Chicago"),
            Some(false),
        );
        ok("2026-06-02T12:00:00", Some("Europe/Oslo"), Some(false));
        ok("2026-06-02T12:00:00.500", Some("Europe/Oslo"), None);
        ok("2026-06-02", None, Some(true));
        ok("2026-06-02", None, None);
    }

    /// The reason the shape check exists: with a calendar move in the same
    /// update, Google's own 400 would arrive AFTER `events.move` succeeded.
    /// Fails if the shape check is removed: the move request would be issued.
    #[tokio::test]
    async fn update_with_a_malformed_end_and_a_move_is_refused_before_the_move() {
        let (client, script) = scripted_client(Vec::new());
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            start: Some(write_time("2026-06-02T12:00:00Z")),
            end: Some(write_time("2026-06-02T13:00:00")),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("a floating end without a timezone is malformed");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(script.requests().is_empty());
    }

    /// End to end: the read-modify-write of an unspecified-end event. Read
    /// projects no end, and the echoed patch goes out without an end at all,
    /// so the stored one is untouched. Fails if the read side projects the
    /// placeholder again, which the echo would then write back as a real end.
    #[tokio::test]
    async fn round_tripping_an_unspecified_end_event_neither_fails_nor_invents_an_end() {
        let read = event_from_google(
            "primary".to_string(),
            GoogleEvent {
                id: Some("e1".to_string()),
                start: Some(timed("2026-06-02T12:00:00Z")),
                end: Some(timed("2026-06-02T12:00:00Z")),
                end_time_unspecified: Some(true),
                ..GoogleEvent::default()
            },
            AccountOperation::EventGet,
        )
        .expect("unspecified end projects");
        assert_eq!(read.end, None);

        let (client, script) = scripted_client(vec![canned_json(StatusCode::OK, json!({}))]);
        update(
            client,
            EventId("primary::e1".to_string()),
            EventPatch {
                title: Some(Some("Renamed".to_string())),
                end: read.end,
                ..EventPatch::default()
            },
        )
        .await
        .expect("the echoed empty end is accepted");

        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value =
            serde_json::from_slice(requests[0].body.as_ref().expect("body")).expect("json");
        assert!(body.get("endTimeUnspecified").is_none());
        assert!(body.get("end").is_none());
        assert_eq!(body["summary"], json!("Renamed"));
    }

    #[test]
    fn google_range_bound_expands_all_day_dates() {
        assert_eq!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02".to_string(),
                    timezone: None,
                },
                "timeMin"
            )
            .unwrap(),
            "2026-06-02T00:00:00Z"
        );
    }

    #[test]
    fn google_range_bound_keeps_existing_offsets() {
        assert_eq!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02T12:00:00+02:00".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                "timeMin"
            )
            .unwrap(),
            "2026-06-02T12:00:00+02:00"
        );
    }

    #[test]
    fn google_range_bound_adds_utc_offset_when_timezone_is_utc() {
        assert_eq!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02T12:00:00".to_string(),
                    timezone: Some("UTC".to_string()),
                },
                "timeMin"
            )
            .unwrap(),
            "2026-06-02T12:00:00Z"
        );
    }

    #[test]
    fn google_range_bound_rejects_offsetless_non_utc_datetime() {
        assert!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02T12:00:00".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                "timeMin",
            )
            .is_err()
        );
    }
}
