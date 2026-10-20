#[path = "calendar_public.rs"]
mod public;

use bifrost_types::{
    AccountError, AccountOperation, AttendeeRole, Calendar, CalendarEvent, CalendarId,
    CalendarProvenance, EventAttendee, EventAvailability, EventCreate, EventId, EventOrganizer,
    EventPatch, EventRange, EventRecurrence, EventSearchRequest, EventStatus, EventTime,
    EventVisibility, Page, ProtocolKind, RsvpStatus,
};
use jiff::civil;
use jiff::tz::{Offset, TimeZone};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::client::Resource;
use crate::types::ODataCollection;

use super::GraphAccount;
use super::graph_error::{self, GraphErrorContext};

const DEFAULT_CALENDAR_ID: &str = "calendar";
/// Sentinel calendar segment for composite `EventId`s whose hosting
/// calendar is unknown. Graph event ids are mailbox-unique, so such ids
/// address through `/me/events/{id}` instead of
/// `/me/calendars/{cal}/events/{id}`. The Graph Search API spans the
/// whole mailbox without reporting each hit's calendar, so search hits
/// are minted with this segment to keep `event_get`/`update`/`delete`
/// routing honest.
const MAILBOX_SCOPE: &str = "$mailbox";
const EVENT_ID_SEPARATOR: &str = "::";
const EVENT_SELECT: &str = "id,subject,body,location,start,end,isAllDay,showAs,sensitivity,organizer,attendees,originalStart,webLink,categories,responseStatus,isCancelled,changeKey,recurrence";
const EVENT_TIMEZONE_PREFER: &str = "outlook.timezone=\"UTC\"";
const EVENT_SEARCH_FIELDS: &[&str] = &[
    "id",
    "subject",
    "body",
    "location",
    "start",
    "end",
    "isAllDay",
    "showAs",
    "sensitivity",
    "organizer",
    "attendees",
    "originalStart",
    "webLink",
    "categories",
    "responseStatus",
    "isCancelled",
    "changeKey",
    "recurrence",
];

pub(crate) async fn calendars_list(account: GraphAccount) -> Result<Vec<Calendar>, AccountError> {
    let prefix = account.client.api_path_prefix();
    let mut calendars = Vec::new();
    let mut walk = crate::paging::PageWalk::new("calendars");
    let mut next = Some(
        account
            .client
            .api_url(&format!(
                "{prefix}/calendars?$select=id,name,canEdit,isDefaultCalendar&$top=250"
            ))
            .map_err(|error| into_error(error, AccountOperation::CalendarsList))?,
    );
    while let Some(url) = next {
        walk.enter(&url)
            .map_err(|error| into_error(error, AccountOperation::CalendarsList))?;
        let page: ODataCollection<GraphCalendar> = account
            .client
            .get(Resource::Outlook, &url)
            .await
            .map_err(|error| into_error(error, AccountOperation::CalendarsList))?;
        calendars.extend(page.value.into_iter().map(calendar_from_graph));
        next = account
            .client
            .admit_next(page.next_link.as_ref())
            .map_err(|error| into_error(error, AccountOperation::CalendarsList))?;
    }
    calendars.extend(public::calendars(&account).await);
    Ok(calendars)
}

pub(crate) async fn events_in_range(
    account: GraphAccount,
    range: EventRange,
) -> Result<Page<CalendarEvent>, AccountError> {
    if let Some(folder) = super::foreign::parse_public_calendar(&range.calendar_id.0) {
        return public::range(&account, folder, &range).await;
    }
    let operation = AccountOperation::EventsInRange;
    let cursor = crate::paging::decode_link_cursor(&account.client, range.page_cursor)
        .map_err(|detail| graph_error::invalid_account_error(operation, detail))?;
    let url = match cursor {
        Some(url) => url,
        None => {
            let prefix = account.client.api_path_prefix();
            let calendar = bifrost_net::url::encode_path_component(&range.calendar_id.0);
            account
                .client
                .api_url(&format!(
                    "{prefix}/calendars/{calendar}/calendarView?startDateTime={}&endDateTime={}&$select={EVENT_SELECT}&$top={}",
                    bifrost_net::url::encode_query_value(&range.start.value),
                    bifrost_net::url::encode_query_value(&range.end.value),
                    range.limit.unwrap_or(250).clamp(1, 250)
                ))
                .map_err(|error| into_error(error, operation))?
        }
    };
    let page: ODataCollection<GraphEvent> = get_event_page(&account, &url, operation).await?;
    let next_cursor = account
        .client
        .admit_next(page.next_link.as_ref())
        .map_err(|error| into_error(error, operation))?
        .map(|next| next.as_str().as_bytes().to_vec());
    Ok(Page {
        truncated: false,
        items: page
            .value
            .into_iter()
            .map(|event| event_from_graph(range.calendar_id.0.clone(), event))
            .collect(),
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

pub(crate) async fn get(
    account: GraphAccount,
    event: EventId,
) -> Result<CalendarEvent, AccountError> {
    if let Some((folder, item)) = super::foreign::parse_public_event(&event.0) {
        return public::get(&account, folder, item).await;
    }
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventGet)?;
    let event = fetch_graph_event(&account, &calendar_id, &event_id).await?;
    Ok(event_from_graph(calendar_id, event))
}

async fn fetch_graph_event(
    account: &GraphAccount,
    calendar_id: &str,
    event_id: &str,
) -> Result<GraphEvent, AccountError> {
    let path = event_url(account, calendar_id, event_id);
    account
        .client
        .get_json_prefer::<GraphEvent>(Resource::Outlook, &path, EVENT_TIMEZONE_PREFER)
        .await
        .map_err(|error| into_error(error, AccountOperation::EventGet))
}

pub(crate) async fn create(
    account: GraphAccount,
    event: EventCreate,
) -> Result<EventId, AccountError> {
    public::reject_calendar(&event.calendar_id, AccountOperation::EventCreate)?;
    reject_create_organizer(&event)?;
    reject_unwritable_status(event.status, AccountOperation::EventCreate)?;
    reject_unusable_write_times(
        AccountOperation::EventCreate,
        Some(&event.start),
        Some(&event.end),
    )?;
    validate_event_create_timezones(&event)?;
    let calendar_id = event.calendar_id.0.clone();
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_path_component(&calendar_id);
    let path = format!("{prefix}/calendars/{encoded}/events");
    let body = graph_event_from_create(&event)?;
    let created = account
        .client
        .post::<GraphEvent, _>(Resource::Outlook, &path, &body)
        .await
        .map_err(|error| into_error(error, AccountOperation::EventCreate))?;
    Ok(EventId(join_event_id(&calendar_id, &created.id)))
}

fn reject_create_organizer(event: &EventCreate) -> Result<(), AccountError> {
    if event.organizer.is_some() {
        return Err(unsupported_error(
            AccountOperation::EventCreate,
            "Graph event organizer is server-derived on event creation".to_string(),
        ));
    }
    Ok(())
}

/// Graph has no writable event status: `isCancelled` is server-derived
/// from cancellation actions, and there is no confirmed/tentative knob.
/// Accept only `Confirmed` (the state a freshly created or patched Graph
/// event reports) and reject any other requested status before payload
/// construction, mirroring the organizer-on-create rejection rather than
/// silently writing a confirmed event.
fn reject_unwritable_status(
    status: EventStatus,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if matches!(status, EventStatus::Confirmed) {
        return Ok(());
    }
    Err(unsupported_error(
        operation,
        "Graph event status is server-derived; only Confirmed can be expressed".to_string(),
    ))
}

fn is_blank_time(time: &EventTime) -> bool {
    time.value.trim().is_empty()
}

/// Write-side guard for event times, run before any request.
///
/// The read projection yields an empty `start` for "Graph did not say" (a
/// delta tombstone with no `start`, or a `dateTime` that is null), and a
/// consumer doing read-modify-write echoes it back; an unsaid end reads as
/// `None`, but a caller may still pass an empty one. Written as is an empty
/// time reaches the wire as `dateTime: ""` under a defaulted UTC zone, which
/// Graph refuses or, worse, reads as an unintended value. So:
///
/// - an empty `start` is `Request(Malformed)` on create and patch: there is
///   no start to write and none this client may invent;
/// - an empty `end` on a create is `Unsupported`: the caller must supply the
///   end it wants, and no default is invented here;
/// - an empty `end` on a patch is NOT refused: `graph_event_from_patch` omits
///   it, so an echoed event leaves the stored end alone.
fn reject_unusable_write_times(
    operation: AccountOperation,
    start: Option<&EventTime>,
    end: Option<&EventTime>,
) -> Result<(), AccountError> {
    if start.is_some_and(is_blank_time) {
        return Err(graph_error::invalid_argument_account_error(
            operation,
            "start",
            "Graph event `start` must not be empty",
        ));
    }
    if operation == AccountOperation::EventCreate && end.is_some_and(is_blank_time) {
        return Err(unsupported_error(
            operation,
            "Graph event create needs an explicit end; an empty end cannot be written".to_string(),
        ));
    }
    Ok(())
}

fn validate_event_create_timezones(event: &EventCreate) -> Result<(), AccountError> {
    validate_graph_time_zone(
        event.start.timezone.as_deref(),
        "start.timezone",
        AccountOperation::EventCreate,
    )?;
    validate_graph_time_zone(
        event.end.timezone.as_deref(),
        "end.timezone",
        AccountOperation::EventCreate,
    )
}

fn validate_event_patch_timezones(patch: &EventPatch) -> Result<(), AccountError> {
    if let Some(start) = &patch.start {
        validate_graph_time_zone(
            start.timezone.as_deref(),
            "start.timezone",
            AccountOperation::EventUpdate,
        )?;
    }
    if let Some(end) = &patch.end {
        validate_graph_time_zone(
            end.timezone.as_deref(),
            "end.timezone",
            AccountOperation::EventUpdate,
        )?;
    }
    Ok(())
}

pub(crate) async fn update(
    account: GraphAccount,
    event: EventId,
    patch: EventPatch,
) -> Result<(), AccountError> {
    let operation = AccountOperation::EventUpdate;
    public::reject_event(&event, operation)?;
    if let Some(calendar) = &patch.calendar_id {
        public::reject_calendar(calendar, operation)?;
    }
    reject_unusable_write_times(operation, patch.start.as_ref(), patch.end.as_ref())?;
    validate_event_patch_timezones(&patch)?;
    if let Some(status) = patch.status {
        reject_unwritable_status(status, operation)?;
    }
    // Refuse a recurrence Graph cannot take before spending the fetch; the
    // range anchor a rule may still need is resolved against the fetched
    // event below.
    if let Some(recurrence) = &patch.recurrence {
        validate_recurrence(recurrence).map_err(|refusal| refusal.into_account_error(operation))?;
    }
    let (event_calendar_id, native_event_id) = split_event_id(&event.0, operation)?;
    reject_calendar_move(&patch, &event_calendar_id)?;
    let current = fetch_graph_event(&account, &event_calendar_id, &native_event_id).await?;
    let series = CurrentSeries {
        recurrence: current.recurrence.as_ref(),
        time_zone: current.original_start_time_zone.as_deref(),
    };
    let body = graph_event_from_patch(&patch, series)?;
    let path = event_url(&account, &event_calendar_id, &native_event_id);
    let result = if let Some(etag) = current.change_key.as_deref() {
        account
            .client
            .patch_if_match(Resource::Outlook, &path, etag, &body)
            .await
    } else {
        account.client.patch(Resource::Outlook, &path, &body).await
    };
    result.map_err(|error| into_error(error, operation))
}

/// Graph has no move for events: PATCHing an event under another calendar's
/// path edits it where it is. A patch naming a calendar other than the one the
/// event id carries (including any calendar for a mailbox-scoped search hit,
/// whose calendar is unknown) is refused rather than reported as a move that
/// never happened.
fn reject_calendar_move(patch: &EventPatch, event_calendar_id: &str) -> Result<(), AccountError> {
    match &patch.calendar_id {
        Some(calendar) if calendar.0 != event_calendar_id => Err(unsupported_error(
            AccountOperation::EventUpdate,
            "Graph event update cannot move an event to another calendar".to_string(),
        )),
        _ => Ok(()),
    }
}

pub(crate) async fn delete(account: GraphAccount, event: EventId) -> Result<(), AccountError> {
    public::reject_event(&event, AccountOperation::EventDelete)?;
    let current = get(account.clone(), event.clone()).await?;
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventDelete)?;
    let path = event_url(&account, &calendar_id, &event_id);
    let result = if let Some(etag) = current.etag.as_deref() {
        account
            .client
            .delete_if_match(Resource::Outlook, &path, etag)
            .await
    } else {
        account.client.delete(Resource::Outlook, &path).await
    };
    result.map_err(|error| into_error(error, AccountOperation::EventDelete))
}

pub(crate) async fn rsvp(
    account: GraphAccount,
    event: EventId,
    status: RsvpStatus,
) -> Result<(), AccountError> {
    public::reject_event(&event, AccountOperation::EventRsvp)?;
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventRsvp)?;
    let action = rsvp_action(status)?;
    let path = format!("{}/{action}", event_url(&account, &calendar_id, &event_id));
    account
        .client
        .post_no_response(
            Resource::Outlook,
            &path,
            &GraphRsvpAction {
                comment: None,
                send_response: false,
            },
        )
        .await
        .map_err(|error| into_error(error, AccountOperation::EventRsvp))
}

/// Sentinel prefix marking a `page_cursor` minted by the Graph Search API
/// path (`search_with_graph_api`). The Search API pages by `from`/`size`
/// offset rather than an `@odata.nextLink`, so its cursor cannot be fed to
/// the local nextLink walk and vice versa. The bytes after the prefix are
/// the decimal next `from` offset. `\u{1f}` cannot appear in a Graph
/// `@odata.nextLink` URL, so the two cursor shapes never collide.
const SEARCH_API_CURSOR_PREFIX: &[u8] = b"\x1fsearchapi:";

pub(crate) async fn search(
    account: GraphAccount,
    request: EventSearchRequest,
) -> Result<Page<CalendarEvent>, AccountError> {
    if let Some(folder) = request
        .calendar_id
        .as_ref()
        .and_then(|id| super::foreign::parse_public_calendar(&id.0))
    {
        return public::search(&account, folder, &request).await;
    }
    // A Search-API cursor must resume through the Search API: a local
    // nextLink walk cannot consume a `from`-offset cursor.
    if let Some(from) = search_api_cursor_offset(request.page_cursor.as_deref()) {
        return search_with_graph_api(account, request, from).await;
    }
    if graph_search_api_supported(&account, &request) {
        return search_with_graph_api(account, request, 0).await;
    }
    search_locally(account, request).await
}

/// Decode a Search-API `page_cursor` into its `from` offset, or `None` if
/// the cursor is absent or is a local nextLink cursor.
fn search_api_cursor_offset(cursor: Option<&[u8]>) -> Option<u32> {
    let bytes = cursor?.strip_prefix(SEARCH_API_CURSOR_PREFIX)?;
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Encode a Search-API next-page `from` offset into an opaque cursor.
fn search_api_cursor(from: u32) -> Vec<u8> {
    let mut cursor = SEARCH_API_CURSOR_PREFIX.to_vec();
    cursor.extend_from_slice(from.to_string().as_bytes());
    cursor
}

async fn search_locally(
    account: GraphAccount,
    request: EventSearchRequest,
) -> Result<Page<CalendarEvent>, AccountError> {
    let explicit_calendar_id = request.calendar_id.clone();
    let calendar_id = explicit_calendar_id
        .clone()
        .unwrap_or_else(|| CalendarId(DEFAULT_CALENDAR_ID.to_string()));
    let operation = AccountOperation::EventSearch;
    let cursor = crate::paging::decode_paged_cursor(&account.client, request.page_cursor)
        .map_err(|detail| graph_error::invalid_account_error(operation, detail))?;
    let (mut url, mut skip) = match cursor {
        Some(position) => position,
        None => {
            let prefix = account.client.api_path_prefix();
            let first = account
                .client
                .api_url(&format!(
                    "{}?$select={EVENT_SELECT}&$top={}",
                    event_search_path(&prefix, explicit_calendar_id.as_ref()),
                    request.limit.unwrap_or(250).min(250)
                ))
                .map_err(|error| into_error(error, operation))?;
            (first, 0)
        }
    };
    let needle = request.query.to_ascii_lowercase();
    let include_cancelled = request.include_cancelled;
    let limit = request
        .limit
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(250)
        .max(1);
    let mut items = Vec::new();
    let mut walk = crate::paging::PageWalk::new("event search");
    let next_cursor;
    loop {
        // Passed through like every other walk: a repeated link is the
        // provider's contract breach and an exhausted budget is this crate's
        // limit, and `PageWalk` already says which.
        walk.enter(&url)
            .map_err(|error| into_error(error, operation))?;
        let page: ODataCollection<GraphEvent> =
            get_event_page(&account, &url, AccountOperation::EventSearch).await?;
        // Admitted at receipt, before it can be followed or minted into the
        // caller's cursor below.
        let next_link = account
            .client
            .admit_next(page.next_link.as_ref())
            .map_err(|error| into_error(error, operation))?;
        let page_len = page.value.len();
        for (index, value) in page.value.into_iter().enumerate().skip(skip) {
            let event = event_from_graph(calendar_id.0.clone(), value);
            // `isCancelled` is selected on every page, so the cancelled
            // filter is client-side. Dropped events still count toward
            // `consumed`, which indexes the raw page, and `estimated_total`
            // is `None` here so nothing over-counts them.
            if (include_cancelled || event.status != EventStatus::Cancelled)
                && event_matches(&event, &needle)
            {
                items.push(event);
                if items.len() == limit {
                    let consumed = index + 1;
                    next_cursor = if consumed < page_len {
                        Some(crate::paging::encode_paged_cursor(&url, consumed))
                    } else {
                        next_link
                            .as_ref()
                            .map(|next| crate::paging::encode_paged_cursor(next, 0))
                    };
                    return Ok(Page {
                        truncated: false,
                        items,
                        next_cursor,
                        estimated_total: None,
                        failed_ids: Vec::new(),
                        skipped_scopes: Vec::new(),
                    });
                }
            }
        }
        let Some(next) = next_link else {
            next_cursor = None;
            break;
        };
        url = next;
        skip = 0;
    }
    Ok(Page {
        truncated: false,
        items,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

async fn search_with_graph_api(
    account: GraphAccount,
    request: EventSearchRequest,
    from: u32,
) -> Result<Page<CalendarEvent>, AccountError> {
    let size = request.limit.unwrap_or(250).clamp(1, 250);
    let body = GraphSearchRequest {
        requests: vec![GraphSearchEntityRequest {
            entity_types: vec!["event"],
            query: GraphSearchQuery {
                query_string: request.query.clone(),
            },
            from,
            size,
            fields: EVENT_SEARCH_FIELDS,
        }],
    };
    let response = account
        .client
        .post::<GraphSearchResponse, _>(Resource::Outlook, "/search/query", &body)
        .await
        .map_err(|error| into_error(error, AccountOperation::EventSearch))?;
    // Graph Search pages by `from`/`size`; `moreResultsAvailable` on the
    // hits container signals a further page. Carry the next `from` offset
    // in a sentinel cursor so the next call resumes the Search API rather
    // than capping at one page.
    let more_results = response
        .value
        .iter()
        .flat_map(|set| set.hits_containers.iter())
        .any(|container| container.more_results_available.unwrap_or(false));
    let all_items: Vec<CalendarEvent> = response
        .value
        .into_iter()
        .flat_map(|set| set.hits_containers)
        .flat_map(|container| container.hits)
        .filter_map(|hit| hit.resource)
        .map(|event| event_from_graph(MAILBOX_SCOPE.to_string(), event))
        .collect();
    // The next `from` offset counts the RAW hits the server returned, not
    // the ones surviving the client-side cancelled filter, or the next page
    // would re-read the dropped hits. `estimated_total` stays `None`, so the
    // filter cannot over-count.
    let raw_len = all_items.len();
    let items: Vec<CalendarEvent> = if request.include_cancelled {
        all_items
    } else {
        all_items
            .into_iter()
            .filter(|event| event.status != EventStatus::Cancelled)
            .collect()
    };
    let next_cursor = (more_results && raw_len > 0)
        .then(|| search_api_cursor(from + u32::try_from(raw_len).unwrap_or(size)));
    Ok(Page {
        truncated: false,
        items,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

fn graph_search_api_supported(account: &GraphAccount, request: &EventSearchRequest) -> bool {
    request.page_cursor.is_none()
        && request.calendar_id.is_none()
        && !request.query.trim().is_empty()
        && account.client.uses_default_mailbox()
}

async fn get_event_page<T: serde::de::DeserializeOwned>(
    account: &GraphAccount,
    url: &crate::origin::AdmittedUrl,
    operation: AccountOperation,
) -> Result<ODataCollection<T>, AccountError> {
    account
        .client
        .get_prefer(Resource::Outlook, url, EVENT_TIMEZONE_PREFER)
        .await
        .map_err(|error| into_error(error, operation))
}

fn calendar_from_graph(calendar: GraphCalendar) -> Calendar {
    let can_edit = calendar.can_edit.unwrap_or(true);
    Calendar {
        id: CalendarId(calendar.id.clone()),
        native_id: calendar.id.clone(),
        name: calendar.name.unwrap_or(calendar.id.clone()),
        color: None,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Graph,
            native: calendar.id,
            calendar_native: None,
        },
        is_default: calendar.is_default_calendar.unwrap_or(false),
        can_create_events: can_edit,
        can_update_events: can_edit,
        can_delete_events: can_edit,
    }
}

fn event_from_graph(calendar_id: String, event: GraphEvent) -> CalendarEvent {
    let native = join_event_id(&calendar_id, &event.id);
    let self_response = rsvp_status(
        event
            .response_status
            .as_ref()
            .and_then(|status| status.get("response"))
            .and_then(Value::as_str),
    );
    CalendarEvent {
        id: EventId(native.clone()),
        calendar_id: CalendarId(calendar_id.clone()),
        native_id: native.clone(),
        uid: Some(event.id.clone()),
        etag: event.change_key.clone(),
        provenance: CalendarProvenance {
            provider: ProtocolKind::Graph,
            native,
            calendar_native: Some(calendar_id),
        },
        title: event.subject,
        description: event.body.and_then(|body| body.content),
        location: event.location.and_then(|location| location.display_name),
        is_all_day: event.is_all_day.unwrap_or(false),
        start: event
            .start
            .map(|time| event_time(time, event.is_all_day.unwrap_or(false)))
            .unwrap_or_else(empty_time),
        // A missing or valueless `end` is no end, not an empty time.
        end: event
            .end
            .map(|time| event_time(time, event.is_all_day.unwrap_or(false)))
            .filter(|time| !time.value.is_empty()),
        status: if event.is_cancelled.unwrap_or(false) {
            EventStatus::Cancelled
        } else {
            EventStatus::Confirmed
        },
        availability: availability(event.show_as.as_deref()),
        visibility: visibility(event.sensitivity.as_deref()),
        self_response,
        organizer: event.organizer.and_then(|recipient| {
            let email = recipient.email_address?;
            Some(EventOrganizer {
                email: email.address?,
                name: email.name,
            })
        }),
        attendees: event
            .attendees
            .unwrap_or_default()
            .into_iter()
            .filter_map(attendee_from_graph)
            .collect(),
        // Microsoft Graph reminders are not projected onto the shared read
        // surface yet; other providers (CalDAV VALARM, JMAP alerts) supply
        // them.
        reminders: Vec::new(),
        recurrence: EventRecurrence {
            rrule: event.recurrence.as_ref().and_then(rrule_from_graph),
            // The shared model documents `recurrence_id` as identifying
            // an OVERRIDDEN OCCURRENCE (iCalendar RECURRENCE-ID
            // semantics). Graph's `originalStart` is that identity;
            // `seriesMasterId` - the previous mapping - names the whole
            // series and misled consumers following the docstring.
            recurrence_id: event.original_start,
            ..EventRecurrence::default()
        },
        html_link: event.web_link,
        raw_ical: None,
    }
}

fn graph_event_from_create(event: &EventCreate) -> Result<GraphEventPatch, AccountError> {
    let recurrence = create_recurrence(event)
        .map_err(|refusal| refusal.into_account_error(AccountOperation::EventCreate))?;
    Ok(GraphEventPatch {
        subject: event.title.clone().map(Value::String),
        body: event.description.as_ref().map(|description| {
            json!(GraphBody {
                content_type: Some("text".to_string()),
                content: Some(description.clone()),
            })
        }),
        location: event.location.as_ref().map(|name| {
            json!(GraphLocation {
                display_name: Some(name.clone()),
            })
        }),
        start: Some(graph_time(&event.start, event.is_all_day)),
        end: Some(graph_time(&event.end, event.is_all_day)),
        is_all_day: Some(event.is_all_day),
        show_as: Some(show_as(event.availability).to_string()),
        sensitivity: Some(sensitivity(event.visibility).to_string()),
        attendees: non_empty(event.attendees.iter().map(graph_attendee)),
        recurrence: recurrence.map(Some),
    })
}

fn create_recurrence(event: &EventCreate) -> Result<Option<GraphRecurrence>, RecurrenceRefusal> {
    validate_recurrence(&event.recurrence)?;
    let Some(rrule) = event.recurrence.rrule.as_deref() else {
        return Ok(None);
    };
    let rule = graph_recurrence_rule(rrule)?;
    let anchor = RecurrenceAnchor::from_event_time(&event.start)?;
    rule.anchored(&anchor).map(Some)
}

/// Refuse the recurrence parts Graph has no field for, and an RRULE it
/// cannot take, before any payload is built.
///
/// Graph's `patternedRecurrence` is a pattern plus a range, nothing else.
/// RDATE's extra occurrences have no carrier at all, and EXDATE exists in
/// Graph only as occurrences deleted after the series is written, which a
/// single create or patch cannot do. Dropping either writes a different series
/// than the one asked for.
fn validate_recurrence(recurrence: &EventRecurrence) -> Result<(), RecurrenceRefusal> {
    if !recurrence.rdate.is_empty() {
        return Err(unexpressible(
            "Graph recurrence has no RDATE: extra occurrences cannot be written",
        ));
    }
    if !recurrence.exdate.is_empty() {
        return Err(unexpressible(
            "Graph recurrence has no EXDATE: an occurrence is removed by deleting it once the \
             series exists",
        ));
    }
    if let Some(rrule) = recurrence.rrule.as_deref() {
        graph_recurrence_rule(rrule)?;
    }
    Ok(())
}

/// What `update` learned about the event it is patching.
#[derive(Debug, Default, Clone, Copy)]
struct CurrentSeries<'a> {
    /// The event's recurrence as Graph holds it; `None` for a one-off or an
    /// occurrence.
    recurrence: Option<&'a GraphRecurrence>,
    /// The event's `originalStartTimeZone`.
    time_zone: Option<&'a str>,
}

impl CurrentSeries<'_> {
    /// The anchor of the series as it stands. The range `startDate` is a
    /// plain date Graph reports untouched by the UTC `Prefer` the read uses,
    /// so it is the series' local start date.
    fn start_anchor(&self) -> Option<RecurrenceAnchor> {
        let range = self.recurrence?.range.as_ref()?;
        let start = parse_start_date(range.start_date.as_deref()?)?;
        let zone = range
            .recurrence_time_zone
            .as_deref()
            .or(self.time_zone)
            .and_then(resolve_time_zone);
        Some(RecurrenceAnchor { start, zone })
    }
}

/// The recurrence write a patch makes: `None` leaves it alone, `Some(None)`
/// clears it (`"recurrence": null`), `Some(Some(_))` writes a series.
///
/// Graph's `recurrenceRange.startDate` must be the series' local start date.
/// A patch carrying `start` supplies it. Without one, an event that already
/// recurs keeps its own range start. An event that does not recur yet has no
/// such date, and its UTC-normalised `start` can sit on a different calendar
/// day than the local one Graph wants, so that patch is refused as
/// `Unsupported` rather than anchored on a guessed date.
///
/// A patch that moves `start` on a recurring event without restating the
/// recurrence re-sends the event's own recurrence with the range start moved
/// along, so the range never disagrees with the event it belongs to.
fn patch_recurrence(
    patch: &EventPatch,
    series: CurrentSeries<'_>,
) -> Result<Option<Option<GraphRecurrence>>, AccountError> {
    let operation = AccountOperation::EventUpdate;
    let refuse = |refusal: RecurrenceRefusal| refusal.into_account_error(operation);
    let Some(requested) = &patch.recurrence else {
        return Ok(restarted_series(patch, series).map_err(refuse)?.map(Some));
    };
    validate_recurrence(requested).map_err(refuse)?;
    let Some(rrule) = requested.rrule.as_deref() else {
        // A recurrence with no rule and no dates is how a patch makes the
        // event a one-off again.
        return Ok(Some(None));
    };
    let rule = graph_recurrence_rule(rrule).map_err(refuse)?;
    let anchor = match (&patch.start, series.start_anchor()) {
        (Some(start), _) => RecurrenceAnchor::from_event_time(start).map_err(refuse)?,
        (None, Some(anchor)) => anchor,
        (None, None) => {
            return Err(unsupported_error(
                operation,
                "Graph recurrence on an event that does not recur yet needs start in the same \
                 patch: the range start is the event's local start date"
                    .to_string(),
            ));
        }
    };
    Ok(Some(Some(rule.anchored(&anchor).map_err(refuse)?)))
}

/// The event's own recurrence with its range start moved to the patch's new
/// `start`, or `None` when the patch moves no start or the event does not
/// recur. The moved start is read by the same rule as an explicit
/// recurrence's anchor, so a value that is not a date is refused here too.
fn restarted_series(
    patch: &EventPatch,
    series: CurrentSeries<'_>,
) -> Result<Option<GraphRecurrence>, RecurrenceRefusal> {
    let (Some(start), Some(current)) = (&patch.start, series.recurrence) else {
        return Ok(None);
    };
    let anchor = RecurrenceAnchor::from_event_time(start)?;
    let mut recurrence = current.clone();
    let Some(range) = recurrence.range.as_mut() else {
        return Ok(None);
    };
    range.start_date = Some(anchor.start.to_string());
    Ok(Some(recurrence))
}

fn graph_event_from_patch(
    patch: &EventPatch,
    series: CurrentSeries<'_>,
) -> Result<GraphEventPatch, AccountError> {
    let recurrence = patch_recurrence(patch, series)?;
    // Effective all-day flag for the time emission. With no explicit
    // `is_all_day`, a date-only start/end value infers all-day. The same
    // value must then drive both the `start`/`end` dateTime shape AND the
    // top-level `isAllDay` field: if `graph_time` writes a midnight
    // `dateTime` (the all-day shape) while `isAllDay` stays unset, Graph
    // keeps a previously-timed event timed at midnight instead of
    // converting it to all-day.
    // An empty `end` says "no end" (see `reject_unusable_write_times`): on a
    // patch it leaves the stored end alone, so it is dropped here before it
    // can drive the all-day inference or be sent as `dateTime: ""`.
    let end = patch.end.as_ref().filter(|time| !is_blank_time(time));
    let effective_all_day = patch.is_all_day.unwrap_or_else(|| {
        patch
            .start
            .as_ref()
            .map(|time| is_all_day_value(&time.value))
            .or_else(|| end.map(|time| is_all_day_value(&time.value)))
            .unwrap_or(false)
    });
    // Only emit `isAllDay` when the patch actually touches a time field
    // (or set it explicitly): a metadata-only patch must not flip the
    // event's all-day state as a side effect.
    let is_all_day_out = patch
        .is_all_day
        .or_else(|| (patch.start.is_some() || end.is_some()).then_some(effective_all_day));
    Ok(GraphEventPatch {
        subject: patch.title.clone().map(|value| match value {
            Some(value) => Value::String(value),
            None => Value::Null,
        }),
        body: patch
            .description
            .as_ref()
            .map(|description| match description {
                Some(description) => json!(GraphBody {
                    content_type: Some("text".to_string()),
                    content: Some(description.clone()),
                }),
                None => Value::Null,
            }),
        location: patch.location.as_ref().map(|location| match location {
            Some(location) => json!(GraphLocation {
                display_name: Some(location.clone()),
            }),
            None => Value::Null,
        }),
        start: patch
            .start
            .as_ref()
            .map(|time| graph_time(time, effective_all_day)),
        end: end.map(|time| graph_time(time, effective_all_day)),
        is_all_day: is_all_day_out,
        show_as: patch
            .availability
            .map(|availability| show_as(availability).to_string()),
        sensitivity: patch
            .visibility
            .map(|visibility| sensitivity(visibility).to_string()),
        // An empty list is sent as `[]`: it is how a patch clears every
        // attendee, and omitting it left the old attendees in place.
        attendees: patch
            .attendees
            .as_ref()
            .map(|attendees| attendees.iter().map(graph_attendee).collect()),
        recurrence,
    })
}

fn event_url(account: &GraphAccount, calendar_id: &str, event_id: &str) -> String {
    let prefix = account.client.api_path_prefix();
    let event = bifrost_net::url::encode_path_component(event_id);
    if calendar_id == MAILBOX_SCOPE {
        // Graph event ids are mailbox-unique; the mailbox-scoped path
        // resolves the hit without knowing its hosting calendar.
        format!("{prefix}/events/{event}")
    } else {
        format!(
            "{prefix}/calendars/{}/events/{event}",
            bifrost_net::url::encode_path_component(calendar_id),
        )
    }
}

fn event_search_path(prefix: &str, calendar_id: Option<&CalendarId>) -> String {
    if let Some(calendar_id) = calendar_id {
        let calendar = bifrost_net::url::encode_path_component(&calendar_id.0);
        format!("{prefix}/calendars/{calendar}/events")
    } else {
        format!("{prefix}/events")
    }
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
        // Every id this crate hands out carries the separator, so one without
        // it is caller input this crate never minted: `Request(Malformed)`,
        // not a capability Graph lacks.
        .ok_or_else(|| {
            graph_error::invalid_argument_account_error(
                operation,
                "event",
                "Graph event id is missing calendar id",
            )
        })
}

fn event_time(time: GraphDateTime, is_all_day: bool) -> EventTime {
    let value = time.date_time.unwrap_or_default();
    EventTime {
        value: if is_all_day {
            all_day_date(&value).unwrap_or(value)
        } else {
            value
        },
        timezone: time.time_zone,
    }
}

fn graph_time(time: &EventTime, is_all_day: bool) -> GraphDateTime {
    GraphDateTime {
        date_time: Some(if is_all_day {
            graph_all_day_date_time(&time.value)
        } else {
            time.value.clone()
        }),
        time_zone: Some(graph_time_zone(time.timezone.as_deref())),
    }
}

fn graph_time_zone(timezone: Option<&str>) -> String {
    graph_time_zone_name(timezone).unwrap_or("UTC").to_string()
}

/// Refuse a timezone Graph cannot be sent, by what the refusal is about.
///
/// An IANA-shaped id (it has a `/`) missing from the outbound mapping may be a
/// perfectly real zone this crate simply has no Windows name for: the gap is
/// the client's, so it is `Unsupported`, not the caller's fault. A value that
/// is neither IANA-shaped nor a multi-word Windows id (`"foo"`, `""`) is not a
/// timezone name in any form this operation accepts: `Request(Malformed)`,
/// naming the field.
fn validate_graph_time_zone(
    timezone: Option<&str>,
    field: &'static str,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    match timezone {
        _ if graph_time_zone_name(timezone).is_some() => Ok(()),
        Some(value) if value.contains('/') => Err(unsupported_error(
            operation,
            "Graph event timezone is not in the outbound IANA-to-Windows mapping".to_string(),
        )),
        _ => Err(graph_error::invalid_argument_account_error(
            operation,
            field,
            "Graph event timezone is neither an IANA nor a Windows timezone id",
        )),
    }
}

/// The outbound IANA -> Windows time zone mapping, one row per Windows id.
/// The first IANA id of a row is the one that Windows id resolves back to
/// when an event's own zone is needed (reading a UTC RRULE `UNTIL` as a local
/// date), so it is the row's canonical zone.
const WINDOWS_ZONES: &[(&str, &[&str])] = &[
    ("UTC", &["Etc/UTC", "UTC"]),
    (
        "W. Europe Standard Time",
        &[
            "Europe/Berlin",
            "Europe/Amsterdam",
            "Europe/Busingen",
            "Europe/Oslo",
            "Europe/Rome",
            "Europe/Stockholm",
            "Europe/Vienna",
            "Europe/Zurich",
        ],
    ),
    (
        "Romance Standard Time",
        &[
            "Europe/Paris",
            "Europe/Brussels",
            "Europe/Copenhagen",
            "Europe/Madrid",
        ],
    ),
    (
        "Central Europe Standard Time",
        &[
            "Europe/Budapest",
            "Europe/Bratislava",
            "Europe/Ljubljana",
            "Europe/Prague",
            "Europe/Warsaw",
            "Europe/Zagreb",
        ],
    ),
    (
        "GMT Standard Time",
        &["Europe/London", "Europe/Dublin", "Europe/Lisbon"],
    ),
    ("GTB Standard Time", &["Europe/Bucharest", "Europe/Athens"]),
    (
        "FLE Standard Time",
        &[
            "Europe/Helsinki",
            "Europe/Kyiv",
            "Europe/Riga",
            "Europe/Sofia",
            "Europe/Tallinn",
            "Europe/Vilnius",
        ],
    ),
    ("Turkey Standard Time", &["Europe/Istanbul"]),
    ("Russian Standard Time", &["Europe/Moscow"]),
    (
        "Eastern Standard Time",
        &[
            "America/New_York",
            "America/Detroit",
            "America/Indiana/Indianapolis",
            "America/Toronto",
        ],
    ),
    ("Central Standard Time", &["America/Chicago"]),
    ("Mountain Standard Time", &["America/Denver"]),
    ("US Mountain Standard Time", &["America/Phoenix"]),
    (
        "Pacific Standard Time",
        &["America/Los_Angeles", "America/Vancouver"],
    ),
    ("Alaskan Standard Time", &["America/Anchorage"]),
    ("Hawaiian Standard Time", &["Pacific/Honolulu"]),
    ("Central Standard Time (Mexico)", &["America/Mexico_City"]),
    (
        "SA Pacific Standard Time",
        &["America/Bogota", "America/Lima", "America/Guayaquil"],
    ),
    ("Pacific SA Standard Time", &["America/Santiago"]),
    ("E. South America Standard Time", &["America/Sao_Paulo"]),
    (
        "Argentina Standard Time",
        &["America/Argentina/Buenos_Aires"],
    ),
    ("Egypt Standard Time", &["Africa/Cairo"]),
    ("South Africa Standard Time", &["Africa/Johannesburg"]),
    ("Arabian Standard Time", &["Asia/Dubai"]),
    ("Israel Standard Time", &["Asia/Jerusalem"]),
    ("Tokyo Standard Time", &["Asia/Tokyo"]),
    ("Korea Standard Time", &["Asia/Seoul"]),
    ("China Standard Time", &["Asia/Shanghai"]),
    ("Hong Kong Standard Time", &["Asia/Hong_Kong"]),
    ("Singapore Standard Time", &["Asia/Singapore"]),
    ("India Standard Time", &["Asia/Kolkata"]),
    ("E. Australia Standard Time", &["Australia/Brisbane"]),
    ("W. Australia Standard Time", &["Australia/Perth"]),
    (
        "AUS Eastern Standard Time",
        &["Australia/Sydney", "Australia/Melbourne"],
    ),
    ("New Zealand Standard Time", &["Pacific/Auckland"]),
];

fn graph_time_zone_name(timezone: Option<&str>) -> Option<&str> {
    let value = timezone.unwrap_or("UTC");
    if let Some((windows, _)) = WINDOWS_ZONES.iter().find(|(_, iana)| iana.contains(&value)) {
        return Some(*windows);
    }
    // An IANA id we don't have a Windows mapping for: reject locally.
    if value.contains('/') {
        return None;
    }
    // A slash-free string is treated as an already-Windows tz id so they
    // round-trip. Windows tz ids are multi-word (e.g. "W. Europe Standard
    // Time"); the sole single-token id is "UTC" (in the table). Reject a
    // slash-free single token as junk rather than forwarding an unresolvable
    // tz to Graph.
    value.contains(' ').then_some(value)
}

/// The IANA id an event zone name stands for: an IANA id as is, a Windows id
/// through its `WINDOWS_ZONES` row. `None` for anything else, including a
/// Windows id outside the table and Graph's `tzone://Microsoft/Custom`.
fn iana_zone_name(name: &str) -> Option<&str> {
    let iana = WINDOWS_ZONES
        .iter()
        .find(|(windows, _)| *windows == name)
        .and_then(|(_, iana)| iana.first().copied())
        .unwrap_or(name);
    (iana.contains('/') && !iana.starts_with("tzone:")).then_some(iana)
}

/// Resolve an event zone name against the tz database. `None` when the name
/// is unknown or the database lacks it.
fn resolve_time_zone(name: &str) -> Option<TimeZone> {
    if matches!(name, "UTC" | "Etc/UTC") {
        return Some(TimeZone::UTC);
    }
    TimeZone::get(iana_zone_name(name)?).ok()
}

fn all_day_date(value: &str) -> Option<String> {
    value.split_once('T').map(|(date, _)| date.to_string())
}

fn graph_all_day_date_time(value: &str) -> String {
    let date = all_day_date(value).unwrap_or_else(|| value.to_string());
    format!("{date}T00:00:00.0000000")
}

fn is_all_day_value(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
}

fn rrule_from_graph(recurrence: &GraphRecurrence) -> Option<String> {
    let pattern = recurrence.pattern.as_ref()?;
    let mut parts = Vec::new();
    match pattern.kind.as_deref()? {
        "daily" => parts.push("FREQ=DAILY".to_string()),
        "weekly" => {
            parts.push("FREQ=WEEKLY".to_string());
            if let Some(days) = pattern.days_of_week.as_ref() {
                let byday = days
                    .iter()
                    .filter_map(|day| graph_day_to_rrule(day))
                    .collect::<Vec<_>>();
                if !byday.is_empty() {
                    parts.push(format!("BYDAY={}", byday.join(",")));
                }
            }
        }
        "absoluteMonthly" => {
            parts.push("FREQ=MONTHLY".to_string());
            if let Some(day) = pattern.day_of_month {
                parts.push(format!("BYMONTHDAY={day}"));
            }
        }
        "relativeMonthly" => {
            parts.push("FREQ=MONTHLY".to_string());
            push_relative_rule_parts(pattern, &mut parts);
        }
        "absoluteYearly" => {
            parts.push("FREQ=YEARLY".to_string());
            if let Some(month) = pattern.month {
                parts.push(format!("BYMONTH={month}"));
            }
            if let Some(day) = pattern.day_of_month {
                parts.push(format!("BYMONTHDAY={day}"));
            }
        }
        "relativeYearly" => {
            parts.push("FREQ=YEARLY".to_string());
            if let Some(month) = pattern.month {
                parts.push(format!("BYMONTH={month}"));
            }
            push_relative_rule_parts(pattern, &mut parts);
        }
        _ => return None,
    }
    if let Some(interval) = pattern.interval.filter(|interval| *interval > 1) {
        parts.push(format!("INTERVAL={interval}"));
        // The week start changes a weekly series only when it skips weeks.
        // RFC 5545 assumes Monday; Graph's documented default is Sunday.
        if pattern.kind.as_deref() == Some("weekly") {
            let week_start = pattern.first_day_of_week.as_deref().unwrap_or("sunday");
            if let Some(day) = graph_day_to_rrule(week_start).filter(|day| *day != "MO") {
                parts.push(format!("WKST={day}"));
            }
        }
    }
    if let Some(range) = recurrence.range.as_ref() {
        match range.kind.as_deref() {
            Some("numbered") => {
                if let Some(count) = range.number_of_occurrences {
                    parts.push(format!("COUNT={count}"));
                }
            }
            Some("endDate") => {
                if let Some(end) = range.end_date.as_deref() {
                    parts.push(format!("UNTIL={}", end.replace('-', "")));
                }
            }
            _ => {}
        }
    }
    Some(parts.join(";"))
}

fn push_relative_rule_parts(pattern: &GraphRecurrencePattern, parts: &mut Vec<String>) {
    if let Some(days) = pattern.days_of_week.as_ref() {
        let byday = days
            .iter()
            .filter_map(|day| graph_day_to_rrule(day))
            .collect::<Vec<_>>();
        if !byday.is_empty() {
            parts.push(format!("BYDAY={}", byday.join(",")));
        }
    }
    if let Some(index) = pattern.index.as_deref().and_then(graph_index_to_setpos) {
        parts.push(format!("BYSETPOS={index}"));
    }
}

/// Why an RRULE cannot be written to Graph, classified by what the refusal is
/// about. A well-formed rule that Graph's `patternedRecurrence` (or this
/// mapping) cannot express is `Unsupported`: the gap is the provider's or this
/// client's, not the caller's. A value that is not an RFC 5545 RRULE at all is
/// the caller's malformed input, `Request(Malformed)` naming
/// `recurrence.rrule`. Either way the write is refused: dropping the rule would
/// turn a recurring event into a one-off, and dropping one part of it would
/// write a different series than the one asked for.
#[derive(Debug)]
enum RecurrenceRefusal {
    Unsupported(String),
    Malformed {
        field: &'static str,
        message: String,
    },
}

impl RecurrenceRefusal {
    fn into_account_error(self, operation: AccountOperation) -> AccountError {
        match self {
            Self::Unsupported(message) => unsupported_error(operation, message),
            Self::Malformed { field, message } => {
                graph_error::invalid_argument_account_error(operation, field, message)
            }
        }
    }
}

fn unexpressible(message: impl Into<String>) -> RecurrenceRefusal {
    RecurrenceRefusal::Unsupported(message.into())
}

fn malformed_rrule(message: impl Into<String>) -> RecurrenceRefusal {
    RecurrenceRefusal::Malformed {
        field: "recurrence.rrule",
        message: message.into(),
    }
}

/// Graph's pattern shape for a validated RRULE. A `None` is a value RFC 5545
/// takes from DTSTART when the rule leaves it out; `anchored` fills it from
/// the event start.
#[derive(Debug)]
enum PatternShape {
    Daily,
    /// `None` is the start's weekday.
    Weekly(Option<Vec<&'static str>>),
    /// `None` is the start's day of the month.
    AbsoluteMonthly(Option<u32>),
    RelativeMonthly {
        days: Vec<&'static str>,
        index: &'static str,
    },
    /// `None`s are the start's month and day.
    AbsoluteYearly {
        month: Option<u32>,
        day: Option<u32>,
    },
    RelativeYearly {
        month: u32,
        days: Vec<&'static str>,
        index: &'static str,
    },
}

#[derive(Debug, Clone, Copy)]
enum RRuleUntil {
    /// A DATE, or a floating DATE-TIME: already the event's local date.
    Local(civil::Date),
    /// A UTC DATE-TIME, whose local date depends on the event's time zone.
    Utc(civil::DateTime),
}

/// A validated RRULE in Graph's terms, still waiting for the event start it
/// is anchored on.
#[derive(Debug)]
struct GraphRecurrenceRule {
    shape: PatternShape,
    interval: u32,
    /// Graph name of the RRULE's WKST, RFC 5545's default Monday when absent.
    first_day_of_week: &'static str,
    count: Option<u32>,
    until: Option<RRuleUntil>,
}

/// What a rule is anchored on: the event's local start date, and the zone a
/// UTC `UNTIL` is read in. `zone` is `None` when the event's zone cannot be
/// resolved, which leaves such an `UNTIL` on its UTC date.
#[derive(Debug, Clone)]
struct RecurrenceAnchor {
    start: civil::Date,
    zone: Option<TimeZone>,
}

impl RecurrenceAnchor {
    fn from_event_time(time: &EventTime) -> Result<Self, RecurrenceRefusal> {
        let start = parse_start_date(&time.value).ok_or_else(|| RecurrenceRefusal::Malformed {
            field: "start",
            message: "Graph recurrence needs the event start to begin with a YYYY-MM-DD date"
                .to_string(),
        })?;
        Ok(Self {
            start,
            zone: resolve_time_zone(time.timezone.as_deref().unwrap_or("UTC")),
        })
    }
}

fn parse_start_date(value: &str) -> Option<civil::Date> {
    value.get(..10)?.parse().ok()
}

const GRAPH_WEEKDAYS: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

fn graph_weekday(date: civil::Date) -> &'static str {
    GRAPH_WEEKDAYS[usize::from(date.weekday().to_sunday_zero_offset().unsigned_abs())]
}

/// The Gregorian calendar repeats every 400 years: a month's length depends
/// only on its month and on its year modulo 400.
const GREGORIAN_CYCLE_MONTHS: i64 = 400 * 12;

/// Days in a month, counted by its index `year * 12 + month - 1`. The year is
/// reduced into one Gregorian cycle first, which leaves the answer unchanged
/// and keeps any index inside the range `civil::Date` accepts.
fn days_in_indexed_month(index: i64) -> i64 {
    let year = 2000 + (index.div_euclid(12) - 2000).rem_euclid(400);
    let month = index.rem_euclid(12) + 1;
    match (i16::try_from(year), i8::try_from(month)) {
        (Ok(year), Ok(month)) => civil::Date::new(year, month, 1)
            .map(|first| i64::from(first.days_in_month()))
            .unwrap_or(31),
        _ => 31,
    }
}

/// Refuse an absolute series that visits a month lacking its day.
///
/// RFC 5545 skips a recurrence date that does not exist (the 31st of a
/// 30-day month, February 29th outside a leap year). Graph's absolute
/// patterns do not skip: Exchange moves the occurrence to the month's last
/// day. Writing such a series would add occurrences the rule never asked for,
/// so it is refused. Only the months the series actually visits count: the
/// 31st every sixth month from July lands on July and January only and is
/// written as is.
///
/// The walk runs from the start's month in `step_months` steps; a candidate
/// before the start is not an occurrence, `COUNT` counts only real
/// occurrences, and `UNTIL` ends the series. A month's length depends only on
/// its position in the 400-year cycle, and the positions a series visits
/// repeat after at most one cycle's worth of steps, so walking that many
/// steps sees every month the series can ever reach: an open-ended or long
/// series is decided exactly, not by a horizon.
fn check_absolute_series(
    start: civil::Date,
    first_month: u32,
    step_months: u32,
    day: u32,
    count: Option<u32>,
    until: Option<civil::Date>,
) -> Result<(), RecurrenceRefusal> {
    let month_index = |date: civil::Date| i64::from(date.year()) * 12 + i64::from(date.month()) - 1;
    let first = i64::from(start.year()) * 12 + i64::from(first_month) - 1;
    let start_index = month_index(start);
    let until = until.map(|until| (month_index(until), i64::from(until.day())));
    let step = i64::from(step_months.max(1));
    let day = i64::from(day);
    let mut occurrences = 0u32;
    for steps in 0..=GREGORIAN_CYCLE_MONTHS {
        if count.is_some_and(|count| occurrences >= count) {
            break;
        }
        let index = first + steps * step;
        if until.is_some_and(|(until_index, _)| index > until_index) {
            break;
        }
        // A yearly series starts its walk at BYMONTH in the start's year,
        // which can lie before the start's month: not an occurrence, whether
        // or not that month has the day.
        if index < start_index {
            continue;
        }
        let in_until_month = until.filter(|(until_index, _)| index == *until_index);
        let last_day = days_in_indexed_month(index);
        if day > last_day {
            // Graph puts this occurrence on the month's last day; past UNTIL
            // it would not be written at all, and the series is over.
            if in_until_month.is_some_and(|(_, until_day)| last_day > until_day) {
                break;
            }
            // A date the month does not have always falls after a start in
            // that month, so it is a skipped occurrence, never a pre-start one.
            let (year, month) = (index.div_euclid(12), index.rem_euclid(12) + 1);
            return Err(unexpressible(format!(
                "Graph recurrence cannot express day {day} in a series that reaches \
                 {year}-{month:02}, which lacks it: Graph moves such an occurrence to the \
                 month's last day where RFC 5545 skips it"
            )));
        }
        let before_start = index == start_index && day < i64::from(start.day());
        let after_until = in_until_month.is_some_and(|(_, until_day)| day > until_day);
        if !before_start && !after_until {
            occurrences += 1;
        }
    }
    Ok(())
}

impl GraphRecurrenceRule {
    fn anchored(self, anchor: &RecurrenceAnchor) -> Result<GraphRecurrence, RecurrenceRefusal> {
        let start = anchor.start;
        let start_day = u32::from(start.day().unsigned_abs());
        let start_month = u32::from(start.month().unsigned_abs());
        let names =
            |days: Vec<&'static str>| Some(days.into_iter().map(ToString::to_string).collect());
        let mut pattern = GraphRecurrencePattern {
            kind: None,
            interval: Some(self.interval),
            month: None,
            day_of_month: None,
            days_of_week: None,
            first_day_of_week: None,
            index: None,
        };
        let until = self.until.map(|until| match until {
            RRuleUntil::Local(date) => date,
            RRuleUntil::Utc(at) => anchor
                .zone
                .as_ref()
                .and_then(|zone| {
                    let instant = Offset::UTC.to_timestamp(at).ok()?;
                    Some(zone.to_datetime(instant).date())
                })
                .unwrap_or_else(|| at.date()),
        });
        let kind = match self.shape {
            PatternShape::Daily => "daily",
            PatternShape::Weekly(days) => {
                pattern.days_of_week = names(days.unwrap_or_else(|| vec![graph_weekday(start)]));
                pattern.first_day_of_week = Some(self.first_day_of_week.to_string());
                "weekly"
            }
            PatternShape::AbsoluteMonthly(day) => {
                let day = day.unwrap_or(start_day);
                check_absolute_series(start, start_month, self.interval, day, self.count, until)?;
                pattern.day_of_month = Some(day);
                "absoluteMonthly"
            }
            PatternShape::RelativeMonthly { days, index } => {
                pattern.days_of_week = names(days);
                pattern.index = Some(index.to_string());
                "relativeMonthly"
            }
            PatternShape::AbsoluteYearly { month, day } => {
                let month = month.unwrap_or(start_month);
                let day = day.unwrap_or(start_day);
                check_absolute_series(
                    start,
                    month,
                    self.interval.saturating_mul(12),
                    day,
                    self.count,
                    until,
                )?;
                pattern.month = Some(month);
                pattern.day_of_month = Some(day);
                "absoluteYearly"
            }
            PatternShape::RelativeYearly { month, days, index } => {
                pattern.month = Some(month);
                pattern.days_of_week = names(days);
                pattern.index = Some(index.to_string());
                "relativeYearly"
            }
        };
        pattern.kind = Some(kind.to_string());
        let range_kind = if self.count.is_some() {
            "numbered"
        } else if until.is_some() {
            "endDate"
        } else {
            "noEnd"
        };
        Ok(GraphRecurrence {
            pattern: Some(pattern),
            range: Some(GraphRecurrenceRange {
                kind: Some(range_kind.to_string()),
                start_date: Some(start.to_string()),
                end_date: until.as_ref().map(ToString::to_string),
                recurrence_time_zone: None,
                number_of_occurrences: self.count,
            }),
        })
    }
}

#[cfg(test)]
fn graph_recurrence_from_rrule(
    rrule: &str,
    start_date: &str,
) -> Result<GraphRecurrence, RecurrenceRefusal> {
    let anchor = RecurrenceAnchor {
        start: parse_start_date(start_date)
            .ok_or_else(|| malformed_rrule("test start is not a date"))?,
        zone: None,
    };
    graph_recurrence_rule(rrule)?.anchored(&anchor)
}

/// Map an RRULE onto a Graph recurrence pattern, or refuse it.
///
/// Every part must land in the pattern with its meaning intact. Equivalent
/// shapes are rewritten: every-day-on-these-weekdays (DAILY with BYDAY) and
/// every-such-weekday of each month or year are weekly patterns, a lone
/// ordinal weekday (`2MO`, `-1FR`) is a relative pattern's index, and values
/// RFC 5545 takes from DTSTART come from the event start. A part Graph would
/// ignore for the chosen pattern type (BYDAY on an absolute pattern, BYMONTH
/// on anything but yearly, BYSETPOS on an absolute pattern) would widen or
/// narrow the series without the caller's knowledge, so it is refused rather
/// than sent.
fn graph_recurrence_rule(rrule: &str) -> Result<GraphRecurrenceRule, RecurrenceRefusal> {
    let parsed = ParsedRRule::parse(rrule)?;
    let frequency = parsed
        .value("FREQ")
        .ok_or_else(|| malformed_rrule("RRULE has no FREQ"))?
        .to_ascii_uppercase();
    match frequency.as_str() {
        "DAILY" | "WEEKLY" | "MONTHLY" | "YEARLY" => {}
        "SECONDLY" | "MINUTELY" | "HOURLY" => {
            return Err(unexpressible(format!(
                "Graph recurrence has no FREQ={frequency}"
            )));
        }
        _ => {
            return Err(malformed_rrule(format!(
                "RRULE FREQ {frequency:?} is not an RFC 5545 frequency"
            )));
        }
    }
    let interval = parsed
        .value("INTERVAL")
        .map(|value| rrule_positive(value, "INTERVAL"))
        .transpose()?
        .unwrap_or(1);
    let count = parsed
        .value("COUNT")
        .map(|value| rrule_positive(value, "COUNT"))
        .transpose()?;
    let until = parsed.value("UNTIL").map(parse_rrule_until).transpose()?;
    if count.is_some() && until.is_some() {
        return Err(malformed_rrule("RRULE sets both COUNT and UNTIL"));
    }
    let month = rrule_single_number(&parsed, "BYMONTH", 1..=12)?;
    let day_of_month = rrule_single_number(&parsed, "BYMONTHDAY", -31..=31)?;
    if day_of_month.is_some_and(|day| day < 0) {
        return Err(unexpressible(
            "Graph recurrence cannot count BYMONTHDAY from the end of the month",
        ));
    }
    let set_pos = rrule_single_number(&parsed, "BYSETPOS", -366..=366)?;
    let index = set_pos
        .map(|position| {
            rrule_setpos_to_graph(position).ok_or_else(|| {
                unexpressible(format!(
                    "Graph recurrence index has no BYSETPOS={position}; it holds 1 to 4 or -1"
                ))
            })
        })
        .transpose()?;
    let month = month.map(i32::unsigned_abs);
    let day_of_month = day_of_month.map(i32::unsigned_abs);
    let days = parsed
        .value("BYDAY")
        .map(|value| parse_rrule_byday(value, &frequency))
        .transpose()?;
    // WKST is carried as Graph's `firstDayOfWeek` on weekly patterns, the
    // only Graph pattern it changes; elsewhere RFC 5545 gives it meaning only
    // alongside BYWEEKNO, which is refused above.
    let first_day_of_week = parsed
        .value("WKST")
        .map(|week_start| {
            rrule_day_to_graph(&week_start.to_ascii_uppercase()).ok_or_else(|| {
                malformed_rrule(format!("RRULE WKST {week_start:?} is not a weekday"))
            })
        })
        .transpose()?
        .unwrap_or("monday");

    let forbid = |key: &str, present: bool| {
        if present {
            Err(unexpressible(format!(
                "Graph recurrence cannot express {key} with FREQ={frequency} in this shape"
            )))
        } else {
            Ok(())
        }
    };
    let shape = match frequency.as_str() {
        "DAILY" => {
            forbid("BYMONTH", month.is_some())?;
            forbid("BYMONTHDAY", day_of_month.is_some())?;
            forbid("BYSETPOS", index.is_some())?;
            match days {
                None => PatternShape::Daily,
                // Every day, kept to the listed weekdays, is a weekly pattern
                // on those days. With INTERVAL > 1 the day stride and the week
                // no longer line up.
                Some(days) if interval == 1 => PatternShape::Weekly(Some(plain_days(&days))),
                Some(_) => {
                    return Err(unexpressible(
                        "Graph recurrence cannot express BYDAY on a FREQ=DAILY rule with \
                         INTERVAL > 1",
                    ));
                }
            }
        }
        "WEEKLY" => {
            forbid("BYMONTH", month.is_some())?;
            forbid("BYMONTHDAY", day_of_month.is_some())?;
            forbid("BYSETPOS", index.is_some())?;
            PatternShape::Weekly(days.as_deref().map(plain_days))
        }
        "MONTHLY" => {
            // BYMONTH on a MONTHLY rule keeps some months only; Graph's
            // monthly patterns run every month.
            forbid("BYMONTH", month.is_some())?;
            if let Some(day) = day_of_month {
                forbid("BYDAY", days.is_some())?;
                forbid("BYSETPOS", index.is_some())?;
                PatternShape::AbsoluteMonthly(Some(day))
            } else if let Some(days) = days {
                match relative_days(&days, index)? {
                    Some((days, index)) => PatternShape::RelativeMonthly { days, index },
                    // Every listed weekday of every month is every such
                    // weekday: a weekly pattern.
                    None if interval == 1 => PatternShape::Weekly(Some(plain_days(&days))),
                    None => {
                        return Err(unexpressible(
                            "Graph recurrence cannot express every BYDAY weekday of every n-th \
                             month",
                        ));
                    }
                }
            } else {
                forbid("BYSETPOS", index.is_some())?;
                PatternShape::AbsoluteMonthly(None)
            }
        }
        // YEARLY; FREQ was validated above.
        _ => {
            if let Some(day) = day_of_month {
                forbid("BYDAY", days.is_some())?;
                forbid("BYSETPOS", index.is_some())?;
                let Some(month) = month else {
                    return Err(unexpressible(
                        "Graph recurrence cannot express BYMONTHDAY in every month of a \
                         FREQ=YEARLY rule without BYMONTH",
                    ));
                };
                PatternShape::AbsoluteYearly {
                    month: Some(month),
                    day: Some(day),
                }
            } else if let Some(days) = days {
                let plain = index.is_none() && days.iter().all(|day| day.ordinal.is_none());
                match month {
                    // With BYMONTH, a BYDAY ordinal counts within the month.
                    Some(month) => match relative_days(&days, index)? {
                        Some((days, index)) => PatternShape::RelativeYearly { month, days, index },
                        None => {
                            return Err(unexpressible(
                                "Graph recurrence cannot express every BYDAY weekday of one \
                                 month",
                            ));
                        }
                    },
                    // Every listed weekday of every year is a weekly pattern.
                    None if plain && interval == 1 => PatternShape::Weekly(Some(plain_days(&days))),
                    None => {
                        return Err(unexpressible(
                            "Graph recurrence cannot express a BYDAY weekday counted through a \
                             whole year",
                        ));
                    }
                }
            } else {
                forbid("BYSETPOS", index.is_some())?;
                PatternShape::AbsoluteYearly { month, day: None }
            }
        }
    };
    Ok(GraphRecurrenceRule {
        shape,
        interval,
        first_day_of_week,
        count,
        until,
    })
}

/// One BYDAY item: a Graph weekday name and its RFC 5545 ordinal, if any.
#[derive(Debug, Clone, Copy)]
struct RRuleByDay {
    ordinal: Option<i32>,
    day: &'static str,
}

fn plain_days(days: &[RRuleByDay]) -> Vec<&'static str> {
    days.iter().map(|day| day.day).collect()
}

/// The `daysOfWeek` and `index` of a Graph relative pattern, or `None` when
/// the BYDAY list names every such weekday (no position at all).
///
/// Graph's `index` picks ONE day out of the `daysOfWeek` set, which is exactly
/// BYSETPOS over plain weekdays. An ordinal weekday maps the same way only on
/// its own: `1MO,1TU` is the first Monday AND the first Tuesday, two
/// occurrences, where Graph's first of Monday-or-Tuesday is one.
fn relative_days(
    days: &[RRuleByDay],
    index: Option<&'static str>,
) -> Result<Option<(Vec<&'static str>, &'static str)>, RecurrenceRefusal> {
    if days.iter().all(|day| day.ordinal.is_none()) {
        return Ok(index.map(|index| (plain_days(days), index)));
    }
    match days {
        [
            RRuleByDay {
                ordinal: Some(ordinal),
                day,
            },
        ] if index.is_none() => rrule_setpos_to_graph(*ordinal)
            .map(|index| Some((vec![*day], index)))
            .ok_or_else(|| {
                unexpressible(format!(
                    "Graph recurrence index has no position {ordinal}; it holds 1 to 4 or -1"
                ))
            }),
        _ => Err(unexpressible(
            "Graph recurrence cannot express several ordinal BYDAY values, or an ordinal BYDAY \
             with BYSETPOS",
        )),
    }
}

fn rrule_positive(value: &str, key: &str) -> Result<u32, RecurrenceRefusal> {
    value
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| malformed_rrule(format!("RRULE {key} {value:?} is not a positive integer")))
}

/// Parse a BYxxx list Graph holds as one value. Each item must be a non-zero
/// integer within `range`; more than one item is a list Graph has no field
/// for.
fn rrule_single_number(
    parsed: &ParsedRRule<'_>,
    key: &str,
    range: std::ops::RangeInclusive<i32>,
) -> Result<Option<i32>, RecurrenceRefusal> {
    let Some(value) = parsed.value(key) else {
        return Ok(None);
    };
    let mut numbers = Vec::new();
    for item in value.split(',') {
        let number = item
            .parse::<i32>()
            .ok()
            .filter(|number| *number != 0 && range.contains(number))
            .ok_or_else(|| {
                malformed_rrule(format!("RRULE {key} value {item:?} is out of range"))
            })?;
        numbers.push(number);
    }
    match numbers.as_slice() {
        [number] => Ok(Some(*number)),
        _ => Err(unexpressible(format!(
            "Graph recurrence holds a single {key} value"
        ))),
    }
}

/// Parse a BYDAY list. An ordinal prefix (`2MO`, `-1FR`) is not valid on a
/// DAILY or WEEKLY rule at all.
fn parse_rrule_byday(value: &str, frequency: &str) -> Result<Vec<RRuleByDay>, RecurrenceRefusal> {
    let mut days = Vec::new();
    for item in value.split(',') {
        let split = item
            .len()
            .checked_sub(2)
            .filter(|at| item.is_char_boundary(*at));
        let Some((ordinal, day)) = split.map(|at| item.split_at(at)) else {
            return Err(malformed_rrule(format!(
                "RRULE BYDAY value {item:?} is not a weekday"
            )));
        };
        let Some(day) = rrule_day_to_graph(&day.to_ascii_uppercase()) else {
            return Err(malformed_rrule(format!(
                "RRULE BYDAY value {item:?} is not a weekday"
            )));
        };
        let ordinal = if ordinal.is_empty() {
            None
        } else {
            let parsed = ordinal
                .parse::<i32>()
                .ok()
                .filter(|ordinal| *ordinal != 0 && (-53..=53).contains(ordinal));
            if parsed.is_none() || matches!(frequency, "DAILY" | "WEEKLY") {
                return Err(malformed_rrule(format!(
                    "RRULE BYDAY value {item:?} has an invalid ordinal for FREQ={frequency}"
                )));
            }
            parsed
        };
        days.push(RRuleByDay { ordinal, day });
    }
    Ok(days)
}

fn graph_day_to_rrule(day: &str) -> Option<&'static str> {
    match day {
        "sunday" => Some("SU"),
        "monday" => Some("MO"),
        "tuesday" => Some("TU"),
        "wednesday" => Some("WE"),
        "thursday" => Some("TH"),
        "friday" => Some("FR"),
        "saturday" => Some("SA"),
        _ => None,
    }
}

fn rrule_day_to_graph(day: &str) -> Option<&'static str> {
    match day {
        "SU" => Some("sunday"),
        "MO" => Some("monday"),
        "TU" => Some("tuesday"),
        "WE" => Some("wednesday"),
        "TH" => Some("thursday"),
        "FR" => Some("friday"),
        "SA" => Some("saturday"),
        _ => None,
    }
}

fn graph_index_to_setpos(index: &str) -> Option<i32> {
    match index {
        "first" => Some(1),
        "second" => Some(2),
        "third" => Some(3),
        "fourth" => Some(4),
        "last" => Some(-1),
        _ => None,
    }
}

fn rrule_setpos_to_graph(position: i32) -> Option<&'static str> {
    match position {
        1 => Some("first"),
        2 => Some("second"),
        3 => Some("third"),
        4 => Some("fourth"),
        -1 => Some("last"),
        _ => None,
    }
}

/// RFC 5545 UNTIL is a DATE (`YYYYMMDD`), a floating DATE-TIME
/// (`YYYYMMDDTHHMMSS`) or a UTC DATE-TIME (the same with `Z`). Graph's range
/// end is a local date, which a UTC value only yields once the event's time
/// zone is known.
fn parse_rrule_until(value: &str) -> Result<RRuleUntil, RecurrenceRefusal> {
    let number = |range: std::ops::Range<usize>| -> Option<i16> {
        let digits = value.get(range)?;
        if !digits.as_bytes().iter().all(u8::is_ascii_digit) {
            return None;
        }
        digits.parse().ok()
    };
    let two = |at: usize| number(at..at + 2).and_then(|value| i8::try_from(value).ok());
    let date = || civil::Date::new(number(0..4)?, two(4)?, two(6)?).ok();
    let time = || civil::Time::new(two(9)?, two(11)?, two(13)?, 0).ok();
    let bytes = value.as_bytes();
    let parsed = match (bytes.len(), bytes.get(8), bytes.get(15)) {
        (8, _, _) => date().map(RRuleUntil::Local),
        (15, Some(b'T'), _) => date().zip(time()).map(|(date, _)| RRuleUntil::Local(date)),
        (16, Some(b'T'), Some(b'Z')) => date()
            .zip(time())
            .map(|(date, time)| RRuleUntil::Utc(date.to_datetime(time))),
        _ => None,
    };
    parsed.ok_or_else(|| {
        malformed_rrule(format!(
            "RRULE UNTIL {value:?} is not an RFC 5545 date or date-time"
        ))
    })
}

/// RRULE parts this mapping translates into a Graph pattern.
const RRULE_MAPPED_PARTS: &[&str] = &[
    "FREQ",
    "INTERVAL",
    "BYMONTH",
    "BYMONTHDAY",
    "BYDAY",
    "BYSETPOS",
    "COUNT",
    "UNTIL",
    "WKST",
];

/// Well-formed RRULE parts (RFC 5545, plus RFC 7529's RSCALE and SKIP) that
/// Graph's `patternedRecurrence` has no field for.
const RRULE_UNMAPPED_PARTS: &[&str] = &[
    "BYSECOND",
    "BYMINUTE",
    "BYHOUR",
    "BYYEARDAY",
    "BYWEEKNO",
    "RSCALE",
    "SKIP",
];

struct ParsedRRule<'a> {
    parts: Vec<(String, &'a str)>,
}

impl<'a> ParsedRRule<'a> {
    fn parse(value: &'a str) -> Result<Self, RecurrenceRefusal> {
        let mut parts: Vec<(String, &'a str)> = Vec::new();
        // An empty segment (a trailing `;`) carries no rule part, so skipping
        // it drops nothing.
        for part in value.split(';').filter(|part| !part.is_empty()) {
            let Some((key, value)) = part.split_once('=') else {
                return Err(malformed_rrule(format!(
                    "RRULE part {part:?} is not NAME=VALUE"
                )));
            };
            let key = key.to_ascii_uppercase();
            if value.is_empty() {
                return Err(malformed_rrule(format!("RRULE part {key} has no value")));
            }
            if parts.iter().any(|(seen, _)| *seen == key) {
                return Err(malformed_rrule(format!("RRULE repeats {key}")));
            }
            if RRULE_UNMAPPED_PARTS.contains(&key.as_str()) || key.starts_with("X-") {
                return Err(unexpressible(format!(
                    "Graph recurrence cannot express RRULE part {key}"
                )));
            }
            if !RRULE_MAPPED_PARTS.contains(&key.as_str()) {
                return Err(malformed_rrule(format!(
                    "RRULE part {key} is not an RFC 5545 rule part"
                )));
            }
            parts.push((key, value));
        }
        Ok(Self { parts })
    }

    fn value(&self, key: &str) -> Option<&'a str> {
        self.parts
            .iter()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, value)| *value)
    }
}

fn empty_time() -> EventTime {
    EventTime {
        value: String::new(),
        timezone: None,
    }
}

fn attendee_from_graph(attendee: GraphAttendee) -> Option<EventAttendee> {
    let email = attendee.email_address?;
    Some(EventAttendee {
        email: email.address?,
        name: email.name,
        role: if attendee.kind.as_deref() == Some("optional") {
            AttendeeRole::Optional
        } else if attendee.kind.as_deref() == Some("resource") {
            AttendeeRole::Resource
        } else {
            AttendeeRole::Required
        },
        status: rsvp_status(
            attendee
                .status
                .and_then(|status| status.response)
                .as_deref(),
        ),
    })
}

fn graph_attendee(attendee: &EventAttendee) -> GraphAttendee {
    GraphAttendee {
        email_address: Some(GraphEmailAddress {
            address: Some(attendee.email.clone()),
            name: attendee.name.clone(),
        }),
        kind: Some(
            match attendee.role {
                AttendeeRole::Optional => "optional",
                AttendeeRole::Resource => "resource",
                AttendeeRole::Required | AttendeeRole::Chair | AttendeeRole::Unknown => "required",
                _ => "required",
            }
            .to_string(),
        ),
        status: Some(GraphResponseStatus {
            response: Some(rsvp_value(attendee.status).to_string()),
        }),
    }
}

fn event_matches(event: &CalendarEvent, needle: &str) -> bool {
    needle.is_empty()
        || event
            .title
            .as_deref()
            .is_some_and(|value| contains(value, needle))
        || event
            .description
            .as_deref()
            .is_some_and(|value| contains(value, needle))
        || event
            .location
            .as_deref()
            .is_some_and(|value| contains(value, needle))
}

fn contains(value: &str, needle: &str) -> bool {
    value.to_ascii_lowercase().contains(needle)
}

fn availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default() {
        "free" => EventAvailability::Free,
        "tentative" => EventAvailability::Tentative,
        "oof" => EventAvailability::OutOfOffice,
        "busy" | "workingElsewhere" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn show_as(value: EventAvailability) -> &'static str {
    match value {
        EventAvailability::Free => "free",
        EventAvailability::Tentative => "tentative",
        EventAvailability::OutOfOffice => "oof",
        EventAvailability::Busy | EventAvailability::Unknown => "busy",
        _ => "busy",
    }
}

fn visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default() {
        "private" => EventVisibility::Private,
        "confidential" => EventVisibility::Confidential,
        "normal" => EventVisibility::Default,
        _ => EventVisibility::Default,
    }
}

fn sensitivity(value: EventVisibility) -> &'static str {
    match value {
        EventVisibility::Private => "private",
        EventVisibility::Confidential => "confidential",
        EventVisibility::Public | EventVisibility::Default => "normal",
        _ => "normal",
    }
}

fn rsvp_status(value: Option<&str>) -> RsvpStatus {
    match value.unwrap_or_default() {
        "accepted" => RsvpStatus::Accepted,
        "declined" => RsvpStatus::Declined,
        "tentativelyAccepted" => RsvpStatus::Tentative,
        "notResponded" => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
}

fn rsvp_value(value: RsvpStatus) -> &'static str {
    match value {
        RsvpStatus::Accepted => "accepted",
        RsvpStatus::Declined => "declined",
        RsvpStatus::Tentative => "tentativelyAccepted",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown | RsvpStatus::Delegated => "notResponded",
        _ => "notResponded",
    }
}

fn rsvp_action(value: RsvpStatus) -> Result<&'static str, AccountError> {
    match value {
        RsvpStatus::Accepted => Ok("accept"),
        RsvpStatus::Declined => Ok("decline"),
        RsvpStatus::Tentative => Ok("tentativelyAccept"),
        RsvpStatus::NeedsAction | RsvpStatus::Delegated | RsvpStatus::Unknown => {
            Err(unsupported_error(
                AccountOperation::EventRsvp,
                "Graph RSVP only supports accepted, declined, or tentative".to_string(),
            ))
        }
        _ => Err(unsupported_error(
            AccountOperation::EventRsvp,
            "Graph RSVP status is unsupported".to_string(),
        )),
    }
}

fn non_empty<T>(iter: impl Iterator<Item = T>) -> Option<Vec<T>> {
    let values = iter.collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

fn into_error(error: crate::error::GraphError, operation: AccountOperation) -> AccountError {
    graph_error::into_account_error(error, GraphErrorContext::graph(operation))
}

/// A request Graph cannot express at all (a server-derived field, a status or
/// RSVP value with no Graph equivalent, a timezone outside the outbound
/// mapping): `Unsupported`. Malformed caller input is `Request(Malformed)`
/// instead, through `graph_error::invalid_argument_account_error`.
fn unsupported_error(operation: AccountOperation, message: String) -> AccountError {
    graph_error::unsupported_account_error(operation)
        .into_builder()
        .text(bifrost_types::DiagnosticText::support_only(message))
        .try_build()
        .expect("valid account error classification")
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphCalendar {
    id: String,
    name: Option<String>,
    can_edit: Option<bool>,
    is_default_calendar: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchRequest<'a> {
    requests: Vec<GraphSearchEntityRequest<'a>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchEntityRequest<'a> {
    entity_types: Vec<&'a str>,
    query: GraphSearchQuery,
    from: u32,
    size: u32,
    fields: &'a [&'a str],
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchQuery {
    query_string: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchResponse {
    value: Vec<GraphSearchSet>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchSet {
    hits_containers: Vec<GraphSearchHitsContainer>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchHitsContainer {
    #[serde(default)]
    hits: Vec<GraphSearchHit>,
    more_results_available: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchHit {
    resource: Option<GraphEvent>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphEvent {
    id: String,
    subject: Option<String>,
    body: Option<GraphBody>,
    location: Option<GraphLocation>,
    start: Option<GraphDateTime>,
    end: Option<GraphDateTime>,
    is_all_day: Option<bool>,
    show_as: Option<String>,
    sensitivity: Option<String>,
    organizer: Option<GraphRecipient>,
    attendees: Option<Vec<GraphAttendee>>,
    /// Present only on occurrence / exception events: the instant the
    /// occurrence ORIGINALLY started, before any override moved it -
    /// Graph's analog of iCalendar RECURRENCE-ID and of Google's
    /// `originalStartTime`.
    original_start: Option<String>,
    recurrence: Option<GraphRecurrence>,
    web_link: Option<String>,
    response_status: Option<Value>,
    is_cancelled: Option<bool>,
    change_key: Option<String>,
    /// The zone the event was created in (a Windows id, usually): the zone a
    /// UTC RRULE `UNTIL` is read in when a patch does not restate `start`.
    original_start_time_zone: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphEventPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<GraphDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<GraphDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_all_day: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    show_as: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sensitivity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attendees: Option<Vec<GraphAttendee>>,
    /// `Some(None)` serializes as `null`, which clears the recurrence.
    #[serde(skip_serializing_if = "Option::is_none")]
    recurrence: Option<Option<GraphRecurrence>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRsvpAction {
    comment: Option<String>,
    send_response: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphDateTime {
    #[serde(rename = "dateTime", skip_serializing_if = "Option::is_none")]
    date_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_zone: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphLocation {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecipient {
    email_address: Option<GraphEmailAddress>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphEmailAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphAttendee {
    #[serde(skip_serializing_if = "Option::is_none")]
    email_address: Option<GraphEmailAddress>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<GraphResponseStatus>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphResponseStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecurrence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pattern: Option<GraphRecurrencePattern>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range: Option<GraphRecurrenceRange>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecurrencePattern {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    month: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    day_of_month: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    days_of_week: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_day_of_week: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecurrenceRange {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recurrence_time_zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    number_of_occurrences: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph_time(value: &str) -> GraphDateTime {
        GraphDateTime {
            date_time: Some(value.to_string()),
            time_zone: Some("UTC".to_string()),
        }
    }

    #[test]
    fn graph_calendar_does_not_project_enum_color_as_shared_color() {
        let calendar = calendar_from_graph(GraphCalendar {
            id: "cal".to_string(),
            name: Some("Calendar".to_string()),
            can_edit: Some(true),
            is_default_calendar: Some(true),
        });

        assert_eq!(calendar.id.0, "cal");
        assert_eq!(calendar.color, None);
        assert!(calendar.is_default);
    }

    #[test]
    fn graph_event_maps_body_cancelled_and_resource_attendee() {
        let event = event_from_graph(
            "calendar".to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: Some("Planning".to_string()),
                body: Some(GraphBody {
                    content_type: Some("text".to_string()),
                    content: Some("Full description".to_string()),
                }),
                location: None,
                start: Some(graph_time("2026-06-02T12:00:00")),
                end: Some(graph_time("2026-06-02T13:00:00")),
                is_all_day: Some(false),
                show_as: Some("busy".to_string()),
                sensitivity: Some("normal".to_string()),
                organizer: None,
                attendees: Some(vec![GraphAttendee {
                    email_address: Some(GraphEmailAddress {
                        address: Some("room@example.test".to_string()),
                        name: None,
                    }),
                    kind: Some("resource".to_string()),
                    status: Some(GraphResponseStatus {
                        response: Some("accepted".to_string()),
                    }),
                }]),
                original_start: None,
                recurrence: None,
                web_link: None,
                response_status: Some(json!({"response": "tentativelyAccepted"})),
                is_cancelled: Some(true),
                change_key: Some("etag".to_string()),
                original_start_time_zone: None,
            },
        );

        assert_eq!(event.description.as_deref(), Some("Full description"));
        assert_eq!(event.status, EventStatus::Cancelled);
        assert_eq!(event.self_response, RsvpStatus::Tentative);
        assert_eq!(event.attendees[0].role, AttendeeRole::Resource);
        assert_eq!(event.attendees[0].status, RsvpStatus::Accepted);
    }

    /// The shared model documents `recurrence_id` as RECURRENCE-ID
    /// semantics: the identity of an OVERRIDDEN OCCURRENCE. Graph's
    /// carrier is `originalStart` (present only on occurrence /
    /// exception events), mirroring Google's `originalStartTime` - NOT
    /// `seriesMasterId`, which names the whole series and was the
    /// previous, misleading mapping.
    #[test]
    fn graph_recurrence_id_is_the_original_start_never_the_series_master() {
        let base = || GraphEvent {
            id: "e1".to_string(),
            subject: None,
            body: None,
            location: None,
            start: Some(graph_time("2026-06-02T12:00:00")),
            end: Some(graph_time("2026-06-02T13:00:00")),
            is_all_day: Some(false),
            show_as: None,
            sensitivity: None,
            organizer: None,
            attendees: None,
            original_start: None,
            recurrence: None,
            web_link: None,
            response_status: None,
            is_cancelled: None,
            change_key: None,
            original_start_time_zone: None,
        };

        let exception = GraphEvent {
            original_start: Some("2026-06-09T12:00:00Z".to_string()),
            ..base()
        };
        assert_eq!(
            event_from_graph("calendar".to_string(), exception)
                .recurrence
                .recurrence_id
                .as_deref(),
            Some("2026-06-09T12:00:00Z")
        );

        // A series master (or plain single event) carries no
        // originalStart and must project no recurrence_id.
        assert_eq!(
            event_from_graph("calendar".to_string(), base())
                .recurrence
                .recurrence_id,
            None
        );
    }

    #[test]
    fn graph_event_create_writes_body_and_resource_attendee() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: Some("Planning".to_string()),
            description: Some("Full description".to_string()),
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: vec![EventAttendee {
                email: "room@example.test".to_string(),
                name: None,
                role: AttendeeRole::Resource,
                status: RsvpStatus::Accepted,
            }],
            recurrence: EventRecurrence::default(),
        })
        .expect("create payload");

        assert_eq!(
            patch
                .body
                .as_ref()
                .and_then(|body| body.get("content"))
                .and_then(Value::as_str),
            Some("Full description")
        );
        assert_eq!(
            patch
                .attendees
                .as_ref()
                .and_then(|attendees| attendees.first())
                .and_then(|attendee| attendee.kind.as_deref()),
            Some("resource")
        );
    }

    #[test]
    fn graph_event_create_organizer_is_rejected() {
        let error = reject_create_organizer(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
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
    fn graph_event_status_rejects_unwritable_values() {
        assert!(
            reject_unwritable_status(EventStatus::Confirmed, AccountOperation::EventCreate).is_ok()
        );
        for status in [
            EventStatus::Cancelled,
            EventStatus::Tentative,
            EventStatus::Unknown,
        ] {
            let error = reject_unwritable_status(status, AccountOperation::EventCreate)
                .expect_err("status should be unsupported");
            assert!(matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
            ));
        }
    }

    #[test]
    fn graph_time_zone_maps_common_iana_names_to_windows() {
        assert_eq!(
            graph_time_zone(Some("Europe/Oslo")),
            "W. Europe Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Europe/Paris")),
            "Romance Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Europe/Warsaw")),
            "Central Europe Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("America/New_York")),
            "Eastern Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("America/Mexico_City")),
            "Central Standard Time (Mexico)"
        );
        assert_eq!(
            graph_time_zone(Some("Asia/Singapore")),
            "Singapore Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Australia/Perth")),
            "W. Australia Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Eastern Standard Time")),
            "Eastern Standard Time"
        );
        assert_eq!(graph_time_zone(Some("America/Unknown")), "UTC");
        assert_eq!(graph_time_zone(None), "UTC");
    }

    #[test]
    fn graph_time_zone_validation_rejects_unknown_iana_names() {
        let error = validate_graph_time_zone(
            Some("America/Unknown"),
            "start.timezone",
            AccountOperation::EventCreate,
        )
        .expect_err("unknown IANA timezone should be unsupported");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
        assert!(
            validate_graph_time_zone(
                Some("Eastern Standard Time"),
                "start.timezone",
                AccountOperation::EventCreate,
            )
            .is_ok()
        );
    }

    /// A value that is no timezone id in any form is the caller's malformed
    /// input, not a capability Graph lacks: it was `Unsupported` alongside
    /// the unmapped-IANA case, which told the operator the provider could
    /// not do something the caller never validly asked for.
    #[test]
    fn a_timezone_that_is_no_timezone_id_is_malformed_input() {
        for junk in ["foo", ""] {
            let error =
                validate_graph_time_zone(Some(junk), "end.timezone", AccountOperation::EventUpdate)
                    .expect_err("junk timezone must be refused");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                ),
                "{junk:?}"
            );
            assert_eq!(error.recovery(), &bifrost_types::RecoveryClass::ClientBug);
        }
    }

    /// An event id this crate never minted (no calendar separator) is
    /// malformed caller input, `ClientBug`, not `Unsupported`.
    #[test]
    fn an_event_id_without_its_calendar_is_malformed_input() {
        for operation in [
            AccountOperation::EventGet,
            AccountOperation::EventUpdate,
            AccountOperation::EventDelete,
            AccountOperation::EventRsvp,
        ] {
            let error = split_event_id("no-separator", operation)
                .expect_err("an unminted id must be refused");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            );
            assert_eq!(error.recovery(), &bifrost_types::RecoveryClass::ClientBug);
        }
    }

    #[test]
    fn graph_event_patch_preserves_scalar_clear_semantics() {
        let patch = graph_event_from_patch(
            &EventPatch {
                title: Some(None),
                description: Some(None),
                location: Some(Some("Room 1".to_string())),
                ..EventPatch::default()
            },
            CurrentSeries::default(),
        )
        .expect("patch payload");
        let value = serde_json::to_value(&patch).expect("patch json");

        assert!(value.get("subject").is_some_and(Value::is_null));
        assert!(value.get("body").is_some_and(Value::is_null));
        assert_eq!(
            value
                .get("location")
                .and_then(|location| location.get("displayName"))
                .and_then(Value::as_str),
            Some("Room 1")
        );
    }

    #[test]
    fn date_only_start_patch_infers_all_day_flag() {
        // A date-only start with no explicit is_all_day must emit
        // isAllDay:true alongside the midnight dateTime, else Graph keeps
        // the event timed at midnight.
        let patch = graph_event_from_patch(
            &EventPatch {
                start: Some(EventTime {
                    value: "2026-06-02".to_string(),
                    timezone: Some("UTC".to_string()),
                }),
                ..EventPatch::default()
            },
            CurrentSeries::default(),
        )
        .expect("patch payload");
        let value = serde_json::to_value(&patch).expect("patch json");
        assert_eq!(value.get("isAllDay"), Some(&json!(true)));
        assert_eq!(
            value
                .get("start")
                .and_then(|start| start.get("dateTime"))
                .and_then(Value::as_str),
            Some("2026-06-02T00:00:00.0000000")
        );
    }

    #[test]
    fn timed_start_patch_does_not_set_all_day() {
        let patch = graph_event_from_patch(
            &EventPatch {
                start: Some(EventTime {
                    value: "2026-06-02T09:30:00".to_string(),
                    timezone: Some("UTC".to_string()),
                }),
                ..EventPatch::default()
            },
            CurrentSeries::default(),
        )
        .expect("patch payload");
        let value = serde_json::to_value(&patch).expect("patch json");
        assert_eq!(value.get("isAllDay"), Some(&json!(false)));
    }

    #[test]
    fn metadata_only_patch_omits_all_day() {
        // A patch touching no time field must not flip the event's
        // all-day state.
        let patch = graph_event_from_patch(
            &EventPatch {
                title: Some(Some("Renamed".to_string())),
                ..EventPatch::default()
            },
            CurrentSeries::default(),
        )
        .expect("patch payload");
        let value = serde_json::to_value(&patch).expect("patch json");
        assert!(value.get("isAllDay").is_none());
    }

    /// FREQ=MONTHLY;BYDAY=MO (every Monday) has no Graph relative-pattern
    /// index, and Graph would silently default a missing one to "first".
    /// Every Monday of every month is every Monday, so it is written as a
    /// weekly pattern instead. Every Monday of June has no Graph shape.
    #[test]
    fn relative_monthly_without_index_is_weekly_or_refused() {
        let every_monday = graph_recurrence_from_rrule("FREQ=MONTHLY;BYDAY=MO", "2026-06-02")
            .expect("every Monday maps");
        let pattern = every_monday.pattern.expect("pattern");
        assert_eq!(pattern.kind.as_deref(), Some("weekly"));
        assert_eq!(pattern.days_of_week, Some(vec!["monday".to_string()]));
        assert_eq!(pattern.index, None);

        assert!(matches!(
            graph_recurrence_from_rrule("FREQ=MONTHLY;INTERVAL=2;BYDAY=MO", "2026-06-02"),
            Err(RecurrenceRefusal::Unsupported(_))
        ));
        assert!(matches!(
            graph_recurrence_from_rrule("FREQ=YEARLY;BYMONTH=6;BYDAY=MO", "2026-06-02"),
            Err(RecurrenceRefusal::Unsupported(_))
        ));
    }

    #[test]
    fn slash_free_junk_timezone_is_rejected() {
        // A multi-word slash-free value round-trips as a Windows tz id; a
        // single-token junk value is rejected rather than forwarded.
        assert_eq!(
            graph_time_zone_name(Some("Eastern Standard Time")),
            Some("Eastern Standard Time")
        );
        assert_eq!(graph_time_zone_name(Some("foo")), None);
        assert_eq!(graph_time_zone_name(Some("UTC")), Some("UTC"));
    }

    #[test]
    fn search_api_cursor_round_trips_offset() {
        let cursor = search_api_cursor(50);
        assert_eq!(search_api_cursor_offset(Some(&cursor)), Some(50));
        // A local nextLink cursor (an https URL) is not a Search-API
        // cursor.
        assert_eq!(
            search_api_cursor_offset(Some(b"https://graph.microsoft.com/next")),
            None
        );
        assert_eq!(search_api_cursor_offset(None), None);
    }

    #[test]
    fn graph_event_maps_all_day_values_to_dates() {
        let event = event_from_graph(
            "calendar".to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: None,
                body: None,
                location: None,
                start: Some(GraphDateTime {
                    date_time: Some("2026-06-02T00:00:00.0000000".to_string()),
                    time_zone: Some("UTC".to_string()),
                }),
                end: Some(GraphDateTime {
                    date_time: Some("2026-06-03T00:00:00.0000000".to_string()),
                    time_zone: Some("UTC".to_string()),
                }),
                is_all_day: Some(true),
                show_as: None,
                sensitivity: None,
                organizer: None,
                attendees: None,
                original_start: None,
                recurrence: None,
                web_link: None,
                response_status: None,
                is_cancelled: None,
                change_key: None,
                original_start_time_zone: None,
            },
        );

        assert!(event.is_all_day);
        assert_eq!(event.start.value, "2026-06-02");
        assert_eq!(
            event.end.as_ref().map(|end| end.value.as_str()),
            Some("2026-06-03")
        );
    }

    /// A delta tombstone carries no `end`, and a null `dateTime` is no end
    /// either: both project as `None`, never as an empty time a consumer
    /// would have to recognize. Fails if the empty-value filter on the `end`
    /// projection is removed.
    #[test]
    fn a_missing_or_valueless_end_projects_as_no_end() {
        for end in [
            None,
            Some(GraphDateTime {
                date_time: None,
                time_zone: Some("UTC".to_string()),
            }),
        ] {
            let event = event_from_graph(
                "calendar".to_string(),
                serde_json::from_value::<GraphEvent>(json!({"id": "e1"}))
                    .map(|event| GraphEvent { end, ..event })
                    .expect("fixture deserializes"),
            );
            assert_eq!(event.end, None);
        }
    }

    #[test]
    fn graph_event_create_writes_all_day_midnight_values() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-03".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: true,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        })
        .expect("create payload");

        assert_eq!(patch.is_all_day, Some(true));
        assert_eq!(
            patch.start.and_then(|time| time.date_time),
            Some("2026-06-02T00:00:00.0000000".to_string())
        );
        assert_eq!(
            patch.end.and_then(|time| time.date_time),
            Some("2026-06-03T00:00:00.0000000".to_string())
        );
    }

    #[test]
    fn graph_event_maps_weekly_recurrence_to_rrule() {
        let event = event_from_graph(
            "calendar".to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: None,
                body: None,
                location: None,
                start: Some(graph_time("2026-06-02T12:00:00")),
                end: Some(graph_time("2026-06-02T13:00:00")),
                is_all_day: Some(false),
                show_as: None,
                sensitivity: None,
                organizer: None,
                attendees: None,
                original_start: None,
                recurrence: Some(GraphRecurrence {
                    pattern: Some(GraphRecurrencePattern {
                        kind: Some("weekly".to_string()),
                        interval: Some(2),
                        month: None,
                        day_of_month: None,
                        days_of_week: Some(vec!["monday".to_string(), "wednesday".to_string()]),
                        first_day_of_week: Some("monday".to_string()),
                        index: None,
                    }),
                    range: Some(GraphRecurrenceRange {
                        kind: Some("numbered".to_string()),
                        start_date: Some("2026-06-02".to_string()),
                        end_date: None,
                        recurrence_time_zone: None,
                        number_of_occurrences: Some(4),
                    }),
                }),
                web_link: None,
                response_status: None,
                is_cancelled: None,
                change_key: None,
                original_start_time_zone: None,
            },
        );

        assert_eq!(
            event.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;BYDAY=MO,WE;INTERVAL=2;COUNT=4")
        );
    }

    #[test]
    fn graph_event_create_writes_recurrence() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some("FREQ=MONTHLY;BYDAY=MO;BYSETPOS=2;COUNT=3".to_string()),
                ..EventRecurrence::default()
            },
        })
        .expect("create payload");
        let recurrence = patch
            .recurrence
            .expect("recurrence write")
            .expect("a series, not a clear");
        let pattern = recurrence.pattern.expect("pattern");
        let range = recurrence.range.expect("range");

        assert_eq!(pattern.kind.as_deref(), Some("relativeMonthly"));
        assert_eq!(pattern.days_of_week, Some(vec!["monday".to_string()]));
        assert_eq!(pattern.index.as_deref(), Some("second"));
        assert_eq!(range.kind.as_deref(), Some("numbered"));
        assert_eq!(range.start_date.as_deref(), Some("2026-06-02"));
        assert_eq!(range.number_of_occurrences, Some(3));
    }

    fn recurring_create(rrule: &str) -> EventCreate {
        EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some(rrule.to_string()),
                ..EventRecurrence::default()
            },
        }
    }

    /// An RRULE Graph cannot express used to be dropped from the payload,
    /// creating a one-off event where a series was asked for.
    #[test]
    fn graph_event_create_rejects_unsupported_rrule_parts() {
        let error = graph_event_from_create(&recurring_create("FREQ=MONTHLY;BYDAY=MO;BYHOUR=9"))
            .expect_err("an unexpressible RRULE must refuse the create");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        );
    }

    #[test]
    fn graph_event_create_rejects_malformed_rrule_as_caller_input() {
        let error = graph_event_from_create(&recurring_create("FREQ=WEEKLY;INTERVAL=abc"))
            .expect_err("a malformed RRULE must refuse the create");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert_eq!(error.recovery(), &bifrost_types::RecoveryClass::ClientBug);
    }

    fn recurrence_patch(rrule: &str, start: Option<&str>) -> EventPatch {
        EventPatch {
            start: start.map(|value| EventTime {
                value: value.to_string(),
                timezone: Some("UTC".to_string()),
            }),
            recurrence: Some(EventRecurrence {
                rrule: Some(rrule.to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        }
    }

    /// The patch path dropped an unexpressible RRULE too, leaving the
    /// series unchanged while the update reported success.
    #[test]
    fn graph_event_patch_rejects_unsupported_rrule_parts() {
        let error = graph_event_from_patch(
            &recurrence_patch("FREQ=DAILY;BYHOUR=9", Some("2026-06-02T12:00:00")),
            CurrentSeries::default(),
        )
        .expect_err("an unexpressible RRULE must refuse the patch");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        );
    }

    fn range_start(patch: &GraphEventPatch) -> Option<&str> {
        patch
            .recurrence
            .as_ref()
            .and_then(Option::as_ref)
            .and_then(|recurrence| recurrence.range.as_ref())
            .and_then(|range| range.start_date.as_deref())
    }

    /// A recurrence as Graph reports it for a series that began on
    /// `start_date`, the only part of it the anchoring reads.
    fn existing_series(start_date: &str) -> GraphRecurrence {
        GraphRecurrence {
            pattern: Some(GraphRecurrencePattern {
                kind: Some("daily".to_string()),
                interval: Some(1),
                month: None,
                day_of_month: None,
                days_of_week: None,
                first_day_of_week: None,
                index: None,
            }),
            range: Some(GraphRecurrenceRange {
                kind: Some("noEnd".to_string()),
                start_date: Some(start_date.to_string()),
                end_date: None,
                recurrence_time_zone: None,
                number_of_occurrences: None,
            }),
        }
    }

    /// A recurrence patch without `start` built its range from an empty
    /// time, sending `startDate: ""`. An event that already recurs is
    /// anchored on its own series start; one that does not is refused.
    #[test]
    fn graph_event_patch_recurrence_without_start_is_anchored_or_refused() {
        let patch = recurrence_patch("FREQ=DAILY;COUNT=3", None);

        let existing = existing_series("2026-05-04");
        let series = CurrentSeries {
            recurrence: Some(&existing),
            time_zone: None,
        };

        let anchored = graph_event_from_patch(&patch, series).expect("series start anchors");
        assert_eq!(range_start(&anchored), Some("2026-05-04"));

        let error = graph_event_from_patch(&patch, CurrentSeries::default())
            .expect_err("no start and no series start must be refused");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        );

        // A start in the same patch wins over the old series start.
        let restarted = graph_event_from_patch(
            &recurrence_patch("FREQ=DAILY;COUNT=3", Some("2026-06-02T12:00:00")),
            series,
        )
        .expect("patch start anchors");
        assert_eq!(range_start(&restarted), Some("2026-06-02"));
    }

    /// Moving `start` on a recurring event without restating the recurrence
    /// re-sends the event's own recurrence with the range start moved along.
    #[test]
    fn graph_event_patch_start_move_carries_the_series_range_along() {
        let existing = existing_series("2026-05-04");
        let series = CurrentSeries {
            recurrence: Some(&existing),
            time_zone: None,
        };
        let moved = EventPatch {
            start: Some(EventTime {
                value: "2026-06-09T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            ..EventPatch::default()
        };

        let patch = graph_event_from_patch(&moved, series).expect("patch payload");
        assert_eq!(range_start(&patch), Some("2026-06-09"));

        // A one-off has no recurrence to carry.
        let one_off =
            graph_event_from_patch(&moved, CurrentSeries::default()).expect("patch payload");
        assert!(one_off.recurrence.is_none());
    }

    /// The carried range start was cut from the moved `start` by byte
    /// offset, so a value that is not a date was written as `startDate`, and
    /// one with a multi-byte character straddling byte ten panicked. It is
    /// now read by the anchor's own rule and refused as the caller's input.
    #[test]
    fn graph_event_patch_start_move_refuses_a_start_that_is_not_a_date() {
        let existing = existing_series("2026-05-04");
        let series = CurrentSeries {
            recurrence: Some(&existing),
            time_zone: None,
        };
        for value in ["2026-06-2\u{e9}", "not a date at all"] {
            let moved = EventPatch {
                start: Some(EventTime {
                    value: value.to_string(),
                    timezone: Some("UTC".to_string()),
                }),
                ..EventPatch::default()
            };
            let error = graph_event_from_patch(&moved, series)
                .expect_err("a start that is not a date cannot anchor the series");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                ),
                "{value:?}"
            );
        }
    }

    /// RFC 5545 skips a date the month lacks; Graph's absolute patterns move
    /// it to the month's last day. A series on a day some recurring month
    /// lacks is refused rather than written with occurrences never asked for,
    /// whether the day is explicit or taken from the start.
    #[test]
    fn an_absolute_day_some_month_lacks_is_refused() {
        let refused = [
            ("FREQ=MONTHLY;COUNT=3", "2026-01-31"),
            ("FREQ=MONTHLY", "2026-01-29"),
            ("FREQ=MONTHLY;BYMONTHDAY=30", "2026-01-01"),
            ("FREQ=YEARLY", "2028-02-29"),
            ("FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=29", "2026-01-01"),
            ("FREQ=YEARLY;BYMONTH=4;BYMONTHDAY=31", "2026-01-01"),
            // Every sixth month from August reaches February.
            ("FREQ=MONTHLY;INTERVAL=6", "2026-08-31"),
            // A leap-year start still reaches a February without a 29th.
            ("FREQ=MONTHLY", "2028-01-29"),
            ("FREQ=MONTHLY;BYMONTHDAY=30", "2026-02-10"),
            // Graph would write February 28th, inside UNTIL; RFC 5545 skips it.
            ("FREQ=MONTHLY;UNTIL=20260228", "2026-01-31"),
            // The second candidate, 2037, is not a leap year: a horizon that
            // stopped before it accepted this series.
            ("FREQ=YEARLY;INTERVAL=9;COUNT=2", "2028-02-29"),
            // 2100 is not a leap year, the nineteenth candidate from 2028.
            ("FREQ=YEARLY;INTERVAL=4", "2028-02-29"),
            ("FREQ=YEARLY;INTERVAL=4;COUNT=19", "2028-02-29"),
            // February 2028 precedes the start and is no occurrence, so
            // COUNT=1 reaches February 2029, which Graph would clamp.
            ("FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=29;COUNT=1", "2028-03-29"),
            // A huge interval must not overflow its way to acceptance.
            ("FREQ=MONTHLY;INTERVAL=4000000000", "2026-01-31"),
        ];
        for (rrule, start) in refused {
            assert!(
                matches!(
                    graph_recurrence_from_rrule(rrule, start),
                    Err(RecurrenceRefusal::Unsupported(_))
                ),
                "{rrule:?} from {start}"
            );
        }
        let accepted = [
            ("FREQ=MONTHLY", "2026-01-28"),
            ("FREQ=MONTHLY;BYMONTHDAY=28", "2026-01-01"),
            ("FREQ=YEARLY", "2026-01-31"),
            ("FREQ=YEARLY;BYMONTH=4;BYMONTHDAY=30", "2026-01-01"),
            // Only the months the series visits count: July and January.
            ("FREQ=MONTHLY;INTERVAL=6;COUNT=3", "2026-07-31"),
            ("FREQ=MONTHLY;INTERVAL=12", "2026-01-31"),
            // The series ends before it reaches a short month.
            ("FREQ=MONTHLY;COUNT=1", "2026-01-31"),
            // Graph would put February's occurrence on the 28th, past UNTIL.
            ("FREQ=MONTHLY;UNTIL=20260227", "2026-01-31"),
            // Every fourth February from a leap year is a leap one until the
            // century rule, so only a series ending before 2100 is exact.
            ("FREQ=YEARLY;INTERVAL=4;UNTIL=20960301", "2028-02-29"),
            ("FREQ=YEARLY;INTERVAL=4;COUNT=18", "2028-02-29"),
            // A day before the start in the start's month is not an occurrence.
            ("FREQ=MONTHLY;BYMONTHDAY=15;COUNT=1", "2026-01-20"),
            // February 2027 lacks the 29th but precedes the start; the one
            // occurrence is February 2028.
            ("FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=29;COUNT=1", "2027-03-01"),
        ];
        for (rrule, start) in accepted {
            assert!(
                graph_recurrence_from_rrule(rrule, start).is_ok(),
                "{rrule:?} from {start}"
            );
        }

        // Through create, the refusal is the operation's Unsupported.
        let mut event = recurring_create("FREQ=MONTHLY;COUNT=3");
        event.start.value = "2026-01-31T12:00:00".to_string();
        event.end.value = "2026-01-31T13:00:00".to_string();
        let error = graph_event_from_create(&event).expect_err("the 31st every month is refused");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        );
    }

    /// An empty recurrence is how a patch makes a series a one-off: it goes
    /// out as `"recurrence": null`, distinct from a patch that omits it.
    #[test]
    fn graph_event_patch_with_empty_recurrence_clears_it() {
        let patch = graph_event_from_patch(
            &EventPatch {
                recurrence: Some(EventRecurrence::default()),
                ..EventPatch::default()
            },
            CurrentSeries::default(),
        )
        .expect("patch payload");
        let value = serde_json::to_value(&patch).expect("patch json");
        assert!(value.get("recurrence").is_some_and(Value::is_null));

        let untouched = graph_event_from_patch(&EventPatch::default(), CurrentSeries::default())
            .expect("patch payload");
        let value = serde_json::to_value(&untouched).expect("patch json");
        assert!(value.get("recurrence").is_none());
    }

    /// Through `update` itself: the series start comes off the fetched
    /// event and reaches the PATCH body on the wire.
    #[tokio::test]
    async fn update_anchors_a_startless_recurrence_patch_on_the_series_start() {
        let current = json!({
            "id": "e1",
            "changeKey": "ck1",
            "recurrence": {
                "pattern": {"type": "daily", "interval": 1},
                "range": {"type": "noEnd", "startDate": "2026-05-04"}
            }
        });
        let (account, script) = scripted_search_account_pages(vec![current, json!({})]);

        update(
            account,
            EventId("calendar::e1".to_string()),
            recurrence_patch("FREQ=WEEKLY;BYDAY=MO", None),
        )
        .await
        .expect("update succeeds");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, reqwest::Method::PATCH);
        let body: Value =
            serde_json::from_slice(requests[1].body.as_ref().expect("patch carries a body"))
                .expect("patch body is json");
        assert_eq!(body["recurrence"]["range"]["startDate"], "2026-05-04");
        assert_eq!(body["recurrence"]["pattern"]["type"], "weekly");
        // RFC 5545's default week start, written explicitly because Graph's
        // own default is Sunday.
        assert_eq!(body["recurrence"]["pattern"]["firstDayOfWeek"], "monday");
    }

    fn timed(value: &str) -> EventTime {
        EventTime {
            value: value.to_string(),
            timezone: Some("UTC".to_string()),
        }
    }

    fn create_with_times(start: &str, end: &str) -> EventCreate {
        let mut event = recurring_create("FREQ=DAILY");
        event.recurrence = EventRecurrence::default();
        event.start = timed(start);
        event.end = timed(end);
        event
    }

    /// An empty `start` has nothing to write: refused as the caller's
    /// malformed input before any request. Fails if the start arm of
    /// `reject_unusable_write_times` is removed from `create` (the empty
    /// script would then panic on the POST).
    #[tokio::test]
    async fn create_with_an_empty_start_is_refused_before_any_request() {
        let (account, script) = scripted_search_account_pages(Vec::new());

        let error = create(account, create_with_times("", "2026-06-02T13:00:00"))
            .await
            .expect_err("an empty start is malformed");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(script.requests().is_empty());
    }

    /// Graph's create needs an explicit end and none is invented: an empty
    /// end is `Unsupported` before any request. Fails if the end arm of
    /// `reject_unusable_write_times` is removed.
    #[tokio::test]
    async fn create_with_an_empty_end_is_refused_before_any_request() {
        let (account, script) = scripted_search_account_pages(Vec::new());

        let error = create(account, create_with_times("2026-06-02T12:00:00", " "))
            .await
            .expect_err("an empty end cannot be created");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
        assert!(script.requests().is_empty());
    }

    /// The empty-start refusal runs before the fetch and before the
    /// calendar-move refusal (which would otherwise answer `Unsupported`).
    /// Fails if the guard is removed from `update` or moved after
    /// `reject_calendar_move`.
    #[tokio::test]
    async fn update_with_an_empty_start_is_refused_before_anything_else() {
        let (account, script) = scripted_search_account_pages(Vec::new());
        let patch = EventPatch {
            calendar_id: Some(CalendarId("elsewhere".to_string())),
            start: Some(timed("")),
            ..EventPatch::default()
        };

        let error = update(account, EventId("calendar::e1".to_string()), patch)
            .await
            .expect_err("an empty start is malformed");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(script.requests().is_empty());
    }

    /// A patch carrying an empty end alongside its start. The start goes out, the end is omitted so the stored one is untouched, and no
    /// `dateTime: ""` reaches the wire. Fails if the blank-end filter in
    /// `graph_event_from_patch` is removed.
    #[tokio::test]
    async fn update_echoing_an_empty_end_omits_it_and_keeps_the_start() {
        let (account, script) =
            scripted_search_account_pages(vec![json!({"id": "e1", "changeKey": "ck1"}), json!({})]);
        let patch = EventPatch {
            start: Some(timed("2026-06-02T12:00:00")),
            end: Some(timed("")),
            ..EventPatch::default()
        };

        update(account, EventId("calendar::e1".to_string()), patch)
            .await
            .expect("an echoed empty end is accepted");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        let body: Value =
            serde_json::from_slice(requests[1].body.as_ref().expect("patch carries a body"))
                .expect("patch body is json");
        assert_eq!(body["start"]["dateTime"], "2026-06-02T12:00:00");
        assert!(body.get("end").is_none());
    }

    /// An empty end alone is not a time change: it must neither be sent nor
    /// drive the all-day inference. Fails if the blank-end filter is applied
    /// to the payload but not to `effective_all_day` / `isAllDay`.
    #[test]
    fn an_empty_end_alone_touches_no_time_field() {
        let patch = graph_event_from_patch(
            &EventPatch {
                title: Some(Some("Renamed".to_string())),
                end: Some(timed("")),
                ..EventPatch::default()
            },
            CurrentSeries::default(),
        )
        .expect("patch payload");
        let value = serde_json::to_value(&patch).expect("patch json");

        assert!(value.get("end").is_none());
        assert!(value.get("isAllDay").is_none());
    }

    /// An empty attendee list is how a patch clears attendees; it was
    /// omitted from the body, so the old attendees stayed.
    #[test]
    fn graph_event_patch_with_no_attendees_clears_them() {
        let patch = graph_event_from_patch(
            &EventPatch {
                attendees: Some(Vec::new()),
                ..EventPatch::default()
            },
            CurrentSeries::default(),
        )
        .expect("patch payload");
        let value = serde_json::to_value(&patch).expect("patch json");
        assert_eq!(value.get("attendees"), Some(&json!([])));

        // A patch not touching attendees still leaves them alone.
        let untouched = graph_event_from_patch(&EventPatch::default(), CurrentSeries::default())
            .expect("patch");
        let value = serde_json::to_value(&untouched).expect("patch json");
        assert!(value.get("attendees").is_none());
    }

    /// RFC 5545 takes whatever a rule leaves out from DTSTART, so a bare
    /// MONTHLY or a YEARLY with only BYMONTH is the start's day of the month
    /// (and day of the year), not something Graph cannot express.
    #[test]
    fn graph_recurrence_takes_missing_values_from_the_start() {
        let monthly = graph_recurrence_from_rrule("FREQ=MONTHLY", "2026-06-02")
            .expect("bare MONTHLY maps")
            .pattern
            .expect("pattern");
        assert_eq!(monthly.kind.as_deref(), Some("absoluteMonthly"));
        assert_eq!(monthly.day_of_month, Some(2));

        let yearly = graph_recurrence_from_rrule("FREQ=YEARLY;BYMONTH=6", "2026-06-02")
            .expect("YEARLY with BYMONTH maps")
            .pattern
            .expect("pattern");
        assert_eq!(yearly.kind.as_deref(), Some("absoluteYearly"));
        assert_eq!(yearly.month, Some(6));
        assert_eq!(yearly.day_of_month, Some(2));

        let bare_yearly = graph_recurrence_from_rrule("FREQ=YEARLY", "2026-06-02")
            .expect("bare YEARLY maps")
            .pattern
            .expect("pattern");
        assert_eq!(bare_yearly.month, Some(6));
        assert_eq!(bare_yearly.day_of_month, Some(2));

        // 2026-06-02 is a Tuesday.
        let weekly = graph_recurrence_from_rrule("FREQ=WEEKLY", "2026-06-02")
            .expect("bare WEEKLY maps")
            .pattern
            .expect("pattern");
        assert_eq!(weekly.days_of_week, Some(vec!["tuesday".to_string()]));
    }

    /// WKST changes which weeks an every-other-week rule lands on, so it is
    /// carried as `firstDayOfWeek` on weekly patterns (RFC 5545's default
    /// Monday when absent) and read back only where it differs from Monday
    /// and matters.
    #[test]
    fn wkst_is_carried_as_first_day_of_week_and_round_trips() {
        let first_day = |rrule: &str| {
            graph_recurrence_from_rrule(rrule, "2026-06-01")
                .expect("rule maps")
                .pattern
                .expect("pattern")
                .first_day_of_week
        };
        assert_eq!(
            first_day("FREQ=WEEKLY;INTERVAL=2;BYDAY=MO;WKST=SU").as_deref(),
            Some("sunday")
        );
        assert_eq!(
            first_day("FREQ=WEEKLY;INTERVAL=2;BYDAY=MO").as_deref(),
            Some("monday")
        );
        // A pattern that is not weekly has no week start to carry.
        assert_eq!(first_day("FREQ=DAILY;WKST=SU"), None);

        let round_trip = |rrule: &str| {
            let recurrence = graph_recurrence_from_rrule(rrule, "2026-06-01").expect("rule maps");
            rrule_from_graph(&recurrence).expect("rrule")
        };
        assert_eq!(
            round_trip("FREQ=WEEKLY;INTERVAL=2;BYDAY=MO;WKST=SU"),
            "FREQ=WEEKLY;BYDAY=MO;INTERVAL=2;WKST=SU"
        );
        assert_eq!(
            round_trip("FREQ=WEEKLY;INTERVAL=2;BYDAY=MO"),
            "FREQ=WEEKLY;BYDAY=MO;INTERVAL=2"
        );

        // A Graph weekly pattern with no `firstDayOfWeek` is read at Graph's
        // documented default, Sunday.
        let mut recurrence =
            graph_recurrence_from_rrule("FREQ=WEEKLY;INTERVAL=2;BYDAY=MO", "2026-06-01")
                .expect("rule maps");
        recurrence
            .pattern
            .as_mut()
            .expect("pattern")
            .first_day_of_week = None;
        assert_eq!(
            rrule_from_graph(&recurrence).as_deref(),
            Some("FREQ=WEEKLY;BYDAY=MO;INTERVAL=2;WKST=SU")
        );
    }

    fn rrule_verdict(rrule: &str) -> &'static str {
        match graph_recurrence_rule(rrule) {
            Ok(_) => "ok",
            Err(RecurrenceRefusal::Unsupported(_)) => "unsupported",
            Err(RecurrenceRefusal::Malformed { .. }) => "malformed",
        }
    }

    /// Each rule here was once turned into something other than what it
    /// says: dropped whole (a one-off event), or written with a part Graph
    /// ignores for that pattern type, or with an unparseable value quietly
    /// defaulted. Every one is now refused, and the refusal says whether the
    /// rule is valid but beyond Graph (`unsupported`) or not an RRULE at all
    /// (`malformed`).
    #[test]
    fn rrules_graph_cannot_take_are_refused_by_kind() {
        let cases = [
            // Valid RFC 5545 that Graph's patternedRecurrence cannot hold.
            ("FREQ=DAILY;BYHOUR=9", "unsupported"),
            ("FREQ=WEEKLY;BYDAY=MO;X-NAME=1", "unsupported"),
            ("FREQ=HOURLY", "unsupported"),
            ("FREQ=DAILY;INTERVAL=2;BYDAY=MO,TU,WE,TH,FR", "unsupported"),
            ("FREQ=MONTHLY;BYMONTHDAY=13;BYDAY=FR", "unsupported"),
            ("FREQ=MONTHLY;BYMONTHDAY=1,15", "unsupported"),
            ("FREQ=MONTHLY;BYMONTHDAY=-1", "unsupported"),
            ("FREQ=MONTHLY;BYMONTH=6;BYMONTHDAY=1", "unsupported"),
            ("FREQ=WEEKLY;BYDAY=MO;BYSETPOS=1", "unsupported"),
            ("FREQ=MONTHLY;BYDAY=MO;BYSETPOS=5", "unsupported"),
            ("FREQ=MONTHLY;BYDAY=1MO,1TU", "unsupported"),
            ("FREQ=MONTHLY;BYDAY=5MO", "unsupported"),
            ("FREQ=MONTHLY;BYDAY=2MO;BYSETPOS=1", "unsupported"),
            ("FREQ=YEARLY;BYMONTHDAY=15", "unsupported"),
            // Not an RRULE.
            ("", "malformed"),
            ("BYDAY=MO", "malformed"),
            ("FREQ=SOMETIMES", "malformed"),
            ("FREQ=DAILY;GARBAGE", "malformed"),
            ("FREQ=DAILY;FOO=1", "malformed"),
            ("FREQ=DAILY;FREQ=WEEKLY", "malformed"),
            ("FREQ=DAILY;INTERVAL=abc", "malformed"),
            ("FREQ=DAILY;INTERVAL=0", "malformed"),
            ("FREQ=DAILY;COUNT=3;UNTIL=20290602", "malformed"),
            ("FREQ=DAILY;UNTIL=2029", "malformed"),
            ("FREQ=WEEKLY;BYDAY=MO,XX", "malformed"),
            ("FREQ=WEEKLY;BYDAY=2MO", "malformed"),
            ("FREQ=YEARLY;BYMONTH=13;BYMONTHDAY=1", "malformed"),
            ("FREQ=WEEKLY;WKST=XX", "malformed"),
            // Accepted: the shape is Graph's own, or an equivalent one, or
            // RFC 5545 takes the missing values from DTSTART.
            ("FREQ=DAILY", "ok"),
            ("FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR", "ok"),
            ("FREQ=MONTHLY", "ok"),
            ("FREQ=MONTHLY;BYDAY=2MO", "ok"),
            ("FREQ=MONTHLY;BYDAY=-1FR", "ok"),
            ("FREQ=YEARLY", "ok"),
            ("FREQ=YEARLY;BYMONTH=6", "ok"),
            ("FREQ=WEEKLY;INTERVAL=2;BYDAY=MO;WKST=MO", "ok"),
            ("freq=weekly;byday=mo,we;", "ok"),
            ("FREQ=WEEKLY;BYDAY=MO;WKST=MO", "ok"),
            ("FREQ=WEEKLY;INTERVAL=2;BYDAY=MO;WKST=SU", "ok"),
            ("FREQ=MONTHLY;BYMONTHDAY=15;UNTIL=20290602T000000Z", "ok"),
            ("FREQ=YEARLY;BYMONTH=6;BYMONTHDAY=2;COUNT=5", "ok"),
            ("FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1", "ok"),
        ];
        for (rrule, expected) in cases {
            assert_eq!(rrule_verdict(rrule), expected, "{rrule:?}");
        }
    }

    #[test]
    fn graph_event_create_writes_relative_yearly_recurrence() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some(
                    "FREQ=YEARLY;BYMONTH=6;BYDAY=MO;BYSETPOS=-1;UNTIL=20290602".to_string(),
                ),
                ..EventRecurrence::default()
            },
        })
        .expect("create payload");
        let recurrence = patch
            .recurrence
            .expect("recurrence write")
            .expect("a series, not a clear");
        let pattern = recurrence.pattern.expect("pattern");
        let range = recurrence.range.expect("range");

        assert_eq!(pattern.kind.as_deref(), Some("relativeYearly"));
        assert_eq!(pattern.month, Some(6));
        assert_eq!(pattern.days_of_week, Some(vec!["monday".to_string()]));
        assert_eq!(pattern.index.as_deref(), Some("last"));
        assert_eq!(range.kind.as_deref(), Some("endDate"));
        assert_eq!(range.end_date.as_deref(), Some("2029-06-02"));
    }

    #[test]
    fn search_hit_id_round_trips_through_mailbox_scope() {
        let event = event_from_graph(
            MAILBOX_SCOPE.to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: Some("Planning".to_string()),
                body: None,
                location: None,
                start: Some(graph_time("2026-06-02T12:00:00")),
                end: Some(graph_time("2026-06-02T13:00:00")),
                is_all_day: Some(false),
                show_as: None,
                sensitivity: None,
                organizer: None,
                attendees: None,
                original_start: None,
                recurrence: None,
                web_link: None,
                response_status: None,
                is_cancelled: None,
                change_key: None,
                original_start_time_zone: None,
            },
        );

        let (calendar_id, event_id) =
            split_event_id(&event.id.0, AccountOperation::EventGet).expect("split");
        assert_eq!(calendar_id, MAILBOX_SCOPE);
        assert_eq!(event_id, "e1");

        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert_eq!(
            event_url(&account, &calendar_id, &event_id),
            "/me/events/e1"
        );
    }

    #[test]
    fn event_url_routes_calendar_scoped_ids_through_calendar_collection() {
        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert_eq!(
            event_url(&account, "calendar", "e1"),
            "/me/calendars/calendar/events/e1"
        );
    }

    #[test]
    fn event_search_path_uses_default_events_collection() {
        assert_eq!(event_search_path("/me", None), "/me/events");
        assert_eq!(
            event_search_path("/me", Some(&CalendarId("calendar with spaces".to_string()))),
            "/me/calendars/calendar%20with%20spaces/events"
        );
    }

    #[test]
    fn graph_search_api_support_requires_unscoped_default_mailbox_search() {
        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert!(graph_search_api_supported(
            &account,
            &EventSearchRequest {
                calendar_id: None,
                query: "planning".to_string(),
                page_cursor: None,
                limit: None,
                include_cancelled: false,
            }
        ));
        assert!(!graph_search_api_supported(
            &account,
            &EventSearchRequest {
                calendar_id: Some(CalendarId("calendar".to_string())),
                query: "planning".to_string(),
                page_cursor: None,
                limit: None,
                include_cancelled: false,
            }
        ));
        assert!(!graph_search_api_supported(
            &account,
            &EventSearchRequest {
                calendar_id: None,
                query: " ".to_string(),
                page_cursor: None,
                limit: None,
                include_cancelled: false,
            }
        ));

        let shared = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token").for_shared_mailbox("shared"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert!(!graph_search_api_supported(
            &shared,
            &EventSearchRequest {
                calendar_id: None,
                query: "planning".to_string(),
                page_cursor: None,
                limit: None,
                include_cancelled: false,
            }
        ));
    }

    #[test]
    fn graph_event_search_request_serializes_entity_type_and_fields() {
        let request = GraphSearchRequest {
            requests: vec![GraphSearchEntityRequest {
                entity_types: vec!["event"],
                query: GraphSearchQuery {
                    query_string: "planning".to_string(),
                },
                from: 0,
                size: 10,
                fields: EVENT_SEARCH_FIELDS,
            }],
        };
        let value = serde_json::to_value(&request).expect("search request json");

        assert_eq!(value["requests"][0]["entityTypes"], json!(["event"]));
        assert_eq!(value["requests"][0]["query"]["queryString"], "planning");
        assert_eq!(value["requests"][0]["size"], 10);
        assert_eq!(value["requests"][0]["fields"][0], "id");
    }

    fn scripted_search_account(
        body: serde_json::Value,
    ) -> (
        GraphAccount,
        std::sync::Arc<bifrost_net::test_support::ScriptedDispatch>,
    ) {
        scripted_search_account_pages(vec![body])
    }

    fn scripted_search_account_pages(
        bodies: Vec<serde_json::Value>,
    ) -> (
        GraphAccount,
        std::sync::Arc<bifrost_net::test_support::ScriptedDispatch>,
    ) {
        use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_account};
        use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource, TokenSource};
        use std::sync::Arc;

        let script = ScriptedDispatch::new(bodies.into_iter().map(|body| Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::from(body.to_string()),
        }));
        let token_source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
        let net = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::clone(&token_source),
            RetryPolicy::disabled(),
        );
        let client = crate::client::GraphClient::with_account_net(
            net,
            "https://graph.contoso.test/v1.0",
            token_source,
        );
        let account =
            GraphAccount::new_for_tests(client, crate::account::PushMode::GraphSubscriptions);
        (account, script)
    }

    fn local_search_request(include_cancelled: bool) -> EventSearchRequest {
        let mut request = EventSearchRequest::new("planning");
        request.calendar_id = Some(CalendarId("calendar".to_string()));
        request.include_cancelled = include_cancelled;
        request
    }

    fn cancelled_and_live_page() -> serde_json::Value {
        json!({
            "value": [
                {"id": "gone", "subject": "Planning", "isCancelled": true},
                {"id": "live", "subject": "Planning", "isCancelled": false}
            ]
        })
    }

    #[tokio::test]
    async fn local_search_drops_cancelled_events_by_default() {
        let (account, _script) = scripted_search_account(cancelled_and_live_page());

        let page = search(account, local_search_request(false))
            .await
            .expect("search loads");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].native_id, "calendar::live");
        assert_eq!(page.estimated_total, None);
    }

    #[tokio::test]
    async fn local_search_keeps_cancelled_events_when_included() {
        let (account, _script) = scripted_search_account(cancelled_and_live_page());

        let page = search(account, local_search_request(true))
            .await
            .expect("search loads");

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].status, EventStatus::Cancelled);
    }

    #[tokio::test]
    async fn local_search_limit_counts_only_surviving_events() {
        let (account, _script) = scripted_search_account(cancelled_and_live_page());
        let mut request = local_search_request(false);
        request.limit = Some(1);

        let page = search(account, request).await.expect("search loads");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].native_id, "calendar::live");
    }

    /// The event-search walk refusing a repeated `nextLink` reports the
    /// provider's contract breach, as every other Graph walk does. It was
    /// mapped through the calendar's local `Unsupported` helper, telling the
    /// operator the account cannot search events at all.
    #[tokio::test]
    async fn a_repeated_event_search_link_is_a_provider_contract_violation() {
        let link = "https://graph.contoso.test/v1.0/me/calendars/calendar/events?page=2";
        let page = json!({"value": [], "@odata.nextLink": link});
        let (account, _script) = scripted_search_account_pages(vec![page.clone(), page]);

        let error = search(account, local_search_request(false))
            .await
            .expect_err("a repeated page link must refuse the walk");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        assert_eq!(
            error.recovery(),
            &bifrost_types::RecoveryClass::ProviderContractViolation
        );
    }

    fn search_api_page() -> serde_json::Value {
        json!({
            "value": [{
                "hitsContainers": [{
                    "hits": [
                        {"resource": {"id": "gone", "subject": "Planning", "isCancelled": true}},
                        {"resource": {"id": "live", "subject": "Planning"}}
                    ],
                    "moreResultsAvailable": true
                }]
            }]
        })
    }

    #[tokio::test]
    async fn search_api_drops_cancelled_but_advances_offset_by_raw_hits() {
        let (account, _script) = scripted_search_account(search_api_page());

        let page = search(account, EventSearchRequest::new("planning"))
            .await
            .expect("search loads");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].status, EventStatus::Confirmed);
        assert_eq!(page.estimated_total, None);
        assert_eq!(page.next_cursor, Some(search_api_cursor(2)));
    }

    #[tokio::test]
    async fn search_api_keeps_cancelled_when_included() {
        let (account, _script) = scripted_search_account(search_api_page());
        let mut request = EventSearchRequest::new("planning");
        request.include_cancelled = true;

        let page = search(account, request).await.expect("search loads");

        assert_eq!(page.items.len(), 2);
    }

    #[test]
    fn rsvp_status_maps_to_graph_action() {
        assert_eq!(rsvp_action(RsvpStatus::Accepted).unwrap(), "accept");
        assert_eq!(rsvp_action(RsvpStatus::Declined).unwrap(), "decline");
        assert_eq!(
            rsvp_action(RsvpStatus::Tentative).unwrap(),
            "tentativelyAccept"
        );
        assert!(rsvp_action(RsvpStatus::NeedsAction).is_err());
    }
}
