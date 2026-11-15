use crate::calendar::recurrence::*;
use crate::calendar::time_zone::*;
use crate::calendar::wire::*;

#[path = "calendar_public.rs"]
mod public;

use bifrost_types::{
    AccountError, AccountOperation, AttendeeRole, Calendar, CalendarEvent, CalendarId,
    CalendarProvenance, EventAttendee, EventAvailability, EventCreate, EventId, EventOrganizer,
    EventPatch, EventRange, EventRecurrence, EventSearchRequest, EventStatus, EventTime,
    EventVisibility, Page, ProtocolKind, RsvpStatus,
};
use serde_json::{Value, json};

use crate::client::Resource;
use crate::etag::GraphETag;
use crate::types::{CalendarEmailAddress, ODataCollection};

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
    let cursor = crate::paging::decode_link_cursor(&account.client, range.page_cursor, operation)
        .map_err(|detail| {
        AccountError::invalid_page_cursor(operation, bifrost_types::Protocol::Graph, detail)
    })?;
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
        .map(|next| crate::paging::encode_cursor(operation, &next.as_str()));
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
    if super::foreign::parse_public_calendar(&event.0).is_some() {
        let (folder, item) = super::foreign::parse_public_event(&event.0).ok_or_else(|| {
            graph_error::invalid_account_error(
                AccountOperation::EventGet,
                "public event id requires a folder and item id",
            )
        })?;
        return public::get(&account, folder, item).await;
    }
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventGet)?;
    let event = fetch_graph_event(
        &account,
        &calendar_id,
        &event_id,
        AccountOperation::EventGet,
    )
    .await?;
    Ok(event_from_graph(calendar_id, event))
}

async fn fetch_graph_event(
    account: &GraphAccount,
    calendar_id: &str,
    event_id: &str,
    operation: AccountOperation,
) -> Result<GraphEvent, AccountError> {
    let path = event_url(account, calendar_id, event_id);
    account
        .client
        .get_json_prefer::<GraphEvent>(Resource::Outlook, &path, EVENT_TIMEZONE_PREFER)
        .await
        .map_err(|error| into_error(error, operation))
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
        .post::<GraphEvent, _>(
            Resource::Outlook,
            &path,
            &body,
            bifrost_net::ReplayPolicy::Never,
        )
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
    let etag = patch
        .expected_version
        .as_ref()
        .map(|version| {
            version.expected_etag(bifrost_types::error::Protocol::Graph, operation, false)
        })
        .transpose()?;
    // Recurrence serialization may need the existing series anchor. This
    // read supplies content only; it never replaces the caller's validator.
    let current =
        fetch_graph_event(&account, &event_calendar_id, &native_event_id, operation).await?;
    let series = CurrentSeries {
        recurrence: current.recurrence.as_ref(),
        time_zone: current.original_start_time_zone.as_deref(),
    };
    let body = graph_event_from_patch(&patch, series)?;
    let path = event_url(&account, &event_calendar_id, &native_event_id);
    let legacy_etag = current
        .change_key
        .as_deref()
        .map(super::inventory::graph_change_key_etag);
    let etag = etag.or(legacy_etag.as_deref());
    let result = if let Some(etag) = etag {
        account
            .client
            .patch_if_match(
                Resource::Outlook,
                &path,
                &GraphETag::from_odata_annotation(etag),
                &body,
                bifrost_net::ReplayPolicy::Never,
            )
            .await
    } else {
        account
            .client
            .patch(
                Resource::Outlook,
                &path,
                &body,
                bifrost_net::ReplayPolicy::Never,
            )
            .await
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
    delete_if_version(account, event, None).await
}

pub(crate) async fn delete_if_version(
    account: GraphAccount,
    event: EventId,
    expected_version: Option<bifrost_types::ServerVersion>,
) -> Result<(), AccountError> {
    public::reject_event(&event, AccountOperation::EventDelete)?;
    let etag = if let Some(version) = &expected_version {
        Some(
            version
                .expected_etag(
                    bifrost_types::error::Protocol::Graph,
                    AccountOperation::EventDelete,
                    false,
                )?
                .to_string(),
        )
    } else {
        get(account.clone(), event.clone())
            .await?
            .etag
            .map(bifrost_types::EntityTag::into_string)
    };
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventDelete)?;
    let path = event_url(&account, &calendar_id, &event_id);
    let result = if let Some(etag) = etag.as_deref() {
        account
            .client
            .delete_if_match(
                Resource::Outlook,
                &path,
                &GraphETag::from_odata_annotation(etag),
                bifrost_net::ReplayPolicy::Never,
            )
            .await
    } else {
        account
            .client
            .delete(Resource::Outlook, &path, bifrost_net::ReplayPolicy::Never)
            .await
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
            bifrost_net::ReplayPolicy::Never,
        )
        .await
        .map_err(|error| into_error(error, AccountOperation::EventRsvp))
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchApiPosition {
    from: u32,
}

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
    let position: SearchApiPosition =
        crate::paging::decode_cursor(cursor?, AccountOperation::EventSearch).ok()?;
    Some(position.from)
}

fn search_api_cursor(from: u32) -> bifrost_types::PageCursor {
    crate::paging::encode_cursor(AccountOperation::EventSearch, &SearchApiPosition { from })
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
    let cursor =
        crate::paging::decode_paged_cursor(&account.client, request.page_cursor, operation)
            .map_err(|detail| {
                AccountError::invalid_page_cursor(operation, bifrost_types::Protocol::Graph, detail)
            })?;
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
                        Some(crate::paging::encode_paged_cursor(
                            &url, consumed, operation,
                        ))
                    } else {
                        next_link
                            .as_ref()
                            .map(|next| crate::paging::encode_paged_cursor(next, 0, operation))
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
        .post::<GraphSearchResponse, _>(
            Resource::Outlook,
            "/search/query",
            &body,
            bifrost_net::ReplayPolicy::Idempotent,
        )
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
        etag: event
            .change_key
            .as_deref()
            .map(super::inventory::graph_change_key_etag)
            .map(GraphETag::into_entity_tag),
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

impl RecurrenceRefusal {
    pub(crate) fn into_account_error(self, operation: AccountOperation) -> AccountError {
        match self {
            Self::Unsupported(message) => unsupported_error(operation, message),
            Self::Malformed { field, message } => {
                graph_error::invalid_argument_account_error(operation, field, message)
            }
        }
    }
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

pub(super) fn join_event_id(calendar_id: &str, event_id: &str) -> String {
    super::foreign::GraphId::Event {
        calendar: calendar_id,
        event: event_id,
    }
    .encode()
}

fn split_event_id(
    value: &str,
    operation: AccountOperation,
) -> Result<(String, String), AccountError> {
    super::foreign::GraphId::parse_event(value)
        .and_then(|id| match id {
            super::foreign::GraphId::Event { calendar, event } => {
                Some((calendar.to_string(), event.to_string()))
            }
            _ => None,
        })
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
        email_address: Some(CalendarEmailAddress {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn graph_time(value: &str) -> GraphDateTime {
        GraphDateTime {
            date_time: Some(value.to_string()),
            time_zone: Some("UTC".to_string()),
        }
    }

    #[tokio::test]
    async fn malformed_public_item_reads_are_refused_before_io() {
        for tail in ["folder", "folder\u{1e}", "\u{1e}item"] {
            let client = crate::client::GraphClient::new("token");
            let account = GraphAccount::new_for_tests(
                client.clone(),
                super::super::PushMode::GraphSubscriptions,
            );
            let error = get(account, EventId(format!("public-calendar\u{1e}{tail}")))
                .await
                .unwrap_err();
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed,
                )
            );
            assert!(client.take_rest_requests().is_empty());
        }
    }

    #[tokio::test]
    async fn caller_event_delete_version_reaches_the_wire_without_a_read() {
        use bifrost_types::Account;

        for expected in [Some("W/\"stale\""), None] {
            let client = crate::client::GraphClient::new("token");
            let account = GraphAccount::new_for_tests(
                client.clone(),
                super::super::PushMode::GraphSubscriptions,
            );
            if expected.is_none() {
                client.script_rest([
                    crate::client::ScriptedRestResponse::json(
                        reqwest::StatusCode::OK,
                        serde_json::json!({"id": "one", "@odata.etag": "W/\"fresh\"", "changeKey": "fresh"}),
                    ),
                    crate::client::ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
                ]);
            } else {
                client.script_rest([crate::client::ScriptedRestResponse::empty(
                    reqwest::StatusCode::PRECONDITION_FAILED,
                )]);
            }
            let result = account
                .event_delete_if_version(
                    EventId("calendar::one".into()),
                    expected.map(|value| bifrost_types::ServerVersion::ETag(value.into())),
                )
                .await;
            if expected.is_some() {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.expect("absent version preserves the immediate-read delete");
            }
            let requests = client.take_rest_requests();
            assert_eq!(requests.len(), if expected.is_some() { 1 } else { 2 });
            let write = requests.last().unwrap();
            assert_eq!(write.method, "DELETE");
            assert_eq!(write.if_match.as_deref(), expected.or(Some("W/\"fresh\"")));
        }
    }

    #[tokio::test]
    async fn caller_event_version_is_not_replaced_by_the_content_read() {
        for expected in [Some("W/\"stale\""), None] {
            let client = crate::client::GraphClient::new("token");
            client.script_rest([
                crate::client::ScriptedRestResponse::json(
                    reqwest::StatusCode::OK,
                    serde_json::json!({"id": "event-1", "changeKey": "fresh"}),
                ),
                crate::client::ScriptedRestResponse::empty(if expected.is_some() {
                    reqwest::StatusCode::PRECONDITION_FAILED
                } else {
                    reqwest::StatusCode::NO_CONTENT
                }),
            ]);
            let account = GraphAccount::new_for_tests(
                client.clone(),
                super::super::PushMode::GraphSubscriptions,
            );
            let result = update(
                account,
                EventId("cal::event-1".into()),
                EventPatch {
                    expected_version: expected
                        .map(|etag| bifrost_types::ServerVersion::ETag(etag.into())),
                    title: Some(Some("changed".into())),
                    ..Default::default()
                },
            )
            .await;
            if expected.is_some() {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.expect("an absent version keeps ordinary writes working");
            }
            let requests = client.take_rest_requests();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[1].method, "PATCH");
            assert_eq!(
                requests[1].if_match.as_deref(),
                expected.or(Some("W/\"fresh\""))
            );
        }
    }

    #[tokio::test]
    async fn event_update_preflight_failure_names_the_update_operation() {
        let client = crate::client::GraphClient::new("token");
        client.script_rest([crate::client::ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({
                "error": { "code": "ErrorItemNotFound", "message": "missing" }
            }),
        )]);
        let account =
            GraphAccount::new_for_tests(client.clone(), super::super::PushMode::GraphSubscriptions);

        let error = update(
            account,
            EventId("cal::event-1".into()),
            EventPatch {
                expected_version: Some(bifrost_types::ServerVersion::ETag(
                    "W/\"caller-version\"".into(),
                )),
                title: Some(Some("changed".into())),
                ..Default::default()
            },
        )
        .await
        .expect_err("a failed update preflight must fail the update");

        assert_eq!(error.operation(), Some(AccountOperation::EventUpdate));
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
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
                    email_address: Some(CalendarEmailAddress {
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
            ("FREQ=MONTHLY;INTERVAL=2147483647", "2026-01-31"),
        ];
        // The shared RRULE parser bounds INTERVAL at i32::MAX, so a larger one
        // is refused as a malformed rule before it reaches the series check.
        assert!(matches!(
            graph_recurrence_from_rrule("FREQ=MONTHLY;INTERVAL=4000000000", "2026-01-31"),
            Err(RecurrenceRefusal::Malformed { .. })
        ));
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
        assert_eq!(
            requests[1]
                .headers
                .get("if-match")
                .and_then(|value| value.to_str().ok()),
            Some("W/\"ck1\""),
            "Graph changeKey must be sent as its weak HTTP entity-tag"
        );
        let body: Value =
            serde_json::from_slice(requests[1].body.as_ref().expect("patch carries a body"))
                .expect("patch body is json");
        assert_eq!(body["recurrence"]["range"]["startDate"], "2026-05-04");
        assert_eq!(body["recurrence"]["pattern"]["type"], "weekly");
        // RFC 5545's default week start, written explicitly because Graph's
        // own default is Sunday.
        assert_eq!(body["recurrence"]["pattern"]["firstDayOfWeek"], "monday");
    }

    #[tokio::test]
    async fn delete_uses_the_change_key_as_an_http_entity_tag() {
        let (account, script) =
            scripted_search_account_pages(vec![json!({"id":"e1", "changeKey":"ck1"}), json!({})]);

        delete(account, EventId("calendar::e1".to_string()))
            .await
            .expect("delete succeeds");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, reqwest::Method::GET);
        assert_eq!(requests[1].method, reqwest::Method::DELETE);
        assert_eq!(
            requests[1]
                .headers
                .get("if-match")
                .and_then(|value| value.to_str().ok()),
            Some("W/\"ck1\"")
        );
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
            ("freq=weekly;byday=mo,we;", "malformed"),
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

#[cfg(test)]
mod sync_identity_tests {
    use super::*;

    #[test]
    fn sync_event_identity_is_accepted_by_the_typed_calendar_parser() {
        for ty in [
            bifrost_types::ObjectType::Event,
            bifrost_types::ObjectType::CalendarEvent,
        ] {
            let scope = bifrost_types::CursorScope::FolderType {
                folder: bifrost_types::FolderId("calendar".into()),
                ty,
            };
            let id = super::super::inventory::sync_object_id(&scope, "event");
            assert_eq!(
                split_event_id(&id.0, AccountOperation::EventGet).expect("typed id"),
                ("calendar".to_string(), "event".to_string())
            );
        }
    }
}
