//! Read-only public calendar routing and bounded expanded-view continuation.
use super::*;
use crate::account::foreign;
use crate::ews::{self, contacts::Node};
use bifrost_types::{ErrorScope, FolderId};
use futures::{StreamExt, stream};
use std::collections::BTreeSet;

const HYDRATE_CHUNK: usize = 20;
const HYDRATE_CONCURRENCY: usize = 4;

const CAP: u32 = 250;
const MAX_REQUESTS: u32 = 10_000;
const MAX_IDS: usize = 100_000;

pub(super) async fn calendars(account: &GraphAccount) -> Vec<Calendar> {
    let routing = account.routing_map.read().await;
    let meta = account.public_folder_meta.read().await;
    let mut result: Vec<_> = meta
        .iter()
        .filter(|(id, meta)| {
            routing.contains_key(*id)
                && meta.effective_rights.read
                && ews::EwsItemKind::from_folder_class(meta.folder_class.as_deref().unwrap_or(""))
                    == ews::EwsItemKind::Calendar
        })
        .map(|(id, meta)| Calendar {
            id: foreign::public_calendar(&id.0),
            native_id: id.0.clone(),
            name: meta.display_name.clone(),
            color: None,
            provenance: CalendarProvenance {
                provider: ProtocolKind::Graph,
                native: id.0.clone(),
                calendar_native: None,
            },
            is_default: false,
            can_create_events: false,
            can_update_events: false,
            can_delete_events: false,
        })
        .collect();
    result.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    result
}

pub(super) fn reject_calendar(id: &CalendarId, op: AccountOperation) -> Result<(), AccountError> {
    if foreign::parse_public_calendar(&id.0).is_some() {
        return Err(graph_error::unsupported_account_error(op));
    }
    Ok(())
}
pub(super) fn reject_event(id: &EventId, op: AccountOperation) -> Result<(), AccountError> {
    if foreign::parse_public_event(&id.0).is_some() {
        return Err(graph_error::unsupported_account_error(op));
    }
    Ok(())
}
async fn routing(
    account: &GraphAccount,
    folder: &str,
    op: AccountOperation,
) -> Result<ews::EwsHeaders, AccountError> {
    account
        .public_folder_routing(&FolderId(folder.into()))
        .await
        .map(|r| r.headers())
        .ok_or_else(|| {
            graph_error::stale_local_state_error(
                op,
                ErrorScope::Calendar {
                    id: foreign::public_calendar(folder),
                },
                "public calendar has no routing entry",
            )
        })
}
fn context(op: AccountOperation, folder: &str) -> GraphErrorContext {
    GraphErrorContext::ews(op).with_scope(ErrorScope::Calendar {
        id: foreign::public_calendar(folder),
    })
}
fn limit(folder: &str) -> AccountError {
    graph_error::into_account_error(
        crate::error::GraphError::LimitExceeded {
            message: "public CalendarView density or continuation budget exceeded".into(),
        },
        context(AccountOperation::EventsInRange, folder),
    )
}
fn malformed(message: &str) -> ews::EwsError {
    ews::EwsError::MalformedXml(bifrost_types::DiagnosticText::support_only(
        message.to_string(),
    ))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    public_calendar_v1: String,
    start: String,
    end: String,
    start_zone: Option<String>,
    end_zone: Option<String>,
    // Integer 100ns ticks preserve Exchange date-time precision. Both children
    // include the split boundary; stable instance ids deduplicate overlap.
    windows: Vec<(i64, i64)>,
    seen: BTreeSet<String>,
    ready: Vec<String>,
    requests: u32,
}
fn ticks(time: &EventTime, op: AccountOperation) -> Result<i64, AccountError> {
    let invalid = || {
        graph_error::invalid_argument_account_error(
            op,
            "range",
            "public CalendarView needs valid start/end instants and a resolvable zone",
        )
    };
    let instant = match time.value.parse::<jiff::Timestamp>() {
        Ok(instant) => instant,
        Err(_) => {
            let local = time
                .value
                .parse::<civil::DateTime>()
                .map_err(|_| invalid())?;
            let zone =
                resolve_time_zone(time.timezone.as_deref().unwrap_or("UTC")).ok_or_else(invalid)?;
            local.to_zoned(zone).map_err(|_| invalid())?.timestamp()
        }
    };
    let nanos = instant.as_nanosecond();
    if nanos % 100 != 0 {
        return Err(graph_error::invalid_argument_account_error(
            op,
            "range",
            "CalendarView boundaries need 100ns precision",
        ));
    }
    i64::try_from(nanos / 100)
        .map_err(|_| graph_error::invalid_account_error(op, "CalendarView boundary out of range"))
}
fn stamp(ticks: i64) -> Result<String, ews::EwsError> {
    jiff::Timestamp::from_nanosecond(i128::from(ticks) * 100)
        .map(|t| t.to_string())
        .map_err(|_| malformed("invalid CalendarView cursor boundary"))
}

pub(super) async fn range(
    account: &GraphAccount,
    folder: &str,
    query: &EventRange,
) -> Result<Page<CalendarEvent>, AccountError> {
    let op = AccountOperation::EventsInRange;
    let headers = routing(account, folder, op).await?;
    let map = |e| graph_error::ews_error_to_account_error(e, context(op, folder));
    let start = ticks(&query.start, op)?;
    let end = ticks(&query.end, op)?;
    if start >= end {
        return Err(graph_error::invalid_argument_account_error(
            op,
            "range",
            "CalendarView end must follow start",
        ));
    }
    let mut pos = if let Some(bytes) = &query.page_cursor {
        let p: Position = serde_json::from_slice(bytes).map_err(|_| {
            graph_error::invalid_account_error(op, "invalid public CalendarView cursor")
        })?;
        if p.public_calendar_v1 != folder
            || p.start != query.start.value
            || p.end != query.end.value
            || p.start_zone != query.start.timezone
            || p.end_zone != query.end.timezone
            || (p.windows.is_empty() && p.ready.is_empty())
            || p.windows
                .iter()
                .any(|(a, b)| *a < start || *b > end || a >= b)
        {
            return Err(graph_error::invalid_account_error(
                op,
                "public CalendarView cursor belongs to another query or has invalid windows",
            ));
        }
        p
    } else {
        Position {
            public_calendar_v1: folder.into(),
            start: query.start.value.clone(),
            end: query.end.value.clone(),
            start_zone: query.start.timezone.clone(),
            end_zone: query.end.timezone.clone(),
            windows: vec![(start, end)],
            seen: BTreeSet::new(),
            ready: Vec::new(),
            requests: 0,
        }
    };
    let client = super::super::public_folder::ews_client(account).map_err(map)?;
    let cap = CAP;
    let page_limit = query.limit.unwrap_or(CAP).clamp(1, CAP) as usize;
    while pos.ready.is_empty() {
        let Some((a, b)) = pos.windows.pop() else {
            break;
        };
        if pos.requests >= MAX_REQUESTS || pos.seen.len() > MAX_IDS {
            return Err(limit(folder));
        }
        pos.requests += 1;
        let xml = client
            .execute(
                &ews::calendar::find_body(
                    folder,
                    &stamp(a).map_err(map)?,
                    &stamp(b).map_err(map)?,
                    cap,
                ),
                &headers,
            )
            .await
            .map_err(map)?;
        let page = ews::parse_find_items_response(&xml).map_err(map)?;
        let nodes = ews::contacts::contact_nodes(&xml).map_err(map)?;
        // A full cap is conservatively split even if the server claims final.
        if !page.includes_last || nodes.len() >= cap as usize {
            if b - a <= 1 {
                return Err(limit(folder));
            }
            let mid = a + (b - a) / 2;
            pos.windows.push((mid, b));
            pos.windows.push((a, mid));
            continue;
        }
        for node in nodes {
            if !is_calendar(&node) {
                return Err(graph_error::unsupported_item_error(
                    op,
                    ErrorScope::Calendar {
                        id: foreign::public_calendar(folder),
                    },
                ));
            }
            let native = node
                .child("ItemId")
                .and_then(|n| n.id.as_deref())
                .ok_or_else(|| map(malformed("calendar item missing id")))?;
            if node.value("CalendarItemType").as_deref() == Some("RecurringMaster") {
                return Err(graph_error::into_account_error(
                    crate::error::GraphError::ContractViolation {
                        message: "CalendarView returned an unexpanded master".into(),
                    },
                    context(op, folder),
                ));
            }
            if pos.seen.insert(native.to_string()) {
                pos.ready.push(native.to_string());
            }
        }
        if pos.seen.len() > MAX_IDS {
            return Err(limit(folder));
        }
        break;
    }
    let ready = pos
        .ready
        .drain(..page_limit.min(pos.ready.len()))
        .collect::<Vec<_>>();
    let (items, failed_ids) = hydrate(account, folder, &ready, op).await?;
    let next_cursor = if pos.windows.is_empty() && pos.ready.is_empty() {
        None
    } else {
        Some(serde_json::to_vec(&pos).expect("serializable CalendarView cursor"))
    };
    Ok(Page {
        items,
        next_cursor,
        estimated_total: None,
        failed_ids,
        skipped_scopes: Vec::new(),
    })
}

pub(super) async fn get(
    account: &GraphAccount,
    folder: &str,
    item: &str,
) -> Result<CalendarEvent, AccountError> {
    let op = AccountOperation::EventGet;
    let scope = ErrorScope::Event {
        id: foreign::public_event(folder, item),
    };
    let map = |e| {
        graph_error::ews_error_to_account_error(
            e,
            GraphErrorContext::ews(op).with_scope(scope.clone()),
        )
    };
    let headers = routing(account, folder, op).await?;
    let client = super::super::public_folder::ews_client(account).map_err(map)?;
    let xml = client
        .execute(&ews::calendar::get_body(item), &headers)
        .await
        .map_err(map)?;
    let nodes = ews::contacts::contact_nodes(&xml).map_err(map)?;
    if nodes.len() != 1 {
        return Err(map(malformed("GetItem must return exactly one item")));
    }
    materialize(folder, item, &nodes[0], op)
}

fn materialize(
    folder: &str,
    item: &str,
    node: &Node,
    op: AccountOperation,
) -> Result<CalendarEvent, AccountError> {
    let scope = ErrorScope::Event {
        id: foreign::public_event(folder, item),
    };
    if node.child("ItemId").and_then(|n| n.id.as_deref()) != Some(item) {
        return Err(graph_error::into_account_error(
            crate::error::GraphError::ContractViolation {
                message: "public calendar GetItem returned a different identity".into(),
            },
            GraphErrorContext::ews(op).with_scope(scope),
        ));
    }
    if !is_calendar(node) {
        return Err(graph_error::unsupported_item_error(op, scope));
    }
    project(folder, node).map_err(|e| {
        graph_error::ews_error_to_account_error(e, GraphErrorContext::ews(op).with_scope(scope))
    })
}

/// Request failures leave the caller's saved cursor replayable. A response
/// message belongs to one id, so its failure is consumed along with that id.
async fn hydrate(
    account: &GraphAccount,
    folder: &str,
    ids: &[String],
    op: AccountOperation,
) -> Result<(Vec<CalendarEvent>, Vec<String>), AccountError> {
    let headers = routing(account, folder, op).await?;
    let client = super::super::public_folder::ews_client(account)
        .map_err(|e| graph_error::ews_error_to_account_error(e, context(op, folder)))?;
    let client = std::sync::Arc::new(client);
    let chunks = ids
        .chunks(HYDRATE_CHUNK)
        .map(<[String]>::to_vec)
        .collect::<Vec<_>>();
    let mut pending = stream::iter(chunks)
        .map(move |chunk| {
            let client = std::sync::Arc::clone(&client);
            let headers = headers.clone();
            async move {
                let result = client.get_calendar_items(&chunk, &headers).await;
                (chunk, result)
            }
        })
        .buffered(HYDRATE_CONCURRENCY);
    let mut items = Vec::new();
    let mut failed = Vec::new();
    while let Some((chunk, response)) = pending.next().await {
        let rows = response
            .map_err(|e| graph_error::ews_error_to_account_error(e, context(op, folder)))?;
        for (native, row) in chunk.iter().zip(rows) {
            let result = row
                .map_err(|e| {
                    graph_error::ews_error_to_account_error(
                        e,
                        GraphErrorContext::ews(op).with_scope(ErrorScope::Event {
                            id: foreign::public_event(folder, native),
                        }),
                    )
                })
                .and_then(|node| materialize(folder, native, &node, op));
            match result {
                Ok(item) => items.push(item),
                Err(error) => {
                    tracing::warn!(
                        message_key = error.message_key(),
                        "public calendar item hydration failed"
                    );
                    failed.push(native.clone());
                }
            }
        }
    }
    Ok((items, failed))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchPosition {
    public_event_search_v1: String,
    query: String,
    include_cancelled: bool,
    offset: u32,
    next_offset: Option<u32>,
    ready: Vec<String>,
    requests: u32,
}

pub(super) async fn search(
    account: &GraphAccount,
    folder: &str,
    request: &EventSearchRequest,
) -> Result<Page<CalendarEvent>, AccountError> {
    let op = AccountOperation::EventSearch;
    let headers = routing(account, folder, op).await?;
    let map = |e| graph_error::ews_error_to_account_error(e, context(op, folder));
    let mut pos = if let Some(bytes) = &request.page_cursor {
        let pos: SearchPosition = serde_json::from_slice(bytes).map_err(|_| {
            graph_error::invalid_account_error(op, "invalid public event search cursor")
        })?;
        if pos.public_event_search_v1 != folder
            || pos.query != request.query
            || pos.include_cancelled != request.include_cancelled
        {
            return Err(graph_error::invalid_account_error(
                op,
                "public event search cursor belongs to another query",
            ));
        }
        pos
    } else {
        SearchPosition {
            public_event_search_v1: folder.into(),
            query: request.query.clone(),
            include_cancelled: request.include_cancelled,
            offset: 0,
            next_offset: Some(0),
            ready: Vec::new(),
            requests: 0,
        }
    };
    let client = super::super::public_folder::ews_client(account).map_err(map)?;
    let limit = request.limit.unwrap_or(CAP).clamp(1, CAP) as usize;
    let needle = request.query.to_ascii_lowercase();
    let mut items = Vec::new();
    let mut failed_ids = Vec::new();
    loop {
        if pos.ready.is_empty() {
            let Some(offset) = pos.next_offset.take() else {
                break;
            };
            if pos.requests >= MAX_REQUESTS {
                return Err(graph_error::into_account_error(
                    crate::error::GraphError::LimitExceeded {
                        message: "public event search page budget exceeded".into(),
                    },
                    context(op, folder),
                ));
            }
            pos.requests += 1;
            pos.offset = offset;
            let xml = client
                .execute(
                    &ews::calendar::search_body(folder, offset, &request.query),
                    &headers,
                )
                .await
                .map_err(map)?;
            let page = ews::parse_find_items_response(&xml).map_err(map)?;
            if !page.includes_last {
                pos.next_offset = Some(page.next_offset.filter(|next| *next > offset).ok_or_else(
                    || {
                        graph_error::into_account_error(
                            crate::error::GraphError::ContractViolation {
                                message: "public event search did not advance indexed offset"
                                    .into(),
                            },
                            context(op, folder),
                        )
                    },
                )?);
            }
            for node in ews::contacts::contact_nodes(&xml).map_err(map)? {
                let id = node
                    .child("ItemId")
                    .and_then(|n| n.id.clone())
                    .ok_or_else(|| map(malformed("search item has no id")))?;
                pos.ready.push(id);
            }
        }
        let count = (limit - items.len()).min(pos.ready.len());
        let ids = pos.ready.drain(..count).collect::<Vec<_>>();
        let (hydrated, failures) = hydrate(account, folder, &ids, op).await?;
        failed_ids.extend(failures);
        items.extend(hydrated.into_iter().filter(|event| {
            (request.include_cancelled || event.status != EventStatus::Cancelled)
                && event_matches(event, &needle)
        }));
        if items.len() >= limit {
            break;
        }
    }
    let next_cursor = if pos.ready.is_empty() && pos.next_offset.is_none() {
        None
    } else {
        Some(serde_json::to_vec(&pos).expect("serializable public event search cursor"))
    };
    Ok(Page {
        items,
        failed_ids,
        next_cursor,
        estimated_total: None,
        skipped_scopes: Vec::new(),
    })
}

fn is_calendar(node: &Node) -> bool {
    node.name == "CalendarItem"
        && node
            .value("ItemClass")
            .is_some_and(|class| ews::is_subclass(&class, "IPM.Appointment"))
}
fn response(value: Option<String>) -> RsvpStatus {
    match value.as_deref() {
        Some("Accept") => RsvpStatus::Accepted,
        Some("Decline") => RsvpStatus::Declined,
        Some("Tentative") => RsvpStatus::Tentative,
        Some("NoResponseReceived") => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
}
fn mailbox(node: &Node) -> Option<(String, Option<String>)> {
    let m = node.child("Mailbox")?;
    if m.value("RoutingType")
        .is_some_and(|s| !s.eq_ignore_ascii_case("SMTP"))
    {
        return None;
    }
    Some((m.value("EmailAddress")?, m.value("Name")))
}
fn recurrence_rule(node: &Node) -> Result<Option<String>, ews::EwsError> {
    let Some(rule) = node.child("Recurrence") else {
        if node.value("CalendarItemType").as_deref() == Some("RecurringMaster") {
            return Err(malformed("recurring master omitted Recurrence"));
        }
        return Ok(None);
    };
    let pattern = rule
        .children
        .first()
        .ok_or_else(|| malformed("empty recurrence"))?;
    let kind = match pattern.name.as_str() {
        "DailyRecurrence" => "daily",
        "WeeklyRecurrence" => "weekly",
        "AbsoluteMonthlyRecurrence" => "absoluteMonthly",
        "RelativeMonthlyRecurrence" => "relativeMonthly",
        "AbsoluteYearlyRecurrence" => "absoluteYearly",
        "RelativeYearlyRecurrence" => "relativeYearly",
        _ => return Err(malformed("unknown calendar recurrence pattern")),
    };
    let days = pattern.value("DaysOfWeek").map(|days| {
        days.split_whitespace()
            .flat_map(|day| match day {
                "Day" => vec![
                    "sunday",
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                ],
                "Weekday" => vec!["monday", "tuesday", "wednesday", "thursday", "friday"],
                "WeekendDay" => vec!["sunday", "saturday"],
                _ => vec![day],
            })
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>()
    });
    let month = pattern.value("Month").and_then(|month| {
        [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
        .iter()
        .position(|name| *name == month)
        .map(|n| n + 1)
    });
    let range = rule
        .children
        .get(1)
        .ok_or_else(|| malformed("recurrence missing range"))?;
    let range_kind = match range.name.as_str() {
        "NoEndRecurrence" => "noEnd",
        "NumberedRecurrence" => "numbered",
        "EndDateRecurrence" => "endDate",
        _ => return Err(malformed("unknown recurrence range")),
    };
    let number = |node: &Node, field: &str| node.value(field).and_then(|n| n.parse::<u32>().ok());
    let wire: GraphRecurrence = serde_json::from_value(json!({
        "pattern": { "type": kind, "interval": number(pattern,"Interval"), "month": month,
            "dayOfMonth": number(pattern,"DayOfMonth"), "daysOfWeek": days,
            "firstDayOfWeek": pattern.value("FirstDayOfWeek").map(|d|d.to_ascii_lowercase()),
            "index": pattern.value("DayOfWeekIndex").map(|d|d.to_ascii_lowercase()) },
        "range": { "type": range_kind, "startDate": range.value("StartDate"), "endDate": range.value("EndDate"), "numberOfOccurrences": number(range,"NumberOfOccurrences") }
    })).map_err(|_|malformed("invalid calendar recurrence"))?;
    rrule_from_graph(&wire)
        .map(Some)
        .ok_or_else(|| malformed("unrepresentable calendar recurrence"))
}

fn project(folder: &str, node: &Node) -> Result<CalendarEvent, ews::EwsError> {
    if !is_calendar(node) {
        return Err(malformed("CalendarView returned a non-calendar item"));
    }
    let id = node
        .child("ItemId")
        .ok_or_else(|| malformed("calendar item missing ItemId"))?;
    let native = id
        .id
        .clone()
        .ok_or_else(|| malformed("calendar item missing id"))?;
    let all_day = node.value("IsAllDayEvent").as_deref() == Some("true");
    let time = |field: &str, zone: &str| -> Result<EventTime, ews::EwsError> {
        let mut value = node
            .value(field)
            .ok_or_else(|| malformed("calendar item missing time"))?;
        let timezone = node.child(zone).and_then(|n| n.id.clone());
        if all_day {
            // EWS defaults response timestamps to UTC, including all-day
            // boundaries. Recover the calendar date in the item's own zone.
            if let Ok(instant) = value.parse::<jiff::Timestamp>() {
                let zone = timezone
                    .as_deref()
                    .and_then(resolve_time_zone)
                    .ok_or_else(|| malformed("all-day calendar timezone cannot be resolved"))?;
                value = instant.to_zoned(zone).date().to_string();
            } else {
                value = value
                    .parse::<civil::DateTime>()
                    .map_err(|_| malformed("invalid all-day time"))?
                    .date()
                    .to_string();
            }
        }
        Ok(EventTime { value, timezone })
    };
    let mut attendees = Vec::new();
    for (field, role) in [
        ("RequiredAttendees", AttendeeRole::Required),
        ("OptionalAttendees", AttendeeRole::Optional),
        ("Resources", AttendeeRole::Resource),
    ] {
        if let Some(group) = node.child(field) {
            for row in &group.children {
                if let Some((email, name)) = mailbox(row) {
                    attendees.push(EventAttendee {
                        email,
                        name,
                        role,
                        status: response(row.value("ResponseType")),
                    });
                }
            }
        }
    }
    let recurrence_id = match node.value("CalendarItemType").as_deref() {
        Some("Occurrence" | "Exception") => Some(
            node.value("OriginalStart")
                .ok_or_else(|| malformed("occurrence missing OriginalStart"))?,
        ),
        _ => None,
    };
    Ok(CalendarEvent {
        id: foreign::public_event(folder, &native),
        calendar_id: foreign::public_calendar(folder),
        native_id: native.clone(),
        uid: node.value("UID"),
        etag: id.change_key.clone(),
        provenance: CalendarProvenance {
            provider: ProtocolKind::Graph,
            native,
            calendar_native: Some(folder.into()),
        },
        title: node.value("Subject"),
        description: node.value("Body").or_else(|| node.value("Preview")),
        location: node.value("Location"),
        start: time("Start", "StartTimeZone")?,
        end: node
            .value("End")
            .map(|_| time("End", "EndTimeZone"))
            .transpose()?,
        is_all_day: all_day,
        status: if node.value("IsCancelled").as_deref() == Some("true") {
            EventStatus::Cancelled
        } else {
            EventStatus::Confirmed
        },
        availability: match node.value("LegacyFreeBusyStatus").as_deref() {
            Some("Free") => EventAvailability::Free,
            Some("Busy") => EventAvailability::Busy,
            Some("Tentative") => EventAvailability::Tentative,
            Some("OOF") => EventAvailability::OutOfOffice,
            _ => EventAvailability::Unknown,
        },
        visibility: match node.value("Sensitivity").as_deref() {
            Some("Private" | "Personal") => EventVisibility::Private,
            Some("Confidential") => EventVisibility::Confidential,
            _ => EventVisibility::Default,
        },
        self_response: response(node.value("MyResponseType")),
        organizer: node
            .child("Organizer")
            .and_then(mailbox)
            .map(|(email, name)| EventOrganizer { email, name }),
        attendees,
        reminders: Vec::new(),
        recurrence: EventRecurrence {
            rrule: recurrence_rule(node)?,
            recurrence_id,
            ..Default::default()
        },
        html_link: None,
        raw_ical: None,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_account};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource, TokenSource};
    use bifrost_types::{AccountErrorKind, InternalErrorKind, ResourceKind};
    use std::sync::Arc;

    fn account(answers: Vec<String>) -> (Arc<ScriptedDispatch>, GraphAccount) {
        account_canned(
            answers
                .into_iter()
                .map(|body| Canned::Response {
                    status: reqwest::StatusCode::OK,
                    headers: reqwest::header::HeaderMap::new(),
                    body: bytes::Bytes::from(body),
                })
                .collect(),
        )
    }
    fn account_canned(answers: Vec<Canned>) -> (Arc<ScriptedDispatch>, GraphAccount) {
        let script = ScriptedDispatch::new(answers);
        let token: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
        let net = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::clone(&token),
            RetryPolicy::disabled(),
        );
        let client =
            crate::client::GraphClient::with_account_net(net, "https://graph.test/v1.0", token);
        (
            script,
            GraphAccount::new_for_tests(client, crate::account::PushMode::GraphSubscriptions),
        )
    }
    async fn seed(account: &GraphAccount, folder: &str, class: &str, read: bool) {
        account
            .seed_public_folder_meta_for_tests(
                FolderId(folder.into()),
                crate::account::cursor::PublicFolderRouting {
                    anchor_mailbox: "anchor@example.test".into(),
                    public_folder_mailbox: Some("content@example.test".into()),
                },
                crate::account::public_folder::PublicFolderMeta {
                    display_name: folder.into(),
                    folder_class: Some(class.into()),
                    parent: None,
                    effective_rights: ews::EwsEffectiveRights {
                        read,
                        ..Default::default()
                    },
                },
            )
            .await;
    }
    fn soap(items: &str, last: bool) -> String {
        format!(
            r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"><s:Body><m:FindItemResponse><m:ResponseMessages><m:FindItemResponseMessage ResponseClass="Success"><m:ResponseCode>NoError</m:ResponseCode><m:RootFolder IncludesLastItemInRange="{last}"><t:Items>{items}</t:Items></m:RootFolder></m:FindItemResponseMessage></m:ResponseMessages></m:FindItemResponse></s:Body></s:Envelope>"#
        )
    }
    fn get_soap(items: &str) -> String {
        soap(items, true)
            .replace("FindItem", "GetItem")
            .replace("<m:RootFolder IncludesLastItemInRange=\"true\">", "")
            .replace("</m:RootFolder>", "")
            .replace("t:Items", "m:Items")
    }
    fn item(id: &str, kind: &str) -> String {
        format!(
            r#"<t:CalendarItem><t:ItemId Id="{id}" ChangeKey="v1"/><t:ItemClass>IPM.Appointment.Custom</t:ItemClass><t:CalendarItemType>{kind}</t:CalendarItemType><t:UID>series</t:UID><t:Subject>Design &amp; build</t:Subject><t:Start>2026-10-20T10:00:00Z</t:Start><t:End>2026-10-20T12:00:00Z</t:End><t:OriginalStart>2026-10-20T09:00:00Z</t:OriginalStart><t:StartTimeZone Id="UTC"/><t:EndTimeZone Id="UTC"/><t:IsAllDayEvent>false</t:IsAllDayEvent><t:Location>Room</t:Location><t:Preview>Short</t:Preview><t:Body>Full text</t:Body><t:LegacyFreeBusyStatus>OOF</t:LegacyFreeBusyStatus><t:MyResponseType>Accept</t:MyResponseType><t:Organizer><t:Mailbox><t:Name>Ada</t:Name><t:EmailAddress>ada@example.test</t:EmailAddress><t:RoutingType>SMTP</t:RoutingType></t:Mailbox></t:Organizer><t:RequiredAttendees><t:Attendee><t:Mailbox><t:Name>Grace</t:Name><t:EmailAddress>grace@example.test</t:EmailAddress></t:Mailbox><t:ResponseType>Tentative</t:ResponseType></t:Attendee></t:RequiredAttendees></t:CalendarItem>"#
        )
    }
    fn batch_soap(responses: &[String]) -> String {
        let frames = responses
            .iter()
            .map(|xml| {
                let start = xml.find("<m:GetItemResponseMessage ").unwrap();
                let end = xml.find("</m:GetItemResponseMessage>").unwrap()
                    + "</m:GetItemResponseMessage>".len();
                &xml[start..end]
            })
            .collect::<String>();
        get_soap("").replace("<m:GetItemResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode><m:Items></m:Items></m:GetItemResponseMessage>", &frames)
    }
    fn item_error(code: &str) -> String {
        get_soap("")
            .replace("ResponseClass=\"Success\"", "ResponseClass=\"Error\"")
            .replace("NoError", code)
    }
    fn query() -> EventRange {
        EventRange {
            calendar_id: foreign::public_calendar("cal"),
            start: EventTime {
                value: "2026-10-20T00:00:00Z".into(),
                timezone: None,
            },
            end: EventTime {
                value: "2026-10-22T00:00:00Z".into(),
                timezone: None,
            },
            page_cursor: None,
            limit: Some(2),
        }
    }
    fn bodies(script: &ScriptedDispatch) -> Vec<String> {
        script
            .requests()
            .iter()
            .map(|r| String::from_utf8(r.body.as_ref().unwrap().to_vec()).unwrap())
            .collect()
    }
    #[tokio::test]
    async fn nc2c_lists_readable_public_calendars_without_pins() {
        let (script, account) = account(vec![r#"{"value":[]}"#.into()]);
        for (id, class, read) in [
            ("cal", "IPF.Appointment.Custom", true),
            ("wrong", "IPF.AppointmentBook", true),
            ("people", "IPF.Contact", true),
            ("denied", "IPF.Appointment", false),
        ] {
            seed(&account, id, class, read).await;
        }
        let list = super::super::calendars_list(account).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, foreign::public_calendar("cal"));
        assert_eq!(list[0].native_id, "cal");
        assert_eq!(list[0].provenance.native, "cal");
        assert!(
            !list[0].can_create_events && !list[0].can_update_events && !list[0].can_delete_events
        );
        assert_eq!(script.requests().len(), 1);
    }
    #[tokio::test]
    async fn nc2c_expands_instances_and_resolves_each_occurrence_independently() {
        let a = item("occurrence-1", "Occurrence");
        let b = item("exception-2", "Exception");
        let (script, account) = account(vec![
            soap(&format!("{a}{b}"), true),
            batch_soap(&[get_soap(&a), get_soap(&b)]),
            get_soap(&a),
            get_soap(&b),
        ]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let page = super::super::events_in_range(account.clone(), query())
            .await
            .unwrap();
        assert!(page.next_cursor.is_none());
        assert_eq!(page.items.len(), 2);
        assert_ne!(page.items[0].id, page.items[1].id);
        for event in page.items {
            assert_eq!(event.title.as_deref(), Some("Design & build"));
            assert_eq!(event.description.as_deref(), Some("Full text"));
            assert_eq!(event.location.as_deref(), Some("Room"));
            assert_eq!(event.start.timezone.as_deref(), Some("UTC"));
            assert_eq!(event.end.as_ref().unwrap().value, "2026-10-20T12:00:00Z");
            assert_eq!(
                event.recurrence.recurrence_id.as_deref(),
                Some("2026-10-20T09:00:00Z")
            );
            assert_eq!(event.organizer.as_ref().unwrap().email, "ada@example.test");
            assert_eq!(event.attendees[0].status, RsvpStatus::Tentative);
            assert_eq!(event.self_response, RsvpStatus::Accepted);
            assert_eq!(event.availability, EventAvailability::OutOfOffice);
            assert_eq!(
                super::super::get(account.clone(), event.id.clone())
                    .await
                    .unwrap(),
                event
            );
        }
        let bodies = bodies(&script);
        assert!(bodies[0].contains("<m:CalendarView"));
        assert!(bodies[0].contains("MaxEntriesReturned=\"250\""));
        assert!(bodies[0].contains("StartDate=\"2026-10-20T00:00:00Z\""));
        assert!(bodies[0].contains("EndDate=\"2026-10-22T00:00:00Z\""));
        assert!(!bodies[0].contains("IndexedPageItemView"));
        assert!(bodies[2].contains("<t:ItemId Id=\"occurrence-1\""));
        assert!(bodies[3].contains("<t:ItemId Id=\"exception-2\""));
        assert!(
            bodies[1].contains("calendar:RequiredAttendees") && bodies[1].contains("item:Body")
        );
        assert!(!bodies[1].contains("message:"));
        for request in script.requests() {
            assert_eq!(request.url.path(), "/EWS/Exchange.asmx");
            assert_eq!(
                request.headers["X-PublicFolderMailbox"],
                "content@example.test"
            );
            assert_eq!(request.headers["X-AnchorMailbox"], "anchor@example.test");
        }
    }
    #[tokio::test]
    async fn nc2c_split_cursor_deduplicates_overlap_and_binds_original_query() {
        let a = item("spanning", "Single");
        let b = item("later", "Exception");
        let (script, account) = account(vec![
            soap(&a, false),
            soap(&a, true),
            get_soap(&a),
            soap(&format!("{a}{b}"), true),
            get_soap(&b),
        ]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let first = super::super::events_in_range(account.clone(), query())
            .await
            .unwrap();
        assert_eq!(first.items.len(), 1);
        let bytes = first.next_cursor.unwrap();
        for changed in 0..4 {
            let mut q = query();
            q.page_cursor = Some(bytes.clone());
            match changed {
                0 => q.calendar_id = foreign::public_calendar("other"),
                1 => q.start.value = "2026-10-19T00:00:00Z".into(),
                2 => q.end.value = "2026-10-23T00:00:00Z".into(),
                _ => q.start.timezone = Some("UTC".into()),
            }
            seed(&account, "other", "IPF.Appointment", true).await;
            let error = super::super::events_in_range(account.clone(), q)
                .await
                .unwrap_err();
            assert!(matches!(error.kind(), AccountErrorKind::Request(_)));
        }
        let mut q = query();
        q.page_cursor = Some(bytes);
        let second = super::super::events_in_range(account, q).await.unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].native_id, "later");
        assert!(second.next_cursor.is_none());
        let b = bodies(&script);
        assert_eq!(b.len(), 5);
        assert!(b[1].contains("EndDate=\"2026-10-21T00:00:00Z\""));
        assert!(b[3].contains("StartDate=\"2026-10-21T00:00:00Z\""));
    }
    #[tokio::test]
    async fn nc2c_small_pages_drain_accepted_window_without_requery() {
        let a = item("a", "Occurrence");
        let b = item("b", "Exception");
        let (script, account) = account(vec![
            soap(&format!("{a}{b}"), true),
            get_soap(&a),
            get_soap(&b),
        ]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let mut q = query();
        q.limit = Some(1);
        let first = super::super::events_in_range(account.clone(), q.clone())
            .await
            .unwrap();
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].native_id, "a");
        q.page_cursor = first.next_cursor;
        assert!(q.page_cursor.is_some());
        let next = super::super::events_in_range(account, q).await.unwrap();
        assert_eq!(next.items.len(), 1);
        assert_eq!(next.items[0].native_id, "b");
        assert!(next.next_cursor.is_none());
        assert_eq!(
            bodies(&script)
                .iter()
                .filter(|b| b.contains("<m:FindItem"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn nc2c_master_and_truncated_xml_are_not_successful_views() {
        for xml in [
            soap(&item("master", "RecurringMaster"), true),
            soap(&item("broken", "Single"), true)
                .trim_end_matches("</s:Envelope>")
                .to_string(),
        ] {
            let (_, account) = account(vec![xml]);
            seed(&account, "cal", "IPF.Appointment", true).await;
            assert!(matches!(
                super::super::events_in_range(account, query())
                    .await
                    .unwrap_err()
                    .kind(),
                AccountErrorKind::Protocol(_)
            ));
        }
    }

    #[tokio::test]
    async fn nc2c_density_and_full_cap_never_report_exhaustion() {
        for last in [true, false] {
            let dense = (0..250)
                .map(|i| item(&format!("instance-{i}"), "Occurrence"))
                .collect::<String>();
            let (script, account) = account(vec![soap(&dense, last)]);
            seed(&account, "cal", "IPF.Appointment", true).await;
            let mut q = query();
            q.limit = Some(1);
            q.end.value = "2026-10-20T00:00:00.0000001Z".into();
            let error = super::super::events_in_range(account, q).await.unwrap_err();
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Internal(InternalErrorKind::LimitExceeded)
            );
            assert_eq!(script.requests().len(), 1);
        }
    }
    #[tokio::test]
    async fn nc2c_get_kind_identity_notfound_and_missing_routing() {
        for body in [
            get_soap(&item("a", "Single").replace("CalendarItem>", "Contact>")),
            get_soap(&item("a", "Single").replace("IPM.Appointment.Custom", "IPM.AppointmentBook")),
        ] {
            let (_, account) = account(vec![body]);
            seed(&account, "cal", "IPF.Appointment", true).await;
            let error = super::super::get(account, foreign::public_event("cal", "a"))
                .await
                .unwrap_err();
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Unsupported(AccountOperation::EventGet)
            );
        }
        let (_, a) = account(vec![get_soap(&item("wrong", "Single"))]);
        seed(&a, "cal", "IPF.Appointment", true).await;
        assert!(matches!(
            super::super::get(a, foreign::public_event("cal", "a"))
                .await
                .unwrap_err()
                .kind(),
            AccountErrorKind::Protocol(_)
        ));
        let notfound = get_soap("")
            .replace("ResponseClass=\"Success\"", "ResponseClass=\"Error\"")
            .replace("NoError", "ErrorItemNotFound");
        let (_, a) = account(vec![notfound]);
        seed(&a, "cal", "IPF.Appointment", true).await;
        let id = foreign::public_event("cal", "gone");
        let error = super::super::get(a, id.clone()).await.unwrap_err();
        assert_eq!(
            error.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Event)
        );
        assert_eq!(error.scope(), Some(&ErrorScope::Event { id }));
        let (script, a) = account(Vec::new());
        assert_eq!(
            super::super::get(a.clone(), foreign::public_event("stale", "a"))
                .await
                .unwrap_err()
                .kind(),
            &AccountErrorKind::ConcurrencyConflict
        );
        assert_eq!(
            super::super::events_in_range(a, query())
                .await
                .unwrap_err()
                .kind(),
            &AccountErrorKind::ConcurrencyConflict
        );
        assert!(script.requests().is_empty());
    }
    #[tokio::test]
    async fn nc2c_every_public_write_refuses_before_dispatch() {
        let (script, a) = account(Vec::new());
        let id = foreign::public_event("cal", "a");
        let create = EventCreate {
            calendar_id: foreign::public_calendar("cal"),
            title: None,
            description: None,
            location: None,
            start: query().start,
            end: query().end,
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        };
        let mut errors = vec![
            (
                AccountOperation::EventCreate,
                super::super::create(a.clone(), create).await.unwrap_err(),
            ),
            (
                AccountOperation::EventUpdate,
                super::super::update(a.clone(), id.clone(), EventPatch::default())
                    .await
                    .unwrap_err(),
            ),
            (
                AccountOperation::EventUpdate,
                super::super::update(
                    a.clone(),
                    EventId("personal::a".into()),
                    EventPatch {
                        calendar_id: Some(foreign::public_calendar("cal")),
                        start: Some(EventTime {
                            value: String::new(),
                            timezone: None,
                        }),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err(),
            ),
            (
                AccountOperation::EventDelete,
                super::super::delete(a.clone(), id.clone())
                    .await
                    .unwrap_err(),
            ),
        ];
        for status in [
            RsvpStatus::Accepted,
            RsvpStatus::Declined,
            RsvpStatus::Tentative,
            RsvpStatus::NeedsAction,
        ] {
            errors.push((
                AccountOperation::EventRsvp,
                super::super::rsvp(a.clone(), id.clone(), status)
                    .await
                    .unwrap_err(),
            ));
        }
        for (op, error) in errors {
            assert_eq!(error.kind(), &AccountErrorKind::Unsupported(op));
        }
        assert!(script.requests().is_empty());
    }
    #[tokio::test]
    async fn nc2c_all_day_uses_local_exclusive_dates_and_preview_fallback() {
        let event = item("day", "Single")
            .replace("2026-10-20T10:00:00Z", "2026-10-19T22:00:00Z")
            .replace("2026-10-20T12:00:00Z", "2026-10-20T22:00:00Z")
            .replace("Id=\"UTC\"", "Id=\"W. Europe Standard Time\"")
            .replace(">false<", ">true<")
            .replace("<t:Body>Full text</t:Body>", "");
        let (_, a) = account(vec![get_soap(&event)]);
        seed(&a, "cal", "IPF.Appointment", true).await;
        let result = super::super::get(a, foreign::public_event("cal", "day"))
            .await
            .unwrap();
        assert_eq!(result.start.value, "2026-10-20");
        assert_eq!(result.end.unwrap().value, "2026-10-21");
        assert_eq!(result.description.as_deref(), Some("Short"));
        assert!(result.is_all_day);
    }
    #[tokio::test]
    async fn nc2c_review_saved_ready_deletion_reports_native_failure_and_advances() {
        let a = item("a", "Single");
        let b = item("b", "Occurrence");
        let c = item("c", "Exception");
        let d = item("d", "Single");
        let (script, account) = account(vec![
            soap(&format!("{a}{b}{c}{d}"), true),
            get_soap(&a),
            batch_soap(&[item_error("ErrorItemNotFound"), get_soap(&c)]),
            get_soap(&d),
        ]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let mut q = query();
        q.limit = Some(1);
        let first = super::super::events_in_range(account.clone(), q.clone())
            .await
            .unwrap();
        q.page_cursor = Some(first.next_cursor.unwrap());
        q.limit = Some(2);
        let second = super::super::events_in_range(account.clone(), q.clone())
            .await
            .unwrap();
        assert_eq!(second.failed_ids, ["b"]);
        assert_eq!(
            second
                .items
                .iter()
                .map(|e| e.native_id.as_str())
                .collect::<Vec<_>>(),
            ["c"]
        );
        assert_ne!(second.next_cursor, q.page_cursor);
        q.page_cursor = Some(second.next_cursor.unwrap());
        let last = super::super::events_in_range(account, q).await.unwrap();
        assert_eq!(last.items[0].native_id, "d");
        assert!(last.next_cursor.is_none());
        assert_eq!(
            bodies(&script)
                .iter()
                .filter(|b| b.contains("<m:FindItem"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn nc2c_review_mixed_batch_errors_kinds_and_identities_are_per_id() {
        let ids = ["denied", "wrong-kind", "wrong-id", "ok"];
        let found = ids.iter().map(|id| item(id, "Single")).collect::<String>();
        let wrong_kind =
            item("wrong-kind", "Single").replace("IPM.Appointment.Custom", "IPM.Contact");
        let (script, account) = account(vec![
            soap(&found, true),
            batch_soap(&[
                item_error("ErrorAccessDenied"),
                get_soap(&wrong_kind),
                get_soap(&item("foreign", "Single")),
                get_soap(&item("ok", "Single")),
            ]),
        ]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let mut q = query();
        q.limit = Some(4);
        let page = super::super::events_in_range(account, q).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].native_id, "ok");
        assert_eq!(page.failed_ids, ["denied", "wrong-kind", "wrong-id"]);
        assert!(page.next_cursor.is_none());
        assert_eq!(bodies(&script)[1].matches("<t:ItemId ").count(), 4);
    }

    #[tokio::test]
    async fn nc2c_review_request_failures_leave_saved_cursor_replayable() {
        let canned = |xml| Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::from(xml),
        };
        let cases = vec![
            (canned(get_soap(&item("b","Single"))), AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ParseFailed)),
            (canned("<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><s:Fault><faultcode>s:Server</faultcode><faultstring>unavailable</faultstring></s:Fault></s:Body></s:Envelope>".to_string()), AccountErrorKind::Server(bifrost_types::ServerErrorKind::Unavailable)),
            (canned(item_error("ErrorServerBusy")), AccountErrorKind::Server(bifrost_types::ServerErrorKind::RateLimited)),
            (Canned::Response {status: reqwest::StatusCode::SERVICE_UNAVAILABLE, headers: reqwest::header::HeaderMap::new(),body: bytes::Bytes::new()}, AccountErrorKind::Server(bifrost_types::ServerErrorKind::Unavailable)),
            (Canned::Error(bifrost_net::Error::Timeout {transmission_state: bifrost_types::TransmissionState::Unsent}), AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Timeout)),
        ];
        for (failure, expected) in cases {
            let a = item("a", "Single");
            let b = item("b", "Single");
            let c = item("c", "Single");
            let (_, account) = account_canned(vec![
                canned(soap(&format!("{a}{b}{c}"), true)),
                canned(get_soap(&a)),
                failure,
                canned(batch_soap(&[get_soap(&b), get_soap(&c)])),
            ]);
            seed(&account, "cal", "IPF.Appointment", true).await;
            let mut q = query();
            q.limit = Some(1);
            q.page_cursor = super::super::events_in_range(account.clone(), q.clone())
                .await
                .unwrap()
                .next_cursor;
            q.limit = Some(2);
            assert_eq!(
                super::super::events_in_range(account.clone(), q.clone())
                    .await
                    .unwrap_err()
                    .kind(),
                &expected
            );
            let retry = super::super::events_in_range(account, q).await.unwrap();
            assert_eq!(retry.items.len(), 2);
            assert!(retry.failed_ids.is_empty());
            assert!(retry.next_cursor.is_none());
        }
    }

    #[tokio::test]
    async fn nc2c_review_local_boundaries_accept_windows_zones() {
        let (script, account) = account(vec![soap("", true)]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let mut q = query();
        q.start.value = "2026-10-20T00:00:00".into();
        q.end.value = "2026-10-22T00:00:00".into();
        q.start.timezone = Some("W. Europe Standard Time".into());
        q.end.timezone = q.start.timezone.clone();
        let result = super::super::events_in_range(account, q).await.unwrap();
        assert!(result.next_cursor.is_none());
        let body = &bodies(&script)[0];
        assert!(body.contains("StartDate=\"2026-10-19T22:00:00Z\""));
        assert!(body.contains("EndDate=\"2026-10-21T22:00:00Z\""));
    }

    #[tokio::test]
    async fn nc2c_review_hydration_chunks_all_ids_in_order() {
        let rows = (0..101)
            .map(|n| item(&format!("event-{n}"), "Single"))
            .collect::<Vec<_>>();
        let mut responses = vec![soap(&rows.concat(), true)];
        responses.extend(
            rows.chunks(20).map(|chunk| {
                batch_soap(&chunk.iter().map(|item| get_soap(item)).collect::<Vec<_>>())
            }),
        );
        let (script, account) = account(responses);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let mut q = query();
        q.limit = Some(250);
        let page = super::super::events_in_range(account, q).await.unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|e| e.native_id.clone())
                .collect::<Vec<_>>(),
            (0..101).map(|n| format!("event-{n}")).collect::<Vec<_>>()
        );
        assert!(page.failed_ids.is_empty());
        assert!(page.next_cursor.is_none());
        let requests = bodies(&script);
        assert_eq!(requests.len(), 7);
        for (n, request) in requests[1..].iter().enumerate() {
            assert_eq!(
                request.matches("<t:ItemId ").count(),
                if n == 5 { 1 } else { 20 }
            );
        }
    }

    #[tokio::test]
    async fn nc2c_review_hydration_starts_four_chunks_and_bounds_fanout() {
        let rows = (0..101)
            .map(|n| item(&format!("event-{n}"), "Single"))
            .collect::<String>();
        let mut responses = vec![Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::from(soap(&rows, true)),
        }];
        responses.extend((0..6).map(|_| Canned::Pending));
        let (script, account) = account_canned(responses);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let mut q = query();
        q.limit = Some(250);
        let future = super::super::events_in_range(account, q);
        futures::pin_mut!(future);
        for _ in 0..32 {
            assert!(
                std::future::Future::poll(
                    future.as_mut(),
                    &mut std::task::Context::from_waker(futures::task::noop_waker_ref())
                )
                .is_pending()
            );
            tokio::task::yield_now().await;
        }
        // One FindItem plus exactly four blocked GetItem requests. A serial
        // fan-out gives two requests; an unbounded one gives seven.
        assert_eq!(script.requests().len(), 5);
    }

    #[tokio::test]
    async fn nc2c_review_public_search_filters_text_cancelled_and_preserves_master() {
        let subject =
            item("subject", "Single").replace("Full text", &format!("{}needle", "x".repeat(600)));
        let location = item("location", "Single").replace("Room", "needle");
        let cancelled = item("cancelled", "Single").replace(
            "<t:Subject>Design &amp; build</t:Subject>",
            "<t:Subject>needle</t:Subject><t:IsCancelled>true</t:IsCancelled>",
        );
        let master = item("master","RecurringMaster").replace("<t:Subject>Design &amp; build</t:Subject>","<t:Subject>needle</t:Subject><t:Recurrence><t:WeeklyRecurrence><t:Interval>2</t:Interval><t:DaysOfWeek>Monday Wednesday</t:DaysOfWeek><t:FirstDayOfWeek>Monday</t:FirstDayOfWeek></t:WeeklyRecurrence><t:NumberedRecurrence><t:StartDate>2026-10-20</t:StartDate><t:NumberOfOccurrences>5</t:NumberOfOccurrences></t:NumberedRecurrence></t:Recurrence>");
        for include_cancelled in [false, true] {
            let negative = item("not-matching", "Single");
            let all = format!("{subject}{location}{cancelled}{master}{negative}");
            let (script, account) = account(vec![
                soap(&all, true),
                batch_soap(&[
                    get_soap(&subject),
                    get_soap(&location),
                    get_soap(&cancelled),
                    get_soap(&master),
                    get_soap(&negative),
                ]),
            ]);
            seed(&account, "cal", "IPF.Appointment", true).await;
            let request = EventSearchRequest {
                calendar_id: Some(foreign::public_calendar("cal")),
                query: "NEEDLE".into(),
                include_cancelled,
                limit: Some(10),
                page_cursor: None,
            };
            let result = super::super::search(account, request).await.unwrap();
            assert_eq!(result.items.len(), if include_cancelled { 4 } else { 3 });
            assert!(result.failed_ids.is_empty());
            assert!(result.next_cursor.is_none());
            assert_eq!(
                result.items.last().unwrap().recurrence.rrule.as_deref(),
                Some("FREQ=WEEKLY;BYDAY=MO,WE;INTERVAL=2;COUNT=5")
            );
            let body = &bodies(&script)[0];
            assert!(body.contains("IndexedPageItemView"));
            assert!(!body.contains("CalendarView"));
            for field in ["item:Subject", "calendar:Location", "item:Body"] {
                assert!(body.contains(&format!("FieldURI=\"{field}\"")));
            }
            assert!(body.contains("ContainmentMode=\"Substring\""));
            assert!(body.contains("ContainmentComparison=\"IgnoreCase\""));
        }
    }

    #[tokio::test]
    async fn nc2c_review_search_continuation_binds_query_and_advances_failed_rows() {
        let a = item("a", "Single");
        let b = item("b", "Single");
        let c = item("c", "Single");
        let d = item("d", "Single");
        let first_page = soap(&format!("{a}{b}{c}"), false).replace(
            "IncludesLastItemInRange=\"false\"",
            "IncludesLastItemInRange=\"false\" IndexedPagingOffset=\"17\"",
        );
        let (script, account) = account(vec![
            first_page,
            get_soap(&a),
            batch_soap(&[item_error("ErrorItemNotFound"), get_soap(&c)]),
            soap(&d, true),
            get_soap(&d),
        ]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let mut request = EventSearchRequest {
            calendar_id: Some(foreign::public_calendar("cal")),
            limit: Some(1),
            ..EventSearchRequest::new("")
        };
        request.page_cursor = super::super::search(account.clone(), request.clone())
            .await
            .unwrap()
            .next_cursor;
        let cursor = request.page_cursor.clone();
        for change in 0..3 {
            let mut other = request.clone();
            match change {
                0 => other.query = "different".into(),
                1 => other.include_cancelled = true,
                _ => {
                    seed(&account, "other", "IPF.Appointment", true).await;
                    other.calendar_id = Some(foreign::public_calendar("other"));
                }
            }
            assert!(matches!(
                super::super::search(account.clone(), other)
                    .await
                    .unwrap_err()
                    .kind(),
                AccountErrorKind::Request(_)
            ));
        }
        request.limit = Some(2);
        let second = super::super::search(account.clone(), request.clone())
            .await
            .unwrap();
        assert_eq!(second.failed_ids, ["b"]);
        assert_eq!(
            second
                .items
                .iter()
                .map(|e| e.native_id.as_str())
                .collect::<Vec<_>>(),
            ["c", "d"]
        );
        assert!(second.next_cursor.is_none());
        assert_ne!(cursor, second.next_cursor);
        assert!(bodies(&script)[3].contains("Offset=\"17\""));
        assert!(!bodies(&script)[0].contains("<m:Restriction>"));
    }

    #[tokio::test]
    async fn nc2c_review_search_escapes_query_and_refuses_stalled_offsets() {
        let (script, account) = account(vec![soap("", false)]);
        seed(&account, "cal", "IPF.Appointment", true).await;
        let request = EventSearchRequest {
            calendar_id: Some(foreign::public_calendar("cal")),
            ..EventSearchRequest::new("A<&\"'")
        };
        assert!(matches!(
            super::super::search(account, request)
                .await
                .unwrap_err()
                .kind(),
            AccountErrorKind::Protocol(_)
        ));
        assert!(bodies(&script)[0].contains("A&lt;&amp;&quot;&apos;"));
    }
}
