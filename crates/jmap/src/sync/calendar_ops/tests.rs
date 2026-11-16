use super::*;
use crate::account::Account as JmapProtoAccount;
use crate::calendar::CalendarRights;
use crate::calendar_event::{
    CalendarEvent as JmapCalendarEvent, CalendarEventCreate, CalendarEventId, CalendarEventPatch,
};
use crate::sync::page_cursor::PageKind;
use bifrost_types::{
    AccountError, AccountErrorKind, AccountOperation, AttendeeRole, CalendarEvent, CalendarId,
    CalendarProvenance, EventAttendee, EventAvailability, EventCreate, EventId, EventOrganizer,
    EventPatch, EventRange, EventRecurrence, EventSearchRequest, EventStatus, EventTime,
    EventVisibility, Page, ProtocolKind, ReminderRelativeTo, ReminderTrigger, RsvpStatus,
};
use serde_json::{Map, Value, json};

fn time(value: &str) -> EventTime {
    EventTime::from_observed(value.to_string(), None)
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
async fn single_event_reads_correlate_identity_and_reject_unanswered_ids() {
    let account = search_account();
    let event = get_raw_event(
        &account,
        CalendarEventId::new("e2"),
        AccountOperation::EventGet,
    )
    .await
    .expect("second returned event");
    assert_eq!(event.0.id(), Some(CalendarEventId::new("e2")));
    for operation in [
        AccountOperation::EventGet,
        AccountOperation::EventUpdate,
        AccountOperation::EventRsvp,
    ] {
        let error = get_raw_event(&account, CalendarEventId::new("absent"), operation)
            .await
            .expect_err("unrelated events do not answer this id");
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::PartialResponse,)
        );
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Event {
                id: EventId("absent".into()),
            })
        );
        assert_eq!(error.operation(), Some(operation));
    }
}

#[tokio::test]
async fn read_minted_event_versions_round_trip_through_writes() {
    let transport = ScriptedQueryTransport::new(vec![
        ScriptedPage {
            ids: vec!["one"],
            position: 0,
            total: 1,
            query_state: "query-only",
        };
        2
    ]);
    let range = EventRange::new(
        CalendarId("cal".into()),
        EventTime::from_observed("2026-06-01T00:00:00Z", None),
        EventTime::from_observed("2026-06-02T00:00:00Z", None),
    );
    let events = [
        get(Some(transport.account()), EventId("one".into()))
            .await
            .unwrap(),
        events_in_range(Some(transport.account()), range)
            .await
            .unwrap()
            .items
            .remove(0),
        search(Some(transport.account()), EventSearchRequest::new("one"))
            .await
            .unwrap()
            .items
            .remove(0),
    ];
    for event in events {
        let version = event
            .version
            .expect("CalendarEvent/get exposes its type state");
        assert_eq!(
            version
                .expected_jmap_state("primary", AccountOperation::EventUpdate)
                .unwrap(),
            "s1"
        );
        assert!(
            version
                .expected_jmap_state("primary", AccountOperation::ContactUpdate)
                .is_err()
        );
        update(
            Some(transport.account()),
            event.id.clone(),
            EventPatch {
                expected_version: Some(version.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        delete_if_version(
            Some(transport.account()),
            event.id.clone(),
            Some(version.clone()),
        )
        .await
        .unwrap();
        rsvp_if_version(
            Some(transport.account()),
            vec!["attendee@example.test".into()],
            event.id,
            RsvpStatus::Accepted,
            Some(version),
        )
        .await
        .unwrap();
    }
    for (_, args) in transport
        .calls()
        .iter()
        .filter(|(method, _)| method == "CalendarEvent/set")
    {
        assert_eq!(args["ifInState"], "s1", "use /get state, never queryState");
    }
}

#[tokio::test]
async fn caller_event_state_is_enforced_without_replacing_it() {
    for state in [Some("old"), Some("s1"), None] {
        let transport = ScriptedQueryTransport::new(Vec::new());
        let expected = state.map(|state| {
            bifrost_types::ServerVersion::jmap_state(
                "primary",
                bifrost_types::ObjectType::CalendarEvent,
                state,
            )
        });
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

/// The scripted server answers `CalendarEvent/set` for "one" only, so a
/// write to "two" comes back omitted from every map. The omission does
/// not prove the write failed: update, RSVP and delete must report the
/// partial response (which reconciles or retries), never the terminal
/// `MissingField` a bare `IdNotFound` classified as.
#[tokio::test]
async fn an_omitted_event_set_answer_is_a_partial_response() {
    let partial = bifrost_types::AccountErrorKind::Protocol(
        bifrost_types::ProtocolErrorKind::PartialResponse,
    );
    for expected in [
        None,
        Some(bifrost_types::ServerVersion::jmap_state(
            "primary",
            bifrost_types::ObjectType::CalendarEvent,
            "s1",
        )),
    ] {
        let transport = ScriptedQueryTransport::new(Vec::new());
        let update = update(
            Some(transport.account()),
            EventId("two".into()),
            EventPatch {
                expected_version: expected.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(update.kind(), &partial, "{update:?}");
        let rsvp = rsvp_if_version(
            Some(transport.account()),
            Vec::new(),
            EventId("two".into()),
            RsvpStatus::Accepted,
            expected.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(rsvp.kind(), &partial, "{rsvp:?}");
        let delete = delete_if_version(
            Some(transport.account()),
            EventId("two".into()),
            expected.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(delete.kind(), &partial, "{delete:?}");
    }
    // The scripted set answers no create either: an omitted create may
    // have landed, so it reconciles rather than terminating.
    let transport = ScriptedQueryTransport::new(Vec::new());
    let create = create(
        Some(transport.account()),
        create_with("2026-06-02T12:00:00Z", "2026-06-02T13:00:00Z"),
    )
    .await
    .unwrap_err();
    assert_eq!(create.kind(), &partial, "{create:?}");
    assert!(create.recovery().requires_reconciliation(), "{create:?}");
}

/// A create answered in the success map without a usable id is refused,
/// never handed back as `EventId("")`.
#[tokio::test]
async fn a_created_event_without_a_usable_id_is_refused() {
    let call = |created: Value| async move {
        let transport = ScriptedQueryTransport::new(Vec::new()).with_created(created);
        create(
            Some(transport.account()),
            create_with("2026-06-02T12:00:00Z", "2026-06-02T13:00:00Z"),
        )
        .await
    };
    for entry in [json!({}), json!({"id": ""})] {
        let error = call(json!({"c0": entry})).await.unwrap_err();
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::MissingField
            ),
            "{error:?}"
        );
    }
    let id = call(json!({"c0": {"id": "event-1"}}))
        .await
        .expect("a usable created id");
    assert_eq!(id, EventId("event-1".into()));
}

#[tokio::test]
async fn wrong_event_version_kind_is_refused_before_io() {
    let transport = ScriptedQueryTransport::new(Vec::new());
    for expected_version in [
        bifrost_types::ServerVersion::etag(
            bifrost_types::Protocol::Jmap,
            bifrost_types::ObjectType::CalendarEvent,
            "s1",
        ),
        bifrost_types::ServerVersion::jmap_state(
            "primary",
            bifrost_types::ObjectType::CalendarEvent,
            String::new(),
        ),
        bifrost_types::ServerVersion::jmap_state(
            "shared",
            bifrost_types::ObjectType::CalendarEvent,
            "s1",
        ),
        bifrost_types::ServerVersion::jmap_state(
            "primary",
            bifrost_types::ObjectType::Contact,
            "s1",
        ),
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

fn event(start: &str, end: &str, expected_all_day: bool) -> CalendarEvent {
    assert_eq!(time(start).is_all_day(), expected_all_day);
    CalendarEvent {
        id: EventId("e1".to_string()),
        calendar_id: CalendarId("c1".to_string()),
        native_id: "e1".to_string(),
        uid: None,
        version: None,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Jmap,
            native: "e1".to_string(),
            calendar_native: Some("c1".to_string()),
        },
        title: None,
        description: None,
        location: None,
        start: time(start),
        end: Some(time(end)).filter(|end| !end.value().is_empty()),
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
        let error =
            event_duration(&time(start), &time(end), all_day, op).expect_err("end before start");
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
fn week_durations_preserve_timed_and_all_day_event_ends() {
    for (start, all_day, expected) in [
        ("2026-06-02T12:00:00", false, "2026-06-09T12:00:00+00:00"),
        ("2026-06-02T00:00:00", true, "2026-06-09"),
    ] {
        let event = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "weekly",
                "start": start,
                "duration": "P1W",
                "showWithoutTime": all_day
            }))
            .expect("event properties"),
        };
        let projected =
            event_from_jmap(event, AccountOperation::EventGet).expect("supported event");
        assert_eq!(projected.end.expect("event end").value(), expected);
    }
    assert_eq!(parse_duration_seconds("P2W"), Some(1_209_600));
    assert_eq!(parse_duration_seconds("P18446744073709551615W"), None);
    assert_eq!(parse_duration_seconds("PT1W"), None);
}

#[test]
fn end_from_duration_handles_timed_and_all_day_events() {
    assert_eq!(
        end_from_start_duration("2026-06-02T12:00:00Z", "PT90M", false).unwrap(),
        "2026-06-02T13:30:00+00:00"
    );
    // Exclusive end: start 2026-06-02 + P3D = 2026-06-05.
    assert_eq!(
        end_from_start_duration("2026-06-02", "P3D", true).unwrap(),
        "2026-06-05"
    );
}

#[test]
fn end_from_duration_rejects_invalid_start_but_preserves_absence() {
    for all_day in [false, true] {
        for start in ["not a time", " ", "2026-99-99", "2026-06-02T99:99:99"] {
            assert_eq!(end_from_start_duration(start, "PT90M", all_day), None);
        }
        assert_eq!(
            end_from_start_duration("", "PT90M", all_day),
            Some(String::new())
        );
    }
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
            &EventTime::from_observed(
                "2026-06-02T12:00:00+02:00".to_string(),
                Some("Europe/Oslo".to_string())
            ),
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
            &EventTime::from_observed(
                "2026-06-02T12:00:00+02:00".to_string(),
                Some("Europe/Oslo".to_string())
            ),
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

    assert!(
        EventRange::new(
            event.calendar_id.clone(),
            time("2026-06-02T00:00:00Z"),
            time("2026-06-02T23:59:59Z"),
        )
        .admits(&event)
    );
}

#[test]
fn range_filter_includes_all_day_on_window_start() {
    // Single all-day event on 2026-06-02 under the exclusive contract:
    // start 2026-06-02, end 2026-06-03.
    let event = event("2026-06-02", "2026-06-03", true);

    assert!(
        EventRange::new(
            event.calendar_id.clone(),
            time("2026-06-02T00:00:00Z"),
            time("2026-06-03T00:00:00Z"),
        )
        .admits(&event)
    );
}

#[test]
fn range_filter_excludes_events_touching_only_an_exclusive_boundary() {
    let before = event("2026-06-01T23:00:00Z", "2026-06-02T00:00:00Z", false);
    let after = event("2026-06-03T00:00:00Z", "2026-06-03T01:00:00Z", false);
    let start = time("2026-06-02T00:00:00Z");
    let end = time("2026-06-03T00:00:00Z");
    assert!(
        !EventRange::new(before.calendar_id.clone(), start.clone(), end.clone(),).admits(&before)
    );
    assert!(
        !EventRange::new(after.calendar_id.clone(), start.clone(), end.clone(),).admits(&after)
    );
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
fn page_cursor(anchor: &str, query_state: &str) -> bifrost_types::PageCursor {
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
    position: u64,
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
    /// The `created` map a `CalendarEvent/set` answers with; `None`
    /// leaves every create unanswered.
    created: Option<Value>,
}

impl ScriptedQueryTransport {
    fn new(pages: Vec<ScriptedPage>) -> Self {
        Self {
            pages: std::sync::Arc::new(std::sync::Mutex::new(pages.into())),
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            created: None,
        }
    }

    fn with_created(mut self, created: Value) -> Self {
        self.created = Some(created);
        self
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
        let arguments = match name.as_str() {
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
                    return Ok(bytes::Bytes::from(
                        json!({
                            "sessionState": "session-1",
                            "methodResponses": [["error", {"type": "stateMismatch"}, call_id]]
                        })
                        .to_string(),
                    ));
                }
                let mut answer = json!({
                    "accountId": "primary", "oldState": "s1", "newState": "s2",
                    "updated": {"one": null}, "destroyed": ["one"]
                });
                if let Some(created) = &self.created {
                    answer["created"] = created.clone();
                }
                answer
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
    page_cursor: Option<bifrost_types::PageCursor>,
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
    let mut cursor: Option<bifrost_types::PageCursor> = None;
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
    assert!(event.start.value().is_empty());
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
    let zoned_start = EventTime::from_observed(
        "2025-01-01T09:00:00+01:00".to_string(),
        Some("Europe/Oslo".to_string()),
    );
    let utc = jmap_recurrence_rule_from_rrule(
        "FREQ=DAILY;UNTIL=20260601T070000Z",
        Some(&zoned_start),
        false,
    )
    .expect("UTC until");
    assert_eq!(utc["until"].as_str(), Some("2026-06-01T09:00:00"));

    let floating_start = EventTime::from_observed("2025-01-01T09:00:00+00:00".to_string(), None);
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
        "FREQ=MONTHLY;INTERVAL=2;COUNT=5;BYDAY=MO,WE;BYMONTH=6;BYMONTHDAY=2",
        None,
        false,
    )
    .expect("rrule should convert");

    assert_eq!(rule["frequency"].as_str(), Some("monthly"));
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
        end_from_start_duration("2026-06-02T00:00:00", "P1D", true).unwrap(),
        "2026-06-03"
    );
    assert_eq!(
        end_from_start_duration("2026-06-02", "P1D", true).unwrap(),
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
        context.start.as_ref().and_then(|start| start.timezone()),
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
fn typed_time_patches_own_the_wire_mode_without_a_sibling_flag() {
    for (start, end, all_day, wire_start, duration) in [
        (
            "2026-06-02",
            "2026-06-03",
            true,
            "2026-06-02T00:00:00",
            "P1D",
        ),
        (
            "2026-06-02T12:00:00",
            "2026-06-02T13:00:00",
            false,
            "2026-06-02T12:00:00",
            "PT3600S",
        ),
    ] {
        for stored_mode in [true, false] {
            let current = JmapCalendarEvent {
                properties: serde_json::from_value(json!({
                    "id": "e1", "start": "2026-06-01T00:00:00",
                    "showWithoutTime": stored_mode
                }))
                .expect("event"),
            };
            let patch = EventPatch {
                start: Some(time(start)),
                end: Some(time(end)),
                ..EventPatch::default()
            };
            let context = event_context_from_patch(&patch, Some(&current));
            let built =
                jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)
                    .expect("patch");
            assert_eq!(built.properties["showWithoutTime"], json!(all_day));
            assert_eq!(built.properties["start"].as_str(), Some(wire_start));
            assert_eq!(built.properties["duration"].as_str(), Some(duration));
        }
    }
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
            offset: "-PT15M".parse().unwrap(),
            relative_to: ReminderRelativeTo::Start,
        }
        && reminder.action.as_deref() == Some("DISPLAY")));
    assert!(reminders.iter().any(|reminder| reminder.trigger
        == ReminderTrigger::Relative {
            offset: "PT5M".parse().unwrap(),
            relative_to: ReminderRelativeTo::End,
        }
        && reminder.action.as_deref() == Some("EMAIL")));
    assert!(reminders.iter().any(|reminder| reminder.trigger
        == ReminderTrigger::Absolute("2026-06-02T11:00:00Z".parse().unwrap())
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

    let patch = jmap_rsvp_patch_from_participants(Some(&participants), &[], RsvpStatus::Accepted)
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

    let error = jmap_rsvp_patch_from_participants(Some(&participants), &[], RsvpStatus::Accepted)
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
#[test]
fn invalid_recurrence_is_malformed_before_provider_projection() {
    for text in [
        "FREQ=DAILY;COUNT=0",
        "FREQ=DAILY;INTERVAL=0",
        "FREQ=DAILY;FREQ=WEEKLY",
        "FREQ=DAILY;BYDAY=1MO",
    ] {
        let recurrence = EventRecurrence {
            rrule: Some(text.into()),
            ..Default::default()
        };
        let error =
            validate_shared_recurrence(&recurrence, None, false, AccountOperation::EventCreate)
                .expect_err("shared grammar must refuse malformed rules");
        assert_malformed_field(&error, "recurrence.rrule");
    }
}

#[test]
fn fractional_duration_survives_the_conversion_boundary() {
    let duration = event_duration(
        &time("2026-06-02T12:00:00Z"),
        &time("2026-06-02T12:00:00.125Z"),
        false,
        AccountOperation::EventCreate,
    )
    .unwrap();
    assert_eq!(duration, "PT0.125000000S");
    assert_eq!(
        end_from_start_duration("2026-06-02T12:00:00", duration.as_str(), false).unwrap(),
        "2026-06-02T12:00:00.125"
    );
}

#[test]
fn malformed_server_duration_is_not_a_zero_length_event() {
    let raw: JmapCalendarEvent = serde_json::from_value(json!({
        "id": "e1", "start": "2026-06-02T12:00:00", "duration": "PT1H2H"
    }))
    .unwrap();
    let error = event_from_jmap(raw, AccountOperation::EventGet).unwrap_err();
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ParseFailed)
    ));
}
