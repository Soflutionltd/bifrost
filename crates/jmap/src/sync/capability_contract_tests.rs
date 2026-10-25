//! The capability mirror, checked against the account it describes.
//!
//! `PimMethodSupport` is a hand-maintained mirror of the `Account` trait: sixty
//! `bool`s with no mechanical link to the methods they describe. Nothing else
//! in the workspace checks that a `false` flag implies the method actually
//! refuses. This drives every gated method on the crate's own `Account` impl
//! and asserts the flag and the observed result agree.
//!
//! **Coverage here is one-directional, and in this crate that is forced rather
//! than chosen.** `JmapAccount` hardwires `ReqwestTransport` in its
//! `MailAccount` type alias, so there is no seam through which a whole
//! `Account` call can be driven to a scripted server: the true direction is
//! simply unreachable from here, and the per-method tests that do exist reach
//! the generic `pim`/`contacts`/`calendar_ops` helpers instead. What IS
//! reachable, and is what this file pins, is the false direction: a `false`
//! flag must make the method refuse locally, without a request. The account
//! below is built around a real `ReqwestTransport` pointed at an unroutable
//! host - if any driven method actually issued a request, it would fail with a
//! transport error rather than `Unsupported`, and the assertion would catch it.
//!
//! The `PimSupport` fixture deliberately declares NOTHING supported, which is
//! the widest false set the capability builder can produce and therefore the
//! widest set this contract can check.

#![cfg(test)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bifrost_types::{
    Account, AccountError, AccountErrorKind, AccountOperation, CloudUploadMeta, ContactCreate,
    ContactId, ContactPatch, ContainerId, ContainerKind, DirectoryGroupId, DraftHandle, DraftPatch,
    EventCreate, EventId, EventPatch, EventRange, EventRecurrence, EventSearchRequest, EventStatus,
    EventTime, FilterScriptCreate, HydrationProjection, Importance, MutationTarget, ObjectId,
    ScriptLanguage, SearchRequest, SendRequest, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, ShareScope, SyncEvent, ThreadId, VacationConfig,
};
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::account::JmapAccount;
use super::capabilities::{self, PimSupport};
use super::push::{PushRouting, ReconnectPolicy, WsState};
use crate::client::{Authorization, Client};
use crate::core::session::Session;
use crate::transport_reqwest::ReqwestTransport;

/// A session advertising the core capability (the builder refuses without it)
/// and nothing else - no submission, no sieve, no contacts, no calendars, no
/// websocket push.
const SESSION: &str = r#"{
    "capabilities": {
        "urn:ietf:params:jmap:core": {
            "maxSizeUpload": 1000,
            "maxConcurrentUpload": 2,
            "maxSizeRequest": 100000,
            "maxConcurrentRequests": 4,
            "maxCallsInRequest": 8,
            "maxObjectsInGet": 256,
            "maxObjectsInSet": 100,
            "collationAlgorithms": []
        },
        "urn:ietf:params:jmap:mail": {}
    },
    "accounts": {},
    "primaryAccounts": {},
    "username": "user@example.test",
    "apiUrl": "https://jmap.invalid/api",
    "downloadUrl": "https://jmap.invalid/dl/{accountId}/{blobId}/{name}/{type}",
    "uploadUrl": "https://jmap.invalid/upload/{accountId}",
    "eventSourceUrl": "https://jmap.invalid/es",
    "state": "session-1"
}"#;

fn account() -> JmapAccount {
    account_with(false, None)
}

/// The same account with primary submission, and optionally one seeded foreign
/// account that advertises submission.
///
/// Both knobs exist for the `send_as` request-field contract and nothing else.
/// `submission` decides `pim_methods.send_message`, which has to be true or the
/// entry-point gate answers before any `send_as` logic runs and the assertion
/// would agree with a gate it is not testing. The foreign account decides
/// `pim_methods.send_as`, which is the flag that selects between the two
/// rejection classes, so a fixture without one can only ever exercise half the
/// contract.
///
/// No transport is scripted for either: an id absent from `foreign_mail` is
/// refused by `route_send_as` before a `MailAccount` is selected, so reaching
/// the wire at all would be the failure.
fn account_with_submission(foreign_submission_id: Option<&str>) -> JmapAccount {
    account_with(true, foreign_submission_id)
}

fn account_with(submission_available: bool, foreign_submission_id: Option<&str>) -> JmapAccount {
    let session: Session = serde_json::from_str(SESSION).expect("session fixture parses");
    let transport = ReqwestTransport::new(
        reqwest::header::HeaderMap::new(),
        Authorization::Basic(String::new()),
        bifrost_types::AccountId("jmap-capability-contract".to_string()),
        std::time::Duration::from_secs(5),
        false,
        Arc::new(HashSet::new()),
    )
    .expect("transport builds");
    let client = Client::with_transport(transport, session.clone(), "https://jmap.invalid/session")
        .expect("client builds");
    let mail = crate::account::Account::new(client.clone(), "acct-1");

    let support = PimSupport {
        submission: submission_available,
        max_delayed_send: 0,
        foreign_submission: foreign_submission_id.is_some(),
        vacation: false,
        quota: false,
        sieve: false,
        contacts: false,
        calendar: false,
    };
    let (caps, limits) = capabilities::build(&session, support).expect("capabilities build");

    let shutdown = CancellationToken::new();
    // `push_available: false` leaves the reader task unspawned, so nothing
    // here opens a socket.
    let ws = WsState::new(
        client.clone(),
        false,
        shutdown.clone(),
        ReconnectPolicy::default(),
        Arc::new(PushRouting::new(
            "acct-1".to_string(),
            std::iter::empty::<&bifrost_types::CursorScope>(),
        )),
    );

    let foreign_mail: HashMap<String, crate::account::Account<ReqwestTransport>> =
        foreign_submission_id
            .map(|id| {
                (
                    id.to_string(),
                    crate::account::Account::new(client.clone(), id),
                )
            })
            .into_iter()
            .collect();
    let foreign_submission: HashSet<String> = foreign_submission_id
        .map(ToString::to_string)
        .into_iter()
        .collect();
    let submission =
        submission_available.then(|| crate::account::Account::new(client.clone(), "acct-1"));

    JmapAccount::new(
        client,
        mail,
        foreign_mail,
        foreign_submission,
        submission,
        0,
        None,
        None,
        None,
        None,
        None,
        Vec::new(),
        caps,
        limits,
        HashMap::new(),
        ws,
        shutdown,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    )
}

/// A send naming a foreign account this fixture cannot hold.
///
/// Unknownness comes from the value being absent from the fixture's routing
/// table, not from the string being degenerate: `MailboxId` has no syntax
/// validator, so an empty or malformed id would invite a future validation
/// layer to reject it for an unrelated reason and quietly stop testing the
/// routing lookup. `scheduled` stays `None` especially, since a scheduled
/// foreign send is refused before the routing question is reached.
fn send_as_request() -> SendRequest {
    let mut request = SendRequest::default();
    request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::MailboxId(
        "contract-unknown-mailbox".to_string(),
    )));
    request
}

/// Assert the `send_as` REQUEST-FIELD contract, whichever branch of the
/// capability flag this account is on.
///
/// Every other macro in this file drives a gated METHOD and can only assert the
/// false direction, because a true flag means the method reaches the network.
/// `send_as` gates a request FIELD, so this file used to miss it entirely - it
/// always passes `send_as: None`. It is also the contract with two answers
/// rather than one, stated on `SendRequest::send_as`: `send_as == false` means
/// the feature is absent, so `Unsupported(Send)`; `send_as == true` means it is
/// present, so an unheld mailbox id is a bad ARGUMENT and gets
/// `Request(Malformed)` with a `send_as.mailbox` field pointer for a UI.
///
/// The true direction needs no network seam, because the behaviour under test
/// IS a local refusal: `route_send_as` runs before a foreign `MailAccount` is
/// selected. That matters more here than elsewhere, because this crate's
/// `MailAccount = Account<ReqwestTransport>` alias means no scripted seam
/// exists at the `Account` level at all.
///
/// The expected kind is DERIVED from the crate's own flag rather than written
/// down, which is the whole point: a backend whose flag and behaviour drift
/// apart fails here instead of agreeing with a local copy of the rule forever.
/// It caught a live violation the day it was written - `route_send_as` did not
/// consult the capability at all, so an account advertising `send_as == false`
/// still answered `Request(Malformed)`, telling a consumer to correct an
/// argument when no argument could have worked.
macro_rules! refuses_field {
    ($caps:expr, $call:expr) => {
        assert!(
            $caps.pim_methods.send_message,
            "refuses_field! asserts nothing where the send_message gate answers \
             first; omit it in that crate rather than letting it pass for the \
             wrong reason"
        );
        let error = $call
            .await
            .expect_err("a send_as naming a mailbox this account cannot hold must be refused");
        if $caps.pim_methods.send_as {
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
                "pim_methods.send_as is true, so the feature is present and an \
                 unheld mailbox is a bad argument, not a missing capability",
            );
            assert!(
                matches!(
                    error.chain().outermost(),
                    bifrost_types::Cause::Request(
                        bifrost_types::RequestCause::InvalidArgument { field, .. },
                    ) if field.as_deref() == Some("send_as.mailbox"),
                ),
                "the rejection must carry the send_as.mailbox field pointer a \
                 consumer highlights; got {:?}",
                error.chain().outermost(),
            );
        } else {
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Unsupported(AccountOperation::Send),
                "pim_methods.send_as is false, so the feature is absent and the \
                 only honest answer is Unsupported(Send)",
            );
        }
    };
}

fn target() -> MutationTarget {
    MutationTarget::Message(ObjectId("m1".to_string()))
}

fn event_time() -> EventTime {
    EventTime {
        value: "2026-06-02T09:00:00Z".to_string(),
        timezone: None,
    }
}

fn event_range() -> EventRange {
    EventRange {
        calendar_id: "cal-1".into(),
        start: event_time(),
        end: event_time(),
        page_cursor: None,
        limit: None,
    }
}

fn event_create() -> EventCreate {
    EventCreate {
        calendar_id: "cal-1".into(),
        title: Some("t".to_string()),
        description: None,
        location: None,
        start: event_time(),
        end: event_time(),
        is_all_day: false,
        status: EventStatus::Confirmed,
        availability: bifrost_types::EventAvailability::Busy,
        visibility: bifrost_types::EventVisibility::Default,
        organizer: None,
        attendees: Vec::new(),
        recurrence: EventRecurrence::default(),
    }
}

fn filter_create() -> ServerFilterCreate {
    ServerFilterCreate::Script(FilterScriptCreate {
        name: Some("s".to_string()),
        language: ScriptLanguage::Sieve,
        body: "keep;".to_string(),
        is_active: true,
    })
}

fn vacation() -> VacationConfig {
    VacationConfig {
        is_enabled: false,
        subject: None,
        body_text: None,
        body_html: None,
        starts_at: None,
        ends_at: None,
    }
}

fn assert_unsupported(flag: &str, expected: AccountOperation, error: &AccountError) {
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Unsupported(expected),
        "pim_methods.{flag} is false, so the method must refuse with Unsupported({expected:?})",
    );
}

/// Await a gated future and assert the documented refusal.
macro_rules! refuses {
    ($caps:expr, $flag:ident, $op:expr, $call:expr) => {
        if !$caps.pim_methods.$flag {
            let error = $call.await.expect_err(concat!(
                "pim_methods.",
                stringify!($flag),
                " is false, so the method must return Err(Unsupported)"
            ));
            assert_unsupported(stringify!($flag), $op, &error);
        }
    };
}

/// Same, for a gated convenience whose default impl dispatches into a
/// primitive: the refusal is still `Unsupported`, but it names the primitive
/// the convenience reached for, not the convenience itself.
macro_rules! refuses_somehow {
    ($caps:expr, $flag:ident, $call:expr) => {
        if !$caps.pim_methods.$flag {
            let error = $call.await.expect_err(concat!(
                "pim_methods.",
                stringify!($flag),
                " is false, so the convenience must return Err(Unsupported)"
            ));
            assert!(
                matches!(error.kind(), AccountErrorKind::Unsupported(_)),
                concat!(
                    "pim_methods.",
                    stringify!($flag),
                    " is false, so the convenience must refuse as Unsupported, got {:?}"
                ),
                error.kind(),
            );
        }
    };
}

/// Same, for a gated stream: the first event must be the refusal.
macro_rules! stream_refuses {
    ($caps:expr, $flag:ident, $op:expr, $call:expr) => {
        if !$caps.pim_methods.$flag {
            let mut stream = $call;
            let first = stream.next().await.expect(concat!(
                "pim_methods.",
                stringify!($flag),
                " is false, so the stream must yield a refusal"
            ));
            match first {
                SyncEvent::Terminated(error) => {
                    assert_unsupported(stringify!($flag), $op, &error);
                }
                other => panic!(
                    concat!(
                        "pim_methods.",
                        stringify!($flag),
                        " is false, so the stream must terminate, got {:?}"
                    ),
                    other
                ),
            }
        }
    };
}

#[tokio::test]
async fn every_false_pim_flag_refuses_without_touching_the_wire() {
    let account = account();
    let caps = account.capabilities().clone();

    refuses!(
        caps,
        category_definitions,
        AccountOperation::CategoryDefinitionsList,
        account.category_definitions_list()
    );
    refuses!(
        caps,
        message_reactions,
        AccountOperation::MessageReactionsRead,
        account.message_reactions(&[ObjectId("m1".to_string())])
    );

    // Mail mutation primitives.
    refuses!(
        caps,
        add_to_container,
        AccountOperation::AddToContainer,
        account.add_to_container(target(), ContainerId("c".to_string()))
    );
    refuses!(
        caps,
        remove_from_container,
        AccountOperation::RemoveFromContainer,
        account.remove_from_container(target(), ContainerId("c".to_string()))
    );
    refuses!(
        caps,
        set_keyword,
        AccountOperation::SetKeyword,
        account.set_keyword(target(), "$flagged".to_string(), true)
    );
    refuses!(
        caps,
        set_label_membership,
        AccountOperation::SetLabelMembership,
        account.set_label_membership(target(), ContainerId("STARRED".to_string()), true)
    );
    refuses!(
        caps,
        set_category,
        AccountOperation::SetCategory,
        account.set_category(target(), "cat".to_string(), true)
    );
    refuses!(
        caps,
        set_extended_property,
        AccountOperation::SetExtendedProperty,
        account.set_extended_property(target(), "prop".to_string(), None)
    );
    refuses!(
        caps,
        set_importance,
        AccountOperation::SetImportance,
        account.set_importance(target(), Importance::High)
    );
    refuses!(
        caps,
        set_is_read,
        AccountOperation::SetIsRead,
        account.set_is_read(target(), true)
    );

    // Composition primitives.
    refuses!(
        caps,
        send_message,
        AccountOperation::Send,
        account.send_message(SendRequest::default())
    );
    // Both branches of the send_as field contract. The bare fixture cannot
    // reach either - its `send_message` is false, so the entry gate answers
    // first - hence two purpose-built accounts rather than a conditional on
    // whatever shape this one happens to be in.
    {
        let no_foreign = account_with_submission(None);
        let no_foreign_caps = no_foreign.capabilities();
        assert!(
            !no_foreign_caps.pim_methods.send_as,
            "no seeded foreign submission account means the feature is absent"
        );
        refuses_field!(no_foreign_caps, no_foreign.send_message(send_as_request()));

        let foreign = account_with_submission(Some("contract-known-mailbox"));
        let foreign_caps = foreign.capabilities();
        assert!(
            foreign_caps.pim_methods.send_as,
            "one seeded submission-capable foreign account makes the feature \
             present; without that this asserts the same branch twice"
        );
        refuses_field!(foreign_caps, foreign.send_message(send_as_request()));
    }
    refuses!(
        caps,
        attachment_upload,
        AccountOperation::AttachmentUpload,
        account.attachment_upload(
            Box::pin(futures::stream::empty::<Result<bytes::Bytes, AccountError>>()),
            "text/plain".to_string(),
        )
    );
    refuses!(
        caps,
        host_attachment,
        AccountOperation::HostAttachment,
        account.host_attachment(
            bytes::Bytes::from_static(b"x"),
            CloudUploadMeta::new("f.bin", "application/octet-stream", 1, ShareScope::Anyone),
        )
    );
    refuses!(
        caps,
        draft_create,
        AccountOperation::DraftCreate,
        account.draft_create(DraftPatch::default())
    );
    refuses!(
        caps,
        draft_update,
        AccountOperation::DraftUpdate,
        account.draft_update(DraftHandle("d1".to_string()), DraftPatch::default())
    );
    refuses!(
        caps,
        draft_discard,
        AccountOperation::DraftDiscard,
        account.draft_discard(DraftHandle("d1".to_string()))
    );
    refuses!(
        caps,
        draft_send,
        AccountOperation::DraftSend,
        account.draft_send(DraftHandle("d1".to_string()))
    );
    // `scheduled_send` gates two primitives outright (it also gates
    // `SendRequest::scheduled`, which is a request-field rejection this file
    // does not model).
    refuses!(
        caps,
        scheduled_send,
        AccountOperation::CancelScheduledSend,
        account.cancel_scheduled_send(ObjectId("s1".to_string()))
    );
    refuses!(
        caps,
        scheduled_send,
        AccountOperation::RescheduleSend,
        account.reschedule_send(
            ObjectId("s1".to_string()),
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(4_000_000_000),
        )
    );

    // Search.
    refuses!(
        caps,
        search,
        AccountOperation::Search,
        account.search(SearchRequest::provider("q"))
    );
    refuses!(
        caps,
        search_messages,
        AccountOperation::SearchMessages,
        account.search_messages(SearchRequest::provider("q"))
    );

    // Container CRUD.
    refuses!(
        caps,
        containers_list,
        AccountOperation::ContainersList,
        account.containers_list()
    );
    refuses!(
        caps,
        container_create,
        AccountOperation::ContainerCreate,
        account.container_create(ContainerKind::Folder, "n".to_string(), None, None)
    );
    refuses!(
        caps,
        container_rename,
        AccountOperation::ContainerRename,
        account.container_rename(ContainerId("c".to_string()), "n".to_string(), None)
    );
    refuses!(
        caps,
        container_move,
        AccountOperation::ContainerMove,
        account.container_move(ContainerId("c".to_string()), None)
    );
    refuses!(
        caps,
        container_delete,
        AccountOperation::ContainerDelete,
        account.container_delete(ContainerId("c".to_string()))
    );

    // Settings.
    refuses!(
        caps,
        identities_list,
        AccountOperation::IdentitiesList,
        account.identities_list()
    );
    refuses!(
        caps,
        identity_update,
        AccountOperation::IdentityUpdate,
        account.identity_update(
            bifrost_types::IdentityId("i1".to_string()),
            bifrost_types::IdentityPatch::default()
        )
    );
    refuses!(
        caps,
        vacation_get,
        AccountOperation::VacationGet,
        account.vacation_get()
    );
    refuses!(
        caps,
        vacation_set,
        AccountOperation::VacationSet,
        account.vacation_set(vacation())
    );
    refuses!(
        caps,
        quota_get,
        AccountOperation::QuotaGet,
        account.quota_get()
    );

    // Hydration.
    refuses!(
        caps,
        thread_hydrate,
        AccountOperation::HydrateThread,
        account.thread_hydrate(ThreadId("t1".to_string()))
    );
    refuses!(
        caps,
        message_hydrate,
        AccountOperation::HydrateMessage,
        account.message_hydrate(ObjectId("m1".to_string()), HydrationProjection::Headers)
    );
    stream_refuses!(
        caps,
        open_raw_rfc822,
        AccountOperation::OpenRawRfc822,
        account.open_raw_rfc822(ObjectId("m1".to_string()))
    );

    // Server-side filters.
    refuses!(
        caps,
        filters_list,
        AccountOperation::FiltersList,
        account.filters_list()
    );
    refuses!(
        caps,
        filter_create,
        AccountOperation::FilterCreate,
        account.filter_create(filter_create())
    );
    refuses!(
        caps,
        filter_update,
        AccountOperation::FilterUpdate,
        account.filter_update(
            ServerFilterId("f1".to_string()),
            ServerFilterPatch::Script(Default::default())
        )
    );
    refuses!(
        caps,
        filter_delete,
        AccountOperation::FilterDelete,
        account.filter_delete(ServerFilterId("f1".to_string()))
    );
    refuses!(
        caps,
        filter_validate,
        AccountOperation::FilterValidate,
        account.filter_validate(filter_create())
    );

    // Contacts.
    refuses!(
        caps,
        address_books_list,
        AccountOperation::AddressBooksList,
        account.address_books_list()
    );
    refuses!(
        caps,
        contacts_list,
        AccountOperation::ContactsList,
        account.contacts_list(None, None)
    );
    refuses!(
        caps,
        contact_get,
        AccountOperation::ContactGet,
        account.contact_get(ContactId("c1".to_string()))
    );
    refuses!(
        caps,
        contact_create,
        AccountOperation::ContactCreate,
        account.contact_create(ContactCreate::default())
    );
    refuses!(
        caps,
        contact_update,
        AccountOperation::ContactUpdate,
        account.contact_update(ContactId("c1".to_string()), ContactPatch::default())
    );
    refuses!(
        caps,
        contact_delete,
        AccountOperation::ContactDelete,
        account.contact_delete(ContactId("c1".to_string()))
    );
    refuses_somehow!(
        caps,
        contact_autocomplete,
        account.contact_autocomplete("q".to_string(), 5)
    );

    refuses!(
        caps,
        directory_search,
        AccountOperation::DirectorySearch,
        account.directory_search("q".to_string(), None, None)
    );
    refuses!(
        caps,
        directory_groups_list,
        AccountOperation::DirectoryGroupsList,
        account.directory_groups_list(None)
    );
    refuses!(
        caps,
        directory_group_expand,
        AccountOperation::DirectoryGroupExpand,
        account.directory_group_expand(DirectoryGroupId("g1".to_string()), None)
    );

    // Calendars.
    refuses!(
        caps,
        calendars_list,
        AccountOperation::CalendarsList,
        account.calendars_list()
    );
    refuses!(
        caps,
        events_in_range,
        AccountOperation::EventsInRange,
        account.events_in_range(event_range())
    );
    refuses!(
        caps,
        event_get,
        AccountOperation::EventGet,
        account.event_get(EventId("e1".to_string()))
    );
    refuses!(
        caps,
        event_create,
        AccountOperation::EventCreate,
        account.event_create(event_create())
    );
    refuses!(
        caps,
        event_update,
        AccountOperation::EventUpdate,
        account.event_update(EventId("e1".to_string()), EventPatch::default())
    );
    refuses!(
        caps,
        event_delete,
        AccountOperation::EventDelete,
        account.event_delete(EventId("e1".to_string()))
    );
    refuses!(
        caps,
        event_rsvp,
        AccountOperation::EventRsvp,
        account.event_rsvp(
            EventId("e1".to_string()),
            bifrost_types::RsvpStatus::Accepted
        )
    );
    refuses!(
        caps,
        event_search,
        AccountOperation::EventSearch,
        account.event_search(EventSearchRequest::new("q"))
    );
    refuses_somehow!(
        caps,
        event_autocomplete,
        account.event_autocomplete("q".to_string(), 5)
    );
}

/// The mirror is only worth checking if the account under test actually
/// advertises a mixture. A snapshot that was all-true or all-false would make
/// the test above vacuous without anyone noticing.
#[test]
fn the_bare_jmap_snapshot_advertises_both_answers() {
    let caps = account().capabilities().clone();
    assert!(caps.pim_methods.set_keyword, "some flag is true");
    assert!(!caps.pim_methods.send_message, "some flag is false");
}

#[test]
fn push_lanes_follow_websocket_capability_and_reconnect_policy() {
    let mut account = account();
    assert!(account.push_lanes().is_empty());
    account.ws.available = true;
    account.ws.policy.initial = std::time::Duration::from_secs(7);
    account.ws.policy.max = std::time::Duration::from_secs(70);
    let lanes = account.push_lanes();
    assert_eq!(lanes.len(), 1);
    assert_eq!(lanes[0].kind, bifrost_types::PushLaneKind::Connection);
    assert_eq!(
        lanes[0].backoff.initial(),
        std::time::Duration::from_secs(7)
    );
    assert_eq!(lanes[0].backoff.max(), std::time::Duration::from_secs(70));
}
