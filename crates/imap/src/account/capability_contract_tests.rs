//! The capability mirror, checked against the account it describes.
//!
//! `PimMethodSupport` is a hand-maintained mirror of the `Account` trait: sixty
//! `bool`s with no mechanical link to the methods they describe. Nothing else
//! in the workspace checks that a `false` flag implies the method actually
//! refuses. This drives every gated method on the crate's own `Account` impl
//! and asserts the flag and the observed result agree.
//!
//! **Coverage here is one-directional, by necessity, not oversight.** A `false`
//! flag must make the method return `Unsupported` BEFORE it does any wire work.
//! The account is built around a `driver_pair` connection whose server half is
//! dropped immediately, so the socket is dead: a method that reached the pool
//! would surface a transport error rather than `Unsupported`, and fail here.
//! A `true` flag means the method WILL drive a command sequence, and
//! `scripted_tests.rs` already owns that ground with real transcripts. So the
//! true direction is deliberately left to those tests, and this file pins the
//! half nothing else pins: nothing advertised `false` may quietly work.
//!
//! Writing this file immediately found three flags that did not gate their
//! methods at all: `remove_from_container` and `draft_discard` (both false
//! when the server offers neither UIDPLUS nor IMAP4rev2, so no UID EXPUNGE)
//! and `thread_hydrate` (false without `THREAD=REFERENCES`). All three decoded
//! their target and proceeded toward the wire regardless of the flag, while
//! `search`, `draft_create` and `quota_get` in the same module read theirs
//! correctly. The methods were fixed to match the flags - the capability is
//! derived from a server capability string, so the flag was the true half -
//! and all three are now driven below like everything else.

#![cfg(test)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

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

use super::capabilities::build_capabilities;
use super::test_support::scripted_account_parts;
use super::{ImapAccount, ImapAccountParts};
use crate::connection::test_support::{driver_pair, preauth_greeting};
use crate::types::{Credentials, ServerProfile};

/// An account carrying the REAL IMAP capability snapshot for a bare server
/// (no UIDPLUS, no THREAD, no QUOTA, no IDLE), no ManageSieve, no submission
/// and no composed DAV sub-accounts - the shape with the widest false set.
///
/// The scripted connection's server half is dropped before the account is
/// used, so the pool holds a dead socket and any wire attempt fails loudly
/// instead of hanging.
async fn account() -> ImapAccount {
    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    drop(server);

    let profile = ServerProfile::new(Vec::new(), Vec::new());
    ImapAccount::new(ImapAccountParts {
        capabilities: build_capabilities(&profile, &[], false, None, None, false, false, 1),
        ..scripted_account_parts(conn, 1)
    })
}

/// The same account with an SMTP submission transport configured, which is
/// what makes `pim_methods.send_message` true.
///
/// Needed for one assertion only, and the reason is worth stating because it is
/// the difference between a test that bites and one that does not. `send_as` is
/// always false on IMAP, so its documented answer is `Unsupported(Send)` - and
/// so is the answer the SUBMISSION gate gives when no transport is configured.
/// Against the bare fixture the assertion therefore holds whether or not
/// `send_as_guard` exists at all. With a transport present, deleting the guard
/// lets the send proceed toward SMTP instead, which is not `Unsupported`, so
/// the assertion is about the guard rather than about the gate behind it.
///
/// Hermetic: `SubmissionTransport::build` opens no socket, and the assertion
/// never gets past the guard.
async fn account_with_submission() -> ImapAccount {
    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    drop(server);

    let parts = scripted_account_parts(conn, 1);
    let submission_config = super::SmtpSubmissionConfig::new(
        "smtp.test.invalid",
        super::SubmissionTls::Plaintext,
        bifrost_types::Address::bare("user@test.invalid"),
    );
    let submission = Arc::new(
        super::SubmissionTransport::build(
            &submission_config,
            &Credentials::password("user", "pass"),
            None,
            parts.bandwidth_budget.clone(),
        )
        .expect("a plaintext relay transport builds without I/O"),
    );
    let profile = ServerProfile::new(Vec::new(), Vec::new());
    ImapAccount::new(ImapAccountParts {
        capabilities: build_capabilities(&profile, &[], false, None, None, true, false, 1),
        submission: Some(submission),
        ..parts
    })
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

/// A send naming a shared mailbox no account in this workspace holds.
///
/// Unknownness comes from the value being absent from every fixture's routing
/// table, not from the string being degenerate: `MailboxId` has no syntax
/// validator, so an empty or malformed id would invite a future validation
/// layer to reject it for an unrelated reason and quietly stop testing the
/// routing lookup. Everything else stays default - `scheduled: None`
/// especially, since a scheduled foreign send is refused before the routing
/// question is even reached.
fn send_as_request() -> SendRequest {
    let mut request = SendRequest::default();
    request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::MailboxId(
        "contract-unknown-mailbox".to_string(),
    )));
    request
}

fn assert_unsupported(flag: &str, expected: AccountOperation, error: &AccountError) {
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Unsupported(expected),
        "pim_methods.{flag} is false, so the method must refuse with Unsupported({expected:?})",
    );
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
/// `Request(Malformed)` with a `send_as.mailbox` field pointer for a UI to
/// highlight.
///
/// Both directions are assertable here, and the true direction needs no network
/// seam, because the behaviour under test IS a local refusal: graph resolves
/// `shared_clients` and jmap runs `route_send_as` before either one touches a
/// transport. That is what makes this different from asserting that a supported
/// method SUCCEEDS, which this file cannot do.
///
/// The expected kind is derived from the crate's own flag rather than written
/// down, which is the point: a backend whose flag and behaviour drift apart
/// fails here instead of agreeing with a local copy of the rule forever. It
/// caught two live violations the day it was written - graph and jmap both
/// answered `Request(Malformed)` while advertising `send_as == false`.
///
/// Only meaningful where `send_message` itself is available. In a crate that
/// refuses `send_message` outright, the entry-point gate answers first and this
/// would pass against a `send_as` implementation that had been deleted.
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
            assert_unsupported("send_as", AccountOperation::Send, &error);
        }
    };
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
    let account = account().await;
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
    // The send_as field contract. IMAP has no mailbox routing table, so only
    // the false branch exists here - but it needs its own fixture, because
    // against the bare account the submission gate would answer with the same
    // kind and the assertion would hold with `send_as_guard` deleted.
    {
        let sending = account_with_submission().await;
        let sending_caps = sending.capabilities();
        refuses_field!(sending_caps, sending.send_message(send_as_request()));
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

    // Contacts. False on a bare IMAP account: no CardDAV sub-account is
    // composed in, so the whole contact surface must refuse.
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
fn the_bare_imap_snapshot_advertises_both_answers() {
    let caps = build_capabilities(
        &ServerProfile::new(Vec::new(), Vec::new()),
        &[],
        false,
        None,
        None,
        false,
        false,
        1,
    );
    assert!(caps.pim_methods.set_keyword, "some flag is true");
    assert!(!caps.pim_methods.search, "some flag is false");
}
