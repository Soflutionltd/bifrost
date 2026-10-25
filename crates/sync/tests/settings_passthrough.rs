//! Account settings passthrough tests.
//!
//! Attaches a recording `Account` double to a `SyncEngine` and drives
//! the five `SyncEngine` settings passthroughs (`identities_list`,
//! `identity_update`, `vacation_get`, `vacation_set`, `quota_get`),
//! asserting that (1) each call forwards its arguments 1:1 to the
//! matching `Account` method and relays the account's return value, (2)
//! an `AccountError` folds into `Error::Account` through the `?` funnel,
//! and (3) an unattached account short-circuits to
//! `Error::AccountNotAttached` before any forwarding happens.

use std::sync::{Arc, Mutex};

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFactory, AccountFuture, AccountId, AccountStream, AddressBook, AddressBookId,
    AttachmentHandle, BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Calendar,
    CalendarEvent, Cause, Change, ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate,
    ContactId, ContactPatch, ContactSearchRequest, ContainerId, ContainerKind, ConvenienceShape,
    CursorDescriptor, CursorEstablishment, CursorFreshness, CursorScope, DraftHandle, DraftPatch,
    EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterRuleShape,
    FilterValidation, FlagOp, HostedAttachment, HydratedObject, HydrationProjection,
    IdempotencyKey, Identity, IdentityId, IdentityPatch, Importance, InventoryEvent, ItemOutcome,
    MembershipScope, Message, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    MutationSuccess, MutationTarget, ObjectId, Page, PimMethodSupport, Priority, Projection,
    PushCapability, QuotaInfo, QuotaSignal, RateLimitClass, RequestCause, RsvpStatus,
    ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId,
    VacationConfig, WatchEvent,
};
use bytes::Bytes;
use futures::stream;

/// Convenience for stubs that just need to signal "this operation is
/// not implemented in this test double."
fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

fn caps() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
            reassertion: bifrost_types::MutationReassertion::NONE,
        },
        batching_policy: BatchingPolicy {
            max_items: 100,
            max_wait: std::time::Duration::from_millis(50),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::None,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport::default(),
        filter_rule_shape: FilterRuleShape::Scripts,
        conveniences: ConvenienceShape::default(),
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

/// What the recording account observed on its last settings call.
#[derive(Default)]
struct Recorded {
    identities_called: bool,
    identity_updated: Option<(IdentityId, IdentityPatch)>,
    vacation_get_called: bool,
    vacation_set: Option<VacationConfig>,
    quota_called: bool,
}

/// Account double: records the arguments each settings primitive is
/// called with (or, when `fail` is set, returns an `AccountError` so the
/// engine's `?` folding can be exercised). Every non-settings method is
/// stubbed - discovery streams are empty so `attach` establishes zero
/// scopes and completes without touching any of them.
struct SettingsAccount {
    caps: AccountCapabilities,
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

struct SettingsFactory {
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

impl AccountFactory for SettingsFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let account = SettingsAccount {
            caps: caps(),
            rec: Arc::clone(&self.rec),
            fail: self.fail,
        };
        Box::pin(async move {
            Ok(bifrost_types::OpenedAccount::complete(
                Arc::new(account) as Arc<dyn Account>
            ))
        })
    }
}

/// A representative identity used as the `identities_list` return value.
fn sample_identity() -> Identity {
    Identity {
        id: IdentityId("listed-identity".to_owned()),
        name: "Listed User".to_owned(),
        address: "listed@example.test".to_owned(),
        signature_text: Some("-- listed".to_owned()),
        signature_html: None,
        reply_to: None,
        is_default: true,
    }
}

impl Account for SettingsAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by settings passthrough tests")
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        Box::pin(stream::empty())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        Box::pin(stream::empty())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        _scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::EstablishCursor,
            ))
        })
    }

    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<InventoryEvent> {
        Box::pin(stream::empty())
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        Box::pin(stream::empty::<SyncEvent<ItemOutcome<HydratedObject>>>())
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        Box::pin(stream::empty())
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::PushSubscribe)) })
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::PushUnsubscribe,
            ))
        })
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        Box::pin(stream::empty())
    }

    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn bulk_set_flags(
        &self,
        _targets: AccountStream<ObjectId>,
        _op: FlagOp,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Ok(()) })
    }

    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::AddToContainer)) })
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::RemoveFromContainer,
            ))
        })
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetKeyword)) })
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::SetLabelMembership,
            ))
        })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetCategory)) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::SetExtendedProperty,
            ))
        })
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetIsRead)) })
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetImportance)) })
    }

    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::Send)) })
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::AttachmentUpload,
            ))
        })
    }

    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HostAttachment)) })
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftCreate)) })
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftUpdate)) })
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftDiscard)) })
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftSend)) })
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::CancelScheduledSend,
            ))
        })
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::RescheduleSend)) })
    }

    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::Search)) })
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SearchMessages)) })
    }

    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainersList)) })
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerCreate,
            ))
        })
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerRename,
            ))
        })
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainerMove)) })
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerDelete,
            ))
        })
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::IdentitiesList));
            }
            rec.lock().expect("recorder lock").identities_called = true;
            Ok(vec![sample_identity()])
        })
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::IdentityUpdate));
            }
            rec.lock().expect("recorder lock").identity_updated = Some((identity, patch));
            Ok(())
        })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::VacationGet));
            }
            rec.lock().expect("recorder lock").vacation_get_called = true;
            Ok(Some(VacationConfig {
                is_enabled: true,
                subject: Some("Away".to_owned()),
                body_text: Some("Out of office".to_owned()),
                body_html: None,
                starts_at: None,
                ends_at: None,
            }))
        })
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::VacationSet));
            }
            rec.lock().expect("recorder lock").vacation_set = Some(config);
            Ok(())
        })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::QuotaGet));
            }
            rec.lock().expect("recorder lock").quota_called = true;
            Ok(Some(QuotaInfo {
                used_bytes: 512,
                total_bytes: Some(4096),
            }))
        })
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FiltersList)) })
    }

    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterCreate)) })
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterUpdate)) })
    }

    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterDelete)) })
    }

    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterValidate)) })
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::AddressBooksList,
            ))
        })
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactsList)) })
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactGet)) })
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactCreate)) })
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactUpdate)) })
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactDelete)) })
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactSearch)) })
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectorySearch,
            ))
        })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectoryGroupsList,
            ))
        })
    }

    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectoryGroupExpand,
            ))
        })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::CalendarsList)) })
    }

    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventsInRange)) })
    }

    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventGet)) })
    }

    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventCreate)) })
    }

    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventUpdate)) })
    }

    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventDelete)) })
    }

    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventRsvp)) })
    }

    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventSearch)) })
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HydrateThread)) })
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HydrateMessage)) })
    }
}

/// Attach a `SettingsAccount` and return the engine plus its shared
/// recorder. `fail` selects the error-returning mode.
async fn attach_settings_account(
    account_id: &AccountId,
    fail: bool,
) -> (SyncEngine, Arc<Mutex<Recorded>>) {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let rec = Arc::new(Mutex::new(Recorded::default()));
    let factory: Arc<dyn AccountFactory> = Arc::new(SettingsFactory {
        rec: Arc::clone(&rec),
        fail,
    });
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds with empty discovery streams");
    (engine, rec)
}

#[tokio::test]
async fn settings_passthroughs_forward_arguments_and_relay_returns() {
    let account_id = AccountId("settings-forward".to_owned());
    let (engine, rec) = attach_settings_account(&account_id, false).await;

    // identities_list: relays the account's returned vec.
    let listed = engine
        .identities_list(&account_id)
        .await
        .expect("identities_list forwards");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, IdentityId("listed-identity".to_owned()));
    assert_eq!(listed[0].address, "listed@example.test");
    assert!(rec.lock().expect("recorder lock").identities_called);

    // identity_update: forwards both the id and the patch.
    let mut patch = IdentityPatch::default();
    patch.name = Some("Renamed".to_owned());
    patch.signature_text = Some(Some("-- new sig".to_owned()));
    patch.is_default = Some(true);
    engine
        .identity_update(&account_id, IdentityId("edit-me".to_owned()), patch)
        .await
        .expect("identity_update forwards");
    match rec.lock().expect("recorder lock").identity_updated.clone() {
        Some((id, patch)) => {
            assert_eq!(id, IdentityId("edit-me".to_owned()));
            assert_eq!(patch.name.as_deref(), Some("Renamed"));
            assert_eq!(patch.signature_text, Some(Some("-- new sig".to_owned())));
            assert_eq!(patch.is_default, Some(true));
        }
        None => panic!("expected forwarded identity patch"),
    }

    // vacation_get: relays the account's returned config.
    let vacation = engine
        .vacation_get(&account_id)
        .await
        .expect("vacation_get forwards")
        .expect("responder configured");
    assert!(vacation.is_enabled);
    assert_eq!(vacation.subject.as_deref(), Some("Away"));
    assert!(rec.lock().expect("recorder lock").vacation_get_called);

    // vacation_set: forwards the config payload.
    let config = VacationConfig {
        is_enabled: false,
        subject: Some("Back soon".to_owned()),
        body_text: Some("Returning Monday".to_owned()),
        body_html: Some("<p>Returning Monday</p>".to_owned()),
        starts_at: None,
        ends_at: None,
    };
    engine
        .vacation_set(&account_id, config)
        .await
        .expect("vacation_set forwards");
    match rec.lock().expect("recorder lock").vacation_set.clone() {
        Some(config) => {
            assert!(!config.is_enabled);
            assert_eq!(config.subject.as_deref(), Some("Back soon"));
            assert_eq!(config.body_text.as_deref(), Some("Returning Monday"));
            assert_eq!(config.body_html.as_deref(), Some("<p>Returning Monday</p>"));
        }
        None => panic!("expected forwarded vacation config"),
    }

    // quota_get: relays the account's returned readout.
    let quota = engine
        .quota_get(&account_id)
        .await
        .expect("quota_get forwards")
        .expect("quota reported");
    assert_eq!(quota.used_bytes, 512);
    assert_eq!(quota.total_bytes, Some(4096));
    assert!(rec.lock().expect("recorder lock").quota_called);
}

#[tokio::test]
async fn settings_passthroughs_fold_account_error_into_error_account() {
    let account_id = AccountId("settings-error".to_owned());
    let (engine, _rec) = attach_settings_account(&account_id, true).await;

    assert!(matches!(
        engine.identities_list(&account_id).await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .identity_update(
                &account_id,
                IdentityId("x".to_owned()),
                IdentityPatch::default(),
            )
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine.vacation_get(&account_id).await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .vacation_set(
                &account_id,
                VacationConfig {
                    is_enabled: false,
                    subject: None,
                    body_text: None,
                    body_html: None,
                    starts_at: None,
                    ends_at: None,
                },
            )
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine.quota_get(&account_id).await,
        Err(Error::Account(_))
    ));
}

#[tokio::test]
async fn settings_passthroughs_reject_unattached_account() {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let missing = AccountId("never-attached".to_owned());

    assert!(matches!(
        engine.identities_list(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .identity_update(
                &missing,
                IdentityId("x".to_owned()),
                IdentityPatch::default()
            )
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine.vacation_get(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .vacation_set(
                &missing,
                VacationConfig {
                    is_enabled: false,
                    subject: None,
                    body_text: None,
                    body_html: None,
                    starts_at: None,
                    ends_at: None,
                },
            )
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine.quota_get(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
}
