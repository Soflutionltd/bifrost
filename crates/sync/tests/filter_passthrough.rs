//! Server-side filter passthrough tests.
//!
//! Attaches a recording `Account` double to a `SyncEngine` and drives
//! the five `SyncEngine::filter_*` passthroughs, asserting that (1) each
//! call forwards its arguments 1:1 to the matching `Account` method and
//! relays the account's return value, (2) an `AccountError` folds into
//! `Error::Account` through the `?` funnel, and (3) an unattached
//! account short-circuits to `Error::AccountNotAttached` before any
//! forwarding happens.

use std::sync::{Arc, Mutex};

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFactory, AccountFuture, AccountId, AccountStream, AddressBook, AddressBookId,
    AttachmentHandle, BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Calendar,
    CalendarEvent, Cause, Change, ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate,
    ContactId, ContactPatch, ContactSearchRequest, ContainerId, ContainerKind, ConvenienceShape,
    CursorDescriptor, CursorEstablishment, CursorFreshness, CursorScope, DraftHandle, DraftPatch,
    EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterDiagnostic,
    FilterDiagnosticSeverity, FilterRuleShape, FilterScriptCreate, FilterScriptPatch,
    FilterValidation, FlagOp, HostedAttachment, HydratedObject, HydrationProjection,
    IdempotencyKey, Identity, IdentityId, IdentityPatch, Importance, InventoryEvent, ItemOutcome,
    MembershipScope, Message, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    MutationSuccess, MutationTarget, ObjectId, Page, PimMethodSupport, Priority, Projection,
    PushCapability, QuotaInfo, QuotaSignal, RateLimitClass, RequestCause, RsvpStatus,
    ScopeLifecycleEvent, ScriptLanguage, SearchRequest, SendRequest, ServerFilter,
    ServerFilterCreate, ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent,
    ThreadHydration, ThreadId, VacationConfig,
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

/// What the recording account observed on its last filter call.
#[derive(Default)]
struct Recorded {
    list_called: bool,
    created: Option<ServerFilterCreate>,
    updated: Option<(ServerFilterId, ServerFilterPatch)>,
    deleted: Option<ServerFilterId>,
    validated: Option<ServerFilterCreate>,
}

/// Account double: records the arguments each filter primitive is
/// called with (or, when `fail` is set, returns an `AccountError` so the
/// engine's `?` folding can be exercised). Every non-filter method is
/// stubbed - discovery streams are empty so `attach` establishes zero
/// scopes and completes without touching any of them.
struct FilterAccount {
    caps: AccountCapabilities,
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

struct FilterFactory {
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

impl AccountFactory for FilterFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let account = FilterAccount {
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

/// A representative script filter used both as the `filters_list` return
/// value and to seed create/validate payloads.
fn sample_script_create(body: &str) -> ServerFilterCreate {
    ServerFilterCreate::Script(FilterScriptCreate {
        name: Some("test-script".to_owned()),
        language: ScriptLanguage::Sieve,
        body: body.to_owned(),
        is_active: true,
    })
}

impl Account for FilterAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by filter passthrough tests")
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

    fn push_stream(
        &self,
        lane: bifrost_types::PushLane,
        _stop: bifrost_types::SessionStop,
        _gate: bifrost_types::PushGate,
    ) -> AccountStream<bifrost_types::PushSessionEvent> {
        Box::pin(futures::stream::iter([
            bifrost_types::PushSessionEvent::Failed(bifrost_types::push_lane_contract_error(
                self.protocol(),
                lane,
                "not a lane of this account",
            )),
        ]))
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
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::IdentitiesList)) })
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::IdentityUpdate)) })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::VacationGet)) })
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::VacationSet)) })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::QuotaGet)) })
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::FiltersList));
            }
            rec.lock().expect("recorder lock").list_called = true;
            Ok(vec![ServerFilter::Script(bifrost_types::FilterScript {
                id: ServerFilterId("listed-script".to_owned()),
                name: Some("listed".to_owned()),
                language: ScriptLanguage::Sieve,
                body: "keep;".to_owned(),
                is_active: true,
            })])
        })
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::FilterCreate));
            }
            rec.lock().expect("recorder lock").created = Some(filter);
            Ok(ServerFilterId("created-id".to_owned()))
        })
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::FilterUpdate));
            }
            rec.lock().expect("recorder lock").updated = Some((filter, patch));
            Ok(())
        })
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::FilterDelete));
            }
            rec.lock().expect("recorder lock").deleted = Some(filter);
            Ok(())
        })
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        let fail = self.fail;
        let rec = Arc::clone(&self.rec);
        Box::pin(async move {
            if fail {
                return Err(unsupported(bifrost_types::AccountOperation::FilterValidate));
            }
            rec.lock().expect("recorder lock").validated = Some(filter);
            Ok(FilterValidation {
                diagnostics: vec![FilterDiagnostic {
                    severity: FilterDiagnosticSeverity::Warning,
                    message: "accepted with warning".to_owned(),
                    line: Some(1),
                    column: None,
                }],
            })
        })
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

/// Attach a `FilterAccount` and return the engine plus its shared
/// recorder. `fail` selects the error-returning mode.
async fn attach_filter_account(
    account_id: &AccountId,
    fail: bool,
) -> (SyncEngine, Arc<Mutex<Recorded>>) {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let rec = Arc::new(Mutex::new(Recorded::default()));
    let factory: Arc<dyn AccountFactory> = Arc::new(FilterFactory {
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
async fn filter_passthroughs_forward_arguments_and_relay_returns() {
    let account_id = AccountId("filter-forward".to_owned());
    let (engine, rec) = attach_filter_account(&account_id, false).await;

    // filters_list: relays the account's returned vec.
    let listed = engine
        .filters_list(&account_id)
        .await
        .expect("filters_list forwards");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id(), &ServerFilterId("listed-script".to_owned()));
    assert!(rec.lock().expect("recorder lock").list_called);

    // filter_create: forwards the create payload, relays the new id.
    let create = sample_script_create("fileinto \"INBOX\";");
    let new_id = engine
        .filter_create(&account_id, create)
        .await
        .expect("filter_create forwards");
    assert_eq!(new_id, ServerFilterId("created-id".to_owned()));
    match rec.lock().expect("recorder lock").created.clone() {
        Some(ServerFilterCreate::Script(script)) => {
            assert_eq!(script.body, "fileinto \"INBOX\";");
            assert_eq!(script.name.as_deref(), Some("test-script"));
        }
        other => panic!("expected forwarded script create, got {other:?}"),
    }

    // filter_update: forwards both the id and the patch.
    let patch = ServerFilterPatch::Script(FilterScriptPatch {
        name: None,
        body: Some("discard;".to_owned()),
        is_active: Some(false),
    });
    engine
        .filter_update(&account_id, ServerFilterId("edit-me".to_owned()), patch)
        .await
        .expect("filter_update forwards");
    match rec.lock().expect("recorder lock").updated.clone() {
        Some((id, ServerFilterPatch::Script(script))) => {
            assert_eq!(id, ServerFilterId("edit-me".to_owned()));
            assert_eq!(script.body.as_deref(), Some("discard;"));
            assert_eq!(script.is_active, Some(false));
        }
        other => panic!("expected forwarded script patch, got {other:?}"),
    }

    // filter_delete: forwards the id.
    engine
        .filter_delete(&account_id, ServerFilterId("drop-me".to_owned()))
        .await
        .expect("filter_delete forwards");
    assert_eq!(
        rec.lock().expect("recorder lock").deleted.clone(),
        Some(ServerFilterId("drop-me".to_owned()))
    );

    // filter_validate: forwards the payload, relays the diagnostics.
    let validation = engine
        .filter_validate(&account_id, sample_script_create("keep;"))
        .await
        .expect("filter_validate forwards");
    assert!(validation.is_valid());
    assert_eq!(validation.diagnostics.len(), 1);
    match rec.lock().expect("recorder lock").validated.clone() {
        Some(ServerFilterCreate::Script(script)) => assert_eq!(script.body, "keep;"),
        other => panic!("expected forwarded validate payload, got {other:?}"),
    }
}

#[tokio::test]
async fn filter_passthroughs_fold_account_error_into_error_account() {
    let account_id = AccountId("filter-error".to_owned());
    let (engine, _rec) = attach_filter_account(&account_id, true).await;

    assert!(matches!(
        engine.filters_list(&account_id).await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_create(&account_id, sample_script_create("keep;"))
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_update(
                &account_id,
                ServerFilterId("x".to_owned()),
                ServerFilterPatch::Script(FilterScriptPatch::default()),
            )
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_delete(&account_id, ServerFilterId("x".to_owned()))
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_validate(&account_id, sample_script_create("keep;"))
            .await,
        Err(Error::Account(_))
    ));
}

#[tokio::test]
async fn filter_passthroughs_reject_unattached_account() {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let missing = AccountId("never-attached".to_owned());

    assert!(matches!(
        engine.filters_list(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_create(&missing, sample_script_create("keep;"))
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_update(
                &missing,
                ServerFilterId("x".to_owned()),
                ServerFilterPatch::Script(FilterScriptPatch::default()),
            )
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_delete(&missing, ServerFilterId("x".to_owned()))
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_validate(&missing, sample_script_create("keep;"))
            .await,
        Err(Error::AccountNotAttached(_))
    ));
}
