//! In-crate `Account` test double for the composition unit tests
//! (bricks 1-5). Deterministic, no server: most methods return
//! `Unsupported`; discovery/inventory/changes/describe/establish are
//! configurable so the IMAP composition wiring can be exercised in
//! isolation.

#![cfg(test)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFuture, AccountOperation, AccountStream, AddressBook, AddressBookId, AttachmentHandle,
    Batch, BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Calendar, CalendarEvent, Cause,
    Change, ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest, ContainerId, ContainerKind, ContainerList, ConvenienceShape, CostClass,
    CursorDescriptor, CursorEstablishment, CursorFreshness, CursorScope, DirectoryCard,
    DirectoryGroup, DirectoryGroupId, DirectoryGroupMember, DraftHandle, DraftPatch, EventCreate,
    EventId, EventPatch, EventRange, EventSearchRequest, FilterRuleShape, FilterValidation, FlagOp,
    HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, Importance, InventoryEntry, InventoryEvent, ItemOutcome, MembershipScope,
    Message, MutationCapabilities, MutationConcurrency, MutationReplaySafety, MutationSuccess,
    MutationTarget, ObjectId, Page, PageBoundary, PimMethodSupport, Priority, Projection,
    PushCapability, QuotaInfo, QuotaSignal, RateLimitClass, RequestCause, RsvpStatus,
    ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent, SyncStrategy,
    ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream;

fn unsupported(op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// Arguments observed by the directory forwarding test.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DirectoryCall {
    pub(crate) query: String,
    pub(crate) limit: Option<u32>,
    pub(crate) cursor: Option<Vec<u8>>,
}

/// A configurable `Account` test double. Records whether each sync
/// entry point was dispatched to it, and emits a fixed set of discovery
/// scopes / a sentinel inventory batch / a sentinel change batch.
pub(crate) struct StubAccount {
    caps: AccountCapabilities,
    pub(crate) directory_call: std::sync::Mutex<Option<DirectoryCall>>,
    scopes: Vec<CursorScope>,
    pub(crate) inventory_called: AtomicBool,
    pub(crate) changes_called: AtomicBool,
    pub(crate) establish_called: AtomicBool,
    pub(crate) describe_called: AtomicBool,
    /// Records the `(keyword, value)` of the last `set_keyword` call so
    /// flag-convenience dispatch (e.g. `mark_mdn_sent`) can be observed.
    pub(crate) last_set_keyword: std::sync::Mutex<Option<(String, bool)>>,
}

/// Sentinel ObjectId an `inventory_stream` / `changes_stream`
/// delegation emits, so tests can confirm the sub-account served the
/// call rather than IMAP.
pub(crate) const STUB_SENTINEL: &str = "stub-sentinel";

impl StubAccount {
    pub(crate) fn new(scopes: Vec<CursorScope>) -> Self {
        Self {
            caps: stub_capabilities(),
            scopes,
            inventory_called: AtomicBool::new(false),
            changes_called: AtomicBool::new(false),
            establish_called: AtomicBool::new(false),
            describe_called: AtomicBool::new(false),
            last_set_keyword: std::sync::Mutex::new(None),
            directory_call: std::sync::Mutex::new(None),
        }
    }

    /// Build with a fully custom capability snapshot (brick 4 needs to
    /// pin specific `pim_methods` flags).
    pub(crate) fn with_capabilities(caps: AccountCapabilities) -> Self {
        Self {
            caps,
            scopes: Vec::new(),
            inventory_called: AtomicBool::new(false),
            changes_called: AtomicBool::new(false),
            establish_called: AtomicBool::new(false),
            describe_called: AtomicBool::new(false),
            last_set_keyword: std::sync::Mutex::new(None),
            directory_call: std::sync::Mutex::new(None),
        }
    }
}

pub(crate) fn stub_capabilities() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
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
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

fn sentinel_inventory_batch() -> SyncEvent<InventoryEntry> {
    SyncEvent::Batch(Batch {
        items: vec![InventoryEntry {
            id: ObjectId(STUB_SENTINEL.to_string()),
            memberships: Vec::new(),
            size: None,
            blob_id: None,
            fingerprint: bifrost_types::Fingerprint {
                server_version: bifrost_types::ServerVersion::Unavailable,
                size: None,
                flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
            },
            thread_id: None,
            message_id: None,
            references: Vec::new(),
            in_reply_to: None,
        }],
        page_boundary: PageBoundary::Final,
        server_latency: std::time::Duration::ZERO,
        bytes_in: 0,
        checkpoint: None,
    })
}

fn sentinel_change_batch() -> SyncEvent<Change> {
    SyncEvent::Batch(Batch {
        items: vec![Change::ObjectChange(bifrost_types::ObjectChange {
            id: ObjectId(STUB_SENTINEL.to_string()),
            kind: bifrost_types::ObjectChangeKind::Updated,
        })],
        page_boundary: PageBoundary::Final,
        server_latency: std::time::Duration::ZERO,
        bytes_in: 0,
        checkpoint: None,
    })
}

impl Account for StubAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        self.describe_called.store(true, Ordering::SeqCst);
        CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::ServerCursor,
            freshness: None,
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        let items = self.scopes.clone();
        Box::pin(stream::iter([
            SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ]))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        Box::pin(stream::iter([
            SyncEvent::Batch(Batch {
                items: Vec::new(),
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ]))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        _scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        self.establish_called.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(CursorEstablishment::EstablishViaInventory) })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        self.inventory_called.store(true, Ordering::SeqCst);
        Box::pin(
            stream::iter([sentinel_inventory_batch(), SyncEvent::Done(None)]).map(
                bifrost_types::lift_complete_walk(bifrost_types::CoverageDomain::full(scope)),
            ),
        )
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        Box::pin(stream::empty())
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        self.changes_called.store(true, Ordering::SeqCst);
        Box::pin(stream::iter([
            sentinel_change_batch(),
            SyncEvent::Done(None),
        ]))
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::PushSubscribe)) })
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::PushUnsubscribe)) })
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

    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::AddToContainer)) })
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::RemoveFromContainer)) })
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        // Records the call and succeeds so flag-convenience dispatch
        // (e.g. mark_mdn_sent) can be observed reaching the success path.
        *self.last_set_keyword.lock().expect("stub mutex") = Some((keyword, value));
        Box::pin(async { Ok(()) })
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::SetLabelMembership)) })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::SetCategory)) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::SetExtendedProperty)) })
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::SetIsRead)) })
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::SetImportance)) })
    }

    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::Send)) })
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::AttachmentUpload)) })
    }

    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::HostAttachment)) })
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DraftCreate)) })
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DraftUpdate)) })
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DraftDiscard)) })
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DraftSend)) })
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::CancelScheduledSend)) })
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::RescheduleSend)) })
    }

    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::Search)) })
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::SearchMessages)) })
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContainersList)) })
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContainerCreate)) })
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContainerRename)) })
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContainerMove)) })
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContainerDelete)) })
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::IdentitiesList)) })
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::IdentityUpdate)) })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::VacationGet)) })
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::VacationSet)) })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::QuotaGet)) })
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::FiltersList)) })
    }

    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::FilterCreate)) })
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::FilterUpdate)) })
    }

    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::FilterDelete)) })
    }

    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::FilterValidate)) })
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::AddressBooksList)) })
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContactsList)) })
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContactGet)) })
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContactCreate)) })
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContactUpdate)) })
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContactDelete)) })
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::ContactSearch)) })
    }

    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        *self.directory_call.lock().expect("lock") = Some(DirectoryCall {
            query,
            limit,
            cursor: page_cursor,
        });
        if !self.caps.pim_methods.directory_search {
            return Box::pin(async { Err(unsupported(AccountOperation::DirectorySearch)) });
        }
        Box::pin(async {
            let mut page = Page::single(
                vec![DirectoryCard {
                    email: "directory-sentinel@example.test".into(),
                    display_name: None,
                    additional_emails: Vec::new(),
                    phones: Vec::new(),
                    company: None,
                    title: None,
                    department: None,
                    provider: bifrost_types::ProtocolKind::CardDav,
                }],
                Vec::new(),
                Vec::new(),
            );
            page.truncated = true;
            Ok(page)
        })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DirectoryGroupsList)) })
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DirectoryGroupExpand)) })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::CalendarsList)) })
    }

    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::EventsInRange)) })
    }

    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::EventGet)) })
    }

    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::EventCreate)) })
    }

    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::EventUpdate)) })
    }

    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::EventDelete)) })
    }

    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::EventRsvp)) })
    }

    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::EventSearch)) })
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::HydrateThread)) })
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::HydrateMessage)) })
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Wrap a `StubAccount` as an `Arc<dyn Account>` for the composition
/// fields.
pub(crate) fn stub_arc(account: StubAccount) -> Arc<dyn Account> {
    Arc::new(account)
}

/// The parts of a real `ImapAccount` whose pool holds exactly `pool_cap`
/// permits and is primed with `conn`, usually one half of
/// `connection::test_support::driver_pair`.
///
/// Every field a test might care about is at its quietest default: stub
/// capabilities, an empty folder registry, no QRESYNC, no submission, no
/// composed DAV. Override what the test needs, then `ImapAccount::new`.
/// The duplex transport cannot dial, so once the primed connection is
/// checked out a second concurrent checkout hangs rather than silently
/// opening another connection.
pub(crate) fn scripted_account_parts(
    conn: crate::ImapConnection,
    pool_cap: usize,
) -> super::ImapAccountParts {
    let config = Arc::new(super::factory::ImapAccountConfig {
        pool_cap,
        ..super::factory::ImapAccountConfig::new(
            crate::ImapConfig::plaintext("test.invalid"),
            crate::types::Credentials::password("user", "pass"),
            crate::types::AuthPolicy::default(),
        )
    });
    let bandwidth_budget = bifrost_net::BandwidthBudget::default();
    let pool = Arc::new(super::Pool::new(
        Arc::clone(&config),
        conn,
        pool_cap,
        None,
        bandwidth_budget.clone(),
    ));
    super::ImapAccountParts {
        config,
        capabilities: stub_capabilities(),
        pool,
        folders: Arc::new(super::folder_registry::FolderRegistry::default()),
        qresync_enabled: false,
        qresync_negotiation_warning: None,
        supports_notify: false,
        bandwidth_budget,
        contacts: None,
        calendars: None,
        dav_scopes: Default::default(),
        submission: None,
        dav_degraded: Vec::new(),
    }
}

#[cfg(test)]
mod mdn_tests {
    use super::*;

    fn caps_with_mdn(mdn_sent_via_keyword: bool) -> AccountCapabilities {
        let mut caps = stub_capabilities();
        caps.conveniences.mdn_sent_via_keyword = mdn_sent_via_keyword;
        caps
    }

    #[tokio::test]
    async fn mark_mdn_sent_dispatches_to_set_keyword_when_enabled() {
        // mdn_sent_via_keyword: true -> mark_mdn_sent flips `$MDNSent`
        // through set_keyword, observed succeeding and recorded.
        let stub = StubAccount::with_capabilities(caps_with_mdn(true));
        stub.mark_mdn_sent(ObjectId("m1".to_string()))
            .await
            .expect("mark_mdn_sent dispatches to set_keyword");
        let recorded = stub.last_set_keyword.lock().expect("stub mutex").clone();
        assert_eq!(recorded, Some(("$MDNSent".to_string(), true)));
    }

    #[tokio::test]
    async fn mark_mdn_sent_unsupported_when_read_only() {
        // mdn_sent_via_keyword: false -> the read-receipt model is
        // read-only (Gmail/Graph), so the convenience surfaces
        // Unsupported(MarkMdnSent) and never touches set_keyword.
        let stub = StubAccount::with_capabilities(caps_with_mdn(false));
        let err = stub
            .mark_mdn_sent(ObjectId("m1".to_string()))
            .await
            .expect_err("mark_mdn_sent is unsupported when read-only");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::MarkMdnSent)
        );
        assert!(stub.last_set_keyword.lock().expect("stub mutex").is_none());
    }
}
