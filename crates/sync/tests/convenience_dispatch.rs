//! `Account` convenience-layer dispatch tests.
//!
//! The default impls on `bifrost_types::Account` encode the
//! capability-driven dispatch table (starred / replied / forwarded /
//! MDN-sent / label provenance) that every protocol crate inherits.
//! Nothing previously pinned that table; a silent change to, say, the
//! `$flagged` sentinel the Category arm sends would break Graph's
//! `is_starred_category` match without any test noticing.
//!
//! The stub records every primitive invocation as a string; each test
//! asserts exactly which primitive fired and with which arguments.

use std::sync::Mutex;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFuture, AccountOperation, AccountStream, AttachmentHandle, BatchingPolicy, BlobHandle,
    BlobRangeSupport, ByteRange, Calendar, CalendarEvent, Cause, Change, ChangeCursor,
    CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch, ContactSearchRequest,
    ContainerId, ContainerKind, ConvenienceShape, CursorDescriptor, CursorEstablishment,
    CursorFreshness, CursorScope, DraftHandle, DraftPatch, EventCreate, EventId, EventPatch,
    EventRange, EventSearchRequest, FilterRuleShape, FilterValidation, FlagOp, HostedAttachment,
    HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId, IdentityPatch,
    Importance, InventoryEvent, InventoryPartition, ItemOutcome, Label, MembershipScope, Message,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, MutationSuccess,
    MutationTarget, ObjectId, Page, PimMethodSupport, Priority, Projection, ProtocolKind,
    Provenance, PushCapability, QuotaInfo, QuotaSignal, RateLimitClass, RequestCause, RsvpStatus,
    ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, StarredFlagShape, SubscriptionHandle, SyncEvent,
    ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

fn unsupported(op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

fn caps_with(conveniences: ConvenienceShape) -> AccountCapabilities {
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
        filter_rule_shape: FilterRuleShape::None,
        conveniences,
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

/// Recorder stub: the label/flag primitives log their arguments and
/// succeed; everything else answers `Unsupported`.
struct RecorderAccount {
    caps: AccountCapabilities,
    calls: Mutex<Vec<String>>,
}

impl RecorderAccount {
    fn with_conveniences(conveniences: ConvenienceShape) -> Self {
        Self {
            caps: caps_with(conveniences),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("poisoned").push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("poisoned").clone()
    }
}

fn ok_future() -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Ok(()) })
}

impl Account for RecorderAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by dispatch tests")
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
        Box::pin(async { Err(unsupported(AccountOperation::EstablishCursor)) })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        self.record("inventory_stream".into());
        Box::pin(stream::once(async move {
            InventoryEvent::Done(bifrost_types::InventoryCompletion::complete(
                bifrost_types::CoverageDomain::full(scope),
                None,
                bifrost_types::PartitionEnd::Exhausted,
            ))
        }))
    }

    fn inventory_resume_stream(
        &self,
        _cursor: ChangeCursor,
    ) -> Option<AccountStream<InventoryEvent>> {
        Some(Box::pin(stream::empty()))
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        Box::pin(stream::empty())
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        Box::pin(stream::empty())
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

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Ok(()) })
    }

    // ---- recorded primitives the conveniences dispatch into ----

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.record(format!("add_to_container:{target:?}:{}", container.0));
        ok_future()
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.record(format!("remove_from_container:{target:?}:{}", container.0));
        ok_future()
    }

    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.record(format!("set_keyword:{target:?}:{keyword}:{value}"));
        ok_future()
    }

    fn set_label_membership(
        &self,
        target: MutationTarget,
        label: ContainerId,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.record(format!(
            "set_label_membership:{target:?}:{}:{value}",
            label.0
        ));
        ok_future()
    }

    fn set_category(
        &self,
        target: MutationTarget,
        category: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.record(format!("set_category:{target:?}:{category}:{value}"));
        ok_future()
    }

    fn set_extended_property(
        &self,
        target: MutationTarget,
        property_id: String,
        value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.record(format!(
            "set_extended_property:{target:?}:{property_id}:{value:?}"
        ));
        ok_future()
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.record(format!("set_is_read:{target:?}:{is_read}"));
        ok_future()
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::SetImportance)) })
    }

    // ---- remaining required methods: unsupported stubs ----

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

    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
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

    fn address_books_list(
        &self,
    ) -> AccountFuture<Result<Vec<bifrost_types::AddressBook>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::AddressBooksList)) })
    }

    fn contacts_list(
        &self,
        _address_book: Option<bifrost_types::AddressBookId>,
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

    /// Records the limit the autocomplete convenience threaded through
    /// and returns an empty single page.
    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        self.record(format!(
            "contact_search:{}:{:?}",
            request.query, request.limit
        ));
        Box::pin(async { Ok(Page::single(Vec::new(), Vec::new(), Vec::new())) })
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DirectorySearch)) })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::DirectoryGroupsList)) })
    }

    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
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

    /// Records the limit and returns an empty page, mirroring
    /// `contact_search`.
    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        self.record(format!(
            "event_search:{}:{:?}",
            request.query, request.limit
        ));
        Box::pin(async { Ok(Page::single(Vec::new(), Vec::new(), Vec::new())) })
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
}

#[test]
fn default_inventory_cursor_classifier_delegates_to_resume_hook() {
    let cursor = ChangeCursor {
        scope: CursorScope::Account,
        server_state: bifrost_types::OpaqueChangeState {
            protocol: ProtocolKind::Gmail,
            envelope_version: 1,
            bytes: Vec::new(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
    };
    assert!(Account::is_inventory_cursor(
        &RecorderAccount::with_conveniences(ConvenienceShape::default()),
        &cursor,
    ));
}

fn target() -> MutationTarget {
    MutationTarget::Message(ObjectId("m-1".into()))
}

fn label(provider: ProtocolKind, kind: ContainerKind, id: &str, native: &str) -> Label {
    Label::new(
        ContainerId(id.into()),
        Provenance {
            provider,
            kind,
            native: native.into(),
        },
        "display".into(),
        None,
    )
}

fn assert_unsupported(err: &AccountError, op: AccountOperation) {
    assert_eq!(
        err.kind(),
        &AccountErrorKind::Unsupported(op),
        "expected Unsupported({op:?}), got {:?}",
        err.kind()
    );
}

// ---------- set_starred ----------

#[tokio::test]
async fn set_starred_keyword_shape_flips_dollar_flagged() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape {
        starred: StarredFlagShape::Keyword,
        ..ConvenienceShape::default()
    });
    account.set_starred(target(), true).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![format!("set_keyword:{:?}:$flagged:true", target())]
    );
}

#[tokio::test]
async fn set_starred_label_shape_flips_the_starred_label() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape {
        starred: StarredFlagShape::LabelMembership,
        ..ConvenienceShape::default()
    });
    account.set_starred(target(), false).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![format!("set_label_membership:{:?}:STARRED:false", target())]
    );
}

#[tokio::test]
async fn set_starred_category_shape_sends_the_dollar_flagged_sentinel() {
    // Graph's `set_category` special-cases the "$flagged" category
    // name (its `is_starred_category`) into a `flag.flagStatus` PATCH.
    // The convenience side of that contract is that Category dispatch
    // sends exactly "$flagged" - change it and Graph starts writing a
    // literal category named "$flagged" onto messages.
    let account = RecorderAccount::with_conveniences(ConvenienceShape {
        starred: StarredFlagShape::Category,
        ..ConvenienceShape::default()
    });
    account.set_starred(target(), true).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![format!("set_category:{:?}:$flagged:true", target())]
    );
}

#[tokio::test]
async fn set_starred_none_shape_is_unsupported() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let err = account.set_starred(target(), true).await.expect_err("err");
    assert_unsupported(&err, AccountOperation::SetStarred);
    assert!(account.calls().is_empty(), "no primitive may fire");
}

// ---------- mark_replied / mark_forwarded / mark_mdn_sent ----------

#[tokio::test]
async fn mark_replied_keyword_path_flips_dollar_answered() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape {
        replied_via_keyword: true,
        ..ConvenienceShape::default()
    });
    account
        .mark_replied(ObjectId("m-1".into()))
        .await
        .expect("ok");
    assert_eq!(
        account.calls(),
        vec![format!("set_keyword:{:?}:$answered:true", target())]
    );
}

#[tokio::test]
async fn mark_replied_extended_property_path_writes_last_verb_102() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape {
        replied_via_extended_property: true,
        ..ConvenienceShape::default()
    });
    account
        .mark_replied(ObjectId("m-1".into()))
        .await
        .expect("ok");
    assert_eq!(
        account.calls(),
        vec![format!(
            "set_extended_property:{:?}:PR_LAST_VERB_EXECUTED:Some(\"102\")",
            target()
        )]
    );
}

#[tokio::test]
async fn mark_replied_prefers_keyword_when_both_flags_are_set() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape {
        replied_via_keyword: true,
        replied_via_extended_property: true,
        ..ConvenienceShape::default()
    });
    account
        .mark_replied(ObjectId("m-1".into()))
        .await
        .expect("ok");
    let calls = account.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].starts_with("set_keyword:"));
}

#[tokio::test]
async fn mark_replied_with_no_shape_is_unsupported() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let err = account
        .mark_replied(ObjectId("m-1".into()))
        .await
        .expect_err("err");
    assert_unsupported(&err, AccountOperation::MarkReplied);
}

#[tokio::test]
async fn mark_forwarded_paths_mirror_replied_with_forwarded_values() {
    let keyword_account = RecorderAccount::with_conveniences(ConvenienceShape {
        forwarded_via_keyword: true,
        ..ConvenienceShape::default()
    });
    keyword_account
        .mark_forwarded(ObjectId("m-1".into()))
        .await
        .expect("ok");
    assert_eq!(
        keyword_account.calls(),
        vec![format!("set_keyword:{:?}:$forwarded:true", target())]
    );

    let disabled = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let err = disabled
        .mark_forwarded(ObjectId("m-1".into()))
        .await
        .expect_err("err");
    assert_unsupported(&err, AccountOperation::MarkForwarded);

    let ext_account = RecorderAccount::with_conveniences(ConvenienceShape {
        forwarded_via_extended_property: true,
        ..ConvenienceShape::default()
    });
    ext_account
        .mark_forwarded(ObjectId("m-1".into()))
        .await
        .expect("ok");
    assert_eq!(
        ext_account.calls(),
        vec![format!(
            "set_extended_property:{:?}:PR_LAST_VERB_EXECUTED:Some(\"104\")",
            target()
        )]
    );
}

#[tokio::test]
async fn mark_mdn_sent_flips_the_mdnsent_keyword_or_refuses() {
    let keyword_account = RecorderAccount::with_conveniences(ConvenienceShape {
        mdn_sent_via_keyword: true,
        ..ConvenienceShape::default()
    });
    keyword_account
        .mark_mdn_sent(ObjectId("m-1".into()))
        .await
        .expect("ok");
    assert_eq!(
        keyword_account.calls(),
        vec![format!("set_keyword:{:?}:$MDNSent:true", target())]
    );

    let disabled = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let err = disabled
        .mark_mdn_sent(ObjectId("m-1".into()))
        .await
        .expect_err("err");
    assert_unsupported(&err, AccountOperation::MarkMdnSent);
}

// ---------- apply_label / remove_label provenance matrix ----------

#[tokio::test]
async fn gmail_label_dispatches_to_label_membership_with_engine_id() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let l = label(
        ProtocolKind::Gmail,
        ContainerKind::Label,
        "Label_42",
        "Label_42_native",
    );
    account.apply_label(target(), l.clone()).await.expect("ok");
    account.remove_label(target(), l).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![
            // Gmail flips membership by the ENGINE id, not the native.
            format!("set_label_membership:{:?}:Label_42:true", target()),
            format!("set_label_membership:{:?}:Label_42:false", target()),
        ]
    );
}

#[tokio::test]
async fn graph_label_and_folder_both_dispatch_to_category_with_native_id() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let as_label = label(
        ProtocolKind::Graph,
        ContainerKind::Label,
        "cat-1",
        "Orange category",
    );
    let as_folder = label(
        ProtocolKind::Graph,
        ContainerKind::Folder,
        "cat-2",
        "Blue category",
    );
    account.apply_label(target(), as_label).await.expect("ok");
    account.apply_label(target(), as_folder).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![
            format!("set_category:{:?}:Orange category:true", target()),
            format!("set_category:{:?}:Blue category:true", target()),
        ]
    );
}

#[tokio::test]
async fn keyword_protocol_label_dispatches_to_set_keyword_with_native() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let l = label(
        ProtocolKind::Jmap,
        ContainerKind::Label,
        "kw-1",
        "$important",
    );
    account.apply_label(target(), l).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![format!("set_keyword:{:?}:$important:true", target())]
    );
}

#[tokio::test]
async fn folder_label_applies_via_container_membership() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let l = label(
        ProtocolKind::Imap,
        ContainerKind::Folder,
        "Archive",
        "Archive",
    );
    account.apply_label(target(), l.clone()).await.expect("ok");
    account.remove_label(target(), l).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![
            format!("add_to_container:{:?}:Archive", target()),
            format!("remove_from_container:{:?}:Archive", target()),
        ]
    );
}

// ---------- misc conveniences and defaults ----------

#[tokio::test]
async fn set_read_is_an_alias_for_set_is_read() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    account.set_read(target(), true).await.expect("ok");
    assert_eq!(
        account.calls(),
        vec![format!("set_is_read:{:?}:true", target())]
    );
}

#[tokio::test]
async fn multi_call_conveniences_default_to_unsupported() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let err = account
        .move_thread(ThreadId("t-1".into()), ContainerId("c".into()), None)
        .await
        .expect_err("move_thread default");
    assert_unsupported(&err, AccountOperation::MoveThread);

    let err = account
        .delete_thread(ThreadId("t-1".into()), None)
        .await
        .expect_err("delete_thread default");
    assert_unsupported(&err, AccountOperation::DeleteThread);

    let err = account
        .send_raw_message(Bytes::from_static(b"MIME-Version: 1.0\r\n"), None)
        .await
        .expect_err("send_raw_message default");
    assert_unsupported(&err, AccountOperation::Send);
    assert!(account.calls().is_empty());
}

#[tokio::test]
async fn contact_autocomplete_threads_the_limit_and_unwraps_the_page() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let items = account
        .contact_autocomplete("ann".into(), 7)
        .await
        .expect("ok");
    assert!(items.is_empty());
    assert_eq!(
        account.calls(),
        vec!["contact_search:ann:Some(7)".to_string()]
    );
}

#[tokio::test]
async fn event_autocomplete_threads_the_limit_and_unwraps_the_page() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());
    let items = account
        .event_autocomplete("standup".into(), 3)
        .await
        .expect("ok");
    assert!(items.is_empty());
    assert_eq!(
        account.calls(),
        vec!["event_search:standup:Some(3)".to_string()]
    );
}

#[tokio::test]
async fn default_partition_stream_serves_full_and_rejects_the_rest() {
    let account = RecorderAccount::with_conveniences(ConvenienceShape::default());

    // Full delegates to inventory_stream.
    let events: Vec<_> = account
        .inventory_partition_stream(CursorScope::Account, InventoryPartition::Full)
        .collect()
        .await;
    assert!(matches!(
        events.as_slice(),
        [InventoryEvent::Done(completion)]
            if completion.checkpoint == bifrost_types::PageCheckpoint::None
    ));
    assert_eq!(account.calls(), vec!["inventory_stream".to_string()]);

    // A partition shape the account never advertised terminates with
    // Unsupported(SyncInventory) and closes without a contradictory Done.
    let events: Vec<_> = account
        .inventory_partition_stream(
            CursorScope::Account,
            InventoryPartition::Page { from: 0, to: 100 },
        )
        .collect()
        .await;
    assert_eq!(events.len(), 1);
    let InventoryEvent::Terminated(err) = &events[0] else {
        panic!("expected Terminated first, got {:?}", events[0]);
    };
    assert_unsupported(err, AccountOperation::SyncInventory);
}
