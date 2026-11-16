use super::*;
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountFuture, AccountId,
    AccountStream, AttachmentHandle, Batch, BlobHandle, ByteRange, Change, ChangeCursor,
    CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch, ContactSearchRequest,
    ContainerId, ContainerKind, CursorDescriptor, CursorEstablishment, CursorScope, DraftHandle,
    DraftPatch, EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation,
    FlagOp, HostedAttachment, HydratedObject, HydratedObjectKind, HydrationProjection, Identity,
    IdentityId, IdentityPatch, Importance, InventoryEvent, ItemOutcome, MembershipScope, Message,
    MutationSuccess, MutationTarget, ObjectId, Page, PageBoundary, PrimaryCause, Priority,
    Projection, QuotaInfo, RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendReceipt,
    SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch,
    SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId, VacationConfig,
};
use bifrost_types::{AddressBook, AddressBookId};
use bifrost_types::{Calendar, CalendarEvent};
use bytes::Bytes;
struct ScriptAccount {
    events: std::sync::Mutex<Option<Vec<SyncEvent<ItemOutcome<HydratedObject>>>>>,
}
#[allow(unused_variables)]
impl Account for ScriptAccount {
    fn protocol(&self) -> bifrost_types::Protocol {
        bifrost_types::Protocol::Unknown
    }

    fn capabilities(&self) -> &AccountCapabilities {
        unreachable!("capabilities unused")
    }
    fn set_priority(&self, _priority: Priority) {
        unreachable!("unexpected account door")
    }
    fn set_bandwidth_cap(&self, _bps: Option<bifrost_types::BandwidthCap>) {
        unreachable!("unexpected account door")
    }
    fn throttle_keys(
        &self,
        _account: &AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        unreachable!("unexpected account door")
    }
    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("unexpected account door")
    }
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        unreachable!("unexpected account door")
    }
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        unreachable!("unexpected account door")
    }
    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        unreachable!("unexpected account door")
    }
    fn establish_initial_cursor(
        &self,
        _scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<InventoryEvent> {
        unreachable!("unexpected account door")
    }
    fn flag_target(
        &self,
        target: &ObjectId,
        observed: &bifrost_types::FlagSet,
        op: &FlagOp,
    ) -> bifrost_types::FlagTargetState {
        assert_ne!(
            target.0, "unrequested",
            "unrequested ids must not reach the judge"
        );
        bifrost_types::flag_target_state(observed, op)
    }
    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        Box::pin(stream::iter(self.events.lock().unwrap().take().unwrap()))
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        unreachable!("unexpected account door")
    }
    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
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
        unreachable!("unexpected account door")
    }
    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        unreachable!("unexpected account door")
    }
    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        unreachable!("unexpected account door")
    }
    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        _op: FlagOp,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unreachable!("unexpected account door")
    }
    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        destination: MembershipScope,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unreachable!("unexpected account door")
    }
    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unreachable!("unexpected account door")
    }
    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: bifrost_types::Flag,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn send_message(
        &self,
        _request: SendRequest,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<SendReceipt, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn directory_groups_list(
        &self,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        unreachable!("unexpected account door")
    }
    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        unreachable!("unexpected account door")
    }
    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        unreachable!("unexpected account door")
    }
}

fn success(id: &str, output_id: &str, flags: &[&str]) -> ItemOutcome<HydratedObject> {
    ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
        bifrost_types::BatchItemId(id.into()),
        HydratedObject {
            id: ObjectId(output_id.into()),
            kind: HydratedObjectKind::FlagsOnly(flags.iter().map(|s| (*s).to_owned()).collect()),
            blobs: Vec::new(),
        },
    ))
}
fn scripted(items: Vec<ItemOutcome<HydratedObject>>) -> ScriptAccount {
    ScriptAccount {
        events: std::sync::Mutex::new(Some(vec![SyncEvent::Batch(Batch {
            items,
            page_boundary: PageBoundary::Final,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: None,
        })])),
    }
}
async fn observe_items(items: Vec<ItemOutcome<HydratedObject>>, ids: &[&str]) -> ReadbackReport {
    let account = scripted(items);
    let op = FlagOp::Add(["\\Seen".to_owned()].into_iter().collect());
    run_object_readback(
        &account,
        ids.iter().map(|id| ObjectId((*id).into())).collect(),
        ReadbackTarget::Flags(&op),
        None,
    )
    .await
}
#[tokio::test]
async fn wrong_projection_is_unresolved_for_flags_and_move() {
    let account = scripted(vec![success("a", "a", &["\\Seen"])]);
    let destination = MembershipScope::Folder(bifrost_types::FolderId("f".into()));
    let report = run_object_readback(
        &account,
        vec![ObjectId("a".into())],
        ReadbackTarget::Move(&destination),
        None,
    )
    .await;
    assert_eq!(
        report.objects[&ObjectId("a".into())],
        ObjectReadback::Unresolved
    );
    let mut item = success("a", "a", &[]);
    if let ItemOutcome::Succeeded(s) = &mut item {
        s.output.kind = HydratedObjectKind::RawMime(Bytes::new());
    }
    let report = observe_items(vec![item], &["a"]).await;
    assert_eq!(
        report.objects[&ObjectId("a".into())],
        ObjectReadback::Unresolved
    );
}
#[tokio::test]
async fn a_hydrated_id_that_disagrees_with_its_envelope_is_unresolved() {
    let report = observe_items(
        vec![
            success("unrequested", "unrequested", &["\\Seen"]),
            success("a", "b", &["\\Seen"]),
        ],
        &["a"],
    )
    .await;
    assert_eq!(
        report.objects[&ObjectId("a".into())],
        ObjectReadback::Unresolved
    );
}
#[tokio::test]
async fn disagreeing_duplicate_observations_are_unresolved() {
    let report = observe_items(
        vec![
            success("a", "a", &["\\Seen"]),
            success("a", "a", &[]),
            success("a", "a", &["\\Seen"]),
            success("b", "b", &["\\Seen"]),
            success("c", "c", &["\\Seen"]),
            success("c", "c", &["\\Seen"]),
        ],
        &["a", "b", "c"],
    )
    .await;
    assert_eq!(
        report.objects[&ObjectId("a".into())],
        ObjectReadback::Unresolved
    );
    for id in ["b", "c"] {
        assert_eq!(
            report.objects[&ObjectId(id.into())],
            ObjectReadback::Satisfied
        );
    }
}
#[tokio::test]
async fn a_per_item_engine_failure_is_reported_not_absorbed() {
    let error = AccountErrorBuilder::new(PrimaryCause::State(
        bifrost_types::StateCause::SchemaIncompatible,
    ))
    .try_build()
    .unwrap();
    let report = observe_items(
        ["a", "unrequested"]
            .into_iter()
            .map(|id| {
                ItemOutcome::Failed(bifrost_types::BatchFailure::new(
                    bifrost_types::BatchItemId(id.into()),
                    error.clone(),
                ))
            })
            .collect(),
        &["a"],
    )
    .await;
    assert_eq!(report.engine_failures.len(), 2);
    assert_eq!(
        report.objects[&ObjectId("a".into())],
        ObjectReadback::Unresolved
    );
}

#[tokio::test]
async fn destroy_readback_requires_message_not_found() {
    use bifrost_types::{BatchFailure, BatchItemId, RequestCause, ResourceKind};

    let cases = [
        ("message", ResourceKind::Message),
        ("mailbox", ResourceKind::Mailbox),
        ("account", ResourceKind::Account),
        ("thread", ResourceKind::Thread),
        ("calendar", ResourceKind::Calendar),
        ("contact", ResourceKind::Contact),
        ("draft", ResourceKind::Draft),
        ("identity", ResourceKind::Identity),
        ("vacation", ResourceKind::Vacation),
        ("push", ResourceKind::PushSubscription),
        ("filter", ResourceKind::Filter),
        ("event", ResourceKind::Event),
    ];
    let account = scripted(
        cases
            .iter()
            .map(|(id, kind)| {
                let error =
                    AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::NotFound {
                        what: *kind,
                        id: None,
                    }))
                    .try_build()
                    .expect("valid not-found classification");
                ItemOutcome::Failed(BatchFailure::new(BatchItemId((*id).into()), error))
            })
            .collect(),
    );
    let outcome = run_destroy_readback_guard(
        &account,
        cases.iter().map(|(id, _)| ObjectId((*id).into())).collect(),
    )
    .await
    .expect("per-item failures do not terminate read-back");

    assert_eq!(outcome.skipped, 1);
    assert_eq!(outcome.still_failed, 0);
    assert_eq!(outcome.uncertain, (cases.len() - 1) as u64);
}

#[tokio::test]
async fn move_readback_bridges_native_mailbox_membership_but_never_owner_tags() {
    let folder = MembershipScope::Folder(bifrost_types::FolderId("a".into()));
    let owner = MembershipScope::Owner(bifrost_types::PrincipalId("a".into()));
    for (destination, observed, expected) in [
        (
            folder.clone(),
            MembershipScope::Mailbox(bifrost_types::MailboxId("a".into())),
            ObjectReadback::Satisfied,
        ),
        (folder, owner.clone(), ObjectReadback::Unsatisfied),
        (owner.clone(), owner, ObjectReadback::Unsatisfied),
    ] {
        let item = ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
            bifrost_types::BatchItemId("message".into()),
            HydratedObject {
                id: ObjectId("message".into()),
                kind: HydratedObjectKind::Metadata(bifrost_types::InventoryEntry {
                    id: ObjectId("message".into()),
                    memberships: vec![observed],
                    blob_id: None,
                    fingerprint: bifrost_types::Fingerprint {
                        size: None,
                        flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
                        server_version: bifrost_types::ServerVersion::Unavailable,
                    },
                    thread_id: None,
                    message_id: None,
                    references: Vec::new(),
                    in_reply_to: None,
                }),
                blobs: Vec::new(),
            },
        ));
        let report = run_object_readback(
            &scripted(vec![item]),
            vec![ObjectId("message".into())],
            ReadbackTarget::Move(&destination),
            None,
        )
        .await;
        assert_eq!(report.objects[&ObjectId("message".into())], expected);
    }
}
