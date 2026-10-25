//! Attach-time recovery from an undecodable durable cursor.
//!
//! `handle_schema_incompatible` - the account-wide schema-clear loop -
//! runs on the reopen listener, and the reopen listener only exists
//! once `attach_inner` has finished spawning workers. A cursor that
//! fails to decode during attach therefore has no listener to route to:
//! if `establish_one` propagated the error, attach would fail, nothing
//! would be left running to clear the row, and every subsequent attach
//! would fail identically. The account would be permanently stranded by
//! one unreadable row.
//!
//! Attach heals by clearing every checkpoint of the account with its ledger
//! preserved, then establishes every scope fresh.
//!
//! On the local `HealAccount` double below: the reusable `StubAccount` seam
//! now lives in `tests/common/mod.rs` and is the model for NEW tests. This
//! file predates it and keeps its own purpose-built double. Migrating it is
//! optional cleanup that is explicitly not owed - the double works, it is
//! shaped tightly around this file's scripted decode failures, and a
//! migration would be churn in a test whose whole value is that it already
//! pins the stranding bug. Write new tests against `common::StubAccount`;
//! leave this one alone unless it needs to change for its own reasons.

mod common;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bifrost_sync::{
    CheckpointStore, CheckpointTransition, DebtLedger, Error, InMemoryCheckpointStore, SyncEngine,
};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFactory, AccountFuture, AccountId, AccountStream, AttachmentHandle, Batch,
    BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Cause, Change, ChangeCursor,
    CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch, ContactSearchRequest,
    ContainerId, ContainerKind, Control, ConvenienceShape, CursorDescriptor, CursorEstablishment,
    CursorFreshness, CursorScope, DraftHandle, DraftPatch, ErrorScope, EventCreate, EventId,
    EventPatch, EventRange, EventSearchRequest, FilterRuleShape, FilterValidation, FlagOp,
    HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, Importance, InventoryEvent, ItemOutcome, MembershipScope, Message,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, MutationSuccess,
    MutationTarget, ObjectId, OpaqueChangeState, Page, PageBoundary, PimMethodSupport, Priority,
    Projection, ProtocolKind, PushCapability, QuotaInfo, QuotaSignal, RateLimitClass, RequestCause,
    RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId,
    VacationConfig,
};
use bifrost_types::{AddressBook, AddressBookId};
use bifrost_types::{BackfillCheckpoint, Checkpoint};
use bifrost_types::{Calendar, CalendarEvent};
use bytes::Bytes;
use futures::stream;
use std::sync::atomic::AtomicUsize as BoundaryCounter;

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
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

fn cursor_for(scope: &CursorScope, state: &[u8]) -> ChangeCursor {
    ChangeCursor {
        scope: scope.clone(),
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: 1,
            bytes: state.to_vec(),
        },
        advanced_through: None,
        envelope_version: 1,
    }
}

/// A store whose `get_change_cursor` reports the row as undecodable -
/// what a consumer store returns when `decode_envelope` hits a schema
/// it cannot read - until the engine deletes the row or clears the account.
#[derive(Default)]
struct PoisonedStore {
    inner: InMemoryCheckpointStore,
    /// Scopes still reporting `SchemaIncompatible`.
    poisoned: Mutex<Vec<CursorScope>>,
    /// Scopes the engine asked to delete one by one, in call order.
    deleted: Mutex<Vec<CursorScope>>,
    fail_reads: bool,
    /// Account-wide clears the engine ran.
    clears: std::sync::atomic::AtomicUsize,
}

impl PoisonedStore {
    fn with_poisoned(scopes: Vec<CursorScope>) -> Self {
        Self {
            poisoned: Mutex::new(scopes),
            ..Default::default()
        }
    }

    fn deleted(&self) -> Vec<CursorScope> {
        self.deleted.lock().expect("deleted lock").clone()
    }

    fn with_read_failure() -> Self {
        Self {
            fail_reads: true,
            ..Default::default()
        }
    }
}

impl CheckpointStore for PoisonedStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            self.inner
                .clear_account_checkpoints(account, ledger)
                .await?;
            self.clears
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.poisoned.lock().expect("poisoned").clear();
            Ok(())
        })
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.apply_transition(account, transition)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DebtLedger, Error>> + Send + 'a>> {
        self.inner.get_ledger(account)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>
    {
        if self.fail_reads {
            return Box::pin(async {
                Err(Error::CheckpointStore(
                    "forced attach-time read failure".into(),
                ))
            });
        }
        let still_poisoned = self
            .poisoned
            .lock()
            .expect("poisoned lock")
            .iter()
            .any(|s| s == scope);
        if still_poisoned {
            return Box::pin(async { Err(Error::SchemaIncompatible) });
        }
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.deleted
            .lock()
            .expect("deleted lock")
            .push(scope.clone());
        // Clearing the row clears the poison: a re-read now behaves
        // like any other absent cursor.
        self.poisoned
            .lock()
            .expect("poisoned lock")
            .retain(|s| s != scope);
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

/// Account that discovers a fixed scope list and establishes a cursor
/// for each, recording which scopes it was asked to establish.
type SubscriptionCalls = Arc<Mutex<Vec<(usize, Vec<CursorScope>)>>>;

struct HealAccount {
    caps: AccountCapabilities,
    scopes: Vec<CursorScope>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
    generation: usize,
    closed_generations: Arc<Mutex<Vec<usize>>>,
    subscribed: SubscriptionCalls,
    unsubscribed: Arc<Mutex<Vec<(usize, SubscriptionHandle)>>>,
    unsubscribe_failures: Arc<AtomicUsize>,
    lifecycle_calls: Arc<Mutex<Vec<usize>>>,
    /// Events the next `scope_lifecycle_stream` call yields before the
    /// stream ends. Empty means every stream ends immediately, which is
    /// the shape most tests want.
    lifecycle_script: Arc<Mutex<VecDeque<ScopeLifecycleEvent>>>,
    /// When set, `contacts_list` answers with a page carrying both loss
    /// lanes populated. Off by default so the stub stays neutral for
    /// every test that does not care.
    degraded_contacts: bool,
    /// When set, `close()` announces that it has been entered and then
    /// parks until released, holding `detach` open inside its teardown
    /// window. `None` for every test that does not stage that window.
    close_gate: Arc<Mutex<Option<CloseGate>>>,
    /// When `Some`, `changes_stream` emits an ENDLESS run of batches that
    /// violate the boundary contract (`PageBoundary::Partial` carrying a
    /// checkpoint), counting each one it hands out. Endless is the point:
    /// a driver that merely logs the violation and re-polls would consume
    /// them without bound, so the count is the spin detector.
    bad_boundary_batches: Option<Arc<AtomicUsize>>,
    /// Emit ONE well-formed `Final` batch carrying a change checkpoint.
    live_checkpoint_batch: bool,
}

/// Test handle for pinning `detach` inside `Account::close()`.
struct CloseGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

fn no_close_gate() -> Arc<Mutex<Option<CloseGate>>> {
    Arc::new(Mutex::new(None))
}

struct HealFactory {
    scopes: Vec<CursorScope>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
    degraded_contacts: bool,
    /// Handed to every account this factory opens; the first `close()`
    /// consumes it.
    close_gate: Arc<Mutex<Option<CloseGate>>>,
}

impl HealFactory {
    fn new(
        scopes: Vec<CursorScope>,
        established: Arc<Mutex<Vec<CursorScope>>>,
        closed: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            scopes,
            established,
            closed,
            degraded_contacts: false,
            close_gate: no_close_gate(),
        }
    }

    /// Opens accounts whose first `close()` parks until the test
    /// releases it, holding `detach` inside its teardown window.
    fn with_close_gate(
        scopes: Vec<CursorScope>,
        established: Arc<Mutex<Vec<CursorScope>>>,
        closed: Arc<AtomicUsize>,
        gate: CloseGate,
    ) -> Self {
        Self {
            close_gate: Arc::new(Mutex::new(Some(gate))),
            ..Self::new(scopes, established, closed)
        }
    }

    /// Opens accounts whose `contacts_list` returns a page that lost
    /// resources and skipped a scope.
    fn with_degraded_contacts(
        scopes: Vec<CursorScope>,
        established: Arc<Mutex<Vec<CursorScope>>>,
        closed: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            degraded_contacts: true,
            ..Self::new(scopes, established, closed)
        }
    }
}

impl AccountFactory for HealFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let scopes = self.scopes.clone();
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        let degraded_contacts = self.degraded_contacts;
        let close_gate = Arc::clone(&self.close_gate);
        Box::pin(async move {
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: caps(),
                scopes,
                established,
                closed,
                generation: 0,
                closed_generations: Arc::new(Mutex::new(Vec::new())),
                subscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                degraded_contacts,
                close_gate: Arc::clone(&close_gate),
                bad_boundary_batches: None,
                live_checkpoint_batch: false,
            });
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

struct RotatingFactory {
    scopes: Mutex<VecDeque<Vec<CursorScope>>>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
    closed_generations: Arc<Mutex<Vec<usize>>>,
    subscribed: SubscriptionCalls,
    unsubscribed: Arc<Mutex<Vec<(usize, SubscriptionHandle)>>>,
    unsubscribe_failures: Arc<AtomicUsize>,
    lifecycle_calls: Arc<Mutex<Vec<usize>>>,
    opens: AtomicUsize,
}

impl AccountFactory for RotatingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let scopes = self
            .scopes
            .lock()
            .expect("scopes lock")
            .pop_front()
            .expect("one scope set per expected open");
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        let closed_generations = Arc::clone(&self.closed_generations);
        let subscribed = Arc::clone(&self.subscribed);
        let unsubscribed = Arc::clone(&self.unsubscribed);
        let unsubscribe_failures = Arc::clone(&self.unsubscribe_failures);
        let lifecycle_calls = Arc::clone(&self.lifecycle_calls);
        Box::pin(async move {
            let mut account_caps = caps();
            account_caps.push = if generation == 0 {
                PushCapability::None
            } else {
                PushCapability::InProcess
            };
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: account_caps,
                scopes,
                established,
                closed,
                generation,
                closed_generations,
                subscribed,
                unsubscribed,
                unsubscribe_failures,
                lifecycle_calls,
                lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                degraded_contacts: false,
                close_gate: no_close_gate(),
                bad_boundary_batches: None,
                live_checkpoint_batch: false,
            });
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

impl Account for HealAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by attach recovery tests")
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        let items: Vec<SyncEvent<CursorScope>> = vec![
            SyncEvent::Batch(Batch {
                items: self.scopes.clone(),
                page_boundary: bifrost_types::PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ];
        Box::pin(stream::iter(items))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        Box::pin(stream::empty())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        self.lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .push(self.generation);
        let scripted: Vec<ScopeLifecycleEvent> = self
            .lifecycle_script
            .lock()
            .expect("lifecycle script lock")
            .drain(..)
            .collect();
        Box::pin(stream::iter(scripted))
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        self.established
            .lock()
            .expect("established lock")
            .push(scope.clone());
        Box::pin(async move {
            Ok(CursorEstablishment::Ready(cursor_for(
                &scope,
                b"freshly-established",
            )))
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
        Box::pin(stream::empty())
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        if self.live_checkpoint_batch {
            let items = vec![SyncEvent::Batch(Batch {
                items: Vec::new(),
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: Some(Checkpoint::Change(cursor)),
            })];
            return Box::pin(stream::iter(items));
        }
        let Some(counter) = self.bad_boundary_batches.clone() else {
            return Box::pin(stream::empty());
        };
        Box::pin(stream::repeat_with(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            SyncEvent::Batch(Batch {
                items: Vec::new(),
                page_boundary: PageBoundary::Partial,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: Some(Checkpoint::Change(cursor.clone())),
            })
        }))
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        self.subscribed
            .lock()
            .expect("subscribed lock")
            .push((self.generation, scopes.to_vec()));
        let handle = SubscriptionHandle(format!("generation-{}", self.generation));
        let scopes = scopes.to_vec();
        Box::pin(async move {
            Ok(
                bifrost_types::PushSubscription::all_succeeded(handle, &scopes)
                    .expect("test push subscription covers every requested scope"),
            )
        })
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.unsubscribed
            .lock()
            .expect("unsubscribed lock")
            .push((self.generation, handle));
        let mut remaining = self.unsubscribe_failures.load(Ordering::SeqCst);
        let fail = loop {
            if remaining == 0 {
                break false;
            }
            match self.unsubscribe_failures.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break true,
                Err(actual) => remaining = actual,
            }
        };
        Box::pin(async move {
            if fail {
                Err(unsupported(
                    bifrost_types::AccountOperation::PushUnsubscribe,
                ))
            } else {
                Ok(())
            }
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
        self.closed.fetch_add(1, Ordering::SeqCst);
        self.closed_generations
            .lock()
            .expect("closed generations lock")
            .push(self.generation);
        let gate = self.close_gate.lock().expect("close gate lock").take();
        Box::pin(async move {
            if let Some(gate) = gate {
                let _ = gate.entered.send(());
                let _ = gate.release.await;
            }
            Ok(())
        })
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
        if !self.degraded_contacts {
            // A successful page with both lanes empty - the shape that
            // must stay silent. An `Err` here would exercise the `?`
            // instead of the emptiness guard.
            return Box::pin(async { Ok(Page::single(Vec::new(), Vec::new(), Vec::new())) });
        }
        // A walk that returned some cards, could not materialize one
        // resource, and gave up on a whole address book part way
        // through - the DAV shape xc-5 was filed from.
        Box::pin(async {
            Ok(Page {
                truncated: false,
                items: Vec::new(),
                next_cursor: None,
                estimated_total: None,
                failed_ids: vec!["urn:uuid:unparseable-vcard".to_owned()],
                skipped_scopes: vec![bifrost_types::SkippedScope {
                    scope: ErrorScope::Mailbox {
                        id: "shared-book".into(),
                    },
                    error: unsupported(bifrost_types::AccountOperation::ContactsList),
                }],
            })
        })
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

/// xc-5. Both page loss lanes ride out in the returned `Page`, so this
/// warning reveals no new DATA - it closes an asymmetry. An open-time
/// skip is announced (`open_skipped_scopes` plus a log line) while a
/// page-time skip was entirely silent, so a consumer had to already know
/// to look at the lanes. The warning gives them the reason to look; the
/// `Page` in hand stays the actionable copy.
#[tokio::test]
async fn a_forwarded_page_with_a_loss_lane_warns_on_the_change_stream() {
    let account_id = AccountId("page-loss".to_owned());
    let scope = CursorScope::Account;
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::with_degraded_contacts(
        vec![scope],
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
    ));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let page = engine
        .contacts_list(&account_id, None, None)
        .await
        .expect("a degraded page is still a successful call");

    // The lanes reach the caller untouched - the warning is additive.
    assert_eq!(page.failed_ids.len(), 1);
    assert_eq!(page.skipped_scopes.len(), 1);

    let warning = loop {
        let event = events.recv().await.expect("the warning is broadcast");
        if let SyncEvent::Warning(warning) = event.event.as_ref() {
            break warning.clone();
        }
    };
    assert_eq!(
        warning.kind,
        bifrost_types::WarningKind::OperatorAttentionNeeded
    );
    let text = warning.message.value.clone();
    assert!(text.contains("contacts_list"), "{text}");
    assert!(text.contains('1'), "the counts ride in the message: {text}");
    // `failed_ids` holds native provider identifiers, which are not
    // user-safe text. The message counts them and never names them.
    assert!(!text.contains("urn:uuid"), "{text}");
    assert!(
        warning.next_action.is_some(),
        "the warning must point at the lanes that carry the detail"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The complement: a clean page must stay silent, or the warning is
/// noise on every successful call and consumers learn to ignore it.
#[tokio::test]
async fn a_clean_forwarded_page_emits_no_warning() {
    let account_id = AccountId("page-clean".to_owned());
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
    ));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let page = engine
        .contacts_list(&account_id, None, None)
        .await
        .expect("the clean page succeeds");
    assert!(page.failed_ids.is_empty() && page.skipped_scopes.is_empty());

    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "a call with no page-lane loss must not warn"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

#[tokio::test]
async fn attach_heals_an_undecodable_cursor_instead_of_failing() {
    let account_id = AccountId("schema-heal".to_owned());
    let scope = CursorScope::Account;
    let store = Arc::new(PoisonedStore::with_poisoned(vec![scope.clone()]));
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));

    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![scope.clone()],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("an undecodable cursor must not fail the attach");

    assert_eq!(
        store.clears.load(Ordering::SeqCst),
        1,
        "the unreadable row must be cleared, otherwise the next attach hits it again"
    );
    assert_eq!(
        *established.lock().expect("established lock"),
        vec![scope.clone()],
        "the scope must be re-established from the live account"
    );
    let persisted = store
        .get_change_cursor(&account_id, &scope)
        .await
        .expect("the healed row decodes")
        .expect("re-establishment persists a replacement cursor");
    assert_eq!(
        persisted.server_state.bytes,
        b"freshly-established".to_vec()
    );

    engine.detach(&account_id).await.expect("detach succeeds");
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

/// One unreadable row clears every durable row of the account at attach: the
/// readable sibling's cursor and completion marker, and the rows of a scope
/// discovery never names. The ledger survives byte for byte, old object id
/// included; `HealFactory`'s idle changes stream means no accepted coverage can
/// discharge it during the assertion.
///
/// Ablations: restore the per-scope `delete_change_cursor` heal (no clear, the
/// sibling resumes its stored cursor and is not established); pass
/// `DebtLedger::default()` from attach to the clear (the ledger bytes differ).
#[tokio::test]
async fn one_undecodable_scope_clears_every_row_of_the_account_at_attach() {
    let account_id = AccountId("schema-heal-sibling".to_owned());
    let poisoned = CursorScope::Type(bifrost_types::ObjectType::Email);
    let healthy = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
    let store = Arc::new(PoisonedStore::with_poisoned(vec![poisoned.clone()]));
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));

    let absent = CursorScope::Type(bifrost_types::ObjectType::Contact);
    for scope in [&healthy, &absent] {
        store
            .put_change_cursor(&account_id, cursor_for(scope, b"old"))
            .await
            .expect("seed");
        store
            .put_backfill(
                &account_id,
                BackfillCheckpoint {
                    scope: scope.clone(),
                    partition: bifrost_sync::backfill::partitioner::completion_partition(),
                    progress_marker: None,
                    progress: Default::default(),
                    envelope_version: 1,
                },
            )
            .await
            .expect("seed marker");
    }
    let mut ledger = DebtLedger::default();
    let report = bifrost_types::InventoryCoverageReport::degraded(
        bifrost_types::CoverageDomain::full(healthy.clone()),
        vec![bifrost_types::InventoryObligation::Object {
            key: bifrost_types::ObligationKey(b"old-id".to_vec()),
            id: ObjectId("old-encoding-id".into()),
            error: unsupported(bifrost_types::AccountOperation::SyncInventory),
            repair: Vec::new(),
        }],
    );
    ledger.ingest_debt_only(&report, 1, 1);
    let ledger_bytes = bifrost_sync::cursor::encode_ledger(&ledger);
    store
        .put_ledger(&account_id, ledger)
        .await
        .expect("seed ledger");
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![poisoned.clone(), healthy.clone()],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    assert_eq!(
        store.clears.load(Ordering::SeqCst),
        1,
        "an unreadable scope clears the account"
    );
    assert!(
        store.deleted().is_empty(),
        "attach must not heal an unreadable row per scope"
    );
    for scope in [&healthy, &absent] {
        assert!(
            store
                .get_backfill(&account_id, scope)
                .await
                .expect("read")
                .is_none()
        );
    }
    assert!(
        store
            .get_change_cursor(&account_id, &absent)
            .await
            .expect("read")
            .is_none()
    );
    assert_eq!(
        bifrost_sync::cursor::encode_ledger(&store.get_ledger(&account_id).await.expect("ledger")),
        ledger_bytes
    );
    // Both scopes establish fresh, including the formerly readable sibling.
    assert_eq!(
        *established.lock().expect("established lock"),
        vec![poisoned, healthy]
    );

    engine.detach(&account_id).await.expect("detach succeeds");
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

/// `detach` removes the slot up front but does its registry cleanup at
/// the very tail - after awaiting workers (up to `detach_timeout`) and
/// `Account::close()`. An `attach` landing inside that window used to
/// see no slot and no in-flight entry, so it succeeded and installed
/// fresh registrations; the still-running detach then unregistered the
/// NEW incarnation from the invalidation sink, the budget gate, the
/// backfill registry, the throttle memberships, and the bandwidth
/// meter. The consumer got an account that reported itself attached and
/// silently dropped every out-of-process push, with nothing logged.
///
/// Rejecting the racing attach is the fix: the id is genuinely still
/// attached and draining, and a caller that gets an error can retry.
#[tokio::test]
async fn attach_cannot_land_inside_an_in_flight_detach() {
    let account_id = AccountId("detach-reattach-race".to_owned());
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let engine = Arc::new(
        SyncEngine::builder()
            .build()
            .expect("default engine config is valid"),
    );

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let gated: Arc<dyn AccountFactory> = Arc::new(HealFactory::with_close_gate(
        vec![CursorScope::Account],
        Arc::clone(&established),
        Arc::clone(&closed),
        CloseGate {
            entered: entered_tx,
            release: release_rx,
        },
    ));
    engine
        .attach(account_id.clone(), Arc::clone(&gated))
        .await
        .expect("attach succeeds");

    // Park the detach inside `Account::close()`, which sits after the
    // slot removal and before every registry cleanup.
    let detaching = {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move { engine.detach(&account_id).await })
    };
    entered_rx.await.expect("detach reaches Account::close");
    assert!(
        !engine.attached_account_ids().contains(&account_id),
        "the slot is already gone; only the lifecycle guard stands between \
         a racing attach and the teardown tail"
    );

    let racing: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));
    let raced = engine.attach(account_id.clone(), racing).await;
    assert!(
        matches!(raced, Err(Error::AccountAlreadyAttached(ref id)) if id == &account_id),
        "an attach inside the teardown window must be refused, not \
         installed and then stripped of its registrations"
    );

    release_tx.send(()).expect("release the gated close");
    detaching
        .await
        .expect("detach task")
        .expect("detach succeeds");

    // The guard releases with the teardown, so the id is reusable.
    let fresh: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        established,
        Arc::clone(&closed),
    ));
    engine
        .attach(account_id.clone(), fresh)
        .await
        .expect("re-attach after the detach completes");
    engine.detach(&account_id).await.expect("detach succeeds");
}

#[tokio::test]
async fn failed_attach_closes_the_opened_account() {
    let account_id = AccountId("attach-failure-close".to_owned());
    let store = Arc::new(PoisonedStore::with_read_failure());
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        established,
        Arc::clone(&closed),
    ));

    let result = engine.attach(account_id.clone(), factory).await;
    assert!(matches!(result, Err(Error::CheckpointStore(_))));
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "every post-open attach failure must close the live handle"
    );
    assert!(
        !engine.attached_account_ids().contains(&account_id),
        "failed attach must not leave a slot behind"
    );
}

#[tokio::test(start_paused = true)]
async fn reattach_refreshes_topology_subscriptions_and_lifecycle_handle() {
    let account_id = AccountId("full-reattach".to_owned());
    let old_scope = CursorScope::Account;
    let new_scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let closed_generations = Arc::new(Mutex::new(Vec::new()));
    let subscribed = Arc::new(Mutex::new(Vec::new()));
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let lifecycle_calls = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![old_scope.clone()],
            vec![new_scope.clone()],
        ])),
        established: Arc::clone(&established),
        closed: Arc::clone(&closed),
        closed_generations: Arc::clone(&closed_generations),
        subscribed: Arc::clone(&subscribed),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::clone(&lifecycle_calls),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("initial attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&old_scope))
        .await
        .expect("initial push subscription");
    engine.reattach(&account_id).await.expect("full reattach");

    assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
    assert_eq!(
        engine
            .account_capabilities(&account_id)
            .expect("capabilities")
            .push,
        PushCapability::InProcess,
        "capability snapshot must come from the replacement handle"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &old_scope)
            .await
            .expect("old cursor lookup")
            .is_none(),
        "vanished scopes must be removed durably"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &new_scope)
            .await
            .expect("new cursor lookup")
            .is_some(),
        "newly discovered scopes must be established"
    );
    assert_eq!(
        *subscribed.lock().expect("subscribed lock"),
        vec![(0, vec![old_scope.clone()])],
        "a vanished requested scope must not widen a replacement subscription"
    );
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![(0, SubscriptionHandle("generation-0".into()))],
        "old subscription must be removed through the old handle"
    );
    assert_eq!(
        *closed_generations.lock().expect("closed generations lock"),
        vec![0],
        "successful reopen closes exactly the old handle"
    );

    // This double's lifecycle stream is empty, so its failed-open count
    // crosses replacement. Allow the earned delay before checking the new
    // handle; healthy streams are separately pinned to reopen immediately.
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    for _ in 0..200 {
        if lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .contains(&1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .contains(&1),
        "lifecycle task must resubscribe to the replacement handle"
    );

    engine.detach(&account_id).await.expect("detach");
    assert_eq!(
        *closed_generations.lock().expect("closed generations lock"),
        vec![0, 1]
    );
}

/// Detach must drop this incarnation's push registry records.
///
/// The registry is per-engine and keyed by `AccountId`, but the handles in
/// it belong to one connection. `detach` used to forget the sink, the
/// budget, the backfill registry, throttles, and the bandwidth meter while
/// leaving `subscriptions` untouched, so re-attaching the same id inherited
/// the previous incarnation's handles and would hand them to the provider
/// as though they were live.
///
/// Observed through `unsubscribed`: after a detach and a fresh attach, a
/// teardown must find nothing to tear down. Nothing here asserts that the
/// SERVER-side subscription was deleted - it deliberately is not, since
/// that stays the consumer's job via `unsubscribe_push` BEFORE detach.
#[tokio::test]
async fn detach_drops_push_records_so_a_reattach_cannot_reuse_dead_handles() {
    let account_id = AccountId("detach-clears-registry".to_owned());
    let scope = CursorScope::Account;
    let subscribed = Arc::new(Mutex::new(Vec::new()));
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([vec![scope.clone()], vec![scope.clone()]])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::clone(&subscribed),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::new(InMemoryCheckpointStore::new()) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), Arc::clone(&factory_trait))
        .await
        .expect("initial attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&scope))
        .await
        .expect("push subscription");
    assert_eq!(
        subscribed.lock().expect("subscribed lock").len(),
        1,
        "the subscription must actually register, or the test proves nothing"
    );

    // Detach WITHOUT unsubscribing - the case the contract warns about.
    engine.detach(&account_id).await.expect("detach");

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("reattach under the same id");
    engine
        .unsubscribe_push(&account_id)
        .await
        .expect("teardown on the fresh incarnation");

    assert!(
        unsubscribed.lock().expect("unsubscribed lock").is_empty(),
        "a reattached account must not inherit the previous incarnation's \
         handles; tearing one down targets a connection that is gone"
    );

    engine.detach(&account_id).await.expect("final detach");
}

/// The opt-in the contract needed: `detach_with_teardown` tears the
/// server-side subscriptions down and then detaches, so a consumer that is
/// done with the account states that once instead of remembering to call
/// `unsubscribe_push` inside a window that closes silently. Plain `detach`
/// is unchanged, which the test above pins. Ablation: with the teardown call
/// removed, `unsubscribed` stays empty.
#[tokio::test]
async fn detach_with_teardown_tears_push_subscriptions_down_before_detaching() {
    let account_id = AccountId("detach-with-teardown".to_owned());
    let scope = CursorScope::Account;
    let subscribed = Arc::new(Mutex::new(Vec::new()));
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([vec![scope.clone()]])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::clone(&subscribed),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::new(InMemoryCheckpointStore::new()) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&scope))
        .await
        .expect("push subscription");
    assert_eq!(subscribed.lock().expect("subscribed lock").len(), 1);

    engine
        .detach_with_teardown(&account_id)
        .await
        .expect("teardown and detach");

    assert_eq!(
        unsubscribed.lock().expect("unsubscribed lock").len(),
        1,
        "the server-side subscription must be torn down by the detach the consumer asked for"
    );
    assert!(
        matches!(
            engine.unsubscribe_push(&account_id).await,
            Err(bifrost_sync::Error::AccountNotAttached(_))
        ),
        "and the account is detached afterwards"
    );
}

#[tokio::test]
async fn reattach_waits_for_resume_without_opening_during_pause() {
    let account_id = AccountId("paused-reopen".to_owned());
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let control = engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach");
    control
        .pause()
        .await
        .expect("idle account pauses immediately");

    let mut reopen = Box::pin(engine.reattach(&account_id));
    tokio::select! {
        result = &mut reopen => panic!("paused reopen completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert_eq!(
        established.lock().expect("established lock").len(),
        1,
        "reopen must not open or establish a replacement while paused"
    );

    // The queued reopen must not retain the subscription-serialization
    // lock while it waits for Run. This is the cleanup ordering consumers
    // use before detach; if the lock is held, this future cannot complete
    // until the resume below and the test deterministically takes the yield
    // arm instead.
    let mut unsubscribe = Box::pin(engine.unsubscribe_push(&account_id));
    tokio::select! {
        biased;
        result = &mut unsubscribe => result.expect("paused reopen leaves push teardown available"),
        () = tokio::task::yield_now() => panic!("push teardown blocked behind a paused reopen"),
    }

    control.resume();
    reopen.await.expect("queued reopen completes after resume");
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "resume releases the queued reattach and swaps the live handle"
    );

    engine.detach(&account_id).await.expect("detach");
}

#[tokio::test]
async fn failed_push_unsubscribe_is_reported_and_the_handle_is_retryable() {
    let account_id = AccountId("retry-push-unsubscribe".to_owned());
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([vec![CursorScope::Account]])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, &[CursorScope::Account])
        .await
        .expect("subscribe");

    assert!(
        matches!(
            engine.unsubscribe_push(&account_id).await,
            Err(Error::Account(_))
        ),
        "a failed account-side teardown must be surfaced to the caller"
    );
    engine
        .unsubscribe_push(&account_id)
        .await
        .expect("retained handle retries successfully");
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![
            (0, SubscriptionHandle("generation-0".into())),
            (0, SubscriptionHandle("generation-0".into())),
        ],
        "the failed handle remains registered for the next teardown call"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Factory that parks inside `open` until the test releases it, so a test can
/// observe the window between "the engine decided to reopen" and "a
/// replacement connection exists". The first open (attach) is never gated.
struct GatingFactory {
    scopes: Vec<CursorScope>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
    opens: AtomicUsize,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl AccountFactory for GatingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        let scopes = self.scopes.clone();
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            if generation > 0 {
                entered.notify_one();
                release.notified().await;
            }
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: caps(),
                scopes,
                established,
                closed,
                generation,
                closed_generations: Arc::new(Mutex::new(Vec::new())),
                subscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                degraded_contacts: false,
                close_gate: no_close_gate(),
                bad_boundary_batches: None,
                live_checkpoint_batch: false,
            });
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

/// `pause` promises the consumer a quiescent account: no engine-initiated
/// protocol work in flight. Registering the reopen's activity only after
/// `factory.open()` returned left a window where a pause could report
/// quiescence while a replacement connection was being established in the
/// background - and where the replacement, on losing that race, was dropped
/// without `close()`.
#[tokio::test]
async fn pause_cannot_report_quiescence_while_a_replacement_is_mid_open() {
    let account_id = AccountId("mid-open-pause".to_owned());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let closed = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn AccountFactory> = Arc::new(GatingFactory {
        scopes: vec![CursorScope::Account],
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::clone(&closed),
        opens: AtomicUsize::new(0),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let engine = Arc::new(
        SyncEngine::builder()
            .build()
            .expect("default engine config is valid"),
    );
    let control = engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach");

    let reopen_engine = Arc::clone(&engine);
    let reopen_id = account_id.clone();
    let reopen = tokio::spawn(async move { reopen_engine.reattach(&reopen_id).await });
    entered.notified().await;

    let mut pause = Box::pin(control.pause());
    for _ in 0..16 {
        tokio::select! {
            biased;
            result = &mut pause => {
                panic!("pause reported quiescence mid-open: {result:?}");
            }
            () = tokio::task::yield_now() => {}
        }
    }

    release.notify_one();
    reopen
        .await
        .expect("reopen task joins")
        .expect("reopen completes");
    pause.await.expect("pause completes once the swap is done");
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "the replacement swap must close exactly the old handle"
    );

    control.resume();
    engine.detach(&account_id).await.expect("detach");
}

/// Correlated teardown failure: the old handle refuses to unsubscribe AND the
/// unwind of the replacement's own subscriptions fails too. `Account::close()`
/// deliberately does not delete server-side subscriptions, so a replacement
/// handle that is merely logged and forgotten here is exactly the orphaned
/// webhook the retained-handle rule exists to prevent.
#[tokio::test]
async fn correlated_teardown_failure_retains_both_sides_for_retry() {
    let account_id = AccountId("correlated-teardown".to_owned());
    let old_scope = CursorScope::Account;
    let new_scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![old_scope.clone()],
            vec![old_scope.clone(), new_scope.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::clone(&unsubscribed),
        // Both the old-handle teardown and the replacement unwind that
        // follows it fail.
        unsubscribe_failures: Arc::new(AtomicUsize::new(2)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&old_scope))
        .await
        .expect("subscribe");

    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "an unconfirmed old-handle teardown must abort the swap"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &new_scope)
            .await
            .expect("new scope lookup")
            .is_none(),
        "a replacement cursor must not become durable when the topology swap aborts"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &old_scope)
            .await
            .expect("old scope lookup")
            .is_some(),
        "an aborted swap must retain the installed topology's durable cursor"
    );
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![
            (0, SubscriptionHandle("generation-0".into())),
            (1, SubscriptionHandle("generation-1".into())),
        ],
        "both sides are attempted, and both fail"
    );

    // The replacement was closed, but its handle must still be registered:
    // a later teardown call is the only thing that can delete it server-side.
    engine
        .unsubscribe_push(&account_id)
        .await
        .expect("both retained handles tear down on retry");
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![
            (0, SubscriptionHandle("generation-0".into())),
            (1, SubscriptionHandle("generation-1".into())),
            (0, SubscriptionHandle("generation-0".into())),
            (0, SubscriptionHandle("generation-1".into())),
        ],
        "neither the old nor the replacement handle may be forgotten"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// A prior session persisted a cursor for a scope this session's account
/// never discovered. A replacement rediscovers the scope, so reattach
/// resumes from the stored row without creating anything - and an aborted
/// swap must therefore leave that row exactly as it found it. Treating
/// "absent from the live registry" as "newly created" would make the abort
/// path delete a legitimately persisted cursor.
#[tokio::test]
async fn aborted_reattach_preserves_preexisting_cursor_for_rediscovered_scope() {
    let account_id = AccountId("rediscovered-preexisting".to_owned());
    let live_scope = CursorScope::Account;
    let rediscovered = CursorScope::Type(bifrost_types::ObjectType::Email);
    let fresh = CursorScope::Type(bifrost_types::ObjectType::Contact);
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live_scope.clone()],
            vec![live_scope.clone(), rediscovered.clone(), fresh.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live_scope))
        .await
        .expect("subscribe");

    // The prior session's row: durable, valid, not in the live registry.
    let prior = cursor_for(&rediscovered, b"prior-session");
    store
        .put_change_cursor(&account_id, prior.clone())
        .await
        .expect("seed prior-session cursor");

    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "the old-handle teardown failure must abort the swap"
    );
    let survived = store
        .get_change_cursor(&account_id, &rediscovered)
        .await
        .expect("rediscovered scope lookup")
        .expect("an aborted swap must not delete a preexisting durable cursor");
    assert_eq!(
        survived.server_state.bytes, prior.server_state.bytes,
        "the preexisting row must survive the aborted swap unchanged"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &fresh)
            .await
            .expect("fresh scope lookup")
            .is_none(),
        "a freshly created cursor must not stay durable after the aborted swap"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Store wrapper whose next `get_change_cursor` for one scripted scope
/// fails once, then delegates. Models a transient store read failure at
/// the worst point of a reattach: the read that decides whether a
/// rediscovered scope's row is preexisting or freshly created.
struct FlakyGetStore {
    inner: Arc<InMemoryCheckpointStore>,
    fail_get_for: Mutex<Option<CursorScope>>,
}

impl CheckpointStore for FlakyGetStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.apply_transition(account, transition)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DebtLedger, Error>> + Send + 'a>> {
        self.inner.get_ledger(account)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>
    {
        {
            let mut scripted = self.fail_get_for.lock().expect("flaky get lock");
            if scripted.as_ref() == Some(scope) {
                scripted.take();
                return Box::pin(async {
                    Err(Error::CheckpointStore("transient read failure".to_owned()))
                });
            }
        }
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

/// A transient store read failure while classifying a rediscovered
/// scope's row must never demote that preexisting row to "freshly
/// created": a scheme that swallows the error as "no row" hands the
/// abort path a legitimately persisted prior-session cursor to delete.
/// The reopen may fail - the read failed - but the row must survive.
#[tokio::test]
async fn transient_get_failure_cannot_demote_preexisting_cursor_to_created() {
    let account_id = AccountId("flaky-get-rediscovered".to_owned());
    let live_scope = CursorScope::Account;
    let rediscovered = CursorScope::Type(bifrost_types::ObjectType::Email);
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live_scope.clone()],
            vec![live_scope.clone(), rediscovered.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let inner = Arc::new(InMemoryCheckpointStore::new());
    let store = Arc::new(FlakyGetStore {
        inner: Arc::clone(&inner),
        fail_get_for: Mutex::new(None),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live_scope))
        .await
        .expect("subscribe");

    let prior = cursor_for(&rediscovered, b"prior-session");
    inner
        .put_change_cursor(&account_id, prior.clone())
        .await
        .expect("seed prior-session cursor");
    // Arm the transient failure only now, so it hits the reopen's
    // classification read for the rediscovered scope and nothing else.
    *store.fail_get_for.lock().expect("flaky get lock") = Some(rediscovered.clone());

    assert!(
        engine.reattach(&account_id).await.is_err(),
        "a failed classification read must abort the swap"
    );
    let survived = inner
        .get_change_cursor(&account_id, &rediscovered)
        .await
        .expect("rediscovered scope lookup")
        .expect("a transient read failure must not cost a preexisting durable cursor");
    assert_eq!(
        survived.server_state.bytes, prior.server_state.bytes,
        "the preexisting row must survive the failed reopen unchanged"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Store wrapper that records every change-cursor mutation and, on the
/// first put, first writes a scripted "concurrent ack" row into the inner
/// store - a deterministic stand-in for the old account's ack writer
/// persisting a newer checkpoint in the middle of a reattach, at the worst
/// possible interleaving point for any snapshot-and-restore scheme.
struct AckRacingStore {
    inner: Arc<InMemoryCheckpointStore>,
    mutated_scopes: Mutex<Vec<CursorScope>>,
    side_write: Mutex<Option<(AccountId, ChangeCursor)>>,
}

impl CheckpointStore for AckRacingStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        if let Checkpoint::Change(cursor) = &transition.checkpoint {
            self.mutated_scopes
                .lock()
                .expect("mutations lock")
                .push(cursor.scope.clone());
        }
        let side = self.side_write.lock().expect("side write lock").take();
        Box::pin(async move {
            if let Some((ack_account, ack_cursor)) = side {
                self.inner
                    .apply_transition(
                        &ack_account,
                        CheckpointTransition {
                            checkpoint: Checkpoint::Change(ack_cursor),
                            ledger: DebtLedger::default(),
                        },
                    )
                    .await?;
            }
            self.inner.apply_transition(account, transition).await
        })
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DebtLedger, Error>> + Send + 'a>> {
        self.inner.get_ledger(account)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>
    {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.mutated_scopes
            .lock()
            .expect("mutations lock")
            .push(scope.clone());
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

/// The old account's ack writer is not serialized against a reopen, so a
/// consumer acknowledgement can persist a newer checkpoint at any point of
/// the reattach - including for a scope the replacement no longer
/// discovers. An aborted swap must leave that newer row standing: the
/// abort path may only delete rows the reattach itself created, and every
/// preexisting row (vanished or live) must remain untouched until the
/// cutover is committed.
#[tokio::test]
async fn aborted_reattach_cannot_clobber_concurrently_acked_vanished_cursor() {
    let account_id = AccountId("ack-race-vanished".to_owned());
    let live_scope = CursorScope::Account;
    let vanished = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
    let fresh = CursorScope::Type(bifrost_types::ObjectType::Contact);
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live_scope.clone(), vanished.clone()],
            vec![live_scope.clone(), fresh.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let inner = Arc::new(InMemoryCheckpointStore::new());
    let newer_ack = cursor_for(&vanished, b"newer-acked-checkpoint");
    let store = Arc::new(AckRacingStore {
        inner: Arc::clone(&inner),
        mutated_scopes: Mutex::new(Vec::new()),
        side_write: Mutex::new(None),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live_scope))
        .await
        .expect("subscribe");
    *store.side_write.lock().expect("side write") = Some((account_id.clone(), newer_ack.clone()));
    // Attach persisted both live scopes; only the reopen's mutations are
    // under scrutiny. The scripted side write fires on the reopen's first
    // put, so it lands mid-reattach by construction.
    store.mutated_scopes.lock().expect("mutations lock").clear();

    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "the old-handle teardown failure must abort the swap"
    );
    let survived = inner
        .get_change_cursor(&account_id, &vanished)
        .await
        .expect("vanished scope lookup")
        .expect("an aborted swap must not delete a vanished scope's durable cursor");
    assert_eq!(
        survived.server_state.bytes, newer_ack.server_state.bytes,
        "the concurrently acked checkpoint must survive the aborted swap"
    );
    assert!(
        inner
            .get_change_cursor(&account_id, &fresh)
            .await
            .expect("fresh scope lookup")
            .is_none(),
        "a freshly created cursor must not stay durable after the aborted swap"
    );
    let mutated = store.mutated_scopes.lock().expect("mutations lock").clone();
    assert!(
        mutated.iter().all(|scope| *scope == fresh),
        "an aborted reopen may only mutate rows it created itself, got {mutated:?}"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Factory whose successive opens answer a scripted
/// `OpenedAccount::skipped_scopes` lane, so the engine-side plumbing of
/// the lane (store on attach, replace on reopen, expose via
/// `open_skipped_scopes`) is pinned without any protocol crate.
struct SkippingFactory {
    skips: Mutex<VecDeque<Vec<bifrost_types::SkippedScope>>>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
}

impl AccountFactory for SkippingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let skipped_scopes = self
            .skips
            .lock()
            .expect("skips lock")
            .pop_front()
            .expect("one skip set per expected open");
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        Box::pin(async move {
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: caps(),
                scopes: vec![CursorScope::Account],
                established,
                closed,
                generation: 0,
                closed_generations: Arc::new(Mutex::new(Vec::new())),
                subscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                degraded_contacts: false,
                close_gate: no_close_gate(),
                bad_boundary_batches: None,
                live_checkpoint_batch: false,
            });
            Ok(bifrost_types::OpenedAccount {
                account,
                skipped_scopes,
            })
        })
    }
}

#[tokio::test]
async fn open_skips_surface_on_attach_and_reattach_replaces_them() {
    let account_id = AccountId("open-skips".to_owned());
    let skip = bifrost_types::SkippedScope {
        scope: bifrost_types::ErrorScope::Mailbox {
            id: "shared-acct".into(),
        },
        error: unsupported(bifrost_types::AccountOperation::Discover),
    };
    let factory: Arc<dyn AccountFactory> = Arc::new(SkippingFactory {
        // First open (attach) reports one degraded scope; the second
        // (reopen) reports none - the outage healed.
        skips: Mutex::new(VecDeque::from([vec![skip.clone()], Vec::new()])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");

    assert!(
        matches!(
            engine.open_skipped_scopes(&account_id),
            Err(Error::AccountNotAttached(_))
        ),
        "an unattached account has no skip lane"
    );

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds despite the skipped scope");
    let skips = engine
        .open_skipped_scopes(&account_id)
        .expect("attached account exposes its skip lane");
    assert_eq!(skips.len(), 1, "attach stores the open-time skip");
    assert!(
        matches!(&skips[0].scope, bifrost_types::ErrorScope::Mailbox { id } if id.0 == "shared-acct"),
        "the lane preserves WHICH scope was skipped: {:?}",
        skips[0].scope
    );
    assert_eq!(
        skips[0].error.message_key(),
        skip.error.message_key(),
        "the lane preserves the classified error"
    );

    engine.reattach(&account_id).await.expect("reopen succeeds");
    assert!(
        engine
            .open_skipped_scopes(&account_id)
            .expect("attached account exposes its skip lane")
            .is_empty(),
        "a reopen whose open reports no skips replaces the stale lane"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// Factory whose first open succeeds with a scripted lifecycle stream and
/// whose every later open fails, so an `Engine(RestartAccount)` recovery can
/// never bump the account generation.
struct ParkedReopenFactory {
    scopes: Vec<CursorScope>,
    lifecycle_script: Arc<Mutex<VecDeque<ScopeLifecycleEvent>>>,
    lifecycle_calls: Arc<Mutex<Vec<usize>>>,
    opens: AtomicUsize,
}

impl AccountFactory for ParkedReopenFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        let scopes = self.scopes.clone();
        let lifecycle_script = Arc::clone(&self.lifecycle_script);
        let lifecycle_calls = Arc::clone(&self.lifecycle_calls);
        Box::pin(async move {
            if generation > 0 {
                return Err(unsupported(bifrost_types::AccountOperation::Discover));
            }
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: caps(),
                scopes,
                established: Arc::new(Mutex::new(Vec::new())),
                closed: Arc::new(AtomicUsize::new(0)),
                generation,
                closed_generations: Arc::new(Mutex::new(Vec::new())),
                subscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                lifecycle_calls,
                lifecycle_script,
                degraded_contacts: false,
                close_gate: no_close_gate(),
                bad_boundary_batches: None,
                live_checkpoint_batch: false,
            });
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

/// End-to-end pin (under paused time) for the lifecycle reader's bounded
/// reopen park. The stream terminates with an `Engine(RestartAccount)`-class
/// error, and every replacement open fails, so the account generation never
/// changes. The reader must neither reconnect immediately (the handover
/// prefers waiting for the replacement connection) nor park forever (a
/// reopen that exhausts its budget produces no generation change): it must
/// come back on its own after the 30s bound.
#[tokio::test(start_paused = true)]
async fn lifecycle_reader_unparks_after_the_bounded_reopen_wait() {
    use bifrost_types::{StateCause, SyncStateErrorKind};

    let account_id = AccountId("bounded-lifecycle-park".to_owned());
    let terminal = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged { delta: None }),
    )
    .operation(bifrost_types::AccountOperation::SyncChanges)
    .try_build()
    .expect("valid account-reopen recovery error");
    let lifecycle_calls = Arc::new(Mutex::new(Vec::new()));
    let factory: Arc<dyn AccountFactory> = Arc::new(ParkedReopenFactory {
        scopes: vec![CursorScope::Account],
        lifecycle_script: Arc::new(Mutex::new(VecDeque::from([
            ScopeLifecycleEvent::Terminated(terminal),
        ]))),
        lifecycle_calls: Arc::clone(&lifecycle_calls),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");

    let start = tokio::time::Instant::now();
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach");

    let mut reconnects = 0;
    for _ in 0..200 {
        reconnects = lifecycle_calls.lock().expect("lifecycle calls lock").len();
        if reconnects >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        reconnects >= 2,
        "the lifecycle reader must reconnect after the bounded wait"
    );
    assert!(
        start.elapsed() >= std::time::Duration::from_secs(30),
        "the reader reconnected before the reopen wait elapsed: {:?}",
        start.elapsed()
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Factory whose first open (attach) succeeds and whose every later open
/// fails, recording the instant of every attempt. The account-wide reopen
/// budget therefore always exhausts, and the recorded instants expose the
/// backoff schedule the engine slept between attempts.
///
/// The lifecycle script is shared with the test so the terminal event can be
/// armed AFTER attach returns: the lifecycle reader reconnects on a bounded
/// backoff and re-reads the script on each reconnect, which lets the test
/// subscribe to the changes and control streams before the reopen storm
/// starts. A broadcast subscriber only sees events sent after it subscribed,
/// so arming the trigger inside the factory would race the assertions.
struct ExhaustingFactory {
    lifecycle_script: Arc<Mutex<VecDeque<ScopeLifecycleEvent>>>,
    opens: AtomicUsize,
    open_instants: Arc<Mutex<Vec<tokio::time::Instant>>>,
}

impl AccountFactory for ExhaustingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        self.open_instants
            .lock()
            .expect("open instants lock")
            .push(tokio::time::Instant::now());
        let lifecycle_script = Arc::clone(&self.lifecycle_script);
        Box::pin(async move {
            if generation > 0 {
                return Err(unsupported(bifrost_types::AccountOperation::Discover));
            }
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: caps(),
                scopes: vec![CursorScope::Account],
                established: Arc::new(Mutex::new(Vec::new())),
                closed: Arc::new(AtomicUsize::new(0)),
                generation,
                closed_generations: Arc::new(Mutex::new(Vec::new())),
                subscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                lifecycle_script,
                degraded_contacts: false,
                close_gate: no_close_gate(),
                bad_boundary_batches: None,
                live_checkpoint_batch: false,
            });
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

/// Three consecutive `factory.open` failures exhaust the account-wide reopen
/// budget. This test's last error is terminal, so it remains unchanged in
/// `SyncEvent::Terminated` before the account pauses with
/// `PauseReason::RetryBudgetExhausted`.
///
/// Run under paused time so the backoff schedule is exact: the gap between
/// two recorded opens IS the slept delay, with no wall-clock noise. The
/// asserted windows are the documented base delays (1s, then 2s) widened by
/// the engine's +/-20% jitter.
#[tokio::test(start_paused = true)]
async fn three_failed_reopens_terminate_and_pause_the_account() {
    use bifrost_types::{StateCause, SyncStateErrorKind};
    use tokio::sync::broadcast::error::RecvError;

    let account_id = AccountId("reopen-budget-exhausted".to_owned());
    let lifecycle_script = Arc::new(Mutex::new(VecDeque::new()));
    let open_instants = Arc::new(Mutex::new(Vec::new()));
    let factory: Arc<dyn AccountFactory> = Arc::new(ExhaustingFactory {
        lifecycle_script: Arc::clone(&lifecycle_script),
        opens: AtomicUsize::new(0),
        open_instants: Arc::clone(&open_instants),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("the first open succeeds, so attach succeeds");

    let mut changes = engine
        .account_changes_stream(&account_id)
        .expect("attached account exposes its change stream");
    let mut control = engine
        .account_control_stream(&account_id)
        .expect("attached account exposes its control stream");

    // Arm the trigger: an `Engine(RestartAccount)`-class lifecycle
    // termination, picked up on the reader's next reconnect.
    let trigger = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged { delta: None }),
    )
    .operation(bifrost_types::AccountOperation::SyncChanges)
    .try_build()
    .expect("valid account-reopen recovery error");
    lifecycle_script
        .lock()
        .expect("lifecycle script lock")
        .push_back(ScopeLifecycleEvent::Terminated(trigger));

    let (scope, error) = tokio::time::timeout(std::time::Duration::from_secs(600), async {
        loop {
            match changes.recv().await {
                Ok(event) => {
                    if let SyncEvent::Terminated(err) = event.event.as_ref() {
                        break (event.scope.clone(), err.clone());
                    }
                }
                Err(RecvError::Closed) => panic!("the change stream closed before Terminated"),
                // Lagged: a slow subscriber dropped events, keep reading.
                Err(_) => {}
            }
        }
    })
    .await
    .expect("an exhausted reopen budget must broadcast Terminated");

    assert_eq!(
        scope,
        CursorScope::Account,
        "an exhausted account-wide budget terminates the account scope"
    );
    let expected = unsupported(bifrost_types::AccountOperation::Discover);
    assert_eq!(
        error.message_key(),
        expected.message_key(),
        "the terminal failure is preserved, not replaced by the lifecycle trigger"
    );
    assert!(
        matches!(error.kind(), AccountErrorKind::Unsupported(_)),
        "the terminating error keeps its classification: {:?}",
        error.kind()
    );

    let pause = tokio::time::timeout(std::time::Duration::from_secs(600), async {
        loop {
            match control.recv().await {
                Ok(bifrost_types::AccountControl::Pause(reason)) => break reason,
                // `AccountControl` is `#[non_exhaustive]`; anything that
                // is not the pause we are waiting for is skipped.
                Ok(_) => {}
                Err(RecvError::Closed) => panic!("the control stream closed before the pause"),
                Err(_) => {}
            }
        }
    })
    .await
    .expect("an exhausted reopen budget must pause the account");
    assert_eq!(
        pause,
        bifrost_types::PauseReason::RetryBudgetExhausted,
        "the pause names the budget, not a generic operator override"
    );

    let instants = open_instants.lock().expect("open instants lock").clone();
    assert_eq!(
        instants.len(),
        4,
        "one open for attach plus exactly three reopen attempts, then the budget stops trying"
    );
    // instants[0] is attach and instants[1] the first reopen attempt; the
    // gap between them also covers the lifecycle reader's own reconnect
    // backoff, so only the inter-attempt gaps pin the reopen schedule.
    let first_backoff = instants[2] - instants[1];
    let second_backoff = instants[3] - instants[2];
    assert!(
        (std::time::Duration::from_millis(800)..=std::time::Duration::from_millis(1200))
            .contains(&first_backoff),
        "the second attempt waits the 1s base delay +/-20% jitter: {first_backoff:?}"
    );
    assert!(
        (std::time::Duration::from_millis(1600)..=std::time::Duration::from_millis(2400))
            .contains(&second_backoff),
        "the third attempt waits the doubled 2s delay +/-20% jitter: {second_backoff:?}"
    );

    engine
        .resume_account(&account_id)
        .expect("the paused account resumes on consumer request");
    engine.detach(&account_id).await.expect("detach");
}

/// A provider that violates the batch boundary contract must STOP the scope,
/// not be logged and retried.
///
/// `Batch`'s fields are public, so nothing prevents an account from emitting
/// `PageBoundary::Partial` with a checkpoint attached. The first guard against
/// it returned a bare engine error, which `handle_drive_outcome` only logs
/// before re-entering the poll loop from the same unchanged cursor: against a
/// provider that keeps doing it, that is an unbounded run of requests with no
/// recovery dispatch, no scope stop, and nothing on the stream telling a
/// consumer the scope has quietly stopped making progress.
///
/// The stub's stream is endless on purpose. Exactly one batch may be drawn
/// from it, the driver must report `Terminated`, the classified error must be
/// terminal so `plan_recovery` stops the scope rather than scheduling another
/// attempt, and a subscriber must see the termination.
#[tokio::test]
async fn a_partial_batch_carrying_a_checkpoint_terminates_the_scope() {
    use bifrost_sync::cancel::Boundary;
    use bifrost_sync::cursor::CursorRegistry;
    use bifrost_sync::multiplexer::{ChangesEvent, drive_changes_stream};

    let scope = CursorScope::Account;
    let emitted = Arc::new(BoundaryCounter::new(0));
    let account = HealAccount {
        caps: caps(),
        scopes: vec![scope.clone()],
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        generation: 0,
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
        degraded_contacts: false,
        close_gate: no_close_gate(),
        bad_boundary_batches: Some(Arc::clone(&emitted)),
        live_checkpoint_batch: false,
    };

    let (changes_tx, mut changes_rx) = tokio::sync::broadcast::channel(16);
    let (boundary, _handle) = Boundary::new();
    let outcome = drive_changes_stream(
        &account,
        scope.clone(),
        cursor_for(&scope, b"live"),
        Arc::new(CursorRegistry::new()),
        Arc::new(bifrost_sync::multiplexer::ChangeDelivery::new(changes_tx)),
        boundary.subscribe(),
        None,
        None,
    )
    .await
    .expect("the violation is reported as a terminated scope, not an engine error");

    let ChangesEvent::Terminated(error) = outcome else {
        panic!("a boundary violation must terminate the scope, got {outcome:?}");
    };
    assert!(
        error.recovery().is_terminal(),
        "a non-terminal class would send the poll loop back at the same cursor: {:?}",
        error.recovery()
    );
    assert_eq!(
        emitted.load(Ordering::SeqCst),
        1,
        "the driver must stop on the first bad batch, never keep drawing from a provider that repeats it"
    );

    let published = changes_rx
        .try_recv()
        .expect("subscribers must be told the scope terminated");
    assert!(
        matches!(published.event.as_ref(), SyncEvent::Terminated(_)),
        "the published event must be the termination, got {:?}",
        published.event
    );
}

/// Every checkpoint the live driver publishes must carry a publication id.
///
/// The acknowledgement path resolves coverage BY PUBLICATION, not by checkpoint
/// equality - two backfill pages can produce byte-identical checkpoints, and
/// fusion publishes its final checkpoint twice. A checkpoint broadcast without
/// an id therefore cannot be acknowledged against its own claim: the writer
/// either finds no claim at all or, worse, matches a neighbour's. The invariant
/// held when it was audited, and this pins it, because nothing else did.
#[tokio::test]
async fn a_published_change_checkpoint_always_carries_its_publication_id() {
    use bifrost_sync::cancel::Boundary;
    use bifrost_sync::cursor::CursorRegistry;
    use bifrost_sync::multiplexer::drive_changes_stream;

    let scope = CursorScope::Account;
    let account = HealAccount {
        caps: caps(),
        scopes: vec![scope.clone()],
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        generation: 0,
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
        degraded_contacts: false,
        close_gate: no_close_gate(),
        bad_boundary_batches: None,
        live_checkpoint_batch: true,
    };

    let (changes_tx, mut changes_rx) = tokio::sync::broadcast::channel(16);
    let (boundary, _handle) = Boundary::new();
    let (priority, _priority_rx) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _bandwidth_rx) = tokio::sync::watch::channel(None);
    let control = bifrost_sync::control::SyncControl::new(
        AccountId("publishes-ids".into()),
        boundary.clone(),
        priority,
        bandwidth,
    );
    drive_changes_stream(
        &account,
        scope.clone(),
        cursor_for(&scope, b"live"),
        Arc::new(CursorRegistry::new()),
        Arc::new(bifrost_sync::multiplexer::ChangeDelivery::new(changes_tx)),
        boundary.subscribe(),
        Some(control),
        None,
    )
    .await
    .expect("the stub stream completes cleanly");

    let published = changes_rx.try_recv().expect("a batch reached subscribers");
    assert!(
        published.checkpoint.is_some(),
        "the stub emits a checkpoint-bearing batch"
    );
    assert!(
        published.publication.is_some(),
        "a checkpoint published without an id cannot be acknowledged against its own claim"
    );
}

/// A reattach that COMMITS must clear the provisional set, or the NEXT
/// reattach's abort deletes cursors belonging to a reattach that succeeded.
///
/// The rollback path is a conditional delete driven by the provisional set:
/// only rows the current reattach inserted may be destroyed. If the commit that
/// promotes those rows to ordinary durable state never runs, the set carries
/// them forward, and the following abort - a routine outcome, since teardown of
/// the outgoing handle is allowed to fail - takes the earlier reattach's work
/// with it. The account then resumes with a scope whose durable cursor silently
/// vanished, and re-establishes it by re-walking inventory.
#[tokio::test]
async fn a_committed_reattach_is_not_rolled_back_by_the_next_aborted_one() {
    let account_id = AccountId("reattach-commit-clears-provisional".to_owned());
    let live = CursorScope::Account;
    let second = CursorScope::Type(bifrost_types::ObjectType::Email);
    let third = CursorScope::Type(bifrost_types::ObjectType::Contact);
    let unsubscribe_failures = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live.clone()],
            vec![live.clone(), second.clone()],
            vec![live.clone(), third.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::clone(&unsubscribe_failures),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("initial attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live))
        .await
        .expect("initial push subscription");

    // First reopen succeeds: `second` becomes ordinary durable state.
    engine.reattach(&account_id).await.expect("first reattach");
    assert!(
        store
            .get_change_cursor(&account_id, &second)
            .await
            .expect("second scope lookup")
            .is_some(),
        "a successful reattach must leave the scope it discovered durable"
    );

    // Second reopen aborts on old-handle teardown. Its rollback may destroy
    // only what IT inserted.
    unsubscribe_failures.store(1, Ordering::SeqCst);
    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "the old-handle teardown failure must abort the second swap"
    );

    assert!(
        store
            .get_change_cursor(&account_id, &second)
            .await
            .expect("second scope lookup")
            .is_some(),
        "an aborted reattach must not delete a cursor a COMMITTED reattach created"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &third)
            .await
            .expect("third scope lookup")
            .is_none(),
        "the aborted reattach must still destroy the rows it created itself"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// A deferred inventory walk (attach-time, so its scope is not registered)
/// that terminates with a schema error triggers the account reset through
/// `ctx.recover(Some(scope), ..)`. The reset must re-establish that scope even
/// though the registry never held it, and the queued sibling item must not
/// walk a second time.
///
/// Ablations: pass an empty `extra` from the directive arm (A is walked once
/// and never registered); drop both of the worker's pre-walk fence checks, the
/// one before the throttle waits and the one after admission (B walks twice:
/// its queued walk runs after the reset, is refused at its first publication,
/// and is re-run through `EstablishIfAbsent`).
#[tokio::test(start_paused = true)]
async fn a_deferred_inventory_that_raises_schema_incompatible_is_established_after_the_reset() {
    let a = CursorScope::Type(bifrost_types::ObjectType::Email);
    let b = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
    let walks = Arc::new(Mutex::new(
        std::collections::HashMap::<CursorScope, usize>::new(),
    ));
    let observed = Arc::clone(&walks);
    let mut account = common::StubAccount::new(vec![a.clone(), b.clone()]);
    account.establishment = |_| bifrost_types::CursorEstablishment::EstablishViaInventory;
    let trigger = a.clone();
    account.inventory_hook = Some(Arc::new(move |scope| {
        let mut walks = observed.lock().expect("walks");
        let count = walks.entry(scope.clone()).or_default();
        *count += 1;
        if *scope == trigger && *count == 1 {
            vec![bifrost_types::InventoryEvent::Terminated(
                bifrost_types::AccountErrorBuilder::new(
                    bifrost_types::AccountErrorKind::SyncState(
                        bifrost_types::SyncStateErrorKind::SchemaIncompatible,
                    ),
                    bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
                )
                .try_build()
                .expect("schema error"),
            )]
        } else {
            vec![bifrost_types::InventoryEvent::Done(
                bifrost_types::InventoryCompletion::complete(
                    bifrost_types::CoverageDomain::full(scope.clone()),
                    Some(Checkpoint::Change(cursor_for(scope, b"fresh"))),
                    bifrost_types::PartitionEnd::Exhausted,
                ),
            )]
        }
    }));
    let id = AccountId("deferred-schema".into());
    let engine = SyncEngine::builder().build().expect("engine");
    engine
        .attach(
            id.clone(),
            Arc::new(common::StubFactory::queue(vec![Arc::new(account)])),
        )
        .await
        .expect("attach");
    let mut receiver = engine.account_changes_stream(&id).expect("subscribe");
    let mut done = std::collections::HashSet::new();
    tokio::time::timeout(Duration::from_secs(300), async {
        while done.len() < 2 {
            let event = receiver.recv().await.expect("event");
            if let Some(checkpoint) = event.checkpoint.clone() {
                engine
                    .ack_checkpoint(
                        &id,
                        event.scope.clone(),
                        checkpoint,
                        event.publication.clone(),
                    )
                    .await
                    .expect("ack");
            }
            if matches!(event.event.as_ref(), SyncEvent::Done(_)) {
                done.insert(event.scope);
            }
        }
    })
    .await
    .expect("both inventories");
    assert_eq!(walks.lock().expect("walks").get(&a), Some(&2));
    assert_eq!(walks.lock().expect("walks").get(&b), Some(&1));
    engine.detach(&id).await.expect("detach");
}
