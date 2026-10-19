//! Reusable in-crate `Account` double for bifrost-sync integration tests.
//!
//! The `Account` trait is wide enough (~60 methods) that every integration
//! test used to re-implement a full stub, and a whole bug-hunt round could
//! not write an end-to-end orchestrator test at all for that reason. This is
//! the seam: `StubAccount` answers every method the engine never drives with
//! `Unsupported` (exactly what a protocol crate returns for an operation it
//! does not offer) or an empty stream (exactly what an idle account yields),
//! and exposes closure hooks for the surfaces the sync pipeline actually
//! exercises - discovery, establishment, inventory, partitioned inventory,
//! and the changes stream.
//!
//! Hermetic by construction: no socket, no port, no sleeps. Keep the double
//! honest - a hook must hand back only shapes a production protocol crate can
//! produce (`validate_boundary`-clean batches, envelope-valid cursors), or
//! every test built on it is confidently wrong.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFactory, AccountFuture, AccountId, AccountStream, AttachmentHandle, Batch,
    BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Cause, Change, ChangeCursor,
    Checkpoint, CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest, ContainerId, ContainerKind, ConvenienceShape, CursorDescriptor,
    CursorEstablishment, CursorFreshness, CursorScope, DraftHandle, DraftPatch, EventCreate,
    EventId, EventPatch, EventRange, EventSearchRequest, FilterRuleShape, FilterValidation, FlagOp,
    HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, Importance, InventoryEvent, InventoryPartition, InventoryPartitioning,
    ItemOutcome, MembershipScope, Message, MutationCapabilities, MutationConcurrency,
    MutationReplaySafety, MutationSuccess, MutationTarget, ObjectId, OpaqueChangeState, Page,
    Priority, Projection, ProtocolKind, PushCapability, QuotaInfo, QuotaSignal, RateLimitClass,
    RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId,
    VacationConfig, WatchEvent,
};
use bifrost_types::{AddressBook, AddressBookId};
use bifrost_types::{Calendar, CalendarEvent};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;

pub fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(bifrost_types::RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// A classified transport failure for a provider that refuses to close. There
/// is no `AccountOperation::Close`, so this is tagged by its cause alone.
pub fn close_refused() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
        Cause::Transport(bifrost_types::TransportCause::new(
            bifrost_types::TransportKind::Network,
            None,
        )),
    )
    .try_build()
    .expect("valid account error classification")
}

/// A classified transport failure for a provider that could not delete a push
/// subscription - a Graph 503 on the DELETE, a JMAP socket that died.
pub fn push_teardown_refused() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
        Cause::Transport(bifrost_types::TransportCause::new(
            bifrost_types::TransportKind::Network,
            None,
        )),
    )
    .try_build()
    .expect("valid account error classification")
}

/// Push traffic recorded by every `StubAccount` sharing it, so a test can see
/// which connection each subscribe and unsubscribe reached.
#[derive(Debug, Default)]
pub struct PushLog {
    /// `(push_label of the account called, scopes)` per `push_subscribe`.
    pub subscribed: Mutex<Vec<(String, Vec<CursorScope>)>>,
    /// `(push_label of the account called, handle)` per `push_unsubscribe`,
    /// failed calls included.
    pub unsubscribed: Mutex<Vec<(String, SubscriptionHandle)>>,
}

impl PushLog {
    #[must_use]
    pub fn subscribed(&self) -> Vec<(String, Vec<CursorScope>)> {
        self.subscribed.lock().expect("push log lock").clone()
    }

    #[must_use]
    pub fn unsubscribed(&self) -> Vec<(String, SubscriptionHandle)> {
        self.unsubscribed.lock().expect("push log lock").clone()
    }
}

/// Neutral capability set: no push, no blob ranges, generous rate class.
pub fn caps() -> AccountCapabilities {
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
        pim_methods: bifrost_types::PimMethodSupport::default(),
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

/// An envelope-valid change cursor for `scope` carrying `state` bytes.
pub fn cursor_for(scope: &CursorScope, state: &[u8]) -> ChangeCursor {
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

/// An inventory walk that ESTABLISHES: its `Done` carries a change cursor for
/// the scope, so fusion reports `Established` and the deferred-inventory worker
/// goes on to its membership refresh - which is where `memberships_stall` holds
/// it.
#[must_use]
pub fn establishing_inventory() -> InventoryHook {
    Arc::new(|scope| {
        vec![InventoryEvent::Done(
            bifrost_types::InventoryCompletion::complete(
                bifrost_types::CoverageDomain::full(scope.clone()),
                Some(Checkpoint::Change(cursor_for(
                    scope,
                    b"inventory-established",
                ))),
                bifrost_types::PartitionEnd::Exhausted,
            ),
        )]
    })
}

/// Produces the `InventoryEvent`s one partition pass yields.
pub type PartitionHook =
    Arc<dyn Fn(&CursorScope, &InventoryPartition) -> Vec<InventoryEvent> + Send + Sync>;

/// Produces the `InventoryEvent`s a whole-scope inventory walk yields.
pub type InventoryHook = Arc<dyn Fn(&CursorScope) -> Vec<InventoryEvent> + Send + Sync>;

/// Answers one inventory repair request.
pub type RepairHook = Arc<
    dyn Fn(&bifrost_types::InventoryRepairRequest) -> bifrost_types::InventoryRepairEvent
        + Send
        + Sync,
>;

/// Produces the `SyncEvent<Change>`s one changes drive yields.
pub type ChangesHook = Arc<dyn Fn(&ChangeCursor) -> Vec<SyncEvent<Change>> + Send + Sync>;

/// The throttle keys a scope drive consumes.
pub type ScopeKeysHook = Arc<dyn Fn(&CursorScope) -> Vec<bifrost_types::ThrottleKey> + Send + Sync>;

/// Configurable `Account` double. Construct with [`StubAccount::new`], then
/// set hooks for the surfaces the test drives; everything else answers
/// `Unsupported` or an empty stream.
pub struct StubAccount {
    pub caps: AccountCapabilities,
    /// Scopes yielded by `discover_cursor_scopes`.
    pub scopes: Vec<CursorScope>,
    /// How `establish_initial_cursor` answers. Default: `Ready` with a fresh
    /// envelope-valid cursor.
    pub establishment: fn(&CursorScope) -> CursorEstablishment,
    /// Answer for `inventory_partitioning(scope)`.
    pub partitioning: InventoryPartitioning,
    /// Hook for `inventory_partition_stream`. Default: empty stream.
    pub partition_hook: Option<PartitionHook>,
    /// Hook for `inventory_stream`. Default: empty stream.
    pub inventory_hook: Option<InventoryHook>,
    /// Hook for `changes_stream`. Default: empty stream.
    pub changes_hook: Option<ChangesHook>,
    /// Throttle keys `throttle_keys` declares for a scope drive. Default:
    /// none, so the engine waits on account-wide holds only - exactly the
    /// trait's own default.
    pub scope_keys: Option<ScopeKeysHook>,
    /// Answers one `repair_inventory` request. Mapped over the request
    /// stream, so an answer always carries the attempt id it belongs to -
    /// which a canned outcome list cannot, since attempt ids are minted at
    /// plan time. Default: empty stream, i.e. every request falls through to
    /// a local deferral.
    pub repair_hook: Option<RepairHook>,
    /// Memberships yielded by `discover_memberships`. Empty means an empty
    /// stream, which is what an account with no folder topology reports.
    ///
    /// The engine's membership INDEX is built from this, and a folder
    /// deletion resolves through that index - so a lifecycle deletion fired
    /// at an account that discovered no memberships silently deletes nothing.
    pub memberships: Vec<MembershipScope>,
    /// Lifecycle events fired on demand, taken by the FIRST
    /// `scope_lifecycle_stream` call; later calls (a reopen re-subscribes)
    /// get an empty stream. Holding the sender keeps the stream open, which
    /// is what an idle provider poll looks like; dropping it ends it.
    pub lifecycle_rx: Mutex<Option<tokio::sync::mpsc::Receiver<ScopeLifecycleEvent>>>,
    /// Every partition the engine asked this account to walk, in call order.
    pub walked: Arc<Mutex<Vec<(CursorScope, InventoryPartition)>>>,
    /// Every scope the engine asked to establish, in call order.
    pub established: Arc<Mutex<Vec<CursorScope>>>,
    /// `close()` call count. Incremented on ENTRY, before `close_gate` is
    /// awaited, so a test can tell "close was reached" from "close returned".
    pub closed: Arc<std::sync::atomic::AtomicUsize>,
    /// `close()` completion count: bumped only once the future has passed
    /// `close_gate` and is about to answer. A caller that abandoned the close
    /// (drop at a timeout) leaves this behind `closed`.
    pub close_returned: Arc<std::sync::atomic::AtomicUsize>,
    /// When set, `close()` parks on this `Notify` before answering - a
    /// provider whose connection teardown hangs. Never notifying it is the
    /// hang-forever case; `notify_waiters` releases it.
    pub close_gate: Option<Arc<tokio::sync::Notify>>,
    /// When true, `close()` answers a classified transport error instead of
    /// `Ok`. Distinct from `close_gate`: a refusal, not a hang.
    pub close_fails: bool,
    /// When set, `inventory_stream` yields whatever `inventory_hook` produced
    /// and then PARKS on this `Notify` before ending; `notify_waiters` lets the
    /// stream finish.
    ///
    /// This does NOT make a straggler worker. `InventoryFusion::run_stream`
    /// polls the provider stream under the walk's shutdown token, so a detach
    /// drops a parked inventory stream as soon as it cancels the slot. Tests
    /// that need a worker `detach` must wait on use `memberships_stall`.
    ///
    /// The deferred-inventory worker waits for a real subscriber before walking
    /// anything, so a test that wants this stream parked must subscribe first.
    pub inventory_stall: Option<Arc<tokio::sync::Notify>>,
    /// Instrumentation for the stalled stream above, taken by the first stalling
    /// `inventory_stream` call. See [`StallProbe`].
    pub inventory_stall_probe: Option<StallProbe>,
    /// A stall for the NEXT `discover_memberships` call, armed at runtime and
    /// taken by the call it stalls. That call yields `memberships` and then
    /// parks on the `Notify`; the optional probe observes the parked stream.
    ///
    /// Armed after attach because attach walks memberships itself. The call it
    /// is aimed at is the deferred-inventory worker's membership refresh after
    /// a successful inventory establishment: that walk polls the provider with
    /// no shutdown arm, so a parked one keeps the worker a STRAGGLER that
    /// `detach` can only abort at its worker deadline. It needs the inventory
    /// walk to establish a cursor first - an `inventory_hook` whose `Done`
    /// carries a change checkpoint for the scope.
    ///
    /// Parking a CHANGES stream does NOT make a straggler, which was
    /// established by measurement: the multiplexer owns its per-scope poll
    /// tasks and retires them itself in its shutdown tail (it CANCELS their
    /// tokens; the explicit abort there is for the lifecycle task), so they are
    /// not among the workers `detach` waits on.
    pub memberships_stall: Mutex<Option<(Arc<tokio::sync::Notify>, Option<StallProbe>)>>,
    /// `inventory_stream()` call count.
    pub inventory_calls: Arc<std::sync::atomic::AtomicUsize>,
    /// When set, `changes_stream` yields `changes_hook`'s events and then
    /// parks forever instead of ending.
    ///
    /// The shape a provider wedge actually takes: the stream neither yields nor
    /// ends, so a driver with no arm on its poll waits on it indefinitely. The
    /// notify is the release valve, so a test can also prove the park was real
    /// by letting it go.
    pub changes_stall: Option<Arc<tokio::sync::Notify>>,
    /// Same wedge for `inventory_partition_stream`, for the backfill runner's
    /// poll arm.
    pub partition_stall: Option<Arc<tokio::sync::Notify>>,
    /// Names this account in `push_log`, and is the handle `push_subscribe`
    /// mints, so a handle says which connection created it. Default `"stub"`.
    pub push_label: String,
    /// Where this account records its push traffic. Share one across the
    /// accounts a factory hands out to see a reopen's calls in order.
    pub push_log: Arc<PushLog>,
    /// How many upcoming `push_unsubscribe` calls on THIS account fail with
    /// [`push_teardown_refused`] before it starts succeeding. Default 0.
    pub push_unsubscribe_failures: std::sync::atomic::AtomicUsize,
    /// Handles whose `push_unsubscribe` on THIS account always fails, however
    /// many times it is retried. Lets a test make some subscriptions refuse and
    /// others succeed within one reopen. Default empty.
    pub push_unsubscribe_refused: Vec<String>,
    /// A provider that errors on a handle it no longer knows: a successful
    /// `push_unsubscribe` remembers the handle, and a second call for it fails
    /// with [`push_teardown_refused`]. Default false.
    pub push_unsubscribe_strict: bool,
    /// Handles deleted by a successful `push_unsubscribe` on THIS account.
    pub push_deleted: Mutex<Vec<String>>,
    /// Parks ONE `push_subscribe` call on the `Notify` once `n` subscriptions
    /// under this account's label have already been made, then disarms. The
    /// call is logged and its handle minted before it parks, so a caller that
    /// drops it there has a provider-side subscription it never learned of.
    pub push_subscribe_park: Mutex<Option<(usize, Arc<tokio::sync::Notify>)>>,
    /// Parks the NEXT `push_unsubscribe` call on the `Notify`, then disarms.
    /// The call is logged (and, on a strict provider, remembered) on entry.
    pub push_unsubscribe_park: Mutex<Option<Arc<tokio::sync::Notify>>>,
    /// How many upcoming `establish_initial_cursor` calls on THIS account fail
    /// with [`push_teardown_refused`] (a classified, retryable transport
    /// error) before it starts succeeding. Default 0. The call is still
    /// recorded in `established`. Set it AFTER attach, which establishes too.
    pub establish_failures: std::sync::atomic::AtomicUsize,
    /// Notified (`notify_one`, so a permit is stored if nobody waits yet) each
    /// time an `establish_initial_cursor` call fails on account of
    /// `establish_failures`. Lets a test act between an establishment attempt
    /// and its retry.
    pub establish_failed: Arc<tokio::sync::Notify>,
}

/// Observes the LIFETIME of a stalled provider stream, not just its effects.
///
/// Two signals, both one-shot so a test can await either whenever it likes
/// rather than having to be waiting at the moment it fires:
///
/// - `parked`: sent from inside the stalled tail the first time it is polled,
///   immediately before it parks. This is what tells a test the straggler is
///   really staged, rather than merely that `inventory_stream` was called.
/// - `destroyed`: sent from inside `Drop` with the instant of destruction. The
///   timestamp is taken INSIDE the drop, not when the test receives it, because
///   the receive is subject to scheduling delay and the whole point of the
///   signal is to measure WHEN the provider stream went away.
///
/// The drop stamp is captured by the tail future at construction, so it is
/// dropped with the stream whether or not the tail was ever polled.
pub struct StallProbe {
    parked: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    destroyed: Mutex<Option<tokio::sync::oneshot::Sender<tokio::time::Instant>>>,
}

/// Fires its instant from `Drop`. Held by the stalled stream's tail future.
struct DropStamp(Option<tokio::sync::oneshot::Sender<tokio::time::Instant>>);

impl Drop for DropStamp {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(tokio::time::Instant::now());
        }
    }
}

impl StallProbe {
    /// Build a probe plus its two receivers: the parked signal and the
    /// destruction instant.
    #[must_use]
    pub fn install() -> (
        Self,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Receiver<tokio::time::Instant>,
    ) {
        let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
        let (destroyed_tx, destroyed_rx) = tokio::sync::oneshot::channel();
        (
            Self {
                parked: Mutex::new(Some(parked_tx)),
                destroyed: Mutex::new(Some(destroyed_tx)),
            },
            parked_rx,
            destroyed_rx,
        )
    }

    fn take_parked(&self) -> Option<tokio::sync::oneshot::Sender<()>> {
        self.parked.lock().expect("parked lock").take()
    }

    fn take_stamp(&self) -> DropStamp {
        DropStamp(self.destroyed.lock().expect("destroyed lock").take())
    }
}

/// `items`, then a tail that parks on `gate` and ends without yielding.
///
/// The probe's drop stamp is captured at CONSTRUCTION so it lives in the tail
/// future from the moment the stream exists, and therefore fires when the
/// stream is destroyed whether or not the tail was ever polled.
fn stalled<T: Send + 'static>(
    items: Vec<T>,
    gate: Arc<tokio::sync::Notify>,
    probe: Option<&StallProbe>,
) -> AccountStream<T> {
    let stamp = probe.map(StallProbe::take_stamp);
    let parked_tx = probe.and_then(StallProbe::take_parked);
    let tail = stream::once(async move {
        let _stamp = stamp;
        // The park future is created before the signal is sent, so a releaser
        // reacting to it cannot notify into the gap.
        let waiter = gate.notified();
        if let Some(tx) = parked_tx {
            let _ = tx.send(());
        }
        waiter.await;
    })
    .filter_map(|()| async move { None::<T> });
    Box::pin(stream::iter(items).chain(tail))
}

impl StubAccount {
    #[must_use]
    pub fn new(scopes: Vec<CursorScope>) -> Self {
        Self {
            caps: caps(),
            scopes,
            establishment: |scope| CursorEstablishment::Ready(cursor_for(scope, b"stub-ready")),
            partitioning: InventoryPartitioning::Full,
            partition_hook: None,
            inventory_hook: None,
            changes_hook: None,
            scope_keys: None,
            repair_hook: None,
            memberships: Vec::new(),
            lifecycle_rx: Mutex::new(None),
            walked: Arc::new(Mutex::new(Vec::new())),
            established: Arc::new(Mutex::new(Vec::new())),
            closed: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            close_returned: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            close_gate: None,
            close_fails: false,
            inventory_stall: None,
            inventory_stall_probe: None,
            memberships_stall: Mutex::new(None),
            inventory_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            changes_stall: None,
            partition_stall: None,
            push_label: "stub".to_owned(),
            push_log: Arc::new(PushLog::default()),
            push_unsubscribe_failures: std::sync::atomic::AtomicUsize::new(0),
            push_unsubscribe_refused: Vec::new(),
            push_unsubscribe_strict: false,
            push_deleted: Mutex::new(Vec::new()),
            push_subscribe_park: Mutex::new(None),
            push_unsubscribe_park: Mutex::new(None),
            establish_failures: std::sync::atomic::AtomicUsize::new(0),
            establish_failed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Partitions walked so far, in call order.
    #[must_use]
    pub fn walked_partitions(&self) -> Vec<(CursorScope, InventoryPartition)> {
        self.walked.lock().expect("walked lock").clone()
    }
}

/// Factory serving one pre-built `StubAccount` per `open` from a queue, or a
/// closure-built one when the queue is empty.
pub struct StubFactory {
    accounts: Mutex<Vec<Arc<StubAccount>>>,
}

impl StubFactory {
    /// A factory whose successive `open` calls hand out `accounts` in order.
    /// Opening more times than accounts were supplied panics, loudly naming
    /// the miscounted expectation.
    #[must_use]
    pub fn queue(accounts: Vec<Arc<StubAccount>>) -> Self {
        let mut accounts = accounts;
        accounts.reverse();
        Self {
            accounts: Mutex::new(accounts),
        }
    }
}

impl AccountFactory for StubFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let account = self
            .accounts
            .lock()
            .expect("factory queue lock")
            .pop()
            .expect("StubFactory::queue ran out of accounts: one per expected open");
        Box::pin(async move {
            let account: Arc<dyn Account> = account;
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

impl Account for StubAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn throttle_keys(
        &self,
        _account: &AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        match (work, self.scope_keys.as_ref()) {
            (bifrost_types::ThrottleWork::Scope(scope), Some(hook)) => hook(scope),
            _ => Vec::new(),
        }
    }

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by StubAccount tests")
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
        let items: Vec<SyncEvent<MembershipScope>> = if self.memberships.is_empty() {
            Vec::new()
        } else {
            vec![
                SyncEvent::Batch(Batch {
                    items: self.memberships.clone(),
                    page_boundary: bifrost_types::PageBoundary::Final,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                }),
                SyncEvent::Done(None),
            ]
        };
        if let Some((gate, probe)) = self
            .memberships_stall
            .lock()
            .expect("memberships stall lock")
            .take()
        {
            // No `Done` before the park: a `Done` ends the engine's walk, and
            // the stall is meant to hold it.
            let items = items
                .into_iter()
                .filter(|event| !matches!(event, SyncEvent::Done(_)))
                .collect();
            return stalled(items, gate, probe.as_ref());
        }
        Box::pin(stream::iter(items))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        match self.lifecycle_rx.lock().expect("lifecycle lock").take() {
            Some(rx) => Box::pin(stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|event| (event, rx))
            })),
            None => Box::pin(stream::empty()),
        }
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        self.established
            .lock()
            .expect("established lock")
            .push(scope.clone());
        let failing = self
            .establish_failures
            .try_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok();
        if failing {
            self.establish_failed.notify_one();
            return Box::pin(async { Err(push_teardown_refused()) });
        }
        let establishment = (self.establishment)(&scope);
        Box::pin(async move { Ok(establishment) })
    }

    fn inventory_partitioning(&self, _scope: &CursorScope) -> InventoryPartitioning {
        self.partitioning.clone()
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        self.inventory_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let items = match &self.inventory_hook {
            Some(hook) => hook(&scope),
            None => Vec::new(),
        };
        if let Some(gate) = &self.inventory_stall {
            return stalled(items, Arc::clone(gate), self.inventory_stall_probe.as_ref());
        }
        Box::pin(stream::iter(items))
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<InventoryEvent> {
        self.walked
            .lock()
            .expect("walked lock")
            .push((scope.clone(), partition.clone()));
        let items = match &self.partition_hook {
            Some(hook) => hook(&scope, &partition),
            None => Vec::new(),
        };
        if let Some(gate) = &self.partition_stall {
            let gate = Arc::clone(gate);
            let tail = stream::once(async move { gate.notified().await })
                .filter_map(|()| async move { None::<InventoryEvent> });
            return Box::pin(stream::iter(items).chain(tail));
        }
        Box::pin(stream::iter(items))
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        Box::pin(stream::empty())
    }

    fn repair_inventory(
        &self,
        requests: AccountStream<bifrost_types::InventoryRepairRequest>,
    ) -> AccountStream<bifrost_types::InventoryRepairEvent> {
        match &self.repair_hook {
            Some(hook) => {
                let hook = Arc::clone(hook);
                Box::pin(requests.map(move |request| hook(&request)))
            }
            None => Box::pin(stream::empty()),
        }
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let items = match &self.changes_hook {
            Some(hook) => hook(&cursor),
            None => Vec::new(),
        };
        if let Some(gate) = &self.changes_stall {
            let gate = Arc::clone(gate);
            let tail = stream::once(async move { gate.notified().await })
                .filter_map(|()| async move { None::<SyncEvent<Change>> });
            return Box::pin(stream::iter(items).chain(tail));
        }
        Box::pin(stream::iter(items))
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        // The first subscription on an account is named by its label, so a
        // single-subscription test reads `call("old", "old")`. Further ones
        // get `label-2`, `label-3`, ...: a test holding several subscriptions
        // on one account needs handles it can tell apart.
        let (handle, park) = {
            let mut subscribed = self.push_log.subscribed.lock().expect("push log lock");
            let prior = subscribed
                .iter()
                .filter(|(label, _)| label == &self.push_label)
                .count();
            subscribed.push((self.push_label.clone(), scopes.to_vec()));
            let mut armed = self
                .push_subscribe_park
                .lock()
                .expect("subscribe park lock");
            let park = match armed.as_ref() {
                Some((after, _)) if *after == prior => armed.take().map(|(_, gate)| gate),
                _ => None,
            };
            let handle = if prior == 0 {
                SubscriptionHandle(self.push_label.clone())
            } else {
                SubscriptionHandle(format!("{}-{}", self.push_label, prior + 1))
            };
            (handle, park)
        };
        let scopes = scopes.to_vec();
        Box::pin(async move {
            if let Some(gate) = park {
                gate.notified().await;
            }
            Ok(bifrost_types::PushSubscription::all_succeeded(
                handle, &scopes,
            ))
        })
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.push_log
            .unsubscribed
            .lock()
            .expect("push log lock")
            .push((self.push_label.clone(), handle.clone()));
        let counted = self
            .push_unsubscribe_failures
            .try_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok();
        let refused = self.push_unsubscribe_refused.contains(&handle.0);
        let mut deleted = self.push_deleted.lock().expect("push deleted lock");
        let unknown = self.push_unsubscribe_strict && deleted.contains(&handle.0);
        let fail = counted || refused || unknown;
        if !fail {
            deleted.push(handle.0);
        }
        drop(deleted);
        let park = self
            .push_unsubscribe_park
            .lock()
            .expect("unsubscribe park lock")
            .take();
        Box::pin(async move {
            if let Some(gate) = park {
                gate.notified().await;
            }
            if fail {
                Err(push_teardown_refused())
            } else {
                Ok(())
            }
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
        self.closed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let gate = self.close_gate.clone();
        let fails = self.close_fails;
        let returned = Arc::clone(&self.close_returned);
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            returned.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if fails { Err(close_refused()) } else { Ok(()) }
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

// Silence Checkpoint import when a consumer test only uses the account half.
const _: Option<Checkpoint> = None;
