//! The push reconciler is ONE task for the whole account, and it is the second
//! producer on every scope's lane. Both properties have consequences: a
//! per-scope recovery delay must not be slept off inside it, and it must honor
//! the same terminal tombstones the poll scan does.

mod common;

use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use bifrost_sync::SchedulerConfig;
use bifrost_sync::cancel::Boundary;
use bifrost_sync::control::SyncControl;
use bifrost_sync::cursor::CursorRegistry;
use bifrost_sync::multiplexer::{ScopeToken, ScopeTokens};
use bifrost_sync::push::Reconciler;
use bifrost_sync::scheduler::{BudgetGate, ConcurrencyBudget, Scheduler};
use bifrost_types::{
    Account, AccountErrorBuilder, AccountErrorKind, AccountId, Cause, CursorScope, FolderId,
    RetryHint, ServerCause, ServerErrorKind, SyncEvent,
};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use common::{StubAccount, cursor_for};

const HINTED_DELAY_SECS: u64 = 600;

fn scope(name: &str) -> CursorScope {
    CursorScope::Folder(FolderId(name.into()))
}

/// A rate-limit failure carrying a very long provider hint.
fn throttled() -> bifrost_types::AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited {
            retry_hint: Some(RetryHint::After(std::time::Duration::from_secs(
                HINTED_DELAY_SECS,
            ))),
        }),
    )
    .try_build()
    .expect("valid rate-limited server error")
}

fn empty_tokens() -> ScopeTokens {
    Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Run one `HintPayload::Unknown` sweep over two folder scopes, both of whose
/// drives terminate with the throttled error above, and report which scopes the
/// account was actually asked to drive.
async fn sweep(scope_tokens: ScopeTokens) -> Vec<CursorScope> {
    let scopes = vec![scope("one"), scope("two")];
    let driven: Arc<Mutex<Vec<CursorScope>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&driven);
    let account = StubAccount {
        changes_hook: Some(Arc::new(move |cursor| {
            recorder
                .lock()
                .expect("driven lock")
                .push(cursor.scope.clone());
            vec![SyncEvent::Terminated(throttled())]
        })),
        ..StubAccount::new(scopes.clone())
    };
    let account: Arc<dyn Account> = Arc::new(account);

    let cursors = Arc::new(CursorRegistry::new());
    for scope in &scopes {
        cursors.put(cursor_for(scope, b"live"));
    }

    let (boundary, boundary_view) = Boundary::new();
    let (priority, _priority_rx) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _bandwidth_rx) = tokio::sync::watch::channel(None);
    let account_id = AccountId("reconcile-sweep".into());
    let control = SyncControl::new(account_id.clone(), boundary, priority, bandwidth);
    let (changes_tx, _changes_rx) = broadcast::channel(64);
    let (reopen_tx, _reopen_rx) = mpsc::channel(8);
    let gate = BudgetGate::new(ConcurrencyBudget::default());
    gate.register(account_id.clone());
    let scheduler = Scheduler::new(SchedulerConfig::default(), gate);

    let reconciler = Reconciler {
        account_id,
        account: Arc::new(ArcSwap::from_pointee(account)),
        cursors,
        delivery: Arc::new(bifrost_sync::multiplexer::ChangeDelivery::new(changes_tx)),
        boundary: boundary_view,
        shutdown: CancellationToken::new(),
        control,
        reopen_tx,
        throttles: Arc::new(std::sync::Mutex::new(
            bifrost_sync::recovery::ThrottleBucket::new(),
        )),
        scheduler,
        scope_tokens,
        account_generation: tokio::sync::watch::channel(0).1,
    };

    let ingress = Arc::new(bifrost_sync::push::PushIngress::new(4));
    ingress.request_full();
    ingress.close();
    reconciler.run(ingress).await;
    let observed = driven.lock().expect("driven lock");
    observed.clone()
}

/// A provider `Retry-After` of minutes on one scope used to freeze push
/// reconciliation for the whole account: the `Terminated -> Retry` arm slept
/// the delay out inline, inside the single reconciler task, while the watch
/// channel filled and coalesced behind it. The deadline is already recorded in
/// the shared throttle bucket that every drive path consults, so the sweep
/// skips the throttled scope and keeps going instead.
#[tokio::test(start_paused = true)]
async fn a_throttled_scope_does_not_freeze_the_account_wide_sweep() {
    let started = tokio::time::Instant::now();
    let driven = sweep(empty_tokens()).await;
    let elapsed = started.elapsed();

    assert_eq!(
        driven.len(),
        2,
        "both hinted scopes must be swept: {driven:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(HINTED_DELAY_SECS),
        "the sweep must not sleep off a per-scope provider hint inside the single \
         account-wide reconciler task (elapsed {elapsed:?})"
    );
}

/// A scope whose poll task exited on a terminal verdict leaves a parked
/// tombstone in `scope_tokens`, and the 1s poll scan honors it. The reconciler
/// is the other producer on that lane: without reading the same record, a later
/// push hint re-drove a terminally failed scope, re-broadcasting `Terminated`
/// and spending a wire call that cannot succeed.
#[tokio::test(start_paused = true)]
async fn a_terminally_parked_scope_is_not_re_driven_by_a_later_hint() {
    let tokens = empty_tokens();
    tokens
        .lock()
        .expect("tokens lock")
        .insert(scope("one"), ScopeToken::tombstone());

    let driven = sweep(tokens).await;

    assert_eq!(
        driven,
        vec![scope("two")],
        "only the live scope may be driven; the parked one stays parked"
    );
}

struct Harness {
    ingress: Arc<bifrost_sync::push::PushIngress>,
    boundary: Boundary,
    shutdown: CancellationToken,
    generation: tokio::sync::watch::Sender<u64>,
    reopen: mpsc::Receiver<bifrost_sync::ReopenRequest>,
    driven: Arc<Mutex<Vec<CursorScope>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Harness {
    fn new(error: Option<bifrost_types::AccountError>, stall: bool) -> Self {
        Self::with_capacity(error, stall, 4)
    }
    fn with_capacity(
        error: Option<bifrost_types::AccountError>,
        stall: bool,
        capacity: usize,
    ) -> Self {
        let scopes = vec![scope("one"), scope("two")];
        let driven = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&driven);
        let mut stub = StubAccount::new(scopes.clone());
        stub.changes_hook = Some(Arc::new(move |cursor| {
            recorder.lock().expect("drives").push(cursor.scope.clone());
            error
                .clone()
                .map(SyncEvent::Terminated)
                .into_iter()
                .collect()
        }));
        if stall {
            stub.changes_stall = Some(Arc::new(tokio::sync::Notify::new()));
        }
        let account: Arc<dyn Account> = Arc::new(stub);
        let cursors = Arc::new(CursorRegistry::new());
        for scope in scopes {
            cursors.put(cursor_for(&scope, b"live"));
        }
        let (boundary, view) = Boundary::new();
        let (priority, _) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
        let (bandwidth, _) = tokio::sync::watch::channel(None);
        let account_id = AccountId("work-retention".into());
        let control = SyncControl::new(account_id.clone(), boundary.clone(), priority, bandwidth);
        let (tx, _) = broadcast::channel(8);
        let (reopen_tx, reopen) = mpsc::channel(8);
        let (generation, generation_rx) = tokio::sync::watch::channel(0);
        let gate = BudgetGate::new(ConcurrencyBudget::default());
        gate.register(account_id.clone());
        let shutdown = CancellationToken::new();
        let ingress = Arc::new(bifrost_sync::push::PushIngress::new(capacity));
        let reconciler = Reconciler {
            account_id,
            account: Arc::new(ArcSwap::from_pointee(account)),
            cursors,
            delivery: Arc::new(bifrost_sync::multiplexer::ChangeDelivery::new(tx)),
            boundary: view,
            shutdown: shutdown.clone(),
            control,
            reopen_tx,
            throttles: Arc::new(Mutex::new(bifrost_sync::ThrottleBucket::new())),
            scheduler: Scheduler::new(SchedulerConfig::default(), gate),
            scope_tokens: empty_tokens(),
            account_generation: generation_rx,
        };
        let task = tokio::spawn(reconciler.run(Arc::clone(&ingress)));
        Self {
            ingress,
            boundary,
            shutdown,
            generation,
            reopen,
            driven,
            task,
        }
    }
    async fn settle(&self) {
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }
    fn drives(&self) -> usize {
        self.driven.lock().expect("drives").len()
    }
    async fn close(self) {
        self.shutdown.cancel();
        self.task.await.expect("reconciler joins");
    }
}

#[tokio::test(start_paused = true)]
async fn a_paused_drive_keeps_its_scope_in_the_work_set() {
    use bifrost_sync::BoundaryRequest;
    let harness = Harness::new(None, true);
    harness
        .ingress
        .invalidate(&bifrost_types::HintPayload::SpecificCursorScope(scope(
            "one",
        )));
    harness.settle().await;
    assert_eq!(harness.drives(), 1);
    harness.boundary.set(BoundaryRequest::Pause);
    harness.settle().await;
    assert_eq!(harness.drives(), 1);
    harness.boundary.set(BoundaryRequest::Run);
    harness.settle().await;
    assert_eq!(harness.drives(), 2, "resume must not need a new hint");
    harness.close().await;
}

#[tokio::test(start_paused = true)]
async fn an_account_wide_directive_holds_the_remainder_until_the_generation_moves() {
    let error = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CapabilityChanged),
        Cause::State(bifrost_types::StateCause::CapabilityChanged { delta: None }),
    )
    .try_build()
    .expect("restart");
    let mut harness = Harness::new(Some(error), false);
    harness.ingress.request_full();
    harness.settle().await;
    assert_eq!(harness.drives(), 1);
    assert!(harness.reopen.try_recv().is_ok());
    harness.generation.send_replace(1);
    harness.settle().await;
    assert_eq!(
        harness.drives(),
        2,
        "undispatched sibling must survive repair"
    );
    harness.close().await;
}

#[tokio::test(start_paused = true)]
async fn an_account_wide_hold_clears_at_its_instant_without_a_generation_change() {
    let error = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CapabilityChanged),
        Cause::State(bifrost_types::StateCause::CapabilityChanged { delta: None }),
    )
    .try_build()
    .expect("restart");
    let harness = Harness::new(Some(error), false);
    harness.ingress.request_full();
    harness.settle().await;
    assert_eq!(harness.drives(), 1);
    tokio::time::advance(std::time::Duration::from_secs(29)).await;
    harness.settle().await;
    assert_eq!(harness.drives(), 1);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    harness.settle().await;
    assert_eq!(harness.drives(), 2);
    harness.close().await;
}

#[tokio::test(start_paused = true)]
async fn a_hold_is_not_cleared_by_an_already_published_generation() {
    let error = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CapabilityChanged),
        Cause::State(bifrost_types::StateCause::CapabilityChanged { delta: None }),
    )
    .try_build()
    .expect("restart");
    let harness = Harness::new(Some(error), false);
    harness.generation.send_replace(1);
    harness.ingress.request_full();
    harness.settle().await;
    assert_eq!(harness.drives(), 1);
    tokio::time::advance(std::time::Duration::from_secs(29)).await;
    harness.settle().await;
    assert_eq!(
        harness.drives(),
        1,
        "a stale watch notification must not clear the new hold"
    );
    harness.close().await;
}

#[tokio::test(start_paused = true)]
async fn a_full_reconcile_sweeps_a_registry_larger_than_the_hint_capacity() {
    let harness = Harness::with_capacity(None, false, 1);
    harness.ingress.request_full();
    harness.settle().await;
    assert_eq!(harness.drives(), 2);
    assert_eq!(harness.ingress.coarsened(), 0);
    harness.settle().await;
    assert_eq!(harness.drives(), 2);
    harness.close().await;
}
