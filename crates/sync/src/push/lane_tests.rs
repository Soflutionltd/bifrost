//! Deterministic lane-level recovery and coverage obligations.
use super::lanes::PushLanes;
use super::*;
use crate::{
    cancel::{Boundary, BoundaryRequest},
    multiplexer::{ChangeDelivery, ReopenRequest},
    recovery::ThrottleBucket,
    test_common::{SessionStep as S, StubAccount},
};
use arc_swap::ArcSwap;
use bifrost_types::{
    Account, BackoffPolicy, PushCapability, PushLane, PushLaneKind, PushLaneSpec,
    PushSessionEnd as End, PushSessionEvent as E,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

struct Fixture {
    stub: Arc<StubAccount>,
    ingress: Arc<PushIngress>,
    generation: watch::Sender<u64>,
    topology: watch::Sender<u64>,
    boundary: watch::Sender<BoundaryRequest>,
    health: watch::Receiver<health::PushHealth>,
    recovery: mpsc::Receiver<ReopenRequest>,
    events: tokio::sync::broadcast::Receiver<crate::multiplexer::MultiplexerEvent>,
    throttles: Arc<Mutex<ThrottleBucket>>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}
impl Fixture {
    async fn new(
        kind: PushLaneKind,
        scripts: Vec<Vec<S>>,
        generation_value: u64,
        paused: bool,
        full: bool,
    ) -> Self {
        Self::with_lanes(kind, scripts, generation_value, paused, full, 1).await
    }
    async fn with_lanes(
        kind: PushLaneKind,
        scripts: Vec<Vec<S>>,
        generation_value: u64,
        paused: bool,
        full: bool,
        count: u16,
    ) -> Self {
        let mut stub = StubAccount::new(Vec::new());
        stub.caps.push = PushCapability::InProcess;
        stub.push_lane_specs = (0..count)
            .map(|n| PushLaneSpec {
                lane: PushLane(n),
                kind,
                backoff: BackoffPolicy::new(Duration::from_secs(5), Duration::from_secs(40), false),
            })
            .collect();
        stub.push_sessions = Mutex::new(scripts.into());
        let stub = Arc::new(stub);
        let account: Arc<dyn Account> = Arc::<StubAccount>::clone(&stub);
        let current = Arc::new(ArcSwap::from_pointee(account));
        let (generation, gen_rx) = watch::channel(generation_value);
        let (topology, topo_rx) = watch::channel(0);
        let (b, boundary_view) = Boundary::new();
        let boundary = b.sender();
        if paused {
            boundary.send_replace(BoundaryRequest::Pause);
        }
        let (reopen_tx, recovery) = mpsc::channel(1);
        if full {
            reopen_tx
                .try_send(ReopenRequest::PushRecovery {
                    scope: None,
                    error: restart(),
                    generation: generation_value,
                })
                .unwrap();
        }
        let ingress = Arc::new(PushIngress::new(4));
        let (sender, events) = tokio::sync::broadcast::channel(16);
        let delivery = Arc::new(ChangeDelivery::new(sender));
        let (health_tx, health) = watch::channel(health::PushHealth {
            generation: generation_value,
            lanes: Vec::new(),
        });
        let throttles = Arc::new(Mutex::new(ThrottleBucket::default()));
        let shutdown = CancellationToken::new();
        let lanes = PushLanes {
            account_id: bifrost_types::AccountId("lanes".into()),
            current,
            generation: gen_rx,
            topology: topo_rx,
            ingress: Arc::clone(&ingress),
            delivery,
            reopen_tx,
            throttles: Arc::clone(&throttles),
            boundary: boundary_view,
            shutdown: shutdown.clone(),
            health: health_tx,
            retired: watch::channel(0).0,
            drain_bound: Duration::from_secs(2),
        };
        let task = tokio::spawn(lanes.run());
        settle().await;
        Self {
            stub,
            ingress,
            generation,
            topology,
            boundary,
            health,
            recovery,
            events,
            throttles,
            shutdown,
            task,
        }
    }
    fn opens(&self) -> usize {
        self.stub.push_session_log.lock().unwrap().len()
    }
    async fn finish(self) {
        self.shutdown.cancel();
        self.task.await.unwrap();
    }
}
fn restart() -> bifrost_types::AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::CapabilityChanged,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::CapabilityChanged { delta: None }),
    )
    .try_build()
    .unwrap()
}
fn terminal() -> bifrost_types::AccountError {
    crate::test_common::unsupported(bifrost_types::AccountOperation::PushStream)
}
fn throttle(seconds: u64) -> bifrost_types::AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Server(bifrost_types::ServerErrorKind::RateLimited),
        bifrost_types::Cause::Server(bifrost_types::ServerCause::RateLimited {
            retry_hint: Some(bifrost_types::RetryHint::After(Duration::from_secs(
                seconds,
            ))),
        }),
    )
    .throttle_scope(bifrost_types::ThrottleScope::Account)
    .try_build()
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_lane_failing_restart_account_through_a_long_pause_submits_one_request() {
    let mut f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(restart()))]; 5],
        0,
        true,
        false,
    )
    .await;
    tokio::time::advance(Duration::from_secs(3600)).await;
    settle().await;
    assert_eq!(f.opens(), 1);
    assert!(f.recovery.try_recv().is_err());
    f.boundary.send_replace(BoundaryRequest::Run);
    settle().await;
    assert!(matches!(
        f.recovery.try_recv(),
        Ok(ReopenRequest::PushRecovery { generation: 0, .. })
    ));
    assert!(f.recovery.try_recv().is_err());
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_full_reopen_queue_does_not_stop_session_polling_or_stop() {
    let f = Fixture::new(
        PushLaneKind::Registration,
        vec![vec![
            S::Emit(E::UnitFailed(restart())),
            S::Emit(E::Healthy),
            S::AwaitStop,
        ]],
        0,
        false,
        true,
    )
    .await;
    assert!(matches!(
        f.health.borrow().lanes[0].state,
        health::PushLaneState::Up
    ));
    let stub = Arc::clone(&f.stub);
    f.finish().await;
    assert!(stub.push_session_log.lock().unwrap()[0].ended);
}
#[tokio::test(start_paused = true)]
async fn a_first_session_on_generation_zero_owes_nothing() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Established), S::AwaitStop]],
        0,
        false,
        false,
    )
    .await;
    assert!(!f.ingress.take().full);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_connection_lane_on_a_new_generation_reconciles_at_establishment() {
    let release = Arc::new(tokio::sync::Notify::new());
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![
            S::Park(Arc::clone(&release)),
            S::Emit(E::Established),
            S::AwaitStop,
        ]],
        1,
        false,
        false,
    )
    .await;
    assert!(!f.ingress.take().full);
    release.notify_one();
    settle().await;
    assert!(f.ingress.take().full);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_registration_lane_on_a_new_generation_reconciles_at_lane_start() {
    let f = Fixture::new(
        PushLaneKind::Registration,
        vec![vec![S::AwaitStop]],
        1,
        false,
        false,
    )
    .await;
    assert!(f.ingress.take().full);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_terminal_parked_connection_lane_owes_a_reconcile_on_restart() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![
            vec![S::Emit(E::Failed(terminal()))],
            vec![S::Emit(E::Established), S::AwaitStop],
        ],
        0,
        false,
        false,
    )
    .await;
    assert!(!f.ingress.take().full);
    f.topology.send_modify(|n| *n += 1);
    settle().await;
    assert!(f.ingress.take().full);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_topology_bump_during_the_failing_session_prevents_parking() {
    let release = Arc::new(tokio::sync::Notify::new());
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![
            S::Park(Arc::clone(&release)),
            S::Emit(E::Failed(terminal())),
        ]],
        0,
        false,
        false,
    )
    .await;
    f.topology.send_modify(|n| *n += 1);
    release.notify_one();
    settle().await;
    assert_eq!(f.opens(), 2);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn unit_failures_record_every_throttle_and_incomplete_waits_the_longest_hint() {
    let f = Fixture::new(
        PushLaneKind::Registration,
        vec![vec![
            S::Emit(E::UnitFailed(
                throttle(20)
                    .into_builder()
                    .throttle_scope(bifrost_types::ThrottleScope::CurrentOperation)
                    .try_build()
                    .unwrap(),
            )),
            S::Emit(E::UnitFailed(throttle(10))),
            S::Emit(E::Ended(End::Incomplete)),
        ]],
        0,
        false,
        false,
    )
    .await;
    tokio::time::advance(Duration::from_secs(19)).await;
    settle().await;
    assert_eq!(f.opens(), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(f.opens(), 2);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_hold_recorded_after_establishment_holds_the_next_maintenance_request() {
    let release = Arc::new(tokio::sync::Notify::new());
    let f = Fixture::new(
        PushLaneKind::Registration,
        vec![vec![
            S::Emit(E::Established),
            S::Park(Arc::clone(&release)),
            S::AwaitGate,
            S::AwaitStop,
        ]],
        0,
        false,
        false,
    )
    .await;
    f.throttles.lock().unwrap().record(
        bifrost_types::ThrottleKey::Account(bifrost_types::AccountId("lanes".into())),
        tokio::time::Instant::now() + Duration::from_secs(20),
    );
    release.notify_one();
    settle().await;
    assert!(f.stub.push_session_log.lock().unwrap()[0].gates.is_empty());
    tokio::time::advance(Duration::from_secs(20)).await;
    settle().await;
    assert_eq!(f.stub.push_session_log.lock().unwrap()[0].gates.len(), 1);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn restart_account_waits_for_the_generation_before_backing_off() {
    let mut f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(restart()))]],
        0,
        false,
        false,
    )
    .await;
    assert!(f.recovery.try_recv().is_ok());
    tokio::time::advance(Duration::from_secs(29)).await;
    settle().await;
    assert_eq!(f.opens(), 1);
    f.generation.send_replace(1);
    settle().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert_eq!(f.opens(), 2);
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn a_connection_lane_owes_a_full_reconcile_after_a_lost_session() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![
            vec![S::Emit(E::Established), S::Emit(E::Ended(End::Lost))],
            vec![S::Emit(E::Established), S::AwaitStop],
        ],
        0,
        false,
        false,
    )
    .await;
    assert!(!f.ingress.take().full);
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert!(f.ingress.take().full);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn reconfigure_restarts_at_once_and_owes_a_reconcile() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![
            vec![S::Emit(E::Established), S::Emit(E::Ended(End::Reconfigure))],
            vec![S::Emit(E::Established), S::AwaitStop],
        ],
        0,
        false,
        false,
    )
    .await;
    assert_eq!(f.opens(), 2);
    assert!(f.ingress.take().full);
    let log = f.stub.push_session_log.lock().unwrap().clone();
    assert_eq!(log[0].opened, log[1].opened);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_parked_lane_restarts_on_a_new_generation() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(terminal()))]],
        0,
        false,
        false,
    )
    .await;
    f.generation.send_replace(1);
    settle().await;
    assert_eq!(f.opens(), 2);
    assert_eq!(f.health.borrow().generation, 1);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_healthy_lane_reopens_at_once_on_a_new_generation() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Healthy), S::AwaitStop]],
        0,
        false,
        false,
    )
    .await;
    let instant = tokio::time::Instant::now();
    f.generation.send_replace(1);
    settle().await;
    assert_eq!(f.opens(), 2);
    assert_eq!(f.stub.push_session_log.lock().unwrap()[1].opened, instant);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn an_engine_directive_is_forwarded_once_per_failed_session() {
    let mut f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(restart()))]; 3],
        0,
        false,
        false,
    )
    .await;
    assert!(f.recovery.try_recv().is_ok());
    assert!(f.recovery.try_recv().is_err());
    tokio::time::advance(Duration::from_secs(30)).await;
    settle().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert!(f.recovery.try_recv().is_ok());
    assert!(f.recovery.try_recv().is_err());
    assert_eq!(f.opens(), 2);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_retry_hint_longer_than_the_backoff_is_honored_and_recorded() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(throttle(20)))]],
        0,
        false,
        false,
    )
    .await;
    assert!(
        crate::recovery::account_throttle_wait(
            &f.throttles,
            &bifrost_types::AccountId("lanes".into())
        )
        .is_some()
    );
    tokio::time::advance(Duration::from_secs(19)).await;
    settle().await;
    assert_eq!(f.opens(), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(f.opens(), 2);
    f.finish().await;
}
#[tokio::test(start_paused = true)]
async fn a_retiring_generation_cannot_mutate_the_replacement_lane() {
    let mut f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::AwaitStop]],
        0,
        false,
        false,
    )
    .await;
    // Replace the retirement script with one that emits a directive after stop.
    // AwaitStop ends cleanly, so a second scripted session supplies the stale
    // failure only while generation one is draining.
    let gate = Arc::new(tokio::sync::Notify::new());
    f.stub.push_sessions.lock().unwrap().push_back(vec![
        S::Park(Arc::clone(&gate)),
        S::Emit(E::Failed(restart())),
    ]);
    f.generation.send_replace(1);
    settle().await;
    f.generation.send_replace(2);
    settle().await;
    gate.notify_one();
    settle().await;
    assert!(f.recovery.try_recv().is_err());
    assert_eq!(f.health.borrow().generation, 2);
    assert!(matches!(
        f.health.borrow().lanes[0].state,
        health::PushLaneState::Starting
    ));
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn a_scope_repair_failed_during_a_pause_is_submitted_after_resume() {
    let scope = bifrost_types::CursorScope::Account;
    let error = bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::CursorInvalid,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::CursorInvalid),
    )
    .scope(bifrost_types::ErrorScope::Cursor(scope.clone()))
    .try_build()
    .unwrap();
    let mut f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(error))]],
        0,
        true,
        false,
    )
    .await;
    tokio::time::advance(Duration::from_secs(3600)).await;
    settle().await;
    assert!(f.recovery.try_recv().is_err());
    f.boundary.send_replace(BoundaryRequest::Run);
    settle().await;
    assert!(
        matches!(f.recovery.try_recv(), Ok(ReopenRequest::PushRecovery { scope: Some(s), .. }) if s == scope)
    );
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn the_disconnect_warning_fires_once_across_lanes() {
    let mut f = Fixture::with_lanes(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Ended(End::Lost))]; 2],
        0,
        false,
        false,
        2,
    )
    .await;
    let events: Vec<_> = std::iter::from_fn(|| f.events.try_recv().ok()).collect();
    assert_eq!(events.len(), 1);
    assert!(
        f.health
            .borrow()
            .lanes
            .iter()
            .all(|l| matches!(l.state, health::PushLaneState::Down { .. }))
    );
    f.finish().await;
}

/// `Healthy` arrives once per IDLE round or WebSocket frame. Repeating it on a
/// lane already `Up` must not wake health receivers.
#[tokio::test(start_paused = true)]
async fn repeated_healthy_progress_does_not_wake_health_receivers() {
    let more = Arc::new(tokio::sync::Notify::new());
    let mut f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![
            S::Emit(E::Established),
            S::Emit(E::Healthy),
            S::Park(Arc::clone(&more)),
            S::Emit(E::Healthy),
            S::Emit(E::Healthy),
            S::AwaitStop,
        ]],
        0,
        false,
        false,
    )
    .await;
    assert!(matches!(
        f.health.borrow_and_update().lanes[0].state,
        health::PushLaneState::Up
    ));
    more.notify_one();
    settle().await;
    assert!(
        !f.health.has_changed().unwrap(),
        "an unchanged Up entry must not notify"
    );
    f.finish().await;
}

/// A lane whose generation stops while it waits out a backoff (no live
/// session to drain) still reads `Draining`, like one that drained a session.
#[tokio::test(start_paused = true)]
async fn a_lane_stopped_in_its_backoff_reads_draining() {
    let f = Fixture::new(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Ended(End::Lost))]],
        0,
        false,
        false,
    )
    .await;
    assert!(matches!(
        f.health.borrow().lanes[0].state,
        health::PushLaneState::Down { .. }
    ));
    let health = f.health.clone();
    f.finish().await;
    assert!(matches!(
        health.borrow().lanes[0].state,
        health::PushLaneState::Draining
    ));
}
