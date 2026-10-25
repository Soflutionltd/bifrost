//! Engine-owned push supervision against scripted in-process sessions.
mod common;
use bifrost_sync::{PushLaneState, SyncEngine};
use bifrost_types::{
    AccountId, BackoffPolicy, PushCapability, PushLane, PushLaneKind, PushLaneSpec,
    PushSessionEnd as End, PushSessionEvent as E,
};
use common::{SessionStep as S, StubAccount, StubFactory};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}
fn stub(kind: PushLaneKind, scripts: Vec<Vec<S>>) -> Arc<StubAccount> {
    let mut stub = StubAccount::new(Vec::new());
    stub.caps.push = PushCapability::InProcess;
    stub.push_lane_specs = vec![PushLaneSpec {
        lane: PushLane(0),
        kind,
        backoff: BackoffPolicy::new(Duration::from_secs(5), Duration::from_secs(40), false),
    }];
    stub.push_sessions = Mutex::new(scripts.into());
    Arc::new(stub)
}
async fn attach(stub: &Arc<StubAccount>) -> (SyncEngine, AccountId) {
    let engine = SyncEngine::builder().build().unwrap();
    let id = AccountId("push-lanes".into());
    engine
        .attach(
            id.clone(),
            Arc::new(StubFactory::queue(vec![Arc::clone(stub)])),
        )
        .await
        .unwrap();
    settle().await;
    (engine, id)
}
fn terminal() -> bifrost_types::AccountError {
    common::unsupported(bifrost_types::AccountOperation::PushStream)
}

#[tokio::test(start_paused = true)]
async fn a_terminal_session_failure_is_broadcast_once_and_parks_the_lane() {
    let stub = stub(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(terminal()))]],
    );
    let (engine, id) = attach(&stub).await;
    assert!(matches!(
        engine.push_health(&id).unwrap().borrow().lanes[0].state,
        PushLaneState::Parked { .. }
    ));
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 1);
    engine.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn a_parked_lane_restarts_on_a_push_topology_bump() {
    let stub = stub(
        PushLaneKind::Connection,
        vec![
            vec![S::Emit(E::Failed(terminal()))],
            vec![S::Emit(E::Established), S::AwaitStop],
        ],
    );
    let (engine, id) = attach(&stub).await;
    engine
        .subscribe_push(&id, &[bifrost_types::CursorScope::Account])
        .await
        .unwrap();
    settle().await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 2);
    assert!(matches!(
        engine.push_health(&id).unwrap().borrow().lanes[0].state,
        PushLaneState::Up
    ));
    engine.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn backoff_grows_across_failed_sessions_and_resets_on_healthy() {
    let stub = stub(
        PushLaneKind::Connection,
        vec![
            vec![S::Emit(E::Ended(End::Lost))],
            vec![S::Emit(E::Ended(End::Lost))],
            vec![
                S::Emit(E::Established),
                S::Emit(E::Healthy),
                S::Emit(E::Ended(End::Lost)),
            ],
        ],
    );
    let (engine, _) = attach(&stub).await;
    for (count, seconds) in [(2, 5), (3, 10), (4, 5)] {
        tokio::time::advance(Duration::from_secs(seconds)).await;
        settle().await;
        assert_eq!(stub.push_session_log.lock().unwrap().len(), count);
    }
    engine.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn blind_reconfigures_are_paced_from_the_fourth() {
    let stub = stub(
        PushLaneKind::Connection,
        (0..7)
            .map(|_| vec![S::Emit(E::Ended(End::Reconfigure))])
            .collect(),
    );
    let (engine, _) = attach(&stub).await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 4);
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 5);
    tokio::time::advance(Duration::from_secs(10)).await;
    settle().await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 6);
    engine.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn reconfigure_returns_a_connection_lane_to_starting() {
    let stub = stub(
        PushLaneKind::Connection,
        vec![vec![
            S::Emit(E::Established),
            S::Emit(E::Ended(End::Reconfigure)),
        ]],
    );
    let (engine, id) = attach(&stub).await;
    let health = engine.push_health(&id).unwrap();
    assert!(matches!(
        health.borrow().lanes[0].state,
        PushLaneState::Starting
    ));
    assert!(!health.borrow().any_down());
    engine.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn a_registration_lane_with_a_retired_unit_reads_down() {
    let stub = stub(
        PushLaneKind::Registration,
        vec![vec![
            S::Emit(E::Established),
            S::Emit(E::UnitFailed(terminal())),
            S::Emit(E::Ended(End::Incomplete)),
        ]],
    );
    let (engine, id) = attach(&stub).await;
    assert!(engine.push_health(&id).unwrap().borrow().any_down());
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert!(engine.push_health(&id).unwrap().borrow().any_down());
    engine.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn detach_stops_sessions_and_drains_their_cleanup_before_close() {
    let stub = stub(
        PushLaneKind::Connection,
        vec![vec![
            S::Emit(E::Established),
            S::AwaitStopFor(Duration::from_secs(1)),
        ]],
    );
    let (engine, id) = attach(&stub).await;
    engine.detach(&id).await.unwrap();
    let log = stub.push_session_log.lock().unwrap();
    assert!(log[0].stopped && log[0].ended);
    assert_eq!(stub.closed.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[tokio::test(start_paused = true)]
async fn a_drain_longer_than_its_bound_is_dropped_before_the_worker_deadline() {
    let stub = stub(
        PushLaneKind::Connection,
        vec![vec![
            S::Emit(E::Established),
            S::AwaitStopFor(Duration::from_secs(30)),
        ]],
    );
    let (engine, id) = attach(&stub).await;
    let start = tokio::time::Instant::now();
    engine.detach(&id).await.unwrap();
    assert!(start.elapsed() < Duration::from_secs(5));
    let log = stub.push_session_log.lock().unwrap();
    assert!(log[0].stopped && !log[0].ended);
}
#[tokio::test(start_paused = true)]
async fn reattach_closes_the_old_account_only_after_its_lanes_drained() {
    let old = stub(
        PushLaneKind::Connection,
        vec![vec![
            S::Emit(E::Established),
            S::AwaitStopFor(Duration::from_secs(1)),
        ]],
    );
    let new = stub(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Established), S::AwaitStop]],
    );
    let engine = SyncEngine::builder().build().unwrap();
    let id = AccountId("cutover".into());
    engine
        .attach(
            id.clone(),
            Arc::new(StubFactory::queue(vec![Arc::clone(&old), Arc::clone(&new)])),
        )
        .await
        .unwrap();
    settle().await;
    engine.reattach(&id).await.unwrap();
    assert!(old.push_session_log.lock().unwrap()[0].ended);
    assert_eq!(old.closed.load(std::sync::atomic::Ordering::SeqCst), 1);
    settle().await;
    assert!(matches!(
        engine.push_health(&id).unwrap().borrow().lanes[0].state,
        PushLaneState::Up
    ));
    engine.shutdown().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn lanes_are_not_run_for_an_account_without_push() {
    let mut raw = StubAccount::new(Vec::new());
    raw.push_lane_specs = vec![PushLaneSpec {
        lane: PushLane(0),
        kind: PushLaneKind::Connection,
        backoff: BackoffPolicy::new(Duration::from_secs(1), Duration::from_secs(1), false),
    }];
    let stub = Arc::new(raw);
    let (engine, id) = attach(&stub).await;
    assert!(stub.push_session_log.lock().unwrap().is_empty());
    assert!(engine.push_health(&id).unwrap().borrow().lanes.is_empty());
    engine.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn restart_account_failures_space_reattaches_by_the_carried_backoff() {
    let restart = bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::CapabilityChanged,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::CapabilityChanged { delta: None }),
    )
    .try_build()
    .unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let accounts = (0..5)
        .map(|_| {
            let mut account = Arc::try_unwrap(stub(
                PushLaneKind::Connection,
                vec![vec![S::Emit(E::Failed(restart.clone()))]],
            ))
            .ok()
            .unwrap();
            account.push_session_log = Arc::clone(&opens);
            Arc::new(account)
        })
        .collect();
    let engine = SyncEngine::builder().build().unwrap();
    let id = AccountId("paced-push-restart".into());
    engine
        .attach(id, Arc::new(StubFactory::queue(accounts)))
        .await
        .unwrap();
    settle().await;
    for (index, delay) in [5, 10, 20].into_iter().enumerate() {
        assert_eq!(opens.lock().unwrap().len(), index + 1);
        tokio::time::advance(Duration::from_secs(delay)).await;
        settle().await;
        let log = opens.lock().unwrap();
        assert_eq!(log.len(), index + 2);
        assert_eq!(
            log[index + 1].opened - log[index].opened,
            Duration::from_secs(delay)
        );
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn push_health_reports_lane_states_to_a_late_subscriber() {
    let stub = stub(
        PushLaneKind::Connection,
        vec![
            vec![S::Emit(E::Ended(End::Lost))],
            vec![S::Emit(E::Established), S::AwaitStop],
        ],
    );
    let (engine, id) = attach(&stub).await;
    let health = engine.push_health(&id).unwrap();
    assert!(matches!(
        health.borrow().lanes[0].state,
        PushLaneState::Down {
            consecutive_failures: 1,
            ..
        }
    ));
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    let late = engine.push_health(&id).unwrap();
    assert!(matches!(late.borrow().lanes[0].state, PushLaneState::Up));
    engine.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn unsubscribe_push_and_the_orphan_retrier_bump_the_push_topology() {
    let mut raw = Arc::try_unwrap(stub(
        PushLaneKind::Connection,
        vec![vec![S::Emit(E::Failed(terminal()))]; 6],
    ))
    .ok()
    .unwrap();
    raw.push_unsubscribe_failures = std::sync::atomic::AtomicUsize::new(1);
    let stub = Arc::new(raw);
    let config = bifrost_sync::EngineConfig {
        push: bifrost_sync::PushConfig {
            orphan_teardown_retry_interval: Some(Duration::from_secs(60)),
        },
        ..Default::default()
    };
    let engine = SyncEngine::builder().config(config).build().unwrap();
    let id = AccountId("topology-orphans".into());
    engine
        .attach(
            id.clone(),
            Arc::new(StubFactory::queue(vec![Arc::clone(&stub)])),
        )
        .await
        .unwrap();
    settle().await;
    engine
        .subscribe_push(&id, &[bifrost_types::CursorScope::Account])
        .await
        .unwrap();
    settle().await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 2);
    engine.unsubscribe_push(&id).await.unwrap_err();
    settle().await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 3);
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(stub.push_session_log.lock().unwrap().len(), 4);
    engine.shutdown().await.unwrap();
}
