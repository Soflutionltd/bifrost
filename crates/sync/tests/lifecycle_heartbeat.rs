//! Lifecycle pacing through the real engine with deterministic provider scripts.
mod common;

use bifrost_sync::SyncEngine;
use bifrost_types::{AccountId, ScopeLifecycleEvent};
use std::{sync::Arc, time::Duration};

async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

async fn check_delays(scripts: Vec<Vec<ScopeLifecycleEvent>>, delays: &[u64]) {
    let mut stub = common::StubAccount::new(Vec::new());
    stub.lifecycle_scripts = std::sync::Mutex::new(scripts.into());
    let stub = Arc::new(stub);
    let engine = SyncEngine::builder().build().expect("engine builds");
    engine
        .attach(
            AccountId("heartbeat".into()),
            Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)])),
        )
        .await
        .expect("attach");
    settle().await;
    assert_eq!(stub.lifecycle_opens.lock().expect("opens").len(), 1);
    for (index, delay) in delays.iter().enumerate() {
        tokio::time::advance(Duration::from_secs(*delay) - Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(stub.lifecycle_opens.lock().expect("opens").len(), index + 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        let opens = stub.lifecycle_opens.lock().expect("opens");
        assert_eq!(opens.len(), index + 2);
        assert_eq!(opens[index + 1] - opens[index], Duration::from_secs(*delay));
    }
    engine.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_polled_heartbeat_resets_the_lifecycle_backoff() {
    let failure = || vec![ScopeLifecycleEvent::Terminated(common::close_refused())];
    check_delays(
        vec![
            failure(),
            failure(),
            failure(),
            vec![ScopeLifecycleEvent::Polled],
        ],
        &[30, 60, 120, 30],
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_empty_lifecycle_stream_keeps_growing_its_backoff() {
    check_delays(Vec::new(), &[30, 60, 120, 240, 480, 480]).await;
}

#[tokio::test(start_paused = true)]
async fn a_lifecycle_restart_account_loop_is_paced_across_generations() {
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, StateCause, SyncStateErrorKind,
    };
    let error = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged { delta: None }),
    )
    .try_build()
    .expect("classified restart");
    let opens = Arc::new(std::sync::Mutex::new(Vec::new()));
    let accounts = (0..5)
        .map(|_| {
            let mut stub = common::StubAccount::new(Vec::new());
            stub.lifecycle_opens = Arc::clone(&opens);
            stub.lifecycle_scripts = std::sync::Mutex::new(
                vec![vec![ScopeLifecycleEvent::Terminated(error.clone())]].into(),
            );
            Arc::new(stub)
        })
        .collect();
    let engine = SyncEngine::builder().build().expect("engine builds");
    engine
        .attach(
            AccountId("restart-heartbeat".into()),
            Arc::new(common::StubFactory::queue(accounts)),
        )
        .await
        .expect("attach");
    settle().await;
    for (index, delay) in [30, 60, 120].into_iter().enumerate() {
        assert_eq!(opens.lock().expect("opens").len(), index + 1);
        tokio::time::advance(Duration::from_secs(delay)).await;
        settle().await;
        let opens = opens.lock().expect("opens");
        assert_eq!(opens.len(), index + 2);
        assert_eq!(opens[index + 1] - opens[index], Duration::from_secs(delay));
    }
    engine.shutdown().await.expect("shutdown");
}
