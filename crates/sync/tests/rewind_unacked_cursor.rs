//! A lagging acknowledger must not lose the changes it never received.
//!
//! A drive installs a batch's cursor in the registry when it publishes the
//! batch, before anyone acknowledges it. If the batch then falls off the
//! ring, the next drive starts from that unacknowledged cursor and a
//! non-replayable delta token never yields those changes again.
//! `rewind_to_acknowledged` puts the scope back on its durable cursor.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bifrost_sync::{CheckpointStore, EngineConfig, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountFactory, AccountId, Batch, Checkpoint, CursorScope, HintPayload, InvalidationHint,
    PageBoundary, PushSource, SyncEvent, WatchEvent,
};

const ADVANCED: &[u8] = b"advanced-unacked";

fn hour_cadence_config() -> EngineConfig {
    let mut config = EngineConfig::default();
    config.multiplexer.poll_initial = Duration::from_secs(60 * 60);
    config.multiplexer.poll_min = Duration::from_secs(60 * 60);
    config.multiplexer.poll_max = Duration::from_secs(60 * 60);
    config
}

fn unknown_hint() -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::GraphSubscription,
            payload: HintPayload::Unknown,
        },
    }
}

async fn wait_until(label: &str, budget: Duration, mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(budget, async {
        while !ready() {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{label}"));
}

#[tokio::test]
async fn rewind_replays_from_the_acknowledged_cursor() {
    let account_id = AccountId("rewind-unacked".to_owned());
    let scope = CursorScope::Account;
    let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));

    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    let hook_seen = Arc::clone(&seen);
    stub.changes_hook = Some(Arc::new(move |cursor| {
        hook_seen
            .lock()
            .expect("seen lock")
            .push(cursor.server_state.bytes.clone());
        if cursor.server_state.bytes == ADVANCED {
            return Vec::new();
        }
        vec![SyncEvent::Batch(Batch {
            items: Vec::new(),
            page_boundary: PageBoundary::Final,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: Some(Checkpoint::Change(common::cursor_for(
                &cursor.scope,
                ADVANCED,
            ))),
        })]
    }));
    let stub = Arc::new(stub);

    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .config(hour_cadence_config())
        .checkpoints(Arc::clone(&store) as _)
        .build()
        .expect("valid long-cadence config");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    // A numbered receiver that never acknowledges: the published batch stays
    // unacknowledged, exactly as when it is dropped by a lag.
    let _receiver = engine
        .account_changes_stream(&account_id)
        .expect("subscribe");

    let seen_count = || seen.lock().expect("seen lock").len();
    let drive = |count: usize| {
        let engine = &engine;
        let account_id = account_id.clone();
        let seen = Arc::clone(&seen);
        async move {
            engine.invalidation_sink().push(account_id, unknown_hint());
            wait_until("hinted drive runs", Duration::from_secs(5), || {
                seen.lock().expect("seen lock").len() >= count
            })
            .await;
        }
    };

    wait_until(
        "the initial poll drive runs",
        Duration::from_secs(5),
        || seen_count() >= 1,
    )
    .await;
    let durable = store
        .get_change_cursor(&account_id, &scope)
        .await
        .expect("store read")
        .expect("attach persisted the established cursor");
    assert_ne!(durable.server_state.bytes, ADVANCED);

    drive(seen_count() + 1).await;
    assert_eq!(
        seen.lock().expect("seen lock").last().map(Vec::as_slice),
        Some(ADVANCED),
        "publishing advances the in-memory cursor before any acknowledgement",
    );

    let rewound = engine
        .rewind_to_acknowledged(&account_id)
        .await
        .expect("rewind");
    assert_eq!(rewound, 1);

    drive(seen_count() + 1).await;
    assert_eq!(
        seen.lock().expect("seen lock").last(),
        Some(&durable.server_state.bytes),
        "the drive after a rewind starts from the acknowledged cursor",
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}
