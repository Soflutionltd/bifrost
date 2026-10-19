//! Slot-lifetime bandwidth ownership, without protocol I/O.
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bifrost_net::{BandwidthDirection, BandwidthRegistry};
use bifrost_sync::SyncEngine;
use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, Control, OpenedAccount,
};

struct Factory {
    registry: Arc<BandwidthRegistry>,
    opens: AtomicUsize,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl AccountFactory for Factory {
    fn open(&self, id: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let nth = self.opens.fetch_add(1, Ordering::SeqCst);
        let registry = Arc::clone(&self.registry);
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            if nth == 0 {
                // No factory owner survives this lookup. Only the engine can
                // keep this distinguishable policy alive for the next lookup.
                registry.budget(id.clone()).set_cap(Some(99));
                assert_eq!(registry.budget(id.clone()).cap(), Some(99));
                registry.budget(id.clone()).set_cap(None);
            }
            let budget = registry.budget(id);
            if nth == 0 {
                assert_eq!(
                    budget.cap(),
                    None,
                    "first attach resets the policy before open"
                );
            } else {
                assert_eq!(
                    budget.cap(),
                    Some(7),
                    "replacement sees the slot cap before open"
                );
                let reservation = budget.reserve(BandwidthDirection::Outbound, 7);
                assert!(
                    reservation.ready_at() > tokio::time::Instant::now(),
                    "replacement reuses the old schedule"
                );
                drop(reservation);
                started.notify_one();
                release.notified().await;
                assert_eq!(
                    budget.cap(),
                    Some(11),
                    "control reaches the budget during open"
                );
            }
            let account: Arc<dyn Account> = Arc::new(common::StubAccount::new(vec![]));
            Ok(OpenedAccount::complete(account))
        })
    }
}

#[tokio::test(start_paused = true)]
async fn slot_budget_precedes_open_survives_reopen_and_drops_on_detach() {
    let registry = Arc::new(BandwidthRegistry::new());
    let id = AccountId("bandwidth".into());
    let engine = Arc::new(
        SyncEngine::builder()
            .with_bandwidth_registry(Arc::clone(&registry))
            .build()
            .expect("engine"),
    );
    let factory = Arc::new(Factory {
        registry: Arc::clone(&registry),
        opens: AtomicUsize::new(0),
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let control = engine
        .attach(id.clone(), Arc::<Factory>::clone(&factory))
        .await
        .expect("attach");
    control.bandwidth_cap(Some(7));
    // Keep the executor runnable, so paused time cannot advance the schedule.
    for _ in 0..100 {
        if registry.budget(id.clone()).cap() == Some(7) {
            break;
        }
        tokio::task::yield_now().await;
    }
    let budget = registry.budget(id.clone());
    assert_eq!(budget.cap(), Some(7));
    let grant = budget.admit(BandwidthDirection::Outbound, 7).await;
    grant.finish(7);
    drop(budget);
    let reopening = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        tokio::spawn(async move { engine.reattach(&id).await })
    };
    factory.started.notified().await;
    control.bandwidth_cap(Some(11));
    for _ in 0..100 {
        if registry.budget(id.clone()).cap() == Some(11) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(registry.budget(id.clone()).cap(), Some(11));
    factory.release.notify_one();
    reopening.await.expect("reopen task").expect("reattach");
    assert_eq!(registry.budget(id.clone()).cap(), Some(11));
    engine.detach(&id).await.expect("detach");
    assert_eq!(
        registry.budget(id).cap(),
        None,
        "no engine strong owner remains"
    );
}
