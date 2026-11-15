//! A refused push teardown is retried on a timer, not only at the next reopen
//! or `unsubscribe_push`.
//!
//! The refused handle stays registered as an orphan so its server-side
//! subscription remains reachable; the account's retrier retries it each
//! `PushConfig::orphan_teardown_retry_interval`, drops it once the provider
//! accepts, and never touches a subscription the consumer still wants.

mod common;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use bifrost_sync::{EngineConfig, PushConfig, SyncControl, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, Control, CursorScope, FolderId, PushCapability};

use common::{PushLog, StubAccount, StubFactory};

const INTERVAL: Duration = Duration::from_secs(60);

fn inbox() -> Vec<CursorScope> {
    vec![CursorScope::Folder(FolderId("inbox".into()))]
}

fn pushing(log: &Arc<PushLog>, failures: usize) -> Arc<StubAccount> {
    let mut caps = common::caps();
    caps.push = PushCapability::Webhook;
    Arc::new(StubAccount {
        caps,
        push_label: "acct".to_owned(),
        push_log: Arc::clone(log),
        push_unsubscribe_failures: AtomicUsize::new(failures),
        ..StubAccount::new(inbox())
    })
}

fn engine(interval: Option<Duration>) -> SyncEngine {
    let config = EngineConfig {
        push: PushConfig {
            orphan_teardown_retry_interval: interval,
        },
        ..EngineConfig::default()
    };
    SyncEngine::builder()
        .config(config)
        .build()
        .expect("engine")
}

async fn attach(engine: &SyncEngine, id: &AccountId, account: Arc<StubAccount>) -> SyncControl {
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![account]));
    engine.attach(id.clone(), factory).await.expect("attach")
}

/// Yield until `done` holds, bounded by iterations so a state that never
/// arrives fails the test instead of hanging it.
async fn settle(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held");
}

/// Ablation: without the retrier, the orphan waits for a reopen or another
/// `unsubscribe_push`, and the second teardown never arrives.
#[tokio::test(start_paused = true)]
async fn a_refused_teardown_is_retried_on_the_timer_and_then_forgotten() {
    let id = AccountId("orphan-retry".into());
    let log = Arc::new(PushLog::default());
    let engine = engine(Some(INTERVAL));
    attach(&engine, &id, pushing(&log, 1)).await;
    engine
        .subscribe_push(&id, &inbox())
        .await
        .expect("consumer subscribes");
    engine
        .unsubscribe_push(&id)
        .await
        .expect_err("the provider refuses the first teardown");
    assert_eq!(log.unsubscribed().len(), 1);

    tokio::time::sleep(INTERVAL + Duration::from_secs(1)).await;
    settle(|| log.unsubscribed().len() == 2).await;

    // Accepted, so the record is gone: neither a later pass nor the
    // consumer's next teardown hands the handle over again.
    tokio::time::sleep(INTERVAL * 3).await;
    engine.unsubscribe_push(&id).await.expect("nothing left");
    assert_eq!(log.unsubscribed().len(), 2, "{:?}", log.unsubscribed());

    engine.detach(&id).await.expect("detach");
}

/// A retry parked on a stalled provider must not block the account's other
/// push calls: `subscribe_push` takes the reopen lock and still gets through
/// while the retry is parked, and once the retry's backstop bound fires the
/// orphan is retried again on a later pass.
///
/// Ablation: a retrier that holds the reopen lock across its attempt keeps
/// the subscribe waiting on the parked call.
#[tokio::test(start_paused = true)]
async fn a_stalled_retry_does_not_block_the_accounts_push_calls() {
    let id = AccountId("orphan-retry-stalled".into());
    let log = Arc::new(PushLog::default());
    let account = pushing(&log, 1);
    let engine = engine(Some(INTERVAL));
    attach(&engine, &id, Arc::clone(&account)).await;
    engine
        .subscribe_push(&id, &inbox())
        .await
        .expect("consumer subscribes");
    engine
        .unsubscribe_push(&id)
        .await
        .expect_err("the provider refuses the first teardown");
    // The retrier's attempt parks and is never released.
    *account.push_unsubscribe_park.lock().expect("park lock") =
        Some(Arc::new(tokio::sync::Notify::new()));

    tokio::time::sleep(INTERVAL + Duration::from_secs(1)).await;
    settle(|| log.unsubscribed().len() == 2).await;

    tokio::time::timeout(Duration::from_secs(1), engine.subscribe_push(&id, &inbox()))
        .await
        .expect("the parked retry holds no lock the subscribe needs")
        .expect("consumer subscribes again");

    // Past the retry's five-minute backstop, and one more interval.
    tokio::time::sleep(Duration::from_secs(5 * 60) + INTERVAL * 2).await;
    settle(|| log.unsubscribed().len() == 3).await;

    engine.detach(&id).await.expect("detach");
}

/// A paused account makes no provider calls on the engine's own initiative:
/// the orphan waits for resume, then the next pass retries it.
#[tokio::test(start_paused = true)]
async fn a_paused_account_defers_its_orphan_retries_until_resume() {
    let id = AccountId("orphan-retry-paused".into());
    let log = Arc::new(PushLog::default());
    let engine = engine(Some(INTERVAL));
    let control = attach(&engine, &id, pushing(&log, 1)).await;
    engine
        .subscribe_push(&id, &inbox())
        .await
        .expect("consumer subscribes");
    engine
        .unsubscribe_push(&id)
        .await
        .expect_err("the provider refuses the first teardown");
    control.pause().await.expect("an idle account pauses");

    tokio::time::sleep(INTERVAL * 5).await;
    assert_eq!(log.unsubscribed().len(), 1, "{:?}", log.unsubscribed());

    engine.resume_account(&id).expect("resume");
    tokio::time::sleep(INTERVAL + Duration::from_secs(1)).await;
    settle(|| log.unsubscribed().len() == 2).await;

    engine.detach(&id).await.expect("detach");
}

/// A subscription the consumer still wants is live coverage, not an orphan.
#[tokio::test(start_paused = true)]
async fn a_wanted_subscription_is_never_torn_down_by_the_timer() {
    let id = AccountId("orphan-retry-wanted".into());
    let log = Arc::new(PushLog::default());
    let engine = engine(Some(INTERVAL));
    attach(&engine, &id, pushing(&log, 0)).await;
    engine
        .subscribe_push(&id, &inbox())
        .await
        .expect("consumer subscribes");

    tokio::time::sleep(INTERVAL * 5).await;
    assert!(log.unsubscribed().is_empty(), "{:?}", log.unsubscribed());

    engine.detach(&id).await.expect("detach");
}

/// `None` turns the timer off: the orphan waits for the consumer.
#[tokio::test(start_paused = true)]
async fn a_disabled_timer_leaves_the_orphan_to_the_consumer() {
    let id = AccountId("orphan-retry-off".into());
    let log = Arc::new(PushLog::default());
    let engine = engine(None);
    attach(&engine, &id, pushing(&log, 1)).await;
    engine
        .subscribe_push(&id, &inbox())
        .await
        .expect("consumer subscribes");
    engine
        .unsubscribe_push(&id)
        .await
        .expect_err("the provider refuses the first teardown");

    tokio::time::sleep(INTERVAL * 5).await;
    assert_eq!(log.unsubscribed().len(), 1);
    engine
        .unsubscribe_push(&id)
        .await
        .expect("the consumer's retry succeeds");
    assert_eq!(log.unsubscribed().len(), 2);

    engine.detach(&id).await.expect("detach");
}
