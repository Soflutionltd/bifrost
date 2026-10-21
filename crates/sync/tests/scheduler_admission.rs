//! Admission control: what `Scheduler::admit` promises the engine.
//!
//! The three properties here are the ones the priority tests cannot see,
//! because those only pull from pre-populated lanes and never involve the
//! budget at all:
//!
//! 1. A blocked request waits in a SCHEDULER lane, so it still counts
//!    against `lane_capacity`.
//! 2. A later `Foreground` request overtakes an earlier `Background` one.
//!    Arrival order is deliberately adverse - the background requests are
//!    queued FIRST - because a symmetric test cannot distinguish priority
//!    selection from the semaphore's own FIFO.
//! 3. One account saturating its per-account sub-pool does not stall a
//!    different account, even when the stalled account's request sits in a
//!    higher lane. That is the property a single-head dispatcher loses.
//!
//! The last test drives the real engine under a budget that admits exactly
//! one sync operation at a time: admission that is correct in isolation and
//! deadlocks the engine is the failure that matters.

mod common;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::scheduler::lanes::WorkKind;
use bifrost_sync::scheduler::{BudgetGate, ConcurrencyBudget, Scheduler};
use bifrost_sync::{EngineConfig, SchedulerConfig, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope, ObjectType, Priority};

/// One sync permit and one mutation permit per account, plenty of global
/// room: the per-account sync pool is the contended resource.
fn single_sync_permit() -> ConcurrencyBudget {
    ConcurrencyBudget {
        per_account: 2,
        global: 64,
        mutation_share_num: 1,
        mutation_share_den: 2,
    }
}

fn scheduler(budget: ConcurrencyBudget) -> Scheduler {
    Scheduler::new(
        SchedulerConfig {
            starvation_floor: 64,
        },
        BudgetGate::new(budget),
    )
}

fn admission_depth(scheduler: &Scheduler) -> usize {
    scheduler
        .admission_snapshot()
        .iter()
        .map(|lane| lane.depth)
        .sum()
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

/// Queue historical work first on the same account and budget pool. If lane
/// selection ignores work kind, FIFO grants the backfill and this test fails.
#[tokio::test]
async fn a_live_poll_overtakes_an_earlier_backfill_on_the_same_account() {
    for priority in [Priority::Foreground, Priority::Normal, Priority::Background] {
        let scheduler = scheduler(single_sync_permit());
        let account = AccountId("work-kind-order".into());
        let held = scheduler
            .admit(account.clone(), priority, WorkKind::Sync)
            .await
            .expect("hold the only sync permit");

        let backfill = scheduler.admit(account.clone(), priority, WorkKind::Backfill);
        tokio::pin!(backfill);
        assert!(futures::poll!(backfill.as_mut()).is_pending());
        assert_eq!(admission_depth(&scheduler), 1, "backfill queued first");

        let poll = scheduler.admit(account, priority, WorkKind::Sync);
        tokio::pin!(poll);
        assert!(futures::poll!(poll.as_mut()).is_pending());
        assert_eq!(admission_depth(&scheduler), 2, "both queued before release");
        drop(held);

        // Poll both futures: under account-only FIFO the backfill becomes
        // ready first. Holding the winner's permit makes the order observable.
        let poll_permit = tokio::select! {
            result = &mut backfill => {
                let _permit = result.expect("backfill admission");
                panic!("backfill queued first but the live poll must win");
            }
            result = &mut poll => result.expect("live poll admission"),
        };
        assert!(futures::poll!(backfill.as_mut()).is_pending());
        drop(poll_permit);
        let _backfill_permit = backfill.await.expect("backfill eventually admitted");
    }
}

#[tokio::test]
async fn a_blocked_admission_waits_in_the_scheduler_lane() {
    let scheduler = scheduler(single_sync_permit());
    let account = AccountId("lane-depth".into());

    let held = scheduler
        .admit(account.clone(), Priority::Normal, WorkKind::Sync)
        .await
        .expect("the free sync permit is granted");
    assert_eq!(
        admission_depth(&scheduler),
        0,
        "a granted request leaves the lanes"
    );

    let waiters: Vec<_> = (0..3)
        .map(|_| {
            let scheduler = scheduler.clone();
            let account = account.clone();
            tokio::spawn(async move {
                scheduler
                    .admit(account, Priority::Background, WorkKind::Sync)
                    .await
                    .map(|_| ())
            })
        })
        .collect();

    wait_until(
        "blocked admissions must be visible as scheduler lane depth, not as \
         invisible semaphore waiters",
        Duration::from_secs(5),
        || admission_depth(&scheduler) == 3,
    )
    .await;

    drop(held);
    for waiter in waiters {
        waiter
            .await
            .expect("waiter task")
            .expect("waiter is admitted once the permit frees");
    }
    assert_eq!(admission_depth(&scheduler), 0, "the lanes drain");
}

#[tokio::test]
async fn a_later_foreground_admission_overtakes_earlier_background_ones() {
    let scheduler = scheduler(single_sync_permit());
    let account = AccountId("overtake".into());
    let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    // Hold the only sync permit, so everything below queues.
    let held = scheduler
        .admit(account.clone(), Priority::Normal, WorkKind::Sync)
        .await
        .expect("the free sync permit is granted");

    let mut tasks = Vec::new();
    for tag in ["background-1", "background-2"] {
        let scheduler = scheduler.clone();
        let account = account.clone();
        let order = Arc::clone(&order);
        tasks.push(tokio::spawn(async move {
            let permit = scheduler
                .admit(account, Priority::Background, WorkKind::Sync)
                .await
                .expect("background admission");
            order.lock().expect("order lock").push(tag);
            drop(permit);
        }));
    }
    wait_until(
        "both background requests must be queued before the foreground one \
         arrives, or the test proves nothing about overtaking",
        Duration::from_secs(5),
        || admission_depth(&scheduler) == 2,
    )
    .await;

    {
        let scheduler = scheduler.clone();
        let account = account.clone();
        let order = Arc::clone(&order);
        tasks.push(tokio::spawn(async move {
            let permit = scheduler
                .admit(account, Priority::Foreground, WorkKind::Sync)
                .await
                .expect("foreground admission");
            order.lock().expect("order lock").push("foreground");
            drop(permit);
        }));
    }
    wait_until(
        "the foreground request must be queued too",
        Duration::from_secs(5),
        || admission_depth(&scheduler) == 3,
    )
    .await;

    drop(held);
    for task in tasks {
        task.await.expect("admission task");
    }

    assert_eq!(
        *order.lock().expect("order lock"),
        vec!["foreground", "background-1", "background-2"],
        "the foreground request must be served first despite arriving last, \
         and the background pair must keep its own FIFO order"
    );
}

#[tokio::test]
async fn one_saturated_account_does_not_stall_another() {
    let scheduler = scheduler(single_sync_permit());
    let busy = AccountId("busy".into());
    let idle = AccountId("idle".into());

    let held = scheduler
        .admit(busy.clone(), Priority::Normal, WorkKind::Sync)
        .await
        .expect("busy account takes its only sync permit");

    // A FOREGROUND request for the saturated account: the most preferred
    // waiter in the scheduler, and ungrantable. A dispatcher that only ever
    // considers the head class blocks here forever.
    let blocked = {
        let scheduler = scheduler.clone();
        let busy = busy.clone();
        tokio::spawn(async move {
            scheduler
                .admit(busy, Priority::Foreground, WorkKind::Sync)
                .await
                .map(|_| ())
        })
    };
    wait_until(
        "the saturated account's foreground request must be queued",
        Duration::from_secs(5),
        || admission_depth(&scheduler) == 1,
    )
    .await;

    // A BULK request for a different account, whose own sub-pool is free.
    let other = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.admit(idle, Priority::Bulk, WorkKind::Sync),
    )
    .await
    .expect("a second account must not wait behind a saturated one")
    .expect("second account admission");

    assert!(
        !blocked.is_finished(),
        "the saturated account's request is still correctly waiting"
    );
    drop(other);
    drop(held);
    blocked
        .await
        .expect("blocked task")
        .expect("admitted once the busy account frees its permit");
}

/// Deferred inventory fusion is a cold-start whole-scope walk and one of
/// the heaviest things the engine does. It ran with no admission at all,
/// so it was concurrent with every other work path regardless of the caps.
///
/// The observable is the walk itself: with the account's only sync permit
/// held by this test, `inventory_stream` must not be called.
#[tokio::test]
async fn deferred_inventory_fusion_waits_for_its_admission() {
    let account_id = AccountId("deferred-fusion-admission".to_owned());
    let scope = CursorScope::Type(ObjectType::Email);
    let walks = Arc::new(AtomicUsize::new(0));

    let mut stub = common::StubAccount::new(vec![scope]);
    stub.establishment = |_| bifrost_types::CursorEstablishment::EstablishViaInventory;
    let hook_walks = Arc::clone(&walks);
    stub.inventory_hook = Some(Arc::new(move |_| {
        hook_walks.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    }));
    let stub = Arc::new(stub);

    let mut config = EngineConfig {
        budget: single_sync_permit(),
        ..EngineConfig::default()
    };
    config.multiplexer.poll_initial = Duration::from_secs(60 * 60);
    config.multiplexer.poll_min = Duration::from_secs(60 * 60);
    config.multiplexer.poll_max = Duration::from_secs(60 * 60);

    let engine = SyncEngine::builder()
        .config(config)
        .build()
        .expect("valid single-permit config");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    // Taken after attach: `BudgetGate::register` installs fresh per-account
    // semaphores, so a permit taken earlier would be against an orphan.
    let held = engine
        .scheduler()
        .admit(account_id.clone(), Priority::Normal, WorkKind::Sync)
        .await
        .expect("the account's only sync permit");

    // The fusion task parks until a real subscriber exists, so nothing is
    // raced away before this point.
    let _events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        walks.load(Ordering::SeqCst),
        0,
        "deferred inventory fusion must wait for a budget permit before it \
         touches the wire"
    );

    drop(held);
    wait_until(
        "the fusion walk must run once the permit frees",
        Duration::from_secs(10),
        || walks.load(Ordering::SeqCst) > 0,
    )
    .await;

    engine.detach(&account_id).await.expect("detach succeeds");
}

#[tokio::test]
async fn the_engine_runs_every_scope_under_a_single_permit_budget() {
    // Two scopes, one sync permit for the account: every poll drive, the
    // backfill orchestrator and the push sweep now contend for the same
    // permit. The property is liveness - a permit held across a drive must
    // be released on every path, and admission must not sit inside a drive
    // lease.
    let account_id = AccountId("single-permit-engine".to_owned());
    let scopes = vec![
        CursorScope::Type(ObjectType::Email),
        CursorScope::Type(ObjectType::CalendarEvent),
    ];
    let drives = Arc::new(AtomicUsize::new(0));
    let per_scope: Arc<Mutex<std::collections::HashMap<CursorScope, usize>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));

    let mut stub = common::StubAccount::new(scopes.clone());
    let hook_drives = Arc::clone(&drives);
    let hook_per_scope = Arc::clone(&per_scope);
    stub.changes_hook = Some(Arc::new(move |cursor| {
        hook_drives.fetch_add(1, Ordering::SeqCst);
        *hook_per_scope
            .lock()
            .expect("per-scope lock")
            .entry(cursor.scope.clone())
            .or_insert(0_usize) += 1;
        Vec::new()
    }));
    let stub = Arc::new(stub);

    let mut config = EngineConfig {
        budget: single_sync_permit(),
        ..EngineConfig::default()
    };
    config.multiplexer.poll_initial = Duration::from_secs(60 * 60);
    config.multiplexer.poll_min = Duration::from_secs(60 * 60);
    config.multiplexer.poll_max = Duration::from_secs(60 * 60);

    let engine = SyncEngine::builder()
        .config(config)
        .build()
        .expect("valid single-permit config");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    wait_until(
        "both scopes must complete a poll drive even though only one may hold \
         the sync permit at a time",
        Duration::from_secs(10),
        || {
            let calls = per_scope.lock().expect("per-scope lock");
            scopes.iter().all(|scope| calls.contains_key(scope))
        },
    )
    .await;

    engine.detach(&account_id).await.expect("detach succeeds");
}
