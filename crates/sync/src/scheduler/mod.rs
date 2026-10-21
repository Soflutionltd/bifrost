//! Four-lane scheduler.
//!
//! The scheduler is a *gate*, not an executor. Tokio's runtime executes
//! futures; the scheduler decides which `Account` operation gets to
//! acquire concurrency tokens, in what order. Lanes are
//! `Foreground` > `Normal` > `Background` > `Bulk` with a starvation
//! floor: after N consecutive higher-lane pulls, the puller takes one
//! from a lower lane regardless.
//!
//! Concurrency caps are enforced by `budget::BudgetGate` (a layered
//! `tokio::sync::Semaphore` pair: per-account and global).
//!
//! Engine poll, push, backfill, and mutation work enters through
//! [`Scheduler::admit`]. Admission is NOT the `submit`/`pull` work-item
//! path: it has its own four scheduler-owned lanes and a single
//! dispatcher task that selects the next request only when the matching
//! [`BudgetGate`] permit can actually be granted. That ordering is the
//! whole point of the gate:
//!
//! - A blocked request waits in a bounded scheduler lane, never in the
//!   semaphore's FIFO. Requests that had already been dequeued and
//!   parked on the semaphore stopped counting against `lane_capacity`,
//!   which made the "bounded lanes" claim vacuous.
//! - Because nothing is parked on the semaphore, a `Foreground` request
//!   arriving after a `Background` one still runs first: the dispatcher
//!   re-plans when a strictly higher lane gains work, rather than
//!   inheriting the semaphore's arrival order.
//!
//! The dispatcher considers every distinct `(account, kind)` budget
//! class present in the lanes at once, not just the head. A single-head
//! dispatcher would block the whole engine behind one account whose
//! per-account sub-pool is exhausted, which is exactly the cross-account
//! starvation `BudgetGate`'s per-account layer exists to prevent. It re-plans
//! on a strictly better lane or a newly present class, and deliberately NOT on
//! an arrival that is neither - restarting the in-flight acquisitions on every
//! submission starves them.
//!
//! The invariant that keeps all of the above true, stated so it is not
//! undone by a later simplification: **nothing may park on the `BudgetGate`
//! semaphore except the dispatcher.** An `admit` caller that awaits the
//! semaphore itself relocates the unboundedness rather than removing it and
//! restores arrival order in place of priority - which is precisely the
//! implementation this design replaced.
//!
//! A refused admission is TRANSIENT, never terminal: `admit` errors when its
//! lane is full or the scheduler is stopping, and both clear by themselves. A
//! caller must back off (the poll loop steps its cadence) or skip just that
//! scope (the push sweep). Returning from a poll loop on an admission error
//! retires that scope's polling forever for a condition that heals, which was
//! a real regression here.

pub mod budget;
pub mod lanes;

pub use budget::{BudgetGate, BudgetPermit, ConcurrencyBudget};
pub use lanes::{LaneQueue, LaneSnapshot, WorkItem, WorkKind};

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::{BoxFuture, select_all};
use tokio::sync::{Notify, oneshot};

use bifrost_types::{AccountId, Priority};

use crate::types::SchedulerConfig;

/// Number of priority lanes. Mirrors `Priority`'s four variants.
const LANES: usize = 4;

/// Public scheduler handle.
///
/// Holds one `LaneQueue` per priority plus the starvation counter, the
/// admission lanes and the budget gate. Engine work uses `admit`;
/// external consumers keep the `submit` / `pull` work-item path, whose
/// non-blocking `pull` semantics are unchanged.
#[derive(Debug, Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
    /// Dropped with the last handle; stops the dispatcher task so it
    /// does not outlive the engine that created it.
    _life: Arc<SchedulerLife>,
}

#[derive(Debug)]
struct SchedulerLife {
    inner: Arc<SchedulerInner>,
}

impl Drop for SchedulerLife {
    fn drop(&mut self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        self.inner.stop_notify.notify_waiters();
    }
}

#[derive(Debug)]
struct SchedulerInner {
    foreground: LaneQueue,
    normal: LaneQueue,
    background: LaneQueue,
    bulk: LaneQueue,
    starvation_floor: u32,
    lane_capacity: usize,
    state: Mutex<SchedulerState>,
    admission: Mutex<AdmissionState>,
    budget: BudgetGate,
    submitted: Notify,
    admitted: Notify,
    stop_notify: Notify,
    stopped: AtomicBool,
    dispatcher_started: AtomicBool,
}

#[derive(Debug, Default)]
struct SchedulerState {
    /// Count of consecutive Foreground/Normal items pulled. Reset
    /// whenever a Background or Bulk item is pulled. When the count
    /// hits `starvation_floor` the next `pull` is forced to take from
    /// the lower lanes.
    higher_consecutive: u32,
}

/// One queued admission request. The permit travels back over `tx`, so
/// dropping the `admit` future closes the channel and the dispatcher
/// prunes the entry without ever spending a permit on it.
#[derive(Debug)]
struct AdmissionRequest {
    account: AccountId,
    kind: WorkKind,
    tx: oneshot::Sender<BudgetPermit>,
}

#[derive(Debug, Default)]
struct AdmissionState {
    lanes: [VecDeque<AdmissionRequest>; LANES],
    higher_consecutive: u32,
}

fn lane_index(priority: Priority) -> usize {
    match priority {
        Priority::Foreground => 0,
        Priority::Normal => 1,
        Priority::Background => 2,
        Priority::Bulk => 3,
        // `Priority` is `#[non_exhaustive]`; an unknown variant lands in
        // the normal lane, matching `Scheduler::lane`.
        _ => 1,
    }
}

fn lane_priority(index: usize) -> Priority {
    match index {
        0 => Priority::Foreground,
        2 => Priority::Background,
        3 => Priority::Bulk,
        _ => Priority::Normal,
    }
}

impl AdmissionState {
    /// Drop requests whose caller has gone away.
    fn prune(&mut self) {
        for lane in &mut self.lanes {
            lane.retain(|request| !request.tx.is_closed());
        }
    }

    /// Lane visit order for the next grant. When the starvation floor
    /// has been reached the lower lanes are visited first, exactly as
    /// `Scheduler::try_pull` does for work items.
    fn visit_order(&self, floor: u32) -> ([usize; LANES], bool) {
        if self.higher_consecutive >= floor {
            ([3, 2, 0, 1], true)
        } else {
            ([0, 1, 2, 3], false)
        }
    }

    fn record_grant(&mut self, lane: usize, floor_hit: bool) {
        if lane >= 2 {
            self.higher_consecutive = 0;
        } else {
            if floor_hit {
                self.higher_consecutive = 0;
            }
            self.higher_consecutive = self.higher_consecutive.saturating_add(1);
        }
    }

    /// Distinct budget classes present, in grant-preference order.
    fn classes(&self, floor: u32) -> Vec<(AccountId, WorkKind)> {
        let (order, _) = self.visit_order(floor);
        let mut out: Vec<(AccountId, WorkKind)> = Vec::new();
        for lane in order {
            for request in &self.lanes[lane] {
                let class = (request.account.clone(), request.kind);
                if !out.contains(&class) {
                    out.push(class);
                }
            }
        }
        out
    }

    /// Lane index of the most preferred non-empty lane, if any.
    fn best_lane(&self, floor: u32) -> Option<usize> {
        let (order, _) = self.visit_order(floor);
        order.into_iter().find(|lane| !self.lanes[*lane].is_empty())
    }

    fn take_for_class(
        &mut self,
        account: &AccountId,
        kind: WorkKind,
        floor: u32,
    ) -> Option<AdmissionRequest> {
        let (order, floor_hit) = self.visit_order(floor);
        for lane in order {
            let Some(position) = self.lanes[lane]
                .iter()
                .position(|request| request.account == *account && request.kind == kind)
            else {
                continue;
            };
            if let Some(request) = self.lanes[lane].remove(position) {
                self.record_grant(lane, floor_hit);
                return Some(request);
            }
        }
        None
    }
}

impl Scheduler {
    #[must_use]
    pub fn new(cfg: SchedulerConfig, budget: BudgetGate) -> Self {
        Self::with_lane_capacity(cfg, budget, LaneQueue::DEFAULT_CAPACITY)
    }

    /// Construct with an explicit per-lane capacity for work-item and
    /// admission queues. Work-item queues use `DropOldest`; admission queues
    /// refuse the incoming request when full.
    #[must_use]
    pub fn with_lane_capacity(
        cfg: SchedulerConfig,
        budget: BudgetGate,
        lane_capacity: usize,
    ) -> Self {
        // A zero floor would satisfy `higher_consecutive >= floor` before
        // any work is granted, reversing the intended priority order.
        let starvation_floor = cfg.starvation_floor.max(1);
        let inner = Arc::new(SchedulerInner {
            foreground: LaneQueue::with_capacity(Priority::Foreground, lane_capacity),
            normal: LaneQueue::with_capacity(Priority::Normal, lane_capacity),
            background: LaneQueue::with_capacity(Priority::Background, lane_capacity),
            bulk: LaneQueue::with_capacity(Priority::Bulk, lane_capacity),
            starvation_floor,
            lane_capacity: lane_capacity.max(1),
            state: Mutex::new(SchedulerState::default()),
            admission: Mutex::new(AdmissionState::default()),
            budget,
            submitted: Notify::new(),
            admitted: Notify::new(),
            stop_notify: Notify::new(),
            stopped: AtomicBool::new(false),
            dispatcher_started: AtomicBool::new(false),
        });
        Self {
            _life: Arc::new(SchedulerLife {
                inner: Arc::clone(&inner),
            }),
            inner,
        }
    }

    pub fn submit(&self, item: WorkItem) {
        self.lane(item.kind.priority(item.priority)).push(item);
        self.inner.submitted.notify_one();
    }

    /// Strict-priority pull with starvation floor. Returns `None` if
    /// every lane is empty.
    ///
    /// This is non-blocking and synchronous; the worker decides
    /// whether to back off or `yield_now` when the scheduler is
    /// empty. Workers normally pair this with a notification source
    /// so they wake on `submit`. [`Scheduler::pull_next`] is the
    /// waiting form.
    ///
    /// This signature is deliberate and was restored after being changed.
    /// Making `pull` `async fn pull(&self) -> WorkItem` was tried and reverted:
    /// it removes the non-blocking "is there work right now?" answer, which is
    /// a lost CAPABILITY rather than a reshaped one, and this crate's standing
    /// rule permits reshaping a published item but not removing one. Keep the
    /// three-way split - `pull`/`try_pull` synchronous, `pull_next` waiting.
    pub fn pull(&self) -> Option<WorkItem> {
        self.try_pull()
    }

    /// Strict-priority pull that waits without polling when every lane
    /// is empty, waking directly on `submit`.
    ///
    /// This is the waiting counterpart to [`Scheduler::pull`], which
    /// stays synchronous and non-blocking so a consumer can still ask
    /// "is there work right now?" without committing to a wait.
    pub async fn pull_next(&self) -> WorkItem {
        loop {
            let notified = self.inner.submitted.notified();
            if let Some(item) = self.try_pull() {
                return item;
            }
            notified.await;
        }
    }

    /// Non-blocking pull. Alias of [`Scheduler::pull`], named for
    /// symmetry with `pull_next`.
    pub fn try_pull(&self) -> Option<WorkItem> {
        let mut state = self.inner.state.lock().expect("poisoned");
        // If the starvation floor has been hit, take from the lowest
        // non-empty lane and reset.
        if state.higher_consecutive >= self.inner.starvation_floor {
            if let Some(item) = self.inner.bulk.try_pop() {
                state.higher_consecutive = 0;
                return Some(item);
            }
            if let Some(item) = self.inner.background.try_pop() {
                state.higher_consecutive = 0;
                return Some(item);
            }
            // Both lower lanes empty; fall through to normal priority
            // order. Reset so the floor doesn't fire on the very next
            // pull and starve Foreground.
            state.higher_consecutive = 0;
        }

        if let Some(item) = self.inner.foreground.try_pop() {
            state.higher_consecutive = state.higher_consecutive.saturating_add(1);
            return Some(item);
        }
        if let Some(item) = self.inner.normal.try_pop() {
            state.higher_consecutive = state.higher_consecutive.saturating_add(1);
            return Some(item);
        }
        if let Some(item) = self.inner.background.try_pop() {
            state.higher_consecutive = 0;
            return Some(item);
        }
        if let Some(item) = self.inner.bulk.try_pop() {
            state.higher_consecutive = 0;
            return Some(item);
        }
        None
    }

    /// Queue this operation for admission and wait for its concurrency
    /// budget. Callers hold the returned permit for the complete
    /// protocol operation, and MUST acquire it outside any drive lease:
    /// admission can block for as long as the budget is saturated.
    ///
    /// `priority` is the account priority. Backfill runs in Background for
    /// Foreground/Normal accounts and Bulk for Background/Bulk accounts.
    /// Live sync and mutations retain the account lane.
    ///
    /// The request waits in a scheduler-owned lane, so it counts against
    /// `lane_capacity` for its whole wait and a later higher-priority
    /// request can overtake it. The permit is minted only when the
    /// budget can actually grant it.
    ///
    /// Returns `Error::Other` when the lane is at capacity (the request
    /// is refused rather than displacing an older waiter) or when the
    /// scheduler is shutting down. Callers on a repeating work path
    /// should treat that as transient and retry at their next tick
    /// rather than exiting.
    pub async fn admit(
        &self,
        account: AccountId,
        priority: Priority,
        kind: WorkKind,
    ) -> Result<BudgetPermit, crate::Error> {
        let priority = kind.priority(priority);
        let kind = kind.budget_kind();
        self.ensure_dispatcher();
        let (tx, rx) = oneshot::channel();
        {
            let mut admission = self.inner.admission.lock().expect("poisoned");
            let lane = lane_index(priority);
            if admission.lanes[lane].len() >= self.inner.lane_capacity {
                tracing::warn!(
                    target: "bifrost.sync.scheduler",
                    ?priority,
                    capacity = self.inner.lane_capacity,
                    "admission lane full; refusing the incoming request"
                );
                return Err(crate::Error::Other(
                    "scheduler admission lane is full".into(),
                ));
            }
            admission.lanes[lane].push_back(AdmissionRequest { account, kind, tx });
        }
        self.inner.admitted.notify_one();
        rx.await
            .map_err(|_| crate::Error::Other("scheduler admission was abandoned".into()))
    }

    fn ensure_dispatcher(&self) {
        if !self.inner.dispatcher_started.swap(true, Ordering::SeqCst) {
            let inner = Arc::clone(&self.inner);
            tokio::spawn(dispatch_admissions(inner));
        }
    }

    /// Snapshot admission lane depths for observability. These are the
    /// requests waiting for a budget permit; a saturated budget shows up
    /// here rather than as invisible semaphore waiters.
    #[must_use]
    pub fn admission_snapshot(&self) -> [LaneSnapshot; LANES] {
        let admission = self.inner.admission.lock().expect("poisoned");
        std::array::from_fn(|lane| LaneSnapshot {
            priority: lane_priority(lane),
            depth: admission.lanes[lane].len(),
        })
    }

    /// Snapshot lane depths for observability.
    #[must_use]
    pub fn snapshot(&self) -> [LaneSnapshot; 4] {
        [
            self.inner.foreground.snapshot(),
            self.inner.normal.snapshot(),
            self.inner.background.snapshot(),
            self.inner.bulk.snapshot(),
        ]
    }

    #[must_use]
    pub fn budget(&self) -> BudgetGate {
        self.inner.budget.clone()
    }

    fn lane(&self, priority: Priority) -> &LaneQueue {
        match priority {
            Priority::Foreground => &self.inner.foreground,
            Priority::Normal => &self.inner.normal,
            Priority::Background => &self.inner.background,
            Priority::Bulk => &self.inner.bulk,
            // `Priority` is `#[non_exhaustive]`; any future variant
            // falls through to the normal lane until the scheduler is
            // updated to handle it explicitly.
            _ => &self.inner.normal,
        }
    }
}

/// One in-flight budget acquisition for a `(account, kind)` class.
type PendingAcquire = BoxFuture<'static, (AccountId, WorkKind, Result<BudgetPermit, crate::Error>)>;

/// The single admission dispatcher.
///
/// One task per `Scheduler`, spawned on the first `admit` call and
/// stopped when the last handle drops. It never mints a permit
/// speculatively: for each distinct `(account, kind)` class waiting in
/// the lanes it races one `BudgetGate::acquire`, and the first class
/// whose budget frees up hands its permit to that class's most
/// preferred waiter. Losing acquisitions are dropped, which is
/// cancellation-safe on `tokio::sync::Semaphore` and returns nothing to
/// the pool that was not already there.
async fn dispatch_admissions(inner: Arc<SchedulerInner>) {
    let floor = inner.starvation_floor;
    loop {
        if inner.stopped.load(Ordering::SeqCst) {
            return;
        }
        let stop = inner.stop_notify.notified();
        tokio::pin!(stop);
        let notified = inner.admitted.notified();
        tokio::pin!(notified);

        let (classes, top_lane, order) = {
            let mut admission = inner.admission.lock().expect("poisoned");
            admission.prune();
            let (order, _) = admission.visit_order(floor);
            (admission.classes(floor), admission.best_lane(floor), order)
        };
        let Some(top_lane) = top_lane else {
            tokio::select! {
                () = &mut notified => continue,
                () = &mut stop => return,
            }
        };
        if classes.is_empty() {
            continue;
        }

        let planned = classes.clone();
        let mut pending: Vec<PendingAcquire> = classes
            .into_iter()
            .map(|(account, kind)| {
                let budget = inner.budget.clone();
                async move {
                    let permit = budget.acquire(&account, kind).await;
                    (account, kind, permit)
                }
                .boxed()
            })
            .collect();

        let granted = loop {
            tokio::select! {
                (granted, _index, _rest) = select_all(pending.iter_mut()) => break Some(granted),
                () = &mut notified => {
                    notified.set(inner.admitted.notified());
                    // Re-plan when a strictly more preferred lane gained
                    // work (so a Foreground arrival preempts a queued
                    // Background one), or when a budget class appeared
                    // that this plan is not racing at all - otherwise a
                    // request for an idle account would wait behind a
                    // saturated account's ungrantable one. Arrivals that
                    // change neither do NOT re-plan: restarting the
                    // in-flight acquisitions on every submission would
                    // starve them under a steady rate.
                    let replan = {
                        let mut admission = inner.admission.lock().expect("poisoned");
                        admission.prune();
                        let better = admission.best_lane(floor).is_some_and(|lane| {
                            preference_rank(&order, lane) < preference_rank(&order, top_lane)
                        });
                        better
                            || admission
                                .classes(floor)
                                .iter()
                                .any(|class| !planned.contains(class))
                    };
                    if replan {
                        break None;
                    }
                }
                () = &mut stop => return,
            }
        };
        let Some((account, kind, permit)) = granted else {
            continue;
        };
        let permit = match permit {
            Ok(permit) => permit,
            Err(error) => {
                tracing::warn!(
                    target: "bifrost.sync.scheduler",
                    %error,
                    "budget acquisition failed; dropping the admission attempt"
                );
                // Fail the waiter rather than spinning on a closed
                // semaphore: its channel drops when the request is taken.
                let mut admission = inner.admission.lock().expect("poisoned");
                let _ = admission.take_for_class(&account, kind, floor);
                continue;
            }
        };
        let taken = {
            let mut admission = inner.admission.lock().expect("poisoned");
            admission.take_for_class(&account, kind, floor)
        };
        match taken {
            // Send failure means the caller went away between selection
            // and hand-off; dropping the permit returns it immediately.
            Some(request) => {
                let _ = request.tx.send(permit);
            }
            None => drop(permit),
        }
    }
}

/// Position of a lane in a visit order: lower is served first.
fn preference_rank(order: &[usize; LANES], lane: usize) -> usize {
    order
        .iter()
        .position(|candidate| *candidate == lane)
        .unwrap_or(LANES)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler(floor: u32) -> Scheduler {
        Scheduler::new(
            SchedulerConfig {
                starvation_floor: floor,
            },
            BudgetGate::new(ConcurrencyBudget::default()),
        )
    }

    fn item(priority: Priority, tag: &'static str) -> WorkItem {
        WorkItem {
            account: AccountId(tag.into()),
            priority,
            kind: WorkKind::Sync,
            run: Box::new(|| {}),
        }
    }

    #[test]
    fn zero_starvation_floor_preserves_initial_work_item_priority() {
        let scheduler = scheduler(0);
        scheduler.submit(item(Priority::Bulk, "bulk"));
        scheduler.submit(item(Priority::Foreground, "foreground"));

        assert_eq!(
            scheduler.try_pull().expect("foreground item").priority,
            Priority::Foreground
        );
        assert_eq!(
            scheduler.try_pull().expect("bulk item").priority,
            Priority::Bulk
        );
    }

    #[tokio::test]
    async fn zero_starvation_floor_preserves_initial_admission_priority() {
        let budget = BudgetGate::new(ConcurrencyBudget {
            per_account: 2,
            global: 4,
            mutation_share_num: 1,
            mutation_share_den: 2,
        });
        let scheduler = Scheduler::new(
            SchedulerConfig {
                starvation_floor: 0,
            },
            budget.clone(),
        );
        let account = AccountId("zero-floor-admission".into());
        // Consume capacity directly so the held permit does not count as a
        // scheduler grant and advance its higher-priority starvation state.
        let held = budget
            .acquire(&account, WorkKind::Sync)
            .await
            .expect("hold the account's only sync permit");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let background = {
            let scheduler = scheduler.clone();
            let account = account.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let permit = scheduler
                    .admit(account, Priority::Background, WorkKind::Sync)
                    .await
                    .expect("background admission");
                tx.send(Priority::Background)
                    .expect("record granted priority");
                drop(permit);
            })
        };
        let foreground = {
            let scheduler = scheduler.clone();
            let account = account.clone();
            tokio::spawn(async move {
                let permit = scheduler
                    .admit(account, Priority::Foreground, WorkKind::Sync)
                    .await
                    .expect("foreground admission");
                tx.send(Priority::Foreground)
                    .expect("record granted priority");
                drop(permit);
            })
        };

        for _ in 0..64 {
            let queued: usize = scheduler
                .admission_snapshot()
                .iter()
                .map(|lane| lane.depth)
                .sum();
            if queued == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let queued: usize = scheduler
            .admission_snapshot()
            .iter()
            .map(|lane| lane.depth)
            .sum();
        assert_eq!(queued, 2, "both requests must queue before release");

        drop(held);
        assert_eq!(
            rx.recv().await,
            Some(Priority::Foreground),
            "zero must not make admission visit lower lanes first"
        );
        background.await.expect("background task");
        foreground.await.expect("foreground task");
    }
}
