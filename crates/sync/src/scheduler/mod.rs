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
//! A full lane is transient pressure: the wire-start gate backs off before
//! retrying, rather than retiring a scope for a condition that heals. A stopped
//! dispatcher, forgotten account budget or broken pool is a distinct typed
//! failure. Retrying those cannot restore this attachment's admission.

pub mod budget;
pub mod lanes;
mod priority;

use priority::{LANES, OverflowPolicy, PriorityLanes, PushOutcome, lane_index};

pub use budget::{BudgetGate, BudgetPermit, ConcurrencyBudget};
pub use lanes::{LaneQueue, LaneSnapshot, WorkItem, WorkKind};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::{BoxFuture, select_all};
use tokio::sync::{Notify, oneshot};

use bifrost_types::{AccountId, Priority};

use crate::types::SchedulerConfig;

/// Public scheduler handle.
///
/// Holds independent work and admission lanes with shared queue and
/// starvation mechanics, plus the budget gate. Engine work uses `admit`;
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
    work: Mutex<PriorityLanes<WorkItem>>,
    admission: Mutex<PriorityLanes<AdmissionRequest>>,
    budget: BudgetGate,
    submitted: Notify,
    admitted: Notify,
    stop_notify: Notify,
    stopped: AtomicBool,
    dispatcher_started: AtomicBool,
}

/// One queued admission request. The permit travels back over `tx`, so
/// dropping the `admit` future closes the channel and the dispatcher
/// prunes the entry without ever spending a permit on it.
#[derive(Debug)]
struct AdmissionRequest {
    account: AccountId,
    kind: WorkKind,
    tx: oneshot::Sender<Result<BudgetPermit, crate::Error>>,
}

impl PriorityLanes<AdmissionRequest> {
    /// Drop requests whose caller has gone away.
    fn prune(&mut self) {
        for lane in &mut self.lanes {
            lane.queue.retain(|request| !request.tx.is_closed());
        }
    }

    /// Distinct budget classes present, in grant-preference order.
    fn classes(&self) -> Vec<(AccountId, WorkKind)> {
        let order = self.visit_order();
        let mut out: Vec<(AccountId, WorkKind)> = Vec::new();
        for lane in order {
            for request in &self.lanes[lane].queue {
                let class = (request.account.clone(), request.kind);
                if !out.contains(&class) {
                    out.push(class);
                }
            }
        }
        out
    }

    fn take_for_class(&mut self, account: &AccountId, kind: WorkKind) -> Option<AdmissionRequest> {
        self.take(|request| {
            request.account == *account && request.kind == kind && !request.tx.is_closed()
        })
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
        let inner = Arc::new(SchedulerInner {
            work: Mutex::new(PriorityLanes::new(
                lane_capacity,
                cfg.starvation_floor,
                OverflowPolicy::ShedOldest,
            )),
            admission: Mutex::new(PriorityLanes::new(
                lane_capacity,
                cfg.starvation_floor,
                OverflowPolicy::Refuse,
            )),
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
        let priority = item.kind.priority(item.priority);
        let mut work = self.inner.work.lock().expect("poisoned");
        if work.push(priority, item) == PushOutcome::ShedOldest {
            tracing::warn!(
                target: "bifrost.sync.scheduler",
                ?priority,
                capacity = work.lanes[lane_index(priority)].capacity,
                "lane full; shedding oldest work item"
            );
        }
        drop(work);
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
        self.inner.work.lock().expect("poisoned").take(|_| true)
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
    /// Returns `Error::Admission(LaneFull { .. })` when the lane is at capacity (the request
    /// is refused rather than displacing an older waiter) or when the
    /// scheduler is shutting down. Only lane saturation is transient;
    /// forgotten accounts and broken budget state retain their typed failure.
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
            // A dropped `admit` future closes its channel but notifies
            // nobody, and the dispatcher only prunes when it wakes. Without
            // this prune a cancelled waiter (a caller that stopped waiting
            // on a pause or shutdown) kept its lane slot until some later
            // wake, refusing live requests as "lane full" meanwhile.
            admission.lanes[lane]
                .queue
                .retain(|request| !request.tx.is_closed());
            let capacity = admission.lanes[lane].capacity;
            if admission.push(priority, AdmissionRequest { account, kind, tx })
                == PushOutcome::Refused
            {
                tracing::warn!(
                    target: "bifrost.sync.scheduler",
                    ?priority,
                    capacity,
                    "admission lane full; refusing the incoming request"
                );
                return Err(crate::Error::Admission(crate::AdmissionFailure::LaneFull {
                    priority,
                    capacity,
                }));
            }
        }
        self.inner.admitted.notify_one();
        // The dispatcher dropped our request unanswered: it is gone, so no
        // retry can succeed. A typed error lets callers tell this apart from
        // a full lane (which is transient).
        rx.await.map_err(|_| crate::Error::ShuttingDown)?
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
        self.inner.admission.lock().expect("poisoned").snapshot()
    }

    /// Snapshot lane depths for observability.
    #[must_use]
    pub fn snapshot(&self) -> [LaneSnapshot; LANES] {
        self.inner.work.lock().expect("poisoned").snapshot()
    }

    #[must_use]
    pub fn budget(&self) -> BudgetGate {
        self.inner.budget.clone()
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
            let order = admission.visit_order();
            (admission.classes(), admission.best_lane(), order)
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
                        let better = admission.best_lane().is_some_and(|lane| {
                            preference_rank(&order, lane) < preference_rank(&order, top_lane)
                        });
                        better
                            || admission
                                .classes()
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
                // Preserve the budget verdict through the response channel.
                // Only a full lane can clear by retrying this attachment.
                let mut admission = inner.admission.lock().expect("poisoned");
                if let Some(request) = admission.take_for_class(&account, kind) {
                    let _ = request.tx.send(Err(error));
                }
                continue;
            }
        };
        let taken = {
            let mut admission = inner.admission.lock().expect("poisoned");
            admission.take_for_class(&account, kind)
        };
        match taken {
            // Send failure means the caller went away between selection
            // and hand-off; dropping the permit returns it immediately.
            Some(request) => {
                let _ = request.tx.send(Ok(permit));
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

    #[test]
    fn idle_work_pulls_do_not_erase_starvation_relief() {
        let scheduler = scheduler(1);
        scheduler.submit(item(Priority::Foreground, "foreground"));
        assert_eq!(
            scheduler.pull().expect("first grant").priority,
            Priority::Foreground
        );
        assert!(scheduler.pull().is_none());
        scheduler.submit(item(Priority::Foreground, "foreground"));
        scheduler.submit(item(Priority::Bulk, "bulk"));
        assert_eq!(
            scheduler.pull().expect("starvation relief").priority,
            Priority::Bulk
        );
        assert_eq!(
            scheduler.pull().expect("remaining work").priority,
            Priority::Foreground
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

    #[tokio::test]
    async fn admission_preserves_forgotten_account_failure() {
        let scheduler = scheduler(1);
        let account = AccountId("retired-admission".into());
        scheduler.budget().forget(&account);
        assert!(matches!(
            scheduler.admit(account.clone(), Priority::Normal, WorkKind::Sync).await,
            Err(crate::Error::Admission(crate::AdmissionFailure::AccountForgotten(id))) if id == account
        ));
    }

    #[tokio::test]
    async fn admission_reports_full_lane_with_priority_and_capacity() {
        let scheduler = Scheduler::with_lane_capacity(
            SchedulerConfig {
                starvation_floor: 1,
            },
            BudgetGate::new(ConcurrencyBudget::default()),
            1,
        );
        // Enqueue by polling, without yielding to the dispatcher. This makes
        // saturation deterministic rather than depending on task scheduling.
        let first = scheduler.admit(AccountId("first".into()), Priority::Normal, WorkKind::Sync);
        tokio::pin!(first);
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(matches!(
            scheduler
                .admit(AccountId("second".into()), Priority::Normal, WorkKind::Sync)
                .await,
            Err(crate::Error::Admission(crate::AdmissionFailure::LaneFull {
                priority: Priority::Normal,
                capacity: 1
            }))
        ));
    }
}
