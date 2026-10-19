//! Shared payload admission for HTTP and stream adapters.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use crate::AccountId;

/// Largest offer, including while unlimited: 256 KiB.
pub const MAX_OFFER: usize = 256 * 1024;

/// Payload direction. Each direction has an independent schedule.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum BandwidthDirection {
    /// Bytes delivered to the consumer.
    Inbound,
    /// Bytes offered to the transport.
    Outbound,
}

impl BandwidthDirection {
    fn index(self) -> usize {
        match self {
            Self::Inbound => 0,
            Self::Outbound => 1,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Policy {
    cap: Option<u64>,
    epoch: u64,
}

#[derive(Default)]
struct Schedule {
    tat: Option<Instant>,
    tail: Option<u64>,
}

struct State {
    policy: Policy,
    next_id: u64,
    schedules: [Schedule; 2],
    // Offers survive policy changes. Their issued sizes are the allowance
    // term in the bound, including offers made while unlimited.
    outstanding: HashMap<u64, usize>,
}

struct BudgetInner {
    state: Mutex<State>,
    changes: watch::Sender<Policy>,
    owner: Option<(Weak<BandwidthRegistry>, AccountId)>,
}

impl Drop for BudgetInner {
    fn drop(&mut self) {
        let identity = std::ptr::from_ref(self);
        if let Some((registry, id)) = &self.owner
            && let Some(registry) = registry.upgrade()
        {
            registry.remove(id, identity);
        }
    }
}

/// Cloneable shared payload smoothing cap, with independent direction budgets.
///
/// Counts HTTP bodies and stream bytes above TLS. TLS, HTTP headers and bytes
/// buffered below the adapter are outside this cap; it is no wire-rate promise.
/// For a window of length T seconds with constant finite cap c, per direction,
/// bytes <= c * (T + 1) + A. A is all unspent allowances at the window start,
/// including queued, ready and offered grants at their issued sizes and
/// outstanding unlimited offers. There is no bound across unlimited intervals.
/// Sequential adapters must hold at most one admission per operation. Idle
/// pending reads pre-spend a slot, wasting capacity without blocking siblings.
#[derive(Clone)]
pub struct BandwidthBudget {
    inner: Arc<BudgetInner>,
}

impl Default for BandwidthBudget {
    fn default() -> Self {
        Self::new(None)
    }
}

impl BandwidthBudget {
    /// Create a budget. Zero is normalized to one byte/second with a warning.
    #[must_use]
    pub fn new(cap: Option<u64>) -> Self {
        Self::with_owner(cap, None)
    }

    fn with_owner(cap: Option<u64>, owner: Option<(Weak<BandwidthRegistry>, AccountId)>) -> Self {
        let policy = Policy {
            cap: normalize(cap),
            epoch: 0,
        };
        let (changes, _) = watch::channel(policy);
        Self {
            inner: Arc::new(BudgetInner {
                state: Mutex::new(State {
                    policy,
                    next_id: 0,
                    schedules: Default::default(),
                    outstanding: HashMap::new(),
                }),
                changes,
                owner,
            }),
        }
    }

    /// Current cap; None means unlimited.
    #[must_use]
    pub fn cap(&self) -> Option<u64> {
        self.inner
            .state
            .lock()
            .expect("bandwidth lock poisoned")
            .policy
            .cap
    }

    /// Current policy epoch, advanced whenever the cap crosses unlimited.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.inner
            .state
            .lock()
            .expect("bandwidth lock poisoned")
            .policy
            .epoch
    }

    /// Change future admission pricing. Issued finite slots never move.
    /// Crossing unlimited invalidates queued grants and wakes their waiters.
    /// Setting the current cap is a no-op.
    pub fn set_cap(&self, cap: Option<u64>) {
        let cap = normalize(cap);
        let mut state = self.inner.state.lock().expect("bandwidth lock poisoned");
        if state.policy.cap == cap {
            return;
        }
        if state.policy.cap.is_none() || cap.is_none() {
            state.policy.epoch = state
                .policy
                .epoch
                .checked_add(1)
                .expect("bandwidth epoch exhausted");
            state.schedules = Default::default();
        }
        state.policy.cap = cap;
        self.inner.changes.send_replace(state.policy);
    }

    /// Issue a fixed slot, bounded by len, one second of cap and MAX_OFFER.
    /// Unlimited reservations are also tracked and bounded by MAX_OFFER.
    #[must_use]
    pub fn reserve(&self, direction: BandwidthDirection, len: usize) -> Reservation {
        let mut state = self.inner.state.lock().expect("bandwidth lock poisoned");
        let policy = state.policy;
        let granted = len.min(MAX_OFFER).min(
            policy
                .cap
                .map_or(usize::MAX, |cap| usize::try_from(cap).unwrap_or(usize::MAX)),
        );
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .expect("bandwidth reservation id exhausted");
        let now = Instant::now();
        let duration = policy
            .cap
            .map_or(Duration::ZERO, |cap| duration_at(granted, cap));
        let schedule = &mut state.schedules[direction.index()];
        let ready_at = if policy.cap.is_some() {
            let end = schedule.tat.unwrap_or(now).max(now) + duration;
            schedule.tat = Some(end);
            schedule.tail = Some(id);
            end.checked_sub(Duration::from_secs(1))
                .unwrap_or(now)
                .max(now)
        } else {
            now
        };
        state.outstanding.insert(id, granted);
        Reservation {
            budget: self.clone(),
            direction,
            id,
            epoch: policy.epoch,
            granted,
            ready_at,
            duration,
            offered: false,
            finished: false,
        }
    }

    /// Wait for a ready slot and validate its epoch under the schedule lock.
    /// Call `Reservation::offer` before I/O to validate again atomically.
    /// Dropping this future refunds only an unoffered current tail.
    pub async fn ready(&self, direction: BandwidthDirection, len: usize) -> Reservation {
        let mut changes = self.inner.changes.subscribe();
        loop {
            let reservation = self.reserve(direction, len);
            // A slot already due (always, when uncapped) returns without
            // touching the timer, so an uncapped transfer costs no extra
            // poll and never waits on a timer tick.
            if reservation.ready_at <= Instant::now() {
                return reservation;
            }
            loop {
                tokio::select! {
                    biased;
                    _ = changes.changed() => {},
                    () = tokio::time::sleep_until(reservation.ready_at) => {},
                }
                let state = self.inner.state.lock().expect("bandwidth lock poisoned");
                if state.policy.epoch != reservation.epoch {
                    break;
                }
                if Instant::now() >= reservation.ready_at {
                    drop(state);
                    return reservation;
                }
            }
        }
    }

    /// Wait and offer atomically against the current epoch. Adapters that
    /// need a final deadline check before offer use `ready` and `offer`.
    pub async fn admit(&self, direction: BandwidthDirection, len: usize) -> Reservation {
        loop {
            let mut reservation = self.ready(direction, len).await;
            if reservation.offer() {
                return reservation;
            }
        }
    }
}

fn normalize(cap: Option<u64>) -> Option<u64> {
    if cap == Some(0) {
        tracing::warn!(target: "bifrost_net::bandwidth", "bandwidth cap Some(0) clamped to 1 B/s; use None for unlimited");
        Some(1)
    } else {
        cap
    }
}

fn duration_at(bytes: usize, cap: u64) -> Duration {
    Duration::from_nanos(
        u64::try_from((bytes as u128 * 1_000_000_000).div_ceil(u128::from(cap)))
            .expect("bounded bandwidth offer"),
    )
}

/// A queued, ready or offered fixed slot. Its identity and issued size never
/// change. Drop before offer refunds only the current epoch's tail. Drop after
/// offer refunds nothing, since I/O may have accepted bytes. Short successful
/// I/O refunds the unused remainder only if this is still the current tail.
pub struct Reservation {
    budget: BandwidthBudget,
    direction: BandwidthDirection,
    id: u64,
    epoch: u64,
    granted: usize,
    ready_at: Instant,
    duration: Duration,
    offered: bool,
    finished: bool,
}

impl Reservation {
    /// Unique identity within this budget.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }
    /// Policy epoch at issue.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Issued allowance in bytes.
    #[must_use]
    pub fn granted(&self) -> usize {
        self.granted
    }
    /// Fixed earliest offer time.
    #[must_use]
    pub fn ready_at(&self) -> Instant {
        self.ready_at
    }

    /// Offer a ready reservation. Returns false after an epoch change or
    /// before its fixed slot. Validation and installation share one lock.
    pub fn offer(&mut self) -> bool {
        let state = self
            .budget
            .inner
            .state
            .lock()
            .expect("bandwidth lock poisoned");
        if self.finished
            || self.offered
            || state.policy.epoch != self.epoch
            || Instant::now() < self.ready_at
        {
            return false;
        }
        self.offered = true;
        true
    }

    /// Finish a successful I/O, returning any unused tail allowance.
    pub fn finish(mut self, used: usize) {
        assert!(self.offered, "bandwidth reservation was not offered");
        self.release(self.granted.saturating_sub(used));
    }

    fn release(&mut self, unused: usize) {
        let mut state = self
            .budget
            .inner
            .state
            .lock()
            .expect("bandwidth lock poisoned");
        state.outstanding.remove(&self.id);
        let current = state.policy.epoch == self.epoch;
        let schedule = &mut state.schedules[self.direction.index()];
        if current && schedule.tail == Some(self.id) {
            let nanos = self.duration.as_nanos() * unused as u128 / self.granted.max(1) as u128;
            let refund = Duration::from_nanos(u64::try_from(nanos).expect("bounded refund"));
            schedule.tat = schedule.tat.and_then(|tat| tat.checked_sub(refund));
            // Do not resurrect a previous id: a finished tail stays finished.
            schedule.tail = None;
        }
        self.finished = true;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.finished {
            self.release(if self.offered { 0 } else { self.granted });
        }
    }
}

/// Injected identity registry. Wire ONE registry into every Net, the engine
/// and the IMAP factory. Matching ids in different registries do not coordinate.
/// Entries are weak; reservations keep their source budget alive across retag.
#[derive(Default)]
pub struct BandwidthRegistry {
    budgets: Mutex<HashMap<AccountId, Weak<BudgetInner>>>,
}

impl BandwidthRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up or create the single live budget for this account id.
    #[must_use]
    pub fn budget(self: &Arc<Self>, id: AccountId) -> BandwidthBudget {
        let mut budgets = self
            .budgets
            .lock()
            .expect("bandwidth registry lock poisoned");
        if let Some(inner) = budgets.get(&id).and_then(Weak::upgrade) {
            return BandwidthBudget { inner };
        }
        budgets.retain(|_, budget| budget.strong_count() != 0);
        let budget = BandwidthBudget::with_owner(None, Some((Arc::downgrade(self), id.clone())));
        budgets.insert(id, Arc::downgrade(&budget.inner));
        budget
    }

    fn remove(&self, id: &AccountId, inner: *const BudgetInner) {
        let mut budgets = self
            .budgets
            .lock()
            .expect("bandwidth registry lock poisoned");
        if budgets.get(id).is_some_and(|entry| entry.as_ptr() == inner) {
            budgets.remove(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use BandwidthDirection::{Inbound, Outbound};

    fn commit(budget: &BandwidthBudget, len: usize) {
        let mut grant = budget.reserve(Inbound, len);
        assert!(grant.offer());
        grant.finish(len);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_keeps_fixed_slots_and_refunds_only_the_tail() {
        let budget = BandwidthBudget::new(Some(10));
        let now = Instant::now();
        commit(&budget, 10);
        let non_tail = budget.reserve(Inbound, 10);
        let follower = budget.reserve(Inbound, 10);
        assert_eq!(non_tail.ready_at(), now + Duration::from_secs(1));
        assert_eq!(follower.ready_at(), now + Duration::from_secs(2));
        drop(non_tail);
        let tail = budget.reserve(Inbound, 10);
        assert_eq!(tail.ready_at(), now + Duration::from_secs(3));
        drop(tail);
        let replacement = budget.reserve(Inbound, 10);
        assert_eq!(replacement.ready_at(), now + Duration::from_secs(3));
        assert_eq!(follower.ready_at(), now + Duration::from_secs(2));
        assert!(replacement.ready_at() > follower.ready_at());
    }

    #[tokio::test(start_paused = true)]
    async fn short_success_refunds_only_an_unused_tail() {
        let budget = BandwidthBudget::new(Some(10));
        let now = Instant::now();
        let mut first = budget.reserve(Inbound, 10);
        assert!(first.offer());
        first.finish(4);
        let next = budget.reserve(Inbound, 10);
        assert_eq!(next.ready_at(), now + Duration::from_millis(400));
        drop(next);
        let mut offered = budget.reserve(Outbound, 10);
        assert!(offered.offer());
        drop(offered);
        assert_eq!(
            budget.reserve(Outbound, 10).ready_at(),
            now + Duration::from_secs(1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn short_success_does_not_refund_a_non_tail_grant() {
        let budget = BandwidthBudget::new(Some(10));
        let now = Instant::now();
        let mut first = budget.reserve(Inbound, 10);
        assert!(first.offer());
        let follower = budget.reserve(Inbound, 10);
        first.finish(1);
        assert_eq!(follower.ready_at(), now + Duration::from_secs(1));
        assert_eq!(
            budget.reserve(Inbound, 10).ready_at(),
            now + Duration::from_secs(2)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn finite_retunes_preserve_grants_and_unchanged_cap_does_nothing() {
        let budget = BandwidthBudget::new(Some(100));
        let mut changes = budget.inner.changes.subscribe();
        commit(&budget, 100);
        let queued = budget.reserve(Inbound, 100);
        let epoch = queued.epoch();
        budget.set_cap(Some(100));
        assert!(!changes.has_changed().expect("watch alive"));
        let next = budget.reserve(Inbound, 100);
        assert_eq!(next.ready_at(), queued.ready_at() + Duration::from_secs(1));
        drop(next);
        budget.set_cap(Some(1));
        assert!(changes.has_changed().expect("watch alive"));
        changes.borrow_and_update();
        assert_eq!(budget.reserve(Outbound, 100).granted(), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        let mut queued = queued;
        assert_eq!(queued.granted(), 100);
        assert_eq!(queued.epoch(), epoch);
        assert!(queued.offer());
    }

    #[tokio::test(start_paused = true)]
    async fn current_epoch_tracks_unlimited_transitions_without_reserving() {
        let budget = BandwidthBudget::new(Some(10));
        let queued = budget.reserve(Inbound, 10);
        assert_eq!(budget.epoch(), queued.epoch());
        assert_eq!(budget.epoch(), 0);
        budget.set_cap(Some(1));
        assert_eq!(budget.epoch(), 0);
        budget.set_cap(Some(1));
        assert_eq!(budget.epoch(), 0);
        let next = budget.reserve(Inbound, 1);
        assert_eq!(next.id(), queued.id() + 1);
        assert_eq!(next.ready_at(), Instant::now() + Duration::from_secs(1));
        budget.set_cap(None);
        assert_eq!(budget.epoch(), 1);
        budget.set_cap(None);
        assert_eq!(budget.epoch(), 1);
        budget.set_cap(Some(1));
        assert_eq!(budget.epoch(), 2);
        assert_ne!(budget.epoch(), queued.epoch());
        assert_eq!(budget.reserve(Inbound, 1).epoch(), budget.epoch());
    }

    #[tokio::test(start_paused = true)]
    async fn cap_lift_wakes_a_long_waiter_without_advancing_time() {
        let budget = BandwidthBudget::new(Some(1));
        commit(&budget, 1);
        let _queued: Vec<_> = (0..100).map(|_| budget.reserve(Inbound, 1)).collect();
        let mut waiting = Box::pin(budget.admit(Inbound, 1));
        assert!(futures::FutureExt::now_or_never(waiting.as_mut()).is_none());
        let now = Instant::now();
        budget.set_cap(None);
        let grant = waiting.await;
        assert_eq!(Instant::now(), now);
        assert_eq!(grant.epoch(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_must_readmit_after_unlimited_and_recapped_epochs() {
        let budget = BandwidthBudget::new(Some(10));
        commit(&budget, 10);
        let mut waiting = Box::pin(budget.admit(Inbound, 10));
        assert!(futures::FutureExt::now_or_never(waiting.as_mut()).is_none());
        budget.set_cap(None);
        budget.set_cap(Some(1));
        commit(&budget, 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        let grant = waiting.await;
        assert_eq!(grant.epoch(), 2);
        assert_eq!(grant.granted(), 1);
        // It reserved again at t=1, rather than using the old 10-byte grant.
        assert!(grant.id() >= 3);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_ready_grants_cannot_install_and_unlimited_offers_stay_counted() {
        let budget = BandwidthBudget::default();
        let mut offer = budget.reserve(Inbound, usize::MAX);
        assert_eq!(offer.granted(), MAX_OFFER);
        assert!(offer.offer());
        let mut stale = budget.reserve(Inbound, 50);
        budget.set_cap(Some(1));
        assert!(!stale.offer());
        assert_eq!(
            budget
                .inner
                .state
                .lock()
                .expect("state")
                .outstanding
                .get(&offer.id()),
            Some(&MAX_OFFER)
        );
        offer.finish(40);
        assert!(
            !budget
                .inner
                .state
                .lock()
                .expect("state")
                .outstanding
                .contains_key(&0)
        );
        // Uncapped offers are allowance, not debt on the new empty schedule.
        let mut current = budget.reserve(Inbound, 1);
        assert!(current.offer());
    }

    #[test]
    fn registry_reuses_live_ids_prunes_dead_ids_and_checks_removal_identity() {
        let registry = Arc::new(BandwidthRegistry::new());
        let id = AccountId("same".into());
        let first = registry.budget(id.clone());
        let same = registry.budget(id.clone());
        assert!(Arc::ptr_eq(&first.inner, &same.inner));
        first.set_cap(Some(17));
        assert_eq!(same.cap(), Some(17));
        let replacement = BandwidthBudget::default();
        registry
            .budgets
            .lock()
            .expect("registry")
            .insert(id.clone(), Arc::downgrade(&replacement.inner));
        drop(same);
        drop(first);
        assert!(Arc::ptr_eq(
            &registry.budget(id.clone()).inner,
            &replacement.inner
        ));
        registry
            .budgets
            .lock()
            .expect("registry")
            .insert(AccountId("dead".into()), Weak::new());
        let live = registry.budget(AccountId("new".into()));
        assert_eq!(registry.budgets.lock().expect("registry").len(), 2);
        drop(live);
        assert_eq!(registry.budgets.lock().expect("registry").len(), 1);
    }

    #[test]
    fn zero_is_normalized_and_a_maximum_cap_is_finite() {
        let budget = BandwidthBudget::new(Some(0));
        assert_eq!(budget.cap(), Some(1));
        budget.set_cap(Some(u64::MAX));
        assert_eq!(budget.cap(), Some(u64::MAX));
    }
}
