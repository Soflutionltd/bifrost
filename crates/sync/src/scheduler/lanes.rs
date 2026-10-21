//! Per-lane FIFO queue primitive.
//!
//! Each lane is an independent bounded `Mutex<VecDeque>`. The
//! scheduler's strict-priority `pull` walks the four lanes in priority
//! order, with the starvation floor diverting one pull to the lower
//! lanes every N pulls.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{AccountId, Priority};

/// Work item submitted to the scheduler. Kept deliberately small;
/// the actual work lives behind the boxed thunk.
pub struct WorkItem {
    pub account: AccountId,
    pub priority: Priority,
    pub kind: WorkKind,
    pub run: Box<dyn FnOnce() + Send + 'static>,
}

impl std::fmt::Debug for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkItem")
            .field("account", &self.account)
            .field("priority", &self.priority)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Work kind determines scheduling priority and the concurrency sub-pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkKind {
    /// Live polling and push reconciliation, at the account priority.
    Sync,
    Mutation,
    /// Historical partitions and deferred inventory, below live work.
    Backfill,
}

impl WorkKind {
    pub(crate) fn priority(self, account_priority: Priority) -> Priority {
        if self != Self::Backfill {
            return account_priority;
        }
        match account_priority {
            Priority::Background | Priority::Bulk => Priority::Bulk,
            _ => Priority::Background,
        }
    }

    // Live and historical work share one budget class so a permit is always
    // handed to the best lane, regardless of which kind queued first.
    pub(crate) fn budget_kind(self) -> Self {
        match self {
            Self::Backfill => Self::Sync,
            other => other,
        }
    }
}

/// Shed policy when a lane is full. `DropOldest` is the default and
/// evicts the head; `DropNewest` rejects the incoming submission. Both
/// increment the queue's shed counter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum LaneShedPolicy {
    #[default]
    DropOldest,
    DropNewest,
}

/// Per-lane FIFO queue.
#[derive(Debug)]
pub struct LaneQueue {
    priority: Priority,
    capacity: usize,
    queue: Mutex<VecDeque<WorkItem>>,
    shed_count: AtomicU64,
    shed_policy: LaneShedPolicy,
}

impl LaneQueue {
    /// Default cap if no `EngineConfig::lane_capacity` is supplied.
    pub const DEFAULT_CAPACITY: usize = 1024;

    #[must_use]
    pub fn new(priority: Priority) -> Self {
        Self::with_capacity(priority, Self::DEFAULT_CAPACITY)
    }

    #[must_use]
    pub fn with_capacity(priority: Priority, capacity: usize) -> Self {
        Self::with_capacity_and_policy(priority, capacity, LaneShedPolicy::default())
    }

    /// Construct a lane with an explicit overflow policy.
    #[must_use]
    pub fn with_capacity_and_policy(
        priority: Priority,
        capacity: usize,
        shed_policy: LaneShedPolicy,
    ) -> Self {
        Self {
            priority,
            capacity: capacity.max(1),
            queue: Mutex::new(VecDeque::new()),
            shed_count: AtomicU64::new(0),
            shed_policy,
        }
    }

    /// Push at the tail. If the queue is at capacity, the oldest item
    /// is shed by the default policy and the shed counter is incremented.
    /// The counter can be read from this queue with `shed_count`; scheduler
    /// queues are private, so the engine does not expose their counters.
    pub fn push(&self, item: WorkItem) {
        let mut g = self.queue.lock().expect("poisoned");
        if g.len() >= self.capacity {
            self.shed_count.fetch_add(1, Ordering::Relaxed);
            match self.shed_policy {
                LaneShedPolicy::DropOldest => {
                    let _ = g.pop_front();
                    tracing::warn!(
                        target: "bifrost.sync.scheduler",
                        priority = ?self.priority,
                        capacity = self.capacity,
                        "lane full; shedding oldest work item"
                    );
                }
                LaneShedPolicy::DropNewest => {
                    tracing::warn!(
                        target: "bifrost.sync.scheduler",
                        priority = ?self.priority,
                        capacity = self.capacity,
                        "lane full; shedding newest work item"
                    );
                    return;
                }
            }
        }
        g.push_back(item);
    }

    pub fn try_pop(&self) -> Option<WorkItem> {
        let mut g = self.queue.lock().expect("poisoned");
        g.pop_front()
    }

    #[must_use]
    pub fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn snapshot(&self) -> LaneSnapshot {
        let g = self.queue.lock().expect("poisoned");
        LaneSnapshot {
            priority: self.priority,
            depth: g.len(),
        }
    }
}

/// Snapshot of a single lane's depth for observability.
#[derive(Debug, Clone, Copy)]
pub struct LaneSnapshot {
    pub priority: Priority,
    pub depth: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(tag: &str) -> WorkItem {
        WorkItem {
            account: AccountId(tag.into()),
            priority: Priority::Normal,
            kind: WorkKind::Sync,
            run: Box::new(|| {}),
        }
    }

    #[test]
    fn work_kind_lanes_keep_live_priority_and_lower_inventory() {
        for (account, backfill) in [
            (Priority::Foreground, Priority::Background),
            (Priority::Normal, Priority::Background),
            (Priority::Background, Priority::Bulk),
            (Priority::Bulk, Priority::Bulk),
        ] {
            assert_eq!(WorkKind::Backfill.priority(account), backfill);
            assert_eq!(WorkKind::Sync.priority(account), account);
            assert_eq!(WorkKind::Mutation.priority(account), account);
            assert_eq!(WorkKind::Backfill.budget_kind(), WorkKind::Sync);
        }
    }

    #[test]
    fn explicit_drop_newest_policy_is_selectable() {
        let lane =
            LaneQueue::with_capacity_and_policy(Priority::Normal, 2, LaneShedPolicy::DropNewest);
        lane.push(item("first"));
        lane.push(item("second"));
        lane.push(item("third"));

        assert_eq!(lane.shed_count(), 1);
        assert_eq!(
            lane.try_pop().expect("first retained item").account,
            AccountId("first".into())
        );
        assert_eq!(
            lane.try_pop().expect("second retained item").account,
            AccountId("second".into())
        );
        assert!(lane.try_pop().is_none());
    }
}
