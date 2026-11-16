//! Shared bounded lanes and grant order for submitted work and admission.

use std::collections::VecDeque;

use bifrost_types::Priority;

use super::lanes::LaneSnapshot;

pub(super) const LANES: usize = 4;

pub(super) fn lane_index(priority: Priority) -> usize {
    match priority {
        Priority::Foreground => 0,
        Priority::Background => 2,
        Priority::Bulk => 3,
        // Future priorities use Normal until explicitly supported.
        _ => 1,
    }
}

pub(super) fn lane_priority(index: usize) -> Priority {
    match index {
        0 => Priority::Foreground,
        2 => Priority::Background,
        3 => Priority::Bulk,
        _ => Priority::Normal,
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum OverflowPolicy {
    ShedOldest,
    Refuse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PushOutcome {
    Enqueued,
    ShedOldest,
    Refused,
}

#[derive(Debug)]
pub(super) struct BoundedLane<T> {
    pub queue: VecDeque<T>,
    pub capacity: usize,
    overflow: OverflowPolicy,
}

impl<T> BoundedLane<T> {
    pub fn new(capacity: usize, overflow: OverflowPolicy) -> Self {
        Self {
            queue: VecDeque::new(),
            capacity: capacity.max(1),
            overflow,
        }
    }

    pub fn push(&mut self, item: T) -> PushOutcome {
        let outcome = if self.queue.len() < self.capacity {
            PushOutcome::Enqueued
        } else {
            match self.overflow {
                OverflowPolicy::ShedOldest => {
                    self.queue.pop_front();
                    PushOutcome::ShedOldest
                }
                OverflowPolicy::Refuse => return PushOutcome::Refused,
            }
        };
        self.queue.push_back(item);
        outcome
    }
}

#[derive(Debug)]
pub(super) struct PriorityLanes<T> {
    pub lanes: [BoundedLane<T>; LANES],
    floor: u32,
    higher_consecutive: u32,
}

impl<T> PriorityLanes<T> {
    pub fn new(capacity: usize, floor: u32, overflow: OverflowPolicy) -> Self {
        Self {
            lanes: std::array::from_fn(|_| BoundedLane::new(capacity, overflow)),
            // Zero must not let lower lanes leapfrog the first live request.
            floor: floor.max(1),
            higher_consecutive: 0,
        }
    }

    pub fn push(&mut self, priority: Priority, item: T) -> PushOutcome {
        self.lanes[lane_index(priority)].push(item)
    }

    pub fn visit_order(&self) -> [usize; LANES] {
        if self.higher_consecutive >= self.floor {
            [3, 2, 0, 1]
        } else {
            [0, 1, 2, 3]
        }
    }

    pub fn best_lane(&self) -> Option<usize> {
        self.visit_order()
            .into_iter()
            .find(|lane| !self.lanes[*lane].queue.is_empty())
    }

    /// Remove the first eligible item in grant order. Admission filters by
    /// budget class and cancellation; submitted work accepts every item.
    /// Only a removal spends the starvation count, never an inspection.
    /// Idle consumer polling must not postpone a user's background work by
    /// erasing the relief already earned through higher-priority grants.
    pub fn take(&mut self, mut eligible: impl FnMut(&T) -> bool) -> Option<T> {
        for lane in self.visit_order() {
            let Some(position) = self.lanes[lane].queue.iter().position(&mut eligible) else {
                continue;
            };
            let item = self.lanes[lane].queue.remove(position)?;
            if lane >= 2 || self.higher_consecutive >= self.floor {
                self.higher_consecutive = 0;
            }
            if lane < 2 {
                self.higher_consecutive = self.higher_consecutive.saturating_add(1);
            }
            return Some(item);
        }
        None
    }

    pub fn snapshot(&self) -> [LaneSnapshot; LANES] {
        std::array::from_fn(|lane| LaneSnapshot {
            priority: lane_priority(lane),
            depth: self.lanes[lane].queue.len(),
        })
    }
}
