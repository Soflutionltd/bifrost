//! Scope-level partition sequencing for the backfill orchestrator.
//!
//! # Why this is not a loop variable
//!
//! A barrier region is a property of the SCOPE, not of the partition that
//! happened to run into it: the region cannot be represented and cannot be
//! replayed, so any later partition's checkpoint certifies a prefix that
//! crosses it. Stopping the partition is therefore only half the rule, and the
//! half that used to be missing was the expensive one - both orchestrator loops
//! read a stopped partition as "merely degraded" and went straight on to the
//! next fixed partition or page window, handing checkpoints out beyond the
//! barrier exactly as if the stop had never happened.
//!
//! The loops no longer own that decision. They ask this driver for the next
//! partition and fold each outcome back into it, so a walk that stopped simply
//! has no next partition to hand out. There is no flag for a loop to forget to
//! check.

use bifrost_types::InventoryPartition;

use super::runner::{BackfillPartitionOutcome, PartitionVerdict, ScopeWalkStep};

/// How the remaining partitions of a scope are produced.
enum PartitionSource {
    /// A known, finite set. Exhaustion is running out of them.
    Fixed(std::vec::IntoIter<InventoryPartition>),
    /// An open-ended page walk. Exhaustion is a partition whose completion
    /// declares `PartitionEnd::Exhausted`.
    OpenPages { from: u32, chunk: u32 },
}

/// Entries and explicit removals accumulated by one scope walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkTotal(u64);

impl WalkTotal {
    /// Observed entries and explicit removals, for progress reporting.
    #[must_use]
    pub const fn count(self) -> u64 {
        self.0
    }

    fn observe(&mut self, outcome: &BackfillPartitionOutcome) {
        self.0 = self.0.saturating_add(outcome.seen);
    }

    /// Completion reports observed work. The durable key, not an inflated
    /// count, makes a completed walk outrank every partial checkpoint.
    pub(crate) fn completion_progress(self) -> bifrost_types::BackfillProgress {
        bifrost_types::BackfillProgress {
            items_done: self.0,
            items_estimated: Some(self.0),
        }
    }
}

/// Drives one scope's partition sequence and carries the walk's verdict.
pub struct ScopeWalkDriver {
    source: PartitionSource,
    stopped_at_barrier: bool,
    // These verdicts are not interchangeable with the scan's settled state:
    // a failed or incomplete partition can end enumeration without proving
    // completion, and a barrier stop remains separately observable to callers.
    // The scan settles only after delivery checks, and can revoke that settle
    // later when a consumer departs. Keeping the walk-local evidence preserves
    // those published capabilities instead of collapsing distinct lifetimes.
    /// Set once the source can produce nothing further, for any reason.
    exhausted: bool,
    awaiting_outcome: bool,
    completed: bool,
    total_seen: WalkTotal,
    /// The window most recently handed out, so an open-ended walk knows where
    /// the next one begins only AFTER its outcome is folded in.
    pending_to: Option<u32>,
}

impl ScopeWalkDriver {
    /// A walk over a known, finite partition set.
    #[must_use]
    pub fn fixed(mut partitions: Vec<InventoryPartition>) -> Self {
        // Partition counts are planning hints. An Outlook user expects an
        // apparently empty folder to be checked, not durably skipped because
        // a stale zero hint prevented the provider from being asked at all.
        if partitions.is_empty() {
            partitions.push(InventoryPartition::Full);
        }
        Self::with_source(PartitionSource::Fixed(partitions.into_iter()))
    }

    /// An open-ended page walk starting at `from`, in windows of `chunk`.
    #[must_use]
    pub fn open_pages(from: u32, chunk: u32) -> Self {
        Self::with_source(PartitionSource::OpenPages { from, chunk })
    }

    fn with_source(source: PartitionSource) -> Self {
        Self {
            source,
            stopped_at_barrier: false,
            exhausted: false,
            awaiting_outcome: false,
            completed: true,
            total_seen: WalkTotal(0),
            pending_to: None,
        }
    }

    /// The next partition to walk, or `None` if the scope must not be walked
    /// any further in this pass.
    pub fn next_partition(&mut self) -> Option<InventoryPartition> {
        if self.exhausted || self.stopped_at_barrier || self.awaiting_outcome {
            return None;
        }
        let next = match &mut self.source {
            PartitionSource::Fixed(partitions) => {
                let next = partitions.next();
                if next.is_none() {
                    self.exhausted = true;
                }
                next
            }
            PartitionSource::OpenPages { from, chunk } => {
                let to = from.saturating_add((*chunk).max(1));
                if to == *from {
                    // MoreBeyond at the representable limit cannot prove exhaustion.
                    // Leave the scope pending instead of repeating an empty window.
                    self.completed = false;
                    self.exhausted = true;
                    return None;
                }
                // `to` saturates upward from `from`, so the window is always
                // ordered; a refusal would only end the walk.
                let window = bifrost_types::PageWindow::new(*from, to)?;
                self.pending_to = Some(to);
                Some(InventoryPartition::Page(window))
            }
        };
        self.awaiting_outcome = next.is_some();
        next
    }

    /// Fold one partition's outcome back in.
    ///
    /// Returns the scope-level verdict, though callers driving the loop through
    /// `next_partition` do not need to inspect it: a stop is already recorded.
    pub fn fold(&mut self, outcome: &BackfillPartitionOutcome) -> ScopeWalkStep {
        self.awaiting_outcome = false;
        self.total_seen.observe(outcome);
        if !outcome.verdict.complete() {
            self.completed = false;
        }
        if outcome.verdict == PartitionVerdict::StoppedAtBarrier {
            self.stopped_at_barrier = true;
        }
        if self.stopped_at_barrier {
            self.completed = false;
            self.exhausted = true;
            return ScopeWalkStep::StopScopeWalk;
        }
        if let PartitionSource::OpenPages { from, .. } = &mut self.source {
            match outcome.verdict {
                PartitionVerdict::Exhausted { .. } => self.exhausted = true,
                PartitionVerdict::MoreBeyond { .. } => {
                    if let Some(to) = self.pending_to.take() {
                        *from = to;
                    }
                }
                PartitionVerdict::EndedUnannounced => self.exhausted = true,
                PartitionVerdict::StoppedAtBarrier => unreachable!("handled above"),
            }
        }
        ScopeWalkStep::RequestNextPartition
    }

    /// A partition failed outright. The scope stays Pending and the walk ends.
    pub fn fail(&mut self) {
        self.completed = false;
        self.exhausted = true;
    }

    /// Whether the scope may be marked complete and its sentinel emitted.
    #[must_use]
    pub fn completed(&self) -> bool {
        self.completed && self.exhausted && !self.awaiting_outcome
    }

    /// Whether the walk ended because a barrier stopped the scope.
    #[must_use]
    pub fn stopped_at_barrier(&self) -> bool {
        self.stopped_at_barrier
    }

    /// Inventory entries and explicit removals observed across every
    /// partition walked.
    #[must_use]
    pub fn total_seen(&self) -> WalkTotal {
        self.total_seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_counts_report_observed_work_without_an_ordering_bonus() {
        for count in [0, 1, 10, u64::MAX] {
            let progress = WalkTotal(count).completion_progress();
            assert_eq!(progress.items_done, count);
            assert_eq!(progress.items_estimated, Some(count));
        }
    }

    fn clean(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            seen,
            kept: seen,
            verdict: PartitionVerdict::MoreBeyond { degraded: false },
        }
    }

    fn exhausted(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            verdict: PartitionVerdict::Exhausted { degraded: false },
            ..clean(seen)
        }
    }

    fn barrier(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            seen,
            kept: seen,
            verdict: PartitionVerdict::StoppedAtBarrier,
        }
    }

    fn degraded(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            seen,
            kept: seen,
            verdict: PartitionVerdict::MoreBeyond { degraded: true },
        }
    }

    #[test]
    fn an_empty_fixed_plan_checks_full_inventory_before_completion() {
        let mut driver = ScopeWalkDriver::fixed(Vec::new());
        assert_eq!(driver.next_partition(), Some(InventoryPartition::Full));
        let _ = driver.fold(&exhausted(0));
        assert!(driver.next_partition().is_none());
        assert!(driver.completed());

        let mut failed = ScopeWalkDriver::fixed(Vec::new());
        assert_eq!(failed.next_partition(), Some(InventoryPartition::Full));
        failed.fail();
        assert!(!failed.completed());
    }

    #[test]
    fn completion_requires_enumeration_and_folded_outcomes() {
        let mut driver = ScopeWalkDriver::fixed(Vec::new());
        assert!(!driver.completed());
        assert!(driver.next_partition().is_some());
        assert!(driver.next_partition().is_none());
        assert!(!driver.completed());
        let _ = driver.fold(&exhausted(0));
        assert!(driver.next_partition().is_none());
        assert!(driver.completed());
    }

    #[test]
    fn more_beyond_the_page_limit_leaves_the_walk_pending() {
        let mut driver = ScopeWalkDriver::open_pages(u32::MAX - 1, 10);
        assert!(driver.next_partition().is_some());
        let _ = driver.fold(&clean(0));
        assert!(driver.next_partition().is_none());
        assert!(!driver.completed());
        let mut zero_chunk = ScopeWalkDriver::open_pages(0, 0);
        assert_eq!(
            zero_chunk.next_partition(),
            Some(InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 1).expect("nonempty window")
            ))
        );
    }

    fn fixed_three() -> ScopeWalkDriver {
        ScopeWalkDriver::fixed(vec![
            InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 10).expect("ordered window"),
            ),
            InventoryPartition::Page(
                bifrost_types::PageWindow::new(10, 20).expect("ordered window"),
            ),
            InventoryPartition::Page(
                bifrost_types::PageWindow::new(20, 30).expect("ordered window"),
            ),
        ])
    }

    /// The defect this driver exists for: a barrier in an EARLY partition of a
    /// MULTI-partition plan must stop the whole scope walk. A single-partition
    /// plan cannot fail this, because there is no later partition to leak into.
    #[test]
    fn a_barrier_in_an_early_partition_stops_every_later_partition() {
        let mut driver = fixed_three();
        let first = driver.next_partition().expect("first partition");
        assert_eq!(
            first,
            InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 10).expect("ordered window")
            )
        );
        let _ = driver.fold(&clean(5));

        let second = driver.next_partition().expect("second partition");
        assert_eq!(
            second,
            InventoryPartition::Page(
                bifrost_types::PageWindow::new(10, 20).expect("ordered window")
            )
        );
        let _ = driver.fold(&barrier(3));

        assert!(
            driver.next_partition().is_none(),
            "no partition after a barrier may be walked; its checkpoint would \
             certify a prefix crossing the blocked region"
        );
        assert!(!driver.completed(), "the sentinel must be withheld");
        assert!(driver.stopped_at_barrier());
    }

    /// A merely degraded partition is NOT a barrier: its obligations stay open
    /// in the ledger and the walk goes on. Conflating the two would stall every
    /// scope with a single unrepresentable object.
    #[test]
    fn a_degraded_partition_does_not_stop_the_walk() {
        let mut driver = fixed_three();
        driver.next_partition().expect("first");
        let _ = driver.fold(&degraded(4));
        assert_eq!(
            driver.next_partition(),
            Some(InventoryPartition::Page(
                bifrost_types::PageWindow::new(10, 20).expect("ordered window")
            ))
        );
        assert!(!driver.completed(), "but the sentinel is still withheld");
    }

    /// The same rule, one layer up in the open-ended walk: a barrier in an
    /// early WINDOW must stop the scope rather than advancing `from`.
    #[test]
    fn a_barrier_in_an_early_page_window_stops_the_open_ended_walk() {
        let mut driver = ScopeWalkDriver::open_pages(0, 50);
        assert_eq!(
            driver.next_partition(),
            Some(InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 50).expect("ordered window")
            ))
        );
        let _ = driver.fold(&clean(50));
        assert_eq!(
            driver.next_partition(),
            Some(InventoryPartition::Page(
                bifrost_types::PageWindow::new(50, 100).expect("ordered window")
            )),
            "a clean window advances"
        );
        let _ = driver.fold(&barrier(20));
        assert!(
            driver.next_partition().is_none(),
            "the window past a barrier must never be requested"
        );
        assert!(!driver.completed());
    }

    /// The walk stops where the producer declares exhaustion, and the entry
    /// count is never read as a declaration in either direction: a short or
    /// empty window that says `MoreBeyond` continues, and a non-empty window
    /// that says `Exhausted` ends the walk cleanly.
    #[test]
    fn open_pages_stop_only_on_a_declared_exhaustion() {
        let mut driver = ScopeWalkDriver::open_pages(0, 50);
        driver.next_partition().expect("first window");
        let _ = driver.fold(&clean(7));
        assert_eq!(
            driver.next_partition(),
            Some(InventoryPartition::Page(
                bifrost_types::PageWindow::new(50, 100).expect("ordered window")
            )),
            "a short page must not be read as end-of-inventory"
        );
        let _ = driver.fold(&clean(0));
        assert_eq!(
            driver.next_partition(),
            Some(InventoryPartition::Page(
                bifrost_types::PageWindow::new(100, 150).expect("ordered window")
            )),
            "an empty window that declares more beyond must not end the walk"
        );
        let _ = driver.fold(&exhausted(3));
        assert!(driver.next_partition().is_none());
        assert!(driver.completed(), "a declared exhaustion is a clean end");
        assert_eq!(driver.total_seen(), super::WalkTotal(10));
    }

    /// A partition stream that ended without any completion declared nothing.
    /// The walk must neither ask for windows forever nor complete the scope.
    #[test]
    fn open_pages_end_without_completing_when_nothing_was_declared() {
        let mut driver = ScopeWalkDriver::open_pages(0, 50);
        driver.next_partition().expect("first window");
        let _ = driver.fold(&BackfillPartitionOutcome {
            verdict: PartitionVerdict::EndedUnannounced,
            ..clean(0)
        });
        assert!(driver.next_partition().is_none());
        assert!(
            !driver.completed(),
            "an undeclared end must not write the completion sentinel"
        );
        assert!(!driver.stopped_at_barrier());
    }

    /// Once stopped, always stopped: a later clean outcome cannot re-open a
    /// walk a barrier closed.
    #[test]
    fn a_stop_is_sticky() {
        let mut driver = fixed_three();
        driver.next_partition().expect("first");
        let _ = driver.fold(&barrier(1));
        assert_eq!(driver.fold(&clean(9)), ScopeWalkStep::StopScopeWalk);
        assert!(driver.next_partition().is_none());
        assert!(!driver.completed());
    }

    #[test]
    fn a_failed_partition_ends_the_walk_without_completing_it() {
        let mut driver = fixed_three();
        driver.next_partition().expect("first");
        driver.fail();
        assert!(driver.next_partition().is_none());
        assert!(!driver.completed());
        assert!(
            !driver.stopped_at_barrier(),
            "a transport failure is not a barrier"
        );
    }
}
