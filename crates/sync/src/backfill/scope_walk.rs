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

use super::runner::{BackfillPartitionOutcome, BackfillScopeWalk, ScopeWalkStep};

/// How the remaining partitions of a scope are produced.
enum PartitionSource {
    /// A known, finite set. Exhaustion is running out of them.
    Fixed(std::vec::IntoIter<InventoryPartition>),
    /// An open-ended page walk. Exhaustion is a partition whose completion
    /// declares `PartitionEnd::Exhausted`.
    OpenPages { from: u32, chunk: u32 },
}

/// Drives one scope's partition sequence and carries the walk's verdict.
pub struct ScopeWalkDriver {
    source: PartitionSource,
    walk: BackfillScopeWalk,
    /// Set once the source can produce nothing further, for any reason.
    exhausted: bool,
    completed: bool,
    total_seen: u64,
    /// The window most recently handed out, so an open-ended walk knows where
    /// the next one begins only AFTER its outcome is folded in.
    pending_to: Option<u32>,
}

impl ScopeWalkDriver {
    /// A walk over a known, finite partition set.
    #[must_use]
    pub fn fixed(partitions: Vec<InventoryPartition>) -> Self {
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
            walk: BackfillScopeWalk::new(),
            exhausted: false,
            completed: true,
            total_seen: 0,
            pending_to: None,
        }
    }

    /// The next partition to walk, or `None` if the scope must not be walked
    /// any further in this pass.
    pub fn next_partition(&mut self) -> Option<InventoryPartition> {
        if self.exhausted || self.walk.stopped() {
            return None;
        }
        match &mut self.source {
            PartitionSource::Fixed(partitions) => partitions.next(),
            PartitionSource::OpenPages { from, chunk } => {
                let to = from.saturating_add(*chunk);
                self.pending_to = Some(to);
                Some(InventoryPartition::Page { from: *from, to })
            }
        }
    }

    /// Fold one partition's outcome back in.
    ///
    /// Returns the scope-level verdict, though callers driving the loop through
    /// `next_partition` do not need to inspect it: a stop is already recorded.
    pub fn fold(&mut self, outcome: &BackfillPartitionOutcome) -> ScopeWalkStep {
        self.total_seen = self.total_seen.saturating_add(outcome.seen);
        if !outcome.complete {
            // The partition finished but left coverage obligations open.
            // Completion is about the enumeration space being exhausted, and it
            // was not: writing the sentinel would make the next attach skip the
            // walk and turn a declared gap into a permanent one.
            self.completed = false;
        }
        let step = self.walk.admit(outcome);
        if step == ScopeWalkStep::StopScopeWalk {
            self.completed = false;
            self.exhausted = true;
            return step;
        }
        if let PartitionSource::OpenPages { from, .. } = &mut self.source {
            // The walk ends where the producer DECLARES its listing ran dry,
            // and the entry count is never read as that declaration: a server
            // capping a page below the window returns a short page with more
            // behind it, and a window whose ids all vanished before hydration
            // is empty with more behind it.
            match outcome.end {
                Some(bifrost_types::PartitionEnd::Exhausted) => self.exhausted = true,
                Some(_) => {
                    if let Some(to) = self.pending_to.take() {
                        *from = to;
                    }
                }
                // No completion at all: the stream ended without saying
                // where the enumeration stands. Walking on could ask for
                // windows forever, and completing would write a sentinel over
                // ground nothing proved, so the walk ends with the scope
                // still Pending.
                None => {
                    self.completed = false;
                    self.exhausted = true;
                }
            }
        }
        step
    }

    /// A partition failed outright. The scope stays Pending and the walk ends.
    pub fn fail(&mut self) {
        self.completed = false;
        self.exhausted = true;
    }

    /// Whether the scope may be marked complete and its sentinel emitted.
    #[must_use]
    pub fn completed(&self) -> bool {
        self.completed
    }

    /// Whether the walk ended because a barrier stopped the scope.
    #[must_use]
    pub fn stopped_at_barrier(&self) -> bool {
        self.walk.stopped()
    }

    /// Inventory entries and explicit removals observed across every
    /// partition walked.
    #[must_use]
    pub fn total_seen(&self) -> u64 {
        self.total_seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bifrost_types::PartitionEnd;

    fn clean(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            seen,
            kept: seen,
            complete: true,
            scope_walk: ScopeWalkStep::RequestNextPartition,
            end: Some(PartitionEnd::MoreBeyond),
        }
    }

    fn exhausted(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            end: Some(PartitionEnd::Exhausted),
            ..clean(seen)
        }
    }

    fn barrier(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            seen,
            kept: seen,
            complete: false,
            scope_walk: ScopeWalkStep::StopScopeWalk,
            end: None,
        }
    }

    fn degraded(seen: u64) -> BackfillPartitionOutcome {
        BackfillPartitionOutcome {
            seen,
            kept: seen,
            complete: false,
            scope_walk: ScopeWalkStep::RequestNextPartition,
            end: Some(PartitionEnd::MoreBeyond),
        }
    }

    fn fixed_three() -> ScopeWalkDriver {
        ScopeWalkDriver::fixed(vec![
            InventoryPartition::Page { from: 0, to: 10 },
            InventoryPartition::Page { from: 10, to: 20 },
            InventoryPartition::Page { from: 20, to: 30 },
        ])
    }

    /// The defect this driver exists for: a barrier in an EARLY partition of a
    /// MULTI-partition plan must stop the whole scope walk. A single-partition
    /// plan cannot fail this, because there is no later partition to leak into.
    #[test]
    fn a_barrier_in_an_early_partition_stops_every_later_partition() {
        let mut driver = fixed_three();
        let first = driver.next_partition().expect("first partition");
        assert_eq!(first, InventoryPartition::Page { from: 0, to: 10 });
        let _ = driver.fold(&clean(5));

        let second = driver.next_partition().expect("second partition");
        assert_eq!(second, InventoryPartition::Page { from: 10, to: 20 });
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
            Some(InventoryPartition::Page { from: 10, to: 20 })
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
            Some(InventoryPartition::Page { from: 0, to: 50 })
        );
        let _ = driver.fold(&clean(50));
        assert_eq!(
            driver.next_partition(),
            Some(InventoryPartition::Page { from: 50, to: 100 }),
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
            Some(InventoryPartition::Page { from: 50, to: 100 }),
            "a short page must not be read as end-of-inventory"
        );
        let _ = driver.fold(&clean(0));
        assert_eq!(
            driver.next_partition(),
            Some(InventoryPartition::Page { from: 100, to: 150 }),
            "an empty window that declares more beyond must not end the walk"
        );
        let _ = driver.fold(&exhausted(3));
        assert!(driver.next_partition().is_none());
        assert!(driver.completed(), "a declared exhaustion is a clean end");
        assert_eq!(driver.total_seen(), 10);
    }

    /// A partition stream that ended without any completion declared nothing.
    /// The walk must neither ask for windows forever nor complete the scope.
    #[test]
    fn open_pages_end_without_completing_when_nothing_was_declared() {
        let mut driver = ScopeWalkDriver::open_pages(0, 50);
        driver.next_partition().expect("first window");
        let _ = driver.fold(&BackfillPartitionOutcome {
            end: None,
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
