//! Partition planning and durable resume decisions.
use super::{BackfillPolicy, BackfillStrategy};
use crate::types::BackfillConfig;
use bifrost_types::{BackfillCheckpoint, CursorScope, InventoryPartition, InventoryPartitioning};

/// Resume decision for an open-ended page scope, derived purely from the
/// durably-persisted (consumer-acked) backfill checkpoint.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OpenPagesResume {
    /// Inventory was exhausted on a prior run; do not walk at all.
    Skip,
    /// Begin (or resume) the page walk at this position.
    ResumeFrom(u32),
}

/// True if the persisted checkpoint is the durable completion marker: a
/// prior run walked the whole scope to exhaustion and the consumer acked
/// it. The single skip-on-complete signal shared by fixed and open-ended
/// plans; `get_backfill` only ever returns consumer-acked checkpoints, so
/// a marker means every page the consumer needed was durably persisted.
pub(crate) fn backfill_complete_recorded(checkpoint: Option<&BackfillCheckpoint>) -> bool {
    checkpoint.is_some_and(|ck| {
        // Empty plans used to emit the legacy marker with zero seen + one.
        // Larger legacy markers prove a nonempty walk and remain valid. An
        // enterprise mail user expects a possibly skipped empty folder to be
        // checked once; new empty inventories earn the versioned marker.
        crate::backfill::marker::BackfillKey::decode(&ck.partition)
            .proves_completion(ck.progress.items_done)
    })
}

/// Map the persisted backfill checkpoint to a resume decision.
///
/// - The completion sentinel means a prior run reached exhaustion and the
///   consumer acked it: skip entirely.
/// - Any other `page:F:T` resumes at `T`. A SHORT page (`items_done <
///   T - F`) is deliberately NOT read as exhaustion: only a partition's
///   declared `PartitionEnd::Exhausted` ends a walk, and a partition may
///   legitimately emit fewer entries than its window width while the
///   scope still has results - ids that vanished between listing and
///   hydration, or objects dropped for arriving without an id. Only the
///   completion marker proves exhaustion; a final page without one costs
///   a single probe window on re-attach, whereas skipping on it would
///   silently drop every message past the window.
/// - No checkpoint, or an unrecognised partition kind, starts fresh at 0.
///
/// Resume never skips a window the consumer has not durably persisted,
/// because `get_backfill` only ever returns consumer-acked checkpoints.
pub(crate) fn open_pages_resume(checkpoint: Option<&BackfillCheckpoint>) -> OpenPagesResume {
    if backfill_complete_recorded(checkpoint) {
        return OpenPagesResume::Skip;
    }
    let Some(checkpoint) = checkpoint else {
        return OpenPagesResume::ResumeFrom(0);
    };
    if let Some((_, to)) = crate::backfill::partitioner::parse_page_partition(&checkpoint.partition)
    {
        return OpenPagesResume::ResumeFrom(to);
    }
    OpenPagesResume::ResumeFrom(0)
}

pub(crate) enum BackfillPlan {
    Fixed(Vec<InventoryPartition>),
    OpenPages { chunk: u32 },
}

/// What the orchestrator does with a scope incarnation once its durable
/// checkpoint has been read back.
///
/// The two plan shapes differ ONLY here. Both drive the same
/// `ScopeWalkDriver` afterwards, so folding their resume decisions into
/// one enum is what lets the walk itself be written once - barrier
/// handling, completion-marker withholding and registry bookkeeping
/// included.
pub(crate) enum ScopeResume {
    /// Durable evidence that a prior run finished this scope. Do not walk.
    Skip,
    /// Walk, starting from wherever the plan says to start.
    Walk(crate::backfill::ScopeWalkDriver),
}

/// Log wording for a walk, so collapsing the two plan arms into one loop
/// does not collapse their operator-facing messages into one another.
pub(crate) struct WalkLabels {
    pub(crate) resume_read_failed: &'static str,
    pub(crate) unresolved_coverage: &'static str,
    pub(crate) barrier_stop: &'static str,
    pub(crate) partition_failed: &'static str,
}

const FIXED_LABELS: WalkLabels = WalkLabels {
    resume_read_failed: "backfill resume read failed; re-walking all partitions",
    unresolved_coverage: "backfill partition completed with unresolved coverage; withholding the \
                          completion marker",
    barrier_stop: "backfill stopped at a barrier; refusing to walk any further partition of this \
                   scope",
    partition_failed: "backfill partition failed; leaving scope Pending",
};

const OPEN_PAGES_LABELS: WalkLabels = WalkLabels {
    resume_read_failed: "backfill resume read failed; re-walking from page 0",
    unresolved_coverage: "backfill page completed with unresolved coverage; withholding the \
                          completion marker",
    barrier_stop: "backfill stopped at a barrier; refusing to walk any further page window of \
                   this scope",
    partition_failed: "backfill page partition failed; leaving scope Pending",
};

impl BackfillPlan {
    pub(crate) fn labels(&self) -> &'static WalkLabels {
        match self {
            BackfillPlan::Fixed(_) => &FIXED_LABELS,
            BackfillPlan::OpenPages { .. } => &OPEN_PAGES_LABELS,
        }
    }

    /// Turn the durably-persisted (consumer-acked) checkpoint into a
    /// resume decision.
    ///
    /// A fixed plan's partitions are a known finite set with no positional
    /// "resume from here", so its durable signal is binary: the completion
    /// marker is present (skip the whole plan) or it is not (walk every
    /// partition; re-emitting acked pages is idempotent, so a crash
    /// mid-plan simply re-walks). An open-ended page walk resumes after the
    /// furthest durably-checkpointed window instead of re-paginating from
    /// page 0 on every re-attach.
    pub(crate) fn resume(self, stored: Option<&BackfillCheckpoint>) -> ScopeResume {
        match self {
            BackfillPlan::Fixed(partitions) => {
                if backfill_complete_recorded(stored) {
                    ScopeResume::Skip
                } else {
                    ScopeResume::Walk(crate::backfill::ScopeWalkDriver::fixed(partitions))
                }
            }
            BackfillPlan::OpenPages { chunk } => match open_pages_resume(stored) {
                OpenPagesResume::Skip => ScopeResume::Skip,
                OpenPagesResume::ResumeFrom(from) => {
                    ScopeResume::Walk(crate::backfill::ScopeWalkDriver::open_pages(from, chunk))
                }
            },
        }
    }

    /// The resume decision to use when the checkpoint READ failed, as
    /// opposed to came back empty. Deliberately the same answer as an
    /// absent checkpoint: a store error proves nothing about coverage, and
    /// skipping on it would silently drop everything a prior run had not
    /// finished.
    pub(crate) fn walk_from_scratch(self) -> ScopeResume {
        self.resume(None)
    }
}

pub(crate) fn backfill_plan_for(
    partitioning: InventoryPartitioning,
    scope: &CursorScope,
    config: BackfillConfig,
    now: jiff::Timestamp,
) -> BackfillPlan {
    match partitioning {
        InventoryPartitioning::Full => BackfillPlan::Fixed(vec![InventoryPartition::Full]),
        InventoryPartitioning::TimeWindowed => {
            let policy = BackfillPolicy::default();
            let plan = crate::backfill::partitioner::plan(&policy, now, 0);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::UidRange {
            max_uid: Some(max_uid),
            uid_validity,
        } => {
            let policy = BackfillPolicy {
                strategy: BackfillStrategy::UidRange {
                    chunk_size: config.uid_range_chunk,
                    uid_validity,
                },
                clock_skew: std::time::Duration::ZERO,
            };
            let plan = crate::backfill::partitioner::plan(&policy, now, max_uid);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::UidRange { max_uid: None, .. } => {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                scope = ?scope,
                "uid-range partitioning requested without max_uid; using full inventory pass"
            );
            BackfillPlan::Fixed(vec![InventoryPartition::Full])
        }
        InventoryPartitioning::PageCount {
            total: Some(total),
            page_size,
        } => {
            let policy = BackfillPolicy {
                strategy: BackfillStrategy::PageCount {
                    items_per_partition: page_size.unwrap_or(config.page_count_chunk).max(1),
                },
                clock_skew: std::time::Duration::ZERO,
            };
            let plan = crate::backfill::partitioner::plan(&policy, now, total);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::PageCount {
            total: None,
            page_size,
        } => BackfillPlan::OpenPages {
            chunk: page_size.unwrap_or(config.page_count_chunk).max(1),
        },
        _ => BackfillPlan::Fixed(vec![InventoryPartition::Full]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::ObjectType;
    /// Build a backfill checkpoint on a given partition key + progress for
    /// the open-pages resume tests.
    #[cfg(test)]
    fn checkpoint_on(
        partition: bifrost_types::Partition,
        items_done: u64,
    ) -> bifrost_types::BackfillCheckpoint {
        bifrost_types::BackfillCheckpoint {
            scope: CursorScope::Type(ObjectType::Email),
            partition,
            progress_marker: None,
            progress: bifrost_types::BackfillProgress {
                items_done,
                items_estimated: None,
            },
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        }
    }

    #[test]
    fn backfill_complete_recorded_detects_sentinel() {
        use super::backfill_complete_recorded;
        assert!(!backfill_complete_recorded(None));

        let marker = checkpoint_on(crate::backfill::partitioner::completion_partition(), 99);
        assert!(backfill_complete_recorded(Some(&marker)));

        // A real partition checkpoint (e.g. a fully-walked Full plan) is
        // not the completion signal.
        let full = checkpoint_on(
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full),
            99,
        );
        assert!(!backfill_complete_recorded(Some(&full)));
    }

    #[test]
    fn open_pages_resume_no_checkpoint_starts_fresh() {
        use super::{OpenPagesResume, open_pages_resume};
        assert_eq!(open_pages_resume(None), OpenPagesResume::ResumeFrom(0));
    }

    #[test]
    fn open_pages_resume_completion_marker_skips() {
        use super::{OpenPagesResume, open_pages_resume};
        let ck = checkpoint_on(crate::backfill::partitioner::completion_partition(), 1234);
        assert_eq!(open_pages_resume(Some(&ck)), OpenPagesResume::Skip);
    }

    #[test]
    fn open_pages_resume_short_final_page_resumes_rather_than_skipping() {
        use super::{OpenPagesResume, open_pages_resume};
        // A 256-of-500 page is NOT proof the inventory ran out inside the
        // window: a partition may emit fewer entries than its width while
        // the scope still has results (ids deleted between listing and
        // hydration, id-less objects dropped). Only the completion marker
        // proves exhaustion. Skipping here would silently drop every
        // message past the window on re-attach.
        let key =
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 500).expect("ordered window"),
            ));
        let ck = checkpoint_on(key, 256);
        assert_eq!(
            open_pages_resume(Some(&ck)),
            OpenPagesResume::ResumeFrom(500)
        );
    }

    #[test]
    fn open_pages_resume_full_page_resumes_after_it() {
        use super::{OpenPagesResume, open_pages_resume};
        let key =
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page(
                bifrost_types::PageWindow::new(500, 1000).expect("ordered window"),
            ));
        let ck = checkpoint_on(key, 500);
        assert_eq!(
            open_pages_resume(Some(&ck)),
            OpenPagesResume::ResumeFrom(1000)
        );
    }

    #[test]
    fn open_pages_resume_unrecognised_partition_starts_fresh() {
        use super::{OpenPagesResume, open_pages_resume};
        let key =
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full);
        let ck = checkpoint_on(key, 7);
        assert_eq!(open_pages_resume(Some(&ck)), OpenPagesResume::ResumeFrom(0));
    }

    /// The orchestrator drives both plan shapes through one shared walk, so
    /// `BackfillPlan::resume` is now the ONLY place the two differ. These pin
    /// each arm on both sides of the skip/walk choice: a wired-up-backwards
    /// resume would either re-walk a finished scope every attach or, far
    /// worse, skip a scope that never finished.
    #[test]
    fn a_fixed_plan_skips_only_on_its_completion_marker() {
        use super::{BackfillPlan, ScopeResume};

        let marker = checkpoint_on(crate::backfill::partitioner::completion_partition(), 99);
        let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
        assert!(matches!(plan.resume(Some(&marker)), ScopeResume::Skip));

        // No marker: walk every partition. A fixed plan has no positional
        // resume, so re-walking is the only correct answer.
        let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
        let ScopeResume::Walk(mut driver) = plan.resume(None) else {
            panic!("a fixed plan without a completion marker must walk");
        };
        assert_eq!(
            driver.next_partition(),
            Some(bifrost_types::InventoryPartition::Full)
        );

        // A checkpoint that is merely a walked partition is NOT the
        // completion signal. Treating any stored row as "done" would strand
        // a plan that crashed after its first partition acked.
        let partial = checkpoint_on(
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full),
            42,
        );
        let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
        let ScopeResume::Walk(mut driver) = plan.resume(Some(&partial)) else {
            panic!("only the completion marker may skip a fixed plan");
        };
        assert_eq!(
            driver.next_partition(),
            Some(bifrost_types::InventoryPartition::Full)
        );
    }

    #[test]
    fn an_open_pages_plan_resumes_at_the_acked_window() {
        use super::{BackfillPlan, ScopeResume};

        let marker = checkpoint_on(crate::backfill::partitioner::completion_partition(), 99);
        let plan = BackfillPlan::OpenPages { chunk: 500 };
        assert!(matches!(plan.resume(Some(&marker)), ScopeResume::Skip));

        let key =
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 500).expect("ordered window"),
            ));
        let acked = checkpoint_on(key, 500);
        let plan = BackfillPlan::OpenPages { chunk: 500 };
        let ScopeResume::Walk(mut driver) = plan.resume(Some(&acked)) else {
            panic!("an unfinished page walk must resume, not skip");
        };
        assert_eq!(
            driver.next_partition(),
            Some(bifrost_types::InventoryPartition::Page(
                bifrost_types::PageWindow::new(500, 1000).expect("ordered window")
            )),
            "the walk resumes after the furthest durably-acked window"
        );
    }

    /// A checkpoint READ failure is not evidence of coverage. Both shapes
    /// must fall back to a full walk rather than a skip, or one flaky store
    /// call silently drops everything a prior run had not finished.
    #[test]
    fn a_failed_resume_read_walks_from_scratch_on_both_plan_shapes() {
        use super::{BackfillPlan, ScopeResume};

        let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
        let ScopeResume::Walk(mut driver) = plan.walk_from_scratch() else {
            panic!("a failed read must not skip a fixed plan");
        };
        assert_eq!(
            driver.next_partition(),
            Some(bifrost_types::InventoryPartition::Full)
        );

        let plan = BackfillPlan::OpenPages { chunk: 250 };
        let ScopeResume::Walk(mut driver) = plan.walk_from_scratch() else {
            panic!("a failed read must not skip a page walk");
        };
        assert_eq!(
            driver.next_partition(),
            Some(bifrost_types::InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 250).expect("ordered window")
            )),
            "a failed read restarts at page 0, never at a guessed offset"
        );
    }

    #[test]
    fn legacy_empty_completion_rechecks_but_nonempty_completion_remains_valid() {
        use super::{BackfillPlan, OpenPagesResume, ScopeResume, open_pages_resume};
        for items_done in [0, 1] {
            let legacy = checkpoint_on(bifrost_types::Partition(b"complete".to_vec()), items_done);
            let ScopeResume::Walk(mut driver) =
                BackfillPlan::Fixed(Vec::new()).resume(Some(&legacy))
            else {
                panic!("legacy empty marker must not skip inventory");
            };
            assert_eq!(
                driver.next_partition(),
                Some(bifrost_types::InventoryPartition::Full)
            );
            assert_eq!(
                open_pages_resume(Some(&legacy)),
                OpenPagesResume::ResumeFrom(0)
            );
        }
        let legacy = checkpoint_on(bifrost_types::Partition(b"complete".to_vec()), 2);
        assert!(matches!(
            BackfillPlan::Fixed(Vec::new()).resume(Some(&legacy)),
            ScopeResume::Skip
        ));
        assert_eq!(open_pages_resume(Some(&legacy)), OpenPagesResume::Skip);
        let checked_empty = checkpoint_on(crate::backfill::partitioner::completion_partition(), 1);
        assert!(matches!(
            BackfillPlan::Fixed(Vec::new()).resume(Some(&checked_empty)),
            ScopeResume::Skip
        ));
        assert_eq!(
            open_pages_resume(Some(&checked_empty)),
            OpenPagesResume::Skip
        );
    }
}
