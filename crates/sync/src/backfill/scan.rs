//! Incarnation scheduling, retained walks and progress projection.
use super::plan::WalkLabels;
use super::{BackfillRegistry, BackfillState};
use crate::cursor::PendingCoverage;
use bifrost_types::{AccountId, CursorScope, InventoryPartition};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// First retry delay for a scope incarnation whose backfill did not
/// complete, and the ceiling the delay doubles towards.
pub(crate) const BACKFILL_RETRY_INITIAL: Duration = Duration::from_secs(5);
pub(crate) const BACKFILL_RETRY_CAP: Duration = Duration::from_secs(300);
const BACKFILL_BACKOFF: bifrost_types::BackoffPolicy =
    bifrost_types::BackoffPolicy::new(BACKFILL_RETRY_INITIAL, BACKFILL_RETRY_CAP, false);

// Owns sequencing and totals, but no admission, activity guard or provider stream.
pub(crate) struct ParkedWalk {
    pub(crate) driver: crate::backfill::ScopeWalkDriver,
    pub(crate) pending: InventoryPartition,
    pub(crate) fence_at_walk_start: crate::cursor::ScopeFence,
    pub(crate) undelivered_before: crate::cursor::UndeliveredMark,
    pub(crate) labels: &'static WalkLabels,
    pub(crate) until: tokio::time::Instant,
}

type ScanKey = (CursorScope, crate::cursor::ScopeIncarnation);

/// Scheduling phases are exclusive; retry history and loss evidence survive
/// holds and parked partitions because neither starts a new attempt.
#[derive(Default)]
enum ScanPhase {
    #[default]
    Fresh,
    Running,
    Parked(Box<ParkedWalk>),
    Failed(tokio::time::Instant),
    Settled,
}

#[derive(Default)]
struct IncarnationState {
    phase: ScanPhase,
    baseline: Option<crate::cursor::UndeliveredMark>,
    lost_pages: bool,
    operator_blocked: bool,
    retry: Option<crate::backoff::ReconnectBackoff>,
}

impl IncarnationState {
    fn baseline(&self) -> crate::cursor::UndeliveredMark {
        self.baseline
            .unwrap_or_else(PendingCoverage::initial_undelivered_mark)
    }

    fn projection(&self, watermark: crate::cursor::UndeliveredMark) -> BackfillState {
        match &self.phase {
            ScanPhase::Settled if self.baseline() == watermark => BackfillState::Completed,
            ScanPhase::Running => BackfillState::Running,
            _ if self.operator_blocked => BackfillState::OperatorBlocked,
            _ if self.lost_pages || self.baseline.is_some_and(|baseline| baseline != watermark) => {
                BackfillState::RestartingAfterLoss
            }
            ScanPhase::Parked(_) => BackfillState::Throttled,
            ScanPhase::Failed(_) => BackfillState::Retrying,
            _ => BackfillState::Pending,
        }
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        match &self.phase {
            ScanPhase::Parked(walk) => Some(walk.until),
            ScanPhase::Failed(until) => Some(*until),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ScanState {
    incarnations: HashMap<ScanKey, IncarnationState>,
    // This is attachment history, not scheduling state. Only the first sighting
    // of a fusion-owned scope is already inventoried by fusion; pruning it on
    // deletion would incorrectly skip the replacement incarnation too.
    fusion_skipped: HashSet<CursorScope>,
}

/// Which scope incarnations the orchestrator's rescan still owes a
/// cold-start pass.
///
/// The distinction that matters here is settled versus attempted. An
/// incarnation leaves the rescan permanently only when its backfill
/// reached a durable conclusion: the plan ran to completion, or the
/// checkpoint store already carries a completion marker. A partition
/// failure or a checkpoint-store read failure is transient by
/// construction - the scope is deliberately left `Pending` so it can be
/// retried - so filtering it out for the rest of the attachment would
/// mean one flaky request costs the scope its entire backfill until the
/// account is reattached. Failed incarnations stay eligible and come
/// back on an exponential delay, which is what keeps a permanently
/// failing scope from re-walking on every rescan tick.
#[derive(Default)]
pub(crate) struct BackfillScan {
    state: Arc<std::sync::Mutex<ScanState>>,
}

impl BackfillScan {
    // The registry retains the same state until detach; it owns no second
    // completion map and cannot independently change a scan verdict.
    pub(crate) fn publish(
        &self,
        registry: &BackfillRegistry,
        account: AccountId,
        cursors: Arc<crate::cursor::CursorRegistry>,
        coverage: Arc<PendingCoverage>,
    ) {
        let state = Arc::clone(&self.state);
        registry.observe(
            account,
            Arc::new(move |scope, expected| {
                let key = cursors.all_scope_incarnations().into_iter().find(
                    |(candidate, incarnation)| {
                        candidate == scope
                            && expected.is_none_or(|expected| expected == *incarnation)
                    },
                )?;
                let state = state.lock().expect("backfill scan poisoned");
                // Newly established scopes are pending even before the next scan.
                let projected = state
                    .incarnations
                    .get(&key)
                    .map_or(BackfillState::Pending, |entry| {
                        entry.projection(coverage.undelivered_watermark(scope))
                    });
                // Topology may have changed while waiting for the scan lock. Never
                // publish the old incarnation's completion as its replacement's.
                cursors
                    .all_scope_incarnations()
                    .contains(&key)
                    .then_some(projected)
            }),
        );
    }

    pub(crate) fn next_wake(&self) -> tokio::time::Instant {
        let state = self.state.lock().expect("backfill scan poisoned");
        let now = tokio::time::Instant::now();
        if state
            .incarnations
            .values()
            .any(|entry| matches!(&entry.phase, ScanPhase::Parked(walk) if walk.until <= now))
        {
            return now;
        }
        state
            .incarnations
            .values()
            .filter_map(IncarnationState::deadline)
            .filter(|until| *until > now)
            .fold(now + Duration::from_secs(1), std::cmp::min)
    }

    pub(crate) fn take_parked(&mut self, key: &ScanKey) -> Option<ParkedWalk> {
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.get_mut(key)?;
        if !matches!(entry.phase, ScanPhase::Parked(_)) {
            return None;
        }
        match std::mem::replace(&mut entry.phase, ScanPhase::Running) {
            ScanPhase::Parked(walk) => Some(*walk),
            _ => unreachable!(),
        }
    }

    pub(crate) fn park(&mut self, key: ScanKey, walk: ParkedWalk) {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .entry(key)
            .or_default()
            .phase = ScanPhase::Parked(Box::new(walk));
    }

    pub(crate) fn select(
        &mut self,
        available: Vec<ScanKey>,
        fusion_owned: &HashSet<CursorScope>,
        now: tokio::time::Instant,
        coverage: &PendingCoverage,
    ) -> Vec<ScanKey> {
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let live: HashSet<ScanKey> = available.iter().cloned().collect();
        // Incarnation identities are never reused: departed settled entries
        // cannot prevent any future walk, and must leave with the other residue.
        state.incarnations.retain(|key, _| live.contains(key));
        let mut pending = Vec::new();
        for key in available {
            let ScanState {
                incarnations,
                fusion_skipped,
            } = &mut *state;
            let entry = incarnations.entry(key.clone()).or_default();
            if let ScanPhase::Parked(walk) = &entry.phase {
                if walk.until <= now {
                    pending.push(key);
                }
                continue;
            }
            let watermark = coverage.undelivered_watermark(&key.0);
            if matches!(entry.phase, ScanPhase::Settled) {
                if entry.baseline() == watermark {
                    continue;
                }
                entry.phase = ScanPhase::Fresh;
                entry.lost_pages = true;
                tracing::warn!(target: "bifrost.sync.backfill", scope = ?key.0,
                    "a settled backfill walk lost pages; reopening the incarnation");
            }
            // A failed attempt retains its original baseline too: a receiver
            // can depart after failure, before the next attempt begins.
            if entry.baseline.is_some_and(|baseline| baseline != watermark) && !entry.lost_pages {
                entry.lost_pages = true;
                tracing::warn!(target: "bifrost.sync.backfill", scope = ?key.0,
                    "a previous backfill attempt lost pages; restarting from the beginning");
            }
            if fusion_owned.contains(&key.0)
                && entry.retry.is_none()
                && fusion_skipped.insert(key.0.clone())
            {
                entry.baseline = Some(watermark);
                entry.phase = ScanPhase::Settled;
                entry.lost_pages = false;
                continue;
            }
            if entry.deadline().is_some_and(|until| until > now) {
                continue;
            }
            pending.push(key);
        }
        pending
    }

    /// Settle against the walk's original baseline, never a fresh loss reading.
    /// An unknown baseline is conservative and reopens on any recorded loss.
    pub(crate) fn settled(&mut self, key: ScanKey) {
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.entry(key).or_default();
        entry.phase = ScanPhase::Settled;
        entry.retry = None;
        entry.lost_pages = false;
        entry.operator_blocked = false;
    }

    pub(crate) fn note_lost_pages(&mut self, key: ScanKey) {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .entry(key)
            .or_default()
            .lost_pages = true;
    }

    /// Snapshot at walk start, rather than selection of a batch of scopes.
    /// Loss during an earlier scope must not count against this scope's walk.
    pub(crate) fn begin_walk(
        &mut self,
        key: &ScanKey,
        coverage: &PendingCoverage,
    ) -> crate::cursor::UndeliveredMark {
        let watermark = coverage.undelivered_watermark(&key.0);
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.entry(key.clone()).or_default();
        entry.baseline = Some(watermark);
        entry.phase = ScanPhase::Running;
        entry.operator_blocked = false;
        watermark
    }

    #[cfg(test)]
    pub(crate) fn baseline_for(&self, key: &ScanKey) -> crate::cursor::UndeliveredMark {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .get(key)
            .map_or_else(
                PendingCoverage::initial_undelivered_mark,
                IncarnationState::baseline,
            )
    }

    pub(crate) fn restarts_from_scratch(&self, key: &ScanKey) -> bool {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .get(key)
            .is_some_and(|entry| entry.lost_pages)
    }

    pub(crate) fn mark_operator_blocked(&mut self, key: &ScanKey) {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .entry(key.clone())
            .or_default()
            .operator_blocked = true;
    }

    pub(crate) fn record_barrier_wait(&mut self, key: ScanKey) {
        self.record_attempt(key.clone(), false);
        self.mark_operator_blocked(&key);
    }

    pub(crate) fn record_attempt(&mut self, key: ScanKey, completed: bool) {
        if completed {
            self.settled(key);
            return;
        }
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.entry(key).or_default();
        let backoff = entry
            .retry
            .get_or_insert_with(|| crate::backoff::ReconnectBackoff::new(BACKFILL_BACKOFF));
        let delay = backoff.record_failure(crate::backoff::jitter_seed());
        entry.phase = ScanPhase::Failed(tokio::time::Instant::now() + delay);
    }
}

#[cfg(test)]
mod scan_projection_tests {
    use super::*;
    use bifrost_types::ChangeCursor;

    fn cursor(scope: CursorScope) -> ChangeCursor {
        ChangeCursor {
            scope,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Jmap,
                envelope_version: bifrost_types::PayloadVersion::new(1),
                bytes: vec![1],
            },
            advanced_through: None,
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        }
    }

    #[test]
    fn registry_projects_live_incarnation_and_loss_without_another_scan() {
        let registry = BackfillRegistry::new();
        let account = AccountId("projection".into());
        let cursors = Arc::new(crate::cursor::CursorRegistry::new());
        let coverage = Arc::new(PendingCoverage::new());
        let scope = CursorScope::Account;
        cursors.put(cursor(scope.clone()));
        let key = cursors.all_scope_incarnations().remove(0);
        let mut scan = BackfillScan::default();
        scan.publish(
            &registry,
            account.clone(),
            Arc::clone(&cursors),
            Arc::clone(&coverage),
        );
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Pending)
        );
        scan.begin_walk(&key, &coverage);
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Running)
        );
        scan.record_attempt(key.clone(), false);
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Retrying)
        );
        scan.record_barrier_wait(key.clone());
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::OperatorBlocked)
        );
        scan.begin_walk(&key, &coverage);
        scan.record_attempt(key.clone(), true);
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Completed)
        );
        coverage.note_undelivered(&scope, 1);
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::RestartingAfterLoss)
        );
        // Re-establishment must not inherit even a loss-free completion.
        scan.begin_walk(&key, &coverage);
        scan.record_attempt(key.clone(), true);
        cursors.delete(&scope);
        assert_eq!(registry.snapshot(&account, &scope), None);
        cursors.put(cursor(scope.clone()));
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Pending)
        );
        assert_eq!(registry.snapshot_incarnation(&account, &scope, key.1), None);
        let replacement = cursors.all_scope_incarnations()[0].1;
        assert_eq!(
            registry.snapshot_incarnation(&account, &scope, replacement),
            Some(BackfillState::Pending)
        );
        registry.forget_account(&account);
        assert_eq!(registry.snapshot(&account, &scope), None);
    }

    #[test]
    fn settled_incarnations_are_pruned_with_their_other_state() {
        let mut scan = BackfillScan::default();
        let coverage = PendingCoverage::new();
        let scope = CursorScope::Account;
        let key = (scope.clone(), crate::cursor::ScopeIncarnation::for_test(1));
        scan.begin_walk(&key, &coverage);
        scan.record_attempt(key.clone(), true);
        assert!(
            scan.select(
                vec![key],
                &HashSet::new(),
                tokio::time::Instant::now(),
                &coverage
            )
            .is_empty()
        );
        scan.select(
            Vec::new(),
            &HashSet::new(),
            tokio::time::Instant::now(),
            &coverage,
        );
        assert!(
            scan.state
                .lock()
                .expect("scan poisoned")
                .incarnations
                .is_empty()
        );
        let replacement = (scope, crate::cursor::ScopeIncarnation::for_test(2));
        assert_eq!(
            scan.select(
                vec![replacement.clone()],
                &HashSet::new(),
                tokio::time::Instant::now(),
                &coverage
            ),
            vec![replacement]
        );
    }
}

#[cfg(test)]
mod sequencing_tests {
    use super::*;
    use bifrost_types::{FolderId, ObjectType};
    #[test]
    fn backfill_skips_only_the_fusion_owned_incarnation_and_rescans_new_scopes() {
        let fused = CursorScope::Type(ObjectType::Email);
        let created = CursorScope::Folder(FolderId("created".into()));
        let fusion_owned = HashSet::from([fused.clone()]);
        let mut scan = BackfillScan::default();
        let coverage = crate::cursor::PendingCoverage::new();
        let now = tokio::time::Instant::now();

        let inc = crate::cursor::ScopeIncarnation::for_test;
        let first = scan.select(vec![(fused.clone(), inc(1))], &fusion_owned, now, &coverage);
        assert!(
            first.is_empty(),
            "fusion's inventory must not be double-walked"
        );

        let later = scan.select(
            vec![(fused.clone(), inc(2)), (created.clone(), inc(3))],
            &fusion_owned,
            now,
            &coverage,
        );
        assert_eq!(later, vec![(fused, inc(2)), (created, inc(3))]);
    }

    /// A completed walk retires the incarnation; a failed one does not.
    /// Filtering a failed incarnation out permanently would let a single
    /// transient partition or checkpoint-store error cost that scope its
    /// entire backfill for the rest of the attachment.
    #[tokio::test(start_paused = true)]
    async fn a_failed_backfill_incarnation_stays_eligible_for_retry() {
        let scope = CursorScope::Type(ObjectType::Email);
        let fusion_owned = HashSet::new();
        let mut scan = BackfillScan::default();
        let coverage = crate::cursor::PendingCoverage::new();
        let available = vec![(scope.clone(), crate::cursor::ScopeIncarnation::for_test(7))];

        assert_eq!(
            scan.select(
                available.clone(),
                &fusion_owned,
                tokio::time::Instant::now(),
                &coverage
            ),
            vec![(scope.clone(), crate::cursor::ScopeIncarnation::for_test(7))]
        );
        scan.record_attempt(
            (scope.clone(), crate::cursor::ScopeIncarnation::for_test(7)),
            false,
        );
        assert!(
            scan.select(
                available.clone(),
                &fusion_owned,
                tokio::time::Instant::now(),
                &coverage
            )
            .is_empty(),
            "a just-failed incarnation must back off rather than re-walk immediately"
        );

        tokio::time::advance(BACKFILL_RETRY_INITIAL * 2).await;
        assert_eq!(
            scan.select(
                available.clone(),
                &fusion_owned,
                tokio::time::Instant::now(),
                &coverage
            ),
            vec![(scope.clone(), crate::cursor::ScopeIncarnation::for_test(7))],
            "a failed incarnation must come back once its backoff elapses"
        );

        scan.record_attempt(
            (scope.clone(), crate::cursor::ScopeIncarnation::for_test(7)),
            true,
        );
        tokio::time::advance(BACKFILL_RETRY_CAP * 2).await;
        assert!(
            scan.select(
                available,
                &fusion_owned,
                tokio::time::Instant::now(),
                &coverage
            )
            .is_empty(),
            "a completed incarnation is never re-walked"
        );
    }

    /// An incarnation that leaves the registry takes its residue with it.
    ///
    /// A scope that is deleted, re-established, or simply never comes back otherwise
    /// leaves a retry deadline, a lost-pages mark and a baseline behind it - one set
    /// per incarnation, for the life of the attachment - and the orchestrator's
    /// "vanished during the subscriber wait" path adds one every time it fires.
    /// `settled` is deliberately kept: it is the memory that stops a concluded
    /// incarnation being walked again, and incarnation numbers only move forward.
    #[tokio::test(start_paused = true)]
    async fn a_departed_incarnation_leaves_no_residue_behind() {
        let scope = CursorScope::Type(ObjectType::Email);
        let fusion_owned = HashSet::new();
        let mut scan = BackfillScan::default();
        let coverage = crate::cursor::PendingCoverage::new();
        coverage.note_undelivered(&scope, 3);
        let key = (scope.clone(), crate::cursor::ScopeIncarnation::for_test(11));

        scan.select(
            vec![key.clone()],
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage,
        );
        assert_eq!(
            scan.begin_walk(&key, &coverage),
            crate::cursor::UndeliveredMark::for_test(3),
            "the walk begins under the scope's current reading"
        );
        scan.note_lost_pages(key.clone());
        assert_eq!(
            scan.baseline_for(&key),
            crate::cursor::UndeliveredMark::for_test(3),
            "which the scan holds for it"
        );
        assert!(scan.restarts_from_scratch(&key));

        // The registry no longer carries it.
        assert!(
            scan.select(
                Vec::new(),
                &fusion_owned,
                tokio::time::Instant::now(),
                &coverage
            )
            .is_empty()
        );
        assert_eq!(
            scan.baseline_for(&key),
            crate::cursor::UndeliveredMark::for_test(0),
            "a departed incarnation's baseline must not be kept for the life of the attachment"
        );
        assert!(!scan.restarts_from_scratch(&key), "nor its lost-pages mark");
    }

    /// A settle must be recorded against the reading its walk was CHECKED against,
    /// so a loss landing after those checks reopens the incarnation.
    ///
    /// The window is small and real: the writer refuses the completion marker and
    /// discards the rows on exactly such a loss, and if the rescan adopts the same
    /// event as its baseline it compares that reading against itself for ever - the
    /// two halves of one repair reading one event in opposite directions, with the
    /// scope never walked again for the rest of the attachment. `BackfillScan` takes
    /// the baseline through `begin_walk`, as the walk starts, for that reason: a
    /// settle cannot be handed a reading nothing validated, because it is not handed
    /// one at all.
    #[tokio::test(start_paused = true)]
    async fn a_loss_after_the_checks_reopens_the_incarnation_it_settled() {
        let scope = CursorScope::Type(ObjectType::Email);
        let fusion_owned = HashSet::new();
        let mut scan = BackfillScan::default();
        let coverage = crate::cursor::PendingCoverage::new();
        let available = vec![(scope.clone(), crate::cursor::ScopeIncarnation::for_test(3))];

        assert_eq!(
            scan.select(
                available.clone(),
                &fusion_owned,
                tokio::time::Instant::now(),
                &coverage
            ),
            vec![(scope.clone(), crate::cursor::ScopeIncarnation::for_test(3))]
        );
        // A loss BEFORE this walk begins belongs to whatever came earlier, and the
        // baseline it starts under has to include it - otherwise every scope after
        // the first in one rescan batch re-walks over its predecessor's losses.
        coverage.note_undelivered(&scope, 4);
        assert_eq!(
            scan.begin_walk(
                &(scope.clone(), crate::cursor::ScopeIncarnation::for_test(3)),
                &coverage
            ),
            crate::cursor::UndeliveredMark::for_test(4)
        );
        // And the loss that lands after every check this pass made, before the
        // settle, is precisely the interleaving the writer acts on.
        coverage.note_undelivered(&scope, 1);
        scan.record_attempt(
            (scope.clone(), crate::cursor::ScopeIncarnation::for_test(3)),
            true,
        );

        assert_eq!(
            scan.select(
                available,
                &fusion_owned,
                tokio::time::Instant::now(),
                &coverage
            ),
            vec![(scope.clone(), crate::cursor::ScopeIncarnation::for_test(3))],
            "the incarnation must be reopened: its walk was settled on checks made \
         before a page of it reached nobody"
        );
        assert!(
            scan.restarts_from_scratch(&(scope, crate::cursor::ScopeIncarnation::for_test(3))),
            "and reopened as a walk that may not trust its stored resume position"
        );
    }

    /// The same reopen, for an incarnation whose last attempt FAILED rather than
    /// settled.
    ///
    /// A departure sweep landing after a transient partition failure reaches nobody:
    /// `note_lost_pages` runs at a walk's END, and the next attempt takes the moved
    /// reading as its own baseline, so it RESUMES past the hole, judges itself whole
    /// and earns a durable marker. The discard request that loss raised then sits
    /// outstanding until `detach` acts on it and deletes the marker the second walk
    /// legitimately earned - a full re-walk on the next attach, once per flaky
    /// partition, for as long as the provider keeps failing. The failed attempt's
    /// baseline therefore SURVIVES the failure, and the rescan compares it exactly as
    /// it compares a settled one.
    #[tokio::test(start_paused = true)]
    async fn a_loss_after_a_failed_attempt_restarts_the_next_walk_from_scratch() {
        let scope = CursorScope::Type(ObjectType::Email);
        let fusion_owned = HashSet::new();
        let mut scan = BackfillScan::default();
        let coverage = crate::cursor::PendingCoverage::new();
        let key = (scope.clone(), crate::cursor::ScopeIncarnation::for_test(7));

        scan.select(
            vec![key.clone()],
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage,
        );
        assert_eq!(
            scan.begin_walk(&key, &coverage),
            crate::cursor::UndeliveredMark::for_test(0)
        );
        // The attempt ends on a transient failure, with every page it published
        // still in a live consumer's hands.
        scan.record_attempt(key.clone(), false);
        assert!(
            !scan.restarts_from_scratch(&key),
            "a plain failure is not a reason to distrust the stored resume position"
        );

        // Only now does the consumer holding one of that attempt's pages depart.
        coverage.note_undelivered(&scope, 1);
        scan.select(
            vec![key.clone()],
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage,
        );

        assert!(
            scan.restarts_from_scratch(&key),
            "the retry must start the walk over: resuming past the hole earns a durable \
         marker while the discard request the loss raised waits for a repair that \
         never comes, and detach then deletes the marker that walk earned"
        );
    }
}
