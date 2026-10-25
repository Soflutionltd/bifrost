//! Push reconciler task.
//!
//! One task per account. Drains bounded invalidation intent and retains
//! pending scopes across pauses and repairs, driving `changes_stream` against the current
//! cursor for each affected scope. Output flows to the multiplexer's
//! broadcast so consumers see a single unified `Change` stream.

use std::collections::HashSet;
use std::sync::Arc;

use bifrost_types::{Account, AccountId, CursorScope, HintPayload};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use arc_swap::ArcSwap;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;
use crate::multiplexer::{ChangeDelivery, ChangesEvent, ReopenRequest, drive_changes_stream};

pub struct Reconciler {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: Arc<CursorRegistry>,
    /// The account's change delivery gate. The reconciler's drives publish
    /// checkpoints, and a published checkpoint has to know whether a NUMBERED
    /// receiver took delivery; warnings and terminations go on its raw sender.
    pub delivery: Arc<ChangeDelivery>,
    pub boundary: BoundaryView,
    pub shutdown: CancellationToken,
    pub control: SyncControl,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    /// Engine-wide throttle bucket. The reconciler honors account-wide
    /// deadlines before driving a hinted scope and records deadlines
    /// its own Retry outcomes carry.
    pub throttles: Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
    pub scheduler: crate::scheduler::Scheduler,
    /// The multiplexer's per-scope poll tokens, shared rather than copied.
    ///
    /// A scope whose poll or push drive reached a TERMINAL verdict leaves a
    /// parked tombstone here, and both the 1s poll scan and this reconciler
    /// honor it. Without the shared record, a later push hint re-drove a
    /// terminally failed scope - re-broadcasting `Terminated` and re-running
    /// a wire call that cannot succeed.
    pub scope_tokens: crate::multiplexer::ScopeTokens,
    pub account_generation: tokio::sync::watch::Receiver<u64>,
    #[cfg(test)]
    pub(crate) attempts: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

#[derive(Default)]
struct WorkSet {
    full: bool,
    scopes: HashSet<CursorScope>,
    hold: Option<(u64, tokio::time::Instant)>,
}

impl WorkSet {
    fn merge(
        &mut self,
        pending: super::PendingPush,
        registry: &CursorRegistry,
        ingress: &super::PushIngress,
    ) -> bool {
        let hinted = !pending.scopes.is_empty() || !pending.memberships.is_empty();
        self.full |= pending.full;
        if !self.full {
            self.scopes.extend(pending.scopes);
            for membership in pending.memberships {
                self.scopes
                    .extend(registry.scopes_for_membership(&membership));
            }
            if hinted && self.scopes.len() > ingress.capacity() {
                self.full = true;
                ingress.count_coarsening();
            }
        }
        if self.full {
            self.scopes.clear();
        }
        pending.closed
    }
}

impl Reconciler {
    /// Retain invalidation work across pause, checkpoint and account repair.
    /// Control events are handled synchronously by the sink.
    pub async fn run(mut self, ingress: Arc<super::PushIngress>) {
        let mut work = WorkSet::default();
        let mut deferred = HashSet::new();
        let mut closed = false;
        loop {
            while matches!(
                self.boundary.peek(),
                crate::cancel::BoundaryRequest::Pause
                    | crate::cancel::BoundaryRequest::CheckpointNow
            ) {
                tokio::select! {
                    () = self.shutdown.cancelled() => return,
                    changed = self.boundary.changed() => if changed.is_none() { return; },
                }
            }
            if self.shutdown.is_cancelled() {
                return;
            }
            closed |= work.merge(ingress.take(), &self.cursors, &ingress);
            if let Some((generation, until)) = work.hold {
                let current = *self.account_generation.borrow_and_update();
                if generation != current || tokio::time::Instant::now() >= until {
                    work.hold = None;
                }
            }
            let ready: Vec<_> = deferred
                .iter()
                .filter(|scope| {
                    crate::recovery::throttle_wait(
                        &self.throttles,
                        &self.account_id,
                        &self.scope_keys(scope),
                    )
                    .is_none()
                })
                .cloned()
                .collect();
            for scope in ready {
                deferred.remove(&scope);
                work.scopes.insert(scope);
            }
            if work.hold.is_none() && (work.full || !work.scopes.is_empty()) {
                if work.full {
                    work.full = false;
                    work.scopes.extend(self.cursors.all_scopes());
                }
                if let Err(error) = self.drive_scopes(&mut work, &mut deferred).await {
                    tracing::warn!(target: "bifrost.sync.reconcile", %error, "push reconcile failed");
                }
                if closed {
                    return;
                }
                continue;
            }
            if closed {
                return;
            }
            let next_due = if work.hold.is_some() {
                None
            } else {
                self.next_deferred_due(&deferred)
            };
            let hold_wait = work
                .hold
                .map(|(_, until)| until.saturating_duration_since(tokio::time::Instant::now()));
            tokio::select! {
                () = self.shutdown.cancelled() => return,
                () = ingress.changed() => {},
                () = sleep_or_forever(next_due) => {},
                () = sleep_or_forever(hold_wait) => {},
                changed = self.account_generation.changed(), if work.hold.is_some() => {
                    if changed.is_err() { return; }
                }
                changed = self.boundary.changed() => if changed.is_none() { return; },
            }
        }
    }

    /// Declared throttle keys for one scope's drive.
    fn scope_keys(&self, scope: &CursorScope) -> Vec<bifrost_types::ThrottleKey> {
        crate::recovery::declared_keys(
            self.account.load_full().as_ref().as_ref(),
            &self.account_id,
            bifrost_types::ThrottleWork::Scope(scope),
        )
    }

    /// The shortest remaining hold across the deferred scopes, `None` when
    /// nothing is deferred. Zero when some deferred scope is already free.
    fn next_deferred_due(&self, deferred: &HashSet<CursorScope>) -> Option<std::time::Duration> {
        deferred
            .iter()
            .map(|scope| {
                let keys = self.scope_keys(scope);
                crate::recovery::throttle_wait(&self.throttles, &self.account_id, &keys)
                    .unwrap_or_default()
            })
            .min()
    }

    /// Drive each scope once, deferring any held by a scope-local throttle.
    /// Parking, deletion and every hold are re-checked here at DISPATCH, not
    /// when the scope was queued, so a scope deferred and then parked or
    /// deleted is never driven.
    async fn drive_scopes(
        &mut self,
        work: &mut WorkSet,
        deferred: &mut HashSet<CursorScope>,
    ) -> Result<(), Error> {
        let scopes: Vec<_> = work.scopes.iter().cloned().collect();
        for scope in scopes {
            #[cfg(test)]
            if let Some(attempts) = &self.attempts {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            work.scopes.remove(&scope);
            // This dispatch speaks for any older deferral of the same scope:
            // drop it, so a fresh hint that drives the scope does not leave a
            // due entry behind for the timer to drive a second time. Every
            // path below that is held again re-inserts it.
            deferred.remove(&scope);
            // A successful scope restart or account replacement changes the
            // registry fence, invalidating the old incarnation's terminal state.
            crate::multiplexer::retire_stale_scope_token(&self.scope_tokens, &self.cursors, &scope);
            if crate::multiplexer::scope_is_parked(&self.scope_tokens, &scope) {
                tracing::debug!(
                    target: "bifrost.sync.reconcile",
                    account = ?self.account_id,
                    scope = ?scope,
                    "hinted scope is terminally parked; not re-driving it"
                );
                continue;
            }
            // An ACCOUNT-WIDE hold blocks every scope equally, so sleeping it
            // out inline stalls nothing that could otherwise run. Re-checked
            // after waking: a longer deadline can land mid-sleep.
            while let Some(wait) =
                crate::recovery::account_throttle_wait(&self.throttles, &self.account_id)
            {
                tracing::debug!(
                    target: "bifrost.sync.reconcile",
                    account = ?self.account_id,
                    scope = ?scope,
                    wait_secs = wait.as_secs(),
                    "reconcile deferred by shared throttle deadline"
                );
                tokio::select! {
                    () = self.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(wait) => {}
                }
            }
            // A SCOPE-LOCAL hold defers this scope alone; the sweep carries on.
            let keys = self.scope_keys(&scope);
            if crate::recovery::throttle_wait(&self.throttles, &self.account_id, &keys).is_some() {
                deferred.insert(scope);
                continue;
            }
            let _admission = tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                permit = self.scheduler.admit(
                    self.account_id.clone(),
                    self.control.priority_snapshot(),
                    crate::scheduler::WorkKind::Sync,
                ) => match permit {
                    Ok(permit) => permit,
                    Err(error) => {
                        // Same rule as E6: one scope's failure must not
                        // abandon its siblings in this sweep.
                        tracing::warn!(
                            target: "bifrost.sync.scheduler",
                            scope = ?scope,
                            %error,
                            "push admission refused; skipping this scope for the sweep"
                        );
                        continue;
                    }
                },
            };
            // Admission can park, and a hold recorded meanwhile predates this
            // drive. Re-check with the permit in hand; on a hold, release it
            // and defer, rather than carry a permit across a throttle wait.
            if crate::recovery::throttle_wait(&self.throttles, &self.account_id, &keys).is_some() {
                drop(_admission);
                deferred.insert(scope);
                continue;
            }
            // Same reasoning as the poll loop's: the drive's own boundary arm
            // answers `Stop` and `Pause`, and this covers root cancellation
            // arriving before this account's detach publishes `Stop`.
            //
            // The reconciler has no entry in `ScopeTokens` - that map is the
            // POLL TASK's lifetime keyed by scope, and this task is a second
            // producer on the same lane - so scope deletion reaches a stalled
            // drive of ours through the registry instead: `with_drive` races
            // the drive against the scope's own liveness token, which
            // `CursorRegistry::delete` cancels as it drops the cursor. Without
            // that, a scope deleted while one of our drives was parked on a
            // wedged provider stream kept the scope's drive lease, and the
            // replacement incarnation waited on `claim_drive` forever.
            let driven = tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                driven = self
                .cursors
                .with_drive(&scope, |cursor, registry_generation| {
                    let account_swap = self.account.load_full();
                    let cursors = Arc::clone(&self.cursors);
                    let delivery = Arc::clone(&self.delivery);
                    let boundary = self.boundary.clone();
                    let control = self.control.clone();
                    let scope = scope.clone();
                    let throttles = Arc::clone(&self.throttles);
                    let account_id = self.account_id.clone();
                    async move {
                        // The LAST check before the wire: `with_drive` can park
                        // on the drive lease after admission, and a hold
                        // recorded meanwhile predates this drive. The keys are
                        // asked of the account this drive runs against, not
                        // cached across the waits: a reopen can change them.
                        let account: &dyn Account = account_swap.as_ref().as_ref();
                        let final_keys = crate::recovery::declared_keys(
                            account,
                            &account_id,
                            bifrost_types::ThrottleWork::Scope(&scope),
                        );
                        if crate::recovery::throttle_wait(&throttles, &account_id, &final_keys)
                            .is_some()
                        {
                            return (final_keys, None);
                        }
                        let outcome = drive_changes_stream(
                            account,
                            scope,
                            cursor,
                            cursors,
                            delivery,
                            boundary,
                            Some(control),
                            Some(registry_generation),
                        )
                        .await;
                        // The keys travel back with the outcome: whether a
                        // throttled drive's hint is kept is a question about
                        // THIS account's declaration, not the one computed
                        // before admission.
                        (final_keys, Some((registry_generation, outcome)))
                    }
                }) => driven,
            };
            let (keys, driven) = match driven {
                Some((keys, driven)) => (keys, Some(driven)),
                None => (keys, None),
            };
            drop(_admission);
            // No result: the scope is gone, or left mid-drive. Either way it is
            // not ours to re-drive, and its siblings in this hint still are.
            let Some(driven) = driven else {
                continue;
            };
            // Held at the last check: defer it like any other held scope.
            let Some(outcome) = driven else {
                deferred.insert(scope);
                continue;
            };
            let (drive_generation, outcome) = outcome;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                // An account error already carries a complete recovery
                // verdict. Surviving sibling scopes of a multi-scope hint
                // must not cost the failing scope its recovery: logging this
                // and moving on leaves its cursor installed, so push
                // re-terminates on every hint until a later poll hits the
                // same failure. Normalize onto the same path an
                // account-authored `Terminated` takes, exactly as the poll
                // loop's `handle_drive_outcome` does.
                Err(Error::Account(error)) => ChangesEvent::Terminated(error),
                // Engine-internal errors carry no recovery verdict and name
                // nothing to reset. Log and keep sweeping.
                Err(error) => {
                    tracing::warn!(
                        target: "bifrost.sync.reconcile",
                        account = ?self.account_id,
                        scope = ?scope,
                        error = %error,
                        "hinted scope drive failed; continuing remaining scopes"
                    );
                    continue;
                }
            };
            match outcome {
                ChangesEvent::Advanced | ChangesEvent::Done | ChangesEvent::Stopped => {}
                ChangesEvent::Paused => {
                    work.scopes.insert(scope);
                    return Ok(());
                }
                ChangesEvent::Terminated(error) => {
                    use crate::recovery::{
                        RecoveryPlan, directive_target_scope, plan_recovery, retry_delay,
                    };
                    let original = error.clone();
                    match plan_recovery(error) {
                        // Retry and Reconcile both record their deadline in the
                        // shared throttle bucket and then SKIP this scope,
                        // continuing the sweep. The reconciler is one task for
                        // the whole account, so sleeping the delay out inline
                        // froze every other scope's push reconciliation for a
                        // provider `Retry-After` that named one scope - while
                        // the watch channel filled and coalesced behind it. The
                        // sleep was redundant anyway: every drive path consults
                        // the bucket (this sweep does so at the top of each
                        // scope, under a shutdown select), so the deadline is
                        // honored without a task-wide stall.
                        RecoveryPlan::Retry(advice) => {
                            crate::recovery::record_throttle(
                                &self.throttles,
                                &self.account_id,
                                &advice,
                                &original,
                            );
                            tracing::debug!(
                                target: "bifrost.sync.reconcile",
                                account = ?self.account_id,
                                scope = ?scope,
                                delay_secs = retry_delay(
                                    &advice,
                                    std::time::SystemTime::now(),
                                    std::time::Duration::from_secs(1),
                                ).as_secs(),
                                "hinted scope throttled; skipping it and continuing the sweep"
                            );
                            self.defer_if_held(&scope, &keys, deferred);
                            continue;
                        }
                        RecoveryPlan::Reconcile(advice) => {
                            crate::recovery::record_reconcile_throttle(
                                &self.throttles,
                                &self.account_id,
                                &advice,
                                &original,
                            );
                            tracing::debug!(
                                target: "bifrost.sync.reconcile",
                                account = ?self.account_id,
                                scope = ?scope,
                                delay_secs = crate::recovery::reconcile_delay(
                                    &advice,
                                    std::time::SystemTime::now(),
                                    std::time::Duration::from_secs(1),
                                ).as_secs(),
                                "hinted scope deferred for reconcile; continuing the sweep"
                            );
                            self.defer_if_held(&scope, &keys, deferred);
                            continue;
                        }
                        RecoveryPlan::Engine(directive) => {
                            let directive_scope = directive_target_scope(&directive);
                            // `directive_target_scope` IS the blast radius:
                            // `Some(scope)` names a single scope to reset, so
                            // the rest of this hint's scopes are unaffected
                            // and must still be swept; `None` is account-wide
                            // (RestartAccount, SchemaIncompatible,
                            // OperatorOverrideRequired), and driving further
                            // scopes into a reset the engine is about to run
                            // wastes wire calls against cursors it is about
                            // to discard.
                            let account_wide = directive_scope.is_none();
                            let _ = self
                                .reopen_tx
                                .send(ReopenRequest::Recovery {
                                    scope: directive_scope,
                                    error: original,
                                })
                                .await;
                            if account_wide {
                                work.hold = Some((
                                    *self.account_generation.borrow_and_update(),
                                    tokio::time::Instant::now()
                                        + std::time::Duration::from_secs(30),
                                ));
                                return Ok(());
                            }
                            continue;
                        }
                        RecoveryPlan::Terminal(_) => {
                            crate::multiplexer::park_scope_for_drive(
                                &self.scope_tokens,
                                &self.cursors,
                                &scope,
                                drive_generation,
                            );
                            continue;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// After a throttled drive recorded its hold: keep the hint alive by
    /// deferring the scope, but ONLY when a hold now actually covers it.
    /// Deferring a scope that nothing holds (a retry carrying no hint, or one
    /// lifted to no key) would make it due immediately and re-drive the same
    /// failure in a tight loop; such a scope is skipped as it always was, and
    /// its poll loop picks the changes up at its own cadence.
    fn defer_if_held(
        &self,
        scope: &CursorScope,
        keys: &[bifrost_types::ThrottleKey],
        deferred: &mut HashSet<CursorScope>,
    ) {
        if crate::recovery::throttle_wait(&self.throttles, &self.account_id, keys).is_some() {
            deferred.insert(scope.clone());
        }
    }
}

/// Sleep for `due`, or never when nothing is deferred.
async fn sleep_or_forever(due: Option<std::time::Duration>) {
    match due {
        Some(due) => tokio::time::sleep(due).await,
        None => std::future::pending().await,
    }
}

/// Enumerate the cursor scopes a hint covers.
///
/// - `SpecificCursorScope(s)` -> `vec![s]`
/// - `SpecificMembership(m)` -> registry's membership index
/// - `Unknown` -> every registered cursor scope.
///
/// The `SpecificMembership` arm has NO production producer today: every push
/// hint built in imap, jmap and graph is `Unknown` or `SpecificCursorScope`.
/// It is kept rather than removed because it is the correct routing for a
/// membership-shaped invalidation and the index it consults is maintained
/// anyway (`membership_to_cursor_scopes` is a live consumer), but do not read
/// this arm as evidence that the push path exercises the membership index -
/// it does not. Nothing is lost while it is unreachable: the `Unknown`
/// fallback is a full reconcile, which is strictly wider.
#[must_use]
pub fn scopes_for_hint(registry: &CursorRegistry, hint: &HintPayload) -> Vec<CursorScope> {
    match hint {
        HintPayload::SpecificCursorScope(s) => vec![s.clone()],
        HintPayload::SpecificMembership(m) => registry.scopes_for_membership(m),
        HintPayload::Unknown => registry.all_scopes(),
        // `HintPayload` is `#[non_exhaustive]`; fall back to a full
        // reconcile under uncertainty - that is the contract for
        // `Unknown` and the safest default for any new variant.
        _ => registry.all_scopes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::push::{PendingPush, PushIngress};
    use bifrost_types::{FolderId, MembershipScope};
    fn scope(name: &str) -> CursorScope {
        CursorScope::Folder(FolderId(name.into()))
    }
    struct Harness {
        ingress: Arc<PushIngress>,
        boundary: crate::cancel::Boundary,
        shutdown: CancellationToken,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Harness {
        fn new(parked: bool, missing: bool, refused: bool) -> Self {
            let account_id = AccountId("retained-work".into());
            let account: Arc<dyn Account> =
                Arc::new(crate::test_common::StubAccount::new(vec![scope("one")]));
            let cursors = Arc::new(CursorRegistry::new());
            if !missing {
                cursors.put(crate::test_common::cursor_for(&scope("one"), b"live"));
            }
            let (boundary, view) = crate::cancel::Boundary::new();
            let (priority, _) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
            let (bandwidth, _) = tokio::sync::watch::channel(None);
            let control =
                SyncControl::new(account_id.clone(), boundary.clone(), priority, bandwidth);
            let (tx, _) = tokio::sync::broadcast::channel(8);
            let (reopen_tx, _) = mpsc::channel(8);
            let gate =
                crate::scheduler::BudgetGate::new(crate::scheduler::ConcurrencyBudget::default());
            gate.register(account_id.clone());
            let scheduler =
                crate::scheduler::Scheduler::new(crate::types::SchedulerConfig::default(), gate);
            if refused {
                scheduler.budget().forget(&account_id);
            }
            let tokens = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
            if parked {
                tokens
                    .lock()
                    .expect("tokens")
                    .insert(scope("one"), crate::multiplexer::ScopeToken::tombstone());
            }
            let ingress = Arc::new(PushIngress::new(4));
            let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let shutdown = CancellationToken::new();
            let reconciler = Reconciler {
                account_id,
                account: Arc::new(ArcSwap::from_pointee(account)),
                cursors,
                delivery: Arc::new(ChangeDelivery::new(tx)),
                boundary: view,
                shutdown: shutdown.clone(),
                control,
                reopen_tx,
                throttles: Arc::new(std::sync::Mutex::new(crate::recovery::ThrottleBucket::new())),
                scheduler,
                scope_tokens: tokens,
                account_generation: tokio::sync::watch::channel(0).1,
                attempts: Some(Arc::clone(&attempts)),
            };
            let task = tokio::spawn(reconciler.run(Arc::clone(&ingress)));
            Self {
                ingress,
                boundary,
                shutdown,
                attempts,
                task,
            }
        }
        fn attempts(&self) -> usize {
            self.attempts.load(std::sync::atomic::Ordering::SeqCst)
        }
        async fn settle(&self) {
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
        }
        async fn close(self) {
            self.shutdown.cancel();
            self.task.await.expect("reconciler joins");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_checkpoint_now_allows_at_most_one_drive_attempt() {
        let harness = Harness::new(false, false, false);
        harness
            .boundary
            .set(crate::cancel::BoundaryRequest::CheckpointNow);
        harness.ingress.request_full();
        harness.settle().await;
        assert!(
            harness.attempts() <= 1,
            "checkpointing must not spin at admission"
        );
        harness.boundary.set(crate::cancel::BoundaryRequest::Run);
        harness.settle().await;
        assert_eq!(harness.attempts(), 1);
        harness.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn undispatchable_scopes_are_discharged_without_spinning() {
        for (parked, missing, refused) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let harness = Harness::new(parked, missing, refused);
            harness
                .ingress
                .invalidate(&HintPayload::SpecificCursorScope(scope("one")));
            harness.settle().await;
            assert_eq!(harness.attempts(), 1);
            harness.settle().await;
            assert_eq!(harness.attempts(), 1);
            harness.close().await;
        }
    }

    #[test]
    fn a_membership_hint_resolves_to_its_scopes_at_merge_time() {
        let registry = CursorRegistry::new();
        let membership = MembershipScope::Folder(FolderId("folder".into()));
        let ingress = PushIngress::new(4);
        ingress.invalidate(&HintPayload::SpecificMembership(membership.clone()));
        registry.link_membership(membership, scope("one"));
        let mut work = WorkSet::default();
        work.merge(ingress.take(), &registry, &ingress);
        assert_eq!(work.scopes, HashSet::from([scope("one")]));
    }

    #[test]
    fn the_work_set_coarsens_on_merge_but_not_on_its_own_full_resolution() {
        let registry = CursorRegistry::new();
        let ingress = PushIngress::new(1);
        let mut work = WorkSet::default();
        for name in ["one", "two"] {
            let pending = PendingPush {
                scopes: HashSet::from([scope(name)]),
                ..PendingPush::default()
            };
            work.merge(pending, &registry, &ingress);
        }
        assert!(work.full && work.scopes.is_empty());
        assert_eq!(ingress.coarsened(), 1);
        work.full = false;
        work.scopes.extend([scope("one"), scope("two")]);
        work.merge(PendingPush::default(), &registry, &ingress);
        assert!(!work.full);
        assert_eq!(work.scopes.len(), 2);
        assert_eq!(ingress.coarsened(), 1);
    }
}
