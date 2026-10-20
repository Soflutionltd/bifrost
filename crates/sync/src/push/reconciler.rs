//! Push reconciler task.
//!
//! One task per account. Reads `WatchEvent`s off the per-account mpsc
//! and, on `Invalidated`, runs `changes_stream` against the current
//! cursor for each affected scope. Output flows to the multiplexer's
//! broadcast so consumers see a single unified `Change` stream.

use std::collections::HashSet;
use std::sync::Arc;

use bifrost_types::{Account, AccountId, CursorScope, HintPayload, InvalidationHint, WatchEvent};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use arc_swap::ArcSwap;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::{Error, Warning};
use crate::multiplexer::{
    ChangeDelivery, ChangesEvent, MultiplexerEvent, ReopenRequest, drive_changes_stream,
};

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
}

impl Reconciler {
    /// Run the reconciler loop. Returns when the channel closes or
    /// the shutdown token trips. Honors the account boundary: when
    /// the engine flips to `Pause` (e.g. `OperatorOverrideRequired`
    /// or `RetryBudgetExhausted`), pushes park alongside polls until
    /// the consumer flips to `Resume`. Buffered `WatchEvent`s remain
    /// in the channel and drain on resume.
    pub async fn run(mut self, mut rx: mpsc::Receiver<WatchEvent>) {
        // Hinted scopes held by a SCOPE-LOCAL throttle (a key the account
        // declares for that scope alone, such as one mailbox's budget). They
        // are kept here instead of being slept on inside a sweep: the
        // reconciler is one task for the whole account, so a sleep inside
        // `reconcile` would stall every later hint - for healthy mailboxes,
        // `Reconnected`, `Terminated` - behind one mailbox's Retry-After.
        // Only the scope is held; no cursor snapshot, lease, permit or
        // activity guard survives the wait. A hint for a scope already
        // deferred is absorbed by the set, and one that arrives during a
        // drive waits in the channel and re-marks the scope once read, so no
        // invalidation is lost - events are processed BETWEEN drives, which
        // is the granularity this task has always had.
        let mut deferred: HashSet<CursorScope> = HashSet::new();
        loop {
            // Park while the account is paused. The boundary watch
            // notifies on every state change; we wake when it flips
            // back to `Run`.
            while matches!(self.boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
                tokio::select! {
                    () = self.shutdown.cancelled() => return,
                    changed = self.boundary.changed() => {
                        match changed {
                            Some(crate::cancel::BoundaryRequest::Run) => break,
                            // Re-check the watch after every state change:
                            // only Pause keeps this wait active. Run resumes
                            // here; CheckpointNow and Stop leave the pause
                            // wait too. The drive's boundary arm handles
                            // Stop, while CheckpointNow has no reconciler
                            // action.
                            Some(_) => continue,
                            None => return,
                        }
                    }
                }
            }
            // Wake for the earliest deferred scope whose hold has passed. A
            // deferred scope with no pending hold any more is due now.
            let next_due = self.next_deferred_due(&deferred);
            tokio::select! {
                () = self.shutdown.cancelled() => return,
                maybe_event = rx.recv() => {
                    let Some(event) = maybe_event else { return; };
                    self.handle(event, &mut deferred).await;
                }
                () = sleep_or_forever(next_due) => {
                    // Copied, not drained: `drive_scopes` removes each
                    // scope as it DISPATCHES it, so a sweep that returns
                    // early (a terminal error, an account-wide directive)
                    // leaves every scope it never reached still deferred.
                    let due: Vec<CursorScope> = deferred.iter().cloned().collect();
                    if let Err(err) = self.drive_scopes(due, &mut deferred).await {
                        tracing::warn!(target: "bifrost.sync.reconcile", error=%err, "deferred reconcile failed");
                    }
                }
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

    async fn handle(&mut self, event: WatchEvent, deferred: &mut HashSet<CursorScope>) {
        match event {
            WatchEvent::Invalidated { hint } => {
                if let Err(err) = self.reconcile(&hint, deferred).await {
                    tracing::warn!(target: "bifrost.sync.reconcile", error=%err, "reconcile failed");
                }
            }
            WatchEvent::Disconnected => {
                let _ = self.delivery.sender().send(self.warning_event(
                    "push transport disconnected",
                    bifrost_types::WarningKind::Other,
                ));
            }
            WatchEvent::Reconnected => {
                // Treat reconnect as a full reconcile across every
                // registered cursor scope.
                let hint = InvalidationHint {
                    source: bifrost_types::PushSource::Coalesced,
                    payload: HintPayload::Unknown,
                };
                if let Err(err) = self.reconcile(&hint, deferred).await {
                    tracing::warn!(target: "bifrost.sync.reconcile", error=%err, "post-reconnect reconcile failed");
                }
            }
            WatchEvent::Terminated(error) => {
                // Push stream cannot be reconnected without engine
                // intervention. Plan recovery and route engine
                // directives through the reopen channel so the engine
                // restarts the subscription (or escalates per its
                // budget). Terminal classes broadcast a Terminated
                // event so consumers observe the structured error.
                use crate::recovery::{RecoveryPlan, directive_target_scope, plan_recovery};
                let original = error.clone();
                match plan_recovery(error) {
                    RecoveryPlan::Engine(directive) => {
                        let scope = directive_target_scope(&directive);
                        let _ = self
                            .reopen_tx
                            .send(ReopenRequest::Recovery {
                                scope,
                                error: original,
                            })
                            .await;
                    }
                    RecoveryPlan::Retry(advice) => {
                        // Push streams cannot retry inline; the
                        // forwarder reconnects on the next iteration.
                        // But a retryable termination can still carry a
                        // provider-documented throttle deadline (the
                        // stream died on a 429), and dropping it here
                        // would hide it from polls and sibling
                        // accounts. Record, then surface the warning.
                        crate::recovery::record_throttle(
                            &self.throttles,
                            &self.account_id,
                            &advice,
                            &original,
                        );
                        let warning = bifrost_types::Warning::user_safe(
                            bifrost_types::WarningKind::Other,
                            "push stream terminated with retryable recovery",
                        );
                        let me = MultiplexerEvent {
                            scope: CursorScope::Account,
                            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = self.delivery.sender().send(me);
                    }
                    RecoveryPlan::Reconcile(advice) => {
                        crate::recovery::record_reconcile_throttle(
                            &self.throttles,
                            &self.account_id,
                            &advice,
                            &original,
                        );
                        // Surface a warning so dashboards observe the
                        // hiccup; the forwarder reconnects on its next
                        // iteration.
                        let warning = bifrost_types::Warning::user_safe(
                            bifrost_types::WarningKind::Other,
                            "push stream terminated with retryable recovery",
                        );
                        let me = MultiplexerEvent {
                            scope: CursorScope::Account,
                            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = self.delivery.sender().send(me);
                    }
                    RecoveryPlan::Terminal(fatal) => {
                        let me = MultiplexerEvent {
                            scope: CursorScope::Account,
                            event: Arc::new(bifrost_types::SyncEvent::Terminated(
                                fatal.into_inner(),
                            )),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = self.delivery.sender().send(me);
                    }
                }
            }
            // `WatchEvent` is `#[non_exhaustive]`; an unknown variant
            // is treated as a no-op wakeup.
            _ => {}
        }
    }

    async fn reconcile(
        &self,
        hint: &InvalidationHint,
        deferred: &mut HashSet<CursorScope>,
    ) -> Result<(), Error> {
        let scopes = scopes_for_hint(&self.cursors, &hint.payload);
        self.drive_scopes(scopes, deferred).await
    }

    /// Drive each scope once, deferring any held by a scope-local throttle.
    /// Parking, deletion and every hold are re-checked here at DISPATCH, not
    /// when the scope was queued, so a scope deferred and then parked or
    /// deleted is never driven.
    async fn drive_scopes(
        &self,
        scopes: Vec<CursorScope>,
        deferred: &mut HashSet<CursorScope>,
    ) -> Result<(), Error> {
        for scope in scopes {
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
                ChangesEvent::Advanced
                | ChangesEvent::Done
                | ChangesEvent::Stopped
                | ChangesEvent::Paused => {}
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

    fn warning_event(&self, message: &str, kind: bifrost_types::WarningKind) -> MultiplexerEvent {
        let warning = Warning::user_safe(kind, message);
        MultiplexerEvent {
            scope: CursorScope::Account,
            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
            checkpoint: None,
            publication: None,
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
