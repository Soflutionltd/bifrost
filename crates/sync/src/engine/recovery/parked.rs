//! Deferred scope repairs and account schema-reset obligations.

use crate::engine::*;

/// Scope repairs declined under a pause that nothing else will raise again,
/// re-offered to the reopen listener once the account runs.
///
/// A repair of a scope that HAS a cursor needs no help: it declines without
/// touching the cursor, and the poll raises the directive again after resume. A
/// scope that has none - a lifecycle `Created` establishment, before any cursor
/// exists, or a deferred inventory scope - is invisible to the
/// multiplexer, which only polls registered scopes, so a declined repair of it
/// would simply be lost.
///
/// One instance per slot, owned by the reopen listener. Each parked scope is one
/// small task that waits for the boundary to read `Run` and then sends
/// [`ReopenRequest::EstablishIfAbsent`] back through the listener's own channel,
/// so the repair runs under the reopen lock with the decline-under-pause check
/// again, and re-checks absence THERE: a check made here, outside the lock,
/// could restart a healthy replacement established between it and the dispatch.
/// It is a task rather than a wait in the listener because the listener is the
/// serialization point for real directives: blocking it across an unbounded
/// pause would hold `RestartAccount` behind it. The task leaves through the
/// shutdown token, so a detach never waits on one.
///
/// Each entry maps a scope to the lifecycle epoch its obligation was recorded
/// under, and only the task that captured that epoch may consume it (`claim`):
/// a folder deleted and recreated during one pause is parked under a newer
/// epoch, the older task finds the entry overwritten and exits silently, and
/// the newer task owns the obligation. Parking the same `(scope, epoch)` again
/// is a no-op, so repeated `Created` events during a long pause do not stack
/// tasks.
#[derive(Clone)]
pub(crate) struct DeferredScopeRepairs {
    parked: Arc<std::sync::Mutex<HashMap<CursorScope, crate::cursor::LifecycleEpoch>>>,
}

impl DeferredScopeRepairs {
    pub(crate) fn new() -> Self {
        Self {
            parked: Arc::default(),
        }
    }

    fn guard(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<CursorScope, crate::cursor::LifecycleEpoch>> {
        self.parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Remove the entry for `scope` only if it still maps to `epoch`, and
    /// report whether it did. A task whose entry was unparked (its folder was
    /// deleted) or overwritten by a newer epoch takes nothing.
    pub(crate) fn claim(&self, scope: &CursorScope, epoch: crate::cursor::LifecycleEpoch) -> bool {
        let mut parked = self.guard();
        if parked.get(scope) != Some(&epoch) {
            return false;
        }
        parked.remove(scope);
        true
    }

    /// Park `scope` under `epoch` until the account runs, then re-offer it as
    /// [`ReopenRequest::EstablishIfAbsent`]. A no-op if exactly this obligation
    /// is already parked; a different epoch overwrites the entry and spawns the
    /// task that owns it.
    pub(crate) fn park(
        &self,
        scope: CursorScope,
        epoch: crate::cursor::LifecycleEpoch,
        control: SyncControl,
        shutdown: CancellationToken,
        reopen_tx: mpsc::Sender<ReopenRequest>,
    ) {
        {
            let mut parked = self.guard();
            if parked.get(&scope) == Some(&epoch) {
                return;
            }
            parked.insert(scope.clone(), epoch);
        }
        let parked = self.clone();
        tokio::spawn(async move {
            // Claimed BEFORE the send, so a repair the listener declines again
            // (a second pause) can park the scope afresh.
            if !control.wait_until_running(&shutdown).await || !parked.claim(&scope, epoch) {
                return;
            }
            tokio::select! {
                () = shutdown.cancelled() => {},
                _ = reopen_tx.send(ReopenRequest::EstablishIfAbsent { scope, epoch }) => {},
            }
        });
    }

    /// Forget a parked scope whose folder was deleted, so its task re-raises
    /// nothing when the account runs again. A no-op if it is not parked.
    pub(crate) fn unpark(&self, scope: &CursorScope) {
        self.guard().remove(scope);
    }
}

/// An account-wide schema reset owed after a decline, plus the reset
/// generation that keeps a parked reset from repeating one that already ran.
///
/// A reset declined under a pause, or one whose clear exhausted its retry
/// budget, cannot rely on a registered failing cursor to raise the directive
/// again: a deferred inventory trigger has none, and the reset already removed
/// the registered ones from the registry before an exhausted clear. The owed
/// scopes are kept here instead, merged across declines (per scope, the greater
/// epoch), and one task re-raises them as [`ReopenRequest::SchemaReset`] once
/// the account runs.
///
/// The generation: on a paused decline the still-registered failing cursors
/// re-raise the directive after resume as well, and without a check one decline
/// would cost two complete resets, the second wiping the cursors and in-flight
/// backfill the first just established. `completed` counts resets whose clear
/// committed during this attachment and `parked_at` records it at the newest
/// decline; the listener compares them at EXECUTION (not when the task takes
/// the map), so a parked request queued behind a poll's directive still sees
/// the reset that directive ran, and only establishes the owed scopes.
#[derive(Default, Clone)]
pub(crate) struct DeferredSchemaReset {
    state: Arc<std::sync::Mutex<ParkedReset>>,
}

/// Completed schema clears, owned by the deferred reset state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResetCount(u64);

impl ResetCount {
    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }

    fn committed(&mut self) {
        self.0 += 1;
    }
}

struct ParkedReset {
    /// Scopes owed re-establishment, `None` when no reset is owed. `Some` of
    /// an empty map is still an owed reset: its scopes are the registered ones
    /// the reset snapshots when it runs.
    owed: Option<HashMap<CursorScope, crate::cursor::LifecycleEpoch>>,
    /// Whether a task is parked waiting to send `owed`.
    task: bool,
    /// Account resets whose clear committed during this attachment.
    completed: ResetCount,
    /// `completed` as of the most recent `park`.
    parked_at: ResetCount,
}

impl Default for ParkedReset {
    fn default() -> Self {
        Self {
            owed: None,
            task: false,
            completed: ResetCount(0),
            parked_at: ResetCount(0),
        }
    }
}

impl DeferredSchemaReset {
    fn guard(&self) -> std::sync::MutexGuard<'_, ParkedReset> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn merge(state: &mut ParkedReset, scopes: HashMap<CursorScope, crate::cursor::LifecycleEpoch>) {
        let owed = state.owed.get_or_insert_with(HashMap::new);
        for (scope, epoch) in scopes {
            let entry = owed.entry(scope).or_insert(epoch);
            *entry = (*entry).max(epoch);
        }
    }

    /// How many account resets have committed during this attachment.
    pub(crate) fn completed(&self) -> ResetCount {
        self.guard().completed
    }

    /// Record that an account reset's clear committed.
    pub(crate) fn reset_completed(&self) {
        self.guard().completed.committed();
    }

    /// Drop one deleted scope from the owed set. An optimisation only: the
    /// execution re-checks every owed scope's lifecycle epoch.
    pub(crate) fn unpark_scope(&self, scope: &CursorScope) {
        if let Some(owed) = &mut self.guard().owed {
            owed.remove(scope);
        }
    }

    /// Merge `scopes` into the owed reset, stamp the newest decline's
    /// generation, and spawn the re-raising task unless one is already parked.
    pub(crate) fn park(
        &self,
        scopes: HashMap<CursorScope, crate::cursor::LifecycleEpoch>,
        control: SyncControl,
        shutdown: CancellationToken,
        reopen_tx: mpsc::Sender<ReopenRequest>,
    ) {
        {
            let mut state = self.guard();
            Self::merge(&mut state, scopes);
            state.parked_at = state.completed;
            if state.task {
                return;
            }
            state.task = true;
        }
        let reset = self.clone();
        tokio::spawn(async move {
            let runs = control.wait_until_running(&shutdown).await;
            let (owed, parked_at) = {
                let mut state = reset.guard();
                state.task = false;
                (state.owed.take(), state.parked_at)
            };
            if runs && let Some(reestablish) = owed {
                tokio::select! {
                    () = shutdown.cancelled() => {},
                    _ = reopen_tx.send(ReopenRequest::SchemaReset { reestablish, parked_at }) => {},
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::ObjectType;

    const SCOPE: CursorScope = CursorScope::Type(ObjectType::Email);

    #[test]
    fn a_park_claim_under_a_stale_epoch_takes_nothing() {
        let repairs = DeferredScopeRepairs::new();
        repairs
            .parked
            .lock()
            .expect("parked")
            .insert(SCOPE, crate::cursor::LifecycleEpoch::for_test(2));
        repairs
            .parked
            .lock()
            .expect("parked")
            .insert(SCOPE, crate::cursor::LifecycleEpoch::for_test(3));
        assert!(!repairs.claim(&SCOPE, crate::cursor::LifecycleEpoch::for_test(2)));
        assert!(repairs.claim(&SCOPE, crate::cursor::LifecycleEpoch::for_test(3)));
    }

    #[test]
    fn parked_schema_resets_merge_by_greater_epoch() {
        let mut state = ParkedReset::default();
        DeferredSchemaReset::merge(
            &mut state,
            HashMap::from([(SCOPE, crate::cursor::LifecycleEpoch::for_test(2))]),
        );
        DeferredSchemaReset::merge(
            &mut state,
            HashMap::from([
                (SCOPE, crate::cursor::LifecycleEpoch::for_test(1)),
                (
                    CursorScope::Account,
                    crate::cursor::LifecycleEpoch::for_test(0),
                ),
            ]),
        );
        assert_eq!(
            state.owed,
            Some(HashMap::from([
                (SCOPE, crate::cursor::LifecycleEpoch::for_test(2)),
                (
                    CursorScope::Account,
                    crate::cursor::LifecycleEpoch::for_test(0)
                )
            ]))
        );
    }
}
