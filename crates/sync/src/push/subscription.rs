//! Push subscription tracking.
//!
//! `Account::push_subscribe` returns a `SubscriptionHandle` the engine
//! stashes here so the consumer can `unsubscribe_push(account_id)`
//! later. The registry is per-engine, not per-account.

use bifrost_types::{AccountId, CursorScope, SubscriptionHandle};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub(crate) struct RegisteredSubscription {
    pub handle: SubscriptionHandle,
    pub scopes: Vec<CursorScope>,
    pub state: SubscriptionState,
}

/// Server handle lifetime and consumer desire are one state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionState {
    Live,
    LiveTeardownFailed,
    TornDownAwaitingRecreate,
    Orphan,
}

impl RegisteredSubscription {
    pub(crate) fn desired(&self) -> bool {
        self.state != SubscriptionState::Orphan
    }

    pub(crate) fn torn_down(&self) -> bool {
        self.state == SubscriptionState::TornDownAwaitingRecreate
    }

    pub(crate) fn teardown_unconfirmed(&self) -> bool {
        matches!(
            self.state,
            SubscriptionState::LiveTeardownFailed | SubscriptionState::Orphan
        )
    }

    #[must_use]
    pub(crate) fn into_orphan(self) -> Self {
        Self {
            state: SubscriptionState::Orphan,
            ..self
        }
    }
}

/// Per-engine subscription handle registry.
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    inner: DashMap<AccountId, Vec<RegisteredSubscription>>,
}

impl SubscriptionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a handle for an account, unless the owning slot is being torn
    /// down. Returns whether the record was written; `false` means the slot's
    /// `shutdown` token was already cancelled and nothing was registered, so the
    /// caller still owns a live subscription the registry will never hold.
    ///
    /// The token is read inside the entry's shard lock, for the reason
    /// [`Self::restore_orphans`] gives: detach cancels the token before it takes
    /// the account's records, so a write that reads the token here is either
    /// ordered before the take (which discards it with the rest of the
    /// incarnation) or after it, where the cancellation is visible and the write
    /// is refused. A record landing after the take would sit under an id a later
    /// attach of the same `AccountId` inherits.
    #[must_use]
    pub fn record(
        &self,
        account: AccountId,
        handle: SubscriptionHandle,
        scopes: Vec<CursorScope>,
        shutdown: &CancellationToken,
    ) -> bool {
        let entry = self.inner.entry(account);
        if shutdown.is_cancelled() {
            return false;
        }
        entry.or_default().push(RegisteredSubscription {
            handle,
            scopes,
            state: SubscriptionState::Live,
        });
        true
    }

    /// Take all records registered for an account that still have a
    /// server-side handle to tear down. Returns an empty vec if none. Callers
    /// that cannot tear down a handle restore its record so the same
    /// account-side handle remains retryable. A `torn_down` record is dropped
    /// here: its server side is already gone and the caller taking the
    /// registry (a consumer unsubscribe, a detach) is ending the desire it
    /// carried.
    #[must_use]
    pub(crate) fn take(&self, account: &AccountId) -> Vec<RegisteredSubscription> {
        match self.inner.remove(account) {
            Some((_, mut v)) => {
                v.retain(|record| !record.torn_down());
                v
            }
            None => Vec::new(),
        }
    }

    /// Restore records whose server-side teardown failed, unless the owning slot
    /// is being torn down. Merge rather than replace so a concurrent
    /// `subscribe_push` registration is preserved. Returns whether the records
    /// were written; `false` means the slot's `shutdown` token was already
    /// cancelled and they were dropped.
    ///
    /// The token is read INSIDE the entry's shard lock, which is the lock
    /// [`Self::take`] removes under, and that is the point. Detach cancels the
    /// token BEFORE it takes the account's records, so this write is either
    /// ordered before the take (which then discards it, as it discards
    /// everything of that incarnation) or after it, where the cancellation is
    /// already visible and the write is refused. A token check made before
    /// calling, with the write a separate step, leaves an interval on a
    /// multi-thread runtime in which detach can cancel AND take, and the orphan
    /// then lands under an id a later attach of the same `AccountId` inherits.
    ///
    /// The slot's reopen lock does not close that interval. Detach does wait
    /// for it before the take, but only for a bounded `detach_timeout`, because
    /// a consumer-driven reattach holds it across `factory.open()` and network
    /// calls and the one call a consumer has for getting rid of an account must
    /// not wait on a wedged open; on expiry detach proceeds without the lock.
    /// Only the token read
    /// inside the shard lock orders this write against the take in every case.
    pub(crate) fn restore_orphans(
        &self,
        account: AccountId,
        records: Vec<RegisteredSubscription>,
        shutdown: &CancellationToken,
    ) -> bool {
        if records.is_empty() {
            return true;
        }
        let entry = self.inner.entry(account);
        if shutdown.is_cancelled() {
            return false;
        }
        entry.or_default().extend(records);
        true
    }

    /// Flag a still-registered handle whose server-side teardown failed.
    /// Reopen keeps carrying the record and retrying it, but stops treating
    /// its failure as a reason to abandon the swap. `desired` is left alone:
    /// the caller is an aborted reopen, which leaves the old account
    /// installed and the consumer still wanting the coverage, so the next
    /// attempt must recreate it on its replacement.
    pub(crate) fn mark_unconfirmed(&self, account: &AccountId, handle: &SubscriptionHandle) {
        if let Some(mut records) = self.inner.get_mut(account) {
            for record in records.iter_mut() {
                // A failed teardown cannot resurrect a handle already known gone.
                if &record.handle == handle && record.state == SubscriptionState::Live {
                    record.state = SubscriptionState::LiveTeardownFailed;
                }
            }
        }
    }

    /// Record that a still-registered handle's server-side teardown
    /// SUCCEEDED in a reopen attempt that may yet abort. A wanted record
    /// stays, flagged `torn_down`, so the next attempt recreates it without
    /// unsubscribing the dead handle again; a pure orphan has nothing left to
    /// carry and is removed.
    pub(crate) fn mark_torn_down(&self, account: &AccountId, handle: &SubscriptionHandle) {
        if let Some(mut records) = self.inner.get_mut(account) {
            records.retain_mut(|record| {
                if &record.handle != handle {
                    return true;
                }
                if record.desired() {
                    record.state = SubscriptionState::TornDownAwaitingRecreate;
                    true
                } else {
                    false
                }
            });
        }
    }

    /// The account's pure orphan records, retaining scopes for throttle admission:
    /// nobody wants them and their server-side teardown is still unconfirmed.
    #[must_use]
    pub(crate) fn orphans(&self, account: &AccountId) -> Vec<RegisteredSubscription> {
        self.inner
            .get(account)
            .map(|records| {
                records
                    .iter()
                    .filter(|record| !record.desired() && record.teardown_unconfirmed())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drop a pure orphan whose teardown has now succeeded. A `desired`
    /// record under the same handle is left alone.
    pub(crate) fn remove_orphan(&self, account: &AccountId, handle: &SubscriptionHandle) {
        if let Some(mut records) = self.inner.get_mut(account) {
            records.retain(|record| record.desired() || &record.handle != handle);
        }
    }

    #[must_use]
    pub(crate) fn snapshot(&self, account: &AccountId) -> Vec<RegisteredSubscription> {
        self.inner
            .get(account)
            .map(|records| records.clone())
            .unwrap_or_default()
    }

    /// Install `records` as the account's whole registry entry, unless the
    /// owning slot is being torn down. `Err` hands the records back untouched:
    /// the `shutdown` token was already cancelled, nothing was written, and the
    /// caller still owns whatever server-side subscriptions the records name.
    ///
    /// Sealed the same way as [`Self::restore_orphans`] and [`Self::record`], by
    /// reading the token inside the entry's shard lock that [`Self::take`] runs
    /// under. Installing an empty set only removes the entry, which detach's take
    /// would have done anyway, so it is never refused.
    pub(crate) fn replace(
        &self,
        account: AccountId,
        records: Vec<RegisteredSubscription>,
        shutdown: &CancellationToken,
    ) -> Result<(), Vec<RegisteredSubscription>> {
        match self.inner.entry(account) {
            Entry::Occupied(mut occupied) => {
                if records.is_empty() {
                    occupied.remove();
                } else if shutdown.is_cancelled() {
                    return Err(records);
                } else {
                    occupied.insert(records);
                }
            }
            Entry::Vacant(vacant) => {
                if records.is_empty() {
                    return Ok(());
                }
                if shutdown.is_cancelled() {
                    return Err(records);
                }
                vacant.insert(records);
            }
        }
        Ok(())
    }

    /// Snapshot for tests.
    #[must_use]
    pub fn len(&self, account: &AccountId) -> usize {
        self.inner.get(account).map(|v| v.len()).unwrap_or(0)
    }

    /// Convenience: total subscription count across accounts.
    #[must_use]
    pub fn total(&self) -> usize {
        self.inner.iter().map(|r| r.value().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orphan(handle: &str) -> RegisteredSubscription {
        RegisteredSubscription {
            handle: SubscriptionHandle(handle.to_owned()),
            scopes: Vec::new(),
            state: SubscriptionState::Live,
        }
        .into_orphan()
    }

    #[test]
    fn a_late_teardown_failure_does_not_resurrect_a_dead_handle() {
        let registry = SubscriptionRegistry::new();
        let account = AccountId("gone".into());
        let handle = SubscriptionHandle("handle".into());
        assert!(registry.record(
            account.clone(),
            handle.clone(),
            vec![],
            &CancellationToken::new()
        ));
        registry.mark_torn_down(&account, &handle);
        registry.mark_unconfirmed(&account, &handle);
        let records = registry.snapshot(&account);
        assert!(records[0].desired());
        assert!(records[0].torn_down());
        assert!(!records[0].teardown_unconfirmed());
        assert!(registry.take(&account).is_empty());
    }

    /// A live slot's orphans are merged into what is registered.
    #[test]
    fn a_live_slot_keeps_its_restored_orphans() {
        let registry = SubscriptionRegistry::new();
        let account = AccountId("live".to_owned());
        assert!(registry.record(
            account.clone(),
            SubscriptionHandle("wanted".to_owned()),
            vec![],
            &CancellationToken::new(),
        ));
        assert!(registry.restore_orphans(
            account.clone(),
            vec![orphan("refused")],
            &CancellationToken::new()
        ));
        assert_eq!(registry.len(&account), 2);
    }

    /// Once the slot's token is cancelled, an orphan is refused: detach cancels
    /// before it takes, so a write ordered after the take must see the
    /// cancellation and leave nothing behind for a later attach to inherit.
    #[test]
    fn a_cancelled_slot_refuses_orphans_and_leaves_no_entry() {
        let registry = SubscriptionRegistry::new();
        let account = AccountId("detached".to_owned());
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        // Detach's order: cancel, then take.
        assert!(registry.take(&account).is_empty());
        assert!(!registry.restore_orphans(account.clone(), vec![orphan("late")], &shutdown));
        assert_eq!(registry.len(&account), 0);
        assert!(
            registry.take(&account).is_empty(),
            "a later attach of the same id inherits nothing"
        );
    }

    /// A registration made after detach's take is refused, and leaves no entry.
    #[test]
    fn a_cancelled_slot_refuses_a_new_record_and_leaves_no_entry() {
        let registry = SubscriptionRegistry::new();
        let account = AccountId("detached-record".to_owned());
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        assert!(registry.take(&account).is_empty());
        assert!(!registry.record(
            account.clone(),
            SubscriptionHandle("late".to_owned()),
            vec![],
            &shutdown
        ));
        assert_eq!(registry.len(&account), 0);
    }

    /// A reattach commit landing after detach's take is refused, hands its
    /// records back, and leaves nothing behind; an empty set is never refused.
    #[test]
    fn a_cancelled_slot_refuses_a_replacement_and_returns_the_records() {
        let registry = SubscriptionRegistry::new();
        let account = AccountId("detached-replace".to_owned());
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        assert!(registry.take(&account).is_empty());
        let refused = registry
            .replace(account.clone(), vec![orphan("late")], &shutdown)
            .expect_err("a cancelled slot refuses the install");
        assert_eq!(refused.len(), 1);
        assert_eq!(registry.len(&account), 0);
        assert!(registry.replace(account, Vec::new(), &shutdown).is_ok());
    }

    /// A live slot's replacement swaps the whole entry.
    #[test]
    fn a_live_slot_installs_a_replacement() {
        let registry = SubscriptionRegistry::new();
        let account = AccountId("live-replace".to_owned());
        let shutdown = CancellationToken::new();
        assert!(registry.record(
            account.clone(),
            SubscriptionHandle("old".to_owned()),
            vec![],
            &shutdown
        ));
        assert!(
            registry
                .replace(account.clone(), vec![orphan("new")], &shutdown)
                .is_ok()
        );
        let held = registry.snapshot(&account);
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].handle, SubscriptionHandle("new".to_owned()));
    }
}
