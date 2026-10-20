//! Concurrency budget.
//!
//! Two-layer `tokio::sync::Semaphore`: per-account first (so two
//! big accounts under one engine do not starve each other), global
//! second. A reserved mutation share carves out a sub-pool so a
//! 200K-flag mutation cannot peg every permit and freeze the
//! multiplexer.
//!
//! Rate-shaping (Gmail's quota-units-per-second budget) is NOT here -
//! it lives in `bifrost-net`. This gate counts concurrent operations,
//! not request rate.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use bifrost_types::AccountId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::scheduler::lanes::WorkKind;

/// Tunable concurrency budget.
#[derive(Debug, Clone, Copy)]
pub struct ConcurrencyBudget {
    /// Per-account permit pool. Default 8.
    pub per_account: usize,
    /// Engine-wide permit pool. Default 64.
    pub global: usize,
    /// Numerator of the per-account mutation share. Default 1.
    ///
    /// Encoded as a rational `(num, den)` so we avoid float math and
    /// avoid clippy's cast lints. Default `(1, 4)` = 25% of
    /// `per_account` reserved for mutation work, rounded up to at
    /// least 1.
    pub mutation_share_num: u32,
    /// Denominator of the per-account mutation share. Default 4.
    pub mutation_share_den: u32,
}

impl Default for ConcurrencyBudget {
    fn default() -> Self {
        Self {
            per_account: 8,
            global: 64,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }
    }
}

impl ConcurrencyBudget {
    /// Reject degenerate configurations: each account must retain at
    /// least one sync permit and one mutation permit, and no zero
    /// field is silently rewritten.
    /// Construct the budget through this validated accessor so the
    /// failure surface is a typed `Error` rather than a silent
    /// rewrite.
    ///
    /// Returns `Error::Other` with a descriptive message on
    /// invalid input; the engine surfaces this to the consumer at
    /// build time.
    pub fn validate(&self) -> Result<(), Error> {
        if self.per_account < 2 {
            return Err(Error::Other(
                "ConcurrencyBudget: per_account must be >= 2".into(),
            ));
        }
        if self.global == 0 {
            return Err(Error::Other("ConcurrencyBudget: global must be > 0".into()));
        }
        if self.mutation_share_num == 0 {
            return Err(Error::Other(
                "ConcurrencyBudget: mutation_share_num must be > 0".into(),
            ));
        }
        if self.mutation_share_den == 0 {
            return Err(Error::Other(
                "ConcurrencyBudget: mutation_share_den must be > 0".into(),
            ));
        }
        if self.mutation_permits() >= self.per_account {
            return Err(Error::Other(
                "ConcurrencyBudget: mutation share must leave at least one sync permit".into(),
            ));
        }
        Ok(())
    }

    /// Number of permits carved out of `per_account` for the mutation
    /// sub-pool. Always at least 1 when `per_account >= 1`.
    ///
    /// Callers should call `validate` first; this accessor preserves
    /// the historical `max(1)` rounding for backwards compatibility
    /// with consumers that construct the struct directly.
    #[must_use]
    pub fn mutation_permits(&self) -> usize {
        if self.per_account == 0 {
            return 0;
        }
        let den = self.mutation_share_den.max(1) as usize;
        let num = (self.mutation_share_num as usize).min(den);
        // Round up: (a * num + den - 1) / den.
        let raw = self.per_account.saturating_mul(num).saturating_add(den - 1) / den;
        raw.max(1).min(self.per_account)
    }

    /// Number of permits left in the sync (non-mutation) sub-pool.
    #[must_use]
    pub fn sync_permits(&self) -> usize {
        self.per_account.saturating_sub(self.mutation_permits())
    }
}

/// Engine-internal layered budget gate.
#[derive(Clone)]
pub struct BudgetGate {
    inner: Arc<BudgetInner>,
}

impl std::fmt::Debug for BudgetGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetGate")
            .field("budget", &self.inner.budget)
            .finish_non_exhaustive()
    }
}

struct BudgetInner {
    budget: ConcurrencyBudget,
    global: Arc<Semaphore>,
    accounts: Mutex<HashMap<AccountId, AccountPoolsEntry>>,
}

struct AccountPools {
    sync: Arc<Semaphore>,
    mutation: Arc<Semaphore>,
    retired: CancellationToken,
}

enum AccountPoolsEntry {
    Active(Arc<AccountPools>),
    // Preserve a lightweight tombstone so a stray acquire after detach does
    // not mistake this id for a never-seen account and lazily recreate pools.
    Forgotten,
}

impl AccountPools {
    fn new(budget: ConcurrencyBudget) -> Arc<Self> {
        Arc::new(Self {
            sync: Arc::new(Semaphore::new(budget.sync_permits())),
            mutation: Arc::new(Semaphore::new(budget.mutation_permits())),
            retired: CancellationToken::new(),
        })
    }

    fn retire(&self) {
        self.retired.cancel();
        self.sync.close();
        self.mutation.close();
    }
}

impl BudgetGate {
    #[must_use]
    pub fn new(budget: ConcurrencyBudget) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                budget,
                // `BudgetGate::new` is public and can be reached without the
                // builder's `ConcurrencyBudget::validate` call. Preserve the
                // constructor invariant here so a direct `global: 0` cannot
                // create a gate that blocks every acquisition forever.
                global: Arc::new(Semaphore::new(budget.global.max(1))),
                accounts: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Ensure per-account semaphores exist for `account`. Re-registering an
    /// active account preserves its pools, including pools lazily created by
    /// an earlier direct `acquire`. Called from `SyncEngine::attach`.
    pub fn register(&self, account: AccountId) {
        let mut accounts = self.inner.accounts.lock().expect("poisoned");
        match accounts.entry(account) {
            Entry::Vacant(entry) => {
                entry.insert(AccountPoolsEntry::Active(AccountPools::new(
                    self.inner.budget,
                )));
            }
            Entry::Occupied(mut entry) => {
                if matches!(entry.get(), AccountPoolsEntry::Forgotten) {
                    entry.insert(AccountPoolsEntry::Active(AccountPools::new(
                        self.inner.budget,
                    )));
                }
            }
        }
    }

    /// Retire per-account semaphores. Called from `SyncEngine::detach` after
    /// workers have drained their granted permits. Pending acquisitions are
    /// canceled; a later `register` creates fresh pools. The tombstone remains
    /// so an acquire racing after detach cannot lazily resurrect this account.
    pub fn forget(&self, account: &AccountId) {
        let mut accounts = self.inner.accounts.lock().expect("poisoned");
        match accounts.entry(account.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(AccountPoolsEntry::Forgotten);
            }
            Entry::Occupied(mut entry) => {
                if let AccountPoolsEntry::Active(pools) = entry.get() {
                    pools.retire();
                }
                entry.insert(AccountPoolsEntry::Forgotten);
            }
        }
    }

    /// Acquire a permit. Returns a `BudgetPermit` that releases the
    /// permit on drop.
    ///
    /// `kind` picks between the sync and mutation sub-pools. The
    /// global outer permit is shared.
    pub async fn acquire(
        &self,
        account: &AccountId,
        kind: WorkKind,
    ) -> Result<BudgetPermit, Error> {
        let pools = self.account_pools(account)?;
        let inner_sem = match kind {
            WorkKind::Sync => Arc::clone(&pools.sync),
            WorkKind::Mutation => Arc::clone(&pools.mutation),
        };
        let inner = tokio::select! {
            acquired = inner_sem.acquire_owned() => acquired
                .map_err(|e| Error::Other(format!("account semaphore closed: {e}")))?,
            () = pools.retired.cancelled() => {
                return Err(Error::Other("account budget was forgotten".into()));
            }
        };
        let outer = tokio::select! {
            acquired = Arc::clone(&self.inner.global).acquire_owned() => acquired
                .map_err(|e| Error::Other(format!("global semaphore closed: {e}")))?,
            () = pools.retired.cancelled() => {
                return Err(Error::Other("account budget was forgotten".into()));
            }
        };
        // If both semaphore acquisition and retirement became ready together,
        // do not hand a permit from the retired generation to the caller.
        if pools.retired.is_cancelled() {
            return Err(Error::Other("account budget was forgotten".into()));
        }
        Ok(BudgetPermit {
            _outer: outer,
            _inner: inner,
        })
    }

    fn account_pools(&self, account: &AccountId) -> Result<Arc<AccountPools>, Error> {
        let mut accounts = self
            .inner
            .accounts
            .lock()
            .map_err(|_| Error::Other("account budget lock was poisoned".into()))?;
        match accounts.entry(account.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                AccountPoolsEntry::Active(pools) => Ok(Arc::clone(pools)),
                AccountPoolsEntry::Forgotten => {
                    Err(Error::Other("account budget was forgotten".into()))
                }
            },
            Entry::Vacant(entry) => {
                let pools = AccountPools::new(self.inner.budget);
                entry.insert(AccountPoolsEntry::Active(Arc::clone(&pools)));
                Ok(pools)
            }
        }
    }
}

/// Held permit; releases on drop.
pub struct BudgetPermit {
    _outer: OwnedSemaphorePermit,
    _inner: OwnedSemaphorePermit,
}

impl std::fmt::Debug for BudgetPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetPermit").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn account_waiter_does_not_hold_a_global_permit() {
        let budget = ConcurrencyBudget {
            per_account: 2,
            global: 2,
            mutation_share_num: 1,
            mutation_share_den: 2,
        };
        let gate = BudgetGate::new(budget);
        let account_a = AccountId("a".into());
        let account_b = AccountId("b".into());
        gate.register(account_a.clone());
        gate.register(account_b.clone());

        let held = gate
            .acquire(&account_a, WorkKind::Sync)
            .await
            .expect("first account permit");
        let waiting_gate = gate.clone();
        let waiting_account = account_a.clone();
        let waiter =
            tokio::spawn(
                async move { waiting_gate.acquire(&waiting_account, WorkKind::Sync).await },
            );
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Without this the permit assertion below passes vacuously if
        // the waiter never got scheduled: it has to be parked on the
        // account semaphore for "does it hold a global permit" to mean
        // anything.
        assert!(
            !waiter.is_finished(),
            "waiter must be parked on the exhausted account semaphore"
        );
        assert_eq!(
            gate.inner.global.available_permits(),
            1,
            "an account-local waiter must not consume the spare global permit"
        );
        let other = gate
            .acquire(&account_b, WorkKind::Sync)
            .await
            .expect("other account uses spare global permit");

        drop(other);
        drop(held);
        waiter
            .await
            .expect("waiter task completes")
            .expect("waiter acquires after release");
    }

    #[tokio::test]
    async fn public_constructor_floors_a_zero_global_budget() {
        let gate = BudgetGate::new(ConcurrencyBudget {
            global: 0,
            ..ConcurrencyBudget::default()
        });
        let account = AccountId("zero-global".into());

        let permit = gate
            .acquire(&account, WorkKind::Sync)
            .await
            .expect("public constructor must retain one global permit");
        drop(permit);
    }

    #[tokio::test]
    async fn register_preserves_a_lazily_created_pool() {
        let gate = BudgetGate::new(ConcurrencyBudget {
            per_account: 2,
            global: 4,
            mutation_share_num: 1,
            mutation_share_den: 2,
        });
        let account = AccountId("lazy-register".into());
        let held = gate
            .acquire(&account, WorkKind::Sync)
            .await
            .expect("first acquire lazily creates the account pools");
        gate.register(account.clone());

        let waiting_gate = gate.clone();
        let waiting_account = account.clone();
        let waiter =
            tokio::spawn(
                async move { waiting_gate.acquire(&waiting_account, WorkKind::Sync).await },
            );
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiter.is_finished(),
            "register must not replace a pool with a permit still held"
        );

        drop(held);
        waiter
            .await
            .expect("waiter task")
            .expect("the preserved pool releases its permit");
    }

    #[tokio::test]
    async fn forget_cancels_pending_acquires_and_blocks_stray_recreation() {
        let gate = BudgetGate::new(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 2,
        });
        let account = AccountId("forgotten-account".into());
        gate.register(account.clone());
        let global_held = Arc::clone(&gate.inner.global)
            .acquire_owned()
            .await
            .expect("hold the only global permit");

        let waiting_gate = gate.clone();
        let waiting_account = account.clone();
        let waiter =
            tokio::spawn(
                async move { waiting_gate.acquire(&waiting_account, WorkKind::Sync).await },
            );
        let pools = gate.account_pools(&account).expect("registered pools");
        for _ in 0..8 {
            if pools.sync.available_permits() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            pools.sync.available_permits(),
            0,
            "the waiter must hold the account permit while it waits globally"
        );

        gate.forget(&account);
        for _ in 0..8 {
            if waiter.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            waiter.is_finished(),
            "forget must cancel an acquisition waiting on the global pool"
        );
        assert!(
            waiter.await.expect("waiter task").is_err(),
            "a retired account acquisition is refused"
        );
        assert!(
            gate.acquire(&account, WorkKind::Sync).await.is_err(),
            "a stray acquire after forget must not recreate account pools"
        );

        drop(global_held);
        gate.register(account.clone());
        let permit = gate
            .acquire(&account, WorkKind::Sync)
            .await
            .expect("reattaching explicitly registers fresh pools");
        drop(permit);
    }
}
