//! `CheckpointStore` trait and in-memory reference implementation.
//!
//! The engine's only persistence contract. Consumers wire their own backing
//! store; the engine never assumes sled / sqlite / rocksdb / anything specific.
//! The in-memory backend is for tests and for consumers happy to lose state on
//! process restart.
//!
//! Three kinds of stored object:
//! - `ChangeCursor` per `(account, scope)`.
//! - `BackfillCheckpoint` per `(account, scope, partition)`.
//! - One `DebtLedger` per account: what enumeration coverage was PROVED, what
//!   is still owed, and what an operator has decided about it.
//!
//! Backfill partitions are addressed by their opaque `Partition` bytes so
//! resume can pick up inside a partition rather than at the start.
//!
//! # The atomicity contract
//!
//! A checkpoint and the ledger state it implies are ONE logical write. They
//! cannot be two store calls, and no ordering of two independent writes is
//! crash-safe:
//!
//! - checkpoint first loses the debt: the cursor advances past objects nothing
//!   recorded, and the changes stream only reports what happens NEXT, so those
//!   objects are permanently invisible;
//! - ledger first records debt for progress that never committed.
//!
//! That is why [`CheckpointStore::apply_transition`] is the only operation that
//! advances a checkpoint, and it takes the ledger with it. The account-wide
//! reset, [`CheckpointStore::clear_account_checkpoints`], is atomic in the same
//! way: the rows go and the ledger is written in one operation. A backend may
//! spread the state across several tables - the representation is
//! backend-owned, the atomicity is not.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bifrost_types::{
    AccountId, BackfillCheckpoint, ChangeCursor, Checkpoint, CursorScope, Partition,
};

use crate::cursor::ledger::DebtLedger;
use crate::error::Error;

/// One durable checkpoint advance, together with the ledger state that becomes
/// true at the same instant.
///
/// The ledger is a whole-account snapshot rather than a delta. The single
/// writer owns the authoritative copy and hands down what it should now be, so
/// a backend never has to replay transitions or reason about ordering to
/// reconstruct it.
#[derive(Debug, Clone)]
pub struct CheckpointTransition {
    pub checkpoint: Checkpoint,
    pub ledger: DebtLedger,
}

/// Persistence contract.
///
/// All methods are async because real persistence backends are IO-bound. The
/// in-memory impl returns ready futures.
pub trait CheckpointStore: Send + Sync {
    /// Persist a checkpoint advance and the account's ledger, atomically.
    ///
    /// The whole transition lands or none of it does. Partial persistence is a
    /// contract violation, not a degraded mode.
    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Drop every change cursor and every backfill checkpoint stored for
    /// `account` - every scope, completion markers included - and persist
    /// `ledger` as the account's debt ledger, as ONE atomic operation.
    ///
    /// Used by the engine's `SchemaIncompatible` recovery and by attach when it
    /// meets a row it cannot read. A schema bump means the ids the consumer
    /// holds were minted under an encoding the protocol has disowned, and any
    /// surviving completion marker - including one of a scope the engine does
    /// not currently track - makes the next establishment of that scope skip
    /// the inventory walk that re-mints them. There is deliberately no default
    /// body: the trait has no enumeration, so any default would leave exactly
    /// those rows behind.
    ///
    /// Atomic, like `apply_transition`: either every row is gone and the
    /// ledger is written, or nothing changed. Rows of other accounts are
    /// untouched. The ledger passed in is the account's existing ledger with
    /// retired debt folded in, never an empty one: a reset proves no coverage.
    ///
    /// What an implementation needs: one transaction spanning the account's
    /// change cursors, its backfill rows and its ledger, plus a way to address
    /// every row of one account (a key prefix or an account column) without the
    /// engine naming the scopes.
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Read the latest change cursor for `(account, scope)` if any.
    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>;

    /// Read the latest backfill checkpoint for `(account, scope)`.
    /// Returns the one with the greatest `items_done` if the store has more
    /// than one partition's worth of state. Ties must be stable: a page
    /// partition with the greatest parsed upper bound wins, followed by
    /// lexicographically-greatest opaque partition bytes.
    ///
    /// The default `InMemoryCheckpointStore` implementation scans every
    /// backfill entry per call (O(n) on total partitions per account).
    /// Production backends backing onto a real store should maintain a
    /// per-`(account, scope)` "latest" index for constant-time reads; the trait
    /// does not require it because the engine calls this infrequently (resume
    /// path, observability), not on the hot path.
    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a>>;

    /// Persist the account's debt ledger with no checkpoint advance.
    ///
    /// For the two durable mutations that genuinely have no cursor to ride: a
    /// barrier incident (nothing advanced, by definition) and an operator
    /// decision. Everything that DOES advance a cursor must use
    /// [`Self::apply_transition`] instead, so the two land together.
    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Read the account's debt ledger.
    ///
    /// This is how open debt is ENUMERATED. `get_backfill` deliberately cannot
    /// serve that: it returns one selected row, so even with per-partition
    /// coverage stored correctly, nothing could list what a scope still owes
    /// across all its partitions.
    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn Future<Output = Result<DebtLedger, Error>> + Send + 'a>>;

    /// Convenience: persist a change cursor that makes no coverage claim.
    ///
    /// DO NOT OVERRIDE. It reads the ledger and writes it back unchanged, so
    /// "this write says nothing about coverage" means the debt survives - it
    /// emphatically does not mean coverage is complete. An earlier revision had
    /// a convenience of this shape that stamped `Complete` on every write, and
    /// that is precisely how a durable record ends up certifying coverage
    /// nothing proved.
    ///
    /// Not atomic in the strict sense - it is two store calls - but it does not
    /// need to be: the ledger it writes back is the one it just read, so a
    /// crash between them loses nothing. Anything that CHANGES the ledger must
    /// use [`Self::apply_transition`].
    fn put_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        cursor: ChangeCursor,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            let ledger = self.get_ledger(account).await?;
            self.apply_transition(
                account,
                CheckpointTransition {
                    checkpoint: Checkpoint::Change(cursor),
                    ledger,
                },
            )
            .await
        })
    }

    /// Convenience: persist a backfill checkpoint that makes no coverage claim.
    ///
    /// DO NOT OVERRIDE. Same contract as [`Self::put_change_cursor`].
    fn put_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        checkpoint: BackfillCheckpoint,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            let ledger = self.get_ledger(account).await?;
            self.apply_transition(
                account,
                CheckpointTransition {
                    checkpoint: Checkpoint::Backfill(checkpoint),
                    ledger,
                },
            )
            .await
        })
    }

    /// Drop the change cursor for `(account, scope)`. Used by the engine's
    /// `EngineDirective::RestartScope` recovery path so the next attach / poll
    /// re-establishes via inventory. This is required because a no-op delete
    /// silently preserves the stale durable cursor and makes restart-scope
    /// recovery ineffective.
    ///
    /// Deliberately does NOT touch the ledger: a cursor invalidation is not
    /// proof that anything the scope owed was found.
    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Drop every backfill checkpoint for `(account, scope)`, the durable
    /// completion marker included. Used by the engine's scope-deletion reset (a
    /// marker surviving into a folder recreated under the same id would skip
    /// its cold-start walk) and by the discard that follows a walk which lost
    /// pages, so a no-op here silently keeps rows the engine relies on being
    /// gone. Routine `RestartScope` and `DisableScope` recovery deliberately do
    /// NOT call this: re-walking a completed backfill after every cursor
    /// invalidation would re-hydrate the whole scope for no reason. Schema
    /// recovery uses [`CheckpointStore::clear_account_checkpoints`] instead.
    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
}

/// Engine-side erased handle type. The slot holds `Arc<dyn CheckpointStore>`
/// cloned across tasks.
pub type DynCheckpointStore = dyn CheckpointStore;

/// In-memory reference implementation. Keyed by `(AccountId, CursorScope)` for
/// change cursors and by `(AccountId, CursorScope, Partition)` for backfill
/// checkpoints.
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    change: HashMap<(AccountId, CursorScope), ChangeCursor>,
    backfill: HashMap<(AccountId, CursorScope, Partition), BackfillCheckpoint>,
    ledgers: HashMap<AccountId, DebtLedger>,
}

impl InMemoryCheckpointStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap in an `Arc` for the engine. Convenience over
    /// `Arc::new(InMemoryCheckpointStore::new())`.
    #[must_use]
    pub fn arc() -> Arc<dyn CheckpointStore> {
        Arc::new(Self::new())
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            guard.change.retain(|(aid, _), _| aid != account);
            guard.backfill.retain(|(aid, _, _), _| aid != account);
            guard.ledgers.insert(account.clone(), ledger);
            Ok(())
        })
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        Box::pin(async move {
            // One lock, one critical section: checkpoint and ledger cannot land
            // apart. A backend spreading them over tables owes the same
            // guarantee through a transaction.
            let mut guard = self.inner.lock().expect("poisoned");
            match transition.checkpoint {
                Checkpoint::Change(cursor) => {
                    guard
                        .change
                        .insert((account.clone(), cursor.scope.clone()), cursor);
                }
                Checkpoint::Backfill(checkpoint) => {
                    let key = (
                        account.clone(),
                        checkpoint.scope.clone(),
                        checkpoint.partition.clone(),
                    );
                    guard.backfill.insert(key, checkpoint);
                }
                _ => {
                    return Err(Error::CheckpointStore(
                        "unknown checkpoint variant in transition".into(),
                    ));
                }
            }
            guard.ledgers.insert(account, transition.ledger);
            Ok(())
        })
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>> {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let guard = self.inner.lock().expect("poisoned");
            Ok(guard.change.get(&(account, scope)).cloned())
        })
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a>> {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let guard = self.inner.lock().expect("poisoned");
            let mut latest: Option<BackfillCheckpoint> = None;
            for ((aid, s, _p), ck) in &guard.backfill {
                if aid == &account && s == &scope {
                    let beats_current = match &latest {
                        None => true,
                        Some(existing) => backfill_checkpoint_is_later(ck, existing),
                    };
                    if beats_current {
                        latest = Some(ck.clone());
                    }
                }
            }
            Ok(latest)
        })
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            guard.ledgers.insert(account, ledger);
            Ok(())
        })
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn Future<Output = Result<DebtLedger, Error>> + Send + 'a>> {
        let account = account.clone();
        Box::pin(async move {
            let guard = self.inner.lock().expect("poisoned");
            Ok(guard.ledgers.get(&account).cloned().unwrap_or_default())
        })
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            guard.change.remove(&(account, scope));
            Ok(())
        })
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            guard
                .backfill
                .retain(|(aid, s, _), _| !(aid == &account && s == &scope));
            Ok(())
        })
    }
}

fn backfill_checkpoint_is_later(
    candidate: &BackfillCheckpoint,
    current: &BackfillCheckpoint,
) -> bool {
    match candidate
        .progress
        .items_done
        .cmp(&current.progress.items_done)
    {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            let candidate_page =
                crate::backfill::partitioner::parse_page_partition(&candidate.partition);
            let current_page =
                crate::backfill::partitioner::parse_page_partition(&current.partition);
            match (candidate_page, current_page) {
                (Some((_, candidate_to)), Some((_, current_to))) if candidate_to != current_to => {
                    candidate_to > current_to
                }
                _ => candidate.partition.0 > current.partition.0,
            }
        }
    }
}
