//! Backfill checkpoint writer.
//!
//! The runner emits a `BackfillCheckpoint` at every partition boundary and a
//! finer-grained partial checkpoint at each `Batch` boundary inside a
//! partition. This module wraps the `CheckpointStore` calls so the runner
//! doesn't need to know the persistence shape.

use std::sync::Arc;

use crate::cursor::store::{CheckpointTransition, DynCheckpointStore};
use crate::engine::writer::WriterHandle;
use crate::error::Error;
use bifrost_types::{AccountId, BackfillCheckpoint, Checkpoint};

/// Where a `BackfillCheckpointWriter` sends its durable writes.
///
/// Two routes, and the difference is not cosmetic. `attached` funnels the write
/// through the account's single writer task, which owns the authoritative debt
/// ledger in memory; that is the only route with no lost update, because a
/// helper that reads the ledger itself, awaits, and writes it back will clobber
/// whatever the writer committed in between. `direct` is the older
/// read-modify-write shape against a bare store, preserved because it is what
/// externally-constructed writers had before the writer task existed - it
/// carries that race and is not what an attached engine should use.
#[derive(Clone)]
pub struct BackfillCheckpointTarget {
    inner: TargetRoute,
}

#[derive(Clone)]
enum TargetRoute {
    Writer(WriterHandle),
    Store(Arc<DynCheckpointStore>),
}

impl BackfillCheckpointTarget {
    /// Route writes through the account's single durable writer.
    pub(crate) fn attached(tx: WriterHandle) -> Self {
        Self {
            inner: TargetRoute::Writer(tx),
        }
    }

    /// Route writes straight at a checkpoint store, read-modify-write.
    ///
    /// Prefer `SyncEngine::backfill_checkpoint_writer`: this route cannot see
    /// the writer task's in-memory ledger and can therefore overwrite debt the
    /// writer raised concurrently.
    ///
    /// The race is disclosed rather than closed, on purpose. Every engine path
    /// routes through the single account writer; this constructor exists so
    /// external code can still build the type from what it can actually
    /// supply: a `CheckpointStore` is consumer-implemented, so an outside
    /// caller holds a store and nothing else. Closing the race properly means
    /// a compare-and-swap on the `CheckpointStore` TRAIT, which is a change to
    /// a published consumer-implemented interface and therefore the repository
    /// owner's call, not a fix to make here. Do not re-file the
    /// read-modify-write as an unnoticed defect.
    #[must_use]
    pub fn direct(store: Arc<DynCheckpointStore>) -> Self {
        Self {
            inner: TargetRoute::Store(store),
        }
    }
}

/// Backfill checkpoint writer.
pub struct BackfillCheckpointWriter {
    pub account_id: AccountId,
    pub store: BackfillCheckpointTarget,
}

impl BackfillCheckpointWriter {
    /// Build a writer against any target. `SyncEngine::backfill_checkpoint_writer`
    /// is the route that cannot race the account's ledger.
    #[must_use]
    pub fn new(account_id: AccountId, store: BackfillCheckpointTarget) -> Self {
        Self { account_id, store }
    }

    /// Persist `checkpoint`, PRESERVING the account's debt ledger.
    ///
    /// It used to write a record asserting complete coverage, which was a
    /// convenience constructor quietly manufacturing a proof: a write that
    /// carries no coverage report means "leave the ledger unchanged", never
    /// "everything is accounted for". The distinction is the whole point of the
    /// ledger, because a spurious `Complete` discharges debt nothing re-read.
    pub async fn persist(&self, checkpoint: BackfillCheckpoint) -> Result<(), Error> {
        match &self.store.inner {
            TargetRoute::Writer(writer) => writer.persist_backfill(checkpoint).await,
            TargetRoute::Store(store) => {
                let ledger = store.get_ledger(&self.account_id).await?;
                store
                    .apply_transition(
                        &self.account_id,
                        CheckpointTransition {
                            checkpoint: Checkpoint::Backfill(checkpoint),
                            ledger,
                        },
                    )
                    .await
            }
        }
    }
}
