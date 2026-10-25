// The crate-internal `Error` aggregates engine failure modes plus a wrapped
// `AccountError` when one crosses a sync boundary. `AccountError` itself is
// `Arc<Inner>`-backed and small; the engine's own variants account for the
// other size. Boxing each variant would add allocation churn in the engine
// hot path (cursor decode, checkpoint write) without changing the trait
// surface that consumers see, which already returns `Result<_, AccountError>`.
#![allow(clippy::result_large_err)]
//! `bifrost-sync` is the engine that drives `bifrost-types::Account`
//! implementations.
//!
//! Three layers (see `reference/jmap.md`, `reference/imap.md` for
//! per-protocol detail):
//!
//! - `bifrost-net` (sibling) - shared transport.
//! - `bifrost-{jmap,imap,gmail,graph,smtp}` (sibling) - per-protocol
//!   wire primitives.
//! - `bifrost-sync` (this crate) - scheduler, multiplexer, backfill,
//!   push reconciliation, mutation pipeline, checkpoint envelope,
//!   cancellation, observability.
//!
//! Compile-time clean: the engine depends only on `bifrost-types`. Any
//! protocol crate is opaque to the engine; consumers wire one
//! `Arc<dyn AccountFactory>` per account and the engine owns the
//! lifecycle.

#![forbid(unsafe_code)]

pub mod backfill;
pub mod cancel;
pub mod control;
pub mod cursor;
pub mod engine;
pub mod error;
mod inventory_walk;
pub mod multiplexer;
pub mod mutation;
pub mod push;
pub mod recovery;
pub mod repair;
pub mod scheduler;
pub mod types;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
pub(crate) mod test_common;

// Engine entry types.
pub use engine::{SyncEngine, SyncEngineBuilder};

// Control surface and priority (re-export from bifrost-types so
// consumers can import them off the engine crate directly).
pub use bifrost_types::{Control, Priority};

// Push surface (sink + watch events).
pub use bifrost_types::{InvalidationSink, WatchEvent};

// Engine-side error and the shared warning vocabulary. Terminal
// account errors are surfaced through `bifrost_types::Fatal` (the
// engine boundary newtype) via `RecoveryPlan::Terminal`.
pub use error::{Error, Warning};

// Throttle bucket so consumers / instrumentation can observe engine
// throttle state if needed.
pub use recovery::ThrottleBucket;

// Cursor and checkpoint plumbing.
pub use cursor::{
    BarrierIncident, CheckpointStore, CheckpointTransition, CursorRegistry, DebtLedger,
    DischargeEvidence, DynCheckpointStore, ENGINE_VERSION, InMemoryCheckpointStore,
    LEDGER_ENVELOPE_VERSION, LedgerEntry, MIN_MIGRATABLE, MIN_MIGRATABLE_LEDGER, PendingCoverage,
    PolicyStatus, ProofStatus, PublicationId, PublicationReceipt, Publications, decode_envelope,
    decode_ledger, encode_envelope, encode_ledger,
};

// Inventory coverage repair.
pub use repair::{DEFAULT_REPAIR_BUDGET, RepairResolution, run_repair_pass};

// Backfill policy types.
pub use backfill::{BackfillPolicy, BackfillStrategy};

// Engine configuration knobs.
pub use scheduler::{BudgetGate, ConcurrencyBudget, Scheduler, WorkKind};
pub use types::{
    BackfillConfig, EngineConfig, MultiplexerConfig, MutationConfig, PushConfig, SchedulerConfig,
};

// Engine-side control implementation, exposed so tests / consumers
// pattern-match on the concrete type when convenient.
pub use control::SyncControl;

// Cancellation primitive.
pub use cancel::{Boundary, BoundaryRequest, BoundaryView};

// Mutation pipeline entry points.
pub use mutation::{
    IdempotencyVendor, MutationCampaignId, MutationCounters, PartialCommitObservation,
    ReadbackOutcome, run_readback_guard,
};

// Multiplexer types consumers want to pattern-match against (the
// per-account broadcast yields `MultiplexerEvent`).
pub use multiplexer::{ChangesReceiver, MultiplexerEvent, ReopenRequest};
