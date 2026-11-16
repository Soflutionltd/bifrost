//! Engine-side error vocabulary.
//!
//! Distinct from `bifrost_types::AccountError` (the per-operation,
//! protocol-facing failure type) and from `bifrost_types::Fatal` (the
//! terminal-only newtype the engine's `RecoveryPlan::Terminal` arm
//! emits). `engine::Error` wraps an `AccountError` when one crosses a
//! sync boundary, plus the engine's own failure modes (account not
//! attached, checkpoint store rejected the write, shutdown in
//! progress).

use bifrost_types::{AccountError, AccountId, CursorScope, ObjectId, Priority};

/// Engine-side failure type.
///
/// Returned from `SyncEngine` orchestration calls (`attach`, `detach`,
/// `bulk_set_flags`, `checkpoint_now`, etc.). The `Account` variant
/// carries the protocol-facing `AccountError` verbatim so callers see
/// the same recovery and diagnostics the protocol crate produced.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Caller addressed an account that was never attached or has
    /// already been detached.
    #[error("account {0:?} is not attached to this engine")]
    AccountNotAttached(AccountId),
    /// `attach` was called twice for the same account id without an
    /// intervening `detach`.
    #[error("account {0:?} is already attached")]
    AccountAlreadyAttached(AccountId),
    /// `AccountFactory::open` returned an error during `attach`. A
    /// caller seeing this knows the account never reached the running
    /// state. Recovery paths after attach use `Account` instead.
    #[error("failed to open account")]
    OpenFailed(#[source] AccountError),
    /// `establish_initial_cursor` returned an error.
    #[error("failed to establish initial cursor: {0}")]
    EstablishCursorFailed(EstablishFailure),
    /// Cursor-establishing inventory ended in a terminal account
    /// error. The full `AccountError` is preserved so callers can
    /// inspect recovery, scope, operation, provider, and diagnostics.
    #[error("cursor establishment terminated")]
    EstablishCursorTerminated(#[source] AccountError),
    /// Cursor-establishing inventory reached no numbered receiver. The walk
    /// remains unfinished and can be replayed under the owner's retry budget.
    #[error("inventory publication reached no numbered receiver")]
    InventoryUndelivered,
    /// Checkpoint persistence failed.
    #[error("checkpoint store rejected the write: {0}")]
    CheckpointStore(String),
    /// A caller's acknowledgement was rejected before a store write.
    #[error("acknowledgement rejected: {0}")]
    AckRejected(AckRejection),
    /// A valid acknowledgement reached the store, which failed. Preserve its
    /// original error without confusing it with a rejected publication token.
    #[error("acknowledgement persistence failed: {0}")]
    AckStore(#[source] Box<Error>),
    #[error("scheduler admission failed: {0}")]
    Admission(#[from] AdmissionFailure),
    #[error("mutation target {0:?} has conflicting caller versions")]
    ConflictingCallerVersions(ObjectId),
    #[error("inventory Done cursor has scope {actual:?}; expected {expected:?}")]
    InventoryScopeMismatch {
        expected: CursorScope,
        actual: CursorScope,
    },
    #[error("inventory fusion carried an unsupported checkpoint")]
    InventoryCheckpointUnsupported,
    #[error("inventory Done carried no checkpoint for establishment")]
    InventoryCheckpointMissing,
    /// Cursor envelope on disk uses a schema this engine cannot read.
    /// Local-only: when this would cross into recovery dispatch, the
    /// engine converts it into an `AccountError` with
    /// `SyncStateErrorKind::SchemaIncompatible` so derivation yields
    /// `EngineDirective::SchemaIncompatible`.
    #[error("checkpoint envelope schema is incompatible")]
    SchemaIncompatible,
    /// Engine is shutting down; no new work accepted.
    #[error("engine is shutting down")]
    ShuttingDown,
    /// Recovery holds consumed the cumulative wait allowance.
    #[error("recovery wait budget exhausted")]
    WaitBudgetExhausted,
    /// Account is paused.
    #[error("account is paused")]
    Paused,
    /// The work was invalidated by a reset or deletion of its scope that
    /// closed while it ran; whoever closed it owns the follow-up. Not an
    /// `AccountError`, and never routed through recovery dispatch.
    #[error("work superseded by a reset or deletion")]
    Superseded,
    /// Account operation surfaced through an engine path that is not
    /// `attach`. The carried error has its derived `RecoveryClass`
    /// intact; consumers route through it the same way they route any
    /// other `AccountError`.
    #[error("account operation failed")]
    Account(#[from] AccountError),
    /// Catch-all for engine-internal errors that have no account
    /// counterpart (config validation, malformed checkpoint type, etc.).
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EstablishFailure {
    #[error("unknown CursorEstablishment variant")]
    UnsupportedVariant,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AdmissionFailure {
    #[error("lane {priority:?} is full (capacity {capacity})")]
    LaneFull { priority: Priority, capacity: usize },
    #[error("account {0:?} budget was forgotten")]
    AccountForgotten(AccountId),
    #[error("account budget lock was poisoned")]
    BudgetPoisoned,
    #[error("account semaphore closed")]
    AccountPoolClosed,
    #[error("global semaphore closed")]
    GlobalPoolClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AckRejection {
    #[error("unknown publication")]
    UnknownPublication,
    #[error("scope does not match checkpoint")]
    ScopeMismatch,
    #[error("unsupported checkpoint")]
    UnsupportedCheckpoint,
    #[error("invalid checkpoint envelope")]
    InvalidEnvelope,
}

/// Successful acknowledgement does not necessarily advance a checkpoint.
/// Enterprise consumers need the distinction before claiming sync is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use = "inspect whether acknowledgement became durable or completion was withheld"]
pub enum AckOutcome {
    /// Checkpoint-free repair delivery accepted; no checkpoint was offered.
    RepairAccepted,
    Durable,
    Withheld(CompletionRefusal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompletionRefusal {
    /// Repair or waive the scope's outstanding obligations before completion.
    OpenDebt,
    /// Some pages were lost; restart the inventory walk.
    WalkNotWhole,
    /// This attachment cannot vouch for the offered publication.
    Unvouchable,
}

/// Re-export of `bifrost_types::Warning`. Engine callers continue to
/// refer to it as `bifrost_sync::Warning` so a later change to the
/// engine's warning surface (currently identical to the types surface)
/// does not churn import paths.
pub type Warning = bifrost_types::Warning;

impl From<bifrost_types::wire::DecodeError> for Error {
    fn from(error: bifrost_types::wire::DecodeError) -> Self {
        match error {
            bifrost_types::wire::DecodeError::SchemaIncompatible => Self::SchemaIncompatible,
            // Preserve malformed-input classification for future shared faults.
            other => Self::Other(other.to_string()),
        }
    }
}
