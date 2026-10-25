//! Engine-side error vocabulary.
//!
//! Distinct from `bifrost_types::AccountError` (the per-operation,
//! protocol-facing failure type) and from `bifrost_types::Fatal` (the
//! terminal-only newtype the engine's `RecoveryPlan::Terminal` arm
//! emits). `engine::Error` wraps an `AccountError` when one crosses a
//! sync boundary, plus the engine's own failure modes (account not
//! attached, checkpoint store rejected the write, shutdown in
//! progress).

use bifrost_types::{AccountError, AccountId};

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
    EstablishCursorFailed(String),
    /// Cursor-establishing inventory ended in a terminal account
    /// error. The full `AccountError` is preserved so callers can
    /// inspect recovery, scope, operation, provider, and diagnostics.
    #[error("cursor establishment terminated")]
    EstablishCursorTerminated(#[source] AccountError),
    /// Checkpoint persistence failed.
    #[error("checkpoint store rejected the write: {0}")]
    CheckpointStore(String),
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

/// Re-export of `bifrost_types::Warning`. Engine callers continue to
/// refer to it as `bifrost_sync::Warning` so a later change to the
/// engine's warning surface (currently identical to the types surface)
/// does not churn import paths.
pub type Warning = bifrost_types::Warning;
