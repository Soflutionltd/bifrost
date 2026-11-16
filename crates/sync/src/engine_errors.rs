//! Classified errors authored by the engine, including cursor contract checks.

use crate::recovery::{RecoveryPlan, plan_recovery};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountOperation, CursorScope, EngineDirective, ErrorScope,
    PrimaryCause, StateCause,
};

/// Build a sync-state cursor-invalid error scoped to a single cursor.
/// Used when the engine needs to ask itself for a fresh establishment
/// of one scope. Scope lifecycle events do not: a restart would delete a
/// healthy cursor, so they raise establish-if-absent requests instead. The derived recovery is
/// `Engine(RestartScope(scope))`.
#[must_use]
pub(crate) fn restart_scope_error(scope: CursorScope, operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::State(StateCause::CursorInvalid {
        scope: scope.clone(),
    }))
    .scope(ErrorScope::Cursor(scope))
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Synthesize scoped operator attention when automatic re-establishment
/// cannot continue. This covers engine errors with no `AccountError` of their
/// own (`EstablishCursorFailed`, `CheckpointStore`, and other non-`Account`
/// variants surfaced from `run_establish`) and provider retry advice after its
/// automatic budget is spent. Without it, a scope exhausted on engine-only
/// errors would broadcast no `SyncEvent::Terminated`, while exhausted retry
/// advice would tell consumers to retry work the engine has abandoned.
/// Emitting CursorInvalid would also be wrong: it would request another scope
/// restart after the automatic recovery budget was exhausted.
#[must_use]
pub(crate) fn establish_failure_error(
    scope: CursorScope,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::State(StateCause::OperatorOverrideNeeded {
        reason: bifrost_types::OperatorOverrideReason::EstablishRetryBudgetExhausted,
    }))
    .scope(ErrorScope::Cursor(scope))
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A stored inventory cursor is scoped state owned by the current account
/// implementation. If its classifier and resume-stream hook disagree, restart
/// only that cursor; this is not evidence that the engine's persisted envelope
/// schema changed.
#[must_use]
pub(crate) fn inventory_resume_contract_failure(scope: CursorScope) -> AccountError {
    restart_scope_error(scope, AccountOperation::SyncInventory)
}

/// Classify inventory tombstones for a cursor scope whose removal semantics
/// the engine does not know. The walk cannot safely guess whether the tombstone
/// removes one membership or destroys the object, so it must stop before
/// publishing the page or its checkpoint.
#[must_use]
pub(crate) fn inventory_removal_scope_contract_failure(scope: &CursorScope) -> AccountError {
    let kind = bifrost_types::InternalErrorKind::AccountContract;
    bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Internal(
        bifrost_types::InternalCause::new(
            kind,
            Some(bifrost_types::DiagnosticText::support_only(
                "inventory removals were emitted for an unsupported cursor scope",
            )),
        ),
    ))
    .operation(AccountOperation::SyncInventory)
    .scope(bifrost_types::ErrorScope::Cursor(scope.clone()))
    .try_build()
    .expect("valid inventory removal scope contract error")
}

/// Make the final recovery event match the action still available after its
/// automatic budget is spent. A terminal provider error remains useful as
/// authored, and an existing operator-attention verdict remains useful too.
/// Retry, reconciliation, and other engine directives no longer describe an
/// action the engine will take, so replace them with the scoped operator
/// attention error used for engine-internal establishment failures. The scope
/// may be `CursorScope::Account` when the exhausted recovery restarted the
/// whole account.
#[must_use]
pub(crate) fn recovery_exhaustion_error(
    last_error: AccountError,
    scope: CursorScope,
    operation: AccountOperation,
) -> AccountError {
    match plan_recovery(last_error.clone()) {
        RecoveryPlan::Terminal(_) => last_error,
        RecoveryPlan::Engine(EngineDirective::OperatorOverrideRequired { .. }) => last_error,
        RecoveryPlan::Retry(_) | RecoveryPlan::Reconcile(_) | RecoveryPlan::Engine(_) => {
            establish_failure_error(scope, operation)
        }
    }
}

/// Translate a cursor envelope schema mismatch into an `AccountError`
/// whose recovery derives to `Engine(SchemaIncompatible)`. Producers
/// (specifically the cursor envelope decoder) hit this path when the
/// on-disk schema is below `MIN_MIGRATABLE`. The original engine
/// `Error::SchemaIncompatible` becomes a derived account error so the
/// engine's recovery dispatch can drive the schema-clear loop.
#[must_use]
pub(crate) fn cursor_decode_failure(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::State(StateCause::SchemaIncompatible))
        .operation(operation)
        .try_build()
        .expect("valid account error classification")
}

/// Envelope provenance decides whether recovery may clear durable state.
#[derive(Clone, Copy)]
pub(crate) enum CursorOrigin {
    Fresh,
    Stored,
}

/// A fresh invalid envelope is an implementation failure. Treating it as a
/// migration would erase healthy account state without repairing the producer.
pub(crate) fn classify_envelope(
    cursor: &bifrost_types::ChangeCursor,
    origin: CursorOrigin,
    operation: AccountOperation,
    scope: &CursorScope,
) -> Result<(), AccountError> {
    cursor.validate_envelope().map_err(|_| match origin {
        CursorOrigin::Fresh => fresh_cursor_contract_error(
            Some(&bifrost_types::Checkpoint::Change(cursor.clone())),
            operation,
            scope,
        ),
        CursorOrigin::Stored => cursor_decode_failure(operation),
    })
}

/// Classify an account's violation of the batch boundary contract: a
/// `PageBoundary::Partial` batch that nonetheless carries a checkpoint.
///
/// This MUST be a classified `AccountError` and not a bare engine error.
/// `Batch`'s fields are public, so the type system cannot stop a protocol
/// crate from emitting the shape, and an engine that merely logged it would
/// re-poll the same unchanged cursor at the scope's cadence forever: no
/// backoff past `poll_max`, no recovery dispatch, no `Terminated` event, and
/// a consumer that sees a scope which has silently stopped making progress
/// with nothing in the stream to say so.
///
/// It is `Internal(AccountContract)`, not a provider fault: the boundary and
/// the checkpoint are minted by the `Account` implementation, which is client
/// code - bifrost's or a consumer's - so "contact provider support" would send
/// the operator to the one party that did nothing wrong. It derives the
/// terminal `InternalFailure`, so `plan_recovery` stops the scope - the honest
/// outcome, because an implementation emitting a nonsense boundary does not
/// heal by being asked again.
///
/// The protocol tag follows checkpoint provenance, which can differ from a
/// composed account's primary `Account::protocol()`. Without a checkpoint tag
/// it remains `Unknown`; this constructor has no account or failing leg.
#[must_use]
pub(crate) fn batch_boundary_violation(
    violation: bifrost_types::BatchBoundaryError,
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
) -> AccountError {
    contract_violation(checkpoint, operation, scope, violation.detail())
}

/// A live batch carrying a BACKFILL checkpoint. The same classification and the
/// same terminal outcome, with its own words: the boundary was fine, the
/// checkpoint belongs to a lane this stream does not publish on, and an operator
/// reading "a PageBoundary::Partial batch carried a checkpoint" would go looking
/// for the wrong thing.
#[must_use]
pub(crate) fn backfill_checkpoint_on_changes(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
) -> AccountError {
    contract_violation(
        checkpoint,
        operation,
        scope,
        "a changes batch carried a backfill checkpoint",
    )
}

/// Fresh account-authored envelopes are implementation contract failures, not
/// evidence that persisted account state needs an account-wide schema reset.
pub(crate) fn fresh_cursor_contract_error(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
) -> AccountError {
    contract_violation(
        checkpoint,
        operation,
        scope,
        "account emitted an unsupported cursor envelope",
    )
}

fn contract_violation(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
    detail: &'static str,
) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Internal(bifrost_types::InternalCause::new(
        bifrost_types::InternalErrorKind::AccountContract,
        Some(bifrost_types::DiagnosticText::support_only(detail)),
    )))
    // The protocol the offending implementation speaks, for telemetry: it
    // used to ride inside the wire cause this replaced.
    .protocol(checkpoint_protocol(checkpoint))
    .operation(operation)
    .scope(ErrorScope::Cursor(scope.clone()))
    .try_build()
    .expect("valid account error classification")
}

fn checkpoint_protocol(checkpoint: Option<&bifrost_types::Checkpoint>) -> bifrost_types::Protocol {
    use bifrost_types::{Checkpoint, Protocol};
    let Some(Checkpoint::Change(cursor)) = checkpoint else {
        // A backfill checkpoint carries no protocol tag at all. Do not guess
        // from the primary account protocol: a composed account can emit this
        // checkpoint from a different protocol leg, and operators need honest
        // provenance rather than blame assigned to the wrong implementation.
        return Protocol::Unknown;
    };
    cursor.server_state.protocol.into()
}

/// The error a `pause` / `checkpoint_now` waiter returns when the account
/// can no longer reach the boundary it asked for.
pub(crate) fn control_wait_ended(detail: &'static str) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::State(StateCause::ControlWaitEnded {
        detail: bifrost_types::DiagnosticText::support_only(detail),
    }))
    .try_build()
    .expect("valid account error classification")
}

/// Invalid answers stop this round; unanswered submitted writes remain uncertain.
pub(crate) fn mutation_outcome_contract_failure() -> AccountError {
    let kind = bifrost_types::InternalErrorKind::AccountContract;
    bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Internal(
        bifrost_types::InternalCause::new(
            kind,
            Some(bifrost_types::DiagnosticText::support_only(
                "mutation outcomes contain unknown, duplicate or missing targets",
            )),
        ),
    ))
    .try_build()
    .expect("valid mutation outcome contract failure")
}

#[cfg(test)]
#[path = "engine_errors_tests.rs"]
mod tests;
