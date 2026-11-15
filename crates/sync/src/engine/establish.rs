//! Shared stored-or-fresh cursor establishment and inline inventory fusion.

use super::recovery::pacing::RepairPacing;
use super::*;
use crate::delivery::PagePublisher;

/// How `run_establish` obtained a scope's cursor. Reattach uses this to
/// decide which durable rows an aborted swap may delete: only a cursor
/// this establishment created fresh corresponds to a row the reattach
/// itself will write, so only those may be compensated by deletion. The
/// distinction is reported from inside `run_establish` - off the single
/// store read it already performs - rather than probed by the caller
/// beforehand, because a separate pre-check races that read (and a
/// pre-check that swallowed a store error once misclassified a
/// preexisting row as freshly created, which an abort then deleted).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::engine) enum EstablishOrigin {
    /// A durable row already existed and was resumed from (directly, or
    /// by finishing the inventory it recorded). The stored row is
    /// preexisting data that no reattach abort may touch.
    ResumedStored,
    /// No durable row existed; whatever cursor the registry now holds
    /// for the scope was created fresh by this establishment.
    CreatedFresh,
}

/// The shared establishment decision. Callers retain ownership of persistence,
/// fencing and inventory delivery: attach must wait for subscribers, while a
/// running repair already has a delivery gate and may finish inventory inline.
pub(in crate::engine) enum Establishment {
    Ready(ChangeCursor),
    Resume {
        cursor: ChangeCursor,
        classified_as_inventory: bool,
        stream: Option<AccountStream<bifrost_types::InventoryEvent>>,
    },
    Inventory,
}

pub(in crate::engine) async fn establishment(
    account: &dyn Account,
    scope: &CursorScope,
    stored: Option<ChangeCursor>,
) -> Result<Establishment, Error> {
    use crate::engine_errors::{CursorOrigin, classify_envelope};
    use bifrost_types::AccountOperation;
    if let Some(cursor) = stored {
        classify_envelope(
            &cursor,
            CursorOrigin::Stored,
            AccountOperation::EstablishCursor,
            scope,
        )
        .map_err(Error::Account)?;
        // Either positive answer stays out of changes_stream. Fusion owns the
        // disagreement check; a malformed inventory contract must not skip mail.
        let classified_as_inventory = account.is_inventory_cursor(&cursor);
        let stream = account.inventory_resume_stream(cursor.clone());
        return if classified_as_inventory || stream.is_some() {
            Ok(Establishment::Resume {
                cursor,
                classified_as_inventory,
                stream,
            })
        } else {
            Ok(Establishment::Ready(cursor))
        };
    }
    match account
        .establish_initial_cursor(scope.clone())
        .await
        .map_err(Error::Account)?
    {
        CursorEstablishment::Ready(cursor) => {
            classify_envelope(
                &cursor,
                CursorOrigin::Fresh,
                AccountOperation::EstablishCursor,
                scope,
            )
            .map_err(Error::Account)?;
            Ok(Establishment::Ready(cursor))
        }
        CursorEstablishment::EstablishViaInventory => Ok(Establishment::Inventory),
        _ => Err(Error::EstablishCursorFailed(
            crate::EstablishFailure::UnsupportedVariant,
        )),
    }
}

/// Re-establish a single scope using the shared establishment decision.
///
/// `walk_fence` is the scope fence the CALLER's ownership of this
/// establishment began under; this function never reads `scope_fence` itself.
/// It awaits the writer's store read and `pacing.admit` before either fusion,
/// and a deletion landing in those awaits raises the fence: a reading taken
/// here would adopt that deletion as the walk's own, so a stale resume stream
/// would publish and install past every conditional check. Every install is
/// therefore `put_if` against `walk_fence`, both fusions take it as their
/// `expected_fence`, and a moved fence ends the call with `Error::Superseded`.
#[allow(clippy::too_many_arguments)]
pub(in crate::engine) async fn run_establish(
    account_id: &AccountId,
    account: &dyn Account,
    scope: CursorScope,
    cursors: Arc<CursorRegistry>,
    writer: &WriterHandle,
    delivery: Arc<ChangeDelivery>,
    control: &SyncControl,
    coverage: Arc<PendingCoverage>,
    persist_ready: bool,
    shutdown: &CancellationToken,
    reattach_attempt: Option<crate::engine::ReattachAttempt>,
    ctx: &RecoveryContext<'_>,
    pacing: &mut RepairPacing,
    walk_fence: crate::cursor::ScopeFence,
) -> Result<EstablishOrigin, Error> {
    let result = async {
        let _activity = control.begin_activity().ok_or(Error::Paused)?;
        // Only a reattach passes an attempt. Its resume read claims a still
        // provisional row through the writer. An ordinary read (the live
        // scope's recovery) is shown a still-provisional row as absent and
        // establishes fresh; its PersistEstablished then owns the row.
        let stored = match writer
            .get_change_cursor(scope.clone(), reattach_attempt)
            .await
        {
            Ok(stored) => stored,
            // A replacement can still abort. Never clear an unreadable stored
            // row here: consumer acknowledgements may have advanced the live
            // account, and provisional-row rollback cannot restore that state.
            Err(Error::SchemaIncompatible) => {
                return Err(Error::Account(crate::engine_errors::cursor_decode_failure(
                    bifrost_types::AccountOperation::EstablishCursor,
                )));
            }
            Err(other) => return Err(other),
        };
        // Durable row classification is local work, not a provider attempt.
        // Diagnose an unreadable row before spending the provider wait budget.
        if let Some(cursor) = &stored {
            crate::engine_errors::classify_envelope(
                cursor,
                crate::engine_errors::CursorOrigin::Stored,
                bifrost_types::AccountOperation::EstablishCursor,
                &scope,
            )
            .map_err(Error::Account)?;
        }
        let origin = if stored.is_some() {
            EstablishOrigin::ResumedStored
        } else {
            EstablishOrigin::CreatedFresh
        };
        // The writer may have parked while another lane recorded a hold.
        let admitted = pacing
            .admit(ctx, control, Some(account), Some(&scope))
            .await?;
        if coverage.scope_fence(&scope) != walk_fence {
            return Err(Error::Superseded);
        }
        // Only consume the previous failure's local hold after admission has
        // honored it; a stored row read must not erase retry pacing.
        pacing.failed_keys = crate::throttle::declared_keys(
            account,
            account_id,
            bifrost_types::ThrottleWork::Scope(&scope),
        );
        pacing.local_deadline = None;
        let decision = establishment(account, &scope, stored).await?;
        drop(admitted);
        let (classified_as_inventory, resume_stream) = match decision {
            Establishment::Ready(cursor) => {
                if persist_ready && origin == EstablishOrigin::CreatedFresh {
                    // Establishment proves no coverage; preserve the ledger.
                    writer.persist_established(cursor.clone()).await?;
                }
                if !cursors.put_if(cursor, || coverage.scope_fence(&scope) == walk_fence) {
                    return Err(Error::Superseded);
                }
                return Ok(origin);
            }
            Establishment::Resume {
                classified_as_inventory,
                stream,
                ..
            } => (Some(classified_as_inventory), stream),
            Establishment::Inventory => (None, None),
        };
        let _admitted = pacing
            .admit(ctx, control, Some(account), Some(&scope))
            .await?;
        if coverage.scope_fence(&scope) != walk_fence {
            return Err(Error::Superseded);
        }
        let fusion = crate::multiplexer::InventoryFusion {
            expected_fence: walk_fence,
            account_id: account_id.clone(),
            cursors: Arc::clone(&cursors),
            publisher: PagePublisher::new(
                delivery,
                control.clone(),
                writer.clone(),
                shutdown.clone(),
            ),
            generation: coverage.next_generation(),
        };
        let outcome = match classified_as_inventory {
            Some(classified) => {
                fusion
                    .run_resume_stream_with_broadcast(scope.clone(), classified, resume_stream)
                    .await?
            }
            None => fusion.run_with_broadcast(account, scope).await?,
        };
        match outcome {
            crate::multiplexer::FusionOutcome::Established
            | crate::multiplexer::FusionOutcome::NoCursor => Ok(origin),
            crate::multiplexer::FusionOutcome::Invalidated => Err(Error::Superseded),
            // Lost delivery is unfinished establishment, not successful NoCursor.
            // Keep it in the owner's bounded retry policy.
            crate::multiplexer::FusionOutcome::Undelivered => Err(Error::InventoryUndelivered),
            crate::multiplexer::FusionOutcome::Terminated(error) => {
                Err(Error::EstablishCursorTerminated(error))
            }
        }
    }
    .await;
    if let Err(Error::Account(error) | Error::EstablishCursorTerminated(error)) = &result {
        pacing.observe(ctx, error);
    }
    result
}
