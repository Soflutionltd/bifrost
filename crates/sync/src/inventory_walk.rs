//! Safety-critical state shared by both inventory walk front ends.
//!
//! Backfill and inventory fusion used to carry two independent copies of the
//! barrier rules, and their DIVERGENCE was the defect: one front end stopped
//! the walk at a barrier while the other kept accepting checkpoints past it.
//! Both now hold an `InventoryWalk` and route every barrier decision through
//! this module, so a rule can only be changed for both at once.
//!
//! What is shared here is the SAFETY-CRITICAL half only: the barrier decision
//! and the last accepted resume checkpoint. Checkpoint MINTING and terminal
//! `Done` handling remain two implementations, because the shapes genuinely
//! differ - fusion forwards the account's own checkpoint, backfill mints a
//! positional `page:F:T` from partition coordinates the account never sees.
//! That is an accepted structural residual, not an oversight: no loss path has
//! been found through it across five rounds and two close passes. What remains
//! is a RE-DIVERGENCE RISK, not a defect - the two minting sites can drift the
//! way the barrier rules once did. Unifying them is worth doing only when one
//! of two things happens: a THIRD inventory front end appears (at which point
//! the shapes have to be reconciled anyway), or a defect is actually traced to
//! the split. Until then the shapes are different enough that a forced common
//! abstraction would cost more than it buys.
//!
//! Both front ends must refuse to announce a barrier the store REFUSED - the
//! backfill partition fails so the scope stays Pending and re-walks, the
//! fusion walk returns the error. A DEPARTED writer (detach or shutdown) is
//! deliberately non-fatal, because there is nothing left to persist to; a
//! store error is fatal, because there is. Getting this onto one front end and
//! not the other is exactly the divergence this module exists to prevent, and
//! it happened once already.

use bifrost_types::{Checkpoint, InventoryCoverageReport};

use crate::error::Error;

/// Project inventory tombstones into the same removal vocabulary as changes.
/// Keep these in the page's synthetic batch so its checkpoint cannot be
/// acknowledged independently of the removals it covers.
pub(crate) fn removals<'a>(
    scope: &bifrost_types::CursorScope,
    removed: impl IntoIterator<Item = &'a bifrost_types::ObjectId>,
) -> Result<Vec<bifrost_types::Change>, bifrost_types::AccountError> {
    use bifrost_types::{
        Change, CursorScope, MembershipScope, ObjectChange, ObjectChangeKind, ScopeChange,
        ScopeChangeKind,
    };
    let mut removed = removed.into_iter().peekable();
    if removed.peek().is_none() {
        return Ok(Vec::new());
    }
    let membership = match scope {
        CursorScope::Folder(folder) | CursorScope::FolderType { folder, .. } => {
            Some(MembershipScope::Folder(folder.clone()))
        }
        CursorScope::Query(query) => Some(MembershipScope::Query(query.clone())),
        CursorScope::Account | CursorScope::Type(_) => None,
        _ => {
            return Err(crate::recovery::inventory_removal_scope_contract_failure(
                scope,
            ));
        }
    };
    Ok(removed
        .map(|id| match &membership {
            Some(membership) => Change::ScopeChange(ScopeChange {
                id: id.clone(),
                membership: membership.clone(),
                kind: ScopeChangeKind::Removed,
            }),
            None => Change::ObjectChange(ObjectChange {
                id: id.clone(),
                kind: ObjectChangeKind::Destroyed,
            }),
        })
        .collect())
}

pub(crate) enum WalkDecision {
    Advance,
    StopAtBarrier { resume_from: Option<Checkpoint> },
}

#[derive(Default)]
pub(crate) struct InventoryWalk {
    last_accepted: Option<Checkpoint>,
}

impl InventoryWalk {
    pub(crate) fn inspect(&self, coverage: &InventoryCoverageReport) -> WalkDecision {
        if coverage.has_barrier() {
            WalkDecision::StopAtBarrier {
                resume_from: self.last_accepted.clone(),
            }
        } else {
            WalkDecision::Advance
        }
    }

    pub(crate) fn accept(&mut self, checkpoint: Option<Checkpoint>) {
        if checkpoint.is_some() {
            self.last_accepted = checkpoint;
        }
    }
}

/// The warning both front ends emit when an inventory stream ends with neither
/// `Done` nor `Terminated`.
///
/// Every walk must end in one of the two, so a silent end is a producer
/// breaking its stream contract, and nothing says how far the enumeration got.
/// Neither front end treats the walk as complete: backfill leaves the scope
/// pending instead of writing its completion marker, and fusion establishes no
/// cursor. Pages acknowledged earlier in the walk still stand; it is the walk
/// as a whole that proved nothing. The user sees a scope that never finishes,
/// so the reason travels on
/// the change stream as a typed warning rather than a log line (ruled
/// 2026-10-04).
pub(crate) fn unannounced_end_warning() -> bifrost_types::Warning {
    bifrost_types::Warning::user_safe(
        bifrost_types::WarningKind::InventoryEndedUnannounced,
        "inventory for this scope ended without reporting completion, so the scope is \
         not treated as fully synced",
    )
}

pub(crate) fn barrier_incidents(
    coverage: &InventoryCoverageReport,
    generation: u64,
    resume_from: Option<Checkpoint>,
) -> Vec<crate::cursor::BarrierIncident> {
    coverage
        .obligations()
        .iter()
        .filter_map(|obligation| {
            let bifrost_types::InventoryObligation::Region {
                key,
                failure_label,
                error,
                recovery,
            } = obligation
            else {
                return None;
            };
            recovery
                .is_barrier()
                .then(|| crate::cursor::BarrierIncident {
                    key: key.clone(),
                    domain: coverage.domain.clone(),
                    generation,
                    failure_label: failure_label.clone(),
                    evidence: error.clone(),
                    policy: crate::cursor::PolicyStatus::Retrying { attempts: 0 },
                    resume_from: resume_from.clone(),
                })
        })
        .collect()
}

/// Persist every barrier incident in `coverage` through the account writer.
///
/// The inner `Result` is the whole point. A dropped sender means the writer
/// task is gone (detach or shutdown), and there is nothing left to persist to
/// or to retry against; but a writer that ran the request and answered
/// `Err(store_error)` has left the incident NOWHERE DURABLE. Treating that as
/// success used to let a walk announce a barrier that a restart would forget
/// entirely, so no operator ever had an object to block or waive - which
/// silently defeats the operator-block path as well. The caller must refuse to
/// announce the barrier when this returns `Err`.
pub(crate) async fn record_barriers(
    writer: Option<&tokio::sync::mpsc::Sender<crate::multiplexer::WriterRequest>>,
    coverage: &InventoryCoverageReport,
    generation: u64,
    resume_from: Option<Checkpoint>,
) -> Result<(), Error> {
    let Some(writer) = writer else {
        return Ok(());
    };
    for incident in barrier_incidents(coverage, generation, resume_from.clone()) {
        let (done, recv) = tokio::sync::oneshot::channel();
        if writer
            .send(crate::multiplexer::WriterRequest::RecordBarrier { incident, done })
            .await
            .is_err()
        {
            return Ok(());
        }
        match recv.await {
            Ok(result) => result?,
            Err(_) => return Ok(()),
        }
    }
    Ok(())
}

/// Ask the account writer whether every barrier in `coverage` is waived now.
///
/// The writer both decides and persists the conversion to unresolved waived
/// debt before answering `true`. This deliberately happens at the barrier hit,
/// not from a walk-start snapshot: an operator decision racing an in-flight
/// walk is ordered against this request by the single writer.
pub(crate) async fn cross_waived_barriers(
    writer: Option<&tokio::sync::mpsc::Sender<crate::multiplexer::WriterRequest>>,
    coverage: &InventoryCoverageReport,
    generation: u64,
) -> Result<bool, Error> {
    let Some(writer) = writer else {
        return Ok(false);
    };
    let (done, recv) = tokio::sync::oneshot::channel();
    if writer
        .send(crate::multiplexer::WriterRequest::CrossWaivedBarriers {
            report: coverage.clone(),
            generation,
            done,
        })
        .await
        .is_err()
    {
        return Ok(false);
    }
    match recv.await {
        Ok(result) => result,
        Err(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, BackfillCheckpoint, BackfillProgress, Cause,
        CoverageDomain, InventoryCoverageReport, InventoryObligation, ObjectType, ObligationKey,
        RegionRecovery, RequestCause, RequestErrorKind,
    };

    fn checkpoint(position: u64) -> Checkpoint {
        Checkpoint::Backfill(BackfillCheckpoint {
            scope: bifrost_types::CursorScope::Type(ObjectType::Email),
            partition: bifrost_types::Partition(
                format!("page:{position}:{}", position + 10).into_bytes(),
            ),
            progress_marker: None,
            progress: BackfillProgress {
                items_done: position,
                items_estimated: None,
            },
            envelope_version: 1,
        })
    }

    fn barrier() -> InventoryCoverageReport {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: bifrost_types::DiagnosticText::support_only("barrier"),
            }),
        )
        .try_build()
        .expect("valid error");
        InventoryCoverageReport::degraded(
            CoverageDomain::full(bifrost_types::CursorScope::Type(ObjectType::Email)),
            vec![InventoryObligation::Region {
                key: ObligationKey(b"blocked-page".to_vec()),
                failure_label: "unidentifiable-value".into(),
                error,
                recovery: RegionRecovery::barrier(),
            }],
        )
    }

    #[test]
    fn a_refused_checkpoint_stops_at_the_previous_resume_position() {
        let mut walk = InventoryWalk::default();
        let accepted = checkpoint(40);
        walk.accept(Some(accepted.clone()));

        let WalkDecision::StopAtBarrier { resume_from } = walk.inspect(&barrier()) else {
            panic!("a barrier must stop the walk");
        };
        assert_eq!(resume_from, Some(accepted));

        let WalkDecision::StopAtBarrier { resume_from } = walk.inspect(&barrier()) else {
            panic!("the refused page must not become accepted");
        };
        assert_eq!(
            resume_from,
            Some(checkpoint(40)),
            "the next resume cannot leap to the barrier page"
        );
    }

    /// A barrier on the FIRST page has no earlier prefix to resume from. A
    /// resume position invented here would certify ground nothing walked.
    #[test]
    fn a_barrier_before_any_accepted_page_resumes_from_nothing() {
        let walk = InventoryWalk::default();
        let WalkDecision::StopAtBarrier { resume_from } = walk.inspect(&barrier()) else {
            panic!("a barrier must stop the walk");
        };
        assert_eq!(resume_from, None);
    }

    /// Both front ends run the same accept/inspect sequence, and the property
    /// that matters is identical for both: a clean page advances the resume
    /// position, a barrier freezes it. Backfill and fusion used to carry
    /// independent copies of this and their DIVERGENCE was the defect, so the
    /// sequence is pinned once, here, on the shared unit they both hold.
    #[test]
    fn a_clean_page_advances_the_resume_position_and_a_barrier_freezes_it() {
        let mut walk = InventoryWalk::default();
        for position in [10, 20, 30] {
            assert!(
                matches!(
                    walk.inspect(&InventoryCoverageReport::complete(CoverageDomain::full(
                        bifrost_types::CursorScope::Type(ObjectType::Email)
                    ))),
                    WalkDecision::Advance
                ),
                "a complete page must never stop the walk"
            );
            walk.accept(Some(checkpoint(position)));
        }
        let WalkDecision::StopAtBarrier { resume_from } = walk.inspect(&barrier()) else {
            panic!("a barrier must stop the walk whatever preceded it");
        };
        assert_eq!(resume_from, Some(checkpoint(30)));
    }

    /// A checkpoint-free page must not erase the resume position. A page that
    /// carries no checkpoint proves nothing about what a prior page certified,
    /// and clearing on it would make the whole walk restart from zero.
    #[test]
    fn a_page_without_a_checkpoint_does_not_erase_the_resume_position() {
        let mut walk = InventoryWalk::default();
        walk.accept(Some(checkpoint(10)));
        walk.accept(None);
        let WalkDecision::StopAtBarrier { resume_from } = walk.inspect(&barrier()) else {
            panic!("a barrier must stop the walk");
        };
        assert_eq!(resume_from, Some(checkpoint(10)));
    }

    #[test]
    fn inventory_removals_follow_the_scope_removal_contract() {
        use bifrost_types::{
            Change, CursorScope, FolderId, MembershipScope, ObjectChangeKind, ObjectId,
            ScopeChangeKind,
        };

        let removed = [ObjectId("gone".into())];
        let folder = removals(
            &CursorScope::Folder(FolderId("inbox".into())),
            removed.iter(),
        )
        .expect("folder removal projects");
        assert_eq!(folder.len(), 1);
        let Change::ScopeChange(scope_change) = &folder[0] else {
            panic!("folder removal must be a ScopeChange");
        };
        assert_eq!(scope_change.id, removed[0]);
        assert_eq!(
            scope_change.membership,
            MembershipScope::Folder(FolderId("inbox".into()))
        );
        assert_eq!(scope_change.kind, ScopeChangeKind::Removed);

        let account =
            removals(&CursorScope::Account, removed.iter()).expect("account removal projects");
        assert_eq!(account.len(), 1);
        let Change::ObjectChange(object_change) = &account[0] else {
            panic!("account removal must be an ObjectChange");
        };
        assert_eq!(object_change.id, removed[0]);
        assert_eq!(object_change.kind, ObjectChangeKind::Destroyed);
        let no_removals: [ObjectId; 0] = [];
        assert!(
            removals(&CursorScope::Account, no_removals.iter())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn unsupported_inventory_removal_scope_has_a_scoped_internal_error() {
        use bifrost_types::{
            AccountErrorKind, AccountOperation, CursorScope, ErrorScope, InternalErrorKind,
            ObjectType, RecoveryClass,
        };

        let scope = CursorScope::Type(ObjectType::Email);
        let error = crate::recovery::inventory_removal_scope_contract_failure(&scope);
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::AccountContract)
        );
        assert_eq!(error.operation(), Some(AccountOperation::SyncInventory));
        assert_eq!(error.scope(), Some(&ErrorScope::Cursor(scope)));
        assert_eq!(error.recovery(), &RecoveryClass::InternalFailure);
    }

    fn incident_writer(
        answer: Option<Result<(), Error>>,
    ) -> (
        tokio::sync::mpsc::Sender<crate::multiplexer::WriterRequest>,
        tokio::task::JoinHandle<usize>,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let worker = tokio::spawn(async move {
            let mut served = 0_usize;
            while let Some(request) = rx.recv().await {
                let crate::multiplexer::WriterRequest::RecordBarrier { done, .. } = request else {
                    continue;
                };
                served += 1;
                match &answer {
                    // Ran the request and failed it: the incident is NOWHERE
                    // durable.
                    Some(Err(_)) => {
                        let _ = done.send(Err(Error::Other("store write failed".into())));
                    }
                    Some(Ok(())) => {
                        let _ = done.send(Ok(()));
                    }
                    // The writer went away mid-request.
                    None => drop(done),
                }
            }
            served
        });
        (tx, worker)
    }

    /// The failure the round-3 code accepted as success: the writer ANSWERS,
    /// and its answer is an error. Treating that as a persisted barrier lets a
    /// walk announce an incident a restart forgets, leaving nothing durable for
    /// an operator to block or waive.
    #[tokio::test]
    async fn a_store_error_while_recording_a_barrier_is_not_success() {
        let (tx, worker) = incident_writer(Some(Err(Error::Other("boom".into()))));
        let result = record_barriers(Some(&tx), &barrier(), 7, None).await;
        assert!(
            result.is_err(),
            "a barrier the store refused must not be reported as recorded"
        );
        drop(tx);
        assert_eq!(worker.await.expect("writer task"), 1);
    }

    #[tokio::test]
    async fn a_recorded_barrier_reports_success() {
        let (tx, worker) = incident_writer(Some(Ok(())));
        record_barriers(Some(&tx), &barrier(), 7, Some(checkpoint(10)))
            .await
            .expect("the writer accepted the incident");
        drop(tx);
        assert_eq!(worker.await.expect("writer task"), 1);
    }

    /// The distinction that keeps the strictness honest: a writer that is GONE
    /// (detach, shutdown) is not a failed write. There is nothing to persist to
    /// and nothing to retry against, so the walk is not failed over it.
    #[tokio::test]
    async fn a_departed_writer_is_not_a_persistence_failure() {
        let (tx, worker) = incident_writer(None);
        record_barriers(Some(&tx), &barrier(), 7, None)
            .await
            .expect("a departed writer is not a store failure");
        drop(tx);
        worker.await.expect("writer task");

        // And a channel already closed before the send even lands.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        record_barriers(Some(&tx), &barrier(), 7, None)
            .await
            .expect("a closed writer channel is not a store failure");
    }

    #[tokio::test]
    async fn no_writer_at_all_records_nothing_and_succeeds() {
        record_barriers(None, &barrier(), 7, None)
            .await
            .expect("nothing to record against");
    }
}
