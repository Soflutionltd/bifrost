//! The repair pass: working inventory coverage debt off.
//!
//! Reads the ledger back, asks the account to re-read what it could not
//! represent, publishes what it recovered, and discharges only what the
//! consumer durably acknowledged.
//!
//! # Why this needs no exclusion against the live change stream
//!
//! It looks like it should. A repaired object is read at one moment and applied
//! at another, so a live deletion in between could be overwritten - the classic
//! resurrection. The reason it cannot happen here is a property of the existing
//! inventory contract rather than anything this module does:
//!
//! **No inventory path delivers object state to a consumer.** `InventoryFusion`
//! and `BackfillRunner` both take the account's `InventoryEntry`, keep the id,
//! discard the entry, and publish `ObjectChange::Created`. The consumer
//! hydrates from the id.
//!
//! Repair mirrors that exactly. A repair publication carries ids and nothing
//! else, so there is no payload that could overwrite a newer representation and
//! nothing to insert after a tombstone. A stale repair `Created` is resolved
//! the same way a stale backfill `Created` already is: hydration returns
//! not-found and the consumer stores nothing. That is why there is no
//! conditional application here, no tombstone requirement, no version-relation
//! hook, and no lease held against polling.
//!
//! The account still returns the rebuilt entry across the ACCOUNT boundary,
//! because constructing it is the proof that the representation failure which
//! raised the obligation has healed - an id alone would only prove the object
//! still exists. The engine validates it, keeps the id, and drops the rest.
//!
//! # What discharge means
//!
//! An obligation says the walk failed to tell the consumer an object exists and
//! the cursor then advanced past it, so the changes stream will never mention
//! it again. A durably acknowledged `Created` is that telling, and it is the
//! most a successful walk ever achieves for any object. Demanding proof of
//! successful hydration would hold repair to a bar the non-degraded path does
//! not meet, and would fuse two failure domains: coverage asks whether
//! enumeration announced the object, hydration asks whether a projection can be
//! fetched right now. A later hydration failure is the hydration lane's, with
//! its own `ItemOutcome`.
//!
//! # Known limit: the REGION lane has no live provider
//!
//! The OBJECT lane is implemented end to end and Google implements it. The
//! REGION lane - `InventoryRepairTarget::Region`, `RegionRecovered`, and every
//! `RegionRepairProof` shape (`ExactReplay`, `Partitioned`, conservation across
//! children, lineage splitting) - is built and tested, but no account crate
//! currently produces it: Graph's only region is a `CheckpointBarrier` by
//! construction. So those paths are exercised by in-crate tests only.
//!
//! That is not a defect; the shapes are unreachable rather than wrong. It does
//! mean the region contract is validated against tests rather than against a
//! provider, so THE FIRST REAL IMPLEMENTOR MUST BE REVIEWED AGAINST THE
//! CONTRACT rather than assumed to fit it - the tests encode this crate's
//! reading of the contract, and nothing has yet forced that reading to survive
//! contact with a real enumeration. A viable candidate needs a replay token
//! that survives its own enumeration, which is exactly what Graph lacks.
//! See `reference/sync.md`, "Inventory coverage" / "Repair", for the contract
//! itself.

use std::collections::HashMap;
use std::sync::Arc;

use crate::cursor::DebtLedger;
use crate::delivery::{ChangeDelivery, EventScope, MultiplexerEvent};
use crate::error::Error;
use bifrost_types::{
    Account, AccountId, Change, CursorScope, InventoryEntry, InventoryRepairEvent,
    InventoryRepairOutcome, InventoryRepairRequest, InventoryRepairTarget, ObjectChange,
    ObjectChangeKind, ObligationKey, RegionRepairProof, RepairAttemptId, SyncEvent,
};
use futures::stream::StreamExt;

/// How many completed attempts a lineage gets before it stops being retried
/// automatically.
///
/// Expiry yields `OperatorBlocked`, never abandonment. Attempts accrue per
/// lineage so re-minting an equivalent obligation cannot reset it.
pub const DEFAULT_REPAIR_BUDGET: u32 = 5;

/// One resolved repair, ready for the writer.
#[derive(Debug, Clone)]
pub enum RepairResolution {
    /// Recovered and published. Discharge once the consumer acknowledges.
    Recovered {
        key: ObligationKey,
        attempt: RepairAttemptId,
        generation: crate::cursor::WalkGeneration,
    },
    /// Proved not owed. Nothing was published, so nothing is awaited.
    Irrelevant {
        key: ObligationKey,
        detail: String,
        generation: crate::cursor::WalkGeneration,
    },
    /// Split into children. Atomic parent-to-children swap.
    Replaced {
        key: ObligationKey,
        proof: Box<RegionRepairProof>,
        generation: crate::cursor::WalkGeneration,
    },
    /// This attempt did not resolve it. Costs one attempt against the lineage.
    Deferred { key: ObligationKey },
}

/// Drive one repair pass for `account`.
///
/// Returns the number of non-deferred outcomes submitted to the writer.
/// Recovered publications still await consumer acknowledgement; stale proofs
/// can be refused by the writer. Deferred and unanswered attempts do not count.
///
/// Errors from the account are per-request wherever the account correlates
/// them. A stream that ends with requests outstanding does NOT mean the account
/// classified them: those become local deferrals, because recording a
/// conclusion nobody reached is exactly the kind of invented certainty the
/// coverage model exists to prevent.
pub async fn run_repair_pass(
    account: &dyn Account,
    account_id: &AccountId,
    ledger: &DebtLedger,
    delivery: Option<&ChangeDelivery>,
    writer_tx: &crate::engine::writer::WriterHandle,
    coverage: &Arc<crate::cursor::PendingCoverage>,
    max_requests: usize,
) -> Result<usize, Error> {
    run_repair_pass_observed(
        account,
        account_id,
        ledger,
        delivery,
        writer_tx,
        coverage,
        max_requests,
        None,
    )
    .await
}

/// Engine-managed repairs observe every provider hold before the next pass.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_repair_pass_observed(
    account: &dyn Account,
    account_id: &AccountId,
    ledger: &DebtLedger,
    delivery: Option<&ChangeDelivery>,
    writer_tx: &crate::engine::writer::WriterHandle,
    coverage: &Arc<crate::cursor::PendingCoverage>,
    max_requests: usize,
    observe: Option<&(dyn Fn(&bifrost_types::AccountError) + Sync)>,
) -> Result<usize, Error> {
    let planned = plan_requests(ledger, max_requests);
    if planned.is_empty() {
        return Ok(0);
    }

    let mut outstanding: HashMap<RepairAttemptId, PlannedRequest> = planned
        .iter()
        .map(|planned| (planned.request.attempt, planned.clone()))
        .collect();
    let requests: Vec<InventoryRepairRequest> = planned.iter().map(|p| p.request.clone()).collect();

    let mut stream = account.repair_inventory(Box::pin(futures::stream::iter(requests)));
    let mut resolutions: Vec<(CursorScope, RepairResolution)> = Vec::new();
    let mut recovered_ids: Vec<(CursorScope, bifrost_types::ObjectId)> = Vec::new();

    while let Some(event) = stream.next().await {
        let outcome = match event {
            InventoryRepairEvent::Outcome(outcome) => outcome,
            // Explains why the stream stopped. It does not answer for the
            // requests still outstanding - those fall through to the local
            // deferral below.
            InventoryRepairEvent::Terminated(error) => {
                if let Some(observe) = observe {
                    observe(&error);
                }
                tracing::warn!(
                    target: "bifrost.sync.repair",
                    account = ?account_id,
                    error = %error,
                    "repair stream terminated with attempts outstanding"
                );
                break;
            }
            _ => continue,
        };

        let attempt = outcome.attempt();
        // An unknown attempt, or a second outcome for one already answered, is
        // a contract violation and is dropped rather than applied to whatever
        // obligation happens to share the key.
        let Some(planned) = outstanding.remove(&attempt) else {
            tracing::warn!(
                target: "bifrost.sync.repair",
                account = ?account_id,
                attempt = attempt.0,
                "repair outcome for an unknown or already-answered attempt"
            );
            continue;
        };

        if let InventoryRepairOutcome::Deferred { error, .. } = &outcome
            && let Some(observe) = observe
        {
            observe(error);
        }
        match outcome {
            InventoryRepairOutcome::ObjectRecovered { entry, .. } => {
                match validate_object_recovery(&planned, &entry) {
                    Ok(id) => {
                        recovered_ids.push((planned.scope.clone(), id));
                        resolutions.push((
                            planned.scope.clone(),
                            RepairResolution::Recovered {
                                key: planned.request.key.clone(),
                                attempt,
                                generation: planned.generation,
                            },
                        ));
                    }
                    Err(reason) => {
                        tracing::warn!(
                            target: "bifrost.sync.repair",
                            account = ?account_id,
                            attempt = attempt.0,
                            reason,
                            "rejecting an object recovery that does not match its request"
                        );
                        resolutions.push((
                            planned.scope.clone(),
                            RepairResolution::Deferred {
                                key: planned.request.key.clone(),
                            },
                        ));
                    }
                }
            }
            InventoryRepairOutcome::RegionRecovered { entries, proof, .. } => {
                if !matches!(planned.request.target, InventoryRepairTarget::Region { .. }) {
                    tracing::warn!(
                        target: "bifrost.sync.repair",
                        account = ?account_id,
                        attempt = attempt.0,
                        "region recovery returned for an object request"
                    );
                    resolutions.push((
                        planned.scope.clone(),
                        RepairResolution::Deferred {
                            key: planned.request.key.clone(),
                        },
                    ));
                    continue;
                }
                for entry in &entries {
                    recovered_ids.push((planned.scope.clone(), entry.id.clone()));
                }
                // Some entries never discharge a region; the completeness proof
                // is what does, and the writer checks it against the
                // obligation's own domain.
                resolutions.push((
                    planned.scope.clone(),
                    RepairResolution::Replaced {
                        key: planned.request.key.clone(),
                        proof: Box::new(proof),
                        generation: planned.generation,
                    },
                ));
            }
            InventoryRepairOutcome::DefinitivelyIrrelevant { evidence, .. } => {
                let detail = match &evidence {
                    bifrost_types::DefinitiveIrrelevance::AbsentUnderCursorBridge { detail }
                    | bifrost_types::DefinitiveIrrelevance::OutOfScope { detail } => detail.clone(),
                    _ => String::new(),
                };
                resolutions.push((
                    planned.scope.clone(),
                    RepairResolution::Irrelevant {
                        key: planned.request.key.clone(),
                        detail,
                        generation: planned.generation,
                    },
                ));
            }
            InventoryRepairOutcome::Deferred { error, .. } => {
                tracing::debug!(
                    target: "bifrost.sync.repair",
                    account = ?account_id,
                    attempt = attempt.0,
                    error = %error,
                    "repair attempt deferred"
                );
                resolutions.push((
                    planned.scope.clone(),
                    RepairResolution::Deferred {
                        key: planned.request.key.clone(),
                    },
                ));
            }
            InventoryRepairOutcome::Replaced { proof, .. } => {
                resolutions.push((
                    planned.scope.clone(),
                    RepairResolution::Replaced {
                        key: planned.request.key.clone(),
                        proof: Box::new(proof),
                        generation: planned.generation,
                    },
                ));
            }
            _ => {
                resolutions.push((
                    planned.scope.clone(),
                    RepairResolution::Deferred {
                        key: planned.request.key.clone(),
                    },
                ));
            }
        }
    }

    // Requests the account never answered. A LOCAL deferral: the engine records
    // that an attempt happened and produced nothing, without claiming the
    // account reached any conclusion about it.
    for planned in outstanding.into_values() {
        resolutions.push((
            planned.scope.clone(),
            RepairResolution::Deferred {
                key: planned.request.key.clone(),
            },
        ));
    }

    // A deferred attempt leaves the obligation owed. Reporting it as progress
    // would tell an operator that incomplete mail had been repaired.
    let resolved = resolutions
        .iter()
        .filter(|(_, resolution)| !matches!(resolution, RepairResolution::Deferred { .. }))
        .count();

    // A pass plans across every repairable obligation, which can span scopes,
    // so the recovered ids are grouped by the scope they came from and each
    // group is published as its own event. `MultiplexerEvent::scope` is how a
    // broadcast consumer routes (or filters) an event, so a single batch filed
    // under whichever scope happened to be recovered first delivers scope B's
    // ids as scope A's - or drops them.
    //
    // Grouping the RESOLUTIONS the same way is what keeps discharge honest
    // after the split: an obligation may only discharge on an acknowledgement
    // of the batch that actually carried its id, never on a sibling scope's
    // batch the consumer happened to apply first. Within a scope the pass is
    // still all-or-nothing, exactly as it was.
    let mut order: Vec<CursorScope> = Vec::new();
    let mut grouped: HashMap<CursorScope, Vec<RepairResolution>> = HashMap::new();
    for (scope, resolution) in resolutions {
        let entry = grouped.entry(scope.clone()).or_insert_with(|| {
            order.push(scope.clone());
            Vec::new()
        });
        entry.push(resolution);
    }
    let mut ids_by_scope: HashMap<CursorScope, Vec<bifrost_types::ObjectId>> = HashMap::new();
    for (scope, id) in recovered_ids {
        ids_by_scope.entry(scope).or_default().push(id);
    }

    for scope in order {
        let resolutions = grouped.remove(&scope).unwrap_or_default();
        let ids = ids_by_scope.remove(&scope).unwrap_or_default();
        let publication = publish_recovered(delivery, coverage, &scope, &ids);
        writer_tx.apply_repair(resolutions, publication).await?;
    }
    Ok(resolved)
}

#[derive(Debug, Clone)]
struct PlannedRequest {
    request: InventoryRepairRequest,
    scope: CursorScope,
    generation: crate::cursor::WalkGeneration,
}

// Repair correlation is independent of proof order. A repair can issue many
// requests against one walk; consuming walk generations here conflates owners.
// Restarting at 0 per process is safe: an id is paired only within the pass
// that minted it (`outstanding`), and the ledger keeps it as audit detail that
// neither the discharge fingerprint nor any reader compares. A durable reader
// keyed on it would need an instance segment, like publication ids.
fn next_repair_attempt() -> RepairAttemptId {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    RepairAttemptId(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
}

fn plan_requests(ledger: &DebtLedger, max_requests: usize) -> Vec<PlannedRequest> {
    ledger
        .repairable()
        .take(max_requests)
        .filter_map(|entry| {
            let target = entry.target.clone()?;
            Some(PlannedRequest {
                request: InventoryRepairRequest {
                    attempt: next_repair_attempt(),
                    key: entry.key.clone(),
                    domain: entry.domain.clone(),
                    target,
                },
                scope: entry.domain.scope.clone(),
                generation: entry.generation,
            })
        })
        .collect()
}

/// An object recovery must answer the request it was given.
fn validate_object_recovery(
    planned: &PlannedRequest,
    entry: &InventoryEntry,
) -> Result<bifrost_types::ObjectId, &'static str> {
    let InventoryRepairTarget::Object { id, .. } = &planned.request.target else {
        return Err("object recovery returned for a region request");
    };
    if entry.id != *id {
        return Err("recovered entry names a different object than the obligation");
    }
    Ok(entry.id.clone())
}

/// Publish recovered ids as ordinary `Created` signals.
///
/// Ids only, exactly as a successful inventory walk publishes them. Nothing
/// here carries object state, which is what keeps repair free of the
/// version-ordering problem it superficially resembles.
///
/// One publication per SCOPE, so discharge is all-or-nothing within the scope:
/// partial consumer application would otherwise force the writer to manufacture
/// residual obligations for whichever ids did not land. Ids from another scope
/// ride their own event and their own publication - an event carries exactly
/// the ids its `scope` field names.
fn publish_recovered(
    delivery: Option<&ChangeDelivery>,
    coverage: &Arc<crate::cursor::PendingCoverage>,
    scope: &CursorScope,
    recovered: &[bifrost_types::ObjectId],
) -> Option<crate::cursor::AckToken> {
    let delivery = delivery?;
    if recovered.is_empty() {
        return None;
    }
    let scope = scope.clone();
    let changes: Vec<Change> = recovered
        .iter()
        .map(|id| {
            Change::ObjectChange(ObjectChange {
                id: id.clone(),
                kind: ObjectChangeKind::Created,
            })
        })
        .collect();
    // Carries no coverage report: a repair proves things about specific
    // obligations, not about a region of an enumeration, so it must not touch
    // the coverage lattice.
    let publication =
        coverage.publish_without_report(crate::cursor::PendingCoverage::initial_generation());
    let batch = bifrost_types::Batch {
        items: changes,
        page_boundary: bifrost_types::PageBoundary::Final,
        server_latency: std::time::Duration::ZERO,
        bytes_in: 0,
        checkpoint: None,
    };
    let event = MultiplexerEvent {
        scope: EventScope::Cursor(scope),
        event: Arc::new(SyncEvent::Batch(batch)),
        checkpoint: None,
        publication: Some(publication.clone()),
    };
    if delivery.publish_acknowledgeable(event) {
        Some(publication)
    } else {
        // No numbered receiver can acknowledge it, so nothing may be
        // discharged on it.
        coverage.retire(publication);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{PlannedRequest, RepairResolution, validate_object_recovery};
    use bifrost_types::{
        CoverageDomain, CursorScope, Fingerprint, InventoryEntry, InventoryRepairRequest,
        InventoryRepairTarget, ObjectId, ObjectType, ObligationKey, RepairAttemptId, ServerVersion,
    };

    fn scope() -> CursorScope {
        CursorScope::Type(ObjectType::Email)
    }

    fn planned(target: InventoryRepairTarget) -> PlannedRequest {
        PlannedRequest {
            request: InventoryRepairRequest {
                attempt: RepairAttemptId(1),
                key: ObligationKey(b"k".to_vec()),
                domain: CoverageDomain::full(scope()),
                target,
            },
            scope: scope(),
            generation: crate::cursor::WalkGeneration::for_test(1),
        }
    }

    fn entry(id: &str) -> InventoryEntry {
        InventoryEntry {
            id: ObjectId(id.into()),
            memberships: Vec::new(),
            blob_id: None,
            fingerprint: Fingerprint {
                server_version: ServerVersion::Unavailable,
                size: None,
                flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
            },
            thread_id: None,
            message_id: None,
            references: Vec::new(),
            in_reply_to: None,
        }
    }

    /// A recovery that names a DIFFERENT object than the one asked about must
    /// not discharge the obligation. Publishing its id would announce an object
    /// nobody owed while the owed one stayed missing, and the ledger would
    /// record the gap as closed.
    #[test]
    fn a_recovery_naming_another_object_is_rejected() {
        let planned = planned(InventoryRepairTarget::Object {
            id: ObjectId("wanted".into()),
            repair: Vec::new(),
        });
        assert!(validate_object_recovery(&planned, &entry("something-else")).is_err());
        assert!(validate_object_recovery(&planned, &entry("wanted")).is_ok());
    }

    /// An object outcome answering a region request is a kind mismatch. The
    /// engine must not treat it as having accounted for the region, which has
    /// completeness requirements a single object cannot meet.
    #[test]
    fn an_object_recovery_for_a_region_request_is_rejected() {
        let planned = planned(InventoryRepairTarget::Region {
            replay: b"tok".to_vec(),
        });
        assert!(validate_object_recovery(&planned, &entry("anything")).is_err());
    }

    #[test]
    fn a_deferred_resolution_names_its_obligation() {
        let resolution = RepairResolution::Deferred {
            key: ObligationKey(b"k".to_vec()),
        };
        let RepairResolution::Deferred { key } = resolution else {
            panic!("shape");
        };
        assert_eq!(key.0, b"k".to_vec());
    }
}
