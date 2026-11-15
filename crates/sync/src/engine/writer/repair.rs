//! Writer operations: repair.

use super::*;

/// Repair proofs and consumer acknowledgements may arrive in either order.
/// Neither side alone authorizes discharging a recovered object's obligation.
#[derive(Default)]
pub(super) struct RepairPairing {
    pub(super) pending: HashMap<crate::cursor::AckToken, Vec<crate::repair::RepairResolution>>,
    pub(super) acknowledged: HashSet<crate::cursor::AckToken>,
}

impl Writer {
    pub(super) async fn apply_repair(
        &mut self,
        resolutions: Vec<crate::repair::RepairResolution>,
        publication: Option<crate::cursor::AckToken>,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");
        let pending_repairs = &mut self.repairs.pending;
        let acknowledged_publications = &mut self.repairs.acknowledged;

        // A repair discharges only once the CONSUMER has acknowledged
        // the batch of recovered ids. The acknowledgement can arrive
        // either side of this request, so the resolutions park here
        // until it does.
        let acknowledged = match publication {
            Some(publication) => {
                if acknowledged_publications.remove(&publication) {
                    true
                } else {
                    park_pending_repair(&account_id, pending_repairs, publication, resolutions);
                    let _ = done.send(Ok(()));
                    return;
                }
            }
            // Never published at all: nothing recovered may discharge.
            None => false,
        };
        let result =
            apply_repair_resolutions(&account_id, &store, ledger, resolutions, acknowledged).await;
        let _ = done.send(result);
    }
    pub(super) async fn acknowledge_publication(
        &mut self,
        publication: crate::cursor::AckToken,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let coverage = Arc::clone(&self.coverage);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");
        let pending_repairs = &mut self.repairs.pending;
        let acknowledged_publications = &mut self.repairs.acknowledged;

        // The REPAIR lane specifically: a checkpoint publication's id
        // resolves to `Unknown` here rather than being consumed, so a
        // consumer cannot discharge a checkpoint through this path and
        // leave the later `ack` short-circuiting as already
        // persisted over a store write that never happened.
        let result = match coverage.claim_repair(publication.clone()) {
            ClaimLookup::Apply(_) | ClaimLookup::Replay(_) => {
                if let Some(resolutions) = pending_repairs.remove(&publication) {
                    let outcome =
                        apply_repair_resolutions(&account_id, &store, ledger, resolutions, true)
                            .await;
                    if outcome.is_ok() {
                        coverage.settle_repair(publication.clone());
                    }
                    outcome.map_err(|error| Error::AckStore(Box::new(error)))
                } else {
                    acknowledged_publications.insert(publication.clone());
                    coverage.settle_repair(publication);
                    Ok(())
                }
            }
            // Idempotent: this publication was already applied.
            ClaimLookup::AlreadyPersisted => Ok(()),
            ClaimLookup::Unknown => Err(Error::AckRejected(AckRejection::UnknownPublication)),
        };
        let _ = done.send(result);
    }
}

/// Ceiling on repair passes parked waiting for a consumer acknowledgement.
///
/// A consumer that never acknowledges repair batches would otherwise
/// accumulate one entry per pass for the life of the attachment. Evicting the
/// oldest costs nothing durable: its obligations were never discharged, so they
/// are still owed and a later pass retries them.
const PARKED_REPAIR_CAP: usize = 64;

pub(super) fn park_pending_repair(
    account_id: &AccountId,
    parked: &mut HashMap<crate::cursor::AckToken, Vec<crate::repair::RepairResolution>>,
    publication: crate::cursor::AckToken,
    resolutions: Vec<crate::repair::RepairResolution>,
) {
    if parked.len() >= PARKED_REPAIR_CAP
        && let Some(oldest) = parked.keys().min().cloned()
    {
        parked.remove(&oldest);
        tracing::warn!(
            target: "bifrost.sync.repair",
            account = ?account_id,
            cap = PARKED_REPAIR_CAP,
            "repair resolutions awaiting acknowledgement at capacity; the oldest pass stays owed"
        );
    }
    parked.insert(publication, resolutions);
}

/// Fold one repair pass's outcomes into the ledger.
///
/// The ordering that matters: a `Recovered` resolution discharges ONLY when the
/// consumer has acknowledged the publication carrying its id. Discharging
/// before that recreates the original silent loss - the engine would forget the
/// obligation while the consumer never learned the object exists. If the
/// publication was never acknowledged (no subscriber, lag, cancellation) the
/// obligation simply stays open and a later pass tries again; a crash between
/// publish and acknowledgement costs a duplicate recovery, which is the same
/// at-least-once asymmetry ordinary checkpoint delivery already has.
pub(super) async fn apply_repair_resolutions(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    ledger: &mut crate::cursor::DebtLedger,
    resolutions: Vec<crate::repair::RepairResolution>,
    published_and_acknowledged: bool,
) -> Result<(), Error> {
    let now = jiff::Timestamp::now().as_second();
    // Keep the authoritative in-memory ledger aligned with durable state. A
    // failed write must not leave a discharge or attempt available to a later
    // unrelated writer request.
    let mut candidate = ledger.stage();

    for resolution in resolutions {
        match resolution {
            crate::repair::RepairResolution::Recovered {
                key,
                attempt,
                generation,
            } => {
                if !published_and_acknowledged {
                    // Recovered but never delivered. Costs an attempt and stays
                    // owed, which is the conservative direction.
                    candidate.record_attempt(&key, crate::repair::DEFAULT_REPAIR_BUDGET);
                    continue;
                }
                if !candidate.discharge_repaired(
                    &key,
                    generation,
                    crate::cursor::DischargeEvidence::RepairedAndPublished { attempt },
                ) {
                    tracing::debug!(
                        target: "bifrost.sync.repair",
                        account = ?account_id,
                        "repair result refused: the obligation moved on since it was read"
                    );
                }
            }
            crate::repair::RepairResolution::Irrelevant {
                key,
                detail,
                generation,
            } => {
                // Nothing was published and nothing needs to be: absence from
                // an old inventory snapshot is not a deletion to apply against
                // current consumer state.
                candidate.discharge_repaired(
                    &key,
                    generation,
                    crate::cursor::DischargeEvidence::ProvedIrrelevant { detail },
                );
            }
            crate::repair::RepairResolution::Replaced {
                key,
                proof,
                generation,
            } => {
                apply_replacement(&mut candidate, &key, &proof, generation, now);
            }
            crate::repair::RepairResolution::Deferred { key } => {
                // Charges `key`'s lineage only while `key` is still open. A
                // pass plans against a ledger snapshot, so a covering proof
                // acknowledged while it is out can discharge the obligation
                // first; that result then charges nothing, which cannot uncap
                // anything because a closed key is never planned again. See
                // `DebtLedger::record_attempt`.
                candidate.record_attempt(&key, crate::repair::DEFAULT_REPAIR_BUDGET);
            }
        }
    }

    persist_ledger_only(account_id, store, &candidate).await?;
    candidate.commit(ledger);
    Ok(())
}

/// Apply a region proof, which either discharges the region or splits it.
fn apply_replacement(
    ledger: &mut crate::cursor::DebtLedger,
    key: &bifrost_types::ObligationKey,
    proof: &bifrost_types::RegionRepairProof,
    generation: crate::cursor::WalkGeneration,
    now: i64,
) {
    match proof {
        // The account consumed the exact region its own durable token named and
        // accounted for every result. Identity is what is checked here, not set
        // inclusion - the lattice cannot do algebra over an opaque token.
        bifrost_types::RegionRepairProof::ExactReplay => {
            ledger.discharge_repaired(
                key,
                generation,
                crate::cursor::DischargeEvidence::ProvedIrrelevant {
                    detail: "exact replay of the named region accounted for every result".into(),
                },
            );
        }
        // Expressible in the lattice, so the engine checks it rather than
        // taking the account's word.
        bifrost_types::RegionRepairProof::CoveredBy { domain } => {
            let covers = ledger
                .entry(key)
                .is_some_and(|entry| domain.covers(&entry.domain));
            if covers {
                ledger.discharge_repaired(
                    key,
                    generation,
                    crate::cursor::DischargeEvidence::CoveringWalk {
                        domain: domain.clone(),
                    },
                );
            }
        }
        bifrost_types::RegionRepairProof::Partitioned {
            proved, residual, ..
        } => {
            // Resolved BEFORE the swap: an accepted replacement discharges
            // `key`, and a discharged key is never charged, so resolving after
            // would silently drop the one charge that caps a key-rotating loop.
            let lineage = ledger.open_lineage(key);
            match ledger.replace_obligation(key, proved, residual, generation, now) {
                Ok(crate::cursor::ReplacementProgress::Stalled) => {
                    // Reshaped without improving anything. Charged against the
                    // lineage so an account cannot loop forever by rotating
                    // keys and tokens over the same unresolved extent.
                    if let Some(lineage) = lineage {
                        ledger.charge_lineage(&lineage, crate::repair::DEFAULT_REPAIR_BUDGET);
                    }
                }
                Ok(crate::cursor::ReplacementProgress::Progressed) => {}
                // An operator decision (or a spent budget) landed while this
                // result was in flight. The refusal preserved it, and charging
                // would bill the lineage for work nobody asked to continue.
                Err(crate::cursor::ReplacementRefusal::PolicyNoLongerRetrying) => {
                    tracing::debug!(
                        target: "bifrost.sync.repair",
                        "region replacement refused: the obligation is no longer retrying"
                    );
                }
                Err(refusal) => {
                    tracing::warn!(
                        target: "bifrost.sync.repair",
                        refusal = ?refusal,
                        "refusing a region replacement"
                    );
                    // A refusal changes nothing, so `key` is exactly as open as
                    // it was: a malformed or colliding split against an open
                    // obligation charges its lineage (repeating it is a loop),
                    // and one against a closed or unknown key charges nothing.
                    ledger.record_attempt(key, crate::repair::DEFAULT_REPAIR_BUDGET);
                }
            }
        }
        _ => {}
    }
}
