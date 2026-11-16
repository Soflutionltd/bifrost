//! Provider-native repair of inventory coverage debt.
//!
//! An `InventoryObligation` records something an enumeration could not account
//! for. Repair is the pass that tries to account for it later, and it is
//! deliberately NOT `get_stream`: that hydrates known ids at a projection and
//! cannot express region replay, completeness proof, or authoritative absence.
//! Only the protocol crate knows what a provider-native re-read needs.
//!
//! # What crosses which boundary
//!
//! A successful inventory walk does NOT deliver `InventoryEntry` values to a
//! consumer. The engine takes each entry, keeps the id, discards the rest, and
//! publishes `ObjectChange::Created`; the consumer hydrates from there. Repair
//! mirrors that exactly:
//!
//! ```text
//! account  ->  ObjectRecovered { entry }
//! engine   ->  validate, keep the id, retain the evidence, discard the entry
//! consumer ->  ObjectChange::Created(id)
//! ```
//!
//! The entry still crosses the ACCOUNT boundary, because constructing it is the
//! proof that the representation failure which raised the obligation has
//! actually healed - an id alone would only prove the object still exists. It
//! does not cross the CONSUMER boundary, because no inventory path does.
//!
//! That is what makes repair free of the version race it looks like it should
//! have. A repair signal carries no object state, so it cannot overwrite a
//! newer representation or resurrect a deleted object: a stale `Created` is
//! resolved by hydration returning not-found, exactly as it already is for a
//! backfill page that races a live deletion. Repair needs no conditional
//! application, no tombstones, no version-relation hook, and no exclusive lease
//! against the live change stream.
//!
//! # The discharge bar
//!
//! An obligation says: the walk failed to tell the consumer this object exists,
//! and the cursor then advanced past it, so the changes stream will never
//! mention it again. A durably acknowledged `Created` signal IS that telling,
//! and it is the most any successful walk ever achieves for any object.
//! Requiring repair to prove successful hydration would hold it to a standard
//! the non-degraded path does not meet and fuse two failure domains: inventory
//! coverage asks whether enumeration announced the object, hydration asks
//! whether a projection can currently be fetched. A later hydration failure
//! belongs to the hydration lane and its `ItemOutcome`.

use crate::coverage::{CoverageDomain, InventoryObligation, ObligationKey};
use crate::error::AccountError;
use crate::events::InventoryEntry;
use crate::ids::ObjectId;

/// Engine-issued identity for one repair execution.
///
/// The obligation key identifies durable debt; it does not identify one
/// attempt. An attempt may be retried after a stream terminates, a stale event
/// may arrive from an abandoned stream, and the same key may be reopened at a
/// newer generation - so correlating on the key alone risks applying an old
/// result to a newly reopened instance of the same obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepairAttemptId(pub u64);

/// What a repair request addresses.
///
/// Built from the account-minted material on the original obligation. The
/// engine persists and returns these bytes without interpreting them.
///
/// There is no barrier variant: a `CheckpointBarrier` region never becomes
/// ledger debt, because no checkpoint advanced past it. Its escape is an
/// operator waiver, not repair.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InventoryRepairTarget {
    Object { id: ObjectId, repair: Vec<u8> },
    Region { replay: Vec<u8> },
}

impl InventoryRepairTarget {
    /// The repair target an obligation implies, or `None` for a barrier.
    #[must_use]
    pub fn from_obligation(obligation: &InventoryObligation) -> Option<Self> {
        match obligation {
            InventoryObligation::Object { id, repair, .. } => Some(Self::Object {
                id: id.clone(),
                repair: repair.clone(),
            }),
            InventoryObligation::Region { recovery, .. } => match recovery {
                crate::coverage::RegionRecovery::DurableReplay { token } => Some(Self::Region {
                    replay: token.clone(),
                }),
                crate::coverage::RegionRecovery::CheckpointBarrier { .. } => None,
            },
        }
    }
}

/// One thing the engine asks an account to repair.
#[derive(Debug, Clone)]
pub struct InventoryRepairRequest {
    pub attempt: RepairAttemptId,
    pub key: ObligationKey,
    /// What the ENGINE believes this obligation covers.
    ///
    /// Supplied even though the account did not mint all of it: it routes
    /// scope-specific clients, states the extent a region proof will be checked
    /// against, and stops an opaque token being replayed under a different
    /// scope after a topology change. The account must validate that the domain
    /// and its own token agree. Supplying the domain is not interpreting the
    /// token.
    pub domain: CoverageDomain,
    pub target: InventoryRepairTarget,
}

/// Why an account considers an object definitively absent from the inventory.
///
/// A status code is not evidence. A 404 may mean the object was deleted, moved
/// out of the addressed scope, lost permission, been routed to the wrong
/// mailbox or tenant, or simply not replicated yet. So the account states the
/// conclusion and the authority it rests on, and the reasoning stays reviewable
/// instead of hiding inside an error mapping.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DefinitiveIrrelevance {
    /// The object is absent from provider state read authoritatively for this
    /// scope, AND the account's change stream bridges from before the walk that
    /// raised the obligation - so its removal is either already reported or
    /// will be. Absence alone is not enough; the bridge is what makes it safe.
    AbsentUnderCursorBridge { detail: String },
    /// The object exists but is not a member of this inventory's scope, so the
    /// enumeration never owed an entry for it.
    OutOfScope { detail: String },
}

/// Proof that a region repair accounted for the whole region.
///
/// Producing some entries never discharges a region. The generic
/// `CoverageDomain` lattice can verify inclusion where the extent is
/// expressible in it, but a replay token may name a non-contiguous object set,
/// a provider query predicate, a server-side shard, or a response segment
/// defined by opaque continuation state - none of which the lattice can do set
/// algebra over. So exact replay and provider-asserted partition are separate
/// proofs rather than being forced through domain comparison.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RegionRepairProof {
    /// The account consumed the exact region named by the original durable
    /// token and accounted for every result in it. The engine verifies attempt
    /// and obligation identity, not set inclusion.
    ExactReplay,
    /// The region is contained in a domain the account enumerated completely.
    /// Checked with `CoverageDomain::covers`.
    CoveredBy { domain: CoverageDomain },
    /// The region was split. `proved` is what is now accounted for, `residual`
    /// is what remains owed.
    ///
    /// The engine enforces conservation - no part of the parent may vanish
    /// merely because no child names it - but where the extent is opaque it
    /// cannot verify the union itself, so the account asserts the partition
    /// explicitly rather than the engine pretending byte equality proved it.
    Partitioned {
        proved: Vec<CoverageDomain>,
        residual: Vec<InventoryObligation>,
        authority: String,
    },
}

/// One terminal result for one repair attempt.
///
/// Exactly one per accepted request. The engine rejects two outcomes for one
/// attempt, an unknown attempt, and an outcome whose kind does not match its
/// request. A stream that ends with requests outstanding does not mean the
/// account classified them: the engine converts those to a LOCAL deferral
/// rather than recording a conclusion nobody reached.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InventoryRepairOutcome {
    /// The object was re-read authoritatively and represented successfully.
    /// The entry proves the representation failure healed; only its id reaches
    /// the consumer.
    ObjectRecovered {
        attempt: RepairAttemptId,
        entry: Box<InventoryEntry>,
    },
    /// The region was replayed and accounted for. Zero entries with a complete
    /// proof is valid: it means the region held nothing relevant.
    RegionRecovered {
        attempt: RepairAttemptId,
        entries: Vec<InventoryEntry>,
        proof: RegionRepairProof,
    },
    /// The object is genuinely not owed. Discharges the obligation and emits
    /// NOTHING to the consumer - absence from an old inventory snapshot is not
    /// a deletion to apply against current consumer state.
    DefinitivelyIrrelevant {
        attempt: RepairAttemptId,
        evidence: DefinitiveIrrelevance,
    },
    /// This attempt did not resolve it. Costs one attempt against the lineage
    /// budget; does not change what is known.
    Deferred {
        attempt: RepairAttemptId,
        error: AccountError,
    },
    /// The obligation is really several smaller ones - a replayed region
    /// exposing individually-broken objects, say. An ATOMIC parent-to-children
    /// swap, never an append: appending would double-count the extent and
    /// replay the parent forever.
    Replaced {
        attempt: RepairAttemptId,
        proof: RegionRepairProof,
    },
}

impl InventoryRepairOutcome {
    #[must_use]
    pub fn attempt(&self) -> RepairAttemptId {
        match self {
            Self::ObjectRecovered { attempt, .. }
            | Self::RegionRecovered { attempt, .. }
            | Self::DefinitivelyIrrelevant { attempt, .. }
            | Self::Deferred { attempt, .. }
            | Self::Replaced { attempt, .. } => *attempt,
        }
    }
}

/// Repair's stream envelope.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InventoryRepairEvent {
    Outcome(InventoryRepairOutcome),
    /// The repair pass cannot continue at all. Explains why the stream stopped;
    /// does NOT stand in for the missing per-request outcomes.
    Terminated(AccountError),
}

#[cfg(test)]
mod tests {
    use super::{InventoryRepairTarget, RepairAttemptId};
    use crate::coverage::{InventoryObligation, ObligationKey, RegionRecovery};
    use crate::error::{AccountErrorBuilder, DiagnosticText, PrimaryCause, RequestCause};
    use crate::ids::ObjectId;

    fn error() -> crate::error::AccountError {
        AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("unrepresentable"),
        }))
        .try_build()
        .expect("valid account error classification")
    }

    #[test]
    fn an_object_obligation_yields_an_object_target() {
        let obligation = InventoryObligation::Object {
            key: ObligationKey(b"k".to_vec()),
            id: ObjectId("m1".into()),
            error: error(),
            repair: b"tok".to_vec(),
        };
        assert_eq!(
            InventoryRepairTarget::from_obligation(&obligation),
            Some(InventoryRepairTarget::Object {
                id: ObjectId("m1".into()),
                repair: b"tok".to_vec(),
            })
        );
    }

    /// A barrier has no repair path by construction: no checkpoint advanced
    /// past it, so it is blocked progress rather than debt, and its only
    /// terminal state is an operator waiver.
    #[test]
    fn a_barrier_region_has_no_repair_target() {
        let obligation = InventoryObligation::Region {
            key: ObligationKey(b"k".to_vec()),
            failure_label: "unidentifiable".into(),
            error: error(),
            recovery: RegionRecovery::barrier(),
        };
        assert!(InventoryRepairTarget::from_obligation(&obligation).is_none());
    }

    #[test]
    fn a_durably_replayable_region_yields_a_region_target() {
        let obligation = InventoryObligation::Region {
            key: ObligationKey(b"k".to_vec()),
            failure_label: "truncated".into(),
            error: error(),
            recovery: RegionRecovery::DurableReplay {
                token: b"replay".to_vec(),
            },
        };
        assert_eq!(
            InventoryRepairTarget::from_obligation(&obligation),
            Some(InventoryRepairTarget::Region {
                replay: b"replay".to_vec(),
            })
        );
    }

    #[test]
    fn attempt_ids_order() {
        assert!(RepairAttemptId(1) < RepairAttemptId(2));
    }
}
