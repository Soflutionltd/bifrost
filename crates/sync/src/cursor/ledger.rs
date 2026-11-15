//! The durable debt ledger: accumulated coverage lifecycle state.
//!
//! Distinct from `InventoryCoverageReport`, and the distinction is the point.
//! A report is one account's observation about one enumeration of one region.
//! The ledger is what the ENGINE has concluded after folding every accepted
//! report, every operator decision and (eventually) every repair result
//! together. Calling both "coverage" hid a seam where the interesting mistakes
//! live: an account reports evidence, the engine owns retry and
//! acceptable-loss policy, and no protocol crate may construct ledger state.
//!
//! Two axes, deliberately independent:
//!
//! - PROOF is what the system knows: `Unresolved` or `Discharged`.
//! - POLICY is what the system will do about it: `Retrying`, `OperatorBlocked`
//!   or `Waived`.
//!
//! Collapsing them is the mistake this module exists to prevent. A waiver is
//! accepted loss, NOT proof that the object was materialized or proved
//! irrelevant, so a waived obligation must never make a record claim complete
//! coverage - otherwise an operator (or a later full walk) cannot tell proved
//! coverage from loss somebody agreed to live with. Waiver may stop the
//! obligation blocking automatic backfill completion; it may not rewrite
//! history.
//!
//! The other rule with teeth: no local counter may produce `Waived` or
//! `Discharged`. A retry budget expiring is evidence that automatic work is not
//! helping, which is `OperatorBlocked` - the obligation stays visible and
//! manually retryable. Only an operator waives, because only an operator can
//! decide what loss is acceptable.
//!
//! The two axes also draw the compaction line: only `Discharged` entries fold
//! into [`DischargeAudit`], because only `Discharged` is terminal. Every
//! `Unresolved` entry stays a live entry, WAIVED ONES INCLUDED - a waiver is a
//! policy decision that never makes the proof terminal. See [`DischargeAudit`]
//! for the full argument.

use std::collections::{BTreeMap, BTreeSet};

use bifrost_types::{
    AccountError, CoverageCoordinate, CoverageDomain, CursorScope, InventoryCoverageReport,
    InventoryObligation, InventoryRepairTarget, ObligationKey,
};

/// What the system KNOWS about an obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProofStatus {
    /// Coverage was never proved for this obligation.
    Unresolved,
    /// A later accepted proof covered it. The gap is genuinely closed.
    Discharged { evidence: DischargeEvidence },
}

/// The retry budget one lineage shares.
///
/// A lineage is an obligation plus every child a repair pass split it into.
/// Its identity is the ROOT key: an entry with `parent: None` is its own
/// lineage, and a child's `parent` names the root directly, never an
/// intermediate - `replace_obligation` re-points every child at the parent's
/// lineage, so the shape is flat by construction and the codec rejects a row
/// that is not.
///
/// The budget lives HERE rather than on any entry. It used to live in the
/// root entry's `Retrying { attempts }`, and because a replacement always
/// discharges its parent, that put the one cap on a key-rotating loop onto a
/// discharged entry - which is what forced compaction to pin discharged roots
/// and keep process-local parent links for folded keys. Worse, expiry wrote
/// `OperatorBlocked` onto that discharged root while every open child kept its
/// own `Retrying` policy, so after the first split the cap was recorded and
/// never enforced. A row keyed by lineage id, alive exactly while the ledger
/// retains a member of the lineage, removes both.
///
/// `exhausted` is STICKY. The budget is a parameter of each charge rather than
/// stored state, so a later charge with a larger budget must not reopen a
/// lineage an earlier charge closed; and a member that joins a spent lineage
/// later (a reopened sibling, say) has to be born blocked, which it can only
/// be if the spent state is recorded rather than recomputed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct LineageBudget {
    /// Completed repair outcomes charged to the lineage.
    pub(crate) attempts: u32,
    /// Whether a charge has reached its budget. Never cleared while the row
    /// lives.
    pub(crate) exhausted: bool,
}

/// The lineage an entry belongs to: its parent when it was split out of one,
/// itself otherwise.
fn lineage_id(entry: &LedgerEntry) -> &ObligationKey {
    entry.parent.as_ref().unwrap_or(&entry.key)
}

/// Entry count at which an ingest folds terminal history into the audit table.
///
/// A threshold rather than a per-discharge sweep, so the common ingest does no
/// extra work and a test-sized ledger keeps every entry verbatim. Compaction
/// itself is one linear pass, which is the cost `record_proof` already pays on
/// every accepted report, so crossing the threshold does not change the shape
/// of an ingest.
const COMPACTION_THRESHOLD: usize = 512;

/// What survives a compacted `Discharged` entry.
///
/// # Why only the proved side compacts
///
/// The line falls out of [`ProofStatus`], and the next person to touch this
/// will be tempted to move it, so the reasoning lives here rather than in a
/// commit message.
///
/// `Discharged` is TERMINAL. Something proved the coverage, and no path in this
/// module transitions a discharged entry into anything an operator can act on -
/// rediscovery does not mutate it, it raises the obligation afresh. So a count
/// plus an audit root loses nothing anybody could have used.
///
/// `Unresolved` is NOT terminal, even when waived. A waiver is a POLICY
/// decision: it stops the entry blocking completion and leaves the proof
/// `Unresolved` forever by design, because nothing was proved. A later walk
/// over covering ground can still discharge it, and an operator may still need
/// its `ObligationKey` to revoke the waiver or drive a manual repair.
/// Compacting a waived entry would destroy both.
///
/// That is also what satisfies the constraint this work was filed under -
/// preserve the proved-versus-waived distinction rather than flattening it -
/// BY CONSTRUCTION rather than by bookkeeping. Only the proved side compacts,
/// so the waived entries are exactly the ones left sitting in the table in
/// full.
///
/// # What the root can and cannot answer
///
/// The root is a DETECTION root. It answers "does this candidate history fold
/// to the same value the ledger recorded", which is what makes a restored,
/// replicated or externally-recorded discharge history checkable, and what
/// makes a discharge that quietly vanished - or a corrupted durable row -
/// detectable. It is emphatically NOT a proof of exact history: the root is a
/// commutative SUM of digests, and a sum of digests is not collision-resistant
/// the way a single digest is. Two different histories can in principle fold to
/// the same root, and nothing here defends against an adversary who can choose
/// obligation keys to make that happen. Accidental divergence and loss is the
/// threat model; forgery is not.
///
/// The audited history is also NARROWER than the word "history" suggests. Only
/// `(key, generation, evidence KIND)` enters the fold. A change to a repair
/// attempt id, a covering domain, a `ReplacedByChildren` child list or the text
/// of a `ProvedIrrelevant` reason is INVISIBLE to the root, at any hash
/// strength, because those payloads are diagnostic and legitimately vary
/// between revisions.
///
/// It deliberately cannot answer "was key K discharged" on its own, and that is
/// a decision rather than an oversight:
///
/// - The actionable state is open debt, and open debt is retained in full.
/// - An exact membership index means retaining the keys, which is the
///   unbounded growth this compaction exists to remove.
/// - A probabilistic index (a Bloom filter, say) errs by reporting DISCHARGED
///   for something that never was. That is precisely the silent-loss shape this
///   module exists to prevent, so a bounded-but-lying answer is worse than no
///   answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DischargeAudit {
    /// How many entries were folded away for this scope.
    pub count: u64,
    /// Order-independent fold of every folded entry's fingerprint.
    ///
    /// Wrapping 256-bit ADDITION, not XOR: an obligation that is discharged,
    /// compacted, rediscovered, discharged and compacted again must count
    /// twice, and under XOR the second fold would silently cancel the first.
    /// Addition keeps the fold commutative - compaction order is not part of
    /// the contract - at the cost of the sum being weaker than its summands,
    /// which is why the guarantee above is detection rather than verification.
    pub root: [u8; 32],
    /// The newest generation folded away, so an audit can bound when the
    /// compacted history stops.
    pub latest_generation: crate::cursor::WalkGeneration,
}

impl Default for DischargeAudit {
    fn default() -> Self {
        Self {
            count: 0,
            root: [0; 32],
            latest_generation: super::PendingCoverage::initial_generation(),
        }
    }
}

impl DischargeAudit {
    /// Fold one discharged obligation in.
    ///
    /// Public so an audit holding a candidate history can rebuild a root and
    /// compare it against the one the ledger carries. The fingerprint is the
    /// stable part of the contract: key, generation, and which KIND of evidence
    /// closed it.
    pub fn fold(
        &mut self,
        key: &ObligationKey,
        generation: crate::cursor::WalkGeneration,
        evidence: &DischargeEvidence,
    ) {
        self.count = self.count.saturating_add(1);
        self.root = add_wrapping_256(self.root, discharge_fingerprint(key, generation, evidence));
        self.latest_generation = self.latest_generation.max(generation);
    }
}

/// Little-endian 256-bit wrapping addition.
///
/// Byte-wise with a carry rather than four `u64` limbs, because the durable
/// encoding is a byte array and a limb split would be one more place for an
/// endianness mistake to hide in a format that has to round-trip exactly.
fn add_wrapping_256(accumulator: [u8; 32], addend: [u8; 32]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut carry = 0u16;
    for ((slot, left), right) in out.iter_mut().zip(accumulator).zip(addend) {
        let sum = u16::from(left) + u16::from(right) + carry;
        *slot = u8::try_from(sum & 0x00ff).expect("masked to one byte");
        carry = sum >> 8;
    }
    out
}

/// Fingerprint of one discharged obligation.
///
/// SHA-256 over an unambiguous framing. The predecessor of this function used a
/// 128-bit FNV-1a, and the additive fold made that genuinely weak rather than
/// merely finite: the histories `{"00", "05"}` and `{"01", "04"}` at the same
/// generation and evidence kind fold to the same count, generation and sum, out
/// of ordinary short keys nobody chose adversarially. `sha2` was already a
/// pinned workspace dependency, so the fix cost a line in `Cargo.toml`.
///
/// The framing is length-prefixed and domain-separated so no two distinct
/// obligations can serialize alike. Only the evidence DISCRIMINANT enters it -
/// see [`DischargeAudit`] for what that leaves unaudited.
#[must_use]
pub fn discharge_fingerprint(
    key: &ObligationKey,
    generation: crate::cursor::WalkGeneration,
    evidence: &DischargeEvidence,
) -> [u8; 32] {
    use sha2::Digest;

    let mut hasher = sha2::Sha256::new();
    hasher.update(b"bifrost.sync.debt-ledger.discharge.v1");
    hasher.update(u64::try_from(key.0.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(&key.0);
    hasher.update(generation.to_le_bytes());
    hasher.update([evidence_tag(evidence)]);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Which kind of proof closed an obligation.
///
/// Only the discriminant enters the fingerprint. The payloads are unbounded
/// (`ReplacedByChildren` carries a key list, `ProvedIrrelevant` a free string)
/// and their exact bytes are diagnostic rather than load-bearing, so hashing
/// them would make the root sensitive to text that legitimately changes between
/// revisions.
fn evidence_tag(evidence: &DischargeEvidence) -> u8 {
    match evidence {
        DischargeEvidence::CoveringWalk { .. } => 0,
        DischargeEvidence::RepairedAndPublished { .. } => 1,
        DischargeEvidence::ProvedIrrelevant { .. } => 2,
        DischargeEvidence::ReplacedByChildren { .. } => 3,
    }
}

/// Whether a replacement improved the situation or merely reshaped it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplacementProgress {
    Progressed,
    /// Same unresolved extent, same granularity, no proof gained, no better
    /// repair authority. Repeated stalls exhaust the lineage budget rather
    /// than looping forever - which the caller has to charge through
    /// [`DebtLedger::charge_lineage`] with a lineage it resolved BEFORE the
    /// swap, since the swap discharges the parent and a discharged key is
    /// never charged.
    Stalled,
}

/// Why the engine refused a replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplacementRefusal {
    UnknownObligation,
    /// The obligation moved on, or was already closed, since this repair result
    /// was computed.
    StaleGeneration,
    /// A child key is already open under a different lineage root. Accepting it
    /// would give one obligation two roots and let a retry budget reset by
    /// picking whichever root is convenient.
    ChildCollidesWithForeignLineage,
    /// A child reuses the parent's own key or its lineage's root key. The swap
    /// would mutate the parent in place and then discharge it, leaving no open
    /// residual, and a child whose key is its lineage id would name itself as
    /// its parent.
    ChildReusesLineageKey,
    /// The parent is no longer `Retrying`: an operator blocked or waived it, or
    /// its lineage budget ran out, while this automatic result was in flight.
    /// Accepting the split would move the debt onto fresh `Retrying` children
    /// and discharge the only entry carrying that decision. The parent stays
    /// open with its policy intact, and nothing is charged.
    PolicyNoLongerRetrying,
}

/// Why an obligation is considered discharged.
///
/// Recorded rather than inferred so an audit can tell a re-enumeration from a
/// provider-native repair - and, once repair exists, so a discharge can be
/// attributed to the pass that earned it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DischargeEvidence {
    /// A later accepted report proved a domain covering this obligation.
    CoveringWalk { domain: CoverageDomain },
    /// A repair pass re-read the object authoritatively, rebuilt its entry, and
    /// the consumer durably accepted the resulting existence notification.
    ///
    /// Both halves are recorded because neither alone suffices: an account
    /// recovery nobody was told about leaves the consumer unaware, and a
    /// published id with no successful account result merely repeats an id
    /// without proving the representation failure healed.
    RepairedAndPublished {
        attempt: bifrost_types::RepairAttemptId,
    },
    /// A repair pass proved the object is not owed at all - absent under a
    /// cursor bridge that will report its removal, or out of scope. Emits
    /// nothing to the consumer: absence from an old inventory snapshot is not a
    /// deletion to apply against current state.
    ProvedIrrelevant { detail: String },
    /// Superseded by the children it was split into. The parent is closed so it
    /// cannot be replayed or double-counted; the debt lives on in the children.
    ReplacedByChildren { children: Vec<ObligationKey> },
}

/// What the system will DO about an obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PolicyStatus {
    /// Eligible for automatic repair. `attempts` counts COMPLETED repair
    /// outcomes charged to the obligation's LINEAGE, never attempts merely
    /// started: a crash after provider work but before the acknowledgement
    /// must cost a repeated attempt, not a consumed budget.
    ///
    /// On a ledger entry this is a VIEW. The count lives in the ledger's
    /// per-lineage budget, and every mutation rewrites the field on each open
    /// `Retrying` member from it, so siblings always agree and nothing reads the
    /// field back to decide anything. On a `BarrierIncident` it is inert.
    Retrying { attempts: u32 },
    /// Automatic work stopped. Still visible, still manually retryable, still
    /// blocking the completion sentinel. NOT abandonment.
    OperatorBlocked,
    /// An operator explicitly accepted this loss. The only terminal policy
    /// state, and the only one a human can put an obligation into.
    Waived { by: String, at_unix_seconds: i64 },
}

impl PolicyStatus {
    #[must_use]
    pub fn is_waived(&self) -> bool {
        matches!(self, Self::Waived { .. })
    }
}

/// One obligation as the engine tracks it over time.
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub key: ObligationKey,
    /// The region whose enumeration raised this. Discharge compares against
    /// THIS, not against the scope: a later walk must actually cover the
    /// ground the gap was on.
    pub domain: CoverageDomain,
    /// Engine-issued walk generation that raised it. Orders proof events; does
    /// not itself prove coverage.
    pub generation: crate::cursor::WalkGeneration,
    pub proof: ProofStatus,
    pub policy: PolicyStatus,
    /// The account-minted material a repair pass needs to address this.
    ///
    /// Retained because the executor cannot reconstruct a request without it:
    /// key, domain and error are engine- or diagnostic-level facts, and the
    /// error in particular must never be used as a repair descriptor because
    /// classifications and messages change between revisions. `None` for an
    /// obligation with no repair path.
    pub target: Option<InventoryRepairTarget>,
    /// The ROOT of the lineage this obligation was split out of, when a repair
    /// pass narrowed a region into smaller pieces - always the root itself,
    /// never an intermediate. Retry budgeting follows the lineage, so an
    /// account cannot reset a budget by re-minting an equivalent obligation.
    /// The root need not still be an entry: it is an identity, not a link
    /// anything dereferences.
    pub parent: Option<ObligationKey>,
    pub first_seen_unix_seconds: i64,
    pub last_error: AccountError,
}

impl LedgerEntry {
    /// Whether this entry still blocks a backfill completion sentinel.
    ///
    /// Waived entries do not block: an operator accepted the loss, and the
    /// point of the waiver is to let a scope finish. They remain `Unresolved`
    /// forever regardless, which is what keeps the audit honest.
    #[must_use]
    pub fn blocks_completion(&self) -> bool {
        match self.proof {
            ProofStatus::Discharged { .. } => false,
            ProofStatus::Unresolved => !self.policy.is_waived(),
        }
    }

    #[must_use]
    pub fn is_open(&self) -> bool {
        matches!(self.proof, ProofStatus::Unresolved)
    }
}

/// A walk that could not advance, recorded separately from the debt ledger.
///
/// A barrier is not debt behind an advanced cursor - no checkpoint was accepted
/// past it, so there is nothing durable it could hang off. But writing nothing
/// at all leaves the scope with no memory: a restart forgets the walk keeps
/// hitting the same wall, attempt history resets, operator visibility vanishes
/// between sessions, and there is no object for an operator to waive. So the
/// blocked progress itself is the durable thing.
///
/// A waived barrier authorizes a later walk to cross only through
/// `cross_waived_barriers`, which atomically converts the incident into an
/// unresolved-but-waived ledger entry. The waiver converts blocked progress
/// into declared accepted loss; it never makes the engine simply forget.
#[derive(Debug, Clone)]
pub struct BarrierIncident {
    pub key: ObligationKey,
    pub domain: CoverageDomain,
    pub generation: crate::cursor::WalkGeneration,
    pub failure_label: String,
    pub evidence: AccountError,
    pub policy: PolicyStatus,
    /// The last checkpoint proved to precede the whole barrier region, if the
    /// walk accepted one. Where a later walk resumes from.
    pub resume_from: Option<bifrost_types::Checkpoint>,
}

/// Per-account accumulated debt.
///
/// Ordered map so enumeration is deterministic - operator output and test
/// assertions both depend on it.
#[derive(Debug, Clone, Default)]
pub struct DebtLedger {
    entries: BTreeMap<ObligationKey, LedgerEntry>,
    barriers: BTreeMap<ObligationKey, BarrierIncident>,
    /// Domains proved complete, with the generation that proved them. Retained
    /// because coverage discharges by UNION: repartitioning can leave old debt
    /// covered by two later windows and by neither alone.
    proved: Vec<(crate::cursor::WalkGeneration, CoverageDomain)>,
    /// Terminal history that no longer has an entry, per scope.
    ///
    /// A `Vec` with a linear lookup rather than a map, because `CursorScope` is
    /// a `#[non_exhaustive]` public enum in `bifrost-types` with no `Ord`, and
    /// the count of scopes on one account is small enough that a scan is not
    /// the interesting cost. Order is deterministic: scopes are appended in the
    /// order compaction first encounters them, and compaction walks `entries`
    /// in key order.
    compacted: Vec<(CursorScope, DischargeAudit)>,
    /// One retry budget per lineage that still has a RETAINED member, keyed by
    /// lineage id. See [`LineageBudget`].
    ///
    /// Membership is decided by the lineage id alone, never by proof or
    /// policy: a lineage whose members are all discharged, waived or blocked
    /// keeps its row, so a member that rejoins it later inherits the budget
    /// instead of a fresh one. `settle_lineages` restores every invariant on
    /// this table after each mutation.
    lineages: BTreeMap<ObligationKey, LineageBudget>,
}

impl DebtLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Include terminal entries and compacted history: compaction must not
    /// reset the durable ordering when the last open obligation disappears.
    pub(crate) fn latest_generation(&self) -> crate::cursor::WalkGeneration {
        self.entries
            .values()
            .map(|entry| entry.generation)
            .chain(self.barriers.values().map(|barrier| barrier.generation))
            .chain(self.proved.iter().map(|(generation, _)| *generation))
            .chain(
                self.compacted
                    .iter()
                    .map(|(_, audit)| audit.latest_generation),
            )
            .max()
            .unwrap_or_else(super::PendingCoverage::initial_generation)
    }

    /// Rebuild a ledger from durable parts.
    ///
    /// The one door into the private maps that does not go through `ingest`,
    /// and deliberately crate-private: it exists for
    /// [`crate::cursor::ledger_envelope::decode_ledger`], which is the only
    /// caller that legitimately holds a whole ledger's state without having
    /// folded a report to get it. Keeping it out of the public surface is what
    /// preserves "no protocol crate may construct ledger state" - a consumer
    /// restores a ledger by decoding bytes this engine wrote, never by
    /// asserting one.
    ///
    /// Takes the parts VERBATIM: nothing is settled or healed here. The codec
    /// calls [`Self::validate_lineages`] on the result and refuses a row that
    /// fails it, because repairing contradictory durable state would mean
    /// choosing which half of the contradiction to believe.
    pub(crate) fn from_parts(
        entries: BTreeMap<ObligationKey, LedgerEntry>,
        barriers: BTreeMap<ObligationKey, BarrierIncident>,
        proved: Vec<(crate::cursor::WalkGeneration, CoverageDomain)>,
        compacted: Vec<(CursorScope, DischargeAudit)>,
        lineages: BTreeMap<ObligationKey, LineageBudget>,
    ) -> Self {
        Self {
            entries,
            barriers,
            proved,
            compacted,
            lineages,
        }
    }

    /// The per-lineage budgets, in their durable order. Crate-private for the
    /// codec, like `proved` and `compacted`.
    pub(crate) fn lineages(&self) -> &BTreeMap<ObligationKey, LineageBudget> {
        &self.lineages
    }

    /// Check the lineage invariants `settle_lineages` maintains, without
    /// repairing anything.
    ///
    /// Every entry's parent is flat (it names neither itself nor an entry that
    /// has a parent - which also rules out every chain and cycle); every
    /// entry's lineage has a row; every row has a retained member; and every
    /// open `Retrying` member mirrors its unexhausted row. A parent naming an
    /// ABSENT key is legal: that is a root compaction folded.
    pub(crate) fn validate_lineages(&self) -> Result<(), String> {
        let mut live: BTreeSet<&ObligationKey> = BTreeSet::new();
        for entry in self.entries.values() {
            if let Some(parent) = &entry.parent {
                if *parent == entry.key {
                    return Err("an entry names itself as its lineage root".into());
                }
                if self
                    .entries
                    .get(parent)
                    .is_some_and(|root| root.parent.is_some())
                {
                    return Err("an entry's lineage root is not a root".into());
                }
            }
            let lineage = lineage_id(entry);
            live.insert(lineage);
            let Some(budget) = self.lineages.get(lineage) else {
                return Err("an entry's lineage has no budget row".into());
            };
            if !entry.is_open() {
                continue;
            }
            if let PolicyStatus::Retrying { attempts } = entry.policy {
                if budget.exhausted {
                    return Err("a retrying entry sits in an exhausted lineage".into());
                }
                if attempts != budget.attempts {
                    return Err("a retrying entry disagrees with its lineage budget".into());
                }
            }
        }
        if self.lineages.keys().any(|lineage| !live.contains(lineage)) {
            return Err("a lineage budget row has no retained member".into());
        }
        Ok(())
    }

    /// The per-scope audit table, in its durable order.
    ///
    /// Crate-private for the same reason `proved` is: it is a consequence of
    /// compaction, not a query. The codec needs it because dropping it on
    /// restart would silently reset an account's discharged count to zero and
    /// make the root unverifiable forever after.
    pub(crate) fn compacted(&self) -> &[(CursorScope, DischargeAudit)] {
        &self.compacted
    }

    /// The compacted terminal history for `scope`, if any has been folded.
    ///
    /// `None` and a zero-count audit mean the same thing to a reader; the
    /// distinction is only that nothing has ever compacted for that scope.
    #[must_use]
    pub fn discharge_audit(&self, scope: &CursorScope) -> Option<&DischargeAudit> {
        self.compacted
            .iter()
            .find(|(candidate, _)| candidate == scope)
            .map(|(_, audit)| audit)
    }

    /// The retained proofs, with the generation that proved each.
    ///
    /// Not public for the same reason the field is not: proof retention is an
    /// engine-internal consequence of `record_proof`, not a query. The codec
    /// needs it because a restart that forgets a retained proof re-opens debt
    /// a later partial walk can no longer discharge by union.
    pub(crate) fn proved(&self) -> &[(crate::cursor::WalkGeneration, CoverageDomain)] {
        &self.proved
    }

    /// Whether this ledger holds nothing worth persisting.
    ///
    /// The audit table counts. A ledger whose entries have all been compacted
    /// away still carries the only surviving record that they existed, and a
    /// caller that skips writing an "empty" ledger would erase exactly the
    /// history compaction was supposed to preserve.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.barriers.is_empty() && self.compacted.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = &LedgerEntry> {
        self.entries.values()
    }

    pub fn barriers(&self) -> impl Iterator<Item = &BarrierIncident> {
        self.barriers.values()
    }

    #[must_use]
    pub fn entry(&self, key: &ObligationKey) -> Option<&LedgerEntry> {
        self.entries.get(key)
    }

    /// Every obligation still owed: unresolved and unwaived.
    pub fn open_debt(&self) -> impl Iterator<Item = &LedgerEntry> {
        self.entries
            .values()
            .filter(|entry| entry.blocks_completion())
    }

    /// Whether a backfill completion sentinel may be written for `scope`.
    ///
    /// Evaluated against the ledger AS IT STANDS, never from a boolean computed
    /// earlier by a walk. A runner that decided "complete" before a sibling
    /// partition's debt was accepted would otherwise write a sentinel that
    /// makes the next attach skip a scope with open obligations - and the
    /// inverse, a runner that saw degraded coverage before an operator waived
    /// it, would leave the scope incomplete for no reason.
    #[must_use]
    pub fn completion_permitted(&self, scope: &bifrost_types::CursorScope) -> bool {
        let blocked_entry = self
            .entries
            .values()
            .any(|entry| entry.domain.scope == *scope && entry.blocks_completion());
        let blocked_barrier = self
            .barriers
            .values()
            .any(|barrier| barrier.domain.scope == *scope && !barrier.policy.is_waived());
        !blocked_entry && !blocked_barrier
    }

    /// Whether a barrier at `key` has been waived, authorizing a walk to cross
    /// it and record the loss instead of stopping.
    #[must_use]
    pub fn barrier_waived(&self, key: &ObligationKey) -> bool {
        self.barriers
            .get(key)
            .is_some_and(|barrier| barrier.policy.is_waived())
    }

    #[must_use]
    pub fn scope_has_blocked_barrier(&self, scope: &bifrost_types::CursorScope) -> bool {
        self.barriers.values().any(|barrier| {
            barrier.domain.scope == *scope
                && matches!(barrier.policy, PolicyStatus::OperatorBlocked)
        })
    }

    /// Convert every barrier in `report` into ordinary accepted-loss debt.
    ///
    /// This is all-or-nothing. A checkpoint crosses the whole report domain,
    /// so one unwaived or unknown barrier keeps the walk stopped. The account
    /// writer calls this and persists the resulting ledger as one ordered
    /// operation, making a waiver or block that races the walk resolve by
    /// writer order rather than by a stale snapshot.
    pub fn cross_waived_barriers(
        &mut self,
        report: &InventoryCoverageReport,
        generation: crate::cursor::WalkGeneration,
        now: i64,
    ) -> bool {
        let barriers: Vec<_> = report
            .obligations()
            .iter()
            .filter(|obligation| obligation.is_barrier())
            .collect();
        if barriers.is_empty()
            || barriers.iter().any(|obligation| {
                let key = obligation.key();
                !self.barrier_waived(key)
                    && !self
                        .entries
                        .get(key)
                        .is_some_and(|entry| entry.policy.is_waived())
            })
        {
            return false;
        }

        for obligation in barriers {
            let key = obligation.key().clone();
            if let Some(incident) = self.barriers.remove(&key) {
                self.entries.insert(
                    key.clone(),
                    LedgerEntry {
                        key,
                        domain: report.domain.clone(),
                        generation,
                        proof: ProofStatus::Unresolved,
                        policy: incident.policy,
                        target: None,
                        parent: None,
                        first_seen_unix_seconds: now,
                        last_error: obligation.error().clone(),
                    },
                );
            } else if let Some(entry) = self.entries.get_mut(&key) {
                // Terminal reports are commonly cumulative with the final
                // page. Repeating an already-crossed barrier must remain
                // authorized by the same waiver rather than recreating a wall
                // one event later.
                entry.domain = report.domain.clone();
                entry.generation = generation;
                entry.proof = ProofStatus::Unresolved;
                entry.target = None;
                entry.last_error = obligation.error().clone();
            }
        }
        self.settle_lineages();
        true
    }

    /// Fold one ACCEPTED coverage report into the ledger.
    ///
    /// "Accepted" is load-bearing: this runs when the consumer acknowledges the
    /// checkpoint the report rode on, not when the account emitted it. A report
    /// whose checkpoint is never acknowledged may describe entries the consumer
    /// never persisted, so it cannot be allowed to prove anything.
    ///
    /// The single exhaustive ingestion path. Nothing else mutates entries.
    pub fn ingest(
        &mut self,
        report: &InventoryCoverageReport,
        generation: crate::cursor::WalkGeneration,
        now: i64,
    ) {
        for obligation in report.obligations() {
            // A barrier never becomes ledger debt here. It is recorded as a
            // blocked-progress incident by the walk that hit it, and only
            // becomes a (waived, unresolved) entry when an operator authorizes
            // crossing it.
            if obligation.is_barrier() {
                continue;
            }
            self.upsert(obligation, &report.domain, generation, now);
        }

        if report.is_complete() {
            self.record_proof(report.domain.clone(), generation);
        }
        self.compact_if_large();
        self.settle_lineages();
    }

    /// Fold in only the DEBT from a report, never its proof.
    ///
    /// For a report that reached the engine without an acknowledgeable
    /// checkpoint - a partition walk's terminal summary, say. Recording debt
    /// for progress that may not have committed is conservative: the worst case
    /// is a scope that stays degraded until something re-reads it. Recording
    /// PROOF on the same terms would not be, because the consumer may never
    /// have persisted the entries that proof rests on.
    pub fn ingest_debt_only(
        &mut self,
        report: &InventoryCoverageReport,
        generation: crate::cursor::WalkGeneration,
        now: i64,
    ) {
        for obligation in report.obligations() {
            if obligation.is_barrier() {
                continue;
            }
            self.upsert(obligation, &report.domain, generation, now);
        }
        self.compact_if_large();
        self.settle_lineages();
    }

    /// Fold terminal history into the audit table once the entry map is large.
    ///
    /// Runs at the END of a fold, in the single writer every durable mutation
    /// goes through, and never mid-transition. That placement is what makes it
    /// safe against a concurrent transition rather than merely unlikely to race
    /// one: see [`Self::compact_discharged`] for why removing a terminal entry
    /// cannot change the outcome of anything in flight. Leaves settling to its
    /// caller, which settles once for the whole fold.
    fn compact_if_large(&mut self) {
        if self.entries.len() >= COMPACTION_THRESHOLD {
            self.fold_discharged();
        }
    }

    /// Restore every invariant on the lineage table after a mutation.
    ///
    /// The one place lineage state is reconciled, run at the end of every
    /// public mutator that changed anything, so no individual transition has
    /// to remember it. One linear pass:
    ///
    /// - the live lineages are the ids of every RETAINED entry, open or
    ///   discharged, whatever its policy. A discharged member still counts
    ///   because it can be rediscovered, and `upsert` reopens it with its
    ///   history intact: dropping the row at discharge would hand it a fresh
    ///   budget, and a provider that alternates a covering walk with a
    ///   rediscovery would never reach the cap;
    /// - a row with no retained member is dropped, so the table is bounded by
    ///   the entry map, which compaction bounds - a folded key raised again is
    ///   a fresh gap by the compaction contract;
    /// - a live lineage without a row gets a fresh one;
    /// - every open `Retrying` member is rewritten from its row: blocked when
    ///   the row is exhausted, otherwise mirroring the row's count.
    ///
    /// Waived and `OperatorBlocked` members are never written, so no operator
    /// decision is ever overwritten; and discharged entries are never touched
    /// at all. There is no path from `OperatorBlocked` back to `Retrying`, so a
    /// member this blocks stays blocked.
    fn settle_lineages(&mut self) {
        let live: BTreeSet<ObligationKey> = self
            .entries
            .values()
            .map(|entry| lineage_id(entry).clone())
            .collect();
        self.lineages.retain(|lineage, _| live.contains(lineage));
        for lineage in live {
            self.lineages.entry(lineage).or_default();
        }
        let Self {
            entries, lineages, ..
        } = self;
        for entry in entries.values_mut() {
            if !entry.is_open() {
                continue;
            }
            let budget = lineages.get(lineage_id(entry)).copied().unwrap_or_default();
            if let PolicyStatus::Retrying { attempts } = &mut entry.policy {
                if budget.exhausted {
                    entry.policy = PolicyStatus::OperatorBlocked;
                } else {
                    *attempts = budget.attempts;
                }
            }
        }
    }

    /// Fold every compactable `Discharged` entry into its scope's audit,
    /// returning how many entries were removed.
    ///
    /// Called automatically once the entry map crosses
    /// `COMPACTION_THRESHOLD`; exposed so an operator tool or a consumer that
    /// knows a burst of repair just closed can reclaim without waiting for one
    /// more report.
    ///
    /// # Why this cannot disturb work in flight
    ///
    /// Only `Discharged` entries are candidates, and every mutating path keyed
    /// on an entry already refuses a non-open one: `discharge_repaired` and
    /// `replace_obligation` both bail on `!is_open()`, and the foreign-lineage
    /// collision check only fires on an open entry. So for those, a removed
    /// terminal entry and a present terminal entry produce the same answer.
    ///
    /// That includes the LINEAGE, which used to be the exception. The retry
    /// budget once lived on the root entry, which a replacement always
    /// discharges, so compaction had to pin every discharged root a live child
    /// charged against. The budget now lives in the ledger's own per-lineage
    /// table, and a child's `parent` is an identity rather than a link anything
    /// dereferences, so a discharged root folds like any other terminal entry
    /// and nothing is pinned.
    ///
    /// A repair result arriving for a key folded while it was in flight
    /// charges nothing: `record_attempt` charges only an open entry. That
    /// cannot let a loop escape the cap - a key with no open entry is never
    /// planned again, so no provider can collect free attempts against it -
    /// and the open siblings it leaves behind are charged by their own
    /// results.
    ///
    /// `DischargeEvidence::ReplacedByChildren` also names entries, and those are
    /// deliberately not retained either. The list is EVIDENCE: nothing in the
    /// engine dereferences it for a decision (the codec encodes and decodes it,
    /// and no other reader exists), so a child key in it is a record of what
    /// happened, not a link something will follow.
    ///
    /// # What a later rediscovery costs
    ///
    /// A compacted key rediscovered by a later walk is raised as a NEW entry,
    /// with `first_seen_unix_seconds` set to now and the retry budget at zero -
    /// where an uncompacted discharged entry would have kept both. That is a
    /// real behaviour change and it is accepted: the entry was proved covered
    /// before it was folded, so a re-raise after proof is a fresh gap rather
    /// than a continuing one, and charging it the old budget would be charging
    /// it for failures that provably stopped.
    ///
    /// One exception: a compacted lineage ROOT re-raised while members of its
    /// lineage are still retained rejoins that lineage's budget, because its
    /// lineage id is its own key and the row is still live. See `upsert`.
    pub fn compact_discharged(&mut self) -> usize {
        let removed = self.fold_discharged();
        self.settle_lineages();
        removed
    }

    /// The fold itself, without settling. See [`Self::compact_discharged`].
    fn fold_discharged(&mut self) -> usize {
        let candidates: Vec<ObligationKey> = self
            .entries
            .values()
            .filter(|entry| !entry.is_open())
            .map(|entry| entry.key.clone())
            .collect();

        let mut removed = 0;
        for key in &candidates {
            let Some(entry) = self.entries.remove(key) else {
                continue;
            };
            let ProofStatus::Discharged { evidence } = &entry.proof else {
                // Unreachable: candidates are exactly the non-open entries.
                // Restored rather than dropped, because losing an entry here
                // would be silent debt loss.
                self.entries.insert(key.clone(), entry);
                continue;
            };
            self.audit_for(&entry.domain.scope)
                .fold(&entry.key, entry.generation, evidence);
            removed += 1;
        }
        removed
    }

    fn audit_for(&mut self, scope: &CursorScope) -> &mut DischargeAudit {
        if let Some(index) = self
            .compacted
            .iter()
            .position(|(candidate, _)| candidate == scope)
        {
            return &mut self.compacted[index].1;
        }
        self.compacted
            .push((scope.clone(), DischargeAudit::default()));
        &mut self
            .compacted
            .last_mut()
            .expect("just pushed an audit entry")
            .1
    }

    fn upsert(
        &mut self,
        obligation: &InventoryObligation,
        domain: &CoverageDomain,
        generation: crate::cursor::WalkGeneration,
        now: i64,
    ) {
        let key = obligation.key().clone();
        match self.entries.get_mut(&key) {
            Some(existing) => {
                // A re-raised obligation keeps its history. Resetting
                // `first_seen` or the attempt count each time the same broken
                // object is rediscovered would make any budget unreachable.
                // Delayed reports must not make an older proof eligible to settle
                // a gap observed by a newer walk.
                existing.generation = existing.generation.max(generation);
                existing.domain = domain.clone();
                existing.last_error = obligation.error().clone();
                // The token may legitimately rotate while naming the same gap,
                // so the descriptor refreshes even though identity does not.
                existing.target = InventoryRepairTarget::from_obligation(obligation);
                // Rediscovery is proof it is still missing, so an earlier
                // discharge was wrong. Reopen it - but never touch policy: an
                // operator's waiver stands until the operator revokes it.
                existing.proof = ProofStatus::Unresolved;
            }
            None => {
                // A key with no entry - never seen, or compacted after proof -
                // is a new entry with no parent, so its lineage id is its own
                // key. That id usually has no row and the budget starts from
                // zero; but a compacted lineage ROOT rediscovered while
                // children of its lineage are still retained finds their row
                // and rejoins it. That is deliberate: it is the same
                // obligation re-raised while its lineage is still being
                // worked, it is what happened when the root could not fold
                // under a live child, and resetting there would let a
                // provider evade the cap by getting the root compacted and
                // re-raised.
                self.entries.insert(
                    key.clone(),
                    LedgerEntry {
                        key,
                        domain: domain.clone(),
                        generation,
                        proof: ProofStatus::Unresolved,
                        policy: PolicyStatus::Retrying { attempts: 0 },
                        target: InventoryRepairTarget::from_obligation(obligation),
                        parent: None,
                        first_seen_unix_seconds: now,
                        last_error: obligation.error().clone(),
                    },
                );
            }
        }
    }

    /// Record a domain proved complete, and discharge whatever it covers.
    fn record_proof(&mut self, domain: CoverageDomain, generation: crate::cursor::WalkGeneration) {
        self.proved.push((generation, domain));
        for entry in self.entries.values_mut() {
            if !entry.is_open() {
                continue;
            }
            // Both conditions, not either. A newer generation stops a stale
            // report overwriting fresh state, but recency alone proves nothing
            // - a newer PARTIAL walk is still partial. The domain has to
            // actually contain the ground the gap was on.
            if proof_union_covers(&self.proved, entry.generation, &entry.domain) {
                entry.proof = ProofStatus::Discharged {
                    evidence: DischargeEvidence::CoveringWalk {
                        domain: entry.domain.clone(),
                    },
                };
            }
        }
        self.barriers.retain(|_, barrier| {
            !proof_union_covers(&self.proved, barrier.generation, &barrier.domain)
        });
        // A proof older than newly-raised debt cannot discharge it, so proofs
        // are load-bearing only while they contribute to an obligation or
        // barrier that is currently open. Drop everything else instead of
        // rewriting an ever-growing history on every acknowledgement.
        self.proved.retain(|(proof_generation, proof)| {
            self.entries.values().any(|entry| {
                entry.is_open()
                    && *proof_generation >= entry.generation
                    && domains_may_join(proof, &entry.domain)
            }) || self.barriers.values().any(|barrier| {
                *proof_generation >= barrier.generation && domains_may_join(proof, &barrier.domain)
            })
        });
    }

    /// Record a walk that stopped at a barrier.
    ///
    /// Idempotent per key: hitting the same wall on every attach must not
    /// accumulate incidents, and must not reset an operator's decision about
    /// it.
    pub fn record_barrier(&mut self, incident: BarrierIncident) {
        match self.barriers.get_mut(&incident.key) {
            Some(existing) => {
                existing.generation = existing.generation.max(incident.generation);
                existing.evidence = incident.evidence;
                existing.resume_from = incident.resume_from;
            }
            None => {
                self.barriers.insert(incident.key.clone(), incident);
            }
        }
    }

    /// Obligations eligible for an automatic repair attempt.
    ///
    /// Unresolved, not waived, not operator-blocked, and carrying a repair
    /// descriptor. A barrier has no descriptor and is correctly excluded: no
    /// checkpoint advanced past it, so it is blocked progress rather than debt,
    /// and its only terminal state is an operator waiver.
    pub fn repairable(&self) -> impl Iterator<Item = &LedgerEntry> {
        self.entries.values().filter(|entry| {
            entry.is_open()
                && entry.target.is_some()
                && matches!(entry.policy, PolicyStatus::Retrying { .. })
        })
    }

    /// The lineage root `key` belongs to: its entry's `parent`, or `key` itself
    /// when it has no parent or no entry.
    ///
    /// Budgets accrue per lineage, not per key, or an account evades every
    /// budget by re-minting an equivalent obligation under a fresh key each
    /// pass. Lineages are flat, so this is one read, not a walk.
    #[must_use]
    pub fn lineage_root(&self, key: &ObligationKey) -> ObligationKey {
        self.entries
            .get(key)
            .map_or_else(|| key.clone(), |entry| lineage_id(entry).clone())
    }

    /// The lineage a charge against `key` would land on: `Some` only while
    /// `key` has an OPEN entry.
    ///
    /// A caller about to mutate `key` out of the open set - a replacement
    /// discharges its parent - resolves this first and charges the result
    /// through [`Self::charge_lineage`] afterwards.
    #[must_use]
    pub fn open_lineage(&self, key: &ObligationKey) -> Option<ObligationKey> {
        self.entries
            .get(key)
            .filter(|entry| entry.is_open())
            .map(|entry| lineage_id(entry).clone())
    }

    /// Record one COMPLETED repair attempt against `key`'s lineage.
    ///
    /// Completed, never merely started: a crash after provider work but before
    /// the acknowledgement must cost a repeated attempt, not a consumed budget,
    /// so nothing durable is written before an outcome comes back.
    ///
    /// Charges only while `key` has an OPEN entry, and never treats a bare key
    /// as a lineage id. A result for an obligation that was discharged (and
    /// perhaps folded) while it was in flight charges nothing: a key with no
    /// open entry is never planned again, so skipping it cannot let a loop
    /// escape the cap. A result for a key that was re-raised as a fresh
    /// occurrence meanwhile charges the new occurrence, which over-charges by
    /// at most the results already in flight - the conservative direction.
    ///
    /// Returns whether the budget expired on this attempt. See
    /// [`Self::charge_lineage`].
    pub fn record_attempt(&mut self, key: &ObligationKey, budget: u32) -> bool {
        match self.open_lineage(key) {
            Some(lineage) => self.charge_lineage(&lineage, budget),
            None => false,
        }
    }

    /// Charge one completed attempt to `lineage` directly.
    ///
    /// For a caller that resolved the lineage with [`Self::open_lineage`]
    /// before a mutation closed the key it came from. A lineage the ledger no
    /// longer retains any member of has no row and is not charged.
    ///
    /// Returns whether THIS charge exhausted the budget. Exhaustion is sticky -
    /// a later charge with a larger budget does not reopen the lineage - and it
    /// turns every open `Retrying` member `OperatorBlocked`: automatic work
    /// stopped, still visible, still blocking. It NEVER yields `Waived` or
    /// `Discharged`: a counter running out is evidence that retrying is not
    /// working, not a decision about what loss is acceptable. Members an
    /// operator already waived or blocked keep that decision.
    pub fn charge_lineage(&mut self, lineage: &ObligationKey, budget: u32) -> bool {
        let Some(row) = self.lineages.get_mut(lineage) else {
            return false;
        };
        row.attempts = row.attempts.saturating_add(1);
        let exhausted_now = !row.exhausted && row.attempts >= budget;
        row.exhausted |= exhausted_now;
        self.settle_lineages();
        exhausted_now
    }

    /// Discharge `key` on proof that a repair recovered it and the consumer
    /// durably accepted the resulting notification.
    ///
    /// Conditional on generation: a result computed against an older view of
    /// the obligation must not close a version of it that has since been
    /// re-raised. Serialization through the single writer orders the messages;
    /// it does not by itself reject stale intent, which is what this check is
    /// for.
    ///
    /// Returns whether anything changed.
    pub fn discharge_repaired(
        &mut self,
        key: &ObligationKey,
        generation: crate::cursor::WalkGeneration,
        evidence: DischargeEvidence,
    ) -> bool {
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        if entry.generation > generation || !entry.is_open() {
            return false;
        }
        entry.proof = ProofStatus::Discharged { evidence };
        self.settle_lineages();
        true
    }

    /// Atomically replace `key` with `children`, recording `proved` as covered.
    ///
    /// A replacement is a SWAP, never an append. Appending children while the
    /// parent stays open double-counts the extent and replays the parent
    /// forever.
    ///
    /// Conservation is the safety property: nothing may vanish from the parent
    /// merely because no child names it. Where the extent is expressible in the
    /// lattice the engine verifies that `proved` plus the children's domains
    /// cover the parent; where it is opaque it cannot, so the account's
    /// assertion is recorded rather than the engine pretending it verified one.
    ///
    /// Children inherit the lineage root, so the retry budget follows the
    /// unresolved lineage rather than resetting per generated key. The swap
    /// discharges the parent, so a caller that wants to charge a `Stalled`
    /// result resolves [`Self::open_lineage`] BEFORE calling this.
    ///
    /// Returns `Err` with a reason when the replacement is refused. A refusal
    /// changes nothing.
    pub fn replace_obligation(
        &mut self,
        key: &ObligationKey,
        proved: &[CoverageDomain],
        children: &[InventoryObligation],
        generation: crate::cursor::WalkGeneration,
        now: i64,
    ) -> Result<ReplacementProgress, ReplacementRefusal> {
        let Some(parent) = self.entries.get(key).cloned() else {
            return Err(ReplacementRefusal::UnknownObligation);
        };
        if parent.generation > generation || !parent.is_open() {
            return Err(ReplacementRefusal::StaleGeneration);
        }
        // A split moves the debt onto fresh children and discharges the parent,
        // so accepting one against a parent an operator blocked or waived - or
        // whose lineage ran out - while this result was in flight would erase
        // that decision from the live debt.
        if !matches!(parent.policy, PolicyStatus::Retrying { .. }) {
            return Err(ReplacementRefusal::PolicyNoLongerRetrying);
        }
        let root = lineage_id(&parent).clone();
        if children
            .iter()
            .any(|child| child.key() == key || *child.key() == root)
        {
            return Err(ReplacementRefusal::ChildReusesLineageKey);
        }
        // A child colliding with an obligation from another lineage would give
        // one key two roots and let a budget reset by choosing the convenient
        // one. Refused as a contract violation rather than resolved silently.
        // That holds for a DISCHARGED retained entry too: `upsert` reopens an
        // existing entry with its policy and history intact, so adopting one
        // would move another lineage's waiver or block onto this residual
        // without any operator deciding it. The same goes for a child whose
        // key is ANOTHER live lineage's id, entry or not: adopting it would
        // give that lineage's members a parent that itself has a parent, which
        // is the chain shape the flat lineage model rules out.
        for child in children {
            let retained_elsewhere = self
                .entries
                .get(child.key())
                .is_some_and(|existing| *lineage_id(existing) != root);
            if retained_elsewhere || self.lineages.contains_key(child.key()) {
                return Err(ReplacementRefusal::ChildCollidesWithForeignLineage);
            }
        }

        let progress = self.classify_progress(&parent, proved, children);

        for domain in proved {
            self.record_proof(domain.clone(), generation);
        }
        for child in children {
            if child.is_barrier() {
                continue;
            }
            self.upsert(child, &parent.domain, generation, now);
            if let Some(entry) = self.entries.get_mut(child.key()) {
                entry.parent = Some(root.clone());
            }
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.proof = ProofStatus::Discharged {
                evidence: DischargeEvidence::ReplacedByChildren {
                    children: children.iter().map(|c| c.key().clone()).collect(),
                },
            };
        }
        self.settle_lineages();
        Ok(progress)
    }

    /// Whether a replacement actually improved the situation.
    ///
    /// Extent equality alone is the wrong test in both directions. It REJECTS
    /// real progress: turning one opaque region into three stable object
    /// obligations leaves the unresolved extent unchanged while making every
    /// piece independently retryable and dischargeable. And it ACCEPTS fake
    /// progress: a provider can repartition a region into children whose union
    /// equals the parent, with fresh keys and rotated tokens, forever.
    ///
    /// So progress is judged across several dimensions, and token rotation or
    /// error reclassification alone is not one of them.
    fn classify_progress(
        &self,
        parent: &LedgerEntry,
        proved: &[CoverageDomain],
        children: &[InventoryObligation],
    ) -> ReplacementProgress {
        if !proved.is_empty() {
            return ReplacementProgress::Progressed;
        }
        // An opaque region becoming addressable objects is progress even at
        // identical extent.
        let parent_is_region = matches!(parent.target, Some(InventoryRepairTarget::Region { .. }));
        let children_are_objects = !children.is_empty()
            && children
                .iter()
                .all(|c| matches!(c, InventoryObligation::Object { .. }));
        if parent_is_region && children_are_objects {
            return ReplacementProgress::Progressed;
        }
        // A barrier becoming durably replayable is progress: repair authority
        // strictly improved.
        if children.iter().any(|child| {
            matches!(
                child,
                InventoryObligation::Region {
                    recovery: bifrost_types::RegionRecovery::DurableReplay { .. },
                    ..
                }
            )
        }) && parent.target.is_none()
        {
            return ReplacementProgress::Progressed;
        }
        ReplacementProgress::Stalled
    }

    /// Operator action: accept the loss at `key`.
    ///
    /// Targets ONE occurrence. A waiver keyed on a failure label would
    /// authorize every future failure of that class - Graph's
    /// "scope:unidentifiable-value" describes a class, not a region - and
    /// authorizing unknown future loss is a different and much larger decision
    /// than accepting a loss you can see.
    ///
    /// Returns whether anything matched.
    pub fn waive(&mut self, key: &ObligationKey, by: String, at_unix_seconds: i64) -> bool {
        let policy = PolicyStatus::Waived {
            by,
            at_unix_seconds,
        };
        let mut waived = false;
        if let Some(entry) = self.entries.get_mut(key) {
            // Policy only. `proof` stays `Unresolved` forever: nothing was
            // proved, somebody decided to live without it.
            entry.policy = policy.clone();
            waived = true;
        }
        if let Some(barrier) = self.barriers.get_mut(key) {
            barrier.policy = policy;
            waived = true;
        }
        self.settle_lineages();
        waived
    }

    /// Operator action: stop automatic retries without accepting the loss.
    pub fn block(&mut self, key: &ObligationKey) -> bool {
        let mut blocked = false;
        if let Some(entry) = self.entries.get_mut(key) {
            entry.policy = PolicyStatus::OperatorBlocked;
            blocked = true;
        }
        if let Some(barrier) = self.barriers.get_mut(key) {
            barrier.policy = PolicyStatus::OperatorBlocked;
            blocked = true;
        }
        self.settle_lineages();
        blocked
    }
}

fn domains_may_join(proof: &CoverageDomain, target: &CoverageDomain) -> bool {
    if proof.scope != target.scope {
        return false;
    }
    match (&proof.coordinate, &target.coordinate) {
        (CoverageCoordinate::Full, _) => true,
        (CoverageCoordinate::TimeRange { .. }, CoverageCoordinate::TimeRange { .. }) => true,
        (
            CoverageCoordinate::UidRange {
                uid_validity: a, ..
            },
            CoverageCoordinate::UidRange {
                uid_validity: b, ..
            },
        ) => a == b,
        (CoverageCoordinate::PageRange { .. }, CoverageCoordinate::PageRange { .. }) => {
            proof.snapshot.same_snapshot_as(&target.snapshot)
        }
        (CoverageCoordinate::ProviderRegion { .. }, CoverageCoordinate::ProviderRegion { .. }) => {
            proof.covers(target)
        }
        _ => false,
    }
}

fn proof_union_covers(
    proofs: &[(crate::cursor::WalkGeneration, CoverageDomain)],
    minimum_generation: crate::cursor::WalkGeneration,
    target: &CoverageDomain,
) -> bool {
    if proofs
        .iter()
        .any(|(generation, proof)| *generation >= minimum_generation && proof.covers(target))
    {
        return true;
    }

    match &target.coordinate {
        CoverageCoordinate::TimeRange {
            from_unix_seconds,
            to_unix_seconds,
        } => interval_union_covers(
            proofs.iter().filter_map(|(generation, proof)| {
                if *generation < minimum_generation || !domains_may_join(proof, target) {
                    return None;
                }
                match proof.coordinate {
                    CoverageCoordinate::TimeRange {
                        from_unix_seconds,
                        to_unix_seconds,
                    } => Some((from_unix_seconds, to_unix_seconds)),
                    _ => None,
                }
            }),
            *from_unix_seconds,
            *to_unix_seconds,
        ),
        CoverageCoordinate::UidRange { from, to, .. } => finite_union_covers(
            proofs.iter().filter_map(|(generation, proof)| {
                if *generation < minimum_generation || !domains_may_join(proof, target) {
                    return None;
                }
                match proof.coordinate {
                    CoverageCoordinate::UidRange { from, to, .. } => Some((from, to)),
                    _ => None,
                }
            }),
            *from,
            *to,
        ),
        CoverageCoordinate::PageRange { from, to } => finite_union_covers(
            proofs.iter().filter_map(|(generation, proof)| {
                if *generation < minimum_generation || !domains_may_join(proof, target) {
                    return None;
                }
                match proof.coordinate {
                    CoverageCoordinate::PageRange { from, to } => {
                        Some((u64::from(from), u64::from(to)))
                    }
                    _ => None,
                }
            }),
            u64::from(*from),
            u64::from(*to),
        ),
        CoverageCoordinate::Full | CoverageCoordinate::ProviderRegion { .. } => false,
        _ => false,
    }
}

fn finite_union_covers(
    intervals: impl Iterator<Item = (u64, u64)>,
    target_from: u64,
    target_to: u64,
) -> bool {
    let mut intervals: Vec<_> = intervals.collect();
    intervals.sort_unstable();
    let mut reached = target_from;
    for (from, to) in intervals {
        if to <= reached || from > reached {
            continue;
        }
        reached = reached.max(to);
        if reached >= target_to {
            return true;
        }
    }
    false
}

fn interval_union_covers(
    intervals: impl Iterator<Item = (Option<i64>, Option<i64>)>,
    target_from: Option<i64>,
    target_to: Option<i64>,
) -> bool {
    let encode_lower = |value: Option<i64>| value.unwrap_or(i64::MIN);
    let encode_upper = |value: Option<i64>| value.unwrap_or(i64::MAX);
    let intervals = intervals.map(|(from, to)| {
        let from = u64::from_be_bytes(encode_lower(from).to_be_bytes()) ^ (1_u64 << 63);
        let to = u64::from_be_bytes(encode_upper(to).to_be_bytes()) ^ (1_u64 << 63);
        (from, to)
    });
    let from = u64::from_be_bytes(encode_lower(target_from).to_be_bytes()) ^ (1_u64 << 63);
    let to = u64::from_be_bytes(encode_upper(target_to).to_be_bytes()) ^ (1_u64 << 63);
    finite_union_covers(intervals, from, to)
}

#[cfg(test)]
mod tests {
    use super::{
        BarrierIncident, DebtLedger, DischargeEvidence, PolicyStatus, ProofStatus,
        ReplacementProgress, ReplacementRefusal,
    };
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, CoverageCoordinate, CoverageDomain,
        CursorScope, DiagnosticText, InventoryCoverageReport, InventoryObligation, ObjectId,
        ObjectType, ObligationKey, RegionRecovery, RequestCause, RequestErrorKind,
        SnapshotIdentity,
    };

    fn scope() -> CursorScope {
        CursorScope::Type(ObjectType::Email)
    }

    fn error() -> bifrost_types::AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("unrepresentable"),
            }),
        )
        .try_build()
        .expect("valid account error classification")
    }

    fn object(key: &str) -> InventoryObligation {
        InventoryObligation::Object {
            key: ObligationKey(key.as_bytes().to_vec()),
            id: ObjectId(key.to_string()),
            error: error(),
            repair: Vec::new(),
        }
    }

    fn time_domain(from: i64, to: i64) -> CoverageDomain {
        CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::TimeRange {
                from_unix_seconds: Some(from),
                to_unix_seconds: Some(to),
            },
            snapshot: SnapshotIdentity::unstable(),
        }
    }

    #[test]
    fn an_obligation_blocks_completion_until_something_resolves_it() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(1),
            100,
        );
        assert!(!ledger.completion_permitted(&scope()));
        assert_eq!(ledger.open_debt().count(), 1);
    }

    /// A later walk over covering ground closes the gap. Without this the
    /// ledger becomes false in the other direction: asserting an outstanding
    /// gap after accepting proof it no longer exists, and blocking the sentinel
    /// forever for no cause.
    #[test]
    fn a_covering_walk_discharges_earlier_debt() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(4),
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport {
                domain: time_domain(0, 180),
                outcome: bifrost_types::CoverageOutcome::Complete,
            },
            crate::cursor::WalkGeneration::for_test(5),
            200,
        );

        let entry = ledger
            .entry(&ObligationKey(b"a".to_vec()))
            .expect("entry retained for audit");
        assert!(matches!(entry.proof, ProofStatus::Discharged { .. }));
        assert!(ledger.completion_permitted(&scope()));
    }

    /// The narrower window does not cover the debt, so it must not discharge
    /// it. Getting this wrong is silent loss with a proof record attached.
    #[test]
    fn a_non_covering_complete_walk_discharges_nothing() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(4),
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport {
                domain: time_domain(7, 60),
                outcome: bifrost_types::CoverageOutcome::Complete,
            },
            crate::cursor::WalkGeneration::for_test(5),
            200,
        );

        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );
        assert!(!ledger.completion_permitted(&scope()));
    }

    #[test]
    fn adjacent_complete_walks_discharge_debt_only_by_their_union() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(4),
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(7, 60)),
            crate::cursor::WalkGeneration::for_test(5),
            200,
        );
        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );

        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(60, 180)),
            crate::cursor::WalkGeneration::for_test(6),
            300,
        );
        assert!(matches!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .proof,
            ProofStatus::Discharged { .. }
        ));
    }

    /// A stale report must not reverse newer proof.
    #[test]
    fn an_older_generation_cannot_discharge_newer_debt() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(9),
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport {
                domain: CoverageDomain::full(scope()),
                outcome: bifrost_types::CoverageOutcome::Complete,
            },
            crate::cursor::WalkGeneration::for_test(3),
            200,
        );
        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );
    }

    /// The load-bearing separation: a waiver is accepted loss, not proof. It
    /// unblocks the sentinel, and the record still says nothing was ever
    /// proved - otherwise an audit cannot tell coverage from resignation.
    #[test]
    fn a_waiver_unblocks_completion_without_claiming_proof() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(1),
            100,
        );
        assert!(ledger.waive(&ObligationKey(b"a".to_vec()), "operator".into(), 500));

        let entry = ledger.entry(&ObligationKey(b"a".to_vec())).expect("entry");
        assert_eq!(
            entry.proof,
            ProofStatus::Unresolved,
            "a waiver proves nothing"
        );
        assert!(entry.policy.is_waived());
        assert!(!entry.blocks_completion());
        assert!(ledger.completion_permitted(&scope()));
    }

    /// Rediscovery reopens proof but must not touch policy: an operator's
    /// decision stands until the operator revokes it.
    #[test]
    fn rediscovery_reopens_proof_and_preserves_history() {
        let mut ledger = DebtLedger::new();
        let report =
            InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]);
        ledger.ingest(&report, crate::cursor::WalkGeneration::for_test(1), 100);
        ledger.block(&ObligationKey(b"a".to_vec()));
        ledger.ingest(&report, crate::cursor::WalkGeneration::for_test(2), 900);

        let entry = ledger.entry(&ObligationKey(b"a".to_vec())).expect("entry");
        assert_eq!(
            entry.first_seen_unix_seconds, 100,
            "re-raising must not reset history, or no budget is ever reachable"
        );
        assert_eq!(entry.policy, PolicyStatus::OperatorBlocked);
        assert!(entry.is_open());
    }

    /// A barrier is blocked progress, not debt behind an advanced cursor - so
    /// it never enters the entry table, but it does block completion and it
    /// does survive as something an operator can act on.
    #[test]
    fn a_barrier_blocks_completion_without_becoming_ledger_debt() {
        let mut ledger = DebtLedger::new();
        let barrier = InventoryObligation::Region {
            key: ObligationKey(b"page-7".to_vec()),
            failure_label: "unidentifiable-value".into(),
            error: error(),
            recovery: RegionRecovery::barrier(),
        };
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![barrier]),
            crate::cursor::WalkGeneration::for_test(1),
            100,
        );
        assert_eq!(
            ledger.entries().count(),
            0,
            "no checkpoint advanced past it, so there is no debt to hang off one"
        );

        ledger.record_barrier(BarrierIncident {
            key: ObligationKey(b"page-7".to_vec()),
            domain: CoverageDomain::full(scope()),
            generation: crate::cursor::WalkGeneration::for_test(1),
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });
        assert!(!ledger.completion_permitted(&scope()));
        assert!(!ledger.barrier_waived(&ObligationKey(b"page-7".to_vec())));

        ledger.waive(&ObligationKey(b"page-7".to_vec()), "operator".into(), 500);
        assert!(ledger.barrier_waived(&ObligationKey(b"page-7".to_vec())));
        assert!(
            ledger.completion_permitted(&scope()),
            "a waived barrier releases the scope; that is the only escape hatch it has"
        );
    }

    #[test]
    fn a_cumulative_report_keeps_an_already_crossed_barrier_waived() {
        let mut ledger = DebtLedger::new();
        let key = ObligationKey(b"page-7".to_vec());
        let report = InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![InventoryObligation::Region {
                key: key.clone(),
                failure_label: "unidentifiable-value".into(),
                error: error(),
                recovery: RegionRecovery::barrier(),
            }],
        );
        ledger.record_barrier(BarrierIncident {
            key: key.clone(),
            domain: report.domain.clone(),
            generation: crate::cursor::WalkGeneration::for_test(1),
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });
        assert!(ledger.waive(&key, "operator".into(), 500));

        assert!(ledger.cross_waived_barriers(
            &report,
            crate::cursor::WalkGeneration::for_test(2),
            600
        ));
        assert!(
            ledger.cross_waived_barriers(&report, crate::cursor::WalkGeneration::for_test(2), 600),
            "a cumulative Done report must not recreate the barrier crossed by its batch"
        );
        assert!(
            ledger
                .entry(&key)
                .expect("accepted-loss entry")
                .policy
                .is_waived()
        );
    }

    #[test]
    fn an_operator_can_block_a_barrier_without_accepting_its_loss() {
        let mut ledger = DebtLedger::new();
        let key = ObligationKey(b"page-7".to_vec());
        ledger.record_barrier(BarrierIncident {
            key: key.clone(),
            domain: CoverageDomain::full(scope()),
            generation: crate::cursor::WalkGeneration::for_test(1),
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });

        assert!(ledger.block(&key));
        assert_eq!(
            ledger.barriers().next().expect("barrier").policy,
            PolicyStatus::OperatorBlocked
        );
        assert!(!ledger.completion_permitted(&scope()));
    }

    /// The rescan park is keyed on `OperatorBlocked` alone. A retrying or a
    /// waived barrier must NOT park the scope: retrying is the ordinary state
    /// a barrier is recorded in, so parking on it would stop every
    /// barrier-stopped scope from ever re-walking, and a waived one is
    /// precisely the barrier the walk is now allowed to cross.
    #[test]
    fn only_an_operator_block_parks_a_scopes_rescan() {
        let mut ledger = DebtLedger::new();
        let key = ObligationKey(b"page-7".to_vec());
        ledger.record_barrier(BarrierIncident {
            key: key.clone(),
            domain: CoverageDomain::full(scope()),
            generation: crate::cursor::WalkGeneration::for_test(1),
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });
        assert!(
            !ledger.scope_has_blocked_barrier(&scope()),
            "a retrying barrier must keep the rescan running"
        );

        assert!(ledger.waive(&key, "operator".into(), 500));
        assert!(
            !ledger.scope_has_blocked_barrier(&scope()),
            "a waived barrier is crossable, not a park"
        );

        assert!(ledger.block(&key));
        assert!(ledger.scope_has_blocked_barrier(&scope()));

        let other = bifrost_types::CursorScope::Type(bifrost_types::ObjectType::CalendarEvent);
        assert!(
            !ledger.scope_has_blocked_barrier(&other),
            "one scope's block must not park a sibling scope"
        );
    }

    fn region(key: &str, recovery: RegionRecovery) -> InventoryObligation {
        InventoryObligation::Region {
            key: ObligationKey(key.as_bytes().to_vec()),
            failure_label: "truncated".into(),
            error: error(),
            recovery,
        }
    }

    fn replayable(key: &str) -> InventoryObligation {
        region(
            key,
            RegionRecovery::DurableReplay {
                token: b"tok".to_vec(),
            },
        )
    }

    fn ingest_one(
        ledger: &mut DebtLedger,
        obligation: InventoryObligation,
        generation: crate::cursor::WalkGeneration,
    ) {
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![obligation]),
            generation,
            100,
        );
    }

    /// A barrier has no repair descriptor, so nothing may try to repair it. Its
    /// only terminal state is an operator waiver.
    #[test]
    fn only_obligations_with_a_repair_descriptor_are_repairable() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            object("a"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            replayable("r"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            region("b", RegionRecovery::barrier()),
            crate::cursor::WalkGeneration::for_test(1),
        );
        assert_eq!(ledger.repairable().count(), 2);
    }

    /// A waived or blocked obligation is not retried automatically.
    #[test]
    fn waived_and_blocked_obligations_are_not_repairable() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            object("a"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            object("b"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ledger.waive(&ObligationKey(b"a".to_vec()), "operator".into(), 1);
        ledger.block(&ObligationKey(b"b".to_vec()));
        assert_eq!(ledger.repairable().count(), 0);
    }

    /// A spent budget stops automatic work and NOTHING else. It must never
    /// reach Waived or Discharged: a counter running out is evidence that
    /// retrying is not helping, not a decision about acceptable loss.
    #[test]
    fn an_expired_budget_blocks_rather_than_abandons() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            object("a"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let key = ObligationKey(b"a".to_vec());

        assert!(!ledger.record_attempt(&key, 3));
        assert!(!ledger.record_attempt(&key, 3));
        assert!(
            ledger.record_attempt(&key, 3),
            "the third attempt exhausts it"
        );

        let entry = ledger.entry(&key).expect("entry");
        assert_eq!(entry.policy, PolicyStatus::OperatorBlocked);
        assert!(entry.is_open(), "a spent budget proves nothing");
        assert!(
            entry.blocks_completion(),
            "blocked is not waived - it must still hold the sentinel"
        );
    }

    /// The budget follows the LINEAGE. An account that answers every pass by
    /// replacing an obligation with an equivalent one under a fresh key would
    /// otherwise never exhaust a budget.
    ///
    /// Rewritten when the budget moved off the discharged root into the
    /// per-lineage table: the rule is unchanged, only where the count lives.
    #[test]
    fn attempts_accrue_at_the_lineage_root() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let parent = ObligationKey(b"parent".to_vec());
        ledger.record_attempt(&parent, 10);

        ledger
            .replace_obligation(
                &parent,
                &[],
                &[replayable("child")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        let child = ObligationKey(b"child".to_vec());
        assert_eq!(ledger.lineage_root(&child), parent);

        // An attempt charged against the child lands on the lineage's budget.
        ledger.record_attempt(&child, 10);
        assert_eq!(
            ledger.lineages().get(&parent).map(|row| row.attempts),
            Some(2),
            "both attempts must accrue to the one lineage"
        );
        assert_eq!(
            ledger.entry(&child).expect("child").policy,
            PolicyStatus::Retrying { attempts: 2 },
            "the open member shows the lineage's count"
        );
    }

    /// The defect the per-lineage budget closed: expiry used to block only the
    /// discharged root while the open child kept its own `Retrying` policy, so
    /// `repairable` went on planning it and the cap never bound after the
    /// first split.
    #[test]
    fn an_expired_lineage_stops_repairing_its_open_members() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let parent = ObligationKey(b"parent".to_vec());
        ledger
            .replace_obligation(
                &parent,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        let a = ObligationKey(b"a".to_vec());
        let b = ObligationKey(b"b".to_vec());

        assert!(!ledger.record_attempt(&a, 2));
        assert!(ledger.record_attempt(&b, 2), "the second charge spends it");

        assert_eq!(
            ledger.repairable().count(),
            0,
            "no member of a spent lineage may be planned again"
        );
        for key in [&a, &b] {
            let entry = ledger.entry(key).expect("member");
            assert_eq!(entry.policy, PolicyStatus::OperatorBlocked);
            assert!(entry.is_open(), "a spent budget proves nothing");
        }
    }

    /// Exhaustion is sticky: a sibling that was closed when the budget ran out
    /// and is later rediscovered rejoins the SPENT lineage, blocked, rather
    /// than coming back `Retrying`.
    #[test]
    fn a_member_reopened_into_a_spent_lineage_comes_back_blocked() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let parent = ObligationKey(b"parent".to_vec());
        ledger
            .replace_obligation(
                &parent,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        let a = ObligationKey(b"a".to_vec());
        let b = ObligationKey(b"b".to_vec());
        assert!(ledger.discharge_repaired(
            &b,
            crate::cursor::WalkGeneration::for_test(1),
            DischargeEvidence::ProvedIrrelevant {
                detail: "covered".into(),
            },
        ));
        assert!(ledger.record_attempt(&a, 1));

        ingest_one(
            &mut ledger,
            replayable("b"),
            crate::cursor::WalkGeneration::for_test(2),
        );
        assert_eq!(
            ledger.entry(&b).expect("reopened").policy,
            PolicyStatus::OperatorBlocked
        );
        assert!(
            !ledger.record_attempt(&a, 100),
            "a larger budget on a later charge does not reopen a spent lineage"
        );
        assert_eq!(ledger.repairable().count(), 0);
    }

    /// Expiry never overwrites an operator decision on a member.
    #[test]
    fn expiry_leaves_a_waived_member_waived() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let parent = ObligationKey(b"parent".to_vec());
        ledger
            .replace_obligation(
                &parent,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        let a = ObligationKey(b"a".to_vec());
        let b = ObligationKey(b"b".to_vec());
        assert!(ledger.waive(&a, "op".into(), 500));
        assert!(ledger.record_attempt(&b, 1));

        assert!(ledger.entry(&a).expect("a").policy.is_waived());
        assert_eq!(
            ledger.entry(&b).expect("b").policy,
            PolicyStatus::OperatorBlocked
        );
    }

    /// A split that arrives after the operator blocked the parent must not
    /// move the debt onto fresh `Retrying` children and discharge the only
    /// entry carrying the block.
    #[test]
    fn a_replacement_against_an_operator_decision_is_refused() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let parent = ObligationKey(b"parent".to_vec());
        assert!(ledger.block(&parent));

        assert_eq!(
            ledger.replace_obligation(
                &parent,
                &[],
                &[replayable("child")],
                crate::cursor::WalkGeneration::for_test(1),
                200
            ),
            Err(ReplacementRefusal::PolicyNoLongerRetrying)
        );
        let entry = ledger.entry(&parent).expect("parent");
        assert!(entry.is_open());
        assert_eq!(entry.policy, PolicyStatus::OperatorBlocked);
        assert!(ledger.entry(&ObligationKey(b"child".to_vec())).is_none());
    }

    /// A child named after its own parent or lineage root would mutate the
    /// parent in place and then discharge it, or name itself as its root.
    #[test]
    fn a_child_reusing_a_lineage_key_is_refused() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("root"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let root = ObligationKey(b"root".to_vec());
        assert_eq!(
            ledger.replace_obligation(
                &root,
                &[],
                &[replayable("root")],
                crate::cursor::WalkGeneration::for_test(1),
                200
            ),
            Err(ReplacementRefusal::ChildReusesLineageKey)
        );

        ledger
            .replace_obligation(
                &root,
                &[],
                &[replayable("a")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        let a = ObligationKey(b"a".to_vec());
        assert_eq!(
            ledger.replace_obligation(
                &a,
                &[],
                &[replayable("a")],
                crate::cursor::WalkGeneration::for_test(1),
                200
            ),
            Err(ReplacementRefusal::ChildReusesLineageKey)
        );
        assert_eq!(
            ledger.replace_obligation(
                &a,
                &[],
                &[replayable("root")],
                crate::cursor::WalkGeneration::for_test(1),
                200
            ),
            Err(ReplacementRefusal::ChildReusesLineageKey)
        );
        assert!(ledger.entry(&a).expect("a").is_open());
    }

    /// A child whose key is another live lineage's id - here a discharged
    /// root whose own child is still open - would give that child a parent
    /// that itself has a parent.
    #[test]
    fn a_child_naming_another_live_lineage_is_refused() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("x"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            replayable("r"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ledger
            .replace_obligation(
                &ObligationKey(b"x".to_vec()),
                &[],
                &[replayable("y")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");

        assert_eq!(
            ledger.replace_obligation(
                &ObligationKey(b"r".to_vec()),
                &[],
                &[replayable("x")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            ),
            Err(ReplacementRefusal::ChildCollidesWithForeignLineage)
        );
        ledger.validate_lineages().expect("still flat");
    }

    /// A compacted lineage ROOT re-raised while its lineage still has a
    /// retained member rejoins that lineage's budget rather than starting
    /// fresh - the conservative direction, and the pre-compaction behaviour.
    #[test]
    fn a_compacted_root_re_raised_under_a_live_lineage_rejoins_it() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("root"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let root = ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(
                &root,
                &[],
                &[replayable("child")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        assert!(ledger.record_attempt(&ObligationKey(b"child".to_vec()), 1));
        assert_eq!(ledger.compact_discharged(), 1, "the root folds");

        ingest_one(
            &mut ledger,
            replayable("root"),
            crate::cursor::WalkGeneration::for_test(2),
        );
        assert_eq!(
            ledger.entry(&root).expect("re-raised").policy,
            PolicyStatus::OperatorBlocked,
            "the spent lineage is not reset by folding and re-raising its root"
        );
    }

    /// A discharged entry of another lineage is refused as a child too:
    /// reopening it would carry that lineage's operator decision onto this
    /// residual without anyone deciding it.
    #[test]
    fn a_child_naming_a_discharged_foreign_entry_is_refused() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("x"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            replayable("r"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ledger
            .replace_obligation(
                &ObligationKey(b"x".to_vec()),
                &[],
                &[replayable("w")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        let w = ObligationKey(b"w".to_vec());
        assert!(ledger.waive(&w, "op".into(), 500));
        assert!(ledger.discharge_repaired(
            &w,
            crate::cursor::WalkGeneration::for_test(1),
            DischargeEvidence::ProvedIrrelevant {
                detail: "covered".into(),
            },
        ));

        assert_eq!(
            ledger.replace_obligation(
                &ObligationKey(b"r".to_vec()),
                &[],
                &[replayable("w")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            ),
            Err(ReplacementRefusal::ChildCollidesWithForeignLineage)
        );
        assert!(!ledger.entry(&w).expect("w").is_open());
    }

    /// Replacement is a SWAP. Leaving the parent open alongside its children
    /// double-counts the extent and replays the parent forever.
    #[test]
    fn replacement_closes_the_parent() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let parent = ObligationKey(b"parent".to_vec());

        ledger
            .replace_obligation(
                &parent,
                &[],
                &[object("child")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");

        assert!(!ledger.entry(&parent).expect("parent retained").is_open());
        assert!(
            ledger
                .entry(&ObligationKey(b"child".to_vec()))
                .expect("child")
                .is_open()
        );
        assert_eq!(ledger.open_debt().count(), 1);
    }

    /// Turning one opaque region into addressable objects is real progress even
    /// though the unresolved extent is unchanged - each piece is now
    /// independently retryable. Extent equality alone would call this a stall.
    #[test]
    fn a_region_becoming_objects_counts_as_progress() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let progress = ledger
            .replace_obligation(
                &ObligationKey(b"parent".to_vec()),
                &[],
                &[object("c1"), object("c2")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        assert_eq!(progress, ReplacementProgress::Progressed);
    }

    /// Repartitioning a region into equally opaque regions, recovering nothing
    /// and proving nothing, is not progress. Left uncharged it is an infinite
    /// loop with fresh keys each pass.
    #[test]
    fn repartitioning_into_equally_opaque_regions_is_a_stall() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let progress = ledger
            .replace_obligation(
                &ObligationKey(b"parent".to_vec()),
                &[],
                &[replayable("r1"), replayable("r2")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        assert_eq!(progress, ReplacementProgress::Stalled);
    }

    /// A result computed against an older view of an obligation must not close
    /// a version of it that has since been re-raised. Serializing through one
    /// writer orders the messages; it does not reject stale intent.
    #[test]
    fn a_stale_repair_result_cannot_discharge_a_reopened_obligation() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            object("a"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        // Rediscovered by a later walk: the obligation moves to generation 5.
        ingest_one(
            &mut ledger,
            object("a"),
            crate::cursor::WalkGeneration::for_test(5),
        );

        let discharged = ledger.discharge_repaired(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(1),
            DischargeEvidence::RepairedAndPublished {
                attempt: bifrost_types::RepairAttemptId(1),
            },
        );
        assert!(
            !discharged,
            "a generation-1 result must not close generation 5"
        );
        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );
    }

    /// A child already open under a different root would give one obligation
    /// two lineages, and a budget could then be reset by picking whichever root
    /// is convenient.
    #[test]
    fn a_child_colliding_with_a_foreign_lineage_is_refused() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent-a"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            replayable("parent-b"),
            crate::cursor::WalkGeneration::for_test(1),
        );

        let refusal = ledger.replace_obligation(
            &ObligationKey(b"parent-a".to_vec()),
            &[],
            &[replayable("parent-b")],
            crate::cursor::WalkGeneration::for_test(1),
            200,
        );
        assert_eq!(
            refusal,
            Err(ReplacementRefusal::ChildCollidesWithForeignLineage)
        );
    }

    /// The ruling's line, in one test. `Discharged` is terminal, so it folds
    /// into a count and a root; `Unresolved` is not terminal even when waived,
    /// so it stays a live entry with its key intact - a later covering walk can
    /// still discharge it and an operator may still need to act on it.
    #[test]
    fn compaction_folds_the_proved_side_and_leaves_every_waived_entry_whole() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            object("proved"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            object("waived"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ingest_one(
            &mut ledger,
            object("owed"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        assert!(ledger.waive(&ObligationKey(b"waived".to_vec()), "op".into(), 500));
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"proved".to_vec()),
            crate::cursor::WalkGeneration::for_test(1),
            DischargeEvidence::RepairedAndPublished {
                attempt: bifrost_types::RepairAttemptId(7),
            },
        ));

        assert_eq!(ledger.compact_discharged(), 1);
        assert!(
            ledger.entry(&ObligationKey(b"proved".to_vec())).is_none(),
            "a terminal entry does not survive as an entry"
        );

        let waived = ledger
            .entry(&ObligationKey(b"waived".to_vec()))
            .expect("a waived entry is never compacted");
        assert_eq!(waived.key, ObligationKey(b"waived".to_vec()));
        assert_eq!(
            waived.proof,
            ProofStatus::Unresolved,
            "a waiver proves nothing, so it is not terminal and cannot compact"
        );
        assert!(waived.policy.is_waived());
        assert!(
            ledger.entry(&ObligationKey(b"owed".to_vec())).is_some(),
            "open debt is untouched"
        );

        let audit = ledger.discharge_audit(&scope()).expect("audit recorded");
        assert_eq!(audit.count, 1);
        assert_eq!(
            audit.latest_generation,
            crate::cursor::WalkGeneration::for_test(1)
        );
    }

    /// PRESERVATION CHECK, not a demonstration of a defect: replace
    /// `compact_discharged` with a no-op and this still passes, because the
    /// waived entry was never a fold candidate. It pins the rule that a waived
    /// entry keeps its key - which is what a later covering walk needs - and it
    /// earns its place on that basis alone. The tests that bite the compaction
    /// logic itself are the lineage ones below.
    #[test]
    fn a_covering_walk_still_discharges_a_waived_entry_after_compaction() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("w")]),
            crate::cursor::WalkGeneration::for_test(4),
            100,
        );
        assert!(ledger.waive(&ObligationKey(b"w".to_vec()), "op".into(), 500));
        ledger.compact_discharged();

        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(0, 180)),
            crate::cursor::WalkGeneration::for_test(5),
            200,
        );
        assert!(matches!(
            ledger
                .entry(&ObligationKey(b"w".to_vec()))
                .expect("entry")
                .proof,
            ProofStatus::Discharged { .. }
        ));
    }

    /// A discharged lineage root is no longer load-bearing: the budget lives in
    /// the lineage table, so compaction folds the root while its child is
    /// still open and the child's charges still reach the cap.
    ///
    /// Rewritten from the pin-era test, which asserted the root SURVIVED
    /// compaction and carried the counter; the rule it pinned (the cap binds
    /// through a live child) is unchanged.
    #[test]
    fn a_discharged_lineage_root_folds_while_its_child_keeps_the_budget() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let parent = ObligationKey(b"parent".to_vec());
        let child = ObligationKey(b"child".to_vec());
        ledger.record_attempt(&parent, 2);
        ledger
            .replace_obligation(
                &parent,
                &[],
                &[replayable("child")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");

        assert_eq!(ledger.compact_discharged(), 1, "the root folds");
        assert!(ledger.entry(&parent).is_none());
        assert_eq!(ledger.lineage_root(&child), parent);

        assert!(
            ledger.record_attempt(&child, 2),
            "the root's earlier charge still counts toward the cap"
        );
        assert_eq!(
            ledger.entry(&child).expect("child").policy,
            PolicyStatus::OperatorBlocked
        );
    }

    /// Once the child closes too, nothing names the root and both fold.
    #[test]
    fn a_lineage_folds_entirely_once_no_open_entry_names_the_root() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("parent"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        ledger
            .replace_obligation(
                &ObligationKey(b"parent".to_vec()),
                &[],
                &[replayable("child")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"child".to_vec()),
            crate::cursor::WalkGeneration::for_test(1),
            DischargeEvidence::ProvedIrrelevant {
                detail: "not owed".into(),
            },
        ));

        assert_eq!(ledger.compact_discharged(), 2);
        assert_eq!(ledger.entries().count(), 0);
        assert_eq!(ledger.discharge_audit(&scope()).expect("audit").count, 2);
    }

    /// What the root PROMISES, and no more: fold a candidate history
    /// independently and the two agree regardless of order, and a history of
    /// the same SIZE over different keys is distinguished. Detection, not proof
    /// of exact history - a sum of digests can in principle collide, and the
    /// doc says so.
    #[test]
    fn audit_fold_is_order_independent_and_distinguishes_sample_histories() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            object("a"),
            crate::cursor::WalkGeneration::for_test(3),
        );
        ingest_one(
            &mut ledger,
            object("b"),
            crate::cursor::WalkGeneration::for_test(3),
        );
        let evidence = || DischargeEvidence::ProvedIrrelevant {
            detail: "not owed".into(),
        };
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            evidence()
        ));
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"b".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            evidence()
        ));
        assert_eq!(ledger.compact_discharged(), 2);
        let audit = *ledger.discharge_audit(&scope()).expect("audit");

        let mut rebuilt = super::DischargeAudit::default();
        rebuilt.fold(
            &ObligationKey(b"b".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            &evidence(),
        );
        rebuilt.fold(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            &evidence(),
        );
        assert_eq!(rebuilt, audit, "the fold is order-independent");

        let mut wrong = super::DischargeAudit::default();
        wrong.fold(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            &evidence(),
        );
        wrong.fold(
            &ObligationKey(b"c".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            &evidence(),
        );
        assert_eq!(wrong.count, audit.count, "same size, different history");
        assert_ne!(wrong.root, audit.root, "the root must see the difference");
    }

    /// The same key discharged, folded, rediscovered and folded again must
    /// count twice. Under an XOR fold the second would cancel the first and the
    /// root would claim the obligation was never discharged at all.
    #[test]
    fn folding_the_same_obligation_twice_does_not_cancel_it() {
        let evidence = DischargeEvidence::ProvedIrrelevant {
            detail: "not owed".into(),
        };
        let mut audit = super::DischargeAudit::default();
        audit.fold(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            &evidence,
        );
        let once = audit.root;
        audit.fold(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(3),
            &evidence,
        );
        assert_eq!(audit.count, 2);
        assert_ne!(audit.root, once);
        assert_ne!(
            audit.root, [0u8; 32],
            "an additive fold must not cancel a repeated obligation back to the empty root"
        );
    }

    /// The concrete collision the additive FNV-1a root admitted, kept as a
    /// regression: two histories of two ordinary short keys, same size, same
    /// generation, same evidence kind, folding to one identical root. Nobody
    /// chose these keys adversarially - they are two-byte decimal strings - so
    /// this was weakness in the construction, not the unavoidable fact that
    /// finite hashes collide.
    #[test]
    fn the_short_key_collision_that_sank_the_additive_fnv_root_is_gone() {
        let evidence = || DischargeEvidence::ProvedIrrelevant {
            detail: "not owed".into(),
        };
        let fold = |left: &[u8], right: &[u8]| {
            let mut audit = super::DischargeAudit::default();
            audit.fold(
                &ObligationKey(left.to_vec()),
                crate::cursor::WalkGeneration::for_test(3),
                &evidence(),
            );
            audit.fold(
                &ObligationKey(right.to_vec()),
                crate::cursor::WalkGeneration::for_test(3),
                &evidence(),
            );
            audit
        };
        let one = fold(b"00", b"05");
        let other = fold(b"01", b"04");
        assert_eq!(one.count, other.count, "same size, different history");
        assert_eq!(one.latest_generation, other.latest_generation);
        assert_ne!(
            one.root, other.root,
            "these two folded to the same 128-bit FNV sum, which is what made \
             the exactness claim false"
        );
    }

    fn lineage_entry(key: &str, parent: Option<&str>, discharged: bool) -> super::LedgerEntry {
        super::LedgerEntry {
            key: ObligationKey(key.as_bytes().to_vec()),
            domain: CoverageDomain::full(scope()),
            generation: crate::cursor::WalkGeneration::for_test(1),
            proof: if discharged {
                ProofStatus::Discharged {
                    evidence: DischargeEvidence::ReplacedByChildren {
                        children: Vec::new(),
                    },
                }
            } else {
                ProofStatus::Unresolved
            },
            policy: PolicyStatus::Retrying { attempts: 0 },
            target: None,
            parent: parent.map(|key| ObligationKey(key.as_bytes().to_vec())),
            first_seen_unix_seconds: 100,
            last_error: error(),
        }
    }

    fn ledger_of(entries: Vec<super::LedgerEntry>, lineages: &[(&str, u32, bool)]) -> DebtLedger {
        let mut map = std::collections::BTreeMap::new();
        for entry in entries {
            map.insert(entry.key.clone(), entry);
        }
        let lineages = lineages
            .iter()
            .map(|(key, attempts, exhausted)| {
                (
                    ObligationKey(key.as_bytes().to_vec()),
                    super::LineageBudget {
                        attempts: *attempts,
                        exhausted: *exhausted,
                    },
                )
            })
            .collect();
        DebtLedger::from_parts(
            map,
            std::collections::BTreeMap::new(),
            Vec::new(),
            Vec::new(),
            lineages,
        )
    }

    /// Lineages are flat by construction, and restored state must be too: a
    /// chain (or a cycle, which is a chain that closes) is refused rather than
    /// resolved by guessing which ancestor is the root.
    #[test]
    fn a_restored_chain_or_self_parent_fails_validation() {
        let chain = ledger_of(
            vec![
                lineage_entry("root", None, true),
                lineage_entry("middle", Some("root"), true),
                lineage_entry("leaf", Some("middle"), false),
            ],
            &[("middle", 0, false)],
        );
        assert!(chain.validate_lineages().is_err());

        let cycle = ledger_of(
            vec![
                lineage_entry("a", Some("b"), false),
                lineage_entry("b", Some("a"), false),
            ],
            &[("a", 0, false), ("b", 0, false)],
        );
        assert!(cycle.validate_lineages().is_err());

        let own = ledger_of(
            vec![lineage_entry("a", Some("a"), false)],
            &[("a", 0, false)],
        );
        assert!(own.validate_lineages().is_err());
    }

    /// Every other lineage invariant, each broken once. A parent naming an
    /// absent key is the one shape that looks odd and is legal: its root was
    /// compacted.
    #[test]
    fn restored_lineage_rows_must_match_the_open_entries() {
        let open_child = || vec![lineage_entry("leaf", Some("gone"), false)];

        assert!(
            ledger_of(open_child(), &[("gone", 0, false)])
                .validate_lineages()
                .is_ok(),
            "a folded root is a legal lineage id"
        );
        assert!(
            ledger_of(open_child(), &[]).validate_lineages().is_err(),
            "an open member needs its budget row"
        );
        assert!(
            ledger_of(open_child(), &[("gone", 0, false), ("orphan", 0, false)])
                .validate_lineages()
                .is_err(),
            "a row with no retained member is stranded"
        );
        assert!(
            ledger_of(open_child(), &[("gone", 3, false)])
                .validate_lineages()
                .is_err(),
            "a retrying member must mirror its row"
        );
        assert!(
            ledger_of(open_child(), &[("gone", 0, true)])
                .validate_lineages()
                .is_err(),
            "no member of an exhausted lineage may still be retrying"
        );
    }

    /// A repair result for a key that a covering proof discharged while the
    /// attempt was in flight charges NOTHING, whether or not the entry has
    /// been folded since. Only an open entry is charged: a key with no open
    /// entry is never planned again, so no provider can collect free attempts
    /// against it, and its surviving sibling is charged by its own results.
    ///
    /// Inverted from the pin-era test, which required this charge to reach
    /// the lineage through a folded parent link. The owner's ruling that
    /// moved the budget off discharged entries also made `record_attempt` skip
    /// non-open ones, and this is that rule observed at its sharpest.
    #[test]
    fn a_result_for_a_key_discharged_in_flight_charges_nothing() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("root"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let root = ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(
                &root,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        let a = ObligationKey(b"a".to_vec());
        assert!(ledger.discharge_repaired(
            &a,
            crate::cursor::WalkGeneration::for_test(1),
            DischargeEvidence::ProvedIrrelevant {
                detail: "covered".into(),
            },
        ));

        assert!(!ledger.record_attempt(&a, 1), "discharged, not yet folded");
        assert_eq!(ledger.compact_discharged(), 2, "root and `a` fold");
        assert!(!ledger.record_attempt(&a, 1), "folded");
        assert_eq!(
            ledger
                .entry(&ObligationKey(b"b".to_vec()))
                .expect("b")
                .policy,
            PolicyStatus::Retrying { attempts: 0 },
            "the sibling's lineage was not charged"
        );
    }

    /// A lineage's budget row lives exactly while the ledger retains a member
    /// of it. Discharging every member keeps the row - a retained member can
    /// be rediscovered - and compaction folding the last one drops it, so the
    /// table stays bounded by the entry map.
    #[test]
    fn a_lineage_budget_lives_while_a_member_is_retained() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("root"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let root = ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(
                &root,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        ledger.record_attempt(&ObligationKey(b"a".to_vec()), 10);
        let irrelevant = || DischargeEvidence::ProvedIrrelevant {
            detail: "covered".into(),
        };
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(1),
            irrelevant()
        ));
        assert!(
            ledger.lineages().contains_key(&root),
            "`b` is still open, so the budget stays"
        );

        assert!(ledger.discharge_repaired(
            &ObligationKey(b"b".to_vec()),
            crate::cursor::WalkGeneration::for_test(1),
            irrelevant()
        ));
        assert!(
            ledger.lineages().contains_key(&root),
            "every member is discharged but retained, so the budget stays"
        );
        ledger.validate_lineages().expect("settled state is valid");

        assert_eq!(ledger.compact_discharged(), 3);
        assert!(ledger.lineages().is_empty());
        ledger.validate_lineages().expect("settled state is valid");
    }

    /// Rediscovery keeps history, budget included. A provider alternating a
    /// covering walk with a rediscovery of the same retained obligation must
    /// not get a fresh budget each time, or no budget is ever reachable.
    #[test]
    fn a_discharged_obligation_rediscovered_keeps_its_budget() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(4),
            100,
        );
        let a = ObligationKey(b"a".to_vec());
        assert!(!ledger.record_attempt(&a, 3));
        assert!(!ledger.record_attempt(&a, 3));

        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(0, 180)),
            crate::cursor::WalkGeneration::for_test(5),
            200,
        );
        assert!(!ledger.entry(&a).expect("retained").is_open());

        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            crate::cursor::WalkGeneration::for_test(6),
            300,
        );
        assert_eq!(
            ledger.entry(&a).expect("reopened").policy,
            PolicyStatus::Retrying { attempts: 2 }
        );
        assert!(
            ledger.record_attempt(&a, 3),
            "the third charge exhausts it, across the discharge"
        );
    }

    /// A rediscovered CHILD key is a FRESH gap: no parent, budget from zero. A
    /// re-raise must not inherit the lineage it was folded out of and charge
    /// an old root for a new gap.
    #[test]
    fn a_rediscovered_key_does_not_inherit_its_folded_lineage() {
        let mut ledger = DebtLedger::new();
        ingest_one(
            &mut ledger,
            replayable("root"),
            crate::cursor::WalkGeneration::for_test(1),
        );
        let root = ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(
                &root,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"a".to_vec()),
            crate::cursor::WalkGeneration::for_test(1),
            DischargeEvidence::ProvedIrrelevant {
                detail: "covered".into(),
            },
        ));
        assert_eq!(ledger.compact_discharged(), 2, "root and `a` fold");

        ingest_one(
            &mut ledger,
            replayable("a"),
            crate::cursor::WalkGeneration::for_test(9),
        );
        let raised = ObligationKey(b"a".to_vec());
        assert_eq!(
            ledger.lineage_root(&raised),
            raised,
            "a re-raise starts its own lineage"
        );
        assert!(!ledger.record_attempt(&raised, 3), "and its own budget");
        assert_eq!(
            ledger.lineages().get(&root).map(|row| row.attempts),
            Some(0),
            "the old lineage must not be charged for a fresh gap"
        );
        assert_eq!(
            ledger.lineages().get(&raised).map(|row| row.attempts),
            Some(1)
        );
    }

    /// Compaction is not something a caller has to remember. An ingest that
    /// carries the entry map over the threshold folds terminal history on its
    /// own, which is what actually bounds the ledger.
    #[test]
    fn a_large_ingest_compacts_without_being_asked() {
        let mut ledger = DebtLedger::new();
        let obligations: Vec<_> = (0..super::COMPACTION_THRESHOLD)
            .map(|index| object(&format!("obj-{index}")))
            .collect();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), obligations),
            crate::cursor::WalkGeneration::for_test(4),
            100,
        );
        assert_eq!(ledger.entries().count(), super::COMPACTION_THRESHOLD);
        assert!(
            ledger.discharge_audit(&scope()).is_none(),
            "nothing is terminal yet, so nothing folds"
        );

        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(0, 180)),
            crate::cursor::WalkGeneration::for_test(5),
            200,
        );
        assert_eq!(
            ledger.entries().count(),
            0,
            "a covering walk discharges them and the same fold reclaims them"
        );
        assert_eq!(
            ledger.discharge_audit(&scope()).expect("audit").count,
            u64::try_from(super::COMPACTION_THRESHOLD).expect("threshold fits a u64")
        );
        assert!(ledger.completion_permitted(&scope()));
    }

    /// Hitting the same wall every attach must not accumulate incidents or
    /// reset the operator's decision about it.
    #[test]
    fn re_recording_a_barrier_is_idempotent_and_keeps_policy() {
        let mut ledger = DebtLedger::new();
        let incident = || BarrierIncident {
            key: ObligationKey(b"page-7".to_vec()),
            domain: CoverageDomain::full(scope()),
            generation: crate::cursor::WalkGeneration::for_test(1),
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        };
        ledger.record_barrier(incident());
        ledger.waive(&ObligationKey(b"page-7".to_vec()), "operator".into(), 500);
        ledger.record_barrier(incident());

        assert_eq!(ledger.barriers().count(), 1);
        assert!(
            ledger.barrier_waived(&ObligationKey(b"page-7".to_vec())),
            "re-hitting the wall must not silently revoke the waiver"
        );
    }
}
