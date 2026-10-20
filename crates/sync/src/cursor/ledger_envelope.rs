//! Durable serialization for the whole [`DebtLedger`].
//!
//! `CheckpointStore::apply_transition` takes a checkpoint and the account's
//! entire ledger as one atomic write. The checkpoint half has had
//! [`encode_envelope`](super::envelope::encode_envelope) since the store trait
//! existed; the ledger half had nothing, so a persistent backend could store
//! only one of the two things the atomicity contract is about. A consumer
//! forced to keep ledgers in a process-lifetime map forgets accepted debt on
//! restart, and a degraded scope comes back looking clean until something
//! re-enumerates it - which is the exact silent-loss shape the ledger exists to
//! prevent, arriving through the persistence layer instead of through a walk.
//!
//! This module is the missing half. It is maintained TOGETHER with
//! `envelope.rs` and copies its conventions on purpose: same header shape, same
//! little-endian length-prefixed primitives, same `Error::SchemaIncompatible`
//! classification for a version outside the readable window, and the same rule
//! about where a panic is allowed (encode only, never decode).
//!
//! # Wire format
//!
//! ```text
//!   1 byte    magic = 0xB6      (0xB5 is the cursor envelope)
//!   3 bytes   reserved (zero)
//!   4 bytes   version (little-endian u32)
//!   4 bytes   entry count       -> N ledger entries
//!   4 bytes   barrier count     -> N barrier incidents
//!   4 bytes   proof count       -> N (generation, CoverageDomain) pairs
//! ```
//!
//! Two further sections follow the proofs, in the payload rather than in the
//! header, each a `u32` count and that many rows:
//!
//! - `(CursorScope, DischargeAudit)` pairs, the compacted terminal history
//!   (since version 2);
//! - `(lineage key, attempts u32, exhausted u8)` rows, the per-lineage retry
//!   budgets (since version 3).
//!
//! The payload must be consumed exactly; trailing bytes are refused.
//!
//! Version 3 moved the retry budget off the discharged lineage-root entry and
//! into its own table. The repository owner ruled that no consumer had
//! persisted a ledger when it did, so it raised `MIN_MIGRATABLE_LEDGER` to
//! itself rather than synthesizing budgets
//! from the old per-entry counters - which, for a split lineage, disagreed
//! with each other (the root held the count, every child held zero).
//!
//! Version 4 added the `Internal` account error kind (tag 10) to the error
//! digest's vocabulary and changed nothing else, so a version-3 row is read as
//! is; a version-3 row carrying tag 10 is corrupt, because no version-3 writer
//! produced one.
//!
//! Version 5 adds Event resource (tag 11) and scope (tag 9) to the error
//! digest. Versions 3 and 4 remain readable without those tags.
//! Version 6 adds DirectoryGateway scope (tag 10); older versions reject it.
//!
//! A `BarrierIncident::resume_from` nests a whole cursor envelope through
//! `encode_envelope` / `decode_envelope` rather than re-deriving one: the
//! checkpoint codec owns that layout and its migration chain, and a second
//! decoder for the same bytes is a second thing to keep correct.
//!
//! # Why the error is a digest and not a reconstruction
//!
//! `LedgerEntry::last_error` and `BarrierIncident::evidence` are the one part
//! of the ledger that does not round-trip byte-for-byte, and the reason is
//! structural rather than expedient. `AccountError` carries a `Cause` chain
//! whose payloads include `&'static str` fields (`AccessCause::InsufficientScope
//! { needed }`, `RequestCause::InvalidArgument { field }`); a decoder holding
//! bytes cannot produce a `&'static str` without leaking, so an exact
//! reconstruction is not merely large, it is unrepresentable.
//!
//! So this codec persists the CLASSIFICATION exactly - `AccountErrorKind`,
//! `ErrorScope`, operation, provider, protocol and the full `DiagnosticInfo` -
//! and rebuilds the error through `AccountErrorBuilder` with the canonical
//! `Cause` its kind demands. The structured payloads of any secondary causes do
//! not survive as typed values.
//!
//! That loss is affordable because nothing decides anything on these fields.
//! Every ledger predicate - `blocks_completion`, `completion_permitted`,
//! `repairable`, `lineage_root`, `record_attempt`, discharge - reads proof,
//! policy, domain, generation, target, parent and the lineage budgets, never
//! the error; `LedgerEntry::target`
//! exists precisely so a repair descriptor is never taken from the error, whose
//! classifications and messages change between revisions. The error is operator
//! evidence, and operator evidence is what a digest preserves.
//!
//! What is NOT preserved, stated so a later reader does not assume otherwise:
//! secondary `Cause` payloads, `idempotency_override`, and `throttle_scope`.
//! The last two are overrides for deriving `RecoveryClass` on a live retry
//! decision, and a restored ledger error is never retried through - it is read
//! by an operator or an audit.

use std::collections::BTreeMap;

use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, Cause, CoverageCoordinate,
    CoverageDomain, CursorScope, DetailVisibility, DiagnosticText, ErrorScope, InternalCause,
    InternalErrorKind, MailboxUnavailableKind, ObjectId, ObligationKey, Protocol,
    ProtocolErrorKind, Provider, RequestCause, RequestErrorKind, ResourceKind, ServerCause,
    ServerErrorKind, SnapshotIdentity, StateCause, StrategyDowngrade, SyncStateErrorKind,
    TransportCause, TransportErrorKind, TransportKind,
};

use super::envelope::{
    decode_envelope, decode_scope, encode_envelope, encode_scope, read_bytes, read_string,
    write_bytes, write_string,
};
use super::ledger::{
    BarrierIncident, DebtLedger, DischargeAudit, DischargeEvidence, LedgerEntry, LineageBudget,
    PolicyStatus, ProofStatus,
};
use crate::error::Error;

/// Current ledger envelope version. Bumped whenever the layout below changes
/// in a way an older decoder would misread, including a new tag in a
/// vocabulary an older decoder would refuse.
pub const LEDGER_ENVELOPE_VERSION: u32 = 6;

/// Lowest ledger envelope version still readable by this engine.
///
/// A durable row outside `[MIN_MIGRATABLE_LEDGER, LEDGER_ENVELOPE_VERSION]`
/// decodes to `Error::SchemaIncompatible`, matching the cursor envelope: both
/// out-of-range directions describe the same situation, a durable row this
/// revision cannot read, and that is the only classification a consumer's
/// healing path can key on. An `Error::Other` would instead read as a store
/// failure and leave the unreadable row in place for every subsequent load.
pub const MIN_MIGRATABLE_LEDGER: u32 = 3;

const MAGIC: u8 = 0xB6;
const HEADER_LEN: usize = 20;

/// Serialize a whole `DebtLedger`.
///
/// # Panics
///
/// Panics on ledger state this revision cannot represent on disk: a
/// `#[non_exhaustive]` enum variant added since this codec was written, or a
/// `BarrierIncident::resume_from` holding a `Checkpoint` the cursor codec
/// refuses. Refusing loudly is the same rule `encode_envelope` follows and for
/// the same reason: the alternative is writing a durable row whose own decoder
/// rejects it, which strands the account instead of healing it. Every such
/// variant must teach this codec a tag before a ledger carrying it can be
/// stored.
#[must_use]
pub fn encode_ledger(ledger: &DebtLedger) -> Vec<u8> {
    let entries: Vec<&LedgerEntry> = ledger.entries().collect();
    let barriers: Vec<&BarrierIncident> = ledger.barriers().collect();
    let proved = ledger.proved();

    let mut out = Vec::new();
    out.push(MAGIC);
    out.extend_from_slice(&[0u8, 0, 0]);
    out.extend_from_slice(&LEDGER_ENVELOPE_VERSION.to_le_bytes());
    write_count(&mut out, entries.len(), "entries");
    write_count(&mut out, barriers.len(), "barriers");
    write_count(&mut out, proved.len(), "proofs");
    debug_assert_eq!(out.len(), HEADER_LEN);

    for entry in entries {
        encode_entry(&mut out, entry);
    }
    for barrier in barriers {
        encode_barrier(&mut out, barrier);
    }
    for (generation, domain) in proved {
        out.extend_from_slice(&generation.to_le_bytes());
        encode_domain(&mut out, domain);
    }

    let compacted = ledger.compacted();
    write_count(&mut out, compacted.len(), "discharge audits");
    for (scope, audit) in compacted {
        write_bytes(&mut out, &encode_scope(scope));
        out.extend_from_slice(&audit.count.to_le_bytes());
        // Fixed 32 bytes, written as the digest sum's own little-endian byte
        // order rather than through an integer type: the root is a 256-bit
        // value, and routing it through anything narrower is how a durable
        // record silently loses half of one.
        out.extend_from_slice(&audit.root);
        out.extend_from_slice(&audit.latest_generation.to_le_bytes());
    }

    let lineages = ledger.lineages();
    write_count(&mut out, lineages.len(), "lineage budgets");
    for (lineage, budget) in lineages {
        write_bytes(&mut out, &lineage.0);
        out.extend_from_slice(&budget.attempts.to_le_bytes());
        out.push(u8::from(budget.exhausted));
    }
    out
}

/// Decode a whole `DebtLedger`.
///
/// Errors:
/// - `Error::Other` for malformed bytes (bad magic, truncation, an unknown
///   tag this codec never wrote).
/// - `Error::SchemaIncompatible` for a version outside
///   `[MIN_MIGRATABLE_LEDGER, LEDGER_ENVELOPE_VERSION]`.
///
/// A version inside the window is MIGRATED, not merely accepted: whatever this
/// returns is current-shaped ledger state, exactly as `decode_envelope` returns
/// a `ChangeCursor` its own consumers will accept.
///
/// Decoded state is VALIDATED, never healed. A duplicated key, a lineage that
/// is not flat, an entry whose lineage has no budget row, a row with no
/// retained member, or a retrying entry disagreeing with its row is
/// `Error::Other`: repairing
/// it would mean choosing which half of a contradiction to believe, and a
/// guessed retry budget is exactly the silent reset budgets exist to stop.
pub fn decode_ledger(bytes: &[u8]) -> Result<DebtLedger, Error> {
    if bytes.len() < HEADER_LEN {
        return Err(Error::Other("ledger envelope: truncated header".into()));
    }
    if bytes[0] != MAGIC {
        return Err(Error::Other("ledger envelope: bad magic".into()));
    }
    if bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 {
        return Err(Error::Other(
            "ledger envelope: reserved header bytes non-zero".into(),
        ));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if !(MIN_MIGRATABLE_LEDGER..=LEDGER_ENVELOPE_VERSION).contains(&version) {
        return Err(Error::SchemaIncompatible);
    }

    let mut reader = Reader::new(&bytes[HEADER_LEN..], version);
    let entry_count = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let barrier_count = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
    let proof_count = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]) as usize;

    let mut entries = BTreeMap::new();
    for _ in 0..entry_count {
        let entry = decode_entry(&mut reader)?;
        if entries.insert(entry.key.clone(), entry).is_some() {
            return Err(duplicate("entry"));
        }
    }
    let mut barriers = BTreeMap::new();
    for _ in 0..barrier_count {
        let barrier = decode_barrier(&mut reader)?;
        if barriers.insert(barrier.key.clone(), barrier).is_some() {
            return Err(duplicate("barrier"));
        }
    }
    let mut proved = Vec::with_capacity(proof_count.min(1024));
    for _ in 0..proof_count {
        let generation = reader.u64()?;
        proved.push((generation, decode_domain(&mut reader)?));
    }

    let audit_count = reader.u32()? as usize;
    let mut compacted = Vec::with_capacity(audit_count.min(1024));
    for _ in 0..audit_count {
        let scope = decode_scope(&reader.bytes()?)?;
        compacted.push((
            scope,
            DischargeAudit {
                count: reader.u64()?,
                root: reader.digest32()?,
                latest_generation: reader.u64()?,
            },
        ));
    }

    let lineage_count = reader.u32()? as usize;
    let mut lineages = BTreeMap::new();
    for _ in 0..lineage_count {
        let lineage = ObligationKey(reader.bytes()?);
        let attempts = reader.u32()?;
        let exhausted = match reader.u8()? {
            0 => false,
            1 => true,
            other => return Err(unknown_tag("lineage exhausted flag", other)),
        };
        let budget = LineageBudget {
            attempts,
            exhausted,
        };
        if lineages.insert(lineage, budget).is_some() {
            return Err(duplicate("lineage budget"));
        }
    }

    if reader.at != reader.bytes.len() {
        return Err(Error::Other("ledger envelope: trailing bytes".into()));
    }

    let ledger = migrate_ledger(
        version,
        DebtLedger::from_parts(entries, barriers, proved, compacted, lineages),
    );
    ledger
        .validate_lineages()
        .map_err(|reason| Error::Other(format!("ledger envelope: {reason}")))?;
    Ok(ledger)
}

/// Bring a ledger decoded from an accepted historical layout up to the current
/// one.
///
/// A no-op while every version in the readable window shares one layout
/// (versions 4 through 6 only widened the error vocabulary). It exists as the
/// named boundary anyway, for the same reason `migrate_change_cursor` does:
/// the codec is the only place in the workspace that may hold ledger state in
/// an older shape, so a fixup written anywhere else spreads knowledge of a dead
/// layout into consumers that have no code to interpret it.
fn migrate_ledger(from_version: u32, ledger: DebtLedger) -> DebtLedger {
    let _ = from_version;
    ledger
}

fn duplicate(what: &str) -> Error {
    Error::Other(format!("ledger envelope: duplicate {what} key"))
}

// ---------- entries and barriers ----------

fn encode_entry(out: &mut Vec<u8>, entry: &LedgerEntry) {
    write_bytes(out, &entry.key.0);
    encode_domain(out, &entry.domain);
    out.extend_from_slice(&entry.generation.to_le_bytes());
    encode_proof(out, &entry.proof);
    encode_policy(out, &entry.policy);
    match &entry.target {
        None => out.push(0),
        Some(bifrost_types::InventoryRepairTarget::Object { id, repair }) => {
            out.push(1);
            write_string(out, &id.0);
            write_bytes(out, repair);
        }
        Some(bifrost_types::InventoryRepairTarget::Region { replay }) => {
            out.push(2);
            write_bytes(out, replay);
        }
        // `InventoryRepairTarget` is `#[non_exhaustive]`.
        Some(_) => panic!("ledger envelope: unknown repair target variant"),
    }
    match &entry.parent {
        None => out.push(0),
        Some(parent) => {
            out.push(1);
            write_bytes(out, &parent.0);
        }
    }
    out.extend_from_slice(&entry.first_seen_unix_seconds.to_le_bytes());
    encode_error(out, &entry.last_error);
}

fn decode_entry(reader: &mut Reader<'_>) -> Result<LedgerEntry, Error> {
    let key = ObligationKey(reader.bytes()?);
    let domain = decode_domain(reader)?;
    let generation = reader.u64()?;
    let proof = decode_proof(reader)?;
    let policy = decode_policy(reader)?;
    let target = match reader.u8()? {
        0 => None,
        1 => Some(bifrost_types::InventoryRepairTarget::Object {
            id: ObjectId(reader.string()?),
            repair: reader.bytes()?,
        }),
        2 => Some(bifrost_types::InventoryRepairTarget::Region {
            replay: reader.bytes()?,
        }),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown repair target tag {other}"
            )));
        }
    };
    let parent = match reader.u8()? {
        0 => None,
        1 => Some(ObligationKey(reader.bytes()?)),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown parent tag {other}"
            )));
        }
    };
    let first_seen_unix_seconds = reader.i64()?;
    let last_error = decode_error(reader)?;
    Ok(LedgerEntry {
        key,
        domain,
        generation,
        proof,
        policy,
        target,
        parent,
        first_seen_unix_seconds,
        last_error,
    })
}

fn encode_barrier(out: &mut Vec<u8>, barrier: &BarrierIncident) {
    write_bytes(out, &barrier.key.0);
    encode_domain(out, &barrier.domain);
    out.extend_from_slice(&barrier.generation.to_le_bytes());
    write_string(out, &barrier.failure_label);
    encode_error(out, &barrier.evidence);
    encode_policy(out, &barrier.policy);
    match &barrier.resume_from {
        None => out.push(0),
        Some(checkpoint) => {
            out.push(1);
            // Nested whole, through the checkpoint codec that owns the layout.
            // A second decoder for these bytes would be a second thing to keep
            // in step with the cursor envelope's migration chain.
            write_bytes(out, &encode_envelope(checkpoint));
        }
    }
}

fn decode_barrier(reader: &mut Reader<'_>) -> Result<BarrierIncident, Error> {
    let key = ObligationKey(reader.bytes()?);
    let domain = decode_domain(reader)?;
    let generation = reader.u64()?;
    let failure_label = reader.string()?;
    let evidence = decode_error(reader)?;
    let policy = decode_policy(reader)?;
    let resume_from = match reader.u8()? {
        0 => None,
        1 => Some(decode_envelope(&reader.bytes()?)?),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown resume_from tag {other}"
            )));
        }
    };
    Ok(BarrierIncident {
        key,
        domain,
        generation,
        failure_label,
        evidence,
        policy,
        resume_from,
    })
}

// ---------- proof / policy ----------

fn encode_proof(out: &mut Vec<u8>, proof: &ProofStatus) {
    match proof {
        ProofStatus::Unresolved => out.push(0),
        ProofStatus::Discharged { evidence } => {
            out.push(1);
            match evidence {
                DischargeEvidence::CoveringWalk { domain } => {
                    out.push(0);
                    encode_domain(out, domain);
                }
                DischargeEvidence::RepairedAndPublished { attempt } => {
                    out.push(1);
                    out.extend_from_slice(&attempt.0.to_le_bytes());
                }
                DischargeEvidence::ProvedIrrelevant { detail } => {
                    out.push(2);
                    write_string(out, detail);
                }
                DischargeEvidence::ReplacedByChildren { children } => {
                    out.push(3);
                    write_count(out, children.len(), "discharge children");
                    for child in children {
                        write_bytes(out, &child.0);
                    }
                }
            }
        }
    }
}

fn decode_proof(reader: &mut Reader<'_>) -> Result<ProofStatus, Error> {
    match reader.u8()? {
        0 => Ok(ProofStatus::Unresolved),
        1 => {
            let evidence = match reader.u8()? {
                0 => DischargeEvidence::CoveringWalk {
                    domain: decode_domain(reader)?,
                },
                1 => DischargeEvidence::RepairedAndPublished {
                    attempt: bifrost_types::RepairAttemptId(reader.u64()?),
                },
                2 => DischargeEvidence::ProvedIrrelevant {
                    detail: reader.string()?,
                },
                3 => {
                    let count = reader.u32()? as usize;
                    let mut children = Vec::with_capacity(count.min(1024));
                    for _ in 0..count {
                        children.push(ObligationKey(reader.bytes()?));
                    }
                    DischargeEvidence::ReplacedByChildren { children }
                }
                other => {
                    return Err(Error::Other(format!(
                        "ledger envelope: unknown discharge evidence tag {other}"
                    )));
                }
            };
            Ok(ProofStatus::Discharged { evidence })
        }
        other => Err(Error::Other(format!(
            "ledger envelope: unknown proof status tag {other}"
        ))),
    }
}

fn encode_policy(out: &mut Vec<u8>, policy: &PolicyStatus) {
    match policy {
        PolicyStatus::Retrying { attempts } => {
            out.push(0);
            out.extend_from_slice(&attempts.to_le_bytes());
        }
        PolicyStatus::OperatorBlocked => out.push(1),
        PolicyStatus::Waived {
            by,
            at_unix_seconds,
        } => {
            out.push(2);
            write_string(out, by);
            out.extend_from_slice(&at_unix_seconds.to_le_bytes());
        }
    }
}

fn decode_policy(reader: &mut Reader<'_>) -> Result<PolicyStatus, Error> {
    match reader.u8()? {
        0 => Ok(PolicyStatus::Retrying {
            attempts: reader.u32()?,
        }),
        1 => Ok(PolicyStatus::OperatorBlocked),
        2 => Ok(PolicyStatus::Waived {
            by: reader.string()?,
            at_unix_seconds: reader.i64()?,
        }),
        other => Err(Error::Other(format!(
            "ledger envelope: unknown policy status tag {other}"
        ))),
    }
}

// ---------- coverage domain ----------

fn encode_domain(out: &mut Vec<u8>, domain: &CoverageDomain) {
    // The scope goes in length-prefixed because `decode_scope` reads a
    // self-delimiting payload rather than reporting how much it consumed.
    write_bytes(out, &encode_scope(&domain.scope));
    match &domain.coordinate {
        CoverageCoordinate::Full => out.push(0),
        CoverageCoordinate::TimeRange {
            from_unix_seconds,
            to_unix_seconds,
        } => {
            out.push(1);
            write_opt_i64(out, *from_unix_seconds);
            write_opt_i64(out, *to_unix_seconds);
        }
        CoverageCoordinate::UidRange {
            uid_validity,
            from,
            to,
        } => {
            out.push(2);
            out.extend_from_slice(&uid_validity.to_le_bytes());
            out.extend_from_slice(&from.to_le_bytes());
            out.extend_from_slice(&to.to_le_bytes());
        }
        CoverageCoordinate::PageRange { from, to } => {
            out.push(3);
            out.extend_from_slice(&from.to_le_bytes());
            out.extend_from_slice(&to.to_le_bytes());
        }
        CoverageCoordinate::ProviderRegion { namespace, region } => {
            out.push(4);
            write_string(out, namespace);
            write_bytes(out, region);
        }
        // `CoverageCoordinate` is `#[non_exhaustive]`.
        _ => panic!("ledger envelope: unknown coverage coordinate variant"),
    }
    match &domain.snapshot.0 {
        None => out.push(0),
        Some(snapshot) => {
            out.push(1);
            write_bytes(out, snapshot);
        }
    }
}

fn decode_domain(reader: &mut Reader<'_>) -> Result<CoverageDomain, Error> {
    let scope = decode_scope(&reader.bytes()?)?;
    let coordinate = match reader.u8()? {
        0 => CoverageCoordinate::Full,
        1 => CoverageCoordinate::TimeRange {
            from_unix_seconds: read_opt_i64(reader)?,
            to_unix_seconds: read_opt_i64(reader)?,
        },
        2 => CoverageCoordinate::UidRange {
            uid_validity: reader.u32()?,
            from: reader.u64()?,
            to: reader.u64()?,
        },
        3 => CoverageCoordinate::PageRange {
            from: reader.u32()?,
            to: reader.u32()?,
        },
        4 => CoverageCoordinate::ProviderRegion {
            namespace: reader.string()?,
            region: reader.bytes()?,
        },
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown coverage coordinate tag {other}"
            )));
        }
    };
    let snapshot = match reader.u8()? {
        0 => SnapshotIdentity(None),
        1 => SnapshotIdentity(Some(reader.bytes()?)),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown snapshot tag {other}"
            )));
        }
    };
    Ok(CoverageDomain {
        scope,
        coordinate,
        snapshot,
    })
}

// ---------- account error digest ----------

fn encode_error(out: &mut Vec<u8>, error: &AccountError) {
    encode_error_kind(out, error.kind());
    match error.scope() {
        None => out.push(0),
        Some(scope) => {
            out.push(1);
            encode_error_scope(out, scope);
        }
    }
    match error.operation() {
        None => out.push(0),
        Some(operation) => {
            out.push(1);
            out.push(encode_operation(operation));
        }
    }
    match error.provider() {
        None => out.push(0),
        Some(provider) => {
            out.push(1);
            out.push(encode_provider(provider));
        }
    }
    match error.protocol() {
        None => out.push(0),
        Some(protocol) => {
            out.push(1);
            out.push(encode_protocol(protocol));
        }
    }

    // Diagnostics. `support_consented` is the widest tier that still returns
    // the free-form text, and `telemetry_fields` carries the bounded tokens;
    // together they are every diagnostic field the builder can put back.
    let consented = error.support_consented();
    let telemetry = &consented.telemetry;
    write_opt_u16(out, telemetry.status);
    write_opt_str(out, telemetry.request_id);
    write_opt_str(out, telemetry.trace_id);
    write_opt_str(out, telemetry.native_code);
    write_count(
        out,
        consented.user_safe_text.len() + consented.support_text.len(),
        "diagnostic text",
    );
    for text in &consented.user_safe_text {
        out.push(0);
        write_string(out, text);
    }
    for text in &consented.support_text {
        out.push(1);
        write_string(out, text);
    }
}

fn decode_error(reader: &mut Reader<'_>) -> Result<AccountError, Error> {
    let kind = decode_error_kind(reader)?;
    let mut scope = match reader.u8()? {
        0 => None,
        1 => Some(decode_error_scope(reader)?),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown error scope tag {other}"
            )));
        }
    };
    let operation = match reader.u8()? {
        0 => None,
        1 => Some(decode_operation(reader.u8()?)?),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown operation tag {other}"
            )));
        }
    };
    let provider = match reader.u8()? {
        0 => None,
        1 => Some(decode_provider(reader.u8()?)?),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown provider tag {other}"
            )));
        }
    };
    let protocol = match reader.u8()? {
        0 => None,
        1 => Some(decode_protocol(reader.u8()?)?),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown protocol tag {other}"
            )));
        }
    };
    let status = read_opt_u16(reader)?;
    let request_id = read_opt_string(reader)?;
    let trace_id = read_opt_string(reader)?;
    let native_code = read_opt_string(reader)?;
    let text_count = reader.u32()? as usize;
    let mut text = Vec::with_capacity(text_count.min(1024));
    for _ in 0..text_count {
        let visibility = match reader.u8()? {
            0 => DetailVisibility::UserSafe,
            1 => DetailVisibility::SupportOnly,
            other => {
                return Err(Error::Other(format!(
                    "ledger envelope: unknown diagnostic visibility tag {other}"
                )));
            }
        };
        text.push((visibility, reader.string()?));
    }

    // `SyncState(CursorInvalid)` without an `ErrorScope::Cursor` is a builder
    // rejection, and a decoder must not fail on state the engine legitimately
    // held: a producer can attach a non-cursor scope, or none. Substituting the
    // account-wide cursor scope keeps the classification readable rather than
    // turning an operator's audit record into a decode error.
    if matches!(
        kind,
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ) && !matches!(scope, Some(ErrorScope::Cursor(_)))
    {
        scope = Some(ErrorScope::Cursor(CursorScope::Account));
    }

    let mut builder = AccountErrorBuilder::new(kind.clone(), canonical_cause(&kind));
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    if let Some(operation) = operation {
        builder = builder.operation(operation);
    }
    if let Some(provider) = provider {
        builder = builder.provider(provider);
    }
    if let Some(protocol) = protocol {
        builder = builder.protocol(protocol);
    }
    builder = builder.status(status);
    if let Some(request_id) = request_id {
        builder = builder.request_id(request_id);
    }
    if let Some(trace_id) = trace_id {
        builder = builder.trace_id(trace_id);
    }
    if let Some(native_code) = native_code {
        builder = builder.native_code(native_code);
    }
    for (visibility, value) in text {
        builder = builder.text(match visibility {
            DetailVisibility::UserSafe => DiagnosticText::user_safe(value),
            _ => DiagnosticText::support_only(value),
        });
    }
    builder.try_build().map_err(|err| {
        // A codec bug, not corrupt input - but decode never panics, because a
        // panic here would take down a consumer loading its own durable rows.
        Error::Other(format!(
            "ledger envelope: restored error failed classification: {err}"
        ))
    })
}

/// The `Cause` a kind demands, per `kind_matches_cause`.
///
/// Synthesized rather than decoded, because a `Cause` payload can hold a
/// `&'static str` no decoder can produce. Where the cause carries a field the
/// kind does not determine, this picks a neutral value; the real payload
/// survives in the diagnostic text the digest carries, which is where an
/// operator reads it anyway.
fn canonical_cause(kind: &AccountErrorKind) -> Cause {
    match kind {
        AccountErrorKind::Transport(kind) => Cause::Transport(TransportCause::new(
            match kind {
                TransportErrorKind::Timeout => TransportKind::Timeout,
                TransportErrorKind::Tls => TransportKind::Tls,
                _ => TransportKind::Network,
            },
            None,
        )),
        AccountErrorKind::Authentication(kind) => Cause::Auth(match kind {
            AuthErrorKind::RefreshTransient => AuthCause::RefreshTransient,
            AuthErrorKind::Revoked => AuthCause::Revoked,
            AuthErrorKind::ReauthorizationRequired => AuthCause::ReauthorizationRequired,
            _ => AuthCause::Expired,
        }),
        AccountErrorKind::Authorization(kind) => Cause::Access(match kind {
            AccessErrorKind::AdminConsentRequired => {
                AccessCause::AdminConsentRequired { needed: "unknown" }
            }
            AccessErrorKind::ConditionalAccessBlocked => AccessCause::ConditionalAccessBlocked,
            AccessErrorKind::PolicyBlocked => AccessCause::PolicyBlocked,
            AccessErrorKind::InsufficientScope => {
                AccessCause::InsufficientScope { needed: "unknown" }
            }
            AccessErrorKind::AccountDisabled => AccessCause::AccountDisabled,
            AccessErrorKind::MailboxUnavailable { kind } => {
                AccessCause::MailboxUnavailable { kind: *kind }
            }
            AccessErrorKind::MailboxNotLicensed => AccessCause::MailboxNotLicensed,
            _ => AccessCause::PermissionDenied { resource: None },
        }),
        AccountErrorKind::Server(kind) => Cause::Server(match kind {
            ServerErrorKind::Unavailable => ServerCause::Unavailable { retry_hint: None },
            ServerErrorKind::RateLimited => ServerCause::RateLimited { retry_hint: None },
            ServerErrorKind::QuotaExhausted => ServerCause::QuotaExhausted { retry_hint: None },
            ServerErrorKind::Error { status } => ServerCause::Error { status: *status },
            _ => ServerCause::Error { status: None },
        }),
        AccountErrorKind::SyncState(kind) => Cause::State(match kind {
            SyncStateErrorKind::StrategyFailure => StateCause::StrategyFailure {
                downgrade: StrategyDowngrade::QResyncToCondstore,
            },
            SyncStateErrorKind::ScopeCapabilityLost => StateCause::ScopeCapabilityLost,
            SyncStateErrorKind::SchemaIncompatible => StateCause::SchemaIncompatible,
            SyncStateErrorKind::CapabilityChanged => StateCause::CapabilityChanged { delta: None },
            SyncStateErrorKind::OperatorOverrideNeeded => StateCause::OperatorOverrideNeeded {
                reason: String::new(),
            },
            SyncStateErrorKind::ScopeRevoked => StateCause::ScopeRevoked,
            _ => StateCause::CursorInvalid,
        }),
        AccountErrorKind::ConcurrencyConflict => Cause::State(StateCause::ConcurrencyConflict),
        AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid) => {
            Cause::Request(RequestCause::BatchInputEmpty)
        }
        AccountErrorKind::Request(_) => Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(""),
        }),
        AccountErrorKind::NotFound(what) => Cause::Request(RequestCause::NotFound {
            what: *what,
            id: None,
        }),
        AccountErrorKind::Unsupported(operation) => Cause::Request(RequestCause::Unsupported {
            operation: *operation,
        }),
        AccountErrorKind::Protocol(_) => Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol: Protocol::Unknown,
            detail: None,
        }),
        AccountErrorKind::Internal(kind) => Cause::Internal(InternalCause::new(*kind, None)),
        // `AccountErrorKind` is `#[non_exhaustive]`; unreachable, because
        // `decode_error_kind` only produces tags this codec wrote. Kept so the
        // match is total without pretending a future kind is a request error.
        _ => Cause::Attempt(AttemptCause::new(bifrost_types::TransmissionState::Unsent)),
    }
}

fn encode_error_kind(out: &mut Vec<u8>, kind: &AccountErrorKind) {
    match kind {
        AccountErrorKind::Transport(kind) => {
            out.push(0);
            out.push(match kind {
                TransportErrorKind::Network => 0,
                TransportErrorKind::Timeout => 1,
                TransportErrorKind::Tls => 2,
                _ => panic!("ledger envelope: unknown transport error kind"),
            });
        }
        AccountErrorKind::Authentication(kind) => {
            out.push(1);
            out.push(match kind {
                AuthErrorKind::Expired => 0,
                AuthErrorKind::RefreshTransient => 1,
                AuthErrorKind::Revoked => 2,
                AuthErrorKind::ReauthorizationRequired => 3,
                _ => panic!("ledger envelope: unknown auth error kind"),
            });
        }
        AccountErrorKind::Authorization(kind) => {
            out.push(2);
            match kind {
                AccessErrorKind::AdminConsentRequired => out.push(0),
                AccessErrorKind::ConditionalAccessBlocked => out.push(1),
                AccessErrorKind::PolicyBlocked => out.push(2),
                AccessErrorKind::InsufficientScope => out.push(3),
                AccessErrorKind::PermissionDenied => out.push(4),
                AccessErrorKind::AccountDisabled => out.push(5),
                AccessErrorKind::MailboxUnavailable { kind } => {
                    out.push(6);
                    out.push(match kind {
                        MailboxUnavailableKind::Transient => 0,
                        MailboxUnavailableKind::Permanent => 1,
                        _ => panic!("ledger envelope: unknown mailbox unavailable kind"),
                    });
                }
                AccessErrorKind::MailboxNotLicensed => out.push(7),
                _ => panic!("ledger envelope: unknown access error kind"),
            }
        }
        AccountErrorKind::Server(kind) => {
            out.push(3);
            match kind {
                ServerErrorKind::Unavailable => out.push(0),
                ServerErrorKind::RateLimited => out.push(1),
                ServerErrorKind::QuotaExhausted => out.push(2),
                ServerErrorKind::Error { status } => {
                    out.push(3);
                    write_opt_u16_value(out, *status);
                }
                _ => panic!("ledger envelope: unknown server error kind"),
            }
        }
        AccountErrorKind::SyncState(kind) => {
            out.push(4);
            out.push(match kind {
                SyncStateErrorKind::CursorInvalid => 0,
                SyncStateErrorKind::StrategyFailure => 1,
                SyncStateErrorKind::ScopeCapabilityLost => 2,
                SyncStateErrorKind::SchemaIncompatible => 3,
                SyncStateErrorKind::CapabilityChanged => 4,
                SyncStateErrorKind::OperatorOverrideNeeded => 5,
                SyncStateErrorKind::ScopeRevoked => 6,
                _ => panic!("ledger envelope: unknown sync state error kind"),
            });
        }
        AccountErrorKind::ConcurrencyConflict => out.push(5),
        AccountErrorKind::Request(kind) => {
            out.push(6);
            out.push(match kind {
                RequestErrorKind::Malformed => 0,
                RequestErrorKind::BatchInputInvalid => 1,
                _ => panic!("ledger envelope: unknown request error kind"),
            });
        }
        AccountErrorKind::NotFound(resource) => {
            out.push(7);
            out.push(encode_resource(*resource));
        }
        AccountErrorKind::Unsupported(operation) => {
            out.push(8);
            out.push(encode_operation(*operation));
        }
        AccountErrorKind::Protocol(kind) => {
            out.push(9);
            out.push(match kind {
                ProtocolErrorKind::ParseFailed => 0,
                ProtocolErrorKind::MissingField => 1,
                ProtocolErrorKind::ContractViolation => 2,
                ProtocolErrorKind::PartialResponse => 3,
                ProtocolErrorKind::Unknown => 4,
                _ => panic!("ledger envelope: unknown protocol error kind"),
            });
        }
        AccountErrorKind::Internal(kind) => {
            out.push(10);
            out.push(match kind {
                InternalErrorKind::InvariantViolated => 0,
                InternalErrorKind::RuntimeFailure => 1,
                InternalErrorKind::Panicked => 2,
                InternalErrorKind::AccountContract => 3,
                InternalErrorKind::LimitExceeded => 4,
                _ => panic!("ledger envelope: unknown internal error kind"),
            });
        }
        // `AccountErrorKind` is `#[non_exhaustive]`; same rule as
        // `encode_obj_type` in the cursor envelope - the codec learns the
        // variant before a ledger carrying it may be stored.
        _ => panic!("ledger envelope: unknown account error kind variant"),
    }
}

fn decode_error_kind(reader: &mut Reader<'_>) -> Result<AccountErrorKind, Error> {
    let kind = match reader.u8()? {
        0 => AccountErrorKind::Transport(match reader.u8()? {
            0 => TransportErrorKind::Network,
            1 => TransportErrorKind::Timeout,
            2 => TransportErrorKind::Tls,
            other => return Err(unknown_tag("transport error kind", other)),
        }),
        1 => AccountErrorKind::Authentication(match reader.u8()? {
            0 => AuthErrorKind::Expired,
            1 => AuthErrorKind::RefreshTransient,
            2 => AuthErrorKind::Revoked,
            3 => AuthErrorKind::ReauthorizationRequired,
            other => return Err(unknown_tag("auth error kind", other)),
        }),
        2 => AccountErrorKind::Authorization(match reader.u8()? {
            0 => AccessErrorKind::AdminConsentRequired,
            1 => AccessErrorKind::ConditionalAccessBlocked,
            2 => AccessErrorKind::PolicyBlocked,
            3 => AccessErrorKind::InsufficientScope,
            4 => AccessErrorKind::PermissionDenied,
            5 => AccessErrorKind::AccountDisabled,
            6 => AccessErrorKind::MailboxUnavailable {
                kind: match reader.u8()? {
                    0 => MailboxUnavailableKind::Transient,
                    1 => MailboxUnavailableKind::Permanent,
                    other => return Err(unknown_tag("mailbox unavailable kind", other)),
                },
            },
            7 => AccessErrorKind::MailboxNotLicensed,
            other => return Err(unknown_tag("access error kind", other)),
        }),
        3 => AccountErrorKind::Server(match reader.u8()? {
            0 => ServerErrorKind::Unavailable,
            1 => ServerErrorKind::RateLimited,
            2 => ServerErrorKind::QuotaExhausted,
            3 => ServerErrorKind::Error {
                status: read_opt_u16(reader)?,
            },
            other => return Err(unknown_tag("server error kind", other)),
        }),
        4 => AccountErrorKind::SyncState(match reader.u8()? {
            0 => SyncStateErrorKind::CursorInvalid,
            1 => SyncStateErrorKind::StrategyFailure,
            2 => SyncStateErrorKind::ScopeCapabilityLost,
            3 => SyncStateErrorKind::SchemaIncompatible,
            4 => SyncStateErrorKind::CapabilityChanged,
            5 => SyncStateErrorKind::OperatorOverrideNeeded,
            6 => SyncStateErrorKind::ScopeRevoked,
            other => return Err(unknown_tag("sync state error kind", other)),
        }),
        5 => AccountErrorKind::ConcurrencyConflict,
        6 => AccountErrorKind::Request(match reader.u8()? {
            0 => RequestErrorKind::Malformed,
            1 => RequestErrorKind::BatchInputInvalid,
            other => return Err(unknown_tag("request error kind", other)),
        }),
        7 => AccountErrorKind::NotFound(decode_resource(reader.u8()?, reader.version)?),
        8 => AccountErrorKind::Unsupported(decode_operation(reader.u8()?)?),
        9 => AccountErrorKind::Protocol(match reader.u8()? {
            0 => ProtocolErrorKind::ParseFailed,
            1 => ProtocolErrorKind::MissingField,
            2 => ProtocolErrorKind::ContractViolation,
            3 => ProtocolErrorKind::PartialResponse,
            4 => ProtocolErrorKind::Unknown,
            other => return Err(unknown_tag("protocol error kind", other)),
        }),
        // Introduced in version 4; a version-3 writer never produced it.
        10 if reader.version >= 4 => AccountErrorKind::Internal(match reader.u8()? {
            0 => InternalErrorKind::InvariantViolated,
            1 => InternalErrorKind::RuntimeFailure,
            2 => InternalErrorKind::Panicked,
            3 => InternalErrorKind::AccountContract,
            4 => InternalErrorKind::LimitExceeded,
            other => return Err(unknown_tag("internal error kind", other)),
        }),
        other => return Err(unknown_tag("account error kind", other)),
    };
    Ok(kind)
}

fn encode_error_scope(out: &mut Vec<u8>, scope: &ErrorScope) {
    match scope {
        ErrorScope::Account => out.push(0),
        ErrorScope::Cursor(cursor) => {
            out.push(1);
            write_bytes(out, &encode_scope(cursor));
        }
        ErrorScope::Mailbox { id } => {
            out.push(2);
            write_string(out, &id.0);
        }
        ErrorScope::Message { id } => {
            out.push(3);
            write_string(out, &id.0);
        }
        ErrorScope::Thread { id } => {
            out.push(4);
            write_string(out, &id.0);
        }
        ErrorScope::Calendar { id } => {
            out.push(5);
            write_string(out, &id.0);
        }
        ErrorScope::CalendarCollection => out.push(6),
        ErrorScope::Contact { id } => {
            out.push(7);
            write_string(out, &id.0);
        }
        ErrorScope::ContactCollection => out.push(8),
        ErrorScope::DirectoryGateway { url } => {
            out.push(10);
            write_string(out, url);
        }
        ErrorScope::Event { id } => {
            out.push(9);
            write_string(out, &id.0);
        }
        // `ErrorScope` is `#[non_exhaustive]`.
        _ => panic!("ledger envelope: unknown error scope variant"),
    }
}

fn decode_error_scope(reader: &mut Reader<'_>) -> Result<ErrorScope, Error> {
    Ok(match reader.u8()? {
        0 => ErrorScope::Account,
        1 => ErrorScope::Cursor(decode_scope(&reader.bytes()?)?),
        2 => ErrorScope::Mailbox {
            id: bifrost_types::MailboxId(reader.string()?),
        },
        3 => ErrorScope::Message {
            id: ObjectId(reader.string()?),
        },
        4 => ErrorScope::Thread {
            id: bifrost_types::ThreadId(reader.string()?),
        },
        5 => ErrorScope::Calendar {
            id: bifrost_types::CalendarId(reader.string()?),
        },
        6 => ErrorScope::CalendarCollection,
        7 => ErrorScope::Contact {
            id: bifrost_types::ContactId(reader.string()?),
        },
        8 => ErrorScope::ContactCollection,
        9 if reader.version >= 5 => ErrorScope::Event {
            id: bifrost_types::EventId(reader.string()?),
        },
        10 if reader.version >= 6 => ErrorScope::DirectoryGateway {
            url: reader.string()?,
        },
        other => return Err(unknown_tag("error scope", other)),
    })
}

fn encode_provider(provider: Provider) -> u8 {
    match provider {
        Provider::Fastmail => 0,
        Provider::Gmail => 1,
        Provider::Microsoft => 2,
        Provider::Icloud => 3,
        Provider::Yahoo => 4,
        Provider::Custom => 5,
        _ => panic!("ledger envelope: unknown provider variant"),
    }
}

fn decode_provider(tag: u8) -> Result<Provider, Error> {
    Ok(match tag {
        0 => Provider::Fastmail,
        1 => Provider::Gmail,
        2 => Provider::Microsoft,
        3 => Provider::Icloud,
        4 => Provider::Yahoo,
        5 => Provider::Custom,
        other => return Err(unknown_tag("provider", other)),
    })
}

fn encode_protocol(protocol: Protocol) -> u8 {
    match protocol {
        Protocol::Jmap => 0,
        Protocol::Imap => 1,
        Protocol::CardDav => 2,
        Protocol::Smtp => 3,
        Protocol::Lmtp => 4,
        Protocol::Gmail => 5,
        Protocol::Graph => 6,
        Protocol::Ews => 7,
        Protocol::CalDav => 8,
        Protocol::Unknown => 9,
        _ => panic!("ledger envelope: unknown protocol variant"),
    }
}

fn decode_protocol(tag: u8) -> Result<Protocol, Error> {
    Ok(match tag {
        0 => Protocol::Jmap,
        1 => Protocol::Imap,
        2 => Protocol::CardDav,
        3 => Protocol::Smtp,
        4 => Protocol::Lmtp,
        5 => Protocol::Gmail,
        6 => Protocol::Graph,
        7 => Protocol::Ews,
        8 => Protocol::CalDav,
        9 => Protocol::Unknown,
        other => return Err(unknown_tag("protocol", other)),
    })
}

fn encode_resource(resource: ResourceKind) -> u8 {
    match resource {
        ResourceKind::Message => 0,
        ResourceKind::Mailbox => 1,
        ResourceKind::Thread => 2,
        ResourceKind::Calendar => 3,
        ResourceKind::Contact => 4,
        ResourceKind::Draft => 5,
        ResourceKind::Identity => 6,
        ResourceKind::Vacation => 7,
        ResourceKind::PushSubscription => 8,
        ResourceKind::Account => 9,
        ResourceKind::Filter => 10,
        ResourceKind::Event => 11,
        _ => panic!("ledger envelope: unknown resource kind variant"),
    }
}

fn decode_resource(tag: u8, version: u32) -> Result<ResourceKind, Error> {
    Ok(match tag {
        0 => ResourceKind::Message,
        1 => ResourceKind::Mailbox,
        2 => ResourceKind::Thread,
        3 => ResourceKind::Calendar,
        4 => ResourceKind::Contact,
        5 => ResourceKind::Draft,
        6 => ResourceKind::Identity,
        7 => ResourceKind::Vacation,
        8 => ResourceKind::PushSubscription,
        9 => ResourceKind::Account,
        10 => ResourceKind::Filter,
        11 if version >= 5 => ResourceKind::Event,
        other => return Err(unknown_tag("resource kind", other)),
    })
}

/// Tag table for `AccountOperation`, in declaration order.
///
/// A table rather than two hand-written matches, because 85 variants written
/// twice is 85 chances to transpose a pair - and a transposed pair here does
/// not fail to compile, it silently relabels an operation in a durable record.
/// The `encode`/`decode` pair below is derived from this one list, so the two
/// directions cannot disagree.
const OPERATIONS: &[AccountOperation] = &[
    AccountOperation::CategoryDefinitionsList,
    AccountOperation::MessageReactionsRead,
    AccountOperation::Discover,
    AccountOperation::EstablishCursor,
    AccountOperation::DiscoverCursorScopes,
    AccountOperation::DiscoverMemberships,
    AccountOperation::ScopeLifecycle,
    AccountOperation::SyncInventory,
    AccountOperation::SyncChanges,
    AccountOperation::Hydrate,
    AccountOperation::HydrateThread,
    AccountOperation::HydrateMessage,
    AccountOperation::OpenBlob,
    AccountOperation::OpenBlobRange,
    AccountOperation::OpenRawRfc822,
    AccountOperation::PushSubscribe,
    AccountOperation::PushUnsubscribe,
    AccountOperation::PushStream,
    AccountOperation::UpdateFlags,
    AccountOperation::SetStarred,
    AccountOperation::MarkReplied,
    AccountOperation::MarkForwarded,
    AccountOperation::MarkMdnSent,
    AccountOperation::BulkMove,
    AccountOperation::BulkDestroy,
    AccountOperation::MoveThread,
    AccountOperation::DeleteThread,
    AccountOperation::AddToContainer,
    AccountOperation::RemoveFromContainer,
    AccountOperation::SetKeyword,
    AccountOperation::SetLabelMembership,
    AccountOperation::SetCategory,
    AccountOperation::SetExtendedProperty,
    AccountOperation::SetImportance,
    AccountOperation::SetIsRead,
    AccountOperation::Send,
    AccountOperation::AttachmentUpload,
    AccountOperation::HostAttachment,
    AccountOperation::DraftCreate,
    AccountOperation::DraftUpdate,
    AccountOperation::DraftDiscard,
    AccountOperation::DraftSend,
    AccountOperation::CancelScheduledSend,
    AccountOperation::RescheduleSend,
    AccountOperation::Search,
    AccountOperation::SearchMessages,
    AccountOperation::ContainersList,
    AccountOperation::ContainerCreate,
    AccountOperation::ContainerRename,
    AccountOperation::ContainerMove,
    AccountOperation::ContainerDelete,
    AccountOperation::IdentitiesList,
    AccountOperation::IdentityUpdate,
    AccountOperation::VacationGet,
    AccountOperation::VacationSet,
    AccountOperation::QuotaGet,
    AccountOperation::FiltersList,
    AccountOperation::FilterCreate,
    AccountOperation::FilterUpdate,
    AccountOperation::FilterDelete,
    AccountOperation::FilterValidate,
    AccountOperation::AddressBooksList,
    AccountOperation::ContactsList,
    AccountOperation::ContactGet,
    AccountOperation::ContactCreate,
    AccountOperation::ContactUpdate,
    AccountOperation::ContactDelete,
    AccountOperation::ContactSearch,
    AccountOperation::ContactAutocomplete,
    AccountOperation::DirectorySearch,
    AccountOperation::DirectoryGroupsList,
    AccountOperation::DirectoryGroupExpand,
    AccountOperation::CalendarsList,
    AccountOperation::EventsInRange,
    AccountOperation::EventGet,
    AccountOperation::EventCreate,
    AccountOperation::EventUpdate,
    AccountOperation::EventDelete,
    AccountOperation::EventRsvp,
    AccountOperation::EventSearch,
    AccountOperation::EventAutocomplete,
    AccountOperation::Close,
    AccountOperation::Expunge,
];

fn encode_operation(operation: AccountOperation) -> u8 {
    let index = OPERATIONS
        .iter()
        .position(|candidate| *candidate == operation)
        // `AccountOperation` is `#[non_exhaustive]`; an operation missing from
        // the table is one this codec has not learned, and writing a durable
        // row that mislabels it is worse than refusing.
        .unwrap_or_else(|| panic!("ledger envelope: unknown account operation {operation:?}"));
    u8::try_from(index).expect("ledger envelope: operation table exceeds u8 tag space")
}

fn decode_operation(tag: u8) -> Result<AccountOperation, Error> {
    OPERATIONS
        .get(tag as usize)
        .copied()
        .ok_or_else(|| unknown_tag("account operation", tag))
}

// ---------- primitives ----------

fn unknown_tag(what: &str, tag: u8) -> Error {
    Error::Other(format!("ledger envelope: unknown {what} tag {tag}"))
}

fn write_count(out: &mut Vec<u8>, count: usize, what: &str) {
    let count = u32::try_from(count)
        .unwrap_or_else(|_| panic!("ledger envelope: {what} exceed the u32 count limit"));
    out.extend_from_slice(&count.to_le_bytes());
}

fn write_opt_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn read_opt_i64(reader: &mut Reader<'_>) -> Result<Option<i64>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.i64()?)),
        other => Err(unknown_tag("optional i64", other)),
    }
}

fn write_opt_u16(out: &mut Vec<u8>, value: Option<u16>) {
    write_opt_u16_value(out, value);
}

fn write_opt_u16_value(out: &mut Vec<u8>, value: Option<u16>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn read_opt_u16(reader: &mut Reader<'_>) -> Result<Option<u16>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.u16()?)),
        other => Err(unknown_tag("optional u16", other)),
    }
}

fn write_opt_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            write_string(out, value);
        }
    }
}

fn read_opt_string(reader: &mut Reader<'_>) -> Result<Option<String>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.string()?)),
        other => Err(unknown_tag("optional string", other)),
    }
}

/// Sequential cursor over the payload.
///
/// The cursor envelope's primitives return `(value, consumed)` and let each
/// call site thread the offset, which is workable for its handful of fields and
/// is not workable for a ledger's nesting depth. Same encodings, one place that
/// tracks the position.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    /// The row's envelope version, for tags a version introduced: a tag an
    /// older writer could not have produced is corruption in an older row.
    version: u32,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], version: u32) -> Self {
        Self {
            bytes,
            at: 0,
            version,
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], Error> {
        let end = self
            .at
            .checked_add(count)
            .ok_or_else(|| Error::Other("ledger envelope: length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(Error::Other("ledger envelope: truncated payload".into()));
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, Error> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, Error> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        let bytes = self.take(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(buf))
    }

    fn digest32(&mut self) -> Result<[u8; 32], Error> {
        let bytes = self.take(32)?;
        let mut buf = [0u8; 32];
        buf.copy_from_slice(bytes);
        Ok(buf)
    }

    fn i64(&mut self) -> Result<i64, Error> {
        let bytes = self.take(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(i64::from_le_bytes(buf))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, Error> {
        let (value, consumed) = read_bytes(&self.bytes[self.at..])?;
        self.at += consumed;
        Ok(value)
    }

    fn string(&mut self) -> Result<String, Error> {
        let (value, consumed) = read_string(&self.bytes[self.at..])?;
        self.at += consumed;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bifrost_types::{AccountErrorBuilder, AccountErrorKind, CursorScope};

    use super::{LEDGER_ENVELOPE_VERSION, decode_ledger, encode_ledger};
    use crate::cursor::ledger::{DebtLedger, DischargeAudit, DischargeEvidence};
    use crate::error::Error;

    /// The empty ledger is the state every account starts in, so a consumer
    /// writes it far more often than any populated one. It must survive the
    /// round trip as EMPTY rather than as "nothing decoded" - the two are the
    /// same value here and would stop being the same value the moment decode
    /// grew a silent early return.
    #[test]
    fn an_empty_ledger_round_trips() {
        let bytes = encode_ledger(&DebtLedger::new());
        let restored = decode_ledger(&bytes).expect("empty ledger decodes");
        assert!(restored.is_empty());
        assert_eq!(restored.entries().count(), 0);
        assert_eq!(restored.barriers().count(), 0);
    }

    #[test]
    fn a_version_above_the_window_is_schema_incompatible() {
        let mut bytes = encode_ledger(&DebtLedger::new());
        bytes[4..8].copy_from_slice(&(LEDGER_ENVELOPE_VERSION + 1).to_le_bytes());
        assert!(matches!(
            decode_ledger(&bytes),
            Err(Error::SchemaIncompatible)
        ));
    }

    #[test]
    fn a_version_below_the_window_is_schema_incompatible() {
        let mut bytes = encode_ledger(&DebtLedger::new());
        bytes[4..8].copy_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            decode_ledger(&bytes),
            Err(Error::SchemaIncompatible)
        ));
    }

    /// Malformed framing is `Error::Other`, NOT `SchemaIncompatible`. The two
    /// mean different things to a consumer: schema-incompatible authorizes
    /// clearing the row and re-establishing, and corrupt bytes must not quietly
    /// acquire that authority.
    #[test]
    fn corrupt_framing_is_not_a_schema_signal() {
        let mut bytes = encode_ledger(&DebtLedger::new());
        bytes[0] = 0x00;
        assert!(matches!(decode_ledger(&bytes), Err(Error::Other(_))));

        let mut reserved = encode_ledger(&DebtLedger::new());
        reserved[2] = 1;
        assert!(matches!(decode_ledger(&reserved), Err(Error::Other(_))));

        assert!(matches!(decode_ledger(&[]), Err(Error::Other(_))));
    }

    /// A truncated payload must be refused rather than silently yielding a
    /// short ledger, which is the failure mode that loses debt without anyone
    /// noticing.
    #[test]
    fn a_truncated_payload_is_refused() {
        let mut bytes = encode_ledger(&DebtLedger::new());
        bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
        assert!(decode_ledger(&bytes).is_err());
    }

    /// Version 2 carried the retry budget on the discharged root entry and has
    /// no lineage table. The owner ruled no consumer had persisted one, so it
    /// is outside the window rather than migrated from counters that disagreed
    /// with each other.
    #[test]
    fn a_version_two_row_is_schema_incompatible() {
        let mut bytes = encode_ledger(&DebtLedger::new());
        bytes[4..8].copy_from_slice(&2u32.to_le_bytes());
        assert!(matches!(
            decode_ledger(&bytes),
            Err(Error::SchemaIncompatible)
        ));
    }

    /// Both trailing sections are genuinely read: dropping the lineage count
    /// truncates the row, and bytes past it are refused rather than ignored.
    #[test]
    fn the_payload_must_be_consumed_exactly() {
        let mut short = encode_ledger(&DebtLedger::new());
        short.truncate(short.len() - 4);
        assert!(matches!(decode_ledger(&short), Err(Error::Other(_))));

        let mut long = encode_ledger(&DebtLedger::new());
        long.push(0);
        assert!(matches!(decode_ledger(&long), Err(Error::Other(_))));
    }

    fn replayable(key: &str) -> bifrost_types::InventoryObligation {
        bifrost_types::InventoryObligation::Region {
            key: bifrost_types::ObligationKey(key.as_bytes().to_vec()),
            failure_label: "truncated".into(),
            error: bifrost_types::AccountErrorBuilder::new(
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed,
                ),
                bifrost_types::Cause::Request(bifrost_types::RequestCause::Malformed {
                    detail: bifrost_types::DiagnosticText::support_only("unrepresentable"),
                }),
            )
            .try_build()
            .expect("valid account error classification"),
            recovery: bifrost_types::RegionRecovery::DurableReplay {
                token: b"tok".to_vec(),
            },
        }
    }

    /// A split lineage with a spent budget crosses the durable boundary with
    /// its budget intact - the table, the flat parent links and the blocked
    /// members all come back, and the restored ledger still refuses to plan
    /// them.
    #[test]
    fn a_split_lineage_round_trips_with_its_budget() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(CursorScope::Account),
                vec![replayable("root")],
            ),
            1,
            100,
        );
        let root = bifrost_types::ObligationKey(b"root".to_vec());
        let a = bifrost_types::ObligationKey(b"a".to_vec());
        ledger
            .replace_obligation(&root, &[], &[replayable("a"), replayable("b")], 1, 200)
            .expect("replacement accepted");
        assert!(!ledger.record_attempt(&a, 2));

        let restored = decode_ledger(&encode_ledger(&ledger)).expect("decodes");
        assert_eq!(restored.lineages(), ledger.lineages());
        assert_eq!(restored.lineage_root(&a), root);

        let mut restored = restored;
        assert!(
            restored.record_attempt(&a, 2),
            "the charge made before the round trip still counts"
        );
        assert_eq!(restored.repairable().count(), 0);
    }

    /// Decode validates rather than heals: an open member whose budget row
    /// was dropped from the bytes is refused, not given a fresh budget.
    #[test]
    fn a_row_missing_its_lineage_budget_is_refused() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(CursorScope::Account),
                vec![replayable("root")],
            ),
            1,
            100,
        );
        let mut bytes = encode_ledger(&ledger);
        // The lineage section is the tail: one row of a 4-byte key length, the
        // 4-byte key, 4 bytes of attempts and the flag byte, after its count.
        bytes.truncate(bytes.len() - (4 + 4 + 4 + 4 + 1));
        bytes.extend_from_slice(&0u32.to_le_bytes());
        assert!(matches!(decode_ledger(&bytes), Err(Error::Other(_))));
    }

    /// The audit is the whole point of compaction, so it must cross the durable
    /// boundary intact. A root that changed value on the way through would make
    /// every later verification fail against a ledger that lost nothing.
    #[test]
    fn a_compacted_audit_round_trips_with_its_root() {
        let mut audit = DischargeAudit::default();
        audit.fold(
            &bifrost_types::ObligationKey(b"gone".to_vec()),
            9,
            &DischargeEvidence::ProvedIrrelevant {
                detail: "out of scope".into(),
            },
        );
        let ledger = DebtLedger::from_parts(
            BTreeMap::new(),
            BTreeMap::new(),
            Vec::new(),
            vec![(CursorScope::Account, audit)],
            BTreeMap::new(),
        );

        let restored = decode_ledger(&encode_ledger(&ledger)).expect("decodes");
        let restored = restored
            .discharge_audit(&CursorScope::Account)
            .expect("audit survives");
        assert_eq!(*restored, audit);
        assert_ne!(
            restored.root, [0u8; 32],
            "a folded entry must move the root"
        );
    }

    /// Every operation tag must survive the table round trip. A transposed pair
    /// in the table would relabel an operation in a durable record without ever
    /// failing to compile.
    #[test]
    fn every_account_operation_tag_round_trips() {
        for (index, operation) in super::OPERATIONS.iter().enumerate() {
            let tag = super::encode_operation(*operation);
            assert_eq!(usize::from(tag), index, "table order defines the tag");
            assert_eq!(
                super::decode_operation(tag).expect("known tag"),
                *operation,
                "the two directions must agree"
            );
        }
        let past_end = u8::try_from(super::OPERATIONS.len()).expect("table fits a u8 tag");
        assert!(super::decode_operation(past_end).is_err());
    }

    #[test]
    fn directory_gateway_scope_digest_round_trips_and_requires_version_six() {
        assert_eq!(
            &encode_ledger(&DebtLedger::new())[4..8],
            &6u32.to_le_bytes(),
            "the new error-scope vocabulary requires a version-six header",
        );
        let scope = bifrost_types::ErrorScope::DirectoryGateway {
            url: "https://dav.example.test/gal/".into(),
        };
        let mut bytes = Vec::new();
        super::encode_error_scope(&mut bytes, &scope);
        assert_eq!(
            super::decode_error_scope(&mut super::Reader::new(&bytes, 6)).expect("scope"),
            scope
        );
        for version in [3, 4, 5] {
            assert!(super::decode_error_scope(&mut super::Reader::new(&bytes, version)).is_err());
            let mut previous = encode_ledger(&DebtLedger::new());
            previous[4..8].copy_from_slice(&version.to_le_bytes());
            assert!(decode_ledger(&previous).is_ok());
        }
    }

    #[test]
    fn nc2c_event_error_digest_retains_resource_and_scope() {
        use bifrost_types::{AccountOperation, ErrorScope, ResourceKind};
        let error = bifrost_types::AccountErrorBuilder::new(
            AccountErrorKind::NotFound(ResourceKind::Event),
            bifrost_types::Cause::Request(bifrost_types::RequestCause::NotFound {
                what: ResourceKind::Event,
                id: Some("public-instance".into()),
            }),
        )
        .operation(AccountOperation::EventGet)
        .scope(ErrorScope::Event {
            id: bifrost_types::EventId("public-instance".into()),
        })
        .try_build()
        .expect("valid error");
        let mut bytes = Vec::new();
        super::encode_error(&mut bytes, &error);
        let restored =
            super::decode_error(&mut super::Reader::new(&bytes, 5)).expect("event digest");
        assert_eq!(restored.kind(), error.kind());
        assert_eq!(restored.scope(), error.scope());
        assert_eq!(restored.operation(), error.operation());
        for version in [3, 4] {
            assert!(super::decode_error(&mut super::Reader::new(&bytes, version)).is_err());
            assert!(super::decode_resource(11, version).is_err());
            let mut scope = Vec::new();
            super::encode_error_scope(&mut scope, error.scope().expect("scope"));
            assert!(super::decode_error_scope(&mut super::Reader::new(&scope, version)).is_err());
        }
        let mut previous = super::encode_ledger(&DebtLedger::new());
        previous[4..8].copy_from_slice(&4u32.to_le_bytes());
        assert!(super::decode_ledger(&previous).is_ok());
    }

    /// Version 4 only widened the error-kind vocabulary, so a version-3 row
    /// is still read as is.
    #[test]
    fn a_version_three_row_still_decodes() {
        let mut bytes = encode_ledger(&DebtLedger::new());
        bytes[4..8].copy_from_slice(&3u32.to_le_bytes());
        assert!(decode_ledger(&bytes).is_ok());
    }

    /// Every internal kind round-trips through the error digest in a
    /// version-4 row, and the same bytes in a version-3 row are corruption:
    /// no version-3 writer could have produced the tag.
    #[test]
    fn the_internal_kind_is_a_version_four_tag() {
        use bifrost_types::InternalErrorKind;
        for kind in [
            InternalErrorKind::InvariantViolated,
            InternalErrorKind::RuntimeFailure,
            InternalErrorKind::Panicked,
            InternalErrorKind::AccountContract,
            InternalErrorKind::LimitExceeded,
        ] {
            let kind = AccountErrorKind::Internal(kind);
            let mut bytes = Vec::new();
            super::encode_error_kind(&mut bytes, &kind);
            assert_eq!(
                super::decode_error_kind(&mut super::Reader::new(&bytes, 4)).expect("v4 tag"),
                kind
            );
            assert!(super::decode_error_kind(&mut super::Reader::new(&bytes, 3)).is_err());
            // The restored digest re-derives a valid classification.
            let restored = AccountErrorBuilder::new(kind.clone(), super::canonical_cause(&kind))
                .try_build()
                .expect("canonical cause matches the kind");
            assert_eq!(
                restored.recovery(),
                &bifrost_types::RecoveryClass::InternalFailure
            );
        }
    }
}
