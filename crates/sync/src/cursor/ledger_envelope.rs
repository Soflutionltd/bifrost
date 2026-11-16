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
//!
//! Version 6 adds DirectoryGateway scope (tag 10); older versions reject it.
//!
//! Version 7 adds Submission, CloudFile and UploadSession scopes (tags 11-13);
//! older versions reject them.
//!
//! Version 8 adds the `ControlWaitEnded` account error kind (kind tag 11);
//! older versions reject it.
//!
//! Version 9 adds Google provider (tag 6) and Calendar, People and Drive
//! protocols (tags 10-12); older versions reject those tags.
//!
//! Version 10 adds `AccountClosed` (kind tag 12); older versions reject it.
//!
//! Version 12 removes the mirrored attempt count from policy tag 0 and splits
//! entry and barrier policy types. Versions 3 through 11 still decode: open
//! entry counters must match the lineage row, and inert barrier counters are
//! discarded. Waiver identities and signed Unix seconds keep their wire form.
//!
//! Version 13 adds the Owner error scope (tag 15), carrying PrincipalId.
//! Older rows retain their native Mailbox scope spelling; ambiguous historical
//! ids are never guessed into principals.
//!
//! Version 14 adds PayloadTooLarge and PreconditionFailed request subkinds
//! (tags 2 and 3 under kind tag 6); older rows reject these new tags.
//!
//! A `BarrierIncident::resume_from` nests a whole cursor envelope through
//! `encode_envelope` / `decode_envelope` rather than re-deriving one: the
//! checkpoint codec owns that layout and its migration chain, and a second
//! decoder for the same bytes is a second thing to keep correct.
//!
//! # Why the error is a digest and not a reconstruction
//!
//! `LedgerEntry::last_error` and `BarrierIncident::evidence` contain
//! `ErrorDigest`: kind, scope, operation, provider, protocol and diagnostics.
//! They are historical operator evidence, not live `AccountError` values.
//! No cause, recovery advice, message key, throttle override or partial-commit
//! record is reconstructed. Repair uses the separately retained target.
//!
//! The digest representation did not change the byte layout of the error
//! record; every version in `[MIN_MIGRATABLE_LEDGER, LEDGER_ENVELOPE_VERSION]`
//! reads through the existing migrations. Diagnostic text keeps its
//! visibility; malformed diagnostic tokens are rejected as corrupt input.

use std::collections::BTreeMap;

use super::envelope::{
    decode_envelope, decode_scope, encode_envelope, encode_scope, write_bytes, write_string,
};
use super::ledger::{
    BarrierIncident, BarrierPolicy, DebtLedger, DischargeAudit, DischargeEvidence, EntryPolicy,
    ErrorDigest, LedgerEntry, LineageBudget, OperatorId, ProofStatus, UnixSeconds,
};
use crate::error::Error;
use bifrost_types::wire::ledger::{
    decode_diagnostics, decode_domain, decode_error_kind, decode_error_scope, decode_operation,
    decode_protocol, decode_provider, decode_repair_target, encode_diagnostics, encode_domain,
    encode_error_kind, encode_error_scope, encode_operation, encode_protocol, encode_provider,
    encode_repair_target,
};
use bifrost_types::{ObligationKey, wire::Reader};

/// Current ledger envelope version. Bumped whenever the layout below changes
/// in a way an older decoder would misread, including a new tag in a
/// vocabulary an older decoder would refuse.
// Version 14 adds payload and target-state request refusal kinds.
pub const LEDGER_ENVELOPE_VERSION: u32 = 14;
const _: () = assert!(LEDGER_ENVELOPE_VERSION >= bifrost_types::wire::ledger::VOCABULARY_VERSION);

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
/// Panics if an in-memory field or count exceeds the u32 wire limit, or a
/// nested change cursor has an unsupported outer envelope version. Shared
/// vocabulary encoders match exhaustively in bifrost-types.
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
    let mut legacy_attempts = BTreeMap::new();
    for _ in 0..entry_count {
        let (entry, attempts) = decode_entry(&mut reader)?;
        if let Some(attempts) = attempts {
            legacy_attempts.insert(entry.key.clone(), attempts);
        }
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
        let generation = crate::cursor::WalkGeneration::restore(reader.u64()?);
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
                root: reader.array()?,
                latest_generation: crate::cursor::WalkGeneration::restore(reader.u64()?),
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

    if !reader.is_finished() {
        return Err(Error::Other("ledger envelope: trailing bytes".into()));
    }

    // Historical writers persisted the count twice. Check their agreement
    // before dropping the old view; migration must not hide corrupt budgets.
    for (key, attempts) in legacy_attempts {
        let entry = &entries[&key];
        if entry.is_open() {
            let lineage = entry.parent.as_ref().unwrap_or(&entry.key);
            if lineages
                .get(lineage)
                .is_none_or(|budget| budget.attempts != attempts)
            {
                return Err(Error::Other(
                    "ledger envelope: legacy policy disagrees with lineage budget".into(),
                ));
            }
        }
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
/// Historical policy counters are checked and removed while decoding, before
/// current lineage validation. This final boundary is otherwise a no-op.
/// Like `migrate_change_cursor`, the codec alone may hold ledger state in
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
    encode_repair_target(out, entry.target.as_ref());
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

fn decode_entry(reader: &mut Reader<'_>) -> Result<(LedgerEntry, Option<u32>), Error> {
    let key = ObligationKey(reader.bytes()?);
    let domain = decode_domain(reader)?;
    let generation = crate::cursor::WalkGeneration::restore(reader.u64()?);
    let proof = decode_proof(reader)?;
    let (policy, legacy_attempts) = decode_policy(reader)?;
    let target = decode_repair_target(reader)?;
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
    Ok((
        LedgerEntry {
            key,
            domain,
            generation,
            proof,
            policy,
            target,
            parent,
            first_seen_unix_seconds,
            last_error,
        },
        legacy_attempts,
    ))
}

fn encode_barrier(out: &mut Vec<u8>, barrier: &BarrierIncident) {
    write_bytes(out, &barrier.key.0);
    encode_domain(out, &barrier.domain);
    out.extend_from_slice(&barrier.generation.to_le_bytes());
    write_string(out, &barrier.failure_label);
    encode_error(out, &barrier.evidence);
    encode_barrier_policy(out, &barrier.policy);
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
    let generation = crate::cursor::WalkGeneration::restore(reader.u64()?);
    let failure_label = reader.string()?;
    let evidence = decode_error(reader)?;
    let (entry_policy, _) = decode_policy(reader)?;
    // The legacy automatic barrier counter was inert, regardless of its value.
    let policy = match entry_policy {
        EntryPolicy::Automatic => BarrierPolicy::Undecided,
        EntryPolicy::OperatorBlocked => BarrierPolicy::OperatorBlocked,
        EntryPolicy::Waived {
            by,
            at_unix_seconds,
        } => BarrierPolicy::Waived {
            by,
            at_unix_seconds,
        },
    };
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

fn encode_policy(out: &mut Vec<u8>, policy: &EntryPolicy) {
    match policy {
        EntryPolicy::Automatic => out.push(0),
        EntryPolicy::OperatorBlocked => out.push(1),
        EntryPolicy::Waived {
            by,
            at_unix_seconds,
        } => {
            out.push(2);
            encode_waiver(out, by, *at_unix_seconds);
        }
    }
}

fn encode_barrier_policy(out: &mut Vec<u8>, policy: &BarrierPolicy) {
    match policy {
        BarrierPolicy::Undecided => out.push(0),
        BarrierPolicy::OperatorBlocked => out.push(1),
        BarrierPolicy::Waived {
            by,
            at_unix_seconds,
        } => {
            out.push(2);
            encode_waiver(out, by, *at_unix_seconds);
        }
    }
}

fn encode_waiver(out: &mut Vec<u8>, by: &OperatorId, at: UnixSeconds) {
    write_string(out, &by.0);
    out.extend_from_slice(&at.0.to_le_bytes());
}

// Returns the historical mirror only at the migration boundary. It never
// enters current ledger state. Barrier callers intentionally discard it.
fn decode_policy(reader: &mut Reader<'_>) -> Result<(EntryPolicy, Option<u32>), Error> {
    match reader.u8()? {
        0 => {
            let attempts = if reader.version() < 12 {
                Some(reader.u32()?)
            } else {
                None
            };
            Ok((EntryPolicy::Automatic, attempts))
        }
        1 => Ok((EntryPolicy::OperatorBlocked, None)),
        2 => Ok((
            EntryPolicy::Waived {
                by: OperatorId(reader.string()?),
                at_unix_seconds: UnixSeconds(reader.i64()?),
            },
            None,
        )),
        other => Err(Error::Other(format!(
            "ledger envelope: unknown policy status tag {other}"
        ))),
    }
}

// ---------- account error digest ----------

fn encode_error(out: &mut Vec<u8>, error: &ErrorDigest) {
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

    encode_diagnostics(out, &error.diagnostics);
}

fn decode_error(reader: &mut Reader<'_>) -> Result<ErrorDigest, Error> {
    let kind = decode_error_kind(reader)?;
    let scope = match reader.u8()? {
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
        1 => Some(decode_provider(reader.u8()?, reader.version())?),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown provider tag {other}"
            )));
        }
    };
    let protocol = match reader.u8()? {
        0 => None,
        1 => Some(decode_protocol(reader.u8()?, reader.version())?),
        other => {
            return Err(Error::Other(format!(
                "ledger envelope: unknown protocol tag {other}"
            )));
        }
    };
    let diagnostics = decode_diagnostics(reader)?;
    Ok(ErrorDigest {
        kind,
        scope,
        operation,
        provider,
        protocol,
        diagnostics,
    })
}

fn unknown_tag(what: &str, tag: u8) -> Error {
    Error::Other(format!("ledger envelope: unknown {what} tag {tag}"))
}

fn write_count(out: &mut Vec<u8>, count: usize, what: &str) {
    let count = u32::try_from(count)
        .unwrap_or_else(|_| panic!("ledger envelope: {what} exceed the u32 count limit"));
    out.extend_from_slice(&count.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bifrost_types::wire::ledger::decode_resource;
    use bifrost_types::{AccountErrorKind, CursorScope};

    use super::{LEDGER_ENVELOPE_VERSION, decode_ledger, encode_ledger};
    use crate::cursor::ledger::{DebtLedger, DischargeAudit, DischargeEvidence, ErrorDigest};
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
            error: bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Request(
                bifrost_types::RequestCause::Malformed {
                    detail: bifrost_types::DiagnosticText::support_only("unrepresentable"),
                },
            ))
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
            crate::cursor::WalkGeneration::for_test(1),
            100,
        );
        let root = bifrost_types::ObligationKey(b"root".to_vec());
        let a = bifrost_types::ObligationKey(b"a".to_vec());
        ledger
            .replace_obligation(
                &root,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
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

    // Rebuild the historical policy prefix explicitly, rather than changing
    // only the header of a current-shaped record.
    fn historical_entry_row(ledger: &DebtLedger, version: u32, attempts: u32) -> Vec<u8> {
        let entry = ledger.entries().next().expect("one entry");
        let mut prefix = Vec::new();
        super::write_bytes(&mut prefix, &entry.key.0);
        super::encode_domain(&mut prefix, &entry.domain);
        prefix.extend_from_slice(&entry.generation.to_le_bytes());
        super::encode_proof(&mut prefix, &entry.proof);
        let mut bytes = encode_ledger(ledger);
        let counter_at = super::HEADER_LEN + prefix.len() + 1;
        bytes.splice(counter_at..counter_at, attempts.to_le_bytes());
        bytes[4..8].copy_from_slice(&version.to_le_bytes());
        bytes
    }

    #[test]
    fn historical_policy_counters_migrate_only_when_the_lineage_agrees() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(CursorScope::Account),
                vec![replayable("root")],
            ),
            crate::cursor::WalkGeneration::for_test(1),
            100,
        );
        let key = bifrost_types::ObligationKey(b"root".to_vec());
        ledger.record_attempt(&key, 3);
        ledger.record_attempt(&key, 3);
        for version in super::MIN_MIGRATABLE_LEDGER..12 {
            let restored = decode_ledger(&historical_entry_row(&ledger, version, 2))
                .expect("historical budget retained");
            assert_eq!(restored.lineage_budget(&key).expect("budget").attempts, 2);
            assert_eq!(encode_ledger(&restored), encode_ledger(&ledger));
            assert!(matches!(
                decode_ledger(&historical_entry_row(&ledger, version, 1)),
                Err(Error::Other(_))
            ));
        }
    }

    #[test]
    fn historical_barrier_counters_are_inert_for_every_readable_version() {
        let mut ledger = DebtLedger::new();
        let obligation = replayable("barrier");
        let incident = super::BarrierIncident {
            key: obligation.key().clone(),
            domain: bifrost_types::CoverageDomain::full(CursorScope::Account),
            generation: crate::cursor::WalkGeneration::for_test(1),
            failure_label: "unknown extent".into(),
            evidence: obligation.error().into(),
            policy: super::BarrierPolicy::Undecided,
            resume_from: None,
        };
        let mut prefix = Vec::new();
        super::write_bytes(&mut prefix, &incident.key.0);
        super::encode_domain(&mut prefix, &incident.domain);
        prefix.extend_from_slice(&incident.generation.to_le_bytes());
        super::write_string(&mut prefix, &incident.failure_label);
        super::encode_error(&mut prefix, &incident.evidence);
        ledger.record_barrier(incident);
        for version in super::MIN_MIGRATABLE_LEDGER..12 {
            let mut bytes = encode_ledger(&ledger);
            let counter_at = super::HEADER_LEN + prefix.len() + 1;
            bytes.splice(counter_at..counter_at, u32::MAX.to_le_bytes());
            bytes[4..8].copy_from_slice(&version.to_le_bytes());
            let restored = decode_ledger(&bytes).expect("inert barrier count migrates");
            assert_eq!(encode_ledger(&restored), encode_ledger(&ledger));
        }
    }

    /// A whole version-12 row - open split lineage, barrier and budget table -
    /// is the last pre-Owner layout a consumer can hold on disk. It must read
    /// back to the same ledger, and an Owner scope (tag 15) inside it is a
    /// version-13 tag that row could never have carried.
    #[test]
    fn a_whole_version_twelve_ledger_decodes() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(CursorScope::Account),
                vec![replayable("root")],
            ),
            crate::cursor::WalkGeneration::for_test(1),
            100,
        );
        let root = bifrost_types::ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(
                &root,
                &[],
                &[replayable("a"), replayable("b")],
                crate::cursor::WalkGeneration::for_test(1),
                200,
            )
            .expect("replacement accepted");
        ledger.record_attempt(&bifrost_types::ObligationKey(b"a".to_vec()), 3);
        let obligation = replayable("barrier");
        ledger.record_barrier(super::BarrierIncident {
            key: obligation.key().clone(),
            domain: bifrost_types::CoverageDomain::full(CursorScope::Account),
            generation: crate::cursor::WalkGeneration::for_test(1),
            failure_label: "unknown extent".into(),
            evidence: obligation.error().into(),
            policy: super::BarrierPolicy::Undecided,
            resume_from: None,
        });
        let current = encode_ledger(&ledger);
        let mut v12 = current.clone();
        v12[4..8].copy_from_slice(&12u32.to_le_bytes());
        let restored = decode_ledger(&v12).expect("version 12 ledger decodes");
        assert_eq!(restored.entries().count(), ledger.entries().count());
        assert_eq!(restored.barriers().count(), 1);
        assert_eq!(restored.lineages(), ledger.lineages());
        assert_eq!(encode_ledger(&restored), current);

        let mut owned = DebtLedger::new();
        let owner_obligation = bifrost_types::InventoryObligation::Region {
            key: bifrost_types::ObligationKey(b"owner".to_vec()),
            failure_label: "truncated".into(),
            error: bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Request(
                bifrost_types::RequestCause::Malformed {
                    detail: bifrost_types::DiagnosticText::support_only("unrepresentable"),
                },
            ))
            .scope(bifrost_types::ErrorScope::Owner {
                id: bifrost_types::PrincipalId("shared".into()),
            })
            .try_build()
            .expect("valid account error classification"),
            recovery: bifrost_types::RegionRecovery::DurableReplay {
                token: b"tok".to_vec(),
            },
        };
        owned.ingest(
            &bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(CursorScope::Account),
                vec![owner_obligation],
            ),
            crate::cursor::WalkGeneration::for_test(1),
            100,
        );
        let mut owned_v12 = encode_ledger(&owned);
        decode_ledger(&owned_v12).expect("current version reads the Owner scope");
        owned_v12[4..8].copy_from_slice(&12u32.to_le_bytes());
        assert!(decode_ledger(&owned_v12).is_err());
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
            crate::cursor::WalkGeneration::for_test(1),
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
            crate::cursor::WalkGeneration::for_test(9),
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

    // Literal historical digest bytes, rather than a round trip through the
    // current encoder, pin the readable window and absence of invented scope.
    #[test]
    fn legacy_cursor_invalid_digest_keeps_absent_scope() {
        let bytes = [4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        for version in super::MIN_MIGRATABLE_LEDGER..=LEDGER_ENVELOPE_VERSION {
            let mut reader = super::Reader::new(&bytes, version);
            let digest = super::decode_error(&mut reader).expect("historical digest");
            assert_eq!(
                digest.kind,
                AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid)
            );
            assert!(
                digest.scope.is_none(),
                "audit evidence must not acquire a scope"
            );
            let mut encoded = Vec::new();
            super::encode_error(&mut encoded, &digest);
            assert_eq!(encoded, bytes);
        }
    }

    #[test]
    fn namespace_scope_requires_version_eleven() {
        let scope = bifrost_types::ErrorScope::Namespace {
            prefix: "Shared/".into(),
        };
        let mut bytes = Vec::new();
        super::encode_error_scope(&mut bytes, &scope);
        assert_eq!(
            super::decode_error_scope(&mut super::Reader::new(&bytes, 11)).unwrap(),
            scope
        );
        assert!(super::decode_error_scope(&mut super::Reader::new(&bytes, 10)).is_err());
    }

    #[test]
    fn directory_gateway_scope_digest_round_trips_and_requires_version_six() {
        assert_eq!(
            &encode_ledger(&DebtLedger::new())[4..8],
            &super::LEDGER_ENVELOPE_VERSION.to_le_bytes(),
            "writers use the current envelope version",
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
        let error = bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Request(
            bifrost_types::RequestCause::NotFound {
                what: ResourceKind::Event,
                id: Some("public-instance".into()),
            },
        ))
        .operation(AccountOperation::EventGet)
        .scope(ErrorScope::Event {
            id: bifrost_types::EventId("public-instance".into()),
        })
        .try_build()
        .expect("valid error");
        let mut bytes = Vec::new();
        super::encode_error(&mut bytes, &ErrorDigest::from(&error));
        let restored =
            super::decode_error(&mut super::Reader::new(&bytes, 5)).expect("event digest");
        assert_eq!(restored.kind(), error.kind());
        assert_eq!(restored.scope(), error.scope());
        assert_eq!(restored.operation(), error.operation());
        for version in [3, 4] {
            assert!(super::decode_error(&mut super::Reader::new(&bytes, version)).is_err());
            assert!(decode_resource(11, version).is_err());
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
        }
    }
}

#[cfg(test)]
mod partial_commit_scope_tests {
    use super::*;
    use bifrost_types::{
        AccountError, AccountErrorKind, AccountOperation, ErrorScope, ObjectId, Protocol, Provider,
        TelemetryToken,
    };
    #[test]
    fn google_api_error_digests_require_version_nine() {
        const { assert!(LEDGER_ENVELOPE_VERSION >= 9) };
        for protocol in [
            Protocol::GoogleCalendar,
            Protocol::GooglePeople,
            Protocol::GoogleDrive,
        ] {
            let error = AccountError::unsupported(AccountOperation::Search, protocol)
                .into_builder()
                .provider(Provider::Google)
                .request_id("google-request")
                .try_build()
                .unwrap();
            let mut bytes = Vec::new();
            encode_error(&mut bytes, &ErrorDigest::from(&error));
            let restored = decode_error(&mut Reader::new(&bytes, 9)).unwrap();
            assert_eq!(restored.provider(), Some(Provider::Google));
            assert_eq!(restored.protocol(), Some(protocol));
            assert_eq!(
                restored
                    .diagnostics
                    .request_id
                    .as_ref()
                    .map(TelemetryToken::as_str),
                Some("google-request")
            );
            for version in MIN_MIGRATABLE_LEDGER..9 {
                assert!(decode_error(&mut Reader::new(&bytes, version)).is_err());
                assert!(decode_protocol(encode_protocol(protocol), version).is_err());
                assert!(decode_provider(encode_provider(Provider::Google), version).is_err());
                let mut legacy = encode_ledger(&DebtLedger::new());
                legacy[4..8].copy_from_slice(&version.to_le_bytes());
                assert!(decode_ledger(&legacy).is_ok());
            }
        }
    }

    #[test]
    fn ledger_account_closed_v10_tag_round_trips() {
        let kind = AccountErrorKind::AccountClosed;
        let mut bytes = Vec::new();
        encode_error_kind(&mut bytes, &kind);
        assert_eq!(
            decode_error_kind(&mut Reader::new(&bytes, 10)).unwrap(),
            kind
        );
        for version in MIN_MIGRATABLE_LEDGER..10 {
            assert!(decode_error_kind(&mut Reader::new(&bytes, version)).is_err());
        }
    }

    #[test]
    fn ledger_control_wait_ended_v8_tag_round_trips() {
        const { assert!(LEDGER_ENVELOPE_VERSION >= 8) };
        let kind = AccountErrorKind::ControlWaitEnded;
        let mut bytes = Vec::new();
        encode_error_kind(&mut bytes, &kind);
        assert_eq!(
            decode_error_kind(&mut Reader::new(&bytes, 8)).unwrap(),
            kind
        );
        assert!(decode_error_kind(&mut Reader::new(&bytes, 7)).is_err());
    }

    #[test]
    fn ledger_error_scope_v7_tags_round_trip() {
        const { assert!(LEDGER_ENVELOPE_VERSION >= 7) };
        for scope in [
            ErrorScope::Submission {
                id: ObjectId("submission".into()),
            },
            ErrorScope::CloudFile { id: "file".into() },
            ErrorScope::UploadSession,
        ] {
            let mut bytes = Vec::new();
            encode_error_scope(&mut bytes, &scope);
            assert_eq!(
                decode_error_scope(&mut Reader::new(&bytes, 7)).unwrap(),
                scope
            );
            assert!(decode_error_scope(&mut Reader::new(&bytes, 6)).is_err());
        }
    }
}
