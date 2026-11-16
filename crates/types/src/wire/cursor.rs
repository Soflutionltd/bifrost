//! Cursor envelope serialization and version-migration entry point.
//!
//! Cursors and backfill checkpoints share the envelope. The wire
//! format is:
//!
//! ```text
//!   1 byte    magic = 0xB5
//!   3 bytes   reserved (zero)
//!   4 bytes   version (little-endian u32)
//!   1 byte    kind (0 = Change, 1 = Backfill)
//!   4 bytes   scope_repr length (little-endian u32)
//!   N bytes   scope_repr
//!   4 bytes   payload length (little-endian u32)
//!   N bytes   payload
//! ```
//!
//! The protocol-owned `OpaqueChangeState::bytes` is itself opaque to
//! the envelope; it nests cleanly.

use crate::{
    BackfillCheckpoint, BackfillProgress, ChangeCursor, Checkpoint, CursorScope, ObjectType,
    OpaqueChangeState, OpaqueProgressBytes, Partition, ProtocolKind,
};

use super::{DecodeError as Error, read_bytes, read_string, write_bytes, write_string};

/// Current envelope version. Bumped each time the layout outside the
/// protocol-owned `OpaqueChangeState::bytes` changes.
pub const ENGINE_VERSION: u32 = crate::CHANGE_CURSOR_ENVELOPE_VERSION;

/// Lowest envelope version still readable by this engine. Engine
/// rejects on-disk envelopes below this with
/// `Error::SchemaIncompatible`; the consumer must clear the cursor
/// and the engine restarts via inventory.
pub const MIN_MIGRATABLE: u32 = 1;

const MAGIC: u8 = 0xB5;

/// Enum tag this codec will never write. Earlier revisions encoded
/// unknown `ObjectType` / `ProtocolKind` variants as this value, which
/// persisted rows nothing could decode. Encoding now uses exhaustive
/// types-owned tag methods. Decoding still maps the tag to
/// `Error::SchemaIncompatible` so rows written by such a revision heal
/// through the schema-clear path rather than stranding the account.
const TAG_RESERVED: u8 = 0xFF;

/// Envelope kind tag. The migration dispatcher uses this to pick the
/// right fixup chain when `MIN_MIGRATABLE < ENGINE_VERSION` eventually.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EnvelopeKind {
    Change,
    Backfill,
}

impl EnvelopeKind {
    fn tag(self) -> u8 {
        match self {
            Self::Change => 0,
            Self::Backfill => 1,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, Error> {
        match tag {
            0 => Ok(Self::Change),
            1 => Ok(Self::Backfill),
            other => Err(Error::Malformed(format!(
                "cursor envelope: unknown kind tag {other}"
            ))),
        }
    }
}

/// Decoded envelope as the engine sees it before unpacking the inner
/// `Checkpoint`.
#[derive(Debug, Clone)]
pub struct CursorEnvelope {
    pub version: u32,
    pub kind: EnvelopeKind,
    pub scope_repr: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Serialize a `Checkpoint` to envelope bytes. Stable across engine
/// versions sharing the same `ENGINE_VERSION`.
///
/// # Panics
///
/// Panics on a `Checkpoint::Change` whose `ChangeCursor::envelope_version` is not
/// `CHANGE_CURSOR_ENVELOPE_VERSION`. Refusing loudly is deliberate and
/// matches the rest of this codec: the alternative is writing a durable row
/// whose own decoder rejects it, which strands the account instead of
/// healing it. Every engine ingress validates the outer version and maps a
/// mismatch to `Error::SchemaIncompatible` before reaching this function,
/// so the panic is not reachable through the engine - only through a direct
/// call with a hand-built cursor.
#[must_use]
pub fn encode_envelope(checkpoint: &Checkpoint) -> Vec<u8> {
    match checkpoint {
        Checkpoint::Change(c) => encode_change(c),
        Checkpoint::Backfill(b) => encode_backfill(b),
    }
}

fn encode_change(c: &ChangeCursor) -> Vec<u8> {
    assert!(
        c.validate_envelope().is_ok(),
        "cursor envelope: unsupported ChangeCursor envelope version"
    );
    let scope = encode_scope(&c.scope);
    let payload = encode_change_payload(c);
    pack_envelope(
        EnvelopeKind::Change,
        c.envelope_version.get(),
        &scope,
        &payload,
    )
}

fn encode_backfill(b: &BackfillCheckpoint) -> Vec<u8> {
    let scope = encode_scope(&b.scope);
    let payload = encode_backfill_payload(b);
    pack_envelope(EnvelopeKind::Backfill, ENGINE_VERSION, &scope, &payload)
}

/// Pack one engine-owned envelope. Change cursors pass their validated
/// trait-owned outer version; backfill checkpoints pass `ENGINE_VERSION`.
fn pack_envelope(kind: EnvelopeKind, version: u32, scope: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(13 + scope.len() + payload.len());
    out.push(MAGIC);
    out.extend_from_slice(&[0u8, 0, 0]);
    out.extend_from_slice(&version.to_le_bytes());
    out.push(kind.tag());
    // Lengths are u32 little-endian; envelopes are always shorter
    // than 4 GB and a varint would buy nothing here.
    let scope_len =
        u32::try_from(scope.len()).expect("cursor envelope: scope exceeds u32 length limit");
    out.extend_from_slice(&scope_len.to_le_bytes());
    out.extend_from_slice(scope);
    let payload_len =
        u32::try_from(payload.len()).expect("cursor envelope: payload exceeds u32 length limit");
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode an envelope and unpack into a `Checkpoint`.
///
/// Errors:
/// - `Error::Malformed` for malformed bytes (missing magic, truncated
///   header).
/// - `Error::SchemaIncompatible` for a version outside
///   `[MIN_MIGRATABLE, ENGINE_VERSION]`.
///
/// Both out-of-range directions are `SchemaIncompatible` because both
/// describe the same situation: a durable row this revision cannot
/// read. That is the only classification the healing paths key on -
/// `establish_one` deletes the row and re-establishes the scope,
/// `run_establish` raises `Engine(SchemaIncompatible)` for the
/// account-wide schema-clear loop. An `Error::Malformed` here instead would
/// propagate as an ordinary establish failure, burn the reopen budget,
/// broadcast `Terminated`, and leave the unreadable row on disk for
/// every subsequent attach to trip over identically.
///
/// A version inside the window is migrated, not merely accepted: every
/// `Checkpoint::Change` this returns satisfies `validate_envelope`. See
/// `migrate_change_cursor` for why decode owns that boundary.
pub fn decode_envelope(bytes: &[u8]) -> Result<Checkpoint, Error> {
    let env = parse_envelope(bytes)?;
    if env.version < MIN_MIGRATABLE || env.version > ENGINE_VERSION {
        return Err(Error::SchemaIncompatible);
    }
    let scope = decode_scope(&env.scope_repr)?;
    match env.kind {
        EnvelopeKind::Change => {
            let cursor = decode_change_payload(scope, env.version, &env.payload)?;
            Ok(Checkpoint::Change(cursor))
        }
        EnvelopeKind::Backfill => {
            let bf = decode_backfill_payload(scope, env.version, &env.payload)?;
            Ok(Checkpoint::Backfill(bf))
        }
    }
}

fn parse_envelope(bytes: &[u8]) -> Result<CursorEnvelope, Error> {
    if bytes.len() < 13 {
        return Err(Error::Malformed("cursor envelope: truncated header".into()));
    }
    if bytes[0] != MAGIC {
        return Err(Error::Malformed("cursor envelope: bad magic".into()));
    }
    // Reserved bytes 1..4 must be zero. Future versions can repurpose
    // them; until then any non-zero value indicates a corrupted or
    // future-versioned envelope this engine cannot read.
    if bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 {
        return Err(Error::Malformed(
            "cursor envelope: reserved header bytes non-zero".into(),
        ));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let kind = EnvelopeKind::from_tag(bytes[8])?;
    let scope_len = u32::from_le_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]) as usize;
    let scope_start: usize = 13;
    let scope_end = scope_start
        .checked_add(scope_len)
        .ok_or_else(|| Error::Malformed("cursor envelope: scope length overflow".into()))?;
    let payload_start = scope_end
        .checked_add(4)
        .ok_or_else(|| Error::Malformed("cursor envelope: scope length overflow".into()))?;
    if bytes.len() < payload_start {
        return Err(Error::Malformed("cursor envelope: truncated scope".into()));
    }
    let payload_len_bytes = &bytes[scope_end..payload_start];
    let payload_len = u32::from_le_bytes([
        payload_len_bytes[0],
        payload_len_bytes[1],
        payload_len_bytes[2],
        payload_len_bytes[3],
    ]) as usize;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| Error::Malformed("cursor envelope: payload length overflow".into()))?;
    if bytes.len() < payload_end {
        return Err(Error::Malformed(
            "cursor envelope: truncated payload".into(),
        ));
    }
    // One stored row is exactly one envelope; accepting a prefix hides corruption.
    if bytes.len() != payload_end {
        return Err(Error::Malformed("cursor envelope: trailing bytes".into()));
    }
    Ok(CursorEnvelope {
        version,
        kind,
        scope_repr: bytes[scope_start..scope_end].to_vec(),
        payload: bytes[payload_start..payload_end].to_vec(),
    })
}

// ---------- scope codec ----------

const SCOPE_ACCOUNT: u8 = 0;
const SCOPE_TYPE: u8 = 1;
const SCOPE_QUERY: u8 = 2;
const SCOPE_FOLDER: u8 = 3;
const SCOPE_FOLDER_TYPE: u8 = 4;

const OBJTYPE_EMAIL: u8 = 0;
const OBJTYPE_MAILBOX: u8 = 1;
const OBJTYPE_THREAD: u8 = 2;
const OBJTYPE_EVENT: u8 = 3;
const OBJTYPE_CONTACT: u8 = 4;
const OBJTYPE_EMAIL_SUBMISSION: u8 = 5;
const OBJTYPE_CALENDAR_EVENT: u8 = 6;
const OBJTYPE_CONTACT_GROUP: u8 = 7;

/// Encode a cursor scope using stable storage tags.
#[must_use]
pub fn encode_scope(scope: &CursorScope) -> Vec<u8> {
    let mut out = Vec::new();
    match scope {
        CursorScope::Account => out.push(SCOPE_ACCOUNT),
        CursorScope::Type(ty) => {
            out.push(SCOPE_TYPE);
            out.push(encode_obj_type(*ty));
        }
        CursorScope::Query(q) => {
            out.push(SCOPE_QUERY);
            write_string(&mut out, &q.0);
        }
        CursorScope::Folder(f) => {
            out.push(SCOPE_FOLDER);
            write_string(&mut out, &f.0);
        }
        CursorScope::FolderType { folder, ty } => {
            out.push(SCOPE_FOLDER_TYPE);
            write_string(&mut out, &folder.0);
            out.push(encode_obj_type(*ty));
        }
    }
    out
}

/// Decode a stored cursor scope, retaining reserved-tag schema recovery.
pub fn decode_scope(bytes: &[u8]) -> Result<CursorScope, Error> {
    if bytes.is_empty() {
        return Err(Error::Malformed("cursor envelope: empty scope".into()));
    }
    let tag = bytes[0];
    let rest = &bytes[1..];
    let (scope, consumed) = match tag {
        SCOPE_ACCOUNT => (CursorScope::Account, 0),
        SCOPE_TYPE => {
            let ty = rest.first().ok_or_else(|| {
                Error::Malformed("cursor envelope: type scope missing object type".into())
            })?;
            (CursorScope::Type(decode_obj_type(*ty)?), 1)
        }
        SCOPE_QUERY => {
            let (s, consumed) = read_string(rest)?;
            (CursorScope::Query(crate::QueryId(s)), consumed)
        }
        SCOPE_FOLDER => {
            let (s, consumed) = read_string(rest)?;
            (CursorScope::Folder(crate::FolderId(s)), consumed)
        }
        SCOPE_FOLDER_TYPE => {
            let (folder, consumed) = read_string(rest)?;
            let ty = rest.get(consumed).ok_or_else(|| {
                Error::Malformed("cursor envelope: folder-type scope missing object type".into())
            })?;
            (
                CursorScope::FolderType {
                    folder: crate::FolderId(folder),
                    ty: decode_obj_type(*ty)?,
                },
                consumed + 1,
            )
        }
        other => {
            return Err(Error::Malformed(format!(
                "cursor envelope: unknown scope tag {other}"
            )));
        }
    };
    // Scope fields have no padding: a stored checkpoint must not silently
    // resume from only a recognized prefix of a corrupted field.
    if consumed != rest.len() {
        return Err(Error::Malformed(
            "cursor envelope: trailing scope bytes".into(),
        ));
    }
    Ok(scope)
}

fn encode_obj_type(ty: ObjectType) -> u8 {
    ty.cursor_tag()
}

fn decode_obj_type(tag: u8) -> Result<ObjectType, Error> {
    if tag == TAG_RESERVED {
        return Err(Error::SchemaIncompatible);
    }
    match tag {
        OBJTYPE_EMAIL => Ok(ObjectType::Email),
        OBJTYPE_MAILBOX => Ok(ObjectType::Mailbox),
        OBJTYPE_THREAD => Ok(ObjectType::Thread),
        OBJTYPE_EVENT => Ok(ObjectType::Event),
        OBJTYPE_CONTACT => Ok(ObjectType::Contact),
        OBJTYPE_EMAIL_SUBMISSION => Ok(ObjectType::EmailSubmission),
        OBJTYPE_CALENDAR_EVENT => Ok(ObjectType::CalendarEvent),
        OBJTYPE_CONTACT_GROUP => Ok(ObjectType::ContactGroup),
        other => Err(Error::Malformed(format!(
            "cursor envelope: unknown object type tag {other}"
        ))),
    }
}

// ---------- change-cursor payload codec ----------

// These persisted cursor tags predate the ledger's broader Protocol vocabulary.
// For example Graph is 3 here and 6 in the ledger. A semantic From conversion
// must never be used to substitute one codec's storage tags for the other's.
const PROTOCOL_JMAP: u8 = 0;
const PROTOCOL_IMAP: u8 = 1;
const PROTOCOL_GMAIL: u8 = 2;
const PROTOCOL_GRAPH: u8 = 3;
const PROTOCOL_CARDDAV: u8 = 4;
const PROTOCOL_CALDAV: u8 = 5;

fn encode_protocol(p: ProtocolKind) -> u8 {
    p.cursor_tag()
}

fn decode_protocol(tag: u8) -> Result<ProtocolKind, Error> {
    if tag == TAG_RESERVED {
        return Err(Error::SchemaIncompatible);
    }
    match tag {
        PROTOCOL_JMAP => Ok(ProtocolKind::Jmap),
        PROTOCOL_IMAP => Ok(ProtocolKind::Imap),
        PROTOCOL_GMAIL => Ok(ProtocolKind::Gmail),
        PROTOCOL_CARDDAV => Ok(ProtocolKind::CardDav),
        PROTOCOL_CALDAV => Ok(ProtocolKind::CalDav),
        PROTOCOL_GRAPH => Ok(ProtocolKind::Graph),
        other => Err(Error::Malformed(format!(
            "cursor envelope: unknown protocol tag {other}"
        ))),
    }
}

fn encode_change_payload(c: &ChangeCursor) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(encode_protocol(c.server_state.protocol));
    out.extend_from_slice(&c.server_state.envelope_version.get().to_le_bytes());
    write_bytes(&mut out, &c.server_state.bytes);
    match &c.advanced_through {
        Some(p) => {
            out.push(1);
            write_bytes(&mut out, &p.0);
        }
        None => out.push(0),
    }
    out
}

fn decode_change_payload(
    scope: CursorScope,
    envelope_version: u32,
    bytes: &[u8],
) -> Result<ChangeCursor, Error> {
    if bytes.is_empty() {
        return Err(Error::Malformed(
            "cursor envelope: empty change payload".into(),
        ));
    }
    let protocol = decode_protocol(bytes[0])?;
    if bytes.len() < 5 {
        return Err(Error::Malformed(
            "cursor envelope: truncated change payload".into(),
        ));
    }
    let inner_version = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let (payload, consumed) = read_bytes(&bytes[5..])?;
    let cursor_bytes_end = 5 + consumed;
    if bytes.len() < cursor_bytes_end + 1 {
        return Err(Error::Malformed(
            "cursor envelope: truncated change payload tail".into(),
        ));
    }
    let has_progress = bytes[cursor_bytes_end];
    let tail = &bytes[cursor_bytes_end + 1..];
    let (advanced_through, consumed) = if has_progress == 0 {
        (None, 0)
    } else {
        let (p, consumed) = read_bytes(tail)?;
        (Some(OpaqueProgressBytes(p)), consumed)
    };
    if consumed != tail.len() {
        return Err(Error::Malformed(
            "cursor envelope: trailing change payload bytes".into(),
        ));
    }
    Ok(migrate_change_cursor(
        envelope_version,
        ChangeCursor {
            scope,
            server_state: OpaqueChangeState {
                protocol,
                envelope_version: crate::PayloadVersion::new(inner_version),
                bytes: payload,
            },
            advanced_through,
            envelope_version: crate::TraitEnvelopeVersion::new(envelope_version),
        },
    ))
}

/// Bring a cursor decoded from an accepted historical layout up to the
/// current one, and stamp it.
///
/// Decode is the single migration boundary for the outer envelope. The
/// codec is the only place in the workspace that knows how to read a layout
/// older than the current one, so it is the only place that may hold a
/// `ChangeCursor` whose `envelope_version` is not
/// `CHANGE_CURSOR_ENVELOPE_VERSION`. Everywhere else - the registry, the ack
/// path, `run_establish`, the changes and inventory seams, and any protocol
/// crate about to interpret `OpaqueChangeState::bytes` - a cursor is either
/// current or unusable, and `ChangeCursor::validate_envelope` is a strict
/// equality gate rather than a range check.
///
/// The alternative contract - teaching `validate_envelope` to accept the
/// whole `MIN_MIGRATABLE..=ENGINE_VERSION` window - was rejected. It spreads
/// knowledge of every historical layout across every consumer, and it hands
/// a protocol crate a cursor in a shape that crate has no code to interpret.
/// Leaving the two disagreeing was the live defect: a version-1 row would
/// decode inside the window and then fail an equality gate, so the engine
/// would classify a perfectly migratable durable checkpoint as
/// `SchemaIncompatible` and discard it, forcing a full resync at the first
/// version bump.
///
/// `MIN_MIGRATABLE == ENGINE_VERSION` today, so the chain is empty and this
/// is only the stamp. When `ENGINE_VERSION` is bumped, the per-version fixup
/// goes here, and `tests/envelope_roundtrip.rs` gains a byte fixture for the
/// outgoing layout - the window loop there widens on its own, but only a
/// fixture can prove the fixup reads the old payload correctly.
fn migrate_change_cursor(from_version: u32, mut cursor: ChangeCursor) -> ChangeCursor {
    let _ = from_version;
    cursor.envelope_version =
        crate::TraitEnvelopeVersion::new(crate::CHANGE_CURSOR_ENVELOPE_VERSION);
    cursor
}

// ---------- backfill-checkpoint payload codec ----------

fn encode_backfill_payload(b: &BackfillCheckpoint) -> Vec<u8> {
    let mut out = Vec::new();
    write_bytes(&mut out, &b.partition.0);
    match &b.progress_marker {
        Some(p) => {
            out.push(1);
            write_bytes(&mut out, &p.0);
        }
        None => out.push(0),
    }
    out.extend_from_slice(&b.progress.items_done.to_le_bytes());
    match b.progress.items_estimated {
        Some(n) => {
            out.push(1);
            out.extend_from_slice(&n.to_le_bytes());
        }
        None => out.push(0),
    }
    out
}

fn decode_backfill_payload(
    scope: CursorScope,
    envelope_version: u32,
    bytes: &[u8],
) -> Result<BackfillCheckpoint, Error> {
    let (partition_bytes, consumed) = read_bytes(bytes)?;
    let rest = &bytes[consumed..];
    if rest.is_empty() {
        return Err(Error::Malformed(
            "cursor envelope: truncated backfill payload".into(),
        ));
    }
    let has_progress = rest[0];
    let (progress_marker, after_progress) = if has_progress == 0 {
        (None, &rest[1..])
    } else {
        let (p, n) = read_bytes(&rest[1..])?;
        (Some(OpaqueProgressBytes(p)), &rest[1 + n..])
    };
    if after_progress.len() < 9 {
        return Err(Error::Malformed(
            "cursor envelope: truncated backfill counters".into(),
        ));
    }
    let items_done = u64::from_le_bytes([
        after_progress[0],
        after_progress[1],
        after_progress[2],
        after_progress[3],
        after_progress[4],
        after_progress[5],
        after_progress[6],
        after_progress[7],
    ]);
    let has_estimate = after_progress[8];
    let (items_estimated, consumed) = if has_estimate == 0 {
        (None, 9)
    } else {
        if after_progress.len() < 17 {
            return Err(Error::Malformed(
                "cursor envelope: truncated backfill estimate".into(),
            ));
        }
        (
            Some(u64::from_le_bytes([
                after_progress[9],
                after_progress[10],
                after_progress[11],
                after_progress[12],
                after_progress[13],
                after_progress[14],
                after_progress[15],
                after_progress[16],
            ])),
            17,
        )
    };
    // Counters end the payload; opaque partition and progress bytes remain
    // untouched, but bytes outside those declared fields are corruption.
    if consumed != after_progress.len() {
        return Err(Error::Malformed(
            "cursor envelope: trailing backfill payload bytes".into(),
        ));
    }
    Ok(BackfillCheckpoint {
        scope,
        partition: Partition(partition_bytes),
        progress_marker,
        progress: BackfillProgress {
            items_done,
            items_estimated,
        },
        envelope_version: crate::TraitEnvelopeVersion::new(envelope_version),
    })
}

// ---------- varint-free length-prefixed primitives ----------

#[cfg(test)]
mod migration_tests {
    use super::*;

    fn cursor_at(version: u32) -> ChangeCursor {
        ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Gmail,
                envelope_version: crate::PayloadVersion::new(7),
                bytes: vec![1, 2, 3],
            },
            advanced_through: None,
            envelope_version: crate::TraitEnvelopeVersion::new(version),
        }
    }

    /// A cursor decoded from an older-but-accepted layout must leave the
    /// codec already stamped current, because every consumer downstream gates
    /// on strict equality. `MIN_MIGRATABLE == ENGINE_VERSION` today, so no
    /// real on-disk row can carry an older version yet; the migration entry
    /// point is driven directly with a synthetic one so the stamp is pinned
    /// before the first bump rather than after it.
    #[test]
    fn migration_stamps_the_current_outer_version() {
        let stale = cursor_at(ENGINE_VERSION.saturating_sub(1));
        let migrated = migrate_change_cursor(ENGINE_VERSION.saturating_sub(1), stale.clone());
        assert_eq!(
            migrated.envelope_version.get(),
            crate::CHANGE_CURSOR_ENVELOPE_VERSION
        );
        assert!(
            migrated.validate_envelope().is_ok(),
            "the codec must not hand out a cursor its own consumers reject"
        );
        assert_eq!(
            migrated.server_state, stale.server_state,
            "the protocol-owned payload and its inner version ride through untouched"
        );
        assert_eq!(migrated.scope, stale.scope);
    }
}
