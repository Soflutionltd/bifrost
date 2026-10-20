use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, ChangeCursor,
    CursorScope, DiagnosticText, ObjectId, OpaqueChangeState, Protocol, ProtocolKind, RequestCause,
    RequestErrorKind, SyncStateErrorKind, ThreadId,
};

use crate::types::MailboxName;

use super::CompactUidSet;

pub(crate) const ENVELOPE_VERSION: u32 = 1;
const MAGIC: &[u8; 8] = b"IMAPCUR1";

// protocol-specific: this is the IMAP payload carried inside OpaqueChangeState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FolderCursor {
    QResync {
        uidvalidity: u32,
        modseq: u64,
        known_uids: CompactUidSet,
        known_uids_complete: bool,
    },
    Condstore {
        uidvalidity: u32,
        modseq: u64,
        known_uids: CompactUidSet,
    },
    Basic {
        uidvalidity: u32,
        uidnext: u32,
        known_uids: CompactUidSet,
    },
}

impl FolderCursor {
    pub(crate) fn uidvalidity(&self) -> u32 {
        match self {
            Self::QResync { uidvalidity, .. }
            | Self::Condstore { uidvalidity, .. }
            | Self::Basic { uidvalidity, .. } => *uidvalidity,
        }
    }

    pub(crate) fn known_uids(&self) -> &CompactUidSet {
        match self {
            Self::QResync { known_uids, .. }
            | Self::Condstore { known_uids, .. }
            | Self::Basic { known_uids, .. } => known_uids,
        }
    }
}

pub(crate) fn encode_cursor(scope: CursorScope, cursor: &FolderCursor) -> ChangeCursor {
    ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Imap,
            envelope_version: ENVELOPE_VERSION,
            bytes: encode_folder_cursor(cursor),
        },
        advanced_through: None,
        envelope_version: bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
    }
}

pub(crate) fn decode_cursor(cursor: &ChangeCursor) -> Result<FolderCursor, AccountError> {
    if cursor.server_state.protocol != ProtocolKind::Imap {
        return Err(schema_incompatible(
            "cursor protocol mismatch: expected IMAP",
        ));
    }
    if cursor.server_state.envelope_version != ENVELOPE_VERSION {
        return Err(schema_incompatible(&format!(
            "unsupported IMAP cursor envelope version {}",
            cursor.server_state.envelope_version
        )));
    }
    decode_folder_cursor(&cursor.server_state.bytes)
}

fn encode_folder_cursor(cursor: &FolderCursor) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    match cursor {
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            known_uids,
            known_uids_complete,
        } => {
            out.push(1);
            push_u32(&mut out, *uidvalidity);
            push_u64(&mut out, *modseq);
            encode_uid_set(&mut out, known_uids);
            out.push(u8::from(*known_uids_complete));
        }
        FolderCursor::Condstore {
            uidvalidity,
            modseq,
            known_uids,
        } => {
            out.push(2);
            push_u32(&mut out, *uidvalidity);
            push_u64(&mut out, *modseq);
            encode_uid_set(&mut out, known_uids);
        }
        FolderCursor::Basic {
            uidvalidity,
            uidnext,
            known_uids,
        } => {
            out.push(3);
            push_u32(&mut out, *uidvalidity);
            push_u32(&mut out, *uidnext);
            encode_uid_set(&mut out, known_uids);
        }
    }
    out
}

fn decode_folder_cursor(bytes: &[u8]) -> Result<FolderCursor, AccountError> {
    let mut input = CursorBytes::new(bytes);
    input.expect_magic()?;
    let tag = input.take_u8()?;
    match tag {
        1 => {
            let uidvalidity = input.take_u32()?;
            let modseq = input.take_u64()?;
            // Three on-disk shapes are accepted:
            //   * pre-baseline v1: no bytes after modseq -> empty set,
            //     incomplete (legacy cursors written before known_uids
            //     was tracked at all)
            //   * mid-PR shape: uid set bytes only, no completeness
            //     byte -> assume complete (the only writer that emits
            //     this shape hardcodes `complete: true`)
            //   * current shape: uid set bytes followed by a u8 flag
            let (known_uids, known_uids_complete) = if input.remaining() == 0 {
                (CompactUidSet::default(), false)
            } else {
                let set = input.take_uid_set()?;
                let complete = if input.remaining() == 0 {
                    true
                } else {
                    input.take_u8()? != 0
                };
                (set, complete)
            };
            Ok(FolderCursor::QResync {
                uidvalidity,
                modseq,
                known_uids,
                known_uids_complete,
            })
        }
        2 => Ok(FolderCursor::Condstore {
            uidvalidity: input.take_u32()?,
            modseq: input.take_u64()?,
            known_uids: input.take_uid_set()?,
        }),
        3 => Ok(FolderCursor::Basic {
            uidvalidity: input.take_u32()?,
            uidnext: input.take_u32()?,
            known_uids: input.take_uid_set()?,
        }),
        _ => Err(schema_incompatible("unknown IMAP cursor tag")),
    }
}

fn encode_uid_set(out: &mut Vec<u8>, set: &CompactUidSet) {
    push_u32(out, u32::try_from(set.ranges().len()).unwrap_or(u32::MAX));
    for range in set.ranges() {
        push_u32(out, range.start);
        push_u32(out, range.end.unwrap_or(0));
    }
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct CursorBytes<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CursorBytes<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self) -> Result<(), AccountError> {
        let magic = self.take(MAGIC.len())?;
        if magic == MAGIC {
            Ok(())
        } else {
            Err(schema_incompatible("IMAP cursor magic bytes did not match"))
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], AccountError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| schema_incompatible("IMAP cursor byte offset overflow"))?;
        let out = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| schema_incompatible("IMAP cursor data truncated"))?;
        self.offset = end;
        Ok(out)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take_u8(&mut self) -> Result<u8, AccountError> {
        self.take(1).map(|b| b[0])
    }

    fn take_u32(&mut self) -> Result<u32, AccountError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| schema_incompatible("IMAP cursor u32 field malformed"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn take_u64(&mut self) -> Result<u64, AccountError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| schema_incompatible("IMAP cursor u64 field malformed"))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn take_uid_set(&mut self) -> Result<CompactUidSet, AccountError> {
        let count = self.take_u32()? as usize;
        // A range consumes two u32 fields. Do not reserve from the
        // untrusted declared count when the payload cannot contain that
        // many ranges.
        let mut ranges = Vec::with_capacity(count.min(self.remaining() / 8));
        for _ in 0..count {
            let start = self.take_u32()?;
            let end = self.take_u32()?;
            // `end == 0` is the single-UID sentinel (UIDs are nz-number,
            // so 0 can never be a real range end). Any other `end < start`
            // is a genuinely malformed range that would otherwise build a
            // backwards `UidRange`; reject it rather than silently decode
            // a corrupt cursor into a nonsensical set.
            let range = if end == 0 {
                crate::types::UidRange::try_single(start)
            } else if end < start {
                return Err(schema_incompatible(
                    "IMAP cursor UID range has end before start",
                ));
            } else {
                crate::types::UidRange::try_range(start, end)
            }
            .ok_or_else(|| schema_incompatible("IMAP cursor UID range contains a zero UID"))?;
            ranges.push(range);
        }
        Ok(CompactUidSet::from_ranges(ranges))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedObjectId {
    pub(crate) folder: MailboxName,
    pub(crate) uidvalidity: u32,
    pub(crate) uid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedThreadId {
    pub(crate) folder: MailboxName,
    pub(crate) uidvalidity: u32,
    pub(crate) uids: Vec<u32>,
}

pub(crate) fn encode_object_id(folder: &MailboxName, uidvalidity: u32, uid: u32) -> ObjectId {
    ObjectId(format!(
        "imap2:{}:{}:{}:{}",
        folder.account_id().len(),
        folder.account_id(),
        uidvalidity,
        uid
    ))
}

pub(crate) fn decode_object_id(
    id: &ObjectId,
    op: AccountOperation,
) -> Result<DecodedObjectId, AccountError> {
    let (folder, rest) = decode_len_prefixed("imap1", "imap2", &id.0, op)?;
    let mut parts = rest.split(':');
    let uidvalidity = parse_nonzero_u32(parts.next(), "IMAP object id uidvalidity", op)?;
    let uid = parse_nonzero_u32(parts.next(), "IMAP object id uid", op)?;
    if parts.next().is_some() {
        return Err(malformed("invalid IMAP object id", op));
    }
    Ok(DecodedObjectId {
        folder,
        uidvalidity,
        uid,
    })
}

pub(crate) fn encode_thread_id(folder: &MailboxName, uidvalidity: u32, uids: &[u32]) -> ThreadId {
    let mut uids = uids.to_vec();
    uids.sort_unstable();
    uids.dedup();
    let uid_list = uids
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    ThreadId(format!(
        "imapthread2:{}:{}:{}:{}",
        folder.account_id().len(),
        folder.account_id(),
        uidvalidity,
        uid_list
    ))
}

pub(crate) fn decode_thread_id(
    id: &ThreadId,
    op: AccountOperation,
) -> Result<DecodedThreadId, AccountError> {
    let (folder, rest) = decode_len_prefixed("imapthread1", "imapthread2", &id.0, op)?;
    let mut parts = rest.splitn(2, ':');
    let uidvalidity = parse_nonzero_u32(parts.next(), "IMAP thread id uidvalidity", op)?;
    let uid_part = parts
        .next()
        .ok_or_else(|| malformed("missing IMAP thread uid set", op))?;
    let mut uids = Vec::new();
    for uid in uid_part.split(',').filter(|part| !part.is_empty()) {
        let uid = uid
            .parse::<u32>()
            .map_err(|_| malformed("invalid IMAP thread uid", op))?;
        if uid == 0 {
            return Err(malformed("invalid IMAP thread uid", op));
        }
        uids.push(uid);
    }
    if uids.is_empty() {
        return Err(malformed("empty IMAP thread uid set", op));
    }
    Ok(DecodedThreadId {
        folder,
        uidvalidity,
        uids,
    })
}

fn decode_len_prefixed(
    legacy_prefix: &str,
    wire_prefix: &str,
    value: &str,
    op: AccountOperation,
) -> Result<(MailboxName, String), AccountError> {
    let wire_identity = value.starts_with(&format!("{wire_prefix}:"));
    let prefix = if wire_identity {
        wire_prefix
    } else {
        legacy_prefix
    };
    let value = value
        .strip_prefix(prefix)
        .and_then(|v| v.strip_prefix(':'))
        .ok_or_else(|| malformed("invalid IMAP id prefix", op))?;
    let Some((len, rest)) = value.split_once(':') else {
        return Err(malformed("invalid IMAP id length", op));
    };
    let len = len
        .parse::<usize>()
        .map_err(|_| malformed("invalid IMAP id length", op))?;
    let folder = rest
        .get(..len)
        .ok_or_else(|| malformed("invalid IMAP id folder length", op))?;
    let after = rest
        .get(len..)
        .and_then(|v| v.strip_prefix(':'))
        .ok_or_else(|| malformed("invalid IMAP id separator", op))?;
    let folder = if wire_identity {
        MailboxName::from_account_id(folder)
    } else {
        MailboxName::new(folder)
    }
    .map_err(|e| malformed(&e.to_string(), op))?;
    Ok((folder, after.to_owned()))
}

fn parse_u32(value: Option<&str>, op: AccountOperation) -> Result<u32, AccountError> {
    value
        .ok_or_else(|| malformed("missing IMAP id field", op))?
        .parse::<u32>()
        .map_err(|_| malformed("invalid IMAP id integer", op))
}

/// UIDs and UIDVALIDITY are RFC 9051 `nz-number`s: this crate never mints
/// a 0, so a 0 on input is a corrupted or foreign id. Rejecting it here
/// closes every downstream hole at once: a wire operand cannot carry 0, so
/// an accepted 0 otherwise fabricates a `Succeeded(Applied)` outcome for a
/// uid the STORE/MOVE never targeted and, for an all-zero group, skips the
/// group so its ids get no outcome at all. The operand layer refuses a 0
/// independently (`UidOperand` reports it as excluded, and the callers turn
/// that into a failure), so neither boundary depends on the other.
fn parse_nonzero_u32(
    value: Option<&str>,
    what: &str,
    op: AccountOperation,
) -> Result<u32, AccountError> {
    let parsed = parse_u32(value, op)?;
    if parsed == 0 {
        return Err(malformed(&format!("{what} must be nonzero"), op));
    }
    Ok(parsed)
}

/// Build a `Request(Malformed)` `AccountError` for IMAP object-id or
/// cursor decode failures. These are caller-side invalid inputs (the
/// object id was not produced by this crate or was corrupted in transit).
///
/// The operation is threaded in from the caller: `decode_object_id` and
/// `decode_thread_id` are always reached while servicing one specific
/// account-trait call (hydration, a mutation lane, a blob open, a PIM
/// primitive), so the op is known at every call site and the resulting
/// error carries it. The recovery class is `ClientBug` either way; the
/// operation is what makes the telemetry attributable to a lane.
pub(super) fn malformed(detail: &str, op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.to_owned()),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// Build a `SyncState(SchemaIncompatible)` `AccountError` for IMAP
/// cursor envelope decoding failures. The cursor bytes are structurally
/// unrecognizable, which triggers the `SchemaIncompatible` engine
/// directive to clear cursor state and re-establish from inventory.
fn schema_incompatible(detail: &str) -> AccountError {
    use bifrost_types::StateCause;
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::EstablishCursor)
    .text(DiagnosticText::support_only(detail.to_owned()))
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    #[test]
    fn legacy_reserved_prefix_folders_remain_literal() {
        let encoded = MailboxName::from_wire("a".into(), b"&AGE-", false).account_id();
        for literal in [encoded.as_str(), "~imap-wire:invalid!"] {
            let folder = MailboxName::new(literal).unwrap();
            let object = ObjectId(format!("imap1:{}:{literal}:7:9", literal.len()));
            let thread = ThreadId(format!("imapthread1:{}:{literal}:7:9", literal.len()));
            assert_eq!(
                decode_object_id(&object, AccountOperation::SyncChanges)
                    .unwrap()
                    .folder,
                folder
            );
            assert_eq!(
                decode_thread_id(&thread, AccountOperation::SyncChanges)
                    .unwrap()
                    .folder,
                folder
            );
            let object = encode_object_id(&folder, 7, 9);
            let thread = encode_thread_id(&folder, 7, &[9]);
            assert!(object.0.starts_with("imap2:"));
            assert!(thread.0.starts_with("imapthread2:"));
            assert_eq!(
                decode_object_id(&object, AccountOperation::SyncChanges)
                    .unwrap()
                    .folder,
                folder
            );
            assert_eq!(
                decode_thread_id(&thread, AccountOperation::SyncChanges)
                    .unwrap()
                    .folder,
                folder
            );
        }
    }

    #[test]
    fn hierarchy_edits_keep_noncanonical_parent_octets() {
        let parent = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let child = parent.child('/', "b").unwrap();
        assert_eq!(child.wire_bytes(false), Some(b"&AGE-/b".as_slice()));
        let recovered = child.parent('/').unwrap().unwrap();
        assert_eq!(recovered, parent);
        assert_eq!(recovered.wire_bytes(false), parent.wire_bytes(false));
        assert!(parent.child('é', "b").is_err());
    }

    #[test]
    fn object_ids_roundtrip_noncanonical_wire_addresses_without_collision() {
        let a = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let b = MailboxName::new("a").unwrap();
        let op = AccountOperation::SyncChanges;
        let a_id = encode_object_id(&a, 7, 9);
        assert_ne!(a_id, encode_object_id(&b, 7, 9));
        let decoded = decode_object_id(&a_id, op).unwrap();
        assert_eq!(decoded.folder, a);
        assert_eq!(decoded.folder.wire_bytes(false), Some(b"&AGE-".as_slice()));
        let controls = MailboxName::from_wire("line\r\nbreak".into(), b"line\r\nbreak", true);
        let restored = MailboxName::from_account_id(&controls.account_id()).unwrap();
        assert_eq!(restored.as_str(), controls.as_str());
        assert_eq!(restored.account_id(), controls.account_id());
        assert!(restored.can_reencode());
        let reserved = MailboxName::new("~imap-wire:literal").unwrap();
        assert_eq!(
            MailboxName::from_account_id(&reserved.account_id()).unwrap(),
            reserved
        );
    }

    use super::*;

    #[test]
    fn cursor_envelope_roundtrips_all_tiers() {
        let cursors = [
            FolderCursor::QResync {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::from_uids([1, 2]),
                known_uids_complete: true,
            },
            FolderCursor::Condstore {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::from_uids([1, 3, 4]),
            },
            FolderCursor::Basic {
                uidvalidity: 7,
                uidnext: 20,
                known_uids: CompactUidSet::from_uids([1, 2, 10]),
            },
        ];

        for cursor in cursors {
            let change = encode_cursor(CursorScope::Account, &cursor);
            assert_eq!(decode_cursor(&change).expect("decode"), cursor);
        }
    }

    #[test]
    fn cursor_envelope_rejects_version_drift() {
        let mut change = encode_cursor(
            CursorScope::Account,
            &FolderCursor::QResync {
                uidvalidity: 1,
                modseq: 2,
                known_uids: CompactUidSet::default(),
                known_uids_complete: true,
            },
        );
        change.server_state.envelope_version = ENVELOPE_VERSION + 1;
        let err = decode_cursor(&change).expect_err("should fail on version mismatch");
        assert!(
            matches!(
                err.kind(),
                AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::SchemaIncompatible)
            ),
            "unexpected kind: {:?}",
            err.kind()
        );
    }

    #[test]
    fn qresync_cursor_decodes_legacy_payload_without_known_uids() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(1);
        push_u32(&mut bytes, 7);
        push_u64(&mut bytes, 99);

        let cursor = decode_folder_cursor(&bytes).expect("decode legacy qresync");
        assert_eq!(
            cursor,
            FolderCursor::QResync {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::default(),
                known_uids_complete: false,
            }
        );
    }

    #[test]
    fn qresync_cursor_decodes_mid_pr_payload_without_complete_flag() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(1);
        push_u32(&mut bytes, 7);
        push_u64(&mut bytes, 99);
        encode_uid_set(&mut bytes, &CompactUidSet::from_uids([1, 2]));

        let cursor = decode_folder_cursor(&bytes).expect("decode mid-pr qresync");
        assert_eq!(
            cursor,
            FolderCursor::QResync {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::from_uids([1, 2]),
                known_uids_complete: true,
            }
        );
    }

    #[test]
    fn qresync_cursor_roundtrips_incomplete_baseline() {
        let cursor = FolderCursor::QResync {
            uidvalidity: 7,
            modseq: 99,
            known_uids: CompactUidSet::default(),
            known_uids_complete: false,
        };
        let change = encode_cursor(CursorScope::Account, &cursor);
        assert_eq!(decode_cursor(&change).expect("decode"), cursor);
    }

    #[test]
    fn cursor_uid_set_rejects_backwards_range() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(3); // Basic
        push_u32(&mut bytes, 7); // uidvalidity
        push_u32(&mut bytes, 20); // uidnext
        push_u32(&mut bytes, 1); // one range
        push_u32(&mut bytes, 10); // start
        push_u32(&mut bytes, 5); // end < start, end != 0 -> malformed
        let err = decode_folder_cursor(&bytes).expect_err("backwards range must be rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[test]
    fn cursor_uid_set_rejects_zero_without_panicking() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(3); // Basic
        push_u32(&mut bytes, 7); // uidvalidity
        push_u32(&mut bytes, 20); // uidnext
        push_u32(&mut bytes, 1); // one range
        push_u32(&mut bytes, 0); // invalid zero start
        push_u32(&mut bytes, 0); // single-UID sentinel
        let err = decode_folder_cursor(&bytes).expect_err("zero UID must be rejected");
        assert!(is_schema_incompatible(&err));
    }

    #[test]
    fn cursor_uid_set_does_not_reserve_from_an_untrusted_count() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(3); // Basic
        push_u32(&mut bytes, 7); // uidvalidity
        push_u32(&mut bytes, 20); // uidnext
        push_u32(&mut bytes, u32::MAX); // impossible declared range count
        let err = decode_folder_cursor(&bytes).expect_err("truncated uid set");
        assert!(is_schema_incompatible(&err));
    }

    #[test]
    fn object_ids_tolerate_colons_in_folder_names() {
        let folder = MailboxName::new("Work:Clients").expect("valid folder");
        let id = encode_object_id(&folder, 10, 42);
        let decoded = decode_object_id(&id, AccountOperation::Hydrate).expect("object id");
        assert_eq!(decoded.folder, folder);
        assert_eq!(decoded.uidvalidity, 10);
        assert_eq!(decoded.uid, 42);
    }

    #[test]
    fn thread_id_tolerates_colons_in_folder_names() {
        let folder = MailboxName::new("Work:Clients").expect("valid folder");
        let id = encode_thread_id(&folder, 10, &[3, 1, 3]);
        let decoded = decode_thread_id(&id, AccountOperation::HydrateThread).expect("thread id");
        assert_eq!(decoded.folder, folder);
        assert_eq!(decoded.uidvalidity, 10);
        assert_eq!(decoded.uids, vec![1, 3]);
    }

    /// UID 0 is not an RFC 9051 `nz-number` and this crate never mints it.
    /// Accepting it at decode used to open a family of downstream holes:
    /// a wire operand cannot carry 0, so a mixed
    /// mutation batch reported a fabricated `Succeeded(Applied)` for the
    /// uid-0 id, an all-zero group produced NO outcome for its ids, and
    /// `open_raw_rfc822` turned a uid-0 id into a successful empty stream.
    /// Rejecting at the decode boundary closes them all at once.
    #[test]
    fn object_and_thread_ids_reject_uid_zero_and_uidvalidity_zero() {
        let err = decode_object_id(
            &ObjectId("imap1:5:INBOX:7:0".into()),
            AccountOperation::Hydrate,
        )
        .expect_err("uid 0 must be rejected");
        assert!(is_malformed(&err), "kind: {:?}", err.kind());
        let err = decode_object_id(
            &ObjectId("imap1:5:INBOX:0:7".into()),
            AccountOperation::Hydrate,
        )
        .expect_err("uidvalidity 0 must be rejected");
        assert!(is_malformed(&err), "kind: {:?}", err.kind());
        let err = decode_thread_id(
            &ThreadId("imapthread1:5:INBOX:7:1,0,3".into()),
            AccountOperation::HydrateThread,
        )
        .expect_err("thread uid 0 must be rejected");
        assert!(is_malformed(&err), "kind: {:?}", err.kind());
        let err = decode_thread_id(
            &ThreadId("imapthread1:5:INBOX:0:1,3".into()),
            AccountOperation::HydrateThread,
        )
        .expect_err("thread uidvalidity 0 must be rejected");
        assert!(is_malformed(&err), "kind: {:?}", err.kind());
    }

    fn is_schema_incompatible(err: &AccountError) -> bool {
        matches!(
            err.kind(),
            AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::SchemaIncompatible)
        )
    }

    fn is_malformed(err: &AccountError) -> bool {
        matches!(
            err.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        )
    }

    // A cursor minted by another protocol must never be decoded as IMAP
    // bytes: the protocol tag is checked before the payload is touched.
    #[test]
    fn cursor_envelope_rejects_a_foreign_protocol_tag() {
        let mut change = encode_cursor(
            CursorScope::Account,
            &FolderCursor::Basic {
                uidvalidity: 1,
                uidnext: 2,
                known_uids: CompactUidSet::default(),
            },
        );
        change.server_state.protocol = ProtocolKind::Jmap;
        let err = decode_cursor(&change).expect_err("foreign protocol must be rejected");
        assert!(is_schema_incompatible(&err), "kind: {:?}", err.kind());
    }

    #[test]
    fn cursor_payload_rejects_bad_magic_and_unknown_tag() {
        let mut wrong_magic = b"NOTMAGIC".to_vec();
        wrong_magic.push(1);
        let err = decode_folder_cursor(&wrong_magic).expect_err("magic mismatch");
        assert!(is_schema_incompatible(&err));

        let mut unknown_tag = MAGIC.to_vec();
        unknown_tag.push(9);
        let err = decode_folder_cursor(&unknown_tag).expect_err("unknown tag");
        assert!(is_schema_incompatible(&err));

        // Magic present but nothing after it: the tag read itself is short.
        let err = decode_folder_cursor(&MAGIC[..]).expect_err("missing tag");
        assert!(is_schema_incompatible(&err));
    }

    #[test]
    fn cursor_payload_rejects_truncated_fields() {
        // Condstore declares a u64 modseq that is not there.
        let mut bytes = MAGIC.to_vec();
        bytes.push(2);
        push_u32(&mut bytes, 7);
        bytes.extend_from_slice(&[0u8; 3]);
        let err = decode_folder_cursor(&bytes).expect_err("truncated modseq");
        assert!(is_schema_incompatible(&err));

        // Basic declares two UID ranges but supplies bytes for one.
        let mut bytes = MAGIC.to_vec();
        bytes.push(3);
        push_u32(&mut bytes, 7);
        push_u32(&mut bytes, 20);
        push_u32(&mut bytes, 2);
        push_u32(&mut bytes, 1);
        push_u32(&mut bytes, 5);
        let err = decode_folder_cursor(&bytes).expect_err("truncated uid set");
        assert!(is_schema_incompatible(&err));
    }

    #[test]
    fn object_id_decode_rejects_malformed_shapes() {
        for bad in [
            "not-an-imap-id",
            "imap1",
            "imap1:INBOX:1:2",
            "imap1:x:INBOX:1:2",
            "imap1:99:INBOX:1:2",
            "imap1:5:INBOXX1:2",
            "imap1:5:INBOX:1:2:3",
            "imap1:5:INBOX:notanumber:2",
            "imap1:5:INBOX:1",
        ] {
            let err = decode_object_id(&ObjectId(bad.to_owned()), AccountOperation::Hydrate)
                .err()
                .unwrap_or_else(|| panic!("{bad} must not decode"));
            assert!(is_malformed(&err), "{bad}: kind {:?}", err.kind());
        }
    }

    #[test]
    fn object_id_roundtrips_a_multibyte_folder_name() {
        // The length prefix is a BYTE count, so a folder whose name is not
        // pure ASCII must still slice back out on a char boundary.
        let folder = MailboxName::new("Ünread/Läst").expect("valid folder");
        let id = encode_object_id(&folder, 3, 9);
        let decoded =
            decode_object_id(&id, AccountOperation::Hydrate).expect("multibyte folder decodes");
        assert_eq!(decoded.folder, folder);
        assert_eq!(decoded.uidvalidity, 3);
        assert_eq!(decoded.uid, 9);
    }

    #[test]
    fn thread_id_decode_rejects_empty_and_non_numeric_uid_sets() {
        let err = decode_thread_id(
            &ThreadId("imapthread1:5:INBOX:10:".to_owned()),
            AccountOperation::HydrateThread,
        )
        .expect_err("empty uid set");
        assert!(is_malformed(&err));

        let err = decode_thread_id(
            &ThreadId("imapthread1:5:INBOX:10:1,x".to_owned()),
            AccountOperation::HydrateThread,
        )
        .expect_err("non-numeric uid");
        assert!(is_malformed(&err));

        let err = decode_thread_id(
            &ThreadId("imapthread1:5:INBOX".to_owned()),
            AccountOperation::HydrateThread,
        )
        .expect_err("missing uid field");
        assert!(is_malformed(&err));
    }
}
