//! Cursor envelope round-trip tests.
//!
//! Asserts:
//! - encode -> decode preserves both `Change` and `Backfill` checkpoints,
//! - rejects bad magic / truncated bodies,
//! - rejects envelope versions outside `[MIN_MIGRATABLE, ENGINE_VERSION]`,
//! - rejects mismatched protocol tags inside the protocol-owned payload
//!   (handled by the protocol crate, but the engine still safely round-trips
//!   the tag).

use bifrost_sync::{ENGINE_VERSION, MIN_MIGRATABLE, decode_envelope, encode_envelope};
use bifrost_types::{
    BackfillCheckpoint, BackfillProgress, ChangeCursor, Checkpoint, CursorScope, FolderId,
    ObjectType, OpaqueChangeState, OpaqueProgressBytes, Partition, ProtocolKind,
};

fn sample_change_cursor() -> ChangeCursor {
    ChangeCursor {
        scope: CursorScope::FolderType {
            folder: FolderId("Inbox".into()),
            ty: ObjectType::Email,
        },
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Graph,
            envelope_version: bifrost_types::PayloadVersion::new(7),
            bytes: b"DELTA_LINK_BYTES".to_vec(),
        },
        advanced_through: Some(OpaqueProgressBytes(b"page=12".to_vec())),
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION),
    }
}

fn sample_backfill_checkpoint() -> BackfillCheckpoint {
    BackfillCheckpoint {
        scope: CursorScope::Type(ObjectType::Email),
        partition: Partition(b"partition-3".to_vec()),
        progress_marker: Some(OpaqueProgressBytes(b"resume-marker".to_vec())),
        progress: BackfillProgress {
            items_done: 12345,
            items_estimated: Some(67890),
        },
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION),
    }
}

#[test]
fn change_cursor_roundtrip() {
    let cursor = sample_change_cursor();
    let ck = Checkpoint::Change(cursor.clone());
    let bytes = encode_envelope(&ck);
    let decoded = decode_envelope(&bytes).expect("decode succeeds");
    match decoded {
        Checkpoint::Change(c) => assert_eq!(c, cursor),
        Checkpoint::Backfill(_) => panic!("expected Change variant"),
        _ => panic!("unknown Checkpoint variant"),
    }
}

#[test]
fn backfill_checkpoint_roundtrip() {
    let bf = sample_backfill_checkpoint();
    let ck = Checkpoint::Backfill(bf.clone());
    let bytes = encode_envelope(&ck);
    let decoded = decode_envelope(&bytes).expect("decode succeeds");
    let Checkpoint::Backfill(decoded_bf) = decoded else {
        panic!("expected Backfill variant");
    };
    assert_eq!(decoded_bf.scope, bf.scope);
    assert_eq!(decoded_bf.partition, bf.partition);
    assert_eq!(decoded_bf.progress_marker, bf.progress_marker);
    assert_eq!(decoded_bf.progress.items_done, bf.progress.items_done);
    assert_eq!(
        decoded_bf.progress.items_estimated,
        bf.progress.items_estimated
    );
    assert_eq!(decoded_bf.envelope_version.get(), bf.envelope_version.get());
}

#[test]
fn change_cursor_without_progress_marker_roundtrips() {
    let mut cursor = sample_change_cursor();
    cursor.advanced_through = None;
    let bytes = encode_envelope(&Checkpoint::Change(cursor.clone()));
    let decoded = decode_envelope(&bytes).expect("decode succeeds");
    let Checkpoint::Change(c) = decoded else {
        panic!("expected Change variant");
    };
    assert_eq!(c.advanced_through, None);
    assert_eq!(c, cursor);
}

#[test]
fn account_scope_roundtrips() {
    let cursor = ChangeCursor {
        scope: CursorScope::Account,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Gmail,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: b"historyId=123456".to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION),
    };
    let bytes = encode_envelope(&Checkpoint::Change(cursor.clone()));
    let decoded = decode_envelope(&bytes).expect("decode succeeds");
    let Checkpoint::Change(c) = decoded else {
        panic!("expected Change variant");
    };
    assert_eq!(c, cursor);
}

#[test]
fn bad_magic_is_rejected() {
    let mut bytes = encode_envelope(&Checkpoint::Change(sample_change_cursor()));
    bytes[0] = 0x00;
    assert!(decode_envelope(&bytes).is_err());
}

#[test]
fn truncated_header_is_rejected() {
    let bytes = encode_envelope(&Checkpoint::Change(sample_change_cursor()));
    let truncated = &bytes[..6];
    assert!(decode_envelope(truncated).is_err());
}

/// An over-version row is as unreadable as an under-version one, and
/// `SchemaIncompatible` is the only classification the two healing
/// paths key on. Classified as anything else the row never heals: the
/// error propagates as an ordinary establish failure, the reopen budget
/// burns, and the next attach trips over the same bytes.
#[test]
fn version_above_engine_is_schema_incompatible() {
    let mut bytes = encode_envelope(&Checkpoint::Change(sample_change_cursor()));
    let bumped = ENGINE_VERSION + 1;
    bytes[4..8].copy_from_slice(&bumped.to_le_bytes());
    let err = decode_envelope(&bytes).expect_err("version above engine should fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("envelope schema is incompatible"),
        "unexpected error: {msg}"
    );
}

/// The trait-owned outer version is preserved and rejected when unknown.
#[test]
fn an_unknown_change_cursor_outer_version_is_detectable() {
    let mut cursor = sample_change_cursor();
    cursor.envelope_version = bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION + 41);
    assert!(cursor.validate_envelope().is_err());
    assert!(
        std::panic::catch_unwind(|| encode_envelope(&Checkpoint::Change(cursor))).is_err(),
        "the public encoder must not produce a row its decoder rejects"
    );
}

/// Decode is the migration boundary, so every version the decoder accepts
/// must come back out as a cursor the strict `validate_envelope` gate - the
/// one the registry, the ack path and `run_establish` all apply before a
/// protocol crate ever sees the payload - accepts.
///
/// Without this, the accepted window and the equality gate disagree: a
/// version-1 row decodes fine, then fails validation, and the engine
/// discards a durable checkpoint it could have migrated. That costs a full
/// resync, and it becomes reachable the moment `ENGINE_VERSION` is bumped.
///
/// The loop is written against the window constants rather than a literal,
/// so it widens on its own at that bump. It cannot, however, prove a future
/// fixup reads an old PAYLOAD correctly - the header version is patched here
/// over current-layout bytes. Bumping `ENGINE_VERSION` therefore also means
/// adding a real byte fixture for the outgoing layout.
#[test]
fn every_accepted_envelope_version_decodes_into_a_cursor_that_validates() {
    for version in MIN_MIGRATABLE..=ENGINE_VERSION {
        let mut bytes = encode_envelope(&Checkpoint::Change(sample_change_cursor()));
        bytes[4..8].copy_from_slice(&version.to_le_bytes());

        let decoded = decode_envelope(&bytes)
            .unwrap_or_else(|e| panic!("version {version} is inside the window but failed: {e:?}"));
        let Checkpoint::Change(cursor) = decoded else {
            panic!("expected Change variant");
        };
        assert!(
            cursor.validate_envelope().is_ok(),
            "version {version} decoded but would be rejected before protocol dispatch",
        );
        assert_eq!(
            cursor.server_state.envelope_version.get(),
            7,
            "the protocol-owned inner version is not touched by outer migration",
        );

        // And it survives the dispatch-side gate end to end: re-encoding the
        // migrated cursor is exactly what the ack path persists.
        let reencoded = encode_envelope(&Checkpoint::Change(cursor));
        assert_eq!(
            u32::from_le_bytes([reencoded[4], reencoded[5], reencoded[6], reencoded[7]]),
            ENGINE_VERSION,
            "a migrated cursor persists at the current layout version",
        );
    }
}

/// Same rule on the backfill lane, which packs through the same helper.
#[test]
fn a_protocol_authored_backfill_version_does_not_reach_the_header() {
    let mut bf = sample_backfill_checkpoint();
    bf.envelope_version = bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION + 41);
    let bytes = encode_envelope(&Checkpoint::Backfill(bf));

    assert_eq!(
        u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        ENGINE_VERSION
    );
    let Checkpoint::Backfill(decoded) = decode_envelope(&bytes).expect("decode succeeds") else {
        panic!("expected Backfill variant");
    };
    assert_eq!(decoded.envelope_version.get(), ENGINE_VERSION);
}

#[test]
fn version_below_min_is_schema_incompatible() {
    if MIN_MIGRATABLE == 0 {
        // Cannot underflow when MIN_MIGRATABLE is already zero. The
        // engine in this case accepts everything down to zero, so the
        // schema-incompatible path is unreachable from a synthetic
        // bytes buffer.
        return;
    }
    let mut bytes = encode_envelope(&Checkpoint::Change(sample_change_cursor()));
    let dropped = MIN_MIGRATABLE - 1;
    bytes[4..8].copy_from_slice(&dropped.to_le_bytes());
    let err = decode_envelope(&bytes).expect_err("version below min should fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("envelope schema is incompatible"),
        "unexpected error: {msg}"
    );
}

#[test]
fn protocol_tag_is_preserved() {
    // Mint cursors for every protocol and assert the protocol tag
    // survives the round trip. This catches an accidental
    // misrouting where the engine hands a JMAP cursor to the Graph
    // impl on dispatch.
    for protocol in [
        ProtocolKind::Jmap,
        ProtocolKind::Imap,
        ProtocolKind::Gmail,
        ProtocolKind::Graph,
    ] {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol,
                envelope_version: bifrost_types::PayloadVersion::new(1),
                bytes: vec![0xAB, 0xCD],
            },
            advanced_through: None,
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION),
        };
        let bytes = encode_envelope(&Checkpoint::Change(cursor.clone()));
        let decoded = decode_envelope(&bytes).expect("decode succeeds");
        let Checkpoint::Change(c) = decoded else {
            panic!("expected Change variant");
        };
        assert_eq!(c.server_state.protocol, protocol);
    }
}
