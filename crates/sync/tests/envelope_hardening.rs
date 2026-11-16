//! Cursor-envelope hardening tests.
//!
//! `envelope_roundtrip.rs` pins the happy paths plus bad-magic /
//! truncated-header / version-window rejection. These tests pin the
//! remaining decoder failure surface byte-by-byte against the wire
//! layout documented in `cursor/envelope.rs`:
//!
//! ```text
//!   [0]      magic 0xB5
//!   [1..4]   reserved (must be zero)
//!   [4..8]   version (LE u32)
//!   [8]      kind tag (0 = Change, 1 = Backfill)
//!   [9..13]  scope_repr length (LE u32)
//!   [13..]   scope_repr, then LE u32 payload length, then payload
//! ```
//!
//! Every mutation below must produce `Err`, never a panic and never a
//! silently-misdecoded checkpoint. A corrupted durable cursor read at
//! attach time flows into `establish_one`, so "fails loudly" here is
//! what keeps a bad store row from becoming a bogus resume point.

use bifrost_sync::{ENGINE_VERSION, decode_envelope, encode_envelope};
use bifrost_types::{
    BackfillCheckpoint, BackfillProgress, ChangeCursor, Checkpoint, CursorScope, ObjectType,
    OpaqueChangeState, OpaqueProgressBytes, Partition, ProtocolKind, QueryId,
};

fn change_envelope(scope: CursorScope) -> Vec<u8> {
    encode_envelope(&Checkpoint::Change(ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: bifrost_types::PayloadVersion::new(3),
            bytes: b"inner".to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION),
    }))
}

fn backfill_envelope() -> Vec<u8> {
    encode_envelope(&Checkpoint::Backfill(BackfillCheckpoint {
        scope: CursorScope::Type(ObjectType::Email),
        partition: Partition(b"page:0:500".to_vec()),
        progress_marker: Some(OpaqueProgressBytes(b"m".to_vec())),
        progress: BackfillProgress {
            items_done: 500,
            items_estimated: Some(9000),
        },
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION),
    }))
}

#[test]
fn nonzero_reserved_bytes_are_rejected() {
    for idx in 1..=3 {
        let mut bytes = change_envelope(CursorScope::Account);
        bytes[idx] = 0x01;
        assert!(
            decode_envelope(&bytes).is_err(),
            "reserved byte {idx} nonzero must fail"
        );
    }
}

#[test]
fn unknown_kind_tag_is_rejected() {
    let mut bytes = change_envelope(CursorScope::Account);
    bytes[8] = 9;
    let err = decode_envelope(&bytes).expect_err("unknown kind tag");
    assert!(format!("{err}").contains("unknown kind tag"));
}

#[test]
fn unknown_scope_tag_is_rejected() {
    let mut bytes = change_envelope(CursorScope::Account);
    // scope_repr starts at offset 13; Account is the single tag byte 0.
    bytes[13] = 250;
    let err = decode_envelope(&bytes).expect_err("unknown scope tag");
    assert!(format!("{err}").contains("unknown scope tag"));
}

#[test]
fn unknown_object_type_tag_is_rejected() {
    let mut bytes = change_envelope(CursorScope::Type(ObjectType::Email));
    // Type scope repr is [tag, obj_type] at offsets 13, 14.
    bytes[14] = 0xFF;
    let err = decode_envelope(&bytes).expect_err("unknown object type");
    assert!(format!("{err}").contains("schema is incompatible"));
}

#[test]
fn unknown_protocol_tag_is_rejected() {
    let bytes = change_envelope(CursorScope::Account);
    // Account scope repr is 1 byte, so payload length sits at 14..18
    // and the payload's first byte (the protocol tag) at 18.
    let mut bytes = bytes;
    bytes[18] = 0xEE;
    let err = decode_envelope(&bytes).expect_err("unknown protocol tag");
    assert!(format!("{err}").contains("unknown protocol tag"));
}

#[test]
fn reserved_future_protocol_tag_is_schema_incompatible() {
    let mut bytes = change_envelope(CursorScope::Account);
    bytes[18] = 0xFF;
    let err = decode_envelope(&bytes).expect_err("future protocol tag");
    assert!(format!("{err}").contains("schema is incompatible"));
}

#[test]
fn invalid_utf8_in_query_scope_is_rejected() {
    let mut bytes = change_envelope(CursorScope::Query(QueryId("abc".into())));
    // Query scope repr: [tag at 13][LE len at 14..18][string at 18..].
    bytes[18] = 0xFF;
    let err = decode_envelope(&bytes).expect_err("invalid UTF-8 query id");
    assert!(format!("{err}").contains("invalid UTF-8"));
}

#[test]
fn every_truncation_of_a_change_envelope_fails_cleanly() {
    let bytes = change_envelope(CursorScope::Query(QueryId("inbox-query".into())));
    for cut in 0..bytes.len() {
        let sliced = &bytes[..cut];
        assert!(
            decode_envelope(sliced).is_err(),
            "prefix of {cut} bytes must not decode"
        );
    }
    // The untruncated buffer still decodes.
    assert!(decode_envelope(&bytes).is_ok());
}

#[test]
fn every_truncation_of_a_backfill_envelope_fails_cleanly() {
    let bytes = backfill_envelope();
    for cut in 0..bytes.len() {
        let sliced = &bytes[..cut];
        assert!(
            decode_envelope(sliced).is_err(),
            "prefix of {cut} bytes must not decode"
        );
    }
    assert!(decode_envelope(&bytes).is_ok());
}

#[test]
fn trailing_garbage_after_payload_is_rejected() {
    // The cursor envelope decoder must consume the payload exactly;
    // extra bytes past the declared payload are refused.
    let mut bytes = change_envelope(CursorScope::Account);
    bytes.extend_from_slice(b"garbage");
    assert!(decode_envelope(&bytes).is_err());
}

#[test]
fn scope_length_larger_than_buffer_is_rejected() {
    let mut bytes = change_envelope(CursorScope::Account);
    // Claim a giant scope length; must fail as truncated, not panic
    // or attempt a giant allocation-driven read.
    bytes[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_envelope(&bytes).is_err());
}

#[test]
fn payload_length_larger_than_buffer_is_rejected() {
    let mut bytes = change_envelope(CursorScope::Account);
    // Account scope repr is 1 byte; payload length lives at 14..18.
    bytes[14..18].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_envelope(&bytes).is_err());
}

#[test]
fn folder_type_scope_roundtrips_with_multibyte_folder_name() {
    // Non-ASCII folder ids must survive the scope codec.
    let scope = CursorScope::FolderType {
        folder: bifrost_types::FolderId("Skr\u{e5}p/Arkiv \u{2764}".into()),
        ty: ObjectType::CalendarEvent,
    };
    let bytes = change_envelope(scope.clone());
    let decoded = decode_envelope(&bytes).expect("multibyte folder decodes");
    let Checkpoint::Change(c) = decoded else {
        panic!("expected Change");
    };
    assert_eq!(c.scope, scope);
}

#[test]
fn backfill_without_estimate_roundtrips() {
    let bytes = encode_envelope(&Checkpoint::Backfill(BackfillCheckpoint {
        scope: CursorScope::Account,
        partition: Partition(b"complete".to_vec()),
        progress_marker: None,
        progress: BackfillProgress {
            items_done: 7,
            items_estimated: None,
        },
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(ENGINE_VERSION),
    }));
    let decoded = decode_envelope(&bytes).expect("decodes");
    let Checkpoint::Backfill(bf) = decoded else {
        panic!("expected Backfill");
    };
    assert_eq!(bf.progress.items_done, 7);
    assert_eq!(bf.progress.items_estimated, None);
    assert_eq!(bf.progress_marker, None);
}
