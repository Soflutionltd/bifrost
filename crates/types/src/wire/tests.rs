//! Literal storage fixtures pin compatibility across the codec ownership move.

use super::{DecodeError, Reader, cursor, ledger};
use crate::{
    AccessErrorKind, AccountErrorKind, AuthErrorKind, CoverageCoordinate, CoverageDomain,
    CursorScope, DetailVisibility, DiagnosticInfo, DiagnosticText, ErrorScope, InternalErrorKind,
    InventoryRepairTarget, MailboxUnavailableKind, ObjectId, Protocol, ProtocolErrorKind, Provider,
    RequestErrorKind, ResourceKind, SnapshotIdentity, SyncStateErrorKind, TelemetryToken,
    TransportErrorKind,
};

#[test]
fn historical_error_kind_bytes_are_stable() {
    use AccountErrorKind as K;
    let cases: Vec<(K, Vec<u8>)> = vec![
        (K::Transport(TransportErrorKind::Tls), vec![0, 2]),
        (K::Authentication(AuthErrorKind::Revoked), vec![1, 2]),
        (
            K::Authorization(AccessErrorKind::MailboxUnavailable {
                kind: MailboxUnavailableKind::Permanent,
            }),
            vec![2, 6, 1],
        ),
        (
            K::Server(crate::ServerErrorKind::Error { status: Some(503) }),
            vec![3, 3, 1, 247, 1],
        ),
        (K::SyncState(SyncStateErrorKind::ScopeRevoked), vec![4, 6]),
        (K::ConcurrencyConflict, vec![5]),
        (K::Request(RequestErrorKind::BatchInputInvalid), vec![6, 1]),
        (K::NotFound(ResourceKind::Event), vec![7, 11]),
        (
            K::Unsupported(crate::AccountOperation::Expunge),
            vec![8, 82],
        ),
        (K::Protocol(ProtocolErrorKind::PartialResponse), vec![9, 3]),
        (K::Internal(InternalErrorKind::AccountContract), vec![10, 3]),
        (K::ControlWaitEnded, vec![11]),
        (K::AccountClosed, vec![12]),
    ];
    for (kind, fixture) in cases {
        let mut encoded = Vec::new();
        ledger::encode_error_kind(&mut encoded, &kind);
        assert_eq!(encoded, fixture);
        let mut reader = Reader::new(&fixture, ledger::VOCABULARY_VERSION);
        assert_eq!(ledger::decode_error_kind(&mut reader).unwrap(), kind);
        assert!(reader.is_finished());
        for end in 0..fixture.len() {
            assert!(ledger::decode_error_kind(&mut Reader::new(&fixture[..end], 11)).is_err());
        }
    }
}

#[test]
fn historical_scope_bytes_and_schema_classification_are_stable() {
    let folder = CursorScope::FolderType {
        folder: crate::FolderId("a".into()),
        ty: crate::ObjectType::CalendarEvent,
    };
    let fixture = [4, 1, 0, 0, 0, b'a', 6];
    assert_eq!(cursor::encode_scope(&folder), fixture);
    assert_eq!(cursor::decode_scope(&fixture).unwrap(), folder);

    let scope = ErrorScope::Cursor(folder);
    let fixture = [1, 7, 0, 0, 0, 4, 1, 0, 0, 0, b'a', 6];
    let mut encoded = Vec::new();
    ledger::encode_error_scope(&mut encoded, &scope);
    assert_eq!(encoded, fixture);
    let mut reader = Reader::new(&fixture, 3);
    assert_eq!(ledger::decode_error_scope(&mut reader).unwrap(), scope);
    assert!(reader.is_finished());

    assert!(matches!(
        cursor::decode_scope(&[1, 255]),
        Err(DecodeError::SchemaIncompatible)
    ));
    assert!(matches!(
        cursor::decode_scope(&[1, 254]),
        Err(DecodeError::Malformed(_))
    ));
    // The reserved object tag retains schema recovery even when nested in a ledger scope.
    assert!(matches!(
        ledger::decode_error_scope(&mut Reader::new(&[1, 2, 0, 0, 0, 1, 255], 3)),
        Err(DecodeError::SchemaIncompatible)
    ));
}

#[test]
fn historical_coverage_and_repair_bytes_are_stable() {
    let domain = CoverageDomain {
        scope: CursorScope::Account,
        coordinate: CoverageCoordinate::PageRange(crate::PageWindow::new(2, 4).unwrap()),
        snapshot: SnapshotIdentity(Some(vec![9])),
    };
    let fixture = [1, 0, 0, 0, 0, 3, 2, 0, 0, 0, 4, 0, 0, 0, 1, 1, 0, 0, 0, 9];
    let mut encoded = Vec::new();
    ledger::encode_domain(&mut encoded, &domain);
    assert_eq!(encoded, fixture);
    let mut reader = Reader::new(&fixture, 3);
    assert_eq!(ledger::decode_domain(&mut reader).unwrap(), domain);
    assert!(reader.is_finished());

    let target = InventoryRepairTarget::Object {
        id: ObjectId("a".into()),
        repair: vec![9],
    };
    let fixture = [1, 1, 0, 0, 0, b'a', 1, 0, 0, 0, 9];
    let mut encoded = Vec::new();
    ledger::encode_repair_target(&mut encoded, Some(&target));
    assert_eq!(encoded, fixture);
    let mut reader = Reader::new(&fixture, 3);
    assert_eq!(
        ledger::decode_repair_target(&mut reader).unwrap(),
        Some(target)
    );
    assert!(reader.is_finished());
}

#[test]
fn historical_diagnostic_visibility_bytes_are_stable() {
    let diagnostics = DiagnosticInfo {
        status: Some(503),
        request_id: TelemetryToken::new("r"),
        text: vec![
            DiagnosticText::user_safe("u"),
            DiagnosticText::support_only("s"),
        ],
        ..DiagnosticInfo::default()
    };
    let fixture = [
        1, 247, 1, 1, 1, 0, 0, 0, b'r', 0, 0, 2, 0, 0, 0, 0, 1, 0, 0, 0, b'u', 1, 1, 0, 0, 0, b's',
    ];
    let mut encoded = Vec::new();
    ledger::encode_diagnostics(&mut encoded, &diagnostics);
    assert_eq!(encoded, fixture);
    let mut reader = Reader::new(&fixture, 3);
    let decoded = ledger::decode_diagnostics(&mut reader).unwrap();
    assert!(reader.is_finished());
    assert_eq!(decoded.status, Some(503));
    assert_eq!(decoded.request_id.as_ref().unwrap().as_str(), "r");
    assert_eq!(decoded.text[0].visibility, DetailVisibility::UserSafe);
    assert_eq!(decoded.text[1].visibility, DetailVisibility::SupportOnly);
    assert_eq!(decoded.text[1].as_str(), "s");
    let mut encoded = Vec::new();
    ledger::encode_diagnostics(&mut encoded, &decoded);
    assert_eq!(encoded, fixture);
}

#[test]
fn new_provider_and_protocol_tags_keep_their_historical_gate() {
    assert_eq!(ledger::encode_provider(Provider::Google), 6);
    assert_eq!(ledger::decode_provider(6, 9).unwrap(), Provider::Google);
    assert!(ledger::decode_provider(6, 8).is_err());
    for (tag, protocol) in [
        (10, Protocol::GoogleCalendar),
        (11, Protocol::GooglePeople),
        (12, Protocol::GoogleDrive),
    ] {
        assert_eq!(ledger::encode_protocol(protocol), tag);
        assert_eq!(ledger::decode_protocol(tag, 9).unwrap(), protocol);
        assert!(ledger::decode_protocol(tag, 8).is_err());
    }
}

#[test]
fn request_refusal_tags_are_versioned_and_roundtrip() {
    for (tag, kind) in [
        (2, RequestErrorKind::PayloadTooLarge),
        (3, RequestErrorKind::PreconditionFailed),
    ] {
        let kind = AccountErrorKind::Request(kind);
        let mut encoded = Vec::new();
        ledger::encode_error_kind(&mut encoded, &kind);
        assert_eq!(encoded, [6, tag]);
        let mut reader = Reader::new(&encoded, 14);
        assert_eq!(ledger::decode_error_kind(&mut reader).unwrap(), kind);
        assert!(reader.is_finished());
        for version in 3..14 {
            assert!(matches!(
                ledger::decode_error_kind(&mut Reader::new(&encoded, version)),
                Err(DecodeError::Malformed(_))
            ));
        }
    }
}
