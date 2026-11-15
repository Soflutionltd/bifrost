//! A repair pass that recovers ids in more than one scope must attribute each
//! id to the scope it came from.
//!
//! `MultiplexerEvent::scope` is the routing key of the broadcast: a consumer
//! that files events by it, or filters on it, is doing exactly what the type is
//! for. A single batch stamped with whichever scope happened to be recovered
//! first therefore delivers one scope's ids as another's, or drops them.

mod common;

use std::sync::Arc;

use bifrost_sync::MultiplexerEvent;
use bifrost_sync::cursor::{DebtLedger, PendingCoverage};
use bifrost_sync::multiplexer::WriterRequest;
use bifrost_sync::run_repair_pass;
use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, Cause, CoverageDomain, CursorScope, DiagnosticText,
    FolderId, InventoryCoverageReport, InventoryEntry, InventoryObligation, InventoryRepairEvent,
    InventoryRepairOutcome, ObjectId, ObligationKey, RequestCause, RequestErrorKind, SyncEvent,
};
use tokio::sync::{broadcast, mpsc};

use common::StubAccount;

fn scope(name: &str) -> CursorScope {
    CursorScope::Folder(FolderId(name.into()))
}

fn obligation(key: &str) -> InventoryObligation {
    let error = AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("unrepresentable"),
        }),
    )
    .try_build()
    .expect("valid account error classification");
    InventoryObligation::Object {
        key: ObligationKey(key.as_bytes().to_vec()),
        id: ObjectId(key.into()),
        error,
        repair: Vec::new(),
    }
}

/// One degraded object obligation per scope, both repairable.
fn two_scope_ledger() -> DebtLedger {
    let mut ledger = DebtLedger::default();
    for name in ["scope-a", "scope-b"] {
        let report = InventoryCoverageReport::degraded(
            CoverageDomain::full(scope(name)),
            vec![obligation(name)],
        );
        ledger.ingest(&report, 1, 1_700_000_000);
    }
    ledger
}

#[tokio::test]
async fn recovered_ids_are_published_under_their_own_scope() {
    let account = StubAccount {
        repair_hook: Some(Arc::new(|request| {
            let ObligationKey(key) = &request.key;
            let id = ObjectId(String::from_utf8(key.clone()).expect("ascii key"));
            InventoryRepairEvent::Outcome(InventoryRepairOutcome::ObjectRecovered {
                attempt: request.attempt,
                entry: Box::new(InventoryEntry {
                    id,
                    memberships: Vec::new(),
                    size: None,
                    blob_id: None,
                    fingerprint: bifrost_types::Fingerprint {
                        server_version: bifrost_types::ServerVersion::Unavailable,
                        size: None,
                        flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
                    },
                    thread_id: None,
                    message_id: None,
                    references: Vec::new(),
                    in_reply_to: None,
                }),
            })
        })),
        ..StubAccount::new(vec![scope("scope-a"), scope("scope-b")])
    };

    let coverage = Arc::new(PendingCoverage::new());
    let (changes_tx, mut sentinel) = broadcast::channel::<MultiplexerEvent>(16);
    let delivery = Arc::new(bifrost_sync::multiplexer::ChangeDelivery::new(changes_tx));
    // A publication only counts as delivered once a NUMBERED receiver - one
    // handed out by the delivery gate, as `account_changes_stream` does - has
    // taken it; the slot's sentinel and any observer do not count.
    let mut consumer = delivery.subscribe(None, Arc::clone(&coverage));

    let (writer_tx, mut writer_rx) = mpsc::channel::<WriterRequest>(16);
    let writer = tokio::spawn(async move {
        let mut applied = Vec::new();
        while let Some(request) = writer_rx.recv().await {
            if let WriterRequest::ApplyRepair {
                resolutions,
                publication,
                done,
            } = request
            {
                applied.push((resolutions.len(), publication.is_some()));
                let _ = done.send(Ok(()));
            }
        }
        applied
    });

    let resolved = run_repair_pass(
        &account,
        &bifrost_types::AccountId("acct".into()),
        &two_scope_ledger(),
        Some(&delivery),
        &writer_tx,
        &coverage,
        8,
    )
    .await
    .expect("repair pass runs");
    assert_eq!(resolved, 2, "both obligations reached a resolution");

    drop(writer_tx);
    let applied = writer.await.expect("writer task");
    assert_eq!(
        applied.len(),
        2,
        "one writer request per scope, each carrying its own publication"
    );
    assert!(
        applied
            .iter()
            .all(|(count, published)| *count == 1 && *published),
        "each scope's resolutions ride the publication that carried its own ids"
    );

    let mut seen: Vec<(CursorScope, Vec<ObjectId>)> = Vec::new();
    while let Ok(event) = consumer.try_recv() {
        let SyncEvent::Batch(batch) = event.event.as_ref() else {
            panic!("repair publishes batches");
        };
        let ids = batch
            .items
            .iter()
            .map(|change| match change {
                bifrost_types::Change::ObjectChange(object) => object.id.clone(),
                other => panic!("repair publishes object changes, got {other:?}"),
            })
            .collect();
        seen.push((event.scope.cursor().expect("cursor event").clone(), ids));
    }
    let _ = sentinel.try_recv();

    seen.sort_by_key(|(scope, _)| format!("{scope:?}"));
    assert_eq!(
        seen,
        vec![
            (scope("scope-a"), vec![ObjectId("scope-a".into())]),
            (scope("scope-b"), vec![ObjectId("scope-b".into())]),
        ],
        "every recovered id must be announced under the scope it was recovered from"
    );
}
