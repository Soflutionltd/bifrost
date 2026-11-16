//! Establish schema recovery contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_sync::{CheckpointStore, DebtLedger, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope, ObjectId, SyncEvent};
use bifrost_types::{BackfillCheckpoint, Checkpoint};
use common::{cursor_for, unsupported};
use recovery::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn attach_heals_an_undecodable_cursor_instead_of_failing() {
    let account_id = AccountId("schema-heal".to_owned());
    let scope = CursorScope::Account;
    let store = Arc::new(PoisonedStore::with_poisoned(vec![scope.clone()]));
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));

    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![scope.clone()],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("an undecodable cursor must not fail the attach");

    assert_eq!(
        store.clears.load(Ordering::SeqCst),
        1,
        "the unreadable row must be cleared, otherwise the next attach hits it again"
    );
    assert_eq!(
        *established.lock().expect("established lock"),
        vec![scope.clone()],
        "the scope must be re-established from the live account"
    );
    let persisted = store
        .get_change_cursor(&account_id, &scope)
        .await
        .expect("the healed row decodes")
        .expect("re-establishment persists a replacement cursor");
    assert_eq!(
        persisted.server_state.bytes,
        b"freshly-established".to_vec()
    );

    engine.detach(&account_id).await.expect("detach succeeds");
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

/// One unreadable row clears every durable row of the account at attach: the
/// readable sibling's cursor and completion marker, and the rows of a scope
/// discovery never names. The ledger survives byte for byte, old object id
/// included; `HealFactory`'s idle changes stream means no accepted coverage can
/// discharge it during the assertion.
///
/// Ablations: restore the per-scope `delete_change_cursor` heal (no clear, the
/// sibling resumes its stored cursor and is not established); pass
/// `DebtLedger::default()` from attach to the clear (the ledger bytes differ).
#[tokio::test]
async fn one_undecodable_scope_clears_every_row_of_the_account_at_attach() {
    let account_id = AccountId("schema-heal-sibling".to_owned());
    let poisoned = CursorScope::Type(bifrost_types::ObjectType::Email);
    let healthy = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
    let store = Arc::new(PoisonedStore::with_poisoned(vec![poisoned.clone()]));
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));

    let absent = CursorScope::Type(bifrost_types::ObjectType::Contact);
    for scope in [&healthy, &absent] {
        store
            .put_change_cursor(&account_id, cursor_for(scope, b"old"))
            .await
            .expect("seed");
        store
            .put_backfill(
                &account_id,
                BackfillCheckpoint {
                    scope: scope.clone(),
                    partition: bifrost_sync::backfill::partitioner::completion_partition(),
                    progress_marker: None,
                    progress: Default::default(),
                    envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
                },
            )
            .await
            .expect("seed marker");
    }
    let mut ledger = DebtLedger::default();
    let report = bifrost_types::InventoryCoverageReport::degraded(
        bifrost_types::CoverageDomain::full(healthy.clone()),
        vec![bifrost_types::InventoryObligation::Object {
            key: bifrost_types::ObligationKey(b"old-id".to_vec()),
            id: ObjectId("old-encoding-id".into()),
            error: unsupported(bifrost_types::AccountOperation::SyncInventory),
            repair: Vec::new(),
        }],
    );
    ledger.ingest_debt_only(&report, walk_generation(1), 1);
    let ledger_bytes = bifrost_sync::cursor::encode_ledger(&ledger);
    store
        .put_ledger(&account_id, ledger)
        .await
        .expect("seed ledger");
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![poisoned.clone(), healthy.clone()],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    assert_eq!(
        store.clears.load(Ordering::SeqCst),
        1,
        "an unreadable scope clears the account"
    );
    assert!(
        store.deleted().is_empty(),
        "attach must not heal an unreadable row per scope"
    );
    for scope in [&healthy, &absent] {
        assert!(
            store
                .get_backfill(&account_id, scope)
                .await
                .expect("read")
                .is_none()
        );
    }
    assert!(
        store
            .get_change_cursor(&account_id, &absent)
            .await
            .expect("read")
            .is_none()
    );
    assert_eq!(
        bifrost_sync::cursor::encode_ledger(&store.get_ledger(&account_id).await.expect("ledger")),
        ledger_bytes
    );
    // Both scopes establish fresh, including the formerly readable sibling.
    assert_eq!(
        *established.lock().expect("established lock"),
        vec![poisoned, healthy]
    );

    engine.detach(&account_id).await.expect("detach succeeds");
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

/// A deferred inventory walk (attach-time, so its scope is not registered)
/// that terminates with a schema error triggers the account reset through
/// `ctx.recover(Some(scope), ..)`. The reset must re-establish that scope even
/// though the registry never held it, and the queued sibling item must not
/// walk a second time.
///
/// Ablations: pass an empty `extra` from the directive arm (A is walked once
/// and never registered); drop both of the worker's pre-walk fence checks, the
/// one before the throttle waits and the one after admission (B walks twice:
/// its queued walk runs after the reset, is refused at its first publication,
/// and is re-run through `EstablishIfAbsent`).
#[tokio::test(start_paused = true)]
async fn a_deferred_inventory_that_raises_schema_incompatible_is_established_after_the_reset() {
    let a = CursorScope::Type(bifrost_types::ObjectType::Email);
    let b = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
    let walks = Arc::new(Mutex::new(
        std::collections::HashMap::<CursorScope, usize>::new(),
    ));
    let observed = Arc::clone(&walks);
    let mut account = common::StubAccount::new(vec![a.clone(), b.clone()]);
    account.establishment = |_| bifrost_types::CursorEstablishment::EstablishViaInventory;
    let trigger = a.clone();
    account.inventory_hook = Some(Arc::new(move |scope| {
        let mut walks = observed.lock().expect("walks");
        let count = walks.entry(scope.clone()).or_default();
        *count += 1;
        if *scope == trigger && *count == 1 {
            vec![bifrost_types::InventoryEvent::Terminated(
                bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::State(
                    bifrost_types::StateCause::SchemaIncompatible,
                ))
                .try_build()
                .expect("schema error"),
            )]
        } else {
            vec![bifrost_types::InventoryEvent::Done(
                bifrost_types::InventoryCompletion::complete(
                    bifrost_types::CoverageDomain::full(scope.clone()),
                    Some(Checkpoint::Change(cursor_for(scope, b"fresh"))),
                    bifrost_types::PartitionEnd::Exhausted,
                ),
            )]
        }
    }));
    let id = AccountId("deferred-schema".into());
    let engine = SyncEngine::builder().build().expect("engine");
    engine
        .attach(
            id.clone(),
            Arc::new(common::StubFactory::queue(vec![Arc::new(account)])),
        )
        .await
        .expect("attach");
    let mut receiver = engine.account_changes_stream(&id).expect("subscribe");
    let mut done = std::collections::HashSet::new();
    tokio::time::timeout(Duration::from_secs(300), async {
        while done.len() < 2 {
            let event = receiver.recv().await.expect("event");
            if event.checkpoint.is_some() {
                assert_eq!(
                    engine
                        .ack(
                            &id,
                            event
                                .ack_token()
                                .cloned()
                                .expect("acknowledgeable publication"),
                        )
                        .await
                        .expect("ack"),
                    bifrost_sync::AckOutcome::Durable,
                );
            }
            if matches!(event.event.as_ref(), SyncEvent::Done(_)) {
                done.insert(event.scope);
            }
        }
    })
    .await
    .expect("both inventories");
    assert_eq!(walks.lock().expect("walks").get(&a), Some(&2));
    assert_eq!(walks.lock().expect("walks").get(&b), Some(&1));
    engine.detach(&id).await.expect("detach");
}

// Mint test proof orders through the same owner as production walks.
fn walk_generation(value: u64) -> bifrost_sync::cursor::WalkGeneration {
    let coverage = bifrost_sync::cursor::PendingCoverage::new();
    for _ in 0..value {
        let _ = coverage.next_generation();
    }
    coverage.next_generation()
}
