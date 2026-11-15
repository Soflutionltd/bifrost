//! An external consumer must still be able to CONSTRUCT the published backfill
//! checkpoint helper.
//!
//! The round-3 reshape replaced `BackfillCheckpointWriter::store`'s bare
//! `Arc<DynCheckpointStore>` with an opaque target, which is what closes the
//! lost-update race against the account's in-memory debt ledger. A published
//! type nobody outside the crate can build is removed in substance even though
//! its name survives, so both routes stay open: the safe one through an
//! attached engine, and the direct-store one external code already had.
//!
//! This file lives outside the crate on purpose - it compiles against exactly
//! the surface a downstream consumer sees.

use std::sync::Arc;

use bifrost_sync::backfill::{BackfillCheckpointTarget, BackfillCheckpointWriter};
use bifrost_sync::cursor::InMemoryCheckpointStore;
use bifrost_sync::cursor::store::DynCheckpointStore;
use bifrost_types::{AccountId, BackfillCheckpoint, BackfillProgress, CursorScope, Partition};

#[tokio::test]
async fn an_external_consumer_can_still_build_and_use_the_writer() {
    let store: Arc<DynCheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let account = AccountId("external".into());

    // Constructor route.
    let writer = BackfillCheckpointWriter::new(
        account.clone(),
        BackfillCheckpointTarget::direct(Arc::clone(&store)),
    );
    // Struct-literal route: the fields are still public, and the target is
    // built from exactly what the old field took.
    let literal = BackfillCheckpointWriter {
        account_id: account.clone(),
        store: BackfillCheckpointTarget::direct(Arc::clone(&store)),
    };
    assert_eq!(literal.account_id, account);

    let scope = CursorScope::Account;
    writer
        .persist(BackfillCheckpoint {
            scope: scope.clone(),
            partition: Partition(b"page:0:50".to_vec()),
            progress_marker: None,
            progress: BackfillProgress {
                items_done: 50,
                items_estimated: None,
            },
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        })
        .await
        .expect("the direct route still writes");

    let persisted = store
        .get_backfill(&account, &scope)
        .await
        .expect("store read")
        .expect("a checkpoint was written");
    assert_eq!(persisted.progress.items_done, 50);
}
