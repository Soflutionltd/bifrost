//! `InMemoryCheckpointStore` contract tests.
//!
//! The in-memory store is the reference `CheckpointStore` every
//! engine test rides on, and its `get_backfill` "latest by
//! `items_done`" selection is load-bearing for backfill resume
//! (`open_pages_resume` / `backfill_complete_recorded` in engine.rs).
//! Equal-progress ties are pinned as well: the furthest page upper
//! bound wins, making resume deterministic and preventing a needless
//! re-walk from an earlier equal-sized window.

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore};
use bifrost_types::{
    AccountId, BackfillCheckpoint, BackfillProgress, ChangeCursor, CursorScope, FolderId,
    ObjectType, OpaqueChangeState, Partition, ProtocolKind,
};

fn account(id: &str) -> AccountId {
    AccountId(id.into())
}

fn change_cursor(scope: &CursorScope, state: &[u8]) -> ChangeCursor {
    ChangeCursor {
        scope: scope.clone(),
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Gmail,
            envelope_version: 1,
            bytes: state.to_vec(),
        },
        advanced_through: None,
        envelope_version: 1,
    }
}

fn backfill(scope: &CursorScope, partition: &[u8], items_done: u64) -> BackfillCheckpoint {
    BackfillCheckpoint {
        scope: scope.clone(),
        partition: Partition(partition.to_vec()),
        progress_marker: None,
        progress: BackfillProgress {
            items_done,
            items_estimated: None,
        },
        envelope_version: 1,
    }
}

#[tokio::test]
async fn change_cursor_is_isolated_per_account_and_scope() {
    let store = InMemoryCheckpointStore::new();
    let scope_a = CursorScope::Folder(FolderId("INBOX".into()));
    let scope_b = CursorScope::Folder(FolderId("Sent".into()));

    store
        .put_change_cursor(&account("one"), change_cursor(&scope_a, b"s1"))
        .await
        .expect("put");
    store
        .put_change_cursor(&account("two"), change_cursor(&scope_a, b"s2"))
        .await
        .expect("put");

    let got = store
        .get_change_cursor(&account("one"), &scope_a)
        .await
        .expect("get");
    assert_eq!(got.expect("present").server_state.bytes, b"s1".to_vec());

    // Different scope on the same account: absent.
    assert!(
        store
            .get_change_cursor(&account("one"), &scope_b)
            .await
            .expect("get")
            .is_none()
    );
    // Same scope on the other account carries the other state.
    let other = store
        .get_change_cursor(&account("two"), &scope_a)
        .await
        .expect("get");
    assert_eq!(other.expect("present").server_state.bytes, b"s2".to_vec());
}

#[tokio::test]
async fn put_change_cursor_replaces_previous_value() {
    let store = InMemoryCheckpointStore::new();
    let scope = CursorScope::Account;
    store
        .put_change_cursor(&account("a"), change_cursor(&scope, b"old"))
        .await
        .expect("put");
    store
        .put_change_cursor(&account("a"), change_cursor(&scope, b"new"))
        .await
        .expect("put");
    let got = store
        .get_change_cursor(&account("a"), &scope)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got.server_state.bytes, b"new".to_vec());
}

#[tokio::test]
async fn delete_change_cursor_removes_only_the_named_scope() {
    let store = InMemoryCheckpointStore::new();
    let scope_a = CursorScope::Folder(FolderId("INBOX".into()));
    let scope_b = CursorScope::Folder(FolderId("Sent".into()));
    store
        .put_change_cursor(&account("a"), change_cursor(&scope_a, b"1"))
        .await
        .expect("put");
    store
        .put_change_cursor(&account("a"), change_cursor(&scope_b, b"2"))
        .await
        .expect("put");

    store
        .delete_change_cursor(&account("a"), &scope_a)
        .await
        .expect("delete");

    assert!(
        store
            .get_change_cursor(&account("a"), &scope_a)
            .await
            .expect("get")
            .is_none(),
        "deleted scope must be gone (a no-op delete would resurrect a stale resume point)"
    );
    assert!(
        store
            .get_change_cursor(&account("a"), &scope_b)
            .await
            .expect("get")
            .is_some(),
        "sibling scope untouched"
    );
}

#[tokio::test]
async fn delete_change_cursor_on_absent_scope_is_a_quiet_no_op() {
    let store = InMemoryCheckpointStore::new();
    store
        .delete_change_cursor(&account("a"), &CursorScope::Account)
        .await
        .expect("delete of absent key succeeds");
}

#[tokio::test]
async fn get_backfill_returns_the_strictly_largest_items_done() {
    let store = InMemoryCheckpointStore::new();
    let scope = CursorScope::Type(ObjectType::Email);
    // Three partitions, distinct progress. The furthest-along row wins.
    for (partition, done) in [
        (b"page:0:500".as_slice(), 500_u64),
        (b"page:500:1000".as_slice(), 500),
        (b"page:1000:1500".as_slice(), 260),
    ] {
        store
            .put_backfill(&account("a"), backfill(&scope, partition, done))
            .await
            .expect("put");
    }
    // 500 appears twice; bump one row so a strict winner exists.
    store
        .put_backfill(&account("a"), backfill(&scope, b"page:500:1000", 501))
        .await
        .expect("put");

    let got = store
        .get_backfill(&account("a"), &scope)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got.partition, Partition(b"page:500:1000".to_vec()));
    assert_eq!(got.progress.items_done, 501);
}

#[tokio::test]
async fn get_backfill_tie_selects_the_furthest_page_window() {
    let store = InMemoryCheckpointStore::new();
    let scope = CursorScope::Type(ObjectType::Email);
    for partition in [
        b"page:1000:1500".as_slice(),
        b"page:0:500".as_slice(),
        b"page:500:1000".as_slice(),
    ] {
        store
            .put_backfill(&account("a"), backfill(&scope, partition, 500))
            .await
            .expect("put");
    }

    let got = store
        .get_backfill(&account("a"), &scope)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got.partition, Partition(b"page:1000:1500".to_vec()));
}

#[tokio::test]
async fn completion_marker_wins_get_backfill_when_items_done_is_larger() {
    // The orchestrator stamps the completion sentinel with
    // total_walked + 1 exactly so it strictly beats every page row.
    let store = InMemoryCheckpointStore::new();
    let scope = CursorScope::Type(ObjectType::Email);
    store
        .put_backfill(&account("a"), backfill(&scope, b"page:0:500", 500))
        .await
        .expect("put");
    store
        .put_backfill(&account("a"), backfill(&scope, b"page:500:1000", 500))
        .await
        .expect("put");
    store
        .put_backfill(&account("a"), backfill(&scope, b"complete", 1001))
        .await
        .expect("put");

    let got = store
        .get_backfill(&account("a"), &scope)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got.partition, Partition(b"complete".to_vec()));
}

#[tokio::test]
async fn get_backfill_is_scope_and_account_isolated() {
    let store = InMemoryCheckpointStore::new();
    let email = CursorScope::Type(ObjectType::Email);
    let contact = CursorScope::Type(ObjectType::Contact);
    store
        .put_backfill(&account("a"), backfill(&email, b"page:0:500", 500))
        .await
        .expect("put");

    assert!(
        store
            .get_backfill(&account("a"), &contact)
            .await
            .expect("get")
            .is_none()
    );
    assert!(
        store
            .get_backfill(&account("b"), &email)
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn put_backfill_replaces_the_same_partition_row() {
    let store = InMemoryCheckpointStore::new();
    let scope = CursorScope::Account;
    store
        .put_backfill(&account("a"), backfill(&scope, b"page:0:500", 100))
        .await
        .expect("put");
    store
        .put_backfill(&account("a"), backfill(&scope, b"page:0:500", 400))
        .await
        .expect("put");
    let got = store
        .get_backfill(&account("a"), &scope)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got.progress.items_done, 400);
}

/// Provider deletion and backfill discard delete every partition of one scope:
/// it must drop EVERY partition row for the named `(account, scope)` -
/// the completion marker included, since that marker is what makes the
/// next attach skip the inventory re-walk that re-mints ids - while
/// leaving other scopes and accounts untouched.
#[tokio::test]
async fn delete_backfill_drops_every_partition_for_the_scope_only() {
    let store = InMemoryCheckpointStore::new();
    let email = CursorScope::Type(ObjectType::Email);
    let contact = CursorScope::Type(ObjectType::Contact);
    store
        .put_backfill(&account("a"), backfill(&email, b"page:0:500", 500))
        .await
        .expect("put");
    store
        .put_backfill(&account("a"), backfill(&email, b"complete", 1001))
        .await
        .expect("put");
    store
        .put_backfill(&account("a"), backfill(&contact, b"page:0:500", 42))
        .await
        .expect("put");
    store
        .put_backfill(&account("b"), backfill(&email, b"page:0:500", 7))
        .await
        .expect("put");

    store
        .delete_backfill(&account("a"), &email)
        .await
        .expect("delete");

    assert!(
        store
            .get_backfill(&account("a"), &email)
            .await
            .expect("get")
            .is_none(),
        "every partition row for the scope is gone, completion marker included"
    );
    assert_eq!(
        store
            .get_backfill(&account("a"), &contact)
            .await
            .expect("get")
            .expect("sibling scope survives")
            .progress
            .items_done,
        42
    );
    assert_eq!(
        store
            .get_backfill(&account("b"), &email)
            .await
            .expect("get")
            .expect("sibling account survives")
            .progress
            .items_done,
        7
    );
}

#[tokio::test]
async fn clear_account_checkpoints_drops_every_row_of_the_account_and_only_that_account() {
    let store = InMemoryCheckpointStore::new();
    let a = account("a");
    let b = account("b");
    let scopes = [CursorScope::Account, CursorScope::Type(ObjectType::Contact)];
    for account in [&a, &b] {
        for scope in &scopes {
            store
                .put_change_cursor(account, change_cursor(scope, b"old"))
                .await
                .expect("seed");
            store
                .put_backfill(
                    account,
                    backfill(
                        scope,
                        &bifrost_sync::backfill::partitioner::completion_partition().0,
                        10,
                    ),
                )
                .await
                .expect("seed");
            store
                .put_backfill(account, backfill(scope, b"page:0:10", 1))
                .await
                .expect("seed");
        }
    }
    let mut ledger = bifrost_sync::cursor::DebtLedger::default();
    ledger.record_barrier(bifrost_sync::cursor::BarrierIncident {
        key: bifrost_types::ObligationKey(b"barrier".to_vec()),
        domain: bifrost_types::CoverageDomain::full(scopes[0].clone()),
        generation: 1,
        failure_label: "blocked".into(),
        evidence: bifrost_types::AccountError::unsupported(
            bifrost_types::AccountOperation::SyncInventory,
            bifrost_types::Protocol::Unknown,
        ),
        policy: bifrost_sync::cursor::PolicyStatus::Retrying { attempts: 0 },
        resume_from: None,
    });
    let bytes = bifrost_sync::cursor::encode_ledger(&ledger);
    store
        .clear_account_checkpoints(&a, ledger)
        .await
        .expect("clear");
    for scope in &scopes {
        assert!(
            store
                .get_change_cursor(&a, scope)
                .await
                .expect("get")
                .is_none()
        );
        assert!(store.get_backfill(&a, scope).await.expect("get").is_none());
        assert!(
            store
                .get_change_cursor(&b, scope)
                .await
                .expect("get")
                .is_some()
        );
        assert!(store.get_backfill(&b, scope).await.expect("get").is_some());
    }
    assert_eq!(
        bifrost_sync::cursor::encode_ledger(&store.get_ledger(&a).await.expect("ledger")),
        bytes
    );
}
