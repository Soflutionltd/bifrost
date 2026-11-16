//! Reattach checkpoint rollback contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_sync::{
    CheckpointStore, CheckpointTransition, DebtLedger, Error, InMemoryCheckpointStore, SyncEngine,
};
use bifrost_types::{AccountFactory, AccountId, ChangeCursor, CursorScope};
use bifrost_types::{BackfillCheckpoint, Checkpoint};
use common::cursor_for;
use recovery::*;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// A prior session persisted a cursor for a scope this session's account
/// never discovered. A replacement rediscovers the scope, so reattach
/// resumes from the stored row without creating anything - and an aborted
/// swap must therefore leave that row exactly as it found it. Treating
/// "absent from the live registry" as "newly created" would make the abort
/// path delete a legitimately persisted cursor.
#[tokio::test]
async fn aborted_reattach_preserves_preexisting_cursor_for_rediscovered_scope() {
    let account_id = AccountId("rediscovered-preexisting".to_owned());
    let live_scope = CursorScope::Account;
    let rediscovered = CursorScope::Type(bifrost_types::ObjectType::Email);
    let fresh = CursorScope::Type(bifrost_types::ObjectType::Contact);
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live_scope.clone()],
            vec![live_scope.clone(), rediscovered.clone(), fresh.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live_scope))
        .await
        .expect("subscribe");

    // The prior session's row: durable, valid, not in the live registry.
    let prior = cursor_for(&rediscovered, b"prior-session");
    store
        .put_change_cursor(&account_id, prior.clone())
        .await
        .expect("seed prior-session cursor");

    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "the old-handle teardown failure must abort the swap"
    );
    let survived = store
        .get_change_cursor(&account_id, &rediscovered)
        .await
        .expect("rediscovered scope lookup")
        .expect("an aborted swap must not delete a preexisting durable cursor");
    assert_eq!(
        survived.server_state.bytes, prior.server_state.bytes,
        "the preexisting row must survive the aborted swap unchanged"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &fresh)
            .await
            .expect("fresh scope lookup")
            .is_none(),
        "a freshly created cursor must not stay durable after the aborted swap"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Store wrapper whose next `get_change_cursor` for one scripted scope
/// fails once, then delegates. Models a transient store read failure at
/// the worst point of a reattach: the read that decides whether a
/// rediscovered scope's row is preexisting or freshly created.
struct FlakyGetStore {
    inner: Arc<InMemoryCheckpointStore>,
    fail_get_for: Mutex<Option<CursorScope>>,
}

impl CheckpointStore for FlakyGetStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.apply_transition(account, transition)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DebtLedger, Error>> + Send + 'a>> {
        self.inner.get_ledger(account)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>
    {
        {
            let mut scripted = self.fail_get_for.lock().expect("flaky get lock");
            if scripted.as_ref() == Some(scope) {
                scripted.take();
                return Box::pin(async {
                    Err(Error::CheckpointStore("transient read failure".to_owned()))
                });
            }
        }
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

/// A transient store read failure while classifying a rediscovered
/// scope's row must never demote that preexisting row to "freshly
/// created": a scheme that swallows the error as "no row" hands the
/// abort path a legitimately persisted prior-session cursor to delete.
/// The reopen may fail - the read failed - but the row must survive.
#[tokio::test]
async fn transient_get_failure_cannot_demote_preexisting_cursor_to_created() {
    let account_id = AccountId("flaky-get-rediscovered".to_owned());
    let live_scope = CursorScope::Account;
    let rediscovered = CursorScope::Type(bifrost_types::ObjectType::Email);
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live_scope.clone()],
            vec![live_scope.clone(), rediscovered.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let inner = Arc::new(InMemoryCheckpointStore::new());
    let store = Arc::new(FlakyGetStore {
        inner: Arc::clone(&inner),
        fail_get_for: Mutex::new(None),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live_scope))
        .await
        .expect("subscribe");

    let prior = cursor_for(&rediscovered, b"prior-session");
    inner
        .put_change_cursor(&account_id, prior.clone())
        .await
        .expect("seed prior-session cursor");
    // Arm the transient failure only now, so it hits the reopen's
    // classification read for the rediscovered scope and nothing else.
    *store.fail_get_for.lock().expect("flaky get lock") = Some(rediscovered.clone());

    assert!(
        engine.reattach(&account_id).await.is_err(),
        "a failed classification read must abort the swap"
    );
    let survived = inner
        .get_change_cursor(&account_id, &rediscovered)
        .await
        .expect("rediscovered scope lookup")
        .expect("a transient read failure must not cost a preexisting durable cursor");
    assert_eq!(
        survived.server_state.bytes, prior.server_state.bytes,
        "the preexisting row must survive the failed reopen unchanged"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Store wrapper that records every change-cursor mutation and, on the
/// first put, first writes a scripted "concurrent ack" row into the inner
/// store - a deterministic stand-in for the old account's ack writer
/// persisting a newer checkpoint in the middle of a reattach, at the worst
/// possible interleaving point for any snapshot-and-restore scheme.
struct AckRacingStore {
    inner: Arc<InMemoryCheckpointStore>,
    mutated_scopes: Mutex<Vec<CursorScope>>,
    side_write: Mutex<Option<(AccountId, ChangeCursor)>>,
}

impl CheckpointStore for AckRacingStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        if let Checkpoint::Change(cursor) = &transition.checkpoint {
            self.mutated_scopes
                .lock()
                .expect("mutations lock")
                .push(cursor.scope.clone());
        }
        let side = self.side_write.lock().expect("side write lock").take();
        Box::pin(async move {
            if let Some((ack_account, ack_cursor)) = side {
                self.inner
                    .apply_transition(
                        &ack_account,
                        CheckpointTransition {
                            checkpoint: Checkpoint::Change(ack_cursor),
                            ledger: DebtLedger::default(),
                        },
                    )
                    .await?;
            }
            self.inner.apply_transition(account, transition).await
        })
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DebtLedger, Error>> + Send + 'a>> {
        self.inner.get_ledger(account)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>
    {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.mutated_scopes
            .lock()
            .expect("mutations lock")
            .push(scope.clone());
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

/// The old account's ack writer is not serialized against a reopen, so a
/// consumer acknowledgement can persist a newer checkpoint at any point of
/// the reattach - including for a scope the replacement no longer
/// discovers. An aborted swap must leave that newer row standing: the
/// abort path may only delete rows the reattach itself created, and every
/// preexisting row (vanished or live) must remain untouched until the
/// cutover is committed.
#[tokio::test]
async fn aborted_reattach_cannot_clobber_concurrently_acked_vanished_cursor() {
    let account_id = AccountId("ack-race-vanished".to_owned());
    let live_scope = CursorScope::Account;
    let vanished = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
    let fresh = CursorScope::Type(bifrost_types::ObjectType::Contact);
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live_scope.clone(), vanished.clone()],
            vec![live_scope.clone(), fresh.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let inner = Arc::new(InMemoryCheckpointStore::new());
    let newer_ack = cursor_for(&vanished, b"newer-acked-checkpoint");
    let store = Arc::new(AckRacingStore {
        inner: Arc::clone(&inner),
        mutated_scopes: Mutex::new(Vec::new()),
        side_write: Mutex::new(None),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live_scope))
        .await
        .expect("subscribe");
    *store.side_write.lock().expect("side write") = Some((account_id.clone(), newer_ack.clone()));
    // Attach persisted both live scopes; only the reopen's mutations are
    // under scrutiny. The scripted side write fires on the reopen's first
    // put, so it lands mid-reattach by construction.
    store.mutated_scopes.lock().expect("mutations lock").clear();

    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "the old-handle teardown failure must abort the swap"
    );
    let survived = inner
        .get_change_cursor(&account_id, &vanished)
        .await
        .expect("vanished scope lookup")
        .expect("an aborted swap must not delete a vanished scope's durable cursor");
    assert_eq!(
        survived.server_state.bytes, newer_ack.server_state.bytes,
        "the concurrently acked checkpoint must survive the aborted swap"
    );
    assert!(
        inner
            .get_change_cursor(&account_id, &fresh)
            .await
            .expect("fresh scope lookup")
            .is_none(),
        "a freshly created cursor must not stay durable after the aborted swap"
    );
    let mutated = store.mutated_scopes.lock().expect("mutations lock").clone();
    assert!(
        mutated.iter().all(|scope| *scope == fresh),
        "an aborted reopen may only mutate rows it created itself, got {mutated:?}"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// A reattach that COMMITS must clear the provisional set, or the NEXT
/// reattach's abort deletes cursors belonging to a reattach that succeeded.
///
/// The rollback path is a conditional delete driven by the provisional set:
/// only rows the current reattach inserted may be destroyed. If the commit that
/// promotes those rows to ordinary durable state never runs, the set carries
/// them forward, and the following abort - a routine outcome, since teardown of
/// the outgoing handle is allowed to fail - takes the earlier reattach's work
/// with it. The account then resumes with a scope whose durable cursor silently
/// vanished, and re-establishes it by re-walking inventory.
#[tokio::test]
async fn a_committed_reattach_is_not_rolled_back_by_the_next_aborted_one() {
    let account_id = AccountId("reattach-commit-clears-provisional".to_owned());
    let live = CursorScope::Account;
    let second = CursorScope::Type(bifrost_types::ObjectType::Email);
    let third = CursorScope::Type(bifrost_types::ObjectType::Contact);
    let unsubscribe_failures = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![live.clone()],
            vec![live.clone(), second.clone()],
            vec![live.clone(), third.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::clone(&unsubscribe_failures),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("initial attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&live))
        .await
        .expect("initial push subscription");

    // First reopen succeeds: `second` becomes ordinary durable state.
    engine.reattach(&account_id).await.expect("first reattach");
    assert!(
        store
            .get_change_cursor(&account_id, &second)
            .await
            .expect("second scope lookup")
            .is_some(),
        "a successful reattach must leave the scope it discovered durable"
    );

    // Second reopen aborts on old-handle teardown. Its rollback may destroy
    // only what IT inserted.
    unsubscribe_failures.store(1, Ordering::SeqCst);
    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "the old-handle teardown failure must abort the second swap"
    );

    assert!(
        store
            .get_change_cursor(&account_id, &second)
            .await
            .expect("second scope lookup")
            .is_some(),
        "an aborted reattach must not delete a cursor a COMMITTED reattach created"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &third)
            .await
            .expect("third scope lookup")
            .is_none(),
        "the aborted reattach must still destroy the rows it created itself"
    );

    engine.detach(&account_id).await.expect("detach");
}
