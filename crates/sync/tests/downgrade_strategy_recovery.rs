//! A strategy downgrade actually restarts the account.
//!
//! `EngineDirective::DowngradeStrategy` is derived from a
//! `SyncState(StrategyFailure)` error and handled by restarting the account,
//! then, when the error names a cursor scope, re-establishing that scope. The
//! recovery dispatch takes the slot's `reopen_lock` for scope-local repairs,
//! and `open_replacement` takes the same non-reentrant mutex for the open and
//! the swap. Holding it across the dispatch of a directive that restarts the
//! account therefore hung recovery forever on the first downgrade, with no
//! error and nothing logged.
//!
//! The staging is built so that only a COMPLETED restart satisfies the
//! assertions. The first account terminates its scope's change stream with the
//! downgrade error exactly once; the replacement account never fails. A hung
//! recovery leaves the factory unopened a second time, the first account
//! unclosed, and the replacement never asked to establish anything, so every
//! observation below is false under the hang. Each wait is bounded by a
//! timeout well under the watchdog, so the hang shows up as a failed assertion
//! rather than a stuck suite.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountFactory, AccountFuture, AccountId, AccountOperation,
    Change, ChangeCursor, CursorScope, ErrorScope, ObjectType, PrimaryCause, StateCause,
    StrategyDowngrade, SyncEvent,
};

use common::{StubAccount, StubFactory};

/// Upper bound on any wait. Far above what a healthy restart needs, far below
/// the 20s test watchdog.
const BOUND: Duration = Duration::from_secs(8);

const SCOPE: CursorScope = CursorScope::Type(ObjectType::Email);

fn downgrade_error(scope: ErrorScope) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::State(StateCause::StrategyFailure {
        downgrade: StrategyDowngrade::CondstoreToBasic,
    }))
    .operation(AccountOperation::SyncChanges)
    .scope(scope)
    .try_build()
    .expect("valid account error classification")
}

/// Counts `open` calls, delegating to a queue of stub accounts.
struct CountingFactory {
    inner: StubFactory,
    opens: Arc<AtomicUsize>,
}

impl AccountFactory for CountingFactory {
    fn open(
        &self,
        account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.open(account_id)
    }
}

/// An account whose scope terminates once with `error`, then goes quiet.
fn failing_once(error: AccountError) -> Arc<StubAccount> {
    let fired = Arc::new(AtomicUsize::new(0));
    let mut account = StubAccount::new(vec![SCOPE]);
    account.changes_hook = Some(Arc::new(move |_cursor: &ChangeCursor| {
        if fired.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![SyncEvent::Terminated(error.clone())]
        } else {
            Vec::<SyncEvent<Change>>::new()
        }
    }));
    Arc::new(account)
}

/// Poll `condition` every few milliseconds until it holds or `BOUND` passes.
/// Returns whether it held.
async fn holds_within_bound(condition: impl Fn() -> bool) -> bool {
    tokio::time::timeout(BOUND, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

struct Staged {
    engine: SyncEngine,
    id: AccountId,
    first: Arc<StubAccount>,
    replacement: Arc<StubAccount>,
    opens: Arc<AtomicUsize>,
}

async fn attach_with_downgrade(label: &str, error: AccountError) -> Staged {
    let id = AccountId(label.to_owned());
    let first = failing_once(error);
    let replacement = Arc::new(StubAccount::new(vec![SCOPE]));
    let opens = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn AccountFactory> = Arc::new(CountingFactory {
        inner: StubFactory::queue(vec![Arc::clone(&first), Arc::clone(&replacement)]),
        opens: Arc::clone(&opens),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    Staged {
        engine,
        id,
        first,
        replacement,
        opens,
    }
}

/// The error names a cursor scope, so recovery restarts the account AND
/// re-establishes that scope on the replacement.
#[tokio::test]
async fn a_scoped_strategy_downgrade_restarts_the_account_and_the_scope() {
    let staged = attach_with_downgrade(
        "downgrade-scoped",
        downgrade_error(ErrorScope::Cursor(SCOPE)),
    )
    .await;

    // Attach itself opened once and established the scope once on the first
    // account, so neither of those counts is evidence of recovery.
    let replacement = Arc::clone(&staged.replacement);
    let reestablished = holds_within_bound(|| {
        !replacement
            .established
            .lock()
            .expect("established lock")
            .is_empty()
    })
    .await;
    assert!(
        reestablished,
        "the scope was never re-established on the replacement account: recovery \
         for the strategy downgrade did not complete (a hang inside the account \
         restart looks exactly like this)"
    );
    assert_eq!(
        *staged.replacement.established.lock().expect("lock"),
        vec![SCOPE],
        "the replacement must establish the downgraded scope exactly once"
    );
    assert_eq!(
        staged.opens.load(Ordering::SeqCst),
        2,
        "the factory must have opened exactly one replacement"
    );
    let first = Arc::clone(&staged.first);
    assert!(
        holds_within_bound(|| first.closed.load(Ordering::SeqCst) == 1).await,
        "the superseded account was never closed"
    );

    tokio::time::timeout(BOUND, staged.engine.detach(&staged.id))
        .await
        .expect("detach after recovery must not hang on a leaked reopen lock")
        .expect("detach succeeds");
    assert_eq!(staged.replacement.closed.load(Ordering::SeqCst), 1);
}

/// The error names no cursor scope, so only the account restart runs. The
/// trailing scope restart is skipped, which also means nothing after the
/// restart could mask a hang inside it.
#[tokio::test]
async fn an_account_scoped_strategy_downgrade_restarts_the_account() {
    let staged =
        attach_with_downgrade("downgrade-account", downgrade_error(ErrorScope::Account)).await;

    let opens = Arc::clone(&staged.opens);
    let first = Arc::clone(&staged.first);
    assert!(
        holds_within_bound(
            || opens.load(Ordering::SeqCst) == 2 && first.closed.load(Ordering::SeqCst) == 1
        )
        .await,
        "no replacement was opened and swapped in: recovery for the strategy \
         downgrade did not complete (opens={}, first closed={})",
        staged.opens.load(Ordering::SeqCst),
        staged.first.closed.load(Ordering::SeqCst)
    );

    tokio::time::timeout(BOUND, staged.engine.detach(&staged.id))
        .await
        .expect("detach after recovery must not hang")
        .expect("detach succeeds");
    assert_eq!(staged.replacement.closed.load(Ordering::SeqCst), 1);
}

/// Serves `first` on the initial open, then refuses every later open. Counts
/// every call, the initial one included.
struct RefusingReplacementFactory {
    first: Mutex<Option<Arc<StubAccount>>>,
    opens: Arc<AtomicUsize>,
}

impl AccountFactory for RefusingReplacementFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        let first = self.first.lock().expect("first lock").take();
        Box::pin(async move {
            match first {
                Some(account) => {
                    let account: Arc<dyn bifrost_types::Account> = account;
                    Ok(bifrost_types::OpenedAccount::complete(account))
                }
                None => Err(common::close_refused()),
            }
        })
    }
}

/// When every replacement open fails, the account restart exhausts its budget
/// and pauses the account. The trailing scope repair must NOT run then:
/// `restart_scope` deletes the scope's cursor before re-establishing it, the
/// establishment is refused on a paused account, and after `resume_account`
/// the scope would never sync again.
///
/// Runs under a paused clock so the restart backoff sleeps are virtual.
#[tokio::test(start_paused = true)]
async fn an_exhausted_downgrade_restart_pauses_without_deleting_the_scope_cursor() {
    let id = AccountId("downgrade-exhausted".to_owned());
    let first = failing_once(downgrade_error(ErrorScope::Cursor(SCOPE)));
    let opens = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn AccountFactory> = Arc::new(RefusingReplacementFactory {
        first: Mutex::new(Some(Arc::clone(&first))),
        opens: Arc::clone(&opens),
    });
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    let mut control = engine
        .account_control_stream(&id)
        .expect("control stream for an attached account");

    // Only a cursor that exists can be observed surviving.
    assert!(
        store
            .get_change_cursor(&id, &SCOPE)
            .await
            .expect("cursor lookup")
            .is_some(),
        "attach must have persisted the scope's cursor, or survival proves nothing"
    );

    let pause = tokio::time::timeout(BOUND, async {
        loop {
            match control.recv().await {
                Ok(bifrost_types::AccountControl::Pause(reason)) => break reason,
                // `AccountControl` is `#[non_exhaustive]`; anything else is
                // not the pause this test waits for.
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("control stream closed before the account paused")
                }
            }
        }
    })
    .await
    .expect("the account never paused after every replacement open failed");
    assert_eq!(pause, bifrost_types::PauseReason::RetryBudgetExhausted);
    assert_eq!(
        opens.load(Ordering::SeqCst),
        4,
        "one attach open plus a three-attempt restart budget"
    );

    // The scope repair, had it run, executes right after the restart returns,
    // so give it every chance to run before reading.
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert!(
        store
            .get_change_cursor(&id, &SCOPE)
            .await
            .expect("cursor lookup")
            .is_some(),
        "the scope's durable cursor was deleted by a scope repair that ran after \
         the account restart gave up"
    );

    // After resume the scope must still be there to sync: the cursor survives
    // and the first account is still the live one.
    engine.resume_account(&id).expect("resume");
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert!(
        store
            .get_change_cursor(&id, &SCOPE)
            .await
            .expect("cursor lookup")
            .is_some(),
        "the scope's durable cursor did not survive the pause and resume"
    );
    assert_eq!(
        first.closed.load(Ordering::SeqCst),
        0,
        "no replacement was ever swapped in, so the first account stays live"
    );

    tokio::time::timeout(BOUND, engine.detach(&id))
        .await
        .expect("detach after an exhausted restart must not hang")
        .expect("detach succeeds");
}
