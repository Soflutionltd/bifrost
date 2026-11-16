//! Supervisor restart budget contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_sync::SyncEngine;
use bifrost_types::{
    Account, AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountFuture,
    AccountId, CursorScope, PrimaryCause, ScopeLifecycleEvent, SyncEvent,
};
use common::{caps, unsupported};
use recovery::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Factory whose first open succeeds with a scripted lifecycle stream and
/// whose every later open fails, so an `Engine(RestartAccount)` recovery can
/// never bump the account generation.
struct ParkedReopenFactory {
    scopes: Vec<CursorScope>,
    lifecycle_script: Arc<Mutex<VecDeque<ScopeLifecycleEvent>>>,
    lifecycle_calls: Arc<Mutex<Vec<usize>>>,
    opens: AtomicUsize,
}

impl AccountFactory for ParkedReopenFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        let scopes = self.scopes.clone();
        let lifecycle_script = Arc::clone(&self.lifecycle_script);
        let lifecycle_calls = Arc::clone(&self.lifecycle_calls);
        Box::pin(async move {
            if generation > 0 {
                return Err(unsupported(bifrost_types::AccountOperation::Discover));
            }
            let account: Arc<dyn Account> = Arc::new(
                HealAccount {
                    caps: caps(),
                    scopes,
                    established: Arc::new(Mutex::new(Vec::new())),
                    closed: Arc::new(AtomicUsize::new(0)),
                    generation,
                    closed_generations: Arc::new(Mutex::new(Vec::new())),
                    subscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                    lifecycle_calls,
                    lifecycle_script,
                    degraded_contacts: false,
                    close_gate: no_close_gate(),
                    bad_boundary_batches: None,
                    live_checkpoint_batch: false,
                }
                .into_stub(),
            );
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

/// End-to-end pin (under paused time) for the lifecycle reader's bounded
/// reopen park. The stream terminates with an `Engine(RestartAccount)`-class
/// error, and every replacement open fails, so the account generation never
/// changes. The reader must neither reconnect immediately (the handover
/// prefers waiting for the replacement connection) nor park forever (a
/// reopen that exhausts its budget produces no generation change): it must
/// come back on its own after the 30s bound.
#[tokio::test(start_paused = true)]
async fn lifecycle_reader_unparks_after_the_bounded_reopen_wait() {
    use bifrost_types::StateCause;

    let account_id = AccountId("bounded-lifecycle-park".to_owned());
    let terminal = AccountErrorBuilder::new(PrimaryCause::State(StateCause::CapabilityChanged {
        delta: None,
    }))
    .operation(bifrost_types::AccountOperation::SyncChanges)
    .try_build()
    .expect("valid account-reopen recovery error");
    let lifecycle_calls = Arc::new(Mutex::new(Vec::new()));
    let factory: Arc<dyn AccountFactory> = Arc::new(ParkedReopenFactory {
        scopes: vec![CursorScope::Account],
        lifecycle_script: Arc::new(Mutex::new(VecDeque::from([
            ScopeLifecycleEvent::Terminated(terminal),
        ]))),
        lifecycle_calls: Arc::clone(&lifecycle_calls),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");

    let start = tokio::time::Instant::now();
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach");

    let mut reconnects = 0;
    for _ in 0..200 {
        reconnects = lifecycle_calls.lock().expect("lifecycle calls lock").len();
        if reconnects >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        reconnects >= 2,
        "the lifecycle reader must reconnect after the bounded wait"
    );
    assert!(
        start.elapsed() >= std::time::Duration::from_secs(30),
        "the reader reconnected before the reopen wait elapsed: {:?}",
        start.elapsed()
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Factory whose first open (attach) succeeds and whose every later open
/// fails, recording the instant of every attempt. The account-wide reopen
/// budget therefore always exhausts, and the recorded instants expose the
/// backoff schedule the engine slept between attempts.
///
/// The lifecycle script is shared with the test so the terminal event can be
/// armed AFTER attach returns: the lifecycle reader reconnects on a bounded
/// backoff and re-reads the script on each reconnect, which lets the test
/// subscribe to the changes and control streams before the reopen storm
/// starts. A broadcast subscriber only sees events sent after it subscribed,
/// so arming the trigger inside the factory would race the assertions.
struct ExhaustingFactory {
    lifecycle_script: Arc<Mutex<VecDeque<ScopeLifecycleEvent>>>,
    opens: AtomicUsize,
    open_instants: Arc<Mutex<Vec<tokio::time::Instant>>>,
}

impl AccountFactory for ExhaustingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        self.open_instants
            .lock()
            .expect("open instants lock")
            .push(tokio::time::Instant::now());
        let lifecycle_script = Arc::clone(&self.lifecycle_script);
        Box::pin(async move {
            if generation > 0 {
                return Err(unsupported(bifrost_types::AccountOperation::Discover));
            }
            let account: Arc<dyn Account> = Arc::new(
                HealAccount {
                    caps: caps(),
                    scopes: vec![CursorScope::Account],
                    established: Arc::new(Mutex::new(Vec::new())),
                    closed: Arc::new(AtomicUsize::new(0)),
                    generation,
                    closed_generations: Arc::new(Mutex::new(Vec::new())),
                    subscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                    lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                    lifecycle_script,
                    degraded_contacts: false,
                    close_gate: no_close_gate(),
                    bad_boundary_batches: None,
                    live_checkpoint_batch: false,
                }
                .into_stub(),
            );
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

/// Three consecutive `factory.open` failures exhaust the account-wide reopen
/// budget. This test's last error is terminal, so it remains unchanged in
/// `SyncEvent::Terminated` before the account pauses with
/// `PauseReason::RetryBudgetExhausted`.
///
/// Run under paused time so the backoff schedule is exact: the gap between
/// two recorded opens IS the slept delay, with no wall-clock noise. The
/// asserted windows are the documented base delays (1s, then 2s) widened by
/// the engine's +0..25% jitter.
#[tokio::test(start_paused = true)]
async fn three_failed_reopens_terminate_and_pause_the_account() {
    use bifrost_types::StateCause;
    use tokio::sync::broadcast::error::RecvError;

    let account_id = AccountId("reopen-budget-exhausted".to_owned());
    let lifecycle_script = Arc::new(Mutex::new(VecDeque::new()));
    let open_instants = Arc::new(Mutex::new(Vec::new()));
    let factory: Arc<dyn AccountFactory> = Arc::new(ExhaustingFactory {
        lifecycle_script: Arc::clone(&lifecycle_script),
        opens: AtomicUsize::new(0),
        open_instants: Arc::clone(&open_instants),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("the first open succeeds, so attach succeeds");

    let mut changes = engine
        .account_changes_stream(&account_id)
        .expect("attached account exposes its change stream");
    let mut control = engine
        .account_control_stream(&account_id)
        .expect("attached account exposes its control stream");

    // Arm the trigger: an `Engine(RestartAccount)`-class lifecycle
    // termination, picked up on the reader's next reconnect.
    let trigger = AccountErrorBuilder::new(PrimaryCause::State(StateCause::CapabilityChanged {
        delta: None,
    }))
    .operation(bifrost_types::AccountOperation::SyncChanges)
    .try_build()
    .expect("valid account-reopen recovery error");
    lifecycle_script
        .lock()
        .expect("lifecycle script lock")
        .push_back(ScopeLifecycleEvent::Terminated(trigger));

    let (scope, error) = tokio::time::timeout(std::time::Duration::from_secs(600), async {
        loop {
            match changes.recv().await {
                Ok(event) => {
                    if let SyncEvent::Terminated(err) = event.event.as_ref() {
                        break (event.scope, err.clone());
                    }
                }
                Err(RecvError::Closed) => panic!("the change stream closed before Terminated"),
                // Lagged: a slow subscriber dropped events, keep reading.
                Err(_) => {}
            }
        }
    })
    .await
    .expect("an exhausted reopen budget must broadcast Terminated");

    assert_eq!(
        scope,
        bifrost_sync::EventScope::Account,
        "an exhausted account-wide budget terminates the attachment"
    );
    let expected = unsupported(bifrost_types::AccountOperation::Discover);
    assert_eq!(
        error.message_key(),
        expected.message_key(),
        "the terminal failure is preserved, not replaced by the lifecycle trigger"
    );
    assert!(
        matches!(error.kind(), AccountErrorKind::Unsupported(_)),
        "the terminating error keeps its classification: {:?}",
        error.kind()
    );

    let pause = tokio::time::timeout(std::time::Duration::from_secs(600), async {
        loop {
            match control.recv().await {
                Ok(bifrost_types::AccountControl::Pause(reason)) => break reason,
                // `AccountControl` is `#[non_exhaustive]`; anything that
                // is not the pause we are waiting for is skipped.
                Ok(_) => {}
                Err(RecvError::Closed) => panic!("the control stream closed before the pause"),
                Err(_) => {}
            }
        }
    })
    .await
    .expect("an exhausted reopen budget must pause the account");
    assert_eq!(
        pause,
        bifrost_types::PauseReason::RetryBudgetExhausted,
        "the pause names the budget, not a generic operator override"
    );

    let instants = open_instants.lock().expect("open instants lock").clone();
    assert_eq!(
        instants.len(),
        4,
        "one open for attach plus exactly three reopen attempts, then the budget stops trying"
    );
    // instants[0] is attach and instants[1] the first reopen attempt; the
    // gap between them also covers the lifecycle reader's own reconnect
    // backoff, so only the inter-attempt gaps pin the reopen schedule.
    let first_backoff = instants[2] - instants[1];
    let second_backoff = instants[3] - instants[2];
    assert!(
        (std::time::Duration::from_millis(1000)..=std::time::Duration::from_millis(1250))
            .contains(&first_backoff),
        "the second attempt waits the 1s base delay +0..25% jitter: {first_backoff:?}"
    );
    assert!(
        (std::time::Duration::from_millis(2000)..=std::time::Duration::from_millis(2500))
            .contains(&second_backoff),
        "the third attempt waits the doubled 2s delay +0..25% jitter: {second_backoff:?}"
    );

    engine
        .resume_account(&account_id)
        .expect("the paused account resumes on consumer request");
    engine.detach(&account_id).await.expect("detach");
}
