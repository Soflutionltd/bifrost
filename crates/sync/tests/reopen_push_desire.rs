//! A reopen must keep the consumer's push subscriptions alive across a failed
//! old-side teardown.
//!
//! Reopen recreates every registered subscription on the replacement and then
//! tears the old handles down on the old account. When one of those teardowns
//! fails (a Graph 503 on the DELETE, a JMAP socket that died), the attempt
//! aborts and the record is flagged `teardown_unconfirmed` so the next attempt
//! does not abort on it again. That flag used to double as "orphan, never
//! recreate", so the next attempt tore the old handle down (or carried it) and
//! recreated nothing: the subscription the consumer still wanted vanished with
//! no error anywhere. Whether the old handle's teardown is confirmed and
//! whether the consumer still wants the coverage are two separate facts, and
//! these tests pin both halves.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{
    AccountFactory, AccountId, CursorScope, FolderId, PushCapability, SubscriptionHandle,
};

use common::{PushLog, StubAccount, StubFactory};

fn inbox() -> Vec<CursorScope> {
    vec![CursorScope::Folder(FolderId("inbox".into()))]
}

/// A push-capable account named `label` whose first `failures` teardowns fail.
fn pushing(label: &str, log: &Arc<PushLog>, failures: usize) -> Arc<StubAccount> {
    let mut caps = common::caps();
    caps.push = PushCapability::Webhook;
    Arc::new(StubAccount {
        caps,
        push_label: label.to_owned(),
        push_log: Arc::clone(log),
        push_unsubscribe_failures: AtomicUsize::new(failures),
        ..StubAccount::new(inbox())
    })
}

fn call(label: &str, handle: &str) -> (String, SubscriptionHandle) {
    (label.to_owned(), SubscriptionHandle(handle.to_owned()))
}

fn subscribed_on(label: &str) -> (String, Vec<CursorScope>) {
    (label.to_owned(), inbox())
}

async fn attached(id: &AccountId, accounts: Vec<Arc<StubAccount>>) -> SyncEngine {
    let engine = SyncEngine::builder().build().expect("engine");
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(accounts));
    engine.attach(id.clone(), factory).await.expect("attach");
    engine
        .subscribe_push(id, &inbox())
        .await
        .expect("consumer subscribes");
    engine
}

/// The old connection is dead, so every teardown on it fails. The first
/// reopen aborts on that; the retry must commit AND leave the consumer
/// subscribed on the replacement, with the old handle still tracked as an
/// orphan for a later teardown.
///
/// Against the pre-fix code the retry never calls `push_subscribe` on
/// `replacement`, so the `subscribed` assertion fails, and the final
/// `unsubscribe_push` finds only the orphaned old handle.
#[tokio::test]
async fn a_failed_old_side_teardown_does_not_cost_the_consumer_its_subscription() {
    let id = AccountId("reopen-dead-old-connection".into());
    let log = Arc::new(PushLog::default());
    let old = pushing("old", &log, usize::MAX);
    let aborted = pushing("aborted", &log, 0);
    let replacement = pushing("replacement", &log, 0);
    let engine = attached(
        &id,
        vec![
            Arc::clone(&old),
            Arc::clone(&aborted),
            Arc::clone(&replacement),
        ],
    )
    .await;

    assert!(
        matches!(engine.reattach(&id).await, Err(Error::Account(_))),
        "a first failure of a live handle's old-side teardown still aborts the swap"
    );
    engine
        .reattach(&id)
        .await
        .expect("the retry proceeds past the already-unconfirmed teardown");

    assert_eq!(
        log.subscribed(),
        vec![
            subscribed_on("old"),
            subscribed_on("aborted"),
            subscribed_on("replacement"),
        ],
        "the consumer's subscription must be recreated on the replacement that \
         was actually installed"
    );
    assert_eq!(old.closed.load(Ordering::SeqCst), 1, "old account retired");
    assert_eq!(
        aborted.closed.load(Ordering::SeqCst),
        1,
        "aborted replacement closed"
    );
    assert_eq!(
        replacement.closed.load(Ordering::SeqCst),
        0,
        "the replacement is the live account"
    );

    engine
        .unsubscribe_push(&id)
        .await
        .expect("both records tear down against the live account");
    assert_eq!(
        log.unsubscribed(),
        vec![
            // Attempt 1: the old handle refuses, the replacement's copy unwinds.
            call("old", "old"),
            call("aborted", "aborted"),
            // Attempt 2: the old handle refuses again and is carried.
            call("old", "old"),
            // The consumer's teardown: the recreated subscription, then the
            // carried orphan.
            call("replacement", "replacement"),
            call("replacement", "old"),
        ],
        "the registry must hold the recreated subscription AND the orphan"
    );

    engine.detach(&id).await.expect("detach");
}

/// A transient failure - the first DELETE fails, the retry's succeeds. The
/// old handle is then genuinely gone, and the replacement must hold the
/// consumer's subscription.
///
/// Against the pre-fix code this was the worse half: the retry's teardown
/// succeeded, so nothing was even carried, and the registry ended up empty.
#[tokio::test]
async fn a_transient_old_side_teardown_failure_still_recreates_on_the_retry() {
    let id = AccountId("reopen-transient-teardown".into());
    let log = Arc::new(PushLog::default());
    let engine = attached(
        &id,
        vec![
            pushing("old", &log, 1),
            pushing("aborted", &log, 0),
            pushing("replacement", &log, 0),
        ],
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    engine.reattach(&id).await.expect("retry commits");

    assert_eq!(
        log.subscribed(),
        vec![
            subscribed_on("old"),
            subscribed_on("aborted"),
            subscribed_on("replacement"),
        ],
    );
    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed(),
        vec![
            call("old", "old"),
            call("aborted", "aborted"),
            call("old", "old"),
            call("replacement", "replacement"),
        ],
        "exactly the recreated subscription is registered, nothing else"
    );

    engine.detach(&id).await.expect("detach");
}

/// Where the hole would move to: the old handle carried past a committed
/// reopen must be carried as an ORPHAN, because its desire now lives in the
/// record recreated on the replacement. Carrying it still marked as wanted
/// would make every later reopen subscribe once per carried copy.
///
/// Bites twice: the pre-fix code subscribes on `later` zero times, and a fix
/// that recreated wanted records without demoting the carried one subscribes
/// on it twice.
#[tokio::test]
async fn the_carried_old_handle_is_not_recreated_by_a_later_reopen() {
    let id = AccountId("reopen-carried-orphan".into());
    let log = Arc::new(PushLog::default());
    let engine = attached(
        &id,
        vec![
            pushing("old", &log, usize::MAX),
            pushing("aborted", &log, 0),
            pushing("replacement", &log, 0),
            pushing("later", &log, 0),
        ],
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    engine.reattach(&id).await.expect("second attempt commits");
    engine.reattach(&id).await.expect("a later reopen commits");

    assert_eq!(
        log.subscribed(),
        vec![
            subscribed_on("old"),
            subscribed_on("aborted"),
            subscribed_on("replacement"),
            subscribed_on("later"),
        ],
        "one wanted subscription, recreated exactly once per installed account"
    );
    assert_eq!(
        log.unsubscribed(),
        vec![
            call("old", "old"),
            call("aborted", "aborted"),
            call("old", "old"),
            // The later reopen retires the replacement's subscription and
            // retries the orphan, which now succeeds.
            call("replacement", "replacement"),
            call("replacement", "old"),
        ],
    );

    engine.detach(&id).await.expect("detach");
}

/// The other half of the distinction, pinned so a fix cannot overcorrect: a
/// subscription the CONSUMER asked to tear down, whose teardown failed, is an
/// orphan. A reopen retries its teardown and must not bring it back.
///
/// This passes against the pre-fix code too; it guards against a fix that
/// simply stopped filtering unconfirmed records.
#[tokio::test]
async fn a_subscription_the_consumer_tore_down_is_not_resurrected_by_reopen() {
    let id = AccountId("reopen-consumer-orphan".into());
    let log = Arc::new(PushLog::default());
    let engine = attached(
        &id,
        vec![pushing("old", &log, 1), pushing("replacement", &log, 0)],
    )
    .await;

    assert!(
        matches!(engine.unsubscribe_push(&id).await, Err(Error::Account(_))),
        "the consumer's teardown fails once"
    );
    engine
        .reattach(&id)
        .await
        .expect("reopen retries the orphan and commits");

    assert_eq!(
        log.subscribed(),
        vec![subscribed_on("old")],
        "a torn-down subscription must not be recreated on the replacement"
    );
    assert_eq!(
        log.unsubscribed(),
        vec![call("old", "old"), call("old", "old")],
        "the reopen retried the orphan's teardown on the old account"
    );

    engine
        .unsubscribe_push(&id)
        .await
        .expect("nothing left to tear down");
    assert_eq!(log.unsubscribed().len(), 2, "the registry is empty");

    engine.detach(&id).await.expect("detach");
}

/// An old account that is strict about handles and refuses to delete exactly
/// the named ones, however often it is asked.
fn strict_refusing(label: &str, log: &Arc<PushLog>, refused: &[&str]) -> Arc<StubAccount> {
    let mut caps = common::caps();
    caps.push = PushCapability::Webhook;
    Arc::new(StubAccount {
        caps,
        push_label: label.to_owned(),
        push_log: Arc::clone(log),
        push_unsubscribe_refused: refused.iter().map(|h| (*h).to_owned()).collect(),
        push_unsubscribe_strict: true,
        ..StubAccount::new(inbox())
    })
}

/// Attach and register `subscriptions` push subscriptions on the first account.
async fn attached_with_subscriptions(
    id: &AccountId,
    accounts: Vec<Arc<StubAccount>>,
    subscriptions: usize,
) -> SyncEngine {
    let engine = SyncEngine::builder().build().expect("engine");
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(accounts));
    engine.attach(id.clone(), factory).await.expect("attach");
    for _ in 0..subscriptions {
        engine
            .subscribe_push(id, &inbox())
            .await
            .expect("consumer subscribes");
    }
    engine
}

/// Three subscriptions on a dead old connection. Every reopen attempt used to
/// flag only the FIRST refusing record and abort, so three records needed four
/// attempts and the engine's budget is three: the account paused for good.
/// Flagging every refusal in one pass makes the whole reopen cost two attempts
/// whatever the count.
///
/// Reverting the pass-continues change in `reattach_account` (aborting at the
/// first refusal again) makes the SECOND `reattach` here return an error, and
/// the unsubscribe log lose the `old-2` / `old-3` entries of attempt 1.
#[tokio::test]
async fn many_failing_old_side_teardowns_cost_one_aborted_attempt() {
    let id = AccountId("reopen-many-dead-handles".into());
    let log = Arc::new(PushLog::default());
    let engine = attached_with_subscriptions(
        &id,
        vec![
            pushing("old", &log, usize::MAX),
            pushing("aborted", &log, 0),
            pushing("replacement", &log, 0),
        ],
        3,
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    engine
        .reattach(&id)
        .await
        .expect("every refusal was flagged by the first attempt, so the second commits");

    assert_eq!(
        log.unsubscribed(),
        vec![
            // Attempt 1 tries every old handle, then unwinds its replacement.
            call("old", "old"),
            call("old", "old-2"),
            call("old", "old-3"),
            call("aborted", "aborted"),
            call("aborted", "aborted-2"),
            call("aborted", "aborted-3"),
            // Attempt 2 carries all three, none of them aborting.
            call("old", "old"),
            call("old", "old-2"),
            call("old", "old-3"),
        ],
    );
    assert_eq!(
        log.subscribed()
            .iter()
            .filter(|(label, _)| label == "replacement")
            .count(),
        3,
        "all three wanted subscriptions were recreated on the installed account"
    );

    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed()[9..],
        [
            call("replacement", "replacement"),
            call("replacement", "replacement-2"),
            call("replacement", "replacement-3"),
            call("replacement", "old"),
            call("replacement", "old-2"),
            call("replacement", "old-3"),
        ],
        "three recreated subscriptions and three carried orphans, no more"
    );

    engine.detach(&id).await.expect("detach");
}

/// Two of three handles delete cleanly and one refuses, on a provider that
/// errors on an unknown handle. The refusal aborts the attempt AFTER the other
/// two are gone; the retry must not delete them a second time.
///
/// Reverting the `torn_down` skip in the teardown pass makes the second
/// `reattach` fail (the strict provider rejects the repeated `old`), and the
/// log shows `old` and `old-3` twice.
#[tokio::test]
async fn an_old_handle_already_torn_down_is_not_torn_down_again_by_the_retry() {
    let id = AccountId("reopen-torn-down-then-abort".into());
    let log = Arc::new(PushLog::default());
    let engine = attached_with_subscriptions(
        &id,
        vec![
            strict_refusing("old", &log, &["old-2"]),
            pushing("aborted", &log, 0),
            pushing("replacement", &log, 0),
        ],
        3,
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    engine
        .reattach(&id)
        .await
        .expect("the retry only meets the handle that is still unconfirmed");

    assert_eq!(
        log.unsubscribed(),
        vec![
            call("old", "old"),
            call("old", "old-2"),
            call("old", "old-3"),
            call("aborted", "aborted"),
            call("aborted", "aborted-2"),
            call("aborted", "aborted-3"),
            // Attempt 2: ONLY the refused handle; the two deleted ones are
            // not asked again.
            call("old", "old-2"),
        ],
    );
    assert_eq!(
        log.subscribed()
            .iter()
            .filter(|(label, _)| label == "replacement")
            .count(),
        3,
        "the torn-down records were still wanted, so all three are recreated"
    );

    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed()[7..],
        [
            call("replacement", "replacement"),
            call("replacement", "replacement-2"),
            call("replacement", "replacement-3"),
            call("replacement", "old-2"),
        ],
        "the deleted handles were dropped, the refused one carried"
    );

    engine.detach(&id).await.expect("detach");
}

/// Where the hole could move to: after an aborted reopen the old account is
/// still installed, and a consumer `unsubscribe_push` lands before the retry.
/// The already-deleted handles must not be sent to the provider again, or a
/// strict provider turns them into permanently failing orphans.
///
/// Reverting the `torn_down` filter in `SubscriptionRegistry::take` makes the
/// log show `old` and `old-3` deleted twice.
#[tokio::test]
async fn a_consumer_unsubscribe_after_an_aborted_reopen_skips_deleted_handles() {
    let id = AccountId("reopen-abort-then-unsubscribe".into());
    let log = Arc::new(PushLog::default());
    let engine = attached_with_subscriptions(
        &id,
        vec![
            strict_refusing("old", &log, &["old-2"]),
            pushing("aborted", &log, 0),
        ],
        3,
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    assert!(
        matches!(engine.unsubscribe_push(&id).await, Err(Error::Account(_))),
        "the refused handle still refuses"
    );

    assert_eq!(
        log.unsubscribed()[6..],
        [call("old", "old-2")],
        "only the handle that was never deleted is asked about again"
    );

    engine.detach(&id).await.expect("detach");
}

/// Yield until `done` holds. Bounded by iterations, not the clock, so a cleanup
/// that never happens fails the test rather than hanging it.
async fn settle(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held");
}

/// Drive `SyncEngine::reattach` until `parked` holds, then drop the future
/// there, as a consumer timeout or a worker abort would.
async fn drop_reattach_once(engine: &SyncEngine, id: &AccountId, parked: impl FnMut() -> bool) {
    let fut = engine.reattach(id);
    tokio::pin!(fut);
    tokio::select! {
        biased;
        _ = &mut fut => panic!("reattach finished before it parked"),
        () = settle(parked) => {}
    }
}

fn parked_forever() -> Arc<tokio::sync::Notify> {
    Arc::new(tokio::sync::Notify::new())
}

/// The filed defect. The replacement's subscription exists, the old handle's
/// teardown is parked, and the reattach future is dropped there. The
/// subscription must be deleted on the replacement (and the replacement closed)
/// rather than left live and registered nowhere, and a later reopen and
/// consumer unsubscribe must see a coherent registry.
///
/// Reverting the `Drop` impl of `ReplacementSubscriptions` (or its spawn) leaves
/// `call("replacement", "replacement")` out of the log and `replacement.closed`
/// at zero, so `settle` panics.
#[tokio::test]
async fn a_reattach_dropped_after_creating_replacement_subscriptions_retires_them() {
    let id = AccountId("reopen-dropped-in-teardown".into());
    let log = Arc::new(PushLog::default());
    let old = pushing("old", &log, 0);
    *old.push_unsubscribe_park.lock().expect("park lock") = Some(parked_forever());
    let replacement = pushing("replacement", &log, 0);
    let later = pushing("later", &log, 0);
    let engine = attached(
        &id,
        vec![
            Arc::clone(&old),
            Arc::clone(&replacement),
            Arc::clone(&later),
        ],
    )
    .await;

    drop_reattach_once(&engine, &id, || log.unsubscribed().len() == 1).await;
    settle(|| replacement.closed.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        log.unsubscribed(),
        vec![call("old", "old"), call("replacement", "replacement")],
        "the replacement's own subscription is deleted on the replacement"
    );
    assert_eq!(old.closed.load(Ordering::SeqCst), 0, "old stays installed");

    engine.reattach(&id).await.expect("a later reopen commits");
    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed()[2..],
        [
            // The retry re-deletes the old handle (the dropped delete never
            // reported back), then the consumer's teardown hits the live one.
            call("old", "old"),
            call("later", "later"),
        ],
        "the dropped attempt's handle is not deleted or carried a second time"
    );
    assert_eq!(replacement.closed.load(Ordering::SeqCst), 1);

    engine.detach(&id).await.expect("detach");
}

/// Same defect one await earlier: the future is dropped parked inside the
/// SECOND `push_subscribe`, so the first replacement subscription exists and
/// the guard must already own it. The parked call's own subscription is the
/// account implementation's cancellation contract; the engine never received
/// its handle and cannot delete it.
///
/// Reverting the `replacement_subscriptions.push(..)` into the guard makes the
/// log lack the `replacement` delete.
#[tokio::test]
async fn a_reattach_dropped_between_two_replacement_subscribes_retires_the_first() {
    let id = AccountId("reopen-dropped-in-subscribe".into());
    let log = Arc::new(PushLog::default());
    let replacement = pushing("replacement", &log, 0);
    *replacement.push_subscribe_park.lock().expect("park lock") = Some((1, parked_forever()));
    let engine = attached_with_subscriptions(
        &id,
        vec![pushing("old", &log, 0), Arc::clone(&replacement)],
        2,
    )
    .await;

    drop_reattach_once(&engine, &id, || log.subscribed().len() == 4).await;
    settle(|| replacement.closed.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        log.unsubscribed(),
        vec![call("replacement", "replacement")],
        "only the handle the engine actually received is retired"
    );

    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed()[1..],
        [call("old", "old"), call("old", "old-2")],
        "the consumer's records were untouched by the dropped attempt"
    );

    engine.detach(&id).await.expect("detach");
}

/// A cleanup delete that is refused must not lose the handle: it is registered
/// as an orphan and retried by the next consumer teardown, against the account
/// that is current then (here the still-installed old one).
///
/// Reverting `register_orphans` (the restore in the retire task) drops the
/// `call("old", "replacement")` entry.
#[tokio::test]
async fn a_refused_cleanup_after_a_dropped_reattach_is_carried_as_an_orphan() {
    let id = AccountId("reopen-dropped-cleanup-refused".into());
    let log = Arc::new(PushLog::default());
    let old = pushing("old", &log, 0);
    *old.push_unsubscribe_park.lock().expect("park lock") = Some(parked_forever());
    let replacement = pushing("replacement", &log, usize::MAX);
    let engine = attached(&id, vec![Arc::clone(&old), Arc::clone(&replacement)]).await;

    drop_reattach_once(&engine, &id, || log.unsubscribed().len() == 1).await;
    settle(|| replacement.closed.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        log.unsubscribed().len(),
        2,
        "the cleanup tried and was refused"
    );

    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed()[2..],
        [call("old", "old"), call("old", "replacement")],
        "the consumer's record, then the carried orphan"
    );

    engine.detach(&id).await.expect("detach");
}

/// The commit path must disarm the guard: the subscriptions it created are
/// the installed account's, and a guard that still owned them when the future
/// finished would delete them.
///
/// Reverting the `disarm()` before the cutover makes the finished future's
/// guard spawn a delete of `replacement` and close the live account.
#[tokio::test]
async fn a_committed_reattach_does_not_retire_the_subscriptions_it_installed() {
    let id = AccountId("reopen-commit-disarms".into());
    let log = Arc::new(PushLog::default());
    let replacement = pushing("replacement", &log, 0);
    let engine = attached(&id, vec![pushing("old", &log, 0), Arc::clone(&replacement)]).await;

    engine.reattach(&id).await.expect("commit");
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(log.unsubscribed(), vec![call("old", "old")]);
    assert_eq!(replacement.closed.load(Ordering::SeqCst), 0);

    engine.detach(&id).await.expect("detach");
}

/// An ordinary abort already unwinds the replacement; the guard's drop must
/// find nothing left and not unwind it a second time or close the replacement
/// twice.
///
/// Reverting the removal of each record in `unwind_replacement_subscriptions`
/// makes the guard's drop re-delete `aborted` and close it twice.
#[tokio::test]
async fn an_ordinary_abort_does_not_unwind_the_replacement_twice() {
    let id = AccountId("reopen-abort-no-double-unwind".into());
    let log = Arc::new(PushLog::default());
    let aborted = pushing("aborted", &log, 0);
    let engine = attached(
        &id,
        vec![pushing("old", &log, usize::MAX), Arc::clone(&aborted)],
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        log.unsubscribed(),
        vec![call("old", "old"), call("aborted", "aborted")]
    );
    assert_eq!(aborted.closed.load(Ordering::SeqCst), 1);

    engine.detach(&id).await.expect("detach");
}

/// A dropped reattach's detached cleanup must not restore a refused handle into
/// the registry while a later reattach is between its snapshot and its commit.
///
/// The interleaving, pinned with the park gates: reattach A is dropped after
/// creating `r1`'s subscription; the cleanup's delete of it is parked and will be
/// refused. Reattach B starts and parks inside its own `push_subscribe`, so its
/// registry snapshot exists. The cleanup's delete is then released and refused.
/// B commits with `replace`, built from its snapshot alone. Unlocked, the orphan
/// the cleanup restored in between is overwritten and the `r1` subscription is
/// never retried; under the slot's reopen lock the restore waits for B's commit
/// and merges into what B installed.
///
/// Removing the lock acquisition around `register_orphans` in
/// `retire_stranded_subscriptions` drops `call("r2", "r1")` from the log.
#[tokio::test]
async fn a_cleanup_refusal_restored_mid_reattach_is_not_overwritten_by_its_commit() {
    let id = AccountId("reopen-cleanup-restore-vs-commit".into());
    let log = Arc::new(PushLog::default());
    let old = pushing("old", &log, 0);
    *old.push_unsubscribe_park.lock().expect("park lock") = Some(parked_forever());
    let r1 = pushing("r1", &log, usize::MAX);
    let cleanup_gate = parked_forever();
    *r1.push_unsubscribe_park.lock().expect("park lock") = Some(Arc::clone(&cleanup_gate));
    let r2 = pushing("r2", &log, 0);
    let commit_gate = parked_forever();
    *r2.push_subscribe_park.lock().expect("park lock") = Some((0, Arc::clone(&commit_gate)));
    let engine = attached(
        &id,
        vec![Arc::clone(&old), Arc::clone(&r1), Arc::clone(&r2)],
    )
    .await;

    // A: dropped with r1's subscription live; the cleanup parks in r1's delete.
    drop_reattach_once(&engine, &id, || log.unsubscribed().len() == 1).await;
    settle(|| log.unsubscribed().len() == 2).await;

    // B: parked inside r2's subscribe, so its registry snapshot is taken.
    let reattach_b = engine.reattach(&id);
    tokio::pin!(reattach_b);
    tokio::select! {
        biased;
        _ = &mut reattach_b => panic!("reattach B finished before it parked"),
        () = settle(|| log.subscribed().iter().any(|(label, _)| label == "r2")) => {}
    }

    // The cleanup's delete is refused now, mid-B.
    cleanup_gate.notify_one();
    for _ in 0..100 {
        tokio::select! {
            biased;
            _ = &mut reattach_b => panic!("reattach B finished while parked"),
            () = tokio::task::yield_now() => {}
        }
    }

    commit_gate.notify_one();
    reattach_b.await.expect("B commits");
    settle(|| r1.closed.load(Ordering::SeqCst) == 1).await;

    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed()[3..],
        [call("r2", "r2"), call("r2", "r1")],
        "the refused cleanup handle survived B's commit as an orphan"
    );

    engine.detach(&id).await.expect("detach");
}
