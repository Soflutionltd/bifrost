use bifrost_net::test_support::{Canned, ScriptedDispatch};
use bytes::Bytes;
use reqwest::StatusCode;
use futures::StreamExt;
// Pull sessions exercise the actor directly, including lost replies and gaps.
use super::*;
use bifrost_types::{PushSessionEvent as E, PushSessionEnd as End, PushGate, SessionStop};

fn response(expiry: Option<u64>) -> Canned {
    let mut body = serde_json::json!({"historyId":"2"});
    if let Some(expiry) = expiry { body["expiration"] = serde_json::Value::String(expiry.to_string()); }
    Canned::Response { status: StatusCode::OK, headers: Default::default(), body: Bytes::from(body.to_string()) }
}
async fn control(expiry: Option<u64>, more: Vec<Canned>, name: &str) -> (Arc<PubSubControl>, Arc<ScriptedDispatch>) {
    let (client, script) = scripted_client(std::iter::once(response(expiry)).chain(more));
    let client = Arc::new(client.with_fixed_clock(UNIX_EPOCH + Duration::from_secs(1000)));
    let control = Arc::new(PubSubControl::new(client, Some(PubSubConfig::new("projects/p/topics/t")), CancellationToken::new(), name));
    push_subscribe(Arc::clone(&control), vec![CursorScope::Account]).await.unwrap();
    (control, script)
}
async fn renew(control: &PubSubControl) -> Result<RenewOutcome, AccountError> {
    let (reply, response) = oneshot::channel();
    control.commands.send(WatchCommand::Renew { reply }).await.unwrap();
    response.await.unwrap()
}
async fn fail_terminal(control: &Arc<PubSubControl>) {
    tokio::time::advance(MIN_RENEW_DELAY).await;
    let mut session = test_session(control);
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Failed(e)) if e.recovery().is_terminal()));
}
#[tokio::test(start_paused = true)]
async fn a_renewal_completing_after_expiry_records_a_gap() {
    let wall = Arc::new(AtomicU64::new(1000));
    let clock = Arc::clone(&wall);
    let (client, script) = scripted_client([response(Some(1_200_000)), response(Some(9_000_000))]);
    let client = Arc::new(client.with_test_clock(Arc::new(move || UNIX_EPOCH + Duration::from_secs(clock.load(Ordering::SeqCst)))));
    let control = Arc::new(PubSubControl::new(client, Some(PubSubConfig::new("projects/p/topics/t")), CancellationToken::new(), "completion-clock"));
    push_subscribe(Arc::clone(&control), vec![CursorScope::Account]).await.unwrap();
    let owner = mailbox_watch("completion-clock");
    let exchange = owner.exchange.lock().await;
    tokio::time::advance(MIN_RENEW_DELAY).await;
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    for _ in 0..20 { tokio::task::yield_now().await; }
    assert_eq!(script.requests().len(), 1);
    wall.store(1300, Ordering::SeqCst);
    drop(exchange);
    assert!(matches!(next.await, Some(E::Invalidated(_))));
    assert!(matches!(session.next().await, Some(E::Healthy)));
}

#[tokio::test(start_paused = true)]
async fn a_renewal_with_an_unknown_previous_expiration_records_a_gap() {
    let (control, _) = control(None, vec![response(Some(9_000_000))], "unknown-gap").await;
    tokio::time::advance(DEFAULT_RENEW_AFTER).await;
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Invalidated(_))));
    assert!(matches!(session.next().await, Some(E::Healthy)));
}
#[tokio::test(start_paused = true)]
async fn a_renewal_reply_lost_with_its_session_still_reaches_the_next_session() {
    let (control, script) = control(Some(0), vec![response(Some(9_000_000))], "lost-reply").await;
    tokio::time::advance(MIN_RENEW_DELAY).await;
    let (reply, response) = oneshot::channel();
    control.commands.send(WatchCommand::Renew { reply }).await.unwrap();
    drop(response);
    assert!(control.has_handles().await);
    assert_eq!(script.requests().len(), 2);
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Invalidated(_))));
    assert!(matches!(session.next().await, Some(E::Established)));
}
#[tokio::test(start_paused = true)]
async fn a_due_renewal_runs_at_session_start_after_a_failure() {
    let (control, script) = control(Some(0), vec![canned(StatusCode::SERVICE_UNAVAILABLE), response(Some(9_000_000))], "due-after-failure").await;
    tokio::time::advance(MIN_RENEW_DELAY).await;
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Failed(e)) if e.recovery().requires_reconciliation()));
    drop(session);
    let start = tokio::time::Instant::now();
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Invalidated(_))));
    assert_eq!(start, tokio::time::Instant::now());
    assert_eq!(script.requests().len(), 3);
}
#[tokio::test(start_paused = true)]
async fn renew_not_due_after_a_concurrent_subscribe_makes_no_wire_call() {
    let (control, script) = control(Some(0), vec![response(Some(9_000_000))], "not-due").await;
    tokio::time::advance(MIN_RENEW_DELAY).await;
    push_subscribe(Arc::clone(&control), vec![CursorScope::Account]).await.unwrap();
    assert!(matches!(renew(&control).await, Ok(RenewOutcome::NotDue)));
    assert_eq!(script.requests().len(), 2);
}
#[tokio::test(start_paused = true)]
async fn a_resubscribe_after_a_terminal_renewal_failure_reports_healthy() {
    let (control, script) = control(Some(0), vec![canned(StatusCode::FORBIDDEN), response(Some(9_000_000))], "fresh-after-terminal").await;
    fail_terminal(&control).await;
    let mut session = test_session(&control);
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    push_subscribe(Arc::clone(&control), vec![CursorScope::Account]).await.unwrap();
    assert!(matches!(next.await, Some(E::Invalidated(_))));
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert_eq!(script.requests().len(), 3);
}
#[tokio::test(start_paused = true)]
async fn a_watch_lapsed_after_a_terminal_failure_and_restored_by_subscribe_records_a_gap() {
    let (control, _) = control(Some(0), vec![canned(StatusCode::FORBIDDEN), response(Some(9_000_000))], "lapsed-subscribe").await;
    fail_terminal(&control).await;
    push_subscribe(Arc::clone(&control), vec![CursorScope::Account]).await.unwrap();
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Invalidated(_))));
}
#[tokio::test(start_paused = true)]
async fn a_resubscribe_before_the_old_watch_expired_records_no_gap() {
    let (control, _) = control(Some(2_000_000), vec![canned(StatusCode::FORBIDDEN), response(Some(9_000_000))], "unexpired-subscribe").await;
    fail_terminal(&control).await;
    push_subscribe(Arc::clone(&control), vec![CursorScope::Account]).await.unwrap();
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Healthy)));
}
#[tokio::test(start_paused = true)]
async fn a_first_subscribe_records_no_gap_and_no_fresh_watch() {
    let (control, script) = control(Some(9_000_000), vec![], "initial-silent").await;
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert!(!control.fresh.load(Ordering::Acquire));
    assert!(!control.gap.load(Ordering::Acquire));
    assert_eq!(script.requests().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn a_renewal_waits_for_the_push_gate() {
    let (control, script) = control(Some(0), vec![response(Some(9_000_000))], "gated-renewal").await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let permit = Arc::clone(&gate);
    let engine_gate = PushGate::new(move || { let permit = Arc::clone(&permit); Box::pin(async move { permit.acquire().await.unwrap().forget(); }) });
    let mut session = push_stream(Arc::clone(&control), bifrost_types::PushLane(0), SessionStop::never(), engine_gate);
    assert!(matches!(session.next().await, Some(E::Established)));
    tokio::time::advance(MIN_RENEW_DELAY).await;
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    for _ in 0..20 {
        assert!(futures::poll!(&mut next).is_pending());
        tokio::task::yield_now().await;
    }
    assert_eq!(script.requests().len(), 1);
    gate.add_permits(1);
    assert!(matches!(next.await, Some(E::Invalidated(_))));
    assert_eq!(script.requests().len(), 2);
}
#[tokio::test(start_paused = true)]
async fn push_lanes_follow_pubsub_configuration() {
    let (client, _) = scripted_client([]);
    for configured in [false, true] {
        let control = PubSubControl::new(Arc::clone(&client), configured.then(|| PubSubConfig::new("projects/p/topics/t")), CancellationToken::new(), "lane-configuration");
        assert_eq!(control.lanes().len(), usize::from(configured));
        if configured { assert_eq!(control.lanes()[0].backoff.initial(), RENEW_RETRY_AFTER); }
    }
}

async fn interrupted_renewal(stopped: bool) {
    let mailbox = if stopped { "stopped-renewal" } else { "dropped-renewal" };
    let (control, script) = control(Some(0), vec![response(Some(9_000_000))], mailbox).await;
    let owner = mailbox_watch(mailbox);
    let exchange = owner.exchange.lock().await;
    tokio::time::advance(MIN_RENEW_DELAY).await;
    let (stopper, stop) = SessionStop::pair();
    let mut session = push_stream(Arc::clone(&control), bifrost_types::PushLane(0), stop, PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    for _ in 0..20 { tokio::task::yield_now().await; }
    assert_eq!(script.requests().len(), 1);
    if stopped {
        stopper.stop();
        assert!(matches!(next.await, Some(E::Ended(End::Stopped))));
    } else { drop(next); drop(stopper); }
    drop(session);
    drop(exchange);
    assert!(control.has_handles().await);
    assert_eq!(script.requests().len(), 2);
    let mut next = test_session(&control);
    assert!(matches!(next.next().await, Some(E::Invalidated(_))));
}
#[tokio::test(start_paused = true)]
async fn a_stopped_session_does_not_cancel_an_in_flight_renewal() { interrupted_renewal(true).await; }
#[tokio::test(start_paused = true)]
async fn a_renewal_reply_lost_by_dropping_its_session_keeps_the_effect() { interrupted_renewal(false).await; }

/// A `NotWatched` reply leaves the schedule the session read unchanged, so the
/// session must wait for a newer one rather than re-send `Renew` against it.
/// `Watched` with no handles is that state; nothing but the test seam reaches
/// it. Reseeding the handle afterwards makes a re-sent `Renew` succeed on the
/// wire, so a session that resends is visible as a second request.
///
/// Ablation, session: removing the wait after a declined `Renew` lets it loop
/// straight back into `Renew`, which renews once the handle is back. Ablation,
/// actor: dropping the no-handles `NotWatched` check renews a watch nobody
/// holds, before the handle is reseeded.
#[tokio::test(start_paused = true)]
async fn a_declined_renewal_waits_for_a_newer_schedule_instead_of_resending() {
    let (control, script) = control(Some(0), vec![response(Some(9_000_000))], "declined-renewal").await;
    control.clear_handles().await;
    tokio::time::advance(MIN_RENEW_DELAY).await;
    let mut session = test_session(&control);
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    for _ in 0..20 {
        assert!(futures::poll!(&mut next).is_pending());
        tokio::task::yield_now().await;
    }
    control.insert_handle(&SubscriptionHandle("reseeded".into())).await;
    for _ in 0..20 {
        assert!(futures::poll!(&mut next).is_pending());
        tokio::task::yield_now().await;
    }
    assert_eq!(script.requests().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_queued_renewal_after_shutdown_makes_no_wire_call() {
    let (control, script) = control(Some(0), vec![response(Some(9_000_000))], "queued-after-close").await;
    tokio::time::advance(MIN_RENEW_DELAY).await;
    control.shutdown.cancel();
    assert!(matches!(renew(&control).await, Ok(RenewOutcome::NotWatched)));
    assert_eq!(script.requests().len(), 1);
}
