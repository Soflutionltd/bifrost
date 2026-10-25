// Direct pull-session checks complement subscription transaction tests.
fn pull_session<T: PushTransport>(transport: T, policy: ReconnectPolicy, lease: bifrost_types::LaneLease, stop: bifrost_types::SessionStop) -> AccountStream<bifrost_types::PushSessionEvent> {
    session(transport, Arc::new(Mutex::new(HashSet::new())), Arc::new(Mutex::new(None)), CancellationToken::new(), policy, Arc::new(routing()), lease, stop)
}
#[tokio::test]
async fn a_bad_ws_frame_close_fails_the_session_retryably() {
    use bifrost_types::PushSessionEvent as E;
    let transport = FramePushTransport { frames: StdMutex::new(Some(vec![Err(crate::Error::WebSocketClosed)])) };
    let mut session = pull_session(transport, ReconnectPolicy::default(), bifrost_types::LaneLease::new(), bifrost_types::SessionStop::never());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Failed(e)) if e.recovery().is_retryable()));
    assert!(session.next().await.is_none());
}
#[tokio::test(start_paused = true)]
async fn a_handshake_past_its_bound_ends_the_session_lost() {
    let transport = HangingTransport { hang: Hang::Handshake, entered: Arc::new(tokio::sync::Semaphore::new(0)) };
    let mut session = pull_session(transport, ReconnectPolicy::default(), bifrost_types::LaneLease::new(), bifrost_types::SessionStop::never());
    assert!(matches!(session.next().await, Some(bifrost_types::PushSessionEvent::Ended(bifrost_types::PushSessionEnd::Lost))));
    assert!(session.next().await.is_none());
}
#[tokio::test]
async fn a_session_that_reads_no_traffic_never_reports_healthy() {
    use bifrost_types::PushSessionEvent as E;
    let transport = RejectingTransport { connects: Arc::new(StdMutex::new(Vec::new())) };
    let mut session = pull_session(transport, ReconnectPolicy::default(), bifrost_types::LaneLease::new(), bifrost_types::SessionStop::never());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Failed(_))));
    assert!(session.next().await.is_none());
}
#[tokio::test]
async fn a_second_session_on_the_lane_fails_the_lease() {
    let lease = bifrost_types::LaneLease::new();
    let silent = || SilentTransport { pings: Arc::new(std::sync::atomic::AtomicUsize::new(0)) };
    let mut first = pull_session(silent(), ReconnectPolicy::default(), lease.clone(), bifrost_types::SessionStop::never());
    assert!(matches!(first.next().await, Some(bifrost_types::PushSessionEvent::Established)));
    let mut second = pull_session(silent(), ReconnectPolicy::default(), lease.clone(), bifrost_types::SessionStop::never());
    assert!(matches!(second.next().await, Some(bifrost_types::PushSessionEvent::Failed(e)) if matches!(e.kind(), bifrost_types::AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract))));
    drop(first);
    let mut third = pull_session(silent(), ReconnectPolicy::default(), lease, bifrost_types::SessionStop::never());
    assert!(matches!(third.next().await, Some(bifrost_types::PushSessionEvent::Established)));
}
#[tokio::test]
async fn a_stop_during_each_websocket_setup_phase_ends_the_session() {
    for hang in [Hang::Handshake, Hang::Reenable] {
        let (stopper, stop) = bifrost_types::SessionStop::pair();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let transport = HangingTransport { hang, entered: Arc::clone(&entered) };
        let mut session = pull_session(transport, ReconnectPolicy::default(), bifrost_types::LaneLease::new(), stop);
        let mut next = Box::pin(session.next());
        assert!(futures::poll!(&mut next).is_pending());
        entered.acquire().await.unwrap().forget();
        stopper.stop();
        assert!(matches!(next.await, Some(bifrost_types::PushSessionEvent::Ended(bifrost_types::PushSessionEnd::Stopped))));
    }
}
