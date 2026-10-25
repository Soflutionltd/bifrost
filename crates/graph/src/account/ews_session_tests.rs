// Session transcripts, entirely in memory.
use bifrost_types::{PushSessionEvent as E, PushSessionEnd as End, SessionStop};
use std::sync::Arc;

async fn setup(steps: Vec<ScriptStep>) -> (GraphAccount, bifrost_types::AccountStream<E>, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let account = GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
    register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
    let (transport, requests) = scripted(steps);
    let session = ews_session(account.clone(), Arc::new(transport), SessionStop::never());
    (account, session, requests)
}

#[tokio::test]
async fn an_uncertain_subscribe_is_forwarded_for_reconciliation() {
    let (_, mut session, mut requests) = setup(vec![ScriptStep::Fail(EwsError::Transport(bifrost_net::Error::Network { message: "lost".into(), transmission_state: bifrost_types::TransmissionState::InFlight, source: None }))]).await;
    assert!(matches!(session.next().await, Some(E::Failed(e)) if e.recovery().requires_reconciliation()));
    assert!(session.next().await.is_none());
    assert!(requests.recv().await.unwrap().contains("Subscribe"));
    assert!(requests.try_recv().is_err());
}
#[tokio::test]
async fn an_unreplayed_subscribe_status_fails_the_session_without_resubscribing() {
    let (_, mut session, mut requests) = setup(vec![ScriptStep::Fail(unreplayed_unavailable())]).await;
    assert!(matches!(session.next().await, Some(E::Failed(e)) if e.recovery().requires_reconciliation()));
    assert!(session.next().await.is_none());
    assert!(requests.recv().await.is_some());
    assert!(requests.try_recv().is_err());
}
#[tokio::test]
async fn a_complete_long_poll_reports_healthy() {
    let (_, mut session, _) = setup(vec![ScriptStep::Respond(subscribe_response_xml("sub-1")), ScriptStep::Respond("<m:GetStreamingEventsResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode></m:GetStreamingEventsResponseMessage>".into()), ScriptStep::Hang]).await;
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Healthy)));
}
#[tokio::test]
async fn a_topology_change_hands_off_the_stream_without_a_false_disconnect() {
    let (account, mut session, mut requests) = setup(vec![ScriptStep::Respond(subscribe_response_xml("sub-1")), ScriptStep::Hang, ScriptStep::Respond(unsubscribe_response_xml())]).await;
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    register(&account, "h2", vec![ews_scope("rest-b", "folder-2")]).await;
    assert!(matches!(next.await, Some(E::Ended(End::Reconfigure))));
    let bodies: Vec<_> = std::iter::from_fn(|| requests.try_recv().ok()).collect();
    assert_eq!(bodies.len(), 3);
    assert!(bodies[2].contains("<m:Unsubscribe>"));
    assert!(session.next().await.is_none());
}
#[tokio::test]
async fn the_first_registration_subscribes_exactly_once() {
    let (_, mut session, mut requests) = setup(vec![ScriptStep::Respond(subscribe_response_xml("sub-1")), ScriptStep::Hang]).await;
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert!(requests.recv().await.unwrap().contains("<m:Subscribe>"));
    assert!(requests.recv().await.unwrap().contains("GetStreamingEvents"));
    assert!(requests.try_recv().is_err());
}
#[tokio::test]
async fn every_classified_stream_failure_ends_the_session_failed() {
    for code in [crate::ews::SoapFaultCode::ErrorAccessDenied, crate::ews::SoapFaultCode::ErrorStreamingSubscriptionInvalid] {
        let (_, mut session, mut requests) = setup(vec![ScriptStep::Respond(subscribe_response_xml("sub-1")), ScriptStep::Fail(EwsError::SoapFault { code, detail: DiagnosticText::support_only("fault") }), ScriptStep::Respond(unsubscribe_response_xml())]).await;
        assert!(matches!(session.next().await, Some(E::Established)));
        assert!(matches!(session.next().await, Some(E::Failed(_))));
        assert!(session.next().await.is_none());
        let bodies: Vec<_> = std::iter::from_fn(|| requests.try_recv().ok()).collect();
        assert_eq!(bodies.len(), 3);
        assert!(bodies[2].contains("Unsubscribe"));
    }
}
/// End-to-end dispatch: a streamed notification for a subscribed folder
/// comes back as `Invalidated` hints naming that folder's registered scope,
/// one per event, and the session then reports the complete poll healthy.
#[tokio::test]
async fn a_streamed_notification_invalidates_the_folder_scope() {
    let (_, mut session, mut requests) = setup(vec![
        ScriptStep::Respond(subscribe_response_xml("sub-1")),
        ScriptStep::Respond(NOTIFICATION_XML.to_string()),
        ScriptStep::Hang,
    ])
    .await;
    assert!(matches!(session.next().await, Some(E::Established)));
    // The fixture carries two events on folder-1; each maps to the one
    // registered scope.
    for _ in 0..2 {
        match session.next().await {
            Some(E::Invalidated(hint)) => {
                assert!(matches!(hint.source, PushSource::EwsStreaming));
                assert!(
                    matches!(&hint.payload, HintPayload::SpecificCursorScope(scope) if *scope == email_scope("rest-a")),
                    "expected the registered scope, got {:?}",
                    hint.payload
                );
            }
            other => panic!("expected Invalidated, got {other:?}"),
        }
    }
    assert!(matches!(session.next().await, Some(E::Healthy)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    let bodies: Vec<_> = std::iter::from_fn(|| requests.try_recv().ok()).collect();
    assert_eq!(bodies.len(), 3, "subscribe, the poll, the re-poll");
    assert!(bodies[2].contains("GetStreamingEvents"));
}

/// The same dispatch, over a response that binds the EWS namespaces to
/// prefixes other than `m:`/`t:`. The framing keys on local names, so this
/// is an ordinary notification - not a silently discarded one that ends the
/// poll `Healthy` with nothing invalidated.
#[tokio::test]
async fn a_notification_under_an_alien_namespace_prefix_still_invalidates() {
    const ALIEN_PREFIX_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<x:Envelope xmlns:x="http://schemas.xmlsoap.org/soap/envelope/">
  <x:Body>
    <a:GetStreamingEventsResponse xmlns:a="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:b="http://schemas.microsoft.com/exchange/services/2006/types">
      <a:ResponseMessages>
        <a:GetStreamingEventsResponseMessage ResponseClass="Success">
          <a:Notifications>
            <b:Notification>
              <b:SubscriptionId>sub-1</b:SubscriptionId>
              <b:NewMailEvent>
                <b:ItemId Id="item-1"/>
                <b:ParentFolderId Id="folder-1"/>
              </b:NewMailEvent>
            </b:Notification>
          </a:Notifications>
        </a:GetStreamingEventsResponseMessage>
      </a:ResponseMessages>
    </a:GetStreamingEventsResponse>
  </x:Body>
</x:Envelope>"#;
    let (_, mut session, _) = setup(vec![
        ScriptStep::Respond(subscribe_response_xml("sub-1")),
        ScriptStep::Respond(ALIEN_PREFIX_XML.to_string()),
        ScriptStep::Hang,
    ])
    .await;
    assert!(matches!(session.next().await, Some(E::Established)));
    match session.next().await {
        Some(E::Invalidated(hint)) => assert!(
            matches!(&hint.payload, HintPayload::SpecificCursorScope(scope) if *scope == email_scope("rest-a")),
            "expected the registered scope, got {:?}",
            hint.payload
        ),
        other => panic!("expected Invalidated, got {other:?}"),
    }
    assert!(matches!(session.next().await, Some(E::Healthy)));
}
#[tokio::test]
async fn a_session_with_no_registrations_parks() {
    let account = GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
    let (transport, mut requests) = scripted(vec![]);
    let (stopper, stop) = SessionStop::pair();
    let mut session = ews_session(account, Arc::new(transport), stop);
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert!(requests.try_recv().is_err());
    stopper.stop();
    assert!(matches!(next.await, Some(E::Ended(End::Stopped))));
}

#[tokio::test]
async fn ews_release_is_claimed_once_and_joined_by_every_caller() {
    let account = GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Notify::new());
    let count = Arc::clone(&calls); let ready = Arc::clone(&gate);
    use futures::FutureExt;
    let releaser: EwsReleaser = Arc::new(move |_| {
        let count = Arc::clone(&count); let ready = Arc::clone(&ready);
        async move { count.fetch_add(1, std::sync::atomic::Ordering::SeqCst); ready.notified().await; Ok(()) }.boxed()
    });
    account.ews_ledger.lock().unwrap().entries.insert("id".into(), EwsEntry { state: EwsEntryState::Live, releaser });
    let first = release(&account, "id"); let second = release(&account, "id");
    tokio::task::yield_now().await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(first.clone().now_or_never().is_none());
    gate.notify_one();
    futures::future::join(first, second).await;
    assert!(account.ews_ledger.lock().unwrap().entries.is_empty());
}

#[tokio::test]
async fn a_failed_ews_release_keeps_its_obligation_without_hiding_a_newer_subscription() {
    use futures::FutureExt;
    let account = GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
    let fail: EwsReleaser = Arc::new(|_| async { Err(EwsError::Internal("failed cleanup".into())) }.boxed());
    let good: EwsReleaser = Arc::new(|_| async { Ok(()) }.boxed());
    {
        let mut ledger = account.ews_ledger.lock().unwrap();
        ledger.entries.insert("old".into(), EwsEntry { state: EwsEntryState::Live, releaser: fail });
        ledger.entries.insert("new".into(), EwsEntry { state: EwsEntryState::Live, releaser: good });
    }
    release(&account, "old").await;
    let ledger = account.ews_ledger.lock().unwrap();
    assert!(matches!(ledger.entries["old"].state, EwsEntryState::Orphaned));
    assert!(matches!(ledger.entries["new"].state, EwsEntryState::Live));
}

#[tokio::test]
async fn a_forced_session_drop_leaves_the_subscription_for_close_to_release() {
    use bifrost_types::Account;
    let (account, mut session, mut requests) = setup(vec![ScriptStep::Respond(subscribe_response_xml("sub-1")), ScriptStep::Respond(unsubscribe_response_xml())]).await;
    assert!(matches!(session.next().await, Some(E::Established)));
    drop(session);
    assert!(matches!(account.ews_ledger.lock().unwrap().entries["sub-1"].state, EwsEntryState::Orphaned));
    account.close().await.unwrap();
    assert!(account.ews_ledger.lock().unwrap().entries.is_empty());
    let bodies: Vec<_> = std::iter::from_fn(|| requests.try_recv().ok()).collect();
    assert_eq!(bodies.len(), 2);
    assert!(bodies[1].contains("Unsubscribe"));
}

#[tokio::test]
async fn a_session_start_releases_orphaned_subscriptions() {
    use futures::FutureExt;
    let account = GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
    let releaser: EwsReleaser = Arc::new(|_| async { Ok(()) }.boxed());
    account.ews_ledger.lock().unwrap().entries.insert("old".into(), EwsEntry { state: EwsEntryState::Orphaned, releaser });
    let (transport, mut requests) = scripted(vec![]);
    let mut session = ews_session(account.clone(), Arc::new(transport), SessionStop::never());
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    for _ in 0..20 { tokio::task::yield_now().await; }
    assert!(account.ews_ledger.lock().unwrap().entries.is_empty());
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn a_subscribe_answered_after_close_is_released_not_recorded() {
    use bifrost_types::Account;
    let account = GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
    register(&account, "h", vec![ews_scope("rest", "ews")]).await;
    let ready = Arc::new(tokio::sync::Notify::new());
    let (transport, mut requests) = scripted(vec![ScriptStep::Delayed(Arc::clone(&ready), subscribe_response_xml("late")), ScriptStep::Respond(unsubscribe_response_xml())]);
    let mut session = ews_session(account.clone(), Arc::new(transport), SessionStop::never());
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert!(requests.recv().await.unwrap().contains("Subscribe"));
    account.close().await.unwrap();
    ready.notify_one();
    assert!(matches!(next.await, Some(E::Ended(End::Stopped))));
    assert!(account.ews_ledger.lock().unwrap().entries.is_empty());
    assert!(requests.recv().await.unwrap().contains("Unsubscribe"));
}

#[tokio::test]
async fn a_terminal_response_error_in_the_stream_terminates_instead_of_reconnecting() {
    let frame = r#"<m:GetStreamingEventsResponseMessage ResponseClass="Error"><m:MessageText>denied</m:MessageText><m:ResponseCode>ErrorAccessDenied</m:ResponseCode></m:GetStreamingEventsResponseMessage>"#;
    let (_, mut session, mut requests) = setup(vec![ScriptStep::Respond(subscribe_response_xml("sub-1")), ScriptStep::Respond(frame.into()), ScriptStep::Respond(unsubscribe_response_xml())]).await;
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Failed(e)) if e.recovery().is_terminal()));
    assert!(session.next().await.is_none());
    let bodies: Vec<_> = std::iter::from_fn(|| requests.try_recv().ok()).collect();
    assert_eq!(bodies.len(), 3);
    assert!(bodies[2].contains("Unsubscribe"));
}
