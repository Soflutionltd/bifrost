use super::test_support::*;

/// A non-NOTIFY server gets one dedicated IDLE session per pushed folder,
/// carved out of `pool_cap`, so the third folder at the default budget of
/// two cannot be pushed. It must be refused in the failed lane rather than
/// silently accepted: bifrost-sync records only the succeeded scopes as
/// pushed and keeps the rest polling, so a silent acceptance would make that
/// folder invisible - neither pushed nor polled.
///
/// Pinned at the exact boundary in both directions. The second folder (item
/// "1") must succeed and the third (item "2") must fail; an off-by-one in
/// either direction moves one of those two. `pool_cap = 4` leaves headroom
/// above the budget, so the budget, not the headroom clamp, is what binds.
#[tokio::test]
async fn non_notify_push_reports_the_folder_beyond_its_budget_as_poll_only() {
    use bifrost_types::{Account, CursorScope, FolderId};

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let account = scripted_account(conn, 4);
    assert_eq!(
        account.config.idle_connection_budget, 2,
        "this transcript is written against the default budget",
    );
    let scopes: Vec<_> = ["A", "B", "C"]
        .into_iter()
        .map(|name| CursorScope::Folder(FolderId(name.to_owned())))
        .collect();
    let result = account
        .push_subscribe(&scopes)
        .await
        .expect("a refused scope is a failed item, never a whole-request Err");
    let succeeded: Vec<_> = result
        .outcomes()
        .succeeded()
        .iter()
        .map(|item| item.item.0)
        .collect();
    assert_eq!(
        succeeded,
        vec![0, 1],
        "every folder up to and including the budget is pushed",
    );
    assert_eq!(result.outcomes().failed().len(), 1);
    assert_eq!(
        result.outcomes().failed()[0].item.0,
        2,
        "the first folder past the budget, and only it, is rejected",
    );
    assert!(
        result.outcomes().failed()[0]
            .error
            .user_safe_text()
            .any(|text| text.contains("budget")),
        "a capacity refusal must not read as an unsupported scope shape",
    );
    assert!(
        result.handle().is_some(),
        "accepted scopes retain one subscription handle"
    );
    // Workers are capacity-many, not folder-many: the pool is started once
    // and idle slots park until a folder is admitted to them. The point of
    // the assertion is that a non-NOTIFY account runs its whole push
    // capacity rather than the single worker a NOTIFY account needs.
    assert_eq!(
        account.push_lanes().len(),
        2,
        "a non-NOTIFY account runs one IDLE worker per push session"
    );
    drop(server);
    account.close().await.unwrap();
}

/// A composed DAV collection scope is a syntactically valid mailbox name but
/// names no mailbox. Push admission must refuse it per-item: admitting it
/// would report the collection as pushed (a misreport bifrost-sync trusts,
/// while the DAV sub-accounts have no push lane at all) and burn an IDLE
/// budget slot on a folder no worker can ever SELECT, displacing a real
/// mailbox later in the same request.
#[tokio::test]
async fn a_composed_dav_collection_scope_is_refused_by_push_admission() {
    use bifrost_types::{Account, CursorScope, FolderId};

    let collection = "https://dav.example.test/books/work/";
    let (dav_scopes, warnings) = crate::account::DavScopeIndex::build(
        vec![FolderId(collection.to_owned())],
        Vec::new(),
        &std::collections::HashSet::new(),
    );
    assert!(warnings.is_empty());
    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let account = scripted_dav_account(conn, 2, false, None, FolderRegistry::default(), dav_scopes);
    let scopes = vec![
        CursorScope::Folder(FolderId(collection.to_owned())),
        CursorScope::Folder(FolderId("INBOX".to_owned())),
    ];
    let result = account
        .push_subscribe(&scopes)
        .await
        .expect("a refused scope is a failed item, never a whole-request Err");
    let succeeded: Vec<_> = result
        .outcomes()
        .succeeded()
        .iter()
        .map(|item| item.item.0)
        .collect();
    assert_eq!(
        succeeded,
        vec![1],
        "the DAV collection must not be reported as pushed, and must not block the mail sibling",
    );
    assert_eq!(result.outcomes().failed().len(), 1);
    assert_eq!(result.outcomes().failed()[0].item.0, 0);
    drop(server);
    account.close().await.unwrap();
}

/// Push never takes the last session, so at `pool_cap = 1` every scope lands
/// in the failed lane, with or without NOTIFY, and no worker starts.
#[tokio::test]
async fn push_is_refused_entirely_when_pool_cap_leaves_no_room() {
    use bifrost_types::Account as _;
    for notify in [false, true] {
        let (account, peer, dials) = session_account(1, 0, notify).await;
        let result = account
            .push_subscribe(&[push_folder("INBOX"), push_folder("Archive")])
            .await
            .unwrap();
        assert_eq!(result.outcomes().failed().len(), 2);
        assert!(result.outcomes().succeeded().is_empty());
        assert!(result.handle().is_none());
        assert!(account.push_lanes().is_empty());
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(peer);
        account.close().await.unwrap();
    }
}

/// NOTIFY folds every folder onto one session, so the smallest cap with
/// push room admits any number of folders on a single lane.
#[tokio::test]
async fn a_notify_account_admits_every_folder_on_one_lane() {
    use bifrost_types::Account as _;
    let (account, peer, _) = session_account(2, 0, true).await;
    let scopes: Vec<_> = ["A", "B", "C", "D", "E"]
        .into_iter()
        .map(push_folder)
        .collect();
    let result = account.push_subscribe(&scopes).await.unwrap();
    assert_eq!(result.outcomes().succeeded().len(), 5);
    assert!(result.outcomes().failed().is_empty());
    assert_eq!(account.push_lanes().len(), 1);
    drop(peer);
    account.close().await.unwrap();
}

/// A live IDLE session holds a session permit and a push slot: with the
/// primed session checked out, a second data checkout waits instead of
/// dialing a third session.
#[tokio::test(start_paused = true)]
async fn an_idle_worker_session_counts_against_pool_cap() {
    use bifrost_types::Account as _;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let driver = drive_push_until_shutdown(&account);
    let mut push_peer = dials.servers.recv().await.unwrap();
    let auth = read_line(&mut push_peer).await;
    session_auth(&mut push_peer, &auth).await;
    session_select(&mut push_peer, "INBOX").await;
    session_idle(&mut push_peer).await;
    assert_eq!(account.pool.available_sessions(), 0);
    assert_eq!(account.push.available_push_slots(), 0);
    let _held = account.pool.checkout_any().await.unwrap();
    let mut second = Box::pin(account.pool.checkout_any());
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(second);
    drop(push_peer);
    drop(peer);
    account.close().await.unwrap();
    driver.await.unwrap();
}

/// A generation bump while the worker authenticates must not cancel the
/// dial: the one-greeting connector refuses a second call. Leg 1 bumps with
/// an accepted same-folder subscribe; leg 2 moves the assignment, which the
/// post-dial re-choice picks up before SELECT.
#[tokio::test(start_paused = true)]
async fn a_subscription_change_mid_dial_keeps_the_pending_session() {
    use bifrost_types::Account as _;
    for reassign in [false, true] {
        let (account, peer, mut dials) = session_account(2, 1, false).await;
        let first = account
            .push_subscribe(&[push_folder("INBOX")])
            .await
            .unwrap()
            .handle()
            .cloned()
            .unwrap();
        let driver = drive_push_until_shutdown(&account);
        let mut push_peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut push_peer).await;
        if reassign {
            account.push_unsubscribe(first).await.unwrap();
        }
        let selected = if reassign { "Archive" } else { "INBOX" };
        account
            .push_subscribe(&[push_folder(selected)])
            .await
            .unwrap();
        session_auth(&mut push_peer, &auth).await;
        session_select(&mut push_peer, selected).await;
        session_idle(&mut push_peer).await;
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(push_peer);
        drop(peer);
        account.close().await.unwrap();
        driver.await.unwrap();
    }
}

/// Both sessions are held by data checkouts when the worker asks, so it
/// waits; once covering, it invalidates the folder for the stretched window.
#[tokio::test(start_paused = true)]
async fn a_push_session_that_waited_for_capacity_invalidates_the_folder_it_starts_covering() {
    use bifrost_types::Account as _;
    use futures::StreamExt as _;
    let (account, peer, mut dials) = session_account(2, 2, false).await;
    let mut events = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    let _held = account.pool.checkout_any().await.unwrap();
    let script = async {
        let mut peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut peer).await;
        session_auth(&mut peer, &auth).await;
        peer
    };
    let (second, mut data_peer) = tokio::join!(account.pool.checkout_any(), script);
    let second = second.unwrap();
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    drop(second);
    let script = async {
        let logout = read_line(&mut data_peer).await;
        assert!(logout.contains("LOGOUT"));
        respond(
            &mut data_peer,
            &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
        )
        .await;
        let mut push_peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut push_peer).await;
        session_auth(&mut push_peer, &auth).await;
        session_select(&mut push_peer, "INBOX").await;
        push_peer
    };
    let (event, push_peer) = tokio::join!(events.next(), script);
    assert!(
        matches!(event, Some(bifrost_types::PushSessionEvent::Invalidated(bifrost_types::InvalidationHint { payload: bifrost_types::HintPayload::SpecificCursorScope(scope), .. })) if scope == push_folder("INBOX"))
    );
    drop(push_peer);
    drop(peer);
    account.close().await.unwrap();
}

/// Capacity available at once: no coverage invalidation, which keeps the
/// `register_notify` no-snapshot decision for the common case.
#[tokio::test(start_paused = true)]
async fn a_push_session_dialed_at_once_publishes_no_coverage_invalidation() {
    use bifrost_types::Account as _;
    use futures::StreamExt as _;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    let mut events = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let script = async {
        let mut push_peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut push_peer).await;
        session_auth(&mut push_peer, &auth).await;
        session_select(&mut push_peer, "INBOX").await;
        push_peer
    };
    let (event, push_peer) = tokio::join!(events.next(), script);
    assert!(matches!(
        event,
        Some(bifrost_types::PushSessionEvent::Established)
    ));
    drop(push_peer);
    assert!(
        matches!(events.next().await, Some(bifrost_types::PushSessionEvent::Failed(e)) if e.recovery().is_retryable())
    );
    drop(peer);
    account.close().await.unwrap();
}

/// The refusing connector stands in for an unreachable host, so a push
/// session whose dial it refuses must end `Failed` with a retryable class
/// and never redial on its own: the engine owns the backoff and the next
/// session. Every push admission test built on `scripted_account_parts`
/// relies on this.
#[tokio::test(start_paused = true)]
async fn a_refused_push_dial_fails_the_session_without_redialing() {
    use bifrost_types::Account as _;
    use futures::StreamExt as _;
    let (account, peer, dials) = session_account(2, 0, false).await;
    let mut events = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    assert!(matches!(
        events.next().await,
        Some(bifrost_types::PushSessionEvent::Failed(e)) if e.recovery().is_retryable()
    ));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(
        dials.calls.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "the session must not redial"
    );
    drop(peer);
    account.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_nonexistent_selected_folder_is_excluded_and_the_session_reconfigures() {
    use bifrost_types::{
        Account, PushGate, PushLane, PushSessionEnd as End, PushSessionEvent as E, SessionStop,
    };
    use futures::StreamExt;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("gone")])
        .await
        .unwrap();
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let script = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("{} NO [NONEXISTENT] gone\r\n", tag_of(&select)),
        )
        .await;
        server
    };
    let (event, server) = tokio::join!(session.next(), script);
    assert!(matches!(event, Some(E::UnitFailed(_))));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Reconfigure))
    ));
    drop(session);
    let mut parked = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let mut next = Box::pin(parked.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(next);
    drop(parked);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_second_session_on_a_driven_lane_fails_the_lease() {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    use futures::StreamExt;
    let (account, peer, _) = session_account(2, 0, false).await;
    let mut first = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let mut next = Box::pin(first.next());
    assert!(futures::poll!(&mut next).is_pending());
    let mut second = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    assert!(
        matches!(second.next().await, Some(E::Failed(e)) if matches!(e.kind(), bifrost_types::AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract)))
    );
    drop(next);
    drop(first);
    let mut third = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let mut next = Box::pin(third.next());
    assert!(futures::poll!(&mut next).is_pending());
    drop(next);
    drop(third);
    drop(peer);
    account.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_completed_idle_round_reports_healthy() {
    completed_push_round_and_stop(true).await;
}

#[tokio::test(start_paused = true)]
async fn a_stopped_session_logs_out_before_ending() {
    completed_push_round_and_stop(false).await;
}

/// A BYE whose code classifies terminal ends the session `Failed` without
/// excluding the selected folder. The BYE arrives before the `+` grant, so the
/// driver's grant wait raises it as `Error::Bye` and the session sees it
/// through `idle()`'s error arm - the only way a BYE reaches the live session.
/// The defensive `TypedEvent::Bye` arm is pinned by a unit test in `push.rs`.
#[tokio::test(start_paused = true)]
async fn a_bye_never_excludes_the_selected_folder() {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    use futures::StreamExt;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        session_select(&mut server, "INBOX").await;
        server
    };
    let (event, mut server) = tokio::join!(session.next(), setup);
    assert!(matches!(event, Some(E::Established)));
    let script = async {
        let idle = read_line(&mut server).await;
        assert!(idle.contains("IDLE"));
        respond(&mut server, "* BYE [NOPERM] denied\r\n").await;
    };
    let (event, ()) = tokio::join!(session.next(), script);
    assert!(matches!(event, Some(E::Failed(e)) if e.recovery().is_terminal()));
    drop(session);
    drop(server);
    // The next dial is refused by the connector. An excluded assignment would
    // park instead of attempting it, so this also pins the session's wiring.
    let mut next = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let event = tokio::time::timeout(Duration::from_secs(30), next.next())
        .await
        .unwrap();
    assert!(matches!(event, Some(E::Failed(e)) if e.recovery().is_retryable()));
    drop(next);
    drop(peer);
    account.close().await.unwrap();
}

/// A server BYE and a server-initiated IDLE termination both leave the push
/// connection unusable, so the live session ends on the round that saw them
/// instead of reporting the round healthy and re-issuing IDLE on a dead
/// socket. They end differently: the BYE is classified like any other
/// connection failure (a plain BYE is `Server(Unavailable)`) and ends
/// `Failed`, while a tagged completion the client never asked for carries no
/// error and ends `Lost`. Either way the session releases its push slot.
///
/// The driver short-circuits an untagged BYE read during IDLE into
/// `Error::Bye` before emitting any event, so on the wire the BYE reaches the
/// session through `idle()`'s error arm, not as `TypedEvent::Bye`. The test
/// pins the outcome on that live path.
#[tokio::test(start_paused = true)]
async fn bye_and_server_termination_close_the_connection() {
    use bifrost_types::{
        Account, AccountErrorKind, PushGate, PushLane, PushSessionEnd as End,
        PushSessionEvent as E, ServerErrorKind, SessionStop,
    };
    use futures::StreamExt;
    for bye in [true, false] {
        let (account, peer, mut dials) = session_account(2, 1, false).await;
        account
            .push_subscribe(&[push_folder("INBOX")])
            .await
            .unwrap();
        let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
        let setup = async {
            let mut server = dials.servers.recv().await.unwrap();
            let auth = read_line(&mut server).await;
            session_auth(&mut server, &auth).await;
            session_select(&mut server, "INBOX").await;
            server
        };
        let (event, mut server) = tokio::join!(session.next(), setup);
        assert!(matches!(event, Some(E::Established)));
        assert_eq!(account.push.available_push_slots(), 0);
        let script = async {
            let idle = read_line(&mut server).await;
            assert!(idle.contains("IDLE"), "{idle}");
            respond(&mut server, "+ idling\r\n").await;
            if bye {
                respond(&mut server, "* BYE logging out\r\n").await;
            } else {
                respond(
                    &mut server,
                    &format!("{} OK IDLE ended by server\r\n", tag_of(&idle)),
                )
                .await;
            }
        };
        let (event, ()) = tokio::join!(session.next(), script);
        if bye {
            assert!(
                matches!(&event, Some(E::Failed(e)) if matches!(e.kind(), AccountErrorKind::Server(ServerErrorKind::Unavailable))),
                "a BYE ends the session Failed with its classification, got {event:?}"
            );
        } else {
            assert!(
                matches!(event, Some(E::Ended(End::Lost))),
                "a server-terminated IDLE ends the session Lost, got {event:?}"
            );
        }
        assert!(session.next().await.is_none(), "the session has ended");
        drop(session);
        // The slot is held until the dropped connection's driver exits, which
        // may tail a LOGOUT nobody answers; paused time lets that run out.
        let mut released = false;
        for _ in 0..600 {
            if account.push.available_push_slots() == 1 {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(
            released,
            "the ended session released its push connection (bye = {bye})"
        );
        drop(server);
        drop(peer);
        account.close().await.unwrap();
    }
}

/// A non-NOTIFY account gives every accepted folder its own worker, so a
/// session watching one of two folders goes straight to IDLE with no NOTIFY
/// SET and no warning that the other folder goes unpushed: it has a worker of
/// its own.
#[tokio::test(start_paused = true)]
async fn a_non_notify_worker_does_not_warn_about_folders_other_workers_cover() {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    let logs = CapturedLogs::default();
    let _logs = logs.install();
    let (account, peer, mut dials) = session_account_with_budget(3, 1, false, 2).await;
    let result = account
        .push_subscribe(&[push_folder("INBOX"), push_folder("Archive")])
        .await
        .unwrap();
    assert_eq!(result.outcomes().succeeded().len(), 2);
    assert_eq!(account.push_lanes().len(), 2);
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        session_select(&mut server, "Archive").await;
        server
    };
    let (events, mut server) = tokio::join!(
        next_events_until(&mut session, |e| matches!(e, E::Established)),
        setup
    );
    assert!(matches!(events.as_slice(), [E::Established]), "{events:?}");
    let script = async {
        let idle = read_line(&mut server).await;
        assert!(idle.contains("IDLE"), "no NOTIFY SET before IDLE: {idle}");
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 2 EXISTS\r\n").await;
        let done = read_line(&mut server).await;
        assert!(done.contains("DONE"), "{done}");
        respond(&mut server, &format!("{} OK idle done\r\n", tag_of(&idle))).await;
    };
    let (events, ()) = tokio::join!(
        next_events_until(&mut session, |e| matches!(e, E::Healthy)),
        script
    );
    assert!(matches!(events.as_slice(), [E::Healthy]), "{events:?}");
    let text = logs.text();
    assert!(
        !text.contains("NOTIFY"),
        "unexpected NOTIFY warning: {text}"
    );
    drop(session);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}

/// A subscription change that leaves this slot's mailbox unchanged ends the
/// IDLE round with DONE and re-issues IDLE on the same connection: no LOGOUT,
/// no second dial, no coarse invalidation.
#[tokio::test(start_paused = true)]
async fn a_same_folder_resubscribe_re_idles_on_the_same_connection() {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        session_select(&mut server, "INBOX").await;
        server
    };
    let (events, mut server) = tokio::join!(
        next_events_until(&mut session, |e| matches!(e, E::Established)),
        setup
    );
    assert!(matches!(events.as_slice(), [E::Established]), "{events:?}");
    let script = async {
        let idle = read_line(&mut server).await;
        assert!(idle.contains("IDLE"), "{idle}");
        respond(&mut server, "+ idling\r\n").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        account
            .push_subscribe(&[push_folder("INBOX")])
            .await
            .unwrap();
        let done = read_line(&mut server).await;
        assert!(done.contains("DONE"), "{done}");
        respond(&mut server, &format!("{} OK idle done\r\n", tag_of(&idle))).await;
        let again = read_line(&mut server).await;
        assert!(again.contains("IDLE"), "re-IDLE, not a redial: {again}");
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 2 EXISTS\r\n").await;
        let done = read_line(&mut server).await;
        assert!(done.contains("DONE"), "{done}");
        respond(&mut server, &format!("{} OK idle done\r\n", tag_of(&again))).await;
    };
    let (events, ()) = tokio::join!(
        next_events_until(&mut session, |e| matches!(e, E::Healthy)),
        script
    );
    assert!(matches!(events.as_slice(), [E::Healthy]), "{events:?}");
    assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(session);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}

/// On a NOTIFY account, a `NOTIFY SET` the server refuses collapses coverage
/// to the selected mailbox while the subscribe outcome already reported every
/// folder pushed. Every round whose registration did not take, the fresh one
/// and a re-IDLE after a subscription change alike, must carry a coarse
/// invalidation; a round whose registration took carries none.
#[tokio::test(start_paused = true)]
async fn a_refused_notify_registration_invalidates_on_fresh_and_interrupted_rounds() {
    use bifrost_types::{
        Account, HintPayload, InvalidationHint, PushGate, PushLane, PushSessionEvent as E,
        SessionStop,
    };
    fn coarse(e: &E) -> bool {
        matches!(
            e,
            E::Invalidated(InvalidationHint {
                payload: HintPayload::Unknown,
                ..
            })
        )
    }
    for refuse in [true, false] {
        let (account, peer, mut dials) = session_account(2, 1, true).await;
        account
            .push_subscribe(&[push_folder("INBOX"), push_folder("Archive")])
            .await
            .unwrap();
        let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
        let answer = |tag: &str| {
            if refuse {
                format!("{tag} NO unsupported filter\r\n")
            } else {
                format!("{tag} OK registered\r\n")
            }
        };
        let setup = async {
            let mut server = dials.servers.recv().await.unwrap();
            let auth = read_line(&mut server).await;
            session_auth_with(&mut server, &auth, "IMAP4rev1 IDLE UNSELECT NOTIFY").await;
            session_select(&mut server, "Archive").await;
            let notify = read_line(&mut server).await;
            assert!(notify.contains("NOTIFY SET"), "{notify}");
            assert!(notify.contains("INBOX"), "{notify}");
            respond(&mut server, &answer(tag_of(&notify))).await;
            server
        };
        let (fresh, mut server) = tokio::join!(
            next_events_until(&mut session, |e| matches!(e, E::Established)),
            setup
        );
        assert_eq!(fresh.iter().any(coarse), refuse, "fresh round: {fresh:?}");
        let script = async {
            let idle = read_line(&mut server).await;
            assert!(idle.contains("IDLE"), "{idle}");
            respond(&mut server, "+ idling\r\n").await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            account
                .push_subscribe(&[push_folder("Z-New")])
                .await
                .unwrap();
            let done = read_line(&mut server).await;
            assert!(done.contains("DONE"), "{done}");
            respond(&mut server, &format!("{} OK idle done\r\n", tag_of(&idle))).await;
            let notify = read_line(&mut server).await;
            assert!(notify.contains("NOTIFY SET"), "re-registered: {notify}");
            assert!(notify.contains("Z-New"), "new folder registered: {notify}");
            respond(&mut server, &answer(tag_of(&notify))).await;
            let again = read_line(&mut server).await;
            assert!(again.contains("IDLE"), "{again}");
            respond(&mut server, "+ idling\r\n").await;
            respond(&mut server, "* 2 EXISTS\r\n").await;
            let done = read_line(&mut server).await;
            assert!(done.contains("DONE"), "{done}");
            respond(&mut server, &format!("{} OK idle done\r\n", tag_of(&again))).await;
        };
        let (interrupted, ()) = tokio::join!(
            next_events_until(&mut session, |e| matches!(e, E::Healthy)),
            script
        );
        assert_eq!(
            interrupted.iter().any(coarse),
            refuse,
            "interrupted round: {interrupted:?}"
        );
        assert_eq!(
            interrupted.iter().any(|event| matches!(event,
                E::Invalidated(InvalidationHint {
                    payload: HintPayload::SpecificCursorScope(bifrost_types::CursorScope::Folder(folder)),
                    ..
                }) if folder.0 == "Z-New"
            )),
            !refuse,
            "new coverage invalidates after acknowledgement: {interrupted:?}"
        );
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(session);
        drop(server);
        drop(peer);
        account.close().await.unwrap();
    }
}
