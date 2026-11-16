use super::*;

/// RFC 3501 Section 6.1.3 owes a BYE only ahead of a tagged OK. A server that
/// refuses LOGOUT has not started closing, so the caller must see the
/// refusal itself, not a "missing BYE" protocol error that blames the server
/// for an omission it never made.
#[tokio::test]
async fn a_refused_logout_reports_the_refusal_not_a_missing_bye() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let logout = read_line(&mut server).await;
        assert!(logout.contains("LOGOUT"), "unexpected command: {logout}");
        let tag = tag_of(&logout).to_owned();
        respond(&mut server, &format!("{tag} NO not now\r\n")).await;
        server
    });

    let err = conn.logout().await.expect_err("a refused LOGOUT fails");
    assert!(
        matches!(err, Error::ServerStatus(crate::error::ServerStatus::No { ref text, .. }) if text.contains("not now")),
        "got {err:?}"
    );
    let _server = script.await.unwrap();
}

/// RFC 3501 Section 6.1.3 requires the server's BYE before LOGOUT's tagged OK.
/// The BYE is part of this command's response sequence and must reach its
/// consumer instead of aborting dispatch as a server-initiated disconnect.
#[tokio::test]
async fn a_successful_logout_consumes_bye_before_its_tagged_ok() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let logout = read_line(&mut server).await;
        assert!(logout.contains("LOGOUT"), "unexpected command: {logout}");
        let tag = tag_of(&logout).to_owned();
        respond(
            &mut server,
            &format!("* BYE logging out\r\n{tag} OK LOGOUT completed\r\n"),
        )
        .await;
        server
    });

    conn.logout()
        .await
        .expect("a conforming BYE followed by tagged OK completes LOGOUT");
    assert_eq!(
        conn.state_rx.borrow().session_state,
        SessionState::Logout,
        "the completed LOGOUT must publish the terminal session state"
    );
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn unsolicited_responses_during_noop_become_typed_events() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("NOOP"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* 12 EXISTS\r\n* 4 EXPUNGE\r\n{tag} OK NOOP completed\r\n"),
        )
        .await;
        server
    });

    conn.noop(Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    let events = conn.drain_events().await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, typed_event::TypedEvent::Exists(12))),
        "EXISTS was not surfaced: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, typed_event::TypedEvent::Expunge(4))),
        "EXPUNGE was not surfaced: {events:?}"
    );
}

#[tokio::test]
async fn capability_response_updates_the_cached_snapshot() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    assert!(!conn.capabilities().contains(&Capability::Idle));

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("CAPABILITY"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 IDLE MOVE\r\n{tag} OK CAPABILITY completed\r\n"),
        )
        .await;
        server
    });

    let caps = conn.capability(Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    assert!(caps.contains(&Capability::Idle));
    assert!(conn.capabilities().contains(&Capability::Move));
}

#[tokio::test]
async fn bye_mid_command_preserves_the_response_code_without_waiting_for_close() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _line = read_line(&mut server).await;
        respond(&mut server, "* BYE [UNAVAILABLE] server shutting down\r\n").await;
        let _ = release_rx.await;
    });

    let err = conn
        .noop(Duration::from_secs(5))
        .await
        .expect_err("the command cannot complete after BYE");

    assert!(
        matches!(
            err,
            Error::Bye {
                code: Some(ResponseCode::Unavailable),
                ..
            }
        ),
        "BYE must preserve its structured response code: {err:?}"
    );
    // The NOOP was on the wire and the BYE did not complete it, so its
    // outcome is unknown: InFlight, not the missing evidence that reads as
    // Unsent.
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "{err:?}"
    );
    assert!(
        !conn.is_alive(),
        "BYE must close the driver command channel even while the peer stays open"
    );
    let _ = release_tx.send(());
    script.await.unwrap();
}

#[tokio::test]
async fn bye_carrying_a_capability_code_still_ends_the_command() {
    // `* BYE [CAPABILITY ...]` is a real shutdown shape (servers repeat their
    // pre-login capabilities on the way out). The response code must not
    // route the response away from the fatal-BYE lane.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _line = read_line(&mut server).await;
        respond(
            &mut server,
            "* BYE [CAPABILITY IMAP4rev1 MOVE] shutting down\r\n",
        )
        .await;
        let _ = release_rx.await;
    });

    let err = tokio::time::timeout(Duration::from_secs(5), conn.noop(Duration::from_secs(30)))
        .await
        .expect("BYE must end the command without waiting for the peer to close")
        .expect_err("the command cannot complete after BYE");

    assert!(
        matches!(
            err,
            Error::Bye {
                code: Some(ResponseCode::Capability(_)),
                ..
            }
        ),
        "BYE must preserve its structured response code: {err:?}"
    );
    assert!(
        !conn.is_alive(),
        "BYE must close the driver command channel"
    );
    let _ = release_tx.send(());
    script.await.unwrap();
}
