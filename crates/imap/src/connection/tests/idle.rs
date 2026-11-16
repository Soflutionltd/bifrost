use super::*;

// ---------------------------------------------------------------------------
// IDLE transcripts (RFC 2177): the DONE handshake and server termination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn idle_returns_the_first_event_and_completes_the_done_handshake() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        assert!(idle.ends_with(" IDLE\r\n"), "expected IDLE, got {idle:?}");
        let tag = tag_of(&idle).to_owned();
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 3 EXISTS\r\n").await;
        // RFC 2177 Section 3: the client ends the session with an untagged
        // DONE, and only then does the tagged completion arrive.
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        respond(&mut server, &format!("{tag} OK IDLE terminated\r\n")).await;
        server
    });

    let event = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("IDLE must complete through the DONE handshake");
    assert!(
        matches!(
            event,
            IdleEvent::Event(crate::connection::typed_event::TypedEvent::Exists(3))
        ),
        "got {event:?}"
    );
    let _server = script.await.unwrap();

    assert!(
        conn.is_alive(),
        "a completed IDLE session leaves the connection reusable"
    );
}

#[tokio::test]
async fn idle_surfaces_a_server_terminated_session_without_done() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        let tag = tag_of(&idle).to_owned();
        respond(&mut server, "+ idling\r\n").await;
        // RFC 2177 Section 3: the server MAY end IDLE unilaterally by
        // sending the tagged completion before any DONE.
        respond(&mut server, &format!("{tag} OK IDLE ended by server\r\n")).await;
        server
    });

    let event = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("a server-terminated IDLE is not an error");
    assert!(
        matches!(event, IdleEvent::ServerTerminated),
        "got {event:?}"
    );
    let _server = script.await.unwrap();
}

/// The driver owned the IDLE command when it died - it had already written
/// it - so the error must say `InFlight`. The event channel and the result
/// channel close together; IDLE's wait reads the event channel first, and that
/// arm used to mint a phase-less `DriverGone`, which reads as `Unsent`.
#[tokio::test]
async fn a_driver_death_mid_idle_is_in_flight() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        assert!(idle.ends_with(" IDLE\r\n"), "expected IDLE, got {idle:?}");
        respond(&mut server, "+ idling\r\n").await;
        let _ = entered_tx.send(());
        server
    });

    let (result, ()) = tokio::join!(
        conn.idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        ),
        async {
            entered_rx.await.unwrap();
            conn.terminate().await;
        }
    );
    let err = result.expect_err("a dead driver must fail the IDLE");
    assert!(
        matches!(
            err,
            Error::Transport {
                kind: crate::error::TransportFailure::DriverGone,
                ..
            } | Error::Internal {
                kind: crate::error::InternalFailure::Panicked(_),
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "the IDLE command was on the wire: {err:?}"
    );
    let _server = script.await.unwrap();
}

/// A BYE mid-IDLE ends the driver after it has answered the IDLE command
/// with the BYE. IDLE's event-channel arm is polled first and used to replace
/// that answer with a generic `DriverGone`, losing the server's text and code.
#[tokio::test]
async fn a_bye_mid_idle_surfaces_as_the_bye_not_as_driver_gone() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n* BYE server shutting down\r\n").await;
        let _ = sent_tx.send(());
        server
    });

    let mut idle = std::pin::pin!(conn.idle(
        Duration::from_secs(5),
        Duration::from_secs(5),
        tokio_util::sync::CancellationToken::new(),
    ));
    assert!(
        futures::FutureExt::now_or_never(&mut idle).is_none(),
        "IDLE is waiting"
    );
    sent_rx.await.unwrap();
    wait_for_driver_exit(&conn).await;

    let err = idle.await.expect_err("a BYE ends the IDLE");
    assert!(
        matches!(err, Error::Bye { ref text, .. } if text.contains("shutting down")),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "the IDLE was on the wire: {err:?}"
    );
    let _server = script.await.unwrap();
}

/// RFC 2177 Section 3: the server grants IDLE with `+`. A tagged OK for IDLE
/// with no `+` first is an omission by a server that finished the exchange:
/// the error must say so in IDLE's terms (it used to be worded as a literal
/// continuation fault) and, the framing being intact, must not retire the
/// connection.
#[tokio::test]
async fn an_idle_completed_without_its_continuation_is_an_idle_omission() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        let tag = tag_of(&idle).to_owned();
        respond(&mut server, &format!("{tag} OK IDLE done already\r\n")).await;
        let noop = read_line(&mut server).await;
        assert!(noop.contains("NOOP"), "unexpected command: {noop}");
        let tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let err = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("IDLE without its continuation is a server omission");
    assert!(
        matches!(err, Error::Provider(crate::error::ProviderFailure::Missing(ref m)) if m.contains("IDLE") && !m.contains("literal")),
        "got {err:?}"
    );
    assert!(
        conn.is_alive(),
        "the exchange completed; the wire is in sync"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection still carries the next command");
    let _server = script.await.unwrap();
}

/// The IDLE command is on the wire once the server grants it, so an EOF in
/// the IDLE read loop is `InFlight`. It used to carry no phase and read as
/// `Unsent`.
#[tokio::test]
async fn an_eof_mid_idle_is_in_flight() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n").await;
        drop(server);
        let _ = sent_tx.send(());
    });

    let mut idle = std::pin::pin!(conn.idle(
        Duration::from_secs(5),
        Duration::from_secs(5),
        tokio_util::sync::CancellationToken::new(),
    ));
    assert!(
        futures::FutureExt::now_or_never(&mut idle).is_none(),
        "IDLE is waiting"
    );
    sent_rx.await.unwrap();
    wait_for_driver_exit(&conn).await;

    let err = idle.await.expect_err("an EOF ends the IDLE");
    assert!(
        matches!(
            err,
            Error::Transport {
                kind: crate::error::TransportFailure::Closed,
                ..
            } | Error::Transport {
                kind: crate::error::TransportFailure::Io(crate::error::IoFailure(_)),
                ..
            }
        ),
        "the driver's own error, not a generic DriverGone: {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "{err:?}"
    );
    script.await.unwrap();
}

/// `next_event` submits nothing, so a dead driver is `Unsent` from its point
/// of view - stated, not left to the missing-evidence default.
#[tokio::test]
async fn next_event_on_a_dead_driver_carries_a_phase() {
    let dead = crate::connection::test_support::detached(SessionState::Selected, Vec::new(), &[]);
    let err = dead
        .next_event(Duration::from_secs(5))
        .await
        .expect_err("a closed event channel is a dead driver");
    assert!(
        matches!(
            err,
            Error::Transport {
                kind: crate::error::TransportFailure::DriverGone,
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
}

// A peer whose TCP is alive but which never answers the tagged OK for IDLE
// used to park the caller in `result_rx.await` forever: past the cancel
// token, past account shutdown, past `close()`. The DONE handshake carries
// the same bound as every other command.
#[tokio::test]
async fn idle_done_handshake_is_bounded_when_the_server_never_completes() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 3 EXISTS\r\n").await;
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        // No tagged completion, ever. The socket stays open.
        tokio::time::sleep(Duration::from_secs(30)).await;
        server
    });

    let err = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_millis(100),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("an unanswered DONE must not park the caller");
    assert!(
        matches!(
            err,
            crate::Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            }
        ),
        "got {err:?}"
    );
    script.abort();
}

/// A DONE handshake that times out leaves the driver inside the IDLE
/// exchange: the connection is abandoned, so it is not reusable, and dropping
/// its last handle aborts the driver, releasing the socket. Without that, a
/// driver blocked in the post-DONE drain against a silent peer would hold the
/// transport for the life of the process.
///
/// Removing IDLE's in-flight guard fails the reusability assertion; removing
/// `ImapConnection`'s `Drop` fails the EOF assertion (the server end never
/// sees the client close).
#[tokio::test(start_paused = true)]
async fn an_idle_whose_done_times_out_is_abandoned_and_its_last_drop_releases_the_socket() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = async {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 3 EXISTS\r\n").await;
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        // No tagged completion.
    };
    let idle = conn.idle(
        Duration::from_secs(5),
        Duration::from_millis(100),
        tokio_util::sync::CancellationToken::new(),
    );
    let (result, ()) = tokio::join!(idle, script);
    assert!(
        matches!(
            result,
            Err(crate::Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            })
        ),
        "got {result:?}"
    );
    assert!(
        !conn.is_reusable(),
        "the driver is still inside the IDLE exchange"
    );

    drop(conn);
    let mut rest = Vec::new();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest),
    )
    .await;
    assert!(
        matches!(closed, Ok(Ok(_))),
        "the abandoned driver kept the socket open after its last handle dropped"
    );
}

/// Ending IDLE through the cancel token with a completed DONE handshake is a
/// completed exchange: the connection stays reusable, which push relies on to
/// re-IDLE on the same session after a subscription change.
#[tokio::test]
async fn an_idle_cancelled_through_its_token_stays_reusable() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let cancel = tokio_util::sync::CancellationToken::new();

    let script = {
        let cancel = cancel.clone();
        async move {
            let idle = read_line(&mut server).await;
            let tag = tag_of(&idle).to_owned();
            respond(&mut server, "+ idling\r\n").await;
            cancel.cancel();
            assert_eq!(read_line(&mut server).await, "DONE\r\n");
            respond(&mut server, &format!("{tag} OK IDLE terminated\r\n")).await;
            server
        }
    };
    let idle = conn.idle(Duration::from_secs(5), Duration::from_secs(5), cancel);
    let (event, _server) = tokio::join!(idle, script);
    assert!(matches!(event, Ok(IdleEvent::Cancelled)), "got {event:?}");
    assert!(
        conn.is_reusable(),
        "a completed DONE leaves the connection reusable"
    );
}

/// IDLE is admitted on the handle like every other command: outside
/// Authenticated/Selected it is caller sequencing, refused before submission.
#[tokio::test]
async fn idle_is_admitted_against_the_session_state() {
    let conn = crate::connection::test_support::detached(
        SessionState::NotAuthenticated,
        vec![Capability::Imap4Rev1, Capability::Idle],
        &[],
    );
    let err = conn
        .idle(
            Duration::from_secs(1),
            Duration::from_secs(1),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("IDLE is not legal before authentication");
    assert!(
        matches!(
            err,
            Error::LocalRefusal(crate::error::LocalRefusal::InvalidState(_))
        ),
        "got {err:?}"
    );
}
