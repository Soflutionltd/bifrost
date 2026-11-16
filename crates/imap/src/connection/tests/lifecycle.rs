use super::*;

/// The tagged OK synchronizes the framing, but the consumer could not make
/// sense of the exchange, so the client no longer knows what state the
/// server is in. The connection must retire before the caller is answered,
/// never be handed back for the next command.
#[tokio::test]
async fn a_consumer_failure_on_a_tagged_ok_retires_the_connection() {
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, tag_of,
    };
    let (conn, mut peer) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let server = tokio::spawn(async move {
        let command = read_line(&mut peer).await;
        respond(&mut peer, &format!("{} OK done\r\n", tag_of(&command))).await;
        peer
    });
    let result = conn
        .submit_regular(Command::Noop, UninterpretableConsumer, None)
        .await;
    assert!(
        matches!(
            result,
            Err(Error::Provider(crate::error::ProviderFailure::Parse(_)))
        ),
        "got {result:?}"
    );
    let _peer = server.await.unwrap();
    assert!(
        !conn.is_alive(),
        "a session whose answer could not be interpreted must not be reused"
    );
}

/// Dropping the last handle detaches the driver task, which then runs
/// the terminal LOGOUT drain with nothing able to abort it. Against a
/// peer that stays connected but never answers, an unbounded drain
/// leaks the task and its socket for the process lifetime - so the
/// drain must be time-bounded. Paused time: the bound elapses
/// instantly, and a regression (unbounded read loop) fails the outer
/// timeout instead of hanging the suite.
#[tokio::test(start_paused = true)]
async fn driver_logout_drain_is_bounded_against_a_silent_peer() {
    let (conn, mut server) = make_driver_test_pair().await;
    let handle = conn
        .driver_handle
        .handle
        .lock()
        .await
        .take()
        .expect("driver handle present");
    drop(conn);

    // The driver writes its best-effort LOGOUT; the peer reads it and
    // then goes silent while keeping the connection open.
    let mut buf = [0u8; 256];
    let n = server.read(&mut buf).await.unwrap();
    assert!(
        String::from_utf8_lossy(&buf[..n]).contains("LOGOUT"),
        "driver must attempt the best-effort LOGOUT"
    );

    tokio::time::timeout(std::time::Duration::from_secs(300), handle)
        .await
        .expect("bounded LOGOUT drain must let the detached driver exit")
        .expect("driver task must not panic");
    drop(server);
}

/// Invariant: a consumer that panics inside the driver task surfaces
/// as `crate::error::InternalFailure::Panicked` on the caller side. The panic message
/// is extracted from the `JoinError` and included in the error.
#[tokio::test]
async fn invariant_driver_panic_surfaces_as_error() {
    let (conn, mut server) = make_driver_test_pair().await;

    let server_task = tokio::spawn(async move {
        // Receive the CAPABILITY command from the driver.
        let mut buf = vec![0u8; 4096];
        let n = server.read(&mut buf).await.unwrap();
        let _cmd = String::from_utf8_lossy(&buf[..n]).to_string();

        // Send an untagged CAPABILITY response. This triggers
        // on_response on the PanickingConsumer, which panics.
        server
            .write_all(b"* CAPABILITY IMAP4rev1\r\n")
            .await
            .unwrap();
        // Also send the tagged OK in case the driver processes it
        // before the panic unwinds (it won't, but be safe).
        server
            .write_all(b"A001 OK CAPABILITY done\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();
        server
    });

    let result = conn
        .submit_regular(Command::Capability, PanickingConsumer, None)
        .await;
    let _server = server_task.await.unwrap();

    match result {
        Err(
            error @ Error::Internal {
                kind: crate::error::InternalFailure::Panicked(_),
                ..
            },
        ) => {
            let Error::Internal {
                kind: crate::error::InternalFailure::Panicked(msg),
                ..
            } = &error
            else {
                unreachable!()
            };
            assert!(
                msg.contains("intentional panic for test"),
                "panic message not propagated: {msg}"
            );
            // The driver owned this command when it died, so it may have
            // been written: `InFlight`, which a non-idempotent operation
            // reads back rather than declaring failed.
            assert_eq!(
                error.attempt(),
                Some(bifrost_types::TransmissionState::InFlight)
            );
        }
        other => panic!("expected DriverPanicked, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Driver-panic  -  subsequent commands fail fast (I7)
// ---------------------------------------------------------------------------

/// After the driver panics, subsequent commands must fail promptly
/// with `DriverPanicked` or `DriverGone`  -  not hang forever.
#[tokio::test]
async fn invariant_driver_panic_subsequent_commands_fail() {
    let (conn, mut server) = make_driver_test_pair().await;

    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        let _ = server.read(&mut buf).await;
        server
            .write_all(b"* CAPABILITY IMAP4rev1\r\nA001 OK done\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();
        server
    });

    // Trigger the panic via PanickingConsumer.
    let _ = conn
        .submit_regular(Command::Capability, PanickingConsumer, None)
        .await;
    let _server = server_task.await.unwrap();

    // Post-panic: the next command must not hang.
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        conn.submit_regular(Command::Noop, dispatch::TaggedOkConsumer::default(), None),
    )
    .await;

    assert!(
        result.is_ok(),
        "post-panic command must complete (not hang)"
    );
    let inner = result.unwrap();
    assert!(
        matches!(
            inner,
            Err(Error::Internal {
                kind: crate::error::InternalFailure::Panicked(_),
                ..
            } | Error::Transport {
                kind: crate::error::TransportFailure::DriverGone,
                ..
            })
        ),
        "post-panic command should be DriverPanicked or DriverGone, got: {inner:?}"
    );
    // This command never reached the dead driver: `Unsent`, whichever of the
    // two the caller observed and whether or not it was first to observe.
    assert_eq!(
        inner.expect_err("post-panic command fails").attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
}

#[tokio::test]
async fn driver_channel_liveness_is_independent_of_session_state() {
    let detached =
        crate::connection::test_support::detached(SessionState::Selected, Vec::new(), &[]);
    assert_eq!(detached.session_state(), SessionState::Selected);
    assert!(!detached.is_alive(), "the command receiver was dropped");

    let (live, _server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    assert!(
        live.is_alive(),
        "the driver still owns the command receiver"
    );
}

/// RFC 3501 Section 6.3.1: a SELECT that completes with a tagged OK but
/// without the mandatory FLAGS response is the server's omission, reported as
/// `ProtocolMissing`. The exchange is over and the framing intact, so the
/// connection must stay usable: the next command on it round-trips.
#[tokio::test]
async fn a_missing_mandatory_response_leaves_the_connection_usable() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"), "unexpected command: {select}");
        let tag = tag_of(&select).to_owned();
        respond(
            &mut server,
            &format!(
                "* 3 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 4242] UIDs valid\r\n\
                 {tag} OK [READ-WRITE] SELECT completed\r\n"
            ),
        )
        .await;
        let noop = read_line(&mut server).await;
        assert!(noop.contains("NOOP"), "unexpected command: {noop}");
        let tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let err = conn
        .select("INBOX", Duration::from_secs(5))
        .await
        .expect_err("a SELECT without FLAGS must fail");
    assert!(
        matches!(
            err,
            Error::Provider(crate::error::ProviderFailure::Missing(_))
        ),
        "got {err:?}"
    );
    assert!(
        conn.is_alive(),
        "an omission after the tagged OK leaves the wire synchronized"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection must still carry the next command");
    let _server = script.await.unwrap();
}

/// The channel capacity is 16. A caller dropped while its command waits for a
/// slot never submitted that command and must not abandon the live connection.
#[tokio::test]
async fn dropping_a_command_waiting_for_channel_capacity_does_not_abandon_connection() {
    use std::future::Future;
    use std::sync::Arc;

    const QUEUED: usize = 16;
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let conn = Arc::new(conn);

    let first_conn = Arc::clone(&conn);
    let first = tokio::spawn(async move { first_conn.noop(Duration::from_secs(5)).await });
    let first_line = read_line(&mut server).await;
    assert!(
        first_line.contains("NOOP"),
        "unexpected command: {first_line}"
    );
    let first_tag = tag_of(&first_line).to_owned();

    let queued: Vec<_> = (0..QUEUED)
        .map(|_| {
            let conn = Arc::clone(&conn);
            tokio::spawn(async move { conn.noop(Duration::from_secs(5)).await })
        })
        .collect();
    for _ in 0..128 {
        if conn.cmd_tx.capacity() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        conn.cmd_tx.capacity(),
        0,
        "all 16 channel slots must be occupied before polling the blocked send"
    );

    let mut blocked = Box::pin(conn.noop(Duration::from_secs(5)));
    let send_is_pending =
        std::future::poll_fn(|cx| std::task::Poll::Ready(blocked.as_mut().poll(cx).is_pending()))
            .await;
    assert!(
        send_is_pending,
        "the seventeenth queued command must wait to submit"
    );
    drop(blocked);
    assert!(
        conn.is_reusable(),
        "dropping before channel admission must not mark a submitted command abandoned"
    );

    respond(&mut server, &format!("{first_tag} OK first completed\r\n")).await;
    first
        .await
        .unwrap()
        .expect("the in-flight command should complete");
    for _ in 0..QUEUED {
        let line = read_line(&mut server).await;
        assert!(line.contains("NOOP"), "unexpected queued command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("{tag} OK queued completed\r\n")).await;
    }
    for task in queued {
        task.await
            .unwrap()
            .expect("each submitted command should complete");
    }
    assert!(
        conn.is_reusable(),
        "draining accepted commands must leave the connection reusable"
    );
}

/// An EOF while draining after DONE is `InFlight` too: the IDLE command and
/// the DONE are both on the wire.
#[tokio::test]
async fn an_eof_while_draining_after_done_is_in_flight() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n* 3 EXISTS\r\n").await;
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        // No tagged completion: the peer goes away instead.
        drop(server);
    });

    let err = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("an EOF during the DONE drain fails the IDLE");
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
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "{err:?}"
    );
    script.await.unwrap();
}

/// Transmission evidence comes from socket progress, not from which write
/// failed. Here the APPEND is one segment (LITERAL+), its first write is
/// accepted, and the peer vanishes while the body is still going out: bytes of
/// the command are on the wire, so the failure is `InFlight` and a
/// non-idempotent APPEND goes to reconcile. Stamping by segment position
/// called this `Unsent` and invited a blind retry that could duplicate it.
#[tokio::test(start_paused = true)]
async fn a_write_that_reached_the_socket_before_failing_is_in_flight() {
    let (conn, mut server) = crate::connection::test_support::driver_pair_with_capacity(
        &preauth_greeting("IMAP4rev1 LITERAL+"),
        64,
    )
    .await;
    let script = tokio::spawn(async move {
        // Take the command line, then vanish with the body still unsent.
        let line = read_line(&mut server).await;
        assert!(line.contains(" APPEND "), "{line:?}");
        drop(server);
    });

    let err = conn
        .append("INBOX", &[], None, &[b'x'; 4096], Duration::from_secs(5))
        .await
        .expect_err("the peer is gone mid-body");
    script.await.expect("transcript");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "bytes of the APPEND reached the socket; got {err:?}"
    );
}

/// The other half of the same rule: a write that fails with no octet of the
/// command accepted by the socket is `Unsent`, so recovery may simply retry.
/// A peer that closed before the command is the stale-pool case.
#[tokio::test(start_paused = true)]
async fn a_write_that_reached_nothing_is_unsent() {
    let (conn, server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+")).await;
    drop(server);

    let err = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .expect_err("the peer is gone");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent),
        "no octet reached the socket; got {err:?}"
    );
}
