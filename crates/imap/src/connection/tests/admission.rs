use super::*;

// ===========================================================================
// One command path: live session legality, pending effects, write evidence
// ===========================================================================

/// Live session legality is a property of every command, not an APPEND
/// special case: a NAMESPACE queued behind an UNAUTHENTICATE that completes
/// first is refused at the head of the queue, before a byte is written, and
/// the refusal says the state moved rather than blaming the caller.
#[tokio::test(start_paused = true)]
async fn a_queued_command_is_refused_when_the_session_left_its_legal_states() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 UNAUTHENTICATE NAMESPACE",
    ))
    .await;
    let conn = std::sync::Arc::new(conn);
    let (unauth_seen_tx, unauth_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let unauth = read_line(&mut server).await;
        assert!(unauth.ends_with(" UNAUTHENTICATE\r\n"), "{unauth:?}");
        let tag = tag_of(&unauth).to_owned();
        unauth_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(&mut server, &format!("{tag} OK Unauthenticated\r\n")).await;

        // The capability refresh `unauthenticate` issues is next. The queued
        // NAMESPACE sat ahead of it, so had it been written it would be here.
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" CAPABILITY\r\n"),
            "the queued NAMESPACE must not have been written; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 NAMESPACE\r\n{tag} OK done\r\n"),
        )
        .await;
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
            .await
            .expect("duplex read");
        let rest = String::from_utf8_lossy(&rest);
        assert!(
            !rest.contains("NAMESPACE"),
            "NAMESPACE was written: {rest:?}"
        );
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.unauthenticate(Duration::from_secs(5)).await }
    });
    unauth_seen_rx.await.expect("server saw UNAUTHENTICATE");
    let namespace = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.namespace(Duration::from_secs(5)).await }
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first
        .await
        .expect("unauthenticate task")
        .expect("UNAUTHENTICATE completes");
    let err = namespace
        .await
        .expect("namespace task")
        .expect_err("the session is no longer legal for NAMESPACE");
    assert!(
        matches!(
            err,
            Error::LocalRefusal(crate::error::LocalRefusal::StateChangedBeforeSend(_))
        ),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
    drop(conn);
    script.await.expect("transcript");
}

/// The driver refuses a batch holding a command that may not be pipelined,
/// before writing a byte of the batch, and the connection stays usable. Only
/// a crate bug can build such a batch, so it is reached here directly.
#[tokio::test(start_paused = true)]
async fn the_driver_refuses_a_batch_holding_a_command_that_cannot_be_pipelined() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let script = tokio::spawn(async move {
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" NOOP\r\n"),
            "no byte of the refused batch may reach the wire; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let (reply, mut pending) = crate::connection::reply::reply_channel(
        (
            vec![
                Command::Noop,
                Command::Append {
                    mailbox: "INBOX".into(),
                    messages: vec![AppendMessage::new(b"HELLO".to_vec())],
                    multi: false,
                },
            ],
            vec![
                Box::new(crate::connection::dispatch::TaggedOkConsumer::default())
                    as Box<dyn driver::ConsumerErased>,
                Box::new(crate::connection::dispatch::AppendConsumer::default()),
            ],
        ),
        &conn.abandoned,
    );
    conn.cmd_tx
        .send(driver::DriverCommand::Pipeline { reply })
        .await
        .expect("driver is running");
    let Err(err) = pending.recv().await.expect("driver answers") else {
        panic!("the batch must be refused");
    };
    assert!(
        matches!(
            err,
            Error::Internal {
                kind: crate::error::InternalFailure::Invariant(_),
                mid_exchange: None
            }
        ),
        "got {err:?}"
    );

    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection survives the refusal");
    let _server = script.await.expect("NOOP transcript");
}

/// A pipeline is admitted when it executes: a Selected-only command in an
/// Authenticated session is caller sequencing, refused before anything is
/// submitted.
#[tokio::test]
async fn a_pipeline_is_admitted_against_the_session_state_at_execution() {
    let conn = crate::connection::test_support::detached(
        SessionState::Authenticated,
        vec![Capability::Imap4Rev1],
        &[],
    );
    let err = conn
        .pipeline()
        .noop()
        .uid_search("ALL".to_owned())
        .execute_dynamic(std::time::Duration::from_secs(30))
        .await
        .expect_err("UID SEARCH needs a selected mailbox");
    assert!(
        matches!(
            err,
            crate::connection::pipeline::PipelineError::Driver(Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidState(_)
            ))
        ),
        "got {err:?}"
    );
}

/// Local driver queries must bound both a full admission queue and an
/// unanswered admitted request, without reporting IMAP transmission.
#[tokio::test(start_paused = true)]
async fn local_driver_queries_bound_admission_and_reply() {
    for full_queue in [false, true] {
        for certificate in [false, true] {
            let mut conn = test_support::detached(SessionState::NotAuthenticated, vec![], &[]);
            let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel(1);
            conn.cmd_tx = cmd_tx;
            let (result_tx, _result_rx) = tokio::sync::oneshot::channel();
            if full_queue {
                assert!(
                    conn.cmd_tx
                        .try_send(driver::DriverCommand::PeerCertificate { result_tx })
                        .is_ok()
                );
            }
            // Keep the receiver alive but never poll it. An empty queue admits
            // the query and stalls the reply; a full queue stalls admission.
            let timeout = Duration::from_secs(1);
            let error = if certificate {
                conn.peer_certificate_der(tokio::time::Instant::now() + timeout)
                    .await
                    .expect_err("a stalled lookup must not become an absent certificate")
            } else {
                conn.set_keepalive(TcpKeepalive::new(timeout, timeout), timeout)
                    .await
                    .expect_err("a stalled socket option must time out")
            };
            assert!(matches!(
                error,
                Error::Transport {
                    kind: crate::error::TransportFailure::Timeout,
                    ..
                }
            ));
            assert_eq!(
                error.attempt(),
                Some(bifrost_types::TransmissionState::Unsent)
            );
        }
    }
}
