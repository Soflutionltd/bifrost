use super::*;

#[tokio::test]
async fn append_waits_for_the_continuation_before_sending_the_literal() {
    // No LITERAL+/LITERAL- advertised, so the APPEND literal is
    // synchronizing (RFC 3501 Section 4.3): the driver must stop after
    // `{n}\r\n` and wait for `+`.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(header.contains("APPEND"), "unexpected command: {header}");
        assert!(
            header.trim_end().ends_with("{5}"),
            "expected a synchronizing literal marker, got: {header}"
        );
        let tag = tag_of(&header).to_owned();
        // The driver tags APPEND from its own generator, like every command.
        assert!(
            tag.len() == 16 && tag.bytes().all(|b| b.is_ascii_hexdigit()),
            "APPEND must carry a driver-generated tag, got: {header}"
        );

        // Nothing more may arrive until we grant the continuation.
        respond(&mut server, "+ Ready for literal data\r\n").await;

        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        let trailer = read_line(&mut server).await;
        assert_eq!(trailer, "\r\n");

        respond(
            &mut server,
            &format!("{tag} OK [APPENDUID 4242 12] APPEND completed\r\n"),
        )
        .await;
        server
    });

    let uid = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();

    assert_eq!(
        uid,
        Some(crate::types::AppendUid {
            validity: crate::types::UidValidity::new(4242).unwrap(),
            uid: crate::types::Uid::new(12).unwrap()
        })
    );
}

#[tokio::test]
async fn append_uses_a_non_synchronizing_literal_under_literal_plus() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(
            header.trim_end().ends_with("{5+}"),
            "expected a LITERAL+ marker, got: {header}"
        );
        let tag = tag_of(&header).to_owned();
        // The payload follows without a continuation.
        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        let trailer = read_line(&mut server).await;
        assert_eq!(trailer, "\r\n");
        respond(&mut server, &format!("{tag} OK APPEND completed\r\n")).await;
        server
    });

    let uid = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
    assert_eq!(uid, None, "no APPENDUID means no id");
}

#[tokio::test]
async fn literal_plus_append_does_not_wait_on_a_marker_shaped_body_line() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(header.trim_end().ends_with("{11+}"));
        let tag = tag_of(&header).to_owned();
        let body = crate::connection::test_support::read_exact(&mut server, 11).await;
        assert_eq!(&body[..], b"abc{3}\r\ndef");
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(&mut server, &format!("{tag} OK APPEND completed\r\n")).await;
        server
    });

    conn.append("INBOX", &[], None, b"abc{3}\r\ndef", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn append_rejects_oversized_messages_before_the_wire() {
    // RFC 7889: APPENDLIMIT is enforced client-side.
    let (conn, _server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 APPENDLIMIT=4"))
            .await;
    match conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
    {
        Err(Error::LocalRefusal(crate::error::LocalRefusal::AppendLimit { size, limit })) => {
            assert_eq!(size, 5);
            assert_eq!(limit, 4);
        }
        other => panic!("expected AppendLimit, got {other:?}"),
    }
}

#[tokio::test]
async fn append_uses_the_most_restrictive_global_appendlimit() {
    let conn = crate::connection::test_support::detached(
        SessionState::Authenticated,
        vec![
            Capability::AppendLimit(Some(10)),
            Capability::AppendLimit(Some(4)),
        ],
        &[],
    );

    assert!(matches!(
        conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
            .await,
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::AppendLimit { size: 5, limit: 4 }
        ))
    ));
}

#[tokio::test]
async fn append_checks_the_mailbox_specific_appendlimit_before_writing() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 APPENDLIMIT"))
            .await;
    let script = tokio::spawn(async move {
        let status = read_line(&mut server).await;
        assert!(
            status.ends_with(" STATUS \"INBOX\" (APPENDLIMIT)\r\n"),
            "expected an APPENDLIMIT STATUS query, got {status:?}"
        );
        let tag = tag_of(&status).to_owned();
        respond(
            &mut server,
            &format!("* STATUS INBOX (APPENDLIMIT 4)\r\n{tag} OK STATUS completed\r\n"),
        )
        .await;
        server
    });

    assert!(matches!(
        conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
            .await,
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::AppendLimit { size: 5, limit: 4 }
        ))
    ));
    let _server = script.await.expect("STATUS transcript");
}

/// A capability that disappears while the APPENDLIMIT preflight is in flight
/// must not leave MULTIAPPEND emitting bytes encoded for the old one.
///
/// This needs NO second handle and no task interleaving, which is what makes it
/// the sharp case. `multi_append` used to snapshot `allow_literal8` BEFORE
/// awaiting the `STATUS`, then patch its literal markers with that stale value.
/// A server may send an unsolicited `* CAPABILITY` at any time (RFC 3501
/// Section 7.2.1), so answering the STATUS with one that drops BINARY left the
/// command still emitting the non-synchronizing `~{n+}` form. A server that does
/// not accept non-synchronizing literal8 then reads the MESSAGE BODY as command
/// lines - a desynchronized parse, not a clean rejection.
///
/// The driver now encodes and validates from its own live state when it
/// executes the command, which is necessarily after the `STATUS` completed. So
/// the vanished BINARY is seen there and the command is refused before a byte
/// is written. The test after this one stages the harder variant, where the
/// state moves while the command sits QUEUED behind another.
#[tokio::test]
async fn multiappend_refuses_bytes_built_before_a_capability_vanished() {
    use crate::types::AppendMessage;

    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 MULTIAPPEND BINARY LITERAL+ APPENDLIMIT",
    ))
    .await;
    let script = tokio::spawn(async move {
        let status = read_line(&mut server).await;
        let tag = tag_of(&status).to_owned();
        // The unsolicited CAPABILITY drops BINARY, so literal8 loses its `+`
        // eligibility (RFC 7888 Section 6) the instant this lands.
        respond(
            &mut server,
            &format!(
                "* STATUS INBOX (APPENDLIMIT 9999)\r\n\
                 * CAPABILITY IMAP4rev1 MULTIAPPEND LITERAL+ APPENDLIMIT\r\n\
                 {tag} OK STATUS completed\r\n"
            ),
        )
        .await;
        server
    });

    // NUL forces literal8, which is the form whose `+` eligibility BINARY
    // governs.
    let messages = vec![AppendMessage::new(b"a\0b".to_vec())];
    let result = conn
        .multi_append("INBOX", &messages, Duration::from_secs(5))
        .await;
    let _server = script.await.expect("STATUS transcript");

    let err = result.expect_err("bytes built against the pre-CAPABILITY state must not be sent");
    assert!(
        matches!(err, Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(ref m)) if m.contains("requires BINARY literal8 support")),
        "the literal kind must be derived from the state at execution, so a \
         vanished BINARY is caught by the driver; got {err:?}"
    );
}

/// A queued APPEND is validated against the state at EXECUTION, not the state
/// when it was submitted.
///
/// Handle A's NOOP is in flight with its reply withheld. Handle B's APPEND of
/// a NUL-bearing body is submitted while BINARY is still advertised and queues
/// behind A. A's reply then carries a CAPABILITY that drops BINARY. The driver
/// encodes B only when it reaches it, so it sees BINARY gone and refuses
/// before writing a byte; a handle-side encoding would have promised a
/// `~{n+}` literal8 the server no longer accepts. (This test used to pin the
/// driver's `WireAssumptions` comparison, which existed only because the bytes
/// were built handle-side and could go stale in the queue.)
///
/// Paused time is what makes the staging deterministic. A 1 ms sleep can only
/// complete by auto-advance, which the runtime performs only once every task
/// is idle - so when it returns, B has necessarily reached its wait on the
/// driver's reply, i.e. it is queued.
#[tokio::test(start_paused = true)]
async fn a_queued_append_is_validated_against_the_state_at_execution() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 BINARY LITERAL+",
    ))
    .await;
    let conn = std::sync::Arc::new(conn);
    let (noop_seen_tx, noop_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let noop = read_line(&mut server).await;
        assert!(noop.ends_with(" NOOP\r\n"), "expected NOOP, got {noop:?}");
        let tag = tag_of(&noop).to_owned();
        noop_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 LITERAL+\r\n{tag} OK NOOP completed\r\n"),
        )
        .await;
        // Everything the client writes from here until it hangs up. Any
        // APPEND in it means the stale bytes reached the wire. Read to EOF
        // rather than expecting a particular next command, so the assertion
        // holds however the refusal leaves the connection.
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
            .await
            .expect("duplex read");
        let rest = String::from_utf8_lossy(&rest);
        assert!(
            !rest.contains("APPEND"),
            "the stale APPEND must never be written, got {rest:?}"
        );
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.noop(Duration::from_secs(5)).await }
    });
    noop_seen_rx.await.expect("server saw the NOOP");
    let append = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        // NUL forces literal8, whose `+` eligibility BINARY governs.
        async move {
            conn.append("INBOX", &[], None, b"a\0b", Duration::from_secs(5))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first.await.expect("noop task").expect("NOOP completes");
    let err = append
        .await
        .expect("append task")
        .expect_err("an APPEND the live state cannot carry must not be sent");
    assert!(
        // The handle admitted it while BINARY was advertised; the driver found
        // the capability gone at the head of the queue, so it is a state change
        // before send (safe to re-issue), not a permanent refusal.
        matches!(
            err,
            Error::LocalRefusal(crate::error::LocalRefusal::StateChangedBeforeSend(ref f))
                if f.message.contains("requires BINARY literal8 support")
        ),
        "the driver must validate the queued APPEND against the state it \
         executes under; got {err:?}"
    );
    // Dropping the last handle closes the client half, which ends the
    // server's read.
    drop(conn);
    script.await.expect("NOOP transcript");
}

/// The same staging, for an APPEND the live state CAN still carry: it must be
/// encoded for the state at execution, not the state at submission.
///
/// LITERAL+ is advertised when the APPEND is submitted, so a handle-side
/// encoding would emit `{5+}` and stream the body without waiting. A's reply
/// drops LITERAL+, and the driver, encoding when it reaches the APPEND, must
/// instead emit the synchronizing `{5}` and wait for `+` (RFC 3501 Section 4.3).
/// Writing the `+` form to a server without LITERAL+ would make it read the body
/// as command lines.
#[tokio::test(start_paused = true)]
async fn a_queued_append_is_encoded_for_the_state_at_execution() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;
    let conn = std::sync::Arc::new(conn);
    let (noop_seen_tx, noop_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let noop = read_line(&mut server).await;
        assert!(noop.ends_with(" NOOP\r\n"), "expected NOOP, got {noop:?}");
        let tag = tag_of(&noop).to_owned();
        noop_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 UIDPLUS\r\n{tag} OK NOOP completed\r\n"),
        )
        .await;

        let header = read_line(&mut server).await;
        assert!(
            header.ends_with(" APPEND \"INBOX\" {5}\r\n"),
            "LITERAL+ is gone by execution time, so the marker must be \
             synchronizing; got {header:?}"
        );
        let tag = tag_of(&header).to_owned();
        respond(&mut server, "+ Ready for literal data\r\n").await;
        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(
            &mut server,
            &format!("{tag} OK [APPENDUID 7 3] APPEND completed\r\n"),
        )
        .await;
        server
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.noop(Duration::from_secs(5)).await }
    });
    noop_seen_rx.await.expect("server saw the NOOP");
    let append = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move {
            conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first.await.expect("noop task").expect("NOOP completes");
    let uid = append
        .await
        .expect("append task")
        .expect("the APPEND is encoded for the live state and succeeds");
    assert_eq!(
        uid,
        Some(crate::types::AppendUid {
            validity: crate::types::UidValidity::new(7).unwrap(),
            uid: crate::types::Uid::new(3).unwrap()
        })
    );
    let _server = script.await.expect("transcript");
}

/// Session legality is re-checked by the driver at execution. An APPEND that
/// was legal when submitted is refused if a command ahead of it moved the
/// session out of Authenticated/Selected, nothing is written, and the
/// connection stays usable: the refusal happens before the first byte, so it
/// must not retire the connection. The variant is `StateChangedBeforeSend`,
/// with `Unsent` evidence: local state moved under a queued command, which is
/// transient, never a provider fault.
#[tokio::test(start_paused = true)]
async fn a_queued_append_is_refused_when_the_session_left_the_legal_states() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 UNAUTHENTICATE"))
            .await;
    let conn = std::sync::Arc::new(conn);
    let (unauth_seen_tx, unauth_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let unauth = read_line(&mut server).await;
        assert!(
            unauth.ends_with(" UNAUTHENTICATE\r\n"),
            "expected UNAUTHENTICATE, got {unauth:?}"
        );
        let tag = tag_of(&unauth).to_owned();
        unauth_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(&mut server, &format!("{tag} OK Unauthenticated\r\n")).await;

        // The very next thing on the wire is the capability refresh the
        // driver owes UNAUTHENTICATE inside the same claimed command. The
        // queued APPEND is claimed only after it, and its refusal must not
        // reach the wire either.
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" CAPABILITY\r\n"),
            "the queued APPEND must not have been written; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1\r\n{tag} OK CAPABILITY completed\r\n"),
        )
        .await;
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
            .await
            .expect("duplex read");
        let rest = String::from_utf8_lossy(&rest);
        assert!(!rest.contains("APPEND"), "APPEND was written: {rest:?}");
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.unauthenticate(Duration::from_secs(5)).await }
    });
    unauth_seen_rx.await.expect("server saw UNAUTHENTICATE");
    let append = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move {
            conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first
        .await
        .expect("unauthenticate task")
        .expect("UNAUTHENTICATE and its CAPABILITY refresh complete");
    let err = append
        .await
        .expect("append task")
        .expect_err("the session is no longer legal for APPEND");
    assert!(
        matches!(
            err,
            Error::LocalRefusal(crate::error::LocalRefusal::StateChangedBeforeSend(_))
        ),
        "session legality must be refused as StateChangedBeforeSend; got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent),
        "nothing was written, and the error must say so"
    );
    drop(conn);
    script.await.expect("transcript");
}

/// An APPEND the driver refuses during validation leaves the connection
/// usable. The refusal (here a NUL-bearing body on a server without BINARY) is
/// made before the first byte, so the framing is intact and retiring the
/// connection would throw away a healthy one. The follow-up NOOP is the proof.
#[tokio::test(start_paused = true)]
async fn a_refused_append_leaves_the_connection_usable() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let script = tokio::spawn(async move {
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" NOOP\r\n"),
            "the refused APPEND must not reach the wire; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let err = conn
        .append("INBOX", &[], None, b"a\0b", Duration::from_secs(5))
        .await
        .expect_err("a NUL body without BINARY must be refused");
    assert!(
        matches!(err, Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(ref m)) if m.contains("BINARY")),
        "expected the BINARY refusal; got {err:?}"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection survives a refusal made before the first byte");
    let _server = script.await.expect("NOOP transcript");
}

/// MULTIAPPEND validates the MULTIAPPEND capability in the driver, at
/// execution, and writes no APPEND when it is missing.
///
/// Paused time because dropping the last handle makes the driver attempt a
/// best-effort LOGOUT against a server that never answers, bounded by a
/// five-second drain timeout that would otherwise run in real time.
#[tokio::test(start_paused = true)]
async fn multiappend_without_the_capability_is_refused_before_the_wire() {
    use crate::types::AppendMessage;

    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+")).await;
    let messages = vec![
        AppendMessage::new(&b"one"[..]),
        AppendMessage::new(&b"two"[..]),
    ];
    let err = conn
        .multi_append("INBOX", &messages, Duration::from_secs(5))
        .await
        .expect_err("MULTIAPPEND is not advertised");
    assert!(
        matches!(err, Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(ref m)) if m.contains("MULTIAPPEND")),
        "got {err:?}"
    );
    drop(conn);
    let mut rest = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
        .await
        .expect("duplex read");
    let rest = String::from_utf8_lossy(&rest);
    assert!(
        !rest.contains("APPEND"),
        "no APPEND may be written, got {rest:?}"
    );
}

/// MULTIAPPEND over the wire with synchronizing literals: one continuation per
/// message body, the second message's syntax follows the first body directly,
/// and the command ends with a single CRLF (RFC 3502 Section 3).
#[tokio::test]
async fn multiappend_waits_for_a_continuation_before_each_body() {
    use crate::types::AppendMessage;

    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 MULTIAPPEND UIDPLUS",
    ))
    .await;
    let script = tokio::spawn(async move {
        let first = read_line(&mut server).await;
        assert!(
            first.ends_with(" APPEND \"INBOX\" (\\Seen) {5}\r\n"),
            "{first:?}"
        );
        let tag = tag_of(&first).to_owned();
        respond(&mut server, "+ go\r\n").await;
        assert_eq!(
            &crate::connection::test_support::read_exact(&mut server, 5).await[..],
            b"HELLO"
        );
        let second = read_line(&mut server).await;
        assert_eq!(second, " {3}\r\n", "second message follows the first body");
        respond(&mut server, "+ go\r\n").await;
        assert_eq!(
            &crate::connection::test_support::read_exact(&mut server, 3).await[..],
            b"BYE"
        );
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(
            &mut server,
            &format!("{tag} OK [APPENDUID 9 10:11] APPEND completed\r\n"),
        )
        .await;
        server
    });

    let mut first = AppendMessage::new(&b"HELLO"[..]);
    first.flags = vec![crate::types::Flag::Seen];
    let messages = vec![first, AppendMessage::new(&b"BYE"[..])];
    let uids = conn
        .multi_append("INBOX", &messages, Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
    assert_eq!(
        uids,
        vec![
            crate::types::AppendUid {
                validity: crate::types::UidValidity::new(9).unwrap(),
                uid: crate::types::Uid::new(10).unwrap()
            },
            crate::types::AppendUid {
                validity: crate::types::UidValidity::new(9).unwrap(),
                uid: crate::types::Uid::new(11).unwrap()
            }
        ]
    );
}

/// The APPENDLIMIT STATUS is a preflight. Its own transmission evidence is
/// about the STATUS, not about the APPEND, and APPEND is non-idempotent: a
/// preflight failure that reported `InFlight` would send DraftCreate recovery
/// down the reconcile path for a message no server ever saw.
#[tokio::test]
async fn append_preflight_failure_is_unsent_evidence_for_the_append() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 APPENDLIMIT"))
            .await;
    let script = tokio::spawn(async move {
        let status = read_line(&mut server).await;
        let tag = tag_of(&status).to_owned();
        // A tagged NO is Acknowledged evidence for the STATUS.
        respond(&mut server, &format!("{tag} NO STATUS failed\r\n")).await;
        server
    });

    let err = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .expect_err("a failed preflight must fail the APPEND");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent),
        "no APPEND octet was written, so the evidence must say so: {err:?}"
    );
    let _server = script.await.expect("STATUS transcript");
}

/// RFC 5465 Section 4: with NOTIFY STATUS active the solicited reply may land
/// in `ambiguous` instead of `items`. Reading only `items` invents a protocol
/// error, or applies a stale higher limit.
#[test]
fn mailbox_append_limit_reads_every_plausible_status_reply() {
    use crate::connection::append::mailbox_append_limit;
    use crate::types::StatusItem;
    use crate::types::mailbox::StatusResult;

    // The heuristic picked a notification; the real answer is ambiguous.
    let status = StatusResult {
        items: vec![StatusItem::Messages(3)],
        ambiguous: vec![vec![StatusItem::AppendLimit(Some(4))]],
    };
    assert_eq!(mailbox_append_limit(&status).unwrap(), Some(4));

    // Both plausible replies name a limit: the smallest one governs.
    let status = StatusResult {
        items: vec![StatusItem::AppendLimit(Some(9))],
        ambiguous: vec![vec![StatusItem::AppendLimit(Some(4))]],
    };
    assert_eq!(mailbox_append_limit(&status).unwrap(), Some(4));

    // An explicit NIL is "no limit", not a missing item.
    let status = StatusResult {
        items: vec![StatusItem::AppendLimit(None)],
        ambiguous: vec![],
    };
    assert_eq!(mailbox_append_limit(&status).unwrap(), None);

    // No reply named APPENDLIMIT at all: the server contradicts itself.
    let status = StatusResult {
        items: vec![StatusItem::Messages(3)],
        ambiguous: vec![vec![StatusItem::Messages(3)]],
    };
    assert!(matches!(
        mailbox_append_limit(&status),
        Err(Error::Provider(crate::error::ProviderFailure::Missing(_)))
    ));
}
