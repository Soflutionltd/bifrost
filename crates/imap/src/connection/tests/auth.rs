use super::*;

// ---------------------------------------------------------------------------
// Driver-panic  -  consumer panic surfaces as DriverPanicked (I7)
// ---------------------------------------------------------------------------

/// The peer-certificate handle routes through the full driver path
/// (handle method -> `DriverCommand::PeerCertificate` -> driver-loop arm
/// -> `WireReader` -> `ImapStream::Memory`) and returns `None` over the
/// non-TLS in-memory transport. This pins the dispatch wiring: a missing
/// driver-loop arm would hang the oneshot and the test would time out.
#[tokio::test]
async fn peer_certificate_der_none_over_memory_stream() {
    let (conn, _server) = make_driver_test_pair().await;
    assert!(
        conn.peer_certificate_der(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
            .is_none(),
        "in-memory (non-TLS) transport must have no peer certificate DER"
    );
}

// ===========================================================================
// Pure helpers defined in `connection/mod.rs`
// ===========================================================================

// ---------------------------------------------------------------------------
// validate_tls_server_name
// ---------------------------------------------------------------------------

#[test]
fn tls_server_name_rejects_empty_and_whitespace_and_nul() {
    assert!(validate_tls_server_name("imap.example.test").is_ok());
    assert!(validate_tls_server_name("").is_err());
    assert!(validate_tls_server_name("imap example.test").is_err());
    assert!(validate_tls_server_name("imap.example.test\n").is_err());
    assert!(validate_tls_server_name("imap\0.example.test").is_err());
}

/// The channel-binding lookup a SCRAM rung makes before AUTHENTICATE is a
/// driver request, so it queues behind a stalled head command. It must spend
/// the attempt's deadline and fail `Timeout` / `Unsent` there, not wait out
/// the stalled command and then authenticate past the deadline.
#[tokio::test(start_paused = true)]
async fn a_scram_binding_lookup_behind_a_stalled_command_honours_the_auth_deadline() {
    use std::sync::Arc;

    let greeting = b"* OK [CAPABILITY IMAP4rev1 AUTH=SCRAM-SHA-256] ready\r\n";
    let (conn, mut server) = crate::connection::test_support::driver_pair(greeting).await;
    let conn = Arc::new(conn);
    let stalled = Arc::clone(&conn);
    let _stalled = tokio::spawn(async move { stalled.noop(Duration::from_secs(600)).await });
    let noop = read_line(&mut server).await;
    assert!(noop.contains("NOOP"), "unexpected command: {noop}");

    let started = tokio::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        conn.authenticate_best(
            &crate::types::Credentials::password("user", "pass"),
            &crate::types::AuthPolicy::default().allow_cleartext_without_tls(),
            Duration::from_secs(1),
        ),
    )
    .await
    .expect("authentication must end at its own deadline");
    let error = outcome.expect_err("the binding lookup cannot complete");
    assert!(
        matches!(
            error,
            Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        error.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(1)
    );
}

/// A SCRAM server-final error is returned while AUTHENTICATE still awaits its
/// tagged completion. Cancel and drain that completion before the next command.
#[tokio::test]
async fn a_scram_server_final_error_cancels_and_leaves_the_connection_reusable() {
    use base64::Engine;
    use bifrost_sasl::{ScramChannelBinding, ScramHash};

    let greeting = b"* OK [CAPABILITY IMAP4rev1 AUTH=SCRAM-SHA-256] ready\r\n";
    let (conn, mut server) = crate::connection::test_support::driver_pair(greeting).await;
    let consumer = dispatch::AuthenticateScramConsumer::new(
        ScramHash::Sha256,
        "user".to_owned(),
        "wrong".to_owned().into(),
        "nonce123".to_owned(),
        false,
        ScramChannelBinding::None,
    )
    .unwrap();
    let command = Command::Authenticate {
        mechanism: "SCRAM-SHA-256".into(),
        initial_response: None,
    };

    let script = tokio::spawn(async move {
        let authenticate = read_line(&mut server).await;
        assert!(
            authenticate.contains("AUTHENTICATE SCRAM-SHA-256"),
            "unexpected command: {authenticate}"
        );
        let auth_tag = tag_of(&authenticate).to_owned();
        respond(&mut server, "+\r\n").await;

        let client_first_line = read_line(&mut server).await;
        let client_first = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(client_first_line.trim())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(client_first, "n,,n=user,r=nonce123");
        let server_first = "r=nonce123server,s=c2FsdA==,i=4096";
        let server_first = base64::engine::general_purpose::STANDARD.encode(server_first);
        respond(&mut server, &format!("+ {server_first}\r\n")).await;

        let _client_final = read_line(&mut server).await;
        let server_final = base64::engine::general_purpose::STANDARD.encode("e=invalid-proof");
        respond(&mut server, &format!("+ {server_final}\r\n")).await;
        assert_eq!(
            read_line(&mut server).await,
            "*\r\n",
            "a local SASL failure must cancel the open exchange"
        );
        respond(
            &mut server,
            &format!("{auth_tag} NO authentication failed\r\n"),
        )
        .await;

        let noop = read_line(&mut server).await;
        assert!(noop.contains("NOOP"), "unexpected next command: {noop}");
        let noop_tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{noop_tag} OK NOOP completed\r\n")).await;
        server
    });

    let error = conn
        .submit_with_continuations(command, consumer, None)
        .await
        .expect_err("the server-final error must reject authentication");
    assert!(
        matches!(
            error,
            Error::ServerStatus(crate::error::ServerStatus::Auth { .. })
        ),
        "got {error:?}"
    );
    assert!(
        conn.is_alive(),
        "a completed SASL cancellation keeps the driver alive"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the next command must follow the consumed tagged NO");
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn starttls_rejects_an_empty_capability_snapshot_before_touching_the_driver() {
    // This models the plaintext `TlsMode::None` connection state. A missing
    // capability is not permission to attempt a downgrade-adjacent upgrade.
    let conn =
        crate::connection::test_support::detached(SessionState::NotAuthenticated, Vec::new(), &[]);
    let connector = native_tls::TlsConnector::builder().build().unwrap();

    assert!(matches!(
        conn.starttls_with_connector(connector, Duration::from_secs(1))
            .await,
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::StartTlsUnavailable
        ))
    ));
}
