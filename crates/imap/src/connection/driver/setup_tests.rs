#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use bifrost_net::{BandwidthBudget, BandwidthDirection};
use bifrost_types::TransmissionState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::connection::{ImapConnection, ImapStream, LeaseHandoff, stream, test_support};

fn connector() -> native_tls::TlsConnector {
    native_tls::TlsConnector::new().unwrap()
}

fn reader(client: tokio::io::DuplexStream, budget: BandwidthBudget) -> wire::WireReader {
    wire::WireReader::new_metered(
        ImapStream::Memory(stream::Tracked::new(client)),
        None,
        Some(budget),
    )
}

async fn setup_deadline_case(phase: usize) {
    let budget = BandwidthBudget::new(Some(64));
    budget
        .admit(
            if phase == 0 {
                BandwidthDirection::Inbound
            } else {
                BandwidthDirection::Outbound
            },
            64,
        )
        .await
        .finish(64);
    let (client, mut peer) = tokio::io::duplex(256);
    peer.write_all(if phase == 1 {
        b"* OK hello\r\n"
    } else {
        b"* OK [CAPABILITY IMAP4rev1 STARTTLS] hello\r\n"
    })
    .await
    .unwrap();
    let reader = reader(client, budget);
    let inbound = reader.metering().inbound_counter();
    let start = tokio::time::Instant::now();
    let result = ImapConnection::from_wire_reader(
        reader,
        "imap.invalid",
        if phase == 2 {
            TlsMode::StartTls
        } else {
            TlsMode::None
        },
        connector(),
        Duration::from_millis(100),
        &LeaseHandoff::none(),
    )
    .await;
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        }
    ));
    assert_eq!(error.attempt(), Some(TransmissionState::Unsent));
    assert_eq!(
        start.elapsed(),
        Duration::from_millis(if phase == 2 { 300 } else { 100 })
    );
    assert_eq!(
        inbound.load(std::sync::atomic::Ordering::Relaxed) == 0,
        phase == 0,
        "only the greeting phase may stall before reading the greeting"
    );
    assert_eq!(
        peer.read(&mut [0; 1]).await.unwrap(),
        0,
        "setup stalled before its command could write"
    );
}

#[tokio::test(start_paused = true)]
async fn bandwidth_setup_greeting_deadline_includes_admission() {
    setup_deadline_case(0).await;
}

#[tokio::test(start_paused = true)]
async fn bandwidth_setup_capability_deadline_includes_admission() {
    setup_deadline_case(1).await;
}

#[tokio::test(start_paused = true)]
async fn bandwidth_setup_starttls_deadline_includes_admission() {
    setup_deadline_case(2).await;
}

async fn setup_peer_case(capability: bool) {
    let (client, mut peer) = tokio::io::duplex(256);
    let writer = tokio::spawn(async move {
        if capability {
            peer.write_all(b"* OK hello\r\n").await.unwrap();
            test_support::read_line(&mut peer).await;
        }
        if capability {
            loop {
                tokio::time::sleep(Duration::from_millis(40)).await;
                if peer.write_all(b"* OK still waiting\r\n").await.is_err() {
                    return;
                }
            }
        } else {
            for byte in b"* OK hello\r\n" {
                tokio::time::sleep(Duration::from_millis(40)).await;
                if peer.write_all(&[*byte]).await.is_err() {
                    return;
                }
            }
        }
    });
    let mut reader = reader(client, BandwidthBudget::default());
    // Exercise the phase bound through the production initializer.
    reader.set_peer_timeout(Some(Duration::from_millis(100)));
    let mut state = state::ProtocolState::new();
    let mut tags = tag::TagGenerator::new();
    let (tx, _rx) = mpsc::channel(8);
    let mut events = DriverEventSink::new(tx, None);
    let start = tokio::time::Instant::now();
    let result = initialize(
        &mut reader,
        &mut state,
        &mut tags,
        &mut events,
        Setup {
            host: "imap.invalid".into(),
            tls_mode: TlsMode::None,
            tls_connector: connector(),
            timeout: Duration::from_millis(100),
            tls_active: Arc::default(),
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        })
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(100));
    writer.abort();
}

#[tokio::test(start_paused = true)]
async fn bandwidth_setup_trickled_greeting_spends_one_peer_budget() {
    setup_peer_case(false).await;
}

#[tokio::test(start_paused = true)]
async fn bandwidth_setup_unsolicited_capability_spends_one_peer_budget() {
    setup_peer_case(true).await;
}

#[tokio::test(start_paused = true)]
async fn setup_timeout_after_a_command_write_is_inflight() {
    for starttls in [false, true] {
        let (client, mut peer) = tokio::io::duplex(256);
        peer.write_all(if starttls {
            b"* OK [CAPABILITY IMAP4rev1 STARTTLS] hi\r\n"
        } else {
            b"* OK hi\r\n"
        })
        .await
        .unwrap();
        // The greeting fits the first inbound grant, but its charged bytes
        // postpone the next read beyond 100 ms. Outbound admission is free,
        // so this expiry is the absolute deadline after a proven write, not
        // the peer timeout (whose remaining allowance has not been spent).
        let leases = LeaseHandoff::none();
        let setup = ImapConnection::from_wire_reader(
            reader(client, BandwidthBudget::new(Some(64))),
            "imap.invalid",
            if starttls {
                TlsMode::StartTls
            } else {
                TlsMode::None
            },
            connector(),
            Duration::from_millis(100),
            &leases,
        );
        let script = async {
            let line = test_support::read_line(&mut peer).await;
            assert!(line.contains(if starttls { " STARTTLS" } else { " CAPABILITY" }));
            // Keep the peer open until the absolute setup deadline expires.
            std::future::pending::<()>().await;
        };
        tokio::pin!(setup, script);
        let error = tokio::select! {
            result = &mut setup => result.unwrap_err(),
            () = &mut script => unreachable!(),
        };
        assert!(matches!(
            error,
            Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            }
        ));
        assert_eq!(error.attempt(), Some(TransmissionState::InFlight));
    }
}

#[tokio::test]
async fn setup_readiness_publishes_state_and_greeting_alert() {
    let (client, mut peer) = tokio::io::duplex(256);
    peer.write_all(b"* PREAUTH [ALERT] welcome\r\n")
        .await
        .unwrap();
    let leases = LeaseHandoff::none();
    let setup = ImapConnection::from_wire_reader(
        reader(client, BandwidthBudget::default()),
        "imap.invalid",
        TlsMode::None,
        connector(),
        Duration::from_secs(1),
        &leases,
    );
    let script = async {
        let line = test_support::read_line(&mut peer).await;
        assert!(line.contains(" CAPABILITY"));
        test_support::respond(
            &mut peer,
            &format!(
                "* CAPABILITY IMAP4rev1\r\n{} OK caps\r\n",
                test_support::tag_of(&line)
            ),
        )
        .await;
    };
    let (conn, ()) = tokio::join!(setup, script);
    let conn = conn.unwrap();
    assert_eq!(conn.session_state(), SessionState::Authenticated);
    assert_eq!(
        conn.capabilities(),
        vec![crate::types::Capability::Imap4Rev1]
    );
    assert!(
        matches!(&conn.drain_events().await[..], [crate::connection::typed_event::TypedEvent::Alert(text)] if text == "welcome")
    );
}

#[tokio::test]
async fn setup_preauth_starttls_closes_before_capability_fallback() {
    let (client, mut peer) = tokio::io::duplex(256);
    peer.write_all(b"* PREAUTH already authenticated\r\n")
        .await
        .unwrap();
    let result = ImapConnection::from_wire_reader(
        reader(client, BandwidthBudget::default()),
        "imap.invalid",
        TlsMode::StartTls,
        connector(),
        Duration::from_secs(1),
        &LeaseHandoff::none(),
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::StartTlsUnavailable
        ))
    ));
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

struct Released(Option<oneshot::Sender<()>>);
impl Drop for Released {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}

#[tokio::test(start_paused = true)]
async fn setup_cancellation_reclaims_the_transport_and_lease_before_readiness() {
    let (client, mut peer) = tokio::io::duplex(256);
    let (released, release) = oneshot::channel();
    let start = tokio::time::Instant::now();
    let leases = LeaseHandoff::new(vec![Box::new(Released(Some(released)))]);
    let mut setup = Box::pin(ImapConnection::from_wire_reader(
        reader(client, BandwidthBudget::default()),
        "imap.invalid",
        TlsMode::None,
        connector(),
        Duration::from_secs(1),
        &leases,
    ));
    std::future::poll_fn(|cx| {
        assert!(setup.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(setup);
    release.await.unwrap();
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "cancellation must not wait for the setup deadline"
    );
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn setup_panic_before_readiness_reclaims_transport_and_lease() {
    let (client, mut peer) = tokio::io::duplex(256);
    let reader = reader(client, BandwidthBudget::default());
    let inbound = reader.metering().inbound_counter();
    let (released, mut release) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (_cmd_tx, cmd_rx) = mpsc::channel(1);
    let (state_tx, _state_rx) = tokio::sync::watch::channel(Default::default());
    let (events_tx, _events_rx) = mpsc::channel(1);
    let setup = setup_then_drive(
        reader,
        Setup {
            host: "imap.invalid".into(),
            tls_mode: TlsMode::None,
            tls_connector: connector(),
            timeout: Duration::from_secs(1),
            tls_active: Arc::default(),
        },
        cmd_rx,
        state_tx,
        DriverEventSink::new(events_tx, None),
        ready_tx,
    );
    let driver = crate::connection::DriverTask::spawn(
        async move {
            tokio::pin!(setup);
            std::future::poll_fn(|cx| -> std::task::Poll<()> {
                assert!(setup.as_mut().poll(cx).is_pending());
                panic!("injected setup panic");
            })
            .await;
        },
        inbound,
        vec![Box::new(Released(Some(released)))],
    );
    // Readiness is lost on unwind, never reported as successful setup.
    assert!(ready_rx.await.is_err());
    let join = driver.handle.lock().await.take().unwrap().await;
    assert!(join.unwrap_err().is_panic());
    assert!(driver.has_exited());
    assert_eq!(release.try_recv(), Ok(()));
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

/// Both phases fit individually but their sum exceeds a single timeout.
#[tokio::test(start_paused = true)]
async fn setup_capability_receives_a_fresh_budget_after_a_slow_greeting() {
    let (client, mut peer) = tokio::io::duplex(256);
    let leases = LeaseHandoff::none();
    let setup = ImapConnection::from_wire_reader(
        reader(client, BandwidthBudget::default()),
        "imap.invalid",
        TlsMode::None,
        connector(),
        Duration::from_millis(100),
        &leases,
    );
    let script = async {
        tokio::time::sleep(Duration::from_millis(75)).await;
        peer.write_all(b"* OK hello\r\n").await.unwrap();
        let line = test_support::read_line(&mut peer).await;
        assert!(line.contains(" CAPABILITY"));
        tokio::time::sleep(Duration::from_millis(75)).await;
        test_support::respond(
            &mut peer,
            &format!(
                "* CAPABILITY IMAP4rev1\r\n{} OK caps\r\n",
                test_support::tag_of(&line)
            ),
        )
        .await;
        peer
    };
    let start = tokio::time::Instant::now();
    let (result, _peer) = tokio::join!(setup, script);
    let _conn = result.unwrap();
    assert_eq!(start.elapsed(), Duration::from_millis(150));
}

#[tokio::test(start_paused = true)]
async fn setup_starttls_budget_grows_only_when_the_upgrade_runs() {
    let (client, mut peer) = tokio::io::duplex(256);
    let leases = LeaseHandoff::none();
    let setup = ImapConnection::from_wire_reader(
        reader(client, BandwidthBudget::default()),
        "imap.invalid",
        TlsMode::StartTls,
        connector(),
        Duration::from_millis(100),
        &leases,
    );
    let script = async {
        tokio::time::sleep(Duration::from_millis(75)).await;
        peer.write_all(b"* OK hello\r\n").await.unwrap();
        let capability = test_support::read_line(&mut peer).await;
        assert!(capability.contains(" CAPABILITY"));
        tokio::time::sleep(Duration::from_millis(75)).await;
        test_support::respond(
            &mut peer,
            &format!(
                "* CAPABILITY IMAP4rev1 STARTTLS\r\n{} OK caps\r\n",
                test_support::tag_of(&capability)
            ),
        )
        .await;
        let starttls = test_support::read_line(&mut peer).await;
        assert!(starttls.contains(" STARTTLS"));
        // No socket or TLS endpoint: stall the protocol phase until its bound.
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    };
    let start = tokio::time::Instant::now();
    let (result, ()) = tokio::join!(setup, script);
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        }
    ));
    assert_eq!(error.attempt(), Some(TransmissionState::InFlight));
    assert_eq!(start.elapsed(), Duration::from_millis(450));
}
