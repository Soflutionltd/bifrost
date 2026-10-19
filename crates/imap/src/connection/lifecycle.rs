#![allow(clippy::wildcard_imports)]
use tokio::net::TcpStream;

use crate::error::ImapAttempt;
use bifrost_types::TransmissionState;

use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Connection lifecycle
    // -----------------------------------------------------------------------

    /// Connect to an IMAP server.
    ///
    /// Performs the TCP connection (with optional TLS), reads the server greeting,
    /// and parses initial capabilities per RFC 3501 Section 6.1.1.
    pub async fn connect(
        host: &str,
        port: u16,
        tls_mode: TlsMode,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let tls_connector = build_default_tls_connector()?;
        Self::connect_with_tls_connector(host, port, tls_mode, tls_connector, timeout).await
    }

    /// Connect with a custom TLS connector (RFC 3501 Section 6.1.1 /
    /// RFC 9051 Section 6.1.1).
    ///
    /// Establishes the TCP/TLS connection, reads the server greeting,
    /// fetches initial capabilities, then spawns the driver task and
    /// returns a handle-based `ImapConnection`.
    #[allow(clippy::too_many_lines)]
    pub async fn connect_with_tls_connector(
        host: &str,
        port: u16,
        tls_mode: TlsMode,
        tls_connector: native_tls::TlsConnector,
        timeout: Duration,
    ) -> Result<Self, Error> {
        Self::connect_with_tls_connector_metered(
            host,
            port,
            tls_mode,
            tls_connector,
            timeout,
            None,
            None,
        )
        .await
    }

    pub(crate) async fn connect_with_tls_connector_metered(
        host: &str,
        port: u16,
        tls_mode: TlsMode,
        tls_connector: native_tls::TlsConnector,
        timeout: Duration,
        meter_sink: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
    ) -> Result<Self, Error> {
        use super::dispatch::CapabilityConsumer;
        use super::driver;

        debug!(host, port, ?tls_mode, "connecting to IMAP server");

        let unsent = ImapAttempt::new(TransmissionState::Unsent);
        let tcp = tokio::time::timeout(timeout, TcpStream::connect((host, port)))
            .await
            .map_err(|_| Error::Timeout {
                attempt: Some(unsent),
            })?
            .map_err(|e| Error::Io {
                source: std::sync::Arc::new(e),
                attempt: Some(unsent),
            })?;
        // Wrapped before anything is layered on it, so every octet the socket
        // accepts - TLS handshake included - is counted for transmission
        // evidence (`stream::Tracked`).
        let tcp = stream::Tracked::new(tcp);

        let stream = if tls_mode.uses_implicit_tls() {
            validate_tls_server_name(host)?;
            let connector = tokio_native_tls::TlsConnector::from(tls_connector.clone());
            let tls_stream = tokio::time::timeout(timeout, connector.connect(host, tcp))
                .await
                .map_err(|_| Error::Timeout {
                    attempt: Some(unsent),
                })?
                .map_err(|e| Error::Io {
                    source: std::sync::Arc::new(std::io::Error::other(e)),
                    attempt: Some(unsent),
                })?;
            ImapStream::Tls(tls_stream)
        } else {
            ImapStream::Plain(tcp)
        };

        // --- Pre-driver phase: read greeting and initial capabilities ---
        //
        // The WireReader and ProtocolState live on the stack here. After
        // the greeting and capability fetch they are moved into the
        // driver task, which owns them for the rest of the connection.
        let mut wire_reader = wire::WireReader::new_metered(stream, meter_sink, bandwidth_budget);
        wire_reader.set_peer_timeout(Some(timeout));
        let mut proto_state = state::ProtocolState::new();
        let mut tag_gen = tag::TagGenerator::new();

        // Set up the event channel.
        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<typed_event::TypedEvent>(256);

        // Read and parse the server greeting (RFC 3501 Section 7.1).
        // This MUST happen before wrapping events_tx in DriverEventSink
        // so we can send a greeting ALERT on the raw channel (emit() is
        // pub(in crate::connection::driver) and inaccessible from here).
        let greeting = setup_timeout(timeout, wire_reader.read_greeting())
            .await
            .map_err(|e| e.with_attempt(TransmissionState::Unsent))?;

        let Response::Greeting(g) = &greeting else {
            return Err(Error::Protocol(
                "expected greeting from server (RFC 3501 Section 7.1)".into(),
            ));
        };

        // Apply greeting to protocol state (sets session state, caches
        // capabilities, returns Err on BYE).
        let greeting_alert = proto_state.apply_greeting(g)?;

        // RFC 3501 Section7.1: if the greeting carried an [ALERT], deliver it
        // on the raw channel before wrapping in DriverEventSink.
        if let Some(alert_text) = greeting_alert {
            // Best-effort: if the channel is full we drop the alert.
            let _ = events_tx.try_send(typed_event::TypedEvent::Alert(alert_text));
        }

        // Wrap the channel in DriverEventSink  -  events_tx moves here.
        let mut event_sink = driver::event_sink::DriverEventSink::new(events_tx, None);

        // If we didn't get capabilities from the greeting, request them
        // explicitly using the driver's run_one_command (RFC 3501 Section6.1.1).
        if proto_state.capabilities().is_empty() {
            let consumer = driver::DriverConsumer::Regular(
                Box::new(CapabilityConsumer::default()) as Box<dyn driver::ConsumerErased>
            );
            wire_reader.set_peer_timeout(Some(timeout));
            let result = setup_timeout(
                timeout,
                driver::run_one_command(
                    &mut wire_reader,
                    &mut proto_state,
                    &mut tag_gen,
                    &mut event_sink,
                    Command::Capability,
                    consumer,
                ),
            )
            .await?;

            // Downcast the erased output back to Vec<Capability>.
            // CapabilityConsumer::Output is Vec<Capability>, so the downcast
            // is provably correct. An Internal error here would indicate a
            // library bug in the consumer/erased-output machinery.
            let caps = result
                .downcast::<Vec<Capability>>()
                .map_err(|_| Error::Internal("CapabilityConsumer output downcast failed".into()))?;
            proto_state.apply_capability_fetch(*caps);
        }

        // --- STARTTLS upgrade (RFC 3501 Section6.2.1) ---
        //
        // When the caller requests `TlsMode::StartTls`, negotiate the
        // upgrade before spawning the driver task.  All required state
        // (`wire_reader`, `proto_state`, `tag_gen`, `event_sink`) is
        // still on the stack.  `run_starttls_upgrade` sends the STARTTLS
        // command, verifies the buffer is empty, performs the TLS
        // handshake, installs a fresh `WireReader`, and re-fetches
        // capabilities  -  so `proto_state` reflects post-TLS caps by the
        // time we snapshot it into `state_tx` below.
        if tls_mode.uses_starttls() {
            // The server must advertise STARTTLS (RFC 3501 Section6.2.1).
            if !proto_state
                .capabilities()
                .iter()
                .any(|c| matches!(c, Capability::StartTls))
            {
                return Err(Error::StartTlsUnavailable);
            }

            validate_tls_server_name(host)?;

            wire_reader.set_peer_timeout(Some(timeout));
            setup_timeout(
                timeout,
                driver::run_starttls_upgrade(
                    &mut wire_reader,
                    &mut proto_state,
                    &mut tag_gen,
                    &mut event_sink,
                    tls_connector,
                    host.to_owned(),
                ),
            )
            .await?;
        }

        wire_reader.set_peer_timeout(None);

        // --- Spawn the driver task ---
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
        let (state_tx, state_rx) = tokio::sync::watch::channel(proto_state.snapshot());

        let handle = tokio::spawn(driver::driver_task(
            wire_reader,
            proto_state,
            tag_gen,
            cmd_rx,
            state_tx,
            event_sink,
        ));

        Ok(Self {
            cmd_tx,
            state_rx,
            events_rx: tokio::sync::Mutex::new(events_rx),
            driver_handle: tokio::sync::Mutex::new(Some(handle)),
            host: host.to_owned(),
            tls_active: std::sync::atomic::AtomicBool::new(
                tls_mode.uses_implicit_tls() || tls_mode.uses_starttls(),
            ),
            abandoned: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Set TCP keepalive on the underlying socket (RFC 1122 Section 4.2.3.6).
    ///
    /// Configures the operating system's TCP keepalive probes via the
    /// driver task. This does not send any data on the IMAP wire  -  it
    /// sets socket options on the underlying file descriptor via
    /// `setsockopt(2)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the `setsockopt` call fails.
    /// Returns [`Error::DriverGone`] if the driver task has exited.
    pub async fn set_keepalive(&self, keepalive: TcpKeepalive) -> Result<(), Error> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = super::driver::DriverCommand::SetKeepalive {
            keepalive,
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic(TransmissionState::Unsent).await);
        }
        match result_rx.await {
            Ok(result) => result,
            Err(_) => Err(self.observe_driver_panic(TransmissionState::InFlight).await),
        }
    }

    /// DER of the peer (server) certificate, or `None` when the transport
    /// is not TLS, native-tls cannot produce the DER, or the driver has
    /// exited.
    ///
    /// A connection whose driver has died has no live TLS session and thus
    /// no channel binding  -  the same observable outcome as a plaintext
    /// connection  -  so the dead-driver path collapses to `None` rather
    /// than surfacing a driver-gone error.
    pub(crate) async fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = super::driver::DriverCommand::PeerCertificate { result_tx };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return None;
        }
        result_rx.await.unwrap_or(None)
    }

    /// If the driver task has terminated, return an error describing
    /// why (panic message if panicked, or `DriverGone` if exited
    /// cleanly), so the caller sees the real reason instead of a generic
    /// `Disconnected`.
    ///
    /// `phase` is how far the caller's command got, and only the CALLER
    /// knows it: `Unsent` when `cmd_tx.send` failed (the driver never
    /// received the command), `InFlight` when the send succeeded and the
    /// reply channel dropped (the driver owned the command when it died, so
    /// it may have been written - "possibly transmitted", the convention
    /// `timeout_inflight` uses). A non-idempotent command in the second
    /// phase must be read back, not declared failed, so the phase is stamped
    /// on every outcome here - including when another caller already took
    /// the join handle, since the evidence cannot depend on being first.
    pub(super) async fn observe_driver_panic(&self, phase: TransmissionState) -> Error {
        let mut guard = self.driver_handle.lock().await;
        let Some(handle) = guard.take() else {
            return Error::driver_gone_at(phase);
        };
        // `handle.is_finished()` avoids blocking if still running.
        if !handle.is_finished() {
            // Put it back  -  still running. The cmd_tx failure may be
            // a TOCTOU race with the driver exiting.
            *guard = Some(handle);
            drop(guard);
            return Error::driver_gone_at(phase);
        }
        match handle.await {
            Err(join_err) if join_err.is_panic() => {
                let panic_msg = join_err
                    .into_panic()
                    .downcast::<String>()
                    .map(|s| *s)
                    .or_else(|p| p.downcast::<&'static str>().map(|s| s.to_string()))
                    .unwrap_or_else(|_| "driver panicked (payload not a String)".to_string());
                Error::DriverPanicked {
                    message: panic_msg,
                    attempt: Some(crate::error::ImapAttempt::new(phase)),
                }
            }
            Ok(()) | Err(_) => Error::driver_gone_at(phase),
        }
    }

    /// Upgrade to TLS via STARTTLS (RFC 3501 Section 6.2.1).
    ///
    /// Valid on an unencrypted, not-authenticated connection when the server
    /// advertises STARTTLS. `TlsMode::None` creates that reachable state;
    /// `TlsMode::StartTls` performs the upgrade during connection setup.
    ///
    /// The upgrade is atomic via the `Poisoned` sentinel pattern (I9, I10)
    ///  -  handled entirely by the driver task.
    pub async fn starttls(&self, timeout: Duration) -> Result<(), Error> {
        self.starttls_with_connector(build_default_tls_connector()?, timeout)
            .await
    }

    /// STARTTLS with a custom TLS connector (RFC 3501 Section 6.2.1 /
    /// RFC 9051 Section 6.2.1).
    ///
    /// The upgrade is atomic via the `Poisoned` sentinel pattern (I9, I10):
    ///
    /// 1. Send STARTTLS, await tagged OK.
    /// 2. Verify the wire buffer is empty (no injected bytes  -  B10 fix).
    /// 3. `mem::replace` the reader with a `Poisoned`-stream reader.
    /// 4. TLS handshake (may suspend). If it fails or the future is
    ///    cancelled, the reader stays wrapping `Poisoned` forever.
    /// 5. Install a fresh `WireReader` on the new TLS stream.
    /// 6. Re-fetch capabilities (RFC 3501 Section6.2.1).
    ///
    /// All steps are executed by the driver task. The caller submits
    /// the upgrade command and awaits the result.
    pub async fn starttls_with_connector(
        &self,
        tls_connector: native_tls::TlsConnector,
        timeout: Duration,
    ) -> Result<(), Error> {
        if self.tls_active.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::InvalidInput(
                "STARTTLS requires an unencrypted connection".into(),
            ));
        }
        self.require_state(crate::types::CommandKind::StartTls)?;

        // A direct upgrade from a plaintext connection follows the same
        // capability rule as connection-time STARTTLS. An empty snapshot is
        // not evidence that STARTTLS is available.
        {
            let snap = self.state_rx.borrow();
            if !snap
                .capabilities
                .iter()
                .any(|c| matches!(c, Capability::StartTls))
            {
                return Err(Error::StartTlsUnavailable);
            }
        }

        // Validate server name before submitting so malformed input fails
        // without involving the driver.
        validate_tls_server_name(&self.host)?;

        debug!("upgrading to TLS via STARTTLS");
        tokio::time::timeout(
            timeout,
            self.submit_upgrade(driver::UpgradePayload::StartTls {
                tls_connector,
                server_name: self.host.clone(),
            }),
        )
        .await
        .map_err(|_| Error::timeout_inflight())??;
        self.tls_active
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

/// An absolute setup exchange deadline, including admission and unsolicited
/// responses. Peer I/O independently spends the reader's cumulative allowance.
pub(super) async fn setup_timeout<T>(
    timeout: Duration,
    exchange: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let deadline = tokio::time::Instant::now() + timeout;
    tokio::time::timeout_at(deadline, exchange)
        .await
        .map_err(|_| Error::Timeout { attempt: None })?
}

#[cfg(test)]
mod bandwidth_review_tests {
    use super::*;
    use bifrost_net::{BandwidthBudget, BandwidthDirection};
    use tokio::io::AsyncWriteExt;
    use tokio::time::Instant;

    async fn setup_deadline_case(phase: usize) {
        let budget = BandwidthBudget::new(Some(4));
        budget
            .admit(
                if phase == 0 {
                    BandwidthDirection::Inbound
                } else {
                    BandwidthDirection::Outbound
                },
                4,
            )
            .await
            .finish(4);
        let (client, mut peer) = tokio::io::duplex(128);
        if phase == 0 {
            peer.write_all(b"* OK hello\r\n").await.unwrap();
        }
        let mut reader = wire::WireReader::new_metered(
            ImapStream::Memory(stream::Tracked::new(client)),
            None,
            Some(budget),
        );
        let mut state = state::ProtocolState::new();
        let (_, Response::Greeting(greeting)) =
            crate::codec::decode::parse_greeting(b"* OK [CAPABILITY IMAP4rev1 STARTTLS] hello\r\n")
                .unwrap()
        else {
            panic!("greeting")
        };
        state.apply_greeting(&greeting).unwrap();
        let mut tags = tag::TagGenerator::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let mut events = driver::event_sink::DriverEventSink::new(tx, None);
        let timeout = Duration::from_millis(100);
        reader.set_peer_timeout(Some(timeout));
        let start = Instant::now();
        let result = setup_timeout(timeout, async {
            match phase {
                0 => reader.read_greeting().await.map(|_| ()),
                1 => driver::run_one_command(
                    &mut reader,
                    &mut state,
                    &mut tags,
                    &mut events,
                    Command::Capability,
                    driver::DriverConsumer::Regular(Box::new(
                        dispatch::CapabilityConsumer::default(),
                    )),
                )
                .await
                .map(|_| ()),
                _ => {
                    driver::run_starttls_upgrade(
                        &mut reader,
                        &mut state,
                        &mut tags,
                        &mut events,
                        build_default_tls_connector().unwrap(),
                        "imap.invalid".into(),
                    )
                    .await
                }
            }
        })
        .await;
        assert!(
            matches!(result, Err(Error::Timeout { .. })),
            "phase {phase}: {result:?}"
        );
        assert_eq!(start.elapsed(), timeout, "phase {phase}");
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
        let mut reader = wire::WireReader::new(ImapStream::Memory(stream::Tracked::new(client)));
        reader.set_peer_timeout(Some(Duration::from_millis(100)));
        let mut state = state::ProtocolState::new();
        let (_, Response::Greeting(greeting)) =
            crate::codec::decode::parse_greeting(b"* OK [CAPABILITY IMAP4rev1 STARTTLS] hello\r\n")
                .unwrap()
        else {
            panic!("greeting")
        };
        state.apply_greeting(&greeting).unwrap();
        let mut tags = tag::TagGenerator::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let mut events = driver::event_sink::DriverEventSink::new(tx, None);
        let writer = tokio::spawn(async move {
            if capability {
                let command = test_support::read_line(&mut peer).await;
                for _ in 0..4 {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    if peer.write_all(b"* OK still waiting\r\n").await.is_err() {
                        return;
                    }
                }
                let _ = peer
                    .write_all(format!("{} OK done\r\n", test_support::tag_of(&command)).as_bytes())
                    .await;
            } else {
                for byte in b"* OK hello\r\n" {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    if peer.write_all(&[*byte]).await.is_err() {
                        return;
                    }
                }
            }
        });
        let start = Instant::now();
        let result = setup_timeout(Duration::from_secs(1), async {
            if capability {
                driver::run_one_command(
                    &mut reader,
                    &mut state,
                    &mut tags,
                    &mut events,
                    Command::Capability,
                    driver::DriverConsumer::Regular(Box::new(
                        dispatch::CapabilityConsumer::default(),
                    )),
                )
                .await
                .map(|_| ())
            } else {
                reader.read_greeting().await.map(|_| ())
            }
        })
        .await;
        assert!(matches!(result, Err(Error::Timeout { .. })));
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
}
