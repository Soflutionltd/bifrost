#![allow(clippy::wildcard_imports)]
use tokio::net::TcpStream;

use bifrost_types::TransmissionState;

use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Connection lifecycle
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn connect_with_tls_connector_metered(
        host: &str,
        port: u16,
        tls_mode: TlsMode,
        tls_connector: native_tls::TlsConnector,
        timeout: Duration,
        meter_sink: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
        leases: &super::LeaseHandoff,
    ) -> Result<Self, Error> {
        debug!(host, port, ?tls_mode, "connecting to IMAP server");

        let unsent = TransmissionState::Unsent;
        let tcp = tokio::time::timeout(timeout, TcpStream::connect((host, port)))
            .await
            .map_err(|_| Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                attempt: Some(unsent),
            })?
            .map_err(|e| Error::Transport {
                kind: crate::error::TransportFailure::Io(crate::error::IoFailure(
                    std::sync::Arc::new(e),
                )),
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
                .map_err(|_| Error::Transport {
                    kind: crate::error::TransportFailure::Timeout,
                    attempt: Some(unsent),
                })?
                .map_err(|e| Error::Transport {
                    kind: crate::error::TransportFailure::Io(crate::error::IoFailure(
                        std::sync::Arc::new(std::io::Error::other(e)),
                    )),
                    attempt: Some(unsent),
                })?;
            ImapStream::Tls(tls_stream)
        } else {
            ImapStream::Plain(tcp)
        };

        Self::from_wire_reader(
            wire::WireReader::new_metered(stream, meter_sink, bandwidth_budget),
            host,
            tls_mode,
            tls_connector,
            timeout,
            leases,
        )
        .await
    }

    /// Start ownership before reading any IMAP bytes. Cancellation while waiting
    /// for readiness aborts setup, including before the driver's first poll.
    pub(super) async fn from_wire_reader(
        wire_reader: wire::WireReader,
        host: &str,
        tls_mode: TlsMode,
        tls_connector: native_tls::TlsConnector,
        timeout: Duration,
        leases: &super::LeaseHandoff,
    ) -> Result<Self, Error> {
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(256);
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
        let (state_tx, state_rx) = tokio::sync::watch::channel(Default::default());
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let inbound_bytes = wire_reader.metering().inbound_counter();
        let tls_active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            tls_mode.uses_implicit_tls(),
        ));
        let driver_task = std::sync::Arc::new(super::DriverTask::spawn(
            driver::setup::setup_then_drive(
                wire_reader,
                driver::setup::Setup {
                    host: host.to_owned(),
                    tls_mode,
                    tls_connector,
                    timeout,
                    tls_active: std::sync::Arc::clone(&tls_active),
                },
                cmd_rx,
                state_tx,
                driver::event_sink::DriverEventSink::new(events_tx, None),
                ready_tx,
            ),
            inbound_bytes,
            leases.take(),
        ));
        let mut setup_guard = SetupGuard(Some(std::sync::Arc::clone(&driver_task)));
        let conn = Self {
            cmd_tx,
            state_rx,
            events_rx: tokio::sync::Mutex::new(events_rx),
            driver_handle: driver_task,
            #[cfg(test)]
            host: host.to_owned(),
            tls_active,
            abandoned: std::sync::atomic::AtomicBool::new(false),
        };
        match ready_rx.await {
            Ok(Ok(())) => {
                setup_guard.0.take();
                Ok(conn)
            }
            result => {
                let error = match result {
                    Ok(Err(error)) => error,
                    Err(_) => conn.observe_driver_panic(TransmissionState::Unsent).await,
                    Ok(Ok(())) => unreachable!("handled readiness success"),
                };
                conn.driver_handle.abort_and_join().await;
                Err(error)
            }
        }
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
    /// Returns [`crate::error::TransportFailure::Timeout`] if admission or execution exceeds `timeout`.
    /// Returns [`crate::error::TransportFailure::Io`] if the `setsockopt` call fails.
    /// Returns [`crate::error::TransportFailure::DriverGone`] if the driver task has exited.
    pub async fn set_keepalive(
        &self,
        keepalive: TcpKeepalive,
        timeout: Duration,
    ) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = super::driver::DriverCommand::SetKeepalive {
            keepalive,
            result_tx,
        };
        // An enterprise user expects stalled account setup to return for retry.
        // One caller budget covers both queue admission and the reply. No IMAP
        // bytes are sent, even if this idempotent option is applied later.
        tokio::time::timeout_at(deadline, async {
            if self.cmd_tx.send(dcmd).await.is_err() {
                return Err(self.observe_driver_panic(TransmissionState::Unsent).await);
            }
            match result_rx.await {
                Ok(result) => result,
                Err(_) => Err(self.observe_driver_panic(TransmissionState::Unsent).await),
            }
        })
        .await
        .map_err(|_| Error::timeout().with_attempt(TransmissionState::Unsent))?
    }

    /// DER of the peer certificate, or `None` for a non-TLS transport,
    /// unavailable DER, or an exited driver. Expiry is an error: treating a
    /// stalled lookup as an absent certificate could downgrade authentication.
    pub(crate) async fn peer_certificate_der(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Vec<u8>>, Error> {
        tokio::time::timeout_at(deadline, async {
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            let dcmd = super::driver::DriverCommand::PeerCertificate { result_tx };
            if self.cmd_tx.send(dcmd).await.is_err() {
                return None;
            }
            result_rx.await.unwrap_or(None)
        })
        .await
        .map_err(|_| Error::timeout().with_attempt(TransmissionState::Unsent))
    }

    /// If the driver task has terminated, return an error describing
    /// why (panic message if panicked, or `DriverGone` if exited
    /// cleanly), so the caller sees the real reason instead of a generic
    /// `Disconnected`.
    ///
    /// `phase` is how far the caller's command got, and only the CALLER
    /// knows it: `Unsent` when the driver never claimed the command (a
    /// closed channel at admission, or a reply dropped while still queued),
    /// `InFlight` when the driver had claimed the command and the reply
    /// channel dropped (so it may have been written - "possibly
    /// transmitted", the convention `timeout_inflight` uses). Non-wire
    /// requests (`set_keepalive`) remain `Unsent`: applying a socket option
    /// transmits no IMAP command. A non-idempotent command
    /// in the second phase must be read back, not declared failed, so the
    /// phase is stamped on every outcome here - including when another
    /// caller already took the join handle, since the evidence cannot depend
    /// on being first.
    pub(super) async fn observe_driver_panic(&self, phase: TransmissionState) -> Error {
        let mut guard = self.driver_handle.handle.lock().await;
        let Some(handle) = guard.as_mut() else {
            return Error::driver_gone_at(phase);
        };
        // `handle.is_finished()` avoids blocking if still running.
        if !handle.is_finished() {
            // The cmd_tx failure may be a TOCTOU race with the driver exiting.
            drop(guard);
            return Error::driver_gone_at(phase);
        }
        let result = handle.await;
        guard.take();
        match result {
            Err(join_err) if join_err.is_panic() => {
                let panic_msg = join_err
                    .into_panic()
                    .downcast::<String>()
                    .map(|s| *s)
                    .or_else(|p| p.downcast::<&'static str>().map(|s| s.to_string()))
                    .unwrap_or_else(|_| "driver panicked (payload not a String)".to_string());
                Error::Internal {
                    kind: crate::error::InternalFailure::Panicked(panic_msg),
                    mid_exchange: Some(phase),
                }
            }
            Ok(()) | Err(_) => Error::driver_gone_at(phase),
        }
    }

    #[cfg(test)]
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
        self.admit_command(&Command::StartTls)?;

        // Validate server name before submitting so malformed input fails
        // without involving the driver.
        validate_tls_server_name(&self.host)?;

        debug!("upgrading to TLS via STARTTLS");
        self.submit_upgrade(
            driver::UpgradePayload::StartTls {
                tls_connector,
                server_name: self.host.clone(),
                tls_active: std::sync::Arc::clone(&self.tls_active),
            },
            Some(tokio::time::Instant::now() + timeout),
        )
        .await?;
        Ok(())
    }
}

/// Setup owns a transport and retained pool leases even before readiness.
/// Destroying the caller must destroy that task, not leave it waiting on a peer.
struct SetupGuard(Option<std::sync::Arc<super::DriverTask>>);

impl Drop for SetupGuard {
    fn drop(&mut self) {
        if let Some(driver) = &self.0 {
            driver.abort();
        }
    }
}

/// Build the default native-tls connector.
pub(super) fn build_default_tls_connector() -> Result<native_tls::TlsConnector, Error> {
    native_tls::TlsConnector::new().map_err(|e| Error::Transport {
        kind: crate::error::TransportFailure::Io(crate::error::IoFailure(Arc::new(
            std::io::Error::other(e),
        ))),
        attempt: None,
    })
}

pub(super) fn validate_tls_server_name(host: &str) -> Result<(), Error> {
    if host.is_empty() {
        return Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput("TLS server name must not be empty".into()),
        ));
    }
    if host.bytes().any(|b| b == 0 || b.is_ascii_whitespace()) {
        return Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!("invalid TLS server name: {host:?}").into(),
            ),
        ));
    }
    Ok(())
}
