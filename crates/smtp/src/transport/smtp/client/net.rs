use std::{
    io::{self, Read, Write},
    net::{IpAddr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

#[cfg(unix)]
use std::os::fd::OwnedFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::path::Path;

use native_tls::TlsStream;
#[cfg(unix)]
use socket2::SockAddr;
use socket2::{Domain, Protocol, Type};

use super::metering::{MAX_OFFER, WireMetering};
use super::{ConnectionState, DEFAULT_TLS_HANDSHAKE_TIMEOUT, TlsParameters};
use crate::transport::smtp::{Error, error};

/// A network stream
pub(crate) struct NetworkStream {
    inner: Option<InnerNetworkStream>,
    state: ConnectionState,
    /// Byte accounting and cap for this connection. `disabled()` unless
    /// an account wired one in.
    ///
    /// This transport is blocking by construction, so the cap is honoured
    /// by sleeping the calling thread - the same semantic its caller has
    /// already accepted by choosing the blocking API. The async transport
    /// parks a timer instead.
    metering: WireMetering,
    /// Thread time slept paying INBOUND throttle debt since the reply reader
    /// last collected it.
    ///
    /// The blocking reader bounds a whole reply with a deadline it re-arms
    /// `SO_RCVTIMEO` from, so unlike the write side - where `SO_SNDTIMEO` is
    /// per `write(2)` and a `charge` between two writes costs nothing - a
    /// `charge` between two reply lines is spent straight out of the reply
    /// budget. That budget is a statement about the PEER, and this sleep is
    /// this crate's own doing at the consumer's request, so the reader takes
    /// the accumulated value and pushes its deadline out by it. The async half
    /// reaches the same place by postponing its `AsyncDeadline` around
    /// `drain_inbound_throttle`.
    inbound_throttle_slept: Duration,
    /// Bytes this stream is admitted to write under the outbound cap and has
    /// not written yet. The blocking twin of `AsyncNetworkStream`'s field of
    /// the same name: reserved against the schedule every connection of the
    /// transport shares, waited for by sleeping the thread BEFORE the write
    /// (outside `SO_SNDTIMEO`, so the cap never counts against a write
    /// deadline), and spent by accepted bytes. `write_all` re-offers the rest
    /// of its buffer, so a short write leaves the remainder for its next call.
    out_allowance: Option<usize>,
}

/// Represents the different types of underlying network streams
// usually only one TLS backend at a time is going to be enabled,
// so clippy::large_enum_variant doesn't make sense here
#[allow(clippy::large_enum_variant)]
enum InnerNetworkStream {
    /// Plain TCP stream
    Tcp(TcpStream),
    /// Plain Unix-domain stream
    #[cfg(unix)]
    Unix(UnixStream),
    /// Encrypted TCP stream
    NativeTls(TlsStream<TcpStream>),
    /// Test-only scripted in-process peer.
    #[cfg(test)]
    Transcript(crate::transport::smtp::test_support::TranscriptStream),
}

impl NetworkStream {
    fn new(inner: InnerNetworkStream) -> Self {
        NetworkStream {
            inner: Some(inner),
            state: ConnectionState::Ok,
            metering: WireMetering::disabled(),
            inbound_throttle_slept: Duration::ZERO,
            out_allowance: None,
        }
    }

    /// How many of `len` bytes the next write may offer, sleeping the thread
    /// until the shared outbound schedule admits it.
    ///
    /// Sleeps in slices of at most a second, re-reading the cap after each, so
    /// a cap lifted mid-wait releases the write within a second. The
    /// reservation gives its time back if the cap is lifted before it is used.
    fn admit_outbound(&mut self, len: usize) -> usize {
        if let Some(allowance) = self.out_allowance {
            return len.min(allowance);
        }
        let Some(reservation) = self.metering.reserve_out(len) else {
            return len.min(MAX_OFFER);
        };
        loop {
            let Some(cap) = self.metering.cap_now() else {
                return len.min(MAX_OFFER);
            };
            let wait = reservation.remaining();
            if wait.is_zero() {
                let allowance = reservation.install(cap);
                self.out_allowance = Some(allowance);
                return len.min(allowance);
            }
            std::thread::sleep(wait.min(Duration::from_secs(1)));
        }
    }

    /// Collect and reset the inbound throttle time slept since the last call.
    /// See the field.
    pub(super) fn take_inbound_throttle_slept(&mut self) -> Duration {
        std::mem::replace(&mut self.inbound_throttle_slept, Duration::ZERO)
    }

    /// Install byte accounting / capping for this connection.
    pub(super) fn set_metering(&mut self, metering: WireMetering) {
        self.metering = metering;
    }

    /// Record `n` received bytes and sleep off any inbound debt the cap
    /// imposes. Blocking, matching this transport's contract.
    ///
    /// `n` is PLAINTEXT bytes, never ciphertext, in both directions. This
    /// stream sits above
    /// native-tls, so the record header, MAC and padding the TLS layer adds are
    /// never seen or charged, and the bytes actually on the wire exceed the cap
    /// by that overhead - negligible for large transfers, proportionally worse
    /// the smaller the cap and the smaller the writes. Metering real wire bytes
    /// would mean putting this meter BELOW the TLS layer, which native-tls does
    /// not expose, so the gap is recorded rather than closed. A consumer sizing
    /// a cap against a hard link budget should leave headroom for it.
    fn charge_in(&mut self, n: usize) {
        if n == 0 || !self.metering.is_enabled() {
            return;
        }
        if let Some(debt) = self.metering.record_in(n) {
            std::thread::sleep(debt);
            self.inbound_throttle_slept = self.inbound_throttle_slept.saturating_add(debt);
        }
    }

    #[cfg(test)]
    pub(crate) fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
    ) -> Self {
        Self::new(InnerNetworkStream::Transcript(transcript.stream()))
    }

    pub(super) fn state(&self) -> ConnectionState {
        self.state
    }

    pub(super) fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }

    /// Shutdowns the connection
    pub(crate) fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        self.state = ConnectionState::Closed;

        match self.inner.as_ref() {
            Some(InnerNetworkStream::Tcp(s)) => s.shutdown(how),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.shutdown(how),
            Some(InnerNetworkStream::NativeTls(s)) => s.get_ref().shutdown(how),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(_)) => Ok(()),
            None => Ok(()),
        }
    }

    /// Accepted coverage gap: this dial path, and the TLS handshake
    /// `upgrade_tls` performs on it, have no hermetic test. Reaching either
    /// needs a peer on a real socket - a listener and a port - which the
    /// crate's testing rules put out of scope, and the in-process `Transcript`
    /// harness enters below the dial by construction. Everything above the
    /// socket is covered instead: address filtering is pinned directly through
    /// `resolved_address_filter`, and the STARTTLS command exchange is pinned
    /// up to the handshake boundary. Closing the gap would require a
    /// production seam that lets a test substitute the connector, not a new
    /// test.
    ///
    /// Setup budgets, none of them a shared deadline: address resolution gets
    /// `timeout` (see `resolve_within`), each candidate address then gets
    /// `timeout` for its own `connect_timeout`, and the implicit-TLS handshake
    /// gets `timeout` per socket read or write (see `upgrade_tls_bounded`).
    pub(crate) fn connect<T: ToSocketAddrs + Send + 'static>(
        server: T,
        timeout: Option<Duration>,
        tls_parameters: Option<&TlsParameters>,
        local_addr: Option<IpAddr>,
    ) -> Result<NetworkStream, Error> {
        fn try_connect<T: ToSocketAddrs + Send + 'static>(
            server: T,
            timeout: Option<Duration>,
            local_addr: Option<IpAddr>,
        ) -> Result<TcpStream, Error> {
            let addrs = resolve_within(
                move || Ok(server.to_socket_addrs()?.collect::<Vec<_>>()),
                timeout,
            )?
            .into_iter()
            .filter(|resolved_addr| resolved_address_filter(resolved_addr, local_addr));

            let mut last_err = None;

            for addr in addrs {
                let socket = socket2::Socket::new(
                    Domain::for_address(addr),
                    Type::STREAM,
                    Some(Protocol::TCP),
                )
                .map_err(error::connection_io)?;
                bind_local_address(&socket, &addr, local_addr)?;

                if let Some(timeout) = timeout {
                    match socket.connect_timeout(&addr.into(), timeout) {
                        Ok(()) => return Ok(socket.into()),
                        Err(err) => last_err = Some(err),
                    }
                } else {
                    match socket.connect(&addr.into()) {
                        Ok(()) => return Ok(socket.into()),
                        Err(err) => last_err = Some(err),
                    }
                }
            }

            Err(match last_err {
                Some(last_err) => error::connection_io(last_err),
                None => error::connection("could not resolve to any address"),
            })
        }

        let tcp_stream = try_connect(server, timeout, local_addr)?;
        let mut stream = NetworkStream::new(InnerNetworkStream::Tcp(tcp_stream));
        if let Some(tls_parameters) = tls_parameters {
            stream.upgrade_tls_bounded(tls_parameters, timeout)?;
        }
        Ok(stream)
    }

    /// The implicit-TLS handshake at connect time, never unbounded.
    ///
    /// This runs before `SmtpConnection::set_timeout` has armed the socket, so
    /// without this the handshake would block the thread on a peer that accepts
    /// the TCP connection and then never answers the `ClientHello`, whatever
    /// timeout was configured. The budget is the same one explicit STARTTLS
    /// draws from: the configured operation timeout, armed as `SO_RCVTIMEO` /
    /// `SO_SNDTIMEO`, so it bounds each read or write of the handshake and not
    /// the handshake as a whole. With no configured timeout the default
    /// handshake bound is armed for the handshake alone and the unbounded
    /// setting put back afterwards.
    fn upgrade_tls_bounded(
        &mut self,
        tls_parameters: &TlsParameters,
        configured: Option<Duration>,
    ) -> Result<(), Error> {
        let bound = configured.unwrap_or(DEFAULT_TLS_HANDSHAKE_TIMEOUT);
        self.set_read_timeout(Some(bound)).map_err(error::network)?;
        self.set_write_timeout(Some(bound))
            .map_err(error::network)?;
        let result = self.upgrade_tls(tls_parameters);
        if configured.is_none() {
            // Restore even after a failure. A failed handshake has usually
            // taken the socket with it, so this fails too and there is nothing
            // to restore; the connection is broken either way.
            let read = self.set_read_timeout(None);
            let write = self.set_write_timeout(None);
            result?;
            read.map_err(error::network)?;
            write.map_err(error::network)?;
            return Ok(());
        }
        result
    }

    #[cfg(unix)]
    pub(crate) fn connect_unix(
        path: &Path,
        timeout: Option<Duration>,
    ) -> Result<NetworkStream, Error> {
        let addr = SockAddr::unix(path).map_err(error::connection_io)?;
        let socket =
            socket2::Socket::new(Domain::UNIX, Type::STREAM, None).map_err(error::connection_io)?;
        if let Some(timeout) = timeout {
            socket
                .connect_timeout(&addr, timeout)
                .map_err(error::connection_io)?;
        } else {
            socket.connect(&addr).map_err(error::connection_io)?;
        }
        let stream = UnixStream::from(OwnedFd::from(socket));
        Ok(NetworkStream::new(InnerNetworkStream::Unix(stream)))
    }

    pub(crate) fn upgrade_tls(&mut self, tls_parameters: &TlsParameters) -> Result<(), Error> {
        self.state.verify()?;

        match self.inner.as_ref() {
            Some(InnerNetworkStream::Tcp(_)) => {
                self.state = ConnectionState::Broken;

                let Some(InnerNetworkStream::Tcp(tcp_stream)) = self.inner.take() else {
                    unreachable!()
                };

                self.inner = Some(Self::upgrade_tls_impl(tcp_stream, tls_parameters)?);
                self.state = ConnectionState::Ok;
                Ok(())
            }
            _ => Err(error::invalid_input(
                "STARTTLS is only supported on TCP connections",
            )),
        }
    }

    fn upgrade_tls_impl(
        tcp_stream: TcpStream,
        tls_parameters: &TlsParameters,
    ) -> Result<InnerNetworkStream, Error> {
        let stream = tls_parameters
            .connector
            .connect(tls_parameters.domain(), tcp_stream)
            .map_err(error::tls_handshake)?;
        Ok(InnerNetworkStream::NativeTls(stream))
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        match self.inner.as_ref() {
            Some(InnerNetworkStream::Tcp(_)) => false,
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(_)) => false,
            Some(InnerNetworkStream::NativeTls(_)) => true,
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(_)) => false,
            None => false,
        }
    }

    /// DER of the peer (server) certificate when the connection is TLS.
    ///
    /// Returns `None` on plaintext, Unix-domain, and disconnected
    /// streams, and when native-tls cannot produce the certificate DER.
    /// The DER is the input to RFC 5929 `tls-server-end-point` channel
    /// binding; computing the binding is the SASL layer's job, not this
    /// accessor's.
    // Plumbing for SCRAM-PLUS channel binding; the first consumer is the
    // SASL layer, so there is no in-crate caller yet.
    #[allow(dead_code)]
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        match self.inner.as_ref() {
            Some(InnerNetworkStream::NativeTls(s)) => s
                .peer_certificate()
                .ok()
                .flatten()
                .and_then(|c| c.to_der().ok()),
            _ => None,
        }
    }

    pub(crate) fn set_read_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(stream)) => stream.set_read_timeout(duration),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(stream)) => stream.set_read_timeout(duration),
            Some(InnerNetworkStream::NativeTls(stream)) => {
                stream.get_ref().set_read_timeout(duration)
            }
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => {
                // No clock to enforce it against, but recording what the
                // driver armed is what makes the per-reply read deadline
                // observable in a hermetic test.
                s.record_read_timeout(duration);
                Ok(())
            }
            None => Err(not_connected()),
        }
    }

    /// Set write timeout for IO calls
    pub(crate) fn set_write_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(stream)) => stream.set_write_timeout(duration),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(stream)) => stream.set_write_timeout(duration),

            Some(InnerNetworkStream::NativeTls(stream)) => {
                stream.get_ref().set_write_timeout(duration)
            }
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(_)) => Ok(()),
            None => Err(not_connected()),
        }
    }
}

impl Read for NetworkStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(s)) => s.read(buf),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.read(buf),
            Some(InnerNetworkStream::NativeTls(s)) => s.read(buf),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => s.read(buf),
            None => Err(not_connected()),
        }?;
        self.charge_in(read);
        Ok(read)
    }
}

impl Write for NetworkStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let offer = self.admit_outbound(buf.len());
        let buf = &buf[..offer];
        let written = match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(s)) => s.write(buf),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.write(buf),
            Some(InnerNetworkStream::NativeTls(s)) => s.write(buf),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => s.write(buf),
            None => Err(not_connected()),
        }?;
        // The allowance is spent by what the socket accepted, not by what
        // was offered; the sink sees only accepted bytes.
        if let Some(allowance) = self.out_allowance {
            let left = allowance.saturating_sub(written);
            self.out_allowance = (left > 0).then_some(left);
        }
        if written > 0 {
            self.metering.record_out(written);
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(s)) => s.flush(),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.flush(),
            Some(InnerNetworkStream::NativeTls(s)) => s.flush(),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => s.flush(),
            None => Err(not_connected()),
        }
    }
}

fn not_connected() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "network stream is not connected",
    )
}

/// If the local address is set, binds the socket to this address.
/// If local address is not set, then destination address is required to determine the default
/// local address on some platforms.
/// See: <https://github.com/hyperium/hyper/blob/faf24c6ad8eee1c3d5ccc9a4d4835717b8e2903f/src/client/connect/http.rs#L560>
fn bind_local_address(
    socket: &socket2::Socket,
    dst_addr: &SocketAddr,
    local_addr: Option<IpAddr>,
) -> Result<(), Error> {
    match local_addr {
        Some(local_addr) => {
            socket
                .bind(&SocketAddr::new(local_addr, 0).into())
                .map_err(error::connection_io)?;
        }
        _ => {
            if cfg!(windows) {
                // Windows requires a socket be bound before calling connect
                let any: SocketAddr = match dst_addr {
                    SocketAddr::V4(_) => ([0, 0, 0, 0], 0).into(),
                    SocketAddr::V6(_) => ([0, 0, 0, 0, 0, 0, 0, 0], 0).into(),
                };
                socket.bind(&any.into()).map_err(error::connection_io)?;
            }
        }
    }
    Ok(())
}

/// Run a blocking address resolution under `timeout`.
///
/// `ToSocketAddrs` has no timeout parameter and `getaddrinfo` cannot be
/// cancelled, so with a timeout the resolution runs on a helper thread and this
/// waits for its result for at most `timeout`. On expiry the caller gets a
/// `Timeout` error, but the helper thread is NOT stopped: it stays parked in the
/// resolver until the OS resolver returns or gives up, then drops its result
/// and exits. A stalled resolver therefore leaks one thread per timed-out
/// attempt for as long as the stall lasts. That is the honest price of a bound
/// on an uncancellable call. With no timeout the resolution runs inline on the
/// calling thread, as `timeout(None)` means "do not limit my operations".
///
/// The budget is the configured timeout for resolution alone; the connect that
/// follows draws its own.
fn resolve_within<F>(resolve: F, timeout: Option<Duration>) -> Result<Vec<SocketAddr>, Error>
where
    F: FnOnce() -> io::Result<Vec<SocketAddr>> + Send + 'static,
{
    let Some(timeout) = timeout else {
        return resolve().map_err(error::connection_io);
    };
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("bifrost-smtp-resolve".to_owned())
        .spawn(move || {
            // The receiver is gone once the wait timed out; nobody wants this.
            let _ = tx.send(resolve());
        })
        .map_err(error::connection_io)?;
    match rx.recv_timeout(timeout) {
        Ok(resolved) => resolved.map_err(error::connection_io),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            Err(error::timeout("address resolution timed out"))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(error::connection(
            "address resolver thread ended without a result",
        )),
    }
}

/// When we have an iterator of resolved remote addresses, we must filter them to be the same
/// protocol as the local address binding. If no local address is set, then all will be matched.
pub(crate) fn resolved_address_filter(
    resolved_addr: &SocketAddr,
    local_addr: Option<IpAddr>,
) -> bool {
    match local_addr {
        Some(local_addr) => match resolved_addr.ip() {
            IpAddr::V4(_) => local_addr.is_ipv4(),
            IpAddr::V6(_) => local_addr.is_ipv6(),
        },
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::{DEFAULT_TLS_HANDSHAKE_TIMEOUT, NetworkStream, resolve_within};
    use crate::transport::smtp::client::TlsParameters;
    use crate::transport::smtp::test_support::Transcript;

    /// A resolver that cannot finish until the test releases it, so the bound
    /// is the only way `resolve_within` can return. No sleep is involved: an
    /// unbounded implementation blocks here until the watchdog kills the test.
    #[test]
    fn a_stalled_resolution_times_out_at_the_bound() {
        let (release, parked) = mpsc::channel::<()>();
        let error = resolve_within(
            move || {
                let _ = parked.recv();
                Ok(Vec::new())
            },
            Some(Duration::ZERO),
        )
        .expect_err("a resolution that never finishes must not be waited for");
        assert!(error.is_timeout(), "expected a timeout, got: {error}");
        drop(release);
    }

    #[test]
    fn a_resolution_inside_the_bound_returns_its_addresses() {
        let addr: SocketAddr = ([127, 0, 0, 1], 25).into();
        let resolved = resolve_within(move || Ok(vec![addr]), Some(Duration::from_secs(30)))
            .expect("a prompt resolver succeeds");
        assert_eq!(resolved, vec![addr]);
    }

    #[test]
    fn a_resolver_failure_is_a_connection_error_not_a_timeout() {
        let error = resolve_within(
            || Err(std::io::Error::other("no such host")),
            Some(Duration::from_secs(30)),
        )
        .expect_err("the resolver failed");
        assert!(error.is_connection(), "got: {error}");
    }

    #[test]
    fn a_resolver_that_dies_without_a_result_is_not_a_timeout() {
        let error = resolve_within(
            || -> std::io::Result<Vec<SocketAddr>> { panic!("resolver panicked") },
            Some(Duration::from_secs(30)),
        )
        .expect_err("no result was produced");
        assert!(error.is_connection(), "got: {error}");
    }

    #[test]
    fn an_unconfigured_resolution_runs_inline() {
        let caller = std::thread::current().id();
        let resolved = resolve_within(
            move || {
                assert_eq!(std::thread::current().id(), caller);
                Ok(Vec::new())
            },
            None,
        )
        .expect("inline resolution");
        assert!(resolved.is_empty());
    }

    /// Arm-and-restore around the implicit-TLS handshake, read back off the
    /// transcript. The transcript stream cannot complete a handshake, but the
    /// timeouts are armed before it is attempted, so the budget the handshake
    /// draws from is observable without a socket. A real stalled handshake
    /// needs a peer on a socket and stays out of scope.
    fn implicit_tls_armed_timeouts(configured: Option<Duration>) -> Vec<Option<Duration>> {
        let transcript = Transcript::new("");
        let mut stream = NetworkStream::from_transcript(transcript.clone());
        let tls = TlsParameters::new("smtp.example".to_owned()).unwrap();
        stream
            .upgrade_tls_bounded(&tls, configured)
            .expect_err("the in-process transcript stream cannot complete a TLS handshake");
        transcript.take_read_timeouts()
    }

    #[test]
    fn the_implicit_tls_handshake_draws_the_configured_timeout() {
        let configured = Duration::from_secs(7);
        let armed = implicit_tls_armed_timeouts(Some(configured));
        assert_eq!(armed, vec![Some(configured)], "armed: {armed:?}");
    }

    #[test]
    fn an_unconfigured_implicit_tls_handshake_is_bounded_and_then_unbounded_again() {
        let armed = implicit_tls_armed_timeouts(None);
        assert_eq!(
            armed,
            vec![Some(DEFAULT_TLS_HANDSHAKE_TIMEOUT), None],
            "armed: {armed:?}"
        );
    }
}
