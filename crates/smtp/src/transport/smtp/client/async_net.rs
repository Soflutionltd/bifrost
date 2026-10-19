#[cfg(unix)]
use std::path::Path;
use std::{
    future::Future,
    io::Result as IoResult,
    mem,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

// Tokio's clock, not `std`'s: every timeout this deadline arms is a
// `tokio::time::timeout`, so measuring the deadline on a different clock
// makes the two disagree wherever tokio's clock is not the system one -
// under `start_paused` test time most visibly, but the principle is that a
// deadline and the timers it hands out must read the same clock.
use tokio::time::Instant;

use std::fmt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[cfg(unix)]
use tokio::net::UnixStream as TokioUnixStream;
use tokio::net::{TcpSocket, TcpStream, ToSocketAddrs};
use tokio::time::Sleep;
use tokio_native_tls::TlsStream as TokioTlsStream;

use super::DEFAULT_TLS_HANDSHAKE_TIMEOUT;
use super::metering::{MAX_OFFER, Reservation, WireMetering};
use super::net::resolved_address_filter;
use super::{ConnectionState, TlsParameters};
use crate::transport::smtp::{Error, error};

#[derive(Clone, Copy, Debug)]
pub(super) struct AsyncDeadline {
    expires_at: Option<Instant>,
}

impl AsyncDeadline {
    pub(super) fn new(timeout: Option<Duration>) -> Self {
        Self {
            expires_at: timeout.map(|timeout| Instant::now() + timeout),
        }
    }

    /// Returns `Ok(None)` for an unbounded operation, `Ok(Some(_))` while
    /// slack remains, and a timeout error once the shared deadline is spent.
    pub(super) fn remaining(self, message: &'static str) -> Result<Option<Duration>, Error> {
        let Some(expires_at) = self.expires_at else {
            return Ok(None);
        };

        expires_at
            .checked_duration_since(Instant::now())
            .ok_or_else(|| error::timeout(message))
            .map(Some)
    }

    /// Push the deadline out by `by`, leaving an unbounded deadline unbounded.
    ///
    /// The one legitimate use is excluding time this crate spent deliberately
    /// NOT touching the socket - bandwidth-throttle debt - from a deadline
    /// whose whole job is to judge the PEER. It must never be applied to the
    /// shared SETUP deadline, which is an absolute ceiling: postponing that
    /// one does not spend it, it overruns it.
    pub(super) fn postponed_by(self, by: Duration) -> Self {
        Self {
            expires_at: self.expires_at.map(|expires_at| expires_at + by),
        }
    }

    /// An unbounded deadline becomes a fresh one of `default`; a bounded one is
    /// returned untouched.
    ///
    /// For the one operation that must never hang whatever the caller asked
    /// for. Substituting only in the unbounded case is what keeps this from
    /// nesting a timer inside another: a bounded deadline already IS the timer
    /// governing the operation and is used as it stands, while an unbounded one
    /// has no timer to nest in.
    pub(super) fn or_bounded(self, default: Duration) -> Self {
        match self.expires_at {
            Some(_) => self,
            None => Self::new(Some(default)),
        }
    }

    async fn timeout<T, F>(self, message: &'static str, future: F) -> Result<T, Error>
    where
        F: Future<Output = T>,
    {
        match self.remaining(message)? {
            None => Ok(future.await),
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| error::timeout(message)),
        }
    }
}

async fn resolve_lookup_until<I, F>(
    deadline: AsyncDeadline,
    local_addr: Option<IpAddr>,
    lookup: F,
) -> Result<Vec<SocketAddr>, Error>
where
    F: Future<Output = std::io::Result<I>>,
    I: IntoIterator<Item = SocketAddr>,
{
    let addrs = deadline
        .timeout("DNS lookup timed out", lookup)
        .await?
        .map_err(error::connection_io)?;

    Ok(addrs
        .into_iter()
        .filter(|resolved_addr| resolved_address_filter(resolved_addr, local_addr))
        .collect())
}

/// A network stream
#[derive(Debug)]
pub(crate) struct AsyncNetworkStream {
    inner: InnerAsyncNetworkStream,
    state: ConnectionState,
    /// Byte accounting and cap for this connection. `disabled()` unless
    /// an account wired one in, so the non-account path is unchanged.
    metering: WireMetering,
    /// Outstanding inbound throttle debt. Polled to completion before the
    /// next read touches the socket, and never consulted by a write.
    ///
    /// Held here rather than awaited inline because both the read and
    /// write funnels are `poll_` methods that cannot await. Parking the
    /// sleep in the stream means a throttled connection yields to the
    /// runtime like any other pending IO instead of blocking the task.
    ///
    /// Inbound and outbound debt are separate slots on purpose: the meter
    /// already keeps separate buckets, and a single shared slot silently
    /// undid that, because the debt parked by the last DATA-body write
    /// gated the read of the server's reply to it. That spends the caller's
    /// per-operation read timeout on outbound debt. See "Bandwidth metering
    /// and throttling" in `reference/smtp.md`.
    throttle_in: Option<Pin<Box<Sleep>>>,
    /// Bytes this stream is admitted to write under the outbound cap and has
    /// not written yet. Installed by `admit_outbound` (or by `poll_write`
    /// itself for a caller that did not admit first), spent by accepted bytes,
    /// and never re-gated on the shared schedule: once admitted, a write only
    /// waits on its socket. Survives short writes; the funnel admits at most
    /// what the current write has left, so a successful write leaves none.
    out_allowance: Option<usize>,
    /// Length of the last offer that returned `Pending`, which the next poll
    /// must offer again. OpenSSL refuses a retry after `WANT_WRITE` that is
    /// shorter than the pending write, so neither a retune nor the allowance
    /// may shrink it. Cleared once the socket answers.
    retry_len: Option<usize>,
    /// `admit_outbound` found no cap in force for the write about to happen,
    /// so its next offer goes out uncharged even if a cap is imposed before
    /// the socket is polled. Without this, a cap appearing between admission
    /// and the timed write would make `poll_write` reserve and WAIT inside the
    /// caller's write timeout.
    uncapped_pass: bool,
    /// Admission `poll_write` is waiting on for a caller that wrote without
    /// `admit_outbound` first. The production funnel never gets here; the wait
    /// it imposes runs inside whatever timer the caller wrapped the write in.
    self_admission: Option<(Reservation, Pin<Box<Sleep>>)>,
    /// Test-injected peer-certificate DER, returned by
    /// [`Self::peer_certificate_der`] ahead of the TLS session's. The
    /// transcript harness is an in-memory duplex with no TLS, so without this
    /// seam no hermetic test can make a certificate exist, and the
    /// connection-level channel-binding gate (`plus_candidate` in `auth`)
    /// would be untestable in both directions.
    #[cfg(test)]
    test_peer_certificate_der: Option<Vec<u8>>,
}

pub(crate) trait AsyncTokioStream:
    AsyncRead + AsyncWrite + Send + Sync + Unpin + fmt::Debug
{
}

impl AsyncTokioStream for TcpStream {}
#[cfg(unix)]
impl AsyncTokioStream for TokioUnixStream {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::AsyncTranscriptStream {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::StalledPeer {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::SlowLinePeer {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::SlowSinkPeer {}

/// Represents the different types of underlying network streams
// usually only one TLS backend at a time is going to be enabled,
// so clippy::large_enum_variant doesn't make sense here
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum InnerAsyncNetworkStream {
    /// Plain Tokio 1.x TCP stream
    TokioTcp(Box<dyn AsyncTokioStream>),
    /// Plain Tokio 1.x Unix-domain stream
    #[cfg(unix)]
    TokioUnix(Box<dyn AsyncTokioStream>),
    /// Encrypted Tokio 1.x TCP stream
    TokioNativeTls(TokioTlsStream<Box<dyn AsyncTokioStream>>),
    /// Test-only scripted in-process peer.
    #[cfg(test)]
    Transcript(Box<dyn AsyncTokioStream>),
    /// Can't be built
    None,
}

impl AsyncNetworkStream {
    fn new(inner: InnerAsyncNetworkStream) -> Self {
        if let InnerAsyncNetworkStream::None = inner {
            debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
        }

        AsyncNetworkStream {
            inner,
            state: ConnectionState::Ok,
            metering: WireMetering::disabled(),
            throttle_in: None,
            out_allowance: None,
            retry_len: None,
            uncapped_pass: false,
            self_admission: None,
            #[cfg(test)]
            test_peer_certificate_der: None,
        }
    }

    /// Install byte accounting / capping for this connection. Called by
    /// the transport when an account supplied a meter sink or cap.
    pub(super) fn set_metering(&mut self, metering: WireMetering) {
        self.metering = metering;
    }

    /// Wait out any inbound throttle debt owed from the previous read.
    ///
    /// Returns `Pending` while the debt is outstanding, which parks the
    /// caller the same way a not-yet-readable socket would.
    fn poll_throttle_in(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(delay) = &mut self.throttle_in {
            std::task::ready!(delay.as_mut().poll(cx));
            self.throttle_in = None;
        }
        Poll::Ready(())
    }

    /// Admit the next write of up to `len` bytes against the shared outbound
    /// schedule, waiting for its slot.
    ///
    /// This is the half of a capped write that is this crate's OWN doing - the
    /// cap the consumer asked for - and the caller's per-operation write
    /// timeout is a statement about the PEER, so the write loop runs this
    /// BEFORE it arms that timeout. Once it returns the stream holds an
    /// allowance, and `poll_write` spends it without consulting the schedule
    /// again: no throttle wait can happen inside the timer, and a peer that
    /// never accepts a byte always reaches its timeout.
    ///
    /// Unbounded on purpose: the CALLER decides what the wait may run outside.
    /// A per-operation write timeout may be left entirely; an absolute setup
    /// deadline may not, since waiting past a ceiling is not the same as
    /// spending it. `write_stream_with_budget` makes that choice per budget.
    /// The wait itself is bounded by the reservations ahead of this one: about
    /// a second per live connection sharing the schedule, plus one.
    ///
    /// Waits in slices of at most a second and re-reads the cap after each,
    /// so a cap lifted mid-wait releases the write within a second.
    ///
    /// Cancel-safe: the reservation lives in this future and gives its time
    /// back when dropped, and the stream is not touched until the allowance is
    /// installed.
    pub(super) async fn admit_outbound(&mut self, len: usize) {
        if self.retry_len.is_some() || self.out_allowance.is_some() {
            return;
        }
        let Some(reservation) = self.metering.reserve_out(len) else {
            self.uncapped_pass = true;
            return;
        };
        loop {
            let Some(cap) = self.metering.cap_now() else {
                drop(reservation);
                self.uncapped_pass = true;
                return;
            };
            let now = Instant::now();
            if now >= reservation.ready_at() {
                self.out_allowance = Some(reservation.install(cap));
                return;
            }
            tokio::time::sleep((reservation.ready_at() - now).min(Duration::from_secs(1))).await;
        }
    }

    /// How many bytes the next socket write may offer out of `len`, or
    /// `Pending` while a self-admitted write waits for its slot.
    fn poll_outbound_offer(&mut self, cx: &mut Context<'_>, len: usize) -> Poll<usize> {
        if let Some(retry) = self.retry_len {
            return Poll::Ready(len.min(retry));
        }
        if let Some(allowance) = self.out_allowance {
            return Poll::Ready(len.min(allowance).min(MAX_OFFER));
        }
        if self.uncapped_pass {
            return Poll::Ready(len.min(MAX_OFFER));
        }
        loop {
            let Some((reservation, sleep)) = &mut self.self_admission else {
                let Some(reservation) = self.metering.reserve_out(len) else {
                    return Poll::Ready(len.min(MAX_OFFER));
                };
                let wake = reservation
                    .ready_at()
                    .min(Instant::now() + Duration::from_secs(1));
                self.self_admission = Some((reservation, Box::pin(tokio::time::sleep_until(wake))));
                continue;
            };
            std::task::ready!(sleep.as_mut().poll(cx));
            let Some(cap) = self.metering.cap_now() else {
                self.self_admission = None;
                return Poll::Ready(len.min(MAX_OFFER));
            };
            let now = Instant::now();
            if now < reservation.ready_at() {
                let wake = reservation.ready_at().min(now + Duration::from_secs(1));
                sleep.as_mut().reset(wake);
                continue;
            }
            let Some((reservation, _)) = self.self_admission.take() else {
                unreachable!("matched just above");
            };
            let allowance = reservation.install(cap);
            self.out_allowance = Some(allowance);
            return Poll::Ready(len.min(allowance));
        }
    }

    /// Await any inbound throttle debt parked by an earlier read, and report
    /// how long that took.
    ///
    /// The read-side counterpart of `admit_outbound`, and it exists for the
    /// same reason: the debt is the cap the CONSUMER asked for, while the read
    /// deadline is a statement about the PEER. It differs in what the caller
    /// does with the answer. A per-operation write timeout is a fresh duration
    /// per write, so the write loop simply waits for admission outside it; a
    /// reply read has
    /// no such thing, because the per-operation budget has been collapsed into
    /// one deadline spanning the whole reply precisely so a trickling peer
    /// cannot stretch it. There is no "outside" a deadline - only past it - so
    /// the reader pushes the reply deadline out by the returned duration
    /// instead, which leaves the peer exactly the patience it was configured
    /// with and charges our own waiting to neither side.
    ///
    /// Cancel-safe for the same reason: a dropped future leaves the `Sleep`
    /// parked in its slot, so the debt is neither lost nor double-counted. The
    /// elapsed measurement is on `tokio::time::Instant`, the clock the `Sleep`
    /// itself runs on.
    pub(super) async fn drain_inbound_throttle(&mut self) -> Duration {
        if self.throttle_in.is_none() {
            return Duration::ZERO;
        }
        let started = Instant::now();
        std::future::poll_fn(|cx| self.poll_throttle_in(cx)).await;
        started.elapsed()
    }

    /// Record `n` received bytes and park the resulting inbound debt, if any.
    ///
    /// `n` is PLAINTEXT bytes, never ciphertext, in both directions, and the
    /// gap is not always negligible. This stream sits above native-tls, so the
    /// record header, MAC and padding the TLS layer adds are invisible here.
    /// Each capped outbound write is at most one second of cap and typically
    /// becomes its own TLS record, so the bytes actually on the wire exceed the
    /// cap by roughly one record's overhead (tens of bytes) per second of cap:
    /// immaterial at a megabyte, around a third at a 100 B/s cap - the smaller
    /// the cap, the worse the ratio. Metering real wire bytes would mean putting
    /// this meter BELOW the TLS layer, which native-tls does not expose, so the
    /// gap is recorded rather than closed. A consumer sizing a cap against a
    /// hard link budget should leave headroom for it.
    fn charge_in(&mut self, n: usize) {
        if n == 0 || !self.metering.is_enabled() {
            return;
        }
        if let Some(debt) = self.metering.record_in(n) {
            self.throttle_in = Some(Box::pin(tokio::time::sleep(debt)));
        }
    }

    /// Inject a peer-certificate DER for tests. See the field's comment.
    #[cfg(test)]
    pub(crate) fn set_test_peer_certificate_der(&mut self, der: Vec<u8>) {
        self.test_peer_certificate_der = Some(der);
    }

    #[cfg(test)]
    pub(crate) fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
    ) -> Self {
        Self::new(InnerAsyncNetworkStream::Transcript(Box::new(
            crate::transport::smtp::test_support::AsyncTranscriptStream::new(transcript),
        )))
    }

    /// Wrap an arbitrary scripted stream. For peers a `Transcript` cannot
    /// express; see `AsyncSmtpConnection::from_raw_stream_for_test`.
    #[cfg(test)]
    pub(crate) fn from_raw_stream_for_test(stream: Box<dyn AsyncTokioStream>) -> Self {
        Self::new(InnerAsyncNetworkStream::Transcript(stream))
    }

    pub(super) fn state(&self) -> ConnectionState {
        self.state
    }

    pub(super) fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }

    /// Accepted coverage gap: this dial path has no hermetic test, for the
    /// same reason as its blocking twin - reaching it needs a peer on a real
    /// socket, which the crate's testing rules put out of scope, and the
    /// in-process `Transcript` harness enters below the dial. The parts that
    /// can be separated from the socket are pinned without one: the setup
    /// deadline is driven against `StalledPeer` under paused tokio time,
    /// including the TLS handshake leg through `upgrade_tls_stream`, and the
    /// STARTTLS exchange is pinned up to the handshake boundary. A real TLS
    /// handshake stays out of scope; closing that would need a production seam
    /// substituting the connector, not another test.
    pub(super) async fn connect_until<T: ToSocketAddrs>(
        server: T,
        deadline: AsyncDeadline,
        tls_parameters: Option<TlsParameters>,
        local_addr: Option<IpAddr>,
    ) -> Result<AsyncNetworkStream, Error> {
        async fn try_connect<T: ToSocketAddrs>(
            server: T,
            deadline: AsyncDeadline,
            local_addr: Option<IpAddr>,
        ) -> Result<TcpStream, Error> {
            let addrs =
                resolve_lookup_until(deadline, local_addr, tokio::net::lookup_host(server)).await?;

            let mut last_err = None;

            for addr in addrs {
                let socket = match addr.ip() {
                    IpAddr::V4(_) => TcpSocket::new_v4(),
                    IpAddr::V6(_) => TcpSocket::new_v6(),
                }
                .map_err(error::connection_io)?;
                if let Some(local_addr) = local_addr {
                    socket
                        .bind(SocketAddr::new(local_addr, 0))
                        .map_err(error::connection_io)?;
                }

                let connect_future = socket.connect(addr);
                match deadline
                    .timeout("connection timed out", connect_future)
                    .await?
                {
                    Ok(stream) => return Ok(stream),
                    Err(err) => last_err = Some(err),
                }
            }

            Err(match last_err {
                Some(last_err) => error::connection_io(last_err),
                None => error::connection("could not resolve to any supported address"),
            })
        }

        let tcp_stream = try_connect(server, deadline, local_addr).await?;
        let mut stream =
            AsyncNetworkStream::new(InnerAsyncNetworkStream::TokioTcp(Box::new(tcp_stream)));
        if let Some(tls_parameters) = tls_parameters {
            stream.upgrade_tls_until(tls_parameters, deadline).await?;
        }
        Ok(stream)
    }

    #[cfg(unix)]
    pub(super) async fn connect_unix_until(
        path: &Path,
        deadline: AsyncDeadline,
    ) -> Result<AsyncNetworkStream, Error> {
        let stream = deadline
            .timeout(
                "Unix socket connection timed out",
                TokioUnixStream::connect(path),
            )
            .await?
            .map_err(error::connection_io)?;
        Ok(AsyncNetworkStream::new(InnerAsyncNetworkStream::TokioUnix(
            Box::new(stream),
        )))
    }

    /// Upgrade to TLS, bounding the handshake by `timeout`, or by
    /// `DEFAULT_TLS_HANDSHAKE_TIMEOUT` when `timeout` is `None`.
    pub(crate) async fn upgrade_tls(
        &mut self,
        tls_parameters: TlsParameters,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        self.upgrade_tls_until(tls_parameters, AsyncDeadline::new(timeout))
            .await
    }

    /// The handshake is never unbounded: a `deadline` with no expiry (a
    /// transport built with `timeout(None)`) is replaced by a fresh
    /// `DEFAULT_TLS_HANDSHAKE_TIMEOUT` for the handshake alone. This covers both
    /// explicit STARTTLS and the implicit-TLS leg of the connect path.
    pub(super) async fn upgrade_tls_until(
        &mut self,
        tls_parameters: TlsParameters,
        deadline: AsyncDeadline,
    ) -> Result<(), Error> {
        self.state.verify()?;
        let deadline = deadline.or_bounded(DEFAULT_TLS_HANDSHAKE_TIMEOUT);

        match &self.inner {
            InnerAsyncNetworkStream::TokioTcp(_) => {
                self.state = ConnectionState::Broken;

                // get owned TcpStream
                let tcp_stream = mem::replace(&mut self.inner, InnerAsyncNetworkStream::None);
                let InnerAsyncNetworkStream::TokioTcp(tcp_stream) = tcp_stream else {
                    unreachable!()
                };

                self.inner = Self::upgrade_tls_stream(tcp_stream, tls_parameters, deadline).await?;
                self.state = ConnectionState::Ok;
                Ok(())
            }
            _ => Err(error::invalid_input(
                "STARTTLS is only supported on TCP connections",
            )),
        }
    }

    #[allow(unused_variables)]
    async fn upgrade_tls_stream(
        tcp_stream: Box<dyn AsyncTokioStream>,
        tls_parameters: TlsParameters,
        deadline: AsyncDeadline,
    ) -> Result<InnerAsyncNetworkStream, Error> {
        let domain = tls_parameters.domain().to_owned();

        use tokio_native_tls::TlsConnector;

        let connector = TlsConnector::from(tls_parameters.connector);
        let handshake = connector.connect(&domain, tcp_stream);
        let stream = deadline
            .timeout("TLS handshake timed out", handshake)
            .await?;
        Ok(InnerAsyncNetworkStream::TokioNativeTls(
            stream.map_err(error::tls)?,
        ))
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        match &self.inner {
            InnerAsyncNetworkStream::TokioTcp(_) => false,
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(_) => false,
            InnerAsyncNetworkStream::TokioNativeTls(_) => true,
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(_) => false,
            InnerAsyncNetworkStream::None => false,
        }
    }

    /// DER of the peer (server) certificate when the connection is TLS.
    ///
    /// See `NetworkStream::peer_certificate_der` for the contract; this is
    /// the Tokio sibling reaching the same native-tls session through
    /// `tokio_native_tls::TlsStream::get_ref`.
    // Plumbing for SCRAM-PLUS channel binding; the first consumer is the
    // SASL layer, so there is no in-crate caller yet.
    #[allow(dead_code)]
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        #[cfg(test)]
        if let Some(der) = &self.test_peer_certificate_der {
            return Some(der.clone());
        }
        match &self.inner {
            InnerAsyncNetworkStream::TokioNativeTls(s) => s
                .get_ref()
                .peer_certificate()
                .ok()
                .flatten()
                .and_then(|c| c.to_der().ok()),
            _ => None,
        }
    }
}

impl AsyncRead for AsyncNetworkStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let this = self.as_mut().get_mut();
        std::task::ready!(this.poll_throttle_in(cx));
        let before = buf.filled().len();
        let result = match &mut this.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_read(cx, buf),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_read(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        };
        if matches!(result, Poll::Ready(Ok(()))) {
            this.charge_in(buf.filled().len().saturating_sub(before));
        }
        result
    }
}

impl AsyncWrite for AsyncNetworkStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        let this = self.as_mut().get_mut();
        // The offer is the admitted allowance (at most one second of cap), a
        // pending TLS retry's original length, or `MAX_OFFER` when uncapped.
        // A caller that did not `admit_outbound` first is admitted here.
        let offer = std::task::ready!(this.poll_outbound_offer(cx, buf.len()));
        let buf = &buf[..offer];
        let result = match &mut this.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_write(cx, buf),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_write(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(0))
            }
        };
        match &result {
            // The allowance is spent by what the socket ACCEPTED: a short
            // write leaves the rest admitted for the next offer.
            Poll::Ready(Ok(n)) => {
                this.retry_len = None;
                this.uncapped_pass = false;
                if let Some(allowance) = this.out_allowance {
                    let left = allowance.saturating_sub(*n);
                    this.out_allowance = (left > 0).then_some(left);
                }
                if *n > 0 {
                    this.metering.record_out(*n);
                }
            }
            Poll::Pending => this.retry_len = Some(offer),
            Poll::Ready(Err(_)) => {
                this.retry_len = None;
                this.uncapped_pass = false;
            }
        }
        result
    }

    /// Admission is not consulted here: flushing writes no new plaintext,
    /// so it has nothing to admit and nothing to charge.
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_flush(cx),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_flush(cx),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.state = ConnectionState::Closed;

        match &mut self.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_shutdown(cx),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_shutdown(cx),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(test)]
mod tokio_test {
    use std::{future::pending, sync::Arc, time::Duration};

    use crate::transport::smtp::test_support::StalledPeer;

    use super::*;

    #[tokio::test(crate = "tokio")]
    async fn tokio_dns_lookup_uses_deadline() {
        let result = resolve_lookup_until(
            AsyncDeadline::new(Some(Duration::from_millis(25))),
            None,
            pending::<std::io::Result<Vec<SocketAddr>>>(),
        )
        .await;

        let error = result.unwrap_err();
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    /// Finding 6: the meter keeps separate inbound and outbound budgets, and
    /// the stream must keep them separate too. A single shared sleep slot made
    /// the debt parked by the final DATA-body write gate the read of the
    /// server's reply to it, spending the caller's read timeout on outbound
    /// throttling. The read below must complete on its first poll even though
    /// the outbound schedule is a second behind.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn an_outbound_backlog_does_not_gate_the_next_read() {
        use std::sync::{Arc, atomic::AtomicU64};

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::Transcript;

        // Six bytes per second, which is exactly one `PING` line: the write
        // spends the schedule's whole second of credit.
        let cap = Arc::new(AtomicU64::new(6));
        let metering = WireMetering::new(None, Some(cap));
        let transcript = Transcript::new("").expect("PING\r\n", "250 pong\r\n");
        let mut stream = AsyncNetworkStream::from_transcript(transcript);
        stream.set_metering(metering.clone());

        stream.write_all(b"PING\r\n").await.unwrap();
        let next_write = metering.reserve_out(6).expect("capped");
        assert!(
            !next_write.remaining().is_zero(),
            "the outbound schedule must be behind for this test to mean anything"
        );
        drop(next_write);
        assert!(
            stream.throttle_in.is_none(),
            "an outbound write must not park inbound debt"
        );

        // Poll the read exactly once. A read gated on outbound debt returns
        // Pending here; a read gated only on inbound debt returns the reply.
        let mut buf = [0_u8; 10];
        let mut read = Box::pin(stream.read(&mut buf));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let polled = read.as_mut().poll(&mut cx);
        let Poll::Ready(Ok(n)) = polled else {
            panic!("the reply read was gated on outbound throttling: {polled:?}");
        };
        assert_eq!(&buf[..n], b"250 pong\r\n");
    }

    /// Drive `write` until it has waited `held` without reaching the socket,
    /// then confirm it completes within a further 2 ms.
    ///
    /// A `Pending` at `held` is proof no byte was accepted, since admission is
    /// decided before the slice is ever offered. The completion is driven under
    /// a 1 ms timeout rather than a single poll, so the peer is allowed its
    /// own wakeup without the assertion depending on how many polls a
    /// zero-length delay takes.
    async fn assert_held_for(
        write: impl std::future::Future<Output = std::io::Result<usize>>,
        held: Duration,
    ) -> usize {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut write = std::pin::pin!(write);
        assert!(
            write.as_mut().poll(&mut cx).is_pending(),
            "the write must wait for its slot"
        );
        tokio::time::advance(held - Duration::from_millis(1)).await;
        assert!(
            write.as_mut().poll(&mut cx).is_pending(),
            "the wait must last until the slot, not merely yield once"
        );
        tokio::time::advance(Duration::from_millis(2)).await;
        tokio::time::timeout(Duration::from_millis(1), write)
            .await
            .expect("past its slot the write must make progress")
            .expect("the peer accepts")
    }

    /// Once the schedule's second of credit is spent, the next second of cap
    /// waits its turn on the same stream.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn the_next_write_waits_for_its_slot() {
        use std::sync::{Arc, atomic::AtomicU64};

        use tokio::io::AsyncWriteExt;

        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowSinkPeer;

        let cap = Arc::new(AtomicU64::new(1000));
        let mut stream = AsyncNetworkStream::from_raw_stream_for_test(Box::new(SlowSinkPeer::new(
            usize::MAX,
            Duration::ZERO,
        )));
        stream.set_metering(WireMetering::new(None, Some(cap)));

        let body = vec![b'x'; 1000];
        assert_eq!(
            stream.write(&body).await.unwrap(),
            1000,
            "the credit is free"
        );
        let written = assert_held_for(stream.write(&body), Duration::from_secs(1)).await;
        assert_eq!(written, 1000);
    }

    /// The defect admission exists to close: a FRESH connection's first write
    /// used to be ungated whatever the shared budget owed, so redialing - or
    /// just running two connections - evaded the cap entirely. Two streams
    /// share one metering exactly as a pooled transport's connections do, and
    /// the second stream's very first write must queue behind the first's.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_second_connection_cannot_spend_the_first_connections_budget() {
        use std::sync::{Arc, atomic::AtomicU64};

        use tokio::io::AsyncWriteExt;

        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowSinkPeer;

        let metering = WireMetering::new(None, Some(Arc::new(AtomicU64::new(1000))));
        let sink = || Box::new(SlowSinkPeer::new(usize::MAX, Duration::ZERO));
        let mut first = AsyncNetworkStream::from_raw_stream_for_test(sink());
        first.set_metering(metering.clone());
        let mut second = AsyncNetworkStream::from_raw_stream_for_test(sink());
        second.set_metering(metering.clone());

        // The first stream spends the whole second of credit. A second of cap
        // on the second stream then ends two seconds out and may start one
        // second from now; a stream with its own budget would start at once.
        assert_eq!(first.write(&vec![b'x'; 1000]).await.unwrap(), 1000);
        let written = assert_held_for(second.write(&[b'y'; 1000]), Duration::from_secs(1)).await;
        assert_eq!(written, 1000);
    }

    /// A peer that answers the first offer `Pending` and accepts everything
    /// after, recording the length of every offer it saw.
    #[derive(Debug, Default)]
    struct RetryRecordingPeer {
        offers: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl AsyncTokioStream for RetryRecordingPeer {}

    impl AsyncRead for RetryRecordingPeer {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<IoResult<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for RetryRecordingPeer {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<IoResult<usize>> {
            let mut offers = self.offers.lock().expect("offer log");
            offers.push(buf.len());
            if offers.len() == 1 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A write that returned `Pending` is retried at its original length, even
    /// when a cap is imposed in between. OpenSSL refuses a retry after
    /// `WANT_WRITE` that is shorter than the pending write, so re-clamping the
    /// retry to the new cap - as the per-poll clamp used to - would fail the
    /// write instead of throttling it. The uncapped offer completes as offered
    /// and uncharged: the one transition exception, bounded by `MAX_OFFER`.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_pending_retry_keeps_its_length_when_a_cap_is_imposed() {
        use std::sync::atomic::{AtomicU64, Ordering};

        use crate::transport::smtp::client::metering::{UNLIMITED_BANDWIDTH, WireMetering};

        let cap = Arc::new(AtomicU64::new(UNLIMITED_BANDWIDTH));
        let peer = RetryRecordingPeer::default();
        let offers = Arc::clone(&peer.offers);
        let mut stream = AsyncNetworkStream::from_raw_stream_for_test(Box::new(peer));
        stream.set_metering(WireMetering::new(None, Some(Arc::clone(&cap))));

        let body = vec![b'x'; 100_000];
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(
            Pin::new(&mut stream)
                .poll_write(&mut cx, &body)
                .is_pending(),
            "the peer answers the first offer Pending"
        );
        cap.store(10, Ordering::Relaxed);
        let Poll::Ready(Ok(written)) = Pin::new(&mut stream).poll_write(&mut cx, &body) else {
            panic!("the retry must go out at once, not wait for a slot at the new cap");
        };
        assert_eq!(written, 100_000);
        assert_eq!(
            *offers.lock().expect("offer log"),
            vec![100_000, 100_000],
            "the retry must offer the pending length, not one second of the new cap"
        );
    }

    /// Under a cap, a single write offers at most one second of it, so a body
    /// is admitted a second at a time and a retune governs the next second
    /// rather than whatever one huge offer covered.
    ///
    /// Paused time is honest here: the schedule runs on tokio's clock, the
    /// same one the admission wait and this assertion's `Instant::now()` read.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_capped_write_offers_at_most_one_second_of_cap() {
        use std::sync::{Arc, atomic::AtomicU64};

        use tokio::io::AsyncWriteExt;

        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowSinkPeer;

        // A sink that accepts everything offered, immediately.
        let cap = Arc::new(AtomicU64::new(100));
        let mut stream = AsyncNetworkStream::from_raw_stream_for_test(Box::new(SlowSinkPeer::new(
            usize::MAX,
            Duration::ZERO,
        )));
        stream.set_metering(WireMetering::new(None, Some(cap)));

        let body = vec![b'x'; 100_000];
        let started = Instant::now();
        let mut offset = 0;
        for _ in 0..3 {
            let written = stream.write(&body[offset..]).await.unwrap();
            assert_eq!(
                written, 100,
                "the offer must be clamped to one second of cap"
            );
            offset += written;
        }
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(2),
            "the first second is credit; each one after waits its turn"
        );
    }

    /// Finding 4b: this used to bind a `TcpListener` and park a thread in a
    /// 250ms `thread::sleep` so the handshake would outlive a 50ms deadline.
    /// `StalledPeer` is the in-memory equivalent - it swallows the opaque
    /// `ClientHello` and never answers - so the deadline is the only thing that
    /// can resolve the handshake, with no socket, no thread and no sleep.
    #[tokio::test(crate = "tokio")]
    async fn tokio_tls_handshake_uses_deadline() {
        let tls_parameters = TlsParameters::new("localhost".to_owned()).unwrap();
        let result = AsyncNetworkStream::upgrade_tls_stream(
            Box::new(StalledPeer),
            tls_parameters,
            AsyncDeadline::new(Some(Duration::from_millis(25))),
        )
        .await;

        let error = result.unwrap_err();
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    /// Drive `upgrade_tls` against a peer that swallows the `ClientHello` and
    /// never answers, and return how long paused time says it took to fail.
    async fn stalled_handshake_elapsed(timeout: Option<Duration>) -> Duration {
        let mut stream =
            AsyncNetworkStream::new(InnerAsyncNetworkStream::TokioTcp(Box::new(StalledPeer)));
        let tls_parameters = TlsParameters::new("localhost".to_owned()).unwrap();

        let started = Instant::now();
        // An outer bound far above the default, so an unbounded handshake fails
        // here instead of hanging: under paused time it costs nothing when the
        // inner bound holds, and it is the only timer an unbounded one leaves.
        let error = tokio::time::timeout(
            DEFAULT_TLS_HANDSHAKE_TIMEOUT * 10,
            stream.upgrade_tls(tls_parameters, timeout),
        )
        .await
        .expect("the handshake must be bounded, not left to hang")
        .expect_err("a stalled handshake must not complete");
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
        started.elapsed()
    }

    /// A transport built with `timeout(None)` still ends a wedged handshake,
    /// and at the documented default rather than at some arbitrary point. Paused
    /// time makes the elapsed value the finding: an unbounded handshake would
    /// hang here instead of failing.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn an_unconfigured_tls_handshake_is_bounded_by_the_default() {
        assert_eq!(
            stalled_handshake_elapsed(None).await,
            DEFAULT_TLS_HANDSHAKE_TIMEOUT
        );
    }

    /// The default substitutes for a MISSING timeout only. A configured one is
    /// used as given, shorter than the default here, so the default is not a
    /// floor or a second timer stacked on the first.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_configured_tls_handshake_timeout_is_not_replaced_by_the_default() {
        let configured = Duration::from_secs(5);
        assert!(configured < DEFAULT_TLS_HANDSHAKE_TIMEOUT);
        assert_eq!(
            stalled_handshake_elapsed(Some(configured)).await,
            configured
        );
    }

    /// The setup-deadline shape: `AsyncDeadline::new(None)` yields no slack, and
    /// the implicit-TLS leg of `connect_until` hands exactly that to
    /// `upgrade_tls_until`. It must be bounded there too.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn an_unbounded_setup_deadline_still_bounds_the_handshake() {
        let mut stream =
            AsyncNetworkStream::new(InnerAsyncNetworkStream::TokioTcp(Box::new(StalledPeer)));
        let tls_parameters = TlsParameters::new("localhost".to_owned()).unwrap();

        let started = Instant::now();
        let error = tokio::time::timeout(
            DEFAULT_TLS_HANDSHAKE_TIMEOUT * 10,
            stream.upgrade_tls_until(tls_parameters, AsyncDeadline::new(None)),
        )
        .await
        .expect("the handshake must be bounded, not left to hang")
        .expect_err("a stalled handshake must not complete");
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
        assert_eq!(started.elapsed(), DEFAULT_TLS_HANDSHAKE_TIMEOUT);
    }
}
