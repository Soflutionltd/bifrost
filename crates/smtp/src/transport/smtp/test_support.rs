//! In-memory transcript harness. Everything the drivers, transports and pool
//! are tested against is scripted here: there are deliberately no listeners,
//! no threads and no sleeps in this module, so a test can never fail because
//! of a port, a socket path or the scheduler.

use std::collections::VecDeque;
#[cfg(test)]
use std::time::Duration;

// Public only through the feature-gated re-export; unit tests also build it
// without that feature.
#[cfg_attr(not(feature = "test-support"), allow(unreachable_pub))]
#[derive(Clone, Debug)]
pub struct Transcript {
    shared: std::sync::Arc<std::sync::Mutex<TranscriptState>>,
}

#[derive(Debug)]
struct TranscriptState {
    pending_server_bytes: VecDeque<u8>,
    steps: VecDeque<TranscriptStep>,
    /// The peer accepted the command and then went silent forever. Reads park
    /// instead of reporting EOF, which is what a timeout or a cancellation
    /// test needs to observe.
    stalled: bool,
    /// The pending server bytes arrived as one TCP segment, so a single read
    /// hands out all of them rather than one reply line.
    coalesced: bool,
    /// The peer closed the connection once the scripted bytes were handed out.
    /// Reads then report EOF and writes fail the way a real closed socket
    /// does, so a driver cannot keep talking to a hung-up peer.
    closed: bool,
    /// The peer never completes a shutdown, so `poll_shutdown` parks forever.
    /// Async-only: it is set by `stall_shutdown` and read by the `AsyncWrite`
    /// impl, both of which are gated, so the field is dead without the feature.
    #[cfg(feature = "tokio")]
    shutdown_stalled: bool,
    /// Every read timeout the blocking driver armed on this stream, in order.
    /// The transcript never enforces them - it has no clock - but recording
    /// them is how the per-reply deadline (as opposed to a per-line one) is
    /// observable hermetically.
    #[cfg(test)]
    read_timeouts: Vec<Option<Duration>>,
}

#[derive(Debug)]
struct TranscriptStep {
    client: Vec<u8>,
    server: Vec<u8>,
    stall: bool,
    coalesced: bool,
    close: bool,
    matched: usize,
    #[cfg(test)]
    generated_server: Option<GeneratedServerReply>,
}

#[cfg(test)]
type GeneratedServerReply = fn(&[u8]) -> Vec<u8>;

#[cfg_attr(not(feature = "test-support"), allow(unreachable_pub))]
impl Transcript {
    pub fn new(greeting: impl AsRef<[u8]>) -> Self {
        Self::with_state(greeting.as_ref().iter().copied().collect(), false)
    }

    /// A peer that accepts the connection and never sends its banner.
    #[cfg(all(test, feature = "tokio"))]
    pub(super) fn silent() -> Self {
        Self::with_state(VecDeque::new(), true)
    }

    fn with_state(pending_server_bytes: VecDeque<u8>, stalled: bool) -> Self {
        Self {
            shared: std::sync::Arc::new(std::sync::Mutex::new(TranscriptState {
                pending_server_bytes,
                steps: VecDeque::new(),
                stalled,
                coalesced: false,
                closed: false,
                #[cfg(feature = "tokio")]
                shutdown_stalled: false,
                #[cfg(test)]
                read_timeouts: Vec::new(),
            })),
        }
    }

    pub fn expect(self, client: impl AsRef<[u8]>, server: impl AsRef<[u8]>) -> Self {
        self.push(client, server, false, false, false)
    }

    /// Accept one complete client write and derive the reply from its bytes.
    /// Used when an in-process protocol transcript must echo a client nonce or
    /// verify a response whose contents are intentionally unpredictable.
    #[cfg(test)]
    pub(super) fn expect_generated(self, server: fn(&[u8]) -> Vec<u8>) -> Self {
        self.shared
            .lock()
            .expect("transcript lock")
            .steps
            .push_back(TranscriptStep {
                client: Vec::new(),
                server: Vec::new(),
                stall: false,
                coalesced: false,
                close: false,
                matched: 0,
                generated_server: Some(server),
            });
        self
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(super) fn stall_shutdown(self) -> Self {
        self.shared
            .lock()
            .expect("transcript lock")
            .shutdown_stalled = true;
        self
    }

    /// A peer that answers with `server` and then hangs up.
    ///
    /// `server` may be a half-written reply line (or empty): the scripted
    /// bytes are handed out, then reads report EOF and any further client
    /// write fails the way a write to a closed socket does. That is the
    /// difference from `expect_then_stall`, where the peer stays
    /// connected and simply never answers.
    #[cfg(test)]
    pub(super) fn expect_then_close(
        self,
        client: impl AsRef<[u8]>,
        server: impl AsRef<[u8]>,
    ) -> Self {
        self.push(client, server, false, false, true)
    }

    /// A step whose reply bytes arrive as one segment, the way a real peer's
    /// TCP stack coalesces adjacent replies. A `BufReader` over the stream
    /// therefore prefetches all of them, which is how the driver can observe
    /// bytes it did not ask for without a blocking read.
    #[cfg(test)]
    pub(super) fn expect_coalesced(
        self,
        client: impl AsRef<[u8]>,
        server: impl AsRef<[u8]>,
    ) -> Self {
        self.push(client, server, false, true, false)
    }

    /// Accept the client bytes, then never answer.
    #[cfg(all(test, feature = "tokio"))]
    pub(super) fn expect_then_stall(self, client: impl AsRef<[u8]>) -> Self {
        self.push(client, b"", true, false, false)
    }

    fn push(
        self,
        client: impl AsRef<[u8]>,
        server: impl AsRef<[u8]>,
        stall: bool,
        coalesced: bool,
        close: bool,
    ) -> Self {
        self.shared
            .lock()
            .expect("transcript lock")
            .steps
            .push_back(TranscriptStep {
                client: client.as_ref().to_vec(),
                server: server.as_ref().to_vec(),
                stall,
                coalesced,
                close,
                matched: 0,
                #[cfg(test)]
                generated_server: None,
            });
        self
    }

    /// Every read timeout armed on this transcript's stream since the last
    /// call, in order, clearing the record.
    #[cfg(test)]
    pub(super) fn take_read_timeouts(&self) -> Vec<Option<Duration>> {
        std::mem::take(&mut self.shared.lock().expect("transcript lock").read_timeouts)
    }

    pub(super) fn stream(&self) -> TranscriptStream {
        TranscriptStream {
            transcript: self.clone(),
        }
    }

    pub fn assert_exhausted(&self) {
        let state = self.shared.lock().expect("transcript lock");
        assert!(
            state.steps.is_empty(),
            "SMTP transcript has {} unconsumed client step(s)",
            state.steps.len()
        );
        assert!(
            state.pending_server_bytes.is_empty(),
            "SMTP transcript has {} unconsumed server byte(s)",
            state.pending_server_bytes.len()
        );
    }
}

/// A synchronous side of an in-process SMTP transcript.
///
/// Each client write exactly matches one scripted step. Its paired server
/// bytes become readable only after that write, so transcripts preserve the
/// request-response sequence a real peer can produce.
#[derive(Clone, Debug)]
pub(super) struct TranscriptStream {
    transcript: Transcript,
}

impl TranscriptStream {
    /// Records the timeout the driver armed. The transcript has no clock, so
    /// nothing is enforced; the record is the observable.
    #[cfg(test)]
    pub(super) fn record_read_timeout(&self, duration: Option<Duration>) {
        self.transcript
            .shared
            .lock()
            .expect("transcript lock")
            .read_timeouts
            .push(duration);
    }
}

impl std::io::Read for TranscriptStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut state = self
            .transcript
            .shared
            .lock()
            .map_err(|_| std::io::Error::other("SMTP transcript lock poisoned"))?;
        if state.pending_server_bytes.is_empty() {
            if state.closed {
                // A closed peer reports EOF, never a stall: the driver must
                // classify the truncated response instead of waiting.
                return Ok(0);
            }
            if state.stalled {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            return Ok(0);
        }
        // Hand out at most one reply line per read. A `BufReader` over this
        // stream would otherwise prefetch a whole pipelined reply group, empty
        // `pending_server_bytes` after the first parsed response, and let the
        // driver write the next command without draining the rest - which is
        // exactly the sequencing bug these transcripts exist to catch.
        let line_end = if state.coalesced {
            state.pending_server_bytes.len()
        } else {
            state
                .pending_server_bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(state.pending_server_bytes.len(), |index| index + 1)
        };
        let count = buf.len().min(line_end);
        for destination in &mut buf[..count] {
            *destination = state
                .pending_server_bytes
                .pop_front()
                .ok_or_else(|| std::io::Error::other("SMTP transcript server bytes disappeared"))?;
        }
        Ok(count)
    }
}

impl std::io::Write for TranscriptStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut state = self
            .transcript
            .shared
            .lock()
            .map_err(|_| std::io::Error::other("SMTP transcript lock poisoned"))?;
        if state.closed {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        if !state.pending_server_bytes.is_empty() {
            return Err(std::io::Error::other(
                "SMTP client wrote before draining the scripted server replies",
            ));
        }
        #[cfg(test)]
        if let Some(generate_server) = state.steps.front().and_then(|step| step.generated_server) {
            let server = generate_server(buf);
            let step = state.steps.pop_front().expect("generated reply step");
            state.pending_server_bytes.extend(server);
            state.stalled = step.stall;
            state.coalesced = step.coalesced;
            state.closed = step.close;
            return Ok(buf.len());
        }
        let Some(step) = state.steps.front_mut() else {
            return Err(std::io::Error::other(
                "SMTP transcript exhausted by client write",
            ));
        };
        let count = buf.len().min(step.client.len() - step.matched);
        if step.client[step.matched..step.matched + count] != buf[..count] {
            return Err(std::io::Error::other(format!(
                "unexpected SMTP client bytes: expected {:?} at {}, got {:?}",
                String::from_utf8_lossy(&step.client),
                step.matched,
                String::from_utf8_lossy(buf)
            )));
        }
        step.matched += count;
        if step.matched != step.client.len() {
            return Ok(count);
        }
        let step = state.steps.pop_front().expect("completed step");
        state.pending_server_bytes.extend(step.server);
        state.stalled = step.stall;
        state.coalesced = step.coalesced;
        state.closed = step.close;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn scram_server_first_reply(client_line: &[u8]) -> Vec<u8> {
    let client_line = std::str::from_utf8(client_line).expect("SCRAM client-first is UTF-8");
    let encoded = client_line
        .strip_suffix("\r\n")
        .expect("SCRAM client-first ends in CRLF");
    let decoded = crate::base64::decode(encoded).expect("SCRAM client-first is base64");
    let decoded = String::from_utf8(decoded).expect("SCRAM client-first payload is UTF-8");
    let client_nonce = decoded
        .strip_prefix("n,,n=user,r=")
        .expect("transcript uses unbound SCRAM for user")
        .to_owned();
    assert!(!client_nonce.is_empty());
    assert!(!client_nonce.contains(','));

    let server_first = format!("r={client_nonce}server,s=QSXCR+Q6sek8bf92,i=4096");
    format!("334 {}\r\n", crate::base64::encode(server_first)).into_bytes()
}

#[cfg(test)]
pub(super) fn scram_enhanced_success_reply(client_line: &[u8]) -> Vec<u8> {
    let client_line = std::str::from_utf8(client_line).expect("SCRAM client-final is UTF-8");
    let encoded = client_line
        .strip_suffix("\r\n")
        .expect("SCRAM client-final ends in CRLF");
    let decoded = crate::base64::decode(encoded).expect("SCRAM client-final is base64");
    let decoded = String::from_utf8(decoded).expect("SCRAM client-final payload is UTF-8");
    assert!(decoded.starts_with("c=biws,r="), "got {decoded}");

    let server_nonce = decoded
        .split(',')
        .nth(1)
        .and_then(|attribute| attribute.strip_prefix("r="))
        .expect("SCRAM client-final has a nonce");
    assert!(server_nonce.ends_with("server"));
    assert!(
        decoded
            .split(',')
            .any(|attribute| attribute.starts_with("p="))
    );
    let client_nonce = server_nonce
        .strip_suffix("server")
        .expect("server nonce extends client nonce");
    let client_first_bare = format!("n=user,r={client_nonce}");
    let server_first = format!("r={server_nonce},s=QSXCR+Q6sek8bf92,i=4096");
    let password =
        bifrost_sasl::prepare_scram_password("pencil").expect("transcript password is valid");
    let (_, signature) = bifrost_sasl::scram_client_final(
        bifrost_sasl::ScramHash::Sha256,
        &password,
        client_nonce,
        &client_first_bare,
        &server_first,
        &bifrost_sasl::ScramChannelBinding::None,
    )
    .expect("transcript server-first is valid");
    let server_final = format!("v={}", crate::base64::encode(signature));
    format!(
        "235 2.7.0 {} authenticated\r\n",
        crate::base64::encode(server_final)
    )
    .into_bytes()
}

/// A peer that trickles one reply line per `gap`, swallowing every write.
///
/// The in-memory stand-in for a pathological server that answers, just very
/// slowly. Each line is preceded by a real (paused-clock) sleep, so under
/// `start_paused` tokio time a reply of N lines costs N gaps of virtual time
/// with no wall-clock cost. That is what distinguishes a per-reply read
/// deadline from a per-line one: the per-line arm gives every line a fresh
/// full timeout and never fires, while one deadline across the reply does.
#[cfg(all(test, feature = "tokio"))]
#[derive(Debug)]
pub(super) struct SlowLinePeer {
    lines: VecDeque<Vec<u8>>,
    gap: Duration,
    /// Armed on the FIRST `poll_read`, not at construction. Arming it in `new`
    /// credits the setup time between building the peer and the first read
    /// against the first line's delay, so a test that constructs the peer early
    /// gets a first line for free - a contract of "a line every gap, counted
    /// from whenever you happened to construct me". Same shape as
    /// `SlowSinkPeer` below.
    next: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

#[cfg(all(test, feature = "tokio"))]
impl SlowLinePeer {
    pub(super) fn new<I: IntoIterator<Item = &'static str>>(lines: I, gap: Duration) -> Self {
        Self {
            lines: lines
                .into_iter()
                .map(|line| line.as_bytes().to_vec())
                .collect(),
            gap,
            next: None,
        }
    }

    /// Same peer, for lines a test builds rather than writes out - notably
    /// lines long enough to move a bandwidth meter, which no `&'static str`
    /// literal wants to be.
    pub(super) fn from_owned_lines<I: IntoIterator<Item = Vec<u8>>>(
        lines: I,
        gap: Duration,
    ) -> Self {
        Self {
            lines: lines.into_iter().collect(),
            gap,
            next: None,
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
impl tokio::io::AsyncRead for SlowLinePeer {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let gap = self.gap;
        let sleep = self
            .next
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(gap)));
        std::task::ready!(std::future::Future::poll(sleep.as_mut(), cx));
        let Some(line) = self.lines.pop_front() else {
            // Out of lines: park forever, the way a peer that simply stops
            // talking does. Only the caller's own deadline resumes this.
            return std::task::Poll::Pending;
        };
        self.next = Some(Box::pin(tokio::time::sleep(gap)));
        buf.put_slice(&line);
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(all(test, feature = "tokio"))]
impl tokio::io::AsyncWrite for SlowLinePeer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// The lazily armed gap, pinned for the line peer the same way `slow_sink_tests`
/// pins it for the sink: a harness whose delay starts at construction silently
/// discounts a test's setup time from the first line, which is a trap for any
/// test that builds its peer before the exchange it is timing.
///
/// Ablation: arm the `Sleep` in `SlowLinePeer::new` again and the first read
/// completes at once, because the idle gaps below already elapsed it.
#[cfg(all(test, feature = "tokio"))]
mod slow_line_tests {
    use super::SlowLinePeer;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    #[tokio::test(start_paused = true)]
    async fn a_slow_line_peer_counts_its_first_gap_from_the_first_read() {
        let gap = Duration::from_secs(5);
        let mut peer = SlowLinePeer::new(["250-one\r\n", "250 two\r\n"], gap);
        // Stand-in for whatever setup a test does between building the peer
        // and reading from it.
        tokio::time::sleep(gap * 2).await;

        let mut line = [0_u8; 32];
        let started = tokio::time::Instant::now();
        let read = peer.read(&mut line).await.expect("the peer sends a line");
        assert_eq!(&line[..read], b"250-one\r\n");
        assert_eq!(
            tokio::time::Instant::now() - started,
            gap,
            "the first line costs one whole gap, measured from the read"
        );

        let started = tokio::time::Instant::now();
        let read = peer.read(&mut line).await.expect("the peer sends a line");
        assert_eq!(&line[..read], b"250 two\r\n");
        assert_eq!(
            tokio::time::Instant::now() - started,
            gap,
            "and every later line costs the same gap"
        );
    }
}

/// A peer that drains an upload slowly but steadily, or not at all.
///
/// `new(chunk, gap)` accepts `chunk` bytes every `gap`, which is a slow link:
/// the transfer takes `len / chunk` gaps of virtual time while every
/// individual write makes progress. `stalled(...)` never accepts a byte and
/// registers no waker, which is a wedged peer. Together they separate the two
/// things a write timeout has to tell apart - the async half must survive the
/// first and still fail the second, which is what `SO_SNDTIMEO` gives the
/// blocking half for free by being per `write(2)`.
#[cfg(all(test, feature = "tokio"))]
#[derive(Debug)]
pub(super) struct SlowSinkPeer {
    /// Bytes accepted per `gap`. `None` means the peer never accepts anything.
    chunk: Option<usize>,
    gap: Duration,
    /// How many more writes may be accepted before the peer wedges for good.
    ///
    /// A test that needs throttle debt AND a peer that then accepts nothing
    /// cannot use two peers - the stream owns one - so the transition has to
    /// live here: `accepting(n, ...)` builds the debt with n writes and is a
    /// `stalled()` peer from then on.
    accepts: usize,
    /// Armed on the FIRST `poll_write`, not at construction. Arming it in
    /// `new` credits the setup time between building the peer and the first
    /// write against the first chunk's delay, so a test that constructs the
    /// peer early gets a first write that costs nothing - a contract of "every
    /// gap, counted from whenever you happened to construct me".
    next: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

#[cfg(all(test, feature = "tokio"))]
impl SlowSinkPeer {
    pub(super) fn new(chunk: usize, gap: Duration) -> Self {
        Self {
            chunk: Some(chunk),
            gap,
            next: None,
            accepts: usize::MAX,
        }
    }

    /// Accepts `accepts` writes of up to `chunk` bytes each, instantly, and
    /// wedges after that.
    pub(super) fn accepting(accepts: usize, chunk: usize) -> Self {
        Self {
            chunk: Some(chunk),
            gap: Duration::ZERO,
            next: None,
            accepts,
        }
    }

    /// A peer that accepts nothing, ever.
    pub(super) fn stalled() -> Self {
        Self {
            chunk: None,
            gap: Duration::ZERO,
            next: None,
            accepts: 0,
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
impl tokio::io::AsyncRead for SlowSinkPeer {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

#[cfg(all(test, feature = "tokio"))]
impl tokio::io::AsyncWrite for SlowSinkPeer {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let Some(chunk) = self.chunk.filter(|_| self.accepts > 0) else {
            // No waker: only the caller's own timeout may resume this.
            return std::task::Poll::Pending;
        };
        let gap = self.gap;
        let sleep = self
            .next
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(gap)));
        std::task::ready!(std::future::Future::poll(sleep.as_mut(), cx));
        self.next = Some(Box::pin(tokio::time::sleep(gap)));
        self.accepts = self.accepts.saturating_sub(1);
        std::task::Poll::Ready(Ok(chunk.min(buf.len())))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// The lazily armed gap, pinned: a harness whose delay starts at construction
/// silently discounts a test's setup time from the first chunk, which is a trap
/// for any test that builds its peer before the exchange it is timing.
///
/// Ablation: arm the `Sleep` in `SlowSinkPeer::new` again and the first write
/// completes at once, because the idle gaps below already elapsed it.
#[cfg(all(test, feature = "tokio"))]
mod slow_sink_tests {
    use super::SlowSinkPeer;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    #[tokio::test(start_paused = true)]
    async fn a_slow_sink_counts_its_first_gap_from_the_first_write() {
        let gap = Duration::from_secs(5);
        let mut peer = SlowSinkPeer::new(4, gap);
        // Stand-in for whatever setup a test does between building the peer
        // and writing to it.
        tokio::time::sleep(gap * 2).await;

        let started = tokio::time::Instant::now();
        let written = peer.write(b"abcd").await.expect("the peer accepts a chunk");
        assert_eq!(written, 4);
        assert_eq!(
            tokio::time::Instant::now() - started,
            gap,
            "the first chunk costs one whole gap, measured from the write"
        );

        let started = tokio::time::Instant::now();
        let written = peer.write(b"efgh").await.expect("the peer accepts a chunk");
        assert_eq!(written, 4);
        assert_eq!(
            tokio::time::Instant::now() - started,
            gap,
            "and every later chunk costs the same gap"
        );
    }
}

/// A peer that accepts every byte written to it and never answers.
///
/// The in-memory stand-in for "a listener that accepts the TCP connection and
/// then sleeps": it is what a setup-deadline test needs, because the handshake
/// bytes it swallows are opaque (a TLS `ClientHello` is not scriptable) and the
/// only thing that may resume the caller is the caller's own deadline. Reads
/// park with no waker registered, exactly as `AsyncTranscriptStream` does for
/// a stalled step, so nothing but a timeout or a cancellation can wake it.
#[cfg(all(test, feature = "tokio"))]
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct StalledPeer;

#[cfg(all(test, feature = "tokio"))]
impl tokio::io::AsyncRead for StalledPeer {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

#[cfg(all(test, feature = "tokio"))]
impl tokio::io::AsyncWrite for StalledPeer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
#[derive(Clone, Debug)]
pub(super) struct AsyncTranscriptStream {
    inner: TranscriptStream,
}

#[cfg(feature = "tokio")]
impl AsyncTranscriptStream {
    pub(super) fn new(transcript: Transcript) -> Self {
        Self {
            inner: transcript.stream(),
        }
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for AsyncTranscriptStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        let read = match std::io::Read::read(&mut self.inner, unfilled) {
            Ok(read) => read,
            // A stalled peer parks the read. No waker is registered on
            // purpose: the only thing that may resume this task is the
            // caller's own timeout or cancellation, which is exactly the
            // behavior these transcripts pin.
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return std::task::Poll::Pending;
            }
            Err(error) => return std::task::Poll::Ready(Err(error)),
        };
        buf.advance(read);
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncWrite for AsyncTranscriptStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(std::io::Write::flush(&mut self.inner))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self
            .inner
            .transcript
            .shared
            .lock()
            .expect("transcript lock")
            .shutdown_stalled
        {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(Ok(()))
        }
    }
}

/// Creates an independent in-memory peer for each new pooled connection.
#[cfg(feature = "test-support")]
#[derive(Clone)]
pub struct ScriptedConnector(std::sync::Arc<dyn Fn() -> Transcript + Send + Sync>);

#[cfg(feature = "test-support")]
impl std::fmt::Debug for ScriptedConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScriptedConnector")
    }
}

#[cfg(feature = "test-support")]
impl ScriptedConnector {
    /// The factory must return a fresh transcript, rather than cloning one.
    pub fn new(factory: impl Fn() -> Transcript + Send + Sync + 'static) -> Self {
        Self(std::sync::Arc::new(factory))
    }

    pub(crate) fn connect(&self) -> Transcript {
        (self.0)()
    }
}

#[cfg(all(test, feature = "test-support"))]
mod connector_tests {
    use super::*;
    use crate::transport::smtp::extension::ClientId;
    use crate::{AsyncLmtpTransport, AsyncSmtpTransport, TokioExecutor};

    #[tokio::test]
    async fn scripted_connector_creates_independent_smtp_and_lmtp_connections() {
        for _ in 0..2 {
            let connector = ScriptedConnector::new(|| {
                Transcript::new("220 peer\r\n")
                    .expect("EHLO client\r\n", "250 peer\r\n")
                    .expect("NOOP\r\n", "250 ok\r\n")
            });
            let smtp = AsyncSmtpTransport::<TokioExecutor>::builder_dangerous("invalid")
                .hello_name(ClientId::Domain("client".into()))
                .scripted_connector(connector.clone())
                .build::<TokioExecutor>();
            assert!(smtp.test_connection().await.unwrap());
            let smtp = AsyncSmtpTransport::<TokioExecutor>::builder_dangerous("invalid")
                .hello_name(ClientId::Domain("client".into()))
                .scripted_connector(connector)
                .build::<TokioExecutor>();
            assert!(smtp.test_connection().await.unwrap());
        }
        let lmtp = AsyncLmtpTransport::<TokioExecutor>::builder_dangerous("invalid")
            .hello_name(ClientId::Domain("client".into()))
            .scripted_connector(ScriptedConnector::new(|| {
                Transcript::new("220 peer\r\n")
                    .expect("LHLO client\r\n", "250 peer\r\n")
                    .expect("NOOP\r\n", "250 ok\r\n")
            }))
            .build::<TokioExecutor>();
        assert!(lmtp.test_connection().await.unwrap());
    }

    #[tokio::test]
    async fn scripted_connector_rejects_tls_before_creating_peer() {
        for starttls in [false, true] {
            let connector = ScriptedConnector::new(|| panic!("TLS must reject before dial"));
            let smtp = if starttls {
                AsyncSmtpTransport::<TokioExecutor>::starttls_relay("example.com").unwrap()
            } else {
                AsyncSmtpTransport::<TokioExecutor>::relay("example.com").unwrap()
            }
            .scripted_connector(connector.clone())
            .build::<TokioExecutor>();
            assert!(smtp.test_connection().await.is_err());
            let parameters =
                crate::transport::smtp::client::TlsParameters::new("example.com".into()).unwrap();
            let tls = if starttls {
                crate::transport::smtp::client::Tls::Required(parameters)
            } else {
                crate::transport::smtp::client::Tls::Wrapper(parameters)
            };
            let lmtp = AsyncLmtpTransport::<TokioExecutor>::builder_dangerous("invalid")
                .tls(tls)
                .scripted_connector(connector)
                .build::<TokioExecutor>();
            assert!(lmtp.test_connection().await.is_err());
        }
    }
}
