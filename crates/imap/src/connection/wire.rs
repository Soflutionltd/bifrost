//! Single wire-reader primitive (RFC 3501 Section2.2.2).
//!
//! Private to `crate::connection`. `WireReader::new` and the internal
//! fields are unreachable outside this module. The ONLY construction
//! site outside this file is `ImapConnection::new_wire_reader`, which
//! itself is only called by the connection constructor and the stream
//! upgrade handler.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use bifrost_net::{BandwidthBudget, BandwidthDirection, MeterSinkHandle, Reservation};
use bytes::BytesMut;
use tracing::{trace, warn};

use crate::codec::decode::parse_response_utf8;
use crate::error::Error;
use crate::types::Response;

use super::ImapStream;

/// Low-level wire reader that owns an IMAP stream and a parse buffer.
///
/// All wire reads flow through this type. The buffer and stream are
/// private  -  no external code can pull bytes or reset state. The only
/// way to "reset" the reader is to consume it via [`into_stream`](Self::into_stream)
/// and construct a fresh reader on the (possibly upgraded) stream.
pub(crate) struct WireReader {
    stream: ImapStream,
    buf: BytesMut,
    metering: WireMetering,
    compress_read: Option<(Reservation, usize)>,
    compress_boundary: bool,
}

impl WireReader {
    /// Create a new reader over the given stream with an 8 KiB initial
    /// parse buffer (RFC 3501 Section2.2.2  -  responses are line-oriented, so
    /// 8 KiB covers the vast majority of single-response reads without
    /// reallocation).
    #[cfg(test)]
    pub(super) fn new(stream: ImapStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            metering: WireMetering::disabled(),
            compress_read: None,
            compress_boundary: false,
        }
    }

    pub(super) fn new_metered(
        stream: ImapStream,
        sink: Option<MeterSinkHandle>,
        bandwidth_budget: Option<BandwidthBudget>,
    ) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            metering: WireMetering::new(sink, bandwidth_budget),
            compress_read: None,
            compress_boundary: false,
        }
    }

    pub(super) fn with_metering(stream: ImapStream, metering: WireMetering) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            metering,
            compress_read: None,
            compress_boundary: false,
        }
    }

    pub(super) fn metering(&self) -> WireMetering {
        self.metering.clone()
    }

    pub(super) fn set_peer_timeout(&mut self, timeout: Option<Duration>) {
        self.metering.peer_timeout = timeout.map(PeerBudget::new);
    }

    pub(super) fn set_peer_budget(&mut self, budget: Option<PeerBudget>) {
        self.metering.peer_timeout = budget;
    }

    pub(in crate::connection) fn peer_budget(&self) -> Option<PeerBudget> {
        self.metering.peer_timeout.clone()
    }

    pub(in crate::connection) fn begin_compress_boundary(&mut self) {
        self.compress_boundary = true;
    }

    pub(in crate::connection) fn finish_compress_boundary(&mut self, suffix: usize) {
        self.finish_compress_read(suffix);
        self.compress_boundary = false;
    }

    fn finish_compress_read(&mut self, suffix: usize) {
        if let Some((grant, read)) = self.compress_read.take() {
            let plaintext = read.saturating_sub(suffix);
            grant.finish(plaintext);
            self.metering.record_in(plaintext);
        }
    }

    /// Read and parse a single IMAP response from the wire.
    ///
    /// Accumulates bytes into the internal buffer, runs the nom parser, and
    /// returns the first complete response. This is the only response reader
    /// in the crate.
    ///
    /// * On parse error -> `Err(Error::Parse)`
    /// * On I/O error -> `Err(Error::Io)`
    /// * On EOF (zero-byte read) -> `Err(Error::Closed)`
    ///
    /// `utf8_mode` should be `true` when `UTF8=ACCEPT` (RFC 6855
    /// Section 3) has been enabled or `IMAP4rev2` is active (RFC 9051
    /// Section 7).
    pub(crate) async fn read_one(&mut self, utf8_mode: bool) -> Result<Response, Error> {
        loop {
            // Try to parse a complete response from the buffer.
            if let Some(resp) = self.try_parse_response_inner(utf8_mode)? {
                return Ok(resp);
            }
            // Need more data from the wire.
            let n = self.read_admitted().await?;
            if n == 0 {
                return Err(Error::closed());
            }
        }
    }

    /// Read and parse a server greeting from the wire (RFC 3501 Section7.1).
    ///
    /// Like [`read_one`](Self::read_one), but uses the greeting parser
    /// instead of the general response parser. The greeting is the very
    /// first message from the server  -  `* OK`, `* PREAUTH`, or `* BYE`.
    pub(crate) async fn read_greeting(&mut self) -> Result<Response, Error> {
        loop {
            if let Some(resp) = self.try_parse_greeting_inner()? {
                return Ok(resp);
            }
            let n = self.read_admitted().await?;
            if n == 0 {
                return Err(Error::closed());
            }
        }
    }

    /// Admission precedes peer I/O. A pending IDLE read pre-spends its slot
    /// without holding a lock or an exclusive turn on the account budget.
    async fn read_admitted(&mut self) -> Result<usize, Error> {
        self.finish_compress_read(0);
        let grant = self
            .metering
            .budget
            .admit(BandwidthDirection::Inbound, 8192)
            .await;
        let read = self.stream.read_buf_limited(&mut self.buf, grant.granted());
        let n = peer_io(self.metering.peer_timeout.clone(), read).await?;
        if self.compress_boundary {
            // The last read can contain compressed bytes after the tagged OK.
            // Keep its grant until the upgrade identifies that uncharged suffix.
            self.compress_read = Some((grant, n));
        } else {
            grant.finish(n);
            self.metering.record_in(n);
        }
        Ok(n)
    }

    /// Write raw bytes to the wire and flush.
    /// Does not read. Does not parse. Does not mutate buf.
    ///
    /// Errors carry no transmission evidence: only the caller knows where
    /// its command began, so it compares [`written`](Self::written) readings
    /// to stamp them.
    pub(crate) async fn write_all(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.write_chunks(std::slice::from_ref(&bytes)).await
    }

    /// Write every chunk in order, then flush once.
    ///
    /// One segment of an encoded command is written this way: its chunks are
    /// the caller's own allocations (a message body is never copied into a
    /// command buffer), and the single flush ends the segment. Errors carry
    /// no transmission evidence, as for [`write_all`](Self::write_all).
    pub(crate) async fn write_chunks<B: AsRef<[u8]>>(&mut self, chunks: &[B]) -> Result<(), Error> {
        for chunk in chunks {
            let chunk = chunk.as_ref();
            // An empty literal is legal and has nothing to write.
            if chunk.is_empty() {
                continue;
            }
            let mut remaining = chunk;
            while !remaining.is_empty() {
                let n = self.write_admitted(remaining).await?;
                remaining = &remaining[n..];
            }
        }
        peer_io(self.metering.peer_timeout.clone(), self.stream.flush()).await?;
        Ok(())
    }

    async fn write_admitted(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let grant = self
            .metering
            .budget
            .admit(BandwidthDirection::Outbound, bytes.len())
            .await;
        // On error or cancellation the offered grant stays spent.
        let n = peer_io(
            self.metering.peer_timeout.clone(),
            self.stream.write(&bytes[..grant.granted()]),
        )
        .await?;
        grant.finish(n);
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "IMAP write made no progress",
            )
            .into());
        }
        self.metering.record_out(n);
        Ok(n)
    }

    /// Octets the socket beneath this reader has accepted since the
    /// connection opened, saturating at `u64::MAX`.
    ///
    /// The transmission-evidence reading: a command whose sender sees this
    /// unchanged since the command began wrote nothing the peer can have
    /// received. See `stream::Tracked`.
    pub(crate) fn written(&self) -> u64 {
        self.stream.written()
    }

    /// Whether the internal parse buffer is empty.
    pub(crate) fn buffer_is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Set TCP keepalive on the underlying socket (RFC 1122 Section 4.2.3.6).
    ///
    /// Delegates to [`ImapStream::set_keepalive`]. Safe to call at any time  -
    /// does not touch the read buffer or affect wire protocol state.
    pub(in crate::connection) fn set_keepalive(
        &self,
        ka: &super::TcpKeepalive,
    ) -> Result<(), Error> {
        self.stream.set_keepalive(ka)
    }

    /// DER of the peer certificate. Delegates to
    /// [`ImapStream::peer_certificate_der`]. Read-only; touches no buffer
    /// or wire state.
    pub(in crate::connection) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        self.stream.peer_certificate_der()
    }

    /// Consume the reader and return the owned stream.
    ///
    /// Used by stream upgrades (STARTTLS, COMPRESS): drop this reader
    /// (and its buffer), transform the stream, and construct a fresh
    /// reader on the new stream. This enforces I10  -  buffer bytes
    /// cannot straddle a stream upgrade.
    pub(super) fn into_stream(self) -> ImapStream {
        self.stream
    }

    /// Take all remaining bytes from the internal parse buffer.
    ///
    /// Used by COMPRESS (RFC 4978 Section3): after the tagged OK, any bytes
    /// already buffered are compressed data that must be preserved in
    /// the new `CompressedStream`'s raw read buffer.
    pub(super) fn take_buffer(&mut self) -> bytes::BytesMut {
        self.buf.split_off(0)
    }

    // -----------------------------------------------------------------------
    // Private helpers  -  ported from ImapConnection::try_parse_response,
    // buffer_may_contain_complete_response, and try_parse_literal_marker
    // in helpers.rs. The logic is identical; only field accesses changed
    // from `self.read_buf` to `self.buf`.
    // -----------------------------------------------------------------------

    /// Try to parse a greeting from the internal buffer (RFC 3501 Section7.1).
    fn try_parse_greeting_inner(&mut self) -> Result<Option<Response>, Error> {
        use crate::codec::decode::parse_greeting;
        if self.buf.is_empty() {
            return Ok(Option::None);
        }
        if !buffer_may_contain_complete_response(&self.buf)? {
            return Ok(Option::None);
        }
        match parse_greeting(&self.buf) {
            Ok((remaining, resp)) => {
                let consumed = self.buf.len() - remaining.len();
                let _ = self.buf.split_to(consumed);
                Ok(Some(resp))
            }
            Err(nom::Err::Incomplete(_)) => Ok(Option::None),
            Err(e) => {
                if !self.buf.ends_with(b"\r\n") {
                    trace!(
                        "greeting parse Error on buffer not ending with CRLF  -  \
                         treating as incomplete ({} bytes buffered)",
                        self.buf.len()
                    );
                    return Ok(Option::None);
                }
                warn!("greeting parse error: {e}");
                Err(Error::Parse(format!("greeting parse error: {e}")))
            }
        }
    }

    /// Try to parse a response from the internal buffer.
    ///
    /// Uses `parse_response_utf8` in UTF-8 mode when the caller indicates
    /// `UTF8=ACCEPT` (RFC 6855 Section 3) or `IMAP4rev2` is active
    /// (RFC 9051 Section 7: quoted-strings carry raw UTF-8).
    fn try_parse_response_inner(&mut self, utf8_mode: bool) -> Result<Option<Response>, Error> {
        if self.buf.is_empty() {
            return Ok(Option::None);
        }

        // The parser uses `nom::bytes::complete` mode, which returns `Error`
        // (not `Incomplete`) when input is truncated mid-response. We must
        // detect incomplete data ourselves to avoid treating partial TCP
        // reads as hard parse failures.
        //
        // Every IMAP response ends with \r\n. If the buffer doesn't contain
        // \r\n at all, the first response is definitely incomplete.
        if !buffer_may_contain_complete_response(&self.buf)? {
            return Ok(Option::None);
        }

        match parse_response_utf8(&self.buf, utf8_mode) {
            Ok((remaining, resp)) => {
                let consumed = self.buf.len() - remaining.len();
                let _ = self.buf.split_to(consumed);
                Ok(Some(resp))
            }
            Err(nom::Err::Incomplete(_)) => Ok(Option::None),
            Err(e) => {
                // The parser failed even though we thought the data was
                // complete. If the buffer doesn't end with \r\n, it's
                // likely a partial read that our heuristic didn't catch
                // (e.g., literal data spanning TCP segments).
                if !self.buf.ends_with(b"\r\n") {
                    trace!(
                        "parse Error on buffer not ending with CRLF  -  treating as incomplete \
                         ({} bytes buffered)",
                        self.buf.len()
                    );
                    return Ok(Option::None);
                }
                warn!("response parse error: {e}");
                Err(Error::Parse(format!("response parse error: {e}")))
            }
        }
    }
}

/// One cumulative peer wait allowance, shared across stream upgrades.
#[derive(Clone)]
pub(in crate::connection) struct PeerBudget(Arc<Mutex<Duration>>);

impl PeerBudget {
    pub(in crate::connection) fn new(timeout: Duration) -> Self {
        Self(Arc::new(Mutex::new(timeout)))
    }
}

struct PeerCharge {
    budget: PeerBudget,
    started: Instant,
}

impl Drop for PeerCharge {
    fn drop(&mut self) {
        let mut remaining = self.budget.0.lock().expect("peer budget lock poisoned");
        *remaining = remaining.saturating_sub(self.started.elapsed());
    }
}

pub(in crate::connection) async fn peer_io<T>(
    budget: Option<PeerBudget>,
    io: impl std::future::Future<Output = std::io::Result<T>>,
) -> Result<T, Error> {
    let Some(budget) = budget else {
        return io.await.map_err(Error::from);
    };
    let remaining = *budget.0.lock().expect("peer budget lock poisoned");
    if remaining.is_zero() {
        return Err(Error::Timeout { attempt: None });
    }
    // Admission has already completed. The guard also spends peer wait time
    // when the I/O future is cancelled, so cancellation cannot reset the clock.
    let _charge = PeerCharge {
        budget,
        started: Instant::now(),
    };
    tokio::time::timeout(remaining, io)
        .await
        .map_err(|_| Error::Timeout { attempt: None })?
        .map_err(Error::from)
}

#[derive(Clone)]
pub(super) struct WireMetering {
    sink: Option<MeterSinkHandle>,
    budget: BandwidthBudget,
    peer_timeout: Option<PeerBudget>,
}

impl WireMetering {
    #[cfg(test)]
    fn disabled() -> Self {
        Self::new(None, None)
    }

    fn new(sink: Option<MeterSinkHandle>, budget: Option<BandwidthBudget>) -> Self {
        Self {
            sink,
            budget: budget.unwrap_or_default(),
            peer_timeout: None,
        }
    }

    fn record_in(&self, n: usize) {
        if let Some(sink) = &self.sink {
            sink.record_bytes_in(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }

    fn record_out(&self, n: usize) {
        if let Some(sink) = &self.sink {
            sink.record_bytes_out(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }
}

// DELIBERATELY ABSENT  -  do NOT add:
// - fn reset_buffer(&mut self)
// - fn set_stream(&mut self, s: ImapStream)
// - fn buffer_mut(&mut self) -> &mut BytesMut
// - any Deref/DerefMut/AsMut impl that exposes buf or stream
// These absences are the enforcement of I10.

// ---------------------------------------------------------------------------
// Module-private standalone helpers  -  ported verbatim from
// ImapConnection::buffer_may_contain_complete_response and
// ImapConnection::try_parse_literal_marker in helpers.rs.
// ---------------------------------------------------------------------------

/// Check whether `buf` likely contains at least one complete IMAP response.
///
/// A complete response always ends with `\r\n`. If the buffer contains
/// literal markers `{N}\r\n`, we verify that N bytes of literal data
/// follow each one, iterating through all literals in the response
/// (e.g., multi-body FETCH responses).
///
/// `Ok(false)` means "not yet"  -  the caller must read more bytes.
/// `Err(Error::Parse)` means "never": the server declared a literal whose
/// octets cannot be delivered to this process, so no amount of reading will
/// complete the response. Returning `Ok(false)` there would park the read
/// loop forever, which is why this is fallible at all. The threshold matches
/// `codec::decode::literal`, so a buffer this accepts is one the decoder can
/// still reject on its own terms, never the reverse.
///
/// Covered by example-based tests only. If property coverage is ever wanted
/// in this crate, this function is the strongest candidate: generate
/// well-formed response streams (including multi-literal FETCH bodies) and
/// assert the generator never lands on the `Err` lane, since a false
/// "never" here is connection-fatal on traffic a real server sends. Judged
/// not worth the dependency so far, because the fatal lane is guarded by an
/// explicit threshold rather than by inference.
pub(super) fn buffer_may_contain_complete_response(buf: &[u8]) -> Result<bool, Error> {
    // Fast path: no \r\n means definitely incomplete.
    let Some(first_crlf) = buf.windows(2).position(|w| w == b"\r\n") else {
        return Ok(false);
    };

    // Check for a literal marker `{digits[+]}\r\n` ending at first_crlf.
    // If present, verify we have the literal body + more data after it.
    // Also handles `~{digits}\r\n` (literal8, RFC 3516).
    //
    // Status responses (OK/NO/BAD/BYE) and tagged responses never contain
    // literals  -  their text is `1*TEXT-CHAR` (RFC 3501 Section 9), not
    // `string`. Skip the literal check for these to avoid misclassifying
    // `{digits}` in status text (e.g. `* OK [ALERT] limit {42}\r\n`).
    let may_contain_literal = if buf.starts_with(b"* ") && buf.len() >= 5 {
        // Untagged status responses: `* OK ...`, `* NO ...`, `* BAD ...`, `* BYE ...`
        // These use `resp-text` which has no literal production.
        // RFC 3501 Section 9: all alphabetic characters are case-insensitive,
        // so compare case-insensitively to handle non-conformant servers.
        let after_star = &buf[2..];
        let is_status = after_star
            .get(..3)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"OK ") || s.eq_ignore_ascii_case(b"NO "))
            || after_star.get(..4).is_some_and(|s| {
                s.eq_ignore_ascii_case(b"BAD ") || s.eq_ignore_ascii_case(b"BYE ")
            });
        !is_status
    } else if buf.starts_with(b"+") {
        // Continuation responses never contain literals.
        false
    } else {
        // Tagged responses (`TAG OK/NO/BAD ...`) use `resp-cond-state`
        // which also has no literal production (RFC 3501 Section 7.1).
        // Detect the tag boundary and check for a status keyword.
        //
        // RFC 3501 Section 9: `tag = 1*<any ASTRING-CHAR except "+">`.
        // A tagged response is `tag SP resp-cond-state CRLF` where
        // resp-cond-state = ("OK" / "NO" / "BAD") SP resp-text.
        // resp-text is `1*TEXT-CHAR` (no literal production), so
        // `{digits}` in the text is NOT a literal marker.
        if let Some(sp) = buf.iter().position(|&b| b == b' ') {
            let after_tag = &buf[sp + 1..];
            let is_tagged_status = after_tag
                .get(..3)
                .is_some_and(|s| s.eq_ignore_ascii_case(b"OK ") || s.eq_ignore_ascii_case(b"NO "))
                || after_tag
                    .get(..4)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"BAD "));
            // Also handle the edge case where the status keyword is
            // followed directly by CRLF (e.g., `TAG OK\r\n` with no
            // resp-text).
            let is_tagged_status = is_tagged_status
                || after_tag
                    .get(..4)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"OK\r\n"))
                || after_tag
                    .get(..4)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"NO\r\n"))
                || after_tag
                    .get(..5)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"BAD\r\n"));
            !is_tagged_status
        } else {
            // No space found  -  cannot be a valid tagged response.
            // Conservatively check for literals.
            true
        }
    };

    if first_crlf >= 2
        && may_contain_literal
        && let Some(literal_len) = try_parse_literal_marker(buf, first_crlf)?
    {
        // Skip past the first literal body and iteratively check
        // for additional literals (e.g., multi-body FETCH).
        let mut pos = advance_past_literal(first_crlf, 2, literal_len)?;
        loop {
            if pos > buf.len() {
                return Ok(false);
            }
            // Find the next \r\n after the current literal body.
            let remaining = &buf[pos..];
            let Some(next_crlf) = remaining.windows(2).position(|w| w == b"\r\n") else {
                return Ok(false);
            };
            // Check if there's another literal marker at this CRLF.
            if let Some(next_literal_len) = try_parse_literal_marker(remaining, next_crlf)? {
                // Another literal  -  skip past it and continue.
                pos = advance_past_literal(pos, next_crlf + 2, next_literal_len)?;
                continue;
            }
            // No more literals  -  this CRLF terminates the response.
            return Ok(true);
        }
    }

    // No literal  -  the first \r\n likely terminates a complete response.
    Ok(true)
}

/// Offset of the byte after a literal body that starts `marker` bytes past
/// `base`.
///
/// Each individual count is already bounded by `try_parse_literal_marker`, but
/// a response can chain many literals, and the running total is what has to
/// fit in the buffer. A total that no `usize` can address is unreachable in
/// the same way an oversized single count is, so it fails the same way rather
/// than stalling the read loop.
fn advance_past_literal(base: usize, marker: usize, literal_len: usize) -> Result<usize, Error> {
    base.checked_add(marker)
        .and_then(|offset| offset.checked_add(literal_len))
        .ok_or_else(|| {
            Error::Parse("literal framing offsets exceed the addressable buffer".to_owned())
        })
}

/// Try to parse a literal marker `{digits[+]}` ending just before `crlf_pos`
/// in `buf`. Returns the literal byte count if a valid marker is found.
///
/// A well-formed marker whose count is out of range is an error, not a
/// non-marker: it is recognized framing that names octets we can never
/// receive (RFC 9051 Section 9 caps `number64` at `i64::MAX`, and a 32-bit
/// target caps it lower still). Reporting `None` would let the caller declare
/// the response complete and hand truncated bytes to the parser; reporting
/// "incomplete" would wait for those octets forever.
fn try_parse_literal_marker(buf: &[u8], crlf_pos: usize) -> Result<Option<usize>, Error> {
    let before_crlf = &buf[..crlf_pos];
    let Some(brace_pos) = before_crlf.iter().rposition(|&b| b == b'{') else {
        return Ok(None);
    };
    use super::literals::LiteralMarker;

    let parsed = super::literals::literal_marker_at(buf, brace_pos);
    let (data_start, size) = match parsed {
        LiteralMarker::Counted {
            data_start, size, ..
        }
        | LiteralMarker::CountOutOfRange { data_start, size } => (data_start, size),
        LiteralMarker::NotAMarker => return Ok(None),
    };
    // A marker that does not end at this CRLF is not this line's framing,
    // whatever its count says.
    if data_start != crlf_pos + 2 {
        return Ok(None);
    }
    if let LiteralMarker::CountOutOfRange { size, .. } = parsed {
        return Err(Error::Parse(format!(
            "literal count {size} exceeds the RFC 9051 number64 range"
        )));
    }
    usize::try_from(size).map(Some).map_err(|_| {
        Error::Parse(format!(
            "literal count {size} exceeds this platform's addressable range"
        ))
    })
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
