//! Admission at the byte-stream boundary.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{BandwidthBudget, BandwidthDirection, MeterSinkHandle, Reservation};

type Admission = Pin<Box<dyn Future<Output = Reservation> + Send>>;

#[derive(Default)]
struct Lane {
    waiting: Option<Admission>,
    offered: Option<Reservation>,
}

impl Lane {
    fn poll_grant(
        &mut self,
        cx: &mut Context<'_>,
        budget: &BandwidthBudget,
        direction: BandwidthDirection,
        len: usize,
    ) -> Poll<usize> {
        loop {
            if let Some(reservation) = &self.offered {
                return Poll::Ready(reservation.granted().min(len));
            }
            let waiting = self.waiting.get_or_insert_with(|| {
                let budget = budget.clone();
                Box::pin(async move { budget.ready(direction, len).await })
            });
            let mut reservation = ready!(waiting.as_mut().poll(cx));
            self.waiting = None;
            if reservation.offer() {
                let granted = reservation.granted().min(len);
                self.offered = Some(reservation);
                return Poll::Ready(granted);
            }
        }
    }

    fn finish(&mut self, result: &io::Result<usize>) {
        if let Some(reservation) = self.offered.take() {
            // An error has no reliable byte count; dropping an offered
            // reservation conservatively keeps its entire charge.
            if let Ok(used) = result {
                reservation.finish(*used);
            }
        }
    }
}

/// A byte stream with independent inbound and outbound bandwidth admission.
///
/// Meters bytes above `S`: wrapping a TLS stream charges plaintext, including
/// any protocol framing above it. Each direction holds at most one admission
/// across Pending, including an offered grant when the inner I/O is pending.
/// Short successful I/O refunds only a current tail reservation. Dropping the
/// adapter drops both admissions according to the budget's cancellation rules.
/// No peer timers are installed here. Timers inside `S` only see admitted I/O;
/// a timeout around this adapter also includes admission waiting.
pub struct MeteredStream<S> {
    inner: S,
    budget: BandwidthBudget,
    sink: Option<MeterSinkHandle>,
    inbound: Lane,
    outbound: Lane,
}

impl<S> MeteredStream<S> {
    /// Wrap a stream using a shared account budget.
    pub fn new(inner: S, budget: BandwidthBudget) -> Self {
        Self {
            inner,
            budget,
            sink: None,
            inbound: Lane::default(),
            outbound: Lane::default(),
        }
    }

    /// Observe actual completed I/O through an optional account-scoped sink.
    /// Pending operations, errors, flush and shutdown do not invent byte counts.
    pub fn with_meter_sink(mut self, sink: Option<MeterSinkHandle>) -> Self {
        self.sink = sink;
        self
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for MeteredStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.get_mut();
        let granted = ready!(this.inbound.poll_grant(
            cx,
            &this.budget,
            BandwidthDirection::Inbound,
            buf.remaining(),
        ));
        let mut limited = ReadBuf::new(buf.initialize_unfilled_to(granted));
        let result = ready!(Pin::new(&mut this.inner).poll_read(cx, &mut limited));
        let used = limited.filled().len();
        buf.advance(used);
        if result.is_ok()
            && let Some(sink) = &this.sink
        {
            sink.record_bytes_in(u64::try_from(used).unwrap_or(u64::MAX));
        }
        this.inbound.finish(
            &result
                .as_ref()
                .map(|()| used)
                .map_err(|e| io::Error::from(e.kind())),
        );
        Poll::Ready(result)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for MeteredStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        let granted = ready!(this.outbound.poll_grant(
            cx,
            &this.budget,
            BandwidthDirection::Outbound,
            buf.len(),
        ));
        let result = ready!(Pin::new(&mut this.inner).poll_write(cx, &buf[..granted]));
        this.outbound.finish(&result);
        if let Ok(used) = &result
            && let Some(sink) = &this.sink
        {
            sink.record_bytes_out(u64::try_from(*used).unwrap_or(u64::MAX));
        }
        Poll::Ready(result)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
    use tokio::time::{Duration, Instant};

    #[tokio::test]
    async fn observation_counts_short_completions_and_not_pending_or_errors() {
        let meter = crate::BandwidthMeter::new();
        let id = crate::AccountId("stream".to_owned());
        meter.register_account(id.clone());
        let account = meter.account(id);
        let (stream, mut peer) = duplex(1);
        let mut stream = MeteredStream::new(stream, BandwidthBudget::default())
            .with_meter_sink(Some(account.sink_handle()));
        assert_eq!(stream.write(b"abcd").await.unwrap(), 1);
        assert_eq!(account.bytes_out(), 1);
        let mut pending = Box::pin(stream.write(b"bc"));
        assert!((&mut pending).now_or_never().is_none());
        assert_eq!(account.bytes_out(), 1);
        drop(pending);
        let mut byte = [0];
        peer.read_exact(&mut byte).await.unwrap();
        peer.write_all(b"z").await.unwrap();
        let mut bytes = [0; 8];
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 1);
        assert_eq!(account.bytes_in(), 1);
        stream.flush().await.unwrap();
        drop(peer);
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 0);
        assert!(stream.write(b"x").await.is_err());
        assert_eq!(account.bytes_in(), 1);
        assert_eq!(account.bytes_out(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn two_writers_share_one_schedule() {
        let budget = BandwidthBudget::new(Some(4));
        let (a, mut peer_a) = duplex(32);
        let (b, mut peer_b) = duplex(32);
        let mut a = MeteredStream::new(a, budget.clone());
        let mut b = MeteredStream::new(b, budget);
        let start = Instant::now();
        assert_eq!(a.write(b"12345678").await.unwrap(), 4);
        let mut write = Box::pin(b.write(b"abcd"));
        assert!((&mut write).now_or_never().is_none());
        let mut received = [0; 4];
        peer_a.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"1234");
        assert_eq!(write.await.unwrap(), 4);
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        peer_b.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"abcd");
    }

    #[tokio::test(start_paused = true)]
    async fn reader_limits_each_inner_read_to_its_grant() {
        let (stream, mut peer) = duplex(32);
        peer.write_all(b"12345678").await.unwrap();
        let mut stream = MeteredStream::new(stream, BandwidthBudget::new(Some(4)));
        let mut buf = [0; 16];
        let start = Instant::now();
        assert_eq!(stream.read(&mut buf).await.unwrap(), 4);
        assert_eq!(&buf[..4], b"1234");
        assert_eq!(stream.read(&mut buf).await.unwrap(), 4);
        assert_eq!(&buf[..4], b"5678");
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn short_write_refunds_only_the_tail() {
        for tail in [true, false] {
            let budget = BandwidthBudget::new(Some(4));
            let (mut stream, mut peer) = duplex(1);
            stream.write_all(b"x").await.unwrap();
            let mut stream = MeteredStream::new(stream, budget.clone());
            let mut write = Box::pin(stream.write(b"1234"));
            assert!((&mut write).now_or_never().is_none());
            // Install a later slot while the first offered write is pending.
            let follower = (!tail).then(|| budget.reserve(BandwidthDirection::Outbound, 4));
            let mut byte = [0];
            peer.read_exact(&mut byte).await.unwrap();
            assert_eq!(write.await.unwrap(), 1);
            let next = budget.reserve(BandwidthDirection::Outbound, 4);
            assert_eq!(
                next.ready_at().duration_since(Instant::now()),
                if tail {
                    Duration::from_millis(250)
                } else {
                    Duration::from_secs(2)
                }
            );
            drop(next);
            drop(follower);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cap_lift_wakes_pending_writer() {
        let budget = BandwidthBudget::new(Some(4));
        let (stream, _peer) = duplex(32);
        let mut stream = MeteredStream::new(stream, budget.clone());
        stream.write_all(b"1234").await.unwrap();
        let start = Instant::now();
        let mut write = Box::pin(stream.write(b"5678"));
        assert!((&mut write).now_or_never().is_none());
        budget.set_cap(None);
        assert_eq!(write.await.unwrap(), 4);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}
