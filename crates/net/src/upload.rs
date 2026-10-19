//! Per-attempt outbound payload admission and header timer accounting.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::task::AtomicWaker;
use http_body::{Body, Frame, SizeHint};
use tokio::time::Instant;

use crate::budget::{BandwidthBudget, BandwidthDirection, Reservation};
use crate::{AccountFuture, Error, TransmissionState};

struct ClockState {
    started: Instant,
    paused: Duration,
    admission: Option<Instant>,
}

/// Time spent waiting for admission is excluded from the header timer.
/// The absolute request deadline remains independent and always runs.
pub(crate) struct HeaderClock {
    state: Mutex<ClockState>,
    waker: AtomicWaker,
}

impl HeaderClock {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ClockState {
                started: Instant::now(),
                paused: Duration::ZERO,
                admission: None,
            }),
            waker: AtomicWaker::new(),
        })
    }

    fn pause(self: &Arc<Self>) -> AdmissionPause {
        self.state
            .lock()
            .expect("header clock lock poisoned")
            .admission = Some(Instant::now());
        self.waker.wake();
        AdmissionPause(Arc::clone(self))
    }

    pub(crate) async fn bound<F: Future>(
        self: &Arc<Self>,
        future: F,
        limit: Option<Duration>,
    ) -> Result<F::Output, Error> {
        let Some(limit) = limit else {
            return Ok(future.await);
        };
        let mut future = std::pin::pin!(future);
        let mut timer = Box::pin(tokio::time::sleep(limit));
        futures::future::poll_fn(|cx| {
            self.waker.register(cx.waker());
            // The inner future is polled before the budget is checked, the
            // same order `tokio::time::timeout` uses: a response that is
            // genuinely ready when the task is polled wins, even if the
            // task was polled late. In normal scheduling the timer fires at
            // the limit and wins first. Checking the budget first would only
            // differ for a task starved past its limit, and would then
            // discard a completed response. Reviewed and kept deliberately;
            // re-raise only with a case where a NOT-yet-ready dispatch
            // escapes the limit.
            if let Poll::Ready(result) = future.as_mut().poll(cx) {
                return Poll::Ready(Ok(result));
            }
            let state = self.state.lock().expect("header clock lock poisoned");
            let now = Instant::now();
            let elapsed = state
                .admission
                .unwrap_or(now)
                .duration_since(state.started)
                .saturating_sub(state.paused);
            let paused = state.admission.is_some();
            drop(state);
            if elapsed >= limit {
                return Poll::Ready(Err(Error::Timeout {
                    transmission_state: TransmissionState::InFlight,
                }));
            }
            if paused {
                return Poll::Pending;
            }
            let remaining = limit.saturating_sub(elapsed);
            timer.as_mut().reset(now + remaining);
            match timer.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(Error::Timeout {
                    transmission_state: TransmissionState::InFlight,
                })),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
}

struct AdmissionPause(Arc<HeaderClock>);

impl Drop for AdmissionPause {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("header clock lock poisoned");
        if let Some(started) = state.admission.take() {
            state.paused += started.elapsed();
        }
        drop(state);
        self.0.waker.wake();
    }
}

pub(crate) struct UploadBody {
    remaining: Bytes,
    budget: BandwidthBudget,
    clock: Arc<HeaderClock>,
    // Reqwest requires Body: Sync. Only poll_frame accesses this future,
    // through &mut self; the mutex supplies Sync without requiring the
    // admission future itself to be Sync.
    admission: Mutex<Option<AccountFuture<Reservation>>>,
}

impl UploadBody {
    pub(crate) fn new(remaining: Bytes, budget: BandwidthBudget, clock: Arc<HeaderClock>) -> Self {
        Self {
            remaining,
            budget,
            clock,
            admission: Mutex::new(None),
        }
    }
}

impl Body for UploadBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.remaining.is_empty() {
            return Poll::Ready(None);
        }
        let admission = this
            .admission
            .get_mut()
            .expect("upload admission lock poisoned");
        if admission.is_none() {
            let budget = this.budget.clone();
            let len = this.remaining.len();
            let clock = Arc::clone(&this.clock);
            *admission = Some(Box::pin(async move {
                let _pause = clock.pause();
                budget.ready(BandwidthDirection::Outbound, len).await
            }));
        }
        let mut grant = match admission
            .as_mut()
            .expect("upload admission exists")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(grant) => grant,
        };
        *admission = None;
        if !grant.offer() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let len = grant.granted();
        let bytes = this.remaining.split_to(len);
        // Yield is admission, not transmission evidence. Once yielded, the
        // entire slice stays charged even if hyper later abandons it.
        grant.finish(len);
        Poll::Ready(Some(Ok(Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.remaining.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn exhausted_header_time_cannot_be_paused_by_late_admission() {
        let clock = HeaderClock::new();
        let dispatch_clock = Arc::clone(&clock);
        let dispatch = async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let _pause = dispatch_clock.pause();
            futures::future::pending::<()>().await;
        };
        let mut bound = Box::pin(clock.bound(dispatch, Some(Duration::from_secs(1))));
        assert!(futures::FutureExt::now_or_never(bound.as_mut()).is_none());
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(matches!(
            futures::FutureExt::now_or_never(bound.as_mut()),
            Some(Err(Error::Timeout {
                transmission_state: TransmissionState::InFlight
            }))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn exact_remaining_hint_and_bounded_frames_survive_admission_pending() {
        let budget = BandwidthBudget::new(Some(2));
        let mut body = UploadBody::new(Bytes::from_static(b"abcde"), budget, HeaderClock::new());
        assert_eq!(body.size_hint().exact(), Some(5));
        let first = futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .expect("first frame")
            .expect("infallible")
            .into_data()
            .expect("data");
        assert_eq!(first, Bytes::from_static(b"ab"));
        assert_eq!(body.size_hint().exact(), Some(3));
        let mut next = Box::pin(futures::future::poll_fn(|cx| {
            Pin::new(&mut body).poll_frame(cx)
        }));
        assert!(futures::FutureExt::now_or_never(next.as_mut()).is_none());
        drop(next);
        assert_eq!(body.size_hint().exact(), Some(3));
        let second = futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .expect("second frame")
            .expect("infallible")
            .into_data()
            .expect("data");
        assert_eq!(second, Bytes::from_static(b"cd"));
        assert_eq!(body.size_hint().exact(), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_an_upload_wait_refunds_before_yield() {
        let budget = BandwidthBudget::new(Some(1));
        budget
            .admit(BandwidthDirection::Outbound, 1)
            .await
            .finish(1);
        let mut body =
            UploadBody::new(Bytes::from_static(b"x"), budget.clone(), HeaderClock::new());
        let mut next = Box::pin(futures::future::poll_fn(|cx| {
            Pin::new(&mut body).poll_frame(cx)
        }));
        assert!(futures::FutureExt::now_or_never(next.as_mut()).is_none());
        drop(next);
        drop(body);
        assert_eq!(
            budget.reserve(BandwidthDirection::Outbound, 1).ready_at(),
            Instant::now() + Duration::from_secs(1)
        );
    }
}
