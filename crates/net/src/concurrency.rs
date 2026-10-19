//! Weighted FIFO admission for shared request concurrency.
//!
//! Handles choose the scope: account-wide, shared across accounts, or attached
//! to individual requests. Rate limiting independently meters request rate.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::task::{Context, Poll, Waker};

/// A shareable bound on admitted request demand.
///
/// Admission is strictly FIFO, with deliberate head-of-line blocking. Demand
/// never exceeds capacity except for one exclusive claim larger than capacity
/// (or while a shrink drains previously admitted work). Oversized claims retain
/// their full demand even if the limit grows. Granted waiters may resume in any
/// order; a granted future neither polled nor dropped reserves capacity.
///
/// Requests hold demand across retries and redirects until the response body
/// ends, fails or is dropped. Keeping a streamed body open while awaiting more
/// work on the same limit can deadlock. Admission waits obey the total request
/// deadline and expire as `Timeout { Unsent }`.
#[derive(Clone)]
pub struct ConcurrencyLimit {
    inner: Arc<Inner>,
}

struct Inner {
    id: u64,
    state: Mutex<State>,
}

struct State {
    limit: usize,
    in_use: usize,
    next_waiter: u64,
    queue: VecDeque<u64>,
    waiters: HashMap<u64, Waiter>,
}

#[derive(Clone, Copy)]
enum WaiterState {
    Queued,
    Granted,
    Transferred,
    Cancelled,
}

struct Waiter {
    demand: usize,
    state: WaiterState,
    waker: Option<Waker>,
}

impl State {
    fn pump(&mut self, wake: &mut Vec<Waker>) {
        while let Some(&id) = self.queue.front() {
            let waiter = self.waiters.get_mut(&id).expect("queued waiter exists");
            let available = self.limit.saturating_sub(self.in_use);
            if waiter.demand > available && !(waiter.demand > self.limit && self.in_use == 0) {
                break;
            }
            self.in_use = self
                .in_use
                .checked_add(waiter.demand)
                .expect("admitted demand fits");
            waiter.state = WaiterState::Granted;
            if let Some(waker) = waiter.waker.take() {
                wake.push(waker);
            }
            self.queue.pop_front();
        }
    }

    fn release(&mut self, demand: usize) {
        self.in_use = self.in_use.checked_sub(demand).expect("held demand exists");
    }
}

impl ConcurrencyLimit {
    /// Construct a bound. Zero is raised to one with a warning.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("concurrency limit identities exhausted");
        Self {
            inner: Arc::new(Inner {
                id,
                state: Mutex::new(State {
                    limit: normalize(limit),
                    in_use: 0,
                    next_waiter: 0,
                    queue: VecDeque::new(),
                    waiters: HashMap::new(),
                }),
            }),
        }
    }

    /// The current capacity.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.state().limit
    }

    /// Full admitted demand, including grants not yet polled.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.state().in_use
    }

    /// Resize without revoking admitted work. Growing pumps the queue;
    /// shrinking blocks further admission until existing demand drains.
    pub fn set_limit(&self, limit: usize) {
        let limit = normalize(limit);
        let mut wake = Vec::new();
        {
            let mut state = self.state();
            state.limit = limit;
            state.pump(&mut wake);
        }
        for waker in wake {
            waker.wake();
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.inner.id
    }

    pub(crate) fn downgrade(&self) -> WeakLimit {
        WeakLimit(Arc::downgrade(&self.inner))
    }

    #[cfg(test)]
    pub(crate) fn acquire(&self) -> Acquire {
        self.acquire_weighted(1)
    }

    pub(crate) fn acquire_weighted(&self, demand: usize) -> Acquire {
        debug_assert!(demand != 0, "zero concurrency demand");
        Acquire {
            limit: self.clone(),
            demand: demand.max(1),
            id: None,
            state: WaiterState::Queued,
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

pub(crate) struct WeakLimit(Weak<Inner>);

impl WeakLimit {
    pub(crate) fn upgrade(&self) -> Option<ConcurrencyLimit> {
        self.0.upgrade().map(|inner| ConcurrencyLimit { inner })
    }
}

pub(crate) struct Acquire {
    limit: ConcurrencyLimit,
    demand: usize,
    id: Option<u64>,
    state: WaiterState,
}

impl Future for Acquire {
    type Output = ConcurrencyPermit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut wake = Vec::new();
        let mut retired = Vec::new();
        let mut replacement = Some(cx.waker().clone());
        let result;
        {
            let mut state = this.limit.state();
            let id = *this.id.get_or_insert_with(|| {
                let id = state.next_waiter;
                state.next_waiter = id.checked_add(1).expect("waiter identities exhausted");
                state.waiters.insert(
                    id,
                    Waiter {
                        demand: this.demand,
                        state: WaiterState::Queued,
                        waker: None,
                    },
                );
                state.queue.push_back(id);
                id
            });
            state.pump(&mut wake);
            let waiter = state
                .waiters
                .get_mut(&id)
                .expect("future polled after completion");
            if matches!(waiter.state, WaiterState::Granted) {
                let mut waiter = state.waiters.remove(&id).expect("granted waiter exists");
                retired.extend(waiter.waker.take());
                waiter.state = WaiterState::Transferred;
                this.state = WaiterState::Transferred;
                result = Poll::Ready(ConcurrencyPermit {
                    limit: this.limit.clone(),
                    demand: this.demand,
                });
            } else {
                retired.extend(
                    waiter
                        .waker
                        .replace(replacement.take().expect("replacement waker")),
                );
                result = Poll::Pending;
            }
        }
        drop(replacement);
        drop(retired);
        for waker in wake {
            waker.wake();
        }
        result
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        if matches!(self.state, WaiterState::Transferred) {
            return;
        }
        let Some(id) = self.id else {
            return;
        };
        let mut wake = Vec::new();
        let retired;
        {
            let mut state = self.limit.state();
            let mut waiter = state.waiters.remove(&id).expect("registered waiter exists");
            match waiter.state {
                WaiterState::Queued => state.queue.retain(|queued| *queued != id),
                WaiterState::Granted => state.release(waiter.demand),
                WaiterState::Transferred | WaiterState::Cancelled => unreachable!(),
            }
            waiter.state = WaiterState::Cancelled;
            retired = waiter.waker.take();
            state.pump(&mut wake);
        }
        drop(retired);
        for waker in wake {
            waker.wake();
        }
    }
}

impl std::fmt::Debug for ConcurrencyLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state();
        f.debug_struct("ConcurrencyLimit")
            .field("limit", &state.limit)
            .field("in_flight", &state.in_use)
            .finish()
    }
}

fn normalize(limit: usize) -> usize {
    if limit == 0 {
        tracing::warn!(target: "bifrost_net::concurrency",
            "concurrency limit of 0 raised to 1; a zero bound would admit nothing");
    }
    limit.max(1)
}

/// An owned claim, released exactly once on drop.
pub(crate) struct ConcurrencyPermit {
    limit: ConcurrencyLimit,
    demand: usize,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let mut wake = Vec::new();
        {
            let mut state = self.limit.state();
            state.release(self.demand);
            state.pump(&mut wake);
        }
        for waker in wake {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll, Waker};

    struct CountingWake(AtomicUsize);

    impl std::task::Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Waker, Arc<CountingWake>) {
        let count = Arc::new(CountingWake(AtomicUsize::new(0)));
        (Waker::from(Arc::clone(&count)), count)
    }

    pub(super) fn poll_with_waker<F: Future>(
        mut future: Pin<&mut F>,
        waker: &Waker,
    ) -> Poll<F::Output> {
        let mut cx = Context::from_waker(waker);
        future.as_mut().poll(&mut cx)
    }

    fn assert_woken(counter: &CountingWake, message: &str) {
        assert!(counter.0.load(Ordering::SeqCst) > 0, "{message}");
    }

    /// Poll once without advancing the executor.
    fn pending<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> bool {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        future.poll(&mut cx).is_pending()
    }

    /// The bound holds: with every slot taken, the next request waits until
    /// one is released, and then it is admitted.
    #[tokio::test]
    async fn a_full_limit_admits_the_next_request_only_after_a_release() {
        let limit = ConcurrencyLimit::new(2);
        let first = limit.acquire().await;
        let _second = limit.acquire().await;
        assert_eq!(limit.in_flight(), 2);

        let mut third = Box::pin(limit.acquire());
        let (waker, wake_count) = counting_waker();
        assert!(
            poll_with_waker(third.as_mut(), &waker).is_pending(),
            "a third request must wait while two hold the slots"
        );
        drop(first);
        assert_woken(
            &wake_count,
            "releasing a guard must wake the next admitted waiter",
        );
        let Poll::Ready(_third) = poll_with_waker(third.as_mut(), &waker) else {
            panic!("the released slot must admit the third request");
        };
        assert_eq!(limit.in_flight(), 2);
    }

    /// Admission is first come, first served. The waiters are distinguishable
    /// by arrival only, so an implementation that let a later waiter overtake
    /// an earlier one would admit the wrong one here.
    #[tokio::test]
    async fn waiters_are_admitted_in_arrival_order() {
        let limit = ConcurrencyLimit::new(1);
        let held = limit.acquire().await;
        let mut early = Box::pin(limit.acquire());
        let mut late = Box::pin(limit.acquire());
        assert!(pending(early.as_mut()));
        assert!(pending(late.as_mut()));

        drop(held);
        assert!(
            pending(late.as_mut()),
            "the later waiter must not overtake the earlier one"
        );
        let early = early.await;
        drop(early);
        let _late = late.await;
    }

    /// A shrink below the in-flight count revokes nothing: the held slots
    /// stay held, no new request is admitted until enough have finished, and
    /// the count then settles at the new bound.
    #[tokio::test]
    async fn a_shrink_blocks_admission_until_demand_drains() {
        let limit = ConcurrencyLimit::new(3);
        let a = limit.acquire().await;
        let b = limit.acquire().await;
        let c = limit.acquire().await;
        limit.set_limit(1);
        assert_eq!(limit.limit(), 1);
        assert_eq!(limit.in_flight(), 3, "a shrink revokes no held slot");

        drop(a);
        drop(b);
        assert_eq!(limit.in_flight(), 1);
        let mut next = Box::pin(limit.acquire());
        assert!(pending(next.as_mut()), "the bound of one is still taken");
        drop(c);
        let _next = next.await;
        assert_eq!(limit.in_flight(), 1);
    }

    /// Growing after shrinking uses the latest capacity.
    #[tokio::test]
    async fn a_grow_after_a_shrink_uses_the_latest_capacity() {
        let limit = ConcurrencyLimit::new(2);
        let a = limit.acquire().await;
        let b = limit.acquire().await;
        limit.set_limit(1);
        limit.set_limit(2);
        let mut next = Box::pin(limit.acquire());
        assert!(
            pending(next.as_mut()),
            "back at a bound of two with two held, nothing is free"
        );
        drop(a);
        let _next = next.await;
        drop(b);
        assert_eq!(limit.in_flight(), 1);
        assert_eq!(limit.limit(), 2);
    }

    #[tokio::test]
    async fn growing_a_limit_wakes_a_newly_admitted_waiter() {
        let limit = ConcurrencyLimit::new(1);
        let held = limit.acquire().await;
        let mut waiter = Box::pin(limit.acquire());
        let (waker, wake_count) = counting_waker();
        assert!(poll_with_waker(waiter.as_mut(), &waker).is_pending());

        limit.set_limit(2);
        assert_woken(
            &wake_count,
            "growing the limit must wake the waiter it admits",
        );
        let Poll::Ready(waiter) = poll_with_waker(waiter.as_mut(), &waker) else {
            panic!("the newly admitted waiter must be ready");
        };
        assert_eq!(limit.in_flight(), 2);
        drop((held, waiter));
    }

    #[tokio::test]
    async fn a_repolled_waiter_registers_its_latest_waker() {
        let limit = ConcurrencyLimit::new(1);
        let held = limit.acquire().await;
        let mut waiter = Box::pin(limit.acquire());
        let (old_waker, old_wakes) = counting_waker();
        let (new_waker, new_wakes) = counting_waker();

        assert!(poll_with_waker(waiter.as_mut(), &old_waker).is_pending());
        assert!(poll_with_waker(waiter.as_mut(), &new_waker).is_pending());
        drop(held);

        assert_eq!(
            old_wakes.0.load(Ordering::SeqCst),
            0,
            "a replaced waker must not be called"
        );
        assert_woken(
            &new_wakes,
            "releasing the slot must call the waiter's latest waker",
        );
        let Poll::Ready(waiter) = poll_with_waker(waiter.as_mut(), &new_waker) else {
            panic!("the waiter admitted by release must be ready");
        };
        assert_eq!(limit.in_flight(), 1);
        drop(waiter);
    }

    /// Cancelling an unpolled grant releases demand even after a shrink.
    #[tokio::test]
    async fn a_cancelled_granted_waiter_releases_demand_after_a_shrink() {
        let limit = ConcurrencyLimit::new(2);
        let held = limit.acquire().await;
        let freed = limit.acquire().await;
        let mut waiter = Box::pin(limit.acquire());
        assert!(pending(waiter.as_mut()), "both slots are taken");

        // Demand is reserved for the queued waiter before it is polled.
        drop(freed);
        limit.set_limit(1);
        // Cancel after the grant and before the next poll.
        drop(waiter);

        let mut intruder = Box::pin(limit.acquire());
        assert!(
            pending(intruder.as_mut()),
            "one request already holds the bound of one"
        );
        drop(held);
        let _admitted = intruder.await;
        assert_eq!(limit.in_flight(), 1);
    }

    #[tokio::test]
    async fn a_weighted_waiter_made_oversized_by_shrink_runs_exclusively() {
        let limit = ConcurrencyLimit::new(5);
        let held = limit.acquire_weighted(3).await;
        let mut waiter = Box::pin(limit.acquire_weighted(4));
        assert!(pending(waiter.as_mut()));
        limit.set_limit(2);
        assert!(pending(waiter.as_mut()));
        drop(held);
        assert_eq!(limit.in_flight(), 4);
        let guard = waiter.await;
        assert_eq!(limit.in_flight(), 4);
        drop(guard);
        assert_eq!(limit.in_flight(), 0);
    }

    #[tokio::test]
    async fn growing_during_an_oversized_run_counts_its_full_demand() {
        let limit = ConcurrencyLimit::new(2);
        let held = limit.acquire_weighted(5).await;
        let mut waiter = Box::pin(limit.acquire_weighted(2));
        assert!(pending(waiter.as_mut()));
        limit.set_limit(6);
        assert_eq!(limit.in_flight(), 5);
        assert!(pending(waiter.as_mut()));
        limit.set_limit(7);
        let waiter = waiter.await;
        assert_eq!(limit.in_flight(), 7);
        drop((held, waiter));
        assert_eq!(limit.in_flight(), 0);
    }

    #[tokio::test]
    async fn cancelling_a_blocked_head_admits_a_cheap_follower() {
        let limit = ConcurrencyLimit::new(3);
        let held = limit.acquire_weighted(2).await;
        let mut head = Box::pin(limit.acquire_weighted(2));
        let mut follower = Box::pin(limit.acquire());
        let (waker, wake_count) = counting_waker();
        assert!(pending(head.as_mut()));
        assert!(
            poll_with_waker(follower.as_mut(), &waker).is_pending(),
            "cheap work cannot overtake the head"
        );
        drop(head);
        assert_woken(
            &wake_count,
            "cancelling the blocked head must wake the follower it admits",
        );
        assert_eq!(limit.in_flight(), 3);
        let Poll::Ready(follower) = poll_with_waker(follower.as_mut(), &waker) else {
            panic!("the follower admitted after head cancellation must be ready");
        };
        drop((held, follower));
        assert_eq!(limit.in_flight(), 0);
    }

    #[tokio::test]
    async fn cancelling_an_unpolled_weighted_grant_returns_all_demand() {
        let limit = ConcurrencyLimit::new(3);
        let held = limit.acquire_weighted(3).await;
        let mut waiter = Box::pin(limit.acquire_weighted(3));
        assert!(pending(waiter.as_mut()));
        drop(held);
        assert_eq!(limit.in_flight(), 3);
        drop(waiter);
        assert_eq!(limit.in_flight(), 0);
        let _next = limit.acquire_weighted(3).await;
    }

    #[tokio::test]
    async fn transferring_a_grant_releases_exactly_once() {
        let limit = ConcurrencyLimit::new(4);
        let held = limit.acquire_weighted(4).await;
        let mut waiter = Box::pin(limit.acquire_weighted(3));
        assert!(pending(waiter.as_mut()));
        drop(held);
        let guard = std::future::poll_fn(|cx| waiter.as_mut().poll(cx)).await;
        drop(waiter);
        assert_eq!(limit.in_flight(), 3);
        let other = limit.acquire().await;
        drop(guard);
        assert_eq!(limit.in_flight(), 1);
        drop(other);
        assert_eq!(limit.in_flight(), 0);
    }

    #[test]
    fn a_zero_limit_is_raised_to_one() {
        let limit = ConcurrencyLimit::new(0);
        assert_eq!(limit.limit(), 1);
        limit.set_limit(0);
        assert_eq!(limit.limit(), 1);
    }
}

/// The limit wired through the real request pipeline, against the scripted
/// wire double.
#[cfg(test)]
mod pipeline_tests {
    use std::task::{Poll, Waker};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt;
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;

    use super::ConcurrencyLimit;
    use super::tests::poll_with_waker;
    use crate::config::NetConfig;
    use crate::error::Error;
    use crate::net::{AccountNet, AccountSpec};
    use crate::test_support::{Canned, ScriptedDispatch, canned, scripted_net};
    use crate::{AccountId, TransmissionState};

    fn account(
        script: &std::sync::Arc<ScriptedDispatch>,
        limit: Option<ConcurrencyLimit>,
    ) -> AccountNet {
        let mut spec = AccountSpec::new(None);
        spec.concurrency_limit = limit;
        scripted_net(script, NetConfig::default())
            .attach_account(AccountId("concurrency".to_owned()), spec)
    }

    fn pending<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> bool {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        future.poll(&mut cx).is_pending()
    }

    /// An account-level limit bounds a fan-out wider than itself. The
    /// dispatcher yields before answering, so without the limit all six legs
    /// would be on the wire together; the peak must be the limit exactly,
    /// which also shows the limit admits concurrency rather than serializing.
    #[tokio::test]
    async fn an_account_limit_bounds_a_wider_fan_out() {
        let script = ScriptedDispatch::yielding((0..6).map(|_| canned(StatusCode::OK, b"ok")));
        let account = account(&script, Some(ConcurrencyLimit::new(2)));
        let legs = (0..6).map(|n| {
            account
                .get(&format!("https://limit.test/{n}"))
                .without_bearer_auth()
                .send()
        });
        for result in futures::future::join_all(legs).await {
            result.expect("every gated leg completes");
        }
        assert_eq!(script.peak_in_flight(), 2);
    }

    /// A streamed body holds its slot until it ends: a second request on the
    /// same limit waits while the first body is unread, and is admitted once
    /// the body has been drained.
    #[tokio::test]
    async fn a_streaming_body_holds_its_slot_until_it_ends() {
        let script = ScriptedDispatch::new([
            Canned::Stream {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                chunks: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            },
            canned(StatusCode::OK, b"second"),
        ]);
        let account = account(&script, None);
        let limit = ConcurrencyLimit::new(1);
        let mut first = account
            .get("https://limit.test/stream")
            .without_bearer_auth()
            .concurrency_limit(limit.clone())
            .send_streaming()
            .await
            .expect("the stream opens");
        assert_eq!(limit.in_flight(), 1);

        let mut second = Box::pin(
            account
                .get("https://limit.test/second")
                .without_bearer_auth()
                .concurrency_limit(limit.clone())
                .send(),
        );
        assert!(
            pending(second.as_mut()),
            "the second request must wait while the first body is open"
        );
        while let Some(chunk) = first.body.next().await {
            chunk.expect("chunk");
        }
        // `first` is still alive here: the second request is admitted because
        // the drained body released its slot at its end, not on drop. The
        // release hands the slot straight to the queued waiter.
        let response = second.await.expect("admitted once the body ended");
        assert_eq!(response.body, Bytes::from_static(b"second"));
        drop(first);
        assert_eq!(limit.in_flight(), 0);
    }

    /// The slot wait is bounded by the request's deadline, and an expiry is
    /// `Unsent`: nothing reached the wire, so any method may be replayed.
    #[tokio::test(start_paused = true)]
    async fn a_slot_wait_expires_as_an_unsent_timeout() {
        let script = ScriptedDispatch::new([Canned::StreamThenStall {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            chunks: vec![Bytes::from_static(b"held")],
        }]);
        let limit = ConcurrencyLimit::new(1);
        let account = account(&script, Some(limit.clone()));
        let _held = account
            .get("https://limit.test/held")
            .without_bearer_auth()
            .send_streaming()
            .await
            .expect("the first request holds the only slot");

        let Err(error) = account
            .post("https://limit.test/waits")
            .without_bearer_auth()
            .timeout(Duration::from_millis(50))
            .send()
            .await
        else {
            panic!("the wait must expire");
        };
        assert!(
            matches!(
                error,
                Error::Timeout {
                    transmission_state: TransmissionState::Unsent
                }
            ),
            "expected an unsent timeout, got {error:?}"
        );
        assert_eq!(
            script.requests().len(),
            1,
            "the waiter never reached the wire"
        );
    }
    #[tokio::test]
    async fn duplicate_account_and_request_claims_sum_and_run_exclusively() {
        let script = ScriptedDispatch::new([Canned::StreamThenStall {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            chunks: vec![],
        }]);
        let limit = ConcurrencyLimit::new(1);
        let account = account(&script, Some(limit.clone()));
        let response = account
            .get("https://limit.test/duplicate")
            .without_bearer_auth()
            .concurrency_limit(limit.clone())
            .concurrency_claim(limit.clone(), 3)
            .send_streaming()
            .await
            .expect("one exclusive summed claim progresses");
        assert_eq!(limit.in_flight(), 5);
        drop(response);
        assert_eq!(limit.in_flight(), 0);
    }

    #[tokio::test]
    async fn claims_acquire_in_identity_order_and_cancel_partial_acquisition() {
        let script = ScriptedDispatch::new([]);
        let account = account(&script, None);
        let first = ConcurrencyLimit::new(1);
        let second = ConcurrencyLimit::new(1);
        let held = second.acquire().await;
        let mut request = Box::pin(
            account
                .get("https://limit.test/ordered")
                .without_bearer_auth()
                .concurrency_limit(second.clone())
                .concurrency_limit(first.clone())
                .send(),
        );
        assert!(pending(request.as_mut()));
        assert_eq!(
            first.in_flight(),
            1,
            "identity order overrides declaration order"
        );
        drop(request);
        assert_eq!(first.in_flight(), 0);
        assert_eq!(second.in_flight(), 1);
        drop(held);
        assert_eq!(second.in_flight(), 0);
        assert!(script.requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_drops_partially_acquired_claims_as_unsent() {
        let script = ScriptedDispatch::new([]);
        let account = account(&script, None);
        let first = ConcurrencyLimit::new(1);
        let second = ConcurrencyLimit::new(1);
        let held = second.acquire().await;
        let mut request = Box::pin(
            account
                .get("https://limit.test/deadline")
                .without_bearer_auth()
                .concurrency_limit(second.clone())
                .concurrency_claim(first.clone(), 2)
                .timeout(Duration::from_millis(50))
                .send(),
        );
        assert!(pending(request.as_mut()));
        assert_eq!(first.in_flight(), 2);
        let result = request.await;
        assert!(matches!(
            result,
            Err(Error::Timeout {
                transmission_state: TransmissionState::Unsent,
            })
        ));
        assert_eq!(first.in_flight(), 0);
        assert_eq!(second.in_flight(), 1);
        drop(held);
        assert!(script.requests().is_empty());
    }

    #[tokio::test]
    async fn a_stream_holds_every_distinct_claim_until_its_end() {
        let script = ScriptedDispatch::new([Canned::Stream {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            chunks: vec![Bytes::from_static(b"body")],
        }]);
        let first = ConcurrencyLimit::new(2);
        let second = ConcurrencyLimit::new(3);
        let account = account(&script, Some(first.clone()));
        let mut response = account
            .get("https://limit.test/claims")
            .without_bearer_auth()
            .concurrency_claim(second.clone(), 3)
            .send_streaming()
            .await
            .expect("both claims admitted");
        assert_eq!(first.in_flight(), 1);
        assert_eq!(second.in_flight(), 3);
        while let Some(chunk) = response.body.next().await {
            chunk.expect("body chunk");
        }
        assert_eq!(first.in_flight(), 0);
        assert_eq!(second.in_flight(), 0);
    }

    #[tokio::test]
    async fn summed_claim_overflow_is_rejected_before_any_acquisition() {
        let script = ScriptedDispatch::new([]);
        let first = ConcurrencyLimit::new(1);
        let held = first.acquire().await;
        let limit = ConcurrencyLimit::new(1);
        let account = account(&script, Some(limit.clone()));
        let mut request = Box::pin(
            account
                .get("https://limit.test/overflow")
                .without_bearer_auth()
                .concurrency_limit(first.clone())
                .concurrency_claim(limit.clone(), usize::MAX)
                .send(),
        );
        let Poll::Ready(result) = poll_with_waker(request.as_mut(), Waker::noop()) else {
            panic!("summed claim overflow must fail on the first poll, without waiting");
        };
        assert!(matches!(
            result,
            Err(Error::InvalidRequest {
                field: "concurrency_claim",
                ..
            })
        ));
        assert_eq!(
            first.in_flight(),
            1,
            "overflow must be rejected before acquiring the occupied earlier limit"
        );
        assert_eq!(limit.in_flight(), 0);
        assert!(script.requests().is_empty());
        drop(held);
    }

    #[test]
    fn keyed_limits_are_weak_shared_per_net_and_keep_existing_capacity() {
        let script = ScriptedDispatch::new([]);
        let net = scripted_net(&script, NetConfig::default());
        let account = net.attach_account(AccountId("first".to_owned()), AccountSpec::new(None));
        let other = net.attach_account(AccountId("other".to_owned()), AccountSpec::new(None));
        let first = account.shared_concurrency_limit("key", 2);
        let id = first.id();
        let same = other.shared_concurrency_limit("key", 9);
        assert_eq!(same.id(), id);
        assert_eq!(same.limit(), 2);
        same.set_limit(3);
        assert_eq!(first.limit(), 3);
        let unrelated = self::account(&script, None).shared_concurrency_limit("key", 7);
        assert_ne!(unrelated.id(), id);
        assert_eq!(unrelated.limit(), 7);
        drop((first, same));
        let fresh = account.shared_concurrency_limit("key", 4);
        assert_ne!(fresh.id(), id);
        assert_eq!(fresh.limit(), 4);
    }
}
