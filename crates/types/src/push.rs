//! Push lanes and sessions: the vocabulary of `Account::push_lanes` and
//! `Account::push_stream`, plus the shared single-driver lease.

use crate::{AccountError, AccountOperation, InvalidationHint, Protocol, Warning};
use futures::{
    FutureExt,
    channel::oneshot,
    future::{BoxFuture, Shared},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

/// One independently reconnecting push session slot of an account instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PushLane(pub u16);

/// How a lane delivers coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushLaneKind {
    /// The session itself carries the notifications (IMAP IDLE, JMAP
    /// WebSocket, EWS streaming): coverage exists while a session is live.
    Connection,
    /// The session maintains a server registration whose notifications reach
    /// the consumer's receiver and then the engine's `InvalidationSink`
    /// (Graph webhooks, Gmail Pub/Sub): coverage outlives any one session.
    Registration,
}

/// One lane an account runs, with the failure pacing the engine applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushLaneSpec {
    pub lane: PushLane,
    pub kind: PushLaneKind,
    pub backoff: BackoffPolicy,
}

/// One item of a push session (`Account::push_stream`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PushSessionEvent {
    /// Drive the hinted scopes. Over-approximation is always allowed.
    Invalidated(InvalidationHint),
    /// Connection lanes: the session reached live coverage (IDLE running,
    /// WebSocket subscription applied, EWS subscription created).
    /// Registration lanes: the registration is believed live.
    Established,
    /// Demonstrated healthy progress: a completed IDLE round, a frame the
    /// peer served, a complete long poll, a fully successful renewal pass.
    /// Reaching a read loop or a successful setup step is not progress.
    Healthy,
    /// Advisory; does not end the session.
    Warning(Warning),
    /// One unit of the session failed (a Graph row, an IMAP folder); the
    /// session continues or ends per the `push_stream` contract.
    UnitFailed(AccountError),
    /// The session ended on a classified failure. Always the last item.
    Failed(AccountError),
    /// The session ended without a classified failure. Always the last item.
    Ended(PushSessionEnd),
}

/// Why a push session ended without a classified failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushSessionEnd {
    /// The stop signal or the account's own shutdown ended it.
    Stopped,
    /// The subscribed topology changed and the session must be rebuilt now.
    Reconfigure,
    /// Unclassified transport loss (EOF, pong timeout, server termination,
    /// a setup step past its bound).
    Lost,
    /// Registration lanes: a maintenance pass finished with at least one
    /// non-terminal `UnitFailed`, or with units still retired by an earlier
    /// terminal failure that the consumer has not yet unsubscribed.
    Incomplete,
}

/// Engine-to-provider request that a push session end. Cloneable. Resolves
/// on `SessionStopper::stop` and also when the stopper is dropped.
#[derive(Clone)]
pub struct SessionStop(Option<Shared<oneshot::Receiver<()>>>);

/// The engine's end of a `SessionStop`.
pub struct SessionStopper(Option<oneshot::Sender<()>>);
impl SessionStop {
    #[must_use]
    pub fn pair() -> (SessionStopper, Self) {
        let (tx, rx) = oneshot::channel();
        (SessionStopper(Some(tx)), Self(Some(rx.shared())))
    }
    /// A stop that never fires, for callers that end sessions by dropping.
    #[must_use]
    pub fn never() -> Self {
        Self(None)
    }
    /// Resolves once stop was requested (or the stopper dropped).
    pub async fn stopped(&self) {
        if let Some(rx) = &self.0 {
            let _ = rx.clone().await;
        } else {
            futures::future::pending::<()>().await;
        }
    }
    /// Non-blocking check.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|rx| rx.clone().now_or_never().is_some())
    }
}
impl SessionStopper {
    pub fn stop(mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}
impl std::fmt::Debug for SessionStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStop")
            .field("stopped", &self.is_stopped())
            .finish()
    }
}
impl std::fmt::Debug for SessionStopper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStopper")
            .field("stopped", &self.0.is_none())
            .finish()
    }
}

/// Engine-to-provider throttle gate. A Registration session awaits `clear()`
/// (selecting on its `SessionStop` and the account shutdown) before every
/// maintenance request, so an engine throttle hold recorded while the session
/// is live holds that session's next request. `bifrost-types` has no Tokio
/// dependency, so the engine supplies the waiting future.
#[derive(Clone)]
pub struct PushGate(Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>);
impl PushGate {
    /// A gate that never holds, for callers without an engine.
    #[must_use]
    pub fn open() -> Self {
        Self(None)
    }
    #[must_use]
    pub fn new(clear: impl Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(clear)))
    }
    /// Resolves once no engine hold covers this lane's work (at once for
    /// `open()`).
    pub fn clear(&self) -> BoxFuture<'static, ()> {
        self.0
            .as_ref()
            .map_or_else(|| futures::future::ready(()).boxed(), |clear| clear())
    }
}
impl std::fmt::Debug for PushGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushGate")
            .field("open", &self.0.is_none())
            .finish()
    }
}

/// One lane's single-driver lease (S11). Cheap to clone; the account keeps
/// one per lane and each session stream takes a guard into itself.
#[derive(Debug, Clone, Default)]
pub struct LaneLease(Arc<AtomicBool>);

/// Releases the lease on drop, so a dropped stream frees it.
#[derive(Debug)]
pub struct LaneLeaseGuard(Arc<AtomicBool>);
impl LaneLease {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// `None` while a live session holds the lease.
    #[must_use]
    pub fn try_claim(&self) -> Option<LaneLeaseGuard> {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| LaneLeaseGuard(Arc::clone(&self.0)))
    }
}
impl Drop for LaneLeaseGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// The S11 failure: `Internal(AccountContract)` for
/// `AccountOperation::PushStream`, with a support-only detail naming the lane
/// and `detail` ("already driven" or "not a lane of this account"). Derives a
/// terminal `InternalFailure`.
#[must_use]
pub fn push_lane_contract_error(
    protocol: Protocol,
    lane: PushLane,
    detail: &'static str,
) -> AccountError {
    crate::AccountErrorBuilder::new(
        crate::AccountErrorKind::Internal(crate::InternalErrorKind::AccountContract),
        crate::Cause::Internal(crate::InternalCause::new(
            crate::InternalErrorKind::AccountContract,
            Some(crate::DiagnosticText::support_only(format!(
                "push lane {}: {detail}",
                lane.0
            ))),
        )),
    )
    .protocol(protocol)
    .operation(AccountOperation::PushStream)
    .try_build()
    .expect("push lane contract error")
}

/// Failure pacing for one lane, owned and applied by the engine. The fields
/// are private so the normalization in `new` cannot be bypassed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    initial: Duration,
    max: Duration,
    /// Add a uniform delay in [0, base/4] to each computed delay.
    jitter: bool,
}

impl BackoffPolicy {
    /// `initial` below one millisecond is raised to one millisecond; `max`
    /// below `initial` is raised to `initial`. The only constructor.
    #[must_use]
    pub fn new(initial: Duration, max: Duration, jitter: bool) -> Self {
        let initial = initial.max(Duration::from_millis(1));
        Self {
            initial,
            max: max.max(initial),
            jitter,
        }
    }

    #[must_use]
    pub fn initial(&self) -> Duration {
        self.initial
    }

    #[must_use]
    pub fn max(&self) -> Duration {
        self.max
    }

    #[must_use]
    pub fn jitter(&self) -> bool {
        self.jitter
    }

    /// Delay earned by a 1-based consecutive failure count, without jitter.
    #[must_use]
    pub fn base_delay(&self, failures: u32) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        let mut delay = self.initial;
        // Duration's entire range is reached within 128 doublings even
        // from a nanosecond. Stop at the ceiling rather than iterating an
        // untrusted failure count.
        for _ in 0..(failures - 1).min(128) {
            delay = delay.saturating_mul(2).min(self.max);
            if delay == self.max {
                break;
            }
        }
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_policy_doubles_to_the_ceiling() {
        let policy = BackoffPolicy::new(Duration::from_secs(5), Duration::from_secs(18), false);
        let delays: Vec<_> = (1..=5).map(|n| policy.base_delay(n).as_secs()).collect();
        assert_eq!(delays, [5, 10, 18, 18, 18]);
    }

    #[test]
    fn backoff_policy_normalizes_inverted_bounds() {
        let policy = BackoffPolicy::new(Duration::ZERO, Duration::ZERO, true);
        assert_eq!(policy.initial(), Duration::from_millis(1));
        assert_eq!(policy.max(), policy.initial());
        assert!(policy.jitter());
        let policy = BackoffPolicy::new(Duration::from_secs(2), Duration::from_secs(1), false);
        assert_eq!(policy.max(), Duration::from_secs(2));
    }

    #[test]
    fn backoff_policy_zero_failures_is_no_delay() {
        assert_eq!(
            BackoffPolicy::new(Duration::ZERO, Duration::ZERO, false).base_delay(0),
            Duration::ZERO
        );
    }

    #[test]
    fn backoff_policy_saturates_on_large_counts() {
        let policy = BackoffPolicy::new(Duration::from_millis(1), Duration::MAX, false);
        assert_eq!(policy.base_delay(u32::MAX), Duration::MAX);
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;
    #[test]
    fn session_stop_resolves_on_stop() {
        let (stopper, stop) = SessionStop::pair();
        assert!(!stop.is_stopped());
        assert!(stop.stopped().now_or_never().is_none());
        stopper.stop();
        assert!(stop.stopped().now_or_never().is_some());
    }
    #[test]
    fn session_stop_resolves_when_the_stopper_drops() {
        let (stopper, stop) = SessionStop::pair();
        drop(stopper);
        assert!(stop.is_stopped());
        assert!(stop.stopped().now_or_never().is_some());
    }
    #[test]
    fn session_stop_never_stays_pending() {
        let stop = SessionStop::never();
        assert!(!stop.is_stopped());
        assert!(stop.stopped().now_or_never().is_none());
    }
    #[test]
    fn session_stop_is_stopped_is_non_blocking() {
        let (_stopper, stop) = SessionStop::pair();
        assert!(!stop.is_stopped());
    }
    #[test]
    fn push_gate_open_clears_at_once() {
        assert!(PushGate::open().clear().now_or_never().is_some());
    }
    #[test]
    fn push_gate_runs_the_engine_future_on_each_clear() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let gate = PushGate::new(move || {
            let count = Arc::clone(&count);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
            }
            .boxed()
        });
        for _ in 0..2 {
            assert!(gate.clear().now_or_never().is_some());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn lane_lease_is_shared_and_released_on_drop() {
        let lease = LaneLease::new();
        let guard = lease.try_claim().unwrap();
        assert!(lease.clone().try_claim().is_none());
        drop(guard);
        assert!(lease.try_claim().is_some());
    }
}
