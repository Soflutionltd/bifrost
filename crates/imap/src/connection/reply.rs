//! Atomic admission shared by the caller and the driver.
//!
//! Every wire command carries one admission cell, shared by the caller's
//! [`Pending`] and the driver's [`Reply`]. The cell is QUEUED until exactly one
//! party wins a compare-exchange: the driver at the head of its queue
//! ([`Reply::claim`], to CLAIMED) or the caller giving up
//! ([`Pending::withdraw`], to WITHDRAWN). The command's payload sits in the
//! cell and only the winner can take it, so the driver can neither run nor
//! answer a command it did not claim, and a withdrawn command is provably
//! never written: its evidence is a true `Unsent`.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use tokio::sync::oneshot;
use tokio::time::Instant;

const QUEUED: u8 = 0;
const CLAIMED: u8 = 1;
const WITHDRAWN: u8 = 2;

/// Shared by both halves. Only the winner of the CAS on `state` ever touches
/// `payload`, so the mutex is uncontended; it exists to make the cell `Sync`.
struct Admission<P> {
    state: AtomicU8,
    payload: Mutex<Option<P>>,
}

/// Driver half. Travels inside the `DriverCommand`. Holds no payload of its
/// own: the payload is reachable only by winning [`claim`](Self::claim).
pub(super) struct Reply<P, T> {
    tx: oneshot::Sender<T>,
    admission: Arc<Admission<P>>,
}

/// What the driver holds once it owns the command. The only way to answer.
pub(super) struct ClaimedReply<T> {
    tx: oneshot::Sender<T>,
}

/// Caller half. Lives in the submitting frame; dropping it unsettled
/// withdraws the command, or abandons the connection when the driver had
/// already claimed it and no answer is in hand.
pub(super) struct Pending<'conn, P, T> {
    rx: oneshot::Receiver<T>,
    admission: Arc<Admission<P>>,
    abandoned: &'conn AtomicBool,
    /// Set once the exchange is over from the caller's side: an answer was
    /// taken, or the receiver reported the reply dropped (a dead driver has
    /// no exchange left to abandon).
    settled: bool,
    /// Set once the receiver has reported closure, so later operations do
    /// not poll a terminated oneshot.
    closed: bool,
}

/// Outcome of [`Pending::withdraw`].
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Withdrawal {
    /// The state is WITHDRAWN (this call won the CAS, or an earlier call
    /// did): the driver will never write this command, and the payload has
    /// already been dropped in the caller's frame.
    Withdrawn,
    /// The driver claimed it first; it is or will be executed.
    Claimed,
}

/// Why [`Pending::recv`] got no answer: the driver dropped the reply.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Unanswered {
    /// Dropped while still QUEUED (driver gone before its claim).
    NeverClaimed,
    /// Dropped after the driver claimed it.
    Claimed,
}

impl Unanswered {
    /// The caller's transmission phase for a driver death.
    pub(super) fn attempt(self) -> bifrost_types::TransmissionState {
        match self {
            Self::NeverClaimed => bifrost_types::TransmissionState::Unsent,
            Self::Claimed => bifrost_types::TransmissionState::InFlight,
        }
    }
}

/// One admission cell holding `payload`, split into its driver and caller
/// halves. `abandoned` is the connection's flag, set by `Pending::drop` when
/// the caller leaves a claimed exchange unanswered.
pub(super) fn reply_channel<P, T>(
    payload: P,
    abandoned: &AtomicBool,
) -> (Reply<P, T>, Pending<'_, P, T>) {
    let admission = Arc::new(Admission {
        state: AtomicU8::new(QUEUED),
        payload: Mutex::new(Some(payload)),
    });
    let (tx, rx) = oneshot::channel();
    (
        Reply {
            tx,
            admission: Arc::clone(&admission),
        },
        Pending {
            rx,
            admission,
            abandoned,
            settled: false,
            closed: false,
        },
    )
}

impl<P, T> Reply<P, T> {
    /// CAS QUEUED -> CLAIMED, then take the payload. `None` means the caller
    /// withdrew (and already dropped the payload); the driver drops the shell
    /// without writing a byte and without answering.
    #[expect(
        clippy::unwrap_in_result,
        reason = "a won claim guarantees the payload; poisoning is an invariant failure"
    )]
    pub(super) fn claim(self) -> Option<(P, ClaimedReply<T>)> {
        self.admission
            .state
            .compare_exchange(QUEUED, CLAIMED, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        let payload = self
            .admission
            .payload
            .lock()
            .expect("admission payload lock poisoned")
            .take()
            .expect("claimed payload");
        Some((payload, ClaimedReply { tx: self.tx }))
    }
}
impl<T> ClaimedReply<T> {
    /// Answer the caller. A caller that already left is not an error.
    pub(super) fn answer(self, value: T) {
        let _ = self.tx.send(value);
    }
}
impl<P, T> Pending<'_, P, T> {
    /// Await the answer. Cancel-safe: the receiver stays in this handle, so a
    /// dropped `recv` future loses nothing. On closure, `withdraw()` decides
    /// `NeverClaimed` versus `Claimed`, and the pending is settled either way.
    ///
    /// Not to be called again after it returned an answer.
    pub(super) async fn recv(&mut self) -> Result<T, Unanswered> {
        if !self.closed {
            match (&mut self.rx).await {
                Ok(value) => {
                    self.settled = true;
                    return Ok(value);
                }
                Err(_) => self.closed = true,
            }
        }
        self.settled = true;
        Err(match self.withdraw() {
            Withdrawal::Withdrawn => Unanswered::NeverClaimed,
            Withdrawal::Claimed => Unanswered::Claimed,
        })
    }
    /// Non-blocking take; settles the pending when it returns `Some`. Safe
    /// after closure (returns `None`). Observing closure settles it too, as in
    /// [`recv`](Self::recv): the reply is gone, so there is no exchange left
    /// for `Drop` to abandon, whatever the claim state.
    pub(super) fn try_take(&mut self) -> Option<T> {
        if self.closed {
            return None;
        }
        match self.rx.try_recv() {
            Ok(value) => {
                self.settled = true;
                Some(value)
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                self.closed = true;
                self.settled = true;
                None
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
        }
    }
    /// CAS QUEUED -> WITHDRAWN; on winning, take the payload out of the cell
    /// and drop it here, after the lock is released. An observed WITHDRAWN is
    /// `Withdrawn`, an observed CLAIMED is `Claimed`. Idempotent.
    pub(super) fn withdraw(&mut self) -> Withdrawal {
        match self.admission.state.compare_exchange(
            QUEUED,
            WITHDRAWN,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                let payload = self
                    .admission
                    .payload
                    .lock()
                    .expect("admission payload lock poisoned")
                    .take();
                drop(payload);
                Withdrawal::Withdrawn
            }
            Err(WITHDRAWN) => Withdrawal::Withdrawn,
            Err(_) => Withdrawal::Claimed,
        }
    }
}
impl<P, T> Drop for Pending<'_, P, T> {
    /// If settled: nothing. Otherwise withdraw. `Withdrawn` leaves the
    /// connection alone. On `Claimed`, an answer already sitting in the
    /// oneshot means the exchange finished and the connection is left alone;
    /// otherwise the driver is still inside an exchange nobody will read, and
    /// `abandoned` is set.
    fn drop(&mut self) {
        if !self.settled && self.withdraw() == Withdrawal::Claimed && self.try_take().is_none() {
            self.abandoned.store(true, Ordering::Release);
        }
    }
}

/// The decision on a deadline that expired after admission.
pub(super) enum Expired<T> {
    /// The answer was already buffered: completion wins the tie.
    Answer(T),
    /// Withdrawal won: the driver will never write the command.
    Withdrawn,
    /// Claimed and unanswered: the exchange is the driver's.
    Claimed,
}
/// The one tie policy for a post-admission expiry, shared by the submit
/// helpers and IDLE's DONE wait. Withdraw first (`Withdrawn` is final: an
/// unclaimed command has no answer), then take any buffered answer, so an
/// answer that landed before the take is never thrown away.
pub(super) fn on_expiry<P, T>(pending: &mut Pending<'_, P, T>) -> Expired<T> {
    match pending.withdraw() {
        Withdrawal::Withdrawn => Expired::Withdrawn,
        Withdrawal::Claimed => pending.try_take().map_or(Expired::Claimed, Expired::Answer),
    }
}
/// Whether a reserved permit may still be sent: `false` when the deadline is
/// at or before `now`, or `cancelled` is set. `tokio::time::timeout_at` polls
/// its inner future before its timer, so a permit can be granted after the
/// deadline has gone; this re-check is what makes admission strict.
pub(super) fn may_send(deadline: Option<Instant>, now: Instant, cancelled: bool) -> bool {
    !cancelled && deadline.is_none_or(|deadline| deadline > now)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    fn flag() -> AtomicBool {
        AtomicBool::new(false)
    }

    #[test]
    fn a_withdrawn_reply_cannot_be_claimed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        assert_eq!(pending.withdraw(), Withdrawal::Withdrawn);
        assert!(reply.claim().is_none());
    }
    #[test]
    fn a_claimed_reply_cannot_be_withdrawn() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let _claimed = reply.claim().unwrap();
        assert_eq!(pending.withdraw(), Withdrawal::Claimed);
    }
    #[test]
    fn withdrawing_twice_reports_withdrawn_both_times() {
        let flag = flag();
        let (_reply, mut pending) = reply_channel::<_, ()>((), &flag);
        assert_eq!(pending.withdraw(), Withdrawal::Withdrawn);
        assert_eq!(pending.withdraw(), Withdrawal::Withdrawn);
    }
    #[test]
    fn claiming_hands_over_the_payload() {
        let flag = flag();
        let (reply, _pending) = reply_channel::<_, ()>(42, &flag);
        assert_eq!(reply.claim().unwrap().0, 42);
    }
    #[test]
    fn withdrawing_drops_the_payload_at_once() {
        let flag = flag();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let (_reply, mut pending) = reply_channel::<_, ()>(tx, &flag);
        assert_eq!(pending.withdraw(), Withdrawal::Withdrawn);
        assert_eq!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        );
    }
    #[tokio::test]
    async fn a_reply_dropped_before_its_claim_reads_never_claimed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply);
        assert_eq!(pending.recv().await, Err(Unanswered::NeverClaimed));
    }
    #[tokio::test]
    async fn a_reply_dropped_after_its_claim_reads_claimed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply.claim().unwrap());
        assert_eq!(pending.recv().await, Err(Unanswered::Claimed));
    }
    #[tokio::test]
    async fn a_pending_whose_claimed_reply_was_dropped_does_not_abandon() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply.claim().unwrap());
        assert_eq!(pending.recv().await, Err(Unanswered::Claimed));
        drop(pending);
        assert!(!flag.load(Ordering::Acquire));
    }
    #[test]
    fn a_pending_that_saw_its_claimed_reply_dropped_through_try_take_does_not_abandon() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply.claim().unwrap());
        assert!(pending.try_take().is_none());
        drop(pending);
        assert!(!flag.load(Ordering::Acquire));
    }
    #[test]
    fn on_expiry_takes_an_answer_that_arrived_before_expiry() {
        let flag = flag();
        let (reply, mut pending) = reply_channel((), &flag);
        reply.claim().unwrap().1.answer(42);
        assert!(matches!(on_expiry(&mut pending), Expired::Answer(42)));
        drop(pending);
        assert!(!flag.load(Ordering::Acquire));
    }
    #[test]
    fn may_send_refuses_an_expired_deadline_or_a_cancellation() {
        let now = Instant::now();
        for deadline in [Some(now), Some(now - std::time::Duration::from_secs(1))] {
            assert!(!may_send(deadline, now, false));
        }
        for deadline in [None, Some(now + std::time::Duration::from_secs(1))] {
            assert!(may_send(deadline, now, false));
            assert!(!may_send(deadline, now, true));
        }
    }
    #[test]
    fn on_expiry_withdraws_a_queued_command() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        assert!(matches!(on_expiry(&mut pending), Expired::Withdrawn));
        assert!(reply.claim().is_none());
    }
    #[test]
    fn on_expiry_reports_a_claimed_unanswered_command_as_claimed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let _claimed = reply.claim().unwrap();
        assert!(matches!(on_expiry(&mut pending), Expired::Claimed));
    }
    #[tokio::test]
    async fn pending_operations_are_safe_after_the_receiver_closed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply);
        assert!(pending.try_take().is_none());
        assert_eq!(pending.recv().await, Err(Unanswered::NeverClaimed));
        assert!(pending.try_take().is_none());
        assert_eq!(pending.recv().await, Err(Unanswered::NeverClaimed));
    }
    #[test]
    fn dropping_an_unanswered_claimed_pending_marks_the_connection_abandoned() {
        let flag = flag();
        let (reply, pending) = reply_channel::<_, ()>((), &flag);
        let _claimed = reply.claim().unwrap();
        drop(pending);
        assert!(flag.load(Ordering::Acquire));
    }
    #[test]
    fn dropping_a_claimed_pending_whose_answer_arrived_does_not_abandon() {
        let flag = flag();
        let (reply, pending) = reply_channel((), &flag);
        reply.claim().unwrap().1.answer(());
        drop(pending);
        assert!(!flag.load(Ordering::Acquire));
    }
    #[test]
    fn dropping_a_queued_pending_withdraws_without_abandoning() {
        let flag = flag();
        let (reply, pending) = reply_channel::<_, ()>((), &flag);
        drop(pending);
        assert!(reply.claim().is_none());
        assert!(!flag.load(Ordering::Acquire));
    }
}
