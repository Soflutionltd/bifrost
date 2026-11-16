//! Atomic admission shared by the caller and the driver.
//!
//! Every wire command carries one admission cell, shared by the caller's
//! [`Pending`] and the driver's [`Reply`]. The cell moves through four states:
//!
//! - QUEUED: submitted, not yet at the head of the driver's queue.
//! - CLAIMED: the driver won [`Reply::claim`] and holds the payload, but has
//!   neither offered a byte of the command to the stream nor answered it.
//!   This is where the driver prepares the command and waits for its first
//!   bandwidth grant.
//! - COMMITTED: the driver won [`CommitGate::commit`], the step it takes
//!   immediately before offering the command's first byte, and before
//!   publishing any answer ([`ClaimedReply::answer`] commits first).
//! - WITHDRAWN: the caller won [`Pending::withdraw`] from QUEUED or CLAIMED.
//!
//! Withdrawal and commitment are one compare-exchange race, so exactly one
//! side wins. A won withdrawal is provably never written and never answered:
//! its evidence is a true `Unsent`, and the connection stays reusable. A
//! committed command is the driver's exchange: a caller that leaves it with no
//! answer in hand reports `InFlight` and abandons the connection.
//!
//! The commit point sits BEFORE the first offer to the stream rather than at
//! the first socket progress on purpose. Once a byte is offered, a TLS or
//! COMPRESS layer may hold it while the socket counter (`stream::Tracked`)
//! has not moved, and nothing could later take it back out of that layer;
//! before the offer, no layer holds anything of the command, and the command
//! before it finished with a successful flush. So no rule about bytes
//! buffered above the socket is needed: withdrawal is simply impossible once
//! any could exist.
//!
//! COMMITTED is "the driver has irrevocably bound itself to writing or
//! answering", not "it has written or answered": between a won commit and the
//! oneshot send there is a window where a leaving caller conservatively reads
//! `InFlight` for a command that is only being refused. That window is
//! accepted; it is never unsafe.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;

const QUEUED: u8 = 0;
const CLAIMED: u8 = 1;
const COMMITTED: u8 = 2;
const WITHDRAWN: u8 = 3;

/// The state word and the driver's wake-up, shared by all three halves. The
/// payload lives apart from it so the driver can hold the gate without
/// naming the payload type.
pub(in crate::connection) struct CommitGate {
    state: AtomicU8,
    /// Woken by a withdrawal from CLAIMED, so a driver parked on its first
    /// bandwidth grant for a command nobody wants returns at once.
    wake: Notify,
}

impl CommitGate {
    /// Driver-owned setup has no queued caller command to withdraw. Its caller
    /// cancels by aborting the entire setup task and destroying the transport.
    pub(in crate::connection) fn committed() -> Self {
        Self {
            state: AtomicU8::new(COMMITTED),
            wake: Notify::new(),
        }
    }

    /// CAS CLAIMED -> COMMITTED. `true` also when already COMMITTED; `false`
    /// when the caller withdrew, in which case the driver must write nothing
    /// and answer nothing.
    pub(in crate::connection) fn commit(&self) -> bool {
        match self
            .state
            .compare_exchange(CLAIMED, COMMITTED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(COMMITTED) => true,
            Err(_) => false,
        }
    }

    /// Resolves once the caller has withdrawn. A notification is only a
    /// wake-up; the state word is the verdict, re-read after every wake.
    pub(in crate::connection) async fn withdrawn(&self) {
        loop {
            if self.state.load(Ordering::Acquire) == WITHDRAWN {
                return;
            }
            self.wake.notified().await;
        }
    }
}

/// Driver half. Travels inside the `DriverCommand`. Holds no payload of its
/// own: the payload is reachable only by winning [`claim`](Self::claim).
pub(super) struct Reply<P, T> {
    tx: oneshot::Sender<T>,
    gate: Arc<CommitGate>,
    payload: Arc<Mutex<Option<P>>>,
}

/// What the driver holds once it owns the command. The only way to answer.
pub(super) struct ClaimedReply<T> {
    tx: oneshot::Sender<T>,
    gate: Arc<CommitGate>,
}

/// Caller half. Lives in the submitting frame; dropping it unsettled
/// withdraws the command, or abandons the connection when the driver had
/// already committed it and no answer is in hand.
pub(super) struct Pending<'conn, P, T> {
    rx: oneshot::Receiver<T>,
    gate: Arc<CommitGate>,
    payload: Arc<Mutex<Option<P>>>,
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
    /// did): the driver will never write or answer this command.
    Withdrawn,
    /// The driver committed it first; it is or will be executed.
    Committed,
}

/// Why [`Pending::recv`] got no answer: the driver dropped the reply.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Unanswered {
    /// Dropped before the driver committed it (still queued, or claimed but
    /// not yet offered): nothing of it was written.
    NeverCommitted,
    /// Dropped after the driver committed it.
    Committed,
}

impl Unanswered {
    /// The caller's transmission phase for a driver death.
    pub(super) fn attempt(self) -> bifrost_types::TransmissionState {
        match self {
            Self::NeverCommitted => bifrost_types::TransmissionState::Unsent,
            Self::Committed => bifrost_types::TransmissionState::InFlight,
        }
    }
}

/// One admission cell holding `payload`, split into its driver and caller
/// halves. `abandoned` is the connection's flag, set by `Pending::drop` when
/// the caller leaves a committed exchange unanswered.
pub(super) fn reply_channel<P, T>(
    payload: P,
    abandoned: &AtomicBool,
) -> (Reply<P, T>, Pending<'_, P, T>) {
    let gate = Arc::new(CommitGate {
        state: AtomicU8::new(QUEUED),
        wake: Notify::new(),
    });
    let payload = Arc::new(Mutex::new(Some(payload)));
    let (tx, rx) = oneshot::channel();
    (
        Reply {
            tx,
            gate: Arc::clone(&gate),
            payload: Arc::clone(&payload),
        },
        Pending {
            rx,
            gate,
            payload,
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
    ///
    /// The payload is taken after the CAS, so a withdrawal from CLAIMED can
    /// land between the two; that withdrawal never touches the payload slot
    /// (see [`Pending::withdraw`]), so the take still finds it.
    #[expect(
        clippy::unwrap_in_result,
        reason = "a won claim guarantees the payload; poisoning is an invariant failure"
    )]
    pub(super) fn claim(self) -> Option<(P, ClaimedReply<T>)> {
        self.gate
            .state
            .compare_exchange(QUEUED, CLAIMED, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        let payload = self
            .payload
            .lock()
            .expect("admission payload lock poisoned")
            .take()
            .expect("claimed payload");
        Some((
            payload,
            ClaimedReply {
                tx: self.tx,
                gate: self.gate,
            },
        ))
    }
}
impl<T> ClaimedReply<T> {
    /// The gate the driver commits through before the command's first byte.
    pub(super) fn gate(&self) -> &CommitGate {
        &self.gate
    }

    /// Answer the caller, committing first: a command whose caller withdrew
    /// before the commit is never answered, so a won withdrawal never
    /// discards a buffered answer. A caller that already left a committed
    /// command is not an error.
    pub(super) fn answer(self, value: T) {
        if self.gate.commit() {
            let _ = self.tx.send(value);
        }
    }
}
impl<P, T> Pending<'_, P, T> {
    /// Await the answer. Cancel-safe: the receiver stays in this handle, so a
    /// dropped `recv` future loses nothing. On closure, `withdraw()` decides
    /// `NeverCommitted` versus `Committed`, and the pending is settled either
    /// way.
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
            Withdrawal::Withdrawn => Unanswered::NeverCommitted,
            Withdrawal::Committed => Unanswered::Committed,
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
    /// Withdraw from QUEUED or CLAIMED. From QUEUED the payload is taken out
    /// of the cell and dropped here, after the lock is released. From CLAIMED
    /// the payload belongs to the driver, which may not have taken it yet, so
    /// the slot is left alone and the driver is woken instead. An observed
    /// WITHDRAWN is `Withdrawn`, an observed COMMITTED is `Committed`.
    /// Idempotent.
    pub(super) fn withdraw(&mut self) -> Withdrawal {
        let mut current = self.gate.state.load(Ordering::Acquire);
        loop {
            match current {
                WITHDRAWN => return Withdrawal::Withdrawn,
                COMMITTED => return Withdrawal::Committed,
                _ => {}
            }
            match self.gate.state.compare_exchange(
                current,
                WITHDRAWN,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(QUEUED) => {
                    let payload = self
                        .payload
                        .lock()
                        .expect("admission payload lock poisoned")
                        .take();
                    drop(payload);
                    return Withdrawal::Withdrawn;
                }
                Ok(_) => {
                    self.gate.wake.notify_one();
                    return Withdrawal::Withdrawn;
                }
                Err(observed) => current = observed,
            }
        }
    }
}
impl<P, T> Drop for Pending<'_, P, T> {
    /// If settled: nothing. Otherwise withdraw. `Withdrawn` leaves the
    /// connection alone. On `Committed`, an answer already sitting in the
    /// oneshot means the exchange finished and the connection is left alone;
    /// otherwise the driver is still inside an exchange nobody will read, and
    /// `abandoned` is set.
    fn drop(&mut self) {
        if !self.settled && self.withdraw() == Withdrawal::Committed && self.try_take().is_none() {
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
    /// Committed and unanswered: the exchange is the driver's.
    Committed,
}
/// The one tie policy for a post-admission expiry, shared by the submit
/// helpers and IDLE's DONE wait. Withdraw first (`Withdrawn` is final: an
/// uncommitted command has no answer, because answering commits), then take
/// any buffered answer, so an answer that landed before the take is never
/// thrown away.
pub(super) fn on_expiry<P, T>(pending: &mut Pending<'_, P, T>) -> Expired<T> {
    match pending.withdraw() {
        Withdrawal::Withdrawn => Expired::Withdrawn,
        Withdrawal::Committed => pending
            .try_take()
            .map_or(Expired::Committed, Expired::Answer),
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
    fn a_committed_reply_cannot_be_withdrawn() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert!(claimed.gate().commit());
        assert_eq!(pending.withdraw(), Withdrawal::Committed);
    }
    #[test]
    fn a_claimed_uncommitted_reply_is_withdrawn_and_cannot_commit() {
        let flag = flag();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let (reply, mut pending) = reply_channel::<_, ()>(tx, &flag);
        let (payload, claimed) = reply.claim().unwrap();
        assert_eq!(pending.withdraw(), Withdrawal::Withdrawn);
        assert!(!claimed.gate().commit());
        // The payload stays the driver's: the withdrawal did not reach into
        // the slot the driver had already emptied.
        assert!(rx.try_recv().is_err());
        drop(payload);
        drop(pending);
        assert!(!flag.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn a_withdrawal_from_claimed_wakes_the_gate() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert_eq!(pending.withdraw(), Withdrawal::Withdrawn);
        // The stored notify permit makes this resolve even though nothing
        // was waiting when the withdrawal landed.
        claimed.gate().withdrawn().await;
    }
    #[test]
    fn an_answer_after_a_won_withdrawal_is_never_delivered() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, u8>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert!(matches!(on_expiry(&mut pending), Expired::Withdrawn));
        claimed.answer(7);
        assert!(pending.try_take().is_none());
    }
    #[test]
    fn an_answer_before_the_expiry_wins_the_tie() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, u8>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        claimed.answer(7);
        assert!(matches!(on_expiry(&mut pending), Expired::Answer(7)));
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
    async fn a_reply_dropped_before_its_claim_reads_never_committed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply);
        assert_eq!(pending.recv().await, Err(Unanswered::NeverCommitted));
    }
    #[tokio::test]
    async fn a_reply_dropped_after_its_claim_but_before_its_commit_reads_never_committed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply.claim().unwrap());
        assert_eq!(pending.recv().await, Err(Unanswered::NeverCommitted));
    }
    #[tokio::test]
    async fn a_reply_dropped_after_its_commit_reads_committed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert!(claimed.gate().commit());
        drop(claimed);
        assert_eq!(pending.recv().await, Err(Unanswered::Committed));
    }
    #[tokio::test]
    async fn a_pending_whose_committed_reply_was_dropped_does_not_abandon() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert!(claimed.gate().commit());
        drop(claimed);
        assert_eq!(pending.recv().await, Err(Unanswered::Committed));
        drop(pending);
        assert!(!flag.load(Ordering::Acquire));
    }
    #[test]
    fn a_pending_that_saw_its_committed_reply_dropped_through_try_take_does_not_abandon() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert!(claimed.gate().commit());
        drop(claimed);
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
    fn on_expiry_reports_a_committed_unanswered_command_as_committed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert!(claimed.gate().commit());
        assert!(matches!(on_expiry(&mut pending), Expired::Committed));
    }
    #[tokio::test]
    async fn pending_operations_are_safe_after_the_receiver_closed() {
        let flag = flag();
        let (reply, mut pending) = reply_channel::<_, ()>((), &flag);
        drop(reply);
        assert!(pending.try_take().is_none());
        assert_eq!(pending.recv().await, Err(Unanswered::NeverCommitted));
        assert!(pending.try_take().is_none());
        assert_eq!(pending.recv().await, Err(Unanswered::NeverCommitted));
    }
    #[test]
    fn dropping_an_unanswered_committed_pending_marks_the_connection_abandoned() {
        let flag = flag();
        let (reply, pending) = reply_channel::<_, ()>((), &flag);
        let (_, claimed) = reply.claim().unwrap();
        assert!(claimed.gate().commit());
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
