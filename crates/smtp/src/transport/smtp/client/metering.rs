//! Per-account byte accounting and bandwidth capping for the raw SMTP
//! socket.
//!
//! SMTP was the only protocol in the workspace whose bytes reached the
//! wire unmetered, which made `Account::set_bandwidth_cap` silently
//! PARTIAL rather than absent: an IMAP-shaped account honoured the cap on
//! its fetch traffic and ignored it on the send path, and the send path
//! is the upstream-heavy one a cap usually exists to protect.
//!
//! The two directions are capped by different mechanisms, on purpose.
//!
//! INBOUND charges after the fact: a `ByteBucket` debited by the bytes a
//! read delivered RETURNS the debt it wants slept rather than sleeping
//! itself, so one bucket serves the async `poll_read` and the blocking
//! `Read`. Nothing can be gated before a read - the peer decides how much
//! arrives.
//!
//! OUTBOUND is ADMITTED before the bytes go out, through an
//! `OutboundSchedule` shared by every connection the transport dials. A
//! charge-after-accept bucket cannot cap a pool: each connection's first
//! write is ungated whatever the shared balance owes, so redialing - or
//! simply running several connections at once - evades the cap entirely.
//! See `OutboundSchedule` for the mechanism and the bound it gives.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// The bucket refills on the SAME clock the async funnel sleeps on.
///
/// `AsyncNetworkStream` parks the debt this bucket returns as a
/// `tokio::time::Sleep`, so a bucket measuring refill on `std::time::Instant`
/// reads a different clock from the timer that pays the debt off. The two
/// disagree wherever tokio's clock is not the system one - which under
/// `start_paused` test time means virtual waiting never refills the bucket at
/// all, so a paused-time test accumulates debt that no amount of waiting can
/// retire and any assertion about elapsed transfer time measures the artifact.
/// `tokio::time::Instant::now()` falls back to the system clock when no tokio
/// clock is in force, so the blocking funnel (which sleeps its own thread) is
/// unaffected. Same reasoning as `AsyncDeadline`, which had to move for the
/// same reason.
#[cfg(feature = "tokio")]
use tokio::time::Instant;

#[cfg(not(feature = "tokio"))]
use std::time::Instant;

use bifrost_net::MeterSinkHandle;

/// Sentinel meaning "no cap" in the shared atomic. Matches the encoding
/// `bifrost-imap` uses so a consumer can drive both from one value.
// pub: consumers write this into the shared cap atomic to mean "no cap".
pub const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

fn bandwidth_cap_from_raw(raw: u64) -> Option<u64> {
    (raw != UNLIMITED_BANDWIDTH).then_some(raw.max(1))
}

/// The most any single socket write offers, capped or not.
///
/// Under a cap the offer is also limited to one second of it (see
/// `OutboundSchedule`); this constant is what bounds an UNCAPPED offer. It
/// exists for one transition: a TLS write that returned `Pending` must be
/// retried with the same length (OpenSSL refuses a shorter retry after
/// `WANT_WRITE`), so a write offered while uncapped and still pending when a
/// cap is imposed completes as offered and uncharged. Bounding every offer
/// bounds that one-time exception at one `MAX_OFFER` per live connection.
/// 256 KiB is far above a TLS record (16 KiB), so the cost is syscall count
/// only.
pub(crate) const MAX_OFFER: usize = 256 * 1024;

/// Byte accounting plus optional throttling for one connection.
///
/// Cheap to clone; every clone shares the same buckets, schedule and cap,
/// so a pooled transport's connections throttle against ONE budget rather
/// than each getting the full cap. Clones of a configured transport BUILDER
/// share it too, so the budget spans every pool built from one
/// `bandwidth_metering` call.
#[derive(Clone)]
pub(crate) struct WireMetering {
    sink: Option<MeterSinkHandle>,
    bandwidth_cap: Option<Arc<AtomicU64>>,
    in_bucket: ByteBucket,
    out_schedule: OutboundSchedule,
}

impl WireMetering {
    /// No sink and no cap: every operation is a no-op returning no debt.
    /// The shape every non-account caller of this crate gets.
    pub(crate) fn disabled() -> Self {
        Self::new(None, None)
    }

    pub(crate) fn new(
        sink: Option<MeterSinkHandle>,
        bandwidth_cap: Option<Arc<AtomicU64>>,
    ) -> Self {
        let initial = bandwidth_cap
            .as_ref()
            .and_then(|cap| bandwidth_cap_from_raw(cap.load(Ordering::Relaxed)));
        Self {
            sink,
            bandwidth_cap,
            in_bucket: ByteBucket::new(initial),
            out_schedule: OutboundSchedule::new(),
        }
    }

    /// True when anything is being recorded or capped. Lets the hot path
    /// skip the bucket entirely on the common unmetered build.
    pub(crate) fn is_enabled(&self) -> bool {
        self.sink.is_some() || self.bandwidth_cap.is_some()
    }

    /// Account for `n` inbound bytes; returns how long the caller must
    /// wait before reading more, if the cap says so.
    pub(crate) fn record_in(&self, n: usize) -> Option<Duration> {
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        if let Some(sink) = &self.sink {
            sink.record_bytes_in(n);
        }
        self.in_bucket.take(n, self.cap_now())
    }

    /// Report `n` outbound bytes the socket ACCEPTED to the sink.
    ///
    /// Accounting only: the cap was enforced when the write was admitted
    /// (`reserve_out`), so nothing is charged or owed here. Offered or
    /// reserved bytes are never reported - a cancelled write would otherwise
    /// count bytes that never left.
    pub(crate) fn record_out(&self, n: usize) {
        if let Some(sink) = &self.sink {
            sink.record_bytes_out(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }

    /// Reserve outbound admission for a write of up to `len` bytes, or `None`
    /// when no cap is in force.
    ///
    /// The reservation covers `min(len, one second of the cap, MAX_OFFER)`
    /// bytes and becomes usable at its `ready_at`. Dropped before it is
    /// installed, it gives its time back. See `OutboundSchedule`.
    pub(crate) fn reserve_out(&self, len: usize) -> Option<Reservation> {
        let cap = self.cap_now()?;
        Some(self.out_schedule.reserve(len, cap))
    }

    /// The cap in force right now, `None` when uncapped. Read fresh on every
    /// call: a consumer can retune it at any time through
    /// `Account::set_bandwidth_cap`, and a connection that snapshotted it at
    /// construction would hold a stale budget for its whole pooled lifetime.
    pub(crate) fn cap_now(&self) -> Option<u64> {
        self.bandwidth_cap
            .as_ref()
            .and_then(|cap| bandwidth_cap_from_raw(cap.load(Ordering::Relaxed)))
    }
}

impl std::fmt::Debug for WireMetering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireMetering")
            .field("metered", &self.sink.is_some())
            .field("capped", &self.bandwidth_cap.is_some())
            .finish()
    }
}

#[derive(Clone)]
struct ByteBucket {
    state: Arc<Mutex<ByteBucketState>>,
}

struct ByteBucketState {
    /// Allowed to go NEGATIVE. A transfer larger than one second of
    /// budget borrows against future seconds and the debt is slept in
    /// full, rather than being clamped to a single second's wait - a
    /// clamp would silently let a low cap be exceeded (a 1 B/s cap with
    /// 16 KiB writes would behave like several hundred B/s).
    tokens: f64,
    last_refill: Instant,
    /// The cap `tokens` is denominated in. Tokens are BYTES and debt is
    /// read back as `-tokens / cap`, so a cap change between two charges
    /// would re-price debt already owed: 1 MB charged at 1 MB/s is one
    /// second of debt, and re-reading it at a 1 KB/s cap makes it ~1000
    /// seconds - which the reader then waits out, postponing the reply
    /// deadline by far more than the traffic ever owed. Rescaling on the way
    /// in keeps the debt fixed in SECONDS, so a retune changes the future rate
    /// without repricing the past.
    priced_at: Option<f64>,
}

impl ByteBucket {
    fn new(cap: Option<u64>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ByteBucketState {
                tokens: cap.map_or(0.0, |cap| cap as f64),
                last_refill: Instant::now(),
                priced_at: cap.map(|cap| cap as f64),
            })),
        }
    }

    /// Refill by elapsed time, debit `n`, and report the resulting debt.
    ///
    /// Synchronous by design: the caller does the waiting, so the mutex
    /// guard cannot be held across a sleep or an await.
    fn take(&self, n: u64, cap: Option<u64>) -> Option<Duration> {
        let cap = cap?;
        if cap == 0 || n == 0 {
            return None;
        }
        let cap_f = cap as f64;
        let mut state = self.lock_state();
        // Re-denominate any standing balance into the cap now in force
        // before it is read as a duration. See `priced_at`.
        if let Some(previous) = state.priced_at
            && previous > 0.0
            && (previous - cap_f).abs() > f64::EPSILON
        {
            state.tokens *= cap_f / previous;
        }
        state.priced_at = Some(cap_f);
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        // Credit never exceeds one second of budget, so an idle
        // connection cannot bank capacity and then burst past the cap.
        state.tokens = (state.tokens + elapsed * cap_f).min(cap_f);
        state.last_refill = now;
        state.tokens -= n as f64;
        (state.tokens < 0.0).then(|| Duration::from_secs_f64(-state.tokens / cap_f))
    }

    fn lock_state(&self) -> MutexGuard<'_, ByteBucketState> {
        self.state.lock().unwrap_or_else(|poisoned| {
            #[cfg(feature = "tracing")]
            tracing::warn!("SMTP byte bucket lock poisoned; recovering metering state");
            poisoned.into_inner()
        })
    }
}

/// Shared outbound admission: every write is RESERVED a slot of time before
/// any of its bytes reach the socket.
///
/// The state is one theoretical arrival time, `tat`: the moment by which every
/// reservation issued so far has been paid for at the cap it was issued under.
/// A request for `o` bytes at cap `c` takes `end = max(tat, now) + o/c`, moves
/// `tat` to `end`, and may start once `end` is no more than one second ahead of
/// now - so an idle schedule admits one second of cap at once and every byte
/// after that waits its turn. Reservations are FIFO by request time, and a
/// waiter that is slow, unpolled or gone holds a span of time rather than a
/// place in a queue: everyone behind it was handed their own slot already.
/// A waiter therefore waits at most about one second per connection that
/// held a reservation or allowance when it REQUESTED, plus one - counted at
/// request time, because an issued slot never moves.
///
/// Why this shape and not the bucket the inbound side uses. Charging after the
/// socket accepted the bytes cannot cap a pool, since a connection's first
/// write is then ungated whatever the shared balance owes. Admission has to
/// happen first, and it has to be ATOMIC against the shared state - a check
/// followed by a write lets every connection pass the same balance. A
/// reservation is that atomic step; its wait happens afterwards and outside
/// the lock.
///
/// Time is in SECONDS, so a retune never reprices what is already committed:
/// a slot issued at the old cap keeps its place, and the new cap prices only
/// later requests. Durations round UP, so truncation can never let the rate
/// creep past the cap.
///
/// The bound this gives, for a fixed cap `c`: in any window of length `T`,
/// accepted bytes are at most `c * (T + 1 + M)`, where `M` counts every live
/// connection sharing this schedule - across every pool built from one
/// configured builder, since builder clones share it. Each live connection
/// holds at most one reservation or installed allowance of at most one second,
/// and that is the `M` term: a write admitted earlier and then held up by its
/// socket can complete late, alongside writes admitted on time. A bound
/// independent of the connection count is not on offer without making a
/// stalled peer block its siblings or charging our own waiting to the peer's
/// timeout, and both of those are worse. The cap is a CEILING on the long-run
/// rate, not a promise of throughput: cancellations, forfeited allowances and
/// the gaps below can leave capacity unused.
///
/// A reservation dropped before it was used gives its time back (see
/// `Reservation`) to requests made AFTER the drop; slots already issued keep
/// their deadlines. So cancellation never makes anyone wait longer than had the
/// cancelled write gone ahead, but it does not advance waiters already queued
/// behind it either - doing that would need a movable schedule and cross-waiter
/// wakeups that none of the guarantees here require. The bound above already
/// counts outstanding reservations, which is what keeps the give-back from
/// opening a burst. The one exception to the
/// accounting is a write offered while UNCAPPED and still pending when a cap is
/// imposed: TLS requires it to be retried at its original length, so it
/// completes uncharged - at most `MAX_OFFER` per live connection, once, at the
/// moment of imposition.
#[derive(Clone)]
struct OutboundSchedule {
    state: Arc<Mutex<ScheduleState>>,
}

struct ScheduleState {
    /// See `OutboundSchedule`. `None` until the first reservation; read as
    /// "long ago", so an idle schedule starts with its full second of credit.
    tat: Option<Instant>,
}

/// One second: the credit an idle schedule extends, and the most any one
/// reservation covers.
const CREDIT: Duration = Duration::from_secs(1);

impl OutboundSchedule {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ScheduleState { tat: None })),
        }
    }

    fn reserve(&self, len: usize, cap: u64) -> Reservation {
        let cap = cap.max(1);
        let bytes = reservation_bytes(len, cap);
        let duration = duration_at(bytes, cap);
        let now = Instant::now();
        let mut state = self.lock_state();
        let start = state.tat.map_or(now, |tat| tat.max(now));
        let end = start + duration;
        state.tat = Some(end);
        Reservation {
            schedule: Some(self.clone()),
            bytes,
            duration,
            ready_at: end.checked_sub(CREDIT).unwrap_or(now).max(now),
        }
    }

    /// Give back an unused reservation's time.
    ///
    /// Plain subtraction: slots already handed to later requesters do not
    /// move, so a request made after this one may be scheduled into time a
    /// sibling also holds. That overlap is real, and it is covered - the bound
    /// counts every outstanding reservation, and the freed time belongs to a
    /// reservation that was never sent, so the same capacity cannot be both
    /// sent and given back.
    fn release(&self, duration: Duration) {
        let mut state = self.lock_state();
        if let Some(tat) = state.tat {
            state.tat = tat.checked_sub(duration).or(Some(tat));
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ScheduleState> {
        self.state.lock().unwrap_or_else(|poisoned| {
            #[cfg(feature = "tracing")]
            tracing::warn!("SMTP outbound schedule lock poisoned; recovering metering state");
            poisoned.into_inner()
        })
    }
}

/// How many bytes one reservation covers: the request, but never more than
/// one second of the cap or `MAX_OFFER`, and never zero.
fn reservation_bytes(len: usize, cap: u64) -> usize {
    let per_second = usize::try_from(cap.max(1)).unwrap_or(usize::MAX);
    len.clamp(1, per_second.min(MAX_OFFER))
}

/// `bytes / cap` seconds, rounded UP to the nanosecond.
fn duration_at(bytes: usize, cap: u64) -> Duration {
    let nanos = (bytes as u128 * 1_000_000_000).div_ceil(u128::from(cap.max(1)));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// A slot of outbound time, held between being reserved and being used.
///
/// Dropping one that was never `install`ed gives its time back to the
/// schedule - that is what makes a write cancelled while waiting (a dropped
/// send future, a setup deadline that ran out) cost later requests nothing.
/// Reservations issued before the drop keep their slots; see
/// `OutboundSchedule`. Once installed, the bytes are committed: an allowance a stream then
/// abandons stays charged, because some of it may already have produced TLS
/// ciphertext on the wire.
pub(crate) struct Reservation {
    /// `None` once installed, so `Drop` gives nothing back.
    schedule: Option<OutboundSchedule>,
    bytes: usize,
    duration: Duration,
    ready_at: Instant,
}

impl Reservation {
    /// When the reservation may be used.
    // Async-only: the blocking writer waits on `remaining` instead.
    #[cfg(feature = "tokio")]
    pub(crate) fn ready_at(&self) -> Instant {
        self.ready_at
    }

    /// How long until the reservation may be used, zero once it may.
    pub(crate) fn remaining(&self) -> Duration {
        self.ready_at.saturating_duration_since(Instant::now())
    }

    /// Commit the reservation and return how many bytes it allows, shrunk to
    /// one second of `cap_now` if the cap fell while it waited.
    ///
    /// Shrinking is legal here and nowhere later: nothing has been offered to
    /// the socket yet, so no TLS retry depends on the length. The reserved
    /// time is not refunded - keeping it is conservative. It is never grown:
    /// a cap that ROSE while the reservation waited governs the next one.
    pub(crate) fn install(mut self, cap_now: u64) -> usize {
        self.schedule = None;
        self.bytes
            .min(usize::try_from(cap_now.max(1)).unwrap_or(usize::MAX))
    }
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservation")
            .field("bytes", &self.bytes)
            .field("duration", &self.duration)
            .field("installed", &self.schedule.is_none())
            .finish()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(schedule) = self.schedule.take() {
            schedule.release(self.duration);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capped(bps: u64) -> (Arc<AtomicU64>, WireMetering) {
        let cap = Arc::new(AtomicU64::new(bps));
        let metering = WireMetering::new(None, Some(Arc::clone(&cap)));
        (cap, metering)
    }

    /// Assert a wait to within the few milliseconds a synchronous test spends
    /// between taking a reservation and reading it back.
    fn assert_wait(reservation: &Reservation, expected: Duration) {
        let wait = reservation.remaining();
        let slack = Duration::from_millis(50);
        assert!(
            wait <= expected && wait + slack >= expected,
            "expected a wait of about {expected:?}, got {wait:?}"
        );
    }

    #[test]
    fn a_disabled_metering_never_throttles() {
        let metering = WireMetering::disabled();
        assert!(!metering.is_enabled());
        assert!(metering.reserve_out(1_000_000).is_none());
        assert_eq!(metering.record_in(1_000_000), None);
    }

    /// A cap of `UNLIMITED_BANDWIDTH` is the "no cap" sentinel, not a cap
    /// of `u64::MAX` bytes per second.
    #[test]
    fn the_unlimited_sentinel_is_not_a_very_large_cap() {
        let (_cap, metering) = capped(UNLIMITED_BANDWIDTH);
        assert!(metering.reserve_out(4096).is_none());
    }

    /// An idle schedule extends one second of credit: the first second of cap
    /// goes at once, the next waits its turn.
    #[test]
    fn an_idle_schedule_admits_one_second_at_once_and_then_queues() {
        let (_cap, metering) = capped(1000);
        let first = metering.reserve_out(1000).expect("capped");
        assert_wait(&first, Duration::ZERO);
        let second = metering.reserve_out(1000).expect("capped");
        assert_wait(&second, Duration::from_secs(1));
        let third = metering.reserve_out(500).expect("capped");
        assert_wait(&third, Duration::from_millis(1500));
    }

    /// One reservation covers at most one second of the cap, so a large write
    /// is admitted a second at a time and owes proportional time overall: a
    /// 1 B/s cap stays 1 B/s whatever size the writes are.
    #[test]
    fn a_reservation_covers_at_most_one_second_of_cap() {
        let (_cap, metering) = capped(1000);
        let first = metering.reserve_out(10_000).expect("capped");
        assert_eq!(first.install(1000), 1000);
        // The rest queue a second apart. Held, not dropped: a dropped
        // reservation gives its time back.
        let queued: Vec<Reservation> = (0..9)
            .map(|_| metering.reserve_out(10_000).expect("capped"))
            .collect();
        assert_wait(&queued[8], Duration::from_secs(9));
    }

    /// `MAX_OFFER` bounds a reservation even under a cap above it.
    #[test]
    fn a_reservation_never_exceeds_max_offer() {
        let (_cap, metering) = capped(u64::MAX - 1);
        let reservation = metering.reserve_out(usize::MAX).expect("capped");
        assert_eq!(reservation.install(u64::MAX - 1), MAX_OFFER);
    }

    /// The redial escape this schedule exists to close: admission is shared,
    /// so a second connection's FIRST write waits behind the first
    /// connection's, instead of each connection getting the cap's credit for
    /// free. Both connections are clones of one metering, exactly as a pooled
    /// transport hands them out.
    #[test]
    fn a_second_connection_queues_behind_the_first() {
        let (_cap, metering) = capped(1000);
        let first_connection = metering.clone();
        let second_connection = metering.clone();
        let first = first_connection.reserve_out(1000).expect("capped");
        assert_wait(&first, Duration::ZERO);
        let second = second_connection.reserve_out(1000).expect("capped");
        assert_wait(&second, Duration::from_secs(1));
    }

    /// A reservation dropped before it was installed gives its time back, so
    /// a cancelled write pushes no later write back.
    #[test]
    fn a_dropped_reservation_gives_its_time_back() {
        let (_cap, metering) = capped(1000);
        let first = metering.reserve_out(1000).expect("capped");
        let _ = first.install(1000);
        let abandoned = metering.reserve_out(1000).expect("capped");
        assert_wait(&abandoned, Duration::from_secs(1));
        drop(abandoned);
        let next = metering.reserve_out(1000).expect("capped");
        assert_wait(&next, Duration::from_secs(1));
    }

    /// An installed reservation is committed: abandoning the allowance does
    /// not refund it, because some of it may already be on the wire.
    #[test]
    fn an_installed_reservation_stays_charged() {
        let (_cap, metering) = capped(1000);
        let first = metering.reserve_out(1000).expect("capped");
        let _ = first.install(1000);
        let next = metering.reserve_out(1000).expect("capped");
        assert_wait(&next, Duration::from_secs(1));
    }

    /// Time already committed is fixed in SECONDS. Lowering the cap by two
    /// orders of magnitude does not stretch the second the earlier write was
    /// admitted for into a hundred; the new cap prices only the new request.
    #[test]
    fn lowering_the_cap_does_not_reprice_time_already_committed() {
        let (cap, metering) = capped(1000);
        let first = metering.reserve_out(1000).expect("capped");
        let _ = first.install(1000);
        cap.store(10, Ordering::Relaxed);
        let next = metering.reserve_out(10).expect("capped");
        assert_wait(&next, Duration::from_secs(1));
    }

    /// A reservation that waited through a cap cut is shrunk to one second of
    /// the new cap when installed; nothing has been offered yet, so the length
    /// is still free to change.
    #[test]
    fn installing_after_a_cap_cut_shrinks_the_allowance() {
        let (_cap, metering) = capped(1000);
        let reservation = metering.reserve_out(1000).expect("capped");
        assert_eq!(reservation.install(10), 10);
    }

    /// Lifting the cap ends admission on the next call.
    #[test]
    fn a_lifted_cap_stops_admission_on_the_next_call() {
        let (cap, metering) = capped(1000);
        let _ = metering.reserve_out(1000).expect("capped").install(1000);
        cap.store(UNLIMITED_BANDWIDTH, Ordering::Relaxed);
        assert!(metering.reserve_out(1_000_000).is_none());
    }

    /// Durations round UP, so truncation can never let the rate creep past
    /// the cap: one byte at 3 B/s is a third of a second, not a hair under.
    #[test]
    fn reservation_durations_round_up() {
        assert_eq!(duration_at(1, 3), Duration::from_nanos(333_333_334));
        assert_eq!(duration_at(3, 3), Duration::from_secs(1));
    }

    /// The property the inbound debt model exists for: a transfer far larger
    /// than the cap owes proportional time, NOT one second. Clamping here is
    /// what would let a 1 B/s cap run at hundreds of B/s.
    #[test]
    fn an_oversized_read_owes_time_proportional_to_its_size() {
        let (_cap, metering) = capped(1000);

        // Burn the initial second of budget, then overspend by 10x.
        assert_eq!(metering.record_in(1000), None);
        let debt = metering.record_in(10_000).expect("an overspend owes time");
        assert!(
            debt >= Duration::from_secs(9) && debt <= Duration::from_secs(11),
            "10_000 bytes at 1000 B/s is about 10s, got {debt:?}"
        );
    }

    /// In and out are separate budgets - a large send must not throttle
    /// the reply that follows it.
    #[test]
    fn the_inbound_and_outbound_budgets_are_independent() {
        let (_cap, metering) = capped(1000);

        let _ = metering.reserve_out(1000).expect("capped").install(1000);
        let queued = metering.reserve_out(1000).expect("capped");
        assert_wait(&queued, Duration::from_secs(1));
        assert_eq!(
            metering.record_in(500),
            None,
            "inbound has its own untouched budget"
        );
    }

    /// Inbound debt is owed in SECONDS, not in bytes-at-whatever-cap-is-
    /// current. Tokens are bytes and the debt is `-tokens / cap`, so lowering
    /// the cap between two charges used to re-price debt already owed: one
    /// second's worth at the old cap read back as a thousand seconds at the
    /// new one.
    #[test]
    fn lowering_the_cap_does_not_reprice_inbound_debt_already_owed() {
        let (cap, metering) = capped(1_000_000);

        // Burn the initial second, then owe about one more second at 1 MB/s.
        assert_eq!(metering.record_in(1_000_000), None);
        let debt = metering
            .record_in(1_000_000)
            .expect("a second megabyte overspends");
        assert!(
            debt <= Duration::from_millis(1100),
            "1 MB at 1 MB/s is about a second, got {debt:?}"
        );

        // A consumer retunes down by three orders of magnitude. The debt
        // standing from the previous charge is a second of time, and it
        // must stay a second of time.
        cap.store(1_000, Ordering::Relaxed);
        let debt = metering.record_in(1).expect("still in debt");
        assert!(
            debt <= Duration::from_millis(1100),
            "the outstanding debt was priced at the old cap; got {debt:?}"
        );
    }
}
