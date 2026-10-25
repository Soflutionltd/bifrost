//! Shared plaintext bandwidth admission and byte accounting for SMTP/LMTP.

use bifrost_net::{BandwidthBudget, BandwidthDirection, MeterSinkHandle};

/// Legacy atomic encoding for unlimited bandwidth. Budgets use `None` instead.
pub const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

// Keep SMTP/LMTP's adapter private to the two network stream implementations.
// BandwidthBudget and its Reservation own scheduling, grant sizes, epoch
// validation, conservative offered cancellation and tail-only refunds. This
// adapter shares those rules and the optional plaintext sink between the
// published blocking and Tokio transports; it does not own another schedule.
//
// The blocking half implements std::io without a Tokio runtime. It uses
// reserve/offer and sleeps the calling thread in at most one-second slices,
// rechecking cap and epoch so even an unlimited-then-recapped transition between
// checks discards the stale slot. Reservation::offer performs the final epoch
// and readiness validation under the shared budget lock. The polling loop
// repeats waiter orchestration, not grant pricing or installation policy;
// blocking_admission_readmits_after_an_unobserved_unlimited_epoch pins it.
// MeteredStream implements Tokio I/O only and cannot replace this wait path.
//
// The Tokio half explicitly admits each read-buffer fill and write before the
// connection arms its peer timer. Cumulative reply peer time excludes admission,
// while an absolute setup deadline includes it. A timeout around MeteredStream
// would include both waits; replacing this adapter must preserve the separate
// admission phase or move peer timing inside the wrapped stream. AsyncNetworkStream
// also retains the offered grant and original retry_len across Pending native-tls
// writes and cap retunes, then finishes and observes only successful plaintext
// I/O. Both directions allow only one outstanding admission on each stream.
//
// A shared blocking admission API could replace the wait loop independently;
// it would not remove these stream and deadline responsibilities. Reuse alone
// does not justify moving SMTP's I/O state into the shared Tokio stream wrapper.
/// Clones share the caller's budget across connections and transport pools.
#[derive(Clone)]
pub(crate) struct WireMetering {
    sink: Option<MeterSinkHandle>,
    budget: BandwidthBudget,
}

impl WireMetering {
    pub(crate) fn disabled() -> Self {
        Self::new(None, None)
    }

    pub(crate) fn new(sink: Option<MeterSinkHandle>, budget: Option<BandwidthBudget>) -> Self {
        Self {
            sink,
            budget: budget.unwrap_or_default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_enabled(&self) -> bool {
        self.sink.is_some() || self.budget.cap().is_some()
    }

    pub(crate) fn record_in(&self, n: usize) {
        if let Some(sink) = &self.sink {
            sink.record_bytes_in(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }

    pub(crate) fn record_out(&self, n: usize) {
        if let Some(sink) = &self.sink {
            sink.record_bytes_out(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }

    #[cfg(test)]
    pub(crate) fn reserve_out(&self, len: usize) -> Reservation {
        Reservation(self.budget.reserve(BandwidthDirection::Outbound, len))
    }

    pub(crate) fn blocking_admit(&self, direction: BandwidthDirection, len: usize) -> Reservation {
        self.blocking_admit_with_wait(direction, len, std::thread::sleep)
    }

    fn blocking_admit_with_wait(
        &self,
        direction: BandwidthDirection,
        len: usize,
        mut sleep: impl FnMut(std::time::Duration),
    ) -> Reservation {
        loop {
            let mut reservation = Reservation(self.budget.reserve(direction, len));
            loop {
                if reservation.offer() {
                    return reservation;
                }
                // This adapter sleeps on a blocking thread. With the normal
                // Tokio clock, `into_std()` and `std::Instant::now()` share
                // the same monotonic clock. Paused Tokio time intentionally
                // cannot advance a blocking thread's clock, so it is not a
                // valid harness for this wait path.
                let wait = reservation
                    .ready_at()
                    .into_std()
                    .saturating_duration_since(std::time::Instant::now());
                if wait.is_zero()
                    || self.budget.cap().is_none()
                    || reservation.epoch() != self.budget.epoch()
                {
                    break;
                }
                // No Tokio runtime is required by the blocking transport.
                // Poll policy changes at most one second apart.
                sleep(wait.min(std::time::Duration::from_secs(1)));
            }
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn admit(&self, direction: BandwidthDirection, len: usize) -> Reservation {
        Reservation(self.budget.admit(direction, len).await)
    }
}

impl std::fmt::Debug for WireMetering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireMetering")
            .field("metered", &self.sink.is_some())
            .field("cap", &self.budget.cap())
            .finish()
    }
}

pub(crate) struct Reservation(bifrost_net::Reservation);

impl std::ops::Deref for Reservation {
    type Target = bifrost_net::Reservation;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Reservation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Reservation {
    pub(crate) fn finish(self, used: usize) {
        self.0.finish(used);
    }
}
impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservation")
            .field("granted", &self.granted())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::Instant;

    fn capped(cap: u64) -> (BandwidthBudget, WireMetering) {
        let budget = BandwidthBudget::new(Some(cap));
        (budget.clone(), WireMetering::new(None, Some(budget)))
    }

    fn finish(mut reservation: Reservation, used: usize) {
        assert!(reservation.offer());
        reservation.finish(used);
    }

    #[test]
    fn a_disabled_metering_never_throttles() {
        let meter = WireMetering::disabled();
        assert!(!meter.is_enabled());
        let mut grant = meter.reserve_out(100);
        assert!(grant.offer());
        assert_eq!(grant.granted(), 100);
    }

    #[test]
    fn the_unlimited_sentinel_is_not_a_very_large_cap() {
        assert_eq!(UNLIMITED_BANDWIDTH, u64::MAX);
        assert_eq!(WireMetering::disabled().budget.cap(), None);
        let (_, finite) = capped(u64::MAX);
        assert_eq!(finite.budget.cap(), Some(u64::MAX));
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_schedule_admits_one_second_at_once_and_then_queues() {
        let (_, meter) = capped(10);
        finish(meter.reserve_out(10), 10);
        assert_eq!(
            meter.reserve_out(10).ready_at(),
            Instant::now() + Duration::from_secs(1)
        );
    }

    #[test]
    fn a_reservation_covers_at_most_one_second_of_cap() {
        let (_, meter) = capped(10);
        assert_eq!(meter.reserve_out(100).granted(), 10);
    }

    #[test]
    fn a_reservation_never_exceeds_max_offer() {
        let meter = WireMetering::disabled();
        assert_eq!(
            meter.reserve_out(usize::MAX).granted(),
            bifrost_net::budget::MAX_OFFER
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_connection_queues_behind_the_first() {
        let (_, meter) = capped(10);
        let sibling = meter.clone();
        finish(meter.reserve_out(10), 10);
        assert_eq!(
            sibling.reserve_out(10).ready_at(),
            Instant::now() + Duration::from_secs(1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_reservation_gives_its_time_back() {
        let (_, meter) = capped(10);
        drop(meter.reserve_out(10));
        let mut replacement = meter.reserve_out(10);
        assert!(replacement.offer());
    }

    #[tokio::test(start_paused = true)]
    async fn an_installed_reservation_stays_charged() {
        let (_, meter) = capped(10);
        let mut offered = meter.reserve_out(10);
        assert!(offered.offer());
        drop(offered);
        assert_eq!(
            meter.reserve_out(10).ready_at(),
            Instant::now() + Duration::from_secs(1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lowering_the_cap_does_not_reprice_time_already_committed() {
        let (budget, meter) = capped(100);
        finish(meter.reserve_out(100), 100);
        let mut queued = meter.reserve_out(100);
        budget.set_cap(Some(1));
        assert_eq!(queued.ready_at(), Instant::now() + Duration::from_secs(1));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(queued.offer());
        assert_eq!(queued.granted(), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn installing_after_a_cap_cut_keeps_the_issued_allowance() {
        let (budget, meter) = capped(100);
        let mut grant = meter.reserve_out(100);
        budget.set_cap(Some(1));
        assert!(grant.offer());
        assert_eq!(grant.granted(), 100);
    }

    #[test]
    fn a_lifted_cap_stops_admission_on_the_next_call() {
        let (budget, meter) = capped(1);
        budget.set_cap(None);
        let mut grant = meter.reserve_out(100);
        assert!(grant.offer());
        assert_eq!(grant.granted(), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn reservation_durations_round_up() {
        let (_, meter) = capped(3);
        finish(meter.reserve_out(3), 3);
        let grant = meter.reserve_out(1);
        assert_eq!(
            grant.ready_at(),
            Instant::now() + Duration::from_nanos(333_333_334)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn blocking_admission_readmits_after_an_unobserved_unlimited_epoch() {
        let (budget, meter) = capped(10);
        finish(meter.reserve_out(10), 10);
        let _queued: Vec<_> = (0..10).map(|_| meter.reserve_out(10)).collect();
        let mut waits = 0;
        let grant = meter.blocking_admit_with_wait(BandwidthDirection::Outbound, 10, |_| {
            waits += 1;
            assert_eq!(waits, 1, "slept again on the invalidated reservation");
            // Both changes land between blocking policy checks.
            budget.set_cap(None);
            budget.set_cap(Some(1));
        });
        assert_eq!(waits, 1);
        assert_eq!(grant.epoch(), budget.epoch());
        assert_eq!(grant.granted(), 1);
        grant.finish(1);
    }

    #[test]
    fn an_oversized_read_is_bounded_before_io() {
        let (_, meter) = capped(10);
        let grant = meter.blocking_admit(BandwidthDirection::Inbound, 1000);
        assert_eq!(grant.granted(), 10);
        grant.finish(4);
    }

    #[tokio::test(start_paused = true)]
    async fn the_inbound_and_outbound_budgets_are_independent() {
        let (_, meter) = capped(10);
        finish(meter.reserve_out(10), 10);
        let grant = meter.blocking_admit(BandwidthDirection::Inbound, 10);
        grant.finish(10);
        assert_eq!(
            meter.reserve_out(10).ready_at(),
            Instant::now() + Duration::from_secs(1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lowering_the_cap_does_not_reprice_inbound_slots_already_owed() {
        let (budget, meter) = capped(10);
        meter
            .blocking_admit(BandwidthDirection::Inbound, 10)
            .finish(10);
        let mut queued = Reservation(budget.reserve(BandwidthDirection::Inbound, 10));
        budget.set_cap(Some(1));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(queued.offer());
        assert_eq!(queued.granted(), 10);
    }
}
