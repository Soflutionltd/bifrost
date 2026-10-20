//! Per-account bandwidth metering.
//!
//! HTTP request and response bodies route through the meter so the
//! sync engine can read `bytes/second` per account without each
//! protocol crate maintaining its own counters. IMAP and SMTP feed
//! the same meter through `MeterSink` because the engine needs one
//! reading per account regardless of underlying transport.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub use crate::budget::{BandwidthBudget, BandwidthDirection, BandwidthRegistry, Reservation};

use crate::AccountId;

/// One second's worth of byte counts. Ten of these in a ring buffer
/// form the sliding `observed_bps` window. Tracking in/out separately
/// makes "uploading 50 MB while downloading something else" readable.
#[derive(Copy, Clone, Default)]
pub(crate) struct WindowSample {
    pub(crate) bytes_in: u64,
    pub(crate) bytes_out: u64,
}

/// Sliding 10-second window of per-second byte counts. Smooths the
/// `observed_bps` reading so a one-shot burst does not pin the
/// reading at the burst peak forever.
pub(crate) struct RateWindow {
    /// Ten one-second buckets indexed by `head`.
    pub(crate) samples: [WindowSample; 10],
    /// Index of the bucket currently being filled.
    pub(crate) head: usize,
    /// Instant at which the head bucket was opened. Each second we
    /// advance `head` and zero the newly current bucket.
    pub(crate) head_started: Instant,
    /// Instant the window itself was first constructed. Used so the
    /// warm-up bps reading divides by the elapsed window instead of
    /// the full ten seconds and under-reports.
    pub(crate) created_at: Instant,
}

impl RateWindow {
    /// Construct an empty window anchored at the given instant.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            samples: [WindowSample::default(); 10],
            head: 0,
            head_started: now,
            created_at: now,
        }
    }

    /// Roll the head bucket forward by the number of whole seconds
    /// that have elapsed since the head was last opened. Zeros each
    /// intermediate bucket. Bounded at 10 steps because beyond that
    /// the entire window is stale anyway.
    fn advance(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.head_started);
        let steps_full = elapsed.as_secs();
        if steps_full == 0 {
            return;
        }
        let steps = usize::try_from(steps_full).unwrap_or(10).min(10);
        for _ in 0..steps {
            self.head = (self.head + 1) % self.samples.len();
            self.samples[self.head] = WindowSample::default();
        }
        // Advance `head_started` by the full elapsed seconds, leaving
        // the sub-second remainder so the next advance accumulates
        // correctly.
        self.head_started += Duration::from_secs(steps_full);
    }

    /// Add inbound bytes to the current head bucket. Rolls first.
    pub(crate) fn add_in(&mut self, now: Instant, bytes: u64) {
        self.advance(now);
        self.samples[self.head].bytes_in = self.samples[self.head].bytes_in.saturating_add(bytes);
    }

    /// Add outbound bytes to the current head bucket. Rolls first.
    pub(crate) fn add_out(&mut self, now: Instant, bytes: u64) {
        self.advance(now);
        self.samples[self.head].bytes_out = self.samples[self.head].bytes_out.saturating_add(bytes);
    }

    /// Bytes-per-second across the trailing 10-second window. Sums
    /// in + out across every bucket and divides by the elapsed
    /// window length, capped at ten seconds. During the warm-up
    /// period (less than ten seconds since construction) the divisor
    /// is the actual elapsed wall-clock time, so a fresh meter does
    /// not under-report a small burst.
    fn bps(&mut self, now: Instant) -> u64 {
        self.advance(now);
        let total: u64 = self
            .samples
            .iter()
            .map(|s| s.bytes_in.saturating_add(s.bytes_out))
            .sum();
        // Divisor in whole seconds. Saturating to >=1 sec keeps the
        // first sub-second sample from producing a huge spike.
        let elapsed = now.duration_since(self.created_at).as_secs().max(1);
        let divisor = elapsed.min(10);
        total / divisor
    }
}

/// Per-account counter pair. Updated by the metered body-readers in
/// `Net::download_stream`, `RequestBuilder::send`, and the IMAP/SMTP
/// `MeterSink` paths.
pub(crate) struct AccountCounters {
    pub(crate) bytes_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
    pub(crate) window: Mutex<RateWindow>,
}

impl AccountCounters {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            window: Mutex::new(RateWindow::new(now)),
        }
    }

    fn record_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
        let now = Instant::now();
        if let Ok(mut w) = self.window.lock() {
            w.add_in(now, n);
        }
    }

    fn record_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
        let now = Instant::now();
        if let Ok(mut w) = self.window.lock() {
            w.add_out(now, n);
        }
    }

    fn bps(&self) -> u64 {
        let now = Instant::now();
        match self.window.lock() {
            Ok(mut w) => w.bps(now),
            Err(_) => 0,
        }
    }
}

/// Top-level meter shared by every `AccountNet` and by IMAP/SMTP
/// `MeterSink` implementations. Holds one `AccountCounters` per
/// registered account.
pub struct BandwidthMeter {
    /// Per-account state. The map grows on `register_account` and
    /// shrinks on `forget_account`; lookups on the hot path go
    /// through `Arc` clones so the map lock is not held while the
    /// counter is updated.
    accounts: Mutex<HashMap<AccountId, MeterRegistration>>,
}

struct MeterRegistration {
    counters: Arc<AccountCounters>,
    attach_count: u64,
}

impl BandwidthMeter {
    /// Construct an empty meter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            accounts: Mutex::new(HashMap::new()),
        }
    }

    /// Register one account attachment with the meter.
    pub fn register_account(&self, account: AccountId) {
        let mut map = self.accounts.lock().expect("meter lock poisoned");
        map.entry(account)
            .and_modify(|registration| {
                registration.attach_count = registration.attach_count.saturating_add(1);
            })
            .or_insert_with(|| MeterRegistration {
                counters: Arc::new(AccountCounters::new(Instant::now())),
                attach_count: 1,
            });
    }

    /// Drop one account attachment. Counters remain registered until
    /// the final matching attachment is forgotten.
    pub fn forget_account(&self, account: &AccountId) {
        let mut map = self.accounts.lock().expect("meter lock poisoned");
        let remove = match map.get_mut(account) {
            Some(registration) => {
                registration.attach_count = registration.attach_count.saturating_sub(1);
                registration.attach_count == 0
            }
            None => false,
        };
        if remove {
            map.remove(account);
        }
    }

    pub(crate) fn retag_account(&self, old: &AccountId, new: AccountId) {
        if old == &new {
            return;
        }
        let mut map = self.accounts.lock().expect("meter lock poisoned");
        let remove_old = match map.get_mut(old) {
            Some(registration) => {
                registration.attach_count = registration.attach_count.saturating_sub(1);
                registration.attach_count == 0
            }
            None => return,
        };
        if remove_old {
            map.remove(old);
        }
        map.entry(new)
            .and_modify(|registration| {
                registration.attach_count = registration.attach_count.saturating_add(1);
            })
            .or_insert_with(|| MeterRegistration {
                counters: Arc::new(AccountCounters::new(Instant::now())),
                attach_count: 1,
            });
    }

    /// Build a handle scoped to one account. Cloneable, cheap.
    #[must_use]
    pub fn account(&self, account: AccountId) -> AccountMeter {
        let counters = self.lookup(&account);
        AccountMeter { account, counters }
    }

    pub(crate) fn inert_account(account: AccountId) -> AccountMeter {
        AccountMeter {
            account,
            counters: None,
        }
    }

    /// Sum bytes/second across every registered account. Used for
    /// process-wide diagnostics; per-account readings go through
    /// `AccountMeter::observed_bps`.
    #[must_use]
    pub fn observed_bps(&self) -> u64 {
        let map = self.accounts.lock().expect("meter lock poisoned");
        let mut total: u64 = 0;
        for registration in map.values() {
            total = total.saturating_add(registration.counters.bps());
        }
        total
    }

    fn lookup(&self, account: &AccountId) -> Option<Arc<AccountCounters>> {
        let map = self.accounts.lock().expect("meter lock poisoned");
        map.get(account)
            .map(|registration| Arc::clone(&registration.counters))
    }
}

impl Default for BandwidthMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl MeterSink for BandwidthMeter {
    fn record_bytes_in(&self, account: &AccountId, n: u64) {
        if let Some(counters) = self.lookup(account) {
            counters.record_in(n);
        }
    }
    fn record_bytes_out(&self, account: &AccountId, n: u64) {
        if let Some(counters) = self.lookup(account) {
            counters.record_out(n);
        }
    }
}

/// Account-scoped view into the meter. Returned from `Net::meter` /
/// `AccountNet::meter` and from `BandwidthMeter::account`.
#[derive(Clone)]
pub struct AccountMeter {
    /// Account this handle is scoped to. Retained for diagnostics and
    /// for `MeterSink` callers that report into a meter shared with
    /// other transports.
    account: AccountId,
    /// Per-account counters. `None` if the account is not registered
    /// with the meter; readings then return 0.
    counters: Option<Arc<AccountCounters>>,
}

impl AccountMeter {
    /// Adapt this account's counters for a non-HTTP byte stream.
    #[must_use]
    pub fn sink_handle(&self) -> MeterSinkHandle {
        struct Sink(AccountMeter);
        impl MeterSink for Sink {
            fn record_bytes_in(&self, _account: &AccountId, n: u64) {
                self.0.record_bytes_in(n);
            }
            fn record_bytes_out(&self, _account: &AccountId, n: u64) {
                self.0.record_bytes_out(n);
            }
        }
        MeterSinkHandle::new(Arc::new(Sink(self.clone())), self.account.clone())
    }

    /// Sliding-window bytes-per-second reading. Returns 0 if no
    /// samples have landed in the window yet.
    #[must_use]
    pub fn observed_bps(&self) -> u64 {
        self.counters.as_deref().map_or(0, AccountCounters::bps)
    }

    /// Cumulative bytes received for this account across the meter's
    /// lifetime.
    #[must_use]
    pub fn bytes_in(&self) -> u64 {
        self.counters
            .as_ref()
            .map_or(0, |c| c.bytes_in.load(Ordering::Relaxed))
    }

    /// Cumulative bytes sent for this account across the meter's
    /// lifetime.
    #[must_use]
    pub fn bytes_out(&self) -> u64 {
        self.counters
            .as_ref()
            .map_or(0, |c| c.bytes_out.load(Ordering::Relaxed))
    }

    /// The account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.account
    }

    /// Record `n` inbound bytes against this account. Crate-internal:
    /// the metered body-readers in `request.rs` and `net.rs` call
    /// this on every chunk.
    pub(crate) fn record_bytes_in(&self, n: u64) {
        if let Some(c) = self.counters.as_ref() {
            c.record_in(n);
        }
    }

    /// Record `n` outbound bytes against this account.
    pub(crate) fn record_bytes_out(&self, n: u64) {
        if let Some(c) = self.counters.as_ref() {
            c.record_out(n);
        }
    }
}

/// Hook used by non-HTTP transports (IMAP, SMTP) to feed
/// bytes-in / bytes-out into the bandwidth meter. The meter lives in
/// `bifrost-net` because the engine already depends on net for the
/// HTTP path; IMAP/SMTP poke a handle rather than carrying a separate
/// metering stack.
///
/// The trait is dyn-safe so a protocol crate can hold
/// `Arc<dyn MeterSink>` without naming the concrete `BandwidthMeter`
/// type, and so test doubles can substitute one in. The HTTP path
/// goes through `AccountMeter` directly because it already owns an
/// `Arc<AccountCounters>` from `attach_account`; raw-socket
/// transports go through a `MeterSinkHandle` which composes the
/// account id with the sink so the transport call site does not
/// need to thread the id alongside every byte count.
pub trait MeterSink: Send + Sync + 'static {
    /// Record `n` inbound bytes for `account`.
    fn record_bytes_in(&self, account: &AccountId, n: u64);
    /// Record `n` outbound bytes for `account`.
    fn record_bytes_out(&self, account: &AccountId, n: u64);
}

/// Account-scoped adapter around a `MeterSink`.
///
/// Raw-socket transports (IMAP, SMTP) construct one of these per
/// connection and call `record_bytes_in` / `record_bytes_out` on
/// every wire read / write. The handle owns the account id so the
/// transport call site does not have to thread it. Cloneable; one
/// per spawned task is the expected usage.
///
#[derive(Clone)]
pub struct MeterSinkHandle {
    sink: Arc<dyn MeterSink>,
    account: AccountId,
}

impl MeterSinkHandle {
    /// Construct an account-scoped handle around a sink.
    #[must_use]
    pub fn new(sink: Arc<dyn MeterSink>, account: AccountId) -> Self {
        Self { sink, account }
    }

    /// Construct a handle backed by the process-wide
    /// `BandwidthMeter` on `Net`. Convenience for raw-socket
    /// transports that already have a `Net` reference.
    #[must_use]
    pub fn from_meter(meter: Arc<BandwidthMeter>, account: AccountId) -> Self {
        Self {
            sink: meter as Arc<dyn MeterSink>,
            account,
        }
    }

    /// Account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.account
    }

    /// Record `n` inbound bytes against the scoped account.
    pub fn record_bytes_in(&self, n: u64) {
        self.sink.record_bytes_in(&self.account, n);
    }

    /// Record `n` outbound bytes against the scoped account.
    pub fn record_bytes_out(&self, n: u64) {
        self.sink.record_bytes_out(&self.account, n);
    }
}

impl std::fmt::Debug for MeterSinkHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeterSinkHandle")
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sliding window is anchored on `Instant`, which no test clock
    /// controls. Every assertion below drives `RateWindow` with
    /// synthetic instants instead, so the roll / zero / divisor maths is
    /// pinned deterministically rather than raced against wall time
    /// (which is what forced `tests/bandwidth_window.rs` to settle for
    /// qualitative assertions).
    fn base() -> Instant {
        Instant::now()
    }

    #[test]
    fn samples_inside_one_second_land_in_the_same_bucket() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        window.add_in(t0, 100);
        window.add_out(t0 + Duration::from_millis(400), 50);
        window.add_in(t0 + Duration::from_millis(999), 25);

        assert_eq!(window.head, 0, "sub-second samples must not roll the head");
        assert_eq!(window.samples[0].bytes_in, 125);
        assert_eq!(window.samples[0].bytes_out, 50);
    }

    #[test]
    fn crossing_a_second_boundary_opens_a_fresh_bucket() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        window.add_in(t0, 100);
        window.add_in(t0 + Duration::from_secs(1), 40);

        assert_eq!(window.head, 1);
        assert_eq!(window.samples[0].bytes_in, 100, "the prior second is kept");
        assert_eq!(window.samples[1].bytes_in, 40);
    }

    /// `head_started` advances by whole seconds only, so the sub-second
    /// remainder carries forward. Without that, a stream of samples at
    /// 1.5 s intervals would roll on every single call.
    #[test]
    fn sub_second_remainder_carries_forward_across_rolls() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        window.add_in(t0 + Duration::from_millis(1500), 10);
        assert_eq!(window.head, 1);
        window.add_in(t0 + Duration::from_millis(1900), 10);
        assert_eq!(
            window.head, 1,
            "1.9 s is still inside the bucket opened at 1.0 s"
        );
        assert_eq!(window.samples[1].bytes_in, 20);
        window.add_in(t0 + Duration::from_millis(2100), 10);
        assert_eq!(window.head, 2);
    }

    #[test]
    fn a_ten_second_gap_empties_the_entire_window() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        window.add_in(t0, 1_000);
        assert_eq!(window.bps(t0), 1_000);
        assert_eq!(
            window.bps(t0 + Duration::from_secs(10)),
            0,
            "ten idle seconds roll every bucket out of the window"
        );
    }

    /// The roll loop is bounded at ten steps, so an hour-long idle gap
    /// costs ten iterations and leaves an empty window rather than
    /// walking 3600 buckets.
    #[test]
    fn a_very_long_gap_is_bounded_and_still_empties_the_window() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        window.add_in(t0, 5_000);
        assert_eq!(window.bps(t0 + Duration::from_secs(3600)), 0);
        window.add_in(t0 + Duration::from_secs(3601), 700);
        assert_eq!(
            window.bps(t0 + Duration::from_secs(3601)),
            70,
            "after an hour idle the divisor is the full ten-second window"
        );
    }

    /// During warm-up the divisor is the elapsed window rather than a
    /// flat ten seconds, so a fresh meter does not under-report by 10x.
    /// The divisor saturates at one second so a sub-second first sample
    /// does not become a division-by-zero spike.
    #[test]
    fn warmup_divisor_is_elapsed_time_saturated_to_one_second() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        window.add_in(t0, 300);
        assert_eq!(
            window.bps(t0 + Duration::from_millis(500)),
            300,
            "under a second the divisor is 1, not 0"
        );

        let mut window = RateWindow::new(t0);
        for second in 0..4u64 {
            window.add_in(t0 + Duration::from_secs(second), 100);
        }
        assert_eq!(
            window.bps(t0 + Duration::from_secs(3)),
            133,
            "400 bytes over a 3 s warm-up window, integer-divided"
        );
    }

    #[test]
    fn divisor_is_capped_at_ten_seconds_once_warm() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        for second in 0..10u64 {
            window.add_in(t0 + Duration::from_secs(second), 90);
        }
        assert_eq!(
            window.bps(t0 + Duration::from_secs(9)),
            100,
            "900 bytes across a nine-second elapsed window"
        );
        let mut window = RateWindow::new(t0);
        for second in 0..30u64 {
            window.add_in(t0 + Duration::from_secs(second), 100);
        }
        assert_eq!(
            window.bps(t0 + Duration::from_secs(29)),
            100,
            "a long-running stream still divides by ten, not by thirty"
        );
    }

    #[test]
    fn per_bucket_byte_counts_saturate_instead_of_overflowing() {
        let t0 = base();
        let mut window = RateWindow::new(t0);
        window.add_in(t0, u64::MAX);
        window.add_in(t0, 1);
        window.add_out(t0, u64::MAX);
        assert_eq!(window.samples[0].bytes_in, u64::MAX);
        // `bps` sums in + out with `saturating_add` too, so the total
        // is clamped rather than wrapping to a small number.
        assert_eq!(window.bps(t0), u64::MAX);
    }

    // ---- meter registration ------------------------------------------

    /// `AccountMeter` for an unregistered account is inert: every
    /// reading is zero and every record is dropped. This is the
    #[test]
    fn unregistered_accounts_are_inert_on_both_meter_entry_points() {
        let meter = BandwidthMeter::new();
        let account = AccountId("never-registered".to_owned());

        let handle = meter.account(account.clone());
        handle.record_bytes_in(4_096);
        assert_eq!(
            handle.bytes_in(),
            0,
            "a handle minted before registration silently drops bytes"
        );

        MeterSink::record_bytes_in(&meter, &account, 4_096);
        assert_eq!(
            meter.account(account).bytes_in(),
            0,
            "MeterSink must not resurrect a detached or unknown account"
        );
    }

    #[test]
    fn account_registration_is_counted_and_final_forget_clears_counters() {
        let meter = BandwidthMeter::new();
        let account = AccountId("acct".to_owned());
        meter.register_account(account.clone());
        MeterSink::record_bytes_in(&meter, &account, 10);
        meter.register_account(account.clone());
        assert_eq!(
            meter.account(account.clone()).bytes_in(),
            10,
            "a second attachment must not reset live counters"
        );

        meter.forget_account(&account);
        assert_eq!(
            meter.account(account.clone()).bytes_in(),
            10,
            "one attachment remains registered"
        );
        meter.forget_account(&account);
        assert_eq!(meter.account(account).bytes_in(), 0);
    }

    #[test]
    fn cached_handle_freezes_after_the_final_detach() {
        let meter = BandwidthMeter::new();
        let account = AccountId("acct".to_owned());
        meter.register_account(account.clone());
        let cached = meter.account(account.clone());
        cached.record_bytes_in(10);

        meter.forget_account(&account);

        assert_eq!(
            cached.bytes_in(),
            10,
            "an already-cached handle retains its detached counter snapshot"
        );
        assert_eq!(
            meter.account(account).bytes_in(),
            0,
            "new lookups do not discover detached counters"
        );
    }

    #[test]
    fn meter_sink_handle_routes_to_the_account_it_owns() {
        let meter = Arc::new(BandwidthMeter::new());
        let account = AccountId("imap-acct".to_owned());
        meter.register_account(account.clone());

        let handle = MeterSinkHandle::from_meter(Arc::clone(&meter), account.clone());
        assert_eq!(handle.account(), &account);
        handle.record_bytes_out(512);
        handle.record_bytes_in(1_024);

        let view = meter.account(account);
        assert_eq!(view.bytes_out(), 512);
        assert_eq!(view.bytes_in(), 1_024);
    }

    /// The account id must not leak through `Debug` alongside secrets;
    /// pin that the handle prints its account and nothing else.
    #[test]
    fn meter_sink_handle_debug_is_non_exhaustive() {
        let meter = Arc::new(BandwidthMeter::new());
        let handle = MeterSinkHandle::from_meter(meter, AccountId("a".to_owned()));
        let rendered = format!("{handle:?}");
        assert!(rendered.starts_with("MeterSinkHandle"));
        assert!(rendered.contains(".."));
    }
}
