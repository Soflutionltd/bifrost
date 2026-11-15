//! Per-host token-bucket rate limiter.
//!
//! Each protocol declares a `RateLimit` per host at client
//! construction. The governor enforces it on every request. `Notify`
//! is used rather than fixed sleeps so a 429 with a long
//! `Retry-After` can refund the unused cost and wake other waiters
//! immediately. Admission is FIFO: each `acquire` takes a ticket in
//! its host's queue and only the head may debit, so a waiter cannot
//! starve behind a stream of later arrivals.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use crate::error::Error;

/// A per-host quota declaration. Gmail's 250 units/sec, Graph's
/// per-tenant 10 units/sec, etc. Protocol crates hand a list of
/// these to `Net::attach_account`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RateLimit {
    /// Host the limit applies to. Matched against `Url::host_str` for
    /// outbound requests.
    pub host: String,
    /// Caller-defined quota scope. `HostOnly` preserves host-wide sharing.
    pub quota_scope: crate::QuotaScope,
    /// Refill rate in units per second.
    pub quota_per_second: f64,
    /// Default per-request cost. Callers may override via
    /// `RequestBuilder::cost`.
    pub cost_default: u32,
    /// Burst capacity. Acts as the maximum number of tokens the
    /// bucket can hold.
    pub burst: u32,
}

/// Trait that lets request bodies declare a non-default cost. The
/// `RequestBuilder::cost` fluent setter is the v1 path; this trait is
/// reserved for typed-body integrations layered on later.
pub trait RequestCost {
    /// Cost in quota units. The default of 1 matches Gmail's cheapest
    /// endpoints and Graph's per-request floor.
    fn cost(&self) -> u32 {
        1
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Identity of the bucket instance that admitted a debit.
pub struct RateGeneration(u64);

impl RateLimit {
    /// Construct a rate declaration for one host.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        quota_per_second: f64,
        cost_default: u32,
        burst: u32,
    ) -> Self {
        Self {
            host: host.into(),
            quota_scope: crate::QuotaScope::HostOnly,
            quota_per_second,
            cost_default,
            burst,
        }
    }

    /// Key this declaration by a provider quota discriminator in addition
    /// to its host. Accounts with different non-empty scopes never share a
    /// bucket; an empty scope retains the host-only compatibility behavior.
    #[must_use]
    pub fn with_quota_scope(mut self, scope: impl Into<crate::QuotaScope>) -> Self {
        self.quota_scope = scope.into();
        self
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RateKey {
    pub(crate) host: String,
    pub(crate) quota_scope: crate::QuotaScope,
}

impl RateKey {
    pub(crate) fn new(host: impl Into<String>, quota_scope: impl Into<crate::QuotaScope>) -> Self {
        let scope = quota_scope.into();
        // Normalize even explicitly constructed Named("") values. Registration
        // and string-based governor lookups must reach the same bucket.
        let quota_scope = if scope.as_str().is_empty() {
            crate::QuotaScope::HostOnly
        } else {
            scope
        };
        Self {
            host: host.into(),
            quota_scope,
        }
    }
}

/// Per-host token-bucket governor. Holds one `HostBucket` per
/// registered host and looks them up by `&str` on every call.
#[derive(Clone)]
pub struct RateLimitGovernor {
    /// Per-host bucket state. Behind `Arc<Mutex<_>>` rather than
    /// plain `Mutex<_>` because `acquire(...)` returns a
    /// `Pin<Box<dyn Future + Send + 'static>>` that must capture
    /// the bucket map by ownership (it cannot borrow from `&self`,
    /// which would require a non-`'static` lifetime). The wait/notify
    /// loop inside the future re-acquires the lock after each park;
    /// the `Arc` clone is what makes that legal. Steady-state path
    /// takes the lock only to debit tokens, which is not held across
    /// `.await`. The `Notify` lives outside the `Mutex` so wake
    /// calls do not contend.
    buckets: Arc<Mutex<HashMap<RateKey, HostBucket>>>,
}

/// Internal bucket state for one host. Public only inside the crate
/// so the governor and the bandwidth meter can share refill math
/// later.
pub(crate) struct HostBucket {
    /// Current token count. Floating-point so partial refills are
    /// representable.
    pub(crate) tokens: f64,
    /// Burst capacity copied from `RateLimit::burst` at registration.
    pub(crate) burst: f64,
    /// Burst capacity as the original `u32`. Kept alongside `burst`
    /// so the cost-exceeds-burst error path can surface the exact
    /// integer the caller registered, without an `f64 -> u32` cast.
    pub(crate) burst_max: u32,
    /// Refill rate copied from `RateLimit::quota_per_second`.
    pub(crate) refill_rate: f64,
    /// Per-host default cost copied from `RateLimit::cost_default` at
    /// registration. `RequestBuilder::send_streaming_inner` looks this
    /// up when the builder did not call `.cost()` so each host can
    /// have a sensible default (Gmail's cheap reads are 5; Graph's
    /// query endpoints are 10) without forcing every call site to
    /// remember.
    pub(crate) cost_default: u32,
    /// Wall-clock instant of the last refill calculation.
    pub(crate) last_refill: Instant,
    /// FIFO admission queue. Only its front entry may debit tokens.
    waiters: VecDeque<RateWaiter>,
    next_waiter_id: u64,
    /// Identity of this bucket *instance*, distinct from the host name.
    /// A host can be unregistered and registered again while a woken
    /// waiter is still on its way back to the lock; without this the
    /// waiter would find a bucket under its host, fail to recognise it
    /// as a different queue, and park on its own `Notify` forever. It
    /// also stops a cancelled waiter's `Drop` from evicting a
    /// same-numbered ticket belonging to the replacement bucket, since
    /// `next_waiter_id` restarts at zero for every new instance.
    generation: u64,
    /// Per-host attach count. Each `register` increments; each
    /// `unregister` decrements; the bucket is dropped when the count
    /// reaches zero. Lets `Net::detach_account` shed unused host
    /// buckets symmetrically without yanking the bucket out from
    /// under accounts that still depend on it.
    pub(crate) attach_count: u32,
}

impl RateLimitGovernor {
    /// Construct an empty governor. Hosts are registered via
    /// `register`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a host with the governor.
    ///
    /// Duplicate-host policy: the first registration wins. A second
    /// registration that disagrees on `quota_per_second`, `burst`, or
    /// `cost_default` is **not** silently dropped: we emit a
    /// `tracing::warn!` so the caller sees the conflict, but leave the
    /// installed bucket in place. The alternative (most-restrictive
    /// merge) would let any consumer poison shared host quotas via a
    /// misconfiguration; sticking with the first registration keeps
    /// the rule deterministic.
    ///
    /// Returns true when this registration joined a bucket and must
    /// later be balanced by `unregister`. Returns false when a new
    /// host declaration was rejected as invalid.
    pub fn register(&self, limit: RateLimit) -> bool {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        let key = RateKey::new(limit.host.clone(), limit.quota_scope.clone());
        match map.get_mut(&key) {
            Some(existing) => {
                #[allow(clippy::float_cmp)]
                let conflict = existing.refill_rate != limit.quota_per_second
                    || existing.burst_max != limit.burst
                    || existing.cost_default != limit.cost_default;
                if conflict {
                    tracing::warn!(
                        target: "bifrost_net::rate",
                        host = %limit.host,
                        existing_quota_per_second = existing.refill_rate,
                        existing_burst = existing.burst_max,
                        existing_cost_default = existing.cost_default,
                        new_quota_per_second = limit.quota_per_second,
                        new_burst = limit.burst,
                        new_cost_default = limit.cost_default,
                        "duplicate RateLimit registration with different quota; keeping first registration",
                    );
                }
                existing.attach_count = existing.attach_count.saturating_add(1);
                true
            }
            None => {
                if !limit.quota_per_second.is_finite()
                    || limit.quota_per_second <= 0.0
                    || limit.burst == 0
                    || limit.burst < limit.cost_default
                {
                    tracing::warn!(
                        target: "bifrost_net::rate",
                        host = %limit.host,
                        quota_per_second = limit.quota_per_second,
                        burst = limit.burst,
                        cost_default = limit.cost_default,
                        "ignoring invalid RateLimit registration",
                    );
                    return false;
                }
                map.insert(
                    key,
                    HostBucket {
                        tokens: f64::from(limit.burst),
                        burst: f64::from(limit.burst),
                        burst_max: limit.burst,
                        refill_rate: limit.quota_per_second,
                        cost_default: limit.cost_default,
                        last_refill: Instant::now(),
                        waiters: VecDeque::new(),
                        next_waiter_id: 0,
                        generation: NEXT_BUCKET_GENERATION.fetch_add(1, Ordering::Relaxed),
                        attach_count: 1,
                    },
                );
                true
            }
        }
    }

    /// Decrement the attach count for a host; drop the bucket when
    /// the count reaches zero. Called from `Net::detach_account` to
    /// keep the governor's map from growing without bound across
    /// account attach/detach cycles.
    pub fn unregister(&self, host: &str) {
        self.unregister_scoped(host, "");
    }

    /// Decrement the attach count for one `(host, quota_scope)` bucket.
    pub fn unregister_scoped(&self, host: &str, quota_scope: &str) {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        let key = RateKey::new(host, quota_scope);
        let drop_it = match map.get_mut(&key) {
            Some(bucket) => {
                bucket.attach_count = bucket.attach_count.saturating_sub(1);
                bucket.attach_count == 0
            }
            None => false,
        };
        // Waking the queue is what makes detach safe: a parked waiter
        // whose bucket just vanished must complete as unmetered rather
        // than sit on a `Notify` nothing will ever fire again.
        if drop_it && let Some(bucket) = map.remove(&key) {
            for waiter in bucket.waiters {
                waiter.notify.notify_one();
            }
        }
    }

    /// Look up the host's registered `cost_default`. Returns `None`
    /// if the host has no registration. Used by
    /// `RequestBuilder::send_streaming_inner` when the builder did
    /// not set a per-request cost via `.cost(n)`.
    #[must_use]
    pub fn cost_default_for(&self, host: &str) -> Option<u32> {
        self.cost_default_for_scoped(host, "")
    }

    /// Look up the default cost for one `(host, quota_scope)` bucket.
    #[must_use]
    pub fn cost_default_for_scoped(&self, host: &str, quota_scope: &str) -> Option<u32> {
        let map = self.buckets.lock().expect("rate-governor lock poisoned");
        map.get(&RateKey::new(host, quota_scope))
            .map(|b| b.cost_default)
    }

    /// Await enough tokens to debit `cost` from the host's bucket.
    /// Returns once the debit succeeds.
    ///
    /// The returned future is `Send + 'static` so it composes with
    /// the protocol crates' own erased futures. If the host is not
    /// registered the future returns immediately - hosts with no
    /// declared quota are unmetered.
    ///
    /// # Errors
    /// Returns `Error::CostExceedsBurst` if `cost` is larger than the
    /// host's burst capacity. The bucket can never accumulate that
    /// many tokens (the refill clamp caps at `burst`), so the future
    /// would otherwise spin forever. This is a caller configuration
    /// bug rather than a transient condition; we surface it instead
    /// of hanging.
    pub fn acquire(
        &self,
        host: &str,
        cost: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'static>> {
        let future = self.acquire_generation(host, cost);
        Box::pin(async move { future.await.map(|_| ()) })
    }

    /// Acquire a debit and return the admitting bucket generation.
    /// Unregistered hosts return `None` because no debit occurred.
    pub fn acquire_generation(
        &self,
        host: &str,
        cost: u32,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<RateGeneration>, Error>>
                + Send
                + 'static,
        >,
    > {
        self.acquire_generation_scoped(host, "", cost)
    }

    /// Acquire from one `(host, quota_scope)` bucket and return its generation.
    pub fn acquire_generation_scoped(
        &self,
        host: &str,
        quota_scope: &str,
        cost: u32,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<RateGeneration>, Error>>
                + Send
                + 'static,
        >,
    > {
        let buckets = Arc::clone(&self.buckets);
        let key = RateKey::new(host, quota_scope);
        let cost_f = f64::from(cost);
        Box::pin(async move {
            let (ticket, waiter_notify) = {
                let mut map = buckets.lock().expect("rate-governor lock poisoned");
                let Some(bucket) = map.get_mut(&key) else {
                    return Ok(None);
                };
                // Validate and enqueue under the same lock. Splitting
                // these operations lets an unregister/register cycle
                // replace the bucket with a smaller burst between the
                // check and the enqueue, leaving an impossible head
                // cost parked forever in the replacement generation.
                if cost_f > bucket.burst {
                    return Err(Error::CostExceedsBurst {
                        cost,
                        burst: bucket.burst_max,
                    });
                }
                let ticket = Ticket {
                    generation: bucket.generation,
                    id: bucket.next_waiter_id,
                };
                bucket.next_waiter_id = bucket.next_waiter_id.wrapping_add(1);
                let notify = Arc::new(Notify::new());
                bucket.waiters.push_back(RateWaiter {
                    id: ticket.id,
                    notify: Arc::clone(&notify),
                });
                (ticket, notify)
            };
            let mut guard = WaiterGuard {
                buckets: Arc::clone(&buckets),
                key: key.clone(),
                ticket,
                armed: true,
            };
            loop {
                let is_front = {
                    let map = buckets.lock().expect("rate-governor lock poisoned");
                    // Three ways this ticket can have stopped being
                    // metered, all of which must complete rather than
                    // park: the host was unregistered outright, the
                    // host was unregistered and registered again by
                    // another account (a different bucket instance,
                    // which never held this ticket), or the ticket was
                    // otherwise drained from the queue we joined.
                    // Completing unmetered matches the documented
                    // detach behaviour; parking on a `Notify` nothing
                    // holds any more would strand the request forever.
                    let Some(bucket) = map.get(&key).filter(|bucket| {
                        bucket.generation == ticket.generation
                            && bucket.waiters.iter().any(|waiter| waiter.id == ticket.id)
                    }) else {
                        guard.armed = false;
                        return Ok(None);
                    };
                    bucket.waiters.front().map(|waiter| waiter.id) == Some(ticket.id)
                };
                if !is_front {
                    waiter_notify.notified().await;
                    continue;
                }
                // Snapshot the wait duration inside the lock; the
                // actual await happens outside.
                let wait_for = {
                    let mut map = buckets.lock().expect("rate-governor lock poisoned");
                    let Some(bucket) = map
                        .get_mut(&key)
                        .filter(|bucket| bucket.generation == ticket.generation)
                    else {
                        // Host has no declared quota, or the bucket we
                        // queued on was replaced; either way this
                        // request is now unmetered.
                        guard.armed = false;
                        return Ok(None);
                    };
                    let now = Instant::now();
                    let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
                    bucket.tokens =
                        (bucket.tokens + elapsed * bucket.refill_rate).min(bucket.burst);
                    bucket.last_refill = now;
                    if bucket.tokens >= cost_f {
                        bucket.tokens -= cost_f;
                        bucket.waiters.pop_front();
                        if let Some(next) = bucket.waiters.front() {
                            next.notify.notify_one();
                        }
                        guard.armed = false;
                        return Ok(Some(RateGeneration(ticket.generation)));
                    }
                    // Not enough tokens. Compute the minimum wait until
                    // the bucket can satisfy the request, capped at
                    // 250 ms so the loop also picks up refunds via
                    // `Notify` wakes promptly even if our refill math
                    // is off. Registration rejects non-finite and
                    // non-positive refill rates, so this calculation
                    // is always finite and positive.
                    let deficit = cost_f - bucket.tokens;
                    let wait_secs = deficit / bucket.refill_rate;
                    let wait_capped = wait_secs.clamp(0.005, 0.25);
                    Duration::from_secs_f64(wait_capped)
                };
                // Either a refund wakes the FIFO head or its refill
                // timer elapses.
                tokio::select! {
                    _ = waiter_notify.notified() => {}
                    _ = tokio::time::sleep(wait_for) => {}
                }
            }
        })
    }

    /// Refund `cost` tokens to the host's bucket, e.g. when the
    /// server returned 429 with a `Retry-After` and the request did
    /// not actually consume the slot.
    ///
    /// **No thundering herd:** the wake goes to the head of the
    /// admission queue and nobody else. Refunded tokens are offered to
    /// exactly the waiter entitled to them, so a burst of refunds (a
    /// chain of 503s from a host that just came back up) cannot wake N
    /// waiters to race for one slot. A refund that arrives with the
    /// queue empty simply raises the token count for the next arrival.
    /// Refund a debit only if the same bucket generation is still installed.
    pub fn refund(&self, host: &str, cost: u32, generation: RateGeneration) {
        self.refund_scoped(host, "", cost, generation);
    }

    /// Refund only the matching scoped bucket generation.
    pub fn refund_scoped(
        &self,
        host: &str,
        quota_scope: &str,
        cost: u32,
        generation: RateGeneration,
    ) {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        let Some(bucket) = map
            .get_mut(&RateKey::new(host, quota_scope))
            .filter(|bucket| bucket.generation == generation.0)
        else {
            return;
        };
        bucket.tokens = (bucket.tokens + f64::from(cost)).min(bucket.burst);
        if let Some(waiter) = bucket.waiters.front() {
            waiter.notify.notify_one();
        }
    }
}

/// Monotonic source of `HostBucket::generation`. Process-wide because
/// bucket instances are compared only against tickets minted from the
/// same instance; uniqueness across governors costs nothing and removes
/// any question about governors that share a host name.
static NEXT_BUCKET_GENERATION: AtomicU64 = AtomicU64::new(0);

struct RateWaiter {
    id: u64,
    notify: Arc<Notify>,
}

/// A waiter's place in one specific bucket instance. The `id` alone is
/// ambiguous across an unregister/register cycle, since `next_waiter_id`
/// restarts at zero with the new bucket.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Ticket {
    generation: u64,
    id: u64,
}

struct WaiterGuard {
    buckets: Arc<Mutex<HashMap<RateKey, HostBucket>>>,
    key: RateKey,
    ticket: Ticket,
    armed: bool,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        // Only ever mutate the bucket instance this ticket was minted
        // against. A cancelled waiter whose host was re-registered
        // meanwhile would otherwise evict an unrelated waiter that
        // happens to hold the same recycled id, and hand its wake to
        // the wrong task.
        let Some(bucket) = map
            .get_mut(&self.key)
            .filter(|bucket| bucket.generation == self.ticket.generation)
        else {
            return;
        };
        let was_front = bucket.waiters.front().map(|waiter| waiter.id) == Some(self.ticket.id);
        bucket.waiters.retain(|waiter| waiter.id != self.ticket.id);
        if was_front && let Some(next) = bucket.waiters.front() {
            next.notify.notify_one();
        }
    }
}

impl Default for RateLimitGovernor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn stale_generation_refund_cannot_credit_replacement_bucket() {
        let governor = RateLimitGovernor::new();
        governor.register(RateLimit::new("churn.test", 0.001, 1, 1));
        let old_generation = governor
            .acquire_generation("churn.test", 1)
            .await
            .expect("old debit succeeds")
            .expect("registered bucket has a generation");
        governor.unregister("churn.test");
        governor.register(RateLimit::new("churn.test", 0.001, 1, 1));
        governor
            .acquire("churn.test", 1)
            .await
            .expect("replacement bucket starts full");

        governor.refund("churn.test", 1, old_generation);
        let blocked = governor.acquire("churn.test", 1);
        assert!(
            tokio::time::timeout(Duration::ZERO, blocked).await.is_err(),
            "stale refund credited replacement generation"
        );
    }
}
