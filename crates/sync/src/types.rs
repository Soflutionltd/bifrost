//! Published engine configuration and account connection identity.

use std::sync::Arc;
use std::time::Duration;

use bifrost_types::Account;

use crate::scheduler::budget::ConcurrencyBudget;

/// Top-level engine configuration. Cloned into every `AccountSlot`.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub budget: ConcurrencyBudget,
    pub scheduler: SchedulerConfig,
    pub multiplexer: MultiplexerConfig,
    pub backfill: BackfillConfig,
    pub push: PushConfig,
    pub mutation: MutationConfig,
    /// Per-PHASE detach / drop timeout. This is not the cost of a
    /// detach: teardown runs four phases, each with its own fresh budget -
    /// awaiting the stream workers, draining the ack writer, waiting for an
    /// in-flight reattach to release the reopen lock, then `Account::close()` -
    /// so a worst-case detach costs `4 * detach_timeout` (20s at the default),
    /// and only when all four phases wedge.
    ///
    /// `SyncEngine::shutdown()` detaches attached accounts SEQUENTIALLY, so
    /// worst-case engine shutdown is `4 * detach_timeout * n_accounts` plus
    /// overhead. Size this knob against that product, not against one phase.
    pub detach_timeout: Duration,
    /// Per-lane cap on the scheduler's work-item and admission queues. A zero
    /// value is floored to one. Work-item queues shed the oldest item and
    /// increment a queue-local counter on overflow; admission queues refuse
    /// the incoming request and do not count the refusal. The scheduler does
    /// not expose its work-item queue counters.
    pub lane_capacity: usize,
    /// Mutation campaign retry cap. The campaign attempts up to this
    /// many resubmissions for ids whose per-item `ItemOutcome::Failed`
    /// carried a retryable `RecoveryClass`.
    pub mutation_max_retries: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            budget: ConcurrencyBudget::default(),
            scheduler: SchedulerConfig::default(),
            multiplexer: MultiplexerConfig::default(),
            backfill: BackfillConfig::default(),
            push: PushConfig::default(),
            mutation: MutationConfig::default(),
            detach_timeout: Duration::from_secs(5),
            lane_capacity: 1024,
            mutation_max_retries: 5,
        }
    }
}

/// Scheduler tuning knobs.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    /// Number of consecutive higher-lane pulls before the starvation
    /// floor forces one lower-lane pull. Default 64. Zero is floored to one
    /// when the scheduler is constructed, preserving priority for the first
    /// pull.
    pub starvation_floor: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            starvation_floor: 64,
        }
    }
}

/// Multiplexer cadence and reopen policy.
#[derive(Debug, Clone, Copy)]
pub struct MultiplexerConfig {
    /// Initial NOOP/STATUS poll interval for non-IDLE scopes.
    pub poll_initial: Duration,
    /// Floor on the adaptive cadence (interval halves on change-seen
    /// down to this minimum).
    pub poll_min: Duration,
    /// Ceiling on the adaptive cadence (interval doubles on five
    /// consecutive no-change ticks up to this maximum).
    pub poll_max: Duration,
    /// Distinct hinted scopes and memberships retained before collapsing
    /// into one full reconcile.
    pub watch_capacity: usize,
    /// Bound on the per-account broadcast carrying the unified
    /// `account_changes_stream` payload.
    pub changes_capacity: usize,
}

impl Default for MultiplexerConfig {
    fn default() -> Self {
        Self {
            poll_initial: Duration::from_secs(60),
            poll_min: Duration::from_secs(30),
            poll_max: Duration::from_secs(30 * 60),
            watch_capacity: 256,
            changes_capacity: 256,
        }
    }
}

/// Backfill partitioner / runner config.
#[derive(Debug, Clone, Copy)]
pub struct BackfillConfig {
    /// IMAP-on-Basic fallback chunk size for `UidRange` strategy.
    pub uid_range_chunk: u32,
    /// JMAP `PageCount` strategy page size.
    pub page_count_chunk: u32,
    /// Clock-skew threshold above which a `Warning::ClockSkew` is
    /// emitted.
    pub clock_skew_warn: Duration,
    /// How many backfill pages may be published but UNREAD before the
    /// cold-start producer parks.
    ///
    /// The bound that makes backfill flow-controlled instead of
    /// loss-and-reconcile. Backfill and live changes share one broadcast ring
    /// (`MultiplexerConfig::changes_capacity`, default 256) with no
    /// producer-side backpressure of its own, so a large mailbox's cold start
    /// used to outrun a consumer as a matter of routine, overwrite unread
    /// pages, and trigger lag abandonment plus a re-read from the last durable
    /// checkpoint. A backfill producer takes one permit per published page and
    /// returns it when a consumer receiver READS that page off the stream.
    ///
    /// Reading, not acknowledging. What overruns a ring is pages nobody has
    /// taken out of it, while an acknowledgement is a promise about the
    /// consumer's own durability, made on whatever schedule the consumer likes -
    /// so this is a knob and not a contract on the consumer's acknowledgement
    /// window. A consumer may batch its acknowledgements as coarsely as it
    /// wishes; the producer follows its reads - or, with several receivers, the
    /// slowest of them, since the ring is shared and a page it has not read is
    /// still occupying a slot.
    ///
    /// What it does not bound is the consumer's unpersisted backlog, exactly as
    /// on any other channel, and coarse acknowledgement is not free even though
    /// the producer no longer waits for it. Until a page is acknowledged the
    /// engine keeps its publication record alive - including, for a page a later
    /// page of the same partition superseded, its receipt and the coverage claim
    /// on it, folded into the survivor - so an account's retained ledger grows
    /// with the unacknowledged run of each partition rather than with this bound.
    /// And a live-lane burst that lags such a consumer abandons every one of
    /// those read-but-unacknowledged pages at once, costing a re-walk of all of
    /// them; acknowledging while the producer is parked has neither cost.
    ///
    /// That retained history is unbounded PER PARTITION, and the `Full` plan
    /// (which is what JMAP Email uses) has one partition per scope, so there it
    /// is unbounded over the whole scope's backfill.
    ///
    /// **Every numbered receiver must be polled or dropped.** Every receiver
    /// `SyncEngine::account_changes_stream` hands out is numbered, and the bound
    /// follows the slowest of them, so a receiver that is held and never polled -
    /// a second stream opened for a UI, say - holds up to this many pages unread
    /// for the life of the attachment and parks cold start permanently. It
    /// reports nothing on its own: the departure warning fires on drop and the lag
    /// warning on poll, and such a receiver does neither, so the engine warns from
    /// the park instead (`bifrost.sync.backfill`, naming the holding sequences).
    /// A receiver that only watches should come from
    /// `SyncEngine::account_changes_observer` instead: an observer counts for
    /// neither the subscriber gate nor the bound and carries no such obligation.
    ///
    /// The bound is also narrower than "the producer follows the slowest reader":
    /// it follows the slowest live numbered reader among pages NOT YET
    /// ACKNOWLEDGED, and an acknowledgement frees a page for every receiver. With
    /// two numbered receivers where the ACKNOWLEDGER is the faster one, its
    /// acknowledgements can therefore run the producer past the slower numbered
    /// receiver until it lags. Two acknowledging receivers are not a supported
    /// shape for this engine in any case, and an observer is unnumbered, so the
    /// case does not arise for it.
    ///
    /// Keep it well under `changes_capacity`: the point of the bound is that a
    /// cold start cannot overrun the ring by itself, which needs room in the
    /// same ring for live traffic. A value of 0 is floored to 1, because a
    /// zero-permit lane is a permanently parked cold start.
    pub lane_capacity: usize,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            uid_range_chunk: 5000,
            page_count_chunk: 1000,
            clock_skew_warn: Duration::from_secs(5 * 60),
            lane_capacity: crate::engine::backfill_bound::DEFAULT_BACKFILL_CAPACITY,
        }
    }
}

/// Push reconciler config.
///
/// Capacity for per-account pending invalidation intent lives on
/// `MultiplexerConfig::watch_capacity`; push-only knobs live here.
#[derive(Debug, Clone, Copy)]
pub struct PushConfig {
    /// How often each attached account retries the teardown of push
    /// subscriptions whose `push_unsubscribe` the provider refused and that
    /// nobody wants any more. `None` disables the timer, leaving the retry
    /// to the next reopen or `unsubscribe_push`, which is how such an orphan
    /// was handled before the timer existed: one on an account that never
    /// reopens keeps delivering until the provider expires it.
    ///
    /// Each pass costs nothing when there are no orphans, and one
    /// `push_unsubscribe` per orphan otherwise; a provider that does not
    /// know a handle accepts the call, so an orphan the provider has already
    /// expired clears on the first pass. The pass runs beside reopens and
    /// consumer calls rather than holding the account's reopen lock, so a
    /// stalled provider delays only the retry itself. An interval below one
    /// second is raised to one. Default 15 minutes.
    pub orphan_teardown_retry_interval: Option<Duration>,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            orphan_teardown_retry_interval: Some(Duration::from_secs(15 * 60)),
        }
    }
}

/// Mutation pipeline config.
#[derive(Debug, Clone, Copy)]
pub struct MutationConfig {
    /// Recommended capacity for the per-account sub-channels a caller
    /// pre-builds for [`crate::mutation::fanout::partition_by_account`].
    ///
    /// Advisory by construction: `partition_by_account` takes senders the
    /// caller already made, so the engine cannot apply this on the caller's
    /// behalf. It is the default a consumer should reach for, not a bound the
    /// engine enforces.
    pub fanout_buffer: usize,
    /// Campaign-wide retry-target allowance shared across all throttle cohorts.
    ///
    /// A target reserves a slot on its first scheduled retry and retains that
    /// slot for later retries. Slots are reserved when the retry is scheduled,
    /// so a held cohort can consume slots before another cohort retries.
    ///
    /// Excess targets remain `PendingRetry` and pass through the read-back
    /// guard. This allowance does not bound the initial submission or the
    /// number of unresolved outcomes retained by the campaign.
    pub retry_queue_cap: usize,
}

impl Default for MutationConfig {
    fn default() -> Self {
        Self {
            fanout_buffer: 256,
            retry_queue_cap: 4096,
        }
    }
}

/// Identity of an installed account connection, owned by `Incarnation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccountGeneration(u64);

impl AccountGeneration {
    pub(crate) fn successor(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub(crate) fn is_replacement(self) -> bool {
        self.0 != 0
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }
}

/// One installed account instance together with the account generation it was
/// installed under.
///
/// The engine swaps whole incarnations, so a worker that loads one holds the
/// account and its generation from the same atomic read. A recovery request
/// stamps the generation of the incarnation its failing operation ran
/// against, and the reopen listener declines it, under the reopen lock, once a
/// reattach has installed a different one. Reading the generation from the
/// account-generation watch instead would straddle the cutover: reattach swaps
/// the account and only afterwards publishes the watch, so a read of the two
/// could pair an old account with a new generation.
///
/// Derefs to the account, so `incarnation.capabilities()` and
/// `incarnation.as_ref().as_ref()` reach it directly.
#[derive(Clone)]
pub struct Incarnation {
    account: Arc<dyn Account>,
    generation: crate::types::AccountGeneration,
}

impl Incarnation {
    /// Initial attachment identity; replacement connections advance from this.
    #[must_use]
    pub const fn initial_generation() -> AccountGeneration {
        AccountGeneration(0)
    }

    #[must_use]
    pub fn new(account: Arc<dyn Account>, generation: crate::types::AccountGeneration) -> Self {
        Self {
            account,
            generation,
        }
    }

    /// Identity to install on the next replacement connection.
    #[must_use]
    pub fn replacement_generation(&self) -> AccountGeneration {
        self.generation.successor()
    }

    /// The account generation this instance was installed under: zero at
    /// attach, one more at every committed reattach.
    #[must_use]
    pub fn generation(&self) -> AccountGeneration {
        self.generation
    }

    #[must_use]
    pub fn account(&self) -> &Arc<dyn Account> {
        &self.account
    }
}

impl std::ops::Deref for Incarnation {
    type Target = Arc<dyn Account>;
    fn deref(&self) -> &Self::Target {
        &self.account
    }
}

impl AsRef<dyn Account> for Incarnation {
    fn as_ref(&self) -> &(dyn Account + 'static) {
        self.account.as_ref()
    }
}

impl std::fmt::Debug for Incarnation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Incarnation")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
