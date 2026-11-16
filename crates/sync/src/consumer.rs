//! Consumer controls, durable boundaries, and injected push ingress.

use bifrost_types::{
    AccountError, AccountId, Checkpoint, CursorScope, InvalidationHint, Partition, Priority,
    Warning,
};

/// Complete durable checkpoint boundary for one account.
///
/// One account may have several cursor scopes and several concurrently driven
/// backfill partitions. A single [`Checkpoint`] cannot describe that state, so
/// control boundaries return this collection instead. There is at most one
/// entry for each change scope and each `(scope, partition)` backfill lane -
/// [`DurableCheckpointSet::new`] enforces that, so a duplicate lane is not
/// representable and equality cannot depend on which side of the comparison a
/// repeated entry landed on.
#[derive(Debug, Clone, Default)]
pub struct DurableCheckpointSet {
    checkpoints: Vec<Checkpoint>,
}

/// Durable row identity for the checkpoint kinds supported by this engine.
#[derive(PartialEq)]
enum CheckpointLane<'a> {
    Change(&'a CursorScope),
    Backfill(&'a CursorScope, &'a Partition),
}

impl<'a> CheckpointLane<'a> {
    fn of(checkpoint: &'a Checkpoint) -> Self {
        match checkpoint {
            Checkpoint::Change(cursor) => Self::Change(&cursor.scope),
            Checkpoint::Backfill(backfill) => Self::Backfill(&backfill.scope, &backfill.partition),
            // Checkpoint is non-exhaustive across crates. A new kind needs an
            // explicit durable row identity before this engine can retain it.
            _ => unreachable!("checkpoint kind requires a durable lane mapping"),
        }
    }
}

impl PartialEq for DurableCheckpointSet {
    /// Order-insensitive, and symmetric because it compares multiplicities in
    /// both directions rather than one-way containment.
    fn eq(&self, other: &Self) -> bool {
        fn count(haystack: &[Checkpoint], needle: &Checkpoint) -> usize {
            haystack.iter().filter(|entry| *entry == needle).count()
        }
        self.checkpoints.len() == other.checkpoints.len()
            && self
                .checkpoints
                .iter()
                .all(|entry| count(&self.checkpoints, entry) == count(&other.checkpoints, entry))
    }
}

impl Eq for DurableCheckpointSet {}

impl DurableCheckpointSet {
    /// Normalizes to at most one entry per durable lane, keeping the last
    /// occurrence of each - callers build the vector in publication order, so
    /// the last one is the most recently made durable.
    #[must_use]
    pub fn new(checkpoints: Vec<Checkpoint>) -> Self {
        let mut normalized: Vec<Checkpoint> = Vec::with_capacity(checkpoints.len());
        for checkpoint in checkpoints {
            let lane = CheckpointLane::of(&checkpoint);
            let existing = normalized
                .iter()
                .position(|candidate| CheckpointLane::of(candidate) == lane);
            match existing {
                Some(slot) => normalized[slot] = checkpoint,
                None => normalized.push(checkpoint),
            }
        }
        Self {
            checkpoints: normalized,
        }
    }

    #[must_use]
    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checkpoints.is_empty()
    }
}

/// Consumer-injected push vocabulary for `InvalidationSink::push`.
/// Invalidations may coalesce into wider reconciles. Health and advisory
/// events are broadcast synchronously; classified failures route through
/// engine recovery. A refused directive is returned to the caller to retry.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WatchEvent {
    Invalidated { hint: InvalidationHint },
    Disconnected,
    Reconnected,
    Terminated(AccountError),
    Warning(Warning),
}

/// Sink for out-of-process push events (Gmail Pub/Sub listeners,
/// Graph webhook receivers). The consumer wires its receiver to
/// invoke `push` on every event. Invalidations enter bounded pending state;
/// control events are acted on synchronously. Callers must retain and retry
/// `SinkPush::Refused`, including through a consumer pause.
pub trait InvalidationSink: Send + Sync + 'static {
    fn push(&self, account: AccountId, event: WatchEvent) -> SinkPush;
}

/// Result of synchronous consumer push delivery.
#[must_use = "a refused event is handed back for the caller to retry"]
#[derive(Debug)]
#[non_exhaustive]
pub enum SinkPush {
    /// Recorded or acted on, possibly as a wider invalidation.
    Accepted,
    /// A directive could not be queued: the account is paused or
    /// checkpointing, or its recovery queue is full. Resubmit after resume
    /// or once the queue drains. Refusals may persist through a pause.
    Refused(WatchEvent),
    /// No such account is attached.
    NotAttached,
}

/// Consumer-to-producer control handle returned alongside every
/// engine-driven stream.
///
/// `pause` and `checkpoint_now` are async because they must wait for
/// "stream is at a safe boundary, here is the latest checkpoint if
/// one exists, you can now drop." The returned set is empty when an idle
/// account has never produced a checkpoint. `resume`, `priority`,
/// `bandwidth_cap`, and
/// `bandwidth_observed` stay synchronous (fire-and-forget signals or
/// pure reads).
pub trait Control: Send + Sync {
    fn pause(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DurableCheckpointSet, AccountError>>
                + Send
                + '_,
        >,
    >;
    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DurableCheckpointSet, AccountError>>
                + Send
                + '_,
        >,
    >;
    fn resume(&self);
    fn priority(&self, p: Priority);
    fn bandwidth_cap(&self, bps: Option<bifrost_types::BandwidthCap>);
    fn bandwidth_observed(&self) -> u64;
}

/// Per-account control signal exchanged between the engine and the
/// consumer. Carried on `AccountControlEvent` channels so the consumer
/// can pause an account (e.g. on operator-override directives) or
/// resume it after intervention.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccountControl {
    /// Negotiated capabilities changed at a committed replacement open.
    CapabilitiesChanged(bifrost_types::CapabilityDelta),
    Pause(PauseReason),
    Resume,
}

/// Reason an account is paused. Bounded enum, not a free-form string
/// or `DiagnosticText`: the engine is the producer and the set of
/// pause causes is enumerable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PauseReason {
    /// The protocol or engine surfaced `EngineDirective::
    /// OperatorOverrideRequired`. Consumer must resolve the underlying
    /// issue (typically reachable through the account stream's prior
    /// `Warning::OperatorAttentionNeeded`) before resuming.
    OperatorOverrideRequired,
    /// Consumer-initiated pause via `Control::pause` or equivalent.
    ConsumerRequested,
    /// Engine paused this account because a tenant-level throttle
    /// covers it. Resumes automatically once the throttle clears.
    TenantThrottle,
    /// Engine exhausted its retry budget for a recurring failure;
    /// consumer intervention required before further attempts.
    RetryBudgetExhausted,
}

#[cfg(test)]
mod tests {
    use super::Checkpoint;
    use bifrost_types::{ChangeCursor, CursorScope, OpaqueChangeState, ProtocolKind};

    fn cursor(scope: CursorScope, bytes: &[u8]) -> Checkpoint {
        Checkpoint::Change(ChangeCursor {
            scope,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: bifrost_types::PayloadVersion::new(1),
                bytes: bytes.to_vec(),
            },
            advanced_through: None,
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        })
    }

    /// A set built from a repeated lane must not compare equal to one built
    /// from two distinct lanes, in EITHER order.
    ///
    /// The first implementation compared equal lengths and then checked one-way
    /// containment, which made `[a, a] == [a, b]` while `[a, b] != [a, a]` - an
    /// asymmetric `PartialEq`, which is undefined behaviour as far as every
    /// generic container that relies on the contract is concerned.
    #[test]
    fn a_repeated_lane_is_never_equal_to_two_distinct_lanes() {
        let a = cursor(CursorScope::Account, b"a");
        let b = cursor(CursorScope::Type(bifrost_types::ObjectType::Email), b"b");

        let repeated = super::DurableCheckpointSet::new(vec![a.clone(), a.clone()]);
        let distinct = super::DurableCheckpointSet::new(vec![a.clone(), b]);

        assert_ne!(repeated, distinct);
        assert_ne!(distinct, repeated);
        assert_eq!(
            repeated,
            super::DurableCheckpointSet::new(vec![a]),
            "a repeated lane normalizes to a single entry"
        );
    }

    /// Order is not part of the value: the same lanes in either order are the
    /// same durable boundary.
    #[test]
    fn lane_order_does_not_change_the_boundary() {
        let a = cursor(CursorScope::Account, b"a");
        let b = cursor(CursorScope::Type(bifrost_types::ObjectType::Email), b"b");
        assert_eq!(
            super::DurableCheckpointSet::new(vec![a.clone(), b.clone()]),
            super::DurableCheckpointSet::new(vec![b, a])
        );
    }

    /// The later entry for a lane wins: callers build the vector in publication
    /// order, so keeping the earlier one would report a superseded checkpoint as
    /// the durable boundary.
    #[test]
    fn the_last_entry_for_a_lane_is_the_one_retained() {
        let older = cursor(CursorScope::Account, b"older");
        let newer = cursor(CursorScope::Account, b"newer");
        let set = super::DurableCheckpointSet::new(vec![older, newer.clone()]);
        assert_eq!(set.checkpoints(), &[newer]);
    }
}
