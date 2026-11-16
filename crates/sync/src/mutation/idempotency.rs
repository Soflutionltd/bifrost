//! `IdempotencyKey` vending.
//!
//! Each mutation campaign uses a consumer-supplied `RunId`. The consumer owns
//! persistence across process restarts; `IdempotencyVendor`
//! then mints `IdempotencyKey`s with monotonically increasing
//! `sequence` numbers; the engine threads them through the protocol
//! crate's `bulk_*` surface for engine-side correlation.
//!
//! Built-in providers never carry `ProtocolSalt` onto the wire. Consumer
//! factories may attach bookkeeping payloads; the engine preserves the salt
//! across retries without interpreting it.

use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{IdempotencyKey, ProtocolKind, ProtocolSalt, RunId};
use uuid::Uuid;

/// Consumer-facing campaign identifier. Wraps a `Uuid` so engine code
/// can correlate vendor + retry queue + counters in one map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MutationCampaignId(pub Uuid);

impl MutationCampaignId {
    /// Mint a fresh campaign id.
    #[must_use]
    pub fn fresh() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Vends `IdempotencyKey`s for a mutation campaign's throttle cohorts.
/// Each cohort allocates once and retains its key across retries.
///
/// Callers that need correlation across process restarts own persistence of
/// `run_id` and supply it through `with_run_id` when resuming. This vendor
/// keeps the supplied identity in memory; it does not persist campaign state.
pub struct IdempotencyVendor {
    run_id: RunId,
    sequence: AtomicU64,
    // A consumer hook, not a second protocol registry. Keep payload-bearing
    // salts reachable even though built-in providers do not read the payload.
    salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync>,
}

impl IdempotencyVendor {
    /// Construct a vendor with a freshly-minted `run_id`. Callers that
    /// resume a campaign across process restarts must instead use
    /// `with_run_id`.
    #[must_use]
    pub fn fresh(salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync>) -> Self {
        Self::with_run_id(RunId(Uuid::new_v4().to_string()), salt_for)
    }

    #[must_use]
    pub fn with_run_id(
        run_id: RunId,
        salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync>,
    ) -> Self {
        Self {
            run_id,
            sequence: AtomicU64::new(0),
            salt_for,
        }
    }

    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    /// Mint the next key for `protocol`. Atomic + monotonic.
    pub fn next(&self, protocol: ProtocolKind) -> IdempotencyKey {
        IdempotencyKey {
            run_id: self.run_id.clone(),
            sequence: self.sequence.fetch_add(1, Ordering::AcqRel),
            protocol_salt: (self.salt_for)(protocol),
        }
    }
}

/// Default bookkeeping salt factory. Payload-bearing variants carry an empty
/// string; built-in providers never send the salt on the wire.
pub fn default_salt_factory() -> Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync> {
    Box::new(ProtocolSalt::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_is_monotonic() {
        let v = IdempotencyVendor::fresh(default_salt_factory());
        let a = v.next(ProtocolKind::Jmap);
        let b = v.next(ProtocolKind::Jmap);
        assert_eq!(a.run_id, b.run_id);
        assert!(b.sequence > a.sequence);
    }

    #[test]
    fn run_id_persists_across_mints() {
        let v = IdempotencyVendor::with_run_id(RunId("fixed".into()), default_salt_factory());
        let a = v.next(ProtocolKind::Imap);
        let b = v.next(ProtocolKind::Imap);
        assert_eq!(a.run_id, RunId("fixed".into()));
        assert_eq!(b.run_id, RunId("fixed".into()));
    }

    /// The default delegates to the exhaustive types-owned conversion.
    #[test]
    fn every_live_protocol_has_its_own_salt() {
        let factory = default_salt_factory();
        let live = [
            (ProtocolKind::Jmap, ProtocolSalt::Jmap(String::new())),
            (ProtocolKind::Gmail, ProtocolSalt::Gmail(String::new())),
            (ProtocolKind::Graph, ProtocolSalt::Graph(String::new())),
            (ProtocolKind::Imap, ProtocolSalt::Imap),
            (ProtocolKind::CardDav, ProtocolSalt::CardDav),
            (ProtocolKind::CalDav, ProtocolSalt::CalDav),
        ];
        for (kind, expected) in live {
            assert_eq!(
                factory(kind),
                expected,
                "{kind:?} must map to its own salt, not fall through the catch-all"
            );
        }
    }
}
