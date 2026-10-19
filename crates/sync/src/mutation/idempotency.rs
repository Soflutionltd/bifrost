//! `IdempotencyKey` vending.
//!
//! Each mutation campaign uses a consumer-supplied `RunId`. The consumer owns
//! persistence across process restarts; `IdempotencyVendor`
//! then mints `IdempotencyKey`s with monotonically increasing
//! `sequence` numbers; the engine threads them through the protocol
//! crate's `bulk_*` surface for engine-side correlation.
//!
//! No protocol today carries `ProtocolSalt` onto the wire. The salt
//! is engine bookkeeping for retry-queue dedup and campaign
//! correlation.

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
/// `run_id` is persisted across process restarts so retries from the
/// previous process look correlated to engine bookkeeping; the
/// consumer-facing `MutationCampaignId` is the key into the
/// `CheckpointStore` slot that holds it.
pub struct IdempotencyVendor {
    run_id: RunId,
    sequence: AtomicU64,
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

/// Default salt factory that returns the empty-string variant per
/// protocol. Engine-internal bookkeeping; never travels on the wire.
pub fn default_salt_factory() -> Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync> {
    Box::new(|protocol| match protocol {
        ProtocolKind::Jmap => ProtocolSalt::Jmap(String::new()),
        ProtocolKind::Gmail => ProtocolSalt::Gmail(String::new()),
        ProtocolKind::Graph => ProtocolSalt::Graph(String::new()),
        ProtocolKind::Imap => ProtocolSalt::Imap,
        ProtocolKind::CardDav => ProtocolSalt::CardDav,
        ProtocolKind::CalDav => ProtocolSalt::CalDav,
        // `ProtocolKind` is `#[non_exhaustive]`; fall back to the IMAP
        // salt for unknown FUTURE protocols. The salt is engine
        // bookkeeping; it never travels on the wire so this is safe.
        //
        // Every protocol that exists today is named above, deliberately:
        // CalDAV used to reach this arm, so the catch-all was absorbing a
        // live protocol rather than a hypothetical one, which is the state
        // that stops being harmless the moment the salt acquires meaning.
        // Keep it that way - a new `ProtocolKind` gets an arm here.
        _ => ProtocolSalt::Imap,
    })
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

    /// Every protocol that EXISTS gets its own salt; the catch-all is reserved
    /// for protocols that do not exist yet. CalDAV used to fall through to the
    /// IMAP salt, so a live account crate was sharing another protocol's tag.
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
