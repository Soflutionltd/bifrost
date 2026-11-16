//! `IdempotencyKey` vending.
//!
//! Each mutation campaign uses a consumer-supplied `RunId`. The consumer owns
//! persistence across process restarts; `IdempotencyVendor`
//! then mints `IdempotencyKey`s with monotonically increasing
//! `sequence` numbers. Keys stay inside the engine and are returned in
//! `MutationCounters::cohort_keys` for consumer bookkeeping.
//!
//! Accounts never receive the key or carry `ProtocolSalt` onto the wire. Consumer
//! factories may attach bookkeeping payloads; the engine preserves the salt
//! across retries without interpreting it.

use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{ProtocolKind, RunId};
use uuid::Uuid;

/// Engine-local identity for a bulk mutation cohort, frozen across retries.
///
/// Distinct from `bifrost_types::MutationConcurrency::StateBased`: that is
/// optimistic concurrency enforced on the wire. This key only correlates the
/// engine's attempts at one cohort; it neither deduplicates server writes nor
/// authorizes replay. Accounts never receive it. Uncertain writes still require
/// the engine's read-back guard and declared desired-state reassertion policy.
#[derive(Debug, Clone)]
pub struct IdempotencyKey {
    pub run_id: RunId,
    pub sequence: u64,
    pub protocol_salt: ProtocolSalt,
}

/// Protocol-tagged salt used by the engine's idempotency bookkeeping.
///
/// Carried alongside `RunId` and `sequence` for protocol-specific correlation.
/// The engine preserves the salt across retries without interpreting it;
/// accounts never receive it. Custom factories may supply
/// nonempty payloads as consumer bookkeeping identity, not cursor provenance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProtocolSalt {
    /// JMAP has no documented wire replay token; `ifInState` is the
    /// orthogonal concurrency primitive.
    Jmap(String),
    /// Gmail has no documented wire replay token; `X-Goog-Request-Id`
    /// is not a dedup primitive on the messages endpoints.
    Gmail(String),
    /// Microsoft Graph has no documented wire replay token;
    /// `client-request-id` is debugging correlation only.
    Graph(String),
    /// IMAP carries no replay token; the engine relies on read-back of
    /// uncertain writes and declared desired-state reassertion.
    Imap,
    /// CardDAV carries no replay token; consumers use etags where
    /// available and the engine keeps idempotency local.
    CardDav,
    /// CalDAV carries no replay token, on the same terms as CardDAV.
    CalDav,
}

impl From<ProtocolKind> for ProtocolSalt {
    /// Default bookkeeping salt. Custom factories can still supply payloads.
    fn from(protocol: ProtocolKind) -> Self {
        // Every known protocol is named explicitly below.
        match protocol {
            ProtocolKind::Jmap => Self::Jmap(String::new()),
            ProtocolKind::Imap => Self::Imap,
            ProtocolKind::Gmail => Self::Gmail(String::new()),
            ProtocolKind::CardDav => Self::CardDav,
            ProtocolKind::CalDav => Self::CalDav,
            ProtocolKind::Graph => Self::Graph(String::new()),
            // ProtocolKind is non_exhaustive and lives in another crate, so a
            // wildcard is mandatory. An unknown protocol has no wire replay
            // token either; the salt carries no meaning for it.
            _ => Self::Imap,
        }
    }
}

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
    // salts reachable for engine-side consumer bookkeeping.
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
/// string; accounts never receive the salt.
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

    #[test]
    fn custom_payload_is_preserved_for_consumer_bookkeeping() {
        let vendor = IdempotencyVendor::with_run_id(
            RunId("custom-run".into()),
            Box::new(|_| ProtocolSalt::Jmap("consumer-correlation".into())),
        );
        let key = vendor.next(ProtocolKind::Jmap);
        assert_eq!(key.run_id, RunId("custom-run".into()));
        assert_eq!(
            key.protocol_salt,
            ProtocolSalt::Jmap("consumer-correlation".into()),
        );
    }

    /// The default delegates to the exhaustive engine-owned conversion.
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
