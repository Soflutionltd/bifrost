//! Mutation, projection, and hydration types.
//!
//! Read- and write-side both share the streaming `SyncEvent<Batch<T>>`
//! contract; the types here are the per-item payloads on each side
//! plus the fingerprint shape consumers diff inventory against.

use std::collections::HashSet;

use bytes::Bytes;

use crate::blob::BlobHandle;
use crate::ids::{ObjectId, RunId};

/// Server-side version stamp used in `Fingerprint`.
///
/// Cross-protocol diff degrades gracefully: when the protocol cannot
/// offer a server version, `Unavailable` falls back to
/// `(size, flags_hash)`-only comparison.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ServerVersion {
    /// IMAP CONDSTORE / QRESYNC per-message MODSEQ.
    ModSeq(u64),
    /// HTTP entity tag, including Graph wire tags and DAV normalized tags.
    ETag(String),
    /// Gmail historyId snapshot covering this item.
    HistoryAt(u64),
    /// JMAP State string covering this item.
    StateAt(String),
    /// IMAP Basic, or any protocol without a server version stamp.
    Unavailable,
}

impl ServerVersion {
    /// Validate a caller version for an entity-tag conditional update.
    ///
    /// DAV accepts its normalized unquoted strong ETags as well as wire ETags.
    /// Unsupported token kinds and weak DAV
    /// validators are refused locally rather than downgraded to a blind write.
    pub fn expected_etag(
        &self,
        protocol: crate::error::Protocol,
        operation: crate::error::AccountOperation,
        strong: bool,
    ) -> Result<&str, crate::error::AccountError> {
        if let Self::ETag(etag) = self {
            let weak = etag
                .get(..2)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"));
            if strong
                && !weak
                && etag
                    .bytes()
                    .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte) || byte >= 0x80)
            {
                return Ok(etag);
            }
            let value = etag.strip_prefix("W/").unwrap_or(etag);
            if (!strong || !etag.starts_with("W/"))
                && value.starts_with('"')
                && value.ends_with('"')
                && value.len() >= 2
                && value[1..value.len() - 1]
                    .bytes()
                    .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte) || byte >= 0x80)
            {
                return Ok(etag);
            }
        }
        Err(crate::error::AccountError::unsupported(operation, protocol))
    }
}

/// Cross-protocol inventory diff fingerprint. The consumer compares
/// `local.fingerprint != server.fingerprint` to decide whether to
/// refetch. `size` is `Option` because Graph does not expose message
/// size on the message resource; consumers fall back to
/// `(server_version, flags_hash)` there.
///
/// `flags_hash` MUST be produced by [`canonical_flags_hash`]. It is the one
/// field here with no self-describing representation, so every producer that
/// invented its own derivation made the field mean something different per
/// crate while looking comparable; the crate now owns the derivation so it
/// means one thing. A producer with no flag set passes an empty iterator
/// rather than hard-coding `0` - the empty set has a defined hash, and `0` is
/// not it. A producer whose object has no flags but does have tracked state
/// (a mailbox, a public-folder item) encodes that state as `key=value`
/// pseudo-flags and runs it through the same function.
///
/// The hash is comparable only within one provider and one object namespace.
/// Providers spell their flags differently (`\Seen` vs `$seen` vs `UNREAD`),
/// so a shared derivation buys a single definition of "same flag set", not
/// cross-provider equality.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fingerprint {
    pub server_version: ServerVersion,
    pub size: Option<u64>,
    pub flags_hash: u64,
}

/// Hash a logical flag set. The single derivation behind
/// [`Fingerprint::flags_hash`].
///
/// Set semantics: flag spelling is ASCII-case-insensitive, and ordering and
/// duplicates do not affect the result. A separator byte terminates each flag
/// so that `{"ab", "c"}` and `{"a", "bc"}` hash differently. The empty set has
/// a defined, non-zero hash.
///
/// FNV-1a, chosen for stability rather than strength: this value is compared,
/// never trusted, and a fixed algorithm here is the whole point - it must not
/// drift between releases or between producers.
#[must_use]
pub fn canonical_flags_hash(flags: impl IntoIterator<Item = impl AsRef<str>>) -> u64 {
    let mut flags = flags
        .into_iter()
        .map(|flag| flag.as_ref().to_ascii_lowercase())
        .collect::<Vec<_>>();
    flags.sort_unstable();
    flags.dedup();

    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for flag in flags {
        for byte in flag.bytes().chain(std::iter::once(0xff)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

/// Projection level for `Account::get_stream`.
///
/// Maps to JMAP `properties`, IMAP `FETCH` part list, Gmail `format`,
/// and Graph `$select`. `Inventory` is intentionally absent because
/// inventory has its own streaming primitive on the trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Projection {
    /// Id + flags only. Cheapest projection; used by the read-back
    /// guard after a retry.
    FlagsOnly,
    /// Id, flags, size, threading headers, memberships. The
    /// `Metadata`-shaped projection an inventory consumer would
    /// otherwise re-derive.
    Metadata,
    /// `Metadata` + Subject, From, To, Cc, Date for list-view UI.
    Headers,
    /// `Headers` + N bytes of decoded text/plain for preview.
    Preview(usize),
    /// `Headers` + the full text/plain part only.
    TextOnly,
    /// All parts decoded, no attachment bytes.
    Full,
    /// `Full` + inline attachment bytes.
    FullWithBlobs,
}

/// Variant of a hydrated object selected by `Projection`.
///
/// The engine treats the inner shapes as protocol-derived blobs of
/// fields. Consumers down-cast based on the projection they asked
/// for. Variants are intentionally coarse; the protocol crate fills
/// in the fields the projection requested.
///
/// Boxing the heaviest arm (Metadata) is not worth it: hydrated
/// streams of the FlagsOnly projection are rare on the hot path,
/// and Metadata is itself the inventory shape consumers expect to
/// pattern-match without a deref step.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum HydratedObjectKind {
    /// `Projection::FlagsOnly`. Carries the canonical flag set as
    /// the protocol observes it (current state, not a mutation).
    FlagsOnly(HashSet<String>),
    /// `Projection::Metadata`. Same shape inventory uses, hydrated
    /// from a fresh fetch.
    Metadata(crate::events::InventoryEntry),
    /// `Projection::Headers`, `Preview`, `TextOnly`, `Full`,
    /// `FullWithBlobs`. The raw RFC 822 / MIME bytes the protocol
    /// returned for the requested projection; consumers parse with
    /// their own MIME library. `Bytes` not `Vec<u8>` so a 5MB
    /// `HydratedObject::clone()` is a cheap refcount bump, not a
    /// copy.
    RawMime(Bytes),
}

/// Hydrated object payload yielded by `get_stream`.
///
/// The shape varies by `Projection`; the engine treats this as
/// opaque-ish and forwards to the consumer. The protocol crate
/// chooses one of the `HydratedObjectKind` variants based on the
/// requested projection.
#[derive(Debug, Clone)]
pub struct HydratedObject {
    pub id: ObjectId,
    pub kind: HydratedObjectKind,
    /// Blob handles for attached parts, when the projection requested
    /// them or when they are cheap to expose.
    pub blobs: Vec<BlobHandle>,
}

/// Idempotency surface for safe retry of bulk mutations.
///
/// Distinct from `MutationConcurrency::StateBased`: that is
/// optimistic concurrency on the wire, this is engine bookkeeping
/// for "did we already submit this exact batch?" No protocol today
/// emits `ProtocolSalt` onto the wire; all four rely on the engine's
/// read-back guard.
#[derive(Debug, Clone)]
pub struct IdempotencyKey {
    pub run_id: RunId,
    pub sequence: u64,
    pub protocol_salt: ProtocolSalt,
}

/// Protocol-tagged salt used by the engine's idempotency bookkeeping.
///
/// Carried alongside `RunId` and `sequence` so the engine can hash
/// requests deterministically per protocol without leaking the salt
/// onto the wire.
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
    /// IMAP carries no replay token; engine relies on the read-back
    /// guard after retry.
    Imap,
    /// CardDAV carries no replay token; consumers use etags where
    /// available and the engine keeps idempotency local.
    CardDav,
    /// CalDAV carries no replay token, on the same terms as CardDAV.
    ///
    /// Present because `ProtocolKind::CalDav` is a live protocol with a live
    /// account crate. Without it, `default_salt_factory`'s catch-all silently
    /// handed CalDAV the IMAP salt - harmless while the salt is engine-internal
    /// bookkeeping separated by `RunId` plus a monotonic sequence, but a
    /// catch-all absorbing a REAL protocol rather than a hypothetical future
    /// one stops being harmless the moment the salt acquires meaning.
    CalDav,
}

/// Bulk flag mutation. Carries both the operation and the flag set
/// in one type so a `FlagOp::Set` actually pins the target state
/// (the prior `FlagSet { add, remove } + separate FlagOp` shape
/// couldn't - `Set` had nowhere to put the target flags). Use
/// `Patch` for the IMAP-style additive+subtractive case where a
/// single STORE needs to mention both adds and removes.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FlagOp {
    /// Union the carried flag set into existing flags
    /// (IMAP `+FLAGS`, Gmail `addLabelIds`).
    Add(HashSet<String>),
    /// Difference the carried flag set from existing flags
    /// (IMAP `-FLAGS`, Gmail `removeLabelIds`).
    Remove(HashSet<String>),
    /// Replace existing flags with the carried set wholesale
    /// (IMAP `FLAGS`). Not directly expressible on Gmail; the
    /// Account impl translates via add+remove.
    Set(HashSet<String>),
    /// Both add and remove in one wire operation
    /// (IMAP `+FLAGS` then `-FLAGS` is two STOREs; Gmail
    /// `messages.batchModify` accepts both in one request).
    Patch {
        add: HashSet<String>,
        remove: HashSet<String>,
    },
}

/// Why a [`FlagOp`] cannot describe one deterministic target transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FlagOpValidationError {
    #[error("an additive or subtractive flag operation must name at least one flag")]
    EmptyDelta,
    #[error("a flag patch cannot add and remove the same flag")]
    ContradictoryPatch,
}

impl FlagOp {
    /// Validate the operation before it crosses an account boundary.
    ///
    /// Flag identity is ASCII-case-insensitive, matching
    /// [`canonical_flags_hash`]. `Set(empty)` remains valid because it means
    /// clear every flag; empty additive/subtractive deltas do not describe a
    /// mutation.
    pub fn validate(&self) -> Result<(), FlagOpValidationError> {
        match self {
            Self::Add(flags) | Self::Remove(flags) if flags.is_empty() => {
                Err(FlagOpValidationError::EmptyDelta)
            }
            Self::Patch { add, remove } if add.is_empty() && remove.is_empty() => {
                Err(FlagOpValidationError::EmptyDelta)
            }
            Self::Patch { add, remove } => {
                let overlaps = add.iter().any(|added| {
                    remove
                        .iter()
                        .any(|removed| added.eq_ignore_ascii_case(removed))
                });
                if overlaps {
                    Err(FlagOpValidationError::ContradictoryPatch)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    /// Validate at an [`Account`](crate::Account) implementation boundary and
    /// return the shared classified caller-error shape.
    pub fn validate_for_account(
        &self,
        protocol: crate::error::Protocol,
    ) -> Result<(), crate::error::AccountError> {
        self.validate().map_err(|error| {
            crate::error::AccountErrorBuilder::new(
                crate::error::AccountErrorKind::Request(crate::error::RequestErrorKind::Malformed),
                crate::error::Cause::Request(crate::error::RequestCause::InvalidArgument {
                    field: Some("flags"),
                    message: Some(crate::error::DiagnosticText::support_only(
                        error.to_string(),
                    )),
                }),
            )
            .operation(crate::error::AccountOperation::UpdateFlags)
            .protocol(protocol)
            .try_build()
            .expect("valid flag operation error classification")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{FlagOp, FlagOpValidationError, canonical_flags_hash};
    use std::collections::HashSet;

    #[test]
    fn caller_etags_cannot_turn_into_a_blind_or_wildcard_condition() {
        use super::ServerVersion;
        use crate::error::{AccountErrorKind, AccountOperation, Protocol};

        let operation = AccountOperation::ContactUpdate;
        for version in [
            ServerVersion::Unavailable,
            ServerVersion::ModSeq(1),
            ServerVersion::ETag("W/\"weak\"".into()),
            ServerVersion::ETag("w/\"weak\"".into()),
            ServerVersion::ETag("bad\r\nvalue".into()),
        ] {
            assert_eq!(
                *version
                    .expected_etag(Protocol::CardDav, operation, true)
                    .unwrap_err()
                    .kind(),
                AccountErrorKind::Unsupported(operation)
            );
        }
        assert_eq!(
            ServerVersion::ETag("normalized".into())
                .expected_etag(Protocol::CardDav, operation, true)
                .unwrap(),
            "normalized"
        );
        assert!(
            ServerVersion::ETag("*".into())
                .expected_etag(Protocol::Graph, operation, false)
                .is_err()
        );
        assert_eq!(
            ServerVersion::ETag("W/\"opaque\"".into())
                .expected_etag(Protocol::Graph, operation, false)
                .unwrap(),
            "W/\"opaque\""
        );
    }

    #[test]
    fn canonical_flag_hash_ignores_order_case_and_duplicates() {
        let first = canonical_flags_hash(["\\Seen", "$Important", "\\Seen"]);
        let second = canonical_flags_hash(["$IMPORTANT", "\\SEEN"]);
        assert_eq!(first, second);
    }

    #[test]
    fn canonical_flag_hash_has_unambiguous_boundaries() {
        assert_ne!(
            canonical_flags_hash(["ab", "c"]),
            canonical_flags_hash(["a", "bc"])
        );
        assert_ne!(canonical_flags_hash(std::iter::empty::<&str>()), 0);
    }

    #[test]
    fn flag_operations_reject_empty_deltas_and_contradictory_patches() {
        assert_eq!(
            FlagOp::Add(HashSet::new()).validate(),
            Err(FlagOpValidationError::EmptyDelta)
        );
        assert_eq!(
            FlagOp::Patch {
                add: HashSet::from(["\\Seen".to_string()]),
                remove: HashSet::from(["\\seen".to_string()]),
            }
            .validate(),
            Err(FlagOpValidationError::ContradictoryPatch)
        );
        assert!(FlagOp::Set(HashSet::new()).validate().is_ok());
    }
}
