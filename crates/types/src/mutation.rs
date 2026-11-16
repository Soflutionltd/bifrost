//! Mutation, projection, and hydration types.
//!
//! Read- and write-side both share the streaming `SyncEvent<Batch<T>>`
//! contract; the types here are the per-item payloads on each side
//! plus the fingerprint shape consumers diff inventory against.

use bytes::Bytes;

use crate::blob::BlobHandle;
use crate::ids::ObjectId;

/// Server observation used in fingerprints and conditional mutations.
///
/// ETag and JMAP observations retain their producing protocol and object type.
/// Consumers pass the read-side value back unchanged; a token for another door
/// is refused locally rather than retried forever as a concurrency conflict.
/// Preconditions follow the request shape: patches own their version, while
/// deletes and RSVP take it as an argument because they have no patch payload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ServerVersion {
    /// IMAP CONDSTORE / QRESYNC per-message MODSEQ.
    ModSeq(u64),
    /// Entity validator with its producing protocol and object type.
    ETag(EntityVersion),
    /// Gmail historyId snapshot covering this item (not a write precondition).
    HistoryAt(u64),
    /// JMAP type state, bound to the native account that produced it.
    StateAt(JmapStateVersion),
    /// No server version; fingerprints fall back to size and flags.
    Unavailable,
}

/// An observed entity validator. Mint with [`ServerVersion::etag`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntityVersion {
    protocol: crate::Protocol,
    object_type: crate::ObjectType,
    tag: crate::EntityTag,
}

impl EntityVersion {
    #[must_use]
    pub fn protocol(&self) -> crate::Protocol {
        self.protocol
    }

    #[must_use]
    pub fn object_type(&self) -> crate::ObjectType {
        self.object_type
    }
}

/// An observed JMAP /get state. Query states are not mutation preconditions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JmapStateVersion {
    account_id: String,
    object_type: crate::ObjectType,
    state: String,
}

impl JmapStateVersion {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn object_type(&self) -> crate::ObjectType {
        self.object_type
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.state
    }
}

impl ServerVersion {
    /// Mint at the provider read boundary, preserving opaque token bytes.
    #[must_use]
    pub fn etag(
        protocol: crate::Protocol,
        object_type: crate::ObjectType,
        tag: impl Into<crate::EntityTag>,
    ) -> Self {
        Self::ETag(EntityVersion {
            protocol,
            object_type,
            tag: tag.into(),
        })
    }

    /// Mint from a JMAP /get response and its selected native account.
    #[must_use]
    pub fn jmap_state(
        account_id: impl Into<String>,
        object_type: crate::ObjectType,
        state: impl Into<String>,
    ) -> Self {
        Self::StateAt(JmapStateVersion {
            account_id: account_id.into(),
            object_type,
            state: state.into(),
        })
    }

    /// Borrow the original observation for display or provider-owned saves.
    /// This accessor does not authorize a conditional mutation.
    #[must_use]
    pub fn entity_tag(&self) -> Option<&crate::EntityTag> {
        match self {
            Self::ETag(version) => Some(&version.tag),
            _ => None,
        }
    }

    /// Validate provenance for opaque JSON validators, such as People source
    /// etags. HTTP header users must additionally call `expected_etag`.
    pub fn expected_opaque_etag(
        &self,
        protocol: crate::Protocol,
        operation: crate::AccountOperation,
    ) -> Result<&str, crate::AccountError> {
        if let Self::ETag(version) = self
            && version.protocol == protocol
            && Some(version.object_type) == mutation_object_type(operation)
            && !version.tag.is_empty()
        {
            return Ok(version.tag.as_str());
        }
        Err(crate::AccountError::unsupported(operation, protocol))
    }

    /// Validate a caller version for an entity-tag conditional update.
    /// DAV-normalized strong tags and wire tags retain their existing syntax.
    pub fn expected_etag(
        &self,
        protocol: crate::Protocol,
        operation: crate::AccountOperation,
        strong: bool,
    ) -> Result<&str, crate::AccountError> {
        let token = self.expected_opaque_etag(protocol, operation)?;
        if self.entity_tag().is_some_and(|tag| tag.valid_http(strong)) {
            return Ok(token);
        }
        Err(crate::AccountError::unsupported(operation, protocol))
    }

    /// Validate both axes of JMAP's (accountId, type) /set precondition.
    pub fn expected_jmap_state(
        &self,
        account_id: &str,
        operation: crate::AccountOperation,
    ) -> Result<&str, crate::AccountError> {
        if let Self::StateAt(version) = self
            && !version.state.is_empty()
            && !version.account_id.is_empty()
            && version.account_id == account_id
            && Some(version.object_type) == mutation_object_type(operation)
        {
            return Ok(&version.state);
        }
        Err(crate::AccountError::unsupported(
            operation,
            crate::Protocol::Jmap,
        ))
    }
}

fn mutation_object_type(operation: crate::AccountOperation) -> Option<crate::ObjectType> {
    use crate::{AccountOperation as Op, ObjectType};
    match operation {
        Op::UpdateFlags | Op::BulkMove | Op::BulkDestroy => Some(ObjectType::Email),
        Op::ContactUpdate | Op::ContactDelete => Some(ObjectType::Contact),
        Op::EventUpdate | Op::EventDelete | Op::EventRsvp => Some(ObjectType::CalendarEvent),
        _ => None,
    }
}

/// One bulk mail target and the version observed by its caller.
/// `None` preserves the provider's ordinary mutation behavior.
#[derive(Debug, Clone)]
pub struct MailMutationTarget {
    pub id: ObjectId,
    pub expected_version: Option<ServerVersion>,
}

impl From<ObjectId> for MailMutationTarget {
    fn from(id: ObjectId) -> Self {
        Self {
            id,
            expected_version: None,
        }
    }
}

/// Compose per-target mutation streams, preserving backpressure and one final
/// completion marker. Providers use this for independently guarded targets.
pub fn versioned_mail_stream(
    targets: crate::AccountStream<MailMutationTarget>,
    apply: impl FnMut(
        MailMutationTarget,
    ) -> crate::AccountStream<
        crate::SyncEvent<crate::ItemOutcome<crate::MutationSuccess>>,
    > + Send
    + 'static,
) -> crate::AccountStream<crate::SyncEvent<crate::ItemOutcome<crate::MutationSuccess>>> {
    use futures::StreamExt;
    let flattened = targets.flat_map(apply);
    // `Terminated` ends a stream, so no `Done` follows it: the completion
    // marker is emitted only when every target ran to its own completion.
    let events = futures::stream::unfold(Some(Box::pin(flattened)), |state| async move {
        let mut stream = state?;
        loop {
            let Some(event) = stream.next().await else {
                return Some((crate::SyncEvent::Done(None), None));
            };
            if matches!(event, crate::SyncEvent::Done(_)) {
                continue;
            }
            if matches!(event, crate::SyncEvent::Terminated(_)) {
                return Some((event, None));
            }
            return Some((event, Some(stream)));
        }
    });
    Box::pin(events)
}

/// A local refusal correlated to a single bulk mail target.
pub fn refused_mail_target(
    id: ObjectId,
    error: crate::AccountError,
) -> crate::AccountStream<crate::SyncEvent<crate::ItemOutcome<crate::MutationSuccess>>> {
    Box::pin(futures::stream::once(async move {
        crate::SyncEvent::Batch(crate::Batch {
            items: vec![crate::ItemOutcome::Failed(crate::BatchFailure::new(
                crate::BatchItemId(id.0),
                error,
            ))],
            page_boundary: crate::PageBoundary::Page,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: None,
        })
    }))
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
/// Set semantics use [`crate::Flag`] identity; ordering and duplicates do not
/// affect the result. A separator byte terminates each flag
/// so that `{"ab", "c"}` and `{"a", "bc"}` hash differently. The empty set has
/// a defined, non-zero hash.
///
/// FNV-1a, chosen for stability rather than strength: this value is compared,
/// never trusted, and a fixed algorithm here is the whole point - it must not
/// drift between releases or between producers.
#[must_use]
pub fn canonical_flags_hash(flags: impl IntoIterator<Item = impl Into<crate::Flag>>) -> u64 {
    let mut flags = flags
        .into_iter()
        .map(Into::into)
        .collect::<Vec<crate::Flag>>();
    flags.sort_unstable_by(|a, b| a.identity().cmp(b.identity()));
    flags.dedup();

    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for flag in flags {
        for byte in flag.identity().bytes().chain(std::iter::once(0xff)) {
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
/// Unlike `crate::HydrationProjection`, this selects flags, inventory
/// metadata or MIME bytes, not fields of a parsed `crate::Message`.
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
/// Consumers pattern-match the returned shape. The object-safe Account
/// stream accepts a runtime projection, so its result type cannot depend
/// on that value. Flag and membership read-back consumers must check the
/// variant before using it as evidence of the requested state.
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
    FlagsOnly(crate::FlagSet),
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
    Add(crate::FlagSet),
    /// Difference the carried flag set from existing flags
    /// (IMAP `-FLAGS`, Gmail `removeLabelIds`).
    Remove(crate::FlagSet),
    /// Replace existing flags with the carried set wholesale
    /// (IMAP `FLAGS`). Not directly expressible on Gmail; the
    /// Account impl translates via add+remove.
    Set(crate::FlagSet),
    /// Both add and remove in one wire operation
    /// (IMAP `+FLAGS` then `-FLAGS` is two STOREs; Gmail
    /// `messages.batchModify` accepts both in one request).
    Patch {
        add: crate::FlagSet,
        remove: crate::FlagSet,
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
    /// Flag identity comes from [`crate::Flag`], matching
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
                let overlaps = !add.is_disjoint(remove);
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
    /// return the shared classified caller-error shape. Every caller enters
    /// through its `bulk_set_flags` door, reached by the engine's `SetFlags`
    /// pipeline, so `UpdateFlags` is the correct operation stamp here.
    pub fn validate_for_account(
        &self,
        protocol: crate::error::Protocol,
    ) -> Result<(), crate::error::AccountError> {
        self.validate().map_err(|error| {
            crate::error::AccountErrorBuilder::new(crate::error::PrimaryCause::Request(
                crate::error::RequestCause::InvalidArgument {
                    field: Some("flags"),
                    message: Some(crate::error::DiagnosticText::support_only(
                        error.to_string(),
                    )),
                },
            ))
            .operation(crate::error::AccountOperation::UpdateFlags)
            .protocol(protocol)
            .try_build()
            .expect("valid flag operation error classification")
        })
    }
}

/// Whether an observed flag state already is the state a flag operation
/// asks for, judged in the observing provider's own flag vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlagTargetState {
    /// The observed state is the requested target state.
    Satisfied,
    /// The observed state is not the requested target state, and applying
    /// the operation would change it.
    Unsatisfied,
    /// The observation cannot decide: the operation names something the
    /// observation cannot see or the write does not model, or the operation
    /// is invalid or unknown.
    Indeterminate,
}

/// The generic flag judge: [`crate::Flag`] identity, matching
/// [`FlagOp::validate`] and [`canonical_flags_hash`]. An operation that fails
/// validation is `Indeterminate`.
///
/// The match below deliberately has no wildcard arm: `#[non_exhaustive]` does
/// not apply inside this crate, so a future `FlagOp` variant fails to compile
/// here and must be classified on purpose.
#[must_use]
pub fn flag_target_state(observed: &crate::FlagSet, op: &FlagOp) -> FlagTargetState {
    if op.validate().is_err() {
        return FlagTargetState::Indeterminate;
    }
    let matches = match op {
        FlagOp::Add(flags) => flags.is_subset(observed),
        FlagOp::Remove(flags) => flags.is_disjoint(observed),
        FlagOp::Set(flags) => flags == observed,
        FlagOp::Patch { add, remove } => add.is_subset(observed) && remove.is_disjoint(observed),
    };
    if matches {
        FlagTargetState::Satisfied
    } else {
        FlagTargetState::Unsatisfied
    }
}

#[cfg(test)]
mod tests {
    use super::{FlagOp, FlagOpValidationError, canonical_flags_hash};

    #[test]
    fn caller_etags_cannot_turn_into_a_blind_or_wildcard_condition() {
        use super::ServerVersion;
        use crate::error::{AccountErrorKind, AccountOperation, Protocol};

        let operation = AccountOperation::ContactUpdate;
        for version in [
            ServerVersion::Unavailable,
            ServerVersion::ModSeq(1),
            ServerVersion::etag(Protocol::CardDav, crate::ObjectType::Contact, "W/\"weak\""),
            ServerVersion::etag(Protocol::CardDav, crate::ObjectType::Contact, "w/\"weak\""),
            ServerVersion::etag(
                Protocol::CardDav,
                crate::ObjectType::Contact,
                "bad\r\nvalue",
            ),
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
            ServerVersion::etag(Protocol::CardDav, crate::ObjectType::Contact, "normalized")
                .expected_etag(Protocol::CardDav, operation, true)
                .unwrap(),
            "normalized"
        );
        assert!(
            ServerVersion::etag(Protocol::Graph, crate::ObjectType::Contact, "*")
                .expected_etag(Protocol::Graph, operation, false)
                .is_err()
        );
        assert_eq!(
            ServerVersion::etag(Protocol::Graph, crate::ObjectType::Contact, "W/\"opaque\"")
                .expected_etag(Protocol::Graph, operation, false)
                .unwrap(),
            "W/\"opaque\""
        );
    }

    #[test]
    fn caller_versions_reject_other_protocols_types_and_jmap_owners() {
        use super::ServerVersion;
        use crate::{AccountOperation as Op, ObjectType as Ty, Protocol};

        for (operation, ty) in [
            (Op::UpdateFlags, Ty::Email),
            (Op::BulkMove, Ty::Email),
            (Op::BulkDestroy, Ty::Email),
            (Op::ContactUpdate, Ty::Contact),
            (Op::ContactDelete, Ty::Contact),
            (Op::EventUpdate, Ty::CalendarEvent),
            (Op::EventDelete, Ty::CalendarEvent),
            (Op::EventRsvp, Ty::CalendarEvent),
        ] {
            let right = ServerVersion::etag(Protocol::Graph, ty, "\"same\"");
            assert_eq!(
                right
                    .expected_etag(Protocol::Graph, operation, false)
                    .unwrap(),
                "\"same\""
            );
            for wrong in [
                ServerVersion::etag(Protocol::Ews, ty, "\"same\""),
                ServerVersion::etag(Protocol::Graph, Ty::Mailbox, "\"same\""),
            ] {
                assert!(
                    wrong
                        .expected_etag(Protocol::Graph, operation, false)
                        .is_err()
                );
                assert!(
                    wrong
                        .expected_opaque_etag(Protocol::Graph, operation)
                        .is_err()
                );
            }
            let right = ServerVersion::jmap_state("primary", ty, "same");
            assert_eq!(
                right.expected_jmap_state("primary", operation).unwrap(),
                "same"
            );
            for wrong in [
                ServerVersion::jmap_state("shared", ty, "same"),
                ServerVersion::jmap_state("primary", Ty::Mailbox, "same"),
                ServerVersion::jmap_state("primary", ty, ""),
            ] {
                assert_ne!(right, wrong);
                assert!(wrong.expected_jmap_state("primary", operation).is_err());
            }
        }
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
            FlagOp::Add(crate::FlagSet::new()).validate(),
            Err(FlagOpValidationError::EmptyDelta)
        );
        assert_eq!(
            FlagOp::Patch {
                add: crate::FlagSet::from(["\\Seen".to_string()]),
                remove: crate::FlagSet::from(["\\seen".to_string()]),
            }
            .validate(),
            Err(FlagOpValidationError::ContradictoryPatch)
        );
        assert!(FlagOp::Set(crate::FlagSet::new()).validate().is_ok());
    }
}

#[cfg(test)]
mod versioned_mail_tests {
    use super::*;
    use futures::{FutureExt, StreamExt};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn terminal_versioned_mutation_does_not_pull_the_next_target() {
        let pulled = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&pulled);
        let targets = futures::stream::iter([ObjectId("first".into()), ObjectId("second".into())])
            .inspect(move |_| {
                count.fetch_add(1, Ordering::Relaxed);
            })
            .map(MailMutationTarget::from);
        let events = versioned_mail_stream(Box::pin(targets), |_| {
            Box::pin(futures::stream::once(async {
                crate::SyncEvent::Terminated(crate::AccountError::unsupported(
                    crate::AccountOperation::UpdateFlags,
                    crate::Protocol::Imap,
                ))
            }))
        })
        .collect::<Vec<_>>()
        .now_or_never()
        .expect("in-memory streams are ready");
        assert!(matches!(
            events.as_slice(),
            [crate::SyncEvent::Terminated(_)]
        ));
        assert_eq!(pulled.load(Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
mod target_tests {
    use super::*;

    fn set(items: &[&str]) -> crate::FlagSet {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn add_target_matches_when_subset_present() {
        let obs = set(&["\\Seen", "\\Flagged"]);
        let op = FlagOp::Add(set(&["\\Seen"]));
        assert_eq!(flag_target_state(&obs, &op), FlagTargetState::Satisfied);
    }

    #[test]
    fn add_target_does_not_match_when_missing() {
        let obs = set(&["\\Flagged"]);
        let op = FlagOp::Add(set(&["\\Seen"]));
        assert_eq!(flag_target_state(&obs, &op), FlagTargetState::Unsatisfied);
    }

    #[test]
    fn remove_target_matches_when_absent() {
        let obs = set(&["\\Flagged"]);
        let op = FlagOp::Remove(set(&["\\Seen"]));
        assert_eq!(flag_target_state(&obs, &op), FlagTargetState::Satisfied);
    }

    #[test]
    fn set_target_requires_exact_match() {
        let obs = set(&["\\Seen"]);
        let op = FlagOp::Set(set(&["\\Seen"]));
        assert_eq!(flag_target_state(&obs, &op), FlagTargetState::Satisfied);
        let op2 = FlagOp::Set(set(&["\\Seen", "\\Flagged"]));
        assert_eq!(flag_target_state(&obs, &op2), FlagTargetState::Unsatisfied);
    }

    #[test]
    fn patch_requires_both_sides() {
        let obs = set(&["\\Seen"]);
        let op = FlagOp::Patch {
            add: set(&["\\Seen"]),
            remove: set(&["\\Flagged"]),
        };
        assert_eq!(flag_target_state(&obs, &op), FlagTargetState::Satisfied);
        let op2 = FlagOp::Patch {
            add: set(&["\\Seen"]),
            remove: set(&["\\Seen"]),
        };
        assert_eq!(
            flag_target_state(&obs, &op2),
            FlagTargetState::Indeterminate
        );
    }

    #[test]
    fn flag_target_state_is_ascii_case_insensitive_for_every_op() {
        let observed = set(&["\\seen"]);
        for op in [
            FlagOp::Add(set(&["\\Seen"])),
            FlagOp::Remove(set(&["\\FLAGGED"])),
            FlagOp::Set(set(&["\\SEEN"])),
            FlagOp::Patch {
                add: set(&["\\Seen"]),
                remove: set(&["\\Flagged"]),
            },
        ] {
            assert_eq!(
                flag_target_state(&observed, &op),
                FlagTargetState::Satisfied
            );
        }
    }

    #[test]
    fn flag_target_state_set_equality_folds_case_duplicates() {
        assert_eq!(
            flag_target_state(&set(&["\\seen"]), &FlagOp::Set(set(&["\\Seen", "\\SEEN"]))),
            FlagTargetState::Satisfied
        );
    }

    #[test]
    fn flag_target_state_refuses_invalid_ops() {
        for op in [
            FlagOp::Add(set(&[])),
            FlagOp::Patch {
                add: set(&["x"]),
                remove: set(&["X"]),
            },
        ] {
            assert_eq!(
                flag_target_state(&set(&[]), &op),
                FlagTargetState::Indeterminate
            );
        }
    }
}
