//! Account-level capabilities.
//!
//! Read once at account-open via `Account::capabilities()`. Capability
//! transitions mid-session are not delivered through this surface; the
//! protocol crate ends affected streams with a capability-change error whose
//! recovery directive makes the engine re-open the account.

use std::time::Duration;

use serde::Serialize;

use crate::filter::FilterRuleShape;

/// How the cursor's identity is established.
///
/// - `ServerIssued`: cursor bytes are minted by the server and we
///   trust them (JMAP `State`, Gmail `historyId`, Graph `@odata.deltaLink`).
/// - `Hybrid`: cursor bytes are derived partly from server state and
///   partly from client-side derivation (IMAP UIDVALIDITY +
///   HIGHESTMODSEQ + UID-list digest in basic mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CursorFreshness {
    /// The account does not expose a cursor or changes primitive.
    None,
    ServerIssued,
    Hybrid,
}

/// Whether the protocol supports byte-range blob fetches.
///
/// `Conditional` means support varies per handle: Graph file attachments
/// support range, item attachments do not; the per-handle truth lives on
/// `BlobCapabilities::supports_range`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlobRangeSupport {
    Yes,
    No,
    Conditional,
}

/// Push surface model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushCapability {
    /// JMAP WebSocket / SSE, IMAP IDLE/NOTIFY, EWS streaming
    /// notifications. Notifications ride the account's `Connection`
    /// push lanes (`Account::push_stream` sessions).
    InProcess,
    /// Gmail Pub/Sub. Subscription CRUD lives on the Account; the
    /// listener is wired by the consumer and feeds the engine's
    /// `InvalidationSink`. The account's `Registration` lane only
    /// maintains the watch.
    OutOfProcessPubsub,
    /// Microsoft Graph `/subscriptions` webhooks. Subscription CRUD
    /// lives on the Account; the receiver is wired by the consumer
    /// and feeds the `InvalidationSink`. The account's `Registration`
    /// lane only maintains the rows.
    Webhook,
    /// Protocol exposes no push primitive; engine falls back to
    /// poll-only scheduling.
    None,
}

/// How mutation concurrency is enforced on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MutationConcurrency {
    /// Legacy provider-observed state guards, without a caller-version guarantee.
    // Retained pending the owner decision on provider-observed state guards.
    StateBased,
    /// Caller versions are enforced for the named doors. Other doors
    /// have no caller-version guarantee. DAV relocation with a version is
    /// refused because its multi-request fallback cannot preserve the guard.
    ExpectedVersion(VersionedDoors),
}

/// Doors that enforce a caller-supplied version with a native atomic guard.
/// The empty set is the sole representation of no caller-version support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VersionedDoors(u8);

impl VersionedDoors {
    pub const NONE: Self = Self(0);
    /// Mail flag writes. IMAP refuses a versioned two-sided patch.
    pub const MAIL_SET_FLAGS: Self = Self(1);
    pub const MAIL_MOVE: Self = Self(1 << 1);
    pub const MAIL_DESTROY: Self = Self(1 << 2);
    pub const CONTACT_UPDATE: Self = Self(1 << 3);
    pub const EVENT_UPDATE: Self = Self(1 << 4);
    pub const CONTACT_DELETE: Self = Self(1 << 5);
    pub const EVENT_DELETE: Self = Self(1 << 6);
    pub const EVENT_RSVP: Self = Self(1 << 7);

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Include a door when its negotiated prerequisite is present.
    #[must_use]
    pub const fn with(self, doors: Self, enabled: bool) -> Self {
        if enabled { self.union(doors) } else { self }
    }

    #[must_use]
    pub const fn contains(self, doors: Self) -> bool {
        self.0 & doors.0 == doors.0
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl MutationConcurrency {
    /// Refuse a supplied caller version unless this operation advertises an
    /// atomic guard. No version leaves the ordinary operation unchanged.
    ///
    /// Admission does not implement a guard: providers must still validate
    /// the token kind and enforce it on the wire. Trait defaults that cannot
    /// enforce versions must continue to refuse even an advertised door.
    pub fn gate_version(
        self,
        operation: crate::AccountOperation,
        expected_version: Option<&crate::ServerVersion>,
        protocol: crate::Protocol,
    ) -> Result<(), crate::AccountError> {
        use crate::AccountOperation;
        let door = match operation {
            AccountOperation::UpdateFlags => VersionedDoors::MAIL_SET_FLAGS,
            AccountOperation::BulkMove => VersionedDoors::MAIL_MOVE,
            AccountOperation::BulkDestroy => VersionedDoors::MAIL_DESTROY,
            AccountOperation::ContactUpdate => VersionedDoors::CONTACT_UPDATE,
            AccountOperation::EventUpdate => VersionedDoors::EVENT_UPDATE,
            AccountOperation::ContactDelete => VersionedDoors::CONTACT_DELETE,
            AccountOperation::EventDelete => VersionedDoors::EVENT_DELETE,
            AccountOperation::EventRsvp => VersionedDoors::EVENT_RSVP,
            _ => VersionedDoors::NONE,
        };
        if expected_version.is_none() || (!door.is_empty() && self.versioned_doors().contains(door))
        {
            Ok(())
        } else {
            Err(crate::AccountError::unsupported(operation, protocol))
        }
    }

    /// Legacy state guards do not promise any caller-version door.
    #[must_use]
    pub const fn versioned_doors(self) -> VersionedDoors {
        match self {
            Self::ExpectedVersion(doors) => doors,
            Self::StateBased => VersionedDoors::NONE,
        }
    }
}

/// Client-mintable wire dedup token availability.
///
/// `ReplayToken` is reserved for a future protocol that documents a
/// real dedup primitive; no protocol today qualifies. Every current
/// Account impl declares `None`. See `MutationReassertion` for desired-state
/// reassertion after observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MutationReplaySafety {
    ReplayToken,
    None,
}

/// Which unversioned bulk mail doors this account declares safe to reapply,
/// after a valid observation shows the target unsatisfied, to the same
/// object. A declaration requires stable, never-reused object identity and a
/// door whose reapplication converges on the requested state without a
/// second, different side effect. It is NOT server deduplication and gives no
/// lost-update guarantee; it authorizes desired-state reassertion only. Moves
/// have no field: the move door is never reasserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MutationReassertion {
    /// `bulk_set_flags` may be reapplied to an unversioned target.
    pub set_flags: bool,
    /// `bulk_destroy` may be reapplied to an unversioned target.
    pub destroy: bool,
}

impl MutationReassertion {
    /// No door is declared reassertable; the default.
    pub const NONE: Self = Self {
        set_flags: false,
        destroy: false,
    };
}

/// Independent declarations for mutation concurrency, wire deduplication,
/// and desired-state reassertion after observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationCapabilities {
    pub concurrency: MutationConcurrency,
    pub replay_safety: MutationReplaySafety,
    pub reassertion: MutationReassertion,
}

/// How the protocol crate's bulk methods batch their input stream.
///
/// Per-protocol defaults: JMAP `{ 500, 100ms }`, Gmail
/// `{ 1000, 200ms }`, Graph `{ 20, 100ms }`, IMAP `{ 5000, 50ms }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchingPolicy {
    pub max_items: usize,
    pub max_wait: Duration,
    pub flush_on_input_close: bool,
}

/// Coarse rate-limit posture. Used by `bifrost-net` and the engine to
/// pick a default request-rate budget for the account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateLimitClass {
    /// No documented rate ceiling, or one so high it never matters in
    /// practice (well-run JMAP self-hosting).
    Generous,
    /// Public cloud tiers with documented quotas (Gmail's
    /// quota-units-per-second tier, Graph's mailbox concurrency tier).
    Tiered,
    /// IMAP per-connection serialization; not rate-limited but
    /// command-serialized.
    PerConnection,
}

/// Quota-shape signal the engine reacts to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QuotaSignal {
    /// Server returns explicit `Retry-After` on 429.
    RetryAfter,
    /// Server emits a quota counter (Gmail quota units in response
    /// headers).
    QuotaUnits,
    /// No explicit signal; engine backs off on observed latency
    /// increase.
    Implicit,
    /// No quota whatsoever (self-hosted IMAP without throttle policy).
    None,
}

// Keep the published snapshot fields and operation-to-flag gates in one
// declaration. Send-as and directory enumeration have no operation mapping:
// an Outlook user can send as a delegate even without a personal send path.
macro_rules! pim_method_support {
    ($(#[$attr:meta])* pub struct $name:ident {
        $($(#[$field_attr:meta])* pub $field:ident: bool => [$($operation:ident),*],)*
    }) => {
        $(#[$attr])*
        pub struct $name {
            $($(#[$field_attr])* pub $field: bool,)*
        }

        impl $name {
            /// Admit an advertised PIM operation or return its canonical refusal.
            ///
            /// `Send` checks the ordinary sending path only. Send-as, scheduled
            /// request fields and empty-query directory enumeration have separate
            /// prerequisites; callers must check them at their request boundary.
            /// Operations without a PIM method flag are refused by this helper.
            /// A successful gate does not replace provider validation or I/O.
            pub fn gate(
                &self,
                operation: crate::AccountOperation,
                protocol: crate::Protocol,
            ) -> Result<(), crate::AccountError> {
                let supported = match operation {
                    $($(crate::AccountOperation::$operation => self.$field,)*)*
                    _ => false,
                };
                if supported {
                    Ok(())
                } else {
                    Err(crate::AccountError::unsupported(operation, protocol))
                }
            }
        }
    };
}

pim_method_support! {
/// Per-method dispatch advertisement for the PIM trait surface.
///
/// Each field describes a primitive, convenience or request path on `Account`.
/// `true` means the implementation handles the method instead of
/// always returning `Err(Error::Unsupported)`. It does not promise a
/// lossless provider model, native server-side filtering, or support
/// for every optional field that can appear on that provider's wire
/// object. `false` means the method is unsupported and should
/// short-circuit. The convenience layer's default impls inspect this
/// flag set to decide whether to dispatch into a primitive or return
/// unsupported. New flags are added with `false` defaults so growing
/// the trait surface stays additive.
///
/// The declaration below generates the fields and their canonical local
/// refusal gates together. Provider implementations and their negotiated
/// prerequisites still need contract tests: a gate cannot prove that a
/// provider implements a method. Consumers must consult the snapshot AND
/// handle `Unsupported` regardless. The snapshot is also incomplete -
/// `send_raw_message`, `repair_inventory`, `bulk_move_from` and
/// `open_blob_range` have documented gating with no flag here, or a flag on a
/// different struct.
///
/// What holds it honest is `capability_contract_tests.rs` in each protocol
/// crate, which drives every gated entry point and asserts the flag agrees.
/// That coverage is false-direction only: a `false` flag is proven to refuse
/// without wire contact, while a `true` flag reaching the network is not
/// asserted, since proving it would need a live seam each crate lacks.
///
/// Ruled 2026-10-03: the struct stays. The rejected alternative replaced it
/// with a runtime `supports(&self, op: AccountOperation)` query defaulted from
/// a per-impl operation set, so the capability answer and the error answer
/// would be one value read twice and a new trait method would default to
/// unsupported. That deletes a published struct, and the contract tests
/// already catch the drift it targets. Re-raise only if those tests stop
/// being able to catch it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PimMethodSupport {
    /// Read Outlook master category definitions.
    pub category_definitions: bool => [CategoryDefinitionsList],
    /// Read Exchange-native message reaction properties.
    pub message_reactions: bool => [MessageReactionsRead],
    // Mail mutation primitives.
    pub add_to_container: bool => [AddToContainer],
    pub remove_from_container: bool => [RemoveFromContainer],
    pub set_keyword: bool => [SetKeyword],
    pub set_label_membership: bool => [SetLabelMembership],
    pub set_category: bool => [SetCategory],
    pub set_extended_property: bool => [SetExtendedProperty],
    /// Native message-importance overwrite (`set_importance`). Graph
    /// `PATCH { importance }`, JMAP/IMAP `$important` keyword. `false`
    /// on Gmail (no importance field) and the DAV crates -> the
    /// primitive returns `Unsupported(SetImportance)`.
    pub set_importance: bool => [SetImportance],
    pub set_is_read: bool => [SetIsRead],
    // Mail composition primitives.
    /// Ordinary sending from the account's own identity: `send_message` with
    /// `SendRequest::send_as` left `None`. `false` -> such a request returns
    /// `Unsupported(Send)`.
    ///
    /// This flag describes one PATH of the `send_message` method, not the whole
    /// method. Send-as is a field on the same request, with prerequisites of its
    /// own, and whether a `Some(send_as)` request can succeed is `send_as`'s
    /// answer - which may be yes while this is `false`. A JMAP session whose
    /// primary account lacks Submission while a seeded foreign account has it
    /// is exactly that: ordinary sends are refused, routed sends work.
    pub send_message: bool => [Send],
    pub attachment_upload: bool => [AttachmentUpload],
    /// Native large-attachment hosting + share-link generation (Google
    /// Drive, Microsoft OneDrive). Gates `host_attachment`. `true` only on
    /// the Gmail and Graph accounts; `false` -> `host_attachment` returns
    /// `Unsupported(HostAttachment)` (JMAP, IMAP, CalDAV, CardDAV). This
    /// flag is the bifrost replacement for ratatoskr's consumer-side
    /// `supports_cloud_upload(provider) = matches!(Graph | Gmail)` (closes
    /// the A8 provider-identity leak A-1).
    pub host_attachment: bool => [HostAttachment],
    pub draft_create: bool => [DraftCreate],
    pub draft_update: bool => [DraftUpdate],
    pub draft_discard: bool => [DraftDiscard],
    pub draft_send: bool => [DraftSend],
    /// Native server-side scheduled send (JMAP `maxDelayedSend > 0`,
    /// Graph `PidTagDeferredSendTime`, IMAP relay FUTURERELEASE). Gates
    /// `SendRequest::scheduled`, `cancel_scheduled_send`, and
    /// `reschedule_send`. `false` -> a scheduled request is rejected
    /// `Unsupported(Send)` and the two primitives return `Unsupported`.
    pub scheduled_send: bool => [CancelScheduledSend, RescheduleSend],
    /// Native send-as / send-on-behalf-of routing for a shared or
    /// delegate mailbox (Microsoft Graph: route the draft-create-and-send
    /// through the shared mailbox's `/users/{id}` client and stamp the
    /// `from`/`sender` fields; JMAP routes through a foreign submission-
    /// capable account). Gates `SendRequest::send_as`.
    ///
    /// Derived from the account's mailbox routing table, not from the
    /// provider: `true` only where that table is non-empty, so a Graph
    /// account with no shared mailboxes configured and a JMAP account with
    /// no seeded foreign submission account both report `false`. A revoked
    /// share must not leave an advertised but unreachable send path.
    ///
    /// The flag selects which rejection a `Some(send_as)` request that
    /// cannot be honored gets: `false` -> `Unsupported(Send)`, the feature
    /// is absent; `true` -> `Request(Malformed)` naming
    /// `send_as.mailbox`, the feature is present and the mailbox is wrong.
    /// Both are terminal, and neither is ever silently downgraded to a send
    /// from the authenticated user's own mailbox. `SendRequest::send_as`
    /// carries the full rule.
    ///
    /// Independent of `send_message`, which covers only the ordinary path:
    /// this flag can be `true` while that one is `false`.
    pub send_as: bool => [],
    // Search primitives.
    pub search: bool => [Search],
    pub search_messages: bool => [SearchMessages],
    // Container CRUD primitives.
    pub containers_list: bool => [ContainersList],
    pub container_create: bool => [ContainerCreate],
    pub container_rename: bool => [ContainerRename],
    pub container_move: bool => [ContainerMove],
    pub container_delete: bool => [ContainerDelete],
    // Settings primitives.
    pub identities_list: bool => [IdentitiesList],
    pub identity_update: bool => [IdentityUpdate],
    pub vacation_get: bool => [VacationGet],
    pub vacation_set: bool => [VacationSet],
    pub quota_get: bool => [QuotaGet],
    // Hydration primitives.
    pub thread_hydrate: bool => [HydrateThread],
    pub message_hydrate: bool => [HydrateMessage],
    /// Native server-assembled whole-message RFC822 read
    /// (`open_raw_rfc822`). IMAP `BODY.PEEK[]`, Gmail `format=raw`,
    /// JMAP whole-message `blobId` + `download`, Graph
    /// `GET /messages/{id}/$value`. Gates the consumer's "show raw
    /// source" / body store / attachment-dedup paths. `false` -> the
    /// method terminates `Unsupported(OpenRawRfc822)` (the two DAV
    /// crates).
    pub open_raw_rfc822: bool => [OpenRawRfc822],
    // Server-side filter primitives.
    pub filters_list: bool => [FiltersList],
    pub filter_create: bool => [FilterCreate],
    pub filter_update: bool => [FilterUpdate],
    pub filter_delete: bool => [FilterDelete],
    pub filter_validate: bool => [FilterValidate],
    // Contact primitives and conveniences.
    pub address_books_list: bool => [AddressBooksList],
    pub contacts_list: bool => [ContactsList],
    pub contact_get: bool => [ContactGet],
    pub contact_create: bool => [ContactCreate],
    pub contact_update: bool => [ContactUpdate],
    pub contact_delete: bool => [ContactDelete],
    pub contact_search: bool => [ContactSearch],
    pub contact_autocomplete: bool => [ContactAutocomplete],
    /// Provider-side directory lookup with a non-empty query. Independent of
    /// enumeration support. CardDAV requires an admitted directory gateway.
    pub directory_search: bool => [DirectorySearch],
    /// Empty-query directory enumeration, via `directory_search`.
    /// A provider without this flag returns classified Unsupported for an
    /// empty query, even when non-empty lookups are supported.
    pub directory_enumerate: bool => [],
    /// Mail-enabled directory-group listing (`directory_groups_list`):
    /// the org-directory groups the authenticated mailbox belongs to
    /// (Microsoft Graph `/me/memberOf` filtered to mail-enabled). `true`
    /// only on Graph; `false` -> `Unsupported(DirectoryGroupsList)`.
    /// Support is protocol-level: a tenant that has not granted
    /// directory-group read consent still fails at call time with a
    /// `NoPermission` error - consent is per-tenant runtime state a
    /// static flag cannot know.
    pub directory_groups_list: bool => [DirectoryGroupsList],
    /// Transitive directory-group member expansion
    /// (`directory_group_expand`): resolve a `DirectoryGroupId` to its
    /// user members, nested groups flattened provider-side (Microsoft
    /// Graph `/groups/{id}/transitiveMembers`). `true` only on Graph;
    /// `false` -> `Unsupported(DirectoryGroupExpand)`. Same per-tenant
    /// consent caveat as `directory_groups_list`.
    pub directory_group_expand: bool => [DirectoryGroupExpand],
    // Calendar primitives and conveniences.
    pub calendars_list: bool => [CalendarsList],
    pub events_in_range: bool => [EventsInRange],
    pub event_get: bool => [EventGet],
    pub event_create: bool => [EventCreate],
    pub event_update: bool => [EventUpdate],
    pub event_delete: bool => [EventDelete],
    pub event_rsvp: bool => [EventRsvp],
    pub event_search: bool => [EventSearch],
    pub event_autocomplete: bool => [EventAutocomplete],
}
}

/// Mapping from each provider's flag namespace onto the canonical
/// keyword the convenience layer expects.
///
/// The trait surface speaks one keyword shape - `$flagged`,
/// `$answered`, `$forwarded`, `$seen` - but each protocol natively
/// stores a different name (IMAP `\Flagged`, Graph
/// `flag.flagStatus`, Gmail STARRED, JMAP `$flagged`). Account impls
/// fill the relevant variant when they advertise support for the
/// `set_starred` / `mark_replied` / `mark_forwarded` conveniences;
/// the convenience layer reads this to choose the dispatch target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StarredFlagShape {
    /// `set_starred` flips an IMAP-style keyword (`\Flagged`).
    Keyword,
    /// `set_starred` flips a Gmail-style label membership
    /// (STARRED). Convenience layer dispatches to
    /// `set_label_membership`.
    LabelMembership,
    /// `set_starred` flips a Graph-style category / flag status.
    /// Convenience layer dispatches to `set_category` or
    /// `set_extended_property`.
    Category,
    /// Provider does not have a starred-equivalent primitive;
    /// convenience returns `Err(Unsupported)`.
    None,
}

/// Primitive used to persist replied state. Gmail derives it during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepliedFlagShape {
    /// Set `$answered` via `set_keyword`.
    Keyword,
    /// Set Graph PR_LAST_VERB_EXECUTED via `set_extended_property`.
    ExtendedProperty,
    None,
}

/// Primitive used to persist forwarded state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ForwardedFlagShape {
    /// Set `$forwarded` via `set_keyword`.
    Keyword,
    /// Set Graph PR_LAST_VERB_EXECUTED via `set_extended_property`.
    ExtendedProperty,
    None,
}

/// Per-convenience dispatch hints. The convenience default impls
/// read these to pick the right primitive without doing a runtime
/// `match` on protocol kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConvenienceShape {
    pub starred: StarredFlagShape,
    pub replied: RepliedFlagShape,
    pub forwarded: ForwardedFlagShape,
    /// True iff `mark_mdn_sent` should flip the `$MDNSent` keyword via
    /// `set_keyword`. `true` on JMAP and IMAP. `false` on Gmail and Graph:
    /// their read-receipt model (Graph `isReadReceiptRequested`) is
    /// read-only, so the convenience returns `Unsupported(MarkMdnSent)`.
    pub mdn_sent_via_keyword: bool,
}

impl Default for ConvenienceShape {
    fn default() -> Self {
        Self {
            starred: StarredFlagShape::None,
            replied: RepliedFlagShape::None,
            forwarded: ForwardedFlagShape::None,
            mdn_sent_via_keyword: false,
        }
    }
}

/// Snapshot of an account's behavior read at open-time.
#[derive(Debug, Clone)]
pub struct AccountCapabilities {
    pub cursor_freshness: CursorFreshness,
    pub blob_range: BlobRangeSupport,
    pub blob_digest_pre_download: bool,
    pub push: PushCapability,
    pub mutation: MutationCapabilities,
    pub batching_policy: BatchingPolicy,
    pub rate_limit_class: RateLimitClass,
    pub quota_signal: QuotaSignal,
    /// IMAP: the consumer must re-check UIDVALIDITY on every
    /// folder open.
    pub requires_uidvalidity_recheck: bool,
    /// Gmail says historyIds are *typically* valid for at least a
    /// week but can age out in hours. `None` because a deterministic
    /// value misleads scheduling.
    pub historyid_expires_after: Option<Duration>,
    /// Graph delta tokens have no fixed lifetime per Microsoft docs;
    /// expiry is an event, not a budget.
    pub delta_token_expires_after: Option<Duration>,
    /// Per-method support flags. The convenience layer and ratatoskr
    /// both read this to disable UI affordances per account.
    pub pim_methods: PimMethodSupport,
    /// Server-side filter model this account exposes.
    pub filter_rule_shape: FilterRuleShape,
    /// Dispatch hints for convenience default impls. Lets the
    /// convenience layer pick the right primitive without matching
    /// on `ProtocolKind`.
    pub conveniences: ConvenienceShape,
    /// True iff rediscovering the account (a full staged reattach) can
    /// surface foreign (other-user / shared) namespaces not visible at the
    /// last open. This is discovery POTENTIAL, not current membership:
    /// an account with zero shares today still reports `true` when a
    /// grant made after open would surface at the next open. Three
    /// conditions gate it:
    ///
    /// - the account discovers its foreign surface from the wire at
    ///   open (IMAP NAMESPACE walk, the JMAP session's non-personal
    ///   accounts), and
    /// - it emits no foreign scope-lifecycle events mid-session, so a
    ///   rediscovery is the only door a new grant can walk through, and
    /// - no provider signal rules foreign namespaces out.
    ///
    /// IMAP has such a signal and refines with it: a personal-only
    /// NAMESPACE response means a grant can never surface, so the flag
    /// is `false` there and a rediscovery reattach is pure waste. JMAP
    /// has no "never" signal - any RFC 8620 session can list a foreign
    /// account tomorrow (RFC 9670 principals support is evidence for
    /// sharing, but its absence proves nothing) - so JMAP is
    /// constitutively `true`. `false` on providers whose foreign
    /// surfaces are configuration-driven rather than discovered while
    /// opening the account (Graph delegates / public-folder pins,
    /// Gmail delegation behind separate authorization, DAV home sets).
    ///
    /// Consumers act on `true` by scheduling a full reattach
    /// (`bifrost-sync` exposes `SyncEngine::reattach`, whose staged
    /// rediscovery establishes newly-appeared scopes); the cadence is
    /// consumer policy, weighed against the reattach's wire cost.
    pub discovers_foreign_namespaces_on_rediscovery: bool,
}

impl AccountCapabilities {
    /// True iff push sessions carry the notifications themselves
    /// (IMAP IDLE / NOTIFY, JMAP WebSocket, EWS streaming). False for
    /// out-of-process push paths (Gmail Pub/Sub, Graph webhooks),
    /// where `Invalidated` arrives via the engine's
    /// `InvalidationSink` and the account's push session only
    /// maintains the server registration.
    ///
    /// Derived from `push` so the two encodings cannot drift.
    #[must_use]
    pub fn push_in_process(&self) -> bool {
        matches!(self.push, PushCapability::InProcess)
    }
}

/// Capability-key surface used to describe changes in
/// `CapabilityDelta`. Opaque newtype around a String so the engine and
/// observability layers can name capabilities without coupling to a
/// specific enum.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct CapabilityKey(pub String);

/// A capability's stringified value at some point in time. Used for
/// both the "before" and "after" sides of a transition; the position
/// in `CapabilityChange` distinguishes them. (The prior draft had
/// separate `OldValue` / `NewValue` newtypes wrapping the same
/// `String` shape, which was redundant.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CapabilityValue(pub String);

/// One named capability transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CapabilityChange {
    pub key: CapabilityKey,
    pub from: CapabilityValue,
    pub to: CapabilityValue,
}

/// Delta between two `AccountCapabilities` snapshots. Carried as structured
/// cause evidence on a capability-change error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CapabilityDelta {
    pub added: Vec<CapabilityKey>,
    pub removed: Vec<CapabilityKey>,
    pub changed: Vec<CapabilityChange>,
}

impl CapabilityDelta {
    /// Compare the complete negotiated snapshots. Fixed snapshot fields are
    /// always present, so transitions populate `changed`, not added/removed.
    #[must_use]
    pub fn between(before: &AccountCapabilities, after: &AccountCapabilities) -> Self {
        fn values(snapshot: &AccountCapabilities) -> Vec<(CapabilityKey, CapabilityValue)> {
            let AccountCapabilities {
                cursor_freshness,
                blob_range,
                blob_digest_pre_download,
                push,
                mutation,
                batching_policy,
                rate_limit_class,
                quota_signal,
                requires_uidvalidity_recheck,
                historyid_expires_after,
                delta_token_expires_after,
                pim_methods,
                filter_rule_shape,
                conveniences,
                discovers_foreign_namespaces_on_rediscovery,
            } = snapshot;
            vec![
                (
                    CapabilityKey("cursor_freshness".into()),
                    CapabilityValue(format!("{cursor_freshness:?}")),
                ),
                (
                    CapabilityKey("blob_range".into()),
                    CapabilityValue(format!("{blob_range:?}")),
                ),
                (
                    CapabilityKey("blob_digest_pre_download".into()),
                    CapabilityValue(format!("{blob_digest_pre_download:?}")),
                ),
                (
                    CapabilityKey("push".into()),
                    CapabilityValue(format!("{push:?}")),
                ),
                (
                    CapabilityKey("mutation".into()),
                    CapabilityValue(format!("{mutation:?}")),
                ),
                (
                    CapabilityKey("batching_policy".into()),
                    CapabilityValue(format!("{batching_policy:?}")),
                ),
                (
                    CapabilityKey("rate_limit_class".into()),
                    CapabilityValue(format!("{rate_limit_class:?}")),
                ),
                (
                    CapabilityKey("quota_signal".into()),
                    CapabilityValue(format!("{quota_signal:?}")),
                ),
                (
                    CapabilityKey("requires_uidvalidity_recheck".into()),
                    CapabilityValue(format!("{requires_uidvalidity_recheck:?}")),
                ),
                (
                    CapabilityKey("historyid_expires_after".into()),
                    CapabilityValue(format!("{historyid_expires_after:?}")),
                ),
                (
                    CapabilityKey("delta_token_expires_after".into()),
                    CapabilityValue(format!("{delta_token_expires_after:?}")),
                ),
                (
                    CapabilityKey("pim_methods".into()),
                    CapabilityValue(format!("{pim_methods:?}")),
                ),
                (
                    CapabilityKey("filter_rule_shape".into()),
                    CapabilityValue(format!("{filter_rule_shape:?}")),
                ),
                (
                    CapabilityKey("conveniences".into()),
                    CapabilityValue(format!("{conveniences:?}")),
                ),
                (
                    CapabilityKey("discovers_foreign_namespaces_on_rediscovery".into()),
                    CapabilityValue(format!("{discovers_foreign_namespaces_on_rediscovery:?}")),
                ),
            ]
        }
        let changed = values(before)
            .into_iter()
            .zip(values(after))
            .filter_map(|((key, from), (_, to))| {
                (from != to).then_some(CapabilityChange { key, from, to })
            })
            .collect();
        Self {
            changed,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

#[cfg(test)]
mod gate_tests {
    use super::*;
    use crate::{AccountErrorKind, AccountOperation, Protocol, ServerVersion};

    #[test]
    fn pim_gate_reads_the_specific_flag_and_preserves_refusal_identity() {
        let caps = PimMethodSupport {
            search: true,
            ..PimMethodSupport::default()
        };
        assert!(caps.gate(AccountOperation::Search, Protocol::Imap).is_ok());
        let error = caps
            .gate(AccountOperation::SearchMessages, Protocol::Imap)
            .expect_err("thread search does not enable message search");
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::SearchMessages)
        );
        assert_eq!(error.operation(), Some(AccountOperation::SearchMessages));
        assert_eq!(error.protocol(), Some(Protocol::Imap));
        assert!(error.recovery().is_terminal());
    }

    #[test]
    fn pim_gate_keeps_request_fields_separate_from_method_support() {
        let caps = PimMethodSupport {
            send_as: true,
            scheduled_send: true,
            directory_enumerate: true,
            ..PimMethodSupport::default()
        };
        for operation in [
            AccountOperation::CancelScheduledSend,
            AccountOperation::RescheduleSend,
        ] {
            assert!(caps.gate(operation, Protocol::Jmap).is_ok());
        }
        for operation in [
            AccountOperation::Send,
            AccountOperation::DirectorySearch,
            AccountOperation::Close,
        ] {
            assert!(caps.gate(operation, Protocol::Jmap).is_err());
        }
    }

    #[test]
    fn version_gate_requires_the_operations_own_door_only_for_supplied_versions() {
        let version = ServerVersion::ETag("caller-owned".into());
        let operations = [
            (
                AccountOperation::UpdateFlags,
                VersionedDoors::MAIL_SET_FLAGS,
            ),
            (AccountOperation::BulkMove, VersionedDoors::MAIL_MOVE),
            (AccountOperation::BulkDestroy, VersionedDoors::MAIL_DESTROY),
            (
                AccountOperation::ContactUpdate,
                VersionedDoors::CONTACT_UPDATE,
            ),
            (AccountOperation::EventUpdate, VersionedDoors::EVENT_UPDATE),
            (
                AccountOperation::ContactDelete,
                VersionedDoors::CONTACT_DELETE,
            ),
            (AccountOperation::EventDelete, VersionedDoors::EVENT_DELETE),
            (AccountOperation::EventRsvp, VersionedDoors::EVENT_RSVP),
        ];
        for (enabled, door) in operations {
            let concurrency = MutationConcurrency::ExpectedVersion(door);
            for (operation, _) in operations {
                assert!(
                    concurrency
                        .gate_version(operation, None, Protocol::Graph)
                        .is_ok()
                );
                let result = concurrency.gate_version(operation, Some(&version), Protocol::Graph);
                if operation == enabled {
                    assert!(result.is_ok());
                } else {
                    let error = result.expect_err("a different door cannot enforce this version");
                    assert_eq!(error.kind(), &AccountErrorKind::Unsupported(operation));
                    assert_eq!(error.operation(), Some(operation));
                    assert_eq!(error.protocol(), Some(Protocol::Graph));
                }
            }
        }
        for concurrency in [
            MutationConcurrency::StateBased,
            MutationConcurrency::ExpectedVersion(VersionedDoors::NONE),
        ] {
            assert!(
                concurrency
                    .gate_version(
                        AccountOperation::UpdateFlags,
                        Some(&version),
                        Protocol::Graph
                    )
                    .is_err()
            );
        }
        assert!(
            MutationConcurrency::ExpectedVersion(VersionedDoors::MAIL_SET_FLAGS)
                .gate_version(AccountOperation::Send, Some(&version), Protocol::Graph)
                .is_err()
        );
    }
}
