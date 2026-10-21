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
    /// notifications. Push events flow on the in-process
    /// `push_stream`.
    InProcess,
    /// Gmail Pub/Sub. Subscription CRUD lives on the Account; the
    /// listener is wired by the consumer and feeds the engine's
    /// `InvalidationSink`.
    OutOfProcessPubsub,
    /// Microsoft Graph `/subscriptions` webhooks. Subscription CRUD
    /// lives on the Account; the receiver is wired by the consumer
    /// and feeds the `InvalidationSink`.
    WebhookOrEwsStream,
    /// Protocol exposes no push primitive; engine falls back to
    /// poll-only scheduling.
    None,
}

/// How mutation concurrency is enforced on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MutationConcurrency {
    /// Legacy provider-observed state guards, without a caller-version guarantee.
    StateBased,
    /// Caller versions are enforced for the named doors. Other doors
    /// have no caller-version guarantee. DAV relocation with a version is
    /// refused because its multi-request fallback cannot preserve the guard.
    ExpectedVersion {
        /// Mail flag writes. IMAP refuses a versioned two-sided patch.
        mail_set_flags: bool,
        mail_move: bool,
        mail_destroy: bool,
        contact_update: bool,
        event_update: bool,
        contact_delete: bool,
        event_delete: bool,
        event_rsvp: bool,
    },
    /// No caller-version concurrency control. Supplied versions are refused.
    None,
}

/// Client-mintable wire dedup token availability.
///
/// `ReplayToken` is reserved for a future protocol that documents a
/// real dedup primitive; no protocol today qualifies. Every current
/// Account impl declares `None` and the engine reads back affected
/// items after retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MutationReplaySafety {
    ReplayToken,
    None,
}

/// Two distinct mutation concerns: lost-update prevention (concurrency)
/// and double-apply prevention (replay safety). Independent axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationCapabilities {
    pub concurrency: MutationConcurrency,
    pub replay_safety: MutationReplaySafety,
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

/// Per-method dispatch advertisement for the PIM trait surface.
///
/// Each field corresponds to a primitive or convenience on `Account`.
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
/// Structural weakness, known and accepted: this is a hand-maintained MIRROR
/// of the trait surface with no mechanical link to it, and consumers must
/// consult it AND handle `Unsupported` regardless. The mirror is also
/// incomplete - `send_raw_message`, `repair_inventory`, `bulk_move_from` and
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
    pub category_definitions: bool,
    /// Read Exchange-native message reaction properties.
    pub message_reactions: bool,
    // Mail mutation primitives.
    pub add_to_container: bool,
    pub remove_from_container: bool,
    pub set_keyword: bool,
    pub set_label_membership: bool,
    pub set_category: bool,
    pub set_extended_property: bool,
    /// Native message-importance overwrite (`set_importance`). Graph
    /// `PATCH { importance }`, JMAP/IMAP `$important` keyword. `false`
    /// on Gmail (no importance field) and the DAV crates -> the
    /// primitive returns `Unsupported(SetImportance)`.
    pub set_importance: bool,
    pub set_is_read: bool,
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
    pub send_message: bool,
    pub attachment_upload: bool,
    /// Native large-attachment hosting + share-link generation (Google
    /// Drive, Microsoft OneDrive). Gates `host_attachment`. `true` only on
    /// the Gmail and Graph accounts; `false` -> `host_attachment` returns
    /// `Unsupported(HostAttachment)` (JMAP, IMAP, CalDAV, CardDAV). This
    /// flag is the bifrost replacement for ratatoskr's consumer-side
    /// `supports_cloud_upload(provider) = matches!(Graph | Gmail)` (closes
    /// the A8 provider-identity leak A-1).
    pub host_attachment: bool,
    pub draft_create: bool,
    pub draft_update: bool,
    pub draft_discard: bool,
    pub draft_send: bool,
    /// Native server-side scheduled send (JMAP `maxDelayedSend > 0`,
    /// Graph `PidTagDeferredSendTime`, IMAP relay FUTURERELEASE). Gates
    /// `SendRequest::scheduled`, `cancel_scheduled_send`, and
    /// `reschedule_send`. `false` -> a scheduled request is rejected
    /// `Unsupported(Send)` and the two primitives return `Unsupported`.
    pub scheduled_send: bool,
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
    pub send_as: bool,
    // Search primitives.
    pub search: bool,
    pub search_messages: bool,
    // Container CRUD primitives.
    pub containers_list: bool,
    pub container_create: bool,
    pub container_rename: bool,
    pub container_move: bool,
    pub container_delete: bool,
    // Settings primitives.
    pub identities_list: bool,
    pub identity_update: bool,
    pub vacation_get: bool,
    pub vacation_set: bool,
    pub quota_get: bool,
    // Hydration primitives.
    pub thread_hydrate: bool,
    pub message_hydrate: bool,
    /// Native server-assembled whole-message RFC822 read
    /// (`open_raw_rfc822`). IMAP `BODY.PEEK[]`, Gmail `format=raw`,
    /// JMAP whole-message `blobId` + `download`, Graph
    /// `GET /messages/{id}/$value`. Gates the consumer's "show raw
    /// source" / body store / attachment-dedup paths. `false` -> the
    /// method terminates `Unsupported(OpenRawRfc822)` (the two DAV
    /// crates).
    pub open_raw_rfc822: bool,
    // Server-side filter primitives.
    pub filters_list: bool,
    pub filter_create: bool,
    pub filter_update: bool,
    pub filter_delete: bool,
    pub filter_validate: bool,
    // Contact primitives and conveniences.
    pub address_books_list: bool,
    pub contacts_list: bool,
    pub contact_get: bool,
    pub contact_create: bool,
    pub contact_update: bool,
    pub contact_delete: bool,
    pub contact_search: bool,
    pub contact_autocomplete: bool,
    /// Provider-side directory lookup with a non-empty query. Independent of
    /// enumeration support. CardDAV requires an admitted directory gateway.
    pub directory_search: bool,
    /// Empty-query directory enumeration, via `directory_search`.
    /// A provider without this flag returns classified Unsupported for an
    /// empty query, even when non-empty lookups are supported.
    pub directory_enumerate: bool,
    /// Mail-enabled directory-group listing (`directory_groups_list`):
    /// the org-directory groups the authenticated mailbox belongs to
    /// (Microsoft Graph `/me/memberOf` filtered to mail-enabled). `true`
    /// only on Graph; `false` -> `Unsupported(DirectoryGroupsList)`.
    /// Support is protocol-level: a tenant that has not granted
    /// directory-group read consent still fails at call time with a
    /// `NoPermission` error - consent is per-tenant runtime state a
    /// static flag cannot know.
    pub directory_groups_list: bool,
    /// Transitive directory-group member expansion
    /// (`directory_group_expand`): resolve a `DirectoryGroupId` to its
    /// user members, nested groups flattened provider-side (Microsoft
    /// Graph `/groups/{id}/transitiveMembers`). `true` only on Graph;
    /// `false` -> `Unsupported(DirectoryGroupExpand)`. Same per-tenant
    /// consent caveat as `directory_groups_list`.
    pub directory_group_expand: bool,
    // Calendar primitives and conveniences.
    pub calendars_list: bool,
    pub events_in_range: bool,
    pub event_get: bool,
    pub event_create: bool,
    pub event_update: bool,
    pub event_delete: bool,
    pub event_rsvp: bool,
    pub event_search: bool,
    pub event_autocomplete: bool,
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

/// Per-convenience dispatch hints. The convenience default impls
/// read these to pick the right primitive without doing a runtime
/// `match` on protocol kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConvenienceShape {
    pub starred: StarredFlagShape,
    /// True iff `mark_replied` should flip `$answered` via
    /// `set_keyword`. Gmail accounts set this `false` because the
    /// flag is derived on sync.
    pub replied_via_keyword: bool,
    /// True iff `mark_replied` should set Graph's
    /// PR_LAST_VERB_EXECUTED via `set_extended_property`.
    pub replied_via_extended_property: bool,
    /// Same shape for the `$forwarded` flag.
    pub forwarded_via_keyword: bool,
    pub forwarded_via_extended_property: bool,
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
            replied_via_keyword: false,
            replied_via_extended_property: false,
            forwarded_via_keyword: false,
            forwarded_via_extended_property: false,
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
    /// True iff `push_stream` delivers `Invalidated` events directly
    /// (IMAP IDLE / NOTIFY, JMAP WebSocket, EWS streaming
    /// notifications). False for out-of-process push paths (Gmail
    /// Pub/Sub, Graph webhooks), where `Invalidated` arrives via
    /// the engine's `InvalidationSink` and `push_stream` carries
    /// only optional subscription-health transitions.
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
