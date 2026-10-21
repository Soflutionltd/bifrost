//! The `Account` trait and the `AccountFactory` companion trait.
//!
//! The trait is dyn-safe by construction: all stream and future
//! returns are erased through `AccountStream<T>` / `AccountFuture<T>`,
//! the cursor state is a concrete tagged blob rather than an
//! associated type, and `close` takes `&self` so it composes with
//! `Arc<dyn Account>`. A compile-time `_dyn_safe` check in
//! `lib.rs` verifies this at every build.
//!
//! Two-tier surface:
//!
//! - Primitives (no default impl): wire-level operations every
//!   protocol crate implements. The Account-impl-side surface.
//! - Conveniences (default impl in terms of primitives): ratatoskr-
//!   shaped composites. Protocol crates override only when the
//!   default is wrong for that provider; consumers who disagree with
//!   ratatoskr's defaults use the primitives directly.

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::Future;
use futures::stream::Stream;

use crate::blob::{BlobHandle, ByteRange};
use crate::calendar::{
    Calendar, CalendarEvent, EventCreate, EventId, EventPatch, EventRange, EventSearchRequest,
    RsvpStatus,
};
use crate::capabilities::{AccountCapabilities, StarredFlagShape};
use crate::cloud::{CloudUploadMeta, HostedAttachment};
use crate::compose::{AttachmentHandle, DraftHandle, DraftPatch, IdentityId, SendRequest};
use crate::contact::{
    AddressBook, AddressBookId, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest,
};
use crate::container::{
    ContainerId, ContainerKind, ContainerList, ContainerStyle, Label, MutationTarget,
};
use crate::cursor::{
    ChangeCursor, CursorDescriptor, CursorEstablishment, CursorScope, MembershipScope,
    ScopeLifecycleEvent,
};
use crate::directory::{DirectoryCard, DirectoryGroup, DirectoryGroupId, DirectoryGroupMember};
use crate::error::{AccountError, AccountOperation, ItemOutcome, MutationSuccess};
use crate::events::{
    Change, InventoryEvent, InventoryPartition, InventoryPartitioning, Priority, SyncEvent,
    WatchEvent,
};
use crate::filter::{
    FilterValidation, ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch,
};
use crate::hydration::{HydrationProjection, Importance, Message, ThreadHydration};
use crate::ids::{AccountId, ObjectId, SubscriptionHandle, ThreadId};
use crate::mutation::{FlagOp, HydratedObject, IdempotencyKey, Projection};
use crate::page::{Page, SkippedScope};
use crate::repair::{InventoryRepairEvent, InventoryRepairOutcome, InventoryRepairRequest};
use crate::search::SearchRequest;
use crate::settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};

/// Result of creating a server-side push subscription.
///
/// Only scopes in `outcomes`' succeeded lane are covered by `handle`.
/// Failed scopes remain available to the consumer's polling path. The handle
/// is present exactly when at least one requested scope was accepted.
/// Constructors validate this relationship; consumers that mutate the public
/// fields must preserve it themselves.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PushSubscription {
    pub handle: Option<SubscriptionHandle>,
    pub outcomes: crate::error::BatchOutcome<CursorScope>,
}

/// A mismatch between a push subscription handle and its succeeded scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PushSubscriptionBuildError {
    /// A handle cannot cover an empty succeeded lane.
    #[error("a push handle requires at least one succeeded scope")]
    HandleWithoutSucceededScopes,
    /// Succeeded scopes must be covered by a subscription handle.
    #[error("succeeded push scopes require a subscription handle")]
    SucceededScopesWithoutHandle,
    /// The common constructor could not close its internally built outcome ledger.
    #[error("push subscription outcome ledger violated its accounting invariant")]
    OutcomeLedgerInvariant,
}

impl PushSubscription {
    /// Build a result only when the handle agrees with the succeeded lane.
    pub fn new(
        handle: Option<SubscriptionHandle>,
        outcomes: crate::error::BatchOutcome<CursorScope>,
    ) -> Result<Self, PushSubscriptionBuildError> {
        match (handle.is_some(), !outcomes.succeeded().is_empty()) {
            (true, false) => Err(PushSubscriptionBuildError::HandleWithoutSucceededScopes),
            (false, true) => Err(PushSubscriptionBuildError::SucceededScopesWithoutHandle),
            (false, false) => {
                // No accepted scope means no handle. An all-failed ledger is
                // still a valid value; Account implementations may promote
                // that whole-request condition to Err at their policy seam.
                Ok(Self { handle, outcomes })
            }
            (true, true) => Ok(Self { handle, outcomes }),
        }
    }

    /// Build the common result where one handle covers every requested scope.
    ///
    /// An empty scope list is rejected because it cannot have a succeeded
    /// scope for the handle to cover.
    pub fn all_succeeded(
        handle: SubscriptionHandle,
        scopes: &[CursorScope],
    ) -> Result<Self, PushSubscriptionBuildError> {
        let expected: Vec<_> = (0..scopes.len())
            .map(|index| crate::error::BatchItemId(index.to_string()))
            .collect();
        let mut builder = crate::error::BatchOutcomeBuilder::new();
        for (item, scope) in expected.iter().cloned().zip(scopes.iter().cloned()) {
            builder.push_succeeded(item, scope);
        }
        let outcomes = builder
            .finalize(&expected)
            .map_err(|_| PushSubscriptionBuildError::OutcomeLedgerInvariant)?;
        Self::new(Some(handle), outcomes)
    }
}

/// A provider category definition. Color is the protocol token, not a UI color.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CategoryDefinition {
    pub name: String,
    pub color: Option<String>,
}

/// Exchange-native reaction state for one message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageReactionState {
    pub id: ObjectId,
    pub owner_reaction: Option<String>,
    pub reactions_count: Option<i64>,
}

/// Erased streaming return type for `Account` methods.
///
/// Each Account method that streams returns a boxed `Stream`. The
/// erasure is required for `dyn Account` to be reachable.
pub type AccountStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

/// Erased async return type for `Account` methods.
///
/// `'static` because the returned future may outlive the Account
/// handle's local borrow.
pub type AccountFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// An inventory stream that refuses `operation` and ends.
///
/// Shared rather than reimplemented per crate: six protocol crates need the
/// same three lines, and six copies of a shared shape is how this workspace
/// accumulated seven CalDAV/CardDAV divergences.
///
/// The stream yields exactly one event, `Terminated`, and ends. It emits no
/// `Done`, and in particular no completeness claim.
///
/// It used to close with `Done(InventoryCompletion::complete(scope, None))`, on
/// the reasoning that a walk enumerating nothing leaves nothing unaccounted
/// for. That was wrong twice over. The claim is scoped to the whole of `scope`
/// while the refusal is usually about one PARTITION of it - the default
/// `inventory_partition_stream` routes every non-`Full` partition request
/// through here - so a refused `Uid` window discharged the coverage debt for
/// the entire scope, which is precisely the ledger lie `coverage.rs` exists to
/// prevent. And a stream carrying both `Terminated` and `Done` contradicts
/// `InventoryEvent`'s own contract, where `Terminated` means the walk cannot
/// continue at all: a consumer reading last-event-wins saw success, one reading
/// first-event-wins saw failure.
///
/// This compatibility entry point has no account from which to obtain a
/// protocol. Account implementations use `unsupported_inventory_stream_with_protocol`.
/// The scope is retained for compatibility; no completeness claim is made.
pub fn unsupported_inventory_stream(
    _scope: CursorScope,
    operation: AccountOperation,
) -> AccountStream<InventoryEvent> {
    unsupported_inventory_stream_with_protocol(_scope, operation, crate::Protocol::Unknown)
}

/// An inventory refusal stamped with the account's protocol.
pub fn unsupported_inventory_stream_with_protocol(
    _scope: CursorScope,
    operation: AccountOperation,
    protocol: crate::Protocol,
) -> AccountStream<InventoryEvent> {
    let error = AccountError::unsupported(operation, protocol);
    Box::pin(futures::stream::once(async move {
        InventoryEvent::Terminated(error)
    }))
}

/// The contract every protocol crate implements and the engine drives.
///
/// All implementations are `Send + Sync`. Construction is consumer-
/// side via an `AccountFactory`; the engine owns the open
/// `Arc<dyn Account>` and calls the factory on reopen.
pub trait Account: Send + Sync {
    /// Protocol used to stamp default-method refusals. Implementations should
    /// override this; `Unknown` preserves compatibility for existing accounts.
    fn protocol(&self) -> crate::Protocol {
        crate::Protocol::Unknown
    }

    /// Read-once snapshot of the account's capabilities. Mid-session
    /// transitions are signaled by ending streams with
    /// a capability-change error that causes an account reopen, never through
    /// a live channel here.
    fn capabilities(&self) -> &AccountCapabilities;

    /// List provider category definitions when the protocol supports them.
    fn category_definitions_list(
        &self,
    ) -> AccountFuture<Result<Vec<CategoryDefinition>, AccountError>> {
        let error =
            AccountError::unsupported(AccountOperation::CategoryDefinitionsList, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Read reaction state. Providers preserving per-item failures override this.
    fn message_reactions(
        &self,
        _ids: &[ObjectId],
    ) -> AccountFuture<Result<crate::BatchOutcome<MessageReactionState>, AccountError>> {
        let error =
            AccountError::unsupported(AccountOperation::MessageReactionsRead, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Apply the engine's current priority hint to this account.
    ///
    /// Implementations that own a `bifrost_net::AccountNet` should
    /// forward this to `AccountNet::set_priority`. Non-HTTP or
    /// priority-agnostic implementations may store it for their own
    /// scheduler.
    fn set_priority(&self, priority: Priority);

    /// Apply a payload-byte cap in bytes per second, independently per direction.
    ///
    /// Each direction has ONE budget shared by every connection, stream,
    /// request and protocol leg of the account, including composed sub-accounts
    /// (for example IMAP with SMTP submission, ManageSieve, CardDAV and CalDAV).
    /// The consumer must wire one `bifrost_net::BandwidthRegistry` into every
    /// `Net`, the engine and the IMAP factory for that sharing to hold.
    /// Implementations owning an `AccountNet` forward to its cap setter.
    ///
    /// This governs payload bytes above TLS, not wire rate. `None` is unlimited;
    /// `Some(0)` is a 1 B/s trickle. See `reference/net.md` for the smoothing
    /// bound and the allowance for outstanding admissions and lower buffering.
    fn set_bandwidth_cap(&self, bps: Option<u64>);

    /// The throttle keys `work` consumes, so the engine can hold it while a
    /// provider deadline recorded under any of them is pending.
    ///
    /// The engine calls this at every wait site before driving work - a scope
    /// drive, a mutation attempt, a read-back - and waits out the longest
    /// pending deadline across the returned keys plus the account-wide ones
    /// it always consults. Lookup is literal: a returned key is consulted
    /// directly, with no enrollment step, so the very first deadline any
    /// account records under a shared key reaches every account that
    /// declares it.
    ///
    /// The answer must follow the same routing decision the operation itself
    /// makes (a composed sub-account's scope answers for the sub-account), and
    /// must include every [`ThrottleBudget`](crate::ThrottleBudget) this
    /// implementation's errors stamp for the same work - a stamped budget no
    /// declaration names is a hold no wait site honours. `account` is the id
    /// the engine records under, passed in so an account-local key
    /// (`ThrottleKey::Mailbox`) matches the recorded one exactly.
    ///
    /// The default declares nothing: the work waits on account-wide holds
    /// only.
    fn throttle_keys(
        &self,
        _account: &AccountId,
        _work: crate::ThrottleWork<'_>,
    ) -> Vec<crate::ThrottleKey> {
        Vec::new()
    }

    /// Cursor introspection. Account-aware because cost depends on
    /// capability state, not the cursor in isolation.
    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor;

    /// Cursor-scope discovery: what scopes does the engine multiplex
    /// `changes_stream` over? Bounded; terminates with `Done`.
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>>;

    /// Membership-scope discovery: what containers can the consumer
    /// ask about (folder trees, labels, mailboxes, queries)? Bounded.
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>>;

    /// Ongoing scope lifecycle events (folder created, renamed, deleted).
    ///
    /// Yields `ScopeLifecycleEvent::Lifecycle(_)` for scope changes
    /// and `ScopeLifecycleEvent::Terminated(AccountError)` when the
    /// long-running poll cannot continue. The structured terminal
    /// signal lets the engine escalate auth-lost or schema breaks
    /// observed by the lifecycle poller; previously the protocol
    /// could only sleep-and-retry silently because the stream element
    /// type was a bare `ScopeLifecycle` with no terminal carrier.
    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent>;

    /// Initial cursor establishment. The engine calls this exactly
    /// once per `(account, scope)` pair before its first
    /// `changes_stream(cursor)` call. See `CursorEstablishment` for
    /// the two outcomes.
    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>>;

    /// Inventory: projection-only cold-start primitive. Also the
    /// cursor-establishment pass for scopes that returned
    /// `CursorEstablishment::EstablishViaInventory`; for those scopes
    /// the terminal `SyncEvent::Done` carries the established cursor
    /// in its checkpoint.
    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent>;

    /// Resume an in-progress inventory cursor that was consumer-acknowledged
    /// at a page boundary. `None` means this protocol has no resumable
    /// inventory state and the engine must use its normal establishment path.
    ///
    /// The `Some`/`None` answer is also how the engine ASKS whether a stored
    /// cursor is a mid-inventory position, so building the stream must be
    /// free of I/O and of observable side effects: the engine may construct
    /// one purely to classify the cursor and drop it without polling.
    fn inventory_resume_stream(
        &self,
        _cursor: ChangeCursor,
    ) -> Option<AccountStream<InventoryEvent>> {
        None
    }

    /// Whether `cursor` is an in-progress inventory position accepted by
    /// `inventory_resume_stream`. This method performs pure classification.
    ///
    /// It must accept EXACTLY what `inventory_resume_stream` accepts, and an
    /// implementor overriding one must override the other. The engine uses
    /// this to route a stored cursor to the deferred inventory worker, which
    /// then calls the hook: a cursor this accepts but the hook refuses leaves
    /// that worker reporting `NoCursor`, and the scope ends up with neither a
    /// live cursor nor a recovery path - where the same cursor reaching
    /// `changes_stream` would have been classified and restarted. Implement
    /// the condition once and have both entry points read it, rather than
    /// having two bodies agree by inspection.
    fn is_inventory_cursor(&self, cursor: &ChangeCursor) -> bool {
        self.inventory_resume_stream(cursor.clone()).is_some()
    }

    /// Which inventory partition shape this account can serve for
    /// `scope`. Implementations that do not override this keep the
    /// original full-pass behavior.
    fn inventory_partitioning(&self, _scope: &CursorScope) -> InventoryPartitioning {
        InventoryPartitioning::Full
    }

    /// Inventory for a single partition. The default implementation
    /// supports only `InventoryPartition::Full`; protocol crates that
    /// advertise a stronger `inventory_partitioning` must override
    /// this to honor the matching partition variants.
    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<InventoryEvent> {
        match partition {
            InventoryPartition::Full => self.inventory_stream(scope),
            _ => unsupported_inventory_stream_with_protocol(
                scope,
                AccountOperation::SyncInventory,
                self.protocol(),
            ),
        }
    }

    /// Work off inventory coverage debt with a provider-native re-read.
    ///
    /// Deliberately not `get_stream`, which hydrates known ids at a projection
    /// and cannot express region replay, completeness proof, or authoritative
    /// absence. Requests stream so the engine can backpressure a long repair
    /// pass, mirroring `get_stream`'s input.
    ///
    /// The contract, all of which the engine enforces:
    ///
    /// - EXACTLY ONE terminal outcome per accepted request, correlated by
    ///   `RepairAttemptId`. Two outcomes for one attempt, an unknown attempt,
    ///   or an outcome whose kind does not match its request are all rejected.
    /// - `Terminated` explains why the stream stopped; it does NOT stand in for
    ///   the missing outcomes. The engine converts still-outstanding requests
    ///   to a local deferral rather than recording a conclusion the account
    ///   never reached.
    /// - A recovered object must be READ AUTHORITATIVELY AND CURRENTLY, not
    ///   replayed out of an old snapshot. `RegionRecovery::DurableReplay` means
    ///   the region can be re-read independently of the walk that found it; it
    ///   does not mean the bytes it returns are current. An account whose
    ///   replay is historical must follow it with current reads and emit only
    ///   current representations.
    /// - `ObjectRecovered` carries the rebuilt `InventoryEntry` because
    ///   constructing it is the proof that the representation failure which
    ///   raised the obligation has healed. Only its id reaches the consumer:
    ///   the engine validates the entry, keeps the id, and discards the rest,
    ///   exactly as a successful inventory walk does.
    ///
    /// The default implementation refuses PER REQUEST rather than terminating
    /// the stream, so every attempt gets its correlated answer and the engine
    /// can classify unsupported repair as `OperatorBlocked` immediately instead
    /// of spending a transient retry budget on a capability that will never
    /// exist.
    fn repair_inventory(
        &self,
        requests: AccountStream<InventoryRepairRequest>,
    ) -> AccountStream<InventoryRepairEvent> {
        use futures::StreamExt;
        let protocol = self.protocol();
        Box::pin(requests.map(move |request| {
            let error = AccountError::unsupported(AccountOperation::SyncInventory, protocol);
            InventoryRepairEvent::Outcome(InventoryRepairOutcome::Deferred {
                attempt: request.attempt,
                error,
            })
        }))
    }

    /// Hydrate known ids at a chosen projection. Input ids are
    /// streamed so the engine can backpressure long fetch passes.
    /// Per-item outcomes flow through `ItemOutcome<HydratedObject>`:
    /// successful hydrations emit `Succeeded`, locally-invalid or
    /// remotely-rejected ids emit `Failed`, transport drops that
    /// leave the item state ambiguous emit `Uncertain`. This is the
    /// same lane shape every other streaming bulk surface uses.
    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>;

    /// Post-cursor diff. Yields `Change` (the sum of `ObjectChange`
    /// and `ScopeChange`).
    ///
    /// **The stream must tolerate being dropped while a poll is pending, and
    /// the account must remain usable afterwards.** This is not only a teardown
    /// concern: `bifrost-sync` cuts a pending poll whenever the account is
    /// asked to stop or pause, and it does so on a LIVE account that it then
    /// keeps driving, so the next `changes_stream` on the same account must
    /// succeed. Anything the abandoned stream borrowed - a pooled connection,
    /// an in-flight request identity, a permit - has to come back, and progress
    /// the provider made but never yielded has to be replayable from the last
    /// published cursor, because nothing durable advanced for an item the
    /// engine never saw. Losing an item that was never yielded is expected and
    /// fine; losing the ability to read it again is not.
    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>>;

    /// Server-side push subscription CRUD: create.
    ///
    /// **This answers per scope, not per request.** `PushSubscription` carries
    /// an optional handle plus a validated per-scope `BatchOutcome` over the
    /// three-lane model in `reference/error-model.md`, and its ids are
    /// submission positions. A single refused scope must NEVER fail its
    /// siblings, and that applies to every bail path - including ones that run
    /// before any translation or resolution step, which is where an
    /// all-or-nothing bail last survived a contract fix and kept the
    /// user-visible symptom alive on every path that mattered. A subscribe that
    /// accepts nothing returns no handle at all.
    ///
    /// `Err` is reserved for genuine whole-request faults: an empty scope list,
    /// no endpoint configured, nothing subscribable at all, or a create failure
    /// that was rolled back.
    ///
    /// Implementors differ legitimately in how much of the list they cover.
    /// caldav, carddav and the IMAP `StubAccount` refuse push outright; google
    /// (`users.watch` is per mailbox) and jmap (per-account PushSubscription)
    /// genuinely cover the whole requested list when they return `Ok`; imap
    /// partially fails by design, since without RFC 5465 NOTIFY it runs a
    /// bounded budget of dedicated IDLE sessions and reports the excess in the
    /// failed lane.
    ///
    /// The coupling that makes partial coverage safe lives in `bifrost-sync`:
    /// push coverage never suppresses polling. `Engine::subscribe_push` records
    /// only `outcomes.succeeded()`, and that registry is teardown bookkeeping
    /// with no scheduler path consulting it, so a refused scope stays polled
    /// rather than becoming invisible. Any future change that lets push
    /// coverage relax polling must first make the uncovered-scope lane
    /// explicit, or it silently reintroduces that hole.
    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<PushSubscription, AccountError>>;

    /// Server-side push subscription CRUD: destroy.
    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Event stream from the protocol crate to the engine. Contents
    /// depend on `push_in_process`:
    ///
    /// - In-process (IMAP IDLE, JMAP WebSocket, EWS streaming): full
    ///   `WatchEvent` traffic - `Invalidated` + `Disconnected` +
    ///   `Reconnected`.
    /// - Out-of-process (Gmail Pub/Sub, Graph webhooks):
    ///   `Invalidated` events flow through `InvalidationSink`, not
    ///   here. The stream may carry `Disconnected` / `Reconnected` on
    ///   sustained subscription-health failure, or yield nothing.
    fn push_stream(&self) -> AccountStream<WatchEvent>;

    /// Open a blob for streaming download.
    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>>;

    /// Open a byte range of a blob. Errors with
    /// `AccountErrorKind::Unsupported(OpenBlobRange)` where the blob's
    /// capability flag is false.
    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>>;

    /// Open a message's assembled RFC822 octets for streaming download.
    /// Yields the verbatim server-assembled MIME bytes (never
    /// re-encoded, never lossy-decoded). `Bytes`, never `String`: 8-bit
    /// and binary MIME parts must survive intact for the body store,
    /// attachment dedup hashes, and the raw-source viewer. Gated by
    /// `capabilities().pim_methods.open_raw_rfc822`; an account whose
    /// flag is false terminates with `Unsupported(OpenRawRfc822)`.
    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<Bytes>>;

    /// Bulk flag mutation. `targets` is a streaming input so
    /// engine-driven mutation pipelines backpressure cleanly.
    /// `op` carries both the operation kind AND the flag set per
    /// variant; a `FlagOp::Set(HashSet)` pins the target flag set
    /// directly.
    ///
    /// Returns per-item `ItemOutcome<MutationSuccess>` envelopes.
    /// Every pulled item produces exactly one outcome. Locally-invalid
    /// items emit `Failed` rather than poisoning the stream. Early
    /// termination via `SyncEvent::Terminated(AccountError)` covers
    /// only already-pulled items.
    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>;

    /// Bulk move. Targets are moved into `destination`.
    ///
    /// Same streaming invariants as `bulk_set_flags`.
    ///
    /// Destination-only. On a label-model provider that is not enough
    /// to express "and leave the label it came from" - see
    /// [`bulk_move_from`](Self::bulk_move_from), which is the surface
    /// to reach for when the consumer knows the source.
    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>;

    /// Bulk move out of a known `source` container.
    ///
    /// Same contract and streaming invariants as
    /// [`bulk_move`](Self::bulk_move), plus: on completion the targets
    /// are no longer members of `source`. `None` is exactly
    /// `bulk_move`.
    ///
    /// The point is request count, not semantics. On the folder-model
    /// providers a move already vacates the source as part of the move
    /// itself (IMAP `MOVE`, Graph `POST /messages/{id}/move`, JMAP
    /// replacing `mailboxIds`), so the default impl below - forward to
    /// `bulk_move`, ignore `source` - already satisfies the contract
    /// for them. On the label-model provider it does not: Gmail's
    /// `batchModify` has to be told to remove the source label, and
    /// without this surface a consumer has to compose `bulk_move` plus
    /// one `remove_from_container` PER ID, which is O(n) requests
    /// against precisely the provider whose bulk endpoint could express
    /// add-and-remove in a single call. Gmail therefore overrides.
    ///
    /// Deliberately not capability-gated: every impl satisfies the
    /// post-condition, so a consumer never has to branch on which
    /// provider it is talking to.
    fn bulk_move_from(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        source: Option<MembershipScope>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let _ = source;
        self.bulk_move(targets, destination, key)
    }

    /// Bulk destroy.
    ///
    /// Same streaming invariants as `bulk_set_flags`.
    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>;

    // ------------------------------------------------------------
    // Mail mutation primitives (S1-W1)
    //
    // Each maps to one wire-level operation. Account impls that do
    // not natively support a given primitive return
    // `Err(Error::Unsupported)` and clear the matching flag in
    // `capabilities().pim_methods`.
    // ------------------------------------------------------------

    /// Add `target` to `container`. JMAP `Email/set` mailboxIds add;
    /// IMAP `COPY`; Gmail `messages.modify` addLabels; Graph
    /// `POST /messages/{id}/move`.
    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Remove `target` from `container`. JMAP `Email/set` mailboxIds
    /// remove; IMAP `+FLAGS \Deleted` + `EXPUNGE`; Gmail
    /// `messages.modify` removeLabels; Graph has no symmetric op
    /// (move replaces source).
    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear a single IMAP / JMAP keyword. Gmail and Graph
    /// return `Err(AccountErrorKind::Unsupported)`; the convenience
    /// layer chooses an alternate primitive instead.
    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear membership in a Gmail-style label. JMAP / IMAP /
    /// Graph return `Err(AccountErrorKind::Unsupported)`.
    fn set_label_membership(
        &self,
        target: MutationTarget,
        label: ContainerId,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear a Graph category. JMAP / IMAP / Gmail return
    /// `Err(AccountErrorKind::Unsupported)`.
    fn set_category(
        &self,
        target: MutationTarget,
        category: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear a Graph singleValueExtendedProperty. JMAP / IMAP
    /// / Gmail return `Err(AccountErrorKind::Unsupported)`.
    fn set_extended_property(
        &self,
        target: MutationTarget,
        property_id: String,
        value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set a message's importance to exactly `level`. The primitive is
    /// *exclusive*: the implementation makes `level` the message's sole
    /// importance, clearing any prior value in the same wire operation.
    /// This exclusivity is the wart absorption - Graph's `importance` is a
    /// single-valued field, so the consumer issues one call, never an
    /// expand-into-two. JMAP/IMAP map `High` -> set `$important`,
    /// `Normal`/`Low` -> clear it. Accounts without an importance concept
    /// clear `set_importance` in `capabilities().pim_methods` and return
    /// `Unsupported(SetImportance)`.
    fn set_importance(
        &self,
        target: MutationTarget,
        level: Importance,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Mark read state. Every protocol implements this; canonical
    /// across the four.
    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    // ------------------------------------------------------------
    // Mail composition primitives (S1-W1)
    // ------------------------------------------------------------

    /// Send an RFC 5322 message. JMAP `EmailSubmission/set`; Gmail
    /// `messages.send`; Graph `POST /me/sendMail`; IMAP via the
    /// configured `bifrost-smtp` transport.
    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>>;

    /// Send a pre-assembled RFC 5322 / RFC 8098 message verbatim. Unlike
    /// [`send_message`](Self::send_message), the caller hands over the
    /// already-serialized MIME octets and the provider routes them through
    /// its submission path without re-rendering: Gmail `messages.send`
    /// with `raw`; Graph create-from-MIME then send; JMAP `Email/import`
    /// of the blob then `EmailSubmission/set`; IMAP SMTP submission with
    /// the envelope parsed from the MIME headers. `save_to_sent` mirrors
    /// [`SendRequest::save_to_sent`]: `None` leaves the choice to the
    /// provider default.
    ///
    /// This is the lane for a structured `SendRequest` cannot express - a
    /// pre-built `multipart/report` MDN (RFC 8098 read receipt). The
    /// default impl returns `Unsupported(Send)` so non-mail accounts
    /// (CalDAV, CardDAV) inherit it; the four mail protocol crates
    /// override.
    fn send_raw_message(
        &self,
        raw: Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let _ = (raw, save_to_sent);
        let error = AccountError::unsupported(AccountOperation::Send, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Streaming upload of an attachment. Returns a handle the
    /// consumer references in subsequent `SendRequest` /
    /// `DraftPatch` payloads. The `mime` argument is the
    /// Content-Type the server should record on the resulting
    /// attachment.
    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>>;

    /// Host an over-limit attachment in the account's cloud drive and return a
    /// shareable link, in one call. Upload + link are atomic from the caller's
    /// view: this returns `Ok(HostedAttachment)` only when the file is on the
    /// drive AND a link exists; any failure (including a failed link step after
    /// a successful upload) returns `Err`. `bytes` is the whole payload;
    /// `meta.size` MUST equal `bytes.len()`.
    ///
    /// Gated by `capabilities().pim_methods.host_attachment`. A `false` flag
    /// (JMAP, IMAP, CalDAV, CardDAV) means this returns
    /// `Unsupported(HostAttachment)`. Google -> Google Drive resumable upload +
    /// sharing permission; Graph -> OneDrive resumable upload + `createLink`.
    fn host_attachment(
        &self,
        bytes: Bytes,
        meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>>;

    /// Create a new draft.
    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>>;

    /// Update an existing draft. The patch is partial: only `Some`
    /// fields are applied.
    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Discard (delete) a draft without sending.
    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>>;

    /// Convert a draft into a sent message. The provider chooses
    /// atomicity; protocols that don't support atomic draft->send
    /// implement this as draft fetch + send + discard.
    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>>;

    /// Cancel a previously scheduled (not yet delivered) send. `handle`
    /// is the `ObjectId` `send_message` returned for the scheduled
    /// submission. Returns `Unsupported(CancelScheduledSend)` where the
    /// provider has no scheduled-send model, and `NotFound` /
    /// `ConcurrencyConflict` where the send already left the queue.
    ///
    /// Gated by `capabilities().pim_methods.scheduled_send`; a
    /// `false` flag means this returns `Unsupported`.
    fn cancel_scheduled_send(&self, handle: ObjectId) -> AccountFuture<Result<(), AccountError>>;

    /// Reschedule a previously scheduled send to a new instant. Same
    /// `handle` semantics as `cancel_scheduled_send`; `scheduled` is
    /// the new absolute send time, validated exactly as
    /// `SendRequest::scheduled` is. Returns the (possibly new)
    /// `ObjectId` of the rescheduled submission.
    ///
    /// Gated by `capabilities().pim_methods.scheduled_send`; a
    /// `false` flag means this returns `Unsupported`.
    fn reschedule_send(
        &self,
        handle: ObjectId,
        scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>>;

    // ------------------------------------------------------------
    // Search primitives (S1-W1)
    // ------------------------------------------------------------

    /// Thread-shaped search. Returns a page of `ThreadId`. Each
    /// call's page cursor is opaque; pass the previous page's
    /// `next_cursor` back to fetch the next page.
    fn search(&self, request: SearchRequest)
    -> AccountFuture<Result<Page<ThreadId>, AccountError>>;

    /// Message-shaped search using the same request AST.
    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>>;

    // ------------------------------------------------------------
    // Container CRUD primitives (S1-W1)
    // ------------------------------------------------------------

    /// Enumerate containers (folders, labels, mailboxes).
    ///
    /// A multi-namespace provider that cannot enumerate one namespace
    /// (an unreachable shared account, a revoked grant) lists the
    /// namespaces it can and records the rest on
    /// [`ContainerList::skipped_scopes`] with their classified errors,
    /// rather than failing the whole enumeration or silently omitting
    /// them. `Err(_)` means the enumeration itself failed.
    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>>;

    /// Create a new container of the given `kind` with `name` under
    /// `parent`. Returns the engine-facing id.
    ///
    /// `style` carries an optional initial color. Only Gmail honors it
    /// (labels are colorable); folder-shaped protocols with no color
    /// concept accept and ignore it.
    ///
    /// Contract: protocols that do not support nesting return
    /// `Err(AccountErrorKind::Unsupported)` when `parent` is `Some`.
    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
        style: Option<ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>>;

    /// Rename a container.
    ///
    /// `style`, when `Some`, also recolors the container in the same
    /// call (a Gmail label recolor). Folder-shaped protocols with no
    /// color concept accept and ignore it.
    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
        style: Option<ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Move a container under a new parent. Folder-kind only;
    /// label-kind containers return `Err(AccountErrorKind::Unsupported)`.
    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Delete a container. Contract: a non-empty container either
    /// fails or moves contents to Trash; protocols MUST NOT silently
    /// drop messages.
    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>>;

    // ------------------------------------------------------------
    // Settings primitives (S1-W1)
    // ------------------------------------------------------------

    /// List all sending identities on this account.
    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>>;

    /// Update one identity. `patch` is partial; only `Some` fields
    /// are applied.
    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Read the vacation responder config, when supported.
    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>>;

    /// Replace the vacation responder config.
    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>>;

    /// Read the storage quota readout, when supported.
    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>>;

    // ------------------------------------------------------------
    // Server-side filter primitives (S2-W1)
    //
    // Accounts advertise the supported model through
    // `capabilities().filter_rule_shape` and per-method support
    // through `capabilities().pim_methods`.
    // ------------------------------------------------------------

    /// List server-side filter rules or scripts.
    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>>;

    /// Create a server-side filter rule or script.
    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>>;

    /// Update a server-side filter rule or script.
    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Delete a server-side filter rule or script.
    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>>;

    /// Validate a server-side filter payload without storing it.
    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>>;

    // ------------------------------------------------------------
    // Contact primitives (S3-W1)
    //
    // Providers advertise support through
    // `capabilities().pim_methods`. Accounts without a native or
    // configured contacts backend return `Err(Unsupported)`.
    // ------------------------------------------------------------

    /// List address books or contact folders.
    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>>;

    /// List contacts, optionally scoped to one address book. Pagination
    /// cursor bytes are provider-owned and should be passed back from
    /// the previous `Page::next_cursor`.
    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>>;

    /// Fetch one contact card by engine-facing id.
    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>>;

    /// Create one contact card.
    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>>;

    /// Partially update one contact card.
    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Delete one contact card.
    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>>;

    /// Delete with an optional caller version. A supplied version must be
    /// enforced or refused before I/O; it must never become a blind delete.
    /// With no version, delegates to the ordinary delete door.
    fn contact_delete_if_version(
        &self,
        contact: ContactId,
        expected_version: Option<crate::mutation::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        if expected_version.is_some() {
            let error = AccountError::unsupported(AccountOperation::ContactDelete, self.protocol());
            return Box::pin(async move { Err(error) });
        }
        self.contact_delete(contact)
    }

    /// Provider-side contact search.
    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>>;

    /// Search the organization directory (Global Address List). The directory
    /// is the org-wide, read-only address corpus - distinct from the per-account
    /// address books `contact_search` serves. When `directory_enumerate` is true,
    /// an empty `query` enumerates the directory (page through
    /// `Page::next_cursor` to exhaust it); a non-empty
    /// `query` is a provider-side lookup. `limit` caps the page; `page_cursor`
    /// resumes from a prior `Page::next_cursor`.
    ///
    /// Gated by `capabilities().pim_methods.directory_search` for non-empty
    /// lookups. An empty query without `directory_enumerate` returns classified
    /// `Unsupported(DirectorySearch)`. Accounts without directory support return
    /// Unsupported before validating a cursor or performing I/O. A provider
    /// without paging rejects a supplied cursor locally. Terminal results may
    /// still be incomplete: inspect `Page::truncated` and the failure lanes.
    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>>;

    /// List the mail-enabled organization-directory groups the
    /// authenticated mailbox belongs to (Microsoft 365 groups,
    /// distribution lists, mail-enabled security groups). Read-only, like
    /// `directory_search`; mail-disabled groups are dropped at the
    /// provider boundary. `page_cursor` resumes from a prior
    /// `Page::next_cursor`.
    ///
    /// Gated by `capabilities().pim_methods.directory_groups_list`.
    /// Accounts without a directory-group surface leave the flag `false`
    /// and return `Unsupported(DirectoryGroupsList)`. A supporting
    /// protocol on a tenant without directory-group read consent fails
    /// with a `NoPermission` error at call time.
    fn directory_groups_list(
        &self,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>>;

    /// Expand one directory group to its user members. Expansion is
    /// transitive and provider-side: nested groups are flattened to
    /// their users and never appear as members. Members without a
    /// resolvable email address are dropped. `page_cursor` resumes from
    /// a prior `Page::next_cursor`.
    ///
    /// Gated by `capabilities().pim_methods.directory_group_expand`,
    /// with the same consent caveat as `directory_groups_list`.
    fn directory_group_expand(
        &self,
        group: DirectoryGroupId,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>>;

    // ------------------------------------------------------------
    // Calendar primitives (S4-W1)
    //
    // Providers advertise support through
    // `capabilities().pim_methods`. Accounts without a native or
    // configured calendar backend return `Err(Unsupported)`.
    // ------------------------------------------------------------

    /// List calendars exposed by this account.
    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>>;

    /// List events in a provider-side time range.
    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>>;

    /// Fetch one event by engine-facing id.
    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>>;

    /// Create one event.
    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>>;

    /// Partially update one event.
    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Delete one event.
    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>>;

    /// Delete with an optional caller version. A supplied version must be
    /// enforced or refused before I/O; it must never become a blind delete.
    /// With no version, delegates to the ordinary delete door.
    fn event_delete_if_version(
        &self,
        event: EventId,
        expected_version: Option<crate::mutation::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        if expected_version.is_some() {
            let error = AccountError::unsupported(AccountOperation::EventDelete, self.protocol());
            return Box::pin(async move { Err(error) });
        }
        self.event_delete(event)
    }

    /// Update the authenticated user's RSVP status for one event.
    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Provider-side event search.
    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>>;

    // ------------------------------------------------------------
    // Threading + hydration primitives (S1-W1)
    // ------------------------------------------------------------

    /// Hydrate every message in a thread. JMAP `Thread/get` +
    /// `Email/get`; IMAP `THREAD REFERENCES` + per-message FETCH;
    /// Gmail `threads.get`; Graph conversation API.
    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>>;

    /// Hydrate one message at a specific projection level.
    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>>;

    // ------------------------------------------------------------
    // Conveniences (S1-W1)
    //
    // Default impls encode ratatoskr's product opinions. Each
    // dispatches into primitives and reads
    // `capabilities().conveniences` to pick the right one.
    //
    // Default-impl strategy notes (see S1-W1 report):
    //
    // - Single-call conveniences (`apply_label`, `remove_label`,
    //   `set_read`, `set_starred`, `mark_replied`, `mark_forwarded`)
    //   have a real default impl. `set_starred`, `mark_replied`,
    //   and `mark_forwarded` use capability-based dispatch through
    //   `capabilities().conveniences` rather than a hard-coded
    //   protocol switch, so consumers can override the dispatch
    //   shape per-account without subclassing the trait.
    // - Multi-call conveniences (`move_thread`, `delete_thread`)
    //   default to `Err(Unsupported)`: the boxed `'static` future
    //   returned by a trait method cannot borrow `&self` across the
    //   await boundary between the first and second primitive, and
    //   demanding every Account impl ship around in `Arc<Self>`
    //   shape was the wrong tradeoff. Protocol crates implement
    //   these in-crate where they have access to their own `Arc`-
    //   shaped handle.
    // ------------------------------------------------------------

    /// Move a thread between containers. `source` is the container
    /// being moved out of (`None` when the consumer is not tracking
    /// a source container, e.g. unifying a flat label-rendering UI).
    ///
    /// Default impl returns `Err(AccountErrorKind::Unsupported)`; protocol
    /// crates override with `add_to_container(target)` followed by
    /// `remove_from_container(source)`. Order: add-then-remove so a
    /// failure on the second step leaves the message in both
    /// containers (recoverable) rather than neither (lost).
    fn move_thread(
        &self,
        _thread: ThreadId,
        _target: ContainerId,
        _source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let error = AccountError::unsupported(AccountOperation::MoveThread, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Apply a label to `target`. Dispatches by `label.provenance`
    /// to the right primitive.
    fn apply_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        dispatch_label(self, target, label, true)
    }

    /// Remove a label from `target`. Same dispatch shape as
    /// `apply_label`.
    fn remove_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        dispatch_label(self, target, label, false)
    }

    /// Alias for `set_is_read`. Exists for naming symmetry with the
    /// other convenience setters.
    fn set_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.set_is_read(target, is_read)
    }

    /// Toggle the starred / flagged bit. Dispatches by
    /// `capabilities().conveniences.starred` to the right primitive.
    ///
    /// Capability-based dispatch (rather than a `match` on
    /// `ProtocolKind`) is the chosen design here: protocol crates
    /// know best which primitive corresponds to "starred" for their
    /// account (Graph has both `flag.flagStatus` and categories;
    /// IMAP has both `\Flagged` and Sieve flag-keyword adjuncts;
    /// custom deployments may differ). The capability shape lets
    /// the protocol crate declare the answer once at open-time
    /// rather than have the convenience embed a hard-coded provider
    /// switch.
    fn set_starred(
        &self,
        target: MutationTarget,
        starred: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        const STARRED_KEYWORD: &str = "$flagged";
        const STARRED_LABEL: &str = "STARRED";
        match self.capabilities().conveniences.starred {
            StarredFlagShape::Keyword => {
                self.set_keyword(target, STARRED_KEYWORD.to_string(), starred)
            }
            StarredFlagShape::LabelMembership => {
                self.set_label_membership(target, ContainerId(STARRED_LABEL.to_string()), starred)
            }
            StarredFlagShape::Category => {
                self.set_category(target, STARRED_KEYWORD.to_string(), starred)
            }
            StarredFlagShape::None => {
                let error =
                    AccountError::unsupported(AccountOperation::SetStarred, self.protocol());
                Box::pin(async move { Err(error) })
            }
        }
    }

    /// Mark a message as replied. Default impl dispatches based on
    /// `capabilities().conveniences.replied_via_keyword` /
    /// `replied_via_extended_property`; if neither flag is set,
    /// returns `Err(Unsupported)` (Gmail's case: replied state is
    /// derived on sync, not a writeable flag).
    fn mark_replied(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        const ANSWERED_KEYWORD: &str = "$answered";
        const PR_LAST_VERB_EXECUTED: &str = "PR_LAST_VERB_EXECUTED";
        const PR_LAST_VERB_REPLIED: &str = "102";
        let target = MutationTarget::Message(message);
        let conv = self.capabilities().conveniences;
        if conv.replied_via_keyword {
            return self.set_keyword(target, ANSWERED_KEYWORD.to_string(), true);
        }
        if conv.replied_via_extended_property {
            return self.set_extended_property(
                target,
                PR_LAST_VERB_EXECUTED.to_string(),
                Some(PR_LAST_VERB_REPLIED.to_string()),
            );
        }

        let error = AccountError::unsupported(AccountOperation::MarkReplied, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Mark a message as forwarded. Same dispatch shape as
    /// `mark_replied`.
    fn mark_forwarded(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        const FORWARDED_KEYWORD: &str = "$forwarded";
        const PR_LAST_VERB_EXECUTED: &str = "PR_LAST_VERB_EXECUTED";
        const PR_LAST_VERB_FORWARDED: &str = "104";
        let target = MutationTarget::Message(message);
        let conv = self.capabilities().conveniences;
        if conv.forwarded_via_keyword {
            return self.set_keyword(target, FORWARDED_KEYWORD.to_string(), true);
        }
        if conv.forwarded_via_extended_property {
            return self.set_extended_property(
                target,
                PR_LAST_VERB_EXECUTED.to_string(),
                Some(PR_LAST_VERB_FORWARDED.to_string()),
            );
        }

        let error = AccountError::unsupported(AccountOperation::MarkForwarded, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Persist that an MDN (read receipt) was dispatched for `message`, by
    /// flipping the `$MDNSent` keyword. Dispatches through
    /// `capabilities().conveniences.mdn_sent_via_keyword`; accounts whose
    /// read-receipt model is read-only (Gmail, Graph) leave it `false` and
    /// the convenience returns `Unsupported(MarkMdnSent)`.
    fn mark_mdn_sent(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        const MDN_SENT_KEYWORD: &str = "$MDNSent";
        let target = MutationTarget::Message(message);
        if self.capabilities().conveniences.mdn_sent_via_keyword {
            return self.set_keyword(target, MDN_SENT_KEYWORD.to_string(), true);
        }

        let error = AccountError::unsupported(AccountOperation::MarkMdnSent, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Contact autocomplete convenience for recipient and attendee
    /// pickers. Default implementation returns the first page of
    /// `contact_search`.
    fn contact_autocomplete(
        &self,
        query: String,
        limit: u32,
    ) -> AccountFuture<Result<Vec<ContactCard>, AccountError>> {
        let mut request = ContactSearchRequest::new(query);
        request.limit = Some(limit);
        let future = self.contact_search(request);
        Box::pin(async move { future.await.map(|page| page.items) })
    }

    /// Event autocomplete convenience for calendar search boxes.
    /// Default implementation returns the first page of `event_search`.
    fn event_autocomplete(
        &self,
        query: String,
        limit: u32,
    ) -> AccountFuture<Result<Vec<CalendarEvent>, AccountError>> {
        let mut request = EventSearchRequest::new(query);
        request.limit = Some(limit);
        let future = self.event_search(request);
        Box::pin(async move { future.await.map(|page| page.items) })
    }

    /// Move a thread to Trash, or delete-permanently if already in
    /// Trash.
    ///
    /// Default impl returns `Err(Unsupported)`. The wire-level
    /// dispatch ("trash if elsewhere; expunge if in Trash") needs
    /// to read the current container set, which is a chained call;
    /// the default impl cannot perform a chained call without
    /// borrowing `&self` across the await boundary into a `'static`
    /// future. Protocol crates ship this convenience using their
    /// own `Arc<Self>`-shaped handle.
    fn delete_thread(
        &self,
        _thread: ThreadId,
        _current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let error = AccountError::unsupported(AccountOperation::DeleteThread, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Graceful local-handle teardown. IMAP `LOGOUT` + pool drain,
    /// JMAP WebSocket close, Graph subscription stream end, local
    /// worker shutdown. Idempotent: takes `&self` so it composes
    /// with `Arc<dyn Account>` and is safe to call more than once.
    ///
    /// Does NOT destroy durable server-side push subscriptions; those
    /// go through `push_unsubscribe` explicitly.
    fn close(&self) -> AccountFuture<Result<(), AccountError>>;
}

/// Engine-facing factory.
///
/// Consumers register one per account so the engine can perform
/// reopen cycles (capability change, transport reset) without
/// knowing protocol config. The engine owns the current open
/// `Arc<dyn Account>` and calls `open(account_id)` when it needs a
/// fresh one. The engine-minted `AccountId` is threaded through so
/// the protocol crate can attach to `bifrost-net` / `MeterSink` /
/// trace correlation under the right key on every reopen.
pub trait AccountFactory: Send + Sync + 'static {
    /// Open the account with the engine's identifier for it.
    ///
    /// The `AccountId` is the engine-minted handle the consumer
    /// passed to `SyncEngine::attach`. Protocol crates that wire
    /// `bifrost-net` (JMAP, Gmail, Graph), drive a `MeterSink`
    /// (IMAP, SMTP), or correlate logs to a per-account trace use
    /// it as the registration key. On reopen the engine calls
    /// `open` with the same id so attached resources can be
    /// re-registered against the same key.
    ///
    /// `Err(_)` means the account did not open: the primary surface
    /// is unavailable. A failure confined to an independently
    /// quarantinable part of the surface - a foreign (shared /
    /// delegate) namespace whose open-time probe failed - must NOT
    /// take that arm: the primary opens, the degraded part is left
    /// out of the handle's surface, and the omission is recorded on
    /// [`OpenedAccount::skipped_scopes`] with its classified error.
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>>;
}

/// The result of a successful [`AccountFactory::open`].
///
/// `account` is the live handle. `skipped_scopes` names the parts of
/// the account's potential surface that open discovered but could not
/// bring up and therefore left out of this handle - a shared JMAP
/// account whose seeding probe hit a 503 after retries, a delegate
/// grant the server now refuses. Each entry carries the classified
/// `AccountError`, so a consumer can tell a transient outage (a
/// reopen rediscovers the namespace) from a revoked grant, and can
/// surface either instead of learning about it from a support ticket.
///
/// The lane exists because neither of the alternative shapes is
/// acceptable: failing the whole open blocks the user's primary mail
/// on someone else's shared mailbox being down, and skipping silently
/// erases the share for the session with no signal anywhere.
///
/// Like `Page`, deliberately not `#[non_exhaustive]`: every factory
/// constructs it directly, so a future lane must break every
/// constructor and each protocol answers the new question instead of
/// silently defaulting it.
#[derive(Clone)]
pub struct OpenedAccount {
    /// The live account handle.
    pub account: Arc<dyn Account>,
    /// Parts of the account surface open skipped instead of bringing
    /// up, with their classified failures. Empty when the whole
    /// discovered surface opened.
    pub skipped_scopes: Vec<SkippedScope>,
}

impl OpenedAccount {
    /// An open with nothing skipped: the whole discovered surface is
    /// live on `account`.
    #[must_use]
    pub fn complete(account: Arc<dyn Account>) -> Self {
        Self {
            account,
            skipped_scopes: Vec::new(),
        }
    }
}

impl std::fmt::Debug for OpenedAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedAccount")
            .field("account", &"Arc<dyn Account>")
            .field("skipped_scopes", &self.skipped_scopes)
            .finish()
    }
}

/// Shared dispatch for `apply_label` / `remove_label`.
///
/// Reads `label.provenance` to decide which primitive to invoke.
/// The mapping is fixed at the trait surface because
/// `Provenance::kind` (Folder | Label) is the load-bearing signal:
/// a label-kind id under Gmail's provider is a label-membership
/// flip; under JMAP / IMAP it is a keyword flip; under Graph it is
/// a category (Graph categories are the closest analog to labels,
/// and Graph cannot express IMAP keywords). Folder-kind ids are
/// container-membership flips for everyone except Graph, which
/// uses categories for its folder-shaped labels too.
fn dispatch_label<T: Account + ?Sized>(
    receiver: &T,
    target: MutationTarget,
    label: Label,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    use crate::cursor::ProtocolKind;
    match (label.provenance.kind, label.provenance.provider) {
        (ContainerKind::Label, ProtocolKind::Gmail) => {
            receiver.set_label_membership(target, label.id, value)
        }
        (ContainerKind::Label, ProtocolKind::Graph) => {
            receiver.set_category(target, label.provenance.native, value)
        }
        (ContainerKind::Label, _) => receiver.set_keyword(target, label.provenance.native, value),
        (ContainerKind::Folder, ProtocolKind::Graph) => {
            receiver.set_category(target, label.provenance.native, value)
        }
        (ContainerKind::Folder, _) => {
            if value {
                receiver.add_to_container(target, label.id)
            } else {
                receiver.remove_from_container(target, label.id)
            }
        }
    }
}

#[cfg(test)]
mod push_subscription_tests {
    use super::*;
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, BatchItemId, BatchOutcome, BatchOutcomeBuilder,
        Cause, RequestCause,
    };

    fn outcomes_with_success() -> BatchOutcome<CursorScope> {
        let item = BatchItemId("0".to_string());
        let mut builder = BatchOutcomeBuilder::new();
        builder.push_succeeded(item.clone(), CursorScope::Account);
        builder.finalize(&[item]).expect("complete outcomes")
    }

    fn outcomes_with_failure() -> BatchOutcome<CursorScope> {
        let operation = AccountOperation::PushSubscribe;
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Unsupported(operation),
            Cause::Request(RequestCause::Unsupported { operation }),
        )
        .try_build()
        .expect("valid unsupported error");
        let item = BatchItemId("0".to_string());
        let mut builder = BatchOutcomeBuilder::new();
        builder.push_failed(item.clone(), error);
        builder.finalize(&[item]).expect("complete outcomes")
    }

    fn empty_outcomes() -> BatchOutcome<CursorScope> {
        BatchOutcomeBuilder::new()
            .finalize(&[])
            .expect("empty outcomes")
    }

    #[test]
    fn new_rejects_mismatched_handle_and_success_lanes() {
        let with_handle = PushSubscription::new(
            Some(SubscriptionHandle("handle".to_string())),
            empty_outcomes(),
        );
        assert_eq!(
            with_handle.unwrap_err(),
            PushSubscriptionBuildError::HandleWithoutSucceededScopes
        );

        let without_handle = PushSubscription::new(None, outcomes_with_success());
        assert_eq!(
            without_handle.unwrap_err(),
            PushSubscriptionBuildError::SucceededScopesWithoutHandle
        );
    }

    #[test]
    fn new_accepts_a_handleless_all_failed_outcome() {
        let result = PushSubscription::new(None, outcomes_with_failure());
        assert!(result.is_ok());
    }

    #[test]
    fn all_succeeded_rejects_an_empty_scope_list() {
        let result = PushSubscription::all_succeeded(SubscriptionHandle("handle".to_string()), &[]);
        assert_eq!(
            result.unwrap_err(),
            PushSubscriptionBuildError::HandleWithoutSucceededScopes
        );
    }
}
