//! IMAP implementation of the shared `Account` trait.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bifrost_types::{
    Account, AccountError, AccountFuture, AccountOperation, AccountStream, AttachmentHandle,
    BlobHandle, ByteRange, Calendar, CalendarEvent, Change, ChangeCursor, CloudUploadMeta,
    ContactCard, ContactCreate, ContactId, ContactPatch, ContactSearchRequest, ContainerId,
    ContainerKind, ContainerList, CursorDescriptor, CursorEstablishment, CursorScope,
    DirectoryCard, DirectoryGroup, DirectoryGroupId, DirectoryGroupMember, DraftHandle, DraftPatch,
    EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation,
    HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, Importance, InventoryEvent, ItemOutcome, MembershipScope, Message,
    MutationSuccess, MutationTarget, ObjectId, Page, Priority, Projection, QuotaInfo, RsvpStatus,
    SearchRequest, SendReceipt, SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId, VacationConfig,
};
use bifrost_types::{AddressBook, AddressBookId};
use futures::stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::types::{MailboxName, SelectOptions, SyncSelectResult, UidValidity};

mod blob;
mod capabilities;
#[cfg(test)]
mod capability_contract_tests;
mod changes;
mod close;
mod envelope;
pub(crate) mod error;
mod factory;
mod folder_registry;
mod get;
mod hydration;
mod inventory;
mod mutate;
/// BODYSTRUCTURE part traversal + durable part-handle codec (imap-G1 stage 1).
/// Not wired into any projection or opener yet; see the module docs.
mod parts;
mod pim;
mod poll;
mod pool;
mod push;
mod scopes;
#[cfg(test)]
#[path = "scripted_tests.rs"]
mod scripted_tests;
pub(crate) mod sieve;
mod submission;
mod targets;
#[cfg(test)]
mod test_support;

// pub: consumers register this factory with bifrost-sync without naming ImapAccount.
pub use factory::{ImapAccountConfig, ImapAccountFactory};
// pub: the UID polling surface, reached through
// `ImapAccountFactory::open_with_poller`.
pub use poll::{
    FolderObservation, ImapMessageRef, ImapPoller, InvalidImapRef, RawMessage, UidListing, UidPage,
    UidScan,
};
pub use sieve::ManageSieveConfig;
// pub: consumers configure IMAP submission (SMTP send) through these.
pub use submission::{SmtpSubmissionConfig, SubmissionCredentials, SubmissionTls};

pub(crate) use submission::SubmissionTransport;

pub(crate) use crate::types::set::UidSet;
pub(crate) use envelope::{
    DecodedObjectId, FolderCursor, decode_cursor, decode_object_id, decode_thread_id,
    encode_cursor, encode_object_id, encode_thread_id,
};
pub(crate) use folder_registry::FolderRegistry;
pub(crate) use pool::{Pool, PooledConn};

const STREAM_CAPACITY: usize = 64;
const BATCH_ITEMS: usize = 128;
/// Maximum decoded target ids retained before a get or mutation stream flushes.
const TARGET_BUFFER_ITEMS: usize = 256;

/// Only changes consumes SELECT's replay payload. Reads and writes must not
/// replay a cached checkpoint that their caller cannot acknowledge.
pub(crate) enum SelectPurpose<'a> {
    Sync(&'a FolderCursor),
    Read,
    Mutate,
}

/// Account policy, distinct from each connection's committed ENABLE profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QresyncNegotiation {
    NotRequested,
    Enabled,
    Refused(QresyncRefusal),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QresyncRefusal {
    KnownBrokenServer,
    EnableNotConfirmed,
    SessionDowngraded,
    /// A QRESYNC SELECT response failed to parse. Kept distinct from the
    /// generic downgrade so support can see the server sent a QRESYNC
    /// response this crate could not read, not that QRESYNC was unavailable.
    SelectParseFailed,
}

impl QresyncRefusal {
    fn reason(self) -> &'static str {
        match self {
            Self::SelectParseFailed => {
                "QRESYNC SELECT failed during response parsing; continuing with CONDSTORE"
            }
            Self::KnownBrokenServer => {
                "server ID matches a known broken QRESYNC implementation; continuing with CONDSTORE"
            }
            Self::EnableNotConfirmed => {
                "server advertised QRESYNC but ENABLE did not confirm it; continuing with CONDSTORE"
            }
            Self::SessionDowngraded => {
                "QRESYNC failed for this account session; continuing with CONDSTORE"
            }
        }
    }
}

/// Open IMAP account handle. The handle owns the connection pool.
#[derive(Clone)]
pub(crate) struct ImapAccount {
    inner: Arc<ImapAccountInner>,
}

pub(crate) struct ImapAccountInner {
    pub(crate) config: Arc<ImapAccountConfig>,
    pub(crate) capabilities: bifrost_types::AccountCapabilities,
    pub(crate) pool: Arc<Pool>,
    pub(crate) folders: Arc<FolderRegistry>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) close_completion: std::sync::OnceLock<close::Completion>,
    pub(crate) send_gate: tokio::sync::RwLock<()>,
    pub(crate) priority: AtomicU8,
    pub(crate) bandwidth_budget: bifrost_net::BandwidthBudget,
    pub(crate) push: push::PushState,
    pub(crate) contacts: Option<Arc<dyn Account>>,
    pub(crate) calendars: Option<Arc<dyn Account>>,
    /// Collision-free ownership index for composed DAV folder scopes.
    pub(crate) dav_scopes: DavScopeIndex,
    pub(crate) submission: Option<Arc<SubmissionTransport>>,
    /// Persistent enumeration gaps; open and container snapshots expose the
    /// same classified failures so missing shares never look like deletions.
    pub(crate) discovery_skips: Vec<bifrost_types::SkippedScope>,
}

pub(crate) struct ImapAccountParts {
    pub(crate) config: Arc<ImapAccountConfig>,
    pub(crate) capabilities: bifrost_types::AccountCapabilities,
    pub(crate) pool: Arc<Pool>,
    pub(crate) folders: Arc<FolderRegistry>,
    pub(crate) qresync: QresyncNegotiation,
    pub(crate) supports_notify: bool,
    pub(crate) bandwidth_budget: bifrost_net::BandwidthBudget,
    pub(crate) contacts: Option<Arc<dyn Account>>,
    pub(crate) calendars: Option<Arc<dyn Account>>,
    /// Collision-free ownership index for composed DAV folder scopes.
    pub(crate) dav_scopes: DavScopeIndex,
    pub(crate) submission: Option<Arc<SubmissionTransport>>,
    pub(crate) discovery_skips: Vec<bifrost_types::SkippedScope>,
}

impl ImapAccount {
    pub(crate) fn new(parts: ImapAccountParts) -> Self {
        let push = push::PushState::new(
            parts.supports_notify,
            // Capability admission is fixed from the open-time IDLE profile.
            // Direct callers must get the same availability as the sync engine.
            if parts.capabilities.push == bifrost_types::PushCapability::None {
                0
            } else {
                parts.config.push_session_capacity(parts.supports_notify)
            },
        );
        // The QRESYNC policy lives on the pool, which reads it at every dial
        // to bring a new member to the account's QRESYNC state. The pool
        // dials nothing before the account exists, so seeding it here is
        // seen by every dial.
        parts.pool.set_qresync_negotiation(parts.qresync);
        Self {
            inner: Arc::new(ImapAccountInner {
                config: parts.config,
                capabilities: parts.capabilities,
                pool: parts.pool,
                folders: parts.folders,
                shutdown: CancellationToken::new(),
                close_completion: std::sync::OnceLock::new(),
                send_gate: tokio::sync::RwLock::new(()),
                priority: AtomicU8::new(Priority::Normal as u8),
                bandwidth_budget: parts.bandwidth_budget,
                push,
                contacts: parts.contacts,
                calendars: parts.calendars,
                dav_scopes: parts.dav_scopes,
                submission: parts.submission,
                discovery_skips: parts.discovery_skips,
            }),
        }
    }
}

impl std::ops::Deref for ImapAccount {
    type Target = ImapAccountInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ImapAccount {
    pub(crate) fn command_timeout(&self) -> Duration {
        self.config.imap.command_timeout
    }

    pub(crate) async fn checkout_for_folder(
        &self,
        folder: &MailboxName,
    ) -> Result<PooledConn, Error> {
        self.pool.checkout_for_folder(folder).await
    }

    /// Classify every mailbox command, including failures after selection.
    /// Outlook users expect a revoked share to disable only that folder.
    pub(crate) fn folder_error(
        &self,
        error: Error,
        folder: &MailboxName,
        ctx: error::ImapErrorContext,
    ) -> AccountError {
        let owner = self
            .folders
            .get(folder)
            .and_then(|entry| entry.shared_owner.clone());
        error::folder_error(error, folder, owner.as_ref(), ctx)
    }

    /// COPY/MOVE can be denied at either endpoint. Prefer a known shared
    /// source, otherwise quarantine a shared destination on permission loss.
    /// A user's personal mailbox must remain usable after a share is withdrawn.
    pub(crate) fn folder_pair_error(
        &self,
        error: Error,
        source: &MailboxName,
        destination: Option<&MailboxName>,
        ctx: error::ImapErrorContext,
    ) -> AccountError {
        let source_shared = self
            .folders
            .get(source)
            .is_some_and(|entry| entry.shared_owner.is_some());
        let folder = if !source_shared && error::permission_denied(&error, &ctx) {
            destination
                .filter(|folder| {
                    self.folders
                        .get(folder)
                        .is_some_and(|entry| entry.shared_owner.is_some())
                })
                .unwrap_or(source)
        } else {
            source
        };
        self.folder_error(error, folder, ctx)
    }

    pub(crate) async fn select_folder(
        &self,
        conn: &mut PooledConn,
        folder: &MailboxName,
        purpose: SelectPurpose<'_>,
    ) -> Result<SyncSelectResult, Error> {
        // Classification stays at each operation's error boundary: SELECT is
        // not the last command that can discover a withdrawn share. Preserve
        // the raw error here for strategy downgrade and preflight restamping.
        let options = self.select_options(purpose)?;
        let result = conn
            .connection()
            .select_for_sync(&folder, &options, self.command_timeout())
            .await;
        // The driver knows whether a refusal preserved the previous mailbox,
        // deselected it, or followed a tagged OK with an incomplete response.
        conn.set_selected_affinity(conn.connection().selected_mailbox());
        let result = result?;
        self.folders.mark_seen(folder);
        Ok(result)
    }

    fn select_options(&self, purpose: SelectPurpose<'_>) -> Result<SelectOptions, Error> {
        let (cursor, mut options) = match purpose {
            SelectPurpose::Sync(cursor) => (Some(cursor), SelectOptions::read_only()),
            SelectPurpose::Read => (None, SelectOptions::read_only()),
            SelectPurpose::Mutate => (None, SelectOptions::read_write()),
        };

        if self.qresync_enabled()
            && let Some(FolderCursor::QResync {
                uidvalidity,
                modseq,
                baseline: envelope::UidBaseline::Complete(known_uids),
            }) = cursor
        {
            // Straight from the compact ranges: the baseline is already
            // sorted and coalesced, and on a large mailbox expanding it into
            // individual UIDs only to re-coalesce them is the entire cost of
            // building the QRESYNC operand.
            let known_uids = crate::types::UidSetExpr::from_ranges(known_uids.ranges());
            // Cached state must fail through the operation boundary if the
            // wire operand validator refuses it, rather than panic at runtime.
            options = options.with_qresync(*uidvalidity, *modseq, known_uids)?;
        }
        Ok(options)
    }

    pub(crate) fn cursor_from_select(
        &self,
        selected: &crate::types::SelectedMailbox,
        known_uids: Option<UidSet>,
    ) -> Result<FolderCursor, Error> {
        let uidvalidity = selected_uidvalidity(selected)?;
        let known_uids = known_uids.unwrap_or_default();
        if self.qresync_enabled()
            && let Some(modseq) = selected.modseq()
        {
            return Ok(FolderCursor::QResync {
                uidvalidity,
                modseq,
                baseline: envelope::UidBaseline::Complete(known_uids),
            });
        }
        if let Some(modseq) = selected.modseq() {
            return Ok(FolderCursor::Condstore {
                uidvalidity,
                modseq,
                known_uids,
            });
        }
        Ok(FolderCursor::Basic {
            uidvalidity,
            uidnext: selected.uid_next,
            known_uids,
        })
    }

    /// The account's QRESYNC policy: whether new work may REQUEST QRESYNC
    /// (SELECT parameters, the QRESYNC change strategy, QResync cursors, and
    /// ENABLE on newly dialed members).
    ///
    /// A strategy-admission policy only. It says nothing about whether a
    /// given connection has QRESYNC enabled - that is the connection's own
    /// committed profile, which is what wire interpretation reads.
    pub(crate) fn qresync_enabled(&self) -> bool {
        self.pool.qresync_policy()
    }

    pub(crate) fn disable_qresync_for_session(&self, refusal: QresyncRefusal) {
        // Other folder syncs may already have passed their QRESYNC gate
        // on this account. They are allowed to finish or independently
        // downgrade; this one-way flag only prevents new QRESYNC work. It
        // does not undo QRESYNC on members already enabled: RFC 7162
        // Section 3.2.3 gives no way back short of closing the connection,
        // and every consumer reads the form a removal arrived in rather
        // than this policy.
        self.pool
            .set_qresync_negotiation(QresyncNegotiation::Refused(refusal));
    }

    pub(crate) fn take_qresync_negotiation_warning(&self) -> Option<String> {
        self.pool.take_qresync_warning()
    }

    /// Un-spend the QRESYNC negotiation one-shot.
    ///
    /// The one-shot is taken inside a spawned streaming task, so a stream
    /// dropped before the warning reaches the consumer would otherwise
    /// burn it with nobody informed. The taker holds a ticket that calls
    /// this if its output channel turns out to be gone.
    pub(crate) fn restore_qresync_negotiation_warning(&self) {
        self.pool.restore_qresync_warning();
    }

    /// Non-consuming read of the QRESYNC negotiation reason. Unlike
    /// `take_qresync_negotiation_warning` (a one-shot that fires the
    /// standalone account-level warning exactly once, to avoid per-folder
    /// spam), this lets every later QRESYNC->CONDSTORE downgrade reuse the
    /// specific session-level reason as its downgrade reason string rather
    /// than falling back to a generic message once the one-shot is spent.
    pub(crate) fn qresync_negotiation_reason(&self) -> Option<String> {
        self.pool.qresync_reason().map(str::to_owned)
    }
}

impl Account for ImapAccount {
    fn protocol(&self) -> bifrost_types::Protocol {
        bifrost_types::Protocol::Imap
    }

    // Account: exposes the factory-built capability snapshot; direct users call server_profile().
    fn capabilities(&self) -> &bifrost_types::AccountCapabilities {
        &self.capabilities
    }

    // Account: stores the engine priority hint; direct ImapConnection calls remain caller scheduled.
    fn set_priority(&self, priority: Priority) {
        // IMAP has no autonomous scheduler: delaying explicit calls would hide
        // fresh mail from an enterprise user. The engine schedules IMAP work.
        // Composed HTTP accounts do own admission schedulers and must see the hint.
        self.priority.store(priority as u8, Ordering::Release);
        if let Some(contacts) = &self.contacts {
            contacts.set_priority(priority);
        }
        if let Some(calendars) = &self.calendars {
            calendars.set_priority(priority);
        }
    }

    // Account: every protocol leg admits against this shared budget.
    fn set_bandwidth_cap(&self, bps: Option<bifrost_types::BandwidthCap>) {
        self.bandwidth_budget
            .set_cap(bps.map(bifrost_types::BandwidthCap::get));
    }

    // Account: names the per-mailbox throttle keys engine work consumes; direct users pace themselves.
    //
    // IMAP's only narrower-than-account throttle is the per-mailbox one
    // `mailbox_throttle` raises for a folder-scoped `[LIMIT]`, which the
    // engine records as `ThrottleKey::Folder { account, folder }` keyed on
    // the folder's name. Every arm below builds that exact key from the same
    // `MailboxName::account_id()` the error producer scopes with, so a recorded
    // hold and the work it should hold meet. Routing follows `route_scope`:
    // a composed DAV collection answers for its sub-account, never as an
    // IMAP mailbox.
    fn throttle_keys(
        &self,
        account: &bifrost_types::AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        use bifrost_types::{ThrottleKey, ThrottleWork};
        let key = |mailbox: &str| ThrottleKey::Folder {
            account: account.clone(),
            folder: bifrost_types::FolderId(mailbox.to_owned()),
        };
        match work {
            ThrottleWork::Scope(scope) => {
                match route_scope(self, scope, bifrost_types::AccountOperation::SyncChanges) {
                    Ok(ScopeHandler::Folder(mailbox)) => vec![key(&mailbox.account_id())],
                    Ok(ScopeHandler::Delegate(sub)) => sub.throttle_keys(account, work),
                    Err(_) => Vec::new(),
                }
            }
            ThrottleWork::Objects(ids) => {
                let mut keys = Vec::new();
                for id in ids {
                    // An id that does not decode names no IMAP folder; its
                    // mutation fails locally without reaching the wire.
                    if let Ok(decoded) =
                        decode_object_id(id, bifrost_types::AccountOperation::UpdateFlags)
                    {
                        let candidate = key(&decoded.folder.account_id());
                        if !keys.contains(&candidate) {
                            keys.push(candidate);
                        }
                    }
                }
                keys
            }
            ThrottleWork::Membership(MembershipScope::Folder(folder)) => self
                .dav_scopes
                .imap_mailbox(folder)
                .map(|mailbox| vec![key(&mailbox.account_id())])
                .unwrap_or_default(),
            // A DAV-owned collection, or a membership IMAP never routes a
            // move to; `ThrottleWork` is `#[non_exhaustive]`.
            _ => Vec::new(),
        }
    }

    // Account: describes opaque engine cursors; direct users inspect SyncSelectResult.
    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        changes::describe_cursor(self, cursor)
    }

    // Account: discovers engine cursor scopes from the folder registry; direct users call LIST.
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        scopes::discover_cursor_scopes(self.clone())
    }

    // Account: reports engine membership scopes; direct users call LIST/LIST-STATUS.
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        scopes::discover_memberships(self.clone())
    }

    // IMAP emits no folder lifecycle events. This stream waits for shutdown;
    // folder discovery is an open-time snapshot, not an IDLE/NOTIFY feed.
    fn scope_lifecycle_stream(&self) -> AccountStream<bifrost_types::ScopeLifecycleEvent> {
        scopes::scope_lifecycle_stream(self.clone())
    }

    // Account: mints an engine cursor from folder state; direct users call select_for_sync().
    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        inventory::establish_initial_cursor(self.clone(), scope)
    }

    // Account: emits engine inventory batches; direct users compose UID SEARCH/FETCH.
    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        inventory::inventory_stream(self.clone(), scope)
    }

    // Account: hydrates bifrost ObjectIds; direct users call uid_fetch* with native UIDs.
    fn get_stream(
        &self,
        ids: AccountStream<bifrost_types::ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        get::get_stream(self.clone(), ids, projection)
    }

    // Account: judges FlagsOnly read-backs; `\Recent` is server-owned.
    fn flag_target(
        &self,
        _target: &ObjectId,
        observed: &bifrost_types::FlagSet,
        op: &bifrost_types::FlagOp,
    ) -> bifrost_types::FlagTargetState {
        inventory::imap_flag_target(observed, op)
    }

    // Account: advances opaque cursors; direct users call sync_fetch() or UID commands.
    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream(self.clone(), cursor)
    }

    // Account: IMAP has no durable server-side subscription, so this starts in-process push.
    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        push::push_subscribe(self.clone(), scopes.to_vec())
    }

    // Account: unsubscribes the synthetic in-process push handle; direct users call notify_none().
    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::push_unsubscribe(self.clone(), handle)
    }

    // One IDLE/NOTIFY session per lane, driven by the engine; direct users
    // of the connection layer call idle().
    fn push_lanes(&self) -> Vec<bifrost_types::PushLaneSpec> {
        push::push_lanes(self)
    }

    fn push_stream(
        &self,
        lane: bifrost_types::PushLane,
        stop: bifrost_types::SessionStop,
        _gate: bifrost_types::PushGate,
    ) -> AccountStream<bifrost_types::PushSessionEvent> {
        push::push_stream(self.clone(), lane, stop)
    }

    // IMAP does not yet mint BlobHandles from BODYSTRUCTURE part metadata.
    // Decoded attachment BYTES do reach the consumer under FullWithBlobs, as
    // AttachmentSource::Inline on the parsed message; a resumable per-part
    // HANDLE is the separate capability that stays unsupported.
    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<bytes::Bytes>> {
        unsupported_stream(AccountOperation::OpenBlob)
    }

    // IMAP has no production BlobHandle surface to range yet.
    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        unsupported_stream(AccountOperation::OpenBlobRange)
    }

    // Account: streams the whole message via BODY.PEEK[]; verbatim RFC822 octets.
    fn open_raw_rfc822(
        &self,
        message: bifrost_types::ObjectId,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_raw_rfc822(self.clone(), message)
    }

    // Account: maps shared FlagOp batches to UID STORE; direct users call uid_store().
    fn bulk_set_flags(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_set_flags(self.clone(), targets, op, key)
    }

    fn bulk_set_flags_if_version(
        self: std::sync::Arc<Self>,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_set_flags_if_version(self.as_ref().clone(), targets, op, key)
    }

    // Account: maps shared membership moves to UID MOVE; direct users call uid_move_messages().
    fn bulk_move(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_move(self.clone(), targets, destination, key)
    }

    // Account: maps shared destroy batches to STORE Deleted plus UID EXPUNGE.
    fn bulk_destroy(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_destroy(self.clone(), targets, key)
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::add_to_container(self.clone(), target, container)
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::remove_from_container(self.clone(), target, container)
    }

    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: bifrost_types::Flag,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_keyword(self.clone(), target, keyword, value)
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(AccountOperation::SetLabelMembership)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(AccountOperation::SetCategory)
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(AccountOperation::SetExtendedProperty)
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_is_read(self.clone(), target, is_read)
    }

    fn set_importance(
        &self,
        target: MutationTarget,
        level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_importance(self.clone(), target, level)
    }

    fn send_message(
        &self,
        request: SendRequest,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        pim::send_message(self.clone(), request)
    }

    fn send_raw_message(
        &self,
        raw: bytes::Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        pim::send_raw_message(self.clone(), raw, save_to_sent)
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<bytes::Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        pim::unsupported_attachment(AccountOperation::AttachmentUpload)
    }

    fn host_attachment(
        &self,
        _bytes: bytes::Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        pim::unsupported_hosted(AccountOperation::HostAttachment)
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        pim::draft_create(self.clone(), patch)
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(AccountOperation::DraftUpdate)
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_discard(self.clone(), draft)
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<SendReceipt, AccountError>> {
        pim::draft_send(self.clone(), draft)
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        // SMTP FUTURERELEASE is fire-and-submit: RFC 4865 has no verb to
        // recall an accepted HOLDUNTIL submission. Honest provider limit.
        pim::unsupported_unit(AccountOperation::CancelScheduledSend)
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::unsupported_object(AccountOperation::RescheduleSend)
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(self.clone(), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(self.clone(), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        pim::containers_list(self.clone())
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        pim::container_create(self.clone(), kind, name, parent, style)
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_rename(self.clone(), container, name, style)
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_move(self.clone(), container, new_parent)
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        pim::container_delete(self.clone(), container)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list()
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get()
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get(self.clone())
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        sieve::filters_list(self.clone())
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        sieve::filter_create(self.clone(), filter)
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        sieve::filter_update(self.clone(), filter, patch)
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        sieve::filter_delete(self.clone(), filter)
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        sieve::filter_validate(self.clone(), filter)
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.address_books_list();
        }
        unsupported_future(AccountOperation::AddressBooksList)
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contacts_list(address_book, page_cursor);
        }
        unsupported_future(AccountOperation::ContactsList)
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_get(contact);
        }
        unsupported_future(AccountOperation::ContactGet)
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_create(contact);
        }
        unsupported_future(AccountOperation::ContactCreate)
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_update(contact, patch);
        }
        unsupported_future(AccountOperation::ContactUpdate)
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_delete(contact);
        }
        unsupported_future(AccountOperation::ContactDelete)
    }

    fn contact_delete_if_version(
        &self,
        contact: ContactId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_delete_if_version(contact, expected_version);
        }
        unsupported_future(AccountOperation::ContactDelete)
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_search(request);
        }
        unsupported_future(AccountOperation::ContactSearch)
    }

    fn contact_autocomplete(
        &self,
        query: String,
        limit: u32,
    ) -> AccountFuture<Result<Vec<ContactCard>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_autocomplete(query, limit);
        }
        unsupported_future(AccountOperation::ContactAutocomplete)
    }

    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.directory_search(query, limit, page_cursor);
        }
        unsupported_future(AccountOperation::DirectorySearch)
    }

    fn directory_groups_list(
        &self,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.directory_groups_list(page_cursor);
        }
        unsupported_future(AccountOperation::DirectoryGroupsList)
    }

    fn directory_group_expand(
        &self,
        group: DirectoryGroupId,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.directory_group_expand(group, page_cursor);
        }
        unsupported_future(AccountOperation::DirectoryGroupExpand)
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.calendars_list();
        }
        unsupported_future(AccountOperation::CalendarsList)
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.events_in_range(range);
        }
        unsupported_future(AccountOperation::EventsInRange)
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_get(event);
        }
        unsupported_future(AccountOperation::EventGet)
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_create(event);
        }
        unsupported_future(AccountOperation::EventCreate)
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_update(event, patch);
        }
        unsupported_future(AccountOperation::EventUpdate)
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_delete(event);
        }
        unsupported_future(AccountOperation::EventDelete)
    }

    fn event_delete_if_version(
        &self,
        event: EventId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_delete_if_version(event, expected_version);
        }
        unsupported_future(AccountOperation::EventDelete)
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_rsvp(event, status);
        }
        unsupported_future(AccountOperation::EventRsvp)
    }

    // Preserve the backend's caller-version contract, including its refusal
    // when it cannot guard RSVP. Never downgrade a protected response.
    fn event_rsvp_if_version(
        &self,
        event: EventId,
        status: RsvpStatus,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_rsvp_if_version(event, status, expected_version);
        }
        unsupported_future(AccountOperation::EventRsvp)
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_search(request);
        }
        unsupported_future(AccountOperation::EventSearch)
    }

    fn event_autocomplete(
        &self,
        query: String,
        limit: u32,
    ) -> AccountFuture<Result<Vec<CalendarEvent>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_autocomplete(query, limit);
        }
        unsupported_future(AccountOperation::EventAutocomplete)
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(self.clone(), thread)
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(self.clone(), message, projection)
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::move_thread(self.clone(), thread, target, source)
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::delete_thread(self.clone(), thread, current)
    }

    // Account: closes the pool; direct users close a single connection with logout().
    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        close::close(self.clone())
    }
}

fn unsupported_future<T: Send + 'static>(
    operation: AccountOperation,
) -> AccountFuture<Result<T, AccountError>> {
    Box::pin(async move { Err(error::unsupported(operation)) })
}

fn unsupported_stream<T: Send + 'static>(
    operation: AccountOperation,
) -> AccountStream<SyncEvent<T>> {
    Box::pin(futures::stream::iter([SyncEvent::Terminated(
        error::unsupported(operation),
    )]))
}

/// The UIDVALIDITY a completed SELECT or EXAMINE owes.
///
/// The server must send it (RFC 3501 6.3.1, RFC 9051 6.3.2), and the
/// connection layer hands the account layer an `Option` so this is where the
/// omission is judged. Zero is also unusable, even for a locally constructed
/// selection that bypasses the SELECT consumer's completion validation.
/// It is the SERVER's omission, so it is
/// `crate::error::ProviderFailure::Missing` (`Protocol(MissingField)`,
/// `ProviderContractViolation`) at every account-layer read, never a
/// caller-blaming `Request(Malformed)`: the one place the classification
/// lives, so the sync lanes and the PIM primitives cannot drift apart again.
pub(crate) fn selected_uidvalidity(
    selected: &crate::types::SelectedMailbox,
) -> Result<UidValidity, Error> {
    selected.uid_validity.ok_or_else(|| {
        Error::Provider(crate::error::ProviderFailure::Missing(
            "SELECT missing or zero UIDVALIDITY".into(),
        ))
    })
}

/// Convert a crate-private `crate::Error` into a public `AccountError`
/// at the account-trait boundary, stamping the supplied `ImapErrorContext`.
///
/// Every call site must populate the context with the calling operation
/// so the central recovery mapping has the operation and scope it needs
/// to derive `RecoveryClass`.
pub(crate) fn account_error_with(err: Error, ctx: error::ImapErrorContext) -> AccountError {
    error::into_account_error(err, ctx)
}

/// Wrap a fatal stream cause as `SyncEvent::Terminated`. Accepts any
/// `Into<TerminatedCause>`: a structured `AccountError` (already built
/// at the account boundary) or an `(Error, ImapErrorContext)` pair to
/// classify on the way out - the single helper the former twin
/// `fatal_event` / `terminated_event` pair collapsed into.
pub(crate) fn terminated_event<T, E: Into<TerminatedCause>>(cause: E) -> SyncEvent<T> {
    SyncEvent::Terminated(cause.into().into_account_error())
}

/// Carrier for the two ways a stream task currently produces a fatal
/// `AccountError`: a structured error built at the boundary, or a
/// crate-private `crate::Error` plus the calling context to classify.
pub(crate) enum TerminatedCause {
    Account(AccountError),
    // Boxed: the classify pair is much larger than a structured AccountError.
    Classify(Box<(Error, error::ImapErrorContext)>),
}

impl TerminatedCause {
    fn into_account_error(self) -> AccountError {
        match self {
            Self::Account(err) => err,
            Self::Classify(pair) => {
                let (err, ctx) = *pair;
                error::into_account_error(err, ctx)
            }
        }
    }
}

impl From<AccountError> for TerminatedCause {
    fn from(err: AccountError) -> Self {
        Self::Account(err)
    }
}

impl From<(Error, error::ImapErrorContext)> for TerminatedCause {
    fn from((err, ctx): (Error, error::ImapErrorContext)) -> Self {
        Self::Classify(Box::new((err, ctx)))
    }
}

/// Wrap output from a detached producer. State-changing work keeps its
/// existing completion policy when the receiver leaves; read-only producers
/// should retain their task handle with `guarded_receiver_stream` instead.
pub(crate) fn boxed_receiver_stream<T: Send + 'static>(
    rx: tokio::sync::mpsc::Receiver<T>,
) -> AccountStream<T> {
    Box::pin(ReceiverStream { rx })
}

/// A receiver stream that aborts the task feeding it when the stream is
/// dropped.
///
/// `Account::changes_stream`, `inventory_stream`, `get_stream` and `open_raw_rfc822` are
/// channel-backed by a detached `tokio::spawn`. Dropping the returned stream drops only the
/// receiver, so without this the task runs on against the account's
/// connection until its next `tx.send` fails - and the engine now cuts a
/// pending poll on every PAUSE, not only at teardown, then calls back in
/// on the same account. That let two drivers issue IMAP commands
/// concurrently over one account while the older one sat on a pool
/// checkout it would not release until it next tried to send.
///
/// Aborting is safe here precisely because the driver model already
/// handles a caller that leaves mid-command: a command the driver has not
/// yet claimed is withdrawn (never written, connection still reusable), and
/// leaving a claimed command without an answer marks the connection
/// abandoned. `PooledConn::drop` parks only a member that is alive and not
/// abandoned. So the abort returns the pooled connection (or retires it)
/// rather than leaking it.
pub(crate) fn guarded_receiver_stream<T: Send + 'static>(
    rx: tokio::sync::mpsc::Receiver<T>,
    task: tokio::task::JoinHandle<()>,
) -> AccountStream<T> {
    Box::pin(GuardedReceiverStream {
        rx,
        _guard: AbortOnDrop(task),
    })
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct GuardedReceiverStream<T> {
    rx: tokio::sync::mpsc::Receiver<T>,
    _guard: AbortOnDrop,
}

impl<T> Stream for GuardedReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

pub(crate) fn iter_stream<T: Send + Unpin + 'static>(items: Vec<T>) -> AccountStream<T> {
    Box::pin(IterStream {
        items: items.into(),
    })
}

struct ReceiverStream<T> {
    rx: tokio::sync::mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

struct IterStream<T> {
    items: VecDeque<T>,
}

impl<T: Unpin> Stream for IterStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.pop_front())
    }
}

pub(crate) fn batch<T>(
    items: Vec<T>,
    boundary: bifrost_types::PageBoundary,
    checkpoint: Option<bifrost_types::Checkpoint>,
    bytes_in: u64,
) -> SyncEvent<T> {
    SyncEvent::Batch(bifrost_types::Batch {
        items,
        page_boundary: boundary,
        server_latency: Duration::ZERO,
        bytes_in,
        checkpoint,
    })
}

pub(crate) fn measured_bytes_in(connection: &crate::ImapConnection, previous: &mut u64) -> u64 {
    measured_bytes_delta(connection.bytes_in(), previous)
}

fn measured_bytes_delta(current: u64, previous: &mut u64) -> u64 {
    let bytes_in = current.saturating_sub(*previous);
    *previous = current;
    bytes_in
}

use scope_router::{DavScopeIndex, DavScopeOwner, ImapFolder, ScopeHandler, route_scope};
#[cfg(test)]
use scope_router::{
    test_folder_from_scope as folder_from_scope, test_route_typed_scope as route_typed_scope,
};

// Only this module can mint an ImapFolder; tasks cannot bypass scope ownership.
mod scope_router {
    use super::*;

    fn folder_from_scope(
        scope: &CursorScope,
        op: bifrost_types::AccountOperation,
    ) -> Result<MailboxName, AccountError> {
        match scope {
            CursorScope::Folder(folder) => MailboxName::from_account_id(&folder.0).map_err(|e| {
                error::into_account_error(Error::from(e), error::ImapErrorContext::operation(op))
            }),
            _ => Err(error::unsupported(op)),
        }
    }

    /// Which composed sub-account owns a composed `CursorScope::Folder`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum DavScopeOwner {
        Contacts,
        Calendars,
    }

    /// Ownership index for legacy bare folder scopes from composed DAV accounts.
    ///
    /// `FolderId` carries no protocol namespace and an IMAP mailbox name is an
    /// arbitrary string, so "this folder id appears in the CardDAV href set" is
    /// NOT by itself evidence that the scope belongs to CardDAV: a mailbox named
    /// exactly like a collection href would be delegated away and never sync as
    /// mail. Rather than leave that to a membership test at every routing call,
    /// the index is built once at open and made unambiguous there:
    ///
    /// - an id equal to a known IMAP mailbox name is NOT indexed, so the mailbox
    ///   keeps its own scope and DAV can never steal it;
    /// - an id claimed by both sub-accounts is NOT indexed, so the tie is never
    ///   resolved silently in favour of contacts.
    ///
    /// Both exclusions are classified open-time skips. Discovery filters the
    /// losing legacy collections instead of scheduling work against another owner.
    ///
    /// **EVERY `CursorScope::Folder` consumer must consult this index**, not only
    /// the four sync lanes. That is the sharpest lesson this seam has paid for: a
    /// round routed the sync lanes correctly and missed `push_subscribe`, a fifth
    /// consumer, so a DAV collection href - a syntactically valid mailbox name -
    /// was admitted as an IDLE mailbox watch. It cost no data (polling is never
    /// suppressed on push coverage) but it was reported in the succeeded lane that
    /// `bifrost-sync` trusts, it burned one of four IDLE budget slots, and it
    /// handed a worker a SELECT that can never succeed. A NEW consumer of `Folder`
    /// scopes must be checked against this index before it ships.
    ///
    /// Newly discovered DAV collections use FolderType with Contact or CalendarEvent,
    /// so equal URL strings cannot collide with mail or the other DAV dialect.
    /// This index retains legacy Folder routing. Losing legacy collections are
    /// classified open-time skips and are filtered from cursor discovery.
    #[derive(Debug, Default)]
    pub(crate) struct DavScopeIndex {
        owners: std::collections::HashMap<bifrost_types::FolderId, DavScopeOwner>,
    }

    impl DavScopeIndex {
        /// Admit PIM containers through the same ownership boundary as sync.
        /// Enterprise users expect a calendar or address book destination to be
        /// refused locally before any IMAP copy, search or hierarchy command.
        pub(super) fn imap_container(
            &self,
            container: &bifrost_types::ContainerId,
            op: AccountOperation,
        ) -> Result<ImapFolder, AccountError> {
            let folder = bifrost_types::FolderId(container.0.clone());
            if self.owner(&folder).is_some() {
                return Err(error::unsupported(op));
            }
            MailboxName::from_account_id(&container.0)
                .map(ImapFolder)
                .map_err(|e| error::pim_malformed(op, e.to_string()))
        }

        /// Mail membership must use the same ownership and canonical identity as
        /// sync routing. Enterprise users expect a mail move into an address book
        /// or calendar to fail locally, never to create mail effects at that URL.
        pub(crate) fn imap_mailbox(&self, folder: &bifrost_types::FolderId) -> Option<ImapFolder> {
            // Container admission owns DAV exclusion and mailbox parsing. The
            // operation only labels the error, which membership callers discard.
            self.imap_container(
                &bifrost_types::ContainerId(folder.0.clone()),
                AccountOperation::BulkMove,
            )
            .ok()
        }

        pub(crate) fn build(
            contact_folders: Vec<bifrost_types::FolderId>,
            calendar_folders: Vec<bifrost_types::FolderId>,
            imap_mailboxes: &std::collections::HashSet<String>,
        ) -> (Self, Vec<bifrost_types::SkippedScope>) {
            let contested: std::collections::HashSet<&bifrost_types::FolderId> = contact_folders
                .iter()
                .filter(|folder| calendar_folders.contains(folder))
                .collect();
            let mut owners = std::collections::HashMap::new();
            let mut skipped = Vec::new();
            let candidates = contact_folders
                .iter()
                .map(|folder| (folder, DavScopeOwner::Contacts))
                .chain(
                    calendar_folders
                        .iter()
                        .map(|folder| (folder, DavScopeOwner::Calendars)),
                );
            for (folder, owner) in candidates {
                if imap_mailboxes.contains(&folder.0) {
                    skipped.push(collision_skip(
                        owner,
                        format!(
                            "composed DAV collection {:?} has the same identity as an IMAP \
                             mailbox; the IMAP mailbox keeps the scope and this collection \
                             will not sync",
                            folder.0
                        ),
                    ));
                    continue;
                }
                if contested.contains(folder) {
                    // Each losing sub-account reports its own collection: the
                    // CardDAV half under ContactCollection, the CalDAV half under
                    // CalendarCollection, so neither gap hides behind the other.
                    skipped.push(collision_skip(
                        owner,
                        format!(
                            "composed DAV collection {:?} is claimed by both the CardDAV \
                             and the CalDAV sub-account; neither will sync it",
                            folder.0
                        ),
                    ));
                    continue;
                }
                owners.insert(folder.clone(), owner);
            }
            (Self { owners }, skipped)
        }

        pub(crate) fn owner(&self, folder: &bifrost_types::FolderId) -> Option<DavScopeOwner> {
            self.owners.get(folder).copied()
        }
    }

    // A collection excluded from routing must be visible as a classified gap:
    // enterprise users expect a missing address book or calendar to be reported
    // at open. It is named under the LOSING sub-account's collection scope, never
    // `Cursor(Folder(_))`: in the mailbox-collision case that folder scope is the
    // IMAP mailbox's own, which does sync, and a consumer would read the mailbox
    // as skipped. The collection URL rides in support-only detail. The cause is a
    // configuration conflict (two sources claiming one identity), which an
    // administrator fixes, so it classifies as `Request(Malformed)` - the same
    // answer Graph gives an unconfigured shared mailbox - not `Internal`, which
    // would ask the user to report a bifrost bug.
    fn collision_skip(owner: DavScopeOwner, detail: String) -> bifrost_types::SkippedScope {
        let scope = match owner {
            DavScopeOwner::Contacts => bifrost_types::ErrorScope::ContactCollection,
            DavScopeOwner::Calendars => bifrost_types::ErrorScope::CalendarCollection,
        };
        bifrost_types::SkippedScope {
            scope: scope.clone(),
            error: account_error_with(
                Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(detail.into())),
                error::ImapErrorContext::operation(AccountOperation::Discover).with_scope(scope),
            ),
        }
    }

    /// An IMAP mailbox admitted by the scope router, retained across task spawning.
    /// Enterprise users expect DAV folders never to become IMAP SELECT targets.
    #[derive(Debug, Clone)]
    pub(crate) struct ImapFolder(MailboxName);

    impl ImapFolder {
        pub(crate) fn into_mailbox(self) -> MailboxName {
            self.0
        }
    }

    impl std::ops::Deref for ImapFolder {
        type Target = MailboxName;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    /// Where a `CursorScope`'s sync work is serviced: IMAP itself for a
    /// folder scope, or a composed sub-account for a typed scope.
    pub(crate) enum ScopeHandler<'a> {
        /// IMAP itself owns this folder scope.
        Folder(ImapFolder),
        /// A composed sub-account owns this typed scope.
        Delegate(&'a Arc<dyn Account>),
    }

    /// Resolve a `CursorScope` to the handler that services its sync work.
    ///
    /// `Folder(_)` scopes route to IMAP via `folder_from_scope`. The two
    /// typed scopes IMAP can compose - `Type(Contact)` and
    /// `Type(CalendarEvent)` - route to the matching sub-account when one is
    /// attached, else `Unsupported`. Every other scope (including the
    /// unrelated `FolderType` object types) falls through to `Unsupported`, preserving the
    /// non-exhaustive guarantee.
    pub(crate) fn route_scope<'a>(
        account: &'a ImapAccount,
        scope: &CursorScope,
        op: bifrost_types::AccountOperation,
    ) -> Result<ScopeHandler<'a>, AccountError> {
        if let CursorScope::Folder(folder) = scope {
            // The index is disjoint from the IMAP mailbox set and from itself by
            // construction (see `DavScopeIndex::build`), so a hit here can never
            // be a real IMAP mailbox and can never be claimed by both sub-accounts.
            match account.dav_scopes.owner(folder) {
                Some(DavScopeOwner::Contacts) => {
                    return account
                        .contacts
                        .as_ref()
                        .map(ScopeHandler::Delegate)
                        .ok_or_else(|| error::unsupported(op));
                }
                Some(DavScopeOwner::Calendars) => {
                    return account
                        .calendars
                        .as_ref()
                        .map(ScopeHandler::Delegate)
                        .ok_or_else(|| error::unsupported(op));
                }
                None => {}
            }
        }
        route_typed_scope(
            scope,
            account.contacts.as_ref(),
            account.calendars.as_ref(),
            op,
        )
    }

    // Ownership was checked by route_scope before this private typed dispatch.
    fn route_typed_scope<'a>(
        scope: &CursorScope,
        contacts: Option<&'a Arc<dyn Account>>,
        calendars: Option<&'a Arc<dyn Account>>,
        op: bifrost_types::AccountOperation,
    ) -> Result<ScopeHandler<'a>, AccountError> {
        use bifrost_types::account_compose::{ScopeTarget, route_typed_scope as route_generic};

        // The generic helper decides self-vs-delegate keyed only on the
        // scope; IMAP maps the `This` case onto its own folder handler
        // (validating the mailbox name) and mints the `Unsupported` error in
        // its own vocabulary when a typed scope has no matching sub.
        match route_generic(scope, contacts, calendars) {
            Some(ScopeTarget::This) => Ok(ScopeHandler::Folder(ImapFolder(folder_from_scope(
                scope, op,
            )?))),
            Some(ScopeTarget::Delegate(sub)) => Ok(ScopeHandler::Delegate(sub)),
            None => Err(error::unsupported(op)),
        }
    }

    #[cfg(test)]
    pub(super) fn test_folder_from_scope(
        scope: &CursorScope,
        op: bifrost_types::AccountOperation,
    ) -> Result<MailboxName, AccountError> {
        folder_from_scope(scope, op)
    }

    #[cfg(test)]
    pub(super) fn test_route_typed_scope<'a>(
        scope: &CursorScope,
        contacts: Option<&'a Arc<dyn Account>>,
        calendars: Option<&'a Arc<dyn Account>>,
        op: bifrost_types::AccountOperation,
    ) -> Result<ScopeHandler<'a>, AccountError> {
        route_typed_scope(scope, contacts, calendars, op)
    }
}

pub(crate) fn folder_scope(folder: &MailboxName) -> CursorScope {
    CursorScope::Folder(bifrost_types::FolderId(folder.account_id()))
}

pub(crate) fn membership_scope(folder: &MailboxName) -> MembershipScope {
    MembershipScope::Folder(bifrost_types::FolderId(folder.account_id()))
}

#[cfg(test)]
mod throttle_key_tests {
    use bifrost_types::{
        Account, AccountId, CursorScope, FolderId, MembershipScope, ThrottleKey, ThrottleWork,
    };

    use super::test_support::{StubAccount, scripted_account_parts, stub_arc};
    use super::{DavScopeIndex, ImapAccount, ImapAccountParts};
    use crate::connection::test_support::{driver_pair, preauth_greeting};
    use crate::types::MailboxName;

    /// Every shape of work on one folder declares the SAME key, and it is
    /// the key a folder-scoped `[LIMIT]` records: `ThrottleKey::Folder`
    /// over the folder's name, under the account id the engine passes in.
    /// A composed DAV collection is never mistaken for an IMAP mailbox.
    #[tokio::test]
    async fn every_work_shape_on_a_folder_declares_the_key_its_limit_records() {
        let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        drop(server);
        let dav = "https://dav.example.test/books/";
        let (dav_scopes, _) = DavScopeIndex::build(
            vec![FolderId(dav.into())],
            Vec::new(),
            &std::collections::HashSet::from(["INBOX".to_string()]),
        );
        let account = ImapAccount::new(ImapAccountParts {
            contacts: Some(stub_arc(StubAccount::new(Vec::new()))),
            dav_scopes,
            ..scripted_account_parts(conn, 1)
        });
        let id = AccountId("imap".into());
        let inbox = vec![ThrottleKey::Folder {
            account: id.clone(),
            folder: FolderId("INBOX".into()),
        }];

        let scope = CursorScope::Folder(FolderId("INBOX".into()));
        assert_eq!(
            account.throttle_keys(&id, ThrottleWork::Scope(&scope)),
            inbox
        );
        let object = super::envelope::encode_object_id(
            &MailboxName::new("INBOX").expect("valid mailbox"),
            crate::types::UidValidity::new(7).expect("nonzero epoch"),
            crate::types::Uid::new(42).expect("valid protocol test value"),
        );
        assert_eq!(
            account.throttle_keys(&id, ThrottleWork::Objects(&[object])),
            inbox
        );
        let folder = MembershipScope::Folder(FolderId("INBOX".into()));
        assert_eq!(
            account.throttle_keys(&id, ThrottleWork::Membership(&folder)),
            inbox
        );

        let dav_scope = CursorScope::Folder(FolderId(dav.into()));
        assert!(
            account
                .throttle_keys(&id, ThrottleWork::Scope(&dav_scope))
                .is_empty(),
            "a DAV collection answers for its sub-account, not as an IMAP mailbox"
        );
        let dav_folder = MembershipScope::Folder(FolderId(dav.into()));
        assert!(
            account
                .throttle_keys(&id, ThrottleWork::Membership(&dav_folder))
                .is_empty()
        );
    }
}

#[cfg(test)]
mod router_tests {
    use bifrost_types::{AccountErrorKind, AccountOperation, CursorScope, FolderId, ObjectType};
    use std::sync::Arc;

    use super::test_support::{StubAccount, stub_arc};
    use super::{DavScopeIndex, DavScopeOwner, ScopeHandler, route_typed_scope};

    fn folder(name: &str) -> FolderId {
        FolderId(name.to_string())
    }

    fn mailboxes(names: &[&str]) -> std::collections::HashSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    /// A DAV collection href that is ALSO an IMAP mailbox name never enters
    /// the index, so the mailbox keeps its own scope.
    ///
    /// `FolderId` has no protocol namespace and IMAP mailbox names are
    /// arbitrary strings, so without this exclusion a mailbox named exactly
    /// like a collection href is delegated to DAV and its mail never syncs.
    #[test]
    fn a_dav_collection_never_captures_an_imap_mailbox_of_the_same_name() {
        let contested = "https://dav.example.test/books/work/";
        let (index, warnings) = DavScopeIndex::build(
            vec![folder(contested)],
            Vec::new(),
            &mailboxes(&["INBOX", contested]),
        );

        assert_eq!(index.owner(&folder(contested)), None);
        assert_eq!(warnings.len(), 1, "the drop must be reported, not silent");
        // Named under the losing collection, never the mailbox's own folder
        // scope (which does sync), and classified as configuration, not a bug.
        assert_eq!(
            warnings[0].scope,
            bifrost_types::ErrorScope::ContactCollection
        );
        assert_eq!(
            warnings[0].error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
    }

    /// A collection both sub-accounts claim is dropped rather than resolved
    /// in favour of contacts.
    #[test]
    fn a_collection_claimed_by_both_sub_accounts_is_not_silently_given_to_contacts() {
        let contested = "https://dav.example.test/shared/";
        let (index, warnings) = DavScopeIndex::build(
            vec![folder(contested), folder("https://dav.example.test/books/")],
            vec![folder(contested), folder("https://dav.example.test/cal/")],
            &mailboxes(&["INBOX"]),
        );

        assert_eq!(index.owner(&folder(contested)), None);
        assert_eq!(
            index.owner(&folder("https://dav.example.test/books/")),
            Some(DavScopeOwner::Contacts)
        );
        assert_eq!(
            index.owner(&folder("https://dav.example.test/cal/")),
            Some(DavScopeOwner::Calendars)
        );
        // Each losing sub-account reports the gap under its own collection.
        let scopes: Vec<_> = warnings.iter().map(|skip| skip.scope.clone()).collect();
        assert_eq!(
            scopes,
            vec![
                bifrost_types::ErrorScope::ContactCollection,
                bifrost_types::ErrorScope::CalendarCollection,
            ],
            "the contested collection is reported once per losing sub-account"
        );
    }

    #[test]
    fn uncontested_dav_collections_index_cleanly() {
        let (index, warnings) = DavScopeIndex::build(
            vec![folder("https://dav.example.test/books/work/")],
            vec![folder("https://dav.example.test/cal/work/")],
            &mailboxes(&["INBOX", "Archive"]),
        );

        assert!(warnings.is_empty());
        assert_eq!(
            index.owner(&folder("https://dav.example.test/books/work/")),
            Some(DavScopeOwner::Contacts)
        );
        assert_eq!(index.owner(&folder("INBOX")), None);
    }

    #[test]
    fn equal_strings_keep_mail_contacts_and_calendars_independent() {
        let contacts = stub_arc(StubAccount::new(Vec::new()));
        let calendars = stub_arc(StubAccount::new(Vec::new()));
        let folder = FolderId("https://dav.example.test/shared/".into());
        for (ty, expected) in [
            (ObjectType::Contact, &contacts),
            (ObjectType::CalendarEvent, &calendars),
        ] {
            let scope = CursorScope::FolderType {
                folder: folder.clone(),
                ty,
            };
            let handler = route_typed_scope(
                &scope,
                Some(&contacts),
                Some(&calendars),
                AccountOperation::SyncChanges,
            )
            .unwrap();
            let ScopeHandler::Delegate(actual) = handler else {
                panic!("DAV collection must delegate")
            };
            assert!(Arc::ptr_eq(actual, expected));
            assert!(route_typed_scope(&scope, None, None, AccountOperation::SyncChanges).is_err());
        }
        assert!(matches!(
            route_typed_scope(
                &CursorScope::Folder(folder),
                Some(&contacts),
                Some(&calendars),
                AccountOperation::SyncChanges
            )
            .unwrap(),
            ScopeHandler::Folder(_)
        ));
    }

    #[test]
    fn route_scope_routes_folder_to_self() {
        let scope = CursorScope::Folder(FolderId("INBOX".to_string()));
        let handler = route_typed_scope(&scope, None, None, AccountOperation::SyncChanges)
            .expect("folder scope routes");
        match handler {
            ScopeHandler::Folder(name) => assert_eq!(name.as_str(), "INBOX"),
            ScopeHandler::Delegate(_) => panic!("folder scope must not delegate"),
        }
    }

    #[test]
    fn route_scope_delegates_contact_to_contacts_sub() {
        let contacts = stub_arc(StubAccount::new(Vec::new()));
        let scope = CursorScope::Type(ObjectType::Contact);
        let handler =
            route_typed_scope(&scope, Some(&contacts), None, AccountOperation::SyncChanges)
                .expect("contact scope routes");
        assert!(
            matches!(handler, ScopeHandler::Delegate(_)),
            "contact scope with a contacts sub must delegate",
        );
    }

    #[test]
    fn route_scope_delegates_calendar_event_to_calendars_sub() {
        let calendars = stub_arc(StubAccount::new(Vec::new()));
        let scope = CursorScope::Type(ObjectType::CalendarEvent);
        let handler = route_typed_scope(
            &scope,
            None,
            Some(&calendars),
            AccountOperation::SyncChanges,
        )
        .expect("calendar-event scope routes");
        assert!(matches!(handler, ScopeHandler::Delegate(_)));
    }

    #[test]
    fn route_scope_unsupported_when_no_matching_sub() {
        let scope = CursorScope::Type(ObjectType::Contact);
        let result = route_typed_scope(&scope, None, None, AccountOperation::SyncChanges);
        let Err(err) = result else {
            panic!("typed scope without a sub must be unsupported");
        };
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::SyncChanges)
        );
    }

    // Brick 3: dispatch. `inventory_stream` / `changes_stream` route the
    // scope and, on `Delegate`, hand the entire call to the sub-account.
    // Exercising the routing + delegation directly avoids standing up a
    // live `ImapAccount` (which needs a real connection) while still
    // pinning that a typed scope reaches the sub and a typed scope with
    // no sub errors `Unsupported`.
    #[tokio::test]
    async fn delegates_typed_scope_inventory_to_sub() {
        use futures::StreamExt;

        let contacts = stub_arc(StubAccount::new(Vec::new()));
        let scope = CursorScope::Type(ObjectType::Contact);
        let handler = route_typed_scope(
            &scope,
            Some(&contacts),
            None,
            AccountOperation::SyncInventory,
        )
        .expect("contact scope routes to delegate");
        let ScopeHandler::Delegate(sub) = handler else {
            panic!("expected delegate");
        };
        let mut stream = sub.inventory_stream(scope);
        let first = stream.next().await.expect("delegated batch");
        match first {
            bifrost_types::InventoryEvent::Batch(batch) => {
                assert_eq!(
                    batch.items.first().map(|entry| entry.id.0.as_str()),
                    Some(super::test_support::STUB_SENTINEL),
                    "delegated inventory must carry the sub-account's sentinel",
                );
            }
            other => panic!("expected delegated batch, got {other:?}"),
        }
    }

    #[test]
    fn delegates_typed_scope_unsupported_without_sub() {
        let scope = CursorScope::Type(ObjectType::CalendarEvent);
        let result = route_typed_scope(&scope, None, None, AccountOperation::SyncInventory);
        let Err(err) = result else {
            panic!("typed scope without a calendars sub must be unsupported");
        };
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::SyncInventory)
        );
    }

    // A non-folder, non-composable scope must not silently resolve to a
    // folder named after it; it is honestly Unsupported.
    #[test]
    fn account_scope_is_unsupported_for_imap() {
        let err = super::folder_from_scope(&CursorScope::Account, AccountOperation::SyncChanges)
            .expect_err("an account-wide scope has no IMAP mailbox");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::SyncChanges)
        );
    }

    // The folder scope string and the membership scope string are the
    // same native mailbox path; the container projection joins on it.
    #[test]
    fn folder_and_membership_scopes_agree_on_the_native_path() {
        let folder = crate::types::MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        assert_eq!(
            super::folder_scope(&folder),
            CursorScope::Folder(FolderId("Shared/alice/Reports".to_string()))
        );
        assert_eq!(
            super::membership_scope(&folder),
            bifrost_types::MembershipScope::Folder(FolderId("Shared/alice/Reports".to_string()))
        );
    }

    // A mailbox name with a NUL or CRLF cannot be sent on the wire, so a
    // scope carrying one is a malformed request, not a folder handler.
    #[test]
    fn a_scope_with_an_unsendable_mailbox_name_is_malformed() {
        let scope = CursorScope::Folder(FolderId("IN\r\nBOX".to_string()));
        let err = super::folder_from_scope(&scope, AccountOperation::SyncChanges)
            .expect_err("CRLF in a mailbox name must be rejected");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
    }
}

#[cfg(test)]
mod guarded_stream_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use futures::StreamExt as _;

    use super::factory::ImapAccountConfig;
    use super::{Pool, guarded_receiver_stream};
    use crate::connection::test_support::{driver_pair, preauth_greeting};
    use crate::types::{AuthPolicy, Credentials};

    fn pool_with(primed: crate::ImapConnection, pool_cap: usize) -> Arc<Pool> {
        let config = Arc::new(ImapAccountConfig {
            pool_cap,
            ..ImapAccountConfig::new(
                crate::ImapConfig::plaintext("test.invalid"),
                Credentials::password("user", "pass"),
                AuthPolicy::default(),
            )
        });
        Arc::new(Pool::new(
            config,
            primed,
            crate::account::test_support::refusing_connector(),
            None,
            bifrost_net::BandwidthBudget::default(),
        ))
    }

    /// Dropping a guarded stream must stop the task feeding it AND give
    /// the pooled connection back, not leave a detached driver running
    /// against the account while the engine starts its successor.
    ///
    /// The pool allows one physical session and refuses fresh dials, so
    /// the second checkout succeeds only if the aborted task returns its
    /// connection. Without the abort the task holds it forever and this
    /// checkout never returns.
    #[tokio::test]
    async fn dropping_a_guarded_stream_aborts_its_task_and_returns_the_connection() {
        let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        let pool = pool_with(conn, 1);

        let (tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
        let task_pool = Arc::clone(&pool);
        let task = tokio::spawn(async move {
            let _conn = task_pool.checkout_any().await.expect("primed checkout");
            // Announce the checkout, then park forever: this is the
            // shape of a changes/inventory task mid-poll.
            tx.send(1).await.ok();
            std::future::pending::<()>().await;
        });

        let mut stream = guarded_receiver_stream(rx, task);
        assert_eq!(
            stream.next().await,
            Some(1),
            "the task must have taken the pooled connection before the drop",
        );
        drop(stream);

        let reclaimed = tokio::time::timeout(Duration::from_secs(5), pool.checkout_any())
            .await
            .expect("a dropped stream must release the task's pooled connection");
        assert!(
            reclaimed.is_ok(),
            "the reclaimed connection must still be checkout-able",
        );
    }
}

#[cfg(test)]
mod directory_forwarding_tests {
    use super::test_support::{
        DirectoryCall, StubAccount, scripted_account_parts, stub_capabilities,
    };
    use super::*;
    use crate::connection::test_support::{driver_pair, preauth_greeting};

    #[tokio::test]
    async fn directory_search_forwards_arguments_and_page_from_carddav() {
        let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        drop(server);
        let mut caps = stub_capabilities();
        caps.pim_methods.directory_search = true;
        let sub = Arc::new(StubAccount::with_capabilities(caps));
        let account = ImapAccount::new(ImapAccountParts {
            contacts: Some(Arc::clone(&sub) as Arc<dyn Account>),
            ..scripted_account_parts(conn, 1)
        });
        let page = account
            .directory_search("needle".into(), Some(17), Some(vec![4, 2].into()))
            .await
            .expect("forward");
        assert_eq!(page.items[0].email, "directory-sentinel@example.test");
        assert!(page.truncated);
        assert_eq!(
            *sub.directory_call.lock().expect("lock"),
            Some(DirectoryCall {
                query: "needle".into(),
                limit: Some(17),
                cursor: Some(vec![4, 2].into()),
            })
        );
    }
}

#[cfg(test)]
mod batch_metering_tests {
    use super::*;

    #[test]
    fn batch_carries_the_measured_inbound_bytes() {
        let event = batch(vec![7], bifrost_types::PageBoundary::Page, None, 23);
        let SyncEvent::Batch(batch) = event else {
            panic!("expected a batch")
        };
        assert_eq!(batch.bytes_in, 23);
    }

    #[test]
    fn measured_byte_delta_advances_each_batch_boundary() {
        let mut previous = 10;
        assert_eq!(measured_bytes_delta(31, &mut previous), 21);
        assert_eq!(previous, 31);
        assert_eq!(measured_bytes_delta(47, &mut previous), 16);
    }
}
