//! IMAP implementation of the shared `Account` trait.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
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
    SearchRequest, SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId, VacationConfig,
    WatchEvent,
};
use bifrost_types::{AddressBook, AddressBookId};
use futures::stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::types::{MailboxName, SyncSelectOptions, SyncSelectResult, UidValidity};

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
pub use sieve::ManageSieveConfig;
// pub: consumers configure IMAP submission (SMTP send) through these.
pub use submission::{SmtpSubmissionConfig, SubmissionCredentials, SubmissionTls};

pub(crate) use submission::SubmissionTransport;

pub(crate) use envelope::{
    DecodedObjectId, FolderCursor, decode_cursor, decode_object_id, decode_thread_id,
    encode_cursor, encode_object_id, encode_thread_id,
};
pub(crate) use folder_registry::{CompactUidSet, FolderRegistry};
pub(crate) use pool::{Pool, PooledConn};

const STREAM_CAPACITY: usize = 64;
const BATCH_ITEMS: usize = 128;
/// Maximum decoded target ids retained before a get or mutation stream flushes.
const TARGET_BUFFER_ITEMS: usize = 256;
const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

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
    pub(crate) qresync_negotiation_warning: Option<String>,
    pub(crate) qresync_negotiation_warning_sent: AtomicBool,
    pub(crate) supports_notify: bool,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: AtomicBool,
    pub(crate) priority: AtomicU8,
    pub(crate) bandwidth_cap: Arc<AtomicU64>,
    pub(crate) push: push::PushState,
    pub(crate) contacts: Option<Arc<dyn Account>>,
    pub(crate) calendars: Option<Arc<dyn Account>>,
    /// Collision-free ownership index for composed DAV folder scopes.
    pub(crate) dav_scopes: DavScopeIndex,
    pub(crate) submission: Option<Arc<SubmissionTransport>>,
    /// Warnings recorded at open when a configured DAV sub-account could
    /// not be attached (brick 5 fail-soft). Read (not drained) by every
    /// `discover_cursor_scopes` and cleared only once a discovery stream
    /// has actually yielded its terminal `Done` to the consumer, so the
    /// engine observes the degradation without taking mail offline and
    /// cannot lose it to a discovery stream that is dropped un-polled.
    pub(crate) dav_degraded: std::sync::Mutex<Vec<bifrost_types::Warning>>,
}

pub(crate) struct ImapAccountParts {
    pub(crate) config: Arc<ImapAccountConfig>,
    pub(crate) capabilities: bifrost_types::AccountCapabilities,
    pub(crate) pool: Arc<Pool>,
    pub(crate) folders: Arc<FolderRegistry>,
    pub(crate) qresync_enabled: bool,
    pub(crate) qresync_negotiation_warning: Option<String>,
    pub(crate) supports_notify: bool,
    pub(crate) bandwidth_cap: Arc<AtomicU64>,
    pub(crate) contacts: Option<Arc<dyn Account>>,
    pub(crate) calendars: Option<Arc<dyn Account>>,
    /// Collision-free ownership index for composed DAV folder scopes.
    pub(crate) dav_scopes: DavScopeIndex,
    pub(crate) submission: Option<Arc<SubmissionTransport>>,
    pub(crate) dav_degraded: Vec<bifrost_types::Warning>,
}

impl ImapAccount {
    pub(crate) fn new(parts: ImapAccountParts) -> Self {
        let push = push::PushState::new(parts.supports_notify, parts.config.idle_connection_budget);
        // The QRESYNC policy lives on the pool, which reads it at every dial
        // to bring a new member to the account's QRESYNC state. The pool
        // dials nothing before the account exists, so seeding it here is
        // seen by every dial.
        parts.pool.set_qresync_policy(parts.qresync_enabled);
        Self {
            inner: Arc::new(ImapAccountInner {
                config: parts.config,
                capabilities: parts.capabilities,
                pool: parts.pool,
                folders: parts.folders,
                qresync_negotiation_warning: parts.qresync_negotiation_warning,
                qresync_negotiation_warning_sent: AtomicBool::new(false),
                supports_notify: parts.supports_notify,
                shutdown: CancellationToken::new(),
                closed: AtomicBool::new(false),
                priority: AtomicU8::new(Priority::Normal as u8),
                bandwidth_cap: parts.bandwidth_cap,
                push,
                contacts: parts.contacts,
                calendars: parts.calendars,
                dav_scopes: parts.dav_scopes,
                submission: parts.submission,
                dav_degraded: std::sync::Mutex::new(parts.dav_degraded),
            }),
        }
    }

    /// Read the degraded-DAV warnings recorded at open WITHOUT consuming
    /// them.
    ///
    /// These warnings are the only signal that a configured CardDAV or
    /// CalDAV half failed to attach and the account is presenting as
    /// mail-only. Draining them at stream-construction time put them
    /// inside a future the consumer is free to drop un-polled, and unlike
    /// the QRESYNC one-shot there is no second copy to fall back on: the
    /// account would then look healthy forever. So the read is
    /// non-consuming and `clear_dav_degraded_warnings` is the
    /// acknowledgement, fired only when a discovery stream has handed its
    /// terminal event to the consumer.
    pub(crate) fn peek_dav_degraded_warnings(&self) -> Vec<bifrost_types::Warning> {
        self.dav_degraded
            .lock()
            .expect("dav_degraded lock poisoned")
            .clone()
    }

    /// Acknowledge delivery of the degraded-DAV warnings. Called once a
    /// discovery stream has yielded them plus its terminal `Done`, so a
    /// later discovery on the same account does not re-report a
    /// degradation the consumer already saw.
    pub(crate) fn clear_dav_degraded_warnings(&self) {
        self.dav_degraded
            .lock()
            .expect("dav_degraded lock poisoned")
            .clear();
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

    pub(crate) async fn select_folder(
        &self,
        conn: &mut PooledConn,
        folder: &MailboxName,
        cursor: Option<&FolderCursor>,
        read_only: bool,
    ) -> Result<SyncSelectResult, Error> {
        let options = self.select_options(cursor, read_only);
        let result = match conn
            .connection()
            .select_for_sync(folder.as_str(), &options, self.command_timeout())
            .await
        {
            Ok(result) => result,
            Err(error) => {
                record_failed_select(conn, folder, &error);
                return Err(error);
            }
        };
        conn.set_selected(folder.clone());
        self.folders.mark_seen(folder);
        Ok(result)
    }

    fn select_options(&self, cursor: Option<&FolderCursor>, read_only: bool) -> SyncSelectOptions {
        let mut options = if read_only {
            SyncSelectOptions::read_only()
        } else {
            SyncSelectOptions::read_write()
        };

        if self.qresync_enabled()
            && let Some(FolderCursor::QResync {
                uidvalidity,
                modseq,
                known_uids,
                known_uids_complete,
            }) = cursor
            && let Some(validity) = UidValidity::new(*uidvalidity)
        {
            // Straight from the compact ranges: the baseline is already
            // sorted and coalesced, and on a large mailbox expanding it into
            // individual UIDs only to re-coalesce them is the entire cost of
            // building the QRESYNC operand.
            let known_uids = if *known_uids_complete {
                crate::types::UidSet::from_ranges(known_uids.ranges())
            } else {
                None
            };
            options =
                options.with_qresync(validity, crate::types::ModSeq::new(*modseq), known_uids);
        }
        options
    }

    pub(crate) fn cursor_from_select(
        &self,
        selected: &crate::types::SelectedMailbox,
        known_uids: Option<CompactUidSet>,
    ) -> Result<FolderCursor, Error> {
        let uidvalidity = selected_uidvalidity(selected)?;
        let known_uids = known_uids.unwrap_or_default();
        if self.qresync_enabled()
            && let Some(modseq) = selected.highest_mod_seq
            && !selected.no_mod_seq
        {
            return Ok(FolderCursor::QResync {
                uidvalidity,
                modseq,
                known_uids,
                known_uids_complete: true,
            });
        }
        if let Some(modseq) = selected.highest_mod_seq
            && !selected.no_mod_seq
        {
            return Ok(FolderCursor::Condstore {
                uidvalidity,
                modseq,
                known_uids,
            });
        }
        Ok(FolderCursor::Basic {
            uidvalidity,
            uidnext: selected.uid_next.unwrap_or_default(),
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

    pub(crate) fn disable_qresync_for_session(&self) {
        // Other folder syncs may already have passed their QRESYNC gate
        // on this account. They are allowed to finish or independently
        // downgrade; this one-way flag only prevents new QRESYNC work. It
        // does not undo QRESYNC on members already enabled: RFC 7162
        // Section 3.2.3 gives no way back short of closing the connection,
        // and every consumer reads the form a removal arrived in rather
        // than this policy.
        self.pool.set_qresync_policy(false);
    }

    pub(crate) fn take_qresync_negotiation_warning(&self) -> Option<String> {
        let warning = self.qresync_negotiation_warning.as_ref()?;
        if self
            .qresync_negotiation_warning_sent
            .swap(true, Ordering::Release)
        {
            return None;
        }
        Some(warning.clone())
    }

    /// Un-spend the QRESYNC negotiation one-shot.
    ///
    /// The one-shot is taken inside a spawned streaming task, so a stream
    /// dropped before the warning reaches the consumer would otherwise
    /// burn it with nobody informed. The taker holds a ticket that calls
    /// this if its output channel turns out to be gone.
    pub(crate) fn restore_qresync_negotiation_warning(&self) {
        self.qresync_negotiation_warning_sent
            .store(false, Ordering::Release);
    }

    /// Non-consuming read of the QRESYNC negotiation reason. Unlike
    /// `take_qresync_negotiation_warning` (a one-shot that fires the
    /// standalone account-level warning exactly once, to avoid per-folder
    /// spam), this lets every later QRESYNC->CONDSTORE downgrade reuse the
    /// specific session-level reason as its downgrade reason string rather
    /// than falling back to a generic message once the one-shot is spent.
    pub(crate) fn qresync_negotiation_reason(&self) -> Option<String> {
        self.qresync_negotiation_warning.clone()
    }
}

impl Account for ImapAccount {
    // Account: exposes the factory-built capability snapshot; direct users call server_profile().
    fn capabilities(&self) -> &bifrost_types::AccountCapabilities {
        &self.capabilities
    }

    // Account: stores the engine priority hint; direct ImapConnection calls remain caller scheduled.
    fn set_priority(&self, priority: Priority) {
        self.priority.store(priority as u8, Ordering::Release);
    }

    // Account: records the engine bandwidth cap until IMAP byte-metering is wired.
    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        let raw = match bps {
            None => UNLIMITED_BANDWIDTH,
            Some(0) => {
                tracing::warn!(
                    target: "bifrost_imap::bandwidth",
                    "set_bandwidth_cap(Some(0)) clamped to 1 B/s; use None for unlimited",
                );
                1
            }
            Some(n) => n,
        };
        self.bandwidth_cap.store(raw, Ordering::Release);
    }

    // Account: names the per-mailbox throttle keys engine work consumes; direct users pace themselves.
    //
    // IMAP's only narrower-than-account throttle is the per-mailbox one
    // `mailbox_throttle` raises for a folder-scoped `[LIMIT]`, which the
    // engine records as `ThrottleKey::Mailbox { account, mailbox }` keyed on
    // the folder's name. Every arm below builds that exact key from the same
    // `MailboxName::as_str()` the error producer scopes with, so a recorded
    // hold and the work it should hold meet. Routing follows `route_scope`:
    // a composed DAV collection answers for its sub-account, never as an
    // IMAP mailbox.
    fn throttle_keys(
        &self,
        account: &bifrost_types::AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        use bifrost_types::{ThrottleKey, ThrottleWork};
        let key = |mailbox: &str| ThrottleKey::Mailbox {
            account: account.clone(),
            mailbox: bifrost_types::MailboxId(mailbox.to_owned()),
        };
        match work {
            ThrottleWork::Scope(scope) => {
                match route_scope(self, scope, bifrost_types::AccountOperation::SyncChanges) {
                    Ok(ScopeHandler::Folder(mailbox)) => vec![key(mailbox.as_str())],
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
                        let candidate = key(decoded.folder.as_str());
                        if !keys.contains(&candidate) {
                            keys.push(candidate);
                        }
                    }
                }
                keys
            }
            ThrottleWork::Membership(MembershipScope::Folder(folder))
                if self.dav_scopes.owner(folder).is_none() =>
            {
                vec![key(&folder.0)]
            }
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

    // Account: streams folder lifecycle events from push state; direct users consume IDLE/NOTIFY.
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

    // Account: forwards IDLE/NOTIFY wakeups as WatchEvents; direct users call idle().
    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::push_stream(self.clone())
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
        keyword: String,
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

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::send_message(self.clone(), request)
    }

    fn send_raw_message(
        &self,
        raw: bytes::Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
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

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
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
        page_cursor: Option<Vec<u8>>,
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

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_search(request);
        }
        unsupported_future(AccountOperation::ContactSearch)
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        unsupported_future(AccountOperation::DirectorySearch)
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        unsupported_future(AccountOperation::DirectoryGroupsList)
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
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

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_search(request);
        }
        unsupported_future(AccountOperation::EventSearch)
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
    Box::pin(futures::stream::iter([
        SyncEvent::Terminated(error::unsupported(operation)),
        SyncEvent::Done(None),
    ]))
}

/// Bring a checkout's mailbox affinity in line with its session after
/// `select_for_sync` failed.
///
/// `PoolMember::selected` is what `deselect_target` reads to decide whether
/// an UNSELECT must precede a DELETE or RENAME, so a stale record makes it skip
/// one the server needs. What the session holds after a failure:
///
/// - Not `Selected`: nothing is selected. A tagged `NO` to SELECT/EXAMINE
///   deselects the previous mailbox (RFC 3501 6.3.1, RFC 9051 6.3.2), and the
///   driver moves the session to `Authenticated`. Recorded as `None`.
/// - `Selected` and the failure is `ProtocolMissing`: the SELECT got its
///   tagged OK (only a tagged OK makes the session `Selected` on the new
///   mailbox) and the response was then found incomplete, which leaves the
///   framing intact and the connection reusable. `folder` is selected.
/// - `Selected` otherwise: the previous selection stands. A tagged `BAD`
///   changes no state (RFC 3501 6), a refused QRESYNC `ENABLE` or a local
///   refusal never sent the SELECT, and the connection-fatal failures retire
///   the connection, so the record is never read again. Left as it was.
fn record_failed_select(conn: &mut PooledConn, folder: &MailboxName, error: &Error) {
    match conn.connection().session_state() {
        crate::connection::SessionState::Selected => {
            if matches!(error, Error::ProtocolMissing(_)) {
                conn.set_selected_affinity(Some(folder.clone()));
            }
        }
        _ => conn.set_selected_affinity(None),
    }
}

/// The UIDVALIDITY a completed SELECT or EXAMINE owes.
///
/// The server must send it (RFC 3501 6.3.1, RFC 9051 6.3.2), and the
/// connection layer hands the account layer an `Option` so this is where the
/// omission is judged. It is the SERVER's omission, so it is
/// `Error::ProtocolMissing` (`Protocol(MissingField)`,
/// `ProviderContractViolation`) at every account-layer read, never a
/// caller-blaming `Request(Malformed)`: the one place the classification
/// lives, so the sync lanes and the PIM primitives cannot drift apart again.
pub(crate) fn selected_uidvalidity(selected: &crate::types::SelectedMailbox) -> Result<u32, Error> {
    selected
        .uid_validity
        .ok_or_else(|| Error::ProtocolMissing("SELECT missing UIDVALIDITY".into()))
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
    Classify(Error, error::ImapErrorContext),
}

impl TerminatedCause {
    fn into_account_error(self) -> AccountError {
        match self {
            Self::Account(err) => err,
            Self::Classify(err, ctx) => error::into_account_error(err, ctx),
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
        Self::Classify(err, ctx)
    }
}

pub(crate) fn boxed_receiver_stream<T: Send + 'static>(
    rx: tokio::sync::mpsc::Receiver<T>,
) -> AccountStream<T> {
    Box::pin(ReceiverStream { rx })
}

/// A receiver stream that aborts the task feeding it when the stream is
/// dropped.
///
/// `Account::changes_stream`, `inventory_stream` and `get_stream` are
/// channel-backed by a detached `tokio::spawn`. Dropping the returned stream drops only the
/// receiver, so without this the task runs on against the account's
/// connection until its next `tx.send` fails - and the engine now cuts a
/// pending poll on every PAUSE, not only at teardown, then calls back in
/// on the same account. That let two drivers issue IMAP commands
/// concurrently over one account while the older one sat on a pool
/// checkout it would not release until it next tried to send.
///
/// Aborting is safe here precisely because the driver model already
/// handles an abandoned command: every submit helper holds an
/// `InFlightGuard` that marks the connection abandoned when its future is
/// dropped mid-command, and `PooledConn::drop` parks only a member that is
/// alive and not abandoned. So the abort returns the pooled connection (or
/// retires it) rather than leaking it.
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
) -> SyncEvent<T> {
    SyncEvent::Batch(bifrost_types::Batch {
        items,
        page_boundary: boundary,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint,
    })
}

pub(crate) fn folder_from_scope(
    scope: &CursorScope,
    op: bifrost_types::AccountOperation,
) -> Result<MailboxName, AccountError> {
    match scope {
        CursorScope::Folder(folder) => MailboxName::new(folder.0.clone()).map_err(|e| {
            error::into_account_error(
                Error::InvalidInput(e.to_string()),
                error::ImapErrorContext::operation(op),
            )
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

/// Ownership index for the folder scopes the composed DAV sub-accounts mint.
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
/// Both exclusions are reported as open-time warnings, which surface through
/// the first discovery stream alongside the other DAV degradations. A dropped
/// entry therefore fails loudly - the scope routes to IMAP or to
/// `Unsupported` - instead of syncing the wrong collection quietly.
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
/// Namespacing `FolderId` instead was considered and rejected: it would force
/// a rewrite of the scope on delegation and back out of the minted
/// `ChangeCursor`, and every one of those rewrite points is another place the
/// fix can open a hole.
///
/// Accepted residual: an excluded scope is still ADVERTISED by discovery,
/// because sub-account scopes pass through `merge_scope_streams` verbatim. A
/// mailbox collision resolves to the same scope value as the real mailbox, and
/// a contested id fails loudly at establishment. Judged honest and sufficient,
/// and warned at open.
#[derive(Debug, Default)]
pub(crate) struct DavScopeIndex {
    owners: std::collections::HashMap<bifrost_types::FolderId, DavScopeOwner>,
}

impl DavScopeIndex {
    pub(crate) fn build(
        contact_folders: Vec<bifrost_types::FolderId>,
        calendar_folders: Vec<bifrost_types::FolderId>,
        imap_mailboxes: &std::collections::HashSet<String>,
    ) -> (Self, Vec<bifrost_types::Warning>) {
        let contested: std::collections::HashSet<&bifrost_types::FolderId> = contact_folders
            .iter()
            .filter(|folder| calendar_folders.contains(folder))
            .collect();
        let mut owners = std::collections::HashMap::new();
        let mut warnings = Vec::new();
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
                warnings.push(bifrost_types::Warning::support_only(
                    bifrost_types::WarningKind::OperatorAttentionNeeded,
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
                // Emitted once, on the contacts pass, so the calendars pass
                // does not duplicate it.
                if owner == DavScopeOwner::Contacts {
                    warnings.push(bifrost_types::Warning::support_only(
                        bifrost_types::WarningKind::OperatorAttentionNeeded,
                        format!(
                            "composed DAV collection {:?} is claimed by both the CardDAV \
                             and the CalDAV sub-account; neither will sync it",
                            folder.0
                        ),
                    ));
                }
                continue;
            }
            owners.insert(folder.clone(), owner);
        }
        (Self { owners }, warnings)
    }

    pub(crate) fn owner(&self, folder: &bifrost_types::FolderId) -> Option<DavScopeOwner> {
        self.owners.get(folder).copied()
    }
}

/// Where a `CursorScope`'s sync work is serviced: IMAP itself for a
/// folder scope, or a composed sub-account for a typed scope.
pub(crate) enum ScopeHandler<'a> {
    /// IMAP itself owns this folder scope.
    Folder(MailboxName),
    /// A composed sub-account owns this typed scope.
    Delegate(&'a Arc<dyn Account>),
}

/// Resolve a `CursorScope` to the handler that services its sync work.
///
/// `Folder(_)` scopes route to IMAP via `folder_from_scope`. The two
/// typed scopes IMAP can compose - `Type(Contact)` and
/// `Type(CalendarEvent)` - route to the matching sub-account when one is
/// attached, else `Unsupported`. Every other scope (including the
/// `FolderType` struct variant, which no protocol composed under IMAP
/// mints today) falls through to `Unsupported`, preserving the
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

/// Pure, account-free core of [`route_scope`]. Keyed only on the scope
/// and the two optional sub-account handles so it tests in isolation and
/// is reusable beyond IMAP (brick 10's `compose` extraction calls this).
pub(crate) fn route_typed_scope<'a>(
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
        Some(ScopeTarget::This) => Ok(ScopeHandler::Folder(folder_from_scope(scope, op)?)),
        Some(ScopeTarget::Delegate(sub)) => Ok(ScopeHandler::Delegate(sub)),
        None => Err(error::unsupported(op)),
    }
}

pub(crate) fn folder_scope(folder: &MailboxName) -> CursorScope {
    CursorScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned()))
}

pub(crate) fn membership_scope(folder: &MailboxName) -> MembershipScope {
    MembershipScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned()))
}

#[cfg(test)]
mod throttle_key_tests {
    use bifrost_types::{
        Account, AccountId, CursorScope, FolderId, MailboxId, MembershipScope, ThrottleKey,
        ThrottleWork,
    };

    use super::test_support::{StubAccount, scripted_account_parts, stub_arc};
    use super::{DavScopeIndex, ImapAccount, ImapAccountParts};
    use crate::connection::test_support::{driver_pair, preauth_greeting};
    use crate::types::MailboxName;

    /// Every shape of work on one folder declares the SAME key, and it is
    /// the key a folder-scoped `[LIMIT]` records: `ThrottleKey::Mailbox`
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
        let inbox = vec![ThrottleKey::Mailbox {
            account: id.clone(),
            mailbox: MailboxId("INBOX".into()),
        }];

        let scope = CursorScope::Folder(FolderId("INBOX".into()));
        assert_eq!(
            account.throttle_keys(&id, ThrottleWork::Scope(&scope)),
            inbox
        );
        let object = super::envelope::encode_object_id(
            &MailboxName::new("INBOX").expect("valid mailbox"),
            7,
            42,
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
        assert_eq!(
            warnings.len(),
            1,
            "the contested collection is reported exactly once"
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
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use futures::StreamExt as _;

    use super::factory::ImapAccountConfig;
    use super::{Pool, guarded_receiver_stream};
    use crate::connection::test_support::{driver_pair, preauth_greeting};
    use crate::types::{AuthPolicy, Credentials};

    fn pool_with(primed: crate::ImapConnection, data_cap: usize) -> Arc<Pool> {
        let config = Arc::new(ImapAccountConfig {
            pool_cap: data_cap,
            ..ImapAccountConfig::new(
                crate::ImapConfig::plaintext("test.invalid"),
                Credentials::password("user", "pass"),
                AuthPolicy::default(),
            )
        });
        Arc::new(Pool::new(
            config,
            primed,
            data_cap,
            None,
            Arc::new(AtomicU64::new(0)),
        ))
    }

    /// Dropping a guarded stream must stop the task feeding it AND give
    /// the pooled connection back, not leave a detached driver running
    /// against the account while the engine starts its successor.
    ///
    /// The pool holds exactly one permit and cannot dial (the duplex
    /// transport has no peer to dial), so the second checkout succeeds
    /// only if the aborted task actually released both its permit and its
    /// connection. Without the abort the task parks forever holding both
    /// and this checkout never returns.
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
