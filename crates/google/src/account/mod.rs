mod blobs;
mod calendar;
mod capabilities;
#[cfg(test)]
mod capability_contract_tests;
mod changes;
mod cloud;
mod contacts;
mod cursor;
pub(crate) mod error;
mod filters;
mod flags;
mod inventory;
mod mutation;
mod pim;
mod push;
mod scopes;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountId,
    AccountOperation, AccountStream, AddressBook, AddressBookId, AttachmentHandle, BlobHandle,
    ByteRange, Calendar, CalendarEvent, Change, ChangeCursor, CloudUploadMeta, ContactCard,
    ContactCreate, ContactId, ContactPatch, ContactSearchRequest, ContainerId, ContainerKind,
    ContainerList, CostClass, CursorDescriptor, CursorEstablishment, CursorScope, DirectoryCard,
    DirectoryGroup, DirectoryGroupId, DirectoryGroupMember, DraftHandle, DraftPatch, EventCreate,
    EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation, FlagOp,
    HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, Importance, InventoryEvent, ItemOutcome, MembershipScope, Message,
    MutationSuccess, MutationTarget, ObjectId, OpaqueChangeState, OpenedAccount, Page, Priority,
    Projection, QuotaInfo, RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendRequest,
    ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch, SubscriptionHandle,
    SyncEvent, SyncStrategy, ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use bifrost_net::TokenSource;

use crate::client::GmailClient;
use crate::types::GmailProfile;

pub use push::PubSubConfig;

use self::capabilities::gmail_capabilities;
use self::cursor::{
    GmailChangeState, cursor_from_state, decode_gmail_state_for_profile, encode_gmail_state,
};
use self::push::PubSubControl;
use self::scopes::{ScopeCache, ScopeCacheState, ScopeSnapshot};

fn non_empty<T>(iter: impl Iterator<Item = T>) -> Option<Vec<T>> {
    let values = iter.collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

/// Factory for opening Google accounts through the shared `Account` API.
pub struct GoogleAccountFactory {
    client: Arc<GmailClient>,
    pubsub: Option<PubSubConfig>,
}

impl GoogleAccountFactory {
    /// Group Gmail quota holds by the quota-consuming Cloud project.
    /// Use the canonical project NUMBER consistently across every factory.
    /// This identity does not send a billing or quota-project header.
    ///
    /// # Errors
    /// Rejects an empty or whitespace-only identity.
    pub fn with_quota_project(mut self, id: impl Into<String>) -> Result<Self, AccountError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(error::into_account_error(
                crate::Error::invalid_request(
                    AccountOperation::Discover,
                    "quota project must not be empty",
                ),
                error::GmailErrorContext::open(),
            ));
        }
        self.client = Arc::new(self.client.with_quota_project(id));
        Ok(self)
    }

    fn from_client(client: GmailClient) -> Self {
        Self {
            client: Arc::new(client),
            pubsub: None,
        }
    }

    /// Construct a Google factory from a bearer access token.
    #[must_use]
    pub fn from_access_token(access_token: impl Into<String>) -> Self {
        Self::from_client(GmailClient::new(access_token))
    }

    /// Construct a Google factory from a shared token source. ratatoskr
    /// supplies an `Arc<dyn TokenSource>` (typically an `OAuthRefresher`
    /// over its own refresh-token store) so a refreshed-and-persisted
    /// token is read live at every Gmail/People request without reopen.
    #[must_use]
    pub fn from_token_source(source: Arc<dyn TokenSource>) -> Self {
        Self::from_client(GmailClient::with_source(source))
    }

    /// Construct a Google factory from a bearer access token, redirecting
    /// the Gmail API base. Test seam mirroring `bifrost-graph`'s
    /// `GraphClient::with_api_base`: lets a harness point Gmail requests at a
    /// mock endpoint instead of `www.googleapis.com`.
    #[must_use]
    pub fn from_access_token_with_api_base(
        access_token: impl Into<String>,
        api_base: impl Into<String>,
    ) -> Self {
        Self::from_client(GmailClient::with_api_base(api_base, access_token))
    }

    /// Construct a Google factory from a shared token source, redirecting the
    /// Gmail API base. Test seam mirroring `bifrost-graph`'s
    /// `GraphClient::with_source`: combines a live `OAuthRefresher` (or any
    /// `TokenSource`) with a mock Gmail endpoint.
    #[must_use]
    pub fn from_token_source_with_api_base(
        source: Arc<dyn TokenSource>,
        api_base: impl Into<String>,
    ) -> Self {
        Self::from_client(GmailClient::with_api_base_and_source(api_base, source))
    }

    /// Redirect the People/contacts API base for opened accounts. Test seam
    /// mirroring the Gmail-base overrides above, but for the third Google
    /// surface: contacts, `otherContacts`, contact groups, directory search,
    /// contact CRUD, and photo endpoints all live on `people.googleapis.com`
    /// in production, a different host from both the Gmail mail base and the
    /// Calendar base. This override is independent of those two, so a harness
    /// can point People traffic at its mock endpoint while leaving the Gmail
    /// and Calendar bases untouched. Composes with the `from_*_with_api_base`
    /// constructors: build the factory with a redirected Gmail base, then call
    /// this to also redirect People.
    #[must_use]
    pub fn with_people_api_base(self, people_base: impl Into<String>) -> Self {
        Self {
            client: Arc::new(self.client.with_people_base(people_base)),
            pubsub: self.pubsub,
        }
    }

    /// Redirect the Calendar API base for opened accounts. Test seam
    /// completing the set alongside the Gmail-base constructors and
    /// [`Self::with_people_api_base`]: Calendar lives under
    /// `www.googleapis.com/calendar/v3` in production - the same host as the
    /// Gmail mail base but a different path root - so it is configured
    /// independently of both. Composes with the other overrides: build the
    /// factory with a redirected Gmail base, then call this and/or
    /// `with_people_api_base` to redirect the others.
    ///
    /// This supersedes the `RATATOSKR_TEST_GCAL_ENDPOINT` environment variable,
    /// which still works as a fallback for existing harnesses but is legacy.
    /// Prefer this: it is per client rather than process-global, so two
    /// accounts in one process can use two different Calendar endpoints; it is
    /// resolved once at construction rather than on every request; and a
    /// library has no business reading its downstream consumer's name out of
    /// the environment. An explicit call here always wins over the variable.
    #[must_use]
    pub fn with_calendar_api_base(self, calendar_base: impl Into<String>) -> Self {
        Self {
            client: Arc::new(self.client.with_calendar_base(calendar_base)),
            pubsub: self.pubsub,
        }
    }

    /// Configure Gmail Cloud Pub/Sub watch ownership for opened accounts.
    #[must_use]
    pub fn with_pubsub_config(mut self, config: PubSubConfig) -> Self {
        self.pubsub = Some(config);
        self
    }

    /// Configure an account-wide Gmail Cloud Pub/Sub watch topic.
    #[must_use]
    pub fn with_pubsub_topic(self, topic: impl Into<String>) -> Self {
        self.with_pubsub_config(PubSubConfig::new(topic))
    }
}

impl AccountFactory for GoogleAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let client = Arc::new(self.client.for_account(account_id));
        let pubsub = self.pubsub.clone();
        Box::pin(async move {
            match GoogleAccount::open(Arc::clone(&client), pubsub).await {
                // Single-namespace account: open probes only the
                // principal's own profile, so nothing can be skipped.
                Ok(account) => Ok(OpenedAccount::complete(account as Arc<dyn Account>)),
                Err(error) => {
                    client.detach_account();
                    Err(error)
                }
            }
        })
    }
}

struct GoogleAccount {
    client: Arc<GmailClient>,
    capabilities: AccountCapabilities,
    profile: GmailProfile,
    seed_state: OpaqueChangeState,
    pubsub: Arc<PubSubControl>,
    scope_cache: ScopeCache,
    shutdown: CancellationToken,
    closed: AtomicBool,
}

impl GoogleAccount {
    async fn open(
        client: Arc<GmailClient>,
        pubsub: Option<PubSubConfig>,
    ) -> Result<Arc<Self>, AccountError> {
        let profile = client
            .get_profile()
            .await
            .map_err(|error| error::into_account_error(error, error::GmailErrorContext::open()))?;
        // Present but not a number: a parse failure in a complete response,
        // not a missing field.
        let history_id = profile.history_id.parse::<u64>().map_err(|error| {
            error::into_account_error(
                crate::error::Error::provider_response(
                    crate::error::ProviderFault::ParseFailed,
                    format!("gmail profile invalid history id: {error}"),
                ),
                error::GmailErrorContext::open(),
            )
        })?;
        let seed_state = encode_gmail_state(&GmailChangeState::new(
            history_id,
            profile.email_address.clone(),
        ));
        let shutdown = CancellationToken::new();
        // Built before `profile` moves into the struct below; the mailbox is
        // the key under which account instances coordinate `users.stop`.
        let pubsub = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            pubsub,
            shutdown.clone(),
            &profile.email_address,
        ));
        Ok(Arc::new(Self {
            client: Arc::clone(&client),
            capabilities: gmail_capabilities(),
            profile,
            seed_state,
            pubsub,
            scope_cache: Arc::new(ScopeCacheState::new(ScopeSnapshot::empty())),
            shutdown,
            closed: AtomicBool::new(false),
        }))
    }
}

/// Runs the transport detach when the close future finishes OR is
/// dropped. Without it a cancelled `close()` would leave the account
/// marked closed - every later `close()` returns `Ok(())` at once - while
/// its rate-limiter registration stayed attached with no way to reclaim
/// it short of dropping the account.
struct DetachOnDrop {
    client: Arc<GmailClient>,
}

impl Drop for DetachOnDrop {
    fn drop(&mut self) {
        self.client.detach_account();
    }
}

impl Drop for GoogleAccount {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.client.detach_account();
        }
    }
}

impl Account for GoogleAccount {
    fn throttle_keys(
        &self,
        _account: &AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        use bifrost_types::ThrottleWork;
        // Generic object hydration and mutation on Google route only to
        // Gmail. Calendar and People use their typed EventId/ContactId APIs.
        let gmail = match work {
            ThrottleWork::Scope(CursorScope::Account) => true,
            ThrottleWork::Objects(ids) => !ids.is_empty(),
            ThrottleWork::Membership(MembershipScope::Label(_)) => true,
            _ => false,
        };
        self.client
            .quota_project()
            .filter(|_| gmail)
            .cloned()
            .map(bifrost_types::ThrottleKey::Budget)
            .into_iter()
            .collect()
    }

    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.client.account_net().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.account_net().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let valid =
            decode_gmail_state_for_profile(&cursor.server_state, &self.profile.email_address)
                .is_ok();
        CursorDescriptor {
            cost_class: if valid {
                CostClass::Cheap
            } else {
                CostClass::Expensive
            },
            strategy: if valid {
                SyncStrategy::ServerCursor
            } else {
                SyncStrategy::None
            },
            freshness: valid.then(Instant::now),
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        scopes::discover_cursor_scopes()
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        scopes::discover_memberships(Arc::clone(&self.client), Arc::clone(&self.scope_cache))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        scopes::scope_lifecycle_stream(Arc::clone(&self.client), self.shutdown.clone())
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let seed = self.seed_state.clone();
        Box::pin(async move {
            if !matches!(scope, CursorScope::Account) {
                return Err(error::into_account_error(
                    crate::error::Error::unsupported(AccountOperation::EstablishCursor),
                    error::GmailErrorContext::establish_cursor(),
                ));
            }
            Ok(CursorEstablishment::Ready(cursor_from_state(seed)))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        // Emits real coverage: this walk records an unreadable object as an
        // obligation and keeps going rather than discarding the partition.
        inventory::inventory_stream_cancellable(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            scope,
            self.shutdown.clone(),
        )
    }

    fn repair_inventory(
        &self,
        requests: AccountStream<bifrost_types::InventoryRepairRequest>,
    ) -> AccountStream<bifrost_types::InventoryRepairEvent> {
        inventory::repair_inventory(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            requests,
        )
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        inventory::get_stream_cancellable(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            ids,
            projection,
            self.shutdown.clone(),
        )
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream_cancellable(
            Arc::clone(&self.client),
            self.profile.clone(),
            cursor,
            self.shutdown.clone(),
        )
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        push::push_subscribe(Arc::clone(&self.pubsub), scopes.to_vec())
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::push_unsubscribe(Arc::clone(&self.pubsub), handle)
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::push_stream(Arc::clone(&self.pubsub), self.shutdown.clone())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_blob(Arc::clone(&self.client), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_blob_range(Arc::clone(&self.client), handle, range)
    }

    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_raw_rfc822(Arc::clone(&self.client), message)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_set_flags_cancellable(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            op,
            key,
            self.shutdown.clone(),
        )
    }

    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_move_cancellable(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            destination,
            None,
            key,
            self.shutdown.clone(),
        )
    }

    // Account: Gmail is the reason `bulk_move_from` exists. `batchModify`
    // carries `addLabelIds` and `removeLabelIds` in one request, so the
    // source detach costs nothing extra here - whereas without it a
    // consumer has to issue one `remove_from_container` per message.
    fn bulk_move_from(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        source: Option<MembershipScope>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_move_cancellable(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            destination,
            source,
            key,
            self.shutdown.clone(),
        )
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_destroy_cancellable(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            key,
            self.shutdown.clone(),
        )
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::add_to_container(Arc::clone(&self.client), target, container)
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::remove_from_container(Arc::clone(&self.client), target, container)
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetKeyword),
                error::GmailErrorContext::mutation(AccountOperation::SetKeyword),
            ))
        })
    }

    fn set_label_membership(
        &self,
        target: MutationTarget,
        label: ContainerId,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_label_membership(Arc::clone(&self.client), target, label, value)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetCategory),
                error::GmailErrorContext::mutation(AccountOperation::SetCategory),
            ))
        })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetExtendedProperty),
                error::GmailErrorContext::mutation(AccountOperation::SetExtendedProperty),
            ))
        })
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_is_read(Arc::clone(&self.client), target, is_read)
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        // Gmail has no message-importance field.
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetImportance),
                error::GmailErrorContext::mutation(AccountOperation::SetImportance),
            ))
        })
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::send_message(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            request,
        )
    }

    fn send_raw_message(
        &self,
        raw: Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::send_raw_message(Arc::clone(&self.client), raw, save_to_sent)
    }

    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        pim::attachment_upload(bytes, mime)
    }

    fn host_attachment(
        &self,
        bytes: Bytes,
        meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        cloud::host_attachment(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            bytes,
            meta,
        )
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        pim::draft_create(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            patch,
        )
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_update(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            draft,
            patch,
        )
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_discard(Arc::clone(&self.client), draft)
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::draft_send(Arc::clone(&self.client), draft)
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        // Gmail's REST API exposes no scheduled-send model.
        pim::cancel_scheduled_send_unsupported()
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::reschedule_send_unsupported()
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(Arc::clone(&self.client), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(Arc::clone(&self.client), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        pim::containers_list(Arc::clone(&self.client), Arc::clone(&self.scope_cache))
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        pim::container_create(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            kind,
            name,
            parent,
            style,
        )
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_rename(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            container,
            name,
            style,
        )
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_move(container, new_parent)
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        pim::container_delete(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            container,
        )
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list(Arc::clone(&self.client))
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(Arc::clone(&self.client), identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get(Arc::clone(&self.client))
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(Arc::clone(&self.client), config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get()
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        filters::list(Arc::clone(&self.client))
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        filters::create(Arc::clone(&self.client), filter)
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::FilterUpdate),
                error::GmailErrorContext::base(AccountOperation::FilterUpdate),
            ))
        })
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        filters::delete(Arc::clone(&self.client), filter)
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        filters::validate(filter)
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        contacts::address_books_list(Arc::clone(&self.client))
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::list(Arc::clone(&self.client), address_book, page_cursor)
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        contacts::get(Arc::clone(&self.client), contact)
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        contacts::create(Arc::clone(&self.client), contact)
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        contacts::update(Arc::clone(&self.client), contact, patch)
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        contacts::delete(Arc::clone(&self.client), contact)
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::search(Arc::clone(&self.client), request)
    }

    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        contacts::directory_search(Arc::clone(&self.client), query, limit, page_cursor)
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::DirectoryGroupsList),
                error::GmailErrorContext::base(AccountOperation::DirectoryGroupsList),
            ))
        })
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::DirectoryGroupExpand),
                error::GmailErrorContext::base(AccountOperation::DirectoryGroupExpand),
            ))
        })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        calendar::calendars_list(Arc::clone(&self.client))
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar::events_in_range(Arc::clone(&self.client), range)
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        calendar::get(Arc::clone(&self.client), event)
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        calendar::create(Arc::clone(&self.client), event)
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar::update(Arc::clone(&self.client), event, patch)
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        calendar::delete(Arc::clone(&self.client), event)
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar::rsvp(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            event,
            status,
        )
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar::search(Arc::clone(&self.client), request)
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            thread,
        )
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            message,
            projection,
        )
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::move_thread(Arc::clone(&self.client), thread, target, source)
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::delete_thread(Arc::clone(&self.client), thread, current)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Box::pin(async { Ok(()) });
        }
        // Linearization point. `closed` is now observable, so everything a
        // closed account promises must already hold before the caller can
        // touch - or drop - the returned future. Cancelling the shutdown
        // token here retires the watch renewer and the push and scope
        // lifecycle streams synchronously, and `DetachOnDrop` runs the
        // transport detach on every exit path of the future, completion and
        // cancellation alike. Only the best-effort `users.stop` round trip is
        // left inside the future, because it needs the transport that the
        // detach sheds; a caller that drops the future mid-call leaves a
        // Gmail-side watch that expires on its own within seven days, and
        // never an account that is marked closed while its renewer, streams,
        // or rate-limiter registration are still live. That surviving remote
        // half is an ACCEPTED residual, not an open defect: retrying the stop
        // would need the transport this close has already shed, and the only
        // closure would be a detached task outliving the account handle. The
        // watch expires on its own.
        self.shutdown.cancel();
        let client = Arc::clone(&self.client);
        let pubsub = Arc::clone(&self.pubsub);
        // Built here, not inside the async block: a future is inert until
        // its first poll, so a guard constructed inside the block would
        // never exist - and never detach - for a caller that drops the
        // returned future unpolled. Owned by the future either way, the
        // guard fires on completion, mid-poll cancellation, and the
        // never-polled drop alike.
        let detach = DetachOnDrop {
            client: Arc::clone(&client),
        };
        Box::pin(async move {
            let _detach = detach;
            push::close_watch(&pubsub).await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_net};
    use bifrost_net::{AccountSpec, NetConfig, RateLimit, RetryPolicy, StaticTokenSource};

    use super::*;

    #[test]
    fn project_hold_factory_rejects_empty_and_preserves_project_through_overrides() {
        assert!(
            GoogleAccountFactory::from_access_token("token")
                .with_quota_project("")
                .is_err()
        );
        assert!(
            GoogleAccountFactory::from_access_token("token")
                .with_quota_project(" ")
                .is_err()
        );
        let factory = GoogleAccountFactory::from_access_token("token")
            .with_quota_project("123")
            .unwrap()
            .with_people_api_base("https://people.test/v1")
            .with_calendar_api_base("https://calendar.test/v3");
        let client = factory.client.for_account(AccountId("factory".into()));
        assert_eq!(
            client.quota_project().unwrap().as_str(),
            "gmail-project|123"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_engine_keys_match_stamped_budget_across_accounts() {
        use bifrost_net::test_support::canned;
        use bifrost_types::{LabelId, ObjectType, ThrottleBudget, ThrottleKey, ThrottleWork};
        let script = ScriptedDispatch::new([
            canned(
                reqwest::StatusCode::OK,
                br#"{"emailAddress":"a@test","historyId":"1"}"#,
            ),
            canned(
                reqwest::StatusCode::OK,
                br#"{"emailAddress":"b@test","historyId":"1"}"#,
            ),
            canned(
                reqwest::StatusCode::OK,
                br#"{"emailAddress":"c@test","historyId":"1"}"#,
            ),
            canned(
                reqwest::StatusCode::FORBIDDEN,
                br#"{"error":{"errors":[{"reason":"dailyLimitExceeded"}]}}"#,
            ),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let now = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        let mut accounts = Vec::new();
        for (id, project) in [("a", "123"), ("b", "123"), ("c", "456")] {
            let mut spec = AccountSpec::new(Some(Arc::new(StaticTokenSource::new("token", None))));
            spec.default_retry = RetryPolicy::disabled();
            let client = GmailClient::with_account_net(
                "https://gmail.test",
                net.attach_account(AccountId(id.into()), spec),
            )
            .with_fixed_clock(now)
            .with_quota_project(project.into());
            accounts.push(GoogleAccount::open(Arc::new(client), None).await.unwrap());
        }
        let budget = ThrottleBudget::new("gmail-project|123");
        let expected = vec![ThrottleKey::Budget(budget.clone())];
        let account_id = AccountId("engine-id".into());
        for account in &accounts[..2] {
            assert_eq!(
                account.throttle_keys(&account_id, ThrottleWork::Scope(&CursorScope::Account)),
                expected
            );
            assert_eq!(
                account.throttle_keys(
                    &account_id,
                    ThrottleWork::Objects(&[ObjectId("message".into())])
                ),
                expected
            );
            assert_eq!(
                account.throttle_keys(
                    &account_id,
                    ThrottleWork::Membership(&MembershipScope::Label(LabelId("INBOX".into())))
                ),
                expected
            );
            for ty in [
                ObjectType::CalendarEvent,
                ObjectType::Event,
                ObjectType::Contact,
                ObjectType::ContactGroup,
            ] {
                assert!(
                    account
                        .throttle_keys(&account_id, ThrottleWork::Scope(&CursorScope::Type(ty)))
                        .is_empty()
                );
            }
            assert!(
                account
                    .throttle_keys(&account_id, ThrottleWork::Objects(&[]))
                    .is_empty()
            );
            assert!(
                account
                    .throttle_keys(
                        &account_id,
                        ThrottleWork::Membership(&MembershipScope::Folder(
                            bifrost_types::FolderId("folder".into())
                        ))
                    )
                    .is_empty()
            );
        }
        assert_ne!(
            accounts[2].throttle_keys(&account_id, ThrottleWork::Scope(&CursorScope::Account)),
            expected
        );
        let error = accounts[0]
            .client
            .get::<serde_json::Value>("/labels")
            .await
            .err()
            .unwrap();
        let error = error::into_account_error(error, error::GmailErrorContext::containers_list());
        assert_eq!(error.throttle_budgets(), &[budget]);
        let error = accounts[1]
            .client
            .get::<serde_json::Value>("/labels")
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            crate::Error::Net(bifrost_net::Error::LocalHold { .. })
        ));
        assert_eq!(script.requests().len(), 4);
    }

    const TEST_HOST: &str = "gmail.test";

    static NEXT_MAILBOX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// An account wired to a scripted transport, with one live watch
    /// handle so `close()` actually issues `users.stop`.
    fn scripted_account(script: &Arc<ScriptedDispatch>) -> (Arc<GoogleAccount>, bifrost_net::Net) {
        let net = scripted_net(script, NetConfig::default());
        let mut spec = AccountSpec::new(Some(Arc::new(StaticTokenSource::new("token", None))));
        spec.hosts = vec![RateLimit::new(TEST_HOST, 100.0, 1, 100)];
        spec.default_retry = RetryPolicy::disabled();
        let account_net = net.attach_account(AccountId("close-test".to_owned()), spec);
        let client = Arc::new(GmailClient::with_account_net(
            format!("https://{TEST_HOST}"),
            account_net,
        ));
        let shutdown = CancellationToken::new();
        let account = Arc::new(GoogleAccount {
            client: Arc::clone(&client),
            capabilities: gmail_capabilities(),
            profile: GmailProfile {
                email_address: "user@gmail.test".to_owned(),
                history_id: "1".to_owned(),
            },
            seed_state: encode_gmail_state(&GmailChangeState::new(1, "user@gmail.test".to_owned())),
            // A mailbox per call: the watch registry is process-wide and
            // libtest runs these tests in parallel.
            pubsub: Arc::new(PubSubControl::new(
                client,
                None,
                shutdown.clone(),
                &format!(
                    "close-test-{}@gmail.test",
                    NEXT_MAILBOX.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                ),
            )),
            scope_cache: Arc::new(ScopeCacheState::new(ScopeSnapshot::empty())),
            shutdown,
            closed: AtomicBool::new(false),
        });
        (account, net)
    }

    /// `close()` flips `closed` before it is awaited, so every later
    /// `close()` short-circuits to `Ok(())`. A caller that drops the
    /// future while `users.stop` is in flight must therefore not be left
    /// holding an account that reports itself closed while its shutdown
    /// token, streams and rate-limiter registration are all still live -
    /// nothing could reclaim them without dropping the account.
    #[tokio::test]
    async fn close_cancelled_mid_stop_still_shuts_down_and_detaches() {
        let script = ScriptedDispatch::new([Canned::Pending]);
        let (account, net) = scripted_account(&script);
        account
            .pubsub
            .insert_handle(&SubscriptionHandle("watch-handle".to_owned()))
            .await;
        assert_eq!(net.governor().cost_default_for(TEST_HOST), Some(1));

        // Drive the future into the stalled `users.stop` and then drop it
        // there, which is exactly the cancellation the guard has to survive.
        let mut close = account.close();
        for _ in 0..8 {
            assert!(
                futures::future::poll_immediate(&mut close).await.is_none(),
                "the stalled users.stop must keep the close future pending",
            );
            tokio::task::yield_now().await;
        }
        drop(close);
        assert_eq!(
            script.requests().len(),
            1,
            "the stop request must be in flight"
        );

        assert!(
            account.shutdown.is_cancelled(),
            "a cancelled close must still retire the renewer and streams",
        );
        assert_eq!(
            net.governor().cost_default_for(TEST_HOST),
            None,
            "a cancelled close must still shed the transport registration",
        );
    }

    /// A future is inert until first polled, so the detach guard must
    /// exist before the caller can drop the future - a guard constructed
    /// inside the async block never runs for a close future that is
    /// dropped unpolled, and `closed` is already set, so nothing else
    /// would ever reclaim the registration.
    #[tokio::test]
    async fn close_dropped_before_first_poll_still_detaches() {
        let script = ScriptedDispatch::new([Canned::Pending]);
        let (account, net) = scripted_account(&script);
        assert_eq!(net.governor().cost_default_for(TEST_HOST), Some(1));

        drop(account.close());

        assert!(account.shutdown.is_cancelled());
        assert_eq!(
            net.governor().cost_default_for(TEST_HOST),
            None,
            "an unpolled dropped close must still shed the transport registration",
        );
    }

    /// The completing path keeps the same guarantees, and reaches the
    /// wire for `users.stop` on its way there.
    #[tokio::test]
    async fn close_completes_by_stopping_the_watch_and_detaching() {
        let script = ScriptedDispatch::new([Canned::Response {
            status: reqwest::StatusCode::NO_CONTENT,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::new(),
        }]);
        let (account, net) = scripted_account(&script);
        account
            .pubsub
            .insert_handle(&SubscriptionHandle("watch-handle".to_owned()))
            .await;

        account.close().await.expect("close reports success");

        let requests = script.requests();
        assert_eq!(requests.len(), 1, "close must reach users.stop");
        assert_eq!(requests[0].url.path(), "/stop");
        assert!(account.shutdown.is_cancelled());
        assert_eq!(net.governor().cost_default_for(TEST_HOST), None);
        assert!(!account.pubsub.has_handles().await);
    }

    /// A profile whose `historyId` is present but not a number did not
    /// parse; it was not missing, and the response arrived whole.
    #[tokio::test]
    async fn open_refuses_an_unparseable_profile_history_id_as_a_parse_failure() {
        let script = ScriptedDispatch::new([Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(
                br#"{"emailAddress":"user@gmail.test","historyId":"not-a-number"}"#,
            ),
        }]);
        let (account, _net) = scripted_account(&script);

        let Err(error) = GoogleAccount::open(Arc::clone(&account.client), None).await else {
            panic!("an unparseable history id must refuse the open");
        };

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            )
        );
        let attempt = error.chain().iter().find_map(|cause| match cause {
            bifrost_types::Cause::Attempt(attempt) => Some(attempt.transmission_state),
            _ => None,
        });
        assert_eq!(
            attempt,
            Some(bifrost_types::TransmissionState::Acknowledged)
        );
    }
}
