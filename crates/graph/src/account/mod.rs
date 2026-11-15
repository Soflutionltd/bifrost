pub(crate) mod autodiscover;
mod batch;
mod batch_routing;
mod blob;
mod calendar;
mod capabilities;
#[cfg(test)]
mod capability_contract_tests;
mod categories;
mod changes;
mod cloud;
mod contacts;
pub(crate) use contacts::DirectoryMode;
mod cursor;
mod delta;
mod error;
mod etag_index;
mod ews_stream;
mod filters;
mod flags;
mod foreign;
mod get;
pub(crate) mod graph_error;
mod groups;
mod inventory;
mod mailbox_router;
mod mutate;
mod pim;
mod public_folder;
mod public_folders;
mod push;
mod push_stream;
mod push_tasks;
mod reactions;
mod scope;
mod scopes;
mod wire_item;

#[cfg(test)]
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountStream,
    AddressBook, AddressBookId, BlobHandle, ByteRange, Calendar, CalendarEvent, Change,
    ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest, CostClass, CursorDescriptor, CursorEstablishment, CursorScope,
    DirectoryCard, DirectoryGroup, DirectoryGroupId, DirectoryGroupMember, DraftHandle, DraftPatch,
    EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation,
    HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Importance,
    InventoryEvent, ItemOutcome, MembershipScope, Message, MutationSuccess, MutationTarget,
    ObjectId, OpenedAccount, Page, Priority, Projection, RsvpStatus, ScopeLifecycleEvent,
    SearchRequest, SendReceipt, SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, SkippedScope, SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration,
    ThreadId, VacationConfig,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

// pub: consumers build a GraphClient before registering GraphAccountFactory with the engine.
pub use crate::client::{GraphClient, MicrosoftCloud};
use crate::types::GraphMailFolder;

// Re-exported for the client-side api-base override test, which pins that a
// redirected Graph base also redirects the Autodiscover endpoints.
#[cfg(test)]
pub(crate) use self::autodiscover::{autodiscover_soap_url, autodiscover_xml_url};

use self::etag_index::{EtagIndex, EtagObservation};
use self::push::PushEndpoint;
use self::scopes::{CursorIndex, FolderTree};

// pub: public-folder consumers name the folders they want synced.
pub use self::public_folder::PublicFolderScope;

/// Defensive bound on the per-item EWS class cache. Sized to the
/// public-folder live-id cap, so a single fully-walked folder fits; past it
/// hydration falls back to the folder-class tier, which is already the right
/// answer for any folder that is not mixed-class.
pub(crate) const PUBLIC_ITEM_CLASS_CACHE_CAP: usize = cursor::PUBLIC_FOLDER_LIVE_IDS_CAP;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum PushMode {
    GraphSubscriptions,
    EwsStreaming,
}

/// One configured push surface. An Outlook user without a receiver keeps
/// polling; advertising push in that case would promise updates we cannot deliver.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) enum PushConfig {
    None,
    Webhook(PushEndpoint),
    EwsStreaming,
}

impl PushConfig {
    pub(crate) fn endpoint(&self) -> Option<&PushEndpoint> {
        match self {
            Self::Webhook(endpoint) => Some(endpoint),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct GraphAccount {
    pub(crate) mailboxes: mailbox_router::MailboxRouter,
    pub(crate) capabilities: AccountCapabilities,
    pub(crate) push_config: PushConfig,
    pub(crate) push_lease: bifrost_types::LaneLease,
    pub(crate) push_tasks: push_tasks::PushTasks,
    pub(crate) webhooks: push::WebhookRegistry,
    pub(crate) ews: ews_stream::EwsRegistry,
    pub(crate) cursor_index: Arc<RwLock<CursorIndex>>,
    pub(crate) folder_tree: Arc<RwLock<FolderTree>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) etag_index: Arc<RwLock<EtagIndex>>,
    pub(crate) public_folders: public_folders::PublicFolders,
    /// The account's primary SMTP, captured at open from the Graph
    /// profile. Seeds the `GetUserSettings` Autodiscover lookups.
    pub(crate) user_email: Option<String>,
    /// The primary mailbox's recursive folder listing as it was read at
    /// `open`, handed to the first cursor-scope discovery instead of
    /// making it walk the hierarchy a second time.
    ///
    /// `open` must list: it seeds `folder_tree`, which paths that never
    /// call discovery still read. The engine then calls
    /// `discover_cursor_scopes` moments later, and that pass listed the
    /// same hierarchy again and re-seeded the same tree with the same
    /// answer - one full recursive walk of duplicate traffic per attach,
    /// unbounded in request count for a deep mailbox. The slot is
    /// one-shot: discovery `take`s it, so a later re-discovery (which
    /// exists precisely to notice new folders) lists for real.
    pub(crate) open_folder_seed: Arc<RwLock<Option<Vec<GraphMailFolder>>>>,
}

impl GraphAccount {
    /// An account view whose every request reports its inbound payload
    /// bytes into a fresh batch accumulator.
    ///
    /// The shared-mailbox clients are enrolled in the SAME accumulator
    /// rather than left unmetered: a scope served by a foreign mailbox
    /// issues its requests through those handles, and leaving them out
    /// would report every such batch as having cost nothing.
    pub(crate) fn metered(&self) -> (Self, crate::client::ByteTally) {
        let (mailboxes, tally) = self.mailboxes.metered();
        let mut metered = self.clone();
        metered.mailboxes = mailboxes;
        (metered, tally)
    }

    pub(crate) fn new(
        client: GraphClient,
        push_config: PushConfig,
        shared_mailboxes: &[String],
        public_folders: Option<PublicFolderScope>,
        user_email: Option<String>,
    ) -> Self {
        let mailboxes = mailbox_router::MailboxRouter::new(client, shared_mailboxes);
        let has_shared_mailboxes = !mailboxes.shared().is_empty();
        Self {
            mailboxes,
            capabilities: capabilities::build_capabilities(&push_config, has_shared_mailboxes),
            push_config,
            push_lease: bifrost_types::LaneLease::new(),
            push_tasks: push_tasks::PushTasks::default(),
            webhooks: push::WebhookRegistry::default(),
            ews: ews_stream::EwsRegistry::default(),
            cursor_index: Arc::default(),
            folder_tree: Arc::default(),
            shutdown: CancellationToken::new(),
            etag_index: Arc::default(),
            public_folders: public_folders::PublicFolders::new(public_folders),
            user_email,
            open_folder_seed: Arc::default(),
        }
    }

    /// Re-derive the capability flags that mirror `shared_clients` after that
    /// map has been replaced.
    ///
    /// `capabilities()` hands out a snapshot taken at construction, and
    /// delegate Autodiscover can widen the map once, during `open`, before
    /// the account is published. Every later reader sees a settled map, so
    /// one refresh at the point of replacement is enough; a consumer that
    /// read the flag never has to re-read it.
    pub(crate) fn refresh_shared_mailbox_capabilities(&mut self) {
        self.capabilities.pim_methods.send_as = !self.mailboxes.shared().is_empty();
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(client: GraphClient, push_mode: PushMode) -> Self {
        Self::new(client, test_push_config(push_mode), &[], None, None)
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests_with_shared(
        client: GraphClient,
        push_mode: PushMode,
        shared_mailboxes: &[String],
    ) -> Self {
        Self::new(
            client,
            test_push_config(push_mode),
            shared_mailboxes,
            None,
            None,
        )
    }

    /// Install an explicit shared-client map instead of deriving one from
    /// the primary client.
    ///
    /// `new_for_tests_with_shared` goes through `shared_clients_map`, whose
    /// `for_shared_mailbox` clients share the primary's scripted-response
    /// queue (as they share its `AccountNet`). That is right
    /// for a test that only wants foreign requests scripted, and useless for
    /// a test that has to prove WHICH client issued a request: either client
    /// answers from the same queue and records into the same log, so a path
    /// that wrongly falls back to the primary looks identical to one that
    /// routes correctly. Rooting the shared client in its own `GraphClient`
    /// splits the two queues, which lets a test arm the primary with an
    /// EMPTY script and turn any fallback onto it into the seam's exhaustion
    /// panic.
    #[cfg(test)]
    pub(crate) fn new_for_tests_with_shared_clients(
        client: GraphClient,
        push_mode: PushMode,
        shared_clients: HashMap<String, GraphClient>,
    ) -> Self {
        let mut account = Self::new(client, test_push_config(push_mode), &[], None, None);
        account.mailboxes.set_shared_clients(shared_clients);
        account.refresh_shared_mailbox_capabilities();
        account
    }

    /// Seed a public-folder routing entry for tests, with placeholder
    /// metadata alongside it.
    ///
    /// Discovery (`seed_and_scope`) writes BOTH maps unconditionally, so a
    /// routing entry without a metadata entry is a state production cannot
    /// reach. This seeder therefore writes both too, even though its callers
    /// only exercise routing: seeding one map alone would let a test drive
    /// `public_folder_containers` down degradation branches that production
    /// never takes. Use `seed_public_folder_meta_for_tests` when the metadata
    /// values themselves matter.
    #[cfg(test)]
    pub(crate) async fn seed_public_folder_for_tests(
        &self,
        folder: bifrost_types::FolderId,
        routing: cursor::PublicFolderRouting,
    ) {
        let meta = public_folder::PublicFolderMeta {
            display_name: folder.0.clone(),
            folder_class: None,
            parent: None,
            effective_rights: crate::ews::EwsEffectiveRights::default(),
        };
        self.seed_public_folder_meta_for_tests(folder, routing, meta)
            .await;
    }

    /// Seed a public folder's projection metadata for tests, alongside its
    /// routing entry - the pair discovery installs together.
    #[cfg(test)]
    pub(crate) async fn seed_public_folder_meta_for_tests(
        &self,
        folder: bifrost_types::FolderId,
        routing: cursor::PublicFolderRouting,
        meta: public_folder::PublicFolderMeta,
    ) {
        self.public_folders.seed(folder, routing, meta).await;
    }

    /// Select the `GraphClient` whose `/users/{mailbox}` prefix routes
    /// this scope. A `FolderType` scope whose `FolderId` parses as a
    /// foreign-mailbox folder and whose mailbox has a configured shared
    /// client routes there. A foreign id whose mailbox is no longer
    /// configured is rejected locally: stripping its owner and sending it
    /// to `/me` can target a different mailbox namespace.
    #[cfg(test)]
    pub(crate) fn client_for_scope(
        &self,
        scope: &CursorScope,
    ) -> Result<&GraphClient, crate::error::GraphError> {
        let parsed = scope::GraphScope::parse(scope).map_err(|_| {
            crate::error::GraphError::Configuration {
                message: "unsupported Graph scope".to_string(),
            }
        })?;
        parsed.client(self)
    }
}

// pub: sync-engine consumers register this factory so reopen can mint fresh Graph accounts.
pub struct GraphAccountFactory {
    client: GraphClient,
    push_config: PushConfig,
    shared_mailboxes: Vec<String>,
    public_folders: Option<PublicFolderScope>,
    delegate_discovery: bool,
    /// Handed to every account this factory opens; see
    /// `push::WebhookRegistry`.
    recreated_subscription_ids: push::RecreatedSubscriptionIds,
}

impl GraphAccountFactory {
    // pub: ergonomic factory construction from an already configured GraphClient.
    pub fn new(client: GraphClient) -> Self {
        Self {
            client,
            push_config: PushConfig::None,
            shared_mailboxes: Vec::new(),
            public_folders: None,
            delegate_discovery: false,
            recreated_subscription_ids: Arc::default(),
        }
    }

    /// Configure credentials for the selected cloud's Outlook audience.
    /// Every open and reopen retains this source for EWS and Autodiscover.
    /// A source already configured on the input GraphClient also suffices.
    #[must_use]
    pub fn with_outlook_source(mut self, source: Arc<dyn bifrost_net::TokenSource>) -> Self {
        // Enterprise administrators expect configured Exchange credentials to
        // reach public folders, delegate discovery and streaming notifications.
        self.client = self.client.with_outlook_source(source);
        self
    }

    /// Opt in to Exchange public-folder discovery, and say which folders may
    /// SYNC. Default off entirely: no existing Graph account pays the
    /// Autodiscover round-trips.
    ///
    /// Discovery always browses and projects the full readable hierarchy (so
    /// `containers_list` shows every public folder the user can see), but only
    /// the folders the `scope` allows become `CursorScope::Folder` scopes
    /// synced by the no-delta-token poll strategy.
    /// [`PublicFolderScope::hierarchy_only`] syncs nothing;
    /// [`PublicFolderScope::pinned`] syncs exactly the listed folders. The
    /// argument is REQUIRED rather than defaulted because an organization can
    /// carry thousands of public folders holding millions of items - a
    /// silently-permissive "sync everything discovered" default is the defect,
    /// not the convenience.
    ///
    /// A per-folder Autodiscover/permission failure is skipped with a scoped
    /// warning, not a discovery-wide failure.
    // pub: public-folder consumers opt in before building the factory.
    #[must_use]
    pub fn with_public_folders(mut self, scope: PublicFolderScope) -> Self {
        self.public_folders = Some(scope);
        self
    }

    /// Register a delegate/shared mailbox by its routing key (the SMTP
    /// address or user id Graph accepts at `/users/{id}`). The mailbox's
    /// folders are discovered, established, and synced alongside the
    /// primary mailbox. Manual registration is no longer the only path:
    /// `with_delegate_discovery()` opt-in auto-enumerates delegates via
    /// Exchange Autodiscover and merges them in additively.
    // pub: shared-mailbox consumers register foreign mailboxes before building the factory.
    pub fn with_shared_mailbox(mut self, mailbox: impl Into<String>) -> Self {
        self.shared_mailboxes.push(mailbox.into());
        self
    }

    /// Opt in to Exchange Autodiscover delegate enumeration. Default off:
    /// like `with_public_folders`, it costs Autodiscover round-trips at
    /// `open`, so no existing Graph account pays for it uninvited. When
    /// set, `open` queries the `alternativeMailboxes` Autodiscover
    /// endpoint for the primary user's delegated mailboxes and seeds them
    /// as foreign mailboxes ADDITIVELY to any `with_shared_mailbox`
    /// entries (deduplicated by routing key). Discovery failure is
    /// non-fatal: the account still opens with its config-supplied
    /// mailboxes.
    // pub: delegate-discovery consumers opt in before building the factory.
    pub fn with_delegate_discovery(mut self) -> Self {
        self.delegate_discovery = true;
        self
    }

    /// Configure Graph webhook delivery with an account-wide `clientState`
    /// secret. Use the same value in the consumer's webhook receiver to
    /// reject notifications not minted for this account.
    ///
    /// This is the only webhook-mode constructor. The secret is not optional:
    /// the removed `with_push_endpoint(url)` let `create_subscription` mint a
    /// random per-resource value and immediately discard it, so its
    /// subscriptions carried a `clientState` no receiver could ever compare
    /// against - a documented "your webhook receiver cannot validate
    /// anything" mode.
    #[must_use]
    pub fn with_push_endpoint_client_state(
        mut self,
        webhook_url: impl Into<String>,
        client_state: impl Into<String>,
    ) -> Self {
        self.push_config = PushConfig::Webhook(PushEndpoint {
            webhook_url: webhook_url.into(),
            client_state: client_state.into(),
        });
        self
    }

    // pub: consumers without a reachable webhook endpoint can select in-process EWS streaming.
    pub fn with_ews_streaming(mut self) -> Self {
        self.push_config = PushConfig::EwsStreaming;
        self
    }
}

impl AccountFactory for GraphAccountFactory {
    fn open(
        &self,
        account_id: bifrost_types::AccountId,
    ) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let client = self.client.clone();
        let push_config = self.push_config.clone();
        let shared_mailboxes = self.shared_mailboxes.clone();
        let public_folders = self.public_folders.clone();
        let delegate_discovery = self.delegate_discovery;
        let recreated_subscription_ids = Arc::clone(&self.recreated_subscription_ids);
        Box::pin(async move {
            client.attach_account(account_id);
            let profile = client.get_profile().await.map_err(|e| {
                super::account::graph_error::into_account_error(
                    e,
                    super::account::graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::Discover,
                    ),
                )
            })?;
            let user_email = profile.mail.or(profile.user_principal_name);
            // Names the primary mailbox in its throttle budget. Set before
            // the account (and its shared clients) is built, and on every
            // primary open, so a reopen picks up a changed address.
            // A derived mailbox client cannot write the shared address slot.
            client.set_primary_address(user_email.clone());
            let mut account = GraphAccount::new(
                client.clone(),
                push_config,
                &shared_mailboxes,
                public_folders,
                user_email.clone(),
            );
            account
                .webhooks
                .set_recreated_ids(recreated_subscription_ids);

            // Opt-in delegate enumeration: additive to config-supplied
            // shared mailboxes, deduped by routing key. Autodiscover is a
            // best-effort round-trip here - a failure (or the absence of a
            // primary SMTP to query with) degrades to "no delegates
            // found" and the account still opens with its configured
            // mailboxes, rather than failing the whole open.
            let mut skipped_scopes: Vec<SkippedScope> = Vec::new();
            if delegate_discovery && let Some(email) = user_email.as_deref() {
                match account.discover_shared_mailboxes(email).await {
                    Ok(discovered) => {
                        // Route the empty-discovery case through the same
                        // helper as a non-empty one, so config-supplied
                        // entries always get the empty-drop / dedup
                        // normalization even when Autodiscover finds
                        // nothing to add.
                        let merged =
                            autodiscover::merge_shared_mailboxes(&shared_mailboxes, &discovered);
                        account.mailboxes.set_shared_mailboxes(&merged);
                        account.refresh_shared_mailbox_capabilities();
                    }
                    Err(error) => {
                        tracing::warn!(
                            "[Graph] delegate Autodiscover failed, opening with \
                             config-supplied shared mailboxes only: {error:?}"
                        );
                        // The whole delegate-discovery pass was skipped,
                        // so shares it would have found are absent from
                        // this handle. Account-scoped because no
                        // narrower scope is knowable - discovery is what
                        // failed.
                        skipped_scopes.push(SkippedScope {
                            scope: bifrost_types::ErrorScope::Account,
                            error,
                        });
                    }
                }
            }
            let folders = client.list_mail_folders_recursive().await.map_err(|e| {
                super::account::graph_error::into_account_error(
                    e,
                    super::account::graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::DiscoverMemberships,
                    ),
                )
            })?;
            account.folder_tree.write().await.replace_mail_folders(
                folders
                    .iter()
                    .map(|folder| (folder.id.clone(), folder.parent_folder_id.clone())),
            );
            // Hand this listing to the first cursor-scope discovery rather
            // than letting it re-walk the same hierarchy moments later.
            *account.open_folder_seed.write().await = Some(folders);
            Ok(OpenedAccount {
                account: Arc::new(account) as Arc<dyn Account>,
                skipped_scopes,
            })
        })
    }
}

impl Account for GraphAccount {
    fn protocol(&self) -> bifrost_types::Protocol {
        bifrost_types::Protocol::Graph
    }

    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        // GraphAccountFactory::open attaches before constructing the account;
        // every client constructor carries a parent Net or an AccountNet.
        match self.mailboxes.primary().account_net() {
            Some(account_net) => account_net.set_priority(priority),
            None => tracing::error!("opened Graph account has no AccountNet"),
        }
    }

    fn set_bandwidth_cap(&self, bps: Option<bifrost_types::BandwidthCap>) {
        // GraphAccountFactory::open attaches before constructing the account;
        // every client constructor carries a parent Net or an AccountNet.
        match self.mailboxes.primary().account_net() {
            Some(account_net) => account_net.set_bandwidth_cap(bps),
            None => tracing::error!("opened Graph account has no AccountNet"),
        }
    }

    /// The Outlook mailbox budget each unit of work is charged to - the same
    /// budget the REST funnel and the `$batch` subresponse classifiers stamp
    /// on that work's throttles, computed from the same client. A move also
    /// declares its destination folder's mailbox (Graph only expresses a
    /// same-mailbox move, so it is normally the targets' own).
    fn throttle_keys(
        &self,
        _account: &bifrost_types::AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        use bifrost_types::{ThrottleKey, ThrottleWork};
        let budgets: Vec<bifrost_types::ThrottleBudget> = match work {
            ThrottleWork::Scope(scope) => {
                self.mailboxes.budget_for_scope(scope).into_iter().collect()
            }
            ThrottleWork::Objects(ids) => {
                let mut budgets = Vec::new();
                for id in ids {
                    if let Some(budget) = self.mailboxes.budget_for_object(id)
                        && !budgets.contains(&budget)
                    {
                        budgets.push(budget);
                    }
                }
                budgets
            }
            ThrottleWork::Membership(bifrost_types::MembershipScope::Folder(folder)) => self
                .mailboxes
                .budget_for_owner(foreign::folder_owner(folder).as_deref())
                .into_iter()
                .collect(),
            // Labels, mailbox tags and queries name no folder a request is
            // routed to; `ThrottleWork` is `#[non_exhaustive]`.
            _ => Vec::new(),
        };
        budgets.into_iter().map(ThrottleKey::Budget).collect()
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let decoded = cursor::decode_cursor(cursor).ok();
        // A public folder is a client-maintained watermark poll, not a
        // server-issued delta token; report `Poll` so the descriptor is
        // honest about its strategy. Delta kinds stay `ServerCursor`.
        let strategy = match decoded.as_ref().map(cursor::GraphCursorPayload::kind) {
            Some(cursor::GraphCursorKind::PublicFolder(_)) => SyncStrategy::Poll,
            Some(_) => SyncStrategy::ServerCursor,
            None => SyncStrategy::None,
        };
        CursorDescriptor {
            cost_class: if decoded.is_some() {
                CostClass::Cheap
            } else {
                CostClass::Expensive
            },
            strategy,
            freshness: decoded.is_some().then(Instant::now),
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        sync_event_stream(scopes::discover_cursor_scope_events(self.clone()))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        sync_event_stream(scopes::discover_membership_events(self.clone()))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::iter(
            scopes::scope_lifecycle_events()
                .into_iter()
                .map(ScopeLifecycleEvent::Lifecycle),
        ))
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let account = self.clone();
        Box::pin(async move {
            let parsed = scope::GraphScope::parse(&scope).map_err(|error| {
                graph_error::cursor_error_to_account_error(
                    error,
                    graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::EstablishCursor,
                    )
                    .with_scope(bifrost_types::ErrorScope::Cursor(scope.clone())),
                )
            })?;
            if let scope::GraphScope::PublicFolder(folder) = &parsed
                && account
                    .public_folders
                    .public_folder_routing(folder)
                    .await
                    .is_none()
            {
                return Err(graph_error::cursor_error_to_account_error(
                    cursor::CursorError::Unsupported,
                    graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::EstablishCursor,
                    )
                    .with_scope(bifrost_types::ErrorScope::Cursor(scope)),
                ));
            }
            Ok(CursorEstablishment::EstablishViaInventory)
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        let parsed = scope::GraphScope::parse(&scope);
        if let Ok(scope::GraphScope::PublicFolder(folder)) = &parsed {
            let account = self.clone();
            let folder = folder.clone();
            return Box::pin(
                stream::once(async move {
                    if account
                        .public_folders
                        .public_folder_routing(&folder)
                        .await
                        .is_some()
                    {
                        public_folder::public_folder_inventory_stream_admitted(
                            account, scope, folder,
                        )
                    } else {
                        inventory::inventory_stream_admitted(
                            account,
                            scope,
                            Err(cursor::CursorError::Unsupported),
                        )
                    }
                })
                .flatten(),
            );
        }
        inventory::inventory_stream_admitted(self.clone(), scope, parsed)
    }

    // Keep the default is_inventory_cursor: it asks this hook, whose stream
    // construction is inert until polled. A second Graph predicate would
    // duplicate acceptance of scope, payload and acknowledged page position.
    fn inventory_resume_stream(
        &self,
        cursor: ChangeCursor,
    ) -> Option<AccountStream<InventoryEvent>> {
        inventory::resume_inventory_stream(self.clone(), cursor)
    }

    fn flag_target(
        &self,
        target: &ObjectId,
        observed: &bifrost_types::FlagSet,
        op: &bifrost_types::FlagOp,
    ) -> bifrost_types::FlagTargetState {
        mutate::graph_flag_target(target, observed, op)
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        get::get_stream(self.clone(), ids, projection)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let parsed = scope::GraphScope::parse(&cursor.scope);
        // A decoded `PublicFolder` payload routes to the no-delta-token
        // poll strategy; any other (or undecodable) cursor stays on the
        // existing delta changes path. The cursor kind is unambiguous,
        // so no routing-map check is needed - the routing rides in the
        // payload.
        if matches!(
            cursor::decode_cursor(&cursor).map(|p| p.kind()),
            Ok(cursor::GraphCursorKind::PublicFolder(_))
        ) {
            return public_folder::public_folder_changes_stream(self.clone(), cursor, parsed);
        }
        changes::changes_stream_admitted(self.clone(), cursor, parsed)
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        let account = self.clone();
        let scopes = scopes.to_vec();
        Box::pin(async move { push::push_subscribe(account, scopes).await })
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { push::push_unsubscribe(account, handle).await })
    }

    fn push_lanes(&self) -> Vec<bifrost_types::PushLaneSpec> {
        push_stream::push_lanes(self)
    }

    fn push_stream(
        &self,
        lane: bifrost_types::PushLane,
        stop: bifrost_types::SessionStop,
        gate: bifrost_types::PushGate,
    ) -> AccountStream<bifrost_types::PushSessionEvent> {
        push_stream::push_stream(self.clone(), lane, stop, gate)
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        blob::open_blob_stream(self.clone(), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        blob::open_blob_range_stream(self.clone(), handle, range)
    }

    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        blob::open_raw_rfc822(self.clone(), message)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_set_flags_stream(self.clone(), targets, op, key)
    }

    fn bulk_set_flags_if_version(
        self: Arc<Self>,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_set_flags_if_version_stream(self.as_ref().clone(), targets, op, key)
    }

    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_move_stream(self.clone(), targets, destination, key)
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_destroy_stream(self.clone(), targets, key)
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: bifrost_types::ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::add_to_container(account, target, container).await })
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: bifrost_types::ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::RemoveFromContainer,
            ))
        })
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: bifrost_types::Flag,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::SetKeyword,
            ))
        })
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: bifrost_types::ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::SetLabelMembership,
            ))
        })
    }

    fn set_category(
        &self,
        target: MutationTarget,
        category: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::set_category(account, target, category, value).await })
    }

    fn set_extended_property(
        &self,
        target: MutationTarget,
        property_id: String,
        value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(
            async move { pim::set_extended_property(account, target, property_id, value).await },
        )
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::set_is_read(account, target, is_read).await })
    }

    fn set_importance(
        &self,
        target: MutationTarget,
        level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::set_importance(account, target, level).await })
    }

    fn send_message(
        &self,
        request: SendRequest,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        let account = self.clone();
        // Graph files the Sent copy server-side; this call does not verify it.
        Box::pin(async move {
            pim::send_message(account, request)
                .await
                .map(SendReceipt::provider_managed)
        })
    }

    fn send_raw_message(
        &self,
        raw: Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        let account = self.clone();
        Box::pin(async move {
            pim::send_raw_message(account, raw, save_to_sent)
                .await
                .map(SendReceipt::provider_managed)
        })
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<bifrost_types::AttachmentHandle, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::AttachmentUpload,
            ))
        })
    }

    fn host_attachment(
        &self,
        bytes: Bytes,
        meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        cloud::host_attachment(self.clone(), bytes, meta)
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_create(account, patch).await })
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_update(account, draft, patch).await })
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_discard(account, draft).await })
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<SendReceipt, AccountError>> {
        let account = self.clone();
        Box::pin(async move {
            pim::draft_send(account, draft)
                .await
                .map(SendReceipt::provider_managed)
        })
    }

    fn cancel_scheduled_send(&self, handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::cancel_scheduled_send(account, handle).await })
    }

    fn reschedule_send(
        &self,
        handle: ObjectId,
        scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::reschedule_send(account, handle, scheduled).await })
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::search(account, request).await })
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::search_messages(account, request).await })
    }

    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::containers_list(account).await })
    }

    fn category_definitions_list(
        &self,
    ) -> AccountFuture<Result<Vec<bifrost_types::CategoryDefinition>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { categories::category_definitions_list(account).await })
    }

    fn message_reactions(
        &self,
        ids: &[bifrost_types::ObjectId],
    ) -> AccountFuture<
        Result<bifrost_types::BatchOutcome<bifrost_types::MessageReactionState>, AccountError>,
    > {
        let account = self.clone();
        let ids = ids.to_vec();
        Box::pin(async move { reactions::message_reactions(account, &ids).await })
    }

    fn container_create(
        &self,
        kind: bifrost_types::ContainerKind,
        name: String,
        parent: Option<bifrost_types::ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<bifrost_types::ContainerId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_create(account, kind, name, parent, style).await })
    }

    fn container_rename(
        &self,
        container: bifrost_types::ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_rename(account, container, name, style).await })
    }

    fn container_move(
        &self,
        container: bifrost_types::ContainerId,
        new_parent: Option<bifrost_types::ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_move(account, container, new_parent).await })
    }

    fn container_delete(
        &self,
        container: bifrost_types::ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_delete(account, container).await })
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<bifrost_types::Identity>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::identities_list(account).await })
    }

    fn identity_update(
        &self,
        _identity: bifrost_types::IdentityId,
        _patch: bifrost_types::IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::IdentityUpdate,
            ))
        })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::vacation_get(account).await })
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::vacation_set(account, config).await })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<bifrost_types::QuotaInfo>, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::QuotaGet,
            ))
        })
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::list(account).await })
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::create(account, filter).await })
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::update(account, filter, patch).await })
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::delete(account, filter).await })
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        Box::pin(async move { filters::validate(filter) })
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::address_books_list(account).await })
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::list(account, address_book, page_cursor).await })
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::get(account, contact).await })
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::create(account, contact).await })
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::update(account, contact, patch).await })
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::delete(account, contact).await })
    }

    fn contact_delete_if_version(
        &self,
        contact: ContactId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(
            async move { contacts::delete_if_version(account, contact, expected_version).await },
        )
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::search(account, request).await })
    }

    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let account = self.clone();
        Box::pin(
            async move { contacts::directory_search(account, query, limit, page_cursor).await },
        )
    }

    fn directory_groups_list(
        &self,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { groups::directory_groups_list(account, page_cursor).await })
    }

    fn directory_group_expand(
        &self,
        group: DirectoryGroupId,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { groups::directory_group_expand(account, group, page_cursor).await })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::calendars_list(account).await })
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::events_in_range(account, range).await })
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::get(account, event).await })
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::create(account, event).await })
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::update(account, event, patch).await })
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::delete(account, event).await })
    }

    fn event_delete_if_version(
        &self,
        event: EventId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::delete_if_version(account, event, expected_version).await })
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::rsvp(account, event, status).await })
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::search(account, request).await })
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::thread_hydrate(account, thread).await })
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::message_hydrate(account, message, projection).await })
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: bifrost_types::ContainerId,
        _source: Option<bifrost_types::ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::move_thread(account, thread, target).await })
    }

    // `apply_label` and `remove_label` rely on the trait's default
    // dispatch in `bifrost_types::Account`. The default routes
    // `(Label, Graph)` and `(Folder, Graph)` provenance through
    // `set_category`, `(Folder, non-Graph)` through
    // `add_to_container` / `remove_from_container`, and the remaining
    // shapes through `set_keyword` / `set_label_membership` - which
    // surface `Unsupported` on Graph if the consumer hands us a
    // cross-account label object. No override needed here.

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<bifrost_types::ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::delete_thread(account, thread, current).await })
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move {
            // Server-side state first, while the account is still usable:
            // cancelling the token stops the workers that would otherwise be
            // the ones retiring it, and both webhook subscriptions and EWS
            // streaming subscriptions outlive this process if nobody asks the
            // server to drop them.
            //
            // The retire walk is also what cancels the shutdown token, and
            // it does so BEFORE it walks: the ordering is load-bearing for
            // `push_subscribe` and the renewal recreate, both of which check
            // the token under the lock they register under. There is no
            // second `cancel()` here because the walk is infallible and
            // cancels unconditionally as its first step.
            push::retire_all_graph_subscriptions(&account).await;
            // Claim a release for every live or orphaned EWS subscription,
            // then join every account-owned push task (those releases and any
            // Graph recreate install-or-undo) under ONE bound. A handle still
            // running at the bound is dropped, never aborted: dropping a
            // `JoinHandle` detaches its task, which then runs to completion,
            // and those tasks own server-side cleanup that must not be cut
            // short.
            account.ews.retire(&account.push_tasks);
            account.push_tasks.join().await;
            Ok(())
        })
    }
}

#[cfg(test)]
fn test_push_config(mode: PushMode) -> PushConfig {
    match mode {
        PushMode::GraphSubscriptions => PushConfig::None,
        PushMode::EwsStreaming => PushConfig::EwsStreaming,
    }
}

fn sync_event_stream<T, F>(future: F) -> AccountStream<SyncEvent<T>>
where
    T: Send + 'static,
    F: Future<Output = Vec<SyncEvent<T>>> + Send + 'static,
{
    Box::pin(stream::once(future).flat_map(stream::iter))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bifrost_types::{
        AccountErrorKind, AccountId, AccountOperation, FolderId, ObjectType, ThrottleKey,
        ThrottleWork,
    };

    use super::cursor::{
        GraphCursorKind, GraphCursorPayload, GraphPageMarker, PublicFolderCursor,
        PublicFolderRouting, encode_cursor, kind_for_scope,
    };
    use super::*;

    #[test]
    fn missing_public_routes_preserve_kind_and_scope_after_discovery() {
        use bifrost_types::{ErrorScope, ResourceKind};
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let folder = FolderId("public-folder".into());
        let cases = [
            (
                AccountOperation::ContactsList,
                ErrorScope::ContactCollection,
                ResourceKind::Contact,
            ),
            (
                AccountOperation::EventsInRange,
                ErrorScope::Calendar {
                    id: "calendar".into(),
                },
                ResourceKind::Calendar,
            ),
            (
                AccountOperation::SyncInventory,
                ErrorScope::Cursor(bifrost_types::CursorScope::Folder(folder.clone())),
                ResourceKind::Mailbox,
            ),
        ];
        for (operation, scope, kind) in cases {
            account.public_folders.set_discovery_complete(false);
            let before = account
                .public_folders
                .missing_public_collection_route_error(
                    operation,
                    &folder,
                    scope.clone(),
                    kind,
                    &folder.0,
                );
            assert_eq!(before.kind(), &AccountErrorKind::ConcurrencyConflict);
            account.public_folders.set_discovery_complete(true);
            let after = account
                .public_folders
                .missing_public_collection_route_error(
                    operation,
                    &folder,
                    scope.clone(),
                    kind,
                    &folder.0,
                );
            assert_eq!(after.kind(), &AccountErrorKind::NotFound(kind));
            assert_eq!(after.scope(), Some(&scope));
        }
    }

    #[test]
    fn mailbox_case_aliases_have_one_client_and_accept_old_owner_tags() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["Shared@Contoso.com".into(), "shared@contoso.com".into()],
        );
        assert_eq!(account.mailboxes.shared().len(), 1);
        // The first configured spelling is canonical and keys the map.
        assert!(
            account
                .mailboxes
                .shared()
                .contains_key("Shared@Contoso.com")
        );
        assert!(std::ptr::eq(
            account
                .mailboxes
                .client_for_owner(Some("SHARED@CONTOSO.COM"))
                .unwrap(),
            account
                .mailboxes
                .client_for_owner(Some("shared@contoso.com"))
                .unwrap(),
        ));
    }

    #[test]
    fn default_factory_has_no_push_capability_or_lane() {
        let factory = GraphAccountFactory::new(GraphClient::new("token"));
        let account = GraphAccount::new(factory.client, factory.push_config, &[], None, None);
        assert_eq!(
            account.capabilities.push,
            bifrost_types::PushCapability::None
        );
        assert!(push_stream::push_lanes(&account).is_empty());
    }

    fn account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
    }

    /// What a scope, an object and a move destination DECLARE is the budget
    /// a throttle of that same work STAMPS - one function of one client - and
    /// two mailboxes never share one.
    ///
    /// The stamp side is driven through the real classifier: a funnel-tagged
    /// 429 for the shared mailbox's client becomes an error whose only
    /// recorded hold is that budget. If the declaration and the stamp ever
    /// disagree, the engine records a hold no wait site honours.
    #[test]
    fn a_mailbox_declares_the_budget_its_throttles_stamp() {
        let client = GraphClient::new("token");
        client.set_primary_address(Some("me@contoso.com".into()));
        let account = GraphAccount::new_for_tests_with_shared(
            client,
            PushMode::GraphSubscriptions,
            &["a@contoso.com".to_string(), "b@contoso.com".to_string()],
        );
        let id = AccountId("graph".into());
        let declare = |work: ThrottleWork<'_>| Account::throttle_keys(&account, &id, work);
        let scope_of = |mailbox: &str| CursorScope::FolderType {
            folder: foreign::encode_foreign(mailbox, "inbox"),
            ty: ObjectType::Email,
        };
        let budget_of = |mailbox: &str| {
            account
                .mailboxes
                .client_for_owner(Some(mailbox))
                .expect("configured")
                .mailbox_budget()
        };

        let a = declare(ThrottleWork::Scope(&scope_of("a@contoso.com")));
        let b = declare(ThrottleWork::Scope(&scope_of("b@contoso.com")));
        assert_eq!(a, vec![ThrottleKey::Budget(budget_of("a@contoso.com"))]);
        assert_ne!(a, b, "two mailboxes must never share a budget");

        let object = ObjectId(foreign::qualify_with_owner(Some("a@contoso.com"), "m1"));
        assert_eq!(declare(ThrottleWork::Objects(&[object])), a);
        let destination = MembershipScope::Folder(foreign::encode_foreign("a@contoso.com", "x"));
        assert_eq!(declare(ThrottleWork::Membership(&destination)), a);
        let primary = declare(ThrottleWork::Scope(&CursorScope::FolderType {
            folder: FolderId("inbox".into()),
            ty: ObjectType::Email,
        }));
        assert_ne!(primary, a, "the primary mailbox is its own budget");
        assert!(
            declare(ThrottleWork::Scope(&CursorScope::Folder(FolderId(
                "pf".into()
            ))))
            .is_empty(),
            "a public folder is read over EWS and declares no mailbox budget"
        );

        let stamped = graph_error::into_account_error(
            crate::error::GraphError::MailboxContext {
                budget: budget_of("a@contoso.com"),
                source: Box::new(crate::error::GraphError::Response(
                    crate::error::GraphResponseError::from_response(
                        reqwest::StatusCode::TOO_MANY_REQUESTS,
                        reqwest::header::HeaderMap::new(),
                        Bytes::from_static(br#"{"error":{"code":"TooManyRequests"}}"#),
                    ),
                )),
            },
            graph_error::GraphErrorContext::graph(AccountOperation::SyncChanges),
        );
        assert_eq!(stamped.throttle_budgets(), &[budget_of("a@contoso.com")]);
        let ThrottleKey::Budget(declared) = &a[0] else {
            panic!("Graph declares budget keys");
        };
        assert_eq!(stamped.throttle_budgets(), std::slice::from_ref(declared));
    }

    #[test]
    fn inventory_cursor_predicate_classifies_without_building_a_stream() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".into()),
            ty: ObjectType::Email,
        };
        let cursor = encode_cursor(
            scope,
            GraphCursorPayload::inventory_page(
                GraphCursorKind::Messages {
                    folder_id: "inbox".into(),
                },
                GraphPageMarker {
                    next_link: "next".into(),
                    last_seen_id: Some("last".into()),
                },
                None,
            ),
        )
        .expect("inventory cursor encodes");

        assert!(Account::is_inventory_cursor(&account(), &cursor));
    }

    /// The predicate must mirror `inventory_resume_stream`'s COMPLETE
    /// acceptance condition, not the `inventory_in_progress` half of it. A
    /// cursor whose outer scope disagrees with its payload is refused by the
    /// hook; if the predicate accepted it anyway, sync would route the scope
    /// to the deferred inventory worker, the worker would get `None` back and
    /// report `NoCursor`, and the scope would be left with no live cursor and
    /// no recovery path. Refused by the predicate, it reaches `changes_stream`
    /// instead, which classifies it as schema-incompatible and restarts.
    #[test]
    fn an_inventory_cursor_whose_scope_contradicts_its_payload_is_not_resumable() {
        let mut cursor = encode_cursor(
            CursorScope::FolderType {
                folder: FolderId("inbox".into()),
                ty: ObjectType::Email,
            },
            GraphCursorPayload::inventory_page(
                GraphCursorKind::Messages {
                    folder_id: "inbox".into(),
                },
                GraphPageMarker {
                    next_link: "next".into(),
                    last_seen_id: Some("last".into()),
                },
                None,
            ),
        )
        .expect("inventory cursor encodes");
        // Same payload, a scope that contradicts it.
        cursor.scope = CursorScope::FolderType {
            folder: FolderId("archive".into()),
            ty: ObjectType::Email,
        };

        let account = account();
        assert!(
            Account::inventory_resume_stream(&account, cursor.clone()).is_none(),
            "the resume hook refuses a scope/payload mismatch"
        );
        assert!(
            !Account::is_inventory_cursor(&account, &cursor),
            "the predicate must refuse exactly what the hook refuses"
        );
    }

    /// A Graph subscription outlives this process: it keeps POSTing to the
    /// consumer's receiver for up to its ~24h expiry, and reopen builds a
    /// fresh account whose resubscribe adds another. Nothing in the `Account`
    /// contract promises `push_unsubscribe` before `close()`, so `close()`
    /// must retire them itself.
    #[tokio::test]
    async fn close_deletes_every_registered_webhook_subscription() {
        let client = GraphClient::new("token");
        client.script_rest([
            crate::client::ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
            crate::client::ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        account.webhooks.groups_for_tests().write().await.insert(
            bifrost_types::SubscriptionHandle("h".to_string()),
            push::GraphSubscriptionGroup::live(vec![
                push::GraphSubscriptionState {
                    server_id: "one".to_string(),
                    expires_at: "2099-01-01T00:00:00Z".to_string(),
                    resource: "/me/mailFolders/inbox/messages".to_string(),
                    scopes: vec![CursorScope::FolderType {
                        folder: FolderId("inbox".to_string()),
                        ty: ObjectType::Email,
                    }],
                    warned_unparseable_expiry: false,
                    terminated: false,
                },
                push::GraphSubscriptionState {
                    server_id: "two".to_string(),
                    expires_at: "2099-01-01T00:00:00Z".to_string(),
                    resource: "/me/calendars/cal/events".to_string(),
                    scopes: vec![CursorScope::FolderType {
                        folder: FolderId("cal".to_string()),
                        ty: ObjectType::Event,
                    }],
                    warned_unparseable_expiry: false,
                    terminated: false,
                },
            ]),
        );

        Account::close(&account).await.expect("close succeeds");

        assert!(account.webhooks.groups_for_tests().read().await.is_empty());
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].url.ends_with("/subscriptions/one"));
        assert!(requests[1].url.ends_with("/subscriptions/two"));
        assert!(account.shutdown.is_cancelled());
    }

    fn public_folder_routing() -> PublicFolderRouting {
        PublicFolderRouting {
            anchor_mailbox: "content@contoso.com".to_string(),
            public_folder_mailbox: Some("pf@contoso.com".to_string()),
        }
    }

    fn public_folder_cursor(folder: &str) -> ChangeCursor {
        let pf = PublicFolderCursor {
            folder_id: folder.to_string(),
            routing: public_folder_routing(),
            watermark: Some("2026-03-01T10:00:00Z".to_string()),
            last_full_scan_at: None,
            live_ids: Vec::new(),
            live_versions: Vec::new(),
            boundary_ids: Vec::new(),
            degraded: false,
            warned_classes: Vec::new(),
        };
        encode_cursor(
            CursorScope::Folder(FolderId(folder.to_string())),
            GraphCursorPayload::public_folder(pf),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn bare_folder_in_routing_map_establishes_via_inventory() {
        let account = account();
        let folder = FolderId("AAMkPF=".to_string());
        account
            .seed_public_folder_for_tests(folder.clone(), public_folder_routing())
            .await;

        // A Folder scope present in the routing map establishes via
        // inventory.
        let in_map = account
            .establish_initial_cursor(CursorScope::Folder(folder))
            .await
            .expect("seeded public folder establishes");
        assert!(matches!(in_map, CursorEstablishment::EstablishViaInventory));

        // A Folder scope absent from the map is rejected exactly as
        // today (preserving initial_delta_url_rejects_bare_folder_scope).
        let absent = account
            .establish_initial_cursor(CursorScope::Folder(FolderId("other".to_string())))
            .await;
        let err = absent.expect_err("bare non-public folder rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EstablishCursor)
        ));
    }

    #[test]
    fn describe_cursor_reports_poll_for_public_folder() {
        let account = account();
        let pf = account.describe_cursor(&public_folder_cursor("AAMkPF="));
        assert_eq!(pf.strategy, SyncStrategy::Poll);

        let messages_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let messages = encode_cursor(
            messages_scope.clone(),
            GraphCursorPayload::new(
                kind_for_scope(&messages_scope).unwrap(),
                &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
                None,
            ),
        )
        .unwrap();
        assert_eq!(
            account.describe_cursor(&messages).strategy,
            SyncStrategy::ServerCursor
        );
    }

    #[tokio::test]
    async fn public_folder_push_subscribe_unsupported() {
        let account = account();
        let scope = CursorScope::Folder(FolderId("AAMkPF=".to_string()));
        let err = account
            .push_subscribe(&[scope])
            .await
            .expect_err("public-folder push is unsupported");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
        ));
    }
}
