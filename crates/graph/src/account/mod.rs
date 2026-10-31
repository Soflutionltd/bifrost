mod autodiscover;
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
mod cursor;
mod error;
mod ews_stream;
mod filters;
mod foreign;
mod get;
mod graph_error;
mod groups;
mod inventory;
mod message_ids;
mod mutate;
mod pim;
mod pin;
mod public_folder;
mod push;
mod push_stream;
mod reactions;
mod scopes;
mod worker_slot;

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
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
    ObjectId, OpenedAccount, Page, Priority, Projection, RsvpReply, RsvpStatus,
    ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SkippedScope, SubscriptionHandle, SyncEvent, SyncStrategy,
    ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::sync::{RwLock, broadcast, watch};
use tokio_util::sync::CancellationToken;

// pub: consumers build a GraphClient before registering GraphAccountFactory with the engine.
pub use crate::client::GraphClient;
use crate::types::GraphMailFolder;

// Re-exported for the client-side api-base override test, which pins that a
// redirected Graph base also redirects the Autodiscover endpoints.
#[cfg(test)]
pub(crate) use self::autodiscover::{autodiscover_soap_url, autodiscover_xml_url};

use self::push::{EwsSubscriptionState, GraphSubscriptionGroup, PushEndpoint};
use self::scopes::{CursorIndex, FolderTree};

// pub: public-folder consumers name the folders they want synced.
pub use self::public_folder::PublicFolderScope;

/// Defensive bound on the per-item EWS class cache. Sized to the
/// public-folder live-id cap, so a single fully-walked folder fits; past it
/// hydration falls back to the folder-class tier, which is already the right
/// answer for any folder that is not mixed-class.
pub(crate) const PUBLIC_ITEM_CLASS_CACHE_CAP: usize = cursor::PUBLIC_FOLDER_LIVE_IDS_CAP;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum PushMode {
    GraphSubscriptions,
    EwsStreaming,
}

#[derive(Clone)]
pub(crate) struct GraphAccount {
    pub(crate) client: GraphClient,
    /// Foreign (shared/delegate) mailbox clients, keyed by the
    /// `/users/{id}` routing key. Built once at `open` from the
    /// factory's configured shared mailboxes via
    /// `GraphClient::for_shared_mailbox`. Selecting the right client by
    /// scope (`client_for_scope`) keeps every existing call site - which
    /// reads `api_path_prefix()` off the client - mailbox-correct by
    /// construction.
    pub(crate) shared_clients: Arc<HashMap<String, GraphClient>>,
    pub(crate) capabilities: AccountCapabilities,
    pub(crate) push_endpoint: Option<PushEndpoint>,
    pub(crate) push_mode: PushMode,
    pub(crate) push_tx: broadcast::Sender<WatchEvent>,
    pub(crate) cursor_index: Arc<RwLock<CursorIndex>>,
    pub(crate) folder_tree: Arc<RwLock<FolderTree>>,
    pub(crate) graph_subscriptions:
        Arc<RwLock<HashMap<SubscriptionHandle, GraphSubscriptionGroup>>>,
    pub(crate) graph_worker: worker_slot::WorkerSlot,
    /// Whether push is currently believed to be DOWN, so `Reconnected` can be
    /// edge-triggered off it rather than published on every successful
    /// subscribe.
    ///
    /// `WatchEvent::Reconnected` is the engine's full-reconcile trigger: the
    /// push reconciler answers it with an account-wide `Coalesced` /
    /// `HintPayload::Unknown` invalidation over every registered cursor
    /// scope. A first subscribe has no gap to cover - the engine has just
    /// established or resumed those cursors itself - so publishing one there
    /// bought a redundant reconcile of the whole account per subscribe. The
    /// latch lives on the account rather than inside the renewal worker
    /// because the two emitters must agree: the worker raises it when a
    /// renewal tick fails, and a subscribe that lands while push is degraded
    /// is a genuine recovery edge and owes the event. Shared across every
    /// `GraphAccount` clone, and reset by construction on reopen.
    pub(crate) push_disconnected: Arc<AtomicBool>,
    pub(crate) ews_subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, EwsSubscriptionState>>>,
    pub(crate) ews_worker: worker_slot::WorkerSlot,
    /// Monotone generation counter bumped by `subscribe_ews` /
    /// `unsubscribe_ews` on every change to `ews_subscriptions`. The EWS
    /// worker marks the current generation seen immediately before each
    /// read of that map, so a bump can never be lost between reading and
    /// waiting: an idle worker wakes to subscribe, a live one abandons its
    /// long poll and re-subscribes to the new scope union. A `watch`
    /// generation rather than a `Notify` because a `Notify` permit stored
    /// by the registration that starts the worker would be consumed right
    /// after its first Subscribe and read as a topology change - turning
    /// every first registration into a guaranteed redundant resubscribe.
    pub(crate) ews_topology: Arc<watch::Sender<u64>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) etag_index: Arc<RwLock<EtagIndex>>,
    /// Well-known Trash folder ids keyed by their owning mailbox. The empty
    /// key is the primary mailbox; shared mailboxes use their routing key.
    /// Folder ids are stable for one opened account, and a fresh account on
    /// reopen starts with an empty cache.
    pub(crate) trash_folder_ids: Arc<RwLock<HashMap<String, String>>>,
    /// Public-folder routing map, keyed by native EWS `FolderId`. A
    /// folder present here is a public folder: it establishes via the
    /// public-folder inventory pass and polls via the no-delta-token
    /// strategy. Seeded empty at open; spec-3's Autodiscover discovery
    /// fills it. The presence-in-map check (`public_folder_routing`) is
    /// the discriminator that keeps a bare `CursorScope::Folder` for a
    /// non-public folder on its existing (reject-on-delta) path.
    pub(crate) routing_map:
        Arc<RwLock<HashMap<bifrost_types::FolderId, cursor::PublicFolderRouting>>>,
    /// Projection metadata for every discovered public folder (display name,
    /// EWS `FolderClass`, parent, effective rights), seeded in the same
    /// discovery pass that fills `routing_map`. `containers_list` reads this
    /// so it can render the full readable hierarchy - including folders that
    /// are visible but not pinned for sync - without re-browsing EWS.
    pub(crate) public_folder_meta:
        Arc<RwLock<HashMap<bifrost_types::FolderId, public_folder::PublicFolderMeta>>>,
    /// Which public folders this account syncs, or `None` when public-folder
    /// discovery is off (the default, so no existing Graph account pays the
    /// Autodiscover round-trips). Opt in via `with_public_folders`.
    /// EWS item classes learned by the public-folder inventory / poll
    /// passes, keyed by the folder-qualified `ObjectId` those passes mint.
    ///
    /// Hydration builds a CLASS-CONDITIONAL `GetItem` body, and the class is
    /// not derivable from an id: this map is how the class the inventory
    /// pass already parsed (`EwsItem.item_class`) reaches the hydration
    /// call. It is a cache, never a source of truth - a miss falls back to
    /// the folder's `FolderClass` in `public_folder_meta` (re-seeded by
    /// discovery on every attach, so a cold reattach that hydrates a
    /// consumer-held id before any inventory pass still gets a container
    /// level answer), and a miss there falls back to the message shape,
    /// which is the historical request. The alternative - encoding the
    /// class into the `ObjectId` - was rejected because the poll's
    /// `Destroyed` ids are minted from the persisted `live_ids` baseline,
    /// which stores bare native item ids: a class-bearing id would stop
    /// byte-matching the id the consumer stored unless the class were also
    /// added to the cursor payload, ticking its schema, and ids already held
    /// by consumers would carry no class either way.
    pub(crate) public_item_classes: Arc<RwLock<HashMap<ObjectId, crate::ews::EwsItemShape>>>,
    pub(crate) public_folders: Option<PublicFolderScope>,
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

/// Change keys are a best-effort mutation optimization. Retaining one per
/// object forever turns a long-running high-churn account into an unbounded
/// cache, so keep only a fixed working set.
pub(crate) const ETAG_INDEX_MAX_ENTRIES: usize = 10_000;

/// How long `close()` waits for the EWS worker to release its streaming
/// subscription before aborting it. Long enough for one round trip to a
/// healthy Exchange, short enough that an unresponsive one cannot make
/// `close()` hang.
const CLOSE_WORKER_JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A change key plus the recency ticket that orders it in `EtagIndex::order`.
struct EtagEntry {
    etag: String,
    seq: u64,
}

/// Bounded LRU over change keys.
///
/// Every operation is one hash lookup plus one or two `BTreeMap` node
/// operations - no scan of the cache. That matters because the write lock
/// guarding this index is taken once per hydrated page and once per mutation
/// preflight: a recency policy that walked the entries would make a full
/// inventory pass cost `messages * capacity` comparisons, serialized, which
/// is a worse trade than the unbounded map it replaced.
#[derive(Default)]
pub(crate) struct EtagIndex {
    /// Keys are `Arc<str>` so a recency ticket can name an entry without a
    /// second copy of the (long, base64url) Graph id.
    entries: HashMap<Arc<str>, EtagEntry>,
    /// Recency tickets, oldest first. Exactly one row per `entries` row,
    /// keyed by that row's current `seq`, so eviction is `pop_first`.
    order: BTreeMap<u64, Arc<str>>,
    next_seq: u64,
}

impl EtagIndex {
    pub(crate) fn insert(&mut self, id: String, etag: String) {
        // `remove_entry` hands back the owned key, so re-caching an id
        // already present does not reallocate it.
        let (key, previous) = match self.entries.remove_entry(id.as_str()) {
            Some((key, entry)) => (key, Some(entry.seq)),
            None => {
                self.evict_to_capacity();
                (Arc::from(id), None)
            }
        };
        let seq = self.promote(Arc::clone(&key), previous);
        self.entries.insert(key, EtagEntry { etag, seq });
    }

    /// Reads a change key AND marks it hot. Mutation preflight is the only
    /// reader, so an id being read is an id about to carry an `If-Match`.
    pub(crate) fn get(&mut self, id: &str) -> Option<String> {
        let (key, entry) = self.entries.remove_entry(id)?;
        let seq = self.promote(Arc::clone(&key), Some(entry.seq));
        let etag = entry.etag.clone();
        self.entries.insert(
            key,
            EtagEntry {
                etag: entry.etag,
                seq,
            },
        );
        Some(etag)
    }

    pub(crate) fn remove(&mut self, id: &str) {
        if let Some(entry) = self.entries.remove(id) {
            self.order.remove(&entry.seq);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn contains_key(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    #[cfg(test)]
    fn order_len(&self) -> usize {
        self.order.len()
    }

    /// Issues `key` a fresh recency ticket, retiring `previous` if it had
    /// one. The caller must store the returned `seq` on the entry, which is
    /// what keeps `order` and `entries` one-to-one.
    fn promote(&mut self, key: Arc<str>, previous: Option<u64>) -> u64 {
        if let Some(previous) = previous {
            self.order.remove(&previous);
        }
        let seq = self.next_seq;
        // A u64 ticket space is not exhaustible by any real account: at one
        // cache write per nanosecond it lasts ~585 years.
        self.next_seq = self.next_seq.wrapping_add(1);
        self.order.insert(seq, key);
        seq
    }

    /// Drops the coldest entries until one more will fit.
    fn evict_to_capacity(&mut self) {
        while self.entries.len() >= ETAG_INDEX_MAX_ENTRIES {
            let Some((_, coldest)) = self.order.pop_first() else {
                break;
            };
            self.entries.remove(&coldest);
        }
    }
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
        let (client, tally) = self.client.metered();
        let shared_clients = self
            .shared_clients
            .iter()
            .map(|(key, client)| (key.clone(), client.with_tally(tally.clone())))
            .collect();
        let mut metered = self.clone();
        metered.client = client;
        metered.shared_clients = Arc::new(shared_clients);
        (metered, tally)
    }

    pub(crate) fn new(
        client: GraphClient,
        push_mode: PushMode,
        push_endpoint: Option<PushEndpoint>,
        shared_mailboxes: &[String],
        public_folders: Option<PublicFolderScope>,
        user_email: Option<String>,
    ) -> Self {
        let (push_tx, _) = broadcast::channel(256);
        let shared_clients = shared_clients_map(&client, shared_mailboxes);
        // `pim_methods.send_as` mirrors this map, so derive it from the map
        // AFTER `shared_clients_map` has dropped unroutable keys: a config
        // of nothing but empty strings leaves no reachable mailbox and must
        // not advertise the flag.
        let has_shared_mailboxes = !shared_clients.is_empty();
        Self {
            shared_clients: Arc::new(shared_clients),
            client,
            capabilities: capabilities::build_capabilities(push_mode, has_shared_mailboxes),
            push_endpoint,
            push_mode,
            push_tx,
            cursor_index: Arc::new(RwLock::new(CursorIndex::default())),
            folder_tree: Arc::new(RwLock::new(FolderTree::default())),
            graph_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            graph_worker: worker_slot::worker_slot(),
            push_disconnected: Arc::new(AtomicBool::new(false)),
            ews_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            ews_worker: worker_slot::worker_slot(),
            ews_topology: Arc::new(watch::channel(0_u64).0),
            shutdown: CancellationToken::new(),
            etag_index: Arc::new(RwLock::new(EtagIndex::default())),
            trash_folder_ids: Arc::new(RwLock::new(HashMap::new())),
            routing_map: Arc::new(RwLock::new(HashMap::new())),
            public_folder_meta: Arc::new(RwLock::new(HashMap::new())),
            public_item_classes: Arc::new(RwLock::new(HashMap::new())),
            public_folders,
            user_email,
            open_folder_seed: Arc::new(RwLock::new(None)),
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
        self.capabilities.pim_methods.send_as = !self.shared_clients.is_empty();
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(client: GraphClient, push_mode: PushMode) -> Self {
        Self::new(client, push_mode, None, &[], None, None)
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests_with_shared(
        client: GraphClient,
        push_mode: PushMode,
        shared_mailboxes: &[String],
    ) -> Self {
        Self::new(client, push_mode, None, shared_mailboxes, None, None)
    }

    /// Install an explicit shared-client map instead of deriving one from
    /// the primary client.
    ///
    /// `new_for_tests_with_shared` goes through `shared_clients_map`, whose
    /// `for_shared_mailbox` clients share the primary's scripted-response
    /// queue (as they share its `AccountNet` and semaphore). That is right
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
        let mut account = Self::new(client, push_mode, None, &[], None, None);
        account.shared_clients = Arc::new(shared_clients);
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
        self.routing_map
            .write()
            .await
            .insert(folder.clone(), routing);
        self.public_folder_meta.write().await.insert(folder, meta);
    }

    /// Select the `GraphClient` whose `/users/{mailbox}` prefix routes
    /// this scope. A `FolderType` scope whose `FolderId` parses as a
    /// foreign-mailbox folder and whose mailbox has a configured shared
    /// client routes there. A foreign id whose mailbox is no longer
    /// configured is rejected locally: stripping its owner and sending it
    /// to `/me` can target a different mailbox namespace.
    pub(crate) fn client_for_scope(
        &self,
        scope: &CursorScope,
    ) -> Result<&GraphClient, crate::error::GraphError> {
        if let CursorScope::FolderType { folder, .. } = scope
            && let Some(foreign) = foreign::parse_folder(folder).foreign()
        {
            self.client_for_owner(Some(&foreign.mailbox))
        } else {
            Ok(&self.client)
        }
    }

    /// Select the `GraphClient` for an owning mailbox decoded from a
    /// foreign-encoded message/blob id. `None` (a primary-mailbox item)
    /// routes through `/me`; a configured shared mailbox routes through
    /// its `/users/{mailbox}` client. An unconfigured foreign owner is a
    /// stale account configuration, never a primary-mailbox id.
    pub(crate) fn client_for_owner(
        &self,
        owner: Option<&str>,
    ) -> Result<&GraphClient, crate::error::GraphError> {
        match owner {
            None => Ok(&self.client),
            Some(mailbox) => self.shared_clients.get(mailbox).ok_or_else(|| {
                crate::error::GraphError::Configuration {
                    message: format!("shared mailbox not configured on this account: {mailbox}"),
                }
            }),
        }
    }

    /// Look up the public-folder routing for a native EWS folder id, or
    /// `None` if the folder is not a public folder. Presence in the map
    /// is the discriminator that routes a `CursorScope::Folder` onto the
    /// public-folder sync path (vs the existing reject-on-delta path).
    pub(crate) async fn public_folder_routing(
        &self,
        folder: &bifrost_types::FolderId,
    ) -> Option<cursor::PublicFolderRouting> {
        self.routing_map.read().await.get(folder).cloned()
    }

    /// Record the EWS item class of every item a public-folder inventory or
    /// poll walk returned, so a later `GetItem` for one of those ids can ask
    /// for the property shape that item's class accepts.
    ///
    /// Keyed by the same folder-qualified `ObjectId` the projections mint,
    /// so the hydration doors look up exactly the id they were handed. The
    /// map is bounded by `PUBLIC_ITEM_CLASS_CACHE_CAP`, which bounds
    /// admission of NEW ids only: past it the folder's `FolderClass` fallback
    /// carries hydration for ids that never got in, which is the correct
    /// answer for every single-class folder and the historical one otherwise.
    /// An id already resident is always refreshed, cap or no cap, so an item
    /// whose `ItemClass` changed does not keep a stale shape.
    pub(crate) async fn record_public_item_classes(
        &self,
        folder: &bifrost_types::FolderId,
        items: &[crate::ews::EwsItem],
    ) {
        if items.is_empty() {
            return;
        }
        let mut map = self.public_item_classes.write().await;
        let mut len = map.len();
        for item in items {
            let id = foreign::encode_public_item_id(folder, &item.item_id);
            let shape = crate::ews::EwsItemShape::from_item_class(&item.item_class);
            match map.entry(id) {
                // An id already in the map is refreshed unconditionally: the
                // cap bounds ADMISSION of new keys, and overwriting an
                // existing key does not grow the map. Without this an item
                // whose `ItemClass` changed would keep its stale shape for
                // the account's lifetime once the map filled up.
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    slot.insert(shape);
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    if len < PUBLIC_ITEM_CLASS_CACHE_CAP {
                        slot.insert(shape);
                        len += 1;
                    }
                }
            }
        }
    }

    /// Drop the cached item classes for ids the public-folder poll just
    /// emitted as `Destroyed`.
    ///
    /// The map is filled at every EWS walk and nothing else removed from
    /// it, so without this the entry for a deleted item lived until the
    /// account was dropped. That is not merely wasted memory: the cap is
    /// an admission bound, not an eviction policy (see
    /// `record_public_item_classes`), so a churning folder whose dead
    /// entries fill the map locks every LATER item out of the cache and
    /// silently demotes hydration to the folder-class tier for the rest of
    /// the process. Pruning at the destroy site keeps the resident set
    /// proportional to the live set; the cap stays as the backstop for a
    /// folder that is genuinely that large.
    pub(crate) async fn forget_public_item_classes(&self, ids: &[ObjectId]) {
        if ids.is_empty() {
            return;
        }
        let mut map = self.public_item_classes.write().await;
        for id in ids {
            map.remove(id);
        }
    }

    /// The `GetItem` property shape to request for a public-folder item.
    ///
    /// Three tiers, most specific first: the class the inventory / poll pass
    /// parsed for this exact item; else the folder's EWS `FolderClass`,
    /// which discovery re-seeds on every attach and which is the right
    /// answer for any folder that is not mixed-class; else the message
    /// shape, which is byte-identical to the request this lane always made -
    /// so an item of unknown class fails exactly as it did before, per item,
    /// rather than being guessed into a shape.
    pub(crate) async fn public_item_shape(
        &self,
        id: &ObjectId,
        folder: &bifrost_types::FolderId,
    ) -> crate::ews::EwsItemShape {
        if let Some(shape) = self.public_item_classes.read().await.get(id) {
            return *shape;
        }
        self.public_folder_meta
            .read()
            .await
            .get(folder)
            .and_then(|meta| meta.folder_class.as_deref())
            .map(crate::ews::EwsItemShape::from_folder_class)
            .unwrap_or_default()
    }

    /// The owning shared-mailbox identity for a foreign scope, or `None`
    /// for a primary-mailbox scope. Used to decide whether a per-scope
    /// permission denial quarantines just this scope (foreign) or
    /// escalates account-wide (primary).
    pub(crate) fn owner_of_scope(&self, scope: &CursorScope) -> Option<bifrost_types::MailboxId> {
        if let CursorScope::FolderType { folder, .. } = scope {
            foreign::parse_folder(folder)
                .foreign()
                .map(|foreign| bifrost_types::MailboxId(foreign.mailbox.clone()))
        } else {
            None
        }
    }
}

// pub: sync-engine consumers register this factory so reopen can mint fresh Graph accounts.
pub struct GraphAccountFactory {
    client: GraphClient,
    push_mode: PushMode,
    push_endpoint: Option<PushEndpoint>,
    shared_mailboxes: Vec<String>,
    public_folders: Option<PublicFolderScope>,
    delegate_discovery: bool,
}

impl GraphAccountFactory {
    // pub: ergonomic factory construction from an already configured GraphClient.
    pub fn new(client: GraphClient) -> Self {
        Self {
            client,
            push_mode: PushMode::GraphSubscriptions,
            push_endpoint: None,
            shared_mailboxes: Vec::new(),
            public_folders: None,
            delegate_discovery: false,
        }
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
        self.push_endpoint = Some(PushEndpoint {
            webhook_url: webhook_url.into(),
            client_state: client_state.into(),
        });
        self.push_mode = PushMode::GraphSubscriptions;
        self
    }

    // pub: consumers without a reachable webhook endpoint can select in-process EWS streaming.
    pub fn with_ews_streaming(mut self) -> Self {
        self.push_endpoint = None;
        self.push_mode = PushMode::EwsStreaming;
        self
    }
}

impl AccountFactory for GraphAccountFactory {
    fn open(
        &self,
        account_id: bifrost_types::AccountId,
    ) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let client = self.client.clone();
        let push_mode = self.push_mode;
        let push_endpoint = self.push_endpoint.clone();
        let shared_mailboxes = self.shared_mailboxes.clone();
        let public_folders = self.public_folders.clone();
        let delegate_discovery = self.delegate_discovery;
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
            let mut account = GraphAccount::new(
                client.clone(),
                push_mode,
                push_endpoint,
                &shared_mailboxes,
                public_folders,
                user_email.clone(),
            );

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
                        account.shared_clients = Arc::new(shared_clients_map(&client, &merged));
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
    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        if let Some(account_net) = self.client.account_net() {
            account_net.set_priority(priority);
        }
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        if let Some(account_net) = self.client.account_net() {
            account_net.set_bandwidth_cap(bps);
        }
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let decoded = cursor::decode_cursor(cursor).ok();
        // A public folder is a client-maintained watermark poll, not a
        // server-issued delta token; report `Poll` so the descriptor is
        // honest about its strategy. Delta kinds stay `ServerCursor`.
        let strategy = match decoded.as_ref().map(|p| &p.kind) {
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
            // Public-folder scope: discriminate by routing-map
            // membership, NOT by the `CursorScope::Folder` variant alone
            // (that variant is already live on the Graph delta path and
            // must keep rejecting non-public folders).
            if let CursorScope::Folder(folder) = &scope
                && account.public_folder_routing(folder).await.is_some()
            {
                return Ok(CursorEstablishment::EstablishViaInventory);
            }
            // Fall through: a bare Folder NOT in the routing map stays
            // Unsupported (preserving the reject-on-bare-Folder
            // invariant), as does any other unsupported scope.
            cursor::kind_for_scope(&scope).map_err(|e| {
                graph_error::cursor_error_to_account_error(
                    e,
                    graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::EstablishCursor,
                    )
                    .with_scope(bifrost_types::ErrorScope::Cursor(scope.clone())),
                )
            })?;
            Ok(CursorEstablishment::EstablishViaInventory)
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        // Route a public-folder `CursorScope::Folder` (present in the
        // routing map) to the no-delta-token inventory pass; everything
        // else (including a bare Folder for a non-public folder) stays
        // on the existing delta inventory path, which still rejects it.
        if let CursorScope::Folder(folder) = &scope {
            let account = self.clone();
            let folder = folder.clone();
            let scope = scope.clone();
            return Box::pin(
                stream::once(async move {
                    if account.public_folder_routing(&folder).await.is_some() {
                        public_folder::public_folder_inventory_stream(account, scope)
                    } else {
                        inventory::inventory_stream(account, scope)
                    }
                })
                .flatten(),
            );
        }
        inventory::inventory_stream(self.clone(), scope)
    }

    fn inventory_resume_stream(
        &self,
        cursor: ChangeCursor,
    ) -> Option<AccountStream<InventoryEvent>> {
        inventory::resume_inventory_stream(self.clone(), cursor)
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        get::get_stream(self.clone(), ids, projection)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        // A decoded `PublicFolder` payload routes to the no-delta-token
        // poll strategy; any other (or undecodable) cursor stays on the
        // existing delta changes path. The cursor kind is unambiguous,
        // so no routing-map check is needed - the routing rides in the
        // payload.
        if matches!(
            cursor::decode_cursor(&cursor).map(|p| p.kind),
            Ok(cursor::GraphCursorKind::PublicFolder(_))
        ) {
            return public_folder::public_folder_changes_stream(self.clone(), cursor);
        }
        changes::changes_stream(self.clone(), cursor)
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

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push_stream::push_stream(self.clone())
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
        _keyword: String,
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

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::send_message(account, request).await })
    }

    fn send_raw_message(
        &self,
        raw: Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::send_raw_message(account, raw, save_to_sent).await })
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

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_send(account, draft).await })
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

    fn category_definition_upsert(
        &self,
        definition: bifrost_types::CategoryDefinition,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { categories::category_definition_upsert(account, definition).await })
    }

    fn category_definition_delete(&self, name: String) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { categories::category_definition_delete(account, name).await })
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

    fn folder_message_ids(
        &self,
        folder: bifrost_types::FolderId,
    ) -> AccountFuture<Result<Vec<bifrost_types::ObjectId>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { message_ids::folder_message_ids(&account, folder).await })
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
        page_cursor: Option<Vec<u8>>,
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
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let account = self.clone();
        Box::pin(
            async move { contacts::directory_search(account, query, limit, page_cursor).await },
        )
    }

    fn directory_groups_list(
        &self,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { groups::directory_groups_list(account, page_cursor).await })
    }

    fn directory_group_expand(
        &self,
        group: DirectoryGroupId,
        page_cursor: Option<Vec<u8>>,
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

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::rsvp(account, event, status, RsvpReply::default()).await })
    }

    fn event_rsvp_reply(
        &self,
        event: EventId,
        status: RsvpStatus,
        reply: RsvpReply,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::rsvp(account, event, status, reply).await })
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
            if let Some(worker) = worker_slot::take_worker(&account.ews_worker).await {
                // Join rather than abort: the worker observes the cancelled
                // token, then sends an EWS `Unsubscribe` for whatever
                // streaming subscription it is holding. An abort here would
                // preempt exactly that, and Exchange caps streaming
                // subscriptions per mailbox, so each reopen would burn one
                // until it timed out on its own. The join is bounded because
                // the release is a network round trip and `close()` must not
                // be able to hang on an unresponsive server.
                let mut worker = worker;
                if tokio::time::timeout(CLOSE_WORKER_JOIN_TIMEOUT, &mut worker)
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        target: "bifrost_graph::ews",
                        "EWS worker did not release its subscription within \
                         the close() budget; aborting"
                    );
                    worker.abort();
                }
            }
            // The renewal worker stops cooperatively: it observes the
            // cancelled token between renewals and drops an in-flight PATCH,
            // and it awaits the task that recreates a vanished subscription,
            // so joining it here also waits for that task's install-or-undo
            // DELETE. The join is bounded like the EWS one, and aborting on
            // timeout is safe rather than merely honest: the recreate runs
            // in its own task, which an abort of the worker does not touch.
            if let Some(mut worker) = worker_slot::take_worker(&account.graph_worker).await
                && tokio::time::timeout(CLOSE_WORKER_JOIN_TIMEOUT, &mut worker)
                    .await
                    .is_err()
            {
                tracing::warn!(
                    target: "bifrost_graph::webhooks",
                    "Graph renewal worker did not stop within the close() budget; aborting"
                );
                worker.abort();
            }
            Ok(())
        })
    }
}

/// Build the `/users/{id}`-keyed foreign-mailbox client map from a list
/// of routing keys (SMTP addresses or user ids), dropping empty keys. The
/// map key is the exact string later looked up by `client_for_scope` /
/// `client_for_owner` and minted into foreign scope/owner tags by
/// `discover_cursor_scopes`, so seeding a key here is self-consistent with
/// every routing site.
///
/// `with_shared_mailbox("")` is constructible, and an empty key used to
/// install a client whose prefix is the malformed `/users/` - which every
/// foreign id with an empty owner then routed to, producing an opaque
/// remote 400 for what is a local configuration error. Dropping it here
/// makes that id fall to the same local `Configuration` rejection every
/// other unconfigured owner gets. `merge_shared_mailboxes` already applies
/// the identical rule on the Autodiscover leg; this is the config leg,
/// which had no such filter.
fn shared_clients_map(client: &GraphClient, mailboxes: &[String]) -> HashMap<String, GraphClient> {
    mailboxes
        .iter()
        .filter(|mailbox| !mailbox.is_empty())
        .map(|mailbox| (mailbox.clone(), client.for_shared_mailbox(mailbox.clone())))
        .collect()
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
    use bifrost_types::{AccountErrorKind, AccountOperation, FolderId, ObjectType};

    use super::cursor::{
        GraphCursorKind, GraphCursorPayload, GraphPageMarker, PublicFolderCursor,
        PublicFolderRouting, encode_cursor, kind_for_scope,
    };
    use super::*;

    fn account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
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
        account.graph_subscriptions.write().await.insert(
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
                },
            ]),
        );

        Account::close(&account).await.expect("close succeeds");

        assert!(account.graph_subscriptions.read().await.is_empty());
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

    #[test]
    fn etag_cache_evicts_the_least_recently_used_entry() {
        let mut cache = EtagIndex::default();
        for index in 0..=ETAG_INDEX_MAX_ENTRIES {
            cache.insert(format!("id-{index}"), format!("etag-{index}"));
        }
        assert_eq!(cache.len(), ETAG_INDEX_MAX_ENTRIES);
        assert!(!cache.contains_key("id-0"));
        assert!(cache.contains_key("id-10000"));

        // A read refreshes the recency position. On the next insertion the
        // oldest untouched entry must go, not the hot one.
        assert_eq!(cache.get("id-1").as_deref(), Some("etag-1"));
        cache.insert("new".to_string(), "etag-new".to_string());
        assert!(cache.contains_key("id-1"));
        assert!(!cache.contains_key("id-2"));
        assert!(cache.contains_key("new"));
    }

    /// Eviction reads the recency order without consulting the entry map, so
    /// the two must stay one-to-one: a re-insert, a hit, and a removal each
    /// have to retire the ticket they replace. A leaked ticket would evict a
    /// key that is no longer there and let the cache grow past its bound; a
    /// missing one would make an entry immortal.
    #[test]
    fn etag_cache_keeps_one_recency_ticket_per_entry() {
        let mut cache = EtagIndex::default();
        cache.insert("a".to_string(), "etag-a".to_string());
        cache.insert("b".to_string(), "etag-b".to_string());

        // Re-caching an id updates it in place.
        cache.insert("a".to_string(), "etag-a2".to_string());
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.order_len(), 2);
        assert_eq!(cache.get("a").as_deref(), Some("etag-a2"));
        assert_eq!(cache.order_len(), 2);

        cache.remove("a");
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.order_len(), 1);
        // Removing an absent id is a no-op, not a ticket leak.
        cache.remove("a");
        assert_eq!(cache.order_len(), 1);

        // The oldest surviving ticket still names a live entry: refill to
        // capacity and only "b" (never touched again) may be evicted.
        for index in 0..ETAG_INDEX_MAX_ENTRIES {
            cache.insert(format!("fill-{index}"), "etag".to_string());
        }
        assert_eq!(cache.len(), ETAG_INDEX_MAX_ENTRIES);
        assert_eq!(cache.order_len(), ETAG_INDEX_MAX_ENTRIES);
        assert!(!cache.contains_key("b"));
        assert!(cache.contains_key("fill-0"));
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
