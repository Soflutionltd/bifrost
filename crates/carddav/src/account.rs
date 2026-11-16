use crate::dialect::CardDavDialect;
#[cfg(test)]
use bifrost_dav_core::SnapshotEntry;
use bifrost_dav_core::append_path;
use bifrost_dav_core::collection::{COLLECTION_PAGE_SIZE, Dialect};
use bifrost_dav_core::collection_account::{
    CollectionSnapshot, DavCollectionAccount, IdleLifecycle, RelocateFailure,
};
use bifrost_dav_core::worse_recovery;
#[cfg(test)]
use bifrost_dav_core::{slice_after_watermark, sorted_candidate_hrefs};
#[cfg(test)]
use bifrost_types::ObjectType;
type Kernel = DavCollectionAccount<CardDavDialect>;
use bifrost_dav_core::same_dav_url;

use std::collections::HashSet;
use std::sync::Arc;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFuture, AccountId, AccountOperation, AccountStream, AddressBook, AddressBookId,
    AttachmentHandle, BlobHandle, ByteRange, Calendar, CalendarEvent, Change, ChangeCursor,
    CloudUploadMeta, ContactCard, ContactCorpus, ContactCreate, ContactId, ContactPatch,
    ContactProvenance, ContactSearchRequest, ContainerId, ContainerKind, ContainerList, CostClass,
    CursorDescriptor, CursorEstablishment, CursorScope, DiagnosticText, DirectoryCard,
    DirectoryGroup, DirectoryGroupId, DirectoryGroupMember, DraftHandle, DraftPatch, ErrorScope,
    EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation, FlagOp,
    HostedAttachment, HydratedObject, HydrationProjection, Identity, IdentityId, IdentityPatch,
    Importance, InternalCause, InternalErrorKind, InventoryEvent, InventoryPartition,
    InventoryPartitioning, ItemOutcome, MembershipScope, Message, MutationSuccess, MutationTarget,
    ObjectId, Page, PrimaryCause, Priority, Protocol, ProtocolKind, QuotaInfo, RsvpStatus,
    SearchRequest, SendReceipt, SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, SkippedScope, SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration,
    ThreadId, VacationConfig,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use uuid::Uuid;

use crate::CardDavConfig;
use crate::capabilities::carddav_capabilities;
use crate::client::{
    CardDavClient, FilteredHrefs, HrefQuery, PutCondition, contact_scope, extend_candidates,
    local_error, not_found_error, unsupported_error,
};
use crate::parse::AddressBookCollection;
use crate::parse::CardDavFetchedVCard;
#[cfg(test)]
use crate::parse::CardDavMultigetReport;
use crate::vcard::{
    VCardParseError, VCardWriteError, contact_from_vcard, vcard_from_create, vcard_from_patch,
};

/// The page size `contacts_list` serves.
///
/// Fixed rather than caller-chosen because the `Account` trait's
/// `contacts_list` takes an address book and a cursor and nothing else - there
/// is no request limit to honour. That is the shape of the trait method, not an
/// omission here; `contact_search`, whose request DOES carry a limit, uses this
/// only as the default.
const CONTACT_PAGE_SIZE: usize = COLLECTION_PAGE_SIZE;

#[derive(Debug)]
pub(crate) struct CardDavAccount {
    lifecycle: IdleLifecycle,
    client: Arc<CardDavClient>,
    capabilities: AccountCapabilities,
    addressbook_home: String,
    directory_gateways: Vec<String>,
    /// The collection a call that names no address book routes to, and `None`
    /// when the home enumerated no collections.
    ///
    /// Deliberately not the addressbook home in that case. See
    /// the shared kernel default-collection policy.
    pub(crate) default_addressbook_url: Option<String>,
    pub(crate) addressbook_urls: Vec<String>,
}

impl CardDavAccount {
    /// Build an account directly around a client, skipping discovery.
    ///
    /// Twin of `CalDavAccount::for_tests`: discovery is several round trips of
    /// its own and would dominate a test about what one call does.
    #[cfg(test)]
    pub(crate) fn for_tests(client: Arc<CardDavClient>, addressbook_home: &str) -> Self {
        Self::for_tests_with_collections(client, addressbook_home, Vec::new())
    }

    /// An account whose home enumerated several address books.
    ///
    /// Twin of `CalDavAccount::for_tests_with_collections`: the default book is
    /// the first discovered collection, and the extra urls follow it in
    /// discovery order.
    #[cfg(test)]
    pub(crate) fn for_tests_with_collections(
        client: Arc<CardDavClient>,
        default_addressbook_url: &str,
        additional_addressbook_urls: Vec<String>,
    ) -> Self {
        Self {
            lifecycle: IdleLifecycle::default(),
            client,
            capabilities: carddav_capabilities(),
            directory_gateways: Vec::new(),
            addressbook_home: default_addressbook_url.to_string(),
            default_addressbook_url: Some(default_addressbook_url.to_string()),
            addressbook_urls: std::iter::once(default_addressbook_url.to_string())
                .chain(additional_addressbook_urls.iter().cloned())
                .collect(),
        }
    }

    /// An account whose addressbook home enumerated no collections.
    ///
    /// The shape `open` produces against an empty backend: no default, and no
    /// discovered collection to fall back on.
    #[cfg(test)]
    pub(crate) fn for_tests_without_collections(client: Arc<CardDavClient>, home: &str) -> Self {
        Self {
            lifecycle: IdleLifecycle::default(),
            client,
            capabilities: carddav_capabilities(),
            directory_gateways: Vec::new(),
            addressbook_home: home.to_string(),
            default_addressbook_url: None,
            addressbook_urls: Vec::new(),
        }
    }

    pub(crate) async fn open(
        account_id: AccountId,
        config: CardDavConfig,
        net: Option<Arc<bifrost_net::Net>>,
    ) -> Result<Self, AccountError> {
        let client = CardDavClient::new_on_net(account_id, &config, net.as_deref());
        Self::open_with_client(client).await
    }

    /// The whole of `open` after the client exists.
    ///
    /// Split out so the discovery-to-account path can be driven against a
    /// scripted transport: `open` itself builds its client from a
    /// `CardDavConfig` and so cannot take one. Twin of
    /// `bifrost-caldav`'s `open_with_client`.
    pub(crate) async fn open_with_client(mut client: CardDavClient) -> Result<Self, AccountError> {
        let discovery = client.discover_addressbook_home().await?;
        let addressbook_home = discovery.home;
        // Principal-advertised directories are known before the home listing,
        // not only after the account is published.
        client.observe_directories(discovery.gateways.iter().cloned());
        client.admit_discovered_urls(
            std::iter::once(addressbook_home.clone()).chain(discovery.gateways.iter().cloned()),
        );
        let mut collections = if client.is_trusted_url(&addressbook_home)
            && client.is_directory_url(&addressbook_home)
        {
            // An advertised directory cannot be enumerated as a personal home.
            // Keep origin refusal intact for an unadmitted/downgraded home.
            Vec::new()
        } else {
            client.list_addressbooks(&addressbook_home).await?
        };
        let mut gateways = discovery.gateways;
        gateways.extend(
            collections
                .iter()
                .filter(|c| c.is_directory)
                .map(|c| c.href.clone()),
        );
        // Listing markers classify collections but cannot delegate origins.
        // Seed monotone exclusions, including refused origins. Later markers
        // can only tighten access; only origins already trusted at open may
        // participate in the frozen directory search surface.
        let excluded_directories = gateways.clone();
        gateways.retain(|url| client.is_trusted_url(url));
        crate::client::dedup_directory_urls(&mut gateways);
        client.initialize_directories(excluded_directories, gateways.clone());
        collections.retain(|c| !c.is_directory && !client.is_directory_url(&c.href));
        let default_addressbook_url = default_collection_url(&collections);
        let addressbook_urls = discovered_collection_urls(&collections);
        let mut capabilities = carddav_capabilities();
        capabilities.pim_methods.directory_search = !gateways.is_empty();
        Ok(Self {
            lifecycle: IdleLifecycle::default(),
            client: Arc::new(client),
            capabilities,
            addressbook_home,
            default_addressbook_url,
            addressbook_urls,
            directory_gateways: gateways,
        })
    }

    fn addressbook_url(
        client: &CardDavClient,
        default_addressbook_url: Option<&str>,
        address_book: Option<AddressBookId>,
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        if let Some(id) = address_book {
            return Ok(client.resolve_url(&id.0));
        }
        default_addressbook_url
            .map(str::to_string)
            .ok_or_else(|| Kernel::no_default(operation))
    }

    fn map_addressbook(collection: AddressBookCollection) -> AddressBook {
        let native = collection.href;
        let can_edit = collection.can_edit.unwrap_or(true);
        let name = collection
            .display_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| "Address Book".to_string());
        AddressBook {
            id: AddressBookId(native.clone()),
            native_id: native.clone(),
            name,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native,
                address_book_native: None,
            },
            corpus: ContactCorpus::Main,
            is_default: false,
            // Derived from `current-user-privilege-set`, as the CalDAV twin
            // derives its `can_*_events`. A server that does not answer the
            // property leaves `can_edit` `None` and the book is assumed
            // writable; one that answers it and names no write privilege is
            // reported read-only, so a consumer's capability gate stops a PUT
            // the server was always going to refuse with a 403.
            can_create_contacts: can_edit,
            can_update_contacts: can_edit,
            can_delete_contacts: can_edit,
        }
    }

    async fn fetch_contact_from_url(
        client: Arc<CardDavClient>,
        default_addressbook_url: Option<String>,
        address_book: Option<AddressBookId>,
        contact: ContactId,
        operation: AccountOperation,
    ) -> Result<ContactCard, AccountError> {
        // The contact's own collection is the first answer and almost always
        // available; the default is only reached for a native id whose parent
        // cannot be derived, and an account with no collections has none.
        let addressbook = match address_book {
            Some(address_book) => client.resolve_url(&address_book.0),
            None => contact_addressbook_url(&client, &contact)
                .or(default_addressbook_url)
                .ok_or_else(|| Kernel::no_default(operation))?,
        };
        client.refuse_directory_access(&addressbook, operation)?;
        client.refuse_directory_access(&client.resolve_url(&contact.0), operation)?;
        let card = Self::fetch_contact_resource(&client, &contact, operation).await?;
        contact_from_vcard(
            card.uri,
            Some(AddressBookId(addressbook)),
            card.etag,
            &card.data,
        )
        .map_err(|error| project_error(operation, &error))
    }

    /// Fetch one contact resource by addressing the resource itself.
    ///
    /// Twin of `bifrost-caldav`'s `fetch_event_from_url`. This used to REPORT an
    /// `addressbook-multiget` against the collection derived from the resource
    /// URL; the GET has no dependence on that derivation being right and reads
    /// the same validator from the `ETag` header. A missing resource still
    /// surfaces as `NotFound(Contact)` scoped to the id, as it did when the
    /// empty multiget report produced it.
    async fn fetch_contact_resource(
        client: &CardDavClient,
        contact: &ContactId,
        operation: AccountOperation,
    ) -> Result<CardDavFetchedVCard, AccountError> {
        let url = client.resolve_url(&contact.0);
        client.get_vcard(&url, operation).await.map_err(|error| {
            if matches!(error.kind(), AccountErrorKind::NotFound(_)) {
                not_found_error(error, contact.0.clone())
            } else {
                error
            }
        })
    }

    /// Move one contact resource into another address book collection.
    ///
    /// The shared kernel performs relocation. WebDAV
    /// `MOVE` is the atomic form and is tried first; a server that does not
    /// implement it falls back to PUT-to-new then DELETE-from-old, which is not
    /// atomic. Typed progress preserves the committed copy and failing leg.
    async fn relocate_contact(
        client: &CardDavClient,
        source_url: &str,
        target_addressbook: &str,
        data: String,
        content_changed: bool,
    ) -> Result<(), RelocateFailure> {
        client
            .refuse_directory_access(source_url, AccountOperation::ContactUpdate)
            .map_err(RelocateFailure::Before)?;
        client
            .refuse_directory_access(target_addressbook, AccountOperation::ContactUpdate)
            .map_err(RelocateFailure::Before)?;
        Kernel::relocate(
            client.collection_dispatch(),
            source_url,
            target_addressbook,
            data,
            content_changed,
            AccountOperation::ContactUpdate,
            || format!("{}.{}", Uuid::new_v4(), CardDavDialect::FILE_EXTENSION),
        )
        .await
    }

    /// The whole collection as candidate hrefs, from the depth-1 PROPFIND.
    ///
    /// The match-all lane's source, and the degrade the text lane falls back to
    /// when the server refuses to run its filter.
    async fn listing_candidates(
        client: &CardDavClient,
        addressbook: &str,
        operation: AccountOperation,
    ) -> Result<HrefQuery, AccountError> {
        let listing = client.list_contacts_listing(addressbook, operation).await?;
        let mut candidates = HrefQuery::default();
        extend_candidates(&mut candidates, listing);
        Ok(candidates)
    }

    /// Slice the page that follows `watermark` out of the candidate hrefs,
    /// multiget ONLY that page, and project it.
    ///
    /// Every paging lane in this crate funnels through here, which is what keeps
    /// one cursor valid across them: the key is always the resource href, which
    /// is also the contact's `native_id`, whether the candidates came from a
    /// server-side text-match or from the depth-1 listing. A text search that
    /// degrades mid-walk therefore neither re-serves nor skips.
    ///
    /// `keep` is the local match, and it is the AUTHORITY over the page even
    /// where the server already filtered - the server side is a prefilter that
    /// may be generous (or, on the degrade lane, absent entirely). A page is
    /// consequently allowed to be short, or empty, while still carrying a
    /// cursor.
    ///
    /// `failed_ids` carries both legs' casualties: the candidate leg's refused
    /// hrefs (collection-wide, and re-observed on every page per the
    /// `Page::failed_ids` contract) and the ones this page's own multiget lost.
    async fn hydrated_contacts_page<K: Fn(&ContactCard) -> bool>(
        client: &CardDavClient,
        addressbook: &str,
        candidates: HrefQuery,
        watermark: Option<&str>,
        page_size: usize,
        operation: AccountOperation,
        keep: K,
    ) -> Result<Page<ContactCard>, AccountError> {
        Kernel::hydrated_page(
            bifrost_dav_core::collection_account::PageRequest {
                collection: addressbook,
                candidates,
                watermark,
                page_size,
            },
            |hrefs| async move { client.fetch_vcards(addressbook, &hrefs, operation).await },
            |card| {
                contact_from_vcard(
                    card.uri,
                    Some(AddressBookId(addressbook.to_owned())),
                    card.etag,
                    &card.data,
                )
                .map(|card| vec![card])
            },
            keep,
            |card| card.native_id.as_str(),
        )
        .await
    }

    /// Snapshot one address book's contact listing plus its ctag.
    ///
    /// `ctag` says where the collection tag comes from, because the callers
    /// differ in what they already know:
    ///
    /// - [`CtagSource::Collection`] reads this collection at depth 0. Cursor
    ///   establishment and inventory use it: they have no prior cursor to
    ///   compare against. The response also carries `resourcetype`, allowing
    ///   the client to observe a directory marker without listing the home.
    /// - [`CtagSource::Known`] takes a value the caller already has. The poll
    ///   path uses it, because deciding whether to short-circuit at all
    ///   required asking for the ctag first, and re-deriving it would spend an
    ///   extra request.
    pub(crate) async fn contact_snapshot(
        client: &CardDavClient,
        ctag: CtagSource,
        addressbook: &str,
        operation: AccountOperation,
    ) -> Result<ContactSnapshot, AccountError> {
        client.refuse_directory_access(addressbook, operation)?;
        let ctag = match ctag {
            CtagSource::Collection => client.collection_ctag(addressbook, operation).await?,
            CtagSource::Known(ctag) => ctag,
        };
        let listing = client.list_contacts_listing(addressbook, operation).await?;
        Kernel::snapshot_from_listing(addressbook, ctag, listing, operation)
    }
}

/// Where [`CardDavAccount::contact_snapshot`] gets a collection's ctag.
pub(crate) enum CtagSource {
    /// Read it directly from the selected address book collection.
    Collection,
    /// Use a value the caller already resolved, spending no request.
    Known(Option<String>),
}

impl Account for CardDavAccount {
    bifrost_dav_core::dav_unsupported_account_methods!(CardDavDialect);

    fn protocol(&self) -> bifrost_types::Protocol {
        bifrost_types::Protocol::CardDav
    }

    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.client.set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<bifrost_types::BandwidthCap>) {
        self.client.set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::ServerCursor,
            freshness: None,
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        Kernel::discover_scopes(
            self.addressbook_urls
                .iter()
                .filter(|url| !self.client.is_directory_url(url))
                .cloned(),
        )
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<bifrost_types::ScopeLifecycleEvent> {
        self.lifecycle.stream()
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let client = Arc::clone(&self.client);
        Kernel::establish(
            scope,
            self.default_addressbook_url.clone(),
            self.addressbook_urls.clone(),
            move |collection| async move {
                Self::contact_snapshot(
                    &client,
                    CtagSource::Collection,
                    &collection,
                    AccountOperation::EstablishCursor,
                )
                .await
            },
        )
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        let (client, tally) = self.client.metered();
        Kernel::inventory(
            scope,
            self.default_addressbook_url.clone(),
            self.addressbook_urls.clone(),
            tally,
            move |collection| async move {
                Self::contact_snapshot(
                    &client,
                    CtagSource::Collection,
                    &collection,
                    AccountOperation::SyncInventory,
                )
                .await
            },
        )
    }

    fn inventory_partitioning(&self, _scope: &CursorScope) -> InventoryPartitioning {
        InventoryPartitioning::Full
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<InventoryEvent> {
        match partition {
            InventoryPartition::Full => self.inventory_stream(scope),
            _ => bifrost_types::account::unsupported_inventory_stream(
                AccountOperation::SyncInventory,
                self.protocol(),
            ),
        }
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let (client, tally) = self.client.metered();
        Kernel::changes(cursor, tally, move |previous| async move {
            changes_from_cursor(&client, &previous).await
        })
    }

    fn push_stream(
        &self,
        lane: bifrost_types::PushLane,
        _stop: bifrost_types::SessionStop,
        _gate: bifrost_types::PushGate,
    ) -> AccountStream<bifrost_types::PushSessionEvent> {
        Box::pin(futures::stream::iter([
            bifrost_types::PushSessionEvent::Failed(bifrost_types::push_lane_contract_error(
                self.protocol(),
                lane,
                "not a lane of this account",
            )),
        ]))
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        let client = Arc::clone(&self.client);
        let home = self.addressbook_home.clone();
        Box::pin(async move {
            let collections = client.list_addressbooks(&home).await?;
            let mut books = collections
                .into_iter()
                .filter(|c| !c.is_directory && !client.is_directory_url(&c.href))
                .map(Self::map_addressbook)
                .collect::<Vec<_>>();
            // An address book home that enumerates zero addressbook
            // collections surfaces as an empty list, not a fabricated
            // placeholder. The depth-1 PROPFIND above returns the home's own
            // response too, so a home that is itself an addressbook collection
            // is already mapped by the parse path. An empty result here
            // therefore means a genuinely empty backend; reporting it as empty
            // lets a consumer reap stale address books rather than chase a
            // phantom home-book whose queries a spec-correct server 404s.
            //
            // The phantom this replaces also advertised `can_create_contacts:
            // true`, so a consumer that trusted it and POSTed a vCard to the
            // home URL got a 404 or 405 it had done nothing to deserve.
            // `bifrost-caldav::calendars_list` removed the identical shape for
            // the identical reasons; the two must not drift apart again.
            if let Some(first) = books.first_mut() {
                first.is_default = true;
            }
            Ok(books)
        })
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let operation = AccountOperation::ContactsList;
            let addressbook = Self::addressbook_url(
                &client,
                default_addressbook_url.as_deref(),
                address_book,
                operation,
            )?;
            client.refuse_directory_access(&addressbook, operation)?;
            let watermark = Kernel::decode_page_cursor(page_cursor, operation)?;
            let candidates = Self::listing_candidates(&client, &addressbook, operation).await?;
            // No request limit: the trait's `contacts_list` takes an address
            // book and a cursor and nothing else, so the page size is fixed
            // here rather than chosen by the caller. That is the shape of the
            // trait method, not an omission in this crate - widening it is a
            // published-signature change in `bifrost-types`.
            Self::hydrated_contacts_page(
                &client,
                &addressbook,
                candidates,
                watermark.as_deref(),
                CONTACT_PAGE_SIZE,
                operation,
                |_| true,
            )
            .await
        })
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            Self::fetch_contact_from_url(
                client,
                default_addressbook_url,
                None,
                contact,
                AccountOperation::ContactGet,
            )
            .await
        })
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let addressbook = Self::addressbook_url(
                &client,
                default_addressbook_url.as_deref(),
                contact.address_book_id.clone(),
                AccountOperation::ContactCreate,
            )?;
            client.refuse_directory_access(&addressbook, AccountOperation::ContactCreate)?;
            // Keep contact identity independent of the resource filename, as
            // CalDAV does: users expect the same contact after a rename or move.
            let uid = Uuid::new_v4().to_string();
            let path = format!("{uid}.vcf");
            let url = append_path(&addressbook, &path);
            let data = vcard_from_create(&contact, &uid)
                .map_err(|error| write_error(AccountOperation::ContactCreate, error))?;
            client
                .put_vcard(
                    &url,
                    data,
                    PutCondition::IfNoneMatch,
                    AccountOperation::ContactCreate,
                )
                .await
                // The URL and the UUID are minted LOCALLY, so nothing else in
                // the failure names them. A dropped create-PUT derives
                // `Reconcile(CheckTarget)`, and a consumer told to check a
                // target the error does not identify can only re-create - two
                // copies of one contact, which is what the unreplayable
                // declaration exists to prevent. Twin of `event_create`'s.
                .map_err(|error| {
                    error
                        .clone()
                        .into_builder()
                        .scope(contact_scope(url.clone()))
                        .try_build()
                        .unwrap_or(error)
                })?;
            Ok(ContactId(url))
        })
    }

    // AccountOperation::ContactUpdate.skips_absent_target() is false: Outlook
    // users need to know that an edit to a vanished contact was not applied.
    // Preserve fetch, PUT and relocation failures, including missing destinations.
    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let expected_etag = patch
                .expected_version
                .as_ref()
                .map(|version| {
                    version.expected_etag(
                        bifrost_types::error::Protocol::CardDav,
                        AccountOperation::ContactUpdate,
                        true,
                    )
                })
                .transpose()?;

            client.refuse_directory_access(
                &client.resolve_url(&contact.0),
                AccountOperation::ContactUpdate,
            )?;
            if let Some(target) = &patch.address_book_id {
                client.refuse_directory_access(
                    &client.resolve_url(&target.0),
                    AccountOperation::ContactUpdate,
                )?;
            }
            // The contact's own collection is the first answer; the default is
            // only reached for a native id whose parent cannot be derived.
            let addressbook = match contact_addressbook_url(&client, &contact) {
                Some(addressbook) => addressbook,
                None => Self::addressbook_url(
                    &client,
                    default_addressbook_url.as_deref(),
                    None,
                    AccountOperation::ContactUpdate,
                )?,
            };
            // An `address_book_id` naming a collection other than the contact's
            // own is a relocation. It used to be refused; it is now performed.
            let relocation = patch
                .address_book_id
                .as_ref()
                .map(|target| client.resolve_url(&target.0))
                .filter(|target| !same_collection_url(target, &addressbook));
            if expected_etag.is_some() && relocation.is_some() {
                return Err(unsupported_error(AccountOperation::ContactUpdate));
            }
            let raw =
                Self::fetch_contact_resource(&client, &contact, AccountOperation::ContactUpdate)
                    .await?;
            let current = contact_from_vcard(
                raw.uri.clone(),
                Some(AddressBookId(addressbook)),
                raw.etag.clone(),
                &raw.data,
            )
            .map_err(|error| project_error(AccountOperation::ContactUpdate, &error))?;
            let data = vcard_from_patch(&current, &raw.data, &patch)
                .map_err(|error| write_error(AccountOperation::ContactUpdate, error))?;
            let url = client.resolve_url(&contact.0);
            let Some(target_addressbook) = relocation else {
                return client
                    .put_vcard(
                        &url,
                        data,
                        expected_etag.map_or_else(
                            || {
                                PutCondition::from_tag(
                                    current
                                        .version
                                        .as_ref()
                                        .and_then(bifrost_types::ServerVersion::entity_tag),
                                )
                            },
                            |etag| PutCondition::from_etag(Some(etag)),
                        ),
                        AccountOperation::ContactUpdate,
                    )
                    .await
                    .map(|_| ());
            };
            // A content patch riding along with the move needs a write of its
            // own; a move-only patch does not, and must not be charged a
            // partial-failure verdict for a leg it never needed.
            let content_changed = patch_changes_content(&patch);
            Self::relocate_contact(&client, &url, &target_addressbook, data, content_changed)
                .await
                .map_err(|failure| Kernel::relocation_error(failure, &url))
        })
    }

    // AccountOperation::ContactDelete.skips_absent_target() is false, but the
    // client's delete_vcard absorbs 404/410 before this door sees them. That
    // is coupled to replay: a retry after a landed DELETE can itself return
    // 404. Do not fake policy compliance with a preflight existence probe or
    // turn Ok into NotFound here; neither can distinguish that committed write
    // from prior absence. Conformance needs a client/dispatch replay change.
    // Conditional delete below already preserves the caller's failure.
    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            let url = client.resolve_url(&contact.0);
            client
                .delete_vcard(&url, AccountOperation::ContactDelete)
                .await
                .map(|_| ())
        })
    }

    fn contact_delete_if_version(
        &self,
        contact: ContactId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let Some(version) = expected_version else {
            return self.contact_delete(contact);
        };
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            let etag = version.expected_etag(
                bifrost_types::error::Protocol::CardDav,
                AccountOperation::ContactDelete,
                true,
            )?;
            let url = client.resolve_url(&contact.0);
            client
                .delete_vcard_if_match(&url, etag, AccountOperation::ContactDelete)
                .await
        })
    }

    /// Searches contacts by text, one page at a time.
    ///
    /// **An omitted `limit` defaults to `CONTACT_PAGE_SIZE` (250)**, so a
    /// consumer that leaves the field unset still walks the collection a page at
    /// a time and receives a `next_cursor` for the remainder. The CalDAV
    /// `events_in_range` and `event_search` defaults are also 250 candidates.
    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let operation = AccountOperation::ContactSearch;
            let addressbook = Self::addressbook_url(
                &client,
                default_addressbook_url.as_deref(),
                request.address_book_id.clone(),
                operation,
            )?;
            client.refuse_directory_access(&addressbook, operation)?;
            let watermark = Kernel::decode_page_cursor(request.page_cursor.clone(), operation)?;
            let needle = request.query.to_lowercase();
            // A zero limit is honored as an empty exhausted page, not clamped
            // up to one. Clamping silently served a contact the caller had
            // asked not to receive; the shared watermark slicer is what makes
            // the zero case terminate instead of pointing at itself.
            //
            // An EXPLICIT limit too large for a `usize` (reachable only where
            // `usize` is narrower than `u32`) saturates to `usize::MAX`, which
            // the slicer reads as "truncate nothing". That is deliberately NOT
            // clamped to `CONTACT_PAGE_SIZE`: the bounded default above is this
            // crate's answer for a caller who expressed no preference, while a
            // caller who names a limit larger than the machine can index has
            // asked for as much as it can give, and the two crates are pinned to
            // handle an explicit limit identically - clamping here would put a
            // divergence into the one half of the contract that has none.
            let page_size = request.limit.map_or(CONTACT_PAGE_SIZE, |limit| {
                usize::try_from(limit).unwrap_or(usize::MAX)
            });
            // Both lanes page the same way: candidate hrefs, sliced at the
            // watermark, and a multiget of only the page. They differ only in
            // where the candidates come from - a server-side text-match for a
            // real query, the depth-1 listing for a match-all or a server that
            // will not run the filter - which is what keeps one cursor valid
            // across a mid-walk degrade.
            let candidates = if needle.is_empty() {
                Self::listing_candidates(&client, &addressbook, operation).await?
            } else {
                match client
                    .query_vcard_hrefs_text(&addressbook, &request.query)
                    .await?
                {
                    FilteredHrefs::Matched(query) => query,
                    FilteredHrefs::FilterUnsupported => {
                        Self::listing_candidates(&client, &addressbook, operation).await?
                    }
                }
            };
            Self::hydrated_contacts_page(
                &client,
                &addressbook,
                candidates,
                watermark.as_deref(),
                page_size,
                operation,
                // The server-side text-match is a PREFILTER; the local match
                // stays the authority over the page. An empty needle matches
                // everything, which is the match-all lane.
                |contact| needle.is_empty() || contact_matches(contact, &needle),
            )
            .await
        })
    }

    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let operation = AccountOperation::DirectorySearch;
        if !self.capabilities.pim_methods.directory_search
            || (query.trim().is_empty() && !self.capabilities.pim_methods.directory_enumerate)
        {
            return Kernel::unsupported_future(operation);
        }
        let client = Arc::clone(&self.client);
        let gateways = self.directory_gateways.clone();
        Box::pin(async move {
            if page_cursor.is_some() {
                return Err(local_error(
                    operation,
                    "CardDAV directory search has no paging",
                ));
            }
            let limit = limit
                .unwrap_or(u32::try_from(CONTACT_PAGE_SIZE).expect("fixed page size fits u32"));
            if limit == 0 {
                return Ok(Page::single(Vec::new(), Vec::new(), Vec::new()));
            }
            // Ordered bounded fan-out: lexical gateway URL order, then wire
            // response order within each gateway. The first duplicate wins.
            let legs = gateways
                .iter()
                .cloned()
                .map(|url| {
                    let client = Arc::clone(&client);
                    let query = query.clone();
                    async move { client.query_directory(&url, &query, limit).await }
                })
                .collect::<Vec<_>>();
            let mut legs = stream::iter(legs).buffered(4);
            let mut page = Page::single(Vec::new(), Vec::new(), Vec::new());
            let mut seen = HashSet::new();
            let mut succeeded = false;
            let mut worst = None;
            let mut index = 0;
            while let Some(result) = legs.next().await {
                let url = &gateways[index];
                index += 1;
                match result {
                    Err(error) => {
                        let error = error
                            .into_builder()
                            .scope(ErrorScope::DirectoryGateway { url: url.clone() })
                            .try_build()
                            .expect("adding gateway scope preserves classification");
                        worst = worse_recovery(worst, error.clone());
                        page.skipped_scopes.push(SkippedScope {
                            scope: ErrorScope::DirectoryGateway { url: url.clone() },
                            error,
                        });
                    }
                    Ok((report, truncated)) => {
                        succeeded = true;
                        page.truncated |= truncated;
                        page.failed_ids
                            .extend(report.failed.into_iter().map(|f| f.href));
                        page.failed_ids.extend(report.missing_data);
                        for raw in report.items {
                            match crate::vcard::directory_from_vcard(&raw.data) {
                                Ok(Some(card)) => {
                                    // Directory identity folds the trimmed primary email
                                    // using Rust Unicode lowercase; aliases never dedup.
                                    if seen.insert(card.email.trim().to_lowercase()) {
                                        if page.items.len() < limit as usize {
                                            page.items.push(card);
                                        } else {
                                            page.truncated = true;
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => page.failed_ids.push(raw.uri),
                            }
                        }
                    }
                }
            }
            if !succeeded {
                // Open accounts only advertise this capability when at least
                // one gateway is admitted. Preserve an internal error if an
                // account constructed outside open violates that invariant.
                return Err(worst.unwrap_or_else(|| directory_gateway_invariant_error(operation)));
            }
            Ok(page)
        })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        Kernel::unsupported_future(AccountOperation::CalendarsList)
    }

    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Kernel::unsupported_future(AccountOperation::EventsInRange)
    }

    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        Kernel::unsupported_future(AccountOperation::EventGet)
    }

    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        Kernel::unsupported_future(AccountOperation::EventCreate)
    }

    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Kernel::unsupported_future(AccountOperation::EventUpdate)
    }

    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        Kernel::unsupported_future(AccountOperation::EventDelete)
    }

    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        Kernel::unsupported_future(AccountOperation::EventRsvp)
    }

    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Kernel::unsupported_future(AccountOperation::EventSearch)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        self.lifecycle.close();
        Box::pin(async { Ok(()) })
    }
}

/// Whether the patch changes anything other than which collection the contact
/// lives in.
///
/// Derived by zeroing the relocation field and comparing against an empty
/// patch, rather than enumerating the content fields: a field added to
/// `ContactPatch` is then covered automatically, where a hand-listed check
/// would silently stop noticing it.
///
/// Comparing SERIALIZED bytes is not equivalent and was tried first. The vCard
/// writer re-emits line endings, so a move-only patch compares unequal to the
/// fetched body and earns a redundant write plus the partial-failure verdict
/// that rides on it.
fn patch_changes_content(patch: &ContactPatch) -> bool {
    let content = ContactPatch {
        address_book_id: None,
        expected_version: None,
        ..patch.clone()
    };
    content != ContactPatch::default()
}

fn contact_addressbook_url(client: &CardDavClient, contact: &ContactId) -> Option<String> {
    let resolved = client.resolve_url(&contact.0);
    bifrost_dav_core::url::parent_collection_url(&resolved)
}

#[cfg(test)]
mod collection_url_tests {
    use super::*;
    use bifrost_dav_core::test_support::{dav_script_empty, scripted_dav_net};

    /// Twin of `bifrost-caldav::a_restated_calendar_url_is_not_a_relocation`:
    /// a restated address book id differing only in spelling must not read as
    /// a cross-book move.
    #[test]
    fn a_restated_address_book_url_is_not_a_relocation() {
        assert!(same_collection_url(
            "https://dav.example.test/books/My%20Book/",
            "https://DAV.example.test:443/books/My Book"
        ));
        assert!(!same_collection_url(
            "https://dav.example.test/books/work/",
            "https://dav.example.test/books/home/"
        ));
    }

    #[test]
    fn contact_addressbook_url_ignores_query_and_fragment_slashes() {
        let client = CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&dav_script_empty()),
        );
        assert_eq!(
            contact_addressbook_url(
                &client,
                &ContactId("https://dav.example.test/books/work/one.vcf?next=/x".to_string())
            )
            .as_deref(),
            Some("https://dav.example.test/books/work/")
        );
        assert_eq!(
            contact_addressbook_url(
                &client,
                &ContactId("https://dav.example.test/books/work/one.vcf#a/b".to_string())
            )
            .as_deref(),
            Some("https://dav.example.test/books/work/")
        );
    }
}

/// Resource identity for a consumer-restated address book id against a URL this
/// crate derived. Delegates to the shared normalizing comparison for the reason
/// `bifrost-caldav::same_url` gives: a spelling difference read as a relocation
/// issues a MOVE onto the collection the resource is already in.
fn same_collection_url(left: &str, right: &str) -> bool {
    same_dav_url(left, right)
}

/// Map a vCard projection failure to an `AccountError` for the single-resource
/// paths (get/update), where there is no listing to degrade a skip into.
///
/// Both callers project a card the SERVER sent, so a card that will not parse
/// is the provider's malformed response - `Protocol(ParseFailed)` with an
/// acknowledged attempt - and not a local refusal. It was `local_error`
/// (`Request(Malformed)` -> `ClientBug`), which blamed the caller for the
/// server's data. The encode side stays the caller's: a card built from the
/// caller's `ContactCreate` / `ContactPatch` is this crate's to express, and
/// nothing about a server's parse failure applies to it.
fn project_error(operation: AccountOperation, error: &VCardParseError) -> AccountError {
    crate::client::parse_error(
        operation,
        format!("CardDAV vCard could not be parsed: {}", error.0),
    )
}

/// A value the vCard writers refuse (`VCardWriteError`) is the caller's own
/// request being unwritable: local `Request(Malformed)` naming the field, raised
/// before any write goes out. The value itself is not echoed into the message,
/// so a hostile string cannot reach a log through it.
fn write_error(operation: AccountOperation, error: VCardWriteError) -> AccountError {
    local_error(
        operation,
        format!("contact {} {}", error.field, error.reason),
    )
}

fn directory_gateway_invariant_error(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Internal(InternalCause::new(
        InternalErrorKind::InvariantViolated,
        Some(DiagnosticText::support_only(
            "CardDAV directory search was enabled without an admitted gateway",
        )),
    )))
    .protocol(Protocol::CardDav)
    .operation(operation)
    .try_build()
    .expect("valid directory-search invariant classification")
}

type ContactSnapshot = CollectionSnapshot;

/// One polled address-book member. The shape, the cursor codec, the diff and
/// the page slicer are all shared with `bifrost-caldav` through
/// `bifrost-dav-core`; only the magic bytes and the token's name differ.
#[cfg(test)]
type ContactSnapshotEntry = SnapshotEntry;

/// The cursor-scope collection set: one entry per DISCOVERED address book, and
/// NOTHING when the home holds none.
///
/// Twin of bifrost-caldav's helper of the same name, and for the same reason:
/// the collection walk already returns the home itself when the home is
/// genuinely an address book, so an empty result means an empty backend.
/// Substituting the home URL would advertise a folder that does not exist and
/// point cursor establishment and inventory at a 404, and would contradict the
/// empty-home contract the listing APIs are pinned to.
fn discovered_collection_urls(collections: &[AddressBookCollection]) -> Vec<String> {
    Kernel::discovered_collections(
        collections
            .iter()
            .map(|collection| collection.href.as_str()),
    )
}

/// The collection a call that names no address book routes to: the first
/// discovered one, and `None` when the home enumerated none.
///
/// Deliberately has no access to the addressbook home, so the fallback this
/// replaced cannot be reintroduced here without also changing the signature.
/// The shared kernel deliberately has no home URL to substitute.
fn default_collection_url(collections: &[AddressBookCollection]) -> Option<String> {
    Kernel::default_collection(
        collections
            .iter()
            .map(|collection| collection.href.as_str()),
    )
}

#[cfg(test)]
fn merge_listing_failures(failed_ids: &mut Vec<String>, listing_failed_hrefs: Vec<String>) {
    failed_ids.extend(listing_failed_hrefs);
}

/// Publish a partially-refused walk as a skipped scope.
///
/// A REPORT leg that failed wholly means this address book was not fully
/// searched. The cards already collected stay valid, but "no more matches"
/// is not what happened, and the consumer needs the classified failure to
/// know whether to reauthorize, retry, or stop. `failed_ids` cannot carry
/// that: it is a bare list of resource ids with no recovery class, and the
/// refused leg often does not even name the resources it lost.
/// `ErrorScope::ContactCollection` carries no id of its own; the refused
/// address book is the one named by the request, and the error's own
/// diagnostics carry the URL.
#[cfg(test)]
fn skipped_addressbook_scope(degraded: Option<AccountError>) -> Vec<SkippedScope> {
    Kernel::skipped_scope("", degraded)
}

/// Project a batch of hydrated vCard resources, splitting successful
/// cards from the native ids of resources that could not be parsed.
///
/// A resource the server returned but whose body will not project is
/// recorded in the returned `failed_ids` rather than dropped, so the
/// consumer can tell a transient per-resource hydration failure apart
/// from a real remote deletion and preserve the row instead of
/// destroying it. The captured id is the same `uri` a successful card
/// would carry as its native id.
/// Parsed multiget reports already use the absolute native-id namespace.
/// DAV response hrefs are rebased at the XML decoding boundary, before this
/// account layer can place them in success or failure lanes.
#[cfg(test)]
fn resolved_report(report: CardDavMultigetReport) -> (Vec<CardDavFetchedVCard>, Vec<String>) {
    let cards = report.items;
    let failed = report
        .failed
        .into_iter()
        .map(|failure| failure.href)
        .chain(report.missing_data)
        .collect();
    (cards, failed)
}

#[cfg(test)]
fn partition_hydrated_vcards(
    addressbook: &str,
    fetched: Vec<CardDavFetchedVCard>,
) -> (Vec<ContactCard>, Vec<String>) {
    let mut cards = Vec::new();
    let mut failed_ids = Vec::new();
    for card in fetched {
        let native = card.uri.clone();
        match contact_from_vcard(
            card.uri,
            Some(AddressBookId(addressbook.to_string())),
            card.etag,
            &card.data,
        ) {
            Ok(contact) => cards.push(contact),
            Err(_) => failed_ids.push(native),
        }
    }
    (cards, failed_ids)
}

fn contact_matches(contact: &ContactCard, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    contact
        .display_name
        .as_deref()
        .is_some_and(|value| contains(value, needle))
        || contact
            .emails
            .iter()
            .any(|email| contains(&email.value, needle))
        || contact
            .phones
            .iter()
            .any(|phone| contains(&phone.value, needle))
        || contact.organizations.iter().any(|org| {
            contains(&org.name, needle)
                || org
                    .title
                    .as_deref()
                    .is_some_and(|title| contains(title, needle))
        })
        || contact.addresses.iter().any(|address| {
            address
                .formatted
                .as_deref()
                .is_some_and(|value| contains(value, needle))
                || address.street.iter().any(|value| contains(value, needle))
                || address
                    .locality
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .region
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .postal_code
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .country
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
        })
        || contact
            .notes
            .as_deref()
            .is_some_and(|value| contains(value, needle))
}

fn contains(value: &str, needle: &str) -> bool {
    // Unicode case folding (matching CalDAV's `event_matches`) so a
    // search needle matches differently-cased non-ASCII letters in
    // international names; `to_ascii_lowercase` only folds A-Z.
    value.to_lowercase().contains(needle)
}

/// Poll the collection token once. An unchanged ctag needs no member listing;
/// an incomplete observation retains the old token for a later retry.
async fn changes_from_cursor(
    client: &CardDavClient,
    previous: &ContactSnapshot,
) -> Result<(ContactSnapshot, Vec<Change>), AccountError> {
    let token = client
        .collection_ctag(&previous.collection_url, AccountOperation::SyncChanges)
        .await?;
    if previous
        .token
        .as_deref()
        .zip(token.as_deref())
        .is_some_and(|(old, new)| old == new)
    {
        return Ok((previous.clone(), Vec::new()));
    }
    let mut current = CardDavAccount::contact_snapshot(
        client,
        CtagSource::Known(token),
        &previous.collection_url,
        AccountOperation::SyncChanges,
    )
    .await?;
    current.preserve_unobserved(previous);
    let changes = current.changes_from(previous);
    Ok((current, changes))
}

#[cfg(test)]
mod tests {
    use super::*;

    use bifrost_dav_core::DavResponse;
    use bifrost_dav_core::test_support::{
        dav_script, dav_script_empty, scripted_dav_net, transcripts,
    };
    use bifrost_types::{
        AccountErrorKind, EngineDirective, ObjectChange, ObjectChangeKind, RecoveryClass,
        SyncStateErrorKind,
    };
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;

    #[tokio::test]
    async fn lifecycle_subscriptions_wait_until_close_and_wake_together() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Context, Poll, Wake, Waker};

        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let client = CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&dav_script_empty()),
        );
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/collection/");
        let mut first = account.scope_lifecycle_stream();
        let mut second = account.scope_lifecycle_stream();
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let second_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let second_waker = Waker::from(Arc::clone(&second_wakes));
        let mut second_context = Context::from_waker(&second_waker);
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            first.as_mut().poll_next(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            second.as_mut().poll_next(&mut second_context),
            Poll::Pending
        ));

        account.close().await.expect("close succeeds");
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert!(second_wakes.0.load(Ordering::SeqCst) > 0);
        assert!(first.next().await.is_none());
        assert!(second.next().await.is_none());
        assert!(account.scope_lifecycle_stream().next().await.is_none());
        account.close().await.expect("repeated close succeeds");
    }

    fn book_multistatus(body: String) -> DavResponse {
        DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/books/")
                .expect("test DAV URL"),
        }
    }

    fn listed_vcard(name: &str) -> String {
        format!(
            "<D:response><D:href>/books/{name}.vcf</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status>\
</D:propstat></D:response>"
        )
    }

    #[tokio::test]
    async fn inventory_batch_counts_the_response_bodies_it_consumes() {
        let addressbook_home = "https://dav.example.test/books/";
        let addressbook = "https://dav.example.test/books/ada/";
        let collection_response = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav" xmlns:CS="http://calendarserver.org/ns/">
              <D:response><D:href>/books/ada/</D:href><D:propstat><D:prop>
                <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
                <CS:getctag>42</CS:getctag>
              </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
            </D:multistatus>"#
                .to_string(),
            url: bifrost_dav_core::DavHref::parse(addressbook).expect("test DAV URL"),
        };
        let contacts_response = book_multistatus(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href>/books/ada/one.vcf</D:href><D:propstat><D:prop><D:getetag>"e1"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#
                .to_string(),
        );
        let expected_bytes = (collection_response.body.len() + contacts_response.body.len()) as u64;
        let script = dav_script([collection_response, contacts_response]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let mut account = CardDavAccount::for_tests_with_collections(client, addressbook, vec![]);
        account.addressbook_home = addressbook_home.to_string();
        let scope = CursorScope::Folder(bifrost_types::FolderId(addressbook.to_string()));
        let events = account.inventory_stream(scope).collect::<Vec<_>>().await;

        let InventoryEvent::Batch(batch) = &events[0] else {
            panic!("expected inventory batch: {events:?}");
        };
        assert_eq!(batch.bytes_in, expected_bytes);

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, addressbook);
        assert_eq!(
            requests[0]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert!(requests[0].body.contains("<D:resourcetype/>"));
        assert_eq!(requests[1].url, addressbook);
        assert_eq!(
            requests[1]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[tokio::test]
    async fn cursor_establishment_reads_ctag_from_its_collection() {
        let home = "https://dav.example.test/books/";
        let addressbook = "https://dav.example.test/books/ada/";
        let collection = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav" xmlns:CS="http://calendarserver.org/ns/">
              <D:response><D:href>/books/ada/</D:href><D:propstat><D:prop>
                <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
                <CS:getctag>42</CS:getctag>
              </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
            </D:multistatus>"#
                .to_string(),
            url: bifrost_dav_core::DavHref::parse(addressbook).expect("test DAV URL"),
        };
        let contacts = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href>/books/ada/one.vcf</D:href>
              <D:propstat><D:prop><D:getetag>"e1"</D:getetag></D:prop>
              <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#
                .to_string(),
            url: bifrost_dav_core::DavHref::parse(addressbook).expect("test DAV URL"),
        };
        let script = dav_script([collection, contacts]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let mut account = CardDavAccount::for_tests_with_collections(client, addressbook, vec![]);
        account.addressbook_home = home.to_string();

        account
            .establish_initial_cursor(CursorScope::Folder(bifrost_types::FolderId(
                addressbook.to_string(),
            )))
            .await
            .expect("collection cursor establishes");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, addressbook);
        assert_eq!(
            requests[0]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert_eq!(requests[1].url, addressbook);
        assert_eq!(
            requests[1]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[tokio::test]
    async fn unchanged_ctag_batch_counts_redirect_and_token_bodies() {
        let addressbook = "https://dav.example.test/books/";
        let response = book_multistatus(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
              <D:response><D:href>/books/</D:href><D:propstat><D:prop>
                <CS:getctag>42</CS:getctag>
              </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
            </D:multistatus>"#
                .to_string(),
        );
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::LOCATION, "/books/".parse().unwrap());
        let redirect = bifrost_dav_core::DavResponse {
            status: reqwest::StatusCode::TEMPORARY_REDIRECT,
            headers,
            body: "redirect body".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let expected_bytes = (redirect.body.len() + response.body.len()) as u64;
        let script = dav_script([redirect, response]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = CardDavAccount::for_tests(client, addressbook);
        let previous = ContactSnapshot {
            collection_url: addressbook.to_string(),
            token: Some("42".to_string()),
            ..ContactSnapshot::default()
        };
        let scope = CursorScope::Folder(bifrost_types::FolderId(addressbook.to_string()));
        let cursor = Kernel::cursor_from_snapshot(scope, &previous);
        let events = account.changes_stream(cursor).collect::<Vec<_>>().await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("expected changes batch: {events:?}");
        };
        assert_eq!(batch.bytes_in, expected_bytes);
    }

    fn hydrated_vcard(name: &str) -> String {
        format!(
            "<D:response><D:href>/books/{name}.vcf</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag>\
<C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:{name}\nEND:VCARD</C:address-data>\
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        )
    }

    /// A hydrated card whose `FN` is chosen by the caller, so a test can make
    /// the local match agree or disagree with the server's filter.
    fn hydrated_vcard_named(name: &str, full_name: &str) -> String {
        format!(
            "<D:response><D:href>/books/{name}.vcf</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag>\
<C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:{full_name}\nEND:VCARD</C:address-data>\
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        )
    }

    /// A member the server named but refused inside the 207.
    fn refused_vcard(name: &str) -> String {
        format!(
            "<D:response><D:href>/books/{name}.vcf</D:href><D:propstat><D:prop>\
<D:getetag/></D:prop><D:status>HTTP/1.1 403 Forbidden</D:status>\
</D:propstat></D:response>"
        )
    }

    /// A member the server refused with a named status, so the failure lane's
    /// classification can be read.
    fn refused_vcard_with(name: &str, status: u16, reason: &str) -> String {
        format!(
            "<D:response><D:href>/books/{name}.vcf</D:href><D:propstat><D:prop>\
<D:getetag/></D:prop><D:status>HTTP/1.1 {status} {reason}</D:status>\
</D:propstat></D:response>"
        )
    }

    fn card_ids(page: &Page<ContactCard>) -> Vec<String> {
        page.items
            .iter()
            .map(|card| card.native_id.clone())
            .collect()
    }

    fn text_search(query: &str, limit: u32) -> ContactSearchRequest {
        ContactSearchRequest {
            query: query.to_string(),
            address_book_id: None,
            limit: Some(limit),
            page_cursor: None,
        }
    }

    /// The dav-B8 residual, closed on this side: a text `contact_search` pushes
    /// the filter to the server, which answers with hrefs, and only the sliced
    /// page is hydrated. The eight legs name overlapping resources, so the
    /// candidate set is only right if it is deduped.
    #[tokio::test]
    async fn a_text_search_page_filters_on_the_server_and_multigets_only_the_page() {
        let query = book_document(&[
            listed_vcard("a"),
            listed_vcard("b"),
            listed_vcard("c"),
            refused_vcard("d"),
        ]);
        let mut responses = vec![book_multistatus(query); 8];
        responses.push(book_multistatus(book_document(&[
            hydrated_vcard_named("a", "plan a"),
            hydrated_vcard_named("b", "plan b"),
        ])));
        let script = dav_script(responses);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contact_search(text_search("plan", 2))
            .await
            .expect("text search page");

        assert_eq!(
            card_ids(&page),
            vec![
                "https://dav.example.test/books/a.vcf".to_string(),
                "https://dav.example.test/books/b.vcf".to_string(),
            ]
        );
        assert_eq!(
            page.next_cursor,
            Some(b"https://dav.example.test/books/b.vcf".to_vec().into()),
            "the cursor is the last href served"
        );
        assert_eq!(page.estimated_total, Some(3), "three candidates, not 24");
        assert_eq!(
            page.failed_ids,
            vec!["https://dav.example.test/books/d.vcf".to_string()],
            "a member the query refused is reported on the page it was observed on"
        );

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 9, "eight query legs and one multiget");
        for request in &requests[..8] {
            assert!(
                request.body.contains("<C:text-match"),
                "the filter must reach the server: {}",
                request.body
            );
            assert!(
                !request.body.contains("<C:address-data/>"),
                "the filtered query must not hydrate: {}",
                request.body
            );
        }
        assert!(
            !requests[8].body.contains("/books/c.vcf"),
            "the multiget must not hydrate a member this page does not serve: {}",
            requests[8].body
        );
    }

    /// A candidate 207 in which EVERY response failed is a complete failure,
    /// not an empty page - the CalDAV twin's rule, closed here for the same
    /// reason. The listing failure lane carries the per-member status, so the
    /// candidate leg runs the same RFC 4918 s13 ladder the multiget lanes use
    /// and a 507 reaches the consumer as a quota condition rather than as an
    /// empty search that a consumer records as a completed walk.
    ///
    /// The script holds only the eight query legs: a lane that went on to
    /// multiget the empty page would starve it and panic.
    #[tokio::test]
    async fn an_all_refused_query_207_classifies_rather_than_serving_an_empty_page() {
        let query = book_document(&[
            refused_vcard_with("a", 507, "Insufficient Storage"),
            refused_vcard_with("b", 507, "Insufficient Storage"),
        ]);
        let script = dav_script(vec![book_multistatus(query); 8]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let error = account
            .contact_search(text_search("plan", 10))
            .await
            .expect_err("an all-refused candidate 207 is a classified failure");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Server(bifrost_types::ServerErrorKind::QuotaExhausted),
            "the member status drives the classification"
        );
    }

    /// The other half of the same rule: a member refused BESIDE members that
    /// answered stays a per-id failure and the page is served. A 403 is the
    /// sharpest case, because alone it would classify as `NoPermission`.
    #[tokio::test]
    async fn a_partly_refused_query_207_still_serves_the_page() {
        let query = book_document(&[listed_vcard("a"), refused_vcard("b"), listed_vcard("c")]);
        let mut responses = vec![book_multistatus(query); 8];
        responses.push(book_multistatus(book_document(&[
            hydrated_vcard_named("a", "plan a"),
            hydrated_vcard_named("c", "plan c"),
        ])));
        let script = dav_script(responses);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contact_search(text_search("plan", 10))
            .await
            .expect("a partially refused 207 is still a page");

        assert_eq!(
            card_ids(&page),
            vec![
                "https://dav.example.test/books/a.vcf".to_string(),
                "https://dav.example.test/books/c.vcf".to_string(),
            ]
        );
        assert_eq!(
            page.failed_ids,
            vec!["https://dav.example.test/books/b.vcf".to_string()],
            "the refused member is per-id news, not a page failure"
        );
    }

    /// The server filter is a PREFILTER; the local match is the authority over
    /// the page. A generous server (a collation that folds more than Rust's
    /// lowercase does, or a property this crate does not project) must not put
    /// a non-matching card into the results.
    #[tokio::test]
    async fn the_local_match_is_the_authority_over_a_generous_server_filter() {
        let query = book_document(&[listed_vcard("a"), listed_vcard("b")]);
        let mut responses = vec![book_multistatus(query); 8];
        responses.push(book_multistatus(book_document(&[
            hydrated_vcard_named("a", "plan a"),
            hydrated_vcard_named("b", "unrelated"),
        ])));
        let script = dav_script(responses);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contact_search(text_search("plan", 10))
            .await
            .expect("text search page");

        assert_eq!(
            card_ids(&page),
            vec!["https://dav.example.test/books/a.vcf".to_string()],
            "a card the server offered but the local match rejects is not served"
        );
    }

    /// One leg reporting an unsupported filter degrades the WHOLE lane to the
    /// listing. Answering out of the properties a server happened to accept
    /// would narrow the search with no signal to the consumer.
    #[tokio::test]
    async fn a_text_search_degrades_when_one_query_leg_refuses_the_filter() {
        let query = book_document(&[listed_vcard("a")]);
        let mut responses = vec![DavResponse {
            status: StatusCode::BAD_REQUEST,
            headers: HeaderMap::new(),
            body: String::new(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/books/")
                .expect("test DAV URL"),
        }];
        responses.extend(vec![book_multistatus(query); 7]);
        responses.push(book_multistatus(book_document(&[
            listed_vcard("a"),
            listed_vcard("b"),
        ])));
        responses.push(book_multistatus(book_document(&[hydrated_vcard_named(
            "a", "plan a",
        )])));
        let script = dav_script(responses);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contact_search(text_search("plan", 1))
            .await
            .expect("a refused filter degrades rather than failing");

        assert_eq!(
            card_ids(&page),
            vec!["https://dav.example.test/books/a.vcf".to_string()]
        );
        assert_eq!(
            page.next_cursor,
            Some(b"https://dav.example.test/books/a.vcf".to_vec().into()),
            "the degrade lane keys the cursor on the href too, so a mid-walk \
             degrade neither re-serves nor skips"
        );

        let requests = transcripts(&script);
        assert_eq!(
            requests.len(),
            10,
            "eight legs, then the listing and the page"
        );
        assert_eq!(requests[8].method.as_str(), "PROPFIND");
    }

    /// Every page reruns the remote walk, so the failure lane is re-observed
    /// per page: a resource that only starts failing while the consumer is on
    /// page two is reported on page two, and only there.
    #[tokio::test]
    async fn a_failure_first_seen_on_a_later_page_is_still_reported() {
        let listing = book_document(&[listed_vcard("a"), listed_vcard("b")]);
        let script = dav_script([
            book_multistatus(listing.clone()),
            book_multistatus(book_document(&[hydrated_vcard("a")])),
            book_multistatus(book_document(&[listed_vcard("a"), refused_vcard("b")])),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let first = account
            .contact_search(ContactSearchRequest {
                query: String::new(),
                address_book_id: None,
                limit: Some(1),
                page_cursor: None,
            })
            .await
            .expect("page one");
        assert!(first.failed_ids.is_empty());
        let cursor = first.next_cursor.expect("page one truncates");

        let second = account
            .contact_search(ContactSearchRequest {
                query: String::new(),
                address_book_id: None,
                limit: Some(1),
                page_cursor: Some(cursor),
            })
            .await
            .expect("page two");
        assert_eq!(
            second.failed_ids,
            vec!["https://dav.example.test/books/b.vcf".to_string()]
        );
    }

    /// The BOUNDED default (dav-F7), pinned by what a consumer observes rather
    /// than by the constant's value: a `contact_search` with no `limit` serves
    /// at most 250 candidates and issues a cursor for the remainder. The
    /// collection is deliberately larger than the bound, because a fixture that
    /// fits inside it would pass against an unbounded default too.
    ///
    /// This test fails if the default becomes unbounded: 300 members would
    /// then all be served in a continuation-less page, and the sixth multiget chunk would exhaust the
    /// script and panic.
    #[tokio::test]
    async fn an_omitted_search_limit_serves_one_bounded_page_and_a_cursor() {
        const MEMBERS: usize = 300;
        const DEFAULT_PAGE: usize = 250;
        let names = (0..MEMBERS)
            .map(|index| format!("c{index:03}"))
            .collect::<Vec<_>>();
        let listing = book_document(
            &names
                .iter()
                .map(|name| listed_vcard(name))
                .collect::<Vec<_>>(),
        );
        let mut responses = vec![book_multistatus(listing)];
        // One scripted answer per multiget chunk, each hydrating its own share
        // of the page, so the merged result is the same set however the
        // bounded-concurrency dispatch pairs legs with answers. Exactly the
        // chunks a 250-card page needs are scripted: a lane that pages the
        // whole collection asks for a sixth and starves the script.
        for chunk in names[..DEFAULT_PAGE].chunks(50) {
            responses.push(book_multistatus(book_document(
                &chunk
                    .iter()
                    .map(|name| hydrated_vcard(name))
                    .collect::<Vec<_>>(),
            )));
        }
        let script = dav_script(responses);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contact_search(ContactSearchRequest {
                query: String::new(),
                address_book_id: None,
                limit: None,
                page_cursor: None,
            })
            .await
            .expect("an omitted limit is a bounded page, not a failure");

        assert_eq!(
            page.items.len(),
            DEFAULT_PAGE,
            "an omitted limit is bounded by the crate's own page size"
        );
        assert_eq!(
            page.items.first().map(|card| card.native_id.clone()),
            Some("https://dav.example.test/books/c000.vcf".to_string())
        );
        assert_eq!(
            page.items.last().map(|card| card.native_id.clone()),
            Some("https://dav.example.test/books/c249.vcf".to_string())
        );
        assert_eq!(
            page.next_cursor,
            Some(b"https://dav.example.test/books/c249.vcf".to_vec().into()),
            "a bounded default must hand back a continuation for the remainder"
        );
        assert_eq!(page.estimated_total, Some(300));
        assert_eq!(
            transcripts(&script).len(),
            6,
            "one listing and five multiget chunks: the page, not the collection"
        );
    }

    fn book_document(responses: &[String]) -> String {
        format!(
            "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">{}</D:multistatus>",
            responses.concat()
        )
    }

    /// The property the watermark cursor was bought for: an empty-query
    /// `contact_search` page multigets ONLY the hrefs it is about to serve.
    ///
    /// The lane used to re-hydrate the whole address book on every page and
    /// then slice `limit` cards out of it. The assertion is written against the
    /// REPORT body because that is where a regression shows: a page that goes
    /// back to hydrating everything names `c.vcf` in it.
    #[tokio::test]
    async fn an_empty_query_contact_page_multigets_only_the_page_hrefs() {
        let script = dav_script([
            book_multistatus(book_document(&[
                listed_vcard("a"),
                listed_vcard("b"),
                listed_vcard("c"),
            ])),
            book_multistatus(book_document(&[hydrated_vcard("a"), hydrated_vcard("b")])),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contact_search(ContactSearchRequest {
                query: String::new(),
                address_book_id: None,
                limit: Some(2),
                page_cursor: None,
            })
            .await
            .expect("match-all page");

        assert_eq!(
            page.items
                .iter()
                .map(|card| card.native_id.clone())
                .collect::<Vec<_>>(),
            vec![
                "https://dav.example.test/books/a.vcf".to_string(),
                "https://dav.example.test/books/b.vcf".to_string(),
            ]
        );
        assert_eq!(
            page.next_cursor,
            Some(b"https://dav.example.test/books/b.vcf".to_vec().into()),
            "the cursor is the last href served"
        );
        assert_eq!(page.estimated_total, Some(3));

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2, "one listing and one multiget");
        assert_eq!(requests[0].method.as_str(), "PROPFIND");
        assert_eq!(requests[1].method.as_str(), "REPORT");
        assert!(requests[1].body.contains("/books/a.vcf"));
        assert!(requests[1].body.contains("/books/b.vcf"));
        assert!(
            !requests[1].body.contains("/books/c.vcf"),
            "the multiget must not hydrate a member this page does not serve: {}",
            requests[1].body
        );
    }

    /// The continuation half, and the exactly-once property under a concurrent
    /// insert: a contact filed BEFORE the watermark between the two pages does
    /// not push the unserved remainder behind the cursor, and is not re-served.
    /// Against an integer offset, `aa` arriving here makes page two return
    /// `b.vcf` again and `c.vcf` is never delivered.
    #[tokio::test]
    async fn a_contact_inserted_before_the_watermark_does_not_displace_the_next_page() {
        let script = dav_script([
            book_multistatus(book_document(&[
                listed_vcard("a"),
                listed_vcard("aa"),
                listed_vcard("b"),
                listed_vcard("c"),
            ])),
            book_multistatus(book_document(&[hydrated_vcard("c")])),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contact_search(ContactSearchRequest {
                query: String::new(),
                address_book_id: None,
                limit: Some(2),
                page_cursor: Some(b"https://dav.example.test/books/b.vcf".to_vec().into()),
            })
            .await
            .expect("continued page");

        assert_eq!(
            page.items
                .iter()
                .map(|card| card.native_id.clone())
                .collect::<Vec<_>>(),
            vec!["https://dav.example.test/books/c.vcf".to_string()]
        );
        assert_eq!(page.next_cursor, None, "the last page ends the walk");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert!(
            !requests[1].body.contains("/books/a.vcf")
                && !requests[1].body.contains("/books/aa.vcf")
                && !requests[1].body.contains("/books/b.vcf"),
            "a member behind the watermark must not be re-served: {}",
            requests[1].body
        );
        assert!(requests[1].body.contains("/books/c.vcf"));
    }

    /// A watermark past every href is the end of the walk: no multiget at all,
    /// an empty page, and no continuation. The single-response script is the
    /// assertion - a page that still hydrates something starves it and panics.
    #[tokio::test]
    async fn a_watermark_past_every_href_is_an_empty_final_page() {
        let script = dav_script([book_multistatus(book_document(&[
            listed_vcard("a"),
            listed_vcard("b"),
        ]))]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        let page = account
            .contacts_list(
                None,
                Some(b"https://dav.example.test/books/z.vcf".to_vec().into()),
            )
            .await
            .expect("final page");

        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
        assert_eq!(transcripts(&script).len(), 1, "no multiget is spent");
    }

    /// `set_priority` and `set_bandwidth_cap` reach the account's transport.
    ///
    /// Twin of bifrost-caldav's; keep them in step. Both were silent no-ops for
    /// as long as this crate ran its own reqwest client, so an IMAP-shaped
    /// account composed `with_carddav` and given a bandwidth cap did not cap its
    /// DAV legs. This is the door dav-B9 exists to open.
    #[tokio::test]
    async fn the_priority_and_bandwidth_doors_reach_the_transport() {
        let net = scripted_dav_net(&dav_script_empty());
        let client = CardDavClient::with_account_net("https://dav.example.test", net.clone());
        let account =
            CardDavAccount::for_tests(Arc::new(client), "https://dav.example.test/books/");

        account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(4_096));
        assert_eq!(net.bandwidth_cap(), Some(4_096));
        account.set_priority(Priority::Background);
        assert_eq!(net.priority(), Priority::Background);

        // And back off again, so a test that only ever sets a value cannot
        // pass against a door wired to a constant.
        account.set_bandwidth_cap(None);
        assert_eq!(net.bandwidth_cap(), None);
        account.set_priority(Priority::Foreground);
        assert_eq!(net.priority(), Priority::Foreground);
    }

    /// The privilege set reaches the published `AddressBook` flags. Twin of
    /// the CalDAV mapping; these three were hardcoded `true`, so a read-only
    /// shared book advertised writable and the consumer's capability gate
    /// passed a PUT the server refuses.
    #[test]
    fn a_read_only_address_book_is_not_advertised_as_writable() {
        let read_only = CardDavAccount::map_addressbook(AddressBookCollection {
            is_directory: false,
            href: "https://dav.example.test/contacts/shared/".to_string(),
            display_name: None,
            ctag: None,
            can_edit: Some(false),
        });
        assert!(!read_only.can_create_contacts);
        assert!(!read_only.can_update_contacts);
        assert!(!read_only.can_delete_contacts);

        let unknown = CardDavAccount::map_addressbook(collection(
            "https://dav.example.test/contacts/personal/",
        ));
        assert!(
            unknown.can_create_contacts,
            "an unanswered privilege set must not lock the user out"
        );
    }

    fn collection(href: &str) -> AddressBookCollection {
        AddressBookCollection {
            is_directory: false,
            href: href.to_string(),
            display_name: None,
            ctag: None,
            can_edit: None,
        }
    }

    /// An empty address book home yields NO cursor scopes. Twin of the CalDAV
    /// assertion: an empty walk is an empty backend, and a fabricated home
    /// scope would point cursor establishment and inventory at a 404.
    #[test]
    fn an_empty_home_produces_no_cursor_scope_collections() {
        assert!(discovered_collection_urls(&[]).is_empty());
    }

    /// An empty home leaves the account with NO default address book, rather
    /// than the addressbook home standing in for one.
    ///
    /// This is the `open`-side half of
    /// `an_empty_backend_refuses_collection_less_calls_before_the_wire`, which
    /// pins what a `None` default does but constructs it directly. Without this
    /// assertion, restoring the home fallback here would leave that test
    /// passing. Twin of the CalDAV assertion; keep them in step.
    #[test]
    fn an_empty_home_leaves_no_default_address_book() {
        assert_eq!(default_collection_url(&[]), None);
        assert_eq!(
            default_collection_url(&[
                collection("https://dav.example.test/books/work/"),
                collection("https://dav.example.test/books/personal/"),
            ])
            .as_deref(),
            Some("https://dav.example.test/books/work/")
        );
    }

    #[test]
    fn every_discovered_address_book_becomes_a_cursor_scope_collection() {
        assert_eq!(
            discovered_collection_urls(&[
                collection("https://dav.example.test/books/work/"),
                collection("https://dav.example.test/books/personal/"),
            ]),
            vec![
                "https://dav.example.test/books/work/".to_string(),
                "https://dav.example.test/books/personal/".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn every_address_book_is_discovered_as_a_cursor_scope() {
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&dav_script_empty()),
        ));
        let account = CardDavAccount::for_tests_with_collections(
            client,
            "https://dav.example.test/books/work/",
            vec!["https://dav.example.test/books/personal/".to_string()],
        );

        let mut stream = account.discover_cursor_scopes();
        let SyncEvent::Batch(batch) = stream.next().await.expect("scope batch") else {
            panic!("expected scope batch");
        };
        assert_eq!(batch.items.len(), 2);
        assert!(batch.items.iter().all(|scope| matches!(
            scope,
            CursorScope::FolderType {
                ty: ObjectType::Contact,
                ..
            }
        )));
    }

    // The next three tests - `carddav_host_attachment_unsupported`,
    // `carddav_directory_search_unsupported` and
    // `carddav_open_raw_rfc822_unsupported` - assert against a HELPER, not
    // against the account. They call `unsupported_future` / `unsupported_stream`
    // directly and check that those helpers return what they were told to
    // return; they never invoke the `Account` method, so they pass regardless of
    // what the impl does. Their capability-flag assertion is the only line with
    // bite. `capability_contract_tests.rs` now covers the same ground for real,
    // driving each gated entry point through the `Account` surface, which makes
    // these redundant as well as vacuous. They are KEPT rather than removed
    // because deleting a test is a deliberate act that wants the repository
    // owner's nod; if that nod comes, all three go.
    #[tokio::test]
    async fn carddav_host_attachment_unsupported() {
        // CardDAV has no cloud-drive hosting; the flag is false (Default) and
        // the leg returns `Unsupported(HostAttachment)`.
        assert!(!carddav_capabilities().pim_methods.host_attachment);

        let err = Kernel::unsupported_future::<HostedAttachment>(AccountOperation::HostAttachment)
            .await
            .expect_err("carddav host_attachment is unsupported");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[tokio::test]
    async fn carddav_directory_search_unsupported() {
        // CardDAV exposes no organization directory; the flag is false (Default)
        // and the leg returns `Unsupported(DirectorySearch)`.
        assert!(!carddav_capabilities().pim_methods.directory_search);

        let err =
            Kernel::unsupported_future::<Page<DirectoryCard>>(AccountOperation::DirectorySearch)
                .await
                .expect_err("carddav directory_search is unsupported");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::DirectorySearch)
        );
    }

    #[tokio::test]
    async fn enabled_directory_search_without_gateways_is_an_internal_error() {
        let script = dav_script_empty();
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let mut account = CardDavAccount::for_tests(client, "https://dav.example.test/books/");
        account.capabilities.pim_methods.directory_search = true;

        let error = account
            .directory_search("ada".to_string(), None, None)
            .await
            .expect_err("an impossible empty gateway set is reported, not panicked");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated)
        );
    }

    #[tokio::test]
    async fn carddav_open_raw_rfc822_unsupported() {
        // CardDAV advertises no raw RFC822 read; the flag is false and the
        // stream's first item terminates `Unsupported(OpenRawRfc822)`.
        assert!(!carddav_capabilities().pim_methods.open_raw_rfc822);

        let mut stream = Kernel::unsupported_stream::<Bytes>(AccountOperation::OpenRawRfc822);
        let first = stream.next().await.expect("first event");
        match first {
            SyncEvent::Terminated(err) => assert_eq!(
                err.kind(),
                &AccountErrorKind::Unsupported(AccountOperation::OpenRawRfc822)
            ),
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    #[test]
    fn multiget_hrefs_are_rebased_onto_the_snapshot_id_namespace() {
        // XML decoding rebases every response href before the account layer
        // sees it, so hydration cannot split native ids from the snapshot.
        let mut report = CardDavMultigetReport {
            items: vec![CardDavFetchedVCard {
                uri: "/contacts/one.vcf".to_string(),
                etag: Some("e1".into()),
                data: "BEGIN:VCARD\r\nFN:Ada\r\nEND:VCARD\r\n".to_string(),
            }],
            failed: vec![crate::parse::CardDavFailedResource {
                href: "/contacts/two.vcf".to_string(),
                status: Some(403),
            }],
            missing_data: vec!["/contacts/empty.vcf".to_string()],
        };

        report.resolve_hrefs("https://dav.example.test/");
        let (cards, failed) = resolved_report(report);

        assert_eq!(cards[0].uri, "https://dav.example.test/contacts/one.vcf");
        assert_eq!(
            failed,
            vec![
                "https://dav.example.test/contacts/two.vcf",
                "https://dav.example.test/contacts/empty.vcf",
            ]
        );
    }

    #[test]
    fn partition_hydrated_vcards_records_unparseable_ids() {
        // A well-formed vCard projects into `items`; a malformed one is
        // captured in `failed_ids` under its native uri rather than dropped,
        // so the snapshot diff can preserve it instead of destroying it.
        let good = CardDavFetchedVCard {
            uri: "/ab/good.vcf".to_string(),
            etag: Some("e1".into()),
            data: "BEGIN:VCARD\r\nFN:Ada Lovelace\r\nEND:VCARD\r\n".to_string(),
        };
        let bad = CardDavFetchedVCard {
            uri: "/ab/bad.vcf".to_string(),
            etag: None,
            // Unterminated quoted parameter: the projector rejects this body.
            data: "BEGIN:VCARD\r\nEMAIL;TYPE=\"work:ada@example.test\r\nEND:VCARD\r\n".to_string(),
        };

        let (cards, failed_ids) = partition_hydrated_vcards("/ab/", vec![good, bad]);

        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].native_id, "/ab/good.vcf");
        assert_eq!(cards[0].corpus, ContactCorpus::Main);
        assert_eq!(failed_ids, vec!["/ab/bad.vcf".to_string()]);
    }

    fn native_ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn the_contact_page_cursor_carries_the_last_href_served() {
        assert_eq!(
            Kernel::decode_page_cursor(
                Some(b"/book/two.vcf".to_vec().into()),
                AccountOperation::ContactsList
            )
            .unwrap(),
            Some("/book/two.vcf".to_string())
        );
        assert_eq!(
            Kernel::decode_page_cursor(None, AccountOperation::ContactsList).unwrap(),
            None
        );
        assert!(
            Kernel::decode_page_cursor(Some(Vec::new().into()), AccountOperation::ContactsList)
                .is_err(),
            "an empty cursor is corrupt, not a restart from the first contact"
        );
    }

    /// Moved down one level with the lane itself: every paging lane now slices
    /// candidate HREFS before hydrating anything, so these pin the key the
    /// cursor actually carries.
    #[test]
    fn a_watermark_page_returns_the_next_cursor() {
        let page = href_page(&["a", "b", "c", "d"], Some("a"), 2);

        assert_eq!(page.items, native_ids(&["b", "c"]));
        assert_eq!(page.next_watermark.as_deref(), Some("c"));

        let tail = href_page(&["a", "b", "c", "d"], Some("c"), 2);
        assert_eq!(tail.items, native_ids(&["d"]));
        assert_eq!(tail.next_watermark, None);
    }

    fn href_page(
        hrefs: &[&str],
        watermark: Option<&str>,
        page_size: usize,
    ) -> bifrost_dav_core::PageSlice<String> {
        let hrefs = sorted_candidate_hrefs(native_ids(hrefs));
        slice_after_watermark(hrefs, watermark, page_size, String::as_str)
    }

    /// A resource named by several of the eight text-search legs is one
    /// candidate, not one per property it matched.
    #[test]
    fn a_resource_named_by_several_query_legs_is_one_candidate() {
        let page = href_page(&["b", "a", "b", "a", "c"], None, 2);
        assert_eq!(page.items, native_ids(&["a", "b"]));
        assert_eq!(page.next_watermark.as_deref(), Some("b"));
    }

    /// A zero page size must terminate. Emitting the current watermark again
    /// gives a consumer that follows `next_cursor` an infinite non-advancing
    /// loop. The CalDAV twin pins the same rule for `limit: Some(0)`.
    #[test]
    fn a_zero_page_size_is_an_exhausted_page_with_no_continuation() {
        let page = href_page(&["a", "b", "c", "d"], None, 0);

        assert!(page.items.is_empty());
        assert_eq!(page.next_watermark, None);
    }

    #[test]
    fn a_materialized_resource_leaves_the_failure_lane() {
        let card = ContactCard {
            id: ContactId("/book/one.vcf".to_string()),
            address_book_id: Some(AddressBookId("/book/".to_string())),
            native_id: "/book/one.vcf".to_string(),
            version: None,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native: "/book/one.vcf".to_string(),
                address_book_native: Some("/book/".to_string()),
            },
            corpus: ContactCorpus::Main,
            display_name: None,
            emails: Vec::new(),
            phones: Vec::new(),
            organizations: Vec::new(),
            addresses: Vec::new(),
            notes: None,
            photo_url: None,
            photo: None,
        };
        let mut failed_ids = vec![
            "/book/one.vcf".to_string(),
            "/book/two.vcf".to_string(),
            "/book/two.vcf".to_string(),
        ];

        bifrost_dav_core::collection_account::one_outcome_per_id(
            &mut failed_ids,
            std::iter::once(card.native_id.as_str()),
        );

        assert_eq!(failed_ids, vec!["/book/two.vcf".to_string()]);
    }

    #[test]
    fn a_degraded_leg_becomes_a_skipped_contact_collection_scope() {
        let error = crate::client::status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::ContactSearch,
            reqwest::StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );
        let recovery = error.recovery().clone();

        let skipped = skipped_addressbook_scope(Some(error));

        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].scope, ErrorScope::ContactCollection);
        assert_eq!(skipped[0].error.recovery(), &recovery);
        assert!(skipped_addressbook_scope(None).is_empty());
    }

    #[test]
    fn a_cursor_collection_must_match_its_folder_scope() {
        let snapshot = ContactSnapshot {
            collection_url: "https://dav.example.test/c/a%40b/".to_string(),
            ..ContactSnapshot::default()
        };
        let equivalent = Kernel::cursor_from_snapshot(
            CursorScope::Folder(bifrost_types::FolderId(
                "https://dav.example.test/c/a@b/".to_string(),
            )),
            &snapshot,
        );
        assert!(Kernel::decode_snapshot(&equivalent).is_ok());
        let mismatched = Kernel::cursor_from_snapshot(
            CursorScope::Folder(bifrost_types::FolderId(
                "https://dav.example.test/c/other/".to_string(),
            )),
            &snapshot,
        );
        assert!(Kernel::decode_snapshot(&mismatched).is_err());
    }

    #[test]
    fn a_failed_poll_keeps_the_token_needed_to_retry_the_member() {
        let mut previous = snapshot_with(&["a", "b"]);
        previous.token = Some("old".to_string());
        let mut current = snapshot_with(&["a"]);
        current.token = Some("new".to_string());
        current.failed_hrefs = vec!["b".to_string()];
        current.preserve_unobserved(&previous);
        assert_eq!(current.token, previous.token);
        assert_eq!(current.entries, previous.entries);
    }

    #[tokio::test]
    async fn inventory_snapshot_refuses_partial_member_coverage() {
        let script = dav_script([book_multistatus(book_document(&[
            listed_vcard("a"),
            refused_vcard("b"),
        ]))]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let result = CardDavAccount::contact_snapshot(
            &client,
            CtagSource::Known(Some("new".to_string())),
            "https://dav.example.test/books/",
            AccountOperation::SyncInventory,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn an_initial_partial_snapshot_does_not_seed_a_ctag() {
        let script = dav_script([book_multistatus(book_document(&[
            listed_vcard("a"),
            refused_vcard("b"),
        ]))]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let snapshot = CardDavAccount::contact_snapshot(
            &client,
            CtagSource::Known(Some("new".to_string())),
            "https://dav.example.test/books/",
            AccountOperation::SyncChanges,
        )
        .await
        .expect("partial snapshot");
        assert!(snapshot.token.is_none());
        assert_eq!(snapshot.failed_hrefs.len(), 1);
    }

    #[test]
    fn contact_cursor_snapshot_round_trips() {
        let snapshot = ContactSnapshot {
            collection_url: "https://dav.example.test/contacts/".to_string(),
            token: Some("42".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "https://dav.example.test/contacts/a.vcf".to_string(),
                    etag: Some("\"a\"".into()),
                },
                ContactSnapshotEntry {
                    uri: "https://dav.example.test/contacts/b.vcf".to_string(),
                    etag: None,
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let cursor =
            Kernel::cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);

        // failed_hrefs is a per-poll observation, not persisted; the
        // decoded cursor carries an empty failed_hrefs.
        let decoded = Kernel::decode_snapshot(&cursor).expect("cursor should decode");

        assert_eq!(decoded, snapshot);
    }

    /// A v1 payload is byte-identical in shape to a v2 one, so the version
    /// gate has to reject it BEFORE the reader runs - a misparse would hand
    /// back base-relative ids that then surface as a wave of deletes plus
    /// creates. The recovery has to be `SchemaIncompatible` specifically, not
    /// a scope restart: only that directive deletes the backfill checkpoint,
    /// and without the re-walk the already-backfilled objects keep their old
    /// id spelling forever.
    #[test]
    fn contact_cursor_rejects_the_base_relative_id_version() {
        let snapshot = ContactSnapshot::default();
        let mut cursor =
            Kernel::cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);
        cursor.server_state.envelope_version = bifrost_types::PayloadVersion::new(1);

        let error = Kernel::decode_snapshot(&cursor).expect_err("v1 cursors are disowned");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
        assert!(matches!(
            error.recovery(),
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn contact_cursor_rejects_impossible_count_before_allocating() {
        let snapshot = ContactSnapshot {
            collection_url: "https://dav.example.test/contacts/".to_string(),
            token: None,
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };
        let mut cursor =
            Kernel::cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);
        let count_offset = cursor.server_state.bytes.len() - 4;
        cursor.server_state.bytes[count_offset..].copy_from_slice(&u32::MAX.to_be_bytes());

        assert!(Kernel::decode_snapshot(&cursor).is_err());
    }

    #[test]
    fn contact_snapshot_diff_classifies_object_changes() {
        let previous = ContactSnapshot {
            collection_url: "book".to_string(),
            token: Some("1".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "a.vcf".to_string(),
                    etag: Some("old".into()),
                },
                ContactSnapshotEntry {
                    uri: "b.vcf".to_string(),
                    etag: Some("same".into()),
                },
                ContactSnapshotEntry {
                    uri: "d.vcf".to_string(),
                    etag: Some("gone".into()),
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let current = ContactSnapshot {
            collection_url: "book".to_string(),
            token: Some("2".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "a.vcf".to_string(),
                    etag: Some("new".into()),
                },
                ContactSnapshotEntry {
                    uri: "b.vcf".to_string(),
                    etag: Some("same".into()),
                },
                ContactSnapshotEntry {
                    uri: "c.vcf".to_string(),
                    etag: Some("created".into()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = current.changes_from(&previous);
        let kinds = changes
            .into_iter()
            .map(|change| match change {
                Change::ObjectChange(change) => (change.id.0, change.kind),
                _ => panic!("unexpected scope change"),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            kinds,
            vec![
                ("a.vcf".to_string(), ObjectChangeKind::Updated),
                ("c.vcf".to_string(), ObjectChangeKind::Created),
                ("d.vcf".to_string(), ObjectChangeKind::Destroyed),
            ]
        );
    }

    fn snapshot_with(entries: &[&str]) -> ContactSnapshot {
        ContactSnapshot {
            collection_url: "book".to_string(),
            token: None,
            entries: entries
                .iter()
                .map(|uri| ContactSnapshotEntry {
                    uri: (*uri).to_string(),
                    etag: Some("e".into()),
                })
                .collect(),
            failed_hrefs: Vec::new(),
        }
    }

    #[test]
    fn diff_contact_snapshots_suppresses_empty_207_mass_delete() {
        // Brick 6: a populated previous diffed against an empty current
        // yields zero changes (no Destroyed) - the suspected-transient
        // empty multistatus.
        let previous = snapshot_with(&["a.vcf", "b.vcf"]);
        let empty = snapshot_with(&[]);
        assert!(empty.changes_from(&previous).is_empty());

        // populated-vs-populated unchanged; empty-vs-empty zero.
        let same = previous.changes_from(&previous);
        assert!(same.is_empty());
        assert!(empty.changes_from(&empty).is_empty());
    }

    #[test]
    fn empty_poll_checkpoint_preserves_snapshot_for_the_recovery_poll() {
        let previous = snapshot_with(&["a.vcf", "b.vcf"]);
        let mut empty = snapshot_with(&[]);
        empty.token = Some("refreshed".to_string());

        empty.preserve_unobserved(&previous);
        assert_eq!(empty.entries.len(), 2);
        assert_eq!(
            empty.token, None,
            "an empty observation must retry the listing"
        );

        let recovered = snapshot_with(&["a.vcf", "b.vcf"]);
        assert!(recovered.changes_from(&empty).is_empty());
    }

    #[test]
    fn listing_failures_survive_into_the_page_failure_lane() {
        let mut failed_ids = vec!["multiget.vcf".to_string()];
        merge_listing_failures(&mut failed_ids, vec!["listing.vcf".to_string()]);
        assert_eq!(failed_ids, vec!["multiget.vcf", "listing.vcf"]);
    }

    #[test]
    fn failed_uri_preserved_in_contact_diff() {
        // Brick 7: a previous entry whose href is in current.failed_hrefs
        // is NOT emitted as Destroyed.
        let previous = snapshot_with(&["a.vcf", "b.vcf"]);
        let mut current = snapshot_with(&["a.vcf"]);
        current.failed_hrefs = vec!["b.vcf".to_string()];

        let changes = current.changes_from(&previous);
        assert!(
            changes.is_empty(),
            "a transiently-failed resource must not be destroyed: {changes:?}"
        );

        // Without the failed-href, b.vcf's absence IS a destroy.
        let current_no_failed = snapshot_with(&["a.vcf"]);
        let changes = current_no_failed.changes_from(&previous);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            changes[0],
            Change::ObjectChange(ObjectChange {
                kind: ObjectChangeKind::Destroyed,
                ..
            })
        ));
    }

    #[test]
    fn contact_search_matches_common_projected_fields() {
        let contact = ContactCard {
            id: ContactId("/book/ada.vcf".to_string()),
            address_book_id: Some(AddressBookId("/book/".to_string())),
            native_id: "/book/ada.vcf".to_string(),
            version: None,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native: "/book/ada.vcf".to_string(),
                address_book_native: Some("/book/".to_string()),
            },
            corpus: ContactCorpus::Main,
            display_name: Some("Ada Lovelace".to_string()),
            emails: vec![bifrost_types::ContactEmail {
                value: "ada@example.test".to_string(),
                kind: None,
                is_primary: true,
            }],
            phones: Vec::new(),
            organizations: vec![bifrost_types::ContactOrganization {
                name: "Analytical Engines".to_string(),
                title: Some("Programmer".to_string()),
            }],
            addresses: vec![bifrost_types::ContactAddress {
                kind: Some("work".to_string()),
                formatted: None,
                street: vec!["1 Analytical Way".to_string()],
                locality: Some("London".to_string()),
                region: None,
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: false,
            }],
            notes: Some("First algorithm".to_string()),
            photo_url: None,
            photo: None,
        };

        assert!(contact_matches(&contact, "lovelace"));
        assert!(contact_matches(&contact, "example.test"));
        assert!(contact_matches(&contact, "programmer"));
        assert!(contact_matches(&contact, "analytical way"));
        assert!(contact_matches(&contact, "algorithm"));
        assert!(!contact_matches(&contact, "missing"));
    }

    #[test]
    fn contact_search_folds_non_ascii_case() {
        // The needle is lowercased by the search path; matching must use
        // Unicode case folding so a non-ASCII capital (here the Nordic
        // `Å`) in the stored value matches its lowercase form. With
        // `to_ascii_lowercase` the `Å`/`å` pair would not fold and the
        // match would be missed.
        let contact = ContactCard {
            id: ContactId("/book/aase.vcf".to_string()),
            address_book_id: Some(AddressBookId("/book/".to_string())),
            native_id: "/book/aase.vcf".to_string(),
            version: None,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native: "/book/aase.vcf".to_string(),
                address_book_native: Some("/book/".to_string()),
            },
            corpus: ContactCorpus::Main,
            display_name: Some("Åse Bø".to_string()),
            emails: Vec::new(),
            phones: Vec::new(),
            organizations: Vec::new(),
            addresses: Vec::new(),
            notes: None,
            photo_url: None,
            photo: None,
        };

        let needle = "åse".to_lowercase();
        assert!(contact_matches(&contact, &needle));
    }
}

#[cfg(test)]
#[path = "directory_tests.rs"]
mod directory_tests;

#[cfg(test)]
mod partial_commit_tests {
    use super::*;
    use bifrost_types::StepEffect;
    use bifrost_types::{ReconcileAction, RecoveryClass, RetryHint, ServerCause};
    fn throttled() -> AccountError {
        AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint: Some(RetryHint::After(std::time::Duration::from_secs(37))),
        }))
        .try_build()
        .unwrap()
    }
    #[test]
    fn partial_commit_moved_then_put_refused() {
        let leaf = unsupported_error(AccountOperation::ContactUpdate);
        let error = Kernel::relocation_error(
            RelocateFailure::MovedThenPut {
                destination: "destination".into(),
                error: leaf.clone(),
            },
            "source",
        );
        assert_eq!(error.recovery(), leaf.recovery());
        let record = error.partial_commit().unwrap();
        assert_eq!(record.steps()[0].effect(), Some(StepEffect::Moved));
        assert_eq!(
            record.primary().targets.refs[0].scope,
            ErrorScope::Contact {
                id: ContactId("destination".into())
            }
        );
    }
    #[test]
    fn partial_commit_copied_then_delete_throttled() {
        let leaf = throttled();
        let expected = leaf.recovery().clone();
        let error = Kernel::relocation_error(
            RelocateFailure::CopiedThenDelete {
                destination: "destination".into(),
                source: "source".into(),
                error: leaf,
            },
            "source",
        );
        assert_eq!(error.leg_recovery(), &expected);
        let RecoveryClass::Reconcile(advice) = error.recovery() else {
            panic!("protected copy")
        };
        assert_eq!(
            advice.guidance.actions,
            [ReconcileAction::CheckTarget, ReconcileAction::ObserveOnly]
        );
        assert_eq!(
            advice.retry_hint,
            Some(RetryHint::After(std::time::Duration::from_secs(37)))
        );
        assert_eq!(
            error.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::Copied)
        );
    }
}
