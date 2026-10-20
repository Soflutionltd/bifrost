use bifrost_dav_core::{
    DavProtocol, SnapshotEntry, append_path, decode_snapshot, decode_watermark_cursor,
    diff_snapshots, encode_snapshot, encode_watermark_cursor,
    inventory_entry as inventory_entry_from_snapshot, preserve_unobserved_entries, same_dav_url,
    slice_after_watermark, sorted_candidate_hrefs, worse_recovery,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFuture, AccountId, AccountOperation, AccountStream, AddressBook, AddressBookId,
    AttachmentHandle, AttemptCause, BlobHandle, ByteRange, Calendar, CalendarEvent, Cause, Change,
    ChangeCursor, Checkpoint, CloudUploadMeta, ContactCard, ContactCorpus, ContactCreate,
    ContactId, ContactPatch, ContactProvenance, ContactSearchRequest, ContainerId, ContainerKind,
    ContainerList, CostClass, CursorDescriptor, CursorEstablishment, CursorScope, DiagnosticText,
    DirectoryCard, DirectoryGroup, DirectoryGroupId, DirectoryGroupMember, DraftHandle, DraftPatch,
    ErrorScope, EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation,
    FlagOp, HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Identity,
    IdentityId, IdentityPatch, Importance, InventoryEvent, InventoryPartition,
    InventoryPartitioning, ItemOutcome, MembershipScope, Message, MutationSuccess, MutationTarget,
    ObjectId, ObjectType, OpaqueChangeState, Page, PageBoundary, Priority, Protocol,
    ProtocolErrorKind, ProtocolKind, QuotaInfo, RsvpStatus, SearchRequest, SendRequest,
    ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch, SkippedScope,
    SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration, ThreadId, TransmissionState,
    VacationConfig, WatchEvent, WireCause,
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
use crate::parse::{AddressBookCollection, CardDavFetchedVCard, CardDavMultigetReport};
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
const CONTACT_PAGE_SIZE: usize = 250;
// Version 2 changes snapshot ids from base-URL-relative to request-URI-relative.
//
// Accepted cost of that bump, recorded so it is not rediscovered as a bug: a
// v1 cursor is REFUSED by `decode_cursor_snapshot` rather than migrated - a v1
// payload is byte-identical in shape to a v2 one, so the ids cannot be
// distinguished and rewritten, only re-derived - so every consumer holding a
// pre-bump cursor pays one full re-sync per DAV account, once. That is a
// one-time cost against ids that were silently wrong, which was worth paying.
// Pinned by `contact_cursor_rejects_the_base_relative_id_version`.
const CURSOR_ENVELOPE_VERSION: u32 = 2;
const CURSOR_MAGIC: &[u8] = b"CDAVCTAG1";

#[derive(Debug)]
pub(crate) struct CardDavAccount {
    client: Arc<CardDavClient>,
    capabilities: AccountCapabilities,
    addressbook_home: String,
    directory_gateways: Vec<String>,
    /// The collection a call that names no address book routes to, and `None`
    /// when the home enumerated no collections.
    ///
    /// Deliberately not the addressbook home in that case. See
    /// `no_default_addressbook`.
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
            .ok_or_else(|| no_default_addressbook(operation))
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
                .ok_or_else(|| no_default_addressbook(operation))?,
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
    /// Twin of `bifrost-caldav`'s `relocate_event`; keep them in step. WebDAV
    /// `MOVE` is the atomic form and is tried first; a server that does not
    /// implement it falls back to PUT-to-new then DELETE-from-old, which is not
    /// atomic, so a failure of the delete leg is wrapped
    /// `Protocol(PartialResponse)` with an acknowledged `Attempt`.
    async fn relocate_contact(
        client: &CardDavClient,
        source_url: &str,
        target_addressbook: &str,
        data: String,
        content_changed: bool,
    ) -> Result<(), AccountError> {
        let destination = append_path(target_addressbook, &resource_file_name(source_url));
        if client
            .move_resource(source_url, &destination, AccountOperation::ContactUpdate)
            .await?
        {
            if !content_changed {
                return Ok(());
            }
            // The move is done and the id has changed. A failure here is a
            // half-applied request, not a failed one.
            return client
                .put_vcard(
                    &destination,
                    data,
                    PutCondition::None,
                    AccountOperation::ContactUpdate,
                )
                .await
                .map(|_| ())
                .map_err(|error| {
                    partial_move_error(&error, "contact moved but the field patch failed")
                });
        }
        // No MOVE support: copy first, then remove the original. A failed copy
        // leaves the contact exactly where it was; a failed delete leaves it
        // readable in two places - the recoverable direction of the pair.
        client
            .put_vcard(
                &destination,
                data,
                PutCondition::IfNoneMatch,
                AccountOperation::ContactUpdate,
            )
            .await
            .map(|_| ())?;
        client
            .delete_vcard(source_url, AccountOperation::ContactUpdate)
            .await
            .map(|_| ())
            .map_err(|error| {
                partial_move_error(
                    &error,
                    "contact copied to the destination address book but the original could not be removed",
                )
            })
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
        let HrefQuery {
            hrefs,
            failed_hrefs,
            degraded,
        } = candidates;
        let hrefs = sorted_candidate_hrefs(hrefs);
        let total = hrefs.len();
        let slice = slice_after_watermark(hrefs, watermark, page_size, String::as_str);
        let fetch = client
            .fetch_vcards(addressbook, &slice.items, operation)
            .await?;
        let skipped_scopes =
            skipped_addressbook_scope(worse_recovery_option(degraded, fetch.degraded));
        let (fetched, mut failed_ids) = resolved_report(fetch.report);
        merge_listing_failures(&mut failed_ids, failed_hrefs);
        let (cards, projection_failures) = partition_hydrated_vcards(addressbook, fetched);
        failed_ids.extend(projection_failures);
        one_outcome_per_id(&mut failed_ids, &cards);
        let mut cards = cards.into_iter().filter(keep).collect::<Vec<_>>();
        // The page is served in the same key order it was sliced in; a
        // multiget answers in whatever order it likes.
        cards.sort_by(|left, right| left.native_id.cmp(&right.native_id));
        Ok(Page {
            truncated: false,
            items: cards,
            next_cursor: slice.next_watermark.as_deref().map(encode_watermark_cursor),
            estimated_total: Some(estimated_total(total)),
            failed_ids,
            skipped_scopes,
        })
    }

    /// Snapshot one address book's contact listing plus its ctag.
    ///
    /// `ctag` says where the collection tag comes from, because the callers
    /// differ in what they already know:
    ///
    /// - [`CtagSource::Home`] runs the depth-1 PROPFIND over the address book
    ///   home and picks this collection out of it. Cursor establishment and
    ///   inventory use it: they have no prior cursor to compare against.
    /// - [`CtagSource::Known`] takes a value the caller already has. The poll
    ///   path uses it, because deciding whether to short-circuit at all
    ///   required asking for the ctag first - re-deriving it from a depth-1
    ///   listing of the whole home made a changed-ctag poll cost three round
    ///   trips where two suffice, and that listing grows with the number of
    ///   address books rather than staying one collection wide.
    pub(crate) async fn contact_snapshot(
        client: &CardDavClient,
        ctag: CtagSource<'_>,
        addressbook: &str,
        operation: AccountOperation,
    ) -> Result<ContactSnapshot, AccountError> {
        client.refuse_directory_access(addressbook, operation)?;
        let ctag = match ctag {
            CtagSource::Home(home) => client
                .list_addressbooks_for_operation(home, operation)
                .await?
                .into_iter()
                .find(|collection| same_collection_url(&collection.href, addressbook))
                .and_then(|collection| collection.ctag),
            CtagSource::Known(ctag) => ctag,
        };
        let listing = client.list_contacts_listing(addressbook, operation).await?;
        // COMPLETE inventory coverage cannot discharge a refused member. Fail
        // before emitting either items or a checkpoint, retaining its status.
        if operation == AccountOperation::SyncInventory
            && let Some(failure) = listing.failed.first()
        {
            return Err(
                match failure
                    .status
                    .and_then(|code| reqwest::StatusCode::from_u16(code).ok())
                {
                    Some(status) => bifrost_dav_core::status_error(
                        operation,
                        status,
                        format!("inventory member refused: {}", failure.href),
                        DavProtocol::CardDav,
                    ),
                    None => bifrost_dav_core::parse_error(
                        operation,
                        "inventory member omitted usable properties",
                        DavProtocol::CardDav,
                    ),
                },
            );
        }
        let failed_hrefs = listing.failed_hrefs();
        let mut entries = listing
            .entries
            .into_iter()
            .map(|entry| ContactSnapshotEntry {
                uri: entry.uri,
                etag: entry.etag,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        let observation = preserve_unobserved_entries(&[], &mut entries, &failed_hrefs);
        let ctag = observation.advance_token(None, ctag);
        Ok(ContactSnapshot {
            addressbook_url: addressbook.to_string(),
            ctag,
            entries,
            failed_hrefs,
        })
    }
}

/// Where [`CardDavAccount::contact_snapshot`] gets a collection's ctag.
pub(crate) enum CtagSource<'a> {
    /// Pick it out of a depth-1 PROPFIND over the address book home.
    Home(&'a str),
    /// Use a value the caller already resolved, spending no request.
    Known(Option<String>),
}

impl Account for CardDavAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.client.net().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.net().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::ServerCursor,
            freshness: None,
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        let scopes = self
            .addressbook_urls
            .iter()
            .filter(|url| !self.client.is_directory_url(url))
            .cloned()
            .map(|url| CursorScope::Folder(bifrost_types::FolderId(url)))
            .collect();
        Box::pin(stream::iter([
            SyncEvent::Batch(bifrost_types::Batch {
                items: scopes,
                page_boundary: PageBoundary::Final,
                server_latency: Default::default(),
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ]))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        unsupported_stream(AccountOperation::DiscoverMemberships)
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<bifrost_types::ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let client = Arc::clone(&self.client);
        let home = self.addressbook_home.clone();
        let default_addressbook = self.default_addressbook_url.clone();
        let addressbook_urls = self.addressbook_urls.clone();
        Box::pin(async move {
            validate_contact_scope(&scope, AccountOperation::EstablishCursor)?;
            let addressbook = collection_url_for_scope(
                &scope,
                default_addressbook.as_deref(),
                &addressbook_urls,
                AccountOperation::EstablishCursor,
            )?;
            let snapshot = Self::contact_snapshot(
                &client,
                CtagSource::Home(&home),
                &addressbook,
                AccountOperation::EstablishCursor,
            )
            .await
            .map_err(|error| directory_sync_error(error, &scope))?;
            Ok(CursorEstablishment::Ready(cursor_from_snapshot(
                scope, &snapshot,
            )))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        let client = Arc::clone(&self.client);
        let home = self.addressbook_home.clone();
        let default_addressbook = self.default_addressbook_url.clone();
        let addressbook_urls = self.addressbook_urls.clone();
        let coverage_scope = scope.clone();
        let coverage_domain = collection_coverage_domain(
            coverage_scope,
            scope_collection_url(&scope, default_addressbook.as_deref()),
        );
        // COMPLETE coverage is accurate here: the walk terminates wholesale on
        // any failure, so it never advances a checkpoint across a gap.
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                if let Err(error) = validate_contact_scope(&scope, AccountOperation::SyncInventory)
                {
                    events.push(SyncEvent::Terminated(directory_sync_error(error, &scope)));
                    return events;
                }
                let addressbook = match collection_url_for_scope(
                    &scope,
                    default_addressbook.as_deref(),
                    &addressbook_urls,
                    AccountOperation::SyncInventory,
                ) {
                    Ok(addressbook) => addressbook,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(directory_sync_error(error, &scope)));
                        return events;
                    }
                };
                let started = Instant::now();
                let snapshot = match Self::contact_snapshot(
                    &client,
                    CtagSource::Home(&home),
                    &addressbook,
                    AccountOperation::SyncInventory,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(directory_sync_error(error, &scope)));
                        return events;
                    }
                };
                let checkpoint = cursor_from_snapshot(scope, &snapshot);
                let items = snapshot
                    .entries
                    .iter()
                    .map(inventory_entry_from_snapshot)
                    .collect();
                events.push(SyncEvent::Batch(bifrost_types::Batch {
                    items,
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                }));
                events.push(SyncEvent::Done(Some(Checkpoint::Change(checkpoint))));
                events
            })
            .flat_map(stream::iter)
            .map(bifrost_types::lift_complete_walk(coverage_domain)),
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
            _ => {
                bifrost_types::unsupported_inventory_stream(scope, AccountOperation::SyncInventory)
            }
        }
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: bifrost_types::Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        unsupported_stream(AccountOperation::Hydrate)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let client = Arc::clone(&self.client);
        // No address book home is captured: the poll path resolves its ctag
        // against the collection itself, never by re-listing the home.
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                let previous = match decode_cursor_snapshot(&cursor) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(directory_sync_error(
                            error,
                            &cursor.scope,
                        )));
                        return events;
                    }
                };
                let started = Instant::now();
                // ctag short-circuit (brick 8): when the prior cursor
                // carries a ctag and a cheap depth-0 getctag PROPFIND
                // shows the collection unchanged, skip the full depth-1
                // PROPFIND + diff and carry the cursor forward with no
                // changes. Mirrors CalDAV's sync-token short-circuit.
                //
                // The ctag is resolved AT MOST ONCE per poll, and the value is
                // carried into the snapshot below rather than re-derived. It
                // used to be fetched here and then recovered a second time from
                // a depth-1 listing of the whole address book home - three
                // round trips for a changed collection, one of them scaling
                // with the number of address books.
                // Resolved unconditionally, not only when the prior cursor
                // carried one. A cursor with no ctag previously had to recover
                // one from the depth-1 home listing, which is both dearer and
                // likelier to come back empty (it finds nothing when the
                // collection does not appear in its own home). Asking the
                // collection directly seeds the ctag for the next poll, so a
                // cursor that starts without one is not stuck without one.
                let current_ctag = match client
                    .collection_ctag(&previous.addressbook_url, AccountOperation::SyncChanges)
                    .await
                {
                    Ok(ctag) => ctag,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(directory_sync_error(
                            error,
                            &cursor.scope,
                        )));
                        return events;
                    }
                };
                if let (Some(prev_ctag), Some(current_ctag)) =
                    (previous.ctag.as_deref(), current_ctag.as_deref())
                    && prev_ctag == current_ctag
                {
                    let checkpoint = cursor_from_snapshot(cursor.scope, &previous);
                    events.push(SyncEvent::Batch(bifrost_types::Batch {
                        items: Vec::new(),
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: 0,
                        checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                    }));
                    events.push(SyncEvent::Done(Some(Checkpoint::Change(checkpoint))));
                    return events;
                }
                // ctag changed, server omits getctag, or the prior cursor
                // carried none: fall through to the full snapshot + diff
                // (which still runs brick 6's empty-207 guard). `Known`
                // spends no request - it reuses whatever the check above
                // resolved, including `None`, which is exactly the value the
                // old depth-1 path would have produced for a server that does
                // not publish getctag.
                let mut current = match Self::contact_snapshot(
                    &client,
                    CtagSource::Known(current_ctag),
                    &previous.addressbook_url,
                    AccountOperation::SyncChanges,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(directory_sync_error(
                            error,
                            &cursor.scope,
                        )));
                        return events;
                    }
                };
                preserve_unobserved_contact_entries(&previous, &mut current);
                let checkpoint = cursor_from_snapshot(cursor.scope, &current);
                let changes = diff_contact_snapshots(&previous, &current);
                events.push(SyncEvent::Batch(bifrost_types::Batch {
                    items: changes,
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                }));
                events.push(SyncEvent::Done(Some(Checkpoint::Change(checkpoint))));
                events
            })
            .flat_map(stream::iter),
        )
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        unsupported_future(AccountOperation::PushSubscribe)
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::PushUnsubscribe)
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        Box::pin(stream::empty())
    }

    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        unsupported_stream(AccountOperation::OpenBlob)
    }

    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        unsupported_stream(AccountOperation::OpenBlobRange)
    }

    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        unsupported_stream(AccountOperation::OpenRawRfc822)
    }

    fn bulk_set_flags(
        &self,
        _targets: AccountStream<ObjectId>,
        op: FlagOp,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        match op.validate_for_account(bifrost_types::Protocol::CardDav) {
            Ok(()) => unsupported_stream(AccountOperation::UpdateFlags),
            Err(error) => Box::pin(futures::stream::once(async move {
                SyncEvent::Terminated(error)
            })),
        }
    }

    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unsupported_stream(AccountOperation::BulkMove)
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unsupported_stream(AccountOperation::BulkDestroy)
    }

    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::AddToContainer)
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::RemoveFromContainer)
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetKeyword)
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetLabelMembership)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetCategory)
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetExtendedProperty)
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetIsRead)
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetImportance)
    }

    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        unsupported_future(AccountOperation::Send)
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        unsupported_future(AccountOperation::AttachmentUpload)
    }

    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        unsupported_future(AccountOperation::HostAttachment)
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        unsupported_future(AccountOperation::DraftCreate)
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::DraftUpdate)
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::DraftDiscard)
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        unsupported_future(AccountOperation::DraftSend)
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::CancelScheduledSend)
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        unsupported_future(AccountOperation::RescheduleSend)
    }

    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        unsupported_future(AccountOperation::Search)
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        unsupported_future(AccountOperation::SearchMessages)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        unsupported_future(AccountOperation::ContainersList)
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        unsupported_future(AccountOperation::ContainerCreate)
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContainerRename)
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContainerMove)
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContainerDelete)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        unsupported_future(AccountOperation::IdentitiesList)
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::IdentityUpdate)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        unsupported_future(AccountOperation::VacationGet)
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::VacationSet)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        unsupported_future(AccountOperation::QuotaGet)
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        unsupported_future(AccountOperation::FiltersList)
    }

    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        unsupported_future(AccountOperation::FilterCreate)
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::FilterUpdate)
    }

    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::FilterDelete)
    }

    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        unsupported_future(AccountOperation::FilterValidate)
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
        page_cursor: Option<Vec<u8>>,
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
            let watermark = decode_contact_page_cursor(page_cursor, operation)?;
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
            let id = format!("{}.vcf", Uuid::new_v4());
            let url = append_path(&addressbook, &id);
            let data = vcard_from_create(&contact, &id)
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

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
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
                        put_condition(current.etag.as_deref()),
                        AccountOperation::ContactUpdate,
                    )
                    .await
                    .map(|_| ());
            };
            // A content patch riding along with the move needs a write of its
            // own; a move-only patch does not, and must not be charged a
            // partial-failure verdict for a leg it never needed.
            let content_changed = patch_changes_content(&patch);
            Self::relocate_contact(&client, &url, &target_addressbook, data, content_changed).await
        })
    }

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
            let watermark = decode_contact_page_cursor(request.page_cursor.clone(), operation)?;
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
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let operation = AccountOperation::DirectorySearch;
        if !self.capabilities.pim_methods.directory_search
            || (query.trim().is_empty() && !self.capabilities.pim_methods.directory_enumerate)
        {
            return unsupported_future(operation);
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
                        for raw in report.cards {
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
                return Err(worst.expect("at least one admitted gateway"));
            }
            Ok(page)
        })
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
        unsupported_future(AccountOperation::CalendarsList)
    }

    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        unsupported_future(AccountOperation::EventsInRange)
    }

    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        unsupported_future(AccountOperation::EventGet)
    }

    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        unsupported_future(AccountOperation::EventCreate)
    }

    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::EventUpdate)
    }

    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::EventDelete)
    }

    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::EventRsvp)
    }

    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        unsupported_future(AccountOperation::EventSearch)
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        unsupported_future(AccountOperation::HydrateThread)
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        unsupported_future(AccountOperation::HydrateMessage)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Ok(()) })
    }
}

fn unsupported_future<T: Send + 'static>(
    operation: AccountOperation,
) -> AccountFuture<Result<T, AccountError>> {
    Box::pin(async move { Err(unsupported_error(operation)) })
}

fn unsupported_stream<T: Send + 'static>(
    operation: AccountOperation,
) -> AccountStream<SyncEvent<T>> {
    Box::pin(stream::iter([
        SyncEvent::Terminated(unsupported_error(operation)),
        SyncEvent::Done(None),
    ]))
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
        ..patch.clone()
    };
    content != ContactPatch::default()
}

/// Reclassify the failure of a later leg of a non-atomic sequence whose
/// earlier leg already landed on the server.
///
/// `Protocol(PartialResponse)` plus an acknowledged `Attempt` is what tells a
/// consumer the request was half-applied rather than refused, so it reconciles
/// instead of replaying a write that already took effect. Twin of
/// `bifrost-caldav`'s `partial_sequence_error`.
///
/// The unreplayable declaration must be RE-STATED here. `dispatch_once` stamps
/// `idempotency_override(false)` on the leg's own failure, but this is a FRESH
/// `AccountErrorBuilder`, and an override is not part of the cause chain that
/// gets copied across - so without the restatement `derive_protocol` falls back
/// to the `AccountOperation` table, which calls `ContactUpdate` idempotent, and
/// answers `Retry(SameRequest)` for a move that is half applied. The declaration
/// must survive the wire, the classification, and any rebuild.
fn partial_move_error(error: &AccountError, detail: &'static str) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::CardDav,
            detail: Some(DiagnosticText::support_only(detail)),
        }),
    )
    .protocol(Protocol::CardDav)
    .operation(AccountOperation::ContactUpdate)
    .idempotency_override(false)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )));
    for cause in error.chain().iter() {
        builder = builder.push_cause(cause.clone());
    }
    builder
        .try_build()
        .expect("valid partial-response classification")
}

/// The last path segment of a resource URL - the name the resource keeps when
/// it moves into another collection.
///
/// Falls back to a fresh UUID-backed name when the URL has no usable final
/// segment, so a move never targets the destination collection itself.
fn resource_file_name(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .path_segments()
                .and_then(|mut segments| segments.next_back().filter(|name| !name.is_empty()))
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("{}.vcf", Uuid::new_v4()))
}

fn contact_addressbook_url(client: &CardDavClient, contact: &ContactId) -> Option<String> {
    let resolved = client.resolve_url(&contact.0);
    bifrost_net::url::parent_collection_url(&resolved)
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

fn put_condition(etag: Option<&str>) -> PutCondition<'_> {
    etag.filter(|etag| {
        !etag
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    })
    .map_or(PutCondition::None, PutCondition::IfMatch)
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ContactSnapshot {
    addressbook_url: String,
    pub(crate) ctag: Option<String>,
    entries: Vec<ContactSnapshotEntry>,
    /// Hrefs the server reported *failed* within the 207 of the poll
    /// that built this snapshot. Not persisted in the cursor (a
    /// per-poll observation); the diff preserves these `previous`
    /// entries rather than destroying them.
    failed_hrefs: Vec<String>,
}

/// One polled address-book member. The shape, the cursor codec, the diff and
/// the page slicer are all shared with `bifrost-caldav` through
/// `bifrost-dav-core`; only the magic bytes and the token's name differ.
type ContactSnapshotEntry = SnapshotEntry;

fn validate_contact_scope(
    scope: &CursorScope,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if matches!(
        scope,
        CursorScope::Type(ObjectType::Contact) | CursorScope::Folder(_)
    ) {
        Ok(())
    } else {
        Err(local_error(
            operation,
            "CardDAV only supports contact cursor scopes",
        ))
    }
}

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
    collections
        .iter()
        .map(|collection| collection.href.clone())
        .collect()
}

/// A collection reclassified as a directory is no longer a personal sync
/// scope. Disable the original scope, rather than retry or try another sync
/// strategy. Keep redirect transmission evidence from the guard's refusal.
fn directory_sync_error(error: AccountError, scope: &CursorScope) -> AccountError {
    let Some(ErrorScope::DirectoryGateway { url }) = error.scope() else {
        return error;
    };
    if !matches!(error.kind(), AccountErrorKind::Unsupported(_)) {
        return error;
    }
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::ScopeRevoked),
        Cause::State(bifrost_types::StateCause::ScopeRevoked),
    )
    .protocol(Protocol::CardDav)
    .scope(ErrorScope::Cursor(scope.clone()))
    .text(DiagnosticText::support_only(format!(
        "personal sync scope targets a directory: {url}"
    )));
    if let Some(operation) = error.operation() {
        builder = builder.operation(operation);
    }
    for cause in error.chain().iter().cloned() {
        builder = builder.push_cause(cause);
    }
    builder
        .try_build()
        .expect("valid directory scope retirement")
}

/// A call that names no address book cannot be routed, because the addressbook
/// home enumerated no collections.
///
/// The alternative - falling back to the addressbook home URL - is what this
/// replaces. The home is not itself a collection in that case
/// (`list_addressbooks` already returns the home when it genuinely is one, so
/// an empty result means an empty backend), so every such request went to a
/// resource a spec-correct server 404s, and reported it as a remote failure
/// rather than as the local routing failure it is. It also contradicted the
/// empty-home contract `address_books_list` is pinned to. Twin of
/// `bifrost-caldav`'s `no_default_calendar`.
fn no_default_addressbook(operation: AccountOperation) -> AccountError {
    local_error(
        operation,
        "CardDAV account has no address book collection to route a call that names none",
    )
}

/// The collection a call that names no address book routes to: the first
/// discovered one, and `None` when the home enumerated none.
///
/// Deliberately has no access to the addressbook home, so the fallback this
/// replaced cannot be reintroduced here without also changing the signature.
/// See `no_default_addressbook` for why the home is the wrong answer.
fn default_collection_url(collections: &[AddressBookCollection]) -> Option<String> {
    collections
        .first()
        .map(|collection| collection.href.clone())
}

fn collection_url_for_scope(
    scope: &CursorScope,
    default_url: Option<&str>,
    collection_urls: &[String],
    operation: AccountOperation,
) -> Result<String, AccountError> {
    match scope {
        CursorScope::Type(ObjectType::Contact) => default_url
            .map(str::to_string)
            .ok_or_else(|| no_default_addressbook(operation)),
        CursorScope::Folder(folder) if collection_urls.contains(&folder.0) => Ok(folder.0.clone()),
        _ => Err(local_error(
            operation,
            "CardDAV cursor scope does not name a discovered address book",
        )),
    }
}

fn scope_collection_url<'a>(
    scope: &'a CursorScope,
    default_url: Option<&'a str>,
) -> Option<&'a str> {
    match scope {
        CursorScope::Folder(folder) => Some(&folder.0),
        _ => default_url,
    }
}

fn collection_coverage_domain(
    scope: CursorScope,
    collection_url: Option<&str>,
) -> bifrost_types::CoverageDomain {
    match scope {
        CursorScope::Folder(_) => bifrost_types::CoverageDomain::full(scope),
        // An unroutable legacy scope still needs a domain to carry the
        // terminating stream; the walk fails before the region is read.
        _ => bifrost_types::CoverageDomain {
            scope,
            coordinate: bifrost_types::CoverageCoordinate::ProviderRegion {
                namespace: "carddav".to_string(),
                region: collection_url.unwrap_or_default().as_bytes().to_vec(),
            },
            snapshot: bifrost_types::SnapshotIdentity::unstable(),
        },
    }
}

fn cursor_from_snapshot(scope: CursorScope, snapshot: &ContactSnapshot) -> ChangeCursor {
    ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::CardDav,
            envelope_version: CURSOR_ENVELOPE_VERSION,
            bytes: encode_cursor_snapshot(snapshot),
        },
        advanced_through: None,
        envelope_version: bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
    }
}

fn encode_cursor_snapshot(snapshot: &ContactSnapshot) -> Vec<u8> {
    encode_snapshot(
        CURSOR_MAGIC,
        &snapshot.addressbook_url,
        snapshot.ctag.as_deref(),
        &snapshot.entries,
    )
}

fn decode_cursor_snapshot(cursor: &ChangeCursor) -> Result<ContactSnapshot, AccountError> {
    validate_contact_scope(&cursor.scope, AccountOperation::SyncChanges)?;
    if cursor.server_state.protocol != ProtocolKind::CardDav
        || cursor.server_state.envelope_version != CURSOR_ENVELOPE_VERSION
    {
        return Err(cursor_error("CardDAV cursor protocol or version mismatch"));
    }
    let decoded = decode_snapshot(
        CURSOR_MAGIC,
        DavProtocol::CardDav.label(),
        &cursor.server_state.bytes,
    )
    .map_err(cursor_error)?;
    if let CursorScope::Folder(folder) = &cursor.scope
        && !same_dav_url(&folder.0, &decoded.collection_url)
    {
        return Err(cursor_error(
            "cursor collection does not match its folder scope",
        ));
    }
    Ok(ContactSnapshot {
        addressbook_url: decoded.collection_url,
        ctag: decoded.token,
        entries: decoded.entries,
        failed_hrefs: Vec::new(),
    })
}

/// Diff two contact snapshots. The rule, the transient-empty-207 suppression
/// and the failed-href preservation all live in `bifrost-dav-core`, which
/// `bifrost-caldav` reads through the same door.
fn diff_contact_snapshots(previous: &ContactSnapshot, current: &ContactSnapshot) -> Vec<Change> {
    diff_snapshots(&previous.entries, &current.entries, &current.failed_hrefs)
}

fn preserve_unobserved_contact_entries(previous: &ContactSnapshot, current: &mut ContactSnapshot) {
    let observation = preserve_unobserved_entries(
        &previous.entries,
        &mut current.entries,
        &current.failed_hrefs,
    );
    current.ctag = observation.advance_token(previous.ctag.clone(), current.ctag.take());
}

fn cursor_error(message: impl Into<String>) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::SchemaIncompatible,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
    )
    .protocol(bifrost_types::Protocol::CardDav)
    .operation(AccountOperation::SyncChanges)
    .text(bifrost_types::DiagnosticText::support_only(message))
    .try_build()
    .expect("valid account error classification")
}

fn decode_contact_page_cursor(
    cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<Option<String>, AccountError> {
    decode_watermark_cursor(cursor, operation, DavProtocol::CardDav)
}

/// Keep the worse of two optional failures, so a refused query leg is not
/// buried under a milder multiget failure or dropped entirely. Twin of the
/// CalDAV function of the same name.
fn worse_recovery_option(
    current: Option<AccountError>,
    candidate: Option<AccountError>,
) -> Option<AccountError> {
    match candidate {
        Some(candidate) => worse_recovery(current, candidate),
        None => current,
    }
}

/// Reduce the failure lane to one outcome per resource id.
///
/// Text search runs a REPORT per property, so the resource the EMAIL query
/// refused can be the same resource the FN query returned in full. The
/// data arrived, so success is the true outcome; reporting the id in both
/// lanes would make a consumer count it twice and treat a contact it can
/// display as lost. The sort and dedup finish the job for ids that failed
/// in more than one REPORT.
///
/// `bifrost-caldav` has a function of the same name and shape, and the two
/// were deliberately NOT collapsed into `bifrost-dav-core` when the propstat
/// machine and the snapshot layer were: this one filters against the
/// `native_id` of the parsed cards, the CalDAV twin against a materialized
/// href set, so they share only their outline and a shared version would
/// need a projection each side supplies anyway.
fn one_outcome_per_id(failed_ids: &mut Vec<String>, cards: &[ContactCard]) {
    let materialized = cards
        .iter()
        .map(|card| card.native_id.as_str())
        .collect::<HashSet<_>>();
    failed_ids.retain(|id| !materialized.contains(id.as_str()));
    failed_ids.sort_unstable();
    failed_ids.dedup();
}

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
fn skipped_addressbook_scope(degraded: Option<AccountError>) -> Vec<SkippedScope> {
    degraded
        .map(|error| SkippedScope {
            scope: ErrorScope::ContactCollection,
            error,
        })
        .into_iter()
        .collect()
}

fn estimated_total(total: usize) -> u64 {
    u64::try_from(total).unwrap_or(u64::MAX)
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
fn resolved_report(report: CardDavMultigetReport) -> (Vec<CardDavFetchedVCard>, Vec<String>) {
    let cards = report.cards;
    let failed = report
        .failed
        .into_iter()
        .map(|failure| failure.href)
        .chain(report.missing_data)
        .collect();
    (cards, failed)
}

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

    fn book_multistatus(body: String) -> DavResponse {
        DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: "https://dav.example.test/books/".to_string(),
        }
    }

    fn listed_vcard(name: &str) -> String {
        format!(
            "<D:response><D:href>/books/{name}.vcf</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status>\
</D:propstat></D:response>"
        )
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
            Some(b"https://dav.example.test/books/b.vcf".to_vec()),
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
            url: "https://dav.example.test/books/".to_string(),
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
            Some(b"https://dav.example.test/books/a.vcf".to_vec()),
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
            Some(b"https://dav.example.test/books/c249.vcf".to_vec()),
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
            Some(b"https://dav.example.test/books/b.vcf".to_vec()),
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
                page_cursor: Some(b"https://dav.example.test/books/b.vcf".to_vec()),
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
            .contacts_list(None, Some(b"https://dav.example.test/books/z.vcf".to_vec()))
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

        account.set_bandwidth_cap(Some(4_096));
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
        assert!(
            batch
                .items
                .iter()
                .all(|scope| matches!(scope, CursorScope::Folder(_)))
        );
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

        let err = unsupported_future::<HostedAttachment>(AccountOperation::HostAttachment)
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

        let err = unsupported_future::<Page<DirectoryCard>>(AccountOperation::DirectorySearch)
            .await
            .expect_err("carddav directory_search is unsupported");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::DirectorySearch)
        );
    }

    #[tokio::test]
    async fn carddav_open_raw_rfc822_unsupported() {
        // CardDAV advertises no raw RFC822 read; the flag is false and the
        // stream's first item terminates `Unsupported(OpenRawRfc822)`.
        assert!(!carddav_capabilities().pim_methods.open_raw_rfc822);

        let mut stream = unsupported_stream::<Bytes>(AccountOperation::OpenRawRfc822);
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
            cards: vec![CardDavFetchedVCard {
                uri: "/contacts/one.vcf".to_string(),
                etag: Some("e1".to_string()),
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
            etag: Some("e1".to_string()),
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
            decode_contact_page_cursor(
                Some(b"/book/two.vcf".to_vec()),
                AccountOperation::ContactsList
            )
            .unwrap(),
            Some("/book/two.vcf".to_string())
        );
        assert_eq!(
            decode_contact_page_cursor(None, AccountOperation::ContactsList).unwrap(),
            None
        );
        assert!(
            decode_contact_page_cursor(Some(Vec::new()), AccountOperation::ContactsList).is_err(),
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
            etag: None,
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

        one_outcome_per_id(&mut failed_ids, std::slice::from_ref(&card));

        assert_eq!(failed_ids, vec!["/book/two.vcf".to_string()]);
    }

    #[test]
    fn a_degraded_leg_becomes_a_skipped_contact_collection_scope() {
        let error = crate::client::status_error(
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
            addressbook_url: "https://dav.example.test/c/a%40b/".to_string(),
            ..ContactSnapshot::default()
        };
        let equivalent = cursor_from_snapshot(
            CursorScope::Folder(bifrost_types::FolderId(
                "https://dav.example.test/c/a@b/".to_string(),
            )),
            &snapshot,
        );
        assert!(decode_cursor_snapshot(&equivalent).is_ok());
        let mismatched = cursor_from_snapshot(
            CursorScope::Folder(bifrost_types::FolderId(
                "https://dav.example.test/c/other/".to_string(),
            )),
            &snapshot,
        );
        assert!(decode_cursor_snapshot(&mismatched).is_err());
    }

    #[test]
    fn a_failed_poll_keeps_the_token_needed_to_retry_the_member() {
        let mut previous = snapshot_with(&["a", "b"]);
        previous.ctag = Some("old".to_string());
        let mut current = snapshot_with(&["a"]);
        current.ctag = Some("new".to_string());
        current.failed_hrefs = vec!["b".to_string()];
        preserve_unobserved_contact_entries(&previous, &mut current);
        assert_eq!(current.ctag, previous.ctag);
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
        assert!(snapshot.ctag.is_none());
        assert_eq!(snapshot.failed_hrefs.len(), 1);
    }

    #[test]
    fn contact_cursor_snapshot_round_trips() {
        let snapshot = ContactSnapshot {
            addressbook_url: "https://dav.example.test/contacts/".to_string(),
            ctag: Some("42".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "https://dav.example.test/contacts/a.vcf".to_string(),
                    etag: Some("\"a\"".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "https://dav.example.test/contacts/b.vcf".to_string(),
                    etag: None,
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);

        // failed_hrefs is a per-poll observation, not persisted; the
        // decoded cursor carries an empty failed_hrefs.
        let decoded = decode_cursor_snapshot(&cursor).expect("cursor should decode");

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
        let mut cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);
        cursor.server_state.envelope_version = 1;

        let error = decode_cursor_snapshot(&cursor).expect_err("v1 cursors are disowned");
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
            addressbook_url: "https://dav.example.test/contacts/".to_string(),
            ctag: None,
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };
        let mut cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);
        let count_offset = cursor.server_state.bytes.len() - 4;
        cursor.server_state.bytes[count_offset..].copy_from_slice(&u32::MAX.to_be_bytes());

        assert!(decode_cursor_snapshot(&cursor).is_err());
    }

    #[test]
    fn contact_snapshot_diff_classifies_object_changes() {
        let previous = ContactSnapshot {
            addressbook_url: "book".to_string(),
            ctag: Some("1".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "a.vcf".to_string(),
                    etag: Some("old".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "b.vcf".to_string(),
                    etag: Some("same".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "d.vcf".to_string(),
                    etag: Some("gone".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let current = ContactSnapshot {
            addressbook_url: "book".to_string(),
            ctag: Some("2".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "a.vcf".to_string(),
                    etag: Some("new".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "b.vcf".to_string(),
                    etag: Some("same".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "c.vcf".to_string(),
                    etag: Some("created".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = diff_contact_snapshots(&previous, &current);
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
            addressbook_url: "book".to_string(),
            ctag: None,
            entries: entries
                .iter()
                .map(|uri| ContactSnapshotEntry {
                    uri: (*uri).to_string(),
                    etag: Some("e".to_string()),
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
        assert!(diff_contact_snapshots(&previous, &empty).is_empty());

        // populated-vs-populated unchanged; empty-vs-empty zero.
        let same = diff_contact_snapshots(&previous, &previous);
        assert!(same.is_empty());
        assert!(diff_contact_snapshots(&empty, &empty).is_empty());
    }

    #[test]
    fn empty_poll_checkpoint_preserves_snapshot_for_the_recovery_poll() {
        let previous = snapshot_with(&["a.vcf", "b.vcf"]);
        let mut empty = snapshot_with(&[]);
        empty.ctag = Some("refreshed".to_string());

        preserve_unobserved_contact_entries(&previous, &mut empty);
        assert_eq!(empty.entries.len(), 2);
        assert_eq!(
            empty.ctag, None,
            "an empty observation must retry the listing"
        );

        let recovered = snapshot_with(&["a.vcf", "b.vcf"]);
        assert!(diff_contact_snapshots(&empty, &recovered).is_empty());
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

        let changes = diff_contact_snapshots(&previous, &current);
        assert!(
            changes.is_empty(),
            "a transiently-failed resource must not be destroyed: {changes:?}"
        );

        // Without the failed-href, b.vcf's absence IS a destroy.
        let current_no_failed = snapshot_with(&["a.vcf"]);
        let changes = diff_contact_snapshots(&previous, &current_no_failed);
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
            etag: None,
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
            etag: None,
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
