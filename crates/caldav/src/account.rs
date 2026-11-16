use crate::dialect::CalDavDialect;
use bifrost_dav_core::append_path;
use bifrost_dav_core::collection::{COLLECTION_PAGE_SIZE, Dialect};
use bifrost_dav_core::collection_account::{
    CollectionSnapshot, DavCollectionAccount, IdleLifecycle, RelocateFailure,
};
#[cfg(test)]
use bifrost_dav_core::{encode_watermark_cursor, slice_after_watermark, sorted_candidate_hrefs};
#[cfg(test)]
use std::collections::HashSet;
type Kernel = DavCollectionAccount<CalDavDialect>;
use bifrost_dav_core::{
    DavHrefKey, SnapshotEntry, dav_href_key, object_change, preserve_unobserved_entries,
    same_dav_url,
};
use bifrost_types::{
    Application, PartialCommitBuilder, PrimaryLeg, StepEffect, StepReplay, StepTargets, TargetRef,
    TargetRole,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use bifrost_types::*;
use bytes::Bytes;
#[cfg(test)]
use futures::StreamExt;
use jiff::civil;
use jiff::tz::Offset;

use crate::capabilities::{caldav_capabilities, scheduling_available};
use crate::client::{
    CalDavClient, FilteredHrefs, HrefQuery, PutCondition, event_scope, extend_candidates,
    missing_event_error, unsupported_error,
};
use crate::ical::{
    EventProjectionError, PatchError, ReplyError, WriteError, create_to_ical, event_from_ical,
    events_from_ical, new_uid, patch_to_ical, rsvp_patch, rsvp_reply_ical, split_event_id,
};
use crate::parse::CalendarCollection;
use crate::{CalDavConfig, CalDavCredentials};

/// How many `sync-collection` REPORTs one poll may spend draining an RFC 6578
/// truncated result. A bound rather than an unbounded loop: a server that keeps
/// reporting truncation must not hold one poll on the wire forever, and what is
/// left over is picked up by the next poll from the checkpointed token.
const SYNC_TRUNCATION_ROUNDS: usize = 16;
/// The page size an OMITTED `limit` means on the paging lanes
/// (`events_in_range` and `event_search`), in candidate RESOURCES.
///
/// An unbounded default would dispatch one REPORT per multiget batch across the
/// whole collection and materialize every projected event, and a recurring
/// resource expands into one event per override, so even the item count is not
/// bounded by the resource count. The value mirrors the CardDAV twin's
/// `CONTACT_PAGE_SIZE`. An explicit `limit` is honoured as given, however large,
/// and continuation rides `page_cursor`. Changing this value changes what a
/// published method returns.
const EVENT_PAGE_SIZE: usize = COLLECTION_PAGE_SIZE;

#[derive(Debug)]
pub(crate) struct CalDavAccount {
    lifecycle: IdleLifecycle,
    client: Arc<CalDavClient>,
    capabilities: AccountCapabilities,
    calendar_home: String,
    /// The collection a call that names no calendar routes to, and `None` when
    /// the home enumerated no collections.
    ///
    /// Deliberately not the calendar home in that case. See the shared kernel
    /// default-collection policy.
    pub(crate) default_calendar_url: Option<String>,
    pub(crate) calendar_urls: Vec<String>,
    rsvp_email: Option<String>,
    schedule_outbox_url: Option<String>,
}

impl CalDavAccount {
    /// Build an account directly around a client, skipping discovery.
    ///
    /// Exists so the `Account` methods can be driven against a scripted
    /// transport: discovery is several round trips of its own and would
    /// dominate a test that is about what a single call puts on the wire.
    #[cfg(test)]
    pub(crate) fn for_tests(client: Arc<CalDavClient>, default_calendar_url: &str) -> Self {
        Self::for_tests_with_collections(client, default_calendar_url, Vec::new())
    }

    /// An account whose calendar home enumerated no collections.
    ///
    /// The shape `open` produces against an empty backend: no default, and no
    /// discovered collection to fall back on.
    #[cfg(test)]
    pub(crate) fn for_tests_without_collections(client: Arc<CalDavClient>, home: &str) -> Self {
        Self {
            lifecycle: IdleLifecycle::default(),
            client,
            capabilities: crate::capabilities::caldav_capabilities(false),
            calendar_home: home.to_string(),
            default_calendar_url: None,
            calendar_urls: Vec::new(),
            rsvp_email: None,
            schedule_outbox_url: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests_with_collections(
        client: Arc<CalDavClient>,
        default_calendar_url: &str,
        additional_calendar_urls: Vec<String>,
    ) -> Self {
        Self {
            lifecycle: IdleLifecycle::default(),
            client,
            capabilities: crate::capabilities::caldav_capabilities(false),
            calendar_home: default_calendar_url.to_string(),
            default_calendar_url: Some(default_calendar_url.to_string()),
            calendar_urls: std::iter::once(default_calendar_url.to_string())
                .chain(additional_calendar_urls.iter().cloned())
                .collect(),
            rsvp_email: None,
            schedule_outbox_url: None,
        }
    }

    pub(crate) async fn open(
        account_id: AccountId,
        config: CalDavConfig,
        net: Option<Arc<bifrost_net::Net>>,
    ) -> Result<Self, AccountError> {
        let client = CalDavClient::new_on_net(account_id, &config, net.as_deref());
        Self::open_with_client(client, rsvp_email_from_config(&config)).await
    }

    /// The whole of `open` after the client exists.
    ///
    /// Split out so the discovery-to-account path can be driven against a
    /// scripted transport: `open` itself builds its client from a `CalDavConfig`
    /// and so cannot take one. Everything `open` decides - the default
    /// collection, the cursor-scope set, the RSVP capability - is decided here.
    pub(crate) async fn open_with_client(
        mut client: CalDavClient,
        configured_rsvp_email: Option<String>,
    ) -> Result<Self, AccountError> {
        let discovery = client.discover_account().await?;
        let rsvp_email = configured_rsvp_email.or(discovery.calendar_user_email);
        let schedule_outbox_url = discovery.schedule_outbox_url;
        client.admit_discovered_urls(
            std::iter::once(discovery.calendar_home.clone())
                .chain(schedule_outbox_url.iter().cloned()),
        );
        // `event_rsvp` is discovery-derived, not assumed: the RSVP path
        // below hard-requires both of these and returns `unsupported`
        // without them, so advertising the method on a scheduling-less
        // RFC 4791 store would only move the failure from the
        // capability gate to the wire.
        let event_rsvp =
            scheduling_available(rsvp_email.as_deref(), schedule_outbox_url.as_deref());
        let calendar_home = discovery.calendar_home;
        let collections = client.list_calendars(&calendar_home).await?;
        let default_calendar_url = default_collection_url(&collections);
        let calendar_urls = discovered_collection_urls(&collections);
        Ok(Self {
            lifecycle: IdleLifecycle::default(),
            client: Arc::new(client),
            capabilities: caldav_capabilities(event_rsvp),
            calendar_home,
            default_calendar_url,
            calendar_urls,
            rsvp_email,
            schedule_outbox_url,
        })
    }

    fn calendar_url(
        client: &CalDavClient,
        default_calendar_url: Option<&str>,
        calendar: Option<CalendarId>,
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        if let Some(id) = calendar {
            return Ok(client.resolve_url(&id.0));
        }
        default_calendar_url
            .map(str::to_string)
            .ok_or_else(|| Kernel::no_default(operation))
    }

    fn map_calendar(collection: CalendarCollection) -> Calendar {
        let native = collection.href;
        let name = collection
            .display_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| "Calendar".to_string());
        let can_edit = collection.can_edit.unwrap_or(true);
        Calendar {
            id: CalendarId(native.clone()),
            native_id: native.clone(),
            name,
            color: collection.color,
            provenance: CalendarProvenance {
                provider: ProtocolKind::CalDav,
                native,
                calendar_native: None,
            },
            is_default: false,
            can_create_events: can_edit,
            can_update_events: can_edit,
            can_delete_events: can_edit,
        }
    }

    /// Move one event resource into another calendar collection.
    ///
    /// WebDAV `MOVE` is the atomic form and is tried first. A server that does
    /// not implement it (405/501) falls back to PUT-to-new then
    /// DELETE-from-old, which is NOT atomic: a failure after the PUT leaves the
    /// event in both collections. Typed progress preserves that copy without
    /// changing the failed leg's classification or transmission evidence.
    ///
    /// The destination keeps the resource's own file name, so a collection that
    /// already holds that name refuses the move (`Overwrite: F`) rather than
    /// overwriting a stranger's resource.
    async fn relocate_event(
        client: &CalDavClient,
        source_url: &str,
        target_calendar: &str,
        body: String,
        content_changed: bool,
    ) -> Result<(), RelocateFailure> {
        Kernel::relocate(
            client.collection_dispatch(),
            source_url,
            target_calendar,
            body,
            content_changed,
            AccountOperation::EventUpdate,
            || format!("{}.{}", new_uid(), CalDavDialect::FILE_EXTENSION),
        )
        .await
    }

    async fn fetch_event_from_url(
        client: Arc<CalDavClient>,
        default_calendar_url: Option<String>,
        calendar: Option<CalendarId>,
        event: EventId,
        operation: AccountOperation,
    ) -> Result<CalendarEvent, AccountError> {
        // A recurrence-qualified id reads the override it names out of the
        // resource; the wire address is the resource alone.
        let (resource, instance) = split_event_id(&event.0);
        let url = client.resolve_url(resource);
        // The event's own collection is the first answer and almost always
        // available; the default is only reached for a native id whose parent
        // cannot be derived, and an account with no collections has none.
        let calendar_url = match calendar {
            Some(calendar) => client.resolve_url(&calendar.0),
            None => event_calendar_url(&url)
                .or(default_calendar_url)
                .ok_or_else(|| Kernel::no_default(operation))?,
        };
        let fetched = client.get_event(&url, operation).await.map_err(|error| {
            error
                .clone()
                .into_builder()
                .scope(event_scope(event.0.clone()))
                .try_build()
                .unwrap_or(error)
        })?;
        event_from_ical(
            fetched.uri.clone(),
            CalendarId(calendar_url),
            fetched.etag,
            &fetched.data,
            instance,
        )
        // The body is the SERVER's answer to a GET, so a body that will not
        // tokenize is the provider's malformed response - `Protocol(ParseFailed)`
        // with an acknowledged attempt - never the caller's malformed request.
        // It was `local_error` (`Request(Malformed)` -> `ClientBug`), which told
        // the consumer to fix a request it had nothing to do with.
        //
        // Both projection failures carry `event_scope`, as the GET failure just
        // above does: they are about the same resource, and a consumer routing
        // on the scope must not lose the target because the server's answer
        // failed one step later. The no-VEVENT case carried the id only inside
        // its cause, and the parse case carried nothing.
        .map_err(|error| {
            let error = match error {
                EventProjectionError::NoVevent | EventProjectionError::NoInstance => {
                    missing_event_error(operation, event.0.clone())
                }
                // A caller error, not the server's: the id does not say which
                // of several overrides it wants, and the listing gave each its
                // own qualified id.
                EventProjectionError::AmbiguousResource => crate::client::local_error(
                    operation,
                    "the resource holds several recurrence overrides and no master, \
                     so the bare resource id names none of them: address one by its \
                     recurrence-qualified id",
                ),
                EventProjectionError::Parse(parse) => crate::client::parse_error(
                    operation,
                    format!("CalDAV resource is not valid iCalendar: {}", parse.0),
                ),
            };
            error
                .into_builder()
                .scope(event_scope(event.0))
                .try_build()
                .expect("adding a scope cannot invalidate a classified error")
        })
    }

    async fn event_snapshot(
        client: &CalDavClient,
        calendar: &str,
        operation: AccountOperation,
    ) -> Result<EventSnapshot, AccountError> {
        let sync_token = client.collection_sync_token(calendar, operation).await?;
        // VEVENT-filtered, not the bare depth-1 PROPFIND: a VTODO or VJOURNAL
        // sharing the collection would otherwise enter the event snapshot and be
        // emitted as a created/updated event change that hydrates to nothing.
        // Degrades to the unfiltered listing when the server refuses the filter.
        let listing = client
            .list_event_hrefs_filtered(calendar, operation)
            .await?;
        Kernel::snapshot_from_listing(calendar, sync_token, listing, operation)
    }
}

impl Account for CalDavAccount {
    bifrost_dav_core::dav_unsupported_account_methods!(CalDavDialect);

    fn protocol(&self) -> bifrost_types::Protocol {
        bifrost_types::Protocol::CalDav
    }

    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.client.net().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<bifrost_types::BandwidthCap>) {
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
        Kernel::discover_scopes(self.calendar_urls.iter().cloned())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        self.lifecycle.stream()
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let client = Arc::clone(&self.client);
        Kernel::establish(
            scope,
            self.default_calendar_url.clone(),
            self.calendar_urls.clone(),
            move |collection| async move {
                Self::event_snapshot(&client, &collection, AccountOperation::EstablishCursor).await
            },
        )
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        let (client, tally) = self.client.metered();
        Kernel::inventory(
            scope,
            self.default_calendar_url.clone(),
            self.calendar_urls.clone(),
            tally,
            move |collection| async move {
                Self::event_snapshot(&client, &collection, AccountOperation::SyncInventory).await
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
        Kernel::unsupported_future(AccountOperation::AddressBooksList)
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Kernel::unsupported_future(AccountOperation::ContactsList)
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        Kernel::unsupported_future(AccountOperation::ContactGet)
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        Kernel::unsupported_future(AccountOperation::ContactCreate)
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Kernel::unsupported_future(AccountOperation::ContactUpdate)
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        Kernel::unsupported_future(AccountOperation::ContactDelete)
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Kernel::unsupported_future(AccountOperation::ContactSearch)
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        Kernel::unsupported_future(AccountOperation::DirectorySearch)
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        let client = Arc::clone(&self.client);
        let home = self.calendar_home.clone();
        Box::pin(async move {
            let collections = client.list_calendars(&home).await?;
            let mut calendars = collections
                .into_iter()
                .map(Self::map_calendar)
                .collect::<Vec<_>>();
            // A calendar-home that enumerates zero calendar collections
            // surfaces as an empty list, not a fabricated placeholder. The
            // depth-1 PROPFIND above returns the home's own response too, so a
            // home that is itself a calendar collection (resourcetype includes
            // <calendar/>) is already mapped by the parse path. An empty result
            // here therefore means a genuinely empty backend; reporting it as
            // empty lets a consumer reap stale calendars rather than chase a
            // phantom home-calendar whose events_in_range REPORT a spec-correct
            // server 404s.
            if let Some(first) = calendars.first_mut() {
                first.is_default = true;
            }
            Ok(calendars)
        })
    }

    /// Lists events overlapping a window, one page at a time.
    ///
    /// **An omitted `limit` defaults to `EVENT_PAGE_SIZE` (250)** candidate
    /// resources, as the CardDAV twin's `contact_search` does with
    /// `CONTACT_PAGE_SIZE`. The page counts RESOURCES, so a recurring resource
    /// that expands into overrides can carry more items than that. An explicit
    /// `limit` is honoured as given, however large, and a page with more
    /// candidates behind it carries a `page_cursor` continuation.
    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            let watermark = Kernel::decode_page_cursor(
                range.page_cursor.clone(),
                AccountOperation::EventsInRange,
            )?;
            let calendar_url = client.resolve_url(&range.calendar_id.0);
            let (range_start, range_end) = caldav_query_range(&range.start, &range.end)?;
            // An omitted `limit` means `EVENT_PAGE_SIZE`; an explicit one is
            // honoured as given (a `u32` that does not fit `usize` saturates,
            // which truncates nothing).
            let page_size = range.limit.map_or(EVENT_PAGE_SIZE, |value| {
                usize::try_from(value).unwrap_or(usize::MAX)
            });
            // Query hrefs before hydration. The shared guard only rejects
            // provably disjoint intervals; the server resolves recurrences
            // and floating dates using the calendar's timezone context.
            let candidates = match client
                .query_event_hrefs_in_range(&calendar_url, Some(&range_start), Some(&range_end))
                .await?
            {
                FilteredHrefs::Matched(query) => query,
                // A server that will not run the filter must not fail the
                // call. Listing the collection is the degrade: it costs one
                // depth-1 PROPFIND, still hydrates only the page, and returns
                // the same events for the same cursor.
                FilteredHrefs::FilterUnsupported => {
                    listing_candidates(&client, &calendar_url, AccountOperation::EventsInRange)
                        .await?
                }
            };
            hydrated_event_page(
                &client,
                &calendar_url,
                candidates,
                watermark.as_deref(),
                page_size,
                AccountOperation::EventsInRange,
                |event| range.admits(event),
            )
            .await
        })
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            Self::fetch_event_from_url(
                client,
                default_calendar_url,
                None,
                event,
                AccountOperation::EventGet,
            )
            .await
        })
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            let calendar_url = Self::calendar_url(
                &client,
                default_calendar_url.as_deref(),
                Some(event.calendar_id.clone()),
                AccountOperation::EventCreate,
            )?;
            let uid = new_uid();
            let path = format!("{uid}.ics");
            let url = append_path(&calendar_url, &path);
            let body = create_to_ical(&event, &uid)
                .map_err(|error| write_error(AccountOperation::EventCreate, error))?;
            client
                .put_event(
                    &url,
                    body,
                    PutCondition::IfNoneMatch,
                    AccountOperation::EventCreate,
                )
                .await
                // The URL and the UID are minted LOCALLY, so nothing else in the
                // failure names them. A dropped create-PUT derives
                // `Reconcile(CheckTarget)`, and a consumer told to check a
                // target the error does not identify can only re-create - two
                // copies of one event, which is exactly what the unreplayable
                // declaration exists to prevent. The scope carries the minted
                // URL (whose final segment is the UID) so the probe is possible.
                .map_err(|error| {
                    error
                        .clone()
                        .into_builder()
                        .scope(event_scope(url.clone()))
                        .try_build()
                        .unwrap_or(error)
                })?;
            Ok(EventId(url))
        })
    }

    // AccountOperation::EventUpdate.skips_absent_target() is false: Outlook
    // users need to know that an edit to a vanished event was not applied.
    // Preserve fetch, PUT and relocation failures, including missing destinations.
    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            let expected_etag = patch
                .expected_version
                .as_ref()
                .map(|version| {
                    version.expected_etag(
                        bifrost_types::error::Protocol::CalDav,
                        AccountOperation::EventUpdate,
                        true,
                    )
                })
                .transpose()?;

            let (resource, instance) = split_event_id(&event.0);
            let url = client.resolve_url(resource);
            // A `calendar_id` naming a collection other than the event's own is
            // a relocation. It used to return `Ok(())` having moved nothing,
            // then was refused outright; it is now performed.
            let relocation = patch
                .calendar_id
                .as_ref()
                .map(|calendar| client.resolve_url(&calendar.0))
                .filter(|target| {
                    event_calendar_url(&url).is_some_and(|source| !same_url(target, &source))
                });
            // A MOVE relocates the whole resource, every occurrence with it,
            // so it cannot be done on behalf of one override.
            if instance.is_some() && relocation.is_some() {
                return Err(crate::client::local_error(
                    AccountOperation::EventUpdate,
                    "a recurrence-instance event id cannot be moved to another \
                     calendar: the move would relocate the whole series resource",
                ));
            }
            if expected_etag.is_some() && relocation.is_some() {
                return Err(unsupported_error(AccountOperation::EventUpdate));
            }
            let current = Self::fetch_event_from_url(
                Arc::clone(&client),
                default_calendar_url,
                patch.calendar_id.clone(),
                event.clone(),
                AccountOperation::EventUpdate,
            )
            .await?;
            let body = patch_to_ical(&current, &patch)
                .map_err(|error| patch_error(AccountOperation::EventUpdate, error))?;
            let Some(target_calendar) = relocation else {
                client
                    .put_event(
                        &url,
                        body,
                        expected_etag.map_or_else(
                            || PutCondition::from_tag(current.etag.as_ref()),
                            |etag| PutCondition::from_etag(Some(etag)),
                        ),
                        AccountOperation::EventUpdate,
                    )
                    .await?;
                return Ok(());
            };
            // A content patch riding along with the move needs a write of its
            // own; a move-only patch does not, and must not be charged a
            // partial-failure verdict for a leg it never needed.
            let content_changed = patch_changes_content(&patch);
            Self::relocate_event(&client, &url, &target_calendar, body, content_changed)
                .await
                .map_err(|failure| Kernel::relocation_error(failure, &url))
        })
    }

    // AccountOperation::EventDelete.skips_absent_target() is false, but the
    // client's delete_event absorbs 404/410 before this door sees them. That
    // is coupled to replay: a retry after a landed DELETE can itself return
    // 404. Do not fake policy compliance with a preflight existence probe or
    // turn Ok into NotFound here; neither can distinguish that committed write
    // from prior absence. Conformance needs a client/dispatch replay change.
    // Conditional delete below already preserves the caller's failure.
    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            reject_recurrence_instance_id(&event, AccountOperation::EventDelete)?;
            let url = client.resolve_url(&event.0);
            client
                .delete_event(&url, AccountOperation::EventDelete)
                .await
        })
    }

    fn event_delete_if_version(
        &self,
        event: EventId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let Some(version) = expected_version else {
            return self.event_delete(event);
        };
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            let etag = version.expected_etag(
                bifrost_types::error::Protocol::CalDav,
                AccountOperation::EventDelete,
                true,
            )?;
            reject_recurrence_instance_id(&event, AccountOperation::EventDelete)?;
            let url = client.resolve_url(&event.0);
            client
                .delete_event_if_match(&url, etag, AccountOperation::EventDelete)
                .await
        })
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        let rsvp_email = self.rsvp_email.clone();
        let schedule_outbox_url = self.schedule_outbox_url.clone();
        Box::pin(async move {
            reject_recurrence_instance_id(&event, AccountOperation::EventRsvp)?;
            // Use the capability predicate here too: a present but blank value
            // cannot identify an attendee or a scheduling destination.
            if !scheduling_available(rsvp_email.as_deref(), schedule_outbox_url.as_deref()) {
                return Err(unsupported_error(AccountOperation::EventRsvp));
            }
            let Some(rsvp_email) = rsvp_email else {
                return Err(unsupported_error(AccountOperation::EventRsvp));
            };
            let Some(schedule_outbox_url) = schedule_outbox_url else {
                return Err(unsupported_error(AccountOperation::EventRsvp));
            };
            let current = Self::fetch_event_from_url(
                Arc::clone(&client),
                default_calendar_url,
                None,
                event.clone(),
                AccountOperation::EventRsvp,
            )
            .await?;
            let reply =
                rsvp_reply_ical(&current, status, &rsvp_email).map_err(|error| match error {
                    ReplyError::Missing(_) => unsupported_error(AccountOperation::EventRsvp),
                    ReplyError::Write(error) => write_error(AccountOperation::EventRsvp, error),
                })?;
            // Defensive on the success path: `rsvp_reply_ical` above
            // already fails when the event names no organizer, so this
            // guard cannot fire after it succeeded. Kept anyway - the
            // alternative is an `expect` that panics if that coupling
            // ever loosens, and an error is the better failure mode.
            let organizer_email = current
                .organizer
                .as_ref()
                .map(|organizer| organizer.email.clone())
                .ok_or_else(|| unsupported_error(AccountOperation::EventRsvp))?;
            client
                .post_schedule_reply(&schedule_outbox_url, &rsvp_email, &organizer_email, reply)
                .await?;
            // Everything from here on runs after the organizer has already
            // been told. Every failure from here on carries the committed
            // reply: a local encoding failure as an effectless leg, a failed
            // PUT as the update leg.
            let patch = rsvp_patch(&current, status, &rsvp_email).map_err(|_| {
                rsvp_error(
                    unsupported_error(AccountOperation::EventRsvp),
                    &event,
                    false,
                )
            })?;
            let body = patch_to_ical(&current, &patch).map_err(|error| {
                rsvp_error(
                    patch_error(AccountOperation::EventRsvp, error),
                    &event,
                    false,
                )
            })?;
            let url = client.resolve_url(&event.0);
            client
                .put_event(
                    &url,
                    body,
                    PutCondition::from_tag(current.etag.as_ref()),
                    AccountOperation::EventRsvp,
                )
                .await
                .map_err(|error| rsvp_error(error, &event, true))?;
            Ok(())
        })
    }

    /// Searches events by text, one page at a time.
    ///
    /// **An omitted `limit` defaults to `EVENT_PAGE_SIZE` (250)** candidate
    /// resources, exactly as in `events_in_range`; an explicit `limit` is
    /// honoured as given and continuation rides `page_cursor`.
    ///
    /// Unless `include_cancelled` is set, events whose status is `Cancelled`
    /// are dropped by a client-side filter on the local match. `estimated_total`
    /// stays the number of candidate resources the server named, an upper bound
    /// on the items that the text match and this filter can only lower.
    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            let watermark = Kernel::decode_page_cursor(
                request.page_cursor.clone(),
                AccountOperation::EventSearch,
            )?;
            let calendar_url = Self::calendar_url(
                &client,
                default_calendar_url.as_deref(),
                request.calendar_id,
                AccountOperation::EventSearch,
            )?;
            let needle = request.query.to_lowercase();
            // Same bounded default as `events_in_range`.
            let page_size = request.limit.map_or(EVENT_PAGE_SIZE, |value| {
                usize::try_from(value).unwrap_or(usize::MAX)
            });
            let include_cancelled = request.include_cancelled;
            // Both lanes below page the same way: get candidate hrefs, slice at
            // the watermark, multiget only the page. They differ only in where
            // the candidates come from - a server-side text-match for a real
            // query, the depth-1 listing for a match-all or a server that will
            // not run the filter - which is what keeps one cursor valid across
            // a mid-walk degrade. Both legs can lose individual resources
            // inside their 207, so both feed `failed_ids`.
            let candidates = if needle.is_empty() {
                listing_candidates(&client, &calendar_url, AccountOperation::EventSearch).await?
            } else {
                match client
                    .query_event_hrefs_text(&calendar_url, &request.query)
                    .await?
                {
                    FilteredHrefs::Matched(query) => query,
                    FilteredHrefs::FilterUnsupported => {
                        listing_candidates(&client, &calendar_url, AccountOperation::EventSearch)
                            .await?
                    }
                }
            };
            hydrated_event_page(
                &client,
                &calendar_url,
                candidates,
                watermark.as_deref(),
                page_size,
                AccountOperation::EventSearch,
                // The server-side text-match is a PREFILTER; the local match
                // stays the authority over the page. An empty needle matches
                // everything, which is the match-all lane. The cancelled filter
                // is client-side for the same reason: a server-side status
                // filter would be lost on the degrade lane anyway.
                |event| {
                    (include_cancelled || event.status != EventStatus::Cancelled)
                        && (needle.is_empty() || event_matches(event, &needle))
                },
            )
            .await
        })
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        self.lifecycle.close();
        Box::pin(async { Ok(()) })
    }
}

fn rsvp_email_from_config(config: &CalDavConfig) -> Option<String> {
    match &config.credentials {
        CalDavCredentials::Basic { username, .. } if username.contains('@') => {
            Some(username.to_ascii_lowercase())
        }
        _ => None,
    }
}

/// The whole collection as candidate hrefs, from the depth-1 PROPFIND.
///
/// The match-all lane's source, and the degrade every filtered lane falls back
/// to when the server refuses to run its filter. The listing is the etag-bearing
/// PROPFIND the poll path already runs, and its href is exactly the key the page
/// cursor carries.
///
/// Accepted limit, intrinsic to the degrade rather than a regression: this is
/// the UNFILTERED depth-1 PROPFIND by definition, so VTODO and VJOURNAL
/// resources in the same collection come back as candidates and nothing can
/// exclude them server-side. They never reach `Page::items` - a resource with
/// no VEVENT projects to no events - but they DO count toward
/// `estimated_total` and they DO consume slots in the sliced page, so a page
/// served off this lane can come back short of `limit` for a reason the
/// consumer cannot observe. Closing it would need per-resource component-type
/// evidence, which only the filtered lane has.
async fn listing_candidates(
    client: &CalDavClient,
    calendar_url: &str,
    operation: AccountOperation,
) -> Result<HrefQuery, AccountError> {
    let listing = client.list_events_listing(calendar_url, operation).await?;
    let mut candidates = HrefQuery::default();
    extend_candidates(&mut candidates, listing);
    Ok(candidates)
}

/// Slice the page that follows `watermark` out of the candidate hrefs, multiget
/// ONLY that page, and project it.
///
/// Every paging lane in this crate funnels through here, which is what keeps one
/// cursor valid across them: the key is always the resource HREF, never the
/// event id. That distinction matters because a recurring resource projects to
/// several events - the recurrence-qualified `EventId` keys (`{uri}#{rid}`)
/// survive on the items, but they are not what the cursor carries. Keying the
/// cursor on the event id instead would make the filtered lane and its
/// whole-collection degrade disagree about what "already served" means, and a
/// server that starts refusing the filter mid-walk would re-serve or skip the
/// override instances of the boundary resource.
///
/// The page size therefore counts RESOURCES, not emitted events: a page may
/// carry more items than `limit` when its resources expand into overrides, and
/// fewer when `keep` rejects some. `estimated_total` is the number of candidate
/// resources the server named, which is an upper bound on the items.
///
/// `keep` is the local match, and it is the AUTHORITY over the page even where
/// the server already filtered - the server side is a prefilter that may be
/// generous (or, on the degrade lane, absent entirely).
///
/// `failed_ids` carries both legs' casualties: the candidate leg's refused
/// hrefs (collection-wide, and re-observed on every page, per the
/// `Page::failed_ids` contract) and the ones this page's own multiget lost.
async fn hydrated_event_page<K: Fn(&CalendarEvent) -> bool>(
    client: &CalDavClient,
    calendar_url: &str,
    candidates: HrefQuery,
    watermark: Option<&str>,
    page_size: usize,
    operation: AccountOperation,
    keep: K,
) -> Result<Page<CalendarEvent>, AccountError> {
    Kernel::hydrated_page(
        bifrost_dav_core::collection_account::PageRequest {
            collection: calendar_url,
            candidates,
            watermark,
            page_size,
        },
        |hrefs| async move { client.fetch_events(calendar_url, &hrefs, operation).await },
        |event| {
            events_from_ical(
                event.uri,
                CalendarId(calendar_url.to_owned()),
                event.etag,
                &event.data,
            )
        },
        keep,
        |event| event.id.0.as_str(),
    )
    .await
}

/// Classify a patch that could not be spliced, by what stopped it.
///
/// Every cause used to map to `Unsupported`, which is right for one of them
/// only: a recurrence replacement on a resource with override VEVENTs is a
/// request this client cannot express.
///
/// The other two are this crate breaking its own invariant, so they are
/// `Internal(InvariantViolated)` - never retried, reported as a bug - with no
/// attempt, since no write was issued. An event with no source body cannot
/// exist (`project_event` always sets one). A body with no VEVENT to splice
/// into cannot either: the event was projected from that very body, and the
/// writer splices by the projection's own parse, so the VEVENT it came from is
/// always there. While the writer re-read the body by rules of its own it was
/// reachable, and every body that reached it was the two readers disagreeing,
/// not the server erring - which is why it is not the provider's fault.
fn patch_error(operation: AccountOperation, error: PatchError) -> AccountError {
    let detail = match error {
        PatchError::RecurrenceOverride => return unsupported_error(operation),
        PatchError::Write(error) => return write_error(operation, error),
        PatchError::NoSpliceableVevent => {
            "CalDAV patch writer found no VEVENT in the body the event was projected from"
        }
        PatchError::MissingSourceBody => {
            "CalDAV event reached the patch writer without its source iCalendar body"
        }
    };
    AccountErrorBuilder::new(PrimaryCause::Internal(InternalCause::new(
        InternalErrorKind::InvariantViolated,
        Some(DiagnosticText::support_only(detail)),
    )))
    .protocol(Protocol::CalDav)
    .operation(operation)
    .try_build()
    .expect("valid internal invariant classification")
}

/// A value the writers refuse (`WriteError`) is the caller's own request being
/// unwritable: local `Request(Malformed)` naming the field, raised before any
/// write goes out (a create sends nothing at all; an update has already read the
/// resource it splices into, since the splice needs its body). The value itself is not echoed into the
/// message, so a hostile string cannot reach a log through it.
fn write_error(operation: AccountOperation, error: WriteError) -> AccountError {
    crate::client::local_error(operation, format!("event {} {}", error.field, error.reason))
}

/// Attach the accepted scheduling reply to a failure after the outbox POST.
///
/// RSVP is non-atomic by nature: iTIP delivers the reply to the organizer
/// through the scheduling outbox, and the attendee's own copy is a separate
/// resource only a second write can update. Nothing un-sends a reply, so the
/// committed `Submitted` step is `Blocking` (a replay re-sends it) and no
/// compensation exists. `write` is true for the PUT leg (an `Updated` primary
/// whose application comes from its leaf) and false for the local encoding
/// failures between POST and PUT (an effectless primary).
fn rsvp_error(error: AccountError, event: &EventId, write: bool) -> AccountError {
    let targets = StepTargets::complete(vec![TargetRef::new(
        TargetRole::Subject,
        ErrorScope::Event { id: event.clone() },
    )]);
    let primary = if write {
        PrimaryLeg::new(
            StepEffect::Updated,
            targets.clone(),
            StepReplay::Repeatable,
            Application::from_leaf(&error),
        )
    } else {
        PrimaryLeg::effectless(targets.clone())
    };
    let mut record = PartialCommitBuilder::new(primary);
    record.committed(StepEffect::Submitted, targets, StepReplay::Blocking);
    error
        .into_builder()
        .partial_commit(record)
        .try_build()
        .expect("valid scheduling record")
}

/// Whether the patch changes anything other than which collection the event
/// lives in.
///
/// Derived by zeroing the relocation field and comparing against an empty
/// patch, rather than enumerating the content fields: a field added to
/// `EventPatch` is then covered automatically, where a hand-listed check would
/// silently stop noticing it.
///
/// Comparing SERIALIZED bytes is not equivalent - the writers re-emit, so a
/// move-only patch can compare unequal to the fetched body and earn a redundant
/// write plus the partial-failure verdict that rides on it. Twin of
/// `bifrost-carddav`'s `patch_changes_content`.
fn patch_changes_content(patch: &EventPatch) -> bool {
    let content = EventPatch {
        calendar_id: None,
        expected_version: None,
        ..patch.clone()
    };
    content != EventPatch::default()
}

/// Derive the parent collection URL of an event resource URL.
///
/// This must go through a real URL parse. A final-slash search over the whole
/// string picks up a slash living in the query or fragment - for
/// `https://dav.test/cal/one.ics?redirect=/foo` it yields
/// `https://dav.test/cal/one.ics?redirect=/`, which is not a collection URL at
/// all, and which then travels onward as `calendar_id` and `calendar_native`.
/// Query and fragment are dropped before the final path segment is removed.
fn event_calendar_url(event_url: &str) -> Option<String> {
    bifrost_dav_core::url::parent_collection_url(event_url)
}

/// Publish a partially-refused walk as a skipped scope.
///
/// A REPORT leg that failed wholly means this calendar was not fully
/// searched. The items already collected stay valid, but "no more matches"
/// is not what happened, and the consumer needs the classified failure to
/// know whether to reauthorize, retry, or stop. `failed_ids` cannot carry
/// that: it is a bare list of resource ids with no recovery class, and the
/// refused leg often does not even name the resources it lost.
#[cfg(test)]
fn skipped_calendar_scope(calendar_url: &str, degraded: Option<AccountError>) -> Vec<SkippedScope> {
    Kernel::skipped_scope(calendar_url, degraded)
}

/// Resource identity for a consumer-restated calendar id against a URL this
/// crate derived. Delegates to the shared normalizing comparison: percent
/// encoding, host case and a redundant default port are spellings, not
/// relocations, and reading one as a move issues a MOVE onto the collection the
/// resource already lives in (refused by `Overwrite: F` as a spurious 412).
fn same_url(left: &str, right: &str) -> bool {
    same_dav_url(left, right)
}

type EventSnapshot = CollectionSnapshot;

/// One polled calendar member. The shape, the cursor codec, the diff and the
/// page slicer are all shared with `bifrost-carddav` through
/// `bifrost-dav-core`; only the magic bytes and the token's name differ.
type EventSnapshotEntry = SnapshotEntry;

/// Refuse an `EventId` that names one occurrence of a recurring series, for the
/// operations that act on the whole resource.
///
/// `events_from_ical` mints `EventId("{uri}#{recurrence_id}")` for an override
/// VEVENT so a consumer's index can tell the occurrences of a series apart.
/// `event_get` and `event_update` resolve such an id to the override it names
/// (`split_event_id` peels the fragment off the wire address and the
/// `RECURRENCE-ID` selects the VEVENT). `event_delete` and `event_rsvp` do not:
/// a DELETE removes the whole `.ics`, so deleting one occurrence would destroy
/// every occurrence, and an RSVP answers for the series through the outbox.
/// Both would act on more than the id names, so they refuse before any I/O with
/// `Request(Malformed)` -> `ClientBug`, which no retry or reopen can heal.
///
/// Real per-occurrence delete means an `EXDATE` on the master or
/// `STATUS:CANCELLED` on the override - they differ in what attendees see -
/// and is deliberately NOT scheduled: this is the only calendar crate with the
/// problem, because it is the only one whose provider has no per-occurrence
/// resource. Graph syncs through `calendarView`, whose occurrences carry
/// genuine Graph ids; JMAP keeps overrides inside the master object and returns
/// `Unsupported` for one it cannot represent; Google's ids are
/// `{calendar}::{event}` over the provider's own instance ids.
fn reject_recurrence_instance_id(
    event: &EventId,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if event.0.contains('#') {
        return Err(crate::client::local_error(
            operation,
            "this operation cannot target one recurrence instance: it acts on the \
             whole series resource, so it would change or destroy every occurrence",
        ));
    }
    Ok(())
}

/// The cursor-scope collection set: one entry per DISCOVERED calendar, and
/// NOTHING when the home holds none.
///
/// `list_calendars` already returns the home itself when the home is genuinely
/// a calendar collection, so an empty result means an empty backend rather
/// than a discovery shape this crate has to paper over. Substituting the home
/// URL here would advertise a folder that does not exist, and cursor
/// establishment and inventory would then issue requests against it and
/// commonly terminate with 404. It would also contradict the empty-home
/// contract the listing APIs are pinned to.
fn discovered_collection_urls(collections: &[crate::parse::CalendarCollection]) -> Vec<String> {
    Kernel::discovered_collections(
        collections
            .iter()
            .map(|collection| collection.href.as_str()),
    )
}

/// The collection a call that names no calendar routes to: the first
/// discovered one, and `None` when the home enumerated none.
///
/// Deliberately has no access to the calendar home, so the fallback this
/// replaced cannot be reintroduced here without also changing the signature.
/// The shared kernel deliberately has no home URL to substitute.
fn default_collection_url(collections: &[crate::parse::CalendarCollection]) -> Option<String> {
    Kernel::default_collection(
        collections
            .iter()
            .map(|collection| collection.href.as_str()),
    )
}

/// Poll for changes, preferring the RFC 6578 `sync-collection` REPORT and
/// falling back to a full listing plus snapshot diff when no token is held.
///
/// Two accepted gaps, recorded so neither is re-filed as untouched work. The
/// token-RETENTION path below - a response that omits the required `sync-token`,
/// where the previous token is kept so the next poll re-asks from the same
/// point rather than silently restarting coverage - has no direct test; it is
/// covered only incidentally through the drain tests. And the omission itself
/// is reported LOG-ONLY: a server violating the RFC this way is not surfaced to
/// the consumer as an error, because retaining the token is the correct and
/// lossless response and there is nothing for a caller to do about it. Raising
/// it would fail a poll that in fact lost nothing.
async fn changes_from_cursor(
    client: &CalDavClient,
    previous: &EventSnapshot,
) -> Result<(EventSnapshot, Vec<Change>), AccountError> {
    if let Some(sync_token) = previous.token.as_deref() {
        let mut current = previous.clone();
        current.failed_hrefs.clear();
        let mut changes = Vec::new();
        let mut token = sync_token.to_string();
        // RFC 6578 s3.6: a server may answer a sync REPORT with only part of
        // the change set, marking it with a 507 response for the collection
        // itself and a token that represents that partial progress. The token
        // is still a valid continuation point, so stopping here would leave
        // changes for the next poll rather than lose them. Drain now to reduce
        // that latency; the round cap and same-token stop remain safe because
        // either leaves the last valid token available for the next poll.
        for _ in 0..SYNC_TRUNCATION_ROUNDS {
            let report = client.sync_events(&previous.collection_url, &token).await?;
            let truncated = report.truncated;
            let next_token = report.sync_token;
            changes.extend(apply_sync_report(&mut current, report.entries));
            let Some(next_token) = next_token else {
                tracing::warn!(
                    target: "bifrost_caldav::sync",
                    calendar = %previous.collection_url,
                    "sync-collection response omitted the required sync-token; retaining the previous token"
                );
                break;
            };
            let observation =
                preserve_unobserved_entries(&[], &mut current.entries, &current.failed_hrefs);
            current.token =
                observation.advance_token(current.token.take(), Some(next_token.clone()));
            if !observation.complete {
                break;
            }
            if !truncated {
                break;
            }
            // Forward progress or stop. A server that repeats the token it was
            // given while still claiming truncation would otherwise spin this
            // loop against the wire for as many rounds as the cap allows.
            if next_token == token {
                tracing::warn!(
                    target: "bifrost_caldav::sync",
                    calendar = %previous.collection_url,
                    "truncated sync-collection response returned the same sync-token; stopping the drain"
                );
                break;
            }
            token = next_token;
        }
        return Ok((current, changes));
    }
    let mut current = CalDavAccount::event_snapshot(
        client,
        &previous.collection_url,
        AccountOperation::SyncChanges,
    )
    .await?;
    current.preserve_unobserved(previous);
    let changes = current.changes_from(previous);
    Ok((current, changes))
}

fn apply_sync_report(
    current: &mut EventSnapshot,
    entries: Vec<crate::parse::CalDavSyncEntry>,
) -> Vec<Change> {
    apply_sync_report_with_key(current, entries, dav_href_key)
}

fn apply_sync_report_with_key(
    current: &mut EventSnapshot,
    entries: Vec<crate::parse::CalDavSyncEntry>,
    mut key_for: impl FnMut(&str) -> DavHrefKey,
) -> Vec<Change> {
    let mut aliases: HashMap<DavHrefKey, BTreeSet<String>> = HashMap::new();
    for entry in &current.entries {
        aliases
            .entry(key_for(&entry.uri))
            .or_default()
            .insert(entry.uri.clone());
    }
    let mut changes = Vec::new();
    for entry in entries {
        let key = key_for(&entry.uri);
        // Keep the already published native id when a REPORT changes only
        // URL spelling. Global canonicalization would also require changing
        // listing and hydration identities and migrating persisted cursors.
        // Exact spelling wins; otherwise retain the first sorted equivalent,
        // just as the former snapshot scan did. Store ids rather than vector
        // offsets, which shift whenever a report inserts or removes a member.
        let uri = if current
            .entries
            .binary_search_by(|known| known.uri.cmp(&entry.uri))
            .is_ok()
        {
            entry.uri.clone()
        } else {
            aliases
                .get(&key)
                .and_then(BTreeSet::first)
                .cloned()
                .unwrap_or_else(|| entry.uri.clone())
        };
        // A per-member status that is neither success nor a removal is a
        // refusal to report on THAT member (403, 507, 503 ...). It says nothing
        // about the resource's content, so the prior snapshot entry is
        // preserved untouched: upserting it would record an etag-less entry and
        // emit a Created/Updated for a resource nobody observed, and the
        // etag-less entry then makes the next poll report an Updated as well.
        if entry
            .status
            .is_some_and(|code| !(200..300).contains(&code) && !matches!(code, 404 | 410))
        {
            current.failed_hrefs.push(uri);
            continue;
        }
        // A member the server positively declared a non-event (RFC 4791
        // `component=` on its content type) is treated as absent: it never
        // enters the snapshot, and one that slipped in earlier - reported as a
        // created event that hydrated to nothing, on a poll before the
        // evidence was read - leaves it now with the Destroyed the consumer
        // needs to drop the phantom. A member with NO such declaration is
        // admitted exactly as before; see `parse::declared_non_vevent`.
        if matches!(entry.status, Some(404 | 410)) || entry.non_vevent {
            // Only emit a Destroyed event for an href the prior snapshot
            // actually held. A 404/410 sync-report entry for an unknown
            // href (a resource created and deleted between polls, or one
            // the consumer never saw) would otherwise surface a phantom
            // delete for an id the consumer has no record of.
            if let Ok(index) = current
                .entries
                .binary_search_by(|known| known.uri.cmp(&uri))
            {
                current.entries.remove(index);
                if let Some(ids) = aliases.get_mut(&key) {
                    ids.remove(&uri);
                }
                changes.push(object_change(&uri, ObjectChangeKind::Destroyed));
            }
            continue;
        }
        let resolved = EventSnapshotEntry {
            uri: uri.clone(),
            etag: entry.etag.map(Into::into),
        };
        match current
            .entries
            .binary_search_by(|known| known.uri.cmp(&resolved.uri))
        {
            Ok(index) => {
                if current.entries[index].etag != resolved.etag {
                    current.entries[index] = resolved;
                    changes.push(object_change(&uri, ObjectChangeKind::Updated));
                }
            }
            Err(index) => {
                current.entries.insert(index, resolved);
                aliases.entry(key).or_default().insert(uri.clone());
                changes.push(object_change(&uri, ObjectChangeKind::Created));
            }
        }
    }
    changes
}

fn caldav_query_time(time: &EventTime) -> Option<String> {
    let instant = if let bifrost_types::EventTimeKind::Date(date) = time.kind() {
        Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok()?
    } else {
        time.instant()?
    };
    Some(
        Offset::UTC
            .to_datetime(instant)
            .strftime("%Y%m%dT%H%M%SZ")
            .to_string(),
    )
}

fn caldav_query_range(
    start: &EventTime,
    end: &EventTime,
) -> Result<(String, String), AccountError> {
    let start = caldav_query_time(start).ok_or_else(|| {
        crate::client::local_error(
            AccountOperation::EventsInRange,
            format!("invalid event range start: {}", start.value()),
        )
    })?;
    let end = caldav_query_time(end).ok_or_else(|| {
        crate::client::local_error(
            AccountOperation::EventsInRange,
            format!("invalid event range end: {}", end.value()),
        )
    })?;
    Ok((start, end))
}

fn event_matches(event: &CalendarEvent, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let needle = needle.to_lowercase();
    event
        .title
        .as_deref()
        .is_some_and(|value| contains(value, &needle))
        || event
            .description
            .as_deref()
            .is_some_and(|value| contains(value, &needle))
        || event
            .location
            .as_deref()
            .is_some_and(|value| contains(value, &needle))
        || event
            .attendees
            .iter()
            .any(|attendee| contains(&attendee.email, &needle))
}

fn contains(value: &str, needle: &str) -> bool {
    value.to_lowercase().contains(needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    use bifrost_dav_core::DavResponse;
    use bifrost_dav_core::test_support::{
        dav_script, dav_script_empty, scripted_dav_net, transcripts,
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

        let client = CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&dav_script_empty()),
        );
        let account =
            CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/collection/");
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

    #[tokio::test]
    async fn blank_scheduling_values_refuse_rsvp_before_io() {
        for (address, outbox) in [
            ("  ", "https://dav.example.test/outbox/"),
            ("me@example.test", "  "),
        ] {
            let client = CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&dav_script_empty()),
            );
            let mut account =
                CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");
            account.rsvp_email = Some(address.to_string());
            account.schedule_outbox_url = Some(outbox.to_string());
            let available = scheduling_available(
                account.rsvp_email.as_deref(),
                account.schedule_outbox_url.as_deref(),
            );
            account.capabilities = caldav_capabilities(available);
            assert!(!account.capabilities.pim_methods.event_rsvp);
            let error = account
                .event_rsvp(
                    EventId("https://dav.example.test/cal/one.ics".to_string()),
                    RsvpStatus::Accepted,
                )
                .await
                .expect_err("blank scheduling must refuse locally");
            assert_eq!(
                error.kind(),
                unsupported_error(AccountOperation::EventRsvp).kind()
            );
        }
    }

    /// `set_priority` and `set_bandwidth_cap` reach the account's transport.
    ///
    /// Both were silent no-ops for as long as this crate ran its own reqwest
    /// client: an IMAP-shaped account composed `with_caldav` and given a
    /// bandwidth cap did not cap its DAV legs, and nothing said so. This is the
    /// door dav-B9 exists to open, so it is pinned at the door rather than
    /// inferred from the transport swap.
    #[tokio::test]
    async fn the_priority_and_bandwidth_doors_reach_the_transport() {
        let net = scripted_dav_net(&dav_script_empty());
        let client = CalDavClient::with_account_net("https://dav.example.test", net.clone());
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

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

    /// The property the watermark cursor was bought for: a match-all page
    /// multigets ONLY the hrefs it is about to serve.
    ///
    /// The previous offset shape listed, multiget the WHOLE calendar, and threw
    /// away all but `limit` of the result - so paging a large collection cost
    /// O(collection) of wire traffic per page. The assertion is written against
    /// the REPORT body because that is where the regression would show: a page
    /// that goes back to hydrating everything names `c.ics` in it.
    #[tokio::test]
    async fn a_match_all_event_page_multigets_only_the_page_hrefs() {
        let multistatus = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/cal/")
                .expect("test DAV URL"),
        };
        let event = |name: &str| {
            format!(
                "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag>\
<C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:{name}\nDTSTART:20260101T090000Z\nDTEND:20260101T100000Z\nEND:VEVENT\nEND:VCALENDAR</C:calendar-data>\
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
            )
        };
        let listed = |name: &str| {
            format!(
                "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status>\
</D:propstat></D:response>"
            )
        };
        let listing = format!(
            "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">{}{}{}</D:multistatus>",
            listed("a"),
            listed("b"),
            listed("c")
        );
        let page = format!(
            "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">{}{}</D:multistatus>",
            event("a"),
            event("b")
        );
        // Exactly two responses: a regression that multigets in more than one
        // chunk, or re-lists, starves the script and panics.
        let script = dav_script([multistatus(&listing), multistatus(&page)]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .event_search(EventSearchRequest {
                query: String::new(),
                calendar_id: None,
                limit: Some(2),
                page_cursor: None,
                include_cancelled: false,
            })
            .await
            .expect("match-all page");

        assert_eq!(
            page.items
                .iter()
                .map(|event| event.id.0.clone())
                .collect::<Vec<_>>(),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
            ]
        );
        assert_eq!(
            page.next_cursor,
            Some(b"https://dav.example.test/cal/b.ics".to_vec().into()),
            "the cursor is the last href served"
        );
        assert_eq!(page.estimated_total, Some(3));

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2, "one listing and one multiget");
        assert_eq!(requests[0].method.as_str(), "PROPFIND");
        assert_eq!(requests[1].method.as_str(), "REPORT");
        assert!(
            requests[1].body.contains("/cal/a.ics"),
            "{}",
            requests[1].body
        );
        assert!(
            requests[1].body.contains("/cal/b.ics"),
            "{}",
            requests[1].body
        );
        assert!(
            !requests[1].body.contains("/cal/c.ics"),
            "the multiget must not hydrate a member this page does not serve: {}",
            requests[1].body
        );
    }

    /// The continuation half: a page fetched with a watermark hydrates only the
    /// members after it, and the final page reports no cursor.
    #[tokio::test]
    async fn a_continued_event_page_multigets_only_what_follows_the_watermark() {
        let multistatus = |body: String| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/cal/")
                .expect("test DAV URL"),
        };
        let listing = "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\
<D:response><D:href>/cal/a.ics</D:href><D:propstat><D:prop><D:getetag>\"a\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>\
<D:response><D:href>/cal/b.ics</D:href><D:propstat><D:prop><D:getetag>\"b\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>\
<D:response><D:href>/cal/c.ics</D:href><D:propstat><D:prop><D:getetag>\"c\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>\
</D:multistatus>";
        let tail = "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\
<D:response><D:href>/cal/c.ics</D:href><D:propstat><D:prop><D:getetag>\"c\"</D:getetag>\
<C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:c\nDTSTART:20260101T090000Z\nDTEND:20260101T100000Z\nEND:VEVENT\nEND:VCALENDAR</C:calendar-data>\
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>";
        let script = dav_script([
            multistatus(listing.to_string()),
            multistatus(tail.to_string()),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .event_search(EventSearchRequest {
                query: String::new(),
                calendar_id: None,
                limit: Some(2),
                page_cursor: Some(b"https://dav.example.test/cal/b.ics".to_vec().into()),
                include_cancelled: false,
            })
            .await
            .expect("continued page");

        assert_eq!(
            page.items
                .iter()
                .map(|event| event.id.0.clone())
                .collect::<Vec<_>>(),
            vec!["https://dav.example.test/cal/c.ics".to_string()]
        );
        assert_eq!(page.next_cursor, None, "the last page ends the walk");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert!(
            !requests[1].body.contains("/cal/a.ics") && !requests[1].body.contains("/cal/b.ics"),
            "a member behind the watermark must not be re-hydrated: {}",
            requests[1].body
        );
        assert!(requests[1].body.contains("/cal/c.ics"));
    }

    fn cal_multistatus(body: String) -> DavResponse {
        DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/cal/")
                .expect("test DAV URL"),
        }
    }

    /// A member of a filtered query's answer: href plus etag, no body.
    fn queried(name: &str) -> String {
        format!(
            "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status>\
</D:propstat></D:response>"
        )
    }

    /// A member the server refused inside the 207.
    fn refused(name: &str) -> String {
        format!(
            "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag/></D:prop><D:status>HTTP/1.1 403 Forbidden</D:status>\
</D:propstat></D:response>"
        )
    }

    /// A member the server refused with a named status, so the failure lane's
    /// classification can be read.
    fn refused_with(name: &str, status: u16, reason: &str) -> String {
        format!(
            "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag/></D:prop><D:status>HTTP/1.1 {status} {reason}</D:status>\
</D:propstat></D:response>"
        )
    }

    fn hydrated(name: &str) -> String {
        format!(
            "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag>\
<C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:{name}\nSUMMARY:plan\nDTSTART:20260101T090000Z\nDTEND:20260101T100000Z\nEND:VEVENT\nEND:VCALENDAR</C:calendar-data>\
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        )
    }

    fn wrap(responses: &[String]) -> String {
        format!(
            "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">{}</D:multistatus>",
            responses.concat()
        )
    }

    /// A canned collection listing with a sync-token candidate. Snapshot tests
    /// use it as the depth-0 token response, even though the body includes the
    /// same properties a depth-1 calendar listing might return.
    fn home_listing() -> DavResponse {
        cal_multistatus(wrap(&[
            "<D:response><D:href>/cal/</D:href><D:propstat><D:prop>\
<D:resourcetype><D:collection/><C:calendar/></D:resourcetype>\
<D:displayname>Work</D:displayname></D:prop>\
<D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
                .to_string(),
        ]))
    }

    fn cursor_entry_uris(cursor: &ChangeCursor) -> Vec<String> {
        Kernel::decode_snapshot(cursor)
            .expect("the cursor this account just minted decodes")
            .entries
            .into_iter()
            .map(|entry| entry.uri)
            .collect()
    }

    fn folder_scope() -> CursorScope {
        CursorScope::FolderType {
            folder: FolderId("https://dav.example.test/cal/".to_string()),
            ty: ObjectType::CalendarEvent,
        }
    }

    /// RestartScope must identify the cursor discovery actually advertised,
    /// so an expired token is replaced instead of replayed on every poll.
    #[tokio::test]
    async fn expired_sync_token_restarts_the_discovered_calendar_cursor() {
        use bifrost_types::{EngineDirective, RecoveryClass};

        for (status, spelling) in [StatusCode::FORBIDDEN, StatusCode::GONE]
            .into_iter()
            .flat_map(|status| (0..4).map(move |spelling| (status, spelling)))
        {
            let script = dav_script([
                DavResponse {
                    status,
                    body: "<D:error xmlns:D=\"DAV:\"><D:valid-sync-token/></D:error>".into(),
                    ..cal_multistatus(String::new())
                },
                cal_multistatus(wrap(&[concat!(
                    "<D:response><D:href>/cal/</D:href><D:propstat><D:prop>",
                    "<D:sync-token>fresh</D:sync-token></D:prop>",
                    "<D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>",
                )
                .into()])),
                cal_multistatus(wrap(&[queried("one")])),
            ]);
            let client = CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            );
            let account =
                CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");
            let scopes = account.discover_cursor_scopes().collect::<Vec<_>>().await;
            let SyncEvent::Batch(batch) = &scopes[0] else {
                panic!("expected discovered scopes")
            };
            let scope = match spelling {
                0 => batch.items[0].clone(),
                1 => CursorScope::Folder(FolderId("https://dav.example.test/cal/".into())),
                2 => CursorScope::Type(ObjectType::CalendarEvent),
                _ => batch.items[0].clone(),
            };
            let stale = Kernel::cursor_from_snapshot(
                scope.clone(),
                &EventSnapshot {
                    collection_url: if spelling == 3 {
                        "https://DAV.example.test:443/cal/".into()
                    } else {
                        "https://dav.example.test/cal/".into()
                    },
                    token: Some("expired".into()),
                    ..EventSnapshot::default()
                },
            );
            let events = account.changes_stream(stale).collect::<Vec<_>>().await;
            let SyncEvent::Terminated(error) = &events[0] else {
                panic!("expected expired token: {events:?}")
            };
            let RecoveryClass::Engine(EngineDirective::RestartScope(restart)) = error.recovery()
            else {
                panic!("expected restart: {:?}", error.recovery())
            };
            assert_eq!(
                restart, &scope,
                "restart must match the stored cursor exactly"
            );
            let CursorEstablishment::Ready(fresh) = account
                .establish_initial_cursor(restart.clone())
                .await
                .unwrap()
            else {
                panic!("DAV establishes synchronously")
            };
            assert_eq!(fresh.scope, scope);
            assert_eq!(
                Kernel::decode_snapshot(&fresh).unwrap().token.as_deref(),
                Some("fresh")
            );
            assert_eq!(transcripts(&script).len(), 3);
        }
    }

    /// caldav-F1: a VTODO or VJOURNAL sharing the collection must not occupy
    /// the event cursor.
    ///
    /// The depth-1 PROPFIND carries no component type, so the only way to keep
    /// a task out of the event snapshot is to make the SERVER apply a VEVENT
    /// `comp-filter`. The assertion is written against the request because that
    /// is where a regression shows: a lane that goes back to the bare listing
    /// sends a PROPFIND, and the task resource the server would then name lands
    /// in the snapshot as a created event that hydrates to nothing.
    #[tokio::test]
    async fn the_cursor_listing_asks_the_server_for_vevent_resources_only() {
        let query = wrap(&[queried("a"), queried("b")]);
        let script = dav_script([home_listing(), cal_multistatus(query)]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let established = account
            .establish_initial_cursor(folder_scope())
            .await
            .expect("initial cursor");
        let CursorEstablishment::Ready(cursor) = established else {
            panic!("a CalDAV cursor is established synchronously");
        };

        assert_eq!(
            cursor_entry_uris(&cursor),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
            ]
        );

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2, "one home listing and one event listing");
        assert_eq!(
            requests[1].method.as_str(),
            "REPORT",
            "the cursor listing must be the filtered query, not the bare PROPFIND"
        );
        assert!(
            requests[1].body.contains("<C:comp-filter name=\"VEVENT\">"),
            "the component filter must reach the server: {}",
            requests[1].body
        );
        assert!(
            !requests[1].body.contains("<C:time-range"),
            "the cursor listing is unbounded in time: {}",
            requests[1].body
        );
        assert!(
            !requests[1].body.contains("<C:calendar-data/>"),
            "the cursor listing must not hydrate: {}",
            requests[1].body
        );
    }

    /// The other half: a server that will not run the filter must not lose its
    /// cursor. The lane degrades to the unfiltered depth-1 PROPFIND, which is
    /// what it did unconditionally before, and mints the same snapshot.
    ///
    /// Without the degrade a store with no `calendar-query` support would fail
    /// every `establish_initial_cursor` outright - strictly worse than carrying
    /// the odd task resource.
    #[tokio::test]
    async fn the_cursor_listing_degrades_to_the_propfind_when_the_filter_is_refused() {
        let refusal = DavResponse {
            status: StatusCode::METHOD_NOT_ALLOWED,
            headers: HeaderMap::new(),
            body: String::new(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/cal/")
                .expect("test DAV URL"),
        };
        let listing = wrap(&[queried("a"), queried("b")]);
        let script = dav_script([home_listing(), refusal, cal_multistatus(listing)]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let established = account
            .establish_initial_cursor(folder_scope())
            .await
            .expect("a refused filter degrades rather than failing the cursor");
        let CursorEstablishment::Ready(cursor) = established else {
            panic!("a CalDAV cursor is established synchronously");
        };

        assert_eq!(
            cursor_entry_uris(&cursor),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
            ]
        );

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].method.as_str(), "REPORT");
        assert_eq!(
            requests[2].method.as_str(),
            "PROPFIND",
            "the degrade lane is the unfiltered collection listing"
        );
    }

    fn event_ids(page: &Page<CalendarEvent>) -> Vec<String> {
        page.items.iter().map(|event| event.id.0.clone()).collect()
    }

    fn range_over_2026() -> EventRange {
        EventRange {
            calendar_id: CalendarId("https://dav.example.test/cal/".to_string()),
            start: time("2026-01-01T00:00:00Z"),
            end: time("2027-01-01T00:00:00Z"),
            limit: Some(2),
            page_cursor: None,
        }
    }

    /// The dav-B8 residual, closed: `events_in_range` pushes the time-range
    /// filter to the server and hydrates only the page it is about to serve.
    ///
    /// Before this the REPORT asked for `calendar-data` and the whole matching
    /// result set arrived on every page. The assertions are written against the
    /// request bodies because that is where the regression shows: a query that
    /// goes back to hydrating names `calendar-data`, and a page that goes back
    /// to slicing after hydration names `c.ics` in its multiget.
    #[tokio::test]
    async fn a_range_page_filters_on_the_server_and_multigets_only_the_page() {
        let query = wrap(&[queried("a"), queried("b"), queried("c"), refused("d")]);
        let page = wrap(&[hydrated("a"), hydrated("b")]);
        // Exactly two responses: a regression that lists, or multigets twice,
        // starves the script and panics.
        let script = dav_script([cal_multistatus(query), cal_multistatus(page)]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .events_in_range(range_over_2026())
            .await
            .expect("range page");

        assert_eq!(
            event_ids(&page),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
            ]
        );
        assert_eq!(
            page.next_cursor,
            Some(b"https://dav.example.test/cal/b.ics".to_vec().into()),
            "the cursor is the last href served"
        );
        assert_eq!(page.estimated_total, Some(3));
        assert_eq!(
            page.failed_ids,
            vec!["https://dav.example.test/cal/d.ics".to_string()],
            "a member the query refused is reported on the page it was observed on"
        );

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2, "one query and one multiget");
        assert_eq!(requests[0].method.as_str(), "REPORT");
        assert!(
            requests[0].body.contains("<C:time-range"),
            "the filter must reach the server: {}",
            requests[0].body
        );
        assert!(
            !requests[0].body.contains("<C:calendar-data/>"),
            "the filtered query must not hydrate: {}",
            requests[0].body
        );
        assert!(requests[1].body.contains("/cal/a.ics"));
        assert!(requests[1].body.contains("/cal/b.ics"));
        assert!(
            !requests[1].body.contains("/cal/c.ics"),
            "the multiget must not hydrate a member this page does not serve: {}",
            requests[1].body
        );
    }

    /// Builds a 300-member collection and a script answering: one range query
    /// naming all of it, then one multiget answer per 50-member chunk of the
    /// first `served` members. The collection is deliberately larger than the
    /// 250-member default page, so a fixture that fits inside that bound cannot
    /// make the default tests pass either way.
    fn large_collection_script(names: &[String], served: usize) -> Vec<DavResponse> {
        let query = wrap(&names.iter().map(|name| queried(name)).collect::<Vec<_>>());
        let mut responses = vec![cal_multistatus(query)];
        // One scripted answer per multiget chunk, each hydrating its own share
        // of the page, so the merged page is the same set however the
        // bounded-concurrency dispatch pairs legs with answers.
        for chunk in names[..served].chunks(50) {
            responses.push(cal_multistatus(wrap(
                &chunk.iter().map(|name| hydrated(name)).collect::<Vec<_>>(),
            )));
        }
        responses
    }

    fn large_range(
        limit: Option<u32>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> EventRange {
        EventRange {
            calendar_id: CalendarId("https://dav.example.test/cal/".to_string()),
            start: time("2026-01-01T00:00:00Z"),
            end: time("2027-01-01T00:00:00Z"),
            limit,
            page_cursor,
        }
    }

    /// The bounded default, pinned by what a consumer observes rather than by
    /// the constant's value: a `limit: None` range serves 250 of 300
    /// candidates, names a watermark, and the continuation serves the rest.
    /// It fails against the former unbounded default, which served all 300 in
    /// one continuation-less page.
    #[tokio::test]
    async fn an_omitted_range_limit_serves_a_bounded_page_and_continues() {
        const MEMBERS: usize = 300;
        let names = (0..MEMBERS)
            .map(|index| format!("e{index:03}"))
            .collect::<Vec<_>>();
        let mut responses = large_collection_script(&names, EVENT_PAGE_SIZE);
        // The continuation re-queries the whole collection and multigets the
        // 50-member tail after the watermark.
        responses.push(cal_multistatus(wrap(
            &names.iter().map(|name| queried(name)).collect::<Vec<_>>(),
        )));
        responses.push(cal_multistatus(wrap(
            &names[EVENT_PAGE_SIZE..]
                .iter()
                .map(|name| hydrated(name))
                .collect::<Vec<_>>(),
        )));
        let script = dav_script(responses);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let first = account
            .events_in_range(large_range(None, None))
            .await
            .expect("an omitted limit is one bounded page");

        assert_eq!(first.items.len(), EVENT_PAGE_SIZE);
        assert!(
            first.next_cursor.is_some(),
            "a bounded page with candidates behind it must continue"
        );
        assert_eq!(first.estimated_total, Some(300));
        assert_eq!(
            transcripts(&script).len(),
            6,
            "one query and five multiget chunks: only the page is hydrated"
        );

        let second = account
            .events_in_range(large_range(None, first.next_cursor))
            .await
            .expect("the continuation serves the remainder");

        assert_eq!(second.items.len(), MEMBERS - EVENT_PAGE_SIZE);
        assert_eq!(second.next_cursor, None);
    }

    /// An explicit large `limit` still serves the whole collection in one
    /// continuation-less page.
    #[tokio::test]
    async fn an_explicit_large_range_limit_still_serves_the_whole_collection() {
        const MEMBERS: usize = 300;
        let names = (0..MEMBERS)
            .map(|index| format!("e{index:03}"))
            .collect::<Vec<_>>();
        let script = dav_script(large_collection_script(&names, MEMBERS));
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .events_in_range(large_range(Some(10_000), None))
            .await
            .expect("an explicit limit is honoured");

        assert_eq!(page.items.len(), MEMBERS);
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.estimated_total, Some(300));
        assert_eq!(
            transcripts(&script).len(),
            7,
            "one query and six multiget chunks: the whole collection, hydrated"
        );
    }

    /// `event_search` shares the bounded default: an empty-query match-all
    /// with no `limit` lists 300 members and multigets only the first 250.
    #[tokio::test]
    async fn an_omitted_search_limit_serves_a_bounded_page() {
        const MEMBERS: usize = 300;
        let names = (0..MEMBERS)
            .map(|index| format!("e{index:03}"))
            .collect::<Vec<_>>();
        let listing = wrap(&names.iter().map(|name| queried(name)).collect::<Vec<_>>());
        let mut responses = vec![cal_multistatus(listing)];
        for chunk in names[..EVENT_PAGE_SIZE].chunks(50) {
            responses.push(cal_multistatus(wrap(
                &chunk.iter().map(|name| hydrated(name)).collect::<Vec<_>>(),
            )));
        }
        let script = dav_script(responses);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .event_search(EventSearchRequest::new(""))
            .await
            .expect("an omitted limit is one bounded page");

        assert_eq!(page.items.len(), EVENT_PAGE_SIZE);
        assert!(page.next_cursor.is_some());
        assert_eq!(page.estimated_total, Some(300));
    }

    fn status_event(name: &str, status_line: &str) -> String {
        format!(
            "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag>\
<C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:{name}\nDTSTART:20260101T090000Z\nDTEND:20260101T100000Z\n{status_line}END:VEVENT\nEND:VCALENDAR</C:calendar-data>\
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        )
    }

    /// Match-all search over three resources: a cancelled one, one with no
    /// STATUS at all, and another cancelled one.
    async fn search_cancelled_fixture(include_cancelled: bool) -> Vec<String> {
        let listed = |name: &str| {
            format!(
                "<D:response><D:href>/cal/{name}.ics</D:href><D:propstat><D:prop>\
<D:getetag>\"{name}\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status>\
</D:propstat></D:response>"
            )
        };
        let envelope = |body: String| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: format!(
                "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">{body}</D:multistatus>"
            ),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/cal/")
                .expect("test DAV URL"),
        };
        let script = dav_script([
            envelope(format!("{}{}{}", listed("a"), listed("b"), listed("c"))),
            envelope(format!(
                "{}{}{}",
                status_event("a", "STATUS:CANCELLED\n"),
                status_event("b", ""),
                status_event("c", "STATUS:CANCELLED\n"),
            )),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");
        let mut request = EventSearchRequest::new("");
        request.include_cancelled = include_cancelled;
        let page = account.event_search(request).await.expect("search loads");
        page.items.into_iter().map(|event| event.id.0).collect()
    }

    #[tokio::test]
    async fn search_drops_cancelled_events_by_default() {
        assert_eq!(
            search_cancelled_fixture(false).await,
            vec!["https://dav.example.test/cal/b.ics".to_string()]
        );
    }

    #[tokio::test]
    async fn search_keeps_cancelled_events_when_included() {
        assert_eq!(
            search_cancelled_fixture(true).await,
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
                "https://dav.example.test/cal/c.ics".to_string(),
            ]
        );
    }

    /// A server that will not run the filter must not fail the call. The lane
    /// degrades to the whole-collection listing, still hydrating only the page,
    /// and answers the same events for the same cursor.
    #[tokio::test]
    async fn a_range_page_degrades_to_the_listing_when_the_filter_is_refused() {
        let refusal = DavResponse {
            status: StatusCode::FORBIDDEN,
            headers: HeaderMap::new(),
            body: "<D:error xmlns:D=\"DAV:\"><C:supported-filter/></D:error>".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/cal/")
                .expect("test DAV URL"),
        };
        let listing = wrap(&[queried("a"), queried("b"), queried("c")]);
        let page = wrap(&[hydrated("a"), hydrated("b")]);
        let script = dav_script([refusal, cal_multistatus(listing), cal_multistatus(page)]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .events_in_range(range_over_2026())
            .await
            .expect("a refused filter degrades rather than failing");

        assert_eq!(
            event_ids(&page),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
            ]
        );
        assert_eq!(
            page.next_cursor,
            Some(b"https://dav.example.test/cal/b.ics".to_vec().into()),
            "the degrade lane keys the cursor on the href too, so a mid-walk \
             degrade neither re-serves nor skips"
        );

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method.as_str(), "REPORT");
        assert_eq!(requests[1].method.as_str(), "PROPFIND");
        assert!(
            !requests[2].body.contains("/cal/c.ics"),
            "the degrade lane still hydrates only the page: {}",
            requests[2].body
        );
    }

    /// A candidate 207 in which EVERY response failed is a complete failure,
    /// not an empty page.
    ///
    /// This was the accepted loss of the server-side-filter round: the
    /// candidate lane is read by the listing parser, whose failure lane carried
    /// hrefs and no statuses, so an all-refused query came back as an empty
    /// candidate set plus `failed_ids` and a consumer recorded a completed walk
    /// over a collection it had been refused. The listing lane now carries the
    /// per-member status and runs the same RFC 4918 s13 ladder the multiget
    /// lanes use, so the recovery class is reachable: a 507 is a quota
    /// condition on the request, not news about four resources.
    ///
    /// The script holds ONE response: a lane that went on to multiget the empty
    /// page would starve it and panic.
    #[tokio::test]
    async fn an_all_refused_query_207_classifies_rather_than_serving_an_empty_page() {
        let query = wrap(&[
            refused_with("a", 507, "Insufficient Storage"),
            refused_with("b", 507, "Insufficient Storage"),
        ]);
        let script = dav_script([cal_multistatus(query)]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let error = account
            .events_in_range(range_over_2026())
            .await
            .expect_err("an all-refused candidate 207 is a classified failure");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            "the member status drives the classification"
        );
    }

    /// The other half of the same rule, and the one that guards against
    /// over-reach: a member refused BESIDE members that answered stays a
    /// per-id failure on `Page::failed_ids`, and the page is served. A 403 is
    /// the sharpest case, because alone it would classify as `NoPermission`.
    #[tokio::test]
    async fn a_partly_refused_query_207_still_serves_the_page() {
        let query = wrap(&[queried("a"), refused("b"), queried("c")]);
        let script = dav_script([
            cal_multistatus(query),
            cal_multistatus(wrap(&[hydrated("a"), hydrated("c")])),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .events_in_range(range_over_2026())
            .await
            .expect("a partially refused 207 is still a page");

        assert_eq!(
            event_ids(&page),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/c.ics".to_string(),
            ]
        );
        assert_eq!(
            page.failed_ids,
            vec!["https://dav.example.test/cal/b.ics".to_string()],
            "the refused member is per-id news, not a page failure"
        );
    }

    /// The text lane's four per-property REPORTs name hrefs, the union is
    /// deduped, and only the page is hydrated. The local match stays the
    /// authority over what the page finally carries.
    #[tokio::test]
    async fn a_text_search_page_filters_on_the_server_and_multigets_only_the_page() {
        let query = wrap(&[queried("a"), queried("b"), queried("c")]);
        // Every leg answers the same three resources, so the union is only
        // right if the candidates are deduped.
        let script = dav_script([
            cal_multistatus(query.clone()),
            cal_multistatus(query.clone()),
            cal_multistatus(query.clone()),
            cal_multistatus(query),
            cal_multistatus(wrap(&[hydrated("a"), hydrated("b")])),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .event_search(EventSearchRequest {
                query: "plan".to_string(),
                calendar_id: None,
                limit: Some(2),
                page_cursor: None,
                include_cancelled: false,
            })
            .await
            .expect("text search page");

        assert_eq!(
            event_ids(&page),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
            ]
        );
        assert_eq!(
            page.estimated_total,
            Some(3),
            "three candidates, not twelve"
        );

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 5, "four query legs and one multiget");
        for request in &requests[..4] {
            assert!(
                request.body.contains("<C:text-match"),
                "the filter must reach the server: {}",
                request.body
            );
            assert!(
                !request.body.contains("<C:calendar-data/>"),
                "the filtered query must not hydrate: {}",
                request.body
            );
        }
        assert!(
            !requests[4].body.contains("/cal/c.ics"),
            "the multiget must not hydrate a member this page does not serve: {}",
            requests[4].body
        );
    }

    /// One leg reporting an unsupported filter degrades the WHOLE lane. Serving
    /// out of the properties a server happened to accept would narrow the
    /// search silently.
    #[tokio::test]
    async fn a_text_search_degrades_when_one_query_leg_refuses_the_filter() {
        let query = wrap(&[queried("a"), queried("b")]);
        let refusal = DavResponse {
            status: StatusCode::BAD_REQUEST,
            headers: HeaderMap::new(),
            body: String::new(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/cal/")
                .expect("test DAV URL"),
        };
        let script = dav_script([
            refusal,
            cal_multistatus(query.clone()),
            cal_multistatus(query.clone()),
            cal_multistatus(query),
            cal_multistatus(wrap(&[queried("a"), queried("b"), queried("c")])),
            cal_multistatus(wrap(&[hydrated("a"), hydrated("b")])),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        let page = account
            .event_search(EventSearchRequest {
                query: "plan".to_string(),
                calendar_id: None,
                limit: Some(2),
                page_cursor: None,
                include_cancelled: false,
            })
            .await
            .expect("a refused filter degrades rather than failing");

        assert_eq!(
            event_ids(&page),
            vec![
                "https://dav.example.test/cal/a.ics".to_string(),
                "https://dav.example.test/cal/b.ics".to_string(),
            ]
        );

        let requests = transcripts(&script);
        assert_eq!(
            requests.len(),
            6,
            "four legs, then the listing and the page"
        );
        assert_eq!(requests[4].method.as_str(), "PROPFIND");
    }

    fn collection(href: &str) -> crate::parse::CalendarCollection {
        crate::parse::CalendarCollection {
            href: href.to_string(),
            display_name: None,
            color: None,
            can_edit: None,
            sync_token: None,
        }
    }

    /// An empty calendar home yields NO cursor scopes.
    ///
    /// The collection walk returns the home itself when the home is a calendar
    /// collection, so an empty walk is an empty backend. Substituting the home
    /// would advertise a folder that does not exist and drive every cursor and
    /// inventory request for it into a 404.
    #[test]
    fn an_empty_home_produces_no_cursor_scope_collections() {
        assert!(discovered_collection_urls(&[]).is_empty());
    }

    /// An empty home leaves the account with NO default calendar, rather than
    /// the calendar home standing in for one.
    ///
    /// This is the `open`-side half of
    /// `an_empty_backend_refuses_collection_less_calls_before_the_wire`, which
    /// pins what a `None` default does but constructs it directly. Without this
    /// assertion, restoring the home fallback here would leave that test
    /// passing. Twin of the CardDAV assertion; keep them in step.
    #[test]
    fn an_empty_home_leaves_no_default_calendar() {
        assert_eq!(default_collection_url(&[]), None);
        assert_eq!(
            default_collection_url(&[
                collection("https://dav.example.test/cal/work/"),
                collection("https://dav.example.test/cal/personal/"),
            ])
            .as_deref(),
            Some("https://dav.example.test/cal/work/")
        );
    }

    #[test]
    fn every_discovered_calendar_becomes_a_cursor_scope_collection() {
        assert_eq!(
            discovered_collection_urls(&[
                collection("https://dav.example.test/cal/work/"),
                collection("https://dav.example.test/cal/home/"),
            ]),
            vec![
                "https://dav.example.test/cal/work/".to_string(),
                "https://dav.example.test/cal/home/".to_string(),
            ]
        );
    }

    /// A CalDAV `CalendarId` IS the resolved collection href, on the listing
    /// surface as well as on the request-routing and `ErrorScope` surfaces.
    ///
    /// This is the agreement a consumer relies on when it correlates an
    /// `ErrorScope::Calendar` against ids from `list_calendars`. It holds
    /// because the XML decode boundary rebases every href against its request
    /// URI, which makes the account layer's `resolve_url` a no-op on an id
    /// that came out of the listing. Pinned rather than merely written down:
    /// if either side stopped absolutizing, the two ids would still both be
    /// `CalendarId` and the mismatch would be silent.
    #[test]
    fn a_listed_calendar_id_is_the_absolute_href_error_scopes_carry() {
        let client = CalDavClient::with_account_net(
            "https://dav.example.com/dav/",
            scripted_dav_net(&dav_script_empty()),
        );
        let mut collection = CalendarCollection {
            href: "work/".to_string(),
            display_name: Some("Work".to_string()),
            color: None,
            can_edit: Some(true),
            sync_token: None,
        };
        collection.resolve_href("https://dav.example.com/dav/calendars/");

        let calendar = CalDavAccount::map_calendar(collection);

        assert_eq!(calendar.id.0, "https://dav.example.com/dav/calendars/work/");
        assert_eq!(calendar.native_id, calendar.id.0);
        assert_eq!(
            client.resolve_url(&calendar.id.0),
            calendar.id.0,
            "the routing/ErrorScope side must not rewrite a listed calendar id"
        );
    }

    #[test]
    fn a_materialized_resource_leaves_the_failure_lane() {
        let mut failed = vec![
            "/cal/one.ics".to_string(),
            "/cal/two.ics".to_string(),
            "/cal/two.ics".to_string(),
        ];
        let materialized = HashSet::from(["/cal/one.ics".to_string()]);

        bifrost_dav_core::collection_account::one_outcome_per_id(
            &mut failed,
            materialized.iter().map(String::as_str),
        );

        assert_eq!(failed, vec!["/cal/two.ics".to_string()]);
    }

    #[test]
    fn a_degraded_leg_becomes_a_skipped_calendar_scope() {
        let error = crate::client::status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::EventSearch,
            reqwest::StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );
        let recovery = error.recovery().clone();

        let skipped = skipped_calendar_scope("https://dav.example.test/cal/", Some(error));

        assert_eq!(skipped.len(), 1);
        assert_eq!(
            skipped[0].scope,
            ErrorScope::Calendar {
                id: ("https://dav.example.test/cal/".to_string()).into(),
            }
        );
        assert_eq!(skipped[0].error.recovery(), &recovery);
        assert!(skipped_calendar_scope("https://dav.example.test/cal/", None).is_empty());
    }

    fn time(value: &str) -> EventTime {
        EventTime::from_observed(value.to_string(), None)
    }

    fn event(start: &str, end: &str, expected_all_day: bool) -> CalendarEvent {
        assert_eq!(time(start).is_all_day(), expected_all_day);
        CalendarEvent {
            id: EventId("/cal/one.ics".to_string()),
            calendar_id: CalendarId("/cal/".to_string()),
            native_id: "/cal/one.ics".to_string(),
            uid: None,
            etag: None,
            provenance: CalendarProvenance {
                provider: ProtocolKind::CalDav,
                native: "/cal/one.ics".to_string(),
                calendar_native: None,
            },
            title: None,
            description: None,
            location: None,
            start: time(start),
            end: Some(time(end)).filter(|end| !end.value().is_empty()),
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            self_response: RsvpStatus::Unknown,
            organizer: None,
            attendees: Vec::new(),
            reminders: Vec::new(),
            recurrence: EventRecurrence::default(),
            html_link: None,
            raw_ical: None,
        }
    }

    fn recurring_event(start: &str, end: &str, rrule: &str) -> CalendarEvent {
        let mut event = event(start, end, false);
        event.recurrence.rrule = Some(rrule.to_string());
        event
    }

    #[tokio::test]
    async fn caldav_host_attachment_unsupported() {
        // CalDAV has no cloud-drive hosting; the flag is false (Default) and
        // the leg returns `Unsupported(HostAttachment)`.
        assert!(!caldav_capabilities(true).pim_methods.host_attachment);

        let err = Kernel::unsupported_future::<HostedAttachment>(AccountOperation::HostAttachment)
            .await
            .expect_err("caldav host_attachment is unsupported");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[tokio::test]
    async fn caldav_open_raw_rfc822_unsupported() {
        // CalDAV advertises no raw RFC822 read; the flag is false and the
        // stream's first item terminates `Unsupported(OpenRawRfc822)`.
        assert!(!caldav_capabilities(true).pim_methods.open_raw_rfc822);

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
    fn range_filter_includes_all_day_on_window_start() {
        let event = event("2026-06-02", "2026-06-03", true);

        assert!(
            EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-02T00:00:00Z"),
                time("2026-06-03T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn range_filter_keeps_all_day_dates_when_the_window_has_an_offset() {
        let event = event("2026-06-02", "2026-06-03", true);

        assert!(
            EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-01T23:30:00+02:00"),
                time("2026-06-02T01:00:00+02:00"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn range_filter_includes_event_that_started_before_window() {
        let event = event("2026-06-01T23:00:00Z", "2026-06-02T01:00:00Z", false);

        assert!(
            EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-02T00:00:00Z"),
                time("2026-06-02T23:59:59Z"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn range_filter_excludes_event_after_window() {
        let event = event("2026-06-04T00:00:00Z", "2026-06-04T01:00:00Z", false);

        assert!(
            !EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-02T00:00:00Z"),
                time("2026-06-03T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    /// RFC 4791 s9.9: a zero-length instant on the window's start is inside
    /// the window (`start <= DTSTART && end > DTSTART`). Ablation: the
    /// duration rule alone (`event_end > range_start`) drops it.
    #[test]
    fn range_filter_includes_a_zero_length_event_on_the_window_start() {
        let event = event("2026-06-02T00:00:00Z", "2026-06-02T00:00:00Z", false);

        assert!(
            EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-02T00:00:00Z"),
                time("2026-06-03T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    /// The same instant on the window's END is outside it: the window is
    /// half-open, and an event with duration ending there is excluded too.
    #[test]
    fn range_filter_excludes_a_zero_length_event_on_the_window_end() {
        let event = event("2026-06-03T00:00:00Z", "2026-06-03T00:00:00Z", false);

        assert!(
            !EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-02T00:00:00Z"),
                time("2026-06-03T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    /// And one instant before the window starts is outside it, so the
    /// inclusive rule is a boundary rule and not a widening.
    #[test]
    fn range_filter_excludes_a_zero_length_event_just_before_the_window() {
        let event = event("2026-06-01T23:59:59Z", "2026-06-01T23:59:59Z", false);

        assert!(
            !EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-02T00:00:00Z"),
                time("2026-06-03T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn range_filter_excludes_event_ending_at_window_start() {
        // Timed, not all-day: all-day DATE events are deliberately kept by the
        // local guard (see `range_filter_keeps_all_day_dates_when_the_window_
        // has_an_offset`), so the half-open end boundary is pinned with instants.
        let event = event("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", false);

        assert!(
            !EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-02T00:00:00Z"),
                time("2026-06-03T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn rsvp_second_leg_error_records_acknowledged_first_leg() {
        let error = crate::client::transport_error(AccountOperation::EventRsvp, "put failed");
        let expected = error.leg_recovery().clone();
        let decorated = rsvp_error(error, &EventId("event".into()), true);
        assert_eq!(decorated.leg_recovery(), &expected);
        assert_eq!(
            decorated.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::Submitted)
        );
        assert!(
            matches!(decorated.recovery(), RecoveryClass::Reconcile(advice) if advice.reason == ReconcileReason::PartialCommit)
        );
    }

    /// A patch that cannot be spliced is classified by cause. Every cause was
    /// `Unsupported`; against that code the invariant assertions fail. Only a
    /// recurrence replacement over overrides is an operation this client
    /// lacks; the other two cannot happen to an event projected from its own
    /// body, so they report a bug here rather than blaming the caller or the
    /// provider.
    #[test]
    fn an_unspliceable_patch_is_classified_by_cause() {
        let refused = patch_error(
            AccountOperation::EventUpdate,
            PatchError::RecurrenceOverride,
        );
        assert_eq!(
            refused.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        );

        for cause in [
            PatchError::NoSpliceableVevent,
            PatchError::MissingSourceBody,
        ] {
            let invariant = patch_error(AccountOperation::EventUpdate, cause);
            assert_eq!(
                invariant.kind(),
                &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
                "{cause:?}"
            );
            assert_eq!(
                invariant.recovery(),
                &RecoveryClass::InternalFailure,
                "{cause:?}"
            );
            assert_eq!(invariant.protocol(), Some(Protocol::CalDav), "{cause:?}");
        }
    }

    #[test]
    fn range_filter_retains_recurring_master_before_window() {
        // A weekly master whose own interval sits a month before the
        // window still has in-window occurrences; the recurrence-aware
        // guard must keep it (the server's time-range REPORT already
        // returned it).
        let event = recurring_event(
            "2026-05-04T12:00:00Z",
            "2026-05-04T13:00:00Z",
            "FREQ=WEEKLY",
        );

        assert!(
            EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-01T00:00:00Z"),
                time("2026-06-08T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn range_filter_retains_bounded_master_for_provider_recurrence_decision() {
        // UNTIL is not an event end, and explicit dates can extend the series.
        // The provider owns recurrence matching.
        let event = recurring_event(
            "2026-05-04T12:00:00Z",
            "2026-05-04T13:00:00Z",
            "FREQ=WEEKLY;UNTIL=20260525T120000Z",
        );

        assert!(
            EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-01T00:00:00Z"),
                time("2026-06-08T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn range_filter_retains_master_for_provider_recurrence_decision() {
        let event = recurring_event(
            "2026-07-04T12:00:00Z",
            "2026-07-04T13:00:00Z",
            "FREQ=WEEKLY",
        );

        assert!(
            EventRange::new(
                event.calendar_id.clone(),
                time("2026-06-01T00:00:00Z"),
                time("2026-06-08T00:00:00Z"),
            )
            .admits(&event)
        );
    }

    #[test]
    fn caldav_query_time_formats_rfc3339_as_utc_basic() {
        assert_eq!(
            caldav_query_time(&time("2026-06-02T02:30:00+02:00")).as_deref(),
            Some("20260602T003000Z")
        );
    }

    #[test]
    fn caldav_query_time_formats_dates_as_utc_midnight() {
        assert_eq!(
            caldav_query_time(&time("2026-06-02")).as_deref(),
            Some("20260602T000000Z")
        );
    }

    #[test]
    fn event_calendar_url_is_the_resource_parent() {
        assert_eq!(
            event_calendar_url("https://dav.example.test/calendars/work/one.ics").as_deref(),
            Some("https://dav.example.test/calendars/work/")
        );
        // A slash in the query or fragment is not a path separator; a raw
        // final-slash search over the string returns a query fragment here.
        assert_eq!(
            event_calendar_url("https://dav.example.test/calendars/work/one.ics?redirect=/foo")
                .as_deref(),
            Some("https://dav.example.test/calendars/work/")
        );
        assert_eq!(
            event_calendar_url("https://dav.example.test/calendars/work/one.ics#a/b").as_deref(),
            Some("https://dav.example.test/calendars/work/")
        );
        assert_eq!(event_calendar_url("not-a-url"), None);
    }

    #[test]
    fn invalid_query_range_is_rejected_before_report_construction() {
        let error = caldav_query_range(&time("not-a-time"), &time("2026-06-03T00:00:00Z"))
            .expect_err("invalid start is rejected");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
        assert_eq!(error.operation(), Some(AccountOperation::EventsInRange));
    }

    #[test]
    fn event_search_matches_unicode_case_folded_text() {
        let mut event = event("2026-06-02T00:00:00Z", "2026-06-02T01:00:00Z", false);
        event.title = Some("A-ring workshop".to_string());
        event.location = Some("\u{00c5}RSTAD".to_string());

        assert!(event_matches(&event, "\u{00e5}"));
        assert!(event_matches(&event, "\u{00e5}r"));
    }

    #[test]
    fn a_cursor_collection_must_match_its_folder_scope() {
        let snapshot = EventSnapshot {
            collection_url: "https://dav.example.test/c/a%40b/".to_string(),
            ..EventSnapshot::default()
        };
        let equivalent = Kernel::cursor_from_snapshot(
            CursorScope::Folder(FolderId("https://dav.example.test/c/a@b/".to_string())),
            &snapshot,
        );
        assert!(Kernel::decode_snapshot(&equivalent).is_ok());
        let mismatched = Kernel::cursor_from_snapshot(
            CursorScope::Folder(FolderId("https://dav.example.test/c/other/".to_string())),
            &snapshot,
        );
        assert!(Kernel::decode_snapshot(&mismatched).is_err());
    }

    #[test]
    fn a_failed_poll_keeps_the_token_needed_to_retry_the_member() {
        let mut previous = event_snapshot_with(&["a", "b"]);
        previous.token = Some("old".to_string());
        let mut current = event_snapshot_with(&["a"]);
        current.token = Some("new".to_string());
        current.failed_hrefs = vec!["b".to_string()];
        current.preserve_unobserved(&previous);
        assert_eq!(current.token, previous.token);
        assert_eq!(current.entries, previous.entries);
    }

    #[test]
    fn event_cursor_snapshot_round_trips() {
        let snapshot = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: Some("token-1".to_string()),
            entries: vec![
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/a.ics".to_string(),
                    etag: Some("a".into()),
                },
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/b.ics".to_string(),
                    etag: None,
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let cursor =
            Kernel::cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);

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
    fn event_cursor_rejects_the_base_relative_id_version() {
        let snapshot = EventSnapshot::default();
        let mut cursor =
            Kernel::cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);
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
    fn event_cursor_rejects_impossible_count_before_allocating() {
        let snapshot = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: None,
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };
        let mut cursor =
            Kernel::cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);
        let count_offset = cursor.server_state.bytes.len() - 4;
        cursor.server_state.bytes[count_offset..].copy_from_slice(&u32::MAX.to_be_bytes());

        assert!(Kernel::decode_snapshot(&cursor).is_err());
    }

    #[test]
    fn event_snapshot_diff_classifies_object_changes() {
        let previous = EventSnapshot {
            collection_url: "cal".to_string(),
            token: None,
            entries: vec![
                EventSnapshotEntry {
                    uri: "a.ics".to_string(),
                    etag: Some("old".into()),
                },
                EventSnapshotEntry {
                    uri: "b.ics".to_string(),
                    etag: Some("same".into()),
                },
                EventSnapshotEntry {
                    uri: "d.ics".to_string(),
                    etag: Some("gone".into()),
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let current = EventSnapshot {
            collection_url: "cal".to_string(),
            token: None,
            entries: vec![
                EventSnapshotEntry {
                    uri: "a.ics".to_string(),
                    etag: Some("new".into()),
                },
                EventSnapshotEntry {
                    uri: "b.ics".to_string(),
                    etag: Some("same".into()),
                },
                EventSnapshotEntry {
                    uri: "c.ics".to_string(),
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
                ("a.ics".to_string(), ObjectChangeKind::Updated),
                ("c.ics".to_string(), ObjectChangeKind::Created),
                ("d.ics".to_string(), ObjectChangeKind::Destroyed),
            ]
        );
    }

    fn event_snapshot_with(entries: &[&str]) -> EventSnapshot {
        EventSnapshot {
            collection_url: "cal".to_string(),
            token: None,
            entries: entries
                .iter()
                .map(|uri| EventSnapshotEntry {
                    uri: (*uri).to_string(),
                    etag: Some("e".into()),
                })
                .collect(),
            failed_hrefs: Vec::new(),
        }
    }

    #[test]
    fn diff_event_snapshots_suppresses_empty_207_mass_delete() {
        // Brick 6: a populated previous diffed against an empty current
        // yields zero changes (no Destroyed).
        let previous = event_snapshot_with(&["a.ics", "b.ics"]);
        let empty = event_snapshot_with(&[]);
        assert!(empty.changes_from(&previous).is_empty());
        assert!(previous.changes_from(&previous).is_empty());
        assert!(empty.changes_from(&empty).is_empty());
    }

    #[test]
    fn empty_poll_checkpoint_preserves_snapshot_for_the_recovery_poll() {
        let previous = event_snapshot_with(&["a.ics", "b.ics"]);
        let mut empty = event_snapshot_with(&[]);
        empty.token = Some("refreshed".to_string());

        empty.preserve_unobserved(&previous);
        assert_eq!(empty.entries.len(), 2);
        assert_eq!(
            empty.token, None,
            "an empty observation must retry the listing"
        );

        let recovered = event_snapshot_with(&["a.ics", "b.ics"]);
        assert!(recovered.changes_from(&empty).is_empty());
    }

    /// Moved down one level with the lane itself: every paging lane now slices
    /// candidate HREFS before hydrating anything, so these pin the key the
    /// cursor actually carries.
    #[test]
    fn event_page_emits_a_cursor_for_truncated_results() {
        let first = href_page(&["a", "b", "c"], None, 2);
        assert_eq!(first.items, vec!["a", "b"]);
        assert_eq!(first.next_watermark.as_deref(), Some("b"));

        let second = href_page(&["a", "b", "c"], Some("b"), 2);
        assert_eq!(second.items, vec!["c"]);
        assert_eq!(second.next_watermark, None);
    }

    /// The page cursor is the last href served, so a decoded cursor and the
    /// resource it names are the same bytes in both directions.
    #[test]
    fn the_event_page_cursor_round_trips_as_the_last_id_served() {
        let slice = href_page(&["a", "b", "c"], None, 2);
        let cursor = encode_watermark_cursor(&slice.next_watermark.expect("page one truncates"));
        assert_eq!(
            Kernel::decode_page_cursor(Some(cursor), AccountOperation::EventsInRange)
                .expect("valid cursor")
                .as_deref(),
            Some("b")
        );
        assert!(
            Kernel::decode_page_cursor(Some(Vec::new().into()), AccountOperation::EventsInRange)
                .is_err(),
            "an empty cursor is corrupt, not a restart from the first event"
        );
    }

    /// The property the watermark buys over an integer offset: an event
    /// inserted BEFORE the watermark between two pages does not push the
    /// unserved remainder behind the cursor. Against an offset, `aa` arriving
    /// between the two calls makes page two return `b` again and `c` is never
    /// delivered at all.
    #[test]
    fn an_event_inserted_before_the_watermark_is_not_re_served() {
        let watermark = href_page(&["a", "b", "c"], None, 2)
            .next_watermark
            .expect("page one truncates");

        let second = href_page(&["a", "aa", "b", "c"], Some(&watermark), 2);
        assert_eq!(second.items, vec!["c"]);

        // And one inserted AFTER the watermark is served on the later page.
        let with_insert = href_page(&["a", "b", "bb", "c"], Some(&watermark), 2);
        assert_eq!(with_insert.items, vec!["bb", "c"]);
    }

    fn href_page(
        hrefs: &[&str],
        watermark: Option<&str>,
        page_size: usize,
    ) -> bifrost_dav_core::PageSlice<String> {
        let hrefs = sorted_candidate_hrefs(hrefs.iter().map(|href| (*href).to_string()).collect());
        slice_after_watermark(hrefs, watermark, page_size, String::as_str)
    }

    /// The cursor is local and every continuation re-runs the REPORT, so an
    /// unchanged result set returned in a DIFFERENT order across the two pages
    /// must still yield each resource exactly once. Against unsorted slicing
    /// page two returns `a` again and `c` is never delivered at all.
    #[test]
    fn watermark_pages_survive_a_reordered_second_report() {
        let first = href_page(&["a", "b", "c"], None, 2);
        assert_eq!(first.items, vec!["a", "b"]);
        let watermark = first.next_watermark.expect("page one truncates");

        // Same three resources, the order the server happened to answer with.
        let second = href_page(&["c", "a", "b"], Some(&watermark), 2);
        assert_eq!(second.items, vec!["c"]);

        let mut delivered = first.items;
        delivered.extend(second.items);
        delivered.sort();
        assert_eq!(delivered, vec!["a", "b", "c"]);
    }

    /// A resource named by several text-search legs is one candidate, not one
    /// per property it matched - otherwise the page size counts duplicates and
    /// the multiget asks for the same href repeatedly.
    #[test]
    fn a_resource_named_by_several_query_legs_is_one_candidate() {
        let page = href_page(&["b", "a", "b", "a", "c"], None, 2);
        assert_eq!(page.items, vec!["a", "b"]);
        assert_eq!(page.next_watermark.as_deref(), Some("b"));
    }

    /// A zero limit must terminate. Emitting the current watermark again gives
    /// a consumer that follows `next_cursor` an infinite non-advancing loop.
    #[test]
    fn a_zero_limit_is_an_exhausted_page_with_no_continuation() {
        let page = href_page(&["a", "b", "c"], None, 0);
        assert!(page.items.is_empty());
        assert_eq!(page.next_watermark, None);
    }

    #[test]
    fn failed_uri_preserved_in_event_diff() {
        // Brick 7: a previous entry whose href is in current.failed_hrefs
        // is NOT emitted as Destroyed.
        let previous = event_snapshot_with(&["a.ics", "b.ics"]);
        let mut current = event_snapshot_with(&["a.ics"]);
        current.failed_hrefs = vec!["b.ics".to_string()];
        assert!(
            current.changes_from(&previous).is_empty(),
            "a transiently-failed resource must not be destroyed"
        );

        // Without the failed-href, b.ics's absence IS a destroy.
        let current_no_failed = event_snapshot_with(&["a.ics"]);
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

    #[tokio::test]
    async fn a_refused_sync_member_does_not_advance_the_collection_token() {
        let script = dav_script([cal_multistatus(format!(
            "<D:multistatus xmlns:D=\"DAV:\"><D:sync-token>new</D:sync-token>{}</D:multistatus>",
            refused("a"),
        ))]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let mut previous = event_snapshot_with(&["https://dav.example.test/cal/a.ics"]);
        previous.collection_url = "https://dav.example.test/cal/".to_string();
        previous.token = Some("old".to_string());
        let (current, changes) = changes_from_cursor(&client, &previous)
            .await
            .expect("partial report");
        assert!(changes.is_empty());
        assert_eq!(current.token, previous.token);
        assert_eq!(current.entries, previous.entries);
    }

    #[test]
    fn sync_href_index_matches_the_shared_equivalence_rule() {
        let pairs = [
            (
                "https://DAV.example.test:443/cal/a%40b.ics",
                "https://dav.example.test/cal/a@b.ics",
                true,
            ),
            (
                "https://dav.example.test/cal/a@b.ics",
                "https://dav.example.test/cal/a@b.ics/",
                true,
            ),
            (
                "https://dav.example.test/cal/a@b.ics",
                "https://dav.example.test/cal/a@b.ics#ignored",
                true,
            ),
            (
                "https://dav.example.test/cal/a@b.ics",
                "https://dav.example.test/cal/a@b.ics?q=1",
                false,
            ),
            (
                "https://dav.example.test/cal/a%2Fb.ics",
                "https://dav.example.test/cal/a/b.ics",
                false,
            ),
            (
                "https://dav.example.test/cal/%FF.ics",
                "https://dav.example.test/cal/%FE.ics",
                false,
            ),
            ("relative/", "relative", true),
            (
                "http://dav.example.test/cal/a@b.ics",
                "https://dav.example.test/cal/a@b.ics",
                false,
            ),
        ];
        for (left, right, expected) in pairs {
            assert_eq!(
                dav_href_key(left) == dav_href_key(right),
                expected,
                "{left} versus {right}"
            );
            assert_eq!(same_dav_url(left, right), expected, "{left} versus {right}");
        }
    }

    #[test]
    fn sync_report_normalizes_each_snapshot_and_report_entry_once() {
        let ids: Vec<String> = (0..10_000)
            .map(|index| format!("https://dav.example.test/cal/{index:05}.ics"))
            .collect();
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let mut snapshot = event_snapshot_with(&refs);
        let entries = (10_000..11_000)
            .map(|index| crate::parse::CalDavSyncEntry {
                uri: format!("https://dav.example.test/cal/{index:05}.ics"),
                etag: Some("new".into()),
                status: Some(200),
                non_vevent: false,
            })
            .collect();
        let mut normalizations = 0;
        let changes = apply_sync_report_with_key(&mut snapshot, entries, |href| {
            normalizations += 1;
            dav_href_key(href)
        });
        assert_eq!(normalizations, 11_000);
        assert_eq!(changes.len(), 1_000);
        assert_eq!(snapshot.entries.len(), 11_000);
    }

    #[test]
    fn sync_report_alias_index_tracks_inserts_and_removals() {
        let encoded = "https://dav.example.test/cal/a%40b.ics";
        let literal = "https://dav.example.test/cal/a@b.ics";
        let mut snapshot = event_snapshot_with(&[encoded, literal]);
        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: literal.to_string(),
                    etag: None,
                    status: Some(404),
                    non_vevent: false,
                },
                crate::parse::CalDavSyncEntry {
                    uri: literal.to_string(),
                    etag: Some("updated".into()),
                    status: Some(200),
                    non_vevent: false,
                },
                crate::parse::CalDavSyncEntry {
                    uri: literal.to_string(),
                    etag: None,
                    status: Some(410),
                    non_vevent: false,
                },
                crate::parse::CalDavSyncEntry {
                    uri: literal.to_string(),
                    etag: Some("new".into()),
                    status: Some(200),
                    non_vevent: false,
                },
                crate::parse::CalDavSyncEntry {
                    uri: encoded.to_string(),
                    etag: Some("again".into()),
                    status: Some(200),
                    non_vevent: false,
                },
            ],
        );
        let expected = [
            (literal, ObjectChangeKind::Destroyed),
            (encoded, ObjectChangeKind::Updated),
            (encoded, ObjectChangeKind::Destroyed),
            (literal, ObjectChangeKind::Created),
            (literal, ObjectChangeKind::Updated),
        ];
        assert_eq!(changes.len(), expected.len());
        for (change, (id, kind)) in changes.iter().zip(expected) {
            assert!(matches!(change, Change::ObjectChange(change)
                if change.id.0 == id && change.kind == kind));
        }
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].uri, literal);
        assert_eq!(snapshot.entries[0].etag.as_deref(), Some("again"));
    }

    #[test]
    fn sync_member_aliases_update_and_delete_the_existing_native_id() {
        let uri = "https://dav.example.test/cal/a%40b.ics";
        let mut snapshot = event_snapshot_with(&[uri]);
        for (status, kind) in [
            (200, ObjectChangeKind::Updated),
            (404, ObjectChangeKind::Destroyed),
        ] {
            let changes = apply_sync_report(
                &mut snapshot,
                vec![crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/a@b.ics".to_string(),
                    etag: Some("changed".into()),
                    status: Some(status),
                    non_vevent: false,
                }],
            );
            assert!(
                matches!(changes.as_slice(), [Change::ObjectChange(change)] if change.id.0 == uri && change.kind == kind)
            );
        }
        assert!(snapshot.entries.is_empty());
    }

    #[tokio::test]
    async fn an_initial_partial_snapshot_does_not_seed_a_sync_token() {
        let mut home = home_listing();
        home.body = home.body.replace(
            "<D:displayname>",
            "<D:sync-token>new</D:sync-token><D:displayname>",
        );
        let script = dav_script([home, cal_multistatus(wrap(&[queried("a"), refused("b")]))]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let snapshot = CalDavAccount::event_snapshot(
            &client,
            "https://dav.example.test/cal/",
            AccountOperation::SyncChanges,
        )
        .await
        .expect("partial snapshot");
        assert!(snapshot.token.is_none());
        assert_eq!(snapshot.failed_hrefs.len(), 1);
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].headers["Depth"], "0");
    }

    #[tokio::test]
    async fn inventory_refuses_partial_member_coverage() {
        let script = dav_script([
            home_listing(),
            cal_multistatus(wrap(&[queried("a"), refused("b")])),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");
        let events = account
            .inventory_stream(folder_scope())
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(events.as_slice(), [InventoryEvent::Terminated(_)]));
    }

    #[tokio::test]
    async fn inventory_batch_counts_token_refused_filter_and_fallback_wire_bodies() {
        let token_response = home_listing();
        let token_bytes = token_response.body.len() as u64;
        let listing_response = cal_multistatus(wrap(&[queried("a")]));
        let listing_bytes = listing_response.body.len() as u64;
        let refusal = DavResponse {
            status: reqwest::StatusCode::METHOD_NOT_ALLOWED,
            headers: reqwest::header::HeaderMap::new(),
            body: "x".repeat(5000),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let refusal_bytes = refusal.body.len() as u64;
        let script = dav_script([token_response, refusal, listing_response]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");
        let events = account
            .inventory_stream(folder_scope())
            .collect::<Vec<_>>()
            .await;

        let InventoryEvent::Batch(batch) = &events[0] else {
            panic!("expected inventory batch: {events:?}");
        };
        assert_eq!(batch.bytes_in, token_bytes + refusal_bytes + listing_bytes);
    }

    #[test]
    fn sync_report_updates_snapshot_and_classifies_changes() {
        let mut snapshot = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: Some("token-1".to_string()),
            entries: vec![
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: Some("old".into()),
                },
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/two.ics".to_string(),
                    etag: Some("same".into()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: Some("new".into()),
                    status: Some(200),
                    non_vevent: false,
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/two.ics".to_string(),
                    etag: None,
                    status: Some(404),
                    non_vevent: false,
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/three.ics".to_string(),
                    etag: Some("created".into()),
                    status: Some(200),
                    non_vevent: false,
                },
            ],
        );
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
                (
                    "https://dav.example.test/cal/one.ics".to_string(),
                    ObjectChangeKind::Updated,
                ),
                (
                    "https://dav.example.test/cal/two.ics".to_string(),
                    ObjectChangeKind::Destroyed,
                ),
                (
                    "https://dav.example.test/cal/three.ics".to_string(),
                    ObjectChangeKind::Created,
                ),
            ]
        );
        assert_eq!(snapshot.entries.len(), 2);
    }

    /// A per-member status that is neither success nor a removal is the
    /// server declining to report on that member. Upserting it recorded an
    /// etag-less entry and emitted a change for a resource nobody observed.
    #[test]
    fn a_refused_sync_member_is_preserved_rather_than_upserted() {
        let mut snapshot = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/one.ics".to_string(),
                etag: Some("kept".into()),
            }],
            failed_hrefs: Vec::new(),
        };

        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: None,
                    status: Some(403),
                    non_vevent: false,
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/two.ics".to_string(),
                    etag: None,
                    status: Some(507),
                    non_vevent: false,
                },
            ],
        );

        assert!(changes.is_empty(), "refusals are not changes: {changes:?}");
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].etag.as_deref(), Some("kept"));
    }

    /// RFC 6578 s3.6: the partial token is a valid continuation point. Draining
    /// now reduces latency; stopping at the round cap still lets the next poll
    /// continue from the last token.
    #[tokio::test]
    async fn a_truncated_sync_report_is_drained_before_the_cursor_advances() {
        use bifrost_dav_core::DavResponse;
        use bifrost_dav_core::test_support::{dav_script, transcripts};
        use reqwest::StatusCode;
        use reqwest::header::HeaderMap;

        let multistatus = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let script = dav_script([
            multistatus(
                "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>/cal/</D:href><D:status>HTTP/1.1 507 Insufficient Storage</D:status></D:response><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><D:getetag>\"a\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response><D:sync-token>partial</D:sync-token></D:multistatus>",
            ),
            multistatus(
                "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>/cal/two.ics</D:href><D:propstat><D:prop><D:getetag>\"b\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response><D:sync-token>complete</D:sync-token></D:multistatus>",
            ),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let previous = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: Some("start".to_string()),
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };

        let (current, changes) = changes_from_cursor(&client, &previous)
            .await
            .expect("the scripted drain succeeds");

        let ids = changes
            .iter()
            .map(|change| match change {
                Change::ObjectChange(change) => change.id.0.clone(),
                _ => panic!("unexpected scope change"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "https://dav.example.test/cal/one.ics".to_string(),
                "https://dav.example.test/cal/two.ics".to_string(),
            ],
            "the second REPORT's members must reach the consumer"
        );
        // The collection's own 507 response is a statement about the REPORT,
        // never a member: it must not become an entry or a phantom Created.
        assert_eq!(current.entries.len(), 2);
        assert!(
            current
                .entries
                .iter()
                .all(|entry| entry.uri.ends_with(".ics")),
            "the collection self-response leaked into the snapshot: {:?}",
            current.entries
        );
        assert_eq!(current.token.as_deref(), Some("complete"));
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2, "the drain must re-issue the REPORT");
        assert!(requests[1].body.contains("partial"));
    }

    #[tokio::test]
    async fn changes_batch_counts_every_drained_report_body() {
        use bifrost_dav_core::DavResponse;
        use bifrost_dav_core::test_support::dav_script;
        use reqwest::StatusCode;
        use reqwest::header::HeaderMap;

        let first_body = "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>/cal/</D:href><D:status>HTTP/1.1 507 Insufficient Storage</D:status></D:response><D:sync-token>partial</D:sync-token></D:multistatus>";
        let second_body = "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><D:getetag>\"a\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response><D:sync-token>complete</D:sync-token></D:multistatus>";
        let multistatus = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let expected_bytes = (first_body.len() + second_body.len()) as u64;
        let script = dav_script([multistatus(first_body), multistatus(second_body)]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let previous = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: Some("start".to_string()),
            ..EventSnapshot::default()
        };
        let cursor = Kernel::cursor_from_snapshot(folder_scope(), &previous);
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");
        let events = account.changes_stream(cursor).collect::<Vec<_>>().await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("expected changes batch: {events:?}");
        };
        assert_eq!(batch.bytes_in, expected_bytes);
    }

    /// A consumer restating the calendar it read back, in the encoding it read
    /// it in, is not asking for a relocation. Byte comparison after a slash
    /// trim read it as one and issued a MOVE onto the collection the event
    /// already lives in, which `Overwrite: F` refuses with a 412 the consumer
    /// sees as a conflict.
    #[test]
    fn a_restated_calendar_url_is_not_a_relocation() {
        assert!(same_url(
            "https://dav.example.test/cal/My%20Cal/",
            "https://DAV.example.test:443/cal/My Cal"
        ));
        assert!(!same_url(
            "https://dav.example.test/cal/work/",
            "https://dav.example.test/cal/home/"
        ));
    }

    #[test]
    fn sync_report_404_for_unknown_href_emits_no_destroyed() {
        let mut snapshot = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/one.ics".to_string(),
                etag: Some("e".into()),
            }],
            failed_hrefs: Vec::new(),
        };

        // A 404/410 entry for an href the snapshot never held (created and
        // deleted between polls) must not surface a phantom Destroyed.
        let changes = apply_sync_report(
            &mut snapshot,
            vec![crate::parse::CalDavSyncEntry {
                uri: "https://dav.example.test/cal/ghost.ics".to_string(),
                etag: None,
                status: Some(404),
                non_vevent: false,
            }],
        );

        assert!(changes.is_empty());
        assert_eq!(snapshot.entries.len(), 1);
    }

    /// A sync member the server declares a non-event never enters the
    /// snapshot, and one that slipped in on an earlier poll leaves it with a
    /// Destroyed. The evidence is positive only: an unlabelled member is
    /// admitted exactly as before. Ablation: without the `non_vevent` arm the
    /// task is Created and the phantom stays.
    #[test]
    fn a_declared_task_is_dropped_from_the_sync_snapshot() {
        let mut snapshot = EventSnapshot {
            collection_url: "https://dav.example.test/cal/".to_string(),
            token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/phantom.ics".to_string(),
                etag: Some("leaked".into()),
            }],
            failed_hrefs: Vec::new(),
        };

        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/task.ics".to_string(),
                    etag: Some("t1".into()),
                    status: Some(200),
                    non_vevent: true,
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/phantom.ics".to_string(),
                    etag: Some("leaked-2".into()),
                    status: Some(200),
                    non_vevent: true,
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/unlabelled.ics".to_string(),
                    etag: Some("u1".into()),
                    status: Some(200),
                    non_vevent: false,
                },
            ],
        );

        let kinds: Vec<(String, ObjectChangeKind)> = changes
            .iter()
            .filter_map(|change| match change {
                Change::ObjectChange(object) => Some((object.id.0.clone(), object.kind)),
                _ => None,
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                (
                    "https://dav.example.test/cal/phantom.ics".to_string(),
                    ObjectChangeKind::Destroyed,
                ),
                (
                    "https://dav.example.test/cal/unlabelled.ics".to_string(),
                    ObjectChangeKind::Created,
                ),
            ]
        );
        assert_eq!(
            snapshot
                .entries
                .iter()
                .map(|entry| entry.uri.as_str())
                .collect::<Vec<_>>(),
            vec!["https://dav.example.test/cal/unlabelled.ics"]
        );
    }
}

#[cfg(test)]
mod partial_commit_tests {
    use super::*;
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
        let leaf = unsupported_error(AccountOperation::EventUpdate);
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
            ErrorScope::Event {
                id: EventId("destination".into())
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

#[cfg(test)]
mod partial_commit_rsvp_tests {
    use super::*;
    #[test]
    fn partial_commit_rsvp_local_refusal_after_post_is_terminal_with_record() {
        let leaf = unsupported_error(AccountOperation::EventRsvp);
        let error = rsvp_error(leaf.clone(), &EventId("event".into()), false);
        assert_eq!(error.recovery(), leaf.recovery());
        assert_eq!(
            error.partial_commit().unwrap().primary().application,
            Application::NotApplicable
        );
        assert_eq!(
            error.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::Submitted)
        );
    }
    #[test]
    fn partial_commit_rsvp_put_drop_reconciles() {
        let leaf = crate::client::transport_error(AccountOperation::EventRsvp, "put failed");
        let expected = leaf.leg_recovery().clone();
        let error = rsvp_error(leaf, &EventId("event".into()), true);
        assert_eq!(error.leg_recovery(), &expected);
        let bifrost_types::RecoveryClass::Reconcile(advice) = error.recovery() else {
            panic!("protected reply")
        };
        assert_eq!(
            advice.guidance.actions,
            [
                bifrost_types::ReconcileAction::CheckTarget,
                bifrost_types::ReconcileAction::DedupeByClientId,
                bifrost_types::ReconcileAction::ObserveOnly
            ]
        );
    }
}
