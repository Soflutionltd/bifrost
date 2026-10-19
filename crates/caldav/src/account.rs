use bifrost_dav_core::{
    DavProtocol, SnapshotEntry, append_path, decode_snapshot, decode_watermark_cursor,
    diff_snapshots, encode_snapshot, encode_watermark_cursor,
    inventory_entry as inventory_entry_from_snapshot, object_change, preserve_unobserved_entries,
    same_dav_url, slice_after_watermark, sorted_candidate_hrefs, worse_recovery,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::*;
use bytes::Bytes;
use futures::{StreamExt, stream};
use jiff::tz::Offset;
use jiff::{Timestamp, civil};
use reqwest::Url;

use crate::capabilities::{caldav_capabilities, scheduling_available};
use crate::client::{
    CalDavClient, FilteredHrefs, HrefQuery, PutCondition, event_scope, extend_candidates,
    local_error, missing_event_error, unsupported_error,
};
use crate::ical::{
    EventProjectionError, PatchError, ReplyError, WriteError, create_to_ical, event_from_ical,
    events_from_ical, new_uid, patch_to_ical, rsvp_patch, rsvp_reply_ical, split_event_id,
};
use crate::parse::CalendarCollection;
use crate::{CalDavConfig, CalDavCredentials};

// Version 2 changes snapshot ids from base-URL-relative to request-URI-relative.
//
// Accepted cost of that bump, recorded so it is not rediscovered as a bug: a
// v1 cursor is REFUSED by `decode_cursor_snapshot` rather than migrated - a v1
// payload is byte-identical in shape to a v2 one, so the ids cannot be
// distinguished and rewritten, only re-derived - so every consumer holding a
// pre-bump cursor pays one full re-sync per DAV account, once. That is a
// one-time cost against ids that were silently wrong, which was worth paying.
// Pinned by `event_cursor_rejects_the_base_relative_id_version`.
const CURSOR_ENVELOPE_VERSION: u32 = 2;
const CURSOR_MAGIC: &[u8] = b"CALDAVET1";
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
const EVENT_PAGE_SIZE: usize = 250;

#[derive(Debug)]
pub(crate) struct CalDavAccount {
    client: Arc<CalDavClient>,
    capabilities: AccountCapabilities,
    calendar_home: String,
    /// The collection a call that names no calendar routes to, and `None` when
    /// the home enumerated no collections.
    ///
    /// Deliberately not the calendar home in that case. See
    /// `no_default_calendar`.
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
            .ok_or_else(|| no_default_calendar(operation))
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
    /// event in both collections. That second-leg failure is wrapped
    /// `Protocol(PartialResponse)` with an acknowledged `Attempt`, the same
    /// treatment `event_rsvp` gives its own non-atomic sequence, so a consumer
    /// can tell "not moved" from "copied but not cleaned up" and reconciles
    /// rather than blindly replaying a write that already landed.
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
    ) -> Result<(), AccountError> {
        let destination = append_path(target_calendar, &resource_file_name(source_url));
        if client
            .move_resource(source_url, &destination, AccountOperation::EventUpdate)
            .await?
        {
            if !content_changed {
                return Ok(());
            }
            // The move is done and the id has changed. A failure here is a
            // half-applied request, not a failed one.
            return client
                .put_event(
                    &destination,
                    body,
                    PutCondition::None,
                    AccountOperation::EventUpdate,
                )
                .await
                .map(|_| ())
                .map_err(|error| {
                    partial_move_error(&error, "event moved but the field patch failed")
                });
        }
        // No MOVE support: copy first, then remove the original. Ordered this
        // way round because a failed copy leaves the event exactly where it
        // was, while a failed delete leaves it readable in two places - the
        // recoverable direction of a non-atomic pair.
        client
            .put_event(
                &destination,
                body,
                PutCondition::IfNoneMatch,
                AccountOperation::EventUpdate,
            )
            .await?;
        client
            .delete_event(source_url, AccountOperation::EventUpdate)
            .await
            .map_err(|error| {
                partial_move_error(
                    &error,
                    "event copied to the destination calendar but the original could not be removed",
                )
            })
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
                .ok_or_else(|| no_default_calendar(operation))?,
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
        home: Option<&str>,
        calendar: &str,
        operation: AccountOperation,
    ) -> Result<EventSnapshot, AccountError> {
        let sync_token = match home {
            Some(home) => client
                .list_calendars_for_operation(home, operation)
                .await?
                .into_iter()
                .find(|collection| same_url(&collection.href, calendar))
                .and_then(|collection| collection.sync_token),
            None => client.collection_sync_token(calendar, operation).await?,
        };
        // VEVENT-filtered, not the bare depth-1 PROPFIND: a VTODO or VJOURNAL
        // sharing the collection would otherwise enter the event snapshot and be
        // emitted as a created/updated event change that hydrates to nothing.
        // Degrades to the unfiltered listing when the server refuses the filter.
        let listing = client
            .list_event_hrefs_filtered(calendar, operation)
            .await?;
        let failed_hrefs = listing.failed_hrefs();
        let mut entries = listing
            .entries
            .into_iter()
            .map(|entry| EventSnapshotEntry {
                uri: entry.uri,
                etag: entry.etag,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        Ok(EventSnapshot {
            calendar_url: calendar.to_string(),
            sync_token,
            entries,
            failed_hrefs,
        })
    }
}

impl Account for CalDavAccount {
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
            .calendar_urls
            .iter()
            .cloned()
            .map(|url| CursorScope::Folder(FolderId(url)))
            .collect();
        Box::pin(stream::iter([
            SyncEvent::Batch(Batch {
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

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let client = Arc::clone(&self.client);
        let home = self.calendar_home.clone();
        let default_calendar = self.default_calendar_url.clone();
        let calendar_urls = self.calendar_urls.clone();
        Box::pin(async move {
            validate_event_scope(&scope, AccountOperation::EstablishCursor)?;
            let calendar = collection_url_for_scope(
                &scope,
                default_calendar.as_deref(),
                &calendar_urls,
                AccountOperation::EstablishCursor,
            )?;
            let snapshot = Self::event_snapshot(
                &client,
                Some(&home),
                &calendar,
                AccountOperation::EstablishCursor,
            )
            .await?;
            Ok(CursorEstablishment::Ready(cursor_from_snapshot(
                scope, &snapshot,
            )))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        let client = Arc::clone(&self.client);
        let home = self.calendar_home.clone();
        let default_calendar = self.default_calendar_url.clone();
        let calendar_urls = self.calendar_urls.clone();
        let coverage_scope = scope.clone();
        let coverage_domain = collection_coverage_domain(
            coverage_scope,
            scope_collection_url(&scope, default_calendar.as_deref()),
        );
        // COMPLETE coverage is an accurate claim here: this walk terminates
        // wholesale on any failure, so it never advances a checkpoint across a
        // gap. A version that starts absorbing per-item failures must build
        // `InventoryBatch` directly rather than converting.
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                if let Err(error) = validate_event_scope(&scope, AccountOperation::SyncInventory) {
                    events.push(SyncEvent::Terminated(error));
                    return events;
                }
                let calendar = match collection_url_for_scope(
                    &scope,
                    default_calendar.as_deref(),
                    &calendar_urls,
                    AccountOperation::SyncInventory,
                ) {
                    Ok(calendar) => calendar,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let started = Instant::now();
                let snapshot = match Self::event_snapshot(
                    &client,
                    Some(&home),
                    &calendar,
                    AccountOperation::SyncInventory,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let checkpoint = cursor_from_snapshot(scope, &snapshot);
                let items = snapshot
                    .entries
                    .iter()
                    .map(inventory_entry_from_snapshot)
                    .collect();
                events.push(SyncEvent::Batch(Batch {
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
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        unsupported_stream(AccountOperation::Hydrate)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let client = Arc::clone(&self.client);
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                let previous = match decode_cursor_snapshot(&cursor) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let started = Instant::now();
                let (current, changes) = match changes_from_cursor(&client, &previous).await {
                    Ok(result) => result,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let checkpoint = cursor_from_snapshot(cursor.scope, &current);
                events.push(SyncEvent::Batch(Batch {
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
        match op.validate_for_account(Protocol::CalDav) {
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
        _property: String,
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
        unsupported_future(AccountOperation::AddressBooksList)
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        unsupported_future(AccountOperation::ContactsList)
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        unsupported_future(AccountOperation::ContactGet)
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        unsupported_future(AccountOperation::ContactCreate)
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContactUpdate)
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContactDelete)
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
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
            let watermark = decode_event_page_cursor(
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
            // The time-range filter runs on the SERVER and answers with hrefs
            // only, so the page is sliced before anything is hydrated. The
            // local overlap check below is still the authority over the page:
            // it is recurrence-aware in ways a bare `time-range` element is
            // not, and RFC 4791 s9.9 leaves a server free to be generous.
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
            let start = range.start;
            let end = range.end;
            hydrated_event_page(
                &client,
                &calendar_url,
                candidates,
                watermark.as_deref(),
                page_size,
                AccountOperation::EventsInRange,
                |event| event_in_range(event, &start, &end),
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

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
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
                        put_condition(current.etag.as_deref()),
                        AccountOperation::EventUpdate,
                    )
                    .await?;
                return Ok(());
            };
            // A content patch riding along with the move needs a write of its
            // own; a move-only patch does not, and must not be charged a
            // partial-failure verdict for a leg it never needed.
            let content_changed = patch_changes_content(&patch);
            Self::relocate_event(&client, &url, &target_calendar, body, content_changed).await
        })
    }

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
            // been told. Local encoding failures are as much a second-leg
            // failure as a refused PUT, so they take the same wrapping
            // rather than reporting a bare `unsupported`.
            let patch = rsvp_patch(&current, status, &rsvp_email).map_err(|_| {
                rsvp_local_write_error(unsupported_error(AccountOperation::EventRsvp))
            })?;
            let body = patch_to_ical(&current, &patch).map_err(|error| {
                rsvp_local_write_error(patch_error(AccountOperation::EventRsvp, error))
            })?;
            let url = client.resolve_url(&event.0);
            client
                .put_event(
                    &url,
                    body,
                    put_condition(current.etag.as_deref()),
                    AccountOperation::EventRsvp,
                )
                .await
                .map_err(rsvp_local_write_error)?;
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
            let watermark = decode_event_page_cursor(
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

fn rsvp_email_from_config(config: &CalDavConfig) -> Option<String> {
    match &config.credentials {
        CalDavCredentials::Basic { username, .. } if username.contains('@') => {
            Some(username.to_ascii_lowercase())
        }
        _ => None,
    }
}

fn decode_event_page_cursor(
    cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<Option<String>, AccountError> {
    decode_watermark_cursor(cursor, operation, DavProtocol::CalDav)
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
    let HrefQuery {
        hrefs,
        failed_hrefs,
        degraded,
    } = candidates;
    let hrefs = sorted_candidate_hrefs(hrefs);
    let total = u64::try_from(hrefs.len()).unwrap_or(u64::MAX);
    let slice = slice_after_watermark(hrefs, watermark, page_size, String::as_str);
    let fetched = client
        .fetch_events(calendar_url, &slice.items, operation)
        .await?;
    let skipped_scopes = skipped_calendar_scope(
        calendar_url,
        worse_recovery_option(degraded, fetched.degraded),
    );
    let fetched = fetched.report;
    let mut failed = fetched.failed_hrefs();
    failed.extend(failed_hrefs);
    let mut events = Vec::new();
    let mut materialized = HashSet::new();
    for event in fetched.events {
        let uri = event.uri;
        match events_from_ical(
            uri.clone(),
            CalendarId(calendar_url.to_string()),
            event.etag,
            &event.data,
        ) {
            Ok(projected) => {
                materialized.insert(uri);
                events.extend(projected.into_iter().filter(|event| keep(event)));
            }
            Err(_) => failed.push(uri),
        }
    }
    one_outcome_per_id(&mut failed, &materialized);
    // Served in the same key order the page was sliced in; a multiget answers
    // in whatever order it likes.
    events.sort_by(|left, right| left.id.0.cmp(&right.id.0));
    Ok(Page {
        items: events,
        next_cursor: slice.next_watermark.as_deref().map(encode_watermark_cursor),
        estimated_total: Some(total),
        failed_ids: failed,
        skipped_scopes,
    })
}

/// Keep the worse of two optional failures, so a refused query leg is not
/// buried under a milder multiget failure or dropped entirely.
fn worse_recovery_option(
    current: Option<AccountError>,
    candidate: Option<AccountError>,
) -> Option<AccountError> {
    match candidate {
        Some(candidate) => worse_recovery(current, candidate),
        None => current,
    }
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
    AccountErrorBuilder::new(
        AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
        Cause::Internal(InternalCause::new(
            InternalErrorKind::InvariantViolated,
            Some(DiagnosticText::support_only(detail)),
        )),
    )
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

/// The scheduling POST completed before the local PUT began. Preserve that
/// acknowledged first-leg evidence on any second-leg failure so callers know
/// the organizer may already have acted on the reply.
///
/// Accepted, not a defect: RSVP is non-atomic BY NATURE - iTIP delivers the
/// reply to the organizer through the scheduling outbox, and the attendee's own
/// copy of the event is a separate resource that only a second write can
/// update. There is no transaction spanning the two and no compensating action
/// (un-sending an iTIP reply is not a thing). So EVERY failure path after the
/// outbox POST is funnelled through here, including the purely local encoding
/// steps between the POST and the PUT, and comes out
/// `Protocol(PartialResponse)` with `TransmissionState::Acknowledged`. A
/// local-looking failure is classified as a partial success on purpose:
/// what matters to the consumer is that the organizer already saw the reply.
fn rsvp_local_write_error(error: AccountError) -> AccountError {
    partial_sequence_error(
        &error,
        AccountOperation::EventRsvp,
        "schedule reply was accepted but the local event update failed",
    )
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
        ..patch.clone()
    };
    content != EventPatch::default()
}

/// Reclassify the failure of a later leg of a non-atomic sequence whose
/// earlier leg already landed on the server.
///
/// `Protocol(PartialResponse)` plus an acknowledged `Attempt` is what tells a
/// consumer the request was half-applied rather than refused, so it reconciles
/// instead of replaying a write that already took effect. The original cause
/// chain rides along as secondary evidence.
///
/// The unreplayable declaration must be RE-STATED here. `dispatch_once` stamps
/// `idempotency_override(false)` on the leg's own failure, but this is a FRESH
/// `AccountErrorBuilder`, and an override is not part of the cause chain that
/// gets copied across - so without the restatement `derive_protocol` falls back
/// to the `AccountOperation` table, which calls `EventUpdate` idempotent, and
/// answers `Retry(SameRequest)` for a sequence that is half applied. The
/// declaration must survive the wire, the classification, and any rebuild.
/// It holds for every caller regardless of what the table says: a sequence whose
/// earlier leg landed cannot be replayed from the top, because the replay
/// re-runs that leg against state it already changed.
fn partial_sequence_error(
    error: &AccountError,
    operation: AccountOperation,
    detail: &'static str,
) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::CalDav,
            detail: Some(DiagnosticText::support_only(detail)),
        }),
    )
    .protocol(Protocol::CalDav)
    .operation(operation)
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

fn partial_move_error(error: &AccountError, detail: &'static str) -> AccountError {
    partial_sequence_error(error, AccountOperation::EventUpdate, detail)
}

/// The last path segment of a resource URL - the name the resource keeps when
/// it moves into another collection.
///
/// Falls back to a fresh UID-backed name when the URL has no usable final
/// segment, so a move never targets the destination collection itself.
fn resource_file_name(url: &str) -> String {
    Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .path_segments()
                .and_then(|mut segments| segments.next_back().filter(|name| !name.is_empty()))
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("{}.ics", new_uid()))
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

/// Derive the parent collection URL of an event resource URL.
///
/// This must go through a real URL parse. A final-slash search over the whole
/// string picks up a slash living in the query or fragment - for
/// `https://dav.test/cal/one.ics?redirect=/foo` it yields
/// `https://dav.test/cal/one.ics?redirect=/`, which is not a collection URL at
/// all, and which then travels onward as `calendar_id` and `calendar_native`.
/// Query and fragment are dropped before the final path segment is removed.
fn event_calendar_url(event_url: &str) -> Option<String> {
    bifrost_net::url::parent_collection_url(event_url)
}

/// Reduce the failure lane to one outcome per resource id.
///
/// Search runs a REPORT per property, so the resource the ATTENDEE query
/// refused can be the same resource the SUMMARY query returned in full.
/// The data arrived, so success is the true outcome; reporting the id in
/// both lanes would make a consumer count it twice and treat an event it
/// can display as lost. The sort and dedup finish the job for ids that
/// failed in more than one REPORT.
///
/// `bifrost-carddav` has a function of the same name and shape, and the two
/// were deliberately NOT collapsed into `bifrost-dav-core` when the propstat
/// machine and the snapshot layer were: this one filters against a
/// materialized href set, the CardDAV twin against the `native_id` of the
/// parsed cards, so they share only their outline and a shared version
/// would need a projection each side supplies anyway.
fn one_outcome_per_id(failed: &mut Vec<String>, materialized: &HashSet<String>) {
    failed.retain(|href| !materialized.contains(href));
    failed.sort_unstable();
    failed.dedup();
}

/// Publish a partially-refused walk as a skipped scope.
///
/// A REPORT leg that failed wholly means this calendar was not fully
/// searched. The items already collected stay valid, but "no more matches"
/// is not what happened, and the consumer needs the classified failure to
/// know whether to reauthorize, retry, or stop. `failed_ids` cannot carry
/// that: it is a bare list of resource ids with no recovery class, and the
/// refused leg often does not even name the resources it lost.
fn skipped_calendar_scope(calendar_url: &str, degraded: Option<AccountError>) -> Vec<SkippedScope> {
    degraded
        .map(|error| SkippedScope {
            scope: ErrorScope::Calendar {
                id: calendar_url.to_string().into(),
            },
            error,
        })
        .into_iter()
        .collect()
}

fn put_condition(etag: Option<&str>) -> PutCondition<'_> {
    etag.filter(|etag| {
        !etag
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    })
    .map_or(PutCondition::None, PutCondition::IfMatch)
}

/// Resource identity for a consumer-restated calendar id against a URL this
/// crate derived. Delegates to the shared normalizing comparison: percent
/// encoding, host case and a redundant default port are spellings, not
/// relocations, and reading one as a move issues a MOVE onto the collection the
/// resource already lives in (refused by `Overwrite: F` as a spurious 412).
fn same_url(left: &str, right: &str) -> bool {
    same_dav_url(left, right)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct EventSnapshot {
    calendar_url: String,
    sync_token: Option<String>,
    entries: Vec<EventSnapshotEntry>,
    /// Hrefs the server reported *failed* within the 207 of the poll
    /// that built this snapshot. Not persisted in the cursor (a
    /// per-poll observation); the diff preserves these `previous`
    /// entries rather than destroying them (brick 7).
    failed_hrefs: Vec<String>,
}

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

fn validate_event_scope(
    scope: &CursorScope,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if matches!(
        scope,
        CursorScope::Type(ObjectType::CalendarEvent) | CursorScope::Folder(_)
    ) {
        Ok(())
    } else {
        Err(crate::client::local_error(
            operation,
            "CalDAV only supports calendar-event cursor scopes",
        ))
    }
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
    collections
        .iter()
        .map(|collection| collection.href.clone())
        .collect()
}

/// A call that names no calendar cannot be routed, because the calendar home
/// enumerated no collections.
///
/// The alternative - falling back to the calendar home URL - is what this
/// replaces. The home is not itself a collection in that case (`list_calendars`
/// already returns the home when it genuinely is one, so an empty result means
/// an empty backend), so every such request went to a resource a spec-correct
/// server 404s, and reported it as a remote failure rather than as the local
/// routing failure it is. It also contradicted the empty-home contract
/// `calendars_list` is pinned to.
fn no_default_calendar(operation: AccountOperation) -> AccountError {
    local_error(
        operation,
        "CalDAV account has no calendar collection to route a call that names none",
    )
}

/// The collection a call that names no calendar routes to: the first
/// discovered one, and `None` when the home enumerated none.
///
/// Deliberately has no access to the calendar home, so the fallback this
/// replaced cannot be reintroduced here without also changing the signature.
/// See `no_default_calendar` for why the home is the wrong answer.
fn default_collection_url(collections: &[crate::parse::CalendarCollection]) -> Option<String> {
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
        CursorScope::Type(ObjectType::CalendarEvent) => default_url
            .map(str::to_string)
            .ok_or_else(|| no_default_calendar(operation)),
        CursorScope::Folder(folder) if collection_urls.contains(&folder.0) => Ok(folder.0.clone()),
        _ => Err(local_error(
            operation,
            "CalDAV cursor scope does not name a discovered calendar",
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

fn collection_coverage_domain(scope: CursorScope, collection_url: Option<&str>) -> CoverageDomain {
    match scope {
        CursorScope::Folder(_) => CoverageDomain::full(scope),
        // An unroutable legacy scope still needs a domain to carry the
        // terminating stream; the walk fails before the region is read.
        _ => CoverageDomain {
            scope,
            coordinate: CoverageCoordinate::ProviderRegion {
                namespace: "caldav".to_string(),
                region: collection_url.unwrap_or_default().as_bytes().to_vec(),
            },
            snapshot: SnapshotIdentity::unstable(),
        },
    }
}

fn cursor_from_snapshot(scope: CursorScope, snapshot: &EventSnapshot) -> ChangeCursor {
    ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::CalDav,
            envelope_version: CURSOR_ENVELOPE_VERSION,
            bytes: encode_cursor_snapshot(snapshot),
        },
        advanced_through: None,
        envelope_version: bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
    }
}

fn encode_cursor_snapshot(snapshot: &EventSnapshot) -> Vec<u8> {
    encode_snapshot(
        CURSOR_MAGIC,
        &snapshot.calendar_url,
        snapshot.sync_token.as_deref(),
        &snapshot.entries,
    )
}

fn decode_cursor_snapshot(cursor: &ChangeCursor) -> Result<EventSnapshot, AccountError> {
    validate_event_scope(&cursor.scope, AccountOperation::SyncChanges)?;
    if cursor.server_state.protocol != ProtocolKind::CalDav
        || cursor.server_state.envelope_version != CURSOR_ENVELOPE_VERSION
    {
        return Err(cursor_error("CalDAV cursor protocol or version mismatch"));
    }
    let decoded = decode_snapshot(
        CURSOR_MAGIC,
        DavProtocol::CalDav.label(),
        &cursor.server_state.bytes,
    )
    .map_err(cursor_error)?;
    Ok(EventSnapshot {
        calendar_url: decoded.collection_url,
        sync_token: decoded.token,
        entries: decoded.entries,
        failed_hrefs: Vec::new(),
    })
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
    if let Some(sync_token) = previous.sync_token.as_deref() {
        let mut current = previous.clone();
        let mut changes = Vec::new();
        let mut token = sync_token.to_string();
        // RFC 6578 s3.6: a server may answer a sync REPORT with only part of
        // the change set, marking it with a 507 response for the collection
        // itself and a token that represents that partial progress. Stopping
        // there and checkpointing the partial token as if it were complete
        // loses every change beyond the truncation point until etag drift
        // happens to surface it. iCloud and large Cyrus collections truncate in
        // practice, so drain the rest by re-issuing the REPORT with the token
        // the server just handed back.
        for _ in 0..SYNC_TRUNCATION_ROUNDS {
            let report = client.sync_events(&previous.calendar_url, &token).await?;
            let truncated = report.truncated;
            let next_token = report.sync_token;
            changes.extend(apply_sync_report(&mut current, report.entries));
            let Some(next_token) = next_token else {
                tracing::warn!(
                    target: "bifrost_caldav::sync",
                    calendar = %previous.calendar_url,
                    "sync-collection response omitted the required sync-token; retaining the previous token"
                );
                break;
            };
            current.sync_token = Some(next_token.clone());
            if !truncated {
                break;
            }
            // Forward progress or stop. A server that repeats the token it was
            // given while still claiming truncation would otherwise spin this
            // loop against the wire for as many rounds as the cap allows.
            if next_token == token {
                tracing::warn!(
                    target: "bifrost_caldav::sync",
                    calendar = %previous.calendar_url,
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
        None,
        &previous.calendar_url,
        AccountOperation::SyncChanges,
    )
    .await?;
    preserve_unobserved_event_entries(previous, &mut current);
    let changes = diff_event_snapshots(previous, &current);
    Ok((current, changes))
}

fn preserve_unobserved_event_entries(previous: &EventSnapshot, current: &mut EventSnapshot) {
    preserve_unobserved_entries(
        &previous.entries,
        &mut current.entries,
        &current.failed_hrefs,
    );
}

fn apply_sync_report(
    current: &mut EventSnapshot,
    entries: Vec<crate::parse::CalDavSyncEntry>,
) -> Vec<Change> {
    let mut changes = Vec::new();
    for entry in entries {
        let uri = entry.uri;
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
            if let Some(index) = current.entries.iter().position(|known| known.uri == uri) {
                current.entries.remove(index);
                changes.push(object_change(&uri, ObjectChangeKind::Destroyed));
            }
            continue;
        }
        let resolved = EventSnapshotEntry {
            uri: uri.clone(),
            etag: entry.etag,
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
                changes.push(object_change(&uri, ObjectChangeKind::Created));
            }
        }
    }
    changes
}

/// Diff two event snapshots. The rule, the transient-empty-207 suppression
/// and the failed-href preservation all live in `bifrost-dav-core`, which
/// `bifrost-carddav` reads through the same door.
fn diff_event_snapshots(previous: &EventSnapshot, current: &EventSnapshot) -> Vec<Change> {
    diff_snapshots(&previous.entries, &current.entries, &current.failed_hrefs)
}

fn cursor_error(message: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .protocol(Protocol::CalDav)
    .operation(AccountOperation::SyncChanges)
    .text(DiagnosticText::support_only(message))
    .try_build()
    .expect("valid account error classification")
}

fn event_in_range(event: &CalendarEvent, start: &EventTime, end: &EventTime) -> bool {
    if start.value.is_empty() || end.value.is_empty() || event.start.value.is_empty() {
        return true;
    }
    let Some((range_start, range_end)) = time_interval(start, Some(end), false) else {
        return true;
    };
    let Some((event_start, event_end)) =
        time_interval(&event.start, event.end.as_ref(), event.is_all_day)
    else {
        return true;
    };
    let rrule = event.recurrence.rrule.as_deref().unwrap_or_default();
    if rrule.is_empty() {
        // Non-recurring: RFC 4791 s9.9's overlap rule, which has TWO shapes.
        // An event with duration overlaps a `[start, end)` window when
        // `start < event_end && end > event_start`. A zero-length instant (no
        // DTEND, no DURATION, not all-day - `time_interval` collapses it to
        // `event_end == event_start`) overlaps when `start <= DTSTART && end >
        // DTSTART`: the instant sitting EXACTLY on the window's start is
        // inside it. The strict rule alone dropped that event, which the
        // server's time-range REPORT had correctly returned. Ruled dav-F9,
        // 2026-09-07; pinned by `range_filter_includes_a_zero_length_event_on_
        // the_window_start` and its two boundary siblings.
        if event_start == event_end {
            return range_start <= event_start && event_start < range_end;
        }
        return event_start < range_end && event_end > range_start;
    }
    // Recurring master: its own interval can sit entirely before the
    // window while a later occurrence lands inside it. The server's
    // time-range REPORT already expanded the RRULE and returned this
    // resource because an occurrence overlaps, so the local guard must
    // not drop it. Occurrences only run forward from the master start, so
    // the series can reach the window unless it begins after the window
    // ends, or provably ends (RRULE UNTIL) before the window starts.
    if event_start >= range_end {
        return false;
    }
    if event_end > range_start {
        return true;
    }
    match rrule_until(rrule) {
        Some(until) => until >= range_start,
        // COUNT-bounded or open-ended series: without full expansion we
        // trust the server's REPORT and retain the master.
        None => true,
    }
}

/// Parse the instant named by an RRULE `UNTIL=` part, if present. Handles
/// both `YYYYMMDD` (date) and `YYYYMMDDTHHMMSS[Z]` (date-time) forms.
fn rrule_until(rrule: &str) -> Option<Timestamp> {
    let value = rrule.split(';').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        key.eq_ignore_ascii_case("UNTIL").then_some(value)
    })?;
    parse_ical_instant(value.trim())
}

fn parse_ical_instant(value: &str) -> Option<Timestamp> {
    if value.len() == 8 {
        let date = civil::Date::strptime("%Y%m%d", value).ok()?;
        return Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok();
    }
    let core = value.trim_end_matches('Z');
    let naive = civil::DateTime::strptime("%Y%m%dT%H%M%S", core).ok()?;
    Offset::UTC.to_timestamp(naive).ok()
}

fn time_interval(
    start: &EventTime,
    end: Option<&EventTime>,
    is_all_day: bool,
) -> Option<(Timestamp, Timestamp)> {
    let start = comparable_time(start, is_all_day)?;
    let end = end
        .and_then(|end| comparable_time(end, is_all_day))
        .unwrap_or(start);
    Some((start, end))
}

fn comparable_time(time: &EventTime, is_all_day: bool) -> Option<Timestamp> {
    if is_all_day || time.value.len() == 10 {
        let date = civil::Date::strptime("%Y-%m-%d", &time.value).ok()?;
        return Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok();
    }
    time.value.parse::<Timestamp>().ok()
}

fn caldav_query_time(time: &EventTime) -> Option<String> {
    let instant = if time.value.len() == 10 {
        let date = civil::Date::strptime("%Y-%m-%d", &time.value).ok()?;
        Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok()?
    } else {
        time.value.parse::<Timestamp>().ok()?
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
            format!("invalid event range start: {}", start.value),
        )
    })?;
    let end = caldav_query_time(end).ok_or_else(|| {
        crate::client::local_error(
            AccountOperation::EventsInRange,
            format!("invalid event range end: {}", end.value),
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
            url: "https://dav.example.test/cal/".to_string(),
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
            Some(b"https://dav.example.test/cal/b.ics".to_vec()),
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
            url: "https://dav.example.test/cal/".to_string(),
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
                page_cursor: Some(b"https://dav.example.test/cal/b.ics".to_vec()),
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
            url: "https://dav.example.test/cal/".to_string(),
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

    /// The calendar-home depth-1 answer the snapshot lane reads its collection
    /// sync token out of.
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
        decode_cursor_snapshot(cursor)
            .expect("the cursor this account just minted decodes")
            .entries
            .into_iter()
            .map(|entry| entry.uri)
            .collect()
    }

    fn folder_scope() -> CursorScope {
        CursorScope::Folder(FolderId("https://dav.example.test/cal/".to_string()))
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
            url: "https://dav.example.test/cal/".to_string(),
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
            Some(b"https://dav.example.test/cal/b.ics".to_vec()),
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

    fn large_range(limit: Option<u32>, page_cursor: Option<Vec<u8>>) -> EventRange {
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
            url: "https://dav.example.test/cal/".to_string(),
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
            url: "https://dav.example.test/cal/".to_string(),
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
            Some(b"https://dav.example.test/cal/b.ics".to_vec()),
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
            url: "https://dav.example.test/cal/".to_string(),
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

        one_outcome_per_id(&mut failed, &materialized);

        assert_eq!(failed, vec!["/cal/two.ics".to_string()]);
    }

    #[test]
    fn a_degraded_leg_becomes_a_skipped_calendar_scope() {
        let error = crate::client::status_error(
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
        EventTime {
            value: value.to_string(),
            timezone: None,
        }
    }

    fn event(start: &str, end: &str, is_all_day: bool) -> CalendarEvent {
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
            end: Some(time(end)).filter(|end| !end.value.is_empty()),
            is_all_day,
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

        let err = unsupported_future::<HostedAttachment>(AccountOperation::HostAttachment)
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
    fn range_filter_includes_all_day_on_window_start() {
        let event = event("2026-06-02", "2026-06-03", true);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_includes_event_that_started_before_window() {
        let event = event("2026-06-01T23:00:00Z", "2026-06-02T01:00:00Z", false);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-02T23:59:59Z")
        ));
    }

    #[test]
    fn range_filter_excludes_event_after_window() {
        let event = event("2026-06-04T00:00:00Z", "2026-06-04T01:00:00Z", false);

        assert!(!event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    /// RFC 4791 s9.9: a zero-length instant on the window's start is inside
    /// the window (`start <= DTSTART && end > DTSTART`). Ablation: the
    /// duration rule alone (`event_end > range_start`) drops it.
    #[test]
    fn range_filter_includes_a_zero_length_event_on_the_window_start() {
        let event = event("2026-06-02T00:00:00Z", "2026-06-02T00:00:00Z", false);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    /// The same instant on the window's END is outside it: the window is
    /// half-open, and an event with duration ending there is excluded too.
    #[test]
    fn range_filter_excludes_a_zero_length_event_on_the_window_end() {
        let event = event("2026-06-03T00:00:00Z", "2026-06-03T00:00:00Z", false);

        assert!(!event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    /// And one instant before the window starts is outside it, so the
    /// inclusive rule is a boundary rule and not a widening.
    #[test]
    fn range_filter_excludes_a_zero_length_event_just_before_the_window() {
        let event = event("2026-06-01T23:59:59Z", "2026-06-01T23:59:59Z", false);

        assert!(!event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_excludes_event_ending_at_window_start() {
        let event = event("2026-06-01", "2026-06-02", true);

        assert!(!event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn rsvp_second_leg_error_records_acknowledged_first_leg() {
        let error = crate::client::transport_error(AccountOperation::EventRsvp, "put failed");
        let decorated = rsvp_local_write_error(error);

        assert!(decorated.chain().iter().any(|cause| matches!(
            cause,
            Cause::Attempt(attempt)
                if attempt.transmission_state == TransmissionState::Acknowledged
        )));
        assert!(matches!(
            decorated.recovery(),
            RecoveryClass::Reconcile(advice)
                if advice.reason == ReconcileReason::PartialCompletionSignal
        ));
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

        assert!(event_in_range(
            &event,
            &time("2026-06-01T00:00:00Z"),
            &time("2026-06-08T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_drops_recurring_master_that_ended_before_window() {
        // A bounded weekly series whose UNTIL is before the window start
        // cannot reach it, so the guard drops the out-of-window master.
        let event = recurring_event(
            "2026-05-04T12:00:00Z",
            "2026-05-04T13:00:00Z",
            "FREQ=WEEKLY;UNTIL=20260525T120000Z",
        );

        assert!(!event_in_range(
            &event,
            &time("2026-06-01T00:00:00Z"),
            &time("2026-06-08T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_drops_recurring_master_starting_after_window() {
        let event = recurring_event(
            "2026-07-04T12:00:00Z",
            "2026-07-04T13:00:00Z",
            "FREQ=WEEKLY",
        );

        assert!(!event_in_range(
            &event,
            &time("2026-06-01T00:00:00Z"),
            &time("2026-06-08T00:00:00Z")
        ));
    }

    #[test]
    fn rrule_until_parses_date_form_and_case_insensitive_key() {
        // UNTIL comes in both date and date-time forms, and RRULE part
        // names are case-insensitive per RFC 5545.
        let until = rrule_until("FREQ=WEEKLY;until=20260525").expect("date-form UNTIL");
        assert_eq!(until.to_string(), "2026-05-25T00:00:00Z");

        let until = rrule_until("FREQ=DAILY;UNTIL=20260525T120000").expect("datetime UNTIL");
        assert_eq!(until.to_string(), "2026-05-25T12:00:00Z");

        let until = rrule_until("FREQ=DAILY;UNTIL=20260525T120000Z").expect("UTC UNTIL");
        assert_eq!(until.to_string(), "2026-05-25T12:00:00Z");

        assert!(rrule_until("FREQ=DAILY").is_none());
        assert!(rrule_until("FREQ=DAILY;UNTIL=garbage").is_none());
        assert!(rrule_until("").is_none());
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
    fn event_cursor_snapshot_round_trips() {
        let snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/a.ics".to_string(),
                    etag: Some("a".to_string()),
                },
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/b.ics".to_string(),
                    etag: None,
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);

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
    fn event_cursor_rejects_the_base_relative_id_version() {
        let snapshot = EventSnapshot::default();
        let mut cursor =
            cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);
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
    fn event_cursor_rejects_impossible_count_before_allocating() {
        let snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: None,
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };
        let mut cursor =
            cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);
        let count_offset = cursor.server_state.bytes.len() - 4;
        cursor.server_state.bytes[count_offset..].copy_from_slice(&u32::MAX.to_be_bytes());

        assert!(decode_cursor_snapshot(&cursor).is_err());
    }

    #[test]
    fn event_snapshot_diff_classifies_object_changes() {
        let previous = EventSnapshot {
            calendar_url: "cal".to_string(),
            sync_token: None,
            entries: vec![
                EventSnapshotEntry {
                    uri: "a.ics".to_string(),
                    etag: Some("old".to_string()),
                },
                EventSnapshotEntry {
                    uri: "b.ics".to_string(),
                    etag: Some("same".to_string()),
                },
                EventSnapshotEntry {
                    uri: "d.ics".to_string(),
                    etag: Some("gone".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let current = EventSnapshot {
            calendar_url: "cal".to_string(),
            sync_token: None,
            entries: vec![
                EventSnapshotEntry {
                    uri: "a.ics".to_string(),
                    etag: Some("new".to_string()),
                },
                EventSnapshotEntry {
                    uri: "b.ics".to_string(),
                    etag: Some("same".to_string()),
                },
                EventSnapshotEntry {
                    uri: "c.ics".to_string(),
                    etag: Some("created".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = diff_event_snapshots(&previous, &current);
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
            calendar_url: "cal".to_string(),
            sync_token: None,
            entries: entries
                .iter()
                .map(|uri| EventSnapshotEntry {
                    uri: (*uri).to_string(),
                    etag: Some("e".to_string()),
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
        assert!(diff_event_snapshots(&previous, &empty).is_empty());
        assert!(diff_event_snapshots(&previous, &previous).is_empty());
        assert!(diff_event_snapshots(&empty, &empty).is_empty());
    }

    #[test]
    fn empty_poll_checkpoint_preserves_snapshot_for_the_recovery_poll() {
        let previous = event_snapshot_with(&["a.ics", "b.ics"]);
        let mut empty = event_snapshot_with(&[]);
        empty.sync_token = Some("refreshed".to_string());

        preserve_unobserved_event_entries(&previous, &mut empty);
        assert_eq!(empty.entries.len(), 2);
        assert_eq!(empty.sync_token.as_deref(), Some("refreshed"));

        let recovered = event_snapshot_with(&["a.ics", "b.ics"]);
        assert!(diff_event_snapshots(&empty, &recovered).is_empty());
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
            decode_event_page_cursor(Some(cursor), AccountOperation::EventsInRange)
                .expect("valid cursor")
                .as_deref(),
            Some("b")
        );
        assert!(
            decode_event_page_cursor(Some(Vec::new()), AccountOperation::EventsInRange).is_err(),
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
            diff_event_snapshots(&previous, &current).is_empty(),
            "a transiently-failed resource must not be destroyed"
        );

        // Without the failed-href, b.ics's absence IS a destroy.
        let current_no_failed = event_snapshot_with(&["a.ics"]);
        let changes = diff_event_snapshots(&previous, &current_no_failed);
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
    fn sync_report_updates_snapshot_and_classifies_changes() {
        let mut snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: Some("old".to_string()),
                },
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/two.ics".to_string(),
                    etag: Some("same".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: Some("new".to_string()),
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
                    etag: Some("created".to_string()),
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
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/one.ics".to_string(),
                etag: Some("kept".to_string()),
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

    /// RFC 6578 s3.6: a truncated result must be drained with the partial
    /// token it came with, or every change past the truncation point is lost
    /// until etag drift happens to surface it.
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
            url: String::new(),
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
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("start".to_string()),
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
        assert_eq!(current.sync_token.as_deref(), Some("complete"));
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2, "the drain must re-issue the REPORT");
        assert!(requests[1].body.contains("partial"));
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
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/one.ics".to_string(),
                etag: Some("e".to_string()),
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
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/phantom.ics".to_string(),
                etag: Some("leaked".to_string()),
            }],
            failed_hrefs: Vec::new(),
        };

        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/task.ics".to_string(),
                    etag: Some("t1".to_string()),
                    status: Some(200),
                    non_vevent: true,
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/phantom.ics".to_string(),
                    etag: Some("leaked-2".to_string()),
                    status: Some(200),
                    non_vevent: true,
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/unlabelled.ics".to_string(),
                    etag: Some("u1".to_string()),
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
