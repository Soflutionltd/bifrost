use std::time::Duration;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, ChangeCursor, Checkpoint, CursorScope, ErrorScope,
    Fingerprint, FolderId, InventoryEntry, MembershipScope, ObjectId, ObjectType, PageBoundary,
    ServerVersion, SyncEvent,
};
use serde_json::Value;

use crate::client::Resource;
use crate::origin::AdmittedUrl;
use crate::types::{CONTACT_SELECT, MESSAGE_SELECT, ODataCollection};

use super::GraphAccount;
use super::cursor::{
    GraphCursorPayload, GraphPageMarker, encode_cursor, kind_for_scope, scope_for_kind,
};
use super::graph_error::{GraphErrorContext, cursor_error_to_account_error};

const EVENT_SELECT: &str = "\
id,subject,bodyPreview,start,end,isAllDay,location,organizer,\
attendees,webLink,iCalUId,categories,recurrence,showAs,\
responseStatus,isCancelled,changeKey";

pub(crate) fn inventory_stream(
    account: GraphAccount,
    scope: CursorScope,
) -> AccountStream<bifrost_types::InventoryEvent> {
    inventory_stream_from(account, scope, None)
}

/// The single acceptance condition for resuming a stored inventory page.
///
/// `Account::is_inventory_cursor` delegates to `resume_inventory_stream`, so
/// this is the single acceptance condition for both classification and resume.
pub(crate) fn resumable_inventory_payload(cursor: &ChangeCursor) -> Option<GraphCursorPayload> {
    let payload = super::cursor::decode_cursor(cursor).ok()?;
    (payload.inventory_in_progress && scope_matches_payload(&cursor.scope, &payload))
        .then_some(payload)
}

pub(crate) fn resume_inventory_stream(
    account: GraphAccount,
    cursor: ChangeCursor,
) -> Option<AccountStream<bifrost_types::InventoryEvent>> {
    let payload = resumable_inventory_payload(&cursor)?;
    Some(inventory_stream_from(account, cursor.scope, Some(payload)))
}

fn inventory_stream_from(
    account: GraphAccount,
    scope: CursorScope,
    resume: Option<GraphCursorPayload>,
) -> AccountStream<bifrost_types::InventoryEvent> {
    Box::pin(async_stream::stream! {
        let sync_ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
            .with_scope(ErrorScope::Cursor(scope.clone()));
        if !sync_scope_readable(&scope) {
            yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(
                super::cursor::CursorError::Unsupported, sync_ctx,
            ));
            return;
        }
        // Results this walk could not account for. Every checkpoint from the
        // first one onward declares the gap.
        let mut obligations: Vec<bifrost_types::InventoryObligation> = Vec::new();

        // One accumulator for the whole delta walk; each emitted page
        // takes and clears it, so a page reports the bytes of the
        // request that produced it rather than a running total.
        let client = match account.client_for_scope(&scope) {
            Ok(client) => client.clone(),
            Err(error) => {
                yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(
                    super::cursor::routing_error(error),
                    sync_ctx,
                ));
                return;
            }
        };
        let start = match resume {
            // The persisted page link is re-admitted onto this scope's
            // client: it is the consumer's durable state, and following a
            // link that does not admit would send the bearer wherever it
            // names.
            Some(payload) => match payload.advanced_through {
                Some(marker) => super::cursor::admit_resume_url(&client, &marker.next_link)
                    .map(|url| (url, payload.calendar_window_end)),
                None => {
                    yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(
                        // The envelope and payload are understood. The
                        // engine acknowledged no outer page marker, so this
                        // is a scope-local restart, not an account-wide
                        // schema reset.
                        super::cursor::CursorError::InventoryInProgress,
                        sync_ctx,
                    ));
                    return;
                }
            },
            None => match initial_delta_request(&account, &scope, jiff::Timestamp::now()) {
                Ok((path, window_end)) => match client.api_url(&path) {
                    Ok(url) => Ok((url, window_end)),
                    Err(error) => {
                        yield bifrost_types::InventoryEvent::Terminated(super::graph_error::into_account_error(error, sync_ctx));
                        return;
                    }
                },
                Err(error) => Err(error),
            },
        };
        let (mut current_url, calendar_window_end) = match start {
            Ok(start) => start,
            Err(error) => {
                yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(error, sync_ctx));
                return;
            }
        };
        let (client, tally) = client.metered();
        // A foreign (shared) scope tags every inventory item with its
        // owning mailbox so the consumer maps the item to its shared
        // owner and the foreign mailbox's native folder ids cannot be
        // conflated with the primary's in the membership index (the
        // A5c-established owner-tag pattern). `None` for a primary scope.
        let owner = account.owner_of_scope(&scope);
        let kind = match kind_for_scope(&scope) {
            Ok(kind) => kind,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(error, ctx));
                return;
            }
        };
        let mut walk = crate::paging::PageWalk::new("inventory delta");

        loop {
            if let Err(error) = walk.enter(&current_url) {
                yield bifrost_types::InventoryEvent::Terminated(super::graph_error::graph_shared_scope_error(
                    error, &scope, owner.as_ref(), sync_ctx.clone(),
                ));
                return;
            }
            let page: ODataCollection<Value> = match fetch_page(&client, &current_url).await {
                Ok(page) => page,
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                        .with_scope(ErrorScope::Cursor(scope.clone()));
                    // A permission denial on a foreign (shared) scope
                    // quarantines just this scope; a primary-scope
                    // denial stays terminal account-wide. `owner` is
                    // hoisted above the loop (it is scope-invariant).
                    yield bifrost_types::InventoryEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error,
                        &scope,
                        owner.as_ref(),
                        ctx,
                    ));
                    return;
                }
            };
            // Both links are admitted at receipt, before either is followed
            // or persisted into a checkpoint. A refused link ends the walk
            // before this page's entries are emitted, so no checkpoint moves.
            let links = client
                .admit_next(page.next_link.as_ref())
                .and_then(|next| Ok((next, client.admit_next(page.delta_link.as_ref())?)));
            let (next_link, delta_link) = match links {
                Ok(links) => links,
                Err(error) => {
                    yield bifrost_types::InventoryEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error, &scope, owner.as_ref(), sync_ctx.clone(),
                    ));
                    return;
                }
            };
            let mut entries = Vec::new();
            let mut etags = Vec::new();
            let mut removed_etag_ids = Vec::new();
            for value in page.value {
                if is_removed(&value) {
                    // InventoryEvent batches carry live InventoryEntry values,
                    // not per-object removals. A tombstone cannot be encoded
                    // as a live entry without asserting the deleted object is
                    // present; ScopeChange::Removed belongs to changes_stream.
                    // Forwarding inventory tombstones needs a shared event and
                    // engine contract that can represent them.
                    if is_mail_scope(&scope)
                        && let Some(id) = value.get("id").and_then(Value::as_str) {
                        removed_etag_ids.push(super::foreign::encode_message_id(&scope, id).0);
                    }
                    continue;
                }
                let Some(mut entry) = inventory_entry_from_value(&scope, &value) else {
                    // A non-removed value with no usable `id`. It used to be
                    // dropped silently while the delta link advanced past it,
                    // which violates the checkpoint contract: the walk cannot
                    // name, retry, or later reconcile the thing it skipped.
                    //
                    // It becomes a REGION obligation, not an object one, and
                    // that distinction is the honest part. An absent id may
                    // mean one malformed value, a schema mismatch affecting
                    // many, a truncated page, or a response that cannot be
                    // correlated with pagination at all - calling it a
                    // single-object loss would claim knowledge the walk does
                    // not have.
                    //
                    // It is a CHECKPOINT BARRIER, not a durable replay. The
                    // only token available at this granularity is the current
                    // page URL, which is a continuation of THIS delta session:
                    // once the walk takes its deltaLink and the cursor
                    // advances, that skip token is dead. Handing it over as a
                    // repair capability would record debt nothing can ever
                    // discharge - silent permanent loss wearing a declared-debt
                    // costume. Graph publishes no durable snapshot handle for a
                    // page of a delta enumeration, so the honest answer is that
                    // the cursor must not cross this page at all.
                    obligations.push(bifrost_types::InventoryObligation::Region {
                        // Identity is the page, not the failure class. The
                        // label alone names every id-less value in the scope
                        // forever, and an operator waiver keyed on that would
                        // silently accept all future ones.
                        key: bifrost_types::ObligationKey(
                            format!("graph:unidentifiable-value:{}", current_url.as_str()).into_bytes(),
                        ),
                        failure_label: format!("{scope:?}:unidentifiable-value"),
                        error: super::graph_error::unsupported_account_error(
                            AccountOperation::SyncInventory,
                        ),
                        recovery: bifrost_types::RegionRecovery::CheckpointBarrier {
                            transient_replay: Some(bifrost_types::TransientReplayHint {
                                token: current_url.as_str().as_bytes().to_vec(),
                                note: "delta-session page link; dies with the enumeration".into(),
                            }),
                        },
                    });
                    continue;
                };
                if is_mail_scope(&scope)
                    && let ServerVersion::ETag(etag) = &entry.fingerprint.server_version {
                    etags.push((entry.id.0.clone(), etag.clone()));
                }
                if let Some(owner) = &owner {
                    entry
                        .memberships
                        .push(bifrost_types::MembershipScope::Mailbox(owner.clone()));
                }
                entries.push(entry);
            }

            if !etags.is_empty() || !removed_etag_ids.is_empty() {
                let mut cache = account.etag_index.write().await;
                for (id, etag) in etags {
                    cache.insert(id, etag);
                }
                for id in removed_etag_ids {
                    cache.remove(&id);
                }
            }

            if let Some(next_link) = next_link {
                let checkpoint_cursor = match encode_cursor(
                    scope.clone(),
                    GraphCursorPayload::inventory_page(
                        kind.clone(),
                        page_marker(&next_link, entries.last().map(|entry| entry.id.0.clone())),
                        calendar_window_end,
                    ),
                ) {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                            .with_scope(ErrorScope::Cursor(scope.clone()));
                        yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(error, ctx));
                        return;
                    }
                };
                yield inventory_batch(entries, PageBoundary::Page, Some(checkpoint_cursor), coverage_of(&scope, &obligations), tally.take());
                current_url = next_link;
            } else if let Some(delta_link) = delta_link {
                let cursor = match encode_cursor(
                    scope.clone(),
                    GraphCursorPayload::new(kind.clone(), &delta_link, None)
                        .with_calendar_window_end_option(calendar_window_end),
                ) {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                            .with_scope(ErrorScope::Cursor(scope.clone()));
                        yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(error, ctx));
                        return;
                    }
                };
                let checkpoint = Checkpoint::Change(cursor.clone());
                yield inventory_batch(entries, PageBoundary::Final, Some(cursor), coverage_of(&scope, &obligations), tally.take());
                yield bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::new(Some(checkpoint), coverage_of(&scope, &obligations), bifrost_types::PartitionEnd::Exhausted));
                return;
            } else {
                // A delta page must advance with either a next link or a
                // delta link. Completing without a checkpoint would make
                // the engine restart inventory from page one forever.
                yield bifrost_types::InventoryEvent::Terminated(super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::ContractViolation,
                    AccountOperation::SyncInventory,
                    Some(ErrorScope::Cursor(scope.clone())),
                    "Graph delta page had neither @odata.nextLink nor @odata.deltaLink",
                ));
                return;
            }
        }
    })
}

pub(crate) async fn fetch_delta_page(
    client: &crate::client::GraphClient,
    url: &AdmittedUrl,
) -> Result<ODataCollection<Value>, crate::error::GraphError> {
    fetch_page(client, url).await
}

pub(crate) fn inventory_entry_from_value(
    scope: &CursorScope,
    value: &Value,
) -> Option<InventoryEntry> {
    let id = value.get("id")?.as_str()?.to_string();
    let membership = membership_from_value(scope, value);
    let etag = graph_etag(value);
    let fingerprint = Fingerprint {
        server_version: etag
            .map(ServerVersion::ETag)
            .unwrap_or(ServerVersion::Unavailable),
        size: None,
        flags_hash: flags_hash(value),
    };
    let message_id = internet_header(value, "Message-ID").or_else(|| {
        value
            .get("internetMessageId")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    let references = internet_header(value, "References")
        .map(|header| header.split_whitespace().map(str::to_string).collect())
        .unwrap_or_default();
    let in_reply_to = internet_header(value, "In-Reply-To");
    let thread_id = value
        .get("conversationId")
        .and_then(Value::as_str)
        .map(|thread| super::foreign::encode_thread_id(scope, thread));

    Some(InventoryEntry {
        id: sync_object_id(scope, &id),
        memberships: vec![membership],
        size: None,
        blob_id: None,
        fingerprint,
        thread_id,
        message_id,
        references,
        in_reply_to,
    })
}

pub(crate) fn membership_from_value(scope: &CursorScope, value: &Value) -> MembershipScope {
    let fallback = match scope {
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => {
            super::foreign::parse_folder(folder).native_id().to_string()
        }
        _ => String::new(),
    };
    let native = value
        .get("parentFolderId")
        .and_then(Value::as_str)
        .filter(|folder| !folder.is_empty())
        .unwrap_or(&fallback);
    // For a foreign (shared/delegate) scope the item's `parentFolderId`
    // is a *native* id within the owning mailbox. Discovery emits the
    // folder membership foreign-encoded (`encode_foreign(mailbox,
    // native)`), so emit the same encoding here - otherwise the engine's
    // covering rule (folder-typed cursor covers the matching folder)
    // cannot reconcile inventory/changes against discovery, and a foreign
    // native id colliding with a primary id (e.g. `inbox`) would conflate
    // into the primary mailbox's membership set.
    let folder = match scope {
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => {
            match super::foreign::parse_folder(folder).foreign() {
                Some(foreign) => super::foreign::encode_foreign(&foreign.mailbox, native),
                None => FolderId(native.to_string()),
            }
        }
        _ => FolderId(native.to_string()),
    };
    MembershipScope::Folder(folder)
}

pub(crate) fn is_mail_scope(scope: &CursorScope) -> bool {
    matches!(
        scope,
        CursorScope::FolderType {
            ty: ObjectType::Email,
            ..
        }
    )
}

pub(crate) fn sync_scope_readable(scope: &CursorScope) -> bool {
    // Typed contact/calendar reads currently use the primary client. Do not
    // emit shared non-mail ids that those doors would read in another mailbox.
    !matches!(scope, CursorScope::FolderType { folder, ty }
        if *ty != ObjectType::Email && super::foreign::parse_folder(folder).foreign().is_some())
}

pub(crate) fn sync_object_id(scope: &CursorScope, id: &str) -> ObjectId {
    match scope {
        CursorScope::FolderType {
            folder,
            ty: ObjectType::Event | ObjectType::CalendarEvent,
        } => ObjectId(super::calendar::join_event_id(&folder.0, id)),
        CursorScope::FolderType {
            ty: ObjectType::Contact,
            ..
        } => ObjectId(id.to_string()),
        _ => super::foreign::encode_message_id(scope, id),
    }
}

pub(crate) fn graph_etag(value: &Value) -> Option<String> {
    // The OData annotation is already an HTTP entity-tag. A change key is
    // only its opaque payload, so never send it bare in If-Match.
    value
        .get("@odata.etag")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            value
                .get("changeKey")
                .and_then(Value::as_str)
                .map(graph_change_key_etag)
        })
}

/// Turn Graph's opaque change key into the weak HTTP entity-tag used by
/// `If-Match` when the provider omitted its `@odata.etag` annotation.
pub(crate) fn graph_change_key_etag(change_key: &str) -> String {
    format!("W/\"{change_key}\"")
}

pub(crate) fn is_removed(value: &Value) -> bool {
    value.get("@removed").is_some()
}

pub(crate) fn removed_id(value: &Value) -> Option<ObjectId> {
    value
        .get("id")
        .and_then(Value::as_str)
        .map(|id| ObjectId(id.to_string()))
}

/// A `calendarView` window bound, `days` from `now`, in the second-precision
/// UTC form Graph expects. A bound that runs off the representable range
/// falls back to `now` rather than widening the window unpredictably.
fn calendar_view_bound(now: jiff::Timestamp, days: i64) -> String {
    format_calendar_timestamp(calendar_view_timestamp(now, days))
}

/// A page marker names a resumable page, so it takes an admitted link: an
/// unadmitted server link cannot be persisted.
pub(crate) fn page_marker(
    next_link: &AdmittedUrl,
    last_seen_id: Option<String>,
) -> GraphPageMarker {
    GraphPageMarker {
        next_link: next_link.as_str().to_string(),
        last_seen_id,
    }
}

#[cfg(test)]
pub(crate) fn initial_delta_url(
    account: &GraphAccount,
    scope: &CursorScope,
) -> Result<String, super::cursor::CursorError> {
    initial_delta_request(account, scope, jiff::Timestamp::now()).map(|(url, _)| url)
}

fn initial_delta_request(
    account: &GraphAccount,
    scope: &CursorScope,
    now: jiff::Timestamp,
) -> Result<(String, Option<i64>), super::cursor::CursorError> {
    // Route the prefix through the scope's owning client (primary or a
    // shared mailbox) and use the *native* folder id in the path - the
    // foreign-mailbox prefix lives in the URL's `/users/{id}` segment,
    // not in the `/mailFolders/{id}` segment.
    let prefix = account
        .client_for_scope(scope)
        .map_err(super::cursor::routing_error)?
        .api_path_prefix();
    match scope {
        CursorScope::FolderType { folder, ty } => {
            let native = super::foreign::parse_folder(folder).native_id().to_string();
            let encoded = bifrost_net::url::encode_path_component(&native);
            match ty {
                ObjectType::Email => Ok((
                    format!(
                        "{prefix}/mailFolders/{encoded}/messages/delta?$select={MESSAGE_SELECT}&$top=50"
                    ),
                    None,
                )),
                ObjectType::Event | ObjectType::CalendarEvent => {
                    let start = calendar_view_bound(now, -90);
                    let end_at = calendar_view_timestamp(now, 365);
                    let end = format_calendar_timestamp(end_at);
                    Ok((
                        format!(
                            "{prefix}/calendars/{encoded}/calendarView/delta?startDateTime={start}&endDateTime={end}&$select={EVENT_SELECT}"
                        ),
                        Some(end_at.as_second()),
                    ))
                }
                ObjectType::Contact => Ok((
                    format!(
                        "{prefix}/contactFolders/{encoded}/contacts/delta?$select={CONTACT_SELECT}&$top=250"
                    ),
                    None,
                )),
                _ => Err(super::cursor::CursorError::Unsupported),
            }
        }
        _ => Err(super::cursor::CursorError::Unsupported),
    }
}

fn calendar_view_timestamp(now: jiff::Timestamp, days: i64) -> jiff::Timestamp {
    // A Timestamp is an absolute UTC instant, so its arithmetic accepts fixed
    // durations rather than calendar-day spans. Treat these UTC window bounds
    // as exact 24-hour days; adding a Span with a non-zero `days` component
    // fails and silently falls back to `now`, producing a zero-width window.
    let seconds = days.saturating_mul(24 * 60 * 60);
    now.checked_add(jiff::SignedDuration::from_secs(seconds))
        .unwrap_or(now)
}

fn format_calendar_timestamp(at: jiff::Timestamp) -> String {
    jiff::tz::Offset::UTC
        .to_datetime(at)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// Inventory page carrying the coverage it advances across.
///
/// Separate from the generic `batch` helper because that one is also used for
/// change streams, where coverage is meaningless.
pub(crate) fn inventory_batch(
    items: Vec<InventoryEntry>,
    page_boundary: PageBoundary,
    cursor: Option<ChangeCursor>,
    coverage: bifrost_types::InventoryCoverageReport,
    bytes_in: u64,
) -> bifrost_types::InventoryEvent {
    bifrost_types::InventoryEvent::Batch(bifrost_types::InventoryBatch {
        items,
        page_boundary,
        server_latency: Duration::default(),
        bytes_in,
        checkpoint: bifrost_types::PageCheckpoint::new(cursor.map(Checkpoint::Change), &coverage),
        coverage,
    })
}

/// Coverage for a checkpoint emitted after `obligations` were observed.
///
/// The domain is the whole scope: a Graph delta enumeration walks the scope in
/// one logical pass, paging through it, rather than serving engine-chosen
/// partitions.
pub(crate) fn coverage_of(
    scope: &CursorScope,
    obligations: &[bifrost_types::InventoryObligation],
) -> bifrost_types::InventoryCoverageReport {
    bifrost_types::InventoryCoverageReport::from_obligations(
        bifrost_types::CoverageDomain::full(scope.clone()),
        obligations,
    )
}

pub(crate) fn batch<T>(
    items: Vec<T>,
    page_boundary: PageBoundary,
    cursor: Option<ChangeCursor>,
    bytes_in: u64,
) -> SyncEvent<T> {
    SyncEvent::Batch(Batch {
        items,
        page_boundary,
        server_latency: Duration::default(),
        bytes_in,
        checkpoint: cursor.map(Checkpoint::Change),
    })
}

pub(crate) fn scope_matches_payload(scope: &CursorScope, payload: &GraphCursorPayload) -> bool {
    let canonical = scope_for_kind(&payload.kind);
    *scope == canonical || event_aliases(scope, &canonical)
}

fn event_aliases(a: &CursorScope, b: &CursorScope) -> bool {
    let (
        CursorScope::FolderType { folder: fa, ty: ta },
        CursorScope::FolderType { folder: fb, ty: tb },
    ) = (a, b)
    else {
        return false;
    };
    let aliased = matches!(
        (ta, tb),
        (ObjectType::Event, ObjectType::CalendarEvent)
            | (ObjectType::CalendarEvent, ObjectType::Event)
    );
    aliased && fa == fb
}

async fn fetch_page(
    client: &crate::client::GraphClient,
    url: &AdmittedUrl,
) -> Result<ODataCollection<Value>, crate::error::GraphError> {
    client.get(Resource::Outlook, url).await
}

fn internet_header(value: &Value, name: &str) -> Option<String> {
    let headers = value.get("internetMessageHeaders")?.as_array()?;
    headers.iter().find_map(|header| {
        let header_name = header.get("name")?.as_str()?;
        header_name.eq_ignore_ascii_case(name).then(|| {
            header
                .get("value")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        })
    })
}

fn flags_hash(value: &Value) -> u64 {
    let is_read = value.get("isRead").and_then(Value::as_bool);
    let status = value
        .get("flag")
        .and_then(|flag| flag.get("flagStatus"))
        .and_then(Value::as_str);
    let categories = value
        .get("categories")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    tracked_flags_hash(is_read, status, categories)
}

pub(crate) fn tracked_flags_hash<'a>(
    is_read: Option<bool>,
    flag_status: Option<&str>,
    categories: impl IntoIterator<Item = &'a str>,
) -> u64 {
    let mut flags = Vec::new();
    if let Some(is_read) = is_read {
        flags.push(format!("isread={is_read}"));
    }
    if let Some(status) = flag_status {
        flags.push(format!("flag={}", status.to_ascii_lowercase()));
    }
    flags.extend(
        categories
            .into_iter()
            .map(|category| format!("category={}", category.to_ascii_lowercase())),
    );
    bifrost_types::canonical_flags_hash(flags)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bifrost_types::QueryId;
    use futures::StreamExt;
    use serde_json::json;

    use crate::client::GraphClient;

    use super::super::PushMode;
    use super::*;
    use crate::client::ScriptedRestResponse;

    fn test_account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
    }

    #[test]
    fn initial_delta_url_supports_email_scope() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("email scope is supported");
        assert!(url.starts_with("/me/mailFolders/inbox/messages/delta?"));
    }

    #[test]
    fn client_for_scope_routes_foreign_prefix() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let foreign_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        let primary_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        // The foreign scope routes through the shared client, whose
        // prefix targets `/users/{mailbox}`.
        assert_eq!(
            account
                .client_for_scope(&foreign_scope)
                .expect("configured")
                .api_path_prefix(),
            "/users/shared%40contoso.com"
        );
        // A primary scope stays on `/me`.
        assert_eq!(
            account
                .client_for_scope(&primary_scope)
                .expect("primary")
                .api_path_prefix(),
            "/me"
        );
        let unconfigured = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("other@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        assert!(matches!(
            account.client_for_scope(&unconfigured),
            Err(crate::error::GraphError::Configuration { .. })
        ));
    }

    /// The engine-facing half of that rejection: a persisted scope whose
    /// shared mailbox left the configuration must quarantine ONE scope, not
    /// escalate. `ScopeRevoked` carrying `ErrorScope::Cursor` derives
    /// `DisableScope`; the same kind without a scope would derive
    /// `RestartAccount` and re-run discovery for every other mailbox over a
    /// condition discovery cannot fix.
    ///
    /// Hermetic: the scope is refused before the first delta request.
    #[tokio::test]
    async fn a_stale_foreign_scope_disables_only_itself() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let stale = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("gone@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        let mut stream = inventory_stream(account, stale.clone());
        let bifrost_types::InventoryEvent::Terminated(error) =
            stream.next().await.expect("an event")
        else {
            panic!("expected InventoryEvent::Terminated");
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::ScopeRevoked
            )
        ));
        assert_eq!(error.scope(), Some(&ErrorScope::Cursor(stale.clone())));
        match error.recovery() {
            bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::DisableScope(
                scope,
            )) => assert_eq!(scope, &stale),
            other => panic!("expected DisableScope, got {other:?}"),
        }
        assert!(stream.next().await.is_none(), "Terminated ends the walk");
    }

    #[tokio::test]
    async fn inventory_walks_the_absolute_next_link_and_rejects_a_page_without_either_link() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{"id": "one", "changeKey": "ck-one"}],
                    "@odata.nextLink": "https://graph.microsoft.com/v1.0/next?page=2"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{"id": "two", "changeKey": "ck-two"}]
                }),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let mut stream = inventory_stream(account, scope);
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Batch(_))
        ));
        let bifrost_types::InventoryEvent::Terminated(error) =
            stream.next().await.expect("neither-link failure")
        else {
            panic!("expected termination")
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        ));
        assert!(stream.next().await.is_none(), "Terminated ends the walk");
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .url
                .contains("/me/mailFolders/inbox/messages/delta?")
        );
        assert_eq!(
            requests[1].url,
            "https://graph.microsoft.com/v1.0/next?page=2"
        );
    }

    /// A delta server that hands back a `nextLink` the walk has already
    /// fetched would spin the inventory forever - `PageWalk` must refuse
    /// the repeat and project the refusal as `Terminated`, exactly like the
    /// folder-list walk (`a_repeated_next_link_refuses_the_folder_walk`).
    #[tokio::test]
    async fn a_repeated_next_link_refuses_the_inventory_delta_walk() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{"id": "one", "changeKey": "ck-one"}],
                    "@odata.nextLink": "https://graph.microsoft.com/v1.0/next?page=2"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{"id": "two", "changeKey": "ck-two"}],
                    "@odata.nextLink": "https://graph.microsoft.com/v1.0/next?page=2"
                }),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let mut stream = inventory_stream(account, scope);
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Batch(_))
        ));
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Batch(_))
        ));
        let bifrost_types::InventoryEvent::Terminated(error) =
            stream.next().await.expect("repeated-link refusal")
        else {
            panic!("expected termination")
        };
        // A repeated link is a semantic breach of the paging contract by a
        // response that parsed fine, not a parse failure.
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        ));
        assert!(stream.next().await.is_none(), "Terminated ends the walk");
        let requests = client.take_rest_requests();
        assert_eq!(
            requests.len(),
            2,
            "the repeated link is refused before a third fetch"
        );
    }

    #[tokio::test]
    async fn an_acked_inventory_page_resumes_at_its_next_link() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let first_client = GraphClient::new("token");
        first_client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{"id": "one", "changeKey": "ck-one"}],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/next?page=2"
            }),
        )]);
        let first = GraphAccount::new_for_tests(first_client, PushMode::GraphSubscriptions);
        let mut stream = inventory_stream(first, scope.clone());
        let bifrost_types::InventoryEvent::Batch(batch) = stream.next().await.expect("first page")
        else {
            panic!("expected first inventory batch");
        };
        let bifrost_types::PageCheckpoint::Advance(Checkpoint::Change(cursor)) = batch.checkpoint
        else {
            panic!("mid-inventory page must checkpoint");
        };

        let resumed_client = GraphClient::new("token");
        resumed_client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{"id": "two", "changeKey": "ck-two"}],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta"
            }),
        )]);
        let resumed =
            GraphAccount::new_for_tests(resumed_client.clone(), PushMode::GraphSubscriptions);
        let mut stream =
            resume_inventory_stream(resumed, cursor).expect("Graph inventory cursor resumes");
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Batch(_))
        ));
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Done(_))
        ));
        let requests = resumed_client.take_rest_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url,
            "https://graph.microsoft.com/v1.0/next?page=2"
        );
    }

    #[tokio::test]
    async fn inventory_tombstone_evicts_its_cached_etag() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{"id":"gone", "@removed":{"reason":"deleted"}}],
                "@odata.deltaLink":"https://graph.microsoft.com/v1.0/delta"
            }),
        )]);
        let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
        account
            .etag_index
            .write()
            .await
            .insert("gone".to_string(), "old".to_string());
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let mut stream = inventory_stream(account.clone(), scope);
        let Some(bifrost_types::InventoryEvent::Batch(batch)) = stream.next().await else {
            panic!("the tombstone page still completes inventory");
        };
        assert!(
            batch.items.is_empty(),
            "a tombstone is not a live inventory entry"
        );
        let _ = stream.next().await;
        assert_eq!(account.etag_index.write().await.get("gone"), None);
    }

    #[tokio::test]
    async fn an_inventory_cursor_without_its_outer_marker_restarts_only_its_scope() {
        let client = GraphClient::new("token");
        client.script_rest([]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::inventory_page(
            kind_for_scope(&scope).expect("email scope maps"),
            GraphPageMarker {
                next_link: "https://graph.microsoft.com/v1.0/next?page=2".to_string(),
                last_seen_id: Some("m1".to_string()),
            },
            None,
        );
        let mut cursor = encode_cursor(scope.clone(), payload).expect("cursor encodes");
        // The outer field is authoritative even when the payload copy has a
        // marker: an unacknowledged page position must not be resumed.
        cursor.advanced_through = None;

        let mut stream = resume_inventory_stream(account, cursor).expect("inventory cursor");
        let Some(bifrost_types::InventoryEvent::Terminated(error)) = stream.next().await else {
            panic!("a cleared in-progress cursor must terminate for scoped restart");
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::CursorInvalid
            )
        ));
        assert!(matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::Engine(
                bifrost_types::EngineDirective::RestartScope(restarted)
            ) if restarted == &scope
        ));
        assert_eq!(client.wire_attempts(), 0, "no cursor URL is followed");
    }

    #[test]
    fn initial_delta_url_uses_shared_mailbox_prefix() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("foreign email scope is supported");
        // The mailbox rides in the `/users/{id}` segment; the native
        // folder id (the foreign prefix stripped) rides in
        // `/mailFolders/{id}`.
        assert!(
            url.starts_with("/users/shared%40contoso.com/mailFolders/AAMk/messages/delta?"),
            "unexpected url: {url}"
        );
    }

    /// Every page of a foreign inventory walk rides the OWNING mailbox's
    /// client, including the absolute `@odata.nextLink` continuation.
    ///
    /// The primary client is armed with an empty script, so a walk that
    /// falls back to it panics at the offending request rather than
    /// answering from a queue it shares with the shared client. Asserting
    /// the URL alone would prove nothing here: `initial_delta_url` has
    /// always built the `/users/{mailbox}` prefix off `client_for_scope`,
    /// and the continuation URL is whatever Graph minted, so both are
    /// identical whichever client carries them.
    #[tokio::test]
    async fn every_page_of_a_foreign_inventory_walk_rides_the_owner_client() {
        let primary = GraphClient::new("primary-token");
        // Armed, empty: the primary must not be asked for anything.
        primary.script_rest([]);
        let shared = GraphClient::new("shared-token").for_shared_mailbox("shared@contoso.com");
        shared.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{ "id": "m1", "changeKey": "ck1" }],
                    "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta?$skiptoken=p2"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [],
                    "@odata.deltaLink": "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta?$deltatoken=d1"
                }),
            ),
        ]);
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary.clone(),
            PushMode::GraphSubscriptions,
            HashMap::from([("shared@contoso.com".to_string(), shared.clone())]),
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };

        let mut stream = inventory_stream(account, scope);
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Batch(_))
        ));
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Batch(_))
        ));
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::InventoryEvent::Done(_))
        ));

        let requests = shared.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .url
                .contains("/users/shared%40contoso.com/mailFolders/AAMk/messages/delta?"),
            "unexpected first URL: {}",
            requests[0].url,
        );
        assert_eq!(
            requests[1].url,
            "https://graph.microsoft.com/v1.0/users/shared%40contoso.com/delta?$skiptoken=p2"
        );
        assert!(
            primary.take_rest_requests().is_empty(),
            "the primary client must not see a foreign-scope request"
        );
    }

    #[test]
    fn initial_delta_url_supports_event_scope() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let now = jiff::Timestamp::new(1_700_000_000, 0).expect("fixed test timestamp");
        let (url, end) =
            initial_delta_request(&account, &scope, now).expect("event scope is supported");
        assert!(url.starts_with("/me/calendars/calendar/calendarView/delta?"));
        assert_eq!(
            end,
            Some(
                now.checked_add(jiff::SignedDuration::from_secs(365 * 24 * 60 * 60))
                    .expect("one year is representable")
                    .as_second()
            ),
            "the stored calendar horizon must match the URL's future end bound"
        );
    }

    #[test]
    fn initial_delta_url_supports_contact_scope() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        };
        let url = initial_delta_url(&account, &scope).expect("contact scope is supported");
        assert!(url.starts_with("/me/contactFolders/contacts/contacts/delta?"));
    }

    #[test]
    fn initial_delta_url_rejects_account_scope() {
        let account = test_account();
        assert!(initial_delta_url(&account, &CursorScope::Account).is_err());
    }

    #[test]
    fn initial_delta_url_rejects_query_scope() {
        let account = test_account();
        let scope = CursorScope::Query(QueryId("q1".to_string()));
        assert!(initial_delta_url(&account, &scope).is_err());
    }

    #[test]
    fn initial_delta_url_rejects_bare_folder_scope() {
        let account = test_account();
        let scope = CursorScope::Folder(FolderId("inbox".to_string()));
        assert!(initial_delta_url(&account, &scope).is_err());
    }

    #[test]
    fn initial_delta_url_rejects_unsupported_object_type() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Mailbox,
        };
        assert!(initial_delta_url(&account, &scope).is_err());
    }

    #[test]
    fn scope_matches_payload_treats_event_and_calendar_event_as_aliases() {
        let event_scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let calendar_event_scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::CalendarEvent,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&calendar_event_scope).expect("calendar event scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );

        assert!(scope_matches_payload(&calendar_event_scope, &payload));
        assert!(scope_matches_payload(&event_scope, &payload));
    }

    #[test]
    fn scope_matches_payload_rejects_unrelated_object_types() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let other_scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&other_scope).expect("event scope maps"),
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );

        assert!(!scope_matches_payload(&scope, &payload));
    }

    #[test]
    fn scope_matches_payload_rejects_aliased_object_types_for_different_folders() {
        use super::super::cursor::GraphCursorKind;

        let payload = GraphCursorPayload::new(
            GraphCursorKind::Events {
                calendar_id: "calendar-a".to_string(),
            },
            &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar-b".to_string()),
            ty: ObjectType::CalendarEvent,
        };
        assert!(!scope_matches_payload(&scope, &payload));
    }

    #[tokio::test]
    async fn inventory_rejects_account_scope_with_fatal() {
        let account = test_account();
        let mut stream = inventory_stream(account, CursorScope::Account);
        let first = stream.next().await.expect("fatal event");
        assert!(matches!(
            first,
            bifrost_types::InventoryEvent::Terminated(_)
        ));
        assert!(stream.next().await.is_none(), "Terminated ends the walk");
    }

    #[tokio::test]
    async fn inventory_rejects_query_scope_with_fatal() {
        let account = test_account();
        let scope = CursorScope::Query(QueryId("q1".to_string()));
        let mut stream = inventory_stream(account, scope);
        let first = stream.next().await.expect("fatal event");
        assert!(matches!(
            first,
            bifrost_types::InventoryEvent::Terminated(_)
        ));
        assert!(stream.next().await.is_none(), "Terminated ends the walk");
    }

    #[tokio::test]
    async fn inventory_rejects_unsupported_folder_type_with_fatal() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Mailbox,
        };
        let mut stream = inventory_stream(account, scope);
        let first = stream.next().await.expect("fatal event");
        assert!(matches!(
            first,
            bifrost_types::InventoryEvent::Terminated(_)
        ));
    }

    #[test]
    fn membership_scope_uses_parent_folder_id() {
        let scope = CursorScope::FolderType {
            folder: FolderId("fallback".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "parentFolderId": "actual-parent"
        });
        assert_eq!(
            membership_from_value(&scope, &value),
            MembershipScope::Folder(FolderId("actual-parent".to_string()))
        );
    }

    #[test]
    fn membership_scope_foreign_encodes_for_foreign_scope() {
        // A foreign (shared) scope must emit the parentFolderId
        // foreign-encoded so it reconciles with discovery
        // (`Folder(encode_foreign(mailbox, native))`) and a foreign
        // native id colliding with a primary id cannot conflate into the
        // primary mailbox's membership set.
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkRoot"),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "parentFolderId": "inbox"
        });
        assert_eq!(
            membership_from_value(&scope, &value),
            MembershipScope::Folder(super::super::foreign::encode_foreign(
                "shared@contoso.com",
                "inbox"
            ))
        );
    }

    #[test]
    fn membership_scope_primary_stays_native() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({ "id": "m1", "parentFolderId": "inbox" });
        assert_eq!(
            membership_from_value(&scope, &value),
            MembershipScope::Folder(FolderId("inbox".to_string()))
        );
    }

    #[test]
    fn inventory_entry_id_encodes_foreign_scope_and_leaves_primary_bare() {
        // Foreign scope: the entry id carries the owning mailbox so later
        // hydration / blob reads route to `/users/{owner}`.
        let foreign_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let value = json!({ "id": "AAMkmsg", "changeKey": "ck1" });
        let entry = inventory_entry_from_value(&foreign_scope, &value).expect("entry");
        assert_eq!(entry.id.0, "shared@contoso.com\u{1f}AAMkmsg");

        // Primary scope: the id stays bare. One encoding per logical item.
        let primary_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let entry = inventory_entry_from_value(&primary_scope, &value).expect("entry");
        assert_eq!(entry.id.0, "AAMkmsg");
    }

    #[test]
    fn inventory_entry_thread_id_encodes_foreign_scope() {
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let entry = inventory_entry_from_value(
            &scope,
            &json!({ "id": "AAMkmsg", "conversationId": "conversation-1" }),
        )
        .expect("entry");
        assert_eq!(
            entry.thread_id.expect("thread").0,
            "shared@contoso.com\u{1f}conversation-1"
        );
    }

    #[test]
    fn membership_falls_back_to_the_scope_folder_when_parent_is_absent() {
        // A delta `@removed` tombstone carries only `{id, @removed}`, so the
        // fallback is the only membership source for a deletion.
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        for value in [
            json!({ "id": "m1" }),
            json!({ "id": "m1", "parentFolderId": "" }),
            json!({ "id": "m1", "@removed": { "reason": "deleted" } }),
        ] {
            assert_eq!(
                membership_from_value(&scope, &value),
                MembershipScope::Folder(FolderId("inbox".to_string()))
            );
        }
    }

    #[test]
    fn foreign_scope_membership_uses_the_native_fallback_when_parent_is_absent() {
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkRoot"),
            ty: ObjectType::Email,
        };
        let tombstone = json!({ "id": "m1", "@removed": { "reason": "deleted" } });

        assert_eq!(
            membership_from_value(&scope, &tombstone),
            MembershipScope::Folder(super::super::foreign::encode_foreign(
                "shared@contoso.com",
                "AAMkRoot"
            ))
        );
    }

    #[tokio::test]
    async fn non_mail_sync_ids_match_typed_doors_and_leave_mail_cache_alone() {
        for ty in [
            ObjectType::Event,
            ObjectType::CalendarEvent,
            ObjectType::Contact,
        ] {
            let client = GraphClient::new("token");
            let page = json!({
                "value": [{"id": "item", "changeKey": "CK1"},
                    {"id": "item", "@removed": {"reason": "deleted"}}],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta"
            });
            client.script_rest([
                ScriptedRestResponse::json(reqwest::StatusCode::OK, page.clone()),
                ScriptedRestResponse::json(reqwest::StatusCode::OK, page),
            ]);
            let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
            account
                .etag_index
                .write()
                .await
                .insert("item".into(), "mail-version".into());
            let scope = CursorScope::FolderType {
                folder: FolderId("collection".into()),
                ty,
            };
            let events: Vec<_> = inventory_stream(account.clone(), scope).collect().await;
            let batch = events
                .into_iter()
                .find_map(|event| match event {
                    bifrost_types::InventoryEvent::Batch(batch) => Some(batch),
                    _ => None,
                })
                .expect("inventory batch");
            let expected = if ty == ObjectType::Contact {
                "item"
            } else {
                "collection::item"
            };
            assert_eq!(batch.items[0].id.0, expected);
            let bifrost_types::PageCheckpoint::Advance(Checkpoint::Change(cursor)) =
                batch.checkpoint
            else {
                panic!("checkpoint");
            };
            let payload =
                super::super::cursor::decode_cursor(&cursor).expect("inventory cursor decodes");
            assert!(
                !payload.inventory_in_progress,
                "unexpected inventory cursor: {payload:?}"
            );
            assert!(
                super::super::inventory::scope_matches_payload(&cursor.scope, &payload),
                "cursor scope and payload differ: {payload:?}"
            );
            let changes: Vec<_> = super::super::changes::changes_stream(account.clone(), cursor)
                .collect()
                .await;
            let ids: Vec<_> = changes
                .iter()
                .filter_map(|event| match event {
                    SyncEvent::Batch(batch) => Some(&batch.items),
                    _ => None,
                })
                .flatten()
                .map(|change| match change {
                    bifrost_types::Change::ObjectChange(change) => change.id.clone(),
                    bifrost_types::Change::ScopeChange(change) => change.id.clone(),
                    _ => panic!("unexpected change"),
                })
                .collect();
            let event_summary: Vec<_> = changes
                .iter()
                .map(|event| match event {
                    SyncEvent::Batch(batch) => {
                        format!(
                            "batch(items={}, boundary={:?})",
                            batch.items.len(),
                            batch.page_boundary
                        )
                    }
                    SyncEvent::Terminated(error) => {
                        format!("terminated({error:?})")
                    }
                    other => format!("{other:?}"),
                })
                .collect();
            assert!(!ids.is_empty(), "changes stream events: {event_summary:?}");
            assert!(ids.iter().all(|id| id.0 == expected));
            assert_eq!(
                account.etag_index.write().await.get("item").as_deref(),
                Some("mail-version")
            );
        }
    }

    #[tokio::test]
    async fn foreign_non_mail_scopes_have_no_typed_read_route() {
        for ty in [
            ObjectType::Event,
            ObjectType::CalendarEvent,
            ObjectType::Contact,
        ] {
            let scope = CursorScope::FolderType {
                folder: super::super::foreign::encode_foreign("shared@example.com", "collection"),
                ty,
            };
            assert!(!sync_scope_readable(&scope));
            let client = GraphClient::new("token");
            client.script_rest([]);
            let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
            let events: Vec<_> = inventory_stream(account.clone(), scope.clone())
                .collect()
                .await;
            assert!(matches!(
                events.as_slice(),
                [bifrost_types::InventoryEvent::Terminated(_)]
            ));
            let payload = GraphCursorPayload::new(
                kind_for_scope(&scope).expect("kind"),
                &crate::origin::AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
                None,
            );
            let cursor = encode_cursor(scope, payload).expect("cursor");
            let events: Vec<_> = super::super::changes::changes_stream(account, cursor)
                .collect()
                .await;
            assert!(matches!(events.as_slice(), [SyncEvent::Terminated(_)]));
            assert!(client.take_rest_requests().is_empty());
        }
        assert_eq!(
            graph_etag(&json!({"changeKey": "CK1"})).as_deref(),
            Some("W/\"CK1\"")
        );
    }

    #[test]
    fn graph_etag_prefers_the_http_entity_tag() {
        assert_eq!(
            graph_etag(&json!({ "changeKey": "CK1", "@odata.etag": "W/\"E1\"" })).as_deref(),
            Some("W/\"E1\"")
        );
        assert_eq!(
            graph_etag(&json!({ "@odata.etag": "W/\"E1\"" })).as_deref(),
            Some("W/\"E1\"")
        );
        assert_eq!(graph_etag(&json!({ "id": "m1" })), None);
    }

    #[test]
    fn removed_tombstones_are_detected_and_keep_their_id() {
        let removed = json!({ "id": "m1", "@removed": { "reason": "deleted" } });
        assert!(is_removed(&removed));
        assert_eq!(removed_id(&removed), Some(ObjectId("m1".to_string())));
        assert!(!is_removed(&json!({ "id": "m1" })));
        assert_eq!(removed_id(&json!({ "@removed": {} })), None);
    }

    #[test]
    fn internet_message_id_falls_back_to_the_odata_property() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "internetMessageId": "<fallback@example.test>"
        });
        let entry = inventory_entry_from_value(&scope, &value).expect("entry");
        assert_eq!(entry.message_id.as_deref(), Some("<fallback@example.test>"));

        // The header wins when both are present.
        let both = json!({
            "id": "m1",
            "internetMessageId": "<fallback@example.test>",
            "internetMessageHeaders": [
                { "name": "message-id", "value": "<header@example.test>" }
            ]
        });
        let entry = inventory_entry_from_value(&scope, &both).expect("entry");
        assert_eq!(entry.message_id.as_deref(), Some("<header@example.test>"));
    }

    #[test]
    fn missing_change_key_projects_server_version_unavailable() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let entry =
            inventory_entry_from_value(&scope, &json!({ "id": "m1" })).expect("entry expected");
        assert!(matches!(
            entry.fingerprint.server_version,
            ServerVersion::Unavailable
        ));
    }

    /// A value with no usable id yields no ENTRY - it cannot, there is nothing
    /// to key it by.
    ///
    /// What that must NOT mean is that the walk forgets it. The caller turns
    /// this `None` into a `Region` obligation so the delta link cannot advance
    /// past the value while claiming complete coverage; this test pins only the
    /// projection half. It previously read as `an_entry_without_an_id_is_dropped`
    /// and, together with the caller's silent `if let Some`, pinned the drop
    /// itself as correct behaviour.
    #[test]
    fn a_value_without_an_id_yields_no_entry() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        assert!(inventory_entry_from_value(&scope, &json!({ "changeKey": "CK1" })).is_none());
    }

    /// An id-less value poisons the page's coverage rather than vanishing.
    ///
    /// The old behaviour dropped it and advanced the delta link, which violates
    /// the checkpoint contract outright: nothing could name, retry, or later
    /// reconcile the skipped value, and the cursor moved past it for good.
    ///
    /// It is a `Region`, not an `Object`, and that is the honest classification:
    /// an absent id may mean one malformed value, a schema mismatch affecting
    /// many, or a truncated page, so claiming a single-object loss would assert
    /// knowledge the walk does not have.
    ///
    /// And it is a CHECKPOINT BARRIER, not a durable replay. The only token at
    /// page granularity is a continuation of this delta session, which dies
    /// when the walk takes its deltaLink - so recording it as repairable would
    /// create debt nothing can ever discharge. An earlier revision did exactly
    /// that, and this assertion is what stops it coming back.
    #[tokio::test]
    async fn an_id_less_value_becomes_a_barrier_region_obligation() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [
                    { "id": "m1", "changeKey": "CK1" },
                    { "changeKey": "CK2" },
                ],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta?token=final"
            }),
        )]);
        let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };

        let events: Vec<_> = inventory_stream(account, scope).collect().await;

        let completion = events
            .iter()
            .find_map(|event| match event {
                bifrost_types::InventoryEvent::Done(completion) => Some(completion),
                _ => None,
            })
            .expect("the walk must finish");
        assert!(
            !completion.coverage.is_complete(),
            "an id-less value must not be reported as complete coverage"
        );
        let obligations = completion.coverage.obligations();
        let region = obligations
            .iter()
            .find_map(|obligation| match obligation {
                bifrost_types::InventoryObligation::Region { recovery, key, .. } => {
                    Some((recovery, key))
                }
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!("an unidentifiable value is a region, not a named object: {obligations:?}")
            });
        assert!(
            region.0.is_barrier(),
            "a delta-session page link is not durable replay, so the cursor must not cross it"
        );
        assert!(
            completion.coverage.has_barrier(),
            "the report must surface the barrier so the engine can refuse the checkpoint"
        );
        // Identity is the page, not the failure class: a waiver keyed on
        // "this scope has id-less values" would accept every future one.
        let key = String::from_utf8_lossy(&region.1.0);
        assert!(
            key.contains("/messages/delta"),
            "the obligation key must name the specific page: {key}"
        );
    }

    #[test]
    fn flags_hash_is_stable_under_category_order_and_case() {
        let a = json!({ "isRead": true, "categories": ["Blue", "Red"] });
        let b = json!({ "isRead": true, "categories": ["red", "BLUE"] });
        assert_eq!(flags_hash(&a), flags_hash(&b));
    }

    #[test]
    fn flags_hash_moves_when_any_tracked_flag_moves() {
        let base = json!({ "isRead": true, "flag": { "flagStatus": "notFlagged" } });
        let read_cleared = json!({ "isRead": false, "flag": { "flagStatus": "notFlagged" } });
        let flagged = json!({ "isRead": true, "flag": { "flagStatus": "flagged" } });
        let categorized = json!({ "isRead": true, "flag": { "flagStatus": "notFlagged" }, "categories": ["Work"] });

        let base_hash = flags_hash(&base);
        assert_ne!(base_hash, flags_hash(&read_cleared));
        assert_ne!(base_hash, flags_hash(&flagged));
        assert_ne!(base_hash, flags_hash(&categorized));
    }

    #[test]
    fn flags_hash_ignores_untracked_fields() {
        // Subject / body churn must not look like a flag change, or every
        // edit would re-hydrate the message.
        let a = json!({ "isRead": true, "subject": "one" });
        let b = json!({ "isRead": true, "subject": "two" });
        assert_eq!(flags_hash(&a), flags_hash(&b));
    }

    #[test]
    fn the_message_select_carries_every_field_the_projection_reads() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("email scope is supported");
        // `parentFolderId` drives membership, `isRead`/`categories`/`flag`
        // drive the flags hash, `conversationId` the thread id, and
        // `internetMessageHeaders` the threading headers. Losing any of
        // them degrades sync silently rather than failing.
        for field in [
            "parentFolderId",
            "isRead",
            "categories",
            "flag",
            "conversationId",
            "internetMessageHeaders",
            "internetMessageId",
        ] {
            assert!(url.contains(field), "{field} missing from {url}");
        }
        assert!(url.contains("$top=50"));
        // `changeKey` is the documented concurrency token. The OData etag
        // remains a useful fallback, but inventory must request it directly.
        assert!(url.contains("changeKey"), "{url}");
    }

    #[test]
    fn initial_delta_url_percent_encodes_opaque_folder_ids() {
        let account = test_account();
        // Graph mail-folder ids are URL-safe base64 but can carry `=`
        // padding, which must not leak into the path unencoded.
        let scope = CursorScope::FolderType {
            folder: FolderId("AAMkAGI2/x=".to_string()),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("email scope is supported");
        assert!(!url.contains("AAMkAGI2/x="), "unencoded id in {url}");
        assert!(url.starts_with("/me/mailFolders/"));
    }

    #[test]
    fn inventory_entry_omits_graph_message_size() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "changeKey": "ck1",
            "conversationId": "thread1",
            "internetMessageHeaders": [
                { "name": "Message-ID", "value": "<m1@example.test>" },
                { "name": "References", "value": "<a> <b>" },
                { "name": "In-Reply-To", "value": "<b>" }
            ],
            "isRead": true,
            "categories": ["Blue"]
        });

        let entry = inventory_entry_from_value(&scope, &value).expect("entry expected");
        assert!(entry.size.is_none());
        assert_eq!(entry.message_id.as_deref(), Some("<m1@example.test>"));
        assert_eq!(entry.references.len(), 2);
        assert!(matches!(
            entry.fingerprint.server_version,
            ServerVersion::ETag(_)
        ));
    }
}
