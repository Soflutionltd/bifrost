//! Account composition: typed routing, discovery fan-in and capability ownership.
//!
//! This layer stays protocol-neutral because all three decisions depend on
//! Account contracts, not IMAP sessions. Keeping capability projection beside
//! routing gives composition one owner instead of a second IMAP-only table.
//! Discovery failures retain the existing warning contract; factory attach
//! failures use OpenedAccount's typed skipped-scopes lane at the caller.
//!
//! The exhaustive PIM projection below requires every new field to choose an
//! owner. Enterprise users expect attached contacts and calendars to retain
//! their backend's operations and version protection without changing mail.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream;

use crate::account::{Account, AccountStream};
use crate::cursor::{CursorScope, ObjectType};
use crate::error::Warning;
use crate::events::{Batch, PageBoundary, SyncEvent};

/// Where a typed `CursorScope`'s sync work is serviced.
pub enum ScopeTarget<'a> {
    /// The composing account itself owns this scope (folder/account
    /// scope, or a typed scope the composing protocol services
    /// natively). The caller maps this onto its own handler.
    This,
    /// A composed sub-account owns this typed scope.
    Delegate(&'a Arc<dyn Account>),
}

/// Route a `CursorScope` to its servicing target without naming any
/// protocol specifics.
///
/// - `Type(Contact)` and `FolderType { ty: Contact, .. }` -> the `contacts` sub-account when present.
/// - `Type(CalendarEvent)` and `FolderType { ty: CalendarEvent, .. }` -> the `calendars` sub-account when present.
/// - every other scope -> [`ScopeTarget::This`] (the composing account
///   handles or rejects it in its own vocabulary).
///
/// Returns `None` only when a typed scope matches a composable object
/// type but the corresponding sub-account is absent - the caller turns
/// that into its own `Unsupported(op)` error.
#[must_use]
pub fn route_typed_scope<'a>(
    scope: &CursorScope,
    contacts: Option<&'a Arc<dyn Account>>,
    calendars: Option<&'a Arc<dyn Account>>,
) -> Option<ScopeTarget<'a>> {
    match scope {
        CursorScope::Type(ObjectType::Contact)
        | CursorScope::FolderType {
            ty: ObjectType::Contact,
            ..
        } => contacts.map(ScopeTarget::Delegate),
        CursorScope::Type(ObjectType::CalendarEvent)
        | CursorScope::FolderType {
            ty: ObjectType::CalendarEvent,
            ..
        } => calendars.map(ScopeTarget::Delegate),
        _ => Some(ScopeTarget::This),
    }
}

/// Merge the composing account's own discovery `items` with each
/// sub-account's discovery stream into one bounded stream: any leading
/// `warnings`, then a single `Final` batch of the merged items, then one
/// terminal `Done(None)`.
///
/// A sub-account discovery error is folded into a `Warning` rather than
/// terminating the merged stream: the engine re-runs discovery on every
/// reopen, so a transient sub failure self-heals.
///
/// Two deliberate properties of this fan-in, called out because they are
/// not obvious from the signature:
///
/// - **Sub-account checkpoints are dropped.** Each sub's discovery stream
///   may carry its own `Batch.checkpoint` / `Done(Some(..))` cursor, but
///   the merged batch emits `checkpoint: None` and `Done(None)`.
///   Discovery is a stateless re-enumeration the engine reruns on every
///   reopen, so there is no merged cursor to persist; a sub's per-stream
///   checkpoint has no meaning once its items are folded into the
///   composing account's single discovery batch.
/// - **Subs are drained sequentially, fully buffered.** There is no
///   per-sub timeout and the first byte is delayed until every sub has
///   reached `Done`/`Terminated`. A sub that never terminates blocks the
///   whole merged stream. This is a constraint of `bifrost-types` having
///   no async runtime / timer of its own (see the module-tail note); a
///   timeout would have to be imposed by the composing crate around the
///   per-sub `discover` closure it supplies, not here.
pub fn merge_scope_streams<T, F>(
    items: Vec<T>,
    warnings: Vec<Warning>,
    subs: Vec<Arc<dyn Account>>,
    discover: F,
) -> AccountStream<SyncEvent<T>>
where
    T: Send + 'static,
    F: Fn(&Arc<dyn Account>) -> AccountStream<SyncEvent<T>> + Send + 'static,
{
    Box::pin(
        stream::once(async move {
            let mut items = items;
            let mut warnings = warnings;
            for sub in &subs {
                drain_sub_discovery(discover(sub), &mut items, &mut warnings).await;
            }
            let mut events: Vec<SyncEvent<T>> =
                warnings.into_iter().map(SyncEvent::Warning).collect();
            events.push(SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            }));
            events.push(SyncEvent::Done(None));
            events
        })
        .flat_map(stream::iter),
    )
}

/// Drain one sub-account discovery stream: collect `Batch` items, forward
/// `Warning`s, fold a `Terminated` into a warning (never fatal), stop at
/// `Done`.
async fn drain_sub_discovery<T>(
    mut stream: AccountStream<SyncEvent<T>>,
    items: &mut Vec<T>,
    warnings: &mut Vec<Warning>,
) {
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => items.extend(batch.items),
            SyncEvent::Warning(warning) => warnings.push(warning),
            SyncEvent::Terminated(error) => {
                warnings.push(
                    Warning::support_only(
                        crate::error::WarningKind::Other,
                        format!(
                            "composed sub-account discovery failed ({}); \
                             retried on next reopen",
                            error.message_key()
                        ),
                    )
                    .with_protocol_detail(crate::error::DiagnosticText::support_only("compose")),
                );
                break;
            }
            SyncEvent::Done(_) => break,
            // `SyncEvent` is non-exhaustive; a wildcard guards future
            // variants. Per-item progress carries nothing the merged
            // single-batch output needs.
            _ => {}
        }
    }
}

// These protocol-agnostic helpers are exercised by their first consumer,
// `bifrost-imap`, which owns an async runtime and an `Account` test
// double: `route_typed_scope` via the IMAP router tests and
// `merge_scope_streams` via the IMAP discovery fan-in tests (both the
// merge and the fold-sub-failure-into-warning paths). `bifrost-types`
// itself has no tokio dependency and no `Account` implementation; a unit
// test here would have to vendor a full ~60-method `Account` double plus
// a hand-rolled busy-loop executor into the foundational crate purely to
// re-prove what the consumer already pins, so the test lives with the
// consumer.

/// Project PIM methods and caller-version doors from their servicing accounts.
/// Mail capabilities remain owned by `primary`; absent delegates contribute none.
/// The exhaustive struct literal deliberately has no default update: adding a
/// PIM field must choose its owner here before the contract crate compiles.
pub fn merge_capabilities(
    primary: &mut crate::AccountCapabilities,
    contacts: Option<&crate::AccountCapabilities>,
    calendars: Option<&crate::AccountCapabilities>,
) {
    use crate::{MutationConcurrency, PimMethodSupport, VersionedDoors};

    let contact_methods = contacts.map(|caps| caps.pim_methods).unwrap_or_default();
    let calendar_methods = calendars.map(|caps| caps.pim_methods).unwrap_or_default();
    primary.pim_methods = PimMethodSupport {
        category_definitions: primary.pim_methods.category_definitions,
        message_reactions: primary.pim_methods.message_reactions,
        add_to_container: primary.pim_methods.add_to_container,
        remove_from_container: primary.pim_methods.remove_from_container,
        set_keyword: primary.pim_methods.set_keyword,
        set_label_membership: primary.pim_methods.set_label_membership,
        set_category: primary.pim_methods.set_category,
        set_extended_property: primary.pim_methods.set_extended_property,
        set_importance: primary.pim_methods.set_importance,
        set_is_read: primary.pim_methods.set_is_read,
        send_message: primary.pim_methods.send_message,
        attachment_upload: primary.pim_methods.attachment_upload,
        host_attachment: primary.pim_methods.host_attachment,
        draft_create: primary.pim_methods.draft_create,
        draft_update: primary.pim_methods.draft_update,
        draft_discard: primary.pim_methods.draft_discard,
        draft_send: primary.pim_methods.draft_send,
        scheduled_send: primary.pim_methods.scheduled_send,
        send_as: primary.pim_methods.send_as,
        search: primary.pim_methods.search,
        search_messages: primary.pim_methods.search_messages,
        containers_list: primary.pim_methods.containers_list,
        container_create: primary.pim_methods.container_create,
        container_rename: primary.pim_methods.container_rename,
        container_move: primary.pim_methods.container_move,
        container_delete: primary.pim_methods.container_delete,
        identities_list: primary.pim_methods.identities_list,
        identity_update: primary.pim_methods.identity_update,
        vacation_get: primary.pim_methods.vacation_get,
        vacation_set: primary.pim_methods.vacation_set,
        quota_get: primary.pim_methods.quota_get,
        thread_hydrate: primary.pim_methods.thread_hydrate,
        message_hydrate: primary.pim_methods.message_hydrate,
        open_raw_rfc822: primary.pim_methods.open_raw_rfc822,
        filters_list: primary.pim_methods.filters_list,
        filter_create: primary.pim_methods.filter_create,
        filter_update: primary.pim_methods.filter_update,
        filter_delete: primary.pim_methods.filter_delete,
        filter_validate: primary.pim_methods.filter_validate,
        address_books_list: contact_methods.address_books_list,
        contacts_list: contact_methods.contacts_list,
        contact_get: contact_methods.contact_get,
        contact_create: contact_methods.contact_create,
        contact_update: contact_methods.contact_update,
        contact_delete: contact_methods.contact_delete,
        contact_search: contact_methods.contact_search,
        contact_autocomplete: contact_methods.contact_autocomplete,
        directory_search: contact_methods.directory_search,
        directory_enumerate: contact_methods.directory_enumerate,
        directory_groups_list: contact_methods.directory_groups_list,
        directory_group_expand: contact_methods.directory_group_expand,
        calendars_list: calendar_methods.calendars_list,
        events_in_range: calendar_methods.events_in_range,
        event_get: calendar_methods.event_get,
        event_create: calendar_methods.event_create,
        event_update: calendar_methods.event_update,
        event_delete: calendar_methods.event_delete,
        event_rsvp: calendar_methods.event_rsvp,
        event_search: calendar_methods.event_search,
        event_autocomplete: calendar_methods.event_autocomplete,
    };
    let mail = primary.mutation.concurrency.versioned_doors();
    let contacts = contacts
        .map(|caps| caps.mutation.concurrency.versioned_doors())
        .unwrap_or(VersionedDoors::NONE);
    let calendars = calendars
        .map(|caps| caps.mutation.concurrency.versioned_doors())
        .unwrap_or(VersionedDoors::NONE);
    // Never union a delegate's whole set: its mail or other PIM lane belongs
    // to that delegate, not to this composite's corresponding handler.
    let mut doors = VersionedDoors::NONE;
    doors = doors.with(
        VersionedDoors::MAIL_SET_FLAGS,
        mail.contains(VersionedDoors::MAIL_SET_FLAGS),
    );
    doors = doors.with(
        VersionedDoors::MAIL_MOVE,
        mail.contains(VersionedDoors::MAIL_MOVE),
    );
    doors = doors.with(
        VersionedDoors::MAIL_DESTROY,
        mail.contains(VersionedDoors::MAIL_DESTROY),
    );
    doors = doors.with(
        VersionedDoors::CONTACT_UPDATE,
        contacts.contains(VersionedDoors::CONTACT_UPDATE),
    );
    doors = doors.with(
        VersionedDoors::CONTACT_DELETE,
        contacts.contains(VersionedDoors::CONTACT_DELETE),
    );
    doors = doors.with(
        VersionedDoors::EVENT_UPDATE,
        calendars.contains(VersionedDoors::EVENT_UPDATE),
    );
    doors = doors.with(
        VersionedDoors::EVENT_DELETE,
        calendars.contains(VersionedDoors::EVENT_DELETE),
    );
    doors = doors.with(
        VersionedDoors::EVENT_RSVP,
        calendars.contains(VersionedDoors::EVENT_RSVP),
    );
    primary.mutation.concurrency = MutationConcurrency::ExpectedVersion(doors);
}
