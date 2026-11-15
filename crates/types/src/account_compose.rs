//! Reusable account-composition helpers.
//!
//! A composing account (today IMAP; tomorrow JMAP / Graph) can attach
//! CardDAV / CalDAV sub-accounts and present their typed scopes as
//! first-class sync scopes. The two pieces every composing account needs
//! are the same:
//!
//! - [`route_typed_scope`] - decide whether a `CursorScope` is serviced
//!   by the composing account itself or delegated to a contacts /
//!   calendars sub-account, keyed only on the scope and `ObjectType`.
//! - [`merge_scope_streams`] - fan the composing account's own discovery
//!   items in with each sub-account's discovery stream into one bounded
//!   stream (warnings, then one final batch, then one `Done`).
//!
//! These are protocol-agnostic: they name only `CursorScope`,
//! `ObjectType`, `Account`, and the stream/event types. The composing
//! crate maps [`ScopeTarget::This`] onto its own per-protocol handler
//! (e.g. IMAP resolves it to a `MailboxName`) and builds the
//! `Unsupported` error in its own error vocabulary.

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
