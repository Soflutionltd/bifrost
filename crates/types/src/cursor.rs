//! Cursor and scope types for the Account trait.
//!
//! Two distinct concepts that protocols disagree about: what a cursor
//! tracks (`CursorScope`) and what containers an object belongs to
//! (`MembershipScope`). Keeping them as separate enums is the difference
//! between an engine that can multiplex four protocols and one that
//! pretends one of them does not exist.

use crate::ids::{FolderId, LabelId, MailboxId, QueryId};

/// Current layout version of the trait-owned [`ChangeCursor`] envelope.
pub const CHANGE_CURSOR_ENVELOPE_VERSION: u32 = 1;

/// A cursor was minted for a different trait-owned envelope layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChangeCursorEnvelopeMismatch {
    pub expected: u32,
    pub found: u32,
}

/// What does a change cursor track?
///
/// - Gmail: `Account` (singleton historyId).
/// - JMAP: `Type(ObjectType)` per typed object; `Query(QueryId)` for
///   `queryChanges` cursors.
/// - IMAP: `Folder(FolderId)` (per-folder modseq).
/// - Graph: `FolderType { folder, ty }` (delta is per-folder per-type).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CursorScope {
    Account,
    Type(ObjectType),
    Query(QueryId),
    Folder(FolderId),
    FolderType { folder: FolderId, ty: ObjectType },
}

/// Container an object lives in.
///
/// Distinct from `CursorScope` because a single message can sit in many
/// labels or mailboxes simultaneously even when the change cursor for
/// that account is account-wide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MembershipScope {
    Folder(FolderId),
    Label(LabelId),
    Mailbox(MailboxId),
    Query(QueryId),
}

/// Object type tag for type-scoped cursors and discovery.
///
/// Open-ended; `#[non_exhaustive]` so a future protocol that exposes a
/// novel object type does not break consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ObjectType {
    Email,
    Mailbox,
    Thread,
    Event,
    Contact,
    EmailSubmission,
    CalendarEvent,
    ContactGroup,
}

/// Which protocol minted an opaque cursor. Tagged so a JMAP cursor
/// cannot be silently handed to the Graph impl on dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProtocolKind {
    Jmap,
    Imap,
    Gmail,
    CardDav,
    CalDav,
    Graph,
}

/// Concrete opaque cursor state. `protocol` and `envelope_version`
/// let the protocol impl reject a cursor minted for a different
/// protocol or an older schema.
///
/// Note on `envelope_version`: this field versions the layout of
/// `bytes` (the protocol-owned cursor payload). `ChangeCursor` carries
/// its own `envelope_version` versioning the enclosing
/// `ChangeCursor` struct layout (e.g. whether `advanced_through` is
/// present, what type aliases compose into the cursor). The two are
/// intentionally independent - one is "protocol said its payload
/// shape ticked", the other is "the trait crate said its outer
/// cursor shape ticked" - and they tick on different events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueChangeState {
    pub protocol: ProtocolKind,
    pub envelope_version: u32,
    pub bytes: Vec<u8>,
}

/// Mid-page resumption marker, opaque bytes owned by the protocol crate.
/// `None` when the protocol's pagination has no mid-page resume point
/// (Gmail history pages, Graph delta pages).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueProgressBytes(pub Vec<u8>);

/// Cursor handed back to the protocol crate on resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeCursor {
    pub scope: CursorScope,
    pub server_state: OpaqueChangeState,
    pub advanced_through: Option<OpaqueProgressBytes>,
    pub envelope_version: u32,
}

impl ChangeCursor {
    /// Validate the trait-owned outer envelope before protocol dispatch.
    pub fn validate_envelope(&self) -> Result<(), ChangeCursorEnvelopeMismatch> {
        if self.envelope_version == CHANGE_CURSOR_ENVELOPE_VERSION {
            Ok(())
        } else {
            Err(ChangeCursorEnvelopeMismatch {
                expected: CHANGE_CURSOR_ENVELOPE_VERSION,
                found: self.envelope_version,
            })
        }
    }
}

/// Result of `Account::establish_initial_cursor(scope)`.
///
/// - `Ready(cursor)`: the protocol minted a cursor cheaply (one
///   round-trip or factory-cached). The engine starts
///   `changes_stream(cursor)` immediately and runs `inventory_stream`
///   in parallel as backfill.
/// - `EstablishViaInventory`: the protocol has no cheap-mint
///   primitive. The engine must call `inventory_stream(scope)` first;
///   its terminal `Done` event carries the established cursor in its
///   checkpoint.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CursorEstablishment {
    Ready(ChangeCursor),
    EstablishViaInventory,
}

/// Scope lifecycle event surfaced from `Account::scope_lifecycle_stream`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScopeLifecycle {
    Created(MembershipScope),
    Renamed {
        old: MembershipScope,
        new: MembershipScope,
        old_name: String,
        new_name: String,
    },
    Deleted(MembershipScope),
}

/// Ongoing lifecycle changes and successful-poll heartbeats. The provider
/// owns successful poll cadence. On the first failure it yields `Terminated`
/// and ends without retrying; the engine owns reconnect, throttle holds,
/// backoff and directive routing. Only `Polled` or `Lifecycle` demonstrates
/// health and resets the engine's consecutive-failure count. Replacement of
/// the account carries that count forward.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScopeLifecycleEvent {
    /// A complete successful poll, including pagination, follow-up reads and
    /// the state commit. Never emitted after a partially failed poll.
    Polled,
    Lifecycle(ScopeLifecycle),
    Terminated(crate::error::AccountError),
}

/// Engine-facing introspection over an opaque cursor.
///
/// Account-aware (not pure-cursor) because cost depends on capability
/// state, not the cursor in isolation: a fresh JMAP `State` is `Cheap`;
/// a Gmail `historyId` that will fall back to full resync is `Expensive`.
#[derive(Debug, Clone)]
pub struct CursorDescriptor {
    pub cost_class: CostClass,
    pub strategy: SyncStrategy,
    pub freshness: Option<std::time::Instant>,
}

/// Coarse cost class used by the engine to schedule work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CostClass {
    Cheap,
    Medium,
    Expensive,
}

/// Resolved strategy a cursor will use to fetch its next batch.
///
/// IMAP picks one of `QResync`/`Condstore`/`Basic`; JMAP and Gmail
/// surface `ServerCursor` (their cursors are always server-issued);
/// Graph delta is `ServerCursor`; `Poll` is a client-maintained
/// watermark poll with no server-issued cursor (Graph public folders);
/// `None` means a cursor type that does not advance (used in tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SyncStrategy {
    QResync,
    Condstore,
    Basic,
    ServerCursor,
    Poll,
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outer_cursor_envelope_validation_rejects_any_other_version() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Gmail,
                envelope_version: 9,
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION + 1,
        };
        let mismatch = cursor
            .validate_envelope()
            .expect_err("version must be gated");
        assert_eq!(mismatch.expected, CHANGE_CURSOR_ENVELOPE_VERSION);
        assert_eq!(mismatch.found, CHANGE_CURSOR_ENVELOPE_VERSION + 1);
    }
}
