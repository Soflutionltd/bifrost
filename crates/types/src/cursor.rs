//! Cursor and scope types for the Account trait.
//!
//! Two distinct concepts that protocols disagree about: what a cursor
//! tracks (`CursorScope`) and what containers an object belongs to
//! (`MembershipScope`). Keeping them as separate enums is the difference
//! between an engine that can multiplex four protocols and one that
//! pretends one of them does not exist.

use crate::ids::{FolderId, LabelId, MailboxId, QueryId};

/// Protocol-owned payload layout. Deliberately distinct from the trait envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PayloadVersion(u32);

impl PayloadVersion {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Trait-owned ChangeCursor and BackfillCheckpoint layout.
/// Both use the same outer storage envelope and migration axis. Payload
/// revisions are independent and cannot be substituted for this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TraitEnvelopeVersion(u32);

impl TraitEnvelopeVersion {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

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
    /// A collection qualified by object type. DAV uses Contact and CalendarEvent
    /// so a collection URL cannot claim an IMAP mailbox or the other dialect.
    FolderType {
        folder: FolderId,
        ty: ObjectType,
    },
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
    pub envelope_version: PayloadVersion,
    pub bytes: Vec<u8>,
}

/// Why persisted cursor state cannot be resumed. The classification belongs
/// here so every provider gives the engine the same recovery instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CursorDecodeFault {
    ProtocolMismatch,
    FutureVersion,
    RetiredVersion,
    Malformed,
    ScopeMismatch,
    /// Token belongs to another account; no stored scope can safely consume it.
    IdentityMismatch,
}

impl CursorDecodeFault {
    /// Build the shared classification; the caller supplies scope and diagnostics.
    pub fn builder(
        self,
        protocol: crate::Protocol,
        operation: crate::AccountOperation,
    ) -> crate::AccountErrorBuilder {
        // An enterprise user expects unreadable durable state to be rebuilt,
        // including dependent backfill state, rather than blamed on the server.
        // A readable token filed under another scope needs only that scope reset.
        let (kind, cause) = match self {
            Self::ScopeMismatch => (
                crate::SyncStateErrorKind::CursorInvalid,
                crate::StateCause::CursorInvalid,
            ),
            Self::ProtocolMismatch
            | Self::FutureVersion
            | Self::RetiredVersion
            | Self::Malformed
            | Self::IdentityMismatch => (
                crate::SyncStateErrorKind::SchemaIncompatible,
                crate::StateCause::SchemaIncompatible,
            ),
        };
        crate::AccountErrorBuilder::new(
            crate::AccountErrorKind::SyncState(kind),
            crate::Cause::State(cause),
        )
        .protocol(protocol)
        .operation(operation)
    }
}

impl OpaqueChangeState {
    /// Open only the expected protocol and exact supported payload layout.
    pub fn open(
        &self,
        protocol: ProtocolKind,
        version: PayloadVersion,
    ) -> Result<&[u8], CursorDecodeFault> {
        if self.protocol != protocol {
            return Err(CursorDecodeFault::ProtocolMismatch);
        }
        if self.envelope_version > version {
            return Err(CursorDecodeFault::FutureVersion);
        }
        if self.envelope_version < version {
            return Err(CursorDecodeFault::RetiredVersion);
        }
        Ok(&self.bytes)
    }
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
    pub envelope_version: TraitEnvelopeVersion,
}

impl ChangeCursor {
    /// Validate the trait-owned outer envelope before protocol dispatch.
    pub fn validate_envelope(&self) -> Result<(), ChangeCursorEnvelopeMismatch> {
        if self.envelope_version.get() == CHANGE_CURSOR_ENVELOPE_VERSION {
            Ok(())
        } else {
            Err(ChangeCursorEnvelopeMismatch {
                expected: CHANGE_CURSOR_ENVELOPE_VERSION,
                found: self.envelope_version.get(),
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

impl ObjectType {
    /// Stable tag in the sync cursor envelope. Append new tags; never renumber.
    #[must_use]
    pub const fn cursor_tag(self) -> u8 {
        match self {
            Self::Email => 0,
            Self::Mailbox => 1,
            Self::Thread => 2,
            Self::Event => 3,
            Self::Contact => 4,
            Self::EmailSubmission => 5,
            Self::CalendarEvent => 6,
            Self::ContactGroup => 7,
        }
    }
}

impl ProtocolKind {
    /// Stable tag in the sync cursor envelope. Append new tags; never renumber.
    #[must_use]
    pub const fn cursor_tag(self) -> u8 {
        match self {
            Self::Jmap => 0,
            Self::Imap => 1,
            Self::Gmail => 2,
            Self::CardDav => 4,
            Self::CalDav => 5,
            Self::Graph => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_cursor_faults_have_one_recovery_policy() {
        use crate::{AccountOperation, EngineDirective, ErrorScope, Protocol, RecoveryClass};
        for fault in [
            CursorDecodeFault::ProtocolMismatch,
            CursorDecodeFault::FutureVersion,
            CursorDecodeFault::RetiredVersion,
            CursorDecodeFault::Malformed,
            CursorDecodeFault::ScopeMismatch,
            CursorDecodeFault::IdentityMismatch,
        ] {
            let error = fault
                .builder(Protocol::Imap, AccountOperation::SyncChanges)
                .scope(ErrorScope::Cursor(CursorScope::Account))
                .try_build()
                .expect("valid cursor classification");
            let expected = if fault == CursorDecodeFault::ScopeMismatch {
                EngineDirective::RestartScope(CursorScope::Account)
            } else {
                EngineDirective::SchemaIncompatible
            };
            assert_eq!(error.recovery(), &RecoveryClass::Engine(expected));
        }
    }

    #[test]
    fn opaque_payload_open_checks_protocol_before_exact_layout() {
        let mut state = OpaqueChangeState {
            protocol: ProtocolKind::Imap,
            envelope_version: PayloadVersion::new(2),
            bytes: vec![7],
        };
        assert_eq!(
            state.open(ProtocolKind::Imap, PayloadVersion::new(2)),
            Ok(&[7][..])
        );
        assert_eq!(
            state.open(ProtocolKind::Graph, PayloadVersion::new(1)),
            Err(CursorDecodeFault::ProtocolMismatch)
        );
        assert_eq!(
            state.open(ProtocolKind::Imap, PayloadVersion::new(1)),
            Err(CursorDecodeFault::FutureVersion)
        );
        state.envelope_version = PayloadVersion::new(1);
        assert_eq!(
            state.open(ProtocolKind::Imap, PayloadVersion::new(2)),
            Err(CursorDecodeFault::RetiredVersion)
        );
    }

    #[test]
    fn outer_cursor_envelope_validation_rejects_any_other_version() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Gmail,
                envelope_version: crate::cursor::PayloadVersion::new(9),
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: crate::cursor::TraitEnvelopeVersion::new(
                CHANGE_CURSOR_ENVELOPE_VERSION + 1,
            ),
        };
        let mismatch = cursor
            .validate_envelope()
            .expect_err("version must be gated");
        assert_eq!(mismatch.expected, CHANGE_CURSOR_ENVELOPE_VERSION);
        assert_eq!(mismatch.found, CHANGE_CURSOR_ENVELOPE_VERSION + 1);
    }
}
