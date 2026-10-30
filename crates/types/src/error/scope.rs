use serde::Serialize;

use crate::calendar::CalendarId;
use crate::contact::ContactId;
use crate::cursor::{CursorScope, ObjectType};
use crate::ids::{MailboxId, ObjectId, ThreadId};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorScope {
    Account,
    Cursor(CursorScope),
    Mailbox { id: MailboxId },
    Message { id: ObjectId },
    Thread { id: ThreadId },
    Calendar { id: CalendarId },
    CalendarCollection,
    Contact { id: ContactId },
    ContactCollection,
}

/// One serialized `(name, value)` pair. Every `ErrorScope` field is a
/// string, so the whole projection fits a flat array.
type ScopeField<'a> = (&'static str, &'a str);

/// Fixed-capacity accumulator for the serialized projection. Four is
/// the widest shape (`Cursor(FolderType)`: kind + scope_kind +
/// folder_id + object_type).
struct ScopeFields<'a> {
    buf: [ScopeField<'a>; 4],
    len: usize,
}

impl<'a> ScopeFields<'a> {
    fn new() -> Self {
        Self {
            buf: [("", ""); 4],
            len: 0,
        }
    }

    fn push(mut self, name: &'static str, value: &'a str) -> Self {
        self.buf[self.len] = (name, value);
        self.len += 1;
        self
    }

    fn as_slice(&self) -> &[ScopeField<'a>] {
        &self.buf[..self.len]
    }
}

/// The serialized projection of an `ErrorScope`, names and values
/// together. `serialize` declares `len()` as the struct field count and
/// then writes exactly these pairs, so the declared count cannot drift
/// from the fields actually written. A mismatch corrupts output in
/// length-prefixed formats (bincode, postcard, compact MessagePack),
/// which self-describing JSON would have hidden.
fn scope_fields(scope: &ErrorScope) -> ScopeFields<'_> {
    let fields = ScopeFields::new();
    match scope {
        ErrorScope::Account => fields.push("kind", "account"),
        ErrorScope::Cursor(cursor) => cursor_scope_fields(fields.push("kind", "cursor"), cursor),
        ErrorScope::Mailbox { id } => fields.push("kind", "mailbox").push("id", &id.0),
        ErrorScope::Message { id } => fields.push("kind", "message").push("id", &id.0),
        ErrorScope::Thread { id } => fields.push("kind", "thread").push("id", &id.0),
        ErrorScope::Calendar { id } => fields.push("kind", "calendar").push("id", &id.0),
        ErrorScope::CalendarCollection => fields.push("kind", "calendar_collection"),
        ErrorScope::Contact { id } => fields.push("kind", "contact").push("id", &id.0),
        ErrorScope::ContactCollection => fields.push("kind", "contact_collection"),
    }
}

fn cursor_scope_fields<'a>(fields: ScopeFields<'a>, scope: &'a CursorScope) -> ScopeFields<'a> {
    match scope {
        CursorScope::Account => fields.push("scope_kind", "account"),
        CursorScope::Type(ty) => fields
            .push("scope_kind", "type")
            .push("object_type", object_type_name(*ty)),
        CursorScope::Query(id) => fields.push("scope_kind", "query").push("query_id", &id.0),
        CursorScope::Folder(id) => fields.push("scope_kind", "folder").push("folder_id", &id.0),
        CursorScope::FolderType { folder, ty } => fields
            .push("scope_kind", "folder_type")
            .push("folder_id", &folder.0)
            .push("object_type", object_type_name(*ty)),
    }
}

impl Serialize for ErrorScope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let fields = scope_fields(self);
        let fields = fields.as_slice();
        let mut state = serializer.serialize_struct("ErrorScope", fields.len())?;
        for (name, value) in fields {
            state.serialize_field(name, value)?;
        }
        state.end()
    }
}

fn object_type_name(ty: ObjectType) -> &'static str {
    match ty {
        ObjectType::Email => "email",
        ObjectType::Mailbox => "mailbox",
        ObjectType::Thread => "thread",
        ObjectType::Event => "event",
        ObjectType::Contact => "contact",
        ObjectType::EmailSubmission => "email_submission",
        ObjectType::CalendarEvent => "calendar_event",
        ObjectType::ContactGroup => "contact_group",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum AccountOperation {
    CategoryDefinitionsList,
    CategoryDefinitionUpsert,
    CategoryDefinitionDelete,
    MessageReactionsRead,
    Discover,
    EstablishCursor,
    DiscoverCursorScopes,
    DiscoverMemberships,
    ScopeLifecycle,
    SyncInventory,
    SyncChanges,
    Hydrate,
    HydrateThread,
    HydrateMessage,
    OpenBlob,
    OpenBlobRange,
    OpenRawRfc822,
    PushSubscribe,
    PushUnsubscribe,
    PushStream,
    UpdateFlags,
    SetStarred,
    MarkReplied,
    MarkForwarded,
    MarkMdnSent,
    BulkMove,
    BulkDestroy,
    MoveThread,
    DeleteThread,
    AddToContainer,
    RemoveFromContainer,
    SetKeyword,
    SetLabelMembership,
    SetCategory,
    SetExtendedProperty,
    SetImportance,
    SetIsRead,
    Send,
    AttachmentUpload,
    HostAttachment,
    DraftCreate,
    DraftUpdate,
    DraftDiscard,
    DraftSend,
    CancelScheduledSend,
    RescheduleSend,
    Search,
    SearchMessages,
    ContainersList,
    ContainerCreate,
    ContainerRename,
    ContainerMove,
    ContainerDelete,
    IdentitiesList,
    IdentityUpdate,
    VacationGet,
    VacationSet,
    QuotaGet,
    FiltersList,
    FilterCreate,
    FilterUpdate,
    FilterDelete,
    FilterValidate,
    AddressBooksList,
    ContactsList,
    ContactGet,
    ContactCreate,
    ContactUpdate,
    ContactDelete,
    ContactSearch,
    ContactAutocomplete,
    DirectorySearch,
    DirectoryGroupsList,
    DirectoryGroupExpand,
    CalendarsList,
    EventsInRange,
    EventGet,
    EventCreate,
    EventUpdate,
    EventDelete,
    EventRsvp,
    EventSearch,
    EventAutocomplete,
    Close,
    Expunge,
}

impl AccountOperation {
    #[must_use]
    pub fn is_idempotent(self) -> bool {
        // The non-idempotent set is every operation whose blind same-request
        // retry could double-apply a side effect: sends, creates, moves,
        // destroys/expunges/discards, attachment uploads, and the draft /
        // destructive container/contact/event/filter operations and other
        // non-repeatable writers. For these, an in-flight transport drop must
        // route to `Reconcile` (probe the target), not a blind
        // `Retry(SameRequest)`.
        //
        // Destroy/expunge/discard are non-idempotent here even though a
        // re-delete of an already-gone object reaches the same end state:
        // an in-flight drop has no proof the destroy reached the server,
        // and the reconcile lane is the correct uniform "did my mutation
        // land?" handling. Moves are non-idempotent for the same reason
        // (the source may already be gone).
        //
        // `ContainerRename` looks like an absolute-state write but is NOT one
        // on every protocol: IMAP `RENAME old new` is keyed on the OLD NAME,
        // not on a stable id, so a blind replay after an in-flight drop that
        // actually landed addresses a mailbox that no longer exists and
        // reports a spurious permanent failure for an operation that
        // succeeded. JMAP/Gmail/Graph rename by id and would be safe, but this
        // table has no provider dimension, so it takes the conservative
        // branch; a provider that renames by id can widen its own case with
        // `idempotency_override(true)`.
        //
        // Absolute-state writes against known ids are idempotent and stay OUT
        // of this set:
        // re-applying `UpdateFlags` / `SetKeyword` / `SetLabelMembership` /
        // `SetCategory` / `SetExtendedProperty` / `SetImportance` /
        // `SetIsRead`, the `*Update` family, and the singleton settings
        // writers drive the target to the same value, so a blind retry
        // is safe (pinned by the JMAP `UpdateFlags` contract test).
        // `FilterUpdate` is the one `*Update` exception, in the set below:
        // it is not an id-keyed absolute-state write on every protocol -
        // Gmail filter "update" is delete+create, and ManageSieve keys on
        // the script name - so a blind replay can double-apply.
        // Read-only and discovery operations are idempotent by omission.
        !matches!(
            self,
            Self::PushSubscribe
                | Self::Send
                | Self::BulkMove
                | Self::BulkDestroy
                | Self::MoveThread
                | Self::DeleteThread
                | Self::AddToContainer
                | Self::RemoveFromContainer
                | Self::AttachmentUpload
                | Self::HostAttachment
                | Self::DraftCreate
                | Self::DraftDiscard
                | Self::DraftSend
                | Self::CancelScheduledSend
                | Self::RescheduleSend
                | Self::ContainerCreate
                | Self::ContainerRename
                | Self::ContainerMove
                | Self::ContainerDelete
                | Self::FilterCreate
                | Self::FilterUpdate
                | Self::FilterDelete
                | Self::ContactCreate
                | Self::ContactDelete
                | Self::EventCreate
                | Self::EventDelete
                | Self::EventRsvp
                | Self::Expunge
                | Self::CategoryDefinitionDelete
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum Provider {
    Fastmail,
    Gmail,
    Microsoft,
    Icloud,
    Yahoo,
    Custom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum Protocol {
    Jmap,
    Imap,
    CardDav,
    Smtp,
    Lmtp,
    Gmail,
    Graph,
    Ews,
    CalDav,
    /// The protocol is not derivable at the point the error was classified.
    ///
    /// Reserved for errors the ENGINE mints about an account's behaviour
    /// rather than errors a protocol crate maps from a wire response: the
    /// engine holds an `Arc<dyn Account>` and there is no protocol accessor on
    /// the trait, so where the offending payload carries no protocol tag of its
    /// own there is nothing honest to report. Protocol crates always know their
    /// own protocol and must never use this.
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::{AccountOperation, ErrorScope, scope_fields};
    use crate::cursor::{CursorScope, ObjectType};
    use crate::ids::{FolderId, QueryId};

    /// `scope_fields` is the single source for both the declared struct
    /// field count and the fields written, so pinning it pins the
    /// serialized shape. A count that overstates the fields written
    /// corrupts length-prefixed output.
    #[test]
    fn scope_fields_pin_the_serialized_shape() {
        let cases: Vec<(ErrorScope, Vec<(&str, &str)>)> = vec![
            (ErrorScope::Account, vec![("kind", "account")]),
            (
                ErrorScope::CalendarCollection,
                vec![("kind", "calendar_collection")],
            ),
            (
                ErrorScope::ContactCollection,
                vec![("kind", "contact_collection")],
            ),
            (
                ErrorScope::Mailbox { id: "b".into() },
                vec![("kind", "mailbox"), ("id", "b")],
            ),
            (
                ErrorScope::Message { id: "m".into() },
                vec![("kind", "message"), ("id", "m")],
            ),
            (
                ErrorScope::Thread { id: "t".into() },
                vec![("kind", "thread"), ("id", "t")],
            ),
            (
                ErrorScope::Calendar { id: "c".into() },
                vec![("kind", "calendar"), ("id", "c")],
            ),
            (
                ErrorScope::Contact { id: "p".into() },
                vec![("kind", "contact"), ("id", "p")],
            ),
            (
                ErrorScope::Cursor(CursorScope::Account),
                vec![("kind", "cursor"), ("scope_kind", "account")],
            ),
            (
                ErrorScope::Cursor(CursorScope::Type(ObjectType::Email)),
                vec![
                    ("kind", "cursor"),
                    ("scope_kind", "type"),
                    ("object_type", "email"),
                ],
            ),
            (
                ErrorScope::Cursor(CursorScope::Query(QueryId("q".into()))),
                vec![
                    ("kind", "cursor"),
                    ("scope_kind", "query"),
                    ("query_id", "q"),
                ],
            ),
            (
                ErrorScope::Cursor(CursorScope::Folder(FolderId("f".into()))),
                vec![
                    ("kind", "cursor"),
                    ("scope_kind", "folder"),
                    ("folder_id", "f"),
                ],
            ),
            (
                ErrorScope::Cursor(CursorScope::FolderType {
                    folder: FolderId("f".into()),
                    ty: ObjectType::Email,
                }),
                vec![
                    ("kind", "cursor"),
                    ("scope_kind", "folder_type"),
                    ("folder_id", "f"),
                    ("object_type", "email"),
                ],
            ),
        ];

        for (scope, expected) in cases {
            let fields = scope_fields(&scope);
            assert_eq!(
                fields.as_slice(),
                expected.as_slice(),
                "serialized projection for {scope:?}"
            );
        }
    }

    #[test]
    fn host_attachment_is_not_idempotent() {
        // An interrupted host may have created a partial/duplicate Drive item;
        // a blind retry is unsafe, so the operation must be non-idempotent.
        assert!(!AccountOperation::HostAttachment.is_idempotent());
    }

    #[test]
    fn directory_search_is_idempotent() {
        // A directory search is a read; a transport drop mid-search is safely
        // retryable, so it stays idempotent by omission from the exclusion set.
        assert!(AccountOperation::DirectorySearch.is_idempotent());
    }

    #[test]
    fn destructive_ops_are_not_idempotent() {
        // An in-flight drop on any of these must reconcile (probe the
        // target), not blind-retry. Regression guard for the table that
        // previously treated destroys/expunge/discard as idempotent while
        // moves were correctly excluded (shortlist #4).
        for op in [
            AccountOperation::BulkDestroy,
            AccountOperation::Expunge,
            AccountOperation::DraftDiscard,
            AccountOperation::CancelScheduledSend,
            AccountOperation::BulkMove,
            AccountOperation::MoveThread,
            AccountOperation::DeleteThread,
        ] {
            assert!(!op.is_idempotent(), "{op:?} must be non-idempotent");
        }
    }

    #[test]
    fn absolute_state_writes_stay_idempotent() {
        // Setting a flag/label/importance to an absolute target value is
        // idempotent: a blind same-request retry drives the target to the
        // same state. These must NOT be in the non-idempotent set (pinned by
        // the JMAP `UpdateFlags` contract test).
        for op in [
            AccountOperation::UpdateFlags,
            AccountOperation::SetKeyword,
            AccountOperation::SetLabelMembership,
            AccountOperation::SetCategory,
            AccountOperation::SetExtendedProperty,
            AccountOperation::SetImportance,
            AccountOperation::SetIsRead,
            AccountOperation::DraftUpdate,
            AccountOperation::ContactUpdate,
            AccountOperation::EventUpdate,
            AccountOperation::IdentityUpdate,
            AccountOperation::VacationSet,
        ] {
            assert!(op.is_idempotent(), "{op:?} must stay idempotent");
        }
    }

    #[test]
    fn rename_is_not_an_absolute_state_write() {
        // IMAP `RENAME old new` keys on the old name, so replaying a rename
        // that already landed addresses a mailbox that is gone. It belongs
        // with the moves, not with the `*Update` family.
        assert!(!AccountOperation::ContainerRename.is_idempotent());
    }

    #[test]
    fn read_only_ops_remain_idempotent() {
        for op in [
            AccountOperation::Discover,
            AccountOperation::SyncInventory,
            AccountOperation::SyncChanges,
            AccountOperation::Hydrate,
            AccountOperation::OpenBlob,
            AccountOperation::Search,
            AccountOperation::ContactGet,
            AccountOperation::QuotaGet,
            AccountOperation::DirectoryGroupsList,
            AccountOperation::DirectoryGroupExpand,
        ] {
            assert!(op.is_idempotent(), "{op:?} must stay idempotent");
        }
    }
}
