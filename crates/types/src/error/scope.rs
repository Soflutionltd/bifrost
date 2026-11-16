use serde::Serialize;

use crate::calendar::{CalendarId, EventId};
use crate::contact::ContactId;
use crate::cursor::{CursorScope, ObjectType};
use crate::ids::{MailboxId, ObjectId, PrincipalId, ThreadId};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorScope {
    Submission {
        id: ObjectId,
    },
    CloudFile {
        id: String,
    },
    UploadSession,
    Account,
    /// An IMAP namespace prefix, not a selectable mailbox.
    Namespace {
        prefix: String,
    },
    Cursor(CursorScope),
    /// A principal whose shared account surface failed.
    Owner {
        id: PrincipalId,
    },
    /// A native mail container, distinct from its owning principal.
    Mailbox {
        id: MailboxId,
    },
    Message {
        id: ObjectId,
    },
    Thread {
        id: ThreadId,
    },
    Calendar {
        id: CalendarId,
    },
    Event {
        id: EventId,
    },
    CalendarCollection,
    Contact {
        id: ContactId,
    },
    ContactCollection,
    /// A read-only directory gateway, identified by its resolved URL.
    DirectoryGateway {
        url: String,
    },
}

impl ErrorScope {
    /// The individual resource identified by this scope, when its kind is known.
    ///
    /// Collection scopes report the kind they collect (`Calendar`, `Contact`)
    /// with no object id, so a collection-scoped miss is still a typed
    /// not-found. Cursor scopes identify sync state rather than the object
    /// that a request missed. Scopes without a corresponding resource kind
    /// return `None`.
    pub fn resource(&self) -> Option<super::ResourceKind> {
        use super::ResourceKind;
        match self {
            Self::Message { .. } => Some(ResourceKind::Message),
            Self::Mailbox { .. } => Some(ResourceKind::Mailbox),
            Self::Thread { .. } => Some(ResourceKind::Thread),
            Self::Calendar { .. } | Self::CalendarCollection => Some(ResourceKind::Calendar),
            Self::Event { .. } => Some(ResourceKind::Event),
            Self::Contact { .. } | Self::ContactCollection => Some(ResourceKind::Contact),
            Self::Owner { .. }
            | Self::Submission { .. }
            | Self::CloudFile { .. }
            | Self::UploadSession
            | Self::Account
            | Self::Namespace { .. }
            | Self::Cursor(_)
            | Self::DirectoryGateway { .. } => None,
        }
    }

    /// The object id carried by this scope, without allocating.
    ///
    /// Cursor ids name sync state, and gateway URLs name endpoints, so neither
    /// is an object id. An id can be present without a known resource kind.
    pub fn object_id(&self) -> Option<&str> {
        match self {
            Self::Submission { id } => Some(&id.0),
            Self::CloudFile { id } => Some(id),
            Self::Mailbox { id } => Some(&id.0),
            Self::Owner { id } => Some(&id.0),
            Self::Message { id } => Some(&id.0),
            Self::Thread { id } => Some(&id.0),
            Self::Calendar { id } => Some(&id.0),
            Self::Event { id } => Some(&id.0),
            Self::Contact { id } => Some(&id.0),
            Self::UploadSession
            | Self::Account
            | Self::Namespace { .. }
            | Self::Cursor(_)
            | Self::CalendarCollection
            | Self::ContactCollection
            | Self::DirectoryGateway { .. } => None,
        }
    }
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
        ErrorScope::Submission { id } => fields.push("kind", "submission").push("id", &id.0),
        ErrorScope::CloudFile { id } => fields.push("kind", "cloud_file").push("id", id),
        ErrorScope::UploadSession => fields.push("kind", "upload_session"),
        ErrorScope::Account => fields.push("kind", "account"),
        ErrorScope::Namespace { prefix } => fields.push("kind", "namespace").push("prefix", prefix),
        ErrorScope::Cursor(cursor) => cursor_scope_fields(fields.push("kind", "cursor"), cursor),
        ErrorScope::Owner { id } => fields.push("kind", "owner").push("id", &id.0),
        ErrorScope::Mailbox { id } => fields.push("kind", "mailbox").push("id", &id.0),
        ErrorScope::Message { id } => fields.push("kind", "message").push("id", &id.0),
        ErrorScope::Thread { id } => fields.push("kind", "thread").push("id", &id.0),
        ErrorScope::Calendar { id } => fields.push("kind", "calendar").push("id", &id.0),
        ErrorScope::Event { id } => fields.push("kind", "event").push("id", &id.0),
        ErrorScope::CalendarCollection => fields.push("kind", "calendar_collection"),
        ErrorScope::Contact { id } => fields.push("kind", "contact").push("id", &id.0),
        ErrorScope::ContactCollection => fields.push("kind", "contact_collection"),
        ErrorScope::DirectoryGateway { url } => {
            fields.push("kind", "directory_gateway").push("url", url)
        }
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
    /// Whether a confirmed absent mutation subject is `MutationSuccess::Skipped`.
    ///
    /// Outlook users expect vanished mail to leave the view when moving,
    /// deleting or editing its presentation, without an error or a claim that
    /// this call applied the edit. Discarding an absent draft is likewise done.
    /// Reads, sends, scheduling changes and non-mail edits retain NotFound:
    /// absence cannot supply content, deliver mail or confirm the requested edit.
    /// This applies to the subject only, never a missing destination or blob.
    #[must_use]
    pub const fn skips_absent_target(self) -> bool {
        match self {
            Self::UpdateFlags
            | Self::SetStarred
            | Self::MarkReplied
            | Self::MarkForwarded
            | Self::MarkMdnSent
            | Self::BulkMove
            | Self::BulkDestroy
            | Self::MoveThread
            | Self::DeleteThread
            | Self::AddToContainer
            | Self::RemoveFromContainer
            | Self::SetKeyword
            | Self::SetLabelMembership
            | Self::SetCategory
            | Self::SetExtendedProperty
            | Self::SetImportance
            | Self::SetIsRead
            | Self::DraftDiscard => true,
            Self::CategoryDefinitionsList
            | Self::MessageReactionsRead
            | Self::Discover
            | Self::EstablishCursor
            | Self::DiscoverCursorScopes
            | Self::DiscoverMemberships
            | Self::ScopeLifecycle
            | Self::SyncInventory
            | Self::SyncChanges
            | Self::Hydrate
            | Self::HydrateThread
            | Self::HydrateMessage
            | Self::OpenBlob
            | Self::OpenBlobRange
            | Self::OpenRawRfc822
            | Self::PushSubscribe
            | Self::PushUnsubscribe
            | Self::PushStream
            | Self::Send
            | Self::AttachmentUpload
            | Self::HostAttachment
            | Self::DraftCreate
            | Self::DraftUpdate
            | Self::DraftSend
            | Self::CancelScheduledSend
            | Self::RescheduleSend
            | Self::Search
            | Self::SearchMessages
            | Self::ContainersList
            | Self::ContainerCreate
            | Self::ContainerRename
            | Self::ContainerMove
            | Self::ContainerDelete
            | Self::IdentitiesList
            | Self::IdentityUpdate
            | Self::VacationGet
            | Self::VacationSet
            | Self::QuotaGet
            | Self::FiltersList
            | Self::FilterCreate
            | Self::FilterUpdate
            | Self::FilterDelete
            | Self::FilterValidate
            | Self::AddressBooksList
            | Self::ContactsList
            | Self::ContactGet
            | Self::ContactCreate
            | Self::ContactUpdate
            | Self::ContactDelete
            | Self::ContactSearch
            | Self::ContactAutocomplete
            | Self::DirectorySearch
            | Self::DirectoryGroupsList
            | Self::DirectoryGroupExpand
            | Self::CalendarsList
            | Self::EventsInRange
            | Self::EventGet
            | Self::EventCreate
            | Self::EventUpdate
            | Self::EventDelete
            | Self::EventRsvp
            | Self::EventSearch
            | Self::EventAutocomplete
            | Self::Close
            | Self::Expunge => false,
        }
    }

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
        )
    }
}

/// Vendor or vendor API identity, including provider expression languages.
/// This is independent of the transport or cursor protocol an account uses.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum Provider {
    /// The account vendor for the non-mail Google APIs (Calendar, People,
    /// Drive). Gmail stays the provider for Google mail and for server-side
    /// filters, including `FilterCondition::ProviderExpression`.
    Google,
    Fastmail,
    /// Google mail, and the provider a Gmail filter expression must name.
    Gmail,
    Microsoft,
    Icloud,
    Yahoo,
    Custom,
}

/// Protocol of the failing API or transport leg. A composed account can report
/// several protocols even though `Account::protocol()` names one primary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[non_exhaustive]
pub enum Protocol {
    GoogleCalendar,
    GooglePeople,
    GoogleDrive,
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
    /// Used for engine errors and provider-neutral helpers. Protocol crates
    /// know their own protocol and must stamp it when propagating a shared
    /// helper's refusal.
    Unknown,
}

impl AccountOperation {
    /// Stable tag in the sync ledger error digest. Append new tags; never renumber.
    #[must_use]
    pub const fn ledger_tag(self) -> u8 {
        match self {
            Self::CategoryDefinitionsList => 0,
            Self::MessageReactionsRead => 1,
            Self::Discover => 2,
            Self::EstablishCursor => 3,
            Self::DiscoverCursorScopes => 4,
            Self::DiscoverMemberships => 5,
            Self::ScopeLifecycle => 6,
            Self::SyncInventory => 7,
            Self::SyncChanges => 8,
            Self::Hydrate => 9,
            Self::HydrateThread => 10,
            Self::HydrateMessage => 11,
            Self::OpenBlob => 12,
            Self::OpenBlobRange => 13,
            Self::OpenRawRfc822 => 14,
            Self::PushSubscribe => 15,
            Self::PushUnsubscribe => 16,
            Self::PushStream => 17,
            Self::UpdateFlags => 18,
            Self::SetStarred => 19,
            Self::MarkReplied => 20,
            Self::MarkForwarded => 21,
            Self::MarkMdnSent => 22,
            Self::BulkMove => 23,
            Self::BulkDestroy => 24,
            Self::MoveThread => 25,
            Self::DeleteThread => 26,
            Self::AddToContainer => 27,
            Self::RemoveFromContainer => 28,
            Self::SetKeyword => 29,
            Self::SetLabelMembership => 30,
            Self::SetCategory => 31,
            Self::SetExtendedProperty => 32,
            Self::SetImportance => 33,
            Self::SetIsRead => 34,
            Self::Send => 35,
            Self::AttachmentUpload => 36,
            Self::HostAttachment => 37,
            Self::DraftCreate => 38,
            Self::DraftUpdate => 39,
            Self::DraftDiscard => 40,
            Self::DraftSend => 41,
            Self::CancelScheduledSend => 42,
            Self::RescheduleSend => 43,
            Self::Search => 44,
            Self::SearchMessages => 45,
            Self::ContainersList => 46,
            Self::ContainerCreate => 47,
            Self::ContainerRename => 48,
            Self::ContainerMove => 49,
            Self::ContainerDelete => 50,
            Self::IdentitiesList => 51,
            Self::IdentityUpdate => 52,
            Self::VacationGet => 53,
            Self::VacationSet => 54,
            Self::QuotaGet => 55,
            Self::FiltersList => 56,
            Self::FilterCreate => 57,
            Self::FilterUpdate => 58,
            Self::FilterDelete => 59,
            Self::FilterValidate => 60,
            Self::AddressBooksList => 61,
            Self::ContactsList => 62,
            Self::ContactGet => 63,
            Self::ContactCreate => 64,
            Self::ContactUpdate => 65,
            Self::ContactDelete => 66,
            Self::ContactSearch => 67,
            Self::ContactAutocomplete => 68,
            Self::DirectorySearch => 69,
            Self::DirectoryGroupsList => 70,
            Self::DirectoryGroupExpand => 71,
            Self::CalendarsList => 72,
            Self::EventsInRange => 73,
            Self::EventGet => 74,
            Self::EventCreate => 75,
            Self::EventUpdate => 76,
            Self::EventDelete => 77,
            Self::EventRsvp => 78,
            Self::EventSearch => 79,
            Self::EventAutocomplete => 80,
            Self::Close => 81,
            Self::Expunge => 82,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AccountOperation, ErrorScope, scope_fields};
    use crate::cursor::{CursorScope, ObjectType};
    use crate::ids::{FolderId, QueryId};

    #[test]
    fn resource_and_object_id_cover_every_scope_shape() {
        use crate::error::ResourceKind;

        let cases = [
            (
                ErrorScope::Message { id: "m".into() },
                Some(ResourceKind::Message),
                Some("m"),
            ),
            (
                ErrorScope::Mailbox { id: "b".into() },
                Some(ResourceKind::Mailbox),
                Some("b"),
            ),
            (
                ErrorScope::Thread { id: "t".into() },
                Some(ResourceKind::Thread),
                Some("t"),
            ),
            (
                ErrorScope::Calendar { id: "c".into() },
                Some(ResourceKind::Calendar),
                Some("c"),
            ),
            (
                ErrorScope::Event {
                    id: crate::calendar::EventId("e".to_owned()),
                },
                Some(ResourceKind::Event),
                Some("e"),
            ),
            (
                ErrorScope::Contact { id: "p".into() },
                Some(ResourceKind::Contact),
                Some("p"),
            ),
            (ErrorScope::Submission { id: "s".into() }, None, Some("s")),
            (ErrorScope::CloudFile { id: "f".into() }, None, Some("f")),
            (ErrorScope::UploadSession, None, None),
            (ErrorScope::Account, None, None),
            (ErrorScope::Cursor(CursorScope::Account), None, None),
            (
                ErrorScope::Cursor(CursorScope::Folder(FolderId("folder".to_owned()))),
                None,
                None,
            ),
            (
                ErrorScope::CalendarCollection,
                Some(ResourceKind::Calendar),
                None,
            ),
            (
                ErrorScope::ContactCollection,
                Some(ResourceKind::Contact),
                None,
            ),
            (
                ErrorScope::DirectoryGateway {
                    url: "https://example.test/".into(),
                },
                None,
                None,
            ),
        ];
        for (scope, resource, id) in cases {
            assert_eq!(scope.resource(), resource, "{scope:?}");
            assert_eq!(scope.object_id(), id, "{scope:?}");
        }
    }

    /// `scope_fields` is the single source for both the declared struct
    /// field count and the fields written, so pinning it pins the
    /// serialized shape. A count that overstates the fields written
    /// corrupts length-prefixed output.
    #[test]
    fn scope_fields_pin_the_serialized_shape() {
        let cases: Vec<(ErrorScope, Vec<(&str, &str)>)> = vec![
            (ErrorScope::Account, vec![("kind", "account")]),
            (
                ErrorScope::Namespace {
                    prefix: "Shared/".into(),
                },
                vec![("kind", "namespace"), ("prefix", "Shared/")],
            ),
            (
                ErrorScope::Submission { id: "s".into() },
                vec![("kind", "submission"), ("id", "s")],
            ),
            (
                ErrorScope::CloudFile { id: "f".into() },
                vec![("kind", "cloud_file"), ("id", "f")],
            ),
            (ErrorScope::UploadSession, vec![("kind", "upload_session")]),
            (
                ErrorScope::DirectoryGateway {
                    url: "https://dav.example.test/gal/".into(),
                },
                vec![
                    ("kind", "directory_gateway"),
                    ("url", "https://dav.example.test/gal/"),
                ],
            ),
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
                ErrorScope::Event {
                    id: crate::EventId("e".into()),
                },
                vec![("kind", "event"), ("id", "e")],
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
