//! Foreign (shared/delegate) mailbox folder codec.
//!
//! A Graph delegate/shared mailbox surfaces as ordinary
//! `CursorScope::FolderType { folder, ty }` scopes; the owning mailbox
//! identity rides inside the `FolderId` string so the scope is distinct
//! per (mailbox, folder) and self-routing on a cold cursor resume. This
//! keeps `CursorScope` untouched (the engine never inspects the payload)
//! and mirrors how IMAP namespaces other-user folders by path.
//!
//! This module owns owner-qualified and public-folder id spellings and the
//! same-owner rule. Graph mailbox routing compares owner keys ASCII-case-insensitively
//! while preserving their stored spelling. JMAP account keys compare exactly
//! and strip qualification only for the matching shared route. Keep these
//! provider policies here; public-folder identities name an EWS routing
//! collection rather than an optional mailbox owner.

#[cfg(test)]
use bifrost_types::CursorScope;
use bifrost_types::{FolderId, MembershipScope, ObjectId, PrincipalId, ThreadId};

/// Reserved separator. A `FolderId` of the form `"<mailbox>\u{1f}<folder>"`
/// is a foreign-mailbox folder; a plain id is the primary mailbox.
/// `\u{1f}` (US, unit separator) cannot appear in a Graph folder id, an
/// SMTP address, or a user id, so it is unambiguous as a delimiter.
const FOREIGN_SEP: char = '\u{1f}';

/// Reserved separator for a PUBLIC-folder item id: `"<folderId>\u{1e}<itemId>"`.
/// A distinct delimiter from `FOREIGN_SEP` because the two namespaces mean
/// different things - a foreign id carries a `/users/{mailbox}` routing key
/// and reads over Graph REST, a public id carries the EWS folder whose
/// `routing_map` entry supplies the `X-AnchorMailbox` /
/// `X-PublicFolderMailbox` pair and reads over EWS `GetItem`. `\u{1e}` (RS,
/// record separator) cannot appear in an EWS folder or item id, so the two
/// forms are mutually unambiguous.
const PUBLIC_SEP: char = '\u{1e}';

/// The outer API type supplies the namespace: a bare thread and a bare
/// message have identical bytes, so a context-free parser cannot distinguish
/// them. Keep the historical spellings: Outlook users must retain access to
/// identities already persisted by the consumer.
#[derive(Debug, Clone, Copy)]
pub(crate) enum GraphId<'a> {
    Native(&'a str),
    Owned {
        owner: &'a str,
        native: &'a str,
    },
    Event {
        calendar: &'a str,
        event: &'a str,
    },
    PublicItem {
        folder: &'a str,
        item: &'a str,
    },
    PublicCollection {
        namespace: &'a str,
        folder: &'a str,
    },
    PublicTypedItem {
        namespace: &'a str,
        folder: &'a str,
        item: &'a str,
    },
}

impl<'a> GraphId<'a> {
    pub(crate) fn parse_owned(value: &'a str) -> Self {
        // Retain first-split decoding, even for malformed old owner tags.
        // Routing must refuse an empty/stale owner, never fall back to /me.
        match value.split_once(FOREIGN_SEP) {
            Some((owner, native)) => Self::Owned { owner, native },
            None => Self::Native(value),
        }
    }

    pub(crate) fn owner_native(self) -> (Option<&'a str>, &'a str) {
        match self {
            Self::Native(native) => (None, native),
            Self::Owned { owner, native } => (Some(owner), native),
            _ => unreachable!("not an owner-qualified id"),
        }
    }

    pub(crate) fn parse_event(value: &'a str) -> Option<Self> {
        let (calendar, event) = value.split_once("::")?;
        (!calendar.is_empty() && !event.is_empty()).then_some(Self::Event { calendar, event })
    }

    pub(crate) fn encode(self) -> String {
        match self {
            Self::Native(native) => native.to_string(),
            Self::Owned { owner, native } => format!("{owner}{FOREIGN_SEP}{native}"),
            Self::Event { calendar, event } => format!("{calendar}::{event}"),
            Self::PublicItem { folder, item } => format!("{folder}{PUBLIC_SEP}{item}"),
            Self::PublicCollection { namespace, folder } => {
                format!("{namespace}{PUBLIC_SEP}{folder}")
            }
            Self::PublicTypedItem {
                namespace,
                folder,
                item,
            } => {
                format!("{namespace}{PUBLIC_SEP}{folder}{PUBLIC_SEP}{item}")
            }
        }
    }
}

/// A foreign-mailbox folder: the routing mailbox plus the native Graph
/// folder id within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignFolder {
    /// The `/users/{mailbox}` routing key (SMTP address or user id).
    pub(crate) mailbox: String,
    /// The native Graph folder id.
    pub(crate) folder: String,
}

/// The result of parsing a `FolderId`: either a primary-mailbox folder
/// (a plain native id) or a foreign-mailbox folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedFolder {
    Primary(String),
    Foreign(ForeignFolder),
}

impl ParsedFolder {
    /// The native Graph folder id, regardless of variant - the value that
    /// goes into a `/mailFolders/{id}` REST path.
    pub(crate) fn native_id(&self) -> &str {
        match self {
            Self::Primary(id) => id,
            Self::Foreign(foreign) => &foreign.folder,
        }
    }

    /// The foreign half, when this folder belongs to a shared mailbox.
    pub(crate) fn foreign(&self) -> Option<&ForeignFolder> {
        match self {
            Self::Primary(_) => None,
            Self::Foreign(foreign) => Some(foreign),
        }
    }
}

/// Encode a `(mailbox, native folder)` pair into the namespaced
/// `FolderId` carried in a foreign scope.
pub(crate) fn encode_foreign(mailbox: &str, folder: &str) -> FolderId {
    FolderId(qualify_with_owner(Some(mailbox), folder))
}

/// Parse a `FolderId` back into a `ParsedFolder`. Splits on the first
/// `FOREIGN_SEP`; a folder id with no separator is a primary-mailbox
/// folder.
pub(crate) fn parse_folder(folder: &FolderId) -> ParsedFolder {
    let (owner, native) = GraphId::parse_owned(&folder.0).owner_native();
    match owner {
        Some(mailbox) => ParsedFolder::Foreign(ForeignFolder {
            mailbox: mailbox.to_string(),
            folder: native.to_string(),
        }),
        None => ParsedFolder::Primary(native.to_string()),
    }
}

/// The `MembershipScope` owner tag for a foreign folder: the mailbox
/// identity as a `PrincipalId`. Emitted on discovery so the consumer maps
/// the scope to its shared-mailbox owner. The engine's covering rule
/// cannot infer ownership from opaque folder identities.
pub(crate) fn owner_tag(mailbox: &str) -> MembershipScope {
    MembershipScope::Owner(PrincipalId(mailbox.to_string()))
}

/// A parsed Graph message/blob `ObjectId`: either a primary-mailbox item
/// (a bare native message id, routed through `/me`) or a foreign-mailbox
/// item carrying the `/users/{mailbox}` routing key it must be fetched
/// from. The owning mailbox is encoded into the id string exactly like the
/// folder codec above (same `\u{1f}` separator, same precedent as the
/// `pim/send.rs` scheduled-send handle): the `bifrost-types::ObjectId` type is
/// unchanged, only its string content carries the routing key.
///
/// This is what makes hydration / blob / raw-RFC822 reads route to
/// `/users/{owner}/...` for a shared mailbox instead of 404-ing on `/me`.
/// The mint sites (inventory / changes projection) encode foreign-scope
/// ids; the request sites decode and route. A primary item is never
/// encoded, so one logical item has exactly one wire form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedMessageId {
    Primary(String),
    Foreign {
        mailbox: String,
        message: String,
    },
    /// An Exchange public-folder item. Carries the EWS folder id, because
    /// that is the `routing_map` key holding the item's
    /// `X-AnchorMailbox` / `X-PublicFolderMailbox` routing pair - a public
    /// folder has no owning mailbox of its own to route by. Read over EWS
    /// `GetItem` / `GetAttachment`, never Graph REST (`/me/messages/{id}`
    /// does not know a raw EWS `ItemId`).
    Public {
        folder: String,
        item: String,
    },
}

impl ParsedMessageId {
    /// The native Graph message id, regardless of variant - the value that
    /// goes into a `/messages/{id}` REST path or an EWS `<t:ItemId>`.
    pub(crate) fn native_id(&self) -> &str {
        match self {
            Self::Primary(id) => id,
            Self::Foreign { message, .. } => message,
            Self::Public { item, .. } => item,
        }
    }

    /// The owning mailbox routing key when this id belongs to a shared
    /// mailbox; `None` for a primary-mailbox or public-folder item.
    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Primary(_) | Self::Public { .. } => None,
            Self::Foreign { mailbox, .. } => Some(mailbox),
        }
    }

    /// The public folder this item lives in, when it is a public-folder
    /// item. `Some(_)` is exactly the discriminator that routes a read onto
    /// the EWS arm instead of the Graph REST arm.
    pub(crate) fn public_folder(&self) -> Option<&str> {
        match self {
            Self::Public { folder, .. } => Some(folder),
            _ => None,
        }
    }
}

/// Encode a `(public folder, native EWS item id)` pair into the `ObjectId`
/// the public-folder inventory / poll projections emit. The folder rides
/// along so hydration and attachment reads can look its routing headers up
/// in `routing_map` - without it a public-folder item would hydrate through
/// Graph REST, which cannot address a raw EWS `ItemId` at all.
pub(crate) fn encode_public_item_id(folder: &FolderId, item: &str) -> ObjectId {
    ObjectId(
        GraphId::PublicItem {
            folder: &folder.0,
            item,
        }
        .encode(),
    )
}

/// Encode a `(scope, native message id)` pair into the `ObjectId` carried
/// out of the inventory / changes projection. When the scope is a foreign
/// (shared/delegate) folder, the owning mailbox is prefixed so later reads
/// route to `/users/{mailbox}`; a primary scope yields a bare native id.
#[cfg(test)]
pub(crate) fn encode_message_id(scope: &CursorScope, native: &str) -> ObjectId {
    match scope {
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => {
            ObjectId(qualify_with_owner(folder_owner(folder).as_deref(), native))
        }
        _ => ObjectId(native.to_string()),
    }
}

/// A parsed Graph conversation id. Like message ids, a thread from a shared
/// mailbox must retain its owner: Graph conversation ids are only unique
/// within a mailbox, and thread operations resolve their member messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedThreadId {
    Primary(String),
    Foreign { mailbox: String, thread: String },
}

impl ParsedThreadId {
    pub(crate) fn native_id(&self) -> &str {
        match self {
            Self::Primary(id) => id,
            Self::Foreign { thread, .. } => thread,
        }
    }

    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Primary(_) => None,
            Self::Foreign { mailbox, .. } => Some(mailbox),
        }
    }
}

/// Qualify a native Graph id with an optional shared-mailbox owner.
///
/// Every id an owner-aware projection re-mints - message, conversation,
/// parent folder - goes through this one function so the four id
/// namespaces cannot drift apart on the separator. `None` yields the bare
/// native id, which is the primary mailbox's only wire form.
pub(crate) fn qualify_with_owner(owner: Option<&str>, native: &str) -> String {
    match owner {
        // Preserve the stored owner spelling; routing alone folds ASCII case.
        Some(owner) => GraphId::Owned { owner, native }.encode(),
        None => GraphId::Native(native).encode(),
    }
}

/// Compare mailbox owners without treating differently cased old owner tags
/// as a cross-mailbox move. None remains the primary route, not a wildcard.
pub(crate) fn same_owner(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

/// The shared-mailbox owner of a `FolderId`, or `None` for the primary
/// mailbox. The folder-shaped twin of `ParsedMessageId::owner`.
pub(crate) fn folder_owner(folder: &FolderId) -> Option<String> {
    parse_folder(folder)
        .foreign()
        .map(|foreign| foreign.mailbox.clone())
}

/// Encode a Graph conversation id with the same mailbox qualification as its
/// messages. Primary-mailbox thread ids deliberately remain bare.
#[cfg(test)]
pub(crate) fn encode_thread_id(scope: &CursorScope, native: &str) -> ThreadId {
    match scope {
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => {
            ThreadId(qualify_with_owner(folder_owner(folder).as_deref(), native))
        }
        _ => ThreadId(native.to_string()),
    }
}

/// Decode a thread id into the native Graph conversation id plus its optional
/// shared-mailbox owner.
pub(crate) fn parse_thread_id(id: &ThreadId) -> ParsedThreadId {
    let (owner, native) = GraphId::parse_owned(&id.0).owner_native();
    match owner {
        Some(mailbox) => ParsedThreadId::Foreign {
            mailbox: mailbox.to_string(),
            thread: native.to_string(),
        },
        None => ParsedThreadId::Primary(native.to_string()),
    }
}

/// Parse an `ObjectId` back into a `ParsedMessageId`. The public-folder form
/// is checked first (its `PUBLIC_SEP` is the more specific marker), then the
/// foreign-mailbox form; an id with neither separator is a primary-mailbox
/// item. Both splits take the FIRST separator only.
pub(crate) fn parse_message_id(id: &ObjectId) -> ParsedMessageId {
    if let Some((folder, item)) = id.0.split_once(PUBLIC_SEP) {
        return ParsedMessageId::Public {
            folder: folder.to_string(),
            item: item.to_string(),
        };
    }
    let (owner, native) = GraphId::parse_owned(&id.0).owner_native();
    match owner {
        Some(mailbox) => ParsedMessageId::Foreign {
            mailbox: mailbox.to_string(),
            message: native.to_string(),
        },
        None => ParsedMessageId::Primary(native.to_string()),
    }
}

/// Typed public contact collections and items carry a namespace tag even
/// before discovery has populated routing. Native EWS ids never contain RS.
pub(crate) fn public_address_book(folder: &str) -> bifrost_types::AddressBookId {
    bifrost_types::AddressBookId(
        GraphId::PublicCollection {
            namespace: "public-contacts",
            folder,
        }
        .encode(),
    )
}

pub(crate) fn public_contact(folder: &str, item: &str) -> bifrost_types::ContactId {
    bifrost_types::ContactId(
        GraphId::PublicTypedItem {
            namespace: "public-contacts",
            folder,
            item,
        }
        .encode(),
    )
}

pub(crate) fn parse_public_address_book(id: &str) -> Option<&str> {
    parse_public_namespace(id, "public-contacts")
}

pub(crate) fn parse_public_contact(id: &str) -> Option<(&str, &str)> {
    parse_public_address_book(id).and_then(parse_public_item)
}

fn parse_public_namespace<'a>(id: &'a str, namespace: &str) -> Option<&'a str> {
    let (tag, tail) = id.split_once(PUBLIC_SEP)?;
    (tag == namespace).then_some(tail)
}

fn parse_public_item(tail: &str) -> Option<(&str, &str)> {
    let (folder, item) = tail.split_once(PUBLIC_SEP)?;
    (!folder.is_empty() && !item.is_empty() && !item.contains(PUBLIC_SEP)).then_some((folder, item))
}

/// Typed calendar ids remain public even when discovery routing is absent.
pub(crate) fn public_calendar(folder: &str) -> bifrost_types::CalendarId {
    bifrost_types::CalendarId(
        GraphId::PublicCollection {
            namespace: "public-calendar",
            folder,
        }
        .encode(),
    )
}
pub(crate) fn public_event(folder: &str, item: &str) -> bifrost_types::EventId {
    bifrost_types::EventId(
        GraphId::PublicTypedItem {
            namespace: "public-calendar",
            folder,
            item,
        }
        .encode(),
    )
}
pub(crate) fn parse_public_calendar(id: &str) -> Option<&str> {
    parse_public_namespace(id, "public-calendar")
}
pub(crate) fn parse_public_event(id: &str) -> Option<(&str, &str)> {
    parse_public_calendar(id).and_then(parse_public_item)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_comparison_folds_ascii_case_but_ids_keep_their_spelling() {
        assert!(same_owner(
            Some("Shared@Contoso.com"),
            Some("shared@contoso.com")
        ));
        assert!(!same_owner(None, Some("shared@contoso.com")));
        assert!(!same_owner(Some("a@contoso.com"), Some("b@contoso.com")));
        // Ids are durable: the configured spelling is never rewritten.
        assert_eq!(
            encode_foreign("Shared@Contoso.com", "Folder").0,
            format!("Shared@Contoso.com{FOREIGN_SEP}Folder")
        );
        assert_eq!(
            qualify_with_owner(Some("Shared@Contoso.com"), "Item"),
            format!("Shared@Contoso.com{FOREIGN_SEP}Item")
        );
        assert_eq!(
            owner_tag("Shared@Contoso.com"),
            MembershipScope::Owner(PrincipalId("Shared@Contoso.com".into()))
        );
        assert!(!same_owner(
            Some("\u{c4}@contoso.com"),
            Some("\u{e4}@contoso.com")
        ));
    }

    #[test]
    fn public_item_ids_require_both_identity_components() {
        for prefix in ["public-contacts", "public-calendar"] {
            for tail in [
                "",
                "folder",
                "folder\u{1e}",
                "\u{1e}item",
                "folder\u{1e}item\u{1e}extra",
            ] {
                let id = format!("{prefix}\u{1e}{tail}");
                assert!(parse_public_contact(&id).is_none());
                assert!(parse_public_event(&id).is_none());
            }
        }
        assert_eq!(
            parse_public_contact(&public_contact("folder", "item").0),
            Some(("folder", "item")),
        );
        assert_eq!(
            parse_public_event(&public_event("folder", "item").0),
            Some(("folder", "item")),
        );
    }

    #[test]
    fn foreign_round_trips_through_folder_id() {
        let id = encode_foreign("shared@contoso.com", "AAMkAGI2");
        match parse_folder(&id) {
            ParsedFolder::Foreign(ForeignFolder { mailbox, folder }) => {
                assert_eq!(mailbox, "shared@contoso.com");
                assert_eq!(folder, "AAMkAGI2");
            }
            other => panic!("expected foreign, got {other:?}"),
        }
    }

    #[test]
    fn foreign_native_id_strips_prefix() {
        let id = encode_foreign("shared@contoso.com", "AAMkAGI2");
        let parsed = parse_folder(&id);
        assert_eq!(parsed.native_id(), "AAMkAGI2");
        assert!(parsed.foreign().is_some());
    }

    #[test]
    fn plain_id_is_primary() {
        let parsed = parse_folder(&FolderId("inbox".to_string()));
        assert_eq!(parsed, ParsedFolder::Primary("inbox".to_string()));
        assert_eq!(parsed.native_id(), "inbox");
        assert!(parsed.foreign().is_none());
    }

    #[test]
    fn owner_tag_is_mailbox_membership() {
        assert_eq!(
            owner_tag("shared@contoso.com"),
            MembershipScope::Owner(PrincipalId("shared@contoso.com".to_string()))
        );
    }

    #[test]
    fn message_id_encodes_foreign_scope_and_round_trips() {
        let scope = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let id = encode_message_id(&scope, "AAMkmessage");
        // The encoded id carries the owning mailbox, not the folder.
        assert_eq!(id.0, format!("shared@contoso.com{FOREIGN_SEP}AAMkmessage"));
        let parsed = parse_message_id(&id);
        assert_eq!(parsed.native_id(), "AAMkmessage");
        assert_eq!(parsed.owner(), Some("shared@contoso.com"));
        assert_eq!(
            parsed,
            ParsedMessageId::Foreign {
                mailbox: "shared@contoso.com".to_string(),
                message: "AAMkmessage".to_string(),
            }
        );
    }

    #[test]
    fn message_id_leaves_primary_scope_bare() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: bifrost_types::ObjectType::Email,
        };
        let id = encode_message_id(&scope, "AAMkmessage");
        assert_eq!(id.0, "AAMkmessage");
        let parsed = parse_message_id(&id);
        assert_eq!(parsed, ParsedMessageId::Primary("AAMkmessage".to_string()));
        assert_eq!(parsed.native_id(), "AAMkmessage");
        assert_eq!(parsed.owner(), None);
    }

    #[test]
    fn thread_id_encodes_foreign_scope_and_round_trips() {
        let scope = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let id = encode_thread_id(&scope, "conversation-1");
        assert_eq!(
            id.0,
            format!("shared@contoso.com{FOREIGN_SEP}conversation-1")
        );
        let parsed = parse_thread_id(&id);
        assert_eq!(parsed.native_id(), "conversation-1");
        assert_eq!(parsed.owner(), Some("shared@contoso.com"));
    }

    #[test]
    fn primary_thread_id_stays_bare() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: bifrost_types::ObjectType::Email,
        };
        let id = encode_thread_id(&scope, "conversation-1");
        assert_eq!(id.0, "conversation-1");
        assert_eq!(parse_thread_id(&id).owner(), None);
    }

    #[test]
    fn message_id_round_trip_is_byte_stable() {
        // Re-encoding the decoded native id under the same scope yields
        // the identical bytes - invariant 1 (one encoding per item).
        let scope = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let once = encode_message_id(&scope, "AAMkmessage");
        let twice = encode_message_id(&scope, parse_message_id(&once).native_id());
        assert_eq!(once, twice);
    }

    #[test]
    fn public_item_id_round_trips_and_is_distinct_from_foreign() {
        let folder = FolderId("AAMkPF=".to_string());
        let id = encode_public_item_id(&folder, "AAMkItem=");
        assert_eq!(id.0, format!("AAMkPF={PUBLIC_SEP}AAMkItem="));

        let parsed = parse_message_id(&id);
        assert_eq!(parsed.native_id(), "AAMkItem=");
        assert_eq!(parsed.public_folder(), Some("AAMkPF="));
        // A public item has no owning mailbox: its routing comes from the
        // folder's `routing_map` entry, not from a `/users/{mailbox}` key.
        assert_eq!(parsed.owner(), None);

        // A foreign-mailbox id is NOT mistaken for a public one, and vice
        // versa.
        let foreign = encode_message_id(
            &CursorScope::FolderType {
                folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
                ty: bifrost_types::ObjectType::Email,
            },
            "AAMkmessage",
        );
        assert_eq!(parse_message_id(&foreign).public_folder(), None);
        assert_eq!(
            parse_message_id(&foreign).owner(),
            Some("shared@contoso.com")
        );

        // Re-encoding a decoded public id is byte-stable.
        let twice = encode_public_item_id(&folder, parse_message_id(&id).native_id());
        assert_eq!(id, twice);
    }

    #[test]
    fn a_bare_folder_scope_mints_an_unqualified_message_id() {
        // `CursorScope::Folder` is the public-folder shape, and its
        // `FolderId` is never foreign-encoded, so `encode_message_id`
        // returns the native id bare. Public-folder projections must
        // therefore use `encode_public_item_id`, NOT this function - the
        // folder qualification is what routes the read onto EWS at all.
        let scope = CursorScope::Folder(FolderId("AAMkPF=".to_string()));
        let id = encode_message_id(&scope, "AAMkItem=");
        assert_eq!(id.0, "AAMkItem=");
        assert_eq!(parse_message_id(&id).public_folder(), None);
    }

    #[test]
    fn a_non_folder_scope_mints_an_unqualified_message_id() {
        let id = encode_message_id(&CursorScope::Account, "AAMkmessage");
        assert_eq!(id.0, "AAMkmessage");
        assert_eq!(parse_message_id(&id).owner(), None);
    }

    #[test]
    fn the_public_separator_is_checked_before_the_foreign_one() {
        // The two namespaces must stay mutually unambiguous even when a
        // single id carries both bytes: the RS-separated public form wins,
        // because that is the only shape either mint site produces.
        let id = ObjectId(format!("AAMkPF={PUBLIC_SEP}mailbox{FOREIGN_SEP}item"));
        match parse_message_id(&id) {
            ParsedMessageId::Public { folder, item } => {
                assert_eq!(folder, "AAMkPF=");
                assert_eq!(item, format!("mailbox{FOREIGN_SEP}item"));
            }
            other => panic!("expected Public, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_mailbox_prefix_still_parses_as_foreign() {
        // Degenerate but constructible from a misconfigured
        // `with_shared_mailbox("")`: the id parses foreign with an empty
        // owner. Routing rejects that owner as stale configuration; it
        // must never be mistaken for a primary-mailbox id.
        let parsed = parse_message_id(&ObjectId(format!("{FOREIGN_SEP}AAMkmsg")));
        assert_eq!(parsed.owner(), Some(""));
        assert_eq!(parsed.native_id(), "AAMkmsg");
    }

    /// And the rejection is real even when the misconfiguration is
    /// literally present: `with_shared_mailbox("")` no longer installs a
    /// client under the empty key, so the empty owner above resolves to
    /// nothing. An installed empty key produced the malformed prefix
    /// `/users/` and turned a local configuration error into an opaque
    /// remote 400.
    #[test]
    fn an_empty_configured_mailbox_installs_no_client_to_route_to() {
        let account = super::super::GraphAccount::new_for_tests_with_shared(
            crate::client::GraphClient::new("token"),
            super::super::PushMode::GraphSubscriptions,
            &[String::new(), "shared@contoso.com".to_string()],
        );
        assert!(matches!(
            account.mailboxes.client_for_owner(Some("")),
            Err(crate::error::GraphError::Configuration { .. })
        ));
        // The well-formed sibling of the same configuration still routes.
        assert_eq!(
            account
                .mailboxes
                .client_for_owner(Some("shared@contoso.com"))
                .expect("configured")
                .api_path_prefix(),
            "/users/shared%40contoso.com"
        );
    }

    #[test]
    fn a_trailing_separator_yields_an_empty_native_id() {
        let parsed = parse_folder(&FolderId(format!("shared@contoso.com{FOREIGN_SEP}")));
        assert_eq!(parsed.native_id(), "");
        assert_eq!(
            parsed.foreign().map(|f| f.mailbox.as_str()),
            Some("shared@contoso.com")
        );
    }

    #[test]
    fn encode_foreign_and_encode_message_id_agree_on_the_separator() {
        // Discovery mints the SCOPE with `encode_foreign(mailbox, folder)`
        // and projections mint the ITEM with `encode_message_id`; both must
        // use the same delimiter or `parse_message_id` would read a folder
        // id as an owner (or vice versa).
        let scope = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let item = encode_message_id(&scope, "AAMkmsg");
        let folder = encode_foreign("shared@contoso.com", "AAMkfolder");
        assert_eq!(
            item.0.split_once(FOREIGN_SEP).map(|(m, _)| m),
            folder.0.split_once(FOREIGN_SEP).map(|(m, _)| m)
        );
    }

    #[test]
    fn bare_message_id_parses_as_primary() {
        let parsed = parse_message_id(&ObjectId("AAMkmessage".to_string()));
        assert_eq!(parsed, ParsedMessageId::Primary("AAMkmessage".to_string()));
        assert_eq!(parsed.owner(), None);
    }

    #[test]
    fn folder_id_with_separator_in_native_part_splits_on_first() {
        // A native folder id can never contain US, but guard the split
        // semantics regardless: only the first separator delimits.
        let id = FolderId(format!("box{FOREIGN_SEP}a{FOREIGN_SEP}b"));
        match parse_folder(&id) {
            ParsedFolder::Foreign(ForeignFolder { mailbox, folder }) => {
                assert_eq!(mailbox, "box");
                assert_eq!(folder, format!("a{FOREIGN_SEP}b"));
            }
            other => panic!("expected foreign, got {other:?}"),
        }
    }
}
