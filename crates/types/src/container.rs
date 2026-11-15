//! Container, label, and provenance types for the unified PIM
//! surface.
//!
//! A `Container` is a folder, label, or mailbox the account exposes
//! as a place a message can live. The unified shape lets the consumer
//! branch on `kind` (Folder | Label) when it wants a folder-binary
//! UI, on `role` when it wants ratatoskr's canonical INBOX / SENT /
//! DRAFT / ARCHIVE / TRASH / SPAM rendering, or on `provenance` when
//! it needs the wire-level native id to round-trip back to the
//! protocol.

use crate::cursor::ProtocolKind;
use crate::ids::{ObjectId, PrincipalId};
use crate::page::SkippedScope;

/// Which namespace a container lives in.
///
/// `Personal` is the account's own mailbox: every container a protocol
/// enumerates for the authenticated principal itself. `Shared` is a
/// delegate / shared / other-user mailbox (Graph `/users/{owner}`, a
/// non-personal JMAP account, an IMAP other-user or shared namespace).
/// `Public` is an Exchange public folder - organization-wide content
/// owned by no principal at all.
///
/// The consumer branches on this to route a container into the right
/// sidebar section and to decide which sync policy applies (a public
/// folder is opt-in and allowlisted; a shared mailbox is not).
/// Defaults to `Personal` so every existing construction site keeps its
/// current meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ContainerNamespace {
    /// The authenticated principal's own mailbox.
    #[default]
    Personal,
    /// A delegate / shared / other-user mailbox, owned by another
    /// principal. [`Container::owner`] names that principal.
    Shared,
    /// An Exchange public folder: organization-wide, no owning
    /// principal.
    Public,
}

/// What kind of items a container holds, when the protocol says so.
///
/// Exchange public folders are typed at the folder level (`IPF.Note`,
/// `IPF.Appointment`, `IPF.Contact`, ...) and a mail client must not
/// present a calendar public folder as a mail folder. Only the Graph
/// public-folder projection populates this today, from the EWS
/// `FolderClass` value; every other provider leaves
/// [`Container::content_class`] `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ContainerContentClass {
    /// Mail items (`IPF.Note`).
    Mail,
    /// Calendar items (`IPF.Appointment`).
    Calendar,
    /// Contacts (`IPF.Contact`).
    Contacts,
    /// A class the unified surface does not model (tasks, journals,
    /// notes, an unrecognized `IPF.*` value).
    Other,
}

/// Folder/label classification. Folder-shaped (IMAP, Graph) versus
/// label-shaped (Gmail). JMAP mailboxes are folder-shaped on the
/// wire (a message lives in one or more mailboxes) but consumers
/// often render them like labels - so consumers branch on `kind`
/// when the rendering distinction matters and ignore it when it does
/// not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ContainerKind {
    /// Hierarchical folder (IMAP mailbox path, Graph mail folder,
    /// JMAP `Mailbox` object).
    Folder,
    /// Flat label (Gmail user label, Gmail system label).
    Label,
}

/// Canonical role a container plays in ratatoskr's UI. `None` (encoded
/// as `Option<FolderRole>::None`) means the container is a user-created
/// container without a system role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FolderRole {
    Inbox,
    Sent,
    Drafts,
    Archive,
    Trash,
    Spam,
}

/// Display color a protocol carries for a container or label.
///
/// Both fields are protocol-native color strings exactly as the wire
/// surfaced them (Gmail label colors are `#rrggbb` hex). The pair lets
/// the consumer reproduce the protocol's own label/folder swatch
/// without re-deriving a palette.
///
/// Only Gmail populates this today, from the label's `color`
/// (`backgroundColor` / `textColor`). Folder-shaped protocols (IMAP
/// special-use, Graph mail folders, JMAP mailboxes) carry no container
/// color and leave `Container::style` / `Label::style` `None`. Graph
/// categories do carry a color, but a Graph category is a message flag,
/// not a container, so it never reaches this surface.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `ContainerStyle` directly when populating containers and labels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerStyle {
    /// Background color (Gmail `color.backgroundColor`).
    pub color_bg: String,
    /// Foreground / text color (Gmail `color.textColor`).
    pub color_fg: String,
}

impl ContainerStyle {
    /// Construct a style from a background / foreground color pair.
    #[must_use]
    pub fn new(color_bg: impl Into<String>, color_fg: impl Into<String>) -> Self {
        Self {
            color_bg: color_bg.into(),
            color_fg: color_fg.into(),
        }
    }
}

/// Per-folder access rights a protocol surfaces for a container.
///
/// Each field maps one-to-one onto an RFC 8621 `Mailbox/myRights`
/// member: the rights the authenticated principal holds on this
/// mailbox. The load-bearing consumer use is shared-mailbox
/// submit-gating - `may_submit` tells the UI whether the principal is
/// allowed to send from this mailbox, and the other members gate
/// read / add / remove / flag / child-create / rename / delete.
///
/// Every field is `Option<bool>`: `Some(b)` is the value the protocol
/// reported, `None` means the protocol did not report that member.
/// JMAP populates this from `Mailbox.myRights`, IMAP from RFC 4314
/// MYRIGHTS on a shared folder, and Graph from EWS folder
/// `EffectiveRights` on a public folder. Label-shaped Gmail has no
/// per-folder ACL model and leaves `Container::rights` `None`, as does
/// any folder the protocol never reported rights for (an unprobed
/// personal IMAP folder, a Graph REST mail folder).
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `ContainerRights` directly when populating containers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ContainerRights {
    /// `mayReadItems`: may read the messages in this mailbox.
    pub may_read_items: Option<bool>,
    /// `mayAddItems`: may add messages to this mailbox.
    pub may_add_items: Option<bool>,
    /// `mayRemoveItems`: may remove messages from this mailbox.
    pub may_remove_items: Option<bool>,
    /// `maySetSeen`: may set the `$seen` keyword on messages here.
    pub may_set_seen: Option<bool>,
    /// `maySetKeywords`: may set any keyword (other than `$seen`) on
    /// messages here.
    pub may_set_keywords: Option<bool>,
    /// `mayCreateChild`: may create a child mailbox under this one.
    pub may_create_child: Option<bool>,
    /// `mayRename`: may rename this mailbox or move it.
    pub may_rename: Option<bool>,
    /// `mayDelete`: may delete this mailbox.
    pub may_delete: Option<bool>,
    /// `maySubmit`: may submit (send) messages from this mailbox.
    pub may_submit: Option<bool>,
    /// May change sharing permissions; `None` means the provider did not report it.
    pub may_share: Option<bool>,
}

/// Wire-level provenance for a container or label id.
///
/// Carries enough context for the consumer to know which protocol
/// minted the id, what kind of object it points at, and what the
/// native id string actually was on the wire. The convenience layer
/// inspects this to decide whether to dispatch `apply_label` into
/// `set_keyword`, `set_label_membership`, `set_category`, or
/// `set_extended_property`.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Provenance` directly when populating containers and labels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Provenance {
    /// Which protocol family the id was minted by.
    pub provider: ProtocolKind,
    /// Folder versus label, as the protocol exposed it.
    pub kind: ContainerKind,
    /// Native id string the protocol uses on the wire.
    pub native: String,
}

/// Engine-facing identifier for a container. Wraps `ObjectId` so the
/// trait surface stays consistent with the rest of bifrost-types but
/// carries the dedicated semantic meaning of "this id points at a
/// container, not a message".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(pub String);

impl From<ObjectId> for ContainerId {
    fn from(id: ObjectId) -> Self {
        Self(id.0)
    }
}

impl From<ContainerId> for ObjectId {
    fn from(id: ContainerId) -> Self {
        Self(id.0)
    }
}

/// Unified container shape returned by `Account::containers_list`.
///
/// `kind` is the folder/label classification. `role` is the canonical
/// ratatoskr role when the protocol claims one. `provenance` is the
/// wire-level metadata so the consumer can round-trip back to the
/// protocol without ambiguity. `native_id()` borrows `provenance.native`
/// rather than storing a second copy that could disagree.
///
/// Deliberately NOT `#[non_exhaustive]`: protocol Account impls and the
/// downstream consumer both construct `Container` values directly, via
/// `Container::new(..)` plus the `with_*` setters. That pairing is the
/// stability contract - `Container::new`'s positional arity must stay
/// fixed, and every future field lands as a defaulted field plus a new
/// `with_*` setter. Adding a positional parameter to `new` would break
/// every call site, including the consumer's, which is exactly what the
/// builder discipline exists to prevent.
#[derive(Debug, Clone)]
pub struct Container {
    /// Engine-facing identifier.
    pub id: ContainerId,
    /// Folder versus label for presentation. This need not equal the wire
    /// classification in provenance: consumers can present a native label as
    /// a folder. Keep both choices expressible rather than silently changing
    /// the protocol primitive used to round-trip its identity.
    pub kind: ContainerKind,
    /// Canonical role, when the container plays one.
    pub role: Option<FolderRole>,
    /// Wire-level provenance.
    provenance: Provenance,
    /// Display name as the protocol surfaces it.
    pub name: String,
    /// Parent container, when nested (Graph mail folders, IMAP
    /// hierarchical mailboxes). `None` for top-level containers and
    /// for flat-namespace protocols.
    pub parent: Option<ContainerId>,
    /// Display color, when the protocol surfaces one. Only Gmail
    /// populates this (from the label's `color`); folder-shaped
    /// protocols leave it `None`. See [`ContainerStyle`].
    pub style: Option<ContainerStyle>,
    /// `true` when the protocol marks this container as a native
    /// system container. The load-bearing case is Gmail: it tags many
    /// more labels as system (`CATEGORY_*`, `IMPORTANT`, `CHAT`, ...)
    /// than ever receive a [`FolderRole`], so `role` alone cannot
    /// reproduce Gmail's system-label-as-folder split - `system` can.
    /// Folder-shaped protocols where `role` already fully determines
    /// folder-ness leave it `false`. Defaults to `false`.
    pub system: bool,
    /// Per-folder access rights, when the protocol surfaces them (JMAP
    /// `Mailbox.myRights`, IMAP MYRIGHTS, Graph public-folder EWS
    /// `EffectiveRights`). Drives shared-mailbox submit-gating and the
    /// read-only-share distinction in the consumer. See
    /// [`ContainerRights`]. Defaults to `None`.
    pub rights: Option<ContainerRights>,
    /// Subscription state, when the protocol surfaces it. Only JMAP
    /// populates this (from `Mailbox.isSubscribed`); other providers
    /// leave it `None`. `Some(true)`/`Some(false)` is the value the
    /// protocol reported. Defaults to `None`.
    pub is_subscribed: Option<bool>,
    /// Which namespace this container lives in. Defaults to
    /// [`ContainerNamespace::Personal`], so a provider that only
    /// enumerates the principal's own mailbox never has to say anything.
    /// See [`ContainerNamespace`].
    pub namespace: ContainerNamespace,
    /// The owning mailbox / principal for a `Shared` container: the
    /// Graph `/users/{id}` routing key, the foreign JMAP `accountId`, or
    /// the IMAP other-user / shared-namespace owner. `None` for
    /// `Personal` and `Public` containers (a public folder has no owning
    /// principal).
    pub owner: Option<PrincipalId>,
    /// The container's native id inside the OWNER's own namespace, never
    /// the foreign-encoded form `native_id` carries. Graph: the bare
    /// mail-folder id. JMAP: the bare mailbox id. IMAP: the full mailbox
    /// path (IMAP has no separate per-owner id space - the path already
    /// is the native id). `None` when the container is not owned by
    /// another principal.
    ///
    /// The pairing matters: `native_id` is the string the engine's cursor
    /// scopes and membership index key on (and must be globally unique
    /// across owners), while `owner_local_id` is what goes back onto the
    /// wire in a request scoped to the owner's mailbox.
    pub owner_local_id: Option<String>,
    /// Best-effort owner email for a shared container. This is metadata only:
    /// a failure to resolve it must never make container discovery fail.
    pub owner_email: Option<String>,
    /// What kind of items the container holds, when the protocol types
    /// its folders. Only populated for Graph public folders (from the EWS
    /// `FolderClass`). See [`ContainerContentClass`].
    pub content_class: Option<ContainerContentClass>,
}

impl Container {
    /// Wire metadata, including the native id.
    pub fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Native id, always read from the wire metadata.
    pub fn native_id(&self) -> &str {
        &self.provenance.native
    }

    /// Replace the wire metadata and its native id together.
    #[must_use]
    pub fn with_provenance(mut self, provenance: Provenance) -> Self {
        self.provenance = provenance;
        self
    }

    /// Construct a container with the system defaults
    /// (`style = None`, `system = false`). Additive fields land here
    /// with sensible defaults so call sites do not break when the
    /// shape grows; recolor / system-tagging paths layer on top via
    /// [`Container::with_style`] and [`Container::with_system`].
    #[must_use]
    pub fn new(
        id: ContainerId,
        kind: ContainerKind,
        role: Option<FolderRole>,
        provenance: Provenance,
        name: String,
        parent: Option<ContainerId>,
    ) -> Self {
        Self {
            id,
            kind,
            role,
            provenance,
            name,
            parent,
            style: None,
            system: false,
            rights: None,
            is_subscribed: None,
            namespace: ContainerNamespace::Personal,
            owner: None,
            owner_local_id: None,
            owner_email: None,
            content_class: None,
        }
    }

    /// Set the display color.
    #[must_use]
    pub fn with_style(mut self, style: Option<ContainerStyle>) -> Self {
        self.style = style;
        self
    }

    /// Set the native-system flag.
    #[must_use]
    pub fn with_system(mut self, system: bool) -> Self {
        self.system = system;
        self
    }

    /// Set the per-folder access rights.
    #[must_use]
    pub fn with_rights(mut self, rights: Option<ContainerRights>) -> Self {
        self.rights = rights;
        self
    }

    /// Set the subscription state. Only the JMAP Account impl layers
    /// this on (from `Mailbox.isSubscribed`); other providers leave it
    /// `None`.
    #[must_use]
    pub fn with_subscription(mut self, is_subscribed: Option<bool>) -> Self {
        self.is_subscribed = is_subscribed;
        self
    }

    /// Set the namespace (personal / shared / public).
    #[must_use]
    pub fn with_namespace(mut self, namespace: ContainerNamespace) -> Self {
        self.namespace = namespace;
        self
    }

    /// Set the owning mailbox / principal for a shared container.
    #[must_use]
    pub fn with_owner(mut self, owner: Option<PrincipalId>) -> Self {
        self.owner = owner;
        self
    }

    /// Set the container's native id inside the owner's own namespace.
    /// Must be the bare (never foreign-encoded) form - see
    /// [`Container::owner_local_id`].
    #[must_use]
    pub fn with_owner_local_id(mut self, owner_local_id: Option<String>) -> Self {
        self.owner_local_id = owner_local_id;
        self
    }

    /// Set the best-effort email address of the owning shared mailbox.
    #[must_use]
    pub fn with_owner_email(mut self, owner_email: Option<String>) -> Self {
        self.owner_email = owner_email;
        self
    }

    /// Set the item class the container holds.
    #[must_use]
    pub fn with_content_class(mut self, content_class: Option<ContainerContentClass>) -> Self {
        self.content_class = content_class;
        self
    }
}

/// Result envelope of `Account::containers_list`.
///
/// `containers` is every container the enumeration materialized.
/// `skipped_scopes` names the namespaces a multi-namespace enumeration
/// skipped instead of listing - a foreign (shared/delegate) JMAP
/// account whose `Mailbox/get` failed, a Graph shared mailbox that
/// answered its folder walk with an error. A skip is advisory: the
/// listed containers remain valid, but the absence of a skipped
/// namespace's containers is not evidence they were deleted. Each entry
/// carries the classified `AccountError` that caused the skip, so the
/// consumer can distinguish a transient outage (retry / reopen heals
/// it) from a revoked grant.
///
/// Like `Page`, deliberately not `#[non_exhaustive]`: protocol Account
/// impls construct it directly, and a future lane must break every
/// constructor so each one answers the new question instead of
/// silently defaulting it.
#[derive(Debug, Clone)]
pub struct ContainerList {
    /// Containers the enumeration materialized.
    pub containers: Vec<Container>,
    /// Namespaces skipped instead of listed, with their classified
    /// failures. Empty for single-namespace providers and for
    /// enumerations where every namespace answered.
    pub skipped_scopes: Vec<SkippedScope>,
}

impl ContainerList {
    /// A fully-enumerated list: every namespace answered.
    #[must_use]
    pub fn complete(containers: Vec<Container>) -> Self {
        Self {
            containers,
            skipped_scopes: Vec::new(),
        }
    }
}

/// Label shape parallel to `Container` for protocols that draw a
/// hard distinction between containers (where a message lives) and
/// labels (a flag-like marker on a message).
///
/// Gmail user labels can be rendered either way - they walk like a
/// flat container but spawn like a flag. `Label` is the side that
/// `apply_label` / `remove_label` conveniences dispatch through; the
/// same id may also appear in `containers_list` when the consumer
/// wants to render labels as containers.
///
/// Not `#[non_exhaustive]` so protocol Account impls can construct
/// `Label` values directly when populating their containers list.
#[derive(Debug, Clone)]
pub struct Label {
    /// Engine-facing identifier.
    pub id: ContainerId,
    /// Wire-level provenance. The convenience layer inspects this
    /// to dispatch into the right primitive.
    pub provenance: Provenance,
    /// Display name.
    pub name: String,
    /// Role, when the label plays a canonical one (Gmail STARRED,
    /// UNREAD, INBOX, etc.).
    pub role: Option<FolderRole>,
    /// Display color, when the protocol surfaces one. Only Gmail
    /// populates this (from the label's `color`). See
    /// [`ContainerStyle`].
    pub style: Option<ContainerStyle>,
    /// `true` when the protocol marks this label as a native system
    /// label. Mirrors [`Container::system`]; defaults to `false`.
    pub system: bool,
}

impl Label {
    /// Construct a label with the system defaults (`style = None`,
    /// `system = false`). Additive fields default here so call sites
    /// survive shape growth; recolor / system-tagging layer on via
    /// [`Label::with_style`] and [`Label::with_system`].
    #[must_use]
    pub fn new(
        id: ContainerId,
        provenance: Provenance,
        name: String,
        role: Option<FolderRole>,
    ) -> Self {
        Self {
            id,
            provenance,
            name,
            role,
            style: None,
            system: false,
        }
    }

    /// Set the display color.
    #[must_use]
    pub fn with_style(mut self, style: Option<ContainerStyle>) -> Self {
        self.style = style;
        self
    }

    /// Set the native-system flag.
    #[must_use]
    pub fn with_system(mut self, system: bool) -> Self {
        self.system = system;
        self
    }
}

/// Mutation target shape used by every mail-mutation primitive.
///
/// JMAP, Gmail, and Graph natively address either a thread or a
/// single message; IMAP only addresses single messages. Account
/// impls that only support per-message mutation fan a `Thread`
/// target out internally rather than forcing the caller to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MutationTarget {
    /// Apply the mutation to every message in the thread.
    Thread(crate::ids::ThreadId),
    /// Apply the mutation to a single message.
    Message(crate::ids::ObjectId),
}

impl MutationTarget {
    /// Thread-shaped target.
    #[must_use]
    pub fn thread(id: crate::ids::ThreadId) -> Self {
        Self::Thread(id)
    }

    /// Message-shaped target.
    #[must_use]
    pub fn message(id: crate::ids::ObjectId) -> Self {
        Self::Message(id)
    }

    /// True iff this target is thread-shaped.
    #[must_use]
    pub fn is_thread(&self) -> bool {
        matches!(self, Self::Thread(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provenance() -> Provenance {
        Provenance {
            provider: ProtocolKind::Gmail,
            kind: ContainerKind::Label,
            native: "Label_42".to_string(),
        }
    }

    #[test]
    fn container_new_defaults_style_none_and_system_false() {
        let c = Container::new(
            ContainerId("Label_42".to_string()),
            ContainerKind::Label,
            None,
            provenance(),
            "Work".to_string(),
            None,
        );
        assert!(c.style.is_none());
        assert!(!c.system);
        assert!(c.rights.is_none());
        assert!(c.is_subscribed.is_none());
        // A container is personal-namespace, unowned, and untyped unless
        // a provider says otherwise.
        assert_eq!(c.namespace, ContainerNamespace::Personal);
        assert_eq!(c.namespace, ContainerNamespace::default());
        assert!(c.owner.is_none());
        assert!(c.owner_local_id.is_none());
        assert!(c.content_class.is_none());
        // native_id mirrors provenance.native.
        assert_eq!(c.native_id(), "Label_42");
    }

    #[test]
    fn container_builders_layer_namespace_owner_and_content_class() {
        let c = Container::new(
            ContainerId("shared\u{1f}AAMk".to_string()),
            ContainerKind::Folder,
            None,
            Provenance {
                provider: ProtocolKind::Graph,
                kind: ContainerKind::Folder,
                native: "shared\u{1f}AAMk".to_string(),
            },
            "Team Inbox".to_string(),
            None,
        )
        .with_namespace(ContainerNamespace::Shared)
        .with_owner(Some(PrincipalId("shared@contoso.com".to_string())))
        .with_owner_local_id(Some("AAMk".to_string()))
        .with_content_class(Some(ContainerContentClass::Mail));
        assert_eq!(c.namespace, ContainerNamespace::Shared);
        assert_eq!(c.owner, Some(PrincipalId("shared@contoso.com".to_string())));
        // `owner_local_id` is the bare owner-namespace id, never the
        // foreign-encoded `native_id`.
        assert_eq!(c.owner_local_id.as_deref(), Some("AAMk"));
        assert_ne!(c.owner_local_id.as_deref(), Some(c.native_id()));
        assert_eq!(c.content_class, Some(ContainerContentClass::Mail));
    }

    #[test]
    fn container_builders_layer_rights_and_subscription() {
        let rights = ContainerRights {
            may_submit: Some(true),
            may_read_items: Some(true),
            ..ContainerRights::default()
        };
        let c = Container::new(
            ContainerId("Label_42".to_string()),
            ContainerKind::Folder,
            None,
            provenance(),
            "Shared".to_string(),
            None,
        )
        .with_rights(Some(rights.clone()))
        .with_subscription(Some(true));
        assert_eq!(c.rights.as_ref(), Some(&rights));
        assert_eq!(c.rights.unwrap().may_submit, Some(true));
        assert_eq!(c.is_subscribed, Some(true));
    }

    #[test]
    fn container_builders_layer_style_and_system() {
        let style = ContainerStyle::new("#fb4c2f", "#ffffff");
        let c = Container::new(
            ContainerId("Label_42".to_string()),
            ContainerKind::Label,
            None,
            provenance(),
            "Work".to_string(),
            None,
        )
        .with_style(Some(style.clone()))
        .with_system(true);
        assert_eq!(c.style.as_ref(), Some(&style));
        assert_eq!(c.style.unwrap().color_bg, "#fb4c2f");
        assert!(c.system);
    }

    #[test]
    fn label_new_defaults_then_builders_layer() {
        let l = Label::new(
            ContainerId("Label_42".to_string()),
            provenance(),
            "Work".to_string(),
            None,
        );
        assert!(l.style.is_none());
        assert!(!l.system);

        let style = ContainerStyle::new("#16a766", "#000000");
        let l = l.with_style(Some(style.clone())).with_system(true);
        assert_eq!(l.style, Some(style));
        assert!(l.system);
    }
}
