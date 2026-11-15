//! Shared cursor coverage and membership identity.
//!
//! Owners annotate objects; they never name containers. An Outlook user opening
//! a shared mailbox must not reach a personal folder with the same id.
//! Untyped folders and queries reveal no object type, so their type-wide
//! coverage is conservative. Provider-qualified ids remain opaque here.

use super::{CursorScope, MembershipScope, ObjectType};
use crate::FolderId;

impl MembershipScope {
    fn container_id(&self) -> Option<&str> {
        match self {
            Self::Folder(id) => Some(&id.0),
            Self::Mailbox(id) => Some(&id.0),
            Self::Owner(_) | Self::Label(_) | Self::Query(_) => None,
        }
    }

    /// Explicit Folder/Mailbox bridge for container-shaped cursor positions.
    #[must_use]
    pub fn folder_id(&self) -> Option<FolderId> {
        match self {
            Self::Folder(id) => Some(id.clone()),
            Self::Mailbox(id) => Some(FolderId::from(id)),
            Self::Owner(_) | Self::Label(_) | Self::Query(_) => None,
        }
    }

    /// Canonical key for membership indexes. Native Mailbox and Folder are
    /// alternate container spellings; all other identities retain their tag.
    #[must_use]
    pub fn canonical(&self) -> Self {
        match self {
            Self::Mailbox(id) => Self::Folder(FolderId::from(id)),
            Self::Folder(_) | Self::Label(_) | Self::Query(_) | Self::Owner(_) => self.clone(),
        }
    }

    /// Container membership identity across the Folder/Mailbox bridge. Owner
    /// annotations never prove container membership, including against another
    /// Owner tag. Compare annotations themselves with ordinary enum equality.
    #[must_use]
    pub fn same_container(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Owner(_), _) | (_, Self::Owner(_)) => false,
            _ => match (self.container_id(), other.container_id()) {
                (Some(a), Some(b)) => a == b,
                _ => self == other,
            },
        }
    }

    fn supports_type(&self, ty: ObjectType) -> bool {
        match self {
            Self::Mailbox(_) => matches!(
                ty,
                ObjectType::Email | ObjectType::Mailbox | ObjectType::Thread
            ),
            Self::Label(_) => matches!(ty, ObjectType::Email | ObjectType::Thread),
            Self::Folder(_) | Self::Query(_) => true,
            Self::Owner(_) => false,
        }
    }
}

/// Meaning of an inventory tombstone within a cursor scope.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RemovalProjection {
    Destroy,
    Membership(MembershipScope),
}

impl CursorScope {
    /// Whether this cursor can cover the membership. Owners have account-wide
    /// coverage only; a type cursor may belong to the primary account while
    /// an owner names a foreign account, so type coverage cannot be inferred.
    #[must_use]
    pub fn covers(&self, membership: &MembershipScope) -> bool {
        match self {
            Self::Account => true,
            Self::Type(ty) => membership.supports_type(*ty),
            Self::Query(id) => matches!(membership, MembershipScope::Query(other) if id == other),
            Self::Folder(folder) => membership.container_id() == Some(folder.0.as_str()),
            Self::FolderType { folder, ty } => {
                membership.supports_type(*ty)
                    && membership.container_id() == Some(folder.0.as_str())
            }
        }
    }

    /// Folder removals use the canonical Folder spelling. Compare against
    /// native mailbox memberships with `same_container`.
    #[must_use]
    pub fn removal_projection(&self) -> RemovalProjection {
        match self {
            Self::Account | Self::Type(_) => RemovalProjection::Destroy,
            Self::Query(query) => {
                RemovalProjection::Membership(MembershipScope::Query(query.clone()))
            }
            Self::Folder(folder) | Self::FolderType { folder, .. } => {
                RemovalProjection::Membership(MembershipScope::Folder(folder.clone()))
            }
        }
    }
}

/// Advertised cursor shapes independent of the live cursor set. Creation and
/// deletion retain separate shape histories in the engine.
#[derive(Debug, Default, Clone)]
pub struct ScopeShapes {
    folder: bool,
    folder_types: Vec<ObjectType>,
    query: bool,
    wide: bool,
}

impl ScopeShapes {
    pub fn record(&mut self, scope: &CursorScope) {
        match scope {
            CursorScope::Folder(_) => self.folder = true,
            CursorScope::FolderType { ty, .. } => {
                if !self.folder_types.contains(ty) {
                    self.folder_types.push(*ty);
                }
            }
            CursorScope::Query(_) => self.query = true,
            CursorScope::Account | CursorScope::Type(_) => self.wide = true,
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.folder |= other.folder;
        self.query |= other.query;
        self.wide |= other.wide;
        for ty in &other.folder_types {
            if !self.folder_types.contains(ty) {
                self.folder_types.push(*ty);
            }
        }
    }

    #[must_use]
    pub fn is_wide(&self) -> bool {
        self.wide
    }

    /// Per-membership scopes admitted by these shapes, each satisfying `covers`.
    /// Wide shapes do not suppress deletion reach; creation owns that policy.
    #[must_use]
    pub fn scopes_for(&self, membership: &MembershipScope) -> Vec<CursorScope> {
        let mut scopes = Vec::new();
        if let Some(folder) = membership.folder_id() {
            if self.folder {
                scopes.push(CursorScope::Folder(folder.clone()));
            }
            for ty in &self.folder_types {
                let scope = CursorScope::FolderType {
                    folder: folder.clone(),
                    ty: *ty,
                };
                if scope.covers(membership) {
                    scopes.push(scope);
                }
            }
        }
        if let MembershipScope::Query(query) = membership
            && self.query
        {
            scopes.push(CursorScope::Query(query.clone()));
        }
        scopes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LabelId, MailboxId, PrincipalId, QueryId};

    #[test]
    fn topology_distinguishes_owner_and_container_with_the_same_id() {
        let owner = MembershipScope::Owner(PrincipalId("a".into()));
        let mailbox = MembershipScope::Mailbox(MailboxId("a".into()));
        let folder = MembershipScope::Folder(FolderId("a".into()));
        assert!(!owner.same_container(&mailbox));
        assert!(!owner.same_container(&folder));
        assert!(!owner.same_container(&owner));
        assert!(mailbox.same_container(&folder));
        assert!(folder.same_container(&mailbox));
        assert!(!CursorScope::Folder(FolderId("a".into())).covers(&owner));
        assert!(CursorScope::Account.covers(&owner));
        assert!(!CursorScope::Type(ObjectType::Email).covers(&owner));
        let mut shapes = ScopeShapes::default();
        shapes.record(&CursorScope::Folder(FolderId("a".into())));
        assert!(shapes.scopes_for(&owner).is_empty());
    }

    #[test]
    fn topology_scope_generation_is_the_inverse_of_narrow_coverage() {
        let scopes = [
            CursorScope::Folder(FolderId("a".into())),
            CursorScope::FolderType {
                folder: FolderId("a".into()),
                ty: ObjectType::Email,
            },
            CursorScope::FolderType {
                folder: FolderId("a".into()),
                ty: ObjectType::Contact,
            },
            CursorScope::Query(QueryId("a".into())),
        ];
        let mut shapes = ScopeShapes::default();
        for scope in &scopes {
            shapes.record(scope);
        }
        for membership in [
            MembershipScope::Folder(FolderId("a".into())),
            MembershipScope::Mailbox(MailboxId("a".into())),
            MembershipScope::Label(LabelId("a".into())),
            MembershipScope::Query(QueryId("a".into())),
            MembershipScope::Owner(PrincipalId("a".into())),
        ] {
            let generated = shapes.scopes_for(&membership);
            for scope in &scopes {
                assert_eq!(
                    generated.contains(scope),
                    scope.covers(&membership),
                    "{scope:?}, {membership:?}"
                );
            }
        }
        assert!(
            !CursorScope::Type(ObjectType::Contact)
                .covers(&MembershipScope::Label(LabelId("a".into())))
        );
        assert!(
            !CursorScope::Type(ObjectType::CalendarEvent)
                .covers(&MembershipScope::Mailbox(MailboxId("a".into())))
        );
        assert!(
            !CursorScope::Folder(FolderId("b".into()))
                .covers(&MembershipScope::Mailbox(MailboxId("a".into())))
        );
    }

    #[test]
    fn topology_folder_removal_projects_to_the_same_container_identity() {
        let mailbox = MembershipScope::Mailbox(MailboxId("a".into()));
        let scope = CursorScope::FolderType {
            folder: FolderId("a".into()),
            ty: ObjectType::Email,
        };
        let RemovalProjection::Membership(removal) = scope.removal_projection() else {
            panic!("membership removal");
        };
        assert!(removal.same_container(&mailbox));
        assert_eq!(
            CursorScope::Account.removal_projection(),
            RemovalProjection::Destroy
        );
    }
}
