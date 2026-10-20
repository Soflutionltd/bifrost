//! Organization-directory (Global Address List) result type.
//!
//! The directory corpus is the org-wide, read-only address list - a
//! distinct corpus from the per-account address books `ContactCard`
//! models. It is surfaced by `Account::directory_search` and backed by
//! provider directory endpoints (Microsoft Graph `/users`, Google
//! People `listDirectoryPeople` / `searchDirectoryPeople`, CardDAV gateways).

use crate::cursor::ProtocolKind;

/// One organization-directory (Global Address List) entry.
///
/// The directory corpus is org-wide and read-only - distinct from the
/// per-account address books `ContactCard` models. A `DirectoryCard` carries
/// no engine-facing id, no etag, and no address-book membership, because no
/// contact/mutation primitive operates on it: it is a lookup result, full
/// stop. `email` is the stable key (providers skip rows without one).
///
/// Not `#[non_exhaustive]`: like `ContactCard` and `Page`, protocol Account
/// impls construct `DirectoryCard` values directly outside this crate, so a
/// non-exhaustive marker would make every provider mapping uncompilable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryCard {
    /// Primary email address. Always present (rows without one are dropped
    /// at the provider boundary, matching ratatoskr's GAL behavior).
    pub email: String,
    pub display_name: Option<String>,
    /// Additional email addresses beyond `email`, in provider order.
    pub additional_emails: Vec<String>,
    /// Phone numbers in provider order (business/home/mobile flattened;
    /// the directory does not reliably type them).
    pub phones: Vec<String>,
    pub company: Option<String>,
    pub title: Option<String>,
    pub department: Option<String>,
    /// Which protocol family produced this row.
    pub provider: ProtocolKind,
}

/// Provider-native identity of one directory group.
///
/// Unlike `DirectoryCard`, a directory group carries real identity: the
/// consumer names a group in order to expand its membership
/// (`Account::directory_group_expand`), so the id must round-trip. The
/// inner string is the provider's native group id (Microsoft Graph
/// `group.id`), opaque to consumers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DirectoryGroupId(pub String);

/// Classification of a mail-enabled directory group.
///
/// The vocabulary is provider-neutral; today only Microsoft Graph
/// produces these rows (`Unified` in `groupTypes` -> `Unified`, else
/// `securityEnabled` -> `MailEnabledSecurity`, else `DistributionList`).
/// Mail-disabled groups are dropped at the provider boundary and never
/// surface here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DirectoryGroupKind {
    /// A modern collaboration group (Microsoft 365 "Unified" group).
    Unified,
    /// A classic distribution list.
    DistributionList,
    /// A mail-enabled security group.
    MailEnabledSecurity,
}

/// One mail-enabled directory group the authenticated mailbox belongs to.
///
/// Surfaced by `Account::directory_groups_list`. Distinct from the
/// personal contact-group labels Google People flattens into
/// `AddressBook`s, and unrelated to the `ObjectType::ContactGroup`
/// cursor variant - this is the org-directory corpus, read-only, like
/// `DirectoryCard`.
///
/// Not `#[non_exhaustive]`: like `DirectoryCard`, protocol Account impls
/// construct these values directly outside this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryGroup {
    /// Provider-native group identity; feed to
    /// `Account::directory_group_expand`.
    pub id: DirectoryGroupId,
    pub display_name: String,
    /// The group's own SMTP address, when it has one.
    pub email: Option<String>,
    pub kind: DirectoryGroupKind,
    /// Which protocol family produced this row.
    pub provider: ProtocolKind,
}

/// One resolved member of a directory group.
///
/// Produced by `Account::directory_group_expand`, which expands
/// membership transitively and provider-side down to users - nested
/// groups never appear as members, so a member needs no identity for
/// follow-up expansion and none is carried. Deliberately minimal: the
/// expansion endpoints project only name and address, so this does NOT
/// reuse `DirectoryCard` (whose corpus fields - phones, company,
/// department - would always be empty here).
///
/// Not `#[non_exhaustive]`: constructed directly by protocol crates,
/// same as `DirectoryCard`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryGroupMember {
    /// Primary email address, lowercased. Always present (members
    /// without a resolvable address are dropped at the provider
    /// boundary, matching the legacy ratatoskr behavior).
    pub email: String,
    pub display_name: Option<String>,
}
