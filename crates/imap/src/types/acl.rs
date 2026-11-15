//! RFC 4314 typed ACL rights.
//!
//! The wire form of an ACL rights set is a flat string of one ASCII letter
//! per right (`AclEntry.rights` / `Response::MyRights.rights`). This module
//! parses that string into an order-insensitive [`MailboxRights`] set and
//! exposes the account-layer gating predicates A5c needs at shared-folder
//! discovery time. It is an advisory pre-flight hint, not an enforcement
//! layer: a folder with no read right is skipped at discovery, but the
//! server's `NO [ACL]` response stays authoritative for individual
//! mutations.

use std::collections::BTreeSet;

/// A single RFC 4314 right. The wire alphabet is one ASCII letter per
/// right (RFC 4314 Section 2.1 + the obsolete `c`/`d` virtual rights of
/// Section 2.1.1, retained for pre-4314 servers).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum AclRight {
    Lookup,         // l - mailbox is visible
    Read,           // r - SELECT, FETCH
    Seen,           // s - keep \Seen across sessions
    Write,          // w - set flags other than \Seen / \Deleted
    Insert,         // i - APPEND, COPY into
    Post,           // p - send mail to submission address
    CreateMailbox,  // k - CREATE child / RENAME into (RFC 4314)
    DeleteMailbox,  // x - DELETE / RENAME the mailbox (RFC 4314)
    DeleteMessages, // t - set \Deleted (RFC 4314)
    Expunge,        // e - EXPUNGE (RFC 4314)
    Administer,     // a - SETACL / DELETEACL / GETACL
    CreateLegacy,   // c - obsolete create (pre-4314 'c')
    DeleteLegacy,   // d - obsolete delete (pre-4314 'd')
    Unknown(char),  // forward-compat: an extension right letter
}

impl AclRight {
    /// Map a wire letter to its right. Unknown letters become
    /// [`AclRight::Unknown`] so a server extension right is preserved
    /// rather than silently dropped.
    fn from_letter(c: char) -> Self {
        match c {
            'l' => Self::Lookup,
            'r' => Self::Read,
            's' => Self::Seen,
            'w' => Self::Write,
            'i' => Self::Insert,
            'p' => Self::Post,
            'k' => Self::CreateMailbox,
            'x' => Self::DeleteMailbox,
            't' => Self::DeleteMessages,
            'e' => Self::Expunge,
            'a' => Self::Administer,
            'c' => Self::CreateLegacy,
            'd' => Self::DeleteLegacy,
            other => Self::Unknown(other),
        }
    }
}

/// Parsed RFC 4314 rights string. Order-insensitive set; the wire form
/// is order-insensitive too (RFC 4314 Section 3.6).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MailboxRights {
    rights: BTreeSet<AclRight>,
}

impl MailboxRights {
    /// Parse a wire rights string (`"lrswipkxtea"`). Unknown letters are
    /// preserved as [`AclRight::Unknown`] rather than dropped - a server
    /// extension right must not silently read as "no rights". Empty string
    /// is the empty set (valid: explicit no-rights).
    // RFC 4314 Section 2.1.1 requires clients to ignore virtual c/d rights
    // in MYRIGHTS, ACL and LISTRIGHTS responses. They do NOT imply their
    // members: a server returns d even when only one delete member is granted.
    // Preserve the wire letters for inspection, but never expand them here.
    // SETACL input expansion is server policy and cannot be inferred from a
    // response string; c may mean k or kx, and d may mean et or etx.
    // Do not replace every ACL string with this granted-set type: SETACL
    // carries +/- operations, LISTRIGHTS carries tied groups, and RIGHTS=
    // advertises an alphabet. Discovery parses MYRIGHTS before projecting it.
    pub(crate) fn parse(s: &str) -> Self {
        Self {
            rights: s.chars().map(AclRight::from_letter).collect(),
        }
    }

    /// `l` + `r` present: the user can SELECT and FETCH. This is the
    /// minimum to sync a folder at all - `l` is the visibility gate, `r`
    /// the access gate (RFC 4314 Section 4).
    pub(crate) fn can_read(&self) -> bool {
        self.contains(AclRight::Lookup) && self.contains(AclRight::Read)
    }

    /// `i` present: COPY/APPEND into this folder (mutation gate).
    pub(crate) fn can_insert(&self) -> bool {
        self.contains(AclRight::Insert)
    }

    /// `t` + `e`: the user can delete-and-expunge (destroy gate).
    pub(crate) fn can_delete(&self) -> bool {
        self.contains(AclRight::DeleteMessages) && self.contains(AclRight::Expunge)
    }

    /// Account authority from a MYRIGHTS response; virtual c/d never grant it.
    pub(crate) fn container_rights(&self) -> bifrost_types::ContainerRights {
        let mailbox_admin = self.contains(AclRight::DeleteMailbox);
        bifrost_types::ContainerRights {
            may_read_items: Some(self.can_read()),
            may_add_items: Some(self.can_insert()),
            // The account removes messages by marking and expunging. An
            // enterprise user expects removal to finish, not just set a flag.
            may_remove_items: Some(self.can_delete()),
            may_set_seen: Some(self.contains(AclRight::Seen)),
            may_set_keywords: Some(self.contains(AclRight::Write)),
            may_create_child: Some(self.contains(AclRight::CreateMailbox)),
            may_rename: Some(mailbox_admin),
            may_delete: Some(mailbox_admin),
            may_submit: Some(self.contains(AclRight::Post)),
            may_share: Some(self.contains(AclRight::Administer)),
        }
    }

    pub(crate) fn contains(&self, right: AclRight) -> bool {
        self.rights.contains(&right)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rights.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_response_rights_never_grant_member_rights() {
        let virtual_only = MailboxRights::parse("lrcd").container_rights();
        assert_eq!(virtual_only.may_create_child, Some(false));
        assert_eq!(virtual_only.may_delete, Some(false));
        assert_eq!(virtual_only.may_remove_items, Some(false));
        let mark_only = MailboxRights::parse("td").container_rights();
        assert_eq!(mark_only.may_remove_items, Some(false));
        assert_eq!(
            MailboxRights::parse("te")
                .container_rights()
                .may_remove_items,
            Some(true)
        );
        assert_eq!(
            MailboxRights::parse("kx").container_rights().may_delete,
            Some(true)
        );
    }

    #[test]
    fn parse_full_rights_set() {
        let rights = MailboxRights::parse("lrswipkxtea");
        assert!(!rights.is_empty());
        assert!(rights.contains(AclRight::Lookup));
        assert!(rights.contains(AclRight::Read));
        assert!(rights.contains(AclRight::Seen));
        assert!(rights.contains(AclRight::Write));
        assert!(rights.contains(AclRight::Insert));
        assert!(rights.contains(AclRight::Post));
        assert!(rights.contains(AclRight::CreateMailbox));
        assert!(rights.contains(AclRight::DeleteMailbox));
        assert!(rights.contains(AclRight::DeleteMessages));
        assert!(rights.contains(AclRight::Expunge));
        assert!(rights.contains(AclRight::Administer));
        assert!(rights.can_read());
        assert!(rights.can_insert());
        assert!(rights.can_delete());
    }

    #[test]
    fn parse_empty_string_is_empty_set() {
        let rights = MailboxRights::parse("");
        assert!(rights.is_empty());
        assert!(!rights.can_read());
        assert!(!rights.can_insert());
        assert!(!rights.can_delete());
    }

    #[test]
    fn parse_unknown_right_preserved() {
        let rights = MailboxRights::parse("lr9");
        assert!(rights.contains(AclRight::Unknown('9')));
        // An unknown extension letter must not poison the known rights.
        assert!(rights.can_read());
    }

    #[test]
    fn parse_is_order_insensitive() {
        assert_eq!(MailboxRights::parse("rl"), MailboxRights::parse("lr"));
    }

    #[test]
    fn read_requires_l_and_r() {
        // `r` alone (no `l`) is not readable: `l` is the visibility gate.
        let rights = MailboxRights::parse("r");
        assert!(!rights.can_read());
    }
}
