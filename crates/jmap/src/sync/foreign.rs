//! Foreign (shared/delegate) JMAP account codec.
//!
//! A non-personal JMAP account surfaces as ONE account-level cursor scope,
//! `CursorScope::Folder(encode_foreign_account(account_id))` - the same
//! `accountId\u{1f}` namespace with an empty mailbox part, because JMAP
//! `Email/changes` state is per `(accountId, type)` and cannot be
//! filtered by mailbox. Its individual mailboxes keep the two-part
//! `encode_foreign(account_id, mailbox_id)` form, which is the CONTAINER
//! and MEMBERSHIP namespace (`containers_list` native ids, qualified
//! `mailboxIds` on inventory and hydration), not a cursor scope. The
//! owning JMAP `accountId` rides inside the `FolderId` string so the
//! scope is self-routing on a cold cursor resume. `Type(_)` scopes
//! cannot carry a foreign account (`Type(Email)` is the same value for
//! every account, so identical scopes for different accounts would
//! collide in the engine's cursor / membership index); the `Folder`
//! shape is the variant-free resolution.
//!
//! Owner meaning stays provider-owned; the legacy consumer encoding is kept
//! here so every JMAP door uses the same parsing and wire conversion.

use bifrost_types::{FolderId, MembershipScope, PrincipalId};

/// Reserved separator. A `FolderId` of the form
/// `"<accountId>\u{1f}<mailboxId>"` is a foreign-account mailbox. `\u{1f}`
/// (US, unit separator) cannot appear in a JMAP id (RFC 8620 §1.2 ids are
/// printable-ASCII-ish and never contain control characters), so it is an
/// unambiguous delimiter.
const FOREIGN_SEP: char = '\u{1f}';

/// Declared ownership is independent of the current session's reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Owner<'a> {
    Primary,
    Shared(&'a str),
}

impl<'a> Owner<'a> {
    pub(crate) fn shared(self) -> Option<&'a str> {
        match self {
            Self::Primary => None,
            Self::Shared(id) => Some(id),
        }
    }
}

/// A borrowed consumer identity. Parsing retains the historical first separator
/// grammar, including empty owners and suffix separators, for persisted keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JmapRef<'a> {
    pub(crate) owner: Owner<'a>,
    pub(crate) native: &'a str,
    raw: &'a str,
}

impl<'a> JmapRef<'a> {
    pub(crate) fn parse(raw: &'a str) -> Self {
        match raw.split_once(FOREIGN_SEP) {
            Some((owner, native)) => Self {
                owner: Owner::Shared(owner),
                native,
                raw,
            },
            None => Self {
                owner: Owner::Primary,
                native: raw,
                raw,
            },
        }
    }

    /// Strip only on the matching shared route. A qualified primary id or a
    /// departed share stays literal: otherwise a colliding primary object could
    /// be read or mutated under somebody else's identity.
    pub(crate) fn wire(self, route: Owner<'_>) -> &'a str {
        match (self.owner, route) {
            (Owner::Shared(owner), Owner::Shared(selected)) if owner == selected => self.native,
            _ => self.raw,
        }
    }

    pub(crate) fn encode(owner: Owner<'_>, native: &str) -> String {
        match owner {
            Owner::Primary => native.to_owned(),
            Owner::Shared(owner) => format!("{owner}{FOREIGN_SEP}{native}"),
        }
    }

    pub(crate) fn same_owner(self, other: Self) -> bool {
        self.owner == other.owner
    }
}

/// A foreign-account mailbox: the owning JMAP `accountId` plus the native
/// JMAP mailbox id within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignMailbox {
    /// The foreign JMAP `accountId`.
    pub(crate) account_id: String,
    /// The native JMAP mailbox id. Empty for the account-level cursor
    /// scope (`encode_foreign_account`).
    pub(crate) mailbox_id: String,
}

impl ForeignMailbox {
    /// True for the account-level cursor scope shape (empty mailbox
    /// part), which names the whole foreign account rather than one of
    /// its mailboxes.
    #[cfg(test)]
    pub(crate) fn is_account_scope(&self) -> bool {
        self.mailbox_id.is_empty()
    }
}

/// Encode a `(accountId, mailboxId)` pair into the namespaced `FolderId`
/// used for foreign containers and memberships.
pub(crate) fn encode_foreign(account_id: &str, mailbox_id: &str) -> FolderId {
    FolderId(JmapRef::encode(Owner::Shared(account_id), mailbox_id))
}

/// Encode a foreign account's single ACCOUNT-LEVEL cursor scope id: the
/// two-part namespace with an empty mailbox part (`"acct\u{1f}"`).
///
/// The empty part is unambiguous because an RFC 8620 `Id` is 1-255
/// characters, so no real mailbox can ever produce it. Routing helpers
/// (`parse_foreign`, `mail_for_scope`, `owner_of_scope`) see the same
/// `account_id` either way; `is_account_scope` distinguishes the two
/// shapes where the mailbox part matters (the inventory filter).
pub(crate) fn encode_foreign_account(account_id: &str) -> FolderId {
    encode_foreign(account_id, "")
}

/// Parse a foreign `FolderId` back into its `(accountId, mailboxId)`
/// pair. Returns `None` for a plain id with no separator (which never
/// occurs for JMAP, whose primary scopes are `Type(_)`, but guards the
/// boundary regardless).
pub(crate) fn parse_foreign(folder: &FolderId) -> Option<ForeignMailbox> {
    let parsed = JmapRef::parse(&folder.0);
    parsed.owner.shared().map(|account_id| ForeignMailbox {
        account_id: account_id.to_owned(),
        mailbox_id: parsed.native.to_owned(),
    })
}

/// The `MembershipScope` owner tag for a foreign mailbox: the owning JMAP
/// `accountId` as a `PrincipalId`. Emitted on discovery so the consumer
/// maps the scope to its shared-account owner.
pub(crate) fn owner_tag(account_id: &str) -> MembershipScope {
    MembershipScope::Owner(PrincipalId(account_id.to_string()))
}

/// Encode a foreign account's native object id (an `Email` id, a `blobId`,
/// a `threadId`) into the same `accountId\u{1f}native` namespace the folder
/// codec uses.
///
/// This is what makes hydration and blob reads route to the FOREIGN
/// account. `Email/get` and blob download are both accountId-scoped calls,
/// but `get_stream` / `open_blob` receive only an id - no scope - so
/// without the owning account riding inside the id string they would run
/// against the primary account and either 404 or, worse, resolve a primary
/// object that happens to share the id. The mint sites are the foreign
/// inventory / changes projections; the request sites decode and select the
/// foreign handle. A primary id is never encoded, so one logical object has
/// exactly one wire form.
///
/// A `threadId` rides here for the same reason and with a sharper
/// consequence: `Thread/get` is accountId-scoped and the thread id is its
/// only operand, so a bare foreign thread id asserts PRIMARY ownership.
/// On an id collision the primary `Thread/get` resolves an unrelated
/// thread and every thread-keyed mutation lands on ITS messages -
/// `delete_thread` destroys them.
pub(crate) fn encode_object(account_id: &str, native: &str) -> String {
    JmapRef::encode(Owner::Shared(account_id), native)
}

/// A parsed object id: `Some((accountId, native))` for a foreign-account
/// object, `None` for a primary-account object (a bare native id).
pub(crate) fn parse_object(id: &str) -> Option<(&str, &str)> {
    match JmapRef::parse(id) {
        JmapRef {
            owner: Owner::Shared(owner),
            native,
            ..
        } => Some((owner, native)),
        _ => None,
    }
}

/// The native (owner-namespace) form of an object id, whether or not it was
/// foreign-encoded.
pub(crate) fn native_object(id: &str) -> &str {
    parse_object(id).map_or(id, |(_, native)| native)
}

/// The JMAP `accountId` a consumer-supplied id belongs to, or `None` when the
/// id is bare and therefore names an object in the primary account. Ids are
/// only ever encoded for foreign accounts, so `None` and `Some(_)` are
/// exactly "primary" and "that share".
///
/// This is the routing question every mutation has to answer for BOTH of its
/// operands. JMAP ids are account-scoped and `Email/set` names exactly one
/// `accountId`, so a message and the mailbox it is being filed into must
/// agree on their owner or the request is not expressible - and, worse than
/// inexpressible, a bare primary mailbox id sent against a foreign account
/// can *resolve* there if that account happens to hold a mailbox under the
/// same id, silently filing the message into the wrong container.
///
/// Deliberately independent of whether the named account is still registered
/// in this session: an id for a departed share still declares its owner, and
/// a caller who names two different owners has asked for something no single
/// `Email/set` can do regardless of reachability.
pub(crate) fn owner_of(id: &str) -> Option<&str> {
    parse_object(id).map(|(owner, _)| owner)
}

/// Render an owner for diagnostics. The primary account has no qualifier in
/// the id namespace, so it needs a name of its own in error text.
pub(crate) fn owner_label(owner: Option<&str>) -> &str {
    owner.unwrap_or("<primary>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_round_trips_through_folder_id() {
        let id = encode_foreign("acct-99", "mbx-12");
        let parsed = parse_foreign(&id).expect("foreign");
        assert_eq!(parsed.account_id, "acct-99");
        assert_eq!(parsed.mailbox_id, "mbx-12");
    }

    #[test]
    fn plain_id_is_not_foreign() {
        assert!(parse_foreign(&FolderId("mbx-12".to_string())).is_none());
    }

    #[test]
    fn account_scope_round_trips_and_is_distinguishable() {
        let id = encode_foreign_account("acct-99");
        let parsed = parse_foreign(&id).expect("account scope parses as foreign");
        assert_eq!(parsed.account_id, "acct-99");
        assert!(parsed.is_account_scope());

        // A real mailbox scope is never mistaken for the account scope:
        // RFC 8620 ids are 1-255 chars, so the mailbox part is non-empty.
        let mailbox = parse_foreign(&encode_foreign("acct-99", "mbx-1")).expect("foreign");
        assert!(!mailbox.is_account_scope());
    }

    #[test]
    fn owner_tag_is_owner_membership() {
        assert_eq!(
            owner_tag("acct-99"),
            MembershipScope::Owner(PrincipalId("acct-99".to_string()))
        );
    }

    #[test]
    fn object_id_round_trips_and_leaves_primary_bare() {
        let encoded = encode_object("acct-9", "M1234");
        assert_eq!(encoded, format!("acct-9{FOREIGN_SEP}M1234"));
        assert_eq!(parse_object(&encoded), Some(("acct-9", "M1234")));
        assert_eq!(native_object(&encoded), "M1234");

        // A primary id has no separator: it parses as primary and its
        // native form is itself.
        assert_eq!(parse_object("M1234"), None);
        assert_eq!(native_object("M1234"), "M1234");
    }

    #[test]
    fn object_id_encoding_is_byte_stable() {
        // Re-encoding a decoded native id under the same account yields the
        // identical bytes: one wire form per logical object.
        let once = encode_object("acct-9", "M1234");
        let twice = encode_object("acct-9", native_object(&once));
        assert_eq!(once, twice);
    }

    #[test]
    fn splits_on_first_separator_only() {
        let id = FolderId(format!("acct{FOREIGN_SEP}mbx{FOREIGN_SEP}weird"));
        let parsed = parse_foreign(&id).expect("foreign");
        assert_eq!(parsed.account_id, "acct");
        assert_eq!(parsed.mailbox_id, format!("mbx{FOREIGN_SEP}weird"));
    }
}
