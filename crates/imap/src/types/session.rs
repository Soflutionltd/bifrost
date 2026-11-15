/// IMAP session state (RFC 3501 Section 3 / RFC 9051 Section 3).
///
/// Tracks the current protocol state of the connection. State transitions
/// are managed automatically by `ImapConnection` methods.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SessionState {
    /// Not Authenticated  -  client must authenticate (RFC 3501 Section 3.1).
    NotAuthenticated,
    /// Authenticated  -  client may select a mailbox (RFC 3501 Section 3.2).
    Authenticated,
    /// Selected  -  a mailbox is open (RFC 3501 Section 3.3).
    Selected,
    /// Logout  -  connection is being closed (RFC 3501 Section 3.4).
    Logout,
}
