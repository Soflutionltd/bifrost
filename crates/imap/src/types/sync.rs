//! Higher-level mailbox sync request and result types.

use super::SelectedMailbox;

/// Result of selecting a mailbox for synchronization.
// protocol-specific: direct IMAP sync helpers expose SELECT extension usage.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSelectResult {
    /// Raw selected-mailbox data.
    pub mailbox: SelectedMailbox,
    /// Whether QRESYNC parameters were sent on this SELECT/EXAMINE, and if
    /// not, why.
    pub qresync: QresyncOutcome,
    /// Whether CONDSTORE was requested for this SELECT/EXAMINE.
    pub condstore_used: bool,
}

/// What became of QRESYNC on one SELECT/EXAMINE (RFC 7162 Section 3.2.5).
///
/// Decided when the command was built, so a reader never has to infer it
/// from state that may have moved since.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QresyncOutcome {
    /// QRESYNC parameters were sent.
    Used,
    /// The options carried no QRESYNC parameters: the caller's policy did not
    /// ask for them.
    NotRequested,
    /// The options asked for QRESYNC, but this connection does not have it
    /// enabled, so the SELECT fell back to CONDSTORE or plain. `ENABLE` is
    /// never issued here: once a mailbox has been selected in the session it
    /// is illegal (RFC 5161 Section 3.1), and connections are brought to
    /// QRESYNC when they are minted instead.
    NotEnabledOnConnection,
}
