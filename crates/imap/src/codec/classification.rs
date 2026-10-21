//! RFC truth table for "can this untagged response appear as part of
//! the solicited result for this command?"
//!
//! Every match arm cites the RFC section that justifies it. This is
//! the single source of routing truth for the entire crate. The
//! dispatcher calls `classify` for every untagged response and routes
//! based on the returned `SolicitationRule`.
//!
//! See plan glossary entries for `classify`, `SolicitationRule`, and
//! `ClassificationContext`.

#[cfg(test)]
#[path = "classification_tests.rs"]
mod tests;

use crate::connection::NotifyFlags;
use crate::types::CommandKind;
use crate::types::response::UntaggedResponse;
use crate::types::validated::MailboxName;

/// What `classify` can say about a (command, response) pair.
#[derive(Debug)]
pub(crate) enum SolicitationRule {
    /// Response can only appear as the solicited reply to this command.
    OnlySolicited,
    /// Response can only appear unsolicited  -  it is not part of any
    /// command's solicited result.
    OnlyUnsolicited,
    /// The RFC does not give a way to disambiguate. The consumer must
    /// decide based on its own state (typically per-position notify
    /// flags). The dispatcher routes these to the consumer AND records
    /// them in a bucket the consumer's finalize inspects.
    Either,
    /// This pair cannot appear under any interpretation of the RFC.
    /// Per Postel's law the dispatcher buffers these as unsolicited
    /// events rather than erroring, tolerating non-conformant servers.
    Impossible,
}

/// Context needed by `classify` beyond the (command, response) pair.
pub(crate) struct ClassificationContext<'a> {
    /// Current NOTIFY registration flags (RFC 5465 Section5.1-5.8).
    pub notify: NotifyFlags,
    /// The mailbox argument of the current command, if applicable.
    /// E.g., for STATUS, this is the mailbox being queried.
    pub command_target: Option<&'a MailboxName>,
}

/// Compare two decoded mailbox names for command-response correlation.
///
/// RFC 3501 Section 5.1: `INBOX` is case-insensitive; all other mailbox
/// names are compared byte-for-byte. Both arguments must be decoded
/// (user-facing UTF-8), not wire-form.
///
pub(crate) fn mailbox_names_eq(a: &str, b: &str) -> bool {
    if a.eq_ignore_ascii_case("INBOX") && b.eq_ignore_ascii_case("INBOX") {
        return true;
    }
    a == b
}

/// Classify a (command, untagged-response) pair into a solicitation rule.
///
/// This is the single source of routing truth for the entire IMAP crate.
/// The dispatcher calls this for every untagged response received while
/// a command is in flight and routes the response to the consumer's
/// accumulator or the event sink based on the returned rule.
///
/// Rows cover RFC 3501 Section6.1-Section6.4 commands plus all currently supported
/// extensions (METADATA, ACL, QUOTA, ESEARCH, ENABLE, QRESYNC, NOTIFY,
/// LIST-EXTENDED/LIST-STATUS, NAMESPACE, ID, THREAD, SORT, COMPRESS).
pub(crate) fn classify(
    cmd: CommandKind,
    resp: &UntaggedResponse,
    ctx: &ClassificationContext,
) -> SolicitationRule {
    use CommandKind as CK;
    use SolicitationRule::{Either, Impossible, OnlySolicited, OnlyUnsolicited};
    use UntaggedResponse as UR;

    // Each arm is an independent RFC truth-table row with its own
    // citation. Arms that return the same variant are intentionally
    // separate for auditability.
    #[allow(clippy::match_same_arms, unreachable_patterns)]
    match (cmd, resp) {
        // ---- CAPABILITY (RFC 3501 Section6.1.1, Section7.2.1) ----
        // A CAPABILITY command solicits exactly one CAPABILITY response.
        (CK::Capability, UR::Capability(_)) => OnlySolicited,
        // RFC 3501 Section7.2.1: CAPABILITY can arrive unsolicited after
        // authentication state changes (LOGIN, AUTHENTICATE, STARTTLS).
        (_, UR::Capability(_)) => Either,

        // ---- LOGIN (RFC 3501 Section6.2.3) ----
        // LOGIN has no untagged responses of its own; any response is
        // either unsolicited async data or a BYE. Handled by the
        // Status row below.

        // ---- SELECT / EXAMINE (RFC 3501 Section6.3.1 / Section6.3.2) ----
        // SELECT/EXAMINE produce mandatory EXISTS, RECENT, FLAGS in
        // their response sequence.
        (CK::Select | CK::Examine, UR::Exists(_) | UR::Recent(_) | UR::Flags(_)) => OnlySolicited,
        // RFC 9051 Section6.3.2: IMAP4rev2 SELECT/EXAMINE solicits exactly
        // one LIST for the selected mailbox. When NOTIFY is active,
        // NOTIFY MailboxName events also arrive as LIST responses.
        // Classify as Either so the consumer can inspect markers and
        // distinguish solicited from asynchronous LIST.
        (CK::Select | CK::Examine, UR::List(_)) => Either,
        // RFC 3501 Section7.3: EXISTS/RECENT arrive asynchronously in
        // selected state from other clients' actions.
        (_, UR::Exists(_) | UR::Recent(_)) => Either,
        // RFC 3501 Section7.2.6: FLAGS can arrive unsolicited after a
        // STORE that changes the set of defined flags.
        (_, UR::Flags(_)) => Either,

        // ---- LIST / LIST-EXTENDED / LIST-STATUS (RFC 3501 Section6.3.8, RFC 5258 Section3, RFC 5819 Section2) ----
        (CK::List | CK::ListStatus, UR::List(_)) => OnlySolicited,
        // RFC 5465 Section5.4-5.5: NOTIFY MailboxName/SubscriptionChange
        // events are delivered as unsolicited LIST responses.
        (_, UR::List(_)) if ctx.notify.list => OnlyUnsolicited,
        // Outside LIST/LIST-STATUS without NOTIFY list events, LIST
        // responses are impossible per RFC 3501.
        (_, UR::List(_)) => Impossible,

        // ---- LSUB (RFC 3501 Section6.3.9) ----
        (CK::Lsub, UR::Lsub(_)) => OnlySolicited,
        (_, UR::Lsub(_)) => Impossible,

        // ---- STATUS (RFC 3501 Section6.3.10) ----
        // STATUS solicits a STATUS response for the named mailbox.
        // If the mailbox matches the command target, it is solicited;
        // otherwise it is unsolicited (e.g., NOTIFY-delivered STATUS).
        (CK::Status, UR::MailboxStatus { mailbox, .. }) => {
            debug_assert!(
                ctx.command_target.is_some(),
                "STATUS command dispatched without setting command_target"
            );
            match ctx.command_target {
                Some(target) if mailbox.matches_mailbox(target) => OnlySolicited,
                _ => OnlyUnsolicited,
            }
        }

        // RFC 5819 Section2: LIST-STATUS solicits STATUS responses for each
        // listed mailbox alongside the LIST responses.
        (CK::ListStatus, UR::MailboxStatus { .. }) => OnlySolicited,
        // RFC 5465 Section5.1-5.2: NOTIFY MessageNew/MessageExpunge on
        // non-selected mailboxes or STATUS indicator delivers unsolicited
        // STATUS responses.
        (_, UR::MailboxStatus { .. }) if ctx.notify.status => OnlyUnsolicited,
        // Outside STATUS/LIST-STATUS without NOTIFY status events,
        // STATUS responses are impossible per RFC 3501 Section7.2.4.
        (_, UR::MailboxStatus { .. }) => Impossible,

        // ---- APPEND (RFC 3501 Section6.3.11) ----
        // APPEND has no untagged responses of its own (the append
        // result is a tagged OK with [APPENDUID]). Any untagged is
        // unsolicited or async  -  handled by the other rows.

        // ---- CHECK (RFC 3501 Section6.4.1) ----
        // CHECK has no untagged responses of its own. Status row covers it.

        // ---- CLOSE (RFC 3501 Section6.4.2) ----
        // RFC 3501 Section6.4.2: CLOSE silently expunges  -  EXPUNGEs are solicited.
        // RFC 3691 Section3: UNSELECT does NOT expunge. Any EXPUNGE during UNSELECT
        // falls through to `(_, UR::Expunge(_)) => Either`  -  correctly treated
        // as an async notification, not a solicited response.
        (CK::Close, UR::Expunge(_)) => OnlySolicited,

        // ---- EXPUNGE (RFC 3501 Section6.4.3) / UID EXPUNGE (RFC 4315) ----
        (CK::Expunge, UR::Expunge(_)) => OnlySolicited,
        // RFC 3501 Section7.4.1: EXPUNGE can arrive during any selected-state
        // command as an asynchronous notification.
        (_, UR::Expunge(_)) => Either,

        // ---- SEARCH (RFC 3501 Section6.4.4) / UID SEARCH (RFC 3501 Section6.4.8) ----
        (CK::Search | CK::SearchReturn | CK::SearchSave, UR::Search { .. }) => OnlySolicited,
        // SEARCH responses cannot appear outside a SEARCH variant.
        (_, UR::Search { .. }) => Impossible,

        // ---- FETCH (RFC 3501 Section6.4.5) / UID FETCH (RFC 3501 Section6.4.8) ----
        // During a FETCH, the response is either the solicited reply or
        // an asynchronous update. RFC 3501 Section7.4.2 permits both.
        (CK::Fetch, UR::Fetch(_)) => Either,
        // Outside a FETCH/UID FETCH command, a FETCH response is an
        // asynchronous notification (e.g., from another client's STORE).
        (_, UR::Fetch(_)) => Either,

        // ---- STORE (RFC 3501 Section6.4.6) / UID STORE (RFC 3501 Section6.4.8) ----
        // STORE produces untagged FETCH responses with the updated flags
        // (Section6.4.6). Those are handled by the Fetch arms above  -  they are
        // Either because the same mailbox may see both solicited STORE-
        // induced FETCH and async FETCH from another client. The consumer
        // decides via its internal state.

        // ---- MOVE (RFC 6851 Section3.3) ----
        // MOVE produces solicited EXPUNGE responses for moved messages.
        // These are indistinguishable from async EXPUNGE notifications on
        // the wire, so they fall through to the Either arm above. The
        // consumer discriminates via its internal state.

        // ---- COPY (RFC 3501 Section6.4.7) / UID COPY (RFC 3501 Section6.4.8) ----
        // COPY has no untagged responses of its own. Async EXPUNGE /
        // EXISTS / FETCH covered by the rows above.

        // ---- OK / NO / BAD / BYE status responses (RFC 3501 Section7.1) ----
        // These are context-dependent: they can be the tagged
        // completion, an unsolicited server status, or BYE shutdown.
        (_, UR::Status { .. }) => Either,

        // ---- NAMESPACE (RFC 2342 Section5) ----
        (CK::Namespace, UR::Namespace { .. }) => OnlySolicited,
        // NAMESPACE responses cannot appear outside a NAMESPACE command.
        (_, UR::Namespace { .. }) => Impossible,

        // ---- ID (RFC 2971 Section3.2) ----
        (CK::Id, UR::Id(_)) => OnlySolicited,
        // ID responses cannot appear outside an ID command.
        (_, UR::Id(_)) => Impossible,

        // ---- ENABLE (RFC 5161 Section3.2) ----
        // ENABLE solicits exactly one ENABLED response listing the
        // extensions the server actually enabled (RFC 5161 Section3.2).
        (CK::Enable, UR::Enabled(_)) => OnlySolicited,
        // ENABLED responses cannot appear outside an ENABLE command.
        (_, UR::Enabled(_)) => Impossible,

        // ---- ESEARCH (RFC 4731 Section3.1) ----
        // SEARCH RETURN and SEARCH RETURN (SAVE) produce ESEARCH
        // instead of the base SEARCH response.
        (CK::Search | CK::SearchReturn | CK::SearchSave, UR::Esearch(_)) => OnlySolicited,
        // ESEARCH responses cannot appear outside a SEARCH variant.
        (_, UR::Esearch(_)) => Impossible,

        // ---- QRESYNC VANISHED (RFC 7162 Section3.2.10) ----
        // SELECT/EXAMINE with QRESYNC solicits VANISHED (EARLIER) in
        // the initial mailbox state response.
        (CK::Select | CK::Examine, UR::Vanished { earlier: true, .. }) => OnlySolicited,
        // UID FETCH with CHANGEDSINCE+VANISHED solicits VANISHED (EARLIER).
        (CK::Fetch, UR::Vanished { earlier: true, .. }) => OnlySolicited,
        // UID EXPUNGE may produce VANISHED (EARLIER) per RFC 7162 Section3.2.10.
        (CK::Expunge, UR::Vanished { earlier: true, .. }) => OnlySolicited,
        // Non-EARLIER VANISHED can arrive asynchronously as an
        // expunge notification (RFC 7162 Section3.2.10).
        (_, UR::Vanished { earlier: false, .. }) => Either,
        // VANISHED (EARLIER) outside SELECT/EXAMINE/FETCH/EXPUNGE is
        // impossible  -  servers only send it in response to those commands.
        (_, UR::Vanished { earlier: true, .. }) => Impossible,

        // ---- QUOTA (RFC 9208 Section5.1 / Section5.2, originally RFC 2087) ----
        (CK::GetQuota | CK::GetQuotaRoot | CK::SetQuota, UR::Quota { .. }) => OnlySolicited,
        (_, UR::Quota { .. }) => Impossible,
        (CK::GetQuotaRoot, UR::QuotaRoot { .. }) => OnlySolicited,
        (_, UR::QuotaRoot { .. }) => Impossible,

        // ---- ACL (RFC 4314 Section3.3-3.5, Section3.6-3.8) ----
        (CK::GetAcl, UR::Acl { .. }) => OnlySolicited,
        (_, UR::Acl { .. }) => Impossible,
        (CK::ListRights, UR::ListRights { .. }) => OnlySolicited,
        (_, UR::ListRights { .. }) => Impossible,
        (CK::MyRights, UR::MyRights { .. }) => OnlySolicited,
        (_, UR::MyRights { .. }) => Impossible,

        // ---- METADATA (RFC 5464 Section4.2 / Section4.4) ----
        (CK::GetMetadata, UR::Metadata { .. }) => OnlySolicited,
        // RFC 5465 Section5.6-5.7: NOTIFY MailboxMetadataChange/ServerMetadataChange
        // events are delivered as unsolicited METADATA responses.
        (_, UR::Metadata { .. }) if ctx.notify.metadata => OnlyUnsolicited,
        // Outside GETMETADATA without NOTIFY metadata events,
        // METADATA responses are impossible.
        (_, UR::Metadata { .. }) => Impossible,

        // ---- THREAD (RFC 5256 Section4) ----
        (CK::Thread, UR::Thread(_)) => OnlySolicited,
        // THREAD responses cannot appear outside a THREAD command.
        (_, UR::Thread(_)) => Impossible,

        // ---- SORT (RFC 5256 Section4) ----
        (CK::Sort, UR::Sort { .. }) => OnlySolicited,
        // SORT responses cannot appear outside a SORT command.
        (_, UR::Sort { .. }) => Impossible,

        // ---- Unknown / extension responses (RFC 9051 Section2.2.2) ----
        // Servers may send unrecognized untagged responses at any time.
        // Per RFC 9051 Section2.2.2, clients MUST tolerate them. Treat them
        // as unsolicited data  -  they are never part of a solicited result.
        (_, UR::Unknown(_)) => OnlyUnsolicited,

        // ---- Default ----
        // All known (command, response) pairs are covered above. This
        // arm should be unreachable; the debug_assert catches any
        // newly added variant that lacks a row.
        _ => {
            debug_assert!(
                false,
                "classify: missing row for cmd={:?}, resp={:?}",
                cmd,
                std::mem::discriminant(resp)
            );
            Impossible
        }
    }
}
