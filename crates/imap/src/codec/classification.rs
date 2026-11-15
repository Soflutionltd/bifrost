//! RFC truth table for "can this untagged response appear as part of
//! the solicited result for this command?"
//!
//! Every match arm cites the RFC section that justifies it. This is
//! the single source of routing truth for the entire crate. The
//! dispatcher calls `classify` for every untagged response and routes
//! based on the returned `SolicitationRule`.
//!

#[cfg(test)]
#[path = "classification_tests.rs"]
mod tests;

use crate::connection::NotifyFlags;
use crate::types::CommandKind;
use crate::types::CommandKind as CK;
use crate::types::response::UntaggedResponse;
use crate::types::response::UntaggedResponse as UR;
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

// The owner sets are used both to classify responses and to keep overlapping
// command families out of the same pipelined sub-batch. Non-pipelinable owners
// participate in classification but cannot bridge pipeline groups.
macro_rules! solicited_rows {
    ($($response:pat => [$($owner:path),+]),+ $(,)?) => {
        fn solicited_owners(resp: &UntaggedResponse) -> &'static [CommandKind] {
            match resp {
                $($response => &[$($owner),+],)+
                UR::Fetch(_) | UR::Status { .. }
                | UR::Vanished { earlier: false, .. } | UR::Unknown(_) => &[],
            }
        }

        const SOLICITED_OWNER_GROUPS: &[&[CommandKind]] = &[
            $(&[$($owner),+],)+
        ];
    };
}

solicited_rows! {
    UR::Capability(_) => [CK::Capability],
    UR::Exists(_) | UR::Recent(_) | UR::Flags(_) => [CK::Select, CK::Examine],
    UR::List(_) => [CK::List, CK::ListStatus],
    UR::Lsub(_) => [CK::Lsub],
    UR::MailboxStatus { .. } => [CK::ListStatus, CK::Status],
    UR::Expunge(_) => [CK::Close, CK::Expunge],
    UR::Search { .. } => [CK::Search, CK::SearchReturn, CK::SearchSave],
    UR::Namespace { .. } => [CK::Namespace],
    UR::Id(_) => [CK::Id],
    UR::Enabled(_) => [CK::Enable],
    UR::Esearch(_) => [CK::Search, CK::SearchReturn, CK::SearchSave],
    UR::Vanished { earlier: true, .. } => [CK::Select, CK::Examine, CK::Fetch, CK::Expunge],
    UR::Quota { .. } => [CK::GetQuota, CK::GetQuotaRoot, CK::SetQuota],
    UR::QuotaRoot { .. } => [CK::GetQuotaRoot],
    UR::Acl { .. } => [CK::GetAcl],
    UR::ListRights { .. } => [CK::ListRights],
    UR::MyRights { .. } => [CK::MyRights],
    UR::Metadata { .. } => [CK::GetMetadata],
    UR::Thread(_) => [CK::Thread],
    UR::Sort { .. } => [CK::Sort],
}

/// Connected components of the solicited owner sets, restricted to commands
/// the pipeline admits. Repeating relaxation closes overlaps such as LIST /
/// LIST-STATUS / STATUS without a second hand-maintained family list.
pub(crate) fn untagged_routing_key(cmd: CommandKind) -> CommandKind {
    if !cmd.pipelinable() {
        return cmd;
    }
    let mut component = vec![cmd];
    loop {
        let previous_len = component.len();
        for group in SOLICITED_OWNER_GROUPS {
            if group.iter().any(|owner| component.contains(owner)) {
                for &owner in *group {
                    if owner.pipelinable() && !component.contains(&owner) {
                        component.push(owner);
                    }
                }
            }
        }
        if component.len() == previous_len {
            return component
                .into_iter()
                .min_by_key(|owner| *owner as usize)
                .unwrap_or(cmd);
        }
    }
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
    use SolicitationRule::{Either, Impossible, OnlySolicited, OnlyUnsolicited};
    use UntaggedResponse as UR;

    // STATUS additionally correlates the mailbox identity below.
    if cmd != CK::Status && solicited_owners(resp).contains(&cmd) {
        return OnlySolicited;
    }

    // Exhaustive over response variants: a new response needs an explicit
    // unsolicited policy as well as a solicited owner set above.
    #[allow(clippy::match_same_arms)]
    match resp {
        // RFC 3501 Sections 7.1, 7.2.1, 7.2.6, 7.3 and 7.4.2.
        UR::Capability(_)
        | UR::Exists(_)
        | UR::Recent(_)
        | UR::Flags(_)
        | UR::Fetch(_)
        | UR::Expunge(_)
        | UR::Status { .. } => Either,
        // RFC 9051 Section 6.3.2; RFC 5465 Sections 5.4-5.5.
        UR::List(_) => match cmd {
            CK::Select | CK::Examine => Either,
            _ if ctx.notify.list => OnlyUnsolicited,
            _ => Impossible,
        },
        // RFC 3501 Section 6.3.10; RFC 5819 Section 2;
        // RFC 5465 Sections 5.1-5.2.
        UR::MailboxStatus { mailbox, .. } => match cmd {
            CK::Status => {
                debug_assert!(
                    ctx.command_target.is_some(),
                    "STATUS command dispatched without setting command_target"
                );
                match ctx.command_target {
                    Some(target) if mailbox.matches_mailbox(target) => OnlySolicited,
                    _ => OnlyUnsolicited,
                }
            }
            _ if ctx.notify.status => OnlyUnsolicited,
            _ => Impossible,
        },
        // RFC 7162 Section 3.2.10.
        UR::Vanished { earlier: false, .. } => Either,
        UR::Vanished { earlier: true, .. } => Impossible,
        // RFC 5464 Section 4.4; RFC 5465 Sections 5.6-5.7.
        UR::Metadata { .. } => {
            if ctx.notify.metadata {
                OnlyUnsolicited
            } else {
                Impossible
            }
        }
        // RFC 3501 Sections 7.2.3, 7.2.5; RFC 2342 Section 5;
        // RFC 2971 Section 3.2; RFC 5161 Section 3.2; RFC 4731 Section 3.1;
        // RFC 9208 Sections 5.1-5.2; RFC 4314 Sections 3.6-3.8;
        // RFC 5256 Section 4. Outside their owners these cannot be solicited.
        UR::Lsub(_)
        | UR::Search { .. }
        | UR::Namespace { .. }
        | UR::Id(_)
        | UR::Enabled(_)
        | UR::Esearch(_)
        | UR::Quota { .. }
        | UR::QuotaRoot { .. }
        | UR::Acl { .. }
        | UR::ListRights { .. }
        | UR::MyRights { .. }
        | UR::Thread(_)
        | UR::Sort { .. } => Impossible,
        // RFC 9051 Section 2.2.2: tolerate unrecognized extension responses.
        UR::Unknown(_) => OnlyUnsolicited,
    }
}
