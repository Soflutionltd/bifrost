use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{
    ResponseCode, StatusKind, TaggedResponse, UntaggedResponse, UntaggedStatus,
};
use crate::types::{Flag, SelectedMailbox};

use super::super::is_notify_list_event;
use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for SELECT (RFC 3501 Section6.3.1) and EXAMINE (RFC 3501 Section6.3.2).
///
/// Accumulates the mandatory untagged response sequence (EXISTS, RECENT,
/// FLAGS) plus optional response codes (UIDVALIDITY, UIDNEXT,
/// PERMANENTFLAGS, HIGHESTMODSEQ, NOMODSEQ, UNSEEN, MAILBOXID,
/// UIDNOTSTICKY) and QRESYNC data (VANISHED EARLIER, FETCH with changed
/// flags  -  RFC 7162 Section3.2.5.2).
///
/// Unlike [`FetchVanishedConsumer`], this consumer does **not** filter
/// `VANISHED (EARLIER)` responses against the `known-uids` set.
/// RFC 7162 Section 3.2.5.2: during SELECT/EXAMINE with QRESYNC,
/// `known-uids` is a server hint for optimization, not a scoping
/// constraint  -  the server may legitimately return expunged UIDs outside
/// the known set based on its own `seq-match-data` computation.
///
/// The NO / BAD / validation-failure paths put their error in
/// `Finalized::output` and still reclassify every accumulated response as
/// an event, so old-mailbox notifications survive a failed SELECT.
pub(crate) struct SelectConsumer {
    /// Whether this is EXAMINE (always read-only) or SELECT.
    is_examine: bool,
    /// All responses delivered by the dispatcher. Partitioned in `finalize`
    /// on the `[CLOSED]` boundary (RFC 7162 Section3.2.11).
    responses: Vec<UntaggedResponse>,
}

impl SelectConsumer {
    pub(crate) fn new(is_examine: bool) -> Self {
        Self {
            is_examine,
            responses: Vec::new(),
        }
    }
}

impl Consumer for SelectConsumer {
    type Output = SelectedMailbox;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // Accumulate everything. Partitioning on [CLOSED] and filtering
        // non-SELECT types happens in finalize.
        self.responses.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<SelectedMailbox> {
        match tagged.status {
            // NO / BAD: reclassify all accumulated responses as events.
            // They may be legitimate unsolicited updates for the previously
            // selected mailbox (RFC 3501 Section7).
            StatusKind::No => Finalized::failure(
                Error::no_with_code(tagged.text, tagged.code),
                self.responses,
            ),
            StatusKind::Bad => Finalized::failure(
                Error::bad_with_code(tagged.text, tagged.code),
                self.responses,
            ),
            StatusKind::Ok => {
                let read_only = if self.is_examine {
                    true
                } else {
                    // RFC 3501 Section6.3.1: [READ-ONLY] in the tagged OK means the
                    // mailbox was opened read-only despite being SELECT'd.
                    tagged.code.as_ref() == Some(&ResponseCode::ReadOnly)
                };

                let (inputs, reclassified, order) = partition_select(self.responses, ctx);
                if let Err(e) = validate_select_responses(&inputs, &tagged, self.is_examine, ctx) {
                    // A malformed result must not hide unrelated events.
                    let mut inputs = inputs.into_iter();
                    let mut events = reclassified.into_iter();
                    let all = order
                        .into_iter()
                        .map(|selected| {
                            (if selected {
                                inputs.next()
                            } else {
                                events.next()
                            })
                            .expect("partition retains each response")
                        })
                        .collect();
                    return Finalized::failure(e, all);
                }
                let result = build_selected_mailbox(&inputs, &tagged, read_only);

                Finalized::success(result, reclassified)
            }
        }
    }
}

/// Partition responses into events after a successful SELECT/EXAMINE.
///
/// Pre-`[CLOSED]` responses are old-mailbox data and always reclassified.
/// Post-`[CLOSED]` responses are split: SELECT-solicited types (EXISTS,
/// RECENT, FLAGS, VANISHED, FETCH, status codes, and the solicited rev2
/// LIST) are consumed by [`build_selected_mailbox`]; everything else
/// (EXPUNGE, NOTIFY-marked LIST, etc.) is reclassified as events.
fn partition_select(
    responses: Vec<UntaggedResponse>,
    ctx: &ConsumerContext,
) -> (Vec<UntaggedResponse>, Vec<UntaggedResponse>, Vec<bool>) {
    let boundary = responses.len() - selected_mailbox_effective_responses(&responses).len();
    let mut inputs = Vec::new();
    let mut events = Vec::new();
    let mut order = Vec::new();
    let mut consumed_select_list = false;
    for (index, response) in responses.into_iter().enumerate() {
        if boundary != 0 && index + 1 == boundary {
            // The CLOSED delimiter itself has no mailbox payload.
            continue;
        }
        if index >= boundary
            && is_select_solicited_response(&response, ctx, &mut consumed_select_list)
        {
            order.push(true);
            inputs.push(response);
        } else {
            order.push(false);
            events.push(response);
        }
    }
    (inputs, events, order)
}

#[cfg(test)]
fn reclassify_select_responses(
    responses: Vec<UntaggedResponse>,
    ctx: &ConsumerContext,
) -> Vec<UntaggedResponse> {
    partition_select(responses, ctx).1
}

/// Check whether a response is one of the types solicited by SELECT/EXAMINE.
///
/// Used to partition post-`[CLOSED]` responses: SELECT types are consumed
/// by [`build_selected_mailbox`]; everything else is reclassified as an
/// event.
///
/// For LIST: only the first unmarked LIST matching the command target is
/// consumed as the mandatory rev2 response (RFC 9051 Section6.3.2). NOTIFY-
/// marked LIST responses (OLDNAME, `\NonExistent`, `\NoAccess`) are
/// always reclassified as events.
///
/// Non-EARLIER VANISHED during SELECT is an asynchronous expunge
/// notification for the selected mailbox and must be reclassified as
/// an event. Only VANISHED (EARLIER) is part of the QRESYNC SELECT
/// result.
fn is_select_solicited_response(
    resp: &UntaggedResponse,
    ctx: &ConsumerContext,
    consumed_select_list: &mut bool,
) -> bool {
    match resp {
        UntaggedResponse::Exists(_)
        | UntaggedResponse::Recent(_)
        | UntaggedResponse::Flags(_)
        | UntaggedResponse::Vanished { earlier: true, .. }
        | UntaggedResponse::Fetch(_) => true,
        // Only successful SELECT metadata belongs to the result. Preserve
        // other coded statuses as events: users must still see warnings and
        // extension signals that arrived while their folder was opening.
        UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code: Some(code),
            ..
        } => matches!(
            code,
            ResponseCode::UidValidity(_)
                | ResponseCode::UidNext(_)
                | ResponseCode::PermanentFlags(_)
                | ResponseCode::HighestModSeq(_)
                | ResponseCode::NoModSeq
                | ResponseCode::Unseen(_)
                | ResponseCode::MailboxId(_)
                | ResponseCode::UidNotSticky
        ),
        // RFC 9051 Section6.3.2: rev2 SELECT solicits exactly one LIST for
        // the selected mailbox. Consume the first unmarked LIST
        // matching the command target; reclassify NOTIFY-marked LIST.
        UntaggedResponse::List(info) => {
            if *consumed_select_list {
                return false;
            }
            if let Some(target) = ctx.command_target()
                && target.matches_mailbox(&info.name)
                && !is_notify_list_event(info, true)
            {
                *consumed_select_list = true;
                return true;
            }
            false
        }
        _ => false,
    }
}

/// Validate that the mandatory SELECT/EXAMINE responses are present
/// (RFC 3501 Section6.3.1-6.3.2, RFC 9051 Section6.3.2-6.3.3).
///
/// For `IMAP4rev1`: FLAGS, EXISTS, and RECENT are required.
/// For `IMAP4rev2`: FLAGS, EXISTS, and a matching LIST are required.
fn validate_select_responses(
    effective: &[UntaggedResponse],
    tagged: &TaggedResponse,
    is_examine: bool,
    ctx: &ConsumerContext,
) -> Result<(), Error> {
    // RFC 9051 Section6.3.1, via the single authority in
    // `crate::types::profile`. This decides whether a SELECT must carry a
    // RECENT (rev1) or a matching LIST (rev2), so an inlined copy that drifted
    // would reject every SELECT against a conforming server.
    let is_rev2 = ctx.session.imap4rev2_active();

    let command_name = if is_examine { "EXAMINE" } else { "SELECT" };
    let section = match (is_rev2, is_examine) {
        (true, false) => "RFC 9051 Section 6.3.2",
        (true, true) => "RFC 9051 Section 6.3.3",
        (false, false) => "RFC 3501 Section 6.3.1",
        (false, true) => "RFC 3501 Section 6.3.2",
    };

    let mut saw_flags = false;
    let mut saw_exists = false;
    let mut saw_recent = false;
    let mut saw_list = false;

    for resp in effective {
        match resp {
            UntaggedResponse::Flags(_) => saw_flags = true,
            UntaggedResponse::Exists(_) => saw_exists = true,
            UntaggedResponse::Recent(_) => saw_recent = true,
            // RFC 9051 Section6.3.2: the solicited SELECT LIST has no NOTIFY
            // markers. Exclude marker-bearing LIST (OLDNAME,
            // \NonExistent, \NoAccess); those are NOTIFY events, not
            // the mandatory solicited response.
            UntaggedResponse::List(info) => {
                if let Some(target) = ctx.command_target()
                    && target.matches_mailbox(&info.name)
                    && !is_notify_list_event(info, true)
                {
                    saw_list = true;
                }
            }
            _ => {}
        }
    }

    if !saw_flags {
        return Err(Error::ProtocolMissing(format!(
            "{command_name} completed without the required FLAGS response ({section})"
        )));
    }
    if !saw_exists {
        return Err(Error::ProtocolMissing(format!(
            "{command_name} completed without the required EXISTS response ({section})"
        )));
    }
    if is_rev2 {
        if !saw_list {
            return Err(Error::ProtocolMissing(format!(
                "{command_name} completed without the required LIST response ({section})"
            )));
        }
    } else if !saw_recent {
        return Err(Error::ProtocolMissing(format!(
            "{command_name} completed without the required RECENT response ({section})"
        )));
    }

    let untagged_validity = effective
        .iter()
        .filter_map(|resp| match resp {
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: Some(ResponseCode::UidValidity(value)),
                ..
            } if *value != 0 => Some(*value),
            _ => None,
        })
        .next_back();
    let validity = match tagged.code.as_ref() {
        Some(ResponseCode::UidValidity(value)) if *value != 0 => Some(*value),
        _ => untagged_validity,
    };
    if validity.is_none_or(|value| value == 0) {
        return Err(Error::ProtocolMissing(format!(
            "{command_name} completed without the required nonzero UIDVALIDITY ({section})"
        )));
    }

    Ok(())
}

/// Build a `SelectedMailbox` from collected untagged and tagged responses.
///
/// Extracts EXISTS, RECENT, FLAGS, UIDVALIDITY, UIDNEXT, PERMANENTFLAGS,
/// HIGHESTMODSEQ, and UIDNOTSTICKY from the responses per RFC 3501 Section 7.
pub(in crate::connection) fn build_selected_mailbox(
    untagged: &[UntaggedResponse],
    tagged: &TaggedResponse,
    read_only: bool,
) -> SelectedMailbox {
    let mut exists = 0;
    let mut recent = 0;
    let mut uid_validity: Option<u32> = None;
    let mut uid_next = Option::None;
    let mut flags = Vec::new();
    let mut permanent_flags = Vec::new();
    let mut modseq_support = crate::types::mailbox::ModSeqSupport::Unknown;
    let mut unseen: Option<u32> = None;
    // RFC 8474 Section 5.1: unique mailbox identifier from [MAILBOXID (<id>)].
    let mut mailbox_id: Option<String> = None;
    // RFC 4315 Section 2 / RFC 9051 Section 7.1: [UIDNOTSTICKY]  -  UIDs are not persistent.
    let mut uid_not_sticky = false;
    // RFC 7162 Section 3.2.5.2: QRESYNC SELECT responses include
    // VANISHED (EARLIER) with UIDs removed since last sync, and
    // FETCH responses with changed flags.
    let mut vanished = Vec::new();
    let mut changed_messages = Vec::new();

    for resp in untagged {
        match resp {
            UntaggedResponse::Exists(n) => exists = *n,
            UntaggedResponse::Recent(n) => recent = *n,
            UntaggedResponse::Flags(f) => flags.clone_from(f),
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: Some(code),
                ..
            } => {
                extract_selected_code(
                    code,
                    &mut uid_validity,
                    &mut uid_next,
                    &mut permanent_flags,
                    &mut modseq_support,
                    &mut unseen,
                    &mut mailbox_id,
                    &mut uid_not_sticky,
                );
            }
            // RFC 7162 Section 3.2.5.2: capture VANISHED (EARLIER) responses
            // sent during QRESYNC SELECT/EXAMINE. Only `earlier: true` responses
            // belong to the initial sync; `earlier: false` are unsolicited and
            // should not be included in the SelectedMailbox.
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                vanished.extend_from_slice(uids);
            }
            // RFC 7162 Section 3.2.5.2: capture FETCH responses with
            // updated flags sent during QRESYNC SELECT/EXAMINE.
            UntaggedResponse::Fetch(fetch) => {
                changed_messages.push((**fetch).clone());
            }
            _ => {}
        }
    }

    // Also check the tagged response code
    if let Some(code) = &tagged.code {
        extract_selected_code(
            code,
            &mut uid_validity,
            &mut uid_next,
            &mut permanent_flags,
            &mut modseq_support,
            &mut unseen,
            &mut mailbox_id,
            &mut uid_not_sticky,
        );
    }

    SelectedMailbox {
        exists,
        recent,
        uid_validity,
        uid_next,
        flags,
        permanent_flags,
        modseq_support,
        unseen,
        mailbox_id,
        read_only,
        uid_not_sticky,
        vanished,
        changed_messages,
    }
}

/// Restrict SELECT/EXAMINE processing to responses belonging to the newly
/// selected mailbox.
///
/// RFC 7162 Section 3.2.11: when switching mailboxes, `* OK [CLOSED]`
/// separates responses for the previously selected mailbox from the new one.
pub(in crate::connection) fn selected_mailbox_effective_responses(
    untagged: &[UntaggedResponse],
) -> &[UntaggedResponse] {
    match untagged.iter().rposition(|r| {
        matches!(
            r,
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: Some(ResponseCode::Closed),
                ..
            }
        )
    }) {
        Some(closed_idx) => &untagged[closed_idx + 1..],
        None => untagged,
    }
}

/// Extract mailbox metadata from a response code (RFC 3501 Section 7.1).
#[allow(clippy::too_many_arguments)]
fn extract_selected_code(
    code: &ResponseCode,
    uid_validity: &mut Option<u32>,
    uid_next: &mut Option<u32>,
    permanent_flags: &mut Vec<Flag>,
    modseq_support: &mut crate::types::mailbox::ModSeqSupport,
    unseen: &mut Option<u32>,
    mailbox_id: &mut Option<String>,
    uid_not_sticky: &mut bool,
) {
    match code {
        // Preserve raw numbers in the decoder, but never turn a zero wire
        // sentinel into a usable mailbox identity. Users expect a broken
        // server epoch to fail before inventory can publish unusable ids.
        ResponseCode::UidValidity(v) => {
            // A later invalid observation must not erase a usable epoch.
            if let Some(value) = crate::types::UidValidity::new(*v) {
                *uid_validity = Some(value.get());
            }
        }
        ResponseCode::UidNext(v) => {
            if let Some(value) = std::num::NonZeroU32::new(*v) {
                *uid_next = Some(value.get());
            }
        }
        ResponseCode::PermanentFlags(f) => permanent_flags.clone_from(f),
        ResponseCode::HighestModSeq(v) => {
            // Once support is explicitly denied in this SELECT, do not
            // enable guarded operations from a contradictory later code.
            // Enterprise users expect conservative concurrency protection.
            if !matches!(
                modseq_support,
                crate::types::mailbox::ModSeqSupport::NoModSeq
            ) {
                *modseq_support = crate::types::mailbox::ModSeqSupport::from_highest(*v);
            }
        }
        ResponseCode::NoModSeq => {
            *modseq_support = crate::types::mailbox::ModSeqSupport::NoModSeq;
        }
        // RFC 3501 Section 7.1: [UNSEEN n]  -  first unseen message sequence number.
        // Dropped in IMAP4rev2 (RFC 9051), but commonly sent by rev1 servers.
        ResponseCode::Unseen(v) => *unseen = Some(*v),
        // RFC 8474 Section 5.1: [MAILBOXID (<id>)]  -  unique mailbox identifier.
        ResponseCode::MailboxId(id) => *mailbox_id = Some(id.clone()),
        // RFC 4315 Section 2 / RFC 9051 Section 7.1: [UIDNOTSTICKY]  -
        // UIDs assigned to messages in this mailbox are not persistent.
        ResponseCode::UidNotSticky => *uid_not_sticky = true,
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MailboxInfo, MailboxName, UidRange};

    #[test]
    fn select_preserves_unrecognized_and_failure_statuses() {
        let warning = UntaggedResponse::Status {
            status: UntaggedStatus::No,
            code: Some(ResponseCode::UidNext(7)),
            text: "warning".into(),
        };
        let unknown = UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code: Some(ResponseCode::Other {
                name: "FUTURE".into(),
                value: None,
            }),
            text: "extension".into(),
        };
        let metadata = UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code: Some(ResponseCode::UidValidity(9)),
            text: "validity".into(),
        };
        let (inputs, events, _) = partition_select(
            vec![warning.clone(), metadata.clone(), unknown.clone()],
            &default_ctx(),
        );
        assert_eq!(inputs, vec![metadata]);
        assert_eq!(events, vec![warning, unknown]);
    }

    #[test]
    fn select_requires_final_nonzero_uidvalidity() {
        let inputs = vec![
            UntaggedResponse::Flags(vec![]),
            UntaggedResponse::Exists(0),
            UntaggedResponse::Recent(0),
        ];
        let mut tagged = TaggedResponse {
            tag: "A001".into(),
            status: StatusKind::Ok,
            code: None,
            text: "done".into(),
        };
        assert!(matches!(
            validate_select_responses(&inputs, &tagged, false, &default_ctx()),
            Err(Error::ProtocolMissing(_))
        ));
        tagged.code = Some(ResponseCode::UidValidity(1));
        assert!(validate_select_responses(&inputs, &tagged, false, &default_ctx()).is_ok());
        tagged.code = Some(ResponseCode::UidValidity(0));
        assert!(validate_select_responses(&inputs, &tagged, false, &default_ctx()).is_err());
    }

    #[test]
    fn later_zero_codes_preserve_valid_selection_numbers() {
        let mut inputs = vec![
            UntaggedResponse::Flags(vec![]),
            UntaggedResponse::Exists(0),
            UntaggedResponse::Recent(0),
        ];
        for code in [
            ResponseCode::UidValidity(9),
            ResponseCode::UidNext(12),
            ResponseCode::UidValidity(0),
            ResponseCode::UidNext(0),
        ] {
            inputs.push(UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: Some(code),
                text: String::new(),
            });
        }
        for code in [ResponseCode::UidValidity(0), ResponseCode::UidNext(0)] {
            let tagged = TaggedResponse {
                tag: "A001".into(),
                status: StatusKind::Ok,
                code: Some(code),
                text: String::new(),
            };
            assert!(validate_select_responses(&inputs, &tagged, false, &default_ctx()).is_ok());
            let selected = build_selected_mailbox(&inputs, &tagged, false);
            assert_eq!(selected.uid_validity, Some(9));
            assert_eq!(selected.uid_next, Some(12));
        }
    }

    fn default_ctx() -> ConsumerContext<'static> {
        ConsumerContext {
            session: crate::types::profile::SessionCapabilities::empty(),
            command_target: None,
            command_tag: "A001",
        }
    }

    #[test]
    fn select_reclassifies_plain_vanished_as_event() {
        let responses = vec![
            UntaggedResponse::Exists(3),
            UntaggedResponse::Recent(0),
            UntaggedResponse::Flags(vec![]),
            UntaggedResponse::Vanished {
                earlier: true,
                uids: vec![UidRange::single(10)],
            },
            UntaggedResponse::Vanished {
                earlier: false,
                uids: vec![UidRange::single(11)],
            },
        ];

        let reclassified = reclassify_select_responses(responses, &default_ctx());

        assert_eq!(
            reclassified,
            vec![UntaggedResponse::Vanished {
                earlier: false,
                uids: vec![UidRange::single(11)],
            }]
        );
    }

    #[test]
    fn select_consumes_only_the_matching_wire_identity_list() {
        let target = MailboxName::new("a").expect("valid mailbox");
        let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let context = ConsumerContext {
            session: crate::types::profile::SessionCapabilities::empty(),
            command_target: Some(&target),
            command_tag: "A001",
        };
        let responses = vec![
            UntaggedResponse::List(MailboxInfo {
                name: noncanonical.clone(),
                ..MailboxInfo::default()
            }),
            UntaggedResponse::List(MailboxInfo {
                name: target.clone(),
                ..MailboxInfo::default()
            }),
        ];

        assert_eq!(
            reclassify_select_responses(responses, &context),
            vec![UntaggedResponse::List(MailboxInfo {
                name: noncanonical,
                ..MailboxInfo::default()
            }),]
        );
    }
}
