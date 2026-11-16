use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{TaggedResponse, UntaggedResponse};
use crate::types::validated::MailboxName;
use crate::types::{MailboxAttribute, MailboxInfo, StatusItem, StatusResult};

use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for LIST (RFC 3501 Section6.3.8).
///
/// Accumulates solicited LIST responses and classifies NOTIFY marker-
/// bearing LIST responses (OLDNAME, `\NonExistent`, `\NoAccess`) as
/// events to be re-emitted by the dispatcher (RFC 5465 Section5.4).
///
/// The `notify_snapshot` parameter on each `on_response` call provides
/// the per-response NOTIFY state. After a mid-stream
/// `[NOTIFICATIONOVERFLOW]`, `apply_side_effects` clears the notify
/// flags, so subsequent snapshots have `list = false`. This replaces
/// the manual `first_notification_overflow_index` approach used by the
/// old hand-rolled loop.
///
/// NOTIFY markers are declined at receipt, before tagged success or failure.
pub(crate) struct ListConsumer {
    /// Solicited LIST entries (marker-less, accumulated on success).
    mailboxes: Vec<MailboxInfo>,
}

impl ListConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mailboxes: Vec::new(),
        }
    }
}

impl Consumer for ListConsumer {
    type Output = Vec<MailboxInfo>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: when NOTIFY LIST events are registered,
                // marker-bearing LIST responses are NOTIFY events. The
                // per-response notify_snapshot handles mid-stream overflow
                // (RFC 5465 Section5.8): after overflow, snapshot.list is false.
                if notify_snapshot.list && is_notify_list_event(&info, true) {
                    return Some(UntaggedResponse::List(info));
                } else {
                    self.mailboxes.push(info);
                }
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<MailboxInfo>> {
        let reclassified = Vec::new();

        match tagged.require_ok() {
            Ok(_) => Finalized::success(self.mailboxes, reclassified),
            Err(e) => {
                // On failure the marker-less LIST accumulator - and only it -
                // is dropped: those entries may be the failed command's own
                // output, and must not leak out as though they were
                // unsolicited notifications (RFC 5465 Section5.4).
                Finalized::failure(e, reclassified)
            }
        }
    }
}

/// Consumer for LSUB (RFC 3501 Section6.3.9).
///
/// Simple accumulator. LSUB has no NOTIFY ambiguity (deprecated in
/// `IMAP4rev2`; RFC 9051 Appendix F item 19). All LSUB responses
/// classified as `OnlySolicited` are accumulated; any `Either` responses
/// are reclassified as events.
#[derive(Default)]
pub(crate) struct LsubConsumer {
    mailboxes: Vec<MailboxInfo>,
}

impl Consumer for LsubConsumer {
    type Output = Vec<MailboxInfo>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        match resp {
            UntaggedResponse::Lsub(info) => {
                self.mailboxes.push(info);
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<MailboxInfo>> {
        match tagged.require_ok() {
            Ok(_) => Finalized::success(self.mailboxes, Vec::new()),
            Err(e) => Finalized::failure(e, Vec::new()),
        }
    }
}

/// Consumer for LIST-EXTENDED (RFC 5258 Section3 / RFC 9051 Section6.3.9).
///
/// Like [`ListConsumer`] but additionally filters selection-mismatch
/// NOTIFY events (RFC 5258 Section3: responses that lack the required
/// `\Subscribed`, `\Remote`, or special-use attributes).
///
/// `filter_extended` controls whether `\NonExistent` / `\NoAccess` are
/// treated as NOTIFY markers. When `SUBSCRIBED` is in the selection
/// options, these attributes are legitimate solicited data (RFC 5258 Section3)
/// and must NOT be filtered.
pub(crate) struct ListExtendedConsumer {
    /// Whether to treat `\NonExistent` / `\NoAccess` as NOTIFY markers.
    /// `true` when SUBSCRIBED is NOT in selection options.
    filter_extended: bool,
    /// Selection options for mismatch detection (owned copies).
    selection_options: Vec<crate::types::command_options::ListSelect>,
    /// Solicited LIST entries.
    mailboxes: Vec<MailboxInfo>,
}

impl ListExtendedConsumer {
    pub(crate) fn new(selection_options: Vec<crate::types::command_options::ListSelect>) -> Self {
        // SUBSCRIBED can legitimately return deleted or inaccessible mailboxes.
        // In other requests those attributes identify NOTIFY events. Derive
        // this policy here so the queued and direct paths cannot disagree.
        let filter_extended = !selection_options.iter().any(|option| {
            matches!(
                option,
                crate::types::command_options::ListSelect::Subscribed
            )
        });
        Self {
            filter_extended,
            selection_options,
            mailboxes: Vec::new(),
        }
    }
}

impl Consumer for ListExtendedConsumer {
    type Output = Vec<MailboxInfo>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        match resp {
            UntaggedResponse::List(info) => {
                if notify_snapshot.list {
                    // RFC 5465 Section5.4: check for NOTIFY marker events.
                    if is_notify_list_event(&info, self.filter_extended) {
                        return Some(UntaggedResponse::List(info));
                    }
                    // Selection keeps its typed identity through response routing.
                    if is_notify_selection_mismatch(&info, &self.selection_options) {
                        return Some(UntaggedResponse::List(info));
                    }
                }
                self.mailboxes.push(info);
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<MailboxInfo>> {
        let reclassified = Vec::new();

        match tagged.require_ok() {
            Ok(_) => Finalized::success(self.mailboxes, reclassified),
            Err(e) => {
                // On failure only the marker-less, non-mismatch LIST
                // accumulator is dropped: it may be the failed command's own
                // output and must not leak out as an unsolicited notification
                // (RFC 5465 Section5.4).
                Finalized::failure(e, reclassified)
            }
        }
    }
}

/// Consumer for LIST with STATUS return option (RFC 5819 Section2).
///
/// Correlates interleaved LIST and STATUS responses by mailbox name.
/// Both LIST and STATUS are classified as `OnlySolicited` during
/// LIST-STATUS (see `classify`). NOTIFY marker-bearing LIST entries
/// are identified via `is_notify_list_event` and reclassified.
///
/// On failure: marker-bearing LIST -> reclassified as events; all STATUS
/// and marker-less LIST -> dropped (STATUS is wire-identical to NOTIFY,
/// RFC 5465 Section4 / RFC 5819 Section2).
pub(crate) struct ListStatusConsumer {
    /// Accumulated solicited LIST entries paired with their STATUS data.
    /// STATUS slot is `None` until the correlated STATUS arrives.
    results: Vec<(MailboxInfo, Option<Vec<StatusItem>>)>,
    /// STATUS responses that arrived before their LIST (valid per
    /// RFC 5819: ordering is not mandated).
    pending_status: Vec<(MailboxName, Vec<StatusItem>)>,
}

impl ListStatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            results: Vec::new(),
            pending_status: Vec::new(),
        }
    }
}

impl Consumer for ListStatusConsumer {
    type Output = Vec<crate::types::ListStatusEntry>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: NOTIFY marker detection.
                if notify_snapshot.list && is_notify_list_event(&info, true) {
                    return Some(UntaggedResponse::List(info));
                }
                // Solicited LIST entry, waiting for correlated STATUS.
                self.results.push((info, None));
            }
            UntaggedResponse::MailboxStatus { mailbox, items } => {
                // Pair with the first unpaired LIST for this mailbox address.
                // RFC 5819 provides no occurrence id when a name is listed
                // more than once, so exact-identity duplicates pair in order.
                if let Some((_, status)) = self
                    .results
                    .iter_mut()
                    .find(|(mb, s)| s.is_none() && mb.name.matches_mailbox(&mailbox))
                {
                    *status = Some(items);
                } else {
                    // STATUS arrived before its LIST. Save for second
                    // pass in finalize (valid LIST-STATUS ordering per
                    // RFC 5819).
                    self.pending_status.push((mailbox, items));
                }
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<crate::types::ListStatusEntry>> {
        let reclassified = Vec::new();

        if let Err(e) = tagged.require_ok() {
            // On failure: drop the solicited accumulators - both the
            // marker-less LIST entries and every STATUS, pending or paired.
            // STATUS is wire-identical to a NOTIFY event (RFC 5465 Section4),
            // and a LIST that may be the failed command's own output must not
            // leak out as though it were unsolicited (RFC 5465 Section5.4).
            // Known NOTIFY events have already been declined to the driver.
            return Finalized::failure(e, reclassified);
        }

        // Second pass: pair STATUS that arrived before their LIST.
        let mut results = self.results;
        for (decoded, items) in self.pending_status {
            if let Some((_, status)) = results
                .iter_mut()
                .find(|(mb, s)| s.is_none() && mb.name.matches_mailbox(&decoded))
            {
                *status = Some(items);
            }
            // Orphaned STATUS inside LIST-STATUS is ambiguous:
            // could be malformed solicited output or NOTIFY
            // delivery. Drop rather than manufacturing a fake
            // notification (RFC 5465 Section4; RFC 5819 Section2).
        }

        // Replace None with empty vec for any LIST without a
        // matching STATUS (non-conformant server or STATUS not
        // yet arrived). See RFC 5465 Section5.5 / RFC 5819 Section2 for
        // the ambiguity reasoning.
        let paired: Vec<crate::types::ListStatusEntry> = results
            .into_iter()
            .map(|(mb, status)| {
                let items = status.unwrap_or_default();
                crate::types::ListStatusEntry {
                    mailbox: mb,
                    status: items,
                }
            })
            .collect();

        Finalized::success(paired, reclassified)
    }
}

/// Consumer for STATUS (RFC 3501 Section6.3.10).
///
/// Accumulates same-mailbox STATUS responses (classified as
/// `OnlySolicited` by `classify`). When NOTIFY STATUS is active
/// (RFC 5465 Section4), additional same-mailbox STATUS responses are
/// ambiguous. The protocol provides no marker to distinguish solicited
/// from NOTIFY. These are surfaced in [`StatusResult::ambiguous`] rather
/// than silently reclassified.
///
/// On failure the `matching` accumulator is dropped: same-mailbox STATUS is
/// wire-identical to a NOTIFY event (RFC 5465 Section4, RFC 3501
/// Section6.3.10), so re-emitting it would leak potentially-solicited data
/// into the event channel. Unrelated responses are declined immediately.
pub(crate) struct StatusConsumer {
    /// Same-mailbox STATUS responses with their per-response notify
    /// snapshot flag. The last entry becomes the primary result;
    /// earlier entries with `had_notify == true` become ambiguous.
    matching: Vec<(UntaggedResponse, bool)>,
}

impl StatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            matching: Vec::new(),
        }
    }
}

impl Consumer for StatusConsumer {
    type Output = StatusResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        match resp {
            UntaggedResponse::MailboxStatus { .. } => {
                // Record whether NOTIFY STATUS was active when this
                // response was generated. Used in finalize to classify
                // extras as ambiguous vs unsolicited.
                self.matching.push((resp, notify_snapshot.status));
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<StatusResult> {
        // Same-mailbox STATUS may be this command's partial answer, even with
        // NOTIFY enabled. Drop it on failure; unrelated events were declined.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, Vec::new());
        }

        let mut matching = self.matching;

        // RFC 3501 Section6.3.10: an OK response MUST include an untagged
        // STATUS for the requested mailbox.
        let Some((last_resp, _)) = matching.pop() else {
            let target = ctx
                .command_target()
                .map_or_else(|| "<unknown>".to_owned(), |t| t.as_str().to_owned());
            return Finalized::failure(
                Error::Provider(crate::error::ProviderFailure::Missing(format!(
                    "STATUS OK but no matching untagged STATUS response \
                     for mailbox '{target}' (RFC 3501 Sections 5.2, 6.3.10)"
                ))),
                Vec::new(),
            );
        };

        // Extract the primary items from the last response.
        let UntaggedResponse::MailboxStatus {
            items: primary_items,
            ..
        } = last_resp
        else {
            // A local invariant, checked after the tagged OK completed the
            // exchange, so the framing is intact and `Internal` (not
            // connection-fatal) is safe.
            return Finalized::failure(
                Error::Internal {
                    kind: crate::error::InternalFailure::Invariant(
                        "matching predicate returned non-MailboxStatus variant".into(),
                    ),
                    mid_exchange: None,
                },
                Vec::new(),
            );
        };

        // RFC 5465 Section4 / Section5.8: classify remaining extras.
        // NOTIFY active -> ambiguous. No NOTIFY -> server anomaly,
        // reclassify as unsolicited per RFC 3501 Section5.2.
        let mut ambiguous = Vec::new();
        let mut non_notify_extras: Vec<UntaggedResponse> = Vec::new();
        for (resp, had_notify) in matching {
            if had_notify {
                if let UntaggedResponse::MailboxStatus { items, .. } = resp {
                    ambiguous.push(items);
                }
            } else {
                non_notify_extras.push(resp);
            }
        }

        let mut reclassified = Vec::new();
        reclassified.extend(non_notify_extras);

        Finalized::success(
            StatusResult {
                items: primary_items,
                ambiguous,
            },
            reclassified,
        )
    }
}

/// Check whether a LIST response's attributes conflict with the selection
/// options of a LIST-EXTENDED command (RFC 5258 Section 3).
///
/// When NOTIFY is active, a concurrent create event may match the wildcard
/// but lack the attributes required by the selection options (SUBSCRIBED,
/// REMOTE, SPECIAL-USE). Returns `true` if the response does NOT satisfy
/// the selection criteria and should be treated as a NOTIFY event.
pub(in crate::connection) fn is_notify_selection_mismatch(
    info: &MailboxInfo,
    selection_options: &[crate::types::command_options::ListSelect],
) -> bool {
    let has_recursivematch = selection_options
        .iter()
        .any(|o| matches!(o, crate::types::command_options::ListSelect::RecursiveMatch));

    for opt in selection_options {
        if matches!(opt, crate::types::command_options::ListSelect::Subscribed) {
            let has_subscribed = info
                .attributes
                .iter()
                .any(|a| matches!(a, MailboxAttribute::Subscribed));
            // RFC 5258 Section 3.5: with RECURSIVEMATCH, a mailbox can be
            // returned without \Subscribed if it has children with matching
            // subscriptions (indicated by non-empty CHILDINFO extended data).
            let has_childinfo = has_recursivematch && !info.child_info.is_empty();
            if !has_subscribed && !has_childinfo {
                return true;
            }
        }
        if matches!(opt, crate::types::command_options::ListSelect::Remote)
            && !info
                .attributes
                .iter()
                .any(|a| matches!(a, MailboxAttribute::Remote))
        {
            return true;
        }
        // RFC 6154 Section 3: SPECIAL-USE requests only mailboxes with
        // a special-use attribute.
        if matches!(opt, crate::types::command_options::ListSelect::SpecialUse)
            && !info.attributes.iter().any(MailboxAttribute::is_special_use)
        {
            return true;
        }
    }
    false
}

/// Check whether a LIST response carries markers that identify it as a
/// NOTIFY event rather than a solicited LIST result (RFC 5465 Section 5.4).
///
/// NOTIFY delivers `MailboxName` events (rename, delete, ACL change,
/// subscription change) as LIST responses. Reliable markers:
///
/// - **OLDNAME** (RFC 9051 Section 6.3.9.7): present on rename events.
///   A solicited LIST lists current mailbox state and never includes OLDNAME.
///   Always treated as a NOTIFY marker.
/// - **`\NonExistent`** (RFC 9051 Section 7.2.2): present on delete events.
///   A solicited plain LIST only returns existing mailboxes so `\NonExistent`
///   is a reliable NOTIFY marker in that context. However, LIST-EXTENDED with
///   SUBSCRIBED can legitimately return subscribed-but-deleted mailboxes with
///   `\NonExistent` (RFC 5258 Section 3).
/// - **`\NoAccess`** (RFC 5465 Section 5.9): present when the logged-in
///   user loses the `l` ACL right on a monitored mailbox. A solicited plain
///   LIST typically does not return inaccessible mailboxes. However,
///   LIST-EXTENDED with SUBSCRIBED may return subscribed-but-inaccessible
///   mailboxes with `\NoAccess`.
///
/// Callers must set `filter_extended_markers` to `false` when
/// `\NonExistent` / `\NoAccess` can legitimately appear on solicited
/// results (i.e., LIST-EXTENDED with SUBSCRIBED).
///
/// Create events carry no distinguishing marker and are not filtered.
// SELECT has no SUBSCRIBED selection option, so its callers always filter
// extended markers. LIST-EXTENDED derives this policy from its own options.
pub(in crate::connection) fn is_notify_list_event(
    info: &MailboxInfo,
    filter_extended_markers: bool,
) -> bool {
    // Rename event: OLDNAME extended data item  -  always a NOTIFY marker.
    if info.old_name.is_some() {
        return true;
    }
    // Delete (\NonExistent) and ACL-loss (\NoAccess) events. Only
    // treated as NOTIFY markers when the caller confirms that these
    // attributes cannot appear on solicited results (i.e., plain LIST
    // or LIST-EXTENDED without SUBSCRIBED).
    if filter_extended_markers
        && info.attributes.iter().any(|a| {
            matches!(
                a,
                MailboxAttribute::NonExistent | MailboxAttribute::NoAccess
            )
        })
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::dispatch::ConsumerContext;
    use crate::types::response::{StatusKind, TaggedResponse};

    fn tagged_ok() -> TaggedResponse {
        TaggedResponse {
            tag: "A001".into(),
            status: StatusKind::Ok,
            code: None,
            text: String::new(),
        }
    }

    #[test]
    fn list_status_pairs_by_wire_identity_and_keeps_duplicate_order() {
        let canonical = MailboxName::new("a").expect("valid mailbox");
        let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let mut consumer =
            crate::connection::test_support::record_consumer(ListStatusConsumer::new());
        let context = ConsumerContext {
            session: crate::types::profile::SessionCapabilities::empty(),
            command_target: None,
            command_tag: "A001",
        };

        for name in [noncanonical.clone(), canonical.clone(), canonical.clone()] {
            consumer.on_response(
                UntaggedResponse::List(MailboxInfo {
                    name,
                    ..MailboxInfo::default()
                }),
                NotifyFlags::default(),
                &context,
            );
        }
        for messages in [1, 2] {
            consumer.on_response(
                UntaggedResponse::MailboxStatus {
                    mailbox: canonical.clone(),
                    items: vec![StatusItem::Messages(messages)],
                },
                NotifyFlags::default(),
                &context,
            );
        }

        let finalized = Box::new(consumer).finalize(tagged_ok(), &context);
        let results = finalized.output.expect("LIST-STATUS succeeds");
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].mailbox.name, noncanonical);
        assert!(results[0].status.is_empty());
        assert_eq!(results[1].status, vec![StatusItem::Messages(1)]);
        assert_eq!(results[2].status, vec![StatusItem::Messages(2)]);
    }

    #[test]
    fn list_status_pairs_pending_status_by_wire_identity() {
        let canonical = MailboxName::new("a").expect("valid mailbox");
        let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let mut consumer =
            crate::connection::test_support::record_consumer(ListStatusConsumer::new());
        let context = ConsumerContext {
            session: crate::types::profile::SessionCapabilities::empty(),
            command_target: None,
            command_tag: "A001",
        };

        consumer.on_response(
            UntaggedResponse::MailboxStatus {
                mailbox: canonical.clone(),
                items: vec![StatusItem::Messages(7)],
            },
            NotifyFlags::default(),
            &context,
        );
        for name in [noncanonical.clone(), canonical.clone()] {
            consumer.on_response(
                UntaggedResponse::List(MailboxInfo {
                    name,
                    ..MailboxInfo::default()
                }),
                NotifyFlags::default(),
                &context,
            );
        }

        let finalized = Box::new(consumer).finalize(tagged_ok(), &context);
        let results = finalized.output.expect("LIST-STATUS succeeds");
        assert!(results[0].status.is_empty());
        assert_eq!(results[1].status, vec![StatusItem::Messages(7)]);
    }
}
