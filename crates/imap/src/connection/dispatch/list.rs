use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{TaggedResponse, UntaggedResponse};
use crate::types::validated::MailboxName;
use crate::types::{MailboxInfo, StatusItem, StatusResult};

use super::super::{is_notify_list_event, is_notify_selection_mismatch};
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
/// The command error travels in `Finalized::output`, so NOTIFY marker
/// events are still reclassified when the tagged response is NO/BAD.
pub(crate) struct ListConsumer {
    /// Solicited LIST entries (marker-less, accumulated on success).
    mailboxes: Vec<MailboxInfo>,
    /// NOTIFY marker-bearing LIST entries. Reclassified as events in
    /// `finalize` regardless of tagged status.
    marker_events: Vec<UntaggedResponse>,
    /// Non-LIST responses routed here via `Either` classification.
    buffered: Vec<UntaggedResponse>,
}

impl ListConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mailboxes: Vec::new(),
            marker_events: Vec::new(),
            buffered: Vec::new(),
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
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: when NOTIFY LIST events are registered,
                // marker-bearing LIST responses are NOTIFY events. The
                // per-response notify_snapshot handles mid-stream overflow
                // (RFC 5465 Section5.8): after overflow, snapshot.list is false.
                if notify_snapshot.list && is_notify_list_event(&info, true) {
                    self.marker_events.push(UntaggedResponse::List(info));
                } else {
                    self.mailboxes.push(info);
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<MailboxInfo>> {
        // Marker events are reclassified as events on both success and
        // failure paths, and so is the generic `Either` buffer: nothing in
        // it is LIST output, so it carries no solicited ambiguity.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.buffered);

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
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LsubConsumer {
    type Output = Vec<MailboxInfo>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Lsub(info) => {
                self.mailboxes.push(info);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<MailboxInfo>> {
        match tagged.require_ok() {
            Ok(_) => Finalized::success(self.mailboxes, self.buffered),
            // The generic `Either` buffer holds no LSUB, so it is surrendered.
            // The LSUB accumulator itself is dropped: an LSUB line after a
            // failed LSUB may be that command's own partial output, and the
            // same reasoning as RFC 5465 Section5.4 applies - do not re-emit
            // it as an unsolicited notification.
            Err(e) => Finalized::failure(e, self.buffered),
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
    selection_options: Vec<String>,
    /// Solicited LIST entries.
    mailboxes: Vec<MailboxInfo>,
    /// NOTIFY marker-bearing LIST entries.
    marker_events: Vec<UntaggedResponse>,
    /// Selection-mismatch NOTIFY events (already decoded and pushed
    /// directly to reclassified, not through `buffer_remaining`).
    mismatch_events: Vec<UntaggedResponse>,
    /// Non-LIST responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl ListExtendedConsumer {
    pub(crate) fn new(filter_extended: bool, selection_options: Vec<String>) -> Self {
        Self {
            filter_extended,
            selection_options,
            mailboxes: Vec::new(),
            marker_events: Vec::new(),
            mismatch_events: Vec::new(),
            buffered: Vec::new(),
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
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                if notify_snapshot.list {
                    // RFC 5465 Section5.4: check for NOTIFY marker events.
                    if is_notify_list_event(&info, self.filter_extended) {
                        self.marker_events.push(UntaggedResponse::List(info));
                        return;
                    }
                    // RFC 5258 Section3: check selection-option mismatch. Build
                    // a temporary &[&str] view for the helper function.
                    let opts: Vec<&str> =
                        self.selection_options.iter().map(String::as_str).collect();
                    if is_notify_selection_mismatch(&info, &opts) {
                        self.mismatch_events.push(UntaggedResponse::List(info));
                        return;
                    }
                }
                self.mailboxes.push(info);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<MailboxInfo>> {
        // Marker events and mismatch events are reclassified on both
        // success and failure paths. They are provably NOTIFY events
        // and must not be dropped. The generic `Either` buffer holds no
        // LIST at all and is surrendered on both paths too.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.mismatch_events);
        reclassified.extend(self.buffered);

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
    /// NOTIFY marker-bearing LIST entries.
    marker_events: Vec<UntaggedResponse>,
    /// Non-LIST/non-STATUS responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl ListStatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            results: Vec::new(),
            pending_status: Vec::new(),
            marker_events: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListStatusConsumer {
    type Output = Vec<(MailboxInfo, Vec<StatusItem>)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: NOTIFY marker detection.
                if notify_snapshot.list && is_notify_list_event(&info, true) {
                    self.marker_events.push(UntaggedResponse::List(info));
                    return;
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
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<(MailboxInfo, Vec<StatusItem>)>> {
        // Marker events are reclassified regardless of success/failure, and
        // so is the generic `Either` buffer - it holds neither LIST nor
        // STATUS, so none of it can be this command's own output.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.buffered);

        if let Err(e) = tagged.require_ok() {
            // On failure: drop the solicited accumulators - both the
            // marker-less LIST entries and every STATUS, pending or paired.
            // STATUS is wire-identical to a NOTIFY event (RFC 5465 Section4),
            // and a LIST that may be the failed command's own output must not
            // leak out as though it were unsolicited (RFC 5465 Section5.4).
            // Only NOTIFY marker events and the generic buffer survive.
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
        let paired: Vec<(MailboxInfo, Vec<StatusItem>)> = results
            .into_iter()
            .map(|(mb, status)| {
                let items = status.unwrap_or_default();
                (mb, items)
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
/// into the event channel. The separate `buffered` field never holds STATUS
/// for this mailbox, so it is surrendered instead.
pub(crate) struct StatusConsumer {
    /// Same-mailbox STATUS responses with their per-response notify
    /// snapshot flag. The last entry becomes the primary result;
    /// earlier entries with `had_notify == true` become ambiguous.
    matching: Vec<(UntaggedResponse, bool)>,
    /// Non-STATUS responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl StatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            matching: Vec::new(),
            buffered: Vec::new(),
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
    ) {
        match resp {
            UntaggedResponse::MailboxStatus { .. } => {
                // Record whether NOTIFY STATUS was active when this
                // response was generated. Used in finalize to classify
                // extras as ambiguous vs unsolicited.
                self.matching.push((resp, notify_snapshot.status));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<StatusResult> {
        // On failure the `matching` accumulator is dropped: leaking
        // potentially-solicited STATUS as an unsolicited event is worse than
        // losing it (RFC 5465 Section4). That reasoning is about STATUS, and
        // `on_response` puts every `MailboxStatus` in `matching`, so the
        // `Either` buffer provably contains none of it - it holds things like
        // `* OK [ALERT]`, EXISTS or EXPUNGE, whose only carrier is the event
        // stream. It is surrendered.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }

        let mut matching = self.matching;

        // RFC 3501 Section6.3.10: an OK response MUST include an untagged
        // STATUS for the requested mailbox.
        let Some((last_resp, _)) = matching.pop() else {
            let target = ctx
                .command_target()
                .map_or_else(|| "<unknown>".to_owned(), |t| t.as_str().to_owned());
            // Stays a real `Error` in `output`, which the driver reads through
            // `is_connection_fatal` before publishing the result; an omission
            // raised after the tagged OK leaves the connection usable. Nothing
            // solicited exists to drop, so the buffer is surrendered.
            return Finalized::failure(
                Error::ProtocolMissing(format!(
                    "STATUS OK but no matching untagged STATUS response \
                     for mailbox '{target}' (RFC 3501 Sections 5.2, 6.3.10)"
                )),
                self.buffered,
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
                Error::Internal("matching predicate returned non-MailboxStatus variant".into()),
                self.buffered,
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

        let mut reclassified = self.buffered;
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
        let mut consumer = ListStatusConsumer::new();
        let context = ConsumerContext {
            capabilities: &[],
            enabled: &[],
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
        assert_eq!(results[0].0.name, noncanonical);
        assert!(results[0].1.is_empty());
        assert_eq!(results[1].1, vec![StatusItem::Messages(1)]);
        assert_eq!(results[2].1, vec![StatusItem::Messages(2)]);
    }

    #[test]
    fn list_status_pairs_pending_status_by_wire_identity() {
        let canonical = MailboxName::new("a").expect("valid mailbox");
        let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let mut consumer = ListStatusConsumer::new();
        let context = ConsumerContext {
            capabilities: &[],
            enabled: &[],
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
        assert!(results[0].1.is_empty());
        assert_eq!(results[1].1, vec![StatusItem::Messages(7)]);
    }
}
