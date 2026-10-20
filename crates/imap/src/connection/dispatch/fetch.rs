use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;

use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{TaggedResponse, UntaggedResponse};
use crate::types::validated::ParsedUidSet;
use crate::types::{FetchResponse, StoreResult, UidRange};

use super::{BackpressureState, Consumer, ConsumerContext, Finalized, StreamingConsumer};

/// Default warn-on-large threshold in bytes (10 MB).
///
/// When the estimated accumulated size of buffered `FetchResponse`s
/// exceeds this limit, a `tracing::warn!` is emitted pointing the
/// caller towards `uid_fetch_streaming`.
pub(crate) const DEFAULT_FETCH_WARN_BYTES: usize = 10 * 1024 * 1024;

/// Rough byte-size estimate for a single [`FetchResponse`].
///
/// Sums the data lengths of body sections and binary sections (the
/// dominant contributors to memory), plus a flat overhead per response
/// for the fixed fields and heap-allocated strings.
pub(crate) fn estimate_fetch_response_bytes(fr: &FetchResponse) -> usize {
    // Flat overhead: seq/uid/flags/envelope/bodystructure/dates/ids etc.
    // Conservative estimate: covers the struct itself plus typical
    // small-string heap allocations.
    let mut size: usize = 256;
    for bs in &fr.body_sections {
        size += bs.data.as_ref().map_or(0, Vec::len);
    }
    for bin in &fr.binary_sections {
        size += bin.data.as_ref().map_or(0, Vec::len);
    }
    // Gmail labels are unbounded server-supplied heap strings, so a
    // labels-only FETCH would otherwise contribute only the flat overhead
    // and slip past both `uid_fetch_limited` and the warn threshold.
    if let Some(labels) = &fr.gmail_labels {
        for label in labels {
            size += label.len();
        }
    }
    size
}

/// Consumer for FETCH / UID FETCH (RFC 3501 Section6.4.5, buffering form).
///
/// Accumulates `FETCH` untagged responses into a `Vec<FetchResponse>`.
/// Logs a warning when the accumulated byte estimate exceeds a
/// configurable threshold (default 10 MB) to nudge callers toward the
/// streaming variant (`uid_fetch_streaming`).
pub(crate) struct FetchConsumer {
    fetches: Vec<FetchResponse>,
    /// Non-FETCH responses routed here (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
    /// Running byte-size estimate of accumulated FETCH data.
    estimated_bytes: usize,
    /// Threshold at which to emit a warn-on-large log.
    warn_threshold: usize,
    /// Hard caller-supplied memory budget.
    hard_limit: Option<usize>,
    /// Fetch-limit error captured when the hard limit was first crossed.
    limit_exceeded: Option<Error>,
    /// Whether the warning has already been emitted (log once).
    warned: bool,
}

impl FetchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            fetches: Vec::new(),
            buffered: Vec::new(),
            estimated_bytes: 0,
            warn_threshold: DEFAULT_FETCH_WARN_BYTES,
            hard_limit: None,
            limit_exceeded: None,
            warned: false,
        }
    }

    pub(crate) fn with_limit(limit: usize) -> Self {
        Self {
            hard_limit: Some(limit),
            ..Self::new()
        }
    }
}

impl Consumer for FetchConsumer {
    type Output = Vec<FetchResponse>;

    fn literal_budget(&self) -> Option<usize> {
        self.hard_limit
            .map(|limit| limit.saturating_sub(self.estimated_bytes))
    }

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.4.2: FETCH responses are the solicited data
        // for FETCH/UID FETCH commands.
        if let UntaggedResponse::Fetch(fr) = resp {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(estimate_fetch_response_bytes(&fr));
            if let Some(limit) = self.hard_limit
                && self.estimated_bytes > limit
            {
                self.limit_exceeded.get_or_insert(Error::FetchLimit {
                    estimated: self.estimated_bytes,
                    limit,
                    seq: Some(fr.seq),
                    uid: fr.uid,
                });
                return;
            }
            if !self.warned && self.estimated_bytes > self.warn_threshold {
                tracing::warn!(
                    estimated_bytes = self.estimated_bytes,
                    threshold = self.warn_threshold,
                    "FETCH response buffer exceeds {} MB; consider \
                     uid_fetch_streaming for large result sets",
                    self.warn_threshold / (1024 * 1024),
                );
                self.warned = true;
            }
            self.fetches.push(*fr);
        } else {
            // Non-FETCH responses (EXISTS, EXPUNGE, FLAGS, etc.)
            // classified as Either; reclassify as events.
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<FetchResponse>> {
        // The `Either` buffer is surrendered on every failure path: a tagged
        // NO/BAD is the strongest evidence those responses were asynchronous
        // server data, and the typed event stream is their only carrier.
        //
        // The accumulated `fetches` are NOT re-emitted as events. They are
        // solicited read-only query payload the caller was never going to
        // receive on a failed FETCH, and they can be arbitrarily large
        // (bodies, binary sections); pushing them into the event sink would
        // turn a failed bulk fetch into an event-stream flood without telling
        // any listener anything it did not already know.
        if let Err(err) = tagged.require_ok() {
            return Finalized::failure(err, self.buffered);
        }
        if let Some(error) = self.limit_exceeded {
            return Finalized::failure(error, self.buffered);
        }
        Finalized::success(self.fetches, self.buffered)
    }
}

/// Streaming consumer for FETCH / UID FETCH (RFC 3501 Section6.4.5).
///
/// Instead of buffering all `FETCH` responses into a `Vec`, pushes each
/// one through an `mpsc::UnboundedSender` as it arrives. The dispatcher keeps
/// reading until the tagged OK regardless of whether the receiver is
/// still alive; this keeps the IMAP stream consistent.
///
/// Non-FETCH responses classified as `Either` are buffered and returned
/// in `finalize` for the dispatcher to re-emit as events.
#[cfg(test)]
pub(crate) struct StreamingFetchConsumer {
    tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
    /// Buffer for ambiguous responses the dispatcher routed here but
    /// that finalize will re-emit as events.
    ambiguous_buffer: Vec<UntaggedResponse>,
}

#[cfg(test)]
impl StreamingFetchConsumer {
    pub(crate) fn new(
        tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
    ) -> Self {
        Self {
            tx,
            ambiguous_buffer: Vec::new(),
        }
    }
}

struct BoundedStreamingPipe<T> {
    tx: Option<tokio::sync::mpsc::Sender<Result<T, Error>>>,
    permit: Option<tokio::sync::mpsc::OwnedPermit<Result<T, Error>>>,
    pending: VecDeque<T>,
    drained: bool,
}

impl<T: Send + 'static> BoundedStreamingPipe<T> {
    fn new(tx: tokio::sync::mpsc::Sender<Result<T, Error>>) -> Self {
        Self {
            tx: Some(tx),
            permit: None,
            pending: VecDeque::new(),
            drained: false,
        }
    }

    fn push(&mut self, item: T) {
        if self.drained {
            return;
        }
        if let Some(permit) = self.permit.take() {
            permit.send(Ok(item));
        } else {
            self.pending.push_back(item);
        }
    }

    fn backpressure_state(&self) -> BackpressureState {
        if self.drained {
            BackpressureState::Drained
        } else if self.permit.is_some() {
            BackpressureState::Ready
        } else {
            BackpressureState::NeedsCapacity
        }
    }

    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move {
            if self.drained || self.permit.is_some() {
                return Ok(());
            }

            let Some(tx) = self.tx.as_ref().cloned() else {
                self.drained = true;
                self.pending.clear();
                return Ok(());
            };

            match tx.reserve_owned().await {
                Ok(permit) => {
                    if let Some(item) = self.pending.pop_front() {
                        permit.send(Ok(item));
                    } else {
                        self.permit = Some(permit);
                    }
                    Ok(())
                }
                Err(_) => {
                    self.drained = true;
                    self.tx = None;
                    self.permit = None;
                    self.pending.clear();
                    Ok(())
                }
            }
        })
    }
}

/// Bounded streaming consumer for FETCH / UID FETCH.
///
/// Capacity is pre-reserved by the driver before each read. If the
/// receiver stops polling, the driver stops reading and TCP backpressure
/// reaches the server.
pub(crate) struct BoundedStreamingFetchConsumer {
    pipe: BoundedStreamingPipe<FetchResponse>,
    remaining_literal_bytes: Option<usize>,
    ambiguous_buffer: Vec<UntaggedResponse>,
}

impl BoundedStreamingFetchConsumer {
    pub(crate) fn new(tx: tokio::sync::mpsc::Sender<Result<FetchResponse, Error>>) -> Self {
        // Channel capacity bounds responses in flight, not their octets.
        // Account-side post-decode budgets cannot protect this reader; callers
        // with an octet ceiling must use the budgeted streaming entry point.
        Self::with_budget(tx, None)
    }

    pub(crate) fn with_budget(
        tx: tokio::sync::mpsc::Sender<Result<FetchResponse, Error>>,
        budget: Option<usize>,
    ) -> Self {
        Self {
            pipe: BoundedStreamingPipe::new(tx),
            remaining_literal_bytes: budget,
            ambiguous_buffer: Vec::new(),
        }
    }
}

impl Consumer for BoundedStreamingFetchConsumer {
    type Output = ();

    fn literal_budget(&self) -> Option<usize> {
        self.remaining_literal_bytes
    }

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if let UntaggedResponse::Fetch(fr) = resp {
            if let Some(remaining) = &mut self.remaining_literal_bytes {
                let octets = fr
                    .body_sections
                    .iter()
                    .map(|section| section.data.as_ref().map_or(0, Vec::len))
                    .chain(
                        fr.binary_sections
                            .iter()
                            .map(|section| section.data.as_ref().map_or(0, Vec::len)),
                    )
                    .fold(0usize, usize::saturating_add);
                *remaining = remaining.saturating_sub(octets);
            }
            self.pipe.push(*fr);
        } else {
            self.ambiguous_buffer.push(resp);
        }
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        // Consuming `self` drops the pipe either way, which closes the
        // delivery channel and ends the stream - that is unchanged. Items
        // already delivered stay delivered. Only the ambiguous buffer has a
        // choice to make, and it is surrendered: a failed FETCH is no reason
        // to swallow an EXPUNGE the server volunteered mid-command.
        if let Err(err) = tagged.require_ok() {
            return Finalized::failure(err, self.ambiguous_buffer);
        }
        Finalized::success((), self.ambiguous_buffer)
    }
}

impl StreamingConsumer for BoundedStreamingFetchConsumer {
    fn backpressure_state(&self) -> BackpressureState {
        self.pipe.backpressure_state()
    }

    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        self.pipe.reserve_capacity()
    }
}

#[cfg(test)]
impl Consumer for StreamingFetchConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.4.2: FETCH responses are the solicited data
        // for FETCH/UID FETCH commands.
        if let UntaggedResponse::Fetch(fr) = resp {
            // If the receiver is dropped, discard the response but keep
            // reading until the tagged OK to preserve stream consistency.
            let _ = self.tx.send(Ok(*fr));
        } else {
            // Non-FETCH response routed to us; ambiguous.
            self.ambiguous_buffer.push(resp);
        }
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        // Dropping self.tx by consuming self signals end of stream on both
        // paths. The ambiguous buffer survives a failure for the same reason
        // as the bounded variant.
        if let Err(err) = tagged.require_ok() {
            return Finalized::failure(err, self.ambiguous_buffer);
        }
        Finalized::success((), self.ambiguous_buffer)
    }
}

/// Item yielded by the VANISHED-aware streaming FETCH consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum FetchStreamItem {
    Fetch(FetchResponse),
    VanishedEarlier(Vec<UidRange>),
}

/// Bounded streaming consumer for UID FETCH CHANGEDSINCE VANISHED.
pub(crate) struct BoundedStreamingFetchVanishedConsumer {
    pipe: BoundedStreamingPipe<FetchStreamItem>,
    requested_set: Option<ParsedUidSet>,
    dropped_vanished_count: usize,
    buffered: Vec<UntaggedResponse>,
}

impl BoundedStreamingFetchVanishedConsumer {
    pub(crate) fn new(
        tx: tokio::sync::mpsc::Sender<Result<FetchStreamItem, Error>>,
        requested_set: Option<ParsedUidSet>,
    ) -> Self {
        Self {
            pipe: BoundedStreamingPipe::new(tx),
            requested_set,
            dropped_vanished_count: 0,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for BoundedStreamingFetchVanishedConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Fetch(fr) => self.pipe.push(FetchStreamItem::Fetch(*fr)),
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                let filtered = if let Some(ref set) = self.requested_set {
                    let (filtered, dropped) = set.intersect_uid_ranges(&uids);
                    self.dropped_vanished_count += dropped;
                    filtered
                } else {
                    uids
                };
                if !filtered.is_empty() {
                    self.pipe.push(FetchStreamItem::VanishedEarlier(filtered));
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        if self.dropped_vanished_count > 0 {
            tracing::debug!(
                dropped = self.dropped_vanished_count,
                "filtered out-of-set VANISHED (EARLIER) UIDs per RFC 7162 Section 3.2.6",
            );
        }
        // VANISHED (EARLIER) that already went down the pipe stays delivered;
        // nothing is retained here to re-emit. The generic `Either` buffer is
        // surrendered on failure.
        if let Err(err) = tagged.require_ok() {
            return Finalized::failure(err, self.buffered);
        }
        Finalized::success((), self.buffered)
    }
}

impl StreamingConsumer for BoundedStreamingFetchVanishedConsumer {
    fn backpressure_state(&self) -> BackpressureState {
        self.pipe.backpressure_state()
    }

    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        self.pipe.reserve_capacity()
    }
}

/// Consumer for STORE / UID STORE (RFC 3501 Section6.4.6, RFC 7162 Section3.1.3).
///
/// Accumulates the implicit FETCH responses that non-`.SILENT` STORE
/// operations produce (RFC 3501 Section6.4.6: "the server SHOULD send an
/// untagged FETCH response for each message whose flags were updated").
/// Also captures the tagged response code, which may contain
/// `[MODIFIED ...]` when UNCHANGEDSINCE was used (RFC 7162 Section3.1.3).
///
/// A tagged `NO` means two different things depending on the command, so the
/// consumer is constructed from the command's own `unchanged_since` rather
/// than from a per-call-site choice. Under `UNCHANGEDSINCE` a tagged `NO
/// [MODIFIED ...]` is a legal, partially-successful CONDSTORE result whose
/// per-UID conflict evidence the mutation layer needs, so it is preserved in
/// `StoreResult::status`. Without `UNCHANGEDSINCE` a tagged `NO` is a plain
/// refusal with nothing partial about it, and is surfaced as `Error::No` -
/// callers of the unconditional STORE APIs read `Ok` as "the server accepted
/// this", and at least one of them (`\Deleted` + EXPUNGE) would destroy mail
/// if that stopped being true.
///
/// There is deliberately no argument-less constructor: strictness is a
/// function of the command that was sent, so no construction site is in a
/// position to get it wrong, and none can omit the decision.
pub(crate) struct StoreConsumer {
    fetches: Vec<FetchResponse>,
    /// Non-FETCH responses routed here (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
    /// True when the command carried `UNCHANGEDSINCE` (RFC 7162 Section3.1.3).
    conditional: bool,
}

impl StoreConsumer {
    pub(crate) fn new(unchanged_since: Option<u64>) -> Self {
        Self {
            fetches: Vec::new(),
            buffered: Vec::new(),
            conditional: unchanged_since.is_some(),
        }
    }
}

impl Consumer for StoreConsumer {
    type Output = StoreResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.4.6: STORE returns implicit FETCH responses
        // with updated flags for each message whose flags were changed.
        // `.SILENT` operations suppress these.
        if let UntaggedResponse::Fetch(fr) = resp {
            self.fetches.push(*fr);
        } else {
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<StoreResult> {
        // Failure disposition for STORE, which is the one consumer here whose
        // solicited accumulator is mutation evidence rather than query payload.
        //
        // The implicit FETCHes a non-`.SILENT` STORE produces are the server
        // telling us flags it has ALREADY changed (RFC 3501 Section6.4.6).
        // That is true whether the command then ends OK, NO or BAD - a BAD on
        // a multi-UID STORE does not un-apply the updates the server already
        // reported. Today's code special-cases exactly this evidence for the
        // conditional `NO [MODIFIED]` path and then destroys it on `BAD`,
        // which is the same evidence treated two ways. Ruling: on every error
        // path the accumulated FETCHes are re-emitted as `FetchUpdate` events
        // alongside the generic `Either` buffer, so the flag state reaches the
        // typed event stream even though no `StoreResult` reaches the caller.
        // This is the opposite call from `FetchConsumer`, deliberately: there
        // the accumulator is read-only payload nobody's cache depends on.
        let surrender = |fetches: Vec<FetchResponse>, buffered: Vec<UntaggedResponse>| {
            let mut events: Vec<UntaggedResponse> = fetches
                .into_iter()
                .map(|fr| UntaggedResponse::Fetch(Box::new(fr)))
                .collect();
            events.extend(buffered);
            events
        };

        if matches!(tagged.status, crate::types::response::StatusKind::Bad) {
            // RFC 7162 Section3.1.3 makes `NO [MODIFIED ...]` meaningful; it
            // says nothing that would make a `BAD` anything but a command
            // error, on either path.
            let events = surrender(self.fetches, self.buffered);
            return Finalized::failure(Error::bad_with_code(tagged.text, tagged.code), events);
        }

        if !self.conditional && matches!(tagged.status, crate::types::response::StatusKind::No) {
            // Without UNCHANGEDSINCE a tagged NO is a plain refusal. Callers of
            // the unconditional STORE APIs read Ok as "the server accepted
            // this" and act on it (`\Deleted` followed by EXPUNGE), so the
            // refusal must stay an error rather than becoming a StoreResult.
            let events = surrender(self.fetches, self.buffered);
            return Finalized::failure(Error::no_with_code(tagged.text, tagged.code), events);
        }

        // OK on either path, or the conditional `NO [MODIFIED ...]` partial
        // success, whose per-UID conflict evidence rides in `status` / `code`.
        Finalized::success(
            StoreResult {
                fetches: self.fetches,
                status: tagged.status,
                code: tagged.code,
            },
            self.buffered,
        )
    }
}

/// Consumer for UID FETCH with VANISHED modifier
/// (RFC 7162 Section3.2.6).
///
/// Accumulates both `FETCH` responses and `VANISHED (EARLIER)`
/// responses. Plain `VANISHED` (earlier: false) are unsolicited
/// real-time expunge notifications and are reclassified as events.
///
/// When `requested_set` is `Some`, `VANISHED (EARLIER)` UIDs are
/// defensively filtered to only include UIDs within the requested
/// set. RFC 7162 Section 3.2.6 says the server SHOULD limit these
/// responses, but non-conformant servers (e.g. Stalwart) may return
/// UIDs outside the requested set. When `requested_set` is `None`
/// (because the sequence set contained `$`, an unresolvable search
/// result reference per RFC 5182), filtering is skipped.
pub(crate) struct FetchVanishedConsumer {
    fetches: Vec<FetchResponse>,
    vanished_uids: Vec<UidRange>,
    /// Parsed requested UID set for defensive filtering of
    /// `VANISHED (EARLIER)` responses (RFC 7162 Section 3.2.6).
    /// `None` when the sequence set contains `$` (RFC 5182).
    requested_set: Option<ParsedUidSet>,
    /// Count of individual UIDs dropped by filtering.
    dropped_vanished_count: usize,
    /// Non-solicited responses (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
    /// Running byte-size estimate for the warn-on-large check.
    estimated_bytes: usize,
    warn_threshold: usize,
    warned: bool,
}

impl FetchVanishedConsumer {
    /// Create a new consumer with an optional parsed UID set for
    /// defensive filtering of `VANISHED (EARLIER)` responses.
    ///
    /// Pass `Some(set)` to filter out-of-set UIDs per RFC 7162
    /// Section 3.2.6. Pass `None` when the sequence set contains `$`
    /// (RFC 5182 search result reference) and cannot be parsed.
    pub(crate) fn new(requested_set: Option<ParsedUidSet>) -> Self {
        Self {
            fetches: Vec::new(),
            vanished_uids: Vec::new(),
            requested_set,
            dropped_vanished_count: 0,
            buffered: Vec::new(),
            estimated_bytes: 0,
            warn_threshold: DEFAULT_FETCH_WARN_BYTES,
            warned: false,
        }
    }
}

impl Consumer for FetchVanishedConsumer {
    type Output = (Vec<FetchResponse>, Vec<UidRange>);

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 7162 Section3.2.6: FETCH responses for messages whose
            // flags changed since the given mod-sequence.
            UntaggedResponse::Fetch(fr) => {
                self.estimated_bytes += estimate_fetch_response_bytes(&fr);
                if !self.warned && self.estimated_bytes > self.warn_threshold {
                    tracing::warn!(
                        estimated_bytes = self.estimated_bytes,
                        threshold = self.warn_threshold,
                        "FETCH response buffer exceeds {} MB; consider \
                         uid_fetch_streaming for large result sets",
                        self.warn_threshold / (1024 * 1024),
                    );
                    self.warned = true;
                }
                self.fetches.push(*fr);
            }
            // RFC 7162 Section3.2.6: VANISHED (EARLIER) lists UIDs expunged
            // since the given mod-sequence. Defensively filter to only
            // include UIDs within the requested set; non-conformant
            // servers may return UIDs outside it.
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                if let Some(ref set) = self.requested_set {
                    let (filtered, dropped) = set.intersect_uid_ranges(&uids);
                    self.dropped_vanished_count += dropped;
                    self.vanished_uids.extend(filtered);
                } else {
                    // No parsed set ($ in sequence set); accept all.
                    self.vanished_uids.extend(uids);
                }
            }
            // Plain VANISHED (earlier: false) and other responses are
            // unsolicited; reclassify as events.
            _ => {
                self.buffered.push(resp);
            }
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<(Vec<FetchResponse>, Vec<UidRange>)> {
        if self.dropped_vanished_count > 0 {
            tracing::debug!(
                dropped = self.dropped_vanished_count,
                "filtered out-of-set VANISHED (EARLIER) UIDs per RFC 7162 Section 3.2.6",
            );
        }
        if let Err(err) = tagged.require_ok() {
            // Two accumulators, two rulings. `vanished_uids` is expunge
            // evidence: the server already removed those messages, and a
            // failed tagged response does not bring them back, so it is
            // surrendered as a VANISHED (EARLIER) event - dropping it is
            // exactly the stale-modseq-cache hole this trait change exists to
            // close. `fetches` is read-only query payload and is dropped, for
            // the same reason as in `FetchConsumer`.
            //
            // This one event is a UNION, not a replay: several VANISHED
            // responses were folded into `vanished_uids` at accumulation time,
            // already filtered to the requested set. That is faithful because
            // the only thing a reader takes from it is "these UIDs are gone",
            // which merging preserves exactly - no flag is being guessed, since
            // only `earlier: true` responses ever reach this field. Contrast
            // `ExpungeConsumer`, which had to STOP flattening and keep its
            // responses verbatim, because there both `earlier` values are
            // admissible and a synthesised flag would have been a fabrication.
            let mut events = self.buffered;
            if !self.vanished_uids.is_empty() {
                events.push(UntaggedResponse::Vanished {
                    earlier: true,
                    uids: self.vanished_uids,
                });
            }
            return Finalized::failure(err, events);
        }
        Finalized::success((self.fetches, self.vanished_uids), self.buffered)
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;
    use crate::types::response::{StatusKind, UidRange};

    fn context() -> ConsumerContext<'static> {
        ConsumerContext {
            capabilities: &[],
            enabled: &[],
            command_target: None,
            command_tag: "A1",
        }
    }

    fn tagged(status: StatusKind, code: Option<crate::types::ResponseCode>) -> TaggedResponse {
        TaggedResponse {
            tag: "A1".to_owned(),
            status,
            code,
            text: "store rejected".to_owned(),
        }
    }

    fn modified() -> Option<crate::types::ResponseCode> {
        Some(crate::types::ResponseCode::Modified(vec![
            UidRange::single(3),
        ]))
    }

    #[test]
    fn tagged_no_modified_reaches_the_store_result() {
        let result = Box::new(StoreConsumer::new(Some(42)))
            .finalize(tagged(StatusKind::No, modified()), &context())
            .output
            .expect("tagged NO is a typed STORE result");
        assert_eq!(result.status, StatusKind::No);
        assert!(matches!(
            result.code,
            Some(crate::types::ResponseCode::Modified(_))
        ));
    }

    #[test]
    fn unconditional_store_tagged_no_is_an_error() {
        // Callers of the unconditional STORE APIs read Ok as "accepted" and
        // act on it (\Deleted followed by EXPUNGE). A refusal must not reach
        // them as a StoreResult.
        for code in [None, modified()] {
            let err = match Box::new(StoreConsumer::new(None))
                .finalize(tagged(StatusKind::No, code), &context())
                .output
            {
                Err(err) => err,
                Ok(_) => panic!("unconditional STORE must not report a tagged NO as success"),
            };
            assert!(matches!(err, Error::No { .. }), "unexpected error: {err:?}");
        }
    }

    #[test]
    fn tagged_bad_is_an_error_on_both_paths() {
        for unchanged_since in [None, Some(42)] {
            let err = match Box::new(StoreConsumer::new(unchanged_since))
                .finalize(tagged(StatusKind::Bad, None), &context())
                .output
            {
                Err(err) => err,
                Ok(_) => panic!("tagged BAD is always a command error"),
            };
            assert!(
                matches!(err, Error::Bad { .. }),
                "unexpected error: {err:?}"
            );
        }
    }

    #[test]
    fn conditional_store_tagged_ok_still_reports_ok() {
        let result = Box::new(StoreConsumer::new(Some(42)))
            .finalize(tagged(StatusKind::Ok, None), &context())
            .output
            .expect("tagged OK is a STORE result");
        assert_eq!(result.status, StatusKind::Ok);
    }

    /// The implicit FETCHes a non-`.SILENT` STORE already produced describe
    /// flag changes the server has applied. A tagged BAD arriving afterwards
    /// does not un-apply them, so they must reach the event stream rather
    /// than dying with the consumer.
    #[test]
    fn tagged_bad_surrenders_the_implicit_fetches_as_events() {
        let mut consumer = StoreConsumer::new(None);
        let fetch = FetchResponse {
            seq: 7,
            uid: Some(7),
            ..FetchResponse::default()
        };
        consumer.on_response(
            UntaggedResponse::Fetch(Box::new(fetch)),
            NotifyFlags::default(),
            &context(),
        );
        consumer.on_response(
            UntaggedResponse::Expunge(4),
            NotifyFlags::default(),
            &context(),
        );

        let finalized = Box::new(consumer).finalize(tagged(StatusKind::Bad, None), &context());
        assert!(finalized.output.is_err());
        assert_eq!(finalized.reclassified_as_events.len(), 2);
        assert!(matches!(
            finalized.reclassified_as_events[0],
            UntaggedResponse::Fetch(_)
        ));
        assert!(matches!(
            finalized.reclassified_as_events[1],
            UntaggedResponse::Expunge(4)
        ));
    }
}
