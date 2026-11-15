use crate::connection::{NotifyFlags, SearchResult};
use crate::error::Error;
use crate::types::response::Removal;
use crate::types::response::{
    EsearchResponse, ResponseCode, TaggedResponse, UntaggedResponse, UntaggedStatus,
};
use crate::types::{CopyResult, ExpungeResult, MoveResult};

use super::super::expand_uid_ranges;
use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for SEARCH and UID SEARCH (RFC 3501 Section6.4.4 / Section6.4.8).
///
/// Accumulates solicited SEARCH and ESEARCH responses. In `finalize`,
/// picks the best match with the same three-pass priority ordering
/// as the old `parse_search_result`:
/// 1. Tag-correlated ESEARCH (highest; unambiguous match)
/// 2. Tagless ESEARCH (servers that omit the correlator)
/// 3. Legacy SEARCH (`IMAP4rev1` fallback)
///
/// ESEARCH UID ranges are expanded into individual IDs. An expansion that
/// would exceed the internal safety limit fails rather than returning a
/// partial [`SearchResult`] (RFC 4731 Section3, RFC 3501 Section6.4.4).
pub(crate) struct SearchConsumer {
    /// Tag-correlated ESEARCH responses (highest priority).
    tag_correlated: Vec<EsearchResponse>,
    /// Tagless ESEARCH responses (second priority).
    tagless_esearch: Vec<EsearchResponse>,
    /// Collected legacy SEARCH responses (lowest priority).
    search_responses: Vec<(Vec<u32>, Option<u64>)>,
    /// Non-SEARCH/ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl SearchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            tag_correlated: Vec::new(),
            tagless_esearch: Vec::new(),
            search_responses: Vec::new(),
            buffered: Vec::new(),
        }
    }

    /// The responses that may reach the event queue once the command has
    /// succeeded: the generic `Either` buffer, in wire order, and nothing else.
    ///
    /// Every SEARCH and ESEARCH this consumer accumulated is dropped, the
    /// chosen one because it is this command's answer and the rest because
    /// they are surplus solicited data: a second tag-correlated ESEARCH, a
    /// tagless ESEARCH when a correlated one won, and every legacy SEARCH when
    /// an ESEARCH won. `classify` makes SEARCH and ESEARCH `OnlySolicited`
    /// inside a search command and `Impossible` anywhere else, so they are
    /// never asynchronous mailbox notifications, and publishing a surplus copy
    /// would emit an event the classifier says cannot exist. This is the same
    /// answer the failure arm of `finalize` gives.
    ///
    /// An ESEARCH correlated to a different tag is not accumulated here;
    /// `on_response` buffers it with the `Either` responses, so it is still
    /// surrendered. In a pipelined batch one correlated to another search of
    /// the batch never gets here: the pipeline router delivers it to the
    /// command its tag names while that command is active, and drops it as
    /// surplus once that command has finalized. What remains is a tag naming
    /// no command that can own it (outside the batch, not yet received, or a
    /// non-search command).
    fn reclassified_extras(self) -> Vec<UntaggedResponse> {
        self.buffered
    }
}

impl Consumer for SearchConsumer {
    type Output = SearchResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 4466 search-correlator: tag-correlated ESEARCH.
            UntaggedResponse::Esearch(e) if e.tag.as_deref() == Some(ctx.command_tag()) => {
                self.tag_correlated.push(e);
            }
            // Tagless ESEARCH. Some servers omit the correlator.
            UntaggedResponse::Esearch(e) if e.tag.is_none() => {
                self.tagless_esearch.push(e);
            }
            UntaggedResponse::Search { uids, mod_seq } => {
                self.search_responses.push((uids, mod_seq));
            }
            // Foreign-tagged ESEARCH and other response types.
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<SearchResult> {
        if let Err(e) = tagged.require_ok() {
            // The generic `Either` buffer is surrendered; the accumulated
            // SEARCH/ESEARCH is dropped. `classify` makes both of those
            // `OnlySolicited` inside SEARCH/UID SEARCH and `Impossible`
            // anywhere else, so they are not asynchronous mailbox
            // notifications and a tagged NO cannot retroactively make them
            // into one. Re-emitting them would publish an event the
            // classifier says cannot exist. Same answer `ListConsumer` gives
            // its marker-less LIST entries for the RFC 5465 Section5.4
            // reason.
            return Finalized::failure(e, self.buffered);
        }

        // Priority: tag-correlated ESEARCH > tagless ESEARCH > legacy
        // SEARCH (RFC 4731 Section3.1, same as the old three-pass ordering).

        // Pass 1: tag-correlated ESEARCH.
        if let Some(esearch) = self.tag_correlated.first() {
            // The expansion may fail, but the response is consumed either
            // way: it is this tag's answer, so it must not be republished as
            // an asynchronous event.
            let output = expand_uid_ranges(&esearch.all).map(|ids| SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
            });
            return Finalized {
                output,
                reclassified_as_events: (*self).reclassified_extras(),
            };
        }

        // Pass 2: tagless ESEARCH.
        if let Some(esearch) = self.tagless_esearch.first() {
            let output = expand_uid_ranges(&esearch.all).map(|ids| SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
            });
            return Finalized {
                output,
                reclassified_as_events: (*self).reclassified_extras(),
            };
        }

        // Pass 3: legacy SEARCH.
        // Any further SEARCH is surplus solicited data and is dropped, like
        // every other surplus SEARCH/ESEARCH (see `reclassified_extras`).
        if let Some((uids, mod_seq)) = self.search_responses.into_iter().next() {
            return Finalized::success(SearchResult { ids: uids, mod_seq }, self.buffered);
        }

        Finalized::failure(
            Error::ProtocolMissing(
                "SEARCH OK but no untagged SEARCH/ESEARCH response \
                 (RFC 3501 Section 6.4.4)"
                    .into(),
            ),
            self.buffered,
        )
    }
}

/// Consumer for SEARCH RETURN and UID SEARCH RETURN (RFC 4731 Section3.2).
///
/// Accumulates the solicited ESEARCH response and returns the full
/// [`EsearchResponse`] with MIN, MAX, COUNT, ALL, and MODSEQ fields.
pub(crate) struct EsearchConsumer {
    /// The first matching ESEARCH response.
    result: Option<EsearchResponse>,
    /// Non-ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl EsearchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for EsearchConsumer {
    type Output = EsearchResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 4731 Section3.1: server MUST return a single ESEARCH response.
            // RFC 4466 search-correlator: accept tag-correlated or tagless
            // ESEARCH. Foreign-tagged ESEARCH belongs to another context.
            // Take the first matching one; extras are reclassified.
            UntaggedResponse::Esearch(e)
                if self.result.is_none()
                    && (e.tag.is_none() || e.tag.as_deref() == Some(ctx.command_tag())) =>
            {
                self.result = Some(e);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<EsearchResponse> {
        let buffered = self.buffered;

        if let Err(e) = tagged.require_ok() {
            // Surrender the generic `Either` buffer, drop the accumulated
            // ESEARCH: it is `OnlySolicited` here and `Impossible` outside a
            // search command, so it has no life as an asynchronous event.
            return Finalized::failure(e, buffered);
        }

        let output = self.result.ok_or_else(|| {
            Error::ProtocolMissing(
                "SEARCH RETURN OK but no ESEARCH response \
                 (RFC 4731 Section 3.1)"
                    .into(),
            )
        });

        Finalized {
            output,
            reclassified_as_events: buffered,
        }
    }
}

/// Consumer for SEARCH RETURN (SAVE) and UID SEARCH RETURN (SAVE)
/// (RFC 5182 Section2).
///
/// The server saves results server-side. RFC 5182 Section2 requires a
/// solicited SEARCH or ESEARCH echo, but some servers (e.g. Dovecot)
/// omit it. Per Postel's law we tolerate the omission. The consumer
/// discards any SEARCH/ESEARCH data and succeeds on tagged OK.
pub(crate) struct SearchSaveConsumer {
    /// Non-SEARCH/ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl SearchSaveConsumer {
    pub(crate) fn new() -> Self {
        Self {
            buffered: Vec::new(),
        }
    }
}

impl Consumer for SearchSaveConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 5182 Section2: the server MUST send a solicited SEARCH or
            // ESEARCH even for SAVE-only requests. We accept and discard
            // the data; the caller only needs tagged OK.
            UntaggedResponse::Search { .. } => {}
            // RFC 4466 search-correlator: only accept tag-correlated or
            // tagless ESEARCH. Foreign-tagged ESEARCH is not solicited.
            UntaggedResponse::Esearch(e)
                if e.tag.is_none() || e.tag.as_deref() == Some(ctx.command_tag()) => {}
            other => self.buffered.push(other),
        }
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        // The SEARCH/ESEARCH data was already discarded at accumulation time
        // (RFC 5182 Section2 SAVE-only), so only the generic `Either` buffer
        // is left, and it is surrendered on both arms.
        Finalized {
            output: tagged.require_ok().map(|_| ()),
            reclassified_as_events: self.buffered,
        }
    }
}

/// Consumer for COPY and UID COPY (RFC 3501 Section6.4.7, RFC 4315 Section3).
///
/// COPY has no solicited untagged responses. The result carries the tagged
/// OK's response code and, separately, the COPYUID (RFC 4315 Section3),
/// taken from the tagged OK or from an untagged `OK [COPYUID ...]` (see
/// `resolve_copyuid`). Any other untagged responses routed here (classified
/// as `Either`) are reclassified as events.
pub(crate) struct CopyConsumer {
    /// All responses routed here. COPY has no solicited untagged
    /// responses, so everything is reclassified as events.
    buffered: Vec<UntaggedResponse>,
    /// The whole untagged `* OK [COPYUID ...]` response, with the length of
    /// `buffered` when it arrived so it can be put back in wire order.
    /// Some servers (e.g. Dovecot) send COPYUID in an untagged OK rather
    /// than in the tagged OK (RFC 4315 Section3).
    ///
    /// The response is kept whole, not narrowed to its `ResponseCode`, so that
    /// whenever it is not consumed as this command's answer it can be
    /// surrendered verbatim like any other untagged OK.
    copyuid: Option<(usize, UntaggedResponse)>,
}

impl CopyConsumer {
    pub(crate) fn new() -> Self {
        Self {
            buffered: Vec::new(),
            copyuid: None,
        }
    }
}

/// `buffered` with the retained untagged COPYUID OK, if any, put back at the
/// position it arrived in. Shared by `CopyConsumer` and `MoveConsumer`, which
/// retain it the same way; for MOVE the mutations are not included.
fn reinsert_copyuid(
    buffered: Vec<UntaggedResponse>,
    retained: Option<(usize, UntaggedResponse)>,
) -> Vec<UntaggedResponse> {
    let mut events = buffered;
    if let Some((pos, resp)) = retained {
        // `buffered` only ever grows, so `pos <= events.len()`.
        events.insert(pos, resp);
    }
    events
}

impl Consumer for CopyConsumer {
    type Output = CopyResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // COPY has no solicited untagged responses (RFC 3501 Section6.4.7).
        // Buffer everything for reclassification as events.
        match resp {
            // RFC 4315 Section3: some servers send COPYUID in an untagged OK.
            resp @ UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: Some(ResponseCode::CopyUid { .. }),
                ..
            } if self.copyuid.is_none() => {
                self.copyuid = Some((self.buffered.len(), resp));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<CopyResult> {
        // RFC 4315 Section3: server SHOULD return COPYUID response code.
        let this = *self;
        match tagged.require_ok() {
            Ok(tagged) => {
                let (code, copy_uid, events) =
                    resolve_copyuid(tagged.code, this.buffered, this.copyuid);
                Finalized::success(CopyResult { code, copy_uid }, events)
            }
            // COPY solicits no untagged response, so everything routed here
            // is an asynchronous notification and survives the failure,
            // including the untagged `OK [COPYUID ...]`, re-emitted verbatim.
            // A COPYUID naming a copy the server then failed is not this
            // command's answer, and it is not this consumer's to drop.
            Err(e) => Finalized::failure(e, reinsert_copyuid(this.buffered, this.copyuid)),
        }
    }
}

/// The success-arm answer shared by `CopyConsumer` and `MoveConsumer`:
/// `(code, copy_uid, events)`.
///
/// `code` is the tagged OK's code when it has one, otherwise the retained
/// untagged `OK [COPYUID ...]`'s code. `copy_uid` is the COPYUID wherever it
/// was: the tagged code when that is COPYUID, otherwise the retained untagged
/// OK. The two are separate because the tagged OK may carry a code of its
/// own - under QRESYNC typically `HIGHESTMODSEQ` (RFC 7162) - and a single
/// slot used to let that code hide the COPYUID, which then survived only as
/// an event.
///
/// The retained untagged OK is consumed exactly when it supplies
/// `copy_uid`, i.e. whenever the tagged code is not itself COPYUID. When it
/// is, the untagged OK was not used as this command's answer and is
/// surrendered verbatim at its wire position among `buffered`, like any
/// other untagged OK.
fn resolve_copyuid(
    tagged_code: Option<ResponseCode>,
    buffered: Vec<UntaggedResponse>,
    retained: Option<(usize, UntaggedResponse)>,
) -> (
    Option<ResponseCode>,
    Option<ResponseCode>,
    Vec<UntaggedResponse>,
) {
    if matches!(tagged_code, Some(ResponseCode::CopyUid { .. })) {
        let copy_uid = tagged_code.clone();
        return (tagged_code, copy_uid, reinsert_copyuid(buffered, retained));
    }
    // `on_response` retains only an untagged OK whose code is COPYUID.
    let untagged_code = match retained {
        Some((_, UntaggedResponse::Status { code, .. })) => code,
        _ => None,
    };
    let code = tagged_code.or_else(|| untagged_code.clone());
    (code, untagged_code, buffered)
}

/// Consumer for MOVE and UID MOVE (RFC 6851 Section3).
///
/// Accumulates EXPUNGE (RFC 3501 Section7.4.1) and VANISHED (RFC 7162
/// Section3.2.10) responses sent by the server before the tagged OK.
/// Returns a [`MoveResult`] with the COPYUID response code and the
/// expunged sequence numbers or UID ranges.
///
/// When QRESYNC is enabled the server sends VANISHED instead of
/// EXPUNGE for a mailbox with persistent mod-sequences (RFC 7162
/// Section3.2.10). The consumer accumulates both forms and reports each as
/// it arrived ([`ExpungeResult`]).
pub(crate) struct MoveConsumer {
    /// `* N EXPUNGE` and `* VANISHED ...` responses, kept verbatim and in
    /// arrival order.
    ///
    /// These are stored as whole responses rather than as pre-flattened
    /// `Vec<u32>` / `Vec<UidRange>` accumulators because the failure arm has
    /// to re-emit them: MOVE CLAIMS them into its typed [`MoveResult`], so
    /// nothing upstream can preserve them on its behalf. Flattening at
    /// accumulation time would have destroyed the `earlier` flag, the
    /// per-response boundaries, and the arrival order between EXPUNGE and
    /// VANISHED - and a reconstruction that guesses any of those
    /// misrepresents what the server sent.
    mutations: Vec<UntaggedResponse>,
    /// Non-EXPUNGE/VANISHED responses for reclassification.
    buffered: Vec<UntaggedResponse>,
    /// The whole untagged `* OK [COPYUID ...]` response, with the length of
    /// `buffered` when it arrived so it can be put back in wire order. RFC
    /// 6851 Section 4.3 asks UIDPLUS servers to send COPYUID for MOVE in an
    /// untagged OK before the expunges, so this is the expected shape, not a
    /// quirk.
    ///
    /// Kept whole rather than narrowed to its `ResponseCode`, as in
    /// `CopyConsumer`, so that whenever it is not consumed as this command's
    /// answer it can be surrendered verbatim like any other untagged OK.
    copyuid: Option<(usize, UntaggedResponse)>,
}

impl MoveConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mutations: Vec::new(),
            buffered: Vec::new(),
            copyuid: None,
        }
    }
}

impl Consumer for MoveConsumer {
    type Output = MoveResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 6851 Section3: EXPUNGE responses for moved messages.
            // RFC 7162 Section3.2.10: VANISHED instead, when QRESYNC is on.
            resp @ (UntaggedResponse::Expunge(_) | UntaggedResponse::Vanished { .. }) => {
                self.mutations.push(resp);
            }
            // RFC 6851 Section4.3 / RFC 4315 Section3: COPYUID in an
            // untagged OK. Only the first is retained; a second one is an
            // ordinary untagged OK and is buffered.
            resp @ UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: Some(ResponseCode::CopyUid { .. }),
                ..
            } if self.copyuid.is_none() => {
                self.copyuid = Some((self.buffered.len(), resp));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<MoveResult> {
        let this = *self;
        match tagged.require_ok() {
            Ok(tagged) => {
                let expunged = fold_mutations(this.mutations);
                let (code, copy_uid, events) =
                    resolve_copyuid(tagged.code, this.buffered, this.copyuid);
                Finalized::success(
                    MoveResult {
                        code,
                        copy_uid,
                        expunged,
                    },
                    events,
                )
            }
            Err(e) => {
                // The sharpest failure arm in the crate. A tagged NO can
                // follow `* 3 EXPUNGE`: the server really did expunge, and
                // MOVE claims those responses into its typed result, so if
                // this arm drops them the evidence of a performed mutation is
                // gone for good and a per-folder modseq cache entry is never
                // cleared. They are `Either`-classified here anyway
                // (`(_, Expunge)` and `(_, Vanished { earlier: false })`), so
                // the generic rule reaches them: surrender both the buffer and
                // the mutations, in that order. They are re-emitted verbatim,
                // not reconstructed, which is why they were never flattened.
                // The untagged `OK [COPYUID ...]` goes back into the buffer
                // at its wire position: a COPYUID naming a move the server
                // then failed is not this command's answer, and it is not
                // this consumer's to drop.
                let mut reclassified = reinsert_copyuid(this.buffered, this.copyuid);
                reclassified.extend(this.mutations);
                Finalized::failure(e, reclassified)
            }
        }
    }
}

/// Consumer for EXPUNGE (RFC 3501 Section6.4.3) and UID EXPUNGE (RFC 4315 Section2).
///
/// Accumulates EXPUNGE sequence numbers and VANISHED UID ranges.
/// When QRESYNC is enabled the server sends VANISHED instead of
/// EXPUNGE (RFC 7162 Section3.2.10).
pub(crate) struct ExpungeConsumer {
    /// `* N EXPUNGE` and `* VANISHED [(EARLIER)] ...` responses, verbatim and
    /// in arrival order. See `MoveConsumer::mutations` for why these are not
    /// flattened at accumulation time; here the `earlier` flag matters even
    /// more, because `classify` admits BOTH `VANISHED` and `VANISHED
    /// (EARLIER)` under `CK::Expunge` and the two mean different things to a
    /// downstream reader.
    mutations: Vec<UntaggedResponse>,
    /// Non-EXPUNGE/VANISHED responses for reclassification.
    buffered: Vec<UntaggedResponse>,
}

impl ExpungeConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mutations: Vec::new(),
            buffered: Vec::new(),
        }
    }

    /// An `ExpungeConsumer` that also yields the tagged OK's response code.
    ///
    /// For the UID MOVE fallback (COPY + STORE + UID EXPUNGE, RFC 6851
    /// Section 3.3): the final UID EXPUNGE's tagged OK plays the part of the
    /// native MOVE's tagged OK, and under QRESYNC it carries the source
    /// mailbox's new `HIGHESTMODSEQ` (RFC 7162), which the plain consumer
    /// discards.
    pub(crate) fn with_tagged_code() -> ExpungeWithCodeConsumer {
        ExpungeWithCodeConsumer(Self::new())
    }
}

/// [`ExpungeConsumer`] plus the tagged OK's response code. Routing and both
/// finalize arms are the inner consumer's, unchanged.
pub(crate) struct ExpungeWithCodeConsumer(ExpungeConsumer);

impl Consumer for ExpungeWithCodeConsumer {
    type Output = (ExpungeResult, Option<ResponseCode>);

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        self.0.on_response(resp, notify_snapshot, ctx);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<(ExpungeResult, Option<ResponseCode>)> {
        let code = tagged.code.clone();
        let inner = Box::new(self.0).finalize(tagged, ctx);
        Finalized {
            output: inner.output.map(|expunged| (expunged, code)),
            reclassified_as_events: inner.reclassified_as_events,
        }
    }
}

/// Fold accumulated EXPUNGE/VANISHED responses into the typed result, each in
/// the form it arrived and in arrival order.
///
/// The form is NOT chosen from the connection's QRESYNC state: a QRESYNC
/// connection still receives EXPUNGE for a `[NOMODSEQ]` mailbox (RFC 7162
/// Section 3.2.10), and choosing by enablement silently dropped those.
fn fold_mutations(mutations: Vec<UntaggedResponse>) -> ExpungeResult {
    let removals = mutations
        .into_iter()
        .filter_map(|resp| match resp {
            UntaggedResponse::Expunge(n) => Some(Removal::Seq(n)),
            UntaggedResponse::Vanished { earlier, uids } => Some(Removal::Uids { earlier, uids }),
            _ => None,
        })
        .collect();
    ExpungeResult { removals }
}

impl Consumer for ExpungeConsumer {
    type Output = ExpungeResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 3501 Section7.4.1: EXPUNGE with sequence numbers.
            // RFC 7162 Section3.2.10: VANISHED with UID ranges when QRESYNC
            // is enabled.
            resp @ (UntaggedResponse::Expunge(_) | UntaggedResponse::Vanished { .. }) => {
                self.mutations.push(resp);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<ExpungeResult> {
        match tagged.require_ok() {
            Ok(_) => Finalized::success(fold_mutations(self.mutations), self.buffered),
            Err(e) => {
                // A failed EXPUNGE may still have expunged something: the
                // server can report `* 1 EXPUNGE` and then fail the command
                // partway. These accumulators are `OnlySolicited` under
                // `CK::Expunge`, so the `Either` rule does not reach them and
                // this is a decision on its own merits - but an EXPUNGE that
                // the client never hears about leaves the message cache and
                // the per-folder modseq permanently ahead of the mailbox,
                // which is worse than publishing an event for a mutation the
                // server did perform. Surrendered verbatim, so the `earlier`
                // flag and the arrival order are intact.
                let mut reclassified = self.buffered;
                reclassified.extend(self.mutations);
                Finalized::failure(e, reclassified)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::types::UidRange;
    use crate::types::response::StatusKind;

    fn removals(removals: Vec<Removal>) -> ExpungeResult {
        ExpungeResult { removals }
    }

    fn ctx() -> ConsumerContext<'static> {
        ConsumerContext {
            session: crate::types::profile::SessionCapabilities::empty(),
            command_target: None,
            command_tag: "A001",
        }
    }

    fn tagged(status: StatusKind, code: Option<ResponseCode>) -> TaggedResponse {
        TaggedResponse {
            tag: "A001".into(),
            status,
            code,
            text: "done".into(),
        }
    }

    fn esearch(tag: Option<&str>, uid: u32) -> UntaggedResponse {
        UntaggedResponse::Esearch(EsearchResponse {
            tag: tag.map(ToOwned::to_owned),
            uid: true,
            all: vec![UidRange::single(uid)],
            ..EsearchResponse::default()
        })
    }

    fn feed(consumer: &mut impl Consumer, resp: UntaggedResponse) {
        consumer.on_response(resp, NotifyFlags::default(), &ctx());
    }

    fn copyuid(dest: u32) -> ResponseCode {
        ResponseCode::CopyUid {
            uid_validity: 7,
            source_uids: vec![UidRange::single(1)],
            dest_uids: vec![UidRange::single(dest)],
        }
    }

    fn untagged_ok(code: Option<ResponseCode>, text: &str) -> UntaggedResponse {
        UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code,
            text: text.into(),
        }
    }

    /// Surplus solicited SEARCH/ESEARCH is dropped on success: a second
    /// correlated ESEARCH, a tagless ESEARCH beside a correlated winner, and
    /// legacy SEARCH beside an ESEARCH winner. Only the `Either` responses
    /// survive, in wire order.
    #[test]
    fn search_success_drops_surplus_search_and_esearch() {
        let mut consumer = SearchConsumer::new();
        feed(&mut consumer, UntaggedResponse::Exists(1));
        feed(&mut consumer, esearch(Some("A001"), 4));
        feed(&mut consumer, esearch(Some("A001"), 9));
        feed(&mut consumer, esearch(None, 11));
        feed(
            &mut consumer,
            UntaggedResponse::Search {
                uids: vec![5],
                mod_seq: None,
            },
        );
        feed(&mut consumer, UntaggedResponse::Expunge(2));

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, None), &ctx());
        assert_eq!(result.output.unwrap().ids, vec![4]);
        assert_eq!(
            result.reclassified_as_events,
            vec![UntaggedResponse::Exists(1), UntaggedResponse::Expunge(2)]
        );
    }

    /// Tagless winner: surplus tagless ESEARCH and legacy SEARCH are dropped.
    #[test]
    fn search_success_drops_surplus_when_tagless_esearch_wins() {
        let mut consumer = SearchConsumer::new();
        feed(&mut consumer, esearch(None, 4));
        feed(&mut consumer, esearch(None, 9));
        feed(
            &mut consumer,
            UntaggedResponse::Search {
                uids: vec![5],
                mod_seq: None,
            },
        );

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, None), &ctx());
        assert_eq!(result.output.unwrap().ids, vec![4]);
        assert!(result.reclassified_as_events.is_empty());
    }

    /// Legacy SEARCH winner: the second SEARCH is dropped.
    #[test]
    fn search_success_drops_surplus_legacy_search() {
        let mut consumer = SearchConsumer::new();
        feed(
            &mut consumer,
            UntaggedResponse::Search {
                uids: vec![3],
                mod_seq: None,
            },
        );
        feed(
            &mut consumer,
            UntaggedResponse::Search {
                uids: vec![8],
                mod_seq: None,
            },
        );

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, None), &ctx());
        assert_eq!(result.output.unwrap().ids, vec![3]);
        assert!(result.reclassified_as_events.is_empty());
    }

    /// A tagged OK without the SEARCH/ESEARCH it owes is an omission by the
    /// server, `ProtocolMissing` (account kind `Protocol(MissingField)`),
    /// not the generic `Protocol` (`Protocol(ContractViolation)`). The same
    /// holds for SEARCH RETURN without its ESEARCH.
    #[test]
    fn search_ok_without_a_result_is_a_missing_response() {
        let result = Box::new(SearchConsumer::new()).finalize(tagged(StatusKind::Ok, None), &ctx());
        assert!(matches!(result.output, Err(Error::ProtocolMissing(_))));

        let result =
            Box::new(EsearchConsumer::new()).finalize(tagged(StatusKind::Ok, None), &ctx());
        assert!(matches!(result.output, Err(Error::ProtocolMissing(_))));
    }

    /// An untagged COPYUID OK that supplies the result is consumed, and the
    /// surrounding `Either` responses survive.
    #[test]
    fn copy_consumes_untagged_copyuid_when_tagged_ok_has_no_code() {
        let mut consumer = CopyConsumer::new();
        feed(&mut consumer, UntaggedResponse::Exists(3));
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "copied"));

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, None), &ctx());
        let output = result.output.unwrap();
        assert_eq!(output.code, Some(copyuid(20)));
        assert_eq!(output.copy_uid, Some(copyuid(20)));
        assert_eq!(
            result.reclassified_as_events,
            vec![UntaggedResponse::Exists(3)]
        );
    }

    /// A tagged COPYUID is the answer, so an untagged COPYUID OK beside it
    /// was not used and is surrendered whole, in wire order.
    #[test]
    fn copy_surrenders_untagged_copyuid_when_tagged_code_is_copyuid() {
        let mut consumer = CopyConsumer::new();
        feed(&mut consumer, UntaggedResponse::Exists(3));
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "copied"));
        feed(&mut consumer, UntaggedResponse::Expunge(1));

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, Some(copyuid(30))), &ctx());
        let output = result.output.unwrap();
        assert_eq!(output.code, Some(copyuid(30)));
        assert_eq!(output.copy_uid, Some(copyuid(30)));
        assert_eq!(
            result.reclassified_as_events,
            vec![
                UntaggedResponse::Exists(3),
                untagged_ok(Some(copyuid(20)), "copied"),
                UntaggedResponse::Expunge(1),
            ]
        );
    }

    /// A tagged OK carrying a code of its own (HIGHESTMODSEQ, as a QRESYNC
    /// server sends) does not hide the untagged COPYUID: the result carries
    /// both, and the untagged OK is consumed as this command's answer rather
    /// than surviving only as an event.
    #[test]
    fn copy_keeps_untagged_copyuid_beside_a_tagged_non_copyuid_code() {
        let mut consumer = CopyConsumer::new();
        feed(&mut consumer, UntaggedResponse::Exists(3));
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "copied"));
        feed(&mut consumer, UntaggedResponse::Expunge(1));

        let result = Box::new(consumer).finalize(
            tagged(StatusKind::Ok, Some(ResponseCode::HighestModSeq(99))),
            &ctx(),
        );
        let output = result.output.unwrap();
        assert_eq!(output.code, Some(ResponseCode::HighestModSeq(99)));
        assert_eq!(output.copy_uid, Some(copyuid(20)));
        assert_eq!(
            result.reclassified_as_events,
            vec![UntaggedResponse::Exists(3), UntaggedResponse::Expunge(1)]
        );
    }

    /// No COPYUID anywhere: `copy_uid` is `None` even though the tagged OK
    /// has a code.
    #[test]
    fn copy_without_any_copyuid_has_none() {
        let result = Box::new(CopyConsumer::new()).finalize(
            tagged(StatusKind::Ok, Some(ResponseCode::HighestModSeq(99))),
            &ctx(),
        );
        let output = result.output.unwrap();
        assert_eq!(output.code, Some(ResponseCode::HighestModSeq(99)));
        assert_eq!(output.copy_uid, None);
    }

    /// On a tagged NO the retained untagged OK survives verbatim, text
    /// included, at its wire position.
    #[test]
    fn copy_failure_surrenders_untagged_copyuid_verbatim() {
        let mut consumer = CopyConsumer::new();
        feed(&mut consumer, UntaggedResponse::Exists(3));
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "copied"));
        feed(&mut consumer, UntaggedResponse::Expunge(1));

        let result = Box::new(consumer).finalize(tagged(StatusKind::No, None), &ctx());
        assert!(result.output.is_err());
        assert_eq!(
            result.reclassified_as_events,
            vec![
                UntaggedResponse::Exists(3),
                untagged_ok(Some(copyuid(20)), "copied"),
                UntaggedResponse::Expunge(1),
            ]
        );
    }

    /// MOVE: an untagged COPYUID OK that supplies the result is consumed; the
    /// `Either` buffer survives and the EXPUNGE is folded into the result.
    #[test]
    fn move_consumes_untagged_copyuid_when_tagged_ok_has_no_code() {
        let mut consumer = MoveConsumer::new();
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "moved"));
        feed(&mut consumer, UntaggedResponse::Expunge(1));
        feed(&mut consumer, UntaggedResponse::Exists(3));

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, None), &ctx());
        let output = result.output.unwrap();
        assert_eq!(output.code, Some(copyuid(20)));
        assert_eq!(output.copy_uid, Some(copyuid(20)));
        assert_eq!(output.expunged, removals(vec![Removal::Seq(1)]));
        assert_eq!(
            result.reclassified_as_events,
            vec![UntaggedResponse::Exists(3)]
        );
    }

    /// MOVE: a tagged COPYUID is the answer, so an untagged COPYUID OK beside
    /// it was not used and is surrendered whole, at its wire position among
    /// the buffered responses.
    #[test]
    fn move_surrenders_untagged_copyuid_when_tagged_code_is_copyuid() {
        let mut consumer = MoveConsumer::new();
        feed(&mut consumer, UntaggedResponse::Exists(3));
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "moved"));
        feed(&mut consumer, UntaggedResponse::Expunge(1));
        feed(&mut consumer, UntaggedResponse::Recent(0));

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, Some(copyuid(30))), &ctx());
        let output = result.output.unwrap();
        assert_eq!(output.code, Some(copyuid(30)));
        assert_eq!(output.copy_uid, Some(copyuid(30)));
        assert_eq!(output.expunged, removals(vec![Removal::Seq(1)]));
        assert_eq!(
            result.reclassified_as_events,
            vec![
                UntaggedResponse::Exists(3),
                untagged_ok(Some(copyuid(20)), "moved"),
                UntaggedResponse::Recent(0),
            ]
        );
    }

    /// MOVE under QRESYNC: the RFC 6851 Section 4.3 untagged COPYUID OK, the
    /// VANISHED, then a tagged OK carrying HIGHESTMODSEQ. The tagged code
    /// used to shadow the COPYUID, which then survived only as an event; the
    /// result now carries both and the untagged OK is consumed.
    #[test]
    fn move_keeps_untagged_copyuid_beside_a_tagged_highestmodseq() {
        let qresync = ["QRESYNC".to_owned()];
        let ctx = ConsumerContext {
            session: &crate::types::profile::SessionCapabilities::new(Vec::new(), qresync.to_vec()),
            command_target: None,
            command_tag: "A001",
        };
        let mut consumer = MoveConsumer::new();
        let vanished = UntaggedResponse::Vanished {
            earlier: false,
            uids: vec![UidRange::single(1)],
        };
        consumer.on_response(
            untagged_ok(Some(copyuid(20)), "moved"),
            NotifyFlags::default(),
            &ctx,
        );
        consumer.on_response(vanished, NotifyFlags::default(), &ctx);
        consumer.on_response(UntaggedResponse::Exists(3), NotifyFlags::default(), &ctx);

        let result = Box::new(consumer).finalize(
            tagged(StatusKind::Ok, Some(ResponseCode::HighestModSeq(99))),
            &ctx,
        );
        let output = result.output.unwrap();
        assert_eq!(output.code, Some(ResponseCode::HighestModSeq(99)));
        assert_eq!(output.copy_uid, Some(copyuid(20)));
        assert_eq!(
            output.expunged,
            removals(vec![Removal::Uids {
                earlier: false,
                uids: vec![UidRange::single(1)],
            }])
        );
        assert_eq!(
            result.reclassified_as_events,
            vec![UntaggedResponse::Exists(3)]
        );
    }

    /// A QRESYNC-enabled connection still receives `* n EXPUNGE` for a
    /// `[NOMODSEQ]` mailbox (RFC 7162 Section 3.2.10), and RFC 6851 Section
    /// 4.4 has a QRESYNC client handle both forms. Every removal is kept, in
    /// the form and order it arrived.
    ///
    /// Choosing the form from the connection's QRESYNC state (the old fold)
    /// fails this: the EXPUNGE is dropped and only the VANISHED survives.
    #[test]
    fn expunge_keeps_every_removal_in_the_form_it_arrived_on_a_qresync_connection() {
        let qresync = ["QRESYNC".to_owned()];
        let ctx = ConsumerContext {
            session: &crate::types::profile::SessionCapabilities::new(Vec::new(), qresync.to_vec()),
            command_target: None,
            command_tag: "A001",
        };
        let mut consumer = ExpungeConsumer::new();
        let vanished = UntaggedResponse::Vanished {
            earlier: false,
            uids: vec![UidRange::single(42)],
        };
        consumer.on_response(UntaggedResponse::Expunge(3), NotifyFlags::default(), &ctx);
        consumer.on_response(vanished, NotifyFlags::default(), &ctx);

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, None), &ctx);
        assert_eq!(
            result.output.unwrap(),
            removals(vec![
                Removal::Seq(3),
                Removal::Uids {
                    earlier: false,
                    uids: vec![UidRange::single(42)],
                },
            ])
        );
    }

    /// MOVE: on a tagged NO the retained untagged OK survives verbatim, text
    /// included, at its position in the buffer, and the mutations follow the
    /// buffer as before.
    #[test]
    fn move_failure_surrenders_untagged_copyuid_verbatim() {
        let mut consumer = MoveConsumer::new();
        feed(&mut consumer, UntaggedResponse::Exists(3));
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "moved"));
        feed(&mut consumer, UntaggedResponse::Expunge(1));
        feed(&mut consumer, UntaggedResponse::Recent(0));

        let result = Box::new(consumer).finalize(tagged(StatusKind::No, None), &ctx());
        assert!(result.output.is_err());
        assert_eq!(
            result.reclassified_as_events,
            vec![
                UntaggedResponse::Exists(3),
                untagged_ok(Some(copyuid(20)), "moved"),
                UntaggedResponse::Recent(0),
                UntaggedResponse::Expunge(1),
            ]
        );
    }

    /// MOVE: only the first untagged COPYUID OK is the candidate answer; a
    /// second one is an ordinary untagged OK and survives on success.
    #[test]
    fn move_second_untagged_copyuid_is_an_ordinary_event() {
        let mut consumer = MoveConsumer::new();
        feed(&mut consumer, untagged_ok(Some(copyuid(20)), "first"));
        feed(&mut consumer, untagged_ok(Some(copyuid(21)), "second"));

        let result = Box::new(consumer).finalize(tagged(StatusKind::Ok, None), &ctx());
        assert_eq!(result.output.unwrap().code, Some(copyuid(20)));
        assert_eq!(
            result.reclassified_as_events,
            vec![untagged_ok(Some(copyuid(21)), "second")]
        );
    }
}
