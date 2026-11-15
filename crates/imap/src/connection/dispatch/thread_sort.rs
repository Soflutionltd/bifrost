use crate::connection::{NotifyFlags, SearchResult};
use crate::types::response::{TaggedResponse, ThreadNode, UntaggedResponse};

use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for THREAD and UID THREAD (RFC 5256 Section3).
///
/// Accumulates the single THREAD response.
#[derive(Default)]
pub(crate) struct ThreadConsumer {
    /// The THREAD response, if received.
    result: Option<Vec<ThreadNode>>,
    /// Non-THREAD responses routed here.
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for ThreadConsumer {
    type Output = Vec<ThreadNode>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Thread(threads) if self.result.is_none() => {
                self.result = Some(threads);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<ThreadNode>> {
        // `buffered` is `Either`-classified data that only the event stream
        // carries, so it is surrendered on both arms.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        // RFC 5256 Section 4: an empty THREAD result (no matching
        // messages) may be represented by the server omitting the
        // untagged THREAD response entirely and sending only tagged OK.
        let threads = self.result.unwrap_or_default();
        Finalized::success(threads, self.buffered)
    }
}

/// Consumer for SORT and UID SORT (RFC 5256 Section2).
///
/// Accumulates the single SORT response with optional MODSEQ
/// (RFC 7162 Section3.1.6).
#[derive(Default)]
pub(crate) struct SortConsumer {
    /// The SORT response, if received.
    result: Option<(Vec<u32>, Option<crate::types::ModSeq>)>,
    /// Non-SORT responses routed here.
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for SortConsumer {
    type Output = SearchResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Sort { nums, mod_seq } if self.result.is_none() => {
                self.result = Some((nums, mod_seq));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<SearchResult> {
        // `buffered` is `Either`-classified data that only the event stream
        // carries, so it is surrendered on both arms.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        // RFC 5256 Section 4: an empty SORT result (no matching
        // messages) may be represented by the server omitting the
        // untagged SORT response entirely and sending only tagged OK.
        let (ids, mod_seq) = self.result.unwrap_or_default();
        Finalized::success(SearchResult { ids, mod_seq }, self.buffered)
    }
}
