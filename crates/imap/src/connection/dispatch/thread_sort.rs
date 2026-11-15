use crate::connection::{NotifyFlags, SearchResult};
use crate::types::response::{TaggedResponse, ThreadNode, UntaggedResponse};

use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for THREAD and UID THREAD (RFC 5256 Section3).
///
/// Accumulates the single THREAD response.
pub(crate) struct ThreadConsumer<I = crate::types::Uid> {
    addressing: std::marker::PhantomData<I>,
    /// The THREAD response, if received.
    result: Option<Vec<ThreadNode<u32>>>,
    /// Non-THREAD responses routed here.
    buffered: Vec<UntaggedResponse>,
}

impl<I: crate::types::search::SearchAddress> Consumer for ThreadConsumer<I> {
    type Output = Vec<ThreadNode<I>>;

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
    ) -> Finalized<Vec<ThreadNode<I>>> {
        // `buffered` is `Either`-classified data that only the event stream
        // carries, so it is surrendered on both arms.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        // RFC 5256 Section 4: an empty THREAD result (no matching
        // messages) may be represented by the server omitting the
        // untagged THREAD response entirely and sending only tagged OK.
        let threads = self
            .result
            .unwrap_or_default()
            .into_iter()
            .map(ThreadNode::addressed)
            .collect();
        Finalized::success(threads, self.buffered)
    }
}

/// Consumer for SORT and UID SORT (RFC 5256 Section2).
///
/// Accumulates the single SORT response with optional MODSEQ
/// (RFC 7162 Section3.1.6).
pub(crate) struct SortConsumer<I = crate::types::Uid> {
    addressing: std::marker::PhantomData<I>,
    /// The SORT response, if received.
    result: Option<(Vec<u32>, Option<crate::types::ModSeq>)>,
    /// Non-SORT responses routed here.
    buffered: Vec<UntaggedResponse>,
}

impl<I: crate::types::search::SearchAddress> Consumer for SortConsumer<I> {
    type Output = SearchResult<I>;

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
    ) -> Finalized<SearchResult<I>> {
        // `buffered` is `Either`-classified data that only the event stream
        // carries, so it is surrendered on both arms.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        // RFC 5256 Section 4: an empty SORT result (no matching
        // messages) may be represented by the server omitting the
        // untagged SORT response entirely and sending only tagged OK.
        let (ids, mod_seq) = self.result.unwrap_or_default();
        Finalized::success(SearchResult::new(ids, mod_seq), self.buffered)
    }
}

impl<I> Default for SortConsumer<I> {
    fn default() -> Self {
        Self {
            result: None,
            buffered: Vec::new(),
            addressing: std::marker::PhantomData,
        }
    }
}

impl<I> Default for ThreadConsumer<I> {
    fn default() -> Self {
        Self {
            result: None,
            buffered: Vec::new(),
            addressing: std::marker::PhantomData,
        }
    }
}
