#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::UidRange;
use crate::types::response::{EsearchResponse, NamespaceDescriptor, StatusKind, TaggedResponse};

// ---------------------------------------------------------------------------
// Helper  -  tagged OK with no response code
// ---------------------------------------------------------------------------

fn tagged_ok() -> TaggedResponse {
    TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::Ok,
        code: None,
        text: "Completed".into(),
    }
}

fn tagged_no() -> TaggedResponse {
    TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::No,
        code: None,
        text: "Rejected".into(),
    }
}

fn tagged_bad() -> TaggedResponse {
    TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::Bad,
        code: None,
        text: "Bad command".into(),
    }
}

fn default_ctx() -> ConsumerContext<'static> {
    ConsumerContext {
        capabilities: &[],
        enabled: &[],
        command_target: None,
        command_tag: "A001",
    }
}

#[tokio::test]
async fn streaming_fetch_consumer_does_not_drop_slow_receiver_backlog() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut consumer = StreamingFetchConsumer::new(tx);
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    for seq in 1..=300 {
        consumer.on_response(
            UntaggedResponse::Fetch(Box::new(FetchResponse {
                seq,
                uid: Some(seq + 10_000),
                ..Default::default()
            })),
            notify,
            &ctx,
        );
    }

    let finalized = Box::new(consumer).finalize(tagged_ok(), &ctx);
    assert!(finalized.output.is_ok());
    assert!(finalized.reclassified_as_events.is_empty());

    let mut received = Vec::new();
    while let Some(fetch) = rx.recv().await {
        received.push(fetch.unwrap());
    }

    assert_eq!(received.len(), 300);
    assert_eq!(received.first().map(|fetch| fetch.seq), Some(1));
    assert_eq!(received.last().map(|fetch| fetch.seq), Some(300));
}

/// Gmail labels are heap strings the server controls, so they must be part of
/// the byte estimate that drives `uid_fetch_limited` and the buffered-fetch
/// warning. A labels-only FETCH would otherwise report the flat overhead.
#[test]
fn fetch_byte_estimate_counts_gmail_labels() {
    let bare = FetchResponse {
        seq: 1,
        ..Default::default()
    };
    let labelled = FetchResponse {
        seq: 1,
        gmail_labels: Some(vec!["x".repeat(4096), "y".repeat(2048)]),
        ..Default::default()
    };
    assert_eq!(
        fetch::estimate_fetch_response_bytes(&labelled),
        fetch::estimate_fetch_response_bytes(&bare) + 4096 + 2048
    );
}

/// A tagged NO still fails the command after partial FETCH data, AND the
/// `Either` response that arrived before it survives the failure. `* N EXISTS`
/// is routed to the in-flight consumer as ambiguous data, and `ProtocolState`
/// models none of EXISTS / EXPUNGE / VANISHED, so `reclassified_as_events` is
/// the only way out. The solicited FETCH payload is deliberately NOT surrendered
/// (see `FetchConsumer::finalize`): it is read-only query data the caller was
/// never going to get, and re-emitting it would flood the event sink.
#[test]
fn fetch_consumer_propagates_terminal_no_and_surrenders_buffered_events() {
    let mut consumer = FetchConsumer::new();
    let ctx = default_ctx();

    consumer.on_response(UntaggedResponse::Exists(12), NotifyFlags::default(), &ctx);
    consumer.on_response(
        UntaggedResponse::Fetch(Box::new(FetchResponse {
            seq: 1,
            uid: Some(1001),
            ..Default::default()
        })),
        NotifyFlags::default(),
        &ctx,
    );

    let finalized = Box::new(consumer).finalize(tagged_no(), &ctx);
    let err = match finalized.output {
        Err(err) => err,
        Ok(_) => panic!("terminal NO must fail FETCH"),
    };
    assert!(matches!(err, Error::No { text, .. } if text == "Rejected"));
    assert_eq!(
        finalized.reclassified_as_events,
        vec![UntaggedResponse::Exists(12)],
        "the ambiguous EXISTS must be surrendered exactly once, and the \
         solicited FETCH payload must not join it"
    );
}

/// Three requirements at once: the tagged NO fails the command, the delivery
/// channel still closes (dropping the consumer drops the sender on every path),
/// and the pre-failure `Either` response is surrendered.
#[tokio::test]
async fn streaming_fetch_consumer_propagates_terminal_no_and_closes_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut consumer = StreamingFetchConsumer::new(tx);
    let ctx = default_ctx();

    consumer.on_response(UntaggedResponse::Exists(12), NotifyFlags::default(), &ctx);
    consumer.on_response(
        UntaggedResponse::Fetch(Box::new(FetchResponse {
            seq: 1,
            uid: Some(1001),
            ..Default::default()
        })),
        NotifyFlags::default(),
        &ctx,
    );

    let finalized = Box::new(consumer).finalize(tagged_no(), &ctx);
    let err = match finalized.output {
        Err(err) => err,
        Ok(_) => panic!("terminal NO must fail streaming FETCH"),
    };
    assert!(matches!(err, Error::No { text, .. } if text == "Rejected"));
    assert_eq!(
        finalized.reclassified_as_events,
        vec![UntaggedResponse::Exists(12)],
        "a failed streaming FETCH must not swallow an EXISTS the server volunteered"
    );

    let first = rx.recv().await.expect("first FETCH should be delivered");
    assert_eq!(first.unwrap().seq, 1);
    assert!(rx.recv().await.is_none());
}

/// A tagged BAD fails EXPUNGE, and BOTH accumulators survive it. Unlike FETCH,
/// `ExpungeConsumer` surrenders its solicited `mutations` too (see its
/// `finalize`): the server may have expunged before failing, and an EXPUNGE the
/// client never hears about leaves the per-folder modseq permanently ahead of
/// the mailbox. Verbatim and in order - `buffered` first, then `mutations`.
#[test]
fn expunge_consumer_propagates_terminal_bad_and_surrenders_both_buffers() {
    let mut consumer = ExpungeConsumer::new();
    let ctx = default_ctx();

    consumer.on_response(UntaggedResponse::Exists(12), NotifyFlags::default(), &ctx);
    consumer.on_response(UntaggedResponse::Expunge(3), NotifyFlags::default(), &ctx);

    let finalized = Box::new(consumer).finalize(tagged_bad(), &ctx);
    let err = match finalized.output {
        Err(err) => err,
        Ok(_) => panic!("terminal BAD must fail EXPUNGE"),
    };
    assert!(matches!(err, Error::Bad { text, .. } if text == "Bad command"));
    assert_eq!(
        finalized.reclassified_as_events,
        vec![UntaggedResponse::Exists(12), UntaggedResponse::Expunge(3)],
        "a failed EXPUNGE must surrender the ambiguous buffer and the \
         expunges the server already performed, exactly once each"
    );
}

// ---------------------------------------------------------------------------
// SortConsumer  -  empty result tolerance (RFC 5256 Section 4)
// ---------------------------------------------------------------------------

#[test]
fn sort_consumer_empty_result_returns_ok() {
    // When the server sends only a tagged OK with no `* SORT` untagged
    // response (permitted by RFC 5256 when no messages match),
    // `SortConsumer::finalize` should return an empty SearchResult.
    let consumer = Box::new(SortConsumer::default());
    let result = consumer.finalize(tagged_ok(), &default_ctx());
    let sorted = result.output.expect("an empty SORT result is not an error");
    assert!(sorted.ids.is_empty());
    assert_eq!(sorted.mod_seq, None);
    assert!(result.reclassified_as_events.is_empty());
}

fn esearch_all(tag: Option<&str>, all: Vec<UidRange>) -> UntaggedResponse {
    UntaggedResponse::Esearch(EsearchResponse {
        tag: tag.map(ToOwned::to_owned),
        uid: true,
        all,
        ..EsearchResponse::default()
    })
}

/// A solicited ESEARCH answers this tag. A failed expansion changes what the
/// command can return, not who the response belongs to: it must NOT be
/// republished on the asynchronous event queue, and the genuine events must
/// keep their wire order.
#[test]
fn search_consumer_keeps_the_solicited_reply_when_uid_expansion_is_incomplete() {
    let mut consumer = SearchConsumer::new();
    let ctx = default_ctx();
    consumer.on_response(UntaggedResponse::Exists(1), NotifyFlags::default(), &ctx);
    consumer.on_response(
        esearch_all(Some("A001"), vec![UidRange::range(1, 3_000_000)]),
        NotifyFlags::default(),
        &ctx,
    );
    consumer.on_response(UntaggedResponse::Expunge(7), NotifyFlags::default(), &ctx);

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx);
    assert!(matches!(
        result.output,
        Err(Error::SearchResultTruncated {
            returned: 1_000_000,
            omitted: Some(2_000_000),
        })
    ));
    assert_eq!(
        result.reclassified_as_events,
        vec![UntaggedResponse::Exists(1), UntaggedResponse::Expunge(7)],
        "only unsolicited responses may become events, in wire order"
    );
}

/// The same partitioning on the success path: the chosen ESEARCH is consumed,
/// and a second one is surplus and DROPPED, as on the failure path. An ESEARCH
/// is never an asynchronous notification, so it must not become an event.
#[test]
fn search_consumer_drops_the_surplus_esearch_on_success() {
    let mut consumer = SearchConsumer::new();
    let ctx = default_ctx();
    consumer.on_response(UntaggedResponse::Exists(1), NotifyFlags::default(), &ctx);
    consumer.on_response(
        esearch_all(Some("A001"), vec![UidRange::single(4)]),
        NotifyFlags::default(),
        &ctx,
    );
    consumer.on_response(
        esearch_all(Some("A001"), vec![UidRange::single(9)]),
        NotifyFlags::default(),
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx);
    assert_eq!(result.output.unwrap().ids, vec![4]);
    assert_eq!(
        result.reclassified_as_events,
        vec![UntaggedResponse::Exists(1)],
        "only the unsolicited EXISTS may become an event"
    );
}

/// `*` is special wherever it appears in a sequence-set, not only as a range
/// endpoint, and a set may legally repeat itself.
#[test]
fn search_consumer_rejects_a_bare_star_and_counts_overlap_once() {
    let ctx = default_ctx();

    let mut consumer = SearchConsumer::new();
    consumer.on_response(
        // `ALL *` is kept distinct from the numeric UID maximum.
        esearch_all(
            Some("A001"),
            vec![UidRange::from_sequence_bounds(
                crate::types::SeqBound::Star,
                None,
            )],
        ),
        NotifyFlags::default(),
        &ctx,
    );
    assert!(
        matches!(
            Box::new(consumer).finalize(tagged_ok(), &ctx).output,
            Err(Error::SearchResultTruncated { omitted: None, .. })
        ),
        "a standalone `*` must not expand to the literal id 4294967295"
    );

    let mut consumer = SearchConsumer::new();
    consumer.on_response(
        esearch_all(
            Some("A001"),
            vec![UidRange::range(1, 600_000), UidRange::range(1, 600_000)],
        ),
        NotifyFlags::default(),
        &ctx,
    );
    let ids = Box::new(consumer)
        .finalize(tagged_ok(), &ctx)
        .output
        .expect("an overlapping set of 600000 UIDs is under the cap");
    assert_eq!(ids.ids.len(), 600_000);
    assert_eq!(ids.ids[0], 1);
    assert_eq!(ids.ids[599_999], 600_000);
}

// ---------------------------------------------------------------------------
// ThreadConsumer  -  empty result tolerance (RFC 5256 Section 4)
// ---------------------------------------------------------------------------

#[test]
fn thread_consumer_empty_result_returns_ok() {
    // Same as SortConsumer  -  when no messages match the THREAD criteria
    // the server may omit the untagged THREAD response entirely.
    let consumer = Box::new(ThreadConsumer::default());
    let result = consumer.finalize(tagged_ok(), &default_ctx());
    assert!(
        result
            .output
            .expect("an empty THREAD result is not an error")
            .is_empty()
    );
    assert!(result.reclassified_as_events.is_empty());
}

// ---------------------------------------------------------------------------
// IdConsumer  -  solicited response NOT leaked as event
// ---------------------------------------------------------------------------

#[test]
fn id_consumer_solicited_response_not_leaked() {
    let mut consumer = IdConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    // Feed a solicited `* ID` response.
    consumer.on_response(
        UntaggedResponse::Id(vec![
            ("name".into(), Some("Dovecot".into())),
            ("version".into(), Some("2.3".into())),
        ]),
        notify,
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx);
    let pairs = result.output.expect("tagged OK must yield the ID pairs");

    // The ID pairs must be captured.
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].0, "name");
    assert_eq!(pairs[0].1.as_deref(), Some("Dovecot"));
    assert_eq!(pairs[1].0, "version");
    assert_eq!(pairs[1].1.as_deref(), Some("2.3"));

    // The solicited `* ID` must NOT leak into reclassified_as_events.
    assert!(
        result.reclassified_as_events.is_empty(),
        "solicited ID response must not be reclassified as event"
    );
}

// ---------------------------------------------------------------------------
// AppendConsumer / MultiAppendConsumer  -  the untagged OK is kept whole
// ---------------------------------------------------------------------------

fn untagged_appenduid_ok(uid_validity: u32, uids: Vec<UidRange>) -> UntaggedResponse {
    UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: Some(ResponseCode::AppendUid { uid_validity, uids }),
        text: "APPEND completed".into(),
    }
}

fn tagged_ok_with(code: ResponseCode) -> TaggedResponse {
    TaggedResponse {
        code: Some(code),
        ..tagged_ok()
    }
}

/// An untagged `OK [APPENDUID]` is the answer when the tagged OK carries no
/// code, and being the answer it is consumed, not also published as an event.
#[test]
fn append_consumer_uses_an_untagged_appenduid_as_the_answer() {
    let mut consumer = AppendConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();
    consumer.on_response(UntaggedResponse::Exists(3), notify, &ctx);
    consumer.on_response(
        untagged_appenduid_ok(4242, vec![UidRange::single(12)]),
        notify,
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx);
    assert_eq!(result.output.expect("tagged OK"), Some((4242, 12)));
    assert_eq!(
        result.reclassified_as_events,
        vec![UntaggedResponse::Exists(3)],
        "the consumed APPENDUID OK must not be republished; the EXISTS must be"
    );
}

/// The tagged code wins, and the unused untagged OK is surrendered whole
/// rather than silently dropped.
#[test]
fn append_consumer_surrenders_an_untagged_appenduid_the_tagged_code_outranks() {
    let mut consumer = AppendConsumer::default();
    let ctx = default_ctx();
    let untagged = untagged_appenduid_ok(1, vec![UidRange::single(1)]);
    consumer.on_response(untagged.clone(), NotifyFlags::default(), &ctx);

    let result = Box::new(consumer).finalize(
        tagged_ok_with(ResponseCode::AppendUid {
            uid_validity: 9,
            uids: vec![UidRange::single(7)],
        }),
        &ctx,
    );
    assert_eq!(result.output.expect("tagged OK"), Some((9, 7)));
    assert_eq!(result.reclassified_as_events, vec![untagged]);
}

/// A failed APPEND must hand back the untagged OK it saw, whole: it can no
/// longer be an answer, and narrowing it to its code at `on_response` time is
/// what used to make it unrecoverable.
#[test]
fn append_consumer_surrenders_the_whole_untagged_ok_on_failure() {
    let mut consumer = AppendConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();
    let untagged = untagged_appenduid_ok(4242, vec![UidRange::single(12)]);
    consumer.on_response(UntaggedResponse::Exists(3), notify, &ctx);
    consumer.on_response(untagged.clone(), notify, &ctx);

    let result = Box::new(consumer).finalize(tagged_no(), &ctx);
    assert!(result.output.is_err());
    assert_eq!(
        result.reclassified_as_events,
        vec![UntaggedResponse::Exists(3), untagged],
        "arrival order is preserved and the OK keeps its text"
    );
}

#[test]
fn multi_append_consumer_expands_an_untagged_appenduid_and_surrenders_on_failure() {
    let ctx = default_ctx();
    let notify = NotifyFlags::default();
    let untagged = untagged_appenduid_ok(5, vec![UidRange::range(10, 12), UidRange::single(20)]);

    let mut consumer = MultiAppendConsumer::new(4);
    consumer.on_response(untagged.clone(), notify, &ctx);
    let result = Box::new(consumer).finalize(tagged_ok(), &ctx);
    assert_eq!(
        result.output.expect("tagged OK"),
        vec![(5, 10), (5, 11), (5, 12), (5, 20)]
    );
    assert!(
        result.reclassified_as_events.is_empty(),
        "the consumed untagged OK is the answer, not an event"
    );

    let mut consumer = MultiAppendConsumer::new(4);
    consumer.on_response(untagged.clone(), notify, &ctx);
    let result = Box::new(consumer).finalize(tagged_bad(), &ctx);
    assert!(result.output.is_err());
    assert_eq!(result.reclassified_as_events, vec![untagged]);
}

fn multi_append_answer(submitted: usize, uids: Vec<UidRange>) -> Vec<(u32, u32)> {
    let consumer = MultiAppendConsumer::new(submitted);
    let tagged = tagged_ok_with(ResponseCode::AppendUid {
        uid_validity: 7,
        uids,
    });
    Box::new(consumer)
        .finalize(tagged, &default_ctx())
        .output
        .expect("tagged OK")
}

/// An APPENDUID naming far more UIDs than messages were submitted is refused
/// by counting, before anything is expanded: the whole `u32` range for a
/// two-message MULTIAPPEND yields no pairs and allocates none.
///
/// Reverting `appenduid_pairs` to expand first and check afterwards makes this
/// test allocate four billion pairs; dropping the check returns them.
#[test]
fn multi_append_consumer_refuses_an_appenduid_whose_count_disagrees_without_expanding_it() {
    assert!(
        multi_append_answer(2, vec![UidRange::range(1, u32::MAX)]).is_empty(),
        "a uid-set larger than the submission is not an answer"
    );
    assert!(
        multi_append_answer(3, vec![UidRange::range(10, 11)]).is_empty(),
        "a uid-set smaller than the submission is not an answer either"
    );
}

/// RFC 3501 Section 9: `12:10` names the same UIDs as `10:12`. A backwards
/// range used to expand to nothing, silently dropping its UIDs.
#[test]
fn multi_append_consumer_expands_a_backwards_range() {
    assert_eq!(
        multi_append_answer(3, vec![UidRange::range(12, 10)]),
        vec![(7, 10), (7, 11), (7, 12)]
    );
}

// ---------------------------------------------------------------------------
// NamespaceConsumer  -  solicited response NOT leaked as event
// ---------------------------------------------------------------------------

#[test]
fn namespace_consumer_solicited_response_not_leaked() {
    let mut consumer = NamespaceConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    // Feed a solicited `* NAMESPACE` response.
    consumer.on_response(
        UntaggedResponse::Namespace {
            personal: vec![NamespaceDescriptor {
                prefix: String::new(),
                delimiter: Some('/'),
                extensions: Vec::new(),
            }],
            other: Vec::new(),
            shared: Vec::new(),
        },
        notify,
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx);
    let ns = result
        .output
        .expect("tagged OK must yield the NAMESPACE data");

    // The namespace data must be captured.
    assert_eq!(ns.personal.len(), 1);
    assert_eq!(ns.personal[0].prefix, "");
    assert_eq!(ns.personal[0].delimiter, Some('/'));
    assert!(ns.other.is_empty());
    assert!(ns.shared.is_empty());

    // The solicited `* NAMESPACE` must NOT leak into reclassified_as_events.
    assert!(
        result.reclassified_as_events.is_empty(),
        "solicited NAMESPACE response must not be reclassified as event"
    );
}

// ---------------------------------------------------------------------------
// EnableConsumer  -  normal capture (RFC 5161 Section 3)
// ---------------------------------------------------------------------------

#[test]
fn enable_consumer_captures_extensions() {
    let mut consumer = EnableConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    // Feed a solicited `* ENABLED CONDSTORE QRESYNC` response.
    consumer.on_response(
        UntaggedResponse::Enabled(vec!["CONDSTORE".into(), "QRESYNC".into()]),
        notify,
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx);

    assert_eq!(
        result
            .output
            .expect("tagged OK must yield the ENABLED list"),
        vec!["CONDSTORE", "QRESYNC"]
    );

    // The solicited `* ENABLED` must NOT leak into reclassified_as_events.
    assert!(
        result.reclassified_as_events.is_empty(),
        "solicited ENABLED response must not be reclassified as event"
    );
}

// ---------------------------------------------------------------------------
// EnableConsumer  -  empty/missing ENABLED (Postel's law tolerance)
// ---------------------------------------------------------------------------

#[test]
fn enable_consumer_missing_enabled_returns_empty() {
    // When the server omits the ENABLED response (non-conformant per
    // RFC 5161 Section 3.2), the consumer tolerates it and returns empty.
    let consumer = Box::new(EnableConsumer::default());
    let result = consumer.finalize(tagged_ok(), &default_ctx());
    assert!(
        result
            .output
            .expect("a missing ENABLED response is tolerated")
            .is_empty()
    );
    assert!(result.reclassified_as_events.is_empty());
}
