//! Capacity accounting, supersession and loss watermarks of PendingCoverage.

use std::sync::Arc;

use super::{CoverageClaim, PendingCoverage};
use bifrost_types::{
    BackfillCheckpoint, BackfillProgress, Checkpoint, CursorScope, ObjectType, Partition,
};

struct Harness {
    coverage: Arc<PendingCoverage>,
}

fn harness() -> Harness {
    Harness {
        coverage: Arc::new(PendingCoverage::new()),
    }
}

fn scope_a() -> CursorScope {
    CursorScope::Account
}

fn scope_b() -> CursorScope {
    CursorScope::Type(ObjectType::Email)
}

fn page(scope: &CursorScope, partition: &str, n: u64) -> Checkpoint {
    Checkpoint::Backfill(BackfillCheckpoint {
        scope: scope.clone(),
        partition: Partition(partition.as_bytes().to_vec()),
        progress_marker: None,
        progress: BackfillProgress {
            items_done: n,
            items_estimated: None,
        },
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    })
}

fn live_cursor(state: &[u8]) -> Checkpoint {
    Checkpoint::Change(bifrost_types::ChangeCursor {
        scope: scope_a(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: state.to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    })
}

impl Harness {
    /// Register and mark delivered, the order a producer uses.
    fn publish(&self, checkpoint: &Checkpoint, seq: u64) -> crate::cursor::AckToken {
        let id = self.coverage.register(
            checkpoint.clone(),
            CoverageClaim {
                reports: Vec::new(),
                generation: crate::cursor::WalkGeneration::for_test(0),
            },
        );
        self.coverage
            .mark_delivered(&id, crate::delivery::ReceiverSequence::for_test(seq));
        id
    }
}

/// A consumer may legitimately acknowledge a batch a later publication has
/// superseded - it really received it. That settles exactly one page.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledging_a_superseded_page_settles_exactly_that_page() {
    let h = harness();
    let mut ids = Vec::new();
    for n in 1..=3 {
        ids.push(h.publish(&page(&scope_a(), "page:0:30", n), 1));
    }
    h.coverage.acknowledge_publication(ids[0].clone());
    assert_eq!(h.coverage.backfill_in_flight(), 2);
    h.coverage.acknowledge_publication(ids[0].clone());
    assert_eq!(
        h.coverage.backfill_in_flight(),
        2,
        "a repeated acknowledgement must not manufacture capacity"
    );
}

/// A repeated acknowledgement of an OLD publication must not
/// release a later walk's capacity. There is no scope-wide release to abuse:
/// settlement is by publication identity, so an id the ledger no longer
/// holds settles nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeated_stale_acknowledgement_releases_nothing() {
    let h = harness();
    let sentinel = page(&scope_a(), "complete", 1);
    let stale = h.publish(&sentinel, 1);
    h.coverage.acknowledge_publication(stale.clone());
    assert_eq!(h.coverage.backfill_in_flight(), 0);

    // A later walk of the same scope.
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.publish(&page(&scope_a(), "page:10:20", 1), 1);
    assert_eq!(h.coverage.backfill_in_flight(), 2);

    for _ in 0..8 {
        h.coverage.acknowledge_publication(stale.clone());
    }
    assert_eq!(
        h.coverage.backfill_in_flight(),
        2,
        "replaying an old sentinel acknowledgement must not defeat the bound"
    );
}

/// Sibling partitions are in flight together and neither subsumes the other,
/// so each holds its own charge and settling one leaves the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sibling_partitions_each_hold_their_own_capacity() {
    let h = harness();
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    let y = h.publish(&page(&scope_a(), "page:10:20", 1), 1);
    h.publish(&page(&scope_b(), "page:0:10", 1), 1);
    assert_eq!(h.coverage.backfill_in_flight(), 3);

    h.coverage.acknowledge_publication(y);
    assert_eq!(h.coverage.backfill_in_flight(), 2);
}

/// The change stream supersedes too, and it never consults the bound,
/// so inheriting the capacity charge there would make a subscriber that
/// drains without acknowledging grow one entry's history without limit -
/// every retained id holding a `PublicationReceipt` and its whole coverage
/// claim. Only backfill has a bound to keep honest, so only backfill keeps
/// the history.
///
/// Bites on the SIZE of what the ledger retains, which is the actual defect;
/// asserting `backfill_in_flight() == 0` alone would pass against it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_change_stream_retains_no_publication_history() {
    let h = harness();
    for round in 0..64u32 {
        h.publish(&live_cursor(&round.to_be_bytes()), 1);
    }
    assert_eq!(h.coverage.backfill_in_flight(), 0);
    assert_eq!(
        h.coverage.pending_checkpoints(),
        1,
        "one change stream, one outstanding registration"
    );
    assert_eq!(
        h.coverage.retained_history(),
        0,
        "and it must be carrying no superseded ids at all; a live subscriber that \
         drains without acknowledging would otherwise grow this for ever"
    );

    // The backfill stream is the one that does keep a history, so the assertion
    // above is about the LANE and not about the mechanism being absent.
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.publish(&page(&scope_a(), "page:0:10", 2), 1);
    assert_eq!(h.coverage.retained_history(), 1);
}

/// Retiring a page a later publication has since superseded must
/// take its charge off the survivor. Searching only `entry.id` left it
/// charged for ever - and a failed store write on a multi-page partition is
/// exactly how that happens in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retiring_a_superseded_page_frees_its_capacity() {
    let h = harness();
    let first = h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.publish(&page(&scope_a(), "page:0:10", 2), 1);
    assert_eq!(h.coverage.backfill_in_flight(), 2);

    h.coverage.retire_publication(first);
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "the superseded page's charge lives on the survivor, so that is where \
         retiring it has to come off"
    );
}

/// An abandonment records the pages it drains as undelivered - but only the
/// ones that were SENT.
///
/// The consumer will never answer for a page abandoned out from under it, so
/// the walk that published it is holed and must not settle. A page that was
/// merely REGISTERED is a different thing entirely: the producer broadcasts
/// it moments later, the consumer receives and acknowledges it, and counting
/// it here would judge a walk holed over a page that arrived intact. That is
/// the same care `release_undelivered` takes with an unsent stamp, and for
/// the same reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandonment_records_only_the_pages_it_had_sent() {
    let h = harness();
    let before = h.coverage.undelivered_watermark(&scope_a());

    // One page broadcast to receiver 0, one registered and not yet sent.
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.coverage.register(
        page(&scope_a(), "page:10:20", 1),
        CoverageClaim {
            reports: Vec::new(),
            generation: crate::cursor::WalkGeneration::for_test(0),
        },
    );
    assert_eq!(h.coverage.backfill_in_flight(), 2);

    assert_eq!(h.coverage.abandon_checkpoints(), 2, "both are drained");
    assert_eq!(
        h.coverage.undelivered_watermark(&scope_a()),
        before.after_loss(1),
        "and exactly the one that had been sent is recorded as reaching nobody; \
         charging the unsent page too judges the walk holed over a page the \
         consumer is about to receive and acknowledge"
    );
}

/// A discard request belongs to ONE scope, and only that scope's walk may
/// take it.
///
/// The walk-end drain used to take every outstanding request. A loss recorded
/// against scope A after A's own walk ended is already repaired - the
/// ack-time refusal withholds A's marker and the pre-retry discard drops its
/// rows - but nothing cleared the request, so the next lossy walk of ANY
/// scope carried it off and acted on it arbitrarily later, deleting a
/// completion A had since re-earned and refusing the consumer's
/// acknowledgement of it. Every producer of a request has a scope; so does
/// every consumer of one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_walks_drain_takes_only_its_own_scopes_discard_request() {
    let h = harness();
    h.coverage.note_undelivered(&scope_a(), 1);
    h.coverage.note_undelivered(&scope_b(), 1);

    assert!(
        h.coverage.take_backfill_discard(&scope_b()),
        "scope B's walk takes B's request"
    );
    assert!(
        !h.coverage.take_backfill_discard(&scope_b()),
        "and taking is draining: the same request is not answered twice"
    );
    assert!(
        h.coverage.take_backfill_discard(&scope_a()),
        "A's request is still A's to answer - B's walk end must not have carried \
         it off and discarded rows A may since have re-earned"
    );
}

/// And it judges each SUBSUMED page by its own stamp, not by the survivor's.
///
/// A survivor can be unsent while the pages folded into it were delivered -
/// publish page A to the consumer, then register page B on the same partition
/// before it goes out - and gating the whole entry on the survivor's stamp
/// then records nothing at all. The page the consumer really lost to the lag
/// goes unrecorded, and the walk's completion marker becomes durable over it.
/// `release_undelivered` asks the question once per page for this reason;
/// this path now does too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandonment_judges_each_subsumed_page_by_its_own_stamp() {
    let h = harness();
    let before = h.coverage.undelivered_watermark(&scope_a());

    // One page of a partition, sent; then a second on the SAME partition,
    // which supersedes it and has not been sent yet.
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.coverage.register(
        page(&scope_a(), "page:0:10", 2),
        CoverageClaim {
            reports: Vec::new(),
            generation: crate::cursor::WalkGeneration::for_test(0),
        },
    );
    assert_eq!(
        h.coverage.retained_history(),
        1,
        "the survivor carries the sent page it superseded"
    );

    assert_eq!(h.coverage.abandon_checkpoints(), 2);
    assert_eq!(
        h.coverage.undelivered_watermark(&scope_a()),
        before.after_loss(1),
        "the SUBSUMED page was delivered and is lost with the abandonment; the \
         unsent survivor is not. Reading the survivor's stamp for both records \
         nothing and lets the marker stand over a page the consumer lost"
    );
}

/// Retirement, abandonment and scope invalidation each free capacity because
/// they already remove the record. No release call site to forget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_ledger_retirement_frees_capacity() {
    let h = harness();
    let retired = h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.coverage.retire_publication(retired);
    assert_eq!(h.coverage.backfill_in_flight(), 0);

    h.publish(&page(&scope_a(), "page:10:20", 1), 1);
    assert_eq!(h.coverage.abandon_checkpoints(), 1);
    assert_eq!(h.coverage.backfill_in_flight(), 0);

    // Two pages of ONE partition, so the survivor carries a subsumed page:
    // the shape the reset used to lose track of.
    h.publish(&page(&scope_a(), "page:20:30", 1), 1);
    h.publish(&page(&scope_a(), "page:20:30", 2), 1);
    h.publish(&page(&scope_b(), "page:0:10", 1), 1);
    assert_eq!(h.coverage.backfill_in_flight(), 3);
    let _ = h.coverage.invalidate_scope(&scope_a());
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "invalidation frees the subsumed page too; only the sibling scope remains"
    );
}

/// Receiver A holds pages, B subscribes afterwards, A drops. The
/// count never reaches zero and B started beyond those pages, so nothing can
/// ever acknowledge them - and subscriber COUNT cannot see it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_no_live_receiver_can_reach_are_released() {
    let h = harness();
    // Published when the account had handed out exactly one receiver (A).
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.publish(&page(&scope_a(), "page:10:20", 1), 1);
    assert_eq!(h.coverage.backfill_in_flight(), 2);

    // B is receiver index 1; A (index 0) is gone. The lowest live sequence
    // is 1, which is not below the sequence those pages were sent at.
    assert_eq!(
        h.coverage
            .release_undelivered(Some(crate::delivery::ReceiverSequence::for_test(1))),
        2
    );
    assert_eq!(h.coverage.backfill_in_flight(), 0);
}

/// The mirror. A receiver that existed when the page went out can still
/// acknowledge it, so the bound must stay in force - otherwise it lapses the
/// moment any second consumer appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receiver_that_predates_the_page_keeps_it_charged() {
    let h = harness();
    // Sent when two receivers had been handed out; receiver 0 is still live.
    h.publish(&page(&scope_a(), "page:0:10", 1), 2);
    assert_eq!(
        h.coverage
            .release_undelivered(Some(crate::delivery::ReceiverSequence::for_test(0))),
        0
    );
    assert_eq!(h.coverage.backfill_in_flight(), 1);
}

/// A page registered but NOT YET SENT must never be judged
/// undeliverable: a receiver subscribing in that window really does receive
/// it. `delivered_at` is `None` until the send, and that is what the sweep
/// keys on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_not_yet_sent_is_never_swept() {
    let h = harness();
    let id = h.coverage.register(
        page(&scope_a(), "page:0:10", 1),
        CoverageClaim {
            reports: Vec::new(),
            generation: crate::cursor::WalkGeneration::for_test(0),
        },
    );
    assert_eq!(
        h.coverage.release_undelivered(None),
        0,
        "an unsent page is not evidence of anything yet"
    );
    assert_eq!(h.coverage.backfill_in_flight(), 1);

    // The replacement subscribes, THEN the send happens and records the
    // sequence that includes it.
    h.coverage
        .mark_delivered(&id, crate::delivery::ReceiverSequence::for_test(2));
    assert_eq!(
        h.coverage
            .release_undelivered(Some(crate::delivery::ReceiverSequence::for_test(1))),
        0
    );
    assert_eq!(h.coverage.backfill_in_flight(), 1);
}

/// "Is somebody subscribed at the end" is not the question; "did
/// every page of this walk reach somebody" is, and only a WATERMARK can
/// answer it.
///
/// A consumer that fills the bound and leaves, is replaced before the walk
/// finishes, and whose replacement acknowledges the rest satisfies every
/// point-in-time check while the pages published in the gap reached nobody.
/// The orchestrator compares this watermark across the walk for exactly that
/// reason.
#[tokio::test(start_paused = true)]
async fn the_undelivered_watermark_records_pages_that_reached_nobody() {
    let h = harness();
    let before = h.coverage.undelivered_watermark(&scope_a());

    // Two pages delivered to receiver 0, which then goes away: the sweep
    // retires them and records the loss.
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    h.publish(&page(&scope_a(), "page:10:20", 1), 1);
    assert_eq!(h.coverage.release_undelivered(None), 2);
    let after_sweep = h.coverage.undelivered_watermark(&scope_a());
    assert!(
        after_sweep > before,
        "a sweep that retires pages nobody can read must record it"
    );

    // A page the producer sends while nobody is listening records it too -
    // that is the runner's retire-on-sentinel-only path.
    h.coverage.note_undelivered(&scope_a(), 1);
    assert!(h.coverage.undelivered_watermark(&scope_a()) > after_sweep);

    // And it is MONOTONIC: a consumer arriving afterwards cannot erase the
    // record, which is the whole point. A subscriber count would read as
    // healthy the instant one appears.
    assert!(h.coverage.undelivered_watermark(&scope_a()) > before);
}

/// And the record is PER SCOPE. An account-wide counter would let a page of
/// one scope that reached nobody make another scope's in-flight walk look
/// holed - so a walk that delivered every one of its own pages would have its
/// completion marker withheld and be sent round again, for ever, for as long
/// as any sibling scope kept losing pages.
///
/// Both routes into the counter are checked, because both take a scope and
/// either could take the wrong one: the departure sweep, which attributes each
/// retired entry through its own publication stream, and the direct
/// retire-on-sentinel-only note.
#[tokio::test(start_paused = true)]
async fn the_undelivered_watermark_is_recorded_against_one_scope_only() {
    let h = harness();
    let b_before = h.coverage.undelivered_watermark(&scope_b());

    // A page of scope A, published to a receiver that then departs.
    h.publish(&page(&scope_a(), "page:0:10", 1), 1);
    assert_eq!(h.coverage.release_undelivered(None), 1);
    assert!(
        h.coverage.undelivered_watermark(&scope_a()) > crate::cursor::UndeliveredMark::for_test(0),
        "the sweep must record against the scope whose page it retired"
    );
    assert_eq!(
        h.coverage.undelivered_watermark(&scope_b()),
        b_before,
        "and against no other: a sibling scope's walk delivered everything and \
         must not be judged holed for it"
    );

    // Same for the publish path's direct note.
    h.coverage.note_undelivered(&scope_a(), 1);
    assert_eq!(
        h.coverage.undelivered_watermark(&scope_b()),
        b_before,
        "the retire-on-sentinel-only path is per scope too"
    );
    // And scope B's own losses are still recorded, on B.
    h.coverage.note_undelivered(&scope_b(), 1);
    assert_eq!(
        h.coverage.undelivered_watermark(&scope_b()),
        b_before.after_loss(1)
    );
}

/// The park warning has to NAME the receiver that stopped reading, or a
/// consumer holding an unpolled receiver sees only "backfill is stuck".
///
/// The crate has no tracing capture harness, so what is pinned is the
/// accessor the warning reads: the live numbered receivers eligible for an
/// outstanding page that have not read it, and that page's scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_charge_holders_name_the_receiver_that_has_not_read() {
    let h = harness();
    h.coverage
        .receiver_joined(crate::delivery::ReceiverSequence::for_test(0));
    h.coverage
        .receiver_joined(crate::delivery::ReceiverSequence::for_test(1));
    // Sent when two receivers had been handed out, so both are eligible.
    let id = h.publish(&page(&scope_a(), "page:0:10", 1), 2);

    assert_eq!(
        h.coverage.backfill_charge_holders(),
        (
            vec![
                crate::delivery::ReceiverSequence::for_test(0),
                crate::delivery::ReceiverSequence::for_test(1)
            ],
            vec![scope_a()]
        ),
        "both eligible receivers are holding the page, and the warning must be \
         able to say which"
    );

    h.coverage
        .mark_received(&id, crate::delivery::ReceiverSequence::for_test(1));
    assert_eq!(
        h.coverage.backfill_charge_holders(),
        (
            vec![crate::delivery::ReceiverSequence::for_test(0)],
            vec![scope_a()]
        ),
        "the fast reader drops out of the list; the slow one is the stall"
    );

    h.coverage
        .mark_received(&id, crate::delivery::ReceiverSequence::for_test(0));
    assert_eq!(
        h.coverage.backfill_charge_holders(),
        (Vec::new(), Vec::new()),
        "a fully read page holds nothing and names nobody"
    );
}

/// Model a refusal delayed between the guarded send and loss attribution.
/// The publication can already be retired, so membership alone cannot tell
/// whether its loss still belongs to the walk that owns the durable rows.
#[test]
fn delayed_publication_loss_cannot_cross_a_reset_or_discard_fence() {
    for reset in 0..3 {
        let h = harness();
        let scope = scope_a();
        let old = h.publish(&page(&scope, "old", 1), 1);
        h.coverage.acknowledge_publication(old.clone());
        match reset {
            0 => {
                h.coverage.invalidate_account();
            }
            1 => {
                h.coverage.invalidate_scope(&scope);
            }
            _ => {
                h.coverage.invalidate_scope_backfill(&scope, None);
            }
        }
        let current = h.publish(&page(&scope, "new", 2), 1);
        let before = h.coverage.undelivered_watermark(&scope);
        h.coverage.note_undelivered_publication(&old);
        assert_eq!(h.coverage.undelivered_watermark(&scope), before);
        assert!(h.coverage.take_backfill_discards().is_empty());
        assert_eq!(h.coverage.backfill_in_flight(), 1);

        // A current loss must still request replay, even after its registration
        // has gone. Detach consumes this same request set before writer exit.
        h.coverage.acknowledge_publication(current.clone());
        h.coverage.note_undelivered_publication(&current);
        assert_eq!(
            h.coverage.undelivered_watermark(&scope),
            before.after_loss(1)
        );
        assert_eq!(
            h.coverage
                .take_backfill_discards()
                .into_iter()
                .map(|request| request.scope)
                .collect::<Vec<_>>(),
            vec![scope]
        );
    }
}

#[test]
fn a_sibling_scope_reset_does_not_suppress_current_publication_loss() {
    let h = harness();
    let id = h.publish(&page(&scope_a(), "current", 1), 1);
    h.coverage.invalidate_scope(&scope_b());
    h.coverage.note_undelivered_publication(&id);
    assert!(h.coverage.take_backfill_discard(&scope_a()));
    assert!(!h.coverage.take_backfill_discard(&scope_b()));
}
