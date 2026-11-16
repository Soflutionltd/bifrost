use super::*;
use tokio::sync::mpsc;

#[test]
fn account_cursor_and_attachment_events_are_distinct() {
    let cursor = EventScope::Cursor(CursorScope::Account);
    assert_ne!(cursor, EventScope::Account);
    assert_eq!(cursor.cursor(), Some(&CursorScope::Account));
    assert_eq!(EventScope::Account.cursor(), None);
}

fn warning_event(message: &str) -> MultiplexerEvent {
    MultiplexerEvent {
        scope: EventScope::Account,
        event: Arc::new(SyncEvent::Warning(bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::Other,
            message,
        ))),
        checkpoint: None,
        publication: None,
    }
}

pub(crate) fn lag_test_control() -> crate::control::SyncControl {
    let (boundary, _view) = crate::cancel::Boundary::new();
    let (priority, _priority_view) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _bandwidth_view) = tokio::sync::watch::channel(None);
    crate::control::SyncControl::new(
        bifrost_types::AccountId("lag".into()),
        boundary,
        priority,
        bandwidth,
    )
}

/// The same control over a caller-supplied ledger, so a test can observe the
/// backfill charge the control's publications create - as `attach` does,
/// where the slot and the control share one `PendingCoverage`.
pub(crate) fn lag_test_control_over(
    coverage: Arc<crate::cursor::PendingCoverage>,
) -> crate::control::SyncControl {
    let (boundary, view) = crate::cancel::Boundary::new();
    let (priority, p) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, b) = tokio::sync::watch::channel(None);
    std::mem::forget((view, p, b));
    crate::control::SyncControl::new_with_publications(
        bifrost_types::AccountId("lag".into()),
        boundary,
        priority,
        bandwidth,
        coverage,
    )
}

pub(crate) fn account_change_checkpoint(state: &[u8]) -> Checkpoint {
    Checkpoint::Change(bifrost_types::ChangeCursor {
        scope: CursorScope::Account,
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: state.to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    })
}

/// A scope deletion (or a detach) must CUT SHORT a recovery delay, not wait
/// it out. The retry sleep used to be un-selected, so a deleted scope or a
/// detaching account sat through the provider's full hint - and detach then
/// burned toward `detach_timeout` and aborted the task instead of draining
/// it.
#[tokio::test]
async fn try_recv_takes_the_same_lag_recovery_path() {
    let (tx, sentinel) = broadcast::channel(1);
    let control = lag_test_control();
    let mut receiver = ChangesReceiver::new(tx.subscribe(), Some(control.clone()), None);
    control.expect_checkpoint(account_change_checkpoint(b"lost"));
    tx.send(warning_event("overwritten"))
        .expect("receivers live");
    tx.send(warning_event("retained")).expect("receivers live");

    let lag = receiver.try_recv().expect("lag becomes an event");
    assert!(matches!(
        lag.event.as_ref(),
        SyncEvent::Warning(warning)
            if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
    ));
    assert!(
        control.abandon_pending_checkpoints() == 0,
        "try_recv's lag must already have abandoned the registration"
    );
    drop(sentinel);
}

/// The lag path's OTHER half, and it has to be exercised through
/// `ChangesReceiver` rather than by calling the ledger directly - the wiring
/// is the thing that can go missing.
///
/// The reasoning it pins: abandonment retires every outstanding
/// registration, so no acknowledgement can arrive for them - and backfill
/// capacity IS those registrations. Leaving them charged converts the
/// in-session loss the warning reports into a cold-start producer that never
/// runs again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lag_frees_the_backfill_bound() {
    let (tx, sentinel) = broadcast::channel(1);
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let control = lag_test_control_over(Arc::clone(&coverage));
    let mut receiver = ChangesReceiver::new(
        tx.subscribe(),
        Some(control.clone()),
        Some(Arc::clone(&coverage)),
    );

    // One outstanding backfill publication, charged exactly as the runner
    // would have left it.
    let checkpoint = Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
        scope: CursorScope::Account,
        partition: bifrost_types::Partition(b"page:0:10".to_vec()),
        progress_marker: None,
        progress: bifrost_types::BackfillProgress::default(),
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    });
    control.expect_checkpoint(checkpoint);
    assert_eq!(coverage.backfill_in_flight(), 1);

    // Overflow the one-slot ring under the receiver.
    for state in [b"a".as_slice(), b"b".as_slice()] {
        let _ = tx.send(MultiplexerEvent::unacked(
            EventScope::Account,
            Arc::new(SyncEvent::Warning(bifrost_types::Warning::user_safe(
                bifrost_types::WarningKind::Other,
                String::from_utf8_lossy(state).into_owned(),
            ))),
        ));
    }

    let lag = receiver.recv().await.expect("lag becomes an event");
    assert!(matches!(
        lag.event.as_ref(),
        SyncEvent::Warning(warning)
            if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
    ));
    assert_eq!(
        coverage.backfill_in_flight(),
        0,
        "a lag abandons the registration, and the registration is the capacity"
    );
    drop(sentinel);
}

/// A backfill delivery harness that publishes exactly the way the runner
/// does - register the publication, then `publish_backfill` to send and stamp
/// under the delivery lock. Nothing here stamps by hand, so the tests below
/// exercise the send/stamp/subscribe/sweep ordering rather than assuming it.
struct Delivery {
    delivery: Arc<ChangeDelivery>,
    coverage: Arc<crate::cursor::PendingCoverage>,
    control: crate::control::SyncControl,
    _sentinel: broadcast::Receiver<MultiplexerEvent>,
}

fn delivery_harness(capacity: usize) -> Delivery {
    let (tx, sentinel) = broadcast::channel(capacity);
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let control = lag_test_control_over(Arc::clone(&coverage));
    Delivery {
        delivery: Arc::new(ChangeDelivery::new(tx)),
        coverage,
        control,
        _sentinel: sentinel,
    }
}

#[test]
fn an_unreachable_page_is_retired_and_records_the_walk_loss() {
    let h = delivery_harness(4);
    let mut observer = h.delivery.observe();
    let (writer_tx, writer_rx) = mpsc::channel(4);
    drop(writer_rx);
    let publisher = PagePublisher::new(
        Arc::clone(&h.delivery),
        h.control.clone(),
        writer_tx,
        CancellationToken::new(),
    );
    let checkpoint = Delivery::backfill_page("page:0:10");
    let before = h.coverage.undelivered_watermark(&CursorScope::Account);
    let event = MultiplexerEvent {
        scope: EventScope::Cursor(CursorScope::Account),
        event: Arc::new(SyncEvent::Done(Some(checkpoint.clone()))),
        checkpoint: Some(checkpoint),
        publication: None,
    };
    assert!(
        !publisher
            .publish_page(
                event,
                crate::cursor::CoverageClaim::new(
                    bifrost_types::InventoryCoverageReport::complete(
                        bifrost_types::CoverageDomain::full(CursorScope::Account),
                    ),
                    h.coverage.next_generation(),
                ),
                h.coverage.scope_fence(&CursorScope::Account),
            )
            .expect("current fence")
    );
    assert!(
        observer
            .try_recv()
            .expect("observer delivery")
            .publication
            .is_some()
    );
    assert_eq!(h.coverage.pending_checkpoints(), 0);
    assert_ne!(
        h.coverage.undelivered_watermark(&CursorScope::Account),
        before
    );
}

/// Exercise both refusal callers through registration and the guarded send.
/// Moving loss attribution into the send callback would re-enter the ledger's
/// synchronous mutex and hang this test.
#[test]
fn closed_delivery_records_page_and_marker_loss_without_reentering_coverage() {
    for marker in [false, true] {
        let h = delivery_harness(4);
        let _receiver = h.subscribe();
        h.delivery.close();
        let (writer_tx, writer_rx) = mpsc::channel(4);
        drop(writer_rx);
        let publisher = PagePublisher::new(
            Arc::clone(&h.delivery),
            h.control.clone(),
            writer_tx,
            CancellationToken::new(),
        );
        let scope = CursorScope::Account;
        let fence = h.coverage.scope_fence(&scope);
        let before = h.coverage.undelivered_watermark(&scope);
        let checkpoint = Delivery::backfill_page("refused");
        let event = MultiplexerEvent {
            scope: EventScope::Cursor(scope.clone()),
            event: Arc::new(SyncEvent::Done(Some(checkpoint.clone()))),
            checkpoint: Some(checkpoint),
            publication: None,
        };
        let reached = if marker {
            publisher.publish_marker(event, before, fence)
        } else {
            publisher
                .publish_page(
                    event,
                    crate::cursor::CoverageClaim::new(
                        bifrost_types::InventoryCoverageReport::complete(
                            bifrost_types::CoverageDomain::full(scope.clone()),
                        ),
                        h.coverage.next_generation(),
                    ),
                    fence,
                )
                .expect("current fence")
        };
        assert!(!reached);
        assert_eq!(h.coverage.pending_checkpoints(), 0);
        assert_ne!(h.coverage.undelivered_watermark(&scope), before);
    }
}

/// Projection is caller-controlled iteration, so it can deterministically
/// reset after the old implementation's early fence read and before its send.
/// That implementation publishes and leaves a stale batch in the receiver.
#[test]
fn checkpoint_free_inventory_checks_the_fence_at_the_send() {
    for account_reset in [false, true] {
        let h = delivery_harness(4);
        let mut receiver = h.subscribe();
        let (writer_tx, writer_rx) = mpsc::channel(4);
        drop(writer_rx);
        let publisher = PagePublisher::new(
            Arc::clone(&h.delivery),
            h.control.clone(),
            writer_tx,
            CancellationToken::new(),
        );
        let scope = CursorScope::Account;
        let fence = h.coverage.scope_fence(&scope);
        let generation = h.coverage.next_generation();
        let batch = bifrost_types::InventoryBatch {
            items: Vec::new(),
            removed: Vec::new(),
            page_boundary: bifrost_types::PageBoundary::Page,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: bifrost_types::PageCheckpoint::None,
            coverage: bifrost_types::InventoryCoverageReport::complete(
                bifrost_types::CoverageDomain::full(scope.clone()),
            ),
        };
        let id = bifrost_types::ObjectId("stale".into());
        let ids = std::iter::once_with(|| {
            if account_reset {
                h.coverage.raise_account_fence();
            } else {
                h.coverage.invalidate_scope(&scope);
            }
            &id
        });
        let result = publisher
            .publish_inventory(&scope, &batch, ids, [], None, generation, fence)
            .expect("valid inventory projection");
        assert!(matches!(result, InventoryPublication::Refused(_)));
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        // Refusal must release both locks and leave the replacement walk usable.
        let result = publisher
            .publish_inventory(
                &scope,
                &batch,
                [&id],
                [],
                None,
                generation,
                h.coverage.scope_fence(&scope),
            )
            .expect("replacement inventory projection");
        assert!(matches!(result, InventoryPublication::Published));
        assert!(matches!(
            receiver
                .try_recv()
                .expect("replacement page")
                .event
                .as_ref(),
            SyncEvent::Batch(_)
        ));
    }
}

#[tokio::test]
async fn numbered_subscribers_cannot_split_between_coverage_ledgers() {
    let h = delivery_harness(4);
    let _first = h.subscribe();
    let other = Arc::new(crate::cursor::PendingCoverage::new());
    let mut second = h.delivery.subscribe(None, other);
    assert!(matches!(
        second.recv().await,
        Err(broadcast::error::RecvError::Closed)
    ));
    assert!(second.subscription.is_none());
    assert!(second.coverage.is_none());
    assert!(h.delivery.has_numbered_subscriber());
}

impl Delivery {
    fn backfill_page(partition: &str) -> Checkpoint {
        Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope: CursorScope::Account,
            partition: bifrost_types::Partition(partition.as_bytes().to_vec()),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        })
    }

    /// Register and broadcast one backfill page, as `run_partition` does.
    fn publish(&self, partition: &str) -> crate::cursor::AckToken {
        self.publish_reaching(partition).0
    }

    /// `publish`, also reporting what the gate reported: whether a
    /// NUMBERED receiver took delivery.
    fn publish_reaching(&self, partition: &str) -> (crate::cursor::AckToken, bool) {
        let checkpoint = Self::backfill_page(partition);
        let id = self.control.expect_checkpoint(checkpoint.clone());
        let event = MultiplexerEvent {
            scope: EventScope::Cursor(CursorScope::Account),
            event: Arc::new(SyncEvent::Batch(bifrost_types::Batch {
                items: Vec::new(),
                page_boundary: bifrost_types::PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: Some(checkpoint.clone()),
            })),
            checkpoint: Some(checkpoint),
            publication: Some(id.clone()),
        };
        let reached = self
            .delivery
            .publish_backfill(event, &self.coverage, Some(&id));
        (id, reached)
    }

    fn subscribe(&self) -> ChangesReceiver {
        self.delivery
            .subscribe(Some(self.control.clone()), Arc::clone(&self.coverage))
    }
}

/// FINDINGS 3, 4 and 5 at the seam that has to notice, and all three are one
/// ordering question. Receiver A takes delivery of two pages, B subscribes
/// afterwards, A drops. The subscriber COUNT never reaches zero, and B joined
/// at the ring's tail beyond A's pages, so nothing can ever acknowledge them
/// and the producer would park for the life of the attachment.
///
/// Every step goes through the real path - `ChangeDelivery::publish_backfill`
/// for the send AND stamp, `subscribe` for the numbering,
/// `ChangesReceiver::drop` for the sweep. A version of this that stamped by
/// hand could not have exercised the ordering at all, which is exactly how
/// three separate interleavings survived the previous round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overlapping_receiver_replacement_frees_pages_nobody_can_reach() {
    let h = delivery_harness(8);
    let a = h.subscribe();
    h.publish("page:0:10");
    h.publish("page:10:20");
    assert_eq!(h.coverage.backfill_in_flight(), 2);

    // B arrives BEFORE A leaves, so the count never touches zero.
    let mut b = h.subscribe();
    assert!(
        h.delivery.has_numbered_subscriber(),
        "a consumer is present throughout"
    );
    assert_eq!(
        h.coverage.backfill_in_flight(),
        2,
        "B's arrival alone settles nothing"
    );
    assert!(
        b.try_recv().is_err(),
        "B really did join beyond those pages - it can never receive them"
    );

    drop(a);

    assert_eq!(
        h.coverage.backfill_in_flight(),
        0,
        "B joined at the ring's tail and can never receive A's pages, so their \
             capacity must come back when A leaves"
    );
}

/// The receipt bound itself. A numbered receiver's READ returns the page's
/// lane capacity, with no acknowledgement anywhere in sight - and the
/// boundary registration survives it, because reading and acknowledging
/// answer different questions.
///
/// The ablation is the whole point: with `note_receipt` removed, or with
/// `backfill_in_flight` summing entries rather than unread pages, the first
/// assertion reads 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_numbered_receivers_read_frees_capacity_without_an_acknowledgement() {
    let h = delivery_harness(8);
    let mut a = h.subscribe();
    h.publish("page:0:10");
    h.publish("page:10:20");
    assert_eq!(h.coverage.backfill_in_flight(), 2);

    a.try_recv().expect("the first page");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "reading a page takes it out of the ring, so its capacity comes back"
    );
    a.try_recv().expect("the second page");
    assert_eq!(h.coverage.backfill_in_flight(), 0);

    assert_eq!(
        h.coverage.pending_checkpoints(),
        2,
        "and neither page is answered for: a read frees the producer, an \
             acknowledgement retires the registration"
    );

    // The completion guarantee is unchanged by the receipt. A receiver that
    // departs holding pages it READ but never acknowledged still strands them.
    drop(a);
    assert_eq!(
        h.coverage.undelivered_watermark(&CursorScope::Account),
        crate::cursor::UndeliveredMark::for_test(2),
        "read-but-unacknowledged pages leaving with their reader are still a loss"
    );
}

/// The bound follows the SLOWEST live numbered reader, not the fastest.
///
/// Every receiver `account_changes_stream` hands out is numbered, and several
/// live subscribers with exactly one acknowledging is a supported and
/// documented shape. While the receipt was a single bit, the first read by ANY
/// of them freed the producer: the fast observer B empties the ring, cold start
/// runs on, the acknowledger A - slower because it persists as it goes - falls
/// `changes_capacity` events behind, lags, and `on_lag` abandons every
/// outstanding backfill page and withholds the walk's marker. That is the
/// overrun the bound exists to prevent, reproduced BY the bound, and it repeats
/// for as long as B stays faster.
///
/// The two halves are one property: the fast reader must not advance the
/// producer past the slow one, and the slow one's DEPARTURE must advance it at
/// once - a departed reader that still parked the producer would trade this
/// failure for the stall the departure sweep exists to prevent.
///
/// Ablation: with first-read semantics the first assertion reads 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bound_follows_the_slowest_live_numbered_reader() {
    let h = delivery_harness(8);
    let mut slow = h.subscribe();
    let mut fast = h.subscribe();
    h.publish("page:0:10");
    h.publish("page:10:20");
    assert_eq!(h.coverage.backfill_in_flight(), 2);

    fast.try_recv().expect("the first page");
    fast.try_recv().expect("the second page");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        2,
        "both pages are still in the ring as far as the slow reader is concerned, \
             so the fast one's reads must buy the producer nothing"
    );

    slow.try_recv().expect("the first page");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "the page every eligible reader has now taken out of the ring is the only \
             one released"
    );

    // And the slow reader walking away frees the rest, rather than parking the
    // producer for the life of the attachment.
    drop(slow);
    assert_eq!(
        h.coverage.backfill_in_flight(),
        0,
        "a departed reader can no longer be holding anything in the ring; the \
             departure has to recompute its charge, not only sweep what nobody can \
             reach"
    );
}

/// A page that a later publication SUPERSEDED keeps its own receipt, on the
/// survivor that inherited its charge.
///
/// Supersession is the one place the "capacity is the live record" rule is
/// corrected, so it is also the one place a per-page bit can be lost by
/// folding. Reading both pages of one partition must free both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reading_a_superseded_page_frees_the_charge_it_left_on_its_survivor() {
    let h = delivery_harness(8);
    let mut a = h.subscribe();
    h.publish("partition:one");
    h.publish("partition:one");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        2,
        "the survivor inherited the superseded page's charge"
    );

    a.try_recv().expect("the superseded page");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "the receipt has to reach the page inside the survivor, not only the \
             survivor's own bit"
    );
    a.try_recv().expect("the survivor");
    assert_eq!(h.coverage.backfill_in_flight(), 0);
}

/// An UNNUMBERED receiver's read frees nothing.
///
/// The bound follows the reader that can acknowledge. A reader the delivery
/// gate never numbered is by construction not that one, and letting its read
/// free the producer would let a walk run a whole ring ahead of the consumer
/// who does answer for the pages - the overrun the bound exists to prevent.
/// This is also the condition the ruled observer subscription rests on.
///
/// A NUMBERED receiver has to be present and unread for this to observe
/// anything. With none, the page's `delivered_at` is 0, no live receiver is
/// eligible, and `page_unread` answers from its "has anybody read it"
/// fallback - so the count reads 1 whether or not the unnumbered read was
/// recorded, and the test passes against the bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unnumbered_receivers_read_frees_no_capacity() {
    let h = delivery_harness(8);
    // The acknowledger, numbered 0, which reads nothing yet.
    let mut acker = h.subscribe();
    let mut observer = h.delivery.observe();
    h.publish("page:0:10");
    assert_eq!(h.coverage.backfill_in_flight(), 1);

    observer.try_recv().expect("the observer takes delivery");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "a reader that never acknowledges must not free the producer to run \
             further ahead of the one that does"
    );

    acker.try_recv().expect("the acknowledger takes delivery");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        0,
        "and the NUMBERED receiver's read is what frees it, so the assertion \
             above is about the numbering and not about the page being unreadable"
    );
}

/// An OBSERVER counts for neither the subscriber gate nor the delivery
/// decision, and its reads touch the bound not at all.
///
/// The three places that used to see only a raw receiver count, driven
/// through the real path. Before the explicit observer subscription, an
/// observer arriving first satisfied the gate and took delivery of pages the
/// acknowledger - joining at the ring's tail - could never see; and once the
/// acknowledger had gone, every later page was judged delivered because the
/// observer had it, stayed charged, and could never be answered for.
///
/// Ablation: an observer minted through `subscribe` fails the first assertion;
/// a delivery decision read off `receiver_count()` fails the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_counts_for_neither_the_gate_nor_the_delivery() {
    let h = delivery_harness(8);
    let mut observer = h.delivery.observe();
    assert!(
        !h.delivery.has_numbered_subscriber(),
        "an observer must not open the subscriber gate"
    );

    let (unreached, reached) = h.publish_reaching("page:0:10");
    assert!(
        !reached,
        "a page that reached only an observer reached nobody who can acknowledge it"
    );
    // What the runner does with that answer: retire the publication, since
    // no acknowledgement can ever come for it.
    h.control.retire_publication(unreached);
    assert_eq!(h.coverage.backfill_in_flight(), 0);
    assert!(
        !h.delivery
            .publish_acknowledgeable(MultiplexerEvent::unacked(
                EventScope::Account,
                Arc::new(SyncEvent::Warning(bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::Other,
                    "live".to_owned(),
                ))),
            )),
        "the same answer on the live-lane path"
    );
    observer
        .try_recv()
        .expect("the observer still takes delivery");
    observer.try_recv().expect("of everything");

    // The acknowledger arrives SECOND, which is the order that used to lose
    // pages, and is now the ordinary one.
    let mut acker = h.subscribe();
    assert!(h.delivery.has_numbered_subscriber());
    let (_, reached) = h.publish_reaching("page:10:20");
    assert!(reached, "a numbered receiver took delivery");
    assert_eq!(h.coverage.backfill_in_flight(), 1);

    observer.try_recv().expect("the observer reads the page");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "an observer's read frees nothing"
    );
    acker.try_recv().expect("the acknowledger reads the page");
    assert_eq!(
        h.coverage.backfill_in_flight(),
        0,
        "the numbered receiver's read is what frees it"
    );

    // And the observer's departure sweeps nothing: it was never holding
    // anything on the ledger's account.
    h.publish("page:20:30");
    assert_eq!(h.coverage.backfill_in_flight(), 1);
    drop(observer);
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "the page is still the acknowledger's to read"
    );
    drop(acker);
}

/// An observer's lag is its own problem. The registrations belong to the
/// acknowledger, and abandoning them because a UI reader fell behind voided
/// every in-flight walk on the account - `on_lag` used to abandon for any
/// receiver that carried a control handle, and the old unnumbered observer
/// carried one.
///
/// The acknowledger lagging on the same ring still abandons, which is the
/// ablation's other half: the difference is the receiver's role, not the
/// ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observers_lag_abandons_nothing() {
    let h = delivery_harness(1);
    let mut acker = h.subscribe();
    let mut observer = h.delivery.observe();
    h.control
        .expect_checkpoint(account_change_checkpoint(b"registered"));
    assert_eq!(h.coverage.pending_checkpoints(), 1);

    // Overflow the one-slot ring under both receivers.
    for text in ["overwritten", "retained"] {
        let _ = h.delivery.publish_unacked(warning_event(text));
    }

    let lag = observer.try_recv().expect("lag becomes an event");
    assert!(matches!(
        lag.event.as_ref(),
        SyncEvent::Warning(warning)
            if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
    ));
    assert_eq!(
        h.coverage.pending_checkpoints(),
        1,
        "an observer falling behind says nothing about whether the acknowledger \
             can still answer for the registration"
    );

    let lag = acker.try_recv().expect("the acknowledger lags too");
    assert!(matches!(
        lag.event.as_ref(),
        SyncEvent::Warning(warning)
            if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
    ));
    assert_eq!(
        h.coverage.pending_checkpoints(),
        0,
        "the acknowledger's lag is the one that abandons"
    );
}

/// Once teardown has begun, a departing receiver's sweep is inert: no loss
/// is recorded and no discard is requested for the pages it was holding.
///
/// The ruling this pins (2026-09-06, at `PendingCoverage::release_undelivered`):
/// a page delivered but unacknowledged at detach is the consumer's, exactly
/// as at a crash, and a drop anywhere in the teardown window behaves like a
/// drop after `detach` returns. The registration itself is untouched - the
/// sweep is what goes inert, not the ledger.
///
/// Ablation: without the teardown check the watermark reads 1 and a discard
/// request is left for a writer that has already been drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_departure_during_teardown_records_no_loss() {
    let h = delivery_harness(8);
    let mut a = h.subscribe();
    h.publish("page:0:10");
    a.try_recv().expect("delivered, never acknowledged");
    h.publish("page:10:20");
    assert_eq!(h.coverage.backfill_in_flight(), 1);

    h.delivery.begin_teardown(&h.coverage);
    drop(a);

    assert_eq!(
        h.coverage.undelivered_watermark(&CursorScope::Account),
        crate::cursor::UndeliveredMark::for_test(0),
        "pages in a consumer's hands at detach are the consumer's, not a loss"
    );
    assert!(
        h.coverage.take_backfill_discards().is_empty(),
        "and nothing asks the departed writer to drop rows"
    );
    assert_eq!(
        h.coverage.pending_checkpoints(),
        2,
        "the sweep is inert; the ledger is not touched"
    );
    // The unread page stays charged under the no-live-receiver fallback
    // ("has anybody read it"), as a page published to nobody always does
    // until something retires it. Harmless here: teardown has already
    // cancelled the producer this charge would otherwise park.
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "the read page is uncharged; the unread one waits for a retirement that \
             teardown will never need"
    );
}

/// The mirror, and the reason the sweep is keyed on the numbering rather than
/// on "did anyone leave": a receiver that predates the page can still
/// acknowledge it, so the bound must stay in force when a LATER receiver
/// departs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_departing_latecomer_does_not_free_a_page_its_predecessor_holds() {
    let h = delivery_harness(8);
    let _a = h.subscribe();
    let b = h.subscribe();
    h.publish("page:0:10");

    drop(b);
    assert_eq!(
        h.coverage.backfill_in_flight(),
        1,
        "A still holds the page and can still acknowledge it"
    );
}

/// FINDING 4 specifically. A receiver that subscribes AFTER the send must not
/// be counted as a possible reader of it. The stamp is taken inside the
/// delivery lock at send time, so a post-hoc read of the numbering - which is
/// what the previous revision did - cannot mis-attribute the batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscriber_that_arrives_after_the_send_is_not_a_possible_reader() {
    let h = delivery_harness(8);
    let a = h.subscribe();
    h.publish("page:0:10");
    let _late = h.subscribe();

    drop(a);
    assert_eq!(
        h.coverage.backfill_in_flight(),
        0,
        "the only receiver that could read the page has gone; a latecomer must not \
             keep it charged"
    );
}

/// FINDING 6. Retiring the last outstanding registration can make the account
/// quiescent, and a `pause` waiter sits on the checkpoint watch rather than on
/// the ledger. The sweep goes through the ledger directly, so it has to
/// republish quiescence itself or the waiter never wakes.
///
/// Current-thread and paused, so "the waiter has parked" is a fact rather
/// than a hope: a spawned task runs deterministically to its first await
/// before control returns here. A multi-threaded version of this passed
/// against the bug, because the waiter had not started when the sweep ran and
/// then found the boundary already gone.
#[tokio::test(start_paused = true)]
async fn a_receiver_drop_sweep_wakes_a_boundary_waiter() {
    use crate::Control as _;

    let h = delivery_harness(8);
    let a = h.subscribe();
    h.publish("page:0:10");

    let waiter_control = h.control.clone();
    let pause = tokio::spawn(async move { waiter_control.pause().await });
    // Let the waiter reach its park on the checkpoint watch, and confirm the
    // outstanding registration really is gating it.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(
        !pause.is_finished(),
        "the outstanding registration must gate the boundary before the sweep"
    );

    drop(a);

    let released = tokio::time::timeout(std::time::Duration::from_secs(5), pause)
        .await
        .expect("the sweep must wake a boundary waiter, not only the capacity gate");
    assert_eq!(
        released.expect("pause task").expect("pause"),
        crate::DurableCheckpointSet::default(),
        "nothing became durable, so nothing may be reported as durable"
    );
}

#[tokio::test(start_paused = true)]
async fn broadcast_lag_releases_boundary_waiters_that_can_never_be_acked() {
    use crate::Control as _;

    let (tx, sentinel) = broadcast::channel(1);
    let control = lag_test_control();
    let mut receiver = ChangesReceiver::new(tx.subscribe(), Some(control.clone()), None);
    control.expect_checkpoint(account_change_checkpoint(b"lost"));
    tx.send(warning_event("overwritten"))
        .expect("receivers live");
    tx.send(warning_event("retained")).expect("receivers live");

    let pause = tokio::spawn({
        let control = control.clone();
        async move { control.pause().await }
    });
    tokio::task::yield_now().await;
    assert!(
        !pause.is_finished(),
        "the outstanding registration must gate the boundary before the lag is observed"
    );

    let _lag = receiver.recv().await.expect("lag becomes an event");
    // Bounded so the regression reports as a failure rather than a
    // hung test: the defect this pins IS an unbounded wait.
    let released = tokio::time::timeout(std::time::Duration::from_secs(5), pause)
        .await
        .expect("a lost batch's registration must stop gating the boundary");
    assert_eq!(
        released.expect("pause task").expect("pause"),
        crate::DurableCheckpointSet::default()
    );
    drop(sentinel);
}

#[tokio::test]
async fn changes_receiver_surfaces_broadcast_lag_as_lag_warning() {
    let (tx, sentinel) = broadcast::channel(1);
    let mut receiver = ChangesReceiver::new(tx.subscribe(), None, None);
    tx.send(warning_event("overwritten"))
        .expect("receivers live");
    tx.send(warning_event("retained")).expect("receivers live");

    let lag = receiver.recv().await.expect("lag becomes an event");
    assert!(matches!(
        lag.event.as_ref(),
        SyncEvent::Warning(warning)
            if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
    ));
    let retained = receiver
        .recv()
        .await
        .expect("retained event follows warning");
    assert!(matches!(
        retained.event.as_ref(),
        SyncEvent::Warning(warning)
            if warning.kind == bifrost_types::WarningKind::Other
    ));
    drop(sentinel);
}

#[test]
fn closing_retained_delivery_drains_then_closes_every_receiver() {
    let h = delivery_harness(4);
    let mut numbered = h.subscribe();
    let mut observer = h.delivery.observe();
    let retained = Arc::clone(&h.delivery);
    let _ = h.delivery.publish_unacked(warning_event("buffered"));
    h.delivery.close();
    retained.close();
    assert!(numbered.try_recv().is_ok());
    assert!(observer.try_recv().is_ok());
    for receiver in [&mut numbered, &mut observer] {
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
    }
    assert!(matches!(
        retained.observe().try_recv(),
        Err(broadcast::error::TryRecvError::Closed)
    ));
    assert!(matches!(
        h.subscribe().try_recv(),
        Err(broadcast::error::TryRecvError::Closed)
    ));
    assert_eq!(
        retained.publish_unacked(warning_event("late")),
        UnackedPublication::NoReceivers
    );
}

#[test]
fn registered_pages_and_markers_are_not_sent_after_invalidation() {
    for marker in [false, true] {
        for discard in [false, true] {
            let h = delivery_harness(4);
            let mut receiver = h.subscribe();
            let scope = CursorScope::Account;
            let fence = h.coverage.scope_fence(&scope);
            let checkpoint = Delivery::backfill_page("registered");
            let claim = crate::cursor::CoverageClaim {
                reports: Vec::new(),
                generation: h.coverage.next_generation(),
            };
            let id = if marker {
                h.control
                    .publish_walk_marker_if_fence(
                        checkpoint.clone(),
                        h.coverage.undelivered_watermark(&scope),
                        fence,
                    )
                    .expect("marker registered")
            } else {
                h.control
                    .publish_checkpoint_if_fence(checkpoint.clone(), claim, fence)
                    .expect("page registered")
            };
            // Stage the exact gap between registration and the guarded send.
            if discard {
                h.coverage.invalidate_scope_backfill(&scope, Some(&id));
                assert_eq!(h.coverage.scope_fence(&scope), fence);
            } else {
                h.coverage.invalidate_scope(&scope);
            }
            let event = MultiplexerEvent {
                scope: EventScope::Cursor(scope.clone()),
                event: Arc::new(SyncEvent::Done(Some(checkpoint.clone()))),
                checkpoint: Some(checkpoint),
                publication: Some(id),
            };
            assert_eq!(
                h.delivery
                    .publish_inventory_if_fence(event, &h.coverage, &scope, fence),
                None
            );
            assert!(matches!(
                receiver.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ));
        }
    }
}

// Registration and sending are separate synchronous operations. Stage a live
// publication in that gap rather than relying on thread scheduling.
#[test]
fn a_superseded_change_inventory_page_is_still_sent() {
    let h = delivery_harness(4);
    let mut receiver = h.subscribe();
    let scope = CursorScope::Account;
    let fence = h.coverage.scope_fence(&scope);
    let checkpoint = account_change_checkpoint(b"inventory");
    let claim = || crate::cursor::CoverageClaim {
        reports: Vec::new(),
        generation: h.coverage.next_generation(),
    };
    let inventory = h
        .control
        .publish_checkpoint_if_fence(checkpoint.clone(), claim(), fence)
        .expect("inventory registered");
    let live = h
        .control
        .publish_checkpoint_if_fence(account_change_checkpoint(b"live"), claim(), fence)
        .expect("live publication supersedes inventory");
    assert_ne!(inventory, live);
    assert_eq!(h.coverage.pending_checkpoints(), 1);
    let event = MultiplexerEvent {
        scope: EventScope::Cursor(scope.clone()),
        event: Arc::new(SyncEvent::Batch(bifrost_types::Batch {
            items: crate::inventory_walk::created(&[bifrost_types::ObjectId("page-item".into())])
                .collect(),
            page_boundary: bifrost_types::PageBoundary::Final,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: Some(checkpoint.clone()),
        })),
        checkpoint: Some(checkpoint.clone()),
        publication: Some(inventory.clone()),
    };
    assert_eq!(
        h.delivery
            .publish_inventory_if_fence(event, &h.coverage, &scope, fence),
        Some(true)
    );
    let received = receiver
        .try_recv()
        .expect("superseded inventory still delivered");
    assert!(matches!(received.event.as_ref(), SyncEvent::Batch(batch) if batch.items.len() == 1));
    assert_eq!(received.checkpoint, Some(checkpoint));
    assert_eq!(received.ack_token(), Some(&inventory));
}
