use super::attach::scope_covers_membership;
use super::backfill::BackfillScan;
use super::bulk::{
    MutationBucket, classify_item_outcome, counters_from_outcomes, queue_unresolved_for_retry,
    should_forward_engine_recovery, unresolved_readback_ids,
};
use super::reattach::accepted_push_scopes;
use super::{
    Arc, ChangeCursor, DynCheckpointStore, WriterHandle, WriterRequest, ack_writer, mpsc, oneshot,
    take_ack_writer,
};
use crate::cursor::store::CheckpointStore;
use crate::types::{WorkerRole, WorkerTask};

fn idle_worker(role: WorkerRole) -> WorkerTask {
    let join = tokio::spawn(async {});
    WorkerTask {
        role,
        abort: join.abort_handle(),
        join,
    }
}

/// Detach's writer-last ordering must survive a reordering of the spawn
/// block. The predecessor took `drained[0]`, which is only the writer
/// because it is spawned first.
#[tokio::test]
async fn the_ack_writer_is_taken_by_role_from_any_position() {
    for writer_position in 0..3 {
        let mut workers: Vec<WorkerTask> = (0..3)
            .map(|index| {
                idle_worker(if index == writer_position {
                    WorkerRole::AckWriter
                } else {
                    WorkerRole::Multiplexer
                })
            })
            .collect();
        let taken = take_ack_writer(&mut workers).expect("the writer is present");
        assert_eq!(taken.role, WorkerRole::AckWriter);
        assert_eq!(workers.len(), 2, "only the writer is removed");
        assert!(
            workers.iter().all(|w| w.role == WorkerRole::Multiplexer),
            "the writer must not be left behind (position {writer_position})"
        );
    }
}

/// An empty or writer-free list must not silently promote a stream
/// worker into the writer's teardown phase.
#[tokio::test]
async fn taking_the_ack_writer_from_a_writerless_list_yields_none() {
    let mut empty: Vec<WorkerTask> = Vec::new();
    assert!(take_ack_writer(&mut empty).is_none());
    let mut streams = vec![idle_worker(WorkerRole::Multiplexer)];
    assert!(take_ack_writer(&mut streams).is_none());
    assert_eq!(streams.len(), 1, "a non-writer list is left intact");
}

use crate::error::Error;
use bifrost_types::{CursorScope, EngineDirective, FolderId, MembershipScope, ObjectType};
use std::collections::HashSet;

/// Harness for driving the account writer directly.
fn writer_harness_observed() -> (
    bifrost_types::AccountId,
    Arc<crate::cursor::InMemoryCheckpointStore>,
    Arc<crate::cursor::PendingCoverage>,
    mpsc::Sender<WriterRequest>,
    tokio::task::JoinHandle<()>,
    crate::control::SyncControl,
) {
    use crate::cursor::InMemoryCheckpointStore;

    let account = bifrost_types::AccountId("acct".into());
    let inner = Arc::new(InMemoryCheckpointStore::new());
    let store: Arc<DynCheckpointStore> = Arc::clone(&inner) as Arc<DynCheckpointStore>;
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let (tx, rx) = mpsc::channel::<WriterRequest>(16);
    let (boundary, _view) = crate::cancel::Boundary::new();
    let (priority, _p) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _b) = tokio::sync::watch::channel(None);
    // The same `PendingCoverage` backs the control's publication ledger, as
    // it does in `attach`: a harness with two separate ledgers would never
    // observe a boundary registration being retired.
    let control = crate::control::SyncControl::new_with_publications(
        account.clone(),
        boundary,
        priority,
        bandwidth,
        Arc::clone(&coverage),
    );
    let observed = control.clone();
    let writer = tokio::spawn(ack_writer(
        account.clone(),
        store,
        control,
        Arc::clone(&coverage),
        rx,
    ));
    // The watch senders must outlive the writer task.
    std::mem::forget((_view, _p, _b));
    (account, inner, coverage, tx, writer, observed)
}

/// A backfill checkpoint on one partition of one scope.
fn lane_page(scope: &CursorScope, partition: &str, done: u64) -> bifrost_types::Checkpoint {
    bifrost_types::Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
        scope: scope.clone(),
        partition: bifrost_types::Partition(partition.as_bytes().to_vec()),
        progress_marker: None,
        progress: bifrost_types::BackfillProgress {
            items_done: done,
            items_estimated: None,
        },
        envelope_version: 1,
    })
}

/// Publish a completion MARKER the way `emit_backfill_complete` does: the walk's
/// undelivered watermark is stamped into the receipt as the publication is
/// minted, so it survives retirement and supersession.
fn publish_a_marker(
    coverage: &crate::cursor::PendingCoverage,
    checkpoint: &bifrost_types::Checkpoint,
    walk_watermark: u64,
) -> crate::cursor::PublicationId {
    let id = coverage.register_walk_marker(
        checkpoint.clone(),
        crate::cursor::CoverageClaim {
            reports: Vec::new(),
            generation: 0,
        },
        walk_watermark,
    );
    coverage.mark_delivered(&id, 1);
    id
}

/// Publish a backfill page the way the runner does, and mark it delivered.
fn publish_a_page(
    control: &crate::control::SyncControl,
    coverage: &crate::cursor::PendingCoverage,
    checkpoint: &bifrost_types::Checkpoint,
) -> crate::cursor::PublicationId {
    let id = control.publish_checkpoint_without_report(checkpoint.clone(), 0);
    coverage.mark_delivered(&id, 1);
    id
}

/// A durable scope reset retires that scope's publications; the WRITER is what
/// has to make that happen, on both of its invalidation passes, and freeing the
/// backfill bound is a consequence of the retirement rather than a second step.
///
/// Driven through a real `ack_writer` on purpose: a test that called
/// `invalidate_scope` itself would pass with the writer's wiring deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scope_reset_frees_the_backfill_bound_through_the_writer() {
    let (_account, _store, coverage, tx, writer, control) = writer_harness_observed();
    let reset_scope = CursorScope::Account;
    let sibling = CursorScope::Type(ObjectType::Email);

    // Two pages of ONE partition, so the survivor carries a SUBSUMED page. That
    // shape is the finding: an id list taken from the ledger names only the
    // survivor, and a release keyed on those ids leaves the subsumed page
    // charged for ever.
    publish_a_page(&control, &coverage, &lane_page(&reset_scope, "p:0:10", 1));
    publish_a_page(&control, &coverage, &lane_page(&reset_scope, "p:0:10", 2));
    publish_a_page(&control, &coverage, &lane_page(&sibling, "p:0:10", 1));
    assert_eq!(coverage.backfill_in_flight(), 3);

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ResetScope {
        scope: reset_scope.clone(),
        delete_backfill: false,
        done,
    })
    .await
    .expect("writer accepts the reset");
    wait.await.expect("writer answers").expect("reset succeeds");

    assert_eq!(
        coverage.backfill_in_flight(),
        1,
        "both of the reset scope's pages come back, the subsumed one included; the \
         sibling scope's does not"
    );

    drop(tx);
    let _ = writer.await;
}

/// Discarding a walk's backfill progress must also FENCE that walk, or a late
/// acknowledgement rebuilds exactly the resume position the discard removed.
///
/// The discard exists because the walk left a hole and its rows point past it.
/// A replacement consumer holding one of that walk's later pages can let the
/// delete finish and acknowledge afterwards in perfectly good faith - and an
/// unfenced writer would honour it, write the row back, and hand the next attach
/// a position beyond the pages nobody received. Driven through a real
/// `ack_writer` on purpose: a test that called `invalidate_scope` itself would
/// pass with the writer's wiring deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_discarded_backfill_attempt_cannot_be_rebuilt_by_a_late_ack() {
    let (account, store, coverage, tx, writer, control) = writer_harness_observed();
    let scope = CursorScope::Account;
    let checkpoint = lane_page(&scope, "page:10:20", 5);
    let publication = publish_a_page(&control, &coverage, &checkpoint);

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::DiscardBackfillProgress {
        scope: scope.clone(),
        done,
    })
    .await
    .expect("writer accepts the discard");
    wait.await
        .expect("writer answers")
        .expect("the discard succeeds");

    // The page was in the consumer's hand the whole time, and it answers now.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(crate::multiplexer::AckRequest {
        scope: scope.clone(),
        checkpoint,
        publication: Some(publication),
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer accepts the request");
    let answer = wait.await.expect("writer answers");
    assert!(
        answer.is_err(),
        "an acknowledgement of a discarded attempt's page must be refused, exactly as a \
         reset's is; got {answer:?}"
    );

    assert!(
        store
            .get_backfill(&account, &scope)
            .await
            .expect("store read")
            .is_none(),
        "and no resume position may be rebuilt: the row the discard removed is back, so \
         the next attach resumes past the very hole the discard existed to re-walk"
    );

    drop(tx);
    let _ = writer.await;
}

/// The discard fences the BACKFILL lane and nothing else.
///
/// Fencing the whole scope refused a replacement consumer's acknowledgement of
/// live batches it had genuinely received - an `Unknown` no retry can satisfy,
/// with no reconciliation instruction, after which the live cursor advances past
/// changes nothing replayed. The discard is about this scope's backfill rows, so
/// both halves are asserted here: the live acknowledgement still lands durably,
/// and the discarded attempt's page is still refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_discard_fences_the_backfill_lane_and_leaves_live_acks_alone() {
    let (account, store, coverage, tx, writer, control) = writer_harness_observed();
    let scope = CursorScope::Folder(FolderId("shared".into()));
    let live = bifrost_types::ChangeCursor {
        scope: scope.clone(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: 1,
            bytes: vec![7],
        },
        advanced_through: None,
        envelope_version: 1,
    };
    let live_checkpoint = bifrost_types::Checkpoint::Change(live);
    let live_publication = control.publish_checkpoint_without_report(live_checkpoint.clone(), 0);
    let page = lane_page(&scope, "page:10:20", 5);
    let page_publication = publish_a_page(&control, &coverage, &page);

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::DiscardBackfillProgress {
        scope: scope.clone(),
        done,
    })
    .await
    .expect("writer accepts the discard");
    wait.await
        .expect("writer answers")
        .expect("the discard succeeds");

    // The live batch was received before the discard and is acknowledged after
    // it. It has nothing to do with the backfill rows.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(crate::multiplexer::AckRequest {
        scope: scope.clone(),
        checkpoint: live_checkpoint,
        publication: Some(live_publication),
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer accepts the request");
    wait.await
        .expect("writer answers")
        .expect("a live-lane acknowledgement must survive a backfill discard");
    assert!(
        store
            .get_change_cursor(&account, &scope)
            .await
            .expect("store read")
            .is_some(),
        "and it must be durable: refusing it advances the live cursor past changes \
         nothing replayed"
    );

    // The discarded attempt's page is still refused.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(crate::multiplexer::AckRequest {
        scope: scope.clone(),
        checkpoint: page,
        publication: Some(page_publication),
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer accepts the request");
    assert!(
        wait.await.expect("writer answers").is_err(),
        "a page of the discarded attempt must still be refused, or it rebuilds the \
         resume position the discard removed"
    );

    drop(tx);
    let _ = writer.await;
}

/// A completion marker's acknowledgement survives a transient STORE FAILURE.
///
/// The engine tells the consumer to retry, and `AckFailure::Store` retires the
/// marker's registration on the way out - correctly, since the batch is no longer
/// in flight. The retry then resolves through the publication receipt, and a
/// watermark that had lived on the boundary entry was gone by then: read as "this
/// walk cannot be vouched for", refused, and answered by DELETING every backfill
/// row of the scope and fencing the attempt, while the consumer was told `Ok`.
/// The scope was already settled and the watermark had not moved, so nothing
/// re-walked it that attachment; the next attach re-walked the whole scope.
///
/// The reading rides the RECEIPT, so retirement cannot take it away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marker_ack_retried_after_a_store_failure_keeps_the_scopes_rows() {
    // Armed only once the page below is durable, so the failure lands on the
    // MARKER's write - the one the retry has to be able to repeat.
    let failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store: Arc<DynCheckpointStore> = Arc::new(FailingOnceStore {
        inner: crate::cursor::InMemoryCheckpointStore::new(),
        remaining: Arc::clone(&failures),
    });
    let (account, coverage, tx, writer) = writer_harness_over(Arc::clone(&store));
    let scope = email_scope();

    // A page of the walk, acknowledged and durable.
    let page = backfill_checkpoint(bifrost_types::Partition(b"page:0:10".to_vec()));
    let page_publication = register(
        &coverage,
        &page,
        crate::cursor::CoverageClaim {
            reports: Vec::new(),
            generation: 0,
        },
    );
    ack(&tx, page, Some(page_publication))
        .await
        .expect("the page persists");

    failures.store(1, std::sync::atomic::Ordering::SeqCst);

    // The marker, published under a watermark that never moves: this walk is
    // whole and its completion is legitimate.
    let marker = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    let marker_publication =
        publish_a_marker(&coverage, &marker, coverage.undelivered_watermark(&scope));

    // The store fails once. The consumer is told so and retries.
    assert!(
        ack(&tx, marker.clone(), Some(marker_publication.clone()))
            .await
            .is_err(),
        "the transient store failure reaches the consumer"
    );
    ack(&tx, marker, Some(marker_publication))
        .await
        .expect("and the retry is honoured");

    let stored = store
        .get_backfill(&account, &scope)
        .await
        .expect("store read")
        .expect("the retry must have written the marker");
    assert!(
        crate::backfill::partitioner::is_completion_partition(&stored.partition),
        "the retried acknowledgement completes the scope rather than deleting its rows: \
         got {:?}",
        stored.partition
    );

    drop(tx);
    let _ = writer.await;
}

/// The same retry, with a page LOST in between: the reading has to survive the
/// retirement in order to still refuse.
///
/// This is what pins the receipt PLACEMENT on its own. The absence rule and the
/// receipt are two halves of the same repair, and either alone answers the plain
/// store-failure case - so a reading moved back onto the boundary entry, where
/// the store failure's retirement takes it away, passes that test. Here a page is
/// swept undelivered between the failure and the retry: the walk really is holed,
/// and only a reading that survived the retirement can say so. With the reading
/// on the entry the retry reads `Unvouchable` instead, withholds without
/// discarding, and leaves the walk's rows in place as a resume position past the
/// hole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marker_ack_retried_after_a_loss_still_discards_the_walks_rows() {
    let failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store: Arc<DynCheckpointStore> = Arc::new(FailingOnceStore {
        inner: crate::cursor::InMemoryCheckpointStore::new(),
        remaining: Arc::clone(&failures),
    });
    let (account, coverage, tx, writer) = writer_harness_over(Arc::clone(&store));
    let scope = email_scope();

    // A page of the walk, acknowledged and durable: the resume position that must
    // not survive.
    let page = backfill_checkpoint(bifrost_types::Partition(b"page:0:10".to_vec()));
    let page_publication = register(
        &coverage,
        &page,
        crate::cursor::CoverageClaim {
            reports: Vec::new(),
            generation: 0,
        },
    );
    ack(&tx, page, Some(page_publication))
        .await
        .expect("the page persists");

    // A second page, delivered and unacknowledged - what the sweep will strand.
    let stranded = register(
        &coverage,
        &backfill_checkpoint(bifrost_types::Partition(b"page:10:20".to_vec())),
        crate::cursor::CoverageClaim {
            reports: Vec::new(),
            generation: 0,
        },
    );
    coverage.mark_delivered(&stranded, 1);

    let marker = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    let marker_publication =
        publish_a_marker(&coverage, &marker, coverage.undelivered_watermark(&scope));

    failures.store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(
        ack(&tx, marker.clone(), Some(marker_publication.clone()))
            .await
            .is_err(),
        "the transient store failure reaches the consumer"
    );

    // The receiver departs before the consumer retries. Its page reached nobody
    // who can answer for it, so this walk is holed.
    assert_eq!(coverage.release_undelivered(None), 1);

    ack(&tx, marker, Some(marker_publication))
        .await
        .expect("the retry is honoured");

    assert!(
        store
            .get_backfill(&account, &scope)
            .await
            .expect("store read")
            .is_none(),
        "the retry must still see the loss and discard the walk's rows; a reading that \
         vanished with the retirement cannot, and leaves a resume position past the hole"
    );

    drop(tx);
    let _ = writer.await;
}

/// The same arm, one attachment later: an acknowledgement REPLAYED after a
/// detach and re-attach must not delete a legitimately durable marker.
///
/// The receipt travels with the id the consumer persisted, so the reading comes
/// back - taken against a per-scope counter that no longer exists, in a ledger
/// that starts every scope at zero. Comparing it there refuses a marker nothing
/// is wrong with. The id's high bits say which `PendingCoverage` minted it, so
/// the answer is "not a marker this attachment can judge", which refuses nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marker_ack_replayed_after_reattach_leaves_the_durable_marker() {
    let scope = email_scope();
    let marker = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);

    // The PREVIOUS attachment mints the marker, under a watermark of its own.
    let previous = crate::cursor::PendingCoverage::new();
    previous.note_undelivered(&scope, 5);
    let replayed = publish_a_marker(&previous, &marker, previous.undelivered_watermark(&scope));

    // A fresh attachment: new ledger, every scope back at zero - and the marker
    // ALREADY DURABLE, which is the case this test is about. Starting from an
    // empty store would let the replay's own write satisfy the assertion, which
    // is the opposite of what is being pinned.
    let (account, store, coverage, tx, writer) = writer_harness();
    store
        .put_backfill(
            &account,
            match &marker {
                bifrost_types::Checkpoint::Backfill(backfill) => backfill.clone(),
                other => panic!("the marker is a backfill checkpoint: {other:?}"),
            },
        )
        .await
        .expect("seed the durable marker an earlier attachment earned");
    assert_eq!(coverage.undelivered_watermark(&scope), 0);
    assert_eq!(
        coverage.walk_watermark(&replayed),
        None,
        "a prior attachment's id is not one this ledger can interpret"
    );
    ack(&tx, marker, Some(replayed))
        .await
        .expect("a replayed acknowledgement is honoured");

    let stored = store
        .get_backfill(&account, &scope)
        .await
        .expect("store read")
        .expect("the replay must not delete the scope's rows");
    assert!(
        crate::backfill::partitioner::is_completion_partition(&stored.partition),
        "a marker replayed across a re-attach carries a reading this ledger cannot \
         interpret; discarding on it deletes a completion an earlier attachment \
         legitimately earned"
    );

    drop(tx);
    let _ = writer.await;
}

/// The other direction of the same rule: a replay must not CREATE a completion
/// either.
///
/// The interleaving is ordinary. A consumer holds an unacknowledged page, a
/// replacement receives the marker, and the detach beats the replacement's
/// acknowledgement to the writer. On reattach the orchestrator parks on
/// `wait_for_real_subscriber`, the replacement subscribes and flushes its pending
/// acknowledgements, and that replayed marker resolves through the receipt
/// fallback - landing a durable completion BEFORE the fresh walk has even read
/// `get_backfill`. The scope is then skipped and the stranded page is never
/// re-offered. A reading this ledger cannot interpret is not evidence in either
/// direction: withhold, and let the new attachment earn its own marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replayed_marker_does_not_create_a_completion_the_new_attachment_never_earned() {
    let scope = email_scope();
    let marker = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);

    let previous = crate::cursor::PendingCoverage::new();
    let replayed = publish_a_marker(&previous, &marker, previous.undelivered_watermark(&scope));

    // A fresh attachment over an EMPTY store: nothing here has earned anything.
    let (account, store, _coverage, tx, writer) = writer_harness();
    ack(&tx, marker, Some(replayed))
        .await
        .expect("a replayed acknowledgement is honoured");

    let stored = store
        .get_backfill(&account, &scope)
        .await
        .expect("store read");
    assert!(
        !super::backfill::backfill_complete_recorded(stored.as_ref()),
        "a replayed marker must not record a completion this ledger cannot vouch for: \
         the fresh walk reads this row back and answers Skip, so the page the departed \
         consumer stranded is never re-offered. got {stored:?}"
    );

    drop(tx);
    let _ = writer.await;
}

/// A failed delete does NOT settle the request it could not satisfy.
///
/// The rows are exactly where they were, so a request consumed by a delete that
/// did not happen is a repair nobody will attempt again: the teardown drain finds
/// nothing to retry, and the next attach resumes from a position past the hole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_discard_leaves_the_request_for_the_next_attempt() {
    let store: Arc<DynCheckpointStore> = Arc::new(FailingDeleteStore {
        inner: crate::cursor::InMemoryCheckpointStore::new(),
    });
    let (_account, coverage, tx, writer) = writer_harness_over(Arc::clone(&store));
    let scope = email_scope();

    // A loss recorded: the request is outstanding.
    coverage.note_undelivered(&scope, 1);

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::DiscardBackfillProgress {
        scope: scope.clone(),
        done,
    })
    .await
    .expect("writer accepts the discard");
    assert!(
        wait.await.expect("writer answers").is_err(),
        "the store refuses the delete"
    );

    assert!(
        coverage.take_backfill_discard(&scope),
        "the request must still be outstanding after a delete that failed - the rows it \
         was asked to remove are still there, and consuming the request would leave \
         teardown with nothing to retry"
    );

    drop(tx);
    let _ = writer.await;
}

/// A CEILINGED discard does not answer the outstanding request, so it must not
/// take it.
///
/// The ack path's discard is bounded by the refused marker's id - it retires that
/// attempt and no newer one - while its delete takes every row of the scope. A
/// delayed W1 marker acknowledgement arriving during W2 therefore leaves W2's own
/// loss unrepaired: W2's consumer goes on acknowledging pages that rebuild rows
/// past W2's hole, and a detach before W2's walk end would find nothing left to
/// drain. Only an unbounded discard - the orchestrator's, which runs when no
/// newer attempt exists - is the whole repair the request asks for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ceilinged_discard_leaves_the_request_outstanding() {
    let (_account, _store, coverage, tx, writer, _control) = writer_harness_observed();
    let scope = email_scope();

    // W1's marker, published under the reading of its own walk.
    let marker = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    let marker_publication =
        publish_a_marker(&coverage, &marker, coverage.undelivered_watermark(&scope));

    // W2 loses a page: the watermark moves and W2's request is outstanding.
    coverage.note_undelivered(&scope, 1);

    // The delayed W1 acknowledgement is refused and discards - ceilinged at W1's
    // marker.
    ack(&tx, marker, Some(marker_publication))
        .await
        .expect("the consumer's acknowledgement is honoured");

    assert!(
        coverage.take_backfill_discard(&scope),
        "W2's request must still be outstanding: the ceilinged discard retired W1's \
         attempt only, so nothing has answered W2's loss, and clearing it here leaves \
         teardown with nothing to drain"
    );

    drop(tx);
    let _ = writer.await;
}

/// And a delayed acknowledgement of an OLD walk's marker must not DELETE the
/// rows a later attempt earned.
///
/// The ceiling bounds the ledger retirement; `delete_backfill` takes every row of
/// the scope and knows nothing of publication ids. So the refusal that protects
/// the retry's publications was still deleting the retry's durable completion -
/// acknowledged by the consumer, legitimately earned, and the only thing standing
/// between the next attach and a full re-walk of the scope. The fence is the whole
/// of what a ceilinged discard may safely do once a newer attempt has published:
/// the request it cannot answer stays outstanding, and the newer attempt's own
/// repair is what removes rows if any need removing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_marker_ack_leaves_a_later_attempts_rows_alone() {
    let (account, store, coverage, tx, writer, control) = writer_harness_observed();
    let scope = email_scope();

    // W1's marker, published under the reading of its own walk.
    let stale = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    let stale_publication =
        publish_a_marker(&coverage, &stale, coverage.undelivered_watermark(&scope));

    // W1 loses a page, so the delayed acknowledgement below will be refused.
    coverage.note_undelivered(&scope, 1);

    // W2 is under way: a page of its own, acknowledged and durable. That row is
    // W2's resume position, and it is the only thing standing between the next
    // attach and a re-walk of everything W2 has already offered.
    let retry_page = lane_page(&scope, "page:0:10", 5);
    let retry_publication = publish_a_page(&control, &coverage, &retry_page);
    ack(&tx, retry_page, Some(retry_publication))
        .await
        .expect("the retry's page persists");
    assert!(
        store
            .get_backfill(&account, &scope)
            .await
            .expect("store read")
            .is_some(),
        "the staging is only meaningful with the later attempt's row durable"
    );

    // Only now does W1's marker acknowledgement arrive.
    ack(&tx, stale, Some(stale_publication))
        .await
        .expect("the consumer's acknowledgement is honoured");

    let stored = store
        .get_backfill(&account, &scope)
        .await
        .expect("store read");
    assert!(
        stored.is_some(),
        "the later attempt's rows are not the refused attempt's to delete: taking them \
         throws away everything W2 has offered so far and costs a walk from scratch"
    );

    drop(tx);
    let _ = writer.await;
}

/// A delayed acknowledgement of an OLD walk's marker must not retire the retry
/// that has already started.
///
/// The refusal is right and the discard it triggers is right; its reach is the
/// question. The marker is the last thing its walk published, so everything of
/// that attempt sits at or below its id - and a retry that is already publishing
/// sits above it. Retiring those too costs the retry its in-flight pages and
/// hands the consumer a stream of refusals it can do nothing about.
///
/// A LEDGER-level pin of the ceiling, and staged as one: the orchestrator cannot
/// actually produce this exact state, because its pre-retry discard retires the
/// old attempt before the retry publishes anything. What is being pinned is the
/// reach of `invalidate_scope_backfill`'s bound, which the writer's own path
/// reaches from a delayed acknowledgement. The refusal here goes through the
/// watermark COMPARISON - asserted below - and not through the "no reading at
/// all" arm, which refuses nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_marker_ack_leaves_the_retrys_pages_alone() {
    let (_account, _store, coverage, tx, writer, control) = writer_harness_observed();
    let scope = CursorScope::Account;

    // The old walk: a page, then its marker, published under a watermark that
    // then moves - a page of that walk reached nobody.
    publish_a_page(&control, &coverage, &lane_page(&scope, "page:0:10", 1));
    let marker = bifrost_types::Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
        scope: scope.clone(),
        partition: crate::backfill::partitioner::completion_partition(),
        progress_marker: None,
        progress: bifrost_types::BackfillProgress {
            items_done: 99,
            items_estimated: None,
        },
        envelope_version: crate::cursor::ENGINE_VERSION,
    });
    let marker_publication =
        publish_a_marker(&coverage, &marker, coverage.undelivered_watermark(&scope));
    coverage.note_undelivered(&scope, 1);

    // The RETRY is already under way: a page of its own, and its own marker
    // registered and tagged - both of which the stale acknowledgement must leave
    // exactly as it found them.
    let retry_page = lane_page(&scope, "page:20:30", 2);
    let retry_publication = publish_a_page(&control, &coverage, &retry_page);
    let retry_marker = bifrost_types::Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
        scope: scope.clone(),
        partition: crate::backfill::partitioner::completion_partition(),
        progress_marker: None,
        progress: bifrost_types::BackfillProgress {
            items_done: 199,
            items_estimated: None,
        },
        envelope_version: crate::cursor::ENGINE_VERSION,
    });
    let retry_tag = coverage.undelivered_watermark(&scope);
    let _retry_marker_publication = publish_a_marker(&coverage, &retry_marker, retry_tag);
    let in_flight_before = coverage.backfill_in_flight();

    // STAGING, not an assertion about the retirement: the reading lives on an
    // immutable receipt field, so neither of these can be moved by anything under
    // test. What they establish is that the acknowledgement below takes the
    // watermark COMPARISON arm - the old marker still reports the reading it was
    // published under, and the scope's has moved - rather than the "no reading at
    // all" arm, which refuses nothing and would leave the charge assertion
    // measuring a discard that never ran.
    assert_eq!(coverage.walk_watermark(&marker_publication), Some(0));
    assert_ne!(coverage.undelivered_watermark(&scope), 0);

    // Now the stale marker is acknowledged. It is refused, and its attempt is
    // discarded.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(crate::multiplexer::AckRequest {
        scope: scope.clone(),
        checkpoint: marker,
        publication: Some(marker_publication),
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer accepts the request");
    wait.await
        .expect("writer answers")
        .expect("the consumer's acknowledgement is honoured");

    // The retry's own registrations are untouched. Acknowledgeability alone does
    // not prove that: `claim_checkpoint` falls back to the publication RECEIPT,
    // so a page whose claim was retired still answers - which is exactly why the
    // earlier version of this test passed against an unbounded retirement. The
    // CHARGE and the tagged watermark are what the retirement takes away.
    assert_eq!(
        coverage.backfill_in_flight(),
        in_flight_before - 2,
        "exactly the refused marker's own attempt loses its charge - its page and the \
         marker itself - and nothing of the retry's: taking the retry's pages too stops \
         the bound binding for as many pages as it held"
    );

    // The retry's page must still be acknowledgeable.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(crate::multiplexer::AckRequest {
        scope,
        checkpoint: retry_page,
        publication: Some(retry_publication),
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer accepts the request");
    wait.await.expect("writer answers").expect(
        "a page the RETRY published is newer than the marker being refused and must \
                 stay acknowledgeable",
    );

    drop(tx);
    let _ = writer.await;
}

/// A completion marker acknowledged with NO publication settles nothing.
///
/// Every check that stands between an acknowledgement and a durable completion -
/// the walk's reading, the scope's reset fence, the claim lookup - is keyed by
/// publication id, so an acknowledgement that carries none skips all of them and
/// the marker becomes durable on the caller's say-so alone. That marker makes
/// every later attach answer `Skip`, which is the one outcome that cannot be
/// walked back. Nothing in the engine acknowledges a marker this way; a consumer
/// that drops the id can, and it must not be able to retire a scope with it.
///
/// A page acknowledged the same way still persists, deliberately: a page row is a
/// resume position, so the cost of an unvouchable one is a re-read rather than a
/// scope nobody walks again.
#[tokio::test]
async fn a_completion_marker_acknowledged_without_a_publication_is_withheld() {
    let (account, store, _coverage, tx, writer) = writer_harness();

    let page = backfill_checkpoint(bifrost_types::Partition(b"page:0:10".to_vec()));
    ack(&tx, page, None).await.expect("the page is honoured");
    assert!(
        store
            .get_backfill(&account, &email_scope())
            .await
            .expect("store read")
            .is_some(),
        "a page acknowledged without a publication still records its position"
    );

    let marker = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    ack(&tx, marker, None)
        .await
        .expect("the consumer's acknowledgement is honoured");

    let stored = store
        .get_backfill(&account, &email_scope())
        .await
        .expect("store read");
    assert!(
        !super::backfill::backfill_complete_recorded(stored.as_ref()),
        "a completion nothing can vouch for must not become durable: it retires the \
         scope for every later attach. got {stored:?}"
    );

    drop(tx);
    let _ = writer.await;
}

/// Open debt must not MASK the discard a lost walk needs.
///
/// The two refusals answer different questions and only one of them is about the
/// walk. Evaluating debt first withholds the marker - which looks like the right
/// outcome - while the walk's positional rows, which point past the page nobody
/// received, sit in the store untouched; a detach before the in-memory retry then
/// hands a fresh attach a resume position beyond the hole.
#[tokio::test]
async fn open_debt_does_not_mask_the_discard_a_lost_walk_needs() {
    let (account, store, coverage, tx, writer) = writer_harness();

    // A degraded page, acknowledged: that is BOTH the scope's open debt and the
    // durable row this walk leaves behind.
    let page = backfill_checkpoint(bifrost_types::Partition(b"page:0:10".to_vec()));
    let page_publication = register(
        &coverage,
        &page,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable("broken")],
            ),
            1,
        ),
    );
    ack(&tx, page, Some(page_publication))
        .await
        .expect("ack persisted");
    assert!(
        store
            .get_backfill(&account, &email_scope())
            .await
            .expect("store read")
            .is_some(),
        "the walk left a durable row, which is what the discard has to remove"
    );

    // The walk's completion marker, published under a watermark that then moves:
    // a page of this walk reached nobody.
    let marker = backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    let marker_publication = publish_a_marker(
        &coverage,
        &marker,
        coverage.undelivered_watermark(&email_scope()),
    );
    coverage.note_undelivered(&email_scope(), 1);

    // Both conditions now hold. The consumer's acknowledgement is honoured; the
    // marker is withheld either way, and the rows must go.
    ack(&tx, marker, Some(marker_publication))
        .await
        .expect("the consumer's acknowledgement is honoured");

    assert!(
        store
            .get_backfill(&account, &email_scope())
            .await
            .expect("store read")
            .is_none(),
        "a walk that lost a page must have its rows discarded whatever the scope's debt \
         state; debt is a property of the scope, and this is a property of the walk"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// The common shape: the control handle is only needed by the tests that
/// assert what a boundary waiter would be told.
fn writer_harness() -> (
    bifrost_types::AccountId,
    Arc<crate::cursor::InMemoryCheckpointStore>,
    Arc<crate::cursor::PendingCoverage>,
    mpsc::Sender<WriterRequest>,
    tokio::task::JoinHandle<()>,
) {
    let (account, store, coverage, tx, writer, _control) = writer_harness_observed();
    (account, store, coverage, tx, writer)
}

/// Same writer, but over a caller-supplied store so a test can observe or
/// stall an individual durable call.
fn writer_harness_over(
    store: Arc<DynCheckpointStore>,
) -> (
    bifrost_types::AccountId,
    Arc<crate::cursor::PendingCoverage>,
    mpsc::Sender<WriterRequest>,
    tokio::task::JoinHandle<()>,
) {
    let account = bifrost_types::AccountId("acct".into());
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let (tx, rx) = mpsc::channel::<WriterRequest>(16);
    let (boundary, _view) = crate::cancel::Boundary::new();
    let (priority, _p) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _b) = tokio::sync::watch::channel(None);
    // The control's publication ledger IS the coverage handed to the writer, as
    // `attach` wires it. Two separate ledgers here would mean the writer's
    // `retire_publication` touches nothing a test can observe - and a test that
    // stages a retirement would silently stage nothing.
    let control = crate::control::SyncControl::new_with_publications(
        account.clone(),
        boundary,
        priority,
        bandwidth,
        Arc::clone(&coverage),
    );
    let writer = tokio::spawn(ack_writer(
        account.clone(),
        store,
        control,
        Arc::clone(&coverage),
        rx,
    ));
    std::mem::forget((_view, _p, _b));
    (account, coverage, tx, writer)
}

/// A page published INSIDE the reset's own store awaits, staged rather than
/// argued.
///
/// The separate-permit design lost this twice over. It released the lane once up
/// front, which woke the parked producer; the still-running walk then published
/// inside the await window and took a permit that the reset's second pass
/// retired-and-fenced but never returned. Its successor released by the ids the
/// ledger reported retired, and hit a narrower version: a publication registered
/// after the retirement kept its permit anyway.
///
/// Both vanish when the registration IS the capacity, and this pins that it does:
/// the page is registered while the writer is parked, and the second invalidation
/// pass frees it because it removes it.
///
/// `GatedDeleteStore` parks the writer inside its deletes, which is the only
/// place this interleaving can be produced on demand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_published_inside_a_resets_await_window_frees_the_bound() {
    let inner = Arc::new(crate::cursor::InMemoryCheckpointStore::new());
    let gate_store = Arc::new(GatedDeleteStore::new(Arc::clone(&inner)));
    let store: Arc<DynCheckpointStore> = Arc::clone(&gate_store) as Arc<DynCheckpointStore>;
    let account = bifrost_types::AccountId("acct".into());
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let (tx, rx) = mpsc::channel::<WriterRequest>(16);
    let (boundary, _view) = crate::cancel::Boundary::new();
    let (priority, _p) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _b) = tokio::sync::watch::channel(None);
    let control = crate::control::SyncControl::new_with_publications(
        account.clone(),
        boundary,
        priority,
        bandwidth,
        Arc::clone(&coverage),
    );
    let observed = control.clone();
    let writer = tokio::spawn(ack_writer(
        account.clone(),
        store,
        control,
        Arc::clone(&coverage),
        rx,
    ));
    std::mem::forget((_view, _p, _b));

    let scope = email_scope();
    publish_a_page(&observed, &coverage, &lane_page(&scope, "page:0:10", 1));
    assert_eq!(coverage.backfill_in_flight(), 1);

    let handle = WriterHandle::new(tx.clone());
    let reset_scope = scope.clone();
    let reset = tokio::spawn(async move { handle.reset_scope_for_restart(reset_scope).await });

    // Park the writer inside the reset's deletes - after its first invalidation
    // pass, before its second.
    gate_store
        .entered
        .acquire()
        .await
        .expect("writer reached the delete")
        .forget();

    // The still-running walk publishes TWO more pages of one partition right
    // here, so the second is a survivor carrying a subsumed predecessor.
    publish_a_page(&observed, &coverage, &lane_page(&scope, "page:10:20", 1));
    publish_a_page(&observed, &coverage, &lane_page(&scope, "page:10:20", 2));
    assert_eq!(
        coverage.backfill_in_flight(),
        2,
        "the in-window pages are charged"
    );

    gate_store.release.add_permits(1);
    reset.await.expect("reset task").expect("reset");

    assert_eq!(
        coverage.backfill_in_flight(),
        0,
        "the reset retired them, and the retirement is what frees the bound; nothing \
         can ever acknowledge a fenced page, so leaving it charged is permanent"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// FINDING 8. A token from one lane presented with another lane's checkpoint is
/// REFUSED, and a refusal is not evidence of delivery: nothing may be retired,
/// and the live publication the token names must keep its capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_acknowledgement_frees_no_capacity() {
    let (_account, _store, coverage, tx, writer, control) = writer_harness_observed();
    let x = lane_page(&CursorScope::Account, "page:0:10", 1);
    let y = lane_page(&CursorScope::Type(ObjectType::Email), "page:0:10", 1);
    publish_a_page(&control, &coverage, &x);
    let y_id = publish_a_page(&control, &coverage, &y);
    assert_eq!(coverage.backfill_in_flight(), 2);

    // Y's token, X's checkpoint. The lanes disagree, so the ledger refuses.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(crate::multiplexer::AckRequest {
        scope: CursorScope::Account,
        checkpoint: x,
        publication: Some(y_id),
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer accepts the request");
    assert!(
        wait.await.expect("writer answers").is_err(),
        "a lane mismatch must be refused"
    );

    assert_eq!(
        coverage.backfill_in_flight(),
        2,
        "a refused acknowledgement is evidence about the caller, not about delivery; \
         retiring on it frees a live publication's capacity"
    );

    drop(tx);
    let _ = writer.await;
}

/// A store failure is the other half of that pair, and DOES retire: the consumer
/// took delivery and answered, so the batch is no longer in flight even though
/// nothing became durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_store_write_still_frees_the_bound() {
    let store: Arc<DynCheckpointStore> = Arc::new(FailingTransitionStore);
    let account = bifrost_types::AccountId("acct".into());
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let (tx, rx) = mpsc::channel::<WriterRequest>(16);
    let (boundary, _view) = crate::cancel::Boundary::new();
    let (priority, _p) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _b) = tokio::sync::watch::channel(None);
    let control = crate::control::SyncControl::new_with_publications(
        account.clone(),
        boundary,
        priority,
        bandwidth,
        Arc::clone(&coverage),
    );
    let observed = control.clone();
    let writer = tokio::spawn(ack_writer(
        account.clone(),
        store,
        control,
        Arc::clone(&coverage),
        rx,
    ));
    std::mem::forget((_view, _p, _b));

    // TWO pages of one partition, so the second SUBSUMES the first - and the
    // acknowledgement below names the SUPERSEDED one. Acknowledging the survivor
    // instead passes without the subsumed-id branch in `retire_publication`,
    // because removing the survivor's entry takes the whole charge with it; only
    // an acknowledgement of the subsumed page can tell the two apart.
    let first = lane_page(&CursorScope::Account, "p", 1);
    let first_id = publish_a_page(&observed, &coverage, &first);
    publish_a_page(
        &observed,
        &coverage,
        &lane_page(&CursorScope::Account, "p", 2),
    );
    assert_eq!(coverage.backfill_in_flight(), 2);

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(crate::multiplexer::AckRequest {
        scope: CursorScope::Account,
        checkpoint: first,
        publication: Some(first_id),
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer accepts the request");
    assert!(wait.await.expect("writer answers").is_err());

    assert_eq!(
        coverage.backfill_in_flight(),
        1,
        "the consumer took delivery of the SUPERSEDED page and answered for it, so its \
         charge must come off the survivor; the survivor's own page is still owed. \
         Holding the bound over a transient store failure parks cold start for the \
         life of the attachment"
    );

    drop(tx);
    let _ = writer.await;
}

/// A store whose `apply_transition` always fails, so a test can separate "the
/// acknowledgement was refused" from "the write did not land".
struct FailingTransitionStore;

impl crate::cursor::store::CheckpointStore for FailingTransitionStore {
    fn put_change_cursor<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _cursor: bifrost_types::ChangeCursor,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Err(Error::CheckpointStore("store is down".into())) })
    }

    fn get_change_cursor<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<bifrost_types::ChangeCursor>, Error>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async { Ok(None) })
    }

    fn put_backfill<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _checkpoint: bifrost_types::BackfillCheckpoint,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Err(Error::CheckpointStore("store is down".into())) })
    }

    fn get_backfill<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, Error>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async { Ok(None) })
    }

    fn delete_change_cursor<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn delete_backfill<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn apply_transition<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _transition: crate::cursor::store::CheckpointTransition,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Err(Error::CheckpointStore("store is down".into())) })
    }

    fn put_ledger<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _ledger: crate::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn get_ledger<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<crate::cursor::DebtLedger, Error>> + Send + 'a>,
    > {
        Box::pin(async { Ok(crate::cursor::DebtLedger::default()) })
    }
}

/// Wraps a store and parks inside `delete_change_cursor` until released, so
/// a test can act inside the writer's await window rather than around it.
/// An in-memory store whose `delete_backfill` always FAILS.
struct FailingDeleteStore {
    inner: crate::cursor::InMemoryCheckpointStore,
}

impl CheckpointStore for FailingDeleteStore {
    fn apply_transition<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        transition: crate::cursor::store::CheckpointTransition,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.apply_transition(account, transition)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>,
    > {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: crate::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<crate::cursor::DebtLedger, Error>> + Send + 'a>,
    > {
        self.inner.get_ledger(account)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        _account: &'a bifrost_types::AccountId,
        _scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Err(Error::CheckpointStore("delete refused".into())) })
    }
}

/// An in-memory store whose first `remaining` checkpoint writes FAIL.
///
/// The transient store error the ack path is built around: the consumer is told,
/// and retries with the same publication id.
struct FailingOnceStore {
    inner: crate::cursor::InMemoryCheckpointStore,
    remaining: Arc<std::sync::atomic::AtomicUsize>,
}

impl CheckpointStore for FailingOnceStore {
    fn apply_transition<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        transition: crate::cursor::store::CheckpointTransition,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        if self
            .remaining
            .try_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |left| left.checked_sub(1),
            )
            .is_ok()
        {
            return Box::pin(async {
                Err(Error::CheckpointStore("transient store failure".into()))
            });
        }
        self.inner.apply_transition(account, transition)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>,
    > {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: crate::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<crate::cursor::DebtLedger, Error>> + Send + 'a>,
    > {
        self.inner.get_ledger(account)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

struct GatedDeleteStore {
    inner: Arc<crate::cursor::InMemoryCheckpointStore>,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl GatedDeleteStore {
    fn new(inner: Arc<crate::cursor::InMemoryCheckpointStore>) -> Self {
        Self {
            inner,
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

impl CheckpointStore for GatedDeleteStore {
    fn apply_transition<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        transition: crate::cursor::store::CheckpointTransition,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.apply_transition(account, transition)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>,
    > {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: crate::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<crate::cursor::DebtLedger, Error>> + Send + 'a>,
    > {
        self.inner.get_ledger(account)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            self.entered.add_permits(1);
            let permit = self.release.acquire().await.expect("gate open");
            permit.forget();
            self.inner.delete_change_cursor(account, scope).await
        })
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

fn email_scope() -> CursorScope {
    CursorScope::Type(ObjectType::Email)
}

fn change_cursor(bytes: &[u8]) -> ChangeCursor {
    ChangeCursor {
        scope: email_scope(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: crate::cursor::ENGINE_VERSION,
            bytes: bytes.to_vec(),
        },
        advanced_through: None,
        envelope_version: crate::cursor::ENGINE_VERSION,
    }
}

fn degraded_claim(key: &str) -> crate::cursor::CoverageClaim {
    crate::cursor::CoverageClaim::new(
        bifrost_types::InventoryCoverageReport::degraded(
            bifrost_types::CoverageDomain::full(email_scope()),
            vec![unrepresentable(key)],
        ),
        1,
    )
}

fn unrepresentable(key: &str) -> bifrost_types::InventoryObligation {
    let error = bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        bifrost_types::Cause::Request(bifrost_types::RequestCause::Malformed {
            detail: bifrost_types::DiagnosticText::support_only("unrepresentable"),
        }),
    )
    .try_build()
    .expect("valid account error classification");
    bifrost_types::InventoryObligation::Object {
        key: bifrost_types::ObligationKey(key.as_bytes().to_vec()),
        id: bifrost_types::ObjectId(key.into()),
        error,
        repair: Vec::new(),
    }
}

fn backfill_checkpoint(partition: bifrost_types::Partition) -> bifrost_types::Checkpoint {
    backfill_checkpoint_at(partition, 1)
}

/// `items_done` matters: `get_backfill` returns the row with the greatest
/// count, and production sets the completion marker one past the total
/// precisely so it wins that query. A test that gave the sentinel the same
/// count as a page row could see the page row come back and pass without
/// ever checking whether the sentinel was written.
fn backfill_checkpoint_at(
    partition: bifrost_types::Partition,
    items_done: u64,
) -> bifrost_types::Checkpoint {
    bifrost_types::Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
        scope: email_scope(),
        partition,
        progress_marker: None,
        progress: bifrost_types::BackfillProgress {
            items_done,
            items_estimated: None,
        },
        envelope_version: crate::cursor::ENGINE_VERSION,
    })
}

async fn ack(
    tx: &mpsc::Sender<WriterRequest>,
    checkpoint: bifrost_types::Checkpoint,
    publication: Option<crate::cursor::PublicationId>,
) -> Result<(), Error> {
    use crate::multiplexer::AckRequest;
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(AckRequest {
        scope: email_scope(),
        checkpoint,
        publication,
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer alive");
    wait.await.expect("writer answered")
}

/// Register a checkpoint publication the way a producer does, so the
/// acknowledgement resolves in the same lane the writer looks it up in.
fn register(
    coverage: &crate::cursor::PendingCoverage,
    checkpoint: &bifrost_types::Checkpoint,
    claim: crate::cursor::CoverageClaim,
) -> crate::cursor::PublicationId {
    coverage.register(checkpoint.clone(), claim)
}

/// An acknowledged DEGRADED backfill page must leave its obligations in the
/// durable ledger.
///
/// This is the defect that started the whole redesign. `BackfillRunner`
/// read `batch.coverage` only to clear a local `complete` flag and never
/// registered it anywhere, while the writer built its durable record from a
/// scope-keyed map that defaulted to "complete" for any scope nothing had
/// reported on. So an acknowledged degraded page persisted a record
/// certifying full coverage over objects nothing had recorded - a durable
/// lie, and precisely the invariant the coverage machinery exists to
/// enforce.
#[tokio::test]
async fn an_acknowledged_degraded_page_persists_its_debt() {
    let (account, store, coverage, tx, writer) = writer_harness();

    let checkpoint = backfill_checkpoint(bifrost_types::Partition(b"page-0".to_vec()));
    let publication = register(
        &coverage,
        &checkpoint,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable("broken")],
            ),
            1,
        ),
    );
    ack(&tx, checkpoint, Some(publication))
        .await
        .expect("ack persisted");

    let ledger = store.get_ledger(&account).await.expect("ledger read");
    assert_eq!(
        ledger.open_debt().count(),
        1,
        "the page's obligation must survive in the durable ledger"
    );
    assert!(
        !ledger.completion_permitted(&email_scope()),
        "a scope with open debt must not be eligible for a completion sentinel"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// Routine cursor invalidation must NOT drop backfill state.
///
/// `delete_backfill` drops the completion marker, and that marker is the
/// only thing that stops the next attach from re-walking and re-hydrating
/// the scope's whole history. `RestartScope` fires on every ordinary cursor
/// invalidation, so deleting it there turns each one into a full historical
/// inventory pass for no schema reason.
#[tokio::test]
async fn a_restart_reset_preserves_completed_backfill() {
    let inner = Arc::new(crate::cursor::InMemoryCheckpointStore::new());
    let store: Arc<DynCheckpointStore> = Arc::clone(&inner) as Arc<DynCheckpointStore>;
    let (account, _coverage, tx, writer) = writer_harness_over(store);

    let bifrost_types::Checkpoint::Backfill(marker) =
        backfill_checkpoint_at(bifrost_types::Partition(b"done".to_vec()), 99)
    else {
        panic!("backfill checkpoint");
    };
    inner
        .put_backfill(&account, marker)
        .await
        .expect("backfill row seeded");
    inner
        .put_change_cursor(&account, change_cursor(b"live"))
        .await
        .expect("cursor seeded");

    WriterHandle::new(tx.clone())
        .reset_scope_for_restart(email_scope())
        .await
        .expect("restart reset");

    assert!(
        inner
            .get_change_cursor(&account, &email_scope())
            .await
            .expect("cursor read")
            .is_none(),
        "a restart must drop the change cursor so the next establish re-runs"
    );
    assert!(
        inner
            .get_backfill(&account, &email_scope())
            .await
            .expect("backfill read")
            .is_some(),
        "a restart must PRESERVE backfill completion; only schema recovery deletes it"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// The other half of the same contract: schema recovery really does delete
/// backfill, because the re-walk is what re-mints ids under the new
/// encoding and the completion marker would make the next attach skip it.
#[tokio::test]
async fn a_schema_recovery_reset_deletes_backfill() {
    let inner = Arc::new(crate::cursor::InMemoryCheckpointStore::new());
    let store: Arc<DynCheckpointStore> = Arc::clone(&inner) as Arc<DynCheckpointStore>;
    let (account, _coverage, tx, writer) = writer_harness_over(store);

    let bifrost_types::Checkpoint::Backfill(marker) =
        backfill_checkpoint_at(bifrost_types::Partition(b"done".to_vec()), 99)
    else {
        panic!("backfill checkpoint");
    };
    inner
        .put_backfill(&account, marker)
        .await
        .expect("backfill row seeded");

    WriterHandle::new(tx.clone())
        .reset_scope_for_schema_recovery(email_scope())
        .await
        .expect("schema reset");

    assert!(
        inner
            .get_backfill(&account, &email_scope())
            .await
            .expect("backfill read")
            .is_none(),
        "schema recovery must drop the completion marker so the re-walk happens"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// A scope that is still running while its reset is in flight registers a
/// further degraded publication. That publication's coverage debt must
/// survive the reset.
///
/// The publication is registered INSIDE the writer's `delete_change_cursor`
/// await, which is the whole point: a reset that snapshots debt first and
/// retires publications after its store calls sees an empty snapshot for
/// this one, retires it anyway, and drops its obligations on the floor.
#[tokio::test]
async fn debt_published_during_a_scope_reset_survives_it() {
    let inner = Arc::new(crate::cursor::InMemoryCheckpointStore::new());
    let gate = Arc::new(GatedDeleteStore::new(Arc::clone(&inner)));
    let store: Arc<DynCheckpointStore> = Arc::clone(&gate) as Arc<DynCheckpointStore>;
    let (account, coverage, tx, writer) = writer_harness_over(store);

    // Registered before the reset: the easy half.
    let before = bifrost_types::Checkpoint::Change(change_cursor(b"before"));
    let _early = coverage.register(before, degraded_claim("before-reset"));

    let handle = WriterHandle::new(tx.clone());
    let reset = tokio::spawn(async move { handle.reset_scope_for_restart(email_scope()).await });

    // Wait until the writer is genuinely parked mid-reset.
    gate.entered
        .acquire()
        .await
        .expect("writer reached the delete")
        .forget();
    let during = bifrost_types::Checkpoint::Change(change_cursor(b"during"));
    let late = coverage.register(during.clone(), degraded_claim("during-reset"));
    gate.release.add_permits(1);

    reset.await.expect("reset task").expect("reset");

    let ledger = inner.get_ledger(&account).await.expect("ledger read");
    assert_eq!(
        ledger.open_debt().count(),
        2,
        "debt registered during the reset's awaits must reach the durable ledger"
    );

    // And the same publication must not be able to re-create the cursor
    // row the reset just deleted.
    assert!(
        matches!(
            coverage.claim_checkpoint(late, &during),
            crate::cursor::ClaimLookup::Unknown
        ),
        "a publication issued against rows the reset deleted must be fenced"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// Two partitions of one scope are in flight together. Acknowledging the
/// first must persist the FIRST one's report.
///
/// A scope-keyed pending map fails this outright: the second partition's
/// report overwrites the first's, and the first acknowledgement then
/// persists coverage belonging to a partition it knows nothing about.
#[tokio::test]
async fn each_partition_acknowledges_its_own_coverage() {
    let (account, store, coverage, tx, writer) = writer_harness();

    let page_a = backfill_checkpoint(bifrost_types::Partition(b"page-a".to_vec()));
    let page_b = backfill_checkpoint(bifrost_types::Partition(b"page-b".to_vec()));
    let first = register(
        &coverage,
        &page_a,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable("from-partition-a")],
            ),
            1,
        ),
    );
    // Partition B publishes AFTER A, and reports a clean walk.
    let _second = register(
        &coverage,
        &page_b,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::complete(bifrost_types::CoverageDomain::full(
                email_scope(),
            )),
            1,
        ),
    );

    ack(&tx, page_a, Some(first)).await.expect("ack persisted");

    let ledger = store.get_ledger(&account).await.expect("ledger read");
    assert_eq!(
        ledger.open_debt().count(),
        1,
        "partition A's debt must not be replaced by partition B's clean report"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// The completion sentinel is evaluated against the ledger AT WRITE TIME.
///
/// The runner decides `complete` while walking, but debt from a sibling
/// partition can be acknowledged after that decision and before the
/// sentinel reaches the writer. A writer that trusted the precomputed flag
/// would persist a marker that makes the next attach skip a scope with open
/// obligations, converting a declared gap into a permanent one.
#[tokio::test]
async fn the_completion_sentinel_is_withheld_over_open_debt() {
    let (account, store, coverage, tx, writer) = writer_harness();

    let page = backfill_checkpoint(bifrost_types::Partition(b"page-0".to_vec()));
    let debt = register(
        &coverage,
        &page,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable("broken")],
            ),
            1,
        ),
    );
    ack(&tx, page, Some(debt)).await.expect("ack persisted");

    let completion =
        backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    let sentinel = register(
        &coverage,
        &completion,
        crate::cursor::CoverageClaim {
            reports: Vec::new(),
            generation: 1,
        },
    );
    ack(&tx, completion, Some(sentinel))
        .await
        .expect("the acknowledgement itself still succeeds");

    assert!(
        store
            .get_backfill(&account, &email_scope())
            .await
            .expect("store read")
            .is_none_or(
                |checkpoint| !crate::backfill::partitioner::is_completion_partition(
                    &checkpoint.partition
                )
            ),
        "no completion marker may be durable while the scope owes obligations"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// Both recovery backoffs (`re_establish_scope_with_backoff` and
/// `restart_account`) wait through this, and a detach must cut the wait
/// short rather than have it wait out: the un-selected sleep it replaced
/// guaranteed that a detach during recovery reached `detach_timeout` and
/// aborted the worker instead of draining it.
#[tokio::test(start_paused = true)]
async fn a_recovery_backoff_is_cut_short_by_shutdown() {
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let started = tokio::time::Instant::now();
    let completed =
        super::reattach::sleep_unless_shutdown(Duration::from_secs(300), &shutdown).await;
    assert!(
        !completed,
        "a cancelled backoff must report that it was cut"
    );
    assert!(
        started.elapsed() < Duration::from_secs(300),
        "the backoff must not be waited out during teardown"
    );

    let live = CancellationToken::new();
    assert!(
        super::reattach::sleep_unless_shutdown(Duration::from_millis(1), &live).await,
        "an uncancelled backoff still runs to completion"
    );
}

/// A withheld sentinel must not be announced as a durable boundary.
///
/// The store never accepted the marker, so reporting it through
/// `pause` / `checkpoint_now` would tell the consumer a backfill-complete
/// boundary is durable while the next attach re-walks the scope. The
/// module's own rule is that nothing durable is invented, so the withheld
/// path retires the publication (releasing boundary waiters) exactly as a
/// failed store write does, instead of recording it.
#[tokio::test]
async fn a_withheld_completion_sentinel_is_not_announced_durable() {
    let (_account, _store, coverage, tx, writer, control) = writer_harness_observed();

    let page = backfill_checkpoint(bifrost_types::Partition(b"page-0".to_vec()));
    let debt = register(
        &coverage,
        &page,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable("broken")],
            ),
            1,
        ),
    );
    ack(&tx, page.clone(), Some(debt)).await.expect("ack lands");

    let completion =
        backfill_checkpoint_at(crate::backfill::partitioner::completion_partition(), 99);
    let sentinel = register(
        &coverage,
        &completion,
        crate::cursor::CoverageClaim {
            reports: Vec::new(),
            generation: 1,
        },
    );
    ack(&tx, completion.clone(), Some(sentinel))
        .await
        .expect("the acknowledgement itself still succeeds");

    let announced = control.durable_snapshot();
    assert!(
        !announced.checkpoints().contains(&completion),
        "a sentinel the store never accepted must not appear in the durable snapshot"
    );
    assert_eq!(
        coverage.pending_checkpoints(),
        0,
        "the withheld publication must still be retired so boundary waiters are released"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// A waiver releases the scope - and only a waiver can.
///
/// Without it the barrier and the withheld sentinel together would leave a
/// permanently-degraded scope with no escape at all, which is the trade the
/// project explicitly refused: declared loss beats permanent
/// non-convergence.
#[tokio::test]
async fn a_waiver_releases_the_sentinel_without_claiming_proof() {
    let (account, store, coverage, tx, writer) = writer_harness();

    let page = backfill_checkpoint(bifrost_types::Partition(b"page-0".to_vec()));
    let debt = register(
        &coverage,
        &page,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable("broken")],
            ),
            1,
        ),
    );
    ack(&tx, page, Some(debt)).await.expect("ack persisted");

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::OperatorDecision {
        key: bifrost_types::ObligationKey(b"broken".to_vec()),
        decision: crate::multiplexer::OperatorDecision::Waive {
            by: "operator".into(),
            at_unix_seconds: 1_700_000_000,
        },
        done,
    })
    .await
    .expect("writer alive");
    assert!(
        wait.await.expect("writer answered").expect("waive"),
        "the obligation must be found and waived"
    );

    let ledger = store.get_ledger(&account).await.expect("ledger read");
    assert!(
        ledger.completion_permitted(&email_scope()),
        "a waived obligation must stop blocking completion"
    );
    let entry = ledger
        .entry(&bifrost_types::ObligationKey(b"broken".to_vec()))
        .expect("the entry survives for audit");
    assert!(
        entry.is_open(),
        "a waiver is accepted loss, never proof - the record must keep saying nothing was \
             ever covered here"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// An acknowledgement naming a publication the writer never issued must be
/// REFUSED, not silently treated as a clean walk. Defaulting an unknown
/// claim to complete coverage is the original lying-record bug in another
/// costume.
#[tokio::test]
async fn an_unknown_publication_is_refused_rather_than_assumed_complete() {
    let (_account, _store, _coverage, tx, writer) = writer_harness();

    let result = ack(
        &tx,
        backfill_checkpoint(bifrost_types::Partition(b"page-0".to_vec())),
        Some(crate::cursor::PendingCoverage::new().publish_without_report(0)),
    )
    .await;
    assert!(
        result.is_err(),
        "an unrecognized publication must not be persisted as complete coverage"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

async fn seed_debt(
    tx: &mpsc::Sender<WriterRequest>,
    coverage: &crate::cursor::PendingCoverage,
    key: &str,
) {
    let page = backfill_checkpoint(bifrost_types::Partition(b"page-0".to_vec()));
    let publication = register(
        coverage,
        &page,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable(key)],
            ),
            1,
        ),
    );
    ack(tx, page, Some(publication))
        .await
        .expect("ack persisted");
}

async fn apply_repair(
    tx: &mpsc::Sender<WriterRequest>,
    resolutions: Vec<crate::repair::RepairResolution>,
    publication: Option<crate::cursor::PublicationId>,
) {
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ApplyRepair {
        resolutions,
        publication,
        done,
    })
    .await
    .expect("writer alive");
    wait.await
        .expect("writer answered")
        .expect("repair applied");
}

/// A recovered object discharges only once the consumer has acknowledged
/// the publication carrying its id.
///
/// Discharging on the account's success alone recreates the original silent
/// loss in a new place: the engine would forget the obligation while the
/// consumer never learned the object exists.
#[tokio::test]
async fn a_recovery_discharges_only_after_the_consumer_acknowledges() {
    let (account, store, coverage, tx, writer) = writer_harness();
    seed_debt(&tx, &coverage, "broken").await;
    let key = bifrost_types::ObligationKey(b"broken".to_vec());

    // Published, but nothing acknowledged it.
    apply_repair(
        &tx,
        vec![crate::repair::RepairResolution::Recovered {
            key: key.clone(),
            attempt: bifrost_types::RepairAttemptId(1),
            generation: 1,
        }],
        None,
    )
    .await;
    assert!(
        store
            .get_ledger(&account)
            .await
            .expect("ledger")
            .entry(&key)
            .expect("entry")
            .is_open(),
        "an unacknowledged recovery must leave the obligation owed"
    );

    // Now with an acknowledgeable publication.
    let publication = coverage.publish_without_report(0);
    apply_repair(
        &tx,
        vec![crate::repair::RepairResolution::Recovered {
            key: key.clone(),
            attempt: bifrost_types::RepairAttemptId(2),
            generation: 1,
        }],
        Some(publication.clone()),
    )
    .await;

    assert!(
        store
            .get_ledger(&account)
            .await
            .expect("ledger")
            .entry(&key)
            .expect("entry")
            .is_open(),
        "publishing alone must not discharge the recovery"
    );
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::AcknowledgePublication {
        publication: publication.clone(),
        done,
    })
    .await
    .expect("writer alive");
    wait.await
        .expect("writer answered")
        .expect("publication acknowledged");

    let ledger = store.get_ledger(&account).await.expect("ledger");
    let entry = ledger.entry(&key).expect("entry");
    assert!(!entry.is_open(), "an acknowledged recovery discharges");
    assert!(matches!(
        entry.proof,
        crate::cursor::ProofStatus::Discharged {
            evidence: crate::cursor::DischargeEvidence::RepairedAndPublished { .. }
        }
    ));
    assert!(ledger.completion_permitted(&email_scope()));

    drop(tx);
    writer.await.expect("writer exits");
}

/// `ack_publication` is the REPAIR lane. Handed a checkpoint publication's
/// id it must refuse, and - this is the part that bites - must leave that
/// publication's claim intact.
///
/// Consuming it would make the later, real `ack_checkpoint` resolve as
/// already persisted and return before writing anything, while the engine
/// went on to announce the checkpoint as a durable boundary. A cursor
/// position reported durable that no store ever accepted is the lying
/// record the whole publication ledger exists to prevent.
#[tokio::test]
async fn a_repair_acknowledgement_cannot_consume_a_checkpoint_publication() {
    let (account, store, coverage, tx, writer) = writer_harness();

    let checkpoint = backfill_checkpoint(bifrost_types::Partition(b"page-0".to_vec()));
    let publication = register(
        &coverage,
        &checkpoint,
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::degraded(
                bifrost_types::CoverageDomain::full(email_scope()),
                vec![unrepresentable("broken")],
            ),
            1,
        ),
    );

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::AcknowledgePublication {
        publication: publication.clone(),
        done,
    })
    .await
    .expect("writer alive");
    assert!(
        wait.await.expect("writer answered").is_err(),
        "a repair acknowledgement must not resolve a checkpoint publication"
    );

    ack(&tx, checkpoint.clone(), Some(publication))
        .await
        .expect("the real acknowledgement still persists");

    assert_eq!(
        store
            .get_backfill(&account, &email_scope())
            .await
            .expect("store read"),
        match checkpoint {
            bifrost_types::Checkpoint::Backfill(marker) => Some(marker),
            _ => unreachable!("built as a backfill checkpoint"),
        },
        "the checkpoint must actually reach the store"
    );
    assert_eq!(
        store
            .get_ledger(&account)
            .await
            .expect("ledger read")
            .open_debt()
            .count(),
        1,
        "and its coverage claim must still have been ingested"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// A definitively-irrelevant object discharges with NOTHING published.
/// Absence from an old inventory snapshot is not a deletion to apply
/// against current consumer state.
#[tokio::test]
async fn definitive_irrelevance_discharges_without_publishing() {
    let (account, store, coverage, tx, writer) = writer_harness();
    seed_debt(&tx, &coverage, "gone").await;
    let key = bifrost_types::ObligationKey(b"gone".to_vec());

    apply_repair(
        &tx,
        vec![crate::repair::RepairResolution::Irrelevant {
            key: key.clone(),
            detail: "absent under cursor bridge".into(),
            generation: 1,
        }],
        None,
    )
    .await;

    let ledger = store.get_ledger(&account).await.expect("ledger");
    assert!(!ledger.entry(&key).expect("entry").is_open());
    assert!(ledger.completion_permitted(&email_scope()));

    drop(tx);
    writer.await.expect("writer exits");
}

/// Deferrals spend the lineage budget and land on `OperatorBlocked` - never
/// on a discharge, and never on a waiver. A counter running out is evidence
/// that retrying is not working, not a decision about acceptable loss.
#[tokio::test]
async fn repeated_deferrals_block_rather_than_abandon() {
    let (account, store, coverage, tx, writer) = writer_harness();
    seed_debt(&tx, &coverage, "stuck").await;
    let key = bifrost_types::ObligationKey(b"stuck".to_vec());

    for _ in 0..crate::repair::DEFAULT_REPAIR_BUDGET {
        apply_repair(
            &tx,
            vec![crate::repair::RepairResolution::Deferred { key: key.clone() }],
            None,
        )
        .await;
    }

    let ledger = store.get_ledger(&account).await.expect("ledger");
    let entry = ledger.entry(&key).expect("entry");
    assert_eq!(entry.policy, crate::cursor::PolicyStatus::OperatorBlocked);
    assert!(entry.is_open(), "a spent budget proves nothing");
    assert!(
        !ledger.completion_permitted(&email_scope()),
        "blocked debt must still hold the completion sentinel"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// A provider that answers every pass by re-splitting the obligation into a
/// fresh key over the same extent is capped by the LINEAGE budget, and the cap
/// binds on the open child, not only on the discharged root.
///
/// Two ways this used to or could fail: expiry once blocked only the
/// discharged root while the open child stayed `Retrying` and repairable; and
/// charging the stall through `record_attempt(key)` after the swap would find
/// the parent discharged and charge nothing. The writer resolves the lineage
/// before the swap for exactly that reason.
#[tokio::test]
async fn repeated_stalled_splits_exhaust_the_lineage() {
    let (account, store, coverage, tx, writer) = writer_harness();
    seed_debt(&tx, &coverage, "stuck").await;

    let mut current = bifrost_types::ObligationKey(b"stuck".to_vec());
    for slice in 0..crate::repair::DEFAULT_REPAIR_BUDGET {
        let child = format!("slice-{slice}");
        apply_repair(
            &tx,
            vec![crate::repair::RepairResolution::Replaced {
                key: current.clone(),
                proof: Box::new(bifrost_types::RegionRepairProof::Partitioned {
                    proved: Vec::new(),
                    residual: vec![bifrost_types::InventoryObligation::Region {
                        key: bifrost_types::ObligationKey(child.clone().into_bytes()),
                        failure_label: "truncated".into(),
                        error: unrepresentable("unused").error().clone(),
                        recovery: bifrost_types::RegionRecovery::DurableReplay {
                            token: child.clone().into_bytes(),
                        },
                    }],
                    authority: "test".into(),
                }),
                generation: 1,
            }],
            None,
        )
        .await;
        current = bifrost_types::ObligationKey(child.into_bytes());
    }

    let ledger = store.get_ledger(&account).await.expect("ledger");
    let entry = ledger.entry(&current).expect("latest slice");
    assert!(entry.is_open(), "a spent budget proves nothing");
    assert_eq!(entry.policy, crate::cursor::PolicyStatus::OperatorBlocked);
    assert_eq!(
        ledger.repairable().count(),
        0,
        "no slice of a spent lineage may be planned again"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// An aborted reattach deletes only rows NO acknowledgement has claimed.
///
/// Reattach used to write and delete cursors directly against the
/// `CheckpointStore` while the ack writer was concurrently persisting
/// acknowledged checkpoints for the same scopes. That is a live race, not a
/// hypothetical: `run_establish` hands `changes_tx` to `InventoryFusion`, so
/// a replacement's inventory pass broadcasts checkpoint-bearing batches
/// BEFORE the cutover, a consumer can acknowledge one, and an abort then
/// deleted the row that acknowledgement had just committed.
///
/// Serializing every durable mutation through the one writer is necessary
/// but NOT sufficient - a plain FIFO queue would order the acknowledged
/// write ahead of the unconditional delete and faithfully destroy it. The
/// provisional set is what turns the abort into a conditional delete, which
/// is why both halves are asserted here: the acknowledged scope survives
/// AND the unacknowledged one is still rolled back. A guard that simply
/// stopped deleting would pass the first assertion and leak every
/// replacement cursor an aborted reattach ever wrote.
#[tokio::test]
async fn an_aborted_reattach_spares_a_cursor_an_ack_has_claimed() {
    use crate::cursor::InMemoryCheckpointStore;
    use crate::multiplexer::AckRequest;
    use bifrost_types::{ChangeCursor, Checkpoint, OpaqueChangeState, ProtocolKind};

    fn cursor(name: &str) -> ChangeCursor {
        ChangeCursor {
            scope: CursorScope::Folder(FolderId(name.into())),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: 1,
                bytes: vec![1],
            },
            advanced_through: None,
            envelope_version: 1,
        }
    }

    let account = bifrost_types::AccountId("acct".into());
    let store: Arc<DynCheckpointStore> = Arc::new(InMemoryCheckpointStore::default());
    let (tx, rx) = mpsc::channel::<WriterRequest>(16);
    let (boundary, _view) = crate::cancel::Boundary::new();
    let (priority, _priority_view) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _bandwidth_view) = tokio::sync::watch::channel(None);
    let control = crate::control::SyncControl::new(account.clone(), boundary, priority, bandwidth);
    let writer = tokio::spawn(ack_writer(
        account.clone(),
        Arc::clone(&store),
        control,
        Arc::new(crate::cursor::PendingCoverage::new()),
        rx,
    ));

    // Both scopes are inserted by the same in-flight reattach.
    for name in ["acked", "orphan"] {
        let (done, wait) = oneshot::channel();
        tx.send(WriterRequest::ReattachInsert {
            attempt: 1,
            cursor: cursor(name),
            done,
        })
        .await
        .expect("writer alive");
        wait.await.expect("writer answered").expect("insert");
    }

    // A consumer acknowledges one of them - exactly what a replacement
    // inventory pass makes possible before the cutover.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(AckRequest {
        scope: cursor("acked").scope,
        checkpoint: Checkpoint::Change(cursor("acked")),
        publication: None,
        auto: false,
        complete: Some(done),
    }))
    .await
    .expect("writer alive");
    wait.await.expect("writer answered").expect("ack persisted");

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ReattachAbort { attempt: 1, done })
        .await
        .expect("writer alive");
    wait.await.expect("abort ran");

    assert!(
        store
            .get_change_cursor(&account, &cursor("acked").scope)
            .await
            .expect("store read")
            .is_some(),
        "an aborted reattach must not delete a cursor a consumer acknowledged"
    );
    assert!(
        store
            .get_change_cursor(&account, &cursor("orphan").scope)
            .await
            .expect("store read")
            .is_none(),
        "a replacement cursor nothing acknowledged must still be rolled back"
    );

    drop(tx);
    writer.await.expect("writer exits");
}

/// A running ack writer over an in-memory store, for the attempt-ownership
/// tests below.
struct ProvisionalWriter {
    account: bifrost_types::AccountId,
    store: Arc<DynCheckpointStore>,
    tx: mpsc::Sender<WriterRequest>,
    task: tokio::task::JoinHandle<()>,
}

impl ProvisionalWriter {
    fn spawn() -> Self {
        let account = bifrost_types::AccountId("acct".into());
        let store: Arc<DynCheckpointStore> =
            Arc::new(crate::cursor::InMemoryCheckpointStore::default());
        let (tx, rx) = mpsc::channel::<WriterRequest>(16);
        let (boundary, _view) = crate::cancel::Boundary::new();
        let (priority, _priority_view) =
            tokio::sync::watch::channel(bifrost_types::Priority::Normal);
        let (bandwidth, _bandwidth_view) = tokio::sync::watch::channel(None);
        let control =
            crate::control::SyncControl::new(account.clone(), boundary, priority, bandwidth);
        let task = tokio::spawn(ack_writer(
            account.clone(),
            Arc::clone(&store),
            control,
            Arc::new(crate::cursor::PendingCoverage::new()),
            rx,
        ));
        Self {
            account,
            store,
            tx,
            task,
        }
    }

    fn scope(name: &str) -> CursorScope {
        CursorScope::Folder(FolderId(name.into()))
    }

    async fn insert(&self, attempt: u64, name: &str) {
        use bifrost_types::{ChangeCursor, OpaqueChangeState, ProtocolKind};
        let (done, wait) = oneshot::channel();
        self.tx
            .send(WriterRequest::ReattachInsert {
                attempt,
                cursor: ChangeCursor {
                    scope: Self::scope(name),
                    server_state: OpaqueChangeState {
                        protocol: ProtocolKind::Imap,
                        envelope_version: 1,
                        bytes: vec![1],
                    },
                    advanced_through: None,
                    envelope_version: 1,
                },
                done,
            })
            .await
            .expect("writer alive");
        wait.await.expect("writer answered").expect("insert");
    }

    async fn abort(&self, attempt: u64) {
        let (done, wait) = oneshot::channel();
        self.tx
            .send(WriterRequest::ReattachAbort { attempt, done })
            .await
            .expect("writer alive");
        wait.await.expect("abort ran");
    }

    async fn commit(&self, attempt: u64) {
        self.tx
            .send(WriterRequest::ReattachCommit { attempt })
            .await
            .expect("writer alive");
    }

    async fn read(&self, name: &str, claim_for: Option<u64>) -> bool {
        let (done, wait) = oneshot::channel();
        self.tx
            .send(WriterRequest::GetChangeCursor {
                scope: Self::scope(name),
                claim_for,
                done,
            })
            .await
            .expect("writer alive");
        wait.await
            .expect("writer answered")
            .expect("read")
            .is_some()
    }

    async fn has(&self, name: &str) -> bool {
        self.store
            .get_change_cursor(&self.account, &Self::scope(name))
            .await
            .expect("store read")
            .is_some()
    }

    async fn finish(self) {
        drop(self.tx);
        self.task.await.expect("writer exits");
    }
}

/// A dropped reattach's abort can reach the writer AFTER the next reattach has
/// inserted: the guard's synchronous enqueue falls back to a spawned send when
/// the queue is full, and that send is unordered against the next attempt. The
/// stale abort must delete only what its own attempt inserted, and the newer
/// attempt's commit must still promote its row.
#[tokio::test]
async fn a_stale_abort_spares_a_later_attempts_rows() {
    let writer = ProvisionalWriter::spawn();

    writer.insert(1, "a-row").await;
    writer.insert(2, "b-row").await;
    writer.abort(1).await;

    assert!(!writer.has("a-row").await, "the aborted attempt's row goes");
    assert!(
        writer.has("b-row").await,
        "a delayed abort from attempt 1 must not delete attempt 2's row"
    );

    writer.commit(2).await;
    // A later abort naming the committed attempt finds nothing provisional.
    writer.abort(2).await;
    assert!(
        writer.has("b-row").await,
        "attempt 2's commit must have promoted its row"
    );

    writer.finish().await;
}

/// When a later attempt re-inserts a scope an earlier attempt already marked,
/// it takes ownership: the earlier attempt's abort spares the row, and only the
/// owner's abort deletes it.
#[tokio::test]
async fn a_reinserting_attempt_takes_ownership_of_the_scope() {
    let writer = ProvisionalWriter::spawn();

    writer.insert(1, "shared").await;
    writer.insert(2, "shared").await;
    writer.abort(1).await;
    assert!(
        writer.has("shared").await,
        "attempt 1 no longer owns the row attempt 2 re-inserted"
    );

    // The earlier attempt's commit is equally inert.
    writer.commit(1).await;
    writer.abort(2).await;
    assert!(
        !writer.has("shared").await,
        "the owning attempt's abort still rolls the row back"
    );

    writer.finish().await;
}

/// Attempt A inserts S and is dropped with its abort still in flight. Attempt B
/// resumes S from the store and commits. S now belongs to the installed
/// topology, so A's late abort must not delete it: B's resume read claimed it.
#[tokio::test]
async fn a_resuming_attempt_claims_a_provisional_row_from_a_stale_abort() {
    let writer = ProvisionalWriter::spawn();

    writer.insert(1, "s").await;
    assert!(writer.read("s", Some(2)).await, "the row resumes as stored");
    writer.commit(2).await;
    writer.abort(1).await;

    assert!(
        writer.has("s").await,
        "a stale abort must not delete a row a later attempt resumed and committed"
    );

    writer.finish().await;
}

/// A read outside a reattach claims nothing, and a claiming read of a row that
/// is not provisional changes nothing.
#[tokio::test]
async fn only_a_reattach_resume_of_a_provisional_row_claims_it() {
    let writer = ProvisionalWriter::spawn();

    // A plain read (attach, scope restart) must not steal A's row.
    writer.insert(1, "s").await;
    assert!(writer.read("s", None).await);
    writer.abort(1).await;
    assert!(
        !writer.has("s").await,
        "a non-reattach read must leave the row with the attempt that inserted it"
    );

    // A committed row is not provisional: a claiming read leaves it alone, so
    // the claimant's abort cannot delete it.
    writer.insert(3, "committed").await;
    writer.commit(3).await;
    assert!(writer.read("committed", Some(4)).await);
    writer.abort(4).await;
    assert!(
        writer.has("committed").await,
        "resuming a committed row must not make it rollback-able"
    );

    writer.finish().await;
}

#[test]
fn backfill_skips_only_the_fusion_owned_incarnation_and_rescans_new_scopes() {
    let fused = CursorScope::Type(ObjectType::Email);
    let created = CursorScope::Folder(FolderId("created".into()));
    let fusion_owned = HashSet::from([fused.clone()]);
    let mut scan = BackfillScan::default();
    let coverage = crate::cursor::PendingCoverage::new();
    let now = tokio::time::Instant::now();

    let first = scan.select(vec![(fused.clone(), 1)], &fusion_owned, now, &coverage);
    assert!(
        first.is_empty(),
        "fusion's inventory must not be double-walked"
    );

    let later = scan.select(
        vec![(fused.clone(), 2), (created.clone(), 3)],
        &fusion_owned,
        now,
        &coverage,
    );
    assert_eq!(later, vec![(fused, 2), (created, 3)]);
}

/// A completed walk retires the incarnation; a failed one does not.
/// Filtering a failed incarnation out permanently would let a single
/// transient partition or checkpoint-store error cost that scope its
/// entire backfill for the rest of the attachment.
#[tokio::test(start_paused = true)]
async fn a_failed_backfill_incarnation_stays_eligible_for_retry() {
    let scope = CursorScope::Type(ObjectType::Email);
    let fusion_owned = HashSet::new();
    let mut scan = BackfillScan::default();
    let coverage = crate::cursor::PendingCoverage::new();
    let available = vec![(scope.clone(), 7)];

    assert_eq!(
        scan.select(
            available.clone(),
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage
        ),
        vec![(scope.clone(), 7)]
    );
    scan.record_attempt((scope.clone(), 7), false);
    assert!(
        scan.select(
            available.clone(),
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage
        )
        .is_empty(),
        "a just-failed incarnation must back off rather than re-walk immediately"
    );

    tokio::time::advance(super::backfill::BACKFILL_RETRY_INITIAL * 2).await;
    assert_eq!(
        scan.select(
            available.clone(),
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage
        ),
        vec![(scope.clone(), 7)],
        "a failed incarnation must come back once its backoff elapses"
    );

    scan.record_attempt((scope.clone(), 7), true);
    tokio::time::advance(super::backfill::BACKFILL_RETRY_CAP * 2).await;
    assert!(
        scan.select(
            available,
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage
        )
        .is_empty(),
        "a completed incarnation is never re-walked"
    );
}

/// An incarnation that leaves the registry takes its residue with it.
///
/// A scope that is deleted, re-established, or simply never comes back otherwise
/// leaves a retry deadline, a lost-pages mark and a baseline behind it - one set
/// per incarnation, for the life of the attachment - and the orchestrator's
/// "vanished during the subscriber wait" path adds one every time it fires.
/// `settled` is deliberately kept: it is the memory that stops a concluded
/// incarnation being walked again, and incarnation numbers only move forward.
#[tokio::test(start_paused = true)]
async fn a_departed_incarnation_leaves_no_residue_behind() {
    let scope = CursorScope::Type(ObjectType::Email);
    let fusion_owned = HashSet::new();
    let mut scan = BackfillScan::default();
    let coverage = crate::cursor::PendingCoverage::new();
    coverage.note_undelivered(&scope, 3);
    let key = (scope.clone(), 11);

    scan.select(
        vec![key.clone()],
        &fusion_owned,
        tokio::time::Instant::now(),
        &coverage,
    );
    assert_eq!(
        scan.begin_walk(&key, &coverage),
        3,
        "the walk begins under the scope's current reading"
    );
    scan.note_lost_pages(key.clone());
    assert_eq!(scan.baseline_for(&key), 3, "which the scan holds for it");
    assert!(scan.restarts_from_scratch(&key));

    // The registry no longer carries it.
    assert!(
        scan.select(
            Vec::new(),
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage
        )
        .is_empty()
    );
    assert_eq!(
        scan.baseline_for(&key),
        0,
        "a departed incarnation's baseline must not be kept for the life of the attachment"
    );
    assert!(!scan.restarts_from_scratch(&key), "nor its lost-pages mark");
}

/// A settle must be recorded against the reading its walk was CHECKED against,
/// so a loss landing after those checks reopens the incarnation.
///
/// The window is small and real: the writer refuses the completion marker and
/// discards the rows on exactly such a loss, and if the rescan adopts the same
/// event as its baseline it compares that reading against itself for ever - the
/// two halves of one repair reading one event in opposite directions, with the
/// scope never walked again for the rest of the attachment. `BackfillScan` takes
/// the baseline through `begin_walk`, as the walk starts, for that reason: a
/// settle cannot be handed a reading nothing validated, because it is not handed
/// one at all.
#[tokio::test(start_paused = true)]
async fn a_loss_after_the_checks_reopens_the_incarnation_it_settled() {
    let scope = CursorScope::Type(ObjectType::Email);
    let fusion_owned = HashSet::new();
    let mut scan = BackfillScan::default();
    let coverage = crate::cursor::PendingCoverage::new();
    let available = vec![(scope.clone(), 3)];

    assert_eq!(
        scan.select(
            available.clone(),
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage
        ),
        vec![(scope.clone(), 3)]
    );
    // A loss BEFORE this walk begins belongs to whatever came earlier, and the
    // baseline it starts under has to include it - otherwise every scope after
    // the first in one rescan batch re-walks over its predecessor's losses.
    coverage.note_undelivered(&scope, 4);
    assert_eq!(scan.begin_walk(&(scope.clone(), 3), &coverage), 4);
    // And the loss that lands after every check this pass made, before the
    // settle, is precisely the interleaving the writer acts on.
    coverage.note_undelivered(&scope, 1);
    scan.record_attempt((scope.clone(), 3), true);

    assert_eq!(
        scan.select(
            available,
            &fusion_owned,
            tokio::time::Instant::now(),
            &coverage
        ),
        vec![(scope.clone(), 3)],
        "the incarnation must be reopened: its walk was settled on checks made \
         before a page of it reached nobody"
    );
    assert!(
        scan.restarts_from_scratch(&(scope, 3)),
        "and reopened as a walk that may not trust its stored resume position"
    );
}

/// The same reopen, for an incarnation whose last attempt FAILED rather than
/// settled.
///
/// A departure sweep landing after a transient partition failure reaches nobody:
/// `note_lost_pages` runs at a walk's END, and the next attempt takes the moved
/// reading as its own baseline, so it RESUMES past the hole, judges itself whole
/// and earns a durable marker. The discard request that loss raised then sits
/// outstanding until `detach` acts on it and deletes the marker the second walk
/// legitimately earned - a full re-walk on the next attach, once per flaky
/// partition, for as long as the provider keeps failing. The failed attempt's
/// baseline therefore SURVIVES the failure, and the rescan compares it exactly as
/// it compares a settled one.
#[tokio::test(start_paused = true)]
async fn a_loss_after_a_failed_attempt_restarts_the_next_walk_from_scratch() {
    let scope = CursorScope::Type(ObjectType::Email);
    let fusion_owned = HashSet::new();
    let mut scan = BackfillScan::default();
    let coverage = crate::cursor::PendingCoverage::new();
    let key = (scope.clone(), 7);

    scan.select(
        vec![key.clone()],
        &fusion_owned,
        tokio::time::Instant::now(),
        &coverage,
    );
    assert_eq!(scan.begin_walk(&key, &coverage), 0);
    // The attempt ends on a transient failure, with every page it published
    // still in a live consumer's hands.
    scan.record_attempt(key.clone(), false);
    assert!(
        !scan.restarts_from_scratch(&key),
        "a plain failure is not a reason to distrust the stored resume position"
    );

    // Only now does the consumer holding one of that attempt's pages depart.
    coverage.note_undelivered(&scope, 1);
    scan.select(
        vec![key.clone()],
        &fusion_owned,
        tokio::time::Instant::now(),
        &coverage,
    );

    assert!(
        scan.restarts_from_scratch(&key),
        "the retry must start the walk over: resuming past the hole earns a durable \
         marker while the discard request the loss raised waits for a repair that \
         never comes, and detach then deletes the marker that walk earned"
    );
}

#[test]
fn rejected_push_scopes_are_not_treated_as_covered() {
    let accepted = CursorScope::Folder(FolderId("accepted".into()));
    let rejected = CursorScope::Folder(FolderId("rejected".into()));
    let expected = vec![
        bifrost_types::BatchItemId("0".into()),
        bifrost_types::BatchItemId("1".into()),
    ];
    let mut builder = bifrost_types::BatchOutcomeBuilder::new();
    builder.push_succeeded(expected[0].clone(), accepted.clone());
    let operation = bifrost_types::AccountOperation::PushSubscribe;
    let error = bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Unsupported(operation),
        bifrost_types::Cause::Request(bifrost_types::RequestCause::Unsupported { operation }),
    )
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(rejected))
    .try_build()
    .expect("valid unsupported push error");
    builder.push_failed(expected[1].clone(), error);
    let result = bifrost_types::PushSubscription::new(
        Some(bifrost_types::SubscriptionHandle("handle".into())),
        builder
            .finalize(&expected)
            .expect("complete scope accounting"),
    )
    .expect("the handle covers the succeeded scope");

    assert_eq!(accepted_push_scopes(&result), vec![accepted]);
}

#[test]
fn jittered_stays_within_plus_minus_20_percent() {
    use super::reattach::jittered;
    use std::time::Duration;
    // Entropy comes from a fresh UUID per call; sample enough draws
    // that the ±20% envelope is exercised without depending on the
    // clock. The math must keep every draw inside [0.8x, 1.2x].
    let base = Duration::from_millis(1_000);
    let lo = Duration::from_millis(800);
    let hi = Duration::from_millis(1_200);
    for _ in 0..10_000 {
        let j = jittered(base);
        assert!(j >= lo && j <= hi, "jitter {j:?} outside ±20% of {base:?}");
    }
}

#[test]
fn jittered_zero_base_is_zero() {
    use super::reattach::jittered;
    use std::time::Duration;
    assert_eq!(jittered(Duration::ZERO), Duration::ZERO);
}

#[test]
fn retryable_stream_termination_requeues_every_unresolved_target() {
    let applied = bifrost_types::ObjectId("applied".into());
    let explicit_retry = bifrost_types::ObjectId("explicit-retry".into());
    let unseen = bifrost_types::ObjectId("unseen".into());
    let remaining = vec![applied.clone(), explicit_retry.clone(), unseen.clone()];
    let outcomes = std::collections::HashMap::from([
        (applied, MutationBucket::Applied),
        (explicit_retry.clone(), MutationBucket::PendingRetry),
    ]);
    let mut retry_ids = vec![explicit_retry.clone()];

    queue_unresolved_for_retry(&remaining, &outcomes, &mut retry_ids);

    assert_eq!(retry_ids, vec![explicit_retry, unseen]);
}

/// The retry sweep must not raid the other two lanes. An id the
/// protocol reported `Uncertain` (or a reconcile that wants its
/// target checked) belongs to the read-back guard - resubmitting it
/// is exactly the blind replay that lane exists to prevent - and an
/// engine-blocked id is waiting on a directive.
#[test]
fn retry_sweep_leaves_readback_and_engine_blocked_ids_alone() {
    let uncertain = bifrost_types::ObjectId("uncertain".into());
    let blocked = bifrost_types::ObjectId("blocked".into());
    let unseen = bifrost_types::ObjectId("unseen".into());
    let remaining = vec![uncertain.clone(), blocked.clone(), unseen.clone()];
    let outcomes = std::collections::HashMap::from([
        (uncertain, MutationBucket::PendingReadback),
        (blocked, MutationBucket::BlockedByEngine),
    ]);
    let mut retry_ids = Vec::new();

    queue_unresolved_for_retry(&remaining, &outcomes, &mut retry_ids);

    assert_eq!(retry_ids, vec![unseen]);
}

/// A `Downgraded` success keeps its own bucket and is neither read back nor
/// resubmitted.
///
/// Filing the weaker effect as `Applied` produced a permanent reconcile loop
/// for Gmail's trash-instead-of-destroy fallback: the engine believed the
/// messages were gone, the next inventory pass saw them, and it destroyed
/// them again forever. The reported effect is retained in campaign counters.
/// It must equally not land in `retry_ids` - replaying a downgrade earns the
/// same downgrade, burning the campaign's attempts to no purpose.
#[test]
fn a_downgraded_mutation_keeps_its_bucket_and_is_not_retried_or_read_back() {
    use bifrost_types::{BatchItemId, BatchSuccess, ItemOutcome, MutationSuccess};

    let id = bifrost_types::ObjectId("message".into());
    let item: ItemOutcome<MutationSuccess> = ItemOutcome::Succeeded(BatchSuccess::new(
        BatchItemId("message".into()),
        MutationSuccess::Downgraded {
            actual: bifrost_types::MutationEffect::MovedToContainer(bifrost_types::ContainerId(
                "trash".into(),
            )),
        },
    ));
    let mut outcomes = std::collections::HashMap::new();
    let mut retry = Vec::new();
    let mut dedupe = 0;
    let throttles = std::sync::Mutex::new(crate::recovery::ThrottleBucket::new());
    let account = bifrost_types::AccountId("acc".into());

    let forwarded = classify_item_outcome(
        item,
        &mut outcomes,
        &mut retry,
        &mut dedupe,
        &throttles,
        &account,
        &mut None,
    );

    assert!(forwarded.is_none(), "a downgrade is not an engine recovery");
    assert_eq!(
        outcomes.get(&id),
        Some(&MutationBucket::Downgraded),
        "a downgrade has its own successful-effect bucket"
    );
    assert!(
        retry.is_empty(),
        "a downgrade is not transient; resubmitting it earns the same downgrade"
    );
    assert!(
        unresolved_readback_ids(&outcomes).is_empty(),
        "the read-back guard only handles unresolved outcomes"
    );
    let counters = counters_from_outcomes(&outcomes);
    assert_eq!(counters.downgraded, 1);
    assert_eq!(counters.pending_retry, 0);
}

#[test]
fn per_item_engine_failure_returns_the_directive_for_forwarding() {
    use bifrost_types::{
        AccountOperation, BatchFailure, BatchItemId, ItemOutcome, MutationSuccess,
    };
    let scope = CursorScope::Folder(FolderId("INBOX".into()));
    let error = crate::recovery::restart_scope_error(scope.clone(), AccountOperation::UpdateFlags);
    let item: ItemOutcome<MutationSuccess> =
        ItemOutcome::Failed(BatchFailure::new(BatchItemId("message".into()), error));
    let mut outcomes = std::collections::HashMap::new();
    let mut retry = Vec::new();
    let mut dedupe = 0;

    let throttles = std::sync::Mutex::new(crate::recovery::ThrottleBucket::new());
    let account = bifrost_types::AccountId("acc".into());
    let (directive, forwarded) = classify_item_outcome(
        item,
        &mut outcomes,
        &mut retry,
        &mut dedupe,
        &throttles,
        &account,
        &mut None,
    )
    .expect("engine recovery must be forwarded");

    assert_eq!(
        crate::recovery::directive_target_scope(&directive),
        Some(scope)
    );
    assert!(forwarded.recovery().requires_engine_action());
    assert_eq!(
        outcomes.get(&bifrost_types::ObjectId("message".into())),
        Some(&MutationBucket::BlockedByEngine)
    );
}

#[test]
fn per_item_engine_recovery_forwards_once_per_distinct_directive() {
    let inbox = CursorScope::Folder(FolderId("INBOX".into()));
    let archive = CursorScope::Folder(FolderId("Archive".into()));
    let mut forwarded = std::collections::HashSet::new();

    assert!(should_forward_engine_recovery(
        &mut forwarded,
        &EngineDirective::RestartScope(inbox.clone())
    ));
    assert!(
        !should_forward_engine_recovery(
            &mut forwarded,
            &EngineDirective::RestartScope(inbox.clone())
        ),
        "every failed object naming one directive shares one reopen request"
    );
    assert!(should_forward_engine_recovery(
        &mut forwarded,
        &EngineDirective::RestartScope(archive)
    ));
}

/// A mixed batch is the case keying on the target scope alone got wrong:
/// three distinct account-wide directives all resolve to `None`, and three
/// distinct directives on one folder all resolve to that folder. Under the
/// old key the first of each group suppressed the rest, so an
/// `OperatorOverrideRequired` or a scope disable could be silently
/// swallowed by an unrelated earlier failure in the same campaign.
#[test]
fn mixed_batch_directives_are_not_suppressed_by_a_shared_target() {
    let inbox = CursorScope::Folder(FolderId("INBOX".into()));
    let mut forwarded = std::collections::HashSet::new();

    for directive in [
        EngineDirective::RestartAccount,
        EngineDirective::SchemaIncompatible,
        EngineDirective::OperatorOverrideRequired {
            reason: "mailbox quota frozen".into(),
        },
        EngineDirective::RestartScope(inbox.clone()),
        EngineDirective::DowngradeCapabilityForScope(inbox.clone()),
        EngineDirective::DisableScope(inbox.clone()),
    ] {
        assert!(
            should_forward_engine_recovery(&mut forwarded, &directive),
            "{directive:?} must reach the reopen listener on its own"
        );
        assert!(
            crate::recovery::directive_target_scope(&directive).is_none()
                || crate::recovery::directive_target_scope(&directive) == Some(inbox.clone()),
            "test fixture must exercise the two colliding target groups"
        );
    }
    assert_eq!(forwarded.len(), 6);
}

/// Both pending lanes reach the guard, and nothing else does.
#[test]
fn readback_queue_is_every_still_pending_id() {
    let outcomes = std::collections::HashMap::from([
        (
            bifrost_types::ObjectId("applied".into()),
            MutationBucket::Applied,
        ),
        (
            bifrost_types::ObjectId("skipped".into()),
            MutationBucket::Skipped,
        ),
        (
            bifrost_types::ObjectId("failed".into()),
            MutationBucket::FailedTerminal,
        ),
        (
            bifrost_types::ObjectId("blocked".into()),
            MutationBucket::BlockedByEngine,
        ),
        (
            bifrost_types::ObjectId("retry".into()),
            MutationBucket::PendingRetry,
        ),
        (
            bifrost_types::ObjectId("readback".into()),
            MutationBucket::PendingReadback,
        ),
    ]);

    let mut ids: Vec<String> = unresolved_readback_ids(&outcomes)
        .into_iter()
        .map(|id| id.0)
        .collect();
    ids.sort();

    assert_eq!(ids, vec!["readback".to_string(), "retry".to_string()]);
}

/// Attach must contain a scope-local establishment failure instead of
/// failing the whole account. One revoked shared mailbox or unreadable
/// public folder previously took the primary mailbox down with it,
/// because the establish loop propagated every error with `?`.
#[test]
fn scope_revoked_establishment_failure_is_contained_to_its_scope() {
    use super::scope_local_establish_failure;
    use bifrost_types::{
        AccessCause, AccessErrorKind, AccountErrorBuilder, AccountErrorKind, AccountOperation,
        Cause, ErrorScope, Protocol, StateCause, SyncStateErrorKind,
    };

    let revoked = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::EstablishCursor)
    .scope(ErrorScope::Cursor(CursorScope::Folder(FolderId(
        "Shared/alice/INBOX".into(),
    ))))
    .try_build()
    .expect("valid account error classification");
    assert!(scope_local_establish_failure(&revoked));

    // An account-wide fact still fails the attach: containment is
    // deliberately narrow, keyed on the protocol crate's own
    // `DisableScope` classification and nothing else.
    let denied = AccountErrorBuilder::new(
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
        Cause::Access(AccessCause::PermissionDenied { resource: None }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::EstablishCursor)
    .try_build()
    .expect("valid account error classification");
    assert!(!scope_local_establish_failure(&denied));
}

#[test]
fn folder_type_scope_covers_matching_mailbox_membership() {
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".into()),
        ty: ObjectType::Email,
    };
    let membership = MembershipScope::Mailbox(bifrost_types::MailboxId("inbox".into()));
    assert!(scope_covers_membership(&scope, &membership));
}

#[test]
fn folder_type_scope_rejects_other_mailbox_membership() {
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".into()),
        ty: ObjectType::Email,
    };
    let membership = MembershipScope::Mailbox(bifrost_types::MailboxId("archive".into()));
    assert!(!scope_covers_membership(&scope, &membership));
}

/// `disable_scope` quarantines a single shared-folder scope by
/// deleting its cursor and dropping its membership index edges. The
/// poll loop self-drains on the next iteration once
/// `cursors.snapshot(scope)` returns `None`. This pins the registry
/// mechanism the quarantine relies on: a shared `Folder` scope tagged
/// with its owning `Mailbox` membership is fully removed by
/// `CursorRegistry::delete` (cursor + membership), and an untargeted
/// sibling scope is untouched.
#[test]
fn disable_scope_deletes_cursor_and_drops_membership() {
    use crate::cursor::CursorRegistry;
    use bifrost_types::{ChangeCursor, MailboxId, OpaqueChangeState, ProtocolKind};

    let cursors = CursorRegistry::new();
    let shared = CursorScope::Folder(FolderId("Shared/alice/INBOX".into()));
    let sibling = CursorScope::Folder(FolderId("INBOX".into()));

    let mk = |scope: &CursorScope| ChangeCursor {
        scope: scope.clone(),
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Imap,
            envelope_version: 1,
            bytes: Vec::new(),
        },
        advanced_through: None,
        envelope_version: 1,
    };
    cursors.put(mk(&shared));
    cursors.put(mk(&sibling));
    cursors.link_membership(
        MembershipScope::Mailbox(MailboxId("alice".into())),
        shared.clone(),
    );
    cursors.link_membership(
        MembershipScope::Folder(FolderId("INBOX".into())),
        sibling.clone(),
    );

    // The mechanism inside `disable_scope`: a single registry delete
    // drops both the cursor and the membership edges for the scope.
    cursors.delete(&shared);

    assert!(cursors.snapshot(&shared).is_none());
    assert!(
        cursors
            .scopes_for_membership(&MembershipScope::Mailbox(MailboxId("alice".into())))
            .is_empty()
    );
    // Sibling scope untouched.
    assert!(cursors.snapshot(&sibling).is_some());
    assert_eq!(
        cursors.scopes_for_membership(&MembershipScope::Folder(FolderId("INBOX".into()))),
        vec![sibling]
    );
}

/// Build a backfill checkpoint on a given partition key + progress for
/// the open-pages resume tests.
#[cfg(test)]
fn checkpoint_on(
    partition: bifrost_types::Partition,
    items_done: u64,
) -> bifrost_types::BackfillCheckpoint {
    bifrost_types::BackfillCheckpoint {
        scope: CursorScope::Type(ObjectType::Email),
        partition,
        progress_marker: None,
        progress: bifrost_types::BackfillProgress {
            items_done,
            items_estimated: None,
        },
        envelope_version: 1,
    }
}

#[test]
fn backfill_complete_recorded_detects_sentinel() {
    use super::backfill::backfill_complete_recorded;
    assert!(!backfill_complete_recorded(None));

    let marker = checkpoint_on(crate::backfill::partitioner::completion_partition(), 99);
    assert!(backfill_complete_recorded(Some(&marker)));

    // A real partition checkpoint (e.g. a fully-walked Full plan) is
    // not the completion signal.
    let full = checkpoint_on(
        crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full),
        99,
    );
    assert!(!backfill_complete_recorded(Some(&full)));
}

#[test]
fn open_pages_resume_no_checkpoint_starts_fresh() {
    use super::backfill::{OpenPagesResume, open_pages_resume};
    assert_eq!(open_pages_resume(None), OpenPagesResume::ResumeFrom(0));
}

#[test]
fn open_pages_resume_completion_marker_skips() {
    use super::backfill::{OpenPagesResume, open_pages_resume};
    let ck = checkpoint_on(crate::backfill::partitioner::completion_partition(), 1234);
    assert_eq!(open_pages_resume(Some(&ck)), OpenPagesResume::Skip);
}

#[test]
fn open_pages_resume_short_final_page_resumes_rather_than_skipping() {
    use super::backfill::{OpenPagesResume, open_pages_resume};
    // A 256-of-500 page is NOT proof the inventory ran out inside the
    // window: a partition may emit fewer entries than its width while
    // the scope still has results (ids deleted between listing and
    // hydration, id-less objects dropped). Only the completion marker
    // proves exhaustion. Skipping here would silently drop every
    // message past the window on re-attach.
    let key =
        crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page {
            from: 0,
            to: 500,
        });
    let ck = checkpoint_on(key, 256);
    assert_eq!(
        open_pages_resume(Some(&ck)),
        OpenPagesResume::ResumeFrom(500)
    );
}

#[test]
fn open_pages_resume_full_page_resumes_after_it() {
    use super::backfill::{OpenPagesResume, open_pages_resume};
    let key =
        crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page {
            from: 500,
            to: 1000,
        });
    let ck = checkpoint_on(key, 500);
    assert_eq!(
        open_pages_resume(Some(&ck)),
        OpenPagesResume::ResumeFrom(1000)
    );
}

#[test]
fn open_pages_resume_unrecognised_partition_starts_fresh() {
    use super::backfill::{OpenPagesResume, open_pages_resume};
    let key = crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full);
    let ck = checkpoint_on(key, 7);
    assert_eq!(open_pages_resume(Some(&ck)), OpenPagesResume::ResumeFrom(0));
}

/// The orchestrator drives both plan shapes through one shared walk, so
/// `BackfillPlan::resume` is now the ONLY place the two differ. These pin
/// each arm on both sides of the skip/walk choice: a wired-up-backwards
/// resume would either re-walk a finished scope every attach or, far
/// worse, skip a scope that never finished.
#[test]
fn a_fixed_plan_skips_only_on_its_completion_marker() {
    use super::backfill::{BackfillPlan, ScopeResume};

    let marker = checkpoint_on(crate::backfill::partitioner::completion_partition(), 99);
    let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
    assert!(matches!(plan.resume(Some(&marker)), ScopeResume::Skip));

    // No marker: walk every partition. A fixed plan has no positional
    // resume, so re-walking is the only correct answer.
    let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
    let ScopeResume::Walk(mut driver) = plan.resume(None) else {
        panic!("a fixed plan without a completion marker must walk");
    };
    assert_eq!(
        driver.next_partition(),
        Some(bifrost_types::InventoryPartition::Full)
    );

    // A checkpoint that is merely a walked partition is NOT the
    // completion signal. Treating any stored row as "done" would strand
    // a plan that crashed after its first partition acked.
    let partial = checkpoint_on(
        crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full),
        42,
    );
    let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
    let ScopeResume::Walk(mut driver) = plan.resume(Some(&partial)) else {
        panic!("only the completion marker may skip a fixed plan");
    };
    assert_eq!(
        driver.next_partition(),
        Some(bifrost_types::InventoryPartition::Full)
    );
}

#[test]
fn an_open_pages_plan_resumes_at_the_acked_window() {
    use super::backfill::{BackfillPlan, ScopeResume};

    let marker = checkpoint_on(crate::backfill::partitioner::completion_partition(), 99);
    let plan = BackfillPlan::OpenPages { chunk: 500 };
    assert!(matches!(plan.resume(Some(&marker)), ScopeResume::Skip));

    let key =
        crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page {
            from: 0,
            to: 500,
        });
    let acked = checkpoint_on(key, 500);
    let plan = BackfillPlan::OpenPages { chunk: 500 };
    let ScopeResume::Walk(mut driver) = plan.resume(Some(&acked)) else {
        panic!("an unfinished page walk must resume, not skip");
    };
    assert_eq!(
        driver.next_partition(),
        Some(bifrost_types::InventoryPartition::Page {
            from: 500,
            to: 1000
        }),
        "the walk resumes after the furthest durably-acked window"
    );
}

/// A checkpoint READ failure is not evidence of coverage. Both shapes
/// must fall back to a full walk rather than a skip, or one flaky store
/// call silently drops everything a prior run had not finished.
#[test]
fn a_failed_resume_read_walks_from_scratch_on_both_plan_shapes() {
    use super::backfill::{BackfillPlan, ScopeResume};

    let plan = BackfillPlan::Fixed(vec![bifrost_types::InventoryPartition::Full]);
    let ScopeResume::Walk(mut driver) = plan.walk_from_scratch() else {
        panic!("a failed read must not skip a fixed plan");
    };
    assert_eq!(
        driver.next_partition(),
        Some(bifrost_types::InventoryPartition::Full)
    );

    let plan = BackfillPlan::OpenPages { chunk: 250 };
    let ScopeResume::Walk(mut driver) = plan.walk_from_scratch() else {
        panic!("a failed read must not skip a page walk");
    };
    assert_eq!(
        driver.next_partition(),
        Some(bifrost_types::InventoryPartition::Page { from: 0, to: 250 }),
        "a failed read restarts at page 0, never at a guessed offset"
    );
}
