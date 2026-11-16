//! Delivery checkpoint contract contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_types::{AccountId, CursorScope, SyncEvent};
use common::{caps, cursor_for};
use recovery::*;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize as BoundaryCounter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// A provider that violates the batch boundary contract must STOP the scope,
/// not be logged and retried.
///
/// `Batch`'s fields are public, so nothing prevents an account from emitting
/// `PageBoundary::Partial` with a checkpoint attached. The first guard against
/// it returned a bare engine error, which `handle_drive_outcome` only logs
/// before re-entering the poll loop from the same unchanged cursor: against a
/// provider that keeps doing it, that is an unbounded run of requests with no
/// recovery dispatch, no scope stop, and nothing on the stream telling a
/// consumer the scope has quietly stopped making progress.
///
/// The stub's stream is endless on purpose. Exactly one batch may be drawn
/// from it, the driver must report `Terminated`, the classified error must be
/// terminal so `plan_recovery` stops the scope rather than scheduling another
/// attempt, and a subscriber must see the termination.
#[tokio::test]
async fn a_partial_batch_carrying_a_checkpoint_terminates_the_scope() {
    use bifrost_sync::cancel::Boundary;
    use bifrost_sync::cursor::CursorRegistry;
    use bifrost_sync::multiplexer::{ChangesEvent, drive_changes_stream};

    let scope = CursorScope::Account;
    let emitted = Arc::new(BoundaryCounter::new(0));
    let account = HealAccount {
        caps: caps(),
        scopes: vec![scope.clone()],
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        generation: 0,
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
        degraded_contacts: false,
        close_gate: no_close_gate(),
        bad_boundary_batches: Some(Arc::clone(&emitted)),
        live_checkpoint_batch: false,
    }
    .into_stub();

    let (changes_tx, mut changes_rx) = tokio::sync::broadcast::channel(16);
    let (boundary, _handle) = Boundary::new();
    let outcome = drive_changes_stream(
        &account,
        scope.clone(),
        cursor_for(&scope, b"live"),
        Arc::new(CursorRegistry::new()),
        Arc::new(bifrost_sync::delivery::ChangeDelivery::new(changes_tx)),
        boundary.subscribe(),
        None,
        None,
    )
    .await
    .expect("the violation is reported as a terminated scope, not an engine error");

    let ChangesEvent::Terminated(error) = outcome else {
        panic!("a boundary violation must terminate the scope, got {outcome:?}");
    };
    assert!(
        error.recovery().is_terminal(),
        "a non-terminal class would send the poll loop back at the same cursor: {:?}",
        error.recovery()
    );
    assert_eq!(
        emitted.load(Ordering::SeqCst),
        1,
        "the driver must stop on the first bad batch, never keep drawing from a provider that repeats it"
    );

    let published = changes_rx
        .try_recv()
        .expect("subscribers must be told the scope terminated");
    assert!(
        matches!(published.event.as_ref(), SyncEvent::Terminated(_)),
        "the published event must be the termination, got {:?}",
        published.event
    );
}

/// Every checkpoint the live driver publishes must carry a publication id.
///
/// The acknowledgement path resolves coverage BY PUBLICATION, not by checkpoint
/// equality - two backfill pages can produce byte-identical checkpoints, and
/// fusion publishes its final checkpoint twice. A checkpoint broadcast without
/// an id therefore cannot be acknowledged against its own claim: the writer
/// either finds no claim at all or, worse, matches a neighbour's. The invariant
/// held when it was audited, and this pins it, because nothing else did.
#[tokio::test]
async fn a_published_change_checkpoint_always_carries_its_publication_id() {
    use bifrost_sync::cancel::Boundary;
    use bifrost_sync::cursor::CursorRegistry;
    use bifrost_sync::multiplexer::drive_changes_stream;

    let scope = CursorScope::Account;
    let account = HealAccount {
        caps: caps(),
        scopes: vec![scope.clone()],
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        generation: 0,
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
        degraded_contacts: false,
        close_gate: no_close_gate(),
        bad_boundary_batches: None,
        live_checkpoint_batch: true,
    }
    .into_stub();

    let (changes_tx, mut changes_rx) = tokio::sync::broadcast::channel(16);
    let (boundary, _handle) = Boundary::new();
    let (priority, _priority_rx) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _bandwidth_rx) = tokio::sync::watch::channel(None);
    let control = bifrost_sync::control::SyncControl::new(
        AccountId("publishes-ids".into()),
        boundary.clone(),
        priority,
        bandwidth,
    );
    drive_changes_stream(
        &account,
        scope.clone(),
        cursor_for(&scope, b"live"),
        Arc::new(CursorRegistry::new()),
        Arc::new(bifrost_sync::delivery::ChangeDelivery::new(changes_tx)),
        boundary.subscribe(),
        Some(control),
        None,
    )
    .await
    .expect("the stub stream completes cleanly");

    let published = changes_rx.try_recv().expect("a batch reached subscribers");
    assert!(
        published.checkpoint.is_some(),
        "the stub emits a checkpoint-bearing batch"
    );
    assert!(
        published.ack_token().cloned().is_some(),
        "a checkpoint published without an id cannot be acknowledged against its own claim"
    );
}
