//! A provider stream that neither yields nor ends must not park its driver.
//!
//! The shape these pin is a wedge, not a slow server: `changes_stream` returns
//! a stream whose next poll is `Pending` forever. Before the poll arms landed,
//! `drive_changes_stream` sat on that poll with no arm at all, and the cost was
//! not the bounded `detach_timeout` the item was filed under. Per-scope poll
//! tasks are spawned with their `JoinHandle` discarded, so a wedged drive is
//! not among the workers `detach` waits on: it cost detach nothing and SURVIVED
//! it, holding an `Arc<dyn Account>`, a scheduler admission and a
//! `ChangeDelivery`, still able to publish into a `PendingCoverage` nobody
//! owned. `SyncControl::pause` was wedged by the same stream, because the
//! drive's activity guard is held across the poll.
//!
//! Every test here would hang rather than fail without the arm, so each one is
//! wrapped in a timeout: the assertion IS promptness.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::cancel::{Boundary, BoundaryRequest};
use bifrost_sync::cursor::CursorRegistry;
use bifrost_sync::multiplexer::{ChangeDelivery, ChangesEvent, drive_changes_stream};
use bifrost_types::{Account, CursorScope};
use tokio::sync::broadcast;

use common::{StubAccount, cursor_for};

/// Long enough that a scheduling hiccup cannot fail the test, short enough that
/// a missing arm cannot pass it. The real distinction is unbounded versus
/// prompt, not one duration versus another.
const PROMPT: Duration = Duration::from_secs(5);

struct Harness {
    account: Arc<dyn Account>,
    cursors: Arc<CursorRegistry>,
    delivery: Arc<ChangeDelivery>,
    release: Arc<tokio::sync::Notify>,
    _rx: broadcast::Receiver<bifrost_sync::MultiplexerEvent>,
}

impl Harness {
    /// An owned future over the drive.
    ///
    /// `drive_changes_stream` borrows its account, so a spawned test needs
    /// something that holds the `Arc` for the duration.
    fn drive(
        &self,
        scope: CursorScope,
        cursor: bifrost_types::ChangeCursor,
        view: bifrost_sync::BoundaryView,
    ) -> impl std::future::Future<Output = Result<ChangesEvent, bifrost_sync::Error>> + Send + 'static
    {
        let account = Arc::clone(&self.account);
        let cursors = Arc::clone(&self.cursors);
        let delivery = Arc::clone(&self.delivery);
        async move {
            drive_changes_stream(
                account.as_ref(),
                scope,
                cursor,
                cursors,
                delivery,
                view,
                None,
                None,
            )
            .await
        }
    }
}

fn harness() -> Harness {
    let scope = CursorScope::Account;
    let mut stub = StubAccount::new(vec![scope]);
    let release = Arc::new(tokio::sync::Notify::new());
    stub.changes_stall = Some(Arc::clone(&release));
    let (tx, rx) = broadcast::channel(16);
    Harness {
        account: Arc::new(stub),
        cursors: Arc::new(CursorRegistry::new()),
        delivery: Arc::new(ChangeDelivery::new(tx)),
        release,
        _rx: rx,
    }
}

/// `Stop` published while the drive is parked on the provider ends it.
///
/// This is the detach case: `DetachingAccount::begin` publishes `Stop` on the boundary
/// before it cancels the slot, so the boundary is the signal that is reliably
/// set by the time it matters.
#[tokio::test]
async fn a_stop_ends_a_drive_parked_on_a_wedged_provider() {
    let h = harness();
    let (boundary, view) = Boundary::new();
    let scope = CursorScope::Account;
    let cursor = cursor_for(&scope, b"stalled");

    let drive = tokio::spawn(h.drive(scope, cursor, view));

    // Let the drive reach its poll and park before the request lands, so this
    // exercises the SELECT arm rather than the entry peek.
    tokio::task::yield_now().await;
    boundary.set(BoundaryRequest::Stop);

    let outcome = tokio::time::timeout(PROMPT, drive)
        .await
        .expect("a stopped drive must not wait on the provider")
        .expect("drive task")
        .expect("stopping is not an error");
    assert!(matches!(outcome, ChangesEvent::Stopped));
    // The provider was never released: the drive left of its own accord.
    h.release.notify_waiters();
}

/// `Pause` ends it too, and that is not a nicety.
///
/// `SyncControl::pause` waits for quiescence while the drive holds its activity
/// guard across the poll, so without this arm a wedged provider hangs `pause`
/// exactly as it hung `detach` - the same defect wearing different clothes.
#[tokio::test]
async fn a_pause_ends_a_drive_parked_on_a_wedged_provider() {
    let h = harness();
    let (boundary, view) = Boundary::new();
    let scope = CursorScope::Account;
    let cursor = cursor_for(&scope, b"stalled");

    let drive = tokio::spawn(h.drive(scope, cursor, view));

    tokio::task::yield_now().await;
    boundary.set(BoundaryRequest::Pause);

    let outcome = tokio::time::timeout(PROMPT, drive)
        .await
        .expect("a paused drive must not wait on the provider")
        .expect("drive task")
        .expect("pausing is not an error");
    assert!(matches!(outcome, ChangesEvent::Paused));
    h.release.notify_waiters();
}

/// A `Stop` the view has ALREADY SEEN still ends the drive.
///
/// This is the one case the select cannot cover, and the entry peek is the only
/// thing that answers it. `changed()` reports versions its receiver has not
/// observed, so merely setting `Stop` before the drive starts is NOT enough to
/// exercise the peek - a fresh view has not seen it, and `changed()` fires
/// normally. (That mistake was made here: the first version of this test passed
/// with the peek deleted.) The `changed()` below is what marks the version
/// seen, and it is not artificial: `spawn_scope_poll_inner`'s pause-park loop
/// consumes exactly this way, calling `changed()` and breaking out on a
/// non-Pause request, and the drive is then built from that same view.
///
/// After that consumption nothing will ever wake this drive again, so without
/// the peek it parks on the wedged provider forever.
#[tokio::test]
async fn a_stop_the_view_has_already_seen_still_ends_the_drive() {
    let h = harness();
    let (boundary, mut view) = Boundary::new();
    boundary.set(BoundaryRequest::Stop);
    // Consume the transition, exactly as the poll loop's pause park does.
    assert_eq!(view.changed().await, Some(BoundaryRequest::Stop));
    let scope = CursorScope::Account;
    let cursor = cursor_for(&scope, b"stalled");

    let outcome = tokio::time::timeout(PROMPT, h.drive(scope, cursor, view))
        .await
        .expect("a drive entered under Stop must not poll the provider at all")
        .expect("stopping is not an error");
    assert!(matches!(outcome, ChangesEvent::Stopped));
    h.release.notify_waiters();
}

/// `CheckpointNow` does NOT cut the poll.
///
/// It is the one request whose meaning is "give me a durable cursor", and only
/// a checkpoint-bearing event can supply one, so answering it by abandoning the
/// wait would be answering it with a lie. Stop and Pause are lifecycle
/// requests and need no checkpoint; this one is not.
///
/// Also the negative half of the arm: it proves the drive is not simply
/// returning on any boundary traffic whatsoever, which is the way this
/// mechanism would most plausibly be wrong.
#[tokio::test]
async fn a_checkpoint_request_does_not_cut_a_parked_poll() {
    let h = harness();
    let (boundary, view) = Boundary::new();
    let scope = CursorScope::Account;
    let cursor = cursor_for(&scope, b"stalled");

    let mut drive = tokio::spawn(h.drive(scope, cursor, view));

    tokio::task::yield_now().await;
    boundary.set(BoundaryRequest::CheckpointNow);
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut drive)
            .await
            .is_err(),
        "a checkpoint request is not satisfied by abandoning the wait for one"
    );

    // And the drive is genuinely still driving rather than dead: releasing the
    // provider ends the stream and the drive completes.
    h.release.notify_waiters();
    let outcome = tokio::time::timeout(PROMPT, drive)
        .await
        .expect("the released stream ends the drive")
        .expect("drive task")
        .expect("ending is not an error");
    assert!(matches!(outcome, ChangesEvent::Done));
}

/// A transition that is not a cancellation resumes the poll rather than ending
/// the drive.
///
/// `changed()` fires for every transition, so an implementation that treated
/// any wakeup as a stop would end a drive on a restoration to `Run`. The
/// sequence here is Run -> CheckpointNow -> Run, with no Stop and no Pause.
#[tokio::test]
async fn an_irrelevant_boundary_transition_resumes_the_poll() {
    let h = harness();
    let (boundary, view) = Boundary::new();
    let scope = CursorScope::Account;
    let cursor = cursor_for(&scope, b"stalled");

    let mut drive = tokio::spawn(h.drive(scope, cursor, view));

    tokio::task::yield_now().await;
    boundary.set(BoundaryRequest::CheckpointNow);
    tokio::task::yield_now().await;
    boundary.set(BoundaryRequest::Run);
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut drive)
            .await
            .is_err(),
        "neither transition asked the drive to stop"
    );

    h.release.notify_waiters();
    let outcome = tokio::time::timeout(PROMPT, drive)
        .await
        .expect("the released stream ends the drive")
        .expect("drive task")
        .expect("ending is not an error");
    assert!(matches!(outcome, ChangesEvent::Done));
}
