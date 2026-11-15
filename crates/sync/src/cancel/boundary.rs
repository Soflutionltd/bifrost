//! Safe-boundary primitive.
//!
//! The engine drives long streams through workers that emit `Batch`s
//! with checkpoints. Pause / checkpoint_now / hard-stop semantics
//! depend on the worker stopping cleanly at a `Batch` boundary - never
//! mid-page - so the checkpoint persists alongside the data.
//!
//! `BoundaryRequest` is a `tokio::sync::watch` value workers peek
//! before pulling the next batch from their underlying stream. The
//! engine flips the request from `Run` to `Pause` / `CheckpointNow` /
//! `Stop` via the `Control` surface; the worker reacts at the next
//! safe boundary.

use tokio::sync::watch;

/// Per-task safe-boundary request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BoundaryRequest {
    /// Default state. Worker proceeds.
    Run,
    /// Persist the next batch's checkpoint, then park until the
    /// request changes back to `Run` or to `Stop`.
    Pause,
    /// Persist the next batch's checkpoint, then restore the previous
    /// boundary state (`Run` or `Pause`) so checkpoint requests compose
    /// with an already-paused account.
    CheckpointNow,
    /// Persist the most recent checkpoint, emit `Done`, exit the
    /// worker.
    Stop,
}

/// Engine-side handle for flipping the boundary request.
#[derive(Debug, Clone)]
pub struct Boundary {
    tx: watch::Sender<BoundaryRequest>,
}

impl Boundary {
    #[must_use]
    pub fn new() -> (Self, BoundaryView) {
        let (tx, rx) = watch::channel(BoundaryRequest::Run);
        (Self { tx }, BoundaryView { rx })
    }

    /// Unconditional write. `Stop` is not special here: the engine's
    /// own teardown path needs to be able to write it over anything.
    /// All non-teardown writes, including engine recovery, should go through
    /// [`Self::set_unless_stopped`] instead.
    pub fn set(&self, req: BoundaryRequest) {
        self.tx.send_replace(req);
    }

    /// Write `req` unless a stop has already been requested. Returns
    /// whether the write happened.
    ///
    /// `Stop` is terminal: the account is tearing down and its workers
    /// are meant to flush a final checkpoint and exit. A consumer
    /// `pause` or `resume`, or an engine auto-pause, racing teardown
    /// must not resurrect them - a `Pause` written over `Stop` parks
    /// the workers instead of letting them drain, and `detach` waits out its whole
    /// timeout.
    pub fn set_unless_stopped(&self, req: BoundaryRequest) -> bool {
        self.tx.send_if_modified(|current| {
            if *current == BoundaryRequest::Stop || *current == req {
                return false;
            }
            *current = req;
            true
        })
    }

    /// Install `CheckpointNow`, returning the request it displaced so
    /// the caller can restore it once the checkpoint lands. `None`
    /// means nothing was installed and nothing should be restored.
    ///
    /// Reading the previous value and installing the request in one
    /// atomic step matters: a separate `snapshot()` + `set()` pair
    /// leaves a window where a concurrent `Pause` is overwritten by
    /// `CheckpointNow` and then restored as the stale pre-pause value,
    /// silently undoing the pause. `Stop` is refused outright (see
    /// [`Self::set_unless_stopped`]).
    pub fn request_checkpoint(&self) -> Option<BoundaryRequest> {
        let mut previous = None;
        self.tx.send_if_modified(|current| {
            if *current == BoundaryRequest::Stop {
                return false;
            }
            previous = Some(*current);
            if *current == BoundaryRequest::CheckpointNow {
                // Already latched by a concurrent request; no write, so
                // no spurious wakeup for parked workers.
                return false;
            }
            *current = BoundaryRequest::CheckpointNow;
            true
        });
        previous
    }

    /// Restore `replacement` only while the current request still
    /// equals `expected`. An interleaved pause, stop, or resume wins.
    pub fn restore_if_current(&self, expected: BoundaryRequest, replacement: BoundaryRequest) {
        self.tx.send_if_modified(|current| {
            if *current == expected {
                *current = replacement;
                true
            } else {
                false
            }
        });
    }

    #[must_use]
    pub fn snapshot(&self) -> BoundaryRequest {
        *self.tx.borrow()
    }

    #[must_use]
    pub fn sender(&self) -> watch::Sender<BoundaryRequest> {
        self.tx.clone()
    }

    pub fn subscribe(&self) -> BoundaryView {
        BoundaryView {
            rx: self.tx.subscribe(),
        }
    }
}

impl Default for Boundary {
    fn default() -> Self {
        Self::new().0
    }
}

impl From<watch::Sender<BoundaryRequest>> for Boundary {
    fn from(tx: watch::Sender<BoundaryRequest>) -> Self {
        Self { tx }
    }
}

/// Per-worker view of the boundary. Cheaply cloneable.
#[derive(Debug, Clone)]
pub struct BoundaryView {
    rx: watch::Receiver<BoundaryRequest>,
}

impl BoundaryView {
    /// Cheap, non-blocking read of the current request. Workers call
    /// this at the top of each batch iteration.
    #[must_use]
    pub fn peek(&self) -> BoundaryRequest {
        *self.rx.borrow()
    }

    /// Park until the request changes. Returns the new request or
    /// `None` if every sender has dropped (engine teardown).
    pub async fn changed(&mut self) -> Option<BoundaryRequest> {
        if self.rx.changed().await.is_err() {
            return None;
        }
        Some(*self.rx.borrow())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_request_is_run() {
        let (b, v) = Boundary::new();
        assert_eq!(b.snapshot(), BoundaryRequest::Run);
        assert_eq!(v.peek(), BoundaryRequest::Run);
    }

    #[test]
    fn set_propagates() {
        let (b, v) = Boundary::new();
        b.set(BoundaryRequest::Pause);
        assert_eq!(v.peek(), BoundaryRequest::Pause);
        b.set(BoundaryRequest::Stop);
        assert_eq!(v.peek(), BoundaryRequest::Stop);
    }

    #[test]
    fn stop_is_terminal_for_consumer_writes() {
        let (b, _v) = Boundary::new();
        b.set(BoundaryRequest::Stop);

        assert!(!b.set_unless_stopped(BoundaryRequest::Pause));
        assert_eq!(b.snapshot(), BoundaryRequest::Stop);
        assert!(!b.set_unless_stopped(BoundaryRequest::Run));
        assert_eq!(b.snapshot(), BoundaryRequest::Stop);
        assert_eq!(b.request_checkpoint(), None);
        assert_eq!(b.snapshot(), BoundaryRequest::Stop);
    }

    #[test]
    fn request_checkpoint_reports_the_request_it_displaced() {
        let (b, _v) = Boundary::new();
        assert_eq!(b.request_checkpoint(), Some(BoundaryRequest::Run));
        assert_eq!(b.snapshot(), BoundaryRequest::CheckpointNow);
        // A second, concurrent request sees the latch and reports it,
        // so its restore is a no-op against the first one's.
        assert_eq!(b.request_checkpoint(), Some(BoundaryRequest::CheckpointNow));
        assert_eq!(b.snapshot(), BoundaryRequest::CheckpointNow);
    }

    #[test]
    fn request_checkpoint_preserves_a_pause_for_restoration() {
        let (b, _v) = Boundary::new();
        b.set(BoundaryRequest::Pause);
        let previous = b.request_checkpoint().expect("checkpoint installed");
        assert_eq!(previous, BoundaryRequest::Pause);
        b.restore_if_current(BoundaryRequest::CheckpointNow, previous);
        assert_eq!(b.snapshot(), BoundaryRequest::Pause);
    }
}
