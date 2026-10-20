//! Non-blocking event sink for the driver task (I8).
//!
//! The driver task cannot suspend on the event queue because this type
//! has no async send method. The only publish operation is [`DriverEventSink::emit`],
//! which calls `try_send` and falls back to an internal pending buffer
//! for critical events.

use std::collections::VecDeque;

use tokio::sync::mpsc;

use super::super::typed_event::{Priority, TypedEvent};

/// Non-blocking event sink. The driver task cannot suspend on the
/// event queue because this type has no `async fn send` (I8).
///
/// Back-pressure strategy:
/// - **Critical** events (ALERTs, BYE, NOTIFICATIONOVERFLOW) are buffered
///   in a pending `VecDeque` when the external channel is full, up to
///   `hard_cap`. Critical events dropped at the cap or while an earlier loss
///   marker is pending are included in that marker.
/// - **Requeryable** / **Resyncable** events are dropped and counted for the
///   same marker (the caller can re-query the server for the latest state).
///
/// The driver task's `select!` loop calls
/// [`drain_pending_nonblocking`](Self::drain_pending_nonblocking) between
/// every `cmd_rx.recv()` tick so that critical events do not stall
/// during quiet periods.
pub(in crate::connection) struct DriverEventSink {
    /// External channel to the public event receiver.
    external: mpsc::Sender<TypedEvent>,
    /// Buffer for critical events that could not be sent immediately.
    pending: VecDeque<TypedEvent>,
    /// Maximum size of the pending buffer before we declare overflow.
    hard_cap: usize,
    /// Tracks how many events were dropped since the last successful drain.
    drop_stats: DropStats,
}

/// Statistics about dropped events during back-pressure.
#[derive(Default)]
struct DropStats {
    dropped_critical: usize,
    dropped_resyncable: usize,
    dropped_requeryable: usize,
    since: Option<std::time::Instant>,
}

impl DropStats {
    /// Total number of events dropped in this window.
    fn dropped_count(&self) -> usize {
        self.dropped_critical + self.dropped_resyncable + self.dropped_requeryable
    }
}

/// Default maximum pending-buffer size before critical events are counted as
/// dropped.
const DEFAULT_HARD_CAP: usize = 4096;

/// Error returned by [`DriverEventSink::emit`] when the sink cannot
/// accept the event.
#[derive(Debug, thiserror::Error)]
pub(in crate::connection::driver) enum EventOverflow {
    /// External receiver has been dropped. The connection is closing.
    #[error("event receiver dropped")]
    CallerGone,
}

impl DriverEventSink {
    /// Creates a new sink wrapping the given external channel.
    ///
    /// `hard_cap` limits the pending buffer for critical events.
    /// Defaults to [`DEFAULT_HARD_CAP`] (4096) if `None`.
    pub(in crate::connection) fn new(
        external: mpsc::Sender<TypedEvent>,
        hard_cap: Option<usize>,
    ) -> Self {
        Self {
            external,
            pending: VecDeque::new(),
            hard_cap: hard_cap.unwrap_or(DEFAULT_HARD_CAP).max(1),
            drop_stats: DropStats::default(),
        }
    }

    /// The ONLY publish operation. Never blocks. Never awaits.
    ///
    /// Drains previously-buffered critical events and their loss marker
    /// before handling the new event. New events cannot overtake critical
    /// events that remain pending if the receiver frees a slot during a
    /// drain. Critical events that exceed the buffer cap are counted for
    /// the loss marker.
    pub(in crate::connection::driver) fn emit(
        &mut self,
        ev: TypedEvent,
    ) -> Result<(), EventOverflow> {
        self.drain_pending()?;
        self.emit_after_drain(ev)
    }

    /// Handles an event only after older pending events have been drained.
    /// If a receiver frees a slot just after `drain_pending` observed a full
    /// channel, keeping later critical events behind the older ones preserves
    /// their wire order. Once there is a loss to report, later events join
    /// that loss window until its marker is delivered.
    fn emit_after_drain(&mut self, ev: TypedEvent) -> Result<(), EventOverflow> {
        let priority = ev.priority();
        if self.drop_stats.dropped_count() > 0 {
            self.record_drop(priority);
            return Ok(());
        }
        if !self.pending.is_empty() {
            match priority {
                Priority::Critical => self.buffer_critical(ev),
                Priority::Requeryable => self.record_drop_requeryable(),
                Priority::Resyncable => self.record_drop_resyncable(),
            }
            return Ok(());
        }
        self.try_emit(ev)
    }

    /// Drain the pending buffer opportunistically. Called by the driver
    /// task's `select!` loop between every `cmd_rx.recv()` tick so that
    /// critical events do not stall during quiet periods.
    ///
    /// Returns `Err(CallerGone)` if the external receiver has been
    /// dropped, allowing the driver to shut down promptly.
    pub(in crate::connection::driver) fn drain_pending_nonblocking(
        &mut self,
    ) -> Result<(), EventOverflow> {
        self.drain_pending()
    }

    /// Attempts to flush buffered critical events to the external channel,
    /// then emits a single [`TypedEvent::QueueOverflow`] marker if any
    /// events were dropped since the last successful drain.
    ///
    /// The marker is emitted at the end, after all pending critical events
    /// have been delivered. This ensures the caller sees the critical
    /// events first, then the summary of what was lost.
    ///
    /// # Marker lifecycle
    ///
    /// The marker is created ephemerally from `drop_stats`  -  it is never
    /// placed in the `pending` buffer. If `try_send` fails (`Full`), the
    /// stats are restored and the marker will be re-created on the next
    /// drain cycle. This makes amplification impossible: the marker
    /// cannot trigger another marker on the next drain.
    ///
    /// Despite being classified as [`Priority::Critical`], `QueueOverflow`
    /// bypasses the `try_emit` back-pressure path and is sent directly
    /// from here. The classification is documentary (it IS critical
    /// information) but the routing is different from Alert/BYE.
    ///
    /// # Analogy
    ///
    /// This is the internal counterpart of RFC 5465 Section 5.8
    /// `NOTIFICATIONOVERFLOW`: where the server tells the client "I
    /// dropped notifications, resync", our `QueueOverflow` marker tells
    /// the caller "the driver dropped events, resync".
    #[allow(clippy::expect_used)] // Invariant: `since` is always `Some` when `dropped_count() > 0`.
    fn drain_pending(&mut self) -> Result<(), EventOverflow> {
        while let Some(ev) = self.pending.pop_front() {
            match self.external.try_send(ev) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(ev)) => {
                    self.pending.push_front(ev);
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.pending.clear();
                    return Err(EventOverflow::CallerGone);
                }
            }
        }

        // All pending critical events drained. If events were dropped
        // during this window, emit a single QueueOverflow summary.
        if self.drop_stats.dropped_count() > 0 {
            let stats = std::mem::take(&mut self.drop_stats);
            let marker = TypedEvent::QueueOverflow {
                dropped_count: stats.dropped_count(),
                // `since` is always `Some` when `dropped_count() > 0` because
                // every record_drop_* method sets it on the first drop.
                since: stats.since.expect(
                    "since is always Some when dropped_count > 0 \
                     (set by a record_drop_* method)",
                ),
            };
            match self.external.try_send(marker) {
                Ok(()) => {} // Marker delivered, stats already cleared.
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Channel full  -  restore stats so the marker is
                    // retried on the next drain cycle. The marker is
                    // NOT buffered in `pending`; it will be re-created
                    // from the restored stats next time.
                    self.drop_stats = stats;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(EventOverflow::CallerGone);
                }
            }
        }

        Ok(())
    }

    /// Attempts to send a single event, applying back-pressure policy.
    fn try_emit(&mut self, ev: TypedEvent) -> Result<(), EventOverflow> {
        match self.external.try_send(ev) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(ev)) => match ev.priority() {
                Priority::Critical => {
                    self.buffer_critical(ev);
                    Ok(())
                }
                Priority::Requeryable => {
                    self.record_drop_requeryable();
                    Ok(())
                }
                Priority::Resyncable => {
                    self.record_drop_resyncable();
                    Ok(())
                }
            },
            Err(mpsc::error::TrySendError::Closed(_)) => Err(EventOverflow::CallerGone),
        }
    }

    /// Buffers a critical event unless the bounded queue is full. A critical
    /// event rejected at the cap still contributes to the loss marker, so
    /// every loss remains observable even though `emit` results are often
    /// intentionally ignored by response-routing code.
    fn buffer_critical(&mut self, ev: TypedEvent) {
        if self.pending.len() >= self.hard_cap {
            self.record_drop_critical();
        } else {
            self.pending.push_back(ev);
        }
    }

    fn record_drop_critical(&mut self) {
        if self.drop_stats.since.is_none() {
            self.drop_stats.since = Some(std::time::Instant::now());
        }
        self.drop_stats.dropped_critical += 1;
    }

    fn record_drop(&mut self, priority: Priority) {
        match priority {
            Priority::Critical => self.record_drop_critical(),
            Priority::Requeryable => self.record_drop_requeryable(),
            Priority::Resyncable => self.record_drop_resyncable(),
        }
    }

    /// Records that a requeryable event was dropped.
    fn record_drop_requeryable(&mut self) {
        if self.drop_stats.since.is_none() {
            self.drop_stats.since = Some(std::time::Instant::now());
        }
        self.drop_stats.dropped_requeryable += 1;
    }

    /// Records that a resyncable event was dropped.
    fn record_drop_resyncable(&mut self) {
        if self.drop_stats.since.is_none() {
            self.drop_stats.since = Some(std::time::Instant::now());
        }
        self.drop_stats.dropped_resyncable += 1;
    }
}

// DELIBERATELY ABSENT  -  do NOT add:
// - async fn send(&self, ev: TypedEvent)
// - impl Sink<TypedEvent> for DriverEventSink
// - pub fn external(&self) -> &mpsc::Sender<TypedEvent>
// - pub fn external_clone(&self) -> mpsc::Sender<TypedEvent>
// These absences are the enforcement of I8. Leaking or cloning the
// inner sender would let callers call `.send().await` on it.

#[cfg(test)]
#[path = "event_sink_tests.rs"]
mod tests;
