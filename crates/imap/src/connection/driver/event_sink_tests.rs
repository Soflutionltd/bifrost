#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Instant;

use tokio::sync::mpsc;

use super::*;
use crate::connection::typed_event::TypedEvent;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates a `DriverEventSink` with the given channel capacity and hard cap.
fn make_sink(channel_cap: usize, hard_cap: usize) -> (DriverEventSink, mpsc::Receiver<TypedEvent>) {
    let (tx, rx) = mpsc::channel(channel_cap);
    let sink = DriverEventSink::new(tx, Some(hard_cap));
    (sink, rx)
}

/// A resyncable event (EXISTS) tagged with a sequence number for identification.
fn resyncable_event(n: u32) -> TypedEvent {
    TypedEvent::Exists(n)
}

/// A critical event (Alert) for testing buffering behavior.
fn critical_event(msg: &str) -> TypedEvent {
    TypedEvent::Alert(msg.to_owned())
}

/// A requeryable event (`CapabilityChange`) for testing drop behavior.
fn requeryable_event() -> TypedEvent {
    TypedEvent::CapabilityChange(vec![])
}

/// Drains all available events from the receiver into a Vec.
fn drain_rx(rx: &mut mpsc::Receiver<TypedEvent>) -> Vec<TypedEvent> {
    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    events
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn basic_emit_with_capacity() {
    let (mut sink, mut rx) = make_sink(8, 16);
    sink.emit(resyncable_event(1)).unwrap();
    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], TypedEvent::Exists(1)));
}

#[test]
fn resyncable_dropped_when_full() {
    let (mut sink, _rx) = make_sink(1, 16);
    // Fill the channel.
    sink.emit(resyncable_event(1)).unwrap();
    // This one should be dropped (resyncable, channel full).
    sink.emit(resyncable_event(2)).unwrap();
    // No error  -  just silently dropped.
}

#[test]
fn requeryable_dropped_when_full() {
    let (mut sink, _rx) = make_sink(1, 16);
    // Fill the channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Requeryable should be dropped.
    sink.emit(requeryable_event()).unwrap();
}

#[test]
fn critical_buffered_when_full() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill the channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Critical event should be buffered in pending.
    sink.emit(critical_event("alert1")).unwrap();
    assert_eq!(sink.pending.len(), 1);

    // Drain the channel to make room.
    drain_rx(&mut rx);
    // Now drain_pending should flush the buffered critical event.
    sink.drain_pending_nonblocking().unwrap();
    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], TypedEvent::Alert(msg) if msg == "alert1"));
}

#[test]
fn overflow_marker_emitted_after_drops_and_drain() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill the channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Drop 3 resyncable events.
    sink.emit(resyncable_event(2)).unwrap();
    sink.emit(resyncable_event(3)).unwrap();
    sink.emit(resyncable_event(4)).unwrap();

    // Drain the channel to make room, then call drain_pending to emit marker.
    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0] {
        TypedEvent::QueueOverflow { dropped_count, .. } => {
            assert_eq!(*dropped_count, 3);
        }
        other => panic!("expected QueueOverflow, got {other:?}"),
    }
}

#[test]
fn cumulative_counts_mixed_drops() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill the channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Drop 2 resyncable + 1 requeryable.
    sink.emit(resyncable_event(2)).unwrap();
    sink.emit(requeryable_event()).unwrap();
    sink.emit(resyncable_event(3)).unwrap();

    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0] {
        TypedEvent::QueueOverflow { dropped_count, .. } => {
            assert_eq!(*dropped_count, 3); // 2 resyncable + 1 requeryable
        }
        other => panic!("expected QueueOverflow, got {other:?}"),
    }
}

#[test]
fn since_timestamp_is_from_first_drop() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill the channel.
    sink.emit(resyncable_event(1)).unwrap();

    let before = Instant::now();
    sink.emit(resyncable_event(2)).unwrap(); // first drop
    let after_first = Instant::now();

    // Second drop  -  should NOT update `since`.
    sink.emit(resyncable_event(3)).unwrap();

    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0] {
        TypedEvent::QueueOverflow { since, .. } => {
            assert!(*since >= before);
            assert!(*since <= after_first);
        }
        other => panic!("expected QueueOverflow, got {other:?}"),
    }
}

#[test]
fn no_marker_when_no_drops() {
    let (mut sink, mut rx) = make_sink(8, 16);
    sink.emit(resyncable_event(1)).unwrap();
    sink.emit(resyncable_event(2)).unwrap();

    // Drain channel and call drain_pending  -  should be a no-op for marker.
    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    assert!(events.is_empty(), "no marker expected, got {events:?}");
}

#[test]
fn stats_reset_after_marker_delivered() {
    let (mut sink, mut rx) = make_sink(2, 16);

    // --- Window 1: drop 2 events ---
    // Fill the channel.
    sink.emit(resyncable_event(1)).unwrap();
    sink.emit(resyncable_event(2)).unwrap();
    // These are dropped.
    sink.emit(resyncable_event(10)).unwrap();
    sink.emit(resyncable_event(11)).unwrap();

    // Drain and flush marker.
    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();
    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0] {
        TypedEvent::QueueOverflow { dropped_count, .. } => {
            assert_eq!(*dropped_count, 2);
        }
        other => panic!("expected QueueOverflow window 1, got {other:?}"),
    }

    // --- Window 2: drop 1 event ---
    sink.emit(resyncable_event(20)).unwrap();
    sink.emit(resyncable_event(21)).unwrap();
    // This one is dropped.
    sink.emit(resyncable_event(22)).unwrap();

    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();
    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0] {
        TypedEvent::QueueOverflow { dropped_count, .. } => {
            assert_eq!(*dropped_count, 1);
        }
        other => panic!("expected QueueOverflow window 2, got {other:?}"),
    }
}

#[test]
fn critical_events_drain_before_marker() {
    let (mut sink, mut rx) = make_sink(2, 16);
    // Fill channel (cap=2).
    sink.emit(resyncable_event(1)).unwrap();
    sink.emit(resyncable_event(99)).unwrap();
    // Buffer a critical event.
    sink.emit(critical_event("alert-first")).unwrap();
    // Drop a resyncable event.
    sink.emit(resyncable_event(2)).unwrap();

    // Clear channel  -  now drain should send critical first, then marker.
    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    // Should have critical event first, then the overflow marker.
    assert!(
        events.len() >= 2,
        "expected at least 2 events, got {events:?}"
    );
    assert!(
        matches!(&events[0], TypedEvent::Alert(msg) if msg == "alert-first"),
        "first event should be alert, got {:?}",
        events[0]
    );
    assert!(
        matches!(
            &events[1],
            TypedEvent::QueueOverflow {
                dropped_count: 1,
                ..
            }
        ),
        "second event should be QueueOverflow with count=1, got {:?}",
        events[1]
    );
}

#[test]
fn marker_does_not_amplify_under_sustained_backpressure() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Drop some events.
    sink.emit(resyncable_event(2)).unwrap();
    sink.emit(resyncable_event(3)).unwrap();

    // Call drain_pending multiple times while channel is still full.
    // The marker cannot be sent, stats should be preserved.
    sink.drain_pending_nonblocking().unwrap();
    sink.drain_pending_nonblocking().unwrap();
    sink.drain_pending_nonblocking().unwrap();

    // Now free the channel and drain.
    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    // Exactly one marker, not three.
    let markers: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, TypedEvent::QueueOverflow { .. }))
        .collect();
    assert_eq!(
        markers.len(),
        1,
        "expected exactly 1 marker, got {markers:?}"
    );
    match markers[0] {
        TypedEvent::QueueOverflow { dropped_count, .. } => {
            assert_eq!(*dropped_count, 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn critical_overflow_at_hard_cap_is_reported_by_queue_marker() {
    let (mut sink, mut rx) = make_sink(1, 2);
    // Fill channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Buffer 2 critical events (hard_cap = 2).
    sink.emit(critical_event("c1")).unwrap();
    sink.emit(critical_event("c2")).unwrap();
    // The third critical event is dropped at the cap, but its loss must be
    // visible even though response-routing callers intentionally ignore emit's
    // result.
    sink.emit(critical_event("c3")).unwrap();
    assert_eq!(sink.drop_stats.dropped_count(), 1);

    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();
    assert!(matches!(drain_rx(&mut rx).as_slice(), [TypedEvent::Alert(msg)] if msg == "c1"));

    sink.drain_pending_nonblocking().unwrap();
    assert!(matches!(drain_rx(&mut rx).as_slice(), [TypedEvent::Alert(msg)] if msg == "c2"));

    sink.drain_pending_nonblocking().unwrap();
    let events = drain_rx(&mut rx);
    assert!(matches!(
        events.as_slice(),
        [TypedEvent::QueueOverflow {
            dropped_count: 1,
            ..
        }]
    ));
}

#[test]
fn event_after_a_full_drain_stays_behind_pending_critical_events() {
    let (mut sink, mut rx) = make_sink(1, 4);
    sink.emit(resyncable_event(1)).unwrap();
    sink.emit(critical_event("older")).unwrap();

    // Model the receiver freeing a slot just after drain_pending observed
    // the full channel. emit_after_drain is the path emit takes next.
    drain_rx(&mut rx);
    sink.emit_after_drain(critical_event("newer")).unwrap();

    assert!(
        rx.try_recv().is_err(),
        "new event overtook the older pending one"
    );
    assert_eq!(sink.pending.len(), 2);
    assert!(matches!(sink.pending.front(), Some(TypedEvent::Alert(msg)) if msg == "older"));
    assert!(matches!(sink.pending.back(), Some(TypedEvent::Alert(msg)) if msg == "newer"));
}

#[test]
fn events_arriving_while_a_loss_marker_is_blocked_join_its_loss_window() {
    let (mut sink, mut rx) = make_sink(1, 4);
    sink.emit(resyncable_event(1)).unwrap();
    sink.emit(critical_event("before-loss")).unwrap();
    sink.emit(resyncable_event(2)).unwrap();
    assert_eq!(sink.drop_stats.dropped_count(), 1);

    // Free the original slot. The next drain can publish the old critical
    // event, but that fills the slot and keeps its loss marker pending.
    drain_rx(&mut rx);
    sink.emit(critical_event("after-loss")).unwrap();
    assert!(sink.pending.is_empty());
    assert_eq!(sink.drop_stats.dropped_count(), 2);
    assert!(matches!(
        drain_rx(&mut rx).as_slice(),
        [TypedEvent::Alert(msg)] if msg == "before-loss"
    ));

    sink.drain_pending_nonblocking().unwrap();
    assert!(matches!(
        drain_rx(&mut rx).as_slice(),
        [TypedEvent::QueueOverflow {
            dropped_count: 2,
            ..
        }]
    ));
}

#[test]
fn caller_gone_when_receiver_dropped() {
    let (mut sink, rx) = make_sink(1, 16);
    drop(rx);
    let result = sink.emit(resyncable_event(1));
    assert!(
        matches!(result, Err(EventOverflow::CallerGone)),
        "expected CallerGone, got {result:?}"
    );
}

#[test]
fn caller_gone_during_drain_pending() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Buffer a critical event.
    sink.emit(critical_event("c1")).unwrap();

    // Drain channel, then drop receiver.
    drain_rx(&mut rx);
    drop(rx);

    // drain_pending should detect CallerGone when trying to flush pending.
    let result = sink.drain_pending_nonblocking();
    assert!(
        matches!(result, Err(EventOverflow::CallerGone)),
        "expected CallerGone, got {result:?}"
    );
}

#[test]
fn marker_retried_on_next_drain_after_full() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Drop an event to create stats.
    sink.emit(resyncable_event(2)).unwrap();

    // drain_pending with channel still full  -  marker can't be sent, stats preserved.
    sink.drain_pending_nonblocking().unwrap();
    assert!(
        sink.drop_stats.dropped_count() > 0,
        "stats should be preserved when marker can't be sent"
    );

    // Now free the channel.
    drain_rx(&mut rx);
    // Second drain should deliver the marker.
    sink.drain_pending_nonblocking().unwrap();
    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(
        matches!(
            &events[0],
            TypedEvent::QueueOverflow {
                dropped_count: 1,
                ..
            }
        ),
        "expected QueueOverflow with count=1, got {:?}",
        events[0]
    );
}

#[test]
fn drops_accumulate_within_one_window() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Drop 5 events across multiple emit calls.
    for i in 2..=6 {
        sink.emit(resyncable_event(i)).unwrap();
    }

    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0] {
        TypedEvent::QueueOverflow { dropped_count, .. } => {
            assert_eq!(*dropped_count, 5);
        }
        other => panic!("expected QueueOverflow, got {other:?}"),
    }
}

#[test]
fn no_double_marker_from_drain_then_emit() {
    let (mut sink, mut rx) = make_sink(2, 16);
    // Fill channel.
    sink.emit(resyncable_event(1)).unwrap();
    sink.emit(resyncable_event(2)).unwrap();
    // Drop one.
    sink.emit(resyncable_event(3)).unwrap();

    // Free one slot (channel cap=2, so one slot opens).
    rx.try_recv().unwrap();
    // drain_pending_nonblocking delivers the marker into the free slot.
    sink.drain_pending_nonblocking().unwrap();
    // Immediately emit another event  -  should NOT produce a second marker.
    // Free one more slot first.
    rx.try_recv().unwrap();
    sink.emit(resyncable_event(4)).unwrap();

    let events = drain_rx(&mut rx);
    let markers: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, TypedEvent::QueueOverflow { .. }))
        .collect();
    assert_eq!(
        markers.len(),
        1,
        "expected exactly 1 marker total, got {markers:?}"
    );
}

#[test]
fn drain_pending_noop_when_nothing_to_do() {
    let (mut sink, mut rx) = make_sink(8, 16);
    // Nothing pending, no drops.
    sink.drain_pending_nonblocking().unwrap();
    let events = drain_rx(&mut rx);
    assert!(events.is_empty());
}

#[test]
fn caller_gone_during_marker_send() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill channel and drop an event to create stats.
    sink.emit(resyncable_event(1)).unwrap();
    sink.emit(resyncable_event(2)).unwrap();

    // Drain the channel so pending is empty, then drop the receiver
    // before drain_pending can send the marker.
    drain_rx(&mut rx);
    drop(rx);

    // drain_pending: pending is empty, dropped_count > 0, tries to send
    // marker via try_send  -  hits Closed, returns CallerGone.
    let result = sink.drain_pending_nonblocking();
    assert!(
        matches!(result, Err(EventOverflow::CallerGone)),
        "expected CallerGone during marker send, got {result:?}"
    );
}

#[test]
fn drops_accumulate_while_marker_is_deferred() {
    let (mut sink, mut rx) = make_sink(1, 16);
    // Fill channel.
    sink.emit(resyncable_event(1)).unwrap();
    // Drop 2 events.
    sink.emit(resyncable_event(2)).unwrap();
    sink.emit(resyncable_event(3)).unwrap();

    // drain_pending: channel still full, marker can't send, stats restored.
    sink.drain_pending_nonblocking().unwrap();
    assert_eq!(
        sink.drop_stats.dropped_count(),
        2,
        "stats should be preserved"
    );

    // Drop 3 more events while the marker is deferred.
    sink.emit(resyncable_event(4)).unwrap();
    sink.emit(resyncable_event(5)).unwrap();
    sink.emit(resyncable_event(6)).unwrap();
    assert_eq!(
        sink.drop_stats.dropped_count(),
        5,
        "new drops should accumulate"
    );

    // Now free the channel and drain.
    drain_rx(&mut rx);
    sink.drain_pending_nonblocking().unwrap();

    let events = drain_rx(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0] {
        TypedEvent::QueueOverflow { dropped_count, .. } => {
            assert_eq!(*dropped_count, 5, "marker should have the combined count");
        }
        other => panic!("expected QueueOverflow, got {other:?}"),
    }
}
