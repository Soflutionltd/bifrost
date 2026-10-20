//! Push reconciler + `InvalidationSink` implementation.
//!
//! Two push paths converge on one per-account `mpsc::Sender<WatchEvent>`:
//! - In-process: the multiplexer forwards `Account::push_stream` items.
//! - Out-of-process: the consumer holds an `Arc<dyn InvalidationSink>`
//!   (from `SyncEngine::invalidation_sink`) and calls `push` from its
//!   Pub/Sub or webhook receiver.
//!
//! The reconciler reads the merged stream and runs `changes_stream`
//! for each affected scope via `drive_to_completion`. Output flows to
//! the same broadcast as the multiplexer's, so consumers do not see
//! "push-derived" vs "poll-derived" changes - just one unified
//! `Change` stream.

pub mod reconciler;
pub mod subscription;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountId, HintPayload, InvalidationHint, InvalidationSink, PushSource, WatchEvent,
};
use dashmap::DashMap;
use tokio::sync::mpsc;

pub use reconciler::{Reconciler, scopes_for_hint};
pub(crate) use subscription::RegisteredSubscription;
pub use subscription::SubscriptionRegistry;

/// Engine-side `InvalidationSink` implementation.
///
/// `senders` is the registry of per-account mpsc senders. `attach`
/// inserts; `detach` removes. The drop counter is exposed as
/// `bifrost_sync_push_dropped_total`.
#[derive(Debug)]
pub struct InvalidationSinkInner {
    senders: DashMap<AccountId, SinkSender>,
    drop_counter: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
enum SinkSender {
    Queued(mpsc::UnboundedSender<WatchEvent>),
    Direct(mpsc::Sender<WatchEvent>),
}

impl InvalidationSinkInner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            senders: DashMap::new(),
            drop_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn register(&self, account: AccountId, sender: mpsc::Sender<WatchEvent>) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // A single per-registration forwarder is the ordering point for
            // every out-of-process event. In particular, once one event has
            // to wait for bounded queue space, a later push cannot bypass it
            // through a successful `try_send`.
            // A sticky full-reconcile bit can replace invalidation payloads,
            // but cannot encode arbitrary Warning or Terminated payloads or
            // preserve their order. This synchronous, non-blocking sink has
            // no backpressure result: making ingress bounded requires an
            // explicit control-event overflow policy or a different API.
            // Retain the queue until that policy is chosen; a bit alone would
            // silently discard control information this path promises to keep.
            let (ingress_tx, ingress_rx) = mpsc::unbounded_channel();
            let drops = Arc::clone(&self.drop_counter);
            handle.spawn(forward_events(ingress_rx, sender, drops, None));
            self.senders.insert(account, SinkSender::Queued(ingress_tx));
            return;
        }
        // `register` is engine machinery and normally runs on the engine's
        // Tokio runtime. Keep the off-runtime fallback explicit for callers
        // that construct the sink directly: it cannot wait for capacity.
        self.senders.insert(account, SinkSender::Direct(sender));
    }

    pub fn unregister(&self, account: &AccountId) {
        self.senders.remove(account);
    }

    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.drop_counter.load(Ordering::Relaxed)
    }
}

async fn forward_events(
    mut ingress: mpsc::UnboundedReceiver<WatchEvent>,
    sender: mpsc::Sender<WatchEvent>,
    drops: Arc<AtomicU64>,
    waiting: Option<mpsc::UnboundedSender<()>>,
) {
    while let Some(event) = ingress.recv().await {
        match sender.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => break,
            Err(mpsc::error::TrySendError::Full(rejected)) => {
                let lossless = requires_lossless_delivery(&rejected);
                let delivery = if lossless {
                    rejected
                } else {
                    // Count payload coarsening immediately, even if the
                    // replacement is delivered. A later timeout can also
                    // lose delivery; the metric cannot distinguish the two.
                    drops.fetch_add(1, Ordering::Relaxed);
                    coalesced_event(rejected)
                };
                if let Some(waiting) = &waiting {
                    let _ = waiting.send(());
                }
                if lossless {
                    if sender.send(delivery).await.is_err() {
                        break;
                    }
                } else {
                    // This timeout can lose the reconcile request itself,
                    // not merely its specific payload. Removing it alone
                    // would let one stalled reconciler retain an unlimited
                    // ingress backlog. A durable pending bit needs a wakeup
                    // owned by the reconciler as well as a control-event
                    // overflow policy; it is not a replacement for this whole
                    // mixed event queue.
                    let deadline = tokio::time::Duration::from_millis(100);
                    let _ = tokio::time::timeout(deadline, sender.send(delivery)).await;
                }
            }
        }
    }
}

impl Default for InvalidationSinkInner {
    fn default() -> Self {
        Self::new()
    }
}

impl InvalidationSink for InvalidationSinkInner {
    fn push(&self, account: AccountId, event: WatchEvent) {
        let Some(tx) = self.senders.get(&account) else {
            return;
        };
        let SinkSender::Direct(tx) = tx.value() else {
            if let SinkSender::Queued(tx) = tx.value() {
                let _ = tx.send(event);
            }
            return;
        };
        match tx.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(rejected)) => {
                let _ = rejected;
                self.drop_counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Account detached during in-flight push.
            }
        }
    }
}

#[must_use]
pub(crate) fn requires_lossless_delivery(event: &WatchEvent) -> bool {
    matches!(event, WatchEvent::Terminated(_) | WatchEvent::Warning(_))
}

pub(crate) fn coalesced_event(event: WatchEvent) -> WatchEvent {
    let source = match event {
        WatchEvent::Invalidated { hint } => hint.source,
        WatchEvent::Disconnected | WatchEvent::Reconnected => PushSource::Coalesced,
        _ => PushSource::Coalesced,
    };
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source,
            payload: HintPayload::Unknown,
        },
    }
}

/// Convenience: wrap in an `Arc`.
#[must_use]
pub fn shared_sink() -> Arc<InvalidationSinkInner> {
    Arc::new(InvalidationSinkInner::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountOperation, CursorScope, Warning, WarningKind};

    fn invalidated() -> WatchEvent {
        WatchEvent::Invalidated {
            hint: InvalidationHint {
                source: PushSource::ImapNotify,
                payload: HintPayload::Unknown,
            },
        }
    }

    #[tokio::test]
    async fn full_sink_preserves_warning_from_a_non_runtime_thread() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("warning".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx.clone());
        tx.try_send(invalidated()).expect("fill queue");

        let outside = Arc::clone(&sink);
        std::thread::spawn(move || {
            outside.push(
                account,
                WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "preserve me")),
            );
        })
        .join()
        .expect("push thread");

        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        let delivered = rx.recv().await;
        assert!(matches!(delivered, Some(WatchEvent::Warning(_))));
        assert_eq!(sink.dropped(), 0, "a delivered warning was not dropped");
    }

    #[tokio::test]
    async fn full_sink_preserves_terminated_classification() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("terminated".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx.clone());
        tx.try_send(invalidated()).expect("fill queue");
        let error = crate::recovery::restart_scope_error(
            CursorScope::Account,
            AccountOperation::SyncChanges,
        );

        sink.push(account, WatchEvent::Terminated(error));

        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        let delivered = rx.recv().await;
        assert!(matches!(delivered, Some(WatchEvent::Terminated(_))));
        assert_eq!(sink.dropped(), 0, "a delivered termination was not dropped");
    }

    #[tokio::test]
    async fn full_sink_counts_a_coalesced_invalidation_as_dropped() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("coalesced".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx.clone());
        tx.try_send(invalidated()).expect("fill queue");

        sink.push(account, invalidated());

        tokio::time::timeout(tokio::time::Duration::from_secs(1), async {
            while sink.dropped() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("forwarder observes the full queue");

        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        assert_eq!(sink.dropped(), 1, "the replaced invalidation was dropped");
    }

    #[tokio::test]
    async fn queued_forwarder_cannot_deliver_a_later_event_first() {
        let (bounded_tx, mut bounded_rx) = mpsc::channel(1);
        bounded_tx
            .try_send(invalidated())
            .expect("fill bounded queue");
        let (ingress_tx, ingress_rx) = mpsc::unbounded_channel();
        let (waiting_tx, mut waiting_rx) = mpsc::unbounded_channel();
        let drops = Arc::new(AtomicU64::new(0));
        let worker = tokio::spawn(forward_events(
            ingress_rx,
            bounded_tx,
            drops,
            Some(waiting_tx),
        ));
        let first = WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "first"));
        let second = WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "second"));
        ingress_tx.send(first).expect("queue first");
        waiting_rx.recv().await.expect("first is waiting for space");
        ingress_tx.send(second).expect("queue second behind first");

        assert!(matches!(
            bounded_rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        let WatchEvent::Warning(first) = bounded_rx.recv().await.expect("first warning") else {
            panic!("first queued event was bypassed");
        };
        let WatchEvent::Warning(second) = bounded_rx.recv().await.expect("second warning") else {
            panic!("second queued event missing");
        };
        assert_eq!(first.message.as_str(), "first");
        assert_eq!(second.message.as_str(), "second");
        drop(ingress_tx);
        worker.await.expect("forwarder exits");
    }

    /// With no captured runtime handle nothing can wait for queue space, so
    /// even a lossless event hitting a full queue is discarded - and a
    /// discard the metric does not count is control information lost with no
    /// trace. This test runs entirely off-runtime so `register` never
    /// captures a handle.
    #[test]
    fn no_runtime_discard_of_a_lossless_event_is_counted() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("no-runtime".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx.clone());
        tx.try_send(invalidated()).expect("fill queue");

        sink.push(
            account,
            WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "discarded")),
        );

        assert_eq!(sink.dropped(), 1, "the discarded warning must be counted");
        assert!(matches!(rx.try_recv(), Ok(WatchEvent::Invalidated { .. })));
        assert!(
            rx.try_recv().is_err(),
            "nothing could redeliver the warning"
        );
    }

    #[test]
    fn full_reconcile_cannot_represent_control_events() {
        let warning = WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "diagnostic"));
        let terminated = WatchEvent::Terminated(crate::recovery::restart_scope_error(
            CursorScope::Account,
            AccountOperation::SyncChanges,
        ));
        for control in [warning, terminated] {
            assert!(requires_lossless_delivery(&control));
            // The only state a full-reconcile bit can regenerate has lost
            // both the diagnostic and the recovery classification.
            assert!(matches!(
                coalesced_event(control),
                WatchEvent::Invalidated { .. }
            ));
        }
    }

    #[test]
    fn only_control_information_requires_lossless_delivery() {
        let warning = WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "warning"));
        let terminated = WatchEvent::Terminated(crate::recovery::restart_scope_error(
            CursorScope::Account,
            AccountOperation::SyncChanges,
        ));
        assert!(requires_lossless_delivery(&warning));
        assert!(requires_lossless_delivery(&terminated));
        assert!(!requires_lossless_delivery(&invalidated()));
        assert!(!requires_lossless_delivery(&WatchEvent::Disconnected));
        assert!(!requires_lossless_delivery(&WatchEvent::Reconnected));
    }
}
