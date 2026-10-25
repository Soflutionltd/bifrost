//! Push: the bounded invalidation ingress, the `InvalidationSink`
//! implementation, the reconciler and the engine-owned push lanes.
//!
//! Two push paths converge on one per-account [`PushIngress`]:
//! - In-process: the lane supervisor (`lanes`) drives every
//!   `Account::push_stream` session and hands its `Invalidated` items to
//!   the ingress.
//! - Out-of-process: the consumer holds an `Arc<dyn InvalidationSink>`
//!   (from `SyncEngine::invalidation_sink`) and calls `push` from its
//!   Pub/Sub or webhook receiver; invalidations enter the ingress and
//!   control events are acted on synchronously (`route_watch_event`).
//!
//! The reconciler drains the ingress into its work set and drives the
//! affected scopes. Output flows to the same broadcast as the
//! multiplexer's, so consumers see one unified `Change` stream.
pub mod health;
pub mod ingress;
pub(crate) mod lanes;
pub mod reconciler;
pub mod subscription;

use crate::cancel::{BoundaryRequest, BoundaryView};
use crate::multiplexer::{ChangeDelivery, MultiplexerEvent, ReopenRequest};
use crate::recovery::{RecoveryPlan, ThrottleBucket, directive_target_scope, plan_recovery};
use bifrost_types::{
    AccountId, CursorScope, InvalidationSink, SinkPush, SyncEvent, Warning, WarningKind, WatchEvent,
};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

pub use ingress::{PendingPush, PushIngress};
pub use reconciler::{Reconciler, scopes_for_hint};
pub(crate) use subscription::RegisteredSubscription;
pub use subscription::SubscriptionRegistry;

/// What the sink needs of one attached account to act on a consumer push.
#[derive(Debug, Clone)]
pub struct SinkTarget {
    pub ingress: Arc<PushIngress>,
    pub delivery: Arc<ChangeDelivery>,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    pub throttles: Arc<std::sync::Mutex<ThrottleBucket>>,
    /// The account's boundary, read synchronously (`peek`) before an engine
    /// directive is queued.
    pub boundary: BoundaryView,
}

/// Engine-side `InvalidationSink` implementation.
///
/// `targets` is the registry of attached accounts: `attach` inserts,
/// `detach` removes. The coarsening counter is exposed as
/// `bifrost_sync_push_dropped_total`.
#[derive(Debug)]
pub struct InvalidationSinkInner {
    targets: DashMap<AccountId, SinkTarget>,
    coarsened: Arc<AtomicU64>,
}

impl InvalidationSinkInner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            targets: DashMap::new(),
            coarsened: Arc::new(AtomicU64::new(0)),
        }
    }
    /// Route consumer pushes for `account` to `target`, replacing any
    /// earlier registration.
    pub fn register(&self, account: AccountId, target: SinkTarget) {
        self.targets.insert(account, target);
    }
    /// Stop routing pushes for `account` and close its ingress, so the
    /// reconciler finishes its sweep and returns.
    pub fn unregister(&self, account: &AccountId) {
        if let Some((_, target)) = self.targets.remove(account) {
            target.ingress.close();
        }
    }
    /// Number of distinct-hint coarsenings into a full reconcile. No
    /// reconciliation obligation is lost by a coarsening.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.coarsened.load(Ordering::Relaxed)
    }
    /// An ingress counting its coarsenings on this sink's counter.
    #[must_use]
    pub fn new_ingress(&self, capacity: usize) -> Arc<PushIngress> {
        Arc::new(PushIngress::with_counter(
            capacity,
            Arc::clone(&self.coarsened),
        ))
    }
}
impl Default for InvalidationSinkInner {
    fn default() -> Self {
        Self::new()
    }
}
impl InvalidationSink for InvalidationSinkInner {
    fn push(&self, account: AccountId, event: WatchEvent) -> SinkPush {
        let Some(target) = self.targets.get(&account) else {
            return SinkPush::NotAttached;
        };
        route_watch_event(target.value(), &account, event)
    }
}

pub(crate) fn broadcast(target: &SinkTarget, event: SyncEvent<bifrost_types::Change>) {
    let _ = target.delivery.sender().send(MultiplexerEvent {
        scope: CursorScope::Account,
        event: Arc::new(event),
        checkpoint: None,
        publication: None,
    });
}

/// The single synchronous handler for a consumer-injected event. Nothing
/// control-shaped is stored: invalidations enter the bounded ingress,
/// warnings and terminal failures are broadcast at once, throttles are
/// recorded at once, and an engine directive is either queued on the reopen
/// channel or handed back as `Refused` (boundary not `Run`, or queue full).
/// Control events therefore overtake invalidations still pending in the
/// ingress, which nothing downstream depends on.
pub(crate) fn route_watch_event(
    target: &SinkTarget,
    account: &AccountId,
    event: WatchEvent,
) -> SinkPush {
    match event {
        WatchEvent::Invalidated { hint } => target.ingress.invalidate(&hint.payload),
        WatchEvent::Reconnected => target.ingress.request_full(),
        WatchEvent::Disconnected => broadcast(
            target,
            SyncEvent::Warning(Warning::user_safe(
                WarningKind::Other,
                "push transport disconnected",
            )),
        ),
        WatchEvent::Warning(warning) => broadcast(target, SyncEvent::Warning(warning)),
        WatchEvent::Terminated(error) => match plan_recovery(error.clone()) {
            RecoveryPlan::Engine(directive) => {
                if target.boundary.peek() != BoundaryRequest::Run {
                    return SinkPush::Refused(WatchEvent::Terminated(error));
                }
                return match target.reopen_tx.try_send(ReopenRequest::Recovery {
                    scope: directive_target_scope(&directive),
                    error,
                }) {
                    Ok(()) => SinkPush::Accepted,
                    Err(mpsc::error::TrySendError::Full(ReopenRequest::Recovery {
                        error, ..
                    })) => SinkPush::Refused(WatchEvent::Terminated(error)),
                    Err(_) => SinkPush::NotAttached,
                };
            }
            RecoveryPlan::Retry(advice) => {
                crate::recovery::record_throttle(&target.throttles, account, &advice, &error);
                broadcast(
                    target,
                    SyncEvent::Warning(Warning::user_safe(
                        WarningKind::Other,
                        "push stream terminated with retryable recovery",
                    )),
                );
            }
            RecoveryPlan::Reconcile(advice) => {
                crate::recovery::record_reconcile_throttle(
                    &target.throttles,
                    account,
                    &advice,
                    &error,
                );
                broadcast(
                    target,
                    SyncEvent::Warning(Warning::user_safe(
                        WarningKind::Other,
                        "push stream terminated with retryable recovery",
                    )),
                );
            }
            RecoveryPlan::Terminal(fatal) => {
                broadcast(target, SyncEvent::Terminated(fatal.into_inner()));
            }
        },
        _ => target.ingress.request_full(),
    }
    SinkPush::Accepted
}

#[must_use]
pub fn shared_sink() -> Arc<InvalidationSinkInner> {
    Arc::new(InvalidationSinkInner::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cancel::Boundary;
    use bifrost_types::{
        AccountError, AccountErrorBuilder, AccountErrorKind, Cause, StateCause, SyncStateErrorKind,
    };
    fn setup() -> (
        InvalidationSinkInner,
        AccountId,
        SinkTarget,
        Boundary,
        mpsc::Receiver<ReopenRequest>,
        tokio::sync::broadcast::Receiver<MultiplexerEvent>,
    ) {
        let sink = InvalidationSinkInner::new();
        let account = AccountId("sink".into());
        let (boundary, view) = Boundary::new();
        let (reopen_tx, reopen_rx) = mpsc::channel(1);
        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let target = SinkTarget {
            ingress: sink.new_ingress(1),
            delivery: Arc::new(ChangeDelivery::new(tx)),
            reopen_tx,
            throttles: Arc::new(std::sync::Mutex::new(ThrottleBucket::new())),
            boundary: view,
        };
        sink.register(account.clone(), target.clone());
        (sink, account, target, boundary, reopen_rx, rx)
    }
    fn restart() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
        )
        .try_build()
        .expect("restart")
    }
    #[test]
    fn sink_reconnected_requests_a_full_reconcile() {
        let (sink, account, target, _, _, _) = setup();
        assert!(matches!(
            sink.push(account, WatchEvent::Reconnected),
            SinkPush::Accepted
        ));
        assert!(target.ingress.take().full);
    }
    #[test]
    fn sink_push_for_an_unknown_account_is_not_attached() {
        assert!(matches!(
            InvalidationSinkInner::new().push(AccountId("unknown".into()), WatchEvent::Reconnected),
            SinkPush::NotAttached
        ));
    }
    #[test]
    fn sink_engine_directive_is_refused_with_its_event_when_the_reopen_queue_is_full() {
        let (sink, account, _, _boundary, mut reopen, _) = setup();
        assert!(matches!(
            sink.push(account.clone(), WatchEvent::Terminated(restart())),
            SinkPush::Accepted
        ));
        let error = restart();
        let SinkPush::Refused(WatchEvent::Terminated(returned)) =
            sink.push(account, WatchEvent::Terminated(error.clone()))
        else {
            panic!("must return original event");
        };
        assert_eq!(returned.message_key(), error.message_key());
        assert!(reopen.try_recv().is_ok());
        assert!(reopen.try_recv().is_err());
    }
    #[test]
    fn sink_engine_directive_is_refused_while_the_boundary_is_not_run() {
        let (sink, account, _, boundary, mut reopen, _) = setup();
        for request in [BoundaryRequest::Pause, BoundaryRequest::CheckpointNow] {
            boundary.set(request);
            assert!(matches!(
                sink.push(account.clone(), WatchEvent::Terminated(restart())),
                SinkPush::Refused(WatchEvent::Terminated(_))
            ));
            assert!(reopen.try_recv().is_err());
        }
        boundary.set(BoundaryRequest::Run);
        assert!(matches!(
            sink.push(account, WatchEvent::Terminated(restart())),
            SinkPush::Accepted
        ));
        assert!(reopen.try_recv().is_ok());
    }
    /// A terminal failure is acted on inside `push` itself: the broadcast
    /// carries `SyncEvent::Terminated` with the pushed error before `push`
    /// returns, and nothing is stored for later - no ingress obligation, no
    /// reopen request.
    #[test]
    fn sink_terminal_is_broadcast_as_terminated_at_push_time() {
        let (sink, account, target, _, mut reopen, mut rx) = setup();
        let error = crate::test_common::unsupported(bifrost_types::AccountOperation::PushStream);
        assert!(error.recovery().is_terminal());
        assert!(matches!(
            sink.push(account, WatchEvent::Terminated(error.clone())),
            SinkPush::Accepted
        ));
        let event = rx.try_recv().expect("broadcast before push returned");
        let SyncEvent::Terminated(broadcast) = event.event.as_ref() else {
            panic!("expected Terminated, got {:?}", event.event);
        };
        assert_eq!(broadcast.message_key(), error.message_key());
        assert!(broadcast.recovery().is_terminal());
        assert!(rx.try_recv().is_err(), "exactly one broadcast");
        let pending = target.ingress.take();
        assert!(!pending.full && pending.scopes.is_empty() && pending.memberships.is_empty());
        assert!(reopen.try_recv().is_err());
    }
    #[test]
    fn sink_terminal_is_broadcast_account_scoped() {
        let (sink, account, _, _, _, mut rx) = setup();
        let error = crate::test_common::unsupported(bifrost_types::AccountOperation::PushStream)
            .into_builder()
            .scope(bifrost_types::ErrorScope::Cursor(CursorScope::Folder(
                bifrost_types::FolderId("folder".into()),
            )))
            .try_build()
            .expect("scoped error");
        assert!(matches!(
            sink.push(account, WatchEvent::Terminated(error)),
            SinkPush::Accepted
        ));
        let event = rx.try_recv().expect("immediate broadcast");
        assert_eq!(event.scope, CursorScope::Account);
        assert!(matches!(event.event.as_ref(), SyncEvent::Terminated(_)));
    }
    #[test]
    fn sink_warning_and_disconnect_are_broadcast_without_queueing() {
        let (sink, account, _, _, _, mut rx) = setup();
        assert!(matches!(
            sink.push(
                account.clone(),
                WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "advice"))
            ),
            SinkPush::Accepted
        ));
        assert!(matches!(
            sink.push(account, WatchEvent::Disconnected),
            SinkPush::Accepted
        ));
        for _ in 0..2 {
            assert!(matches!(
                rx.try_recv().expect("immediate broadcast").event.as_ref(),
                SyncEvent::Warning(_)
            ));
        }
    }
    #[test]
    fn sink_never_blocks_on_a_full_queue_from_a_non_runtime_thread() {
        let (sink, account, _, _boundary, _reopen, _) = setup();
        std::thread::spawn(move || {
            assert!(matches!(
                sink.push(account.clone(), WatchEvent::Terminated(restart())),
                SinkPush::Accepted
            ));
            assert!(matches!(
                sink.push(account, WatchEvent::Terminated(restart())),
                SinkPush::Refused(_)
            ));
        })
        .join()
        .expect("non-runtime receiver returns");
    }
    #[tokio::test(start_paused = true)]
    async fn sink_retry_termination_records_its_throttle_at_push_time() {
        use bifrost_types::{RetryHint, ServerCause, ServerErrorKind, ThrottleScope};
        let (sink, account, target, _, _, mut rx) = setup();
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited {
                retry_hint: Some(RetryHint::After(std::time::Duration::from_secs(20))),
            }),
        )
        .throttle_scope(ThrottleScope::Account)
        .try_build()
        .expect("throttle");
        assert!(matches!(
            sink.push(account.clone(), WatchEvent::Terminated(error)),
            SinkPush::Accepted
        ));
        assert_eq!(
            crate::recovery::account_throttle_wait(&target.throttles, &account),
            Some(std::time::Duration::from_secs(20))
        );
        assert!(matches!(
            rx.try_recv().expect("warning").event.as_ref(),
            SyncEvent::Warning(_)
        ));
    }
}

#[cfg(test)]
mod lane_tests;
