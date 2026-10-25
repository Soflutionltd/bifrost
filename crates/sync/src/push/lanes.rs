//! Engine-owned session supervision, recovery pacing and generation draining.
use super::{
    PushIngress, SinkTarget, broadcast,
    health::{PushHealth, PushLaneHealth, PushLaneState},
};
use crate::{
    backoff::{ReconnectBackoff, jitter_seed},
    cancel::{BoundaryRequest, BoundaryView},
    multiplexer::{ChangeDelivery, ReopenRequest},
    recovery::{self, RecoveryPlan, ThrottleBucket},
};
use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountError, AccountId, EngineDirective, PushCapability, PushGate, PushLaneKind,
    PushLaneSpec, PushSessionEnd, PushSessionEvent, SessionStop, SyncEvent, Warning, WarningKind,
};
use futures::{FutureExt, StreamExt};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub(crate) struct PushLanes {
    pub account_id: AccountId,
    pub current: Arc<ArcSwap<Arc<dyn Account>>>,
    pub generation: watch::Receiver<u64>,
    pub topology: watch::Receiver<u64>,
    pub ingress: Arc<PushIngress>,
    pub delivery: Arc<ChangeDelivery>,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    pub throttles: Arc<std::sync::Mutex<ThrottleBucket>>,
    pub boundary: BoundaryView,
    pub shutdown: CancellationToken,
    pub health: watch::Sender<PushHealth>,
    /// Zero before retirement; generation + 1 after that generation drains.
    /// Reattach waits on it before closing the account it replaced.
    pub retired: watch::Sender<u64>,
    /// `EngineConfig::detach_timeout / 2`, so a drain that uses its whole
    /// bound still finishes inside detach's worker deadline.
    pub drain_bound: Duration,
}
impl PushLanes {
    pub async fn run(mut self) {
        let mut carried = HashMap::new();
        loop {
            let generation = *self.generation.borrow_and_update();
            let account = self.current.load_full();
            let specs = if account.capabilities().push == PushCapability::None {
                Vec::new()
            } else {
                account.push_lanes()
            };
            self.health.send_replace(PushHealth {
                generation,
                lanes: specs
                    .iter()
                    .map(|s| PushLaneHealth {
                        lane: s.lane,
                        kind: s.kind,
                        state: PushLaneState::Starting,
                    })
                    .collect(),
            });
            let stop = self.shutdown.child_token();
            let results = {
                let joined =
                    futures::future::join_all(specs.iter().enumerate().map(|(index, spec)| {
                        self.run_lane(
                            account.as_ref().as_ref(),
                            *spec,
                            index,
                            generation,
                            stop.clone(),
                            carried.get(&spec.lane).copied().unwrap_or(0),
                        )
                    }));
                tokio::pin!(joined);
                // Use a separate receiver so lane futures may borrow the supervisor.
                let mut generation_rx = self.generation.clone();
                if *generation_rx.borrow_and_update() != generation {
                    stop.cancel();
                }
                tokio::select! {
                    result = &mut joined => result,
                    _ = generation_rx.changed() => { stop.cancel(); joined.await },
                    () = self.shutdown.cancelled() => { stop.cancel(); joined.await },
                }
            };
            carried = specs
                .iter()
                .zip(results)
                .map(|(s, n)| (s.lane, n))
                .collect();
            self.retired.send_replace(generation.saturating_add(1));
            if self.shutdown.is_cancelled() {
                return;
            }
            if *self.generation.borrow() == generation {
                tokio::select! {
                    () = self.shutdown.cancelled() => return,
                    result = self.generation.changed() => if result.is_err() { return; },
                }
            }
        }
    }
    fn target(&self) -> SinkTarget {
        SinkTarget {
            ingress: Arc::clone(&self.ingress),
            delivery: Arc::clone(&self.delivery),
            reopen_tx: self.reopen_tx.clone(),
            throttles: Arc::clone(&self.throttles),
            boundary: self.boundary.clone(),
        }
    }
    /// Write this lane's entry, notifying receivers only on a change of
    /// state: `Healthy` arrives once per IDLE round or WebSocket frame, and a
    /// late subscriber's watch must not wake for every one of them.
    fn state(&self, index: usize, state: PushLaneState) {
        self.health.send_if_modified(|h| {
            let entry = &mut h.lanes[index].state;
            let unchanged = matches!(
                (&*entry, &state),
                (PushLaneState::Starting, PushLaneState::Starting)
                    | (PushLaneState::Up, PushLaneState::Up)
                    | (PushLaneState::Draining, PushLaneState::Draining)
            );
            if !unchanged {
                *entry = state;
            }
            !unchanged
        });
    }
    fn down(
        &self,
        index: usize,
        backoff: &mut ReconnectBackoff,
        error: &Option<AccountError>,
    ) -> Duration {
        let delay = backoff.record_failure(jitter_seed());
        let mut edge = false;
        self.health.send_modify(|h| {
            edge = !h.any_down();
            h.lanes[index].state = PushLaneState::Down {
                consecutive_failures: backoff.failures(),
                last_error: error.clone(),
            };
        });
        if edge {
            broadcast(
                &self.target(),
                SyncEvent::Warning(Warning::user_safe(
                    WarningKind::Other,
                    "push transport disconnected",
                )),
            );
        }
        delay
    }
    fn failure(
        &self,
        error: AccountError,
        generation: u64,
        pending: &mut VecDeque<ReopenRequest>,
    ) -> (Duration, bool, bool) {
        match recovery::plan_recovery(error.clone()) {
            RecoveryPlan::Retry(a) => {
                recovery::record_throttle(&self.throttles, &self.account_id, &a, &error);
                (
                    recovery::retry_delay(&a, SystemTime::now(), Duration::ZERO),
                    false,
                    false,
                )
            }
            RecoveryPlan::Reconcile(a) => {
                recovery::record_reconcile_throttle(&self.throttles, &self.account_id, &a, &error);
                (
                    recovery::reconcile_delay(&a, SystemTime::now(), Duration::ZERO),
                    false,
                    false,
                )
            }
            RecoveryPlan::Engine(d) => {
                let restart = matches!(d, EngineDirective::RestartAccount);
                pending.push_back(ReopenRequest::PushRecovery {
                    scope: recovery::directive_target_scope(&d),
                    error,
                    generation,
                });
                (Duration::ZERO, false, restart)
            }
            RecoveryPlan::Terminal(f) => {
                broadcast(&self.target(), SyncEvent::Terminated(f.into_inner()));
                (Duration::ZERO, true, false)
            }
        }
    }
    async fn wait(
        &self,
        duration: Duration,
        pending: &mut VecDeque<ReopenRequest>,
        stop: &CancellationToken,
        boundary: &mut BoundaryView,
    ) -> bool {
        let deadline = tokio::time::sleep(duration);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => return false,
                permit = self.reopen_tx.reserve(), if !pending.is_empty() && boundary.peek() == BoundaryRequest::Run => {
                    let Ok(permit) = permit else { return false; };
                    permit.send(pending.pop_front().expect("pending directive"));
                }
                _ = boundary.changed(), if !pending.is_empty() => {},
                () = &mut deadline, if pending.is_empty() => return true,
            }
        }
    }
    /// Run one lane until its generation stops; returns the lane's final
    /// consecutive-failure count, which seeds the same lane id on the next
    /// generation. A lane that stops outside a live session (in a backoff,
    /// park or pending flush) reads `Draining` too, like one that drained a
    /// session.
    async fn run_lane(
        &self,
        account: &dyn Account,
        spec: PushLaneSpec,
        index: usize,
        generation: u64,
        stop: CancellationToken,
        carried: u32,
    ) -> u32 {
        let failures = self
            .drive_lane(account, spec, index, generation, stop.clone(), carried)
            .await;
        if stop.is_cancelled() {
            self.state(index, PushLaneState::Draining);
        }
        failures
    }

    async fn drive_lane(
        &self,
        account: &dyn Account,
        spec: PushLaneSpec,
        index: usize,
        generation: u64,
        stop: CancellationToken,
        carried: u32,
    ) -> u32 {
        let mut backoff = ReconnectBackoff::with_failures(spec.backoff, carried);
        let mut pending = VecDeque::new();
        let mut boundary = self.boundary.clone();
        let mut topology = self.topology.clone();
        let connection = spec.kind == PushLaneKind::Connection;
        let mut owe = connection && generation > 0;
        let mut last_error = None;
        let mut blind = 0_u32;
        if !connection && generation > 0 {
            self.ingress.request_full();
        }
        if !self
            .wait(backoff.carried_delay(), &mut pending, &stop, &mut boundary)
            .await
        {
            return backoff.failures();
        }
        loop {
            if !self
                .wait(Duration::ZERO, &mut pending, &stop, &mut boundary)
                .await
            {
                return backoff.failures();
            }
            while let Some(wait) =
                recovery::account_throttle_wait(&self.throttles, &self.account_id)
            {
                if !self.wait(wait, &mut pending, &stop, &mut boundary).await {
                    return backoff.failures();
                }
            }
            if stop.is_cancelled() {
                return backoff.failures();
            }
            let topo_seen = *topology.borrow_and_update();
            let (stopper, session_stop) = SessionStop::pair();
            let throttles = Arc::clone(&self.throttles);
            let aid = self.account_id.clone();
            let gate = PushGate::new(move || {
                let throttles = Arc::clone(&throttles);
                let aid = aid.clone();
                async move {
                    while let Some(wait) = recovery::account_throttle_wait(&throttles, &aid) {
                        tokio::time::sleep(wait).await;
                    }
                }
                .boxed()
            });
            let mut session = account.push_stream(spec.lane, session_stop, gate);
            let mut established = false;
            let mut max_hint = Duration::ZERO;
            let outcome = loop {
                tokio::select! {
                    biased;
                    () = stop.cancelled() => {
                        self.state(index, PushLaneState::Draining);
                        stopper.stop();
                        let drain = async {
                            while let Some(event) = session.next().await {
                                match event {
                                    PushSessionEvent::Invalidated(h) => self.ingress.invalidate(&h.payload),
                                    PushSessionEvent::Failed(_) | PushSessionEvent::Ended(_) => break,
                                    _ => {},
                                }
                            }
                        };
                        let _ = tokio::time::timeout(self.drain_bound, drain).await;
                        return backoff.failures();
                    }
                    permit = self.reopen_tx.reserve(), if !pending.is_empty() && boundary.peek() == BoundaryRequest::Run => {
                        let Ok(permit) = permit else { return backoff.failures(); };
                        permit.send(pending.pop_front().expect("pending directive"));
                    }
                    _ = boundary.changed(), if !pending.is_empty() => {},
                    next = session.next() => match next {
                        Some(PushSessionEvent::Invalidated(h)) => self.ingress.invalidate(&h.payload),
                        Some(PushSessionEvent::Established) => {
                            established = true; blind = 0;
                            self.health.send_if_modified(|h| {
                                let entry = &mut h.lanes[index].state;
                                let lift = match entry {
                                    PushLaneState::Up => false,
                                    PushLaneState::Starting => true,
                                    _ => connection,
                                };
                                if lift { *entry = PushLaneState::Up; }
                                lift
                            });
                            if owe { self.ingress.request_full(); owe = false; }
                        }
                        Some(PushSessionEvent::Healthy) => { backoff.reset(); last_error = None; self.state(index, PushLaneState::Up); }
                        Some(PushSessionEvent::Warning(w)) => broadcast(&self.target(), SyncEvent::Warning(w)),
                        Some(PushSessionEvent::UnitFailed(e)) => {
                            last_error = Some(e.clone());
                            max_hint = max_hint.max(self.failure(e, generation, &mut pending).0);
                        }
                        Some(e @ (PushSessionEvent::Failed(_) | PushSessionEvent::Ended(_))) => break e,
                        None => {
                            tracing::warn!(lane = spec.lane.0, "push session ended without a terminal item");
                            break PushSessionEvent::Ended(PushSessionEnd::Lost);
                        }
                        _ => {},
                    }
                }
            };
            drop(session);
            let delay = match outcome {
                PushSessionEvent::Ended(PushSessionEnd::Stopped) => {
                    stop.cancelled().await;
                    return backoff.failures();
                }
                PushSessionEvent::Ended(PushSessionEnd::Reconfigure) => {
                    if connection {
                        owe = true;
                        self.state(index, PushLaneState::Starting);
                    }
                    if !established {
                        blind = blind.saturating_add(1);
                    }
                    if blind >= 4 {
                        tracing::warn!(
                            lane = spec.lane.0,
                            "push session repeatedly reconfigured without establishing coverage"
                        );
                        backoff.record_failure(jitter_seed())
                    } else {
                        Duration::ZERO
                    }
                }
                PushSessionEvent::Failed(e) => {
                    last_error = Some(e.clone());
                    let (hint, terminal, restart) =
                        self.failure(e.clone(), generation, &mut pending);
                    if connection {
                        owe = true;
                    }
                    if terminal && connection {
                        self.state(index, PushLaneState::Parked { error: e });
                        if *topology.borrow_and_update() == topo_seen {
                            tokio::select! {
                                () = stop.cancelled() => return backoff.failures(),
                                changed = topology.changed() => if changed.is_err() {
                                    // The slot dropped its topology sender: nothing
                                    // can unpark this lane except its generation.
                                    stop.cancelled().await;
                                    return backoff.failures();
                                },
                            }
                        }
                        continue;
                    }
                    let delay = self.down(index, &mut backoff, &last_error).max(hint);
                    if restart {
                        if !self
                            .wait(Duration::ZERO, &mut pending, &stop, &mut boundary)
                            .await
                        {
                            return backoff.failures();
                        }
                        if !self
                            .wait(Duration::from_secs(30), &mut pending, &stop, &mut boundary)
                            .await
                        {
                            return backoff.failures();
                        }
                    }
                    delay
                }
                _ => {
                    if connection {
                        owe = true;
                    }
                    self.down(index, &mut backoff, &last_error).max(max_hint)
                }
            };
            if !self.wait(delay, &mut pending, &stop, &mut boundary).await {
                return backoff.failures();
            }
        }
    }
}
