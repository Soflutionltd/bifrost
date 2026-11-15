//! Admission for engine-initiated cleanup and operator-requested repair.

use super::*;

pub(super) struct MaintenanceGate<'a> {
    pub control: &'a SyncControl,
    pub scheduler: &'a Scheduler,
    pub current: &'a ArcSwap<crate::types::Incarnation>,
    pub throttles: &'a crate::recovery::ThrottleBucket,
    pub account_id: &'a AccountId,
    pub shutdown: &'a CancellationToken,
    pub boundary_tx: &'a watch::Sender<BoundaryRequest>,
    /// Ruling (wave 7): what a non-`Run` boundary means for this caller.
    /// `true` for caller-driven repair: an enterprise user who asked for a
    /// repair expects it to run once a checkpoint barrier or pause lifts,
    /// not to fail with a transient `Paused`. It waits exactly as the bulk
    /// mutation campaign does (`SyncControl::wait_until_running`): bounded
    /// by shutdown and by `Stop`, which refuse. `false` for the orphan
    /// teardown timer, which refuses and retries on its next tick.
    pub wait_for_run: bool,
}

impl MaintenanceGate<'_> {
    pub async fn acquire(
        &self,
        scopes: &[CursorScope],
    ) -> Result<
        (
            Arc<crate::types::Incarnation>,
            SyncActivityGuard,
            crate::scheduler::BudgetPermit,
        ),
        Error,
    > {
        let mut boundary = self.boundary_tx.subscribe();
        loop {
            if self.shutdown.is_cancelled() {
                return Err(Error::ShuttingDown);
            }
            let request = *boundary.borrow_and_update();
            match request {
                BoundaryRequest::Run => {}
                BoundaryRequest::Pause | BoundaryRequest::CheckpointNow if self.wait_for_run => {
                    tokio::select! {
                        biased;
                        () = self.shutdown.cancelled() => return Err(Error::ShuttingDown),
                        changed = boundary.changed() => {
                            if changed.is_err() {
                                return Err(Error::ShuttingDown);
                            }
                        }
                    }
                    continue;
                }
                _ => return Err(Error::Paused),
            }
            let admission = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Err(Error::ShuttingDown),
                _ = boundary.changed() => continue,
                result = self.scheduler.admit(
                    self.account_id.clone(), self.control.priority_snapshot(),
                    crate::scheduler::WorkKind::Sync,
                ) => result?,
            };
            // Re-read the incarnation and holds after admission: either may have
            // changed while queued. Never hold a scheduler permit while sleeping.
            let account = self.current.load_full();
            let keys = scopes
                .iter()
                .flat_map(|scope| {
                    crate::recovery::declared_keys(
                        account.as_ref().as_ref(),
                        self.account_id,
                        bifrost_types::ThrottleWork::Scope(scope),
                    )
                })
                .collect::<Vec<_>>();
            if let Some(wait) =
                crate::recovery::throttle_wait(self.throttles, self.account_id, &keys)
            {
                drop(admission);
                tokio::select! {
                    biased;
                    () = self.shutdown.cancelled() => return Err(Error::ShuttingDown),
                    _ = boundary.changed() => {},
                    () = tokio::time::sleep(wait) => {},
                }
                continue;
            }
            // An enterprise user's pause includes maintenance. Register only
            // after waits, then retain activity through the provider future so
            // pause/checkpoint quiescence cannot race an untracked wire call.
            match self.control.begin_activity() {
                Some(activity) => return Ok((account, activity, admission)),
                // A pause landed between the boundary check and registration.
                // A waiting caller releases its admission and waits it out.
                None if self.wait_for_run => {
                    drop(admission);
                    // Registration checks the control's own boundary, which
                    // can differ from this gate's boundary (including in tests).
                    // Wait on that authority without retaining scarce capacity.
                    tokio::select! {
                        biased;
                        () = self.shutdown.cancelled() => return Err(Error::ShuttingDown),
                        changed = boundary.changed() => {
                            if changed.is_err() { return Err(Error::ShuttingDown); }
                        }
                        running = self.control.wait_until_running(self.shutdown) => {
                            if !running { return Err(Error::Paused); }
                        }
                    }
                    continue;
                }
                None => return Err(Error::Paused),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::Control;

    #[tokio::test]
    async fn maintenance_is_counted_until_pause_reaches_quiescence() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("maintenance".into());
        let control = engine
            .attach(
                id.clone(),
                Arc::new(crate::test_common::StubFactory::queue(vec![Arc::new(
                    crate::test_common::StubAccount::new(Vec::new()),
                )])),
            )
            .await
            .expect("attach");
        let slot = Arc::clone(engine.accounts.get(&id).expect("slot").value());
        let gate = MaintenanceGate {
            control: &slot.control,
            scheduler: &slot.scheduler,
            current: &slot.current,
            throttles: &slot.throttles,
            account_id: &id,
            shutdown: &slot.shutdown,
            boundary_tx: &slot.boundary_tx,
            wait_for_run: false,
        };
        let held = gate.acquire(&[]).await.expect("running maintenance");
        let mut pause = control.pause();
        assert!(futures::poll!(&mut pause).is_pending());
        drop(held);
        pause.await.expect("quiescent pause");
        assert!(matches!(gate.acquire(&[]).await, Err(Error::Paused)));
        engine.detach(&id).await.expect("detach");
    }

    #[tokio::test]
    async fn checkpoint_boundary_refuses_new_maintenance() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("maintenance-checkpoint".into());
        engine
            .attach(
                id.clone(),
                Arc::new(crate::test_common::StubFactory::queue(vec![Arc::new(
                    crate::test_common::StubAccount::new(Vec::new()),
                )])),
            )
            .await
            .expect("attach");
        let slot = Arc::clone(engine.accounts.get(&id).expect("slot").value());
        slot.boundary_tx
            .send_replace(BoundaryRequest::CheckpointNow);
        let gate = MaintenanceGate {
            control: &slot.control,
            scheduler: &slot.scheduler,
            current: &slot.current,
            throttles: &slot.throttles,
            account_id: &id,
            shutdown: &slot.shutdown,
            boundary_tx: &slot.boundary_tx,
            wait_for_run: false,
        };
        assert!(matches!(gate.acquire(&[]).await, Err(Error::Paused)));
        engine.detach(&id).await.expect("detach");
    }

    #[tokio::test]
    async fn caller_driven_repair_waits_out_a_checkpoint_barrier() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("maintenance-wait".into());
        engine
            .attach(
                id.clone(),
                Arc::new(crate::test_common::StubFactory::queue(vec![Arc::new(
                    crate::test_common::StubAccount::new(Vec::new()),
                )])),
            )
            .await
            .expect("attach");
        let slot = Arc::clone(engine.accounts.get(&id).expect("slot").value());
        // A private boundary channel keeps the account's own control at Run,
        // so only the gate sees the barrier.
        let (boundary_tx, _boundary_rx) = watch::channel(BoundaryRequest::CheckpointNow);
        let gate = MaintenanceGate {
            control: &slot.control,
            scheduler: &slot.scheduler,
            current: &slot.current,
            throttles: &slot.throttles,
            account_id: &id,
            shutdown: &slot.shutdown,
            boundary_tx: &boundary_tx,
            wait_for_run: true,
        };
        let mut waiting = Box::pin(gate.acquire(&[]));
        assert!(
            futures::poll!(&mut waiting).is_pending(),
            "a barrier is waited out, not refused"
        );
        boundary_tx.send_replace(BoundaryRequest::Run);
        let held = waiting.await.expect("admitted once the barrier lifts");
        drop(held);
        boundary_tx.send_replace(BoundaryRequest::Stop);
        assert!(
            matches!(gate.acquire(&[]).await, Err(Error::Paused)),
            "Stop still refuses"
        );
        engine.detach(&id).await.expect("detach");
    }

    /// A boundary change drops the gate's queued `admit` future. The lane slot
    /// it occupied must be free immediately: before the scheduler pruned on
    /// `admit`, the dead waiter kept the only slot, so the next live request
    /// was refused as "lane full" while the dispatcher slept on the budget.
    #[tokio::test]
    async fn a_boundary_change_frees_the_queued_admission_slot() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("maintenance-admission".into());
        engine
            .attach(
                id.clone(),
                Arc::new(crate::test_common::StubFactory::queue(vec![Arc::new(
                    crate::test_common::StubAccount::new(Vec::new()),
                )])),
            )
            .await
            .expect("attach");
        let slot = Arc::clone(engine.accounts.get(&id).expect("slot").value());
        let budget = crate::scheduler::BudgetGate::new(crate::scheduler::ConcurrencyBudget {
            // Two per-account permits split one mutation, one sync; a single
            // permit would leave the sync pool empty and hang `acquire`.
            // The global cap of one is what saturates the budget.
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 2,
        });
        let scheduler = crate::scheduler::Scheduler::with_lane_capacity(
            crate::types::SchedulerConfig {
                starvation_floor: 1,
            },
            budget.clone(),
            1,
        );
        // Saturate the budget so the gate's request waits in its lane.
        let held = budget
            .acquire(&id, crate::scheduler::WorkKind::Sync)
            .await
            .expect("hold the only permit");
        let (boundary_tx, _boundary_rx) = watch::channel(BoundaryRequest::Run);
        let gate = MaintenanceGate {
            control: &slot.control,
            scheduler: &scheduler,
            current: &slot.current,
            throttles: &slot.throttles,
            account_id: &id,
            shutdown: &slot.shutdown,
            boundary_tx: &boundary_tx,
            wait_for_run: false,
        };
        let mut queued = Box::pin(gate.acquire(&[]));
        assert!(futures::poll!(&mut queued).is_pending());
        let depth: usize = scheduler
            .admission_snapshot()
            .iter()
            .map(|lane| lane.depth)
            .sum();
        assert_eq!(depth, 1, "the gate's request occupies the lane's only slot");
        boundary_tx.send_replace(BoundaryRequest::Pause);
        assert!(matches!(queued.await, Err(Error::Paused)));
        // The observable: a live request in the same lane is accepted, and it
        // (not the dropped waiter) receives the permit once the budget frees.
        let live = scheduler.admit(
            id.clone(),
            slot.control.priority_snapshot(),
            crate::scheduler::WorkKind::Sync,
        );
        tokio::pin!(live);
        assert!(
            futures::poll!(&mut live).is_pending(),
            "the slot was free, not refused"
        );
        drop(held);
        live.await
            .expect("the live waiter is granted the freed permit");
        engine.detach(&id).await.expect("detach");
    }
}
