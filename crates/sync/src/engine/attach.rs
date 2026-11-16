//! Attach wiring: scope discovery, cursor establishment, and the
//! per-slot worker spawn block.

use super::*;

/// Owns pre-installation cleanup even when the caller drops attach. Keep the
/// lifecycle reservation through bounded writer drain and account close. A retained
/// sender must not prevent the user from attaching this account again forever.
struct AttachingWriter {
    lifecycle: Option<(
        Arc<AsyncMutex<std::collections::HashSet<AccountId>>>,
        AccountId,
    )>,
    account: Option<Arc<dyn Account>>,
    writer: Option<tokio::task::JoinHandle<()>>,
    close_timeout: Duration,
    runtime: tokio::runtime::Handle,
    bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
}

impl AttachingWriter {
    fn launch_cleanup(&mut self) -> tokio::task::JoinHandle<()> {
        let lifecycle = self.lifecycle.take();
        let account = self.account.take();
        let writer = self.writer.take();
        let close_timeout = self.close_timeout;
        let bandwidth_budget = self.bandwidth_budget.take();
        self.runtime.spawn(async move {
            // Workers or consumer-owned checkpoint handles can retain senders
            // after setup fails. As on detach, bound the drain so a retained
            // handle cannot reserve this account forever. A timed-out writer
            // forfeits unpersisted acknowledgements, allowing redelivery.
            if let Some(mut writer) = writer
                && tokio::time::timeout(close_timeout, &mut writer)
                    .await
                    .is_err()
            {
                writer.abort();
                tracing::warn!(
                    target: "bifrost.sync.attach",
                    timeout = ?close_timeout,
                    "ack writer did not drain while unwinding attach; aborted with \
                     potentially unpersisted checkpoints"
                );
            }
            if let Some(account) = account {
                match close_within(account.as_ref(), close_timeout).await {
                    CloseOutcome::Closed => {}
                    CloseOutcome::Refused(error) => tracing::warn!(
                        target: "bifrost.sync.attach",
                        error = ?error,
                        "account close failed while unwinding attach"
                    ),
                    CloseOutcome::TimedOut => tracing::warn!(
                        target: "bifrost.sync.attach",
                        timeout = ?close_timeout,
                        "account close timed out while unwinding attach"
                    ),
                }
            }
            drop(bandwidth_budget);
            if let Some((inflight, account_id)) = lifecycle {
                inflight.lock().await.remove(&account_id);
            }
        })
    }
}

impl Drop for AttachingWriter {
    fn drop(&mut self) {
        if self.lifecycle.is_some() {
            // Dropping the join handle detaches cleanup, rather than cancelling
            // it along with the attach future. Ownership moved before spawning.
            drop(self.launch_cleanup());
        }
    }
}

impl SyncEngine {
    /// Attach an account to the engine.
    ///
    /// Flow per `bifrost-sync.md` -> attach:
    /// 1. Call `factory.open(account_id)` to obtain the first
    ///    `Arc<dyn Account>`.
    /// 2. Read `capabilities()` and stash on the slot.
    /// 3. For each scope from `discover_cursor_scopes()`, call
    ///    `establish_initial_cursor(scope)` and react accordingly.
    /// 4. Spawn multiplexer, backfill orchestrator, push reconciler.
    /// 5. Hand back a `SyncControl`.
    pub async fn attach(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
    ) -> Result<SyncControl, Error> {
        // Take the per-account lifecycle guard to close the duplicate-
        // attach race (existence check + factory.open + spawn workers
        // is a multi-await window between the early bail and the
        // final insert). A detach still tearing this id down also holds
        // the guard, so this reports `AccountAlreadyAttached` - which is
        // literally true, the incarnation is attached and draining -
        // rather than installing a slot the detach would then strip of
        // its sink, budget, and throttle registrations.
        {
            let mut guard = self.lifecycle_inflight.lock().await;
            if guard.contains(&account_id) || self.accounts.contains_key(&account_id) {
                return Err(Error::AccountAlreadyAttached(account_id));
            }
            guard.insert(account_id.clone());
        }
        let mut cleanup = AttachingWriter {
            lifecycle: Some((Arc::clone(&self.lifecycle_inflight), account_id.clone())),
            account: None,
            writer: None,
            close_timeout: self.config.detach_timeout,
            runtime: tokio::runtime::Handle::current(),
            bandwidth_budget: None,
        };
        let result = self.attach_inner(account_id, factory, &mut cleanup).await;
        if result.is_ok() {
            cleanup.account = None;
        }
        // The spawned owner survives cancellation during this final wait too.
        let _ = cleanup.launch_cleanup().await;
        result
    }

    async fn attach_inner(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
        cleanup: &mut AttachingWriter,
    ) -> Result<SyncControl, Error> {
        let bandwidth_budget = self.bandwidth_registry.as_ref().map(|registry| {
            let budget = registry.budget(account_id.clone());
            budget.set_cap(None);
            budget
        });
        // Cleanup retains the budget through writer drain and account close.
        cleanup.bandwidth_budget = bandwidth_budget.clone();
        let OpenedAccount {
            account: opened,
            skipped_scopes,
        } = factory
            .open(account_id.clone())
            .await
            .map_err(Error::OpenFailed)?;
        cleanup.account = Some(Arc::clone(&opened));
        log_open_skips(&account_id, &skipped_scopes, "attach");
        self.attach_opened(
            account_id,
            factory,
            opened,
            skipped_scopes,
            bandwidth_budget,
            cleanup,
        )
        .await
    }

    async fn attach_opened(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
        opened: Arc<dyn Account>,
        skipped_scopes: Vec<SkippedScope>,
        bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
        cleanup: &mut AttachingWriter,
    ) -> Result<SyncControl, Error> {
        // The open-time skip lane, kept live for the slot's lifetime:
        // reopen replaces it with the replacement open's answer, and
        // `open_skipped_scopes` exposes it to the consumer.
        let open_skips = Arc::new(std::sync::Mutex::new(skipped_scopes));
        let capabilities = Arc::new(std::sync::RwLock::new(opened.capabilities().clone()));

        let cursors = Arc::new(CursorRegistry::new());

        // Per-account broadcast for the unified Change stream. Keep a
        // sentinel receiver on the slot so the channel never closes
        // when subscribers come and go.
        let (changes_tx, sentinel_rx) =
            broadcast::channel::<MultiplexerEvent>(self.config.multiplexer.changes_capacity);

        // The delivery gate: the same broadcast sender, plus the numbering of
        // the receivers handed out on it. A backfill page's send and its delivery
        // stamp happen together under its lock, as do a receiver's subscribe and
        // its number, and a departing receiver's unregister and its sweep - which
        // is what makes "can any live receiver still reach this page" an exact
        // question rather than an approximate one. See
        // `delivery::ChangeDelivery`.
        let delivery = Arc::new(crate::delivery::ChangeDelivery::new(changes_tx));

        // Per-account control broadcast. The engine publishes
        // `AccountControl::Pause(reason)` on this channel when it
        // auto-pauses the account (operator override, retry budget
        // exhausted). Consumers subscribe via
        // `SyncEngine::account_control_stream`.
        let (account_control_tx, account_control_sentinel) =
            broadcast::channel::<AccountControl>(16);

        // Notify fired when a real subscriber arrives so deferred
        // inventory workers can park without hot-polling.
        let subscriber_notify = Arc::new(Notify::new());

        // Engine-wide throttle bucket (one per SyncEngine, shared via
        // Mutex because recovery paths cross task boundaries). Shared
        // across accounts so a `Budget` deadline recorded by one account
        // pauses every sibling declaring the same budget.
        let throttle_bucket = Arc::clone(&self.throttles);

        // Load before publishing registrations or spawning producers. Failure
        // unwinds the opened account without leaving a running unseeded lane.
        // The same lifecycle guard excludes the preceding attachment's writer.
        let pending_coverage =
            Arc::new(writer::restore_proof_order(&account_id, self.checkpoints.as_ref()).await?);

        // Wire boundary + priority watches.
        let (boundary, boundary_view) = Boundary::new();
        let (priority_tx, priority_rx) = watch::channel(Priority::Normal);
        let (bandwidth_cap_tx, bandwidth_cap_rx) = watch::channel(None);

        // Control handle shared between the SyncControl returned to
        // the consumer and the engine's spawned workers (so workers
        // can call `record_checkpoint`).
        let control = SyncControl::new_with_publications(
            account_id.clone(),
            boundary.clone(),
            priority_tx.clone(),
            bandwidth_cap_tx.clone(),
            Arc::clone(&pending_coverage),
        );

        control.bind_control_events(account_control_tx.clone());

        let (ack_tx, ack_rx) = mpsc::channel::<WriterRequest>(256);
        let writer = WriterHandle::new(ack_tx);
        cleanup.writer = Some(tokio::spawn(ack_writer(
            account_id.clone(),
            Arc::clone(&self.checkpoints),
            control.clone(),
            Arc::clone(&pending_coverage),
            ack_rx,
        )));

        // Drive cursor establishment per scope. We do this before
        // spawning stream workers because multiplexer + backfill
        // need the registry pre-populated for `Ready` scopes. For
        // `EstablishViaInventory` scopes the inventory walk IS the
        // cursor establishment AND surfaces inventory items - we
        // defer those walks until the slot can be subscribed to, so
        // consumers observe cold-start data the same way they observe
        // live changes.
        let scopes = self.discover_scopes(opened.as_ref()).await?;
        // Discovery is the account's cursor model, including scopes deferred
        // to inventory that hold no cursor yet; lifecycle mapping reads it.
        cursors.note_discovered(&scopes);
        // Read every discovered scope's stored cursor BEFORE establishing any.
        //
        // An unreadable row (an envelope outside the migration window, or one
        // failing validation) has unknowable provenance, and a release that
        // moves the envelope makes EVERY row unreadable at once. Healing each
        // row per scope, as attach once did, deleted the only evidence before
        // any protocol door could fail, so the account-wide schema reset never
        // ran and every completion marker survived to suppress the inventory
        // walk that re-mints ids under the new encoding. One unreadable row
        // therefore clears every change cursor and backfill row of the account,
        // undiscovered scopes included; the debt ledger survives. Corruption and
        // downgrade take the same path, conservatively: those signals cannot be
        // told apart from an encoding change. A failed clear fails the attach -
        // establishing fresh over surviving markers would recreate the defect.
        let mut stored = HashMap::new();
        let mut unreadable = false;
        for scope in &scopes {
            match writer.get_change_cursor(scope.clone(), None).await {
                Ok(Some(cursor))
                    if crate::engine_errors::classify_envelope(
                        &cursor,
                        crate::engine_errors::CursorOrigin::Stored,
                        bifrost_types::AccountOperation::EstablishCursor,
                        scope,
                    )
                    .is_ok() =>
                {
                    stored.insert(scope.clone(), cursor);
                }
                Ok(None) => {}
                Ok(Some(_)) | Err(Error::SchemaIncompatible) => unreadable = true,
                Err(error) => return Err(error),
            }
        }
        if unreadable {
            tracing::warn!(
                target: "bifrost.sync.attach",
                account = ?account_id,
                "an unreadable durable cursor; clearing every durable row of the account \
                 and establishing every scope fresh"
            );
            writer.reset_account_for_schema_recovery().await?;
            stored.clear();
        }
        let mut deferred_inventory_scopes = Vec::new();
        for scope in scopes.clone() {
            let established = self
                .establish_one(
                    &writer,
                    opened.as_ref(),
                    scope.clone(),
                    stored.remove(&scope),
                    Arc::clone(&cursors),
                )
                .await;
            match established {
                Ok(InitialScope::Ready) => {}
                Ok(InitialScope::DeferredInventory(mut inventory)) => {
                    // The writer's account schema reset advances the coverage
                    // fence even before producers exist. Carry that new fence,
                    // or the deferred worker would discard every fresh walk.
                    inventory.fence = pending_coverage.scope_fence(inventory.scope());
                    deferred_inventory_scopes.push(inventory);
                }
                // A scope-local failure must stay scope-local. The running
                // path already quarantines one revoked shared/public scope
                // without touching its siblings (`disable_scope`); attach did
                // not, so a single unreachable shared mailbox or public folder
                // failed the whole account and the consumer got no sync at
                // all - primary mail included. Drop the scope and continue;
                // the next reopen re-runs discovery and picks it back up if
                // access returned.
                Err(Error::Account(error)) if scope_local_establish_failure(&error) => {
                    tracing::warn!(
                        target: "bifrost.sync.attach",
                        account = ?account_id,
                        scope = ?scope,
                        error = %error,
                        "scope-local establishment failure; skipping scope, account continues"
                    );
                }
                Err(other) => return Err(other),
            }
        }

        // Drive membership discovery to populate the push-reconciler's
        // side-index. Bounded stream; one walk per attach.
        self.discover_and_link_memberships(opened.as_ref(), Arc::clone(&cursors))
            .await?;

        let ingress = self
            .sink
            .new_ingress(self.config.multiplexer.watch_capacity);

        // Register per-account budget semaphores.
        self.scheduler.budget().register(account_id.clone());
        if let Some(meter) = &self.bandwidth_meter {
            meter.register_account(account_id.clone());
        }

        // Shutdown token tree: per-slot child of engine root.
        let shutdown = self.root_cancel.child_token();
        let throttles = Arc::new(throttle_bucket.for_slot(shutdown.clone()));

        // ArcSwap holds the current `Arc<dyn Account>` so spawned
        // workers see post-reopen handles immediately.
        let current: Arc<ArcSwap<crate::types::Incarnation>> = Arc::new(ArcSwap::from_pointee(
            crate::types::Incarnation::new(opened, crate::types::Incarnation::initial_generation()),
        ));
        let (account_generation_tx, account_generation_rx) =
            watch::channel(crate::types::Incarnation::initial_generation());
        let reopen_lock = Arc::new(AsyncMutex::new(()));

        // Reopen channel: the multiplexer's per-scope tasks raise
        // requests when a stream ends with an `EngineDirective`-class
        // recovery, carrying the originating `AccountError`.
        //
        // Depth 16 with a serial listener, and that pairing was examined and
        // kept. The finding it was raised against ("a full channel wedges
        // every scope") had real force only while poll tasks sent on it WHILE
        // HOLDING THE DRIVE LEASE: a slow reopen then blocked recovery
        // reporting, which blocked the poll task, which blocked push
        // reconciliation for that scope. `CursorRegistry::with_drive` released
        // the lease before that send, and what remains is a bounded channel
        // doing its job - backpressure on the ORIGINATING poll task, and
        // nowhere else. Keep every `reopen_tx.send().await` outside the lease
        // and this stays true. Re-open the question only on evidence that the
        // backpressure crosses into a scope the sender does not own; the bare
        // observation that the channel is bounded and the listener is serial
        // is not that evidence.
        let (reopen_tx, reopen_rx) = mpsc::channel::<ReopenRequest>(16);

        let sink_target = crate::push::SinkTarget {
            ingress: Arc::clone(&ingress),
            delivery: Arc::clone(&delivery),
            reopen_tx: reopen_tx.clone(),
            throttles: Arc::clone(&throttles),
            boundary: boundary_view.clone(),
        };
        self.sink.register(account_id.clone(), sink_target.clone());

        let mut workers: Vec<WorkerTask> = Vec::new();

        // Helper to track both the JoinHandle and a clone of its
        // AbortHandle so detach can fire `abort()` on timeout.
        let mut spawn = |role: crate::engine::context::WorkerRole,
                         fut: tokio::task::JoinHandle<()>| {
            workers.push(WorkerTask {
                role,
                abort: fut.abort_handle(),
                join: fut,
            });
        };

        // Transfer the early writer into slot teardown ownership only after
        // all fallible discovery and establishment operations have completed.
        spawn(
            crate::engine::context::WorkerRole::AckWriter,
            cleanup.writer.take().expect("attach owns writer"),
        );

        // Control applier: forwards priority and bandwidth-cap
        // changes to the currently-open protocol handle. Reopen also
        // reapplies the snapshots to the replacement handle.
        {
            let applying = control.clone();
            let control_budget = bandwidth_budget.clone();
            let control_account = Arc::clone(&current);
            let control_shutdown = shutdown.clone();
            let mut priority_view = priority_rx.clone();
            let mut bandwidth_view = bandwidth_cap_rx.clone();
            spawn(
                crate::engine::context::WorkerRole::ControlApplier,
                tokio::spawn(async move {
                    {
                        let account = control_account.load_full();
                        applying.apply_to(account.as_ref().as_ref(), control_budget.as_ref());
                    }
                    loop {
                        tokio::select! {
                            () = control_shutdown.cancelled() => return,
                            changed = priority_view.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                let account = control_account.load_full();
                                applying.apply_to(account.as_ref().as_ref(), control_budget.as_ref());
                            }
                            changed = bandwidth_view.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                let account = control_account.load_full();
                                applying.apply_to(account.as_ref().as_ref(), control_budget.as_ref());
                            }
                        }
                    }
                }),
            );
        }

        // Poll-task ownership only. Terminal verdicts live in `cursors`,
        // shared by polling and reconciliation independently of task ownership.
        let scope_tokens: crate::multiplexer::ScopeTokens =
            Arc::new(std::sync::Mutex::new(HashMap::new()));

        // Spawn push reconciler.
        let reconciler = crate::push::Reconciler {
            account_id: account_id.clone(),
            account: Arc::clone(&current),
            cursors: Arc::clone(&cursors),
            delivery: Arc::clone(&delivery),
            boundary: boundary_view.clone(),
            shutdown: shutdown.clone(),
            control: control.clone(),
            reopen_tx: reopen_tx.clone(),
            throttles: Arc::clone(&throttles),
            scheduler: self.scheduler.clone(),
            account_generation: account_generation_rx.clone(),
            #[cfg(test)]
            attempts: None,
        };
        spawn(
            crate::engine::context::WorkerRole::PushReconciler,
            tokio::spawn(reconciler.run(Arc::clone(&ingress))),
        );

        let (push_health_tx, _) = watch::channel(crate::push::health::PushHealth {
            generation: crate::types::Incarnation::initial_generation(),
            lanes: bifrost_types::effective_push_lanes(current.load().as_ref().as_ref())
                .into_iter()
                .map(|spec| crate::push::health::PushLaneHealth {
                    lane: spec.lane,
                    kind: spec.kind,
                    state: crate::push::health::PushLaneState::Starting,
                })
                .collect(),
        });
        let (push_topology_tx, push_topology_rx) =
            watch::channel(crate::engine::PushTopologyGeneration::default());
        let (push_retired_tx, _) = watch::channel(crate::types::Incarnation::initial_generation());
        let push_supervisor = crate::push::supervisor::PushSupervisor {
            account_id: account_id.clone(),
            current: Arc::clone(&current),
            generation: account_generation_rx.clone(),
            topology: push_topology_rx,
            ingress: Arc::clone(&ingress),
            delivery: Arc::clone(&delivery),
            reopen_tx: reopen_tx.clone(),
            throttles: Arc::clone(&throttles),
            boundary: boundary_view.clone(),
            shutdown: shutdown.clone(),
            health: push_health_tx.clone(),
            retired: push_retired_tx.clone(),
            drain_bound: self.config.detach_timeout / 2,
        };
        spawn(
            crate::engine::context::WorkerRole::PushSupervisor,
            tokio::spawn(push_supervisor.run()),
        );

        // Spawn the multiplexer.
        let multiplexer_cancel = shutdown.child_token();
        let mux = Multiplexer {
            account_id: account_id.clone(),
            account: Arc::clone(&current),
            account_generation: account_generation_rx,
            cursors: Arc::clone(&cursors),
            config: self.config.multiplexer,
            boundary: boundary_view.clone(),
            delivery: Arc::clone(&delivery),
            control: control.clone(),
            shutdown: multiplexer_cancel.clone(),
            reopen_tx: reopen_tx.clone(),
            scope_tokens: Arc::clone(&scope_tokens),
            throttles: Arc::clone(&throttles),
            scheduler: self.scheduler.clone(),
        };
        spawn(
            crate::engine::context::WorkerRole::Multiplexer,
            tokio::spawn(mux.run()),
        );

        // Spawn the backfill orchestrator. It walks the registered
        // scopes and runs one `BackfillRunner::run_partition` per
        // scope under a default policy. Items + checkpoints flow onto
        // the same per-account broadcast.
        // The de-dup set the partition runner filters against. Nothing
        // populates it - see the `LiveSupersedes` type docs for why
        // broadcasting a live change is not evidence the consumer got
        // it, and therefore not a sound basis for suppressing that
        // object's inventory copy.
        let live_supersedes = Arc::new(LiveSupersedes::new());

        // The slot-wide handle bundle every spawned worker below shares.
        // Cheap to clone; see `SlotContext`.
        let ctx = Arc::new(AccountSlot {
            bandwidth_budget: bandwidth_budget.clone(),
            factory: Arc::clone(&factory),
            current: Arc::clone(&current),
            cursors: Arc::clone(&cursors),
            account_id: account_id.clone(),
            control: control.clone(),
            account_control_tx: account_control_tx.clone(),
            throttles: Arc::clone(&throttles),
            boundary: boundary.clone(),
            capabilities: Arc::clone(&capabilities),
            subscriptions: Arc::new(SubscriptionRegistry::new(shutdown.clone())),
            account_generation_tx: account_generation_tx.clone(),
            push_retired_tx: push_retired_tx.clone(),
            push_topology_tx: push_topology_tx.clone(),
            push_drain_wait: self.config.detach_timeout,
            reopen_lock: Arc::clone(&reopen_lock),
            open_skips: Arc::clone(&open_skips),
            shutdown: shutdown.clone(),
            writer: std::sync::Mutex::new(Some(writer.clone())),
            coverage: Arc::clone(&pending_coverage),
            subscriber_notify: Arc::clone(&subscriber_notify),
            scheduler: self.scheduler.clone(),
            delivery: Arc::clone(&delivery),
            backfill_capacity: self.config.backfill.lane_capacity,
            reopen_tx: reopen_tx.clone(),
            deferred_repairs: Arc::new(DeferredScopeRepairs::new()),
            deferred_schema_reset: Arc::new(DeferredSchemaReset::default()),
            scope_tokens: Arc::clone(&scope_tokens),
            push_health_tx,
            multiplexer: MultiplexerHandle {
                cancel: multiplexer_cancel,
            },
            _sentinel_rx: sentinel_rx,
            _account_control_sentinel: account_control_sentinel,
            workers: std::sync::Mutex::new(Vec::new()),
            registrations: std::sync::Mutex::new(Some(context::SlotRegistrations {
                account_id: account_id.clone(),
                sink: Arc::clone(&self.sink),
                budget: self.scheduler.budget(),
                backfill_registry: Arc::clone(&self.backfill_registry),
                bandwidth_meter: self.bandwidth_meter.clone(),
            })),
        });

        let backfill_wiring = BackfillWiring {
            live: Arc::clone(&live_supersedes),
            registry: Arc::clone(&self.backfill_registry),
            config: self.config.backfill,
            fusion_owned_scopes: deferred_inventory_scopes
                .iter()
                .map(|inventory| inventory.scope().clone())
                .collect(),
        };
        let backfill_ctx = Arc::clone(&ctx);
        spawn(
            crate::engine::context::WorkerRole::BackfillOrchestrator,
            tokio::spawn(async move {
                run_backfill_orchestrator(backfill_ctx, backfill_wiring).await;
            }),
        );

        // Deferred inventory establishment must happen after the slot
        // can be subscribed to. The worker waits for a real subscriber
        // before broadcasting cold-start inventory batches, so those
        // batches do not vanish during attach.
        if !deferred_inventory_scopes.is_empty() {
            let inventory_ctx = Arc::clone(&ctx);
            spawn(
                crate::engine::context::WorkerRole::DeferredInventory,
                tokio::spawn(async move {
                    run_deferred_inventory_establishment(inventory_ctx, deferred_inventory_scopes)
                        .await;
                }),
            );
        }

        // The account supervisor serializes recovery and lifecycle requests.
        spawn(
            crate::engine::context::WorkerRole::ReopenListener,
            tokio::spawn(super::supervisor::run(Arc::clone(&ctx), reopen_rx)),
        );

        if let Some(interval) = self.config.push.orphan_teardown_retry_interval {
            let retrier_ctx = Arc::clone(&ctx);
            spawn(
                crate::engine::context::WorkerRole::OrphanTeardownRetrier,
                tokio::spawn(super::orphan_teardown::run_orphan_teardown_retrier(
                    retrier_ctx,
                    interval,
                )),
            );
        }

        // Bandwidth feed: optional periodic task that polls
        // `BandwidthMeter::account(id).observed_bps()` into the
        // control's atomic.
        if let Some(meter) = &self.bandwidth_meter {
            let meter_handle = Arc::clone(meter);
            let bw_aid = account_id.clone();
            let bw_control = control.clone();
            let bw_shutdown = shutdown.clone();
            spawn(
                crate::engine::context::WorkerRole::BandwidthFeed,
                tokio::spawn(async move {
                    let view = meter_handle.account(bw_aid);
                    loop {
                        tokio::select! {
                            () = bw_shutdown.cancelled() => return,
                            () = tokio::time::sleep(Duration::from_secs(1)) => {
                                let bps = view.observed_bps();
                                bw_control.observe_bandwidth(bps);
                            }
                        }
                    }
                }),
            );
        }

        *ctx.workers.lock().expect("worker list lock poisoned") = workers;
        self.accounts.insert(account_id.clone(), ctx);

        Ok(control)
    }

    // ---------- internal helpers ----------

    async fn discover_scopes(&self, account: &dyn Account) -> Result<Vec<CursorScope>, Error> {
        discover_scopes_from(account).await
    }

    /// Attach preflights every durable row before this shared decision, since
    /// one unreadable row requires an account-wide reset before any scope starts.
    /// Inventory is deferred until subscription so cold-start mail is delivered.
    async fn establish_one(
        &self,
        writer: &WriterHandle,
        account: &dyn Account,
        scope: CursorScope,
        stored: Option<ChangeCursor>,
        cursors: Arc<CursorRegistry>,
    ) -> Result<InitialScope, Error> {
        let fresh = stored.is_none();
        match super::establish::establishment(account, &scope, stored).await? {
            super::establish::Establishment::Ready(cursor) => {
                if fresh {
                    // No coverage claim: the single writer preserves debt.
                    writer.persist_established(cursor.clone()).await?;
                }
                cursors.put(cursor);
                Ok(InitialScope::Ready)
            }
            super::establish::Establishment::Resume { cursor, .. } => {
                Ok(InitialScope::DeferredInventory(DeferredInventory {
                    epoch: cursors.lifecycle_epoch(&scope),
                    fence: crate::cursor::PendingCoverage::initial_fence(),
                    kind: DeferredKind::Resume(cursor),
                }))
            }
            super::establish::Establishment::Inventory => {
                Ok(InitialScope::DeferredInventory(DeferredInventory {
                    epoch: cursors.lifecycle_epoch(&scope),
                    fence: crate::cursor::PendingCoverage::initial_fence(),
                    kind: DeferredKind::Start(scope),
                }))
            }
        }
    }

    /// Drive the bounded `discover_memberships` stream to completion
    /// and link each `MembershipScope` to a covering `CursorScope` in
    /// the registry. This populates the side-index the push reconciler
    /// consults on `HintPayload::SpecificMembership`.
    ///
    /// Cost is one bounded enumeration per attach; the engine accepts
    /// that cost to make hint-targeted reconciles actually scope-bound.
    async fn discover_and_link_memberships(
        &self,
        account: &dyn Account,
        cursors: Arc<CursorRegistry>,
    ) -> Result<(), Error> {
        link_discovered_memberships(account, &cursors).await
    }
}

pub(super) async fn discover_scopes_from(account: &dyn Account) -> Result<Vec<CursorScope>, Error> {
    let mut out = Vec::new();
    let mut stream = account.discover_cursor_scopes();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => out.extend(batch.items),
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(err) => return Err(Error::Account(err)),
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(out)
}

pub(super) async fn run_deferred_inventory_establishment(
    ctx: SlotContext,
    scopes: Vec<DeferredInventory>,
) {
    // Owned handles for the walk itself; `ctx` stays intact so a
    // terminated fusion can be dispatched through `ctx.recovery`.
    let account = Arc::clone(&ctx.current);
    let account_id = ctx.account_id.clone();
    let control = ctx.control.clone();
    let cursors = Arc::clone(&ctx.cursors);
    let coverage = Arc::clone(&ctx.coverage);
    let scheduler = ctx.scheduler.clone();
    let shutdown = ctx.shutdown.clone();
    let throttles = Arc::clone(&ctx.throttles);
    let writer_tx = ctx.writer();
    // The walks run one after another, so a scope held by a throttle on IT
    // ALONE (a key the account declares for that scope, such as one mailbox's
    // budget) goes to the back of the queue instead of being slept on: the
    // scopes behind it are not held and should not wait. Only once every
    // queued scope has been found held in a row does the worker sleep, until
    // the earliest of those holds lifts.
    let mut queue: std::collections::VecDeque<DeferredInventory> = scopes.into();
    let mut held_in_a_row = 0_usize;
    let mut earliest_hold: Option<std::time::Duration> = None;
    'queue: while let Some(inventory) = queue.pop_front() {
        // Subscription is not permanent: the last reader may leave during a
        // walk or between scopes. Keep undelivered work here until a reader
        // joins, without spending recovery retries or polling the provider.
        if !wait_for_real_subscriber(&ctx.delivery, &ctx.subscriber_notify, &shutdown).await {
            return;
        }
        let scope = inventory.scope().clone();
        if shutdown.is_cancelled() {
            return;
        }
        // Superseded before it was popped: a reset or deletion of this scope
        // closed since attach recorded the item. Skip the walk and hand the
        // scope to the listener, which establishes it only if it is still
        // absent and was not deleted since (`epoch`). A reset this worker's own
        // `ctx.recover` triggered has committed before the next item is
        // popped, so this sees it. Re-checked after admission below.
        if coverage.scope_fence(&scope) != inventory.fence {
            let request = ReopenRequest::EstablishIfAbsent {
                scope,
                epoch: inventory.epoch,
            };
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ctx.reopen_tx.send(request) => {},
            }
            continue;
        }
        // The generation of the incarnation the walk last ran against. A
        // terminated walk's recovery is stamped with it: dispatched in-task, it
        // can still reach the listener's arms after a reattach, and must not act
        // on the replacement, whose own reattach discovery establishes the
        // scope afresh.
        let mut walked_generation: crate::types::AccountGeneration;
        let outcome = loop {
            if !control.wait_until_running(&shutdown).await {
                return;
            }
            let gate = super::drive_start::DriveStart {
                current: &account,
                control: &control,
                scheduler: &scheduler,
                throttles: &throttles,
                account_id: &account_id,
                shutdown: &shutdown,
            };
            let admitted = match gate
                .admit_drive(
                    bifrost_types::ThrottleWork::Scope(&scope),
                    crate::scheduler::WorkKind::Backfill,
                )
                .await
            {
                Ok(admitted) => admitted,
                Err(super::drive_start::StartRefused::ShuttingDown) => return,
                Err(super::drive_start::StartRefused::Paused) => continue,
                Err(super::drive_start::StartRefused::Held { until, .. }) => {
                    let wait = until.saturating_duration_since(tokio::time::Instant::now());
                    queue.push_back(inventory);
                    held_in_a_row += 1;
                    earliest_hold = Some(earliest_hold.map_or(wait, |e| e.min(wait)));
                    if held_in_a_row >= queue.len() {
                        let sleep_for = earliest_hold.take().unwrap_or_default();
                        held_in_a_row = 0;
                        tokio::select! {
                            () = shutdown.cancelled() => return,
                            () = tokio::time::sleep(sleep_for) => {}
                        }
                    }
                    continue 'queue;
                }
            };
            held_in_a_row = 0;
            earliest_hold = None;
            let acc_arc = &admitted.incarnation;
            walked_generation = acc_arc.generation();
            let acc: &dyn Account = acc_arc.as_ref().as_ref();
            // The pre-walk fence check again, now that every wait above - a
            // pause, a throttle, admission - is behind us. A reset can commit
            // inside any of them (a schema reset parked under a pause runs the
            // moment the account resumes), and a queued `Resume` holds a cursor
            // of the encoding that reset disowned: opening its stream is the
            // likeliest thing to fail with the same schema error and run a
            // second complete account reset. The in-fusion guard would refuse
            // the walk's first publication, but only after that provider call.
            // Admission is released first: the follow-up `EstablishIfAbsent`
            // must not be sent while holding it.
            if coverage.scope_fence(&scope) != inventory.fence {
                drop(admitted);
                break Ok(crate::multiplexer::FusionOutcome::Invalidated);
            }
            let fusion = crate::multiplexer::InventoryFusion {
                expected_fence: inventory.fence,
                account_id: account_id.clone(),
                cursors: Arc::clone(&cursors),
                publisher: crate::delivery::PagePublisher::new(
                    Arc::clone(&ctx.delivery),
                    control.clone(),
                    writer_tx.clone(),
                    shutdown.clone(),
                ),
                generation: coverage.next_generation(),
            };
            let result = match &inventory.kind {
                DeferredKind::Start(_) => fusion.run_with_broadcast(acc, scope.clone()).await,
                DeferredKind::Resume(cursor) => {
                    fusion.run_resume_with_broadcast(acc, cursor.clone()).await
                }
            };
            drop(admitted);
            if matches!(result, Err(Error::Paused)) {
                continue;
            }
            if matches!(result, Err(Error::ShuttingDown)) {
                return;
            }
            break result;
        };
        match outcome {
            Ok(crate::multiplexer::FusionOutcome::Undelivered) => {
                // Preserve Start versus Resume and the fence. If a reset or
                // deletion superseded this walk, the next queue turn hands it
                // to the listener instead of resurrecting an obsolete cursor.
                queue.push_back(inventory);
            }
            // A reset or deletion superseded the walk. Hand it to the listener
            // with no admission or activity held; it establishes only if the
            // scope is absent and its lifecycle epoch has not moved since attach.
            Ok(crate::multiplexer::FusionOutcome::Invalidated) => {
                let request = ReopenRequest::EstablishIfAbsent {
                    scope,
                    epoch: inventory.epoch,
                };
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    _ = ctx.reopen_tx.send(request) => {},
                }
            }
            Ok(crate::multiplexer::FusionOutcome::Established) => {
                let current = account.load_full();
                // No shutdown arm on this provider poll, unlike the walk above:
                // a wedged `discover_memberships` keeps this worker alive until
                // detach aborts it at its worker deadline. The detach teardown
                // tests rely on exactly that to stage a straggler, so arming it
                // means restaging them - with the `Drop`-instant probe kept.
                if let Err(err) =
                    link_discovered_memberships(current.as_ref().as_ref(), &cursors).await
                {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        scope = ?scope,
                        error = %err,
                        "deferred inventory: membership refresh failed"
                    );
                }
            }
            Ok(crate::multiplexer::FusionOutcome::NoCursor) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?scope,
                    "deferred inventory completed without a cursor"
                );
            }
            Ok(crate::multiplexer::FusionOutcome::Terminated(error)) => {
                // Fusion publishes the termination; this owner records its
                // recovery effects once before forwarding to the listener.
                let _ = crate::recovery::dispatch_stream_failure(
                    error.clone(),
                    &ctx.throttles,
                    &account_id,
                );
                ctx.recover(Some(scope.clone()), error, Some(walked_generation))
                    .await;
            }
            Err(err) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?scope,
                    error = %err,
                    "deferred inventory failed"
                );
            }
        }
    }
}

/// Park until a NUMBERED subscriber - one that can acknowledge - is present
/// on the account's change stream.
///
/// Asked of the delivery gate, which holds the live numbered set, and not of
/// the broadcast's receiver count: the count includes the slot's sentinel and
/// every observer, and an observer opened the gate once - the walk ran its
/// pages into a reader that never acknowledges, and the acknowledger, joining
/// at the ring's tail afterwards, could never see them. We use the slot's
/// `subscriber_notify` (fired by `SyncEngine::account_changes_stream`, and
/// deliberately NOT by `account_changes_observer`) so this waits without
/// hot-polling. (sync-N3)
///
/// The `Notified` future is created BEFORE the predicate is checked, on every
/// turn of the loop. `Notify::notify_waiters` stores no permit: it wakes only
/// the `Notified` futures that already exist. So a check that found nobody,
/// followed by a subscribe-and-notify on another thread, followed by creating
/// the future, slept for ever with the acknowledger already subscribed - cold
/// start never began until a SECOND subscription happened to arrive. A future
/// that exists before the check is guaranteed to observe a `notify_waiters`
/// issued after it was created, even if it has not been polled yet, which
/// closes the window. Structural rather than tested: the interleaving sits
/// between two statements of one poll, and no test can preempt a poll there.
pub(super) async fn wait_for_real_subscriber(
    delivery: &crate::delivery::ChangeDelivery,
    subscriber_notify: &Notify,
    shutdown: &CancellationToken,
) -> bool {
    loop {
        let notified = subscriber_notify.notified();
        tokio::pin!(notified);
        // Register interest before checking, so a notify that lands between the
        // check and the wait is still delivered to this future.
        notified.as_mut().enable();
        if delivery.has_numbered_subscriber() {
            return true;
        }
        tokio::select! {
            () = shutdown.cancelled() => return false,
            () = &mut notified => {
                // Loop and re-check; the notification might have been
                // spurious or a subscriber may have left between the
                // notify and our check.
            }
        }
    }
}

pub(super) async fn link_discovered_memberships(
    account: &dyn Account,
    cursors: &CursorRegistry,
) -> Result<(), Error> {
    let mut stream = account.discover_memberships();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => {
                let known_scopes = cursors.all_scopes();
                for membership in batch.items {
                    for scope in &known_scopes {
                        if scope.covers(&membership) {
                            cursors.link_membership(membership.clone(), scope.clone());
                        }
                    }
                }
            }
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(err) => {
                // Surface the structured error to the caller instead
                // of returning `Ok(())` with an empty membership index.
                // A silent return here left the push reconciler routing
                // hints to "every registered scope" because the index
                // was never populated. Callers log the error and may
                // route via `reopen_tx` for terminal / engine-action
                // classes; auth-lost etc. then escalates rather than
                // silently degrading routing for the session lifetime.
                return Err(Error::Account(err));
            }
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::CheckpointStore;
    use crate::test_common as common;

    #[tokio::test(start_paused = true)]
    async fn failed_attach_cleanup_bounds_a_writer_with_a_retained_sender() {
        let id = AccountId("failed-attach-retained-sender".into());
        let lifecycle = Arc::new(AsyncMutex::new(std::collections::HashSet::from([
            id.clone()
        ])));
        let (sender, mut receiver) = mpsc::channel::<()>(1);
        let writer = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        let abort = writer.abort_handle();
        let mut cleanup = AttachingWriter {
            lifecycle: Some((Arc::clone(&lifecycle), id.clone())),
            account: None,
            writer: Some(writer),
            close_timeout: Duration::from_secs(1),
            runtime: tokio::runtime::Handle::current(),
            bandwidth_budget: None,
        };
        let task = cleanup.launch_cleanup();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("retained sender must not block cleanup")
            .expect("cleanup task");
        assert!(!lifecycle.lock().await.contains(&id));
        // Join cancellation is asynchronous; yield until the writer observes it.
        tokio::time::timeout(Duration::from_secs(1), async {
            while !abort.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writer aborted");
        drop(sender);
    }

    #[tokio::test]
    async fn attaching_cleanup_can_be_dropped_outside_an_entered_runtime() {
        let id = AccountId("attach-drop-outside-runtime".into());
        let lifecycle = Arc::new(AsyncMutex::new(std::collections::HashSet::from([
            id.clone()
        ])));
        let cleanup = AttachingWriter {
            lifecycle: Some((Arc::clone(&lifecycle), id.clone())),
            account: None,
            writer: None,
            close_timeout: Duration::from_secs(1),
            runtime: tokio::runtime::Handle::current(),
            bandwidth_budget: None,
        };
        std::thread::spawn(move || drop(cleanup))
            .join()
            .expect("drop must not panic");
        tokio::time::timeout(Duration::from_secs(5), async {
            while lifecycle.lock().await.contains(&id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("captured runtime executes cleanup");
    }

    #[tokio::test]
    async fn attach_defers_a_resume_stream_rejected_by_its_classifier() {
        let account_id = AccountId("attach-resume-contract".into());
        let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
        let cursor = common::cursor_for(&scope, b"inventory-page");
        let store = Arc::new(InMemoryCheckpointStore::new());
        store
            .apply_transition(
                &account_id,
                CheckpointTransition {
                    checkpoint: Checkpoint::Change(cursor.clone()),
                    ledger: crate::cursor::DebtLedger::default(),
                },
            )
            .await
            .expect("seed stored cursor");
        let engine = SyncEngine::builder()
            .checkpoints(store)
            .build()
            .expect("engine");
        let mut account = common::StubAccount::new(Vec::new());
        account.resume_inventory = true;
        account.resume_inventory_classifier = Some(false);
        let cursors = Arc::new(CursorRegistry::new());

        let (writer_tx, _writer_rx) = mpsc::channel(1);
        let writer = WriterHandle::new(writer_tx);
        let established = engine
            .establish_one(
                &writer,
                &account,
                scope.clone(),
                Some(cursor.clone()),
                Arc::clone(&cursors),
            )
            .await
            .expect("stored cursor is deferred for the resume guard");

        let InitialScope::DeferredInventory(DeferredInventory {
            kind: DeferredKind::Resume(resumed),
            ..
        }) = established
        else {
            panic!("a supplied resume stream must stay out of the changes registry")
        };
        assert_eq!(resumed, cursor);
        assert!(cursors.snapshot(&scope).is_none());
        assert_eq!(
            account
                .inventory_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}
