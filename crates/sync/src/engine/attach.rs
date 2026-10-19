//! Attach wiring: scope discovery, cursor establishment, and the
//! per-slot worker spawn block.

use super::*;

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
        let result = self.attach_inner(account_id.clone(), factory).await;
        // Always release the in-flight guard, success or failure.
        {
            let mut guard = self.lifecycle_inflight.lock().await;
            guard.remove(&account_id);
        }
        result
    }

    async fn attach_inner(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
    ) -> Result<SyncControl, Error> {
        let OpenedAccount {
            account: opened,
            skipped_scopes,
        } = factory
            .open(account_id.clone())
            .await
            .map_err(Error::OpenFailed)?;
        log_open_skips(&account_id, &skipped_scopes, "attach");
        let cleanup = Arc::clone(&opened);
        let result = self
            .attach_opened(account_id, factory, opened, skipped_scopes)
            .await;
        if result.is_err()
            && let Err(error) = cleanup.close().await
        {
            tracing::warn!(
                target: "bifrost.sync.attach",
                error = ?error,
                "account close failed while unwinding attach"
            );
        }
        result
    }

    async fn attach_opened(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
        opened: Arc<dyn Account>,
        skipped_scopes: Vec<SkippedScope>,
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
        // `multiplexer::ChangeDelivery`.
        let delivery = Arc::new(crate::multiplexer::ChangeDelivery::new(changes_tx.clone()));

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
        // across accounts so `Tenant` / `Provider` deadlines recorded
        // by one account pause its siblings.
        let throttles = Arc::clone(&self.throttles);

        // Drive cursor establishment per scope. We do this before
        // spawning long-running tasks because multiplexer + backfill
        // need the registry pre-populated for `Ready` scopes. For
        // `EstablishViaInventory` scopes the inventory walk IS the
        // cursor establishment AND surfaces inventory items - we
        // defer those walks until the slot can be subscribed to, so
        // consumers observe cold-start data the same way they observe
        // live changes.
        let scopes = self.discover_scopes(opened.as_ref()).await?;
        let mut deferred_inventory_scopes = Vec::new();
        for scope in scopes.clone() {
            let established = self
                .establish_one(
                    &account_id,
                    opened.as_ref(),
                    scope.clone(),
                    Arc::clone(&cursors),
                )
                .await;
            match established {
                Ok(InitialScope::Ready) => {}
                Ok(InitialScope::DeferredInventory(inventory)) => {
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

        // Wire boundary + priority watches.
        let (boundary, boundary_view) = Boundary::new();
        let (priority_tx, priority_rx) = watch::channel(Priority::Normal);
        let (bandwidth_cap_tx, bandwidth_cap_rx) = watch::channel(None);

        // Per-account watch-event sender / receiver. The reconciler
        // owns the receiver; the multiplexer (in-process forwarder)
        // and the engine's `InvalidationSink` both feed the sender.
        let (watch_tx, watch_rx) =
            mpsc::channel::<WatchEvent>(self.config.multiplexer.watch_capacity);
        self.sink.register(account_id.clone(), watch_tx.clone());

        // Per-account ack channel: consumers push
        // (scope, checkpoint) here after committing the matching
        // batch; a dedicated writer task persists them to
        // `CheckpointStore`.
        let (ack_tx, ack_rx) = mpsc::channel::<WriterRequest>(256);
        self.ack_senders.insert(account_id.clone(), ack_tx.clone());

        // Register per-account budget semaphores.
        self.scheduler.budget().register(account_id.clone());
        if let Some(meter) = &self.bandwidth_meter {
            meter.register_account(account_id.clone());
        }

        // Shutdown token tree: per-slot child of engine root.
        let shutdown = self.root_cancel.child_token();

        // ArcSwap holds the current `Arc<dyn Account>` so spawned
        // workers see post-reopen handles immediately.
        let current: Arc<ArcSwap<Arc<dyn Account>>> = Arc::new(ArcSwap::from(Arc::new(opened)));
        let (account_generation_tx, account_generation_rx) = watch::channel(0_u64);
        let reopen_lock = Arc::new(AsyncMutex::new(()));

        // Control handle shared between the SyncControl returned to
        // the consumer and the engine's spawned workers (so workers
        // can call `record_checkpoint`).
        let pending_coverage = Arc::new(PendingCoverage::new());
        let control = SyncControl::new_with_publications(
            account_id.clone(),
            boundary.clone(),
            priority_tx.clone(),
            bandwidth_cap_tx.clone(),
            Arc::clone(&pending_coverage),
        );

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
        let (reopen_tx, mut reopen_rx) = mpsc::channel::<ReopenRequest>(16);

        let mut workers: Vec<WorkerTask> = Vec::new();

        // Helper to track both the JoinHandle and a clone of its
        // AbortHandle so detach can fire `abort()` on timeout.
        let mut spawn = |role: crate::types::WorkerRole, fut: tokio::task::JoinHandle<()>| {
            workers.push(WorkerTask {
                role,
                abort: fut.abort_handle(),
                join: fut,
            });
        };

        // Ack writer: durably persists cursors as they are acked. Lives
        // as long as the slot does.
        let ack_writer_store = Arc::clone(&self.checkpoints);
        let ack_writer_aid = account_id.clone();
        let ack_writer_control = control.clone();
        // Producers record what each enumeration proved; the single writer
        // reads it back when the matching acknowledgement arrives.
        spawn(
            crate::types::WorkerRole::AckWriter,
            tokio::spawn(ack_writer(
                ack_writer_aid,
                ack_writer_store,
                ack_writer_control,
                Arc::clone(&pending_coverage),
                ack_rx,
            )),
        );

        // Control applier: forwards priority and bandwidth-cap
        // changes to the currently-open protocol handle. Reopen also
        // reapplies the snapshots to the replacement handle.
        {
            let control_account = Arc::clone(&current);
            let control_shutdown = shutdown.clone();
            let mut priority_view = priority_rx.clone();
            let mut bandwidth_view = bandwidth_cap_rx.clone();
            spawn(
                crate::types::WorkerRole::ControlApplier,
                tokio::spawn(async move {
                    {
                        let account = control_account.load_full();
                        account
                            .as_ref()
                            .as_ref()
                            .set_priority(*priority_view.borrow());
                        account
                            .as_ref()
                            .as_ref()
                            .set_bandwidth_cap(*bandwidth_view.borrow());
                    }
                    loop {
                        tokio::select! {
                            () = control_shutdown.cancelled() => return,
                            changed = priority_view.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                let priority = *priority_view.borrow();
                                let account = control_account.load_full();
                                account.as_ref().as_ref().set_priority(priority);
                            }
                            changed = bandwidth_view.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                let cap = *bandwidth_view.borrow();
                                let account = control_account.load_full();
                                account.as_ref().as_ref().set_bandwidth_cap(cap);
                            }
                        }
                    }
                }),
            );
        }

        // The multiplexer's per-scope poll tokens. Created here rather than
        // inside `Multiplexer` because the push reconciler - spawned first -
        // shares them: it is the other producer on the same lane and must
        // honor the same terminal tombstones the 1s poll scan does.
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
            scope_tokens: Arc::clone(&scope_tokens),
        };
        spawn(
            crate::types::WorkerRole::PushReconciler,
            tokio::spawn(reconciler.run(watch_rx)),
        );

        // Push forwarder: drain `Account::push_stream` into the
        // per-account mpsc. In-process accounts carry invalidations
        // and health here; out-of-process accounts may carry only
        // subscription-health transitions.
        {
            let acc = Arc::clone(&current);
            let aid = account_id.clone();
            let tx = watch_tx.clone();
            let sd = shutdown.clone();
            let mut generation = account_generation_rx.clone();
            spawn(
                crate::types::WorkerRole::PushForwarder,
                tokio::spawn(async move {
                    let mut reconnect_delay = Duration::from_millis(50);
                    loop {
                        if sd.is_cancelled() {
                            return;
                        }
                        let acc_arc = acc.load_full();
                        if acc_arc.capabilities().push == bifrost_types::PushCapability::None {
                            tokio::select! {
                                () = sd.cancelled() => return,
                                changed = generation.changed() => {
                                    if changed.is_err() {
                                        return;
                                    }
                                }
                            }
                            continue;
                        }
                        let mut stream = acc_arc.push_stream();
                        let mut received = false;
                        let mut reopened = false;
                        loop {
                            tokio::select! {
                                () = sd.cancelled() => return,
                                changed = generation.changed() => {
                                    if changed.is_err() {
                                        return;
                                    }
                                    reopened = true;
                                    break;
                                }
                                next = stream.next() => {
                                    let Some(event) = next else { break; };
                                    if !matches!(&event, WatchEvent::Terminated(_)) {
                                        received = true;
                                    }
                                    match tx.try_send(event) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(rejected)) => {
                                            tracing::trace!(
                                                target: "bifrost.sync.changes",
                                                account = ?aid,
                                                "in-process push: queue full, coalesced"
                                            );
                                            let lossless =
                                                crate::push::requires_lossless_delivery(&rejected);
                                            let delivery = if lossless {
                                                rejected
                                            } else {
                                                crate::push::coalesced_event(rejected)
                                            };
                                            if lossless {
                                                tokio::select! {
                                                    () = sd.cancelled() => return,
                                                    _ = tx.send(delivery) => {}
                                                }
                                            } else {
                                                tokio::select! {
                                                    () = sd.cancelled() => return,
                                                    _ = tokio::time::timeout(
                                                        Duration::from_millis(100),
                                                        tx.send(delivery),
                                                    ) => {}
                                                }
                                            }
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => return,
                                    }
                                }
                            }
                        }
                        if reopened {
                            reconnect_delay = Duration::from_millis(50);
                            continue;
                        }
                        if received {
                            reconnect_delay = Duration::from_millis(50);
                        }
                        // push_stream ended; loop reloads the (possibly
                        // reopened) handle and restarts. Exponential
                        // backoff prevents an empty stream implementation
                        // from reconstructing itself 20 times per second.
                        tokio::select! {
                            () = sd.cancelled() => return,
                            () = tokio::time::sleep(reconnect_delay) => {}
                        }
                        reconnect_delay = reconnect_delay
                            .saturating_mul(2)
                            .min(Duration::from_secs(30));
                    }
                }),
            );
        }

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
            // The SAME map the reconciler got above, not a fresh one. A private
            // map here silently un-wires the terminal tombstone: the poll scan
            // would write parks nobody reads and the reconciler would read a
            // map nothing writes, so `scope_is_parked` answered `false`
            // unconditionally and a push hint re-drove a scope whose poll loop
            // had permanently retired.
            scope_tokens: Arc::clone(&scope_tokens),
            throttles: Arc::clone(&throttles),
            scheduler: self.scheduler.clone(),
        };
        spawn(
            crate::types::WorkerRole::Multiplexer,
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
        let ctx = SlotContext {
            factory: Arc::clone(&factory),
            current: Arc::clone(&current),
            cursors: Arc::clone(&cursors),
            changes_tx: changes_tx.clone(),
            account_id: account_id.clone(),
            control: control.clone(),
            account_control_tx: account_control_tx.clone(),
            throttles: Arc::clone(&throttles),
            boundary_tx: boundary.sender(),
            capabilities: Arc::clone(&capabilities),
            subscriptions: Arc::clone(&self.subscriptions),
            account_generation_tx: account_generation_tx.clone(),
            reopen_lock: Arc::clone(&reopen_lock),
            open_skips: Arc::clone(&open_skips),
            shutdown: shutdown.clone(),
            writer_tx: ack_tx.clone(),
            coverage: Arc::clone(&pending_coverage),
            subscriber_notify: Arc::clone(&subscriber_notify),
            scheduler: self.scheduler.clone(),
            delivery: Arc::clone(&delivery),
            backfill_capacity: self.config.backfill.lane_capacity,
            reopen_tx: reopen_tx.clone(),
            deferred_repairs: Arc::new(DeferredScopeRepairs::new()),
            scope_tokens: Arc::clone(&scope_tokens),
        };

        let backfill_wiring = BackfillWiring {
            live: Arc::clone(&live_supersedes),
            store: Arc::clone(&self.checkpoints),
            registry: Arc::clone(&self.backfill_registry),
            config: self.config.backfill,
            fusion_owned_scopes: deferred_inventory_scopes
                .iter()
                .map(|inventory| inventory.scope().clone())
                .collect(),
            changes_tx: Some(changes_tx.clone()),
        };
        let backfill_ctx = ctx.clone();
        spawn(
            crate::types::WorkerRole::BackfillOrchestrator,
            tokio::spawn(async move {
                run_backfill_orchestrator(backfill_ctx, backfill_wiring).await;
            }),
        );

        // Deferred inventory establishment must happen after the slot
        // can be subscribed to. The worker waits for a real subscriber
        // before broadcasting cold-start inventory batches, so those
        // batches do not vanish during attach.
        if !deferred_inventory_scopes.is_empty() {
            let inventory_ctx = ctx.clone();
            spawn(
                crate::types::WorkerRole::DeferredInventory,
                tokio::spawn(async move {
                    run_deferred_inventory_establishment(inventory_ctx, deferred_inventory_scopes)
                        .await;
                }),
            );
        }

        // Spawn the reopen listener.
        let reopen_ctx = ctx.clone();
        spawn(
            crate::types::WorkerRole::ReopenListener,
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = reopen_ctx.shutdown.cancelled() => return,
                        req = reopen_rx.recv() => {
                            let Some(req) = req else { return; };
                            match req {
                                ReopenRequest::Recovery { scope, error } => {
                                    reopen_ctx.recover(scope, error).await;
                                }
                                ReopenRequest::ScopeDeleted { scope } => {
                                    // A repair parked for this scope under a
                                    // pause must not establish it after resume:
                                    // the folder it was for is gone.
                                    reopen_ctx.deferred_repairs.unpark(&scope);
                                    // A cursor here was established after the
                                    // deletion was raised: a `Created` whose
                                    // repair was still queued ahead of this
                                    // request on this same channel. Retire it
                                    // exactly as the multiplexer retires a
                                    // registered scope - registry first, then
                                    // the poll token - before purging its rows,
                                    // or the rows would be deleted out from
                                    // under a live cursor for a folder that no
                                    // longer exists.
                                    if reopen_ctx.cursors.snapshot(&scope).is_some() {
                                        reopen_ctx.cursors.delete(&scope);
                                        crate::multiplexer::cancel_scope_token(
                                            &reopen_ctx.scope_tokens,
                                            &scope,
                                        );
                                    }
                                    // A provider-deleted folder retires its
                                    // durable rows in full: change cursor and
                                    // backfill rows, completion marker
                                    // included. A surviving marker would make
                                    // a folder recreated under the same id
                                    // skip its cold-start walk entirely.
                                    let writer = reopen_ctx.writer();
                                    if let Err(error) =
                                        writer.reset_scope_for_deletion(scope.clone()).await
                                    {
                                        tracing::warn!(
                                            target: "bifrost.sync.changes",
                                            account = ?reopen_ctx.account_id,
                                            scope = ?scope,
                                            error = %error,
                                            "failed to purge durable rows for a provider-deleted scope"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }),
        );

        if let Some(interval) = self.config.push.orphan_teardown_retry_interval {
            let retrier_ctx = ctx.clone();
            spawn(
                crate::types::WorkerRole::OrphanTeardownRetrier,
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
                crate::types::WorkerRole::BandwidthFeed,
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

        let multiplexer = MultiplexerHandle {
            cancel: multiplexer_cancel,
            changes_tx: changes_tx.clone(),
        };
        let slot = Arc::new(AccountSlot {
            scheduler: self.scheduler.clone(),
            factory,
            current,
            account_generation_tx,
            reopen_lock,
            capabilities,
            multiplexer,
            cursors: Arc::clone(&cursors),
            coverage: Arc::clone(&pending_coverage),
            boundary_tx: boundary.sender(),
            shutdown: shutdown.clone(),
            control: control.clone(),
            _sentinel_rx: sentinel_rx,
            workers: std::sync::Mutex::new(workers),
            account_control_tx,
            _account_control_sentinel: account_control_sentinel,
            subscriber_notify,
            reopen_tx: reopen_tx.clone(),
            throttles: Arc::clone(&throttles),
            open_skips,
            delivery,
        });

        self.accounts.insert(account_id.clone(), slot);

        Ok(control)
    }

    // ---------- internal helpers ----------

    async fn discover_scopes(&self, account: &dyn Account) -> Result<Vec<CursorScope>, Error> {
        discover_scopes_from(account).await
    }

    async fn establish_one(
        &self,
        account_id: &AccountId,
        account: &dyn Account,
        scope: CursorScope,
        cursors: Arc<CursorRegistry>,
    ) -> Result<InitialScope, Error> {
        // Check the store first - resume path.
        //
        // A schema-incompatible envelope is healed HERE rather than
        // handed to the recovery translator. The running-account path
        // (`run_establish`) can return a typed `AccountError` because a
        // reopen listener is alive to receive
        // `Engine(SchemaIncompatible)` and run the schema-clear loop;
        // at attach that listener does not exist yet - it is spawned
        // further down `attach_inner`. Returning an error here instead
        // would fail the whole attach with nothing left running to
        // clear the undecodable row, so the account would refuse to
        // attach on every subsequent start with no recovery path.
        // Dropping the row and re-establishing this scope is exactly
        // what `handle_schema_incompatible` would have done, scoped to
        // the one cursor that cannot be read. (sync-D10)
        match self.checkpoints.get_change_cursor(account_id, &scope).await {
            Ok(Some(existing)) if existing.validate_envelope().is_ok() => {
                if account.is_inventory_cursor(&existing) {
                    return Ok(InitialScope::DeferredInventory(DeferredInventory::Resume(
                        existing,
                    )));
                }
                cursors.put(existing);
                return Ok(InitialScope::Ready);
            }
            Ok(None) => {}
            Ok(Some(_)) | Err(Error::SchemaIncompatible) => {
                tracing::warn!(
                    target: "bifrost.sync.attach",
                    account = ?account_id,
                    scope = ?scope,
                    "schema-incompatible cursor envelope; clearing the row and re-establishing the scope"
                );
                if let Err(err) = self
                    .checkpoints
                    .delete_change_cursor(account_id, &scope)
                    .await
                {
                    // Non-fatal: re-establishment overwrites the row on
                    // success anyway. Log and keep going rather than
                    // failing the attach on a store hiccup.
                    tracing::warn!(
                        target: "bifrost.sync.attach",
                        account = ?account_id,
                        scope = ?scope,
                        error = %err,
                        "delete_change_cursor failed while clearing an unreadable envelope"
                    );
                }
            }
            Err(other) => return Err(other),
        }
        match account
            .establish_initial_cursor(scope.clone())
            .await
            .map_err(Error::Account)?
        {
            CursorEstablishment::Ready(cursor) => {
                self.persist_cursor(account_id, cursor, cursors).await?;
                Ok(InitialScope::Ready)
            }
            CursorEstablishment::EstablishViaInventory => Ok(InitialScope::DeferredInventory(
                DeferredInventory::Start(scope),
            )),
            // `CursorEstablishment` is `#[non_exhaustive]`; treat any
            // future variant as "cannot establish" so the engine fails
            // closed rather than silently continuing.
            _ => Err(Error::EstablishCursorFailed(
                "unknown CursorEstablishment variant".into(),
            )),
        }
    }

    async fn persist_cursor(
        &self,
        account_id: &AccountId,
        cursor: ChangeCursor,
        cursors: Arc<CursorRegistry>,
    ) -> Result<(), Error> {
        cursor
            .validate_envelope()
            .map_err(|_| Error::SchemaIncompatible)?;
        // Carries no coverage report, so the ledger is preserved rather than
        // overwritten: establishing a cursor proves nothing about what any
        // enumeration covered.
        let ledger = self.checkpoints.get_ledger(account_id).await?;
        self.checkpoints
            .apply_transition(
                account_id,
                CheckpointTransition {
                    checkpoint: Checkpoint::Change(cursor.clone()),
                    ledger,
                },
            )
            .await?;
        cursors.put(cursor);
        Ok(())
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

/// True if the given cursor scope covers the given membership scope.
///
/// The mapping is engine policy: account-wide cursors cover every
/// membership; folder-typed cursors cover only the matching folder;
/// query cursors cover only the matching query.
pub(super) fn scope_covers_membership(scope: &CursorScope, membership: &MembershipScope) -> bool {
    use bifrost_types::FolderId;
    match (scope, membership) {
        // Account-wide cursors cover every membership.
        (CursorScope::Account, _) | (CursorScope::Type(_), _) => true,
        // Folder-typed cursors cover the matching folder only.
        (CursorScope::Folder(folder), MembershipScope::Folder(m)) => folder == m,
        (CursorScope::FolderType { folder, .. }, MembershipScope::Folder(m)) => folder == m,
        // Folder-typed cursors over an arbitrary folder match mailbox
        // memberships that share the same id (Graph maps folders 1:1
        // to mailboxes; we treat the FolderId / MailboxId String the
        // same in the side index).
        (CursorScope::Folder(FolderId(folder_id)), MembershipScope::Mailbox(mbx)) => {
            folder_id == &mbx.0
        }
        (
            CursorScope::FolderType {
                folder: FolderId(folder_id),
                ..
            },
            MembershipScope::Mailbox(mbx),
        ) => folder_id == &mbx.0,
        // Query cursors cover the same query membership.
        (CursorScope::Query(q), MembershipScope::Query(mq)) => q == mq,
        _ => false,
    }
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
    let writer_tx = ctx.writer_tx.clone();
    if !wait_for_real_subscriber(&ctx.delivery, &ctx.subscriber_notify, &shutdown).await {
        return;
    }
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
        let scope = inventory.scope().clone();
        if shutdown.is_cancelled() {
            return;
        }
        // The account-wide hold is slept out FIRST and the scope-local decision
        // taken after it, so a short account hold overlapping a long mailbox
        // hold cannot leave this scope in the inline wait below with every
        // later scope queued behind it.
        while let Some(wait) = crate::recovery::account_throttle_wait(&throttles, &account_id) {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(wait) => {}
            }
        }
        let mut keys = crate::recovery::declared_keys(
            account.load_full().as_ref().as_ref(),
            &account_id,
            bifrost_types::ThrottleWork::Scope(&scope),
        );
        if let Some(wait) = crate::recovery::throttle_wait(&throttles, &account_id, &keys) {
            tracing::debug!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                wait_secs = wait.as_secs(),
                "deferred inventory requeued behind a throttle on this scope"
            );
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
            continue;
        }
        held_in_a_row = 0;
        earliest_hold = None;
        let outcome = loop {
            if !control.wait_until_running(&shutdown).await {
                return;
            }
            // Honor every throttle deadline this walk consumes before the
            // inventory walk, mirroring the backfill partition runner:
            // deferred establishment drives the same heavy inventory
            // lane. Loop back to the boundary check after waking.
            if crate::recovery::throttle_wait(&throttles, &account_id, &keys).is_some() {
                // A hold that appeared during the pause park or admission.
                // Only the ACCOUNT-WIDE part is slept here, never the
                // combined wait: a scope-local hold sends this scope back on
                // the queue (the outer check counts and sleeps it), so it
                // never holds the scopes behind it.
                let Some(wait) = crate::recovery::account_throttle_wait(&throttles, &account_id)
                else {
                    queue.push_back(inventory);
                    continue 'queue;
                };
                tracing::debug!(
                    target: "bifrost.sync.backfill",
                    account = ?account_id,
                    scope = ?scope,
                    wait_secs = wait.as_secs(),
                    "deferred inventory deferred by shared throttle deadline"
                );
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
                continue;
            }
            // Deferred fusion is one of the heaviest cold-start walks in
            // the engine, so it takes a permit like every other protocol
            // path. Held only for the walk itself; the throttle wait and
            // the recovery handling below run outside it.
            let admission = tokio::select! {
                () = shutdown.cancelled() => return,
                permit = scheduler.admit(
                    account_id.clone(),
                    control.priority_snapshot(),
                    crate::scheduler::WorkKind::Sync,
                ) => match permit {
                    Ok(permit) => permit,
                    Err(error) => {
                        tracing::warn!(
                            target: "bifrost.sync.scheduler",
                            account = ?account_id,
                            scope = ?scope,
                            %error,
                            "deferred inventory admission refused; retrying"
                        );
                        tokio::select! {
                            () = shutdown.cancelled() => return,
                            () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                        }
                        continue;
                    }
                },
            };
            // Admission can park, and a hold recorded meanwhile predates this
            // walk. Release the permit and go back round to the wait above.
            //
            // The keys are asked of the account the walk is about to run
            // against: a reopen during the waits above can install one whose
            // declaration differs from the keys computed before them.
            let acc_arc = account.load_full();
            let acc: &dyn Account = acc_arc.as_ref().as_ref();
            let final_keys = crate::recovery::declared_keys(
                acc,
                &account_id,
                bifrost_types::ThrottleWork::Scope(&scope),
            );
            if crate::recovery::throttle_wait(&throttles, &account_id, &final_keys).is_some() {
                // Carry the fresh keys to the wait at the top of the loop, or
                // it would check the stale ones and re-admit straight away.
                keys = final_keys;
                drop(admission);
                continue;
            }
            let fusion = crate::multiplexer::InventoryFusion {
                account_id: account_id.clone(),
                cursors: Arc::clone(&cursors),
                control: Some(control.clone()),
                coverage: Some(Arc::clone(&coverage)),
                writer_tx: Some(writer_tx.clone()),
                generation: coverage.next_generation(),
                shutdown: Some(shutdown.clone()),
            };
            let result = match &inventory {
                DeferredInventory::Start(_) => {
                    fusion
                        .run_with_broadcast(acc, scope.clone(), Some(Arc::clone(&ctx.delivery)))
                        .await
                }
                DeferredInventory::Resume(cursor) => {
                    fusion
                        .run_resume_with_broadcast(
                            acc,
                            cursor.clone(),
                            Some(Arc::clone(&ctx.delivery)),
                        )
                        .await
                }
            };
            drop(admission);
            if matches!(result, Err(Error::Paused)) {
                continue;
            }
            if matches!(result, Err(Error::ShuttingDown)) {
                return;
            }
            break result;
        };
        match outcome {
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
                ctx.recover(Some(scope.clone()), error).await;
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
    delivery: &crate::multiplexer::ChangeDelivery,
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
                        if scope_covers_membership(scope, &membership) {
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
