//! Per-account multiplexer.
//!
//! One task per account drives:
//! - `changes_stream(cursor)` for each known scope,
//! - the adaptive NOOP/STATUS poll cadence per scope,
//! - `scope_lifecycle_stream()` for scope creation / rename / deletion,
//! - inventory fusion for `EstablishViaInventory` scopes (the
//!   inventory's terminal `Done` carries the cursor, which the
//!   multiplexer registers exactly once).
//!
//! Output is a unified `SyncEvent<Change>` stream on the account's
//! broadcast channel.

pub mod changes;
pub mod fusion;
mod lifecycle;
pub mod poll;
use crate::cursor::membership::membership_to_cursor_scopes;
use crate::cursor::membership::membership_to_deleted_scopes;
use lifecycle::LifecycleWorker;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
#[cfg(test)]
use bifrost_types::SyncEvent;
use bifrost_types::{
    Account, AccountError, AccountId, ChangeCursor, Checkpoint, CursorScope, ScopeLifecycle,
};
use futures::stream::StreamExt;
#[cfg(test)]
use tokio::sync::broadcast;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;
use crate::types::MultiplexerConfig;

pub(crate) use crate::delivery::{ChangeDelivery, EventScope, MultiplexerEvent, PagePublisher};
pub use changes::{ChangesEvent, drive_changes_stream};
pub use fusion::{FusionOutcome, InventoryFusion};
pub use poll::{AdaptiveCadence, PollSchedule};

/// Handle the engine stashes in the per-account slot for the
/// multiplexer task. Cancellation is rooted in the slot's shutdown
/// token; explicit shutdown shuts the task down between two
/// `boundary.peek()` calls.
#[derive(Debug)]
pub struct MultiplexerHandle {
    pub cancel: CancellationToken,
}

use crate::engine::supervisor::ReopenRequest;

/// Ownership stamp for a spawned poll task; only the token owner mints it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScopeTokenGeneration(u64);

impl ScopeTokenGeneration {
    fn mint() -> Self {
        Self(SCOPE_TOKEN_GENERATION.fetch_add(1, Ordering::Relaxed))
    }
}

/// A per-scope poll task's cancellation token, tagged with the
/// identity of the task that owns it.
///
/// The generation is what makes cleanup safe. A task removing its
/// entry by key alone can evict a SUCCESSOR's token: task A exits
/// naturally while its scope is momentarily absent from the registry
/// (mid-`restart_scope`), the 1s scan in `Multiplexer::run` sees a
/// re-established cursor with no live token and spawns task B, and
/// A's cleanup then deletes B's entry. The next scan sees no token and
/// spawns C, leaving B and C polling the same scope forever - doubled
/// wire traffic, doubled broadcasts, and two concurrent producers on
/// one lane+scope, which is exactly what `SyncControl`'s checkpoint
/// supersession assumes cannot happen.
#[derive(Debug, Clone)]
pub struct ScopeToken {
    generation: ScopeTokenGeneration,
    token: CancellationToken,
    drive_generation: Option<crate::cursor::DriveGeneration>,
}

impl ScopeToken {
    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    fn cancel(&self) {
        self.token.cancel();
    }
}

/// Per-scope poll tokens, keyed by the scope the task polls.
pub type ScopeTokens = Arc<StdMutex<HashMap<CursorScope, ScopeToken>>>;

/// Monotonic source of `ScopeToken::generation`. Process-wide rather
/// than per-multiplexer: the value only has to be unique, and threading
/// another `Arc` through the poll-spawn argument list buys nothing.
static SCOPE_TOKEN_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Multiplexer task state. Constructed in `engine::attach` and run on
/// a `tokio::spawn`.
pub struct Multiplexer {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<crate::types::Incarnation>>,
    pub account_generation: watch::Receiver<crate::types::AccountGeneration>,
    pub cursors: Arc<CursorRegistry>,
    pub config: MultiplexerConfig,
    pub boundary: BoundaryView,
    /// The account's change delivery gate: the broadcast sender plus the
    /// numbering of the receivers that may acknowledge. Every checkpoint the
    /// poll loop publishes goes through it, so "did a numbered receiver take
    /// delivery" is decided under its lock rather than read off a count.
    pub delivery: Arc<ChangeDelivery>,
    pub control: SyncControl,
    pub shutdown: CancellationToken,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    /// Per-scope cancellation tokens keyed by membership-id so
    /// `ScopeLifecycle::Deleted` can stop the matching poll task.
    pub scope_tokens: ScopeTokens,
    /// Engine-wide throttle bucket. Poll tasks consult it before each
    /// drive so a tenant- or account-wide `Retry-After` recorded by one
    /// scope (or a sibling account) pauses the others; the Retry arm
    /// records deadlines it observes.
    pub throttles: Arc<crate::throttle::ThrottleBucket>,
    pub scheduler: crate::scheduler::Scheduler,
}

impl Multiplexer {
    /// Apply the adaptive cadence rule to a scope: halve on change,
    /// double after five consecutive no-change ticks, clamped to
    /// `[poll_min, poll_max]`. Pure function; tested below.
    #[must_use]
    pub fn updated_cadence(
        cur: AdaptiveCadence,
        seen_change: bool,
        min: Duration,
        max: Duration,
    ) -> AdaptiveCadence {
        if seen_change {
            AdaptiveCadence {
                interval: (cur.interval / 2).max(min),
                no_change_streak: 0,
            }
        } else {
            let new_streak = cur.no_change_streak.saturating_add(1);
            let interval = if new_streak >= 5 {
                cur.interval.saturating_mul(2).min(max)
            } else {
                cur.interval
            };
            AdaptiveCadence {
                interval,
                no_change_streak: if new_streak >= 5 { 0 } else { new_streak },
            }
        }
    }

    /// Bookkeeping helper: register a cursor in the registry. Idempotent.
    pub fn register_cursor(&self, cursor: ChangeCursor) {
        self.cursors.put(cursor);
    }

    /// Run the multiplexer loop until the shutdown token is cancelled.
    ///
    /// Spawns per-scope poll tasks (each driving
    /// `changes_stream(cursor)` with adaptive cadence) and a single
    /// `scope_lifecycle_stream` task. New scopes added at runtime get
    /// their own poll task via `ScopeLifecycle::Created`; deleted
    /// scopes have their per-scope cancellation token tripped and the
    /// cursor entry dropped from the registry.
    pub async fn run(self) {
        let Self {
            account_id,
            account,
            account_generation,
            cursors,
            config,
            boundary,
            delivery,
            control,
            shutdown,
            reopen_tx,
            scope_tokens,
            throttles,
            scheduler,
        } = self;

        // Snapshot the known scopes from the registry; the engine has
        // populated it via `establish_one` before spawning us.
        spawn_missing_scope_polls(
            account_id.clone(),
            Arc::clone(&account),
            Arc::clone(&cursors),
            config,
            boundary.clone(),
            Arc::clone(&delivery),
            control.clone(),
            shutdown.clone(),
            reopen_tx.clone(),
            Arc::clone(&scope_tokens),
            Arc::clone(&throttles),
            scheduler.clone(),
        );

        // Drive scope_lifecycle in the background. `Created` asks the
        // engine to establish a cursor; the scan loop below notices it
        // and starts polling. `Deleted` cancels and drops the matching
        // scope; `Renamed` is delete+create across ids, and only the
        // establish-if-absent half on an unchanged id.
        // Own the reader before awaiting: dropping the multiplexer aborts a
        // lifecycle send parked behind a busy supervisor as well as its stream.
        let _lifecycle_guard = AbortOnDrop(tokio::spawn(
            LifecycleWorker {
                account: Arc::clone(&account),
                shutdown: shutdown.clone(),
                cursors: Arc::clone(&cursors),
                control: control.clone(),
                tokens: Arc::clone(&scope_tokens),
                reopen: reopen_tx.clone(),
                changes: Arc::clone(&delivery),
                generation: account_generation,
                throttles: Arc::clone(&throttles),
                account_id: account_id.clone(),
            }
            .run(),
        ));

        // Park until shutdown while periodically noticing cursors
        // established by deferred inventory or lifecycle recovery.
        let mut scan = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = scan.tick() => {
                    spawn_missing_scope_polls(
                        account_id.clone(),
                        Arc::clone(&account),
                        Arc::clone(&cursors),
                        config,
                        boundary.clone(),
                        Arc::clone(&delivery),
                        control.clone(),
                        shutdown.clone(),
                        reopen_tx.clone(),
                        Arc::clone(&scope_tokens),
                        Arc::clone(&throttles),
                        scheduler.clone(),
                    );
                }
            }
        }

        // Cancel every tracked scope token so per-scope polls exit
        // cleanly. Tokens are dropped as part of the map clear.
        let drained: Vec<ScopeToken> = {
            let mut g = scope_tokens.lock().expect("poisoned");
            g.drain().map(|(_, t)| t).collect()
        };
        for token in drained {
            token.cancel();
        }
        // `_lifecycle_guard` drops here and aborts the lifecycle reader.
    }

    /// Apply an inventory-fusion `Done`: only the terminal `Done` of
    /// an `EstablishViaInventory` scope carries the cursor that should
    /// be registered. Mid-inventory `Batch` checkpoints are ignored.
    pub fn fuse_inventory_done(
        &self,
        scope: &CursorScope,
        done: Option<Checkpoint>,
    ) -> Result<(), Error> {
        match done {
            Some(Checkpoint::Change(c)) if &c.scope == scope => {
                // This cursor was just minted by Account, not decoded from disk.
                crate::engine_errors::classify_envelope(
                    &c,
                    crate::engine_errors::CursorOrigin::Fresh,
                    bifrost_types::AccountOperation::SyncInventory,
                    scope,
                )
                .map_err(Error::Account)?;
                self.cursors.put(c);
                Ok(())
            }
            Some(Checkpoint::Change(cursor)) => Err(Error::InventoryScopeMismatch {
                expected: scope.clone(),
                actual: cursor.scope,
            }),
            Some(Checkpoint::Backfill(_)) | Some(_) => Err(Error::InventoryCheckpointUnsupported),
            None => Err(Error::InventoryCheckpointMissing),
        }
    }
}

/// Spawn one polling task for a single scope and track its
/// cancellation token in `scope_tokens` so lifecycle events can stop
/// it later.
#[allow(clippy::too_many_arguments)]
fn spawn_and_track_scope_poll(
    account_id: AccountId,
    account: Arc<ArcSwap<crate::types::Incarnation>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    boundary: BoundaryView,
    delivery: Arc<ChangeDelivery>,
    control: SyncControl,
    shutdown: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    scope_tokens: ScopeTokens,
    throttles: Arc<crate::throttle::ThrottleBucket>,
    scheduler: crate::scheduler::Scheduler,
    scope: CursorScope,
) {
    let scope_cancel = cursors
        .scope_cancel(&scope)
        .map(|token| token.child_token())
        .unwrap_or_else(|| shutdown.child_token());
    let generation = ScopeTokenGeneration::mint();
    let drive_generation = cursors.drive_generation(&scope);
    scope_tokens.lock().expect("poisoned").insert(
        scope.clone(),
        ScopeToken {
            generation,
            token: scope_cancel.clone(),
            drive_generation: Some(drive_generation),
        },
    );
    let cleanup_tokens = Arc::clone(&scope_tokens);
    let cleanup_scope = scope.clone();
    tokio::spawn(async move {
        spawn_scope_poll_inner(
            account_id,
            account,
            cursors,
            config,
            boundary,
            delivery,
            control,
            shutdown,
            scope_cancel,
            reopen_tx,
            throttles,
            scheduler,
            scope,
        )
        .await;
        // Parking belongs to the registry. Task cleanup only retires this
        // task's registration, and can never remove a successor's entry.
        retire_scope_token(&cleanup_tokens, &cleanup_scope, generation);
    });
}

/// Drop an exiting poll task's registration, by identity.
///
/// Removing by key alone is what produces the duplicate-poll
/// interleaving described on [`ScopeToken`]: the entry under this scope
/// may already belong to a successor spawned by the 1s scan while this
/// task was on its way out, and evicting it leaves that successor
/// untracked, so the next scan spawns a second live poll for the same
/// scope.
fn retire_scope_token(tokens: &ScopeTokens, scope: &CursorScope, generation: ScopeTokenGeneration) {
    let mut g = tokens.lock().expect("poisoned");
    if matches!(g.get(scope), Some(entry) if entry.generation == generation) {
        g.remove(scope);
    }
}

/// Account replacement also retires retained scopes' old poll tasks, including
/// a task stalled inside the old provider stream. The task's fence is ownership
/// bookkeeping only; terminal state belongs entirely to the registry.
fn retire_stale_scope_token(tokens: &ScopeTokens, cursors: &CursorRegistry, scope: &CursorScope) {
    let current = cursors.drive_generation(scope);
    let mut entries = tokens.lock().expect("poisoned");
    if entries.get(scope).is_some_and(|entry| {
        entry
            .drive_generation
            .is_some_and(|generation| generation != current)
    }) && let Some(entry) = entries.remove(scope)
    {
        entry.cancel();
    }
}

pub(crate) fn cancel_scope_token(tokens: &ScopeTokens, scope: &CursorScope) {
    let current = tokens.lock().expect("poisoned").get(scope).cloned();
    if let Some(entry) = current {
        entry.token.cancel();
        retire_scope_token(tokens, scope, entry.generation);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_missing_scope_polls(
    account_id: AccountId,
    account: Arc<ArcSwap<crate::types::Incarnation>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    boundary: BoundaryView,
    delivery: Arc<ChangeDelivery>,
    control: SyncControl,
    shutdown: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    scope_tokens: ScopeTokens,
    throttles: Arc<crate::throttle::ThrottleBucket>,
    scheduler: crate::scheduler::Scheduler,
) {
    for scope in cursors.all_scopes() {
        retire_stale_scope_token(&scope_tokens, &cursors, &scope);
        if cursors.scope_is_parked(&scope) {
            continue;
        }
        let should_spawn = {
            let mut g = scope_tokens.lock().expect("poisoned");
            match g.get(&scope) {
                Some(token) if !token.is_cancelled() => false,
                Some(_) => {
                    g.remove(&scope);
                    true
                }
                None => true,
            }
        };
        if should_spawn {
            spawn_and_track_scope_poll(
                account_id.clone(),
                Arc::clone(&account),
                Arc::clone(&cursors),
                config,
                boundary.clone(),
                Arc::clone(&delivery),
                control.clone(),
                shutdown.clone(),
                reopen_tx.clone(),
                Arc::clone(&scope_tokens),
                Arc::clone(&throttles),
                scheduler.clone(),
                scope,
            );
        }
    }
}

/// Per-scope poll loop. Drives `changes_stream(cursor)` and sleeps
/// for the scope's adaptive cadence between passes. Stream-end
/// recovery actions are propagated to the engine via `ReopenRequest`
/// with the originating `AccountError`.
#[allow(clippy::too_many_arguments)]
async fn spawn_scope_poll_inner(
    account_id: AccountId,
    account: Arc<ArcSwap<crate::types::Incarnation>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    mut boundary: BoundaryView,
    delivery: Arc<ChangeDelivery>,
    control: SyncControl,
    shutdown: CancellationToken,
    scope_cancel: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    throttles: Arc<crate::throttle::ThrottleBucket>,
    scheduler: crate::scheduler::Scheduler,
    scope: CursorScope,
) -> PollExit {
    let mut cadence = AdaptiveCadence {
        interval: config.poll_initial,
        no_change_streak: 0,
    };
    loop {
        if shutdown.is_cancelled() || scope_cancel.is_cancelled() {
            return PollExit::Retire;
        }
        // If the boundary is asking us to pause, park here until it
        // changes back to Run (or Stop / Shutdown trips).
        if matches!(boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
            // Park until the request changes.
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return PollExit::Retire,
                    () = scope_cancel.cancelled() => return PollExit::Retire,
                    next = boundary.changed() => {
                        match next {
                            None => return PollExit::Retire,
                            Some(crate::cancel::BoundaryRequest::Pause) => continue,
                            Some(_) => break,
                        }
                    }
                }
            }
        }
        tokio::select! {
            () = scope_cancel.cancelled() => return PollExit::Retire,
            running = control.wait_until_running(&shutdown) => {
                if !running { return PollExit::Retire; }
            }
        }
        // The drive's OWN boundary arm answers `Stop` and `Pause`, which is
        // what a detach publishes. This outer select answers the two signals
        // the boundary cannot carry: scope deletion or restart, which cancels
        // `scope_cancel` while the ACCOUNT boundary stays `Run`, and root
        // cancellation, which lands before this account's own detach writes
        // `Stop`. The generation fence stops the stale publication but does not
        // release the lease, so without a cancellation arm a stalled drive
        // keeps it and the replacement incarnation waits behind it on
        // `claim_drive` indefinitely. The registry's own per-scope token inside
        // `with_drive` is the general answer to that (it covers the restart
        // paths and the reconciler, neither of which has an entry in
        // `ScopeTokens`); this arm remains because it also retires the poll
        // task itself, which the registry knows nothing about.
        //
        // Before the stream opens, cancellation drops lease/admission waits.
        // During the drive it cuts only at the stream poll: a select cannot
        // destroy a branch mid-poll, so synchronous page registration and
        // broadcast cannot be split. Dropping the future releases the lease,
        // scheduler permit, activity guard and stream.
        let driven = tokio::select! {
            () = shutdown.cancelled() => return PollExit::Retire,
            () = scope_cancel.cancelled() => return PollExit::Retire,
            driven = cursors
            .with_drive(&scope, |cursor, registry_generation| {
                let pre_advance_state = cursor.server_state.bytes.clone();
                let account = Arc::clone(&account);
                let scheduler = scheduler.clone();
                let shutdown = shutdown.clone();
                let drive_cursors = Arc::clone(&cursors);
                let advance_cursors = Arc::clone(&cursors);
                let advance_scope = scope.clone();
                let scope = scope.clone();
                let delivery = Arc::clone(&delivery);
                let boundary = boundary.clone();
                let control = control.clone();
                let throttles = Arc::clone(&throttles);
                let account_id = account_id.clone();
                async move {
                    let gate = crate::engine::drive_start::DriveStart {
                        current: &account, control: &control, scheduler: &scheduler,
                        throttles: &throttles, account_id: &account_id, shutdown: &shutdown,
                    };
                    let admitted = match gate.admit_drive(
                        bifrost_types::ThrottleWork::Scope(&scope),
                        crate::scheduler::WorkKind::Sync,
                    ).await {
                        Ok(admitted) => admitted,
                        Err(refusal) => return Err(refusal),
                    };
                    // The registry checked its generation before invoking us.
                    // Admission can wait, so check it again before the wire.
                    if drive_cursors.drive_generation(&scope) != registry_generation {
                        return Err(crate::engine::drive_start::StartRefused::Paused);
                    }
                    let acc_arc = &admitted.incarnation;
                    let acc: &dyn Account = acc_arc.as_ref().as_ref();
                    let outcome = drive_changes_stream(
                        acc,
                        scope,
                        cursor,
                        drive_cursors,
                        delivery,
                        boundary,
                        Some(control),
                        Some(registry_generation),
                    )
                    .await;
                    // Read the post-drive cursor while the lease is still
                    // held. The lease no longer covers the tail of the poll
                    // iteration, so a push reconcile can advance this scope
                    // the instant we return; comparing out there would credit
                    // this drive with the reconciler's progress and hold the
                    // cadence at `poll_min` on a scope this loop never moved.
                    let advanced = advance_cursors
                        .snapshot(&advance_scope)
                        .is_some_and(|c| c.server_state.bytes != pre_advance_state);
                    Ok((advanced, outcome, acc_arc.generation(), registry_generation))
                }
            }) => driven,
        };
        let Some(driven) = driven else {
            // The scope left the registry - either before the drive started or
            // while it ran, `with_drive` reporting both the same way. Exit
            // cleanly; if the scope is re-established the 1s scan respawns us.
            return PollExit::Retire;
        };
        let (advanced, outcome, generation, drive_generation) = match driven {
            Ok(driven) => driven,
            Err(crate::engine::drive_start::StartRefused::ShuttingDown) => return PollExit::Retire,
            Err(crate::engine::drive_start::StartRefused::Paused) => continue,
            Err(crate::engine::drive_start::StartRefused::Held { until, .. }) => {
                tokio::select! {
                    () = shutdown.cancelled() => return PollExit::Retire,
                    () = scope_cancel.cancelled() => return PollExit::Retire,
                    () = tokio::time::sleep_until(until) => {}
                }
                continue;
            }
        };
        let recovered = handle_drive_outcome(
            &scope,
            outcome,
            advanced,
            generation,
            &reopen_tx,
            &account_id,
            &throttles,
            &shutdown,
            &scope_cancel,
        )
        .await;

        cadence = Multiplexer::updated_cadence(
            cadence,
            recovered.advanced,
            config.poll_min,
            config.poll_max,
        );
        if let Some(exit) = recovered.exit {
            if exit == PollExit::Park {
                cursors.park_scope_for_drive(&scope, drive_generation);
            }
            return exit;
        }
        if matches!(
            boundary.peek(),
            crate::cancel::BoundaryRequest::CheckpointNow
        ) {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return PollExit::Retire,
                    () = scope_cancel.cancelled() => return PollExit::Retire,
                    next = boundary.changed() => {
                        match next {
                            None | Some(crate::cancel::BoundaryRequest::Stop) => {
                                return PollExit::Retire;
                            }
                            Some(crate::cancel::BoundaryRequest::CheckpointNow) => continue,
                            Some(crate::cancel::BoundaryRequest::Pause) => break,
                            Some(crate::cancel::BoundaryRequest::Run) => break,
                        }
                    }
                }
            }
            if matches!(boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
                continue;
            }
        }
        tokio::select! {
            () = shutdown.cancelled() => return PollExit::Retire,
            () = scope_cancel.cancelled() => return PollExit::Retire,
            () = tokio::time::sleep(cadence.interval) => {}
        }
    }
}

/// Aborts its task when dropped, unlike `JoinHandle`, which detaches it.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Whether the poll loop retired normally or reached a terminal verdict.
/// The loop installs a terminal verdict in the registry under the failing
/// drive's generation before returning. Task ownership always retires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollExit {
    Retire,
    Park,
}

struct DriveRecovery {
    advanced: bool,
    exit: Option<PollExit>,
}

/// A recovery delay cut short by detach or scope deletion. The scope is left
/// RETIRED, never parked: shutdown and deletion do not issue terminal verdicts.
fn retired() -> DriveRecovery {
    DriveRecovery {
        advanced: false,
        exit: Some(PollExit::Retire),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_drive_outcome(
    scope: &CursorScope,
    outcome: Result<ChangesEvent, Error>,
    // `advanced`: whether this drive moved the scope's cursor, measured by
    // the caller while it still held the drive lease. Deliberately not
    // re-read here - the lease is already released by the time this runs, so
    // a concurrent push reconcile could be credited to this drive.
    advanced: bool,
    // The generation of the incarnation the drive ran against, stamped on any
    // engine directive so the listener declines it once that account is gone.
    generation: crate::types::AccountGeneration,
    reopen_tx: &mpsc::Sender<ReopenRequest>,
    account_id: &AccountId,
    throttles: &crate::throttle::ThrottleBucket,
    // Detach and scope deletion must interrupt a recovery delay, not wait it
    // out. A provider `Retry-After` of minutes otherwise kept a deleted scope
    // or a detaching account parked for the full hint, and detach then burned
    // toward `detach_timeout` and aborted the task instead of draining it.
    shutdown: &CancellationToken,
    scope_cancel: &CancellationToken,
) -> DriveRecovery {
    // Account errors already carry the complete recovery verdict. Normalize
    // them onto the same path as an account-authored Terminated event instead
    // of letting the generic engine-error arm silently choose "retry".
    let outcome = match outcome {
        Err(Error::Account(error)) => Ok(ChangesEvent::Terminated(error)),
        other => other,
    };
    match outcome {
        Ok(ChangesEvent::Advanced | ChangesEvent::Done) => DriveRecovery {
            advanced,
            exit: None,
        },
        Ok(ChangesEvent::Stopped) => DriveRecovery {
            advanced: false,
            exit: Some(PollExit::Retire),
        },
        // Pause is a temporary park; the poll loop's boundary check picks it
        // up at the top of the next iteration.
        Ok(ChangesEvent::Paused) => DriveRecovery {
            advanced,
            exit: None,
        },
        Ok(ChangesEvent::Terminated(error)) => {
            use crate::recovery::{RecoveryPlan, retry_delay};
            // Dispatch via `dispatch_stream_failure` so the closed `RecoveryPlan`
            // drives the branch. Retry and reconcile keep the poll
            // loop alive; engine directives hand off to the reopen
            // listener carrying the original account error; terminal
            // errors stop the loop (the broadcast already carried the
            // terminating event). The account error needs to ride
            // along on the engine directive arm; we clone it before
            // planning.
            let original = error.clone();
            match crate::recovery::dispatch_stream_failure(error, throttles, account_id) {
                RecoveryPlan::Retry(advice) => {
                    // Share any provider-documented throttle deadline
                    // with sibling scopes (and, via tenant/provider
                    // keys, sibling accounts) before sleeping it off
                    // locally.
                    let delay = retry_delay(
                        &advice,
                        std::time::SystemTime::now(),
                        crate::recovery::HINTLESS_RETRY_WAIT,
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => return retired(),
                        () = scope_cancel.cancelled() => return retired(),
                        () = tokio::time::sleep(delay) => {}
                    }
                    DriveRecovery {
                        advanced: false,
                        exit: None,
                    }
                }
                RecoveryPlan::Reconcile(advice) => {
                    // A read stream's reconcile collapses to "rerun
                    // this scope soon". Sleep briefly so we do not
                    // hot-spin, then re-enter the poll loop. The
                    // mutation-side `Reconcile` actions (CheckTarget /
                    // DedupeByClientId) drive through the mutation
                    // pipeline; the change-stream side does not have a
                    // per-item lane to dedupe.
                    let delay = crate::recovery::reconcile_delay(
                        &advice,
                        std::time::SystemTime::now(),
                        crate::recovery::HINTLESS_RETRY_WAIT,
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => return retired(),
                        () = scope_cancel.cancelled() => return retired(),
                        () = tokio::time::sleep(delay) => {}
                    }
                    DriveRecovery {
                        advanced: false,
                        exit: None,
                    }
                }
                RecoveryPlan::Engine(directive) => {
                    let directive_scope = directive.target_scope();
                    let account_wide = directive_scope.is_none();
                    let request = ReopenRequest::Recovery {
                        scope: directive_scope,
                        error: original,
                        generation: Some(generation),
                    };
                    // Nothing is held here: admission, lease and activity
                    // ended inside the caller's lease-protected drive. Per-scope poll tasks are not detach workers -
                    // nobody holds their handles - so a bare send against a
                    // full channel behind a busy listener (which keeps its
                    // receiver while inside a repair) would outlive detach
                    // until the listener died. Shutdown wins and the owed
                    // directive is dropped; the reopen channel has no
                    // shutdown drain. The scope token is deliberately NOT
                    // selected: the directive may be account-wide, and a
                    // deleted scope does not revoke it.
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => return retired(),
                        _ = reopen_tx.send(request) => {}
                    }
                    if account_wide {
                        // Match the reconciler's bounded handoff: avoid driving
                        // the old account while its directive is being handled.
                        tokio::select! {
                            () = shutdown.cancelled() => return retired(),
                            () = scope_cancel.cancelled() => return retired(),
                            () = tokio::time::sleep(crate::recovery::REOPEN_WAIT) => {}
                        }
                    }
                    DriveRecovery {
                        advanced: false,
                        exit: None,
                    }
                }
                RecoveryPlan::Terminal(fatal) => {
                    let err = fatal.as_ref();
                    let view = err.telemetry_fields();
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        scope = ?scope,
                        kind = view.kind_discriminant,
                        message_key = view.message_key,
                        recovery = view.recovery_discriminant,
                        operation = ?view.operation,
                        "changes stream terminated with terminal recovery"
                    );
                    // Park, not retire: retiring the token would let the 1s
                    // scan respawn this poll against a cursor whose provider
                    // just said "nothing left to try", turning a terminal
                    // error into a once-per-second re-drive that
                    // re-broadcasts `Terminated` forever.
                    DriveRecovery {
                        advanced: false,
                        exit: Some(PollExit::Park),
                    }
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                error = %err,
                "drive_changes_stream errored"
            );
            DriveRecovery {
                advanced: false,
                exit: None,
            }
        }
    }
}

/// Take a provider-deleted scope out of service in memory: revoke its
/// obligations (lifecycle epoch), revoke its fence-guarded producers (scope
/// fence), and only THEN drop its registry entry, if any. The order is the
/// point: a fusion finalizing before the fence raise put a cursor the delete
/// below removes; one finalizing after it fails `put_if`. The epoch moves before
/// the fence so a caller that reads the fence and then checks the epoch either
/// sees the deletion in the epoch or holds a fence the deletion has since moved.
/// Both deletion front ends (the multiplexer and the reopen listener) call this,
/// so the sequence is written in exactly one place.
pub(crate) fn retire_deleted_scope(
    cursors: &CursorRegistry,
    control: &SyncControl,
    scope: &CursorScope,
) {
    cursors.bump_lifecycle_epoch(scope);
    control.raise_scope_fence(scope);
    cursors.delete(scope);
}

#[cfg(test)]
mod tests;
