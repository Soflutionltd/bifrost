//! Engine-side recovery dispatch helpers.
//!
//! Protocol crates derive a `RecoveryClass` on every `AccountError`
//! they produce. The engine's job is to dispatch the verdict, not to
//! reclassify. These helpers exist for the small amount of plumbing the
//! engine reuses across multiple dispatch sites:
//!
//! - `plan_recovery` collapses a derived `RecoveryClass` into a closed
//!   four-arm `RecoveryPlan` so the engine never variant-matches the
//!   open `RecoveryClass` enum directly. Adding a future variant fails
//!   to compile here, not silently routes to terminal.
//! - `retry_delay` resolves a `RetryAdvice` to a concrete `Duration`
//!   using the cause's [`RetryHint`] when present.
//! - `directive_target_scope` resolves the `CursorScope` an
//!   [`EngineDirective`] targets, when any. Its required catch-all
//!   defaults an unknown future variant to account-wide until a human
//!   adds the appropriate scope-bearing arm.
//! - `restart_scope_error` constructs the single account-error shape
//!   the engine itself emits when synthesizing a `RestartScope`
//!   directive (scope lifecycle Created / Renamed, or fresh
//!   establishment via the reopen listener).
//! - `cursor_decode_failure` translates a cursor envelope schema
//!   mismatch into an `AccountError` whose recovery derives to
//!   `Engine(SchemaIncompatible)`.
//! - [`ThrottleBucket`] holds engine-scope throttle waits keyed by
//!   [`ThrottleKey`]. `ThrottleScope::CurrentOperation` never enters
//!   the bucket; the caller delays the single work item inline.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Throttle deadlines live on the SAME clock as the timers that retire them.
///
/// Every engine wait site parks a throttle deadline as a
/// `tokio::time::sleep`, so a bucket holding `SystemTime` deadlines is read on
/// a different clock from the timer that pays them off. The two agree only
/// where tokio's clock is the system one. Under `start_paused` they do not:
/// the sleep returns with wall time unmoved, the wait site re-derives the
/// full remaining wait, and the re-check loop spins forever without ever
/// making progress - which is exactly what blocked a hermetic test of the
/// deferral. `tokio::time::Instant::now()` falls back to the system clock
/// when no tokio clock is in force, so production behaviour is unchanged.
/// Same move `bifrost-smtp`'s `AsyncDeadline` and `ByteBucket` made, for the
/// same reason.
///
/// Nothing here is durable: the bucket is one in-memory `Arc<Mutex<_>>` per
/// `SyncEngine`, never serialized, never checkpointed, and dropped with the
/// engine. A monotonic instant is therefore safe - it never has to survive a
/// process. [`record_throttle_parts`] resolves a provider `RetryHint` to a
/// remaining duration and immediately anchors it to `Instant::now()`.
/// Recovery repairs also retain a local deadline: [`record_repair_throttle`]
/// resolves an absolute hint once, anchors the local deadline, and passes an
/// equivalent duration hint to the shared recorder.
use tokio::time::Instant;

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountId, AccountOperation, Cause,
    CursorScope, EngineDirective, ErrorScope, Fatal, ReconcileAdvice, RecoveryClass, RetryAdvice,
    StateCause, SyncStateErrorKind, ThrottleKey, ThrottleScope,
};

#[cfg(test)]
use bifrost_types::MailboxId;

/// Closed four-arm plan derived from a `RecoveryClass`. Every engine
/// dispatch site routes through [`plan_recovery`] so an added
/// `RecoveryClass` variant fails to compile here rather than silently
/// landing in the terminal arm.
#[derive(Debug, Clone)]
pub(crate) enum RecoveryPlan {
    Retry(RetryAdvice),
    Reconcile(ReconcileAdvice),
    Engine(EngineDirective),
    Terminal(Fatal),
}

/// Collapse an `AccountError`'s recovery into a [`RecoveryPlan`]. The
/// helper consumes the error so the terminal arm owns the value handed
/// to [`Fatal::try_from`]; engine sites that need to retain a copy for
/// telemetry should clone before calling.
///
/// Branching uses the four mutually-exclusive helpers on
/// `RecoveryClass` (`is_retryable`, `requires_reconciliation`,
/// `requires_engine_action`, `is_terminal`). The wildcard at the end
/// is unreachable because the helpers cover every variant; we panic
/// rather than silently choose a default if a future helper bug
/// breaks the invariant.
pub(crate) fn plan_recovery(error: AccountError) -> RecoveryPlan {
    let recovery = error.recovery();
    if recovery.is_retryable() {
        match recovery.clone() {
            RecoveryClass::Retry(advice) => RecoveryPlan::Retry(advice),
            other => unreachable!("is_retryable lied about {other:?}"),
        }
    } else if recovery.requires_reconciliation() {
        match recovery.clone() {
            RecoveryClass::Reconcile(advice) => RecoveryPlan::Reconcile(advice),
            other => unreachable!("requires_reconciliation lied about {other:?}"),
        }
    } else if recovery.requires_engine_action() {
        match recovery.clone() {
            RecoveryClass::Engine(directive) => RecoveryPlan::Engine(directive),
            other => unreachable!("requires_engine_action lied about {other:?}"),
        }
    } else {
        // Terminal. `Fatal::try_from` rejects non-terminal errors;
        // because `is_terminal` returned true here, the conversion
        // always succeeds.
        let fatal = Fatal::try_from(error)
            .expect("is_terminal returned true but Fatal::try_from rejected the error");
        RecoveryPlan::Terminal(fatal)
    }
}

/// Resolve the effective sleep an engine path should observe before
/// retrying. `now` is the caller's notion of the current wall clock;
/// `fallback` is the duration used when the advice carries no hint.
#[must_use]
pub(crate) fn retry_delay(advice: &RetryAdvice, now: SystemTime, fallback: Duration) -> Duration {
    advice
        .retry_hint
        .map_or(fallback, |hint| hint.min_delay(now))
}

/// The reconcile counterpart of [`retry_delay`]. A throttle that lands
/// mid-flight on a non-idempotent operation classifies as
/// `Reconcile(ThrottledMidFlight)` rather than `Retry`, and it carries
/// the provider's retry hint; honoring it keeps the read-back probe
/// from re-entering the same rate limit it just hit.
#[must_use]
pub(crate) fn reconcile_delay(
    advice: &ReconcileAdvice,
    now: SystemTime,
    fallback: Duration,
) -> Duration {
    advice
        .retry_hint
        .map_or(fallback, |hint| hint.min_delay(now))
}

/// True when `directive` targets a single cursor scope. Used by the
/// multiplexer to populate `ReopenRequest::Recovery::scope`.
///
/// `EngineDirective` is `#[non_exhaustive]` from `bifrost-types`, so the
/// `_ =>` arm is required for compilation. New variants land here as
/// account-wide by default; review this match when extending
/// `EngineDirective` to decide whether a new variant deserves a scope.
#[must_use]
pub(crate) fn directive_target_scope(directive: &EngineDirective) -> Option<CursorScope> {
    match directive {
        EngineDirective::RestartScope(scope)
        | EngineDirective::DowngradeCapabilityForScope(scope)
        | EngineDirective::DisableScope(scope) => Some(scope.clone()),
        EngineDirective::DowngradeStrategy(_)
        | EngineDirective::RestartAccount
        | EngineDirective::SchemaIncompatible
        | EngineDirective::OperatorOverrideRequired { .. } => None,
        // `EngineDirective` is `#[non_exhaustive]` from `bifrost-types`, so
        // this wildcard is REQUIRED for compilation across the crate
        // boundary even though every current variant is named above. A new
        // scope-bearing variant still defaults account-wide here until a
        // human adds its arm.
        //
        // Ruled sufficient rather than merely tolerated, and the reasons are
        // why no further machinery is owed here: the account-wide default is
        // the CONSERVATIVE direction (it is never narrower than the directive
        // asked for, so a new variant degrades to doing too much rather than
        // too little); `DirectiveKey::Other` bounds how coarse the dedupe can
        // get to the pre-existing behaviour; and `handle_engine_directive`
        // carries a required fallback that LOGS an unhandled variant, so a
        // new one is visible in operation rather than silent. Making this a
        // compile error instead would mean dropping `#[non_exhaustive]` from
        // a public, re-exported `bifrost-types` enum - an API-stability
        // change far larger than the footgun it would close.
        _ => None,
    }
}

/// Build a sync-state cursor-invalid error scoped to a single cursor.
/// Used by scope lifecycle (`Created`, `Renamed`) and the reopen
/// listener when the engine needs to ask itself for a fresh
/// establishment of one scope. The derived recovery is
/// `Engine(RestartScope(scope))`.
#[must_use]
pub(crate) fn restart_scope_error(scope: CursorScope, operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .scope(ErrorScope::Cursor(scope))
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Synthesize an `AccountError` for an engine-level re-establishment
/// failure that has no `AccountError` of its own (`EstablishCursorFailed`,
/// `CheckpointStore`, and the other non-`Account` `engine::Error`
/// variants surfaced from `run_establish`). Without this, a scope that
/// exhausts its reopen budget on purely engine-level errors would
/// broadcast no `SyncEvent::Terminated` - only the operator warning -
/// violating the documented "after three failures broadcast
/// `Terminated(last_error)`" contract. Classified as
/// `SyncState(CursorInvalid)` (the engine could not establish or persist
/// the scope's cursor); the carried scope lets consumers route the
/// termination to the affected scope.
#[must_use]
pub(crate) fn establish_failure_error(
    scope: CursorScope,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .scope(ErrorScope::Cursor(scope))
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Translate a cursor envelope schema mismatch into an `AccountError`
/// whose recovery derives to `Engine(SchemaIncompatible)`. Producers
/// (specifically the cursor envelope decoder) hit this path when the
/// on-disk schema is below `MIN_MIGRATABLE`. The original engine
/// `Error::SchemaIncompatible` becomes a derived account error so the
/// engine's recovery dispatch can drive the schema-clear loop.
#[must_use]
pub(crate) fn cursor_decode_failure(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Classify an account's violation of the batch boundary contract: a
/// `PageBoundary::Partial` batch that nonetheless carries a checkpoint.
///
/// This MUST be a classified `AccountError` and not a bare engine error.
/// `Batch`'s fields are public, so the type system cannot stop a protocol
/// crate from emitting the shape, and an engine that merely logged it would
/// re-poll the same unchanged cursor at the scope's cadence forever: no
/// backoff past `poll_max`, no recovery dispatch, no `Terminated` event, and
/// a consumer that sees a scope which has silently stopped making progress
/// with nothing in the stream to say so.
///
/// It is `Internal(AccountContract)`, not a provider fault: the boundary and
/// the checkpoint are minted by the `Account` implementation, which is client
/// code - bifrost's or a consumer's - so "contact provider support" would send
/// the operator to the one party that did nothing wrong. It derives the
/// terminal `InternalFailure`, so `plan_recovery` stops the scope - the honest
/// outcome, because an implementation emitting a nonsense boundary does not
/// heal by being asked again.
///
/// The protocol tag is read off the offending checkpoint. There is no
/// protocol accessor on `dyn Account`, so where the checkpoint carries no
/// tag of its own the error says `Protocol::Unknown` rather than guessing.
#[must_use]
pub(crate) fn batch_boundary_violation(
    violation: bifrost_types::BatchBoundaryError,
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
) -> AccountError {
    contract_violation(checkpoint, operation, scope, violation.detail())
}

/// A live batch carrying a BACKFILL checkpoint. The same classification and the
/// same terminal outcome, with its own words: the boundary was fine, the
/// checkpoint belongs to a lane this stream does not publish on, and an operator
/// reading "a PageBoundary::Partial batch carried a checkpoint" would go looking
/// for the wrong thing.
#[must_use]
pub(crate) fn backfill_checkpoint_on_changes(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
) -> AccountError {
    contract_violation(
        checkpoint,
        operation,
        scope,
        "a changes batch carried a backfill checkpoint",
    )
}

fn contract_violation(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
    detail: &'static str,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract),
        Cause::Internal(bifrost_types::InternalCause::new(
            bifrost_types::InternalErrorKind::AccountContract,
            Some(bifrost_types::DiagnosticText::support_only(detail)),
        )),
    )
    // The protocol the offending implementation speaks, for telemetry: it
    // used to ride inside the wire cause this replaced.
    .protocol(checkpoint_protocol(checkpoint))
    .operation(operation)
    .scope(ErrorScope::Cursor(scope.clone()))
    .try_build()
    .expect("valid account error classification")
}

fn checkpoint_protocol(checkpoint: Option<&bifrost_types::Checkpoint>) -> bifrost_types::Protocol {
    use bifrost_types::{Checkpoint, Protocol, ProtocolKind};
    let Some(Checkpoint::Change(cursor)) = checkpoint else {
        // A backfill checkpoint carries no protocol tag at all.
        return Protocol::Unknown;
    };
    match cursor.server_state.protocol {
        ProtocolKind::Jmap => Protocol::Jmap,
        ProtocolKind::Imap => Protocol::Imap,
        ProtocolKind::Gmail => Protocol::Gmail,
        ProtocolKind::CardDav => Protocol::CardDav,
        ProtocolKind::CalDav => Protocol::CalDav,
        ProtocolKind::Graph => Protocol::Graph,
        // `ProtocolKind` is `#[non_exhaustive]`; a variant added upstream is
        // unmappable here, and naming the wrong protocol is worse than
        // declining to name one.
        _ => Protocol::Unknown,
    }
}

/// Engine-scope throttle bucket. Records "do not drive work for this
/// key before `wait_until`" deadlines and answers "how long should I
/// pause work that maps to this key now?".
///
/// One bucket per `SyncEngine`, shared by every attached account.
/// `Budget` keys cross account boundaries by design: a key names a
/// provider budget, and every account whose work is charged to it pauses
/// on it. `Mailbox` and `Account` keys are per-account.
///
/// A budget reaches an account by DECLARATION: a wait site asks
/// `Account::throttle_keys` what its work consumes and looks each returned
/// key up literally ([`throttle_wait`]), so an account that has never
/// failed still sees the first deadline a sibling records.
///
/// Deadlines are `tokio::time::Instant` - see the clock note at the top of
/// this module. They are in-memory only; nothing persists a throttle wait.
#[derive(Debug, Default)]
pub struct ThrottleBucket {
    waits: HashMap<ThrottleKey, Instant>,
}

impl ThrottleBucket {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a wait-until deadline for `key`. If a wait already
    /// exists, the later of the two deadlines wins (a stricter throttle
    /// from a parallel response should not shorten an earlier one).
    pub fn record(&mut self, key: ThrottleKey, wait_until: Instant) {
        self.waits
            .entry(key)
            .and_modify(|existing| {
                if wait_until > *existing {
                    *existing = wait_until;
                }
            })
            .or_insert(wait_until);
    }

    /// Record a deadline observed by `account`. Every key the engine
    /// records either embeds its account (`Account`, `Mailbox`) or is
    /// declared by the accounts that consume it (`Budget`), so the
    /// observing account needs no enrollment and this is `record`.
    pub fn record_for(&mut self, _account: &AccountId, key: ThrottleKey, wait_until: Instant) {
        self.record(key, wait_until);
    }

    /// Return the remaining wait for `key` at `now`. `None` means no
    /// throttle applies (or the recorded deadline has expired).
    #[must_use]
    pub fn wait_for(&self, key: &ThrottleKey, now: Instant) -> Option<Duration> {
        let until = *self.waits.get(key)?;
        until.checked_duration_since(now).filter(|d| !d.is_zero())
    }

    /// Longest remaining wait that applies to `account` as a whole at
    /// `now`: the account's own `Account` key. `Mailbox` and `Budget` keys
    /// are deliberately excluded - they hold one mailbox's work, never the
    /// whole account, and reach the work they hold through the keys the
    /// account declares for it ([`throttle_wait`]).
    #[must_use]
    pub fn wait_for_account(&self, account: &AccountId, now: Instant) -> Option<Duration> {
        self.wait_for(&ThrottleKey::Account(account.clone()), now)
    }

    /// Drop expired entries. Called opportunistically by the engine
    /// (e.g. between work items) to keep the map small.
    pub fn cleanup_expired(&mut self, now: Instant) {
        self.waits.retain(|_, until| *until > now);
    }

    /// Called at detach. The bucket holds no per-account state beyond
    /// deadlines, so there is nothing to forget: deadlines under the
    /// account's own key are left to expire on their own - they are
    /// per-account facts a reattach of the same account may still want to
    /// honor.
    pub fn forget_account(&mut self, _account: &AccountId) {}
}

/// Resolve the `ThrottleKey` a `ThrottleScope` maps to, given the
/// identities the classified error actually carries. Mailbox scope never
/// falls back to the account key: an account key is wider than the
/// producer's throttle.
///
/// - `CurrentOperation` never enters the bucket (per-call hint).
/// - `Mailbox` uses the error's mailbox identity in EITHER shape it is
///   produced in - `ErrorScope::Mailbox { id }` or
///   `ErrorScope::Cursor(CursorScope::Folder(id))`, the latter being what
///   every production folder producer actually builds. An error that names
///   no mailbox at all remains local to the current operation.
/// - `Account` uses the account key.
///
/// A throttle that spans accounts is a stamped and declared budget, never a
/// scope. An error that stamps budgets never reaches here:
/// `record_throttle_parts` records exactly the stamped budgets instead.
///
/// The mailbox rule is deliberate. Widening a per-mailbox 429 to the account
/// key converts one mailbox's
/// throttle into an account-wide stall, so a mailbox scope whose error names no
/// mailbox stays operation-local instead. Do not "complete" the match by
/// giving it an account fallback.
///
/// `Account`-keyed deadlines also deliberately survive a detach until they
/// expire. They describe the stable account IDENTITY, not the connection
/// incarnation, so clearing them on detach would let a detach-plus-reattach
/// bypass a provider wait. Growth is bounded by expiry pruning.
#[must_use]
pub(crate) fn resolve_throttle_key(
    scope: ThrottleScope,
    account: &AccountId,
    error: &AccountError,
) -> Option<ThrottleKey> {
    let account_key = || ThrottleKey::Account(account.clone());
    match scope {
        ThrottleScope::CurrentOperation => None,
        // BOTH mailbox shapes must be accepted here. Folder producers build
        // `ErrorScope::Cursor(Folder(id))`, not `ErrorScope::Mailbox { id }` -
        // and IMAP's `mailbox_throttle`, the only production emitter of
        // `ThrottleScope::Mailbox` in the workspace, fires for either. Matching
        // only the latter made `ThrottleKey::Mailbox` unreachable in
        // production: every real per-folder `[LIMIT]` recorded no deadline at
        // all, so the poll loop, push reconciler and backfill runner kept
        // driving that folder at full cadence and re-earned the 429. The
        // synthetic `Mailbox { .. }` shape is built only by tests.
        ThrottleScope::Mailbox => match error.scope() {
            Some(ErrorScope::Mailbox { id }) => Some(ThrottleKey::Mailbox {
                account: account.clone(),
                mailbox: id.clone(),
            }),
            Some(ErrorScope::Cursor(CursorScope::Folder(folder))) => Some(ThrottleKey::Mailbox {
                account: account.clone(),
                mailbox: bifrost_types::MailboxId(folder.0.clone()),
            }),
            _ => None,
        },
        ThrottleScope::Account => Some(account_key()),
        // ThrottleScope is #[non_exhaustive]; new variants default to
        // "no bucket entry" (the conservative classification) and
        // require explicit handling here.
        _ => None,
    }
}

/// Record a `RetryAdvice`'s throttle deadline on the shared bucket,
/// resolving the key from the error's own identities. No-ops when the
/// advice carries no throttle scope or no retry hint, and when the
/// bucket mutex is poisoned (a throttle wait is advisory; panicking a
/// worker over it would trade a pause for an outage).
pub(crate) fn record_throttle(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    advice: &RetryAdvice,
    error: &AccountError,
) {
    record_throttle_parts(
        bucket,
        account,
        advice.throttle_scope,
        advice.retry_hint,
        error,
    );
}

pub(crate) fn record_reconcile_throttle(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    advice: &ReconcileAdvice,
    error: &AccountError,
) {
    record_throttle_parts(
        bucket,
        account,
        advice.throttle_scope,
        advice.retry_hint,
        error,
    );
}

/// Record a recovery establishment failure and capture its local hold once.
/// Resolve absolute hints before recording so the local and shared waits use
/// the same duration, even under a paused monotonic clock.
pub(crate) fn record_repair_throttle(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    error: &AccountError,
) -> Option<Instant> {
    let now = Instant::now();
    match plan_recovery(error.clone()) {
        RecoveryPlan::Retry(mut advice) => {
            let signal = advice.throttle_scope.is_some()
                || advice.retry_hint.is_some()
                || !error.throttle_budgets().is_empty()
                || matches!(advice.reason, bifrost_types::RetryReason::RateLimited);
            if !signal {
                return None;
            }
            let delay = retry_delay(&advice, SystemTime::now(), Duration::from_secs(1));
            advice.retry_hint = advice
                .retry_hint
                .map(|_| bifrost_types::RetryHint::After(delay));
            record_throttle(bucket, account, &advice, error);
            Some(now + delay)
        }
        RecoveryPlan::Reconcile(mut advice) => {
            let signal = advice.throttle_scope.is_some()
                || advice.retry_hint.is_some()
                || !error.throttle_budgets().is_empty()
                || matches!(
                    advice.reason,
                    bifrost_types::ReconcileReason::ThrottledMidFlight
                );
            if !signal {
                return None;
            }
            let delay = reconcile_delay(&advice, SystemTime::now(), Duration::from_secs(1));
            advice.retry_hint = advice
                .retry_hint
                .map(|_| bifrost_types::RetryHint::After(delay));
            record_reconcile_throttle(bucket, account, &advice, error);
            Some(now + delay)
        }
        _ => None,
    }
}

/// The hold recorded for a STAMPED budget whose response carried no retry
/// hint. Equal to the fallback every engine retry path sleeps locally
/// (`retry_delay` / `reconcile_delay` callers pass one second), so a sibling
/// declaring the budget is paced exactly as the observer paces itself. An
/// unstamped error without a hint still records nothing, as it always has:
/// there the key is lifted from a coarse `ThrottleScope`, and a guessed
/// deadline on an account or provider key would pause far more than one
/// request's worth of work.
const STAMPED_BUDGET_FALLBACK: Duration = Duration::from_secs(1);

fn record_throttle_parts(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    throttle_scope: Option<ThrottleScope>,
    retry_hint: Option<bifrost_types::RetryHint>,
    error: &AccountError,
) {
    // Precedence: a producer that stamped budgets has named exactly what the
    // response says to hold, so those are recorded and NOTHING is lifted from
    // the `ThrottleScope`. Recording the lifted account key as well would put
    // the hold back on every sibling mailbox the budget exists to keep
    // running.
    let budgets = error.throttle_budgets();
    if !budgets.is_empty() {
        let delay = retry_hint.map_or(STAMPED_BUDGET_FALLBACK, |hint| {
            hint.min_delay(SystemTime::now())
        });
        let wait_until = Instant::now() + delay;
        if let Ok(mut guard) = bucket.lock() {
            for budget in budgets {
                guard.record_for(account, ThrottleKey::Budget(budget.clone()), wait_until);
            }
        }
        return;
    }
    let Some(scope) = throttle_scope else {
        return;
    };
    let Some(key) = resolve_throttle_key(scope, account, error) else {
        return;
    };
    // A hintless lifted throttle records nothing, EXCEPT on a `Mailbox`
    // key: that key is as narrow as a stamped budget, so a guessed
    // fallback hold there costs one mailbox's work the same second the
    // observer sleeps anyway. IMAP's folder-scoped `[LIMIT]` carries no
    // hint at all, so without this its mailbox hold was never recorded and
    // the keys IMAP declares never saw it. A wider key (account, provider)
    // still needs the provider's own timing before it pauses anything.
    let hint = match retry_hint {
        Some(hint) => hint,
        None if matches!(key, ThrottleKey::Mailbox { .. }) => {
            bifrost_types::RetryHint::After(STAMPED_BUDGET_FALLBACK)
        }
        None => return,
    };
    // The shared bucket's clock boundary. A provider hint arrives as evidence -
    // either a bare duration (`After`) or an absolute HTTP date (`At`). It is
    // resolved to a REMAINING DURATION against the wall clock here, once, and
    // immediately anchored to the monotonic clock the wait sites sleep on.
    // `min_delay` is the right resolver for both shapes: it returns the
    // duration verbatim for `After` and the wall-clock delta for `At`,
    // saturating at zero for a date already past. Recovery repairs normalize
    // an absolute hint before this call when capturing their local deadline,
    // so this recorder receives the equivalent duration in that case.
    // Never retain an absolute hint for re-resolution after a monotonic sleep.
    let wait_until = Instant::now() + hint.min_delay(SystemTime::now());
    if let Ok(mut guard) = bucket.lock() {
        guard.record_for(account, key, wait_until);
    }
}

/// Longest account-wide throttle wait currently pending for `account`,
/// pruning expired deadlines on the way. `None` when the account may
/// drive work now (including when the mutex is poisoned - see
/// [`record_throttle`]).
///
/// Deliberately takes no `now`: there is exactly one correct answer for an
/// engine wait site - the timer clock's notion of now - and letting each
/// caller supply one is how the wrong clock got read in the first place.
/// Tests that need an explicit instant use `ThrottleBucket::wait_for_account`
/// directly.
#[must_use]
pub(crate) fn account_throttle_wait(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
) -> Option<Duration> {
    throttle_wait(bucket, account, &[])
}

/// Longest pending wait for work that consumes `declared` (the keys
/// `Account::throttle_keys` named for it) on `account`: the account-wide
/// wait plus every declared key, each looked up LITERALLY. A declared
/// shared key is consulted directly rather than through the membership
/// index, so an account that has never failed still sees the first deadline
/// a sibling records under it - which is the whole reason declarations
/// exist. Same clock and poison rules as [`account_throttle_wait`].
#[must_use]
pub(crate) fn throttle_wait(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    declared: &[ThrottleKey],
) -> Option<Duration> {
    let now = Instant::now();
    let mut guard = bucket.lock().ok()?;
    guard.cleanup_expired(now);
    let declared_wait = declared.iter().filter_map(|key| guard.wait_for(key, now));
    guard
        .wait_for_account(account, now)
        .into_iter()
        .chain(declared_wait)
        .max()
}

/// Keys the work consumes, as the account declares them. A thin seam so
/// every wait site asks the same question the same way.
#[must_use]
pub(crate) fn declared_keys(
    account: &dyn bifrost_types::Account,
    account_id: &AccountId,
    work: bifrost_types::ThrottleWork<'_>,
) -> Vec<ThrottleKey> {
    account.throttle_keys(account_id, work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AttemptCause, AuthCause, AuthErrorKind, RetryDisposition, RetryHint, RetryReason,
        ServerCause, ServerErrorKind, TransmissionState, TransportCause, TransportErrorKind,
        TransportKind,
    };

    /// The two contract violations say different things.
    ///
    /// They share a classification and a terminal outcome, and for a while they
    /// shared a message too - so a live batch carrying a BACKFILL checkpoint was
    /// reported as "a PageBoundary::Partial batch carried a checkpoint", sending
    /// whoever read the log after the wrong thing entirely. The boundary was fine;
    /// the checkpoint belonged to a lane that stream does not publish on.
    ///
    /// The same holds for the inventory checkpoint whose withheld state
    /// disagrees with its coverage: it rides the boundary helper but is not a
    /// partial-boundary fault, and must not read as one.
    #[test]
    fn the_contract_violations_do_not_share_a_message() {
        let scope = CursorScope::Account;
        let boundary = format!(
            "{:?}",
            batch_boundary_violation(
                bifrost_types::BatchBoundaryError::PartialCarriesCheckpoint,
                None,
                AccountOperation::SyncChanges,
                &scope
            )
            .chain()
        );
        let disagreement = format!(
            "{:?}",
            batch_boundary_violation(
                bifrost_types::BatchBoundaryError::CheckpointDisagreesWithCoverage,
                None,
                AccountOperation::SyncInventory,
                &scope
            )
            .chain()
        );
        let backfill = format!(
            "{:?}",
            backfill_checkpoint_on_changes(None, AccountOperation::SyncChanges, &scope).chain()
        );
        assert_ne!(
            boundary, backfill,
            "an operator has to be able to tell a bad boundary from a checkpoint on the \
             wrong lane"
        );
        assert_ne!(boundary, disagreement);
        assert!(
            backfill.contains("backfill checkpoint"),
            "and the message must name what actually happened: {backfill}"
        );
        assert!(
            disagreement.contains("withheld"),
            "and the message must name what actually happened: {disagreement}"
        );
    }

    /// Both violations are the `Account` implementation's, not the
    /// provider's: they classify `Internal(AccountContract)`, stop the scope,
    /// point at the implementation, and keep the protocol the offending
    /// checkpoint names.
    #[test]
    fn a_contract_violation_blames_the_implementation_not_the_provider() {
        let scope = CursorScope::Account;
        let checkpoint = bifrost_types::Checkpoint::Change(bifrost_types::ChangeCursor {
            scope: scope.clone(),
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: 1,
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
        });
        for error in [
            batch_boundary_violation(
                bifrost_types::BatchBoundaryError::PartialCarriesCheckpoint,
                Some(&checkpoint),
                AccountOperation::SyncChanges,
                &scope,
            ),
            backfill_checkpoint_on_changes(
                Some(&checkpoint),
                AccountOperation::SyncChanges,
                &scope,
            ),
        ] {
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract)
            );
            assert_eq!(error.recovery(), &RecoveryClass::InternalFailure);
            assert_eq!(
                error.suggested_remediation(),
                Some(&bifrost_types::RemediationAction::ReportBug)
            );
            assert_eq!(error.protocol(), Some(bifrost_types::Protocol::Imap));
        }
    }

    fn build_retry() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
    }

    fn build_reconcile() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .operation(AccountOperation::Send)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .try_build()
        .expect("valid")
    }

    fn build_engine() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
    }

    fn build_terminal() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
    }

    #[test]
    fn plan_recovery_retry_arm() {
        assert!(matches!(
            plan_recovery(build_retry()),
            RecoveryPlan::Retry(_)
        ));
    }

    #[test]
    fn plan_recovery_reconcile_arm() {
        assert!(matches!(
            plan_recovery(build_reconcile()),
            RecoveryPlan::Reconcile(_)
        ));
    }

    #[test]
    fn plan_recovery_engine_arm() {
        assert!(matches!(
            plan_recovery(build_engine()),
            RecoveryPlan::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn plan_recovery_terminal_arm() {
        assert!(matches!(
            plan_recovery(build_terminal()),
            RecoveryPlan::Terminal(_)
        ));
    }

    #[test]
    fn retry_delay_uses_retry_hint_when_present() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let advice = RetryAdvice::new(
            RetryDisposition::SameRequest,
            Some(RetryHint::After(Duration::from_secs(7))),
            RetryReason::Transport,
            None,
        );
        assert_eq!(
            retry_delay(&advice, now, Duration::from_secs(1)),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn retry_delay_uses_fallback_without_hint() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let advice = RetryAdvice::new(
            RetryDisposition::SameRequest,
            None,
            RetryReason::Transport,
            None,
        );
        assert_eq!(
            retry_delay(&advice, now, Duration::from_secs(3)),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn restart_scope_error_derives_engine_restart_scope() {
        let scope = CursorScope::Account;
        let err = restart_scope_error(scope.clone(), AccountOperation::SyncChanges);
        assert!(matches!(
            err.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(got)) if *got == scope
        ));
    }

    #[test]
    fn cursor_decode_failure_routes_to_schema_incompatible() {
        let err = cursor_decode_failure(AccountOperation::SyncChanges);
        assert!(matches!(
            err.recovery(),
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn directive_target_scope_picks_scope_bound_directives() {
        let scope = CursorScope::Account;
        assert_eq!(
            directive_target_scope(&EngineDirective::RestartScope(scope.clone())),
            Some(scope.clone())
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::DowngradeCapabilityForScope(scope.clone())),
            Some(scope.clone())
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::DisableScope(scope.clone())),
            Some(scope)
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::RestartAccount),
            None
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::SchemaIncompatible),
            None
        );
    }

    #[test]
    fn throttle_bucket_records_and_expires() {
        let mut bucket = ThrottleBucket::new();
        // A bucket instant is monotonic now; anchor the test at a fixed
        // point on that clock rather than on the wall clock.
        let now = Instant::now();
        let key = ThrottleKey::Account(AccountId("a".into()));
        bucket.record(key.clone(), now + Duration::from_secs(5));

        assert_eq!(bucket.wait_for(&key, now), Some(Duration::from_secs(5)));
        // After the deadline.
        assert_eq!(bucket.wait_for(&key, now + Duration::from_secs(10)), None);
    }

    #[test]
    fn throttle_bucket_take_max_of_overlapping_records() {
        let mut bucket = ThrottleBucket::new();
        // A bucket instant is monotonic now; anchor the test at a fixed
        // point on that clock rather than on the wall clock.
        let now = Instant::now();
        let key = ThrottleKey::Account(AccountId("a".into()));
        bucket.record(key.clone(), now + Duration::from_secs(5));
        bucket.record(key.clone(), now + Duration::from_secs(2));
        assert_eq!(bucket.wait_for(&key, now), Some(Duration::from_secs(5)));
    }

    #[test]
    fn throttle_bucket_cleanup_drops_expired() {
        let mut bucket = ThrottleBucket::new();
        // A bucket instant is monotonic now; anchor the test at a fixed
        // point on that clock rather than on the wall clock.
        let now = Instant::now();
        let a = ThrottleKey::Account(AccountId("a".into()));
        let b = ThrottleKey::Account(AccountId("b".into()));
        bucket.record(a.clone(), now + Duration::from_secs(1));
        bucket.record(b.clone(), now + Duration::from_secs(60));
        bucket.cleanup_expired(now + Duration::from_secs(30));
        assert!(bucket.wait_for(&a, now + Duration::from_secs(30)).is_none());
        assert!(bucket.wait_for(&b, now + Duration::from_secs(30)).is_some());
    }

    #[test]
    fn wait_for_account_reads_only_the_account_key() {
        // A budget the account recorded holds the work that declares it,
        // not the whole account.
        let mut bucket = ThrottleBucket::new();
        // A bucket instant is monotonic now; anchor the test at a fixed
        // point on that clock rather than on the wall clock.
        let now = Instant::now();
        let a = AccountId("acc-1".into());
        bucket.record_for(
            &a,
            ThrottleKey::Account(a.clone()),
            now + Duration::from_secs(3),
        );
        bucket.record_for(
            &a,
            ThrottleKey::Budget(bifrost_types::ThrottleBudget::new("test|mailbox:x")),
            now + Duration::from_secs(8),
        );
        assert_eq!(
            bucket.wait_for_account(&a, now),
            Some(Duration::from_secs(3))
        );
    }

    #[test]
    fn wait_for_account_excludes_mailbox_keys() {
        // A per-mailbox throttle must not pause the whole account.
        let mut bucket = ThrottleBucket::new();
        // A bucket instant is monotonic now; anchor the test at a fixed
        // point on that clock rather than on the wall clock.
        let now = Instant::now();
        let a = AccountId("acc-1".into());
        bucket.record_for(
            &a,
            ThrottleKey::Mailbox {
                account: a.clone(),
                mailbox: MailboxId("shared@example.com".into()),
            },
            now + Duration::from_secs(30),
        );
        assert_eq!(bucket.wait_for_account(&a, now), None);
    }

    fn throttled_error(scope: Option<ErrorScope>) -> AccountError {
        let mut builder = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
        .operation(AccountOperation::SyncChanges);
        if let Some(scope) = scope {
            builder = builder.scope(scope);
        }
        builder.try_build().expect("valid")
    }

    #[test]
    fn reconcile_throttle_is_recorded_for_the_account() {
        let account = AccountId("a".into());
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited {
                retry_hint: Some(RetryHint::After(Duration::from_secs(30))),
            }),
        )
        .operation(AccountOperation::Send)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .throttle_scope(ThrottleScope::Account)
        .try_build()
        .expect("valid");
        let RecoveryClass::Reconcile(advice) = error.recovery() else {
            panic!("in-flight send must reconcile");
        };
        let bucket = std::sync::Mutex::new(ThrottleBucket::new());

        record_reconcile_throttle(&bucket, &account, advice, &error);

        assert!(account_throttle_wait(&bucket, &account).is_some());

        // The drive loop and the push reconciler sleep this before
        // probing; the provider hint must win over the 1s fallback or
        // the read-back walks straight back into the rate limit.
        let delay = reconcile_delay(advice, SystemTime::now(), Duration::from_secs(1));
        assert!(
            delay > Duration::from_secs(25),
            "throttled reconcile must honor the provider hint, got {delay:?}"
        );
    }

    /// A stamped budget is recorded ALONE, and holds even without a hint.
    ///
    /// The error also carries an `Account` throttle scope, as every Graph
    /// throttle does. Lifting that into the account key as well would pause
    /// every sibling mailbox of the account; and a hintless stamp that
    /// recorded nothing would leave a sibling declaring the budget to walk
    /// straight into the same 429 the observer is pacing itself against.
    #[test]
    fn a_stamped_budget_is_recorded_alone_and_holds_without_a_hint() {
        let account = AccountId("a".into());
        let budget = bifrost_types::ThrottleBudget::new("test|mailbox:x");
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        )
        .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
        .operation(AccountOperation::SyncChanges)
        .throttle_scope(ThrottleScope::Account)
        .throttle_budget(budget.clone())
        .try_build()
        .expect("valid");
        let RecoveryClass::Retry(advice) = error.recovery() else {
            panic!("an unsent rate limit retries");
        };
        let bucket = std::sync::Mutex::new(ThrottleBucket::new());

        record_throttle(&bucket, &account, advice, &error);

        assert_eq!(
            account_throttle_wait(&bucket, &account),
            None,
            "the lifted account key must not be recorded beside a stamped budget"
        );
        let wait = throttle_wait(&bucket, &account, &[ThrottleKey::Budget(budget)]);
        assert!(
            wait.is_some_and(|wait| wait <= STAMPED_BUDGET_FALLBACK),
            "a hintless stamped budget holds for the local fallback, got {wait:?}"
        );
    }

    /// A hintless folder-scoped mailbox throttle (IMAP's `[LIMIT]`) is held
    /// for the fallback on its `Mailbox` key, while a hintless account-scoped
    /// one still records nothing.
    #[test]
    fn a_hintless_mailbox_throttle_holds_its_mailbox_for_the_fallback() {
        let account = AccountId("a".into());
        let build = |scope: ThrottleScope, error_scope: Option<ErrorScope>| {
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                Cause::Server(ServerCause::RateLimited { retry_hint: None }),
            )
            .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
            .operation(AccountOperation::SyncChanges)
            .throttle_scope(scope);
            if let Some(error_scope) = error_scope {
                builder = builder.scope(error_scope);
            }
            builder.try_build().expect("valid")
        };
        let record = |bucket: &std::sync::Mutex<ThrottleBucket>, error: &AccountError| {
            let RecoveryClass::Retry(advice) = error.recovery() else {
                panic!("an unsent rate limit retries");
            };
            record_throttle(bucket, &account, advice, error);
        };

        let bucket = std::sync::Mutex::new(ThrottleBucket::new());
        record(
            &bucket,
            &build(
                ThrottleScope::Mailbox,
                Some(ErrorScope::Cursor(CursorScope::Folder(
                    bifrost_types::FolderId("INBOX".into()),
                ))),
            ),
        );
        let inbox = ThrottleKey::Mailbox {
            account: account.clone(),
            mailbox: MailboxId("INBOX".into()),
        };
        assert!(
            throttle_wait(&bucket, &account, &[inbox])
                .is_some_and(|wait| wait <= STAMPED_BUDGET_FALLBACK)
        );

        let bucket = std::sync::Mutex::new(ThrottleBucket::new());
        record(&bucket, &build(ThrottleScope::Account, None));
        assert_eq!(account_throttle_wait(&bucket, &account), None);
    }

    /// A declared shared key is consulted directly: an account that never
    /// recorded anything, so was never enrolled, still sees a deadline
    /// another account recorded under a key it declares.
    #[test]
    fn a_declared_key_reaches_an_account_that_was_never_enrolled() {
        let recorder = AccountId("recorder".into());
        let bystander = AccountId("bystander".into());
        let key = ThrottleKey::Budget(bifrost_types::ThrottleBudget::new("test|mailbox:x"));
        let bucket = std::sync::Mutex::new(ThrottleBucket::new());
        bucket.lock().expect("bucket").record_for(
            &recorder,
            key.clone(),
            Instant::now() + Duration::from_secs(30),
        );

        assert_eq!(account_throttle_wait(&bucket, &bystander), None);
        assert!(throttle_wait(&bucket, &bystander, &[key]).is_some());
    }

    #[test]
    fn reconcile_delay_falls_back_when_no_hint_is_carried() {
        // A transport drop mid-send reconciles without any provider
        // timing signal; the caller's fallback is what must apply.
        let error = build_reconcile();
        let RecoveryClass::Reconcile(advice) = error.recovery() else {
            panic!("in-flight non-idempotent transport drop must reconcile");
        };
        assert_eq!(advice.retry_hint, None);

        assert_eq!(
            reconcile_delay(advice, SystemTime::now(), Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn resolve_throttle_key_current_operation_does_not_enter_bucket() {
        let account = AccountId("a".into());
        let error = throttled_error(None);
        assert!(resolve_throttle_key(ThrottleScope::CurrentOperation, &account, &error).is_none());
    }

    #[test]
    fn resolve_throttle_key_reads_identities_from_the_error() {
        let account = AccountId("a".into());
        let mailbox_err = throttled_error(Some(ErrorScope::Mailbox {
            id: "shared@example.com".into(),
        }));
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Mailbox, &account, &mailbox_err),
            Some(ThrottleKey::Mailbox {
                account: account.clone(),
                mailbox: MailboxId("shared@example.com".into()),
            })
        );
    }

    /// The shape production actually produces.
    ///
    /// The test above builds `ErrorScope::Mailbox { .. }`, which only tests
    /// construct: IMAP's `with_mailbox` is annotated production-dead, and every
    /// real folder producer goes through `with_folder_scope`, yielding
    /// `Cursor(Folder(_))`. IMAP's `mailbox_throttle` is the only production
    /// emitter of `ThrottleScope::Mailbox` anywhere in the workspace and it
    /// fires for both shapes, so a reader accepting only the synthetic one
    /// degraded on every real per-folder throttle and on none of the tested
    /// ones - the bucket entry was simply never recorded.
    #[test]
    fn a_folder_scoped_throttle_keys_the_mailbox_bucket() {
        let account = AccountId("a".into());
        let folder_err = throttled_error(Some(ErrorScope::Cursor(CursorScope::Folder(
            bifrost_types::FolderId("INBOX/shared".into()),
        ))));
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Mailbox, &account, &folder_err),
            Some(ThrottleKey::Mailbox {
                account,
                mailbox: MailboxId("INBOX/shared".into()),
            }),
            "a folder-scoped per-mailbox throttle must record a mailbox bucket \
             entry, not fall through to no entry at all"
        );
    }

    #[test]
    fn a_mailbox_throttle_naming_no_mailbox_never_widens_to_the_account() {
        let account = AccountId("a".into());
        let bare = throttled_error(None);
        let account_key = Some(ThrottleKey::Account(account.clone()));
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Mailbox, &account, &bare),
            None
        );
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Account, &account, &bare),
            account_key
        );
    }
}
