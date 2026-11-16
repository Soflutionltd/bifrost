//! Engine-wide monotonic throttle deadlines and declaration-based lookup.

use bifrost_types::{AccountError, AccountId, ThrottleKey};
#[cfg(test)]
use bifrost_types::{ReconcileAdvice, RetryAdvice, ThrottleScope};
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
///
/// Nothing here is durable: the bucket is one in-memory `Arc<ThrottleBucket>` per
/// `SyncEngine`, never serialized, never checkpointed, and dropped with the
/// engine. A monotonic instant is therefore safe - it never has to survive a
/// process. [`ThrottleBucket::observe`] resolves a provider `RetryHint` to a
/// remaining duration and immediately anchors it to `Instant::now()`.
/// Recovery repairs also retain a local deadline: [`record_repair_throttle`]
/// uses the same observation deadline for its local wait and shared holds.
use tokio::time::Instant;

/// Engine-scope throttle bucket. Records "do not drive work for this
/// key before `wait_until`" deadlines and answers "how long should I
/// pause work that maps to this key now?".
///
/// One bucket per `SyncEngine`, shared by every attached account.
/// `Budget` keys cross account boundaries by design: a key names a
/// provider budget, and every account whose work is charged to it pauses
/// on it. `Mailbox` and `Account` keys are per-account. Detach does not
/// erase deadlines: a new attachment of the same account still honors the
/// provider hold until it expires.
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
    waits: std::sync::Mutex<HashMap<ThrottleKey, Instant>>,
}

impl ThrottleBucket {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe every error leg and return the longest local pacing deadline.
    pub fn observe(&self, account: &AccountId, error: &AccountError) -> Option<Instant> {
        let now = Instant::now();
        let wall = SystemTime::now();
        error
            .throttle_holds()
            .into_iter()
            .map(|(target, hint)| {
                let deadline = now + hint.min_delay(wall);
                if let Some(key) = hold_key(target, account) {
                    self.record(key, deadline);
                }
                deadline
            })
            .max()
    }

    /// Record a wait-until deadline for `key`. If a wait already
    /// exists, the later of the two deadlines wins (a stricter throttle
    /// from a parallel response should not shorten an earlier one).
    pub fn record(&self, key: ThrottleKey, wait_until: Instant) {
        let Ok(mut waits) = self.waits.lock() else {
            return;
        };
        waits
            .entry(key)
            .and_modify(|existing| {
                if wait_until > *existing {
                    *existing = wait_until;
                }
            })
            .or_insert(wait_until);
    }

    /// Record an observed deadline. The key supplies its account identity
    /// or names a shared budget; no observing-account enrollment is needed.
    pub fn record_for(&self, key: ThrottleKey, wait_until: Instant) {
        self.record(key, wait_until);
    }

    /// Return the remaining wait for `key` at `now`. `None` means no
    /// throttle applies (or the recorded deadline has expired).
    #[must_use]
    pub fn wait_for(&self, key: &ThrottleKey, now: Instant) -> Option<Duration> {
        let until = *self.waits.lock().ok()?.get(key)?;
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
    pub fn cleanup_expired(&self, now: Instant) {
        if let Ok(mut waits) = self.waits.lock() {
            waits.retain(|_, until| *until > now);
        }
    }
}

/// Resolve the `ThrottleKey` a `ThrottleScope` maps to, given the
/// identities the classified error actually carries. Mailbox scope never
/// falls back to the account key: an account key is wider than the
/// producer's throttle.
///
/// - `CurrentOperation` never enters the bucket (per-call hint).
/// - `Mailbox` uses an Owner scope as a principal key and a native Mailbox
///   or Folder/FolderType cursor scope as a canonical folder key. A missing
///   identity remains local to the current operation.
/// - `Account` uses the account key.
///
/// A throttle that spans accounts is a stamped and declared budget, never a
/// scope. An error that stamps budgets never reaches here:
/// `AccountError::throttle_holds` records exactly the stamped budgets instead.
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
#[cfg(test)]
pub(crate) fn resolve_throttle_key(
    scope: ThrottleScope,
    account: &AccountId,
    error: &AccountError,
) -> Option<ThrottleKey> {
    let error = error
        .clone()
        .into_builder()
        .with_throttle(|throttle| throttle.scope = Some(scope))
        .try_build()
        .unwrap();
    error
        .throttle_holds()
        .into_iter()
        .find_map(|(target, _)| hold_key(target, account))
}

/// The one mapping from a provider-side hold target to the engine bucket key.
/// `CurrentOperation` (and any future target) paces only the observer, so it
/// enters no shared bucket.
fn hold_key(target: bifrost_types::HoldTarget, account: &AccountId) -> Option<ThrottleKey> {
    match target {
        bifrost_types::HoldTarget::Account => Some(ThrottleKey::Account(account.clone())),
        bifrost_types::HoldTarget::Owner(owner) => Some(ThrottleKey::Owner {
            account: account.clone(),
            owner,
        }),
        bifrost_types::HoldTarget::Folder(folder) => Some(ThrottleKey::Folder {
            account: account.clone(),
            folder,
        }),
        bifrost_types::HoldTarget::Budget(budget) => Some(ThrottleKey::Budget(budget)),
        _ => None,
    }
}

/// Record a `RetryAdvice`'s throttle deadline on the shared bucket,
/// resolving the key from the error's own identities. No-ops when the
/// advice carries no throttle scope or no retry hint, and when the
/// bucket mutex is poisoned (a throttle wait is advisory; panicking a
/// worker over it would trade a pause for an outage).
#[cfg(test)]
pub(crate) fn record_throttle(
    bucket: &ThrottleBucket,
    account: &AccountId,
    _advice: &RetryAdvice,
    error: &AccountError,
) {
    bucket.observe(account, error);
}

#[cfg(test)]
pub(crate) fn record_reconcile_throttle(
    bucket: &ThrottleBucket,
    account: &AccountId,
    _advice: &ReconcileAdvice,
    error: &AccountError,
) {
    bucket.observe(account, error);
}

/// Record every leg and capture the same monotonic deadline for repair pacing.
pub(crate) fn record_repair_throttle(
    bucket: &ThrottleBucket,
    account: &AccountId,
    error: &AccountError,
) -> Option<Instant> {
    bucket.observe(account, error)
}

#[cfg(test)]
const STAMPED_BUDGET_FALLBACK: Duration = bifrost_types::HINTLESS_HOLD_WAIT;

/// Longest account-wide throttle wait currently pending for `account`,
/// pruning expired deadlines on the way. `None` when the account may
/// drive work now (including when the mutex is poisoned - see
/// `record_throttle`).
///
/// Deliberately takes no `now`: there is exactly one correct answer for an
/// engine wait site - the timer clock's notion of now - and letting each
/// caller supply one is how the wrong clock got read in the first place.
/// Tests that need an explicit instant use `ThrottleBucket::wait_for_account`
/// directly.
#[must_use]
pub(crate) fn account_throttle_wait(
    bucket: &ThrottleBucket,
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
    bucket: &ThrottleBucket,
    account: &AccountId,
    declared: &[ThrottleKey],
) -> Option<Duration> {
    let now = Instant::now();
    let guard = bucket;
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
#[path = "throttle_tests.rs"]
mod tests;
