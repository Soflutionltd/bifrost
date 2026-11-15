//! Staged account replacement and its cancellation guard.

use super::establish::{EstablishOrigin, run_establish};
use super::recovery::{broadcast_terminated, engine_pause, pacing::*, stale_generation};
use super::*;

mod guard;
mod transaction;

#[cfg(test)]
pub(super) use transaction::{ReattachScopeStep, reattach_account_paced, reattach_scope_step};

pub use guard::ReattachAttempt;
use guard::{
    ReattachGuard, commit_reattach_inserts, rollback_reattach_inserts,
    unwind_replacement_subscriptions,
};
pub(super) use transaction::{
    ReplacementOpen, accepted_push_scopes, open_replacement, reattach_account, restart_account,
};
