//! Read-back guard for `MutationReplaySafety::None` protocols.
//!
//! After a retry, the engine cannot tell whether the server applied
//! the original attempt or just the retry. The read-back guard
//! re-fetches each retried batch via
//! `account.get_stream(_, Projection::FlagsOnly)` and reconciles
//! `Failed` outcomes against actual server state: if the flags now
//! match the intended target, the failure is downgraded to `Skipped`
//! (the original apply succeeded).
//!
//! Applies to all four protocols today; the
//! `MutationReplaySafety::ReplayToken` variant is reserved but unused.

use std::collections::HashSet;

use bifrost_types::{
    Account, AccountErrorKind, FlagOp, HydratedObject, HydratedObjectKind, ItemOutcome,
    MembershipScope, ObjectId, Projection, SyncEvent,
};
use futures::stream::{self, StreamExt};

use crate::error::Error;

/// Outcome of a read-back guard pass.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadbackOutcome {
    /// Items whose post-mutation flag state matched the requested
    /// target. These were applied (possibly by the pre-retry attempt)
    /// and are downgraded to `Skipped`.
    pub skipped: u64,
    /// Items whose state did NOT match the target. The protocol's
    /// `Failed` outcome stands.
    pub still_failed: u64,
    /// Items the read-back could not classify because the protocol
    /// reported `ItemOutcome::Failed` / `Uncertain` for the hydrated
    /// fetch itself. The original mutation `Failed` outcome stands.
    pub uncertain: u64,
}

/// Run the read-back guard against a set of ids and a target flag-op.
///
/// `account.get_stream(ids, Projection::FlagsOnly)` yields
/// `HydratedObjectKind::FlagsOnly(HashSet<String>)` per id. The guard
/// compares each id's hydrated flags against the target state encoded
/// in `op`:
///
/// - `Add(set)` -> target satisfied iff every flag in `set` is present.
/// - `Remove(set)` -> target satisfied iff no flag in `set` is present.
/// - `Set(set)` -> target satisfied iff hydrated == `set`.
/// - `Patch { add, remove }` -> add + remove must both hold.
pub async fn run_readback_guard(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    op: &FlagOp,
) -> Result<ReadbackOutcome, Error> {
    run_readback_guard_observed(account, ids, op, None).await
}

/// Where a read-back pass reports every per-item hydration failure it
/// absorbs. The guards count such a failure as `uncertain` and move on, which
/// is right for the ACCOUNTING - but a hydration that failed on a 429 carries
/// a provider throttle deadline, and absorbing it silently hid that deadline
/// from every other lane and every sibling account. The engine's campaign
/// passes a sink that records it in the shared throttle bucket; the published
/// guards pass none, keeping their behaviour exactly as documented.
/// Stream-level terminations need no sink: they are returned to the caller as
/// `Error::Account`, which records them itself.
pub(crate) type ReadbackErrorSink<'a> = Option<&'a (dyn Fn(&bifrost_types::AccountError) + Sync)>;

fn observe(sink: ReadbackErrorSink<'_>, item: &ItemOutcome<HydratedObject>) {
    if let Some(sink) = sink {
        match item {
            ItemOutcome::Failed(failure) => sink(&failure.error),
            ItemOutcome::Uncertain(uncertain) => sink(&uncertain.error),
            ItemOutcome::Succeeded(_) => {}
        }
    }
}

pub(crate) async fn run_readback_guard_observed(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    op: &FlagOp,
    sink: ReadbackErrorSink<'_>,
) -> Result<ReadbackOutcome, Error> {
    let input = stream::iter(ids).boxed();
    let mut stream = account.get_stream(input, Projection::FlagsOnly);
    let mut outcome = ReadbackOutcome::default();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => {
                for item in batch.items {
                    observe(sink, &item);
                    match item {
                        ItemOutcome::Succeeded(success) => {
                            if matches_target(&success.output, op) {
                                outcome.skipped = outcome.skipped.saturating_add(1);
                            } else {
                                outcome.still_failed = outcome.still_failed.saturating_add(1);
                            }
                        }
                        ItemOutcome::Failed(_) | ItemOutcome::Uncertain(_) => {
                            // The hydration itself failed or was
                            // ambiguous; the read-back cannot
                            // disambiguate so the mutation's original
                            // `Failed` outcome stands.
                            outcome.uncertain = outcome.uncertain.saturating_add(1);
                        }
                    }
                }
            }
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(err) => {
                return Err(Error::Account(err));
            }
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(outcome)
}

fn matches_target(hydrated: &HydratedObject, op: &FlagOp) -> bool {
    let HydratedObjectKind::FlagsOnly(observed) = &hydrated.kind else {
        // Wrong projection - cannot reconcile. Be safe: treat as not
        // matched so the original `Failed` outcome stands.
        return false;
    };
    matches_target_set(observed, op)
}

#[must_use]
pub(crate) fn matches_target_set(observed: &HashSet<String>, op: &FlagOp) -> bool {
    match op {
        FlagOp::Add(set) => set.iter().all(|f| observed.contains(f)),
        FlagOp::Remove(set) => set.iter().all(|f| !observed.contains(f)),
        FlagOp::Set(set) => observed == set,
        FlagOp::Patch { add, remove } => {
            add.iter().all(|f| observed.contains(f)) && remove.iter().all(|f| !observed.contains(f))
        }
        // `FlagOp` is `#[non_exhaustive]`; an unknown operation cannot
        // be reconciled against observed flags, so the original
        // `Failed` outcome stands.
        _ => false,
    }
}

/// Read-back guard for a bulk-move campaign.
///
/// The flag guard cannot reconcile a move: the post-mutation signal is
/// container membership, not a flag set. This guard re-fetches each
/// unresolved id at `Projection::Metadata` (whose hydrated shape carries
/// the object's `memberships`) and checks whether `destination` is now
/// present:
///
/// - membership contains `destination` -> the move landed (possibly on
///   the pre-retry attempt); downgrade the apparent failure to `Skipped`.
/// - membership does not contain it -> the protocol's `Failed` outcome
///   stands (`still_failed`).
/// - the hydration itself `Failed` / `Uncertain`, or came back at the
///   wrong projection -> `uncertain`; the original `Failed` stands.
pub async fn run_move_readback_guard(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    destination: &MembershipScope,
) -> Result<ReadbackOutcome, Error> {
    run_move_readback_guard_observed(account, ids, destination, None).await
}

pub(crate) async fn run_move_readback_guard_observed(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    destination: &MembershipScope,
    sink: ReadbackErrorSink<'_>,
) -> Result<ReadbackOutcome, Error> {
    let input = stream::iter(ids).boxed();
    let mut stream = account.get_stream(input, Projection::Metadata);
    let mut outcome = ReadbackOutcome::default();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => {
                for item in batch.items {
                    observe(sink, &item);
                    match item {
                        ItemOutcome::Succeeded(success) => {
                            if membership_contains(&success.output, destination) {
                                outcome.skipped = outcome.skipped.saturating_add(1);
                            } else {
                                outcome.still_failed = outcome.still_failed.saturating_add(1);
                            }
                        }
                        ItemOutcome::Failed(_) | ItemOutcome::Uncertain(_) => {
                            outcome.uncertain = outcome.uncertain.saturating_add(1);
                        }
                    }
                }
            }
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(err) => {
                return Err(Error::Account(err));
            }
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(outcome)
}

/// Read-back guard for a bulk-destroy campaign.
///
/// A destroy succeeds when the object is *gone*, so the reconciliation
/// is the inverse of the flag guard. This guard re-fetches each
/// unresolved id at `Projection::Metadata`:
///
/// - the fetch `Succeeded` -> the object is still hydratable, so the
///   destroy did NOT land; the `Failed` outcome stands (`still_failed`).
/// - the fetch `Failed(NotFound)` -> the server no longer returns the
///   id, so the destroy landed; downgrade to `Skipped`.
/// - any other fetch `Failed` -> the read-back itself failed; classify
///   it as uncertain so the original mutation outcome stands.
/// - the fetch was `Uncertain` -> `uncertain`; the original `Failed`
///   stands.
///
/// An id the protocol simply omits from the hydrated stream (rather than
/// emitting `Failed` for) produces no outcome here and stays counted as
/// `pending_retry`. That is the conservative bias: a destroy is only
/// reclassified as `Skipped` on an explicit not-found signal.
pub async fn run_destroy_readback_guard(
    account: &dyn Account,
    ids: Vec<ObjectId>,
) -> Result<ReadbackOutcome, Error> {
    run_destroy_readback_guard_observed(account, ids, None).await
}

pub(crate) async fn run_destroy_readback_guard_observed(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    sink: ReadbackErrorSink<'_>,
) -> Result<ReadbackOutcome, Error> {
    let input = stream::iter(ids).boxed();
    let mut stream = account.get_stream(input, Projection::Metadata);
    let mut outcome = ReadbackOutcome::default();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => {
                for item in batch.items {
                    observe(sink, &item);
                    match item {
                        ItemOutcome::Succeeded(_) => {
                            outcome.still_failed = outcome.still_failed.saturating_add(1);
                        }
                        ItemOutcome::Failed(failure) => {
                            if destroy_readback_saw_absence(&failure.error) {
                                outcome.skipped = outcome.skipped.saturating_add(1);
                            } else {
                                outcome.uncertain = outcome.uncertain.saturating_add(1);
                            }
                        }
                        ItemOutcome::Uncertain(_) => {
                            outcome.uncertain = outcome.uncertain.saturating_add(1);
                        }
                    }
                }
            }
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(err) => {
                return Err(Error::Account(err));
            }
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(outcome)
}

fn destroy_readback_saw_absence(error: &bifrost_types::AccountError) -> bool {
    matches!(error.kind(), AccountErrorKind::NotFound(_))
}

fn membership_contains(hydrated: &HydratedObject, destination: &MembershipScope) -> bool {
    let HydratedObjectKind::Metadata(entry) = &hydrated.kind else {
        // Wrong projection - cannot reconcile. Be safe: treat as not
        // matched so the original `Failed` outcome stands.
        return false;
    };
    entry.memberships.iter().any(|m| m == destination)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, ResourceKind, ServerCause, ServerErrorKind,
    };
    use std::collections::HashSet;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn add_target_matches_when_subset_present() {
        let obs = set(&["\\Seen", "\\Flagged"]);
        let op = FlagOp::Add(set(&["\\Seen"]));
        assert!(matches_target_set(&obs, &op));
    }

    #[test]
    fn add_target_does_not_match_when_missing() {
        let obs = set(&["\\Flagged"]);
        let op = FlagOp::Add(set(&["\\Seen"]));
        assert!(!matches_target_set(&obs, &op));
    }

    #[test]
    fn remove_target_matches_when_absent() {
        let obs = set(&["\\Flagged"]);
        let op = FlagOp::Remove(set(&["\\Seen"]));
        assert!(matches_target_set(&obs, &op));
    }

    #[test]
    fn set_target_requires_exact_match() {
        let obs = set(&["\\Seen"]);
        let op = FlagOp::Set(set(&["\\Seen"]));
        assert!(matches_target_set(&obs, &op));
        let op2 = FlagOp::Set(set(&["\\Seen", "\\Flagged"]));
        assert!(!matches_target_set(&obs, &op2));
    }

    #[test]
    fn patch_requires_both_sides() {
        let obs = set(&["\\Seen"]);
        let op = FlagOp::Patch {
            add: set(&["\\Seen"]),
            remove: set(&["\\Flagged"]),
        };
        assert!(matches_target_set(&obs, &op));
        let op2 = FlagOp::Patch {
            add: set(&["\\Seen"]),
            remove: set(&["\\Seen"]),
        };
        assert!(!matches_target_set(&obs, &op2));
    }

    #[test]
    fn destroy_readback_only_accepts_not_found_as_absence() {
        let not_found = AccountErrorBuilder::new(
            AccountErrorKind::NotFound(ResourceKind::Message),
            Cause::Request(bifrost_types::RequestCause::NotFound {
                what: ResourceKind::Message,
                id: None,
            }),
        )
        .try_build()
        .expect("valid not-found classification");
        assert!(destroy_readback_saw_absence(&not_found));

        let rate_limited = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        )
        .try_build()
        .expect("valid rate-limit classification");
        assert!(!destroy_readback_saw_absence(&rate_limited));
    }
}
