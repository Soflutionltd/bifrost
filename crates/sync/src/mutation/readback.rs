//! Per-object read-back of uncertain writes, preserving partial observations.
//! Identity and projection are checked here; accounts judge their own flag
//! vocabulary. Campaigns may reassert declared unversioned flag and destroy
//! targets after valid unsatisfied observations, within shared retry limits.

use std::collections::{HashMap, HashSet};

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
    /// Valid observations whose current state does not satisfy the target.
    /// This does not prove that an earlier write never landed.
    pub still_failed: u64,
    /// Targets with missing, failed, indeterminate or invalid observations,
    /// including conflicting duplicates and inconsistent hydrated identities.
    pub uncertain: u64,
}

/// Run the read-back guard against a set of ids and a target flag operation.
/// Complete FlagsOnly observations are judged through `Account::flag_target`.
/// Invalid projections, inconsistent identities and conflicting duplicates
/// remain uncertain. This guard only observes; campaigns own reassertion.
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
    guard_outcome(run_object_readback(account, ids, ReadbackTarget::Flags(op), sink).await)
}

/// The published guards' accounting over one report: a stream termination is
/// the guard's `Err`; otherwise every requested id lands in exactly one count.
fn guard_outcome(report: ReadbackReport) -> Result<ReadbackOutcome, Error> {
    if let Some(error) = report.termination {
        return Err(error);
    }
    let mut outcome = ReadbackOutcome::default();
    for result in report.objects.values() {
        match result {
            ObjectReadback::Satisfied => outcome.skipped += 1,
            ObjectReadback::Unsatisfied => outcome.still_failed += 1,
            ObjectReadback::Unresolved => outcome.uncertain += 1,
        }
    }
    Ok(outcome)
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
    guard_outcome(run_object_readback(account, ids, ReadbackTarget::Move(destination), sink).await)
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
/// emitting `Failed` for) counts as `uncertain` here and `pending_retry` in
/// the campaign. A destroy is only
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
    guard_outcome(run_object_readback(account, ids, ReadbackTarget::Destroy, sink).await)
}

/// Per-object observations retain identity and partial progress even when the
/// hydration stream terminates. Missing ids remain explicitly unresolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObjectReadback {
    Satisfied,
    Unsatisfied,
    Unresolved,
}

pub(crate) enum ReadbackTarget<'a> {
    Flags(&'a FlagOp),
    Move(&'a MembershipScope),
    Destroy,
}

pub(crate) struct ReadbackReport {
    /// One entry per requested id, pre-filled `Unresolved`; the placeholder is
    /// not an observation.
    pub(crate) objects: HashMap<ObjectId, ObjectReadback>,
    /// Per-item hydration failures whose recovery is `Engine(_)`, including
    /// failures for ids that were never requested.
    pub(crate) engine_failures: Vec<bifrost_types::AccountError>,
    /// A stream-level termination, returned as before.
    pub(crate) termination: Option<Error>,
}

pub(crate) async fn run_object_readback(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    target: ReadbackTarget<'_>,
    sink: ReadbackErrorSink<'_>,
) -> ReadbackReport {
    let mut report = ReadbackReport {
        objects: ids
            .iter()
            .cloned()
            .map(|id| (id, ObjectReadback::Unresolved))
            .collect(),
        engine_failures: Vec::new(),
        termination: None,
    };
    let mut observed = HashSet::new();
    let mut disputed = HashSet::new();
    let projection = match target {
        ReadbackTarget::Flags(_) => Projection::FlagsOnly,
        _ => Projection::Metadata,
    };
    let mut stream = account.get_stream(stream::iter(ids).boxed(), projection);
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => {
                for item in batch.items {
                    // Even an unrequested failure can carry account-wide engine
                    // evidence or a shared throttle; report it before identity checks.
                    observe(sink, &item);
                    let error = match &item {
                        ItemOutcome::Failed(f) => Some(&f.error),
                        ItemOutcome::Uncertain(u) => Some(&u.error),
                        ItemOutcome::Succeeded(_) => None,
                    };
                    if let Some(error) = error
                        && matches!(error.recovery(), bifrost_types::RecoveryClass::Engine(_))
                    {
                        report.engine_failures.push(error.clone());
                    }
                    let (id, observation) = match item {
                        ItemOutcome::Succeeded(success) => {
                            let id = ObjectId(success.item.0);
                            if !report.objects.contains_key(&id) {
                                continue;
                            }
                            let result = if success.output.id != id {
                                ObjectReadback::Unresolved
                            } else {
                                match (&target, &success.output.kind) {
                                    (
                                        ReadbackTarget::Flags(op),
                                        HydratedObjectKind::FlagsOnly(flags),
                                    ) => match account.flag_target(&id, flags, op) {
                                        bifrost_types::FlagTargetState::Satisfied => {
                                            ObjectReadback::Satisfied
                                        }
                                        bifrost_types::FlagTargetState::Unsatisfied => {
                                            ObjectReadback::Unsatisfied
                                        }
                                        _ => ObjectReadback::Unresolved,
                                    },
                                    (
                                        ReadbackTarget::Move(destination),
                                        HydratedObjectKind::Metadata(entry),
                                    ) => {
                                        if entry.memberships.contains(destination) {
                                            ObjectReadback::Satisfied
                                        } else {
                                            ObjectReadback::Unsatisfied
                                        }
                                    }
                                    (ReadbackTarget::Destroy, _) => ObjectReadback::Unsatisfied,
                                    _ => ObjectReadback::Unresolved,
                                }
                            };
                            (id, result)
                        }
                        ItemOutcome::Failed(f) => {
                            let result = if matches!(target, ReadbackTarget::Destroy)
                                && destroy_readback_saw_absence(&f.error)
                            {
                                ObjectReadback::Satisfied
                            } else {
                                ObjectReadback::Unresolved
                            };
                            (ObjectId(f.item.0), result)
                        }
                        ItemOutcome::Uncertain(u) => {
                            (ObjectId(u.item.0), ObjectReadback::Unresolved)
                        }
                    };
                    if let Some(value) = report.objects.get_mut(&id) {
                        if observed.insert(id.clone()) {
                            *value = observation;
                        } else if *value != observation || disputed.contains(&id) {
                            disputed.insert(id);
                            *value = ObjectReadback::Unresolved;
                        }
                    }
                }
            }
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(error) => {
                report.termination = Some(Error::Account(error));
                break;
            }
            _ => {}
        }
    }
    report
}

fn destroy_readback_saw_absence(error: &bifrost_types::AccountError) -> bool {
    matches!(error.kind(), AccountErrorKind::NotFound(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, ResourceKind, ServerCause, ServerErrorKind,
    };

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

#[cfg(test)]
#[path = "readback_tests.rs"]
mod observation_tests;
