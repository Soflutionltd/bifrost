//! Id-to-outcome accounting for one wire command.
//!
//! Every account-layer path that turns decoded object ids into a UID operand
//! and then attributes the server's answer back to those ids used to
//! re-derive "requested set -> wire operand -> outcome attribution" by hand.
//! Four copies of that derivation (flag mutation, destroy, move, hydration)
//! agreed only by convention, and the class of bug that convention admits is
//! real: an id silently dropped between the requested set and the operand
//! became either a fabricated `Succeeded(Applied)` or an id with no outcome
//! at all, both of which break the one-outcome-per-id contract the sync
//! engine holds.
//!
//! [`TargetBatch`] is the single owner of that derivation. It takes the
//! decoded ids, builds the wire [`UidSetExpr`] from those ids and nothing else,
//! retains typed message identities through operand construction and is the
//! only way to mint the batch's outcomes.
//!
//! Exactly-once is a property of the types rather than of an assertion.
//! [`TargetBatch::settle`] drives the classification itself: it walks its own
//! targets and hands the caller one [`Target`] at a time - a non-`Clone`
//! permit carrying the id and the `BatchItemId` stamped from it - and the
//! caller must hand back the [`Sealed`] outcome that permit produced.
//! `Sealed` has no constructor but [`Target::seal`], which consumes the
//! permit, so a classifier cannot answer for an id twice (the permit is
//! gone), cannot skip one (it owes a `Sealed` per call and has no other way
//! to make one), and cannot stamp an outcome with an id the batch did not
//! give it (the batch stamps it, not the caller). No length check, no
//! id-keyed lookup, and nothing for a release build to skip.
//!
//! The one property Rust cannot express is that a `TargetBatch` must be
//! consumed at all: a value can always be dropped. That single residue is
//! still a debug assertion in [`Drop`], so a path that returns early with ids
//! unaccounted for cannot go unnoticed.

use super::error::excluded_from_operand_error;

use bifrost_types::{
    AccountError, AccountOperation, BatchFailure, BatchItemId, BatchSuccess, ItemOutcome,
};

use super::envelope::DecodedObjectId;
use crate::types::{MailboxName, Uid, UidSetExpr};

/// The single predicate for "this UID can be named on the wire".
///
/// Every operand in the account layer is built from this and nothing else,
/// so the set of UIDs a command names and the set of UIDs a caller believes
/// it named cannot disagree.
// Keep this wire admission separate from text decoding and cache observation:
// raw operands report excluded targets, while decoded message identities are
// typed and cannot carry zero. Observed sets discard invalid numeric values.
// Sharing those failure policies would erase their different contracts.
fn operand_uid(uid: u32) -> Option<Uid> {
    Uid::new(uid)
}

/// A wire UID operand plus the UIDs that did not reach it.
///
/// This is the one place a `UidSetExpr` is built from decoded UIDs.
/// [`TargetBatch`] is the batch-shaped wrapper around it, for lanes that owe
/// one [`ItemOutcome`] per id; the single-id and PIM-primitive lanes, whose
/// results are not per-id outcomes, use `UidOperand` directly so that they
/// share the operand's definition of what a UID set can carry rather than
/// re-deriving it. A lane with no per-id outcome lane has nowhere to report
/// an excluded UID, so it must refuse the whole command
/// ([`UidOperand::excluded`] non-empty) instead of quietly sending a
/// narrower operand than it was asked for.
pub(crate) struct UidOperand {
    uids: Vec<u32>,
    excluded: Vec<u32>,
    uid_set: Option<UidSetExpr>,
}

impl UidOperand {
    /// Split `uids` into the operand and the UIDs it cannot carry.
    pub(crate) fn build<I: IntoIterator<Item = u32>>(uids: I) -> Self {
        let mut kept = Vec::new();
        let mut excluded = Vec::new();
        let mut carried = Vec::new();
        for uid in uids {
            match operand_uid(uid) {
                Some(valid) => {
                    kept.push(uid);
                    carried.push(valid);
                }
                None => excluded.push(uid),
            }
        }
        Self {
            uids: kept,
            excluded,
            uid_set: UidSetExpr::from_uids(carried),
        }
    }

    /// The wire operand, or `None` when no UID reached it.
    pub(crate) fn uid_set(&self) -> Option<&UidSetExpr> {
        self.uid_set.as_ref()
    }

    /// The UIDs the operand carries, in request order.
    pub(crate) fn uids(&self) -> &[u32] {
        &self.uids
    }

    /// The UIDs the operand could not carry, in request order.
    pub(crate) fn excluded(&self) -> &[u32] {
        &self.excluded
    }
}

/// The typed identities one wire command is accountable for.
pub(crate) struct TargetBatch {
    /// Ids whose UID is in `uid_set`, in request order.
    targets: Vec<DecodedObjectId>,
    /// The UIDs of `targets`, positionally aligned with it.
    uids: Vec<u32>,
    /// The wire operand, built from `uids` only. `None` when there is no
    /// target to send.
    uid_set: Option<UidSetExpr>,
    settled: bool,
}

impl TargetBatch {
    /// Build the operand from the identities themselves. Zero is already
    /// unrepresentable, so no target can disappear during wire admission.
    pub(crate) fn new(ids: Vec<DecodedObjectId>) -> Self {
        let uids = ids.iter().map(|id| id.uid.get()).collect();
        let uid_set = UidSetExpr::from_uids(ids.iter().map(|id| id.uid));
        Self {
            targets: ids,
            uids,
            uid_set,
            settled: false,
        }
    }

    /// The wire operand, or `None` when no id reached it.
    pub(crate) fn uid_set(&self) -> Option<&UidSetExpr> {
        self.uid_set.as_ref()
    }

    /// The UIDs in the operand, in request order.
    pub(crate) fn uids(&self) -> &[u32] {
        &self.uids
    }

    /// A follow-up operand covering a subset of this batch's UIDs (the
    /// applied subset of a guarded STORE, the UIDs to EXPUNGE after a
    /// `\Deleted` mark). UIDs outside the batch are refused: a second wire
    /// command must never name a message this batch was not given.
    pub(crate) fn subset_uid_set(&self, uids: &[u32]) -> Option<UidSetExpr> {
        debug_assert!(
            uids.iter().all(|uid| self.uids.contains(uid)),
            "a follow-up operand may only name UIDs from its own batch"
        );
        UidSetExpr::from_uids(
            self.targets
                .iter()
                .map(|id| id.uid)
                .filter(|uid| uids.contains(&uid.get())),
        )
    }

    /// Classify and seal exactly one answer for every requested identity.
    pub(crate) fn settle<T, F>(
        self,
        _operation: AccountOperation,
        _folder: &MailboxName,
        mut classify: F,
    ) -> Vec<ItemOutcome<T>>
    where
        F: FnMut(Target) -> Sealed<T>,
    {
        self.settle_streaming()
            .into_iter()
            .map(|target| classify(target).0)
            .collect()
    }

    /// Settle a batch that sent no command because no id reached the wire
    /// operand.
    ///
    /// `UidSetExpr::from_uids` returns `None` only for an empty input, so
    /// `uid_set()` is `None` exactly when the target lane is empty and there
    /// is nothing to classify. The fallback classifier is unreachable for
    /// an empty batch and refuses a target if that invariant regresses.
    pub(crate) fn settle_unsent<T>(
        self,
        operation: AccountOperation,
        folder: &MailboxName,
    ) -> Vec<ItemOutcome<T>> {
        self.settle(operation, folder, |target| {
            target.seal(Verdict::Failed(excluded_from_operand_error(
                operation, folder,
            )))
        })
    }

    /// Take the target permits for streaming publication. A dropped walk may
    /// leave targets unresolved, but each permit can still answer at most once.
    pub(crate) fn settle_streaming(mut self) -> Vec<Target> {
        self.settled = true;
        std::mem::take(&mut self.targets)
            .into_iter()
            .map(Target::new)
            .collect()
    }
}

/// The verdict a classifier reaches for one target, without the id: the
/// [`Target`] stamps that, so an outcome can never name a message the batch
/// was not accountable for.
pub(crate) enum Verdict<T> {
    Succeeded(T),
    Failed(AccountError),
    SubmissionError(AccountError),
}

/// The permit to answer for exactly one id.
///
/// Not `Clone`, not `Copy`, and constructible only by [`TargetBatch`]. It is
/// consumed by [`Target::seal`], which is the only way to produce a
/// [`Sealed`] outcome.
pub(crate) struct Target {
    id: DecodedObjectId,
    item: BatchItemId,
}

impl Target {
    fn new(id: DecodedObjectId) -> Self {
        let item = item_id(&id);
        Self { id, item }
    }

    pub(crate) fn id(&self) -> &DecodedObjectId {
        &self.id
    }

    pub(crate) fn uid(&self) -> u32 {
        self.id.uid.get()
    }

    /// Spend the permit on a verdict, stamping the batch's own id onto it.
    pub(crate) fn seal<T>(self, verdict: Verdict<T>) -> Sealed<T> {
        Sealed(match verdict {
            Verdict::Succeeded(value) => {
                ItemOutcome::Succeeded(BatchSuccess::new(self.item, value))
            }
            Verdict::Failed(error) => ItemOutcome::Failed(BatchFailure::new(self.item, error)),
            Verdict::SubmissionError(error) => ItemOutcome::from_submission_error(self.item, error),
        })
    }
}

/// An outcome that provably came from a spent [`Target`]. The wrapper has no
/// other constructor, which is what makes "one outcome per permit" a fact
/// about the type rather than a count taken afterwards.
pub(crate) struct Sealed<T>(ItemOutcome<T>);

impl<T> Sealed<T> {
    /// Unwrap for a lane that publishes its outcomes itself (hydration's
    /// streaming settle) rather than returning them through [`TargetBatch`].
    pub(crate) fn into_outcome(self) -> ItemOutcome<T> {
        self.0
    }
}

impl Drop for TargetBatch {
    fn drop(&mut self) {
        // Never assert while another panic is unwinding: a panic in a drop
        // during unwind aborts the process, which would turn a clear test
        // failure into an unreadable one.
        if std::thread::panicking() {
            return;
        }
        debug_assert!(
            self.settled || self.targets.is_empty(),
            "TargetBatch dropped with {} target ids unaccounted for",
            self.targets.len()
        );
    }
}

fn item_id(id: &DecodedObjectId) -> BatchItemId {
    BatchItemId(id.id.0.clone())
}

#[cfg(test)]
mod tests {
    use bifrost_types::{AccountOperation, ItemOutcome, MutationSuccess};

    use super::{Sealed, Target, TargetBatch, UidOperand, Verdict, item_id};
    use crate::account::envelope::DecodedObjectId;
    use crate::types::MailboxName;

    fn folder() -> MailboxName {
        MailboxName::new("INBOX").expect("valid mailbox")
    }

    fn id(uid: u32) -> DecodedObjectId {
        DecodedObjectId::canonical(
            folder(),
            crate::types::UidValidity::new(7).expect("nonzero epoch"),
            crate::types::Uid::new(uid).expect("nonzero test UID"),
        )
    }

    fn applied(target: Target) -> Sealed<MutationSuccess> {
        target.seal(Verdict::Succeeded(MutationSuccess::Applied))
    }

    // `UidSetExpr` operands must never go out empty, and UID 0 is not a UID
    // (RFC 3501 Section 9), so an all-zero request yields no operand rather
    // than an empty or `0`-bearing sequence set - and the zeros are visible
    // in `excluded()` instead of vanishing.
    #[test]
    fn the_shared_operand_reports_what_it_could_not_carry() {
        let empty = UidOperand::build([]);
        assert!(empty.uid_set().is_none());
        assert!(empty.excluded().is_empty());

        let zeros = UidOperand::build([0, 0]);
        assert!(zeros.uid_set().is_none());
        assert_eq!(zeros.excluded(), &[0, 0], "a dropped UID stays visible");
        assert!(zeros.uids().is_empty());

        let mixed = UidOperand::build([3, 1, 2, 0, 2]);
        assert_eq!(
            mixed
                .uid_set()
                .expect("non-empty")
                .as_sequence_set()
                .as_str(),
            "1:3",
            "adjacent UIDs coalesce into a range and the 0 is dropped",
        );
        assert_eq!(mixed.uids(), &[3, 1, 2, 2]);
        assert_eq!(mixed.excluded(), &[0]);
    }

    // Typed targets and raw-number admission share the finite-set owner.
    #[test]
    fn the_batch_operand_is_the_shared_operand() {
        let uids = [4u32, 5, 6];
        let batch = TargetBatch::new(uids.iter().map(|uid| id(*uid)).collect());
        let shared = UidOperand::build(uids);
        assert_eq!(batch.uids(), shared.uids());
        assert_eq!(
            batch.uid_set().map(|set| set.as_sequence_set().as_str()),
            shared.uid_set().map(|set| set.as_sequence_set().as_str()),
        );
        assert_eq!(
            batch
                .subset_uid_set(&[6])
                .map(|set| set.as_sequence_set().as_str().to_owned()),
            UidOperand::build([6u32])
                .uid_set()
                .map(|set| set.as_sequence_set().as_str().to_owned()),
        );
        let _ = batch.settle(AccountOperation::UpdateFlags, &folder(), applied);
    }

    #[test]
    fn every_typed_target_is_carried_by_the_operand() {
        let batch = TargetBatch::new(vec![id(1), id(2), id(3)]);
        assert_eq!(batch.uids(), &[1, 2, 3]);
        assert_eq!(batch.uid_set().unwrap().to_string(), "1:3");
        let results = batch.settle(AccountOperation::UpdateFlags, &folder(), applied);
        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
        );
        assert!(crate::types::Uid::new(0).is_none());
    }

    #[test]
    fn every_requested_id_yields_exactly_one_outcome() {
        // Every typed target is represented and receives exactly one answer.
        let ids = vec![id(1), id(2), id(3)];
        let expected: Vec<_> = ids.iter().map(item_id).collect();
        let batch = TargetBatch::new(ids);
        let results = batch.settle(AccountOperation::BulkDestroy, &folder(), applied);
        let seen: Vec<_> = results
            .iter()
            .map(|outcome| match outcome {
                ItemOutcome::Succeeded(success) => success.item.clone(),
                ItemOutcome::Failed(failure) => failure.item.clone(),
                ItemOutcome::Uncertain(uncertain) => uncertain.item.clone(),
            })
            .collect();
        assert_eq!(seen.len(), expected.len());
        for want in &expected {
            assert_eq!(
                seen.iter().filter(|got| *got == want).count(),
                1,
                "each requested id is answered exactly once"
            );
        }
    }

    // Dropping or duplicating an id during classification is not tested
    // because it cannot be written: `settle` walks its own targets, a
    // `Target` is consumed by `seal`, and `Sealed` has no other constructor,
    // so a classifier that skipped or double-answered an id would not
    // compile. What is testable is that the outcome carries the batch's id
    // and not the classifier's idea of it.
    #[test]
    fn the_batch_stamps_the_id_the_classifier_answers_for() {
        let batch = TargetBatch::new(vec![id(11), id(12)]);
        let results = batch.settle(AccountOperation::UpdateFlags, &folder(), |target| {
            let uid = target.uid();
            assert_eq!(target.id().uid.get(), uid);
            target.seal(Verdict::Succeeded(MutationSuccess::Applied))
        });
        let seen: Vec<_> = results
            .iter()
            .map(|outcome| match outcome {
                ItemOutcome::Succeeded(success) => success.item.clone(),
                ItemOutcome::Failed(failure) => failure.item.clone(),
                ItemOutcome::Uncertain(uncertain) => uncertain.item.clone(),
            })
            .collect();
        assert_eq!(seen, vec![item_id(&id(11)), item_id(&id(12))]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "unaccounted for")]
    fn an_unsettled_batch_is_caught_on_drop() {
        let batch = TargetBatch::new(vec![id(1)]);
        drop(batch);
    }

    #[test]
    fn a_follow_up_operand_is_built_only_from_the_batch() {
        let batch = TargetBatch::new(vec![id(4), id(5)]);
        let set = batch.subset_uid_set(&[5]).expect("non-empty subset");
        assert_eq!(set.as_sequence_set().as_str(), "5");
        assert!(batch.subset_uid_set(&[]).is_none());
        // A UID outside the batch is filtered out of the operand in release
        // builds and caught by the debug assertion in debug builds; the
        // debug half is pinned by the panic test below.
        #[cfg(not(debug_assertions))]
        assert!(batch.subset_uid_set(&[9]).is_none());
        let _ = batch.settle(AccountOperation::UpdateFlags, &folder(), applied);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "only name UIDs from its own batch")]
    fn a_follow_up_operand_naming_a_foreign_uid_is_caught() {
        let batch = TargetBatch::new(vec![id(4)]);
        let _ = batch.subset_uid_set(&[9]);
    }

    #[test]
    fn a_streaming_settle_keeps_every_typed_target() {
        let batch = TargetBatch::new(vec![id(1), id(2)]);
        let targets = batch.settle_streaming();
        let ids: Vec<DecodedObjectId> = targets.iter().map(|target| target.id().clone()).collect();
        assert_eq!(ids, vec![id(1), id(2)]);
        // The permits still stamp the batch's own ids.
        let sealed: Vec<ItemOutcome<MutationSuccess>> = targets
            .into_iter()
            .map(|target| applied(target).into_outcome())
            .collect();
        assert_eq!(sealed.len(), 2);
    }
}
