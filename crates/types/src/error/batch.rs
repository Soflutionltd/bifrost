use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::Serialize;

use super::account_error::AccountError;
use super::builder::AccountErrorBuilder;
use super::cause::{BatchInputInvalidItem, BatchInputInvalidReason, PrimaryCause, RequestCause};

use super::scope::AccountOperation;
use crate::Protocol;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchItem<I> {
    pub id: BatchItemId,
    pub input: I,
}

impl<I> BatchItem<I> {
    #[must_use]
    pub fn new(id: BatchItemId, input: I) -> Self {
        Self { id, input }
    }
}

/// Collected outcomes keyed by submission identity. The default `BatchItemId`
/// belongs to caller-minted `Vec<BatchItem<_>>` inputs; object reads use
/// `ObjectId`, and positional ledgers use `SubmissionIndex`.
/// Immutable: lanes are populated via `BatchOutcomeBuilder` and frozen
/// by `finalize`. The three lanes are closed by design - adding a
/// fourth lane is a deliberate breaking change.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchOutcome<T, K = BatchItemId> {
    succeeded: Vec<BatchSuccess<T, K>>,
    failed: Vec<BatchFailure<K>>,
    uncertain: Vec<BatchUncertain<K>>,
    order: Vec<BatchLane>,
}

impl<T, K> BatchOutcome<T, K> {
    #[must_use]
    pub fn succeeded(&self) -> &[BatchSuccess<T, K>] {
        &self.succeeded
    }

    #[must_use]
    pub fn failed(&self) -> &[BatchFailure<K>] {
        &self.failed
    }

    #[must_use]
    pub fn uncertain(&self) -> &[BatchUncertain<K>] {
        &self.uncertain
    }

    /// Attach caller payloads to a closed ledger without changing lanes or order.
    /// The mapper is called once per entry; it must preserve submission identity.
    #[must_use]
    pub fn map_keys<L>(self, mut map: impl FnMut(K) -> L) -> BatchOutcome<T, L> {
        BatchOutcome {
            succeeded: self
                .succeeded
                .into_iter()
                .map(|entry| BatchSuccess::new(map(entry.item), entry.output))
                .collect(),
            failed: self
                .failed
                .into_iter()
                .map(|entry| BatchFailure::new(map(entry.item), entry.error))
                .collect(),
            uncertain: self
                .uncertain
                .into_iter()
                .map(|entry| BatchUncertain::new(map(entry.item), entry.error))
                .collect(),
            order: self.order,
        }
    }

    /// Iterate over per-item outcomes in builder push order.
    pub fn iter(&self) -> impl Iterator<Item = BatchItemOutcome<'_, T, K>> {
        self.order.iter().map(|lane| match lane {
            BatchLane::Succeeded(index) => BatchItemOutcome::Succeeded(
                self.succeeded
                    .get(*index)
                    .expect("BatchOutcome order references existing success"),
            ),
            BatchLane::Failed(index) => BatchItemOutcome::Failed(
                self.failed
                    .get(*index)
                    .expect("BatchOutcome order references existing failure"),
            ),
            BatchLane::Uncertain(index) => BatchItemOutcome::Uncertain(
                self.uncertain
                    .get(*index)
                    .expect("BatchOutcome order references existing uncertainty"),
            ),
        })
    }
}

/// Mutable builder for [`BatchOutcome`]. Protocol crates push per-item
/// results as they classify them, then call [`finalize`](BatchOutcomeBuilder::finalize)
/// with the original submitted ids. `finalize` validates that every
/// submitted id appears exactly once across the three lanes and
/// returns the immutable outcome.
#[derive(Clone, Debug)]
pub struct BatchOutcomeBuilder<T, K = BatchItemId> {
    succeeded: Vec<BatchSuccess<T, K>>,
    failed: Vec<BatchFailure<K>>,
    uncertain: Vec<BatchUncertain<K>>,
    order: Vec<BatchLane>,
}

impl<T, K> Default for BatchOutcomeBuilder<T, K> {
    fn default() -> Self {
        Self {
            succeeded: Vec::new(),
            failed: Vec::new(),
            uncertain: Vec::new(),
            order: Vec::new(),
        }
    }
}

impl<T, K> BatchOutcomeBuilder<T, K> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_succeeded(&mut self, item: K, output: T) {
        let index = self.succeeded.len();
        self.succeeded.push(BatchSuccess::new(item, output));
        self.order.push(BatchLane::Succeeded(index));
    }

    pub fn push_failed(&mut self, item: K, error: AccountError) {
        let index = self.failed.len();
        self.failed.push(BatchFailure::new(item, error));
        self.order.push(BatchLane::Failed(index));
    }

    pub fn push_uncertain(&mut self, item: K, error: AccountError) {
        let index = self.uncertain.len();
        self.uncertain.push(BatchUncertain::new(item, error));
        self.order.push(BatchLane::Uncertain(index));
    }

    /// Freeze the builder into an immutable [`BatchOutcome`]. Validates
    /// that every SUBMISSION in `expected` receives exactly one lane
    /// entry, and that no unknown ids were added.
    ///
    /// `expected` is a list of submissions, not a set of ids: an id listed
    /// twice was submitted twice and requires two lane entries. Treating it
    /// as a set let one outcome discharge both copies, so a real item
    /// silently received no outcome at all. Producers whose ids can repeat
    /// (`push_subscribe`, where two requested scopes may name the same
    /// folder) already key on submission position, so they are unaffected.
    /// Returns `Err(BatchInvariantError<K>)` on any violation; protocol
    /// crates with bugs that miscount items surface them here rather
    /// than silently shipping a wrong outcome.
    pub fn finalize(self, expected: &[K]) -> Result<BatchOutcome<T, K>, BatchInvariantError<K>>
    where
        K: Clone + Eq + std::hash::Hash,
    {
        // Index `expected` once. The membership test used to be a
        // linear `expected.iter().any(..)` of `String` comparisons
        // inside the per-item loop, i.e. O(n*m) on a hot mutation path:
        // a 1000-item Gmail batch cost roughly a million string
        // compares to prove an invariant that is almost always
        // satisfied.
        let mut remaining: HashMap<&K, usize> = HashMap::new();
        for id in expected {
            *remaining.entry(id).or_default() += 1;
        }
        let known: HashSet<&K> = remaining.keys().copied().collect();
        let mut duplicates: Vec<K> = Vec::new();
        let mut unknown: Vec<K> = Vec::new();

        for id in self
            .succeeded
            .iter()
            .map(|s| &s.item)
            .chain(self.failed.iter().map(|f| &f.item))
            .chain(self.uncertain.iter().map(|u| &u.item))
        {
            if !known.contains(id) {
                unknown.push(id.clone());
                continue;
            }
            let count = remaining.get_mut(id).expect("known id has an expectation");
            if *count == 0 {
                duplicates.push(id.clone());
            } else {
                *count -= 1;
            }
        }

        let mut missing = Vec::new();
        for id in expected {
            let count = remaining.get_mut(id).expect("expected id is indexed");
            if *count != 0 {
                missing.push(id.clone());
                *count -= 1;
            }
        }

        if !missing.is_empty() || !duplicates.is_empty() || !unknown.is_empty() {
            return Err(BatchInvariantError {
                missing,
                duplicates,
                unknown,
            });
        }

        Ok(BatchOutcome {
            succeeded: self.succeeded,
            failed: self.failed,
            uncertain: self.uncertain,
            order: self.order,
        })
    }
}

/// Invariant violation detected by `BatchOutcomeBuilder::finalize`.
/// Producer-bug surface: every submitted item must appear in exactly
/// one lane. Tests inside protocol crates catch this before it ships.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchInvariantError<K = BatchItemId> {
    /// Submitted ids that did not appear in any lane.
    pub missing: Vec<K>,
    /// Ids that appeared in more than one lane.
    pub duplicates: Vec<K>,
    /// Ids appearing in a lane that were not in the submitted vec.
    pub unknown: Vec<K>,
}

impl<K> fmt::Display for BatchInvariantError<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BatchOutcome invariant violation: missing={} duplicates={} unknown={}",
            self.missing.len(),
            self.duplicates.len(),
            self.unknown.len(),
        )
    }
}

impl<K: fmt::Debug> std::error::Error for BatchInvariantError<K> {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchLane {
    Succeeded(usize),
    Failed(usize),
    Uncertain(usize),
}

#[derive(Clone, Debug)]
pub enum BatchItemOutcome<'a, T, K = BatchItemId> {
    Succeeded(&'a BatchSuccess<T, K>),
    Failed(&'a BatchFailure<K>),
    Uncertain(&'a BatchUncertain<K>),
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchSuccess<T, K = BatchItemId> {
    pub item: K,
    pub output: T,
}

impl<T, K> BatchSuccess<T, K> {
    #[must_use]
    pub fn new(item: K, output: T) -> Self {
        Self { item, output }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchFailure<K = BatchItemId> {
    pub item: K,
    pub error: AccountError,
}

impl<K> BatchFailure<K> {
    #[must_use]
    pub fn new(item: K, error: AccountError) -> Self {
        Self { item, error }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchUncertain<K = BatchItemId> {
    pub item: K,
    pub error: AccountError,
}

impl<K> BatchUncertain<K> {
    #[must_use]
    pub fn new(item: K, error: AccountError) -> Self {
        Self { item, error }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct BatchItemId(pub String);

/// Zero-based position in the original submission, including repeated inputs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct SubmissionIndex(pub usize);

/// Pre-flight validation for Vec-batch input. Protocol crates that build a
/// `Vec<BatchItem<_>>` interface must call this before any byte crosses the
/// side-effect boundary so empty/duplicate identifiers surface as
/// `Err(AccountError { kind: Request(BatchInputInvalid), .. })` rather than
/// silently splitting the caller's intent.
///
/// `operation` is not decoration. It is the field telemetry and support
/// exports identify the rejected operation by, and it is also what
/// `AccountOperation::is_idempotent` reads to derive retry safety, so a
/// caller that omitted it would report an unattributed client bug. It is a
/// required parameter rather than a post-hoc decoration precisely because
/// centralizing the construction here is what made losing it possible.
pub fn validate_batch_input<I>(
    items: &[BatchItem<I>],
    protocol: Protocol,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if items.is_empty() {
        return Err(batch_input_error(
            protocol,
            operation,
            RequestCause::BatchInputEmpty,
        ));
    }

    let mut seen = HashSet::new();
    let mut invalid = Vec::new();
    for item in items {
        if item.id.0.is_empty() {
            invalid.push(BatchInputInvalidItem {
                id: item.id.clone(),
                reason: BatchInputInvalidReason::Empty,
            });
            continue;
        }
        if !seen.insert(item.id.clone()) {
            invalid.push(BatchInputInvalidItem {
                id: item.id.clone(),
                reason: BatchInputInvalidReason::Duplicate,
            });
        }
    }

    if invalid.is_empty() {
        Ok(())
    } else {
        Err(batch_input_error(
            protocol,
            operation,
            RequestCause::BatchInputInvalid { items: invalid },
        ))
    }
}

fn batch_input_error(
    protocol: Protocol,
    operation: AccountOperation,
    cause: RequestCause,
) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Request(cause))
        .protocol(protocol)
        .operation(operation)
        .try_build()
        .expect("valid batch input error classification")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AccountErrorBuilder, Cause, DiagnosticText, PrimaryCause, RequestCause};

    fn error() -> AccountError {
        AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("bad input"),
        }))
        .try_build()
        .expect("valid account error classification")
    }

    #[test]
    fn typed_keys_and_payload_mapping_preserve_all_lanes_and_order() {
        let expected = [SubmissionIndex(2), SubmissionIndex(0), SubmissionIndex(1)];
        let mut builder = BatchOutcomeBuilder::new();
        builder.push_uncertain(expected[0], error());
        builder.push_succeeded(expected[1], "delivered");
        builder.push_failed(expected[2], error());
        let outcome = builder
            .finalize(&expected)
            .unwrap()
            .map_keys(|key| BatchItem::new(BatchItemId(format!("opaque-{}", key.0)), key));
        assert_eq!(outcome.succeeded()[0].output, "delivered");
        assert_eq!(outcome.succeeded()[0].item.input, SubmissionIndex(0));
        assert_eq!(outcome.failed()[0].item.input, SubmissionIndex(1));
        assert_eq!(outcome.uncertain()[0].item.input, SubmissionIndex(2));
        let order: Vec<_> = outcome
            .iter()
            .map(|entry| match entry {
                BatchItemOutcome::Succeeded(entry) => entry.item.input,
                BatchItemOutcome::Failed(entry) => entry.item.input,
                BatchItemOutcome::Uncertain(entry) => entry.item.input,
            })
            .collect();
        assert_eq!(order, expected);
    }

    fn ids(strs: &[&str]) -> Vec<BatchItemId> {
        strs.iter().map(|s| BatchItemId(s.to_string())).collect()
    }

    #[test]
    fn iter_preserves_push_order() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_failed(BatchItemId("b".to_string()), error());
        builder.push_succeeded(BatchItemId("a".to_string()), ());
        builder.push_uncertain(BatchItemId("c".to_string()), error());

        let outcome = builder.finalize(&ids(&["b", "a", "c"])).unwrap();

        let lanes = outcome
            .iter()
            .map(|item| match item {
                BatchItemOutcome::Succeeded(success) => success.item.0.as_str(),
                BatchItemOutcome::Failed(failure) => failure.item.0.as_str(),
                BatchItemOutcome::Uncertain(uncertain) => uncertain.item.0.as_str(),
            })
            .collect::<Vec<_>>();

        assert_eq!(lanes, ["b", "a", "c"]);
    }

    #[test]
    fn lane_vectors_preserve_lane_order() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_succeeded(BatchItemId("a".to_string()), ());
        builder.push_failed(BatchItemId("b".to_string()), error());
        builder.push_succeeded(BatchItemId("c".to_string()), ());

        let outcome = builder.finalize(&ids(&["a", "b", "c"])).unwrap();

        assert_eq!(outcome.succeeded()[0].item.0, "a");
        assert_eq!(outcome.succeeded()[1].item.0, "c");
        assert_eq!(outcome.failed()[0].item.0, "b");
    }

    #[test]
    fn finalize_detects_missing_duplicate_and_unknown_ids() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_succeeded(BatchItemId("a".to_string()), ());
        builder.push_succeeded(BatchItemId("a".to_string()), ()); // duplicate
        builder.push_succeeded(BatchItemId("z".to_string()), ()); // unknown
        // missing: "b"

        let err = builder.finalize(&ids(&["a", "b"])).expect_err("invalid");
        assert_eq!(err.missing, ids(&["b"]));
        assert_eq!(err.duplicates, ids(&["a"]));
        assert_eq!(err.unknown, ids(&["z"]));
    }

    /// `finalize` indexes `expected` into a `HashSet` rather than
    /// rescanning it per item. A caller that submitted a duplicate id
    /// must reach the same verdict it did under the linear scan: the id
    /// is known, so it is not `unknown`, and one lane entry satisfies
    /// both copies of the expectation rather than reporting `missing`.
    #[test]
    fn duplicated_expectations_require_one_outcome_each() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_succeeded(BatchItemId("a".to_string()), ());

        let err = builder
            .finalize(&ids(&["a", "a"]))
            .expect_err("one submitted item is missing");
        assert_eq!(err.missing, ids(&["a"]));
    }

    #[test]
    fn validate_rejects_empty_and_duplicate_ids() {
        let items = vec![
            BatchItem {
                id: BatchItemId(String::new()),
                input: (),
            },
            BatchItem {
                id: BatchItemId("a".to_string()),
                input: (),
            },
            BatchItem {
                id: BatchItemId("a".to_string()),
                input: (),
            },
        ];

        let error = validate_batch_input(&items, Protocol::Smtp, AccountOperation::Send)
            .expect_err("input must be invalid");
        let Cause::Request(RequestCause::BatchInputInvalid { items }) = error.chain().outermost()
        else {
            panic!("item diagnostics expected");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].reason, BatchInputInvalidReason::Empty);
        assert_eq!(items[1].reason, BatchInputInvalidReason::Duplicate);
    }

    /// Centralizing the error construction here is exactly what made it
    /// possible to drop the operation the caller was performing. Both
    /// rejection shapes must carry it, or telemetry and support exports
    /// cannot say which operation refused the input, and the derived
    /// idempotency is wrong on top of that.
    #[test]
    fn both_rejection_shapes_carry_the_operation_and_protocol() {
        let empty: Vec<BatchItem<()>> = Vec::new();
        let duplicated = [
            BatchItem {
                id: BatchItemId("a".to_string()),
                input: (),
            },
            BatchItem {
                id: BatchItemId("a".to_string()),
                input: (),
            },
        ];

        for items in [&empty[..], &duplicated[..]] {
            let error = validate_batch_input(items, Protocol::Lmtp, AccountOperation::Send)
                .expect_err("input must be invalid");
            assert_eq!(error.operation(), Some(AccountOperation::Send));
            assert_eq!(error.protocol(), Some(Protocol::Lmtp));
        }
    }
}
