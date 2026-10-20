use super::batch::{BatchFailure, BatchSuccess, BatchUncertain};
use crate::container::ContainerId;

/// Per-item outcome in a streaming bulk operation. The three-lane
/// model is closed by design: adding a fourth lane is a deliberate
/// breaking change, not a smooth extension. Wildcard arms would let
/// stale consumer policy silently apply to new lanes and lose audit
/// signal.
#[derive(Clone, Debug)]
pub enum ItemOutcome<T> {
    Succeeded(BatchSuccess<T>),
    Failed(BatchFailure),
    Uncertain(BatchUncertain),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutationSuccess {
    /// The requested mutation was performed as asked.
    Applied,
    /// Nothing was done, and nothing needed to be: the target was already
    /// in the requested state.
    Skipped,
    /// Something was done, but NOT what was asked - the provider accepted a
    /// weaker form of the operation.
    ///
    /// Distinct from `Applied` because the target is not in the requested
    /// state, and from `Skipped` because the target DID change. Reporting
    /// either in its place is a wrong answer a consumer cannot detect: claiming
    /// `Applied` makes an engine believe a state it will keep re-observing as
    /// false, and claiming `Skipped` hides a mutation that really happened.
    ///
    /// "The target DID change" is a precondition, not a description: an
    /// operation a provider could not perform AT ALL is not a downgrade and
    /// must not be reported here. A protocol crate that finds nothing it can
    /// express owes the caller a failure lane, not the weakest available
    /// success. `bifrost-sync` counts `Downgraded` in its own
    /// `MutationCounters::downgraded` bucket, retains the reported effect in
    /// `MutationCounters::effects`, and excludes the target from read-back.
    /// A no-op reported here would be miscounted as a successful weaker
    /// mutation and bypass observation that could expose the non-landing.
    ///
    /// The motivating case is Gmail `bulk_destroy` under the `gmail.modify`
    /// OAuth scope, which does not permit permanent delete. `messages/batchDelete`
    /// answers 403 and the account falls back to a TRASH label patch: the
    /// messages move, they do not cease to exist. Reported as `Applied`, they
    /// came back on the next inventory pass and were destroyed again forever.
    ///
    /// `bifrost-sync` retains this reported effect in
    /// `MutationCounters::effects` and counts the target as downgraded. It
    /// does not retry the target or read it back against the original request.
    Downgraded { actual: MutationEffect },
}

/// The weaker mutation a provider actually applied.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutationEffect {
    MovedToContainer(ContainerId),
    /// Only the provider-representable subset of a requested flag operation
    /// was applied; the listed flags were not.
    ///
    /// Requires a non-empty representable subset that actually reached the
    /// provider. An operation whose flags are ALL unrepresentable changed
    /// nothing and belongs in the failure lane, classified
    /// `Unsupported(operation)`, not here.
    FlagsPartiallyApplied {
        unsupported: Vec<String>,
    },
}
