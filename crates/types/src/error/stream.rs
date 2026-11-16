use super::batch::{BatchFailure, BatchItemId, BatchSuccess, BatchUncertain};
use super::{AccountError, Application};
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

impl<T> ItemOutcome<T> {
    /// Attribute a whole-submission failure to one submitted item.
    ///
    /// Transmission ambiguity is independent of retry advice: an idempotent
    /// in-flight write can be retryable and still have applied. For a compound
    /// write, the recorded primary leg decides this lane; earlier commits do
    /// not turn an acknowledged refusal of that leg into an uncertain answer.
    /// An enterprise mail user expects ambiguous writes to be checked, while
    /// an explicit refusal remains a failure even after an earlier effect.
    pub fn from_submission_error(item: BatchItemId, error: AccountError) -> Self {
        let application = error.partial_commit().map_or_else(
            || Application::from_leaf(&error),
            |record| record.primary().application,
        );
        if application == Application::Uncertain {
            Self::Uncertain(BatchUncertain::new(item, error))
        } else {
            Self::Failed(BatchFailure::new(item, error))
        }
    }
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
    /// Distinct from `Applied` because the requested operation was not the
    /// one performed, and from `Skipped` because the provider acknowledged a
    /// real write. Reporting either in its place is a wrong answer a consumer
    /// cannot detect: claiming `Applied` makes an engine believe a state it
    /// will keep re-observing as false, and claiming `Skipped` hides a
    /// mutation that really happened.
    ///
    /// An acknowledged, non-empty weaker write is a precondition. It proves
    /// the provider accepted the weaker operation, not that the target's
    /// observable state moved: a weaker flag write against a message already
    /// in that state is still `Downgraded`. An operation a provider could not
    /// perform AT ALL is not a downgrade and must not be reported here. A protocol crate that finds nothing it can
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
    /// Requires a non-empty representable write that actually reached the
    /// provider and was acknowledged. That write need not name a supplied
    /// flag: a full-replace `Set` naming only unrepresentable flags still
    /// clears every flag the provider does model. An operation that projects
    /// onto NO write changed nothing and belongs in the failure lane,
    /// classified `Unsupported(operation)`, not here.
    FlagsPartiallyApplied {
        unsupported: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        AccountErrorBuilder, AccountOperation, AttemptCause, Cause, PrimaryCause,
        ProtocolErrorKind, ServerCause, TransmissionState,
    };

    #[test]
    fn submission_lane_uses_classification_evidence_not_retry_or_decoration() {
        for (attempt, uncertain) in [
            (AttemptCause::new(TransmissionState::Unsent), false),
            (AttemptCause::new(TransmissionState::Acknowledged), false),
            (AttemptCause::new(TransmissionState::InFlight), true),
        ] {
            let error = AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::Unavailable {
                retry_hint: None,
            }))
            .operation(AccountOperation::UpdateFlags)
            .push_cause(Cause::Attempt(attempt))
            .try_build()
            .expect("valid submission error");
            assert!(error.recovery().is_retryable());
            // Historical attempt evidence cannot change this submission's lane.
            let error = error
                .into_builder()
                .push_cause(Cause::Attempt(AttemptCause::new(
                    TransmissionState::InFlight,
                )))
                .try_build()
                .expect("valid decorated submission error");
            let outcome =
                ItemOutcome::<()>::from_submission_error(BatchItemId("item".into()), error);
            assert_eq!(matches!(outcome, ItemOutcome::Uncertain(_)), uncertain);
        }

        let error = AccountErrorBuilder::new(PrimaryCause::Protocol {
            kind: ProtocolErrorKind::ParseFailed,
            wire: crate::error::WireCause::MalformedResponse {
                protocol: crate::error::Protocol::Graph,
                detail: Some(crate::error::DiagnosticText::support_only(
                    "unreadable success",
                )),
            },
        })
        .push_cause(Cause::Attempt(AttemptCause::unreadable_success()))
        .try_build()
        .expect("valid unreadable success");
        assert!(matches!(
            ItemOutcome::<()>::from_submission_error(BatchItemId("item".into()), error),
            ItemOutcome::Uncertain(_)
        ));
    }
}
