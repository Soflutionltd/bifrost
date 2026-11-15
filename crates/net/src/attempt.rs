//! Logical-request transmission evidence shared with self-walking callers.
use crate::{Error, TransmissionState};
use reqwest::StatusCode;

/// An observed wire outcome, independent of translated diagnostic causes.
#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub enum AttemptOutcome {
    Response(StatusCode),
    Transport(TransmissionState),
}

/// Whether the logical exchange contains work whose result needs reconciliation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CommitEvidence {
    #[default]
    Unsent,
    Acknowledged,
    MayHaveCommitted,
}

/// Ordered wire observations and their sticky logical-request verdict.
#[derive(Clone, Debug, Default)]
pub struct AttemptLedger {
    outcomes: Vec<AttemptOutcome>,
    verdict: CommitEvidence,
}

/// Explicit authentication, quota and unsupported-method refusals prove no work.
/// A 503 can follow a commit at an upstream server. Enterprise mail users must
/// never duplicate a possibly committed write, but must be able to retry work
/// proven not to have happened. Replayable attempts do not create ambiguity for
/// a later unsent failure; their caller explicitly permits replay.
#[must_use]
pub fn is_proven_refusal(status: StatusCode) -> bool {
    matches!(status.as_u16(), 401 | 429 | 405 | 501)
}

impl AttemptLedger {
    pub fn record(&mut self, outcome: AttemptOutcome, replayable: bool) {
        let state = match outcome {
            AttemptOutcome::Response(status) => {
                if !replayable && !is_proven_refusal(status) {
                    self.verdict = CommitEvidence::MayHaveCommitted;
                }
                TransmissionState::Acknowledged
            }
            AttemptOutcome::Transport(state) => {
                if !replayable && state != TransmissionState::Unsent {
                    self.verdict = CommitEvidence::MayHaveCommitted;
                }
                state
            }
        };
        if self.verdict == CommitEvidence::Unsent && state != TransmissionState::Unsent {
            self.verdict = CommitEvidence::Acknowledged;
        }
        self.outcomes.push(outcome);
    }

    #[must_use]
    pub fn verdict(&self) -> CommitEvidence {
        self.verdict
    }

    #[must_use]
    pub fn outcomes(&self) -> &[AttemptOutcome] {
        &self.outcomes
    }

    #[must_use]
    pub fn transmission_state(&self) -> TransmissionState {
        match self.verdict {
            CommitEvidence::Unsent => TransmissionState::Unsent,
            CommitEvidence::Acknowledged => TransmissionState::Acknowledged,
            CommitEvidence::MayHaveCommitted => TransmissionState::InFlight,
        }
    }

    // Read raw transport evidence, never the account-error diagnostic chain.
    pub(crate) fn record_error(&mut self, error: &Error, replayable: bool) {
        match error {
            Error::AttemptHistory {
                source,
                attempts,
                may_have_committed,
            } => {
                for attempt in attempts {
                    self.record_error(attempt, replayable);
                }
                self.record_error(source, replayable);
                if *may_have_committed {
                    self.verdict = CommitEvidence::MayHaveCommitted;
                }
            }
            Error::LocalHold {
                attempts,
                may_have_committed,
                ..
            } => {
                for attempt in attempts {
                    self.record_error(attempt, replayable);
                }
                if *may_have_committed {
                    self.verdict = CommitEvidence::MayHaveCommitted;
                }
            }
            Error::Network {
                transmission_state, ..
            }
            | Error::Timeout { transmission_state }
            | Error::Tls {
                transmission_state, ..
            } => self.record(AttemptOutcome::Transport(*transmission_state), replayable),
            Error::Status { code, .. } => self.record(AttemptOutcome::Response(*code), replayable),
            Error::UnexpectedResponse { response } => {
                self.record(AttemptOutcome::Response(response.status), replayable);
            }
            Error::RetryBudgetExhausted {
                final_response: Some(response),
                ..
            }
            | Error::RateLimited {
                final_response: response,
                ..
            }
            | Error::UnauthenticatedRedirectHop {
                final_response: response,
                ..
            } => self.record(AttemptOutcome::Response(response.status), replayable),
            Error::AuthLost {
                transmission_state: Some(state),
                final_response,
            } => {
                if let Some(response) = final_response {
                    self.record(AttemptOutcome::Response(response.status), replayable);
                } else {
                    self.record(AttemptOutcome::Transport(*state), replayable);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn retain_possible_commit(&mut self, possible: bool) {
        if possible {
            self.verdict = CommitEvidence::MayHaveCommitted;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_and_replay_policy_decide_commit_evidence() {
        for status in [401, 429, 405, 501] {
            let mut ledger = AttemptLedger::default();
            ledger.record(
                AttemptOutcome::Response(StatusCode::from_u16(status).unwrap()),
                false,
            );
            assert_eq!(ledger.verdict(), CommitEvidence::Acknowledged);
        }
        let mut ledger = AttemptLedger::default();
        ledger.record(
            AttemptOutcome::Response(StatusCode::SERVICE_UNAVAILABLE),
            false,
        );
        ledger.record(AttemptOutcome::Transport(TransmissionState::Unsent), false);
        assert_eq!(ledger.verdict(), CommitEvidence::MayHaveCommitted);
        let mut ledger = AttemptLedger::default();
        ledger.record(AttemptOutcome::Response(StatusCode::SEE_OTHER), true);
        ledger.record(AttemptOutcome::Transport(TransmissionState::Unsent), true);
        assert_eq!(ledger.verdict(), CommitEvidence::Acknowledged);
        ledger.record(AttemptOutcome::Response(StatusCode::SEE_OTHER), false);
        ledger.record(AttemptOutcome::Response(StatusCode::UNAUTHORIZED), true);
        assert_eq!(ledger.verdict(), CommitEvidence::MayHaveCommitted);
        assert_eq!(ledger.outcomes().len(), 4);
    }
}
