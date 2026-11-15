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
            Error::AttemptHistory { source, evidence } => {
                for attempt in evidence.attempts() {
                    self.record_error(attempt, replayable);
                }
                self.record_error(source, replayable);
                self.retain_commit(evidence.verdict());
            }
            Error::LocalHold { evidence, .. } => {
                for attempt in evidence.attempts() {
                    self.record_error(attempt, replayable);
                }
                self.retain_commit(evidence.verdict());
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

    pub(crate) fn retain_commit(&mut self, commit: CommitEvidence) {
        if commit == CommitEvidence::MayHaveCommitted
            || (self.verdict == CommitEvidence::Unsent && commit == CommitEvidence::Acknowledged)
        {
            self.verdict = commit;
        }
    }
}

/// Owned diagnostic history coupled to the ledger that classifies it.
/// Callers can inspect outcomes but cannot mutate evidence independently of
/// its verdict. Nested historical envelopes are flattened at this boundary.
#[derive(Debug, Default)]
pub struct RequestEvidence {
    attempts: Vec<Error>,
    ledger: AttemptLedger,
}

impl RequestEvidence {
    /// Admit diagnostics and a typed verdict, retaining the strongest evidence.
    #[must_use]
    pub fn new(attempts: Vec<Error>, commit: CommitEvidence) -> Self {
        let mut evidence = Self::default();
        for attempt in attempts {
            evidence.push(attempt);
        }
        evidence.ledger.retain_commit(commit);
        evidence
    }

    pub(crate) fn from_ledger(attempts: Vec<Error>, mut ledger: AttemptLedger) -> Self {
        let mut evidence = Self::new(attempts, ledger.verdict());
        ledger.retain_commit(evidence.verdict());
        evidence.ledger = ledger;
        evidence
    }

    /// The ordered wire observations and their sticky verdict.
    #[must_use]
    pub fn ledger(&self) -> &AttemptLedger {
        &self.ledger
    }

    fn push(&mut self, error: Error) {
        match error {
            Error::AttemptHistory { source, evidence } => {
                self.ledger.retain_commit(evidence.verdict());
                for attempt in evidence.attempts {
                    self.push(attempt);
                }
                self.push(*source);
            }
            error => {
                self.ledger.record_error(&error, true);
                self.attempts.push(error);
            }
        }
    }

    /// Earlier diagnostic outcomes in wire order, without nested histories.
    #[must_use]
    pub fn attempts(&self) -> &[Error] {
        &self.attempts
    }

    /// The ledger's authoritative logical-request verdict.
    #[must_use]
    pub fn verdict(&self) -> CommitEvidence {
        self.ledger.verdict()
    }

    pub(crate) fn into_parts(self) -> (Vec<Error>, AttemptLedger) {
        (self.attempts, self.ledger)
    }

    pub(crate) fn redact_request_text(self) -> Self {
        Self {
            attempts: self
                .attempts
                .into_iter()
                .map(Error::redact_request_text)
                .collect(),
            ledger: self.ledger,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supplied_ledger_cannot_weaken_nested_commit_evidence() {
        let inner = RequestEvidence::new(Vec::new(), CommitEvidence::MayHaveCommitted);
        let evidence = RequestEvidence::from_ledger(
            vec![Error::AttemptHistory {
                source: Box::new(Error::Cancelled),
                evidence: inner,
            }],
            AttemptLedger::default(),
        );
        assert_eq!(evidence.verdict(), CommitEvidence::MayHaveCommitted);
    }

    #[test]
    fn public_evidence_flattens_history_without_weakening_its_verdict() {
        let inner = RequestEvidence::new(
            vec![Error::Status {
                code: StatusCode::SERVICE_UNAVAILABLE,
                headers: Default::default(),
                body: Default::default(),
            }],
            CommitEvidence::MayHaveCommitted,
        );
        let evidence = RequestEvidence::new(
            vec![Error::AttemptHistory {
                source: Box::new(Error::Timeout {
                    transmission_state: TransmissionState::Unsent,
                }),
                evidence: inner,
            }],
            CommitEvidence::Unsent,
        );
        assert_eq!(evidence.verdict(), CommitEvidence::MayHaveCommitted);
        assert!(matches!(
            evidence.attempts(),
            [Error::Status { .. }, Error::Timeout { .. }]
        ));
        assert_eq!(evidence.ledger().outcomes().len(), 2);
    }

    #[test]
    fn public_evidence_retains_acknowledgement_without_diagnostic_attempts() {
        let evidence = RequestEvidence::new(Vec::new(), CommitEvidence::Acknowledged);
        assert_eq!(
            evidence.ledger().transmission_state(),
            TransmissionState::Acknowledged
        );
    }

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
