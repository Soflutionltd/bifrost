//! Engine-owned consecutive-failure pacing.
use bifrost_types::BackoffPolicy;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) struct ReconnectBackoff {
    policy: BackoffPolicy,
    failures: u32,
}

impl ReconnectBackoff {
    pub(crate) fn new(policy: BackoffPolicy) -> Self {
        Self::with_failures(policy, 0)
    }

    pub(crate) fn with_failures(policy: BackoffPolicy, failures: u32) -> Self {
        Self {
            policy: BackoffPolicy::new(policy.initial(), policy.max(), policy.jitter()),
            failures,
        }
    }

    pub(crate) fn carried_delay(&self) -> Duration {
        self.policy.base_delay(self.failures)
    }
    pub(crate) fn reset(&mut self) {
        self.failures = 0;
    }
    pub(crate) fn failures(&self) -> u32 {
        self.failures
    }

    pub(crate) fn record_failure(&mut self, jitter_seed: u64) -> Duration {
        self.failures = self.failures().saturating_add(1);
        let base = self.carried_delay();
        if self.policy.jitter() {
            let quarter = u64::try_from(base.as_millis() / 4).unwrap_or(u64::MAX - 1);
            base.saturating_add(Duration::from_millis(
                jitter_seed % quarter.saturating_add(1),
            ))
        } else {
            base
        }
    }
}

pub(crate) fn jitter_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy(jitter: bool) -> BackoffPolicy {
        BackoffPolicy::new(Duration::from_secs(1), Duration::from_secs(4), jitter)
    }
    #[test]
    fn reconnect_backoff_records_failures_and_resets() {
        let mut backoff = ReconnectBackoff::new(policy(false));
        for expected in [1, 2, 4, 4] {
            assert_eq!(backoff.record_failure(0), Duration::from_secs(expected));
        }
        assert_eq!(backoff.failures(), 4);
        backoff.reset();
        assert_eq!(backoff.record_failure(0), Duration::from_secs(1));
    }
    #[test]
    fn reconnect_backoff_jitter_stays_within_a_quarter() {
        for seed in [0, 1, 250, 251, u64::MAX] {
            let mut backoff = ReconnectBackoff::new(policy(true));
            let delay = backoff.record_failure(seed);
            assert!((Duration::from_secs(1)..=Duration::from_millis(1250)).contains(&delay));
        }
    }
    #[test]
    fn reconnect_backoff_carried_count_earns_its_delay_and_zero_earns_none() {
        assert_eq!(
            ReconnectBackoff::with_failures(policy(false), 0).carried_delay(),
            Duration::ZERO
        );
        assert_eq!(
            ReconnectBackoff::with_failures(policy(false), 3).carried_delay(),
            Duration::from_secs(4)
        );
    }
}
