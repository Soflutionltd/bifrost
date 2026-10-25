//! Retry policy for transient HTTP failures.
//!
//! One policy applies to every request unless the caller overrides
//! via `RequestBuilder::retry`. The defaults cover the union of what
//! Gmail and Graph were doing before the consolidation: 429 plus the
//! 5xx family, three attempts, jittered exponential backoff, and a
//! capped honor for `Retry-After`.

use std::time::Duration;

use reqwest::StatusCode;

/// Per-request retry policy. Used by the transport's retry loop to
/// decide whether to retry, how long to wait, and when to surface a
/// `RetryBudgetExhausted` error.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Maximum number of attempts including the initial one. A value
    /// of 1 disables retry. Replay policy may stop before this upper bound.
    pub max_attempts: u32,
    /// Initial backoff before the second attempt. Subsequent attempts
    /// use decorrelated jitter capped by `max_backoff`.
    pub initial_backoff: Duration,
    /// Hard cap on any single backoff sleep.
    pub max_backoff: Duration,
    /// Hard cap on any single `Retry-After` server hint. Servers
    /// occasionally return absurd values during partial outage; the
    /// cap prevents multi-hour stalls.
    pub honor_retry_after_cap: Duration,
    /// Additional retry candidates on top of the 5xx family. The resolved
    /// request replay policy still gates retries, except for explicit 401 and
    /// 429 refusals. Listing a status does not make replay safe.
    pub statuses: Vec<StatusCode>,
    /// Whether to retry transport-level failures (connect, reset,
    /// idle timeout, TLS handshake).
    pub network_errors: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            honor_retry_after_cap: Duration::from_secs(60),
            statuses: vec![
                StatusCode::TOO_MANY_REQUESTS,
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusCode::BAD_GATEWAY,
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::GATEWAY_TIMEOUT,
            ],
            network_errors: true,
        }
    }
}

impl RetryPolicy {
    /// Build a `RetryPolicy` with the defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable retries entirely. Useful for one-shot operations where
    /// the caller wants the raw status surfaced.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_matches_the_documented_shape() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_backoff, Duration::from_secs(1));
        assert_eq!(policy.max_backoff, Duration::from_secs(60));
        assert_eq!(policy.honor_retry_after_cap, Duration::from_secs(60));
        assert!(policy.network_errors);
        assert!(
            policy.statuses.contains(&StatusCode::TOO_MANY_REQUESTS),
            "429 is the one 4xx the default retries"
        );
        assert_eq!(
            RetryPolicy::new().max_attempts,
            RetryPolicy::default().max_attempts
        );
    }

    /// `statuses` is documented as the *additive* set on top of an
    /// unconditional `is_server_error()`. Pin that the only non-5xx
    /// member is 429, so a reader cannot mistake the list for the
    /// complete retry set.
    #[test]
    fn the_only_non_server_error_in_the_default_set_is_429() {
        let policy = RetryPolicy::default();
        let non_5xx: Vec<StatusCode> = policy
            .statuses
            .iter()
            .filter(|status| !status.is_server_error())
            .copied()
            .collect();
        assert_eq!(non_5xx, vec![StatusCode::TOO_MANY_REQUESTS]);
    }

    /// `disabled()` sets `max_attempts = 1` rather than emptying
    /// `statuses`: the retry loop's exhausted-budget branch is what
    /// surfaces the failure, so a 503 still becomes
    /// `RetryBudgetExhausted` (with its final-response evidence) on the
    /// very first attempt instead of a bare `Status`.
    #[test]
    fn disabled_caps_attempts_but_keeps_the_status_set() {
        let policy = RetryPolicy::disabled();
        assert_eq!(policy.max_attempts, 1);
        assert_eq!(policy.statuses, RetryPolicy::default().statuses);
        assert!(policy.network_errors);
    }

    #[test]
    fn honor_retry_after_cap_is_the_single_capping_knob() {
        let policy = RetryPolicy {
            honor_retry_after_cap: Duration::from_secs(3600),
            ..RetryPolicy::default()
        };
        let hint = Duration::from_secs(1800);
        assert_eq!(
            hint.min(policy.honor_retry_after_cap),
            hint,
            "raising the cap must actually let a long server hint through"
        );
    }
}
