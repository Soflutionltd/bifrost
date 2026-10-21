//! Shared provider holds, retained by the transport until they expire.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::time::Instant;

/// An absolute deadline resolved once from a provider response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct HoldDeadline {
    pub instant: Instant,
    pub at: SystemTime,
}

impl HoldDeadline {
    /// Pair a reporting clock with the monotonic enforcement clock.
    #[must_use]
    pub fn new(at: SystemTime, wall_now: SystemTime, now: Instant) -> Self {
        Self {
            instant: now + at.duration_since(wall_now).unwrap_or_default(),
            at,
        }
    }

    #[must_use]
    pub fn remaining(self) -> Duration {
        self.instant.saturating_duration_since(Instant::now())
    }
}

/// Provider classification belonging to the winning deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HoldKind {
    RateLimited,
    QuotaExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Held {
    pub deadline: HoldDeadline,
    pub kind: HoldKind,
}

#[derive(Default)]
pub(crate) struct HoldRegistry(Mutex<HashMap<String, Held>>);

/// A key in a Net-owned registry. Dropping handles cannot erase a hold.
/// Handles look up the key on every access, so pruning expired entries cannot
/// fork an old handle away from a newly opened one.
#[derive(Clone)]
pub struct SharedHold {
    registry: Arc<HoldRegistry>,
    key: String,
}

impl SharedHold {
    pub(crate) fn new(registry: Arc<HoldRegistry>, key: String) -> Self {
        Self { registry, key }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Keep the later deadline with its kind; quota wins an exact tie.
    pub fn record(&self, deadline: HoldDeadline, kind: HoldKind) {
        let now = Instant::now();
        let mut entries = self.registry.0.lock().expect("hold registry poisoned");
        entries.retain(|_, held| held.deadline.instant > now);
        if deadline.instant <= now {
            return;
        }
        let candidate = Held { deadline, kind };
        entries
            .entry(self.key.clone())
            .and_modify(|held| {
                if deadline.instant > held.deadline.instant
                    || (deadline.instant == held.deadline.instant
                        && kind == HoldKind::QuotaExhausted)
                {
                    *held = candidate;
                }
            })
            .or_insert(candidate);
    }

    #[must_use]
    pub fn current(&self) -> Option<Held> {
        let now = Instant::now();
        let mut entries = self.registry.0.lock().expect("hold registry poisoned");
        entries.retain(|_, held| held.deadline.instant > now);
        entries.get(&self.key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Canned, ScriptedDispatch, canned, scripted_net};
    use crate::{
        AccountId, AccountSpec, ConcurrencyLimit, Error, NetConfig, RateLimit, RetryPolicy,
    };
    use bifrost_types::{AccountOperation, Cause, Protocol, Provider, TransmissionState};
    use futures::FutureExt;
    use reqwest::StatusCode;

    fn deadline(seconds: u64) -> HoldDeadline {
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        HoldDeadline::new(wall + Duration::from_secs(seconds), wall, Instant::now())
    }

    fn account(net: &crate::Net) -> crate::AccountNet {
        net.attach_account(AccountId("holds".into()), AccountSpec::new(None))
    }

    #[tokio::test(start_paused = true)]
    async fn hold_registry_survives_all_handles_and_reopen() {
        let script = ScriptedDispatch::new([]);
        let net = scripted_net(&script, NetConfig::default());
        let gate = net.shared_hold("project");
        let end = deadline(60);
        gate.record(end, HoldKind::QuotaExhausted);
        drop(gate);
        let first = account(&net);
        drop(first);
        let reopened = account(&net);
        let error = reopened
            .get("https://hold.test")
            .without_bearer_auth()
            .hold_gate("project")
            .send()
            .await
            .err()
            .expect("held request fails");
        assert!(
            matches!(error, Error::LocalHold { held, attempts, .. } if held.deadline == end && attempts.is_empty())
        );
        assert!(script.requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn hold_kind_follows_the_winning_deadline_and_quota_wins_ties() {
        let script = ScriptedDispatch::new([]);
        let net = scripted_net(&script, NetConfig::default());
        let gate = net.shared_hold("merge");
        let long = deadline(100);
        gate.record(long, HoldKind::QuotaExhausted);
        gate.record(deadline(10), HoldKind::RateLimited);
        assert_eq!(
            gate.current(),
            Some(Held {
                deadline: long,
                kind: HoldKind::QuotaExhausted
            })
        );
        gate.record(long, HoldKind::RateLimited);
        assert_eq!(gate.current().unwrap().kind, HoldKind::QuotaExhausted);
        let later = deadline(200);
        gate.record(later, HoldKind::RateLimited);
        assert_eq!(
            gate.current(),
            Some(Held {
                deadline: later,
                kind: HoldKind::RateLimited
            })
        );
        gate.record(later, HoldKind::QuotaExhausted);
        assert_eq!(gate.current().unwrap().kind, HoldKind::QuotaExhausted);
    }

    #[tokio::test(start_paused = true)]
    async fn hold_expiry_reuses_old_and_new_handles_without_forking() {
        let script = ScriptedDispatch::new([]);
        let net = scripted_net(&script, NetConfig::default());
        let old = net.shared_hold("expiry");
        old.record(deadline(1), HoldKind::QuotaExhausted);
        tokio::time::advance(Duration::from_secs(1)).await;
        let new = net.shared_hold("expiry");
        assert!(new.current().is_none());
        old.record(deadline(10), HoldKind::RateLimited);
        assert_eq!(old.current(), new.current());
        assert!(new.current().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn hold_refusals_never_extend_the_deadline_and_report_remaining_time() {
        let script = ScriptedDispatch::new([]);
        let net = scripted_net(&script, NetConfig::default());
        let account = account(&net);
        let gate = net.shared_hold("repeat");
        let end = deadline(60);
        gate.record(end, HoldKind::RateLimited);
        for elapsed in [0, 10, 10] {
            tokio::time::advance(Duration::from_secs(elapsed)).await;
            let error = account
                .get("https://hold.test")
                .without_bearer_auth()
                .hold_gate("repeat")
                .send()
                .await
                .err()
                .expect("held request fails");
            let error = crate::into_account_error_with_throttle_scope(
                error,
                crate::NetErrorContext {
                    operation: AccountOperation::SyncInventory,
                    provider: Some(Provider::Gmail),
                    protocol: Protocol::Gmail,
                    scope: None,
                },
                Some(bifrost_types::ThrottleScope::Account),
            );
            let Cause::Server(bifrost_types::ServerCause::RateLimited { retry_hint }) =
                error.chain().outermost()
            else {
                panic!("rate cause")
            };
            assert_eq!(
                *retry_hint,
                Some(bifrost_types::RetryHint::After(end.remaining()))
            );
            assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Attempt(attempt) if attempt.transmission_state == TransmissionState::Unsent)));
            assert_eq!(gate.current().unwrap().deadline, end);
        }
        assert!(script.requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn hold_recorded_during_concurrency_admission_prevents_dispatch() {
        let script = ScriptedDispatch::new([Canned::Pending]);
        let net = scripted_net(&script, NetConfig::default());
        let account = account(&net);
        let limit = ConcurrencyLimit::new(1);
        let mut first = Box::pin(
            account
                .get("https://hold.test")
                .without_bearer_auth()
                .concurrency_limit(limit.clone())
                .send(),
        );
        assert!(first.as_mut().now_or_never().is_none());
        assert_eq!(script.requests().len(), 1);
        let mut queued = Box::pin(
            account
                .get("https://hold.test")
                .without_bearer_auth()
                .concurrency_limit(limit)
                .hold_gate("parked")
                .send(),
        );
        assert!(queued.as_mut().now_or_never().is_none());
        net.shared_hold("parked")
            .record(deadline(60), HoldKind::QuotaExhausted);
        drop(first);
        assert!(matches!(queued.await, Err(Error::LocalHold { .. })));
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn hold_recorded_during_rate_admission_prevents_dispatch() {
        let script = ScriptedDispatch::new([canned(StatusCode::OK, b"")]);
        let net = scripted_net(&script, NetConfig::default());
        let mut spec = AccountSpec::new(None);
        spec.hosts = vec![RateLimit::new("hold.test", 1.0, 1, 1)];
        let account = net.attach_account(AccountId("rate".into()), spec);
        account
            .get("https://hold.test")
            .without_bearer_auth()
            .send()
            .await
            .unwrap();
        let mut queued = Box::pin(
            account
                .get("https://hold.test")
                .without_bearer_auth()
                .hold_gate("rate")
                .send(),
        );
        assert!(queued.as_mut().now_or_never().is_none());
        net.shared_hold("rate")
            .record(deadline(60), HoldKind::RateLimited);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(queued.await, Err(Error::LocalHold { .. })));
        assert_eq!(script.requests().len(), 1);
    }

    async fn retry_is_refused(status: StatusCode) {
        let script = ScriptedDispatch::new([canned(status, b"provider quota")]);
        let net = scripted_net(&script, NetConfig::default());
        let account = account(&net);
        let gate = net.shared_hold("retry");
        let end = deadline(60);
        let mut policy = RetryPolicy::default();
        policy.statuses.push(status);
        policy.max_backoff = Duration::ZERO;
        let error = account
            .get("https://hold.test")
            .without_bearer_auth()
            .retry(policy)
            .hold_gate("retry")
            .observe_errors(move |response| {
                assert_eq!(response.status, status);
                assert_eq!(response.body.as_ref(), b"provider quota");
                gate.record(end, HoldKind::QuotaExhausted);
            })
            .send()
            .await
            .err()
            .expect("held request fails");
        let Error::LocalHold { attempts, held, .. } = &error else {
            panic!("local refusal: {error:?}")
        };
        assert_eq!(held.deadline, end);
        assert_eq!(attempts.len(), 1);
        assert!(
            matches!(&attempts[0], Error::Status { code, body, .. } if *code == status && body.as_ref() == b"provider quota")
        );
        let error = crate::into_account_error_with_throttle_scope(
            error,
            crate::NetErrorContext {
                operation: AccountOperation::Send,
                provider: Some(Provider::Gmail),
                protocol: Protocol::Gmail,
                scope: None,
            },
            Some(bifrost_types::ThrottleScope::Account),
        );
        let states: Vec<_> = error
            .chain()
            .iter()
            .filter_map(|cause| match cause {
                Cause::Attempt(a) => Some(a.transmission_state),
                _ => None,
            })
            .collect();
        assert!(!states.is_empty());
        assert!(
            states
                .iter()
                .all(|state| *state == TransmissionState::Acknowledged)
        );
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn hold_hook_refuses_internal_retry_after_429() {
        retry_is_refused(StatusCode::TOO_MANY_REQUESTS).await;
    }

    #[tokio::test(start_paused = true)]
    async fn hold_hook_refuses_internal_retry_after_403() {
        retry_is_refused(StatusCode::FORBIDDEN).await;
    }

    #[tokio::test(start_paused = true)]
    async fn hold_hook_observes_capped_error_prefix_only() {
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: StatusCode::BAD_REQUEST,
                headers: Default::default(),
                body: bytes::Bytes::from(vec![b'x'; crate::STATUS_BODY_CAP + 100]),
            },
            canned(StatusCode::TEMPORARY_REDIRECT, b"redirect body"),
            canned(StatusCode::OK, b"success"),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let account = account(&net);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for expected in [400, 307, 200] {
            let calls = Arc::clone(&calls);
            let result = account
                .get("https://hold.test")
                .without_bearer_auth()
                .observe_errors(move |response| {
                    calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    assert_eq!(response.status.as_u16(), 400);
                    assert!(
                        response
                            .body
                            .starts_with(&vec![b'x'; crate::STATUS_BODY_CAP])
                    );
                    assert!(response.body.ends_with(b" ... (truncated)"));
                })
                .send()
                .await;
            if expected == 400 {
                assert!(result.is_err());
            } else {
                let response = result.unwrap();
                assert_eq!(
                    response.body.as_ref(),
                    if expected == 307 {
                        b"redirect body".as_slice()
                    } else {
                        b"success".as_slice()
                    }
                );
            }
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn hold_recorded_during_token_refresh_prevents_dispatch() {
        struct ParkedToken(Mutex<Option<tokio::sync::oneshot::Receiver<crate::AccessToken>>>);
        impl crate::TokenSource for ParkedToken {
            fn current(&self) -> crate::AccountFuture<Result<crate::AccessToken, Error>> {
                let receiver = self.0.lock().unwrap().take().expect("one token lookup");
                Box::pin(async move { Ok(receiver.await.expect("token supplied")) })
            }
            fn refresh(&self) -> crate::AccountFuture<Result<crate::AccessToken, Error>> {
                self.current()
            }
        }
        let script = ScriptedDispatch::new([]);
        let net = scripted_net(&script, NetConfig::default());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let source = Arc::new(ParkedToken(Mutex::new(Some(receiver))));
        let account = net.attach_account(AccountId("token".into()), AccountSpec::new(Some(source)));
        let mut request = Box::pin(account.get("https://hold.test").hold_gate("token").send());
        assert!(request.as_mut().now_or_never().is_none());
        net.shared_hold("token")
            .record(deadline(60), HoldKind::QuotaExhausted);
        sender.send(crate::AccessToken::new("fresh", None)).unwrap();
        assert!(matches!(request.await, Err(Error::LocalHold { .. })));
        assert!(script.requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn hold_between_attempts_preserves_inflight_outcome_and_reconciliation() {
        struct HoldingDispatch {
            script: Arc<ScriptedDispatch>,
            gate: Arc<Mutex<Option<SharedHold>>>,
        }
        impl crate::request::Dispatch for HoldingDispatch {
            fn send(
                &self,
                request: reqwest::RequestBuilder,
            ) -> crate::AccountFuture<Result<reqwest::Response, Error>> {
                let result = crate::request::Dispatch::send(&*self.script, request);
                let gate = Arc::clone(&self.gate);
                Box::pin(async move {
                    let result = result.await;
                    gate.lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .record(deadline(60), HoldKind::RateLimited);
                    result
                })
            }
        }
        let script = ScriptedDispatch::new([Canned::Error(Error::Network {
            message: "earlier inflight reset".into(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        })]);
        let gate = Arc::new(Mutex::new(None));
        let net = crate::Net::new_with_dispatch(
            NetConfig::default(),
            Arc::new(HoldingDispatch {
                script: Arc::clone(&script),
                gate: Arc::clone(&gate),
            }),
        )
        .unwrap();
        *gate.lock().unwrap() = Some(net.shared_hold("between"));
        let policy = RetryPolicy {
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        };
        let error = account(&net)
            .post("https://hold.test")
            .idempotent(true)
            .without_bearer_auth()
            .retry(policy)
            .hold_gate("between")
            .send()
            .await
            .err()
            .expect("retry refused");
        let Error::LocalHold { attempts, .. } = &error else {
            panic!("local refusal")
        };
        assert!(
            matches!(attempts.as_slice(), [Error::Network { transmission_state: TransmissionState::InFlight, message, .. }] if message == "earlier inflight reset")
        );
        let error = crate::into_account_error_with_throttle_scope(
            error,
            crate::NetErrorContext {
                operation: AccountOperation::Send,
                provider: Some(Provider::Gmail),
                protocol: Protocol::Gmail,
                scope: None,
            },
            Some(bifrost_types::ThrottleScope::Account),
        );
        assert!(
            matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(advice) if advice.reason == bifrost_types::ReconcileReason::ThrottledMidFlight)
        );
        assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Attempt(a) if a.transmission_state == TransmissionState::InFlight)));
        assert!(
            error
                .chain()
                .iter()
                .any(|cause| matches!(cause, Cause::Transport(_)))
        );
        assert_eq!(script.requests().len(), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn hold_hook_observes_error_even_when_body_deadline_expires() {
        let script = ScriptedDispatch::new([Canned::StreamThenStall {
            status: StatusCode::FORBIDDEN,
            headers: Default::default(),
            chunks: vec![bytes::Bytes::from_static(b"prefix")],
        }]);
        let net = scripted_net(&script, NetConfig::default());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&observed);
        let error = account(&net)
            .get("https://hold.test")
            .without_bearer_auth()
            .timeout(Duration::from_secs(1))
            .observe_errors(move |response| sink.lock().unwrap().push(response.clone()))
            .send()
            .await
            .err()
            .expect("body deadline expires");
        assert!(matches!(
            error.terminal(),
            Error::Timeout {
                transmission_state: TransmissionState::Acknowledged
            }
        ));
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].status, StatusCode::FORBIDDEN);
        assert_eq!(
            observed[0].body.as_ref(),
            b"prefix ... (body read timed out)"
        );
    }
    async fn review_r1_committed_attempt(status: StatusCode) {
        struct CommitDispatch {
            script: Arc<ScriptedDispatch>,
            gate: Arc<Mutex<Option<SharedHold>>>,
        }
        impl crate::request::Dispatch for CommitDispatch {
            fn send(
                &self,
                request: reqwest::RequestBuilder,
            ) -> crate::AccountFuture<Result<reqwest::Response, Error>> {
                let result = crate::request::Dispatch::send(&*self.script, request);
                let gate = Arc::clone(&self.gate);
                Box::pin(async move {
                    let response = result.await;
                    gate.lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .record(deadline(60), HoldKind::QuotaExhausted);
                    response
                })
            }
        }
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::LOCATION, "/result".parse().unwrap());
        let script = ScriptedDispatch::new([Canned::Response {
            status,
            headers,
            body: bytes::Bytes::from_static(b"mutation outcome"),
        }]);
        let gate = Arc::new(Mutex::new(None));
        let net = crate::Net::new_with_dispatch(
            NetConfig::default(),
            Arc::new(CommitDispatch {
                script: Arc::clone(&script),
                gate: Arc::clone(&gate),
            }),
        )
        .unwrap();
        *gate.lock().unwrap() = Some(net.shared_hold("committed"));
        let mut policy = RetryPolicy {
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        };
        policy.statuses.push(status);
        let error = account(&net)
            .post("https://hold.test/commit")
            .without_bearer_auth()
            .hold_gate("committed")
            .retry(policy)
            .send()
            .await
            .err()
            .expect("replacement attempt is held");
        assert!(
            matches!(&error, Error::LocalHold { attempts, .. } if matches!(attempts.as_slice(), [Error::Status { code, body, .. }] if *code == status && body.as_ref() == b"mutation outcome"))
        );
        let error = crate::into_account_error_with_throttle_scope(
            error,
            crate::NetErrorContext {
                operation: AccountOperation::Send,
                provider: Some(Provider::Gmail),
                protocol: Protocol::Gmail,
                scope: None,
            },
            Some(bifrost_types::ThrottleScope::Account),
        );
        assert!(
            matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(advice) if advice.reason == bifrost_types::ReconcileReason::ThrottledMidFlight),
            "must reconcile the original mutation: {:?}",
            error.recovery()
        );
        assert!(
            matches!(error.chain().iter().find(|cause| matches!(cause, Cause::Attempt(_))), Some(Cause::Attempt(attempt)) if attempt.transmission_state == TransmissionState::InFlight)
        );
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn review_r1_post_303_then_hold_reconciles() {
        review_r1_committed_attempt(StatusCode::SEE_OTHER).await;
    }
    #[tokio::test(start_paused = true)]
    async fn review_r1_post_307_then_hold_reconciles() {
        review_r1_committed_attempt(StatusCode::TEMPORARY_REDIRECT).await;
    }
    #[tokio::test(start_paused = true)]
    async fn review_r1_post_retry_then_hold_reconciles() {
        review_r1_committed_attempt(StatusCode::FORBIDDEN).await;
    }

    async fn review_r1_streaming(status: StatusCode, disable_redirects: bool) {
        use futures::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-response-evidence", "headers-ready".parse().unwrap());
        let script = ScriptedDispatch::new([Canned::StreamThenStall {
            status,
            headers,
            chunks: vec![bytes::Bytes::from_static(b"prefix")],
        }]);
        let net = scripted_net(&script, NetConfig::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let sink = Arc::clone(&calls);
        let mut request = account(&net)
            .get("https://hold.test/stream")
            .without_bearer_auth()
            .observe_errors(move |_| {
                sink.fetch_add(1, Ordering::Relaxed);
            });
        if disable_redirects {
            request = request.follow_redirects(crate::FollowRedirects::Disabled);
        }
        let mut response = tokio::time::timeout(Duration::from_secs(1), request.send_streaming())
            .await
            .expect("headers must arrive without draining the stalled body")
            .expect("streaming headers");
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-response-evidence"], "headers-ready");
        assert_eq!(response.bytes_in.bytes_in(), 0);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            response.body.next().await.unwrap().unwrap().as_ref(),
            b"prefix"
        );
        assert_eq!(response.bytes_in.bytes_in(), 6);
        assert!(
            response.body.next().now_or_never().is_none(),
            "body remains stalled after the prefix"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn review_r1_success_streaming_headers_precede_stalled_body() {
        review_r1_streaming(StatusCode::OK, false).await;
    }
    #[tokio::test(start_paused = true)]
    async fn review_r1_disabled_redirect_streaming_headers_precede_stalled_body() {
        review_r1_streaming(StatusCode::TEMPORARY_REDIRECT, true).await;
    }
    #[tokio::test(start_paused = true)]
    async fn review_r1_passthrough_streaming_headers_precede_stalled_body() {
        review_r1_streaming(StatusCode::TEMPORARY_REDIRECT, false).await;
    }
    fn final_review_reconciles(error: Error) {
        let error = crate::into_account_error_with_throttle_scope(
            error,
            crate::NetErrorContext {
                operation: AccountOperation::Send,
                provider: Some(Provider::Gmail),
                protocol: Protocol::Gmail,
                scope: None,
            },
            Some(bifrost_types::ThrottleScope::Account),
        );
        assert!(
            matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(_)),
            "possibly committed mutation must reconcile: {:?}",
            error.recovery()
        );
        assert!(
            matches!(error.chain().iter().find(|c| matches!(c, Cause::Attempt(_))),
            Some(Cause::Attempt(a)) if a.transmission_state == TransmissionState::InFlight)
        );
        assert!(
            error.chain().iter().any(|c| matches!(c, Cause::Attempt(a)
            if a.transmission_state == TransmissionState::Acknowledged)),
            "keep response evidence"
        );
    }

    async fn final_review_token_deadline(status: StatusCode) {
        struct Token(std::sync::atomic::AtomicUsize);
        impl crate::TokenSource for Token {
            fn current(&self) -> crate::AccountFuture<Result<crate::AccessToken, Error>> {
                let first = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                Box::pin(async move {
                    if !first {
                        futures::future::pending::<()>().await;
                    }
                    Ok(crate::AccessToken::new("token", None))
                })
            }
            fn refresh(&self) -> crate::AccountFuture<Result<crate::AccessToken, Error>> {
                self.current()
            }
        }
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("location", "/result".parse().unwrap());
        let script = ScriptedDispatch::new([Canned::Response {
            status,
            headers,
            body: bytes::Bytes::from_static(b"committed"),
        }]);
        let net = scripted_net(&script, NetConfig::default());
        let mut spec = AccountSpec::new(Some(Arc::new(Token(
            std::sync::atomic::AtomicUsize::new(0),
        ))));
        spec.token_max_age = Duration::ZERO;
        let account = net.attach_account(AccountId("token-deadline".into()), spec);
        let error = account
            .post("https://hold.test/commit")
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .err()
            .expect("replacement token timed out");
        if status == StatusCode::UNAUTHORIZED {
            let error = crate::into_account_error_with_throttle_scope(
                error,
                crate::NetErrorContext {
                    operation: AccountOperation::Send,
                    provider: Some(Provider::Gmail),
                    protocol: Protocol::Gmail,
                    scope: None,
                },
                Some(bifrost_types::ThrottleScope::Account),
            );
            assert!(
                matches!(
                    error.recovery(),
                    bifrost_types::RecoveryClass::Retry(advice)
                        if advice.disposition == bifrost_types::RetryDisposition::SameRequest
                ),
                "a rejected POST followed by a refresh timeout remains retryable: {:?}",
                error.recovery()
            );
            assert!(error.chain().iter().any(|cause| matches!(
                cause,
                Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired)
            )));
            assert!(!error.chain().iter().any(|cause| matches!(
                cause,
                Cause::Attempt(attempt)
                    if attempt.transmission_state == TransmissionState::Acknowledged
            )));
        } else {
            final_review_reconciles(error);
        }
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_post_redirect_token_deadline_reconciles() {
        final_review_token_deadline(StatusCode::SEE_OTHER).await;
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_post_auth_refresh_deadline_retries() {
        final_review_token_deadline(StatusCode::UNAUTHORIZED).await;
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_retry_after_past_deadline_reconciles() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("retry-after", "60".parse().unwrap());
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers,
            body: bytes::Bytes::from_static(b"quota"),
        }]);
        let net = scripted_net(&script, NetConfig::default());
        let error = account(&net)
            .post("https://hold.test/commit")
            .without_bearer_auth()
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .err()
            .expect("retry wait timed out");
        final_review_reconciles(error);
        assert_eq!(script.requests().len(), 1);
    }
    fn final_review_redirect(location: &str) -> Canned {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("location", location.parse().unwrap());
        Canned::Response {
            status: StatusCode::SEE_OTHER,
            headers,
            body: bytes::Bytes::from_static(b"committed"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_every_replacement_exit_keeps_commit_evidence() {
        let outcomes = [
            Canned::Error(Error::Cancelled),
            Canned::Error(Error::AuthLost {
                transmission_state: None,
                final_response: None,
            }),
            Canned::Error(Error::Network {
                message: "replacement connect failed".into(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            Canned::Error(Error::Tls {
                message: "replacement TLS failed".into(),
                transmission_state: TransmissionState::Unsent,
            }),
            Canned::Pending,
            canned(StatusCode::UNAUTHORIZED, b"replacement refused"),
            canned(StatusCode::TOO_MANY_REQUESTS, b"replacement quota"),
            Canned::StreamThenStall {
                status: StatusCode::OK,
                headers: Default::default(),
                chunks: vec![bytes::Bytes::from_static(b"partial")],
            },
            Canned::StreamThenStall {
                status: StatusCode::FORBIDDEN,
                headers: Default::default(),
                chunks: vec![bytes::Bytes::from_static(b"partial")],
            },
        ];
        for outcome in outcomes {
            let script = ScriptedDispatch::new([final_review_redirect("/result"), outcome]);
            let net = scripted_net(&script, NetConfig::default());
            let error = account(&net)
                .post("https://hold.test/commit")
                .without_bearer_auth()
                .retry(RetryPolicy::disabled())
                .timeout(Duration::from_secs(1))
                .send()
                .await
                .err()
                .expect("replacement failed");
            final_review_reconciles(error);
            assert_eq!(script.requests().len(), 2);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_redirect_processing_keeps_commit_evidence() {
        for (location, guard, max_hops) in [
            ("http://[broken", true, 10),
            ("/result", false, 10),
            ("/result", true, 0),
        ] {
            let script = ScriptedDispatch::new([final_review_redirect(location)]);
            let net = scripted_net(&script, NetConfig::default());
            let error = account(&net)
                .post("https://hold.test/commit")
                .without_bearer_auth()
                .redirect_guard(move |_, _| guard)
                .follow_redirects(crate::FollowRedirects::Enabled(
                    crate::RedirectPolicy::with_hops(max_hops),
                ))
                .send()
                .await
                .err()
                .expect("redirect processing refused");
            final_review_reconciles(error);
            assert_eq!(script.requests().len(), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_replacement_rate_deadline_reconciles() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::OK, b"prime"),
            final_review_redirect("https://rate.test/result"),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let mut spec = AccountSpec::new(None);
        spec.hosts = vec![RateLimit::new("rate.test", 0.01, 1, 1)];
        let account = net.attach_account(AccountId("rate-deadline".into()), spec);
        account
            .get("https://rate.test/prime")
            .without_bearer_auth()
            .send()
            .await
            .unwrap();
        let error = account
            .post("https://hold.test/commit")
            .without_bearer_auth()
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .err()
            .expect("replacement rate timeout");
        final_review_reconciles(error);
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_streaming_body_keeps_commit_evidence() {
        use futures::StreamExt;
        let script = ScriptedDispatch::new([
            final_review_redirect("/result"),
            Canned::StreamThenStall {
                status: StatusCode::OK,
                headers: Default::default(),
                chunks: vec![bytes::Bytes::from_static(b"partial")],
            },
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let mut response = account(&net)
            .post("https://hold.test/commit")
            .without_bearer_auth()
            .timeout(Duration::from_secs(1))
            .send_streaming()
            .await
            .unwrap();
        assert_eq!(
            response.body.next().await.unwrap().unwrap().as_ref(),
            b"partial"
        );
        final_review_reconciles(response.body.next().await.unwrap().unwrap_err());
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_buffer_ceiling_keeps_commit_evidence() {
        let script = ScriptedDispatch::new([
            final_review_redirect("/result"),
            canned(StatusCode::OK, b"oversize"),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let mut spec = AccountSpec::new(None);
        spec.max_buffered_response = Some(1);
        let account = net.attach_account(AccountId("ceiling".into()), spec);
        let error = account
            .post("https://hold.test/commit")
            .without_bearer_auth()
            .send()
            .await
            .err()
            .expect("body exceeds ceiling");
        final_review_reconciles(error);
    }
    #[tokio::test(start_paused = true)]
    async fn final_review_unsent_history_keeps_auth_classification() {
        let script = ScriptedDispatch::new([
            Canned::Error(Error::Network {
                message: "no dispatch".into(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            Canned::Error(Error::AuthLost {
                transmission_state: None,
                final_response: None,
            }),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let error = account(&net)
            .post("https://hold.test/commit")
            .without_bearer_auth()
            .retry(RetryPolicy {
                max_backoff: Duration::ZERO,
                ..RetryPolicy::default()
            })
            .send()
            .await
            .err()
            .expect("auth failure");
        assert!(
            matches!(&error, Error::AttemptHistory { attempts, may_have_committed: false, .. }
            if matches!(attempts.as_slice(), [Error::Network { transmission_state: TransmissionState::Unsent, .. }]))
        );
        let classified = crate::into_account_error_with_throttle_scope(
            error,
            crate::NetErrorContext {
                operation: AccountOperation::Send,
                provider: None,
                protocol: Protocol::Gmail,
                scope: None,
            },
            None,
        );
        assert!(matches!(
            classified.kind(),
            bifrost_types::AccountErrorKind::Authentication(_)
        ));
        assert!(!matches!(
            classified.recovery(),
            bifrost_types::RecoveryClass::Reconcile(_)
        ));
    }
}
