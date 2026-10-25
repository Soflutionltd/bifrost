//! Gmail project quota classification and absolute response-time deadlines.

use std::time::{Duration, SystemTime};

use bifrost_net::{HoldDeadline, HoldKind};
use bifrost_types::ThrottleBudget;
use jiff::{Timestamp, ToSpan};

use crate::error::{GmailResponseHeaders, parse_gmail_envelope};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiSurface {
    Gmail,
    Calendar,
    People,
    Drive,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectObservation {
    pub(crate) budget: ThrottleBudget,
    pub(crate) deadline: HoldDeadline,
}

pub(crate) fn project_kind(status: u16, body: &[u8]) -> Option<HoldKind> {
    let envelope = parse_gmail_envelope(body)?;
    // A concurrent-request 429 is explicitly per user, including when the
    // envelope reuses rateLimitExceeded as its general reason code.
    let user_concurrency = envelope
        .message
        .iter()
        .chain(
            envelope
                .errors
                .iter()
                .filter_map(|detail| detail.message.as_ref()),
        )
        .any(|message| {
            message
                .to_ascii_lowercase()
                .contains("too many concurrent requests for user")
        });
    if status == 429 && user_concurrency {
        return None;
    }
    match envelope.primary_reason()? {
        "dailyLimitExceeded" => Some(HoldKind::QuotaExhausted),
        "rateLimitExceeded" => Some(HoldKind::RateLimited),
        _ => None,
    }
}

pub(crate) fn resolve_deadline(
    kind: HoldKind,
    headers: &GmailResponseHeaders,
    wall_now: SystemTime,
    now: tokio::time::Instant,
) -> HoldDeadline {
    let at = headers.retry_at.unwrap_or_else(|| match kind {
        HoldKind::QuotaExhausted => next_pacific_midnight(wall_now),
        // The engine uses the same one-second fallback for a stamped budget
        // without a provider hint. Resolve it here once, not at each refusal.
        _ => wall_now + Duration::from_secs(1),
    });
    HoldDeadline::new(at, wall_now, now)
}

pub(crate) fn next_pacific_midnight(now: SystemTime) -> SystemTime {
    let zoned = Timestamp::try_from(now)
        .expect("system clock representable by jiff")
        .in_tz("America/Los_Angeles")
        .expect("bundled Pacific timezone available");
    let tomorrow = zoned.date().checked_add(1.day()).expect("next civil date");
    let midnight = tomorrow
        .at(0, 0, 0, 0)
        .to_zoned(zoned.time_zone().clone())
        .expect("Pacific midnight exists");
    SystemTime::from(midnight.timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::error::{GmailErrorContext, into_account_error};
    use crate::client::GmailClient;
    use bifrost_net::test_support::{Canned, ScriptedDispatch, canned, scripted_net};
    use bifrost_net::{AccountId, AccountSpec, NetConfig, RetryPolicy, StaticTokenSource};
    use bifrost_types::{
        AccountErrorKind, AccountOperation, Cause, RetryHint, ServerCause, ServerErrorKind,
        ThrottleScope,
    };
    use bytes::Bytes;
    use reqwest::{
        StatusCode,
        header::{HeaderMap, HeaderValue, RETRY_AFTER},
    };
    use std::sync::Arc;

    fn wall() -> SystemTime {
        SystemTime::from("2026-10-19T12:00:00Z".parse::<Timestamp>().unwrap())
    }

    fn check_midnight(start: &str, end: &str, hours: u64) {
        let start = SystemTime::from(start.parse::<Timestamp>().unwrap());
        let end = SystemTime::from(end.parse::<Timestamp>().unwrap());
        assert_eq!(next_pacific_midnight(start), end);
        assert_eq!(
            end.duration_since(start).unwrap(),
            Duration::from_secs(hours * 3600)
        );
    }

    #[test]
    fn project_hold_midnight_spring_is_23_hours() {
        check_midnight("2026-03-08T08:00:00Z", "2026-03-09T07:00:00Z", 23);
    }

    #[test]
    fn project_hold_midnight_autumn_is_25_hours() {
        check_midnight("2026-11-01T07:00:00Z", "2026-11-02T08:00:00Z", 25);
    }

    #[test]
    fn project_hold_midnight_ordinary_is_24_hours() {
        check_midnight("2026-10-19T07:00:00Z", "2026-10-20T07:00:00Z", 24);
        check_midnight("2026-10-19T12:00:00Z", "2026-10-20T07:00:00Z", 19);
    }

    fn quota_response(status: StatusCode, reason: &str, retry_after: Option<&str>) -> Canned {
        let mut headers = HeaderMap::new();
        if let Some(value) = retry_after {
            headers.insert(RETRY_AFTER, HeaderValue::from_str(value).unwrap());
        }
        Canned::Response {
            status,
            headers,
            body: Bytes::from(format!(
                r#"{{"error":{{"errors":[{{"reason":"{reason}"}}]}}}}"#
            )),
        }
    }

    fn client(
        net: &bifrost_net::Net,
        account: &str,
        project: Option<&str>,
        retry: RetryPolicy,
    ) -> GmailClient {
        let mut spec = AccountSpec::new(Some(Arc::new(StaticTokenSource::new("token", None))));
        spec.default_retry = retry;
        let client = GmailClient::with_account_net(
            "https://gmail.test",
            net.attach_account(AccountId(account.into()), spec),
        )
        .with_fixed_clock(wall());
        match project {
            Some(project) => client.with_quota_project(project.into()),
            None => client,
        }
    }

    async fn check_daily_hint(value: &str, expected: SystemTime) {
        let script = ScriptedDispatch::new([quota_response(
            StatusCode::FORBIDDEN,
            "dailyLimitExceeded",
            Some(value),
        )]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "daily", Some("123"), RetryPolicy::disabled());
        let error = client
            .get::<serde_json::Value>("/profile")
            .await
            .err()
            .unwrap();
        let crate::Error::ObservedNet(observed) = &error else {
            panic!("observed response")
        };
        assert_eq!(
            observed.headers.retry_after,
            if value == "7200" {
                Some(crate::error::RetryAfter::Delta(Duration::from_secs(7200)))
            } else {
                Some(crate::error::RetryAfter::At(expected))
            }
        );
        let error = into_account_error(error, GmailErrorContext::inventory());
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        );
        assert!(
            matches!(error.chain().outermost(), Cause::Server(ServerCause::QuotaExhausted { retry_hint: Some(RetryHint::At(at)) }) if *at == expected)
        );
        let budget = ThrottleBudget::new("gmail-project|123");
        assert_eq!(error.throttle_budgets(), std::slice::from_ref(&budget));
        let held = net.shared_hold(budget.as_str()).current().unwrap();
        assert_eq!(held.deadline.at, expected);
        assert_eq!(
            held.deadline.remaining(),
            expected.duration_since(wall()).unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_daily_delta_retry_after_is_absolute_and_preserved() {
        check_daily_hint("7200", wall() + Duration::from_secs(7200)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_daily_http_date_retry_after_is_preserved() {
        check_daily_hint(
            "Mon, 19 Oct 2026 14:00:00 GMT",
            wall() + Duration::from_secs(7200),
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_daily_without_hint_uses_pacific_midnight() {
        let script = ScriptedDispatch::new([quota_response(
            StatusCode::FORBIDDEN,
            "dailyLimitExceeded",
            None,
        )]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "daily", Some("123"), RetryPolicy::disabled());
        let error = into_account_error(
            client
                .get::<serde_json::Value>("/profile")
                .await
                .err()
                .unwrap(),
            GmailErrorContext::inventory(),
        );
        let midnight = SystemTime::from("2026-10-20T07:00:00Z".parse::<Timestamp>().unwrap());
        assert!(
            matches!(error.chain().outermost(), Cause::Server(ServerCause::QuotaExhausted { retry_hint: Some(RetryHint::At(at)) }) if *at == midnight)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_rate_without_hint_uses_short_stamped_fallback() {
        let script = ScriptedDispatch::new([quota_response(
            StatusCode::FORBIDDEN,
            "rateLimitExceeded",
            None,
        )]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "rate", Some("123"), RetryPolicy::disabled());
        let error = into_account_error(
            client
                .get::<serde_json::Value>("/profile")
                .await
                .err()
                .unwrap(),
            GmailErrorContext::inventory(),
        );
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
        assert_eq!(
            error.throttle_budgets(),
            &[ThrottleBudget::new("gmail-project|123")]
        );
        assert!(
            matches!(error.chain().outermost(), Cause::Server(ServerCause::RateLimited { retry_hint: Some(RetryHint::At(at)) }) if *at == wall() + Duration::from_secs(1))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_accounts_share_and_different_projects_do_not() {
        let script = ScriptedDispatch::new([
            quota_response(StatusCode::FORBIDDEN, "dailyLimitExceeded", Some("7200")),
            canned(StatusCode::OK, b"{}"),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let a = client(&net, "a", Some("123"), RetryPolicy::disabled());
        let b = client(&net, "b", Some("123"), RetryPolicy::disabled());
        let other = client(&net, "c", Some("456"), RetryPolicy::disabled());
        assert!(a.get::<serde_json::Value>("/profile").await.is_err());
        let refusal = into_account_error(
            b.get::<serde_json::Value>("/profile").await.err().unwrap(),
            GmailErrorContext::inventory(),
        );
        assert_eq!(script.requests().len(), 1);
        assert_eq!(
            refusal.throttle_budgets(),
            &[ThrottleBudget::new("gmail-project|123")]
        );
        assert_eq!(
            refusal.kind(),
            &AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        );
        assert!(
            matches!(refusal.chain().outermost(), Cause::Server(ServerCause::QuotaExhausted { retry_hint: Some(RetryHint::After(delay)) }) if *delay == Duration::from_secs(7200))
        );
        assert!(other.get::<serde_json::Value>("/profile").await.is_ok());
        assert_eq!(script.requests().len(), 2);
        drop(a);
        drop(b);
        let reopened = client(&net, "reopened", Some("123"), RetryPolicy::disabled());
        assert!(reopened.get::<serde_json::Value>("/profile").await.is_err());
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_unconfigured_keeps_account_scope() {
        let script = ScriptedDispatch::new([quota_response(
            StatusCode::FORBIDDEN,
            "dailyLimitExceeded",
            Some("60"),
        )]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "no-project", None, RetryPolicy::disabled());
        let error = into_account_error(
            client
                .get::<serde_json::Value>("/profile")
                .await
                .err()
                .unwrap(),
            GmailErrorContext::inventory(),
        );
        assert!(error.throttle_budgets().is_empty());
        assert!(
            matches!(error.chain().outermost(), Cause::Server(ServerCause::QuotaExhausted { retry_hint: Some(RetryHint::At(at)) }) if *at == wall() + Duration::from_secs(60))
        );
        let bifrost_types::RecoveryClass::Retry(advice) = error.recovery() else {
            panic!("retry")
        };
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
        assert!(net.shared_hold("gmail-project|123").current().is_none());
    }

    async fn check_surface(surface: ApiSurface, reason: &str) {
        let script =
            ScriptedDispatch::new([quota_response(StatusCode::FORBIDDEN, reason, Some("60"))]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "non-gmail", Some("123"), RetryPolicy::disabled())
            .with_calendar_base("https://gmail.test/calendar/v3")
            .with_people_base("https://gmail.test/people/v1");
        let (url, ctx) = match surface {
            ApiSurface::Calendar => (
                format!("{}/users/me/calendarList", client.calendar_base()),
                GmailErrorContext::calendar_collection(AccountOperation::CalendarsList),
            ),
            ApiSurface::People => (
                format!("{}/people/me/connections", client.people_base()),
                GmailErrorContext::contact_collection(AccountOperation::ContactsList),
            ),
            _ => unreachable!(),
        };
        assert!(client.gmail_quota_cost(&url, "GET").is_none());
        let error = into_account_error(
            client.get::<serde_json::Value>(&url).await.err().unwrap(),
            ctx.clone(),
        );
        assert!(error.throttle_budgets().is_empty());
        // The contextual surface must also reject a project annotation, even
        // when a caller hands it Gmail-shaped response metadata.
        let mut headers =
            crate::error::GmailResponseHeaders::from_headers_at(&HeaderMap::new(), wall());
        headers.project = Some(ProjectObservation {
            budget: ThrottleBudget::new("gmail-project|123"),
            deadline: HoldDeadline::new(
                wall() + Duration::from_secs(60),
                wall(),
                tokio::time::Instant::now(),
            ),
        });
        let body = Bytes::from(format!(
            r#"{{"error":{{"errors":[{{"reason":"{reason}"}}]}}}}"#
        ));
        let forged = crate::Error::response_from_parts(
            crate::error::GmailService::GmailApi,
            403,
            headers,
            body,
        );
        assert!(
            into_account_error(forged, ctx)
                .throttle_budgets()
                .is_empty()
        );
        assert!(net.shared_hold("gmail-project|123").current().is_none());
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_calendar_daily_does_not_stamp() {
        check_surface(ApiSurface::Calendar, "dailyLimitExceeded").await;
    }
    #[tokio::test(start_paused = true)]
    async fn project_hold_calendar_rate_does_not_stamp() {
        check_surface(ApiSurface::Calendar, "rateLimitExceeded").await;
    }
    #[tokio::test(start_paused = true)]
    async fn project_hold_people_daily_does_not_stamp() {
        check_surface(ApiSurface::People, "dailyLimitExceeded").await;
    }
    #[tokio::test(start_paused = true)]
    async fn project_hold_people_rate_does_not_stamp() {
        check_surface(ApiSurface::People, "rateLimitExceeded").await;
    }

    async fn check_account_signal(reason: &str, status: StatusCode) {
        let script = ScriptedDispatch::new([quota_response(status, reason, None)]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "user", Some("123"), RetryPolicy::disabled());
        let error = into_account_error(
            client
                .get::<serde_json::Value>("/profile")
                .await
                .err()
                .unwrap(),
            GmailErrorContext::inventory(),
        );
        assert!(error.throttle_budgets().is_empty());
        let bifrost_types::RecoveryClass::Retry(advice) = error.recovery() else {
            panic!("retry")
        };
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
        assert!(net.shared_hold("gmail-project|123").current().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_user_rate_is_account_only() {
        check_account_signal("userRateLimitExceeded", StatusCode::FORBIDDEN).await;
    }
    #[tokio::test(start_paused = true)]
    async fn project_hold_concurrent_429_is_account_only() {
        check_account_signal("", StatusCode::TOO_MANY_REQUESTS).await;
        let script = ScriptedDispatch::new([canned(StatusCode::TOO_MANY_REQUESTS,
            br#"{"error":{"message":"Too many concurrent requests for user.","errors":[{"reason":"rateLimitExceeded"}]}}"#)]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "concurrency", Some("123"), RetryPolicy::disabled());
        let error = into_account_error(
            client
                .get::<serde_json::Value>("/profile")
                .await
                .err()
                .unwrap(),
            GmailErrorContext::inventory(),
        );
        assert!(error.throttle_budgets().is_empty());
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
        let bifrost_types::RecoveryClass::Retry(advice) = error.recovery() else {
            panic!("retry")
        };
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
        assert!(net.shared_hold("gmail-project|123").current().is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn project_hold_quota_exceeded_is_account_only() {
        check_account_signal("quotaExceeded", StatusCode::FORBIDDEN).await;
    }

    async fn check_retry(status: StatusCode) {
        let script =
            ScriptedDispatch::new([quota_response(status, "rateLimitExceeded", Some("7200"))]);
        let net = scripted_net(&script, NetConfig::default());
        let mut policy = RetryPolicy::default();
        policy.statuses.push(status);
        policy.honor_retry_after_cap = Duration::ZERO;
        let client = client(&net, "retry", Some("123"), policy);
        let error = client
            .get::<serde_json::Value>("/profile")
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            crate::Error::ObservedNet(ref observed) if matches!(observed.error, bifrost_net::Error::LocalHold { .. })
        ));
        let error = into_account_error(error, GmailErrorContext::inventory());
        assert_eq!(
            error.throttle_budgets(),
            &[ThrottleBudget::new("gmail-project|123")]
        );
        assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Attempt(a) if a.transmission_state == bifrost_types::TransmissionState::Acknowledged)));
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_google_429_internal_retry_is_refused() {
        check_retry(StatusCode::TOO_MANY_REQUESTS).await;
    }
    #[tokio::test(start_paused = true)]
    async fn project_hold_google_403_internal_retry_is_refused() {
        check_retry(StatusCode::FORBIDDEN).await;
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_all_gmail_funnels_refuse_without_moving_deadline() {
        let script = ScriptedDispatch::new([]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "funnels", Some("123"), RetryPolicy::disabled());
        let gate = net.shared_hold("gmail-project|123");
        let deadline = HoldDeadline::new(
            wall() + Duration::from_secs(7200),
            wall(),
            tokio::time::Instant::now(),
        );
        gate.record(deadline, HoldKind::QuotaExhausted);
        assert!(client.get::<serde_json::Value>("/labels").await.is_err());
        assert!(
            client
                .post::<serde_json::Value, _>(
                    "/watch",
                    &serde_json::json!({}),
                    bifrost_net::ReplayPolicy::Never
                )
                .await
                .is_err()
        );
        assert!(
            client
                .post_no_content(
                    "/stop",
                    &serde_json::json!({}),
                    bifrost_net::ReplayPolicy::Never
                )
                .await
                .is_err()
        );
        let raw = client
            .account_net()
            .post("https://gmail.test/messages/batchModify");
        assert!(
            client
                .execute_builder(raw, "Gmail API", bifrost_net::ReplayPolicy::Never)
                .await
                .is_err()
        );
        assert_eq!(gate.current().unwrap().deadline, deadline);
        assert!(script.requests().is_empty());
    }
    async fn review_r1_cross_surface(from: ApiSurface, to: ApiSurface) {
        let base = "https://gmail.test";
        let path = |surface| match surface {
            ApiSurface::Gmail => "/profile",
            ApiSurface::Calendar => "/calendar/v3/users/me/calendarList",
            ApiSurface::People => "/people/v1/people/me/connections",
            _ => unreachable!(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_str(&format!("{base}{}", path(to))).unwrap(),
        );
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: StatusCode::TEMPORARY_REDIRECT,
                headers,
                body: Bytes::new(),
            },
            quota_response(StatusCode::FORBIDDEN, "dailyLimitExceeded", Some("60")),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "redirect", Some("123"), RetryPolicy::disabled())
            .with_calendar_base(format!("{base}/calendar/v3"))
            .with_people_base(format!("{base}/people/v1"));
        let gate = net.shared_hold("gmail-project|123");
        if from != ApiSurface::Gmail {
            gate.record(
                HoldDeadline::new(
                    wall() + Duration::from_secs(120),
                    wall(),
                    tokio::time::Instant::now(),
                ),
                HoldKind::QuotaExhausted,
            );
        }
        let before = gate.current();
        let error = client
            .get::<serde_json::Value>(&format!("{base}{}", path(from)))
            .await
            .expect_err("cross-surface redirect rejected");
        assert_eq!(
            script.requests().len(),
            1,
            "target surface must never be dispatched"
        );
        assert_eq!(
            gate.current(),
            before,
            "foreign response must not change Gmail's hold"
        );
        assert!(
            matches!(
                error,
                crate::Error::Net(ref net) if matches!(net.terminal(), bifrost_net::Error::RedirectRejected { .. })
            ),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn review_r1_calendar_redirect_to_held_gmail_is_rejected() {
        review_r1_cross_surface(ApiSurface::Calendar, ApiSurface::Gmail).await;
    }
    #[tokio::test(start_paused = true)]
    async fn review_r1_people_redirect_to_held_gmail_is_rejected() {
        review_r1_cross_surface(ApiSurface::People, ApiSurface::Gmail).await;
    }
    #[tokio::test(start_paused = true)]
    async fn review_r1_gmail_redirect_to_calendar_cannot_record_hold() {
        review_r1_cross_surface(ApiSurface::Gmail, ApiSurface::Calendar).await;
    }
    #[tokio::test(start_paused = true)]
    async fn review_r1_gmail_redirect_to_people_cannot_record_hold() {
        review_r1_cross_surface(ApiSurface::Gmail, ApiSurface::People).await;
    }

    #[tokio::test(start_paused = true)]
    async fn review_r1_same_surface_redirect_remains_supported() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::LOCATION, "/labels".parse().unwrap());
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: StatusCode::TEMPORARY_REDIRECT,
                headers,
                body: Bytes::new(),
            },
            canned(StatusCode::OK, b"{}"),
        ]);
        let net = scripted_net(&script, NetConfig::default());
        let client = client(&net, "same-surface", Some("123"), RetryPolicy::disabled());
        client.get::<serde_json::Value>("/profile").await.unwrap();
        assert_eq!(script.requests().len(), 2);
        assert_eq!(script.requests()[1].url.path(), "/labels");
    }
}
