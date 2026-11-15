//! Shared exchange state and absolute request deadlines.

use std::time::Duration;

use crate::{AccountNet, Error, TransmissionState};
use reqwest::header::HeaderMap;

/// One absolute deadline for admission, dispatch, retries and response bodies.
#[derive(Clone, Copy, Debug)]
pub struct RequestDeadline(Option<tokio::time::Instant>);

impl RequestDeadline {
    /// Start a deadline now; None permits an unbounded exchange.
    #[must_use]
    pub fn from_timeout(timeout: Option<Duration>) -> Self {
        Self(timeout.map(|timeout| tokio::time::Instant::now() + timeout))
    }

    /// Deadline check for the response-body path.
    ///
    /// Expiry here is not the same event as expiry before dispatch:
    /// response headers already arrived, so the server acted on the
    /// request and the evidence is `Acknowledged`. That routes to
    /// `Protocol(PartialResponse)` rather than a transport class, which
    /// is what a truncated body is. The point is that a body cut short
    /// by the deadline must never surface as a clean end-of-stream -
    /// `send()` would otherwise hand back a prefix of the payload as if
    /// it were the whole response.
    pub fn check_body(self) -> Result<(), Error> {
        match self.0 {
            Some(at) if tokio::time::Instant::now() >= at => Err(Error::Timeout {
                transmission_state: TransmissionState::Acknowledged,
            }),
            _ => Ok(()),
        }
    }

    /// Bound a wait on the response-body path by the total deadline.
    /// Shared bandwidth admission waits are the reason this exists: they are the one class of wait the
    /// retry loop never sees, and with a low cap they can outlast the
    /// caller's total timeout by minutes.
    pub async fn bound_body<F: std::future::Future>(self, future: F) -> Result<F::Output, Error> {
        match self.0 {
            Some(at) => tokio::time::timeout_at(at, future)
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::Acknowledged,
                }),
            None => Ok(future.await),
        }
    }

    /// Remaining pre-dispatch time, or an unsent timeout once expired.
    pub fn remaining(self) -> Result<Option<Duration>, Error> {
        self.0
            .map(|deadline| {
                deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|remaining| !remaining.is_zero())
                    .ok_or(Error::Timeout {
                        transmission_state: TransmissionState::Unsent,
                    })
            })
            .transpose()
    }

    /// Bound a pre-acknowledgement wait by the total deadline.
    ///
    /// Everything this covers - rate-limit admission, token lookup, the
    /// forced refresh after a 401 - happens before the replacement
    /// attempt puts anything on the wire, so expiry is `Unsent`. The
    /// post-header counterpart is `bound_body`, which is `Acknowledged`;
    /// keeping the two apart is the whole point of routing every wait
    /// through this type rather than reaching for the instant directly.
    pub async fn bound<F: std::future::Future>(self, future: F) -> Result<F::Output, Error> {
        match self.0 {
            Some(at) => tokio::time::timeout_at(at, future)
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::Unsent,
                }),
            None => Ok(future.await),
        }
    }

    /// Bound dispatch, including upload admission, by the absolute deadline.
    /// Once dispatch starts, expiry carries in-flight transmission evidence.
    pub async fn bound_dispatch<F: std::future::Future>(
        self,
        future: F,
    ) -> Result<F::Output, Error> {
        match self.0 {
            Some(at) => tokio::time::timeout_at(at, future)
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::InFlight,
                }),
            None => Ok(future.await),
        }
    }

    /// Wait between attempts without extending the exchange deadline.
    pub async fn sleep(self, delay: Duration) -> Result<(), Error> {
        match self.0 {
            Some(deadline) => tokio::time::timeout_at(deadline, tokio::time::sleep(delay))
                .await
                .map_err(|_| Error::Timeout {
                    transmission_state: TransmissionState::Unsent,
                }),
            None => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
        }
    }
}

/// Mutable state for one hop. The public exchange door is RequestBuilder plus
/// HopPolicy; consumers never need to synchronize a second set of these fields.
pub(crate) struct Hop {
    pub method: reqwest::Method,
    pub url: reqwest::Url,
    pub body: Option<bytes::Bytes>,
    pub headers: reqwest::header::HeaderMap,
    pub replay: Option<crate::ReplayPolicy>,
    pub auth_for_next_hop: bool,
    pub carrying_auth: bool,
    pub auth_self_stripped: bool,
    pub prior_url: Option<reqwest::Url>,
    pub host: Option<String>,
    pub cost: Option<u32>,
    pub quota_scope_override: Option<crate::QuotaScope>,
    pub quota_scope: crate::QuotaScope,
    pub cost_units: u32,
}

impl Hop {
    pub fn resolve_rate_key(&mut self, account: &crate::AccountNet) {
        self.host = host_from_url(self.url.clone());
        self.quota_scope = resolve_quota_scope(
            account,
            self.host.as_deref(),
            self.quota_scope_override.as_ref(),
        );
        self.cost_units = recompute_cost_units(
            account.net().governor(),
            self.host.as_deref(),
            &self.quota_scope,
            self.cost,
        );
    }

    pub fn credential(
        &mut self,
        policy: Option<&dyn crate::HopPolicy>,
    ) -> Result<Option<crate::HopCredential>, Error> {
        let Some(policy) = policy else {
            return Ok(None);
        };
        // Admission precedes token lookup, on retries as well as relocations.
        self.headers.remove(reqwest::header::AUTHORIZATION);
        policy.admit(self.prior_url.as_ref(), &self.url).map(Some)
    }

    pub fn follow(
        &mut self,
        step: crate::RedirectStep,
        policy: Option<&dyn crate::HopPolicy>,
        account: &crate::AccountNet,
    ) -> Result<(), Error> {
        let prior = self.url.clone();
        if let Some(policy) = policy {
            let next = &step.next_url;
            // Admit before a protocol rewrite names a secondary write target.
            policy.admit(Some(&prior), next)?;
            policy.rewrite_headers(&prior, next, &mut self.headers)?;
        }
        if step.next_method != self.method {
            self.replay = None;
        }
        self.prior_url = Some(prior);
        self.method = step.next_method;
        self.url = step.next_url;
        if !step.preserve_body {
            self.body = None;
            strip_body_headers(&mut self.headers);
        }
        self.auth_for_next_hop &= step.keep_auth;
        if !step.keep_auth {
            self.headers.remove(reqwest::header::AUTHORIZATION);
            strip_origin_bound_credentials(&mut self.headers);
            // A later unauthenticated 401 cannot prove credential loss.
            self.auth_self_stripped |= self.carrying_auth;
            self.carrying_auth = false;
        }
        let next_host = host_from_url(self.url.clone());
        // Quota units belong to the original host. A foreign bucket uses its
        // own default instead of charging units the caller never declared.
        self.cost = cost_override_for_hop(self.host.as_deref(), next_host.as_deref(), self.cost);
        self.resolve_rate_key(account);
        Ok(())
    }
}

/// Extract host string from a URL. Returns `None` if the URL is not
/// parseable; the rate-limit governor then no-ops for this request.
fn host_from_url(url: impl reqwest::IntoUrl) -> Option<String> {
    url.into_url()
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned))
}

/// Decide which quota scope a wire attempt debits.
///
/// An explicit `RequestBuilder::quota_scope` wins for every hop of the
/// request. Otherwise the account's declared scope for this host
/// applies; an account that declared no scope for the host falls back
/// to the empty host-only bucket, which is also the only bucket an
/// unregistered host could ever match.
fn resolve_quota_scope(
    account: &AccountNet,
    host: Option<&str>,
    override_: Option<&crate::QuotaScope>,
) -> crate::QuotaScope {
    match override_ {
        Some(scope) => scope.clone(),
        None => {
            // Only an unscoped request to a multi-scope host is ambiguous.
            // Warn here, per request, rather than at attach: an operator
            // should hear about the misrouted request, not about a correct
            // per-surface declaration.
            if let Some(host) = host
                && account.has_ambiguous_rate_scope(host)
            {
                tracing::warn!(
                    host = %host,
                    default_scope = %account.rate_scope_for(host),
                    "request reached a host with several declared quota scopes \
                     without RequestBuilder::quota_scope; debiting the \
                     first-declared scope",
                );
            }
            host.map_or(crate::QuotaScope::HostOnly, |host| {
                account.rate_scope_for(host).clone()
            })
        }
    }
}

/// Decide whether a per-request `.cost(n)` override still applies after
/// a redirect hop.
///
/// A cost is denominated in the quota units of the host the caller
/// addressed: Graph counts requests, Gmail counts quota units, and a
/// host the caller never named counts whatever it counts. Carrying the
/// number verbatim across an origin boundary debits a foreign bucket in
/// units nobody agreed on - and an override larger than the new host's
/// burst is rejected outright as `CostExceedsBurst`, failing a request
/// the caller never over-costed. On a host change the override is
/// dropped so the hop re-resolves that host's registered default, which
/// is what the no-override path already did per hop. A same-host hop
/// keeps it: the units mean the same thing on the same bucket.
///
/// Host comparison is case-insensitive (RFC 3986 section 3.2.2), matching the
/// redirect classifier's own same-origin test.
fn cost_override_for_hop(
    prior_host: Option<&str>,
    next_host: Option<&str>,
    cost: Option<u32>,
) -> Option<u32> {
    let same_host = match (prior_host, next_host) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        (None, None) => true,
        _ => false,
    };
    if same_host { cost } else { None }
}

fn recompute_cost_units(
    governor: &crate::rate::RateLimitGovernor,
    host: Option<&str>,
    quota_scope: &crate::QuotaScope,
    cost_override: Option<u32>,
) -> u32 {
    match cost_override {
        Some(n) => n,
        None => host
            .and_then(|h| governor.cost_default_for_scoped(h, quota_scope))
            .unwrap_or(1),
    }
}

/// Strip body-describing headers after a redirect demands the body
/// be dropped (RFC 7231 section 6.4 method rewrite to GET). Without this,
/// the next hop carries `Content-Type` / `Content-Length` /
/// `Content-Encoding` for a body that no longer exists.
fn strip_body_headers(headers: &mut HeaderMap) {
    headers.remove(reqwest::header::CONTENT_TYPE);
    headers.remove(reqwest::header::CONTENT_LENGTH);
    headers.remove(reqwest::header::CONTENT_ENCODING);
}

/// Remove caller-supplied credential headers the redirect walk does not
/// re-mint. Called on every origin change alongside the Authorization strip:
/// a credential is bound to the origin it was set for, so a cookie jar value
/// or proxy credential never follows a redirect to another origin.
fn strip_origin_bound_credentials(headers: &mut HeaderMap) {
    headers.remove(reqwest::header::COOKIE);
    headers.remove(reqwest::header::PROXY_AUTHORIZATION);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RateLimit, RateLimitGovernor};
    use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};

    // ---- cost resolution ---------------------------------------------

    #[test]
    fn cost_precedence_is_override_then_host_default_then_one() {
        let governor = RateLimitGovernor::new();
        governor.register(RateLimit {
            host: "cost.test".to_owned(),
            quota_scope: crate::QuotaScope::HostOnly,
            quota_per_second: 10.0,
            cost_default: 5,
            burst: 50,
        });

        assert_eq!(
            recompute_cost_units(
                &governor,
                Some("cost.test"),
                &crate::QuotaScope::HostOnly,
                Some(9)
            ),
            9,
            "an explicit .cost(n) wins over the host default"
        );
        assert_eq!(
            recompute_cost_units(
                &governor,
                Some("cost.test"),
                &crate::QuotaScope::HostOnly,
                None
            ),
            5,
            "without an override the host's registered default applies"
        );
        assert_eq!(
            recompute_cost_units(
                &governor,
                Some("unknown.test"),
                &crate::QuotaScope::HostOnly,
                None
            ),
            1,
            "an unregistered host falls back to one unit"
        );
        assert_eq!(
            recompute_cost_units(&governor, None, &crate::QuotaScope::HostOnly, None),
            1,
            "an unparseable URL has no host bucket and costs one unit"
        );
    }

    // ---- URL / header helpers ----------------------------------------

    #[test]
    fn host_extraction_handles_ports_userinfo_and_junk() {
        assert_eq!(
            host_from_url("https://a.example/path?q=1"),
            Some("a.example".to_owned())
        );
        assert_eq!(
            host_from_url("https://user:pw@b.example:8443/p"),
            Some("b.example".to_owned()),
            "the bucket key is the host alone, without userinfo or port"
        );
        assert_eq!(host_from_url("not a url"), None);
        assert_eq!(
            host_from_url("https://A.EXAMPLE/"),
            Some("a.example".to_owned()),
            "reqwest lowercases the host, so bucket keys are case-normalised"
        );
    }

    #[test]
    fn body_headers_are_stripped_but_others_survive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(
            reqwest::header::CONTENT_LENGTH,
            HeaderValue::from_static("42"),
        );
        headers.insert(
            reqwest::header::CONTENT_ENCODING,
            HeaderValue::from_static("gzip"),
        );
        headers.insert(
            HeaderName::from_static("x-keep"),
            HeaderValue::from_static("yes"),
        );
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer t"));

        strip_body_headers(&mut headers);

        assert!(!headers.contains_key(reqwest::header::CONTENT_TYPE));
        assert!(!headers.contains_key(reqwest::header::CONTENT_LENGTH));
        assert!(!headers.contains_key(reqwest::header::CONTENT_ENCODING));
        assert!(
            headers.contains_key(HeaderName::from_static("x-keep")),
            "only body-describing headers are dropped on a method rewrite"
        );
        assert!(
            headers.contains_key(AUTHORIZATION),
            "Authorization is the redirect loop's business, not the body strip's"
        );
    }

    #[test]
    fn cost_override_is_kept_per_host_and_dropped_across_hosts() {
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("a.test"), Some(20)),
            Some(20),
            "a same-host hop keeps the caller's units"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("A.Test"), Some(20)),
            Some(20),
            "hosts are case-insensitive; this is the same bucket"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("b.test"), Some(20)),
            None,
            "a cross-host hop must re-resolve the new host's own default"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), None, Some(20)),
            None,
            "a hop with no resolvable host has no bucket the units apply to"
        );
        assert_eq!(
            cost_override_for_hop(Some("a.test"), Some("b.test"), None),
            None,
            "no override stays no override"
        );
    }
}
