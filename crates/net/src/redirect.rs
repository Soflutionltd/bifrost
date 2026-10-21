//! Method-aware HTTP redirect policy.
//!
//! RFC 7231 §6.4 says 301 / 302 / 303 SHOULD rewrite POST -> GET and
//! drop the request body; 307 / 308 MUST preserve method and body.
//! Reqwest's default `redirect::Policy::limited(10)` follows redirects
//! but does the rewrite blindly (the implementation is conservative
//! about preserving method/body), and it cannot enforce a trusted-
//! host allowlist. The HTTP protocol crates that need both (JMAP for
//! its bearer-stripping; Gmail / Graph for cross-host attachment
//! flows) carried their own redirect loops.
//!
//! This module owns the redirect loop so the protocol crates do not.
//! `bifrost-net`'s request pipeline installs `redirect::Policy::none()`
//! whenever a structured policy is configured and walks 3xx responses
//! itself, applying:
//!
//! - RFC 7231 method rewriting on 301/302/303.
//! - Method/body preservation on 307/308.
//! - Trusted-host allowlist - cross-host hops to hosts outside the
//!   allowlist abort with `Error::RedirectRejected`.
//! - `Authorization` stripping on every cross-host hop, regardless
//!   of allowlist membership, so a bearer token never leaks to a
//!   host the original request did not target.
//! - Maximum hop count (default `10`, matching reqwest's classic
//!   limit) to prevent loop traps.

use std::collections::HashSet;

use reqwest::{
    Method, StatusCode,
    header::{HeaderMap, HeaderValue, LOCATION},
};

use crate::error::{Error, MalformedRedirectKind};

/// Top-level redirect policy. Stored per account on `AccountSpec`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum FollowRedirects {
    /// Do not chase 3xx responses; the caller sees them as a
    /// terminal status the same way they see a 304 Not Modified.
    /// The HTTP client's underlying redirect policy is set to
    /// `redirect::Policy::none()`.
    Disabled,
    /// Walk redirects per RFC 7231, honoring the supplied trust
    /// settings and hop count.
    Enabled(RedirectPolicy),
}

impl FollowRedirects {
    /// Default-on policy with no trusted-host allowlist (every host
    /// is acceptable) and 10 hops.
    #[must_use]
    pub fn default_on() -> Self {
        Self::Enabled(RedirectPolicy::default())
    }
}

impl Default for FollowRedirects {
    fn default() -> Self {
        Self::default_on()
    }
}

/// Configurable redirect policy.
///
/// `trusted_hosts` is the allowlist for cross-host hops. When empty,
/// every cross-host hop is accepted; when populated, only hops whose
/// target host appears in the set are accepted and `Authorization`
/// is still stripped on every cross-host hop regardless of
/// membership.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RedirectPolicy {
    /// Maximum number of redirect hops before the loop aborts with
    /// `Error::RedirectLoop`.
    pub max_hops: u8,
    /// Refuse every hop that changes scheme, host, or effective port.
    pub same_origin_only: bool,
    /// Cross-host targets that the policy accepts. Empty means
    /// "every host is allowed"; populated means strict allowlist.
    /// Host comparison is case-insensitive (HTTP hosts are
    /// case-insensitive in practice and this matches the IETF
    /// guidance in RFC 3986 §3.2.2).
    pub trusted_hosts: HashSet<String>,
}

/// The default redirect hop cap. A named constant rather than a bare
/// literal in `Default` because non-pipeline redirect walkers (dav-core's
/// self-walked loop) cite the same number; changing the default must
/// change them together, visibly.
pub const DEFAULT_MAX_HOPS: u8 = 10;

impl Default for RedirectPolicy {
    fn default() -> Self {
        Self {
            max_hops: DEFAULT_MAX_HOPS,
            same_origin_only: false,
            trusted_hosts: HashSet::new(),
        }
    }
}

impl RedirectPolicy {
    /// Construct a policy with a maximum hop count of `max_hops`
    /// and no trusted-host allowlist.
    #[must_use]
    pub fn with_hops(max_hops: u8) -> Self {
        Self {
            max_hops,
            same_origin_only: false,
            trusted_hosts: HashSet::new(),
        }
    }

    /// Restrict redirects to the departing origin.
    #[must_use]
    pub fn same_origin(mut self) -> Self {
        self.same_origin_only = true;
        self
    }

    /// Add a trusted host to the allowlist. Builder-style; returns
    /// `self`.
    #[must_use]
    pub fn trust_host(mut self, host: impl Into<String>) -> Self {
        self.trusted_hosts.insert(host.into().to_ascii_lowercase());
        self
    }

    /// True iff `host` is acceptable as a cross-host redirect
    /// target. When the allowlist is empty, every host is
    /// acceptable.
    #[must_use]
    pub fn allows_host(&self, host: &str) -> bool {
        if self.same_origin_only {
            return false;
        }
        if self.trusted_hosts.is_empty() {
            return true;
        }
        self.trusted_hosts.contains(&host.to_ascii_lowercase())
    }

    /// The follow / stop decision for one hop. The single rule the
    /// request pipeline's `classify_redirect` applies to decide whether
    /// a hop is admissible.
    ///
    /// A same-origin hop is always admitted; the allowlist governs
    /// cross-origin hops only. `prior` is `None` when the departing URL
    /// is unknown, which is treated conservatively as cross-origin.
    fn admits_hop(&self, prior: Option<&reqwest::Url>, next: &reqwest::Url) -> bool {
        if prior.is_some_and(|prior| same_origin(prior, next)) {
            return true;
        }
        self.allows_host(next.host_str().unwrap_or(""))
    }
}

/// Result of `classify_redirect`: what to do on a 3xx response.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RedirectAction {
    /// The status code is not a redirect we follow (e.g. 304 Not
    /// Modified). Pass it through to the caller as a terminal
    /// status.
    PassThrough,
    /// Follow the redirect. Carries the rewriting decision and the
    /// resolved next-hop URL.
    Follow(RedirectStep),
}

/// One redirect step. The pipeline mints a fresh request from this.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RedirectStep {
    /// Method for the next request. May differ from the prior method
    /// if RFC 7231 §6.4 rewriting applied.
    pub next_method: Method,
    /// Resolved absolute URL of the next hop.
    pub next_url: String,
    /// Whether the body should be preserved on the next hop.
    /// `false` means the body is dropped; the HTTP method change to
    /// GET on 301/302/303 forces this.
    pub preserve_body: bool,
    /// Whether the prior `Authorization` header should be carried
    /// through to the next hop. `false` on every cross-host hop
    /// regardless of allowlist membership; `true` only for same-host
    /// hops where bearer credentials are still scoped correctly.
    pub keep_auth: bool,
}

/// Classify a 3xx response under the configured policy.
///
/// Returns `PassThrough` for redirect statuses we deliberately do
/// not follow (304, 305, 306). Returns `Follow(step)` for the
/// 301/302/303/307/308 set after applying RFC 7231 method-rewrite
/// rules and resolving the relative `Location` against the prior
/// URL.
///
/// Errors:
///
/// - `Error::MalformedRedirect` when `Location` is missing or unparseable.
/// - `Error::RedirectRejected` when the target host is outside the
///   trusted-host allowlist.
///
/// `result_large_err` is allowed here because the redirect path
/// already crosses the same boundary that constructs `Error::Status`;
/// boxing the error would push the box into the request pipeline.
#[allow(clippy::result_large_err)]
pub(crate) fn classify_redirect(
    policy: &RedirectPolicy,
    prior_method: &Method,
    prior_url: &reqwest::Url,
    status: StatusCode,
    headers: &HeaderMap,
) -> Result<RedirectAction, Error> {
    if !matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    ) {
        // 304 Not Modified, 305 Use Proxy (deprecated), 306
        // (reserved/unused) flow through to the caller. Conditional-
        // request callers rely on 304 not being chased.
        return Ok(RedirectAction::PassThrough);
    }
    let Some(location) = headers.get(LOCATION) else {
        // A followed-redirect status with no `Location` header is not a
        // redirect anyone can follow. Rather than treat it as a malformed
        // redirect, hand it back to the caller as a terminal status the same
        // way a 304 is handed back. This is load-bearing for resumable
        // uploads: Google Drive's chunk PUT signals "resume incomplete" with a
        // 308 carrying a `Range` header and NO `Location`, and the cloud chunk
        // loop reads that status + `Range` itself. A present-but-malformed
        // `Location` (below) stays a hard error; only the absent one passes
        // through.
        return Ok(RedirectAction::PassThrough);
    };
    let location_str = location_to_str(location)?;
    let next_url = resolve_location(prior_url, &location_str)?;
    let cross_host = !same_origin(prior_url, &next_url);

    // RFC 7231 §6.4: 301 / 302 / 303 rewrite to GET when the prior
    // method is non-safe. 307 / 308 preserve method+body. The rewrite
    // also drops the request body because the next method (GET) is
    // semantically distinct from the prior POST.
    let safe_prior = prior_method == Method::GET || prior_method == Method::HEAD;
    let (next_method, preserve_body) = match status {
        StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND if !safe_prior => (Method::GET, false),
        StatusCode::SEE_OTHER => (Method::GET, false),
        StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT => {
            (prior_method.clone(), true)
        }
        // 301 / 302 with a safe method: method preserved; body
        // empty on GET/HEAD anyway.
        _ => (prior_method.clone(), prior_method != Method::HEAD),
    };

    if !policy.admits_hop(Some(prior_url), &next_url) {
        let next_host = next_url.host_str().unwrap_or("");
        return Err(Error::RedirectRejected {
            message: format!("redirect to host {next_host:?} rejected by trusted-host allowlist"),
        });
    }

    Ok(RedirectAction::Follow(RedirectStep {
        next_method,
        next_url: next_url.into(),
        preserve_body,
        keep_auth: !cross_host,
    }))
}

#[allow(clippy::result_large_err)]
fn location_to_str(value: &HeaderValue) -> Result<String, Error> {
    value
        .to_str()
        .map(str::to_owned)
        .map_err(|e| Error::MalformedRedirect {
            kind: MalformedRedirectKind::InvalidLocationEncoding,
            message: format!("Location header was not valid UTF-8: {e}"),
        })
}

/// Same-origin test for the redirect hop's `keep_auth` decision.
///
/// HTTP hosts are case-insensitive (RFC 3986 §3.2.2), so a mixed-case
/// redirect back to the same host must NOT be treated as cross-host -
/// otherwise a valid same-host hop needlessly strips `Authorization`
/// and can be rejected by a populated trusted-host allowlist.
///
/// Port is part of the origin (RFC 6454): a redirect that changes only
/// the port (`host:443` -> `host:8443`) IS cross-origin and must strip
/// credentials. `port_or_known_default` collapses the scheme's implicit
/// port (e.g. `https` -> 443) so `https://h/` and `https://h:443/`
/// compare equal.
///
/// Scheme is the third component of an RFC 6454 origin and is compared
/// explicitly. It previously was not, on the reasoning that the
/// pipeline only ever issues `https` - but nothing enforces that, and
/// an `https://h/` to `http://h/` downgrade was classified cross-origin
/// only because `port_or_known_default` happens to answer 443 and 80.
/// A server that spells the downgrade `http://h:443/` would have had
/// the hop treated as same-origin and the bearer token carried onto it
/// in cleartext. A credential-stripping decision must not rest on a
/// port coincidence.
fn same_origin(a: &reqwest::Url, b: &reqwest::Url) -> bool {
    if a.scheme() != b.scheme() {
        return false;
    }
    let host_eq = match (a.host_str(), b.host_str()) {
        (Some(ha), Some(hb)) => ha.eq_ignore_ascii_case(hb),
        (None, None) => true,
        _ => false,
    };
    host_eq && a.port_or_known_default() == b.port_or_known_default()
}

/// Resolve `location` against `base`. Handles absolute URLs and
/// relative paths the same way `reqwest::redirect` does internally.
#[allow(clippy::result_large_err)]
fn resolve_location(base: &reqwest::Url, location: &str) -> Result<reqwest::Url, Error> {
    base.join(location).map_err(|e| Error::MalformedRedirect {
        kind: MalformedRedirectKind::UnresolvableLocation,
        message: format!("Location {location:?} could not be resolved against base: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> reqwest::Url {
        s.parse().expect("test URL parses")
    }

    fn header(name: reqwest::header::HeaderName, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(name, HeaderValue::from_str(value).expect("valid header"));
        h
    }

    /// The precondition the two paths must share: the allowlist governs
    /// cross-origin hops only. A same-origin hop is followed even when
    /// the allowlist does not name that host - which is exactly what
    /// `classify_redirect` does (it tests `allows_host` only under
    /// `cross_host`). Before this was fixed, a populated allowlist that
    /// omitted the origin's own host stopped a plain same-host redirect
    /// here while the pipeline followed it.
    #[test]
    fn a_same_origin_hop_is_followed_even_when_the_allowlist_omits_its_host() {
        let policy = RedirectPolicy::default().trust_host("cdn.example");
        assert!(
            policy.admits_hop(
                Some(&url("https://dav.example/a")),
                &url("https://dav.example/b"),
            ),
            "a same-origin hop must not consult the allowlist"
        );
        assert!(
            policy.admits_hop(
                Some(&url("https://dav.example/a")),
                &url("https://DAV.Example/b"),
            ),
            "hosts are case-insensitive, so this is still the same origin"
        );

        // And the pipeline agrees: same-host hop, allowlist without the
        // host, still a Follow.
        let h = header(LOCATION, "https://dav.example/b");
        let action = classify_redirect(
            &policy,
            &Method::GET,
            &url("https://dav.example/a"),
            StatusCode::FOUND,
            &h,
        )
        .expect("the pipeline follows a same-host hop regardless of the allowlist");
        assert!(matches!(action, RedirectAction::Follow(_)));
    }

    /// The allowlist still bites where it is meant to: a hop that leaves
    /// the origin, including one that changes only the port or only the
    /// scheme, is checked against it.
    #[test]
    fn a_cross_origin_hop_is_checked_against_the_allowlist() {
        let policy = RedirectPolicy::default().trust_host("cdn.example");
        assert!(
            !policy.admits_hop(
                Some(&url("https://dav.example/a")),
                &url("https://evil.example/b"),
            ),
            "an unlisted foreign host is stopped"
        );
        assert!(
            policy.admits_hop(
                Some(&url("https://dav.example/a")),
                &url("https://cdn.example/b"),
            ),
            "a listed foreign host is followed"
        );
        assert!(
            !policy.admits_hop(
                Some(&url("https://dav.example/a")),
                &url("https://dav.example:8443/b"),
            ),
            "port is part of the origin, so this hop is checked and its host is unlisted"
        );
        assert!(
            !policy.admits_hop(None, &url("https://dav.example/b")),
            "an unknown departing origin is treated conservatively as cross-origin"
        );
    }

    #[test]
    fn allows_host_rejects_a_host_outside_the_allowlist() {
        let policy = RedirectPolicy::default().trust_host("dav.example");
        assert!(!policy.allows_host("evil.example"));
        assert!(
            policy.allows_host("DAV.Example"),
            "host comparison is case-insensitive"
        );
    }

    #[test]
    fn an_empty_allowlist_admits_any_host_and_with_hops_sets_the_cap() {
        let policy = RedirectPolicy::with_hops(5);
        assert!(policy.allows_host("anything.example"));
        assert_eq!(policy.max_hops, 5);
        assert_eq!(
            RedirectPolicy::default().max_hops,
            DEFAULT_MAX_HOPS,
            "the default cap is the shared constant"
        );
    }

    #[test]
    fn same_origin_policy_rejects_every_origin_change() {
        let policy = RedirectPolicy::default()
            .same_origin()
            .trust_host("a.example");
        let prior = url("https://a.example/");
        for target in [
            "https://other.example/",
            "https://a.example:8443/",
            "http://a.example:443/",
        ] {
            let result = classify_redirect(
                &policy,
                &Method::GET,
                &prior,
                StatusCode::FOUND,
                &header(LOCATION, target),
            );
            assert!(matches!(result, Err(Error::RedirectRejected { .. })));
        }
        assert!(policy.admits_hop(Some(&prior), &url("https://a.example:443/next")));
    }

    #[test]
    fn pass_through_for_304() {
        let policy = RedirectPolicy::default();
        let h = HeaderMap::new();
        let action = classify_redirect(
            &policy,
            &Method::GET,
            &url("https://a.example/"),
            StatusCode::NOT_MODIFIED,
            &h,
        )
        .expect("304 is pass-through, not an error");
        assert!(matches!(action, RedirectAction::PassThrough));
    }

    #[test]
    fn rewrite_post_to_get_on_303() {
        let policy = RedirectPolicy::default();
        let h = header(LOCATION, "https://a.example/next");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::SEE_OTHER,
            &h,
        )
        .expect("see-other should classify");
        let step = match action {
            RedirectAction::Follow(s) => s,
            RedirectAction::PassThrough => panic!("expected follow"),
        };
        assert_eq!(step.next_method, Method::GET);
        assert!(!step.preserve_body);
    }

    #[test]
    fn preserve_method_and_body_on_307() {
        let policy = RedirectPolicy::default();
        let h = header(LOCATION, "/elsewhere");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect("307 should classify");
        let step = match action {
            RedirectAction::Follow(s) => s,
            RedirectAction::PassThrough => panic!("expected follow"),
        };
        assert_eq!(step.next_method, Method::POST);
        assert!(step.preserve_body);
        assert!(step.keep_auth, "same host preserves auth");
    }

    #[test]
    fn strip_auth_on_cross_host_hop() {
        let policy = RedirectPolicy::default();
        let h = header(LOCATION, "https://b.example/next");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect("cross-host 307 should classify with no allowlist");
        let step = match action {
            RedirectAction::Follow(s) => s,
            RedirectAction::PassThrough => panic!("expected follow"),
        };
        assert!(!step.keep_auth, "cross-host always strips auth");
    }

    #[test]
    fn allowlist_rejects_unknown_host() {
        let policy = RedirectPolicy::default().trust_host("a.example");
        let h = header(LOCATION, "https://c.example/next");
        let err = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect_err("cross-host hop to unlisted host must be rejected");
        assert!(matches!(err, Error::RedirectRejected { .. }));
    }

    #[test]
    fn allowlist_admits_listed_host() {
        let policy = RedirectPolicy::default().trust_host("b.example");
        let h = header(LOCATION, "https://b.example/next");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect("listed host admitted");
        assert!(matches!(action, RedirectAction::Follow(_)));
    }

    #[test]
    fn mixed_case_same_host_keeps_auth_and_is_not_cross_host() {
        // A redirect back to the same host with different letter case is
        // NOT cross-host: HTTP hosts are case-insensitive (RFC 3986
        // §3.2.2). Treating it as cross-host would needlessly strip the
        // bearer and could trip a populated allowlist.
        let policy = RedirectPolicy::default();
        let h = header(LOCATION, "https://A.Example/next");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect("same-host mixed-case 307 should classify");
        let step = match action {
            RedirectAction::Follow(s) => s,
            RedirectAction::PassThrough => panic!("expected follow"),
        };
        assert!(
            step.keep_auth,
            "mixed-case same host must be treated as same-origin"
        );
    }

    #[test]
    fn mixed_case_same_host_admitted_by_allowlist() {
        // With a populated allowlist, a mixed-case same-host hop must not
        // be rejected as a foreign host.
        let policy = RedirectPolicy::default().trust_host("a.example");
        let h = header(LOCATION, "https://A.EXAMPLE/next");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect("mixed-case same host must be admitted by the allowlist");
        assert!(matches!(action, RedirectAction::Follow(_)));
    }

    #[test]
    fn different_port_same_host_is_cross_origin_and_strips_auth() {
        // Port is part of the origin (RFC 6454): a redirect that only
        // changes the port crosses an origin boundary and must strip
        // credentials.
        let policy = RedirectPolicy::default();
        let h = header(LOCATION, "https://a.example:8443/next");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect("different-port 307 should classify");
        let step = match action {
            RedirectAction::Follow(s) => s,
            RedirectAction::PassThrough => panic!("expected follow"),
        };
        assert!(
            !step.keep_auth,
            "a different-port hop is cross-origin and must strip auth"
        );
    }

    #[test]
    fn implicit_and_explicit_default_port_are_same_origin() {
        // `https://h/` and `https://h:443/` are the same origin; the
        // implicit default port must not be treated as a port change.
        let policy = RedirectPolicy::default();
        let h = header(LOCATION, "https://a.example:443/next");
        let action = classify_redirect(
            &policy,
            &Method::POST,
            &url("https://a.example/"),
            StatusCode::TEMPORARY_REDIRECT,
            &h,
        )
        .expect("explicit default port 307 should classify");
        let step = match action {
            RedirectAction::Follow(s) => s,
            RedirectAction::PassThrough => panic!("expected follow"),
        };
        assert!(
            step.keep_auth,
            "explicit :443 equals implicit https default port"
        );
    }

    /// Scheme is part of an RFC 6454 origin. The plain downgrade was
    /// already classified cross-origin, but only as a side effect of
    /// `port_or_known_default` answering 443 for https and 80 for http.
    /// A downgrade that names 443 explicitly defeated that coincidence
    /// and would have carried the bearer token onto a cleartext hop.
    #[test]
    fn a_scheme_downgrade_is_cross_origin_and_strips_auth() {
        let policy = RedirectPolicy::default();
        for location in [
            "http://a.example/next",
            "http://a.example:443/next",
            "http://a.example:80/next",
        ] {
            let h = header(LOCATION, location);
            let action = classify_redirect(
                &policy,
                &Method::POST,
                &url("https://a.example/"),
                StatusCode::TEMPORARY_REDIRECT,
                &h,
            )
            .expect("scheme-downgrade 307 should classify");
            let step = match action {
                RedirectAction::Follow(s) => s,
                RedirectAction::PassThrough => panic!("expected follow"),
            };
            assert!(
                !step.keep_auth,
                "{location} downgrades the scheme; credentials must not follow"
            );
        }
    }

    #[test]
    fn redirect_without_location_passes_through() {
        // A followed-redirect status with no `Location` header is not a
        // redirect anyone can follow; it must pass through to the caller (the
        // resumable-upload chunk loop reads the status + `Range` itself). This
        // pins the load-bearing fix for Google Drive's 308 Resume Incomplete.
        let policy = RedirectPolicy::default();
        let h = HeaderMap::new();
        for status in [
            StatusCode::PERMANENT_REDIRECT,
            StatusCode::TEMPORARY_REDIRECT,
            StatusCode::FOUND,
        ] {
            let action = classify_redirect(
                &policy,
                &Method::PUT,
                &url("https://a.example/"),
                status,
                &h,
            )
            .expect("missing Location is pass-through, not an error");
            assert!(
                matches!(action, RedirectAction::PassThrough),
                "{status} without Location must pass through"
            );
        }
    }

    #[test]
    fn present_but_malformed_location_is_a_malformed_redirect_error() {
        // A present-but-unresolvable `Location` is still a real protocol fault
        // and must stay a hard `MalformedRedirect`. Only the *missing* header
        // (above) becomes pass-through.
        let policy = RedirectPolicy::default();
        let mut h = HeaderMap::new();
        h.insert(
            LOCATION,
            HeaderValue::from_str("http://[bad").expect("valid header bytes"),
        );
        let err = classify_redirect(
            &policy,
            &Method::GET,
            &url("https://a.example/"),
            StatusCode::FOUND,
            &h,
        )
        .expect_err("302 with an unresolvable Location is an error");
        assert!(matches!(
            err,
            Error::MalformedRedirect {
                kind: MalformedRedirectKind::UnresolvableLocation,
                ..
            }
        ));
    }
}
