//! The DAV request record, response shape, and origin comparison.
//!
//! DAV traffic rides `bifrost-net` like every other HTTP protocol crate, so the
//! retry budget, per-host rate limiting, bandwidth metering, redirect walk,
//! refresh and total deadline are shared. DAV supplies credential-origin
//! admission and dialect header/target rules through net's hop policy.

use std::time::Duration;

use bifrost_types::{AccountError, AccountOperation};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::error::DavProtocol;

/// Wall-clock ceiling on a single DAV request.
pub const DAV_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct DavResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
    /// Effective request URI, after any redirects the walk followed.
    ///
    /// RFC 4918 relative hrefs in a Multi-Status resolve against the effective
    /// request URI, not the URI the caller submitted. The DAV redirect walk
    /// permits same-origin hops, so a PROPFIND on `/calendar` that lands on
    /// `/dav/users/ada/calendar/` is a real deployment shape; resolving
    /// `one.ics` against the submitted URI there mints a wrong native id and a
    /// wrong follow-up request URL.
    pub url: crate::DavHref,
}

/// A DAV response body paired with the effective URI that produced it, so href
/// resolution has the base RFC 4918 requires.
#[derive(Clone)]
pub struct DavBody {
    pub text: String,
    pub url: crate::DavHref,
}

/// A DAV request as an owned record rather than a builder.
///
/// Wrappers assemble this record without reading credentials. The dispatcher
/// forwards its owned bytes and replay declaration to net once; net rebuilds
/// each retry and redirect hop and consults DAV's credential and target policy.
#[derive(Clone)]
pub struct DavRequest {
    pub(crate) method: Method,
    pub(crate) url: Result<crate::DavHref, String>,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<Bytes>,
    /// Declared replay policy for this wire leg. `None` uses the method
    /// default; either way it is resolved against `headers` by
    /// `bifrost_net::request_is_replayable`, the rule net applies too.
    pub(crate) replay: Option<bifrost_net::ReplayPolicy>,
    /// The first header this record could not carry, if any.
    ///
    /// `header` is a fluent builder and cannot return a `Result` without
    /// rewriting every call chain in both dialects, so a rejection is recorded
    /// here and raised by the dispatcher before the request goes out. Silently
    /// dropping it put a request on the wire MISSING a header the caller asked
    /// for - an unconditional PUT where an `If-Match` was intended, say - which
    /// is a far worse failure than a local `Request(Malformed)`.
    pub(crate) invalid_header: Option<String>,
}

// Servers can echo credentials in arbitrary headers and bodies. Keep header
// names and byte counts for support, but never guess which values are safe.
struct DebugHeaders<'a>(&'a HeaderMap);

impl std::fmt::Debug for DebugHeaders<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for name in self.0.keys() {
            map.entry(name, &"<redacted>");
        }
        map.finish()
    }
}

impl std::fmt::Debug for DavResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavResponse")
            .field("status", &self.status)
            .field("headers", &DebugHeaders(&self.headers))
            .field("body_bytes", &self.body.len())
            .field("url", &self.url)
            .finish()
    }
}

impl std::fmt::Debug for DavBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavBody")
            .field("text_bytes", &self.text.len())
            .field("url", &self.url)
            .finish()
    }
}

impl std::fmt::Debug for DavRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavRequest")
            .field("method", &self.method)
            .field("url", &crate::url::diagnostic_origin(self.url()))
            .field("headers", &DebugHeaders(&self.headers))
            .field("body_bytes", &self.body.as_ref().map(bytes::Bytes::len))
            .field("replay", &self.replay)
            .field("invalid_header", &self.invalid_header)
            .finish()
    }
}

impl DavRequest {
    #[must_use]
    pub fn new(method: Method, url: impl Into<String>) -> Self {
        let url = url.into();
        let url = crate::DavHref::parse(&url).map_err(|_| url);
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: None,
            replay: None,
            invalid_header: None,
        }
    }

    /// Set one header.
    ///
    /// A name or value the HTTP grammar rejects is RECORDED rather than
    /// dropped: the dispatcher refuses the request locally with
    /// `Request(Malformed)` before any I/O. Every call site passes either a
    /// `HeaderName` constant or a value it has already validated, so this is
    /// unreachable in practice - but an etag or destination path carrying a
    /// stray control byte used to produce a request that went out without the
    /// header, and a conditional write silently demoted to an unconditional one
    /// is exactly the failure a lost-update guard exists to prevent.
    #[must_use]
    pub fn header<K, V>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<HeaderName>,
        V: TryInto<HeaderValue>,
    {
        match (name.try_into(), value.try_into()) {
            (Ok(name), Ok(value)) => {
                self.headers.insert(name, value);
            }
            (Ok(name), Err(_)) => self.record_invalid_header(name.as_str().to_string()),
            (Err(_), _) => self.record_invalid_header("<unnameable>".to_string()),
        }
        self
    }

    /// Keep the FIRST rejection: it is the one closest to the caller's mistake,
    /// and a later one cannot make the request sendable again.
    fn record_invalid_header(&mut self, name: String) {
        if self.invalid_header.is_none() {
            self.invalid_header = Some(name);
        }
    }

    /// The first header this request could not carry, if any.
    #[must_use]
    pub fn invalid_header(&self) -> Option<&str> {
        self.invalid_header.as_deref()
    }

    #[must_use]
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        for (name, value) in &headers {
            self.headers.insert(name, value.clone());
        }
        self
    }

    #[must_use]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Declare replay safety for this wire leg.
    #[must_use]
    pub fn replay(mut self, policy: bifrost_net::ReplayPolicy) -> Self {
        self.replay = Some(policy);
        self
    }

    /// Compatibility spelling for [`Self::replay`]: `true` declares
    /// `ReplayPolicy::Idempotent`, `false` declares `ReplayPolicy::Never`.
    #[must_use]
    pub fn idempotent(self, idempotent: bool) -> Self {
        self.replay(if idempotent {
            bifrost_net::ReplayPolicy::Idempotent
        } else {
            bifrost_net::ReplayPolicy::Never
        })
    }

    /// Whether this request may be replayed, by the one rule net's retry loop
    /// also applies (`bifrost_net::request_is_replayable`). Net sees these
    /// headers plus the minted `Authorization`, which is not a precondition,
    /// so both sides resolve identically.
    #[must_use]
    pub fn replayable(&self) -> bool {
        bifrost_net::request_is_replayable(self.replay, &self.method, &self.headers)
    }

    #[must_use]
    pub fn url(&self) -> &str {
        match &self.url {
            Ok(href) => href.as_str(),
            Err(raw) => raw,
        }
    }
}

/// Classify a completed DAV response and keep the effective URI attached to the
/// body, so the caller resolves hrefs against the URI that actually served the
/// Multi-Status rather than the one it submitted.
pub fn settle_body(
    response: DavResponse,
    operation: AccountOperation,
    protocol: DavProtocol,
) -> Result<DavBody, AccountError> {
    if response.status.is_success() {
        Ok(DavBody {
            text: response.body,
            url: response.url,
        })
    } else {
        Err(crate::error::status_error(
            &response.headers,
            operation,
            response.status,
            response.body,
            protocol,
        ))
    }
}

/// Whether a URL's scheme carries an authenticated, encrypted transport.
///
/// Only `https` qualifies; an unparseable URL is treated as insecure so the
/// downgrade check fails closed.
#[must_use]
pub fn origin_is_secure(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| url.scheme().eq_ignore_ascii_case("https"))
}

/// The scheme/host/effective-port triple a credential gate compares on.
#[must_use]
pub fn url_origin(value: &str) -> Option<bifrost_net::Origin> {
    let url = Url::parse(value).ok()?;
    bifrost_net::origin_key(&url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_debug_omits_credentials_and_echoed_content() {
        let url =
            "https://user-secret:password-secret@dav.test/path-secret?query-secret#fragment-secret";
        let mut headers = HeaderMap::new();
        for name in [
            "Authorization",
            "Proxy-Authorization",
            "Cookie",
            "Set-Cookie",
            "Location",
            "Destination",
            "X-Echo",
        ] {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_static("header-secret"),
            );
        }
        headers.append(
            "set-cookie",
            HeaderValue::from_static("second-cookie-secret"),
        );
        let request = DavRequest::new(Method::PUT, url)
            .headers(headers.clone())
            .body("body-secret");
        let response = DavResponse {
            status: StatusCode::FORBIDDEN,
            headers,
            body: "body-secret".to_string(),
            url: crate::DavHref::parse(url).unwrap(),
        };
        let body = DavBody {
            text: "body-secret".to_string(),
            url: response.url.clone(),
        };
        let invalid = DavRequest::new(Method::GET, "invalid-url-secret");
        for debug in [
            format!("{:?}", response.url),
            format!("{:#?}", response.url),
            format!("{request:?}"),
            format!("{request:#?}"),
            format!("{response:?}"),
            format!("{response:#?}"),
            format!("{body:?}"),
            format!("{body:#?}"),
            format!("{invalid:?}"),
            format!("{invalid:#?}"),
        ] {
            assert!(!debug.contains("secret"), "credential leaked: {debug}");
        }
        let debug = format!("{request:?}");
        assert!(debug.contains("PUT"));
        assert!(debug.contains("https://dav.test:443"));
        assert!(debug.contains("authorization"));
        assert!(debug.contains("proxy-authorization"));
        assert!(debug.contains("cookie"));
        assert!(debug.contains("set-cookie"));
        assert!(debug.contains("body_bytes: Some(11)"));
        assert!(format!("{response:?}").contains("403"));
        assert!(format!("{body:?}").contains("text_bytes: 11"));
        // Formatting must not mutate the values needed for dispatch or parsing.
        assert_eq!(request.headers["authorization"], "header-secret");
        assert_eq!(response.body, "body-secret");
        assert_eq!(body.text, "body-secret");
        assert_eq!(response.url.as_str(), url);
    }

    #[test]
    fn url_origin_normalizes_case_and_default_port() {
        assert_eq!(
            url_origin("HTTPS://DAV.Example.Test/cal/")
                .map(|origin| origin.to_string())
                .as_deref(),
            Some("https://dav.example.test:443")
        );
        assert_eq!(
            url_origin("https://dav.example.test:8443/cal/")
                .map(|origin| origin.to_string())
                .as_deref(),
            Some("https://dav.example.test:8443")
        );
        assert_eq!(url_origin("not a url"), None);
    }

    #[test]
    fn only_https_counts_as_secure() {
        assert!(origin_is_secure("https://dav.example.test"));
        assert!(!origin_is_secure("http://dav.example.test"));
        // Fails closed rather than open.
        assert!(!origin_is_secure("not a url"));
    }

    /// An invalid header value is RECORDED, not dropped.
    ///
    /// Dropping it produced a request that went out missing a header the caller
    /// asked for - the `If-Match` case turns a conditional write into an
    /// unconditional one, which is the exact failure the validator exists to
    /// prevent. The record is what lets the dispatcher refuse locally instead.
    #[test]
    fn an_invalid_header_value_is_recorded_rather_than_dropped() {
        let request = DavRequest::new(Method::PUT, "https://dav.example.test/one.vcf")
            .header("If-Match", "\"etag\u{7f}\"")
            .header(reqwest::header::CONTENT_TYPE, "text/vcard");
        assert_eq!(request.invalid_header(), Some("if-match"));
        assert!(
            !request.headers.contains_key("if-match"),
            "the rejected value must not reach the header map"
        );
        // The first rejection is the one kept, and a later good header still
        // lands.
        assert!(request.headers.contains_key(reqwest::header::CONTENT_TYPE));
    }

    /// A body no longer makes a request unreplayable. The previous walk held a
    /// `reqwest::RequestBuilder` and called `try_clone`, which returns `None`
    /// for a streaming body, so a redirected PUT failed locally with
    /// "redirected DAV request cannot be replayed" rather than following.
    #[test]
    fn a_request_carrying_a_body_survives_being_rebuilt() {
        let request = DavRequest::new(Method::PUT, "https://dav.example.test/one.ics")
            .header(reqwest::header::CONTENT_TYPE, "text/calendar")
            .body("BEGIN:VCALENDAR");
        let replayed = request.clone();
        assert_eq!(replayed.body, request.body);
        assert_eq!(replayed.headers, request.headers);
        assert_eq!(replayed.method, Method::PUT);
    }
    #[test]
    fn dav_requests_resolve_replay_through_the_net_rule() {
        use bifrost_net::ReplayPolicy;
        assert!(
            DavRequest::new(Method::from_bytes(b"PROPFIND").unwrap(), "https://dav.test")
                .replay(ReplayPolicy::Idempotent)
                .replayable()
        );
        assert!(
            !DavRequest::new(Method::from_bytes(b"MOVE").unwrap(), "https://dav.test")
                .replay(ReplayPolicy::Never)
                .replayable()
        );
        assert!(
            !DavRequest::new(Method::PUT, "https://dav.test")
                .replay(ReplayPolicy::IdempotentUnlessConditional)
                .header("If-Match", "v1")
                .replayable()
        );
        for method in [Method::PUT, Method::DELETE] {
            assert!(DavRequest::new(method.clone(), "https://dav.test").replayable());
            for name in ["If-Match", "If-None-Match"] {
                assert!(
                    !DavRequest::new(method.clone(), "https://dav.test")
                        .header(name, "v1")
                        .replayable()
                );
            }
        }
    }
}
