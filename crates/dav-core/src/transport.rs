//! The DAV request record, response shape, and origin comparison.
//!
//! DAV traffic rides `bifrost-net` like every other HTTP protocol crate, so the
//! retry budget, per-host rate limiting, bandwidth metering and observability
//! are the shared ones rather than a second implementation. What DAV keeps for
//! itself is the credential-origin gate and the redirect walk that enforces it:
//! `AccountNet` strips `Authorization` on a cross-origin hop with no way to
//! restore it, and its trusted-host allowlist compares hosts where the DAV gate
//! compares scheme, host and effective port. Redirects are therefore disabled on
//! the account spec and walked here, one hop at a time, each with credentials
//! minted for the origin actually being addressed.

use std::time::Duration;

use bifrost_types::{AccountError, AccountOperation};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::error::DavProtocol;

/// Wall-clock ceiling on a single DAV request.
pub const DAV_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
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
    pub url: String,
}

/// A DAV response body paired with the effective URI that produced it, so href
/// resolution has the base RFC 4918 requires.
#[derive(Debug, Clone)]
pub struct DavBody {
    pub text: String,
    pub url: String,
}

/// A DAV request as an owned record rather than a builder.
///
/// The redirect walk re-dispatches the same logical request against a new
/// origin, with the credential for that origin swapped in. A `reqwest`-style
/// builder cannot express that: it is consumed on send and only conditionally
/// cloneable, so the previous walk replayed through `try_clone` and gave up
/// with a local error when a body made the request unclonable. An owned record
/// is rebuildable per hop by construction.
#[derive(Debug, Clone)]
pub struct DavRequest {
    pub(crate) method: Method,
    pub(crate) url: String,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<Bytes>,
    /// Whether replaying this request cannot change server state.
    ///
    /// `bifrost-net` derives replay safety from the method and treats every
    /// extension method as unsafe, which is right for `MOVE` and wrong for
    /// `PROPFIND` and `REPORT` - both are reads, and refusing to retry them
    /// after a dropped connection loses resilience for nothing.
    pub(crate) idempotent: Option<bool>,
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

impl DavRequest {
    #[must_use]
    pub fn new(method: Method, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: HeaderMap::new(),
            body: None,
            idempotent: None,
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

    /// Override the method-derived replay-safety default.
    #[must_use]
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = Some(idempotent);
        self
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
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
pub fn url_origin(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    Some(format!(
        "{}://{}:{}",
        url.scheme().to_ascii_lowercase(),
        host,
        url.port_or_known_default()?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_origin_normalizes_case_and_default_port() {
        assert_eq!(
            url_origin("HTTPS://DAV.Example.Test/cal/").as_deref(),
            Some("https://dav.example.test:443")
        );
        assert_eq!(
            url_origin("https://dav.example.test:8443/cal/").as_deref(),
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
}
