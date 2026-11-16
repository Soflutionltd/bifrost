//! Scripted DAV wire seam, shared by the CalDAV and CardDAV test suites.
//!
//! DAV rides `bifrost-net`, so the place to script a DAV flow is
//! `bifrost_net::test_support` - below retry, rate limiting, the byte meter and
//! the buffered ceiling, which is the layer no seam above `AccountNet` can
//! reach. This module is the DAV-shaped front for it: the two crates were
//! scripting `DavResponse` values through a local `DavTransport` double, and
//! that double sat ABOVE the transport, so nothing it pinned said anything
//! about what the real pipeline would do with the same bytes.
//!
//! Two contract facts inherited from the net seam, and they change how a DAV
//! test is written:
//!
//! - DAV selects client refusals other than 401/408/429, plus 501, as data.
//!   A scripted 404 or 501 reaches the DAV status ladder immediately, with its
//!   complete body. A scripted 503 is retried first for replayable requests,
//!   and the script must answer every attempt.
//! - An exhausted script panics rather than reaching a socket, so a test that
//!   under-scripts fails loudly.
//!
//! Gated behind the `test-support` feature; the DAV crates enable it as a
//! dev-dependency.

pub mod collection;
pub mod discovery;

use std::sync::Arc;
use std::time::Duration;

use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_net};
use bifrost_net::{AccountId, AccountNet, AccountSpec, FollowRedirects, NetConfig, RetryPolicy};
use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;

/// A redirect to `location`, for driving the dispatcher's hop walk.
///
/// A redirect is now scripted as the 3xx it is, rather than by handing a
/// response an effective URL that differs from the request's. The old
/// `DavTransport` double did the latter, which meant the walk itself - the
/// re-credentialing per hop, the hop cap, the untrusted-origin refusal - was
/// never on the path a test exercised.
///
/// # Panics
/// Panics if `location` is not a valid header value.
#[must_use]
pub fn dav_redirect(status: StatusCode, location: &str) -> Canned {
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::LOCATION,
        location.parse().expect("valid Location header"),
    );
    Canned::Response {
        status,
        headers,
        body: Bytes::new(),
    }
}

/// What one scripted request looked like on the wire, in the shape DAV
/// assertions want: an owned URL string and a decoded body.
///
/// `RequestSnapshot` carries a `Url` and raw `Bytes`; every DAV assertion
/// compares against string literals, so the conversion lives here once instead
/// of at each of the call sites.
#[derive(Debug, Clone)]
pub struct DavTranscript {
    pub method: reqwest::Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: String,
}

/// Every request the script has answered, in wire order.
#[must_use]
pub fn transcripts(script: &Arc<ScriptedDispatch>) -> Vec<DavTranscript> {
    script
        .requests()
        .into_iter()
        .map(|request| DavTranscript {
            method: request.method,
            url: request.url.to_string(),
            headers: request.headers,
            body: request.body.map_or_else(String::new, |body| {
                String::from_utf8_lossy(&body).into_owned()
            }),
        })
        .collect()
}

/// Build a scripted wire dispatcher answering the given outcomes in order.
#[must_use]
pub fn dav_script(steps: impl IntoIterator<Item = impl Into<Canned>>) -> Arc<ScriptedDispatch> {
    ScriptedDispatch::new(steps.into_iter().map(Into::into))
}

/// One retryable response, repeated for every attempt the default budget makes.
///
/// A 5xx or a 429 is now retried before it becomes an error - the capability
/// DAV gained by moving onto `bifrost-net`. A test scripting one such response
/// and expecting one request is asserting against a pipeline this crate no
/// longer has, so the repetition is spelled out rather than left for the
/// "scripted dispatch exhausted" panic to explain.
#[must_use]
pub fn dav_retried(response: crate::DavResponse) -> Vec<Canned> {
    (0..RetryPolicy::default().max_attempts)
        .map(|_| response.clone().into())
        .collect()
}

/// A connection that dies after the request bytes are on the wire and before
/// any status line comes back.
///
/// The one wire outcome that separates a replayable request from an
/// unreplayable one: `bifrost-net` retries this when the request is replayable
/// and surfaces it otherwise. Scripting a SINGLE one of these is what makes a
/// no-replay assertion bite - a request that is replayed exhausts the script and
/// panics rather than quietly passing.
#[must_use]
pub fn dav_dropped_after_send() -> Canned {
    Canned::Error(bifrost_net::Error::Network {
        message: "connection reset before the status line".to_owned(),
        transmission_state: bifrost_types::TransmissionState::InFlight,
        source: None,
    })
}

/// A script that answers nothing.
///
/// Any request at all exhausts it and panics, which is how a test proves a
/// refusal was decided locally rather than after a round trip. Spelled as its
/// own function because an empty array gives [`dav_script`] no element type to
/// infer.
#[must_use]
pub fn dav_script_empty() -> Arc<ScriptedDispatch> {
    ScriptedDispatch::new([])
}

/// A script that yields once before answering, so `peak_in_flight` measures a
/// caller's real fan-out. For the multiget leg-concurrency bounds.
#[must_use]
pub fn dav_script_yielding(
    steps: impl IntoIterator<Item = impl Into<Canned>>,
) -> Arc<ScriptedDispatch> {
    ScriptedDispatch::yielding(steps.into_iter().map(Into::into))
}

/// A scripted `DavResponse` is the status, headers and body it carries.
///
/// Its `url` is deliberately ignored. That field is the EFFECTIVE request URI,
/// which is now an output of the dispatcher's own redirect walk rather than
/// something a transport double gets to assert - a test that wants a request to
/// land somewhere else scripts the 3xx that moves it, via [`dav_redirect`].
impl From<crate::DavResponse> for Canned {
    fn from(response: crate::DavResponse) -> Self {
        Self::Response {
            status: response.status,
            headers: response.headers,
            body: Bytes::from(response.body.into_bytes()),
        }
    }
}

/// An `AccountNet` whose wire dispatch answers from `script`, configured the
/// way a real DAV account is.
///
/// No account token source: DAV selects its own credentials through the hop
/// policy. The DAV dispatcher installs net's redirect walk per request, even
/// when this injected account disables redirects.
#[must_use]
pub fn scripted_dav_net(script: &Arc<ScriptedDispatch>) -> AccountNet {
    scripted_dav_net_capped(script, None)
}

/// [`scripted_dav_net`] with the buffered-response ceiling lowered.
///
/// The production ceiling is 64 MiB, so a test that wants the oversized-body
/// arm would otherwise have to script 64 MiB of body. Lowering the cap reaches
/// the same `Error::ResponseTooLarge` through the same drain loop with a body
/// small enough to write in a test.
#[must_use]
pub fn scripted_dav_net_capped(
    script: &Arc<ScriptedDispatch>,
    max_buffered_response: Option<usize>,
) -> AccountNet {
    let mut spec = AccountSpec::new(None);
    if let Some(limit) = max_buffered_response {
        spec.max_buffered_response = Some(limit);
    }
    spec.follow_redirects = FollowRedirects::Disabled;
    // The production retry budget, with the waits removed. Keeping
    // `max_attempts` real matters: a scripted 503 is retried here exactly as
    // many times as in production, so a test that scripts one and expects one
    // request fails rather than passing against a pipeline it does not have.
    // Zeroing the backoff is what keeps that from costing seconds of sleep.
    let mut retry = RetryPolicy::default();
    retry.initial_backoff = Duration::ZERO;
    retry.max_backoff = Duration::ZERO;
    spec.default_retry = retry;
    scripted_net(script, NetConfig::default())
        .attach_account(AccountId("scripted-dav".to_string()), spec)
}
