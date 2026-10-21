//! Typed errors surfaced by the HTTP transport.
//!
//! Protocol crates map each variant onto their own typed errors and
//! recovery classes. The engine never sees `bifrost_net::Error`
//! directly.
//!
//! `Error::Status` (and `Response` in `request.rs`) carries
//! `reqwest::StatusCode` and `reqwest::header::HeaderMap` straight
//! through. That couples downstream protocol crates to reqwest; a
//! future swap to a different HTTP client would break every
//! pattern-match on status across the four protocol crates.
//! Acceptable for v1 because all four current protocols are on
//! reqwest and `StatusCode` is a thin u16 wrapper. Phase 2 may wrap
//! these in opaque newtypes if the surface fans out further.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bifrost_types::TransmissionState;
use bytes::Bytes;
use reqwest::{StatusCode, header::HeaderMap};
use thiserror::Error;

/// Maximum body bytes captured in `Error::Status`. The terminal-error
/// path runs to completion regardless of body size, so a multi-megabyte
/// HTML error page would otherwise be moved into the typed error and
/// kept alive until the protocol crate dropped the error. 4 KB is large
/// enough to fit a typical JSON error envelope plus stack hint while
/// keeping the error allocation small.
pub const STATUS_BODY_CAP: usize = 4096;
/// Marker appended when `Error::Status::body` was truncated. Callers
/// that pattern-match the body for diagnostics can detect truncation
/// without an extra field on the variant.
const STATUS_BODY_TRUNCATED_MARKER: &[u8] = b" ... (truncated)";
pub(crate) const STATUS_BODY_TIMEOUT_MARKER: &[u8] = b" ... (body read timed out)";
pub(crate) const STATUS_BODY_CONNECTION_MARKER: &[u8] = b" ... (body read failed)";

/// Final HTTP response evidence preserved for retry-budget,
/// rate-limit, and repeated-auth failures. `body` is capped with the
/// same policy as `Error::Status`.
///
/// Deliberately NOT `#[non_exhaustive]`: this is crate-produced EVIDENCE that
/// consumers read, not configuration they build, so sealing it would only stop
/// a consumer from pattern-matching what this crate handed them.
/// `NetErrorContext` is likewise unsealed for the mirror-image reason - it is
/// consumer-constructed and has no constructor. Both are considered; an audit
/// that flags either as a missing seal has found the intended state.
#[derive(Clone, Debug)]
pub struct FinalResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RangeFailureKind {
    LocalInvalid,
    ResponseNotPartial,
    MissingContentRange,
    ContentRangeMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MalformedRedirectKind {
    InvalidLocationEncoding,
    UnresolvableLocation,
}

/// Truncate a response body to at most `STATUS_BODY_CAP` bytes,
/// appending a visible marker when truncation occurred. Used by the
/// retry loop before constructing `Error::Status` so terminal errors
/// do not retain megabyte-sized response payloads.
#[must_use]
pub fn cap_status_body(body: Bytes) -> Bytes {
    if body.len() <= STATUS_BODY_CAP {
        return body;
    }
    let mut buf = Vec::with_capacity(STATUS_BODY_CAP + STATUS_BODY_TRUNCATED_MARKER.len());
    buf.extend_from_slice(&body[..STATUS_BODY_CAP]);
    buf.extend_from_slice(STATUS_BODY_TRUNCATED_MARKER);
    Bytes::from(buf)
}

/// Errors returned by `Net`, `AccountNet`, and `RequestBuilder`.
///
/// Marked `#[non_exhaustive]` to allow new variants without breaking
/// downstream matches.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    /// Local refusal before a wire attempt. Prior outcomes are retained in
    /// order; an empty list is the only case with no dispatched attempt.
    #[error("request refused by local hold {key}")]
    LocalHold {
        key: String,
        held: crate::Held,
        attempts: Vec<Error>,
        /// A prior acknowledged non-idempotent attempt may have committed.
        /// The logical operation must reconcile even if a later GET was held.
        may_have_committed: bool,
    },

    /// A later stage failed after earlier wire attempts. The terminal error
    /// describes that stage; earlier outcomes remain ordered and must be
    /// considered before deciding whether the logical operation is replayable.
    #[error("logical request failed after earlier attempts: {source}")]
    AttemptHistory {
        #[source]
        source: Box<Error>,
        attempts: Vec<Error>,
        may_have_committed: bool,
    },

    /// Connect, reset, DNS, TLS handshake, or other transport-level
    /// failure surfaced by the underlying HTTP stack. The source is
    /// boxed so the public surface does not leak the `hyper` /
    /// `reqwest` error types directly.
    #[error("network error: {message}")]
    Network {
        /// Human-readable description of the underlying failure.
        message: String,
        /// Whether bytes crossed the target request boundary.
        transmission_state: TransmissionState,
        /// Boxed underlying error. Optional because some call sites
        /// synthesize a `Network` error without a backing source.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Per-request timeout elapsed.
    #[error("request timed out")]
    Timeout {
        /// Whether bytes crossed the target request boundary.
        transmission_state: TransmissionState,
    },

    /// TLS handshake or certificate validation failure. Kept distinct
    /// from `Network` so consumers can present trust-store-specific
    /// guidance without sniffing error strings.
    #[error("TLS error: {message}")]
    Tls {
        /// Description of the TLS failure.
        message: String,
        /// Whether bytes crossed the target request boundary.
        transmission_state: TransmissionState,
    },

    /// Server returned a non-success status code outside the retry
    /// set, or returned a status code inside the set but the retry
    /// budget was exhausted before the call escalated to
    /// `RetryBudgetExhausted`.
    #[error("HTTP {code}")]
    Status {
        /// HTTP status code.
        code: StatusCode,
        /// Response body bytes as received.
        body: Bytes,
        /// Response headers as received.
        headers: HeaderMap,
    },

    /// A protocol expected a successful response but received a passed-through
    /// status (for example 304 or a redirect without Location). The transport
    /// itself returns these responses as data; callers opt into this error.
    #[error("unexpected HTTP response {}", response.status)]
    UnexpectedResponse {
        /// Status, headers and bounded diagnostic excerpt of the response.
        response: FinalResponse,
    },

    /// Retry budget exhausted. The final response is preserved when
    /// one was received so protocol crates keep status, headers, and
    /// capped body evidence after the retry loop gives up.
    #[error("retry budget exhausted")]
    RetryBudgetExhausted {
        /// Preserved final response when one was received.
        final_response: Option<FinalResponse>,
        /// `Retry-After` durations honored across attempts, in order.
        retry_after_history: Vec<Duration>,
    },

    /// OAuth token refresh failed or the server returned 401 after a
    /// forced refresh and retry. The protocol crate maps this to a
    /// terminal-auth recovery class.
    #[error("auth lost")]
    AuthLost {
        /// Attempt state for the target request, when there was one.
        transmission_state: Option<TransmissionState>,
        /// Final 401 response evidence for repeated target failures.
        final_response: Option<FinalResponse>,
    },

    /// Server returned 429 and the retry budget was exhausted.
    #[error("rate limited")]
    RateLimited {
        /// Server-supplied retry hint from `Retry-After`, if present.
        retry_after: Option<Duration>,
        /// Preserved 429 response evidence.
        final_response: FinalResponse,
    },

    /// Request was cancelled before completion.
    #[error("request cancelled")]
    Cancelled,

    /// A buffered response body exceeded
    /// `AccountSpec::max_buffered_response`.
    ///
    /// A contract violation rather than a transport failure: the server
    /// answered, and what it answered with is not something this client
    /// agreed to hold in memory. Retrying cannot help, so it must not
    /// classify as transient.
    #[error("response body exceeded the {limit}-byte buffered ceiling")]
    ResponseTooLarge {
        /// The configured ceiling, in bytes.
        limit: usize,
    },

    /// Caller asked the per-host governor to debit a cost that exceeds
    /// the bucket's burst capacity. The bucket can never fill that high,
    /// so the request would block forever; we surface a typed error
    /// instead. Configuration bug rather than runtime condition.
    #[error("request cost {cost} exceeds bucket burst {burst}")]
    CostExceedsBurst {
        /// Requested debit in quota units.
        cost: u32,
        /// Configured burst capacity for the host.
        burst: u32,
    },

    /// Request body could not be serialized to bytes (e.g. `RequestBuilder::json`
    /// received a value whose `Serialize` impl returned an error). The
    /// error is captured at the builder call site and surfaced on the
    /// subsequent `send` so the fluent API does not have to return a
    /// `Result` on every chained setter.
    #[error("request body encoding failed: {message}")]
    EncodeBody {
        /// Human-readable description of the encoding failure.
        message: String,
        /// Boxed underlying error (typically `serde_json::Error`).
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Request header name or value could not be encoded. Surfaced
    /// lazily from `send` like `EncodeBody` so the fluent builder API
    /// remains chainable while malformed headers do not disappear
    /// silently.
    #[error("invalid request header: {message}")]
    InvalidHeader {
        /// Human-readable description of the header failure.
        message: String,
        /// Boxed underlying error from the HTTP header parser.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Local request construction failed before the request crossed
    /// the wire boundary.
    #[error("invalid request {field}: {detail}")]
    InvalidRequest {
        /// Stable producer field name.
        field: &'static str,
        /// Human-readable detail for support diagnostics.
        detail: String,
    },

    /// OAuth refresh attempt failed for a reason other than the refresh
    /// token being rejected. The original error is preserved behind an
    /// `Arc` so multiple waiters on a single-flight refresh can share
    /// it without `Error` having to be `Clone`. Surfaced when the
    /// refresher would otherwise have to flatten a `Network` or
    /// `Timeout` failure into `AuthLost` and lose the transient-vs-
    /// permanent distinction.
    #[error("OAuth refresh failed: {source}")]
    RefreshFailed {
        /// Provider-supplied retry deadline parsed by the auth layer,
        /// when the token endpoint returned a `Retry-After` hint.
        retry_after: Option<SystemTime>,
        /// Underlying error from the token source. Shared with any
        /// concurrent waiters that were single-flighted onto the same
        /// refresh attempt.
        source: Arc<Error>,
    },

    /// Caller requested a byte range but the server returned a response
    /// that did not honor it (either `200 OK` collapsed the range, or
    /// `206 Partial Content` with a `Content-Range` that disagrees with
    /// the request). Returned before any body bytes are yielded so the
    /// caller does not assemble a misaligned blob.
    #[error("range not honored: {message}")]
    RangeNotHonored {
        /// Whether the failure is local input or response evidence.
        kind: RangeFailureKind,
        /// Description of the mismatch.
        message: String,
    },

    /// Server attempted to redirect to a host outside the configured
    /// `RedirectPolicy::trusted_hosts` allowlist. Returned by the
    /// `bifrost-net` redirect loop before any further request is
    /// issued.
    #[error("redirect rejected: {message}")]
    RedirectRejected {
        /// Description of why the redirect was rejected.
        message: String,
    },

    /// A hop in a redirect chain answered `401 Unauthorized` on a
    /// request whose `Authorization` header **this transport itself
    /// stripped** on an earlier cross-origin hop.
    ///
    /// This is deliberately not `AuthLost`. A bare 401 normally means
    /// the credential is dead and the user must re-authorize; here the
    /// credential was never presented, because the redirect walker
    /// removed it when the chain crossed an origin boundary (and a
    /// chain that bounces `A -> B -> A` arrives back at its own origin
    /// unauthenticated). Reporting that as credential loss would drive
    /// a re-authorization prompt for a working account, so the
    /// self-stripped provenance is carried here instead and classifies
    /// as a provider contract violation.
    #[error("unauthenticated redirect hop: {message}")]
    UnauthenticatedRedirectHop {
        /// Description of the chain and the hop that answered 401.
        message: String,
        /// The rejecting hop's response evidence.
        final_response: FinalResponse,
    },

    /// Server returned a malformed redirect response.
    #[error("malformed redirect: {message}")]
    MalformedRedirect {
        /// Malformed redirect category.
        kind: MalformedRedirectKind,
        /// Description of the malformed redirect.
        message: String,
    },

    /// Redirect chain exceeded `RedirectPolicy::max_hops`. The
    /// `bifrost-net` redirect loop surfaces this rather than spin
    /// indefinitely.
    #[error("redirect loop: {hops} hops exceeded the configured maximum")]
    RedirectLoop {
        /// Hops walked before the loop was aborted.
        hops: u16,
    },
}

impl Error {
    /// The error at the failing stage of a logical request. Earlier attempt
    /// evidence remains on `AttemptHistory`; classification must use the whole
    /// error rather than this diagnostic view.
    #[must_use]
    pub fn terminal(&self) -> &Self {
        match self {
            Self::AttemptHistory { source, .. } => source.terminal(),
            error => error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_at_or_under_the_cap_pass_through_unchanged() {
        let short = Bytes::from_static(b"{\"error\":\"nope\"}");
        assert_eq!(cap_status_body(short.clone()), short);

        let exact = Bytes::from(vec![b'x'; STATUS_BODY_CAP]);
        let capped = cap_status_body(exact.clone());
        assert_eq!(capped.len(), STATUS_BODY_CAP);
        assert_eq!(capped, exact, "exactly at the cap is not truncation");
    }

    #[test]
    fn oversize_bodies_are_truncated_with_a_visible_marker() {
        let huge = Bytes::from(vec![b'x'; STATUS_BODY_CAP * 4]);
        let capped = cap_status_body(huge);
        assert_eq!(
            capped.len(),
            STATUS_BODY_CAP + STATUS_BODY_TRUNCATED_MARKER.len()
        );
        assert!(capped.ends_with(STATUS_BODY_TRUNCATED_MARKER));
        assert_eq!(&capped[..STATUS_BODY_CAP], vec![b'x'; STATUS_BODY_CAP]);
    }

    /// One byte past the cap still truncates, so the marker is the
    /// unambiguous signal rather than a length comparison.
    #[test]
    fn one_byte_over_the_cap_truncates() {
        let over = Bytes::from(vec![b'y'; STATUS_BODY_CAP + 1]);
        let capped = cap_status_body(over);
        assert!(capped.ends_with(STATUS_BODY_TRUNCATED_MARKER));
    }

    /// The cap slices on a byte boundary, not a character boundary, so
    /// a multi-byte glyph straddling the cap is split. Pin it: callers
    /// must treat the body as bytes, and `String::from_utf8` on it can
    /// fail even when the original body was valid UTF-8.
    #[test]
    fn truncation_can_split_a_multibyte_character() {
        let mut raw = vec![b'a'; STATUS_BODY_CAP - 1];
        raw.extend_from_slice("é".as_bytes());
        raw.extend_from_slice(&[b'b'; 16]);
        let capped = cap_status_body(Bytes::from(raw));
        assert!(std::str::from_utf8(&capped[..STATUS_BODY_CAP]).is_err());
    }

    #[test]
    fn empty_body_is_preserved() {
        assert_eq!(cap_status_body(Bytes::new()), Bytes::new());
    }

    /// `Error` renders without leaking the response body into the
    /// Display text; consumers read `body` explicitly when they want it.
    #[test]
    fn status_display_names_only_the_code() {
        let err = Error::Status {
            code: StatusCode::IM_A_TEAPOT,
            body: Bytes::from_static(b"secret-token-in-body"),
            headers: HeaderMap::new(),
        };
        let rendered = err.to_string();
        // `StatusCode` renders its canonical reason too, so pin the prefix
        // rather than coupling this to the `http` crate's reason phrases.
        assert!(rendered.starts_with("HTTP 418"), "got {rendered:?}");
        assert!(!rendered.contains("secret-token-in-body"));
    }
}
