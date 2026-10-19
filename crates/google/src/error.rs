use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::time::{Duration, SystemTime};

use bifrost_types::AccountOperation;
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};
use serde::Deserialize;

const MAX_BODY_EXCERPT_BYTES: usize = 4096;

/// Result type for Gmail client operations.
pub(crate) type Result<T> = std::result::Result<T, Error>;

/// Base64 alphabet used by a failed Gmail body decoder.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum Base64Encoding {
    /// RFC 4648 URL-safe alphabet without padding, as used by Gmail bodies.
    UrlSafeNoPad,
}

impl Display for Base64Encoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UrlSafeNoPad => f.write_str("base64url without padding"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum GmailService {
    GmailApi,
}

impl Display for GmailService {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::GmailApi => f.write_str("Gmail API"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GmailResponseHeaders {
    pub(crate) retry_after: Option<RetryAfter>,
    pub(crate) retry_at: Option<SystemTime>,
    pub(crate) project: Option<crate::quota::ProjectObservation>,
    pub(crate) request_id: Option<String>,
    pub(crate) trace_id: Option<String>,
}

impl GmailResponseHeaders {
    pub(crate) fn from_headers(headers: &HeaderMap) -> Self {
        Self::from_headers_at(headers, SystemTime::now())
    }

    pub(crate) fn from_headers_at(headers: &HeaderMap, now: SystemTime) -> Self {
        let retry_after = parse_retry_after_value(headers.get(RETRY_AFTER));
        Self {
            retry_at: retry_after.and_then(|hint| hint.resolve(now)),
            retry_after,
            project: None,
            request_id: first_header(
                headers,
                &["x-goog-request-id", "x-google-request-id", "x-request-id"],
            ),
            trace_id: trace_id_from_headers(headers),
        }
    }
}

/// Retain the provider's syntax as well as its once-resolved deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryAfter {
    Delta(Duration),
    At(SystemTime),
}

impl RetryAfter {
    fn resolve(self, now: SystemTime) -> Option<SystemTime> {
        match self {
            Self::Delta(delta) => now.checked_add(delta),
            Self::At(at) => Some(at),
        }
    }
}

fn parse_retry_after_value(value: Option<&HeaderValue>) -> Option<RetryAfter> {
    let raw = value?.to_str().ok()?.trim();
    raw.parse::<u64>()
        .ok()
        .map(|seconds| RetryAfter::Delta(Duration::from_secs(seconds)))
        .or_else(|| httpdate::parse_http_date(raw).ok().map(RetryAfter::At))
}

fn first_header(headers: &HeaderMap, names: &[&str]) -> Option<String> {
    for name in names {
        if let Some(value) = header_text(headers, name) {
            return Some(value);
        }
    }
    None
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    let name = HeaderName::from_bytes(name.as_bytes()).ok()?;
    let value = headers.get(name)?;
    let text = value.to_str().ok()?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_owned())
    }
}

fn trace_id_from_headers(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = header_text(headers, "traceparent") {
        let mut parts = value.split('-');
        let _version = parts.next()?;
        let trace_id = parts.next()?;
        if trace_id.len() == 32 && trace_id.as_bytes().iter().all(u8::is_ascii_hexdigit) {
            return Some(trace_id.to_owned());
        }
    }
    header_text(headers, "x-cloud-trace-context").and_then(|value| {
        let trace_id = match value.split_once('/') {
            Some((trace_id, _)) => trace_id.trim(),
            None => value.trim(),
        };
        if trace_id.is_empty() {
            None
        } else {
            Some(trace_id.to_owned())
        }
    })
}

#[derive(Debug)]
pub(crate) struct GmailResponseError {
    pub(crate) service: GmailService,
    pub(crate) status: u16,
    pub(crate) headers: GmailResponseHeaders,
    pub(crate) body: Bytes,
    pub(crate) envelope: Option<GmailErrorEnvelope>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct GmailErrorEnvelope {
    #[serde(default)]
    pub(crate) message: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) errors: Vec<GmailErrorDetail>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct GmailErrorDetail {
    #[serde(default)]
    pub(crate) reason: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<String>,
}

impl GmailErrorEnvelope {
    /// Returns the first non-empty `errors[].reason`, falling back to
    /// the top-level `status` string, then `None`.
    pub(crate) fn primary_reason(&self) -> Option<&str> {
        for detail in &self.errors {
            if let Some(reason) = detail.reason.as_deref() {
                let trimmed = reason.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed);
                }
            }
        }
        self.status
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Primary human-readable message from the envelope.
    pub(crate) fn primary_message(&self) -> Option<&str> {
        for detail in &self.errors {
            if let Some(msg) = detail.message.as_deref() {
                let trimmed = msg.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed);
                }
            }
        }
        self.message
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

#[derive(Debug)]
pub(crate) enum GmailLocalError {
    Unsupported {
        operation: AccountOperation,
        detail: Option<String>,
    },
    InvalidRequest {
        operation: AccountOperation,
        detail: String,
    },
    InvalidCursor {
        kind: GmailCursorFailure,
        detail: String,
    },
    AccountIdentityMismatch {
        cursor_email: String,
        profile_email: String,
    },
    /// A complete provider response lacked a field it must carry (a created
    /// event with no id, a raw projection with no `raw`). Raised only after
    /// the response arrived, so it maps to `Protocol(MissingField)` with
    /// `AttemptCause(Acknowledged)`. Never raise it before a request is sent:
    /// a missing LOCAL value is `Internal`, and missing caller input is
    /// `InvalidRequest`.
    MissingField {
        field: &'static str,
        detail: String,
    },
    BlobRangeUnsupported {
        blob_id: String,
    },
    /// A state this crate believes impossible was reached (a change stream
    /// with no cursor, say). The client's fault: `Internal(InvariantViolated)`.
    Internal {
        detail: String,
    },
    /// A pagination walk was handed a page token it had already followed:
    /// the provider is cycling, a contract breach no further paging fixes.
    /// `Protocol(ContractViolation)`, acknowledged (a complete response
    /// carried the token).
    PageTokenRepeated {
        detail: String,
    },
    /// A pagination walk reached this crate's own page budget. That is also
    /// what an honestly huge collection looks like, so it is neither a
    /// provider fault nor a bug: the implementation declined to go further
    /// (`Internal(LimitExceeded)`).
    PageBudgetExceeded {
        detail: String,
    },
    /// A complete, successful-status provider response broke the contract of
    /// the exchange it answered: a header it must carry was absent, a value
    /// it carried did not parse, or the value contradicts the protocol. The
    /// provider's fault, `Protocol(_)` by [`ProviderFault`], and
    /// `Acknowledged` because the response arrived whole.
    ProviderResponse {
        fault: ProviderFault,
        detail: String,
    },
    /// A value an EARLIER provider response supplied (the profile's email
    /// address, say) is unusable for the request now being built, and the
    /// request is refused before any byte of it is sent. The provider's
    /// fault, `Protocol(_)` by [`ProviderFault`], but `Unsent`: this
    /// exchange never reached the wire, so it is neither `Acknowledged` nor
    /// the caller's `Request(Malformed)`.
    ProviderValueUnusable {
        fault: ProviderFault,
        detail: String,
    },
    /// This crate's own safety limit stopped a multi-request exchange after
    /// every request sent so far had been answered. `Internal(LimitExceeded)`,
    /// `Acknowledged`: nothing is left in flight, and the provider did nothing
    /// the protocol forbids.
    LimitExceededAfterResponse {
        detail: String,
    },
}

/// Which part of a provider response's contract was broken; selects the
/// `ProtocolErrorKind` of [`GmailLocalError::ProviderResponse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderFault {
    /// A header or field the exchange requires was absent.
    MissingField,
    /// A header or field was present and did not parse.
    ParseFailed,
    /// The response parsed and contradicts the protocol.
    ContractViolation,
}

/// Why a pagination walk stopped following its page token. The two guards
/// every Google walk runs; kept as one type so each walk classifies them the
/// same way (see [`GmailLocalError::PageTokenRepeated`] and
/// [`GmailLocalError::PageBudgetExceeded`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PageRefusal {
    RepeatedToken(String),
    BudgetExceeded(String),
}

impl PageRefusal {
    #[cfg(test)]
    pub(crate) fn detail(&self) -> &str {
        match self {
            Self::RepeatedToken(detail) | Self::BudgetExceeded(detail) => detail,
        }
    }
}

impl From<PageRefusal> for Error {
    fn from(refusal: PageRefusal) -> Self {
        Self::Local(match refusal {
            PageRefusal::RepeatedToken(detail) => GmailLocalError::PageTokenRepeated { detail },
            PageRefusal::BudgetExceeded(detail) => GmailLocalError::PageBudgetExceeded { detail },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum GmailCursorFailure {
    ProtocolMismatch,
    EnvelopeMismatch,
    SchemaMismatch,
    MalformedPayload,
}

impl Display for GmailLocalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { operation, detail } => match detail {
                Some(text) => write!(f, "unsupported operation {operation:?}: {text}"),
                None => write!(f, "unsupported operation {operation:?}"),
            },
            Self::InvalidRequest { operation, detail } => {
                write!(f, "invalid request for {operation:?}: {detail}")
            }
            Self::InvalidCursor { kind, detail } => {
                write!(f, "invalid cursor ({kind:?}): {detail}")
            }
            Self::AccountIdentityMismatch {
                cursor_email,
                profile_email,
            } => write!(
                f,
                "cursor account {cursor_email} does not match open account {profile_email}"
            ),
            Self::MissingField { field, detail } => {
                write!(f, "missing required field `{field}`: {detail}")
            }
            Self::BlobRangeUnsupported { blob_id } => {
                write!(f, "blob {blob_id} does not support range reads")
            }
            Self::Internal { detail } => write!(f, "internal gmail error: {detail}"),
            Self::PageTokenRepeated { detail } | Self::PageBudgetExceeded { detail } => {
                write!(f, "pagination refused: {detail}")
            }
            Self::ProviderResponse { fault, detail } => {
                write!(
                    f,
                    "provider response broke its contract ({fault:?}): {detail}"
                )
            }
            Self::ProviderValueUnusable { fault, detail } => {
                write!(f, "provider-supplied value unusable ({fault:?}): {detail}")
            }
            Self::LimitExceededAfterResponse { detail } => {
                write!(f, "client limit exceeded: {detail}")
            }
        }
    }
}

/// Error type for Gmail API operations.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Error {
    /// A transport-level failure from `bifrost-net`.
    ///
    /// Carries the original `bifrost_net::Error` so the account-side
    /// translation boundary can inspect transmission state, retry-after
    /// history, and other forensic evidence rather than relying on a
    /// flattened string.
    Net(bifrost_net::Error),
    /// Response-time metadata retained across net's internal retry decisions.
    ObservedNet(Box<ObservedNetError>),
    /// Gmail returned an unsuccessful HTTP response.
    ///
    /// The raw body bytes are preserved (capped at
    /// `MAX_BODY_EXCERPT_BYTES`) along with the Gmail JSON envelope
    /// (if parseable), so the account-side mapper can route on
    /// stable Gmail reason codes rather than substring matches.
    Response(Box<GmailResponseError>),
    /// JSON decoding of a successful Gmail response failed.
    JsonDecode {
        service: GmailService,
        source: serde_json::Error,
    },
    /// Gmail body or raw-message base64 data could not be decoded.
    Base64 {
        /// Alphabet that was attempted.
        encoding: Base64Encoding,
        /// Decoder failure.
        source: base64::DecodeError,
    },
    /// A locally-detected error (unsupported operation, malformed
    /// caller input, cursor envelope mismatch, etc.).
    Local(GmailLocalError),
}

#[derive(Debug)]
pub(crate) struct ObservedNetError {
    pub(crate) error: bifrost_net::Error,
    pub(crate) headers: GmailResponseHeaders,
}

impl Error {
    pub(crate) fn response_from_parts(
        service: GmailService,
        status: u16,
        headers: GmailResponseHeaders,
        body: Bytes,
    ) -> Self {
        let body = body_excerpt(body);
        let envelope = parse_gmail_envelope(&body);
        Self::Response(Box::new(GmailResponseError {
            service,
            status,
            headers,
            body,
            envelope,
        }))
    }

    pub(crate) fn base64url(source: base64::DecodeError) -> Self {
        Self::Base64 {
            encoding: Base64Encoding::UrlSafeNoPad,
            source,
        }
    }

    pub(crate) fn unsupported(operation: AccountOperation) -> Self {
        Self::Local(GmailLocalError::Unsupported {
            operation,
            detail: None,
        })
    }

    pub(crate) fn unsupported_with(operation: AccountOperation, detail: impl Into<String>) -> Self {
        Self::Local(GmailLocalError::Unsupported {
            operation,
            detail: Some(detail.into()),
        })
    }

    pub(crate) fn invalid_request(operation: AccountOperation, detail: impl Into<String>) -> Self {
        Self::Local(GmailLocalError::InvalidRequest {
            operation,
            detail: detail.into(),
        })
    }

    pub(crate) fn provider_response(fault: ProviderFault, detail: impl Into<String>) -> Self {
        Self::Local(GmailLocalError::ProviderResponse {
            fault,
            detail: detail.into(),
        })
    }

    pub(crate) fn provider_value_unusable(fault: ProviderFault, detail: impl Into<String>) -> Self {
        Self::Local(GmailLocalError::ProviderValueUnusable {
            fault,
            detail: detail.into(),
        })
    }

    pub(crate) fn limit_exceeded_after_response(detail: impl Into<String>) -> Self {
        Self::Local(GmailLocalError::LimitExceededAfterResponse {
            detail: detail.into(),
        })
    }

    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        Self::Local(GmailLocalError::Internal {
            detail: detail.into(),
        })
    }

    /// See [`GmailLocalError::MissingField`]: only after a complete response.
    pub(crate) fn missing_field(field: &'static str, detail: impl Into<String>) -> Self {
        Self::Local(GmailLocalError::MissingField {
            field,
            detail: detail.into(),
        })
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Net(err) => write!(f, "net transport error: {err}"),
            Self::ObservedNet(observed) => write!(f, "net transport error: {}", observed.error),
            Self::Response(resp) => {
                write!(
                    f,
                    "{} returned HTTP {}: {}",
                    resp.service,
                    resp.status,
                    String::from_utf8_lossy(resp.body.as_ref())
                )
            }
            Self::JsonDecode { service, source } => {
                write!(f, "{service} JSON decode failed: {source}")
            }
            Self::Base64 { encoding, source } => {
                write!(f, "{encoding} decode failed: {source}")
            }
            Self::Local(local) => Display::fmt(local, f),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Net(err) => Some(err),
            Self::ObservedNet(observed) => Some(&observed.error),
            Self::JsonDecode { source, .. } => Some(source),
            Self::Base64 { source, .. } => Some(source),
            Self::Response(_) | Self::Local(_) => None,
        }
    }
}

impl From<bifrost_net::Error> for Error {
    fn from(err: bifrost_net::Error) -> Self {
        // Preserve the structured net error. The account-side
        // translation boundary in `account/error.rs` is responsible
        // for inspecting Status bodies, retry-after, and transmission
        // state via `bifrost_net::into_account_error`. We deliberately
        // do NOT promote `Error::Status` into `Error::Response` here:
        // the body parsing happens after the net error has been
        // contextualized with the Gmail operation/scope.
        Self::Net(err)
    }
}

fn body_excerpt(body: Bytes) -> Bytes {
    if body.len() <= MAX_BODY_EXCERPT_BYTES {
        return body;
    }
    body.slice(..MAX_BODY_EXCERPT_BYTES)
}

/// Gmail wraps error responses under a top-level `error` object. This
/// helper parses only the inner envelope; if the body is not Gmail-
/// shaped JSON the caller falls back to HTTP status classification.
pub(crate) fn parse_gmail_envelope(body: &[u8]) -> Option<GmailErrorEnvelope> {
    #[derive(Deserialize)]
    struct Wrapper {
        error: GmailErrorEnvelope,
    }
    serde_json::from_slice::<Wrapper>(body)
        .ok()
        .map(|w| w.error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gmail_error_envelope() {
        let body = br#"{
            "error": {
                "code": 429,
                "message": "User-rate limit exceeded.",
                "status": "RESOURCE_EXHAUSTED",
                "errors": [
                    {
                        "domain": "usageLimits",
                        "reason": "userRateLimitExceeded",
                        "message": "User Rate Limit Exceeded"
                    }
                ]
            }
        }"#;
        let env = parse_gmail_envelope(body).expect("envelope");
        assert_eq!(env.primary_reason(), Some("userRateLimitExceeded"));
        assert_eq!(env.status.as_deref(), Some("RESOURCE_EXHAUSTED"));
    }

    #[test]
    fn primary_reason_falls_back_to_status() {
        let body = br#"{"error":{"status":"NOT_FOUND","code":404}}"#;
        let env = parse_gmail_envelope(body).expect("envelope");
        assert_eq!(env.primary_reason(), Some("NOT_FOUND"));
    }

    #[test]
    fn non_gmail_body_returns_none() {
        let body = br#"<html>oops</html>"#;
        assert!(parse_gmail_envelope(body).is_none());
    }

    #[test]
    fn response_truncates_long_body() {
        let body = Bytes::from(vec![b'x'; MAX_BODY_EXCERPT_BYTES + 64]);
        let err = Error::response_from_parts(
            GmailService::GmailApi,
            400,
            GmailResponseHeaders::default(),
            body,
        );
        let Error::Response(resp) = err else {
            panic!("expected response error");
        };
        assert_eq!(resp.body.len(), MAX_BODY_EXCERPT_BYTES);
    }
}
