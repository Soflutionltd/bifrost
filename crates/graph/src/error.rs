//! Structured Graph error representation.
//!
//! `GraphError` carries every piece of structured evidence that
//! survives from the Graph wire to the account boundary: HTTP status,
//! headers, Graph `error.code`, `error.message`, `innerError`, and the
//! underlying `bifrost_net::Error` when the request never produced a
//! response. The account layer translates `GraphError` into
//! `bifrost_types::AccountError` through
//! `account::graph_error::into_account_error`; no substring matching is
//! involved in classification.

use bifrost_types::GraphSignal;
use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde::Deserialize;

/// Top-level Graph error. Either the request never received a
/// response (`Net`), the response decoded with a non-success status
/// (`Response`), or a success body failed local parsing (`Json`), among the
/// local and contract variants below.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum GraphError {
    /// A locally detected account configuration mismatch. No request was
    /// issued, so it must not be disguised as a Graph `/me` response.
    Configuration { message: String },

    /// Transport / retry / auth-loss / rate-limit failure surfaced by
    /// `bifrost-net`. The original error is preserved so the account
    /// boundary can delegate to `bifrost_net::into_account_error` with
    /// Graph context.
    Net(bifrost_net::Error),

    /// Graph returned a non-success status. The response is parsed
    /// into a `GraphResponseError` so classification works on the
    /// typed `GraphSignal`, never on `error.message` substrings.
    Response(GraphResponseError),

    /// A provider success body did not parse (`Protocol(ParseFailed)`), not
    /// a transport error. Despite the name it is not JSON-only: Graph REST
    /// JSON, the Autodiscover XML answers, and the OneDrive upload-session
    /// 202 body all raise it, so `message` must name its source and format.
    Json {
        message: String,
        body: Option<Bytes>,
    },

    /// A link a Graph response supplied (`@odata.nextLink`,
    /// `@odata.deltaLink`) named a URL the bearer must not go to. Raised at
    /// receipt, so the provider's answer has been read and the refused
    /// follow-up was never sent.
    ProviderLinkRefused { reason: String },

    /// A state this crate believes impossible was reached: a client used
    /// with no attached `AccountNet`. The
    /// client's fault, never the provider's or the network's, and not
    /// something a retry of the same request can repair
    /// (`Internal(InvariantViolated)`).
    Internal { message: String },

    /// A complete Graph (or OneDrive) response that is well-formed but
    /// semantically impossible: an offset that does not advance, a completion
    /// signal the protocol does not allow at that point. The provider's
    /// contract breach, acknowledged (`Protocol(ContractViolation)`); a body
    /// that does not parse at all stays `Json`.
    ContractViolation { message: String },

    /// An implementation-defined safety or resource limit this crate imposes
    /// on itself was reached (a pagination walk's page budget, say). Not a
    /// provider fault and not a bug: `Internal(LimitExceeded)`.
    LimitExceeded { message: String },

    /// A local facility this crate depends on failed at runtime (the system
    /// entropy source, say): `Internal(RuntimeFailure)`.
    RuntimeFailure { message: String },

    /// A throttle-shaped failure (429 / 503 / 507) of a request charged to one
    /// Outlook mailbox, tagged at the REST funnel with that mailbox's
    /// throttle budget. Raised only by `GraphClient`, which alone knows
    /// which mailbox a request was routed to; `source` is the failure
    /// exactly as it would otherwise have surfaced, and every reader that
    /// asks about the failure itself (`response_status`) sees through the
    /// tag.
    MailboxThrottled {
        budget: bifrost_types::ThrottleBudget,
        source: Box<GraphError>,
    },
}

/// Parsed Graph error response.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) struct GraphResponseError {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
    /// `error.code` decoded into the typed signal enum. Stable
    /// Microsoft codes map onto named variants; everything else lands
    /// in `GraphSignal::Unknown { code }` carrying the verbatim token.
    /// `None` distinguishes "no Graph error envelope was parseable"
    /// from "envelope decoded with an empty `code`"; the boundary
    /// translates the former into `WireCause::MalformedResponse` while
    /// the latter still flows through `GraphSignal::Unknown`.
    pub(crate) signal: Option<GraphSignal>,
    /// `error.message`. Kept for support-only diagnostic text. Must
    /// never drive classification.
    pub(crate) message: Option<String>,
    /// Parsed `innerError`, when present.
    pub(crate) inner: Option<GraphInnerError>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub(crate) struct GraphInnerError {
    pub(crate) code: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) request_id: Option<String>,
    pub(crate) client_request_id: Option<String>,
    pub(crate) date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: Option<String>,
    message: Option<String>,
    #[serde(rename = "innerError")]
    inner_error: Option<InnerErrorBody>,
}

#[derive(Debug, Deserialize)]
struct InnerErrorBody {
    code: Option<String>,
    message: Option<String>,
    #[serde(rename = "request-id")]
    request_id: Option<String>,
    #[serde(rename = "client-request-id")]
    client_request_id: Option<String>,
    date: Option<String>,
}

impl GraphError {
    /// The HTTP status the SERVER answered with, wherever that evidence
    /// survives.
    ///
    /// A non-success status reaches this crate two ways and control flow
    /// must not care which. `Response` carries one directly (a `$batch`
    /// subresponse, an EWS HTTP failure, the OneDrive chunk PUT). Every
    /// ordinary REST call instead gets a `bifrost_net::Error`, because the
    /// transport converts 4xx/5xx into `Status` / `RateLimited` /
    /// `RetryBudgetExhausted` / `AuthLost` before returning - matching only
    /// the `Response` shape here meant a 404 from `DELETE /subscriptions`
    /// was never recognized as "already gone" on the live path.
    #[must_use]
    pub(crate) fn response_status(&self) -> Option<StatusCode> {
        match self {
            Self::MailboxThrottled { source, .. } => source.response_status(),
            Self::Response(response) => Some(response.status),
            Self::Net(error) => match error.terminal() {
                bifrost_net::Error::Status { code, .. } => Some(*code),
                bifrost_net::Error::RateLimited { final_response, .. }
                | bifrost_net::Error::RetryBudgetExhausted {
                    final_response: Some(final_response),
                    ..
                }
                | bifrost_net::Error::AuthLost {
                    final_response: Some(final_response),
                    ..
                } => Some(final_response.status),
                _ => None,
            },
            Self::Configuration { .. }
            | Self::Json { .. }
            | Self::ProviderLinkRefused { .. }
            | Self::Internal { .. }
            | Self::ContractViolation { .. }
            | Self::LimitExceeded { .. }
            | Self::RuntimeFailure { .. } => None,
        }
    }
}

impl GraphResponseError {
    /// Parse a Graph error response from its raw status/headers/body.
    /// Best-effort decode: if the body is not a recognizable Graph
    /// error envelope, the result still carries the status and bytes
    /// and the `signal` falls back to `GraphSignal::Unknown` keyed on
    /// the HTTP status reason phrase, so downstream classification
    /// falls through to status-based handling.
    #[must_use]
    pub(crate) fn from_response(status: StatusCode, headers: HeaderMap, body: Bytes) -> Self {
        let envelope: Option<ErrorEnvelope> = if body.is_empty() {
            None
        } else {
            serde_json::from_slice(body.as_ref()).ok()
        };

        let (signal, message, inner) = match envelope {
            Some(envelope) => {
                let code = envelope.error.code.unwrap_or_default();
                let signal = classify_code(&code);
                let inner = envelope.error.inner_error.map(|ie| GraphInnerError {
                    code: ie.code,
                    message: ie.message,
                    request_id: ie.request_id,
                    client_request_id: ie.client_request_id,
                    date: ie.date,
                });
                (Some(signal), envelope.error.message, inner)
            }
            None => (None, None, None),
        };

        Self {
            status,
            headers,
            body,
            signal,
            message,
            inner,
        }
    }
}

/// Map a Graph `error.code` token onto the typed wire signal. Stable
/// Microsoft tokens land on named `GraphSignal` variants; unknown
/// tokens (and the empty string) fall through to `Unknown { code }`
/// carrying the verbatim provider string so the audit can flag stable
/// codes that need to be promoted under the wire-enum escape hatch.
#[must_use]
pub(crate) fn classify_code(code: &str) -> GraphSignal {
    match code {
        "Gone" => GraphSignal::Gone,
        "InvalidAuthenticationToken" => GraphSignal::InvalidAuthenticationToken,
        "AccessDenied" => GraphSignal::AccessDenied,
        "Forbidden" => GraphSignal::Forbidden,
        "AccessRestricted" => GraphSignal::AccessRestricted,
        "ConditionalAccessBlocked" => GraphSignal::ConditionalAccessBlocked,
        "AdminConsentRequired" => GraphSignal::AdminConsentRequired,
        "MailboxNotEnabledForRESTAPI" => GraphSignal::MailboxNotEnabledForRestApi,
        "MailboxStoreUnavailable" => GraphSignal::MailboxStoreUnavailable,
        "ResyncRequired" => GraphSignal::ResyncRequired,
        "InvalidDeltaToken" => GraphSignal::InvalidDeltaToken,
        // Microsoft has shipped both capitalizations in the wild.
        "SyncStateNotFound" | "syncStateNotFound" => GraphSignal::SyncStateNotFound,
        "TooManyRequests" => GraphSignal::TooManyRequests,
        "GenericFileError" => GraphSignal::GenericFileError,
        "PreconditionFailed" => GraphSignal::PreconditionFailed,
        "NotFound" => GraphSignal::NotFound,
        other => GraphSignal::Unknown {
            code: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(json: &str) -> Bytes {
        Bytes::copy_from_slice(json.as_bytes())
    }

    #[test]
    fn parses_typical_graph_error_envelope() {
        let json = r#"{
            "error": {
                "code": "InvalidAuthenticationToken",
                "message": "Access token is empty.",
                "innerError": {
                    "request-id": "abc-123",
                    "client-request-id": "ccc-999",
                    "date": "2026-05-01T00:00:00"
                }
            }
        }"#;

        let err = GraphResponseError::from_response(
            StatusCode::UNAUTHORIZED,
            HeaderMap::new(),
            body(json),
        );
        assert!(matches!(
            err.signal,
            Some(GraphSignal::InvalidAuthenticationToken)
        ));
        assert_eq!(err.message.as_deref(), Some("Access token is empty."));
        let inner = err.inner.expect("inner present");
        assert_eq!(inner.request_id.as_deref(), Some("abc-123"));
        assert_eq!(inner.client_request_id.as_deref(), Some("ccc-999"));
    }

    #[test]
    fn invalid_delta_token_classifies_to_typed_variant() {
        let json = r#"{"error":{"code":"InvalidDeltaToken","message":"bad token"}}"#;
        let err = GraphResponseError::from_response(
            StatusCode::BAD_REQUEST,
            HeaderMap::new(),
            body(json),
        );
        assert!(
            matches!(err.signal, Some(GraphSignal::InvalidDeltaToken)),
            "expected InvalidDeltaToken, got {:?}",
            err.signal
        );
    }

    #[test]
    fn unknown_code_preserves_verbatim_token() {
        let json = r#"{"error":{"code":"SomeUnknownCode","message":"unknown"}}"#;
        let err = GraphResponseError::from_response(
            StatusCode::BAD_REQUEST,
            HeaderMap::new(),
            body(json),
        );
        match err.signal {
            Some(GraphSignal::Unknown { code }) => assert_eq!(code, "SomeUnknownCode"),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn body_without_envelope_falls_through_to_empty_unknown() {
        let err = GraphResponseError::from_response(
            StatusCode::BAD_GATEWAY,
            HeaderMap::new(),
            body("Bad Gateway\n"),
        );
        // No envelope decoded -> `signal: None`; the boundary turns this
        // into `WireCause::MalformedResponse`.
        assert!(err.signal.is_none());
        assert!(err.message.is_none());
        assert!(err.inner.is_none());
    }

    #[test]
    fn classifies_known_codes() {
        for (token, want) in [
            ("Gone", "Gone"),
            ("ResyncRequired", "ResyncRequired"),
            ("InvalidDeltaToken", "InvalidDeltaToken"),
            ("SyncStateNotFound", "SyncStateNotFound"),
            ("syncStateNotFound", "SyncStateNotFound"),
            ("PreconditionFailed", "PreconditionFailed"),
            ("TooManyRequests", "TooManyRequests"),
            ("MailboxNotEnabledForRESTAPI", "MailboxNotEnabledForRESTAPI"),
        ] {
            // Round-trip through the GraphSignal::code accessor would
            // require access to the inherent impl in bifrost-types,
            // which is `pub` per crate. Match the discriminant via
            // classification to lock in the mapping.
            let signal = classify_code(token);
            let label = match signal {
                GraphSignal::Gone => "Gone",
                GraphSignal::ResyncRequired => "ResyncRequired",
                GraphSignal::InvalidDeltaToken => "InvalidDeltaToken",
                GraphSignal::SyncStateNotFound => "SyncStateNotFound",
                GraphSignal::PreconditionFailed => "PreconditionFailed",
                GraphSignal::TooManyRequests => "TooManyRequests",
                GraphSignal::MailboxNotEnabledForRestApi => "MailboxNotEnabledForRESTAPI",
                other => panic!("token {token} classified as {other:?}"),
            };
            assert_eq!(label, want);
        }
    }
}
