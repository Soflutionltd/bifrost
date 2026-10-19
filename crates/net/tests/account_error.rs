use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bifrost_net::{
    Error, FinalResponse, MalformedRedirectKind, NetErrorContext, RangeFailureKind,
    into_account_error,
};
use bifrost_types::{
    AccountError, AccountErrorKind, AccountOperation, AuthErrorKind, Cause, ErrorScope, Protocol,
    ProtocolErrorKind, Provider, RecoveryClass, RequestCause, RequestErrorKind, ResourceKind,
    RetryDisposition, ServerErrorKind, ThrottleScope, TransmissionState, TransportErrorKind,
};
use bytes::Bytes;
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};

fn ctx(operation: AccountOperation) -> NetErrorContext {
    NetErrorContext {
        provider: None,
        protocol: Protocol::Jmap,
        operation,
        scope: None,
    }
}

fn graph_ctx(operation: AccountOperation) -> NetErrorContext {
    NetErrorContext {
        provider: Some(Provider::Microsoft),
        protocol: Protocol::Graph,
        operation,
        scope: None,
    }
}

fn message_ctx(operation: AccountOperation, id: &str) -> NetErrorContext {
    NetErrorContext {
        provider: None,
        protocol: Protocol::Jmap,
        operation,
        scope: Some(ErrorScope::Message {
            id: (id.to_owned()).into(),
        }),
    }
}

fn convert(error: Error, context: NetErrorContext) -> AccountError {
    into_account_error(error, context)
}

fn network(state: TransmissionState) -> Error {
    Error::Network {
        message: "network dropped".to_owned(),
        transmission_state: state,
        source: None,
    }
}

fn final_response(status: StatusCode, headers: HeaderMap, body: &'static [u8]) -> FinalResponse {
    FinalResponse {
        status,
        headers,
        body: Bytes::from_static(body),
    }
}

fn header(name: &'static str, value: &'static str) -> (HeaderName, HeaderValue) {
    (
        HeaderName::from_static(name),
        HeaderValue::from_static(value),
    )
}

fn support_text_contains(error: &AccountError, needle: &str) -> bool {
    let support = error.support_consented();
    support
        .support_text
        .iter()
        .any(|text| text.contains(needle))
}

#[test]
fn network_unsent_retries_same_request() {
    let err = convert(
        network(TransmissionState::Unsent),
        ctx(AccountOperation::Send),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Transport(TransportErrorKind::Network)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::Unsent)
    );
    assert!(err.recovery().is_retryable());
}

#[test]
fn network_inflight_non_idempotent_reconciles() {
    let err = convert(
        network(TransmissionState::InFlight),
        ctx(AccountOperation::Send),
    );

    assert!(err.recovery().requires_reconciliation());
}

#[test]
fn network_inflight_idempotent_retries() {
    let err = convert(
        network(TransmissionState::InFlight),
        ctx(AccountOperation::UpdateFlags),
    );

    assert!(err.recovery().is_retryable());
}

#[test]
fn acknowledged_body_failure_is_partial_response() {
    let err = convert(
        network(TransmissionState::Acknowledged),
        ctx(AccountOperation::Send),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
    );
    assert!(err.recovery().requires_reconciliation());
}

#[test]
fn timeout_unsent_uses_transport_timeout() {
    let err = convert(
        Error::Timeout {
            transmission_state: TransmissionState::Unsent,
        },
        ctx(AccountOperation::Hydrate),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Transport(TransportErrorKind::Timeout)
    );
    assert_eq!(err.message_key(), "transport.timeout");
}

#[test]
fn tls_acknowledged_defensive_routes_to_protocol_partial_response() {
    let err = convert(
        Error::Tls {
            message: "unexpected TLS body failure".to_owned(),
            transmission_state: TransmissionState::Acknowledged,
        },
        ctx(AccountOperation::Send),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::Acknowledged)
    );
}

#[test]
fn rate_limited_preserves_body_and_headers() {
    let mut headers = HeaderMap::new();
    let (name, value) = header("x-request-id", "req-123");
    headers.insert(name, value);
    let (name, value) = header("retry-after", "30");
    headers.insert(name, value);
    let (name, value) = header("x-ms-ags-diagnostic", "{\"server\":\"graph\"}");
    headers.insert(name, value);

    let err = convert(
        Error::RateLimited {
            retry_after: Some(Duration::from_secs(30)),
            final_response: final_response(
                StatusCode::TOO_MANY_REQUESTS,
                headers,
                br#"{"error":{"code":"TooManyRequests"}}"#,
            ),
        },
        graph_ctx(AccountOperation::Search),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Server(ServerErrorKind::RateLimited)
    );
    let telemetry = err.telemetry_fields();
    assert_eq!(telemetry.request_id, Some("req-123"));
    assert_eq!(telemetry.trace_id, None);
    // Account, not Tenant: Microsoft meters Outlook per mailbox, and this
    // layer has no target to name a narrower budget with.
    assert_eq!(telemetry.throttle_scope, Some(ThrottleScope::Account));
    assert!(support_text_contains(&err, "TooManyRequests"));
    assert!(support_text_contains(&err, "x-ms-ags-diagnostic"));
}

#[test]
fn retry_budget_exhausted_preserves_body_and_headers() {
    let mut headers = HeaderMap::new();
    let (name, value) = header("x-request-id", "req-503");
    headers.insert(name, value);

    let err = convert(
        Error::RetryBudgetExhausted {
            final_response: Some(final_response(
                StatusCode::SERVICE_UNAVAILABLE,
                headers,
                b"temporarily unavailable",
            )),
            retry_after_history: vec![Duration::from_secs(10)],
        },
        ctx(AccountOperation::SyncChanges),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Server(ServerErrorKind::Unavailable)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::Acknowledged)
    );
    assert!(support_text_contains(&err, "temporarily unavailable"));
}

#[test]
fn retry_budget_none_status_defensive_arm() {
    let err = convert(
        Error::RetryBudgetExhausted {
            final_response: None,
            retry_after_history: Vec::new(),
        },
        ctx(AccountOperation::Send),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Transport(TransportErrorKind::Network)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::InFlight)
    );
}

#[test]
fn status_404_with_message_scope_maps_not_found() {
    let err = convert(
        Error::Status {
            code: StatusCode::NOT_FOUND,
            body: Bytes::new(),
            headers: HeaderMap::new(),
        },
        message_ctx(AccountOperation::HydrateMessage, "m1"),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::NotFound(ResourceKind::Message)
    );
    assert!(matches!(
        err.chain().outermost(),
        Cause::Request(RequestCause::NotFound {
            what: ResourceKind::Message,
            id: Some(id),
        }) if id == "m1"
    ));
}

#[test]
fn status_410_cursor_scope_restarts_scope() {
    let err = convert(
        Error::Status {
            code: StatusCode::GONE,
            body: Bytes::new(),
            headers: HeaderMap::new(),
        },
        NetErrorContext {
            provider: None,
            protocol: Protocol::Jmap,
            operation: AccountOperation::SyncChanges,
            scope: Some(ErrorScope::Cursor(bifrost_types::CursorScope::Account)),
        },
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid)
    );
    assert!(err.recovery().requires_engine_action());
}

#[test]
fn malformed_header_is_client_bug_without_attempt() {
    let err = convert(
        Error::InvalidHeader {
            message: "bad header".to_owned(),
            source: None,
        },
        ctx(AccountOperation::Search),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
    assert_eq!(err.telemetry_fields().transmission_state, None);
}

#[test]
fn invalid_request_is_client_bug_without_attempt() {
    let err = convert(
        Error::InvalidRequest {
            field: "url",
            detail: "relative URL rejected".to_owned(),
        },
        ctx(AccountOperation::Search),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
    assert!(matches!(
        err.chain().outermost(),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("url"),
            ..
        })
    ));
    assert_eq!(err.telemetry_fields().transmission_state, None);
}

#[test]
fn malformed_redirect_invalid_encoding_is_contract_violation() {
    assert_malformed_redirect_kind(MalformedRedirectKind::InvalidLocationEncoding);
}

#[test]
fn malformed_redirect_unresolvable_is_contract_violation() {
    assert_malformed_redirect_kind(MalformedRedirectKind::UnresolvableLocation);
}

fn assert_malformed_redirect_kind(kind: MalformedRedirectKind) {
    let err = convert(
        Error::MalformedRedirect {
            kind,
            message: "bad redirect".to_owned(),
        },
        ctx(AccountOperation::Hydrate),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::Acknowledged)
    );
}

#[test]
fn redirect_rejected_is_malformed_request_with_acknowledged_attempt() {
    let err = convert(
        Error::RedirectRejected {
            message: "redirect rejected".to_owned(),
        },
        ctx(AccountOperation::Hydrate),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::Acknowledged)
    );
}

#[test]
fn redirect_loop_is_contract_violation() {
    let err = convert(
        Error::RedirectLoop { hops: 11 },
        ctx(AccountOperation::Hydrate),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::Acknowledged)
    );
}

#[test]
fn encode_body_is_client_bug_without_attempt() {
    let err = convert(
        Error::EncodeBody {
            message: "json encoding failed".to_owned(),
            source: None,
        },
        ctx(AccountOperation::Send),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
    assert_eq!(err.telemetry_fields().transmission_state, None);
}

#[test]
fn auth_lost_refresh_response_preserves_headers_without_target_attempt() {
    let mut headers = HeaderMap::new();
    let (name, value) = header("x-request-id", "auth-401");
    headers.insert(name, value);
    let err = convert(
        Error::AuthLost {
            transmission_state: None,
            final_response: Some(final_response(
                StatusCode::UNAUTHORIZED,
                headers,
                br#"{"error":"invalid_grant"}"#,
            )),
        },
        ctx(AccountOperation::SyncChanges),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
    );
    assert_eq!(err.telemetry_fields().status, Some(401));
    assert_eq!(err.telemetry_fields().request_id, Some("auth-401"));
    assert_eq!(err.telemetry_fields().transmission_state, None);
    assert!(support_text_contains(&err, "invalid_grant"));
}

#[test]
fn cancelled_inflight_non_idempotent_reconciles() {
    let err = convert(Error::Cancelled, ctx(AccountOperation::Send));

    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::InFlight)
    );
    assert!(err.recovery().requires_reconciliation());
}

#[test]
fn refresh_failed_is_auth_refresh_transient() {
    let source: Arc<Error> = Arc::new(network(TransmissionState::Unsent));
    let err = convert(
        Error::RefreshFailed {
            retry_after: None,
            source,
        },
        ctx(AccountOperation::SyncChanges),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient)
    );
    assert!(matches!(
        err.recovery(),
        RecoveryClass::Retry(advice)
            if advice.disposition == RetryDisposition::AfterAuthRefresh
    ));
}

#[test]
fn refresh_failed_with_deadline_propagates_to_recovery() {
    let mut headers = HeaderMap::new();
    let (name, value) = header("retry-after", "30");
    headers.insert(name, value);
    let before = SystemTime::now();
    let deadline = before
        .checked_add(Duration::from_secs(30))
        .expect("test deadline in range");
    let err = convert(
        Error::RefreshFailed {
            retry_after: Some(deadline),
            source: Arc::new(Error::Status {
                code: StatusCode::TOO_MANY_REQUESTS,
                body: Bytes::new(),
                headers,
            }),
        },
        ctx(AccountOperation::SyncChanges),
    );
    let after = SystemTime::now();

    let RecoveryClass::Retry(advice) = err.recovery() else {
        panic!("expected retry recovery");
    };
    let Some(hint) = advice.retry_hint else {
        panic!("expected retry hint");
    };
    let now = before;
    let not_before = hint.not_before(now);
    let lower = before
        .checked_add(Duration::from_secs(30))
        .expect("test lower bound in range");
    let upper = after
        .checked_add(Duration::from_secs(30))
        .expect("test upper bound in range");
    assert_eq!(advice.disposition, RetryDisposition::AfterAuthRefresh);
    assert!(not_before >= lower);
    assert!(not_before <= upper);
}

#[test]
fn status_400_body_is_not_duplicated_in_support_text() {
    let err = convert(
        Error::Status {
            code: StatusCode::BAD_REQUEST,
            body: Bytes::from_static(b"bad input"),
            headers: HeaderMap::new(),
        },
        ctx(AccountOperation::Search),
    );

    let support = err.support_consented();
    let occurrences = support
        .support_text
        .iter()
        .filter(|text| text.contains("bad input"))
        .count();
    assert_eq!(occurrences, 1);
}

#[test]
fn range_response_mismatch_is_contract_violation() {
    let err = convert(
        Error::RangeNotHonored {
            kind: RangeFailureKind::ContentRangeMismatch,
            message: "content-range mismatch".to_owned(),
        },
        ctx(AccountOperation::OpenBlobRange),
    );

    assert_eq!(
        err.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    );
    assert_eq!(
        err.telemetry_fields().transmission_state,
        Some(TransmissionState::Acknowledged)
    );
}

#[test]
fn traceparent_extracts_w3c_trace_id() {
    let mut headers = HeaderMap::new();
    let (name, value) = header(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    );
    headers.insert(name, value);

    let err = convert(
        Error::Status {
            code: StatusCode::SERVICE_UNAVAILABLE,
            body: Bytes::new(),
            headers,
        },
        ctx(AccountOperation::SyncChanges),
    );

    assert_eq!(
        err.telemetry_fields().trace_id,
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
}

#[test]
fn x_ms_ags_diagnostic_is_support_only() {
    let mut headers = HeaderMap::new();
    let (name, value) = header("x-ms-ags-diagnostic", "{\"server\":\"graph\"}");
    headers.insert(name, value);

    let err = convert(
        Error::Status {
            code: StatusCode::SERVICE_UNAVAILABLE,
            body: Bytes::new(),
            headers,
        },
        graph_ctx(AccountOperation::SyncChanges),
    );

    assert_eq!(err.telemetry_fields().trace_id, None);
    assert!(support_text_contains(&err, "x-ms-ags-diagnostic"));
}
