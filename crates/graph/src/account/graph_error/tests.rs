use bifrost_net::STATUS_BODY_CAP;

use super::*;
use crate::error::{GraphError, GraphResponseError};
use bifrost_types::{
    AccessErrorKind, AccountErrorKind, AuthErrorKind, CursorScope, EngineDirective,
    ProtocolErrorKind, ReconcileAction, ReconcileReason, RecoveryClass, ServerErrorKind,
    SyncStateErrorKind,
};
use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

fn body(json: &str) -> Bytes {
    Bytes::copy_from_slice(json.as_bytes())
}

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        let name = HeaderName::from_bytes(k.as_bytes()).expect("valid header name");
        let value = HeaderValue::from_str(v).expect("valid header value");
        h.insert(name, value);
    }
    h
}

fn classify(
    status: StatusCode,
    json: &str,
    ctx: GraphErrorContext,
    extra_headers: &[(&str, &str)],
) -> AccountError {
    let response = GraphResponseError::from_response(status, headers(extra_headers), body(json));
    into_account_error(GraphError::Response(response), ctx)
}

fn graph_ctx(op: AccountOperation) -> GraphErrorContext {
    GraphErrorContext::graph(op)
}

#[test]
fn shared_http_status_envelope_and_plain_body_agree() {
    let budget = bifrost_types::ThrottleBudget::new("mailbox-test");
    let ctx = graph_ctx(AccountOperation::Hydrate).with_budget(budget.clone());
    for status in [
        413, 400, 404, 408, 409, 410, 412, 422, 429, 500, 502, 503, 504, 507, 599,
    ] {
        let status = StatusCode::from_u16(status).unwrap();
        let response = classify(
            status,
            r#"{"error":{"code":"NewUnknownCode"}}"#,
            ctx.clone(),
            &[("retry-after", "17")],
        );
        let transport = into_account_error(
            GraphError::Net(bifrost_net::Error::Status {
                code: status,
                headers: headers(&[("retry-after", "17")]),
                body: Bytes::from_static(b"plain failure"),
            }),
            ctx.clone(),
        );
        assert_eq!(response.kind(), transport.kind(), "status {status}");
        assert_eq!(response.recovery(), transport.recovery(), "status {status}");
        assert_eq!(
            response.chain().iter().next(),
            transport.chain().iter().next(),
            "status {status}"
        );
        assert_eq!(response.throttle_budgets(), transport.throttle_budgets());
        let expected = if matches!(status.as_u16(), 429 | 507) {
            std::slice::from_ref(&budget)
        } else {
            &[]
        };
        assert_eq!(transport.throttle_budgets(), expected);
    }
}

/// A locally-detected configuration rejection is raised PER id (a stale
/// shared-mailbox message), per cursor scope, or per subscription
/// resource, and every caller supplies the one it means. Routing it
/// through `invalid_account_error` - which takes an operation and
/// nothing else - discarded that scope, so telemetry and every support
/// export carried a bare `request.malformed` an operator could not tie
/// to a message.
#[test]
fn a_configuration_rejection_keeps_the_callers_error_scope() {
    let error = into_account_error(
        GraphError::Configuration {
            message: "shared mailbox not configured on this account: gone@contoso.com".to_string(),
        },
        graph_ctx(AccountOperation::BulkDestroy).with_scope(ErrorScope::Message {
            id: ("AAMkmsg".to_string()).into(),
        }),
    );
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    // Stale configuration is nothing the engine can retry its way out
    // of; the operator has to restore the mailbox or drop the id.
    assert!(error.recovery().is_terminal());
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Message {
            id: ("AAMkmsg".to_string()).into()
        })
    );
}

/// Cursor errors are raised PER cursor, and one of them changes
/// classification on the scope's presence rather than merely losing
/// telemetry: `SyncState(ScopeRevoked)` derives `DisableScope(scope)`
/// with a scope and `RestartAccount` without. A stale shared mailbox is
/// exactly what account-wide rediscovery cannot fix, so the scope-less
/// form sent the engine around a rediscovery loop on every pass instead
/// of quarantining the one folder.
#[test]
fn a_revoked_cursor_scope_disables_that_scope_rather_than_the_account() {
    let scope = CursorScope::FolderType {
        folder: bifrost_types::FolderId("gone@contoso.com\u{1f}AAMk".to_string()),
        ty: bifrost_types::ObjectType::Email,
    };
    let error = cursor_error_to_account_error(
        crate::account::cursor::CursorError::Configuration("mailbox gone".to_string()),
        graph_ctx(AccountOperation::SyncInventory).with_scope(ErrorScope::Cursor(scope.clone())),
    );
    assert!(matches!(
        error.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
    ));
    match error.recovery() {
        RecoveryClass::Engine(EngineDirective::DisableScope(disabled)) => {
            assert_eq!(disabled, &scope);
        }
        other => panic!("expected DisableScope, got {other:?}"),
    }
}

/// The same stamping applies to every other cursor arm, which is the
/// ordinary reason to want it: the support export has to say which
/// cursor the envelope belonged to.
#[test]
fn a_cursor_schema_rejection_names_its_cursor() {
    let scope = CursorScope::FolderType {
        folder: bifrost_types::FolderId("inbox".to_string()),
        ty: bifrost_types::ObjectType::Email,
    };
    let error = cursor_error_to_account_error(
        crate::account::cursor::CursorError::SchemaIncompatible,
        graph_ctx(AccountOperation::SyncChanges).with_scope(ErrorScope::Cursor(scope.clone())),
    );
    assert!(matches!(
        error.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
    ));
    assert_eq!(error.scope(), Some(&ErrorScope::Cursor(scope)));
}

/// A client-side invariant (an unattached client) is
/// `Internal`, terminal: never the `Transport(Network)` retry it used to
/// be dressed as.
#[test]
fn a_graph_internal_error_is_internal_not_a_network_retry() {
    let error = into_account_error(
        GraphError::Internal {
            message: "Graph client is not attached to an account".to_string(),
        },
        graph_ctx(AccountOperation::SyncChanges),
    );
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated)
    );
    assert_eq!(
        error.recovery(),
        &bifrost_types::RecoveryClass::InternalFailure
    );
}

/// A cursor this crate built that will not serialize is the client's
/// own invariant, not a provider contract violation.
#[test]
fn a_cursor_encode_failure_is_internal_not_a_provider_fault() {
    let error = cursor_error_to_account_error(
        crate::account::cursor::CursorError::Encode("serializer refused".to_string()),
        graph_ctx(AccountOperation::SyncChanges),
    );
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated)
    );
    assert_eq!(
        error.recovery(),
        &bifrost_types::RecoveryClass::InternalFailure
    );
}

/// The scope is genuinely optional - the call sites that have no id to
/// name (a factory-level misconfiguration) must still classify.
#[test]
fn a_scopeless_configuration_rejection_still_classifies() {
    let error = into_account_error(
        GraphError::Configuration {
            message: "no shared mailbox".to_string(),
        },
        graph_ctx(AccountOperation::Hydrate),
    );
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert_eq!(error.scope(), None);
}

/// An omitted `$batch` subrequest is AMBIGUOUS, and the shared
/// contract splits that ambiguity on idempotency: read-back the
/// non-idempotent ones, re-issue the idempotent ones. Neither may be
/// terminal - `Protocol(ContractViolation)`, the classification this
/// replaced, derives `ProviderContractViolation`, so the engine filed
/// a move or destroy that may well have LANDED as `failed_terminal`
/// with no read-back at all.
#[test]
fn a_missing_batch_subresponse_is_ambiguous_never_terminal() {
    for (operation, idempotent) in [
        (AccountOperation::Hydrate, true),
        (AccountOperation::UpdateFlags, true),
        (AccountOperation::BulkMove, false),
        (AccountOperation::BulkDestroy, false),
    ] {
        let err = batch_response_missing(
            operation,
            Some(ErrorScope::Message {
                id: ("m1".to_string()).into(),
            }),
            "Graph $batch returned no response for m1",
            true,
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
        ));
        assert!(!err.recovery().is_terminal(), "{operation:?}");
        assert_eq!(err.message_key(), "protocol.partial-response");

        if idempotent {
            assert!(err.recovery().is_retryable(), "{operation:?}");
        } else {
            let RecoveryClass::Reconcile(advice) = err.recovery() else {
                panic!("{operation:?} should reconcile, got {:?}", err.recovery());
            };
            assert_eq!(advice.reason, ReconcileReason::PartialCompletionSignal);
            assert!(
                advice
                    .guidance
                    .actions
                    .contains(&ReconcileAction::CheckTarget)
            );
        }

        // The outer `$batch` envelope decoded, so the request was
        // acknowledged. Defaulting to `Unsent` would tell a consumer
        // nothing crossed the wire.
        assert_eq!(
            err.telemetry_fields().transmission_state,
            Some(TransmissionState::Acknowledged),
            "{operation:?}"
        );
    }
}

#[test]
fn invalid_authentication_token_maps_to_reauth() {
    let err = classify(
        StatusCode::UNAUTHORIZED,
        r#"{"error":{"code":"InvalidAuthenticationToken","message":"x"}}"#,
        graph_ctx(AccountOperation::SyncChanges),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
    ));
    assert!(matches!(err.recovery(), RecoveryClass::AuthLost));
    assert_eq!(err.provider(), Some(Provider::Microsoft));
    assert_eq!(err.protocol(), Some(Protocol::Graph));
}

#[test]
fn access_denied_maps_to_permission_denied() {
    let err = classify(
        StatusCode::FORBIDDEN,
        r#"{"error":{"code":"AccessDenied","message":"nope"}}"#,
        graph_ctx(AccountOperation::Hydrate),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
}

#[test]
fn forbidden_maps_to_permission_denied() {
    let err = classify(
        StatusCode::FORBIDDEN,
        r#"{"error":{"code":"Forbidden","message":"nope"}}"#,
        graph_ctx(AccountOperation::Hydrate),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
}

#[test]
fn access_restricted_maps_to_conditional_access() {
    let err = classify(
        StatusCode::FORBIDDEN,
        r#"{"error":{"code":"AccessRestricted"}}"#,
        graph_ctx(AccountOperation::SyncChanges),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked)
    ));
}

#[test]
fn admin_consent_required_maps_to_admin_consent() {
    let err = classify(
        StatusCode::FORBIDDEN,
        r#"{"error":{"code":"AdminConsentRequired"}}"#,
        graph_ctx(AccountOperation::SyncChanges),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired)
    ));
    assert!(matches!(
        err.recovery(),
        RecoveryClass::NeedsAdminConsent { .. }
    ));
}

#[test]
fn mailbox_not_enabled_maps_to_not_licensed() {
    let err = classify(
        StatusCode::FORBIDDEN,
        r#"{"error":{"code":"MailboxNotEnabledForRESTAPI"}}"#,
        graph_ctx(AccountOperation::SyncInventory),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed)
    ));
}

#[test]
fn mailbox_store_unavailable_is_transient() {
    let err = classify(
        StatusCode::SERVICE_UNAVAILABLE,
        r#"{"error":{"code":"MailboxStoreUnavailable"}}"#,
        graph_ctx(AccountOperation::SyncInventory),
        &[],
    );
    match err.kind() {
        AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
            kind: MailboxUnavailableKind::Transient,
        }) => {}
        other => panic!("expected mailbox unavailable transient, got {other:?}"),
    }
}

#[test]
fn resync_required_maps_to_cursor_invalid_with_engine_restart() {
    let cursor_scope = ErrorScope::Cursor(CursorScope::Account);
    let err = classify(
        StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"ResyncRequired"}}"#,
        graph_ctx(AccountOperation::SyncChanges).with_scope(cursor_scope.clone()),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ));
    match err.recovery() {
        RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
        other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
    }
}

#[test]
fn invalid_delta_token_maps_to_cursor_invalid() {
    let err = classify(
        StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"InvalidDeltaToken"}}"#,
        graph_ctx(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(CursorScope::Account)),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ));
}

#[test]
fn sync_state_not_found_maps_to_cursor_invalid() {
    for code in ["SyncStateNotFound", "syncStateNotFound"] {
        let err = classify(
            StatusCode::BAD_REQUEST,
            &format!(r#"{{"error":{{"code":"{code}"}}}}"#),
            graph_ctx(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(CursorScope::Account)),
            &[],
        );
        assert!(
            matches!(
                err.kind(),
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
            ),
            "code={code} did not classify as CursorInvalid"
        );
    }
}

#[test]
fn novel_400_code_on_cursor_scope_restarts_not_terminal() {
    // A 400 carrying a code this build does not yet type
    // (`GraphSignal::Unknown` -> `classify_by_status`) on a cursor
    // scope is almost certainly a delta-token rejection Graph added
    // later. It must route to the cursor-invalid restart, not a
    // terminal `Request(Malformed)`, matching the typed
    // InvalidDeltaToken / 410 cursor cases (forward-compat).
    let err = classify(
        StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"SomeFutureDeltaTokenSignal"}}"#,
        graph_ctx(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(CursorScope::Account)),
        &[],
    );
    assert!(
        matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ),
        "expected CursorInvalid, got {:?}",
        err.kind()
    );
    match err.recovery() {
        RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
        other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
    }
}

#[test]
fn novel_400_code_off_cursor_scope_stays_malformed() {
    // The same novel 400 outside a cursor scope is a genuine bad
    // request and stays terminal `Request(Malformed)`.
    let err = classify(
        StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"SomeFutureSignal"}}"#,
        graph_ctx(AccountOperation::BulkMove),
        &[],
    );
    assert!(
        matches!(
            err.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ),
        "expected Request(Malformed), got {:?}",
        err.kind()
    );
}

#[test]
fn too_many_requests_carries_retry_hint_and_throttle_scope() {
    let err = classify(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"code":"TooManyRequests"}}"#,
        graph_ctx(AccountOperation::BulkMove),
        &[("retry-after", "30")],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Server(ServerErrorKind::RateLimited)
    ));
    match err.recovery() {
        RecoveryClass::Retry(advice) => {
            assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
            assert!(advice.retry_hint.is_some());
        }
        other => panic!("expected Retry, got {other:?}"),
    }
}

#[test]
fn elapsed_retry_after_date_survives_classification() {
    let date = "Sun, 06 Nov 1994 08:49:37 GMT";
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(784111777);
    let err = classify(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"code":"TooManyRequests"}}"#,
        graph_ctx(AccountOperation::SyncChanges),
        &[("retry-after", date)],
    );
    let RecoveryClass::Retry(advice) = err.recovery() else {
        panic!("expected retry");
    };
    assert_eq!(advice.retry_hint, Some(RetryHint::At(at)));
}

#[test]
fn retry_after_http_date_parses() {
    // RFC 7231 IMF-fixdate format. Use a date deep in the future
    // so the resulting hint is non-zero even on slow CI clocks.
    let err = classify(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"code":"TooManyRequests"}}"#,
        graph_ctx(AccountOperation::SyncChanges),
        &[("retry-after", "Wed, 21 Oct 2099 07:28:00 GMT")],
    );
    match err.recovery() {
        RecoveryClass::Retry(advice) => assert!(advice.retry_hint.is_some()),
        other => panic!("expected Retry, got {other:?}"),
    }
}

#[test]
fn generic_file_error_is_retryable_unavailable() {
    let err = classify(
        StatusCode::INTERNAL_SERVER_ERROR,
        r#"{"error":{"code":"GenericFileError"}}"#,
        graph_ctx(AccountOperation::OpenBlob),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Server(ServerErrorKind::Unavailable)
    ));
}

#[test]
fn precondition_failed_maps_to_concurrency_conflict() {
    let err = classify(
        StatusCode::PRECONDITION_FAILED,
        r#"{"error":{"code":"PreconditionFailed"}}"#,
        graph_ctx(AccountOperation::SetIsRead),
        &[],
    );
    assert!(matches!(err.kind(), AccountErrorKind::ConcurrencyConflict));
    match err.recovery() {
        RecoveryClass::Retry(_) => {}
        other => panic!("expected Retry recovery, got {other:?}"),
    }
}

#[test]
fn http_412_without_signal_still_maps_to_concurrency_conflict() {
    let err = classify(
        StatusCode::PRECONDITION_FAILED,
        "",
        graph_ctx(AccountOperation::BulkMove),
        &[],
    );
    assert!(matches!(err.kind(), AccountErrorKind::ConcurrencyConflict));
}

#[test]
fn http_410_on_delta_maps_to_cursor_invalid() {
    let err = classify(
        StatusCode::GONE,
        "",
        graph_ctx(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(CursorScope::Account)),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ));
    match err.recovery() {
        RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
        other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
    }
}

#[test]
fn http_410_outside_delta_maps_to_provider_server_error() {
    let err = classify(
        StatusCode::GONE,
        "",
        graph_ctx(AccountOperation::OpenBlob),
        &[],
    );
    match err.kind() {
        AccountErrorKind::Server(ServerErrorKind::Error { status: Some(410) }) => {}
        other => panic!("expected Server(Error 410), got {other:?}"),
    }
}

#[test]
fn http_404_with_message_scope_maps_to_notfound_message() {
    let err = classify(
        StatusCode::NOT_FOUND,
        "",
        graph_ctx(AccountOperation::HydrateMessage).with_scope(ErrorScope::Message {
            id: ("abc".to_string()).into(),
        }),
        &[],
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::NotFound(ResourceKind::Message)
    ));
}

#[test]
fn response_evidence_is_safe_to_debug_and_export() {
    // All text channels can echo credentials, including nested messages and ids.
    let response = GraphResponseError::from_response(
        StatusCode::FORBIDDEN,
        HeaderMap::from_iter([
            (
                HeaderName::from_static("request-id"),
                HeaderValue::from_static("Bearer-echoed-credential"),
            ),
            (
                HeaderName::from_static("x-ms-ags-diagnostic"),
                HeaderValue::from_static("echoed-credential"),
            ),
        ]),
        body(
            r#"{"error":{"code":"AccessDenied","message":"echoed-credential","innerError":{"code":"InnerCode","message":"echoed-credential","date":"echoed-credential","request-id":"Bearer-echoed-credential","client-request-id":"Bearer-echoed-credential"}}}"#,
        ),
    );
    assert!(!format!("{response:?}").contains("echoed-credential"));
    let error = response_to_account_error(response, &graph_ctx(AccountOperation::SyncInventory));
    assert_eq!(error.telemetry_fields().request_id, None);
    assert_eq!(error.telemetry_fields().trace_id, None);
    assert_eq!(error.telemetry_fields().status, Some(403));
    let support = format!("{:?}", error.support_internal());
    assert!(support.contains("AccessDenied"));
    assert!(support.contains("InnerCode"));
    assert!(!support.contains("echoed-credential"));
}

#[test]
fn graph_request_ids_are_preserved() {
    let err = classify(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"code":"TooManyRequests","innerError":{"request-id":"inner-rid","client-request-id":"inner-crid"}}}"#,
        graph_ctx(AccountOperation::SyncChanges),
        &[
            ("request-id", "header-rid"),
            ("client-request-id", "header-crid"),
        ],
    );
    let telemetry = err.telemetry_fields();
    assert_eq!(telemetry.request_id, Some("header-rid"));
    assert_eq!(telemetry.trace_id, Some("header-crid"));
}

#[test]
fn unparseable_body_keeps_status_without_exporting_text() {
    let err = classify(
        StatusCode::INTERNAL_SERVER_ERROR,
        "definitely not JSON",
        graph_ctx(AccountOperation::SyncInventory),
        &[],
    );
    // A non-empty 5xx body that does not parse as an envelope was not
    // written by Graph, so it stays on bifrost-net's status mapping
    // (see `net_to_account_error`) rather than becoming a terminal
    // contract violation. Support consent must not export the raw page.
    match err.kind() {
        AccountErrorKind::Server(_) => {}
        other => panic!("expected Server error, got {other:?}"),
    }
    let support: Vec<_> = err.support_consented().support_text.into_iter().collect();
    assert!(!support.iter().any(|t| t.contains("definitely not JSON")));
}

#[test]
fn body_diagnostic_exports_only_size() {
    let large = "x".repeat(STATUS_BODY_CAP * 4);
    let err = classify(
        StatusCode::INTERNAL_SERVER_ERROR,
        &large,
        graph_ctx(AccountOperation::SyncInventory),
        &[],
    );
    let support: Vec<&str> = err.support_consented().support_text;
    // Size evidence remains useful, but no prefix of a page is safe to export.
    assert!(
        support
            .iter()
            .any(|text| text.contains("captured response body bytes"))
    );
    assert!(!support.iter().any(|text| text.contains(&"x".repeat(32))));
}

/// The whole `GraphSignal` table is reachable only if a REST failure
/// carrying an envelope gets re-decoded here. `bifrost-net` converts
/// every 4xx/5xx into an `Error` before this crate sees it, so without
/// the re-decode the table applied to `$batch` subresponses alone and a
/// live delta-token expiry classified `Request(Malformed)` -> terminal
/// `ClientBug` instead of restarting the scope.
#[test]
fn a_transport_status_carrying_a_graph_envelope_classifies_on_its_signal() {
    let error = into_account_error(
        GraphError::Net(bifrost_net::Error::Status {
            code: StatusCode::BAD_REQUEST,
            body: body(r#"{"error":{"code":"InvalidDeltaToken","message":"expired"}}"#),
            headers: HeaderMap::new(),
        }),
        graph_ctx(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(CursorScope::Account)),
    );
    assert!(
        matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ),
        "{:?}",
        error.kind()
    );
    assert!(matches!(
        error.recovery(),
        RecoveryClass::Engine(EngineDirective::RestartScope(_))
    ));

    // Same for a status the retry loop escalated: the envelope rides
    // along in the preserved final response.
    let consent = into_account_error(
        GraphError::Net(bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(bifrost_net::FinalResponse {
                status: StatusCode::FORBIDDEN,
                headers: HeaderMap::new(),
                body: body(r#"{"error":{"code":"AdminConsentRequired"}}"#),
            }),
            retry_after_history: Vec::new(),
            stop_reason: bifrost_net::RetryStopReason::AttemptBudget,
        }),
        graph_ctx(AccountOperation::SyncInventory),
    );
    assert!(matches!(
        consent.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired)
    ));

    // A bodyless failure has no envelope to read, but the status table
    // is then the whole answer and Graph's is the richer one: a bare
    // 412 on a conditioned write is a concurrency conflict the engine
    // resolves with a state refresh, not the opaque server error
    // bifrost-net's table produces.
    let conflict = into_account_error(
        GraphError::Net(bifrost_net::Error::Status {
            code: StatusCode::PRECONDITION_FAILED,
            body: Bytes::new(),
            headers: HeaderMap::new(),
        }),
        graph_ctx(AccountOperation::UpdateFlags),
    );
    assert!(
        matches!(conflict.kind(), AccountErrorKind::ConcurrencyConflict),
        "{:?}",
        conflict.kind()
    );
}

/// No envelope, no refinement. The status is then all the evidence
/// there is, and bifrost-net's mapping additionally carries the
/// transport's own (retry history, transmission state) that a rebuilt
/// response error would drop. It also keeps the non-JSON legs honest:
/// an edge device answering the Autodiscover POST with an HTML 403 is
/// a permission failure, not the `Protocol(ContractViolation)` the
/// Graph-envelope classifier reads a non-envelope body as.
#[test]
fn a_transport_status_without_a_graph_envelope_keeps_the_net_classification() {
    let error = into_account_error(
        GraphError::Net(bifrost_net::Error::Status {
            code: StatusCode::FORBIDDEN,
            body: body("<html><body>Forbidden</body></html>"),
            headers: HeaderMap::new(),
        }),
        graph_ctx(AccountOperation::Discover),
    );
    assert!(
        matches!(
            error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ),
        "{:?}",
        error.kind()
    );
}

#[test]
fn non_envelope_batch_server_failures_keep_status_recovery() {
    for status in [500, 502, 503, 504, 507] {
        let ctx = graph_ctx(AccountOperation::Hydrate);
        let empty = batch_status_error(status, HeaderMap::new(), Bytes::new(), &ctx);
        let malformed = batch_status_error(
            status,
            HeaderMap::new(),
            body("<html>upstream failure</html>"),
            &ctx,
        );
        assert_eq!(malformed.kind(), empty.kind(), "HTTP {status}");
        assert_eq!(malformed.recovery(), empty.recovery(), "HTTP {status}");
    }
}

#[test]
fn net_error_delegates_to_net_conversion() {
    let net_err = bifrost_net::Error::Network {
        message: "connection reset".to_string(),
        transmission_state: TransmissionState::InFlight,
        source: None,
    };
    let err = into_account_error(
        GraphError::Net(net_err),
        graph_ctx(AccountOperation::SyncChanges),
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network)
    ));
    assert_eq!(err.protocol(), Some(Protocol::Graph));
    assert_eq!(err.provider(), Some(Provider::Microsoft));
}

#[test]
fn ews_soap_fault_classifies_to_account_error_with_ews_protocol() {
    let ctx = GraphErrorContext::ews(AccountOperation::PushStream);
    let detail = DiagnosticText::support_only("ErrorAccessDenied".to_string());
    let err = ews_error_to_account_error(
        EwsError::SoapFault {
            code: SoapFaultCode::Client,
            detail,
        },
        ctx,
    );
    assert_eq!(err.protocol(), Some(Protocol::Ews));
    assert_eq!(err.provider(), Some(Provider::Microsoft));
    match err.kind() {
        AccountErrorKind::Server(ServerErrorKind::Error { status: Some(400) }) => {}
        other => panic!("expected Server(Error 400), got {other:?}"),
    }
}

#[test]
fn ews_transport_classifies_via_net_conversion() {
    let net = bifrost_net::Error::Network {
        message: "connection reset".to_string(),
        transmission_state: TransmissionState::InFlight,
        source: None,
    };
    let err = ews_error_to_account_error(
        EwsError::Transport(net),
        GraphErrorContext::ews(AccountOperation::PushStream),
    );
    assert_eq!(err.protocol(), Some(Protocol::Ews));
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
    ));
}

#[test]
fn ews_malformed_xml_is_protocol_parse_failed() {
    let err = ews_error_to_account_error(
        EwsError::MalformedXml(DiagnosticText::support_only("bad xml".to_string())),
        GraphErrorContext::ews(AccountOperation::PushStream),
    );
    assert_eq!(err.protocol(), Some(Protocol::Ews));
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
    ));
}

#[test]
fn invalid_batch_statuses_are_contract_violations() {
    for status in [0, 99, 1000, u16::MAX] {
        let outcome = mutation_item_outcome(
            status,
            HeaderMap::new(),
            Bytes::new(),
            crate::account::batch::AbsentMeans::Failure,
            bifrost_types::ObjectId("m1".into()),
            AccountOperation::UpdateFlags,
            ErrorScope::Message { id: "m1".into() },
            None,
        );
        assert!(matches!(outcome, bifrost_types::ItemOutcome::Failed(f)
            if f.error.kind() == &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)));
    }
}

#[test]
fn mutation_2xx_is_applied() {
    let outcome = mutation_item_outcome(
        204,
        HeaderMap::new(),
        Bytes::new(),
        crate::account::batch::AbsentMeans::Failure,
        bifrost_types::ObjectId("m1".to_string()),
        AccountOperation::UpdateFlags,
        ErrorScope::Message {
            id: ("m1".to_string()).into(),
        },
        None,
    );
    assert!(matches!(
        outcome,
        bifrost_types::ItemOutcome::Succeeded(ref s) if matches!(s.output, bifrost_types::MutationSuccess::Applied)
    ));
}

#[test]
fn mutation_destroy_404_is_skipped() {
    let outcome = mutation_item_outcome(
        404,
        HeaderMap::new(),
        Bytes::new(),
        crate::account::batch::AbsentMeans::Success,
        bifrost_types::ObjectId("m1".to_string()),
        AccountOperation::BulkDestroy,
        ErrorScope::Message {
            id: ("m1".to_string()).into(),
        },
        None,
    );
    assert!(matches!(
        outcome,
        bifrost_types::ItemOutcome::Succeeded(ref s) if matches!(s.output, bifrost_types::MutationSuccess::Skipped)
    ));
}

#[test]
fn mutation_non_destroy_404_is_failed_notfound() {
    let outcome = mutation_item_outcome(
        404,
        HeaderMap::new(),
        Bytes::new(),
        crate::account::batch::AbsentMeans::Failure,
        bifrost_types::ObjectId("m1".to_string()),
        AccountOperation::SetIsRead,
        ErrorScope::Message {
            id: ("m1".to_string()).into(),
        },
        None,
    );
    match outcome {
        bifrost_types::ItemOutcome::Failed(failure) => {
            assert!(matches!(
                failure.error.kind(),
                AccountErrorKind::NotFound(ResourceKind::Message)
            ));
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn mutation_412_is_failed_concurrency_conflict_not_skipped() {
    let outcome = mutation_item_outcome(
        412,
        HeaderMap::new(),
        Bytes::new(),
        crate::account::batch::AbsentMeans::Failure,
        bifrost_types::ObjectId("m1".to_string()),
        AccountOperation::UpdateFlags,
        ErrorScope::Message {
            id: ("m1".to_string()).into(),
        },
        None,
    );
    match outcome {
        bifrost_types::ItemOutcome::Failed(failure) => {
            assert!(matches!(
                failure.error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ));
        }
        other => panic!("expected Failed (412 must not be Skipped), got {other:?}"),
    }
}

#[test]
fn mutation_429_carries_retry_hint_and_throttle_scope() {
    let outcome = mutation_item_outcome(
        429,
        headers(&[("retry-after", "10")]),
        Bytes::new(),
        crate::account::batch::AbsentMeans::Failure,
        bifrost_types::ObjectId("m1".to_string()),
        AccountOperation::BulkMove,
        ErrorScope::Message {
            id: ("m1".to_string()).into(),
        },
        None,
    );
    match outcome {
        bifrost_types::ItemOutcome::Failed(failure) => {
            assert!(matches!(
                failure.error.kind(),
                AccountErrorKind::Server(ServerErrorKind::RateLimited)
            ));
            match failure.error.recovery() {
                RecoveryClass::Retry(advice) => {
                    assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
                    assert!(advice.retry_hint.is_some());
                }
                other => panic!("expected Retry, got {other:?}"),
            }
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn mutation_429_routes_non_idempotent_with_retry_classification() {
    // Bulk-move per-item 429: with `AttemptCause(Acknowledged)`
    // pushed at the response boundary, recovery resolves to
    // `Retry::SameRequest` rather than `ProviderRefused`.
    let outcome = mutation_item_outcome(
        429,
        HeaderMap::new(),
        Bytes::new(),
        crate::account::batch::AbsentMeans::Failure,
        bifrost_types::ObjectId("m1".to_string()),
        AccountOperation::BulkMove,
        ErrorScope::Message {
            id: ("m1".to_string()).into(),
        },
        None,
    );
    let bifrost_types::ItemOutcome::Failed(failure) = outcome else {
        panic!("expected Failed");
    };
    match failure.error.recovery() {
        RecoveryClass::Retry(_) => {}
        other => panic!("expected Retry, got {other:?}"),
    }
}

#[test]
fn json_parse_failure_is_protocol_parse_failed() {
    let err = into_account_error(
        GraphError::Json {
            message: "expected `,` or `}`".to_string(),
            body: Some(body("{not-json")),
        },
        graph_ctx(AccountOperation::SyncInventory),
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
    ));
}

/// `GraphError::Json` also carries the Autodiscover XML parsers' and the
/// OneDrive 202 range read's failures. The diagnostic used to call every
/// one of them a "Graph response JSON parse failed", so a support export
/// for a truncated Autodiscover XML answer named the wrong format and
/// the wrong service. A format-neutral label also keeps parser prose private.
#[test]
fn a_non_json_parse_failure_is_not_labelled_a_json_parse() {
    for message in [
        "Autodiscover GetUserSettings: malformed XML: unexpected end",
        "OneDrive 202 nextExpectedRanges entry is not a byte range",
    ] {
        let err = into_account_error(
            GraphError::Json {
                message: message.to_string(),
                body: None,
            },
            graph_ctx(AccountOperation::Discover),
        );
        let debug = format!("{err:?}");
        assert!(!debug.contains("JSON"), "{debug}");
        assert!(!debug.contains(message), "{debug}");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        );
    }
}

// `GraphAccount::remove_from_container` is a one-line tail call to
// `unsupported_account_error(RemoveFromContainer)`, so pinning the
// helper fully pins the method's behavior end-to-end: the returned
// `AccountError` carries the exact `Unsupported(RemoveFromContainer)`
// kind and operation, not a generic protocol error.
#[test]
fn remove_from_container_is_unsupported() {
    let err = unsupported_account_error(AccountOperation::RemoveFromContainer);
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Unsupported(AccountOperation::RemoveFromContainer)
    ));
    assert_eq!(err.operation(), Some(AccountOperation::RemoveFromContainer));
    assert!(matches!(err.recovery(), RecoveryClass::Unsupported(_)));
}

fn forbidden_response() -> GraphError {
    GraphError::Response(GraphResponseError::from_response(
        StatusCode::FORBIDDEN,
        HeaderMap::new(),
        body(r#"{"error":{"code":"Forbidden","message":"nope"}}"#),
    ))
}

fn foreign_email_scope() -> CursorScope {
    CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("shared@contoso.com", "AAMk"),
        ty: bifrost_types::ObjectType::Email,
    }
}

fn primary_email_scope() -> CursorScope {
    CursorScope::FolderType {
        folder: bifrost_types::FolderId("inbox".to_string()),
        ty: bifrost_types::ObjectType::Email,
    }
}

#[test]
fn shared_mailbox_permission_loss_derives_disable_scope() {
    let scope = foreign_email_scope();
    let owner = PrincipalId("shared@contoso.com".to_string());
    let err = graph_shared_scope_error(
        forbidden_response(),
        &scope,
        Some(&owner),
        GraphErrorContext::graph(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(scope.clone())),
    );

    assert!(matches!(
        err.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
    ));
    assert_eq!(
        *err.recovery(),
        RecoveryClass::Engine(EngineDirective::DisableScope(scope.clone()))
    );
    assert_eq!(err.scope(), Some(&ErrorScope::Cursor(scope)));
    let original = std::error::Error::source(&err)
        .unwrap()
        .downcast_ref::<AccountError>()
        .unwrap();
    assert_eq!(
        original.kind(),
        &AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
    );
    assert_eq!(err.protocol(), original.protocol());
    assert!(
        original
            .chain()
            .iter()
            .all(|cause| { err.chain().iter().any(|retained| retained == cause) })
    );
}

#[test]
fn public_folder_access_denied_quarantines_scope() {
    // A public folder's content mailbox is always its owner, so an
    // EWS ErrorAccessDenied quarantines just this scope via the A5a
    // ScopeRevoked path rather than escalating account-wide.
    let scope = CursorScope::Folder(bifrost_types::FolderId("AAMkPF=".to_string()));
    let owner = PrincipalId("content@contoso.com".to_string());
    let err = ews_shared_scope_error(
        EwsError::SoapFault {
            code: SoapFaultCode::ErrorAccessDenied,
            detail: DiagnosticText::support_only("ErrorAccessDenied".to_string()),
        },
        &scope,
        Some(&owner),
        GraphErrorContext::ews(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(scope.clone())),
    );

    assert!(matches!(
        err.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
    ));
    assert_eq!(
        *err.recovery(),
        RecoveryClass::Engine(EngineDirective::DisableScope(scope))
    );
    let original = std::error::Error::source(&err)
        .unwrap()
        .downcast_ref::<AccountError>()
        .unwrap();
    assert_eq!(
        original.kind(),
        &AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
    );
    assert_eq!(err.protocol(), original.protocol());
    assert!(
        original
            .chain()
            .iter()
            .all(|cause| { err.chain().iter().any(|retained| retained == cause) })
    );
}

// End-to-end through the WIRE parse path: a 200-OK EWS body whose
// ResponseMessage is `ResponseClass="Error"` / `ErrorAccessDenied`
// (the canonical EWS error shape, NOT a SOAP `<Fault>`) must reach
// `check_response_error` -> `EwsError::SoapFault` ->
// `ews_shared_scope_error` -> `ScopeRevoked` -> `DisableScope`. The
// direct-construction test above bypasses the parse; this one proves
// the quarantine is reachable from the actual response body.
#[test]
fn wire_error_access_denied_quarantines_public_folder_scope() {
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Error">
          <m:MessageText>Access is denied.</m:MessageText>
          <m:ResponseCode>ErrorAccessDenied</m:ResponseCode>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
    let parsed = crate::ews::check_response_error(xml)
        .expect_err("ResponseClass=Error must surface as an EwsError");

    let scope = CursorScope::Folder(bifrost_types::FolderId("AAMkPF=".to_string()));
    let owner = PrincipalId("content@contoso.com".to_string());
    let err = ews_shared_scope_error(
        parsed,
        &scope,
        Some(&owner),
        GraphErrorContext::ews(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(scope.clone())),
    );

    assert!(matches!(
        err.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
    ));
    assert_eq!(
        *err.recovery(),
        RecoveryClass::Engine(EngineDirective::DisableScope(scope))
    );
}

// End-to-end through the wire parse: `ErrorServerBusy` in a 200-OK
// body must reach `Server(RateLimited)` with an `Account` throttle
// scope so the engine holds the EWS lane's next session.
#[test]
fn wire_error_server_busy_is_rate_limited_with_account_throttle() {
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Error">
          <m:MessageText>The server is busy.</m:MessageText>
          <m:ResponseCode>ErrorServerBusy</m:ResponseCode>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;
    let parsed = crate::ews::check_response_error(xml).expect_err("ServerBusy must surface");
    let err =
        ews_error_to_account_error(parsed, GraphErrorContext::ews(AccountOperation::PushStream));
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Server(ServerErrorKind::RateLimited)
    ));
    match err.recovery() {
        RecoveryClass::Retry(advice) => {
            assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
        }
        other => panic!("expected Retry, got {other:?}"),
    }
}

#[test]
fn streaming_subscription_response_codes_reconnect() {
    for code in [
        "ErrorSubscriptionNotFound",
        "ErrorInvalidSubscription",
        "ErrorSubscriptionUnsubscribed",
        "ErrorInvalidWatermark",
        "ErrorInternalServerTransientError",
    ] {
        let xml = format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Error">
          <m:MessageText>subscription problem</m:MessageText>
          <m:ResponseCode>{code}</m:ResponseCode>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#
        );
        let parsed = crate::ews::check_response_error(&xml)
            .expect_err("an Error-classed response must surface");
        let err = ews_error_to_account_error(
            parsed,
            GraphErrorContext::ews(AccountOperation::PushStream),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
        assert!(
            err.recovery().is_retryable(),
            "{code} recovery {:?} is not retryable",
            err.recovery()
        );
    }
}

#[test]
fn ews_item_not_found_is_a_scoped_not_found_not_a_contract_violation() {
    // The classified EWS codes must keep their typed mapping: a
    // missing public-folder item is a per-item NotFound, so a hydration
    // batch reports it against that id rather than tearing down.
    let err = ews_error_to_account_error(
        EwsError::SoapFault {
            code: SoapFaultCode::ErrorItemNotFound,
            detail: DiagnosticText::support_only("gone".to_string()),
        },
        GraphErrorContext::ews(AccountOperation::Hydrate).with_scope(ErrorScope::Message {
            id: ("AAMkItem=".to_string()).into(),
        }),
    );
    assert!(matches!(
        err.kind(),
        AccountErrorKind::NotFound(ResourceKind::Message)
    ));
}

#[test]
fn nc2b_review_ews_not_found_keeps_existing_mail_read_and_unscoped_defaults() {
    let message = ErrorScope::Message {
        id: "public-message".into(),
    };
    for (operation, scope) in [
        (AccountOperation::Hydrate, Some(message.clone())),
        (AccountOperation::HydrateMessage, Some(message.clone())),
        (AccountOperation::OpenBlob, Some(message.clone())),
        (
            AccountOperation::SyncInventory,
            Some(ErrorScope::Cursor(CursorScope::Folder(
                bifrost_types::FolderId("public-folder".into()),
            ))),
        ),
        (
            AccountOperation::SyncChanges,
            Some(ErrorScope::Cursor(CursorScope::Folder(
                bifrost_types::FolderId("public-folder".into()),
            ))),
        ),
        (AccountOperation::PushStream, None),
    ] {
        let error = ews_error_to_account_error(
            EwsError::SoapFault {
                code: SoapFaultCode::ErrorItemNotFound,
                detail: DiagnosticText::support_only("gone".to_string()),
            },
            GraphErrorContext {
                scope: scope.clone(),
                ..GraphErrorContext::ews(operation)
            },
        );
        assert_eq!(
            error.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Message)
        );
        assert_eq!(error.scope(), scope.as_ref());
        assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Request(RequestCause::NotFound { what: ResourceKind::Message, id }) if id.as_deref() == if scope.as_ref() == Some(&message) { Some("public-message") } else { None })));
    }
}

#[test]
fn ews_mailbox_move_in_progress_is_retryable_not_terminal() {
    let err = ews_error_to_account_error(
        EwsError::SoapFault {
            code: SoapFaultCode::ErrorMailboxMoveInProgress,
            detail: DiagnosticText::support_only("moving".to_string()),
        },
        GraphErrorContext::ews(AccountOperation::SyncChanges),
    );
    assert!(!err.recovery().is_terminal());
}

#[test]
fn primary_mailbox_permission_loss_stays_terminal() {
    let scope = primary_email_scope();
    // A primary folder is untagged (`owner == None`): the same 403
    // must still derive terminal `NoPermission`, not quarantine.
    let err = graph_shared_scope_error(
        forbidden_response(),
        &scope,
        None,
        GraphErrorContext::graph(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(scope.clone())),
    );

    assert!(matches!(
        err.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    assert!(err.recovery().is_terminal());
    assert!(matches!(err.recovery(), RecoveryClass::NoPermission { .. }));
}

#[test]
fn ews_fault_and_xml_exports_withhold_provider_prose() {
    let marker = "password=echoed-credential";
    for error in [
        EwsError::SoapFault {
            code: SoapFaultCode::ErrorServerBusy,
            detail: DiagnosticText::support_only(marker),
        },
        EwsError::MalformedXml(DiagnosticText::support_only(marker)),
    ] {
        let error = ews_error_to_account_error(
            error,
            GraphErrorContext::ews(AccountOperation::SyncChanges),
        );
        assert!(!format!("{error:?}").contains(marker));
        assert!(
            !error
                .support_consented()
                .support_text
                .iter()
                .any(|s| s.contains(marker))
        );
    }
}

#[test]
fn auxiliary_provider_prose_never_enters_support_exports() {
    let ctx = graph_ctx(AccountOperation::Discover);
    for code in ["InvalidRequest", "Bearer-echoed-credential"] {
        let error = autodiscover_error_code_to_account_error(
            "Autodiscover",
            code,
            Some("Bearer echoed-credential"),
            &ctx,
        );
        assert!(!format!("{:?}", error.support_internal()).contains("echoed-credential"));
        let error = id_translation_refused(
            AccountOperation::PushSubscribe,
            ErrorScope::Account,
            Some(code),
            Some("Bearer echoed-credential"),
        );
        assert!(!format!("{:?}", error.support_internal()).contains("echoed-credential"));
    }
}
