//! Translation of `bifrost-net` transport outcomes into the shared
//! `AccountError` contract.
//!
//! Audit boundary: the 2026-07 google+net bug sweep exercised this module
//! only through its integration suite, not by a line audit; the same is
//! true of `trace.rs` beyond its construction-level invariants. Listed so a
//! future auditor knows where that sweep's coverage stopped.

use std::time::{Duration, SystemTime};

use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, Cause, DiagnosticText, ErrorScope,
    Protocol, ProtocolErrorKind, Provider, RequestCause, RequestErrorKind, ResourceKind, RetryHint,
    ServerCause, ServerErrorKind, StateCause, SyncStateErrorKind, ThrottleScope, TransmissionState,
    TransportCause, TransportErrorKind, TransportKind, WireCause,
};
use bytes::Bytes;
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderName, RETRY_AFTER},
};

use crate::error::{Error, FinalResponse, RangeFailureKind};
use crate::request::parse_retry_after_hint;

#[derive(Clone, Debug)]
pub struct NetErrorContext {
    pub provider: Option<Provider>,
    pub protocol: Protocol,
    pub operation: AccountOperation,
    pub scope: Option<ErrorScope>,
}

#[must_use]
pub fn into_account_error(error: Error, ctx: NetErrorContext) -> AccountError {
    let throttle_scope = legacy_throttle_scope(&ctx);
    into_account_error_with_throttle_scope(error, ctx, throttle_scope)
}

/// Translate a transport error using the caller's provider throttle policy.
///
/// Provider crates know whether a server's throttle should pause an account,
/// mailbox, or only the current operation. This entry point lets them carry
/// that decision through net's generic status and local-hold translation.
/// The compatibility [`into_account_error`] entry point keeps its historical
/// provider inference for downstream callers of that published API.
#[must_use]
pub fn into_account_error_with_throttle_scope(
    error: Error,
    ctx: NetErrorContext,
    throttle_scope: Option<ThrottleScope>,
) -> AccountError {
    match error {
        Error::AttemptHistory {
            source,
            attempts,
            may_have_committed,
        } => {
            let terminal =
                into_account_error_with_throttle_scope(*source, ctx.clone(), throttle_scope);
            let rejected_unauthorized: Vec<_> = attempts
                .iter()
                .map(|attempt| {
                    matches!(attempt, Error::Status { code, .. } if *code == StatusCode::UNAUTHORIZED)
                })
                .collect();
            let previous: Vec<_> = attempts
                .into_iter()
                .map(|error| {
                    into_account_error_with_throttle_scope(error, ctx.clone(), throttle_scope)
                })
                .collect();
            let states: Vec<_> = previous
                .iter()
                .chain(std::iter::once(&terminal))
                .flat_map(|error| error.chain().iter())
                .filter_map(|cause| match cause {
                    Cause::Attempt(a) => Some(a.transmission_state),
                    _ => None,
                })
                .collect();
            let terminal_state = terminal.chain().iter().find_map(|cause| match cause {
                Cause::Attempt(a) => Some(a.transmission_state),
                _ => None,
            });
            let inflight = may_have_committed || states.contains(&TransmissionState::InFlight);
            let acknowledged_may_have_committed = previous.iter().zip(&rejected_unauthorized).any(
                |(error, rejected_unauthorized)| {
                    // A 401 rejects this attempt before authentication. Its
                    // auth cause remains in the chain, but it cannot make a
                    // later local or token-refresh failure ambiguous about
                    // the mutation.
                    !*rejected_unauthorized
                        && error.chain().iter().any(|cause| {
                            matches!(cause, Cause::Attempt(a)
                                if a.transmission_state == TransmissionState::Acknowledged)
                        })
                },
            ) || terminal_state
                == Some(TransmissionState::Acknowledged);
            // A later auth, policy, or admission failure cannot establish the
            // outcome of an earlier mutation. PartialResponse is the existing
            // reconcile-required lane for an incomplete logical exchange.
            let reclassified = inflight
                || (acknowledged_may_have_committed
                    && terminal_state == Some(TransmissionState::Unsent));
            let mut builder = if reclassified {
                partial_response(
                    &ctx,
                    "logical request failed after earlier attempts".into(),
                    if inflight {
                        TransmissionState::InFlight
                    } else {
                        TransmissionState::Acknowledged
                    },
                )
                .into_builder()
            } else {
                let builder = terminal.clone().into_builder();
                if acknowledged_may_have_committed && terminal_state.is_none() {
                    push_attempt(builder, TransmissionState::Acknowledged)
                } else {
                    builder
                }
            };
            for (error, rejected_unauthorized) in previous.iter().zip(&rejected_unauthorized) {
                for cause in error.chain().iter() {
                    // Keep the rejected-auth cause, but do not attach an
                    // acknowledged transmission to a later transport retry:
                    // the builder correctly refuses to classify a transport
                    // failure with acknowledged transmission evidence.
                    if *rejected_unauthorized
                        && matches!(cause, Cause::Attempt(a)
                            if a.transmission_state == TransmissionState::Acknowledged)
                    {
                        continue;
                    }
                    builder = builder.push_cause(cause.clone());
                }
            }
            if reclassified {
                for cause in terminal.chain().iter() {
                    builder = builder.push_cause(cause.clone());
                }
            }
            finish(builder, &ctx)
        }
        Error::LocalHold {
            held,
            attempts,
            may_have_committed,
            ..
        } => {
            let hint = Some(RetryHint::After(held.deadline.remaining()));
            let (kind, cause) = match held.kind {
                crate::HoldKind::RateLimited => (
                    ServerErrorKind::RateLimited,
                    ServerCause::RateLimited { retry_hint: hint },
                ),
                crate::HoldKind::QuotaExhausted => (
                    ServerErrorKind::QuotaExhausted,
                    ServerCause::QuotaExhausted { retry_hint: hint },
                ),
            };
            let diagnostics = format!("prior attempts: {attempts:?}");
            let evidence: Vec<_> = attempts
                .into_iter()
                .map(|attempt| {
                    into_account_error_with_throttle_scope(attempt, ctx.clone(), throttle_scope)
                })
                .collect();
            let states: Vec<_> = evidence
                .iter()
                .flat_map(|error| error.chain().iter())
                .filter_map(|cause| match cause {
                    Cause::Attempt(attempt) => Some(attempt.transmission_state),
                    _ => None,
                })
                .collect();
            let state = if may_have_committed || states.contains(&TransmissionState::InFlight) {
                TransmissionState::InFlight
            } else if states.contains(&TransmissionState::Acknowledged) {
                TransmissionState::Acknowledged
            } else {
                TransmissionState::Unsent
            };
            let mut builder = push_attempt(
                base_builder(&ctx, AccountErrorKind::Server(kind), Cause::Server(cause)),
                state,
            );
            for error in evidence {
                for cause in error.chain().iter() {
                    builder = builder.push_cause(cause.clone());
                }
            }
            builder = builder.text(DiagnosticText::support_only(diagnostics));
            if let Some(scope) = throttle_scope {
                builder = builder.throttle_scope(scope);
            }
            finish(builder, &ctx)
        }
        Error::Network {
            message,
            transmission_state,
            source,
        } => transport_or_partial(
            &ctx,
            TransportKind::Network,
            TransportErrorKind::Network,
            message,
            transmission_state,
            source.as_ref().map(ToString::to_string),
        ),
        Error::Timeout { transmission_state } => transport_or_partial(
            &ctx,
            TransportKind::Timeout,
            TransportErrorKind::Timeout,
            "request timed out".to_owned(),
            transmission_state,
            None,
        ),
        Error::Tls {
            message,
            transmission_state,
        } => transport_or_partial(
            &ctx,
            TransportKind::Tls,
            TransportErrorKind::Tls,
            message,
            transmission_state,
            None,
        ),
        Error::Status {
            code,
            body,
            headers,
        } => status_error(&ctx, code, &headers, &body, None, throttle_scope),
        Error::RetryBudgetExhausted {
            final_response,
            retry_after_history,
        } => match final_response {
            Some(response) => status_error(
                &ctx,
                response.status,
                &response.headers,
                &response.body,
                retry_after_history.last().copied(),
                throttle_scope,
            ),
            // DEFENSIVE ARM, unreachable from the retry loop. `request.rs`
            // mints `RetryBudgetExhausted` only from the status branch, always
            // with `Some(final_response)`; a budget spent on transport failures
            // returns the LAST transport error itself (`Network` / `Timeout` /
            // `Tls`), which carries its own transmission state and so derives
            // `Retry(SameRequest)` for an `Unsent` attempt as it should. The
            // variant carries no per-attempt evidence, so `InFlight` is the
            // conservative stamp for a caller that builds one by hand. A filed
            // finding that this arm sends an all-`Unsent` DELETE or create-PUT
            // to `Reconcile(CheckTarget)` was checked 2026-09-07 and is not a
            // defect: no production path reaches it. Re-raise only if a
            // producer starts minting the variant without a response.
            None => {
                let detail = retry_history_text(&retry_after_history);
                let mut builder = base_builder(
                    &ctx,
                    AccountErrorKind::Transport(TransportErrorKind::Network),
                    Cause::Transport(TransportCause::new(TransportKind::Network, detail.clone())),
                );
                if let Some(text) = detail {
                    builder = builder.text(text);
                }
                finish(push_attempt(builder, TransmissionState::InFlight), &ctx)
            }
        },
        Error::AuthLost {
            transmission_state,
            final_response,
        } => auth_lost(&ctx, transmission_state, final_response.as_ref()),
        Error::RateLimited {
            retry_after,
            final_response,
        } => rate_limited(&ctx, retry_after, &final_response, throttle_scope),
        // Cancellation has no dispatch evidence. Google Drive upload shutdown
        // returns this directly, including after a request may have left.
        // Keep InFlight conservatively: a non-idempotent operation must read
        // back rather than replay unless the caller supplies stronger evidence.
        Error::Cancelled => {
            let builder = base_builder(
                &ctx,
                AccountErrorKind::Transport(TransportErrorKind::Network),
                Cause::Transport(TransportCause::new(
                    TransportKind::Network,
                    Some(DiagnosticText::support_only("request cancelled")),
                )),
            )
            .text(DiagnosticText::support_only("request cancelled"));
            finish(push_attempt(builder, TransmissionState::InFlight), &ctx)
        }
        Error::CostExceedsBurst { cost, burst } => invalid_argument(
            &ctx,
            "request_cost",
            format!("request cost {cost} exceeds bucket burst {burst}"),
        ),
        Error::EncodeBody { message, source } => invalid_argument_with_source(
            &ctx,
            "body",
            message,
            source.as_ref().map(ToString::to_string),
        ),
        Error::InvalidHeader { message, source } => invalid_argument_with_source(
            &ctx,
            "header",
            message,
            source.as_ref().map(ToString::to_string),
        ),
        Error::InvalidRequest { field, detail } => invalid_argument(&ctx, field, detail),
        Error::RefreshFailed {
            retry_after,
            source,
        } => refresh_failed(&ctx, retry_after, source.as_ref()),
        Error::RangeNotHonored { kind, message } => match kind {
            RangeFailureKind::LocalInvalid => invalid_argument(&ctx, "range", message),
            RangeFailureKind::ResponseNotPartial
            | RangeFailureKind::MissingContentRange
            | RangeFailureKind::ContentRangeMismatch => {
                contract_violation(&ctx, message, TransmissionState::Acknowledged)
            }
        },
        Error::RedirectRejected { message } => {
            let builder = invalid_argument_builder(&ctx, "redirect_policy", message);
            finish(push_attempt(builder, TransmissionState::Acknowledged), &ctx)
        }
        Error::MalformedRedirect { message, .. } => {
            contract_violation(&ctx, message, TransmissionState::Acknowledged)
        }
        // Not `auth_lost`. The 401 came back on a hop that carried no
        // credential *because this transport removed it* crossing an
        // origin boundary; the account's credential is untested, so
        // classifying it as `ReauthorizationRequired` (terminal
        // `AuthLost`) would send the user to a re-auth prompt for a
        // working account. The provider redirecting an authenticated
        // request onto an origin it then demands auth for is the
        // contract fault.
        Error::UnauthenticatedRedirectHop {
            message,
            final_response,
        } => {
            let builder = base_builder(
                &ctx,
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                malformed_response_cause(&ctx, message),
            );
            let builder = response_diagnostics(
                builder,
                final_response.status,
                &final_response.headers,
                &final_response.body,
            );
            finish(push_attempt(builder, TransmissionState::Acknowledged), &ctx)
        }
        Error::RedirectLoop { hops } => contract_violation(
            &ctx,
            format!("redirect chain exceeded configured maximum after {hops} hops"),
            TransmissionState::Acknowledged,
        ),
        // The server answered; what it answered with is not something
        // this client agreed to hold in memory. Terminal, not
        // transient - the same response comes back on a retry.
        Error::ResponseTooLarge { limit } => contract_violation(
            &ctx,
            format!("response body exceeded the {limit}-byte buffered ceiling"),
            TransmissionState::Acknowledged,
        ),
    }
}

fn transport_or_partial(
    ctx: &NetErrorContext,
    transport_kind: TransportKind,
    error_kind: TransportErrorKind,
    message: String,
    transmission_state: TransmissionState,
    source_message: Option<String>,
) -> AccountError {
    if transmission_state == TransmissionState::Acknowledged {
        let detail = match source_message {
            Some(source) if !source.trim().is_empty() => {
                format!("{message}; source: {source}")
            }
            _ => message,
        };
        return partial_response(ctx, detail, transmission_state);
    }

    let diagnostic = maybe_support_text(message.clone());
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Transport(error_kind),
        Cause::Transport(TransportCause::new(transport_kind, diagnostic.clone())),
    );
    if let Some(text) = diagnostic {
        builder = builder.text(text);
    }
    if let Some(source) = maybe_support_text_from_option(source_message) {
        builder = builder.text(source);
    }
    finish(push_attempt(builder, transmission_state), ctx)
}

fn auth_lost(
    ctx: &NetErrorContext,
    transmission_state: Option<TransmissionState>,
    final_response: Option<&FinalResponse>,
) -> AccountError {
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
        Cause::Auth(AuthCause::ReauthorizationRequired),
    );
    if let Some(response) = final_response {
        builder = response_diagnostics(builder, response.status, &response.headers, &response.body);
    }
    if let Some(state) = transmission_state {
        builder = push_attempt(builder, state);
    }
    finish(builder, ctx)
}

/// `format!("{source}")` goes through `Display`, so any HTTP body the
/// underlying refresh error carried is lost here. Accepted: the error
/// contract's non-recursion rule on `source()` is binding, and the
/// support-only summary plus `support_cause_from_source` below are the
/// agreed fidelity - do not re-raise.
fn refresh_failed(
    ctx: &NetErrorContext,
    retry_after: Option<SystemTime>,
    source: &Error,
) -> AccountError {
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient),
        Cause::Auth(AuthCause::RefreshTransient),
    )
    .text(DiagnosticText::support_only(format!(
        "OAuth refresh failed: {source}"
    )));

    if let Some(cause) = support_cause_from_source(source) {
        builder = builder.push_cause(cause);
    }
    // Keep the issuer's typed hint when present. The outer quiet deadline
    // also matters for transport failures whose cause has no server hint.
    let has_hint = matches!(
        support_cause_from_source(source),
        Some(Cause::Server(
            ServerCause::Unavailable {
                retry_hint: Some(_)
            } | ServerCause::RateLimited {
                retry_hint: Some(_)
            } | ServerCause::QuotaExhausted {
                retry_hint: Some(_)
            }
        ))
    );
    if !has_hint && let Some(at) = retry_after {
        builder = builder.push_cause(Cause::Server(ServerCause::Unavailable {
            retry_hint: Some(RetryHint::At(at)),
        }));
    }
    finish(builder, ctx)
}

fn rate_limited(
    ctx: &NetErrorContext,
    retry_after: Option<Duration>,
    final_response: &FinalResponse,
    throttle_scope: Option<ThrottleScope>,
) -> AccountError {
    let retry_hint = parse_retry_after_hint(final_response.headers.get(RETRY_AFTER))
        .or_else(|| retry_after.map(RetryHint::After));
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited { retry_hint }),
    );
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(
        builder,
        final_response.status,
        &final_response.headers,
        &final_response.body,
    );
    if let Some(scope) = throttle_scope {
        builder = builder.throttle_scope(scope);
    }
    finish(builder, ctx)
}

fn status_error(
    ctx: &NetErrorContext,
    status: StatusCode,
    headers: &HeaderMap,
    body: &Bytes,
    retry_hint_param: Option<Duration>,
    throttle_scope: Option<ThrottleScope>,
) -> AccountError {
    let code = status.as_u16();
    let retry_hint = parse_retry_after_hint(headers.get(RETRY_AFTER))
        .or_else(|| retry_hint_param.map(RetryHint::After));
    let (kind, cause, throttle) = status_kind_cause(ctx, code, body, retry_hint);
    let mut builder = base_builder(ctx, kind, cause);
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(builder, status, headers, body);
    if throttle && let Some(scope) = throttle_scope {
        builder = builder.throttle_scope(scope);
    }
    finish(builder, ctx)
}

fn status_kind_cause(
    ctx: &NetErrorContext,
    code: u16,
    body: &Bytes,
    retry_hint: Option<RetryHint>,
) -> (AccountErrorKind, Cause, bool) {
    match code {
        400 | 422 => {
            let detail = status_detail(code, body);
            (
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed { detail }),
                false,
            )
        }
        401 => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
            false,
        ),
        403 => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            false,
        ),
        404 => match resource_from_scope(ctx.scope.as_ref()) {
            Some(resource) => (
                AccountErrorKind::NotFound(resource),
                Cause::Request(RequestCause::NotFound {
                    what: resource,
                    id: id_from_scope(ctx.scope.as_ref()),
                }),
                false,
            ),
            None => server_error(code),
        },
        409 => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            false,
        ),
        410 if matches!(ctx.scope.as_ref(), Some(ErrorScope::Cursor(_))) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            false,
        ),
        410 => server_error(code),
        408 | 502 | 503 | 504 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint }),
            false,
        ),
        429 => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
            true,
        ),
        507 => (
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_hint }),
            true,
        ),
        500..=599 => server_error(code),
        _ => server_error(code),
    }
}

fn server_error(code: u16) -> (AccountErrorKind, Cause, bool) {
    (
        AccountErrorKind::Server(ServerErrorKind::Error { status: Some(code) }),
        Cause::Server(ServerCause::Error { status: Some(code) }),
        false,
    )
}

fn invalid_argument(ctx: &NetErrorContext, field: &'static str, detail: String) -> AccountError {
    let builder = invalid_argument_builder(ctx, field, detail);
    finish(builder, ctx)
}

fn invalid_argument_with_source(
    ctx: &NetErrorContext,
    field: &'static str,
    detail: String,
    source: Option<String>,
) -> AccountError {
    let mut builder = invalid_argument_builder(ctx, field, detail);
    if let Some(text) = maybe_support_text_from_option(source) {
        builder = builder.text(text);
    }
    finish(builder, ctx)
}

fn invalid_argument_builder(
    ctx: &NetErrorContext,
    field: &'static str,
    detail: String,
) -> AccountErrorBuilder {
    let message = maybe_support_text(detail);
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some(field),
            message: message.clone(),
        }),
    );
    if let Some(text) = message {
        builder = builder.text(text);
    }
    builder
}

fn contract_violation(
    ctx: &NetErrorContext,
    detail: String,
    transmission_state: TransmissionState,
) -> AccountError {
    let builder = base_builder(
        ctx,
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        malformed_response_cause(ctx, detail),
    );
    finish(push_attempt(builder, transmission_state), ctx)
}

fn partial_response(
    ctx: &NetErrorContext,
    detail: String,
    transmission_state: TransmissionState,
) -> AccountError {
    let builder = base_builder(
        ctx,
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        malformed_response_cause(ctx, detail),
    );
    finish(push_attempt(builder, transmission_state), ctx)
}

fn malformed_response_cause(ctx: &NetErrorContext, detail: String) -> Cause {
    Cause::Wire(WireCause::MalformedResponse {
        protocol: ctx.protocol,
        detail: maybe_support_text(detail),
    })
}

fn base_builder(
    ctx: &NetErrorContext,
    kind: AccountErrorKind,
    cause: Cause,
) -> AccountErrorBuilder {
    let builder = AccountErrorBuilder::new(kind, cause)
        .protocol(ctx.protocol)
        .operation(ctx.operation);
    match ctx.provider {
        Some(provider) => builder.provider(provider),
        None => builder,
    }
}

fn finish(builder: AccountErrorBuilder, ctx: &NetErrorContext) -> AccountError {
    let builder = match &ctx.scope {
        Some(scope) => builder.scope(scope.clone()),
        None => builder,
    };
    builder
        .try_build()
        .expect("valid account error classification")
}

fn push_attempt(
    builder: AccountErrorBuilder,
    transmission_state: TransmissionState,
) -> AccountErrorBuilder {
    builder.push_cause(Cause::Attempt(AttemptCause::new(transmission_state)))
}

fn response_diagnostics(
    mut builder: AccountErrorBuilder,
    status: StatusCode,
    headers: &HeaderMap,
    body: &Bytes,
) -> AccountErrorBuilder {
    builder = builder.status(Some(status.as_u16()));
    if let Some(id) = first_header(headers, REQUEST_ID_HEADERS) {
        builder = builder.request_id(id);
    }
    if let Some(trace_id) = trace_id_from_headers(headers) {
        builder = builder.trace_id(trace_id);
    }
    for name in SUPPORT_ONLY_HEADER_TEXT {
        if let Some(value) = header_text(headers, name) {
            builder = builder.text(DiagnosticText::support_only(format!("{name}: {value}")));
        }
    }
    if let Some(text) = body_diagnostic(body) {
        builder = builder.text(text);
    }
    builder
}

const REQUEST_ID_HEADERS: &[&str] = &[
    "x-request-id",
    "request-id",
    "x-ms-request-id",
    "x-goog-request-id",
    "x-guploader-uploadid",
];

const SUPPORT_ONLY_HEADER_TEXT: &[&str] = &[
    "traceparent",
    "x-cloud-trace-context",
    "x-ms-ags-diagnostic",
];

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
    if let Some(value) = header_text(headers, "traceparent")
        && let Some(trace_id) = traceparent_trace_id(&value)
    {
        return Some(trace_id);
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

fn traceparent_trace_id(value: &str) -> Option<String> {
    let mut parts = value.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let parent_id = parts.next()?;
    let flags = parts.next()?;
    if parts.next().is_some()
        || version.len() != 2
        || trace_id.len() != 32
        || parent_id.len() != 16
        || flags.len() != 2
    {
        return None;
    }
    if !trace_id.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    Some(trace_id.to_owned())
}

fn body_diagnostic(body: &Bytes) -> Option<DiagnosticText> {
    if body.is_empty() {
        return None;
    }
    Some(DiagnosticText::support_only(format!(
        "response body: {}",
        String::from_utf8_lossy(body)
    )))
}

/// The detail is deliberately synthetic: the response body is not
/// summarized into `RequestCause::Malformed.detail`. The body reaches
/// support-text via `response_diagnostics` at the call site, and that
/// is the accepted home for it - do not re-raise.
fn status_detail(code: u16, _body: &Bytes) -> DiagnosticText {
    DiagnosticText::support_only(format!("HTTP {code} malformed request"))
}

fn maybe_support_text(value: String) -> Option<DiagnosticText> {
    if value.trim().is_empty() {
        None
    } else {
        Some(DiagnosticText::support_only(value))
    }
}

fn maybe_support_text_from_option(value: Option<String>) -> Option<DiagnosticText> {
    value.and_then(maybe_support_text)
}

fn retry_history_text(history: &[Duration]) -> Option<DiagnosticText> {
    if history.is_empty() {
        None
    } else {
        Some(DiagnosticText::support_only(format!(
            "retry budget exhausted after retry-after history: {history:?}"
        )))
    }
}

fn resource_from_scope(scope: Option<&ErrorScope>) -> Option<ResourceKind> {
    match scope {
        Some(ErrorScope::Message { .. }) => Some(ResourceKind::Message),
        Some(ErrorScope::Mailbox { .. }) => Some(ResourceKind::Mailbox),
        Some(ErrorScope::Thread { .. }) => Some(ResourceKind::Thread),
        Some(ErrorScope::Event { .. }) => Some(ResourceKind::Event),
        Some(ErrorScope::Calendar { .. }) => Some(ResourceKind::Calendar),
        Some(ErrorScope::Contact { .. }) => Some(ResourceKind::Contact),
        Some(_) | None => None,
    }
}

fn id_from_scope(scope: Option<&ErrorScope>) -> Option<String> {
    match scope {
        Some(ErrorScope::Message { id }) => Some(id.0.clone()),
        Some(ErrorScope::Mailbox { id }) => Some(id.0.clone()),
        Some(ErrorScope::Thread { id }) => Some(id.0.clone()),
        Some(ErrorScope::Event { id }) => Some(id.0.clone()),
        Some(ErrorScope::Calendar { id }) => Some(id.0.clone()),
        Some(ErrorScope::Contact { id }) => Some(id.0.clone()),
        Some(_) | None => None,
    }
}

/// Preserve the old context-derived behavior for callers of
/// `into_account_error`. Generic translation has no account-specific identity;
/// provider funnels should use `into_account_error_with_throttle_scope` and
/// supply their own documented policy. Workspace migration does not prove
/// downstream callers no longer depend on this published entry point's behavior;
/// removing the inference needs an explicit compatibility decision.
fn legacy_throttle_scope(ctx: &NetErrorContext) -> Option<ThrottleScope> {
    if ctx.protocol == Protocol::Graph
        || ctx.provider == Some(Provider::Microsoft)
        || ctx.protocol == Protocol::Gmail
        || ctx.provider == Some(Provider::Gmail)
        || (ctx.protocol == Protocol::Jmap && ctx.provider == Some(Provider::Fastmail))
    {
        Some(ThrottleScope::Account)
    } else {
        None
    }
}

fn support_cause_from_source(error: &Error) -> Option<Cause> {
    match error {
        Error::AttemptHistory { source, .. } => support_cause_from_source(source),
        Error::Network { message, .. } => Some(Cause::Transport(TransportCause::new(
            TransportKind::Network,
            maybe_support_text(message.clone()),
        ))),
        Error::Timeout { .. } => Some(Cause::Transport(TransportCause::new(
            TransportKind::Timeout,
            Some(DiagnosticText::support_only("request timed out")),
        ))),
        Error::Tls { message, .. } => Some(Cause::Transport(TransportCause::new(
            TransportKind::Tls,
            maybe_support_text(message.clone()),
        ))),
        Error::AuthLost { .. } => Some(Cause::Auth(AuthCause::ReauthorizationRequired)),
        Error::RefreshFailed { .. } => Some(Cause::Auth(AuthCause::RefreshTransient)),
        Error::RateLimited {
            retry_after,
            final_response,
        } => Some(Cause::Server(ServerCause::RateLimited {
            retry_hint: parse_retry_after_hint(final_response.headers.get(RETRY_AFTER))
                .or_else(|| retry_after.map(RetryHint::After)),
        })),
        Error::RetryBudgetExhausted {
            final_response: Some(response),
            retry_after_history,
        } => server_cause_from_status(
            response.status.as_u16(),
            response.headers.get(RETRY_AFTER),
            retry_after_history.last().copied(),
        ),
        Error::RetryBudgetExhausted {
            final_response: None,
            ..
        } => Some(Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only("retry budget exhausted")),
        ))),
        Error::Status { code, headers, .. } => {
            server_cause_from_status(code.as_u16(), headers.get(RETRY_AFTER), None)
        }
        Error::Cancelled => Some(Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only("request cancelled")),
        ))),
        Error::LocalHold { .. }
        | Error::CostExceedsBurst { .. }
        | Error::EncodeBody { .. }
        | Error::InvalidHeader { .. }
        | Error::InvalidRequest { .. }
        | Error::RangeNotHonored { .. }
        | Error::RedirectRejected { .. }
        | Error::MalformedRedirect { .. }
        | Error::RedirectLoop { .. }
        | Error::UnauthenticatedRedirectHop { .. }
        | Error::ResponseTooLarge { .. } => None,
    }
}

fn server_cause_from_status(
    code: u16,
    retry_after_header: Option<&reqwest::header::HeaderValue>,
    retry_hint_param: Option<Duration>,
) -> Option<Cause> {
    let retry_hint = parse_retry_after_hint(retry_after_header)
        .or_else(|| retry_hint_param.map(RetryHint::After));
    match code {
        408 | 502 | 503 | 504 => Some(Cause::Server(ServerCause::Unavailable { retry_hint })),
        429 => Some(Cause::Server(ServerCause::RateLimited { retry_hint })),
        507 => Some(Cause::Server(ServerCause::QuotaExhausted { retry_hint })),
        500..=599 => Some(Cause::Server(ServerCause::Error { status: Some(code) })),
        _ => None,
    }
}

#[cfg(test)]
mod retry_hint_tests {
    use super::*;
    use bifrost_types::RecoveryClass;

    #[test]
    fn final_http_date_survives_status_and_exhausted_errors() {
        let date = "Sun, 06 Nov 1994 08:49:37 GMT";
        let at = httpdate::parse_http_date(date).expect("fixed HTTP-date");
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, reqwest::header::HeaderValue::from_static(date));
        let response = FinalResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: headers.clone(),
            body: Bytes::new(),
        };
        let errors = [
            Error::Status {
                code: StatusCode::SERVICE_UNAVAILABLE,
                headers,
                body: Bytes::new(),
            },
            Error::RateLimited {
                retry_after: Some(Duration::from_secs(1)),
                final_response: response.clone(),
            },
            Error::RetryBudgetExhausted {
                retry_after_history: vec![Duration::from_secs(1)],
                final_response: Some(response),
            },
        ];
        for error in errors {
            let error = into_account_error(
                error,
                NetErrorContext {
                    provider: None,
                    protocol: Protocol::Jmap,
                    operation: AccountOperation::SyncChanges,
                    scope: None,
                },
            );
            let RecoveryClass::Retry(advice) = error.recovery() else {
                panic!("expected retry: {:?}", error.recovery());
            };
            assert_eq!(advice.retry_hint, Some(RetryHint::At(at)));
        }
    }
}

#[cfg(test)]
mod refresh_deadline_tests {
    use super::*;

    #[test]
    fn refresh_transport_failure_preserves_outer_quiet_deadline() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1234);
        let error = into_account_error(
            Error::RefreshFailed {
                retry_after: Some(at),
                source: std::sync::Arc::new(Error::Timeout {
                    transmission_state: TransmissionState::Unsent,
                }),
            },
            NetErrorContext {
                provider: None,
                protocol: Protocol::Jmap,
                operation: AccountOperation::SyncChanges,
                scope: None,
            },
        );
        let bifrost_types::RecoveryClass::Retry(advice) = error.recovery() else {
            panic!("refresh failure must remain retryable");
        };
        assert_eq!(advice.retry_hint, Some(RetryHint::At(at)));
    }
}

#[cfg(test)]
mod explicit_throttle_scope_tests {
    use super::*;

    #[test]
    fn caller_policy_overrides_legacy_provider_inference() {
        let ctx = NetErrorContext {
            provider: Some(Provider::Microsoft),
            protocol: Protocol::Graph,
            operation: AccountOperation::SyncChanges,
            scope: None,
        };
        let error = Error::Status {
            code: StatusCode::TOO_MANY_REQUESTS,
            body: Bytes::new(),
            headers: HeaderMap::new(),
        };

        let unscoped = into_account_error_with_throttle_scope(error, ctx.clone(), None);
        assert_eq!(
            unscoped.telemetry_fields().throttle_scope,
            None,
            "an explicit lack of caller policy must not fall back to provider inference"
        );

        let scoped = into_account_error_with_throttle_scope(
            Error::Status {
                code: StatusCode::TOO_MANY_REQUESTS,
                body: Bytes::new(),
                headers: HeaderMap::new(),
            },
            ctx,
            Some(ThrottleScope::Mailbox),
        );
        assert_eq!(
            scoped.telemetry_fields().throttle_scope,
            Some(ThrottleScope::Mailbox)
        );
    }
}
