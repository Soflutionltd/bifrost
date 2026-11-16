//! Translation of `bifrost-net` transport outcomes into the shared
//! `AccountError` contract.
//!
//! Audit boundary: the 2026-07 google+net bug sweep exercised this module
//! only through its integration suite, not by a line audit; the same is
//! true of `trace.rs` beyond its construction-level invariants. Listed so a
//! future auditor knows where that sweep's coverage stopped.

#[cfg(test)]
use bifrost_types::ThrottleScope;
#[cfg(test)]
use std::time::{Duration, SystemTime};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountOperation, AttemptCause, AuthCause, Cause,
    DiagnosticText, ErrorScope, PrimaryCause, Protocol, ProtocolErrorKind, Provider, RequestCause,
    RetryHint, ServerCause, TransmissionState, TransportCause, TransportErrorKind, WireCause,
};
use bytes::Bytes;
use reqwest::{
    StatusCode,
    header::{HeaderMap, RETRY_AFTER},
};

use crate::error::{Error, FinalResponse, RangeFailureKind};
use crate::request::parse_retry_after_hint;
use crate::{HttpErrorPolicy, StatusPolicy, classify_status};

#[derive(Clone, Debug)]
pub struct NetErrorContext {
    pub provider: Option<Provider>,
    pub protocol: Protocol,
    pub operation: AccountOperation,
    pub scope: Option<ErrorScope>,
}

impl NetErrorContext {
    /// The failing leg's identity, shared with provider-owned translations.
    #[must_use]
    pub fn identity(&self) -> bifrost_types::AccountErrorContext {
        bifrost_types::AccountErrorContext::new(self.provider, self.protocol)
    }
}

/// Classify a local token-source failure (`TokenSource::current` or
/// `snapshot`) for a protocol that authenticates with the token.
///
/// The one owner for every protocol crate's token lookup: IMAP, ManageSieve
/// and SMTP all route here, so a token endpoint outage keeps net's typed
/// recovery (an enterprise user expects a retry, not a demand for a new
/// password) while real credential loss stays an auth failure. The lookup has
/// no provider, mailbox or transaction scope to name, so none is stamped; the
/// caller's own error context adds its scope afterwards.
#[must_use]
pub fn token_source_account_error(
    error: Error,
    protocol: Protocol,
    operation: AccountOperation,
) -> AccountError {
    into_account_error_with_throttle_scope(
        error,
        NetErrorContext {
            provider: None,
            protocol,
            operation,
            scope: None,
        },
        None,
    )
}

/// Translate a transport error using the caller's provider throttle policy.
///
/// Provider crates know whether a server's throttle should pause an account,
/// mailbox, or only the current operation. This entry point lets them carry
/// that decision through net's generic status and local-hold translation.
/// Pass `HttpErrorPolicy` for provider status overrides and a throttle budget;
/// an `Option<ThrottleScope>` selects the common HTTP table without a budget.
#[must_use]
pub fn into_account_error_with_throttle_scope(
    error: Error,
    ctx: NetErrorContext,
    policy: impl Into<HttpErrorPolicy>,
) -> AccountError {
    translate(error, ctx, &policy.into())
}

fn translate(error: Error, ctx: NetErrorContext, policy: &HttpErrorPolicy) -> AccountError {
    match error {
        Error::HopPolicy(error) => *error,
        Error::AttemptHistory { source, evidence } => {
            let commit = evidence.verdict();
            let (attempts, mut ledger) = evidence.into_parts();
            ledger.record_error(&source, true);
            let lone_transport = attempts.is_empty()
                && matches!(
                    source.as_ref(),
                    Error::Network { .. } | Error::Timeout { .. } | Error::Tls { .. }
                );
            let typed_hop_refusal = matches!(source.as_ref(), Error::HopPolicy(_));
            let terminal = translate(*source, ctx.clone(), policy);
            let previous: Vec<_> = attempts
                .into_iter()
                .map(|error| translate(error, ctx.clone(), policy))
                .collect();
            let reclassified =
                !lone_transport && ledger.verdict() == crate::CommitEvidence::MayHaveCommitted;
            let mut builder = if reclassified {
                partial_response(
                    &ctx,
                    "logical request failed after earlier attempts".into(),
                    ledger.transmission_state(),
                )
                .into_builder()
            } else {
                terminal.clone().into_builder()
            };
            for error in &previous {
                for cause in error.chain().iter() {
                    // Keep the rejected-auth cause, but do not attach an
                    // acknowledged transmission to a later transport retry:
                    // the builder correctly refuses to classify a transport
                    // failure with acknowledged transmission evidence.
                    // A DAV target refusal is local, but earlier answered
                    // hops remain evidence. Transport failures keep their
                    // terminal attempt state for recovery classification.
                    if !reclassified && !typed_hop_refusal && matches!(cause, Cause::Attempt(_)) {
                        continue;
                    }
                    builder = builder.push_cause(cause.clone());
                }
            }
            if reclassified {
                // Reclassification changes recovery, not the terminal response's
                // diagnostic evidence. Preserve its consent tiers and tokens.
                let diagnostics = terminal.support_consented();
                builder = builder.status(diagnostics.telemetry.status);
                if let Some(id) = diagnostics.telemetry.request_id {
                    builder = builder.request_id(id);
                }
                if let Some(id) = diagnostics.telemetry.trace_id {
                    builder = builder.trace_id(id);
                }
                if let Some(code) = diagnostics.telemetry.native_code {
                    builder = builder.native_code(code);
                }
                for text in diagnostics.user_safe_text {
                    builder = builder.text(DiagnosticText::user_safe(text));
                }
                for text in diagnostics.support_text {
                    builder = builder.text(DiagnosticText::support_only(text));
                }
                for cause in terminal.chain().iter() {
                    builder = builder.push_cause(cause.clone());
                }
            }
            // A request that was not replayable may have reached the server.
            // A blind same-request retry of the logical operation would replay
            // it, so derive must see it as non-idempotent whatever the
            // operation table says. The override only ever narrows.
            if commit == crate::CommitEvidence::MayHaveCommitted {
                builder = builder.idempotency_override(false);
            }
            finish(builder, &ctx)
        }
        Error::LocalHold { held, evidence, .. } => {
            let hint = RetryHint::After(held.deadline.remaining());
            let cause = held.kind.server_cause(hint);
            let commit = evidence.verdict();
            let (attempts, ledger) = evidence.into_parts();
            let diagnostics = format!("prior attempts: {attempts:?}");
            let evidence: Vec<_> = attempts
                .into_iter()
                .map(|attempt| translate(attempt, ctx.clone(), policy))
                .collect();
            let state = ledger.transmission_state();
            let mut builder = push_attempt(
                base_builder(&ctx, policy.apply_throttle(PrimaryCause::Server(cause))),
                state,
            );
            for error in evidence {
                for cause in error.chain().iter() {
                    builder = builder.push_cause(cause.clone());
                }
            }
            if commit == crate::CommitEvidence::MayHaveCommitted {
                builder = builder.idempotency_override(false);
            }
            builder = builder.text(DiagnosticText::support_only(diagnostics));
            builder = builder.with_throttle(|throttle| throttle.local_hold = true);
            finish(builder, &ctx)
        }
        Error::Network {
            message,
            transmission_state,
            source: _,
        } => transport_or_partial(
            &ctx,
            TransportErrorKind::Network,
            message,
            transmission_state,
        ),
        Error::Timeout { transmission_state } => transport_or_partial(
            &ctx,
            TransportErrorKind::Timeout,
            "request timed out".to_owned(),
            transmission_state,
        ),
        Error::Tls {
            message,
            transmission_state,
        } => transport_or_partial(&ctx, TransportErrorKind::Tls, message, transmission_state),
        Error::Status {
            code,
            body,
            headers,
        } => status_error(
            &ctx,
            code,
            &headers,
            &body,
            parse_retry_after_hint(headers.get(RETRY_AFTER)),
            policy,
        ),
        Error::RetryBudgetExhausted {
            final_response,
            retry_after_history,
            ..
        } => match final_response {
            Some(response) => status_error(
                &ctx,
                response.status,
                &response.headers,
                &response.body,
                parse_retry_after_hint(response.headers.get(RETRY_AFTER)),
                policy,
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
                    PrimaryCause::Transport(TransportCause::new(
                        TransportErrorKind::Network,
                        detail.clone(),
                    )),
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
        } => rate_limited(&ctx, retry_after, &final_response, policy),
        // Cancellation has no dispatch evidence. Google Drive upload shutdown
        // returns this directly, including after a request may have left.
        // Keep InFlight conservatively: a non-idempotent operation must read
        // back rather than replay unless the caller supplies stronger evidence.
        Error::Cancelled => {
            let builder = base_builder(
                &ctx,
                PrimaryCause::Transport(TransportCause::new(
                    TransportErrorKind::Network,
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
        Error::EncodeBody { .. } => {
            invalid_argument(&ctx, "body", "request body encoding failed".to_owned())
        }
        Error::InvalidHeader { .. } => {
            invalid_argument(&ctx, "header", "invalid request header".to_owned())
        }
        Error::InvalidRequest { field, .. } => {
            invalid_argument(&ctx, field, "invalid request".to_owned())
        }
        Error::RefreshFailed {
            retry_after,
            source,
        } => refresh_failed(&ctx, retry_after, source.as_ref()),
        Error::RangeNotHonored { kind, .. } => {
            let message = format!("range not honored: {kind:?}");
            match kind {
                RangeFailureKind::LocalInvalid => invalid_argument(&ctx, "range", message),
                RangeFailureKind::ResponseNotPartial
                | RangeFailureKind::MissingContentRange
                | RangeFailureKind::ContentRangeMismatch => {
                    contract_violation(&ctx, message, TransmissionState::Acknowledged)
                }
            }
        }
        Error::UnexpectedResponse { response } => {
            let builder = base_builder(
                &ctx,
                PrimaryCause::Protocol {
                    kind: ProtocolErrorKind::ContractViolation,
                    wire: malformed_response_cause(
                        &ctx,
                        format!("unexpected HTTP response {}", response.status),
                    ),
                },
            );
            let builder =
                response_diagnostics(builder, response.status, &response.headers, &response.body);
            finish(push_attempt(builder, TransmissionState::Acknowledged), &ctx)
        }
        Error::RedirectRejected { .. } => {
            let message = "redirect rejected".to_owned();
            let builder = invalid_argument_builder(&ctx, "redirect_policy", message);
            finish(push_attempt(builder, TransmissionState::Acknowledged), &ctx)
        }
        Error::MalformedRedirect { kind, .. } => {
            let message = format!("malformed redirect: {kind:?}");
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
            message: _,
            final_response,
        } => {
            let builder = base_builder(
                &ctx,
                PrimaryCause::Protocol {
                    kind: ProtocolErrorKind::ContractViolation,
                    wire: malformed_response_cause(&ctx, "unauthenticated redirect hop".to_owned()),
                },
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
    transport_kind: TransportErrorKind,
    message: String,
    transmission_state: TransmissionState,
) -> AccountError {
    if transmission_state == TransmissionState::Acknowledged {
        return partial_response(ctx, message, transmission_state);
    }

    let diagnostic = maybe_support_text(message.clone());
    let mut builder = base_builder(
        ctx,
        PrimaryCause::Transport(TransportCause::new(transport_kind, diagnostic.clone())),
    );
    if let Some(text) = diagnostic {
        builder = builder.text(text);
    }
    finish(push_attempt(builder, transmission_state), ctx)
}

fn auth_lost(
    ctx: &NetErrorContext,
    transmission_state: Option<TransmissionState>,
    final_response: Option<&FinalResponse>,
) -> AccountError {
    let mut builder = base_builder(ctx, PrimaryCause::Auth(AuthCause::ReauthorizationRequired));
    if let Some(response) = final_response {
        builder = response_diagnostics(builder, response.status, &response.headers, &response.body);
    }
    if let Some(state) = transmission_state {
        builder = push_attempt(builder, state);
    }
    finish(builder, ctx)
}

// Foreign token-source text may quote a rejected credential. Keep only the
// fixed summary, typed supporting cause and retry hint.
fn refresh_failed(
    ctx: &NetErrorContext,
    retry_after: Option<RetryHint>,
    source: &Error,
) -> AccountError {
    let mut builder = base_builder(ctx, PrimaryCause::Auth(AuthCause::RefreshTransient))
        .text(DiagnosticText::support_only("OAuth refresh failed"));

    let cause = match source.terminal() {
        Error::Status { code, .. } => server_cause_from_status(code.as_u16(), retry_after),
        _ => support_cause_from_source(source),
    };
    // Keep the issuer's typed hint when present. The outer quiet hint
    // also matters for transport failures whose cause has no server hint.
    let has_hint = matches!(
        cause.as_ref(),
        Some(Cause::Server(
            ServerCause::Unavailable {
                retry_hint: Some(_)
            } | ServerCause::RateLimited {
                retry_hint: Some(_),
                ..
            } | ServerCause::QuotaExhausted {
                retry_hint: Some(_),
                ..
            }
        ))
    );
    if let Some(cause) = cause {
        builder = builder.push_cause(cause);
    }
    if !has_hint && let Some(at) = retry_after {
        builder = builder.push_cause(Cause::Server(ServerCause::Unavailable {
            retry_hint: Some(at),
        }));
    }
    finish(builder, ctx)
}

fn rate_limited(
    ctx: &NetErrorContext,
    retry_after: Option<RetryHint>,
    final_response: &FinalResponse,
    policy: &HttpErrorPolicy,
) -> AccountError {
    let retry_hint = retry_after;
    let mut builder = base_builder(
        ctx,
        policy.apply_throttle(PrimaryCause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint,
        })),
    );
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(
        builder,
        final_response.status,
        &final_response.headers,
        &final_response.body,
    );
    finish(builder, ctx)
}

fn status_error(
    ctx: &NetErrorContext,
    status: StatusCode,
    headers: &HeaderMap,
    body: &Bytes,
    retry_hint_param: Option<RetryHint>,
    policy: &HttpErrorPolicy,
) -> AccountError {
    let cause = classify_status(
        status.as_u16(),
        ctx.scope.as_ref(),
        retry_hint_param,
        policy.status,
    );
    let mut builder = base_builder(ctx, policy.apply_throttle(cause));
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(builder, status, headers, body);
    finish(builder, ctx)
}

fn invalid_argument(ctx: &NetErrorContext, field: &'static str, detail: String) -> AccountError {
    let builder = invalid_argument_builder(ctx, field, detail);
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
        PrimaryCause::Request(RequestCause::InvalidArgument {
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
        PrimaryCause::Protocol {
            kind: ProtocolErrorKind::ContractViolation,
            wire: malformed_response_cause(ctx, detail),
        },
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
        PrimaryCause::Protocol {
            kind: ProtocolErrorKind::PartialResponse,
            wire: malformed_response_cause(ctx, detail),
        },
    );
    finish(push_attempt(builder, transmission_state), ctx)
}

fn malformed_response_cause(ctx: &NetErrorContext, detail: String) -> WireCause {
    WireCause::MalformedResponse {
        protocol: ctx.protocol,
        detail: maybe_support_text(detail),
    }
}

fn base_builder(ctx: &NetErrorContext, cause: PrimaryCause) -> AccountErrorBuilder {
    AccountErrorBuilder::new(cause)
        .context(ctx.identity())
        .operation(ctx.operation)
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
    if let Some(id) = request_id_from_headers(headers) {
        builder = builder.request_id(id);
    }
    if let Some(id) = trace_id_from_headers(headers) {
        builder = builder.trace_id(id);
    }
    builder = push_header_names(builder, headers);
    builder.text(body_size_text(body.len()))
}

/// Append one support-only line per response header name.
///
/// Support consent does not authorize disclosure of credentials echoed by a
/// server, so a mapper preserves bounded structural evidence, never arbitrary
/// wire text: header names, never header values.
pub fn push_header_names(
    mut builder: AccountErrorBuilder,
    headers: &HeaderMap,
) -> AccountErrorBuilder {
    for name in headers.keys() {
        builder = builder.text(DiagnosticText::support_only(format!(
            "response header: {name}"
        )));
    }
    builder
}

/// The support-only size of a captured response body, exported in place of
/// the body itself, which may echo credentials.
pub fn body_size_text(len: usize) -> DiagnosticText {
    DiagnosticText::support_only(format!("captured response body bytes: {len}"))
}

fn maybe_support_text(value: String) -> Option<DiagnosticText> {
    if value.trim().is_empty() {
        None
    } else {
        Some(DiagnosticText::support_only(value))
    }
}

/// Response headers that carry a provider request id, in priority order.
///
/// The one list every HTTP provider mapper reads; a provider with an extra
/// request-id header adds it here rather than keeping a local list.
pub const REQUEST_ID_HEADERS: &[&str] = &[
    "x-request-id",
    "request-id",
    "x-ms-request-id",
    "x-goog-request-id",
    "x-google-request-id",
    "x-guploader-uploadid",
];

/// The first request-id header in [`REQUEST_ID_HEADERS`] whose value passes
/// [`correlation_id`]. A header that fails the check is skipped, not trimmed.
pub fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    REQUEST_ID_HEADERS
        .iter()
        .find_map(|name| correlation_header(headers, name))
}

/// A trace id from a W3C `traceparent` header (see [`traceparent_trace_id`]),
/// falling back to the id part of `x-cloud-trace-context`. The whole
/// `x-cloud-trace-context` value must pass [`correlation_id`] before any part
/// of it is extracted.
pub fn trace_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .and_then(traceparent_trace_id)
        .or_else(|| {
            correlation_header(headers, "x-cloud-trace-context").and_then(|value| {
                let id = value.split('/').next()?;
                (!id.is_empty()).then(|| id.to_owned())
            })
        })
}

/// The value of header `name`, if it passes [`correlation_id`].
pub fn correlation_header(headers: &HeaderMap, name: &str) -> Option<String> {
    correlation_id(headers.get(name)?.to_str().ok()?)
}

/// The workspace's single shape check for a provider correlation id, from a
/// header or from a response body field.
///
/// Enterprise support needs provider request and trace ids, but a server can
/// echo credentials into any field, and echoed credentials must never become
/// telemetry. The entire value is validated, never trimmed or partly
/// extracted: non-empty, at most 256 bytes, ASCII alphanumerics and `-_./;=`
/// only, and no credential marker such as `bearer` or `token` anywhere in it.
/// Every provider mapper exports an id only through this function.
pub fn correlation_id(value: &str) -> Option<String> {
    let lower = value.to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_./;=".contains(&byte))
        || [
            "authorization",
            "bearer",
            "basic",
            "password",
            "secret",
            "token",
            "cookie",
            "credential",
            "api-key",
            "api_key",
            "apikey",
        ]
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return None;
    }
    Some(value.to_owned())
}

/// The trace-id field of a W3C `traceparent` value, or `None` unless the
/// whole value is well formed: version, trace id, parent id and flags as
/// lowercase hex of lengths 2, 32, 16 and 2, version not `ff`, and neither id
/// all zeros.
pub fn traceparent_trace_id(value: &str) -> Option<String> {
    if value.len() != 55 {
        return None;
    }
    let parts: Vec<_> = value.split('-').collect();
    if parts.len() != 4
        || parts.iter().zip([2, 32, 16, 2]).any(|(part, len)| {
            part.len() != len
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        || parts[0] == "ff"
        || parts[1].bytes().all(|byte| byte == b'0')
        || parts[2].bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some(parts[1].to_owned())
}

fn retry_history_text(history: &[RetryHint]) -> Option<DiagnosticText> {
    if history.is_empty() {
        None
    } else {
        Some(DiagnosticText::support_only(format!(
            "retry budget exhausted after retry-after history: {history:?}"
        )))
    }
}

fn support_cause_from_source(error: &Error) -> Option<Cause> {
    match error {
        Error::AttemptHistory { source, .. } => support_cause_from_source(source),
        Error::Network { .. } => Some(Cause::Transport(TransportCause::new(
            TransportErrorKind::Network,
            Some(DiagnosticText::support_only("token source network failure")),
        ))),
        Error::Timeout { .. } => Some(Cause::Transport(TransportCause::new(
            TransportErrorKind::Timeout,
            Some(DiagnosticText::support_only("request timed out")),
        ))),
        Error::Tls { .. } => Some(Cause::Transport(TransportCause::new(
            TransportErrorKind::Tls,
            Some(DiagnosticText::support_only("token source TLS failure")),
        ))),
        Error::AuthLost { .. } => Some(Cause::Auth(AuthCause::ReauthorizationRequired)),
        Error::RefreshFailed { .. } => Some(Cause::Auth(AuthCause::RefreshTransient)),
        Error::RateLimited { retry_after, .. } => Some(Cause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint: *retry_after,
        })),
        Error::RetryBudgetExhausted {
            final_response: Some(response),
            ..
        } => server_cause_from_status(
            response.status.as_u16(),
            parse_retry_after_hint(response.headers.get(RETRY_AFTER)),
        ),
        Error::RetryBudgetExhausted {
            final_response: None,
            ..
        } => Some(Cause::Transport(TransportCause::new(
            TransportErrorKind::Network,
            Some(DiagnosticText::support_only("retry budget exhausted")),
        ))),
        Error::Status { code, headers, .. } => server_cause_from_status(
            code.as_u16(),
            parse_retry_after_hint(headers.get(RETRY_AFTER)),
        ),
        Error::Cancelled => Some(Cause::Transport(TransportCause::new(
            TransportErrorKind::Network,
            Some(DiagnosticText::support_only("request cancelled")),
        ))),
        Error::HopPolicy(_)
        | Error::LocalHold { .. }
        | Error::CostExceedsBurst { .. }
        | Error::EncodeBody { .. }
        | Error::InvalidHeader { .. }
        | Error::InvalidRequest { .. }
        | Error::RangeNotHonored { .. }
        | Error::UnexpectedResponse { .. }
        | Error::RedirectRejected { .. }
        | Error::MalformedRedirect { .. }
        | Error::RedirectLoop { .. }
        | Error::UnauthenticatedRedirectHop { .. }
        | Error::ResponseTooLarge { .. } => None,
    }
}

// Server is the shared table's fallback category, not a claim that the status
// is 5xx. Preserve unlisted 4xx status evidence too: users need the actual
// refusal for diagnosis, and guessing malformed input or permission loss would
// invent a recovery policy. Provider-specific primary classifications remain
// separate from this context-free supporting cause.
fn server_cause_from_status(code: u16, retry_hint: Option<RetryHint>) -> Option<Cause> {
    match Cause::from(classify_status(
        code,
        None,
        retry_hint,
        StatusPolicy::default(),
    )) {
        cause @ Cause::Server(_) => Some(cause),
        _ => None,
    }
}

#[cfg(test)]
mod retry_hint_tests {
    use super::*;
    use bifrost_types::RecoveryClass;

    #[test]
    fn unexpected_response_is_an_acknowledged_contract_violation() {
        let error = into_account_error_with_throttle_scope(
            Error::UnexpectedResponse {
                response: FinalResponse {
                    status: StatusCode::NOT_MODIFIED,
                    headers: HeaderMap::new(),
                    body: Bytes::from_static(b"stream unavailable"),
                },
            },
            NetErrorContext {
                provider: None,
                protocol: Protocol::Jmap,
                operation: AccountOperation::PushStream,
                scope: None,
            },
            None,
        );
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
        assert!(error.chain().iter().any(|cause| matches!(cause,
            Cause::Attempt(attempt) if attempt.transmission_state == TransmissionState::Acknowledged)));
    }

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
                retry_after: Some(RetryHint::At(at)),
                final_response: response.clone(),
            },
            Error::RetryBudgetExhausted {
                retry_after_history: vec![RetryHint::At(at)],
                final_response: Some(response),
                stop_reason: crate::RetryStopReason::AttemptBudget,
            },
        ];
        for error in errors {
            let error = into_account_error_with_throttle_scope(
                error,
                NetErrorContext {
                    provider: None,
                    protocol: Protocol::Jmap,
                    operation: AccountOperation::SyncChanges,
                    scope: None,
                },
                None,
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
        let error = into_account_error_with_throttle_scope(
            Error::RefreshFailed {
                retry_after: Some(RetryHint::At(at)),
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
            None,
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
    fn caller_policy_sets_scope_without_provider_inference() {
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
            "an explicit lack of caller policy must not infer a throttle scope"
        );

        let scoped = into_account_error_with_throttle_scope(
            Error::Status {
                code: StatusCode::TOO_MANY_REQUESTS,
                body: Bytes::new(),
                headers: HeaderMap::new(),
            },
            ctx,
            Some(ThrottleScope::ScopedResource),
        );
        assert_eq!(
            scoped.telemetry_fields().throttle_scope,
            Some(ThrottleScope::ScopedResource)
        );
    }
}

#[cfg(test)]
mod safe_constructor_tests {
    use super::*;

    #[test]
    fn account_exports_withhold_constructor_and_source_prose() {
        let marker = "password=echoed-credential";
        for error in [
            Error::EncodeBody {
                message: marker.into(),
                source: Some(Box::new(std::io::Error::other(marker))),
            },
            Error::InvalidHeader {
                message: marker.into(),
                source: None,
            },
            Error::InvalidRequest {
                field: "url",
                detail: marker.into(),
            },
            Error::Network {
                message: "connection reset".into(),
                transmission_state: TransmissionState::Unsent,
                source: Some(Box::new(std::io::Error::other(marker))),
            },
        ] {
            let error = into_account_error_with_throttle_scope(
                error,
                NetErrorContext {
                    provider: None,
                    protocol: Protocol::Jmap,
                    operation: bifrost_types::AccountOperation::SyncChanges,
                    scope: None,
                },
                None,
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
}
