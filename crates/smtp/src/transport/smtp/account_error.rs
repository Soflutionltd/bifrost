//! SMTP -> shared `AccountError` translation boundary.
//!
//! This is the only place that builds `AccountError` from SMTP transport-level
//! errors and message-builder errors. All `AccountErrorBuilder::new` call sites
//! for SMTP-originated failures live in this file; the rest of the crate keeps
//! emitting the low-level `transport::smtp::Error` / `crate::error::Error`
//! shapes and decorates SMTP errors with attempt and phase evidence.
//!
//! Recovery is never constructed here. Every conversion funnels through
//! `AccountErrorBuilder::try_build` (via `finish`; the infallible `build` is
//! gone), which derives recovery centrally from the kind + cause chain +
//! transmission state + idempotency.

use bifrost_types::error::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, Cause, DiagnosticText,
    EnhancedStatusCode as TypesEnhancedStatusCode, InternalCause, InternalErrorKind,
    MailboxUnavailableKind, Protocol, ProtocolErrorKind, Provider, RequestCause, RequestErrorKind,
    ResourceKind, ServerCause, ServerErrorKind, ThrottleScope, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};

use crate::error::Error as MessageError;
use crate::transport::smtp::error::{
    Error as SmtpError, ErrorKind, SmtpCommandPhase, SmtpTransmissionState,
};
use crate::transport::smtp::response::{EnhancedStatusCode as WireEnhancedStatusCode, Response};

/// Single funnel for every `AccountErrorBuilder::try_build` site in this
/// translation boundary. Protocol crates `expect(...)` on construction
/// errors because an invalid kind+cause combination is a library bug, not
/// recoverable state. Centralizing the call keeps the message stable.
fn finish(builder: AccountErrorBuilder) -> AccountError {
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Call-site evidence that is not carried on the low-level `SmtpError`.
///
/// There is deliberately no `scope` field. No `ErrorScope` variant can
/// honestly locate an SMTP failure: the id-bearing variants take typed
/// account-surface ids, and an SMTP envelope address is not one of them,
/// while `ErrorScope::Account` would claim an account-wide fault for what
/// is a single transaction on a single connection. Batch lanes correlate
/// through their `BatchItemId` instead. Omitting the field is what keeps
/// the blocking and async drivers in step here rather than convention.
#[derive(Clone, Debug)]
pub(crate) struct SmtpErrorContext {
    pub(crate) operation: Option<AccountOperation>,
    pub(crate) provider: Option<Provider>,
    pub(crate) protocol: Protocol,
    pub(crate) idempotency_override: Option<bool>,
    pub(crate) transmission_state: Option<SmtpTransmissionState>,
}

impl SmtpErrorContext {
    pub(crate) fn send(protocol: Protocol) -> Self {
        assert!(
            matches!(protocol, Protocol::Smtp | Protocol::Lmtp),
            "SmtpErrorContext requires Protocol::Smtp or Protocol::Lmtp"
        );
        Self {
            operation: Some(AccountOperation::Send),
            provider: None,
            protocol,
            idempotency_override: Some(false),
            transmission_state: None,
        }
    }
}

/// Convert a low-level SMTP transport `Error` into an `AccountError`.
pub(crate) fn into_account_error(error: SmtpError, ctx: SmtpErrorContext) -> AccountError {
    let attempt_state = error.attempt().or(ctx.transmission_state);
    // Wire-side phase knowledge has one carrier. Keeping a second phase on the
    // context would let batch call sites construct disagreeing evidence.
    let phase = error.phase();
    let diagnostic = error.diagnostic_text();

    if let Some(account_error) = error.preclassified_account_error() {
        return recontextualize_preclassified(account_error, &ctx);
    }

    match error.kind() {
        ErrorKind::Authentication => {
            let (kind, cause) = auth_reauth();
            let detail = name_mechanism(diagnostic.clone(), auth_mechanism(phase));
            build_basic(&ctx, kind, cause, attempt_state, detail.as_deref(), None)
        }
        ErrorKind::Parse => build_basic(
            &ctx,
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: diagnostic.clone().map(DiagnosticText::support_only),
            }),
            attempt_state,
            diagnostic.as_deref(),
            None,
        ),
        ErrorKind::InvalidInput if matches!(phase, Some(SmtpCommandPhase::Auth { .. })) => {
            // "No compatible authentication mechanism" and other local AUTH
            // refusals must surface as Authorization(PolicyBlocked) so
            // consumers route to a policy/reauth UX instead of "malformed
            // request" -> ClientBug (internal telemetry).
            //
            // A SCRAM policy error arrives here too, and that one does
            // know its rung, so the support text names it.
            let detail = name_mechanism(diagnostic.clone(), auth_mechanism(phase));
            build_basic(
                &ctx,
                AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
                Cause::Access(AccessCause::PolicyBlocked),
                attempt_state,
                detail.as_deref(),
                None,
            )
        }
        ErrorKind::InvalidInput => build_basic(
            &ctx,
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    diagnostic.clone().unwrap_or_else(|| "invalid input".into()),
                ),
            }),
            attempt_state,
            diagnostic.as_deref(),
            None,
        ),
        ErrorKind::FeatureUnsupported => build_basic(
            &ctx,
            // A relay that does not advertise the requested extension
            // (e.g. FUTURERELEASE) is reported as an unsupported send,
            // distinct from a malformed request so the IMAP boundary can
            // surface a stable `Unsupported(Send)` kind.
            AccountErrorKind::Unsupported(AccountOperation::Send),
            Cause::Request(RequestCause::Unsupported {
                operation: AccountOperation::Send,
            }),
            attempt_state,
            diagnostic.as_deref(),
            None,
        ),
        ErrorKind::ParameterOverLimit => build_basic(
            &ctx,
            // The value is out of the server-allowed window: a malformed
            // request, not an unsupported feature.
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    diagnostic
                        .clone()
                        .unwrap_or_else(|| "parameter over limit".into()),
                ),
            }),
            attempt_state,
            diagnostic.as_deref(),
            None,
        ),
        // A client-side invariant (a poisoned pool lock, a command that will
        // not serialize, a reply routed to the error helper, the LMTP status
        // bookkeeping disagreeing with itself): the implementation's fault,
        // never the relay's (`reference/error-model.md`).
        ErrorKind::Internal => build_basic(
            &ctx,
            AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
            Cause::Internal(InternalCause::new(
                InternalErrorKind::InvariantViolated,
                diagnostic.clone().map(DiagnosticText::support_only),
            )),
            attempt_state,
            diagnostic.as_deref(),
            None,
        ),
        ErrorKind::Policy => build_basic(
            &ctx,
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
            Cause::Access(AccessCause::PolicyBlocked),
            attempt_state,
            diagnostic.as_deref(),
            None,
        ),
        ErrorKind::Connection | ErrorKind::Network => build_transport(
            &ctx,
            TransportErrorKind::Network,
            TransportKind::Network,
            attempt_state,
            diagnostic.as_deref(),
        ),
        ErrorKind::Timeout => build_transport(
            &ctx,
            TransportErrorKind::Timeout,
            TransportKind::Timeout,
            attempt_state,
            diagnostic.as_deref(),
        ),
        ErrorKind::Tls => build_transport(
            &ctx,
            TransportErrorKind::Tls,
            TransportKind::Tls,
            attempt_state,
            diagnostic.as_deref(),
        ),
        ErrorKind::TransportShutdown => build_transport(
            &ctx,
            TransportErrorKind::Network,
            TransportKind::Network,
            attempt_state,
            diagnostic.as_deref(),
        ),
        ErrorKind::Transient(response) | ErrorKind::Permanent(response) => {
            response_to_account_error(response, &ctx, phase, attempt_state)
        }
    }
}

/// Keep a dependency's kind and cause chain while applying the SMTP call-site
/// context. OAuth token resolution happens before SMTP writes any message
/// command, and bifrost-net already knows whether its failure is auth loss,
/// refresh-transient, or another typed outcome.
fn recontextualize_preclassified(error: AccountError, ctx: &SmtpErrorContext) -> AccountError {
    let mut builder = error.into_builder().protocol(ctx.protocol);
    if let Some(operation) = ctx.operation {
        builder = builder.operation(operation);
    }
    if let Some(provider) = ctx.provider {
        builder = builder.provider(provider);
    }
    if let Some(idempotency_override) = ctx.idempotency_override {
        builder = builder.idempotency_override(idempotency_override);
    }
    finish(builder)
}

/// The SASL rung an auth-phase error was running, if the stamp names one.
fn auth_mechanism(phase: Option<SmtpCommandPhase>) -> Option<&'static str> {
    match phase {
        Some(SmtpCommandPhase::Auth { mechanism }) => mechanism,
        _ => None,
    }
}

/// Fold a mechanism name into the support-only diagnostic text.
///
/// The name rides the SUPPORT-ONLY tier (`DiagnosticText::support_only`,
/// read back through `AccountError::support_consented`), never the telemetry
/// tier. Not because the token is sensitive - it crosses the wire in clear on
/// the AUTH line and in the EHLO advertisement - but because the telemetry
/// tier carries no free-form text at all, and a mechanism name is not one of
/// the structured discriminants the shared error model declares. It is
/// deliberately not a new typed field on `AccountError` either.
///
/// This closes the asymmetry with the local-policy path, where
/// `password_mechanism` already spells out what it would not attempt: before
/// this, a server rejecting a rung (535, or 504 5.5.4) was the one auth
/// failure that could not say which rung.
///
/// Only the mechanism NAME crosses this boundary. The credential and the SASL
/// client responses stay inside the exchange.
fn name_mechanism(text: Option<String>, mechanism: Option<&'static str>) -> Option<String> {
    match (text, mechanism) {
        (Some(text), Some(mechanism)) if !text.is_empty() => {
            Some(format!("{text} (mechanism {mechanism})"))
        }
        (_, Some(mechanism)) => Some(format!("server refused mechanism {mechanism}")),
        // Unnamed lane: byte-identical to what it emitted before the name
        // existed, empty text included.
        (text, None) => text,
    }
}

fn build_basic(
    ctx: &SmtpErrorContext,
    kind: AccountErrorKind,
    cause: Cause,
    attempt: Option<SmtpTransmissionState>,
    diagnostic: Option<&str>,
    extra: Option<Cause>,
) -> AccountError {
    let mut builder = match kind {
        // Classification tables return parts so they can retain wire evidence.
        AccountErrorKind::Unsupported(operation) => {
            AccountError::unsupported(operation, ctx.protocol).into_builder()
        }
        _ => AccountErrorBuilder::new(kind, cause).protocol(ctx.protocol),
    };
    builder = apply_context(builder, ctx);
    if let Some(state) = attempt {
        builder = builder.push_cause(Cause::Attempt(AttemptCause::new(to_types_state(state))));
    }
    if let Some(extra) = extra {
        builder = builder.push_cause(extra);
    }
    if let Some(text) = diagnostic {
        builder = builder.text(DiagnosticText::support_only(text.to_owned()));
    }
    finish(builder)
}

fn build_transport(
    ctx: &SmtpErrorContext,
    kind: TransportErrorKind,
    cause_kind: TransportKind,
    attempt: Option<SmtpTransmissionState>,
    diagnostic: Option<&str>,
) -> AccountError {
    // Transport errors cannot have Acknowledged transmission state: if a
    // terminal response arrived, the kind would be Server/Auth/etc., not
    // Transport. Drop the attempt cause defensively rather than panicking.
    let attempt = attempt.filter(|s| *s != SmtpTransmissionState::Acknowledged);
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Transport(kind),
        Cause::Transport(TransportCause::new(
            cause_kind,
            diagnostic.map(|t| DiagnosticText::support_only(t.to_owned())),
        )),
    )
    .protocol(ctx.protocol);
    builder = apply_context(builder, ctx);
    if let Some(state) = attempt {
        builder = builder.push_cause(Cause::Attempt(AttemptCause::new(to_types_state(state))));
    }
    if let Some(text) = diagnostic {
        builder = builder.text(DiagnosticText::support_only(text.to_owned()));
    }
    finish(builder)
}

fn apply_context(mut builder: AccountErrorBuilder, ctx: &SmtpErrorContext) -> AccountErrorBuilder {
    if let Some(op) = ctx.operation {
        builder = builder.operation(op);
    }
    if let Some(provider) = ctx.provider {
        builder = builder.provider(provider);
    }
    if let Some(over) = ctx.idempotency_override {
        builder = builder.idempotency_override(over);
    }
    builder
}

pub(crate) fn to_types_state(state: SmtpTransmissionState) -> TransmissionState {
    match state {
        SmtpTransmissionState::Unsent => TransmissionState::Unsent,
        SmtpTransmissionState::InFlight => TransmissionState::InFlight,
        SmtpTransmissionState::Acknowledged => TransmissionState::Acknowledged,
    }
}

/// Build the shared `WireCause::Smtp(EnhancedStatusCode)` from an SMTP reply.
pub(crate) fn smtp_wire_cause(response: &Response) -> TypesEnhancedStatusCode {
    let enhanced = response
        .enhanced_status_code()
        .map(|code| DiagnosticText::support_only(code.to_string()));
    let text = response
        .message()
        .next()
        .map(|line| DiagnosticText::support_only(line.to_owned()));
    TypesEnhancedStatusCode::new(u16::from(response.code()), enhanced, text)
}

/// Convert an SMTP reply (4xx or 5xx) into an `AccountError`. This is the lane
/// classifier used both for the top-level transport mapper and for per-
/// recipient lane errors in batch send.
pub(crate) fn response_to_account_error(
    response: &Response,
    ctx: &SmtpErrorContext,
    phase: Option<SmtpCommandPhase>,
    attempt: Option<SmtpTransmissionState>,
) -> AccountError {
    let wire = smtp_wire_cause(response);
    let status = u16::from(response.code());
    let native = wire
        .enhanced
        .as_ref()
        .map(|d| d.value.clone())
        .unwrap_or_else(|| status.to_string());
    // On the AUTH lane the support text names the rung the server refused.
    // The classification itself is untouched by the name: it is read from
    // the reply code and the enhanced code, exactly as before.
    let text_first = name_mechanism(
        response.message().next().map(str::to_owned),
        auth_mechanism(phase),
    );

    let (kind, primary_cause) = classify_response(response, phase, ctx.protocol);
    // Decided from the same classification the builder is given, so the
    // throttle hint below cannot drift away from the kind it describes.
    let rate_or_quota = kind_is_rate_or_quota(&kind);

    let mut builder = match kind {
        // Classification tables return parts so they can retain wire evidence.
        AccountErrorKind::Unsupported(operation) => {
            AccountError::unsupported(operation, ctx.protocol).into_builder()
        }
        _ => AccountErrorBuilder::new(kind, primary_cause).protocol(ctx.protocol),
    }
    .status(Some(status))
    .native_code(native);
    builder = apply_context(builder, ctx);
    builder = builder.push_cause(Cause::Wire(WireCause::Smtp(wire)));
    // Negative SMTP replies are by definition Acknowledged unless the caller
    // overrode it (e.g. drained reply but body unsent).
    let attempt = attempt.unwrap_or(SmtpTransmissionState::Acknowledged);
    builder = builder.push_cause(Cause::Attempt(AttemptCause::new(to_types_state(attempt))));
    if let Some(text) = text_first {
        builder = builder.text(DiagnosticText::support_only(text));
    }
    // Wire 4xx rate-limit text gets a throttle-scope hint so recovery can
    // surface throttle scope. Default to ThrottleScope::Account.
    if rate_or_quota {
        builder = builder.throttle_scope(ThrottleScope::Account);
    }
    finish(builder)
}

fn kind_is_rate_or_quota(kind: &AccountErrorKind) -> bool {
    matches!(
        kind,
        AccountErrorKind::Server(ServerErrorKind::RateLimited | ServerErrorKind::QuotaExhausted)
    )
}

fn classify_response(
    response: &Response,
    phase: Option<SmtpCommandPhase>,
    protocol: Protocol,
) -> (AccountErrorKind, Cause) {
    let status = u16::from(response.code());
    let enhanced = response.enhanced_status_code();
    if let Some(code) = enhanced
        && let Some(mapping) = classify_enhanced(code, response, phase, protocol)
    {
        return mapping;
    }
    classify_status(status, response, phase, protocol)
}

fn diag_detail(response: &Response) -> DiagnosticText {
    let text = response
        .message()
        .next()
        .map(str::to_owned)
        .unwrap_or_default();
    DiagnosticText::support_only(text)
}

fn classify_enhanced(
    code: WireEnhancedStatusCode,
    response: &Response,
    phase: Option<SmtpCommandPhase>,
    protocol: Protocol,
) -> Option<(AccountErrorKind, Cause)> {
    let class = code.class;
    let is_recipient_lane = matches!(phase, Some(SmtpCommandPhase::RcptTo));
    let is_auth_lane = matches!(phase, Some(SmtpCommandPhase::Auth { .. }));
    let status = u16::from(response.code());
    // Class 2 inside an error path is a contract violation: a positive code
    // appeared on a path that already classified as an error. Handle first so
    // it does not fall through to the X.* arms.
    if class == 2 {
        return Some(contract_violation(response, protocol));
    }
    match (class, code.subject, code.detail) {
        // X.1 address status
        (_, 1, 0) => None,
        (_, 1, 1 | 2 | 6) => Some((
            AccountErrorKind::NotFound(ResourceKind::Mailbox),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Mailbox,
                id: response.message().next().map(str::to_owned),
            }),
        )),
        (_, 1, 3 | 4 | 7 | 8) => Some(malformed(response)),
        (_, 1, 5) => None,
        // X.2 mailbox status
        (_, 2, 0) => None,
        (_, 2, 1) => Some((
            AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                kind: if class == 5 {
                    MailboxUnavailableKind::Permanent
                } else {
                    MailboxUnavailableKind::Transient
                },
            }),
            Cause::Access(AccessCause::MailboxUnavailable {
                kind: if class == 5 {
                    MailboxUnavailableKind::Permanent
                } else {
                    MailboxUnavailableKind::Transient
                },
            }),
        )),
        (_, 2, 2) => Some((
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
        )),
        (_, 2, 3) => Some(malformed(response)),
        (_, 2, 4) => None,
        // X.3 mail system status
        (_, 3, 0) => None,
        (_, 3, 1) => Some((
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
        )),
        (_, 3, 2) => Some((
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        )),
        (_, 3, 3) => Some(unsupported_send()),
        (_, 3, 4) => Some(malformed(response)),
        (_, 3, 5) => Some(server_error(u16::from(response.code()))),
        // X.4 network and routing
        (_, 4, 0) => Some(if class == 4 {
            (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            )
        } else {
            server_error(u16::from(response.code()))
        }),
        (_, 4, 1..=3) => Some((
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        )),
        (_, 4, 4) if class == 5 && is_recipient_lane => Some((
            AccountErrorKind::NotFound(ResourceKind::Mailbox),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Mailbox,
                id: response.message().next().map(str::to_owned),
            }),
        )),
        (_, 4, 4) => Some((
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        )),
        (_, 4, 5) => Some((
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        )),
        (_, 4, 6) => Some(server_error(u16::from(response.code()))),
        (_, 4, 7) => Some(if class == 4 {
            (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            )
        } else {
            server_error(u16::from(response.code()))
        }),
        // X.5 delivery protocol status
        (_, 5, 0 | 1) => Some(contract_violation(response, protocol)),
        (_, 5, 2) => Some(malformed(response)),
        (_, 5, 3) => Some((
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        )),
        // RFC 4954 Section 6: `504 5.5.4` on the AUTH lane is "unrecognized
        // authentication type" - the server refused the MECHANISM, and
        // nothing about the request was malformed. The same X.5.4 subcode
        // reached by `501` really is an invalid-argument syntax fault, so the
        // reply code, not the phase alone, picks the arm.
        (_, 5, 4) if is_auth_lane && status == 504 => Some(auth_mechanism_refused()),
        (_, 5, 4) => Some(malformed(response)),
        (_, 5, 5) => Some(unsupported_send()),
        // X.6 content
        (_, 6, 0 | 1 | 2 | 3 | 5) => Some(malformed(response)),
        (_, 6, 4) => Some((
            AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
            Cause::Wire(WireCause::MalformedResponse {
                protocol,
                detail: Some(diag_detail(response)),
            }),
        )),
        // X.7 security/policy
        //
        // RFC 4954 Section 6: `454 4.7.0` is "temporary authentication
        // failure" - the server could not complete the exchange right now and
        // invites a retry. `Server(Unavailable)` loses that it was AUTH that
        // failed, and `PolicyBlocked` is terminal, which is the opposite
        // answer. Only the transient class carries this meaning; `5.7.0`
        // ("authentication required") stays a policy block.
        (4, 7, 0) if is_auth_lane => Some(auth_refresh_transient()),
        (_, 7, 0) => Some(policy_blocked()),
        (_, 7, 1 | 2) => Some(permission_denied()),
        (_, 7, 3..=6) => Some(policy_blocked()),
        (_, 7, 7) => Some(malformed(response)),
        (4, 7, 8) => Some(auth_refresh_transient()),
        (5, 7, 8) => Some(auth_reauth()),
        (_, 7, 9) => Some(policy_blocked()),
        (_, 7, 10 | 11) => Some(policy_blocked()),
        (_, 7, 12) => Some(auth_reauth()),
        (_, 7, 13) => Some((
            AccountErrorKind::Authorization(AccessErrorKind::AccountDisabled),
            Cause::Access(AccessCause::AccountDisabled),
        )),
        (_, 7, 14 | 15) => Some(policy_blocked()),
        (_, 7, 16) => Some(malformed(response)),
        (_, 7, 17 | 18) => Some(permission_denied()),
        (_, 7, 19) => Some((
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        )),
        (_, 7, 20..=22) => Some(malformed(response)),
        (_, 7, 23..=26) => Some(policy_blocked()),
        (_, 7, 27) => Some(malformed(response)),
        (_, 7, 28) => Some((
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        )),
        (_, 7, 29) => Some(malformed(response)),
        (_, 7, 30) => Some(policy_blocked()),
        // X.7 fallback by class
        (4, 7, _) => {
            let text = response.message().next().unwrap_or("").to_ascii_lowercase();
            let throttled =
                text.contains("rate") || text.contains("limit") || text.contains("throttle");
            Some(if throttled {
                (
                    AccountErrorKind::Server(ServerErrorKind::RateLimited),
                    Cause::Server(ServerCause::RateLimited { retry_hint: None }),
                )
            } else {
                (
                    AccountErrorKind::Server(ServerErrorKind::Unavailable),
                    Cause::Server(ServerCause::Unavailable { retry_hint: None }),
                )
            })
        }
        (5, 7, _) => Some(policy_blocked()),
        _ => None,
    }
}

fn classify_status(
    status: u16,
    response: &Response,
    phase: Option<SmtpCommandPhase>,
    protocol: Protocol,
) -> (AccountErrorKind, Cause) {
    let is_recipient_lane = matches!(phase, Some(SmtpCommandPhase::RcptTo));
    let is_auth_lane = matches!(phase, Some(SmtpCommandPhase::Auth { .. }));
    match status {
        // RFC 4954 Section 6, before the generic 4xx and 5xx rows below.
        // Outside the AUTH lane these codes keep their RFC 5321 meanings:
        // 454 is "TLS not available", 502/504 are command-not-implemented.
        454 if is_auth_lane => auth_refresh_transient(),
        502 | 504 if is_auth_lane => auth_mechanism_refused(),
        421 | 450 | 451 | 455 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
        432 => auth_refresh_transient(),
        452 => (
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
        ),
        454 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
        400..=499 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
        500 | 501 => malformed(response),
        502 | 504 => unsupported_send(),
        503 => contract_violation(response, protocol),
        530 | 535 => auth_reauth(),
        534 | 538 => policy_blocked(),
        550 => {
            if is_recipient_lane {
                (
                    AccountErrorKind::NotFound(ResourceKind::Mailbox),
                    Cause::Request(RequestCause::NotFound {
                        what: ResourceKind::Mailbox,
                        id: response.message().next().map(str::to_owned),
                    }),
                )
            } else {
                permission_denied()
            }
        }
        551 => (
            AccountErrorKind::NotFound(ResourceKind::Mailbox),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Mailbox,
                id: response.message().next().map(str::to_owned),
            }),
        ),
        552 => {
            let text = response.message().next().unwrap_or("").to_ascii_lowercase();
            if text.contains("mailbox") || text.contains("storage") || text.contains("quota") {
                (
                    AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                    Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
                )
            } else {
                malformed(response)
            }
        }
        553 => malformed(response),
        554 => {
            let text = response.message().next().unwrap_or("").to_ascii_lowercase();
            if text.contains("policy") || text.contains("security") {
                policy_blocked()
            } else {
                server_error(554)
            }
        }
        555 => unsupported_send(),
        500..=599 => server_error(status),
        _ => contract_violation(response, protocol),
    }
}

fn malformed(response: &Response) -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: diag_detail(response),
        }),
    )
}

fn unsupported_send() -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Unsupported(AccountOperation::Send),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::Send,
        }),
    )
}

/// The server refused the AUTH mechanism itself (RFC 4954 `504 5.5.4`
/// "unrecognized authentication type", or a bare `502`/`504` on the AUTH
/// lane).
///
/// `Authorization(PolicyBlocked)` -> `NeedsPolicyChange`, matching the LOCAL
/// refusal already built a few hundred lines up for "no compatible
/// authentication mechanism was found". Client and server failing to agree on
/// a mechanism is one condition and gets one answer regardless of which side
/// noticed it first.
///
/// The two neighbours are both wrong answers, not merely less precise ones.
/// `Unsupported(Send)` tells the consumer that SENDING MAIL is unsupported on
/// a server that only declined one SASL mechanism, and it is terminal, so the
/// account is written off over a mechanism-list mismatch. `Authentication(_)`
/// derives `AuthLost`, sending the user to re-enter credentials that were
/// never presented and were never the problem - the server rejected the
/// mechanism before any credential crossed the wire.
fn auth_mechanism_refused() -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
        Cause::Access(AccessCause::PolicyBlocked),
    )
}

fn auth_reauth() -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
        Cause::Auth(AuthCause::ReauthorizationRequired),
    )
}

fn auth_refresh_transient() -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient),
        Cause::Auth(AuthCause::RefreshTransient),
    )
}

fn policy_blocked() -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
        Cause::Access(AccessCause::PolicyBlocked),
    )
}

fn permission_denied() -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
        Cause::Access(AccessCause::PermissionDenied { resource: None }),
    )
}

fn contract_violation(response: &Response, protocol: Protocol) -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        Cause::Wire(WireCause::MalformedResponse {
            protocol,
            detail: Some(diag_detail(response)),
        }),
    )
}

fn server_error(status: u16) -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Server(ServerErrorKind::Error {
            status: Some(status),
        }),
        Cause::Server(ServerCause::Error {
            status: Some(status),
        }),
    )
}

/// Boundary translation: map a message-builder `crate::error::Error` into the
/// shared `AccountError` shape. Every variant of `MessageError` is
/// exhaustively matched - adding a variant to `MessageError` forces a refresh
/// here.
///
/// No production caller today: nothing inside the workspace builds a `Message`
/// (IMAP submission takes raw RFC822 bytes), and the live boundary
/// `into_account_error` dispatches on `SmtpError::kind`, which never carries
/// `MessageError` variants. This exists so that an account-oriented caller
/// that DOES construct a `Message` - a draft pipeline, a future `Account`
/// impl - has one translation point rather than inventing a second mapping.
/// Kept deliberately; see the `dead_code` allowance below.
#[allow(dead_code)]
pub(crate) fn message_error_to_account_error(
    error: MessageError,
    protocol: Protocol,
) -> AccountError {
    let (kind, cause, detail): (AccountErrorKind, Cause, String) = match &error {
        MessageError::MissingFrom => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    "missing source address, invalid envelope".to_owned(),
                ),
            }),
            "missing source address, invalid envelope".to_owned(),
        ),
        MessageError::MissingTo => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    "missing destination address, invalid envelope".to_owned(),
                ),
            }),
            "missing destination address, invalid envelope".to_owned(),
        ),
        MessageError::TooManyFrom => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    "there can only be one source address".to_owned(),
                ),
            }),
            "there can only be one source address".to_owned(),
        ),
        MessageError::EmailMissingAt => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("missing @ in email address".to_owned()),
            }),
            "missing @ in email address".to_owned(),
        ),
        MessageError::EmailMissingLocalPart => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    "missing local part in email address".to_owned(),
                ),
            }),
            "missing local part in email address".to_owned(),
        ),
        MessageError::EmailMissingDomain => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("missing domain in email address".to_owned()),
            }),
            "missing domain in email address".to_owned(),
        ),
        MessageError::CannotParseFilename => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    "could not parse attachment filename".to_owned(),
                ),
            }),
            "could not parse attachment filename".to_owned(),
        ),
        MessageError::NonAsciiChars => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("contains non-ASCII chars".to_owned()),
            }),
            "contains non-ASCII chars".to_owned(),
        ),
        MessageError::InvalidInput(message) => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(message.clone()),
            }),
            message.clone(),
        ),
        MessageError::Io(io) => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(io.to_string()),
            }),
            io.to_string(),
        ),
    };

    let builder = AccountErrorBuilder::new(kind, cause)
        .protocol(protocol)
        .operation(AccountOperation::Send)
        .idempotency_override(false)
        .text(DiagnosticText::support_only(detail));
    finish(builder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::smtp::error as smtp_error;
    use crate::transport::smtp::response::{Category, Code, Detail, Severity};
    use bifrost_types::error::{
        AccountErrorKind, AuthErrorKind, ReconcileReason, RecoveryClass, RetryReason,
        ServerErrorKind,
    };

    fn response(class: Severity, category: Category, detail: Detail, lines: &[&str]) -> Response {
        Response::new(
            Code::new(class, category, detail),
            lines.iter().map(|s| (*s).to_owned()).collect(),
        )
    }

    fn ctx_smtp_send() -> SmtpErrorContext {
        SmtpErrorContext::send(Protocol::Smtp)
    }

    #[test]
    fn transport_network_unsent_is_retryable() {
        let err = smtp_error::network(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "boom",
        ))
        .with_attempt(SmtpTransmissionState::Unsent);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Transport(TransportErrorKind::Network)
        ));
        assert!(matches!(
            account.recovery(),
            RecoveryClass::Retry(advice) if matches!(advice.reason, RetryReason::Transport)
        ));
    }

    /// A client-side invariant is the implementation's fault, never the
    /// relay's: `Internal(InvariantViolated)`, reported as a bug. Before the
    /// send it is terminal; after a send that may have reached the relay it
    /// reads the target back, because the message may already be queued.
    #[test]
    fn an_internal_error_is_internal_not_a_relay_fault() {
        let unsent = into_account_error(
            smtp_error::internal("connection pool lock poisoned")
                .with_attempt(SmtpTransmissionState::Unsent),
            ctx_smtp_send(),
        );
        assert_eq!(
            unsent.kind(),
            &AccountErrorKind::Internal(bifrost_types::InternalErrorKind::InvariantViolated)
        );
        assert_eq!(unsent.recovery(), &RecoveryClass::InternalFailure);
        assert_eq!(
            unsent.suggested_remediation(),
            Some(&bifrost_types::RemediationAction::ReportBug)
        );

        let in_flight = into_account_error(
            smtp_error::internal("server returned fewer LMTP statuses than accepted recipients")
                .with_attempt(SmtpTransmissionState::InFlight),
            ctx_smtp_send(),
        );
        assert!(matches!(
            in_flight.recovery(),
            RecoveryClass::Reconcile(advice)
                if advice.reason == ReconcileReason::InternalFailureAfterSend
        ));
    }

    #[test]
    fn transport_network_inflight_send_reconciles() {
        let err = smtp_error::network(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "boom",
        ))
        .with_attempt(SmtpTransmissionState::InFlight);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.recovery(),
            RecoveryClass::Reconcile(advice) if matches!(advice.reason, ReconcileReason::TransportDropAfterSend)
        ));
    }

    #[test]
    fn transport_timeout_with_attempt() {
        let err = smtp_error::network(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out",
        ))
        .with_attempt(SmtpTransmissionState::InFlight);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Transport(TransportErrorKind::Timeout)
        ));
    }

    #[test]
    fn transport_tls_maps_to_tls() {
        let err = smtp_error::tls("handshake failed").with_attempt(SmtpTransmissionState::Unsent);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Transport(TransportErrorKind::Tls)
        ));
    }

    #[test]
    fn policy_kind_maps_to_policy_blocked() {
        let err = smtp_error::policy("plaintext auth refused");
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
    }

    #[test]
    fn enhanced_5_1_1_maps_to_not_found_mailbox() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::Information,
            Detail::One,
            &["5.1.1 user unknown"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::NotFound(ResourceKind::Mailbox)
        ));
    }

    #[test]
    fn enhanced_5_1_3_maps_to_malformed() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::Information,
            Detail::Three,
            &["5.1.3 bad syntax"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn enhanced_4_2_2_quota_retries() {
        let resp = response(
            Severity::TransientNegativeCompletion,
            Category::Connections,
            Detail::Two,
            &["4.2.2 mailbox full temporarily"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        ));
        assert_eq!(
            account.telemetry_fields().throttle_scope,
            Some(ThrottleScope::Account)
        );
        // Acknowledged + non-idempotent Send falls through to retry by table.
        assert!(account.recovery().is_retryable() || account.recovery().requires_reconciliation());
    }

    #[test]
    fn enhanced_5_2_2_quota() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::Connections,
            Detail::Two,
            &["5.2.2 mailbox full"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        ));
    }

    #[test]
    fn enhanced_4_4_5_rate_limited() {
        let resp = response(
            Severity::TransientNegativeCompletion,
            Category::Unspecified4,
            Detail::Five,
            &["4.4.5 network congestion"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Server(ServerErrorKind::RateLimited)
        ));
        assert_eq!(
            account.telemetry_fields().throttle_scope,
            Some(ThrottleScope::Account)
        );
    }

    #[test]
    fn enhanced_5_5_2_malformed() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::MailSystem,
            Detail::Two,
            &["5.5.2 syntax error"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn enhanced_5_7_1_policy_or_permission() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::MailSystem,
            Detail::One,
            &["5.7.1 delivery not authorized"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
    }

    #[test]
    fn enhanced_5_7_8_auth_reauth() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::MailSystem,
            Detail::Eight,
            &["5.7.8 bad credentials"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
    }

    #[test]
    fn enhanced_4_7_8_auth_refresh_transient() {
        let resp = response(
            Severity::TransientNegativeCompletion,
            Category::MailSystem,
            Detail::Eight,
            &["4.7.8 try again"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient)
        ));
    }

    #[test]
    fn status_421_fallback() {
        let resp = response(
            Severity::TransientNegativeCompletion,
            Category::Connections,
            Detail::One,
            &["service not available"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
    }

    #[test]
    fn status_452_fallback() {
        let resp = response(
            Severity::TransientNegativeCompletion,
            Category::MailSystem,
            Detail::Two,
            &["insufficient system storage"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        ));
    }

    #[test]
    fn status_535_fallback_reauth() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::Unspecified3,
            Detail::Five,
            &["authentication failed"],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
    }

    #[test]
    fn status_555_fallback_unsupported() {
        let resp = response(
            Severity::PermanentNegativeCompletion,
            Category::MailSystem,
            Detail::Five,
            &["parameters not recognized"],
        );
        // Use 555 by constructing a different code.
        let code = Code::new(
            Severity::PermanentNegativeCompletion,
            Category::MailSystem,
            Detail::Five,
        );
        assert_eq!(u16::from(code), 555);
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    #[test]
    fn auth_no_compatible_mechanism_routes_to_policy_blocked() {
        // smtp-D2: an InvalidInput error originating from the AUTH path
        // (e.g. server advertises no compatible mechanism for our
        // credentials) must surface as Authorization(PolicyBlocked), not
        // Request(Malformed) -> ClientBug. Consumer UX must offer reauth/
        // policy-change, not "library bug, see internal telemetry".
        let err = crate::transport::smtp::error::invalid_input(
            "No compatible authentication mechanism was found",
        )
        .with_phase(SmtpCommandPhase::Auth { mechanism: None });
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
    }

    #[test]
    fn invalid_input_without_phase_stays_malformed() {
        // Phase-less InvalidInput keeps the original Request(Malformed)
        // routing; only the Auth-tagged path elevates to PolicyBlocked.
        let err = crate::transport::smtp::error::invalid_input("bad SIZE parameter");
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn future_release_unsupported_and_over_limit_map_to_distinct_kinds() {
        // A4 brick 4.6a: the FUTURERELEASE-unsupported and HOLDFOR-over-
        // limit cases must be told apart downstream. The former is an
        // unsupported send (so the IMAP boundary can surface a stable
        // Unsupported(Send) kind); the latter is a malformed request (the
        // time is outside the server-allowed window).
        let unsupported = crate::transport::smtp::error::feature_unsupported(
            "FUTURERELEASE requires server FUTURERELEASE support",
        );
        let unsupported = into_account_error(unsupported, ctx_smtp_send());
        assert!(matches!(
            unsupported.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));

        let over_limit = crate::transport::smtp::error::parameter_over_limit(
            "HOLDFOR exceeds the server-advertised FUTURERELEASE limit",
        );
        let over_limit = into_account_error(over_limit, ctx_smtp_send());
        assert!(matches!(
            over_limit.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));

        assert_ne!(unsupported.kind(), over_limit.kind());
    }

    #[test]
    fn error_phase_drives_auth_classification() {
        // Phase has one carrier: `SmtpErrorContext` cannot hold one, so batch
        // conversion cannot construct disagreeing evidence.
        let err = crate::transport::smtp::error::invalid_input("x")
            .with_phase(SmtpCommandPhase::Auth { mechanism: None });
        let account = into_account_error(err, ctx_smtp_send());
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
    }

    fn auth_error(resp: Response) -> AccountError {
        into_account_error(
            smtp_error::status(resp).with_phase(SmtpCommandPhase::Auth { mechanism: None }),
            ctx_smtp_send(),
        )
    }

    fn auth_error_named(resp: Response, mechanism: &'static str) -> AccountError {
        into_account_error(
            smtp_error::status(resp).with_phase(SmtpCommandPhase::Auth {
                mechanism: Some(mechanism),
            }),
            ctx_smtp_send(),
        )
    }

    fn support_text(account: &AccountError) -> Vec<String> {
        account
            .support_consented()
            .support_text
            .iter()
            .map(|text| (*text).to_owned())
            .collect()
    }

    #[test]
    fn a_named_rung_reaches_the_support_tier_without_moving_the_classification() {
        // 535 and `504 5.5.4` are the two server rejections that say "auth
        // failed" without saying which rung. The name belongs in the
        // support-only tier: it is not sensitive (it travels in clear on the
        // AUTH line and in EHLO), but the telemetry tier carries no free-form
        // text and a mechanism name is not a declared discriminant. The
        // classification is read from the reply code and must be identical to
        // the unnamed case.
        let rejected = response(
            Severity::PermanentNegativeCompletion,
            Category::Unspecified3,
            Detail::Five,
            &["5.7.8 credentials rejected"],
        );
        let named = auth_error_named(rejected.clone(), "SCRAM-SHA-256-PLUS");
        let unnamed = auth_error(rejected);
        assert_eq!(named.kind(), unnamed.kind());
        assert!(matches!(
            named.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
        assert!(
            support_text(&named)
                .iter()
                .any(|text| text == "5.7.8 credentials rejected (mechanism SCRAM-SHA-256-PLUS)"),
            "{:?}",
            support_text(&named)
        );
        // The unnamed lane is unchanged: bare server text, no invented name.
        assert!(
            support_text(&unnamed)
                .iter()
                .any(|text| text == "5.7.8 credentials rejected"),
            "{:?}",
            support_text(&unnamed)
        );
        assert!(
            !support_text(&unnamed)
                .iter()
                .any(|text| text.contains("mechanism")),
            "{:?}",
            support_text(&unnamed)
        );

        let refused = auth_error_named(
            response(
                Severity::PermanentNegativeCompletion,
                Category::Syntax,
                Detail::Four,
                &["5.5.4 unrecognized authentication type"],
            ),
            "SCRAM-SHA-1-PLUS",
        );
        assert!(matches!(
            refused.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
        assert!(
            support_text(&refused).iter().any(|text| text
                == "5.5.4 unrecognized authentication type (mechanism SCRAM-SHA-1-PLUS)"),
            "{:?}",
            support_text(&refused)
        );
    }

    #[test]
    fn a_textless_rejection_still_reports_the_refused_rung() {
        // A bare `535\r\n` carries no message line at all, which is exactly
        // the case where the operator has nothing else to go on. The name
        // must stand alone rather than being dropped with the empty text.
        let account = auth_error_named(
            response(
                Severity::PermanentNegativeCompletion,
                Category::Unspecified3,
                Detail::Five,
                &[],
            ),
            "OAUTHBEARER",
        );
        assert_eq!(
            support_text(&account),
            ["server refused mechanism OAUTHBEARER"]
        );
    }

    #[test]
    fn scram_invalid_proof_matches_a_535_credential_rejection() {
        for protocol in [Protocol::Smtp, Protocol::Lmtp] {
            let ctx = SmtpErrorContext::send(protocol);
            let sasl = bifrost_sasl::verify_server_final("e=invalid-proof", &[]).unwrap_err();
            let error: SmtpError = sasl.into();
            let account = into_account_error(
                error.with_phase(SmtpCommandPhase::Auth {
                    mechanism: Some("SCRAM-SHA-256-PLUS"),
                }),
                ctx,
            );
            let reply = auth_error(response(
                Severity::PermanentNegativeCompletion,
                Category::Unspecified3,
                Detail::Five,
                &["5.7.8 credentials rejected"],
            ));
            assert_eq!(account.kind(), reply.kind());
            assert_eq!(account.recovery(), reply.recovery());
            assert!(matches!(account.recovery(), RecoveryClass::AuthLost));
            assert!(support_text(&account).iter().any(|text| {
                text.contains("invalid-proof") && text.contains("SCRAM-SHA-256-PLUS")
            }));
        }
    }

    #[test]
    fn a_scram_policy_error_carries_its_rung_through_the_policy_lane() {
        let sasl =
            bifrost_sasl::verify_server_final("e=channel-binding-not-supported", &[]).unwrap_err();
        let error: SmtpError = sasl.into();
        let account = into_account_error(
            error.with_phase(SmtpCommandPhase::Auth {
                mechanism: Some("SCRAM-SHA-256"),
            }),
            ctx_smtp_send(),
        );
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
        assert!(
            support_text(&account).iter().any(|text| text
                == "SCRAM server error: channel-binding-not-supported (mechanism SCRAM-SHA-256)"),
            "{:?}",
            support_text(&account)
        );
    }

    #[test]
    fn auth_lane_504_is_a_mechanism_refusal_not_an_unsupported_send() {
        // RFC 4954 Section 6: 504 on the AUTH lane is "unrecognized
        // authentication type". Routing it through `unsupported_send` told the
        // consumer that SENDING MAIL is unsupported on a server that had only
        // declined one SASL mechanism.
        let account = auth_error(response(
            Severity::PermanentNegativeCompletion,
            Category::Syntax,
            Detail::Four,
            &["unrecognized authentication type"],
        ));
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
        assert_eq!(account.message_key(), "authz.policy-blocked");
    }

    #[test]
    fn auth_lane_504_5_5_4_is_a_mechanism_refusal_not_malformed() {
        // Same reply carrying its enhanced code, which took the `(_, 5, 4)`
        // arm and landed on `Request(Malformed)` -> ClientBug: a library-bug
        // report for a server-side mechanism refusal.
        let account = auth_error(response(
            Severity::PermanentNegativeCompletion,
            Category::Syntax,
            Detail::Four,
            &["5.5.4 unrecognized authentication type"],
        ));
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
    }

    #[test]
    fn non_auth_504_and_501_5_5_4_keep_their_old_classification() {
        // The fix is lane-scoped and code-scoped. A 504 outside the AUTH lane
        // is still an unsupported send, and `501 5.5.4` - invalid command
        // ARGUMENTS, not an unrecognized mechanism - stays malformed even on
        // the AUTH lane.
        let off_lane = into_account_error(
            smtp_error::status(response(
                Severity::PermanentNegativeCompletion,
                Category::Syntax,
                Detail::Four,
                &["command not implemented"],
            )),
            ctx_smtp_send(),
        );
        assert!(matches!(
            off_lane.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));

        let bad_args = auth_error(response(
            Severity::PermanentNegativeCompletion,
            Category::Syntax,
            Detail::One,
            &["5.5.4 invalid command arguments"],
        ));
        assert!(matches!(
            bad_args.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn auth_lane_454_is_a_transient_auth_failure_not_a_server_outage() {
        // RFC 4954 Section 6: `454 4.7.0` is "temporary authentication
        // failure". It used to land on `Server(Unavailable)` (via both the
        // status row and the `(_, 7, 0)` enhanced row), which loses that AUTH
        // is what failed and retries without refreshing the credential.
        for lines in [
            ["temporary authentication failure"],
            ["4.7.0 temporary authentication failure"],
        ] {
            let account = auth_error(response(
                Severity::TransientNegativeCompletion,
                Category::MailSystem,
                Detail::Four,
                &lines,
            ));
            assert!(
                matches!(
                    account.kind(),
                    AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient)
                ),
                "got {:?} for {lines:?}",
                account.kind()
            );
            assert!(matches!(
                account.recovery(),
                RecoveryClass::Retry(advice)
                    if matches!(advice.reason, RetryReason::RefreshTransient)
            ));
        }
    }

    #[test]
    fn non_auth_454_stays_server_unavailable() {
        // Outside AUTH, RFC 5321 gives 454 to "TLS not available", which is a
        // server condition and must keep its old answer.
        let account = into_account_error(
            smtp_error::status(response(
                Severity::TransientNegativeCompletion,
                Category::MailSystem,
                Detail::Four,
                &["TLS not available due to temporary reason"],
            )),
            ctx_smtp_send(),
        );
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
    }

    #[test]
    fn a_5_7_0_on_the_auth_lane_is_still_a_policy_block() {
        // Only the TRANSIENT class carries the RFC 4954 meaning. `530 5.7.0`
        // ("authentication required") must not follow 4.7.0 onto the retry
        // lane.
        let account = auth_error(response(
            Severity::PermanentNegativeCompletion,
            Category::Unspecified3,
            Detail::Zero,
            &["5.7.0 authentication required"],
        ));
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
    }

    #[test]
    fn message_error_missing_from_is_malformed() {
        let err = message_error_to_account_error(crate::error::Error::MissingFrom, Protocol::Smtp);
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn message_error_every_variant_maps() {
        // smtp-D6 contract: every MessageError variant maps to AccountError
        // without panicking. If a new variant is added to MessageError, the
        // match in message_error_to_account_error becomes non-exhaustive
        // and this test catches the gap at compile time via the boundary
        // function. Here we just exercise each constructor.
        use crate::error::Error as ME;
        let cases: Vec<ME> = vec![
            ME::MissingFrom,
            ME::MissingTo,
            ME::TooManyFrom,
            ME::EmailMissingAt,
            ME::EmailMissingLocalPart,
            ME::EmailMissingDomain,
            ME::CannotParseFilename,
            ME::NonAsciiChars,
            ME::InvalidInput("payload".to_owned()),
            ME::Io(std::io::Error::other("io")),
        ];
        for case in cases {
            let account = message_error_to_account_error(case, Protocol::Smtp);
            // All map to Request(Malformed) today; ClientBug recovery.
            assert!(matches!(
                account.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ));
            assert!(account.recovery().is_terminal());
        }
    }

    #[test]
    fn wire_cause_includes_status_enhanced_and_text() {
        let resp = Response::new(
            Code::new(
                Severity::PermanentNegativeCompletion,
                Category::MailSystem,
                Detail::Zero,
            ),
            vec!["5.1.1 user unknown".to_owned()],
        );
        let err = smtp_error::status(resp);
        let account = into_account_error(err, ctx_smtp_send());
        // The chain must contain a WireCause::Smtp with the right pieces. We
        // can't inspect the chain directly through the opaque AccountError, so
        // assert via diagnostics: status 550 and native_code 5.1.1.
        let view = account.telemetry_fields();
        assert_eq!(view.status, Some(550));
        assert_eq!(view.native_code, Some("5.1.1"));
    }

    #[test]
    fn lmtp_contract_violations_keep_the_lmtp_protocol() {
        let status_response = response(
            Severity::PositiveCompletion,
            Category::MailSystem,
            Detail::Zero,
            &["bad command sequence"],
        );
        let (_, status_cause) = classify_response(&status_response, None, Protocol::Lmtp);
        assert!(matches!(
            status_cause,
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Lmtp,
                ..
            })
        ));

        let enhanced_response = response(
            Severity::PermanentNegativeCompletion,
            Category::MailSystem,
            Detail::Four,
            &["5.6.4 conversion required"],
        );
        let (_, enhanced_cause) = classify_response(&enhanced_response, None, Protocol::Lmtp);
        assert!(matches!(
            enhanced_cause,
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Lmtp,
                ..
            })
        ));
    }
}

#[cfg(test)]
mod unsupported_constructor_tests {
    use super::*;

    #[test]
    fn unsupported_constructor_stamps_protocol_and_kind() {
        let operation = bifrost_types::AccountOperation::Send;
        for protocol in [Protocol::Smtp, Protocol::Lmtp] {
            let error = into_account_error(
                crate::transport::smtp::error::feature_unsupported("unavailable"),
                SmtpErrorContext::send(protocol),
            );
            assert_eq!(error.protocol(), Some(protocol));
            assert_eq!(error.operation(), Some(operation));
            assert_eq!(error.kind(), &AccountErrorKind::Unsupported(operation));
        }
    }
}
