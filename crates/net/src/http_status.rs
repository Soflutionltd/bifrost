//! Shared HTTP status classification and provider-declared refinements.

use bifrost_types::{
    AccessCause, AccessErrorKind, AccountErrorBuilder, AccountErrorKind, AuthCause, AuthErrorKind,
    Cause, DiagnosticText, ErrorScope, RequestCause, RequestErrorKind, ResourceKind, RetryHint,
    ServerCause, ServerErrorKind, StateCause, SyncStateErrorKind, ThrottleBudget, ThrottleScope,
};

/// A provider exception to the common HTTP table. Typed response-body signals
/// remain in the provider boundary and take precedence over status fallback.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum StatusOverride {
    ConcurrencyConflict,
    CursorInvalid,
}

/// Provider facts needed by status fallback, including responses without a
/// provider envelope. Resource hints take precedence over the diagnostic scope.
#[derive(Clone, Copy, Debug, Default)]
pub struct StatusPolicy {
    pub resource: Option<ResourceKind>,
    pub overrides: &'static [(u16, StatusOverride)],
}

#[derive(Clone, Debug)]
pub struct StatusClass {
    pub kind: AccountErrorKind,
    pub cause: Cause,
    pub throttles: bool,
}

impl StatusClass {
    /// Also used after a provider's typed signal overrides the status fallback.
    #[must_use]
    pub fn new(kind: AccountErrorKind, cause: Cause) -> Self {
        let throttles = matches!(
            kind,
            AccountErrorKind::Server(
                ServerErrorKind::RateLimited | ServerErrorKind::QuotaExhausted
            )
        );
        Self {
            kind,
            cause,
            throttles,
        }
    }
}

/// Status and throttle policy carried through the complete transport funnel.
#[derive(Clone, Debug, Default)]
pub struct HttpErrorPolicy {
    pub status: StatusPolicy,
    pub throttle_scope: Option<ThrottleScope>,
    pub throttle_budget: Option<ThrottleBudget>,
}

impl From<Option<ThrottleScope>> for HttpErrorPolicy {
    fn from(throttle_scope: Option<ThrottleScope>) -> Self {
        Self {
            throttle_scope,
            ..Self::default()
        }
    }
}

impl HttpErrorPolicy {
    /// Stamp provider throttle identity once, only on the final classification.
    #[must_use]
    pub fn apply_throttle(
        &self,
        mut builder: AccountErrorBuilder,
        throttles: bool,
    ) -> AccountErrorBuilder {
        if throttles {
            if let Some(scope) = self.throttle_scope {
                builder = builder.throttle_scope(scope);
            }
            if let Some(budget) = &self.throttle_budget {
                builder = builder.throttle_budget(budget.clone());
            }
        }
        builder
    }
}

/// Classify HTTP evidence independently of its body format. An enterprise user
/// expects the same refusal to recover the same way when a proxy or provider
/// changes the response body. A failed precondition refreshes state; a locked
/// DAV resource is declared by that provider. Unknown server statuses preserve
/// net's status-line classification rather than guessing from response prose.
#[must_use]
pub fn classify_status(
    code: u16,
    scope: Option<&ErrorScope>,
    retry_hint: Option<RetryHint>,
    policy: StatusPolicy,
) -> StatusClass {
    let resource = policy
        .resource
        .or_else(|| scope.and_then(ErrorScope::resource));
    let override_class = policy.overrides.iter().find(|(status, _)| *status == code);
    let (kind, cause) = match override_class.map(|(_, class)| class) {
        Some(StatusOverride::ConcurrencyConflict) => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        Some(StatusOverride::CursorInvalid) if matches!(scope, Some(ErrorScope::Cursor(_))) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        _ => match code {
            400 | 422 => (
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(format!("HTTP {code} malformed request")),
                }),
            ),
            401 => (
                AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
                Cause::Auth(AuthCause::ReauthorizationRequired),
            ),
            403 => (
                AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                Cause::Access(AccessCause::PermissionDenied { resource }),
            ),
            404 if resource.is_some() => {
                let what = resource.expect("resource checked");
                (
                    AccountErrorKind::NotFound(what),
                    Cause::Request(RequestCause::NotFound {
                        what,
                        id: scope.and_then(ErrorScope::object_id).map(str::to_owned),
                    }),
                )
            }
            409 | 412 => (
                AccountErrorKind::ConcurrencyConflict,
                Cause::State(StateCause::ConcurrencyConflict),
            ),
            // An enterprise user with an oversized attachment needs a size
            // refusal so they can reduce it, rather than a client-bug report.
            413 => (
                AccountErrorKind::Request(RequestErrorKind::PayloadTooLarge),
                Cause::Request(RequestCause::PayloadTooLarge {
                    detail: DiagnosticText::support_only("HTTP 413 payload too large"),
                }),
            ),
            410 if matches!(scope, Some(ErrorScope::Cursor(_))) => (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            ),
            408 | 502 | 503 | 504 => (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_hint }),
            ),
            429 => (
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                Cause::Server(ServerCause::RateLimited { retry_hint }),
            ),
            507 => (
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_hint }),
            ),
            _ => (
                AccountErrorKind::Server(ServerErrorKind::Error { status: Some(code) }),
                Cause::Server(ServerCause::Error { status: Some(code) }),
            ),
        },
    };
    StatusClass::new(kind, cause)
}
