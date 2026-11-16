//! Shared HTTP status classification and provider-declared refinements.

use bifrost_types::{
    AccessCause, AuthCause, DiagnosticText, ErrorScope, PrimaryCause, RequestCause, ResourceKind,
    RetryHint, ServerCause, StateCause, ThrottleBudget, ThrottleScope,
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
    pub fn apply_throttle(&self, mut cause: PrimaryCause) -> PrimaryCause {
        if let Some(throttle) = cause.throttle_mut() {
            if let Some(scope) = self.throttle_scope {
                throttle.scope = Some(scope);
            }
            if let Some(budget) = &self.throttle_budget {
                throttle.add_budget(budget.clone());
            }
        }
        cause
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
) -> PrimaryCause {
    let resource = policy
        .resource
        .or_else(|| scope.and_then(ErrorScope::resource));
    let override_class = policy.overrides.iter().find(|(status, _)| *status == code);
    match override_class.map(|(_, class)| class) {
        Some(StatusOverride::ConcurrencyConflict) => {
            PrimaryCause::State(StateCause::ConcurrencyConflict)
        }
        Some(StatusOverride::CursorInvalid) if matches!(scope, Some(ErrorScope::Cursor(_))) => {
            PrimaryCause::State(StateCause::CursorInvalid {
                scope: scope
                    .and_then(|scope| match scope {
                        ErrorScope::Cursor(scope) => Some(scope.clone()),
                        _ => None,
                    })
                    .expect("cursor scope checked"),
            })
        }
        _ => match code {
            400 | 422 => PrimaryCause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!("HTTP {code} malformed request")),
            }),
            401 => PrimaryCause::Auth(AuthCause::ReauthorizationRequired),
            403 => PrimaryCause::Access(AccessCause::PermissionDenied { resource }),
            404 if resource.is_some() => {
                let what = resource.expect("resource checked");
                PrimaryCause::Request(RequestCause::NotFound {
                    what,
                    id: scope.and_then(ErrorScope::object_id).map(str::to_owned),
                })
            }
            409 | 412 => PrimaryCause::State(StateCause::ConcurrencyConflict),
            // An enterprise user with an oversized attachment needs a size
            // refusal so they can reduce it, rather than a client-bug report.
            413 => PrimaryCause::Request(RequestCause::PayloadTooLarge {
                detail: DiagnosticText::support_only("HTTP 413 payload too large"),
            }),
            410 if matches!(scope, Some(ErrorScope::Cursor(_))) => {
                PrimaryCause::State(StateCause::CursorInvalid {
                    scope: scope
                        .and_then(|scope| match scope {
                            ErrorScope::Cursor(scope) => Some(scope.clone()),
                            _ => None,
                        })
                        .expect("cursor scope checked"),
                })
            }
            408 | 502 | 503 | 504 => PrimaryCause::Server(ServerCause::Unavailable { retry_hint }),
            429 => PrimaryCause::Server(ServerCause::RateLimited {
                throttle: Default::default(),
                retry_hint,
            }),
            507 => PrimaryCause::Server(ServerCause::QuotaExhausted {
                throttle: Default::default(),
                retry_hint,
            }),
            _ => PrimaryCause::Server(ServerCause::Error { status: Some(code) }),
        },
    }
}
