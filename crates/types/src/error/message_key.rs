use super::kind::{
    AccessErrorKind, AccountErrorKind, AuthErrorKind, InternalErrorKind, ProtocolErrorKind,
    RequestErrorKind, ResourceKind, ServerErrorKind, SyncStateErrorKind, TransportErrorKind,
};

#[must_use]
pub(crate) fn derive(kind: &AccountErrorKind) -> &'static str {
    match kind {
        AccountErrorKind::Transport(TransportErrorKind::Network) => "transport.network",
        AccountErrorKind::Transport(TransportErrorKind::Timeout) => "transport.timeout",
        AccountErrorKind::Transport(TransportErrorKind::Tls) => "transport.tls",
        AccountErrorKind::Authentication(AuthErrorKind::Expired) => "auth.expired",
        AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient) => {
            "auth.refresh-transient"
        }
        AccountErrorKind::Authentication(AuthErrorKind::Revoked) => "auth.revoked",
        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired) => {
            "auth.reauthorization-required"
        }
        AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired) => {
            "authz.admin-consent-required"
        }
        AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked) => {
            "authz.conditional-access-blocked"
        }
        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked) => "authz.policy-blocked",
        AccountErrorKind::Authorization(AccessErrorKind::InsufficientScope) => {
            "authz.insufficient-scope"
        }
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied) => {
            "authz.permission-denied"
        }
        AccountErrorKind::Authorization(AccessErrorKind::AccountDisabled) => {
            "authz.account-disabled"
        }
        AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable { .. }) => {
            "authz.mailbox-unavailable"
        }
        AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed) => {
            "authz.mailbox-not-licensed"
        }
        AccountErrorKind::Server(ServerErrorKind::Unavailable) => "server.unavailable",
        AccountErrorKind::Server(ServerErrorKind::RateLimited) => "server.rate-limited",
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted) => "server.quota-exhausted",
        AccountErrorKind::Server(ServerErrorKind::Error { .. }) => "server.error",
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid) => {
            "syncstate.cursor-invalid"
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure) => {
            "syncstate.strategy-failure"
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeCapabilityLost) => {
            "syncstate.scope-capability-lost"
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible) => {
            "syncstate.schema-incompatible"
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged) => {
            "syncstate.capability-changed"
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::OperatorOverrideNeeded) => {
            "syncstate.operator-override-needed"
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked) => "syncstate.scope-revoked",
        AccountErrorKind::ConcurrencyConflict => "concurrency.conflict",
        AccountErrorKind::Request(RequestErrorKind::Malformed) => "request.malformed",
        AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid) => {
            "request.batch-input-invalid"
        }
        AccountErrorKind::NotFound(ResourceKind::Message) => "notfound.message",
        AccountErrorKind::NotFound(ResourceKind::Mailbox) => "notfound.mailbox",
        AccountErrorKind::NotFound(ResourceKind::Thread) => "notfound.thread",
        AccountErrorKind::NotFound(ResourceKind::Event) => "notfound.event",
        AccountErrorKind::NotFound(ResourceKind::Calendar) => "notfound.calendar",
        AccountErrorKind::NotFound(ResourceKind::Contact) => "notfound.contact",
        AccountErrorKind::NotFound(ResourceKind::Draft) => "notfound.draft",
        AccountErrorKind::NotFound(ResourceKind::Identity) => "notfound.identity",
        AccountErrorKind::NotFound(ResourceKind::Vacation) => "notfound.vacation",
        AccountErrorKind::NotFound(ResourceKind::PushSubscription) => "notfound.push-subscription",
        AccountErrorKind::NotFound(ResourceKind::Account) => "notfound.account",
        AccountErrorKind::NotFound(ResourceKind::Filter) => "notfound.filter",
        AccountErrorKind::Unsupported(_) => "unsupported.operation",
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed) => "protocol.parse-failed",
        AccountErrorKind::Protocol(ProtocolErrorKind::MissingField) => "protocol.missing-field",
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation) => {
            "protocol.contract-violation"
        }
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse) => {
            "protocol.partial-response"
        }
        AccountErrorKind::Protocol(ProtocolErrorKind::Unknown) => "protocol.unknown",
        AccountErrorKind::Internal(InternalErrorKind::InvariantViolated) => {
            "internal.invariant-violated"
        }
        AccountErrorKind::Internal(InternalErrorKind::RuntimeFailure) => "internal.runtime-failure",
        AccountErrorKind::Internal(InternalErrorKind::Panicked) => "internal.panicked",
        AccountErrorKind::Internal(InternalErrorKind::AccountContract) => {
            "internal.account-contract"
        }
        AccountErrorKind::Internal(InternalErrorKind::LimitExceeded) => "internal.limit-exceeded",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        AccountOperation, MailboxUnavailableKind, ProtocolErrorKind, RequestErrorKind,
    };

    #[test]
    fn documented_message_keys_are_derived() {
        let cases = [
            (
                AccountErrorKind::Transport(TransportErrorKind::Network),
                "transport.network",
            ),
            (
                AccountErrorKind::Transport(TransportErrorKind::Timeout),
                "transport.timeout",
            ),
            (
                AccountErrorKind::Transport(TransportErrorKind::Tls),
                "transport.tls",
            ),
            (
                AccountErrorKind::Authentication(AuthErrorKind::Expired),
                "auth.expired",
            ),
            (
                AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient),
                "auth.refresh-transient",
            ),
            (
                AccountErrorKind::Authentication(AuthErrorKind::Revoked),
                "auth.revoked",
            ),
            (
                AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
                "auth.reauthorization-required",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired),
                "authz.admin-consent-required",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked),
                "authz.conditional-access-blocked",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
                "authz.policy-blocked",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::InsufficientScope),
                "authz.insufficient-scope",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                "authz.permission-denied",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::AccountDisabled),
                "authz.account-disabled",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                    kind: MailboxUnavailableKind::Transient,
                }),
                "authz.mailbox-unavailable",
            ),
            (
                AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed),
                "authz.mailbox-not-licensed",
            ),
            (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                "server.unavailable",
            ),
            (
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                "server.rate-limited",
            ),
            (
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                "server.quota-exhausted",
            ),
            (
                AccountErrorKind::Server(ServerErrorKind::Error { status: Some(500) }),
                "server.error",
            ),
            (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                "syncstate.cursor-invalid",
            ),
            (
                AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure),
                "syncstate.strategy-failure",
            ),
            (
                AccountErrorKind::SyncState(SyncStateErrorKind::ScopeCapabilityLost),
                "syncstate.scope-capability-lost",
            ),
            (
                AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
                "syncstate.schema-incompatible",
            ),
            (
                AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
                "syncstate.capability-changed",
            ),
            (
                AccountErrorKind::SyncState(SyncStateErrorKind::OperatorOverrideNeeded),
                "syncstate.operator-override-needed",
            ),
            (
                AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
                "syncstate.scope-revoked",
            ),
            (
                AccountErrorKind::ConcurrencyConflict,
                "concurrency.conflict",
            ),
            (
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                "request.malformed",
            ),
            (
                AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid),
                "request.batch-input-invalid",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Message),
                "notfound.message",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Mailbox),
                "notfound.mailbox",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Thread),
                "notfound.thread",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Calendar),
                "notfound.calendar",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Contact),
                "notfound.contact",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Draft),
                "notfound.draft",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Identity),
                "notfound.identity",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Vacation),
                "notfound.vacation",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::PushSubscription),
                "notfound.push-subscription",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Account),
                "notfound.account",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Event),
                "notfound.event",
            ),
            (
                AccountErrorKind::NotFound(ResourceKind::Filter),
                "notfound.filter",
            ),
            (
                AccountErrorKind::Unsupported(AccountOperation::Search),
                "unsupported.operation",
            ),
            (
                AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
                "protocol.parse-failed",
            ),
            (
                AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
                "protocol.missing-field",
            ),
            (
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                "protocol.contract-violation",
            ),
            (
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
                "protocol.partial-response",
            ),
            (
                AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
                "protocol.unknown",
            ),
            (
                AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
                "internal.invariant-violated",
            ),
            (
                AccountErrorKind::Internal(InternalErrorKind::RuntimeFailure),
                "internal.runtime-failure",
            ),
            (
                AccountErrorKind::Internal(InternalErrorKind::Panicked),
                "internal.panicked",
            ),
            (
                AccountErrorKind::Internal(InternalErrorKind::AccountContract),
                "internal.account-contract",
            ),
            (
                AccountErrorKind::Internal(InternalErrorKind::LimitExceeded),
                "internal.limit-exceeded",
            ),
        ];

        for (kind, key) in cases {
            assert_eq!(derive(&kind), key);
        }
    }
}
