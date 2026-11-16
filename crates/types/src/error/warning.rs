use super::diagnostic::DiagnosticText;
use super::{AccountError, ErrorScope, OperatorOverrideReason, StrategyDowngrade};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Warning {
    pub kind: WarningKind,
    pub message: DiagnosticText,
    pub next_action: Option<DiagnosticText>,
    pub protocol_detail: Option<DiagnosticText>,
    pub retry_count: u32,
    /// Advisory evidence, never an instruction to retry the successful parent
    /// operation. A skipped share must retain its grant/transport distinction.
    pub error: Option<AccountError>,
    pub scope: Option<ErrorScope>,
    pub strategy_downgrade: Option<StrategyDowngrade>,
}

impl Warning {
    #[must_use]
    pub fn new(kind: WarningKind, message: DiagnosticText) -> Self {
        Self {
            kind,
            message,
            next_action: None,
            protocol_detail: None,
            retry_count: 0,
            error: None,
            scope: None,
            strategy_downgrade: None,
        }
    }

    #[must_use]
    pub fn user_safe(kind: WarningKind, message: impl Into<String>) -> Self {
        Self::new(kind, DiagnosticText::user_safe(message))
    }

    #[must_use]
    pub fn support_only(kind: WarningKind, message: impl Into<String>) -> Self {
        Self::new(kind, DiagnosticText::support_only(message))
    }

    #[must_use]
    pub fn with_next_action(mut self, next_action: DiagnosticText) -> Self {
        self.next_action = Some(next_action);
        self
    }

    #[must_use]
    pub fn with_protocol_detail(mut self, protocol_detail: DiagnosticText) -> Self {
        self.protocol_detail = Some(protocol_detail);
        self
    }

    #[must_use]
    pub fn with_retry_count(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }

    #[must_use]
    pub fn with_error(mut self, error: AccountError) -> Self {
        if self.scope.is_none() {
            self.scope = error.scope().cloned();
        }
        self.error = Some(error);
        self
    }

    #[must_use]
    pub fn with_optional_error(self, error: Option<AccountError>) -> Self {
        match error {
            Some(error) => self.with_error(error),
            None => self,
        }
    }

    #[must_use]
    pub fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub fn with_strategy_downgrade(mut self, downgrade: StrategyDowngrade) -> Self {
        self.strategy_downgrade = Some(downgrade);
        self
    }

    /// Discovery's advisory skip lane, using the same evidence as page/open
    /// skips. Primary discovery can finish while a delegate is unavailable.
    #[must_use]
    pub fn skipped(scope: Option<ErrorScope>, error: AccountError) -> Self {
        let mut warning = Self::support_only(WarningKind::ScopeSkipped, "scope discovery skipped")
            .with_error(error);
        if let Some(scope) = scope {
            warning.scope = Some(scope);
        }
        warning
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WarningKind {
    StrategyDowngraded,
    ScopeSkipped,
    UidCountMismatch {
        expected: u32,
        actual: usize,
    },
    NotifyRefused,
    Engine(EngineNotice),
    OperatorAttentionNeeded,
    Throttled,
    ClockSkew,
    BlobNotByteStream,
    ReadbackSkipped,
    /// The change-stream broadcast ring overflowed and batches were lost;
    /// the consumer must reconcile from its last durable checkpoint.
    ///
    /// Structural on purpose: a consumer's whole recovery path (detach,
    /// re-drive, reconcile) hangs on telling this apart from the other
    /// account-scoped warnings, and before this variant existed the only
    /// discriminator was the message-text prefix - a contract no one had
    /// signed. Minted exclusively by the multiplexer's `ChangesReceiver`
    /// when it converts `RecvError::Lagged` into an observable event.
    ChangeStreamLagged,
    /// An inventory walk ended without the `Done` or `Terminated` every walk
    /// must end in, so nothing proved how far the enumeration got, and the walk
    /// is not treated as complete: backfill withholds the scope's completion
    /// marker, and the inventory fusion walk establishes no cursor. Pages
    /// acknowledged earlier in the walk still stand. A producer defect, not
    /// something the user can fix, but the user sees a scope that never
    /// finishes and this says why. Minted by both engine inventory front ends.
    InventoryEndedUnannounced,
    Other,
}

/// Branchable engine advisories. Counts describe this observation, not a
/// durable total. The enclosing warning carries scope and classified evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EngineNotice {
    RecoveryWaitBudgetExhausted,
    EstablishRetryBudgetExhausted,
    SchemaRecoveryFailed,
    ScopeDisabled,
    ScopeCapabilityDowngraded,
    OperatorOverrideRequired(OperatorOverrideReason),
    BarrierStop {
        regions: usize,
    },
    DegradedCoverage {
        obligations: usize,
    },
    WalkSuperseded,
    PushDisconnected {
        lane: Option<crate::PushLane>,
    },
    PushRetrying,
    DedupeByClientId,
    IncompletePage {
        skipped_scopes: usize,
        failed_ids: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountErrorBuilder, AccountOperation, CursorScope, FolderId, PrimaryCause, ServerCause,
    };

    #[test]
    fn skipped_discovery_retains_classification_and_explicit_scope() {
        let failure_scope = ErrorScope::Cursor(CursorScope::Folder(FolderId("calendars".into())));
        let scope = ErrorScope::Account;
        let error = AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::Unavailable {
            retry_hint: None,
        }))
        .scope(failure_scope)
        .operation(AccountOperation::DiscoverCursorScopes)
        .try_build()
        .expect("classified failure");
        let warning = Warning::skipped(Some(scope.clone()), error.clone());
        assert_eq!(warning.kind, WarningKind::ScopeSkipped);
        assert_eq!(warning.scope, Some(scope));
        let retained = warning.error.expect("originating error");
        assert_eq!(retained.kind(), error.kind());
        assert_eq!(retained.recovery(), error.recovery());
        assert_eq!(retained.scope(), error.scope());
        assert_eq!(retained.operation(), error.operation());
        assert_eq!(
            retained.suggested_remediation(),
            error.suggested_remediation()
        );
    }
}
