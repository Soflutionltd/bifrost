pub mod account_error;
pub mod batch;
pub mod builder;
pub mod cause;
pub mod diagnostic;
pub mod kind;
pub mod message_key;
pub mod partial;
pub mod recovery;
pub mod scope;
pub mod stream;
pub mod warning;

pub use account_error::{AccountError, HINTLESS_HOLD_WAIT, HoldTarget};
pub use batch::{
    BatchFailure, BatchInvariantError, BatchItem, BatchItemId, BatchItemOutcome, BatchOutcome,
    BatchOutcomeBuilder, BatchSuccess, BatchUncertain, SubmissionIndex, validate_batch_input,
};
pub use builder::{AccountErrorBuildError, AccountErrorBuilder};
pub use cause::{
    AccessCause, AnswerEvidence, AttemptCause, AuthCause, BatchInputInvalidItem,
    BatchInputInvalidReason, Cause, CauseChain, EnhancedStatusCode, GmailSignal, GraphSignal,
    ImapResponseCode, InternalCause, JmapMethod, PrimaryCause, RequestCause, ServerCause,
    StateCause, Throttle, TransmissionState, TransportCause, WireCause,
};
pub use diagnostic::{
    DetailVisibility, DiagnosticInfo, DiagnosticText, SupportExportConsented,
    SupportExportInternal, SupportExportMinimal, TelemetryToken, TelemetryView,
};
pub use kind::{
    AccessErrorKind, AccountErrorKind, AuthErrorKind, InternalErrorKind, MailboxUnavailableKind,
    ProtocolErrorKind, RequestErrorKind, ResourceKind, ServerErrorKind, SyncStateErrorKind,
    TransportErrorKind, UnsupportedReason,
};
pub use recovery::{
    EngineDirective, EngineDirectiveIdentity, Fatal, OperatorOverrideReason, ReconcileAction,
    ReconcileAdvice, ReconcileGuidance, ReconcileReason, RecoveryClass, RemediationAction,
    RetryAdvice, RetryDisposition, RetryHint, RetryReason, StrategyDowngrade, ThrottleBudget,
    ThrottleKey, ThrottleScope, ThrottleWork,
};
pub use scope::{AccountOperation, ErrorScope, Protocol, Provider};
pub use stream::{ItemOutcome, MutationEffect, MutationSuccess};
pub use warning::{EngineNotice, Warning, WarningKind};

pub use partial::{
    Application, Compensation, CompensationExport, CompensationOutcome, OperationPlan,
    PartialCommit, PartialCommitBuilder, PartialCommitExport, PartialCommitTelemetry,
    PlannedDirective, Prerequisites, PrimaryLeg, ReplaySafety, Step, StepEffect, StepExport,
    StepId, StepOutcome, StepReplay, StepTargets, TargetRef, TargetRole,
};
