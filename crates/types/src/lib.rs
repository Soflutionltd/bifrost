//! `bifrost-types` is the trait crate every other bifrost workspace
//! crate depends on.
//!
//! It defines the `Account` trait protocol crates implement, the
//! sync-engine event vocabulary (`SyncEvent`, `Batch`, `Change`,
//! `Checkpoint`), the capability snapshot the engine reads at open,
//! the unified PIM surface (containers, send, search, drafts,
//! settings, hydration), and the error / recovery taxonomy used to
//! wire fault recovery between protocol crates and the engine.
//!
//! Zero workspace-internal dependencies. The engine, every protocol
//! crate, and `bifrost-net` depend on this crate; this crate depends
//! on nothing else in the workspace.

#![forbid(unsafe_code)]

pub mod account;
pub mod account_compose;
pub mod blob;
pub mod calendar;
pub mod capabilities;
pub mod cloud;
pub mod compose;
pub mod contact;
pub mod container;
pub mod coverage;
pub mod cursor;
pub mod directory;
pub mod error;
pub mod events;
pub mod filter;
pub mod hydration;
pub mod ids;
pub mod mime;
pub mod mutation;
pub mod page;
pub mod repair;
pub mod search;
pub mod settings;

// Account trait + factory + erased return aliases.
pub use account::{
    Account, AccountFactory, AccountFuture, AccountStream, CategoryDefinition,
    MessageReactionState, OpenedAccount, PushSubscription, unsupported_inventory_stream,
};

// Blob types.
pub use blob::{BlobCapabilities, BlobEncoding, BlobHandle, ByteRange, Digest, DigestAlgorithm};

// Capability snapshot + per-method support flags.
pub use capabilities::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CapabilityChange, CapabilityDelta,
    CapabilityKey, CapabilityValue, ConvenienceShape, CursorFreshness, MutationCapabilities,
    MutationConcurrency, MutationReassertion, MutationReplaySafety, PimMethodSupport,
    PushCapability, QuotaSignal, RateLimitClass, StarredFlagShape,
};

// Calendar primitives.
pub use calendar::{
    AttendeeRole, Calendar, CalendarEvent, CalendarId, CalendarProvenance, EventAttendee,
    EventAvailability, EventCreate, EventId, EventOrganizer, EventPatch, EventRange,
    EventRecurrence, EventReminder, EventSearchRequest, EventStatus, EventTime, EventVisibility,
    ReminderRelativeTo, ReminderTrigger, RsvpStatus,
};

// Cloud-storage attachment hosting.
pub use cloud::{CloudUploadMeta, HostedAttachment, ShareScope};

// Server-side filter rules and scripts.
pub use filter::{
    FilterAction, FilterCondition, FilterDiagnostic, FilterDiagnosticSeverity, FilterRule,
    FilterRuleCreate, FilterRulePatch, FilterRuleShape, FilterScript, FilterScriptCreate,
    FilterScriptPatch, FilterValidation, ScriptLanguage, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch,
};

// Mail composition types.
pub use compose::{
    Address, AttachmentHandle, AttachmentInline, DraftHandle, DraftPatch, IdentityId, SendAs,
    SendRequest, validate_scheduled,
};

// Shared RFC 5322 / MIME assembler.
pub use mime::{
    ComposedMessage, DecodedAttachment, DecodedBody, Defect, MimeLimits, MimePart, ParsedMessage,
    PartBody, RenderedMessage, SelectOptions, SubmissionEnvelope, TransferEncoding,
    decode_encoded_words, parse_message, render_rfc5322, select_body, select_body_with,
    send_request_to_rfc5322,
};

// Container / label / mutation-target types.
pub use container::{
    Container, ContainerContentClass, ContainerId, ContainerKind, ContainerList,
    ContainerNamespace, ContainerRights, ContainerStyle, FolderRole, Label, MutationTarget,
    Provenance,
};

// Address book and contact-card primitives.
pub use contact::{
    AddressBook, AddressBookId, ContactAddress, ContactCard, ContactCorpus, ContactCreate,
    ContactEmail, ContactId, ContactOrganization, ContactPatch, ContactPhone, ContactPhoto,
    ContactProvenance, ContactSearchRequest,
};

// Organization-directory (GAL) result type.
pub use directory::DirectoryCard;
// Organization-directory group types.
pub use directory::{DirectoryGroup, DirectoryGroupId, DirectoryGroupKind, DirectoryGroupMember};

// Cursor + scope types.
pub use cursor::{
    CHANGE_CURSOR_ENVELOPE_VERSION, ChangeCursor, ChangeCursorEnvelopeMismatch, CostClass,
    CursorDescriptor, CursorEstablishment, CursorScope, MembershipScope, ObjectType,
    OpaqueChangeState, OpaqueProgressBytes, ProtocolKind, ScopeLifecycle, ScopeLifecycleEvent,
    SyncStrategy,
};

pub use error::partial::{
    Application, Compensation, CompensationExport, CompensationOutcome, OperationPlan,
    PartialCommit, PartialCommitBuilder, PartialCommitExport, PartialCommitTelemetry,
    PlannedDirective, Prerequisites, PrimaryLeg, ReplaySafety, Step, StepEffect, StepExport,
    StepId, StepOutcome, StepReplay, StepTargets, TargetRef, TargetRole,
};

// Error + recovery vocabulary.
pub use error::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuildError, AccountErrorBuilder,
    AccountErrorKind, AccountOperation, AttemptCause, AuthCause, AuthErrorKind, BatchFailure,
    BatchInputInvalidItem, BatchInputInvalidReason, BatchInvariantError, BatchItem, BatchItemId,
    BatchItemOutcome, BatchOutcome, BatchOutcomeBuilder, BatchSuccess, BatchUncertain, Cause,
    CauseChain, DetailVisibility, DiagnosticInfo, DiagnosticText, EngineDirective,
    EnhancedStatusCode, ErrorScope, Fatal, GmailSignal, GraphSignal, ImapResponseCode,
    InternalCause, InternalErrorKind, ItemOutcome, JmapMethod, MailboxUnavailableKind,
    MutationEffect, MutationSuccess, Protocol, ProtocolErrorKind, Provider, ReconcileAction,
    ReconcileAdvice, ReconcileGuidance, ReconcileReason, RecoveryClass, RemediationAction,
    RequestCause, RequestErrorKind, ResourceKind, RetryAdvice, RetryDisposition, RetryHint,
    RetryReason, ServerCause, ServerErrorKind, StateCause, StrategyDowngrade,
    SupportExportConsented, SupportExportInternal, SupportExportMinimal, SyncStateErrorKind,
    TelemetryView, ThrottleBudget, ThrottleKey, ThrottleScope, ThrottleWork, TransmissionState,
    TransportCause, TransportErrorKind, TransportKind, Warning, WarningKind, WireCause,
    validate_batch_input,
};

// Stream-event types.
pub use events::{
    AccountControl, BackfillCheckpoint, BackfillProgress, Batch, BatchBoundaryError, Change,
    Checkpoint, Control, DurableCheckpointSet, HintPayload, InvalidationHint, InvalidationSink,
    InventoryBatch, InventoryCompletion, InventoryEntry, InventoryEvent, InventoryPartition,
    InventoryPartitioning, ObjectChange, ObjectChangeKind, PageBoundary, PageCheckpoint, Partition,
    PartitionEnd, PauseReason, Priority, Progress, PushSource, ScopeChange, ScopeChangeKind,
    SyncEvent, WatchEvent, lift_complete_walk,
};

// Provider-native repair of coverage debt.
pub use repair::{
    DefinitiveIrrelevance, InventoryRepairEvent, InventoryRepairOutcome, InventoryRepairRequest,
    InventoryRepairTarget, RegionRepairProof, RepairAttemptId,
};

// Inventory coverage: what an enumeration proved, and about which region of
// which snapshot. Kept out of `events` because the coverage lattice is a
// domain of its own, not stream plumbing.
pub use coverage::{
    CoverageCoordinate, CoverageDomain, CoverageOutcome, InventoryCoverageReport,
    InventoryObligation, NonEmptyInventoryObligations, ObligationKey, RegionRecovery,
    SnapshotIdentity, TransientReplayHint,
};

// Threading + hydration types.
pub use hydration::{
    AttachmentSource, HydrationProjection, Importance, Message, MessageAttachment, ThreadHydration,
};

// Newtype ids.
pub use ids::{
    AccountId, BlobId, FolderId, LabelId, MailboxId, ObjectId, QueryId, RunId, SubscriptionHandle,
    ThreadId,
};

// Mutation + projection + hydration types.
pub use mutation::{
    Fingerprint, FlagOp, FlagOpValidationError, FlagTargetState, HydratedObject,
    HydratedObjectKind, IdempotencyKey, MailMutationTarget, Projection, ProtocolSalt,
    ServerVersion, canonical_flags_hash, flag_target_state,
};

// Pagination.
pub use page::{Page, SkippedScope};

// Search request AST.
pub use search::{SearchFilter, SearchRequest};

// Settings: identities, vacation, quota.
pub use settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};

/// Compile-time dyn-safety check for `Account`. If this function
/// fails to compile, the trait has gained a non-dyn-safe method
/// signature and the workspace will not link.
#[allow(dead_code)]
fn _dyn_safe(_: &dyn Account) {}
