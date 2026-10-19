use std::error::Error as StdError;
use std::fmt;

use serde::Serialize;

use crate::capabilities::CapabilityDelta;

use super::batch::BatchItemId;
use super::diagnostic::DiagnosticText;
use super::kind::{InternalErrorKind, MailboxUnavailableKind, ResourceKind, TransportErrorKind};
use super::recovery::{RetryHint, StrategyDowngrade};
use super::scope::{AccountOperation, Protocol};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CauseChain {
    causes: Vec<Cause>,
}

impl CauseChain {
    pub(crate) fn try_new(causes: Vec<Cause>) -> Option<Self> {
        (!causes.is_empty()).then_some(Self { causes })
    }

    #[cfg(test)]
    pub(crate) fn new(causes: Vec<Cause>) -> Self {
        Self::try_new(causes).expect("test cause chain must be non-empty")
    }

    #[must_use]
    pub fn outermost(&self) -> &Cause {
        &self.causes[0]
    }

    #[must_use]
    pub fn root(&self) -> &Cause {
        self.causes
            .last()
            .expect("CauseChain is non-empty by construction")
    }

    pub fn iter(&self) -> impl Iterator<Item = &Cause> {
        self.causes.iter()
    }

    /// Consume the chain and return the underlying ordered `Cause` vector.
    /// Used by `AccountError::into_builder` to round-trip a built error
    /// back through `AccountErrorBuilder` for decoration.
    #[must_use]
    pub(crate) fn into_vec(self) -> Vec<Cause> {
        self.causes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Cause {
    Transport(TransportCause),
    Attempt(AttemptCause),
    Auth(AuthCause),
    Access(AccessCause),
    Server(ServerCause),
    State(StateCause),
    Request(RequestCause),
    Wire(WireCause),
    /// A client-side failure; the cause behind `AccountErrorKind::Internal`.
    Internal(InternalCause),
}

impl Cause {
    #[must_use]
    pub(crate) fn transmission_state(&self) -> Option<TransmissionState> {
        match self {
            Self::Attempt(cause) => Some(cause.transmission_state),
            _ => None,
        }
    }

    #[must_use]
    pub(crate) fn summary(&self) -> CauseSummary<'_> {
        match self {
            Self::Transport(cause) => CauseSummary {
                kind: "transport",
                detail: cause.message.as_ref().map(DiagnosticText::as_str),
                transmission_state: None,
                status: None,
                native_code: None,
                transport_kind: Some(cause.kind),
                ..CauseSummary::empty("transport")
            },
            Self::Attempt(cause) => CauseSummary {
                kind: "attempt",
                detail: None,
                transmission_state: Some(cause.transmission_state),
                status: None,
                native_code: None,
                ..CauseSummary::empty("attempt")
            },
            Self::Auth(cause) => CauseSummary {
                kind: "auth",
                detail: None,
                transmission_state: None,
                status: None,
                native_code: cause.native_code(),
                ..CauseSummary::empty("auth")
            },
            Self::Access(cause) => CauseSummary {
                kind: "access",
                detail: None,
                transmission_state: None,
                status: None,
                native_code: cause.native_code(),
                resource: match cause {
                    AccessCause::PermissionDenied { resource } => *resource,
                    _ => None,
                },
                needed: match cause {
                    AccessCause::InsufficientScope { needed }
                    | AccessCause::AdminConsentRequired { needed } => Some(*needed),
                    _ => None,
                },
                ..CauseSummary::empty("access")
            },
            Self::Server(cause) => CauseSummary {
                kind: "server",
                detail: None,
                transmission_state: None,
                status: cause.status(),
                native_code: None,
                ..CauseSummary::empty("server")
            },
            Self::State(cause) => CauseSummary {
                kind: "state",
                detail: cause.detail(),
                transmission_state: None,
                status: None,
                native_code: None,
                downgrade: match cause {
                    StateCause::StrategyFailure { downgrade } => Some(*downgrade),
                    _ => None,
                },
                capability_delta: match cause {
                    StateCause::CapabilityChanged { delta } => delta.as_ref(),
                    _ => None,
                },
                ..CauseSummary::empty("state")
            },
            Self::Request(cause) => CauseSummary {
                kind: "request",
                detail: cause.detail(),
                transmission_state: None,
                status: None,
                native_code: None,
                resource: match cause {
                    RequestCause::NotFound { what, .. } => Some(*what),
                    _ => None,
                },
                batch_items: match cause {
                    RequestCause::BatchInputInvalid { items } => Some(items.as_slice()),
                    _ => None,
                },
                operation: match cause {
                    RequestCause::Unsupported { operation } => Some(*operation),
                    _ => None,
                },
                field: match cause {
                    RequestCause::InvalidArgument { field, .. } => *field,
                    _ => None,
                },
                ..CauseSummary::empty("request")
            },
            Self::Wire(cause) => CauseSummary {
                kind: "wire",
                detail: cause.detail(),
                transmission_state: None,
                status: cause.status(),
                native_code: cause.native_code(),
                ..CauseSummary::empty("wire")
            },
            Self::Internal(cause) => CauseSummary {
                kind: "internal",
                detail: cause.detail.as_ref().map(DiagnosticText::as_str),
                ..CauseSummary::empty("internal")
            },
        }
    }
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(cause) => write!(f, "{cause}"),
            Self::Attempt(cause) => write!(f, "{cause}"),
            Self::Auth(cause) => write!(f, "{cause}"),
            Self::Access(cause) => write!(f, "{cause}"),
            Self::Server(cause) => write!(f, "{cause}"),
            Self::State(cause) => write!(f, "{cause}"),
            Self::Request(cause) => write!(f, "{cause}"),
            Self::Wire(cause) => write!(f, "{cause}"),
            Self::Internal(cause) => write!(f, "{cause}"),
        }
    }
}

impl StdError for Cause {}

/// The cause behind an `AccountErrorKind::Internal`: which client-side
/// failure it was, and support-only detail naming the site.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct InternalCause {
    pub kind: InternalErrorKind,
    pub detail: Option<DiagnosticText>,
}

impl InternalCause {
    #[must_use]
    pub fn new(kind: InternalErrorKind, detail: Option<DiagnosticText>) -> Self {
        Self { kind, detail }
    }
}

impl fmt::Display for InternalCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.detail {
            Some(detail) => write!(f, "internal {:?}: {}", self.kind, detail.as_str()),
            None => write!(f, "internal {:?}", self.kind),
        }
    }
}

impl StdError for InternalCause {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TransportCause {
    pub kind: TransportKind,
    pub message: Option<DiagnosticText>,
}

impl TransportCause {
    #[must_use]
    pub fn new(kind: TransportKind, message: Option<DiagnosticText>) -> Self {
        Self { kind, message }
    }

    #[must_use]
    pub(crate) fn error_kind(&self) -> TransportErrorKind {
        match self.kind {
            TransportKind::Network => TransportErrorKind::Network,
            TransportKind::Timeout => TransportErrorKind::Timeout,
            TransportKind::Tls => TransportErrorKind::Tls,
        }
    }
}

impl fmt::Display for TransportCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(message) => write!(f, "transport {:?}: {}", self.kind, message.as_str()),
            None => write!(f, "transport {:?}", self.kind),
        }
    }
}

impl StdError for TransportCause {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum TransportKind {
    Network,
    Timeout,
    Tls,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AttemptCause {
    pub transmission_state: TransmissionState,
}

impl AttemptCause {
    #[must_use]
    pub fn new(transmission_state: TransmissionState) -> Self {
        Self { transmission_state }
    }
}

impl fmt::Display for AttemptCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "attempt {:?}", self.transmission_state)
    }
}

impl StdError for AttemptCause {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum TransmissionState {
    Unsent,
    InFlight,
    Acknowledged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AuthCause {
    Expired,
    RefreshTransient,
    Revoked,
    ReauthorizationRequired,
}

impl AuthCause {
    fn native_code(&self) -> Option<&'static str> {
        match self {
            Self::Expired => Some("expired"),
            Self::RefreshTransient => Some("refresh_transient"),
            Self::Revoked => Some("revoked"),
            Self::ReauthorizationRequired => Some("reauthorization_required"),
        }
    }
}

impl fmt::Display for AuthCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "auth {self:?}")
    }
}

impl StdError for AuthCause {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AccessCause {
    InsufficientScope { needed: &'static str },
    AdminConsentRequired { needed: &'static str },
    ConditionalAccessBlocked,
    PolicyBlocked,
    AccountDisabled,
    MailboxUnavailable { kind: MailboxUnavailableKind },
    MailboxNotLicensed,
    PermissionDenied { resource: Option<ResourceKind> },
}

impl AccessCause {
    fn native_code(&self) -> Option<&'static str> {
        match self {
            Self::InsufficientScope { .. } => Some("insufficient_scope"),
            Self::AdminConsentRequired { .. } => Some("admin_consent_required"),
            Self::ConditionalAccessBlocked => Some("conditional_access_blocked"),
            Self::PolicyBlocked => Some("policy_blocked"),
            Self::AccountDisabled => Some("account_disabled"),
            Self::MailboxUnavailable { .. } => Some("mailbox_unavailable"),
            Self::MailboxNotLicensed => Some("mailbox_not_licensed"),
            Self::PermissionDenied { .. } => Some("permission_denied"),
        }
    }
}

impl fmt::Display for AccessCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "access {self:?}")
    }
}

impl StdError for AccessCause {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServerCause {
    Unavailable { retry_hint: Option<RetryHint> },
    RateLimited { retry_hint: Option<RetryHint> },
    QuotaExhausted { retry_hint: Option<RetryHint> },
    Error { status: Option<u16> },
}

impl ServerCause {
    #[must_use]
    pub(crate) fn retry_hint(&self) -> Option<RetryHint> {
        match self {
            Self::Unavailable { retry_hint }
            | Self::RateLimited { retry_hint }
            | Self::QuotaExhausted { retry_hint } => *retry_hint,
            Self::Error { .. } => None,
        }
    }

    #[must_use]
    pub(crate) fn status(&self) -> Option<u16> {
        match self {
            Self::Error { status } => *status,
            Self::Unavailable { .. } | Self::RateLimited { .. } | Self::QuotaExhausted { .. } => {
                None
            }
        }
    }
}

impl fmt::Display for ServerCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "server {self:?}")
    }
}

impl StdError for ServerCause {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StateCause {
    CursorInvalid,
    StrategyFailure {
        downgrade: StrategyDowngrade,
    },
    ScopeCapabilityLost,
    SchemaIncompatible,
    /// Capability shift evidence. The optional delta payload is for
    /// forensic exports only; producers that cannot compute a real
    /// delta pass `None`. Recovery routes any capability shift to
    /// `EngineDirective::RestartAccount` regardless of the payload.
    CapabilityChanged {
        delta: Option<CapabilityDelta>,
    },
    OperatorOverrideNeeded {
        reason: String,
    },
    ConcurrencyConflict,
    /// Access to one cursor scope was revoked mid-sync. Pairs with
    /// `SyncStateErrorKind::ScopeRevoked`.
    ScopeRevoked,
}

impl StateCause {
    fn detail(&self) -> Option<&str> {
        match self {
            Self::OperatorOverrideNeeded { reason } => Some(reason.as_str()),
            Self::CursorInvalid
            | Self::StrategyFailure { .. }
            | Self::ScopeCapabilityLost
            | Self::SchemaIncompatible
            | Self::CapabilityChanged { .. }
            | Self::ConcurrencyConflict
            | Self::ScopeRevoked => None,
        }
    }
}

impl fmt::Display for StateCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "state {self:?}")
    }
}

impl StdError for StateCause {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RequestCause {
    Malformed {
        detail: DiagnosticText,
    },
    BatchInputInvalid {
        items: Vec<BatchInputInvalidItem>,
    },
    /// The batch itself was empty, so there is no item id to report.
    BatchInputEmpty,
    Unsupported {
        operation: AccountOperation,
    },
    /// Deliberately has NO distinct `AccountErrorKind`:
    /// `kind_matches_cause` maps it onto `Request(Malformed)`, and
    /// message-key / recovery treat the two identically. It exists as a
    /// richer cause payload (the offending field, a message) for
    /// diagnostics, not as a separate classification - do not expect a
    /// 1:1 cause-to-kind correspondence here.
    InvalidArgument {
        field: Option<&'static str>,
        message: Option<DiagnosticText>,
    },
    NotFound {
        what: ResourceKind,
        id: Option<String>,
    },
}

impl RequestCause {
    fn detail(&self) -> Option<&str> {
        match self {
            Self::Malformed { detail } => Some(detail.as_str()),
            Self::InvalidArgument {
                message: Some(message),
                ..
            } => Some(message.as_str()),
            Self::NotFound { id: Some(id), .. } => Some(id.as_str()),
            Self::BatchInputInvalid { .. }
            | Self::BatchInputEmpty
            | Self::Unsupported { .. }
            | Self::InvalidArgument { message: None, .. }
            | Self::NotFound { id: None, .. } => None,
        }
    }
}

impl fmt::Display for RequestCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request {self:?}")
    }
}

impl StdError for RequestCause {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WireCause {
    Graph(GraphSignal),
    Jmap(JmapMethod),
    Imap(ImapResponseCode),
    Smtp(EnhancedStatusCode),
    Gmail(GmailSignal),
    MalformedResponse {
        protocol: Protocol,
        detail: Option<DiagnosticText>,
    },
}

impl WireCause {
    fn detail(&self) -> Option<&str> {
        match self {
            Self::Graph(GraphSignal::Unknown { code })
            | Self::Jmap(JmapMethod::Unknown { code })
            | Self::Gmail(GmailSignal::Unknown { code }) => Some(code.as_str()),
            Self::Imap(ImapResponseCode::Unknown {
                value: Some(value), ..
            }) => Some(value.as_str()),
            Self::Smtp(code) => code.text.as_ref().map(DiagnosticText::as_str),
            Self::MalformedResponse {
                detail: Some(detail),
                ..
            } => Some(detail.as_str()),
            Self::Graph(_)
            | Self::Jmap(_)
            | Self::Imap(_)
            | Self::Gmail(_)
            | Self::MalformedResponse { detail: None, .. } => None,
        }
    }

    fn native_code(&self) -> Option<&str> {
        match self {
            Self::Graph(signal) => Some(signal.code()),
            Self::Jmap(method) => Some(method.code()),
            Self::Imap(code) => Some(code.code()),
            Self::Smtp(code) => code.enhanced.as_ref().map(DiagnosticText::as_str),
            Self::Gmail(signal) => Some(signal.code()),
            Self::MalformedResponse { .. } => None,
        }
    }

    fn status(&self) -> Option<u16> {
        match self {
            Self::Smtp(code) => Some(code.reply),
            Self::Graph(_)
            | Self::Jmap(_)
            | Self::Imap(_)
            | Self::Gmail(_)
            | Self::MalformedResponse { .. } => None,
        }
    }
}

impl fmt::Display for WireCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "wire {self:?}")
    }
}

impl StdError for WireCause {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphSignal {
    Gone,
    InvalidAuthenticationToken,
    AccessDenied,
    Forbidden,
    AccessRestricted,
    ConditionalAccessBlocked,
    AdminConsentRequired,
    MailboxNotEnabledForRestApi,
    MailboxStoreUnavailable,
    ResyncRequired,
    InvalidDeltaToken,
    SyncStateNotFound,
    TooManyRequests,
    GenericFileError,
    PreconditionFailed,
    NotFound,
    Unknown { code: String },
}

impl GraphSignal {
    fn code(&self) -> &str {
        match self {
            Self::Gone => "Gone",
            Self::InvalidAuthenticationToken => "InvalidAuthenticationToken",
            Self::AccessDenied => "AccessDenied",
            Self::Forbidden => "Forbidden",
            Self::AccessRestricted => "AccessRestricted",
            Self::ConditionalAccessBlocked => "ConditionalAccessBlocked",
            Self::AdminConsentRequired => "AdminConsentRequired",
            Self::MailboxNotEnabledForRestApi => "MailboxNotEnabledForRESTAPI",
            Self::MailboxStoreUnavailable => "MailboxStoreUnavailable",
            Self::ResyncRequired => "ResyncRequired",
            Self::InvalidDeltaToken => "InvalidDeltaToken",
            Self::SyncStateNotFound => "SyncStateNotFound",
            Self::TooManyRequests => "TooManyRequests",
            Self::GenericFileError => "GenericFileError",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotFound => "NotFound",
            Self::Unknown { code } => code.as_str(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JmapMethod {
    // Method-level errors (RFC 8620 §3.6.2).
    ServerUnavailable,
    ServerFail,
    ServerPartialFail,
    UnknownMethod,
    InvalidArguments,
    InvalidResultReference,
    Forbidden,
    AccountNotFound,
    AccountNotSupportedByMethod,
    AccountReadOnly,
    RequestTooLarge,
    CannotCalculateChanges,
    StateMismatch,
    AlreadyExists,
    FromAccountNotFound,
    FromAccountNotSupportedByMethod,
    AnchorNotFound,
    UnsupportedSort,
    UnsupportedFilter,
    TooManyChanges,
    UnknownCapability,
    NotJson,
    NotRequest,
    Limit,
    // Set-error vocabulary (RFC 8620 §5.3 / RFC 8621). `Forbidden` and
    // `AlreadyExists` above are shared with the set-error vocabulary;
    // the remaining set-error codes are listed here. Protocol crates
    // pick the variant by wire code, not by JMAP method-vs-set context.
    OverQuota,
    TooLarge,
    RateLimit,
    NotFound,
    InvalidPatch,
    WillDestroy,
    Singleton,
    MailboxHasChild,
    MailboxHasEmail,
    BlobNotFound,
    TooManyKeywords,
    TooManyMailboxes,
    ForbiddenFrom,
    InvalidEmail,
    TooManyRecipients,
    NoRecipients,
    InvalidRecipients,
    ForbiddenMailFrom,
    ForbiddenToSend,
    CannotUnsend,
    InvalidScript,
    ScriptIsActive,
    InvalidProperties,
    Unknown { code: String },
}

impl JmapMethod {
    fn code(&self) -> &str {
        match self {
            Self::ServerUnavailable => "serverUnavailable",
            Self::ServerFail => "serverFail",
            Self::ServerPartialFail => "serverPartialFail",
            Self::UnknownMethod => "unknownMethod",
            Self::InvalidArguments => "invalidArguments",
            Self::InvalidResultReference => "invalidResultReference",
            Self::Forbidden => "forbidden",
            Self::AccountNotFound => "accountNotFound",
            Self::AccountNotSupportedByMethod => "accountNotSupportedByMethod",
            Self::AccountReadOnly => "accountReadOnly",
            Self::RequestTooLarge => "requestTooLarge",
            Self::CannotCalculateChanges => "cannotCalculateChanges",
            Self::StateMismatch => "stateMismatch",
            Self::AlreadyExists => "alreadyExists",
            Self::FromAccountNotFound => "fromAccountNotFound",
            Self::FromAccountNotSupportedByMethod => "fromAccountNotSupportedByMethod",
            Self::AnchorNotFound => "anchorNotFound",
            Self::UnsupportedSort => "unsupportedSort",
            Self::UnsupportedFilter => "unsupportedFilter",
            Self::TooManyChanges => "tooManyChanges",
            Self::UnknownCapability => "unknownCapability",
            Self::NotJson => "notJSON",
            Self::NotRequest => "notRequest",
            Self::Limit => "limit",
            Self::OverQuota => "overQuota",
            Self::TooLarge => "tooLarge",
            Self::RateLimit => "rateLimit",
            Self::NotFound => "notFound",
            Self::InvalidPatch => "invalidPatch",
            Self::WillDestroy => "willDestroy",
            Self::Singleton => "singleton",
            Self::MailboxHasChild => "mailboxHasChild",
            Self::MailboxHasEmail => "mailboxHasEmail",
            Self::BlobNotFound => "blobNotFound",
            Self::TooManyKeywords => "tooManyKeywords",
            Self::TooManyMailboxes => "tooManyMailboxes",
            Self::ForbiddenFrom => "forbiddenFrom",
            Self::InvalidEmail => "invalidEmail",
            Self::TooManyRecipients => "tooManyRecipients",
            Self::NoRecipients => "noRecipients",
            Self::InvalidRecipients => "invalidRecipients",
            Self::ForbiddenMailFrom => "forbiddenMailFrom",
            Self::ForbiddenToSend => "forbiddenToSend",
            Self::CannotUnsend => "cannotUnsend",
            Self::InvalidScript => "invalidScript",
            Self::ScriptIsActive => "scriptIsActive",
            Self::InvalidProperties => "invalidProperties",
            Self::Unknown { code } => code.as_str(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ImapResponseCode {
    Alert,
    BadCharset,
    Capability,
    Parse,
    PermanentFlags,
    ReadOnly,
    ReadWrite,
    TryCreate,
    UidNext,
    UidValidity,
    Unseen,
    AppendUid,
    CopyUid,
    HighestModSeq,
    Modified,
    NoModSeq,
    Closed,
    MailboxId,
    Unavailable,
    AuthenticationFailed,
    AuthorizationFailed,
    Expired,
    PrivacyRequired,
    ContactAdmin,
    NoPerm,
    InUse,
    ExpungeIssued,
    Corruption,
    ServerBug,
    ClientBug,
    Cannot,
    Limit,
    OverQuota,
    AlreadyExists,
    NonExistent,
    NewName,
    Referral,
    UrlMech,
    BadUrl,
    BadComparator,
    Annotate,
    Annotations,
    TempFail,
    MaxConvertMessages,
    MaxConvertParts,
    NoUpdate,
    NotificationOverflow,
    BadEvent,
    UndefinedFilter,
    UidNotSticky,
    NotSaved,
    HasChildren,
    UnknownCte,
    TooBig,
    CompressionActive,
    UseAttr,
    MetadataLongEntries,
    MetadataMaxSize,
    MetadataTooMany,
    MetadataNoPrivate,
    Unknown {
        code: String,
        value: Option<DiagnosticText>,
    },
}

impl ImapResponseCode {
    fn code(&self) -> &str {
        match self {
            Self::Alert => "ALERT",
            Self::BadCharset => "BADCHARSET",
            Self::Capability => "CAPABILITY",
            Self::Parse => "PARSE",
            Self::PermanentFlags => "PERMANENTFLAGS",
            Self::ReadOnly => "READ-ONLY",
            Self::ReadWrite => "READ-WRITE",
            Self::TryCreate => "TRYCREATE",
            Self::UidNext => "UIDNEXT",
            Self::UidValidity => "UIDVALIDITY",
            Self::Unseen => "UNSEEN",
            Self::AppendUid => "APPENDUID",
            Self::CopyUid => "COPYUID",
            Self::HighestModSeq => "HIGHESTMODSEQ",
            Self::Modified => "MODIFIED",
            Self::NoModSeq => "NOMODSEQ",
            Self::Closed => "CLOSED",
            Self::MailboxId => "MAILBOXID",
            Self::Unavailable => "UNAVAILABLE",
            Self::AuthenticationFailed => "AUTHENTICATIONFAILED",
            Self::AuthorizationFailed => "AUTHORIZATIONFAILED",
            Self::Expired => "EXPIRED",
            Self::PrivacyRequired => "PRIVACYREQUIRED",
            Self::ContactAdmin => "CONTACTADMIN",
            Self::NoPerm => "NOPERM",
            Self::InUse => "INUSE",
            Self::ExpungeIssued => "EXPUNGEISSUED",
            Self::Corruption => "CORRUPTION",
            Self::ServerBug => "SERVERBUG",
            Self::ClientBug => "CLIENTBUG",
            Self::Cannot => "CANNOT",
            Self::Limit => "LIMIT",
            Self::OverQuota => "OVERQUOTA",
            Self::AlreadyExists => "ALREADYEXISTS",
            Self::NonExistent => "NONEXISTENT",
            Self::NewName => "NEWNAME",
            Self::Referral => "REFERRAL",
            Self::UrlMech => "URLMECH",
            Self::BadUrl => "BADURL",
            Self::BadComparator => "BADCOMPARATOR",
            Self::Annotate => "ANNOTATE",
            Self::Annotations => "ANNOTATIONS",
            Self::TempFail => "TEMPFAIL",
            Self::MaxConvertMessages => "MAXCONVERTMESSAGES",
            Self::MaxConvertParts => "MAXCONVERTPARTS",
            Self::NoUpdate => "NOUPDATE",
            Self::NotificationOverflow => "NOTIFICATIONOVERFLOW",
            Self::BadEvent => "BADEVENT",
            Self::UndefinedFilter => "UNDEFINED-FILTER",
            Self::UidNotSticky => "UIDNOTSTICKY",
            Self::NotSaved => "NOTSAVED",
            Self::HasChildren => "HASCHILDREN",
            Self::UnknownCte => "UNKNOWN-CTE",
            Self::TooBig => "TOOBIG",
            Self::CompressionActive => "COMPRESSIONACTIVE",
            Self::UseAttr => "USEATTR",
            Self::MetadataLongEntries => "METADATA LONGENTRIES",
            Self::MetadataMaxSize => "METADATA MAXSIZE",
            Self::MetadataTooMany => "METADATA TOOMANY",
            Self::MetadataNoPrivate => "METADATA NOPRIVATE",
            Self::Unknown { code, .. } => code.as_str(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct EnhancedStatusCode {
    /// SMTP numeric reply code (250, 550, etc.). Named `reply` to
    /// disambiguate from the dotted enhanced status (`X.Y.Z`), which
    /// lives in the `enhanced` field as a `DiagnosticText`.
    pub reply: u16,
    pub enhanced: Option<DiagnosticText>,
    pub text: Option<DiagnosticText>,
}

impl EnhancedStatusCode {
    #[must_use]
    pub fn new(reply: u16, enhanced: Option<DiagnosticText>, text: Option<DiagnosticText>) -> Self {
        Self {
            reply,
            enhanced,
            text,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GmailSignal {
    InvalidQuery,
    FailedPrecondition,
    InvalidCredentials,
    AuthError,
    QuotaExceeded,
    DailyLimitExceeded,
    RateLimitExceeded,
    UserRateLimitExceeded,
    Forbidden,
    NotFound,
    PreconditionFailed,
    BackendError,
    HistoryNotFound,
    PubSubSubscriptionDeleted,
    PubSubSubscriptionExpired,
    Unknown { code: String },
}

impl GmailSignal {
    fn code(&self) -> &str {
        match self {
            Self::InvalidQuery => "invalidQuery",
            Self::FailedPrecondition => "failedPrecondition",
            Self::InvalidCredentials => "invalidCredentials",
            Self::AuthError => "authError",
            Self::QuotaExceeded => "quotaExceeded",
            Self::DailyLimitExceeded => "dailyLimitExceeded",
            Self::RateLimitExceeded => "rateLimitExceeded",
            Self::UserRateLimitExceeded => "userRateLimitExceeded",
            Self::Forbidden => "forbidden",
            Self::NotFound => "notFound",
            Self::PreconditionFailed => "preconditionFailed",
            Self::BackendError => "backendError",
            Self::HistoryNotFound => "historyNotFound",
            Self::PubSubSubscriptionDeleted => "pubsubSubscriptionDeleted",
            Self::PubSubSubscriptionExpired => "pubsubSubscriptionExpired",
            Self::Unknown { code } => code.as_str(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CauseSummary<'a> {
    pub kind: &'static str,
    pub detail: Option<&'a str>,
    pub transmission_state: Option<TransmissionState>,
    pub status: Option<u16>,
    pub native_code: Option<&'a str>,
    pub transport_kind: Option<TransportKind>,
    pub resource: Option<ResourceKind>,
    pub needed: Option<&'a str>,
    pub downgrade: Option<StrategyDowngrade>,
    pub capability_delta: Option<&'a CapabilityDelta>,
    pub batch_items: Option<&'a [BatchInputInvalidItem]>,
    pub operation: Option<AccountOperation>,
    pub field: Option<&'static str>,
}

impl<'a> CauseSummary<'a> {
    fn empty(kind: &'static str) -> Self {
        Self {
            kind,
            detail: None,
            transmission_state: None,
            status: None,
            native_code: None,
            transport_kind: None,
            resource: None,
            needed: None,
            downgrade: None,
            capability_delta: None,
            batch_items: None,
            operation: None,
            field: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BatchInputInvalidItem {
    pub id: BatchItemId,
    pub reason: BatchInputInvalidReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum BatchInputInvalidReason {
    Malformed,
    Duplicate,
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cause_chain_preserves_order() {
        let chain = CauseChain::new(vec![
            Cause::Attempt(AttemptCause {
                transmission_state: TransmissionState::InFlight,
            }),
            Cause::Wire(WireCause::Jmap(JmapMethod::StateMismatch)),
        ]);

        assert!(matches!(
            chain.outermost(),
            Cause::Attempt(AttemptCause {
                transmission_state: TransmissionState::InFlight
            })
        ));
        assert!(matches!(
            chain.root(),
            Cause::Wire(WireCause::Jmap(JmapMethod::StateMismatch))
        ));
        assert_eq!(chain.iter().count(), 2);
    }

    #[test]
    fn cause_chain_rejects_empty_input() {
        assert!(CauseChain::try_new(Vec::new()).is_none());
    }

    #[test]
    fn cause_summary_carries_attempt_state() {
        let cause = Cause::Attempt(AttemptCause {
            transmission_state: TransmissionState::Acknowledged,
        });

        assert_eq!(
            cause.summary().transmission_state,
            Some(TransmissionState::Acknowledged)
        );
    }

    #[test]
    fn cause_summary_preserves_structured_internal_evidence() {
        let items = vec![BatchInputInvalidItem {
            id: BatchItemId("duplicate".into()),
            reason: BatchInputInvalidReason::Duplicate,
        }];
        let request = Cause::Request(RequestCause::BatchInputInvalid { items });
        assert_eq!(request.summary().batch_items.expect("items").len(), 1);

        let access = Cause::Access(AccessCause::PermissionDenied {
            resource: Some(ResourceKind::Calendar),
        });
        assert_eq!(access.summary().resource, Some(ResourceKind::Calendar));

        let state = Cause::State(StateCause::StrategyFailure {
            downgrade: StrategyDowngrade::QResyncToCondstore,
        });
        assert_eq!(
            state.summary().downgrade,
            Some(StrategyDowngrade::QResyncToCondstore)
        );
    }
}
