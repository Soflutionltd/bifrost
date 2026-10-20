use super::scope::AccountOperation;
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AccountErrorKind {
    Transport(TransportErrorKind),
    Authentication(AuthErrorKind),
    Authorization(AccessErrorKind),
    Server(ServerErrorKind),
    SyncState(SyncStateErrorKind),
    ConcurrencyConflict,
    Request(RequestErrorKind),
    NotFound(ResourceKind),
    Unsupported(AccountOperation),
    Protocol(ProtocolErrorKind),
    /// A failure on the CLIENT side of the wire: this library, or any
    /// `Account` implementation (bifrost's own or a consumer's), never the
    /// provider and never the caller's request. `Protocol(_)` means the
    /// provider sent something wrong and `Request(_)` means the caller asked
    /// for something inexpressible; this is the third party, the code in
    /// between. It derives `RecoveryClass::InternalFailure` (or a
    /// read-back reconcile when a non-idempotent operation may already have
    /// reached the server) and remediation `ReportBug`.
    Internal(InternalErrorKind),
}

/// What failed on the client side. See [`AccountErrorKind::Internal`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum InternalErrorKind {
    /// A state the implementation believes impossible was reached: a
    /// poisoned lock, a type-erased result of the wrong type, a missing
    /// slot, a cursor this code built that will not serialize.
    InvariantViolated,
    /// A local facility the implementation depends on failed at runtime
    /// (the system entropy source, for example) - not a logic error, but
    /// not the provider's or the caller's either.
    RuntimeFailure,
    /// Code panicked and the panic was contained (an IMAP driver task, for
    /// example). The resource it ran on is dead.
    Panicked,
    /// An `Account` implementation broke the contract the sync engine relies
    /// on (a partial batch carrying a checkpoint, say). Raised by the engine
    /// about the implementation it drives, which may be a consumer's.
    AccountContract,
    /// An implementation-defined safety or resource limit was reached (a
    /// pagination walk's page budget, for example). The operation is
    /// supported; the implementation declined to go further, and only
    /// whoever ships it can raise or remove the limit.
    LimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum TransportErrorKind {
    Network,
    Timeout,
    Tls,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum RequestErrorKind {
    Malformed,
    BatchInputInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum AuthErrorKind {
    Expired,
    RefreshTransient,
    Revoked,
    ReauthorizationRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum AccessErrorKind {
    AdminConsentRequired,
    ConditionalAccessBlocked,
    PolicyBlocked,
    InsufficientScope,
    PermissionDenied,
    AccountDisabled,
    MailboxUnavailable { kind: MailboxUnavailableKind },
    MailboxNotLicensed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ServerErrorKind {
    Unavailable,
    RateLimited,
    QuotaExhausted,
    Error { status: Option<u16> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum SyncStateErrorKind {
    CursorInvalid,
    StrategyFailure,
    ScopeCapabilityLost,
    SchemaIncompatible,
    CapabilityChanged,
    OperatorOverrideNeeded,
    /// Access to one cursor scope was revoked mid-sync (an admin pulled
    /// rights on a single shared/other-user IMAP folder). Quarantine just
    /// that scope; do NOT escalate to account-wide auth loss.
    ScopeRevoked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ProtocolErrorKind {
    ParseFailed,
    MissingField,
    ContractViolation,
    PartialResponse,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[non_exhaustive]
pub enum ResourceKind {
    Message,
    Mailbox,
    Thread,
    Calendar,
    Contact,
    Draft,
    Identity,
    Vacation,
    /// Provider-neutral push subscription. Covers Gmail Pub/Sub watch,
    /// Graph webhook subscriptions, JMAP push subscriptions, and any
    /// future IMAP IDLE abstraction.
    PushSubscription,
    /// Account-level resource (Gmail `GmailResource::Account`, Graph
    /// service principals, etc.). Distinct from `Message` so consumers
    /// route an account-level `NotFound` away from the message UX.
    Account,
    /// Server-side mail filter / rule. Covers a ManageSieve script
    /// (RFC 5804 `NONEXISTENT`), a Gmail filter, and a JMAP SieveScript.
    /// The operation vocabulary already treats filters as first class
    /// (`FiltersList` / `FilterCreate` / `FilterUpdate` / `FilterDelete`
    /// / `FilterValidate`), so a missing one has its own resource rather
    /// than borrowing an unrelated variant.
    Filter,
    Event,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum MailboxUnavailableKind {
    Transient,
    Permanent,
}
